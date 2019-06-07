#include "execution/vm/vm.h"

#include <execution/sql/projected_columns_iterator.h>
#include <numeric>
#include <string>
#include <vector>

#include "execution/sql/value.h"
#include "execution/util/common.h"
#include "execution/util/timer.h"
#include "execution/vm/bytecode_function_info.h"
#include "execution/vm/bytecode_handlers.h"
#include "execution/vm/module.h"

namespace tpl::vm {

/**
 * An execution frame where all function's local variables and parameters live
 * for the duration of the function's lifetime.
 */
class VM::Frame {
  friend class VM;

 public:
  /**
   * Constructor
   */
  Frame(u8 *frame_data, std::size_t frame_size) : frame_data_(frame_data), frame_size_(frame_size) {
    TPL_ASSERT(frame_data_ != nullptr, "Frame data cannot be null");
    TPL_ASSERT(frame_size_ >= 0, "Frame size must be >= 0");
    (void)frame_size_;
  }

  /**
   * Access the local variable at the given index in the fame. @em index is an
   * encoded LocalVar that contains both the byte offset of the variable to
   * load and the access mode, i.e., whether the local variable is accessed
   * accessed by address or value.
   * @tparam T The type of the variable the user expects
   * @param index The encoded index into the frame where the variable is
   * @return The value of the variable. Note that this is copied!
   */
  template <typename T>
  T LocalAt(u32 index) const {
    LocalVar local = LocalVar::Decode(index);

    EnsureInFrame(local);

    auto val = reinterpret_cast<uintptr_t>(&frame_data_[local.GetOffset()]);

    if (local.GetAddressMode() == LocalVar::AddressMode::Value) {
      return *reinterpret_cast<T *>(val);
    }

    return (T)(val);  // NOLINT (both static/reinterpret cast semantics)
  }

 private:
#ifndef NDEBUG
  // Ensure the local variable is valid
  void EnsureInFrame(LocalVar var) const {
    if (var.GetOffset() >= frame_size_) {
      std::string error_msg =
          fmt::format("Accessing local at offset {}, beyond frame of size {}", var.GetOffset(), frame_size_);
      EXECUTION_LOG_ERROR("{}", error_msg);
      throw std::runtime_error(error_msg);
    }
  }
#else
  void EnsureInFrame(UNUSED LocalVar var) const {}
#endif

 private:
  u8 *frame_data_;
  std::size_t frame_size_;
};

// ---------------------------------------------------------
// Virtual Machine
// ---------------------------------------------------------

// The maximum amount of stack to use. If the function requires more than 16K
// bytes, acquire space from the heap.
static constexpr const u32 kMaxStackAllocSize = 1ull << 14ull;
// A soft-maximum amount of stack to use. If a function's frame requires more
// than 4K (the soft max), try the stack and fallback to heap. If the function
// requires less, use the stack.
static constexpr const u32 kSoftMaxStackAllocSize = 1ull << 12ull;

VM::VM(const Module *module) : module_(module) {}

// static
void VM::InvokeFunction(const Module *module, const FunctionId func_id, const u8 args[]) {
  // The function's info
  const FunctionInfo *func_info = module->GetFuncInfoById(func_id);
  TPL_ASSERT(func_info != nullptr, "Function doesn't exist in module!");
  const std::size_t frame_size = func_info->frame_size();

  // Let's try to get some space
  bool used_heap = false;
  u8 *raw_frame = nullptr;
  if (frame_size > kMaxStackAllocSize) {
    used_heap = true;
    raw_frame = static_cast<u8 *>(std::aligned_alloc(alignof(u64), frame_size));
  } else if (frame_size > kSoftMaxStackAllocSize) {
    // TODO(pmenon): Check stack before allocation
    raw_frame = static_cast<u8 *>(alloca(frame_size));
  } else {
    raw_frame = static_cast<u8 *>(alloca(frame_size));
  }

  // Copy args into frame
  std::memcpy(raw_frame + func_info->params_start_pos(), args, func_info->params_size());

  EXECUTION_LOG_DEBUG("Executing function '{}'", func_info->name());

  // Let's go. First, create the virtual machine instance.
  VM vm(module);

  // Now get the bytecode for the function and fire it off
  const u8 *bytecode = module->bytecode_module()->GetBytecodeForFunction(*func_info);
  TPL_ASSERT(bytecode != nullptr, "Bytecode cannot be null");
  Frame frame(raw_frame, frame_size);
  vm.Interpret(bytecode, &frame);

  // Cleanup
  if (used_heap) {
    std::free(raw_frame);
  }
}

namespace {

template <typename T>
inline ALWAYS_INLINE T Read(const u8 **ip) {
  static_assert(std::is_integral_v<T>,
                "Read() should only be used to read primitive integer types "
                "directly from the bytecode instruction stream");
  auto ret = *reinterpret_cast<const T *>(*ip);
  (*ip) += sizeof(T);
  return ret;
}

template <typename T>
inline ALWAYS_INLINE T Peek(const u8 **ip) {
  static_assert(std::is_integral_v<T>,
                "Peek() should only be used to read primitive integer types "
                "directly from the bytecode instruction stream");
  return *reinterpret_cast<const T *>(*ip);
}

}  // namespace

// NOLINTNEXTLINE (google-readability-function-size,readability-function-size)
void VM::Interpret(const u8 *ip, Frame *frame) {
  static void *kDispatchTable[] = {
#define ENTRY(name, ...) &&op_##name,
      BYTECODE_LIST(ENTRY)
#undef ENTRY
  };

#ifdef TPL_DEBUG_TRACE_INSTRUCTIONS
#define DEBUG_TRACE_INSTRUCTIONS(op)                                                                                  \
  do {                                                                                                                \
    auto bytecode = Bytecodes::FromByte(op);                                                                          \
    bytecode_counts_[op]++;                                                                                           \
    EXECUTION_LOG_INFO("{0:p}: {1:s}", ip - sizeof(std::underlying_type_t<Bytecode>), Bytecodes::ToString(bytecode)); \
  } while (false)
#else
#define DEBUG_TRACE_INSTRUCTIONS(op) (void)op
#endif

  // TODO(pmenon): Should these READ/PEEK macros take in a vm::OperandType so
  // that we can infer primitive types using traits? This minimizes number of
  // changes if the underlying offset/bytecode/register sizes changes?
#define PEEK_JMP_OFFSET() Peek<i32>(&ip)
#define READ_IMM1() Read<i8>(&ip)
#define READ_IMM2() Read<i16>(&ip)
#define READ_IMM4() Read<i32>(&ip)
#define READ_IMM8() Read<i64>(&ip)
#define READ_UIMM2() Read<u16>(&ip)
#define READ_UIMM4() Read<u32>(&ip)
#define READ_JMP_OFFSET() READ_IMM4()
#define READ_LOCAL_ID() Read<u32>(&ip)
#define READ_OP() Read<std::underlying_type_t<Bytecode>>(&ip)
#define READ_FUNC_ID() READ_UIMM2()

#define OP(name) op_##name
#define DISPATCH_NEXT()           \
  do {                            \
    auto op = READ_OP();          \
    DEBUG_TRACE_INSTRUCTIONS(op); \
    goto *kDispatchTable[op];     \
  } while (false)

  /*****************************************************************************
   *
   * Below this comment begins the primary section of TPL's register-based
   * virtual machine (VM) dispatch area. The VM uses indirect threaded
   * interpretation; each bytecode handler's label is statically generated and
   * stored in @ref kDispatchTable at server compile time. Bytecode handler
   * logic is written as a case using the CASE_OP macro. Handlers can read from
   * and write to registers using the local execution frame's register file
   * (i.e., through @ref Frame::LocalAt()).
   *
   * Upon entry, the instruction pointer (IP) points to the first bytecode of
   * function that is running. The READ_* macros can be used to directly read
   * values from the bytecode stream. The READ_* macros read values from the
   * bytecode stream and advance the IP whereas the PEEK_* macros do only the
   * former, leaving the IP unmodified.
   *
   * IMPORTANT:
   * ----------
   * Bytecode handler code here should only be simple register/IP manipulation
   * (i.e., reading from and writing to registers). Actual full-blown bytecode
   * logic must be implemented externally and invoked from stubs here. This is a
   * strict requirement necessary because it makes code generation to LLVM much
   * simpler.
   *
   ****************************************************************************/

  // Jump to the first instruction
  DISPATCH_NEXT();

  // -------------------------------------------------------
  // Primitive comparison operations
  // -------------------------------------------------------

#define DO_GEN_COMPARISON(op, type)                       \
  OP(op##_##type) : {                                     \
    auto *dest = frame->LocalAt<bool *>(READ_LOCAL_ID()); \
    auto lhs = frame->LocalAt<type>(READ_LOCAL_ID());     \
    auto rhs = frame->LocalAt<type>(READ_LOCAL_ID());     \
    Op##op##_##type(dest, lhs, rhs);                      \
    DISPATCH_NEXT();                                      \
  }
#define GEN_COMPARISON_TYPES(type, ...)     \
  DO_GEN_COMPARISON(GreaterThan, type)      \
  DO_GEN_COMPARISON(GreaterThanEqual, type) \
  DO_GEN_COMPARISON(Equal, type)            \
  DO_GEN_COMPARISON(LessThan, type)         \
  DO_GEN_COMPARISON(LessThanEqual, type)    \
  DO_GEN_COMPARISON(NotEqual, type)

  INT_TYPES(GEN_COMPARISON_TYPES)
#undef GEN_COMPARISON_TYPES
#undef DO_GEN_COMPARISON

  // -------------------------------------------------------
  // Primitive arithmetic and binary operations
  // -------------------------------------------------------

#define DO_GEN_ARITHMETIC_OP(op, test, type)              \
  OP(op##_##type) : {                                     \
    auto *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    auto lhs = frame->LocalAt<type>(READ_LOCAL_ID());     \
    auto rhs = frame->LocalAt<type>(READ_LOCAL_ID());     \
    if ((test) && rhs == 0u) {                            \
      /* TODO(pmenon): Proper error */                    \
      EXECUTION_LOG_ERROR("Division by zero error!");     \
    }                                                     \
    Op##op##_##type(dest, lhs, rhs);                      \
    DISPATCH_NEXT();                                      \
  }
#define GEN_ARITHMETIC_OP(type, ...)        \
  DO_GEN_ARITHMETIC_OP(Add, false, type)    \
  DO_GEN_ARITHMETIC_OP(Sub, false, type)    \
  DO_GEN_ARITHMETIC_OP(Mul, false, type)    \
  DO_GEN_ARITHMETIC_OP(Div, true, type)     \
  DO_GEN_ARITHMETIC_OP(Rem, true, type)     \
  DO_GEN_ARITHMETIC_OP(BitAnd, false, type) \
  DO_GEN_ARITHMETIC_OP(BitOr, false, type)  \
  DO_GEN_ARITHMETIC_OP(BitXor, false, type)

  INT_TYPES(GEN_ARITHMETIC_OP)
#undef GEN_ARITHMETIC_OP
#undef DO_GEN_ARITHMETIC_OP

  // -------------------------------------------------------
  // Bitwise and integer negation
  // -------------------------------------------------------

#define GEN_NEG_OP(type, ...)                             \
  OP(Neg##_##type) : {                                    \
    auto *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    auto input = frame->LocalAt<type>(READ_LOCAL_ID());   \
    OpNeg##_##type(dest, input);                          \
    DISPATCH_NEXT();                                      \
  }                                                       \
  OP(BitNeg##_##type) : {                                 \
    auto *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    auto input = frame->LocalAt<type>(READ_LOCAL_ID());   \
    OpBitNeg##_##type(dest, input);                       \
    DISPATCH_NEXT();                                      \
  }

  INT_TYPES(GEN_NEG_OP)
#undef GEN_NEG_OP

  OP(Not) : {
    auto *dest = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto input = frame->LocalAt<bool>(READ_LOCAL_ID());
    OpNot(dest, input);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Jumps
  // -------------------------------------------------------

  OP(Jump) : {
    auto skip = PEEK_JMP_OFFSET();
    if (TPL_LIKELY(OpJump())) {
      ip += skip;
    }
    DISPATCH_NEXT();
  }

  OP(JumpIfTrue) : {
    auto cond = frame->LocalAt<bool>(READ_LOCAL_ID());
    auto skip = PEEK_JMP_OFFSET();
    if (OpJumpIfTrue(cond)) {
      ip += skip;
    } else {
      READ_JMP_OFFSET();
    }
    DISPATCH_NEXT();
  }

  OP(JumpIfFalse) : {
    auto cond = frame->LocalAt<bool>(READ_LOCAL_ID());
    auto skip = PEEK_JMP_OFFSET();
    if (OpJumpIfFalse(cond)) {
      ip += skip;
    } else {
      READ_JMP_OFFSET();
    }
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Low-level memory operations
  // -------------------------------------------------------

  OP(IsNullPtr) : {
    auto *result = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *input_ptr = frame->LocalAt<const void *>(READ_LOCAL_ID());
    OpIsNullPtr(result, input_ptr);
    DISPATCH_NEXT();
  }

  OP(IsNotNullPtr) : {
    auto *result = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *input_ptr = frame->LocalAt<const void *>(READ_LOCAL_ID());
    OpIsNotNullPtr(result, input_ptr);
    DISPATCH_NEXT();
  }

#define GEN_DEREF(type, size)                             \
  OP(Deref##size) : {                                     \
    auto *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    auto *src = frame->LocalAt<type *>(READ_LOCAL_ID());  \
    OpDeref##size(dest, src);                             \
    DISPATCH_NEXT();                                      \
  }
  GEN_DEREF(i8, 1);
  GEN_DEREF(i16, 2);
  GEN_DEREF(i32, 4);
  GEN_DEREF(i64, 8);
#undef GEN_DEREF

  OP(DerefN) : {
    auto *dest = frame->LocalAt<byte *>(READ_LOCAL_ID());
    auto *src = frame->LocalAt<byte *>(READ_LOCAL_ID());
    auto len = READ_UIMM4();
    OpDerefN(dest, src, len);
    DISPATCH_NEXT();
  }

#define GEN_ASSIGN(type, size)                            \
  OP(Assign##size) : {                                    \
    auto *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    auto src = frame->LocalAt<type>(READ_LOCAL_ID());     \
    OpAssign##size(dest, src);                            \
    DISPATCH_NEXT();                                      \
  }                                                       \
  OP(AssignImm##size) : {                                 \
    auto *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    OpAssignImm##size(dest, READ_IMM##size());            \
    DISPATCH_NEXT();                                      \
  }
  GEN_ASSIGN(i8, 1);
  GEN_ASSIGN(i16, 2);
  GEN_ASSIGN(i32, 4);
  GEN_ASSIGN(i64, 8);
#undef GEN_ASSIGN

  OP(Lea) : {
    auto **dest = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *src = frame->LocalAt<byte *>(READ_LOCAL_ID());
    auto offset = READ_UIMM4();
    OpLea(dest, src, offset);
    DISPATCH_NEXT();
  }

  OP(LeaScaled) : {
    auto **dest = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *src = frame->LocalAt<byte *>(READ_LOCAL_ID());
    auto index = frame->LocalAt<u32>(READ_LOCAL_ID());
    auto scale = READ_UIMM4();
    auto offset = READ_UIMM4();
    OpLeaScaled(dest, src, index, scale, offset);
    DISPATCH_NEXT();
  }

  OP(Call) : {
    ip = ExecuteCall(ip, frame);
    DISPATCH_NEXT();
  }

  OP(Return) : {
    OpReturn();
    return;
  }

  // -------------------------------------------------------
  // Execution Context
  // -------------------------------------------------------

  OP(ExecutionContextGetMemoryPool) : {
    auto *memory_pool = frame->LocalAt<sql::MemoryPool **>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    OpExecutionContextGetMemoryPool(memory_pool, exec_ctx);
    DISPATCH_NEXT();
  }

  OP(ThreadStateContainerInit) : {
    auto *thread_state_container = frame->LocalAt<sql::ThreadStateContainer *>(READ_LOCAL_ID());
    auto *memory = frame->LocalAt<tpl::sql::MemoryPool *>(READ_LOCAL_ID());
    OpThreadStateContainerInit(thread_state_container, memory);
    DISPATCH_NEXT();
  }

  OP(ThreadStateContainerIterate) : {
    auto *thread_state_container = frame->LocalAt<sql::ThreadStateContainer *>(READ_LOCAL_ID());
    auto ctx = frame->LocalAt<void *>(READ_LOCAL_ID());
    auto iterate_fn_id = READ_FUNC_ID();

    auto iterate_fn =
        reinterpret_cast<sql::ThreadStateContainer::IterateFn>(module_->GetRawFunctionImpl(iterate_fn_id));
    OpThreadStateContainerIterate(thread_state_container, ctx, iterate_fn);
    DISPATCH_NEXT();
  }

  OP(ThreadStateContainerReset) : {
    auto *thread_state_container = frame->LocalAt<sql::ThreadStateContainer *>(READ_LOCAL_ID());
    auto size = frame->LocalAt<u32>(READ_LOCAL_ID());
    auto init_fn_id = READ_FUNC_ID();
    auto destroy_fn_id = READ_FUNC_ID();
    auto *ctx = frame->LocalAt<void *>(READ_LOCAL_ID());

    auto init_fn = reinterpret_cast<sql::ThreadStateContainer::InitFn>(module_->GetRawFunctionImpl(init_fn_id));
    auto destroy_fn =
        reinterpret_cast<sql::ThreadStateContainer::DestroyFn>(module_->GetRawFunctionImpl(destroy_fn_id));
    OpThreadStateContainerReset(thread_state_container, size, init_fn, destroy_fn, ctx);
    DISPATCH_NEXT();
  }

  OP(ThreadStateContainerFree) : {
    auto *thread_state_container = frame->LocalAt<sql::ThreadStateContainer *>(READ_LOCAL_ID());
    OpThreadStateContainerFree(thread_state_container);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Table Vector and ProjectedColumns Iterator (PCI) ops
  // -------------------------------------------------------

  OP(TableVectorIteratorInit) : {
    auto *iter = frame->LocalAt<sql::TableVectorIterator *>(READ_LOCAL_ID());
    auto db_oid = READ_UIMM4();
    auto ns_oid = READ_UIMM4();
    auto table_oid = READ_UIMM4();
    auto exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    OpTableVectorIteratorInit(iter, db_oid, ns_oid, table_oid, exec_ctx);
    DISPATCH_NEXT();
  }

  OP(TableVectorIteratorPerformInit) : {
    auto *iter = frame->LocalAt<sql::TableVectorIterator *>(READ_LOCAL_ID());
    OpTableVectorIteratorPerformInit(iter);
    DISPATCH_NEXT();
  }

  OP(TableVectorIteratorNext) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::TableVectorIterator *>(READ_LOCAL_ID());
    OpTableVectorIteratorNext(has_more, iter);
    DISPATCH_NEXT();
  }

  OP(TableVectorIteratorFree) : {
    auto *iter = frame->LocalAt<sql::TableVectorIterator *>(READ_LOCAL_ID());
    OpTableVectorIteratorFree(iter);
    DISPATCH_NEXT();
  }

  OP(TableVectorIteratorGetPCI) : {
    auto *pci = frame->LocalAt<sql::ProjectedColumnsIterator **>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::TableVectorIterator *>(READ_LOCAL_ID());
    OpTableVectorIteratorGetPCI(pci, iter);
    DISPATCH_NEXT();
  }

  OP(ParallelScanTable) : {
    auto db_oid = READ_UIMM4();
    auto table_oid = READ_UIMM4();
    auto query_state = frame->LocalAt<void *>(READ_LOCAL_ID());
    auto thread_state_container = frame->LocalAt<sql::ThreadStateContainer *>(READ_LOCAL_ID());
    auto scan_fn_id = READ_FUNC_ID();

    auto scan_fn = reinterpret_cast<sql::TableVectorIterator::ScanFn>(module_->GetRawFunctionImpl(scan_fn_id));
    OpParallelScanTable(db_oid, table_oid, query_state, thread_state_container, scan_fn);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // PCI iteration operations
  // -------------------------------------------------------

  OP(PCIIsFiltered) : {
    auto *is_filtered = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::ProjectedColumnsIterator *>(READ_LOCAL_ID());
    OpPCIIsFiltered(is_filtered, iter);
    DISPATCH_NEXT();
  }

  OP(PCIHasNext) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::ProjectedColumnsIterator *>(READ_LOCAL_ID());
    OpPCIHasNext(has_more, iter);
    DISPATCH_NEXT();
  }

  OP(PCIHasNextFiltered) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::ProjectedColumnsIterator *>(READ_LOCAL_ID());
    OpPCIHasNextFiltered(has_more, iter);
    DISPATCH_NEXT();
  }

  OP(PCIAdvance) : {
    auto *iter = frame->LocalAt<sql::ProjectedColumnsIterator *>(READ_LOCAL_ID());
    OpPCIAdvance(iter);
    DISPATCH_NEXT();
  }

  OP(PCIAdvanceFiltered) : {
    auto *iter = frame->LocalAt<sql::ProjectedColumnsIterator *>(READ_LOCAL_ID());
    OpPCIAdvanceFiltered(iter);
    DISPATCH_NEXT();
  }

  OP(PCIMatch) : {
    auto *iter = frame->LocalAt<sql::ProjectedColumnsIterator *>(READ_LOCAL_ID());
    auto match = frame->LocalAt<bool>(READ_LOCAL_ID());
    OpPCIMatch(iter, match);
    DISPATCH_NEXT();
  }

  OP(PCIReset) : {
    auto *iter = frame->LocalAt<sql::ProjectedColumnsIterator *>(READ_LOCAL_ID());
    OpPCIReset(iter);
    DISPATCH_NEXT();
  }

  OP(PCIResetFiltered) : {
    auto *iter = frame->LocalAt<sql::ProjectedColumnsIterator *>(READ_LOCAL_ID());
    OpPCIResetFiltered(iter);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // PCI element access
  // -------------------------------------------------------

#define GEN_PCI_ACCESS(type_str, type)                                            \
  OP(PCIGet##type_str) : {                                                        \
    auto *result = frame->LocalAt<type *>(READ_LOCAL_ID());                       \
    auto *pci = frame->LocalAt<sql::ProjectedColumnsIterator *>(READ_LOCAL_ID()); \
    auto col_idx = READ_UIMM4();                                                  \
    OpPCIGet##type_str(result, pci, col_idx);                                     \
    DISPATCH_NEXT();                                                              \
  }                                                                               \
  OP(PCIGet##type_str##Null) : {                                                  \
    auto *result = frame->LocalAt<type *>(READ_LOCAL_ID());                       \
    auto *pci = frame->LocalAt<sql::ProjectedColumnsIterator *>(READ_LOCAL_ID()); \
    auto col_idx = READ_UIMM4();                                                  \
    OpPCIGet##type_str##Null(result, pci, col_idx);                               \
    DISPATCH_NEXT();                                                              \
  }
  GEN_PCI_ACCESS(SmallInt, sql::Integer)
  GEN_PCI_ACCESS(Integer, sql::Integer)
  GEN_PCI_ACCESS(BigInt, sql::Integer)
  GEN_PCI_ACCESS(Real, sql::Real)
  GEN_PCI_ACCESS(Double, sql::Real)
  GEN_PCI_ACCESS(Decimal, sql::Decimal)
#undef GEN_PCI_ACCESS

#define GEN_PCI_FILTER(Op)                                                         \
  OP(PCIFilter##Op) : {                                                            \
    auto *size = frame->LocalAt<u32 *>(READ_LOCAL_ID());                           \
    auto *iter = frame->LocalAt<sql::ProjectedColumnsIterator *>(READ_LOCAL_ID()); \
    auto col_idx = READ_UIMM4();                                                   \
    auto type = READ_IMM1();                                                       \
    auto val = READ_IMM8();                                                        \
    OpPCIFilter##Op(size, iter, col_idx, type, val);                               \
    DISPATCH_NEXT();                                                               \
  }
  GEN_PCI_FILTER(Equal)
  GEN_PCI_FILTER(GreaterThan)
  GEN_PCI_FILTER(GreaterThanEqual)
  GEN_PCI_FILTER(LessThan)
  GEN_PCI_FILTER(LessThanEqual)
  GEN_PCI_FILTER(NotEqual)
#undef GEN_PCI_FILTER

  // ------------------------------------------------------
  // Hashing
  // ------------------------------------------------------

  OP(HashInt) : {
    auto *hash_val = frame->LocalAt<hash_t *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpHashInt(hash_val, input);
    DISPATCH_NEXT();
  }

  OP(HashReal) : {
    auto *hash_val = frame->LocalAt<hash_t *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    OpHashReal(hash_val, input);
    DISPATCH_NEXT();
  }

  OP(HashString) : {
    auto *hash_val = frame->LocalAt<hash_t *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<sql::VarBuffer *>(READ_LOCAL_ID());
    OpHashString(hash_val, input);
    DISPATCH_NEXT();
  }

  OP(HashCombine) : {
    auto *hash_val = frame->LocalAt<hash_t *>(READ_LOCAL_ID());
    auto new_hash_val = frame->LocalAt<hash_t>(READ_LOCAL_ID());
    OpHashCombine(hash_val, new_hash_val);
    DISPATCH_NEXT();
  }

  // ------------------------------------------------------
  // Filter Manager
  // ------------------------------------------------------

  OP(FilterManagerInit) : {
    auto *filter_manager = frame->LocalAt<sql::FilterManager *>(READ_LOCAL_ID());
    OpFilterManagerInit(filter_manager);
    DISPATCH_NEXT();
  }

  OP(FilterManagerStartNewClause) : {
    auto *filter_manager = frame->LocalAt<sql::FilterManager *>(READ_LOCAL_ID());
    OpFilterManagerStartNewClause(filter_manager);
    DISPATCH_NEXT();
  }

  OP(FilterManagerInsertFlavor) : {
    auto *filter_manager = frame->LocalAt<sql::FilterManager *>(READ_LOCAL_ID());
    auto func_id = READ_FUNC_ID();
    auto fn = reinterpret_cast<sql::FilterManager::MatchFn>(module_->GetRawFunctionImpl(func_id));
    OpFilterManagerInsertFlavor(filter_manager, fn);
    DISPATCH_NEXT();
  }

  OP(FilterManagerFinalize) : {
    auto *filter_manager = frame->LocalAt<sql::FilterManager *>(READ_LOCAL_ID());
    OpFilterManagerFinalize(filter_manager);
    DISPATCH_NEXT();
  }

  OP(FilterManagerRunFilters) : {
    auto *filter_manager = frame->LocalAt<sql::FilterManager *>(READ_LOCAL_ID());
    auto *pci = frame->LocalAt<sql::ProjectedColumnsIterator *>(READ_LOCAL_ID());
    OpFilterManagerRunFilters(filter_manager, pci);
    DISPATCH_NEXT();
  }

  OP(FilterManagerFree) : {
    auto *filter_manager = frame->LocalAt<sql::FilterManager *>(READ_LOCAL_ID());
    OpFilterManagerFree(filter_manager);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // SQL Integer Comparison Operations
  // -------------------------------------------------------

  OP(ForceBoolTruth) : {
    auto *result = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *sql_int = frame->LocalAt<sql::BoolVal *>(READ_LOCAL_ID());
    OpForceBoolTruth(result, sql_int);
    DISPATCH_NEXT();
  }

  OP(InitBool) : {
    auto *sql_bool = frame->LocalAt<sql::BoolVal *>(READ_LOCAL_ID());
    auto val = frame->LocalAt<bool>(READ_LOCAL_ID());
    OpInitBool(sql_bool, val);
    DISPATCH_NEXT();
  }

  OP(InitInteger) : {
    auto *sql_int = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto val = frame->LocalAt<i32>(READ_LOCAL_ID());
    OpInitInteger(sql_int, val);
    DISPATCH_NEXT();
  }

  OP(InitReal) : {
    auto *sql_real = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto val = frame->LocalAt<double>(READ_LOCAL_ID());
    OpInitReal(sql_real, val);
    DISPATCH_NEXT();
  }

#define GEN_CMP(op)                                                 \
  OP(op##Integer) : {                                               \
    auto *result = frame->LocalAt<sql::BoolVal *>(READ_LOCAL_ID()); \
    auto *left = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());   \
    auto *right = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());  \
    Op##op##Integer(result, left, right);                           \
    DISPATCH_NEXT();                                                \
  }
  GEN_CMP(GreaterThan);
  GEN_CMP(GreaterThanEqual);
  GEN_CMP(Equal);
  GEN_CMP(LessThan);
  GEN_CMP(LessThanEqual);
  GEN_CMP(NotEqual);
#undef GEN_CMP

#define GEN_MATH_OPS(op)                                            \
  OP(op##Integer) : {                                               \
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID()); \
    auto *left = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());   \
    auto *right = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());  \
    Op##op##Integer(result, left, right);                           \
    DISPATCH_NEXT();                                                \
  }

  GEN_MATH_OPS(Add)
  GEN_MATH_OPS(Sub)
  GEN_MATH_OPS(Mul)
  GEN_MATH_OPS(Div)
  GEN_MATH_OPS(Rem)

  // -------------------------------------------------------
  // Aggregations
  // -------------------------------------------------------

  OP(AggregationHashTableInit) : {
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    auto *memory = frame->LocalAt<tpl::sql::MemoryPool *>(READ_LOCAL_ID());
    auto payload_size = frame->LocalAt<u32>(READ_LOCAL_ID());
    OpAggregationHashTableInit(agg_hash_table, memory, payload_size);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableInsert) : {
    auto *result = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    auto hash = frame->LocalAt<hash_t>(READ_LOCAL_ID());
    OpAggregationHashTableInsert(result, agg_hash_table, hash);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableLookup) : {
    auto *result = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    auto hash = frame->LocalAt<hash_t>(READ_LOCAL_ID());
    auto key_eq_fn_id = READ_FUNC_ID();
    auto *iters = frame->LocalAt<void **>(READ_LOCAL_ID());

    auto key_eq_fn = reinterpret_cast<sql::AggregationHashTable::KeyEqFn>(module_->GetRawFunctionImpl(key_eq_fn_id));
    OpAggregationHashTableLookup(result, agg_hash_table, hash, key_eq_fn, iters);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableProcessBatch) : {
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    auto **iters = frame->LocalAt<sql::ProjectedColumnsIterator **>(READ_LOCAL_ID());
    auto hash_fn_id = READ_FUNC_ID();
    auto key_eq_fn_id = READ_FUNC_ID();
    auto init_agg_fn_id = READ_FUNC_ID();
    auto merge_agg_fn_id = READ_FUNC_ID();

    auto hash_fn = reinterpret_cast<sql::AggregationHashTable::HashFn>(module_->GetRawFunctionImpl(hash_fn_id));
    auto key_eq_fn = reinterpret_cast<sql::AggregationHashTable::KeyEqFn>(module_->GetRawFunctionImpl(key_eq_fn_id));
    auto init_agg_fn =
        reinterpret_cast<sql::AggregationHashTable::InitAggFn>(module_->GetRawFunctionImpl(init_agg_fn_id));
    auto advance_agg_fn =
        reinterpret_cast<sql::AggregationHashTable::AdvanceAggFn>(module_->GetRawFunctionImpl(merge_agg_fn_id));
    OpAggregationHashTableProcessBatch(agg_hash_table, iters, hash_fn, key_eq_fn, init_agg_fn, advance_agg_fn);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableTransferPartitions) : {
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    auto *thread_state_container = frame->LocalAt<sql::ThreadStateContainer *>(READ_LOCAL_ID());
    auto agg_ht_offset = frame->LocalAt<u32>(READ_LOCAL_ID());
    auto merge_partition_fn_id = READ_FUNC_ID();

    auto merge_partition_fn = reinterpret_cast<sql::AggregationHashTable::MergePartitionFn>(
        module_->GetRawFunctionImpl(merge_partition_fn_id));
    OpAggregationHashTableTransferPartitions(agg_hash_table, thread_state_container, agg_ht_offset, merge_partition_fn);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableParallelPartitionedScan) : {
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    auto *query_state = frame->LocalAt<void *>(READ_LOCAL_ID());
    auto *thread_state_container = frame->LocalAt<sql::ThreadStateContainer *>(READ_LOCAL_ID());
    auto scan_partition_fn_id = READ_FUNC_ID();

    auto scan_partition_fn =
        reinterpret_cast<sql::AggregationHashTable::ScanPartitionFn>(module_->GetRawFunctionImpl(scan_partition_fn_id));
    OpAggregationHashTableParallelPartitionedScan(agg_hash_table, query_state, thread_state_container,
                                                  scan_partition_fn);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableFree) : {
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    OpAggregationHashTableFree(agg_hash_table);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableIteratorInit) : {
    auto *iter = frame->LocalAt<sql::AggregationHashTableIterator *>(READ_LOCAL_ID());
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    OpAggregationHashTableIteratorInit(iter, agg_hash_table);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableIteratorHasNext) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::AggregationHashTableIterator *>(READ_LOCAL_ID());
    OpAggregationHashTableIteratorHasNext(has_more, iter);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableIteratorNext) : {
    auto *agg_hash_table_iter = frame->LocalAt<sql::AggregationHashTableIterator *>(READ_LOCAL_ID());
    OpAggregationHashTableIteratorNext(agg_hash_table_iter);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableIteratorGetRow) : {
    auto *row = frame->LocalAt<const byte **>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::AggregationHashTableIterator *>(READ_LOCAL_ID());
    OpAggregationHashTableIteratorGetRow(row, iter);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableIteratorFree) : {
    auto *agg_hash_table_iter = frame->LocalAt<sql::AggregationHashTableIterator *>(READ_LOCAL_ID());
    OpAggregationHashTableIteratorFree(agg_hash_table_iter);
    DISPATCH_NEXT();
  }

  OP(AggregationOverflowPartitionIteratorHasNext) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *overflow_iter = frame->LocalAt<sql::AggregationOverflowPartitionIterator *>(READ_LOCAL_ID());
    OpAggregationOverflowPartitionIteratorHasNext(has_more, overflow_iter);
    DISPATCH_NEXT();
  }

  OP(AggregationOverflowPartitionIteratorNext) : {
    auto *overflow_iter = frame->LocalAt<sql::AggregationOverflowPartitionIterator *>(READ_LOCAL_ID());
    OpAggregationOverflowPartitionIteratorNext(overflow_iter);
    DISPATCH_NEXT();
  }

  OP(AggregationOverflowPartitionIteratorGetHash) : {
    auto *hash = frame->LocalAt<hash_t *>(READ_LOCAL_ID());
    auto *overflow_iter = frame->LocalAt<sql::AggregationOverflowPartitionIterator *>(READ_LOCAL_ID());
    OpAggregationOverflowPartitionIteratorGetHash(hash, overflow_iter);
    DISPATCH_NEXT();
  }

  OP(AggregationOverflowPartitionIteratorGetRow) : {
    auto *row = frame->LocalAt<const byte **>(READ_LOCAL_ID());
    auto *overflow_iter = frame->LocalAt<sql::AggregationOverflowPartitionIterator *>(READ_LOCAL_ID());
    OpAggregationOverflowPartitionIteratorGetRow(row, overflow_iter);
    DISPATCH_NEXT();
  }

  OP(CountAggregateInit) : {
    auto *agg = frame->LocalAt<sql::CountAggregate *>(READ_LOCAL_ID());
    OpCountAggregateInit(agg);
    DISPATCH_NEXT();
  }

  OP(CountAggregateAdvance) : {
    auto *agg = frame->LocalAt<sql::CountAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Val *>(READ_LOCAL_ID());
    OpCountAggregateAdvance(agg, val);
    DISPATCH_NEXT();
  }

  OP(CountAggregateMerge) : {
    auto *agg_1 = frame->LocalAt<sql::CountAggregate *>(READ_LOCAL_ID());
    auto *agg_2 = frame->LocalAt<sql::CountAggregate *>(READ_LOCAL_ID());
    OpCountAggregateMerge(agg_1, agg_2);
    DISPATCH_NEXT();
  }

  OP(CountAggregateReset) : {
    auto *agg = frame->LocalAt<sql::CountAggregate *>(READ_LOCAL_ID());
    OpCountAggregateReset(agg);
    DISPATCH_NEXT();
  }

  OP(CountAggregateGetResult) : {
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto *agg = frame->LocalAt<sql::CountAggregate *>(READ_LOCAL_ID());
    OpCountAggregateGetResult(result, agg);
    DISPATCH_NEXT();
  }

  OP(CountAggregateFree) : {
    auto *agg = frame->LocalAt<sql::CountAggregate *>(READ_LOCAL_ID());
    OpCountAggregateFree(agg);
    DISPATCH_NEXT();
  }

  OP(CountStarAggregateInit) : {
    auto *agg = frame->LocalAt<sql::CountStarAggregate *>(READ_LOCAL_ID());
    OpCountStarAggregateInit(agg);
    DISPATCH_NEXT();
  }

  OP(CountStarAggregateAdvance) : {
    auto *agg = frame->LocalAt<sql::CountStarAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Val *>(READ_LOCAL_ID());
    OpCountStarAggregateAdvance(agg, val);
    DISPATCH_NEXT();
  }

  OP(CountStarAggregateMerge) : {
    auto *agg_1 = frame->LocalAt<sql::CountStarAggregate *>(READ_LOCAL_ID());
    auto *agg_2 = frame->LocalAt<sql::CountStarAggregate *>(READ_LOCAL_ID());
    OpCountStarAggregateMerge(agg_1, agg_2);
    DISPATCH_NEXT();
  }

  OP(CountStarAggregateReset) : {
    auto *agg = frame->LocalAt<sql::CountStarAggregate *>(READ_LOCAL_ID());
    OpCountStarAggregateReset(agg);
    DISPATCH_NEXT();
  }

  OP(CountStarAggregateGetResult) : {
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto *agg = frame->LocalAt<sql::CountStarAggregate *>(READ_LOCAL_ID());
    OpCountStarAggregateGetResult(result, agg);
    DISPATCH_NEXT();
  }

  OP(CountStarAggregateFree) : {
    auto *agg = frame->LocalAt<sql::CountStarAggregate *>(READ_LOCAL_ID());
    OpCountStarAggregateFree(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerSumAggregateInit) : {
    auto *agg = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    OpIntegerSumAggregateInit(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerSumAggregateAdvance) : {
    auto *agg = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpIntegerSumAggregateAdvance(agg, val);
    DISPATCH_NEXT();
  }

  OP(IntegerSumAggregateAdvanceNullable) : {
    auto *agg = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpIntegerSumAggregateAdvanceNullable(agg, val);
    DISPATCH_NEXT();
  }

  OP(IntegerSumAggregateMerge) : {
    auto *agg_1 = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    auto *agg_2 = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    OpIntegerSumAggregateMerge(agg_1, agg_2);
    DISPATCH_NEXT();
  }

  OP(IntegerSumAggregateReset) : {
    auto *agg = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    OpIntegerSumAggregateReset(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerSumAggregateGetResult) : {
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto *agg = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    OpIntegerSumAggregateGetResult(result, agg);
    DISPATCH_NEXT();
  }

  OP(IntegerSumAggregateFree) : {
    auto *agg = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    OpIntegerSumAggregateFree(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerMaxAggregateInit) : {
    auto *agg = frame->LocalAt<sql::IntegerMaxAggregate *>(READ_LOCAL_ID());
    OpIntegerMaxAggregateInit(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerMaxAggregateAdvance) : {
    auto *agg = frame->LocalAt<sql::IntegerMaxAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpIntegerMaxAggregateAdvance(agg, val);
    DISPATCH_NEXT();
  }

  OP(IntegerMaxAggregateAdvanceNullable) : {
    auto *agg = frame->LocalAt<sql::IntegerMaxAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpIntegerMaxAggregateAdvanceNullable(agg, val);
    DISPATCH_NEXT();
  }

  OP(IntegerMaxAggregateMerge) : {
    auto *agg_1 = frame->LocalAt<sql::IntegerMaxAggregate *>(READ_LOCAL_ID());
    auto *agg_2 = frame->LocalAt<sql::IntegerMaxAggregate *>(READ_LOCAL_ID());
    OpIntegerMaxAggregateMerge(agg_1, agg_2);
    DISPATCH_NEXT();
  }

  OP(IntegerMaxAggregateReset) : {
    auto *agg = frame->LocalAt<sql::IntegerMaxAggregate *>(READ_LOCAL_ID());
    OpIntegerMaxAggregateReset(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerMaxAggregateGetResult) : {
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto *agg = frame->LocalAt<sql::IntegerMaxAggregate *>(READ_LOCAL_ID());
    OpIntegerMaxAggregateGetResult(result, agg);
    DISPATCH_NEXT();
  }

  OP(IntegerMaxAggregateFree) : {
    auto *agg = frame->LocalAt<sql::IntegerMaxAggregate *>(READ_LOCAL_ID());
    OpIntegerMaxAggregateFree(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerMinAggregateInit) : {
    auto *agg = frame->LocalAt<sql::IntegerMinAggregate *>(READ_LOCAL_ID());
    OpIntegerMinAggregateInit(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerMinAggregateAdvance) : {
    auto *agg = frame->LocalAt<sql::IntegerMinAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpIntegerMinAggregateAdvance(agg, val);
    DISPATCH_NEXT();
  }

  OP(IntegerMinAggregateAdvanceNullable) : {
    auto *agg = frame->LocalAt<sql::IntegerMinAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpIntegerMinAggregateAdvanceNullable(agg, val);
    DISPATCH_NEXT();
  }

  OP(IntegerMinAggregateMerge) : {
    auto *agg_1 = frame->LocalAt<sql::IntegerMinAggregate *>(READ_LOCAL_ID());
    auto *agg_2 = frame->LocalAt<sql::IntegerMinAggregate *>(READ_LOCAL_ID());
    OpIntegerMinAggregateMerge(agg_1, agg_2);
    DISPATCH_NEXT();
  }

  OP(IntegerMinAggregateReset) : {
    auto *agg = frame->LocalAt<sql::IntegerMinAggregate *>(READ_LOCAL_ID());
    OpIntegerMinAggregateReset(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerMinAggregateGetResult) : {
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto *agg = frame->LocalAt<sql::IntegerMinAggregate *>(READ_LOCAL_ID());
    OpIntegerMinAggregateGetResult(result, agg);
    DISPATCH_NEXT();
  }

  OP(IntegerMinAggregateFree) : {
    auto *agg = frame->LocalAt<sql::IntegerMinAggregate *>(READ_LOCAL_ID());
    OpIntegerMinAggregateFree(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerAvgAggregateInit) : {
    auto *agg = frame->LocalAt<sql::IntegerAvgAggregate *>(READ_LOCAL_ID());
    OpIntegerAvgAggregateInit(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerAvgAggregateAdvance) : {
    auto *agg = frame->LocalAt<sql::IntegerAvgAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpIntegerAvgAggregateAdvance(agg, val);
    DISPATCH_NEXT();
  }

  OP(IntegerAvgAggregateAdvanceNullable) : {
    auto *agg = frame->LocalAt<sql::IntegerAvgAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpIntegerAvgAggregateAdvanceNullable(agg, val);
    DISPATCH_NEXT();
  }

  OP(IntegerAvgAggregateMerge) : {
    auto *agg_1 = frame->LocalAt<sql::IntegerAvgAggregate *>(READ_LOCAL_ID());
    auto *agg_2 = frame->LocalAt<sql::IntegerAvgAggregate *>(READ_LOCAL_ID());
    OpIntegerAvgAggregateMerge(agg_1, agg_2);
    DISPATCH_NEXT();
  }

  OP(IntegerAvgAggregateReset) : {
    auto *agg = frame->LocalAt<sql::IntegerAvgAggregate *>(READ_LOCAL_ID());
    OpIntegerAvgAggregateReset(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerAvgAggregateGetResult) : {
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto *agg = frame->LocalAt<sql::IntegerAvgAggregate *>(READ_LOCAL_ID());
    OpIntegerAvgAggregateGetResult(result, agg);
    DISPATCH_NEXT();
  }

  OP(IntegerAvgAggregateFree) : {
    auto *agg = frame->LocalAt<sql::IntegerAvgAggregate *>(READ_LOCAL_ID());
    OpIntegerAvgAggregateFree(agg);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Hash Joins
  // -------------------------------------------------------

  OP(JoinHashTableInit) : {
    auto *join_hash_table = frame->LocalAt<sql::JoinHashTable *>(READ_LOCAL_ID());
    auto *memory = frame->LocalAt<sql::MemoryPool *>(READ_LOCAL_ID());
    auto tuple_size = frame->LocalAt<u32>(READ_LOCAL_ID());
    OpJoinHashTableInit(join_hash_table, memory, tuple_size);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableAllocTuple) : {
    auto *result = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *join_hash_table = frame->LocalAt<sql::JoinHashTable *>(READ_LOCAL_ID());
    auto hash = frame->LocalAt<hash_t>(READ_LOCAL_ID());
    OpJoinHashTableAllocTuple(result, join_hash_table, hash);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableIterInit) : {
    auto *iterator = frame->LocalAt<sql::JoinHashTableIterator *>(READ_LOCAL_ID());
    auto *join_hash_table = frame->LocalAt<sql::JoinHashTable *>(READ_LOCAL_ID());
    auto hash = frame->LocalAt<hash_t>(READ_LOCAL_ID());
    OpJoinHashTableIterInit(iterator, join_hash_table, hash);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableIterHasNext) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *iterator = frame->LocalAt<sql::JoinHashTableIterator *>(READ_LOCAL_ID());
    auto cmp_func_id = READ_FUNC_ID();
    auto cmp_fn = reinterpret_cast<sql::JoinHashTableIterator::KeyEq>(module_->GetRawFunctionImpl(cmp_func_id));
    auto *opaque_ctx = frame->LocalAt<void *>(READ_LOCAL_ID());
    auto *probe_tuple = frame->LocalAt<void *>(READ_LOCAL_ID());
    OpJoinHashTableIterHasNext(has_more, iterator, cmp_fn, opaque_ctx, probe_tuple);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableIterGetRow) : {
    auto *result = frame->LocalAt<const byte **>(READ_LOCAL_ID());
    auto *iterator = frame->LocalAt<sql::JoinHashTableIterator *>(READ_LOCAL_ID());
    OpJoinHashTableIterGetRow(result, iterator);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableIterClose) : {
    auto *iterator = frame->LocalAt<sql::JoinHashTableIterator *>(READ_LOCAL_ID());
    OpJoinHashTableIterClose(iterator);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableBuild) : {
    auto *join_hash_table = frame->LocalAt<sql::JoinHashTable *>(READ_LOCAL_ID());
    OpJoinHashTableBuild(join_hash_table);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableBuildParallel) : {
    auto *join_hash_table = frame->LocalAt<sql::JoinHashTable *>(READ_LOCAL_ID());
    auto *thread_state_container = frame->LocalAt<sql::ThreadStateContainer *>(READ_LOCAL_ID());
    auto jht_offset = frame->LocalAt<u32>(READ_LOCAL_ID());
    OpJoinHashTableBuildParallel(join_hash_table, thread_state_container, jht_offset);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableFree) : {
    auto *join_hash_table = frame->LocalAt<sql::JoinHashTable *>(READ_LOCAL_ID());
    OpJoinHashTableFree(join_hash_table);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Sorting
  // -------------------------------------------------------

  OP(SorterInit) : {
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    auto *memory = frame->LocalAt<tpl::sql::MemoryPool *>(READ_LOCAL_ID());
    auto cmp_func_id = READ_FUNC_ID();
    auto tuple_size = frame->LocalAt<u32>(READ_LOCAL_ID());

    auto cmp_fn = reinterpret_cast<sql::Sorter::ComparisonFunction>(module_->GetRawFunctionImpl(cmp_func_id));
    OpSorterInit(sorter, memory, cmp_fn, tuple_size);
    DISPATCH_NEXT();
  }

  OP(SorterAllocTuple) : {
    auto *result = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    OpSorterAllocTuple(result, sorter);
    DISPATCH_NEXT();
  }

  OP(SorterAllocTupleTopK) : {
    auto *result = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    auto top_k = frame->LocalAt<u64>(READ_LOCAL_ID());
    OpSorterAllocTupleTopK(result, sorter, top_k);
    DISPATCH_NEXT();
  }

  OP(SorterAllocTupleTopKFinish) : {
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    auto top_k = frame->LocalAt<u64>(READ_LOCAL_ID());
    OpSorterAllocTupleTopKFinish(sorter, top_k);
    DISPATCH_NEXT();
  }

  OP(SorterSort) : {
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    OpSorterSort(sorter);
    DISPATCH_NEXT();
  }

  OP(SorterSortParallel) : {
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    auto *thread_state_container = frame->LocalAt<sql::ThreadStateContainer *>(READ_LOCAL_ID());
    auto sorter_offset = frame->LocalAt<u32>(READ_LOCAL_ID());
    OpSorterSortParallel(sorter, thread_state_container, sorter_offset);
    DISPATCH_NEXT();
  }

  OP(SorterSortTopKParallel) : {
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    auto *thread_state_container = frame->LocalAt<sql::ThreadStateContainer *>(READ_LOCAL_ID());
    auto sorter_offset = frame->LocalAt<u32>(READ_LOCAL_ID());
    auto top_k = frame->LocalAt<u64>(READ_LOCAL_ID());
    OpSorterSortTopKParallel(sorter, thread_state_container, sorter_offset, top_k);
    DISPATCH_NEXT();
  }

  OP(SorterFree) : {
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    OpSorterFree(sorter);
    DISPATCH_NEXT();
  }

  OP(SorterIteratorInit) : {
    auto *iter = frame->LocalAt<sql::SorterIterator *>(READ_LOCAL_ID());
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    OpSorterIteratorInit(iter, sorter);
    DISPATCH_NEXT();
  }

  OP(SorterIteratorHasNext) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::SorterIterator *>(READ_LOCAL_ID());
    OpSorterIteratorHasNext(has_more, iter);
    DISPATCH_NEXT();
  }

  OP(SorterIteratorNext) : {
    auto *iter = frame->LocalAt<sql::SorterIterator *>(READ_LOCAL_ID());
    OpSorterIteratorNext(iter);
    DISPATCH_NEXT();
  }

  OP(SorterIteratorGetRow) : {
    const auto **row = frame->LocalAt<const byte **>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::SorterIterator *>(READ_LOCAL_ID());
    OpSorterIteratorGetRow(row, iter);
    DISPATCH_NEXT();
  }

  OP(SorterIteratorFree) : {
    auto *iter = frame->LocalAt<sql::SorterIterator *>(READ_LOCAL_ID());
    OpSorterIteratorFree(iter);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Real-value functions
  // -------------------------------------------------------

  // -------------------------------------------------------
  // Trig functions
  // -------------------------------------------------------

  OP(Acos) : {
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    OpAcos(result, input);
    DISPATCH_NEXT();
  }

  OP(Asin) : {
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    OpAsin(result, input);
    DISPATCH_NEXT();
  }

  OP(Atan) : {
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    OpAtan(result, input);
    DISPATCH_NEXT();
  }

  OP(Atan2) : {
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto *arg_1 = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto *arg_2 = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    OpAtan2(result, arg_1, arg_2);
    DISPATCH_NEXT();
  }

  OP(Cos) : {
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    OpCos(result, input);
    DISPATCH_NEXT();
  }

  OP(Cot) : {
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    OpCot(result, input);
    DISPATCH_NEXT();
  }

  OP(Sin) : {
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    OpSin(result, input);
    DISPATCH_NEXT();
  }

  OP(Tan) : {
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    OpTan(result, input);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Output Calls
  // -------------------------------------------------------
  OP(OutputAlloc) : {
    auto exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *result = frame->LocalAt<byte **>(READ_LOCAL_ID());
    OpOutputAlloc(exec_ctx, result);
    DISPATCH_NEXT();
  }

  OP(OutputAdvance) : {
    auto exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    OpOutputAdvance(exec_ctx);
    DISPATCH_NEXT();
  }

  OP(OutputSetNull) : {
    auto exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto idx = frame->LocalAt<u32>(READ_LOCAL_ID());
    OpOutputSetNull(exec_ctx, idx);
    DISPATCH_NEXT();
  }

  OP(OutputFinalize) : {
    auto exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    OpOutputFinalize(exec_ctx);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Inserts
  // -------------------------------------------------------
  OP(Insert) : {
    auto db_id = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    auto ns_id = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    auto table_id = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    auto values = frame->LocalAt<byte *>(READ_LOCAL_ID());
    auto exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    OpInsert(exec_ctx, db_id, ns_id, table_id, values);
    EXECUTION_LOG_TRACE("Inserted into table ", table_id);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Index Iterator
  // -------------------------------------------------------
  OP(IndexIteratorInit) : {
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    auto index_oid = READ_UIMM4();
    auto exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    OpIndexIteratorInit(iter, index_oid, exec_ctx);
    DISPATCH_NEXT();
  }

  OP(IndexIteratorScanKey) : {
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    auto *key = frame->LocalAt<byte *>(READ_LOCAL_ID());
    OpIndexIteratorScanKey(iter, key);
    DISPATCH_NEXT();
  }

  OP(IndexIteratorFree) : {
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    OpIndexIteratorFree(iter);
    DISPATCH_NEXT();
  }

  OP(IndexIteratorHasNext) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    OpIndexIteratorHasNext(has_more, iter);
    DISPATCH_NEXT();
  }

  OP(IndexIteratorAdvance) : {
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    OpIndexIteratorAdvance(iter);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // IndexIterator element access
  // -------------------------------------------------------

#define GEN_INDEX_ITERATOR_ACCESS(type_str, type)                       \
  OP(IndexIteratorGet##type_str) : {                                    \
    auto *result = frame->LocalAt<type *>(READ_LOCAL_ID());             \
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID()); \
    auto col_idx = READ_UIMM4();                                        \
    OpIndexIteratorGet##type_str(result, iter, col_idx);                \
    DISPATCH_NEXT();                                                    \
  }                                                                     \
  OP(IndexIteratorGet##type_str##Null) : {                              \
    auto *result = frame->LocalAt<type *>(READ_LOCAL_ID());             \
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID()); \
    auto col_idx = READ_UIMM4();                                        \
    OpIndexIteratorGet##type_str##Null(result, iter, col_idx);          \
    DISPATCH_NEXT();                                                    \
  }
  GEN_INDEX_ITERATOR_ACCESS(SmallInt, sql::Integer)
  GEN_INDEX_ITERATOR_ACCESS(Integer, sql::Integer)
  GEN_INDEX_ITERATOR_ACCESS(BigInt, sql::Integer)
  GEN_INDEX_ITERATOR_ACCESS(Decimal, sql::Decimal)
#undef GEN_INDEX_ITERATOR_ACCESS

  // Impossible
  UNREACHABLE("Impossible to reach end of interpreter loop. Bad code!");
}  // NOLINT (function is too long)

const u8 *VM::ExecuteCall(const u8 *ip, VM::Frame *caller) {
  // Read the function ID and the argument count to the function first
  const auto func_id = READ_FUNC_ID();
  const auto num_params = READ_UIMM2();

  // Lookup the function
  const FunctionInfo *func_info = module_->GetFuncInfoById(func_id);
  TPL_ASSERT(func_info != nullptr, "Function doesn't exist in module!");
  const std::size_t frame_size = func_info->frame_size();

  // Get some space for the function's frame
  bool used_heap = false;
  u8 *raw_frame = nullptr;
  if (frame_size > kMaxStackAllocSize) {
    used_heap = true;
    raw_frame = static_cast<u8 *>(std::aligned_alloc(alignof(u64), frame_size));
  } else if (frame_size > kSoftMaxStackAllocSize) {
    // TODO(pmenon): Check stack before allocation
    raw_frame = static_cast<u8 *>(alloca(frame_size));
  } else {
    raw_frame = static_cast<u8 *>(alloca(frame_size));
  }

  // Set up the arguments to the function
  for (u32 i = 0; i < num_params; i++) {
    const LocalInfo &param_info = func_info->locals()[i];
    const void *param = caller->LocalAt<void *>(READ_LOCAL_ID());
    std::memcpy(raw_frame + param_info.offset(), &param, param_info.size());
  }

  EXECUTION_LOG_DEBUG("Executing function '{}'", func_info->name());

  // Let's go
  const u8 *bytecode = module_->bytecode_module()->GetBytecodeForFunction(*func_info);
  TPL_ASSERT(bytecode != nullptr, "Bytecode cannot be null");
  VM::Frame callee(raw_frame, func_info->frame_size());
  Interpret(bytecode, &callee);

  if (used_heap) {
    std::free(raw_frame);
  }

  return ip;
}

}  // namespace tpl::vm
