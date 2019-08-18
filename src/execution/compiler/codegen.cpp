#include "execution/compiler/codegen.h"

#include <string>
#include <utility>
#include "type/transient_value_peeker.h"

namespace terrier::execution::compiler {

CodeGen::CodeGen(Query* query)
: query_(query)
, state_struct_{Context()->GetIdentifier("State")}
, state_var_{Context()->GetIdentifier("state")}
, exec_ctx_var_(Context()->GetIdentifier("execCtx"))
, main_fn_(Context()->GetIdentifier("main"))
, setup_fn_(Context()->GetIdentifier("setupFn"))
, teardown_fn_(Context()->GetIdentifier("teardownFn"))
{}

ast::BlockStmt *CodeGen::EmptyBlock() {
  util::RegionVector<ast::Stmt *> stmts(Region());
  return Factory()->NewBlockStmt(DUMMY_POS, DUMMY_POS, std::move(stmts));
}

ast::Decl* CodeGen::MakeStruct(ast::Identifier struct_name, util::RegionVector<ast::FieldDecl*> && fields) {
  ast::StructTypeRepr * struct_type = Factory()->NewStructType(DUMMY_POS, std::move(fields));
  return Factory()->NewStructDecl(DUMMY_POS, struct_name, struct_type);
}

util::RegionVector<ast::FieldDecl *> CodeGen::MainParams() {
  // Exec Context Parameter
  ast::Expr *exec_ctx_type = PointerType(BuiltinType(ast::BuiltinType::Kind::ExecutionContext));
  ast::FieldDecl * param = MakeField(exec_ctx_var_, exec_ctx_type);
  return {{param}, Region()};
}

util::RegionVector<ast::FieldDecl *> CodeGen::ExecParams() {
  // State parameter
  ast::Expr * state_type = PointerType(GetStateType());
  ast::FieldDecl * state_param = MakeField(state_var_, state_type);

  // Exec Context Parameter
  ast::Expr *exec_ctx_type = PointerType(BuiltinType(ast::BuiltinType::Kind::ExecutionContext));
  ast::FieldDecl * exec_ctx_param = MakeField(exec_ctx_var_, exec_ctx_type);

  // Function parameter
  return {{state_param, exec_ctx_param}, Region()};
}

ast::Stmt* CodeGen::ExecCall(ast::Identifier fn_name) {
  ast::Expr* func = MakeExpr(fn_name);
  ast::Expr* state_arg = PointerTo(state_var_);
  ast::Expr* exec_ctx_arg = MakeExpr(exec_ctx_var_);
  util::RegionVector<ast::Expr*> params{{state_arg, exec_ctx_arg}, Region()};
  return MakeStmt(Factory()->NewCallExpr(func, std::move(params)));
}

ast::Expr* CodeGen::GetStateMemberPtr(ast::Identifier ident) {
  return PointerTo(MemberExpr(state_var_, ident));
}

ast::Identifier CodeGen::NewIdentifier() {
  return NewIdentifier("id");
}

ast::Identifier CodeGen::NewIdentifier(const std::string &prefix) {
  // Use the custom allocator because the id will outlive the std::string.
  std::string id = prefix + std::to_string(id_count_++);
  auto *id_str = Region()->AllocateArray<char>(id.size() + 1);
  std::memcpy(id_str, id.c_str(), id.size() + 1);
  return Context()->GetIdentifier(id_str);

}

ast::Expr* CodeGen::OneArgCall(ast::Builtin builtin, ast::Expr* arg) {
  ast::Expr * fun = BuiltinFunction(builtin);
  util::RegionVector<ast::Expr*> args{{arg}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::OneArgCall(ast::Builtin builtin, ast::Identifier ident, bool take_ptr) {
  ast::Expr * arg;
  if (take_ptr) {
    arg = PointerTo(ident);
  } else {
    arg = MakeExpr(ident);
  }
  return OneArgCall(builtin, arg);
}



ast::Expr* CodeGen::OneArgStateCall(execution::ast::Builtin builtin, execution::ast::Identifier ident) {
  ast::Expr * arg = GetStateMemberPtr(ident);
  return OneArgCall(builtin, arg);
}


ast::Expr* CodeGen::PtrCast(ast::Identifier base, ast::Expr* arg) {
  return PtrCast(MakeExpr(base), arg);
}


ast::Expr* CodeGen::PtrCast(ast::Expr* base, ast::Expr* arg) {
  ast::Expr* fun = BuiltinFunction(ast::Builtin::PtrCast);
  ast::Expr* ptr = Factory()->NewUnaryOpExpr(DUMMY_POS, parsing::Token::Type::STAR, base);
  util::RegionVector<ast::Expr*> cast_args{{ptr, arg}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(cast_args));
}

ast::Expr* CodeGen::OutputAlloc() {
  return OneArgCall(ast::Builtin::OutputAlloc, exec_ctx_var_, false);
}

ast::Expr* CodeGen::OutputAdvance() {
  return OneArgCall(ast::Builtin::OutputAdvance, exec_ctx_var_, false);
}

ast::Expr* CodeGen::OutputFinalize() {
  return OneArgCall(ast::Builtin::OutputFinalize, exec_ctx_var_, false);
}

ast::Expr* CodeGen::TableIterInit(ast::Identifier tvi, uint32_t table_oid) {
  ast::Expr * fun = BuiltinFunction(ast::Builtin::TableIterInit);
  ast::Expr * tvi_ptr = PointerTo(tvi);
  ast::Expr * table_oid_expr = IntLiteral(static_cast<i32>(table_oid));
  ast::Expr * exec_ctx_expr = MakeExpr(exec_ctx_var_);

  util::RegionVector<ast::Expr*> args{{tvi_ptr, table_oid_expr, exec_ctx_expr}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::TableIterAdvance(ast::Identifier tvi) {
  return OneArgCall(ast::Builtin::TableIterAdvance, tvi, true);
}

ast::Expr* CodeGen::TableIterGetPCI(execution::ast::Identifier tvi) {
  return OneArgCall(ast::Builtin::TableIterGetPCI, tvi, true);
}


ast::Expr* CodeGen::TableIterClose(execution::ast::Identifier tvi) {
  return OneArgCall(ast::Builtin::TableIterClose, tvi, true);
}

ast::Expr* CodeGen::PCIHasNext(execution::ast::Identifier pci) {
  return OneArgCall(ast::Builtin::PCIHasNext, pci, false);
}


ast::Expr* CodeGen::PCIAdvance(execution::ast::Identifier pci) {
  return OneArgCall(ast::Builtin::PCIAdvance, pci, false);
}


// TODO(Amadou): Depending on whether the column is nullable or not, generate @pciGetTypeNull vs @pciGetType
// Right now, I am always generating null because it's the safest option.
// Once the catalog accessor is in, lookup the column type to decide what to call.
ast::Expr* CodeGen::PCIGet(execution::ast::Identifier pci, terrier::type::TypeId type, uint32_t idx) {
  ast::Builtin builtin;
  switch (type) {
    case terrier::type::TypeId::INTEGER:
      builtin = ast::Builtin::PCIGetIntNull;
      break;
    case terrier::type::TypeId::SMALLINT:
      builtin = ast::Builtin::PCIGetSmallIntNull;
      break;
    case terrier::type::TypeId::BIGINT:
      builtin = ast::Builtin::PCIGetBigIntNull;
      break;
    case terrier::type::TypeId::DECIMAL:
      builtin = ast::Builtin ::PCIGetDoubleNull;
      break;
    case terrier::type::TypeId::DATE:
      builtin = ast::Builtin ::PCIGetDateNull;
      break;
    case terrier::type::TypeId::VARCHAR:
      builtin = ast::Builtin ::PCIGetVarlenNull;
      break;
    default:
      // TODO: Support other types.
      UNREACHABLE("Cannot @pciGetType unsupported type");
  }
  ast::Expr * fun = BuiltinFunction(builtin);
  ast::Expr * pci_expr = MakeExpr(pci);
  ast::Expr * idx_expr = Factory()->NewIntLiteral(DUMMY_POS, idx);
  util::RegionVector<ast::Expr*> args{{pci_expr, idx_expr}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr *CodeGen::PCIFilter(ast::Identifier pci, terrier::parser::ExpressionType comp_type, uint32_t col_idx, terrier::type::TypeId col_type, ast::Expr* filter_val) {
  // Call @FilterComp(pci, col_idx, col_type, filter_val)
  ast::Builtin builtin;
  switch (comp_type) {
    case terrier::parser::ExpressionType::COMPARE_EQUAL:
      builtin = ast::Builtin::FilterEq;
      break;
    case terrier::parser::ExpressionType::COMPARE_NOT_EQUAL:
      builtin = ast::Builtin::FilterNe;
      break;
    case terrier::parser::ExpressionType::COMPARE_LESS_THAN:
      builtin = ast::Builtin::FilterLt;
      break;
    case terrier::parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
      builtin = ast::Builtin::FilterLe;
      break;
    case terrier::parser::ExpressionType::COMPARE_GREATER_THAN:
      builtin = ast::Builtin::FilterGt;
      break;
    case terrier::parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
      builtin = ast::Builtin::FilterGe;
      break;
    default:
      UNREACHABLE("Impossible filter comparison!");
  }
  ast::Expr * fun = BuiltinFunction(builtin);
  ast::Expr * pci_expr = MakeExpr(pci);
  ast::Expr * idx_expr = IntLiteral(col_idx);
  ast::Expr * type_expr = IntLiteral(static_cast<i8>(col_type));
  util::RegionVector<ast::Expr*> args{{pci_expr, idx_expr, type_expr, filter_val}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}


ast::Expr* CodeGen::Hash(execution::util::RegionVector<execution::ast::Expr *> &&args) {
  ast::Expr * fun = BuiltinFunction(ast::Builtin::Hash);
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::ExecCtxGetMem() {
  return OneArgCall(ast::Builtin::ExecutionContextGetMemoryPool, exec_ctx_var_, false);
}

ast::Expr* CodeGen::SizeOf(ast::Identifier type_name) {
  return OneArgCall(ast::Builtin::SizeOf, type_name, false);
}

ast::Expr* CodeGen::InitCall(ast::Builtin builtin, execution::ast::Identifier object, execution::ast::Identifier struct_type) {
  // Init Function
  ast::Expr * fun = BuiltinFunction(builtin);
  // The object to initialize
  ast::Expr* obj_ptr = GetStateMemberPtr(object);
  // Then get @execCtxGetMem(execCtx)
  ast::Expr* get_mem_call = ExecCtxGetMem();
  // Then get @sizeof(Struct)
  ast::Expr* sizeof_call = SizeOf(struct_type);
  // Finally make the init call
  util::RegionVector<ast::Expr*> args{{obj_ptr, get_mem_call, sizeof_call}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::AggHashTableInit(ast::Identifier ht, ast::Identifier payload_struct) {
  // @aggHTInit(&state.agg_hash_table, @execCtxGetMem(execCtx), @sizeOf(AggPayload))
  return InitCall(ast::Builtin::AggHashTableInit, ht, payload_struct);
}

ast::Expr* CodeGen::AggHashTableLookup(ast::Identifier ht, ast::Identifier hash_val, ast::Identifier key_check, ast::Identifier values) {
  // @aggHTLookup((&state.agg_ht, agg_hash_val, keyCheck, &agg_values)
  ast::Expr * fun = BuiltinFunction(ast::Builtin::AggHashTableLookup);
  ast::Expr* agg_ht = GetStateMemberPtr(ht);
  ast::Expr* hash_val_expr = MakeExpr(hash_val);
  ast::Expr* key_check_expr = MakeExpr(key_check);
  ast::Expr* agg_values_ptr = PointerTo(values);
  util::RegionVector<ast::Expr*> args{{agg_ht, hash_val_expr, key_check_expr, agg_values_ptr}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::AggHashTableInsert(ast::Identifier ht, ast::Identifier hash_val) {
  // @aggHTInsert(&state.ht, hash_val)
  ast::Expr * fun = BuiltinFunction(ast::Builtin::AggHashTableInsert);
  ast::Expr* agg_ht = GetStateMemberPtr(ht);
  ast::Expr* hash_val_expr = MakeExpr(hash_val);
  util::RegionVector<ast::Expr*> args{{agg_ht, hash_val_expr}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::AggHashTableFree(ast::Identifier ht) {
  // @aggHTFree(&state.agg_ht)
  return OneArgStateCall(ast::Builtin::AggHashTableFree, ht);
}

ast::Expr* CodeGen::AggHashTableIterInit(ast::Identifier iter, ast::Identifier ht) {
  // @aggHTIterInit(&agg_iter, &state.agg_table)
  ast::Expr * fun = BuiltinFunction(ast::Builtin::AggHashTableIterInit);
  ast::Expr* iter_ptr = PointerTo(iter);
  ast::Expr* agg_ht = GetStateMemberPtr(ht);
  util::RegionVector<ast::Expr*> args{{iter_ptr, agg_ht}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::AggHashTableIterHasNext(ast::Identifier iter) {
  // @aggHTIterHasNext(&agg_iter)
  return OneArgCall(ast::Builtin::AggHashTableIterHasNext, iter, true);
}

ast::Expr* CodeGen::AggHashTableIterNext(ast::Identifier iter) {
  // @aggHTIterNext(&agg_iter)
  return OneArgCall(ast::Builtin::AggHashTableIterNext, iter, true);
}

ast::Expr* CodeGen::AggHashTableIterGetRow(ast::Identifier iter) {
  // @aggHTIterGetRow(&agg_iter)
  return OneArgCall(ast::Builtin::AggHashTableIterGetRow, iter, true);
}

ast::Expr* CodeGen::AggHashTableIterClose(ast::Identifier iter) {
  // @aggHTIterClose(&agg_iter)
  return OneArgCall(ast::Builtin::AggHashTableIterClose, iter, true);
}

ast::Expr* CodeGen::AggInit(ast::Expr* agg) {
  // @aggInit(agg)
  return OneArgCall(ast::Builtin::AggInit, agg);
}

ast::Expr* CodeGen::AggAdvance(ast::Expr* agg, ast::Expr* val) {
  // @aggAdvance(agg, val)
  ast::Expr * fun = BuiltinFunction(ast::Builtin::AggAdvance);
  util::RegionVector<ast::Expr*> args{{agg, val}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::AggResult(ast::Expr* agg) {
  // @aggResult(agg)
  return OneArgCall(ast::Builtin::AggResult, agg);
}

ast::Expr* CodeGen::JoinHashTableInit(ast::Identifier ht, ast::Identifier build_struct) {
  // @joinHTInit(&state.join_hash_table, @execCtxGetMem(execCtx), @sizeOf(BuildRow))
  return InitCall(ast::Builtin::JoinHashTableInit, ht, build_struct);
}

ast::Expr* CodeGen::JoinHashTableInsert(execution::ast::Identifier ht, execution::ast::Identifier hash_val) {
  // @joinHTInsert(&state.join_table, hash_val)
  ast::Expr * fun = BuiltinFunction(ast::Builtin::JoinHashTableInsert);
  ast::Expr* join_ht = GetStateMemberPtr(ht);
  ast::Expr* hash_val_expr = MakeExpr(hash_val);
  util::RegionVector<ast::Expr*> args{{join_ht, hash_val_expr}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::JoinHashTableIterInit(execution::ast::Identifier iter,
                                          execution::ast::Identifier ht,
                                          execution::ast::Identifier hash_val) {
  // Call @joinHTIterInit(&iter, &state.ht, hash_val)
  ast::Expr * fun = BuiltinFunction(ast::Builtin::JoinHashTableIterInit);
  ast::Expr * iter_ptr = PointerTo(iter);
  ast::Expr* join_ht = GetStateMemberPtr(ht);
  ast::Expr* hash_val_expr = MakeExpr(hash_val);
  util::RegionVector<ast::Expr*> args{{iter_ptr, join_ht, hash_val_expr}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::JoinHashTableIterHasNext(execution::ast::Identifier iter,
                                             execution::ast::Identifier key_check,
                                             execution::ast::Identifier probe_row,
                                             bool is_probe_ptr) {
  // @joinHTIterHasNext(&iter, key_check, execCtx, &probe_row)
  ast::Expr * fun = BuiltinFunction(ast::Builtin::JoinHashTableIterHasNext);
  ast::Expr * iter_ptr = PointerTo(iter);
  ast::Expr * key_check_fn = MakeExpr(key_check);
  ast::Expr * exec_ctx_arg = MakeExpr(exec_ctx_var_);
  ast::Expr* probe_row_ptr;
  if (is_probe_ptr) {
    probe_row_ptr = MakeExpr(probe_row);
  } else {
    probe_row_ptr = PointerTo(probe_row);
  }
  util::RegionVector<ast::Expr*> args{{iter_ptr, key_check_fn, exec_ctx_arg, probe_row_ptr}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::JoinHashTableIterGetRow(ast::Identifier iter) {
  // @joinHTIterGetRow(&iter)
  return OneArgCall(ast::Builtin::JoinHashTableIterGetRow, iter, true);
}

ast::Expr* CodeGen::JoinHashTableIterClose(execution::ast::Identifier iter) {
  // @joinHTIterClose(&iter)
  return OneArgCall(ast::Builtin::JoinHashTableIterClose, iter, true);
}

ast::Expr* CodeGen::JoinHashTableBuild(execution::ast::Identifier ht) {
  // @joinHTIterBuild&state.ht)
  return OneArgStateCall(ast::Builtin::JoinHashTableBuild, ht);
}

ast::Expr* CodeGen::JoinHashTableFree(execution::ast::Identifier ht) {
  // @joinHTIterBuild&state.ht)
  return OneArgStateCall(ast::Builtin::JoinHashTableFree, ht);
}

ast::Expr* CodeGen::SorterInit(execution::ast::Identifier sorter,
                               execution::ast::Identifier comp_fn,
                               execution::ast::Identifier sorter_struct) {
  // @sorterInit(&state.sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterStruct))
  // Init Function
  ast::Expr * fun = BuiltinFunction(ast::Builtin::SorterInit);
  // Get the sorter
  ast::Expr* sorter_ptr = GetStateMemberPtr(sorter);
  // Then get @execCtxGetMem(execCtx)
  ast::Expr* get_mem_call = ExecCtxGetMem();
  // The comparison function
  ast::Expr* comp_fn_expr = MakeExpr(comp_fn);
  // Then get @sizeof(sorter_truct)
  ast::Expr* sizeof_call = SizeOf(sorter_struct);
  // Finally make the init call
  util::RegionVector<ast::Expr*> args{{sorter_ptr, get_mem_call, comp_fn_expr, sizeof_call}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::SorterInsert(execution::ast::Identifier sorter) {
  // @sorterInsert(&state.sorter)
  return OneArgStateCall(ast::Builtin::SorterInsert, sorter);
}

ast::Expr* CodeGen::SorterSort(execution::ast::Identifier sorter) {
  // @sorterSort(&state.sorter)
  return OneArgStateCall(ast::Builtin::SorterSort, sorter);
}

ast::Expr* CodeGen::SorterFree(execution::ast::Identifier sorter) {
  // @sorterFree(&state.sorter)
  return OneArgStateCall(ast::Builtin::SorterFree, sorter);
}


ast::Expr* CodeGen::SorterIterInit(execution::ast::Identifier iter, execution::ast::Identifier sorter) {
  // @sorterIterInit(&iter, &state.sorter)
  ast::Expr * fun = BuiltinFunction(ast::Builtin::SorterIterInit);
  ast::Expr* iter_ptr = PointerTo(iter);
  ast::Expr* sorter_ptr = GetStateMemberPtr(sorter);
  util::RegionVector<ast::Expr*> args{{iter_ptr, sorter_ptr}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::SorterIterHasNext(execution::ast::Identifier iter) {
  // @sorterIterHasNext(&iter)
  return OneArgCall(ast::Builtin::SorterIterHasNext, iter, true);
}

ast::Expr* CodeGen::SorterIterNext(execution::ast::Identifier iter) {
  // @sorterIterNext(&iter)
  return OneArgCall(ast::Builtin::SorterIterNext, iter, true);
}

ast::Expr* CodeGen::SorterIterGetRow(execution::ast::Identifier iter) {
  // @sorterIterGetRow(&iter)
  return OneArgCall(ast::Builtin::SorterIterGetRow, iter, true);
}

ast::Expr* CodeGen::SorterIterClose(execution::ast::Identifier iter) {
  // @sorterIterClose(&iter)
  return OneArgCall(ast::Builtin::SorterIterClose, iter, true);
}

ast::Expr* CodeGen::IndexIteratorInit(execution::ast::Identifier iter, uint32_t table_oid, uint32_t index_oid) {
  // @indexIteratorInit(&iter, table_oid, index_oid, execCtx)
  ast::Expr * fun = BuiltinFunction(ast::Builtin::IndexIteratorInit);
  ast::Expr* iter_ptr = PointerTo(iter);
  ast::Expr* table_oid_expr = IntLiteral(static_cast<i32>(table_oid));
  ast::Expr* index_oid_expr = IntLiteral(static_cast<i32>(index_oid));
  ast::Expr* exec_ctx_expr = MakeExpr(exec_ctx_var_);
  util::RegionVector<ast::Expr*> args{{iter_ptr, table_oid_expr, index_oid_expr, exec_ctx_expr}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::IndexIteratorScanKey(execution::ast::Identifier iter, execution::ast::Identifier key) {
  // @indexIteratorScanKey(&iter, @ptrCast(*int8, &key))
  ast::Expr * fun = BuiltinFunction(ast::Builtin::IndexIteratorScanKey);
  ast::Expr* iter_ptr = PointerTo(iter);
  ast::Expr* cast_call = PtrCast(BuiltinType(ast::BuiltinType::Kind::Int8), PointerTo(key));
  util::RegionVector<ast::Expr*> args{{iter_ptr, cast_call}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::IndexIteratorAdvance(execution::ast::Identifier iter) {
  // @indexIteratorHasNext(&iter)
  return OneArgCall(ast::Builtin::IndexIteratorAdvance, iter, true);
}


ast::Expr* CodeGen::IndexIteratorFree(execution::ast::Identifier iter) {
  // @indexIteratorFree(&iter)
  return OneArgCall(ast::Builtin::IndexIteratorFree, iter, true);
}

// TODO(Amadou): Generator GetNull calls if the columns is nullable
ast::Expr* CodeGen::IndexIteratorGet(execution::ast::Identifier iter, terrier::type::TypeId type, uint32_t attr_idx) {
  // @indexIteratorHasGetType(&iter, attr_idx)
  ast::Builtin builtin;
  switch (type) {
    case terrier::type::TypeId::INTEGER:
      builtin = ast::Builtin::IndexIteratorGetInt;
      break;
    case terrier::type::TypeId::SMALLINT:
      builtin = ast::Builtin::IndexIteratorGetSmallInt;
      break;
    case terrier::type::TypeId::BIGINT:
      builtin = ast::Builtin::IndexIteratorGetBigInt;
      break;
    default:
      // TODO: Support other types.
      builtin = ast::Builtin::IndexIteratorGetInt;
  }
  ast::Expr * fun = BuiltinFunction(builtin);
  ast::Expr * iter_ptr = PointerTo(iter);
  ast::Expr * idx_expr = Factory()->NewIntLiteral(DUMMY_POS, attr_idx);
  util::RegionVector<ast::Expr*> args{{iter_ptr, idx_expr}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr *CodeGen::PeekValue(const terrier::type::TransientValue &transient_val) {
  switch (transient_val.Type()) {
    case terrier::type::TypeId::TINYINT: {
      auto val = terrier::type::TransientValuePeeker::PeekTinyInt(transient_val);
      return Factory()->NewIntLiteral(DUMMY_POS, val);
    }
    case terrier::type::TypeId::SMALLINT: {
      auto val = terrier::type::TransientValuePeeker::PeekSmallInt(transient_val);
      return Factory()->NewIntLiteral(DUMMY_POS, val);
    }
    case terrier::type::TypeId::INTEGER: {
      auto val = terrier::type::TransientValuePeeker::PeekInteger(transient_val);
      return Factory()->NewIntLiteral(DUMMY_POS, static_cast<i32>(val));
    }
    case terrier::type::TypeId::BIGINT: {
      // TODO(WAN): the factory's IntLiteral only goes to i32
      auto val = terrier::type::TransientValuePeeker::PeekBigInt(transient_val);
      return Factory()->NewIntLiteral(DUMMY_POS, static_cast<i32>(val));
    }
    case terrier::type::TypeId::BOOLEAN: {
      auto val = terrier::type::TransientValuePeeker::PeekBoolean(transient_val);
      return Factory()->NewBoolLiteral(DUMMY_POS, val);
    }
    case terrier::type::TypeId::DATE:
    case terrier::type::TypeId::TIMESTAMP:
    case terrier::type::TypeId::DECIMAL:
    case terrier::type::TypeId::VARCHAR:
    case terrier::type::TypeId::VARBINARY:
    default:
      // TODO(WAN): error out
      return nullptr;
  }
}

ast::Expr *CodeGen::TplType(terrier::type::TypeId type) {
  switch (type) {
    case terrier::type::TypeId::TINYINT:
    case terrier::type::TypeId::SMALLINT:
    case terrier::type::TypeId::INTEGER:
    case terrier::type::TypeId::BIGINT: {
      return BuiltinType(ast::BuiltinType::Kind::Integer);
    }
    case terrier::type::TypeId::BOOLEAN: {
      return BuiltinType(ast::BuiltinType::Kind::Boolean);
    }
    case terrier::type::TypeId::DATE:
      return BuiltinType(ast::BuiltinType::Kind::Date);
    case terrier::type::TypeId::DECIMAL:
      return BuiltinType(ast::BuiltinType::Kind::Real);
    case terrier::type::TypeId::VARCHAR:
      return BuiltinType(ast::BuiltinType::Kind::StringVal);
    case terrier::type::TypeId::VARBINARY:
    case terrier::type::TypeId::TIMESTAMP:
    default:
      UNREACHABLE("Cannot codegen unsupported type.");
  }
}

ast::Expr* CodeGen::BuiltinFunction(ast::Builtin builtin) {
  ast::Identifier fun = Context()->GetBuiltinFunction(builtin);
  return MakeExpr(fun);
}

ast::Expr* CodeGen::BuiltinType(execution::ast::BuiltinType::Kind kind) {
  ast::Identifier typ = Context()->GetBuiltinType(kind);
  return MakeExpr(typ);
}

ast::Expr* CodeGen::PointerType(ast::Expr* base_expr) {
  return Factory()->NewPointerType(DUMMY_POS, base_expr);
}

ast::Expr* CodeGen::PointerType(execution::ast::Identifier base_type) {
  ast::Expr* base_expr = MakeExpr(base_type);
  return PointerTo(base_expr);
}


#define AGGTYPE(AggName, terrier_type) \
 switch(terrier_type) { \
  case terrier::type::TypeId::TINYINT: \
  case terrier::type::TypeId::SMALLINT: \
  case terrier::type::TypeId::INTEGER: \
  case terrier::type::TypeId::BIGINT: \
    return BuiltinType(ast::BuiltinType::Integer##AggName);\
  case terrier::type::TypeId::DECIMAL: \
    return BuiltinType(ast::BuiltinType::Real##AggName);\
  default: \
    UNREACHABLE("Unsupported aggregate type");\
  }

ast::Expr* CodeGen::AggregateType(terrier::parser::ExpressionType agg_type, terrier::type::TypeId ret_type) {
  switch (agg_type) {
    case terrier::parser::ExpressionType::AGGREGATE_COUNT:
      return BuiltinType(ast::BuiltinType::Kind::CountAggregate);
    case terrier::parser::ExpressionType::AGGREGATE_COUNT_STAR:
      return BuiltinType(ast::BuiltinType::Kind::CountStarAggregate);
    case terrier::parser::ExpressionType::AGGREGATE_AVG:
      AGGTYPE(AvgAggregate, ret_type);
    case terrier::parser::ExpressionType::AGGREGATE_MIN:
      AGGTYPE(MinAggregate, ret_type);
    case terrier::parser::ExpressionType::AGGREGATE_MAX:
      AGGTYPE(MaxAggregate, ret_type);
    case terrier::parser::ExpressionType::AGGREGATE_SUM:
      AGGTYPE(SumAggregate, ret_type);
    default:
      UNREACHABLE("AggregateType() should only be called with aggregates");
  }
}

ast::Stmt* CodeGen::DeclareVariable(ast::Identifier name, ast::Expr *typ, ast::Expr *init) {
  ast::Decl *decl = Factory()->NewVariableDecl(DUMMY_POS, name, typ, init);
  return Factory()->NewDeclStmt(decl);
}

ast::Stmt* CodeGen::MakeStmt(ast::Expr* expr) {
  return Factory()->NewExpressionStmt(expr);
}

ast::Stmt* CodeGen::Assign(execution::ast::Expr *lhs, execution::ast::Expr *rhs) {
  return Factory()->NewAssignmentStmt(DUMMY_POS, lhs, rhs);
}

ast::Expr* CodeGen::MakeExpr(ast::Identifier ident) {
  return Factory()->NewIdentifierExpr(DUMMY_POS, ident);
}

ast::Expr* CodeGen::PointerTo(ast::Identifier ident) {
  return PointerTo(MakeExpr(ident));
}

ast::Expr* CodeGen::PointerTo(ast::Expr* base) {
  return Factory()->NewUnaryOpExpr(DUMMY_POS, parsing::Token::Type::AMPERSAND, base);
}

ast::Expr* CodeGen::MemberExpr(ast::Identifier lhs, ast::Identifier rhs) {
  ast::Expr* object = MakeExpr(lhs);
  ast::Expr* member = MakeExpr(rhs);
  return Factory()->NewMemberExpr(DUMMY_POS, object, member);
}

ast::Expr* CodeGen::IntToSql(i64 num) {
  ast::Expr* int_lit = IntLiteral(num);
  return OneArgCall(ast::Builtin::IntToSql, int_lit);
}

ast::Expr* CodeGen::FloatToSql(f64 num) {
  ast::Expr* float_lit = FloatLiteral(num);
  return OneArgCall(ast::Builtin::FloatToSql, float_lit);
}

ast::Expr* CodeGen::DateToSql(i16 year, u8 month, u8 day) {
  ast::Expr* fun = BuiltinFunction(ast::Builtin::DateToSql);
  ast::Expr* year_lit = IntLiteral(year);
  ast::Expr* month_lit = IntLiteral(month);
  ast::Expr* day_lit = IntLiteral(day);
  util::RegionVector<ast::Expr*> args{{year_lit, month_lit, day_lit}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::StringToSql(std::string_view str) {
  ast::Identifier str_ident = Context()->GetIdentifier(str.data());
  ast::Expr* str_lit = Factory()->NewStringLiteral(DUMMY_POS, str_ident);
  return OneArgCall(ast::Builtin::StringToSql, str_lit);
}

}  // namespace terrier::execution::compiler
