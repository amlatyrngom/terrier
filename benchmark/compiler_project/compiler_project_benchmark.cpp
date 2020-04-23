#include <memory>
#include <random>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include <array>
#include <memory>
#include <vector>


#include "catalog/catalog_defs.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/util/timer.h"
#include "execution/exec/execution_context.h"
#include "execution/table_generator/table_generator.h"
#include "execution/tpl_test.h"
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "storage/garbage_collector.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/timestamp_manager.h"



namespace terrier::execution::sql {

class SqlBasedBench : public benchmark::Fixture {
 public:
  SqlBasedBench() = default;

  void SetUp(const benchmark::State &state) override {
    benchmark::Fixture::SetUp(state);
    // Initialize terrier objects

    db_main_ = terrier::DBMain::Builder().SetUseGC(true).SetUseGCThread(true).SetUseCatalog(true).Build();

    block_store_ = db_main_->GetStorageLayer()->GetBlockStore();
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();

    test_txn_ = txn_manager_->BeginTransaction();

    // Create catalog and test namespace
    test_db_oid_ = catalog_->CreateDatabase(common::ManagedPointer(test_txn_), "test_db", true);
    ASSERT_NE(test_db_oid_, catalog::INVALID_DATABASE_OID) << "Default database does not exist";
    accessor_ = catalog_->GetAccessor(common::ManagedPointer(test_txn_), test_db_oid_);
    test_ns_oid_ = accessor_->GetDefaultNamespace();
  }

  ~SqlBasedBench() override { txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr); }

  catalog::namespace_oid_t NSOid() { return test_ns_oid_; }

  common::ManagedPointer<storage::BlockStore> BlockStore() { return block_store_; }

  std::unique_ptr<exec::ExecutionContext> MakeExecCtx(exec::OutputCallback &&callback = nullptr,
                                                      const planner::OutputSchema *schema = nullptr) {
    return std::make_unique<exec::ExecutionContext>(test_db_oid_, common::ManagedPointer(test_txn_), callback, schema,
                                                    common::ManagedPointer(accessor_));
  }

  void GenerateTestTables(exec::ExecutionContext *exec_ctx) {
    sql::TableGenerator table_generator{exec_ctx, block_store_, test_ns_oid_};
    table_generator.GenerateTestTables(false);
  }

  parser::ConstantValueExpression DummyCVE() {
    return parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(0));
  }

  std::unique_ptr<terrier::catalog::CatalogAccessor> MakeAccessor() {
    return catalog_->GetAccessor(common::ManagedPointer(test_txn_), test_db_oid_);
  }

 private:
  std::unique_ptr<DBMain> db_main_;
  common::ManagedPointer<storage::BlockStore> block_store_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  catalog::db_oid_t test_db_oid_{0};
  catalog::namespace_oid_t test_ns_oid_;
  transaction::TransactionContext *test_txn_;
  std::unique_ptr<catalog::CatalogAccessor> accessor_;
};


class CompilerProjectBenchmark : public SqlBasedBench {
 public:
  void SetUp(const benchmark::State &state) override {
    // Create the test tables
    SqlBasedBench::SetUp(state);
    exec_ctx_ = MakeExecCtx();
    GenerateTestTables(exec_ctx_.get());
  }

 protected:
  /**
   * Execution context to use for the test
   */
  std::unique_ptr<exec::ExecutionContext> exec_ctx_;
};


constexpr int num_ops = 6;
struct Instructions {
  template <int NumOps>
  static int32_t Apply(int32_t val) {
    // Get a different column every time.
    if constexpr (NumOps == 0) {
      return val;
    } else {
      // The following is to prevent compiler optimizations. I have verified it for up to NumOps=10.
      if constexpr (NumOps % num_ops == 0) {
        return Instructions::Apply<NumOps-1>(val) * (val + (0x37370000ll + NumOps));
      } else if constexpr (NumOps % num_ops == 1) {
        return Instructions::Apply<NumOps-1>(val) * (val & (0x37370000ll + NumOps));
      } else if constexpr (NumOps % num_ops == 2) {
        return Instructions::Apply<NumOps-1>(val) * (val | (0x37370000ll + NumOps));
      } else if constexpr (NumOps % num_ops == 3) {
        return Instructions::Apply<NumOps-1>(val) * (val + (0x3837370000ll + NumOps));
      } else if constexpr (NumOps % num_ops == 4) {
        return Instructions::Apply<NumOps-1>(val) * (val & (0x3837370000ll + NumOps));
      } else if constexpr (NumOps % num_ops == 5) {
        return Instructions::Apply<NumOps-1>(val) * (val | (0x3837370000ll + NumOps));
      }
    }
  }
};


bool ComplexFilter(int32_t val, int32_t c) {
  return static_cast<uint32_t>(Instructions::Apply<10>(val)) % 10  > 5;
}

bool SimpleFilter(int32_t val, int32_t c) {
  return val < c;
}

BENCHMARK_DEFINE_F(CompilerProjectBenchmark, UnorderedFilterBench)(benchmark::State &state) {
  for (auto _ : state) {
    auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
    std::array<uint32_t, 1> col_oids{1};
    TableVectorIterator iter(exec_ctx_.get(), !table_oid, col_oids.data(), static_cast<uint32_t>(col_oids.size()));
    iter.Init();
    ProjectedColumnsIterator *pci = iter.GetProjectedColumnsIterator();
    int64_t count = 0;
    while (iter.Advance()) {
      for (; pci->HasNext(); pci->Advance()) {
        auto val = *pci->Get<int32_t, false>(2, nullptr);
        if (SimpleFilter(val, 5000)) {
          if (ComplexFilter(val, 0)) {
            count += val;
          }
        }
      }
      pci->Reset();
    }
    benchmark::DoNotOptimize(count);
  }
}

BENCHMARK_DEFINE_F(CompilerProjectBenchmark, OrderedFilterBench)(benchmark::State &state) {
  BENCHMARK_DEFINE_F(CompilerProjectBenchmark, UnorderedFilterBench)(benchmark::State &state) {
    for (auto _ : state) {
      auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
      std::array<uint32_t, 1> col_oids{1};
      TableVectorIterator iter(exec_ctx_.get(), !table_oid, col_oids.data(), static_cast<uint32_t>(col_oids.size()));
      iter.Init();
      ProjectedColumnsIterator *pci = iter.GetProjectedColumnsIterator();
      int64_t count = 0;
      while (iter.Advance()) {
        for (; pci->HasNext(); pci->Advance()) {
          auto val = *pci->Get<int32_t, false>(2, nullptr);
          if (ComplexFilter(val, 5000)) {
            if (SimpleFilter(val, 0)) {
              count += val;
            }
          }
        }
        pci->Reset();
      }
      benchmark::DoNotOptimize(count);
    }
  }

}


}