#pragma once

#include <memory>
#include <vector>
#include "catalog/catalog_defs.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/projected_columns_iterator.h"
#include "storage/storage_defs.h"

namespace terrier::execution::sql{

class EXPORT Inserter {
 public:
  explicit Inserter(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid);

  storage::ProjectedRow *GetTablePR();

  storage::ProjectedRow *GetIndexPR(catalog::index_oid_t index_oid);

  storage::TupleSlot TableInsert();

  bool IndexInsert();

 private:
  catalog::table_oid_t table_oid_;
  exec::ExecutionContext *exec_ctx_;
  std::vector<terrier::catalog::col_oid_t> col_oids_;
  common::ManagedPointer<terrier::storage::SqlTable> table_;
  common::ManagedPointer<storage::index::Index> index_;

  storage::TupleSlot table_tuple_slot_;
  storage::RedoRecord *table_redo_{nullptr};
  storage::RedoRecord *index_redo_{nullptr};
  storage::ProjectedRow *table_pr_{nullptr};
  storage::ProjectedRow *index_pr_{nullptr};

};

} // namespace terrier::execution::sql