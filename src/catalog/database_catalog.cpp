#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/database_catalog.h"
#include "catalog/index_schema.h"
#include "catalog/postgres/pg_attribute.h"
#include "catalog/postgres/pg_class.h"
#include "catalog/postgres/pg_constraint.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/postgres/pg_type.h"
#include "catalog/schema.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"
#include "type/type_id.h"

namespace terrier::catalog {

namespace_oid_t DatabaseCatalog::CreateNamespace(transaction::TransactionContext *txn, const std::string &name) {
  namespace_oid_t ns_oid{next_oid_++};
  if (CreateNamespace(txn, name, ns_oid)) {
    return INVALID_NAMESPACE_OID;
  }
  return ns_oid;
}

bool DatabaseCatalog::CreateNamespace(transaction::TransactionContext *txn, const std::string &name,
                                      namespace_oid_t ns_oid) {
  // Step 1: Insert into table
  storage::VarlenEntry name_varlen = postgres::AttributeHelper::CreateVarlen(name);
  // Get & Fill Redo Record
  std::vector<col_oid_t> table_oids{NSPNAME_COL_OID, NSPOID_COL_OID};
  // NOLINTNEXTLINE (C++17 only)
  auto [pri, pm] = namespaces_->InitializerForProjectedRow(table_oids);
  auto *redo = txn->StageWrite(db_oid_, NAMESPACE_TABLE_OID, pri);
  auto *oid_entry = reinterpret_cast<namespace_oid_t *>(redo->Delta()->AccessForceNotNull(pm[NSPOID_COL_OID]));
  auto *name_entry = reinterpret_cast<storage::VarlenEntry *>(redo->Delta()->AccessForceNotNull(pm[NSPNAME_COL_OID]));
  *oid_entry = ns_oid;
  *name_entry = name_varlen;
  // Finally, insert into the table to get the tuple slot
  auto tupleslot = namespaces_->Insert(txn, redo);

  // Step 2: Insert into name index
  auto name_pri = namespaces_name_index_->GetProjectedRowInitializer();
  byte *buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());
  auto *pr = name_pri.InitializeRow(buffer);
  name_entry = reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0));
  *name_entry = name_varlen;

  if (!namespaces_name_index_->InsertUnique(txn, *pr, tupleslot)) {
    // There was a name conflict and we need to abort.  Free the buffer and
    // return INVALID_DATABASE_OID to indicate the database was not created.
    delete[] buffer;
    return false;
  }

  // Step 3: Insert into oid index
  auto oid_pri = namespaces_oid_index_->GetProjectedRowInitializer();
  // Reuse buffer since an u32 column is smaller than a varlen column
  pr = oid_pri.InitializeRow(buffer);
  oid_entry = reinterpret_cast<namespace_oid_t *>(pr->AccessForceNotNull(0));
  *oid_entry = ns_oid;
  const bool UNUSED_ATTRIBUTE result = namespaces_oid_index_->InsertUnique(txn, *pr, tupleslot);
  TERRIER_ASSERT(result, "Assigned namespace OID failed to be unique");

  delete[] buffer;
  return true;
}

bool DatabaseCatalog::DeleteNamespace(transaction::TransactionContext *txn, namespace_oid_t ns) {
  // Step 1: Read the oid index
  std::vector<col_oid_t> table_oids{NSPNAME_COL_OID};
  // NOLINTNEXTLINE (C++17 only)
  auto [table_pri, table_pm] = namespaces_->InitializerForProjectedRow(table_oids);
  auto name_pri = namespaces_name_index_->GetProjectedRowInitializer();
  auto oid_pri = namespaces_oid_index_->GetProjectedRowInitializer();
  // Buffer is large enough for all prs.
  byte *buffer = common::AllocationUtil::AllocateAligned(table_pri.ProjectedRowSize());
  auto pr = oid_pri.InitializeRow(buffer);
  // Scan index
  auto *oid_entry = reinterpret_cast<namespace_oid_t *>(pr->AccessForceNotNull(0));
  *oid_entry = ns;
  std::vector<storage::TupleSlot> index_results;
  namespaces_oid_index_->ScanKey(*txn, *pr, &index_results);
  if (index_results.empty()) {
    delete[] buffer;
    return false;
  }
  TERRIER_ASSERT(index_results.size() == 1, "Namespace OID not unique in index");

  // Step 2: Select from the table to get the name
  pr = table_pri.InitializeRow(buffer);
  if (!namespaces_->Select(txn, index_results[0], pr)) {
    // Nothing visible
    delete[] buffer;
    return false;
  }
  auto name_varlen = *reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(table_pm[NSPNAME_COL_OID]));

  // Step 3: Delete from table
  if (!namespaces_->Delete(txn, index_results[0])) {
    // Someone else has a write-lock
    delete[] buffer;
    return false;
  }

  // Step 4: Delete from oid index
  pr = oid_pri.InitializeRow(buffer);
  oid_entry = reinterpret_cast<namespace_oid_t *>(pr->AccessForceNotNull(table_pm[NSPOID_COL_OID]));
  *oid_entry = ns;
  namespaces_oid_index_->Delete(txn, *pr, index_results[0]);

  // Step 5: Delete from name index
  pr = name_pri.InitializeRow(buffer);
  auto name_entry = reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(table_pm[NSPNAME_COL_OID]));
  *name_entry = name_varlen;
  namespaces_name_index_->Delete(txn, *pr, index_results[0]);

  // Finish
  delete[] buffer;
  return true;
}

namespace_oid_t DatabaseCatalog::GetNamespaceOid(transaction::TransactionContext *txn, const std::string &name) {
  // Step 1: Read the name index
  std::vector<col_oid_t> table_oids{NSPNAME_COL_OID};
  // NOLINTNEXTLINE
  auto [table_pri, table_pm] = namespaces_->InitializerForProjectedRow(table_oids);
  auto name_pri = namespaces_name_index_->GetProjectedRowInitializer();
  // Buffer is large enough to hold all prs
  byte *buffer = common::AllocationUtil::AllocateAligned(table_pri.ProjectedRowSize());
  auto pr = name_pri.InitializeRow(buffer);
  // Scan the name index
  auto *name_entry = reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0));
  *name_entry = postgres::AttributeHelper::CreateVarlen(name);
  std::vector<storage::TupleSlot> index_results;
  namespaces_name_index_->ScanKey(*txn, *pr, &index_results);
  if (index_results.empty()) {
    delete[] buffer;
    return INVALID_NAMESPACE_OID;
  }
  TERRIER_ASSERT(index_results.size() == 1, "Namespace name not unique in index");

  // Step 2: Scan the table to get the oid
  pr = table_pri.InitializeRow(buffer);
  if (!namespaces_->Select(txn, index_results[0], pr)) {
    // Nothing visible
    delete[] buffer;
    return INVALID_NAMESPACE_OID;
  }
  auto ns_oid = *reinterpret_cast<namespace_oid_t *>(pr->AccessForceNotNull(table_pm[NSPOID_COL_OID]));
  delete[] buffer;
  return ns_oid;
}

template <typename Column>
bool DatabaseCatalog::CreateTableAttribute(transaction::TransactionContext *txn, uint32_t class_oid, const Column &col,
                                           const parser::AbstractExpression *default_val) {
  // Step 1: Insert into the table
  std::vector<col_oid_t> table_oids{ATTNUM_COL_OID, ATTRELID_COL_OID,   ATTNAME_COL_OID, ATTTYPID_COL_OID,
                                    ATTLEN_COL_OID, ATTNOTNULL_COL_OID, ADBIN_COL_OID,   ADSRC_COL_OID};
  // NOLINTNEXTLINE
  auto [table_pri, table_pm] = columns_->InitializerForProjectedRow(table_oids);
  auto *redo = txn->StageWrite(db_oid_, COLUMN_TABLE_OID, table_pri);
  auto oid_entry = reinterpret_cast<uint32_t *>(redo->Delta()->AccessForceNotNull(table_pm[ATTNUM_COL_OID]));
  auto relid_entry = reinterpret_cast<uint32_t *>(redo->Delta()->AccessForceNotNull(table_pm[ATTRELID_COL_OID]));
  auto name_entry =
      reinterpret_cast<storage::VarlenEntry *>(redo->Delta()->AccessForceNotNull(table_pm[ATTNAME_COL_OID]));
  auto type_entry = reinterpret_cast<type::TypeId *>(redo->Delta()->AccessForceNotNull(table_pm[ATTTYPID_COL_OID]));
  auto len_entry = reinterpret_cast<uint16_t *>(redo->Delta()->AccessForceNotNull(table_pm[ATTLEN_COL_OID]));
  auto notnull_entry = reinterpret_cast<bool *>(redo->Delta()->AccessForceNotNull(table_pm[ATTNOTNULL_COL_OID]));
  auto dbin_entry = reinterpret_cast<intptr_t *>(redo->Delta()->AccessForceNotNull(table_pm[ADBIN_COL_OID]));
  auto dsrc_entry =
      reinterpret_cast<storage::VarlenEntry *>(redo->Delta()->AccessForceNotNull(table_pm[ADSRC_COL_OID]));
  *oid_entry = !col.GetOid();
  *relid_entry = class_oid;
  storage::VarlenEntry name_varlen = postgres::AttributeHelper::MakeNameVarlen<Column>(col);
  *name_entry = name_varlen;
  *type_entry = col.GetType();
  // TODO(Amadou): Figure out what really goes here for varlen
  *len_entry = (col.GetType() == type::TypeId::VARCHAR || col.GetType() == type::TypeId::VARBINARY)
                   ? col.GetMaxVarlenSize()
                   : col.GetAttrSize();
  *notnull_entry = !col.GetNullable();
  *dbin_entry = reinterpret_cast<intptr_t>(default_val);
  storage::VarlenEntry dsrc_varlen = postgres::AttributeHelper::CreateVarlen(default_val->ToJson().dump());
  *dsrc_entry = dsrc_varlen;
  // Finally insert
  auto tupleslot = columns_->Insert(txn, redo);

  // Step 2: Insert into name index
  auto name_pri = columns_name_index_->GetProjectedRowInitializer();
  // Create a buffer large enough for all columns
  auto buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());
  auto pr = name_pri.InitializeRow(buffer);
  name_entry = reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0));
  *name_entry = name_varlen;
  relid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(1));
  *relid_entry = class_oid;

  if (!columns_name_index_->InsertUnique(txn, *pr, tupleslot)) {
    delete[] buffer;
    return false;
  }

  // Step 3: Insert into oid index
  auto oid_pri = columns_oid_index_->GetProjectedRowInitializer();
  pr = oid_pri.InitializeRow(buffer);
  oid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(0));
  *oid_entry = col.GetOid();
  relid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(1));
  *relid_entry = class_oid;

  if (!columns_oid_index_->InsertUnique(txn, *pr, tupleslot)) {
    delete[] buffer;
    return false;
  }

  // Step 4: Insert into class index
  auto class_pri = columns_class_index_->GetProjectedRowInitializer();
  pr = class_pri.InitializeRow(buffer);
  relid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(0));
  *relid_entry = class_oid;

  if (!columns_class_index_->Insert(txn, *pr, tupleslot)) {
    // Should probably not happen.
    delete[] buffer;
    return false;
  }

  delete[] buffer;
  return true;
}

template <typename Column>
std::unique_ptr<Column> DatabaseCatalog::GetTableAttribute(transaction::TransactionContext *txn,
                                                           storage::VarlenEntry *col_name, uint32_t class_oid) {
  // Step 1: Read Index
  std::vector<col_oid_t> table_oids{ATTNUM_COL_OID, ATTNAME_COL_OID,    ATTTYPID_COL_OID,
                                    ATTLEN_COL_OID, ATTNOTNULL_COL_OID, ADBIN_COL_OID};
  // NOLINTNEXTLINE
  auto [table_pri, table_pm] = columns_->InitializerForProjectedRow(table_oids);
  auto name_pri = columns_name_index_->GetProjectedRowInitializer();
  // Buffer is large enough to hold all prs
  byte *buffer = common::AllocationUtil::AllocateAligned(table_pri.ProjectedRowSize());
  // Scan the name index
  auto pr = name_pri.InitializeRow(buffer);
  auto *name_entry = reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0));
  *name_entry = *col_name;
  auto *relid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(1));
  *relid_entry = class_oid;
  std::vector<storage::TupleSlot> index_results;
  columns_name_index_->ScanKey(*txn, *pr, &index_results);
  if (index_results.empty()) {
    delete[] buffer;
    return nullptr;
  }
  TERRIER_ASSERT(index_results.size() == 1, "Columns name not unique in index");

  // Step 2: Scan the table to get the column
  pr = table_pri.InitializeRow(buffer);
  if (!columns_->Select(txn, index_results[0], pr)) {
    // Nothing visible
    delete[] buffer;
    return nullptr;
  }
  auto col = postgres::AttributeHelper::MakeColumn<Column>(pr, table_pm);
  delete[] buffer;
  return col;
}

template <typename Column>
std::unique_ptr<Column> DatabaseCatalog::GetTableAttribute(transaction::TransactionContext *txn, uint32_t col_oid,
                                                           uint32_t class_oid) {
  // Step 1: Read Index
  std::vector<col_oid_t> table_oids{ATTNUM_COL_OID, ATTNAME_COL_OID,    ATTTYPID_COL_OID,
                                    ATTLEN_COL_OID, ATTNOTNULL_COL_OID, ADBIN_COL_OID};
  // NOLINTNEXTLINE
  auto [table_pri, table_pm] = columns_->InitializerForProjectedRow(table_oids);
  auto oid_pri = columns_oid_index_->GetProjectedRowInitializer();
  // Buffer is large enough to hold all prs
  byte *buffer = common::AllocationUtil::AllocateAligned(table_pri.ProjectedRowSize());
  // Scan the oid index
  auto pr = oid_pri.InitializeRow(buffer);
  auto *oid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(0));
  *oid_entry = col_oid;
  auto *relid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(1));
  *relid_entry = class_oid;
  std::vector<storage::TupleSlot> index_results;
  columns_oid_index_->ScanKey(*txn, *pr, &index_results);
  if (index_results.empty()) {
    delete[] buffer;
    return nullptr;
  }
  TERRIER_ASSERT(index_results.size() == 1, "Columns oid not unique in index");

  // Step 2: Scan the table to get the column
  pr = table_pri.InitializeRow(buffer);
  if (!columns_->Select(txn, index_results[0], pr)) {
    // Nothing visible
    delete[] buffer;
    return nullptr;
  }
  auto col = postgres::AttributeHelper::MakeColumn<Column>(pr, table_pm);
  delete[] buffer;
  return col;
}

template <typename Column>
std::vector<std::unique_ptr<Column>> DatabaseCatalog::GetTableAttributes(transaction::TransactionContext *txn,
                                                                         uint32_t class_oid) {
  // Step 1: Read Index
  std::vector<col_oid_t> table_oids{ATTNUM_COL_OID, ATTNAME_COL_OID,    ATTTYPID_COL_OID,
                                    ATTLEN_COL_OID, ATTNOTNULL_COL_OID, ADBIN_COL_OID};
  // NOLINTNEXTLINE
  auto [table_pri, table_pm] = columns_->InitializerForProjectedRow(table_oids);
  auto class_pri = columns_class_index_->GetProjectedRowInitializer();
  // Buffer is large enough to hold all prs
  byte *buffer = common::AllocationUtil::AllocateAligned(table_pri.ProjectedRowSize());
  // Scan the class index
  auto pr = class_pri.InitializeRow(buffer);
  auto *relid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(1));
  *relid_entry = class_oid;
  std::vector<storage::TupleSlot> index_results;
  columns_class_index_->ScanKey(*txn, *pr, &index_results);
  if (index_results.empty()) {
    delete[] buffer;
    return nullptr;
  }

  // Step 2: Scan the table to get the columns
  std::vector<std::unique_ptr<Column>> cols;
  pr = table_pri.InitializeRow(buffer);
  for (const auto &slot : index_results) {
    if (!columns_->Select(txn, slot, pr)) {
      // Nothing visible
      delete[] buffer;
      return nullptr;
    }
    cols.emplace_back(postgres::AttributeHelper::MakeColumn<Column>(pr, table_pm));
  }
  delete[] buffer;
  return cols;
}

template <typename Column>
void DatabaseCatalog::DeleteColumns(transaction::TransactionContext *txn, uint32_t class_oid) {
  // Step 1: Read Index
  std::vector<col_oid_t> table_oids{ATTNUM_COL_OID, ATTNAME_COL_OID,    ATTTYPID_COL_OID,
                                    ATTLEN_COL_OID, ATTNOTNULL_COL_OID, ADBIN_COL_OID};
  // NOLINTNEXTLINE
  auto [table_pri, table_pm] = columns_->InitializerForProjectedRow(table_oids);
  auto class_pri = columns_class_index_->GetProjectedRowInitializer();
  // Buffer is large enough to hold all prs
  byte *buffer = common::AllocationUtil::AllocateAligned(table_pri.ProjectedRowSize());
  // Scan the class index
  auto pr = class_pri.InitializeRow(buffer);
  auto *relid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(1));
  *relid_entry = class_oid;
  std::vector<storage::TupleSlot> index_results;
  columns_class_index_->ScanKey(*txn, *pr, &index_results);
  if (index_results.empty()) {
    delete[] buffer;
  }

  // Step 2: Scan the table to get the columns
  pr = table_pri.InitializeRow(buffer);
  for (const auto &slot : index_results) {
    if (!columns_->Select(txn, slot, pr)) {
      // Nothing visible
      delete[] buffer;
      return;
    }
    auto col = postgres::AttributeHelper::MakeColumn<Column>(pr, table_pm);
    // 1. Delete from class index
    class_pri = columns_class_index_->GetProjectedRowInitializer();
    pr = class_pri.InitializeRow(buffer);
    relid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(0));
    *relid_entry = class_oid;
    columns_class_index_->Delete(txn, *pr, slot);

    // 2. Delete from oid index
    auto oid_pri = columns_oid_index_->GetProjectedRowInitializer();
    pr = oid_pri.InitializeRow(buffer);
    auto oid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(0));
    *oid_entry = !col->GetOid();
    relid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(1));
    *relid_entry = class_oid;
    columns_oid_index_->Delete(txn, *pr, slot);

    // 3. Delete from name index
    auto name_pri = columns_name_index_->GetProjectedRowInitializer();
    pr = name_pri.InitializeRow(buffer);
    auto name_entry = reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0));
    *name_entry = postgres::AttributeHelper::MakeNameVarlen(*col);
    relid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(1));
    *relid_entry = class_oid;
    columns_name_index_->Delete(txn, *pr, slot);
  }
  delete[] buffer;
}

// namespace_oid_t DatabaseCatalog::CreateNamespace(transaction::TransactionContext *txn, const std::string &name);

// bool DatabaseCatalog::DeleteNamespace(transaction::TransactionContext *txn, namespace_oid_t ns);

// namespace_oid_t DatabaseCatalog::GetNamespaceOid(transaction::TransactionContext *txn, const std::string &name);

// table_oid_t DatabaseCatalog::CreateTable(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string
// &name,
//                           const Schema &schema);

// bool DatabaseCatalog::DeleteTable(transaction::TransactionContext *txn, table_oid_t table);

// table_oid_t DatabaseCatalog::GetTableOid(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string
// &name);

// bool DatabaseCatalog::RenameTable(transaction::TransactionContext *txn, table_oid_t table, const std::string &name);

// bool DatabaseCatalog::UpdateSchema(transaction::TransactionContext *txn, table_oid_t table, Schema *new_schema);

// const Schema &DatabaseCatalog::GetSchema(transaction::TransactionContext *txn, table_oid_t table);

// std::vector<constraint_oid_t> DatabaseCatalog::GetConstraints(transaction::TransactionContext *txn, table_oid_t);

// std::vector<index_oid_t> DatabaseCatalog::GetIndexes(transaction::TransactionContext *txn, table_oid_t);

// index_oid_t DatabaseCatalog::CreateIndex(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string
// &name,
//                                          table_oid_t table, IndexSchema *schema);

// bool DatabaseCatalog::DeleteIndex(transaction::TransactionContext *txn, index_oid_t index);

// index_oid_t DatabaseCatalog::GetIndexOid(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string
// &name);

// const IndexSchema &DatabaseCatalog::GetIndexSchema(transaction::TransactionContext *txn, index_oid_t index);

void DatabaseCatalog::TearDown(transaction::TransactionContext *txn) {
  std::vector<parser::AbstractExpression *> expressions;
  std::vector<Schema *> table_schemas;
  std::vector<storage::SqlTable *> tables;
  std::vector<IndexSchema *> index_schemas;
  std::vector<storage::index::Index *> indexes;

  std::vector<col_oid_t> col_oids;

  // pg_class (schemas & objects) [this is the largest projection]
  col_oids.emplace_back(RELKIND_COL_OID);
  col_oids.emplace_back(REL_SCHEMA_COL_OID);
  col_oids.emplace_back(REL_PTR_COL_OID);
  auto [pci, pm] = classes_->InitializerForProjectedColumns(col_oids, 100);

  byte *buffer = common::AllocationUtil::AllocateAligned(pci.ProjectedColumnsSize());
  auto pc = pci.Initialize(buffer);

  // Fetch pointers to the start each in the projected columns
  auto classes = reinterpret_cast<postgres::ClassKind *>(pc->ColumnStart(pm[RELKIND_COL_OID]));
  auto schemas = reinterpret_cast<void **>(pc->ColumnStart(pm[REL_SCHEMA_COL_OID]));
  auto objects = reinterpret_cast<void **>(pc->ColumnStart(pm[REL_PTR_COL_OID]));

  // Scan the table
  auto table_iter = classes_->begin();
  while (table_iter != classes_->end()) {
    classes_->Scan(txn, &table_iter, pc);
    for (uint i = 0; i < pc->NumTuples(); i++) {
      switch (classes[i]) {
        case postgres::ClassKind::REGULAR_TABLE:
          table_schemas.emplace_back(reinterpret_cast<Schema *>(schemas[i]));
          tables.emplace_back(reinterpret_cast<storage::SqlTable *>(objects[i]));
          break;
        case postgres::ClassKind::INDEX:
          index_schemas.emplace_back(reinterpret_cast<IndexSchema *>(schemas[i]));
          indexes.emplace_back(reinterpret_cast<storage::index::Index *>(objects[i]));
          break;
        default:
          throw std::runtime_error("Unimplemented destructor needed");
      }
    }
  }

  // pg_attribute (expressions)
  col_oids.clear();
  col_oids.emplace_back(ADBIN_COL_OID);
  std::tie(pci, pm) = columns_->InitializerForProjectedColumns(col_oids, 100);
  pc = pci.Initialize(buffer);

  auto exprs = reinterpret_cast<parser::AbstractExpression **>(pc->ColumnStart(0));

  table_iter = columns_->begin();
  while (table_iter != columns_->end()) {
    columns_->Scan(txn, &table_iter, pc);

    for (uint i = 0; i < pc->NumTuples(); i++) {
      expressions.emplace_back(exprs[i]);
    }
  }

  // pg_constraint (expressions)
  col_oids.clear();
  col_oids.emplace_back(CONBIN_COL_OID);
  std::tie(pci, pm) = constraints_->InitializerForProjectedColumns(col_oids, 100);
  pc = pci.Initialize(buffer);

  exprs = reinterpret_cast<parser::AbstractExpression **>(pc->ColumnStart(0));

  table_iter = constraints_->begin();
  while (table_iter != constraints_->end()) {
    constraints_->Scan(txn, &table_iter, pc);

    for (uint i = 0; i < pc->NumTuples(); i++) {
      expressions.emplace_back(exprs[i]);
    }
  }

  // No new transactions can see these object but there may be deferred index
  // and other operation.  Therefore, we need to defer the deallocation on delete
  txn->RegisterCommitAction([=, tables{std::move(tables)}, indexes{std::move(indexes)},
                             table_schemas{std::move(table_schemas)}, index_schemas{std::move(index_schemas)},
                             expressions{std::move(expressions)}] {
    txn->GetTransactionManager()->DeferAction(
        [=, tables{std::move(tables)}, indexes{std::move(indexes)}, table_schemas{std::move(table_schemas)},
         index_schemas{std::move(index_schemas)}, expressions{std::move(expressions)}] {
          for (auto table : tables) delete table;

          for (auto index : indexes) delete index;

          for (auto schema : table_schemas) delete schema;

          for (auto schema : index_schemas) delete schema;

          for (auto expr : expressions) delete expr;
        });
  });
}

type_oid_t DatabaseCatalog::GetTypeOidForType(type::TypeId type) { return type_oid_t(static_cast<uint8_t>(type)); }

void DatabaseCatalog::InsertType(transaction::TransactionContext *txn, type::TypeId internal_type,
                                 const std::string &name, namespace_oid_t namespace_oid, int16_t len, bool by_val,
                                 postgres::Type type_category) {
  std::vector<col_oid_t> table_col_oids;
  table_col_oids.emplace_back(TYPOID_COL_OID);
  table_col_oids.emplace_back(TYPNAME_COL_OID);
  table_col_oids.emplace_back(TYPNAMESPACE_COL_OID);
  table_col_oids.emplace_back(TYPLEN_COL_OID);
  table_col_oids.emplace_back(TYPBYVAL_COL_OID);
  table_col_oids.emplace_back(TYPTYPE_COL_OID);
  auto initializer_pair = types_->InitializerForProjectedRow(table_col_oids);
  auto initializer = initializer_pair.first;
  auto col_map = initializer_pair.second;

  // Stage the write into the table
  auto redo_record = txn->StageWrite(db_oid_, TYPE_TABLE_OID, initializer);
  auto *delta = redo_record->Delta();

  // Populate oid
  auto offset = col_map[TYPOID_COL_OID];
  auto type_oid = GetTypeOidForType(internal_type);
  memcpy(delta->AccessForceNotNull(offset), &type_oid, sizeof(type_oid_t));

  // Populate type name
  offset = col_map[TYPNAME_COL_OID];
  storage::VarlenEntry name_varlen;
  if (name.size() > storage::VarlenEntry::InlineThreshold()) {
    byte *contents = common::AllocationUtil::AllocateAligned(name.size());
    std::memcpy(contents, name.data(), name.size());
    name_varlen = storage::VarlenEntry::Create(contents, static_cast<uint32_t>(name.size()), true);
  } else {
    name_varlen = storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *>(name.data()),
                                                     static_cast<uint32_t>(name.size()));
  }
  *(reinterpret_cast<storage::VarlenEntry *>(delta->AccessForceNotNull(offset))) = name_varlen;

  // Populate namespace
  offset = col_map[TYPNAMESPACE_COL_OID];
  memcpy(delta->AccessForceNotNull(offset), &namespace_oid, sizeof(namespace_oid_t));

  // Populate len
  offset = col_map[TYPLEN_COL_OID];
  memcpy(delta->AccessForceNotNull(offset), &len, sizeof(int16_t) /* SMALLINT */);

  // Populate byval
  offset = col_map[TYPBYVAL_COL_OID];
  memcpy(delta->AccessForceNotNull(offset), &by_val, sizeof(bool));

  // Populate type
  offset = col_map[TYPTYPE_COL_OID];
  // TODO(Gus): make sure this cast works
  auto type = static_cast<uint8_t>(type_category);
  memcpy(delta->AccessForceNotNull(offset), &type, sizeof(uint8_t) /* TINYINT */);

  // Insert into table
  auto tuple_slot = types_->Insert(txn, redo_record);

  // Allocate buffer of largest size needed
  uint32_t buffer_size = 0;
  buffer_size = std::max(buffer_size, types_oid_index_->GetProjectedRowInitializer().ProjectedRowSize());
  buffer_size = std::max(buffer_size, types_name_index_->GetProjectedRowInitializer().ProjectedRowSize());
  buffer_size = std::max(buffer_size, types_namespace_index_->GetProjectedRowInitializer().ProjectedRowSize());
  byte *buffer = common::AllocationUtil::AllocateAligned(buffer_size);

  // Insert into oid index
  auto oid_index_delta = types_oid_index_->GetProjectedRowInitializer().InitializeRow(buffer);
  auto oid_index_offset = types_oid_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(1));
  memcpy(oid_index_delta->AccessForceNotNull(oid_index_offset), &type_oid, sizeof(type_oid_t));
  auto result UNUSED_ATTRIBUTE = types_oid_index_->InsertUnique(txn, *oid_index_delta, tuple_slot);
  TERRIER_ASSERT(result, "Insert into type oid index should always succeed");

  // Insert into (namespace_oid, name) index
  buffer = common::AllocationUtil::AllocateAligned(types_name_index_->GetProjectedRowInitializer().ProjectedRowSize());
  auto name_index_delta = types_name_index_->GetProjectedRowInitializer().InitializeRow(buffer);
  // Populate namespace
  auto name_index_offset = types_name_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(1));
  memcpy(name_index_delta->AccessForceNotNull(name_index_offset), &namespace_oid, sizeof(namespace_oid_t));
  // Populate type name
  name_index_offset = types_name_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(2));
  *(reinterpret_cast<storage::VarlenEntry *>(name_index_delta->AccessForceNotNull(name_index_offset))) = name_varlen;
  result = types_name_index_->InsertUnique(txn, *name_index_delta, tuple_slot);
  TERRIER_ASSERT(result, "Insert into type name index should always succeed");

  // Insert into (non-unique) namespace oid index
  buffer =
      common::AllocationUtil::AllocateAligned(types_namespace_index_->GetProjectedRowInitializer().ProjectedRowSize());
  auto namespace_index_delta = types_namespace_index_->GetProjectedRowInitializer().InitializeRow(buffer);
  auto namespace_index_offset = types_namespace_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(1));
  memcpy(namespace_index_delta->AccessForceNotNull(namespace_index_offset), &namespace_oid, sizeof(namespace_oid_t));
  result = types_namespace_index_->Insert(txn, *name_index_delta, tuple_slot);
  TERRIER_ASSERT(result, "Insert into type namespace index should always succeed");

  // Clean up buffer
  delete[] buffer;
}

void DatabaseCatalog::BootstrapTypes(transaction::TransactionContext *txn) {
  InsertType(txn, type::TypeId::INVALID, "INVALID", NAMESPACE_CATALOG_NAMESPACE_OID, 1, true, postgres::Type::BASE);

  InsertType(txn, type::TypeId::BOOLEAN, "BOOLEAN", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(bool), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::TINYINT, "TINYINT", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(int8_t), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::SMALLINT, "SMALLINT", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(int16_t), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::INTEGER, "INTEGER", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(int32_t), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::BIGINT, "BIGINT", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(int64_t), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::DECIMAL, "DECIMAL", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(double), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::TIMESTAMP, "TIMESTAMP", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(type::timestamp_t),
             true, postgres::Type::BASE);

  InsertType(txn, type::TypeId::DATE, "DATE", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(type::date_t), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::VARCHAR, "VARCHAR", NAMESPACE_CATALOG_NAMESPACE_OID, -1, false, postgres::Type::BASE);

  InsertType(txn, type::TypeId::VARBINARY, "VARBINARY", NAMESPACE_CATALOG_NAMESPACE_OID, -1, false,
             postgres::Type::BASE);
}

}  // namespace terrier::catalog
