#pragma once
#include <iostream>
#include <memory>
#include <fstream>
#include "type/type_id.h"
#include "catalog/schema.h"
#include "catalog/catalog.h"
#include "transaction/transaction_context.h"


namespace tpl::reader {
using namespace terrier;

// Maps from index columns to table columns.
using IndexTableMap = std::vector<uint16_t>;

/**
 * Stores info about an index
 */
struct IndexInfo {
  /**
   * Constructor
   */
  IndexInfo() = default;


  /**
   * Index Name
   */
  std::string index_name;
  /**
   * Index Schema
   */
  storage::index::IndexKeySchema schema;


  /**
   * Mapping from index column to table column
   */
  IndexTableMap index_map;
};

/**
 * Stores table information
 */
struct TableInfo {
  /**
   * Constructor
   */
  TableInfo() = default;


  /**
   * Table Name
   */
  std::string table_name;
  /**
   * Table Schema
   */
  std::unique_ptr<terrier::catalog::Schema> schema;

  /**
   * indexes
   */
  std::vector<std::unique_ptr<IndexInfo>> indexes;
};



/**
 * Reads .schema file
 * File format:
 * table_name num_cols
 * col_name1(string), type1(string), nullable1(0 or 1)
 * ...
 * col_nameN(string), typeN(string), nullableN(0 or 1), varchar_size if type == varchar
 * num_indexes
 * index_name1 num_index_cols1
 * table_col_idx1 table_col_idxN
 * ...
 * ...
 * index_nameM num_index_colM
 * ...
 */
class SchemaReader {
 public:
  explicit SchemaReader(catalog::Catalog * catalog)
  : catalog_{catalog}
  {
    InitTypeNames();
  }

  /**
   * Reads table metadata
   * @param filename name of the file containing the metadate
   * @return the struct containing information about the table
   */
  std::unique_ptr<TableInfo> ReadTableInfo(const std::string & filename) {
    // Allocate table information
    auto table_info = std::make_unique<TableInfo>();
    // Open file to read
    std::ifstream schema_file;
    schema_file.open(filename);
    // Read Table name and num_cols
    uint32_t num_cols;
    schema_file >> table_info->table_name >> num_cols;
    std::cout << "Reading table " << table_info->table_name << " with " << num_cols << " columns." << std::endl;
    // Read columns & create table schema
    std::vector<terrier::catalog::Schema::Column> cols{ReadColumns(&schema_file, num_cols)};
    table_info->schema = std::make_unique<catalog::Schema>(cols);

    // Read num_indexes & create index information
    uint32_t num_indexes;
    schema_file >> num_indexes;
    ReadIndexSchemas(&schema_file, table_info.get(), num_indexes);
    return table_info;
  }

 private:
  void ReadIndexSchemas(std::ifstream * in, TableInfo * table_info, uint32_t num_indexes) {
    uint32_t num_index_cols;
    for (uint32_t i = 0; i < num_indexes; i++) {
      auto index_info = std::make_unique<IndexInfo>();
      // Read index name and num_index_cols
      *in >> index_info->index_name >> num_index_cols;
      // Read each index column
      uint16_t col_idx;
      for (uint32_t j = 0; j < num_index_cols; j++) {
        *in >> col_idx;
        index_info->index_map.emplace_back(col_idx);
        terrier::catalog::indexkeycol_oid_t col_oid(catalog_->GetNextOid());
        const auto & table_column = table_info->schema->GetColumn(col_idx);
        index_info->schema.emplace_back(col_oid, table_column.GetType(), table_column.GetNullable());
      }
      // Update list of indexes
      table_info->indexes.emplace_back(std::move(index_info));
    }
  }


  std::vector<terrier::catalog::Schema::Column> ReadColumns(std::ifstream * in, uint32_t num_cols) {
    std::vector<terrier::catalog::Schema::Column> cols;
    // Read each column
    std::string col_name;
    std::string col_type_str;
    terrier::type::TypeId col_type;
    uint32_t varchar_size{0};
    bool nullable;
    for (uint32_t i = 0; i < num_cols; i++) {
      *in >> col_name >> col_type_str >> nullable;
      catalog::col_oid_t col_oid{catalog_->GetNextOid()};
      col_type = type_names_[col_type_str];
      if (col_type == type::TypeId::VARCHAR) {
        *in >> varchar_size;
        cols.emplace_back(col_name, col_type, varchar_size, nullable, col_oid);
      } else {
        cols.emplace_back(col_name, col_type, nullable, col_oid);
      }
      std::cout << "Read column: ";
      std::cout << "col_name=" << col_name << ", ";
      std::cout << "col_type=" << col_type_str << ", ";
      if (col_type == type::TypeId::VARCHAR) {
        std::cout << "varchar_size=" << varchar_size << ", ";
      }
      std::cout << "nullable=" << nullable << ", ";
      std::cout << "col_oid=" << !col_oid << std::endl;
    }
    return cols;
  }

  void InitTypeNames() {
    type_names_["tinyint"] = terrier::type::TypeId::TINYINT;
    type_names_["smallint"] = terrier::type::TypeId::SMALLINT;
    type_names_["int"] = terrier::type::TypeId::INTEGER;
    type_names_["bigint"] = terrier::type::TypeId::BIGINT;
    type_names_["bool"] = terrier::type::TypeId::BOOLEAN;
    type_names_["int"] = terrier::type::TypeId::INTEGER;
    type_names_["real"] = terrier::type::TypeId::DECIMAL;
    type_names_["decimal"] = terrier::type::TypeId::DECIMAL;
    type_names_["varchar"] = terrier::type::TypeId::VARCHAR;
    type_names_["varlen"] = terrier::type::TypeId::VARCHAR;
    type_names_["date"] = terrier::type::TypeId::DATE;
  }

 private:
  catalog::Catalog * catalog_;
  std::unordered_map<std::string, terrier::type::TypeId> type_names_{};

};
}