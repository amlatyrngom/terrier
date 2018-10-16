//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sql_type.cpp
//
// Identification: src/execution/type/sql_type.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/type/sql_type.h"

#include "common/exception.h"
#include "execution/type/array_type.h"
#include "execution/type/bigint_type.h"
#include "execution/type/boolean_type.h"
#include "execution/type/date_type.h"
#include "execution/type/decimal_type.h"
#include "execution/type/integer_type.h"
#include "execution/type/smallint_type.h"
#include "execution/type/timestamp_type.h"
#include "execution/type/tinyint_type.h"
#include "execution/type/varbinary_type.h"
#include "execution/type/varchar_type.h"
#include "execution/value.h"

namespace terrier::execution {

namespace type {

class Invalid : public SqlType {
 public:
  bool IsVariableLength() const override { throw Exception{"INVALID type know if it is variable in length"}; }

  Value GetMinValue(UNUSED_ATTRIBUTE CodeGen &codegen) const override {
    throw Exception{"INVALID type doesn't have a minimum value"};
  }

  Value GetMaxValue(UNUSED_ATTRIBUTE CodeGen &codegen) const override {
    throw Exception{"INVALID type doesn't have a maximum value"};
  }

  Value GetNullValue(UNUSED_ATTRIBUTE CodeGen &codegen) const override {
    throw Exception{"INVALID type doesn't have a NULL value"};
  }

  void GetTypeForMaterialization(UNUSED_ATTRIBUTE CodeGen &codegen, UNUSED_ATTRIBUTE llvm::Type *&val_type,
                                 UNUSED_ATTRIBUTE llvm::Type *&len_type) const override {
    throw Exception{"INVALID type doesn't have a materialization type"};
  }

  llvm::Function *GetInputFunction(UNUSED_ATTRIBUTE CodeGen &codegen,
                                   UNUSED_ATTRIBUTE const Type &type) const override {
    throw Exception{"INVALID type does not have an input function"};
  }

  llvm::Function *GetOutputFunction(UNUSED_ATTRIBUTE CodeGen &codegen,
                                    UNUSED_ATTRIBUTE const Type &type) const override {
    throw Exception{"INVALID type does not have an output function"};
  }

  const TypeSystem &GetTypeSystem() const override { throw Exception{"INVALID type doesn't have a type system"}; }

 private:
  Invalid() : SqlType(::terrier::type::TypeId::INVALID) {}
};

namespace {

// The order of elements here **must** be the same as ::terrier::type::TypeId
static const SqlType *kTypeTable[] = {
    &Invalid::Instance(),    // The invalid type
    &Invalid::Instance(),    // The parameter offset type ... which isn't a real
                             // SQL type
    &Boolean::Instance(),    // The boolean type
    &TinyInt::Instance(),    // The tinyint type (1 byte)
    &SmallInt::Instance(),   // The smallint type (2 byte)
    &Integer::Instance(),    // The integer type (4 byte)
    &BigInt::Instance(),     // The bigint type (8 byte)
    &Decimal::Instance(),    // The decimal type (8 byte)
    &Timestamp::Instance(),  // The timestamp type (8 byte)
    &Date::Instance(),       // The date type (4 byte)
    &Varchar::Instance(),    // The varchar type
    &Varbinary::Instance(),  // The varbinary type
    &Array::Instance(),      // The array type
    &Invalid::Instance()     // A user-defined type
};

}  // anonymous namespace

const SqlType &SqlType::LookupType(::terrier::type::TypeId type_id) {
  return *kTypeTable[static_cast<uint32_t>(type_id)];
}

}  // namespace type

}  // namespace terrier::execution
