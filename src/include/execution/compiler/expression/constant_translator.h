#pragma once
#include "execution/compiler/expression/expression_translator.h"

namespace tpl::compiler {

/**
 * Constant Translator
 */
class ConstantTranslator : public ExpressionTranslator {
 public:
  /**
   * Constructor
   * @param expression expression to translate
   * @param context compilation context to use
   */
  ConstantTranslator(const terrier::parser::AbstractExpression *expression, CompilationContext *context);

  ast::Expr *DeriveExpr(const terrier::parser::AbstractExpression *expression, RowBatch *row) override;
};
}  // namespace tpl::compiler