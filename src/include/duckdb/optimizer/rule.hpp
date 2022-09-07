//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/optimizer/matcher/logical_operator_matcher.hpp"

namespace duckdb {
class ExpressionRewriter;

class Rule {
public:
	explicit Rule(ExpressionRewriter &rewriter) : rewriter(rewriter) {
	}
	virtual ~Rule() {
	}

	//! The expression rewriter this rule belongs to
	ExpressionRewriter &rewriter; // 表达式重写器
	//! The root
	unique_ptr<LogicalOperatorMatcher> logical_root; // 算子matcher
	//! The expression matcher of the rule
	unique_ptr<ExpressionMatcher> root; // 表达式matcher

	virtual unique_ptr<Expression> Apply(LogicalOperator &op, vector<Expression *> &bindings, bool &fixed_point,
	                                     bool is_root) = 0;
};

} // namespace duckdb
