#include "duckdb/planner/expression_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_default_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

ExpressionBinder::ExpressionBinder(Binder &binder, ClientContext &context, bool replace_binder)
    : binder(binder), context(context), stored_binder(nullptr) {
	if (replace_binder) {
		stored_binder = binder.GetActiveBinder();
		binder.SetActiveBinder(this);
	} else {
		binder.PushExpressionBinder(this);
	}
}

ExpressionBinder::~ExpressionBinder() {
	if (binder.HasActiveBinder()) {
		if (stored_binder) {
			binder.SetActiveBinder(stored_binder);
		} else {
			binder.PopExpressionBinder();
		}
	}
}

BindResult ExpressionBinder::BindExpression(unique_ptr<ParsedExpression> *expr, idx_t depth, bool root_expression) {
	auto &expr_ref = **expr;
	switch (expr_ref.expression_class) {
	case ExpressionClass::BETWEEN:
		return BindExpression((BetweenExpression &)expr_ref, depth);
	case ExpressionClass::CASE:
		return BindExpression((CaseExpression &)expr_ref, depth);
	case ExpressionClass::CAST:
		return BindExpression((CastExpression &)expr_ref, depth);
	case ExpressionClass::COLLATE:
		return BindExpression((CollateExpression &)expr_ref, depth);
	case ExpressionClass::COLUMN_REF:
		return BindExpression((ColumnRefExpression &)expr_ref, depth);
	case ExpressionClass::COMPARISON:
		return BindExpression((ComparisonExpression &)expr_ref, depth);
	case ExpressionClass::CONJUNCTION:
		return BindExpression((ConjunctionExpression &)expr_ref, depth);
	case ExpressionClass::CONSTANT:
		return BindExpression((ConstantExpression &)expr_ref, depth);
	case ExpressionClass::FUNCTION:
		// binding function expression has extra parameter needed for macro's
		return BindExpression((FunctionExpression &)expr_ref, depth, expr);
	case ExpressionClass::LAMBDA:
		return BindExpression((LambdaExpression &)expr_ref, depth);
	case ExpressionClass::OPERATOR:
		return BindExpression((OperatorExpression &)expr_ref, depth);
	case ExpressionClass::SUBQUERY:
		return BindExpression((SubqueryExpression &)expr_ref, depth);
	case ExpressionClass::PARAMETER:
		return BindExpression((ParameterExpression &)expr_ref, depth);
	case ExpressionClass::POSITIONAL_REFERENCE:
		return BindExpression((PositionalReferenceExpression &)expr_ref, depth);
	default:
		throw NotImplementedException("Unimplemented expression class");
	}
}

// 绑定关联列
bool ExpressionBinder::BindCorrelatedColumns(unique_ptr<ParsedExpression> &expr) {
	// try to bind in one of the outer queries, if the binding error occurred in a subquery
	auto &active_binders = binder.GetActiveBinders();
	// make a copy of the set of binders, so we can restore it later
	auto binders = active_binders;
	active_binders.pop_back(); // 弹掉当前的ExpressionBinder(自己)
	idx_t depth = 1; // 自己depth为0
	bool success = false; 
	while (!active_binders.empty()) { // 只要还存在ExpressionBinder
		auto &next_binder = active_binders.back();
		ExpressionBinder::QualifyColumnNames(next_binder->binder, expr); // 获取qualify name
		auto bind_result = next_binder->Bind(&expr, depth);
		if (bind_result.empty()) {
			success = true;
			break;
		}
		depth++;
		active_binders.pop_back();
	}
	active_binders = binders; // 恢复binder
	return success;
}

void ExpressionBinder::BindChild(unique_ptr<ParsedExpression> &expr, idx_t depth, string &error) {
	if (expr) {
		string bind_error = Bind(&expr, depth);
		if (error.empty()) {
			error = bind_error;
		}
	}
}

// 提取相关列的信息，并保存到binder中
void ExpressionBinder::ExtractCorrelatedExpressions(Binder &binder, Expression &expr) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = (BoundColumnRefExpression &)expr;
		if (bound_colref.depth > 0) {
			binder.AddCorrelatedColumn(CorrelatedColumnInfo(bound_colref));
		}
	}
	ExpressionIterator::EnumerateChildren(expr,
	                                      [&](Expression &child) { ExtractCorrelatedExpressions(binder, child); });
}

bool ExpressionBinder::ContainsType(const LogicalType &type, LogicalTypeId target) {
	if (type.id() == target) {
		return true;
	}
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP: {
		auto child_count = StructType::GetChildCount(type);
		for (idx_t i = 0; i < child_count; i++) {
			if (ContainsType(StructType::GetChildType(type, i), target)) {
				return true;
			}
		}
		return false;
	}
	case LogicalTypeId::LIST:
		return ContainsType(ListType::GetChildType(type), target);
	default:
		return false;
	}
}

LogicalType ExpressionBinder::ExchangeType(const LogicalType &type, LogicalTypeId target, LogicalType new_type) {
	if (type.id() == target) {
		return new_type;
	}
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP: {
		// we make a copy of the child types of the struct here
		auto child_types = StructType::GetChildTypes(type);
		for (auto &child_type : child_types) {
			child_type.second = ExchangeType(child_type.second, target, new_type);
		}
		return type.id() == LogicalTypeId::MAP ? LogicalType::MAP(move(child_types))
		                                       : LogicalType::STRUCT(move(child_types));
	}
	case LogicalTypeId::LIST:
		return LogicalType::LIST(ExchangeType(ListType::GetChildType(type), target, new_type));
	default:
		return type;
	}
}
// 返回是否包含SQLNULL类型
bool ExpressionBinder::ContainsNullType(const LogicalType &type) {
	return ContainsType(type, LogicalTypeId::SQLNULL);
}

// 把SQLNULL类型转换成整数类型
LogicalType ExpressionBinder::ExchangeNullType(const LogicalType &type) {
	return ExchangeType(type, LogicalTypeId::SQLNULL, LogicalType::INTEGER);
}

// 类型是UNKNOWN则变成VARCHAR
void ExpressionBinder::ResolveParameterType(LogicalType &type) {
	if (type.id() == LogicalTypeId::UNKNOWN) {
		type = LogicalType::VARCHAR;
	}
}

// 解决参数类型
void ExpressionBinder::ResolveParameterType(unique_ptr<Expression> &expr) {
    // 如果表达式的返回类型中有UNKNOWN的类型(给参数表达式使用的类型)
	if (ContainsType(expr->return_type, LogicalTypeId::UNKNOWN)) {
        // 为什么将类型变成VARCHAR？而不是其他的？
        auto result_type = ExchangeType(expr->return_type, LogicalTypeId::UNKNOWN, LogicalType::VARCHAR);
        // 添加Cast Expression做类型转换
		expr = BoundCastExpression::AddCastToType(move(expr), result_type);
	}
}

unique_ptr<Expression> ExpressionBinder::Bind(unique_ptr<ParsedExpression> &expr, LogicalType *result_type,
                                              bool root_expression) {
	// bind the main expression
	auto error_msg = Bind(&expr, 0, root_expression);
	if (!error_msg.empty()) { // 绑定失败
		// failed to bind: try to bind correlated columns in the expression (if any)
		bool success = BindCorrelatedColumns(expr); // 绑定相关列
		if (!success) {
			throw BinderException(error_msg); // 如果绑定相关列失败，就返回错误
		}
		auto bound_expr = (BoundExpression *)expr.get();
		ExtractCorrelatedExpressions(binder, *bound_expr->expr);
	}
	D_ASSERT(expr->expression_class == ExpressionClass::BOUND_EXPRESSION);
	auto bound_expr = (BoundExpression *)expr.get(); // 这是只是一个包装类而已
	unique_ptr<Expression> result = move(bound_expr->expr); // 拿出实际的表达式
	if (target_type.id() != LogicalTypeId::INVALID) {
		// the binder has a specific target type: add a cast to that type
        // 如果对表达式的返回类型有要求，则添加一个Cast Expression来做类型转换
		result = BoundCastExpression::AddCastToType(move(result), target_type);
	} else {
		if (!binder.can_contain_nulls) {
			// SQL NULL type is only used internally in the binder
			// cast to INTEGER if we encounter it outside of the binder
			if (ContainsNullType(result->return_type)) {
                // 包含SQLNULL TYPE，则转换成整型
				auto result_type = ExchangeNullType(result->return_type);
				result = BoundCastExpression::AddCastToType(move(result), result_type);
			}
		}
		// check if we failed to convert any parameters
		// if we did, we push a cast
        // parameter expression是给prepare statement使用的
		ExpressionBinder::ResolveParameterType(result); // 将UNKNOWN转成VARCHAR
	}
	if (result_type) {
		*result_type = result->return_type;
	}
	return result;
}

string ExpressionBinder::Bind(unique_ptr<ParsedExpression> *expr, idx_t depth, bool root_expression) {
	// bind the node, but only if it has not been bound yet
	auto &expression = **expr;
	auto alias = expression.alias;
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION) { // 已经绑定了
		// already bound, don't bind it again
		return string();
	}
	// bind the expression
	BindResult result = BindExpression(expr, depth, root_expression);
	if (result.HasError()) { // 有错误，返回错误值
		return result.error;
	} else {
		// successfully bound: replace the node with a BoundExpression
		*expr = make_unique<BoundExpression>(move(result.expression));
		auto be = (BoundExpression *)expr->get();
		D_ASSERT(be);
		be->alias = alias;
		if (!alias.empty()) {
			be->expr->alias = alias;
		}
		return string();
	}
}

} // namespace duckdb
