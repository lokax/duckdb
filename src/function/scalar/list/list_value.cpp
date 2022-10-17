#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/storage/statistics/list_statistics.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

static void ListValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
    // 拿出child type
	auto &child_type = ListType::GetChildType(result.GetType());

    // 一开始设置成constant vector
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
	}

	auto result_data = FlatVector::GetData<list_entry_t>(result);
    // 遍历每一条数据
	for (idx_t i = 0; i < args.size(); i++) {
        // offset设置当前列表的大小作为起使位置
		result_data[i].offset = ListVector::GetListSize(result);
        // 每一列数据作为列表的数据推进去
		for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
			auto val = args.GetValue(col_idx, i).DefaultCastAs(child_type);
			ListVector::PushBack(result, val);
		}
		result_data[i].length = args.ColumnCount();
	}
	result.Verify(args.size());
}

static unique_ptr<FunctionData> ListValueBind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	// collect names and deconflict, construct return type
    // SQLNULL用在这个地方上
	LogicalType child_type = arguments.empty() ? LogicalType::SQLNULL : arguments[0]->return_type;
	for (idx_t i = 1; i < arguments.size(); i++) {
        // 获取最大的类型
		child_type = LogicalType::MaxLogicalType(child_type, arguments[i]->return_type);
	}

	// this is more for completeness reasons
    // 一开始是ANY type, 现在改成child type
	bound_function.varargs = child_type;
    // 返回类型是列表类型
	bound_function.return_type = LogicalType::LIST(move(child_type));
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

unique_ptr<BaseStatistics> ListValueStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	auto list_stats = make_unique<ListStatistics>(expr.return_type);
	for (idx_t i = 0; i < child_stats.size(); i++) {
		if (child_stats[i]) {
			list_stats->child_stats->Merge(*child_stats[i]);
		} else {
			list_stats->child_stats.reset();
			return move(list_stats);
		}
	}
	return move(list_stats);
}

void ListValueFun::RegisterFunction(BuiltinFunctions &set) {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun("list_value", {}, LogicalTypeId::LIST, ListValueFunction, ListValueBind, nullptr,
	                   ListValueStats);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	set.AddFunction(fun);
	fun.name = "list_pack";
	set.AddFunction(fun);
}

} // namespace duckdb
