#include "duckdb/optimizer/filter_pushdown.hpp"

#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::Rewrite(unique_ptr<LogicalOperator> op) {
	D_ASSERT(!combiner.HasFilters());
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return PushdownAggregate(move(op));
	case LogicalOperatorType::LOGICAL_FILTER:
		return PushdownFilter(move(op));
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return PushdownCrossProduct(move(op));
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		return PushdownJoin(move(op));
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return PushdownProjection(move(op));
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_UNION:
		return PushdownSetOperation(move(op));
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		// we can just push directly through these operations without any rewriting
		op->children[0] = Rewrite(move(op->children[0]));
		return op;
	}
	case LogicalOperatorType::LOGICAL_GET:
		return PushdownGet(move(op));
	case LogicalOperatorType::LOGICAL_LIMIT:
		return PushdownLimit(move(op));
	default:
		return FinishPushdown(move(op));
	}
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownJoin(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	         op->type == LogicalOperatorType::LOGICAL_ANY_JOIN || op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN);
	auto &join = (LogicalJoin &)*op;
	unordered_set<idx_t> left_bindings, right_bindings;
	LogicalJoin::GetTableReferences(*op->children[0], left_bindings);
	LogicalJoin::GetTableReferences(*op->children[1], right_bindings);

	switch (join.join_type) {
	case JoinType::INNER:
		return PushdownInnerJoin(move(op), left_bindings, right_bindings);
	case JoinType::LEFT:
		return PushdownLeftJoin(move(op), left_bindings, right_bindings);
	case JoinType::MARK:
		return PushdownMarkJoin(move(op), left_bindings, right_bindings);
	case JoinType::SINGLE:
		return PushdownSingleJoin(move(op), left_bindings, right_bindings);
	default:
		// unsupported join type: stop pushing down
		return FinishPushdown(move(op));
	}
}
void FilterPushdown::PushFilters() {
	for (auto &f : filters) {
		auto result = combiner.AddFilter(move(f->filter));
		D_ASSERT(result == FilterResult::SUCCESS);
		(void)result;
	}
	filters.clear();
}

FilterResult FilterPushdown::AddFilter(unique_ptr<Expression> expr) {
	PushFilters();
	// split up the filters by AND predicate
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(move(expr));
	LogicalFilter::SplitPredicates(expressions);
	// push the filters into the combiner
	for (auto &child_expr : expressions) {
		if (combiner.AddFilter(move(child_expr)) == FilterResult::UNSATISFIABLE) {
			return FilterResult::UNSATISFIABLE;
		}
	}
	return FilterResult::SUCCESS;
}

void FilterPushdown::GenerateFilters() {
	if (!filters.empty()) {
		D_ASSERT(!combiner.HasFilters());
		return;
	}
    // combiner有点意思
	combiner.GenerateFilters([&](unique_ptr<Expression> filter) {
		auto f = make_unique<Filter>();
		f->filter = move(filter);
		f->ExtractBindings();
		filters.push_back(move(f));
	});
}

unique_ptr<LogicalOperator> FilterPushdown::FinishPushdown(unique_ptr<LogicalOperator> op) {
	// unhandled type, first perform filter pushdown in its children
	for (auto &child : op->children) {
		FilterPushdown pushdown(optimizer);
		child = pushdown.Rewrite(move(child));
	}
	// now push any existing filters
	if (filters.empty()) {
		// no filters to push
		return op;
	}
	auto filter = make_unique<LogicalFilter>();
	for (auto &f : filters) {
		filter->expressions.push_back(move(f->filter));
	}
	filter->children.push_back(move(op));
	return move(filter);
}

void FilterPushdown::Filter::ExtractBindings() {
	bindings.clear();
	LogicalJoin::GetExpressionBindings(*filter, bindings);
}

} // namespace duckdb

EXPLAIN SELECT 
    coalesce(anon_1.month, anon_2.month) AS month, 
    coalesce(coalesce(CAST(anon_1.value AS REAL), 0.0) + coalesce(CAST(anon_2.value AS REAL), 0.0), 0.0) AS value
FROM (
    SELECT coalesce(anon_3.month, anon_4.month) AS month, 
    coalesce(coalesce(CAST(anon_3.value AS REAL), 0.0) + coalesce(CAST(anon_4.value AS REAL), 0.0), 0.0) AS value
    FROM (
        SELECT month AS month, sum(anon_5.value) AS value
        FROM (
            SELECT date_trunc('month', day) AS month, coalesce(sum(value), 0.0) AS value
            FROM df1
            WHERE CAST(day AS DATE) >= '2022-01-01 00:00:00' 
            AND CAST(day AS DATE) <= '2022-01-31 00:00:00' 
            AND (organization ILIKE 'org4') 
            GROUP BY date_trunc('month', day)
        ) AS anon_5 
        GROUP BY GROUPING SETS((month))
    ) AS anon_3 
    FULL OUTER JOIN (
        SELECT month AS month, sum(anon_6.value) AS value
        FROM (
            SELECT date_trunc('month', day) AS month, coalesce(sum(value), 0.0) AS value
            FROM df2
            WHERE CAST(day AS DATE) >= '2022-01-01 00:00:00' 
            AND CAST(day AS DATE) <= '2022-01-31 00:00:00' 
            GROUP BY date_trunc('month', day)
        ) AS anon_6 
        GROUP BY GROUPING SETS((month))
    ) AS anon_4 ON anon_3.month = anon_4.month
) AS anon_1 
FULL OUTER JOIN (
    SELECT month AS month, sum(anon_7.value) AS value
    FROM (
        SELECT date_trunc('month', day) AS month, coalesce(sum(value), 0.0) AS value
        FROM df3
        WHERE CAST(day AS DATE) >= '2022-01-01 00:00:00' 
        AND CAST(day AS DATE) <= '2022-01-31 00:00:00' 
        GROUP BY date_trunc('month', day)
    ) AS anon_7 
    GROUP BY GROUPING SETS((month))
) AS anon_2 ON anon_1.month = anon_2.month

┌───────────────────────────┐                                                          
│         PROJECTION        │                                                          
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                                                          
│           month           │                                                          
│           value           │                                                          
└─────────────┬─────────────┘                                                                                       
┌─────────────┴─────────────┐                                                          
│         HASH_JOIN         │                                                          
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                                                          
│            FULL           ├───────────────────────────────────────────┐              
│       month = month       │                                           │              
└─────────────┬─────────────┘                                           │                                           
┌─────────────┴─────────────┐                             ┌─────────────┴─────────────┐
│         PROJECTION        │                             │       HASH_GROUP_BY       │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                             │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│           month           │                             │             #0            │
│           value           │                             │          sum(#1)          │
└─────────────┬─────────────┘                             └─────────────┬─────────────┘                             
┌─────────────┴─────────────┐                             ┌─────────────┴─────────────┐
│         HASH_JOIN         │                             │         PROJECTION        │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                             │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│            FULL           ├──────────────┐              │           month           │
│       month = month       │              │              │           value           │
└─────────────┬─────────────┘              │              └─────────────┬─────────────┘                             
┌─────────────┴─────────────┐┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│       HASH_GROUP_BY       ││       HASH_GROUP_BY       ││         PROJECTION        │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│             #0            ││             #0            ││           month           │
│          sum(#1)          ││          sum(#1)          ││           value           │
└─────────────┬─────────────┘└─────────────┬─────────────┘└─────────────┬─────────────┘                             
┌─────────────┴─────────────┐┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│         PROJECTION        ││         PROJECTION        ││       HASH_GROUP_BY       │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│           month           ││           month           ││             #0            │
│           value           ││           value           ││          sum(#1)          │
└─────────────┬─────────────┘└─────────────┬─────────────┘└─────────────┬─────────────┘                             
┌─────────────┴─────────────┐┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│         PROJECTION        ││         PROJECTION        ││         PROJECTION        │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│           month           ││           month           ││  date_trunc('month', day) │
│           value           ││           value           ││           value           │
└─────────────┬─────────────┘└─────────────┬─────────────┘└─────────────┬─────────────┘                             
┌─────────────┴─────────────┐┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│       HASH_GROUP_BY       ││       HASH_GROUP_BY       ││          SEQ_SCAN         │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│             #0            ││             #0            ││            df3            │
│          sum(#1)          ││          sum(#1)          ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│                           ││                           ││            day            │
│                           ││                           ││           value           │
│                           ││                           ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│                           ││                           ││  Filters: day>=2022-01-01 │
│                           ││                           ││ AND day<=2022-01-31 A...  │
│                           ││                           ││         IS NOT NULL       │
└─────────────┬─────────────┘└─────────────┬─────────────┘└───────────────────────────┘                             
┌─────────────┴─────────────┐┌─────────────┴─────────────┐                             
│         PROJECTION        ││         PROJECTION        │                             
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                             
│  date_trunc('month', day) ││  date_trunc('month', day) │                             
│           value           ││           value           │                             
└─────────────┬─────────────┘└─────────────┬─────────────┘                                                          
┌─────────────┴─────────────┐┌─────────────┴─────────────┐                             
│         PROJECTION        ││          SEQ_SCAN         │                             
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                             
│             #0            ││            df2            │                             
│             #2            ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                             
│                           ││            day            │                             
│                           ││           value           │                             
│                           ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                             
│                           ││  Filters: day>=2022-01-01 │                             
│                           ││ AND day<=2022-01-31 A...  │                             
│                           ││         IS NOT NULL       │                             
└─────────────┬─────────────┘└───────────────────────────┘                                                          
┌─────────────┴─────────────┐                                                          
│           FILTER          │                                                          
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                                                          
│ (organization ~~* 'org4') │                                                          
└─────────────┬─────────────┘                                                                                       
┌─────────────┴─────────────┐                                                          
│          SEQ_SCAN         │                                                          
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                                                          
│            df1            │                                                          
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                                                          
│            day            │                                                          
│        organization       │                                                          
│           value           │                                                          
└───────────────────────────┘                


EXPLAIN SELECT 
    coalesce(anon_1.month, anon_2.month) AS month, 
    coalesce(coalesce(CAST(anon_1.value AS REAL), 0.0) + coalesce(CAST(anon_2.value AS REAL), 0.0), 0.0) AS value
FROM (
    SELECT coalesce(anon_3.month, anon_4.month) AS month, 
    coalesce(coalesce(CAST(anon_3.value AS REAL), 0.0) + coalesce(CAST(anon_4.value AS REAL), 0.0), 0.0) AS value
    FROM (
        SELECT month AS month, sum(anon_6.value) AS value
        FROM (
            SELECT date_trunc('month', day) AS month, coalesce(sum(value), 0.0) AS value
            FROM df2
            WHERE CAST(day AS DATE) >= '2022-01-01 00:00:00' 
            AND CAST(day AS DATE) <= '2022-01-31 00:00:00' 
            GROUP BY date_trunc('month', day)
        ) AS anon_6 
        GROUP BY GROUPING SETS((month))
    ) AS anon_3 
    FULL OUTER JOIN (
        SELECT month AS month, sum(anon_5.value) AS value
        FROM (
            SELECT date_trunc('month', day) AS month, coalesce(sum(value), 0.0) AS value
            FROM df1
            WHERE CAST(day AS DATE) >= '2022-01-01 00:00:00' 
            AND CAST(day AS DATE) <= '2022-01-31 00:00:00' 
            AND (organization ILIKE 'org4') 
            GROUP BY date_trunc('month', day)
        ) AS anon_5 
        GROUP BY GROUPING SETS((month))
    ) AS anon_4 ON anon_3.month = anon_4.month
) AS anon_1 
FULL OUTER JOIN (
    SELECT month AS month, sum(anon_7.value) AS value
    FROM (
        SELECT date_trunc('month', day) AS month, coalesce(sum(value), 0.0) AS value
        FROM df3
        WHERE CAST(day AS DATE) >= '2022-01-01 00:00:00' 
        AND CAST(day AS DATE) <= '2022-01-31 00:00:00' 
        GROUP BY date_trunc('month', day)
    ) AS anon_7 
    GROUP BY GROUPING SETS((month))
) AS anon_2 ON anon_1.month = anon_2.month