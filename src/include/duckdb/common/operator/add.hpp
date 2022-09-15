//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/add.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {
// 这个相加是没有溢出检查的, 那些特化的会做溢出检查
struct AddOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return left + right;
	}
};

template <>
float AddOperator::Operation(float left, float right);
template <>
double AddOperator::Operation(double left, double right);
template <>
date_t AddOperator::Operation(date_t left, int32_t right);
template <>
date_t AddOperator::Operation(int32_t left, date_t right);
template <>
timestamp_t AddOperator::Operation(date_t left, dtime_t right);
template <>
timestamp_t AddOperator::Operation(dtime_t left, date_t right);
template <>
interval_t AddOperator::Operation(interval_t left, interval_t right);
template <>
date_t AddOperator::Operation(date_t left, interval_t right);
template <>
date_t AddOperator::Operation(interval_t left, date_t right);
template <>
timestamp_t AddOperator::Operation(timestamp_t left, interval_t right);
template <>
timestamp_t AddOperator::Operation(interval_t left, timestamp_t right);

// 这些不对float之类的处理吗，可能是因为那些对于AddOperator而已已经做了溢出检查?
struct TryAddOperator {
	template <class TA, class TB, class TR>
	static inline bool Operation(TA left, TB right, TR &result) {
		throw InternalException("Unimplemented type for TryAddOperator");
	}
};

template <>
bool TryAddOperator::Operation(uint8_t left, uint8_t right, uint8_t &result);
template <>
bool TryAddOperator::Operation(uint16_t left, uint16_t right, uint16_t &result);
template <>
bool TryAddOperator::Operation(uint32_t left, uint32_t right, uint32_t &result);
template <>
bool TryAddOperator::Operation(uint64_t left, uint64_t right, uint64_t &result);

template <>
bool TryAddOperator::Operation(int8_t left, int8_t right, int8_t &result);
template <>
bool TryAddOperator::Operation(int16_t left, int16_t right, int16_t &result);
template <>
bool TryAddOperator::Operation(int32_t left, int32_t right, int32_t &result);
template <>
DUCKDB_API bool TryAddOperator::Operation(int64_t left, int64_t right, int64_t &result);

// 这个是可以检查溢出情况的
struct AddOperatorOverflowCheck {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		TR result;
		if (!TryAddOperator::Operation(left, right, result)) {
			throw OutOfRangeException("Overflow in addition of %s (%d + %d)!", TypeIdToString(GetTypeId<TA>()), left,
			                          right);
		}
		return result;
	}
};

struct TryDecimalAdd {
	template <class TA, class TB, class TR>
	static inline bool Operation(TA left, TB right, TR &result) {
		throw InternalException("Unimplemented type for TryDecimalAdd");
	}
};
// 注意这个地方会做声明，具体实现在.cpp文件中
template <>
bool TryDecimalAdd::Operation(int16_t left, int16_t right, int16_t &result);
template <>
bool TryDecimalAdd::Operation(int32_t left, int32_t right, int32_t &result);
template <>
bool TryDecimalAdd::Operation(int64_t left, int64_t right, int64_t &result);
template <>
bool TryDecimalAdd::Operation(hugeint_t left, hugeint_t right, hugeint_t &result);
// 有溢出情况的Decimal检查,不检查溢出的话，我觉得可以用上面的AddOperator就行了
struct DecimalAddOverflowCheck {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		TR result;
		if (!TryDecimalAdd::Operation<TA, TB, TR>(left, right, result)) {
            // 这个报错信息看起来不准确
			throw OutOfRangeException("Overflow in addition of DECIMAL(18) (%d + %d). You might want to add an "
			                          "explicit cast to a bigger decimal.",
			                          left, right);
		}
		return result;
	}
};
// hugeint的加法做了特化
template <>
hugeint_t DecimalAddOverflowCheck::Operation(hugeint_t left, hugeint_t right);

struct AddTimeOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right);
};

template <>
dtime_t AddTimeOperator::Operation(dtime_t left, interval_t right);
template <>
dtime_t AddTimeOperator::Operation(interval_t left, dtime_t right);

} // namespace duckdb
