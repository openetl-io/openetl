//! The types an output's columns can have: the engine's logical types.

use std::sync::Arc;

use arrow_schema::{DataType, Field, IntervalUnit, TimeUnit};

/// The type an output column of type `t` has: `t` where it's a logical type,
/// or else one that holds its values. `None` where there's no such type, and
/// the statement has to cast.
pub(crate) fn output(t: &DataType) -> Option<DataType> {
    use DataType as T;
    Some(match t {
        T::Boolean
        | T::Int16
        | T::Int32
        | T::Int64
        | T::Float32
        | T::Float64
        | T::Utf8
        | T::Binary
        | T::Date32
        | T::Time64(TimeUnit::Microsecond)
        | T::Interval(IntervalUnit::MonthDayNano)
        | T::FixedSizeBinary(16) => t.clone(),
        T::Decimal128(p, s) if (1..=38).contains(p) && *s >= 0 && s.unsigned_abs() <= *p => {
            t.clone()
        }
        T::Decimal32(p, s) | T::Decimal64(p, s) => output(&T::Decimal128(*p, *s))?,
        T::Int8 | T::UInt8 => T::Int16,
        T::UInt16 => T::Int32,
        T::UInt32 => T::Int64,
        // Lengths, counts and positions: none passes Int64, and one that did
        // would fail its batch rather than wrap.
        T::UInt64 => T::Int64,
        T::Float16 => T::Float32,
        T::Utf8View | T::LargeUtf8 => T::Utf8,
        T::BinaryView | T::LargeBinary => T::Binary,
        T::Timestamp(unit, tz) => T::Timestamp(
            match unit {
                TimeUnit::Nanosecond => TimeUnit::Nanosecond,
                _ => TimeUnit::Microsecond,
            },
            tz.as_ref().map(|_| "UTC".into()),
        ),
        T::List(f) | T::LargeList(f) | T::FixedSizeList(f, _) => T::List(Arc::new(Field::new(
            f.name(),
            output(f.data_type())?,
            f.is_nullable(),
        ))),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn types_map_to_logical_ones_or_none() {
        let ts = |u, tz: Option<&str>| DataType::Timestamp(u, tz.map(Into::into));
        let cases = [
            (DataType::Utf8View, Some(DataType::Utf8)),
            (DataType::UInt64, Some(DataType::Int64)),
            (
                ts(TimeUnit::Second, Some("+00:00")),
                Some(ts(TimeUnit::Microsecond, Some("UTC"))),
            ),
            (
                ts(TimeUnit::Nanosecond, None),
                Some(ts(TimeUnit::Nanosecond, None)),
            ),
            (
                DataType::Decimal128(12, 2),
                Some(DataType::Decimal128(12, 2)),
            ),
            (DataType::Decimal128(12, -2), None),
            (DataType::Null, None),
            (DataType::Time64(TimeUnit::Nanosecond), None),
            (DataType::Struct(Vec::<Field>::new().into()), None),
        ];
        for (t, want) in cases {
            assert_eq!(output(&t), want, "{t}");
        }
    }
}
