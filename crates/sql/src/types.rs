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

/// The type to write in `CAST(… AS type)` for a statement to give an output
/// column `f`'s logical type; `None` for one no cast gives, such as JSON's,
/// a UUID's or a timestamp with a time zone.
pub fn cast_type(f: &Field) -> Option<String> {
    use DataType as T;
    if f.metadata().contains_key("ARROW:extension:name") {
        return None;
    }
    Some(match f.data_type() {
        T::Boolean => "BOOLEAN".into(),
        T::Int16 => "SMALLINT".into(),
        T::Int32 => "INTEGER".into(),
        T::Int64 => "BIGINT".into(),
        T::Float32 => "REAL".into(),
        T::Float64 => "DOUBLE".into(),
        T::Utf8 => "VARCHAR".into(),
        T::Binary => "BYTEA".into(),
        T::Date32 => "DATE".into(),
        T::Timestamp(TimeUnit::Microsecond, None) => "TIMESTAMP(6)".into(),
        T::Timestamp(TimeUnit::Nanosecond, None) => "TIMESTAMP(9)".into(),
        T::Decimal128(p, s) => format!("DECIMAL({p}, {s})"),
        _ => return None,
    })
}

/// A column's name as a statement writes it: as it is where it's a plain
/// lower-case identifier that isn't a keyword, and otherwise in double
/// quotes, with each one inside doubled.
pub fn ident(name: &str) -> String {
    let mut chars = name.chars();
    let plain = chars
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && datafusion_sql::sqlparser::keywords::ALL_KEYWORDS
            .binary_search(&name.to_ascii_uppercase().as_str())
            .is_err();
    match plain {
        true => name.to_owned(),
        false => format!("\"{}\"", name.replace('"', "\"\"")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A name ident writes reads back as the column it names.
    #[test]
    fn idents_name_their_columns() {
        assert_eq!(ident("email"), "email");
        assert_eq!(ident("select"), "\"select\"");
        let names = ["email", "Email Address", "select", "a\"b", "9lives", "Ünï"];
        let mut catalog = crate::Catalog::new();
        let fields: Vec<Field> = names
            .iter()
            .map(|n| Field::new(*n, DataType::Utf8, true))
            .collect();
        let stream = crate::Stream {
            schema: Arc::new(arrow_schema::Schema::new(fields)),
            key: Vec::new(),
            changelog: false,
        };
        catalog
            .insert("s", stream)
            .unwrap_or_else(|e| panic!("{e}"));
        for n in names {
            let sql = format!(
                "INSERT INTO o SELECT CAST({} AS VARCHAR) AS {} FROM s",
                ident(n),
                ident(n)
            );
            let plan = crate::plan(&sql, &catalog).unwrap_or_else(|e| panic!("{sql}: {e}"));
            assert_eq!(plan.statements[0].schema.field(0).name(), n, "{sql}");
        }
    }

    /// Each type cast_type names gives the output column the type it was
    /// asked for.
    #[test]
    fn casts_give_the_type_they_name() {
        let ts = |u, tz: Option<&str>| DataType::Timestamp(u, tz.map(Into::into));
        let types = [
            DataType::Boolean,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::Binary,
            DataType::Date32,
            ts(TimeUnit::Microsecond, None),
            ts(TimeUnit::Nanosecond, None),
            DataType::Decimal128(12, 2),
        ];
        let mut catalog = crate::Catalog::new();
        let schema = arrow_schema::Schema::new(vec![Field::new("x", DataType::Utf8, true)]);
        let stream = crate::Stream {
            schema: Arc::new(schema),
            key: Vec::new(),
            changelog: false,
        };
        catalog
            .insert("s", stream)
            .unwrap_or_else(|e| panic!("{e}"));
        for t in types {
            let name = cast_type(&Field::new("x", t.clone(), true))
                .unwrap_or_else(|| panic!("{t} has no cast"));
            let sql = format!("INSERT INTO o SELECT CAST(x AS {name}) AS x FROM s");
            let plan = crate::plan(&sql, &catalog).unwrap_or_else(|e| panic!("{sql}: {e}"));
            let got = plan.statements[0].schema.field(0).data_type().clone();
            assert_eq!(got, t, "{sql}");
        }
        let json = Field::new("j", DataType::Utf8, true)
            .with_metadata([("ARROW:extension:name".to_owned(), "arrow.json".to_owned())].into());
        assert_eq!(cast_type(&json), None);
        assert_eq!(
            cast_type(&Field::new(
                "t",
                ts(TimeUnit::Microsecond, Some("UTC")),
                true
            )),
            None
        );
    }

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
