//! A row's version: the hash of its values, which tells two versions of a
//! record that share an `_openetl_id` apart, and leaves a record read again
//! unchanged with the hash it had.
//!
//! The hash is the xxh3-64 of `openetl.version.v1`, then each column's name
//! and value, by name, of the columns that aren't null. A name, and a
//! scalar value, is a little-endian u32 length, then its bytes. A held
//! cell's value is its text. The metadata columns, `_openetl_*`, and hidden
//! columns don't count. Scalars are encoded as [`crate::keygroup`] encodes a
//! key's, by logical family, so that a widening keeps a row's hash; but
//! times, timestamps and durations are nanoseconds whatever their unit, an
//! interval is its months, days and nanoseconds, and a view is its bytes.
//!
//! Other values are structures, each starting with its tag, a u32 that no
//! length is, and a count as a little-endian u64: a list is [`LIST`], its
//! length, then each element; a struct is [`STRUCT`], its number of fields,
//! then each field's name and value, by name; a map is [`MAP`], its length,
//! then each entry's key and value, in the order of their encodings. A
//! union is [`UNION`], then its variant's name and value. A null within a
//! structure is [`NULL`]. A dictionary's or a run's value is the value it
//! stands for. So the hash is of a row's values alone, whatever their
//! arrays' layout and the rows beside them.
//!
//! A schema's version is the xxh3-64 of `openetl.version.v1` and each
//! counted column's name and family, by name. It changes when a column
//! comes or goes, or changes family, as from a number to a string, which
//! changes the hashes of rows read again unchanged. A structure's family is
//! its tag and its parts' families: a struct's and a union's with their
//! names, by name, and a map's of its key and value.

use std::collections::HashMap;
use std::ops::Range;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Decimal32Type, Decimal64Type, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Int8Type, Int16Type, Int32Type, Int64Type,
    IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType, RunEndIndexType,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, RecordBatch};
use arrow_buffer::ArrowNativeType;
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use twox_hash::XxHash3_64;

use crate::hold::HeldCell;
use crate::keygroup;

const VERSION: &[u8] = b"openetl.version.v1";

/// A null within a structure.
const NULL: u32 = u32::MAX;
const LIST: u32 = u32::MAX - 1;
const STRUCT: u32 = u32::MAX - 2;
const MAP: u32 = u32::MAX - 3;
const UNION: u32 = u32::MAX - 4;

/// A batch's column of its rows' hashes, between the stage and the writer,
/// which takes it off. Its field's metadata has the schema's version, at
/// [`SCHEMA_KEY`].
pub const HASH_COLUMN: &str = "\0hash";
/// The schema's version, as decimal text, in [`HASH_COLUMN`]'s field's
/// metadata.
pub const SCHEMA_KEY: &str = "openetl.version.schema";

/// Whether a column counts in a row's hash: not a metadata column, nor one
/// of the stream's `volatile` columns, whose values can change on every read
/// while the row doesn't.
fn counted(name: &str, volatile: &[String]) -> bool {
    !name.starts_with("_openetl_") && !name.starts_with('\0') && !volatile.iter().any(|v| v == name)
}

/// Each row's hash, of `batch`'s columns but `volatile`, where `cells` are
/// the cells held instead of their values, which the batch has null, by row
/// and column name.
pub fn row_hashes(batch: &RecordBatch, cells: &[HeldCell], volatile: &[String]) -> Vec<u64> {
    let schema = batch.schema();
    let mut columns: Vec<(&Field, &dyn Array)> = schema
        .fields()
        .iter()
        .map(AsRef::as_ref)
        .zip(batch.columns().iter().map(AsRef::as_ref))
        .filter(|(f, _)| counted(f.name(), volatile))
        .collect();
    columns.sort_by(|a, b| a.0.name().cmp(b.0.name()));
    let mut held: HashMap<(usize, &str), &str> = HashMap::new();
    for c in cells {
        held.insert((c.row, c.column.as_str()), c.raw.as_str());
    }
    let mut buf = Vec::new();
    let mut text = String::new();
    let mut out = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        buf.clear();
        buf.extend_from_slice(VERSION);
        for (f, a) in &columns {
            if let Some(raw) = held.get(&(row, f.name().as_str())) {
                keygroup::part(&mut buf, f.name().as_bytes());
                keygroup::part(&mut buf, raw.as_bytes());
                continue;
            }
            if null(*a, row) {
                continue;
            }
            keygroup::part(&mut buf, f.name().as_bytes());
            value(&mut buf, &mut text, keygroup::json(f), *a, row);
        }
        out.push(XxHash3_64::oneshot(&buf));
    }
    out
}

/// The version of a schema, of the columns [`row_hashes`] counts.
pub fn schema_version(schema: &Schema, volatile: &[String]) -> u64 {
    let mut fields: Vec<&Field> = schema
        .fields()
        .iter()
        .map(AsRef::as_ref)
        .filter(|f| counted(f.name(), volatile) && *f.data_type() != DataType::Null)
        .collect();
    fields.sort_by(|a, b| a.name().cmp(b.name()));
    let mut buf = VERSION.to_vec();
    for f in fields {
        keygroup::part(&mut buf, f.name().as_bytes());
        family(&mut buf, f.data_type(), keygroup::json(f));
    }
    XxHash3_64::oneshot(&buf)
}

fn tag(buf: &mut Vec<u8>, t: u32) {
    buf.extend_from_slice(&t.to_le_bytes());
}

fn count(buf: &mut Vec<u8>, n: usize) {
    buf.extend_from_slice(&(n as u64).to_le_bytes());
}

/// Fields by name.
fn by_name<'a>(fields: impl Iterator<Item = &'a Field>) -> Vec<&'a Field> {
    let mut fields: Vec<&Field> = fields.collect();
    fields.sort_by(|a, b| a.name().cmp(b.name()));
    fields
}

/// Encodes a type's family; `json` where its field is JSON.
fn family(buf: &mut Vec<u8>, dt: &DataType, json: bool) {
    use DataType as D;
    let name = match dt {
        D::Boolean => "boolean",
        D::Int8
        | D::Int16
        | D::Int32
        | D::Int64
        | D::UInt8
        | D::UInt16
        | D::UInt32
        | D::UInt64
        | D::Float16
        | D::Float32
        | D::Float64
        | D::Decimal32(..)
        | D::Decimal64(..)
        | D::Decimal128(..)
        | D::Decimal256(..) => "number",
        D::Utf8 | D::LargeUtf8 | D::Utf8View if json => "json",
        D::Utf8
        | D::LargeUtf8
        | D::Utf8View
        | D::Binary
        | D::LargeBinary
        | D::BinaryView
        | D::FixedSizeBinary(_) => "bytes",
        D::Date32 | D::Date64 => "date",
        D::Time32(_) | D::Time64(_) => "time",
        D::Timestamp(..) => "timestamp",
        D::Duration(_) => "duration",
        D::Interval(_) => "interval",
        D::List(f)
        | D::LargeList(f)
        | D::ListView(f)
        | D::LargeListView(f)
        | D::FixedSizeList(f, _) => {
            tag(buf, LIST);
            return family(buf, f.data_type(), keygroup::json(f));
        }
        D::Struct(fields) => {
            tag(buf, STRUCT);
            count(buf, fields.len());
            for f in by_name(fields.iter().map(AsRef::as_ref)) {
                keygroup::part(buf, f.name().as_bytes());
                family(buf, f.data_type(), keygroup::json(f));
            }
            return;
        }
        D::Map(entries, _) => {
            tag(buf, MAP);
            if let D::Struct(kv) = entries.data_type() {
                for f in kv.iter().take(2) {
                    family(buf, f.data_type(), keygroup::json(f));
                }
            }
            return;
        }
        D::Union(fields, _) => {
            tag(buf, UNION);
            count(buf, fields.len());
            for f in by_name(fields.iter().map(|(_, f)| f.as_ref())) {
                keygroup::part(buf, f.name().as_bytes());
                family(buf, f.data_type(), keygroup::json(f));
            }
            return;
        }
        D::Dictionary(_, v) => return family(buf, v, json),
        D::RunEndEncoded(_, v) => {
            return family(buf, v.data_type(), json || keygroup::json(v));
        }
        D::Null => "null",
    };
    keygroup::part(buf, name.as_bytes());
}

/// Whether row `i` of `a` is null: a dictionary's, a run's or a union's
/// where the value it stands for is.
fn null(a: &dyn Array, i: usize) -> bool {
    use DataType as D;
    match a.data_type() {
        D::Null => true,
        D::Dictionary(..) => {
            let d = a.as_any_dictionary();
            d.keys().is_null(i) || null(d.values().as_ref(), key(d.keys(), i))
        }
        D::RunEndEncoded(..) => {
            let (values, p) = run(a, i);
            null(values, p)
        }
        D::Union(..) => {
            let u = a.as_union();
            null(u.child(u.type_id(i)).as_ref(), u.value_offset(i))
        }
        _ => a.is_null(i),
    }
}

/// The index into a dictionary's values of row `i` of its `keys`.
fn key(keys: &dyn Array, i: usize) -> usize {
    use DataType as D;
    match keys.data_type() {
        D::Int8 => keys.as_primitive::<Int8Type>().values()[i].as_usize(),
        D::Int16 => keys.as_primitive::<Int16Type>().values()[i].as_usize(),
        D::Int32 => keys.as_primitive::<Int32Type>().values()[i].as_usize(),
        D::Int64 => keys.as_primitive::<Int64Type>().values()[i].as_usize(),
        D::UInt8 => keys.as_primitive::<UInt8Type>().values()[i].as_usize(),
        D::UInt16 => keys.as_primitive::<UInt16Type>().values()[i].as_usize(),
        D::UInt32 => keys.as_primitive::<UInt32Type>().values()[i].as_usize(),
        _ => keys.as_primitive::<UInt64Type>().values()[i].as_usize(),
    }
}

/// A run-end encoded array's values, and the index among them of row `i`.
fn run(a: &dyn Array, i: usize) -> (&dyn Array, usize) {
    fn of<R: RunEndIndexType>(a: &dyn Array, i: usize) -> (&dyn Array, usize) {
        let r = a.as_run::<R>();
        (r.values().as_ref(), r.get_physical_index(i))
    }
    match a.data_type() {
        DataType::RunEndEncoded(ends, _) if *ends.data_type() == DataType::Int16 => {
            of::<Int16Type>(a, i)
        }
        DataType::RunEndEncoded(ends, _) if *ends.data_type() == DataType::Int32 => {
            of::<Int32Type>(a, i)
        }
        _ => of::<Int64Type>(a, i),
    }
}

/// Encodes row `i` of `a` within a structure, null or not.
fn element(buf: &mut Vec<u8>, text: &mut String, json: bool, a: &dyn Array, i: usize) {
    if null(a, i) {
        tag(buf, NULL);
    } else {
        value(buf, text, json, a, i);
    }
}

/// A list's item field, its values, and the values row `i` holds; None
/// where `a` isn't a list.
fn items(a: &dyn Array, i: usize) -> Option<(&Field, &dyn Array, Range<usize>)> {
    use DataType as D;
    let span = |start: usize, n: usize| start..start + n;
    Some(match a.data_type() {
        D::List(f) => {
            let l = a.as_list::<i32>();
            let o = l.value_offsets();
            (
                f.as_ref(),
                l.values().as_ref(),
                o[i].as_usize()..o[i + 1].as_usize(),
            )
        }
        D::LargeList(f) => {
            let l = a.as_list::<i64>();
            let o = l.value_offsets();
            (
                f.as_ref(),
                l.values().as_ref(),
                o[i].as_usize()..o[i + 1].as_usize(),
            )
        }
        D::ListView(f) => {
            let l = a.as_list_view::<i32>();
            let rows = span(
                l.value_offsets()[i].as_usize(),
                l.value_sizes()[i].as_usize(),
            );
            (f.as_ref(), l.values().as_ref(), rows)
        }
        D::LargeListView(f) => {
            let l = a.as_list_view::<i64>();
            let rows = span(
                l.value_offsets()[i].as_usize(),
                l.value_sizes()[i].as_usize(),
            );
            (f.as_ref(), l.values().as_ref(), rows)
        }
        D::FixedSizeList(f, n) => {
            let l = a.as_fixed_size_list();
            let rows = span(l.value_offset(i).as_usize(), n.as_usize());
            (f.as_ref(), l.values().as_ref(), rows)
        }
        _ => return None,
    })
}

/// Encodes a list of `values`' rows `rows`, whose field is `item`.
fn list(
    buf: &mut Vec<u8>,
    text: &mut String,
    item: &Field,
    values: &dyn Array,
    rows: Range<usize>,
) {
    tag(buf, LIST);
    count(buf, rows.len());
    let json = keygroup::json(item);
    for j in rows {
        element(buf, text, json, values, j);
    }
}

/// Encodes row `i` of `a`, which isn't null; `json` where its field is
/// JSON.
fn value(buf: &mut Vec<u8>, text: &mut String, json: bool, a: &dyn Array, i: usize) {
    use DataType as D;
    use TimeUnit as U;
    let nanos = |v: i64, unit: &U| -> i128 {
        i128::from(v)
            * match unit {
                U::Second => 1_000_000_000,
                U::Millisecond => 1_000_000,
                U::Microsecond => 1_000,
                U::Nanosecond => 1,
            }
    };
    let n = match a.data_type() {
        D::Timestamp(u, _) => nanos(
            match u {
                U::Second => a.as_primitive::<TimestampSecondType>().values()[i],
                U::Millisecond => a.as_primitive::<TimestampMillisecondType>().values()[i],
                U::Microsecond => a.as_primitive::<TimestampMicrosecondType>().values()[i],
                U::Nanosecond => a.as_primitive::<TimestampNanosecondType>().values()[i],
            },
            u,
        ),
        D::Time32(u @ U::Second) => {
            nanos(a.as_primitive::<Time32SecondType>().values()[i].into(), u)
        }
        D::Time32(u) => nanos(
            a.as_primitive::<Time32MillisecondType>().values()[i].into(),
            u,
        ),
        D::Time64(u @ U::Microsecond) => {
            nanos(a.as_primitive::<Time64MicrosecondType>().values()[i], u)
        }
        D::Time64(u) => nanos(a.as_primitive::<Time64NanosecondType>().values()[i], u),
        D::Duration(u) => nanos(
            match u {
                U::Second => a.as_primitive::<DurationSecondType>().values()[i],
                U::Millisecond => a.as_primitive::<DurationMillisecondType>().values()[i],
                U::Microsecond => a.as_primitive::<DurationMicrosecondType>().values()[i],
                U::Nanosecond => a.as_primitive::<DurationNanosecondType>().values()[i],
            },
            u,
        ),
        _ => return other(buf, text, json, a, i),
    };
    keygroup::part(buf, n.to_string().as_bytes());
}

/// Encodes row `i` of `a`, which isn't null, nor a time, a timestamp or a
/// duration.
fn other(buf: &mut Vec<u8>, text: &mut String, json: bool, a: &dyn Array, i: usize) {
    use DataType as D;
    use std::fmt::Write;
    if let Some((item, values, rows)) = items(a, i) {
        return list(buf, text, item, values, rows);
    }
    match a.data_type() {
        dt if keygroup::hashable(dt) => keygroup::value(buf, text, json, a, i),
        D::Decimal32(_, s) | D::Decimal64(_, s) => {
            let digits = match a.data_type() {
                D::Decimal32(..) => a.as_primitive::<Decimal32Type>().values()[i].to_string(),
                _ => a.as_primitive::<Decimal64Type>().values()[i].to_string(),
            };
            text.clear();
            let _ = keygroup::decimal(text, &digits, *s);
            keygroup::part(buf, text.as_bytes());
        }
        D::Utf8View => {
            let s = a
                .as_string_view()
                .iter()
                .nth(i)
                .flatten()
                .unwrap_or_default();
            match json {
                true => keygroup::json_part(buf, s.as_bytes()),
                false => keygroup::part(buf, s.as_bytes()),
            }
        }
        D::BinaryView => {
            let b = a
                .as_binary_view()
                .iter()
                .nth(i)
                .flatten()
                .unwrap_or_default();
            keygroup::part(buf, b);
        }
        D::Interval(unit) => {
            let (months, days, nanos) = match unit {
                IntervalUnit::YearMonth => {
                    (a.as_primitive::<IntervalYearMonthType>().values()[i], 0, 0)
                }
                IntervalUnit::DayTime => {
                    let v = a.as_primitive::<IntervalDayTimeType>().values()[i];
                    (0, v.days, i64::from(v.milliseconds) * 1_000_000)
                }
                IntervalUnit::MonthDayNano => {
                    let v = a.as_primitive::<IntervalMonthDayNanoType>().values()[i];
                    (v.months, v.days, v.nanoseconds)
                }
            };
            text.clear();
            let _ = write!(text, "{months} {days} {nanos}");
            keygroup::part(buf, text.as_bytes());
        }
        D::Struct(_) => {
            let s = a.as_struct();
            let mut fields: Vec<_> = s.fields().iter().zip(s.columns()).collect();
            fields.sort_by(|x, y| x.0.name().cmp(y.0.name()));
            tag(buf, STRUCT);
            count(buf, fields.len());
            for (f, c) in fields {
                keygroup::part(buf, f.name().as_bytes());
                element(buf, text, keygroup::json(f), c.as_ref(), i);
            }
        }
        D::Map(..) => {
            let m = a.as_map();
            let o = m.value_offsets();
            let fields = m.entries().fields();
            let (k, v) = (keygroup::json(&fields[0]), keygroup::json(&fields[1]));
            let mut entries: Vec<Vec<u8>> = (o[i].as_usize()..o[i + 1].as_usize())
                .map(|j| {
                    let mut e = Vec::new();
                    element(&mut e, text, k, m.keys().as_ref(), j);
                    element(&mut e, text, v, m.values().as_ref(), j);
                    e
                })
                .collect();
            entries.sort();
            tag(buf, MAP);
            count(buf, entries.len());
            for e in entries {
                buf.extend_from_slice(&e);
            }
        }
        D::Union(fields, _) => {
            let u = a.as_union();
            let t = u.type_id(i);
            let f = fields.iter().find(|(id, _)| *id == t).map(|(_, f)| f);
            tag(buf, UNION);
            keygroup::part(buf, f.map_or("", |f| f.name()).as_bytes());
            let json = f.is_some_and(|f| keygroup::json(f));
            element(buf, text, json, u.child(t).as_ref(), u.value_offset(i));
        }
        D::Dictionary(..) => {
            let d = a.as_any_dictionary();
            value(buf, text, json, d.values().as_ref(), key(d.keys(), i));
        }
        D::RunEndEncoded(_, v) => {
            let (values, p) = run(a, i);
            value(buf, text, json || keygroup::json(v), values, p);
        }
        // Null, whose rows are null.
        _ => {}
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{
        ArrayBuilder, FixedSizeListBuilder, Int64Builder, LargeListBuilder, ListBuilder,
        MapBuilder, StringBuilder, StructBuilder,
    };
    use arrow_array::types::{Int32Type, Int64Type, IntervalDayTime, IntervalMonthDayNano};
    use arrow_array::{
        ArrayRef, BinaryArray, BinaryViewArray, Decimal32Array, Decimal64Array, Decimal128Array,
        DictionaryArray, Float32Array, Float64Array, Int32Array, Int64Array, IntervalDayTimeArray,
        IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeStringArray, ListArray,
        StringArray, StringViewArray, StructArray, TimestampMicrosecondArray,
        TimestampNanosecondArray, UInt32Array,
    };
    use arrow_schema::{Field, Fields, Schema};
    use arrow_select::concat::concat_batches;
    use arrow_select::take::take_record_batch;

    use crate::{EXTENSION_NAME_KEY, JSON_EXTENSION};

    use super::*;

    fn batch(cols: Vec<(&str, ArrayRef)>) -> RecordBatch {
        let fields: Vec<Field> = cols
            .iter()
            .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
            .collect();
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            cols.into_iter().map(|(_, a)| a).collect(),
        )
        .unwrap()
    }

    #[test]
    fn a_widening_keeps_a_rows_hash_and_its_schemas_version() {
        let narrow = batch(vec![
            ("id", Arc::new(Int32Array::from(vec![1]))),
            ("price", Arc::new(Float32Array::from(vec![1.5]))),
            (
                "amount",
                Arc::new(
                    Decimal128Array::from(vec![150])
                        .with_precision_and_scale(5, 2)
                        .unwrap(),
                ),
            ),
            (
                "at",
                Arc::new(TimestampMicrosecondArray::from(vec![1_000_001])),
            ),
        ]);
        let wide = batch(vec![
            (
                "at",
                Arc::new(TimestampNanosecondArray::from(vec![1_000_001_000])),
            ),
            (
                "amount",
                Arc::new(
                    Decimal128Array::from(vec![15_000])
                        .with_precision_and_scale(10, 4)
                        .unwrap(),
                ),
            ),
            ("id", Arc::new(Int64Array::from(vec![1]))),
            ("price", Arc::new(Float64Array::from(vec![1.5]))),
            ("gained", Arc::new(StringArray::from(vec![None::<&str>]))),
        ]);
        assert_eq!(row_hashes(&narrow, &[], &[]), row_hashes(&wide, &[], &[]));
        let null = |b: &RecordBatch| {
            let s = b.schema();
            Schema::new(
                s.fields()
                    .iter()
                    .filter(|f| f.name() != "gained")
                    .cloned()
                    .collect::<Vec<_>>(),
            )
        };
        assert_eq!(
            schema_version(&narrow.schema(), &[]),
            schema_version(&null(&wide), &[])
        );
        let to_text = batch(vec![("id", Arc::new(StringArray::from(vec!["1"])))]);
        let number = batch(vec![("id", Arc::new(Int64Array::from(vec![1])))]);
        assert_ne!(
            schema_version(&to_text.schema(), &[]),
            schema_version(&number.schema(), &[])
        );
    }

    #[test]
    fn metadata_columns_and_held_cells() {
        let a = batch(vec![
            ("id", Arc::new(Int64Array::from(vec![1, 1]))),
            ("_openetl_synced_at", Arc::new(Int64Array::from(vec![5, 6]))),
            ("v", Arc::new(StringArray::from(vec![None::<&str>, None]))),
        ]);
        let held = |raw: &str| HeldCell {
            row: 0,
            column: "v".into(),
            reason: String::new(),
            raw: raw.into(),
        };
        let h = row_hashes(&a, &[], &[]);
        assert_eq!(h[0], h[1], "a metadata column counted");
        let x = row_hashes(&a, &[held("x")], &[]);
        let y = row_hashes(&a, &[held("y")], &[]);
        assert_ne!(x[0], y[0], "a held cell's value didn't count");
        assert_ne!(x[0], h[0]);
        let shown = batch(vec![
            ("id", Arc::new(Int64Array::from(vec![1]))),
            ("v", Arc::new(StringArray::from(vec!["x"]))),
        ]);
        assert_eq!(
            row_hashes(&shown, &[], &[])[0],
            x[0],
            "a held cell hashed apart from its value"
        );
    }

    #[test]
    fn volatile_columns_dont_count() {
        let a = batch(vec![
            ("id", Arc::new(Int64Array::from(vec![1, 1]))),
            ("fetched_at", Arc::new(Int64Array::from(vec![5, 6]))),
        ]);
        let volatile = ["fetched_at".to_owned()];
        let h = row_hashes(&a, &[], &volatile);
        assert_eq!(h[0], h[1], "a volatile column counted");
        assert_ne!(row_hashes(&a, &[], &[])[0], h[0]);
        let id = batch(vec![("id", Arc::new(Int64Array::from(vec![1])))]);
        assert_eq!(h[0], row_hashes(&id, &[], &[])[0]);
        assert_eq!(
            schema_version(&a.schema(), &volatile),
            schema_version(&id.schema(), &[])
        );
    }

    /// The hash names a version in the state an engine keeps and the IDs
    /// it gives: a change here gives every row read again unchanged an ID
    /// of its own, which lands again.
    #[test]
    fn hashes_are_pinned() {
        let b = batch(vec![
            ("id", Arc::new(Int64Array::from(vec![1]))),
            ("name", Arc::new(StringArray::from(vec!["a"]))),
            (
                "at",
                Arc::new(TimestampMicrosecondArray::from(vec![1_700_000_000_000_000])),
            ),
        ]);
        assert_eq!(row_hashes(&b, &[], &[]), vec![0x7012_9633_2c59_fec6]);
        assert_eq!(schema_version(&b.schema(), &[]), 0x46a5_e4bb_fb9d_be06);

        let mut nested = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
        nested.values().values().append_value(1);
        nested.values().append(true);
        nested.values().append(true);
        nested.values().append(false);
        nested.append(true);
        let mut map = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        map.keys().append_value("k");
        map.values().append_value(1);
        map.keys().append_value("j");
        map.values().append_null();
        map.append(true).unwrap();
        let columns: Vec<(&str, ArrayRef, u64)> = vec![
            (
                "ints",
                Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                    Some(vec![Some(1), None, Some(3)]),
                ])),
                0x4ec5_cbcd_3e44_6bf7,
            ),
            (
                "strings",
                strings(&[Some(vec![Some("a"), Some("b, c")])]),
                0xf0a3_c073_0f48_8a75,
            ),
            ("nested", Arc::new(nested.finish()), 0x4d92_1906_dcec_0fc3),
            (
                "struct",
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(Field::new("a", DataType::Int64, true)),
                        Arc::new(Int64Array::from(vec![None])) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("b", DataType::Utf8, true)),
                        Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
                    ),
                ])),
                0x5b54_d74c_aa59_40d3,
            ),
            ("map", Arc::new(map.finish()), 0x23ea_9ffe_8fa7_ff2f),
            (
                "decimal",
                Arc::new(
                    Decimal128Array::from(vec![1250])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
                0xef7c_e183_7c99_1917,
            ),
            (
                "decimal64",
                Arc::new(
                    Decimal64Array::from(vec![-5])
                        .with_precision_and_scale(12, 3)
                        .unwrap(),
                ),
                0xa98d_2f60_3989_f109,
            ),
            (
                "interval",
                Arc::new(IntervalMonthDayNanoArray::from(vec![
                    IntervalMonthDayNano::new(1, 2, 3),
                ])),
                0x1ca2_4675_03b7_b8c1,
            ),
            (
                "view",
                Arc::new(StringViewArray::from(vec!["view"])),
                0x220d_21fd_0ef8_c922,
            ),
            (
                "binary_view",
                Arc::new(BinaryViewArray::from(vec![&b"\x00\x01"[..]])),
                0xcce2_1819_541a_13ff,
            ),
            (
                "dictionary",
                Arc::new(
                    vec![Some("d")]
                        .into_iter()
                        .collect::<DictionaryArray<Int32Type>>(),
                ),
                0x97a0_6453_53d4_d6e1,
            ),
        ];
        let got: Vec<(&str, u64)> = columns
            .iter()
            .map(|(name, a, _)| (*name, hashes(a.clone())[0]))
            .collect();
        let want: Vec<(&str, u64)> = columns.iter().map(|(name, _, h)| (*name, *h)).collect();
        assert_eq!(got, want);
        let all = batch(columns.iter().map(|(n, a, _)| (*n, a.clone())).collect());
        assert_eq!(schema_version(&all.schema(), &[]), 0xac3c_677e_3fa3_e0e1);
    }

    /// The bytes the hash is of, as the module's comment says.
    #[test]
    fn structures_encode_as_documented() {
        let part = |buf: &mut Vec<u8>, b: &str| keygroup::part(buf, b.as_bytes());
        let tagged = |buf: &mut Vec<u8>, t: u32, n: u64| {
            tag(buf, t);
            buf.extend_from_slice(&n.to_le_bytes());
        };
        let mut want = VERSION.to_vec();
        part(&mut want, "ints");
        tagged(&mut want, LIST, 3);
        part(&mut want, "1");
        tag(&mut want, NULL);
        part(&mut want, "3");
        let ints = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
            Some(1),
            None,
            Some(3),
        ])]);
        let b = batch(vec![("ints", Arc::new(ints))]);
        assert_eq!(row_hashes(&b, &[], &[]), [XxHash3_64::oneshot(&want)]);

        let mut want = VERSION.to_vec();
        part(&mut want, "s");
        tagged(&mut want, STRUCT, 2);
        part(&mut want, "a");
        tag(&mut want, NULL);
        part(&mut want, "b");
        part(&mut want, "x");
        let s = StructArray::from(vec![
            (
                Arc::new(Field::new("b", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("a", DataType::Int64, true)),
                Arc::new(Int64Array::from(vec![None])) as ArrayRef,
            ),
        ]);
        let b = batch(vec![("s", Arc::new(s))]);
        assert_eq!(row_hashes(&b, &[], &[]), [XxHash3_64::oneshot(&want)]);

        // The entry of key j sorts first: its key's encoding is less.
        let mut want = VERSION.to_vec();
        part(&mut want, "m");
        tagged(&mut want, MAP, 2);
        part(&mut want, "j");
        tag(&mut want, NULL);
        part(&mut want, "k");
        part(&mut want, "1");
        let mut m = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        m.keys().append_value("k");
        m.values().append_value(1);
        m.keys().append_value("j");
        m.values().append_null();
        m.append(true).unwrap();
        let b = batch(vec![("m", Arc::new(m.finish()))]);
        assert_eq!(row_hashes(&b, &[], &[]), [XxHash3_64::oneshot(&want)]);
    }

    fn hashes(a: ArrayRef) -> Vec<u64> {
        row_hashes(&batch(vec![("c", a)]), &[], &[])
    }

    fn strings(rows: &[Option<Vec<Option<&str>>>]) -> ArrayRef {
        let mut b = ListBuilder::new(StringBuilder::new());
        for row in rows {
            match row {
                Some(items) => {
                    for i in items {
                        b.values().append_option(*i);
                    }
                    b.append(true);
                }
                None => b.append(false),
            }
        }
        Arc::new(b.finish())
    }

    fn assert_distinct(what: &str, h: &[u64]) {
        for (i, x) in h.iter().enumerate() {
            if let Some(j) = h[..i].iter().position(|y| y == x) {
                panic!("{what}: rows {j} and {i} hash alike");
            }
        }
    }

    /// Values that differ hash differently, at any depth: the elements of a
    /// list, a null and an empty value, and a struct's fields.
    #[test]
    fn nested_values_are_told_apart() {
        let h = hashes(strings(&[
            Some(vec![Some("a, b")]),
            Some(vec![Some("a"), Some("b")]),
            Some(vec![Some("ab")]),
            Some(vec![Some("a"), Some("b"), None]),
            Some(vec![]),
            Some(vec![None]),
            Some(vec![None, None]),
            Some(vec![Some("")]),
            Some(vec![Some("null")]),
            None,
        ]));
        assert_distinct("List<Utf8>", &h);
        assert_eq!(h[9], XxHash3_64::oneshot(VERSION), "a null list counted");

        let mut b = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
        for row in [
            vec![Some(vec![1]), Some(vec![2])],
            vec![Some(vec![1, 2])],
            vec![Some(vec![1]), Some(vec![]), Some(vec![2])],
            vec![Some(vec![1]), None, Some(vec![2])],
            vec![Some(vec![12])],
            vec![],
            vec![None],
            vec![Some(vec![])],
        ] {
            for inner in row {
                match inner {
                    Some(v) => {
                        b.values().values().append_slice(&v);
                        b.values().append(true);
                    }
                    None => b.values().append(false),
                }
            }
            b.append(true);
        }
        assert_distinct("List<List<Int64>>", &hashes(Arc::new(b.finish())));

        let fields = Fields::from(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let pairs = [
            (Some(""), None),
            (None, Some("")),
            (None, None),
            (Some(""), Some("")),
            (Some("x"), None),
            (None, Some("x")),
        ];
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(
                pairs.iter().map(|p| p.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                pairs.iter().map(|p| p.1).collect::<Vec<_>>(),
            )),
        ];
        let s = StructArray::new(fields, columns, None);
        assert_distinct("Struct", &hashes(Arc::new(s)));
    }

    /// The same values hash alike whatever their layout: a dictionary, a
    /// view, a large or fixed-size list, the order of a map's entries or a
    /// struct's fields, a JSON value's keys, a decimal's width and an
    /// interval's unit.
    #[test]
    fn layouts_of_one_value_hash_alike() {
        let utf8: ArrayRef = Arc::new(StringArray::from(vec![Some("x"), Some("yz"), None]));
        let dict: ArrayRef = Arc::new(
            vec![Some("x"), Some("yz"), None]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        );
        let reordered: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(1), Some(0), Some(2)]),
                Arc::new(StringArray::from(vec![Some("yz"), Some("x"), None])),
            )
            .unwrap(),
        );
        let view: ArrayRef = Arc::new(StringViewArray::from(vec![Some("x"), Some("yz"), None]));
        let large: ArrayRef = Arc::new(LargeStringArray::from(vec![Some("x"), Some("yz"), None]));
        for other in [dict, reordered, view, large] {
            assert_eq!(
                hashes(other.clone()),
                hashes(utf8.clone()),
                "{}",
                other.data_type()
            );
        }

        let binary: ArrayRef = Arc::new(BinaryArray::from(vec![
            &b"\x00\x01"[..],
            b"long enough to spill",
        ]));
        let binary_view: ArrayRef = Arc::new(BinaryViewArray::from(vec![
            &b"\x00\x01"[..],
            b"long enough to spill",
        ]));
        assert_eq!(hashes(binary_view), hashes(binary));

        let list = strings(&[Some(vec![Some("a"), None]), Some(vec![]), None]);
        let mut large = LargeListBuilder::new(StringBuilder::new());
        large.values().append_value("a");
        large.values().append_null();
        large.append(true);
        large.append(true);
        large.append(false);
        assert_eq!(hashes(Arc::new(large.finish())), hashes(list.clone()));
        let mut fixed = FixedSizeListBuilder::new(StringBuilder::new(), 2);
        fixed.values().append_value("a");
        fixed.values().append_null();
        fixed.append(true);
        let two = strings(&[Some(vec![Some("a"), None])]);
        assert_eq!(hashes(Arc::new(fixed.finish())), hashes(two));
        let views = arrow_cast::cast(
            &list,
            &DataType::ListView(Arc::new(Field::new_list_field(DataType::Utf8, true))),
        )
        .unwrap();
        assert_eq!(hashes(views), hashes(list));

        let map = |entries: &[(&str, Option<i64>)]| -> ArrayRef {
            let mut b = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
            for (k, v) in entries {
                b.keys().append_value(k);
                b.values().append_option(*v);
            }
            b.append(true).unwrap();
            Arc::new(b.finish())
        };
        assert_eq!(
            hashes(map(&[("a", Some(1)), ("b", None)])),
            hashes(map(&[("b", None), ("a", Some(1))]))
        );
        assert_ne!(
            hashes(map(&[("a", Some(1)), ("b", None)])),
            hashes(map(&[("a", None), ("b", Some(1))]))
        );

        let st = |names: [&str; 2], values: [i64; 2]| -> ArrayRef {
            Arc::new(StructArray::from(vec![
                (
                    Arc::new(Field::new(names[0], DataType::Int64, true)),
                    Arc::new(Int64Array::from(vec![values[0]])) as ArrayRef,
                ),
                (
                    Arc::new(Field::new(names[1], DataType::Int64, true)),
                    Arc::new(Int64Array::from(vec![values[1]])) as ArrayRef,
                ),
            ]))
        };
        assert_eq!(
            hashes(st(["a", "b"], [1, 2])),
            hashes(st(["b", "a"], [2, 1]))
        );
        assert_eq!(
            schema_version(&batch(vec![("c", st(["a", "b"], [1, 2]))]).schema(), &[]),
            schema_version(&batch(vec![("c", st(["b", "a"], [2, 1]))]).schema(), &[])
        );

        let json_item = Arc::new(
            Field::new_list_field(DataType::Utf8, true)
                .with_metadata([(EXTENSION_NAME_KEY.to_owned(), JSON_EXTENSION.to_owned())].into()),
        );
        let json_list = |text: &str| -> ArrayRef {
            let mut b = ListBuilder::new(StringBuilder::new()).with_field(json_item.clone());
            b.values().append_value(text);
            b.append(true);
            Arc::new(b.finish())
        };
        assert_eq!(
            hashes(json_list(r#"{"a": 1, "b": [2]}"#)),
            hashes(json_list(r#"{"b":[2],"a":1}"#))
        );

        let decimal32: ArrayRef = Arc::new(
            Decimal32Array::from(vec![1250])
                .with_precision_and_scale(6, 2)
                .unwrap(),
        );
        let decimal64: ArrayRef = Arc::new(
            Decimal64Array::from(vec![125])
                .with_precision_and_scale(12, 1)
                .unwrap(),
        );
        let decimal128: ArrayRef = Arc::new(
            Decimal128Array::from(vec![125_000])
                .with_precision_and_scale(20, 4)
                .unwrap(),
        );
        assert_eq!(hashes(decimal32.clone()), hashes(decimal128.clone()));
        assert_eq!(hashes(decimal64), hashes(decimal128));

        let months: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![14]));
        let mdn: ArrayRef = Arc::new(IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNano::new(14, 0, 0),
        ]));
        assert_eq!(hashes(months), hashes(mdn.clone()));
        let day_time: ArrayRef = Arc::new(IntervalDayTimeArray::from(vec![IntervalDayTime::new(
            3, 1500,
        )]));
        let as_mdn: ArrayRef = Arc::new(IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNano::new(0, 3, 1_500_000_000),
        ]));
        assert_eq!(hashes(day_time), hashes(as_mdn.clone()));
        assert_ne!(hashes(mdn), hashes(as_mdn));
    }

    fn splitmix(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Random rows of nested columns, with nulls at every depth: a list of
    /// structs of an int and a list of strings, and a map.
    fn nested_rows(n: usize, seed: u64) -> RecordBatch {
        let mut rng = seed;
        let mut r = move |m: u64| splitmix(&mut rng) % m;
        let item = Fields::from(vec![
            Field::new("n", DataType::Int64, true),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                true,
            ),
        ]);
        let mut lists = ListBuilder::new(StructBuilder::from_fields(item, 0));
        let mut maps = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        for _ in 0..n {
            for _ in 0..r(4) {
                let s = lists.values();
                let valid = r(5) != 0;
                let n = (r(3) != 0).then(|| r(7) as i64);
                s.field_builder::<Int64Builder>(0).unwrap().append_option(n);
                let tags = s
                    .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(1)
                    .unwrap();
                for _ in 0..r(3) {
                    let t = tags
                        .values()
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .unwrap();
                    match r(4) {
                        0 => t.append_null(),
                        k => t.append_value("ab".repeat(k as usize - 1)),
                    }
                }
                tags.append(r(4) != 0);
                s.append(valid);
            }
            lists.append(r(6) != 0);
            for k in 0..r(3) {
                maps.keys().append_value(format!("k{k}"));
                maps.values()
                    .append_option((r(2) == 0).then(|| r(3) as i64));
            }
            maps.append(r(6) != 0).unwrap();
        }
        batch(vec![
            ("items", Arc::new(lists.finish())),
            ("attrs", Arc::new(maps.finish())),
        ])
    }

    /// A row's hash is of its own values: the same alone, sliced, at
    /// another place in a batch, beside other rows, and in a batch built
    /// again, whatever the neighbours' nulls in the buffers it shares.
    #[test]
    fn a_rows_hash_is_its_own() {
        let all = nested_rows(200, 7);
        let h = row_hashes(&all, &[], &[]);
        let others = nested_rows(50, 8);
        for (i, &want) in h.iter().enumerate() {
            assert_eq!(
                row_hashes(&all.slice(i, 1), &[], &[]),
                [want],
                "row {i} sliced"
            );
            let j = (i * 7 + 3) % all.num_rows();
            let k = (i * 13 + 1) % all.num_rows();
            let taken =
                take_record_batch(&all, &UInt32Array::from(vec![j as u32, i as u32, k as u32]))
                    .unwrap();
            assert_eq!(
                row_hashes(&taken, &[], &[])[1],
                want,
                "row {i} among {j} and {k}"
            );
            let o = others.slice(i % 40, 10);
            let mixed = concat_batches(&all.schema(), [&o, &all.slice(i, 1), &o]).unwrap();
            assert_eq!(
                row_hashes(&mixed, &[], &[])[10],
                want,
                "row {i} after others"
            );
        }
    }

    /// Rows with equal values hash alike, and rows with other values don't:
    /// random rows, compared as Arrow compares their values.
    #[test]
    fn equal_rows_and_only_they_hash_alike() {
        let all = nested_rows(400, 11);
        let h = row_hashes(&all, &[], &[]);
        let row =
            |i: usize| -> Vec<ArrayRef> { all.columns().iter().map(|c| c.slice(i, 1)).collect() };
        let mut equal = 0;
        for (i, hi) in h.iter().enumerate() {
            for (j, hj) in h[..i].iter().enumerate() {
                let same = row(i)
                    .iter()
                    .zip(row(j))
                    .all(|(a, b)| a.as_ref() == b.as_ref());
                assert_eq!(hi == hj, same, "rows {j} and {i}");
                equal += usize::from(same);
            }
        }
        assert!(equal > 0, "no two rows alike");
    }
}
