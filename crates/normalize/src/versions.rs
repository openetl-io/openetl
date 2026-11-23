//! A row's version: the hash of its values, which tells two versions of a
//! record that share an `_openetl_id` apart, and leaves a record read again
//! unchanged with the hash it had.
//!
//! The hash is the xxh3-64 of `openetl.version.v1`, then each column's name
//! and value, by name, of the columns that aren't null: each a
//! little-endian u32 length, then its bytes. A held cell's value is its
//! text. The metadata columns, `_openetl_*`, and hidden columns don't
//! count. Values are encoded as [`crate::keygroup`] encodes a key's, by
//! logical family, so that a widening keeps a row's hash; but times,
//! timestamps and durations are nanoseconds whatever their unit. A type a
//! key can't have is its Arrow display text.
//!
//! A schema's version is the xxh3-64 of `openetl.version.v1` and each
//! counted column's name and family, by name. It changes when a column
//! comes or goes, or changes family, as from a number to a string, which
//! changes the hashes of rows read again unchanged.

use std::collections::HashMap;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType, DurationSecondType,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow_array::{Array, RecordBatch};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use twox_hash::XxHash3_64;

use crate::hold::HeldCell;
use crate::keygroup;

const VERSION: &[u8] = b"openetl.version.v1";

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
            if a.is_null(row) {
                continue;
            }
            keygroup::part(&mut buf, f.name().as_bytes());
            value(&mut buf, &mut text, f, *a, row);
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
        keygroup::part(&mut buf, family(f).as_bytes());
    }
    XxHash3_64::oneshot(&buf)
}

fn family(f: &Field) -> String {
    use DataType as D;
    match f.data_type() {
        D::Boolean => "boolean".into(),
        dt if dt.is_integer() || dt.is_floating() => "number".into(),
        D::Decimal32(..) | D::Decimal64(..) | D::Decimal128(..) | D::Decimal256(..) => {
            "number".into()
        }
        D::Utf8 | D::LargeUtf8 | D::Utf8View if keygroup::json(f) => "json".into(),
        D::Utf8
        | D::LargeUtf8
        | D::Utf8View
        | D::Binary
        | D::LargeBinary
        | D::BinaryView
        | D::FixedSizeBinary(_) => "bytes".into(),
        D::Date32 | D::Date64 => "date".into(),
        D::Time32(_) | D::Time64(_) => "time".into(),
        D::Timestamp(..) => "timestamp".into(),
        D::Duration(_) => "duration".into(),
        dt => dt.to_string(),
    }
}

/// Encodes row `i` of `a`, of field `f`, which isn't null.
fn value(buf: &mut Vec<u8>, text: &mut String, f: &Field, a: &dyn Array, i: usize) {
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
        D::Timestamp(u, _) => Some(nanos(
            match u {
                U::Second => a.as_primitive::<TimestampSecondType>().values()[i],
                U::Millisecond => a.as_primitive::<TimestampMillisecondType>().values()[i],
                U::Microsecond => a.as_primitive::<TimestampMicrosecondType>().values()[i],
                U::Nanosecond => a.as_primitive::<TimestampNanosecondType>().values()[i],
            },
            u,
        )),
        D::Time32(u @ U::Second) => Some(nanos(
            a.as_primitive::<Time32SecondType>().values()[i].into(),
            u,
        )),
        D::Time32(u) => Some(nanos(
            a.as_primitive::<Time32MillisecondType>().values()[i].into(),
            u,
        )),
        D::Time64(u @ U::Microsecond) => Some(nanos(
            a.as_primitive::<Time64MicrosecondType>().values()[i],
            u,
        )),
        D::Time64(u) => Some(nanos(
            a.as_primitive::<Time64NanosecondType>().values()[i],
            u,
        )),
        D::Duration(u) => Some(nanos(
            match u {
                U::Second => a.as_primitive::<DurationSecondType>().values()[i],
                U::Millisecond => a.as_primitive::<DurationMillisecondType>().values()[i],
                U::Microsecond => a.as_primitive::<DurationMicrosecondType>().values()[i],
                U::Nanosecond => a.as_primitive::<DurationNanosecondType>().values()[i],
            },
            u,
        )),
        _ => None,
    };
    if let Some(n) = n {
        keygroup::part(buf, n.to_string().as_bytes());
        return;
    }
    if keygroup::hashable(a.data_type()) {
        keygroup::value(buf, text, f, a, i);
        return;
    }
    text.clear();
    if let Ok(fmt) = ArrayFormatter::try_new(a, &FormatOptions::new()) {
        text.push_str(&fmt.value(i).to_string());
    }
    keygroup::part(buf, text.as_bytes());
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
        TimestampMicrosecondArray, TimestampNanosecondArray,
    };
    use arrow_schema::{Field, Schema};

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
    }
}
