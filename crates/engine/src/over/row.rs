//! A row waiting for the watermark, as `OVER`'s state keeps it: the hash
//! of the schema it's written under, its record ID, then each column's
//! value. Values of the types a group window keeps take a few bytes, floats
//! exactly as they came; a value of any other type is an Arrow IPC stream
//! of its own. State keeps each schema rows are written under beside them,
//! by its hash, so a row written before the operator's input changed, as
//! when the source added a column that passes through, is read under the
//! schema it was written with and then as the plan's.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use twox_hash::XxHash3_64;

use crate::window::codec::{self, Value, take};

const NULL: u8 = 0;
const FALSE: u8 = 1;
const TRUE: u8 = 2;
const INT: u8 = 3;
const FLOAT: u8 = 4;
const DECIMAL: u8 = 5;
const BYTES: u8 = 6;
/// A value of a type the others don't keep, as an Arrow IPC stream.
const OTHER: u8 = 7;

/// Casts that fail where a value doesn't fit, rather than give null.
const STRICT: arrow_cast::CastOptions<'static> = arrow_cast::CastOptions {
    safe: false,
    format_options: arrow_cast::display::FormatOptions::new(),
};

/// A schema's hash, as rows name it, and the schema as state keeps it.
pub fn fingerprint(schema: &Schema) -> Result<(u64, Vec<u8>), String> {
    let ipc = crate::ipc::encode_schema(schema).map_err(|e| e.to_string())?;
    Ok((XxHash3_64::oneshot(&ipc), ipc))
}

/// The hash of the schema a row is written under.
pub fn schema_of(row: &[u8]) -> Result<u64, String> {
    let mut at = row;
    Ok(u64::from_be_bytes(take(&mut at)?))
}

/// What a row is ordered by among its key's rows of one event time: its
/// record ID, then its values.
pub fn tie(row: &[u8]) -> &[u8] {
    row.get(8..).unwrap_or_default()
}

/// A row's values, without its schema's hash and its record ID.
pub fn values(row: &[u8]) -> &[u8] {
    row.get(24..).unwrap_or_default()
}

/// Whether a column of type `t` keeps its values in a few bytes, as
/// [`codec::build`] gives them back.
fn compact(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Boolean
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Date32
            | DataType::Time64(TimeUnit::Microsecond)
            | DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, _)
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(..)
            | DataType::Utf8
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
    )
}

/// Appends `v` exactly: a float keeps its sign and its NaN's bits.
pub fn put(v: Option<&Value>, out: &mut Vec<u8>) {
    match v {
        None => out.push(NULL),
        Some(Value::Bool(false)) => out.push(FALSE),
        Some(Value::Bool(true)) => out.push(TRUE),
        Some(Value::Int(i)) => {
            out.push(INT);
            out.extend_from_slice(&i.to_be_bytes());
        }
        Some(Value::Float(f)) => {
            out.push(FLOAT);
            out.extend_from_slice(&f.to_bits().to_be_bytes());
        }
        Some(Value::Decimal(d)) => {
            out.push(DECIMAL);
            out.extend_from_slice(&d.to_be_bytes());
        }
        Some(Value::Bytes(b)) => {
            out.push(BYTES);
            out.extend_from_slice(&codec::len32(b.len()).to_be_bytes());
            out.extend_from_slice(b);
        }
    }
}

/// Appends value `r` of `c`, of type `t`, as a row keeps it.
pub fn value(c: &ArrayRef, t: &DataType, r: usize, out: &mut Vec<u8>) -> Result<(), String> {
    if c.is_null(r) {
        out.push(NULL);
        return Ok(());
    }
    if compact(t) {
        put(Value::read(c.as_ref(), r).as_ref(), out);
        return Ok(());
    }
    let one = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", t.clone(), true)])),
        vec![c.slice(r, 1)],
    )
    .map_err(|e| e.to_string())?;
    let ipc = crate::ipc::encode_batch(&one).map_err(|e| e.to_string())?;
    out.push(OTHER);
    out.extend_from_slice(&codec::len32(ipc.len()).to_be_bytes());
    out.extend_from_slice(&ipc);
    Ok(())
}

/// Appends row `r` of `batch`, written under the schema of hash `schema`,
/// with its record ID, `id`, of 16 bytes.
pub fn encode(
    batch: &RecordBatch,
    id: Option<&[u8]>,
    r: usize,
    schema: u64,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    out.extend_from_slice(&schema.to_be_bytes());
    match id {
        Some(id) if id.len() == 16 => out.extend_from_slice(id),
        _ => return Err(format!("row {r} has no record ID")),
    }
    for (c, f) in batch.columns().iter().zip(batch.schema().fields()) {
        value(c, f.data_type(), r, out)?;
    }
    Ok(())
}

/// One column's values, as rows have them.
enum Column {
    Compact(Vec<Option<Value>>),
    Other(Vec<ArrayRef>),
}

/// Rows written under `schema`, as a batch of it, with their record IDs.
pub fn decode(rows: &[&[u8]], schema: &SchemaRef) -> Result<(RecordBatch, Vec<[u8; 16]>), String> {
    let fields = schema.fields();
    let mut columns: Vec<Column> = fields
        .iter()
        .map(|f| match compact(f.data_type()) {
            true => Column::Compact(Vec::with_capacity(rows.len())),
            false => Column::Other(Vec::with_capacity(rows.len())),
        })
        .collect();
    let mut ids = Vec::with_capacity(rows.len());
    for row in rows {
        let mut at = *row;
        let _: [u8; 8] = take(&mut at)?;
        ids.push(take::<16>(&mut at)?);
        for (c, f) in columns.iter_mut().zip(fields) {
            match c {
                Column::Compact(vs) => vs.push(codec::decode(&mut at)?),
                Column::Other(arrays) => {
                    let [tag] = take::<1>(&mut at)?;
                    arrays.push(match tag {
                        NULL => arrow_array::new_null_array(f.data_type(), 1),
                        OTHER => {
                            let n = u32::from_be_bytes(take(&mut at)?);
                            let n = usize::try_from(n).map_err(|e| e.to_string())?;
                            if at.len() < n {
                                return Err("a value past the end".into());
                            }
                            let (ipc, rest) = at.split_at(n);
                            at = rest;
                            let b = crate::ipc::decode_batch(ipc.to_vec())
                                .map_err(|e| e.to_string())?;
                            match b.num_columns() == 1 && b.num_rows() == 1 {
                                true => b.column(0).clone(),
                                false => return Err("a value of more than one".into()),
                            }
                        }
                        other => return Err(format!("a value's tag {other}")),
                    });
                }
            }
        }
        if !at.is_empty() {
            return Err("a row with bytes past its values".into());
        }
    }
    let mut arrays = Vec::with_capacity(fields.len());
    for (c, f) in columns.into_iter().zip(fields) {
        arrays.push(match c {
            Column::Compact(vs) => codec::build(f.data_type(), &vs)?,
            Column::Other(parts) if parts.is_empty() => arrow_array::new_empty_array(f.data_type()),
            Column::Other(parts) => {
                let refs: Vec<&dyn Array> = parts.iter().map(|a| a.as_ref()).collect();
                arrow_select::concat::concat(&refs).map_err(|e| e.to_string())?
            }
        });
    }
    let loose = Arc::new(Schema::new_with_metadata(
        fields
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ));
    let batch = RecordBatch::try_new_with_options(
        loose,
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )
    .map_err(|e| e.to_string())?;
    Ok((batch, ids))
}

/// `batch` as a batch of `to`: its columns by name, cast where their type
/// changed, and null where it lacks one. Gives the column that can't be
/// read as `to` has it, and why, where one can't, as where a value doesn't
/// fit the new type: no value becomes null in a cast.
pub fn conform(batch: &RecordBatch, to: &SchemaRef) -> Result<RecordBatch, (String, String)> {
    let n = batch.num_rows();
    let mut columns = Vec::with_capacity(to.fields().len());
    for f in to.fields() {
        columns.push(match batch.column_by_name(f.name()) {
            None => arrow_array::new_null_array(f.data_type(), n),
            Some(c) if c.data_type() == f.data_type() => c.clone(),
            Some(c) => arrow_cast::cast_with_options(c, f.data_type(), &STRICT).map_err(|e| {
                (
                    f.name().clone(),
                    format!("from {} to {}: {e}", c.data_type(), f.data_type()),
                )
            })?,
        });
    }
    let loose = Arc::new(Schema::new_with_metadata(
        to.fields()
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
        to.metadata().clone(),
    ));
    RecordBatch::try_new_with_options(
        loose,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(n)),
    )
    .map_err(|e| (String::new(), e.to_string()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use arrow_array::builder::{ListBuilder, StringBuilder};
    use arrow_array::{Float64Array, Int32Array, StringArray, TimestampMicrosecondArray};

    fn id(i: usize) -> [u8; 16] {
        [u8::try_from(i).unwrap(); 16]
    }

    /// Every value reads back as it was written: floats with their sign,
    /// lists through IPC, nulls of either kind.
    #[test]
    fn rows_read_back_as_written() {
        let mut tags = ListBuilder::new(StringBuilder::new());
        tags.values().append_value("a");
        tags.append(true);
        tags.append_null();
        tags.values().append_value("b");
        tags.values().append_value("c");
        tags.append(true);
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int32, true),
            Field::new("f", DataType::Float64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new(
                "at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new_list("tags", Field::new("item", DataType::Utf8, true), true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None, Some(-3)])),
                Arc::new(Float64Array::from(vec![Some(-0.0), Some(f64::NAN), None])),
                Arc::new(StringArray::from(vec![Some("x"), Some(""), None])),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(5), Some(-7), None])
                        .with_timezone("UTC"),
                ),
                Arc::new(tags.finish()),
            ],
        )
        .unwrap();
        let (fp, _) = fingerprint(&schema).unwrap();
        let rows: Vec<Vec<u8>> = (0..3)
            .map(|r| {
                let mut b = Vec::new();
                encode(&batch, Some(&id(r)), r, fp, &mut b).unwrap();
                b
            })
            .collect();
        assert_eq!(schema_of(&rows[1]).unwrap(), fp);
        let refs: Vec<&[u8]> = rows.iter().map(Vec::as_slice).collect();
        let (got, got_ids) = decode(&refs, &schema).unwrap();
        assert_eq!(got_ids, [[0; 16], [1; 16], [2; 16]]);
        for (a, b) in got.columns().iter().zip(batch.columns()) {
            assert_eq!(a.to_data(), b.to_data());
        }
        let f: Vec<Option<f64>> = got
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .collect();
        assert!(f[0].unwrap().is_sign_negative() && f[1].unwrap().is_nan());
    }

    /// A row written before a column passed through was added, and another
    /// widened, reads as the plan has them now.
    #[test]
    fn a_row_conforms_to_a_changed_schema() {
        let old = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int32, true),
            Field::new("gone", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            old,
            vec![
                Arc::new(Int32Array::from(vec![7])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        )
        .unwrap();
        let new = Arc::new(Schema::new(vec![
            Field::new("added", DataType::Utf8, true),
            Field::new("n", DataType::Int64, true),
        ]));
        let got = conform(&batch, &new).unwrap();
        assert!(got.column(0).is_null(0));
        let n: Vec<Option<i64>> = got
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(n, [Some(7)]);
        let binary = Arc::new(Schema::new(vec![Field::new(
            "n",
            DataType::Struct(vec![Field::new("x", DataType::Int64, true)].into()),
            true,
        )]));
        let (column, _) = conform(&batch, &binary).unwrap_err();
        assert_eq!(column, "n");
    }

    /// A value that doesn't fit a narrower type fails the cast, rather than
    /// reading as null.
    #[test]
    fn a_value_that_doesnt_fit_its_new_type_fails() {
        let wide = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            wide,
            vec![Arc::new(arrow_array::Int64Array::from(vec![2_147_483_648]))],
        )
        .unwrap();
        let narrow = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, true)]));
        let (column, why) = conform(&batch, &narrow).unwrap_err();
        assert_eq!(column, "n");
        assert!(why.starts_with("from Int64 to Int32"), "{why}");
    }
}
