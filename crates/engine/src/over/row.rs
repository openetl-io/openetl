//! A row waiting for the watermark, as `OVER`'s state keeps it: the hash
//! of the schema it's written under, its record ID, then each column's
//! value. Values of the types a group window keeps take a few bytes, floats
//! exactly as they came; a value of any other type is an Arrow IPC stream
//! of its own, of buffers that hold the value alone, so that equal values
//! have equal bytes wherever their batches had them. State keeps each
//! schema rows are written under beside them, by its hash, so a row written
//! before the operator's input changed, as when the source added a column
//! that passes through, is read under the schema it was written with and
//! then as the plan's.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use twox_hash::XxHash3_64;

use super::canonical::canonical;
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

/// Appends value `r` of `c`, of type `t`, as a row keeps it: of the value
/// alone, wherever `c` has it and whatever is beside it.
pub fn value(c: &ArrayRef, t: &DataType, r: usize, out: &mut Vec<u8>) -> Result<(), String> {
    if c.is_null(r) {
        out.push(NULL);
        return Ok(());
    }
    if compact(t) {
        put(Value::read(c.as_ref(), r).as_ref(), out);
        return Ok(());
    }
    let v = c.slice(r, 1);
    // A dictionary's null value, or a union's or a run's, is null too.
    if v.logical_null_count() > 0 {
        out.push(NULL);
        return Ok(());
    }
    let one = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", t.clone(), true)])),
        vec![canonical(v.as_ref(), None).map_err(|e| e.to_string())?],
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
    use arrow_array::builder::{
        BooleanBuilder, Int32Builder, Int64Builder, LargeListBuilder, ListBuilder, MapBuilder,
        StringBuilder, StringDictionaryBuilder,
    };
    use arrow_array::types::{Int8Type, Int32Type, Int64Type};
    use arrow_array::{
        DictionaryArray, FixedSizeListArray, Float64Array, Int32Array, Int64Array,
        LargeStringArray, ListArray, RunArray, StringArray, StringViewArray, StructArray,
        TimestampMicrosecondArray, UnionArray,
    };
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_schema::{Fields, UnionFields};

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

    /// Row `r` of `a` as a row keeps it.
    fn kept(a: &ArrayRef, r: usize) -> Vec<u8> {
        let mut b = Vec::new();
        value(a, a.data_type(), r, &mut b).unwrap();
        b
    }

    fn nulls(valid: &[bool]) -> Option<NullBuffer> {
        Some(NullBuffer::from(valid.to_vec()))
    }

    /// Int64s with `under` under their nulls.
    fn ints(values: &[Option<i64>], under: i64) -> ArrayRef {
        let v: Vec<i64> = values.iter().map(|v| v.unwrap_or(under)).collect();
        let valid: Vec<bool> = values.iter().map(Option::is_some).collect();
        Arc::new(Int64Array::new(v.into(), nulls(&valid)))
    }

    /// Lists of `values` between `offsets`, null where `valid` is false,
    /// whatever their offsets cover there.
    fn list(values: ArrayRef, offsets: &[i32], valid: &[bool]) -> ArrayRef {
        let f = Arc::new(Field::new("item", values.data_type().clone(), true));
        Arc::new(
            ListArray::try_new(
                f,
                OffsetBuffer::new(offsets.to_vec().into()),
                values,
                nulls(valid),
            )
            .unwrap(),
        )
    }

    fn strings(rows: &[Option<Vec<Option<&str>>>]) -> ArrayRef {
        let mut b = ListBuilder::new(StringBuilder::new());
        for r in rows {
            match r {
                Some(items) => {
                    for i in items {
                        b.values().append_option(*i);
                    }
                    b.append(true);
                }
                None => b.append_null(),
            }
        }
        Arc::new(b.finish())
    }

    /// Rows of lists of lists of int32s.
    type Nested = Option<Vec<Option<Vec<Option<i32>>>>>;

    fn nested(rows: &[Nested]) -> ArrayRef {
        let mut b = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
        for r in rows {
            match r {
                Some(lists) => {
                    for l in lists {
                        match l {
                            Some(items) => {
                                for i in items {
                                    b.values().values().append_option(*i);
                                }
                                b.values().append(true);
                            }
                            None => b.values().append_null(),
                        }
                    }
                    b.append(true);
                }
                None => b.append_null(),
            }
        }
        Arc::new(b.finish())
    }

    fn structs(a: ArrayRef, b: ArrayRef, valid: &[bool]) -> ArrayRef {
        let fields = vec![
            Field::new("a", a.data_type().clone(), true),
            Field::new("b", b.data_type().clone(), true),
        ];
        Arc::new(StructArray::try_new(fields.into(), vec![a, b], nulls(valid)).unwrap())
    }

    fn texts(values: &[Option<&str>]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    fn flags(values: &[Option<bool>]) -> ArrayRef {
        let mut b = LargeListBuilder::new(BooleanBuilder::new());
        for v in values.chunks(3) {
            for x in v {
                b.values().append_option(*x);
            }
            b.append(true);
        }
        Arc::new(b.finish())
    }

    fn dictionary(keys: &[Option<i32>], values: &[&str]) -> ArrayRef {
        Arc::new(
            DictionaryArray::try_new(
                Int32Array::from(keys.to_vec()),
                Arc::new(StringArray::from(values.to_vec())),
            )
            .unwrap(),
        )
    }

    fn views(values: &[Option<&str>], before: usize) -> ArrayRef {
        let long = "a value too long to keep in its view".to_owned();
        let mut all: Vec<Option<String>> =
            (0..before).map(|i| Some(format!("{long} {i}"))).collect();
        all.extend(values.iter().map(|v| v.map(str::to_owned)));
        let a = StringViewArray::from_iter(all);
        Arc::new(a.slice(before, values.len()))
    }

    fn sparse(ids: &[i8], ints: ArrayRef, texts: ArrayRef) -> ArrayRef {
        let fields = UnionFields::try_new(
            [0, 1],
            [
                Field::new("i", DataType::Int64, true),
                Field::new("s", DataType::Utf8, true),
            ],
        )
        .unwrap();
        Arc::new(UnionArray::try_new(fields, ids.to_vec().into(), None, vec![ints, texts]).unwrap())
    }

    fn dense(ids: &[i8], offsets: &[i32], ints: ArrayRef, texts: ArrayRef) -> ArrayRef {
        let fields = UnionFields::try_new(
            [0, 1],
            [
                Field::new("i", DataType::Int64, true),
                Field::new("s", DataType::Utf8, true),
            ],
        )
        .unwrap();
        Arc::new(
            UnionArray::try_new(
                fields,
                ids.to_vec().into(),
                Some(offsets.to_vec().into()),
                vec![ints, texts],
            )
            .unwrap(),
        )
    }

    fn runs(ends: &[i32], values: &[Option<&str>]) -> ArrayRef {
        Arc::new(
            RunArray::<Int32Type>::try_new(
                &Int32Array::from(ends.to_vec()),
                &StringArray::from(values.to_vec()),
            )
            .unwrap(),
        )
    }

    /// Arrays of a type each, whose rows hold some values more than once,
    /// in other layouts: nulls at any depth with other bytes under them,
    /// null lists over values, other dictionaries, other buffers. And
    /// whether Arrow's equality tells their values apart as they are.
    fn families() -> Vec<(&'static str, Vec<ArrayRef>, bool)> {
        let int_lists = vec![
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                Some(vec![Some(1), None]),
                Some(vec![Some(2), Some(3)]),
                None,
                Some(vec![]),
                Some(vec![None]),
                Some(vec![
                    Some(4),
                    None,
                    Some(5),
                    None,
                    Some(6),
                    None,
                    Some(7),
                    None,
                ]),
            ])) as ArrayRef,
            // [1, null], null over [9, 9, 9], [], [null, 2], with 77 and 55
            // under the nulls.
            list(
                ints(
                    &[Some(1), None, Some(9), Some(9), Some(9), None, Some(2)],
                    77,
                ),
                &[0, 2, 5, 5, 7],
                &[true, false, true, true],
            ),
            list(ints(&[None, Some(1), None], 55), &[0, 1, 3], &[true, true]),
        ];
        let text_lists = vec![
            strings(&[
                Some(vec![Some("a"), None, Some("")]),
                Some(vec![None]),
                Some(vec![Some(""), Some("a")]),
                None,
                Some(vec![Some("b")]),
            ]),
            strings(&[
                Some(vec![Some(""), Some("a")]),
                Some(vec![Some("a"), None, Some("")]),
                Some(vec![None, None]),
                Some(vec![Some("a, b")]),
                Some(vec![Some("a"), Some("b")]),
            ]),
        ];
        let lists_of_lists = vec![
            nested(&[
                Some(vec![Some(vec![Some(1), None]), None, Some(vec![])]),
                Some(vec![Some(vec![None])]),
                None,
                Some(vec![Some(vec![Some(2)]), Some(vec![Some(3), None])]),
            ]),
            nested(&[
                Some(vec![Some(vec![Some(2)]), Some(vec![Some(3), None])]),
                Some(vec![None]),
                Some(vec![Some(vec![Some(1), None]), None, Some(vec![])]),
                Some(vec![Some(vec![None]), Some(vec![])]),
            ]),
        ];
        let struct_rows = vec![
            structs(
                ints(&[Some(1), None, Some(5), Some(2)], 0),
                texts(&[None, Some("x"), Some("gone"), Some("y")]),
                &[true, true, false, true],
            ),
            structs(
                ints(&[Some(2), None, Some(1), None], 13),
                texts(&[Some("y"), None, None, Some("x")]),
                &[true, false, true, true],
            ),
        ];
        let lists_of_structs = vec![
            list(
                structs(
                    ints(&[Some(1), Some(8), None, Some(2)], 0),
                    texts(&[None, Some("z"), Some("x"), None]),
                    &[true, false, true, true],
                ),
                &[0, 2, 4],
                &[true, true],
            ),
            list(
                structs(
                    ints(&[None, Some(2), Some(1), Some(3)], 21),
                    texts(&[Some("x"), None, None, Some("w")]),
                    &[true, true, true, false],
                ),
                &[0, 2, 4],
                &[true, true],
            ),
        ];
        let item = Arc::new(Field::new("item", DataType::Int64, true));
        let pairs = vec![
            Arc::new(FixedSizeListArray::new(
                item.clone(),
                2,
                ints(
                    &[
                        Some(1),
                        None,
                        Some(5),
                        Some(6),
                        None,
                        None,
                        Some(3),
                        Some(4),
                    ],
                    0,
                ),
                nulls(&[true, false, true, true]),
            )) as ArrayRef,
            Arc::new(FixedSizeListArray::new(
                item,
                2,
                ints(
                    &[Some(3), Some(4), None, None, Some(1), None, None, Some(7)],
                    99,
                ),
                nulls(&[true, true, true, false]),
            )),
        ];
        let mut maps = Vec::new();
        for rows in [
            vec![Some(vec![("a", Some(1)), ("b", None)]), None, Some(vec![])],
            vec![
                Some(vec![]),
                Some(vec![("a", Some(1)), ("b", None)]),
                Some(vec![("b", None)]),
            ],
        ] {
            let mut b = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
            for r in rows {
                match r {
                    Some(entries) => {
                        for (k, v) in entries {
                            b.keys().append_value(k);
                            b.values().append_option(v);
                        }
                        b.append(true).unwrap();
                    }
                    None => b.append(false).unwrap(),
                }
            }
            maps.push(Arc::new(b.finish()) as ArrayRef);
        }
        let booleans = vec![
            flags(&[Some(true), None, Some(false), None, None, Some(true)]),
            flags(&[None, None, Some(true), Some(true), None, Some(false)]),
        ];
        let dictionaries = vec![
            dictionary(
                &[Some(0), Some(1), None, Some(2), Some(1)],
                &["a", "b", "c"],
            ),
            dictionary(&[Some(2), Some(1), Some(0), None], &["c", "b", "a", "z"]),
        ];
        let mut dictionary_lists = Vec::new();
        for (rows, first) in [
            (
                vec![Some(vec![Some("a"), None]), Some(vec![Some("b")])],
                "z",
            ),
            (
                vec![Some(vec![Some("b")]), Some(vec![Some("a"), None])],
                "a",
            ),
        ] {
            let mut b = ListBuilder::new(StringDictionaryBuilder::<Int8Type>::new());
            b.values().append_value(first);
            b.append(true);
            for r in rows.into_iter().flatten() {
                for v in r {
                    b.values().append_option(v);
                }
                b.append(true);
            }
            dictionary_lists.push(Arc::new(b.finish()) as ArrayRef);
        }
        // ["a", null], ["a", "a"] by two keys of equal values, ["b"]; then
        // ["a", "a"] by one key.
        for (keys, values, offsets) in [
            (
                vec![Some(0), None, Some(2), Some(0), Some(1)],
                vec!["a", "b", "a"],
                vec![0, 2, 4, 5],
            ),
            (vec![Some(0), Some(0)], vec!["a"], vec![0, 2]),
        ] {
            let d = DictionaryArray::<Int8Type>::try_new(
                arrow_array::Int8Array::from(keys),
                Arc::new(StringArray::from(values)),
            )
            .unwrap();
            let valid = vec![true; offsets.len() - 1];
            dictionary_lists.push(list(Arc::new(d), &offsets, &valid));
        }
        let view_lists = vec![
            views(
                &[Some("short"), None, Some("a long one, past twelve bytes")],
                0,
            ),
            views(
                &[Some("a long one, past twelve bytes"), Some("short"), None],
                3,
            ),
        ];
        let large = vec![
            Arc::new(LargeStringArray::from(vec![Some("a"), None, Some("")])) as ArrayRef,
            Arc::new(LargeStringArray::from(vec![None, Some(""), Some("a")])),
        ];
        let sparse_unions = vec![
            sparse(
                &[0, 1, 0],
                ints(&[Some(1), Some(8), None], 0),
                texts(&[Some("g"), Some("x"), Some("h")]),
            ),
            sparse(
                &[1, 0, 0],
                ints(&[Some(5), None, Some(1)], 3),
                texts(&[Some("x"), None, Some("k")]),
            ),
        ];
        let dense_unions = vec![
            dense(
                &[0, 1, 0],
                &[0, 0, 1],
                ints(&[Some(1), None], 0),
                texts(&[Some("x")]),
            ),
            dense(
                &[1, 0, 0],
                &[1, 2, 0],
                ints(&[Some(1), Some(7), None], 4),
                texts(&[Some("q"), Some("x")]),
            ),
        ];
        let run_ends = vec![
            runs(&[2, 3, 5], &[Some("a"), None, Some("b")]),
            runs(&[1, 2, 3, 4], &[Some("b"), Some("a"), Some("a"), None]),
        ];
        vec![
            ("list<int64>", int_lists, true),
            ("list<string>", text_lists, true),
            ("list<list<int32>>", lists_of_lists, true),
            ("struct", struct_rows, true),
            ("list<struct>", lists_of_structs, true),
            ("fixed_size_list<int64, 2>", pairs, true),
            ("map<string, int64>", maps, true),
            ("large_list<boolean>", booleans, true),
            ("dictionary<int32, string>", dictionaries, true),
            ("list<dictionary<int8, string>>", dictionary_lists, true),
            ("string_view", view_lists, true),
            ("large_string", large, true),
            ("sparse union", sparse_unions, false),
            ("dense union", dense_unions, false),
            ("run_end_encoded<int32, string>", run_ends, false),
        ]
    }

    /// A value keeps the same bytes wherever it is: alone, sliced, first or
    /// last in a batch, after rows that start it at any bit of a byte, and
    /// whatever its neighbours or the bytes under nulls beside it; and
    /// another value other bytes. So a row read again in another batch has
    /// the hash it had, and its place among its peers.
    #[test]
    fn a_value_is_kept_alike_wherever_it_is() {
        for (name, arrays, compares) in families() {
            let mut all: Vec<(ArrayRef, Vec<u8>)> = Vec::new();
            for (i, a) in arrays.iter().enumerate() {
                let other = &arrays[(i + 1) % arrays.len()];
                for r in 0..a.len() {
                    let want = kept(a, r);
                    let at = |b: &ArrayRef, k: usize, how: &str| {
                        assert!(kept(b, k) == want, "{name}: array {i}'s row {r}, {how}");
                    };
                    let one = a.slice(r, 1);
                    at(&one, 0, "sliced");
                    let index = arrow_array::UInt32Array::from(vec![u32::try_from(r).unwrap()]);
                    at(
                        &arrow_select::take::take(a.as_ref(), &index, None).unwrap(),
                        0,
                        "taken",
                    );
                    for k in 0..=other.len() {
                        let parts = [other.slice(0, k), one.clone(), other.clone()];
                        let refs: Vec<&dyn Array> = parts.iter().map(AsRef::as_ref).collect();
                        let around = arrow_select::concat::concat(&refs).unwrap();
                        at(&around, k, &format!("after {k} rows"));
                        at(
                            &around.slice(k, around.len() - k),
                            0,
                            &format!("after {k}, sliced"),
                        );
                    }
                    // It reads back as it was.
                    let schema = Arc::new(Schema::new(vec![Field::new(
                        "v",
                        a.data_type().clone(),
                        true,
                    )]));
                    let batch = RecordBatch::try_new(schema.clone(), vec![one.clone()]).unwrap();
                    let mut row = Vec::new();
                    encode(&batch, Some(&id(0)), 0, 0, &mut row).unwrap();
                    let (back, _) = decode(&[&row], &schema).unwrap();
                    at(back.column(0), 0, "read back");
                    if compares {
                        assert_eq!(back.column(0).to_data(), one.to_data(), "{name}: read back");
                    }
                    all.push((one, want));
                }
            }
            if !compares {
                continue;
            }
            for (a, x) in &all {
                for (b, y) in &all {
                    assert_eq!(
                        x == y,
                        a.to_data() == b.to_data(),
                        "{name}: {a:?} and {b:?}"
                    );
                }
            }
        }
    }

    /// Values Arrow's equality can't compare as they are: a union's and a
    /// run-end array's.
    #[test]
    fn unions_and_runs_keep_their_values_apart() {
        let rows = |a: &ArrayRef| (0..a.len()).map(|r| kept(a, r)).collect::<Vec<_>>();
        let families = families();
        let get = |name: &str| &families.iter().find(|f| f.0 == name).unwrap().1;
        // 1, "x", null; then "x", null, 1.
        for name in ["sparse union", "dense union"] {
            let (a, b) = (rows(&get(name)[0]), rows(&get(name)[1]));
            assert!(a[0] == b[2] && a[1] == b[0] && a[2] == b[1], "{name}");
            assert!(a[0] != a[1] && a[0] != a[2] && a[1] != a[2], "{name}");
        }
        // a, a, null, b, b; then b, a, a, null.
        let (a, b) = (
            rows(&get("run_end_encoded<int32, string>")[0]),
            rows(&get("run_end_encoded<int32, string>")[1]),
        );
        assert!(a[0] == b[1] && a[1] == b[2] && a[3] == b[0], "runs");
        assert!(a[0] != a[3], "runs");

        // A union's slot under a null struct, or whose value is null, is
        // null whatever its type id: [null, {u: 1}], with the int 5 or the
        // string "junk" under the null struct; and [null], an int's null or
        // a string's.
        for mode in ["sparse", "dense"] {
            let under = |id: i8| {
                let five_and_one = ints(&[Some(5), Some(1)], 0);
                let u = match mode {
                    "sparse" => sparse(&[id, 0], five_and_one, texts(&[Some("junk"), None])),
                    _ => dense(&[id, 0], &[0, 1], five_and_one, texts(&[Some("junk")])),
                };
                let s = structs(u, ints_column(&[7, 8]), &[false, true]);
                list(s, &[0, 2], &[true])
            };
            assert!(
                kept(&under(0), 0) == kept(&under(1), 0),
                "{mode}: under a null struct"
            );
            let null_of = |id: i8| {
                let u = match mode {
                    "sparse" => sparse(&[id], ints(&[None], 3), texts(&[None])),
                    _ => dense(&[id], &[0], ints(&[None], 3), texts(&[None])),
                };
                list(u, &[0, 1], &[true])
            };
            assert!(
                kept(&null_of(0), 0) == kept(&null_of(1), 0),
                "{mode}: a null"
            );
            assert!(kept(&null_of(0), 0) != kept(&under(0), 0), "{mode}");
        }
    }

    fn ints_column(values: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    /// A value is kept under its type exactly, whatever its nested fields'
    /// names, nullability and metadata, a map's sorted flag, a union's type
    /// ids or a timestamp's zone: its one-row batch has the column's type,
    /// and it reads back as that type.
    #[test]
    fn a_value_keeps_its_type_exactly() {
        use arrow_array::types::Int16Type;
        use arrow_array::{MapArray, make_array};
        use arrow_data::ArrayDataBuilder;
        let md = std::collections::HashMap::from([("k".to_owned(), "v".to_owned())]);
        let runs_of = |ends: &[i16], values: &[&str]| -> ArrayRef {
            let t = DataType::RunEndEncoded(
                Arc::new(Field::new("ends", DataType::Int16, false)),
                Arc::new(Field::new("v", DataType::Utf8, false).with_metadata(md.clone())),
            );
            let d = ArrayDataBuilder::new(t)
                .len(usize::try_from(*ends.last().unwrap()).unwrap())
                .add_child_data(arrow_array::Int16Array::from(ends.to_vec()).to_data())
                .add_child_data(StringArray::from(values.to_vec()).to_data())
                .build()
                .unwrap();
            make_array(d)
        };
        let element =
            Arc::new(Field::new("element", DataType::Int64, false).with_metadata(md.clone()));
        let entries = Fields::from(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]);
        let map = MapArray::try_new(
            Arc::new(Field::new("kv", DataType::Struct(entries.clone()), false)),
            OffsetBuffer::new(vec![0, 2, 2].into()),
            StructArray::try_new(
                entries,
                vec![
                    Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![1, 2])),
                ],
                None,
            )
            .unwrap(),
            nulls(&[true, false]),
            true,
        )
        .unwrap();
        let zoned =
            arrow_array::TimestampMillisecondArray::from(vec![1, 2]).with_timezone("+05:30");
        let union_fields = UnionFields::try_new(
            [5, 2],
            [
                Field::new("n", DataType::Int64, false),
                Field::new("t", DataType::Utf8, true).with_metadata(md.clone()),
            ],
        )
        .unwrap();
        let runs_in_list = {
            let r = runs_of(&[2, 3], &["a", "b"]);
            let f = Arc::new(Field::new("x", r.data_type().clone(), false));
            Arc::new(ListArray::try_new(f, OffsetBuffer::new(vec![0, 3].into()), r, None).unwrap())
                as ArrayRef
        };
        let arrays: Vec<ArrayRef> = vec![
            runs_of(&[2, 3], &["a", "b"]),
            runs_in_list,
            Arc::new(
                ListArray::try_new(
                    element.clone(),
                    OffsetBuffer::new(vec![0, 2, 3].into()),
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                    nulls(&[true, false]),
                )
                .unwrap(),
            ),
            Arc::new(FixedSizeListArray::new(
                element,
                2,
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                nulls(&[false, true]),
            )),
            Arc::new(map),
            Arc::new(
                StructArray::try_new(
                    Fields::from(vec![
                        Field::new("a", DataType::Int64, false).with_metadata(md.clone()),
                    ]),
                    vec![Arc::new(Int64Array::from(vec![1, 2]))],
                    nulls(&[true, false]),
                )
                .unwrap(),
            ),
            Arc::new(
                DictionaryArray::<Int16Type>::try_new(
                    arrow_array::Int16Array::from(vec![1, 0]),
                    Arc::new(LargeStringArray::from(vec!["a", "b"])),
                )
                .unwrap(),
            ),
            Arc::new(
                UnionArray::try_new(
                    union_fields,
                    vec![5i8, 2].into(),
                    None,
                    vec![
                        Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                        Arc::new(StringArray::from(vec![Some("a"), None])),
                    ],
                )
                .unwrap(),
            ),
            list(Arc::new(zoned), &[0, 1, 2], &[true, true]),
        ];
        for a in &arrays {
            let t = a.data_type();
            for r in 0..a.len() {
                let one = a.slice(r, 1);
                let copy = canonical(one.as_ref(), None).unwrap_or_else(|e| panic!("{t}: {e}"));
                assert_eq!(copy.data_type(), t);
                let want = kept(a, r);
                let schema = Arc::new(Schema::new(vec![Field::new("v", t.clone(), true)]));
                let batch = RecordBatch::try_new(schema.clone(), vec![one]).unwrap();
                let mut row = Vec::new();
                encode(&batch, Some(&id(0)), 0, 0, &mut row).unwrap();
                let (back, _) = decode(&[&row], &schema).unwrap();
                assert_eq!(back.column(0).data_type(), t);
                assert!(kept(back.column(0), 0) == want, "{t}: row {r} read back");
            }
        }
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
