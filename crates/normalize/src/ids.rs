//! Record IDs. The same version of a record gets the same `_openetl_id` on
//! every read, which is what dedupe downstream relies on.
//!
//! An ID is a version 8 UUID made from the xxh3-128 hash of:
//! - `openetl.id.v1`, then the stream name;
//! - for a keyed stream, `0x01` and each primary-key value in key order;
//!   otherwise `0x02` and each non-null column's name and value, sorted by
//!   name;
//! - the record's sequence: its cursor value, or the stream's read start.
//!
//! Each name and value is a little-endian u32 length, then its bytes; a null
//! is the length `u32::MAX` alone. Raw rows encode values as JSON text, with
//! object keys sorted, before inference, so a column widened between two
//! reads doesn't change an ID.

use arrow_array::cast::AsArray;
use arrow_array::types::ByteArrayType;
use arrow_array::types::{
    Date32Type, Date64Type, Decimal128Type, Decimal256Type, Float16Type, Float32Type, Float64Type,
    Int8Type, Int16Type, Int32Type, Int64Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef, FixedSizeBinaryArray, GenericByteArray, RecordBatch};
use arrow_buffer::ArrowNativeType;
use arrow_schema::{ArrowError, DataType, TimeUnit};
use serde_json::{Map, Value};
use twox_hash::XxHash3_128;

const VERSION: &[u8] = b"openetl.id.v1";
const KEYED: u8 = 1;
const UNKEYED: u8 = 2;
const NULL: u32 = u32::MAX;

/// What a stream's record IDs and sequence come from. Column names are the
/// source's.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Identity {
    pub stream: String,
    /// Empty for an unkeyed stream.
    pub key: Vec<String>,
    pub cursor: Option<String>,
    /// Microseconds since the Unix epoch: the sequence of a stream without a
    /// cursor.
    pub read_start: i64,
}

impl Identity {
    fn start(&self, buf: &mut Vec<u8>) {
        buf.clear();
        buf.extend_from_slice(VERSION);
        part(buf, Some(self.stream.as_bytes()));
        buf.push(if self.key.is_empty() { UNKEYED } else { KEYED });
    }

    fn read_start_part(&self, buf: &mut Vec<u8>) {
        part(buf, Some(self.read_start.to_string().as_bytes()));
    }

    /// The ID of one raw row.
    pub(crate) fn raw(&self, row: &Map<String, Value>, buf: &mut Vec<u8>) -> [u8; 16] {
        self.start(buf);
        if self.key.is_empty() {
            let mut fields: Vec<(&String, &Value)> =
                row.iter().filter(|(_, v)| !v.is_null()).collect();
            fields.sort_by(|a, b| a.0.cmp(b.0));
            for (name, v) in fields {
                part(buf, Some(name.as_bytes()));
                json_part(buf, Some(v));
            }
        } else {
            for k in &self.key {
                json_part(buf, row.get(k));
            }
        }
        match &self.cursor {
            Some(c) => json_part(buf, row.get(c)),
            None => self.read_start_part(buf),
        }
        uuid(buf)
    }

    /// The IDs of a typed batch's rows.
    pub(crate) fn typed(&self, batch: &RecordBatch) -> Result<FixedSizeBinaryArray, ArrowError> {
        let schema = batch.schema();
        let parts: Vec<(&str, &ArrayRef)> = if self.key.is_empty() {
            let mut all: Vec<_> = schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .zip(batch.columns())
                .collect();
            all.sort_by(|a, b| a.0.cmp(b.0));
            all
        } else {
            let mut key = Vec::with_capacity(self.key.len());
            for k in &self.key {
                let i = schema.index_of(k).map_err(|_| {
                    ArrowError::SchemaError(format!("the primary key column {k:?} is missing"))
                })?;
                key.push((k.as_str(), batch.column(i)));
            }
            key
        };
        let cursor = self.cursor.as_ref().map(|c| {
            schema
                .index_of(c)
                .ok()
                .map(|i| (c.as_str(), batch.column(i)))
        });
        for (name, a) in parts.iter().chain(cursor.iter().flatten()) {
            if !supported(a.data_type()) {
                return Err(ArrowError::NotYetImplemented(format!(
                    "a record ID from column {name:?} of type {}",
                    a.data_type()
                )));
            }
        }

        let mut buf = Vec::new();
        let mut text = String::new();
        let mut ids = Vec::with_capacity(batch.num_rows() * 16);
        for row in 0..batch.num_rows() {
            self.start(&mut buf);
            for (name, a) in &parts {
                if self.key.is_empty() {
                    if a.is_null(row) {
                        continue;
                    }
                    part(&mut buf, Some(name.as_bytes()));
                }
                typed_part(&mut buf, &mut text, a.as_ref(), row);
            }
            match cursor {
                Some(Some((_, a))) => typed_part(&mut buf, &mut text, a.as_ref(), row),
                Some(None) => part(&mut buf, None),
                None => self.read_start_part(&mut buf),
            }
            ids.extend_from_slice(&uuid(&buf));
        }
        FixedSizeBinaryArray::try_new(16, ids.into(), None)
    }
}

fn part(buf: &mut Vec<u8>, bytes: Option<&[u8]>) {
    match bytes {
        Some(b) => {
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        None => buf.extend_from_slice(&NULL.to_le_bytes()),
    }
}

fn json_part(buf: &mut Vec<u8>, v: Option<&Value>) {
    match v {
        None | Some(Value::Null) => part(buf, None),
        Some(v) => part(buf, Some(sorted(v).to_string().as_bytes())),
    }
}

/// The value with every object's keys sorted.
fn sorted(v: &Value) -> Value {
    match v {
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            Value::Object(
                keys.into_iter()
                    .map(|k| (k.clone(), sorted(&m[k])))
                    .collect(),
            )
        }
        Value::Array(a) => Value::Array(a.iter().map(sorted).collect()),
        v => v.clone(),
    }
}

fn uuid(buf: &[u8]) -> [u8; 16] {
    let mut b = XxHash3_128::oneshot(buf).to_be_bytes();
    b[6] = (b[6] & 0x0f) | 0x80;
    b[8] = (b[8] & 0x3f) | 0x80;
    b
}

fn supported(dt: &DataType) -> bool {
    use DataType as D;
    matches!(
        dt,
        D::Null
            | D::Boolean
            | D::Int8
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
            | D::Decimal128(..)
            | D::Decimal256(..)
            | D::Utf8
            | D::LargeUtf8
            | D::Binary
            | D::LargeBinary
            | D::FixedSizeBinary(_)
            | D::Date32
            | D::Date64
            | D::Time32(_)
            | D::Time64(_)
            | D::Timestamp(..)
    )
}

/// Encodes row `i` of `a`, whose type is supported.
fn typed_part(buf: &mut Vec<u8>, text: &mut String, a: &dyn Array, i: usize) {
    use DataType as D;
    use std::fmt::Write;
    if a.is_null(i) {
        part(buf, None);
        return;
    }
    text.clear();
    let _ = match a.data_type() {
        D::Boolean => write!(text, "{}", a.as_boolean().values().value(i)),
        D::Int8 => write!(text, "{}", a.as_primitive::<Int8Type>().values()[i]),
        D::Int16 => write!(text, "{}", a.as_primitive::<Int16Type>().values()[i]),
        D::Int32 => write!(text, "{}", a.as_primitive::<Int32Type>().values()[i]),
        D::Int64 => write!(text, "{}", a.as_primitive::<Int64Type>().values()[i]),
        D::UInt8 => write!(text, "{}", a.as_primitive::<UInt8Type>().values()[i]),
        D::UInt16 => write!(text, "{}", a.as_primitive::<UInt16Type>().values()[i]),
        D::UInt32 => write!(text, "{}", a.as_primitive::<UInt32Type>().values()[i]),
        D::UInt64 => write!(text, "{}", a.as_primitive::<UInt64Type>().values()[i]),
        D::Float16 => write!(text, "{}", a.as_primitive::<Float16Type>().values()[i]),
        D::Float32 => write!(text, "{}", a.as_primitive::<Float32Type>().values()[i]),
        D::Float64 => write!(text, "{}", a.as_primitive::<Float64Type>().values()[i]),
        D::Decimal128(_, s) => {
            write!(
                text,
                "{}e-{s}",
                a.as_primitive::<Decimal128Type>().values()[i]
            )
        }
        D::Decimal256(_, s) => {
            write!(
                text,
                "{}e-{s}",
                a.as_primitive::<Decimal256Type>().values()[i]
            )
        }
        D::Utf8 => return part(buf, Some(bytes(a.as_string::<i32>(), i))),
        D::LargeUtf8 => return part(buf, Some(bytes(a.as_string::<i64>(), i))),
        D::Binary => return part(buf, Some(bytes(a.as_binary::<i32>(), i))),
        D::LargeBinary => return part(buf, Some(bytes(a.as_binary::<i64>(), i))),
        D::FixedSizeBinary(_) => {
            let f = a.as_fixed_size_binary();
            let w = f.value_length() as usize;
            return part(buf, Some(&f.value_data()[i * w..(i + 1) * w]));
        }
        D::Date32 => write!(text, "{}", a.as_primitive::<Date32Type>().values()[i]),
        D::Date64 => write!(text, "{}", a.as_primitive::<Date64Type>().values()[i]),
        D::Time32(TimeUnit::Second) => {
            write!(text, "{}", a.as_primitive::<Time32SecondType>().values()[i])
        }
        D::Time32(_) => write!(
            text,
            "{}",
            a.as_primitive::<Time32MillisecondType>().values()[i]
        ),
        D::Time64(TimeUnit::Microsecond) => {
            write!(
                text,
                "{}",
                a.as_primitive::<Time64MicrosecondType>().values()[i]
            )
        }
        D::Time64(_) => write!(
            text,
            "{}",
            a.as_primitive::<Time64NanosecondType>().values()[i]
        ),
        D::Timestamp(TimeUnit::Second, _) => {
            write!(
                text,
                "{}",
                a.as_primitive::<TimestampSecondType>().values()[i]
            )
        }
        D::Timestamp(TimeUnit::Millisecond, _) => {
            write!(
                text,
                "{}",
                a.as_primitive::<TimestampMillisecondType>().values()[i]
            )
        }
        D::Timestamp(TimeUnit::Microsecond, _) => {
            write!(
                text,
                "{}",
                a.as_primitive::<TimestampMicrosecondType>().values()[i]
            )
        }
        D::Timestamp(TimeUnit::Nanosecond, _) => {
            write!(
                text,
                "{}",
                a.as_primitive::<TimestampNanosecondType>().values()[i]
            )
        }
        _ => Ok(()),
    };
    part(buf, Some(text.as_bytes()));
}

fn bytes<T: ByteArrayType>(a: &GenericByteArray<T>, i: usize) -> &[u8] {
    let o = a.value_offsets();
    &a.value_data()[o[i].as_usize()..o[i + 1].as_usize()]
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{Field, Schema};

    fn obj(json: &str) -> Map<String, Value> {
        serde_json::from_str(json).unwrap()
    }

    fn id(identity: &Identity, json: &str) -> [u8; 16] {
        identity.raw(&obj(json), &mut Vec::new())
    }

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    fn unkeyed() -> Identity {
        Identity {
            stream: "s".into(),
            read_start: 1_700_000_000_000_000,
            ..Default::default()
        }
    }

    #[test]
    fn ids_are_version_8_uuids_that_never_change() {
        let a = id(&unkeyed(), r#"{"a": 1, "b": "x"}"#);
        assert_eq!(a[6] >> 4, 8);
        assert_eq!(a[8] >> 6, 0b10);
        // A changed encoding changes every ID in every destination.
        assert_eq!(hex(&a), "7c149fa1eeb68f5fb457e1524bdaf421");
    }

    #[test]
    fn unkeyed_ids_ignore_order_and_nulls() {
        let i = unkeyed();
        let a = id(&i, r#"{"a": 1, "b": {"y": 2, "x": [1]}}"#);
        assert_eq!(a, id(&i, r#"{"b": {"x": [1], "y": 2}, "c": null, "a": 1}"#));
        assert_ne!(a, id(&i, r#"{"a": 2, "b": {"x": [1], "y": 2}}"#));
        assert_ne!(a, id(&i, r#"{"a": "1", "b": {"x": [1], "y": 2}}"#));
        let later = Identity {
            read_start: i.read_start + 1,
            ..i.clone()
        };
        assert_ne!(a, id(&later, r#"{"a": 1, "b": {"y": 2, "x": [1]}}"#));
        let other = Identity {
            stream: "t".into(),
            ..i
        };
        assert_ne!(a, id(&other, r#"{"a": 1, "b": {"y": 2, "x": [1]}}"#));
    }

    #[test]
    fn keyed_ids_use_the_key_and_the_cursor() {
        let i = Identity {
            stream: "s".into(),
            key: vec!["id".into()],
            cursor: Some("at".into()),
            read_start: 0,
        };
        let a = id(&i, r#"{"id": 1, "at": "2026-01-01", "v": "x"}"#);
        assert_eq!(a, id(&i, r#"{"id": 1, "at": "2026-01-01", "v": "y"}"#));
        assert_ne!(a, id(&i, r#"{"id": 1, "at": "2026-01-02", "v": "x"}"#));
        assert_ne!(a, id(&i, r#"{"id": 2, "at": "2026-01-01", "v": "x"}"#));
        assert_ne!(
            id(&i, r#"{"id": 1}"#),
            id(&i, r#"{"id": 1, "at": ""}"#),
            "a null cursor isn't an empty one"
        );
    }

    #[test]
    fn typed_ids_follow_the_same_rules() {
        let i = Identity {
            stream: "s".into(),
            key: vec!["id".into()],
            cursor: None,
            read_start: 5,
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 1])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])),
            ],
        )
        .unwrap();
        let ids = i.typed(&batch).unwrap();
        assert_eq!(ids.len(), 3);
        let first = &ids.value_data()[0..16];
        assert_eq!(first, &ids.value_data()[32..48]);
        assert_ne!(first, &ids.value_data()[16..32]);

        let unkeyed = Identity {
            key: Vec::new(),
            ..i.clone()
        };
        let ids = unkeyed.typed(&batch).unwrap();
        assert_ne!(&ids.value_data()[0..16], &ids.value_data()[32..48]);

        let missing = Identity {
            key: vec!["nope".into()],
            ..i
        };
        assert!(missing.typed(&batch).is_err());
    }
}
