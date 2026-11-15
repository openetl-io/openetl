//! Values as a group window's state keeps them: read from a column, written
//! in a few bytes, compared, and built back into a column of their type.
//! Every type the planner lets a window group by, or take the `min`, `max`
//! or `approx_distinct` of, is one of these.

use std::cmp::Ordering;
use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, FixedSizeBinaryBuilder,
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder, TimestampNanosecondBuilder,
    UInt64Builder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::ByteArrayType;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Time64MicrosecondType, TimestampMicrosecondType, TimestampNanosecondType,
};
use arrow_array::{Array, ArrayRef, GenericByteArray};
use arrow_buffer::ArrowNativeType;
use arrow_schema::{DataType, TimeUnit};

/// A value that isn't null.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Bool(bool),
    /// Integers, dates, times and timestamps, in their unit.
    Int(i64),
    Float(f64),
    /// A decimal's unscaled value.
    Decimal(i128),
    /// Text, binary and UUIDs.
    Bytes(Vec<u8>),
}

const NULL: u8 = 0;
const FALSE: u8 = 1;
const TRUE: u8 = 2;
const INT: u8 = 3;
const FLOAT: u8 = 4;
const DECIMAL: u8 = 5;
const BYTES: u8 = 6;

impl Value {
    /// Row `i` of `a`; `None` where it's null or of a type that isn't
    /// kept.
    pub fn read(a: &dyn Array, i: usize) -> Option<Value> {
        if a.is_null(i) {
            return None;
        }
        Some(match a.data_type() {
            DataType::Boolean => Value::Bool(a.as_boolean().values().value(i)),
            DataType::Int16 => Value::Int(i64::from(a.as_primitive::<Int16Type>().values()[i])),
            DataType::Int32 => Value::Int(i64::from(a.as_primitive::<Int32Type>().values()[i])),
            DataType::Int64 => Value::Int(a.as_primitive::<Int64Type>().values()[i]),
            DataType::Date32 => Value::Int(i64::from(a.as_primitive::<Date32Type>().values()[i])),
            DataType::Time64(TimeUnit::Microsecond) => {
                Value::Int(a.as_primitive::<Time64MicrosecondType>().values()[i])
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Value::Int(a.as_primitive::<TimestampMicrosecondType>().values()[i])
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Value::Int(a.as_primitive::<TimestampNanosecondType>().values()[i])
            }
            DataType::Float32 => {
                Value::Float(f64::from(a.as_primitive::<Float32Type>().values()[i]))
            }
            DataType::Float64 => Value::Float(a.as_primitive::<Float64Type>().values()[i]),
            DataType::Decimal128(..) => {
                Value::Decimal(a.as_primitive::<Decimal128Type>().values()[i])
            }
            DataType::Utf8 => Value::Bytes(bytes(a.as_string::<i32>(), i).to_vec()),
            DataType::Binary => Value::Bytes(bytes(a.as_binary::<i32>(), i).to_vec()),
            DataType::FixedSizeBinary(_) => {
                let f = a.as_fixed_size_binary();
                let w = usize::try_from(f.value_length()).unwrap_or_default();
                Value::Bytes(f.value_data().get(i * w..(i + 1) * w)?.to_vec())
            }
            _ => return None,
        })
    }

    /// The order `min` and `max` go by: numbers by value, floats with
    /// NaN above every number and -0 equal to 0, bytes bytewise, false
    /// before true.
    pub fn cmp(&self, other: &Value) -> Ordering {
        match (self, other) {
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => canonical(*a).total_cmp(&canonical(*b)),
            (Value::Decimal(a), Value::Decimal(b)) => a.cmp(b),
            (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
            (a, b) => tag(Some(a)).cmp(&tag(Some(b))),
        }
    }
}

/// The bytes of row `i`, which isn't null.
fn bytes<T: ByteArrayType>(a: &GenericByteArray<T>, i: usize) -> &[u8] {
    let o = a.value_offsets();
    let (start, end) = (o[i].as_usize(), o[i + 1].as_usize());
    a.value_data().get(start..end).unwrap_or_default()
}

/// A float as it's compared and kept: one NaN, and one zero.
pub fn canonical(f: f64) -> f64 {
    if f.is_nan() {
        f64::NAN
    } else if f == 0.0 {
        0.0
    } else {
        f
    }
}

fn tag(v: Option<&Value>) -> u8 {
    match v {
        None => NULL,
        Some(Value::Bool(false)) => FALSE,
        Some(Value::Bool(true)) => TRUE,
        Some(Value::Int(_)) => INT,
        Some(Value::Float(_)) => FLOAT,
        Some(Value::Decimal(_)) => DECIMAL,
        Some(Value::Bytes(_)) => BYTES,
    }
}

/// Appends `v` to `out`: a tag, then its bytes.
pub fn encode(v: Option<&Value>, out: &mut Vec<u8>) {
    out.push(tag(v));
    match v {
        None | Some(Value::Bool(_)) => {}
        Some(Value::Int(i)) => out.extend_from_slice(&i.to_be_bytes()),
        Some(Value::Float(f)) => out.extend_from_slice(&canonical(*f).to_bits().to_be_bytes()),
        Some(Value::Decimal(d)) => out.extend_from_slice(&d.to_be_bytes()),
        Some(Value::Bytes(b)) => {
            out.extend_from_slice(&len32(b.len()).to_be_bytes());
            out.extend_from_slice(b);
        }
    }
}

/// The value at the start of `buf`, which moves past it; `Err` where the
/// bytes aren't one.
pub fn decode(buf: &mut &[u8]) -> Result<Option<Value>, String> {
    let (&t, rest) = buf.split_first().ok_or("a value past the end")?;
    *buf = rest;
    Ok(match t {
        NULL => None,
        FALSE => Some(Value::Bool(false)),
        TRUE => Some(Value::Bool(true)),
        INT => Some(Value::Int(i64::from_be_bytes(take(buf)?))),
        FLOAT => Some(Value::Float(f64::from_bits(u64::from_be_bytes(take(buf)?)))),
        DECIMAL => Some(Value::Decimal(i128::from_be_bytes(take(buf)?))),
        BYTES => {
            let n = u32::from_be_bytes(take(buf)?);
            let n = usize::try_from(n).map_err(|e| e.to_string())?;
            if buf.len() < n {
                return Err("bytes past the end".into());
            }
            let (b, rest) = buf.split_at(n);
            *buf = rest;
            Some(Value::Bytes(b.to_vec()))
        }
        other => return Err(format!("a value's tag {other}")),
    })
}

/// The first `N` bytes of `buf`, which moves past them.
pub fn take<const N: usize>(buf: &mut &[u8]) -> Result<[u8; N], String> {
    if buf.len() < N {
        return Err("a field past the end".into());
    }
    let (a, rest) = buf.split_at(N);
    *buf = rest;
    a.try_into().map_err(|_| "a field past the end".to_owned())
}

/// A length under 4 GiB, as every value here is.
pub fn len32(n: usize) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

/// A column of type `t` holding `values`.
pub fn build(t: &DataType, values: &[Option<Value>]) -> Result<ArrayRef, String> {
    let wrong = |v: &Value| format!("{v:?} in a column of {t}");
    macro_rules! ints {
        ($builder:ty, $conv:expr) => {{
            let mut b = <$builder>::with_capacity(values.len());
            for v in values {
                match v {
                    None => b.append_null(),
                    Some(Value::Int(i)) => {
                        b.append_value($conv(*i).map_err(|_| wrong(&Value::Int(*i)))?)
                    }
                    Some(other) => return Err(wrong(other)),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }};
    }
    let same = |i: i64| Ok::<i64, ()>(i);
    Ok(match t {
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(values.len());
            for v in values {
                match v {
                    None => b.append_null(),
                    Some(Value::Bool(x)) => b.append_value(*x),
                    Some(other) => return Err(wrong(other)),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Int16 => ints!(Int16Builder, |i: i64| i16::try_from(i)),
        DataType::Int32 => ints!(Int32Builder, |i: i64| i32::try_from(i)),
        DataType::Int64 => ints!(Int64Builder, same),
        DataType::UInt64 => ints!(UInt64Builder, |i: i64| u64::try_from(i)),
        DataType::Date32 => ints!(Date32Builder, |i: i64| i32::try_from(i)),
        DataType::Time64(TimeUnit::Microsecond) => ints!(Time64MicrosecondBuilder, same),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let a = ints!(TimestampMicrosecondBuilder, same);
            with_zone(a, tz)?
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let a = ints!(TimestampNanosecondBuilder, same);
            with_zone(a, tz)?
        }
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(values.len());
            for v in values {
                match v {
                    None => b.append_null(),
                    Some(Value::Float(f)) => b.append_value(*f as f32),
                    Some(other) => return Err(wrong(other)),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    None => b.append_null(),
                    Some(Value::Float(f)) => b.append_value(*f),
                    Some(other) => return Err(wrong(other)),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Decimal128(p, s) => {
            let mut b = Decimal128Builder::with_capacity(values.len());
            for v in values {
                match v {
                    None => b.append_null(),
                    Some(Value::Decimal(d)) => b.append_value(*d),
                    Some(other) => return Err(wrong(other)),
                }
            }
            Arc::new(
                b.finish()
                    .with_precision_and_scale(*p, *s)
                    .map_err(|e| e.to_string())?,
            )
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(values.len(), 0);
            for v in values {
                match v {
                    None => b.append_null(),
                    Some(Value::Bytes(x)) => {
                        b.append_value(std::str::from_utf8(x).map_err(|e| e.to_string())?)
                    }
                    Some(other) => return Err(wrong(other)),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Binary => {
            let mut b = BinaryBuilder::with_capacity(values.len(), 0);
            for v in values {
                match v {
                    None => b.append_null(),
                    Some(Value::Bytes(x)) => b.append_value(x),
                    Some(other) => return Err(wrong(other)),
                }
            }
            Arc::new(b.finish())
        }
        DataType::FixedSizeBinary(n) => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(values.len(), *n);
            for v in values {
                match v {
                    None => b.append_null(),
                    Some(Value::Bytes(x)) => b.append_value(x).map_err(|e| e.to_string())?,
                    Some(other) => return Err(wrong(other)),
                }
            }
            Arc::new(b.finish())
        }
        other => return Err(format!("a group window keeps no values of {other}")),
    })
}

fn with_zone(a: ArrayRef, tz: &Option<Arc<str>>) -> Result<ArrayRef, String> {
    match tz {
        None => Ok(a),
        Some(tz) => arrow_cast_zone(a, tz),
    }
}

fn arrow_cast_zone(a: ArrayRef, tz: &Arc<str>) -> Result<ArrayRef, String> {
    use arrow_array::{TimestampMicrosecondArray, TimestampNanosecondArray};
    if let Some(x) = a.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return Ok(Arc::new(x.clone().with_timezone(tz.clone())));
    }
    if let Some(x) = a.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Ok(Arc::new(x.clone().with_timezone(tz.clone())));
    }
    Err("a timestamp column that isn't one".into())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn values_read_back_as_written() {
        let values = [
            None,
            Some(Value::Bool(true)),
            Some(Value::Int(-7)),
            Some(Value::Float(-0.0)),
            Some(Value::Decimal(-12345)),
            Some(Value::Bytes(b"h\x00i".to_vec())),
        ];
        let mut buf = Vec::new();
        for v in &values {
            encode(v.as_ref(), &mut buf);
        }
        let mut at = buf.as_slice();
        for v in &values {
            let got = decode(&mut at).unwrap();
            match (v, &got) {
                (Some(Value::Float(_)), Some(Value::Float(g))) => assert_eq!(g.to_bits(), 0),
                _ => assert_eq!(&got, v),
            }
        }
        assert!(at.is_empty());
        assert!(decode(&mut &[BYTES, 0, 0, 0, 9][..]).is_err());
    }

    #[test]
    fn nan_is_above_every_number_and_zeros_are_equal() {
        let f = |x| Value::Float(x);
        assert_eq!(f(f64::NAN).cmp(&f(f64::INFINITY)), Ordering::Greater);
        assert_eq!(f(-0.0).cmp(&f(0.0)), Ordering::Equal);
        assert_eq!(f(-1.0).cmp(&f(0.0)), Ordering::Less);
    }
}
