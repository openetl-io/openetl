//! Key groups (ADR-0027). A key's group is `h mod n` for a pipeline's `n`
//! groups, a power of two, where `h` is the xxh3-64 hash of `openetl.kg.v1`
//! and each of the key's values in key order: a little-endian u32 length,
//! then its bytes, or the length `u32::MAX` alone for a null.
//!
//! Values are encoded by their logical family, so every widening `widens`
//! allows keeps a key in its group:
//! - integers, decimals and floats as decimal text: no `+`, no exponent, no
//!   leading zeros, no trailing zeros after the point, and no point for an
//!   integral value. Floats go through float64 first, so a float32 widened
//!   to float64 keeps its text; an integer past 2^53 that float64 rounds
//!   changes.
//! - strings and binary as their bytes, JSON as its text with object keys
//!   sorted;
//! - booleans as `0` or `1`;
//! - dates as days, and times and timestamps as microseconds, or
//!   nanoseconds where the type keeps them.
//!
//! A widening to a string changes a key's text, and with it the group.

use arrow_array::cast::AsArray;
use arrow_array::types::ByteArrayType;
use arrow_array::types::{
    Date32Type, Date64Type, Decimal128Type, Decimal256Type, Float16Type, Float32Type, Float64Type,
    Int8Type, Int16Type, Int32Type, Int64Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{Array, GenericByteArray, RecordBatch, UInt32Array};
use arrow_buffer::ArrowNativeType;
use arrow_schema::{ArrowError, DataType, Field, TimeUnit};
use arrow_select::take::take_record_batch;
use twox_hash::XxHash3_64;

use crate::{EXTENSION_NAME_KEY, JSON_EXTENSION};

/// The hash's name and version.
pub const KEY_GROUP_HASH: &str = "openetl.kg.v1";

/// The key group of each row of `batch`, from its columns named `key`, out
/// of `groups`, a power of two.
pub fn key_groups(
    batch: &RecordBatch,
    key: &[String],
    groups: u32,
) -> Result<Vec<u32>, ArrowError> {
    if !groups.is_power_of_two() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "{groups} key groups, not a power of two"
        )));
    }
    let schema = batch.schema();
    let mut cols = Vec::with_capacity(key.len());
    for k in key {
        let i = schema.index_of(k).map_err(|_| {
            ArrowError::SchemaError(format!("the primary key column {k:?} is missing"))
        })?;
        let f = schema.field(i);
        if !hashable(f.data_type()) {
            return Err(ArrowError::NotYetImplemented(format!(
                "a key group from column {k:?} of type {}",
                f.data_type()
            )));
        }
        cols.push((f, batch.column(i).as_ref()));
    }
    let mut buf = Vec::new();
    let mut text = String::new();
    let mut out = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        buf.clear();
        buf.extend_from_slice(KEY_GROUP_HASH.as_bytes());
        for (f, a) in &cols {
            value(&mut buf, &mut text, f, *a, row);
        }
        out.push((XxHash3_64::oneshot(&buf) % u64::from(groups)) as u32);
    }
    Ok(out)
}

/// `batch`'s rows by the part owning their key's group, of `parts` contiguous
/// ranges of `groups`; None for a part without rows. Rows keep their order
/// within a part.
pub fn by_key_group(
    batch: &RecordBatch,
    key: &[String],
    groups: u32,
    parts: usize,
) -> Result<Vec<Option<RecordBatch>>, ArrowError> {
    let mut rows = vec![Vec::new(); parts];
    for (row, g) in key_groups(batch, key, groups)?.into_iter().enumerate() {
        rows[g as usize * parts / groups as usize].push(row as u32);
    }
    rows.into_iter()
        .map(|r| match r.is_empty() {
            true => Ok(None),
            false => take_record_batch(batch, &UInt32Array::from(r)).map(Some),
        })
        .collect()
}

fn hashable(dt: &DataType) -> bool {
    use DataType as D;
    matches!(
        dt,
        D::Boolean
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

fn part(buf: &mut Vec<u8>, b: &[u8]) {
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}

/// Encodes row `i` of `a`, of field `f`, whose type is hashable.
fn value(buf: &mut Vec<u8>, text: &mut String, f: &Field, a: &dyn Array, i: usize) {
    use DataType as D;
    use std::fmt::Write;
    if a.is_null(i) {
        buf.extend_from_slice(&u32::MAX.to_le_bytes());
        return;
    }
    text.clear();
    let micros = |v: i64, per_second: i64| v.saturating_mul(1_000_000 / per_second);
    let _ = match a.data_type() {
        D::Boolean => write!(text, "{}", u8::from(a.as_boolean().values().value(i))),
        D::Int8 => write!(text, "{}", a.as_primitive::<Int8Type>().values()[i]),
        D::Int16 => write!(text, "{}", a.as_primitive::<Int16Type>().values()[i]),
        D::Int32 => write!(text, "{}", a.as_primitive::<Int32Type>().values()[i]),
        D::Int64 => write!(text, "{}", a.as_primitive::<Int64Type>().values()[i]),
        D::UInt8 => write!(text, "{}", a.as_primitive::<UInt8Type>().values()[i]),
        D::UInt16 => write!(text, "{}", a.as_primitive::<UInt16Type>().values()[i]),
        D::UInt32 => write!(text, "{}", a.as_primitive::<UInt32Type>().values()[i]),
        D::UInt64 => write!(text, "{}", a.as_primitive::<UInt64Type>().values()[i]),
        D::Float16 => float(text, f64::from(a.as_primitive::<Float16Type>().values()[i])),
        D::Float32 => float(text, f64::from(a.as_primitive::<Float32Type>().values()[i])),
        D::Float64 => float(text, a.as_primitive::<Float64Type>().values()[i]),
        D::Decimal128(_, s) => decimal(
            text,
            &a.as_primitive::<Decimal128Type>().values()[i].to_string(),
            *s,
        ),
        D::Decimal256(_, s) => decimal(
            text,
            &a.as_primitive::<Decimal256Type>().values()[i].to_string(),
            *s,
        ),
        D::Utf8 if json(f) => return json_part(buf, bytes(a.as_string::<i32>(), i)),
        D::LargeUtf8 if json(f) => return json_part(buf, bytes(a.as_string::<i64>(), i)),
        D::Utf8 => return part(buf, bytes(a.as_string::<i32>(), i)),
        D::LargeUtf8 => return part(buf, bytes(a.as_string::<i64>(), i)),
        D::Binary => return part(buf, bytes(a.as_binary::<i32>(), i)),
        D::LargeBinary => return part(buf, bytes(a.as_binary::<i64>(), i)),
        D::FixedSizeBinary(_) => {
            let f = a.as_fixed_size_binary();
            let w = f.value_length() as usize;
            return part(buf, &f.value_data()[i * w..(i + 1) * w]);
        }
        D::Date32 => write!(text, "{}", a.as_primitive::<Date32Type>().values()[i]),
        D::Date64 => write!(
            text,
            "{}",
            a.as_primitive::<Date64Type>().values()[i].div_euclid(86_400_000)
        ),
        D::Time32(TimeUnit::Second) => write!(
            text,
            "{}",
            micros(
                i64::from(a.as_primitive::<Time32SecondType>().values()[i]),
                1
            )
        ),
        D::Time32(_) => write!(
            text,
            "{}",
            micros(
                i64::from(a.as_primitive::<Time32MillisecondType>().values()[i]),
                1000
            )
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
        D::Timestamp(TimeUnit::Second, _) => write!(
            text,
            "{}",
            micros(a.as_primitive::<TimestampSecondType>().values()[i], 1)
        ),
        D::Timestamp(TimeUnit::Millisecond, _) => write!(
            text,
            "{}",
            micros(
                a.as_primitive::<TimestampMillisecondType>().values()[i],
                1000
            )
        ),
        D::Timestamp(TimeUnit::Microsecond, _) => write!(
            text,
            "{}",
            a.as_primitive::<TimestampMicrosecondType>().values()[i]
        ),
        D::Timestamp(TimeUnit::Nanosecond, _) => write!(
            text,
            "{}",
            a.as_primitive::<TimestampNanosecondType>().values()[i]
        ),
        _ => Ok(()),
    };
    part(buf, text.as_bytes());
}

fn json(f: &Field) -> bool {
    f.metadata().get(EXTENSION_NAME_KEY).map(String::as_str) == Some(JSON_EXTENSION)
}

fn json_part(buf: &mut Vec<u8>, s: &[u8]) {
    match serde_json::from_slice::<serde_json::Value>(s) {
        // serde_json's maps keep their keys sorted.
        Ok(v) => part(buf, v.to_string().as_bytes()),
        Err(_) => part(buf, s),
    }
}

fn bytes<T: ByteArrayType>(a: &GenericByteArray<T>, i: usize) -> &[u8] {
    let o = a.value_offsets();
    &a.value_data()[o[i].as_usize()..o[i + 1].as_usize()]
}

/// Rust prints floats without an exponent, in the fewest digits that read
/// back the same.
fn float(text: &mut String, v: f64) -> std::fmt::Result {
    use std::fmt::Write;
    if v == 0.0 {
        text.push('0');
        return Ok(());
    }
    write!(text, "{v}")
}

/// `digits` is the unscaled value as an integer's text.
fn decimal(text: &mut String, digits: &str, scale: i8) -> std::fmt::Result {
    let (neg, digits) = match digits.strip_prefix('-') {
        Some(d) => (true, d),
        None => (false, digits),
    };
    let (int, frac) = if scale <= 0 {
        (
            format!("{digits}{}", "0".repeat(usize::from(scale.unsigned_abs()))),
            String::new(),
        )
    } else {
        let s = usize::from(scale.unsigned_abs());
        let padded = format!("{digits:0>width$}", width = s + 1);
        let (i, f) = padded.split_at(padded.len() - s);
        (i.to_string(), f.trim_end_matches('0').to_string())
    };
    let int = int.trim_start_matches('0');
    let int = if int.is_empty() { "0" } else { int };
    if neg && (int != "0" || !frac.is_empty()) {
        text.push('-');
    }
    text.push_str(int);
    if !frac.is_empty() {
        text.push('.');
        text.push_str(&frac);
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, Decimal128Array, Float32Array, Int16Array, Int32Array, Int64Array, RecordBatch,
        StringArray,
    };
    use arrow_cast::cast;
    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    use crate::widens;

    #[test]
    fn parts_own_contiguous_ranges_and_keep_order() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ids: Vec<i64> = (0..1000).collect();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids))]).unwrap();
        let key = ["id".to_string()];
        let parts = by_key_group(&batch, &key, 256, 4).unwrap();
        assert_eq!(parts.len(), 4);
        let mut seen = 0;
        for (p, part) in parts.iter().enumerate() {
            let part = part.as_ref().unwrap();
            seen += part.num_rows();
            let groups = key_groups(part, &key, 256).unwrap();
            assert!(groups.iter().all(|g| *g as usize * 4 / 256 == p));
            let ids = part
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert!(ids.values().windows(2).all(|w| w[0] < w[1]));
        }
        assert_eq!(seen, 1000);
    }

    fn groups(cols: &[(&str, ArrayRef)], n: u32) -> Vec<u32> {
        let fields: Vec<Field> = cols
            .iter()
            .map(|(name, a)| Field::new(*name, a.data_type().clone(), true))
            .collect();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            cols.iter().map(|(_, a)| a.clone()).collect(),
        )
        .unwrap();
        let key: Vec<String> = cols.iter().map(|(n, _)| (*n).to_string()).collect();
        key_groups(&batch, &key, n).unwrap()
    }

    #[test]
    fn numbers_of_any_family_share_a_group() {
        let seven: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![7])),
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(
                Decimal128Array::from(vec![7000])
                    .with_precision_and_scale(12, 3)
                    .unwrap(),
            ),
            Arc::new(arrow_array::Float64Array::from(vec![7.0])),
        ];
        let want = groups(&[("k", seven[0].clone())], 256);
        for a in &seven[1..] {
            assert_eq!(groups(&[("k", a.clone())], 256), want, "{}", a.data_type());
        }
    }

    #[test]
    fn decimals_print_normalized() {
        for (digits, scale, want) in [
            ("7000", 3, "7"),
            ("-7500", 3, "-7.5"),
            ("5", 3, "0.005"),
            ("-0", 2, "0"),
            ("12", -2, "1200"),
            ("0", 0, "0"),
        ] {
            let mut s = String::new();
            decimal(&mut s, digits, scale).unwrap();
            assert_eq!(s, want, "{digits}e-{scale}");
        }
        let mut s = String::new();
        float(&mut s, -0.0).unwrap();
        assert_eq!(s, "0");
    }

    #[test]
    fn doubling_the_groups_splits_each_in_two() {
        let keys: ArrayRef = Arc::new(StringArray::from(
            (0..1000).map(|i| format!("k{i}")).collect::<Vec<_>>(),
        ));
        let g256 = groups(&[("k", keys.clone())], 256);
        let g512 = groups(&[("k", keys)], 512);
        for (a, b) in g256.iter().zip(&g512) {
            assert_eq!(b % 256, *a);
        }
        assert!(g256.iter().all(|g| *g < 256));
    }

    #[test]
    fn rejects_counts_that_are_not_powers_of_two() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)])),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        assert!(key_groups(&batch, &["k".into()], 255).is_err());
    }

    fn splitmix(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Every widening of a number keeps every key in its group: random keys
    /// of each type, cast to each wider type the evolver allows.
    #[test]
    fn widening_keeps_every_key_in_its_group() {
        let mut rng = 42;
        let narrow: Vec<ArrayRef> = vec![
            Arc::new(Int16Array::from(
                (0..2000)
                    .map(|_| Some(splitmix(&mut rng) as i16))
                    .chain([None])
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                (0..2000)
                    .map(|_| Some(splitmix(&mut rng) as i32))
                    .collect::<Vec<_>>(),
            )),
            // Exact in float64.
            Arc::new(Int64Array::from(
                (0..2000)
                    .map(|_| {
                        Some(
                            (splitmix(&mut rng) >> 11) as i64
                                * if splitmix(&mut rng) & 1 == 0 { 1 } else { -1 },
                        )
                    })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float32Array::from(
                (0..2000)
                    .map(|_| {
                        Some(f32::from_bits(splitmix(&mut rng) as u32)).filter(|f| f.is_finite())
                    })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(
                Decimal128Array::from(
                    (0..2000)
                        .map(|_| {
                            Some((splitmix(&mut rng) % 10_000_000_000) as i128 - 5_000_000_000)
                        })
                        .collect::<Vec<_>>(),
                )
                .with_precision_and_scale(10, 3)
                .unwrap(),
            ),
        ];
        let wider = [
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(10, 3),
            DataType::Decimal128(12, 3),
            DataType::Decimal128(20, 0),
            DataType::Decimal128(25, 5),
            DataType::Decimal128(38, 10),
        ];
        let mut checked = 0;
        for a in &narrow {
            let from = Field::new("k", a.data_type().clone(), true);
            let want = groups(&[("k", a.clone())], 256);
            for to in &wider {
                if from.data_type() == to || !widens(&from, &Field::new("k", to.clone(), true)) {
                    continue;
                }
                let b = cast(a, to).unwrap();
                assert_eq!(
                    b.null_count(),
                    a.null_count(),
                    "{} to {to} lost values",
                    a.data_type()
                );
                assert_eq!(groups(&[("k", b)], 256), want, "{} to {to}", a.data_type());
                checked += 1;
            }
        }
        assert!(checked >= 10, "only {checked} widenings checked");
    }
}
