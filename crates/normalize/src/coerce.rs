//! Declared types. A stream may declare its columns' logical types; values
//! are coerced to them, and a value that doesn't fit rejects its row with a
//! reason that starts with a stable code: `type`, `range`, `precision` or
//! `nan`. Reasons never contain the value.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, FixedSizeBinaryBuilder,
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    Time64MicrosecondBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::ArrowPrimitiveType;
use arrow_array::types::{
    Date32Type, Int64Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int64Array, PrimitiveArray, RecordBatch, make_array,
};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use base64::Engine;
use chrono::NaiveDate;
use serde_json::Value;

use crate::{EXTENSION_NAME_KEY, Error, JSON_EXTENSION, TypeOptions, UUID_EXTENSION};

/// What becomes of infinite dates and timestamps: `infinity` and
/// `-infinity` in text, and in typed batches the largest and smallest
/// values of Date32 and of timestamps in any unit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Infinity {
    /// To the logical type's largest or smallest value.
    #[default]
    Clamp,
    /// Rejected with reason `range`.
    Quarantine,
}

impl Infinity {
    fn apply<T>(self, limit: T) -> Result<T, Unfit> {
        match self {
            Infinity::Clamp => Ok(limit),
            Infinity::Quarantine => Err(Unfit::new("range", "is infinite")),
        }
    }
}

/// Infinite dates and timestamps are clamped to these.
const MAX_DATE: i32 = 2_932_896; // 9999-12-31
const MIN_DATE: i32 = -719_162; // 0001-01-01
const MAX_TIMESTAMP: i64 = 253_402_300_799_999_999; // 9999-12-31 23:59:59.999999
const MIN_TIMESTAMP: i64 = -62_135_596_800_000_000; // 0001-01-01 00:00:00
const NANOS_PER_DAY: i64 = 86_400_000_000_000;

/// A logical type that values can be coerced to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Target {
    Boolean,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Json,
    Binary,
    Decimal(u8, i8),
    Timestamp {
        utc: bool,
        /// Only for streams that keep nanoseconds.
        nanos: bool,
    },
    Date,
    Time,
    Uuid,
    /// Logical, but only typed batches carry it: interval and list.
    TypedOnly,
}

impl Target {
    pub(crate) fn of(f: &Field) -> Option<Target> {
        let ext = f.metadata().get(EXTENSION_NAME_KEY).map(String::as_str);
        Some(match f.data_type() {
            DataType::Boolean => Target::Boolean,
            DataType::Int16 => Target::Int16,
            DataType::Int32 => Target::Int32,
            DataType::Int64 => Target::Int64,
            DataType::Float32 => Target::Float32,
            DataType::Float64 => Target::Float64,
            DataType::Utf8 if ext == Some(JSON_EXTENSION) => Target::Json,
            DataType::Utf8 => Target::String,
            DataType::Binary => Target::Binary,
            DataType::Decimal128(p, s) if (1..=38).contains(p) && (0..=*p as i8).contains(s) => {
                Target::Decimal(*p, *s)
            }
            DataType::Timestamp(unit @ (TimeUnit::Microsecond | TimeUnit::Nanosecond), tz)
                if tz.as_deref().is_none_or(|z| z == "UTC") =>
            {
                Target::Timestamp {
                    utc: tz.is_some(),
                    nanos: *unit == TimeUnit::Nanosecond,
                }
            }
            DataType::Date32 => Target::Date,
            DataType::Time64(TimeUnit::Microsecond) => Target::Time,
            DataType::FixedSizeBinary(16) if ext == Some(UUID_EXTENSION) => Target::Uuid,
            DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano) => Target::TypedOnly,
            DataType::List(item) if Target::of(item).is_some() => Target::TypedOnly,
            _ => return None,
        })
    }
}

/// A declared schema: every column has a logical type. Timestamps are in
/// microseconds; a stream's option makes them nanoseconds.
pub fn check_declared(schema: &Schema) -> Result<(), Error> {
    for f in schema.fields() {
        if matches!(
            Target::of(f),
            None | Some(Target::Timestamp { nanos: true, .. })
        ) {
            return Err(Error::Declared(format!(
                "column {:?} is declared {}, which isn't a logical type",
                f.name(),
                f.data_type()
            )));
        }
    }
    Ok(())
}

/// `schema` with its timestamps in nanoseconds.
pub(crate) fn nanoseconds(schema: &Schema) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, tz) => f
                .as_ref()
                .clone()
                .with_data_type(DataType::Timestamp(TimeUnit::Nanosecond, tz.clone())),
            _ => f.as_ref().clone(),
        })
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

pub(crate) struct Unfit {
    code: &'static str,
    why: String,
}

impl Unfit {
    fn new(code: &'static str, why: impl Into<String>) -> Self {
        Self {
            code,
            why: why.into(),
        }
    }

    fn reason(&self, column: &str) -> String {
        format!("{}: column {column:?} {}", self.code, self.why)
    }
}

fn not_a(what: &str) -> Unfit {
    Unfit::new("type", format!("isn't {what}"))
}

enum Builder {
    Boolean(BooleanBuilder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    String(StringBuilder, bool),
    Binary(BinaryBuilder),
    Decimal(Decimal128Builder, u8, i8),
    Timestamp(Int64Builder, Ts),
    Date(Date32Builder, Infinity),
    Time(Time64MicrosecondBuilder),
    Uuid(FixedSizeBinaryBuilder),
}

/// How text becomes a timestamp column.
#[derive(Clone, Copy)]
struct Ts {
    utc: bool,
    nanos: bool,
    inf: Infinity,
}

impl Ts {
    fn unit(self) -> TimeUnit {
        match self.nanos {
            true => TimeUnit::Nanosecond,
            false => TimeUnit::Microsecond,
        }
    }

    fn array(self, ticks: Int64Array) -> ArrayRef {
        timestamps(ticks, self.unit(), self.utc.then(|| "UTC".into()))
    }
}

/// The smallest and largest timestamps in a unit, which infinities clamp
/// to.
fn limits(unit: &TimeUnit) -> (i64, i64) {
    match unit {
        TimeUnit::Nanosecond => (i64::MIN, i64::MAX),
        _ => (MIN_TIMESTAMP, MAX_TIMESTAMP),
    }
}

fn timestamps(ticks: Int64Array, unit: TimeUnit, tz: Option<Arc<str>>) -> ArrayRef {
    match unit {
        TimeUnit::Nanosecond => Arc::new(
            ticks
                .reinterpret_cast::<TimestampNanosecondType>()
                .with_timezone_opt(tz),
        ),
        _ => Arc::new(
            ticks
                .reinterpret_cast::<TimestampMicrosecondType>()
                .with_timezone_opt(tz),
        ),
    }
}

impl Builder {
    fn new(t: Target, n: usize, inf: Infinity) -> Option<Self> {
        Some(match t {
            Target::Boolean => Builder::Boolean(BooleanBuilder::with_capacity(n)),
            Target::Int16 => Builder::Int16(Int16Builder::with_capacity(n)),
            Target::Int32 => Builder::Int32(Int32Builder::with_capacity(n)),
            Target::Int64 => Builder::Int64(Int64Builder::with_capacity(n)),
            Target::Float32 => Builder::Float32(Float32Builder::with_capacity(n)),
            Target::Float64 => Builder::Float64(Float64Builder::with_capacity(n)),
            Target::String => Builder::String(StringBuilder::with_capacity(n, 0), false),
            Target::Json => Builder::String(StringBuilder::with_capacity(n, 0), true),
            Target::Binary => Builder::Binary(BinaryBuilder::with_capacity(n, 0)),
            Target::Decimal(p, s) => Builder::Decimal(
                Decimal128Builder::with_capacity(n).with_data_type(DataType::Decimal128(p, s)),
                p,
                s,
            ),
            Target::Timestamp { utc, nanos } => {
                Builder::Timestamp(Int64Builder::with_capacity(n), Ts { utc, nanos, inf })
            }
            Target::Date => Builder::Date(Date32Builder::with_capacity(n), inf),
            Target::Time => Builder::Time(Time64MicrosecondBuilder::with_capacity(n)),
            Target::Uuid => Builder::Uuid(FixedSizeBinaryBuilder::with_capacity(n, 16)),
            Target::TypedOnly => return None,
        })
    }

    fn null(&mut self) {
        match self {
            Builder::Boolean(b) => b.append_null(),
            Builder::Int16(b) => b.append_null(),
            Builder::Int32(b) => b.append_null(),
            Builder::Int64(b) => b.append_null(),
            Builder::Float32(b) => b.append_null(),
            Builder::Float64(b) => b.append_null(),
            Builder::String(b, _) => b.append_null(),
            Builder::Binary(b) => b.append_null(),
            Builder::Decimal(b, ..) => b.append_null(),
            Builder::Timestamp(b, ..) => b.append_null(),
            Builder::Date(b, _) => b.append_null(),
            Builder::Time(b) => b.append_null(),
            Builder::Uuid(b) => b.append_null(),
        }
    }

    fn json(&mut self, v: &Value) -> Result<(), Unfit> {
        match (self, v) {
            (Builder::Boolean(b), Value::Bool(x)) => b.append_value(*x),
            (Builder::String(b, false), Value::String(s)) => b.append_value(s),
            (Builder::String(b, _), v) => b.append_value(v.to_string()),
            (
                b @ (Builder::Int16(_)
                | Builder::Int32(_)
                | Builder::Int64(_)
                | Builder::Float32(_)
                | Builder::Float64(_)
                | Builder::Decimal(..)),
                Value::Number(n),
            ) => return b.text(&n.to_string()),
            (b, Value::String(s)) => return b.text(s),
            (b, _) => return Err(not_a(b.expected())),
        }
        Ok(())
    }

    fn text(&mut self, s: &str) -> Result<(), Unfit> {
        match self {
            Builder::Boolean(b) => b.append_value(match s {
                "true" => true,
                "false" => false,
                _ => return Err(not_a("a boolean")),
            }),
            Builder::Int16(b) => b.append_value(int(s)?),
            Builder::Int32(b) => b.append_value(int(s)?),
            Builder::Int64(b) => b.append_value(int(s)?),
            Builder::Float32(b) => b.append_value(float(s)?),
            Builder::Float64(b) => b.append_value(float(s)?),
            Builder::String(b, false) => b.append_value(s),
            Builder::String(b, true) => {
                serde_json::from_str::<serde::de::IgnoredAny>(s).map_err(|_| not_a("JSON"))?;
                b.append_value(s)
            }
            Builder::Binary(b) => b.append_value(
                base64::engine::general_purpose::STANDARD
                    .decode(s)
                    .map_err(|_| not_a("base64"))?,
            ),
            Builder::Decimal(b, p, sc) => b.append_value(decimal(s, *p, *sc)?),
            Builder::Timestamp(b, ts) => b.append_value(timestamp(s, *ts)?),
            Builder::Date(b, inf) => b.append_value(date(s, *inf)?),
            Builder::Time(b) => b.append_value(time(s)?),
            Builder::Uuid(b) => b
                .append_value(uuid(s).ok_or_else(|| not_a("a UUID"))?)
                .map_err(|_| not_a("a UUID"))?,
        }
        Ok(())
    }

    fn expected(&self) -> &'static str {
        match self {
            Builder::Boolean(_) => "a boolean",
            Builder::Int16(_) | Builder::Int32(_) | Builder::Int64(_) => "an integer",
            Builder::Float32(_) | Builder::Float64(_) | Builder::Decimal(..) => "a number",
            Builder::String(..) => "a string",
            Builder::Binary(_) => "base64",
            Builder::Timestamp(..) => "a date-time",
            Builder::Date(..) => "a date",
            Builder::Time(_) => "a time",
            Builder::Uuid(_) => "a UUID",
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Builder::Boolean(mut b) => Arc::new(b.finish()),
            Builder::Int16(mut b) => Arc::new(b.finish()),
            Builder::Int32(mut b) => Arc::new(b.finish()),
            Builder::Int64(mut b) => Arc::new(b.finish()),
            Builder::Float32(mut b) => Arc::new(b.finish()),
            Builder::Float64(mut b) => Arc::new(b.finish()),
            Builder::String(mut b, _) => Arc::new(b.finish()),
            Builder::Binary(mut b) => Arc::new(b.finish()),
            Builder::Decimal(mut b, ..) => Arc::new(b.finish()),
            Builder::Timestamp(mut b, ts) => ts.array(b.finish()),
            Builder::Date(mut b, _) => Arc::new(b.finish()),
            Builder::Time(mut b) => Arc::new(b.finish()),
            Builder::Uuid(mut b) => Arc::new(b.finish()),
        }
    }
}

/// The failures of one column: the row and its reason.
pub(crate) type Failures = Vec<(usize, String)>;

/// Coerces one column of raw rows. Rows that fail are null in the array.
pub(crate) fn json<'a>(
    f: &Field,
    t: Target,
    values: impl ExactSizeIterator<Item = Option<&'a Value>>,
    inf: Infinity,
) -> Result<(ArrayRef, Failures), Error> {
    let mut b = Builder::new(t, values.len(), inf).ok_or_else(|| raw_unsupported(f))?;
    let mut failed = Vec::new();
    for (row, v) in values.enumerate() {
        match v {
            None | Some(Value::Null) => b.null(),
            Some(v) => {
                if let Err(u) = b.json(v) {
                    b.null();
                    failed.push((row, u.reason(f.name())));
                }
            }
        }
    }
    Ok((b.finish(), failed))
}

pub(crate) fn raw_unsupported(f: &Field) -> Error {
    Error::Declared(format!(
        "column {:?} is declared {}, which raw rows can't fill yet",
        f.name(),
        f.data_type()
    ))
}

/// Gives a typed batch the logical types: timestamps and times in
/// microseconds, truncated where finer (timestamps in nanoseconds if the
/// stream keeps them), zoned timestamps in UTC, and
/// infinite dates and timestamps clamped or rejected; then text columns
/// parsed to their declared types. Returns the batch without the rows that
/// failed, and those rows' indexes in `batch` with reasons.
pub(crate) fn conform(
    batch: &RecordBatch,
    declared: Option<&Schema>,
    opts: TypeOptions,
) -> Result<(RecordBatch, Failures), Error> {
    let schema = batch.schema();
    let mut fields = Vec::with_capacity(schema.fields().len());
    let mut columns = Vec::with_capacity(batch.num_columns());
    let mut failed: HashMap<usize, String> = HashMap::new();
    let mut fail = |rows: Failures| {
        for (row, reason) in rows {
            failed.entry(row).or_insert(reason);
        }
    };
    for (f, a) in schema.fields().iter().zip(batch.columns()) {
        let (mut field, mut col) = (f.as_ref().clone(), a.clone());
        if let Some((dt, c, rows)) = temporal(f, a, opts) {
            fail(rows);
            field = field.with_data_type(dt).with_nullable(true);
            col = c;
        }
        let d = declared.and_then(|d| d.field_with_name(f.name()).ok());
        if let Some(d) = d {
            if field.data_type() != d.data_type() {
                let t = Target::of(d).ok_or_else(|| Error::Declared(f.name().clone()))?;
                let (c, rows) = match (field.data_type(), t) {
                    // A decimal column overridden to another type.
                    (DataType::Decimal128(..), Target::String | Target::Float64) => {
                        (arrow_cast::cast(&col, d.data_type())?, Vec::new())
                    }
                    _ => text_column(d, t, &col, opts.infinity).ok_or_else(|| Error::Mismatch {
                        column: f.name().clone(),
                        from: field.data_type().clone(),
                        to: d.data_type().clone(),
                    })?,
                };
                fail(rows);
                col = c;
            }
            field = d.clone().with_nullable(true);
        }
        fields.push(field);
        columns.push(col);
    }
    let out = RecordBatch::try_new_with_options(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        columns,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?;
    if failed.is_empty() {
        return Ok((out, Vec::new()));
    }
    let keep =
        BooleanArray::from_iter((0..batch.num_rows()).map(|r| Some(!failed.contains_key(&r))));
    let out = arrow_select::filter::filter_record_batch(&out, &keep)?;
    let mut rows: Failures = failed.into_iter().collect();
    rows.sort_by_key(|(r, _)| *r);
    Ok((out, rows))
}

/// Parses a text column to `t`. None if `a` isn't text or `t` has no parser.
pub(crate) fn text_column(
    f: &Field,
    t: Target,
    a: &ArrayRef,
    inf: Infinity,
) -> Option<(ArrayRef, Failures)> {
    let strings: Box<dyn Iterator<Item = Option<&str>>> = match a.data_type() {
        DataType::Utf8 => Box::new(a.as_string::<i32>().iter()),
        DataType::LargeUtf8 => Box::new(a.as_string::<i64>().iter()),
        DataType::Utf8View => Box::new(a.as_string_view().iter()),
        _ => return None,
    };
    let mut b = Builder::new(t, a.len(), inf)?;
    let mut failed = Vec::new();
    for (row, s) in strings.enumerate() {
        match s {
            None => b.null(),
            Some(s) => {
                if let Err(u) = b.text(s) {
                    b.null();
                    failed.push((row, u.reason(f.name())));
                }
            }
        }
    }
    Some((b.finish(), failed))
}

type Converted = Option<(DataType, ArrayRef, Failures)>;

fn unit_name(u: &TimeUnit) -> &'static str {
    match u {
        TimeUnit::Second => "seconds",
        TimeUnit::Millisecond => "milliseconds",
        TimeUnit::Microsecond => "microseconds",
        TimeUnit::Nanosecond => "nanoseconds",
    }
}

fn per_second(u: &TimeUnit) -> i64 {
    match u {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

/// Timestamps in microseconds, or nanoseconds if the stream keeps them,
/// times in microseconds, zoned timestamps in UTC, and infinite timestamps
/// and dates clamped or rejected; None if `a` needs none of that.
fn temporal(f: &Field, a: &ArrayRef, opts: TypeOptions) -> Converted {
    let name = f.name();
    let inf = opts.infinity;
    Some(match a.data_type() {
        DataType::Timestamp(unit, tz) => {
            let to = match opts.nanoseconds {
                true => TimeUnit::Nanosecond,
                false => TimeUnit::Microsecond,
            };
            let ticks = arrow_cast::cast(a, &DataType::Int64).ok()?;
            let infinite = ticks
                .as_primitive::<Int64Type>()
                .values()
                .iter()
                .any(|&v| v == i64::MAX || v == i64::MIN);
            if *unit == to && tz.as_deref().is_none_or(|z| z == "UTC") && !infinite {
                return None;
            }
            let (from_n, to_n) = (per_second(unit), per_second(&to));
            let (min, max) = limits(&to);
            let range = || Unfit::new("range", format!("is out of range for {}", unit_name(&to)));
            let (out, failed) = scale::<Int64Type, Int64Type>(&ticks, name, |v| match v {
                i64::MAX => inf.apply(max),
                i64::MIN => inf.apply(min),
                v if to_n >= from_n => v.checked_mul(to_n / from_n).ok_or_else(range),
                v => Ok(v.div_euclid(from_n / to_n)),
            });
            let tz = tz.as_ref().map(|_| Arc::<str>::from("UTC"));
            (
                DataType::Timestamp(to, tz.clone()),
                timestamps(out, to, tz),
                failed,
            )
        }
        DataType::Date32 => {
            let days = a.as_primitive::<Date32Type>();
            if !days
                .values()
                .iter()
                .any(|&v| v == i32::MAX || v == i32::MIN)
            {
                return None;
            }
            let (out, failed) = scale::<Date32Type, Date32Type>(a, name, |v| match v {
                i32::MAX => inf.apply(MAX_DATE),
                i32::MIN => inf.apply(MIN_DATE),
                v => Ok(v),
            });
            (DataType::Date32, Arc::new(out), failed)
        }
        DataType::Time32(unit) => {
            let (out, failed) = match unit {
                TimeUnit::Second => {
                    scale::<Time32SecondType, Time64MicrosecondType>(a, name, |v| {
                        Ok(i64::from(v) * 1_000_000)
                    })
                }
                _ => scale::<Time32MillisecondType, Time64MicrosecondType>(a, name, |v| {
                    Ok(i64::from(v) * 1_000)
                }),
            };
            (
                DataType::Time64(TimeUnit::Microsecond),
                Arc::new(out),
                failed,
            )
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let (out, failed) =
                scale::<Time64NanosecondType, Time64MicrosecondType>(a, name, |v| {
                    Ok(v.div_euclid(1_000))
                });
            (
                DataType::Time64(TimeUnit::Microsecond),
                Arc::new(out),
                failed,
            )
        }
        _ => return None,
    })
}

fn scale<I, O>(
    a: &ArrayRef,
    column: &str,
    f: impl Fn(I::Native) -> Result<O::Native, Unfit>,
) -> (PrimitiveArray<O>, Failures)
where
    I: ArrowPrimitiveType,
    O: ArrowPrimitiveType,
{
    let a = a.as_primitive::<I>();
    let mut failed = Vec::new();
    let out = a
        .iter()
        .enumerate()
        .map(|(row, v)| match f(v?) {
            Ok(v) => Some(v),
            Err(u) => {
                failed.push((row, u.reason(column)));
                None
            }
        })
        .collect();
    (out, failed)
}

/// Row `row` of a typed batch as a JSON object, for quarantine.
pub fn render_row(batch: &RecordBatch, row: usize) -> String {
    let opts = FormatOptions::new();
    let mut obj = serde_json::Map::new();
    for (f, a) in batch.schema().fields().iter().zip(batch.columns()) {
        let v = if a.is_null(row) {
            Value::Null
        } else if let Some(inf) = infinity_text(a, row) {
            Value::String(inf.to_owned())
        } else {
            let a = offset_zone(&a.slice(row, 1));
            let text = ArrayFormatter::try_new(a.as_ref(), &opts)
                .map(|fmt| fmt.value(0).to_string())
                .unwrap_or_default();
            match a.data_type() {
                DataType::Boolean => Value::Bool(text == "true"),
                dt if dt.is_integer() || dt.is_floating() => {
                    text.parse().map_or(Value::String(text), Value::Number)
                }
                _ => Value::String(text),
            }
        };
        obj.insert(f.name().clone(), v);
    }
    Value::Object(obj).to_string()
}

/// `a`, with a zoned timestamp's zone given as an offset: arrow displays and
/// casts named zones, `UTC` included, only with chrono-tz. Values are UTC
/// either way.
pub(crate) fn offset_zone(a: &ArrayRef) -> ArrayRef {
    let DataType::Timestamp(unit, Some(_)) = a.data_type() else {
        return a.clone();
    };
    let data = a
        .to_data()
        .into_builder()
        .data_type(DataType::Timestamp(*unit, Some("+00:00".into())));
    data.build().map_or_else(|_| a.clone(), make_array)
}

/// `infinity` or `-infinity` for a typed date or timestamp that stands for
/// one.
fn infinity_text(a: &ArrayRef, row: usize) -> Option<&'static str> {
    let v = match a.data_type() {
        DataType::Date32 => match a.as_primitive::<Date32Type>().values()[row] {
            i32::MAX => i64::MAX,
            i32::MIN => i64::MIN,
            _ => return None,
        },
        DataType::Timestamp(TimeUnit::Second, _) => {
            a.as_primitive::<TimestampSecondType>().values()[row]
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            a.as_primitive::<TimestampMillisecondType>().values()[row]
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            a.as_primitive::<TimestampMicrosecondType>().values()[row]
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            a.as_primitive::<TimestampNanosecondType>().values()[row]
        }
        _ => return None,
    };
    match v {
        i64::MAX => Some("infinity"),
        i64::MIN => Some("-infinity"),
        _ => None,
    }
}

/// An integer, also spelled as an exact decimal such as `2.0` or `1e3`.
fn int<T: TryFrom<i128>>(s: &str) -> Result<T, Unfit> {
    let v = match s.parse::<i64>() {
        Ok(v) => i128::from(v),
        Err(_) => decimal(s, 38, 0).map_err(|u| match u.code {
            "precision" => not_a("an integer"),
            _ => u,
        })?,
    };
    T::try_from(v).map_err(|_| Unfit::new("range", "is out of range"))
}

fn float<T: std::str::FromStr + Into<f64> + Copy>(s: &str) -> Result<T, Unfit> {
    let v: T = s.parse().map_err(|_| not_a("a number"))?;
    if v.into().is_infinite() && !s.to_ascii_lowercase().contains("inf") {
        return Err(Unfit::new("range", "is out of range"));
    }
    Ok(v)
}

/// Exactly, or not at all.
fn decimal(s: &str, p: u8, scale: i8) -> Result<i128, Unfit> {
    let bare = s.strip_prefix(['+', '-']).unwrap_or(s);
    if bare.eq_ignore_ascii_case("nan") {
        return Err(Unfit::new(
            "nan",
            format!("is NaN, which decimal({p}, {scale}) can't hold"),
        ));
    }
    if bare.eq_ignore_ascii_case("infinity") || bare.eq_ignore_ascii_case("inf") {
        return Err(Unfit::new(
            "range",
            format!("is infinite, which decimal({p}, {scale}) can't hold"),
        ));
    }
    let negative = s.starts_with('-');
    let (mantissa, exp) = match bare.find(['e', 'E']) {
        Some(i) => (
            &bare[..i],
            bare[i + 1..]
                .parse::<i32>()
                .map_err(|_| not_a("a number"))?,
        ),
        None => (bare, 0),
    };
    let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if int.is_empty() && frac.is_empty()
        || !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit())
    {
        return Err(not_a("a number"));
    }
    let digits: Vec<u8> = int.bytes().chain(frac.bytes()).map(|b| b - b'0').collect();
    // The value is digits × 10^(exp − frac.len()); scaled, × 10^shift.
    let shift = i64::from(exp) - frac.len() as i64 + i64::from(scale);
    let Some(first) = digits.iter().position(|&d| d != 0) else {
        return Ok(0);
    };
    let digits = &digits[first..];
    let kept = if shift >= 0 {
        digits
    } else {
        let cut = usize::try_from(-shift).unwrap_or(usize::MAX);
        if cut >= digits.len() {
            return Err(precision(p, scale));
        }
        let (kept, dropped) = digits.split_at(digits.len() - cut);
        if dropped.iter().any(|&d| d != 0) {
            return Err(precision(p, scale));
        }
        kept
    };
    let zeros = shift.max(0);
    if kept.len() as i64 + zeros > i64::from(p) {
        return Err(precision(p, scale));
    }
    let mut v: i128 = 0;
    for &d in kept {
        v = v * 10 + i128::from(d);
    }
    for _ in 0..zeros {
        v *= 10;
    }
    Ok(if negative { -v } else { v })
}

fn precision(p: u8, s: i8) -> Unfit {
    Unfit::new("precision", format!("doesn't fit decimal({p}, {s})"))
}

fn digits(s: &str, n: usize) -> Option<(u32, &str)> {
    let d = s.get(..n)?;
    if !d.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some((d.parse().ok()?, &s[n..]))
}

fn infinity<T>(s: &str, inf: Infinity, max: T, min: T) -> Option<Result<T, Unfit>> {
    match s {
        "infinity" | "+infinity" => Some(inf.apply(max)),
        "-infinity" => Some(inf.apply(min)),
        _ => None,
    }
}

/// `YYYY-MM-DD`, at least four year digits; days since 1970-01-01.
fn date_part(s: &str) -> Option<(i64, &str)> {
    let year_len = s.bytes().take_while(u8::is_ascii_digit).count();
    if year_len < 4 {
        return None;
    }
    let (y, rest) = digits(s, year_len)?;
    let (m, rest) = digits(rest.strip_prefix('-')?, 2)?;
    let (d, rest) = digits(rest.strip_prefix('-')?, 2)?;
    let date = NaiveDate::from_ymd_opt(i32::try_from(y).ok()?, m, d)?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    Some(((date - epoch).num_days(), rest))
}

fn date(s: &str, inf: Infinity) -> Result<i32, Unfit> {
    if let Some(d) = infinity(s, inf, MAX_DATE, MIN_DATE) {
        return d;
    }
    match date_part(s) {
        Some((d, "")) => i32::try_from(d).map_err(|_| Unfit::new("range", "is out of range")),
        _ => Err(not_a("a date")),
    }
}

/// `HH:MM:SS[.fraction]`; nanoseconds since midnight, truncated.
fn time_part(s: &str) -> Option<(i64, &str)> {
    let (h, rest) = digits(s, 2)?;
    let (m, rest) = digits(rest.strip_prefix(':')?, 2)?;
    let (sec, mut rest) = digits(rest.strip_prefix(':')?, 2)?;
    let mut nanos = 0;
    if let Some(f) = rest.strip_prefix('.') {
        let n = f.bytes().take_while(u8::is_ascii_digit).count();
        if n == 0 {
            return None;
        }
        for (i, b) in f.bytes().take(n.min(9)).enumerate() {
            nanos += i64::from(b - b'0') * 10_i64.pow(8 - i as u32);
        }
        rest = &f[n..];
    }
    let t = (i64::from(h) * 3600 + i64::from(m) * 60 + i64::from(sec)) * 1_000_000_000 + nanos;
    let valid = (h < 24 && m < 60 && sec < 60) || t == NANOS_PER_DAY;
    valid.then_some((t, rest))
}

/// Microseconds since midnight.
fn time(s: &str) -> Result<i64, Unfit> {
    match time_part(s) {
        Some((t, "")) => Ok(t / 1_000),
        _ => Err(not_a("a time")),
    }
}

/// A UTC offset in seconds: `Z`, `±HH`, `±HHMM` or `±HH:MM`.
fn offset(s: &str) -> Option<i64> {
    if s.eq_ignore_ascii_case("z") {
        return Some(0);
    }
    let sign = match s.as_bytes().first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let (h, rest) = digits(&s[1..], 2)?;
    let m = match rest {
        "" => 0,
        r => {
            digits(r.strip_prefix(':').unwrap_or(r), 2)
                .filter(|(_, r)| r.is_empty())?
                .0
        }
    };
    (h < 24 && m < 60).then_some(sign * i64::from(h * 3600 + m * 60))
}

fn timestamp(s: &str, ts: Ts) -> Result<i64, Unfit> {
    let (min, max) = limits(&ts.unit());
    if let Some(t) = infinity(s, ts.inf, max, min) {
        return t;
    }
    let parsed = date_part(s).and_then(|(days, rest)| {
        let rest = rest.strip_prefix(['T', 't', ' '])?;
        let (t, zone) = time_part(rest)?;
        Some((
            i128::from(days) * i128::from(NANOS_PER_DAY) + i128::from(t),
            zone,
        ))
    });
    let Some((local, zone)) = parsed else {
        return Err(not_a("a date-time"));
    };
    let nanos = match (zone, ts.utc) {
        ("", true) => return Err(Unfit::new("type", "has no UTC offset")),
        ("", false) => local,
        (_, false) => {
            return Err(Unfit::new(
                "type",
                "has a UTC offset, but its column has no time zone",
            ));
        }
        (z, true) => match offset(z) {
            Some(o) => local - i128::from(o) * 1_000_000_000,
            None => return Err(not_a("a date-time")),
        },
    };
    let v = match ts.nanos {
        true => nanos,
        false => nanos.div_euclid(1_000),
    };
    i64::try_from(v).map_err(|_| {
        Unfit::new(
            "range",
            format!("is out of range for {}", unit_name(&ts.unit())),
        )
    })
}

fn uuid(s: &str) -> Option<[u8; 16]> {
    let hex: Vec<u8> = match s.len() {
        32 => s.bytes().collect(),
        36 if [8, 13, 18, 23].iter().all(|&i| s.as_bytes()[i] == b'-') => {
            s.bytes().filter(|&b| b != b'-').collect()
        }
        _ => return None,
    };
    let mut out = [0; 16];
    for (i, pair) in hex.chunks(2).enumerate() {
        let pair = std::str::from_utf8(pair).ok()?;
        out[i] = u8::from_str_radix(pair, 16).ok()?;
    }
    Some(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use arrow_array::StringArray;
    use arrow_array::types::{Decimal128Type, Int16Type};

    const C: Infinity = Infinity::Clamp;

    fn us(utc: bool) -> Ts {
        Ts {
            utc,
            nanos: false,
            inf: C,
        }
    }

    fn reason(r: Result<impl std::fmt::Debug, Unfit>) -> String {
        match r {
            Ok(v) => panic!("fits: {v:?}"),
            Err(u) => u.reason("c"),
        }
    }

    #[test]
    fn decimals_fit_exactly_or_not_at_all() {
        assert_eq!(decimal("12.30", 12, 2).ok(), Some(1230));
        assert_eq!(decimal("-0.5", 3, 2).ok(), Some(-50));
        assert_eq!(decimal("1.5e3", 10, 0).ok(), Some(1500));
        assert_eq!(decimal("1200e-2", 4, 0).ok(), Some(12));
        assert_eq!(decimal("0.000", 1, 0).ok(), Some(0));
        assert_eq!(decimal(".5", 2, 1).ok(), Some(5));
        assert_eq!(decimal("000123", 3, 0).ok(), Some(123));
        let big = "12345678901234567890123456789.123456789";
        assert_eq!(
            decimal(big, 38, 9).ok(),
            Some(12345678901234567890123456789123456789)
        );

        assert!(reason(decimal("1.001", 10, 2)).starts_with("precision:"));
        assert!(reason(decimal("1000", 3, 0)).starts_with("precision:"));
        assert!(reason(decimal("1e-20", 38, 9)).starts_with("precision:"));
        assert!(reason(decimal(&format!("{big}1"), 38, 9)).starts_with("precision:"));
        assert!(reason(decimal("NaN", 38, 9)).starts_with("nan:"));
        assert!(reason(decimal("-Infinity", 38, 9)).starts_with("range:"));
        for bad in ["", ".", "1.2.3", "abc", "1e", "--1", "1 "] {
            assert!(reason(decimal(bad, 38, 9)).starts_with("type:"), "{bad:?}");
        }
    }

    #[test]
    fn timestamps_are_microseconds_since_the_epoch() {
        assert_eq!(
            timestamp("1970-01-01T00:00:01Z", us(true)).ok(),
            Some(1_000_000)
        );
        assert_eq!(timestamp("1970-01-01 01:00:00+01", us(true)).ok(), Some(0));
        assert_eq!(
            timestamp("1970-01-01T05:30:00+05:30", us(true)).ok(),
            Some(0)
        );
        assert_eq!(
            timestamp("1969-12-31T23:59:59.9999999Z", us(true)).ok(),
            Some(-1)
        );
        assert_eq!(
            timestamp("2000-01-01 00:00:00.123456789", us(false)).ok(),
            Some(946_684_800_123_456)
        );
        assert_eq!(timestamp("infinity", us(true)).ok(), Some(MAX_TIMESTAMP));
        assert_eq!(timestamp("-infinity", us(false)).ok(), Some(MIN_TIMESTAMP));
        assert_eq!(
            timestamp("9999-12-31 23:59:59.999999", us(false)).ok(),
            Some(MAX_TIMESTAMP)
        );
        assert_eq!(
            timestamp("0001-01-01T00:00:00", us(false)).ok(),
            Some(MIN_TIMESTAMP)
        );

        assert!(reason(timestamp("2000-01-01T00:00:00", us(true))).contains("no UTC offset"));
        assert!(reason(timestamp("2000-01-01T00:00:00Z", us(false))).contains("has a UTC offset"));
        for bad in [
            "2000-02-30T00:00:00Z",
            "2000-01-01",
            "2000-01-01T25:00:00Z",
            "x",
            "2000-01-01T00:00:00+0",
        ] {
            assert!(
                reason(timestamp(bad, us(true))).starts_with("type:"),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn dates_and_times() {
        assert_eq!(date("1970-01-02", C).ok(), Some(1));
        assert_eq!(date("9999-12-31", C).ok(), Some(MAX_DATE));
        assert_eq!(date("0001-01-01", C).ok(), Some(MIN_DATE));
        assert_eq!(date("infinity", C).ok(), Some(MAX_DATE));
        assert_eq!(date("-infinity", C).ok(), Some(MIN_DATE));
        assert!(reason(date("2001-02-29", C)).starts_with("type:"));
        assert_eq!(time("00:00:01.5").ok(), Some(1_500_000));
        assert_eq!(
            time("23:59:59.9999999").ok(),
            Some((NANOS_PER_DAY / 1_000) - 1)
        );
        assert_eq!(time("24:00:00").ok(), Some(NANOS_PER_DAY / 1_000));
        assert!(reason(time("24:00:01")).starts_with("type:"));
        assert!(reason(time("12:00:00+02")).starts_with("type:"));
    }

    #[test]
    fn json_values_coerce_to_declared_types() {
        let rows: Vec<Value> = ["1", "\"2\"", "1.5", "70000", "true", "null"]
            .iter()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        let f = Field::new("c", DataType::Int16, true);
        let (a, failed) = json(&f, Target::Int16, rows.iter().map(Some), C).unwrap();
        let a = a.as_primitive::<Int16Type>();
        assert_eq!(
            a.iter().collect::<Vec<_>>(),
            vec![Some(1), Some(2), None, None, None, None]
        );
        let codes: Vec<(usize, &str)> = failed
            .iter()
            .map(|(r, why)| (*r, why.split(':').next().unwrap()))
            .collect();
        assert_eq!(codes, vec![(2, "type"), (3, "range"), (4, "type")]);
    }

    #[test]
    fn json_numbers_keep_their_digits_in_decimals() {
        let v: Value = serde_json::from_str("12345678901234567890.123456789").unwrap();
        let f = Field::new("c", DataType::Decimal128(38, 9), true);
        let (a, failed) = json(&f, Target::Decimal(38, 9), [Some(&v)].into_iter(), C).unwrap();
        assert!(failed.is_empty());
        assert_eq!(
            a.as_primitive::<Decimal128Type>().values()[0],
            12345678901234567890123456789
        );
    }

    #[test]
    fn uuids_and_binary() {
        let u = uuid("0190b8a4-1f2e-7c3d-8e4f-5a6b7c8d9e0f").unwrap();
        assert_eq!(u[0], 0x01);
        assert_eq!(u, uuid("0190B8A41F2E7C3D8E4F5A6B7C8D9E0F").unwrap());
        assert!(uuid("0190b8a4-1f2e-7c3d-8e4f-5a6b7c8d9e0").is_none());
        assert!(uuid("0190b8a4_1f2e_7c3d_8e4f_5a6b7c8d9e0f").is_none());

        let mut b = Builder::new(Target::Binary, 2, C).unwrap();
        assert!(b.text("aGk=").is_ok());
        assert!(b.text("!!").is_err());
    }

    #[test]
    fn typed_batches_get_microseconds_and_declared_types() {
        use arrow_array::TimestampNanosecondArray;
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "at",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+02:00".into())),
                true,
            ),
            Field::new("price", DataType::Utf8, true),
            Field::new("other", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(
                    TimestampNanosecondArray::from(vec![Some(-1), Some(1_999), None])
                        .with_timezone("+02:00"),
                ),
                Arc::new(StringArray::from(vec![Some("1.5"), Some("NaN"), None])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            ],
        )
        .unwrap();
        let declared = Schema::new(vec![Field::new(
            "price",
            DataType::Decimal128(10, 2),
            false,
        )]);
        let (out, failed) = conform(&batch, Some(&declared), TypeOptions::default()).unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].0, 1);
        assert!(
            failed[0].1.starts_with("nan: column \"price\""),
            "{}",
            failed[0].1
        );
        assert_eq!(out.num_rows(), 2);
        assert_eq!(
            out.schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        let at = out.column(0).as_primitive::<TimestampMicrosecondType>();
        assert_eq!(at.iter().collect::<Vec<_>>(), vec![Some(-1), None]);
        assert_eq!(
            out.schema().field(1).data_type(),
            &DataType::Decimal128(10, 2)
        );
        assert!(out.schema().field(1).is_nullable());
        assert_eq!(
            out.column(1).as_primitive::<Decimal128Type>().values()[0],
            150
        );

        let mismatch = Schema::new(vec![Field::new("at", DataType::Int64, true)]);
        assert!(matches!(
            conform(&batch, Some(&mismatch), TypeOptions::default()),
            Err(Error::Mismatch { .. })
        ));
    }

    #[test]
    fn infinities_clamp_or_quarantine() {
        use arrow_array::{Date32Array, TimestampMicrosecondArray, TimestampNanosecondArray};
        let q = Infinity::Quarantine;
        assert_eq!(
            reason(date("infinity", q)),
            "range: column \"c\" is infinite"
        );
        assert!(reason(timestamp("-infinity", Ts { inf: q, ..us(true) })).starts_with("range:"));
        assert_eq!(date("2026-11-01", q).ok(), Some(20_758));

        let batch = RecordBatch::try_from_iter([
            (
                "d",
                Arc::new(Date32Array::from(vec![
                    Some(i32::MAX),
                    Some(1),
                    None,
                    Some(2),
                ])) as ArrayRef,
            ),
            (
                "t",
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(5), Some(i64::MIN), None, Some(6)])
                        .with_timezone("UTC"),
                ),
            ),
            (
                "n",
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(1_999),
                    None,
                    Some(i64::MAX),
                    Some(7_000),
                ])),
            ),
        ])
        .unwrap();
        let (out, failed) = conform(&batch, None, TypeOptions::default()).unwrap();
        assert!(failed.is_empty());
        let d = out.column(0).as_primitive::<Date32Type>();
        assert_eq!(
            d.iter().collect::<Vec<_>>(),
            vec![Some(MAX_DATE), Some(1), None, Some(2)]
        );
        let t = out.column(1).as_primitive::<TimestampMicrosecondType>();
        assert_eq!(
            t.iter().collect::<Vec<_>>(),
            vec![Some(5), Some(MIN_TIMESTAMP), None, Some(6)]
        );
        let n = out.column(2).as_primitive::<TimestampMicrosecondType>();
        assert_eq!(
            n.iter().collect::<Vec<_>>(),
            vec![Some(1), None, Some(MAX_TIMESTAMP), Some(7)]
        );

        let (out, failed) = conform(
            &batch,
            None,
            TypeOptions {
                infinity: q,
                ..Default::default()
            },
        )
        .unwrap();
        let rows: Vec<(usize, &str)> = failed.iter().map(|(r, w)| (*r, w.as_str())).collect();
        assert_eq!(
            rows,
            vec![
                (0, "range: column \"d\" is infinite"),
                (1, "range: column \"t\" is infinite"),
                (2, "range: column \"n\" is infinite"),
            ]
        );
        assert_eq!(out.num_rows(), 1);
        let n = out.column(2).as_primitive::<TimestampMicrosecondType>();
        assert_eq!(n.values()[0], 7);
        assert_eq!(
            render_row(&batch, 3),
            r#"{"d":"1970-01-03","t":"1970-01-01T00:00:00.000006Z","n":"1970-01-01T00:00:00.000007"}"#
        );
        assert!(render_row(&batch, 0).contains(r#""d":"infinity""#));
        assert!(render_row(&batch, 1).contains(r#""t":"-infinity""#));
    }

    #[test]
    fn streams_can_keep_nanoseconds() {
        use arrow_array::{
            TimestampMicrosecondArray, TimestampNanosecondArray, TimestampSecondArray,
        };
        let ns = Ts {
            nanos: true,
            ..us(false)
        };
        assert_eq!(
            timestamp("2000-01-01 00:00:00.123456789", ns).ok(),
            Some(946_684_800_123_456_789)
        );
        assert_eq!(timestamp("infinity", ns).ok(), Some(i64::MAX));
        assert_eq!(
            reason(timestamp("2300-01-01 00:00:00", ns)),
            "range: column \"c\" is out of range for nanoseconds"
        );

        let batch = RecordBatch::try_from_iter([
            (
                "n",
                Arc::new(TimestampNanosecondArray::from(vec![1_999, 5])) as ArrayRef,
            ),
            (
                "u",
                Arc::new(TimestampMicrosecondArray::from(vec![i64::MIN, 6]).with_timezone("UTC")),
            ),
            (
                "s",
                Arc::new(TimestampSecondArray::from(vec![1, 10_000_000_000])),
            ),
        ])
        .unwrap();
        let opts = TypeOptions {
            nanoseconds: true,
            ..Default::default()
        };
        let (out, failed) = conform(&batch, None, opts).unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].0, 1);
        assert_eq!(
            failed[0].1,
            "range: column \"s\" is out of range for nanoseconds"
        );
        let schema = out.schema();
        assert_eq!(
            schema.field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
        let col = |i: usize| {
            out.column(i)
                .as_primitive::<TimestampNanosecondType>()
                .values()
                .to_vec()
        };
        assert_eq!(col(0), vec![1_999]);
        assert_eq!(col(1), vec![i64::MIN]);
        assert_eq!(col(2), vec![1_000_000_000]);

        let declared = Schema::new(vec![Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]);
        assert_eq!(
            nanoseconds(&declared).field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }

    #[test]
    fn typed_decimals_cast_to_their_override() {
        use arrow_array::Decimal128Array;
        use arrow_array::types::Float64Type;
        let d = Decimal128Array::from(vec![Some(1_500_000_000), None])
            .with_precision_and_scale(38, 9)
            .unwrap();
        let batch = RecordBatch::try_from_iter([
            ("s", Arc::new(d.clone()) as ArrayRef),
            ("f", Arc::new(d) as ArrayRef),
        ])
        .unwrap();
        let declared = Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("f", DataType::Float64, true),
        ]);
        let (out, failed) = conform(&batch, Some(&declared), TypeOptions::default()).unwrap();
        assert!(failed.is_empty());
        let s = out.column(0).as_string::<i32>();
        assert_eq!(
            s.iter().collect::<Vec<_>>(),
            vec![Some("1.500000000"), None]
        );
        let f = out.column(1).as_primitive::<Float64Type>();
        assert_eq!(f.iter().collect::<Vec<_>>(), vec![Some(1.5), None]);
    }

    #[test]
    fn renders_typed_rows_as_json() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("b", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![Some(7)])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(BooleanArray::from(vec![Some(true)])),
            ],
        )
        .unwrap();
        assert_eq!(render_row(&batch, 0), r#"{"n":7,"s":null,"b":true}"#);
    }

    #[test]
    fn checks_declared_schemas() {
        let ok = Schema::new(vec![
            Field::new("d", DataType::Decimal128(38, 9), true),
            Field::new(
                "l",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
        ]);
        assert!(check_declared(&ok).is_ok());
        for dt in [
            DataType::UInt64,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Decimal128(39, 0),
        ] {
            let bad = Schema::new(vec![Field::new("x", dt, true)]);
            assert!(check_declared(&bad).is_err());
        }
    }
}
