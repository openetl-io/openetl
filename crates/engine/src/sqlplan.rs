//! `openetl-engine plan`: plans a transform's SQL for the control plane,
//! which doesn't link the engine. It reads a request as JSON and writes the
//! answer as JSON. No job runs, and nothing is read but the request.
//!
//! The request:
//!
//! ```json
//! {"sql": "INSERT INTO …",
//!  "streams": [{"name": "public.orders",
//!               "columns": [{"name": "id", "type": "int64"}],
//!               "key": ["id"], "changelog": true, "nanoseconds": false}]}
//! ```
//!
//! A column's type is Arrow's, named as the catalog names it: `int64`,
//! `utf8`, `decimal(10, 2)`, `timestamp[us, tz=UTC]`,
//! `list<item: int64, nullable>`, and `extension<arrow.uuid>` or
//! `extension<arrow.json>` for those logical types. Statements see every
//! column as nullable, as the engine's first half types them, and a stream
//! with `nanoseconds` keeps its timestamps in nanoseconds.
//!
//! The answer has `statements`, `views`, `notes` and `errors`. Each
//! statement has where its `INSERT` is (`at`, a line and column from 1), its
//! `output`, whether it `replaces` the source stream of that name, its
//! `class` and `tier`, the source streams it reads (`inputs`) and the views
//! (`views`), the output's `columns`, its `arrow_schema` (an Arrow IPC stream
//! holding only the schema, in base64), its `key`, the columns its rows are
//! exchanged by (`exchange`, null for none) and its stateful operator's ID
//! (`operator`, null for none). Each error has a `kind`, a `class` and
//! `tier` for a later tier's statement, an `at` unless it isn't about the
//! text, and a `message`.
//!
//! Exit codes: 0 when the SQL planned; 1 when it has errors, which the
//! answer lists; 2 when the call failed (usage, or a request that can't be
//! read), with the reason on stderr.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, IntervalUnit, Schema, SchemaRef, TimeUnit};
use base64::Engine as _;
use openetl_normalize::{EXTENSION_METADATA_KEY, EXTENSION_NAME_KEY};
use openetl_sql::{Catalog, ErrorKind, Operator, Position, Statement, Stream};
use serde::{Deserialize, Serialize};

use crate::ipc;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub sql: String,
    #[serde(default)]
    pub streams: Vec<RequestStream>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestStream {
    /// Its whole name, as the source's catalog lists it.
    pub name: String,
    pub columns: Vec<RequestColumn>,
    /// The primary key's columns; empty if it has none.
    #[serde(default)]
    pub key: Vec<String>,
    /// Its rows are inserts, updates and deletes of their key, as CDC's are.
    #[serde(default)]
    pub changelog: bool,
    /// Its timestamps keep nanoseconds.
    #[serde(default)]
    pub nanoseconds: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
}

/// What planning gave. With errors, the rest is empty.
#[derive(Debug, Default, Serialize)]
pub struct Answer {
    pub statements: Vec<AnswerStatement>,
    pub views: Vec<AnswerView>,
    pub notes: Vec<AnswerNote>,
    pub errors: Vec<AnswerError>,
}

#[derive(Debug, Serialize)]
pub struct AnswerStatement {
    pub at: At,
    pub output: String,
    pub replaces: bool,
    pub class: &'static str,
    pub tier: u8,
    pub inputs: Vec<String>,
    pub views: Vec<String>,
    pub columns: Vec<AnswerColumn>,
    pub arrow_schema: String,
    pub key: Vec<String>,
    pub exchange: Option<Vec<String>>,
    pub operator: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AnswerView {
    pub name: String,
    pub at: At,
    pub inputs: Vec<String>,
    pub columns: Vec<AnswerColumn>,
    pub key: Vec<String>,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct AnswerColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub nullable: bool,
}

#[derive(Debug, Serialize)]
pub struct AnswerNote {
    pub at: At,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct AnswerError {
    pub kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub class: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tier: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub at: Option<At>,
    pub message: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct At {
    pub line: u64,
    pub column: u64,
}

impl From<Position> for At {
    fn from(p: Position) -> Self {
        At {
            line: p.line,
            column: p.column,
        }
    }
}

/// Plans the request's SQL. Fails only when the request itself is wrong:
/// a type it doesn't know, or a catalog the planner refuses.
pub fn plan(req: &Request) -> Result<Answer, String> {
    let mut catalog = Catalog::new();
    for s in &req.streams {
        catalog
            .insert(s.name.clone(), stream(s)?)
            .map_err(|e| e.message)?;
    }
    let planned = match openetl_sql::plan(&req.sql, &catalog) {
        Ok(p) => p,
        Err(errors) => {
            return Ok(Answer {
                errors: errors.iter().map(error).collect(),
                ..Answer::default()
            });
        }
    };
    let mut out = Answer::default();
    for s in &planned.statements {
        out.statements.push(statement(s)?);
    }
    for v in &planned.views {
        out.views.push(AnswerView {
            name: v.name.clone(),
            at: v.at.into(),
            inputs: v.inputs.clone(),
            columns: columns(&v.schema),
            key: v.key.clone(),
        });
    }
    out.notes = planned
        .notes
        .iter()
        .map(|n| AnswerNote {
            at: n.at.into(),
            message: n.message.clone(),
        })
        .collect();
    Ok(out)
}

/// A stream as statements see it.
fn stream(s: &RequestStream) -> Result<Stream, String> {
    let mut fields = Vec::with_capacity(s.columns.len());
    for c in &s.columns {
        let f = field(&c.name, &c.ty)
            .map_err(|e| format!("stream {} column {}: {e}", s.name, c.name))?;
        let f = match f.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, tz) if s.nanoseconds => f
                .clone()
                .with_data_type(DataType::Timestamp(TimeUnit::Nanosecond, tz.clone())),
            _ => f,
        };
        fields.push(f.with_nullable(true));
    }
    Ok(Stream {
        schema: Arc::new(Schema::new(fields)),
        key: s.key.clone(),
        changelog: s.changelog,
    })
}

fn statement(s: &Statement) -> Result<AnswerStatement, String> {
    let schema = ipc::encode_schema(&s.schema)
        .map_err(|e| format!("statement at {}: its schema: {e}", s.at))?;
    Ok(AnswerStatement {
        at: s.at.into(),
        output: s.output.clone(),
        replaces: s.replaces,
        class: s.class.code(),
        tier: s.class.tier(),
        inputs: s.inputs.clone(),
        views: s.views.clone(),
        columns: columns(&s.schema),
        arrow_schema: base64::engine::general_purpose::STANDARD.encode(schema),
        key: s.key.clone(),
        exchange: s.exchange().map(<[String]>::to_vec),
        operator: operator_id(&s.operator),
    })
}

/// The operator's ID, which only stateful operators have.
fn operator_id(op: &Operator) -> Option<String> {
    match op {
        Operator::RowWise(_) => None,
    }
}

fn error(e: &openetl_sql::Error) -> AnswerError {
    let class = match e.kind {
        ErrorKind::LaterTier(c) => Some(c),
        _ => None,
    };
    AnswerError {
        kind: e.kind.code(),
        class: class.map(|c| c.code()),
        tier: class.map(|c| c.tier()),
        at: e.at.map(At::from),
        message: e.message.clone(),
    }
}

fn columns(schema: &SchemaRef) -> Vec<AnswerColumn> {
    schema
        .fields()
        .iter()
        .map(|f| AnswerColumn {
            name: f.name().clone(),
            ty: type_name(f),
            nullable: f.is_nullable(),
        })
        .collect()
}

/// A field of a type named as the catalog names it.
pub fn field(name: &str, ty: &str) -> Result<Field, String> {
    if let Some(ext) = ty
        .strip_prefix("extension<")
        .and_then(|t| t.strip_suffix('>'))
    {
        let dt = match ext {
            openetl_normalize::UUID_EXTENSION => DataType::FixedSizeBinary(16),
            openetl_normalize::JSON_EXTENSION => DataType::Utf8,
            _ => return Err(format!("{ty} isn't a type the engine knows")),
        };
        return Ok(Field::new(name, dt, true).with_metadata(HashMap::from([
            (EXTENSION_NAME_KEY.to_owned(), ext.to_owned()),
            (EXTENSION_METADATA_KEY.to_owned(), String::new()),
        ])));
    }
    Ok(Field::new(name, data_type(ty)?, true))
}

fn data_type(t: &str) -> Result<DataType, String> {
    use DataType as T;
    let unknown = || format!("{t} isn't a type the engine knows");
    Ok(match t {
        "null" => T::Null,
        "bool" => T::Boolean,
        "int8" => T::Int8,
        "int16" => T::Int16,
        "int32" => T::Int32,
        "int64" => T::Int64,
        "uint8" => T::UInt8,
        "uint16" => T::UInt16,
        "uint32" => T::UInt32,
        "uint64" => T::UInt64,
        "float16" => T::Float16,
        "float32" => T::Float32,
        "float64" => T::Float64,
        "utf8" => T::Utf8,
        "large_utf8" => T::LargeUtf8,
        "utf8_view" => T::Utf8View,
        "binary" => T::Binary,
        "large_binary" => T::LargeBinary,
        "binary_view" => T::BinaryView,
        "date32" => T::Date32,
        "date64" => T::Date64,
        "time32[s]" => T::Time32(TimeUnit::Second),
        "time32[ms]" => T::Time32(TimeUnit::Millisecond),
        "time64[us]" => T::Time64(TimeUnit::Microsecond),
        "time64[ns]" => T::Time64(TimeUnit::Nanosecond),
        "month_interval" => T::Interval(IntervalUnit::YearMonth),
        "day_time_interval" => T::Interval(IntervalUnit::DayTime),
        "month_day_nano_interval" => T::Interval(IntervalUnit::MonthDayNano),
        _ => {
            if let Some(rest) = t
                .strip_prefix("timestamp[")
                .and_then(|r| r.strip_suffix(']'))
            {
                let (unit, tz) = match rest.split_once(", tz=") {
                    Some((u, tz)) => (u, Some(tz.into())),
                    None => (rest, None),
                };
                T::Timestamp(unit_of(unit).ok_or_else(unknown)?, tz)
            } else if let Some(n) = t
                .strip_prefix("fixed_size_binary[")
                .and_then(|r| r.strip_suffix(']'))
            {
                T::FixedSizeBinary(n.parse().map_err(|_| unknown())?)
            } else if let Some(ps) = t.strip_prefix("decimal(").and_then(|r| r.strip_suffix(')')) {
                let (p, s) = precision_scale(ps).ok_or_else(unknown)?;
                T::Decimal128(p, s)
            } else if let Some(ps) = t
                .strip_prefix("decimal256(")
                .and_then(|r| r.strip_suffix(')'))
            {
                let (p, s) = precision_scale(ps).ok_or_else(unknown)?;
                T::Decimal256(p, s)
            } else if let Some(item) = t.strip_prefix("list<").and_then(|r| r.strip_suffix('>')) {
                T::List(Arc::new(list_item(item).ok_or_else(unknown)?))
            } else if let Some(item) = t
                .strip_prefix("large_list<")
                .and_then(|r| r.strip_suffix('>'))
            {
                T::LargeList(Arc::new(list_item(item).ok_or_else(unknown)?))
            } else {
                return Err(unknown());
            }
        }
    })
}

fn unit_of(u: &str) -> Option<TimeUnit> {
    Some(match u {
        "s" => TimeUnit::Second,
        "ms" => TimeUnit::Millisecond,
        "us" => TimeUnit::Microsecond,
        "ns" => TimeUnit::Nanosecond,
        _ => return None,
    })
}

fn unit_name(u: &TimeUnit) -> &'static str {
    match u {
        TimeUnit::Second => "s",
        TimeUnit::Millisecond => "ms",
        TimeUnit::Microsecond => "us",
        TimeUnit::Nanosecond => "ns",
    }
}

fn precision_scale(ps: &str) -> Option<(u8, i8)> {
    let (p, s) = ps.split_once(", ")?;
    Some((p.parse().ok()?, s.parse().ok()?))
}

/// A list's item, as `NAME: TYPE` with `, nullable` if it is.
fn list_item(item: &str) -> Option<Field> {
    let (name, rest) = item.split_once(": ")?;
    let (ty, nullable) = match rest.strip_suffix(", nullable") {
        Some(ty) => (ty, true),
        None => (rest, false),
    };
    Some(field(name, ty).ok()?.with_nullable(nullable))
}

/// The name [`field`] reads back as the field's type.
pub fn type_name(f: &Field) -> String {
    if let Some(ext) = f.metadata().get(EXTENSION_NAME_KEY) {
        return format!("extension<{ext}>");
    }
    data_type_name(f.data_type())
}

fn data_type_name(t: &DataType) -> String {
    use DataType as T;
    match t {
        T::Null => "null".into(),
        T::Boolean => "bool".into(),
        T::Int8 => "int8".into(),
        T::Int16 => "int16".into(),
        T::Int32 => "int32".into(),
        T::Int64 => "int64".into(),
        T::UInt8 => "uint8".into(),
        T::UInt16 => "uint16".into(),
        T::UInt32 => "uint32".into(),
        T::UInt64 => "uint64".into(),
        T::Float16 => "float16".into(),
        T::Float32 => "float32".into(),
        T::Float64 => "float64".into(),
        T::Utf8 => "utf8".into(),
        T::LargeUtf8 => "large_utf8".into(),
        T::Utf8View => "utf8_view".into(),
        T::Binary => "binary".into(),
        T::LargeBinary => "large_binary".into(),
        T::BinaryView => "binary_view".into(),
        T::Date32 => "date32".into(),
        T::Date64 => "date64".into(),
        T::Time32(u) => format!("time32[{}]", unit_name(u)),
        T::Time64(u) => format!("time64[{}]", unit_name(u)),
        T::Interval(IntervalUnit::YearMonth) => "month_interval".into(),
        T::Interval(IntervalUnit::DayTime) => "day_time_interval".into(),
        T::Interval(IntervalUnit::MonthDayNano) => "month_day_nano_interval".into(),
        T::Timestamp(u, Some(tz)) => format!("timestamp[{}, tz={tz}]", unit_name(u)),
        T::Timestamp(u, None) => format!("timestamp[{}]", unit_name(u)),
        T::FixedSizeBinary(n) => format!("fixed_size_binary[{n}]"),
        T::Decimal128(p, s) => format!("decimal({p}, {s})"),
        T::Decimal256(p, s) => format!("decimal256({p}, {s})"),
        T::List(f) => format!("list<{}>", item_name(f)),
        T::LargeList(f) => format!("large_list<{}>", item_name(f)),
        // Not a type planning gives; written so a person can read it.
        t => t.to_string(),
    }
}

fn item_name(f: &Field) -> String {
    let nullable = if f.is_nullable() { ", nullable" } else { "" };
    format!("{}: {}{nullable}", f.name(), type_name(f))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn answer(req: Value) -> Value {
        let req: Request = serde_json::from_value(req).unwrap();
        serde_json::to_value(plan(&req).unwrap()).unwrap()
    }

    fn customers() -> Value {
        json!({"name": "customers", "key": ["id"], "changelog": true, "columns": [
            {"name": "id", "type": "int64"},
            {"name": "email", "type": "utf8"},
            {"name": "created_at", "type": "timestamp[us, tz=UTC]"},
        ]})
    }

    #[test]
    fn type_names_read_back() {
        for t in [
            "bool",
            "int16",
            "int32",
            "int64",
            "uint64",
            "float32",
            "float64",
            "utf8",
            "large_utf8",
            "binary",
            "date32",
            "time64[us]",
            "timestamp[us, tz=UTC]",
            "timestamp[ns]",
            "decimal(10, 2)",
            "decimal256(40, 0)",
            "fixed_size_binary[16]",
            "month_day_nano_interval",
            "list<item: int64, nullable>",
            "list<item: utf8>",
            "list<item: timestamp[us, tz=UTC], nullable>",
            "list<item: extension<arrow.uuid>, nullable>",
            "extension<arrow.uuid>",
            "extension<arrow.json>",
        ] {
            let f = field("c", t).unwrap();
            assert_eq!(type_name(&f), t);
        }
        for t in [
            "string",
            "timestamp[us, UTC]",
            "decimal(10,2)",
            "struct<a: int64>",
            "extension<x.y>",
            "list<int64>",
        ] {
            assert!(field("c", t).is_err(), "{t}");
        }
    }

    #[test]
    fn two_statements() {
        let got = answer(json!({
            "sql": "INSERT INTO customers\nSELECT id, lower(email) AS email, created_at FROM customers;\n\nINSERT INTO contacts SELECT id AS customer_id, email AS contact_email FROM customers",
            "streams": [customers()],
        }));
        let s = &got["statements"];
        assert_eq!(s[0]["at"], json!({"line": 1, "column": 1}));
        assert_eq!(s[0]["output"], "customers");
        assert_eq!(s[0]["replaces"], true);
        assert_eq!(s[0]["class"], "row_wise");
        assert_eq!(s[0]["tier"], 1);
        assert_eq!(s[0]["inputs"], json!(["customers"]));
        assert_eq!(s[0]["key"], json!(["id"]));
        assert_eq!(s[0]["exchange"], Value::Null);
        assert_eq!(s[0]["operator"], Value::Null);
        assert_eq!(
            s[0]["columns"],
            json!([
                {"name": "id", "type": "int64", "nullable": true},
                {"name": "email", "type": "utf8", "nullable": true},
                {"name": "created_at", "type": "timestamp[us, tz=UTC]", "nullable": true},
            ])
        );
        assert_eq!(s[1]["at"], json!({"line": 4, "column": 1}));
        assert_eq!(s[1]["output"], "contacts");
        assert_eq!(s[1]["replaces"], false);
        assert_eq!(s[1]["key"], json!(["customer_id"]));
        assert_eq!(got["errors"], json!([]));

        let raw = base64::engine::general_purpose::STANDARD
            .decode(s[1]["arrow_schema"].as_str().unwrap())
            .unwrap();
        let schema = ipc::decode_schema(&raw).unwrap();
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["customer_id", "contact_email"]);
    }

    #[test]
    fn views_and_notes() {
        let got = answer(json!({
            "sql": "CREATE VIEW cleaned AS SELECT id, lower(email) AS email FROM customers;\nINSERT INTO destination.customers SELECT * FROM cleaned",
            "streams": [customers()],
        }));
        assert_eq!(got["views"][0]["name"], "cleaned");
        assert_eq!(got["views"][0]["inputs"], json!(["customers"]));
        assert_eq!(got["statements"][0]["views"], json!(["cleaned"]));
        assert_eq!(got["statements"][0]["at"], json!({"line": 2, "column": 1}));
        assert!(got["notes"].as_array().unwrap().is_empty());
    }

    #[test]
    fn errors_have_kinds_and_positions() {
        let got = answer(json!({
            "sql": "INSERT INTO hourly\nSELECT count(*) AS n FROM customers\nGROUP BY tumble(created_at, INTERVAL '1 hour');\nINSERT INTO x SELECT nope FROM customers",
            "streams": [customers()],
        }));
        assert_eq!(got["statements"], json!([]));
        let e = &got["errors"];
        assert_eq!(e[0]["kind"], "later_tier");
        assert_eq!(e[0]["class"], "group_window");
        assert_eq!(e[0]["tier"], 2);
        assert_eq!(e[0]["at"]["line"], 3);
        assert_eq!(e[1]["kind"], "bind");
        assert_eq!(e[1]["at"]["line"], 4);
        assert!(e[1].get("tier").is_none());
        assert!(e[1]["message"].as_str().unwrap().contains("nope"));
    }

    #[test]
    fn nanoseconds_and_nullable() {
        let req: Request = serde_json::from_value(json!({
            "sql": "INSERT INTO t SELECT id, created_at FROM customers",
            "streams": [{"name": "customers", "nanoseconds": true, "columns": [
                {"name": "id", "type": "int64"},
                {"name": "created_at", "type": "timestamp[us, tz=UTC]"},
            ]}],
        }))
        .unwrap();
        let got = plan(&req).unwrap();
        assert_eq!(
            got.statements[0].columns[1],
            AnswerColumn {
                name: "created_at".into(),
                ty: "timestamp[ns, tz=UTC]".into(),
                nullable: true
            }
        );
    }

    #[test]
    fn a_wrong_request_fails_the_call() {
        let bad_type: Request = serde_json::from_value(json!({
            "sql": "INSERT INTO t SELECT 1 AS one FROM s",
            "streams": [{"name": "s", "columns": [{"name": "a", "type": "string"}]}],
        }))
        .unwrap();
        assert!(plan(&bad_type).unwrap_err().contains("stream s column a"));
        let bad_key: Request = serde_json::from_value(json!({
            "sql": "INSERT INTO t SELECT 1 AS one FROM s",
            "streams": [{"name": "s", "key": ["id"], "columns": []}],
        }))
        .unwrap();
        assert!(plan(&bad_key).is_err());
        assert!(serde_json::from_value::<Request>(json!({"sql": "", "stream": []})).is_err());
    }
}
