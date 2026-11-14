//! What both engines share: what a record's SQL is, the tables and rows a
//! suite declares, and values written the same way whichever engine gave
//! them, so their results compare as text.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::temporal_conversions::{
    date32_to_datetime, date64_to_datetime, time32ms_to_time, time32s_to_time, time64ns_to_time,
    time64us_to_time, timestamp_ms_to_datetime, timestamp_ns_to_datetime, timestamp_s_to_datetime,
    timestamp_us_to_datetime,
};
use arrow_array::types::{
    Date32Type, Date64Type, Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef, ListArray, RecordBatch, StringArray};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_cast::cast::{CastOptions, cast_with_options};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

/// What a record's SQL is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    /// `CREATE TABLE`: a stream, with `WITH (changelog)` for a changelog.
    Table,
    /// `INSERT INTO t [(columns)] VALUES …`: a table's rows.
    Values,
    View,
    /// A `SELECT`, `WITH` or `VALUES` of its own, which our engine runs as
    /// `INSERT INTO slt_result` with it.
    Query,
    /// `DESCRIBE` and a query: the output's columns, types and key.
    Describe,
    /// A transform's statements as they're written.
    Transform,
}

pub(super) fn kind(sql: &str) -> Kind {
    let t = tokens(sql).unwrap_or_default();
    let word = |i: usize| match t.get(i) {
        Some(Tok::Word(w)) => w.to_ascii_uppercase(),
        Some(Tok::Punct(p)) => p.to_string(),
        _ => String::new(),
    };
    match (word(0).as_str(), word(1).as_str()) {
        ("CREATE", "TABLE") => Kind::Table,
        ("CREATE", "VIEW") => Kind::View,
        ("DESCRIBE", _) => Kind::Describe,
        ("SELECT" | "WITH" | "VALUES" | "(", _) => Kind::Query,
        ("INSERT", "INTO") if inserts_values(&t) => Kind::Values,
        _ => Kind::Transform,
    }
}

/// Whether `INSERT INTO name [(columns)]` is followed by `VALUES`.
fn inserts_values(t: &[Tok]) -> bool {
    let mut i = 2;
    while matches!(
        t.get(i),
        Some(Tok::Word(_) | Tok::Quoted(_) | Tok::Punct('.'))
    ) {
        if matches!(t.get(i), Some(Tok::Word(w)) if w.eq_ignore_ascii_case("VALUES")) {
            return true;
        }
        i += 1;
    }
    if t.get(i) == Some(&Tok::Punct('(')) {
        while i < t.len() && t[i] != Tok::Punct(')') {
            i += 1;
        }
        i += 1;
    }
    matches!(t.get(i), Some(Tok::Word(w)) if w.eq_ignore_ascii_case("VALUES"))
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum Tok {
    /// A name or keyword as written.
    Word(String),
    /// A double-quoted name.
    Quoted(String),
    /// A single-quoted string.
    Str(String),
    Num(String),
    Punct(char),
}

pub(super) fn tokens(s: &str) -> Result<Vec<Tok>, String> {
    let c: Vec<char> = s.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < c.len() {
        let ch = c[i];
        if ch.is_whitespace() {
            i += 1;
        } else if ch == '-' && c.get(i + 1) == Some(&'-') {
            while i < c.len() && c[i] != '\n' {
                i += 1;
            }
        } else if ch.is_alphabetic() || ch == '_' {
            let start = i;
            while i < c.len() && (c[i].is_alphanumeric() || c[i] == '_' || c[i] == '$') {
                i += 1;
            }
            out.push(Tok::Word(c[start..i].iter().collect()));
        } else if ch.is_ascii_digit() {
            let start = i;
            while i < c.len()
                && (c[i].is_ascii_alphanumeric()
                    || c[i] == '.'
                    || (matches!(c[i], '+' | '-') && matches!(c[i - 1], 'e' | 'E')))
            {
                i += 1;
            }
            out.push(Tok::Num(c[start..i].iter().collect()));
        } else if ch == '\'' || ch == '"' {
            let mut v = String::new();
            i += 1;
            loop {
                match c.get(i) {
                    None => return Err(format!("a quote that doesn't end: {s}")),
                    Some(&q) if q == ch && c.get(i + 1) == Some(&ch) => {
                        v.push(ch);
                        i += 2;
                    }
                    Some(&q) if q == ch => {
                        i += 1;
                        break;
                    }
                    Some(&x) => {
                        v.push(x);
                        i += 1;
                    }
                }
            }
            out.push(match ch {
                '\'' => Tok::Str(v),
                _ => Tok::Quoted(v),
            });
        } else {
            out.push(Tok::Punct(ch));
            i += 1;
        }
    }
    Ok(out)
}

struct Parser {
    t: Vec<Tok>,
    i: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.t.get(self.i)
    }

    fn word(&mut self, w: &str) -> bool {
        match self.peek() {
            Some(Tok::Word(x)) if x.eq_ignore_ascii_case(w) => {
                self.i += 1;
                true
            }
            _ => false,
        }
    }

    fn expect_word(&mut self, w: &str) -> Result<(), String> {
        match self.word(w) {
            true => Ok(()),
            false => Err(format!("{w} expected, at {:?}", self.peek())),
        }
    }

    fn punct(&mut self, p: char) -> bool {
        match self.peek() {
            Some(Tok::Punct(x)) if *x == p => {
                self.i += 1;
                true
            }
            _ => false,
        }
    }

    fn expect_punct(&mut self, p: char) -> Result<(), String> {
        match self.punct(p) {
            true => Ok(()),
            false => Err(format!("{p} expected, at {:?}", self.peek())),
        }
    }

    fn ident(&mut self) -> Result<String, String> {
        match self.peek().cloned() {
            Some(Tok::Word(w) | Tok::Quoted(w)) => {
                self.i += 1;
                Ok(w)
            }
            t => Err(format!("a name expected, at {t:?}")),
        }
    }

    /// A dotted name, its parts joined with dots.
    fn name(&mut self) -> Result<String, String> {
        let mut name = self.ident()?;
        while self.punct('.') {
            name.push('.');
            name.push_str(&self.ident()?);
        }
        Ok(name)
    }

    fn number(&mut self) -> Result<u8, String> {
        match self.peek().cloned() {
            Some(Tok::Num(n)) => {
                self.i += 1;
                n.parse().map_err(|_| format!("{n} isn't a small number"))
            }
            t => Err(format!("a number expected, at {t:?}")),
        }
    }

    fn end(&self) -> Result<(), String> {
        match self.peek() {
            None | Some(Tok::Punct(';')) => Ok(()),
            Some(t) => Err(format!("{t:?} after the statement")),
        }
    }

    /// A column's type, and whether it's JSON.
    fn data_type(&mut self) -> Result<(DataType, bool), String> {
        let w = self.ident()?.to_ascii_uppercase();
        let mut json = false;
        let mut t = match w.as_str() {
            "BIGINT" | "INT8" => DataType::Int64,
            "INTEGER" | "INT" | "INT4" => DataType::Int32,
            "SMALLINT" | "INT2" => DataType::Int16,
            "DOUBLE" => {
                self.word("PRECISION");
                DataType::Float64
            }
            "FLOAT8" => DataType::Float64,
            "REAL" | "FLOAT4" | "FLOAT" => DataType::Float32,
            "TEXT" | "VARCHAR" => DataType::Utf8,
            "BOOLEAN" | "BOOL" => DataType::Boolean,
            "DATE" => DataType::Date32,
            "TIMESTAMP" => DataType::Timestamp(TimeUnit::Microsecond, None),
            "TIMESTAMPTZ" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            "TIME" => DataType::Time64(TimeUnit::Microsecond),
            "BLOB" | "BYTEA" => DataType::Binary,
            "JSON" => {
                json = true;
                DataType::Utf8
            }
            "DECIMAL" | "NUMERIC" => {
                self.expect_punct('(')?;
                let p = self.number()?;
                self.expect_punct(',')?;
                let s = self.number()?;
                self.expect_punct(')')?;
                DataType::Decimal128(p, i8::try_from(s).map_err(|e| e.to_string())?)
            }
            _ => return Err(format!("{w} isn't a type these suites know")),
        };
        while self.punct('[') {
            self.expect_punct(']')?;
            if json {
                return Err("JSON[] isn't a type these suites know".into());
            }
            t = DataType::List(Arc::new(Field::new_list_field(t, true)));
        }
        Ok((t, json))
    }

    fn literal(&mut self) -> Result<Lit, String> {
        let t = self.peek().cloned();
        self.i += 1;
        Ok(match t {
            Some(Tok::Word(w)) if w.eq_ignore_ascii_case("NULL") => Lit::Null,
            Some(Tok::Word(w)) if w.eq_ignore_ascii_case("TRUE") => Lit::Text("true".into()),
            Some(Tok::Word(w)) if w.eq_ignore_ascii_case("FALSE") => Lit::Text("false".into()),
            Some(Tok::Word(w)) if w.eq_ignore_ascii_case("ARRAY") => {
                self.expect_punct('[')?;
                self.list()?
            }
            Some(Tok::Punct('[')) => self.list()?,
            Some(Tok::Punct('-')) => match self.peek().cloned() {
                Some(Tok::Num(n)) => {
                    self.i += 1;
                    Lit::Text(format!("-{n}"))
                }
                t => return Err(format!("a number expected after -, at {t:?}")),
            },
            Some(Tok::Num(n) | Tok::Str(n)) => Lit::Text(n),
            t => return Err(format!("a literal expected, at {t:?}")),
        })
    }

    /// The rest of a list literal, after its `[`.
    fn list(&mut self) -> Result<Lit, String> {
        let mut items = Vec::new();
        if self.punct(']') {
            return Ok(Lit::List(items));
        }
        loop {
            items.push(self.literal()?);
            if self.punct(']') {
                return Ok(Lit::List(items));
            }
            self.expect_punct(',')?;
        }
    }
}

/// A table a suite creates: a stream, under its whole name.
#[derive(Clone, Debug)]
pub(super) struct Table {
    pub name: String,
    pub schema: SchemaRef,
    pub key: Vec<String>,
    pub changelog: bool,
}

/// `CREATE TABLE name (column type [NOT NULL] [PRIMARY KEY], …
/// [, PRIMARY KEY (columns)]) [WITH (changelog)]`. Key columns aren't null.
pub(super) fn table(sql: &str) -> Result<Table, String> {
    let mut p = Parser {
        t: tokens(sql)?,
        i: 2,
    };
    let name = p.name()?;
    p.expect_punct('(')?;
    let mut columns = Vec::new();
    let mut key = Vec::new();
    loop {
        if p.word("PRIMARY") {
            p.expect_word("KEY")?;
            p.expect_punct('(')?;
            loop {
                key.push(p.ident()?);
                if !p.punct(',') {
                    break;
                }
            }
            p.expect_punct(')')?;
        } else {
            let column = p.ident()?;
            let (t, json) = p.data_type()?;
            let mut nullable = true;
            loop {
                if p.word("NOT") {
                    p.expect_word("NULL")?;
                    nullable = false;
                } else if p.word("PRIMARY") {
                    p.expect_word("KEY")?;
                    key.push(column.clone());
                } else {
                    break;
                }
            }
            columns.push((column, t, json, nullable));
        }
        if !p.punct(',') {
            break;
        }
    }
    p.expect_punct(')')?;
    let changelog = p.word("WITH");
    if changelog {
        p.expect_punct('(')?;
        p.expect_word("changelog")?;
        p.expect_punct(')')?;
    }
    p.end()?;
    let fields: Vec<Field> = columns
        .into_iter()
        .map(|(column, t, json, nullable)| {
            let nullable = nullable && !key.contains(&column);
            let f = Field::new(column, t, nullable);
            match json {
                true => f.with_metadata(json_metadata()),
                false => f,
            }
        })
        .collect();
    if let Some(k) = key.iter().find(|k| !fields.iter().any(|f| f.name() == *k)) {
        return Err(format!("key column {k} isn't a column of {name}"));
    }
    Ok(Table {
        name,
        schema: Arc::new(Schema::new(fields)),
        key,
        changelog,
    })
}

/// What marks a string column as JSON, as the engine marks one.
pub(super) fn json_metadata() -> HashMap<String, String> {
    HashMap::from([
        ("ARROW:extension:name".to_owned(), "arrow.json".to_owned()),
        ("ARROW:extension:metadata".to_owned(), String::new()),
    ])
}

pub(super) fn is_json(f: &Field) -> bool {
    f.metadata().get("ARROW:extension:name").map(String::as_str) == Some("arrow.json")
}

#[derive(Clone, Debug)]
pub(super) enum Lit {
    Null,
    /// A number, a boolean or a string, as text a cast reads.
    Text(String),
    List(Vec<Lit>),
}

/// `INSERT INTO name [(columns)] VALUES (…), …`.
pub(super) struct Values {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub rows: Vec<Vec<Lit>>,
}

pub(super) fn values(sql: &str) -> Result<Values, String> {
    let mut p = Parser {
        t: tokens(sql)?,
        i: 2,
    };
    let table = p.name()?;
    let mut columns = None;
    if p.punct('(') {
        let mut names = Vec::new();
        loop {
            names.push(p.ident()?);
            if !p.punct(',') {
                break;
            }
        }
        p.expect_punct(')')?;
        columns = Some(names);
    }
    p.expect_word("VALUES")?;
    let mut rows = Vec::new();
    loop {
        p.expect_punct('(')?;
        let mut row = Vec::new();
        loop {
            row.push(p.literal()?);
            if !p.punct(',') {
                break;
            }
        }
        p.expect_punct(')')?;
        rows.push(row);
        if !p.punct(',') {
            break;
        }
    }
    p.end()?;
    Ok(Values {
        table,
        columns,
        rows,
    })
}

/// A column of type `t` from literals, each cast from its text as a cast
/// from a string does.
pub(super) fn array(t: &DataType, lits: &[&Lit]) -> Result<ArrayRef, String> {
    if let DataType::List(f) = t {
        let mut offsets = vec![0i32];
        let mut items = Vec::new();
        let mut valid = Vec::with_capacity(lits.len());
        for l in lits {
            match l {
                Lit::Null => valid.push(false),
                Lit::List(xs) => {
                    items.extend(xs.iter());
                    valid.push(true);
                }
                Lit::Text(x) => return Err(format!("{x} isn't a list")),
            }
            offsets.push(i32::try_from(items.len()).map_err(|e| e.to_string())?);
        }
        let values = array(f.data_type(), &items)?;
        let list = ListArray::try_new(
            f.clone(),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            values,
            Some(NullBuffer::from(valid)),
        )
        .map_err(|e| e.to_string())?;
        return Ok(Arc::new(list));
    }
    let mut texts = Vec::with_capacity(lits.len());
    for l in lits {
        texts.push(match l {
            Lit::Null => None,
            Lit::Text(x) => Some(x.as_str()),
            Lit::List(_) => return Err(format!("a list for a column of {t}")),
        });
    }
    let strings: ArrayRef = Arc::new(StringArray::from(texts));
    match t {
        DataType::Utf8 => Ok(strings),
        _ => cast_with_options(
            &strings,
            t,
            &CastOptions {
                safe: false,
                ..Default::default()
            },
        )
        .map_err(|e| e.to_string()),
    }
}

/// Each row of `batch`, its values written by [`cell`].
pub(super) fn rows(batch: &RecordBatch) -> Vec<Vec<String>> {
    (0..batch.num_rows())
        .map(|r| {
            batch
                .columns()
                .iter()
                .map(|c| cell(c.as_ref(), r))
                .collect()
        })
        .collect()
}

/// A value as text, the same whichever engine gave it and whatever width
/// of its kind of type: integers as they are; floats to 12 significant
/// digits, or 6 for 32 bits; decimals without trailing zeros; timestamps
/// in UTC with `+00` where they have a time zone; binary in hex; lists in
/// brackets. Null is `NULL` and an empty string `(empty)`.
pub(super) fn cell(a: &dyn Array, i: usize) -> String {
    if a.is_null(i) {
        return "NULL".into();
    }
    let one = a.slice(i, 1);
    let a = one.as_ref();
    let t = a.data_type();
    match t {
        DataType::Boolean => shown(first(a.as_boolean().iter())),
        DataType::Int8 => shown(first(a.as_primitive::<Int8Type>().iter())),
        DataType::Int16 => shown(first(a.as_primitive::<Int16Type>().iter())),
        DataType::Int32 => shown(first(a.as_primitive::<Int32Type>().iter())),
        DataType::Int64 => shown(first(a.as_primitive::<Int64Type>().iter())),
        DataType::UInt8 => shown(first(a.as_primitive::<UInt8Type>().iter())),
        DataType::UInt16 => shown(first(a.as_primitive::<UInt16Type>().iter())),
        DataType::UInt32 => shown(first(a.as_primitive::<UInt32Type>().iter())),
        DataType::UInt64 => shown(first(a.as_primitive::<UInt64Type>().iter())),
        DataType::Float32 => {
            shown(first(a.as_primitive::<Float32Type>().iter()).map(|v| float(f64::from(v), 6)))
        }
        DataType::Float64 => {
            shown(first(a.as_primitive::<Float64Type>().iter()).map(|v| float(v, 12)))
        }
        DataType::Decimal128(_, _) => {
            decimal(&a.as_primitive::<Decimal128Type>().value_as_string(0))
        }
        DataType::Decimal256(_, _) => {
            decimal(&a.as_primitive::<Decimal256Type>().value_as_string(0))
        }
        DataType::Utf8 => shown(first(a.as_string::<i32>().iter()).map(text)),
        DataType::LargeUtf8 => shown(first(a.as_string::<i64>().iter()).map(text)),
        DataType::Utf8View => shown(first(a.as_string_view().iter()).map(text)),
        DataType::Binary => shown(first(a.as_binary::<i32>().iter()).map(hex)),
        DataType::LargeBinary => shown(first(a.as_binary::<i64>().iter()).map(hex)),
        DataType::BinaryView => shown(first(a.as_binary_view().iter()).map(hex)),
        DataType::FixedSizeBinary(_) => shown(first(a.as_fixed_size_binary().iter()).map(hex)),
        DataType::Date32 => shown(
            first(a.as_primitive::<Date32Type>().iter())
                .and_then(date32_to_datetime)
                .map(|d| d.date()),
        ),
        DataType::Date64 => shown(
            first(a.as_primitive::<Date64Type>().iter())
                .and_then(date64_to_datetime)
                .map(|d| d.date()),
        ),
        DataType::Timestamp(unit, tz) => {
            let at = match unit {
                TimeUnit::Second => first(a.as_primitive::<TimestampSecondType>().iter())
                    .and_then(timestamp_s_to_datetime),
                TimeUnit::Millisecond => first(a.as_primitive::<TimestampMillisecondType>().iter())
                    .and_then(timestamp_ms_to_datetime),
                TimeUnit::Microsecond => first(a.as_primitive::<TimestampMicrosecondType>().iter())
                    .and_then(timestamp_us_to_datetime),
                TimeUnit::Nanosecond => first(a.as_primitive::<TimestampNanosecondType>().iter())
                    .and_then(timestamp_ns_to_datetime),
            };
            let zone = if tz.is_some() { "+00" } else { "" };
            format!("{}{zone}", shown(at))
        }
        DataType::Time32(TimeUnit::Second) => {
            shown(first(a.as_primitive::<Time32SecondType>().iter()).and_then(time32s_to_time))
        }
        DataType::Time32(_) => shown(
            first(a.as_primitive::<Time32MillisecondType>().iter()).and_then(time32ms_to_time),
        ),
        DataType::Time64(TimeUnit::Microsecond) => shown(
            first(a.as_primitive::<Time64MicrosecondType>().iter()).and_then(time64us_to_time),
        ),
        DataType::Time64(_) => {
            shown(first(a.as_primitive::<Time64NanosecondType>().iter()).and_then(time64ns_to_time))
        }
        DataType::List(_) => shown(first(a.as_list::<i32>().iter()).map(|l| list(l.as_ref()))),
        DataType::LargeList(_) => shown(first(a.as_list::<i64>().iter()).map(|l| list(l.as_ref()))),
        DataType::FixedSizeList(_, _) => {
            shown(first(a.as_fixed_size_list().iter()).map(|l| list(l.as_ref())))
        }
        DataType::Struct(fields) => {
            let s = a.as_struct();
            let parts: Vec<String> = fields
                .iter()
                .zip(s.columns())
                .map(|(f, c)| format!("{}: {}", f.name(), cell(c.as_ref(), 0)))
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
        _ => match arrow_cast::display::ArrayFormatter::try_new(a, &Default::default()) {
            Ok(f) => f.value(0).to_string(),
            Err(e) => format!("({t}: {e})"),
        },
    }
}

/// The value of a slice of one value that isn't null.
fn first<T>(mut values: impl Iterator<Item = Option<T>>) -> Option<T> {
    values.next().flatten()
}

fn shown<T: std::fmt::Display>(v: Option<T>) -> String {
    v.map_or_else(|| "(out of range)".into(), |v| v.to_string())
}

fn text(s: &str) -> String {
    match s.is_empty() {
        true => "(empty)".into(),
        false => s.to_owned(),
    }
}

fn hex(b: &[u8]) -> String {
    match b.is_empty() {
        true => "(empty)".into(),
        false => b.iter().map(|x| format!("{x:02x}")).collect(),
    }
}

fn list(a: &dyn Array) -> String {
    let items: Vec<String> = (0..a.len()).map(|j| cell(a, j)).collect();
    format!("[{}]", items.join(", "))
}

fn float(v: f64, digits: usize) -> String {
    if v.is_nan() {
        return "NaN".into();
    }
    if v.is_infinite() {
        return if v > 0.0 { "Infinity" } else { "-Infinity" }.into();
    }
    let rounded: f64 = format!("{v:.prec$e}", prec = digits - 1)
        .parse()
        .unwrap_or(v);
    // Without a sign on zero.
    format!("{}", rounded + 0.0)
}

fn decimal(s: &str) -> String {
    let s = match s.contains('.') {
        true => s.trim_end_matches('0').trim_end_matches('.'),
        false => s,
    };
    match s {
        "-0" => "0".into(),
        s => s.to_owned(),
    }
}

/// The type of a column's values in a `query` record's types: integers,
/// floating point, or text for the rest.
pub(super) fn column_type(t: &DataType) -> sqllogictest::DefaultColumnType {
    use sqllogictest::DefaultColumnType as C;
    match t {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => C::Integer,
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => C::FloatingPoint,
        _ => C::Text,
    }
}

mod tests {
    use super::*;

    #[test]
    fn a_table_parses_with_its_key_and_kind() {
        let t = table(
            "CREATE TABLE \"public.Orders\" (id BIGINT, tags TEXT[] NOT NULL, doc JSON, \
             amount DECIMAL(12, 2), at TIMESTAMPTZ, PRIMARY KEY (id)) WITH (changelog)",
        );
        let t = t.unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(t.name, "public.Orders");
        assert_eq!(t.key, ["id"]);
        assert!(t.changelog);
        assert!(!t.schema.field(0).is_nullable());
        assert!(is_json(t.schema.field(2)));
        assert_eq!(t.schema.field(3).data_type(), &DataType::Decimal128(12, 2));
    }

    #[test]
    fn values_cast_from_their_text() {
        let v = values("INSERT INTO t VALUES (1, 'a''b', [1, NULL], -2.5), (NULL, NULL, NULL, 3)")
            .unwrap_or_else(|e| panic!("{e}"));
        let column = |j: usize| -> Vec<&Lit> { v.rows.iter().map(|r| &r[j]).collect() };
        let list = DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true)));
        let a = array(&list, &column(2)).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(cell(a.as_ref(), 0), "[1, NULL]");
        assert_eq!(cell(a.as_ref(), 1), "NULL");
        let s = array(&DataType::Utf8, &column(1)).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(cell(s.as_ref(), 0), "a'b");
        let d = array(&DataType::Decimal128(6, 2), &column(3)).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(cell(d.as_ref(), 0), "-2.5");
        assert_eq!(cell(d.as_ref(), 1), "3");
        assert_eq!(
            kind("insert into t (a) values (1)"),
            Kind::Values,
            "an INSERT of values"
        );
        assert_eq!(kind("INSERT INTO t SELECT * FROM s"), Kind::Transform);
    }

    #[test]
    fn floats_round_to_their_precision() {
        assert_eq!(float(0.1 + 0.2, 12), "0.3");
        assert_eq!(float(-0.0, 12), "0");
        assert_eq!(float(3.0, 12), "3");
        assert_eq!(float(f64::from(1.1f32), 6), "1.1");
    }
}
