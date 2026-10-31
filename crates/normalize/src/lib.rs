//! Raw rows to Arrow. A [`Normalizer`] infers one stream's schema from the
//! JSON objects it sees, and only ever widens it.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use serde_json::{Map, Value};

pub const EXTENSION_NAME_KEY: &str = "ARROW:extension:name";
pub const EXTENSION_METADATA_KEY: &str = "ARROW:extension:metadata";
/// The canonical extension name of `json` columns, stored as strings.
pub const JSON_EXTENSION: &str = "arrow.json";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Boolean,
    Int64,
    Float64,
    String,
    /// A nested object or array.
    Json,
}

impl Kind {
    fn of(v: &Value) -> Option<Kind> {
        match v {
            Value::Null => None,
            Value::Bool(_) => Some(Kind::Boolean),
            Value::Number(n) if n.is_i64() => Some(Kind::Int64),
            Value::Number(_) => Some(Kind::Float64),
            Value::String(_) => Some(Kind::String),
            Value::Array(_) | Value::Object(_) => Some(Kind::Json),
        }
    }

    /// The narrowest kind that holds values of both.
    fn join(self, other: Kind) -> Kind {
        match (self, other) {
            (a, b) if a == b => a,
            (Kind::Int64, Kind::Float64) | (Kind::Float64, Kind::Int64) => Kind::Float64,
            _ => Kind::String,
        }
    }

    fn from_field(f: &Field) -> Option<Kind> {
        let json = f.metadata().get(EXTENSION_NAME_KEY).map(String::as_str) == Some(JSON_EXTENSION);
        match f.data_type() {
            DataType::Boolean => Some(Kind::Boolean),
            DataType::Int64 => Some(Kind::Int64),
            DataType::Float64 => Some(Kind::Float64),
            DataType::Utf8 if json => Some(Kind::Json),
            DataType::Utf8 => Some(Kind::String),
            _ => None,
        }
    }

    fn field(self, name: &str) -> Field {
        let dt = match self {
            Kind::Boolean => DataType::Boolean,
            Kind::Int64 => DataType::Int64,
            Kind::Float64 => DataType::Float64,
            Kind::String | Kind::Json => DataType::Utf8,
        };
        let f = Field::new(name, dt, true);
        if self != Kind::Json {
            return f;
        }
        f.with_metadata(HashMap::from([
            (EXTENSION_NAME_KEY.to_owned(), JSON_EXTENSION.to_owned()),
            (EXTENSION_METADATA_KEY.to_owned(), String::new()),
        ]))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The message never contains row contents.
    #[error("row {row} is not valid JSON: {source}")]
    Json {
        row: usize,
        source: serde_json::Error,
    },
    #[error("row {row} is not a JSON object")]
    NotAnObject { row: usize },
    #[error("column {column:?} is {data_type}, which raw rows don't infer")]
    Unsupported { column: String, data_type: DataType },
    #[error(transparent)]
    Arrow(#[from] ArrowError),
}

#[derive(Debug)]
pub struct Normalized {
    /// Has the stream's current schema.
    pub batch: RecordBatch,
    /// The stream's schema changed with this batch.
    pub schema_changed: bool,
}

/// Infers and holds one stream's schema. Columns appear in the order their
/// first non-null value arrived, and types only widen: `int64` to `float64`
/// to `string`, and anything else to `string`.
#[derive(Debug)]
pub struct Normalizer {
    columns: Vec<(String, Kind)>,
    index: HashMap<String, usize>,
    schema: SchemaRef,
}

impl Default for Normalizer {
    fn default() -> Self {
        Self {
            columns: Vec::new(),
            index: HashMap::new(),
            schema: Arc::new(Schema::empty()),
        }
    }
}

impl Normalizer {
    /// Continues inference from a schema the stream had before.
    pub fn with_schema(schema: &Schema) -> Result<Self, Error> {
        let mut n = Self::default();
        for f in schema.fields() {
            let kind = Kind::from_field(f).ok_or_else(|| Error::Unsupported {
                column: f.name().clone(),
                data_type: f.data_type().clone(),
            })?;
            n.index.insert(f.name().clone(), n.columns.len());
            n.columns.push((f.name().clone(), kind));
        }
        let fields: Vec<Field> = n.columns.iter().map(|(c, k)| k.field(c)).collect();
        n.schema = Arc::new(Schema::new(fields));
        Ok(n)
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Converts rows, one JSON object each, to a batch. On error, the schema
    /// is unchanged.
    pub fn push<'a, I>(&mut self, rows: I) -> Result<Normalized, Error>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let mut objects = Vec::new();
        for (row, raw) in rows.into_iter().enumerate() {
            // Parsing to Value first keeps contents out of the error.
            let v: Value =
                serde_json::from_slice(raw).map_err(|source| Error::Json { row, source })?;
            let Value::Object(obj) = v else {
                return Err(Error::NotAnObject { row });
            };
            objects.push(obj);
        }

        let mut changed = false;
        for (name, v) in objects.iter().flatten() {
            let Some(kind) = Kind::of(v) else { continue };
            match self.index.get(name) {
                Some(&i) => {
                    let cur = self.columns[i].1;
                    let widened = cur.join(kind);
                    if widened != cur {
                        self.columns[i].1 = widened;
                        changed = true;
                    }
                }
                None => {
                    self.index.insert(name.clone(), self.columns.len());
                    self.columns.push((name.clone(), kind));
                    changed = true;
                }
            }
        }
        if changed {
            let fields: Vec<Field> = self.columns.iter().map(|(n, k)| k.field(n)).collect();
            self.schema = Arc::new(Schema::new(fields));
        }

        let arrays = self
            .columns
            .iter()
            .map(|(name, kind)| build(*kind, name, &objects))
            .collect();
        let opts = RecordBatchOptions::new().with_row_count(Some(objects.len()));
        let batch = RecordBatch::try_new_with_options(self.schema.clone(), arrays, &opts)?;
        Ok(Normalized {
            batch,
            schema_changed: changed,
        })
    }
}

fn build(kind: Kind, name: &str, rows: &[Map<String, Value>]) -> ArrayRef {
    let values = rows.iter().map(|r| r.get(name).filter(|v| !v.is_null()));
    match kind {
        Kind::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            values.for_each(|v| b.append_option(v.and_then(Value::as_bool)));
            Arc::new(b.finish())
        }
        Kind::Int64 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            values.for_each(|v| b.append_option(v.and_then(Value::as_i64)));
            Arc::new(b.finish())
        }
        Kind::Float64 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            values.for_each(|v| b.append_option(v.and_then(Value::as_f64)));
            Arc::new(b.finish())
        }
        Kind::String | Kind::Json => {
            let mut b = StringBuilder::with_capacity(rows.len(), 0);
            for v in values {
                match v {
                    None => b.append_null(),
                    Some(Value::String(s)) if kind == Kind::String => b.append_value(s),
                    Some(v) => b.append_value(v.to_string()),
                }
            }
            Arc::new(b.finish())
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float64Type, Int64Type};

    fn push(n: &mut Normalizer, rows: &[&str]) -> Normalized {
        n.push(rows.iter().map(|r| r.as_bytes())).unwrap()
    }

    fn types(n: &Normalizer) -> Vec<(String, DataType)> {
        n.schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect()
    }

    #[test]
    fn infers_scalars_in_first_appearance_order() {
        let mut n = Normalizer::default();
        let out = push(
            &mut n,
            &[r#"{"b": true, "a": 1}"#, r#"{"c": 1.5, "d": "x"}"#],
        );
        assert!(out.schema_changed);
        assert_eq!(
            types(&n),
            vec![
                ("b".into(), DataType::Boolean),
                ("a".into(), DataType::Int64),
                ("c".into(), DataType::Float64),
                ("d".into(), DataType::Utf8),
            ]
        );
        assert_eq!(out.batch.num_rows(), 2);
        let a = out.batch.column(1).as_primitive::<Int64Type>();
        assert_eq!(a.iter().collect::<Vec<_>>(), vec![Some(1), None]);
    }

    #[test]
    fn widens_int_to_float_within_a_batch() {
        let mut n = Normalizer::default();
        let out = push(&mut n, &[r#"{"x": 1}"#, r#"{"x": 2.5}"#]);
        assert_eq!(types(&n), vec![("x".into(), DataType::Float64)]);
        let x = out.batch.column(0).as_primitive::<Float64Type>();
        assert_eq!(x.iter().collect::<Vec<_>>(), vec![Some(1.0), Some(2.5)]);
    }

    #[test]
    fn integers_out_of_range_are_floats() {
        let mut n = Normalizer::default();
        push(&mut n, &[r#"{"x": 18446744073709551615}"#]);
        assert_eq!(types(&n), vec![("x".into(), DataType::Float64)]);
    }

    #[test]
    fn mixed_kinds_widen_to_string_as_json_text() {
        let mut n = Normalizer::default();
        let out = push(
            &mut n,
            &[
                r#"{"x": 1, "y": true, "z": {"k": 1}}"#,
                r#"{"x": "a", "y": "b", "z": 2}"#,
            ],
        );
        assert_eq!(
            types(&n),
            vec![
                ("x".into(), DataType::Utf8),
                ("y".into(), DataType::Utf8),
                ("z".into(), DataType::Utf8),
            ]
        );
        let col = |i: usize| -> Vec<Option<String>> {
            out.batch
                .column(i)
                .as_string::<i32>()
                .iter()
                .map(|v| v.map(str::to_owned))
                .collect()
        };
        assert_eq!(col(0), vec![Some("1".into()), Some("a".into())]);
        assert_eq!(col(1), vec![Some("true".into()), Some("b".into())]);
        assert_eq!(col(2), vec![Some(r#"{"k":1}"#.into()), Some("2".into())]);
        assert!(n.schema().field(2).metadata().is_empty());
    }

    #[test]
    fn nested_values_are_json_columns() {
        let mut n = Normalizer::default();
        let out = push(
            &mut n,
            &[r#"{"o": {"b": 2, "a": [1, null]}}"#, r#"{"o": [3]}"#],
        );
        let f = n.schema().field(0).clone();
        assert_eq!(f.data_type(), &DataType::Utf8);
        assert_eq!(
            f.metadata().get(EXTENSION_NAME_KEY).map(String::as_str),
            Some(JSON_EXTENSION)
        );
        let o: Vec<_> = out.batch.column(0).as_string::<i32>().iter().collect();
        assert_eq!(o, vec![Some(r#"{"b":2,"a":[1,null]}"#), Some("[3]")]);
    }

    #[test]
    fn null_only_columns_wait_for_a_typed_value() {
        let mut n = Normalizer::default();
        let out = push(&mut n, &[r#"{"a": null}"#, r#"{}"#]);
        assert!(!out.schema_changed);
        assert_eq!(out.batch.num_columns(), 0);
        assert_eq!(out.batch.num_rows(), 2);

        let out = push(&mut n, &[r#"{"a": null}"#, r#"{"a": "x"}"#]);
        assert!(out.schema_changed);
        assert_eq!(types(&n), vec![("a".into(), DataType::Utf8)]);
        assert_eq!(out.batch.column(0).null_count(), 1);
    }

    #[test]
    fn schema_persists_and_only_widens_across_batches() {
        let mut n = Normalizer::default();
        assert!(push(&mut n, &[r#"{"a": 1.5, "b": 1}"#]).schema_changed);

        // Missing columns fill with nulls; a narrower value keeps the wider type.
        let out = push(&mut n, &[r#"{"a": 2}"#]);
        assert!(!out.schema_changed);
        assert_eq!(out.batch.num_columns(), 2);
        assert_eq!(out.batch.column(1).null_count(), 1);
        assert_eq!(
            types(&n),
            vec![
                ("a".into(), DataType::Float64),
                ("b".into(), DataType::Int64)
            ]
        );

        let out = push(&mut n, &[r#"{"c": false, "b": "x"}"#]);
        assert!(out.schema_changed);
        assert_eq!(
            types(&n),
            vec![
                ("a".into(), DataType::Float64),
                ("b".into(), DataType::Utf8),
                ("c".into(), DataType::Boolean),
            ]
        );
    }

    #[test]
    fn continues_from_a_previous_schema() {
        let mut first = Normalizer::default();
        push(&mut first, &[r#"{"a": 1.5, "o": {"k": 1}, "b": 1}"#]);

        let mut n = Normalizer::with_schema(&first.schema()).unwrap();
        assert_eq!(n.schema(), first.schema());
        let out = push(&mut n, &[r#"{"b": 2, "a": 3, "c": "x"}"#]);
        assert!(out.schema_changed);
        assert_eq!(
            types(&n),
            vec![
                ("a".into(), DataType::Float64),
                ("o".into(), DataType::Utf8),
                ("b".into(), DataType::Int64),
                ("c".into(), DataType::Utf8),
            ]
        );
        assert_eq!(n.schema().field(1), first.schema().field(1));

        let other = Schema::new(vec![Field::new("t", DataType::Date32, true)]);
        let err = Normalizer::with_schema(&other).unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }), "{err}");
    }

    #[test]
    fn bad_rows_fail_without_changing_the_schema() {
        let mut n = Normalizer::default();
        push(&mut n, &[r#"{"a": 1}"#]);
        let before = n.schema();

        let err = n
            .push([br#"{"a": "x"}"#.as_slice(), br#"["secret"]"#.as_slice()])
            .unwrap_err();
        assert!(matches!(err, Error::NotAnObject { row: 1 }));
        assert!(!err.to_string().contains("secret"));

        let err = n.push([br#"{"a": "secret"#.as_slice()]).unwrap_err();
        assert!(matches!(err, Error::Json { row: 0, .. }));
        assert!(!err.to_string().contains("secret"));
        assert_eq!(n.schema(), before);
    }
}
