//! Raw rows to Arrow. A [`Normalizer`] infers one stream's schema from the
//! JSON objects it sees, and only ever widens it; declared columns keep
//! their types. A [`Shaper`] gives a stream's columns their destination
//! names, on either lane.

mod coerce;
mod ids;
pub mod naming;
mod shape;

pub use coerce::{check_declared, render_row};
pub use ids::Identity;
pub use shape::{Shaped, Shaper};

use coerce::Target;

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, BooleanArray, FixedSizeBinaryArray, RecordBatch, RecordBatchOptions};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use serde_json::{Map, Value};

pub const EXTENSION_NAME_KEY: &str = "ARROW:extension:name";
pub const EXTENSION_METADATA_KEY: &str = "ARROW:extension:metadata";
/// The canonical extension name of `json` columns, stored as strings.
pub const JSON_EXTENSION: &str = "arrow.json";
/// The canonical extension name of `uuid` columns, stored as 16 bytes.
pub const UUID_EXTENSION: &str = "arrow.uuid";

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
    #[error("the restored schema has {0}")]
    Restored(String),
    #[error("{0}")]
    Declared(String),
    #[error("column {column:?} arrives as {from}, but is declared {to}")]
    Mismatch {
        column: String,
        from: DataType,
        to: DataType,
    },
    #[error(transparent)]
    Arrow(#[from] ArrowError),
}

/// A row left out of a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejected {
    /// The row's position among those pushed.
    pub row: usize,
    /// Never contains the row's contents.
    pub reason: String,
}

#[derive(Debug)]
pub struct Normalized {
    /// Has the stream's current schema, and every row but the rejected.
    pub batch: RecordBatch,
    pub rejected: Vec<Rejected>,
    /// The stream's schema changed with this batch.
    pub schema_changed: bool,
    /// Each row's `_openetl_id`, if the normalizer has an identity.
    pub ids: Option<FixedSizeBinaryArray>,
}

/// A column's type: inferred from values, or declared.
#[derive(Clone, Debug)]
enum Ty {
    Inferred(Kind),
    Declared(Field, Target),
}

impl Ty {
    fn field(&self, name: &str) -> Field {
        match self {
            Ty::Inferred(k) => k.field(name),
            Ty::Declared(f, _) => f.clone().with_name(name).with_nullable(true),
        }
    }
}

/// Infers and holds one stream's schema. Columns appear in the order their
/// first non-null value arrived, and inferred types only widen: `int64` to
/// `float64` to `string`, and anything else to `string`. Declared columns
/// keep their types, and a row whose value doesn't fit is rejected.
#[derive(Debug)]
pub struct Normalizer {
    columns: Vec<(String, Ty)>,
    index: HashMap<String, usize>,
    /// By source name, in declared order.
    declared: Vec<(String, Ty)>,
    schema: SchemaRef,
    identity: Option<Identity>,
}

impl Default for Normalizer {
    fn default() -> Self {
        Self {
            columns: Vec::new(),
            index: HashMap::new(),
            declared: Vec::new(),
            schema: Arc::new(Schema::empty()),
            identity: None,
        }
    }
}

impl Normalizer {
    /// Fails if raw rows can't fill a declared type.
    pub fn new(declared: Option<&Schema>) -> Result<Self, Error> {
        let mut n = Self::default();
        for f in declared.map(|d| d.fields().iter()).into_iter().flatten() {
            let t = Target::of(f)
                .filter(|t| *t != Target::TypedOnly)
                .ok_or_else(|| coerce::raw_unsupported(f))?;
            n.declared
                .push((f.name().clone(), Ty::Declared(f.as_ref().clone(), t)));
        }
        Ok(n)
    }

    /// Continues from a schema the stream had before. Declared types win
    /// over restored ones.
    pub fn with_schema(restored: &Schema, declared: Option<&Schema>) -> Result<Self, Error> {
        let mut n = Self::new(declared)?;
        for f in restored.fields() {
            let ty = match n.declared_ty(f.name()) {
                Some(ty) => ty,
                None => match (Kind::from_field(f), Target::of(f)) {
                    (Some(k), _) => Ty::Inferred(k),
                    (None, Some(t)) if t != Target::TypedOnly => {
                        Ty::Declared(f.as_ref().clone(), t)
                    }
                    _ => {
                        return Err(Error::Unsupported {
                            column: f.name().clone(),
                            data_type: f.data_type().clone(),
                        });
                    }
                },
            };
            n.index.insert(f.name().clone(), n.columns.len());
            n.columns.push((f.name().clone(), ty));
        }
        n.schema = n.build_schema();
        Ok(n)
    }

    fn declared_ty(&self, name: &str) -> Option<Ty> {
        self.declared
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, t)| t.clone())
    }

    fn build_schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self.columns.iter().map(|(n, t)| t.field(n)).collect();
        Arc::new(Schema::new(fields))
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Makes push compute each row's `_openetl_id`.
    pub fn identify(&mut self, identity: Identity) {
        self.identity = Some(identity);
    }

    /// Converts rows, one JSON object each, to a batch. Rows that aren't JSON
    /// objects, or whose declared columns don't fit, are left out and listed.
    /// On error, the schema is unchanged.
    pub fn push<'a, I>(&mut self, rows: I) -> Result<Normalized, Error>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let mut objects = Vec::new();
        // Each object's row among those pushed.
        let mut at = Vec::new();
        let mut rejected = Vec::new();
        for (row, raw) in rows.into_iter().enumerate() {
            // Parsing to Value first keeps contents out of the error.
            let err = match serde_json::from_slice(raw) {
                Ok(Value::Object(obj)) => {
                    objects.push(obj);
                    at.push(row);
                    continue;
                }
                Ok(_) => Error::NotAnObject { row },
                Err(source) => Error::Json { row, source },
            };
            rejected.push(Rejected {
                row,
                reason: err.to_string(),
            });
        }

        // Declared columns first, so rows that don't fit infer nothing.
        let mut coerced: HashMap<&str, ArrayRef> = HashMap::new();
        let mut failed: Vec<Option<String>> = vec![None; objects.len()];
        for (name, ty) in &self.declared {
            let Ty::Declared(f, t) = ty else { continue };
            let present = objects
                .iter()
                .any(|o| o.get(name).is_some_and(|v| !v.is_null()));
            if !present && !self.index.contains_key(name) {
                continue;
            }
            let values = objects.iter().map(|o| o.get(name));
            let (array, failures) = coerce::json(f, *t, values)?;
            for (i, reason) in failures {
                failed[i].get_or_insert(reason);
            }
            coerced.insert(name, array);
        }

        let mut columns = self.columns.clone();
        let mut index = self.index.clone();
        let mut changed = false;
        let kept = objects.iter().zip(&failed).filter(|(_, f)| f.is_none());
        for (name, v) in kept.flat_map(|(o, _)| o) {
            let Some(kind) = Kind::of(v) else { continue };
            match index.get(name) {
                Some(&i) => {
                    if let Ty::Inferred(cur) = columns[i].1 {
                        let widened = cur.join(kind);
                        if widened != cur {
                            columns[i].1 = Ty::Inferred(widened);
                            changed = true;
                        }
                    }
                }
                None => {
                    let ty = self.declared_ty(name).unwrap_or(Ty::Inferred(kind));
                    index.insert(name.clone(), columns.len());
                    columns.push((name.clone(), ty));
                    changed = true;
                }
            }
        }

        let mut arrays = Vec::with_capacity(columns.len());
        for (name, ty) in &columns {
            arrays.push(match ty {
                Ty::Inferred(kind) => build(*kind, name, &objects),
                Ty::Declared(..) => match coerced.get(name.as_str()) {
                    Some(a) => a.clone(),
                    None => {
                        return Err(Error::Declared(format!("column {name:?} wasn't coerced")));
                    }
                },
            });
        }
        let fields: Vec<Field> = columns.iter().map(|(n, t)| t.field(n)).collect();
        let schema = if changed {
            Arc::new(Schema::new(fields))
        } else {
            self.schema.clone()
        };
        let opts = RecordBatchOptions::new().with_row_count(Some(objects.len()));
        let mut batch = RecordBatch::try_new_with_options(schema.clone(), arrays, &opts)?;
        if failed.iter().any(Option::is_some) {
            let keep = BooleanArray::from_iter(failed.iter().map(|f| Some(f.is_none())));
            batch = arrow_select::filter::filter_record_batch(&batch, &keep)?;
            for (i, reason) in failed.iter().enumerate() {
                if let Some(reason) = reason {
                    rejected.push(Rejected {
                        row: at[i],
                        reason: reason.clone(),
                    });
                }
            }
            rejected.sort_by_key(|r| r.row);
        }
        let ids = match &self.identity {
            Some(identity) => {
                let mut buf = Vec::new();
                let mut ids = Vec::with_capacity(batch.num_rows() * 16);
                for (obj, _) in objects.iter().zip(&failed).filter(|(_, f)| f.is_none()) {
                    ids.extend_from_slice(&identity.raw(obj, &mut buf));
                }
                Some(FixedSizeBinaryArray::try_new(16, ids.into(), None)?)
            }
            None => None,
        };
        self.columns = columns;
        self.index = index;
        self.schema = schema;
        Ok(Normalized {
            batch,
            rejected,
            schema_changed: changed,
            ids,
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

        let mut n = Normalizer::with_schema(&first.schema(), None).unwrap();
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

        let other = Schema::new(vec![Field::new("t", DataType::UInt64, true)]);
        let err = Normalizer::with_schema(&other, None).unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }), "{err}");
    }

    fn declared() -> Schema {
        Schema::new(vec![
            Field::new("price", DataType::Decimal128(10, 2), false),
            Field::new(
                "at",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ])
    }

    #[test]
    fn declared_columns_keep_their_types_and_reject_what_doesnt_fit() {
        use arrow_array::types::Decimal128Type;
        let d = declared();
        let mut n = Normalizer::new(Some(&d)).unwrap();
        let out = push(
            &mut n,
            &[
                r#"{"id": 1, "price": 1.5, "at": "2026-01-01T00:00:00Z"}"#,
                r#"{"id": "x", "price": "NaN", "extra": true}"#,
                r#"{"id": 3, "price": "2.25"}"#,
            ],
        );
        assert_eq!(
            types(&n),
            vec![
                ("id".into(), DataType::Int64),
                ("price".into(), DataType::Decimal128(10, 2)),
                (
                    "at".into(),
                    DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()))
                ),
            ],
            "the rejected row widens nothing and adds nothing"
        );
        assert!(n.schema().field(1).is_nullable());
        assert_eq!(out.batch.num_rows(), 2);
        assert_eq!(out.rejected.len(), 1);
        assert_eq!(out.rejected[0].row, 1);
        assert!(out.rejected[0].reason.starts_with("nan: column \"price\""));
        let price = out.batch.column(1).as_primitive::<Decimal128Type>();
        assert_eq!(price.iter().collect::<Vec<_>>(), vec![Some(150), Some(225)]);
    }

    #[test]
    fn declared_types_win_over_restored_ones() {
        let restored = Schema::new(vec![
            Field::new("price", DataType::Utf8, true),
            Field::new("day", DataType::Date32, true),
        ]);
        let d = declared();
        let n = Normalizer::with_schema(&restored, Some(&d)).unwrap();
        assert_eq!(
            types(&n),
            vec![
                ("price".into(), DataType::Decimal128(10, 2)),
                ("day".into(), DataType::Date32),
            ]
        );
        let interval = Schema::new(vec![Field::new(
            "i",
            DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
            true,
        )]);
        assert!(matches!(
            Normalizer::new(Some(&interval)),
            Err(Error::Declared(_))
        ));
    }

    #[test]
    fn bad_rows_are_left_out_and_listed() {
        let mut n = Normalizer::default();
        push(&mut n, &[r#"{"a": 1}"#]);
        let before = n.schema();

        let out = n
            .push([
                br#"["secret"]"#.as_slice(),
                br#"{"a": 2}"#.as_slice(),
                br#"{"a": "secret"#.as_slice(),
            ])
            .unwrap();
        assert_eq!(out.batch.num_rows(), 1);
        assert_eq!(n.schema(), before);
        let rows: Vec<usize> = out.rejected.iter().map(|r| r.row).collect();
        assert_eq!(rows, vec![0, 2]);
        assert!(out.rejected[0].reason.contains("not a JSON object"));
        assert!(out.rejected.iter().all(|r| !r.reason.contains("secret")));
    }
}
