use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{Schema, SchemaRef};

use crate::naming::{METADATA_COLUMNS, Namer, Naming, SOURCE_NAMES_KEY, TABLE_KEY, source_names};
use crate::{Error, Normalized, Normalizer};

/// One stream's schema as the destination sees it: columns under their
/// destination names, and the table in the schema's metadata.
#[derive(Debug)]
pub struct Shaper {
    namer: Namer,
    table: String,
    norm: Option<Normalizer>,
    /// The restored schema under source names, until raw rows need it.
    restored: Option<Schema>,
    current: Option<SchemaRef>,
    /// The last typed batch's schema, and its shaped form.
    typed: Option<(SchemaRef, SchemaRef)>,
}

impl Shaper {
    /// `restored` is a schema [`Shaper::schema`] returned before.
    pub fn new(naming: Naming, table: String, restored: Option<SchemaRef>) -> Result<Self, Error> {
        let mut namer = Namer::new(naming, &METADATA_COLUMNS);
        let mut source = None;
        if let Some(r) = &restored {
            let names = source_names(r)
                .ok_or_else(|| Error::Restored(format!("its {SOURCE_NAMES_KEY} metadata")))?;
            let mut fields = Vec::with_capacity(r.fields().len());
            for f in r.fields() {
                let name = names.get(f.name()).unwrap_or(f.name());
                if !namer.keep(name, f.name()) {
                    return Err(Error::Restored(format!("two columns named {:?}", f.name())));
                }
                fields.push(f.as_ref().clone().with_name(name));
            }
            source = Some(Schema::new(fields));
        }
        Ok(Self {
            namer,
            table,
            norm: None,
            restored: source,
            current: restored,
            typed: None,
        })
    }

    /// None until the stream has a column.
    pub fn schema(&self) -> Option<SchemaRef> {
        self.current.clone()
    }

    /// Converts raw rows. On error, the schema is unchanged.
    pub fn rows<'a, I>(&mut self, rows: I) -> Result<Normalized, Error>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        if self.norm.is_none() {
            self.norm = Some(match &self.restored {
                Some(s) => Normalizer::with_schema(s)?,
                None => Normalizer::default(),
            });
            self.restored = None;
        }
        let Some(norm) = self.norm.as_mut() else {
            return Err(Error::Restored(String::new()));
        };
        let n = norm.push(rows)?;
        let schema = match (&self.current, n.schema_changed) {
            (Some(s), false) => s.clone(),
            _ => self.shape(&n.batch.schema()),
        };
        Ok(Normalized {
            batch: with_schema(schema, &n.batch)?,
            schema_changed: n.schema_changed,
        })
    }

    /// Renames a typed batch's columns.
    pub fn batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch, Error> {
        let schema = match &self.typed {
            Some((from, to)) if *from == batch.schema() => to.clone(),
            _ => {
                let to = self.shape(&batch.schema());
                self.typed = Some((batch.schema(), to.clone()));
                to
            }
        };
        if !schema.fields().is_empty() {
            self.current = Some(schema.clone());
        }
        with_schema(schema, batch)
    }

    fn shape(&mut self, schema: &Schema) -> SchemaRef {
        let mut renamed = self.namer.rename(schema);
        renamed
            .metadata
            .insert(TABLE_KEY.to_owned(), self.table.clone());
        let renamed = Arc::new(renamed);
        if !renamed.fields().is_empty() {
            self.current = Some(renamed.clone());
        }
        renamed
    }
}

fn with_schema(schema: SchemaRef, batch: &RecordBatch) -> Result<RecordBatch, Error> {
    let opts = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        schema,
        batch.columns().to_vec(),
        &opts,
    )?)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};

    fn names(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    fn rows(s: &mut Shaper, rows: &[&str]) -> Normalized {
        s.rows(rows.iter().map(|r| r.as_bytes())).unwrap()
    }

    #[test]
    fn names_raw_columns_and_tags_the_table() {
        let mut s = Shaper::new(Naming::default(), "users".into(), None).unwrap();
        assert!(s.schema().is_none());
        let n = rows(&mut s, &[r#"{"Id": 1, "id": "x", "First Name": true}"#]);
        assert!(n.schema_changed);
        let schema = n.batch.schema();
        assert_eq!(names(&schema), vec!["id", "id_2", "first_name"]);
        assert_eq!(schema.metadata().get(TABLE_KEY).unwrap(), "users");
        assert_eq!(s.schema().unwrap(), schema);

        let n = rows(&mut s, &[r#"{"First Name": false}"#]);
        assert!(!n.schema_changed);
        assert_eq!(n.batch.schema(), schema);
    }

    #[test]
    fn restored_names_never_change() {
        let mut first = Shaper::new(Naming::default(), "t".into(), None).unwrap();
        rows(&mut first, &[r#"{"foobar": 1}"#, r#"{"FooBar": 2.5}"#]);
        let restored = first.schema().unwrap();
        assert_eq!(names(&restored), vec!["foobar", "foobar_2"]);

        // In a new attempt, FooBar arrives first, and keeps its name and type.
        let mut s = Shaper::new(Naming::default(), "t".into(), Some(restored.clone())).unwrap();
        assert_eq!(s.schema().unwrap(), restored);
        let n = rows(&mut s, &[r#"{"FooBar": 3, "FOOBAR": "x"}"#]);
        let schema = n.batch.schema();
        assert_eq!(names(&schema), vec!["foobar", "foobar_2", "foobar_3"]);
        assert_eq!(schema.field(1).data_type(), &DataType::Float64);
    }

    #[test]
    fn renames_typed_batches() {
        let mut s = Shaper::new(Naming::default(), "t".into(), None).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("ID", DataType::Int64, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        let out = s.batch(&batch).unwrap();
        assert_eq!(names(&out.schema()), vec!["id"]);
        assert_eq!(out.column(0), batch.column(0));
        assert_eq!(s.schema().unwrap(), out.schema());
        assert_eq!(s.batch(&batch).unwrap().schema(), out.schema());
    }

    #[test]
    fn rejects_a_restored_schema_with_a_repeated_name() {
        let bad = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("x", DataType::Int64, true),
                Field::new("x_2", DataType::Utf8, true),
            ],
            std::collections::HashMap::from([(
                SOURCE_NAMES_KEY.to_owned(),
                r#"{"x_2":"x"}"#.to_owned(),
            )]),
        ));
        let err = Shaper::new(Naming::default(), "t".into(), Some(bad)).unwrap_err();
        assert!(matches!(err, Error::Restored(_)), "{err}");
    }
}
