use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{
    ArrayRef, FixedSizeBinaryArray, RecordBatch, RecordBatchOptions, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::ids::Identity;
use crate::naming::{METADATA_COLUMNS, Namer, Naming, SOURCE_NAMES_KEY, TABLE_KEY, source_names};
use crate::{EXTENSION_METADATA_KEY, EXTENSION_NAME_KEY, Error, Normalizer};

const UUID_EXTENSION: &str = "arrow.uuid";

#[derive(Debug)]
pub struct Shaped {
    pub batch: RecordBatch,
    pub rejected: Vec<crate::Rejected>,
    /// The stream's schema changed with this batch.
    pub schema_changed: bool,
}

/// One stream's schema as the destination sees it: columns under their
/// destination names, then the metadata columns, with the table in the
/// schema's metadata.
#[derive(Debug)]
pub struct Shaper {
    namer: Namer,
    table: String,
    identity: Identity,
    norm: Option<Normalizer>,
    /// The restored schema's source columns under their source names, until
    /// raw rows need it.
    restored: Option<Schema>,
    current: Option<SchemaRef>,
    /// current came from this attempt, not from the checkpoint.
    shaped: bool,
    /// The last typed batch's schema, and its shaped form.
    typed: Option<(SchemaRef, SchemaRef)>,
}

impl Shaper {
    /// `restored` is a schema [`Shaper::schema`] returned before.
    pub fn new(
        naming: Naming,
        table: String,
        identity: Identity,
        restored: Option<SchemaRef>,
    ) -> Result<Self, Error> {
        let mut namer = Namer::new(naming, &METADATA_COLUMNS);
        let mut source = None;
        if let Some(r) = &restored {
            let names = source_names(r)
                .ok_or_else(|| Error::Restored(format!("its {SOURCE_NAMES_KEY} metadata")))?;
            let mut fields = Vec::with_capacity(r.fields().len());
            for f in r.fields() {
                if METADATA_COLUMNS.contains(&f.name().as_str()) {
                    continue;
                }
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
            identity,
            norm: None,
            restored: source,
            current: restored,
            shaped: false,
            typed: None,
        })
    }

    /// None until the stream has a column.
    pub fn schema(&self) -> Option<SchemaRef> {
        self.current.clone()
    }

    /// Converts raw rows. On error, the schema is unchanged.
    pub fn rows<'a, I>(&mut self, rows: I) -> Result<Shaped, Error>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        if self.norm.is_none() {
            let mut n = match &self.restored {
                Some(s) => Normalizer::with_schema(s)?,
                None => Normalizer::default(),
            };
            n.identify(self.identity.clone());
            self.norm = Some(n);
            self.restored = None;
        }
        let Some(norm) = self.norm.as_mut() else {
            return Err(Error::Restored(String::new()));
        };
        let n = norm.push(rows)?;
        let ids = n.ids.ok_or_else(|| Error::Restored(String::new()))?;
        let schema = match &self.current {
            Some(s) if self.shaped && !n.schema_changed => s.clone(),
            _ => self.shape(&n.batch),
        };
        Ok(Shaped {
            batch: self.assemble(schema, &n.batch, ids)?,
            rejected: n.rejected,
            schema_changed: n.schema_changed,
        })
    }

    /// Renames a typed batch's columns and adds the metadata columns.
    pub fn batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch, Error> {
        let ids = self.identity.typed(batch)?;
        let schema = match &self.typed {
            Some((from, to)) if *from == batch.schema() => to.clone(),
            _ => {
                let to = self.shape(batch);
                self.typed = Some((batch.schema(), to.clone()));
                to
            }
        };
        self.assemble(schema, batch, ids)
    }

    /// The destination schema for a batch under source names.
    fn shape(&mut self, batch: &RecordBatch) -> SchemaRef {
        let source = batch.schema();
        let renamed = self.namer.rename(&source);
        let mut fields: Vec<Field> = renamed
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let data = !fields.is_empty();
        fields.push(
            Field::new("_openetl_id", DataType::FixedSizeBinary(16), false).with_metadata(
                HashMap::from([
                    (EXTENSION_NAME_KEY.to_owned(), UUID_EXTENSION.to_owned()),
                    (EXTENSION_METADATA_KEY.to_owned(), String::new()),
                ]),
            ),
        );
        fields.push(Field::new("_openetl_op", DataType::Utf8, false));
        match self.seq_field(&source) {
            Some(f) => fields.push(f.with_name("_openetl_seq")),
            None if self.identity.cursor.is_none() => fields.push(Field::new(
                "_openetl_seq",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            )),
            None => {}
        }
        fields.push(Field::new(
            "_openetl_synced_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ));
        let mut md = renamed.metadata().clone();
        md.insert(TABLE_KEY.to_owned(), self.table.clone());
        let schema = Arc::new(Schema::new_with_metadata(fields, md));
        if data {
            self.current = Some(schema.clone());
            self.shaped = true;
        }
        schema
    }

    /// The cursor column's field, once the stream has one.
    fn seq_field(&self, source: &Schema) -> Option<Field> {
        let c = self.identity.cursor.as_ref()?;
        source
            .field_with_name(c)
            .ok()
            .map(|f| f.clone().with_nullable(true))
    }

    fn assemble(
        &self,
        schema: SchemaRef,
        batch: &RecordBatch,
        ids: FixedSizeBinaryArray,
    ) -> Result<RecordBatch, Error> {
        let n = batch.num_rows();
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns.push(Arc::new(ids));
        columns.push(Arc::new(StringArray::from_iter_values(
            std::iter::repeat_n("insert", n),
        )));
        if schema.index_of("_openetl_seq").is_ok() {
            columns.push(match &self.identity.cursor {
                Some(c) => batch.column(batch.schema().index_of(c)?).clone(),
                None => Arc::new(
                    TimestampMicrosecondArray::from_value(self.identity.read_start, n)
                        .with_timezone("UTC"),
                ),
            });
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_micros() as i64);
        columns.push(Arc::new(
            TimestampMicrosecondArray::from_value(now, n).with_timezone("UTC"),
        ));
        let opts = RecordBatchOptions::new().with_row_count(Some(n));
        Ok(RecordBatch::try_new_with_options(schema, columns, &opts)?)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_array::cast::AsArray;

    fn names(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    fn identity() -> Identity {
        Identity {
            stream: "users".into(),
            read_start: 42,
            ..Default::default()
        }
    }

    fn shaper(restored: Option<SchemaRef>) -> Shaper {
        Shaper::new(Naming::default(), "users".into(), identity(), restored).unwrap()
    }

    fn rows(s: &mut Shaper, rows: &[&str]) -> Shaped {
        s.rows(rows.iter().map(|r| r.as_bytes())).unwrap()
    }

    fn ids(batch: &RecordBatch) -> Vec<Vec<u8>> {
        let col = batch.column(batch.schema().index_of("_openetl_id").unwrap());
        col.as_fixed_size_binary()
            .value_data()
            .chunks(16)
            .map(<[u8]>::to_vec)
            .collect()
    }

    const META: [&str; 4] = [
        "_openetl_id",
        "_openetl_op",
        "_openetl_seq",
        "_openetl_synced_at",
    ];

    #[test]
    fn names_raw_columns_and_adds_metadata_columns() {
        let mut s = shaper(None);
        assert!(s.schema().is_none());
        let n = rows(&mut s, &[r#"{"Id": 1, "id": "x", "First Name": true}"#]);
        assert!(n.schema_changed);
        let schema = n.batch.schema();
        let mut want = vec!["id", "id_2", "first_name"];
        want.extend(META);
        assert_eq!(names(&schema), want);
        assert_eq!(schema.metadata().get(TABLE_KEY).unwrap(), "users");
        assert_eq!(s.schema().unwrap(), schema);
        let seq = n
            .batch
            .column(5)
            .as_primitive::<arrow_array::types::TimestampMicrosecondType>();
        assert_eq!(seq.values()[0], 42);
        assert_eq!(
            n.batch.column(4).as_string::<i32>().iter().next(),
            Some(Some("insert"))
        );

        let n = rows(&mut s, &[r#"{"First Name": false}"#]);
        assert!(!n.schema_changed);
        assert_eq!(n.batch.schema(), schema);
    }

    #[test]
    fn restored_names_and_ids_never_change() {
        let mut first = shaper(None);
        let a = rows(&mut first, &[r#"{"foobar": 1}"#, r#"{"FooBar": 2.5}"#]);
        let restored = first.schema().unwrap();
        assert_eq!(names(&restored)[..2], ["foobar", "foobar_2"]);

        // In a new attempt, FooBar arrives first, and keeps its name and type.
        let mut s = shaper(Some(restored.clone()));
        assert_eq!(s.schema().unwrap(), restored);
        let n = rows(
            &mut s,
            &[r#"{"FooBar": 3, "FOOBAR": "x"}"#, r#"{"foobar": 1}"#],
        );
        let schema = n.batch.schema();
        assert_eq!(names(&schema)[..3], ["foobar", "foobar_2", "foobar_3"]);
        assert_eq!(schema.field(1).data_type(), &DataType::Float64);
        assert_eq!(
            ids(&n.batch)[1],
            ids(&a.batch)[0],
            "the same row, the same ID"
        );
    }

    #[test]
    fn the_cursor_is_the_sequence_once_it_has_a_column() {
        let mut s = Shaper::new(
            Naming::default(),
            "t".into(),
            Identity {
                stream: "t".into(),
                key: vec!["id".into()],
                cursor: Some("At".into()),
                read_start: 0,
            },
            None,
        )
        .unwrap();
        let n = rows(&mut s, &[r#"{"id": 1}"#]);
        assert!(!names(&n.batch.schema()).contains(&"_openetl_seq".to_string()));
        let n = rows(&mut s, &[r#"{"id": 1, "At": 7}"#]);
        assert!(n.schema_changed);
        let schema = n.batch.schema();
        let seq = schema.index_of("_openetl_seq").unwrap();
        assert_eq!(schema.field(seq).data_type(), &DataType::Int64);
        assert_eq!(
            n.batch.column(seq),
            n.batch.column(schema.index_of("at").unwrap())
        );
    }

    #[test]
    fn renames_typed_batches() {
        let mut s = shaper(None);
        let schema = Arc::new(Schema::new(vec![Field::new("ID", DataType::Int64, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        let out = s.batch(&batch).unwrap();
        let mut want = vec!["id"];
        want.extend(META);
        assert_eq!(names(&out.schema()), want);
        assert_eq!(out.column(0), batch.column(0));
        assert_eq!(s.schema().unwrap(), out.schema());
        assert_eq!(s.batch(&batch).unwrap().schema(), out.schema());
        assert_ne!(ids(&out)[0], ids(&out)[1]);
    }

    #[test]
    fn rejects_a_restored_schema_with_a_repeated_name() {
        let bad = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("x", DataType::Int64, true),
                Field::new("x_2", DataType::Utf8, true),
            ],
            HashMap::from([(SOURCE_NAMES_KEY.to_owned(), r#"{"x_2":"x"}"#.to_owned())]),
        ));
        let err = Shaper::new(Naming::default(), "t".into(), identity(), Some(bad)).unwrap_err();
        assert!(matches!(err, Error::Restored(_)), "{err}");
    }
}
