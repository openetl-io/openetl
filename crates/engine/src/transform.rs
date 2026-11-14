//! A job's SQL transform: its statements planned against the streams it
//! reads, and which outputs each stream feeds. Nothing planned outlives the
//! job: every attempt plans again from the SQL and the streams' schemas.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use arrow_schema::{Field, Schema, SchemaRef};
use openetl_normalize::VERSIONS_KEY;
use openetl_normalize::naming::{METADATA_COLUMNS, TABLE_KEY, source_names};
use openetl_proto::engine::v1::{StreamPlan, Transform as TransformPlan};
use openetl_sql::{Catalog, Operator, Position, RowWise, Sql, Stream};

use crate::error::JobError;
use crate::ipc;

/// A stream the statements read, as planning sees it.
#[derive(Clone, Debug)]
pub struct Input {
    pub name: String,
    /// Its columns under source names, with logical types; None until a
    /// source declares them or a sync records them.
    pub schema: Option<SchemaRef>,
    pub key: Vec<String>,
    pub changelog: bool,
}

impl Input {
    /// What a StreamPlan says of the stream: its restored schema, which a
    /// transform's input reports as its statements read it, with the
    /// declared types over it, then the declared columns it lacks.
    pub fn of(s: &StreamPlan) -> Result<Self, JobError> {
        let bad = |what: &str, e: String| {
            JobError::config(format!("stream {}: {what} schema: {e}", s.name))
        };
        let declared = match s.arrow_schema.is_empty() {
            true => None,
            false => {
                let d = ipc::decode_schema(&s.arrow_schema)
                    .map_err(|e| bad("declared", e.to_string()))?;
                Some(match s.nanosecond_timestamps {
                    true => openetl_normalize::nanoseconds(&d),
                    false => d,
                })
            }
        };
        let restored = match s.restored_schema.is_empty() {
            true => None,
            false => {
                let r = ipc::decode_schema(&s.restored_schema)
                    .map_err(|e| bad("restored", e.to_string()))?;
                Some(as_read(&r))
            }
        };
        Ok(Self {
            name: s.name.clone(),
            schema: merged(restored, declared),
            key: s.primary_key.clone(),
            changelog: s.changelog,
        })
    }
}

/// A stream's schema as statements read it, from the one a sync recorded,
/// as `as_read` gives it, and the one its source declares: the recorded
/// columns with the declared types over them, then the declared columns it
/// lacks. None if it has neither.
pub fn merged(restored: Option<SchemaRef>, declared: Option<SchemaRef>) -> Option<SchemaRef> {
    match (restored, declared) {
        (None, d) => d,
        (Some(r), None) => Some(r),
        (Some(r), Some(d)) => {
            let mut fields: Vec<Field> = r
                .fields()
                .iter()
                .map(|f| match d.field_with_name(f.name()) {
                    Ok(df) => df.clone(),
                    Err(_) => f.as_ref().clone(),
                })
                .collect();
            for f in d.fields() {
                if r.field_with_name(f.name()).is_err() {
                    fields.push(f.as_ref().clone());
                }
            }
            Some(Arc::new(Schema::new(fields)))
        }
    }
}

/// A stream's recorded schema as statements read it. One recorded before
/// the pipeline had a transform is as the destination got it, with its
/// table in its metadata: its columns go back to their source names and
/// current versions, and the metadata columns go.
pub fn as_read(schema: &Schema) -> SchemaRef {
    if !schema.metadata().contains_key(TABLE_KEY) {
        return Arc::new(schema.clone());
    }
    let names = source_names(schema).unwrap_or_default();
    let versions: BTreeMap<String, u32> = schema
        .metadata()
        .get(VERSIONS_KEY)
        .and_then(|v| serde_json::from_str(v).ok())
        .unwrap_or_default();
    let mut superseded = BTreeSet::new();
    let mut current = HashMap::new();
    for (s, &k) in &versions {
        superseded.insert(s.clone());
        for j in 2..k {
            superseded.insert(format!("{s}__v{j}"));
        }
        current.insert(format!("{s}__v{k}"), s.clone());
    }
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .filter(|f| !METADATA_COLUMNS.contains(&f.name().as_str()))
        .filter_map(|f| {
            let name = names.get(f.name()).unwrap_or(f.name());
            if superseded.contains(name) {
                return None;
            }
            let name = current.get(name).unwrap_or(name);
            Some(f.as_ref().clone().with_name(name))
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// One `INSERT INTO`, planned.
#[derive(Debug)]
pub struct Statement {
    pub output: String,
    /// The stream it reads, by its index among the inputs.
    pub input: usize,
    pub at: Position,
    /// The output's columns, with logical types.
    pub schema: SchemaRef,
    /// The output's primary key; empty where it can only be appended.
    pub key: Vec<String>,
    pub op: Arc<RowWise>,
}

/// How a stream reaches one of the outputs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    /// It is that output, as it would be without a transform.
    Pass,
    /// A statement, by its index, writes that output from it.
    Statement(usize),
}

/// A job's SQL, planned.
#[derive(Debug)]
pub struct Transform {
    sql: Sql,
    pub inputs: Vec<Input>,
    pub statements: Vec<Statement>,
}

impl Transform {
    /// Plans `sql` against `inputs`. Refuses SQL that doesn't plan, a
    /// statement that isn't row-wise, and one that reads a stream whose
    /// columns aren't known.
    pub fn plan(sql: &str, inputs: Vec<Input>) -> Result<Self, JobError> {
        let parsed =
            Sql::parse(sql).map_err(|e| JobError::config(format!("transform.sql: {e}")))?;
        let statements = plan_statements(&parsed, &inputs, None)?;
        Ok(Self {
            sql: parsed,
            inputs,
            statements,
        })
    }

    /// The statement planned again with the input it reads at `schema`, as
    /// a lane that sees the stream at another schema needs it. Its output
    /// and key stay the job's, or the job stops with SCHEMA_DRIFT.
    pub fn again(&self, statement: usize, schema: SchemaRef) -> Result<Statement, JobError> {
        let st = &self.statements[statement];
        let drift = |what: String| {
            JobError::new(
                openetl_proto::connector::v1::ErrorCode::SchemaDrift,
                format!(
                    "transform.sql: {}: stream {}'s schema changed, and {what}",
                    st.at, self.inputs[st.input].name
                ),
            )
        };
        let mut planned = plan_statements(&self.sql, &self.inputs, Some((st.input, schema)))
            .map_err(|e| {
                drift(format!(
                    "the statement writing {} no longer plans: {}",
                    st.output, e.message
                ))
            })?;
        if statement >= planned.len() {
            return Err(JobError::internal("planning again gave fewer statements"));
        }
        let again = planned.swap_remove(statement);
        if again.output != st.output || again.key != st.key {
            return Err(drift(format!(
                "output {}'s primary key would change from ({}) to ({})",
                st.output,
                st.key.join(", "),
                again.key.join(", ")
            )));
        }
        Ok(again)
    }

    /// Each stream's routes to the outputs, by output index: the statements
    /// that read it, and itself if it's an output.
    pub fn routes(&self, outputs: &[String]) -> Vec<Vec<(usize, Route)>> {
        let index: HashMap<&str, usize> = outputs
            .iter()
            .enumerate()
            .map(|(i, o)| (o.as_str(), i))
            .collect();
        let mut routes = vec![Vec::new(); self.inputs.len()];
        for (i, s) in self.statements.iter().enumerate() {
            if let Some(&o) = index.get(s.output.as_str()) {
                routes[s.input].push((o, Route::Statement(i)));
            }
        }
        for (i, input) in self.inputs.iter().enumerate() {
            if self.statements.iter().any(|s| s.output == input.name) {
                continue;
            }
            if let Some(&o) = index.get(input.name.as_str()) {
                routes[i].push((o, Route::Pass));
            }
        }
        routes
    }

    /// Checks the job's outputs and what a pipeline version's planning gave,
    /// where the request has it, against this plan. Each output is a
    /// statement's or a stream no statement writes, under the key it has.
    pub fn check(&self, t: &TransformPlan, outputs: &[StreamPlan]) -> Result<(), JobError> {
        if let Some(id) = t.operators.first() {
            return Err(JobError::config(format!(
                "transform.sql: the pipeline's version has stateful operator {id:?}, and its statements plan none now; plan and apply the pipeline again"
            )));
        }
        if !t.expected_outputs.is_empty() {
            let want: BTreeSet<&str> = t.expected_outputs.iter().map(|e| e.name.as_str()).collect();
            let got: BTreeSet<&str> = self.statements.iter().map(|s| s.output.as_str()).collect();
            if want != got {
                return Err(JobError::config(format!(
                    "transform.sql: the pipeline's version writes outputs {}, and its statements write {} now; plan and apply the pipeline again",
                    list(want),
                    list(got)
                )));
            }
            for e in &t.expected_outputs {
                if let Some(s) = self.statements.iter().find(|s| s.output == e.name)
                    && s.key != e.primary_key
                {
                    return Err(key_changed(&e.name, &e.primary_key, &s.key));
                }
            }
        }
        for s in &self.statements {
            let Some(o) = outputs.iter().find(|o| o.name == s.output) else {
                return Err(JobError::config(format!(
                    "the statement at {} writes {}, which isn't one of the job's outputs",
                    s.at, s.output
                )));
            };
            if o.primary_key != s.key {
                return Err(key_changed(&o.name, &o.primary_key, &s.key));
            }
            if o.changelog != self.inputs[s.input].changelog {
                return Err(JobError::config(format!(
                    "output {} is a changelog where its stream {} isn't, or isn't where it is",
                    o.name, self.inputs[s.input].name
                )));
            }
        }
        for o in outputs {
            if self.statements.iter().any(|s| s.output == o.name) {
                continue;
            }
            let Some(input) = self.inputs.iter().find(|i| i.name == o.name) else {
                return Err(JobError::config(format!(
                    "output {} is neither written by a statement nor a stream passed through",
                    o.name
                )));
            };
            if o.primary_key != input.key || o.changelog != input.changelog {
                return Err(JobError::config(format!(
                    "output {} passes stream {} through, and has another key or kind",
                    o.name, input.name
                )));
            }
        }
        Ok(())
    }
}

fn key_changed(output: &str, was: &[String], now: &[String]) -> JobError {
    JobError::config(format!(
        "output {output} has primary key ({}), and its statement gives it ({}) now; plan and apply the pipeline again",
        was.join(", "),
        now.join(", ")
    ))
}

fn list(names: BTreeSet<&str>) -> String {
    match names.is_empty() {
        true => "none".into(),
        false => names.into_iter().collect::<Vec<_>>().join(", "),
    }
}

/// Plans every statement, with one input's schema replaced if `changed`.
fn plan_statements(
    sql: &Sql,
    inputs: &[Input],
    changed: Option<(usize, SchemaRef)>,
) -> Result<Vec<Statement>, JobError> {
    let mut catalog = Catalog::new();
    let mut unknown = Vec::new();
    for (i, input) in inputs.iter().enumerate() {
        let schema = match &changed {
            Some((c, s)) if *c == i => Some(s.clone()),
            _ => input.schema.clone(),
        };
        let stream = match schema {
            // Every column is nullable: a batch may lack one, and a
            // delete has only its key.
            Some(schema) => Stream {
                schema: Arc::new(Schema::new(
                    schema
                        .fields()
                        .iter()
                        .map(|f| f.as_ref().clone().with_nullable(true))
                        .collect::<Vec<_>>(),
                )),
                key: input.key.clone(),
                changelog: input.changelog,
            },
            None => {
                unknown.push(input.name.as_str());
                Stream {
                    schema: Arc::new(Schema::empty()),
                    key: Vec::new(),
                    changelog: input.changelog,
                }
            }
        };
        catalog
            .insert(input.name.clone(), stream)
            .map_err(|e| JobError::config(format!("transform.sql: {e}")))?;
    }
    let unknown_note = |names: &[&str]| {
        format!(
            "{} {} no known columns: a statement reads a stream whose source declares its columns, or one an earlier sync has recorded",
            match names.len() {
                1 => format!("stream {}", names[0]),
                _ => format!("streams {}", names.join(", ")),
            },
            if names.len() == 1 { "has" } else { "have" }
        )
    };
    let plan = sql.plan(&catalog).map_err(|e| {
        let mut msg = format!("transform.sql: {e}");
        if !unknown.is_empty() {
            msg.push('\n');
            msg.push_str(&unknown_note(&unknown));
        }
        JobError::config(msg)
    })?;
    let index: HashMap<&str, usize> = inputs
        .iter()
        .enumerate()
        .map(|(i, s)| (s.name.as_str(), i))
        .collect();
    let mut out = Vec::with_capacity(plan.statements.len());
    for s in plan.statements {
        let Operator::RowWise(op) = s.operator;
        let Some(&input) = index.get(op.input.as_str()) else {
            return Err(JobError::internal(format!(
                "the statement at {} reads {}, which isn't a stream",
                s.at, op.input
            )));
        };
        let reads_unknown: Vec<&str> = s
            .inputs
            .iter()
            .map(String::as_str)
            .filter(|i| unknown.contains(i))
            .collect();
        if !reads_unknown.is_empty() {
            return Err(JobError::config(format!(
                "transform.sql: {}: {}",
                s.at,
                unknown_note(&reads_unknown)
            )));
        }
        out.push(Statement {
            output: s.output,
            input,
            at: s.at,
            schema: s.schema,
            key: s.key,
            op: Arc::new(op),
        });
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    pub(crate) fn input(name: &str, key: &[&str], changelog: bool) -> Input {
        Input {
            name: name.into(),
            schema: Some(Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("email", DataType::Utf8, true),
            ]))),
            key: key.iter().map(|k| k.to_string()).collect(),
            changelog,
        }
    }

    #[test]
    fn statements_route_their_input_to_their_output() {
        let t = Transform::plan(
            "INSERT INTO a SELECT id, lower(email) AS email FROM a; INSERT INTO c SELECT id AS cid FROM a",
            vec![input("a", &["id"], false), input("b", &[], false)],
        )
        .unwrap();
        let outputs = ["a", "b", "c"].map(String::from);
        let routes = t.routes(&outputs);
        assert_eq!(
            routes[0],
            [(0, Route::Statement(0)), (2, Route::Statement(1))]
        );
        assert_eq!(routes[1], [(1, Route::Pass)]);
        assert_eq!(t.statements[1].key, ["cid"]);
        assert!(t.routes(&outputs[..1])[1].is_empty());
    }

    #[test]
    fn a_stream_without_known_columns_cant_be_read() {
        let mut unknown = input("b", &[], false);
        unknown.schema = None;
        let e =
            Transform::plan("INSERT INTO c SELECT * FROM b", vec![unknown.clone()]).unwrap_err();
        assert!(
            e.message.contains("1:1: stream b has no known columns"),
            "{}",
            e.message
        );
        let e = Transform::plan("INSERT INTO c SELECT id FROM b", vec![unknown]).unwrap_err();
        assert!(e.message.contains("there's no column id"), "{}", e.message);
        assert!(
            e.message.contains("stream b has no known columns"),
            "{}",
            e.message
        );
    }

    #[test]
    fn later_tiers_are_refused_with_their_position() {
        let e = Transform::plan(
            "INSERT INTO c\nSELECT count(*) AS n FROM a",
            vec![input("a", &["id"], false)],
        )
        .unwrap_err();
        assert_eq!(
            e.code,
            openetl_proto::connector::v1::ErrorCode::ConfigInvalid
        );
        assert!(e.message.starts_with("transform.sql: 2:"), "{}", e.message);
    }

    #[test]
    fn planning_again_keeps_the_output_and_its_key() {
        let t = Transform::plan(
            "INSERT INTO c SELECT id, email FROM a",
            vec![input("a", &["id"], false)],
        )
        .unwrap();
        let wider = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let again = t.again(0, wider).unwrap();
        assert_eq!(again.key, ["id"]);
        let gone = Arc::new(Schema::new(vec![Field::new("email", DataType::Utf8, true)]));
        let e = t.again(0, gone).unwrap_err();
        assert_eq!(e.code, openetl_proto::connector::v1::ErrorCode::SchemaDrift);
    }

    /// A schema a transform's input recorded is as statements read it, so
    /// a source column with a metadata column's name stays.
    #[test]
    fn a_recorded_input_schema_keeps_its_columns() {
        let typed = Schema::new(vec![
            Field::new("_openetl_id", DataType::Utf8, true),
            Field::new("id", DataType::Int64, true),
        ]);
        let read = as_read(&typed);
        let names: Vec<&str> = read.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["_openetl_id", "id"]);
    }

    /// A schema recorded before the pipeline had a transform is as the
    /// destination got it.
    #[test]
    fn a_schema_the_destination_got_reads_as_the_source_sent_it() {
        let shaped = Schema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("x", DataType::Int64, true),
            Field::new("x__v2", DataType::Utf8, true),
            Field::new("_openetl_id", DataType::FixedSizeBinary(16), false),
            Field::new("_openetl_op", DataType::Utf8, false),
        ])
        .with_metadata(HashMap::from([
            (
                openetl_normalize::naming::SOURCE_NAMES_KEY.to_owned(),
                r#"{"user_id":"UserId"}"#.to_owned(),
            ),
            (VERSIONS_KEY.to_owned(), r#"{"x":2}"#.to_owned()),
            (
                openetl_normalize::naming::TABLE_KEY.to_owned(),
                "users".to_owned(),
            ),
        ]));
        let read = as_read(&shaped);
        let fields: Vec<(&str, &DataType)> = read
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.data_type()))
            .collect();
        assert_eq!(
            fields,
            [("UserId", &DataType::Int64), ("x", &DataType::Utf8)]
        );
        assert!(read.metadata().is_empty());
    }

    #[test]
    fn the_restored_schema_takes_declared_types_and_columns() {
        let declared = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "at",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]);
        let restored = Schema::new(vec![
            Field::new("extra", DataType::Utf8, true),
            Field::new("id", DataType::Utf8, true),
        ]);
        let s = StreamPlan {
            name: "a".into(),
            arrow_schema: ipc::encode_schema(&Arc::new(declared)).unwrap(),
            restored_schema: ipc::encode_schema(&Arc::new(restored)).unwrap(),
            nanosecond_timestamps: true,
            ..Default::default()
        };
        let schema = Input::of(&s).unwrap().schema.unwrap();
        let names: Vec<(&str, &DataType)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.data_type()))
            .collect();
        assert_eq!(
            names,
            [
                ("extra", &DataType::Utf8),
                ("id", &DataType::Int64),
                (
                    "at",
                    &DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("UTC".into()))
                ),
            ]
        );
    }
}
