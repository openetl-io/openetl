//! A job's SQL transform: its statements planned against the streams it
//! reads, and which outputs each stream feeds. Nothing planned outlives the
//! job: every attempt plans again from the SQL and the streams' schemas.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use arrow_schema::{Field, Schema, SchemaRef};
use openetl_normalize::VERSIONS_KEY;
use openetl_normalize::naming::{METADATA_COLUMNS, TABLE_KEY, source_names};
use openetl_proto::engine::v1::{StreamPlan, Transform as TransformPlan};
use openetl_sql::{
    Catalog, Dedupe, GroupWindow, Keep, Operator, Over, Position, RowWise, Sql, Stream,
};

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
    /// The column of its rows' event time, which group windows and OVER go
    /// by.
    pub event_time: Option<String>,
    /// How long after a group window over it closes a row may still come,
    /// in microseconds. OVER doesn't take it.
    pub lateness: i64,
    /// How long a dedupe that keeps each key's first row remembers a key,
    /// in microseconds of event time; `openetl_state::NEVER` for good, and
    /// None where the stream sets none.
    pub dedupe: Option<i64>,
    /// It's read whole each sync, each read with no state for the
    /// statements that read it.
    pub full_read: bool,
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
            event_time: s
                .event_time
                .as_ref()
                .map(|e| e.column.clone())
                .filter(|t| !t.is_empty()),
            lateness: s.event_time.as_ref().map_or(0, |e| e.lateness_micros),
            dedupe: s
                .event_time
                .as_ref()
                .and_then(|e| e.dedupe_retention_micros),
            full_read: s.event_time.as_ref().is_some_and(|e| e.full_read),
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
    /// Its row-wise steps: a stateful operator's, those before it.
    pub op: Arc<RowWise>,
    /// Its stateful operator, which keyed lanes run, and the operator's ID;
    /// None for a row-wise statement.
    pub keyed: Option<(Keyed, String)>,
}

/// A statement's stateful operator. Its input's rows go by the key groups
/// of its need to the keyed lanes that own them, which keep its state.
#[derive(Debug, Clone)]
pub enum Keyed {
    Window(Arc<GroupWindow>),
    Over(Arc<Over>),
    Dedupe(Arc<Dedupe>),
}

impl Keyed {
    /// The stream's columns whose key groups its rows go by; none for every
    /// row on one keyed lane.
    pub fn need(&self) -> &[String] {
        match self {
            Keyed::Window(g) => &g.need,
            Keyed::Over(o) => &o.need,
            Keyed::Dedupe(d) => &d.need,
        }
    }

    /// Whether its rows go to it with their record IDs, which it keeps.
    pub fn ids(&self) -> bool {
        !matches!(self, Keyed::Window(_))
    }

    /// What it is, for messages.
    pub fn what(&self) -> &'static str {
        match self {
            Keyed::Window(_) => "the group window",
            Keyed::Over(_) => "OVER",
            Keyed::Dedupe(_) => "the dedupe",
        }
    }
}

/// How a stream reaches one of the outputs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    /// It is that output, as it would be without a transform.
    Pass,
    /// A statement, by its index, writes that output from it.
    Statement(usize),
    /// A stateful statement, by its index, writes that output from it: its
    /// rows go by key group to the keyed lanes that run it.
    Keyed(usize),
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
    /// a lane that sees the stream at another schema, from `was`, needs it.
    /// Its output and key stay the job's, or the job stops with
    /// SCHEMA_DRIFT.
    pub fn again(
        &self,
        statement: usize,
        was: &Schema,
        schema: SchemaRef,
    ) -> Result<Statement, JobError> {
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
        // The columns it reads whose types changed, each with the cast that
        // reads it as before.
        let casts: Vec<String> = st
            .op
            .reads
            .fields()
            .iter()
            .filter_map(|r| {
                let old = was.field_with_name(r.name()).ok()?;
                let new = schema.field_with_name(r.name()).ok()?;
                if old.data_type() == new.data_type() {
                    return None;
                }
                let t = openetl_sql::cast_type(old)?;
                Some(format!(
                    "{} changed from {} to {}: write CAST({} AS {t}) in its place to read it as before",
                    r.name(),
                    openetl_normalize::logical_type(old),
                    openetl_normalize::logical_type(new),
                    openetl_sql::ident(r.name())
                ))
            })
            .collect();
        let mut planned = plan_statements(&self.sql, &self.inputs, Some((st.input, schema)))
            .map_err(|e| {
                let mut what = format!(
                    "the statement writing {} no longer plans: {}",
                    st.output, e.message
                );
                if !casts.is_empty() {
                    what.push_str("; column ");
                    what.push_str(&casts.join("; column "));
                }
                drift(what)
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

    /// The stateful operators' IDs.
    pub fn operators(&self) -> impl Iterator<Item = &str> {
        self.statements
            .iter()
            .filter_map(|s| s.keyed.as_ref().map(|(_, id)| id.as_str()))
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
                let route = match s.keyed {
                    Some(_) => Route::Keyed(i),
                    None => Route::Statement(i),
                };
                routes[s.input].push((o, route));
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
        let want: BTreeSet<&str> = t.operators.iter().map(String::as_str).collect();
        let got: BTreeSet<&str> = self.operators().collect();
        if want != got {
            return Err(JobError::config(format!(
                "transform.sql: the pipeline's version has stateful operators {}, and its statements plan {} now; plan and apply the pipeline again",
                list(want),
                list(got)
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

/// Whether jobs run an operator: every one that plans.
pub(crate) fn runs(op: &Operator) -> bool {
    match op {
        Operator::RowWise(_)
        | Operator::GroupWindow(_)
        | Operator::Over(_)
        | Operator::Dedupe(_) => true,
    }
}

/// Why a dedupe that keeps each key's first row, of the statement at `at`
/// that writes `output`, is refused over `stream`, which sets no
/// retention: what to set, as the plan says it.
pub(crate) fn no_retention(at: Position, output: &str, stream: &str) -> String {
    format!(
        "transform.sql:{at}: the dedupe writing \"{output}\", ROW_NUMBER() … = 1 in ascending order of the event time, keeps each key's first row, \
         and stream \"{stream}\" sets no dedupe.retention, how long it remembers a key: set source.streams[].dedupe.retention to a duration of event time, \
         such as 24h, after which a key's next row is written again, or to unbounded, to remember every key"
    )
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
            // delete may have only its key.
            Some(schema) => Stream {
                event_time: input
                    .event_time
                    .clone()
                    .filter(|t| schema.field_with_name(t).is_ok()),
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
                    event_time: None,
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
    let plan = sql
        .plan(&catalog)
        .and_then(|p| p.refuse_unless(runs))
        .map_err(|e| {
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
        let (op, keyed) = match s.operator {
            Operator::RowWise(op) => (op, None),
            Operator::GroupWindow(g) => {
                let id = g.id.clone();
                (g.input.clone(), Some((Keyed::Window(Arc::new(*g)), id)))
            }
            Operator::Over(o) => {
                let id = o.id.clone();
                (o.input.clone(), Some((Keyed::Over(Arc::new(*o)), id)))
            }
            Operator::Dedupe(d) => {
                let id = d.id.clone();
                (d.input.clone(), Some((Keyed::Dedupe(Arc::new(*d)), id)))
            }
        };
        let Some(&input) = index.get(op.input.as_str()) else {
            return Err(JobError::internal(format!(
                "the statement at {} reads {}, which isn't a stream",
                s.at, op.input
            )));
        };
        if let Some((Keyed::Dedupe(d), _)) = &keyed
            && d.keep == Keep::First
            && inputs[input].dedupe.is_none()
        {
            return Err(JobError::config(no_retention(
                s.at,
                &s.output,
                &inputs[input].name,
            )));
        }
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
            keyed,
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
            event_time: None,
            lateness: 0,
            dedupe: None,
            full_read: false,
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

    /// A chain of views `depth` deep, each reading the one before and its
    /// column twice, and a statement over the last.
    pub(crate) fn chain(depth: usize) -> String {
        let mut sql = String::from("CREATE VIEW v0 AS SELECT id, email FROM a;\n");
        for i in 1..=depth {
            sql.push_str(&format!(
                "CREATE VIEW v{i} AS SELECT id + id AS id, email FROM v{};\n",
                i - 1
            ));
        }
        sql.push_str(&format!("INSERT INTO c SELECT id, email FROM v{depth}"));
        sql
    }

    /// A job plans the deepest chain of views on a thread with a tokio
    /// worker's stack, and refuses a deeper one as CONFIG_INVALID.
    #[test]
    fn a_chain_of_views_plans_to_its_limit_on_a_workers_stack() {
        std::thread::Builder::new()
            .stack_size(2 << 20)
            .spawn(|| {
                // v0 and the views after it: VIEW_DEPTH views.
                let deepest = chain(openetl_sql::VIEW_DEPTH - 1);
                Transform::plan(&deepest, vec![input("a", &["id"], false)]).unwrap();
                let e = Transform::plan(
                    &chain(openetl_sql::VIEW_DEPTH),
                    vec![input("a", &["id"], false)],
                )
                .unwrap_err();
                assert_eq!(
                    e.code,
                    openetl_proto::connector::v1::ErrorCode::ConfigInvalid
                );
                assert!(
                    e.message
                        .contains("view v32 is 33 views deep, counting each view it reads through: views go at most 32 deep"),
                    "{}",
                    e.message
                );
            })
            .unwrap()
            .join()
            .unwrap();
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

    /// OVER and dedupe run on keyed lanes, by their PARTITION BY; a dedupe
    /// that keeps each key's first row needs its stream's retention.
    #[test]
    fn over_and_dedupe_run_on_keyed_lanes() {
        let mut a = input("a", &["id"], false);
        a.schema = Some(Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, true),
            Field::new(
                "at",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ])));
        a.event_time = Some("at".into());
        let t = Transform::plan(
            "INSERT INTO c\nSELECT id, sum(amount) OVER (PARTITION BY id ORDER BY at) AS total FROM a",
            vec![a.clone()],
        )
        .unwrap();
        let Some((keyed, id)) = &t.statements[0].keyed else {
            panic!("{:?}", t.statements[0]);
        };
        assert!(matches!(keyed, Keyed::Over(_)), "{keyed:?}");
        assert_eq!(keyed.need(), ["id"]);
        assert!(id.starts_with("over-"), "{id}");
        assert_eq!(t.routes(&["c".into()])[0], [(0, Route::Keyed(0))]);
        assert_eq!(t.operators().collect::<Vec<_>>(), [id.as_str()]);
        let dedupe = |order: &str| {
            format!(
                "INSERT INTO c\nSELECT id, amount FROM a\nQUALIFY row_number() OVER (PARTITION BY id ORDER BY at {order}) = 1"
            )
        };
        let t = Transform::plan(&dedupe("DESC"), vec![a.clone()]).unwrap();
        let Some((keyed, id)) = &t.statements[0].keyed else {
            panic!("{:?}", t.statements[0]);
        };
        assert!(
            matches!(keyed, Keyed::Dedupe(_)) && keyed.ids(),
            "{keyed:?}"
        );
        assert!(id.starts_with("dedupe-"), "{id}");
        let e = Transform::plan(&dedupe(""), vec![a.clone()]).unwrap_err();
        assert_eq!(
            e.code,
            openetl_proto::connector::v1::ErrorCode::ConfigInvalid
        );
        // Word for word what the plan says, as internal/plan's dedupeProblems
        // has it.
        assert_eq!(
            e.message,
            "transform.sql:1:1: the dedupe writing \"c\", ROW_NUMBER() … = 1 in ascending order of the event time, keeps each key's first row, \
             and stream \"a\" sets no dedupe.retention, how long it remembers a key: set source.streams[].dedupe.retention to a duration of event time, \
             such as 24h, after which a key's next row is written again, or to unbounded, to remember every key"
        );
        a.dedupe = Some(openetl_state::NEVER);
        Transform::plan(&dedupe(""), vec![a]).unwrap();
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
        let was = t.inputs[0].schema.clone().unwrap();
        let again = t.again(0, &was, wider).unwrap();
        assert_eq!(again.key, ["id"]);
        let gone = Arc::new(Schema::new(vec![Field::new("email", DataType::Utf8, true)]));
        let e = t.again(0, &was, gone).unwrap_err();
        assert_eq!(e.code, openetl_proto::connector::v1::ErrorCode::SchemaDrift);
    }

    /// A statement that no longer plans says which columns it reads
    /// changed, and the cast that reads each as before.
    #[test]
    fn a_statement_that_no_longer_plans_gives_the_cast_to_write() {
        let t = Transform::plan(
            "INSERT INTO c SELECT id, lower(email) AS email FROM a",
            vec![input("a", &["id"], false)],
        )
        .unwrap();
        let was = t.inputs[0].schema.clone().unwrap();
        let changed = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Int64, true),
        ]));
        let e = t.again(0, &was, changed).unwrap_err();
        assert_eq!(e.code, openetl_proto::connector::v1::ErrorCode::SchemaDrift);
        assert!(
            e.message.contains(
                "column email changed from string to int64: write CAST(email AS VARCHAR) in its place"
            ),
            "{}",
            e.message
        );
    }

    /// A column whose name needs quotes gets them in the cast, which plans.
    #[test]
    fn the_cast_to_write_quotes_a_name_that_needs_it() {
        let schema = |t: DataType| {
            Some(Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("Email Address", t, true),
            ])))
        };
        let a = |t: DataType| Input {
            name: "a".into(),
            schema: schema(t),
            key: vec!["id".into()],
            changelog: false,
            event_time: None,
            lateness: 0,
            dedupe: None,
            full_read: false,
        };
        let t = Transform::plan(
            r#"INSERT INTO c SELECT id, lower("Email Address") AS e FROM a"#,
            vec![a(DataType::Utf8)],
        )
        .unwrap();
        let was = t.inputs[0].schema.clone().unwrap();
        let e = t
            .again(0, &was, schema(DataType::Int64).unwrap())
            .unwrap_err();
        let cast = r#"CAST("Email Address" AS VARCHAR)"#;
        assert!(e.message.contains(cast), "{}", e.message);
        let fixed = format!("INSERT INTO c SELECT id, lower({cast}) AS e FROM a");
        Transform::plan(&fixed, vec![a(DataType::Int64)]).unwrap();
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
