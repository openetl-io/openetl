//! Row-wise SQL statements in a lane. A statement that writes a stream it
//! reads replaces that stream's rows with its own. It binds to the stream's
//! schema at the stream's first batch, and again whenever that changes.
//! `_openetl_*` columns aren't the statement's: they pass through after its
//! outputs, filtered with them.

use std::str::FromStr;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use openetl_proto::connector::v1::ErrorCode;
use openetl_sql::{Catalog, Operator, RowWise, Sql, Stream};

use crate::error::JobError;

const METADATA: &str = "_openetl_";

/// A job's SQL.
#[derive(Debug)]
pub struct Transforms {
    sql: Sql,
}

impl FromStr for Transforms {
    type Err = JobError;

    fn from_str(text: &str) -> Result<Self, JobError> {
        let sql = Sql::parse(text).map_err(|e| JobError::config(format!("transform.sql: {e}")))?;
        Ok(Transforms { sql })
    }
}

impl Transforms {
    /// The statement that replaces `stream`: one that writes it and reads
    /// only it.
    pub fn replacing(&self, stream: &str) -> Option<usize> {
        (0..self.sql.len())
            .find(|&i| self.sql.output(i) == stream && self.sql.inputs(i) == [stream])
    }
}

/// A stream's statement in one lane.
pub struct Stage {
    sql: Arc<Transforms>,
    statement: usize,
    stream: String,
    changelog: bool,
    bound: Option<Bound>,
    /// The schema of what it gave last.
    gave: Option<SchemaRef>,
}

struct Bound {
    input: SchemaRef,
    op: RowWise,
    key: Vec<String>,
    carry: Vec<String>,
}

/// What a stage gives for a batch.
pub struct Applied {
    pub batch: RecordBatch,
    /// Its schema isn't the last batch's.
    pub schema_changed: bool,
}

impl Stage {
    pub fn new(sql: Arc<Transforms>, statement: usize, stream: &str, changelog: bool) -> Self {
        Stage {
            sql,
            statement,
            stream: stream.to_owned(),
            changelog,
            bound: None,
            gave: None,
        }
    }

    /// The output's primary key, once bound.
    pub fn key(&self) -> Option<&[String]> {
        self.bound.as_ref().map(|b| b.key.as_slice())
    }

    fn bind(&mut self, schema: &SchemaRef, key: Vec<String>) -> Result<&Bound, JobError> {
        let (mine, carry): (Vec<_>, Vec<_>) = schema
            .fields()
            .iter()
            .cloned()
            .partition(|f| !f.name().starts_with(METADATA));
        let catalog = Catalog::default().with(
            self.stream.clone(),
            Stream {
                schema: Arc::new(Schema::new(mine)),
                key,
                changelog: self.changelog,
            },
        );
        let statement = self
            .sql
            .sql
            .bind(self.statement, &catalog)
            .map_err(|e| JobError::config(format!("transform.sql: {e}")))?;
        let Operator::RowWise(op) = statement.operator else {
            return Err(JobError::config(format!(
                "transform.sql: {}: the statement writing {} isn't row-wise, which a lane can't run yet",
                statement.at, self.stream
            )));
        };
        Ok(self.bound.insert(Bound {
            input: schema.clone(),
            op,
            key: statement.key,
            carry: carry.iter().map(|f| f.name().clone()).collect(),
        }))
    }

    /// Runs the statement on a shaped batch of the stream, whose key is
    /// `key`.
    pub fn apply(
        &mut self,
        batch: &RecordBatch,
        key: impl FnOnce() -> Vec<String>,
    ) -> Result<Applied, JobError> {
        let schema = batch.schema();
        let b = match &self.bound {
            Some(b) if b.input == schema => b,
            _ => self.bind(&schema, key())?,
        };
        let carry: Vec<&str> = b.carry.iter().map(String::as_str).collect();
        let out = b.op.eval(batch, &carry).map_err(|e| {
            JobError::new(
                ErrorCode::DataInvalid,
                format!("transform.sql on {}: {e}", self.stream),
            )
        })?;
        let changed = self.gave.as_ref() != Some(&out.schema());
        if changed {
            self.gave = Some(out.schema());
        }
        Ok(Applied {
            batch: out,
            schema_changed: changed,
        })
    }
}
