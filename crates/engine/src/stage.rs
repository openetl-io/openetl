//! What runs on a lane between the normalizer's two halves, from a stream to
//! one of the outputs it feeds: nothing, or a row-wise statement. It takes
//! batches parsed and typed, under the source's names, and gives what
//! destination naming, metadata columns and holds apply to.
//!
//! A statement keeps each row's `_openetl_id`, op and seq. On a changelog
//! stream a delete passes as a delete of its key, and an insert or update
//! that a filter drops becomes one. A row `UNNEST` gives gets an ID of its
//! own, derived from its source row's. A row the statement can't evaluate
//! is the caller's to quarantine.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, RecordBatch, RecordBatchOptions, UInt64Array,
    new_null_array,
};
use arrow_schema::{Field, Schema, SchemaRef};
use openetl_normalize::{Control, Typed, element_ids};
use openetl_proto::connector::v1::ErrorCode;
use openetl_sql::{RowWise, Step};

use crate::error::JobError;
use crate::transform::Transform;

#[derive(Debug, Default)]
pub enum Stage {
    /// Batches pass as they are.
    #[default]
    Identity,
    Statement(Box<Bound>),
}

/// What a stage gives for a batch.
#[derive(Debug)]
pub struct Applied {
    /// Its rows are the output's, and `rows` gives each one's row among
    /// those the stream's batch was typed from. Nothing in it is rejected.
    pub typed: Typed,
    /// Rows the statement couldn't evaluate, by their row among those the
    /// stream's batch was typed from, and why.
    pub failed: Vec<(usize, String)>,
}

impl Stage {
    /// Statement `statement` of `transform`, on a lane.
    pub fn statement(transform: &Arc<Transform>, statement: usize) -> Self {
        let st = &transform.statements[statement];
        let input = &transform.inputs[st.input];
        Stage::Statement(Box::new(Bound {
            transform: transform.clone(),
            statement,
            op: st.op.clone(),
            planned: input.schema.clone().unwrap_or_else(|| st.op.reads.clone()),
            deps: Deps::of(&st.op),
            names: st
                .schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
            schema: nullable(&st.schema),
        }))
    }

    /// `typed` is a batch of the stream, and `pushed` each of its rows' row
    /// among those it was typed from. `cursor` is the stream's cursor
    /// column, whose values order an append-only output's versions.
    pub fn apply(
        &mut self,
        typed: &Typed,
        pushed: &[usize],
        cursor: Option<&str>,
    ) -> Result<Applied, JobError> {
        match self {
            Stage::Identity => Ok(Applied {
                typed: Typed {
                    batch: typed.batch.clone(),
                    ids: typed.ids.clone(),
                    control: typed.control.clone(),
                    cells: typed.cells.clone(),
                    rejected: Vec::new(),
                    rows: Some(pushed.to_vec()),
                },
                failed: Vec::new(),
            }),
            Stage::Statement(b) => b.apply(typed, pushed, cursor),
        }
    }
}

/// A statement as one lane runs it, planned against the schema the lane
/// last saw its stream at.
#[derive(Debug)]
pub struct Bound {
    transform: Arc<Transform>,
    statement: usize,
    op: Arc<RowWise>,
    /// The stream's schema it's planned against. A batch with a column it
    /// lacks, or a column of another type, plans it again; one without a
    /// column it reads gives nulls.
    planned: SchemaRef,
    deps: Deps,
    /// The output's columns' names.
    names: Vec<String>,
    /// The output's schema, every column nullable, since a delete carries
    /// only its key.
    schema: SchemaRef,
}

/// Which of the columns a statement reads each part of it depends on, by
/// their index among them.
#[derive(Debug, Default)]
struct Deps {
    filters: BTreeSet<usize>,
    /// By output column.
    columns: Vec<BTreeSet<usize>>,
    /// By step, then by the column it gives; empty for a step that isn't a
    /// projection.
    steps: Vec<Vec<BTreeSet<usize>>>,
}

impl Deps {
    fn of(op: &RowWise) -> Self {
        let mut cur: Vec<BTreeSet<usize>> = (0..op.reads.fields().len())
            .map(|i| BTreeSet::from([i]))
            .collect();
        let mut filters = BTreeSet::new();
        let mut steps = Vec::with_capacity(op.steps.len());
        let of = |cur: &[BTreeSet<usize>], inputs: &[usize]| -> BTreeSet<usize> {
            inputs
                .iter()
                .filter_map(|&j| cur.get(j))
                .flatten()
                .copied()
                .collect()
        };
        for step in &op.steps {
            match step {
                Step::Filter(p) => {
                    filters.extend(of(&cur, p.inputs()));
                    steps.push(Vec::new());
                }
                Step::Project(p) => {
                    cur = p
                        .columns
                        .iter()
                        .map(|c| of(&cur, c.expr.inputs()))
                        .collect();
                    steps.push(cur.clone());
                }
                Step::Unnest(u) => {
                    cur = u
                        .sources
                        .iter()
                        .map(|&s| cur.get(s).cloned().unwrap_or_default())
                        .collect();
                    steps.push(Vec::new());
                }
            }
        }
        Self {
            filters,
            columns: cur,
            steps,
        }
    }
}

/// What an update didn't send, as the statement reads it.
struct Unsent {
    /// The columns the statement reads that it didn't send, by index.
    reads: BTreeSet<usize>,
    /// The output columns it leaves unchanged.
    unchanged: Vec<String>,
}

/// The output of running a statement's steps on some rows.
struct Ran {
    batch: RecordBatch,
    /// Each row's row of the input.
    rows: Vec<usize>,
    ids: FixedSizeBinaryArray,
}

impl Bound {
    fn st(&self) -> &crate::transform::Statement {
        &self.transform.statements[self.statement]
    }

    fn reason(&self, why: &str) -> String {
        let st = self.st();
        format!("transform.sql {}, writing {}: {why}", st.at, st.output)
    }

    /// Plans the statement again if the batch has a column the plan lacks,
    /// or one of another type.
    fn plan_for(&mut self, schema: &Schema) -> Result<(), JobError> {
        let changed = schema.fields().iter().any(|f| {
            self.planned
                .field_with_name(f.name())
                .map_or(true, |p| p.data_type() != f.data_type())
        });
        if !changed {
            return Ok(());
        }
        let mut fields: Vec<Field> = self
            .planned
            .fields()
            .iter()
            .map(|p| match schema.field_with_name(p.name()) {
                Ok(f) => f.clone(),
                Err(_) => p.as_ref().clone(),
            })
            .collect();
        for f in schema.fields() {
            if self.planned.field_with_name(f.name()).is_err() {
                fields.push(f.as_ref().clone());
            }
        }
        let planned = Arc::new(Schema::new(fields));
        let again = self.transform.again(self.statement, planned.clone())?;
        self.deps = Deps::of(&again.op);
        self.names = again
            .schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        self.schema = nullable(&again.schema);
        self.op = again.op;
        self.planned = planned;
        Ok(())
    }

    /// The columns the statement reads, from the batch, or null where it
    /// lacks one.
    fn input(&self, batch: &RecordBatch) -> Result<RecordBatch, JobError> {
        let n = batch.num_rows();
        let mut fields = Vec::with_capacity(self.op.reads.fields().len());
        let mut columns = Vec::with_capacity(fields.capacity());
        for f in self.op.reads.fields() {
            let column = match batch.column_by_name(f.name()) {
                Some(c) if c.data_type() == f.data_type() => c.clone(),
                Some(c) => {
                    return Err(JobError::internal(format!(
                        "column {} is {}, and the statement was planned for {}",
                        f.name(),
                        c.data_type(),
                        f.data_type()
                    )));
                }
                None => new_null_array(f.data_type(), n),
            };
            fields.push(f.as_ref().clone().with_nullable(true));
            columns.push(column);
        }
        batch_of(Arc::new(Schema::new(fields)), columns, n)
    }

    fn apply(
        &mut self,
        typed: &Typed,
        pushed: &[usize],
        cursor: Option<&str>,
    ) -> Result<Applied, JobError> {
        self.plan_for(&typed.batch.schema())?;
        let input = self.input(&typed.batch)?;
        match self.op.changelog {
            true => self.changelog(typed, input, pushed),
            false => self.append(typed, input, pushed, cursor),
        }
    }

    fn append(
        &self,
        typed: &Typed,
        input: RecordBatch,
        pushed: &[usize],
        cursor: Option<&str>,
    ) -> Result<Applied, JobError> {
        let all: Vec<usize> = (0..input.num_rows()).collect();
        let (ran, failed) = self.run_rows(&input, &typed.ids, &all, &BTreeSet::new())?;
        let seq = match (
            &typed.control.seq,
            cursor.and_then(|c| typed.batch.column_by_name(c)),
        ) {
            (Some(seq), _) | (None, Some(seq)) => Some(take(seq, &ran.rows)?),
            (None, None) => None,
        };
        let batch = self.output(&ran.batch)?;
        Ok(Applied {
            typed: Typed {
                batch,
                ids: ran.ids,
                control: Control {
                    op: None,
                    seq,
                    unchanged: None,
                },
                cells: Vec::new(),
                rejected: Vec::new(),
                rows: Some(ran.rows.iter().map(|&r| pushed[r]).collect()),
            },
            failed: failed
                .into_iter()
                .map(|(r, why)| (pushed[r], why))
                .collect(),
        })
    }

    /// A changelog's batch: every row the statement can evaluate gives one,
    /// in order. Inserts and updates that pass its filters are the
    /// statement's rows; deletes, and the rest, deletes of their key.
    fn changelog(
        &self,
        typed: &Typed,
        input: RecordBatch,
        pushed: &[usize],
    ) -> Result<Applied, JobError> {
        let n = input.num_rows();
        let ops: Vec<Option<&str>> = match &typed.control.op {
            Some(o) => o.as_string::<i32>().iter().collect(),
            None => vec![None; n],
        };
        let lists: Vec<Option<ArrayRef>> = match &typed.control.unchanged {
            Some(u) => u.as_list::<i32>().iter().collect(),
            None => vec![None; n],
        };
        let op = |r: usize| ops.get(r).copied().flatten();
        let mut failed = Vec::new();
        let mut unchanged: Vec<Option<Vec<String>>> = vec![None; n];
        // The rows to run, by the columns the statement reads that their
        // update didn't send.
        let mut runs: BTreeMap<BTreeSet<usize>, Vec<usize>> = BTreeMap::new();
        let mut dead = vec![false; n];
        for (r, u) in unchanged.iter_mut().enumerate() {
            if op(r) == Some("delete") {
                continue;
            }
            let names = lists
                .get(r)
                .cloned()
                .flatten()
                .filter(|_| op(r) == Some("update"));
            match self.unchanged(names) {
                Ok(None) => runs.entry(BTreeSet::new()).or_default().push(r),
                Ok(Some(unsent)) => {
                    *u = Some(unsent.unchanged);
                    runs.entry(unsent.reads).or_default().push(r);
                }
                Err(why) => {
                    failed.push((r, why));
                    dead[r] = true;
                }
            }
        }
        // Each row that passes: its run, and its row in the run's output.
        let mut passed = vec![None; n];
        let mut parts = Vec::with_capacity(runs.len());
        for (unsent, rows) in &runs {
            let (ran, more) = self.run_rows(&input, &typed.ids, rows, unsent)?;
            for (r, why) in more {
                dead[r] = true;
                failed.push((r, why));
            }
            if let Some(why) = self.entering(unsent) {
                for &r in &ran.rows {
                    dead[r] = true;
                    failed.push((r, why.clone()));
                }
                continue;
            }
            for (i, &r) in ran.rows.iter().enumerate() {
                passed[r] = Some((parts.len(), i));
            }
            parts.push(self.output(&ran.batch)?);
        }
        failed.sort_by_key(|(r, _)| *r);

        // Rows that pass, then deletes of the rest's keys, interleaved in
        // the order they came.
        let deleted: Vec<usize> = (0..n)
            .filter(|&r| !dead[r] && passed[r].is_none())
            .collect();
        let keys = self.keys(&typed.batch, &deleted)?;
        let mut order = Vec::with_capacity(n);
        let mut d = 0;
        let mut rows = Vec::with_capacity(n);
        for r in (0..n).filter(|&r| !dead[r]) {
            rows.push(r);
            match passed[r] {
                Some(at) => order.push(at),
                None => {
                    order.push((parts.len(), d));
                    d += 1;
                }
            }
        }
        let mut columns = Vec::with_capacity(keys.len());
        for (c, k) in keys.iter().enumerate() {
            let mut arrays: Vec<&dyn Array> = parts.iter().map(|p| p.column(c).as_ref()).collect();
            arrays.push(k.as_ref());
            columns.push(
                arrow_select::interleave::interleave(&arrays, &order)
                    .map_err(|e| JobError::internal(e.to_string()))?,
            );
        }
        let batch = batch_of(self.schema.clone(), columns, rows.len())?;

        let mut out_ops = StringBuilder::with_capacity(rows.len(), rows.len() * 6);
        let mut lists = ListBuilder::new(StringBuilder::new());
        for &r in &rows {
            match passed[r] {
                Some(_) => {
                    out_ops.append_value(op(r).unwrap_or("insert"));
                    match &unchanged[r] {
                        Some(names) => {
                            for name in names {
                                lists.values().append_value(name);
                            }
                            lists.append(true);
                        }
                        None => lists.append_null(),
                    }
                }
                None => {
                    out_ops.append_value("delete");
                    lists.append_null();
                }
            }
        }
        let ids = take(&(Arc::new(typed.ids.clone()) as ArrayRef), &rows)?;
        let seq = match &typed.control.seq {
            Some(seq) => Some(take(seq, &rows)?),
            None => None,
        };
        Ok(Applied {
            typed: Typed {
                batch,
                ids: ids.as_fixed_size_binary().clone(),
                control: Control {
                    op: Some(Arc::new(out_ops.finish())),
                    seq,
                    unchanged: typed
                        .control
                        .unchanged
                        .as_ref()
                        .map(|_| Arc::new(lists.finish()) as ArrayRef),
                },
                cells: Vec::new(),
                rejected: Vec::new(),
                rows: Some(rows.iter().map(|&r| pushed[r]).collect()),
            },
            failed: failed
                .into_iter()
                .map(|(r, why)| (pushed[r], why))
                .collect(),
        })
    }

    /// What an update that didn't send `names` leaves unchanged: the output
    /// columns that depend only on such columns. An update whose filter, or
    /// one of whose output columns, depends on both kinds can't be
    /// evaluated.
    fn unchanged(&self, names: Option<ArrayRef>) -> Result<Option<Unsent>, String> {
        let Some(names) = names else {
            return Ok(None);
        };
        let reads = &self.op.reads;
        let unsent: BTreeSet<usize> = names
            .as_string::<i32>()
            .iter()
            .flatten()
            .filter_map(|n| reads.index_of(n).ok())
            .collect();
        let name = |i: usize| reads.field(i).name().clone();
        if let Some(&i) = unsent.intersection(&self.deps.filters).next() {
            return Err(self.reason(&format!(
                "the update didn't send column {}, which the statement's filter reads",
                name(i)
            )));
        }
        let mut out = Vec::new();
        for (c, deps) in self.deps.columns.iter().enumerate() {
            if deps.is_disjoint(&unsent) {
                continue;
            }
            if !deps.is_subset(&unsent) {
                let i = deps
                    .intersection(&unsent)
                    .next()
                    .copied()
                    .unwrap_or_default();
                return Err(self.reason(&format!(
                    "the update didn't send column {}, which output column {} needs with columns it did send",
                    name(i),
                    self.names[c]
                )));
            }
            out.push(self.names[c].clone());
        }
        Ok(Some(Unsent {
            reads: unsent,
            unchanged: out,
        }))
    }

    /// Why an update that didn't send `unsent`, columns the statement reads
    /// by index, can't be written where it passes the statement's filter:
    /// it may have failed it before, and then the destination doesn't have
    /// the values of the output columns that depend on them. None without a
    /// filter, or where no output column depends on them.
    fn entering(&self, unsent: &BTreeSet<usize>) -> Option<String> {
        if !self.op.steps.iter().any(|s| matches!(s, Step::Filter(_))) {
            return None;
        }
        let (c, deps) = self
            .deps
            .columns
            .iter()
            .enumerate()
            .find(|(_, d)| !d.is_disjoint(unsent))?;
        let i = deps.intersection(unsent).next()?;
        Some(self.reason(&format!(
            "the update didn't send column {}, which output column {} needs, and passes the statement's filter, which it may have failed before, so the destination may not have the column's value",
            self.op.reads.field(*i).name(),
            self.names[c]
        )))
    }

    /// Each output column for deletes of `rows`' keys: the key's columns'
    /// values, and nulls.
    fn keys(&self, batch: &RecordBatch, rows: &[usize]) -> Result<Vec<ArrayRef>, JobError> {
        let st = self.st();
        let input = &self.transform.inputs[st.input];
        let mut out = Vec::with_capacity(self.schema.fields().len());
        for f in self.schema.fields() {
            let column = match st.key.iter().position(|k| k == f.name()) {
                Some(i) => {
                    let Some(c) = input.key.get(i).and_then(|k| batch.column_by_name(k)) else {
                        return Err(JobError::internal(format!(
                            "the stream lacks key column {} of output {}",
                            f.name(),
                            st.output
                        )));
                    };
                    let c = take(c, rows)?;
                    if c.data_type() != f.data_type() {
                        return Err(JobError::internal(format!(
                            "key column {} is {} in the stream and {} in output {}",
                            f.name(),
                            c.data_type(),
                            f.data_type(),
                            st.output
                        )));
                    }
                    c
                }
                None => new_null_array(f.data_type(), rows.len()),
            };
            out.push(column);
        }
        Ok(out)
    }

    /// Runs the steps on `rows` of `input`, which didn't send `unsent`. If
    /// a batch fails, each row runs on its own, and those that fail are left
    /// out, with why.
    fn run_rows(
        &self,
        input: &RecordBatch,
        ids: &FixedSizeBinaryArray,
        rows: &[usize],
        unsent: &BTreeSet<usize>,
    ) -> Result<(Ran, Vec<(usize, String)>), JobError> {
        let (some, some_ids) = match rows.len() == input.num_rows() {
            true => (input.clone(), ids.clone()),
            false => (
                take_batch(input, rows)?,
                take(&(Arc::new(ids.clone()) as ArrayRef), rows)?
                    .as_fixed_size_binary()
                    .clone(),
            ),
        };
        let mut failed = Vec::new();
        let ok: Vec<usize> = match self.run(some.clone(), some_ids.clone(), unsent) {
            Ok(mut ran) => {
                ran.rows = ran.rows.iter().map(|&r| rows[r]).collect();
                return Ok((ran, failed));
            }
            Err(_) => (0..rows.len())
                .filter(
                    |&i| match self.run(some.slice(i, 1), some_ids.slice(i, 1), unsent) {
                        Ok(_) => true,
                        Err(e) => {
                            failed.push((rows[i], self.reason(&e.message)));
                            false
                        }
                    },
                )
                .collect(),
        };
        let kept = take_batch(&some, &ok)?;
        let kept_ids = take(&(Arc::new(some_ids) as ArrayRef), &ok)?
            .as_fixed_size_binary()
            .clone();
        let mut ran = self
            .run(kept, kept_ids, unsent)
            .map_err(|e| JobError::new(ErrorCode::DataInvalid, self.reason(&e.message)))?;
        ran.rows = ran.rows.iter().map(|&r| rows[ok[r]]).collect();
        Ok((ran, failed))
    }

    /// The steps, on every row of `batch`: filters drop rows, and `UNNEST`
    /// gives several, with IDs of their own. `unsent` are the columns the
    /// statement reads, by index, that the rows' update didn't send: a
    /// column that depends only on them is null, not evaluated, since the
    /// destination keeps its value.
    fn run(
        &self,
        batch: RecordBatch,
        ids: FixedSizeBinaryArray,
        unsent: &BTreeSet<usize>,
    ) -> Result<Ran, openetl_sql::Error> {
        let eval = |e: arrow_schema::ArrowError| openetl_sql::Error {
            kind: openetl_sql::ErrorKind::Eval,
            at: None,
            message: e.to_string(),
        };
        let mut cur = batch;
        let mut rows: Vec<usize> = (0..cur.num_rows()).collect();
        let mut ids = ids;
        for (step, deps) in self.op.steps.iter().zip(&self.deps.steps) {
            match step {
                Step::Filter(p) => {
                    let keep = p.eval_predicate(&cur)?;
                    if keep.true_count() == keep.len() {
                        continue;
                    }
                    cur = arrow_select::filter::filter_record_batch(&cur, &keep).map_err(eval)?;
                    ids = arrow_select::filter::filter(&ids, &keep)
                        .map_err(eval)?
                        .as_fixed_size_binary()
                        .clone();
                    rows = rows
                        .iter()
                        .zip(keep.values().iter())
                        .filter_map(|(&r, k)| k.then_some(r))
                        .collect();
                }
                Step::Project(p) if unsent.is_empty() => cur = p.eval(&cur)?,
                Step::Project(p) => {
                    let mut columns = Vec::with_capacity(p.columns.len());
                    for ((c, f), d) in p.columns.iter().zip(p.schema.fields()).zip(deps) {
                        columns.push(match !d.is_empty() && d.is_subset(unsent) {
                            true => new_null_array(f.data_type(), cur.num_rows()),
                            false => c.expr.eval(&cur)?,
                        });
                    }
                    cur = RecordBatch::try_new_with_options(
                        nullable(&p.schema),
                        columns,
                        &RecordBatchOptions::new().with_row_count(Some(cur.num_rows())),
                    )
                    .map_err(eval)?;
                }
                Step::Unnest(u) => {
                    let un = u.eval(&cur)?;
                    ids = element_ids(&ids, &un.rows, &un.ordinals).map_err(eval)?;
                    rows = un.rows.iter().map(|&r| rows[r]).collect();
                    cur = un.batch;
                }
            }
        }
        Ok(Ran {
            batch: cur,
            rows,
            ids,
        })
    }

    /// The steps' last batch under the output's schema.
    fn output(&self, batch: &RecordBatch) -> Result<RecordBatch, JobError> {
        if batch.num_columns() != self.schema.fields().len() {
            return Err(JobError::internal(format!(
                "the statement gave {} columns, and output {} has {}",
                batch.num_columns(),
                self.st().output,
                self.schema.fields().len()
            )));
        }
        batch_of(
            self.schema.clone(),
            batch.columns().to_vec(),
            batch.num_rows(),
        )
    }
}

fn nullable(schema: &Schema) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

fn take(a: &ArrayRef, rows: &[usize]) -> Result<ArrayRef, JobError> {
    let indices = UInt64Array::from_iter_values(rows.iter().map(|&r| r as u64));
    arrow_select::take::take(a.as_ref(), &indices, None)
        .map_err(|e| JobError::internal(e.to_string()))
}

fn take_batch(batch: &RecordBatch, rows: &[usize]) -> Result<RecordBatch, JobError> {
    let indices = UInt64Array::from_iter_values(rows.iter().map(|&r| r as u64));
    arrow_select::take::take_record_batch(batch, &indices)
        .map_err(|e| JobError::internal(e.to_string()))
}

fn batch_of(
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
    rows: usize,
) -> Result<RecordBatch, JobError> {
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(|e| JobError::internal(e.to_string()))
}

/// Each of a typed batch's rows' row among those it was typed from.
pub fn pushed_rows(typed: &Typed) -> Vec<usize> {
    if let Some(rows) = &typed.rows {
        return rows.clone();
    }
    let gone: BTreeSet<usize> = typed.rejected.iter().map(|r| r.row).collect();
    (0..)
        .filter(|r| !gone.contains(r))
        .take(typed.batch.num_rows())
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use arrow_array::types::Int64Type;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::DataType;

    use crate::transform::Input;

    fn input(changelog: bool) -> Input {
        Input {
            name: "a".into(),
            schema: Some(Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("email", DataType::Utf8, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("at", DataType::Int64, true),
            ]))),
            key: vec!["id".into()],
            changelog,
        }
    }

    fn stage(sql: &str, changelog: bool) -> Stage {
        let t = Arc::new(Transform::plan(sql, vec![input(changelog)]).unwrap());
        Stage::statement(&t, 0)
    }

    type Row<'a> = (i64, Option<&'a str>, Option<&'a str>);

    fn batch(rows: &[Row]) -> RecordBatch {
        let ids: Int64Array = rows.iter().map(|r| Some(r.0)).collect();
        let emails: StringArray = rows.iter().map(|r| r.1).collect();
        let names: StringArray = rows.iter().map(|r| r.2).collect();
        let at: Int64Array = rows.iter().map(|r| Some(r.0 * 10)).collect();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, true),
                Field::new("id", DataType::Int64, true),
                Field::new("email", DataType::Utf8, true),
                Field::new("at", DataType::Int64, true),
            ])),
            vec![
                Arc::new(names) as ArrayRef,
                Arc::new(ids),
                Arc::new(emails),
                Arc::new(at),
            ],
        )
        .unwrap()
    }

    fn record_ids(n: usize) -> FixedSizeBinaryArray {
        FixedSizeBinaryArray::try_from_iter((0..n).map(|i| {
            let mut id = [0u8; 16];
            id[..8].copy_from_slice(&u64::try_from(i).unwrap().to_le_bytes());
            id
        }))
        .unwrap()
    }

    fn typed(batch: RecordBatch, control: Control) -> Typed {
        let n = batch.num_rows();
        Typed {
            batch,
            ids: record_ids(n),
            control,
            cells: Vec::new(),
            rejected: Vec::new(),
            rows: None,
        }
    }

    /// A changelog's control columns: each row's op, its seq, and the
    /// columns an update didn't send.
    fn control(ops: &[&str], unsent: &[Option<&[&str]>]) -> Control {
        let mut lists = ListBuilder::new(StringBuilder::new());
        for u in unsent {
            match u {
                Some(names) => {
                    for n in *names {
                        lists.values().append_value(n);
                    }
                    lists.append(true);
                }
                None => lists.append_null(),
            }
        }
        Control {
            op: Some(Arc::new(StringArray::from(ops.to_vec()))),
            seq: Some(Arc::new(Int64Array::from_iter_values(
                (0..ops.len()).map(|i| i64::try_from(i).unwrap()),
            ))),
            unchanged: Some(Arc::new(lists.finish())),
        }
    }

    fn ints(b: &RecordBatch, column: &str) -> Vec<Option<i64>> {
        b.column_by_name(column)
            .unwrap()
            .as_primitive::<Int64Type>()
            .iter()
            .collect()
    }

    fn strs(a: &ArrayRef) -> Vec<Option<String>> {
        a.as_string::<i32>()
            .iter()
            .map(|v| v.map(str::to_owned))
            .collect()
    }

    fn some(v: &[&str]) -> Vec<Option<String>> {
        v.iter().map(|s| Some(s.to_string())).collect()
    }

    fn names(b: &RecordBatch) -> Vec<String> {
        b.schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    fn id(ids: &FixedSizeBinaryArray, i: usize) -> Vec<u8> {
        ids.iter().nth(i).flatten().unwrap().to_vec()
    }

    #[test]
    fn a_statement_projects_and_filters_keeping_ids_rows_and_sequences() {
        let mut st = stage(
            "INSERT INTO c SELECT id AS user_id, lower(email) AS email FROM a WHERE id > 1",
            false,
        );
        let t = typed(
            batch(&[
                (1, Some("A@x"), None),
                (2, Some("B@x"), None),
                (3, None, None),
            ]),
            Control::default(),
        );
        let out = st.apply(&t, &[10, 11, 12], Some("at")).unwrap();
        assert!(out.failed.is_empty());
        let b = &out.typed.batch;
        assert_eq!(names(b), ["user_id", "email"]);
        assert_eq!(ints(b, "user_id"), [Some(2), Some(3)]);
        assert_eq!(strs(b.column(1)), [Some("b@x".into()), None]);
        assert_eq!(out.typed.rows, Some(vec![11, 12]));
        assert_eq!(id(&out.typed.ids, 0), id(&t.ids, 1));
        assert_eq!(id(&out.typed.ids, 1), id(&t.ids, 2));
        let seq = out.typed.control.seq.unwrap();
        assert_eq!(seq.as_primitive::<Int64Type>().values(), &[20, 30]);
        assert!(out.typed.control.op.is_none());
    }

    #[test]
    fn identity_passes_the_batch_as_it_came() {
        let t = typed(batch(&[(1, Some("a"), None)]), Control::default());
        let out = Stage::Identity.apply(&t, &[4], Some("at")).unwrap();
        assert_eq!(out.typed.batch, t.batch);
        assert_eq!(out.typed.rows, Some(vec![4]));
        assert!(out.typed.control.seq.is_none());
    }

    #[test]
    fn unnest_gives_each_row_an_id_of_its_own_every_time() {
        let mut st = stage(
            "INSERT INTO parts SELECT id, unnest(string_to_array(email, ',')) AS part FROM a",
            false,
        );
        let t = typed(
            batch(&[
                (1, Some("x,y"), None),
                (2, None, None),
                (3, Some("z"), None),
            ]),
            Control::default(),
        );
        let out = st.apply(&t, &[0, 1, 2], None).unwrap();
        let b = &out.typed.batch;
        assert_eq!(ints(b, "id"), [Some(1), Some(1), Some(3)]);
        assert_eq!(strs(b.column(1)), some(&["x", "y", "z"]));
        assert_eq!(out.typed.rows, Some(vec![0, 0, 2]));
        let ids: Vec<&[u8]> = out.typed.ids.iter().flatten().collect();
        assert!(ids[0] != ids[1] && ids[0] != id(&t.ids, 0) && ids[2] != id(&t.ids, 2));
        let again = st.apply(&t, &[0, 1, 2], None).unwrap();
        assert_eq!(again.typed.ids, out.typed.ids);
    }

    #[test]
    fn on_a_changelog_deletes_pass_and_rows_the_filter_drops_become_deletes() {
        let mut st = stage(
            "INSERT INTO a SELECT id AS key, upper(email) AS email FROM a WHERE email <> 'void'",
            true,
        );
        let t = typed(
            batch(&[
                (1, Some("new"), None),
                (2, Some("void"), None),
                (3, None, None),
                (4, Some("x"), None),
            ]),
            control(&["insert", "update", "delete", "update"], &[None; 4]),
        );
        let out = st.apply(&t, &[0, 1, 2, 3], None).unwrap();
        assert!(out.failed.is_empty());
        let b = &out.typed.batch;
        assert_eq!(ints(b, "key"), [Some(1), Some(2), Some(3), Some(4)]);
        assert_eq!(
            strs(b.column(1)),
            [Some("NEW".into()), None, None, Some("X".into())]
        );
        assert_eq!(
            strs(out.typed.control.op.as_ref().unwrap()),
            some(&["insert", "delete", "delete", "update"])
        );
        assert_eq!(out.typed.ids, t.ids);
        let seq = out.typed.control.seq.unwrap();
        assert_eq!(seq.as_primitive::<Int64Type>().values(), &[0, 1, 2, 3]);
    }

    #[test]
    fn an_update_leaves_unchanged_what_depends_only_on_columns_it_didnt_send() {
        let mut st = stage(
            "INSERT INTO a SELECT id, email, lower(email) AS low, name FROM a",
            true,
        );
        let t = typed(
            batch(&[(1, None, Some("n")), (2, Some("E"), Some("m"))]),
            control(&["update", "update"], &[Some(&["email"]), None]),
        );
        let out = st.apply(&t, &[0, 1], None).unwrap();
        assert!(out.failed.is_empty());
        let unchanged = out.typed.control.unchanged.unwrap();
        let lists = unchanged.as_list::<i32>();
        assert_eq!(
            strs(&lists.iter().next().flatten().unwrap()),
            some(&["email", "low"])
        );
        assert!(lists.is_null(1));
    }

    #[test]
    fn an_update_that_didnt_send_what_a_filter_or_a_mixed_column_reads_fails() {
        let t = typed(
            batch(&[(1, None, Some("n")), (2, Some("E"), Some("m"))]),
            control(&["update", "insert"], &[Some(&["email"]), None]),
        );
        let mut st = stage(
            "INSERT INTO a SELECT id, email || name AS label FROM a",
            true,
        );
        let out = st.apply(&t, &[5, 6], None).unwrap();
        assert_eq!(out.failed.len(), 1);
        assert_eq!(out.failed[0].0, 5);
        assert!(
            out.failed[0].1.contains("output column label"),
            "{}",
            out.failed[0].1
        );
        assert_eq!(ints(&out.typed.batch, "id"), [Some(2)]);
        assert_eq!(out.typed.rows, Some(vec![6]));

        let mut st = stage(
            "INSERT INTO a SELECT id, name FROM a WHERE email IS NOT NULL",
            true,
        );
        let out = st.apply(&t, &[5, 6], None).unwrap();
        assert!(
            out.failed[0].1.contains("filter reads"),
            "{}",
            out.failed[0].1
        );
    }

    #[test]
    fn rows_a_statement_cant_evaluate_fail_and_the_rest_pass() {
        let mut st = stage(
            "INSERT INTO c SELECT id, CAST(email AS BIGINT) AS n FROM a",
            false,
        );
        let t = typed(
            batch(&[
                (1, Some("12"), None),
                (2, Some("x"), None),
                (3, Some("7"), None),
            ]),
            Control::default(),
        );
        let out = st.apply(&t, &[0, 1, 2], None).unwrap();
        assert_eq!(out.failed.len(), 1);
        assert_eq!(out.failed[0].0, 1);
        assert!(
            out.failed[0]
                .1
                .starts_with("transform.sql 1:1, writing c: "),
            "{}",
            out.failed[0].1
        );
        assert_eq!(ints(&out.typed.batch, "n"), [Some(12), Some(7)]);
        assert_eq!(out.typed.rows, Some(vec![0, 2]));
        assert_eq!(id(&out.typed.ids, 1), id(&t.ids, 2));
    }

    #[test]
    fn a_new_column_reaches_star_and_a_type_that_doesnt_plan_stops_the_job() {
        let mut st = stage("INSERT INTO c SELECT * FROM a", false);
        let b = batch(&[(1, Some("e"), None)]);
        let mut fields: Vec<Field> = b
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new("extra", DataType::Utf8, true));
        let mut columns = b.columns().to_vec();
        columns.push(Arc::new(StringArray::from(vec!["x"])));
        let wider = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let out = st
            .apply(&typed(wider, Control::default()), &[0], None)
            .unwrap();
        assert_eq!(
            names(&out.typed.batch),
            ["id", "email", "name", "at", "extra"]
        );

        let mut st = stage("INSERT INTO c SELECT id, at + 1 AS next FROM a", false);
        let bool_at = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("at", DataType::Boolean, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(arrow_array::BooleanArray::from(vec![true])),
            ],
        )
        .unwrap();
        let e = st
            .apply(&typed(bool_at, Control::default()), &[0], None)
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::SchemaDrift, "{}", e.message);
    }

    /// A column declared not null is null where a batch lacks it.
    #[test]
    fn a_column_declared_not_null_that_the_batch_lacks_is_null() {
        let mut declared = input(false);
        declared.schema = Some(Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, false),
        ])));
        let t = Arc::new(
            Transform::plan(
                "INSERT INTO c SELECT id, email AS address FROM a",
                vec![declared],
            )
            .unwrap(),
        );
        let mut st = Stage::statement(&t, 0);
        let b = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )
        .unwrap();
        let out = st
            .apply(&typed(b, Control::default()), &[0, 1], None)
            .unwrap();
        assert!(out.failed.is_empty(), "{:?}", out.failed);
        assert_eq!(out.typed.batch.column(1).null_count(), 2);
    }

    /// An output column that depends only on columns an update didn't
    /// send isn't evaluated: the destination keeps its value. Such updates
    /// keep their place among the batch's other rows.
    #[test]
    fn an_update_doesnt_evaluate_what_it_leaves_unchanged() {
        let mut st = stage(
            "INSERT INTO a SELECT id, CAST(coalesce(email, 'x') AS BIGINT) AS n, name FROM a",
            true,
        );
        let t = typed(
            batch(&[
                (1, None, Some("n1")),
                (2, Some("12"), Some("n2")),
                (3, None, Some("n3")),
                (4, None, None),
            ]),
            control(
                &["update", "update", "update", "delete"],
                &[Some(&["email"]), None, Some(&["email"]), None],
            ),
        );
        let out = st.apply(&t, &[0, 1, 2, 3], None).unwrap();
        assert!(out.failed.is_empty(), "{:?}", out.failed);
        let b = &out.typed.batch;
        assert_eq!(ints(b, "id"), [Some(1), Some(2), Some(3), Some(4)]);
        assert_eq!(ints(b, "n"), [None, Some(12), None, None]);
        assert_eq!(
            strs(b.column(2)),
            [
                Some("n1".into()),
                Some("n2".into()),
                Some("n3".into()),
                None
            ]
        );
        assert_eq!(
            strs(out.typed.control.op.as_ref().unwrap()),
            some(&["update", "update", "update", "delete"])
        );
        assert_eq!(out.typed.ids, t.ids);
        let unchanged = out.typed.control.unchanged.unwrap();
        let lists: Vec<Option<ArrayRef>> = unchanged.as_list::<i32>().iter().collect();
        assert_eq!(strs(lists[0].as_ref().unwrap()), some(&["n"]));
        assert!(lists[1].is_none());
        assert_eq!(strs(lists[2].as_ref().unwrap()), some(&["n"]));
        assert!(lists[3].is_none());
    }

    /// An update that didn't send a column an output column needs can't be
    /// written where it passes a filter: it may have failed it before, and
    /// left the destination without the column's value. Where it fails the
    /// filter, it's a delete of its key, as any row that fails it is.
    #[test]
    fn an_update_that_didnt_send_what_an_output_needs_cant_enter_a_filter() {
        let mut st = stage(
            "INSERT INTO a SELECT id, name FROM a WHERE email = 'active'",
            true,
        );
        let t = typed(
            batch(&[
                (1, Some("active"), None),
                (2, Some("inactive"), None),
                (3, Some("active"), Some("n3")),
                (4, Some("active"), Some("n4")),
            ]),
            control(
                &["update", "update", "update", "update"],
                &[Some(&["name"]), Some(&["name"]), None, Some(&["at"])],
            ),
        );
        let out = st.apply(&t, &[10, 11, 12, 13], None).unwrap();
        assert_eq!(out.failed.len(), 1, "{:?}", out.failed);
        assert_eq!(out.failed[0].0, 10);
        assert!(
            out.failed[0].1.contains("passes the statement's filter"),
            "{}",
            out.failed[0].1
        );
        let b = &out.typed.batch;
        assert_eq!(ints(b, "id"), [Some(2), Some(3), Some(4)]);
        assert_eq!(
            strs(b.column(1)),
            [None, Some("n3".into()), Some("n4".into())]
        );
        assert_eq!(
            strs(out.typed.control.op.as_ref().unwrap()),
            some(&["delete", "update", "update"])
        );
        assert_eq!(out.typed.rows, Some(vec![11, 12, 13]));

        let mut st = stage("INSERT INTO a SELECT id, name FROM a", true);
        let out = st.apply(&t, &[10, 11, 12, 13], None).unwrap();
        assert!(out.failed.is_empty(), "{:?}", out.failed);
    }

    #[test]
    fn a_column_the_batch_lacks_is_null() {
        let mut st = stage("INSERT INTO c SELECT id, name FROM a", false);
        let b = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )
        .unwrap();
        let out = st
            .apply(&typed(b, Control::default()), &[0, 1], None)
            .unwrap();
        assert_eq!(out.typed.batch.column(1).null_count(), 2);
    }

    /// The operator's rows per second on one core, for a projection with
    /// renames and an expression and a filter that keeps half the rows, on
    /// 20-column batches of 8192 rows. Prints the median and best of its
    /// runs. A benchmark, so it runs only when asked:
    ///
    /// cargo test --release -p openetl-engine -- --ignored --nocapture rows_per_second
    #[test]
    #[ignore = "a benchmark"]
    fn rows_per_second_through_a_projection_and_filter() {
        use arrow_array::{BooleanArray, Float64Array, TimestampMicrosecondArray};
        const ROWS: usize = 8192;
        const BATCHES: usize = 244;
        const RUNS: usize = 9;
        let mut fields = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();
        let n = || 0..i64::try_from(ROWS).unwrap();
        for c in 0..20 {
            let name = format!("c{c}");
            match c {
                0..=9 => {
                    fields.push(Field::new(name, DataType::Int64, true));
                    columns.push(Arc::new(Int64Array::from_iter_values(
                        n().map(|i| i * 7 + c),
                    )));
                }
                10..=15 => {
                    fields.push(Field::new(name, DataType::Utf8, true));
                    columns.push(Arc::new(StringArray::from_iter_values(
                        n().map(|i| format!("value {i} of {c}")),
                    )));
                }
                16 | 17 => {
                    fields.push(Field::new(name, DataType::Float64, true));
                    columns.push(Arc::new(Float64Array::from_iter_values(
                        n().map(|i| i as f64 * 0.5),
                    )));
                }
                18 => {
                    fields.push(Field::new(name, DataType::Boolean, true));
                    columns.push(Arc::new(BooleanArray::from_iter(
                        n().map(|i| Some(i % 3 != 0)),
                    )));
                }
                _ => {
                    let ts = DataType::Timestamp(
                        arrow_schema::TimeUnit::Microsecond,
                        Some("UTC".into()),
                    );
                    fields.push(Field::new(name, ts, true));
                    columns.push(Arc::new(
                        TimestampMicrosecondArray::from_iter_values(n().map(|i| i * 1_000_000))
                            .with_timezone("UTC"),
                    ));
                }
            }
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let t = Arc::new(
            Transform::plan(
                "INSERT INTO out SELECT c0 AS id, c1, c2, c3 + 1 AS c3, c4, c5, c6, c7, c8, c9, \
                 c10 AS label, c11, c12, c13, c14, c15, c16, c17, c18, c19 AS at \
                 FROM t WHERE c1 % 2 = 0",
                vec![Input {
                    name: "t".into(),
                    schema: Some(schema),
                    key: vec!["c0".into()],
                    changelog: false,
                }],
            )
            .unwrap(),
        );
        let mut st = Stage::statement(&t, 0);
        let typed = typed(batch, Control::default());
        let pushed: Vec<usize> = (0..ROWS).collect();
        let mut times = Vec::with_capacity(RUNS);
        let mut kept = 0;
        for _ in 0..RUNS {
            let start = std::time::Instant::now();
            kept = 0;
            for _ in 0..BATCHES {
                let out = st.apply(&typed, &pushed, Some("c19")).unwrap();
                kept += std::hint::black_box(out).typed.batch.num_rows();
            }
            times.push(start.elapsed());
        }
        times.sort();
        let rows = (ROWS * BATCHES) as f64;
        println!(
            "{} rows in, {kept} out per run: median {:.1}M rows/s, best {:.1}M rows/s",
            ROWS * BATCHES,
            rows / times[RUNS / 2].as_secs_f64() / 1e6,
            rows / times[0].as_secs_f64() / 1e6,
        );
    }
}
