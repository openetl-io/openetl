//! Row-wise steps on a batch, keeping each row's place: those before a
//! window, which give the rows it groups, and those after, HAVING and the
//! projection to the output.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, RecordBatch, UInt64Array};
use openetl_normalize::{Cause, Code};
use openetl_proto::connector::v1::ErrorCode;
use openetl_sql::{Error, Expr, Step};

use super::Out;
use crate::error::JobError;

/// What the steps gave: the batch, and each of its rows' row of the input.
pub struct Ran {
    pub batch: RecordBatch,
    pub rows: Vec<usize>,
}

/// Runs `steps` on `batch`. Filters drop rows, unless not `filter`, and
/// `UNNEST` gives several.
pub fn run(steps: &[Step], batch: RecordBatch, filter: bool) -> Result<Ran, Error> {
    let mut rows: Vec<usize> = (0..batch.num_rows()).collect();
    let mut cur = batch;
    for step in steps {
        match step {
            Step::Filter(_) if !filter => {}
            Step::Filter(p) => {
                let keep = p.eval_predicate(&cur)?;
                if keep.true_count() == keep.len() {
                    continue;
                }
                cur = arrow_select::filter::filter_record_batch(&cur, &keep).map_err(eval)?;
                rows = rows
                    .iter()
                    .zip(keep.values().iter())
                    .filter(|(_, k)| *k)
                    .map(|(r, _)| *r)
                    .collect();
            }
            Step::Project(p) => cur = p.eval(&cur)?,
            Step::Unnest(u) => {
                let out = u.eval(&cur)?;
                rows = out.rows.iter().map(|&r| rows[r]).collect();
                cur = out.batch;
            }
        }
    }
    Ok(Ran { batch: cur, rows })
}

/// As [`run`], and where the batch fails, a row at a time: the rows that
/// fail, by their row of the input, with why, and what the rest give.
pub fn run_rows(
    steps: &[Step],
    batch: RecordBatch,
    filter: bool,
) -> Result<(Ran, Vec<(usize, String)>), Error> {
    match run(steps, batch.clone(), filter) {
        Ok(ran) => return Ok((ran, Vec::new())),
        Err(e) if e.kind != openetl_sql::ErrorKind::Eval => return Err(e),
        Err(_) => {}
    }
    let mut failed = Vec::new();
    let mut ok = Vec::new();
    for i in 0..batch.num_rows() {
        match run(steps, batch.slice(i, 1), filter) {
            Ok(_) => ok.push(i),
            Err(e) => failed.push((i, e.message)),
        }
    }
    let kept = take(&batch, &ok)?;
    let mut ran = run(steps, kept, filter)?;
    ran.rows = ran.rows.iter().map(|&r| ok[r]).collect();
    Ok((ran, failed))
}

/// The output's rows that delete, by their key, the rows `rows` gave or
/// would give: the steps' projections without their filters. A column
/// that gives the same value for the same input gives it again; one that
/// may not, as one that reads `now()`, isn't evaluated, so it's null, or
/// its type's zero where it can't be.
pub fn deletes(steps: &[Step], rows: RecordBatch) -> Result<Ran, String> {
    let n = rows.num_rows();
    let mut cur = rows;
    for step in steps {
        match step {
            Step::Filter(_) => {}
            Step::Project(p) => {
                let mut columns = Vec::with_capacity(p.columns.len());
                for (c, f) in p.columns.iter().zip(p.schema.fields()) {
                    columns.push(match c.expr.is_deterministic() {
                        true => c.expr.eval(&cur).map_err(|e| e.message)?,
                        false => empty(f, n)?,
                    });
                }
                cur = RecordBatch::try_new_with_options(
                    p.schema.clone(),
                    columns,
                    &arrow_array::RecordBatchOptions::new().with_row_count(Some(n)),
                )
                .map_err(|e| e.to_string())?;
            }
            Step::Unnest(_) => return Err("an output that unnests".into()),
        }
    }
    Ok(Ran {
        batch: cur,
        rows: (0..n).collect(),
    })
}

/// `n` values of `f` for a column whose value doesn't matter: nulls, or
/// where it can't be null, its type's zero.
pub fn empty(f: &arrow_schema::Field, n: usize) -> Result<ArrayRef, String> {
    let nulls = arrow_array::new_null_array(f.data_type(), n);
    if f.is_nullable() {
        return Ok(nulls);
    }
    let zeros = nulls
        .to_data()
        .into_builder()
        .nulls(None)
        .build()
        .map_err(|e| e.to_string())?;
    Ok(arrow_array::make_array(zeros))
}

fn eval(e: arrow_schema::ArrowError) -> Error {
    Error {
        kind: openetl_sql::ErrorKind::Eval,
        at: None,
        message: e.to_string(),
    }
}

/// Runs a statement's steps before its stateful operator on `input`: what
/// they gave, and what to give for the rows they fail on, rejected with
/// `reason` of why.
pub fn before(
    steps: &[Step],
    input: &RecordBatch,
    reason: impl Fn(&str) -> String,
) -> Result<(Ran, Out), JobError> {
    let (ran, failed) =
        run_rows(steps, input.clone(), true).map_err(|e| JobError::internal(reason(&e.message)))?;
    let mut out = Out::default();
    for (r, why) in failed {
        out.rejected
            .push((r, reason(&why), Cause::new(Code::Statement)));
    }
    Ok((ran, out))
}

/// Each aggregate's argument and FILTER on some rows, as columns of all of
/// them, and the rows one fails on, with why.
pub type Arguments = (
    Vec<Option<ArrayRef>>,
    Vec<Option<BooleanArray>>,
    BTreeMap<usize, String>,
);

/// Each of `aggregates`' FILTER on `rows`, then its argument on the rows
/// its FILTER admits, each aggregate its argument and FILTER: a row an
/// aggregate's FILTER leaves out never evaluates its argument, which is
/// null there. Where one fails on the batch, it's evaluated row by row, and
/// a row it fails on is given with `reason` of why: no aggregate takes it.
pub fn arguments<'a>(
    rows: &RecordBatch,
    aggregates: impl Iterator<Item = (Option<&'a Expr>, Option<&'a Expr>)>,
    reason: &dyn Fn(&str) -> String,
) -> Result<Arguments, JobError> {
    let n = rows.num_rows();
    let all: Vec<usize> = (0..n).collect();
    let mut bad = BTreeMap::new();
    let (mut args, mut filters) = (Vec::new(), Vec::new());
    for (arg, filter) in aggregates {
        let filter = match filter {
            None => None,
            Some(e) => {
                let got = each(
                    rows,
                    &all,
                    |b| e.eval_predicate(b).map(|p| Arc::new(p) as ArrayRef),
                    &mut bad,
                    reason,
                )?;
                Some(arrow_array::cast::AsArray::as_boolean(got.as_ref()).clone())
            }
        };
        let arg = match arg {
            None => None,
            Some(e) => {
                let admitted: Vec<usize> = (0..n)
                    .filter(|&r| super::accumulate::passes(filter.as_ref(), r))
                    .collect();
                Some(each(rows, &admitted, |b| e.eval(b), &mut bad, reason)?)
            }
        };
        filters.push(filter);
        args.push(arg);
    }
    Ok((args, filters, bad))
}

/// `f` on the rows `at` of `rows`, as a column of all of them: null at
/// the others, and at a row it fails on, which `bad` gets with `reason` of
/// why.
fn each(
    rows: &RecordBatch,
    at: &[usize],
    f: impl Fn(&RecordBatch) -> Result<ArrayRef, Error>,
    bad: &mut BTreeMap<usize, String>,
    reason: &dyn Fn(&str) -> String,
) -> Result<ArrayRef, JobError> {
    let internal = |e: Error| JobError::internal(e.message);
    let some = take(rows, at).map_err(internal)?;
    let (got, ok) = match f(&some) {
        Ok(a) => (a, at.to_vec()),
        Err(_) => {
            let mut ok = Vec::with_capacity(at.len());
            for (i, &r) in at.iter().enumerate() {
                match f(&some.slice(i, 1)) {
                    Ok(_) => ok.push(r),
                    Err(e) => {
                        bad.entry(r).or_insert_with(|| reason(&e.message));
                    }
                }
            }
            let kept = take(rows, &ok).map_err(internal)?;
            let got = f(&kept).map_err(|e| {
                JobError::new(
                    ErrorCode::DataInvalid,
                    reason(&format!(
                        "rows that it evaluates one at a time fail together ({})",
                        e.kind.code()
                    )),
                )
            })?;
            (got, ok)
        }
    };
    let mut index = vec![None; rows.num_rows()];
    for (i, &r) in ok.iter().enumerate() {
        index[r] = Some(i as u64);
    }
    arrow_select::take::take(got.as_ref(), &UInt64Array::from(index), None)
        .map_err(|e| JobError::internal(e.to_string()))
}

/// The rows of `batch` at `rows`, in that order.
/// Rows `rows` of one column.
pub fn take_column(column: &ArrayRef, rows: &[usize]) -> Result<ArrayRef, Error> {
    let indices = UInt64Array::from_iter_values(rows.iter().map(|&r| r as u64));
    arrow_select::take::take(column.as_ref(), &indices, None).map_err(eval)
}

pub fn take(batch: &RecordBatch, rows: &[usize]) -> Result<RecordBatch, Error> {
    if rows.len() == batch.num_rows() && rows.iter().enumerate().all(|(i, &r)| i == r) {
        return Ok(batch.clone());
    }
    let indices = UInt64Array::from_iter_values(rows.iter().map(|&r| r as u64));
    arrow_select::take::take_record_batch(batch, &indices).map_err(eval)
}
