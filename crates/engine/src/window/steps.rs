//! Row-wise steps on a batch, keeping each row's place: those before a
//! window, which give the rows it groups, and those after, HAVING and the
//! projection to the output.

use arrow_array::{RecordBatch, UInt64Array};
use openetl_sql::{Error, Step};

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

fn eval(e: arrow_schema::ArrowError) -> Error {
    Error {
        kind: openetl_sql::ErrorKind::Eval,
        at: None,
        message: e.to_string(),
    }
}

/// The rows of `batch` at `rows`, in that order.
pub fn take(batch: &RecordBatch, rows: &[usize]) -> Result<RecordBatch, Error> {
    if rows.len() == batch.num_rows() && rows.iter().enumerate().all(|(i, &r)| i == r) {
        return Ok(batch.clone());
    }
    let indices = UInt64Array::from_iter_values(rows.iter().map(|&r| r as u64));
    arrow_select::take::take_record_batch(batch, &indices).map_err(eval)
}
