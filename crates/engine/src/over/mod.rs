//! The `OVER` operator: aggregates, `lag` and `first_value` over a frame of
//! each key's rows that ends at the current row, in event-time order, with
//! the key's frame and its waiting rows in state through [`StateBackend`].
//!
//! Its input is the statement's: the columns it reads of the stream's
//! rows, each with its record ID, which run the row-wise steps before the
//! operator. A row waits in state under its key, `PARTITION BY`'s values,
//! at its event time. It leaves once the watermark passes its time, which
//! [`Operator::advance`] moves: `t`, in microseconds, means every row
//! whose event time is before `t` has come, so a row at `t` or after waits,
//! and a row before it that comes now is late, and quarantined. A row
//! whose event time is null is quarantined too. `i64::MAX` is the end of
//! input: every row leaves.
//!
//! A key's rows leave in event-time order, and those of one time, its
//! peers, in the order of their record IDs, then of their values: what
//! each row's frame holds depends on the rows alone, never on how they
//! came, so a restore or a redo gives each row the values it had. A
//! function's value goes beside the row's own, the steps after the
//! operator run, and the row leaves once, with its record ID and its event
//! time as its sequence. State keeps each key's frame, as [`frame`] says,
//! and the rows waiting, as [`row`] writes them.
//!
//! Where it drops repeats, a row that comes again while it waits, as a
//! read gives rows again, of one record ID and one hash of its stream's
//! row, doesn't wait twice: each waiting row's are in a set under a second
//! operator ID, to expire as the row leaves. One that comes after it left
//! is late.

mod canonical;
mod frame;
pub(crate) mod partitioned;
pub(crate) mod row;
#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    BooleanArray, FixedSizeBinaryArray, RecordBatch, RecordBatchOptions, UInt32Array, UInt64Array,
};
use bytes::Bytes;
use openetl_normalize::{Cause, Code, render_row};
use openetl_sql::{OverKind, Step};
use openetl_state::{DueRow, KeyRef, OperatorId, StateBackend};

use self::frame::{Func, Got, Kept, Peer};
use self::partitioned::{Partitioned, RELEASE};
use crate::error::JobError;
use crate::window::codec::{self, Value, take};
use crate::window::{Out, steps};

/// The layout of a key's frame, which the operator's ID covers.
const LAYOUT: u8 = openetl_sql::OVER_STATE;

/// What a value in the operator's state is: a key's frame. The schemas its
/// rows are written under are kept beside them ([`Partitioned`]).
const FRAME: u8 = 1;

/// `OVER` of a statement, on one lane's key groups.
pub struct Operator {
    pub id: OperatorId,
    /// What its waiting rows are as repeats go, where it drops them, each
    /// to expire at the row's time: its horizon is the operator's.
    waiting: OperatorId,
    plan: Arc<openetl_sql::Over>,
    base: Partitioned,
    funcs: Vec<Func>,
    /// Rows are due that it hasn't given yet.
    due: bool,
    /// It drops repeats: its rows' record IDs tell a repeat from a new row.
    pub drops_repeats: bool,
    /// The rows it dropped as repeats since its caller last took them.
    pub repeats: u64,
}

impl Operator {
    /// `id` is the statement's operator's, `at` where it is, and
    /// `key_groups` the pipeline's.
    pub fn new(
        id: &str,
        at: openetl_sql::Position,
        plan: Arc<openetl_sql::Over>,
        output: &str,
        key_groups: u32,
    ) -> Result<Self, JobError> {
        let base = Partitioned::new(
            id, at, output, &plan.rows, &plan.keys, plan.time, key_groups,
        )?;
        Ok(Self {
            id: base.id,
            waiting: OperatorId(base.id.0 ^ 1),
            funcs: plan.functions.iter().map(Func::of).collect(),
            base,
            due: false,
            drops_repeats: false,
            repeats: 0,
            plan,
        })
    }

    /// Its watermark in microseconds, if it has advanced.
    pub fn watermark_micros(&self) -> Option<i64> {
        self.base.watermark_micros()
    }

    /// The watermark `wm` as rows tell it apart: whether a row is late
    /// depends on every microsecond of it.
    pub fn level(&self, wm: i64) -> i64 {
        wm
    }

    fn reason(&self, why: &str) -> String {
        self.base.reason(why)
    }

    /// Takes a batch of the statement's input, the columns it reads, with
    /// each row's record ID, and where it drops repeats, each row's hash:
    /// each row waits for the watermark, or is rejected.
    pub async fn rows<S: StateBackend>(
        &mut self,
        state: &mut S,
        input: &RecordBatch,
        ids: &FixedSizeBinaryArray,
        hashes: Option<&UInt64Array>,
    ) -> Result<Out, JobError> {
        let plan = self.plan.clone();
        let (taken, mut out) = self.base.take(&plan.input.steps, input, ids)?;
        let mut placed: Vec<(usize, i64)> = (0..taken.rows.num_rows())
            .filter_map(|r| Some((r, self.base.placed(state, self.id, &taken, r, &mut out)?)))
            .collect();
        if self.drops_repeats {
            let Some(hashes) = hashes else {
                return Err(JobError::internal(self.reason("rows without their hashes")));
            };
            let hashes = hashes.values();
            let seen: Vec<crate::window::Seen> = placed
                .iter()
                .map(|&(r, _)| {
                    let h = hashes.get(taken.at[r]).copied().unwrap_or_default();
                    crate::window::seen(&taken.ids[r], h)
                })
                .collect();
            let refs: Vec<KeyRef<'_>> = placed
                .iter()
                .zip(&seen)
                .map(|(&(r, _), s)| KeyRef::new(taken.groups[r], s.as_slice()))
                .collect();
            let expires: Vec<i64> = placed.iter().map(|&(_, t)| t).collect();
            let fresh = state
                .insert_new(self.waiting, &refs, &vec![i64::MIN; refs.len()], &expires)
                .await
                .map_err(|e| state_error(&e))?;
            self.repeats += fresh.iter().filter(|new| !**new).count() as u64;
            let mut new = fresh.into_iter();
            placed.retain(|_| new.next().unwrap_or(true));
        }
        for (r, t) in placed {
            let row = self.base.encode(state, &taken, r)?;
            state.append(
                self.id,
                KeyRef::new(taken.groups[r], &taken.keys[r]),
                t,
                Bytes::from(row),
            );
        }
        out.rejected.sort_by_key(|(r, ..)| *r);
        Ok(out)
    }

    /// Moves the watermark to `t`, in microseconds, if it's later, and
    /// gives the rows due: those before it, each key's in order. It gives
    /// at most about [`RELEASE`] rows a call, and says whether more are due,
    /// for another call with the same `t`. Once every row due has left, it
    /// moves state's horizon to the latest time they left at.
    pub async fn advance<S: StateBackend>(
        &mut self,
        state: &mut S,
        t: i64,
    ) -> Result<(Out, bool), JobError> {
        self.due |= self.base.advance(t);
        let mut out = Out::default();
        if !self.due {
            return Ok((out, false));
        }
        let upto = self.base.left();
        let mut given = 0;
        loop {
            let (rows, full) = self.base.take_due(state, self.id, upto).await?;
            given += rows.len();
            out.add(self.release(state, rows).await?)?;
            if !full {
                break;
            }
            if given >= RELEASE {
                return Ok((out, true));
            }
        }
        state.expire(self.id, upto);
        state.expire(self.waiting, upto);
        self.due = false;
        Ok((out, false))
    }

    /// Gives rows that left state: each key's in order, with each
    /// function's value, through the steps after the operator.
    async fn release<S: StateBackend>(
        &mut self,
        state: &mut S,
        due: Vec<DueRow>,
    ) -> Result<Out, JobError> {
        let mut out = Out::default();
        if due.is_empty() {
            return Ok(out);
        }
        let kept: Vec<(u32, &[u8])> = due.iter().map(|d| (d.group, d.row.as_ref())).collect();
        let (rows, ids) = self.base.decode(state, &kept).await?;
        let n = rows.num_rows();
        // Each function's argument and FILTER, row by row: a row one fails
        // on is quarantined, and has no place in any frame.
        let (args, filters, bad) = self.arguments(&rows)?;
        let defaults = self.defaults(&rows)?;
        // Each key's frame.
        let mut names: Vec<(u32, Vec<u8>)> =
            due.iter().map(|d| (d.group, d.key.to_vec())).collect();
        names.dedup();
        let named: Vec<Vec<u8>> = names.iter().map(|(_, k)| frame_key(k)).collect();
        let refs: Vec<KeyRef<'_>> = names
            .iter()
            .zip(&named)
            .map(|((g, _), k)| KeyRef::new(*g, k))
            .collect();
        let got = state
            .get(self.id, &refs)
            .await
            .map_err(|e| state_error(&e))?;
        let mut frames: BTreeMap<(u32, Vec<u8>), Vec<Kept>> = BTreeMap::new();
        for (name, value) in names.into_iter().zip(got) {
            let kept = match value {
                Some(v) => self.decode_frame(&v)?,
                None => self.funcs.iter().map(Func::start).collect(),
            };
            frames.insert(name, kept);
        }
        // Each row's value of each function, by peers: rows of a key and a
        // time.
        let mut values: Vec<Vec<Got>> = vec![vec![Ok(None); n]; self.funcs.len()];
        let mut i = 0;
        while i < n {
            let d = &due[i];
            let mut j = i;
            while j < n && due[j].group == d.group && due[j].key == d.key && due[j].time == d.time {
                j += 1;
            }
            let peers: Vec<usize> = (i..j).filter(|r| !bad.contains_key(r)).collect();
            if let Some(kept) = frames.get_mut(&(d.group, d.key.to_vec()))
                && !peers.is_empty()
            {
                for (f, func) in self.funcs.iter().enumerate() {
                    let rows: Vec<Peer> = peers
                        .iter()
                        .map(|&r| {
                            let v = args[f].as_ref().and_then(|a| func.value(a.as_ref(), r));
                            (v, frame_passes(filters[f].as_ref(), r))
                        })
                        .collect();
                    let got = func.peers(&mut kept[f], d.time, &rows, defaults[f].as_ref());
                    for (&r, v) in peers.iter().zip(got) {
                        values[f][r] = v;
                    }
                }
            }
            i = j;
        }
        for ((g, k), kept) in &frames {
            let mut b = vec![LAYOUT];
            for k in kept {
                k.encode(&mut b);
            }
            state.put(self.id, KeyRef::new(*g, &frame_key(k)), Bytes::from(b));
        }
        // Rows that can't leave: a function that fails on them, or whose
        // value doesn't fit its type.
        let mut good = Vec::with_capacity(n);
        for r in 0..n {
            if let Some(why) = bad.get(&r) {
                out.failed.push((
                    render_row(&rows, r),
                    why.clone(),
                    Cause::new(Code::Statement),
                ));
                continue;
            }
            match values.iter().find_map(|v| v[r].as_ref().err()) {
                Some(m) => out.failed.push((
                    render_row(&rows, r),
                    self.reason(m),
                    Cause::new(Code::Aggregate),
                )),
                None => good.push(r),
            }
        }
        if good.is_empty() {
            return Ok(out);
        }
        let kept = steps::take(&rows, &good).map_err(|e| JobError::internal(e.message))?;
        let mut columns = kept.columns().to_vec();
        for (f, def) in self.plan.functions.iter().enumerate() {
            let vs: Vec<Option<Value>> = good
                .iter()
                .map(|&r| values[f][r].clone().ok().flatten())
                .collect();
            columns.push(
                codec::build(&def.data_type, &vs)
                    .map_err(|e| JobError::internal(self.reason(&e)))?,
            );
        }
        let framed = RecordBatch::try_new_with_options(
            self.plan.framed.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(good.len())),
        )
        .map_err(|e| JobError::internal(self.reason(&e.to_string())))?;
        let (ran, failed) = steps::run_rows(&self.plan.steps, framed.clone(), true)
            .map_err(|e| JobError::internal(self.reason(&e.message)))?;
        for (r, why) in &failed {
            out.failed.push((
                render_row(&framed, *r),
                self.reason(why),
                Cause::new(Code::Statement),
            ));
        }
        if ran.batch.num_rows() == 0 {
            return Ok(out);
        }
        let at: Vec<usize> = ran.rows.iter().map(|&r| good[r]).collect();
        let ids = at.iter().map(|&r| ids[r]);
        out.rows = Some(self.base.typed(ran.batch, &rows, &at, ids, None)?);
        Ok(out)
    }

    /// Each function's FILTER on `rows`, then its argument on the rows its
    /// FILTER admits, as a group window's aggregates do, and the rows one
    /// fails on, with why.
    fn arguments(&self, rows: &RecordBatch) -> Result<steps::Arguments, JobError> {
        let functions = self
            .plan
            .functions
            .iter()
            .map(|f| (f.arg.as_ref(), f.filter.as_ref()));
        steps::arguments(rows, functions, &|why| self.reason(why))
    }

    /// `lag`'s defaults, constants of the value's type, as `rows` give them.
    fn defaults(&self, rows: &RecordBatch) -> Result<Vec<Option<Value>>, JobError> {
        let one = rows.slice(0, rows.num_rows().min(1));
        self.plan
            .functions
            .iter()
            .map(|f| match &f.kind {
                OverKind::Lag {
                    default: Some(e), ..
                } => {
                    let a = e
                        .eval(&one)
                        .map_err(|e| JobError::internal(self.reason(&e.message)))?;
                    Ok(match a.is_empty() {
                        true => None,
                        false => Value::read(a.as_ref(), 0),
                    })
                }
                _ => Ok(None),
            })
            .collect()
    }

    fn decode_frame(&self, v: &[u8]) -> Result<Vec<Kept>, JobError> {
        let mut at = v;
        let parsed = (|| {
            let [layout] = take::<1>(&mut at)?;
            if layout != LAYOUT {
                return Err(format!("a key's frame of layout {layout}"));
            }
            let mut out = Vec::with_capacity(self.funcs.len());
            for _ in &self.funcs {
                out.push(Kept::decode(&mut at)?);
            }
            Ok(out)
        })();
        parsed.map_err(|e| self.base.corrupt(&e))
    }
}

/// A key's frame's key in state.
fn frame_key(k: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + k.len());
    b.push(FRAME);
    b.extend_from_slice(k);
    b
}

/// Whether the steps before the operator unnest, so a row may give
/// several.
pub(crate) fn unnests(steps: &[Step]) -> bool {
    steps.iter().any(|s| matches!(s, Step::Unnest(_)))
}

/// The record IDs of the rows the steps before the operator gave, from
/// their input rows' `ids` at `rows`. Where they unnest, the k-th row of
/// an input row has an ID of its own, derived from that row's.
pub(crate) fn ids_of(
    ids: &FixedSizeBinaryArray,
    rows: &[usize],
    unnest: bool,
) -> Result<FixedSizeBinaryArray, String> {
    match unnest {
        false => {
            let taken = arrow_select::take::take(
                ids,
                &UInt32Array::from_iter_values(rows.iter().map(|&r| r as u32)),
                None,
            )
            .map_err(|e| e.to_string())?;
            Ok(taken.as_fixed_size_binary().clone())
        }
        true => {
            let mut seen: HashMap<usize, usize> = HashMap::new();
            let ordinals: Vec<usize> = rows
                .iter()
                .map(|r| {
                    let k = seen.entry(*r).or_default();
                    *k += 1;
                    *k - 1
                })
                .collect();
            openetl_normalize::element_ids(ids, rows, &ordinals).map_err(|e| e.to_string())
        }
    }
}

/// Whether row `r` counts for a function with a FILTER: its predicate is
/// true there.
fn frame_passes(filter: Option<&BooleanArray>, r: usize) -> bool {
    crate::window::accumulate::passes(filter, r)
}

pub(crate) fn state_error(e: &openetl_state::Error) -> JobError {
    crate::state::state_error("OVER's state", e)
}
