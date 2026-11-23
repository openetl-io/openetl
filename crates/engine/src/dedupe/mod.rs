//! Dedupe by `ROW_NUMBER() OVER (PARTITION BY keys ORDER BY event time)
//! = 1`: one row for each key, with its state through [`StateBackend`].
//!
//! Its input is the statement's, as `OVER`'s is ([`Partitioned`]): each
//! row has a key, `PARTITION BY`'s values, an event time and a record ID.
//! A key's rows with the same event time go by their record IDs, then
//! their values, so which row a key keeps depends on the rows alone, never
//! on how they came. A row whose event time is null is quarantined.
//!
//! Ascending, it keeps each key's first row, which the output appends. A
//! kept key is in a set with its first row's time. A row of a kept key,
//! less than the retention after that time, is a repeat, and is dropped as
//! it comes, late or not. Any other row waits in state until the watermark
//! passes its time, when no earlier row of its key can come but a late
//! one; then a key's rows leave in order, and each is written where its
//! key isn't kept by then, and the key is kept from it. A row at or past
//! the retention is so written once the watermark passes its own time,
//! which is past the key's retention by then. A late row of a key not kept
//! is quarantined, as it might have been the key's first. With a
//! retention, a key is forgotten once the watermark passes its first row's
//! time plus the retention: the set's horizon trails the watermark by the
//! retention, so a changed retention applies to every key kept, and a key
//! forgotten stays forgotten. Unbounded, a key is never forgotten.
//!
//! Descending, it keeps each key's latest row, as upserts keyed by the
//! partition keys, as rows come: a row later than its key's, by event time,
//! record ID, then a hash of its values, is written, and any other dropped,
//! whatever the watermark, so an older row that comes after a newer one
//! never replaces it. The key's latest rank stays in state for good, with
//! whether the output has the key's row. A row the filters after the
//! operator drop is written as a delete of its key alone, its key's
//! columns and nulls, where the output has the key's row. Over a stream
//! read whole each sync (full_refresh), whose reads each start with no
//! state, the output may have a key's row from an earlier read: there a
//! key is unknown until a row of it is written or deleted, and a dropped
//! row of an unknown key is written as a delete too. So the output holds a
//! key's latest row where it passes them, and none where it doesn't. Of a
//! key's rows, only one later than every row written of it is written, and
//! its sequence is its event time, so a merge, which keeps a key's row of
//! the greatest sequence and of one sequence the last staged, ends with
//! the key's latest row's.
//!
//! A written row has its record ID, and its event time as its sequence.

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_array::{ArrayRef, FixedSizeBinaryArray, RecordBatch, RecordBatchOptions, StringArray};
use bytes::Bytes;
use openetl_normalize::{Cause, Code, render_row};
use openetl_sql::{Keep, Step};
use openetl_state::{DueRow, KeyRef, NEVER, OperatorId, StateBackend};

use crate::error::JobError;
use crate::over::partitioned::{Partitioned, RELEASE, TAKE, Taken};
use crate::over::state_error;
use crate::window::codec::take;
use crate::window::{Out, steps};

/// The layout of a key's latest row, which the operator's ID covers.
const LAYOUT: u8 = openetl_sql::DEDUPE_STATE;

/// What a value in the operator's state is: a key's latest row's time and
/// ID. Partitioned keeps the schemas rows are written under beside them.
const LATEST: u8 = 3;
/// Under the operator's marks: how many keys a bucket of first times kept,
/// and the timer that counts them forgotten.
const BUCKET: u8 = 4;

/// The buckets of first times a retention is cut into, to count the keys it
/// forgets: each counts them at most a sixteenth of it late.
const BUCKETS: i64 = 16;

/// A dedupe of a statement, on one lane's key groups.
pub struct Operator {
    /// The set of kept keys', whose horizon trails the watermark by the
    /// retention.
    pub id: OperatorId,
    /// The rows waiting for the watermark's, whose horizon in each key group
    /// is where they have left up to, in the event time's unit, and the
    /// count of keys kept in each bucket of first times.
    marks: OperatorId,
    plan: Arc<openetl_sql::Dedupe>,
    base: Partitioned,
    /// A first-row dedupe's, in the event time's unit; None for unbounded
    /// and for a latest-row dedupe.
    retention: Option<i64>,
    /// Its stream is read whole each sync, each read with no state.
    full_read: bool,
    /// Rows are due that it hasn't given yet.
    due: bool,
    /// Keys it began keeping, keys it forgot, and rows it dropped, since its
    /// caller last took them.
    pub kept: u64,
    pub forgot: u64,
    pub repeats: u64,
}

impl Operator {
    /// `id` is the statement's operator's, `at` where it is, `retention`
    /// the stream's, in microseconds or NEVER, which a first-row dedupe
    /// needs, `full_read` whether the stream is read whole each sync, and
    /// `key_groups` the pipeline's.
    pub fn new(
        id: &str,
        at: openetl_sql::Position,
        plan: Arc<openetl_sql::Dedupe>,
        output: &str,
        (retention, full_read): (Option<i64>, bool),
        key_groups: u32,
    ) -> Result<Self, JobError> {
        let base = Partitioned::new(
            id, at, output, &plan.rows, &plan.keys, plan.time, key_groups,
        )?;
        let retention = match (plan.keep, retention) {
            (Keep::Latest, _) | (Keep::First, Some(NEVER)) => None,
            (Keep::First, Some(r)) if r > 0 => Some(r.saturating_mul(base.per_micro)),
            (Keep::First, Some(r)) => {
                return Err(JobError::config(base.reason(&format!(
                    "a dedupe's retention of {r}µs: it keeps a key for a while, more than none"
                ))));
            }
            (Keep::First, None) => {
                return Err(JobError::config(crate::transform::no_retention(
                    at,
                    output,
                    &plan.input.input,
                )));
            }
        };
        let id = base.id;
        Ok(Self {
            id,
            marks: OperatorId(id.0 ^ 1),
            plan,
            base,
            retention,
            full_read,
            due: false,
            kept: 0,
            forgot: 0,
            repeats: 0,
        })
    }

    /// Its watermark in microseconds, if it has advanced.
    pub fn watermark_micros(&self) -> Option<i64> {
        self.base.watermark_micros()
    }

    /// The watermark `wm` as rows tell it apart: a first-row dedupe's rows
    /// wait for every microsecond of it, and a latest-row dedupe's for none.
    pub fn level(&self, wm: i64) -> i64 {
        match self.plan.keep {
            Keep::First => wm,
            Keep::Latest => i64::MIN,
        }
    }

    /// Whether its state is done at the end of input, which a later read
    /// starts again: but for a latest-row dedupe over a stream not read
    /// whole, whose keys' latest rows go on in the next sync's read.
    pub fn ends_with_input(&self) -> bool {
        self.full_read || self.plan.keep == Keep::First
    }

    /// The time a kept key's first row must be after, for a row of it at
    /// `t` to be a repeat: none where nothing is forgotten.
    fn forgets_before(&self, t: i64) -> i64 {
        match self.retention {
            Some(r) => t.saturating_sub(r),
            None => i64::MIN,
        }
    }

    /// Takes a batch of the statement's input, the columns it reads, with
    /// each row's record ID: rows of kept keys are dropped, a first-row
    /// dedupe's other rows wait for the watermark, and a latest-row
    /// dedupe's are written where they're their key's latest.
    pub async fn rows<S: StateBackend>(
        &mut self,
        state: &mut S,
        input: &RecordBatch,
        ids: &FixedSizeBinaryArray,
    ) -> Result<Out, JobError> {
        let plan = self.plan.clone();
        let (taken, mut out) = self.base.take(&plan.input.steps, input, ids)?;
        let timed: Vec<(usize, i64)> = (0..taken.rows.num_rows())
            .filter_map(|r| match taken.times[r] {
                Some(t) => Some((r, t)),
                None => {
                    out.rejected.push((
                        taken.at[r],
                        self.base.reason("its event time is null"),
                        Cause::new(Code::EventTime),
                    ));
                    None
                }
            })
            .collect();
        match plan.keep {
            Keep::First => self.first(state, &taken, &timed, &mut out).await?,
            Keep::Latest => self.latest(state, &taken, &timed, &mut out).await?,
        }
        out.rejected.sort_by_key(|(r, ..)| *r);
        Ok(out)
    }

    /// A first-row dedupe's rows: those of keys not kept wait for the
    /// watermark, or are late.
    async fn first<S: StateBackend>(
        &mut self,
        state: &mut S,
        taken: &Taken,
        timed: &[(usize, i64)],
        out: &mut Out,
    ) -> Result<(), JobError> {
        if timed.is_empty() {
            return Ok(());
        }
        // Whether each row's key is kept: an entry that expires at once
        // keeps nothing, so the set only answers.
        let refs: Vec<KeyRef<'_>> = timed
            .iter()
            .map(|&(r, _)| KeyRef::new(taken.groups[r], &taken.keys[r]))
            .collect();
        let at: Vec<i64> = timed.iter().map(|&(_, t)| self.forgets_before(t)).collect();
        let fresh = state
            .insert_new(self.id, &refs, &at, &vec![i64::MIN; timed.len()])
            .await
            .map_err(|e| state_error(&e))?;
        for (&(r, t), new) in timed.iter().zip(fresh) {
            if !new {
                self.repeats += 1;
            } else if self.base.late(state, self.marks, taken.groups[r], t) {
                out.rejected.push((
                    taken.at[r],
                    self.base.reason(
                        "late: the watermark passed its event time before it came, and it may have been its key's first row",
                    ),
                    Cause::new(Code::Late),
                ));
            } else {
                let row = self.base.encode(state, taken, r)?;
                let key = KeyRef::new(taken.groups[r], &taken.keys[r]);
                state.append(self.marks, key, t, Bytes::from(row));
            }
        }
        Ok(())
    }

    /// Moves the watermark to `t`, in microseconds, if it's later. A
    /// first-row dedupe gives the rows before it whose keys aren't kept, at
    /// most about [`RELEASE`] a call, and says whether more are due, for
    /// another call with the same `t`; then it forgets the keys the
    /// retention has passed. A latest-row dedupe gives nothing.
    pub async fn advance<S: StateBackend>(
        &mut self,
        state: &mut S,
        t: i64,
    ) -> Result<(Out, bool), JobError> {
        self.due |= self.base.advance(t);
        let mut out = Out::default();
        if !self.due || self.plan.keep == Keep::Latest {
            self.due = false;
            return Ok((out, false));
        }
        let upto = self.base.left();
        let mut given = 0;
        loop {
            let (rows, full) = self.base.take_due(state, self.marks, upto).await?;
            given += rows.len();
            out.add(self.release(state, rows).await?)?;
            if !full {
                break;
            }
            if given >= RELEASE {
                return Ok((out, true));
            }
        }
        self.forget(state, upto).await?;
        self.due = false;
        Ok((out, false))
    }

    /// Gives the rows that left state, each key's in order, where the key
    /// isn't kept by then, and keeps their keys.
    async fn release<S: StateBackend>(
        &mut self,
        state: &mut S,
        due: Vec<DueRow>,
    ) -> Result<Out, JobError> {
        let mut out = Out::default();
        if due.is_empty() {
            return Ok(out);
        }
        let refs: Vec<KeyRef<'_>> = due.iter().map(|d| KeyRef::new(d.group, &d.key)).collect();
        let at: Vec<i64> = due.iter().map(|d| self.forgets_before(d.time)).collect();
        let expires: Vec<i64> = due
            .iter()
            .map(|d| match self.retention {
                Some(_) => d.time,
                None => NEVER,
            })
            .collect();
        let fresh = state
            .insert_new(self.id, &refs, &at, &expires)
            .await
            .map_err(|e| state_error(&e))?;
        let kept: Vec<&DueRow> = due
            .iter()
            .zip(fresh)
            .filter_map(|(d, new)| new.then_some(d))
            .collect();
        self.repeats += (due.len() - kept.len()) as u64;
        self.count_kept(state, &kept).await?;
        let rows: Vec<(u32, &[u8])> = kept.iter().map(|d| (d.group, d.row.as_ref())).collect();
        let (rows, ids) = self.base.decode(state, &rows).await?;
        let (ran, failed) = steps::run_rows(&self.plan.steps, rows.clone(), true)
            .map_err(|e| JobError::internal(self.base.reason(&e.message)))?;
        for (r, why) in failed {
            out.failed.push((
                render_row(&rows, r),
                self.base.reason(&why),
                Cause::new(Code::Statement),
            ));
        }
        if ran.batch.num_rows() > 0 {
            let ids = ran.rows.iter().map(|&r| ids[r]);
            out.rows = Some(self.base.typed(ran.batch, &rows, &ran.rows, ids, None)?);
        }
        Ok(out)
    }

    /// Counts the keys a first-row dedupe began keeping, by bucket of their
    /// first rows' times, so that it counts them forgotten once the
    /// watermark passes a bucket's end plus the retention.
    async fn count_kept<S: StateBackend>(
        &mut self,
        state: &mut S,
        kept: &[&DueRow],
    ) -> Result<(), JobError> {
        self.kept += kept.len() as u64;
        let Some(r) = self.retention else {
            return Ok(());
        };
        let width = (r / BUCKETS).max(self.base.per_micro.saturating_mul(1_000_000));
        let mut buckets: BTreeMap<(u32, i64), u64> = BTreeMap::new();
        for d in kept {
            let end = d.time - d.time.rem_euclid(width);
            *buckets
                .entry((d.group, end.saturating_add(width - 1)))
                .or_default() += 1;
        }
        let names: Vec<(u32, Vec<u8>)> = buckets
            .keys()
            .map(|(g, end)| (*g, named(BUCKET, &end.to_be_bytes())))
            .collect();
        let refs: Vec<KeyRef<'_>> = names.iter().map(|(g, k)| KeyRef::new(*g, k)).collect();
        let got = state
            .get(self.marks, &refs)
            .await
            .map_err(|e| state_error(&e))?;
        for (((_, end), n), (k, before)) in buckets.iter().zip(refs.iter().zip(got)) {
            let before = before.as_deref().map(count).transpose();
            let before = before.map_err(|e| self.base.corrupt(&e))?.unwrap_or(0);
            state.put(
                self.marks,
                *k,
                Bytes::copy_from_slice(&(before + n).to_be_bytes()),
            );
            state.set_timer(self.marks, *k, end.saturating_add(r));
        }
        Ok(())
    }

    /// Forgets the keys the retention has passed, counting each bucket of
    /// them, and marks where rows have left up to.
    async fn forget<S: StateBackend>(&mut self, state: &mut S, upto: i64) -> Result<(), JobError> {
        if let Some(r) = self.retention {
            loop {
                let gone = state
                    .fire(self.marks, upto, TAKE)
                    .await
                    .map_err(|e| state_error(&e))?;
                let refs: Vec<KeyRef<'_>> =
                    gone.iter().map(|g| KeyRef::new(g.group, &g.key)).collect();
                let got = state
                    .get(self.marks, &refs)
                    .await
                    .map_err(|e| state_error(&e))?;
                for (k, n) in refs.iter().zip(got) {
                    let n = n.as_deref().map(count).transpose();
                    self.forgot += n.map_err(|e| self.base.corrupt(&e))?.unwrap_or(0);
                    state.delete(self.marks, *k);
                }
                if gone.len() < TAKE {
                    break;
                }
            }
            state.expire(self.id, upto.saturating_sub(r));
        }
        state.expire(self.marks, upto);
        Ok(())
    }

    /// A latest-row dedupe's rows: each one later than its key's latest is
    /// written, an upsert where it passes the filters after the operator,
    /// and a delete of its key where it doesn't and the output has the
    /// key's row, or, over a full read, may have it. A row the filters fail
    /// on is quarantined, and the output keeps the key's row it has.
    async fn latest<S: StateBackend>(
        &mut self,
        state: &mut S,
        taken: &Taken,
        timed: &[(usize, i64)],
        out: &mut Out,
    ) -> Result<(), JobError> {
        // A key state doesn't have: over a full read, the output may have
        // its row from an earlier read.
        let unseen = match self.full_read {
            true => Shown::Unknown,
            false => Shown::No,
        };
        let mut latest: BTreeMap<(u32, Vec<u8>), Latest> = timed
            .iter()
            .map(|&(r, _)| {
                let name = (taken.groups[r], named(LATEST, &taken.keys[r]));
                let l = Latest {
                    stored: None,
                    shown: unseen,
                    row: None,
                };
                (name, l)
            })
            .collect();
        let refs: Vec<KeyRef<'_>> = latest.keys().map(|(g, k)| KeyRef::new(*g, k)).collect();
        let got = match refs.is_empty() {
            true => Vec::new(),
            false => state
                .get(self.id, &refs)
                .await
                .map_err(|e| state_error(&e))?,
        };
        for (l, v) in latest.values_mut().zip(got) {
            if let Some(v) = v {
                (l.stored, l.shown) = decode_latest(&v).map_err(|e| self.base.corrupt(&e))?;
            }
        }
        // Rows of one time and ID go by their values' hashes, which only
        // such ties need.
        let base = &self.base;
        let mut hashes: HashMap<usize, u64> = HashMap::new();
        let mut hash = |r: usize| -> Result<u64, JobError> {
            if let Some(h) = hashes.get(&r) {
                return Ok(*h);
            }
            let h = base.values_hash(taken, r)?;
            hashes.insert(r, h);
            Ok(h)
        };
        let mut repeats = 0;
        for &(r, t) in timed {
            let name = (taken.groups[r], named(LATEST, &taken.keys[r]));
            let Some(l) = latest.get_mut(&name) else {
                continue;
            };
            let current = match (l.row, &l.stored) {
                (Some((b, tb)), _) => Some((tb, taken.ids[b], Err(b))),
                (None, Some(s)) => Some((s.time, s.id, Ok(s.values))),
                (None, None) => None,
            };
            let later = match current {
                None => true,
                Some((ct, cid, values)) => match (t, taken.ids[r]).cmp(&(ct, cid)) {
                    Ordering::Greater => true,
                    Ordering::Less => false,
                    Ordering::Equal => {
                        let theirs = match values {
                            Ok(v) => v,
                            Err(b) => hash(b)?,
                        };
                        hash(r)? > theirs
                    }
                },
            };
            match later {
                true => {
                    repeats += u64::from(l.row.is_some());
                    l.row = Some((r, t));
                }
                false => repeats += 1,
            }
        }
        self.repeats += repeats;
        let changed: Vec<((u32, Vec<u8>), Latest)> = latest
            .into_iter()
            .filter(|(_, l)| l.row.is_some())
            .collect();
        if changed.is_empty() {
            return Ok(());
        }
        let at: Vec<usize> = changed
            .iter()
            .filter_map(|(_, l)| l.row.map(|r| r.0))
            .collect();
        let internal = |e: openetl_sql::Error| JobError::internal(e.message);
        let rows = steps::take(&taken.rows, &at).map_err(internal)?;
        let (ran, failed) = steps::run_rows(&self.plan.steps, rows.clone(), true)
            .map_err(|e| JobError::internal(self.base.reason(&e.message)))?;
        let failed: HashMap<usize, String> = failed.into_iter().collect();
        let mut passed = vec![None; at.len()];
        for (i, &r) in ran.rows.iter().enumerate() {
            passed[r] = Some(i);
        }
        let (mut written, mut ops, mut deleted) = (Vec::new(), Vec::new(), Vec::new());
        for (i, ((g, name), l)) in changed.iter().enumerate() {
            let mut shown = l.shown;
            if let Some(p) = passed[i] {
                written.push(p);
                ops.push(match shown {
                    Shown::Yes => "update",
                    Shown::No | Shown::Unknown => "insert",
                });
                shown = Shown::Yes;
            } else if let Some(why) = failed.get(&i) {
                // The output keeps what it has, known or not.
                out.failed.push((
                    render_row(&rows, i),
                    self.base.reason(why),
                    Cause::new(Code::Statement),
                ));
            } else if shown != Shown::No {
                deleted.push(i);
                shown = Shown::No;
            }
            self.kept += u64::from(l.stored.is_none());
            let (r, time) = l.row.unwrap_or_default();
            let rank = Rank {
                time,
                id: taken.ids[r],
                values: hash(r)?,
            };
            state.put(self.id, KeyRef::new(*g, name), latest_value(&rank, shown));
        }
        let mut batches = Vec::new();
        if !written.is_empty() {
            batches.push(steps::take(&ran.batch, &written).map_err(internal)?);
        }
        if !deleted.is_empty() {
            batches.push(self.key_deletes(&rows, &deleted)?);
            ops.extend(deleted.iter().map(|_| "delete"));
        }
        if batches.is_empty() {
            return Ok(());
        }
        // Each written row's row of `rows`: the upserts', then the deletes'.
        let order: Vec<usize> = written
            .iter()
            .map(|&p| ran.rows[p])
            .chain(deleted.iter().copied())
            .collect();
        let batch = match batches.len() {
            1 => batches.remove(0),
            _ => arrow_select::concat::concat_batches(&batches[0].schema(), &batches)
                .map_err(|e| JobError::internal(e.to_string()))?,
        };
        let ops: ArrayRef = Arc::new(StringArray::from(ops));
        let ids = order.iter().map(|&i| taken.ids[at[i]]);
        out.rows = Some(self.base.typed(batch, &rows, &order, ids, Some(ops))?);
        Ok(())
    }

    /// The output's rows that delete the keys of rows `at` of `rows`, rows
    /// of the operator's input: each output column that passes a
    /// `PARTITION BY` column through has its value, and the rest are null,
    /// or their type's zero where they can't be. No step after the
    /// operator runs, as a filter dropped these rows before its
    /// projections.
    fn key_deletes(&self, rows: &RecordBatch, at: &[usize]) -> Result<RecordBatch, JobError> {
        let schema = self
            .plan
            .steps
            .iter()
            .rev()
            .find_map(|s| match s {
                Step::Project(p) => Some(p.schema.clone()),
                _ => None,
            })
            .unwrap_or_else(|| self.plan.rows.clone());
        let internal = |e: String| {
            JobError::internal(self.base.reason(&format!("a delete of a key's row: {e}")))
        };
        let n = at.len();
        let mut columns = Vec::with_capacity(schema.fields().len());
        for f in schema.fields() {
            let key = self.plan.keys.iter().find(|k| {
                self.plan
                    .passes
                    .iter()
                    .any(|(stream, out)| *stream == k.stream && out == f.name())
            });
            columns.push(match key {
                Some(k) => {
                    let c = steps::take_column(rows.column(k.column), at)
                        .map_err(|e| internal(e.message))?;
                    if c.data_type() != f.data_type() {
                        return Err(internal(format!(
                            "key column {} is {} in the rows kept and {} in the output",
                            f.name(),
                            c.data_type(),
                            f.data_type()
                        )));
                    }
                    c
                }
                None => steps::empty(f, n).map_err(internal)?,
            });
        }
        RecordBatch::try_new_with_options(
            schema,
            columns,
            &RecordBatchOptions::new().with_row_count(Some(n)),
        )
        .map_err(|e| internal(e.to_string()))
    }
}

/// A latest-row dedupe's key in a batch: its latest row as state has it,
/// whether the output has its row, and the row of the batch later than
/// that, with its time.
struct Latest {
    stored: Option<Rank>,
    shown: Shown,
    row: Option<(usize, i64)>,
}

/// Whether the output has a key's row: unknown, over a full read, until a
/// row of the key is written or deleted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shown {
    No,
    Yes,
    Unknown,
}

/// Where a row stands among its key's, for a latest-row dedupe: by event
/// time, then record ID, then a hash of its values.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
struct Rank {
    time: i64,
    id: [u8; 16],
    values: u64,
}

/// A key's name in state, of what `kind` holds of it.
fn named(kind: u8, key: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + key.len());
    b.push(kind);
    b.extend_from_slice(key);
    b
}

/// A key's latest row's rank, and whether the output has it: 0 no, 1 yes,
/// 2 unknown.
fn latest_value(rank: &Rank, shown: Shown) -> Bytes {
    let mut b = Vec::with_capacity(34);
    b.push(LAYOUT);
    b.extend_from_slice(&rank.time.to_be_bytes());
    b.extend_from_slice(&rank.id);
    b.extend_from_slice(&rank.values.to_be_bytes());
    b.push(match shown {
        Shown::No => 0,
        Shown::Yes => 1,
        Shown::Unknown => 2,
    });
    Bytes::from(b)
}

fn decode_latest(v: &[u8]) -> Result<(Option<Rank>, Shown), String> {
    let mut at = v;
    let [layout] = take::<1>(&mut at)?;
    if layout != LAYOUT {
        return Err(format!("a key's latest row of layout {layout}"));
    }
    let rank = Rank {
        time: i64::from_be_bytes(take(&mut at)?),
        id: take(&mut at)?,
        values: u64::from_be_bytes(take(&mut at)?),
    };
    let shown = match take::<1>(&mut at)? {
        [0] => Shown::No,
        [1] => Shown::Yes,
        [2] => Shown::Unknown,
        [b] => return Err(format!("a key's latest row shown as {b}")),
    };
    Ok((Some(rank), shown))
}

fn count(v: &[u8]) -> Result<u64, String> {
    let mut at = v;
    Ok(u64::from_be_bytes(take(&mut at)?))
}
