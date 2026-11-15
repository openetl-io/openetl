//! The group-window operator: `tumble`, `hop` and `session` windows over a
//! stream's event time, keyed by the group keys, with their accumulators
//! in state through [`StateBackend`].
//!
//! Its input is the statement's: the columns it reads of the stream's
//! rows, which run the row-wise steps before the window. A row goes to
//! each window its event time falls in. A window's state is a value under
//! its group keys and start: its accumulators, how many times its row has
//! been given, and whether the output has it now. A session's is a value
//! under its group keys: every session the keys have open or kept.
//!
//! The watermark goes by [`Operator::advance`]: `t` means every row whose
//! event time is before `t` has come, in microseconds. A window closes once
//! the watermark reaches its end, and a session once it passes its last
//! row's time and the gap: its row is given then, if it passes HAVING.
//! Timers close windows. With allowed lateness, a closed window's state is
//! kept until the watermark reaches its end plus the lateness: a row for it
//! until then gives its row again, as an upsert, or a delete where it no
//! longer passes HAVING, and a session it merges away gives a delete. A row
//! all of whose windows are past that, or whose event time is null, is
//! late and quarantined. `i64::MAX` is the end of input: every window
//! closes.
//!
//! Output rows are keyed by `window_start` and the group keys: an insert
//! the first time, then updates; each has an ID and a seq from its key and
//! how many times it has been given, so a replay gives the same rows.

mod accumulate;
mod codec;
mod steps;
#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bytes::Bytes;
use openetl_normalize::{Control, Typed, key_groups, render_row};
use openetl_proto::connector::v1::ErrorCode;
use openetl_sql::{GroupWindow, Window};
use openetl_state::{KeyRef, OperatorId, StateBackend};
use twox_hash::XxHash3_128;

use self::accumulate::{Acc, Spec, passes};
use self::codec::{Value, take};
use crate::error::JobError;

/// Timers asked of state at once.
const TIMERS: usize = 1024;

const CLOSE: u8 = 0;
const EXPIRE: u8 = 1;

/// A window's state.
#[derive(Clone, Debug, PartialEq)]
struct State {
    /// How many times its row has been given.
    emitted: u32,
    /// The output has its row now.
    visible: bool,
    accs: Vec<Acc>,
}

/// The layout of a window's state, and of a key's sessions.
const LAYOUT: u8 = 1;

impl State {
    fn new(specs: &[Spec]) -> Self {
        Self {
            emitted: 0,
            visible: false,
            accs: specs.iter().map(Acc::new).collect(),
        }
    }

    fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.emitted.to_be_bytes());
        out.push(u8::from(self.visible));
        for a in &self.accs {
            a.encode(out);
        }
    }

    fn decode(specs: &[Spec], buf: &mut &[u8]) -> Result<Self, String> {
        let emitted = u32::from_be_bytes(take(buf)?);
        let [visible] = take::<1>(buf)?;
        let mut accs = Vec::with_capacity(specs.len());
        for _ in specs {
            accs.push(Acc::decode(buf)?);
        }
        Ok(Self {
            emitted,
            visible: visible == 1,
            accs,
        })
    }

    fn merge(&mut self, other: State) {
        for (a, b) in self.accs.iter_mut().zip(other.accs) {
            a.merge(b);
        }
    }
}

/// A session: its first row's time and its last's plus the gap, and its
/// state.
#[derive(Clone, Debug, PartialEq)]
struct Session {
    start: i64,
    end: i64,
    state: State,
}

/// What a call gave.
#[derive(Default)]
pub struct Out {
    /// The output's rows: under the statement's schema, with IDs, ops and
    /// seqs; None for none.
    pub rows: Option<Typed>,
    /// Rows of the input the operator can't take, with why: late, without
    /// an event time, or failing a step.
    pub rejected: Vec<(usize, String)>,
    /// Windows whose row can't be given, rendered as JSON, with why: an
    /// aggregate that overflows, or a step after the window that fails.
    pub failed: Vec<(String, String)>,
}

impl Out {
    fn add(&mut self, other: Out) -> Result<(), JobError> {
        self.rejected.extend(other.rejected);
        self.failed.extend(other.failed);
        self.rows = match (self.rows.take(), other.rows) {
            (None, b) => b,
            (a, None) => a,
            (Some(a), Some(b)) => Some(concat(&a, &b)?),
        };
        Ok(())
    }
}

/// A group window of a statement, on one lane's key groups.
pub struct Operator {
    pub id: OperatorId,
    plan: Arc<GroupWindow>,
    /// The statement's place and output, for messages.
    at: String,
    output: String,
    schema: SchemaRef,
    specs: Vec<Spec>,
    key_names: Vec<String>,
    key_types: Vec<DataType>,
    time_type: DataType,
    /// The event time's unit per microsecond: 1, or 1000 for nanoseconds.
    per_micro: i64,
    /// In the event time's unit.
    lateness: i64,
    key_groups: u32,
    /// In microseconds; `i64::MIN` until it advances.
    watermark: i64,
}

impl Operator {
    /// `id` is the statement's operator's, `at` where it is, `schema` its
    /// output's, `lateness` in microseconds and `key_groups` the
    /// pipeline's.
    pub fn new(
        id: &str,
        at: openetl_sql::Position,
        plan: Arc<GroupWindow>,
        output: &str,
        schema: SchemaRef,
        lateness: i64,
        key_groups: u32,
    ) -> Result<Self, JobError> {
        let time_type = plan.rows.field(plan.time).data_type().clone();
        let per_micro = match &time_type {
            DataType::Timestamp(TimeUnit::Nanosecond, _) => 1000,
            _ => 1,
        };
        let specs = plan
            .aggregates
            .iter()
            .map(|a| Spec {
                kind: a.kind,
                arg: a.arg.as_ref().map(|e| e.data_type().clone()),
                out: a.data_type.clone(),
            })
            .collect();
        Ok(Self {
            id: operator(id),
            at: at.to_string(),
            output: output.to_owned(),
            schema,
            specs,
            key_names: plan
                .keys
                .iter()
                .map(|k| plan.rows.field(k.column).name().clone())
                .collect(),
            key_types: plan
                .keys
                .iter()
                .map(|k| plan.rows.field(k.column).data_type().clone())
                .collect(),
            time_type,
            per_micro,
            lateness: lateness.max(0).saturating_mul(per_micro),
            key_groups,
            watermark: i64::MIN,
            plan,
        })
    }

    /// The watermark, in the event time's unit.
    fn watermark(&self) -> i64 {
        self.watermark.saturating_mul(self.per_micro)
    }

    /// A time of the event time's unit, as the microsecond a timer at it
    /// fires at: the first the watermark covers it at.
    fn timer_at(&self, t: i64) -> i64 {
        match self.per_micro {
            1 => t,
            m => t.div_euclid(m) + i64::from(t.rem_euclid(m) != 0),
        }
    }

    fn reason(&self, why: &str) -> String {
        format!("transform.sql {}, writing {}: {why}", self.at, self.output)
    }

    /// Takes a batch of the statement's input: the columns it reads.
    pub async fn rows<S: StateBackend>(
        &mut self,
        state: &mut S,
        input: &RecordBatch,
    ) -> Result<Out, JobError> {
        let mut out = Out::default();
        let plan = self.plan.clone();
        let (ran, failed) = steps::run_rows(&plan.input.steps, input.clone(), true)
            .map_err(|e| JobError::internal(self.reason(&e.message)))?;
        for (r, why) in failed {
            out.rejected.push((r, self.reason(&why)));
        }
        let rows = ran.batch;
        if rows.num_rows() == 0 {
            return Ok(out);
        }
        let n = rows.num_rows();
        // Equal JSON is one key, whatever its spaces or its objects' order,
        // and in one key group, as the exchange before the window has it.
        let key_columns: Vec<ArrayRef> = plan
            .keys
            .iter()
            .map(|g| {
                let c = rows.column(g.column).clone();
                let json = is_json(rows.schema().field(g.column))
                    || input.schema().field_with_name(&g.stream).is_ok_and(is_json);
                match json {
                    true => canonical_json(&c),
                    false => c,
                }
            })
            .collect();
        let key_batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::new(
                plan.keys
                    .iter()
                    .map(|g| rows.schema().field(g.column).clone())
                    .collect::<Vec<_>>(),
            )),
            key_columns.clone(),
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(n)),
        )
        .map_err(|e| JobError::internal(self.reason(&e.to_string())))?;
        let groups = key_groups(&key_batch, &self.key_names, self.key_groups)
            .map_err(|e| JobError::internal(self.reason(&e.to_string())))?;
        let keys: Vec<Vec<u8>> = (0..n)
            .map(|r| {
                let mut k = Vec::new();
                for c in &key_columns {
                    codec::encode(Value::read(c.as_ref(), r).as_ref(), &mut k);
                }
                k
            })
            .collect();
        let times = rows.column(plan.time).clone();
        // Each aggregate's argument and filter, row by row: a row that
        // fails one is quarantined, and the rest are taken.
        let ((args, filters), bad) = self.arguments(&rows)?;
        for (r, why) in bad {
            out.rejected.push((ran.rows[r], why));
        }
        let mut take: Vec<(usize, i64)> = Vec::with_capacity(n);
        for r in 0..n {
            if out.rejected.iter().any(|(i, _)| *i == ran.rows[r]) {
                continue;
            }
            match Value::read(times.as_ref(), r) {
                Some(Value::Int(t)) => take.push((r, t)),
                _ => out
                    .rejected
                    .push((ran.rows[r], self.reason("its event time is null"))),
            }
        }
        let rows_of = |r: usize| ran.rows[r];
        let emitted = match plan.window {
            Window::Session { gap } => {
                let (emits, mut lists) = self
                    .sessions(
                        state, gap, &take, &groups, &keys, &args, &filters, &rows_of, &mut out,
                    )
                    .await?;
                let (emitted, states) = self.emit_states(emits)?;
                for e in states {
                    if let Some((_, _, list)) = lists
                        .iter_mut()
                        .find(|(g, k, _)| *g == e.group && *k == e.key)
                        && let Some(s) = list.iter_mut().find(|s| s.start == e.start)
                    {
                        s.state.emitted = e.state.emitted;
                        s.state.visible = e.state.visible;
                    }
                }
                for (g, k, list) in &lists {
                    self.put_sessions(state, *g, k, list);
                }
                emitted
            }
            _ => {
                let emits = self
                    .windows(
                        state, &take, &groups, &keys, &args, &filters, &rows_of, &mut out,
                    )
                    .await?;
                let (emitted, states) = self.emit_states(emits)?;
                for e in states {
                    state.put(
                        self.id,
                        KeyRef::new(e.group, &slot_key(&e.key, e.start)),
                        self.encode_state(&e.state),
                    );
                }
                emitted
            }
        };
        out.add(emitted)?;
        out.rejected.sort_by_key(|(r, _)| *r);
        Ok(out)
    }

    /// Each aggregate's filter on `rows`, then its argument on the rows
    /// its filter admits: a row an aggregate's filter leaves out never
    /// evaluates its argument. Where one fails on the batch, it's evaluated
    /// row by row, and a row it fails on is given with why: no aggregate
    /// takes it.
    fn arguments(&self, rows: &RecordBatch) -> Result<(Arguments, Vec<(usize, String)>), JobError> {
        let n = rows.num_rows();
        let all: Vec<usize> = (0..n).collect();
        let mut bad = BTreeMap::new();
        let mut args = Vec::with_capacity(self.plan.aggregates.len());
        let mut filters = Vec::with_capacity(self.plan.aggregates.len());
        for a in &self.plan.aggregates {
            let filter = match &a.filter {
                None => None,
                Some(e) => {
                    let got = self.each(
                        rows,
                        &all,
                        |b| e.eval_predicate(b).map(|p| Arc::new(p) as ArrayRef),
                        &mut bad,
                    )?;
                    Some(arrow_array::cast::AsArray::as_boolean(got.as_ref()).clone())
                }
            };
            let arg = match &a.arg {
                None => None,
                Some(e) => {
                    let admitted: Vec<usize> =
                        (0..n).filter(|&r| passes(filter.as_ref(), r)).collect();
                    Some(self.each(rows, &admitted, |b| e.eval(b), &mut bad)?)
                }
            };
            filters.push(filter);
            args.push(arg);
        }
        Ok(((args, filters), bad.into_iter().collect()))
    }

    /// `f` on the rows `at` of `rows`, as a column of all of them: null at
    /// the others, and at a row it fails on, which `bad` gets with why.
    fn each(
        &self,
        rows: &RecordBatch,
        at: &[usize],
        f: impl Fn(&RecordBatch) -> Result<ArrayRef, openetl_sql::Error>,
        bad: &mut BTreeMap<usize, String>,
    ) -> Result<ArrayRef, JobError> {
        let internal = |e: openetl_sql::Error| JobError::internal(e.message);
        let some = steps::take(rows, at).map_err(internal)?;
        let (got, ok) = match f(&some) {
            Ok(a) => (a, at.to_vec()),
            Err(_) => {
                let mut ok = Vec::with_capacity(at.len());
                for (i, &r) in at.iter().enumerate() {
                    match f(&some.slice(i, 1)) {
                        Ok(_) => ok.push(r),
                        Err(e) => {
                            bad.entry(r).or_insert_with(|| self.reason(&e.message));
                        }
                    }
                }
                let kept = steps::take(rows, &ok).map_err(internal)?;
                let got = f(&kept).map_err(|e| {
                    JobError::new(
                        ErrorCode::DataInvalid,
                        self.reason(&format!(
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
        arrow_select::take::take(got.as_ref(), &arrow_array::UInt64Array::from(index), None)
            .map_err(|e| JobError::internal(e.to_string()))
    }

    /// The starts of a tumbling or hopping window that `t` falls in.
    fn starts(&self, t: i64) -> Vec<i64> {
        match self.plan.window {
            Window::Tumble { size } => vec![t - t.rem_euclid(size)],
            Window::Hop { slide, size } => {
                let mut out = Vec::new();
                let mut s = t - t.rem_euclid(slide);
                while s.checked_add(size).is_some_and(|e| e > t) {
                    out.push(s);
                    match s.checked_sub(slide) {
                        Some(next) => s = next,
                        None => break,
                    }
                }
                out
            }
            Window::Session { .. } => Vec::new(),
        }
    }

    fn size(&self) -> i64 {
        match self.plan.window {
            Window::Tumble { size } | Window::Hop { size, .. } => size,
            Window::Session { gap } => gap,
        }
    }

    /// Tumbling and hopping windows take their rows. Gives the windows
    /// already closed that rows reached, to give again and then write
    /// back.
    #[allow(clippy::too_many_arguments)]
    async fn windows<S: StateBackend>(
        &mut self,
        state: &mut S,
        take: &[(usize, i64)],
        groups: &[u32],
        keys: &[Vec<u8>],
        args: &[Option<ArrayRef>],
        filters: &[Option<BooleanArray>],
        rows_of: &(dyn Fn(usize) -> usize + Sync),
        out: &mut Out,
    ) -> Result<Vec<Emit>, JobError> {
        let w = self.watermark();
        let size = self.size();
        // Each window's rows, by its group, keys and start.
        let mut slots: BTreeMap<(u32, Vec<u8>, i64), Vec<usize>> = BTreeMap::new();
        for &(r, t) in take {
            let mut placed = false;
            for s in self.starts(t) {
                let end = s.saturating_add(size);
                if end.saturating_add(self.lateness) <= w {
                    continue;
                }
                placed = true;
                slots
                    .entry((groups[r], keys[r].clone(), s))
                    .or_default()
                    .push(r);
            }
            if !placed {
                out.rejected.push((
                    rows_of(r),
                    self.reason("late: its windows closed past their allowed lateness"),
                ));
            }
        }
        if slots.is_empty() {
            return Ok(Vec::new());
        }
        let names: Vec<Vec<u8>> = slots.keys().map(|(_, k, s)| slot_key(k, *s)).collect();
        let refs: Vec<KeyRef<'_>> = slots
            .keys()
            .zip(&names)
            .map(|((g, _, _), n)| KeyRef::new(*g, n))
            .collect();
        let got = state
            .get(self.id, &refs)
            .await
            .map_err(|e| state_error(&e))?;
        let mut again = Vec::new();
        for (((g, k, s), rs), value) in slots.into_iter().zip(got) {
            let end = s.saturating_add(size);
            let fresh = value.is_none();
            let mut st = match value {
                Some(v) => self.decode_state(&v)?,
                None => State::new(&self.specs),
            };
            for &r in &rs {
                for (i, acc) in st.accs.iter_mut().enumerate() {
                    if passes(filters[i].as_ref(), r) {
                        acc.update(args[i].as_deref(), r);
                    }
                }
            }
            let name = slot_key(&k, s);
            let closed = end <= w;
            if fresh {
                let timer = match closed {
                    true => EXPIRE,
                    false => CLOSE,
                };
                state.set_timer(
                    self.id,
                    KeyRef::new(g, &timer_key(timer, &k, s)),
                    self.timer_at(match timer {
                        CLOSE => end,
                        _ => end.saturating_add(self.lateness),
                    }),
                );
                if !closed && self.lateness > 0 {
                    state.set_timer(
                        self.id,
                        KeyRef::new(g, &timer_key(EXPIRE, &k, s)),
                        self.timer_at(end.saturating_add(self.lateness)),
                    );
                }
            }
            match closed {
                true => again.push(Emit {
                    group: g,
                    key: k,
                    start: s,
                    end,
                    state: st,
                    merged: false,
                }),
                false => state.put(self.id, KeyRef::new(g, &name), self.encode_state(&st)),
            }
        }
        Ok(again)
    }

    /// Sessions take their rows, merging as they meet. Gives the sessions
    /// already closed that rows reached, to give again, deletes of the
    /// sessions they merged away, and each key's sessions, to write back
    /// once they're given.
    #[allow(clippy::too_many_arguments)]
    async fn sessions<S: StateBackend>(
        &mut self,
        state: &mut S,
        gap: i64,
        take: &[(usize, i64)],
        groups: &[u32],
        keys: &[Vec<u8>],
        args: &[Option<ArrayRef>],
        filters: &[Option<BooleanArray>],
        rows_of: &(dyn Fn(usize) -> usize + Sync),
        out: &mut Out,
    ) -> Result<(Vec<Emit>, Lists), JobError> {
        let w = self.watermark();
        let mut by_key: BTreeMap<Keys, Vec<(usize, i64)>> = BTreeMap::new();
        for &(r, t) in take {
            by_key
                .entry((groups[r], keys[r].clone()))
                .or_default()
                .push((r, t));
        }
        if by_key.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let refs: Vec<KeyRef<'_>> = by_key.keys().map(|(g, k)| KeyRef::new(*g, k)).collect();
        let got = state
            .get(self.id, &refs)
            .await
            .map_err(|e| state_error(&e))?;
        let mut emits = Vec::new();
        let mut lists = Vec::new();
        for (((g, k), rs), value) in by_key.into_iter().zip(got) {
            let mut list = match &value {
                Some(v) => self.decode_sessions(v)?,
                None => Vec::new(),
            };
            let before: BTreeMap<i64, Session> =
                list.iter().map(|s| (s.start, s.clone())).collect();
            for (r, t) in rs {
                let end = t.saturating_add(gap);
                let meets = |s: &Session| s.start <= end && t <= s.end;
                let (start, last) = list
                    .iter()
                    .filter(|s| meets(s))
                    .fold((t, end), |(a, b), s| (a.min(s.start), b.max(s.end)));
                if last.saturating_add(1).saturating_add(self.lateness) <= w {
                    out.rejected.push((
                        rows_of(r),
                        self.reason("late: its session closed past its allowed lateness"),
                    ));
                    continue;
                }
                let mut merged = State::new(&self.specs);
                let mut rest = Vec::with_capacity(list.len());
                for s in list {
                    match meets(&s) {
                        true if s.start == start => {
                            merged.emitted = s.state.emitted;
                            merged.visible = s.state.visible;
                            merged.merge(s.state);
                        }
                        true => merged.merge(s.state),
                        false => rest.push(s),
                    }
                }
                for (i, acc) in merged.accs.iter_mut().enumerate() {
                    if passes(filters[i].as_ref(), r) {
                        acc.update(args[i].as_deref(), r);
                    }
                }
                rest.push(Session {
                    start,
                    end: last,
                    state: merged,
                });
                rest.sort_by_key(|s| s.start);
                list = rest;
            }
            // Timers of the sessions that changed, and deletes of those
            // merged away; closed sessions that changed give their row
            // again.
            let after: BTreeMap<i64, &Session> = list.iter().map(|s| (s.start, s)).collect();
            for (start, old) in &before {
                let same = after
                    .get(start)
                    .is_some_and(|s| s.end == old.end && s.state == old.state);
                if same {
                    continue;
                }
                state.delete_timer(
                    self.id,
                    KeyRef::new(g, &timer_key(CLOSE, &k, *start)),
                    self.timer_at(old.end.saturating_add(1)),
                );
                state.delete_timer(
                    self.id,
                    KeyRef::new(g, &timer_key(EXPIRE, &k, *start)),
                    self.timer_at(old.end.saturating_add(1).saturating_add(self.lateness)),
                );
                if !after.contains_key(start) && old.state.visible {
                    emits.push(Emit {
                        group: g,
                        key: k.clone(),
                        start: old.start,
                        end: old.end,
                        state: State {
                            emitted: old.state.emitted,
                            visible: true,
                            accs: Vec::new(),
                        },
                        merged: true,
                    });
                }
            }
            for s in &list {
                let unchanged = before
                    .get(&s.start)
                    .is_some_and(|o| o.end == s.end && o.state == s.state);
                if unchanged {
                    continue;
                }
                let close = s.end.saturating_add(1);
                let closed = close <= w;
                if !closed {
                    state.set_timer(
                        self.id,
                        KeyRef::new(g, &timer_key(CLOSE, &k, s.start)),
                        self.timer_at(close),
                    );
                }
                if closed || self.lateness > 0 {
                    state.set_timer(
                        self.id,
                        KeyRef::new(g, &timer_key(EXPIRE, &k, s.start)),
                        self.timer_at(close.saturating_add(self.lateness)),
                    );
                }
                if closed {
                    emits.push(Emit {
                        group: g,
                        key: k.clone(),
                        start: s.start,
                        end: s.end,
                        state: s.state.clone(),
                        merged: false,
                    });
                }
            }
            lists.push((g, k, list));
        }
        Ok((emits, lists))
    }

    /// Moves the watermark to `t`, in microseconds, if it's later: closes
    /// every window it reaches the end of, giving its row, and forgets
    /// those past their allowed lateness.
    pub async fn advance<S: StateBackend>(
        &mut self,
        state: &mut S,
        t: i64,
    ) -> Result<Out, JobError> {
        let mut out = Out::default();
        if t <= self.watermark {
            return Ok(out);
        }
        self.watermark = t;
        loop {
            let timers = state
                .fire(self.id, t, TIMERS)
                .await
                .map_err(|e| state_error(&e))?;
            let more = timers.len() == TIMERS;
            let mut parsed = Vec::with_capacity(timers.len());
            for timer in &timers {
                parsed.push((timer.group, parse_timer(&timer.key)?));
            }
            let emitted = match self.plan.window {
                Window::Session { .. } => self.fire_sessions(state, &parsed).await?,
                _ => self.fire_windows(state, &parsed).await?,
            };
            out.add(emitted)?;
            if !more {
                break;
            }
        }
        Ok(out)
    }

    async fn fire_windows<S: StateBackend>(
        &mut self,
        state: &mut S,
        timers: &[Fired],
    ) -> Result<Out, JobError> {
        let names: Vec<Vec<u8>> = timers
            .iter()
            .map(|(_, (_, k, s))| slot_key(k, *s))
            .collect();
        let refs: Vec<KeyRef<'_>> = timers
            .iter()
            .zip(&names)
            .map(|((g, _), n)| KeyRef::new(*g, n))
            .collect();
        let got = state
            .get(self.id, &refs)
            .await
            .map_err(|e| state_error(&e))?;
        let mut closing = Vec::new();
        // A window both closes and expires once a watermark passes both.
        let mut gone = BTreeSet::new();
        for (((g, (kind, k, s)), name), value) in timers.iter().zip(&names).zip(got) {
            let Some(value) = value else { continue };
            if gone.contains(name) {
                continue;
            }
            let st = self.decode_state(&value)?;
            let end = s.saturating_add(self.size());
            if *kind == EXPIRE
                || self.lateness == 0
                || end.saturating_add(self.lateness) <= self.watermark()
            {
                state.delete(self.id, KeyRef::new(*g, name));
                gone.insert(name.clone());
            }
            if *kind == CLOSE {
                closing.push(Emit {
                    group: *g,
                    key: k.clone(),
                    start: *s,
                    end,
                    state: st,
                    merged: false,
                });
            }
        }
        let (out, states) = self.emit_states(closing)?;
        for e in states {
            let name = slot_key(&e.key, e.start);
            if !gone.contains(&name) {
                state.put(
                    self.id,
                    KeyRef::new(e.group, &name),
                    self.encode_state(&e.state),
                );
            }
        }
        Ok(out)
    }

    async fn fire_sessions<S: StateBackend>(
        &mut self,
        state: &mut S,
        timers: &[Fired],
    ) -> Result<Out, JobError> {
        let mut by_key: BTreeMap<Keys, Vec<(u8, i64)>> = BTreeMap::new();
        for (g, (kind, k, s)) in timers {
            by_key.entry((*g, k.clone())).or_default().push((*kind, *s));
        }
        let refs: Vec<KeyRef<'_>> = by_key.keys().map(|(g, k)| KeyRef::new(*g, k)).collect();
        let got = state
            .get(self.id, &refs)
            .await
            .map_err(|e| state_error(&e))?;
        let w = self.watermark();
        let mut closing = Vec::new();
        let mut lists = Vec::new();
        for (((g, k), fired), value) in by_key.into_iter().zip(got) {
            let Some(value) = value else { continue };
            let mut list = self.decode_sessions(&value)?;
            for (kind, start) in fired {
                let Some(i) = list.iter().position(|s| s.start == start) else {
                    continue;
                };
                if kind == CLOSE {
                    closing.push((g, k.clone(), list[i].clone()));
                }
                let end = list[i].end.saturating_add(1);
                if kind == EXPIRE || self.lateness == 0 || end.saturating_add(self.lateness) <= w {
                    list.remove(i);
                }
            }
            lists.push((g, k, list));
        }
        let emits: Vec<Emit> = closing
            .iter()
            .map(|(g, k, s)| Emit {
                group: *g,
                key: k.clone(),
                start: s.start,
                end: s.end,
                state: s.state.clone(),
                merged: false,
            })
            .collect();
        let (out, states) = self.emit_states(emits)?;
        for e in states {
            if let Some((_, _, list)) = lists
                .iter_mut()
                .find(|(g, k, _)| *g == e.group && *k == e.key)
                && let Some(s) = list.iter_mut().find(|s| s.start == e.start)
            {
                s.state.emitted = e.state.emitted;
                s.state.visible = e.state.visible;
            }
        }
        for (g, k, list) in &lists {
            self.put_sessions(state, *g, k, list);
        }
        Ok(out)
    }

    /// The rows of `windows`: each that passes HAVING as an insert or an
    /// update, and each that the output has but that no longer passes, or
    /// that was merged away (no accumulators), as a delete. Gives the
    /// windows with their counts and visibility updated.
    fn emit_states(&self, windows: Vec<Emit>) -> Result<(Out, Vec<Emit>), JobError> {
        let mut out = Out::default();
        let mut gone = Vec::new();
        let mut live = Vec::new();
        for e in windows {
            match e.merged {
                true => gone.push(e),
                false => live.push(e),
            }
        }
        // Each live window's aggregates; one that doesn't fit keeps its
        // row as it was.
        let mut values: Vec<Vec<Option<Value>>> = Vec::with_capacity(live.len());
        let mut ok = Vec::with_capacity(live.len());
        let mut kept = Vec::new();
        for e in live {
            let mut vs = Vec::with_capacity(self.specs.len());
            let mut why = None;
            for (a, spec) in e.state.accs.iter().zip(&self.specs) {
                match a.value(spec) {
                    Ok(v) => vs.push(v),
                    Err(m) => {
                        why.get_or_insert(m);
                        vs.push(None);
                    }
                }
            }
            match why {
                Some(m) => {
                    let grouped = self.grouped(&[&e], &[vs])?;
                    out.failed.push((render_row(&grouped, 0), self.reason(&m)));
                    kept.push(e);
                }
                None => {
                    values.push(vs);
                    ok.push(e);
                }
            }
        }
        let refs: Vec<&Emit> = ok.iter().collect();
        let grouped = self.grouped(&refs, &values)?;
        let (ran, failed) = steps::run_rows(&self.plan.steps, grouped.clone(), true)
            .map_err(|e| JobError::internal(self.reason(&e.message)))?;
        let failing: BTreeSet<usize> = failed.iter().map(|(r, _)| *r).collect();
        for (r, why) in &failed {
            out.failed
                .push((render_row(&grouped, *r), self.reason(why)));
        }
        let passing: BTreeSet<usize> = ran.rows.iter().copied().collect();
        let mut parts: Vec<(RecordBatch, Vec<(usize, &'static str)>)> = Vec::new();
        let mut updated: Vec<Emit> = Vec::with_capacity(ok.len());
        let mut ups = Vec::with_capacity(ran.rows.len());
        for &r in &ran.rows {
            let op = match ok[r].state.emitted {
                0 => "insert",
                _ => "update",
            };
            ups.push((r, op));
        }
        parts.push((ran.batch, ups));
        let deleted: Vec<usize> = (0..ok.len())
            .filter(|r| !passing.contains(r) && !failing.contains(r) && ok[*r].state.visible)
            .collect();
        if !deleted.is_empty() {
            let rows =
                steps::take(&grouped, &deleted).map_err(|e| JobError::internal(e.message))?;
            let (ran, _) = steps::run_rows(&self.plan.steps, rows, false)
                .map_err(|e| JobError::internal(self.reason(&e.message)))?;
            let dels = ran.rows.iter().map(|&i| (deleted[i], "delete")).collect();
            parts.push((ran.batch, dels));
        }
        // Rows of sessions merged away: their key, and nothing else.
        let mut gone_parts = Vec::new();
        if !gone.is_empty() {
            let refs: Vec<&Emit> = gone.iter().collect();
            let empty: Vec<Vec<Option<Value>>> =
                gone.iter().map(|_| vec![None; self.specs.len()]).collect();
            let rows = self.grouped(&refs, &empty)?;
            let (ran, _) = steps::run_rows(&self.plan.steps, rows, false)
                .map_err(|e| JobError::internal(self.reason(&e.message)))?;
            gone_parts.push(ran);
        }
        let mut batches = Vec::new();
        let mut ids = Vec::new();
        let mut ops: Vec<&str> = Vec::new();
        let mut seqs = Vec::new();
        let mut counts: HashMap<usize, (u32, bool)> = HashMap::new();
        for (batch, rows) in parts {
            for (r, op) in rows {
                let e = &ok[r];
                let n = e.state.emitted + 1;
                ids.extend_from_slice(&self.row_id(e, n));
                ops.push(op);
                seqs.push(i64::from(n));
                counts.insert(r, (n, op != "delete"));
            }
            batches.push(batch);
        }
        for ran in gone_parts {
            for &r in &ran.rows {
                let e = &gone[r];
                let n = e.state.emitted + 1;
                ids.extend_from_slice(&self.row_id(e, n));
                ops.push("delete");
                seqs.push(i64::from(n));
            }
            batches.push(ran.batch);
        }
        for (r, mut e) in ok.into_iter().enumerate() {
            if let Some((n, visible)) = counts.get(&r) {
                e.state.emitted = *n;
                e.state.visible = *visible;
            }
            updated.push(e);
        }
        updated.extend(kept);
        let n: usize = batches.iter().map(RecordBatch::num_rows).sum();
        if n > 0 {
            let batch = arrow_select::concat::concat_batches(&self.schema_of(&batches), &batches)
                .map_err(|e| JobError::internal(e.to_string()))?;
            let ids = FixedSizeBinaryArray::try_new(16, ids.into(), None)
                .map_err(|e| JobError::internal(e.to_string()))?;
            out.rows = Some(Typed {
                batch,
                ids,
                control: Control {
                    op: Some(Arc::new(StringArray::from(ops)) as ArrayRef),
                    seq: Some(Arc::new(Int64Array::from(seqs)) as ArrayRef),
                    unchanged: None,
                },
                cells: Vec::new(),
                rejected: Vec::new(),
                rows: None,
            });
        }
        Ok((out, updated))
    }

    fn schema_of(&self, batches: &[RecordBatch]) -> SchemaRef {
        batches
            .first()
            .map(RecordBatch::schema)
            .unwrap_or_else(|| self.schema.clone())
    }

    /// The grouped rows of `windows`, with their aggregates' values.
    fn grouped(
        &self,
        windows: &[&Emit],
        values: &[Vec<Option<Value>>],
    ) -> Result<RecordBatch, JobError> {
        let bad = |e: String| JobError::internal(self.reason(&e));
        let n = windows.len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.plan.grouped.fields().len());
        let bounds = |f: fn(&Emit) -> i64| -> Vec<Option<Value>> {
            windows.iter().map(|e| Some(Value::Int(f(e)))).collect()
        };
        columns.push(codec::build(&self.time_type, &bounds(|e| e.start)).map_err(bad)?);
        columns.push(codec::build(&self.time_type, &bounds(|e| e.end)).map_err(bad)?);
        let mut keys: Vec<Vec<Option<Value>>> = vec![Vec::with_capacity(n); self.key_types.len()];
        for e in windows {
            let mut at = e.key.as_slice();
            for k in &mut keys {
                k.push(codec::decode(&mut at).map_err(bad)?);
            }
        }
        for (k, t) in keys.iter().zip(&self.key_types) {
            columns.push(codec::build(t, k).map_err(bad)?);
        }
        for (i, spec) in self.specs.iter().enumerate() {
            let vs: Vec<Option<Value>> = values.iter().map(|v| v[i].clone()).collect();
            columns.push(codec::build(&spec.out, &vs).map_err(bad)?);
        }
        RecordBatch::try_new_with_options(
            self.plan.grouped.clone(),
            columns,
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(n)),
        )
        .map_err(|e| bad(e.to_string()))
    }

    /// An output row's ID: of the operator, the window, and how many times
    /// its row has been given.
    fn row_id(&self, e: &Emit, n: u32) -> [u8; 16] {
        let mut b = Vec::with_capacity(32 + e.key.len());
        b.extend_from_slice(b"openetl.window.v1");
        b.extend_from_slice(&self.id.0.to_be_bytes());
        b.extend_from_slice(&e.group.to_be_bytes());
        b.extend_from_slice(&e.key);
        b.extend_from_slice(&e.start.to_be_bytes());
        b.extend_from_slice(&n.to_be_bytes());
        XxHash3_128::oneshot(&b).to_be_bytes()
    }

    fn encode_state(&self, st: &State) -> Bytes {
        let mut b = vec![LAYOUT];
        st.encode(&mut b);
        Bytes::from(b)
    }

    fn decode_state(&self, v: &[u8]) -> Result<State, JobError> {
        let mut at = v;
        let parsed = take::<1>(&mut at).and_then(|[layout]| match layout {
            LAYOUT => State::decode(&self.specs, &mut at),
            other => Err(format!("a window's state of layout {other}")),
        });
        parsed.map_err(|e| JobError::internal(self.reason(&format!("its state: {e}"))))
    }

    fn decode_sessions(&self, v: &[u8]) -> Result<Vec<Session>, JobError> {
        let mut at = v;
        let parsed = (|| {
            let [layout] = take::<1>(&mut at)?;
            if layout != LAYOUT {
                return Err(format!("a key's sessions of layout {layout}"));
            }
            let n = u32::from_be_bytes(take(&mut at)?);
            let mut out = Vec::new();
            for _ in 0..n {
                let start = i64::from_be_bytes(take(&mut at)?);
                let end = i64::from_be_bytes(take(&mut at)?);
                out.push(Session {
                    start,
                    end,
                    state: State::decode(&self.specs, &mut at)?,
                });
            }
            Ok(out)
        })();
        parsed.map_err(|e| JobError::internal(self.reason(&format!("its state: {e}"))))
    }

    fn put_sessions<S: StateBackend>(&self, state: &mut S, g: u32, k: &[u8], list: &[Session]) {
        if list.is_empty() {
            state.delete(self.id, KeyRef::new(g, k));
            return;
        }
        let mut b = vec![LAYOUT];
        b.extend_from_slice(&codec::len32(list.len()).to_be_bytes());
        for s in list {
            b.extend_from_slice(&s.start.to_be_bytes());
            b.extend_from_slice(&s.end.to_be_bytes());
            s.state.encode(&mut b);
        }
        state.put(self.id, KeyRef::new(g, k), Bytes::from(b));
    }
}

/// Each key's sessions, by its group and keys.
type Lists = Vec<(u32, Vec<u8>, Vec<Session>)>;

/// Each aggregate's argument and filter, on some rows.
type Arguments = (Vec<Option<ArrayRef>>, Vec<Option<BooleanArray>>);

/// A key group and group keys.
type Keys = (u32, Vec<u8>);

/// A timer that fired: its key group, and what it does to which window.
type Fired = (u32, (u8, Vec<u8>, i64));

/// A window whose row is given, or a session merged away.
struct Emit {
    group: u32,
    key: Vec<u8>,
    start: i64,
    end: i64,
    state: State,
    /// A session merged into another: its row is deleted.
    merged: bool,
}

/// A window's state's key: its group keys, then its start.
fn slot_key(k: &[u8], start: i64) -> Vec<u8> {
    let mut b = Vec::with_capacity(k.len() + 8);
    b.extend_from_slice(k);
    b.extend_from_slice(&start.to_be_bytes());
    b
}

/// A timer's key: what it does, the window's group keys and its start.
fn timer_key(kind: u8, k: &[u8], start: i64) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + k.len() + 8);
    b.push(kind);
    b.extend_from_slice(k);
    b.extend_from_slice(&start.to_be_bytes());
    b
}

fn parse_timer(k: &[u8]) -> Result<(u8, Vec<u8>, i64), JobError> {
    let bad = || JobError::internal("a group window's timer that isn't one");
    let (&kind, rest) = k.split_first().ok_or_else(bad)?;
    if rest.len() < 8 {
        return Err(bad());
    }
    let (key, start) = rest.split_at(rest.len() - 8);
    let start = i64::from_be_bytes(start.try_into().map_err(|_| bad())?);
    Ok((kind, key.to_vec(), start))
}

fn is_json(f: &Field) -> bool {
    f.metadata()
        .get(openetl_normalize::EXTENSION_NAME_KEY)
        .map(String::as_str)
        == Some(openetl_normalize::JSON_EXTENSION)
}

/// A JSON column's values as keys take them: one text for equal JSON.
fn canonical_json(a: &ArrayRef) -> ArrayRef {
    let Some(s) = arrow_array::cast::AsArray::as_string_opt::<i32>(a.as_ref()) else {
        return a.clone();
    };
    let out: StringArray = s
        .iter()
        .map(|v| {
            v.map(|t| {
                openetl_normalize::keygroup::canonical_json(t.as_bytes())
                    .unwrap_or_else(|| t.to_owned())
            })
        })
        .collect();
    Arc::new(out)
}

/// The state layer's operator for a plan's operator ID.
pub fn operator(id: &str) -> OperatorId {
    let h = twox_hash::XxHash3_64::oneshot(id.as_bytes());
    OperatorId(u32::try_from(h >> 32).unwrap_or_default())
}

fn state_error(e: &openetl_state::Error) -> JobError {
    JobError::internal(format!("a group window's state: {e}"))
}

/// `a` then `b`, of one schema.
fn concat(a: &Typed, b: &Typed) -> Result<Typed, JobError> {
    let bad = |e: arrow_schema::ArrowError| JobError::internal(e.to_string());
    let batch = arrow_select::concat::concat_batches(&a.batch.schema(), [&a.batch, &b.batch])
        .map_err(bad)?;
    let ids = arrow_select::concat::concat(&[&a.ids, &b.ids]).map_err(bad)?;
    let pair = |x: &Option<ArrayRef>, y: &Option<ArrayRef>| -> Result<Option<ArrayRef>, JobError> {
        match (x, y) {
            (Some(x), Some(y)) => arrow_select::concat::concat(&[x.as_ref(), y.as_ref()])
                .map(Some)
                .map_err(bad),
            _ => Ok(None),
        }
    };
    Ok(Typed {
        batch,
        ids: arrow_array::cast::AsArray::as_fixed_size_binary(ids.as_ref()).clone(),
        control: Control {
            op: pair(&a.control.op, &b.control.op)?,
            seq: pair(&a.control.seq, &b.control.seq)?,
            unchanged: None,
        },
        cells: Vec::new(),
        rejected: Vec::new(),
        rows: None,
    })
}
