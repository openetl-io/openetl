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

mod frame;
mod row;
#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, RecordBatch, RecordBatchOptions,
    UInt32Array,
};
use arrow_schema::{DataType, Schema, SchemaRef, TimeUnit};
use bytes::Bytes;
use openetl_normalize::{Cause, Code, Control, Typed, key_groups, render_row};
use openetl_proto::connector::v1::ErrorCode;
use openetl_sql::{OverKind, Step};
use openetl_state::{DueRow, KeyRef, OperatorId, StateBackend};

use self::frame::{Func, Got, Kept, Peer};
use crate::error::JobError;
use crate::window::codec::{self, Value, take};
use crate::window::{Out, steps};

/// Rows taken from state at once.
const TAKE: usize = 1024;
/// Rows an advance gives before it lets its caller take them.
const RELEASE: usize = 8192;

/// The layout of a key's frame, which the operator's ID covers.
const LAYOUT: u8 = openetl_sql::OVER_STATE;

/// What a value in the operator's state is: a key's frame, or a schema its
/// rows are written under.
const FRAME: u8 = 1;
const SCHEMA: u8 = 2;

/// `OVER` of a statement, on one lane's key groups.
pub struct Operator {
    pub id: OperatorId,
    plan: Arc<openetl_sql::Over>,
    /// The statement's place and output, for messages.
    at: String,
    output: String,
    funcs: Vec<Func>,
    key_names: Vec<String>,
    key_groups: u32,
    /// The event time's unit per microsecond: 1, or 1000 for nanoseconds.
    per_micro: i64,
    /// In microseconds; `i64::MIN` until it advances.
    watermark: i64,
    /// Rows are due that it hasn't given yet.
    due: bool,
    /// Rows taken from state that the next take may add peers to.
    held: Vec<DueRow>,
    /// The hash of the schema it writes rows under, and the schema.
    schema: (u64, Bytes),
    /// The schemas of rows it has read or written, by their hashes.
    schemas: HashMap<u64, SchemaRef>,
    /// The key groups whose state holds the schema it writes rows under.
    registered: HashSet<u32>,
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
        let per_micro = match plan.rows.field(plan.time).data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, _) => 1000,
            _ => 1,
        };
        let (fp, ipc) = row::fingerprint(&plan.rows).map_err(JobError::internal)?;
        Ok(Self {
            id: crate::window::operator(id),
            at: at.to_string(),
            output: output.to_owned(),
            funcs: plan.functions.iter().map(Func::of).collect(),
            key_names: plan
                .keys
                .iter()
                .map(|k| plan.rows.field(k.column).name().clone())
                .collect(),
            key_groups,
            per_micro,
            watermark: i64::MIN,
            due: false,
            held: Vec::new(),
            schema: (fp, Bytes::from(ipc)),
            schemas: HashMap::from([(fp, plan.rows.clone())]),
            registered: HashSet::new(),
            plan,
        })
    }

    /// Its watermark in microseconds, if it has advanced.
    pub fn watermark_micros(&self) -> Option<i64> {
        (self.watermark != i64::MIN).then_some(self.watermark)
    }

    /// The watermark `wm` as rows tell it apart: whether a row is late
    /// depends on every microsecond of it.
    pub fn level(&self, wm: i64) -> i64 {
        wm
    }

    fn reason(&self, why: &str) -> String {
        format!("transform.sql {}, writing {}: {why}", self.at, self.output)
    }

    /// The latest event time, in its unit, of the rows that have left: the
    /// watermark less one, or every time at the end of input.
    fn left(&self) -> i64 {
        match self.watermark {
            i64::MIN => i64::MIN,
            i64::MAX => i64::MAX,
            w => w.saturating_mul(self.per_micro).saturating_sub(1),
        }
    }

    /// Whether a row at `t` in key group `g` is late: the watermark has
    /// passed it, or the rows of its time there have left, as a group a
    /// restore brought at a later horizon had.
    fn late<S: StateBackend>(&self, state: &S, g: u32, t: i64) -> bool {
        let h = state.horizon(self.id, g);
        t <= self.left() || (h > i64::MIN && t <= h)
    }

    /// Takes a batch of the statement's input, the columns it reads, with
    /// each row's record ID: each row waits for the watermark, or is
    /// rejected.
    pub async fn rows<S: StateBackend>(
        &mut self,
        state: &mut S,
        input: &RecordBatch,
        ids: &FixedSizeBinaryArray,
    ) -> Result<Out, JobError> {
        let plan = self.plan.clone();
        let (ran, mut out) = steps::before(&plan.input.steps, input, |why| self.reason(why))?;
        let rows = ran.batch;
        let n = rows.num_rows();
        if n == 0 {
            return Ok(out);
        }
        let ids = ids_of(ids, &ran.rows, unnests(&plan.input.steps))
            .map_err(|e| JobError::internal(self.reason(&e)))?;
        let (groups, keys) = self.keys(&rows, input)?;
        let times = rows.column(plan.time).clone();
        let ids: Vec<Option<&[u8]>> = ids.iter().collect();
        let mut buf = Vec::new();
        for r in 0..n {
            let t = match Value::read(times.as_ref(), r) {
                Some(Value::Int(t)) => t,
                _ => {
                    out.rejected.push((
                        ran.rows[r],
                        self.reason("its event time is null"),
                        Cause::new(Code::EventTime),
                    ));
                    continue;
                }
            };
            let g = groups[r];
            if self.late(state, g, t) {
                out.rejected.push((
                    ran.rows[r],
                    self.reason("late: the watermark passed its event time before it came"),
                    Cause::new(Code::Late),
                ));
                continue;
            }
            if self.registered.insert(g) {
                let (fp, ipc) = &self.schema;
                state.put(self.id, KeyRef::new(g, &schema_key(*fp)), ipc.clone());
            }
            buf.clear();
            row::encode(&rows, ids[r], r, self.schema.0, &mut buf)
                .map_err(|e| JobError::internal(self.reason(&e)))?;
            state.append(
                self.id,
                KeyRef::new(g, &keys[r]),
                t,
                Bytes::copy_from_slice(&buf),
            );
        }
        out.rejected.sort_by_key(|(r, ..)| *r);
        Ok(out)
    }

    /// Each row's key group and key: `PARTITION BY`'s values, equal JSON
    /// one key whatever its spaces or its objects' order, in the key group
    /// the exchange before the operator gives it.
    fn keys(
        &self,
        rows: &RecordBatch,
        input: &RecordBatch,
    ) -> Result<(Vec<u32>, Vec<Vec<u8>>), JobError> {
        let n = rows.num_rows();
        let plan = &self.plan;
        let columns: Vec<ArrayRef> = plan
            .keys
            .iter()
            .map(|g| {
                let c = rows.column(g.column).clone();
                let json = crate::window::is_json(rows.schema().field(g.column))
                    || input
                        .schema()
                        .field_with_name(&g.stream)
                        .is_ok_and(crate::window::is_json);
                match json {
                    true => crate::window::canonical_json(&c),
                    false => c,
                }
            })
            .collect();
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::new(
                plan.keys
                    .iter()
                    .map(|g| rows.schema().field(g.column).clone())
                    .collect::<Vec<_>>(),
            )),
            columns.clone(),
            &RecordBatchOptions::new().with_row_count(Some(n)),
        )
        .map_err(|e| JobError::internal(self.reason(&e.to_string())))?;
        let groups = key_groups(&batch, &self.key_names, self.key_groups)
            .map_err(|e| JobError::internal(self.reason(&e.to_string())))?;
        let keys = (0..n)
            .map(|r| {
                let mut k = Vec::new();
                for c in &columns {
                    codec::encode(Value::read(c.as_ref(), r).as_ref(), &mut k);
                }
                k
            })
            .collect();
        Ok((groups, keys))
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
        if t > self.watermark {
            self.watermark = t;
            self.due = true;
        }
        let mut out = Out::default();
        if !self.due {
            return Ok((out, false));
        }
        let upto = self.left();
        let mut given = 0;
        loop {
            let taken = state
                .take_rows(self.id, upto, TAKE)
                .await
                .map_err(|e| state_error(&e))?;
            let full = taken.len() == TAKE;
            let prior = self.held.len();
            let mut rows = std::mem::take(&mut self.held);
            rows.extend(taken);
            // The next take may hold more of the last key's rows at its
            // time, its peers, which leave together. The rows held are one
            // key's peers already, so only the rows just taken can end them.
            if full && let Some(last) = rows.last() {
                let (g, t, k) = (last.group, last.time, last.key.clone());
                let differs = |r: &DueRow| r.group != g || r.time != t || r.key != k;
                let at = match rows[prior..].iter().rposition(differs) {
                    Some(i) => prior + i + 1,
                    None if prior > 0 && differs(&rows[prior - 1]) => prior,
                    None => 0,
                };
                self.held = rows.split_off(at);
            }
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
        self.due = false;
        Ok((out, false))
    }

    /// Gives rows that left state: each key's in order, with each
    /// function's value, through the steps after the operator.
    async fn release<S: StateBackend>(
        &mut self,
        state: &mut S,
        mut due: Vec<DueRow>,
    ) -> Result<Out, JobError> {
        let mut out = Out::default();
        if due.is_empty() {
            return Ok(out);
        }
        due.sort_by(|a, b| {
            (a.group, &a.key, a.time, row::tie(&a.row)).cmp(&(
                b.group,
                &b.key,
                b.time,
                row::tie(&b.row),
            ))
        });
        let (rows, ids) = self.decode(state, &due).await?;
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
        let ids = FixedSizeBinaryArray::try_from_iter(at.iter().map(|&r| ids[r]))
            .map_err(|e| JobError::internal(e.to_string()))?;
        let times = arrow_select::take::take(
            rows.column(self.plan.time).as_ref(),
            &UInt32Array::from_iter_values(at.iter().map(|&r| r as u32)),
            None,
        )
        .map_err(|e| JobError::internal(e.to_string()))?;
        out.rows = Some(Typed {
            batch: ran.batch,
            ids,
            control: Control {
                op: None,
                seq: Some(times),
                unchanged: None,
            },
            cells: Vec::new(),
            rejected: Vec::new(),
            rows: None,
        });
        Ok(out)
    }

    /// The rows of `due`, in order, as the plan's input to the operator,
    /// and their record IDs: each read under the schema it was written
    /// with, which state keeps beside it.
    async fn decode<S: StateBackend>(
        &mut self,
        state: &mut S,
        due: &[DueRow],
    ) -> Result<(RecordBatch, Vec<[u8; 16]>), JobError> {
        let mut by: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
        for (i, d) in due.iter().enumerate() {
            let fp = row::schema_of(&d.row).map_err(|e| self.corrupt(&e))?;
            by.entry(fp).or_default().push(i);
        }
        let mut parts = Vec::with_capacity(by.len());
        let mut ids = vec![[0u8; 16]; due.len()];
        let mut order = vec![0u32; due.len()];
        let mut at = 0u32;
        for (fp, rows) in &by {
            let schema = self.schema_named(state, *fp, due[rows[0]].group).await?;
            let bytes: Vec<&[u8]> = rows.iter().map(|&i| due[i].row.as_ref()).collect();
            let (batch, got) = row::decode(&bytes, &schema).map_err(|e| self.corrupt(&e))?;
            let batch = row::conform(&batch, &self.plan.rows).map_err(|(column, why)| {
                JobError::new(
                    ErrorCode::SchemaDrift,
                    self.reason(&format!(
                        "rows that waited in state can't be read as the statement reads them now: column {column} changed {why}. Reset the stream it reads to start OVER's state again: openetl reset --stream STREAM NAME"
                    )),
                )
            })?;
            for (&i, id) in rows.iter().zip(got) {
                ids[i] = id;
                order[i] = at;
                at += 1;
            }
            parts.push(batch);
        }
        let all = match parts.len() {
            1 => parts.remove(0),
            _ => arrow_select::concat::concat_batches(&parts[0].schema(), &parts)
                .map_err(|e| JobError::internal(e.to_string()))?,
        };
        let rows = arrow_select::take::take_record_batch(&all, &UInt32Array::from(order))
            .map_err(|e| JobError::internal(e.to_string()))?;
        let schema = Arc::new(Schema::new_with_metadata(
            self.plan.rows.fields().clone(),
            self.plan.rows.metadata().clone(),
        ));
        let rows = RecordBatch::try_new_with_options(
            schema,
            rows.columns().to_vec(),
            &RecordBatchOptions::new().with_row_count(Some(rows.num_rows())),
        )
        .map_err(|e| JobError::internal(self.reason(&e.to_string())))?;
        Ok((rows, ids))
    }

    /// The schema rows of hash `fp` are written under, from state's key
    /// group `g` where it isn't known yet.
    async fn schema_named<S: StateBackend>(
        &mut self,
        state: &mut S,
        fp: u64,
        g: u32,
    ) -> Result<SchemaRef, JobError> {
        if let Some(s) = self.schemas.get(&fp) {
            return Ok(s.clone());
        }
        let key = schema_key(fp);
        let got = state
            .get(self.id, &[KeyRef::new(g, &key)])
            .await
            .map_err(|e| state_error(&e))?;
        let Some(Some(ipc)) = got.into_iter().next() else {
            return Err(self.corrupt(&format!("rows of a schema {fp:016x} it doesn't hold")));
        };
        let schema = crate::ipc::decode_schema(&ipc).map_err(|e| self.corrupt(&e.to_string()))?;
        self.schemas.insert(fp, schema.clone());
        Ok(schema)
    }

    fn corrupt(&self, why: &str) -> JobError {
        JobError::new(
            ErrorCode::CheckpointCorrupt,
            self.reason(&format!("its state: {why}")),
        )
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
        parsed.map_err(|e| self.corrupt(&e))
    }
}

/// A key's frame's key in state.
fn frame_key(k: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + k.len());
    b.push(FRAME);
    b.extend_from_slice(k);
    b
}

/// The key in state of the schema rows of hash `fp` are written under.
fn schema_key(fp: u64) -> Vec<u8> {
    let mut b = vec![SCHEMA];
    b.extend_from_slice(&fp.to_be_bytes());
    b
}

/// Whether the steps before the operator unnest, so a row may give
/// several.
fn unnests(steps: &[Step]) -> bool {
    steps.iter().any(|s| matches!(s, Step::Unnest(_)))
}

/// The record IDs of the rows the steps before the operator gave, from
/// their input rows' `ids` at `rows`. Where they unnest, the k-th row of
/// an input row has an ID of its own, derived from that row's.
fn ids_of(
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

fn state_error(e: &openetl_state::Error) -> JobError {
    crate::state::state_error("OVER's state", e)
}
