//! Keyed lanes: where a group window's statement runs. A lane that reads a
//! window's stream sends each typed row to the keyed lane owning the key
//! group of the window's keys, and its barriers, watermarks and end to every
//! keyed lane: the exchange before the statement. Keyed lanes are the
//! attempt's lanes' twins, each owning its lane's key groups: inside one
//! engine they're its channels, and with a mesh, other engines' are reached
//! over their links (see [`crate::mesh`]). Each keyed lane aligns every
//! sender of the attempt on each barrier, as a writer lane does, runs the
//! window on its key groups' state, then names and holds the output as a
//! lane does and sends it to its own writer lane, in its own engine: its
//! range is inside that writer lane's, so an output keyed by the window's
//! keys needs no second exchange.
//!
//! The watermark of a window is the least of its senders', each the
//! stream's on that lane, which comes after the rows that moved it, from
//! the stream's floor: where the record's splits were, so a window that
//! closed before a restore or a new read stays closed. Windows close as it
//! passes them, in the epoch it does, and the keyed lane passes it on to
//! its writer lane as the output's, and reports it at each barrier.
//!
//! A redo of one writer lane's part of an epoch keeps the output's key
//! groups alone (Plan::keep): only rows of the window's keys in them reach a
//! keyed lane, whose state is of those, and its windows stay at the floor
//! while the epoch is read again, to close at the barrier by the watermark
//! the epoch had there (EventTime::closes_at).

use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::RangeInclusive;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, RecordBatch, RecordBatchOptions, StringArray, UInt32Array, new_null_array,
};
use arrow_schema::{DataType, Field, Schema};
use openetl_normalize::{Cause, Code, NotHeld, Rejected, Typed, key_groups, render_row};
use openetl_proto::connector::v1::{Epoch, ErrorCode};
use openetl_proto::engine::v1::{Barrier, FrameKind, OperatorEpoch, StateRequests, Watermark};
use openetl_state::runs::RunName;
use openetl_state::{KeyGroupRange, Layer};
use prost::Message;
use tokio::sync::mpsc;

use super::{
    Flow, Lane, LaneStream, Quarantined, copy, hold, quarantine_file, shape_error, shaped,
};
use crate::error::JobError;
use crate::frame;
use crate::ipc;
use crate::mesh::Target;
use crate::plan::Plan;
use crate::watermark::{self, Senders};
use crate::window::{Operator, Out};

/// What a lane sends a keyed lane, for a window's statement by its index.
pub(crate) enum In {
    /// Rows of the statement's stream: the columns it reads. Each row the
    /// statement can't read, as a cell the stream holds, has the cell's
    /// column and why it's held.
    Rows(usize, RecordBatch, Vec<Option<(String, String)>>),
    /// The sender's watermark of the statement's stream, in microseconds:
    /// every row before it has come, but late ones. It never goes back.
    Watermark(usize, i64),
    Barrier(usize, Epoch),
    End(usize),
}

impl In {
    fn statement(&self) -> usize {
        match self {
            In::Rows(s, ..) | In::Watermark(s, _) | In::Barrier(s, _) | In::End(s) => *s,
        }
    }
}

/// The columns a BATCH to another engine's keyed lane carries after the
/// statement's: each row's held cell, its column and why.
const HELD_COLUMN: &str = "\0held_column";
const HELD_REASON: &str = "\0held_reason";

/// A frame to another engine's keyed lane: its kind, statement and
/// payload.
pub(crate) fn wire(item: In) -> Result<(FrameKind, u32, Vec<u8>), JobError> {
    let index = |s: usize| u32::try_from(s).map_err(|_| JobError::internal("a statement past u32"));
    Ok(match item {
        In::Rows(s, batch, held) => {
            let schema = batch.schema();
            let mut fields: Vec<Field> =
                schema.fields().iter().map(|f| f.as_ref().clone()).collect();
            fields.push(Field::new(HELD_COLUMN, DataType::Utf8, true));
            fields.push(Field::new(HELD_REASON, DataType::Utf8, true));
            let mut columns = batch.columns().to_vec();
            let of = |f: fn(&(String, String)) -> &str| -> Arc<dyn Array> {
                Arc::new(
                    held.iter()
                        .map(|h| h.as_ref().map(f))
                        .collect::<StringArray>(),
                )
            };
            columns.push(of(|h| &h.0));
            columns.push(of(|h| &h.1));
            let b = RecordBatch::try_new_with_options(
                Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
            )
            .map_err(|e| JobError::internal(e.to_string()))?;
            (
                FrameKind::Batch,
                index(s)?,
                ipc::encode_batch(&b).map_err(|e| JobError::internal(e.to_string()))?,
            )
        }
        In::Watermark(s, micros) => (
            FrameKind::Watermark,
            index(s)?,
            Watermark { micros }.encode_to_vec(),
        ),
        In::Barrier(s, e) => (
            FrameKind::Barrier,
            index(s)?,
            Barrier { epoch: Some(e) }.encode_to_vec(),
        ),
        In::End(s) => (FrameKind::EndOfStream, index(s)?, Vec::new()),
    })
}

/// What `wire` made of a frame, from an engine of generation `generation`.
pub(crate) fn unwire(f: frame::Frame, generation: u64) -> Result<In, String> {
    let s = f.stream as usize;
    match f.kind {
        FrameKind::Batch => {
            let b = ipc::decode_batch(f.payload).map_err(|e| e.to_string())?;
            let n = b.num_columns();
            let schema = b.schema();
            if n < 2
                || schema.field(n - 2).name() != HELD_COLUMN
                || schema.field(n - 1).name() != HELD_REASON
            {
                return Err("a batch for a keyed lane without its held cells".into());
            }
            let (Some(column), Some(reason)) = (
                b.column(n - 2).as_string_opt::<i32>(),
                b.column(n - 1).as_string_opt::<i32>(),
            ) else {
                return Err("a batch for a keyed lane whose held cells aren't strings".into());
            };
            let held = column
                .iter()
                .zip(reason.iter())
                .map(|(c, r)| Some((c?.to_owned(), r?.to_owned())))
                .collect();
            let rows = b
                .project(&(0..n - 2).collect::<Vec<_>>())
                .map_err(|e| e.to_string())?;
            Ok(In::Rows(s, rows, held))
        }
        FrameKind::Watermark => Ok(In::Watermark(
            s,
            Watermark::decode(f.payload.as_slice())
                .map_err(|e| e.to_string())?
                .micros,
        )),
        FrameKind::Barrier => {
            let e = Barrier::decode(f.payload.as_slice())
                .map_err(|e| e.to_string())?
                .epoch
                .unwrap_or_default();
            if e.generation != generation {
                return Err(format!(
                    "a barrier of generation {}, not {generation}",
                    e.generation
                ));
            }
            Ok(In::Barrier(s, e))
        }
        FrameKind::EndOfStream => Ok(In::End(s)),
        k => Err(format!("a {} frame for a keyed lane", k.as_str_name())),
    }
}

/// The group windows' statements, by their index in the transform.
pub(crate) fn statements(plan: &Plan) -> Vec<usize> {
    plan.transform.as_ref().map_or_else(Vec::new, |t| {
        (0..t.sql.statements.len())
            .filter(|&i| t.sql.statements[i].window.is_some())
            .collect()
    })
}

/// The output a statement writes, by its index in the plan's streams.
fn output_of(plan: &Plan, st: &crate::transform::Statement) -> Option<usize> {
    plan.streams.iter().position(|s| *s == st.output)
}

/// The lane of `lanes`, contiguous ranges in order, that owns key group g.
fn lane_of(lanes: &[RangeInclusive<u32>], g: u32) -> usize {
    lanes
        .partition_point(|r| *r.end() < g)
        .min(lanes.len().saturating_sub(1))
}

/// Sends the window statements' rows of a typed batch of their stream to
/// the keyed lanes owning their keys' groups: those the output keeps.
pub(super) async fn rows(lane: &Lane, statements: &[usize], typed: &Typed) -> Result<(), JobError> {
    let plan = &lane.flow.plan;
    let Some(t) = &plan.transform else {
        return Ok(());
    };
    for &i in statements {
        let st = &t.sql.statements[i];
        let Some((window, _)) = &st.window else {
            continue;
        };
        let batch = reads(st, &typed.batch, &t.sql.inputs[st.input].name)?;
        let mut held: Vec<Option<(String, String)>> = vec![None; batch.num_rows()];
        // A cell the stream holds is no value only for a column the
        // statement reads.
        for c in typed
            .cells
            .iter()
            .filter(|c| st.op.reads.column_with_name(&c.column).is_some())
        {
            if let Some(h) = held.get_mut(c.row) {
                h.get_or_insert_with(|| (c.column.clone(), c.reason.clone()));
            }
        }
        let groups = key_groups(&batch, &window.need, plan.key_groups)
            .map_err(|e| shape_error(&t.sql.inputs[st.input].name, e.into()))?;
        let keep = output_of(plan, st).and_then(|o| plan.keep[o]);
        let mut parts: Vec<Vec<u32>> = vec![Vec::new(); plan.attempt_lanes.len()];
        for (r, g) in groups.into_iter().enumerate() {
            if keep.is_some_and(|(first, last)| g < first || g > last) {
                continue;
            }
            parts[lane_of(&plan.attempt_lanes, g)].push(r as u32);
        }
        for (k, rows) in parts.into_iter().enumerate() {
            if rows.is_empty() {
                continue;
            }
            let part = match rows.len() == batch.num_rows() {
                true => batch.clone(),
                false => {
                    arrow_select::take::take_record_batch(&batch, &UInt32Array::from(rows.clone()))
                        .map_err(|e| JobError::internal(e.to_string()))?
                }
            };
            let h = rows.iter().map(|&r| held[r as usize].clone()).collect();
            send(lane, k, In::Rows(i, part, h)).await?;
        }
    }
    Ok(())
}

/// Sends `f(statement)` to every keyed lane of the attempt, for each
/// statement.
pub(super) async fn all(
    lane: &Lane,
    statements: &[usize],
    f: impl Fn(usize) -> In,
) -> Result<(), JobError> {
    for &i in statements {
        for k in 0..lane.flow.plan.attempt_lanes.len() {
            send(lane, k, f(i)).await?;
        }
    }
    Ok(())
}

/// Sends to the keyed lane of the attempt's lane `k`, from the first of the
/// plan's senders: the engine's own, or another's over its link.
async fn send(lane: &Lane, k: usize, item: In) -> Result<(), JobError> {
    let plan = &lane.flow.plan;
    let to = plan.senders().start + k;
    let from = plan.first_lane + lane.lane;
    let Some(local) = to
        .checked_sub(plan.first_lane)
        .filter(|&l| l < lane.keyed.len())
    else {
        return lane.remote(Target::Keyed(to), wire(item)?).await;
    };
    let stopped = || JobError::stopped("a keyed lane stopped");
    let item = match lane.keyed[local].try_send((from, item)) {
        Ok(()) => return Ok(()),
        Err(mpsc::error::TrySendError::Full(item)) => item,
        Err(mpsc::error::TrySendError::Closed(_)) => return Err(stopped()),
    };
    lane.enter(super::Doing::Backpressured);
    let sent = lane.keyed[local].send(item).await.map_err(|_| stopped());
    lane.enter(super::Doing::Busy);
    sent
}

/// The columns a statement reads, from a batch of its stream: null where
/// the batch lacks one. One of another type changes what the window keeps,
/// which needs its state built again.
fn reads(
    st: &crate::transform::Statement,
    batch: &RecordBatch,
    stream: &str,
) -> Result<RecordBatch, JobError> {
    let n = batch.num_rows();
    let schema = batch.schema();
    let mut fields = Vec::with_capacity(st.op.reads.fields().len());
    let mut columns = Vec::with_capacity(fields.capacity());
    for f in st.op.reads.fields() {
        let column = match batch.column_by_name(f.name()) {
            Some(c) if c.data_type() == f.data_type() => c.clone(),
            Some(c) => {
                return Err(JobError::new(
                    ErrorCode::SchemaDrift,
                    format!(
                        "transform.sql: {}: stream {stream}'s column {} changed from {} to {}, and the group window writing {} keeps state of what it read: the pipeline's operator needs its state built again from the stream",
                        st.at,
                        f.name(),
                        f.data_type(),
                        c.data_type(),
                        st.output
                    ),
                ));
            }
            None => new_null_array(f.data_type(), n),
        };
        // A column keeps what its field says of it, as that it's JSON,
        // which its key group goes by.
        let mut metadata = f.metadata().clone();
        if let Ok(b) = schema.field_with_name(f.name()) {
            metadata.extend(b.metadata().clone());
        }
        fields.push(Field::new(f.name(), f.data_type().clone(), true).with_metadata(metadata));
        columns.push(column);
    }
    RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        columns,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(n)),
    )
    .map_err(|e| JobError::internal(e.to_string()))
}

/// How a sender stands at a keyed lane, for one statement.
#[derive(Clone, Copy, PartialEq)]
enum Standing {
    Open,
    AtBarrier(Epoch),
    Ended,
}

/// One window's statement on a keyed lane.
struct Keyed {
    statement: usize,
    output: usize,
    op: Operator,
    layer: Layer,
    ls: LaneStream,
    standing: Vec<Standing>,
    /// What each sender sent past its barrier, until every sender is there.
    waiting: Vec<VecDeque<In>>,
    watermarks: Senders,
    /// The window's watermark: where it has closed windows up to.
    wm: i64,
    /// A redo's: where it closes windows at the barrier, its senders'
    /// watermarks moving nothing before.
    closes_at: Option<i64>,
    ended: bool,
    /// What it did since its last barrier.
    counts: Counts,
}

/// What a window's statement did on a keyed lane in an epoch.
#[derive(Default)]
struct Counts {
    rows_in: u64,
    inserts: u64,
    updates: u64,
    deletes: u64,
    late: u64,
}

/// Runs keyed lane `lane.lane` until every window's stream has ended on
/// every sender of the attempt.
pub(super) async fn run(
    flow: Arc<Flow>,
    lane: Lane,
    mut rx: mpsc::Receiver<(usize, In)>,
) -> Result<(), JobError> {
    let plan = &flow.plan;
    let Some(t) = &plan.transform else {
        return Ok(());
    };
    let senders = plan.senders();
    let owned = plan.layout.lane(lane.lane);
    let mut windows: Vec<Keyed> = Vec::new();
    for (i, st) in t.sql.statements.iter().enumerate() {
        let Some((window, id)) = &st.window else {
            continue;
        };
        let Some(output) = output_of(plan, st) else {
            continue;
        };
        let op = Operator::new(
            id,
            st.at,
            window.clone(),
            &st.output,
            st.schema.clone(),
            t.sql.inputs[st.input].lateness,
            plan.key_groups,
        )?;
        // A redo keeps the rows of its key groups alone, and so the state.
        let range = match plan.keep[output] {
            None => Some(owned.clone()),
            Some((first, last)) => Some(first.max(*owned.start())..=last.min(*owned.end()))
                .filter(|r| r.start() <= r.end()),
        };
        // A record names an operator's state on every lane, or on none
        // where none has written any: then the lane starts empty.
        let named: Vec<&openetl_state::Snapshot> = plan
            .state
            .restored
            .iter()
            .filter(|(o, _)| o == id)
            .map(|(_, s)| s)
            .collect();
        let cant_open = |why: String| {
            let stream = &t.sql.inputs[st.input].name;
            format!(
                "transform.sql {}: the group window writing {} can't open the state the checkpoint names, {why}. Reset stream {stream} to build the state again from a new read of it: openetl reset --stream {stream} NAME",
                st.at, st.output
            )
        };
        let (range, restored) = match range {
            Some(r) => {
                let range = KeyGroupRange::new(*r.start(), *r.end());
                if let Some(g) = crate::state::uncovered(&named, range) {
                    return Err(JobError::new(
                        ErrorCode::CheckpointCorrupt,
                        cant_open(format!("as none of its snapshots holds key group {g}")),
                    ));
                }
                let restored: Vec<openetl_state::Snapshot> = named
                    .into_iter()
                    .filter(|s| s.range.overlaps(&range))
                    .cloned()
                    .collect();
                (range, restored)
            }
            // None of its key groups is kept: no row reaches it, and it
            // keeps nothing.
            None => (KeyGroupRange::new(*owned.start(), *owned.end()), Vec::new()),
        };
        let Some(state) = &plan.state.job else {
            return Err(JobError::internal("a group window without the job's state"));
        };
        let mut layer = Layer::open(
            &state.stores(id),
            plan.generation,
            plan.first_epoch,
            range,
            &restored,
        )
        .await
        .map_err(|e| {
            let err = crate::state::state_error("its state", &e);
            JobError::new(err.code, cant_open(err.message))
        })?;
        op.open(&mut layer);
        let event_time = t.inputs[st.input].event_time.as_ref();
        let floor = event_time.map_or(watermark::NONE, |e| e.floor);
        let closes_at = event_time
            .and_then(|e| e.closes_at)
            .filter(|_| plan.state.job.as_ref().is_some_and(|j| j.read_only()));
        let ls = {
            let o = flow.owners[output].lock();
            LaneStream {
                stream: output,
                shaper: o.shaper.fork(),
                version: o.version,
                quarantined: Quarantined::default(),
                cells: Default::default(),
                replay: None,
            }
        };
        let n = senders.len();
        let mut w = Keyed {
            statement: i,
            output,
            op,
            layer,
            ls,
            standing: vec![Standing::Open; n],
            waiting: (0..n).map(|_| VecDeque::new()).collect(),
            watermarks: Senders::new(n, floor),
            wm: watermark::NONE,
            closes_at,
            ended: false,
            counts: Counts::default(),
        };
        // Windows the record's watermark passed closed before it.
        if floor != watermark::NONE {
            w.advance(&lane, floor).await?;
        }
        windows.push(w);
    }
    loop {
        if windows.iter().all(|w| w.ended) {
            return Ok(());
        }
        lane.enter(super::Doing::Idle);
        let got = rx.recv().await;
        lane.enter(super::Doing::Busy);
        let Some((from, item)) = got else {
            return Err(JobError::stopped("the lanes feeding a keyed lane stopped"));
        };
        let Some(sender) = from
            .checked_sub(senders.start)
            .filter(|&s| s < senders.len())
        else {
            return Err(JobError::internal(format!(
                "lane {from} sent to a keyed lane, whose senders are {senders:?}"
            )));
        };
        let Some(w) = windows.iter_mut().find(|w| w.statement == item.statement()) else {
            return Err(JobError::internal(
                "a keyed lane got rows of a statement it doesn't run",
            ));
        };
        match w.standing[sender] {
            Standing::Open => w.take(&lane, sender, item).await?,
            _ => w.waiting[sender].push_back(item),
        }
        w.align(&lane).await?;
    }
}

impl Keyed {
    /// Takes what an open sender sent.
    async fn take(&mut self, lane: &Lane, sender: usize, item: In) -> Result<(), JobError> {
        match item {
            In::Rows(_, batch, held) => {
                self.counts.rows_in += batch.num_rows() as u64;
                let flow = lane.flow.clone();
                let Some(st) = flow
                    .plan
                    .transform
                    .as_ref()
                    .map(|t| &t.sql.statements[self.statement])
                else {
                    return Err(JobError::internal("a keyed lane without a transform"));
                };
                let mut rejected = Vec::new();
                let keep: Vec<usize> = (0..batch.num_rows())
                    .filter(|&r| match &held[r] {
                        Some((column, why)) => {
                            rejected.push(Rejected {
                                row: r,
                                reason: format!(
                                    "transform.sql {}, writing {}: column {column} has a value that doesn't fit its type, which the stream holds: {why}",
                                    st.at, st.output
                                ),
                                cause: Cause::value(why, column, Some(NotHeld::Transform)),
                            });
                            false
                        }
                        None => true,
                    })
                    .collect();
                self.reject(lane, &batch, rejected)?;
                let input = match keep.len() == batch.num_rows() {
                    true => batch.clone(),
                    false => arrow_select::take::take_record_batch(
                        &batch,
                        &UInt32Array::from_iter_values(keep.iter().map(|&r| r as u32)),
                    )
                    .map_err(|e| JobError::internal(e.to_string()))?,
                };
                let out = self.op.rows(&mut self.layer, &input).await?;
                self.counts.late += out
                    .rejected
                    .iter()
                    .filter(|(_, _, cause)| cause.code == Code::Late)
                    .count() as u64;
                let rejected = out
                    .rejected
                    .iter()
                    .map(|(r, why, cause)| Rejected {
                        row: keep[*r],
                        reason: why.clone(),
                        cause: cause.clone(),
                    })
                    .collect();
                self.reject(lane, &batch, rejected)?;
                self.give(lane, out).await
            }
            In::Watermark(_, t) => {
                let advanced = self
                    .watermarks
                    .take(sender, t)
                    .map_err(|e| JobError::internal(format!("a keyed lane's group window: {e}")))?;
                match advanced {
                    Some(wm) if self.closes_at.is_none() => self.advance(lane, wm).await,
                    _ => Ok(()),
                }
            }
            In::Barrier(_, e) => {
                self.standing[sender] = Standing::AtBarrier(e);
                Ok(())
            }
            In::End(_) => {
                self.standing[sender] = Standing::Ended;
                Ok(())
            }
        }
    }

    /// Closes the windows the watermark `wm` passes, and passes it on to the
    /// output's writer lane after their rows.
    async fn advance(&mut self, lane: &Lane, wm: i64) -> Result<(), JobError> {
        self.wm = wm;
        let out = self.op.advance(&mut self.layer, wm).await?;
        self.give(lane, out).await?;
        let s = self.output;
        lane.all(s, || super::Out::Watermark(s, wm)).await
    }

    /// Quarantines rows of a batch of the input as the output's.
    fn reject(
        &mut self,
        lane: &Lane,
        batch: &RecordBatch,
        rejected: Vec<Rejected>,
    ) -> Result<(), JobError> {
        if rejected.is_empty() {
            return Ok(());
        }
        let flow = &lane.flow;
        hold(
            &flow.plan,
            &flow.plan.streams[self.output],
            &rejected,
            |r| render_row(batch, r),
            &mut self.ls.quarantined,
            &flow.counters[self.output],
        )
    }

    /// Names, holds and sends what the window gave, and quarantines the
    /// windows whose row it couldn't give.
    async fn give(&mut self, lane: &Lane, out: Out) -> Result<(), JobError> {
        let flow = lane.flow.clone();
        let plan = &flow.plan;
        let name = &plan.streams[self.output];
        let c = &flow.counters[self.output];
        if !out.failed.is_empty() {
            let rejected: Vec<Rejected> = out
                .failed
                .iter()
                .enumerate()
                .map(|(i, (_, why, cause))| Rejected {
                    row: i,
                    reason: why.clone(),
                    cause: cause.clone(),
                })
                .collect();
            hold(
                plan,
                name,
                &rejected,
                |i| out.failed[i].0.clone(),
                &mut self.ls.quarantined,
                c,
            )?;
        }
        let Some(typed) = out.rows else {
            return Ok(());
        };
        let records: Vec<String> = (0..typed.batch.num_rows())
            .map(|r| render_row(&typed.batch, r))
            .collect();
        let mut n = self
            .ls
            .shape_own(
                plan,
                &flow.owners[self.output],
                |_| Ok(copy(&typed)),
                |_, e| shape_error(name, e),
            )
            .await?;
        let rejected = std::mem::take(&mut n.rejected);
        self.count_written(&typed, &rejected);
        hold(
            plan,
            name,
            &rejected,
            |r| records[r].clone(),
            &mut self.ls.quarantined,
            c,
        )?;
        shaped(lane, &mut self.ls, n, c).await
    }

    /// Counts the rows of `typed` that the output didn't reject, by op.
    fn count_written(&mut self, typed: &Typed, rejected: &[Rejected]) {
        let gone: HashSet<usize> = rejected.iter().map(|r| r.row).collect();
        let ops: Vec<Option<&str>> = match &typed.control.op {
            Some(a) => a.as_string::<i32>().iter().collect(),
            None => Vec::new(),
        };
        for r in (0..typed.batch.num_rows()).filter(|r| !gone.contains(r)) {
            match ops.get(r).copied().flatten() {
                Some("update") => self.counts.updates += 1,
                Some("delete") => self.counts.deletes += 1,
                _ => self.counts.inserts += 1,
            }
        }
    }

    /// What the operator did since its last barrier, and holds at this one,
    /// `e` of generation `generation`, whose snapshots are `snapshots`;
    /// `memory` is what its state held before the barrier wrote the epoch's
    /// changes. The barrier's request is the run of `e` the lane wrote, if
    /// any, which its snapshots name, and a merge's the output of one that
    /// they name first.
    fn epoch(
        &mut self,
        id: &str,
        (generation, e): (u64, u64),
        snapshots: &[openetl_state::Snapshot],
        memory: usize,
    ) -> Result<OperatorEpoch, JobError> {
        let c = std::mem::take(&mut self.counts);
        let dir = crate::state::dir(id);
        let mut runs = HashMap::new();
        let mut barrier = StateRequests::default();
        let mut merge_write = StateRequests::default();
        for sn in snapshots {
            let named = openetl_state::runs::runs(sn)
                .map_err(|e| crate::state::state_error(&format!("operator {id}'s state"), &e))?;
            for r in named {
                let name = format!("{dir}/{}", r.name);
                let written =
                    RunName::parse(&r.name).filter(|n| n.generation == generation && n.epoch == e);
                if let Some(n) = written
                    && !runs.contains_key(&name)
                {
                    let req = if n.merged {
                        &mut merge_write
                    } else {
                        &mut barrier
                    };
                    req.count += 1;
                    req.bytes += r.size;
                }
                runs.insert(name, r.size);
            }
        }
        Ok(OperatorEpoch {
            operator: id.to_owned(),
            rows_in: c.rows_in,
            inserts: c.inserts,
            updates: c.updates,
            deletes: c.deletes,
            late: c.late,
            windows_opened: std::mem::take(&mut self.op.opened),
            windows_dropped: std::mem::take(&mut self.op.dropped),
            watermark: self.op.watermark_micros(),
            memory_bytes: memory as u64,
            runs,
            barrier: Some(barrier),
            merge_write: Some(merge_write),
            ..Default::default()
        })
    }

    /// Passes the statement's barrier, or its end, once every sender is
    /// there, and takes what waited behind it.
    async fn align(&mut self, lane: &Lane) -> Result<(), JobError> {
        loop {
            if self.ended {
                return Ok(());
            }
            // Rows after their last barrier are never committed, so a
            // sender's end ends those waiting at a barrier it won't reach.
            if self.standing.contains(&Standing::Ended) {
                for i in 0..self.standing.len() {
                    if self.waiting[i].iter().any(|o| matches!(o, In::End(_))) {
                        self.standing[i] = Standing::Ended;
                    }
                    if self.standing[i] == Standing::Ended {
                        self.waiting[i].clear();
                    }
                }
                if self.standing.iter().all(|s| *s == Standing::Ended) {
                    self.end(lane).await?;
                }
                return Ok(());
            }
            let Some(Standing::AtBarrier(e)) = self.standing.first().copied() else {
                return Ok(());
            };
            if !self
                .standing
                .iter()
                .all(|s| matches!(s, Standing::AtBarrier(_)))
            {
                return Ok(());
            }
            if self.standing.iter().any(|s| *s != Standing::AtBarrier(e)) {
                return Err(JobError::internal(
                    "a keyed lane's senders passed different barriers",
                ));
            }
            if let Some(c) = self.closes_at.filter(|c| *c > self.wm) {
                self.advance(lane, c).await?;
            }
            self.barrier(lane, e).await?;
            for i in 0..self.standing.len() {
                self.standing[i] = Standing::Open;
                while self.standing[i] == Standing::Open {
                    let Some(item) = self.waiting[i].pop_front() else {
                        break;
                    };
                    Box::pin(self.take(lane, i, item)).await?;
                }
            }
        }
    }

    /// The epoch's state is the layer's barrier: a run of what the epoch
    /// changed, which counts once the record names the lane's snapshot, and
    /// that once every writer lane has passed the epoch; then the output's
    /// barrier goes on. A read-only job's epoch changes nothing, and reports
    /// no numbers. Each keyed lane reports the window's watermark there, the
    /// same on all of them.
    async fn barrier(&mut self, lane: &Lane, e: Epoch) -> Result<(), JobError> {
        let flow = lane.flow.clone();
        let plan = &flow.plan;
        let s = self.output;
        let Some((st, input)) = plan.transform.as_ref().map(|t| {
            let st = &t.sql.statements[self.statement];
            (st, &t.sql.inputs[st.input].name)
        }) else {
            return Err(JobError::internal("a keyed lane without a transform"));
        };
        let id = st
            .window
            .as_ref()
            .map(|(_, id)| id.clone())
            .unwrap_or_default();
        {
            let mut align = flow.align.lock().await;
            let marks = &mut align.pieces(e.n).window_marks;
            let wm = marks.get(input).map_or(self.wm, |m| (*m).min(self.wm));
            marks.insert(input.clone(), wm);
        }
        if plan.state.job.as_ref().is_some_and(|j| j.read_only()) {
            // A redo's epoch is in the record already, numbers and all.
            self.layer.discard();
            self.counts = Counts::default();
            (self.op.opened, self.op.dropped) = (0, 0);
        } else {
            let memory = self.layer.memory().total();
            let snapshots = self.layer.barrier(e.n).await.map_err(|err| {
                crate::state::state_error(
                    &format!("operator {id}'s state at epoch {}'s barrier", e.n),
                    &err,
                )
            })?;
            let named = snapshots
                .iter()
                .map(|sn| crate::state::named(&id, sn))
                .collect::<Result<Vec<_>, _>>()?;
            let op = self.epoch(&id, (plan.generation, e.n), &snapshots, memory)?;
            let mut align = flow.align.lock().await;
            let pieces = align.pieces(e.n);
            // At the end of input every window has closed and gone, at
            // every keyed lane's barrier alike, and a later read of the
            // stream starts them again: the record keeps none of it.
            if self.wm != watermark::END {
                pieces.snapshots.extend(named);
            }
            pieces.operator(op);
        }
        let outputs_q = plan
            .transform
            .as_ref()
            .and_then(|t| t.quarantine.as_deref());
        let q = quarantine_file(
            outputs_q,
            e,
            &plan.streams[s],
            lane,
            &mut self.ls.quarantined,
            true,
        )
        .await?;
        flow.owners[s].cross(plan, s, e.n, q, std::mem::take(&mut self.ls.cells), None)?;
        lane.all(s, || super::Out::Barrier(s, e)).await
    }

    async fn end(&mut self, lane: &Lane) -> Result<(), JobError> {
        self.ended = true;
        let flow = &lane.flow;
        let s = self.output;
        flow.owners[s].end(&flow.plan, s)?;
        lane.all(s, || super::Out::End(s)).await
    }
}
