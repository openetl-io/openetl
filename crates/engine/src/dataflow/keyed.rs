//! Keyed lanes: where a statement with state runs, a group window, `OVER`
//! or a dedupe. A lane that reads the statement's stream sends each typed
//! row to the keyed lane owning the key group of the operator's keys, the
//! window's group keys or the `PARTITION BY` of `OVER` or a dedupe, whose
//! output is keyed by it, and its barriers, watermarks and
//! end to every keyed lane: the exchange before the statement. Keyed lanes are the
//! attempt's lanes' twins, each owning its lane's key groups: inside one
//! engine they're its channels, and with a mesh, other engines' are reached
//! over their links (see [`crate::mesh`]). Each keyed lane aligns every
//! sender of the attempt on each barrier, as a writer lane does, runs the
//! operator on its key groups' state, then names and holds the output as a
//! lane does and sends it to its own writer lane, in its own engine: its
//! range is inside that writer lane's, so an output keyed by the window's
//! keys needs no second exchange. `OVER`'s output, keyed by its input's
//! key, takes no second exchange either: each of its rows leaves once, on
//! the writer lane of its `PARTITION BY`'s key group.
//!
//! The watermark of a window is the least of its senders', each the
//! stream's on that lane, which comes after the rows that moved it, from
//! the stream's floor: where the record's splits were, so a window that
//! closed before a restore or a new read stays closed. Windows close as it
//! passes them, in the epoch it does, and the keyed lane passes it on to
//! its writer lane as the output's, and reports it at each barrier.
//!
//! Where a redo may need them, each keyed lane records the steps of the
//! window's watermark that its rows met in the epoch, by the split and
//! place each row was read at (watermark::Steps), and reports them at the
//! barrier. Past watermark::STEPS of them in an epoch, it holds the
//! window's watermark until the barrier, and each split's rows take at
//! most one more step, to the held watermark.
//!
//! A redo of one writer lane's part of an epoch keeps the output's key
//! groups alone (Plan::keep): only rows of the window's keys in them reach a
//! keyed lane, whose state is of those, and its windows stay at the floor
//! while the epoch is read again. Each row comes with the watermark it met
//! in the epoch, which the lane that read it finds in the epoch's steps: a
//! row that met the floor is taken at once, and the others wait for the
//! barrier. There the keyed lane moves the window's watermark to each
//! watermark the rows met in turn, taking the rows that met it, then closes
//! the windows by the watermark the epoch had at its barrier
//! (EventTime::closes_at). Each row then meets what it met in the epoch.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::RangeInclusive;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, RecordBatch, RecordBatchOptions, StringArray, UInt32Array, new_null_array,
};
use arrow_schema::{DataType, Field, Schema};
use openetl_normalize::{Cause, Code, NotHeld, Rejected, Typed, key_groups, render_row};
use openetl_proto::connector::v1::{Epoch, ErrorCode};
use openetl_proto::engine::v1::{
    Barrier, FrameKind, KeyGroupRange as KeyGroups, LaneSteps, OperatorEpoch, SplitSteps,
    StateRequests, Watermark,
};
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
use crate::transform::Keyed as Planned;
use crate::watermark::{self, Senders, Steps};
use crate::window::{self, Out};
use crate::{dedupe, over};

/// Each row's cell the stream holds, of a column the statement reads: the
/// column and why.
type Held = Vec<Option<(String, String)>>;

/// What a lane sends a keyed lane, for a stateful statement by its index.
pub(crate) enum In {
    /// Rows of the statement's stream: the columns it reads, and for `OVER`
    /// and a dedupe, each row's record ID after them, in ID_COLUMN. Each row the
    /// statement can't read, as a cell the stream holds, has the cell's
    /// column and why it's held. With where they were read, where the
    /// epoch's steps are recorded or replayed.
    Rows(
        usize,
        RecordBatch,
        Vec<Option<(String, String)>>,
        Option<Origin>,
    ),
    /// The sender's watermark of the statement's stream, in microseconds:
    /// every row before it has come, but late ones. It never goes back.
    Watermark(usize, i64),
    Barrier(usize, Epoch),
    End(usize),
}

/// Where rows of a window's stream were read.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Origin {
    /// Their split, and the place of the first among the rows the lane
    /// read of it in the epoch, from 0: a keyed lane that records the
    /// epoch's steps records the watermark the rows meet from there on.
    At(Arc<str>, u64),
    /// A redo's: the watermark each row met in the epoch, as the epoch's
    /// steps have it; None where it met the epoch's start.
    Met(Vec<Option<i64>>),
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
/// Its schema's metadata of an Origin::At: the split, and the place.
const SPLIT: &str = "\0split";
const PLACE: &str = "\0place";
/// The column of each row's record ID, after the columns `OVER` or a dedupe reads.
const ID_COLUMN: &str = "\0id";

/// A frame to another engine's keyed lane: its kind, statement and
/// payload.
pub(crate) fn wire(item: In) -> Result<(FrameKind, u32, Vec<u8>), JobError> {
    let index = |s: usize| u32::try_from(s).map_err(|_| JobError::internal("a statement past u32"));
    Ok(match item {
        In::Rows(s, batch, held, origin) => {
            let schema = batch.schema();
            let mut metadata = schema.metadata().clone();
            match origin {
                Some(Origin::At(split, place)) => {
                    metadata.insert(SPLIT.into(), split.to_string());
                    metadata.insert(PLACE.into(), place.to_string());
                }
                Some(Origin::Met(_)) => {
                    return Err(JobError::internal(
                        "a redo's rows for another engine's keyed lane",
                    ));
                }
                None => {}
            }
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
                Arc::new(Schema::new_with_metadata(fields, metadata)),
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
            let mut metadata = rows.schema().metadata().clone();
            let origin = match (metadata.remove(SPLIT), metadata.remove(PLACE)) {
                (Some(split), Some(place)) => Some(Origin::At(
                    split.into(),
                    place
                        .parse()
                        .map_err(|_| format!("rows at place {place:?}"))?,
                )),
                (None, None) => None,
                _ => return Err("rows with a split and no place, or a place and no split".into()),
            };
            let schema = Schema::new_with_metadata(rows.schema().fields().clone(), metadata);
            let rows = RecordBatch::try_new_with_options(
                Arc::new(schema),
                rows.columns().to_vec(),
                &RecordBatchOptions::new().with_row_count(Some(rows.num_rows())),
            )
            .map_err(|e| e.to_string())?;
            Ok(In::Rows(s, rows, held, origin))
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

/// The stateful statements, by their index in the transform.
pub(crate) fn statements(plan: &Plan) -> Vec<usize> {
    plan.transform.as_ref().map_or_else(Vec::new, |t| {
        (0..t.sql.statements.len())
            .filter(|&i| t.sql.statements[i].keyed.is_some())
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

/// Sends the stateful statements' rows of a typed batch of their stream to
/// the keyed lanes owning their keys' groups: those the output keeps. `at`
/// is the split the batch was read from, and the rows the lane read of it
/// in the epoch before the batch.
pub(super) async fn rows(
    lane: &Lane,
    statements: &[usize],
    typed: &Typed,
    at: Option<(&str, u64)>,
) -> Result<(), JobError> {
    let plan = &lane.flow.plan;
    let Some(t) = &plan.transform else {
        return Ok(());
    };
    // Each row's place among its split's rows of the epoch, where the
    // epoch's steps are recorded or replayed.
    let mut placed: Option<(Arc<str>, Vec<u64>)> = None;
    for &i in statements {
        let st = &t.sql.statements[i];
        let Some((keyed, id)) = &st.keyed else {
            continue;
        };
        let batch = reads(st, keyed, &typed.batch, &t.sql.inputs[st.input].name)?;
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
        let groups = key_groups(&batch, keyed.need(), plan.key_groups)
            .map_err(|e| shape_error(&t.sql.inputs[st.input].name, e.into()))?;
        let batch = match keyed.ids() {
            true => with_ids(&batch, &typed.ids)?,
            false => batch,
        };
        let keep = output_of(plan, st).and_then(|o| plan.keep[o]);
        let mut parts: Vec<Vec<u32>> = vec![Vec::new(); plan.attempt_lanes.len()];
        for (r, &g) in groups.iter().enumerate() {
            if keep.is_some_and(|(first, last)| g < first || g > last) {
                continue;
            }
            parts[lane_of(&plan.attempt_lanes, g)].push(r as u32);
        }
        let met = plan.state.steps.get(id);
        if let Some((split, before)) = at.filter(|_| met.is_some() || plan.state.record_steps)
            && placed.is_none()
        {
            let places = typed
                .input_rows()
                .into_iter()
                .map(|r| before + r as u64)
                .collect();
            placed = Some((split.into(), places));
        }
        for (k, rows) in parts.into_iter().enumerate() {
            if rows.is_empty() {
                continue;
            }
            let origin = match (&placed, met) {
                (Some((split, places)), Some(met)) => Some(Origin::Met(
                    rows.iter()
                        .map(|&r| met.at(groups[r as usize], split, places[r as usize]))
                        .collect(),
                )),
                (Some((split, places)), None) => {
                    Some(Origin::At(split.clone(), places[rows[0] as usize]))
                }
                (None, _) => None,
            };
            let part = match rows.len() == batch.num_rows() {
                true => batch.clone(),
                false => {
                    arrow_select::take::take_record_batch(&batch, &UInt32Array::from(rows.clone()))
                        .map_err(|e| JobError::internal(e.to_string()))?
                }
            };
            let h = rows.iter().map(|&r| held[r as usize].clone()).collect();
            send(lane, k, In::Rows(i, part, h, origin)).await?;
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

/// `batch` with each row's record ID after its columns, in ID_COLUMN.
fn with_ids(
    batch: &RecordBatch,
    ids: &arrow_array::FixedSizeBinaryArray,
) -> Result<RecordBatch, JobError> {
    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(ID_COLUMN, ids.data_type().clone(), true));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(ids.clone()));
    RecordBatch::try_new_with_options(
        Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        )),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|e| JobError::internal(e.to_string()))
}

/// The rows of `OVER` or a dedupe without the record IDs after them, and
/// the IDs.
fn without_ids(
    batch: &RecordBatch,
) -> Result<(RecordBatch, arrow_array::FixedSizeBinaryArray), JobError> {
    let n = batch.num_columns();
    let last = n.checked_sub(1);
    let ids = last
        .filter(|&i| batch.schema().field(i).name() == ID_COLUMN)
        .and_then(|i| batch.column(i).as_fixed_size_binary_opt().cloned());
    let (Some(ids), Some(last)) = (ids, last) else {
        return Err(JobError::internal("OVER's rows without their record IDs"));
    };
    let rows = batch
        .project(&(0..last).collect::<Vec<_>>())
        .map_err(|e| JobError::internal(e.to_string()))?;
    Ok((rows, ids))
}

/// The columns a statement reads, from a batch of its stream: null where
/// the batch lacks one. One of another type changes what its operator
/// keeps, which needs its state built again.
fn reads(
    st: &crate::transform::Statement,
    keyed: &Planned,
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
                        "transform.sql: {}: stream {stream}'s column {} changed from {} to {}, and {} writing {} keeps state of what it read: the pipeline's operator needs its state built again from the stream",
                        st.at,
                        f.name(),
                        f.data_type(),
                        c.data_type(),
                        keyed.what(),
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

/// A statement's stateful operator, as a keyed lane runs it.
pub(crate) enum Op {
    Window(window::Operator),
    Over(over::Operator),
    Dedupe(dedupe::Operator),
}

impl Op {
    pub(crate) fn new(
        st: &crate::transform::Statement,
        keyed: &Planned,
        id: &str,
        input: &crate::transform::Input,
        key_groups: u32,
    ) -> Result<Op, JobError> {
        let lateness = input.lateness;
        Ok(match keyed {
            Planned::Dedupe(d) => Op::Dedupe(dedupe::Operator::new(
                id,
                st.at,
                d.clone(),
                &st.output,
                input.dedupe,
                key_groups,
            )?),
            Planned::Window(w) => Op::Window(window::Operator::new(
                id,
                st.at,
                w.clone(),
                &st.output,
                st.schema.clone(),
                lateness,
                key_groups,
            )?),
            Planned::Over(o) => Op::Over(over::Operator::new(
                id,
                st.at,
                o.clone(),
                &st.output,
                key_groups,
            )?),
        })
    }

    /// Takes the state its lane opened.
    fn open(&self, layer: &mut Layer) {
        match self {
            Op::Window(w) => w.open(layer),
            Op::Over(_) | Op::Dedupe(_) => {}
        }
    }

    /// The watermark `wm` as far as rows can tell it apart.
    fn level(&self, wm: i64) -> i64 {
        match self {
            Op::Window(w) => w.level(wm),
            Op::Over(o) => o.level(wm),
            Op::Dedupe(d) => d.level(wm),
        }
    }

    fn watermark_micros(&self) -> Option<i64> {
        match self {
            Op::Window(w) => w.watermark_micros(),
            Op::Over(o) => o.watermark_micros(),
            Op::Dedupe(d) => d.watermark_micros(),
        }
    }

    /// Takes rows of the statement's input: for `OVER` and a dedupe, with
    /// `ids`, their record IDs.
    pub(crate) async fn rows(
        &mut self,
        layer: &mut Layer,
        input: &RecordBatch,
        ids: Option<&arrow_array::FixedSizeBinaryArray>,
    ) -> Result<Out, JobError> {
        match (self, ids) {
            (Op::Window(w), _) => w.rows(layer, input).await,
            (Op::Over(o), Some(ids)) => o.rows(layer, input, ids).await,
            (Op::Dedupe(d), Some(ids)) => d.rows(layer, input, ids).await,
            (Op::Over(_) | Op::Dedupe(_), None) => {
                Err(JobError::internal("rows without their record IDs"))
            }
        }
    }

    /// Moves the watermark to `wm`: what it gave, and whether it has more
    /// to give at it.
    pub(crate) async fn advance(
        &mut self,
        layer: &mut Layer,
        wm: i64,
    ) -> Result<(Out, bool), JobError> {
        match self {
            Op::Window(w) => Ok((w.advance(layer, wm).await?, false)),
            Op::Over(o) => o.advance(layer, wm).await,
            Op::Dedupe(d) => d.advance(layer, wm).await,
        }
    }

    /// The windows, sessions or keys it began keeping and stopped keeping
    /// since the last call, and the rows it dropped as repeats.
    fn take_counts(&mut self) -> (u64, u64, u64) {
        match self {
            Op::Window(w) => (
                std::mem::take(&mut w.opened),
                std::mem::take(&mut w.dropped),
                0,
            ),
            Op::Over(_) => (0, 0, 0),
            Op::Dedupe(d) => (
                std::mem::take(&mut d.kept),
                std::mem::take(&mut d.forgot),
                std::mem::take(&mut d.repeats),
            ),
        }
    }
}

/// One stateful statement on a keyed lane.
struct Keyed {
    statement: usize,
    output: usize,
    /// Its operator's ID.
    id: String,
    /// What it holds as the job's state; it stops counting once dropped.
    held: Option<crate::state::LaneState>,
    op: Op,
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
    /// A redo's: rows that met a later watermark in the epoch than the
    /// window's, by that watermark, which wait for the barrier.
    later: BTreeMap<i64, Vec<(RecordBatch, Held)>>,
    /// The key groups it owns.
    owns: RangeInclusive<u32>,
    /// Where it records the epoch's steps: the steps of the window's
    /// watermark its rows met since the last barrier.
    steps: Option<Steps>,
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
        let Some((keyed, id)) = &st.keyed else {
            continue;
        };
        let Some(output) = output_of(plan, st) else {
            continue;
        };
        let op = Op::new(st, keyed, id, &t.sql.inputs[st.input], plan.key_groups)?;
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
                "transform.sql {}: {} writing {} can't open the state the checkpoint names, {why}. Reset stream {stream} to build the state again from a new read of it: openetl reset --stream {stream} NAME",
                st.at,
                keyed.what(),
                st.output
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
            return Err(JobError::internal(
                "a stateful statement without the job's state",
            ));
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
        layer.set_flush_at(state.flush_at(plan.layout.lanes().len()));
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
                epoch: plan.first_epoch,
                owner_sealed: o.sealed,
                sealed_after: o.sealed_after,
                noted: false,
            }
        };
        let n = senders.len();
        let mut w = Keyed {
            statement: i,
            output,
            id: id.clone(),
            held: Some(state.lane(id, lane.lane)),
            op,
            layer,
            ls,
            standing: vec![Standing::Open; n],
            waiting: (0..n).map(|_| VecDeque::new()).collect(),
            watermarks: Senders::new(n, floor),
            wm: watermark::NONE,
            closes_at,
            later: BTreeMap::new(),
            owns: owned.clone(),
            steps: None,
            ended: false,
            counts: Counts::default(),
        };
        // Windows the record's watermark passed closed before it.
        if floor != watermark::NONE {
            w.advance(&lane, floor).await?;
        }
        if plan.state.record_steps {
            w.steps = Some(Steps::new(w.op.level(w.wm)));
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
            In::Rows(_, batch, held, Some(Origin::Met(met))) if self.closes_at.is_some() => {
                self.replay(lane, batch, held, &met).await
            }
            In::Rows(_, batch, held, origin) => {
                if let (Some(steps), Some(Origin::At(split, place))) = (&mut self.steps, origin) {
                    steps.met(&split, place, self.op.level(self.wm));
                }
                self.rows(lane, batch, held).await
            }
            In::Watermark(_, t) => {
                let advanced = self
                    .watermarks
                    .take(sender, t)
                    .map_err(|e| JobError::internal(format!("a keyed lane's operator: {e}")))?;
                match advanced {
                    Some(wm) if self.closes_at.is_none() && !self.holds(wm) => {
                        self.advance(lane, wm).await
                    }
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

    /// It has taken as many steps as it takes this epoch, and `wm` would be
    /// another: the window's watermark holds until the barrier. The end of
    /// input never holds.
    fn holds(&self, wm: i64) -> bool {
        self.steps.as_ref().is_some_and(Steps::full)
            && wm != watermark::END
            && self.op.level(wm) != self.op.level(self.wm)
    }

    /// A redo's rows, each with the watermark it met in the epoch: those
    /// that met the window's are taken now, and the others wait for the
    /// barrier.
    async fn replay(
        &mut self,
        lane: &Lane,
        batch: RecordBatch,
        held: Held,
        met: &[Option<i64>],
    ) -> Result<(), JobError> {
        let mut now = Vec::new();
        let mut later: BTreeMap<i64, Vec<u32>> = BTreeMap::new();
        for (r, m) in met.iter().enumerate() {
            match m {
                Some(w) if *w > self.wm => later.entry(*w).or_default().push(r as u32),
                _ => now.push(r as u32),
            }
        }
        let some = |rows: &[u32]| -> Result<_, JobError> {
            let b =
                arrow_select::take::take_record_batch(&batch, &UInt32Array::from(rows.to_vec()))
                    .map_err(|e| JobError::internal(e.to_string()))?;
            let h = rows.iter().map(|&r| held[r as usize].clone()).collect();
            Ok((b, h))
        };
        for (w, rows) in later {
            let part = some(&rows)?;
            let bytes = part.0.get_array_memory_size() as u64;
            let deferred = &lane.flow.plan.state.deferred;
            deferred
                .hold(bytes)
                .map_err(|limit| self.held_over(lane, limit))?;
            self.later.entry(w).or_default().push(part);
        }
        match now.len() {
            0 => Ok(()),
            n if n == batch.num_rows() => self.rows(lane, batch, held).await,
            _ => {
                let (b, h) = some(&now)?;
                self.rows(lane, b, h).await
            }
        }
    }

    /// A redo's error where the rows that wait for its barrier need more
    /// than the `limit` bytes its keyed lanes may hold.
    fn held_over(&self, lane: &Lane, limit: u64) -> JobError {
        let plan = &lane.flow.plan;
        let (at, output) = plan.transform.as_ref().map_or_else(
            || (String::new(), String::new()),
            |t| {
                let st = &t.sql.statements[self.statement];
                (st.at.to_string(), st.output.clone())
            },
        );
        let limit = match limit >= 1 << 20 {
            true => format!("{} MiB", limit >> 20),
            false => format!("{limit} bytes"),
        };
        JobError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "transform.sql {at}: the redo of a writer lane's part of epoch {}, writing {output}, holds the rows that met a later watermark of the statement's operator in the epoch until the epoch's barrier, and they need more than the {limit} it may hold, half of the job's memory. Run the pipeline with a larger resources.size, so that the next attempt can settle the epoch",
                plan.first_epoch
            ),
        )
    }

    /// Takes rows of the statement's stream, by the operator's watermark
    /// now.
    async fn rows(&mut self, lane: &Lane, batch: RecordBatch, held: Held) -> Result<(), JobError> {
        let (batch, ids) = match &self.op {
            Op::Over(_) | Op::Dedupe(_) => {
                let (rows, ids) = without_ids(&batch)?;
                (rows, Some(ids))
            }
            Op::Window(_) => (batch, None),
        };
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
        let (input, ids) = match keep.len() == batch.num_rows() {
            true => (batch.clone(), ids),
            false => {
                let at = UInt32Array::from_iter_values(keep.iter().map(|&r| r as u32));
                let input = arrow_select::take::take_record_batch(&batch, &at)
                    .map_err(|e| JobError::internal(e.to_string()))?;
                let ids = match ids {
                    Some(ids) => Some(
                        arrow_select::take::take(&ids, &at, None)
                            .map_err(|e| JobError::internal(e.to_string()))?
                            .as_fixed_size_binary()
                            .clone(),
                    ),
                    None => None,
                };
                (input, ids)
            }
        };
        let out = self.op.rows(&mut self.layer, &input, ids.as_ref()).await?;
        self.relieve(lane).await?;
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

    /// Closes the windows the watermark `wm` passes, or gives the rows `OVER`
    /// has waiting before it, and passes it on to the output's writer lane
    /// after their rows.
    async fn advance(&mut self, lane: &Lane, wm: i64) -> Result<(), JobError> {
        self.wm = wm;
        loop {
            let (out, more) = self.op.advance(&mut self.layer, wm).await?;
            self.relieve(lane).await?;
            self.give(lane, out).await?;
            if !more {
                break;
            }
        }
        let s = self.output;
        lane.all(s, || super::Out::Watermark(s, wm)).await
    }

    /// Near the engine's memory limit, flushes the epoch's changes early, if
    /// they're enough to be worth a run, and counts what the state holds:
    /// if, flushed, it's still more than the job lets it hold, the job
    /// fails, saying what to raise, rather than reach the limit. A lane
    /// past its part of the memory flushes as it next reads.
    async fn relieve(&mut self, lane: &Lane) -> Result<(), JobError> {
        let flow = lane.flow.clone();
        let Some(state) = &flow.plan.state.job else {
            return Ok(());
        };
        if state.pressure()
            && !state.read_only()
            && self.layer.memory().changes + self.layer.memory().pending >= crate::state::MIN_EARLY
        {
            self.layer.flush().await.map_err(|e| {
                crate::state::state_error(&format!("operator {}'s state", self.id), &e)
            })?;
        }
        let held = self.count();
        let budget = state.budget();
        // As it is now: the flush took a while, and other lanes may have
        // released memory meanwhile.
        if budget == 0 || held <= budget || !state.pressure() {
            return Ok(());
        }
        let (at, output) = flow.plan.transform.as_ref().map_or_else(
            || (String::new(), String::new()),
            |t| {
                let st = &t.sql.statements[self.statement];
                (st.at.to_string(), st.output.clone())
            },
        );
        let what = match self.op {
            Op::Window(_) => "group window",
            Op::Over(_) => "OVER statement",
            Op::Dedupe(_) => "dedupe",
        };
        Err(JobError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "transform.sql {at}: the {what} writing {output} keeps more state in memory than the job's size allows: its lanes and caches hold {} MiB, past the {} MiB, half of the job's {} MiB, that state may hold, with the engine near its memory limit. Run the pipeline with a larger resources.size",
                held >> 20,
                budget >> 20,
                state.memory() >> 20
            ),
        ))
    }

    /// Counts what the lane's state holds now as the job's, and gives the
    /// job's.
    fn count(&self) -> u64 {
        self.held
            .as_ref()
            .map_or(0, |h| h.hold(self.layer.memory().total()))
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

    /// Names, holds and sends what the operator gave, and quarantines the
    /// windows, or rows, it couldn't give.
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
        let (opened, dropped, repeats) = self.op.take_counts();
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
            windows_opened: opened,
            windows_dropped: dropped,
            repeats,
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
            match self.closes_at {
                // A redo's rows that met later watermarks, each in turn,
                // then the windows the epoch closed.
                Some(c) => {
                    for (w, parts) in std::mem::take(&mut self.later) {
                        self.advance(lane, w).await?;
                        for (b, h) in parts {
                            let bytes = b.get_array_memory_size() as u64;
                            self.rows(lane, b, h).await?;
                            lane.flow.plan.state.deferred.release(bytes);
                        }
                    }
                    if c > self.wm {
                        self.advance(lane, c).await?;
                    }
                }
                // Where it held the watermark, it catches up.
                None => {
                    let wm = self.watermarks.watermark();
                    if wm > self.wm {
                        self.advance(lane, wm).await?;
                    }
                }
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

    /// The epoch's state is the layer's barrier: what the lane changed
    /// since its last run, in its snapshot, or past its share of the
    /// record, a run of it. It counts once the record names the lane's
    /// snapshot, and that once every writer lane has passed the epoch; then
    /// the output's
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
            .keyed
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
            self.op.take_counts();
            self.count();
        } else {
            let memory = self.layer.memory().total();
            let snapshots = self.layer.barrier(e.n).await.map_err(|err| {
                crate::state::state_error(
                    &format!("operator {id}'s state at epoch {}'s barrier", e.n),
                    &err,
                )
            })?;
            // The barrier released the epoch's changes and the sections
            // that died.
            self.count();
            let named = snapshots
                .iter()
                .map(|sn| crate::state::named(&id, sn))
                .collect::<Result<Vec<_>, _>>()?;
            let mut op = self.epoch(&id, (plan.generation, e.n), &snapshots, memory)?;
            let start = self.op.level(self.wm);
            if let Some(steps) = self.steps.as_mut().map(|s| s.take(start))
                && !steps.is_empty()
            {
                op.steps.push(LaneSteps {
                    key_groups: Some(KeyGroups {
                        first: *self.owns.start(),
                        last: *self.owns.end(),
                    }),
                    splits: steps
                        .into_iter()
                        .map(|(split, steps)| SplitSteps {
                            split,
                            places: steps.iter().map(|s| s.0).collect(),
                            watermarks: steps.iter().map(|s| s.1).collect(),
                        })
                        .collect(),
                });
            }
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
        self.ls.epoch = e.n + 1;
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
