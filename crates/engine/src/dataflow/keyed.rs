//! Keyed lanes: where a group window's statement runs. A lane that reads a
//! window's stream sends each typed row to the keyed lane owning the key
//! group of the window's keys, by the layout's lane ranges, and its
//! barriers, watermarks and end to every keyed lane, which is the exchange
//! before the statement inside one engine. Each keyed lane aligns its
//! senders on each barrier, as a writer lane does, runs the window on its
//! key groups' state, then names and holds the output as a lane does and
//! sends it to its own writer lane: its range is inside that writer lane's,
//! so an output keyed by the window's keys needs no second exchange.
//!
//! The watermark of a window is the least of its senders', each the
//! stream's on that lane, which comes after the rows that moved it, from
//! the stream's floor: where the record's splits were, so a window that
//! closed before a restore or a new read stays closed. Windows close as it
//! passes them, in the epoch it does, and the keyed lane passes it on to
//! its writer lane as the output's.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow_array::{RecordBatch, UInt32Array, new_null_array};
use arrow_schema::{Field, Schema};
use openetl_normalize::{Rejected, Typed, key_groups, render_row};
use openetl_proto::connector::v1::{Epoch, ErrorCode};
use openetl_state::{KeyGroupRange, Layer};
use tokio::sync::mpsc;

use super::{Flow, Lane, LaneStream, copy, hold, quarantine_file, shape_error, shaped};
use crate::error::JobError;
use crate::watermark::{self, Senders};
use crate::window::{Operator, Out};

/// What a lane sends a keyed lane, for a window's statement by its index.
pub(super) enum In {
    /// Rows of the statement's stream: the columns it reads. Each row the
    /// statement can't read, as a cell the stream holds, has why.
    Rows(usize, RecordBatch, Vec<Option<String>>),
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

/// Sends the window statements' rows of a typed batch of their stream to
/// the keyed lanes owning their keys' groups.
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
        let mut why: Vec<Option<String>> = vec![None; batch.num_rows()];
        // A cell the stream holds is no value only for a column the
        // statement reads.
        for c in typed
            .cells
            .iter()
            .filter(|c| st.op.reads.column_with_name(&c.column).is_some())
        {
            if let Some(w) = why.get_mut(c.row) {
                w.get_or_insert_with(|| {
                    format!(
                        "transform.sql {}, writing {}: column {} has a value that doesn't fit its type, which the stream holds: {}",
                        st.at, st.output, c.column, c.reason
                    )
                });
            }
        }
        let groups = key_groups(&batch, &window.need, plan.key_groups)
            .map_err(|e| shape_error(&t.sql.inputs[st.input].name, e.into()))?;
        let mut parts: Vec<Vec<u32>> = vec![Vec::new(); lane.keyed.len()];
        for (r, g) in groups.into_iter().enumerate() {
            parts[plan.layout.lane_of(g)].push(r as u32);
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
            let w = rows.iter().map(|&r| why[r as usize].clone()).collect();
            send(lane, k, In::Rows(i, part, w)).await?;
        }
    }
    Ok(())
}

/// Sends `f(statement)` to every keyed lane, for each statement.
pub(super) async fn all(
    lane: &Lane,
    statements: &[usize],
    f: impl Fn(usize) -> In,
) -> Result<(), JobError> {
    for &i in statements {
        for k in 0..lane.keyed.len() {
            send(lane, k, f(i)).await?;
        }
    }
    Ok(())
}

async fn send(lane: &Lane, k: usize, item: In) -> Result<(), JobError> {
    let stopped = || JobError::stopped("a keyed lane stopped");
    let item = match lane.keyed[k].try_send((lane.lane, item)) {
        Ok(()) => return Ok(()),
        Err(mpsc::error::TrySendError::Full(item)) => item,
        Err(mpsc::error::TrySendError::Closed(_)) => return Err(stopped()),
    };
    lane.enter(super::Doing::Backpressured);
    let sent = lane.keyed[k].send(item).await.map_err(|_| stopped());
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
    ended: bool,
}

/// Runs keyed lane `lane.lane` until every window's stream has ended on
/// every sender.
pub(super) async fn run(
    flow: Arc<Flow>,
    lane: Lane,
    mut rx: mpsc::Receiver<(usize, In)>,
) -> Result<(), JobError> {
    let plan = &flow.plan;
    let Some(t) = &plan.transform else {
        return Ok(());
    };
    let senders = plan.inbound.len();
    let range = plan.layout.lane(lane.lane);
    let range = KeyGroupRange::new(*range.start(), *range.end());
    let mut windows: Vec<Keyed> = Vec::new();
    for (i, st) in t.sql.statements.iter().enumerate() {
        let Some((window, id)) = &st.window else {
            continue;
        };
        let Some(output) = plan.streams.iter().position(|s| *s == st.output) else {
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
        let Some(state) = &plan.state.job else {
            return Err(JobError::internal("a group window without the job's state"));
        };
        let layer = Layer::open(
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
        let floor = t.inputs[st.input]
            .event_time
            .as_ref()
            .map_or(watermark::NONE, |e| e.floor);
        let ls = {
            let o = flow.owners[output].lock();
            LaneStream {
                stream: output,
                shaper: o.shaper.fork(),
                version: o.version,
                quarantined: (Vec::new(), 0),
                cells: Default::default(),
                replay: None,
            }
        };
        let mut w = Keyed {
            statement: i,
            output,
            op,
            layer,
            ls,
            standing: vec![Standing::Open; senders],
            waiting: (0..senders).map(|_| VecDeque::new()).collect(),
            watermarks: Senders::new(senders, floor),
            ended: false,
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
        let Some((sender, item)) = got else {
            return Err(JobError::stopped("the lanes feeding a keyed lane stopped"));
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
            In::Rows(_, batch, why) => {
                let mut rejected = Vec::new();
                let keep: Vec<usize> = (0..batch.num_rows())
                    .filter(|&r| match &why[r] {
                        Some(w) => {
                            rejected.push(Rejected {
                                row: r,
                                reason: w.clone(),
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
                let rejected = out
                    .rejected
                    .iter()
                    .map(|(r, why)| Rejected {
                        row: keep[*r],
                        reason: why.clone(),
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
                    Some(wm) => self.advance(lane, wm).await,
                    None => Ok(()),
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
                .map(|(i, (_, why))| Rejected {
                    row: i,
                    reason: why.clone(),
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
    /// barrier goes on. A read-only job's epoch changes nothing.
    async fn barrier(&mut self, lane: &Lane, e: Epoch) -> Result<(), JobError> {
        let flow = lane.flow.clone();
        let plan = &flow.plan;
        let s = self.output;
        let id = plan
            .transform
            .as_ref()
            .and_then(|t| t.sql.statements[self.statement].window.as_ref())
            .map(|(_, id)| id.clone())
            .unwrap_or_default();
        if plan.state.job.as_ref().is_some_and(|j| j.read_only()) {
            self.layer.discard();
        } else {
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
            flow.align.lock().await.pieces(e.n).snapshots.extend(named);
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
