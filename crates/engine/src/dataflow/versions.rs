//! The versions of a stream's records, at a writer lane: for each record ID
//! of its key groups, the hash of the version that holds it. A version read
//! again with another hash gets an ID of its own, so that a destination that
//! skips the IDs it has takes it: a second change within one value of the
//! cursor, which the next read gives again at its inclusive floor.
//!
//! An entry is the owner's hash and its schema's version, at the read that
//! last gave its ID, by the read's start: the ID's one row at that time,
//! which a lookup reads by its key rather than a scan. The entries the
//! stream's last two reads gave stay: a read's first row at a writer lane
//! moves each of its key groups on to the read, and the entries of the
//! reads before the last expire, so a read's runs go two reads after it. A
//! group's reads are horizons, which the record carries, as a value would
//! keep the run it's in named after the run's entries had gone. A row without a live entry of its schema's
//! version keeps its ID, and its hash becomes the owner. So does a row of a
//! key group the writer lane doesn't keep: an entry is missed, and the
//! version skipped as it was before, but never given two IDs. Versions of a
//! layout other than this engine's start again, as versions that can't be
//! read do. A run that's gone is forgotten at the read that finds it, and
//! the rest kept.

use std::collections::HashMap;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, FixedSizeBinaryArray, RecordBatch, RecordBatchOptions};
use arrow_schema::Schema;
use bytes::Bytes;
use openetl_normalize::naming::source_names;
use openetl_normalize::versions::{HASH_COLUMN, SCHEMA_KEY};
use openetl_normalize::{key_groups, version_id};
use openetl_proto::connector::v1::ErrorCode;
use openetl_state::{Error as StateError, KeyGroupRange, KeyRef, Layer, OperatorId, StateBackend};

use super::{Flow, Out};
use crate::error::JobError;

const ENTRIES: OperatorId = OperatorId(0);
/// Each key group's last read, as the horizon of an operator that holds
/// nothing. The horizon of [`ENTRIES`] is the start of the read before it,
/// less one.
const LAST: OperatorId = OperatorId(1);
/// The layout of a lane's versions, as its snapshots name it: the state's,
/// and the versions' own in the bits above it, so that an engine that
/// keeps them otherwise starts them again rather than misread them.
const LAYOUT: u32 = openetl_state::LAYOUT | (3 << 16);
const GONE: &str = "a run of the record's versions of the stream's records is gone, so a record read again whose entry it held keeps its ID";
const UNREAD: &str = "the record's versions of the stream's records can't be read, so a record read again keeps its ID until they're kept again";

/// A writer lane's versions of each stream that keeps them.
pub(super) struct Versions {
    streams: Vec<Option<Stream>>,
}

struct Stream {
    id: String,
    layer: Layer,
    /// The key groups whose entries the lane keeps; None for none.
    range: Option<KeyGroupRange>,
    /// The stream's key columns, by source name.
    key: Vec<String>,
    /// The key columns as the stream's last batch named them, which its held
    /// values' batches, after it, name them too but don't say.
    named: Vec<String>,
    /// The read's start.
    read: i64,
    /// Whether the lane's key groups moved on to the read in the open epoch.
    moved: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Reads {
    before: Option<i64>,
    last: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Entry {
    hash: u64,
    schema: u64,
}

impl Entry {
    fn decode(b: &[u8]) -> Option<Self> {
        Some(Entry {
            hash: u64::from_le_bytes(b.get(..8)?.try_into().ok()?),
            schema: u64::from_le_bytes(b.get(8..16)?.try_into().ok()?),
        })
    }

    fn encode(&self) -> Bytes {
        let mut b = self.hash.to_le_bytes().to_vec();
        b.extend_from_slice(&self.schema.to_le_bytes());
        Bytes::from(b)
    }
}

/// The key groups writer lane `writer` of an attempt's `writers` owns, of
/// `groups`, as the layout has them.
fn owned(groups: u32, writer: usize, writers: usize) -> KeyGroupRange {
    let (g, n, w) = (u64::from(groups), writers.max(1) as u64, writer as u64);
    let first = (w * g).div_ceil(n);
    let last = ((w + 1) * g).div_ceil(n).saturating_sub(1);
    KeyGroupRange::new(
        u32::try_from(first).unwrap_or(u32::MAX),
        u32::try_from(last).unwrap_or(u32::MAX),
    )
}

impl Versions {
    /// Opens writer lane `writer`'s versions of each stream that keeps them,
    /// from the restored record's. State the record names that can't be
    /// read is left, with a warning: its rows keep their IDs.
    pub(super) async fn open(flow: &Flow, writer: usize) -> Result<Self, JobError> {
        let plan = &flow.plan;
        let mut streams = Vec::with_capacity(plan.streams.len());
        for (s, id) in plan.versions.iter().enumerate() {
            let Some(id) = id else {
                streams.push(None);
                continue;
            };
            let Some(state) = &plan.state.job else {
                return Err(JobError::internal(
                    "a stream's versions without the job's state",
                ));
            };
            let r = owned(
                plan.key_groups,
                plan.first_writer_lane + writer,
                plan.attempt_writer_lanes,
            );
            // A redo writes no state, and reads its key groups' on every
            // writer lane, whichever its rows reach.
            let range = match plan.kept(s) {
                None => Some(r),
                Some((first, last)) if state.read_only() => Some(KeyGroupRange::new(first, last)),
                Some((first, last)) => {
                    Some(KeyGroupRange::new(r.first.max(first), r.last.min(last)))
                        .filter(|r| r.first <= r.last)
                }
            };
            let open = range.unwrap_or(KeyGroupRange::new(0, 0));
            let mut restored: Vec<openetl_state::Snapshot> = plan
                .state
                .restored
                .iter()
                .filter(|(o, sn)| o == id && range.is_some() && sn.range.overlaps(&open))
                .map(|(_, sn)| sn.clone())
                .collect();
            let stores = state.stores(id);
            let opened = match restored.iter().find(|sn| sn.layout != LAYOUT) {
                Some(sn) => Err(StateError::Snapshot(format!(
                    "versions of layout {:#x}, where this engine keeps {LAYOUT:#x}",
                    sn.layout
                ))),
                None => {
                    for sn in &mut restored {
                        sn.layout = openetl_state::LAYOUT;
                    }
                    Layer::open(&stores, plan.generation, plan.first_epoch, open, &restored).await
                }
            };
            let layer = match opened {
                Ok(l) => l,
                Err(
                    e @ (StateError::Snapshot(_) | StateError::Corrupt(_) | StateError::Gone(_)),
                ) => {
                    tracing::warn!(stream = %plan.streams[s], error = %e, "{UNREAD}");
                    Layer::open(&stores, plan.generation, plan.first_epoch, open, &[])
                        .await
                        .map_err(|e| crate::state::state_error("a stream's versions", &e))?
                }
                Err(e) => return Err(crate::state::state_error("a stream's versions", &e)),
            };
            let mut layer = layer;
            if !state.read_only() {
                layer.set_flush_at(state.flush_at(plan.writer_lanes));
            }
            streams.push(Some(Stream {
                id: id.clone(),
                layer,
                range,
                key: plan.identities[s].key.clone(),
                named: plan.identities[s].key.clone(),
                read: plan.identities[s].read_start,
                moved: false,
            }));
        }
        Ok(Self { streams })
    }

    /// Takes what the writer lane writes next: a batch's rows, and its held
    /// values, get the IDs of their versions and lose their hashes, and a
    /// barrier adds the versions to its epoch.
    pub(super) async fn apply(&mut self, flow: &Flow, out: Out) -> Result<Out, JobError> {
        match out {
            Out::Batch(s, b) => Ok(Out::Batch(s, self.versioned(flow, s, b, true).await?)),
            Out::Held(s, b) => Ok(Out::Held(s, self.versioned(flow, s, b, false).await?)),
            Out::Barrier(s, e) => {
                if let Some(st) = self.streams[s].as_mut() {
                    st.barrier(flow, e.n).await?;
                }
                Ok(Out::Barrier(s, e))
            }
            out => Ok(out),
        }
    }

    async fn versioned(
        &mut self,
        flow: &Flow,
        s: usize,
        batch: RecordBatch,
        write: bool,
    ) -> Result<RecordBatch, JobError> {
        let Some((batch, hashes, schema)) = without_hashes(&batch)? else {
            return Ok(batch);
        };
        match self.streams[s].as_mut() {
            Some(st) => st.ids(flow, batch, &hashes, schema, write).await,
            None => Ok(batch),
        }
    }
}

impl Stream {
    fn failed(&self, e: &StateError) -> JobError {
        crate::state::state_error(&format!("operator {}'s state", self.id), e)
    }

    /// The batch with each row's ID its version's. Where `write`, the rows'
    /// versions are kept; else, as for held values, the rows of a batch
    /// this took before, they're only read.
    async fn ids(
        &mut self,
        flow: &Flow,
        batch: RecordBatch,
        hashes: &[u64],
        schema: u64,
        write: bool,
    ) -> Result<RecordBatch, JobError> {
        let Some(range) = self.range else {
            return Ok(batch);
        };
        let plan = &flow.plan;
        if write {
            self.named = named(&self.key, &batch.schema());
        }
        let groups = key_groups(&batch, &self.named, plan.key_groups)
            .map_err(|e| JobError::internal(format!("a stream's key groups: {e}")))?;
        let at = batch
            .schema()
            .index_of("_openetl_id")
            .map_err(|e| JobError::internal(e.to_string()))?;
        let ids = batch.column(at).as_fixed_size_binary().clone();
        let raw = ids.value_data();
        let id_of = |r: usize| &raw[r * 16..(r + 1) * 16];
        let rows: Vec<usize> = (0..batch.num_rows())
            .filter(|&r| range.contains(groups[r]) && !ids.is_null(r))
            .collect();
        if rows.is_empty() {
            return Ok(batch);
        }
        if write {
            self.move_on(range);
        }
        let reads: Vec<Reads> = rows.iter().map(|&r| self.reads(groups[r])).collect();
        let mut slots = Vec::with_capacity(rows.len() * 2);
        for (&r, reads) in rows.iter().zip(&reads) {
            let g = groups[r];
            slots.push((KeyRef::new(g, id_of(r)), reads.last));
            if let Some(b) = reads.before {
                slots.push((KeyRef::new(g, id_of(r)), b));
            }
        }
        // A run that's gone, as a runner from before the versions deletes
        // after a rollback, is forgotten, before a row of its key groups is
        // read: all reads see the same entries, the rest of the record's. A
        // run that's there and can't be read fails the attempt, saying where
        // it is, so a person can delete it.
        let found = loop {
            match self.layer.row(ENTRIES, &slots).await {
                Ok(found) => break found,
                Err(StateError::Gone(run)) if self.layer.forget(&run) => {
                    let run = self.location(flow, &run);
                    tracing::warn!(operator = %self.id, run = %run, "{GONE}");
                }
                Err(StateError::Unreadable { run, why }) => {
                    return Err(self.unreadable(flow, &run, &why));
                }
                Err(e) => return Err(self.failed(&e)),
            }
        };
        let mut found = found.into_iter();
        // This batch's entries, which reads of the layer don't see yet.
        let mut taken: HashMap<(u32, &[u8]), Entry> = HashMap::new();
        let mut out: Vec<u8> = raw.to_vec();
        let mut changed = false;
        for (&r, reads) in rows.iter().zip(reads) {
            let g = groups[r];
            let last = found.next().flatten();
            let before = match reads.before {
                Some(_) => found.next().flatten(),
                None => None,
            };
            let id = id_of(r);
            let (entry, fresh) = match taken.get(&(g, id)) {
                Some(e) => (Some(*e), true),
                None => match last.as_deref().and_then(Entry::decode) {
                    Some(e) => (Some(e), true),
                    None => (before.as_deref().and_then(Entry::decode), false),
                },
            };
            let owner = match entry {
                Some(e) if e.schema == schema => {
                    if e.hash != hashes[r] {
                        let v = version_id(id, hashes[r]);
                        out[r * 16..(r + 1) * 16].copy_from_slice(&v);
                        changed = true;
                    }
                    (!fresh).then_some(e)
                }
                _ => Some(Entry {
                    hash: hashes[r],
                    schema,
                }),
            };
            if let Some(e) = owner.filter(|_| write) {
                self.layer
                    .put_row(ENTRIES, KeyRef::new(g, id), reads.last, e.encode());
                taken.insert((g, id), e);
            }
        }
        if !changed {
            return Ok(batch);
        }
        let ids = FixedSizeBinaryArray::try_new(16, out.into(), ids.nulls().cloned())
            .map_err(|e| JobError::internal(e.to_string()))?;
        let mut columns = batch.columns().to_vec();
        columns[at] = std::sync::Arc::new(ids) as ArrayRef;
        RecordBatch::try_new(batch.schema(), columns).map_err(|e| JobError::internal(e.to_string()))
    }

    /// A key group's reads: those its horizons keep, and this one where
    /// the group hasn't moved on to it.
    fn reads(&self, g: u32) -> Reads {
        let last = self.layer.horizon(LAST, g);
        if last == i64::MIN {
            return Reads {
                before: None,
                last: self.read,
            };
        }
        if last >= self.read {
            let before = self.layer.horizon(ENTRIES, g);
            return Reads {
                before: (before > i64::MIN).then(|| before + 1),
                last,
            };
        }
        Reads {
            before: Some(last),
            last: self.read,
        }
    }

    /// Moves each of the lane's key groups on to the read, once, and again
    /// after a discard: the entries of the reads before the last expire.
    fn move_on(&mut self, range: KeyGroupRange) {
        if self.moved {
            return;
        }
        self.moved = true;
        for g in range.first..=range.last {
            let reads = self.reads(g);
            if let Some(b) = reads.before {
                self.layer.expire_in(ENTRIES, g, b - 1);
            }
            self.layer.expire_in(LAST, g, reads.last);
        }
    }

    /// Ends the epoch's versions: a redo's are dropped, as its record has
    /// them already; others are its record's.
    async fn barrier(&mut self, flow: &Flow, epoch: u64) -> Result<(), JobError> {
        let plan = &flow.plan;
        if plan.state.job.as_ref().is_some_and(|j| j.read_only()) {
            self.layer.discard();
            self.moved = false;
            return Ok(());
        }
        let snapshots = self.layer.barrier(epoch).await.map_err(|e| {
            crate::state::state_error(
                &format!("operator {}'s state at epoch {epoch}'s barrier", self.id),
                &e,
            )
        })?;
        let named = snapshots
            .into_iter()
            .map(|sn| {
                let sn = openetl_state::Snapshot {
                    layout: LAYOUT,
                    ..sn
                };
                crate::state::named(&self.id, &sn)
            })
            .collect::<Result<Vec<_>, _>>()?;
        flow.align
            .lock()
            .await
            .pieces(epoch)
            .snapshots
            .extend(named);
        Ok(())
    }

    /// Where run `run` of the versions is: a URL of the job's state.
    fn location(&self, flow: &Flow, run: &str) -> String {
        match &flow.plan.state.job {
            Some(j) => j.url(&self.id, run),
            None => run.to_owned(),
        }
    }

    /// The attempt's error where run `run` is there and can't be read: it
    /// names the stream and where the run is, and how to go on. A reset
    /// keeps the versions, and a rewind names the run again, so a person
    /// deletes it; the next attempt then finds it gone and forgets it.
    fn unreadable(&self, flow: &Flow, run: &str, why: &str) -> JobError {
        let plan = &flow.plan;
        let stream = plan
            .versions
            .iter()
            .position(|v| v.as_deref() == Some(self.id.as_str()))
            .map_or(self.id.as_str(), |s| plan.streams[s].as_str());
        JobError::new(
            ErrorCode::CheckpointCorrupt,
            format!(
                "stream {stream}: {} keeps the versions of the stream's records and can't be read: {why}. \
                 Delete it to go on: the next sync finds it gone and forgets it, and a row whose entry it held keeps its ID, \
                 so a second version of that row within one cursor value is skipped once",
                self.location(flow, run)
            ),
        )
    }
}

/// Key columns, by source name, as `schema` names them: a renamed column by
/// the name the schema's metadata gives its source name. A batch carries
/// its names, which the writer lane's engine may not know yet, as before a
/// new stream's first batch there.
fn named(key: &[String], schema: &Schema) -> Vec<String> {
    let renamed = source_names(schema).unwrap_or_default();
    key.iter()
        .map(|k| {
            renamed
                .iter()
                .find(|(_, source)| *source == k)
                .map_or(k, |(dest, _)| dest)
                .clone()
        })
        .collect()
}

/// The batch without its hashes, its rows' hashes and its schema's version;
/// None where it has none.
fn without_hashes(batch: &RecordBatch) -> Result<Option<(RecordBatch, Vec<u64>, u64)>, JobError> {
    let schema = batch.schema();
    let Ok(at) = schema.index_of(HASH_COLUMN) else {
        return Ok(None);
    };
    let f = schema.field(at);
    let version = f
        .metadata()
        .get(SCHEMA_KEY)
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| {
            JobError::new(
                ErrorCode::Internal,
                format!("{HASH_COLUMN:?} without its schema's version"),
            )
        })?;
    let hashes = batch
        .column(at)
        .as_primitive_opt::<arrow_array::types::UInt64Type>()
        .ok_or_else(|| JobError::internal(format!("{HASH_COLUMN:?} isn't u64")))?
        .values()
        .to_vec();
    let mut fields: Vec<_> = schema.fields().iter().cloned().collect();
    fields.remove(at);
    let mut columns = batch.columns().to_vec();
    columns.remove(at);
    let opts = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    let batch = RecordBatch::try_new_with_options(
        std::sync::Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        columns,
        &opts,
    )
    .map_err(|e| JobError::internal(e.to_string()))?;
    Ok(Some((batch, hashes, version)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_lanes_own_the_layouts_ranges() {
        for (groups, writers) in [(128u32, 1usize), (128, 3), (16, 5), (4, 4)] {
            let layout = crate::layout::Layout::new(groups, writers, writers)
                .unwrap_or_else(|e| panic!("{e}"));
            for w in 0..writers {
                let r = layout.writer(w);
                assert_eq!(
                    owned(groups, w, writers),
                    KeyGroupRange::new(*r.start(), *r.end())
                );
            }
        }
    }
}
