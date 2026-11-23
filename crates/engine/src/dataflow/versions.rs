//! The versions of a stream's records, at a writer lane: for each record ID
//! of its key groups, the hash of the version that holds it. A version read
//! again with another hash gets an ID of its own, so that a destination that
//! skips the IDs it has takes it: a second change within one value of the
//! cursor, which the next read gives again at its inclusive floor.
//!
//! An entry is the owner's hash and its schema's version, at the read that
//! last gave its ID, by the read's start. The entries the stream's last two
//! reads gave stay, and the reads before those expire as a read gives the
//! first row of a key group, a read given again once a row of the read
//! before it was. A row without a live entry of its schema's version keeps
//! its ID, and its hash becomes the owner. So does a row of a key group the
//! writer lane doesn't keep: an entry is missed, and the version skipped as
//! it was before, but never given two IDs. Versions of a layout other than
//! this engine's start again, as versions that can't be read do.

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
/// The layout of a lane's versions, as its snapshots name it: the state's,
/// and the versions' own in the bits above it, so that an engine that
/// keeps them otherwise starts them again rather than misread them.
const LAYOUT: u32 = openetl_state::LAYOUT | (1 << 16);
/// Each key group's reads, as a value: the last two that gave a row of it.
const READS: &[u8] = b"reads";

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
    groups: HashMap<u32, Reads>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Reads {
    before: Option<i64>,
    last: i64,
}

impl Reads {
    fn decode(b: &[u8]) -> Option<Self> {
        let last = i64::from_le_bytes(b.get(..8)?.try_into().ok()?);
        let before = match b.get(8..16) {
            Some(p) => Some(i64::from_le_bytes(p.try_into().ok()?)),
            None => None,
        };
        Some(Reads { before, last })
    }

    fn encode(&self) -> Bytes {
        let mut b = self.last.to_le_bytes().to_vec();
        if let Some(p) = self.before {
            b.extend_from_slice(&p.to_le_bytes());
        }
        Bytes::from(b)
    }
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
                Err(e @ (StateError::Snapshot(_) | StateError::Corrupt(_))) => {
                    tracing::warn!(
                        stream = %plan.streams[s],
                        error = %e,
                        "the record's versions of the stream's records can't be read, so a record read again keeps its ID until they're kept again"
                    );
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
                groups: HashMap::new(),
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
        self.reads(rows.iter().map(|&r| groups[r]), write).await?;
        let mut slots = Vec::with_capacity(rows.len() * 2);
        for &r in &rows {
            let g = groups[r];
            let reads = self.groups[&g];
            slots.push((KeyRef::new(g, id_of(r)), reads.last));
            if let Some(b) = reads.before {
                slots.push((KeyRef::new(g, id_of(r)), b));
            }
        }
        let found = self
            .layer
            .rows(ENTRIES, &slots)
            .await
            .map_err(|e| self.failed(&e))?;
        let mut found = found.into_iter();
        // This batch's entries, which reads of the layer don't see yet.
        let mut taken: HashMap<(u32, &[u8]), Entry> = HashMap::new();
        let mut out: Vec<u8> = raw.to_vec();
        let mut changed = false;
        for &r in &rows {
            let g = groups[r];
            let reads = self.groups[&g];
            let last = found.next().unwrap_or_default();
            let before = match reads.before {
                Some(_) => found.next().unwrap_or_default(),
                None => Vec::new(),
            };
            let id = id_of(r);
            let (entry, fresh) = match taken.get(&(g, id)) {
                Some(e) => (Some(*e), true),
                None => match last.last().and_then(|b| Entry::decode(b)) {
                    Some(e) => (Some(e), true),
                    None => (before.last().and_then(|b| Entry::decode(b)), false),
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
                    .append(ENTRIES, KeyRef::new(g, id), reads.last, e.encode());
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

    /// Loads each group's reads the lane hasn't yet, and moves those of a
    /// group the read hasn't given a row of before on to it, which expires
    /// the entries of the reads before the last.
    async fn reads(
        &mut self,
        groups: impl Iterator<Item = u32>,
        write: bool,
    ) -> Result<(), JobError> {
        let mut new: Vec<u32> = groups.filter(|g| !self.groups.contains_key(g)).collect();
        new.sort_unstable();
        new.dedup();
        if new.is_empty() {
            return Ok(());
        }
        let keys: Vec<KeyRef<'_>> = new.iter().map(|&g| KeyRef::new(g, READS)).collect();
        let got = self
            .layer
            .get(ENTRIES, &keys)
            .await
            .map_err(|e| self.failed(&e))?;
        for (g, v) in new.into_iter().zip(got) {
            let was = v.as_deref().and_then(Reads::decode);
            let reads = match was {
                Some(r) if r.last >= self.read => r,
                Some(r) => Reads {
                    before: Some(r.last),
                    last: self.read,
                },
                None => Reads {
                    before: None,
                    last: self.read,
                },
            };
            if write && was != Some(reads) {
                self.layer
                    .put(ENTRIES, KeyRef::new(g, READS), reads.encode());
                if let Some(b) = reads.before {
                    self.layer.expire_in(ENTRIES, g, b - 1);
                }
            }
            self.groups.insert(g, reads);
        }
        Ok(())
    }

    /// Ends the epoch's versions: a redo's are dropped, as its record has
    /// them already; others are its record's.
    async fn barrier(&mut self, flow: &Flow, epoch: u64) -> Result<(), JobError> {
        let plan = &flow.plan;
        if plan.state.job.as_ref().is_some_and(|j| j.read_only()) {
            self.layer.discard();
            self.groups.clear();
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
}

/// The batch without its hashes, its rows' hashes and its schema's version;
/// None where it has none.
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
