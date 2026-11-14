//! Keyed state for the engine's operators: one layer for every consumer.
//!
//! A spike. Consumers see [`StateBackend`] and its four primitives: a value
//! per key, rows per key, a set with expiry, and timers. Behind it, the
//! [`layer::Layer`] keeps the epoch's changes and a cache of hot keys in
//! memory, and a [`Store`] makes each epoch's changes durable as immutable
//! runs on object storage, by key-group range. Two stores implement it, so
//! they can be measured against each other: [`runs::RunStore`], runs of our
//! own named by the checkpoint record, and [`slate::SlateStore`], SlateDB.
//!
//! State changes are durable when the record of their epoch is, and only
//! then: a store returns at a barrier the [`Snapshot`] the record keeps, and a
//! restore opens exactly the snapshots a record names.

use std::future::Future;
use std::ops::RangeInclusive;

use bytes::Bytes;

pub mod counting;
pub mod key;
pub mod layer;
pub mod runs;
#[cfg(feature = "slatedb")]
pub mod slate;

pub use key::{Kind, OperatorId};

/// A key's group, out of the pipeline's key groups.
pub type KeyGroup = u16;

/// Key groups `first` to `last`, inclusive, as the record names them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyGroupRange {
    pub first: KeyGroup,
    pub last: KeyGroup,
}

impl KeyGroupRange {
    pub fn new(first: KeyGroup, last: KeyGroup) -> Self {
        Self { first, last }
    }

    pub fn contains(&self, group: KeyGroup) -> bool {
        (self.first..=self.last).contains(&group)
    }

    pub fn overlaps(&self, other: &KeyGroupRange) -> bool {
        self.first <= other.last && other.first <= self.last
    }

    pub fn groups(&self) -> RangeInclusive<KeyGroup> {
        self.first..=self.last
    }

    /// `lanes` contiguous ranges of `groups` key groups, as lanes own them.
    pub fn split(groups: u32, lanes: u32) -> Vec<KeyGroupRange> {
        let lanes = lanes.max(1);
        (0..lanes)
            .map(|i| {
                let first = groups * i / lanes;
                let last = groups * (i + 1) / lanes - 1;
                KeyGroupRange::new(group(first), group(last))
            })
            .collect()
    }
}

fn group(g: u32) -> KeyGroup {
    KeyGroup::try_from(g).unwrap_or(KeyGroup::MAX)
}

/// A key of a consumer's, with its group.
#[derive(Clone, Copy, Debug)]
pub struct KeyRef<'a> {
    pub group: KeyGroup,
    pub key: &'a [u8],
}

/// What a store made durable for an epoch, for one key-group range: the
/// record keeps it, and a restore opens it. Opaque outside its store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub range: KeyGroupRange,
    pub data: Bytes,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("object store: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[cfg(feature = "slatedb")]
    #[error("slatedb: {0}")]
    SlateDb(#[from] slatedb::Error),
    #[error("corrupt state: {0}")]
    Corrupt(String),
    #[error("{0}")]
    Other(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Runs `futs`, at most `n` at once and finishing in any order, so one slow
/// request doesn't hold up the rest, and returns their outputs in order.
pub async fn unordered<T, F>(futs: Vec<F>, n: usize) -> Result<Vec<T>>
where
    F: Future<Output = Result<T>>,
{
    use futures::StreamExt;
    let mut out: Vec<Option<T>> = Vec::new();
    out.resize_with(futs.len(), || None);
    let mut s =
        futures::stream::iter(futs.into_iter().enumerate().map(index)).buffer_unordered(n.max(1));
    while let Some((i, r)) = s.next().await {
        out[i] = Some(r?);
    }
    Ok(out.into_iter().flatten().collect())
}

async fn index<T, F: Future<Output = T>>((i, f): (usize, F)) -> (usize, T) {
    (i, f.await)
}

/// A change in an epoch: a value, or `None` for a delete.
pub type Change = (Bytes, Option<Bytes>);

/// Durable runs of sorted keys, by key-group range. Keys are encoded by
/// [`key`], so a range's keys are contiguous.
pub trait Store: Send + Sync {
    /// The newest value of each key, or `None`.
    fn get_many(&self, keys: Vec<Bytes>)
    -> impl Future<Output = Result<Vec<Option<Bytes>>>> + Send;

    /// Every live entry with `start <= key < end`, in key order.
    fn scan(
        &self,
        start: Bytes,
        end: Bytes,
    ) -> impl Future<Output = Result<Vec<(Bytes, Bytes)>>> + Send;

    /// Makes epoch `epoch`'s changes durable, sorted by key, and returns what
    /// the record names. `watermark` is the epoch's: entries of a set that
    /// expire at or before it may be dropped from now on.
    fn commit(
        &mut self,
        epoch: u64,
        watermark: i64,
        changes: Vec<Change>,
    ) -> impl Future<Output = Result<Vec<Snapshot>>> + Send;

    /// The records before `epoch` are gone: what only they named can go.
    fn release(&mut self, epoch: u64) -> impl Future<Output = Result<()>> + Send;
}

/// The primitives every consumer of state uses. Batched and async: a
/// consumer calls them per Arrow batch, so lookups that miss memory overlap.
/// Times are event times, or times a record carries, in microseconds, never
/// the clock at the moment of a call.
pub trait StateBackend {
    /// A value per key: window accumulators, UDF state, a key's last row.
    fn get(
        &mut self,
        op: OperatorId,
        keys: &[KeyRef<'_>],
    ) -> impl Future<Output = Result<Vec<Option<Bytes>>>> + Send;
    fn put(&mut self, op: OperatorId, key: KeyRef<'_>, value: Bytes);
    fn delete(&mut self, op: OperatorId, key: KeyRef<'_>);

    /// Rows per key, ordered by `seq`: frames, rows waiting for the watermark.
    fn append(&mut self, op: OperatorId, key: KeyRef<'_>, seq: u64, row: Bytes);
    /// Takes every row of each key, oldest first, and deletes them.
    fn take_rows(
        &mut self,
        op: OperatorId,
        keys: &[KeyRef<'_>],
    ) -> impl Future<Output = Result<Vec<Vec<(u64, Bytes)>>>> + Send;

    /// A set with expiry: dedupe. Inserts each id that is absent, or expired
    /// as of its record's time `at`, to expire at `expires`, and says which
    /// were inserted. An id an earlier call of the same batch inserted counts.
    fn insert_new(
        &mut self,
        op: OperatorId,
        ids: &[KeyRef<'_>],
        at: &[i64],
        expires: &[i64],
    ) -> impl Future<Output = Result<Vec<bool>>> + Send;

    /// Timers: a key at a time, fired once the watermark passes it.
    fn set_timer(&mut self, op: OperatorId, time: i64, key: KeyRef<'_>);
    /// Removes and returns the timers at or before `watermark`, in time order.
    fn fire(&mut self, op: OperatorId, watermark: i64) -> Vec<(i64, KeyGroup, Bytes)>;

    /// The barrier of epoch `epoch` with its watermark: makes the epoch's
    /// changes durable and returns what the record names.
    fn barrier(
        &mut self,
        epoch: u64,
        watermark: i64,
    ) -> impl Future<Output = Result<Vec<Snapshot>>> + Send;
}
