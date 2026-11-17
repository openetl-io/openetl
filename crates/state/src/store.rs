//! The seam under the memory tier: a store keeps a lane's committed state,
//! sorted by key, for one range of key groups, and makes each epoch's
//! changes a version that a snapshot names.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;

use crate::{KeyGroup, KeyGroupRange, OperatorId, Result};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A key of the layout and its new value, or `None` for a delete.
pub type Change = (Bytes, Option<Bytes>);

/// Each operator's horizon in each key group that has one, as
/// [`crate::StateBackend::expire`] moved it: its rows and timers at or
/// before it, and its set's entries that expire at or before it, are gone.
/// A version keeps them beside its state.
pub type Horizons = BTreeMap<(OperatorId, KeyGroup), i64>;

/// One version of a store's state, for a range of key groups: what the
/// checkpoint record keeps, and a restore opens. `data` means something
/// only to the kind of store that wrote it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub range: KeyGroupRange,
    /// The [`crate::LAYOUT`] of its keys.
    pub layout: u32,
    pub data: Bytes,
}

/// Where stores come from: object storage, or memory.
pub trait Stores: Send + Sync {
    /// A store for `range` holding exactly what `snapshots` hold of its key
    /// groups. Empty `snapshots` give an empty store; otherwise they're
    /// disjoint and hold every group of `range`. Its versions are
    /// `generation`'s, so an older attempt's can never be taken for them.
    fn open<'a>(
        &'a self,
        generation: u64,
        range: KeyGroupRange,
        snapshots: &'a [Snapshot],
    ) -> BoxFuture<'a, Result<Box<dyn Store>>>;
}

/// A lane's committed state. Its keys are the layout's, all in its range.
pub trait Store: Send + Sync {
    /// Each key's value, or `None`.
    fn get<'a>(&'a self, keys: &'a [Bytes]) -> BoxFuture<'a, Result<Vec<Option<Bytes>>>>;

    /// Up to `limit` entries with `start <= key < end`, in key order. Fewer
    /// than `limit` means there are no more. Entries a horizon has passed
    /// may be among them.
    fn scan<'a>(
        &'a self,
        start: &'a [u8],
        end: &'a [u8],
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<(Bytes, Bytes)>>>;

    /// The horizons the version it opened keeps, in its key groups.
    fn horizons(&self) -> Horizons;

    /// Applies epoch `epoch`'s changes, sorted by key with each key once,
    /// makes the result a version with `horizons`, and returns the
    /// snapshots that name it. What the horizons have passed the version
    /// may leave out. Nothing reads the version until a restore that a
    /// record names it for. An error leaves the store unusable.
    fn commit<'a>(
        &'a mut self,
        epoch: u64,
        changes: Vec<Change>,
        horizons: &'a Horizons,
    ) -> BoxFuture<'a, Result<Vec<Snapshot>>>;

    /// Whether it takes [`Store::flush`]: a store that keeps everything
    /// in memory releases nothing by it.
    fn flushes(&self) -> bool {
        false
    }

    /// Writes some of open epoch `epoch`'s changes before its barrier,
    /// sorted by key with each key once, so the memory they hold is
    /// released: the store reads them, under what a later flush or the
    /// barrier's commit writes, and the barrier's snapshots name them. They
    /// count only once a record names those. Changes that `horizons`
    /// passed it may leave out. An error leaves the store unusable.
    fn flush<'a>(
        &'a mut self,
        epoch: u64,
        changes: Vec<Change>,
        horizons: &'a Horizons,
    ) -> BoxFuture<'a, Result<()>> {
        let _ = (epoch, changes, horizons);
        Box::pin(std::future::ready(Err(crate::Error::Invalid(
            "this store keeps its changes in memory".into(),
        ))))
    }

    /// Forgets what [`Store::flush`] wrote of open epoch `epoch`, which
    /// failed: the store reads as its last commit left it.
    fn discard(&mut self, epoch: u64) {
        let _ = epoch;
    }

    /// The versions this store committed before `epoch` can go: no record
    /// that's kept names them.
    fn release(&mut self, epoch: u64) -> BoxFuture<'_, Result<()>>;

    /// What it holds in memory, in bytes, about.
    fn memory_bytes(&self) -> usize;
}
