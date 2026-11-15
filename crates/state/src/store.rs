//! The seam under the memory tier: a store keeps a lane's committed state,
//! sorted by key, for one range of key groups, and makes each epoch's
//! changes a version that a snapshot names.

use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;

use crate::{KeyGroupRange, Result};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A key of the layout and its new value, or `None` for a delete.
pub type Change = (Bytes, Option<Bytes>);

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
    /// than `limit` means there are no more.
    fn scan<'a>(
        &'a self,
        start: &'a [u8],
        end: &'a [u8],
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<(Bytes, Bytes)>>>;

    /// Applies epoch `epoch`'s changes, sorted by key with each key once,
    /// makes the result a version, and returns the snapshots that name it.
    /// Nothing reads the version until a restore that a record names it
    /// for. An error leaves the store unusable.
    fn commit(&mut self, epoch: u64, changes: Vec<Change>) -> BoxFuture<'_, Result<Vec<Snapshot>>>;

    /// The versions this store committed before `epoch` can go: no record
    /// that's kept names them.
    fn release(&mut self, epoch: u64) -> BoxFuture<'_, Result<()>>;

    /// What it holds in memory, in bytes, about.
    fn memory_bytes(&self) -> usize;
}
