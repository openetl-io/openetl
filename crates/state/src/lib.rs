//! Keyed state for the engine's stateful operators: one layer for every
//! consumer, which reaches it through [`StateBackend`] and its four
//! primitives:
//!
//! - a value per key: window accumulators, UDF state, a key's last row;
//! - rows per key, by the time they fall due: rows waiting for the
//!   watermark;
//! - a set with expiry: dedupe on `_openetl_id`;
//! - timers: closing windows, expiring state.
//!
//! A new stateful feature adds a primitive here, never a store of its own.
//!
//! State is scoped by operator and key group. A lane opens one [`Layer`] for
//! its range of key groups, which all its operators share. The layer keeps
//! the open epoch's changes in memory over a [`Store`], and a failed epoch
//! discards them. A barrier adds them to the lane's pending set, its
//! changes since it last committed to the store. While the pending set is
//! within the lane's share of what a record may carry, the [`Snapshot`]s the
//! checkpoint record names carry it, and the store writes nothing; past it,
//! the barrier commits it to the store, and the set starts empty. A restore
//! opens exactly what a record names, its pending sets too, for any range
//! of its key groups, so state moves by range to other lanes and workers.
//! State is durable when the record of its epoch is, and only then: a layer
//! never reads a version no record names.
//!
//! Times are `i64`s from the data, by convention microseconds of event time,
//! or a time a record carries; never the clock at a call, so a replay sees
//! what the first run saw. `i64::MAX` is the watermark at the end of input.
//!
//! Two stores are here: [`RunStores`], runs of our own on object storage,
//! and [`MemoryBucket`], which keeps every version in the process's memory,
//! for tests: nothing it holds outlives the process.

use std::future::Future;

use bytes::Bytes;

mod layer;
mod layout;
mod memory;
mod pending;
pub mod runs;
mod store;

pub use layer::{Layer, Memory};
pub use layout::{LAYOUT, SectionKind};
pub use memory::MemoryBucket;
pub use pending::VERSION as PENDING_VERSION;
pub use runs::{MergeConfig, ObjectStorage, Objects, RunConfig, RunStores};
pub use store::{BoxFuture, Change, Horizons, PendingLimit, Snapshot, Store, Stores};

/// A key's group, out of the pipeline's key groups, as routing gives it.
/// The layer takes groups up to `u16::MAX`.
pub type KeyGroup = u32;

/// A stateful operator, stable across the pipeline's versions while its
/// state's meaning is: the plan derives it from the operator's ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OperatorId(pub u32);

/// An expiry that never comes: a set's entry kept for good.
pub const NEVER: i64 = i64::MAX;

/// Key groups `first` to `last`, inclusive.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct KeyGroupRange {
    pub first: KeyGroup,
    pub last: KeyGroup,
}

impl KeyGroupRange {
    pub fn new(first: KeyGroup, last: KeyGroup) -> Self {
        Self { first, last }
    }

    pub fn contains(&self, group: KeyGroup) -> bool {
        self.first <= group && group <= self.last
    }

    pub fn overlaps(&self, other: &KeyGroupRange) -> bool {
        self.first <= other.last && other.first <= self.last
    }
}

/// A consumer's key and its group. The key's bytes are the consumer's own
/// encoding: a window's group keys and start, an `_openetl_id`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyRef<'a> {
    pub group: KeyGroup,
    pub key: &'a [u8],
}

impl<'a> KeyRef<'a> {
    pub fn new(group: KeyGroup, key: &'a [u8]) -> Self {
        Self { group, key }
    }
}

/// A timer that fired.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Timer {
    pub time: i64,
    pub group: KeyGroup,
    pub key: Bytes,
}

/// A row the watermark released.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DueRow {
    pub group: KeyGroup,
    pub time: i64,
    pub key: Bytes,
    pub row: Bytes,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The caller's mistake: a key outside the lane's key groups, slices of
    /// different lengths, an epoch out of turn.
    #[error("{0}")]
    Invalid(String),
    /// A snapshot this layer can't restore.
    #[error("snapshot: {0}")]
    Snapshot(String),
    /// What a store holds doesn't parse.
    #[error("corrupt state: {0}")]
    Corrupt(String),
    #[error("store: {0}")]
    Store(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The primitives every consumer of state uses, scoped by operator and key
/// group. Reads are batched and async, called per Arrow batch, so lookups
/// that miss memory overlap. Writes go to the open epoch's changes and
/// can't fail there: a write outside the lane's key groups fails the
/// epoch's barrier instead. A read outside them is an error.
///
/// Keys order rows and timers that tie on time and group: a shorter key
/// first, then bytewise.
pub trait StateBackend: Send {
    /// The value of each key, or `None`.
    fn get(
        &mut self,
        op: OperatorId,
        keys: &[KeyRef<'_>],
    ) -> impl Future<Output = Result<Vec<Option<Bytes>>>> + Send;

    fn put(&mut self, op: OperatorId, key: KeyRef<'_>, value: Bytes);

    fn delete(&mut self, op: OperatorId, key: KeyRef<'_>);

    /// Adds `row` to the key's rows at `time`, the time it falls due: a
    /// window's end, a row's event time. A row at or before the operator's
    /// horizon is gone at once.
    fn append(&mut self, op: OperatorId, key: KeyRef<'_>, time: i64, row: Bytes);

    /// The rows of each key at each time, in the order appended. They stay.
    fn rows(
        &mut self,
        op: OperatorId,
        slots: &[(KeyRef<'_>, i64)],
    ) -> impl Future<Output = Result<Vec<Vec<Bytes>>>> + Send;

    /// Removes and returns up to `limit` rows at or before `watermark`: by
    /// key group, then by time, then by key, and a key's at a time in the
    /// order appended. Fewer than `limit` means none are left.
    fn take_rows(
        &mut self,
        op: OperatorId,
        watermark: i64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<DueRow>>> + Send;

    /// For each id in turn, whether it's new: absent, or expired as of
    /// `at`, its record's time. A new id is kept to expire at its
    /// `expires`, or [`NEVER`]; a repeat keeps the expiry it had. An entry
    /// is live while its expiry is past both `at` and the operator's
    /// horizon. An id that an earlier one in the call inserted is a repeat.
    /// An `expires` other than [`NEVER`] at or before its `at` keeps
    /// nothing, so a call of only those asks which ids are live and changes
    /// nothing. [`NEVER`] keeps a new id whatever its `at`.
    fn insert_new(
        &mut self,
        op: OperatorId,
        ids: &[KeyRef<'_>],
        at: &[i64],
        expires: &[i64],
    ) -> impl Future<Output = Result<Vec<bool>>> + Send;

    /// Moves the operator's horizon to `horizon` in the lane's key groups,
    /// if it's later: its rows and timers at or before it, and its set's
    /// entries that expire at or before it, are gone for every later call,
    /// whatever its `at`, and the store may drop them. A consumer moves it
    /// with its watermark, once it has taken the rows and fired the timers
    /// it wants. `i64::MIN` is no horizon.
    fn expire(&mut self, op: OperatorId, horizon: i64);

    /// As [`StateBackend::expire`], in key group `group` of the lane alone:
    /// for a consumer whose groups stand at different times, as lanes
    /// restored onto one may.
    fn expire_in(&mut self, op: OperatorId, group: KeyGroup, horizon: i64);

    /// The operator's horizon in a key group of the lane, or `i64::MIN` for
    /// none. Lanes restored onto one may bring their groups different
    /// horizons. What's at or before a group's is gone, so a consumer takes
    /// it as the least of its watermark in that group: it sets no timer
    /// and appends no row there that the horizon passed.
    fn horizon(&self, op: OperatorId, group: KeyGroup) -> i64;

    /// Sets a timer for the key at `time`. A timer is set or not: setting
    /// it again changes nothing. A timer at or before the operator's
    /// horizon is gone at once.
    fn set_timer(&mut self, op: OperatorId, key: KeyRef<'_>, time: i64);

    fn delete_timer(&mut self, op: OperatorId, key: KeyRef<'_>, time: i64);

    /// Removes and returns up to `limit` timers at or before `watermark`, by
    /// time, then by key group, then by key. A timer fires once. Fewer than
    /// `limit` means none are left.
    fn fire(
        &mut self,
        op: OperatorId,
        watermark: i64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<Timer>>> + Send;
}
