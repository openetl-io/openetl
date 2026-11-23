//! The memory tier: a lane's open epoch of changes, and its pending set,
//! over its store.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, btree_map};
use std::iter::Peekable;
use std::ops::Bound;

use bytes::Bytes;

use crate::layout::{self, SectionKind};
use crate::pending;
use crate::store::{Change, Horizons, Snapshot, Store, Stores};
use crate::{
    DueRow, Error, KeyGroup, KeyGroupRange, KeyRef, LAYOUT, NEVER, OperatorId, Result,
    StateBackend, Timer,
};

/// What an entry costs in memory beyond its key's and value's bytes, about:
/// its share of a map's nodes and its handles.
pub(crate) const ENTRY_OVERHEAD: usize = 64;

/// Entries asked of the store at once in a scan.
const PAGE: usize = 1024;

/// What a layer holds in memory, in bytes, about.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Memory {
    /// The open epoch's changes.
    pub changes: usize,
    /// The pending set's.
    pub pending: usize,
    /// The store's.
    pub store: usize,
}

impl Memory {
    pub fn total(&self) -> usize {
        self.changes + self.pending + self.store
    }
}

/// The seq of a row [`Layer::put_row`] puts, which no appended row has:
/// their epochs start at 1.
const PUT: u64 = 0;

/// A lane's state, for its range of key groups and every operator of the
/// lane, as [`StateBackend`] serves it. The open epoch's changes stay in
/// memory until [`Layer::barrier`] adds them to the pending set, or
/// [`Layer::discard`] drops them. The pending set holds the lane's changes
/// since it last committed to its store, newer than all the store holds,
/// and stays in memory while the record carries it. Under memory pressure,
/// [`Layer::flush`] writes the pending set and the open epoch's changes to
/// the store early, which reads them from then on: before the barrier,
/// where the layer may pass [`Layer::set_flush_at`]'s threshold, and
/// wherever its consumer calls it.
pub struct Layer {
    store: Box<dyn Store>,
    range: KeyGroupRange,
    /// The epoch the next barrier closes.
    epoch: u64,
    /// The open epoch's changes by key: a value, or `None` for a delete.
    changes: BTreeMap<Bytes, Option<Bytes>>,
    change_bytes: usize,
    /// The changes of the epochs since the lane last committed to its
    /// store, as `changes` holds them.
    pending: BTreeMap<Bytes, Option<Bytes>>,
    pending_bytes: usize,
    /// What a record may carry of the pending set, in bytes of its
    /// encoding.
    share: usize,
    /// Barriers that committed a pending set to the store.
    commits: u64,
    /// Each operator's horizon in each group, as the store keeps it.
    horizons: Horizons,
    /// The set's horizons a store of layout 1 kept as values, by operator
    /// and group, as read.
    kept: HashMap<(OperatorId, u16), i64>,
    /// Horizons the open epoch moved, for every group of the range.
    moved: HashMap<OperatorId, i64>,
    /// Horizons the open epoch moved in one group.
    moved_in: HashMap<(OperatorId, u16), i64>,
    /// Rows appended in the open epoch.
    appended: u32,
    /// What fails the open epoch's barrier: the first write it can't keep.
    fault: Option<Error>,
    /// A barrier failed, so the layer is unusable.
    failed: bool,
    /// The open epoch's changes past which a read flushes them first; 0
    /// for never.
    flush_at: usize,
    /// Early flushes it made.
    flushes: u64,
}

impl Layer {
    /// Opens a lane's state for `range`, as `snapshots` hold it: those of
    /// the record it restores, empty for none, with the part of their
    /// pending sets in `range`. `epoch` is the first the lane commits, one
    /// past the record's, and `generation` the attempt's.
    pub async fn open(
        stores: &dyn Stores,
        generation: u64,
        epoch: u64,
        range: KeyGroupRange,
        snapshots: &[Snapshot],
    ) -> Result<Layer> {
        if range.first > range.last || range.last > u32::from(u16::MAX) {
            return Err(Error::Invalid(format!(
                "key groups {} to {}: the layer takes a range within 0 to {}",
                range.first,
                range.last,
                u16::MAX
            )));
        }
        if epoch == 0 {
            return Err(Error::Invalid("epochs start at 1".into()));
        }
        if let Some(s) = snapshots.iter().find(|s| s.layout != LAYOUT) {
            return Err(Error::Snapshot(format!(
                "layout {}, where this engine reads {LAYOUT}",
                s.layout
            )));
        }
        let store = stores.open(generation, range, snapshots).await?;
        let mut horizons = store.horizons();
        horizons.retain(|(_, g), h| range.contains(*g) && *h > i64::MIN);
        // Snapshots are disjoint, so a key is in one pending set at most.
        let mut pending = BTreeMap::new();
        for s in snapshots.iter().filter(|s| s.range.overlaps(&range)) {
            for (k, v) in pending::decode(&s.pending, s.range)? {
                if layout::group_of(&k).is_some_and(|g| range.contains(g.into())) {
                    pending.insert(k, v);
                }
            }
        }
        let pending_bytes = memory_of(&pending);
        Ok(Layer {
            store,
            range,
            epoch,
            changes: BTreeMap::new(),
            change_bytes: 0,
            pending,
            pending_bytes,
            share: stores.pending_share(range),
            commits: 0,
            horizons,
            kept: HashMap::new(),
            moved: HashMap::new(),
            moved_in: HashMap::new(),
            appended: 0,
            fault: None,
            failed: false,
            flush_at: 0,
            flushes: 0,
        })
    }

    /// Has each read first flush the open epoch's changes early once they
    /// hold more than `bytes`, where the store takes early flushes; 0 for
    /// never, as a layer that may write nothing needs.
    pub fn set_flush_at(&mut self, bytes: usize) {
        self.flush_at = bytes;
    }

    /// The early flushes it made.
    pub fn flushes(&self) -> u64 {
        self.flushes
    }

    /// Writes the pending set and the open epoch's changes to the store
    /// before the barrier, so their memory goes, where the store takes
    /// that; the store reads them from then on, and the barrier's snapshots
    /// name them, carrying no pending set but what the epoch changes after.
    /// Whether it wrote. They count once a record names those snapshots, as
    /// the barrier's own do; if the epoch is discarded, the store stops
    /// reading the epoch's changes and goes on reading the pending set it
    /// took. After an error the layer is unusable, as after a barrier's.
    pub async fn flush(&mut self) -> Result<bool> {
        self.usable()?;
        let empty = self.changes.is_empty() && self.pending.is_empty();
        if empty || self.fault.is_some() || !self.store.flushes() {
            return Ok(false);
        }
        self.failed = true;
        // The pending set is older than the epoch's changes and newer than
        // all the store holds, so it goes first, as a run a discard of the
        // epoch leaves; the store reads both from then on.
        if !self.pending.is_empty() {
            // What the open epoch's horizons passed stays: a discard undoes
            // them, and not this.
            let pending: Vec<Change> = std::mem::take(&mut self.pending)
                .into_iter()
                .filter(|(k, v)| !self.passed_by(k, v.as_deref(), false))
                .collect();
            self.store
                .flush(self.epoch, pending, &self.horizons, true)
                .await?;
            self.pending_bytes = 0;
        }
        if !self.changes.is_empty() {
            let changes = std::mem::take(&mut self.changes)
                .into_iter()
                .filter(|(k, v)| !self.passed(k, v.as_deref()))
                .collect();
            self.store
                .flush(self.epoch, changes, &self.horizons, false)
                .await?;
            self.change_bytes = 0;
        }
        self.flushes += 1;
        self.failed = false;
        Ok(true)
    }

    /// Flushes the pending set and the open epoch's changes early if they
    /// passed the threshold.
    async fn relieve(&mut self) -> Result<()> {
        if self.flush_at > 0 && self.change_bytes + self.pending_bytes > self.flush_at {
            self.flush().await?;
        }
        Ok(())
    }

    pub fn range(&self) -> KeyGroupRange {
        self.range
    }

    /// The epoch the next barrier closes.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn memory(&self) -> Memory {
        Memory {
            changes: self.change_bytes,
            pending: self.pending_bytes,
            store: self.store.memory_bytes(),
        }
    }

    /// What a record may carry of the lane's pending set, in bytes of its
    /// encoding: its share of the job's.
    pub fn pending_share(&self) -> usize {
        self.share
    }

    /// How many of its barriers committed a pending set, not empty, to the
    /// store.
    pub fn commits(&self) -> u64 {
        self.commits
    }

    /// Reads now what a read of the store's version needs first, so that
    /// what it can't read, as a run that's gone, is an error here.
    pub async fn check(&self) -> Result<()> {
        self.store.check().await
    }

    /// Puts `row` as the key's one row at `time`, in place of the one put
    /// there before, which [`Layer::row`] reads by its key rather than a
    /// scan. A slot takes these or [`StateBackend::append`]'s, not both. A
    /// row at or before the operator's horizon is gone at once.
    pub fn put_row(&mut self, op: OperatorId, key: KeyRef<'_>, time: i64, row: Bytes) {
        if let Some(g) = self.place(key.group)
            && !self.gone(op, g, time)
        {
            self.change(layout::row(g, op, time, key.key, PUT), Some(row));
        }
    }

    /// The row [`Layer::put_row`] put at each key's time, or `None`, read
    /// together.
    pub async fn row(
        &mut self,
        op: OperatorId,
        slots: &[(KeyRef<'_>, i64)],
    ) -> Result<Vec<Option<Bytes>>> {
        self.usable()?;
        self.relieve().await?;
        let mut out = Vec::with_capacity(slots.len());
        let (mut missing, mut at) = (Vec::new(), Vec::new());
        for (i, (k, time)) in slots.iter().enumerate() {
            let g = self.group(k.group)?;
            if self.gone(op, g, *time) {
                out.push(None);
                continue;
            }
            let key = layout::row(g, op, *time, k.key, PUT);
            match self.changed(&key) {
                Some(v) => out.push(v.clone()),
                None => {
                    out.push(None);
                    missing.push(key);
                    at.push(i);
                }
            }
        }
        if !missing.is_empty() {
            let got = self.store.get(&missing).await?;
            for (i, v) in at.into_iter().zip(got) {
                out[i] = v;
            }
        }
        Ok(out)
    }

    /// The barrier of epoch `epoch`, the open one: adds its changes to the
    /// pending set, and returns the snapshots the record names for the
    /// lane. They carry the pending set while its encoding is within the
    /// lane's share; past it, the barrier commits the set to the store,
    /// and the set starts empty. What the horizons passed leaves the set,
    /// and so do deletes of what the store can't hold. The snapshots count
    /// once the record is written. Another epoch is refused; after any
    /// other error the layer is unusable, and the lane opens again from a
    /// record.
    pub async fn barrier(&mut self, epoch: u64) -> Result<Vec<Snapshot>> {
        self.usable()?;
        if epoch != self.epoch {
            return Err(Error::Invalid(format!(
                "the barrier of epoch {epoch}, where epoch {} is open",
                self.epoch
            )));
        }
        self.failed = true;
        if let Some(e) = self.fault.take() {
            return Err(e);
        }
        self.settle_horizons();
        let mut pending = std::mem::take(&mut self.pending);
        pending.append(&mut self.changes);
        pending.retain(|k, v| {
            !self.passed(k, v.as_deref()) && (v.is_some() || self.store.may_hold(k))
        });
        let changes: Vec<Change> = match pending::encoded_len(pending.iter()) <= self.share {
            true => Vec::new(),
            false => std::mem::take(&mut pending).into_iter().collect(),
        };
        let committed = !changes.is_empty();
        let mut snapshots = self.store.commit(epoch, changes, &self.horizons).await?;
        self.commits += u64::from(committed);
        for s in &mut snapshots {
            let r = s.range;
            s.pending = match r.contains(self.range.first) && r.contains(self.range.last) {
                true => pending::encode(pending.iter()),
                // Timers aren't ordered by group.
                false => {
                    let mine: Vec<_> = pending
                        .iter()
                        .filter(|(k, _)| layout::group_of(k).is_some_and(|g| r.contains(g.into())))
                        .collect();
                    pending::encode(mine.into_iter())
                }
            };
        }
        self.pending_bytes = memory_of(&pending);
        self.pending = pending;
        self.epoch += 1;
        self.change_bytes = 0;
        self.appended = 0;
        self.failed = false;
        Ok(snapshots)
    }

    /// Drops the open epoch's changes, which failed: the layer is as the
    /// last barrier left it, its pending set too, and the epoch is open
    /// again.
    pub fn discard(&mut self) {
        self.store.discard(self.epoch);
        self.changes.clear();
        self.change_bytes = 0;
        self.moved.clear();
        self.moved_in.clear();
        self.appended = 0;
        self.fault = None;
    }

    /// The versions this lane committed before `epoch` can go: no record
    /// that's kept names them.
    pub async fn release(&mut self, epoch: u64) -> Result<()> {
        self.usable()?;
        self.store.release(epoch).await
    }

    fn usable(&self) -> Result<()> {
        match self.failed {
            true => Err(Error::Invalid(
                "a barrier failed: open the lane again from a record".into(),
            )),
            false => Ok(()),
        }
    }

    /// Moves the store's horizons as the epoch moved them.
    fn settle_horizons(&mut self) {
        for (op, h) in std::mem::take(&mut self.moved) {
            if h == i64::MIN {
                continue;
            }
            for g in self.range.first..=self.range.last {
                let at = self.horizons.entry((op, g)).or_insert(h);
                *at = (*at).max(h);
            }
        }
        for ((op, g), h) in std::mem::take(&mut self.moved_in) {
            let at = self.horizons.entry((op, g.into())).or_insert(h);
            *at = (*at).max(h);
        }
    }

    /// Whether a change is of a row, a timer or a set's entry that its
    /// operator's horizon has passed, so the store needn't keep it.
    fn passed(&self, k: &[u8], v: Option<&[u8]>) -> bool {
        self.passed_by(k, v, true)
    }

    /// As [`Layer::passed`], by the store's horizons alone where `moved`
    /// is false: the open epoch may yet be discarded, and its moves with
    /// it.
    fn passed_by(&self, k: &[u8], v: Option<&[u8]>, moved: bool) -> bool {
        let (Some(kind), Some(op), Some(g)) = (
            layout::section_of(k),
            layout::operator_of(k),
            layout::group_of(k),
        ) else {
            return false;
        };
        let time = match kind {
            SectionKind::Values => None,
            SectionKind::Rows => layout::parse_row(k).map(|r| r.1),
            SectionKind::Timers => layout::parse_timer(k).map(|t| t.0),
            SectionKind::Set => v.and_then(layout::parse_time_value).filter(|e| *e != NEVER),
        };
        time.is_some_and(|t| match moved {
            true => self.gone(op, g, t),
            false => self
                .horizons
                .get(&(op, g.into()))
                .is_some_and(|&h| h > i64::MIN && t <= h),
        })
    }

    /// The operator's horizon in a group: the store's, or the open epoch's
    /// move; `i64::MIN` for none.
    fn horizon_in(&self, op: OperatorId, g: u16) -> i64 {
        let stored = self
            .horizons
            .get(&(op, g.into()))
            .copied()
            .unwrap_or(i64::MIN);
        let moved = |h: Option<&i64>| h.copied().unwrap_or(i64::MIN);
        stored
            .max(moved(self.moved.get(&op)))
            .max(moved(self.moved_in.get(&(op, g))))
    }

    /// Whether the operator's horizon in a group has passed `time`.
    fn gone(&self, op: OperatorId, g: u16, time: i64) -> bool {
        let h = self.horizon_in(op, g);
        h > i64::MIN && time <= h
    }

    /// The least horizon the operator has in the lane's groups.
    fn least_horizon(&self, op: OperatorId) -> i64 {
        self.groups()
            .into_iter()
            .map(|g| self.horizon_in(op, g))
            .min()
            .unwrap_or(i64::MIN)
    }

    /// Reads the layout-1 horizons of `want` that aren't read yet.
    async fn load_horizons(&mut self, want: &[(OperatorId, u16)]) -> Result<()> {
        let mut need: Vec<(OperatorId, u16)> = want
            .iter()
            .filter(|w| !self.kept.contains_key(w))
            .copied()
            .collect();
        need.sort();
        need.dedup();
        if need.is_empty() {
            return Ok(());
        }
        let keys: Vec<Bytes> = need
            .iter()
            .map(|(op, g)| layout::horizon(*g, *op))
            .collect();
        let got = self.store.get(&keys).await?;
        for (w, v) in need.into_iter().zip(got) {
            let h = match v {
                Some(v) => layout::parse_time_value(&v)
                    .ok_or_else(|| Error::Corrupt("a horizon that isn't a time".into()))?,
                None => i64::MIN,
            };
            self.kept.insert(w, h);
        }
        Ok(())
    }

    /// The lane's key groups, which [`Layer::open`] checked fit a u16.
    fn groups(&self) -> Vec<u16> {
        (self.range.first..=self.range.last)
            .filter_map(|g| u16::try_from(g).ok())
            .collect()
    }

    /// The horizon a set's entry must expire after: the operator's, or the
    /// one a store of layout 1 kept, which [`Layer::load_horizons`] read.
    fn set_horizon(&self, op: OperatorId, g: u16) -> i64 {
        let kept = self.kept.get(&(op, g)).copied().unwrap_or(i64::MIN);
        self.horizon_in(op, g).max(kept)
    }

    /// A read's group, which must be the lane's.
    fn group(&self, g: KeyGroup) -> Result<u16> {
        match self.range.contains(g) {
            true => u16::try_from(g).map_err(|_| self.outside(g)),
            false => Err(self.outside(g)),
        }
    }

    /// A write's group, or `None` and the epoch's fault.
    fn place(&mut self, g: KeyGroup) -> Option<u16> {
        match self.group(g) {
            Ok(g) => Some(g),
            Err(e) => {
                self.fault.get_or_insert(e);
                None
            }
        }
    }

    fn outside(&self, g: KeyGroup) -> Error {
        Error::Invalid(format!(
            "key group {g} is outside the lane's, {} to {}",
            self.range.first, self.range.last
        ))
    }

    /// A key's change: the open epoch's, or else the pending set's.
    fn changed(&self, k: &[u8]) -> Option<&Option<Bytes>> {
        self.changes.get(k).or_else(|| self.pending.get(k))
    }

    fn change(&mut self, k: Bytes, v: Option<Bytes>) {
        let (key, value) = (k.len(), v.as_ref().map_or(0, |v| v.len()));
        match self.changes.insert(k, v) {
            Some(old) => {
                self.change_bytes -= old.map_or(0, |v| v.len());
                self.change_bytes += value;
            }
            None => self.change_bytes += key + value + ENTRY_OVERHEAD,
        }
    }

    /// A row's seq, the epoch's and the row's count in it: rows appended
    /// later sort later, across epochs too, and none of a restored record's
    /// can tie with a new one's.
    fn next_seq(&mut self) -> Option<u64> {
        let epoch = u32::try_from(self.epoch).ok()?;
        let n = self.appended;
        self.appended = n.checked_add(1)?;
        Some((u64::from(epoch) << 32) | u64::from(n))
    }

    /// Up to `limit` live entries with `start <= key < end`, in key order:
    /// the store's, under the pending set, under the open epoch's changes.
    async fn live(&self, start: &[u8], end: &[u8], limit: usize) -> Result<Vec<(Bytes, Bytes)>> {
        let mut out = Vec::new();
        if limit == 0 || start >= end {
            return Ok(out);
        }
        let bounds = (Bound::Included(start), Bound::Excluded(end));
        let mut changes = Overlay {
            newer: self.changes.range::<[u8], _>(bounds).peekable(),
            older: self.pending.range::<[u8], _>(bounds).peekable(),
        }
        .peekable();
        let mut page = Vec::new().into_iter();
        let mut from = Bytes::copy_from_slice(start);
        let mut more = true;
        let mut stored: Option<(Bytes, Bytes)> = None;
        while out.len() < limit {
            if stored.is_none() {
                stored = page.next();
                if stored.is_none() && more {
                    let got = self.store.scan(&from, end, PAGE).await?;
                    more = got.len() == PAGE;
                    if let Some((k, _)) = got.last() {
                        from = layout::after(k);
                    }
                    page = got.into_iter();
                    stored = page.next();
                }
            }
            let changed_first = match (&stored, changes.peek()) {
                (None, None) => break,
                (Some(_), None) => false,
                (None, Some(_)) => true,
                (Some((s, _)), Some((c, _))) => c <= &s,
            };
            if !changed_first {
                out.extend(stored.take());
                continue;
            }
            if let Some((k, v)) = changes.next() {
                if stored.as_ref().is_some_and(|(s, _)| s == k) {
                    stored = None;
                }
                if let Some(v) = v {
                    out.push((k.clone(), v.clone()));
                }
            }
        }
        Ok(out)
    }
}

impl StateBackend for Layer {
    async fn get(&mut self, op: OperatorId, keys: &[KeyRef<'_>]) -> Result<Vec<Option<Bytes>>> {
        self.usable()?;
        self.relieve().await?;
        let mut out = Vec::with_capacity(keys.len());
        let mut missing = Vec::new();
        let mut at = Vec::new();
        for (i, k) in keys.iter().enumerate() {
            let key = layout::value(self.group(k.group)?, op, k.key);
            match self.changed(&key) {
                Some(v) => out.push(v.clone()),
                None => {
                    out.push(None);
                    missing.push(key);
                    at.push(i);
                }
            }
        }
        if !missing.is_empty() {
            let got = self.store.get(&missing).await?;
            for (i, v) in at.into_iter().zip(got) {
                out[i] = v;
            }
        }
        Ok(out)
    }

    fn put(&mut self, op: OperatorId, key: KeyRef<'_>, value: Bytes) {
        if let Some(g) = self.place(key.group) {
            self.change(layout::value(g, op, key.key), Some(value));
        }
    }

    fn delete(&mut self, op: OperatorId, key: KeyRef<'_>) {
        if let Some(g) = self.place(key.group) {
            self.change(layout::value(g, op, key.key), None);
        }
    }

    fn append(&mut self, op: OperatorId, key: KeyRef<'_>, time: i64, row: Bytes) {
        let Some(g) = self.place(key.group) else {
            return;
        };
        if self.gone(op, g, time) {
            return;
        }
        match self.next_seq() {
            Some(seq) => self.change(layout::row(g, op, time, key.key, seq), Some(row)),
            None => {
                self.fault.get_or_insert(Error::Invalid(format!(
                    "a row appended in epoch {}: rows take epochs up to {} and {} rows in each",
                    self.epoch,
                    u32::MAX,
                    u32::MAX
                )));
            }
        }
    }

    async fn rows(
        &mut self,
        op: OperatorId,
        slots: &[(KeyRef<'_>, i64)],
    ) -> Result<Vec<Vec<Bytes>>> {
        self.usable()?;
        self.relieve().await?;
        let mut out = Vec::with_capacity(slots.len());
        for (k, time) in slots {
            let g = self.group(k.group)?;
            if self.gone(op, g, *time) {
                out.push(Vec::new());
                continue;
            }
            let prefix = layout::row_slot(g, op, *time, k.key);
            let end = layout::successor(&prefix);
            let got = self.live(&prefix, &end, usize::MAX).await?;
            out.push(got.into_iter().map(|(_, row)| row).collect());
        }
        Ok(out)
    }

    async fn take_rows(
        &mut self,
        op: OperatorId,
        watermark: i64,
        limit: usize,
    ) -> Result<Vec<DueRow>> {
        self.usable()?;
        self.relieve().await?;
        let mut out = Vec::new();
        for g in self.groups() {
            if out.len() >= limit {
                break;
            }
            let h = self.horizon_in(op, g);
            if h > i64::MIN && h >= watermark {
                continue;
            }
            let (start, end) = layout::rows_due_after(g, op, h, watermark);
            for (k, row) in self.live(&start, &end, limit - out.len()).await? {
                let (group, time, key, _) =
                    layout::parse_row(&k).ok_or_else(|| Error::Corrupt("a row's key".into()))?;
                out.push(DueRow {
                    group: group.into(),
                    time,
                    key: Bytes::copy_from_slice(key),
                    row,
                });
                self.change(k, None);
            }
        }
        Ok(out)
    }

    async fn insert_new(
        &mut self,
        op: OperatorId,
        ids: &[KeyRef<'_>],
        at: &[i64],
        expires: &[i64],
    ) -> Result<Vec<bool>> {
        self.usable()?;
        self.relieve().await?;
        if ids.len() != at.len() || ids.len() != expires.len() {
            return Err(Error::Invalid(format!(
                "{} ids, {} times and {} expiries",
                ids.len(),
                at.len(),
                expires.len()
            )));
        }
        let mut keys = Vec::with_capacity(ids.len());
        let mut groups = Vec::with_capacity(ids.len());
        for id in ids {
            let g = self.group(id.group)?;
            keys.push(layout::set(g, op, id.key));
            groups.push((op, g));
        }
        self.load_horizons(&groups).await?;
        let ask: Vec<Bytes> = keys
            .iter()
            .filter(|k| self.changed(k).is_none())
            .cloned()
            .collect();
        let got = self.store.get(&ask).await?;
        let stored: HashMap<Bytes, Option<Bytes>> = ask.into_iter().zip(got).collect();
        let mut out = Vec::with_capacity(keys.len());
        for (i, k) in keys.into_iter().enumerate() {
            let now = at[i].max(self.set_horizon(op, groups[i].1));
            let current = match self.changed(&k) {
                Some(v) => v.clone(),
                None => stored.get(&k).cloned().flatten(),
            };
            let live = match current {
                Some(v) => {
                    let e = layout::parse_time_value(&v)
                        .ok_or_else(|| Error::Corrupt("an expiry that isn't a time".into()))?;
                    e == NEVER || e > now
                }
                None => false,
            };
            if !live && (expires[i] == NEVER || expires[i] > now) {
                self.change(k, Some(layout::time_value(expires[i])));
            }
            out.push(!live);
        }
        Ok(out)
    }

    fn expire(&mut self, op: OperatorId, horizon: i64) {
        let h = self.moved.entry(op).or_insert(i64::MIN);
        *h = (*h).max(horizon);
    }

    fn expire_in(&mut self, op: OperatorId, group: KeyGroup, horizon: i64) {
        if let Some(g) = self.place(group)
            && horizon > self.horizon_in(op, g)
        {
            self.moved_in.insert((op, g), horizon);
        }
    }

    fn horizon(&self, op: OperatorId, group: KeyGroup) -> i64 {
        match u16::try_from(group) {
            Ok(g) if self.range.contains(group) => self.horizon_in(op, g),
            _ => i64::MIN,
        }
    }

    fn set_timer(&mut self, op: OperatorId, key: KeyRef<'_>, time: i64) {
        if let Some(g) = self.place(key.group)
            && !self.gone(op, g, time)
        {
            self.change(layout::timer(op, time, g, key.key), Some(Bytes::new()));
        }
    }

    fn delete_timer(&mut self, op: OperatorId, key: KeyRef<'_>, time: i64) {
        if let Some(g) = self.place(key.group)
            && !self.gone(op, g, time)
        {
            self.change(layout::timer(op, time, g, key.key), None);
        }
    }

    async fn fire(&mut self, op: OperatorId, watermark: i64, limit: usize) -> Result<Vec<Timer>> {
        self.usable()?;
        self.relieve().await?;
        let least = self.least_horizon(op);
        let mut out = Vec::new();
        if least > i64::MIN && least >= watermark {
            return Ok(out);
        }
        let (mut from, end) = layout::timers_due_after(op, least, watermark);
        // Timers a group's later horizon passed are skipped, and more read.
        while out.len() < limit {
            let want = limit - out.len();
            let page = self.live(&from, &end, want).await?;
            let more = page.len() == want;
            for (k, _) in page {
                let (time, group, key) = layout::parse_timer(&k)
                    .ok_or_else(|| Error::Corrupt("a timer's key".into()))?;
                from = layout::after(&k);
                if self.gone(op, group, time) {
                    continue;
                }
                out.push(Timer {
                    time,
                    group: group.into(),
                    key: Bytes::copy_from_slice(key),
                });
                self.change(k, None);
            }
            if !more {
                break;
            }
        }
        Ok(out)
    }
}

/// What a map of changes holds in memory, about, as [`Layer::change`]
/// counts it.
fn memory_of(m: &BTreeMap<Bytes, Option<Bytes>>) -> usize {
    m.iter()
        .map(|(k, v)| k.len() + v.as_ref().map_or(0, |v| v.len()) + ENTRY_OVERHEAD)
        .sum()
}

/// Two maps' entries in a range, in key order: of a key in both, `newer`'s.
struct Overlay<'a> {
    newer: Peekable<btree_map::Range<'a, Bytes, Option<Bytes>>>,
    older: Peekable<btree_map::Range<'a, Bytes, Option<Bytes>>>,
}

impl<'a> Iterator for Overlay<'a> {
    type Item = (&'a Bytes, &'a Option<Bytes>);

    fn next(&mut self) -> Option<Self::Item> {
        let order = match (self.newer.peek(), self.older.peek()) {
            (None, None) => return None,
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some(a), Some(b)) => a.0.cmp(b.0),
        };
        match order {
            Ordering::Less => self.newer.next(),
            Ordering::Greater => self.older.next(),
            Ordering::Equal => {
                self.older.next();
                self.newer.next()
            }
        }
    }
}
