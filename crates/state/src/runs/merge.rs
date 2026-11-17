//! Merging a lane's sections, by kind: which to merge, and the merge.
//!
//! - Rows and timers are never merged: they die whole as the horizon
//!   passes them.
//! - Values merge lazily, by size: only while a lane names more values
//!   sections than [`MergeConfig::values_bound`], since most windows'
//!   accumulators die before a merge would reach them. A merge that takes
//!   the oldest section drops deletes.
//! - A set whose entries expire merges only within a time bucket: its live
//!   expiries are cut into [`MergeConfig::buckets`] buckets, and small
//!   sections of the newest bucket merge by size. Older buckets are left to
//!   die whole. A merge drops entries the horizon has passed.
//! - A set kept for good merges by size.
//!
//! A merge takes sections next to each other in a lane's order, newest
//! first, and never some of an epoch's and not the rest: its output holds
//! the newest of each key, as the newest of its inputs' epoch, so it reads
//! where they did. By size is by tiers: sections within a factor of
//! [`MergeConfig::fanout`] of each other share a tier, and a merge takes
//! sections of one tier.

use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;

use super::format::{BlockMeta, Built, RunWriter};
use super::{Content, Run, Shared, key_hash, open_run, read_blocks};
use crate::layout::{self, SectionKind};
use crate::store::Horizons;
use crate::{Error, KeyGroupRange, NEVER, OperatorId, Result};

/// How a lane merges its sections. Every lane of a
/// [`super::RunStores`] shares it.
#[derive(Clone, Debug)]
pub struct MergeConfig {
    /// Values sections a lane names before it merges some.
    pub values_bound: usize,
    /// A tier's width, as a factor of bytes, and the sections of one tier
    /// that a set's merge waits for.
    pub fanout: u64,
    /// A set's section with expiries, of at most these stored bytes, is
    /// small, and merges while it's in the newest bucket.
    pub small_bytes: u64,
    /// The buckets a lane's live expiries are cut into.
    pub buckets: u64,
    /// The most bytes the merges in flight of a [`super::RunStores`]' lanes
    /// take at once: their inputs, uncompressed. It bounds what they hold
    /// in memory, each its output until the barrier that writes it.
    pub max_bytes: u64,
    /// A barrier waits for a merge in flight, and writes it: merges land at
    /// the barrier after the one that started them, for tests and
    /// measurements. Otherwise a barrier takes a merge that has ended, and
    /// leaves one that hasn't to a later barrier.
    pub wait: bool,
}

impl Default for MergeConfig {
    fn default() -> Self {
        Self {
            values_bound: 8,
            fanout: 4,
            small_bytes: 256 << 10,
            buckets: 8,
            max_bytes: 64 << 20,
            wait: false,
        }
    }
}

/// A section as the policy weighs it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub epoch: u64,
    /// Its blocks' bytes in its run.
    pub stored: u64,
    /// Its entries' bytes, uncompressed.
    pub raw: u64,
    /// Its least and greatest time: a set's expiries.
    pub times: Option<(i64, i64)>,
}

/// A merge to make: of a kind's sections `at`, in the lane's order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Plan {
    pub kind: SectionKind,
    pub at: Range<usize>,
    /// It takes the lane's oldest, so a delete shadows nothing older.
    pub oldest: bool,
}

/// The merge a lane makes next, if any, of each kind's sections, newest
/// first.
pub fn plan(sections: &[Vec<Candidate>; 4], config: &MergeConfig) -> Option<Plan> {
    let values = &sections[SectionKind::Values.index()];
    if values.len() > config.values_bound
        && let Some(at) = pick(values, |_| true, 2, config)
    {
        return Some(planned(SectionKind::Values, at, values.len()));
    }
    let set = &sections[SectionKind::Set.index()];
    let expiring = |c: &Candidate| c.times.is_some_and(|(_, hi)| hi != NEVER);
    let newest = set.iter().filter(|c| expiring(c)).map(|c| c.times);
    let (lo, hi) = newest.fold((i64::MAX, i64::MIN), |(lo, hi), t| match t {
        Some((a, b)) => (lo.min(a), hi.max(b)),
        None => (lo, hi),
    });
    // Buckets are aligned in time, a power of two wide, so a bucket stays
    // one as the span of live expiries moves; the newest is the one of the
    // latest expiry.
    let span = u64::try_from(hi.saturating_sub(lo)).unwrap_or(0);
    let width = i64::try_from((span / config.buckets.max(1)).max(1).next_power_of_two())
        .unwrap_or(i64::MAX);
    let fresh = |c: &Candidate| {
        expiring(c)
            && c.stored <= config.small_bytes
            && c.times
                .is_some_and(|(_, b)| b.div_euclid(width) == hi.div_euclid(width))
    };
    let fanout = usize::try_from(config.fanout).unwrap_or(usize::MAX).max(2);
    let small = pick(set, fresh, fanout, config);
    let kept = pick(set, |c| !expiring(c), fanout, config);
    let best = [small, kept].into_iter().flatten().min_by_key(|r| {
        (
            tier(set[r.start].stored, config),
            std::cmp::Reverse(r.len()),
            r.start,
        )
    })?;
    Some(planned(SectionKind::Set, best, set.len()))
}

fn planned(kind: SectionKind, at: Range<usize>, of: usize) -> Plan {
    Plan {
        kind,
        oldest: at.end == of,
        at,
    }
}

/// The tier of a section of `stored` bytes.
fn tier(stored: u64, config: &MergeConfig) -> u32 {
    let f = config.fanout.max(2);
    let mut x = stored.max(1);
    let mut t = 0;
    while x >= f {
        x /= f;
        t += 1;
    }
    t
}

/// Whether a merge may start or end before `i`: it splits no epoch.
fn cut(c: &[Candidate], i: usize) -> bool {
    i == 0 || i == c.len() || c[i - 1].epoch != c[i].epoch
}

/// The sections to merge of those `may` allows, at least `least` of one
/// tier next to each other, within the bytes a merge takes: of the lowest
/// tier, then the most, then the newest.
fn pick(
    c: &[Candidate],
    may: impl Fn(&Candidate) -> bool,
    least: usize,
    config: &MergeConfig,
) -> Option<Range<usize>> {
    let mut best: Option<(u32, Range<usize>)> = None;
    let mut i = 0;
    while i < c.len() {
        if !may(&c[i]) {
            i += 1;
            continue;
        }
        let t = tier(c[i].stored, config);
        let mut j = i + 1;
        while j < c.len() && may(&c[j]) && tier(c[j].stored, config) == t {
            j += 1;
        }
        if let Some(r) = fit(c, i..j, config)
            && r.len() >= least
        {
            let better = match &best {
                None => true,
                Some((bt, br)) => {
                    (t, std::cmp::Reverse(r.len())) < (*bt, std::cmp::Reverse(br.len()))
                }
            };
            if better {
                best = Some((t, r));
            }
        }
        i = j;
    }
    best.map(|(_, r)| r)
}

/// The longest part of `r` that splits no epoch and is within the bytes a
/// merge takes, the newest of those as long. Its ends are cuts, so it
/// moves two pointers over them: linear in `r`'s length.
fn fit(c: &[Candidate], r: Range<usize>, config: &MergeConfig) -> Option<Range<usize>> {
    let cuts: Vec<usize> = (r.start..=r.end).filter(|&i| cut(c, i)).collect();
    // Bytes before each position of `r`, from its start.
    let mut before = Vec::with_capacity(r.len() + 1);
    before.push(0u64);
    for x in &c[r.clone()] {
        let last = before.last().copied().unwrap_or(0);
        before.push(last.saturating_add(x.raw));
    }
    let at = |i: usize| before[i - r.start];
    let mut best: Option<Range<usize>> = None;
    let mut end = 0;
    for start in 0..cuts.len() {
        end = end.max(start);
        while end + 1 < cuts.len() && at(cuts[end + 1]) - at(cuts[start]) <= config.max_bytes {
            end += 1;
        }
        let (a, b) = (cuts[start], cuts[end]);
        if b > a && best.as_ref().is_none_or(|x| b - a > x.len()) {
            best = Some(a..b);
        }
    }
    best
}

/// A section a merge reads: its run, and the key groups it counts for.
pub struct Input {
    pub run: Arc<Run>,
    pub groups: KeyGroupRange,
}

/// A merge's output: a run of one section, or nothing where every entry it
/// read was gone; and for values, what its section holds.
pub struct Merged {
    pub built: Option<Built>,
    pub content: Option<Content>,
}

/// A section's blocks in order, read a span at a time.
struct Cursor {
    run: Arc<Run>,
    groups: KeyGroupRange,
    blocks: Vec<BlockMeta>,
    next: usize,
    /// Read and not yet merged, of its groups.
    entries: VecDeque<(Bytes, Option<Bytes>)>,
    held: usize,
}

impl Cursor {
    fn head(&self) -> Option<&Bytes> {
        self.entries.front().map(|e| &e.0)
    }

    /// Reads its next span of blocks if it has none left read: up to
    /// `span` bytes, or one block.
    async fn fill(&mut self, shared: &Shared, span: u64) -> Result<()> {
        if !self.entries.is_empty() || self.next >= self.blocks.len() {
            return Ok(());
        }
        let first = self.next;
        let start = self.blocks[first].offset;
        let mut end = self.blocks[first].end();
        let mut n = first + 1;
        while n < self.blocks.len() && self.blocks[n].end() - start <= span {
            end = end.max(self.blocks[n].end());
            n += 1;
        }
        self.next = n;
        let want: Vec<&BlockMeta> = self.blocks[first..n].iter().collect();
        let read = read_blocks(
            shared,
            &self.run,
            start..end,
            want,
            &shared.counters.merge_read,
        )
        .await?;
        self.held = 0;
        for (_, block) in read {
            let mut at = 0;
            while at < block.len() {
                let (k, v) = super::format::entry(&block, &mut at)?;
                if layout::group_of(&k).is_some_and(|g| self.groups.contains(g.into())) {
                    self.held += k.len() + v.as_ref().map_or(0, Bytes::len);
                    self.entries.push_back((k, v));
                }
            }
        }
        Ok(())
    }
}

/// Merges `inputs`, sections of `kind` newest first, into one section:
/// each key's newest entry, less deletes where `oldest`, and a set's
/// entries `horizons` passed. `memory` follows what it holds.
pub async fn merge(
    shared: Arc<Shared>,
    kind: SectionKind,
    inputs: Vec<Input>,
    oldest: bool,
    horizons: Horizons,
    memory: Arc<AtomicUsize>,
) -> Result<Merged> {
    for i in &inputs {
        if i.run.open.get().is_none() {
            open_run(&shared, &i.run, &shared.counters.merge_read).await?;
        }
    }
    let config = &shared.config;
    let mut cursors = Vec::with_capacity(inputs.len());
    for i in inputs {
        let open = i.run.section(kind)?;
        let blocks = open
            .blocks
            .iter()
            .filter(
                |b| match (layout::group_of(&b.first), layout::group_of(&b.last)) {
                    (Some(a), Some(z)) => {
                        i.groups.overlaps(&KeyGroupRange::new(a.into(), z.into()))
                    }
                    _ => true,
                },
            )
            .cloned()
            .collect();
        cursors.push(Cursor {
            run: i.run.clone(),
            groups: i.groups,
            blocks,
            next: 0,
            entries: VecDeque::new(),
            held: 0,
        });
    }
    // What a cursor reads at once: a share of what the merge may hold.
    let budget = config.merge.as_ref().map_or(1 << 20, |m| m.max_bytes);
    let span = (budget / (4 * cursors.len().max(1) as u64))
        .max(64 << 10)
        .min(config.max_read_bytes.max(1));
    let mut w = RunWriter::new(config, false, 0);
    w.section(kind)?;
    let mut content = Vec::new();
    let mut written = 0usize;
    loop {
        for c in &mut cursors {
            c.fill(&shared, span).await?;
        }
        let Some(least) = cursors.iter().filter_map(Cursor::head).min().cloned() else {
            break;
        };
        // The newest input that holds the key answers for it.
        let mut newest: Option<Option<Bytes>> = None;
        for c in &mut cursors {
            if c.head() == Some(&least)
                && let Some((_, v)) = c.entries.pop_front()
            {
                newest.get_or_insert(v);
            }
        }
        let Some(v) = newest else {
            return Err(Error::Store("a merge lost its least key".into()));
        };
        if gone(kind, &least, v.as_deref(), oldest, &horizons) {
            continue;
        }
        if kind == SectionKind::Values {
            content.push((key_hash(&least), v.is_none()));
        }
        w.add(&least, v.as_deref())?;
        written += 1;
        if written.is_multiple_of(1024) {
            let held: usize = cursors.iter().map(|c| c.held).sum();
            memory.store(w.bytes() + held + content.len() * 17, Ordering::Relaxed);
        }
    }
    let built = w.finish()?;
    memory.store(built.bytes.len() + content.len() * 17, Ordering::Relaxed);
    Ok(Merged {
        built: (!built.sections.is_empty()).then_some(built),
        content: (kind == SectionKind::Values).then(|| Content::new(content)),
    })
}

/// Whether a merge leaves out an entry, its key's newest: a delete with
/// nothing older to shadow, or a set's entry the horizon passed.
fn gone(kind: SectionKind, k: &[u8], v: Option<&[u8]>, oldest: bool, horizons: &Horizons) -> bool {
    let Some(v) = v else {
        return oldest;
    };
    if kind != SectionKind::Set {
        return false;
    }
    let (Some(op), Some(g), Some(expiry)) = (
        layout::operator_of(k),
        layout::group_of(k),
        layout::parse_time_value(v),
    ) else {
        return false;
    };
    let h = horizons
        .get(&(OperatorId(op.0), g.into()))
        .copied()
        .unwrap_or(i64::MIN);
    expiry != NEVER && h > i64::MIN && expiry <= h
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn c(epoch: u64, stored: u64, times: Option<(i64, i64)>) -> Candidate {
        Candidate {
            epoch,
            stored,
            raw: stored,
            times,
        }
    }

    fn of(kind: SectionKind, secs: Vec<Candidate>) -> [Vec<Candidate>; 4] {
        let mut out: [Vec<Candidate>; 4] = Default::default();
        out[kind.index()] = secs;
        out
    }

    #[test]
    fn values_merge_only_past_the_bound_and_by_tier() {
        let config = MergeConfig::default();
        let few: Vec<_> = (0..8).rev().map(|e| c(e + 1, 1000, None)).collect();
        assert_eq!(plan(&of(SectionKind::Values, few), &config), None);
        // Newest first: four small, then large ones of one tier each.
        let mut many: Vec<_> = (0..4).map(|i| c(20 - i, 100, None)).collect();
        many.extend((0..5).map(|i| c(10 - i, 100 << (2 * i + 4), None)));
        let p = plan(&of(SectionKind::Values, many), &config).unwrap();
        assert_eq!(
            p,
            Plan {
                kind: SectionKind::Values,
                at: 0..4,
                oldest: false
            }
        );
        // Nine of one tier: all of them, deletes and all.
        let same: Vec<_> = (0..9).map(|i| c(20 - i, 1000, None)).collect();
        let p = plan(&of(SectionKind::Values, same), &config).unwrap();
        assert_eq!(
            p,
            Plan {
                kind: SectionKind::Values,
                at: 0..9,
                oldest: true
            }
        );
    }

    #[test]
    fn a_merge_splits_no_epoch_and_stays_within_its_bytes() {
        let config = MergeConfig {
            values_bound: 2,
            max_bytes: 2500,
            ..MergeConfig::default()
        };
        // Two sections of epoch 7, from lanes that were restored onto one.
        let secs = vec![
            c(9, 1000, None),
            c(7, 1000, None),
            c(7, 1000, None),
            c(5, 1000, None),
        ];
        let p = plan(&of(SectionKind::Values, secs), &config).unwrap();
        assert_eq!(
            p,
            Plan {
                kind: SectionKind::Values,
                at: 1..3,
                oldest: false
            }
        );
        let split = vec![c(9, 1000, None), c(7, 1000, None), c(7, 2000, None)];
        assert_eq!(plan(&of(SectionKind::Values, split), &config), None);
    }

    /// What `fit` gives, by trying every start and scanning forward.
    fn fit_by_scanning(
        c: &[Candidate],
        r: Range<usize>,
        config: &MergeConfig,
    ) -> Option<Range<usize>> {
        let mut best: Option<Range<usize>> = None;
        for start in (r.start..r.end).filter(|&i| cut(c, i)) {
            let mut bytes = 0u64;
            for i in start..r.end {
                bytes = bytes.saturating_add(c[i].raw);
                if bytes > config.max_bytes {
                    break;
                }
                if cut(c, i + 1) && best.as_ref().is_none_or(|b| i + 1 - start > b.len()) {
                    best = Some(start..i + 1);
                }
            }
        }
        best
    }

    #[test]
    fn fit_is_the_longest_newest_part_within_the_bytes() {
        let mut x = 7u64;
        let mut next = |n: u64| {
            x = x
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (x >> 33) % n
        };
        let mut compared = 0;
        for _ in 0..3000 {
            let n = 1 + next(30) as usize;
            let mut epoch = 1000;
            let c: Vec<Candidate> = (0..n)
                .map(|_| {
                    epoch -= next(2);
                    Candidate {
                        epoch,
                        stored: 1,
                        raw: next(50),
                        times: None,
                    }
                })
                .collect();
            let config = MergeConfig {
                max_bytes: next(200),
                ..MergeConfig::default()
            };
            let a = next(n as u64 + 1) as usize;
            let b = a + next((n - a) as u64 + 1) as usize;
            assert_eq!(
                fit(&c, a..b, &config),
                fit_by_scanning(&c, a..b, &config),
                "{c:?} {a}..{b}"
            );
            compared += 1;
        }
        assert_eq!(compared, 3000);
    }

    #[test]
    fn a_set_merges_small_sections_of_its_newest_bucket() {
        let config = MergeConfig::default();
        // Newest first: epoch e's ids expire from e to e + 100. Live
        // expiries span 1 to 140, so buckets are 32 wide, and the newest
        // holds 128 to 159: epochs 28 to 40.
        let secs: Vec<_> = (1..=40)
            .rev()
            .map(|e| c(e as u64, 1000, Some((e, e + 100))))
            .collect();
        let p = plan(&of(SectionKind::Set, secs.clone()), &config).unwrap();
        assert_eq!(
            p,
            Plan {
                kind: SectionKind::Set,
                at: 0..13,
                oldest: false
            }
        );
        // A large one among them ends the run.
        let mut big = secs.clone();
        big[5].stored = 1 << 20;
        let p = plan(&of(SectionKind::Set, big), &config).unwrap();
        assert_eq!(p.at, 6..13);
        // Fewer than the fanout in the newest bucket: none, though older
        // buckets hold more.
        let few: Vec<_> = secs[10..].to_vec();
        assert_eq!(plan(&of(SectionKind::Set, few), &config), None);
    }

    #[test]
    fn a_set_kept_for_good_merges_by_tier() {
        let config = MergeConfig::default();
        let never = |e: u64, stored: u64| c(e, stored, Some((5, NEVER)));
        let mut secs = vec![never(9, 100), never(8, 100), never(7, 100)];
        assert_eq!(plan(&of(SectionKind::Set, secs.clone()), &config), None);
        secs.push(never(6, 100));
        secs.push(never(5, 100 << 10));
        let p = plan(&of(SectionKind::Set, secs), &config).unwrap();
        assert_eq!(
            p,
            Plan {
                kind: SectionKind::Set,
                at: 0..4,
                oldest: false
            }
        );
    }
}
