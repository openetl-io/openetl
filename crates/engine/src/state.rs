//! Where a job's stateful operators keep their state, and how the record
//! names it. Each operator's lanes write runs below a directory of the
//! job's state URL of the operator's own. A lane's changes since its last
//! run ride in the record, in its snapshot's pending set, while they're
//! within its share of what a record may carry ([`PENDING_BYTES`]); at the
//! barrier where they'd pass it, the lane writes them as a run. A lane's
//! snapshot names the runs that hold what it reads, less what's dead:
//! windows deleted and timers fired, which the operator's horizon, its
//! watermark, has passed. A lane merges some of its sections in the
//! background, by kind, and a barrier writes the merge's run and names it
//! in their place. A run counts once the epoch's record does: a run no
//! committed record names is never read, and retention deletes it. A
//! restore opens what the record names, by each lane's own range of key
//! groups, its part of the pending sets too, so state moves to any number
//! of lanes, and reads no run until a lane needs it.
//!
//! With a scratch directory, the job's operators keep what they read of
//! their runs, and the runs they write, in a disk cache there, within half
//! of the job's scratch. Under memory pressure a lane flushes its pending
//! set and its epoch's changes early, as a run its next record names; a
//! job whose state still doesn't fit fails with RESOURCE_EXHAUSTED.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use openetl_proto::connector::v1::ErrorCode;
use openetl_proto::engine::v1::{KeyGroupRange as ProtoRange, StateSnapshot};
use openetl_state::runs::{MergeConfig, ObjectStorage, RunConfig, RunStores, runs};
use openetl_state::{DiskCache, Error as StateError, KeyGroupRange, PendingLimit, Snapshot};

use crate::error::JobError;
use crate::store::Location;

/// What the block caches of a job's operators may hold together: a
/// sixteenth of the job's memory, and between these for each operator.
/// Each operator's lanes merge within as much again: the merges in flight
/// of all its lanes take no more, their inputs uncompressed, and hold
/// about as much, each its output until the barrier that writes it.
const CACHE_SHARE: u64 = 16;
const MIN_CACHE: usize = 1 << 20;
const MAX_CACHE: usize = 64 << 20;

/// What a record carries at most of the pending sets of a job's stateful
/// operators, in bytes of their encoding: this, or a [`PENDING_SHARE`]th of
/// the job's memory where that's less, shared evenly by the operators, and
/// by each one's lanes on every engine as their key groups are of the
/// pipeline's.
pub const PENDING_BYTES: usize = 1 << 20;
pub const PENDING_SHARE: u64 = 64;

/// The share of the job's scratch its state's disk cache takes.
const DISK_SHARE: u64 = 2;
/// The share of the job's memory its lanes' open epochs of changes may
/// hold together: each lane flushes its own early past its part of it.
const CHANGES_SHARE: u64 = 8;
/// The least a lane's part is.
const MIN_CHANGES: usize = 64 << 10;
/// The share of the job's memory its state may hold in all, block caches
/// and all, while the engine is near its memory limit, before the job
/// fails.
const STATE_SHARE: u64 = 2;
/// A lane flushes early near the memory limit only once its changes hold
/// this much, so it doesn't write a run per batch.
pub const MIN_EARLY: usize = 256 << 10;

/// The engine's scratch: a directory of its own on local disk, and what
/// the job may keep there.
#[derive(Clone, Debug)]
pub struct Scratch {
    pub dir: PathBuf,
    pub bytes: u64,
}
/// A job's state: where its operators' runs are, and whether it writes
/// them.
pub struct JobState {
    location: Location,
    read_only: bool,
    config: RunConfig,
    /// Each operator's.
    pending: PendingLimit,
    /// Each operator's, which its lanes share with their block cache.
    stores: Mutex<HashMap<String, RunStores>>,
    operators: usize,
    /// The job's memory; 0 for no limit.
    memory: u64,
    disk: Option<Arc<DiskCache>>,
    /// What each operator's lanes hold in memory, by operator and lane.
    held: Mutex<HashMap<(String, usize), usize>>,
    /// A test's: the engine is near its memory limit whatever it holds.
    #[cfg(test)]
    pub pressed: std::sync::atomic::AtomicBool,
    /// The most [`JobState::hold`] counted.
    #[cfg(test)]
    pub peak: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobState")
            .field("base", &self.location.base)
            .field("read_only", &self.read_only)
            .finish()
    }
}

impl JobState {
    /// Opens `url` as [`Location::open`] does, for `operators` stateful
    /// operators in a job of `memory` bytes, 0 for no limit, of a pipeline
    /// of `key_groups`, whose records carry at most `pending` bytes of
    /// their pending sets, or `None` for [`PENDING_BYTES`] or the job's
    /// share, with its disk cache in `scratch`, if any. A scratch directory
    /// that can't be made leaves the cache off.
    pub fn open(
        url: &str,
        read_only: bool,
        operators: usize,
        memory: u64,
        key_groups: u32,
        pending: Option<u64>,
        scratch: Option<&Scratch>,
    ) -> Result<Self, JobError> {
        let location = Location::open(url, "state URL")?;
        let disk =
            scratch.filter(|s| s.bytes / DISK_SHARE > 0).and_then(|s| {
                match DiskCache::open(&s.dir.join("state"), s.bytes / DISK_SHARE) {
                    Ok(d) => Some(d),
                    Err(e) => {
                        tracing::warn!(
                            dir = %s.dir.display(),
                            error = %e,
                            "the state's disk cache is off: its directory can't be made"
                        );
                        None
                    }
                }
            });
        let cache = match memory {
            0 => MAX_CACHE,
            m => usize::try_from(m / CACHE_SHARE / operators.max(1) as u64)
                .unwrap_or(MAX_CACHE)
                .clamp(MIN_CACHE, MAX_CACHE),
        };
        let pending = match (pending, memory) {
            (Some(p), _) => usize::try_from(p).unwrap_or(usize::MAX),
            (None, 0) => PENDING_BYTES,
            (None, m) => usize::try_from(m / PENDING_SHARE)
                .unwrap_or(PENDING_BYTES)
                .min(PENDING_BYTES),
        };
        Ok(Self {
            location,
            read_only,
            pending: PendingLimit {
                bytes: pending / operators.max(1),
                key_groups,
            },
            config: RunConfig {
                cache_bytes: cache,
                merge: Some(MergeConfig {
                    max_bytes: cache as u64,
                    ..MergeConfig::default()
                }),
                ..RunConfig::default()
            },
            stores: Mutex::new(HashMap::new()),
            operators: operators.max(1),
            memory,
            disk,
            held: Mutex::new(HashMap::new()),
            #[cfg(test)]
            pressed: Default::default(),
            #[cfg(test)]
            peak: Default::default(),
        })
    }

    /// Whether the engine is near its memory limit.
    pub fn pressure(&self) -> bool {
        #[cfg(test)]
        if self.pressed.load(std::sync::atomic::Ordering::Relaxed) {
            return true;
        }
        crate::memory::pressure()
    }

    /// The bytes of a lane's open epoch of changes past which it flushes
    /// them early, of `lanes` keyed lanes in the engine: its part of the
    /// job's memory. 0 for never: with no limit, or in a job that writes
    /// nothing.
    pub fn flush_at(&self, lanes: usize) -> usize {
        if self.read_only || self.memory == 0 {
            return 0;
        }
        let part = self.memory / CHANGES_SHARE / (self.operators * lanes.max(1)) as u64;
        usize::try_from(part).unwrap_or(usize::MAX).max(MIN_CHANGES)
    }

    /// Notes that `operator`'s lane `lane` holds `bytes` in memory, and
    /// gives what the job's state holds in all, its block caches with it.
    pub fn hold(&self, operator: &str, lane: usize, bytes: usize) -> u64 {
        self.count(Some((operator, lane, bytes)), None)
    }

    /// What `operator`'s lane `lane` holds counts for the job's state while
    /// the handle lives: dropped, as the lane ends however it ends, the lane
    /// no longer counts.
    pub fn lane(self: &Arc<Self>, operator: &str, lane: usize) -> LaneState {
        LaneState {
            job: self.clone(),
            operator: operator.to_owned(),
            lane,
        }
    }

    /// Sets a lane's count, or forgets one, and gives the job's.
    fn count(&self, set: Option<(&str, usize, usize)>, forget: Option<(&str, usize)>) -> u64 {
        let lanes: usize = {
            let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some((operator, lane, bytes)) = set {
                held.insert((operator.to_owned(), lane), bytes);
            }
            if let Some((operator, lane)) = forget {
                held.remove(&(operator.to_owned(), lane));
            }
            held.values().sum()
        };
        let caches: usize = self
            .stores
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(RunStores::cache_bytes)
            .sum();
        let all = (lanes + caches) as u64;
        crate::memory::set_state(all);
        #[cfg(test)]
        self.peak
            .fetch_max(all, std::sync::atomic::Ordering::Relaxed);
        all
    }

    /// What the job's state may hold in memory, in all, near the engine's
    /// memory limit; 0 for no limit.
    pub fn budget(&self) -> u64 {
        self.memory / STATE_SHARE
    }

    /// The job's memory; 0 for no limit.
    pub fn memory(&self) -> u64 {
        self.memory
    }

    #[cfg(test)]
    pub fn disk(&self) -> Option<&Arc<DiskCache>> {
        self.disk.as_ref()
    }

    /// The job's stateful operators read their restored state and write
    /// nothing.
    pub fn read_only(&self) -> bool {
        self.read_only
    }

    /// Where `operator`'s lanes keep their runs.
    pub fn stores(&self, operator: &str) -> RunStores {
        let mut stores = self.stores.lock().unwrap_or_else(PoisonError::into_inner);
        stores
            .entry(operator.to_owned())
            .or_insert_with(|| {
                let prefix = self.location.prefix.clone().join(dir(operator));
                let objects = Arc::new(ObjectStorage::new(self.location.store.clone(), prefix));
                match &self.disk {
                    Some(d) => RunStores::with_disk(
                        objects,
                        self.config.clone(),
                        d.clone(),
                        &dir(operator),
                    ),
                    None => RunStores::new(objects, self.config.clone()),
                }
                .with_pending(self.pending)
            })
            .clone()
    }

    /// What a record carries at most of each operator's pending sets.
    #[cfg(test)]
    pub fn pending(&self) -> PendingLimit {
        self.pending
    }
}

/// A keyed lane's part of what the job's state holds: [`JobState::lane`].
pub struct LaneState {
    job: Arc<JobState>,
    operator: String,
    lane: usize,
}

impl LaneState {
    /// Notes that the lane holds `bytes`, and gives what the job's state
    /// holds in all.
    pub fn hold(&self, bytes: usize) -> u64 {
        self.job.hold(&self.operator, self.lane, bytes)
    }
}

impl Drop for LaneState {
    fn drop(&mut self) {
        self.job.count(None, Some((&self.operator, self.lane)));
    }
}

/// The directory below the state URL that holds an operator's runs: its ID,
/// where that's only letters, digits, `-` and `_`, and otherwise `x.` and
/// the ID's bytes in hex, which no such ID can be.
pub fn dir(operator: &str) -> String {
    let plain = !operator.is_empty()
        && operator
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    match plain {
        true => operator.to_owned(),
        false => {
            let hex: String = operator.bytes().map(|b| format!("{b:02x}")).collect();
            format!("x.{hex}")
        }
    }
}

/// The record's name for a lane's snapshot of `operator`, with the runs it
/// names below the state URL.
pub fn named(operator: &str, s: &Snapshot) -> Result<StateSnapshot, JobError> {
    let dir = dir(operator);
    let names = runs(s).map_err(|e| state_error(&format!("operator {operator}'s state"), &e))?;
    Ok(StateSnapshot {
        operator: operator.to_owned(),
        key_groups: Some(ProtoRange {
            first: s.range.first,
            last: s.range.last,
        }),
        layout: s.layout,
        data: s.data.to_vec(),
        pending: s.pending.to_vec(),
        runs: names
            .into_iter()
            .map(|r| format!("{dir}/{}", r.name))
            .collect(),
    })
}

/// The first key group of `range` that none of an operator's `snapshots`
/// holds, where it has any: a record names an operator's state on every
/// lane, which hold every key group between them, or on none.
pub fn uncovered(snapshots: &[&Snapshot], range: KeyGroupRange) -> Option<u32> {
    if snapshots.is_empty() {
        return None;
    }
    (range.first..=range.last).find(|g| !snapshots.iter().any(|s| s.range.contains(*g)))
}

/// A snapshot the record names, and its operator.
pub fn snapshot(s: &StateSnapshot) -> Result<(String, Snapshot), JobError> {
    let Some(r) = &s.key_groups else {
        return Err(JobError::new(
            ErrorCode::CheckpointCorrupt,
            format!(
                "the checkpoint names state of operator {:?} without its key groups",
                s.operator
            ),
        ));
    };
    Ok((
        s.operator.clone(),
        Snapshot {
            range: KeyGroupRange::new(r.first, r.last),
            layout: s.layout,
            data: Bytes::copy_from_slice(&s.data),
            pending: Bytes::copy_from_slice(&s.pending),
        },
    ))
}

/// A state error as the job's, `what` saying whose: storage that failed,
/// which may not again; state the checkpoint names that can't be read as
/// it says; or the engine's own mistake.
pub fn state_error(what: &str, e: &StateError) -> JobError {
    let code = match e {
        StateError::Store(_) => ErrorCode::Transient,
        StateError::Snapshot(_) | StateError::Corrupt(_) | StateError::Gone(_) => {
            ErrorCode::CheckpointCorrupt
        }
        StateError::Invalid(_) => ErrorCode::Internal,
    };
    JobError::new(code, format!("{what}: {e}"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use openetl_state::runs::{RunRef, encode};

    #[test]
    fn a_snapshot_reads_back_as_named_with_its_runs() {
        let s = Snapshot {
            range: KeyGroupRange::new(64, 127),
            layout: openetl_state::LAYOUT,
            data: encode(&[RunRef {
                name: "g2/e9/kg64-127.run".into(),
                format: openetl_state::runs::FORMAT,
                size: 10,
                sections: Vec::new(),
            }]),
            pending: Bytes::from_static(b"carried as it is"),
        };
        let named = named("window-1", &s).unwrap();
        assert_eq!(named.runs, ["window-1/g2/e9/kg64-127.run"]);
        assert_eq!(snapshot(&named).unwrap(), ("window-1".into(), s));
        let bad = StateSnapshot {
            key_groups: None,
            ..named
        };
        assert_eq!(
            snapshot(&bad).unwrap_err().code,
            ErrorCode::CheckpointCorrupt
        );
    }

    /// A job's state merges its lanes' sections in the background, and a
    /// barrier names the merge's run, which a restore reads.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_jobs_lanes_merge_and_restore_what_they_merged() {
        use openetl_state::{KeyRef, Layer, OperatorId, StateBackend};
        let dir = tempfile::tempdir().unwrap();
        let url = format!("file://{}", dir.path().display());
        let op = OperatorId(1);
        let range = KeyGroupRange::new(0, 15);
        // Records carry none of the lanes' changes, so they write runs, and
        // merge them.
        let state = JobState::open(&url, false, 1, 0, 16, Some(0), None).unwrap();
        let stores = state.stores("window-1");
        let mut lane = Layer::open(&stores, 1, 1, range, &[]).await.unwrap();
        let key = |i: u32| format!("key-{i}").into_bytes();
        let mut snapshots = Vec::new();
        let mut epoch = 1;
        // Values that never die: past 8 sections, the lane merges some.
        while stores.counts().merge_write.count == 0 {
            assert!(epoch < 200, "no merge written by epoch {epoch}");
            for i in 0..50 {
                let k = key(epoch as u32 * 50 + i);
                lane.put(op, KeyRef::new(i % 16, &k), Bytes::from(k.clone()));
            }
            snapshots = lane.barrier(epoch).await.unwrap();
            epoch += 1;
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let names: Vec<String> = snapshots
            .iter()
            .flat_map(|s| named("window-1", s).unwrap().runs)
            .collect();
        assert!(names.iter().any(|n| n.ends_with(".merge.run")), "{names:?}");
        let again = JobState::open(&url, false, 1, 0, 16, None, None).unwrap();
        let back = again.stores("window-1");
        let mut lane = Layer::open(&back, 2, epoch, range, &snapshots)
            .await
            .unwrap();
        let mut read = 0;
        for e in 1..epoch {
            let keys: Vec<Vec<u8>> = (0..50).map(|i| key(e as u32 * 50 + i)).collect();
            let refs: Vec<KeyRef> = (0..50)
                .map(|i| KeyRef::new(i % 16, &keys[i as usize]))
                .collect();
            let got = lane.get(op, &refs).await.unwrap();
            for (k, v) in keys.iter().zip(got) {
                assert_eq!(v.as_deref(), Some(&k[..]));
                read += 1;
            }
        }
        assert_eq!(read, (epoch - 1) * 50);
    }

    /// What records carry of pending sets: 1 MiB, or a 64th of a smaller
    /// job's memory, shared by the operators, and by each one's lanes as
    /// their key groups are of the pipeline's, so every lane of every
    /// operator together stays within it.
    #[test]
    fn records_carry_at_most_a_mib_of_pending_sets_shared_by_lanes() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("file://{}", dir.path().display());
        for (operators, memory, asked, total) in [
            (1, 0, None, 1 << 20),
            (3, 0, None, 1 << 20),
            (1, 4 << 30, None, 1 << 20),
            (2, 32 << 20, None, 512 << 10),
            (1, 4 << 10, None, 64),
            (2, 4 << 30, Some(3000), 3000),
            (1, 0, Some(0), 0),
        ] {
            use openetl_state::Stores;
            let state = JobState::open(&url, false, operators, memory, 256, asked, None).unwrap();
            let limit = state.pending();
            assert_eq!(limit.bytes, total / operators, "{operators}, {memory}");
            assert_eq!(limit.key_groups, 256);
            for lanes in [1u32, 3, 7, 64, 256] {
                let mut sum = 0;
                for i in 0..lanes {
                    let range = KeyGroupRange::new(256 * i / lanes, 256 * (i + 1) / lanes - 1);
                    sum += state.stores("w").pending_share(range);
                }
                assert!(sum * operators <= total, "{sum} on {lanes} lanes");
                assert!(sum + lanes as usize >= limit.bytes, "{sum}");
            }
        }
    }

    /// A lane that ends, holding state, stops counting toward the job's,
    /// while one that goes on still counts.
    #[test]
    fn a_lane_that_ends_stops_counting() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("file://{}", dir.path().display());
        let state = Arc::new(JobState::open(&url, false, 1, 64 << 20, 16, None, None).unwrap());
        let (a, b) = (state.lane("window-1", 0), state.lane("window-1", 1));
        a.hold(300 << 20);
        assert_eq!(b.hold(260 << 20), 560 << 20);
        drop(a);
        assert_eq!(b.hold(260 << 20), 260 << 20);
        let c = state.lane("window-2", 0);
        c.hold(5);
        drop((b, c));
        assert_eq!(state.hold("probe", 9, 0), 0);
    }

    #[test]
    fn an_operators_directory_is_its_id_or_its_bytes() {
        assert_eq!(dir("window-0123abcd_x"), "window-0123abcd_x");
        assert_eq!(dir("a/b"), "x.612f62");
        assert_eq!(dir("x.61"), "x.782e3631");
        assert_eq!(dir(""), "x.");
    }
}
