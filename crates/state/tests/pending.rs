//! Pending sets: a lane's changes since its last run, which its snapshots
//! carry while they're within its share of what a record may carry, and a
//! run takes once they pass it; how a restore reads them, a lane's own part
//! of them after a rescale, deletes of what older runs hold, and the bound
//! on what a record carries.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path;
use openetl_state::runs::runs;
use openetl_state::{
    Error, KeyGroupRange, KeyRef, Layer, ObjectStorage, OperatorId, PendingLimit, RunConfig,
    RunStores, Snapshot, StateBackend,
};

const OP: OperatorId = OperatorId(1);
const GROUPS: u32 = 64;

fn stores(objects: &Arc<ObjectStorage>, bytes: usize) -> RunStores {
    let limit = PendingLimit {
        bytes,
        key_groups: GROUPS,
    };
    RunStores::new(objects.clone(), RunConfig::default()).with_pending(limit)
}

fn bucket() -> Arc<ObjectStorage> {
    Arc::new(ObjectStorage::new(
        Arc::new(InMemory::new()),
        Path::from("state"),
    ))
}

fn ranges(lanes: u32) -> Vec<KeyGroupRange> {
    (0..lanes)
        .map(|i| KeyGroupRange::new(GROUPS * i / lanes, GROUPS * (i + 1) / lanes - 1))
        .collect()
}

async fn open(
    stores: &RunStores,
    generation: u64,
    epoch: u64,
    lanes: u32,
    from: &[Snapshot],
) -> Vec<Layer> {
    let mut out = Vec::new();
    for r in ranges(lanes) {
        let mine: Vec<Snapshot> = from
            .iter()
            .filter(|s| s.range.overlaps(&r))
            .cloned()
            .collect();
        out.push(
            Layer::open(stores, generation, epoch, r, &mine)
                .await
                .unwrap(),
        );
    }
    out
}

async fn barrier(lanes: &mut [Layer], epoch: u64) -> Vec<Snapshot> {
    let mut out = Vec::new();
    for l in lanes {
        out.extend(l.barrier(epoch).await.unwrap());
    }
    out
}

fn lane_of(lanes: &[Layer], g: u32) -> usize {
    lanes.iter().position(|l| l.range().contains(g)).unwrap()
}

fn key(i: u32) -> Vec<u8> {
    format!("window-{i:05}").into_bytes()
}

fn carried(snapshots: &[Snapshot]) -> usize {
    snapshots.iter().map(|s| s.pending.len()).sum()
}

fn named_runs(snapshots: &[Snapshot]) -> usize {
    snapshots.iter().map(|s| runs(s).unwrap().len()).sum()
}

/// Windows that open, take rows and close within a few epochs: a value and
/// a timer each. Gives the values open after `epochs`.
async fn windows(lanes: &mut [Layer], from: u64, epochs: u64, per: u32) -> Vec<Snapshot> {
    let mut snapshots = Vec::new();
    for e in from..from + epochs {
        let base = u32::try_from(e).unwrap() * per;
        for i in base..base + per {
            let g = i % GROUPS;
            let k = key(i);
            let l = &mut lanes[lane_of(lanes, g)];
            l.put(OP, KeyRef::new(g, &k), Bytes::from(vec![7; 40]));
            l.set_timer(OP, KeyRef::new(g, &k), i64::from(i) + 3 * i64::from(per));
        }
        // Each lane closes the windows due.
        let due = i64::from(base);
        for l in lanes.iter_mut() {
            for t in l.fire(OP, due, usize::MAX).await.unwrap() {
                l.delete(OP, KeyRef::new(t.group, &t.key));
            }
            l.expire(OP, due);
        }
        snapshots = barrier(lanes, e).await;
    }
    snapshots
}

/// A quiet pipeline's state costs no request: its lanes' records carry
/// every change, which stays within the shares, and a restore onto another
/// number of lanes reads it all back from the record alone.
#[tokio::test]
async fn a_quiet_lanes_record_carries_its_changes_and_no_run_is_written() {
    let objects = bucket();
    let s = stores(&objects, 1 << 20);
    let mut lanes = open(&s, 1, 1, 4, &[]).await;
    let snaps = windows(&mut lanes, 1, 40, 30).await;
    assert_eq!(s.counts().barrier.count, 0, "{:?}", s.counts());
    assert_eq!(named_runs(&snaps), 0);
    assert!(carried(&snaps) > 0);
    assert!(carried(&snaps) <= 1 << 20);
    for (l, sn) in lanes.iter().zip(&snaps) {
        assert!(
            sn.pending.len() <= l.pending_share(),
            "{} bytes",
            sn.pending.len()
        );
        assert_eq!(l.pending_share(), (1 << 20) / 4);
    }
    // The windows still open: the last barrier, at 1,200, fired the timers
    // of every key up to 1,110.
    let open_keys: Vec<u32> = (1111..1230).collect();
    let back = stores(&objects, 1 << 20);
    let mut restored = open(&back, 2, 41, 3, &snaps).await;
    let mut found = 0;
    for &i in &open_keys {
        let g = i % GROUPS;
        let k = key(i);
        let at = lane_of(&restored, g);
        let l = &mut restored[at];
        let got = l.get(OP, &[KeyRef::new(g, &k)]).await.unwrap();
        found += usize::from(got[0].is_some());
    }
    assert_eq!(found, open_keys.len());
    // A window closed in the first attempt stays closed.
    let at = lane_of(&restored, 30);
    let l = &mut restored[at];
    assert_eq!(
        l.get(OP, &[KeyRef::new(30, &key(30))]).await.unwrap(),
        [None]
    );
    assert_eq!(back.counts().restore.count + back.counts().lookup.count, 0);
    // Every timer still due is restored, and fires once.
    let mut fired = 0;
    for l in restored.iter_mut() {
        fired += l.fire(OP, i64::MAX, usize::MAX).await.unwrap().len();
    }
    assert_eq!(fired, found);
}

/// A lane whose pending set passes its share writes it as one run at that
/// barrier, and starts again from empty: the next small epoch rides in the
/// record over that run.
#[tokio::test]
async fn past_its_share_a_lane_writes_one_run_and_its_pending_set_starts_empty() {
    let objects = bucket();
    let s = stores(&objects, 4096);
    let range = KeyGroupRange::new(0, GROUPS - 1);
    let mut lane = Layer::open(&s, 1, 1, range, &[]).await.unwrap();
    assert_eq!(lane.pending_share(), 4096);
    let mut epoch = 1;
    let mut written = 0;
    while written == 0 {
        assert!(epoch < 100);
        for i in 0..10 {
            let k = key(u32::try_from(epoch).unwrap() * 10 + i);
            lane.put(OP, KeyRef::new(i, &k), Bytes::from(vec![1; 50]));
        }
        let snaps = lane.barrier(epoch).await.unwrap();
        written = s.counts().barrier.count;
        match written {
            0 => assert!(!snaps[0].pending.is_empty() && snaps[0].pending.len() <= 4096),
            _ => {
                assert_eq!(written, 1);
                assert!(snaps[0].pending.is_empty());
                assert_eq!(lane.memory().pending, 0);
                assert_eq!(lane.commits(), 1);
                assert_eq!(named_runs(&snaps), 1);
            }
        }
        epoch += 1;
    }
    assert!(epoch > 3, "a run at epoch {}", epoch - 1);
    lane.put(OP, KeyRef::new(3, b"later"), Bytes::from_static(b"v"));
    let snaps = lane.barrier(epoch).await.unwrap();
    assert_eq!(s.counts().barrier.count, 1);
    assert!(!snaps[0].pending.is_empty());
    let mut back = Layer::open(&s, 2, epoch + 1, range, &snaps).await.unwrap();
    let got = back
        .get(OP, &[KeyRef::new(3, b"later"), KeyRef::new(0, &key(10))])
        .await
        .unwrap();
    assert_eq!(got[0].as_deref(), Some(&b"v"[..]));
    assert_eq!(got[1].as_deref(), Some(&[1; 50][..]));
}

/// A pending set carries a delete of what an older run holds, and the
/// record with it restores the key as gone; a delete of what no run holds
/// leaves the set, so windows that open and close between runs cost the
/// record nothing.
#[tokio::test]
async fn a_pending_delete_of_what_a_run_holds_is_carried_and_one_of_nothing_isnt() {
    let objects = bucket();
    let range = KeyGroupRange::new(0, GROUPS - 1);
    // An attempt that writes a run at every barrier.
    let every = stores(&objects, 0);
    let mut lane = Layer::open(&every, 1, 1, range, &[]).await.unwrap();
    lane.put(OP, KeyRef::new(5, b"old"), Bytes::from_static(b"in a run"));
    let first = lane.barrier(1).await.unwrap();
    assert_eq!(named_runs(&first), 1);
    // The next carries its changes in the record.
    let s = stores(&objects, 1 << 20);
    let mut lane = Layer::open(&s, 2, 2, range, &first).await.unwrap();
    lane.delete(OP, KeyRef::new(5, b"old"));
    // A window looks its key up first, which opens the run: a run not open
    // may hold anything.
    let got = lane.get(OP, &[KeyRef::new(6, b"brief")]).await.unwrap();
    assert_eq!(got, [None]);
    lane.put(OP, KeyRef::new(6, b"brief"), Bytes::from_static(b"x"));
    let second = lane.barrier(2).await.unwrap();
    assert!(!second[0].pending.is_empty());
    let with_delete = second[0].pending.len();
    lane.delete(OP, KeyRef::new(6, b"brief"));
    let third = lane.barrier(3).await.unwrap();
    assert_eq!(s.counts().barrier.count, 0);
    assert_eq!(named_runs(&third), 1, "the run of epoch 1 is still named");
    assert!(
        third[0].pending.len() < with_delete,
        "the brief window's delete left"
    );
    assert!(!third[0].pending.is_empty(), "the old key's delete stays");
    let mut back = Layer::open(&s, 3, 4, range, &third).await.unwrap();
    let keys = [KeyRef::new(5, b"old"), KeyRef::new(6, b"brief")];
    assert_eq!(back.get(OP, &keys).await.unwrap(), [None, None]);
    // Without the pending set, the run's value would come back: the
    // record's pending set is what deletes it.
    let stripped = vec![Snapshot {
        pending: Bytes::new(),
        ..third[0].clone()
    }];
    let mut without = Layer::open(&s, 4, 4, range, &stripped).await.unwrap();
    let got = without.get(OP, &keys[..1]).await.unwrap();
    assert_eq!(got[0].as_deref(), Some(&b"in a run"[..]));
    // Once the delete is written as a run, the record stops naming the
    // value's section and the delete with it.
    let mut back = Layer::open(&every, 5, 4, range, &third).await.unwrap();
    let fourth = back.barrier(4).await.unwrap();
    assert!(fourth[0].pending.is_empty());
    assert_eq!(named_runs(&fourth), 0, "{:?}", runs(&fourth[0]).unwrap());
}

/// After a rescale each lane takes the part of the pending sets in its own
/// key groups, and the record carries each entry once.
#[tokio::test]
async fn a_lane_takes_only_its_part_of_the_pending_sets() {
    let objects = bucket();
    let s = stores(&objects, 1 << 20);
    let mut two = open(&s, 1, 1, 2, &[]).await;
    for i in 0..200 {
        let g = i % GROUPS;
        let k = key(i);
        let at = lane_of(&two, g);
        two[at].put(OP, KeyRef::new(g, &k), Bytes::from_static(b"v"));
    }
    let snaps = barrier(&mut two, 1).await;
    let before: usize = two.iter().map(|l| l.memory().pending).sum();
    for lanes in [5, 1, 3] {
        let mut more = open(&s, 2, 2, lanes, &snaps).await;
        let after: usize = more.iter().map(|l| l.memory().pending).sum();
        assert_eq!(after, before, "on {lanes} lanes");
        let again = barrier(&mut more, 2).await;
        for (l, sn) in more.iter_mut().zip(&again) {
            assert_eq!(sn.pending.is_empty(), l.memory().pending == 0);
            let r = l.range();
            for i in 0..200 {
                let g = i % GROUPS;
                if r.contains(g) {
                    let got = l.get(OP, &[KeyRef::new(g, &key(i))]).await.unwrap();
                    assert_eq!(got[0].as_deref(), Some(&b"v"[..]));
                }
            }
            assert!(sn.pending.len() <= l.pending_share());
        }
    }
    assert_eq!(s.counts().barrier.count, 0);
}

/// What the horizon passes leaves the pending set: rows taken, and timers
/// and a set's entries due.
#[tokio::test]
async fn the_horizon_empties_the_pending_set() {
    let objects = bucket();
    let s = stores(&objects, 1 << 20);
    let range = KeyGroupRange::new(0, GROUPS - 1);
    let mut lane = Layer::open(&s, 1, 1, range, &[]).await.unwrap();
    for i in 0..20u32 {
        let k = key(i);
        lane.append(
            OP,
            KeyRef::new(i, &k),
            i64::from(i),
            Bytes::from_static(b"row"),
        );
        lane.set_timer(OP, KeyRef::new(i, &k), i64::from(i));
        lane.insert_new(OP, &[KeyRef::new(i, &k)], &[0], &[i64::from(i) + 1])
            .await
            .unwrap();
    }
    let snaps = lane.barrier(1).await.unwrap();
    assert!(!snaps[0].pending.is_empty());
    lane.expire(OP, 100);
    let snaps = lane.barrier(2).await.unwrap();
    assert!(snaps[0].pending.is_empty());
    assert_eq!(lane.memory().pending, 0);
    assert_eq!(s.counts().barrier.count, 0);
}

/// A pending set that doesn't read fails the restore; a snapshot from
/// before pending sets, which has none, restores.
#[tokio::test]
async fn a_pending_set_that_doesnt_read_fails_the_restore() {
    let objects = bucket();
    let s = stores(&objects, 1 << 20);
    let range = KeyGroupRange::new(0, GROUPS - 1);
    let mut lane = Layer::open(&s, 1, 1, range, &[]).await.unwrap();
    lane.put(OP, KeyRef::new(1, b"k"), Bytes::from_static(b"v"));
    let snaps = lane.barrier(1).await.unwrap();
    let mut bad = snaps[0].pending.to_vec();
    bad[3] ^= 0x40;
    let broken = vec![Snapshot {
        pending: Bytes::from(bad),
        ..snaps[0].clone()
    }];
    let err = Layer::open(&s, 2, 2, range, &broken).await.err().unwrap();
    assert!(matches!(err, Error::Snapshot(_)), "{err}");
    // Its key groups are another snapshot's.
    let moved = vec![
        Snapshot {
            range: KeyGroupRange::new(2, GROUPS - 1),
            ..snaps[0].clone()
        },
        Snapshot {
            range: KeyGroupRange::new(0, 1),
            ..snaps[0].clone()
        },
    ];
    let err = Layer::open(&s, 2, 2, range, &moved).await.err().unwrap();
    assert!(matches!(err, Error::Snapshot(_)), "{err}");
    let old = vec![Snapshot {
        pending: Bytes::new(),
        ..snaps[0].clone()
    }];
    assert!(Layer::open(&s, 2, 2, range, &old).await.is_ok());
}
