//! The layer's lifecycle: what a barrier commits, what a restore and a
//! rescale read, what a failed epoch leaves, and what it refuses.

#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use openetl_state::{
    Error, KeyGroupRange, KeyRef, LAYOUT, Layer, MemoryBucket, NEVER, OperatorId, Snapshot,
    StateBackend, Timer,
};

const OP: OperatorId = OperatorId(1);
const GROUPS: u32 = 256;

fn ranges(lanes: u32) -> Vec<KeyGroupRange> {
    (0..lanes)
        .map(|i| KeyGroupRange::new(GROUPS * i / lanes, GROUPS * (i + 1) / lanes - 1))
        .collect()
}

async fn lanes(
    bucket: &MemoryBucket,
    generation: u64,
    epoch: u64,
    n: u32,
    snapshots: &[Snapshot],
) -> Vec<Layer> {
    let mut out = Vec::new();
    for r in ranges(n) {
        out.push(
            Layer::open(bucket, generation, epoch, r, snapshots)
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

fn lane_of(lanes: &mut [Layer], g: u32) -> &mut Layer {
    lanes.iter_mut().find(|l| l.range().contains(g)).unwrap()
}

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

/// Whether `id` in group 0 is new at `at`.
async fn ins(l: &mut Layer, id: &'static [u8], at: i64, expires: i64) -> bool {
    let id = KeyRef::new(0, id);
    l.insert_new(OP, &[id], &[at], &[expires]).await.unwrap()[0]
}

#[tokio::test]
async fn an_unrecorded_epoch_leaves_no_trace() {
    let bucket = MemoryBucket::new();
    let mut ls = lanes(&bucket, 1, 1, 2, &[]).await;
    let k = KeyRef::new(200, b"k");
    lane_of(&mut ls, 200).put(OP, k, b("one"));
    let recorded = barrier(&mut ls, 1).await;
    let l = lane_of(&mut ls, 200);
    l.put(OP, k, b("two"));
    l.append(OP, k, 5, b("row"));
    l.set_timer(OP, k, 5);
    assert_eq!(
        l.insert_new(OP, &[k], &[0], &[NEVER]).await.unwrap(),
        [true]
    );
    barrier(&mut ls, 2).await;
    // The attempt dies before record 2 is written.
    drop(ls);
    let mut ls = lanes(&bucket, 2, 2, 2, &recorded).await;
    let l = lane_of(&mut ls, 200);
    assert_eq!(l.get(OP, &[k]).await.unwrap(), [Some(b("one"))]);
    assert_eq!(l.rows(OP, &[(k, 5)]).await.unwrap(), [Vec::<Bytes>::new()]);
    assert!(l.fire(OP, i64::MAX, 10).await.unwrap().is_empty());
    assert_eq!(
        l.insert_new(OP, &[k], &[0], &[NEVER]).await.unwrap(),
        [true]
    );
}

#[tokio::test]
async fn a_restore_reads_exactly_the_recorded_epoch() {
    let bucket = MemoryBucket::new();
    let mut ls = lanes(&bucket, 1, 1, 1, &[]).await;
    let k = KeyRef::new(7, b"k");
    let mut records = Vec::new();
    for epoch in 1..=4 {
        ls[0].put(OP, k, b(&epoch.to_string()));
        records.push(barrier(&mut ls, epoch).await);
    }
    for (i, snapshots) in records.iter().enumerate() {
        let mut ls = lanes(&bucket, 2 + i as u64, i as u64 + 2, 4, snapshots).await;
        let got = lane_of(&mut ls, 7).get(OP, &[k]).await.unwrap();
        assert_eq!(got, [Some(b(&(i + 1).to_string()))]);
    }
}

/// State written on 4 lanes, restored on 3 and on 8, then on 1: every
/// value, row, timer and id is where its key group went.
#[tokio::test]
async fn a_rescale_moves_state_by_key_group() {
    let bucket = MemoryBucket::new();
    let mut ls = lanes(&bucket, 1, 1, 4, &[]).await;
    for g in 0..GROUPS {
        let l = lane_of(&mut ls, g);
        let k = KeyRef::new(g, b"k");
        l.put(OP, k, b(&format!("v{g}")));
        l.append(OP, k, i64::from(g), b(&format!("r{g}")));
        l.set_timer(OP, k, i64::from(g % 7));
        l.insert_new(OP, &[k], &[0], &[100]).await.unwrap();
    }
    let written = barrier(&mut ls, 1).await;
    assert_eq!(written.len(), 4);
    let mut recorded = Vec::new();
    for (generation, n) in [(2, 3), (3, 8), (4, 1)] {
        let mut ls = lanes(&bucket, generation, 2, n, &written).await;
        for g in 0..GROUPS {
            let k = KeyRef::new(g, b"k");
            let l = lane_of(&mut ls, g);
            assert_eq!(l.get(OP, &[k]).await.unwrap(), [Some(b(&format!("v{g}")))]);
            let rows = l.rows(OP, &[(k, i64::from(g))]).await.unwrap();
            assert_eq!(rows, [vec![b(&format!("r{g}"))]]);
            assert_eq!(l.insert_new(OP, &[k], &[1], &[100]).await.unwrap(), [false]);
        }
        let mut timers = 0;
        for l in &mut ls {
            let range = l.range();
            let fired = l.fire(OP, 6, usize::MAX).await.unwrap();
            assert!(fired.iter().all(|t| range.contains(t.group)));
            assert!(
                fired
                    .windows(2)
                    .all(|w| (w[0].time, w[0].group) < (w[1].time, w[1].group))
            );
            timers += fired.len();
            let due = l.take_rows(OP, i64::MAX, usize::MAX).await.unwrap();
            let groups: Vec<u32> = due.iter().map(|r| r.group).collect();
            assert_eq!(groups, (range.first..=range.last).collect::<Vec<_>>());
        }
        assert_eq!(timers, GROUPS as usize);
        // What fired and was taken on 3 and 1 lanes is in no record.
        if n == 8 {
            recorded = barrier(&mut ls, 2).await;
        }
    }
    // After the epoch recorded on 8 lanes, the timers and rows are gone.
    let mut ls = lanes(&bucket, 5, 3, 2, &recorded).await;
    for l in &mut ls {
        assert!(l.fire(OP, i64::MAX, usize::MAX).await.unwrap().is_empty());
        assert!(
            l.take_rows(OP, i64::MAX, usize::MAX)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

/// A lane of every key group the layer takes drains rows and timers in
/// group order, and an empty drain reads each group once.
#[tokio::test]
async fn a_lane_of_every_key_group_drains() {
    let bucket = MemoryBucket::new();
    let range = KeyGroupRange::new(0, u32::from(u16::MAX));
    let mut l = Layer::open(&bucket, 1, 1, range, &[]).await.unwrap();
    for g in [65_535, 0, 40_000] {
        let k = KeyRef::new(g, b"k");
        l.append(OP, k, 1, b(&format!("r{g}")));
        l.set_timer(OP, k, 1);
    }
    let record = l.barrier(1).await.unwrap();
    let mut l = Layer::open(&bucket, 2, 2, range, &record).await.unwrap();
    let due = l.take_rows(OP, i64::MAX, usize::MAX).await.unwrap();
    let groups: Vec<u32> = due.iter().map(|r| r.group).collect();
    assert_eq!(groups, [0, 40_000, 65_535]);
    assert!(
        l.take_rows(OP, i64::MAX, usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );
    let fired = l.fire(OP, 1, usize::MAX).await.unwrap();
    assert_eq!(
        fired.iter().map(|t| t.group).collect::<Vec<_>>(),
        [0, 40_000, 65_535]
    );
}

#[tokio::test]
async fn a_failed_epoch_is_discarded() {
    let bucket = MemoryBucket::new();
    let mut l = Layer::open(&bucket, 1, 1, KeyGroupRange::new(0, 3), &[])
        .await
        .unwrap();
    let k = KeyRef::new(2, b"k");
    l.put(OP, k, b("kept"));
    l.set_timer(OP, k, 1);
    l.barrier(1).await.unwrap();
    l.put(OP, k, b("lost"));
    l.delete_timer(OP, k, 1);
    l.append(OP, k, 0, b("lost"));
    l.expire(OP, 50);
    assert!(l.memory().changes > 0);
    l.discard();
    assert_eq!(l.memory().changes, 0);
    assert_eq!(l.epoch(), 2);
    assert_eq!(l.get(OP, &[k]).await.unwrap(), [Some(b("kept"))]);
    assert!(l.take_rows(OP, i64::MAX, 9).await.unwrap().is_empty());
    let fired = l.fire(OP, 1, 9).await.unwrap();
    assert_eq!(fired.len(), 1);
    // The discarded horizon expired nothing.
    assert_eq!(l.insert_new(OP, &[k], &[0], &[10]).await.unwrap(), [true]);
    assert_eq!(l.insert_new(OP, &[k], &[5], &[10]).await.unwrap(), [false]);
    l.barrier(2).await.unwrap();
}

#[tokio::test]
async fn timers_fire_once_in_time_order_and_again_only_after_a_restore() {
    let bucket = MemoryBucket::new();
    let mut ls = lanes(&bucket, 1, 1, 2, &[]).await;
    for (g, t) in [(3, 5), (200, 3), (3, 9), (200, 5), (3, 3)] {
        lane_of(&mut ls, g).set_timer(OP, KeyRef::new(g, b"w"), t);
    }
    // Set twice, it's one timer.
    lane_of(&mut ls, 3).set_timer(OP, KeyRef::new(3, b"w"), 9);
    let record = barrier(&mut ls, 1).await;
    let times = |ts: &[Timer]| ts.iter().map(|t| (t.time, t.group)).collect::<Vec<_>>();
    assert_eq!(times(&ls[0].fire(OP, 4, 9).await.unwrap()), [(3, 3)]);
    assert!(ls[0].fire(OP, 4, 9).await.unwrap().is_empty());
    assert_eq!(times(&ls[0].fire(OP, 9, 1).await.unwrap()), [(5, 3)]);
    assert_eq!(times(&ls[0].fire(OP, 9, 9).await.unwrap()), [(9, 3)]);
    assert_eq!(
        times(&ls[1].fire(OP, i64::MAX, 9).await.unwrap()),
        [(3, 200), (5, 200)]
    );
    // Fired in an epoch no record names: a restore fires them again.
    let mut ls = lanes(&bucket, 2, 2, 1, &record).await;
    let fired = ls[0].fire(OP, i64::MAX, 9).await.unwrap();
    assert_eq!(times(&fired), [(3, 3), (3, 200), (5, 3), (5, 200), (9, 3)]);
    let record = barrier(&mut ls, 2).await;
    let mut ls = lanes(&bucket, 3, 3, 4, &record).await;
    for l in &mut ls {
        assert!(l.fire(OP, i64::MAX, 9).await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn the_set_expires_by_time_and_by_horizon() {
    let bucket = MemoryBucket::new();
    let mut l = Layer::open(&bucket, 1, 1, KeyGroupRange::new(0, 0), &[])
        .await
        .unwrap();
    let id = |k: &'static [u8]| KeyRef::new(0, k);
    // Live while its expiry is past the record's time.
    assert!(ins(&mut l, b"a", 0, 10).await);
    assert!(!ins(&mut l, b"a", 9, 20).await);
    assert!(ins(&mut l, b"a", 10, 20).await);
    assert!(!ins(&mut l, b"a", 19, 30).await);
    // An id twice in one call is a repeat the second time.
    let twice = l
        .insert_new(OP, &[id(b"b"), id(b"b")], &[0, 0], &[5, 5])
        .await
        .unwrap();
    assert_eq!(twice, [true, false]);
    assert!(ins(&mut l, b"forever", 0, NEVER).await);
    l.barrier(1).await.unwrap();
    // The horizon expires what's at or before it, whatever the time asked
    // at, and keeps the rest.
    l.expire(OP, 20);
    l.expire(OP, 3);
    assert!(ins(&mut l, b"a", 0, 30).await);
    assert!(!ins(&mut l, b"a", 0, 30).await);
    assert!(ins(&mut l, b"b", 0, 5).await);
    assert!(!ins(&mut l, b"forever", 1 << 40, NEVER).await);
    // An id that expires before it's asked for isn't kept.
    assert!(ins(&mut l, b"c", 0, 15).await);
    assert!(ins(&mut l, b"c", 0, 15).await);
    let record = l.barrier(2).await.unwrap();
    let before = l.memory().store;
    l.expire(OP, i64::MAX);
    l.barrier(3).await.unwrap();
    assert!(l.memory().store < before);
    assert!(!ins(&mut l, b"forever", i64::MAX, NEVER).await);
    // The horizon is restored with the record, in its key groups.
    let mut r = Layer::open(&bucket, 2, 3, KeyGroupRange::new(0, 0), &record)
        .await
        .unwrap();
    assert!(ins(&mut r, b"b", 0, 5).await);
    assert!(!ins(&mut r, b"a", 25, 30).await);
}

#[tokio::test]
async fn an_older_attempts_versions_never_count() {
    let bucket = MemoryBucket::new();
    let k = KeyRef::new(0, b"k");
    let range = KeyGroupRange::new(0, 255);
    let mut old = Layer::open(&bucket, 1, 1, range, &[]).await.unwrap();
    old.put(OP, k, b("one"));
    let record = old.barrier(1).await.unwrap();
    let mut new = Layer::open(&bucket, 2, 2, range, &record).await.unwrap();
    // Both commit epoch 2; neither takes the other's.
    old.put(OP, k, b("zombie"));
    old.barrier(2).await.unwrap();
    new.put(OP, k, b("two"));
    let record = new.barrier(2).await.unwrap();
    let mut l = Layer::open(&bucket, 3, 3, range, &record).await.unwrap();
    assert_eq!(l.get(OP, &[k]).await.unwrap(), [Some(b("two"))]);
}

#[tokio::test]
async fn a_key_outside_the_lanes_groups_is_refused() {
    let bucket = MemoryBucket::new();
    let mut l = Layer::open(&bucket, 1, 1, KeyGroupRange::new(4, 7), &[])
        .await
        .unwrap();
    let out = KeyRef::new(8, b"k");
    assert!(matches!(l.get(OP, &[out]).await, Err(Error::Invalid(_))));
    assert!(matches!(
        l.rows(OP, &[(out, 0)]).await,
        Err(Error::Invalid(_))
    ));
    let at = l.insert_new(OP, &[KeyRef::new(3, b"k")], &[0], &[1]).await;
    assert!(matches!(at, Err(Error::Invalid(_))));
    let short = l.insert_new(OP, &[KeyRef::new(4, b"k")], &[0], &[]).await;
    assert!(matches!(short, Err(Error::Invalid(_))));
    // A write out of range fails the barrier, and the layer with it.
    l.put(OP, KeyRef::new(5, b"k"), b("fine"));
    l.set_timer(OP, out, 1);
    assert!(matches!(l.barrier(2).await, Err(Error::Invalid(_))));
    assert!(matches!(l.barrier(1).await, Err(Error::Invalid(_))));
    assert!(matches!(
        l.get(OP, &[KeyRef::new(5, b"k")]).await,
        Err(Error::Invalid(_))
    ));
    assert!(bucket.is_empty());
}

#[tokio::test]
async fn a_restore_takes_whole_disjoint_snapshots_of_this_layout() {
    let bucket = MemoryBucket::new();
    let mut ls = lanes(&bucket, 1, 1, 4, &[]).await;
    let all = barrier(&mut ls, 1).await;
    let range = KeyGroupRange::new(0, GROUPS - 1);
    let open = |snapshots: Vec<Snapshot>| {
        let bucket = bucket.clone();
        async move { Layer::open(&bucket, 2, 2, range, &snapshots).await.err() }
    };
    assert!(open(all.clone()).await.is_none());
    let missing = all[..3].to_vec();
    assert!(matches!(open(missing).await, Some(Error::Snapshot(_))));
    let mut twice = all.clone();
    twice.push(all[0].clone());
    assert!(matches!(open(twice).await, Some(Error::Snapshot(_))));
    let mut other = all.clone();
    other[1].layout = LAYOUT + 1;
    assert!(matches!(open(other).await, Some(Error::Snapshot(_))));
    let mut unknown = all.clone();
    unknown[2].data = b("g9/e9/kg128-191");
    assert!(matches!(open(unknown).await, Some(Error::Snapshot(_))));
    // A part of the range needs only the snapshots that hold it.
    let part = Layer::open(&bucket, 2, 2, KeyGroupRange::new(70, 130), &all[1..3]).await;
    assert!(part.is_ok());
    let wide = Layer::open(&bucket, 2, 2, KeyGroupRange::new(0, 65536), &[]).await;
    assert!(matches!(wide.err(), Some(Error::Invalid(_))));
    assert!(Layer::open(&bucket, 2, 0, range, &all).await.is_err());
}

#[tokio::test]
async fn memory_counts_the_epochs_changes_and_the_store() {
    let bucket = MemoryBucket::new();
    let mut l = Layer::open(&bucket, 1, 1, KeyGroupRange::new(0, 15), &[])
        .await
        .unwrap();
    assert_eq!(l.memory().total(), 0);
    let big = Bytes::from(vec![7u8; 10_000]);
    for g in 0..16 {
        l.put(OP, KeyRef::new(g, b"k"), big.clone());
    }
    let m = l.memory();
    assert!(m.changes >= 160_000 && m.changes < 170_000, "{m:?}");
    assert_eq!(m.store, 0);
    // Replacing a value counts the new one only.
    l.put(OP, KeyRef::new(0, b"k"), b("small"));
    assert!(l.memory().changes < m.changes - 9_000);
    l.barrier(1).await.unwrap();
    let m = l.memory();
    assert_eq!(m.changes, 0);
    assert!(m.store >= 150_000 && m.store < 160_000, "{m:?}");
    for g in 0..16 {
        l.delete(OP, KeyRef::new(g, b"k"));
    }
    l.barrier(2).await.unwrap();
    assert_eq!(l.memory().total(), 0);
}
