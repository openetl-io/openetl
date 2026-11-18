//! Early flushes beside pending sets: what a flush mid-epoch writes when a
//! pending set exists, what the barrier's snapshot then carries, the order
//! reads take (the open epoch, the pending set, early runs, older runs),
//! a discard after such a flush, and what the layer counts in memory.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path;
use openetl_state::runs::{RunName, runs};
use openetl_state::{
    KeyGroupRange, KeyRef, Layer, ObjectStorage, OperatorId, PendingLimit, RunConfig, RunStores,
    Snapshot, StateBackend,
};

const OP: OperatorId = OperatorId(1);
const GROUPS: u32 = 4;

fn stores(objects: &Arc<ObjectStorage>) -> RunStores {
    let limit = PendingLimit {
        bytes: 1 << 20,
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

async fn value(l: &mut Layer, k: &str) -> Option<String> {
    let got = l.get(OP, &[KeyRef::new(1, k.as_bytes())]).await.unwrap();
    got[0]
        .as_ref()
        .map(|v| String::from_utf8(v.to_vec()).unwrap())
}

fn put(l: &mut Layer, k: &str, v: &str) {
    l.put(OP, KeyRef::new(1, k.as_bytes()), Bytes::from(v.to_owned()));
}

/// The runs a snapshot names: (epoch, early flush's number or none).
fn named(s: &[Snapshot]) -> Vec<(u64, Option<u32>)> {
    let mut out: Vec<_> = s
        .iter()
        .flat_map(|s| runs(s).unwrap())
        .map(|r| {
            let n = RunName::parse(&r.name).unwrap();
            (n.epoch, n.early)
        })
        .collect();
    out.sort();
    out
}

/// Epoch 1 flushes early, epoch 2 rides in the record as a pending set.
/// Epoch 3 flushes early: the pending set goes as a run of its own, first,
/// then the epoch's changes, and memory holds neither. Reads take the open
/// epoch over the pending set over the early runs over the older run, and
/// the barrier's snapshot names both early runs and carries what epoch 3
/// changed after the flush; a restore reads the same.
#[tokio::test]
async fn a_flush_takes_the_pending_set_and_reads_keep_their_order() {
    let objects = bucket();
    let s = stores(&objects);
    let range = KeyGroupRange::new(0, GROUPS - 1);
    let mut l = Layer::open(&s, 1, 1, range, &[]).await.unwrap();
    put(&mut l, "a", "a1");
    put(&mut l, "b", "b1");
    put(&mut l, "c", "c1");
    put(&mut l, "d", "d1");
    l.flush().await.unwrap();
    l.barrier(1).await.unwrap();
    // Epoch 2: within the share, so it rides in the record.
    put(&mut l, "b", "b2");
    put(&mut l, "c", "c2");
    put(&mut l, "d", "d2");
    let two = l.barrier(2).await.unwrap();
    assert!(two.iter().any(|s| !s.pending.is_empty()));
    assert!(l.memory().pending > 0);
    // Epoch 3 changes c and d, flushes, then changes d again.
    put(&mut l, "c", "c3");
    put(&mut l, "d", "d3");
    let before = s.counts().early.count;
    assert!(l.flush().await.unwrap());
    assert_eq!(
        s.counts().early.count - before,
        2,
        "the pending set's run, and the epoch's"
    );
    let m = l.memory();
    assert_eq!((m.changes, m.pending), (0, 0));
    put(&mut l, "d", "d4");
    for (k, want) in [("a", "a1"), ("b", "b2"), ("c", "c3"), ("d", "d4")] {
        assert_eq!(value(&mut l, k).await.as_deref(), Some(want), "{k}");
    }
    let three = l.barrier(3).await.unwrap();
    // Epoch 1's flush took all it changed, so its barrier wrote no run.
    assert_eq!(named(&three), [(1, Some(1)), (3, Some(1)), (3, Some(2))]);
    assert!(three.iter().any(|s| !s.pending.is_empty()), "d4 rides");
    let mut back = Layer::open(&stores(&objects), 2, 4, range, &three)
        .await
        .unwrap();
    for (k, want) in [("a", "a1"), ("b", "b2"), ("c", "c3"), ("d", "d4")] {
        assert_eq!(
            value(&mut back, k).await.as_deref(),
            Some(want),
            "restored {k}"
        );
    }
    // A restore of epoch 2's record reads none of epoch 3's flushes.
    let mut two_back = Layer::open(&stores(&objects), 3, 3, range, &two)
        .await
        .unwrap();
    for (k, want) in [("a", "a1"), ("b", "b2"), ("c", "c2"), ("d", "d2")] {
        assert_eq!(
            value(&mut two_back, k).await.as_deref(),
            Some(want),
            "{k} of epoch 2"
        );
    }
}

/// A discard after an early flush that took the pending set forgets the
/// epoch's own changes and keeps the pending set's: reads, and the next
/// barrier's snapshot, are as if the epoch had never started.
#[tokio::test]
async fn a_discard_after_a_flush_keeps_the_pending_set() {
    let objects = bucket();
    let s = stores(&objects);
    let range = KeyGroupRange::new(0, GROUPS - 1);
    let mut l = Layer::open(&s, 1, 1, range, &[]).await.unwrap();
    put(&mut l, "a", "a1");
    l.flush().await.unwrap();
    l.barrier(1).await.unwrap();
    put(&mut l, "a", "a2");
    put(&mut l, "b", "b2");
    l.barrier(2).await.unwrap();
    // Epoch 3 flushes, pending set and all, and fails.
    put(&mut l, "a", "a3");
    put(&mut l, "c", "c3");
    assert!(l.flush().await.unwrap());
    l.discard();
    for (k, want) in [("a", Some("a2")), ("b", Some("b2")), ("c", None)] {
        assert_eq!(value(&mut l, k).await.as_deref(), want, "{k}");
    }
    // Epoch 3 again: its barrier names the pending set's run, which a
    // restore reads, and not the discarded one.
    put(&mut l, "d", "d3");
    let three = l.barrier(3).await.unwrap();
    // Epoch 1's run is dead: the pending set's overrides all it held.
    assert_eq!(named(&three), [(3, Some(1))]);
    let mut back = Layer::open(&stores(&objects), 2, 4, range, &three)
        .await
        .unwrap();
    for (k, want) in [
        ("a", Some("a2")),
        ("b", Some("b2")),
        ("c", None),
        ("d", Some("d3")),
    ] {
        assert_eq!(value(&mut back, k).await.as_deref(), want, "restored {k}");
    }
}

/// The threshold counts the pending set with the open epoch's changes: a
/// lane whose pending set alone passes it flushes as it next reads.
#[tokio::test]
async fn the_threshold_counts_the_pending_set() {
    let objects = bucket();
    let s = stores(&objects);
    let range = KeyGroupRange::new(0, GROUPS - 1);
    let mut l = Layer::open(&s, 1, 1, range, &[]).await.unwrap();
    for i in 0..50 {
        put(&mut l, &format!("k{i:03}"), "v");
    }
    l.barrier(1).await.unwrap();
    let pending = l.memory().pending;
    assert!(pending > 0 && l.memory().changes == 0);
    assert_eq!(l.memory().total(), pending + l.memory().store);
    l.set_flush_at(pending - 1);
    assert_eq!(value(&mut l, "k007").await.as_deref(), Some("v"));
    assert_eq!(l.flushes(), 1);
    assert_eq!(l.memory().pending, 0);
}
