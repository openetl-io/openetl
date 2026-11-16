//! What a snapshot leaves out: sections of rows, timers and a set once
//! their operators' horizons pass their latest entry, and values sections
//! once newer ones override every entry or their deletes delete nothing
//! older; what a restore reads of those it keeps; and lanes that know a
//! restored section only once they read it.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path;
use openetl_state::runs::{Counts, named, runs};
use openetl_state::{
    KeyGroupRange, KeyRef, Layer, NEVER, ObjectStorage, OperatorId, RunConfig, RunStores,
    SectionKind, Snapshot, StateBackend,
};

const A: OperatorId = OperatorId(1);
const B: OperatorId = OperatorId(2);

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

fn bucket() -> Arc<ObjectStorage> {
    Arc::new(ObjectStorage::new(
        Arc::new(InMemory::new()),
        Path::from("state"),
    ))
}

fn stores(objects: &Arc<ObjectStorage>, drop: bool) -> RunStores {
    let config = RunConfig {
        drop,
        ..RunConfig::default()
    };
    RunStores::new(objects.clone(), config)
}

async fn lane(
    stores: &RunStores,
    generation: u64,
    epoch: u64,
    range: (u32, u32),
    from: &[Snapshot],
) -> Layer {
    let range = KeyGroupRange::new(range.0, range.1);
    let mine: Vec<Snapshot> = from
        .iter()
        .filter(|s| s.range.overlaps(&range))
        .cloned()
        .collect();
    Layer::open(stores, generation, epoch, range, &mine)
        .await
        .unwrap()
}

/// Each section a snapshot names: its run's epoch, its kind and its
/// groups.
fn sections(snapshots: &[Snapshot]) -> Vec<(u64, SectionKind, u32, u32)> {
    let mut out = Vec::new();
    for s in snapshots {
        for r in runs(s).unwrap() {
            for sec in r.sections {
                out.push((sec.epoch, sec.kind, sec.groups.first, sec.groups.last));
            }
        }
    }
    out.sort();
    out
}

fn kinds_of(snapshots: &[Snapshot], epoch: u64) -> Vec<SectionKind> {
    sections(snapshots)
        .into_iter()
        .filter(|s| s.0 == epoch)
        .map(|s| s.1)
        .collect()
}

/// A section of timers or rows that holds entries the horizon passed and
/// later ones stays, and a restore reads only the later ones: the deletes
/// of the earlier ones may be gone with their own sections. Once the
/// horizon passes its latest, it goes too.
#[tokio::test]
async fn a_section_straddling_the_horizon_stays_and_hides_what_it_passed() {
    for drop in [true, false] {
        let objects = bucket();
        let s = stores(&objects, drop);
        let mut l = lane(&s, 1, 1, (0, 3), &[]).await;
        let k = KeyRef::new(1, b"k");
        for t in [5, 20] {
            l.set_timer(A, k, t);
            l.append(A, k, t, b(&format!("row {t}")));
        }
        let one = l.barrier(1).await.unwrap();
        assert_eq!(kinds_of(&one, 1), [SectionKind::Rows, SectionKind::Timers]);
        // Epoch 2 fires and takes what's due at 10, and moves the horizon
        // there: its deletes are of what the horizon passed, so it writes
        // nothing, and epoch 1's sections straddle it.
        assert_eq!(l.fire(A, 10, usize::MAX).await.unwrap().len(), 1);
        assert_eq!(l.take_rows(A, 10, usize::MAX).await.unwrap().len(), 1);
        l.expire(A, 10);
        let two = l.barrier(2).await.unwrap();
        assert_eq!(s.counts().barrier.count, 1, "drop {drop}");
        assert_eq!(kinds_of(&two, 1), [SectionKind::Rows, SectionKind::Timers]);
        let h = named(&two[0]).unwrap().horizons;
        assert_eq!((h.len(), h[0].operator, h[0].at), (1, A, 10));
        // Restored, the timer and the row at 5 stay gone.
        let r = stores(&objects, drop);
        let mut back = lane(&r, 2, 3, (0, 3), &two).await;
        assert!(back.rows(A, &[(k, 5)]).await.unwrap()[0].is_empty());
        let fired = back.fire(A, i64::MAX, usize::MAX).await.unwrap();
        assert_eq!(fired.iter().map(|t| t.time).collect::<Vec<_>>(), [20]);
        let due = back.take_rows(A, i64::MAX, usize::MAX).await.unwrap();
        assert_eq!(due.iter().map(|r| r.time).collect::<Vec<_>>(), [20]);
        // A timer or a row at or before the horizon is gone at once.
        back.set_timer(A, k, 10);
        back.append(A, k, 3, b("late"));
        assert!(back.fire(A, 10, usize::MAX).await.unwrap().is_empty());
        assert!(back.rows(A, &[(k, 3)]).await.unwrap()[0].is_empty());
        // Once the horizon passes 20, epoch 1's sections are dead.
        l.fire(A, 30, usize::MAX).await.unwrap();
        l.take_rows(A, 30, usize::MAX).await.unwrap();
        l.expire(A, 30);
        let three = l.barrier(3).await.unwrap();
        match drop {
            true => assert!(sections(&three).is_empty(), "{:?}", sections(&three)),
            false => assert_eq!(sections(&three), sections(&one)),
        }
    }
}

/// A set's section goes once the horizon passes each expiry it holds, and
/// never while it holds one that never expires.
#[tokio::test]
async fn a_set_section_goes_when_every_entry_it_holds_expired() {
    let s = stores(&bucket(), true);
    let mut l = lane(&s, 1, 1, (0, 0), &[]).await;
    let id = |i: &'static [u8]| KeyRef::new(0, i);
    l.insert_new(A, &[id(b"a"), id(b"b")], &[0, 0], &[10, 20])
        .await
        .unwrap();
    l.barrier(1).await.unwrap();
    l.insert_new(A, &[id(b"c")], &[0], &[NEVER]).await.unwrap();
    l.barrier(2).await.unwrap();
    l.expire(A, 15);
    let three = l.barrier(3).await.unwrap();
    assert_eq!(kinds_of(&three, 1), [SectionKind::Set], "b expires at 20");
    l.expire(A, i64::MAX);
    let four = l.barrier(4).await.unwrap();
    assert_eq!(sections(&four), [(2, SectionKind::Set, 0, 0)]);
    let mut back = lane(&s, 2, 5, (0, 0), &four).await;
    let fresh = back
        .insert_new(A, &[id(b"a"), id(b"b"), id(b"c")], &[0; 3], &[NEVER; 3])
        .await
        .unwrap();
    assert_eq!(fresh, [true, true, false]);
}

/// A section of several operators goes once each one's horizon passed it.
#[tokio::test]
async fn a_section_of_two_operators_goes_once_both_horizons_pass_it() {
    let s = stores(&bucket(), true);
    let mut l = lane(&s, 1, 1, (0, 1), &[]).await;
    l.set_timer(A, KeyRef::new(0, b"a"), 5);
    l.set_timer(B, KeyRef::new(1, b"b"), 5);
    l.barrier(1).await.unwrap();
    l.fire(A, 9, usize::MAX).await.unwrap();
    l.expire(A, 9);
    assert_eq!(
        kinds_of(&l.barrier(2).await.unwrap(), 1),
        [SectionKind::Timers]
    );
    l.fire(B, 9, usize::MAX).await.unwrap();
    l.expire(B, 9);
    assert!(sections(&l.barrier(3).await.unwrap()).is_empty());
}

/// A values section goes once newer sections hold every key it holds; a
/// delete that's the newest of its key stays while an older section holds
/// the key, so the older value doesn't come back, and goes with it.
#[tokio::test]
async fn a_delete_stays_while_an_older_section_holds_its_key() {
    let objects = bucket();
    let s = stores(&objects, true);
    let mut l = lane(&s, 1, 1, (0, 0), &[]).await;
    let (a, c) = (KeyRef::new(0, b"a"), KeyRef::new(0, b"c"));
    l.put(A, a, b("a1"));
    l.put(A, c, b("c1"));
    l.barrier(1).await.unwrap();
    l.put(A, a, b("a2"));
    l.barrier(2).await.unwrap();
    // Epoch 3 deletes a: epoch 2's section, all overridden, goes; epoch
    // 1's holds c still, and an a that the delete must keep hiding.
    l.delete(A, a);
    let three = l.barrier(3).await.unwrap();
    let values = |snaps: &[Snapshot]| -> Vec<u64> {
        sections(snaps)
            .into_iter()
            .filter(|s| s.1 == SectionKind::Values)
            .map(|s| s.0)
            .collect()
    };
    assert_eq!(values(&three), [1, 3]);
    let r = stores(&objects, true);
    let mut back = lane(&r, 2, 4, (0, 0), &three).await;
    assert_eq!(back.get(A, &[a, c]).await.unwrap(), [None, Some(b("c1"))]);
    // Once c is overridden, epoch 1's section goes, and with it the
    // delete, which hides nothing any more.
    l.put(A, c, b("c4"));
    let four = l.barrier(4).await.unwrap();
    assert_eq!(values(&four), [4]);
    let r = stores(&objects, true);
    let mut back = lane(&r, 3, 5, (0, 0), &four).await;
    assert_eq!(back.get(A, &[a, c]).await.unwrap(), [None, Some(b("c4"))]);
}

/// An operator whose every window closed names nothing: its values were
/// deleted and its timers fired, and the horizon passed them. The
/// snapshot keeps the horizon.
#[tokio::test]
async fn an_operator_with_nothing_live_left_names_nothing() {
    let objects = bucket();
    let s = stores(&objects, true);
    let mut l = lane(&s, 1, 1, (0, 7), &[]).await;
    for g in 0..8u32 {
        let k = KeyRef::new(g, b"w");
        l.put(A, k, b("open"));
        l.set_timer(A, k, 60);
    }
    let one = l.barrier(1).await.unwrap();
    assert_eq!(sections(&one).len(), 2);
    for t in l.fire(A, 60, usize::MAX).await.unwrap() {
        l.delete(A, KeyRef::new(t.group, &t.key));
    }
    l.expire(A, 60);
    let two = l.barrier(2).await.unwrap();
    assert!(runs(&two[0]).unwrap().is_empty());
    assert_eq!(
        s.counts().barrier.count,
        1,
        "its deletes left nothing to write"
    );
    assert_eq!(named(&two[0]).unwrap().horizons.len(), 1);
    let r = stores(&objects, true);
    let mut back = lane(&r, 2, 3, (0, 7), &two).await;
    assert!(back.fire(A, i64::MAX, usize::MAX).await.unwrap().is_empty());
    assert_eq!(back.get(A, &[KeyRef::new(3, b"w")]).await.unwrap(), [None]);
    assert_eq!(r.counts(), Counts::default());
}

/// After a rescale, two lanes name one run's sections for their own key
/// groups, and each leaves its part out alone; the run stays named while
/// either names part of it.
#[tokio::test]
async fn lanes_after_a_rescale_leave_out_their_parts_of_one_run_alone() {
    let objects = bucket();
    let s = stores(&objects, true);
    let mut one = lane(&s, 1, 1, (0, 255), &[]).await;
    for g in 0..256u32 {
        let k = KeyRef::new(g, b"k");
        one.put(A, k, b(&format!("v{g}")));
        one.set_timer(A, k, i64::from(g));
    }
    let first = one.barrier(1).await.unwrap();
    let names = |snaps: &[Snapshot]| -> Vec<String> {
        let mut n: Vec<String> = snaps
            .iter()
            .flat_map(|s| runs(s).unwrap())
            .map(|r| r.name)
            .collect();
        n.sort();
        n.dedup();
        n
    };
    let s2 = stores(&objects, true);
    let mut lo = lane(&s2, 2, 2, (0, 127), &first).await;
    let mut hi = lane(&s2, 2, 2, (128, 255), &first).await;
    // The low lane overrides its keys and passes its timers.
    for g in 0..128u32 {
        lo.put(A, KeyRef::new(g, b"k"), b(&format!("w{g}")));
    }
    lo.fire(A, 300, usize::MAX).await.unwrap();
    lo.expire(A, 300);
    hi.put(A, KeyRef::new(200, b"other"), b("x"));
    let two = [lo.barrier(2).await.unwrap(), hi.barrier(2).await.unwrap()].concat();
    // The low lane learned run 1's part it reads, which newer keys hold.
    assert_eq!(
        sections(&two),
        [
            (1, SectionKind::Values, 128, 255),
            (1, SectionKind::Timers, 128, 255),
            (2, SectionKind::Values, 0, 127),
            (2, SectionKind::Values, 200, 200),
        ]
    );
    assert_eq!(
        names(&two),
        [
            "g1/e1/kg0-255.run",
            "g2/e2/kg0-127.run",
            "g2/e2/kg128-255.run"
        ]
    );
    // Restored onto one lane, each record reads as its lanes left it; the
    // timers left are the high lane's.
    let check = |snaps: Vec<Snapshot>, high: &'static str| {
        let objects = objects.clone();
        async move {
            let r = stores(&objects, true);
            let mut all = lane(&r, 9, 9, (0, 255), &snaps).await;
            for g in [0u32, 127, 128, 255] {
                let want = match g < 128 {
                    true => format!("w{g}"),
                    false => format!("{high}{g}"),
                };
                let got = all.get(A, &[KeyRef::new(g, b"k")]).await.unwrap();
                assert_eq!(got, [Some(b(&want))], "{g}");
            }
            let fired = all.fire(A, i64::MAX, usize::MAX).await.unwrap();
            fired.iter().map(|t| t.group).collect::<Vec<_>>()
        }
    };
    assert_eq!(
        check(two.clone(), "v").await,
        (128..256).collect::<Vec<_>>()
    );
    // The high lane does the same: run 1 is named no more.
    for g in 128..256u32 {
        hi.put(A, KeyRef::new(g, b"k"), b(&format!("w{g}")));
    }
    hi.fire(A, 300, usize::MAX).await.unwrap();
    hi.expire(A, 300);
    let three = [lo.barrier(3).await.unwrap(), hi.barrier(3).await.unwrap()].concat();
    assert_eq!(
        names(&three),
        [
            "g2/e2/kg0-127.run",
            "g2/e2/kg128-255.run",
            "g2/e3/kg128-255.run"
        ]
    );
    assert!(check(three, "w").await.is_empty());
}

/// A restored lane doesn't know what the values sections it reads hold,
/// so none of them goes, and no delete newer than them, until it reads
/// them whole, at its first barrier that writes a run: a read of each,
/// which counts as a restore's.
#[tokio::test]
async fn a_restored_lane_reads_its_values_sections_once_it_writes() {
    let objects = bucket();
    let s = stores(&objects, true);
    let mut l = lane(&s, 1, 1, (0, 0), &[]).await;
    for e in 1..=3u64 {
        l.put(A, KeyRef::new(0, format!("k{e}").as_bytes()), b("v"));
        l.put(A, KeyRef::new(0, format!("gone{e}").as_bytes()), b("v"));
        l.barrier(e).await.unwrap();
    }
    for e in 1..=3u64 {
        l.delete(A, KeyRef::new(0, format!("gone{e}").as_bytes()));
    }
    let record = l.barrier(4).await.unwrap();
    assert_eq!(sections(&record).len(), 4, "the deletes hide what's older");
    let r = stores(&objects, true);
    let mut back = lane(&r, 2, 5, (0, 0), &record).await;
    // A barrier that writes nothing reads nothing.
    let same = back.barrier(5).await.unwrap();
    assert_eq!(sections(&same), sections(&record));
    assert_eq!(r.counts(), Counts::default());
    for e in 1..=3u64 {
        back.put(A, KeyRef::new(0, format!("k{e}").as_bytes()), b("w"));
    }
    let six = back.barrier(6).await.unwrap();
    let c = r.counts();
    // Four runs opened and each section read whole; epoch 4's section of
    // deletes goes with those it hid.
    assert_eq!(c.restore.count, 8, "{c:?}");
    assert_eq!(c.lookup.count, 0, "{c:?}");
    assert_eq!(sections(&six), [(6, SectionKind::Values, 0, 0)]);
    let r = stores(&objects, true);
    let mut again = lane(&r, 3, 7, (0, 0), &six).await;
    for e in 1..=3u64 {
        let (k, gone) = (format!("k{e}"), format!("gone{e}"));
        let keys = [
            KeyRef::new(0, k.as_bytes()),
            KeyRef::new(0, gone.as_bytes()),
        ];
        assert_eq!(again.get(A, &keys).await.unwrap(), [Some(b("w")), None]);
    }
}

/// Lanes with horizons of their own restore onto one, which keeps each
/// group's: a section goes once the horizon of each group it's named for
/// has passed it.
#[tokio::test]
async fn horizons_stay_by_group_across_a_rescale() {
    let objects = bucket();
    let s = stores(&objects, true);
    let mut both = lane(&s, 1, 1, (0, 1), &[]).await;
    both.set_timer(A, KeyRef::new(0, b"x"), 10);
    both.set_timer(A, KeyRef::new(1, b"y"), 10);
    let first = both.barrier(1).await.unwrap();
    let s2 = stores(&objects, true);
    let mut zero = lane(&s2, 2, 2, (0, 0), &first).await;
    let mut one = lane(&s2, 2, 2, (1, 1), &first).await;
    zero.fire(A, 20, usize::MAX).await.unwrap();
    zero.expire(A, 20);
    one.expire(A, 5);
    let two = [
        zero.barrier(2).await.unwrap(),
        one.barrier(2).await.unwrap(),
    ]
    .concat();
    assert_eq!(sections(&two), [(1, SectionKind::Timers, 1, 1)]);
    let s3 = stores(&objects, true);
    let mut merged = lane(&s3, 3, 3, (0, 1), &two).await;
    assert_eq!(merged.barrier(3).await.unwrap().len(), 1);
    let fired = merged.fire(A, 15, usize::MAX).await.unwrap();
    assert_eq!(fired.iter().map(|t| t.group).collect::<Vec<_>>(), [1]);
    merged.expire(A, 15);
    assert!(sections(&merged.barrier(4).await.unwrap()).is_empty());
}

/// A reset restores none of an operator's snapshots, and with them goes its
/// horizon: the operator starts over with none, and what it sets at a time
/// the old horizon had passed counts.
#[tokio::test]
async fn a_reset_starts_with_no_horizon() {
    let objects = bucket();
    let s = stores(&objects, true);
    let mut l = lane(&s, 1, 1, (0, 0), &[]).await;
    let k = KeyRef::new(0, b"k");
    l.set_timer(A, k, 50);
    l.put(A, k, b("v"));
    l.expire(A, 40);
    let one = l.barrier(1).await.unwrap();
    assert_eq!(named(&one[0]).unwrap().horizons.len(), 1);
    let r = stores(&objects, true);
    let mut reset = lane(&r, 2, 2, (0, 0), &[]).await;
    reset.set_timer(A, k, 10);
    reset.append(A, k, 10, b("row"));
    assert_eq!(reset.rows(A, &[(k, 10)]).await.unwrap(), [vec![b("row")]]);
    let fired = reset.fire(A, 60, usize::MAX).await.unwrap();
    assert_eq!(fired.iter().map(|t| t.time).collect::<Vec<_>>(), [10]);
    assert_eq!(reset.get(A, &[k]).await.unwrap(), [None]);
    let two = reset.barrier(2).await.unwrap();
    assert!(named(&two[0]).unwrap().horizons.is_empty());
    assert_eq!(sections(&two), [(2, SectionKind::Rows, 0, 0)]);
}

/// Reading restored values sections whole holds no more at once than the
/// block cache's bound, however many blocks they have: each read's blocks
/// go once their keys are taken.
#[tokio::test]
async fn reading_restored_values_whole_stays_within_the_cache_bound() {
    let objects = bucket();
    let small = RunConfig {
        block_bytes: 256,
        ..RunConfig::default()
    };
    let s = RunStores::new(objects.clone(), small.clone());
    let mut l = lane(&s, 1, 1, (0, 15), &[]).await;
    let key = |i: u32| format!("key-{i:06}");
    for i in 0..20_000u32 {
        l.put(
            A,
            KeyRef::new(i % 16, key(i).as_bytes()),
            b(&format!("value {i}")),
        );
    }
    let record = l.barrier(1).await.unwrap();
    let written = runs(&record[0]).unwrap()[0].sections[0].raw_bytes;
    let bound = 16 << 10;
    assert!(written > 20 * bound, "{written} bytes of values");
    let r = RunStores::new(
        objects,
        RunConfig {
            cache_bytes: usize::try_from(bound).unwrap(),
            ..small
        },
    );
    let mut back = lane(&r, 2, 2, (0, 15), &record).await;
    for i in 0..20_000u32 {
        back.put(A, KeyRef::new(i % 16, key(i).as_bytes()), b("again"));
    }
    let two = back.barrier(2).await.unwrap();
    // The restored section, all of it overridden, goes.
    assert_eq!(sections(&two), [(2, SectionKind::Values, 0, 15)]);
    assert!(r.counts().restore.count > 20, "{:?}", r.counts());
    let peak = r.learned_peak();
    assert!(
        peak > 0 && peak <= bound,
        "held {peak} bytes at once, over {bound}"
    );
}
