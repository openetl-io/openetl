//! The store of runs on object storage: what a barrier writes, what a
//! restore and a rescale read, what reads cost, what a failed barrier
//! leaves, corruption, and a recorded run of each format.

#![allow(clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use openetl_state::runs::{Counts, RunName, named_runs, runs};
use openetl_state::{
    BoxFuture, Error, KeyGroupRange, KeyRef, LAYOUT, Layer, NEVER, ObjectStorage, Objects,
    OperatorId, Result, RunConfig, RunStores, SectionKind, Snapshot, StateBackend,
};

const OP: OperatorId = OperatorId(1);
const GROUPS: u32 = 256;

fn ranges(lanes: u32) -> Vec<KeyGroupRange> {
    (0..lanes)
        .map(|i| KeyGroupRange::new(GROUPS * i / lanes, GROUPS * (i + 1) / lanes - 1))
        .collect()
}

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

/// What a test can make storage do to a put.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Puts {
    Work = 0,
    /// Fail, writing nothing.
    Fail = 1,
    /// Write, then fail: the barrier never learns it was written.
    WriteThenFail = 2,
}

/// An in-memory bucket behind [`ObjectStorage`], whose puts a test can
/// fail, and whose objects it can read and change.
struct Bucket {
    memory: Arc<InMemory>,
    storage: ObjectStorage,
    puts: AtomicU8,
}

impl Bucket {
    fn new() -> Arc<Self> {
        let memory = Arc::new(InMemory::new());
        let storage = ObjectStorage::new(memory.clone(), Path::from("state"));
        Arc::new(Self {
            memory,
            storage,
            puts: AtomicU8::new(Puts::Work as u8),
        })
    }

    fn set_puts(&self, p: Puts) {
        self.puts.store(p as u8, Ordering::Relaxed);
    }

    async fn names(&self) -> BTreeSet<String> {
        let prefix = Path::from("state");
        let listed: Vec<_> = self.memory.list(Some(&prefix)).try_collect().await.unwrap();
        listed
            .into_iter()
            .map(|m| {
                m.location
                    .as_ref()
                    .strip_prefix("state/")
                    .unwrap()
                    .to_string()
            })
            .collect()
    }

    fn path(name: &str) -> Path {
        Path::from(format!("state/{name}"))
    }

    async fn read(&self, name: &str) -> Bytes {
        let got = self.memory.get(&Self::path(name)).await.unwrap();
        got.bytes().await.unwrap()
    }

    async fn write(&self, name: &str, data: Vec<u8>) {
        let path = Self::path(name);
        self.memory.put(&path, data.into()).await.unwrap();
    }
}

impl Objects for Bucket {
    fn put_new<'a>(&'a self, name: &'a str, data: Bytes) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            match self.puts.load(Ordering::Relaxed) {
                1 => Err(Error::Store("a put that failed".into())),
                2 => {
                    self.storage.put_new(name, data).await?;
                    Err(Error::Store("a put that wrote and failed".into()))
                }
                _ => self.storage.put_new(name, data).await,
            }
        })
    }

    fn get_range<'a>(&'a self, name: &'a str, range: Range<u64>) -> BoxFuture<'a, Result<Bytes>> {
        self.storage.get_range(name, range)
    }
}

fn stores(bucket: &Arc<Bucket>, config: RunConfig) -> RunStores {
    RunStores::new(bucket.clone(), config)
}

async fn open(
    stores: &RunStores,
    generation: u64,
    epoch: u64,
    lanes: u32,
    snapshots: &[Snapshot],
) -> Vec<Layer> {
    let mut out = Vec::new();
    for r in ranges(lanes) {
        let l = Layer::open(stores, generation, epoch, r, snapshots).await;
        out.push(l.unwrap());
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

fn lane(lanes: &mut [Layer], g: u32) -> &mut Layer {
    lanes.iter_mut().find(|l| l.range().contains(g)).unwrap()
}

/// Every kind of state in every group, written on 4 lanes over 2 epochs.
async fn write_every_kind(stores: &RunStores) -> Vec<Snapshot> {
    let mut ls = open(stores, 1, 1, 4, &[]).await;
    for g in 0..GROUPS {
        let l = lane(&mut ls, g);
        let k = KeyRef::new(g, b"k");
        l.put(OP, k, b(&format!("v{g}")));
        l.append(OP, k, i64::from(g), b(&format!("r{g}")));
        l.set_timer(OP, k, i64::from(g % 7));
        l.insert_new(OP, &[k], &[0], &[100]).await.unwrap();
    }
    barrier(&mut ls, 1).await;
    for g in (0..GROUPS).step_by(3) {
        lane(&mut ls, g).put(OP, KeyRef::new(g, b"k"), b(&format!("w{g}")));
    }
    barrier(&mut ls, 2).await
}

fn value(g: u32) -> Bytes {
    match g % 3 {
        0 => b(&format!("w{g}")),
        _ => b(&format!("v{g}")),
    }
}

/// What [`write_every_kind`] wrote, as `ls` reads it.
async fn check_every_kind(ls: &mut [Layer]) {
    for g in 0..GROUPS {
        let k = KeyRef::new(g, b"k");
        let l = lane(ls, g);
        assert_eq!(l.get(OP, &[k]).await.unwrap(), [Some(value(g))], "{g}");
        let rows = l.rows(OP, &[(k, i64::from(g))]).await.unwrap();
        assert_eq!(rows, [vec![b(&format!("r{g}"))]]);
        assert_eq!(l.insert_new(OP, &[k], &[1], &[100]).await.unwrap(), [false]);
    }
    let mut timers = 0;
    for l in ls.iter_mut() {
        let range = l.range();
        let fired = l.fire(OP, 6, usize::MAX).await.unwrap();
        assert!(fired.iter().all(|t| range.contains(t.group)));
        timers += fired.len();
        let due = l.take_rows(OP, i64::MAX, usize::MAX).await.unwrap();
        let groups: Vec<u32> = due.iter().map(|r| r.group).collect();
        assert_eq!(groups, (range.first..=range.last).collect::<Vec<_>>());
    }
    assert_eq!(timers, GROUPS as usize);
}

/// Written on 4 lanes, restored on 4, 3, 8 and 1: opening writes and
/// reads nothing, each lane opens a run in one read the first time it
/// reads it, and every value, row, timer and id is where its key group
/// went.
#[tokio::test]
async fn a_rescale_reads_by_key_group_and_writes_nothing() {
    let bucket = Bucket::new();
    let written = write_every_kind(&stores(&bucket, RunConfig::default())).await;
    assert_eq!(written.len(), 4);
    let objects = bucket.names().await.len();
    assert_eq!(objects, 8);
    for (generation, n) in [(2, 4), (3, 3), (4, 8), (5, 1)] {
        let s = stores(&bucket, RunConfig::default());
        let mut ls = open(&s, generation, 3, n, &written).await;
        assert_eq!(s.counts(), Counts::default());
        check_every_kind(&mut ls).await;
        let c = s.counts();
        assert_eq!(c.barrier.count, 0);
        // Each lane opens each run of the record that holds its groups.
        let opened: u64 = ranges(n)
            .iter()
            .map(|r| 2 * ranges(4).iter().filter(|w| w.overlaps(r)).count() as u64)
            .sum();
        assert_eq!(c.restore.count, opened, "{n} lanes");
        assert_eq!(bucket.names().await.len(), objects);
    }
}

/// A lane that owns part of a run keeps only its groups' filters, so the
/// filters of 8 lanes over 4 lanes' runs come to about what 4 lanes keep.
#[tokio::test]
async fn a_lane_keeps_only_its_groups_filters() {
    let bucket = Bucket::new();
    let s = stores(&bucket, RunConfig::default());
    let mut ls = open(&s, 1, 1, 4, &[]).await;
    for g in 0..GROUPS {
        let ids: Vec<Vec<u8>> = (0..200u32).map(|i| i.to_be_bytes().to_vec()).collect();
        let keys: Vec<KeyRef> = ids.iter().map(|id| KeyRef::new(g, id)).collect();
        let n = keys.len();
        let l = lane(&mut ls, g);
        l.insert_new(OP, &keys, &vec![0; n], &vec![NEVER; n])
            .await
            .unwrap();
    }
    let record = barrier(&mut ls, 1).await;
    let mut kept = Vec::new();
    for (generation, n) in [(2, 4), (3, 8)] {
        let s = stores(&bucket, RunConfig::default());
        let mut ls = open(&s, generation, 2, n, &record).await;
        for l in &mut ls {
            let g = l.range().first;
            l.insert_new(OP, &[KeyRef::new(g, b"new")], &[0], &[NEVER])
                .await
                .unwrap();
        }
        kept.push(ls.iter().map(|l| l.memory().store).sum::<usize>());
    }
    assert!(kept[0] > 100_000, "{kept:?}");
    assert!(kept[1] < kept[0] * 11 / 10, "{kept:?}");
}

/// A dedupe of new ids: the set's filters keep nearly every lookup from
/// storage, and a repeat reads the one block that holds it.
#[tokio::test]
async fn new_ids_rarely_reach_storage() {
    let bucket = Bucket::new();
    let s = stores(&bucket, RunConfig::default());
    let range = KeyGroupRange::new(0, 15);
    let mut l = Layer::open(&s, 1, 1, range, &[]).await.unwrap();
    let id = |i: u32| (i % 16, i.to_be_bytes());
    let mut record = Vec::new();
    for epoch in 1..=4u32 {
        let ids: Vec<(u32, [u8; 4])> = (epoch * 20_000..(epoch + 1) * 20_000).map(id).collect();
        let keys: Vec<KeyRef> = ids.iter().map(|(g, k)| KeyRef::new(*g, k)).collect();
        let n = keys.len();
        let fresh = l
            .insert_new(OP, &keys, &vec![0; n], &vec![NEVER; n])
            .await
            .unwrap();
        assert!(fresh.iter().all(|f| *f));
        record = l.barrier(u64::from(epoch)).await.unwrap();
    }
    let s = stores(&bucket, RunConfig::default());
    let mut l = Layer::open(&s, 2, 5, range, &record).await.unwrap();
    let ids: Vec<(u32, [u8; 4])> = (1_000_000..1_050_000).map(id).collect();
    let keys: Vec<KeyRef> = ids.iter().map(|(g, k)| KeyRef::new(*g, k)).collect();
    let n = keys.len() as u64;
    let fresh = l
        .insert_new(OP, &keys, &vec![0; keys.len()], &vec![NEVER; keys.len()])
        .await
        .unwrap();
    assert!(fresh.iter().all(|f| *f));
    let c = s.counts();
    assert_eq!(c.hits, 0);
    // 4 sections at 20 bits a key: about 0.03% of lookups go past one.
    assert!(c.false_positives < n / 500, "{c:?}");
    assert_eq!(c.restore.count, 4);
    assert!(c.lookup.count <= c.false_positives, "{c:?}");
    let before = s.counts();
    let (g, k) = id(30_000);
    let repeat = [KeyRef::new(g, &k)];
    assert_eq!(
        l.insert_new(OP, &repeat, &[0], &[NEVER]).await.unwrap(),
        [false]
    );
    let c = s.counts() - before;
    assert_eq!((c.hits, c.lookup.count), (1, 1), "{c:?}");
}

/// The blocks a batch of lookups needs from a run are read together where
/// they're near each other, and apart where they aren't.
#[tokio::test]
async fn a_batch_reads_neighbouring_blocks_at_once() {
    let bucket = Bucket::new();
    let config = RunConfig {
        block_bytes: 64,
        ..Default::default()
    };
    let s = stores(&bucket, config.clone());
    let range = KeyGroupRange::new(0, 0);
    let mut l = Layer::open(&s, 1, 1, range, &[]).await.unwrap();
    let key = |i: u32| format!("key-{i:06}");
    for i in 0..2_000 {
        l.put(OP, KeyRef::new(0, key(i).as_bytes()), b(&format!("{i}")));
    }
    let record = l.barrier(1).await.unwrap();
    let batch: Vec<String> = (100..140).chain(1_500..1_510).map(key).collect();
    let keys: Vec<KeyRef> = batch.iter().map(|k| KeyRef::new(0, k.as_bytes())).collect();
    let want: Vec<Option<Bytes>> = (100..140)
        .chain(1_500..1_510)
        .map(|i| Some(b(&format!("{i}"))))
        .collect();
    for (gap, reads) in [(0, 2), (1 << 20, 1)] {
        let s = stores(
            &bucket,
            RunConfig {
                gap_bytes: gap,
                ..config.clone()
            },
        );
        let mut l = Layer::open(&s, 2, 2, range, &record).await.unwrap();
        assert_eq!(l.get(OP, &keys).await.unwrap(), want);
        let c = s.counts();
        assert_eq!(c.lookup.count, reads, "gap {gap}: {c:?}");
        assert!(c.hits >= 50, "{c:?}");
        // Read once, the blocks are cached.
        assert_eq!(l.get(OP, &keys).await.unwrap(), want);
        assert_eq!(s.counts().lookup.count, reads);
    }
}

/// A barrier with nothing to change writes nothing, and nor does a delete
/// of a key no run can hold, or one that leaves nothing live: the record
/// then names no run.
#[tokio::test]
async fn a_barrier_writes_only_what_changed() {
    let bucket = Bucket::new();
    let s = stores(&bucket, RunConfig::default());
    let mut l = Layer::open(&s, 1, 1, KeyGroupRange::new(0, 3), &[])
        .await
        .unwrap();
    let k = KeyRef::new(1, b"k");
    l.put(OP, k, b("one"));
    let first = l.barrier(1).await.unwrap();
    assert_eq!(s.counts().barrier.count, 1);
    assert_eq!(l.barrier(2).await.unwrap(), first);
    // Appended and taken, set and fired, in one epoch: nothing to keep.
    l.append(OP, k, 3, b("row"));
    l.set_timer(OP, k, 3);
    assert_eq!(l.take_rows(OP, 5, 9).await.unwrap().len(), 1);
    assert_eq!(l.fire(OP, 5, 9).await.unwrap().len(), 1);
    l.delete(OP, KeyRef::new(2, b"never"));
    assert_eq!(l.barrier(3).await.unwrap(), first);
    assert_eq!(s.counts().barrier.count, 1);
    l.delete(OP, k);
    let second = l.barrier(4).await.unwrap();
    assert_ne!(second, first);
    assert!(runs(&second[0]).unwrap().is_empty());
    assert_eq!(s.counts().barrier.count, 1);
    let mut r = Layer::open(&s, 2, 5, KeyGroupRange::new(0, 3), &second)
        .await
        .unwrap();
    assert_eq!(r.get(OP, &[k]).await.unwrap(), [None]);
}

/// A snapshot names each section by its run, with the key groups it counts
/// for and the times it holds; the runs no kept record names are what
/// retention may delete.
#[tokio::test]
async fn snapshots_name_sections_by_run() {
    let bucket = Bucket::new();
    let s = stores(&bucket, RunConfig::default());
    let mut ls = open(&s, 7, 1, 2, &[]).await;
    let k = KeyRef::new(3, b"k");
    ls[0].put(OP, k, b("v"));
    ls[0].append(OP, k, -5, b("row"));
    ls[0].append(OP, k, 9, b("row"));
    ls[0].set_timer(OP, k, 4);
    ls[0].insert_new(OP, &[k], &[0], &[50]).await.unwrap();
    ls[1].put(OP, KeyRef::new(200, b"k"), b("v"));
    let one = barrier(&mut ls, 1).await;
    ls[0].put(OP, k, b("w"));
    let two = barrier(&mut ls, 2).await;
    let first = runs(&one[0]).unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].name, "g7/e1/kg0-127.run");
    let kinds: Vec<(SectionKind, Option<(i64, i64)>)> = first[0]
        .sections
        .iter()
        .map(|s| (s.kind, s.times))
        .collect();
    assert_eq!(
        kinds,
        [
            (SectionKind::Values, None),
            (SectionKind::Rows, Some((-5, 9))),
            (SectionKind::Set, Some((50, 50))),
            (SectionKind::Timers, Some((4, 4))),
        ]
    );
    assert!(
        first[0]
            .sections
            .iter()
            .all(|s| s.groups == KeyGroupRange::new(3, 3) && s.epoch == 1)
    );
    let second = runs(&two[0]).unwrap();
    assert_eq!(
        second.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
        ["g7/e2/kg0-127.run", "g7/e1/kg0-127.run"]
    );
    let named = named_runs(&two).unwrap();
    let all = bucket.names().await;
    assert_eq!(named, all);
    let parsed: Vec<RunName> = all.iter().filter_map(|n| RunName::parse(n)).collect();
    assert_eq!(parsed.len(), 3);
    assert!(named_runs(&one).unwrap().len() < all.len());
}

/// A barrier that fails, one written but never recorded, and an older
/// attempt that goes on writing leave only runs no record names; a restore
/// from the last record reads what it named, and nothing else.
#[tokio::test]
async fn a_failed_barrier_leaves_only_orphans() {
    let bucket = Bucket::new();
    let s = stores(&bucket, RunConfig::default());
    let mut ls = open(&s, 1, 1, 2, &[]).await;
    let (a, z) = (KeyRef::new(5, b"a"), KeyRef::new(250, b"z"));
    lane(&mut ls, 5).put(OP, a, b("one"));
    lane(&mut ls, 250).put(OP, z, b("one"));
    let record = barrier(&mut ls, 1).await;
    let kept = named_runs(&record).unwrap();
    // Epoch 2: the first lane's run is written and the barrier told it
    // failed; the second's fails outright.
    for l in &mut ls {
        let k = KeyRef::new(l.range().first, b"a");
        l.put(OP, k, b("two"));
        l.append(OP, k, 1, b("two"));
    }
    bucket.set_puts(Puts::WriteThenFail);
    assert!(ls[0].barrier(2).await.is_err());
    bucket.set_puts(Puts::Fail);
    assert!(ls[1].barrier(2).await.is_err());
    bucket.set_puts(Puts::Work);
    assert!(ls[0].get(OP, &[a]).await.is_err());
    // The next attempt restores record 1, while the first goes on in a
    // lane of its own.
    let mut old = Layer::open(&s, 1, 2, ranges(2)[1], &record).await.unwrap();
    let mut ls = open(&s, 2, 2, 3, &record).await;
    old.put(OP, z, b("zombie"));
    old.barrier(2).await.unwrap();
    lane(&mut ls, 5).put(OP, KeyRef::new(6, b"b"), b("two"));
    let next = barrier(&mut ls, 2).await;
    let named = named_runs(&next).unwrap();
    assert!(kept.is_subset(&named));
    let all = bucket.names().await;
    let orphans: Vec<&String> = all.iter().filter(|n| !named.contains(*n)).collect();
    assert_eq!(orphans, ["g1/e2/kg0-127.run", "g1/e2/kg128-255.run"]);
    // What record 1 names, read anew, is as it was.
    for record in [&record, &next] {
        let s = stores(&bucket, RunConfig::default());
        let mut ls = open(&s, 3, 3, 1, record).await;
        let one = Some(b("one"));
        assert_eq!(ls[0].get(OP, &[a, z]).await.unwrap(), [one.clone(), one]);
        assert!(ls[0].rows(OP, &[(a, 1)]).await.unwrap()[0].is_empty());
    }
    assert!(named.contains("g2/e2/kg0-84.run"));
}

#[tokio::test]
async fn a_run_is_never_written_twice() {
    let bucket = Bucket::new();
    let s = stores(&bucket, RunConfig::default());
    let range = KeyGroupRange::new(0, 9);
    let mut a = Layer::open(&s, 4, 1, range, &[]).await.unwrap();
    let mut b2 = Layer::open(&s, 4, 1, range, &[]).await.unwrap();
    a.put(OP, KeyRef::new(1, b"k"), b("a"));
    b2.put(OP, KeyRef::new(1, b"k"), b("b"));
    let record = a.barrier(1).await.unwrap();
    assert!(matches!(b2.barrier(1).await, Err(Error::Store(_))));
    let mut r = Layer::open(&s, 5, 2, range, &record).await.unwrap();
    assert_eq!(
        r.get(OP, &[KeyRef::new(1, b"k")]).await.unwrap(),
        [Some(b("a"))]
    );
}

/// Changed bytes anywhere in a run, a run cut short, or a snapshot that
/// doesn't match its run: a read fails as the run's, unreadable, never
/// with wrong state.
#[tokio::test]
async fn corruption_is_caught() {
    let bucket = Bucket::new();
    let s = stores(&bucket, RunConfig::default());
    let range = KeyGroupRange::new(0, 0);
    let mut l = Layer::open(&s, 1, 1, range, &[]).await.unwrap();
    for i in 0..300 {
        l.put(OP, KeyRef::new(0, format!("{i:04}").as_bytes()), b("value"));
    }
    let record = l.barrier(1).await.unwrap();
    let name = "g1/e1/kg0-0.run";
    let good = bucket.read(name).await;
    let read_all = |record: Vec<Snapshot>| {
        let bucket = bucket.clone();
        async move {
            let s = stores(&bucket, RunConfig::default());
            let mut l = Layer::open(&s, 2, 2, range, &record).await?;
            let keys: Vec<String> = (0..300).map(|i| format!("{i:04}")).collect();
            let keys: Vec<KeyRef> = keys.iter().map(|k| KeyRef::new(0, k.as_bytes())).collect();
            l.get(OP, &keys).await
        }
    };
    assert!(read_all(record.clone()).await.is_ok());
    let meta = runs(&record[0]).unwrap()[0].sections[0].meta;
    let places = [
        20,
        good.len() / 4,
        usize::try_from(meta.0).unwrap() + 3,
        good.len() - 30,
        good.len() - 1,
    ];
    for at in places {
        let mut bad = good.to_vec();
        bad[at] ^= 0x10;
        bucket.write(name, bad).await;
        let got = read_all(record.clone()).await;
        let named = matches!(&got, Err(Error::Unreadable { run, .. }) if run == name);
        assert!(named, "byte {at}: {got:?}");
    }
    bucket.write(name, good[..good.len() - 8].to_vec()).await;
    assert!(read_all(record.clone()).await.is_err());
    bucket.write(name, good.to_vec()).await;
    assert!(read_all(record.clone()).await.is_ok());
    let mut runs = runs(&record[0]).unwrap();
    runs[0].sections[0].entries += 1;
    let lying = vec![Snapshot {
        data: openetl_state::runs::encode(&runs),
        ..record[0].clone()
    }];
    let got = read_all(lying).await;
    assert!(matches!(got, Err(Error::Unreadable { .. })), "{got:?}");
}

/// What a restore refuses: snapshots that overlap or leave a group out,
/// another store's, another format's.
#[tokio::test]
async fn a_restore_refuses_what_it_cant_read() {
    let bucket = Bucket::new();
    let s = stores(&bucket, RunConfig::default());
    let mut ls = open(&s, 1, 1, 4, &[]).await;
    for l in &mut ls {
        l.put(OP, KeyRef::new(l.range().first, b"k"), b("v"));
    }
    let all = barrier(&mut ls, 1).await;
    let range = KeyGroupRange::new(0, GROUPS - 1);
    let open = |snapshots: Vec<Snapshot>| {
        let s = s.clone();
        async move { Layer::open(&s, 2, 2, range, &snapshots).await.err() }
    };
    assert!(open(all.clone()).await.is_none());
    assert!(matches!(
        open(all[..3].to_vec()).await,
        Some(Error::Snapshot(_))
    ));
    let mut twice = all.clone();
    twice.push(all[0].clone());
    assert!(matches!(open(twice).await, Some(Error::Snapshot(_))));
    let mut memory = all.clone();
    memory[1].data = b("g1/e1/kg64-127");
    assert!(matches!(open(memory).await, Some(Error::Snapshot(_))));
    let mut runs = runs(&all[2]).unwrap();
    runs[0].format = 2;
    let mut newer = all.clone();
    newer[2].data = openetl_state::runs::encode(&runs);
    assert!(matches!(open(newer).await, Some(Error::Snapshot(_))));
    let mut layout = all.clone();
    layout[0].layout = LAYOUT + 1;
    assert!(matches!(open(layout).await, Some(Error::Snapshot(_))));
    // A run's sections hold its epoch's changes of its key groups, and its
    // name says which.
    let lies: [fn(&mut openetl_state::runs::RunRef); 4] = [
        |r| r.sections[0].epoch = 2,
        |r| r.sections[0].groups = KeyGroupRange::new(0, 64),
        |r| r.name = "g1/e2/kg0-63.run".into(),
        |r| r.name = "state.run".into(),
    ];
    for lie in lies {
        let mut named = openetl_state::runs::runs(&all[0]).unwrap();
        lie(&mut named[0]);
        let mut lying = all.clone();
        lying[0].data = openetl_state::runs::encode(&named);
        assert!(matches!(open(lying).await, Some(Error::Snapshot(_))));
    }
}

/// A row put in place of another reads back by its key, from the open
/// epoch and from a restore's runs, newest first, until a horizon passes
/// its time.
#[tokio::test]
async fn a_put_row_reads_back_by_its_key() {
    let s = stores(&Bucket::new(), RunConfig::default());
    let mut ls = open(&s, 1, 1, 1, &[]).await;
    let k = KeyRef::new(7, b"id");
    ls[0].put_row(OP, k, 10, b("a"));
    ls[0].put_row(OP, k, 10, b("b"));
    ls[0].put_row(OP, k, 20, b("c"));
    let slots = [(k, 10), (k, 20), (k, 30), (KeyRef::new(8, b"id"), 10)];
    let want = vec![Some(b("b")), Some(b("c")), None, None];
    assert_eq!(ls[0].row(OP, &slots).await.unwrap(), want);
    let all = barrier(&mut ls, 1).await;
    let mut ls = open(&s, 2, 2, 1, &all).await;
    assert_eq!(ls[0].row(OP, &slots).await.unwrap(), want);
    // Put again in a later epoch, the slot's row is in two runs, and the
    // newer one's reads.
    ls[0].put_row(OP, k, 10, b("d"));
    let all = barrier(&mut ls, 2).await;
    let mut ls = open(&s, 3, 3, 1, &all).await;
    assert_eq!(
        ls[0].row(OP, &slots).await.unwrap(),
        vec![Some(b("d")), Some(b("c")), None, None]
    );
    ls[0].expire_in(OP, 7, 10);
    assert_eq!(
        ls[0].row(OP, &slots[..2]).await.unwrap(),
        vec![None, Some(b("c"))]
    );
}

/// A store forgets a run its snapshots name only before it reads from it,
/// as if it held nothing: a read could have found what one after it
/// wouldn't.
#[tokio::test]
async fn a_run_is_forgotten_only_before_its_read() {
    let s = stores(&Bucket::new(), RunConfig::default());
    let mut ls = open(&s, 1, 1, 1, &[]).await;
    let k = KeyRef::new(7, b"id");
    ls[0].put_row(OP, k, 10, b("a"));
    let all = barrier(&mut ls, 1).await;
    let name = runs(&all[0]).unwrap()[0].name.clone();
    let mut ls = open(&s, 2, 2, 1, &all).await;
    assert!(ls[0].forget(&name));
    assert_eq!(ls[0].row(OP, &[(k, 10)]).await.unwrap(), vec![None]);
    assert!(runs(&barrier(&mut ls, 2).await[0]).unwrap().is_empty());
    let mut ls = open(&s, 3, 2, 1, &all).await;
    assert_eq!(ls[0].row(OP, &[(k, 10)]).await.unwrap(), vec![Some(b("a"))]);
    assert!(!ls[0].forget(&name), "a run read from");
}

/// An object store that answers its next `misses` reads, looks among
/// them, with not found, whatever it holds, as some do in an outage.
#[derive(Debug)]
struct Missing {
    inner: InMemory,
    misses: std::sync::atomic::AtomicU64,
}

impl std::fmt::Display for Missing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Missing({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for Missing {
    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let missed = self
            .misses
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .is_ok();
        match missed {
            true => Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: "missing".into(),
            }),
            false => self.inner.get_opts(location, options).await,
        }
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A run read as missing is gone only once looks after a wait find it
/// missing too: a store that answers not found once and then serves it
/// gives what it holds, and one that goes on answering not found gives
/// [`Error::Gone`], which a lane may forget.
#[tokio::test]
async fn a_run_is_gone_only_once_it_stays_missing() {
    let store = Arc::new(Missing {
        inner: InMemory::new(),
        misses: Default::default(),
    });
    let s = RunStores::new(
        Arc::new(ObjectStorage::new(store.clone(), Path::from("state"))),
        RunConfig::default(),
    );
    let mut ls = open(&s, 1, 1, 1, &[]).await;
    let k = KeyRef::new(7, b"id");
    ls[0].put_row(OP, k, 10, b("a"));
    let all = barrier(&mut ls, 1).await;
    let name = runs(&all[0]).unwrap()[0].name.clone();

    store.misses.store(1, Ordering::Relaxed);
    let mut ls = open(&s, 2, 2, 1, &all).await;
    assert_eq!(ls[0].row(OP, &[(k, 10)]).await.unwrap(), vec![Some(b("a"))]);
    assert_eq!(store.misses.load(Ordering::Relaxed), 0);
    assert_eq!(runs(&barrier(&mut ls, 2).await[0]).unwrap()[0].name, name);

    store.misses.store(u64::MAX, Ordering::Relaxed);
    let mut ls = open(&s, 3, 2, 1, &all).await;
    let started = std::time::Instant::now();
    match ls[0].row(OP, &[(k, 10)]).await {
        Err(Error::Gone(run)) => assert_eq!(run, name),
        got => panic!("{got:?}"),
    }
    assert!(started.elapsed() >= std::time::Duration::from_millis(1200));
    // A read and a look after each of the two waits.
    assert_eq!(u64::MAX - store.misses.load(Ordering::Relaxed), 3);
    assert!(ls[0].forget(&name));
    assert_eq!(ls[0].row(OP, &[(k, 10)]).await.unwrap(), vec![None]);
}

/// Where the recorded runs of each format are, under this crate.
const RECORDED: &str = "tests/testdata/runs-v1";

/// What the recorded runs hold: two lanes over 16 key groups and three
/// epochs, recorded after the third. Changing it changes nothing recorded:
/// [`recorded_runs_restore`] checks what was.
async fn record_script(stores: &RunStores) -> Vec<Snapshot> {
    let ops = [OperatorId(1), OperatorId(9)];
    let mut ls = Vec::new();
    for r in [KeyGroupRange::new(0, 7), KeyGroupRange::new(8, 15)] {
        ls.push(Layer::open(stores, 1, 1, r, &[]).await.unwrap());
    }
    for g in 0..16u32 {
        let l = lane(&mut ls, g);
        for (i, op) in ops.into_iter().enumerate() {
            let k = format!("key-{g}-{i}");
            let k = KeyRef::new(g, k.as_bytes());
            l.put(op, k, b(&format!("value-{g}-{i}")));
            l.append(op, k, i64::from(g) * 10, b(&format!("row-{g}-{i}-a")));
            l.append(op, k, i64::from(g) * 10, b(&format!("row-{g}-{i}-b")));
            l.set_timer(op, k, i64::from(g) - 8);
            let id = format!("id-{g}-{i}");
            let expires = if g % 2 == 0 {
                NEVER
            } else {
                1_000 + i64::from(g)
            };
            l.insert_new(op, &[KeyRef::new(g, id.as_bytes())], &[0], &[expires])
                .await
                .unwrap();
        }
    }
    // Values that don't compress, each in a block of its own once the
    // first ends one, so some blocks are stored raw.
    for n in [b"noise-0", b"noise-1", b"noise-2"] {
        lane(&mut ls, 5).put(ops[0], KeyRef::new(5, n), noise());
    }
    for l in &mut ls {
        l.barrier(1).await.unwrap();
    }
    for g in (0..16u32).step_by(4) {
        let l = lane(&mut ls, g);
        let k = format!("key-{g}-0");
        l.delete(ops[0], KeyRef::new(g, k.as_bytes()));
        let k = format!("key-{g}-1");
        l.put(
            ops[1],
            KeyRef::new(g, k.as_bytes()),
            b(&format!("again-{g}")),
        );
    }
    for l in &mut ls {
        l.fire(ops[0], -5, usize::MAX).await.unwrap();
        l.take_rows(ops[1], 30, usize::MAX).await.unwrap();
        l.barrier(2).await.unwrap();
    }
    let mut record = Vec::new();
    for l in &mut ls {
        l.expire(ops[0], 1_005);
        record.extend(l.barrier(3).await.unwrap());
    }
    record
}

/// 400 bytes of xorshift.
fn noise() -> Bytes {
    let mut x = 0x9e37_79b9_7f4a_7c15u64;
    let mut out = Vec::new();
    for _ in 0..50 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    Bytes::from(out)
}

/// Restores the recorded runs onto 3 lanes and reads every kind of state.
async fn check_recorded(stores: &RunStores, record: &[Snapshot]) {
    let ops = [OperatorId(1), OperatorId(9)];
    let mut ls = Vec::new();
    for r in [(0, 4), (5, 11), (12, 15)] {
        let r = KeyGroupRange::new(r.0, r.1);
        ls.push(Layer::open(stores, 2, 4, r, record).await.unwrap());
    }
    for g in 0..16u32 {
        let l = lane(&mut ls, g);
        for (i, op) in ops.into_iter().enumerate() {
            let k = format!("key-{g}-{i}");
            let k = KeyRef::new(g, k.as_bytes());
            let want = match (g % 4, i) {
                (0, 0) => None,
                (0, _) => Some(b(&format!("again-{g}"))),
                _ => Some(b(&format!("value-{g}-{i}"))),
            };
            assert_eq!(l.get(op, &[k]).await.unwrap(), [want], "{g} {i}");
            let rows = l.rows(op, &[(k, i64::from(g) * 10)]).await.unwrap();
            let want = match i == 1 && g <= 3 {
                true => vec![],
                false => vec![b(&format!("row-{g}-{i}-a")), b(&format!("row-{g}-{i}-b"))],
            };
            assert_eq!(rows, [want], "{g} {i}");
            let id = format!("id-{g}-{i}");
            let id = KeyRef::new(g, id.as_bytes());
            let fresh = l.insert_new(op, &[id], &[10], &[NEVER]).await.unwrap()[0];
            let expired = i == 0 && g % 2 == 1 && 1_000 + i64::from(g) <= 1_005;
            assert_eq!(fresh, expired, "{g} {i}");
        }
    }
    let noisy = [b"noise-0", b"noise-1", b"noise-2"].map(|n| KeyRef::new(5, n));
    let noisy = lane(&mut ls, 5).get(ops[0], &noisy).await;
    assert_eq!(noisy.unwrap(), vec![Some(noise()); 3]);
    let mut fired = Vec::new();
    for l in &mut ls {
        for op in ops {
            let got = l.fire(op, i64::MAX, usize::MAX).await.unwrap();
            fired.extend(got.into_iter().map(|t| (op.0, t.group, t.time)));
        }
    }
    fired.sort();
    let mut want: Vec<(u32, u32, i64)> = (0..16u32)
        .flat_map(|g| [(1, g, i64::from(g) - 8), (9, g, i64::from(g) - 8)])
        .filter(|(op, g, _)| !(*op == 1 && *g <= 3))
        .collect();
    want.sort();
    assert_eq!(fired, want);
}

fn recorded(path: &str) -> std::path::PathBuf {
    recorded_in(RECORDED, path)
}

fn recorded_in(folder: &str, path: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(folder)
        .join(path)
}

/// Where runs of format 1 named by snapshots of version 2 are recorded:
/// with horizons, each section's operators, and dead sections left out.
const RECORDED_V2: &str = "tests/testdata/snapshots-v2";

/// What the snapshots of version 2 hold: two lanes over 16 key groups and
/// three epochs, recorded after the third, once operator 1's horizon is
/// at 100 and operator 9's at 120. Changing it changes nothing recorded.
async fn record_dropping(stores: &RunStores) -> Vec<Snapshot> {
    let ops = [OperatorId(1), OperatorId(9)];
    let mut ls = Vec::new();
    for r in [KeyGroupRange::new(0, 7), KeyGroupRange::new(8, 15)] {
        ls.push(Layer::open(stores, 1, 1, r, &[]).await.unwrap());
    }
    for g in 0..16u32 {
        let l = lane(&mut ls, g);
        let (k1, k9) = (format!("key-{g}-1"), format!("key-{g}-9"));
        let (k1, k9) = (KeyRef::new(g, k1.as_bytes()), KeyRef::new(g, k9.as_bytes()));
        l.put(ops[0], k1, b(&format!("value-{g}-1")));
        l.append(ops[0], k1, i64::from(g) * 10, b(&format!("row-{g}")));
        l.set_timer(ops[0], k1, i64::from(g));
        l.put(ops[1], k9, b(&format!("value-{g}-9")));
        l.set_timer(ops[1], k9, 50 + i64::from(g));
        let id = format!("id-{g}");
        let expires = if g % 2 == 0 {
            NEVER
        } else {
            100 + i64::from(g)
        };
        l.insert_new(ops[1], &[KeyRef::new(g, id.as_bytes())], &[0], &[expires])
            .await
            .unwrap();
    }
    for l in &mut ls {
        l.barrier(1).await.unwrap();
    }
    for g in 0..16u32 {
        let k = format!("key-{g}-1");
        lane(&mut ls, g).put(
            ops[0],
            KeyRef::new(g, k.as_bytes()),
            b(&format!("again-{g}")),
        );
    }
    for l in &mut ls {
        l.fire(ops[0], 100, usize::MAX).await.unwrap();
        l.take_rows(ops[0], 100, usize::MAX).await.unwrap();
        l.expire(ops[0], 100);
        l.barrier(2).await.unwrap();
    }
    for g in (0..16u32).step_by(4) {
        let k = format!("key-{g}-9");
        lane(&mut ls, g).delete(ops[1], KeyRef::new(g, k.as_bytes()));
    }
    let mut record = Vec::new();
    for l in &mut ls {
        l.fire(ops[1], 200, usize::MAX).await.unwrap();
        l.expire(ops[1], 120);
        record.extend(l.barrier(3).await.unwrap());
    }
    record
}

/// Restores what [`record_dropping`] wrote onto 3 lanes and reads every
/// kind of state.
async fn check_dropping(stores: &RunStores, record: &[Snapshot]) {
    let ops = [OperatorId(1), OperatorId(9)];
    let mut rows = Vec::new();
    for s in record {
        assert_eq!(s.data[1], 2, "a snapshot of version 2");
        let named = openetl_state::runs::named(s).unwrap();
        assert_eq!(named.horizons.len(), 2, "{named:?}");
        let sections: Vec<&openetl_state::runs::SectionRef> =
            named.runs.iter().flat_map(|r| &r.sections).collect();
        assert!(sections.iter().all(|s| s.operators.is_some()));
        // Epoch 1's timers went everywhere, and its rows where the
        // horizon passed them all.
        assert!(!sections.iter().any(|s| s.kind == SectionKind::Timers));
        rows.extend(
            sections
                .iter()
                .filter(|s| s.kind == SectionKind::Rows)
                .map(|s| (s.groups.first, s.times)),
        );
    }
    assert_eq!(rows, [(8, Some((80, 150)))], "the rows straddling 100");
    let mut ls = Vec::new();
    for r in [(0, 4), (5, 11), (12, 15)] {
        let r = KeyGroupRange::new(r.0, r.1);
        ls.push(Layer::open(stores, 2, 4, r, record).await.unwrap());
    }
    for g in 0..16u32 {
        let l = lane(&mut ls, g);
        let (k1, k9) = (format!("key-{g}-1"), format!("key-{g}-9"));
        let (k1, k9) = (KeyRef::new(g, k1.as_bytes()), KeyRef::new(g, k9.as_bytes()));
        assert_eq!(
            l.get(ops[0], &[k1]).await.unwrap(),
            [Some(b(&format!("again-{g}")))]
        );
        let want = (g % 4 != 0).then(|| b(&format!("value-{g}-9")));
        assert_eq!(l.get(ops[1], &[k9]).await.unwrap(), [want], "{g}");
        let rows = l.rows(ops[0], &[(k1, i64::from(g) * 10)]).await.unwrap();
        let want = match g * 10 <= 100 {
            true => vec![],
            false => vec![b(&format!("row-{g}"))],
        };
        assert_eq!(rows, [want], "{g}");
        let id = format!("id-{g}");
        let fresh = l
            .insert_new(ops[1], &[KeyRef::new(g, id.as_bytes())], &[10], &[NEVER])
            .await
            .unwrap();
        assert_eq!(fresh, [g % 2 == 1], "{g}");
    }
    for l in &mut ls {
        for op in ops {
            assert!(l.fire(op, i64::MAX, usize::MAX).await.unwrap().is_empty());
        }
        let due = l.take_rows(ops[0], i64::MAX, usize::MAX).await.unwrap();
        let range = l.range();
        let want: Vec<u32> = (11..16).filter(|g| range.contains(*g)).collect();
        assert_eq!(due.iter().map(|r| r.group).collect::<Vec<_>>(), want);
    }
}

/// Runs of format 1 named by snapshots of version 2, recorded when it was
/// made, restore.
#[tokio::test]
async fn recorded_snapshots_of_version_2_restore() {
    if recording(RECORDED_V2) {
        let bucket = Bucket::new();
        let config = RunConfig {
            block_bytes: 160,
            ..Default::default()
        };
        let record = record_dropping(&stores(&bucket, config)).await;
        check_dropping(&stores(&bucket, RunConfig::default()), &record).await;
        for name in named_runs(&record).unwrap() {
            let path = recorded_in(RECORDED_V2, &name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, bucket.read(&name).await).unwrap();
        }
        std::fs::write(recorded_in(RECORDED_V2, "record"), snapshot_lines(&record)).unwrap();
    }
    let text = std::fs::read_to_string(recorded_in(RECORDED_V2, "record")).unwrap();
    let record = parse_snapshots(&text);
    let bucket = Bucket::new();
    for name in named_runs(&record).unwrap() {
        let data = std::fs::read(recorded_in(RECORDED_V2, &name)).unwrap();
        bucket.write(&name, data).await;
    }
    let s = stores(&bucket, RunConfig::default());
    check_dropping(&s, &record).await;
    assert_eq!(s.counts().barrier.count, 0);
}

/// Each snapshot as a line: its first and last key group, its layout and
/// its data in hex.
fn snapshot_lines(record: &[Snapshot]) -> String {
    record
        .iter()
        .map(|s| {
            let hex: String = s.data.iter().map(|b| format!("{b:02x}")).collect();
            format!("{} {} {} {hex}\n", s.range.first, s.range.last, s.layout)
        })
        .collect()
}

fn parse_snapshots(text: &str) -> Vec<Snapshot> {
    text.lines()
        .map(|line| {
            let f: Vec<&str> = line.split(' ').collect();
            let data = (0..f[3].len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&f[3][i..i + 2], 16).unwrap())
                .collect::<Vec<u8>>();
            Snapshot {
                range: KeyGroupRange::new(f[0].parse().unwrap(), f[1].parse().unwrap()),
                layout: f[2].parse().unwrap(),
                data: Bytes::from(data),
                pending: Bytes::new(),
            }
        })
        .collect()
}

/// Whether `OPENETL_STATE_RECORD` names `folder`: then a test records it
/// anew, which only a new format or snapshot version may do, into a folder
/// of its own.
fn recording(folder: &str) -> bool {
    std::env::var("OPENETL_STATE_RECORD").is_ok_and(|f| folder.rsplit('/').next() == Some(&f))
}

/// Runs of format 1 and their snapshots of version 1, recorded when they
/// were made, restore: a later format must still read them.
#[tokio::test]
async fn recorded_runs_restore() {
    if recording(RECORDED) {
        let bucket = Bucket::new();
        // Small blocks, so runs have several, raw and compressed.
        let config = RunConfig {
            block_bytes: 160,
            ..Default::default()
        };
        let record = record_script(&stores(&bucket, config)).await;
        check_recorded(&stores(&bucket, RunConfig::default()), &record).await;
        for name in named_runs(&record).unwrap() {
            let path = recorded(&name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, bucket.read(&name).await).unwrap();
        }
        std::fs::write(recorded("record"), snapshot_lines(&record)).unwrap();
    }
    let record = parse_snapshots(&std::fs::read_to_string(recorded("record")).unwrap());
    let bucket = Bucket::new();
    for name in named_runs(&record).unwrap() {
        bucket
            .write(&name, std::fs::read(recorded(&name)).unwrap())
            .await;
    }
    let s = stores(&bucket, RunConfig::default());
    check_recorded(&s, &record).await;
    assert_eq!(s.counts().barrier.count, 0);
}
