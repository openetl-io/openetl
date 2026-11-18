//! The dedupe operator on state's layers: which row each key keeps, when,
//! repeats, late rows, retention and its changes, a latest row's upserts
//! and deletes, and that what's written doesn't depend on how rows arrive,
//! how barriers fall, or how many lanes restore them.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{FixedSizeBinaryArray, RecordBatch, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use openetl_normalize::Code;
use openetl_sql::{Catalog, Operator as Planned, Stream};
use openetl_state::{
    KeyGroupRange, Layer, MemoryBucket, NEVER, ObjectStorage, PendingLimit, RunConfig, RunStores,
    Snapshot, Stores,
};

use super::Operator;
use crate::window::tests::{REGIONS, next, orders, ranges, routed};

const GROUPS: u32 = 256;
const SEC: i64 = 1_000_000;

fn stream() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
        Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]))
}

/// The dedupe of a statement over `orders`, whose event time is `at`, with
/// `retention` in microseconds.
fn planned(sql: &str, retention: Option<i64>) -> Operator {
    let mut c = Catalog::new();
    c.insert(
        "orders",
        Stream {
            schema: stream(),
            key: Vec::new(),
            changelog: false,
            event_time: Some("at".into()),
        },
    )
    .unwrap();
    let mut plan = openetl_sql::plan(sql, &c).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let st = plan.statements.remove(0);
    let Planned::Dedupe(d) = st.operator else {
        panic!("{sql}: not a dedupe");
    };
    let d = Arc::new(*d);
    Operator::new(&d.id, st.at, d.clone(), &st.output, retention, GROUPS).unwrap()
}

/// Each region's first order.
const FIRST: &str = "INSERT INTO o SELECT region, amount, at FROM orders \
     QUALIFY row_number() OVER (PARTITION BY region ORDER BY at) = 1";
/// Each region and amount's first order, of those with an amount.
const FIRST_OF_TWO: &str = "INSERT INTO o SELECT region, amount, at FROM orders WHERE amount IS NOT NULL \
     QUALIFY row_number() OVER (PARTITION BY region, amount ORDER BY at) = 1";
/// Each region's latest order.
const LATEST: &str = "INSERT INTO o SELECT region, amount, at FROM orders \
     QUALIFY row_number() OVER (PARTITION BY region ORDER BY at DESC) = 1";
/// Each region's latest order, where its amount is under 50.
const LATEST_SMALL: &str = "INSERT INTO o SELECT region, amount, at FROM \
     (SELECT *, row_number() OVER (PARTITION BY region ORDER BY at DESC) AS rn FROM orders) \
     WHERE rn = 1 AND amount < 50";

/// A row: its ID, region, amount and time.
type Row = (u64, &'static str, i64, i64);

/// Row n's record ID: its IDs order as their numbers do.
fn id(n: u64) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&n.to_be_bytes());
    id
}

fn number(id: &[u8]) -> u64 {
    u64::from_be_bytes(id[..8].try_into().unwrap())
}

/// A batch of the statement's input, what it reads of `orders`' rows, and
/// their record IDs.
fn batch(op: &Operator, rows: &[Row]) -> (RecordBatch, FixedSizeBinaryArray) {
    let full = orders(rows.iter().map(|r| (r.1, r.2, r.3)));
    let ids = FixedSizeBinaryArray::try_from_iter(rows.iter().map(|r| id(r.0))).unwrap();
    (op.plan.input.input(&full).unwrap(), ids)
}

/// What the output holds: for a first-row dedupe, every row written, by
/// ID; for a latest-row dedupe, each key's row as a merge leaves it, by its
/// region.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Output {
    appended: BTreeMap<u64, String>,
    merged: BTreeMap<String, (u64, String)>,
    ops: Vec<String>,
    /// Each op's region and sequence, in the order they're written.
    seqs: Vec<(String, String)>,
}

/// Runs batches through the operator on lanes, routing each row to the
/// lane owning its key's group, with watermarks between them.
struct Run {
    sql: &'static str,
    ops: Vec<Operator>,
    layers: Vec<Layer>,
    out: Output,
    late: usize,
    rejected: usize,
    repeats: u64,
    kept: u64,
    forgot: u64,
    /// The bytes of a lane's changes past which it flushes them early; 0
    /// for never.
    flush_at: usize,
    /// The early flushes of the lanes before a restore.
    flushes: u64,
}

impl Run {
    async fn new(
        sql: &'static str,
        retention: Option<i64>,
        bucket: &dyn Stores,
        (generation, epoch): (u64, u64),
        lanes: u32,
        snapshots: &[Snapshot],
    ) -> Run {
        let mut ops = Vec::new();
        let mut layers = Vec::new();
        for r in ranges(lanes) {
            let mine: Vec<Snapshot> = snapshots
                .iter()
                .filter(|s| s.range.overlaps(&r))
                .cloned()
                .collect();
            layers.push(
                Layer::open(bucket, generation, epoch, r, &mine)
                    .await
                    .unwrap(),
            );
            ops.push(planned(sql, retention));
        }
        Run {
            sql,
            ops,
            layers,
            out: Output::default(),
            late: 0,
            rejected: 0,
            repeats: 0,
            kept: 0,
            forgot: 0,
            flush_at: 0,
            flushes: 0,
        }
    }

    /// Has each lane flush its changes early past `bytes`.
    fn flush_at(&mut self, bytes: usize) {
        self.flush_at = bytes;
        for l in &mut self.layers {
            l.set_flush_at(bytes);
        }
    }

    /// The early flushes its lanes made, and those before its restores.
    fn flushes(&self) -> u64 {
        self.flushes + self.layers.iter().map(Layer::flushes).sum::<u64>()
    }

    /// Another attempt restores `snapshots` on `lanes` lanes, with
    /// `retention`, at the watermark this one had, keeping what was
    /// written.
    async fn restore(
        self,
        bucket: &dyn Stores,
        at: (u64, u64),
        lanes: u32,
        retention: Option<i64>,
        snapshots: &[Snapshot],
    ) -> Run {
        let watermark = self.ops[0].base.watermark;
        let mut run = Run::new(self.sql, retention, bucket, at, lanes, snapshots).await;
        run.flush_at(self.flush_at);
        run.flushes = self.flushes();
        run.out = self.out;
        (run.late, run.rejected) = (self.late, self.rejected);
        (run.repeats, run.kept, run.forgot) = (self.repeats, self.kept, self.forgot);
        if watermark > i64::MIN {
            run.advance(watermark).await;
        }
        run
    }

    fn give(&mut self, out: &crate::window::Out) {
        assert!(out.failed.is_empty(), "{}: {:?}", self.sql, out.failed);
        let Some(t) = &out.rows else { return };
        let b = &t.batch;
        let seq = t.control.seq.as_ref().unwrap();
        let ops: Vec<Option<&str>> = match &t.control.op {
            Some(o) => o.as_string::<i32>().iter().collect(),
            None => vec![None; b.num_rows()],
        };
        for (r, id) in t.ids.iter().enumerate() {
            let cells: Vec<String> = b
                .schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    let v = arrow_cast::display::array_value_to_string(b.column(i), r).unwrap();
                    format!("{}={v}", f.name())
                })
                .collect();
            let n = number(id.unwrap());
            let seq = arrow_cast::display::array_value_to_string(seq, r).unwrap();
            let row = format!("{} seq={seq}", cells.join(" "));
            match ops[r] {
                None => assert!(
                    self.out.appended.insert(n, row).is_none(),
                    "{}: row {n} written twice",
                    self.sql
                ),
                Some(op) => {
                    let region =
                        arrow_cast::display::array_value_to_string(b.column(0), r).unwrap();
                    self.out.ops.push(format!("{op} {region}"));
                    self.out.seqs.push((region.clone(), seq.clone()));
                    // A merge with hard deletes: a delete of a key the
                    // output doesn't have does nothing.
                    match op {
                        "delete" => {
                            self.out.merged.remove(&region);
                        }
                        // An insert upserts: the output may have the key's
                        // row from before the state began.
                        _ => {
                            let had = self.out.merged.insert(region, (n, row.clone()));
                            assert!(had.is_some() || op == "insert", "{op} {row}");
                        }
                    }
                }
            }
        }
    }

    async fn rows(&mut self, rows: &[Row]) {
        let (all, all_ids) = batch(&self.ops[0], rows);
        let ranges: Vec<KeyGroupRange> = self.layers.iter().map(Layer::range).collect();
        for (i, at) in routed(&all, &self.ops[0].plan.need, &ranges)
            .into_iter()
            .enumerate()
        {
            let Some(at) = at else { continue };
            let part = arrow_select::take::take_record_batch(&all, &at).unwrap();
            let part_ids = arrow_select::take::take(&all_ids, &at, None).unwrap();
            let part_ids = part_ids.as_fixed_size_binary().clone();
            let out = self.ops[i]
                .rows(&mut self.layers[i], &part, &part_ids)
                .await
                .unwrap();
            self.late += out
                .rejected
                .iter()
                .filter(|(_, _, c)| c.code == Code::Late)
                .count();
            self.rejected += out.rejected.len();
            self.give(&out);
            self.count(i);
        }
    }

    fn count(&mut self, i: usize) {
        let op = &mut self.ops[i];
        self.repeats += std::mem::take(&mut op.repeats);
        self.kept += std::mem::take(&mut op.kept);
        self.forgot += std::mem::take(&mut op.forgot);
    }

    async fn advance(&mut self, t: i64) {
        for i in 0..self.ops.len() {
            loop {
                let (out, more) = self.ops[i].advance(&mut self.layers[i], t).await.unwrap();
                self.give(&out);
                if !more {
                    break;
                }
            }
            self.count(i);
        }
    }

    async fn barrier(&mut self, epoch: u64) -> Vec<Snapshot> {
        let mut out = Vec::new();
        for l in &mut self.layers {
            out.extend(l.barrier(epoch).await.unwrap());
        }
        out
    }
}

/// The rows a first-row dedupe writes of `rows`, worked out apart: each
/// key's rows in order of time, then of ID, and each written where its
/// key's last written row is `retention` or more before it, or it has
/// none.
fn first(rows: &[Row], retention: Option<i64>, two: bool) -> BTreeSet<u64> {
    let mut by: BTreeMap<(&str, Option<i64>), Vec<&Row>> = BTreeMap::new();
    for r in rows {
        by.entry((r.1, two.then_some(r.2))).or_default().push(r);
    }
    let mut out = BTreeSet::new();
    for rs in by.values_mut() {
        rs.sort_by_key(|r| (r.3, r.0));
        let mut last: Option<i64> = None;
        for r in rs.iter() {
            let new = match (last, retention) {
                (None, _) => true,
                (Some(_), None) => false,
                (Some(l), Some(ret)) => r.3 - l >= ret,
            };
            if new {
                out.insert(r.0);
                last = Some(r.3);
            }
        }
    }
    out
}

/// What a latest-row dedupe leaves the output of `rows`, worked out apart:
/// each region's row latest in time, then in ID, where its amount passes
/// `passes`.
fn latest(rows: &[Row], passes: fn(i64) -> bool) -> BTreeMap<String, u64> {
    let mut by: BTreeMap<&str, &Row> = BTreeMap::new();
    for r in rows {
        let e = by.entry(r.1).or_insert(r);
        if (r.3, r.0) > (e.3, e.0) {
            *e = r;
        }
    }
    by.into_iter()
        .filter(|(_, r)| passes(r.2))
        .map(|(k, r)| (k.to_owned(), r.0))
        .collect()
}

/// A region's first row waits until the watermark passes its time, as an
/// earlier one may come; of its rows of one time, the least ID is kept. A
/// row of a kept region is dropped as a repeat, late or not, and a late
/// row of another region is quarantined, as is a row without a time.
#[tokio::test]
async fn the_first_row_waits_for_the_watermark_and_repeats_are_dropped() {
    let bucket = MemoryBucket::new();
    let mut run = Run::new(FIRST, Some(NEVER), &bucket, (1, 1), 1, &[]).await;
    run.rows(&[
        (2, "eu", 5, 10 * SEC),
        (1, "eu", 7, 10 * SEC),
        (3, "eu", 1, 20 * SEC),
    ])
    .await;
    run.advance(10 * SEC).await;
    assert!(run.out.appended.is_empty());
    run.advance(10 * SEC + 1).await;
    assert_eq!(run.out.appended.keys().copied().collect::<Vec<_>>(), [1]);
    assert!(run.out.appended[&1].contains("amount=7 "), "{:?}", run.out);
    assert_eq!(run.repeats, 1);
    // Behind the watermark: eu's is a repeat, us's is late.
    run.rows(&[(4, "eu", 1, 5 * SEC), (5, "us", 1, 5 * SEC)])
        .await;
    assert_eq!((run.late, run.rejected, run.repeats), (1, 1, 2));
    let (b, i) = batch(&run.ops[0], &[(6, "apac", 1, 0)]);
    let nulls = RecordBatch::try_new(
        b.schema(),
        vec![
            b.column(0).clone(),
            b.column(1).clone(),
            Arc::new(TimestampMicrosecondArray::from(vec![None::<i64>]).with_timezone("UTC")),
        ],
    )
    .unwrap();
    let out = run.ops[0]
        .rows(&mut run.layers[0], &nulls, &i)
        .await
        .unwrap();
    assert_eq!(out.rejected.len(), 1);
    assert_eq!(out.rejected[0].2.code, Code::EventTime);
    run.advance(i64::MAX).await;
    // Row 3, waiting at 20 seconds, is a repeat of row 1 when it leaves.
    assert_eq!(run.out.appended.len(), 1);
    assert_eq!((run.repeats, run.kept, run.forgot), (3, 1, 0));
    assert!(
        run.out.appended[&1].ends_with("seq=1970-01-01T00:00:10Z"),
        "{:?}",
        run.out
    );
}

/// A region is forgotten once the watermark passes its first row's time
/// plus the retention: a row at that time or after is written again, and
/// one before is a repeat, even if it comes after the watermark passed it.
#[tokio::test]
async fn a_key_is_forgotten_past_its_retention() {
    let bucket = MemoryBucket::new();
    let mut run = Run::new(FIRST, Some(10 * SEC), &bucket, (1, 1), 1, &[]).await;
    let rows = [
        (1, "eu", 1, 0),
        (2, "eu", 2, 5 * SEC),
        (3, "eu", 3, 10 * SEC),
        (4, "eu", 4, 12 * SEC),
        (5, "eu", 5, 25 * SEC),
    ];
    run.rows(&rows).await;
    run.advance(26 * SEC).await;
    let written: Vec<u64> = run.out.appended.keys().copied().collect();
    assert_eq!(written, [1, 3, 5]);
    assert_eq!(
        first(&rows, Some(10 * SEC), false),
        BTreeSet::from([1, 3, 5])
    );
    assert_eq!((run.repeats, run.kept, run.forgot), (2, 3, 2));
    // At 30 seconds, row 5's key is kept until 35: a row at 27, behind the
    // watermark, and one at 34 are repeats, and one at 35 is written.
    run.advance(30 * SEC).await;
    run.rows(&[(6, "eu", 6, 27 * SEC), (7, "eu", 7, 34 * SEC)])
        .await;
    assert_eq!((run.repeats, run.late), (4, 0));
    run.rows(&[(8, "eu", 8, 35 * SEC)]).await;
    assert!(!run.out.appended.contains_key(&8));
    // Row 8 waits for the watermark to pass its own time, by when the key
    // kept at 25 seconds is forgotten.
    run.advance(36 * SEC).await;
    assert!(run.out.appended.contains_key(&8));
    // A row of a key forgotten before it comes, behind the watermark, is
    // late: us, kept at 37 seconds, is forgotten once the watermark passes
    // 47.
    run.rows(&[(9, "us", 9, 37 * SEC)]).await;
    run.advance(48 * SEC).await;
    run.rows(&[(10, "us", 10, 40 * SEC)]).await;
    assert_eq!((run.late, run.repeats), (1, 4));
    run.advance(i64::MAX).await;
    let written: Vec<u64> = run.out.appended.keys().copied().collect();
    assert_eq!(written, [1, 3, 5, 8, 9]);
    assert_eq!((run.kept, run.forgot), (5, 5));
}

/// A changed retention applies to every key kept, but a key the old one
/// forgot stays forgotten: its next row is written again.
#[tokio::test]
async fn a_changed_retention_applies_to_kept_keys_and_forgotten_ones_stay_forgotten() {
    let bucket = MemoryBucket::new();
    let mut run = Run::new(FIRST, Some(10 * SEC), &bucket, (1, 1), 1, &[]).await;
    run.rows(&[(1, "eu", 1, 0), (2, "us", 1, 25 * SEC)]).await;
    run.advance(30 * SEC).await;
    let snaps = run.barrier(1).await;
    // eu is gone at 30 seconds; us is kept until 35, and longer with an
    // hour's retention.
    let mut run = run
        .restore(&bucket, (2, 2), 2, Some(3600 * SEC), &snaps)
        .await;
    run.rows(&[(3, "eu", 1, 40 * SEC), (4, "us", 1, 40 * SEC)])
        .await;
    run.advance(i64::MAX).await;
    let written: Vec<u64> = run.out.appended.keys().copied().collect();
    assert_eq!(written, [1, 2, 3]);
    assert_eq!(run.repeats, 1);
}

/// Each region's latest row is written as it comes, an insert and then
/// updates, whatever the watermark; an earlier one that comes after it,
/// and one of the same time with a lesser ID, are dropped.
#[tokio::test]
async fn the_latest_row_is_an_upsert_and_an_older_one_never_replaces_it() {
    let bucket = MemoryBucket::new();
    let mut run = Run::new(LATEST, None, &bucket, (1, 1), 1, &[]).await;
    run.rows(&[(5, "eu", 1, 10 * SEC), (6, "eu", 2, 10 * SEC)])
        .await;
    assert_eq!(run.out.merged["eu"].0, 6);
    run.advance(100 * SEC).await;
    run.rows(&[
        (7, "eu", 3, 5 * SEC),
        (4, "eu", 4, 10 * SEC),
        (8, "us", 5, 0),
    ])
    .await;
    assert_eq!(run.out.merged["eu"].0, 6);
    run.rows(&[(9, "eu", 9, 11 * SEC)]).await;
    assert_eq!(run.out.merged["eu"].0, 9);
    assert!(run.out.merged["eu"].1.ends_with("seq=1970-01-01T00:00:11Z"));
    assert_eq!(run.out.ops, ["insert eu", "insert us", "update eu"]);
    assert_eq!((run.repeats, run.kept, run.late), (3, 2, 0));
}

/// A filter after a latest-row dedupe holds the output to a key's latest
/// row where it passes: a later row that fails it deletes the key's row,
/// and so does one of a key state doesn't have, whose row the output may
/// have from before; one of a key state has, which the output doesn't
/// have, writes nothing.
#[tokio::test]
async fn a_latest_row_a_filter_drops_deletes_its_keys_row() {
    let bucket = MemoryBucket::new();
    let mut run = Run::new(LATEST_SMALL, None, &bucket, (1, 1), 1, &[]).await;
    run.rows(&[(1, "eu", 5, SEC), (2, "us", 500, SEC)]).await;
    assert_eq!(run.out.ops, ["insert eu", "delete us"]);
    run.rows(&[(3, "eu", 500, 2 * SEC)]).await;
    assert!(run.out.merged.is_empty(), "{:?}", run.out);
    run.rows(&[(6, "us", 600, 2 * SEC)]).await;
    assert_eq!(run.out.ops, ["insert eu", "delete us", "delete eu"]);
    run.rows(&[(4, "eu", 7, 3 * SEC), (5, "us", 6, 3 * SEC)])
        .await;
    let mut last: Vec<&str> = run.out.ops[3..].iter().map(String::as_str).collect();
    last.sort_unstable();
    assert_eq!(last, ["insert eu", "insert us"]);
    let ids: Vec<u64> = run.out.merged.values().map(|r| r.0).collect();
    assert_eq!(ids, [4, 5]);
    assert_eq!(
        latest(
            &[
                (1, "eu", 5, SEC),
                (2, "us", 500, SEC),
                (3, "eu", 500, 2 * SEC),
                (6, "us", 600, 2 * SEC),
                (4, "eu", 7, 3 * SEC),
                (5, "us", 6, 3 * SEC)
            ],
            |a| a < 50
        )
        .len(),
        2
    );
}

/// The next full read starts with no state, and the output has the last
/// read's rows: a key whose latest row the filter now drops is deleted, a
/// key whose latest row passes is written again, and a key the read
/// doesn't have keeps its row, as nothing of it comes.
#[tokio::test]
async fn a_read_without_state_deletes_the_keys_the_filter_drops() {
    let bucket = MemoryBucket::new();
    let mut run = Run::new(LATEST_SMALL, None, &bucket, (1, 1), 1, &[]).await;
    run.rows(&[(1, "eu", 5, SEC), (2, "us", 6, SEC), (3, "jp", 7, SEC)])
        .await;
    assert_eq!(run.out.merged.len(), 3);
    let before = run.out.clone();
    let mut next = Run::new(LATEST_SMALL, None, &MemoryBucket::new(), (2, 1), 1, &[]).await;
    next.out = before;
    next.rows(&[(1, "eu", 5, SEC), (4, "eu", 90, 2 * SEC), (2, "us", 6, SEC)])
        .await;
    let mut ops: Vec<&str> = next.out.ops[3..].iter().map(String::as_str).collect();
    ops.sort_unstable();
    assert_eq!(ops, ["delete eu", "insert us"]);
    let ids: BTreeMap<&str, u64> = next
        .out
        .merged
        .iter()
        .map(|(k, r)| (k.as_str(), r.0))
        .collect();
    assert_eq!(ids, BTreeMap::from([("jp", 3), ("us", 2)]));
}

/// Whatever order a key's rows come in, one batch or several, its row ends
/// as its latest row has it: only a row later than every row written of the
/// key is written, so the writes of a key come in the order of their
/// sequences, and a delete of an earlier row that fails the filter never
/// lands after the latest row's upsert. A merge keeps the greatest sequence.
#[tokio::test]
async fn a_keys_row_ends_as_its_latest_whatever_the_order() {
    // At 1s it fails the filter, at 2s it passes, at 3s it fails again.
    let passes_last: [Row; 2] = [(1, "eu", 90, SEC), (2, "eu", 20, 2 * SEC)];
    let fails_last: [Row; 3] = [
        (1, "eu", 90, SEC),
        (2, "eu", 20, 2 * SEC),
        (3, "eu", 95, 3 * SEC),
    ];
    let mut compared = 0;
    for (rows, want) in [(&passes_last[..], Some(2)), (&fails_last[..], None)] {
        let mut orders: Vec<Vec<usize>> =
            vec![(0..rows.len()).collect(), (0..rows.len()).rev().collect()];
        if rows.len() == 3 {
            orders.push(vec![1, 0, 2]);
            orders.push(vec![2, 0, 1]);
        }
        for order in orders {
            for one_batch in [true, false] {
                // The output has eu's row from an earlier read.
                let mut run =
                    Run::new(LATEST_SMALL, None, &MemoryBucket::new(), (1, 1), 1, &[]).await;
                run.out.merged.insert("eu".into(), (0, "earlier".into()));
                let picked: Vec<Row> = order.iter().map(|&i| rows[i]).collect();
                if one_batch {
                    run.rows(&picked).await;
                } else {
                    for r in &picked {
                        run.rows(&[*r]).await;
                    }
                }
                assert_eq!(
                    run.out.merged.get("eu").map(|r| r.0),
                    want,
                    "{order:?}, one batch {one_batch}: {:?}",
                    run.out
                );
                // Its writes' sequences never go down, so a merge that
                // keeps the greatest ends with the last.
                let seqs: Vec<&String> = run.out.seqs.iter().map(|(_, q)| q).collect();
                assert!(seqs.is_sorted() && !seqs.is_empty(), "{order:?}: {seqs:?}");
                compared += 1;
            }
        }
    }
    assert_eq!(compared, 12);
}

/// A key state has keeps whether the output has its row across a restore:
/// a key the output doesn't have writes no delete, and one it has writes
/// one. A key state doesn't have yet, which the read's retry hasn't come
/// to, is deleted where the filter drops its latest row, as the output may
/// have its row from the last read.
#[tokio::test]
async fn a_restore_keeps_what_state_knows_of_the_output() {
    let bucket = MemoryBucket::new();
    let mut run = Run::new(LATEST_SMALL, None, &bucket, (1, 1), 1, &[]).await;
    // jp's row is in the output from the last read.
    run.out.merged.insert("jp".into(), (0, "earlier".into()));
    run.rows(&[(1, "eu", 5, SEC), (2, "us", 500, SEC)]).await;
    let snaps = run.barrier(1).await;
    let mut run = run.restore(&bucket, (2, 2), 2, None, &snaps).await;
    let done = run.out.ops.len();
    run.rows(&[
        (3, "us", 600, 2 * SEC),
        (4, "eu", 700, 2 * SEC),
        (5, "jp", 80, SEC),
    ])
    .await;
    let mut ops: Vec<&str> = run.out.ops[done..].iter().map(String::as_str).collect();
    ops.sort_unstable();
    assert_eq!(ops, ["delete eu", "delete jp"]);
    assert!(run.out.merged.is_empty(), "{:?}", run.out);
}

/// A delete of a key whose latest row a filter drops has the key's columns
/// and nulls: a projection after the filter, which would fail on the row,
/// never runs on it.
#[tokio::test]
async fn a_delete_runs_no_projection_its_filter_kept_from_the_row() {
    let sql = "INSERT INTO o SELECT region, 100 / amount AS share, at FROM \
               (SELECT *, row_number() OVER (PARTITION BY region ORDER BY at DESC) AS rn FROM orders) \
               WHERE rn = 1 AND amount <> 0";
    let bucket = MemoryBucket::new();
    let mut run = Run::new(sql, None, &bucket, (1, 1), 1, &[]).await;
    run.rows(&[(1, "eu", 5, SEC)]).await;
    assert!(
        run.out.merged["eu"].1.contains("share=20 "),
        "{:?}",
        run.out
    );
    let (b, i) = batch(&run.ops[0], &[(2, "eu", 0, 2 * SEC)]);
    let out = run.ops[0].rows(&mut run.layers[0], &b, &i).await.unwrap();
    let t = out.rows.as_ref().unwrap();
    let cells: Vec<String> = (0..t.batch.num_columns())
        .map(|c| arrow_cast::display::array_value_to_string(t.batch.column(c), 0).unwrap())
        .collect();
    assert_eq!(cells, ["eu", "", ""]);
    run.give(&out);
    assert_eq!(run.out.ops, ["insert eu", "delete eu"]);
    assert!(run.out.merged.is_empty() && out.failed.is_empty());
}

/// Rows of a key with one time and record ID, and other values, as a
/// source can give, go by their values' hashes: the same row is the key's
/// latest whichever comes first.
#[tokio::test]
async fn equal_times_and_ids_go_by_their_values_whatever_the_order() {
    let rows = [
        (5, "eu", 1, 10 * SEC),
        (5, "eu", 2, 10 * SEC),
        (5, "eu", 3, 10 * SEC),
    ];
    let mut kept = BTreeSet::new();
    for order in [[0, 1, 2], [2, 1, 0], [1, 2, 0]] {
        let bucket = MemoryBucket::new();
        let mut run = Run::new(LATEST, None, &bucket, (1, 1), 1, &[]).await;
        for i in order {
            run.rows(&[rows[i]]).await;
        }
        kept.insert(run.out.merged["eu"].1.clone());
        assert_eq!(run.repeats + run.out.ops.len() as u64, 3);
    }
    assert_eq!(kept.len(), 1, "{kept:?}");
}

/// A first-row dedupe needs a retention, which must be more than none.
#[test]
fn a_first_row_dedupe_needs_a_retention() {
    let mut c = Catalog::new();
    c.insert(
        "orders",
        Stream {
            schema: stream(),
            key: Vec::new(),
            changelog: false,
            event_time: Some("at".into()),
        },
    )
    .unwrap();
    let mut plan = openetl_sql::plan(FIRST, &c).unwrap();
    let st = plan.statements.remove(0);
    let Planned::Dedupe(d) = st.operator else {
        panic!("not a dedupe");
    };
    let d = Arc::new(*d);
    for (retention, want) in [
        (None, "and stream \"orders\" sets no dedupe.retention"),
        (Some(0), "a dedupe's retention of 0µs"),
    ] {
        let Err(e) = Operator::new(&d.id, st.at, d.clone(), "o", retention, GROUPS) else {
            panic!("{retention:?} takes");
        };
        assert!(e.message.contains(want), "{}", e.message);
    }
}

/// Rows over five minutes, in whole seconds, so many share a time, with
/// amounts that some keys share.
fn rows(seed: u64, n: usize) -> Vec<Row> {
    let mut s = seed;
    (0..n)
        .map(|i| {
            let r = REGIONS[usize::try_from(next(&mut s) % 5).unwrap()];
            let a = i64::try_from(next(&mut s) % 8).unwrap();
            let t = i64::try_from(next(&mut s) % 300).unwrap() * SEC;
            (u64::try_from(i).unwrap(), r, a, t)
        })
        .collect()
}

/// Any arrival order within the watermark's bound, any cut into batches,
/// barriers whose records carry only pending sets or runs beside them,
/// early flushes of the set and the waiting rows before barriers,
/// and restores onto another number of lanes anywhere, write what the
/// rows give worked out apart: a first-row dedupe, unbounded or with a
/// retention, each key's first rows, and a latest-row dedupe, with a filter
/// after it or not, each key's latest.
#[tokio::test]
async fn what_is_written_doesnt_depend_on_order_barriers_or_lanes() {
    let cases: [(&'static str, Option<i64>); 5] = [
        (FIRST, Some(NEVER)),
        (FIRST_OF_TWO, Some(30 * SEC)),
        (FIRST, Some(45 * SEC)),
        (LATEST, None),
        (LATEST_SMALL, None),
    ];
    let (mut compared, mut restores, mut only_pending, mut beside_runs) = (0, 0, 0, 0);
    let (mut flushes, mut early_beside_pending) = (0, 0);
    let input = rows(7, 300);
    for (sql, retention) in cases {
        for seed in 0..8u64 {
            let mut s = seed + 100;
            // Shuffled, with the watermark only ever at the least time of
            // the rows still to come: none is late.
            let mut shuffled = input.clone();
            for i in (1..shuffled.len()).rev() {
                let j = usize::try_from(next(&mut s) % (i as u64 + 1)).unwrap();
                shuffled.swap(i, j);
            }
            let objects = Arc::new(ObjectStorage::new(
                Arc::new(object_store::memory::InMemory::new()),
                object_store::path::Path::from("state"),
            ));
            let bytes = if seed % 2 == 0 { 1 << 30 } else { 4 << 10 };
            let stores = RunStores::new(objects, RunConfig::default()).with_pending(PendingLimit {
                bytes,
                key_groups: GROUPS,
            });
            let mut lanes = 1 + u32::try_from(next(&mut s) % 4).unwrap();
            let mut run = Run::new(sql, retention, &stores, (1, 1), lanes, &[]).await;
            // Half the seeds flush a lane's changes early past a KiB, the
            // set's entries and waiting rows among them, beside pending
            // sets that the record carries or that pass its share.
            if seed % 4 >= 2 {
                run.flush_at(1 << 10);
            }
            let (mut epoch, mut generation, mut at) = (1, 1, 0);
            while at < shuffled.len() {
                let n = 1 + usize::try_from(next(&mut s) % 40).unwrap();
                let part = &shuffled[at..(at + n).min(shuffled.len())];
                at += part.len();
                run.rows(part).await;
                let left = shuffled[at..].iter().map(|r| r.3).min().unwrap_or(i64::MAX);
                if next(&mut s).is_multiple_of(3) {
                    run.advance(left.min(i64::MAX - 1)).await;
                }
                if next(&mut s).is_multiple_of(4) {
                    let snaps = run.barrier(epoch).await;
                    let pending = snaps.iter().any(|sn| !sn.pending.is_empty());
                    let named = snaps
                        .iter()
                        .any(|sn| !openetl_state::runs::runs(sn).unwrap_or_default().is_empty());
                    match (pending, named) {
                        (true, false) => only_pending += 1,
                        (true, true) => beside_runs += 1,
                        _ => {}
                    }
                    let early = snaps.iter().any(|sn| {
                        openetl_state::runs::runs(sn)
                            .unwrap_or_default()
                            .iter()
                            .any(|r| r.name.contains(".early"))
                    });
                    early_beside_pending += u32::from(early && pending);
                    epoch += 1;
                    if next(&mut s).is_multiple_of(2) {
                        // The attempt dies after the record; another
                        // restores it on other lanes.
                        generation += 1;
                        restores += 1;
                        lanes = 1 + u32::try_from(next(&mut s) % 4).unwrap();
                        run = run
                            .restore(&stores, (generation, epoch), lanes, retention, &snaps)
                            .await;
                    }
                }
            }
            run.advance(i64::MAX).await;
            assert_eq!(run.rejected, 0, "{sql}, seed {seed}");
            flushes += run.flushes();
            match sql {
                LATEST | LATEST_SMALL => {
                    let passes: fn(i64) -> bool = match sql {
                        LATEST => |_| true,
                        _ => |a| a < 50,
                    };
                    let got: BTreeMap<String, u64> = run
                        .out
                        .merged
                        .iter()
                        .map(|(k, r)| (k.clone(), r.0))
                        .collect();
                    assert_eq!(got, latest(&input, passes), "{sql}, seed {seed}");
                    compared += got.len();
                }
                _ => {
                    let got: BTreeSet<u64> = run.out.appended.keys().copied().collect();
                    let retention = retention.filter(|r| *r != NEVER);
                    let want = first(&input, retention, sql == FIRST_OF_TWO);
                    assert_eq!(got, want, "{sql}, seed {seed}");
                    assert_eq!(
                        run.kept + run.repeats,
                        input.len() as u64,
                        "{sql}, seed {seed}"
                    );
                    compared += got.len();
                }
            }
        }
    }
    assert!(compared > 300, "{compared} rows compared");
    assert!(restores > 12, "{restores} restores");
    assert!(
        only_pending > 0 && beside_runs > 0,
        "{only_pending} records with only pending sets, {beside_runs} with runs beside them"
    );
    assert!(
        flushes > 0 && early_beside_pending > 0,
        "{flushes} early flushes, {early_beside_pending} records with early runs beside pending sets"
    );
}
