//! The `OVER` operator on state's layers: what it gives and when, late
//! rows, rows waiting through a schema change, and that what each row
//! leaves with doesn't depend on how rows arrive, how barriers fall, or
//! how many lanes restore them.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{FixedSizeBinaryArray, RecordBatch, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use openetl_normalize::Code;
use openetl_sql::{Catalog, Operator as Planned, Stream};
use openetl_state::{KeyGroupRange, Layer, MemoryBucket, Snapshot, Stores};

use super::{Operator, RELEASE, TAKE};
use crate::window::tests::{REGIONS, next, orders, ranges, routed};

const GROUPS: u32 = 256;
const SEC: i64 = 1_000_000;

fn stream(extra: bool) -> SchemaRef {
    let mut fields = vec![
        Field::new("region", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
        Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ];
    if extra {
        fields.push(Field::new("note", DataType::Utf8, true));
    }
    Arc::new(Schema::new(fields))
}

/// The planned `OVER` of a statement over `orders`, whose event time is
/// `at`, and the operator.
fn planned(sql: &str, extra: bool) -> (Arc<openetl_sql::Over>, Operator) {
    let mut c = Catalog::new();
    c.insert(
        "orders",
        Stream {
            schema: stream(extra),
            key: Vec::new(),
            changelog: false,
            event_time: Some("at".into()),
        },
    )
    .unwrap();
    let mut plan = openetl_sql::plan(sql, &c).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let st = plan.statements.remove(0);
    let Planned::Over(o) = st.operator else {
        panic!("{sql}: not OVER");
    };
    let o = Arc::new(*o);
    let op = Operator::new(&o.id, st.at, o.clone(), &st.output, GROUPS).unwrap();
    (o, op)
}

/// A row: its ID, region, amount and time.
type Row = (u64, &'static str, i64, i64);

/// A batch of the statement's input, what it reads of `orders`' rows, and
/// their record IDs.
fn batch(op: &Operator, rows: &[Row]) -> (RecordBatch, FixedSizeBinaryArray) {
    let full = orders(rows.iter().map(|r| (r.1, r.2, r.3)));
    (op.plan.input.input(&full).unwrap(), ids(rows))
}

/// Row n's record ID: its IDs order as their numbers do.
fn id(n: u64) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&n.to_be_bytes());
    id
}

fn ids(rows: &[Row]) -> FixedSizeBinaryArray {
    FixedSizeBinaryArray::try_from_iter(rows.iter().map(|r| id(r.0))).unwrap()
}

/// Each row the output was given, by its record ID: its columns, as text.
type Given = BTreeMap<[u8; 16], String>;

/// Runs batches through the operator on lanes, routing each row to the
/// lane owning its key's group, with watermarks between them.
struct Run {
    sql: &'static str,
    ops: Vec<Operator>,
    layers: Vec<Layer>,
    given: Given,
    late: usize,
    rejected: usize,
}

impl Run {
    async fn new(
        sql: &'static str,
        bucket: &dyn Stores,
        generation: u64,
        epoch: u64,
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
            ops.push(planned(sql, false).1);
        }
        Run {
            sql,
            ops,
            layers,
            given: Given::new(),
            late: 0,
            rejected: 0,
        }
    }

    fn give(&mut self, out: &crate::window::Out) {
        assert!(out.failed.is_empty(), "{}: {:?}", self.sql, out.failed);
        let Some(t) = &out.rows else { return };
        assert!(t.control.op.is_none());
        let b = &t.batch;
        let seq = t.control.seq.as_ref().unwrap();
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
            let id: [u8; 16] = id.unwrap().try_into().unwrap();
            let seq = arrow_cast::display::array_value_to_string(seq, r).unwrap();
            let row = format!("{} seq={seq}", cells.join(" "));
            assert!(
                self.given.insert(id, row).is_none(),
                "{}: row {id:?} given twice",
                self.sql
            );
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
            let part_ids = arrow_array::cast::AsArray::as_fixed_size_binary(&part_ids).clone();
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
            assert!(out.rows.is_none());
        }
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

/// Rows over five minutes, in whole seconds, so many share a time.
fn rows(seed: u64, n: usize) -> Vec<Row> {
    let mut s = seed;
    (0..n)
        .map(|i| {
            let r = REGIONS[usize::try_from(next(&mut s) % 5).unwrap()];
            let a = i64::try_from(next(&mut s) % 100).unwrap() - 20;
            let t = i64::try_from(next(&mut s) % 300).unwrap() * SEC;
            (u64::try_from(i).unwrap(), r, a, t)
        })
        .collect()
}

const RUNNING: &str = "INSERT INTO o SELECT region, amount, at, sum(amount) OVER (PARTITION BY region ORDER BY at) AS total, \
     count(*) OVER (PARTITION BY region ORDER BY at ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS n3, \
     lag(amount) OVER (PARTITION BY region ORDER BY at) AS before, \
     first_value(amount) OVER (PARTITION BY region ORDER BY at ROWS 1 PRECEDING) AS prev \
     FROM orders";

/// The values RUNNING gives each row, worked out apart: each region's rows
/// in order of time, then of ID.
fn running(rows: &[Row]) -> BTreeMap<u64, (i64, i64, Option<i64>, i64)> {
    let mut by: BTreeMap<&str, Vec<&Row>> = BTreeMap::new();
    for r in rows {
        by.entry(r.1).or_default().push(r);
    }
    let mut out = BTreeMap::new();
    for rs in by.values_mut() {
        rs.sort_by_key(|r| (r.3, r.0));
        for (i, r) in rs.iter().enumerate() {
            let total: i64 = rs.iter().filter(|o| o.3 <= r.3).map(|o| o.2).sum();
            let n3 = i64::try_from(i.min(2) + 1).unwrap();
            let before = i.checked_sub(1).map(|j| rs[j].2);
            let prev = rs[i.saturating_sub(1)].2;
            out.insert(r.0, (total, n3, before, prev));
        }
    }
    out
}

/// A running total, a ROWS frame, lag and first_value give each row what
/// its region's rows before it in time, then in ID, give it, worked out
/// apart, whatever order rows come in.
#[tokio::test]
async fn each_row_leaves_with_its_frames_values() {
    let input = rows(3, 200);
    let want = running(&input);
    let bucket = MemoryBucket::new();
    let mut run = Run::new(RUNNING, &bucket, 1, 1, 1, &[]).await;
    let mut shuffled = input.clone();
    shuffled.reverse();
    run.rows(&shuffled).await;
    assert!(run.given.is_empty());
    run.advance(i64::MAX).await;
    assert_eq!(run.given.len(), input.len());
    let mut compared = 0;
    for (got, row) in &run.given {
        let n = u64::from_be_bytes(got[..8].try_into().unwrap());
        let (total, n3, before, prev) = want[&n];
        let before = before.map_or("".to_owned(), |b| b.to_string());
        assert!(
            row.contains(&format!(
                " total={total} n3={n3} before={before} prev={prev} "
            )),
            "row {n}: {row}, want {:?}",
            want[&n]
        );
        compared += 1;
    }
    assert_eq!(compared, 200);
}

/// A row waits until the watermark passes its time, and rows of one time
/// leave together; one that comes behind the watermark is late, and one
/// without a time has no place in the order.
#[tokio::test]
async fn rows_wait_for_the_watermark_and_late_ones_are_quarantined() {
    let bucket = MemoryBucket::new();
    let mut run = Run::new(RUNNING, &bucket, 1, 1, 1, &[]).await;
    run.rows(&[
        (1, "eu", 5, 10 * SEC),
        (2, "eu", 7, 10 * SEC),
        (3, "eu", 1, 20 * SEC),
    ])
    .await;
    // At 10 seconds, the rows at 10 may still have peers to come.
    run.advance(10 * SEC).await;
    assert!(run.given.is_empty());
    run.advance(10 * SEC + 1).await;
    assert_eq!(run.given.len(), 2);
    // A peer of rows that left is late, as is any row behind the
    // watermark; one at it waits.
    run.rows(&[
        (4, "eu", 1, 10 * SEC),
        (5, "us", 1, 10 * SEC),
        (6, "eu", 2, 10 * SEC + 1),
    ])
    .await;
    assert_eq!((run.late, run.rejected), (2, 2));
    let (b, i) = batch(&run.ops[0], &[(7, "eu", 1, 0)]);
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
    assert_eq!(run.given.len(), 4);
    // The row at 10 seconds plus a microsecond came after the two at 10,
    // and before the one at 20.
    let six = run.given[&id(6)].clone();
    assert!(six.contains("total=14 "), "{six}");
    let three = run.given[&id(3)].clone();
    assert!(
        three.contains("total=15 ") && three.contains("before=2 "),
        "{three}"
    );
    // Every row left, and the frames stay, in state's values alone.
    run.layers[0].barrier(1).await.unwrap();
}

/// More rows than an advance gives at once leave over several calls, peers
/// together, each once.
#[tokio::test]
async fn many_rows_leave_over_several_calls() {
    let bucket = MemoryBucket::new();
    let mut run = Run::new(RUNNING, &bucket, 1, 1, 1, &[]).await;
    let n = RELEASE * 2 + 100;
    let input: Vec<Row> = (0..n)
        .map(|i| {
            let i = u64::try_from(i).unwrap();
            (i, "eu", 1, i64::try_from(i / 3).unwrap())
        })
        .collect();
    run.rows(&input).await;
    let (out, more) = run.ops[0]
        .advance(&mut run.layers[0], i64::MAX)
        .await
        .unwrap();
    assert!(more);
    run.give(&out);
    assert!(run.given.len() >= RELEASE && run.given.len() < n);
    run.advance(i64::MAX).await;
    assert_eq!(run.given.len(), n);
    let last = run.given[&id(u64::try_from(n - 1).unwrap())].clone();
    assert!(last.contains(&format!("total={n} ")), "{last}");
}

/// A key's peers, rows of one time, leave together however many takes
/// from state they span: a RANGE frame gives each the same value, and a
/// ROWS frame takes them one at a time.
#[tokio::test]
async fn a_peer_group_of_several_takes_leaves_together() {
    let bucket = MemoryBucket::new();
    let mut run = Run::new(RUNNING, &bucket, 1, 1, 1, &[]).await;
    let n = TAKE * 2 + 500;
    let input: Vec<Row> = (0..n)
        .map(|i| (u64::try_from(i).unwrap(), "eu", 1, SEC))
        .chain([(u64::try_from(n).unwrap(), "eu", 1, 2 * SEC)])
        .collect();
    run.rows(&input).await;
    run.advance(i64::MAX).await;
    assert_eq!(run.given.len(), n + 1);
    for (i, row) in input.iter().enumerate().take(n) {
        let got = &run.given[&id(row.0)];
        let n3 = (i + 1).min(3);
        assert!(got.contains(&format!(" total={n} n3={n3} ")), "{got}");
    }
    let last = &run.given[&id(u64::try_from(n).unwrap())];
    assert!(last.contains(&format!(" total={} ", n + 1)), "{last}");
}

/// An aggregate's argument runs only on the rows its FILTER takes: a row
/// it leaves out, whose argument would fail, keeps its place and leaves,
/// as it does in a group window.
#[tokio::test]
async fn an_argument_runs_only_where_its_filter_takes_the_row() {
    let sql = "INSERT INTO o SELECT region, amount, \
               sum(10 / amount) FILTER (WHERE amount <> 0) OVER (PARTITION BY region ORDER BY at) AS tenths FROM orders";
    let bucket = MemoryBucket::new();
    let mut run = Run::new(sql, &bucket, 1, 1, 1, &[]).await;
    run.rows(&[(1, "eu", 0, SEC), (2, "eu", 5, 2 * SEC)]).await;
    run.advance(i64::MAX).await;
    assert_eq!(run.rejected, 0);
    let first = &run.given[&id(1)];
    let second = &run.given[&id(2)];
    assert!(first.contains(" tenths= "), "{first}");
    assert!(second.contains(" tenths=2 "), "{second}");
}

/// A FILTER that fails on every row that leaves quarantines each of them,
/// with why, and gives no row.
#[tokio::test]
async fn a_filter_that_fails_on_every_row_quarantines_them() {
    let sql = "INSERT INTO o SELECT region, amount, \
               count(*) FILTER (WHERE 10 / amount > 1) OVER (PARTITION BY region ORDER BY at) AS n FROM orders";
    let bucket = MemoryBucket::new();
    let mut run = Run::new(sql, &bucket, 1, 1, 1, &[]).await;
    run.rows(&[(1, "eu", 0, SEC), (2, "eu", 0, 2 * SEC)]).await;
    let (out, more) = run.ops[0]
        .advance(&mut run.layers[0], i64::MAX)
        .await
        .unwrap();
    assert!(!more && out.rows.is_none());
    assert_eq!(out.failed.len(), 2, "{:?}", out.failed);
    assert!(out.failed.iter().all(|(_, _, c)| c.code == Code::Statement));
}

/// Rows that waited through a change of the stream's columns, as when the
/// source added one that passes through, leave under the plan's schema
/// now, with nulls in the new column: the operator's ID stays.
#[tokio::test]
async fn rows_written_before_a_column_was_added_leave_with_it() {
    let sql = "INSERT INTO o SELECT *, sum(amount) OVER (PARTITION BY region ORDER BY at) AS total FROM orders";
    let bucket = MemoryBucket::new();
    let range = KeyGroupRange::new(0, GROUPS - 1);
    let (before, mut op) = planned(sql, false);
    let mut layer = Layer::open(&bucket, 1, 1, range, &[]).await.unwrap();
    let (b, i) = batch(&op, &[(1, "eu", 5, SEC), (2, "eu", 7, 2 * SEC)]);
    op.rows(&mut layer, &b, &i).await.unwrap();
    let snaps = layer.barrier(1).await.unwrap();
    let (after, mut op) = planned(sql, true);
    assert_eq!(before.id, after.id);
    assert_ne!(before.rows, after.rows);
    let mut layer = Layer::open(&bucket, 2, 2, range, &snaps).await.unwrap();
    let (out, more) = op.advance(&mut layer, i64::MAX).await.unwrap();
    assert!(!more);
    let t = out.rows.unwrap();
    assert_eq!(t.batch.num_rows(), 2);
    let note = t.batch.column_by_name("note").unwrap();
    assert_eq!(arrow_array::Array::null_count(note.as_ref()), 2);
    let total = t.batch.column_by_name("total").unwrap();
    let totals: Vec<String> = (0..2)
        .map(|r| arrow_cast::display::array_value_to_string(total, r).unwrap())
        .collect();
    assert_eq!(totals, ["5", "12"]);
}

/// Any arrival order within the watermark's bound, any cut into batches,
/// barriers and restores onto another number of lanes anywhere give each
/// row the values it has when rows come in order on one lane: rows of a
/// time leave in their IDs' order whatever order they came in.
#[tokio::test]
async fn each_rows_values_dont_depend_on_order_barriers_or_lanes() {
    let queries = [
        RUNNING,
        "INSERT INTO o SELECT region, amount, at, avg(amount) OVER (PARTITION BY region ORDER BY at RANGE BETWEEN INTERVAL '30 seconds' PRECEDING AND CURRENT ROW) AS a, \
         min(amount) OVER (PARTITION BY region ORDER BY at ROWS UNBOUNDED PRECEDING) AS lo, \
         max(amount) FILTER (WHERE amount > 10) OVER (PARTITION BY region ORDER BY at ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS hi, \
         lag(amount, 2, -1) OVER (PARTITION BY region ORDER BY at) AS two \
         FROM orders",
        "INSERT INTO o SELECT region, amount, at, count(*) OVER (ORDER BY at) AS n, first_value(region) OVER (ORDER BY at) AS opening FROM orders",
    ];
    let (mut compared, mut restores) = (0, 0);
    for sql in queries {
        let input = rows(7, 300);
        let bucket = MemoryBucket::new();
        let mut want = Run::new(sql, &bucket, 1, 1, 1, &[]).await;
        let mut ordered = input.clone();
        ordered.sort_by_key(|r| (r.3, r.0));
        want.rows(&ordered).await;
        want.advance(i64::MAX).await;
        assert_eq!(want.given.len(), input.len(), "{sql}");
        for seed in 0..12u64 {
            let mut s = seed + 100;
            // Shuffled, with the watermark only ever at the least time of
            // the rows still to come: none is late.
            let mut shuffled = input.clone();
            for i in (1..shuffled.len()).rev() {
                let j = usize::try_from(next(&mut s) % (i as u64 + 1)).unwrap();
                shuffled.swap(i, j);
            }
            let bucket = MemoryBucket::new();
            let mut lanes = 1 + u32::try_from(next(&mut s) % 4).unwrap();
            let mut run = Run::new(sql, &bucket, 1, 1, lanes, &[]).await;
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
                    epoch += 1;
                    if next(&mut s).is_multiple_of(2) {
                        // The attempt dies after the record; another
                        // restores it on other lanes.
                        generation += 1;
                        restores += 1;
                        lanes = 1 + u32::try_from(next(&mut s) % 4).unwrap();
                        let given = std::mem::take(&mut run.given);
                        let watermark = run.ops[0].watermark;
                        run = Run::new(sql, &bucket, generation, epoch, lanes, &snaps).await;
                        run.given = given;
                        if watermark > i64::MIN {
                            run.advance(watermark).await;
                        }
                    }
                }
            }
            run.advance(i64::MAX).await;
            assert_eq!(run.rejected, 0, "{sql}, seed {seed}");
            assert_eq!(run.given, want.given, "{sql}, seed {seed}");
            compared += run.given.len();
        }
    }
    assert_eq!(compared, 3 * 12 * 300);
    assert!(restores > 12, "{restores} restores");
}
