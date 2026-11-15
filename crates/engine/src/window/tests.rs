//! The group-window operator on state's layers: what windows give and
//! when, lateness, sessions merging, and that the final rows don't depend
//! on how rows arrive, how barriers fall, or how many lanes restore them.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use openetl_normalize::key_groups;
use openetl_sql::{Catalog, Operator as Planned, Stream};
use openetl_state::{KeyGroupRange, Layer, MemoryBucket, Snapshot};

use super::Operator;

const GROUPS: u32 = 256;
const MIN: i64 = 60_000_000;

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

/// The operator of a statement over `orders`, whose event time is `at`.
fn operator(sql: &str, lateness: i64) -> Operator {
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
    let Planned::GroupWindow(g) = st.operator else {
        panic!("{sql}: not a group window");
    };
    let id = g.id.clone();
    Operator::new(
        &id,
        st.at,
        Arc::new(*g),
        &st.output,
        st.schema,
        lateness,
        GROUPS,
    )
    .unwrap()
}

type Row = (&'static str, i64, i64);

/// A batch of the statement's input: what it reads of `orders`' rows.
fn batch(op: &Operator, rows: &[Row]) -> RecordBatch {
    let full = RecordBatch::try_new(
        stream(),
        vec![
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(rows.iter().map(|r| r.2))
                    .with_timezone("UTC"),
            ),
        ],
    )
    .unwrap();
    op.plan.input.input(&full).unwrap()
}

fn ranges(lanes: u32) -> Vec<KeyGroupRange> {
    (0..lanes)
        .map(|i| KeyGroupRange::new(GROUPS * i / lanes, GROUPS * (i + 1) / lanes - 1))
        .collect()
}

/// A destination that merges: each key's row, as the output's rows,
/// applied in order, leave it.
#[derive(Debug, Default, PartialEq, Eq, Clone)]
struct Table(BTreeMap<String, String>);

impl Table {
    fn apply(&mut self, out: &super::Out, key: &[&str]) {
        let Some(t) = &out.rows else { return };
        let b = &t.batch;
        let ops: Vec<&str> = t
            .control
            .op
            .as_ref()
            .unwrap()
            .as_string::<i32>()
            .iter()
            .map(Option::unwrap)
            .collect();
        for (r, op) in ops.iter().enumerate() {
            let cell = |name: &str| {
                let c = b.column(b.schema().index_of(name).unwrap());
                arrow_cast::display::array_value_to_string(c, r).unwrap()
            };
            let k: Vec<String> = key.iter().map(|k| cell(k)).collect();
            let all: Vec<String> = b
                .schema()
                .fields()
                .iter()
                .map(|f| format!("{}={}", f.name(), cell(f.name())))
                .collect();
            match *op {
                "delete" => {
                    self.0.remove(&k.join("|"));
                }
                _ => {
                    self.0.insert(k.join("|"), all.join(" "));
                }
            }
        }
    }
}

/// Runs batches through the operator on lanes, routing each row to the
/// lane owning its key's group, with watermarks between them.
struct Run {
    ops: Vec<Operator>,
    layers: Vec<Layer>,
    table: Table,
    rejected: usize,
    key: Vec<&'static str>,
}

impl Run {
    async fn new(
        sql: &str,
        lateness: i64,
        bucket: &MemoryBucket,
        generation: u64,
        epoch: u64,
        lanes: u32,
        snapshots: &[Snapshot],
    ) -> Run {
        let mut ops = Vec::new();
        let mut layers = Vec::new();
        for r in ranges(lanes) {
            ops.push(operator(sql, lateness));
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
        }
        Run {
            ops,
            layers,
            table: Table::default(),
            rejected: 0,
            key: vec!["window_start", "region"],
        }
    }

    async fn rows(&mut self, rows: &[Row]) {
        let all = batch(&self.ops[0], rows);
        let need = self.ops[0].plan.need.clone();
        let groups = key_groups(&all, &need, GROUPS).unwrap();
        for (i, l) in self.layers.iter_mut().enumerate() {
            let mine: Vec<u32> = groups
                .iter()
                .enumerate()
                .filter(|(_, g)| l.range().contains(**g))
                .map(|(r, _)| r as u32)
                .collect();
            if mine.is_empty() {
                continue;
            }
            let part =
                arrow_select::take::take_record_batch(&all, &arrow_array::UInt32Array::from(mine))
                    .unwrap();
            let out = self.ops[i].rows(l, &part).await.unwrap();
            self.rejected += out.rejected.len();
            self.table.apply(&out, &self.key);
        }
    }

    async fn advance(&mut self, t: i64) {
        for (op, l) in self.ops.iter_mut().zip(&mut self.layers) {
            let out = op.advance(l, t).await.unwrap();
            assert!(out.failed.is_empty(), "{:?}", out.failed);
            self.table.apply(&out, &self.key);
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

const TUMBLE: &str = "INSERT INTO o SELECT window_start, window_end, region, sum(amount) AS total, count(*) AS n, max(amount) AS hi \
     FROM orders GROUP BY tumble(at, INTERVAL '1 minute'), region";

#[tokio::test]
async fn a_window_gives_its_row_once_the_watermark_reaches_its_end() {
    let bucket = MemoryBucket::new();
    let mut run = Run::new(TUMBLE, 0, &bucket, 1, 1, 1, &[]).await;
    run.rows(&[
        ("eu", 5, 1),
        ("us", 7, 2),
        ("eu", 3, MIN - 1),
        ("eu", 4, MIN),
    ])
    .await;
    run.advance(MIN - 1).await;
    assert!(run.table.0.is_empty(), "{:?}", run.table);
    run.advance(MIN).await;
    assert_eq!(
        run.table.0.values().cloned().collect::<Vec<_>>(),
        [
            "window_start=1970-01-01T00:00:00Z window_end=1970-01-01T00:01:00Z region=eu total=8 n=2 hi=5",
            "window_start=1970-01-01T00:00:00Z window_end=1970-01-01T00:01:00Z region=us total=7 n=1 hi=7",
        ]
    );
    run.advance(i64::MAX).await;
    assert_eq!(run.table.0.len(), 3);
    // Past the watermark, without lateness, a row is late.
    run.rows(&[("eu", 1, 30)]).await;
    assert_eq!(run.rejected, 1);
    assert_eq!(run.table.0.len(), 3);
}

#[tokio::test]
async fn hops_put_a_row_in_each_window_it_falls_in() {
    let bucket = MemoryBucket::new();
    let sql = "INSERT INTO o SELECT window_start, region, count(*) AS n FROM orders \
               GROUP BY hop(at, INTERVAL '1 minute', INTERVAL '3 minutes'), region";
    let mut run = Run::new(sql, 0, &bucket, 1, 1, 2, &[]).await;
    run.rows(&[("eu", 1, 2 * MIN + 5), ("eu", 1, 3 * MIN)])
        .await;
    run.advance(i64::MAX).await;
    let n: Vec<String> = run
        .table
        .0
        .values()
        .map(|v| v.split(' ').next_back().unwrap().to_owned())
        .collect();
    // Windows from minute 0 to 3 hold the first; from 1 to 3 the second.
    assert_eq!(n, ["n=1", "n=2", "n=2", "n=1"]);
}

/// A row late within the allowed lateness gives its window's row again,
/// as an update; later than that, it's quarantined.
#[tokio::test]
async fn lateness_gives_a_window_again_until_it_passes() {
    let bucket = MemoryBucket::new();
    let mut run = Run::new(TUMBLE, MIN, &bucket, 1, 1, 1, &[]).await;
    run.rows(&[("eu", 5, 1)]).await;
    run.advance(MIN).await;
    assert!(run.table.0.values().next().unwrap().contains("total=5 n=1"));
    run.rows(&[("eu", 2, 10)]).await;
    assert!(run.table.0.values().next().unwrap().contains("total=7 n=2"));
    assert_eq!(run.rejected, 0);
    run.advance(2 * MIN).await;
    run.rows(&[("eu", 100, 20)]).await;
    assert_eq!(run.rejected, 1);
    assert!(run.table.0.values().next().unwrap().contains("total=7 n=2"));
}

/// A window that no longer passes HAVING leaves the output.
#[tokio::test]
async fn a_window_given_again_that_fails_having_is_deleted() {
    let bucket = MemoryBucket::new();
    let sql = "INSERT INTO o SELECT window_start, region, sum(amount) AS total FROM orders \
               GROUP BY tumble(at, INTERVAL '1 minute'), region HAVING sum(amount) > 0";
    let mut run = Run::new(sql, MIN, &bucket, 1, 1, 1, &[]).await;
    run.rows(&[("eu", 5, 1), ("us", -1, 1)]).await;
    run.advance(MIN).await;
    assert_eq!(run.table.0.len(), 1);
    run.rows(&[("eu", -9, 2), ("us", 4, 2)]).await;
    assert_eq!(run.table.0.len(), 1);
    assert!(run.table.0.keys().next().unwrap().contains("us"));
}

/// A row an aggregate's FILTER leaves out never evaluates its argument,
/// so it still counts for the others.
#[tokio::test]
async fn a_filter_keeps_rows_it_leaves_out_from_its_argument() {
    let bucket = MemoryBucket::new();
    let sql = "INSERT INTO o SELECT window_start, region, sum(10 / amount) FILTER (WHERE amount <> 0) AS s, count(*) AS n \
               FROM orders GROUP BY tumble(at, INTERVAL '1 minute'), region";
    let mut run = Run::new(sql, 0, &bucket, 1, 1, 1, &[]).await;
    run.rows(&[("eu", 0, 1), ("eu", 2, 2)]).await;
    run.advance(i64::MAX).await;
    assert_eq!(run.rejected, 0);
    let v = run.table.0.values().next().unwrap();
    assert!(v.ends_with("s=5 n=2"), "{v}");
}

/// A window with no aggregates gives its row as any other does.
#[tokio::test]
async fn a_window_without_aggregates_gives_its_rows() {
    let bucket = MemoryBucket::new();
    let sql = "INSERT INTO o SELECT window_start, region FROM orders \
               GROUP BY tumble(at, INTERVAL '1 minute'), region";
    let mut run = Run::new(sql, 0, &bucket, 1, 1, 1, &[]).await;
    run.rows(&[("eu", 1, 1), ("us", 1, 2), ("eu", 1, MIN)])
        .await;
    run.advance(i64::MAX).await;
    assert_eq!(run.table.0.len(), 3, "{:?}", run.table);
}

/// Windows that close together keep or drop their state each by its own
/// lateness, even when one's row can't be given: the other's is written
/// back, so a late row gives it again as an update.
#[tokio::test]
async fn windows_closing_together_keep_their_own_state() {
    let bucket = MemoryBucket::new();
    let sql = "INSERT INTO o SELECT window_start, region, sum(amount) AS total FROM orders \
               GROUP BY tumble(at, INTERVAL '1 minute'), region";
    let mut run = Run::new(sql, MIN, &bucket, 1, 1, 1, &[]).await;
    // Window A, at minute 0, overflows; window B is at minute 1.
    run.rows(&[("eu", i64::MAX, 1), ("eu", i64::MAX, 2), ("us", 5, MIN + 1)])
        .await;
    // A is past its lateness now, and B within it.
    let out = run.ops[0]
        .advance(&mut run.layers[0], 2 * MIN + 1)
        .await
        .unwrap();
    assert_eq!(out.failed.len(), 1, "{:?}", out.failed);
    let late = batch(&run.ops[0], &[("us", 1, MIN + 2)]);
    let out = run.ops[0].rows(&mut run.layers[0], &late).await.unwrap();
    let t = out.rows.unwrap();
    let ops: Vec<Option<&str>> = t
        .control
        .op
        .as_ref()
        .unwrap()
        .as_string::<i32>()
        .iter()
        .collect();
    assert_eq!(ops, [Some("update")]);
    let total = t.batch.column(t.batch.schema().index_of("total").unwrap());
    assert_eq!(
        arrow_cast::display::array_value_to_string(total, 0).unwrap(),
        "6"
    );
    // A was forgotten, and past the end of input so is B.
    run.ops[0]
        .advance(&mut run.layers[0], i64::MAX)
        .await
        .unwrap();
    run.layers[0].barrier(1).await.unwrap();
    assert_eq!(run.layers[0].memory().store, 0);
}

const SESSION: &str = "INSERT INTO o SELECT window_start, window_end, region, sum(amount) AS total \
     FROM orders GROUP BY session(at, INTERVAL '30 seconds'), region";
const SEC: i64 = 1_000_000;

#[tokio::test]
async fn sessions_merge_as_rows_meet() {
    let bucket = MemoryBucket::new();
    let mut run = Run::new(SESSION, 0, &bucket, 1, 1, 1, &[]).await;
    run.rows(&[("eu", 1, 0), ("eu", 2, 10 * SEC), ("eu", 4, 100 * SEC)])
        .await;
    run.rows(&[("eu", 8, 70 * SEC)]).await;
    // 10s and 70s are 60s apart; 70s and 100s, 30s: those two meet.
    run.advance(i64::MAX).await;
    assert_eq!(
        run.table.0.values().cloned().collect::<Vec<_>>(),
        [
            "window_start=1970-01-01T00:00:00Z window_end=1970-01-01T00:00:40Z region=eu total=3",
            "window_start=1970-01-01T00:01:10Z window_end=1970-01-01T00:02:10Z region=eu total=12",
        ]
    );
    // A row that bridges two sessions merges them.
    let mut run = Run::new(SESSION, 0, &bucket, 2, 1, 1, &[]).await;
    run.rows(&[("eu", 1, 0), ("eu", 2, 60 * SEC), ("eu", 4, 30 * SEC)])
        .await;
    run.advance(i64::MAX).await;
    assert_eq!(run.table.0.len(), 1);
    assert!(run.table.0.values().next().unwrap().ends_with("total=7"));
}

/// Within lateness, a late row that moves a session's start deletes the
/// row the session had, and gives it under its new start.
#[tokio::test]
async fn a_late_row_that_moves_a_sessions_start_deletes_its_old_row() {
    let bucket = MemoryBucket::new();
    let mut run = Run::new(SESSION, MIN, &bucket, 1, 1, 1, &[]).await;
    run.rows(&[("eu", 1, 40 * SEC)]).await;
    run.advance(80 * SEC).await;
    assert!(
        run.table
            .0
            .keys()
            .next()
            .unwrap()
            .starts_with("1970-01-01T00:00:40Z")
    );
    run.rows(&[("eu", 2, 20 * SEC)]).await;
    assert_eq!(run.table.0.len(), 1);
    let (k, v) = run.table.0.iter().next().unwrap();
    assert!(k.starts_with("1970-01-01T00:00:20Z"), "{k}");
    assert!(v.ends_with("total=3"), "{v}");
}

/// A late row that bridges two closed sessions deletes the row of the one
/// merged away, under a count, whose column can't be null.
#[tokio::test]
async fn a_session_merged_away_is_deleted_under_a_count() {
    let bucket = MemoryBucket::new();
    let sql = "INSERT INTO o SELECT window_start, region, count(*) AS n FROM orders \
               GROUP BY session(at, INTERVAL '30 seconds'), region";
    let mut run = Run::new(sql, 2 * MIN, &bucket, 1, 1, 1, &[]).await;
    run.rows(&[("eu", 1, 0), ("eu", 1, 60 * SEC)]).await;
    run.advance(100 * SEC).await;
    assert_eq!(run.table.0.len(), 2, "{:?}", run.table);
    run.rows(&[("eu", 1, 30 * SEC)]).await;
    assert_eq!(run.rejected, 0);
    assert_eq!(
        run.table.0.values().cloned().collect::<Vec<_>>(),
        ["window_start=1970-01-01T00:00:00Z region=eu n=3"]
    );
}

/// A session reopened by a late row has values no row of it was given
/// with. Merged away, its delete gives the values its row has, which its
/// projection took once, so the delete isn't lost.
#[tokio::test]
async fn a_reopened_session_merged_away_deletes_the_row_it_gave() {
    let bucket = MemoryBucket::new();
    let sql = "INSERT INTO o SELECT window_start, region, 10 / sum(amount) AS r FROM orders \
               GROUP BY session(at, INTERVAL '30 seconds'), region";
    let mut run = Run::new(sql, 2 * MIN, &bucket, 1, 1, 1, &[]).await;
    run.rows(&[("eu", 1, 0), ("eu", 1, 60 * SEC)]).await;
    run.advance(100 * SEC).await;
    assert_eq!(run.table.0.len(), 2, "{:?}", run.table);
    // The second session reopens, to 115 seconds, with a sum of 0.
    run.rows(&[("eu", -1, 85 * SEC)]).await;
    // Then a row bridges the two.
    run.rows(&[("eu", 1, 30 * SEC)]).await;
    assert_eq!(
        run.table.0.keys().cloned().collect::<Vec<_>>(),
        ["1970-01-01T00:00:00Z|eu"]
    );
    run.advance(i64::MAX).await;
    assert_eq!(
        run.table.0.values().cloned().collect::<Vec<_>>(),
        ["window_start=1970-01-01T00:00:00Z region=eu r=5"]
    );
}

/// A session that grew after its row was given, and then fails HAVING,
/// deletes the row the output has: with that row's end and values.
#[tokio::test]
async fn a_delete_gives_the_end_of_the_row_the_output_has() {
    let bucket = MemoryBucket::new();
    let sql = "INSERT INTO o SELECT window_start, window_end, region, sum(amount) AS total FROM orders \
               GROUP BY session(at, INTERVAL '30 seconds'), region HAVING sum(amount) > 0";
    let mut run = Run::new(sql, 2 * MIN, &bucket, 1, 1, 1, &[]).await;
    run.rows(&[("eu", 5, 0)]).await;
    run.advance(40 * SEC).await;
    assert_eq!(
        run.table.0.values().cloned().collect::<Vec<_>>(),
        ["window_start=1970-01-01T00:00:00Z window_end=1970-01-01T00:00:30Z region=eu total=5"]
    );
    // A late row grows the session to 50 seconds, and its sum below 0.
    run.rows(&[("eu", -9, 20 * SEC)]).await;
    let out = run.ops[0]
        .advance(&mut run.layers[0], 60 * SEC)
        .await
        .unwrap();
    let t = out.rows.unwrap();
    let cell = |name: &str| {
        let c = t.batch.column(t.batch.schema().index_of(name).unwrap());
        arrow_cast::display::array_value_to_string(c, 0).unwrap()
    };
    let ops: Vec<Option<&str>> = t
        .control
        .op
        .as_ref()
        .unwrap()
        .as_string::<i32>()
        .iter()
        .collect();
    assert_eq!(ops, [Some("delete")]);
    assert_eq!(cell("window_end"), "1970-01-01T00:00:30Z");
    assert_eq!(cell("total"), "5");
}

/// The ops and window ends of what a call gave.
fn ops_and_ends(out: super::Out) -> Vec<(String, String)> {
    let t = out.rows.unwrap();
    let ends = t
        .batch
        .column(t.batch.schema().index_of("window_end").unwrap())
        .clone();
    t.control
        .op
        .as_ref()
        .unwrap()
        .as_string::<i32>()
        .iter()
        .enumerate()
        .map(|(i, op)| {
            (
                op.unwrap().to_owned(),
                arrow_cast::display::array_value_to_string(&ends, i).unwrap(),
            )
        })
        .collect()
}

/// Without aggregates too, a session that grew after its row was given
/// deletes the row with the end it has: merged away, or failing HAVING.
#[tokio::test]
async fn a_delete_without_aggregates_gives_the_end_the_output_has() {
    let bucket = MemoryBucket::new();
    let sql = "INSERT INTO o SELECT window_start, window_end, region FROM orders \
               GROUP BY session(at, INTERVAL '30 seconds'), region";
    let mut run = Run::new(sql, 2 * MIN, &bucket, 1, 1, 1, &[]).await;
    run.rows(&[("eu", 1, 0), ("eu", 1, 60 * SEC)]).await;
    run.advance(100 * SEC).await;
    // The second session grows to 115 seconds; then a row bridges the two.
    run.rows(&[("eu", 1, 85 * SEC)]).await;
    let late = batch(&run.ops[0], &[("eu", 1, 30 * SEC)]);
    let out = run.ops[0].rows(&mut run.layers[0], &late).await.unwrap();
    assert_eq!(
        ops_and_ends(out),
        [("delete".to_owned(), "1970-01-01T00:01:30Z".to_owned())]
    );

    let sql = "INSERT INTO o SELECT window_start, window_end, region FROM orders \
               GROUP BY session(at, INTERVAL '30 seconds'), region \
               HAVING window_end - window_start < INTERVAL '40 seconds'";
    let mut run = Run::new(sql, 2 * MIN, &bucket, 2, 1, 1, &[]).await;
    run.rows(&[("eu", 1, 0)]).await;
    run.advance(40 * SEC).await;
    assert_eq!(run.table.0.len(), 1);
    // It grows to 50 seconds, which fails HAVING once it closes.
    run.rows(&[("eu", 1, 20 * SEC)]).await;
    let out = run.ops[0]
        .advance(&mut run.layers[0], 60 * SEC)
        .await
        .unwrap();
    assert_eq!(
        ops_and_ends(out),
        [("delete".to_owned(), "1970-01-01T00:00:30Z".to_owned())]
    );
}

fn next(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *seed;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

const REGIONS: [&str; 5] = ["eu", "us", "apac", "latam", "mena"];

/// Rows over ten minutes, each with an amount, its time a microsecond
/// apart from the next so that sessions vary.
fn rows(seed: u64, n: usize) -> Vec<Row> {
    let mut s = seed;
    (0..n)
        .map(|_| {
            let r = REGIONS[usize::try_from(next(&mut s) % 5).unwrap()];
            let a = i64::try_from(next(&mut s) % 100).unwrap() - 20;
            let t = i64::try_from(next(&mut s) % 600).unwrap() * SEC;
            (r, a, t)
        })
        .collect()
}

/// Any arrival order, any cut into batches, barriers and restores onto
/// another number of lanes anywhere, and watermarks that never make a row
/// late, give the same final rows.
#[tokio::test]
async fn the_final_rows_dont_depend_on_order_barriers_or_lanes() {
    let queries = [
        (TUMBLE, vec!["window_start", "region"]),
        (
            "INSERT INTO o SELECT window_start, region, avg(amount) AS a, min(amount) AS lo, approx_distinct(amount) AS d, median(amount) AS m \
             FROM orders GROUP BY hop(at, INTERVAL '1 minute', INTERVAL '2 minutes'), region HAVING count(*) > 1",
            vec!["window_start", "region"],
        ),
        (SESSION, vec!["window_start", "region"]),
        (
            "INSERT INTO o SELECT window_start, sum(amount) AS total FROM orders GROUP BY tumble(at, INTERVAL '2 minutes')",
            vec!["window_start"],
        ),
    ];
    for (sql, key) in queries {
        let input = rows(7, 300);
        let bucket = MemoryBucket::new();
        let mut want = Run::new(sql, 0, &bucket, 1, 1, 1, &[]).await;
        want.key = key.clone();
        want.rows(&input).await;
        want.advance(i64::MAX).await;
        assert!(!want.table.0.is_empty());
        for seed in 0..12u64 {
            let mut s = seed + 100;
            // Shuffled within each split of time: no row is ever late,
            // since the watermark only passes times before every row left.
            let mut shuffled = input.clone();
            for i in (1..shuffled.len()).rev() {
                let j = usize::try_from(next(&mut s) % (i as u64 + 1)).unwrap();
                shuffled.swap(i, j);
            }
            let bucket = MemoryBucket::new();
            let mut lanes = 1 + u32::try_from(next(&mut s) % 4).unwrap();
            let mut run = Run::new(sql, 0, &bucket, 1, 1, lanes, &[]).await;
            run.key = key.clone();
            let mut epoch = 1;
            let mut generation = 1;
            let mut at = 0;
            while at < shuffled.len() {
                let n = 1 + usize::try_from(next(&mut s) % 40).unwrap();
                let part = &shuffled[at..(at + n).min(shuffled.len())];
                at += part.len();
                run.rows(part).await;
                let left = shuffled[at..].iter().map(|r| r.2).min().unwrap_or(i64::MAX);
                if next(&mut s).is_multiple_of(3) && left > i64::MIN {
                    run.advance(left.min(i64::MAX - 1)).await;
                }
                if next(&mut s).is_multiple_of(4) {
                    let snaps = run.barrier(epoch).await;
                    epoch += 1;
                    if next(&mut s).is_multiple_of(2) {
                        // The attempt dies after the record; another
                        // restores it on other lanes.
                        generation += 1;
                        lanes = 1 + u32::try_from(next(&mut s) % 4).unwrap();
                        let table = run.table.clone();
                        let watermark = run.ops[0].watermark;
                        run = Run::new(sql, 0, &bucket, generation, epoch, lanes, &snaps).await;
                        run.key = key.clone();
                        run.table = table;
                        run.advance(watermark).await;
                    }
                }
            }
            run.advance(i64::MAX).await;
            assert_eq!(run.rejected, 0, "{sql}, seed {seed}");
            assert_eq!(run.table, want.table, "{sql}, seed {seed}");
        }
    }
}
