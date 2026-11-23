//! The group-window operator on state's layers: what windows give and
//! when, lateness, sessions merging, that the final rows don't depend on
//! how rows arrive, how barriers fall, or how many lanes restore them, and
//! that what its snapshots name stays bounded as windows close.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use openetl_normalize::key_groups;
use openetl_sql::{Catalog, Operator as Planned, Stream};
use openetl_state::runs::{Counts, runs};
use openetl_state::{
    KeyGroupRange, Layer, MemoryBucket, ObjectStorage, RunConfig, RunStores, Snapshot, Stores,
};

use super::Operator;

const GROUPS: u32 = 256;
const MIN: i64 = 60_000_000;

pub(crate) fn stream() -> SchemaRef {
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

pub(crate) type Row = (&'static str, i64, i64);

/// A batch of `orders`, of rows of a region, an amount and a time.
pub(crate) fn orders(rows: impl Iterator<Item = Row> + Clone) -> RecordBatch {
    RecordBatch::try_new(
        stream(),
        vec![
            Arc::new(StringArray::from_iter_values(rows.clone().map(|r| r.0))) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(rows.clone().map(|r| r.1))),
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(rows.map(|r| r.2)).with_timezone("UTC"),
            ),
        ],
    )
    .unwrap()
}

/// A batch of the statement's input: what it reads of `orders`' rows.
fn batch(op: &Operator, rows: &[Row]) -> RecordBatch {
    op.plan.input.input(&orders(rows.iter().copied())).unwrap()
}

/// Each lane's rows of `all`, of the key groups of `need` in its range, as
/// indexes; None for a lane with none.
pub(crate) fn routed(
    all: &RecordBatch,
    need: &[String],
    ranges: &[KeyGroupRange],
) -> Vec<Option<arrow_array::UInt32Array>> {
    let groups = key_groups(all, need, GROUPS).unwrap();
    ranges
        .iter()
        .map(|range| {
            let mine: Vec<u32> = groups
                .iter()
                .enumerate()
                .filter(|(_, g)| range.contains(**g))
                .map(|(r, _)| r as u32)
                .collect();
            (!mine.is_empty()).then(|| arrow_array::UInt32Array::from(mine))
        })
        .collect()
}

pub(crate) fn ranges(lanes: u32) -> Vec<KeyGroupRange> {
    (0..lanes)
        .map(|i| KeyGroupRange::new(GROUPS * i / lanes, GROUPS * (i + 1) / lanes - 1))
        .collect()
}

/// A destination that merges: each key's row, as the output's rows,
/// applied in order, leave it.
#[derive(Debug, Default, PartialEq, Eq, Clone)]
struct Table(BTreeMap<String, String>);

impl Table {
    /// Applies what `out` gave, and gives the keys of its rows.
    fn apply(&mut self, out: &super::Out, key: &[&str]) -> Vec<String> {
        let mut written = Vec::new();
        let Some(t) = &out.rows else {
            return written;
        };
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
            written.push(k.join("|"));
        }
        written
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
    /// How many rows the output was given of each key.
    writes: BTreeMap<String, usize>,
}

impl Run {
    async fn new(
        sql: &str,
        lateness: i64,
        bucket: &dyn Stores,
        generation: u64,
        epoch: u64,
        lanes: u32,
        snapshots: &[Snapshot],
    ) -> Run {
        let mut ops = Vec::new();
        let mut layers = Vec::new();
        for r in ranges(lanes) {
            let op = operator(sql, lateness);
            let mine: Vec<Snapshot> = snapshots
                .iter()
                .filter(|s| s.range.overlaps(&r))
                .cloned()
                .collect();
            let mut layer = Layer::open(bucket, generation, epoch, r, &mine)
                .await
                .unwrap();
            op.open(&mut layer);
            ops.push(op);
            layers.push(layer);
        }
        Run {
            ops,
            layers,
            table: Table::default(),
            rejected: 0,
            key: vec!["window_start", "region"],
            writes: BTreeMap::new(),
        }
    }

    fn give(&mut self, out: &super::Out) {
        for k in self.table.apply(out, &self.key) {
            *self.writes.entry(k).or_default() += 1;
        }
    }

    async fn rows(&mut self, rows: &[Row]) {
        let all = batch(&self.ops[0], rows);
        let ranges: Vec<KeyGroupRange> = self.layers.iter().map(Layer::range).collect();
        for (i, mine) in routed(&all, &self.ops[0].plan.need, &ranges)
            .into_iter()
            .enumerate()
        {
            let Some(mine) = mine else { continue };
            let part = arrow_select::take::take_record_batch(&all, &mine).unwrap();
            let out = self.ops[i]
                .rows(&mut self.layers[i], &part, None)
                .await
                .unwrap();
            self.rejected += out.rejected.len();
            self.give(&out);
        }
    }

    async fn advance(&mut self, t: i64) {
        for i in 0..self.ops.len() {
            self.advance_lane(i, t).await;
        }
    }

    async fn advance_lane(&mut self, i: usize, t: i64) {
        let out = self.ops[i].advance(&mut self.layers[i], t).await.unwrap();
        assert!(out.failed.is_empty(), "{:?}", out.failed);
        self.give(&out);
    }

    async fn barrier(&mut self, epoch: u64) -> Vec<Snapshot> {
        let mut out = Vec::new();
        for l in &mut self.layers {
            out.extend(l.barrier(epoch).await.unwrap());
        }
        out
    }

    /// The windows its lanes began keeping less those they stopped keeping,
    /// since the last call.
    fn kept(&mut self) -> i64 {
        let mut n = 0;
        for op in &mut self.ops {
            n += i64::try_from(std::mem::take(&mut op.opened)).unwrap();
            n -= i64::try_from(std::mem::take(&mut op.dropped)).unwrap();
        }
        n
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

/// What rows can tell apart of a watermark: without lateness, the latest
/// end of a tumbling or hopping window at or before it, as a timer there
/// fires; a session's, or with lateness, the watermark itself.
#[test]
fn rows_tell_a_watermark_apart_by_the_windows_ends() {
    let tumble = operator(TUMBLE, 0);
    assert_eq!(
        [MIN - 1, MIN, MIN + 5, -1].map(|w| tumble.level(w)),
        [0, MIN, MIN, -MIN]
    );
    assert_eq!(
        [i64::MIN, i64::MAX].map(|w| tumble.level(w)),
        [i64::MIN, i64::MAX]
    );
    let hop = |size: &str| {
        operator(
            &format!(
                "INSERT INTO o SELECT window_start, region, count(*) AS n FROM orders \
                 GROUP BY hop(at, INTERVAL '1 minute', INTERVAL '{size}'), region"
            ),
            0,
        )
    };
    // Ends a minute apart, at whole minutes, or half past.
    assert_eq!(hop("3 minutes").level(2 * MIN + 5), 2 * MIN);
    assert_eq!(hop("90 seconds").level(2 * MIN + 5), 3 * MIN / 2);
    assert_eq!(operator(TUMBLE, 30_000_000).level(MIN + 5), MIN + 5);
    let session = operator(
        "INSERT INTO o SELECT window_start, region, count(*) AS n FROM orders \
         GROUP BY session(at, INTERVAL '1 minute'), region",
        0,
    );
    assert_eq!(session.level(MIN + 5), MIN + 5);
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

impl Run {
    /// The attempt dies after the barrier of `epoch`, and another, of
    /// `generation`, restores its record on `lanes` lanes with `lateness`,
    /// at the watermark `at`: it goes on with the output as this one left
    /// it.
    async fn restore(
        &self,
        sql: &str,
        lateness: i64,
        stores: &dyn Stores,
        (generation, epoch, lanes): (u64, u64, u32),
        snaps: &[Snapshot],
        at: i64,
    ) -> Run {
        let mut back = Run::new(sql, lateness, stores, generation, epoch + 1, lanes, snaps).await;
        (back.table, back.writes, back.rejected) =
            (self.table.clone(), self.writes.clone(), self.rejected);
        back.key = self.key.clone();
        back.advance(at).await;
        back
    }

    /// The windows its lanes stopped keeping since their callers last
    /// took them.
    fn dropped(&self) -> u64 {
        self.ops.iter().map(|o| o.dropped).sum()
    }
}

/// A window gone under the lateness it had stays gone when the lateness is
/// raised: a row of it within the new lateness is late, and the window's
/// row stays as it was. A window open at the change takes the new lateness,
/// and a row of it within that updates its row.
#[tokio::test]
async fn a_window_gone_before_a_raised_lateness_stays_gone() {
    let bucket = MemoryBucket::new();
    let mut run = Run::new(TUMBLE, 0, &bucket, 1, 1, 1, &[]).await;
    run.rows(&[("eu", 5, 1), ("eu", 2, 10), ("eu", 3, MIN + 10 * SEC)])
        .await;
    run.advance(MIN + 5).await;
    let first = "1970-01-01T00:00:00Z|eu";
    let second = "1970-01-01T00:01:00Z|eu";
    assert!(
        run.table.0[first].contains("total=7 n=2"),
        "{:?}",
        run.table
    );
    let snaps = run.barrier(1).await;
    let mut run = run
        .restore(TUMBLE, 2 * MIN, &bucket, (2, 1, 2), &snaps, MIN + 5)
        .await;
    // Within the new lateness of the first minute, which had gone.
    run.rows(&[("eu", 4, 20)]).await;
    assert_eq!(run.rejected, 1);
    assert!(
        run.table.0[first].contains("total=7 n=2"),
        "{:?}",
        run.table
    );
    // The second minute was open at the change: it keeps its state for
    // the new lateness once it closes.
    run.advance(2 * MIN + 5).await;
    assert!(
        run.table.0[second].contains("total=3 n=1"),
        "{:?}",
        run.table
    );
    run.rows(&[("eu", 6, MIN + 20 * SEC)]).await;
    run.advance(4 * MIN - 1).await;
    run.rows(&[("eu", 1, MIN + 30 * SEC)]).await;
    assert_eq!(run.rejected, 1);
    assert!(
        run.table.0[second].contains("total=10 n=3"),
        "{:?}",
        run.table
    );
    run.advance(4 * MIN).await;
    run.rows(&[("eu", 1, MIN + 40 * SEC)]).await;
    assert_eq!(run.rejected, 2);
    run.advance(i64::MAX).await;
    assert_eq!(run.writes[first], 1, "{:?}", run.writes);
    assert_eq!(run.writes[second], 3, "{:?}", run.writes);
    assert_eq!(run.table.0.len(), 2);
}

/// A row that would give a live session its start, where its own gap has
/// gone, is late, with an unchanged lateness too: the start is a gone
/// session's, whose row the output keeps as it was.
#[tokio::test]
async fn a_row_that_would_take_a_gone_sessions_start_is_late() {
    let bucket = MemoryBucket::new();
    let mut run = Run::new(SESSION, 0, &bucket, 1, 1, 1, &[]).await;
    run.rows(&[("eu", 5, 0)]).await;
    run.advance(40 * SEC).await;
    run.rows(&[("eu", 2, 20 * SEC)]).await;
    assert_eq!(run.rejected, 0);
    run.rows(&[("eu", 4, 0)]).await;
    assert_eq!(run.rejected, 1);
    // One that moves a live session's start where its gap reaches past
    // the floor still does.
    run.rows(&[("eu", 3, 15 * SEC)]).await;
    assert_eq!(run.rejected, 1);
    run.advance(i64::MAX).await;
    assert_eq!(
        run.table.0.values().cloned().collect::<Vec<_>>(),
        [
            "window_start=1970-01-01T00:00:00Z window_end=1970-01-01T00:00:30Z region=eu total=5",
            "window_start=1970-01-01T00:00:15Z window_end=1970-01-01T00:00:50Z region=eu total=5",
        ]
    );
    assert_eq!(run.writes["1970-01-01T00:00:00Z|eu"], 1);
}

/// Where the floor is, after a raise: a tumbling window that ends at it is
/// gone, and a session is gone once its end, its last row's time plus the
/// gap, is before it. A row whose own gap ends at the floor may still give
/// a session its start, and one a microsecond earlier may not.
#[tokio::test]
async fn the_floor_is_a_windows_end_and_after_a_sessions() {
    let bucket = MemoryBucket::new();
    let mut run = Run::new(TUMBLE, 0, &bucket, 1, 1, 1, &[]).await;
    run.rows(&[("eu", 1, 1)]).await;
    run.advance(MIN).await;
    let snaps = run.barrier(1).await;
    let mut run = run
        .restore(TUMBLE, MIN, &bucket, (2, 1, 1), &snaps, MIN)
        .await;
    run.rows(&[("eu", 1, MIN - 1)]).await;
    assert_eq!(run.rejected, 1, "the minute ends at the floor");
    run.rows(&[("eu", 1, MIN)]).await;
    assert_eq!(run.rejected, 1, "the next minute ends after it");
    assert_eq!(run.writes["1970-01-01T00:00:00Z|eu"], 1);

    let mut run = Run::new(SESSION, 0, &bucket, 3, 1, 1, &[]).await;
    run.rows(&[("eu", 5, 0)]).await;
    run.advance(40 * SEC).await;
    let snaps = run.barrier(1).await;
    // The floor is at 40 seconds, and the lateness no longer decides.
    let mut run = run
        .restore(SESSION, 2 * MIN, &bucket, (4, 1, 1), &snaps, 40 * SEC)
        .await;
    run.rows(&[("eu", 2, 20 * SEC)]).await;
    run.rows(&[("eu", 1, 10 * SEC - 1)]).await;
    assert_eq!(run.rejected, 1, "its gap ends before the floor");
    run.rows(&[("eu", 3, 10 * SEC)]).await;
    assert_eq!(run.rejected, 1, "its gap ends at the floor");
    run.advance(i64::MAX).await;
    assert_eq!(
        run.table.0.values().cloned().collect::<Vec<_>>(),
        [
            "window_start=1970-01-01T00:00:00Z window_end=1970-01-01T00:00:30Z region=eu total=5",
            "window_start=1970-01-01T00:00:10Z window_end=1970-01-01T00:00:50Z region=eu total=5",
        ]
    );
}

/// Groups restored with horizons above the lane's watermark keep a floor
/// of their own: a window gone in one under a lowered lateness, at its
/// group's watermark, stays gone when a later attempt raises it.
#[tokio::test]
async fn each_groups_floor_follows_its_own_watermark() {
    let objects = Arc::new(ObjectStorage::new(
        Arc::new(object_store::memory::InMemory::new()),
        object_store::path::Path::from("state"),
    ));
    let stores = || RunStores::new(objects.clone(), RunConfig::default());
    let s = stores();
    let mut run = Run::new(TUMBLE, 2 * MIN, &s, 1, 1, 2, &[]).await;
    let op = &run.ops[0];
    let group = |region: &'static str| -> u32 {
        key_groups(&batch(op, &[(region, 1, 1)]), &op.plan.need, GROUPS).unwrap()[0]
    };
    const NAMES: [&str; 12] = [
        "eu", "us", "apac", "latam", "mena", "a", "b", "c", "d", "e", "f", "g",
    ];
    let lead = *NAMES.iter().find(|n| group(n) < GROUPS / 2).unwrap();
    let lag = *NAMES.iter().find(|n| group(n) >= GROUPS / 2).unwrap();
    run.rows(&[(lead, 1, 70 * SEC), (lag, 1, 70 * SEC)]).await;
    // The leading lane's second minute closes, within its lateness.
    run.advance_lane(0, 150 * SEC).await;
    run.advance_lane(1, 70 * SEC).await;
    let snaps = run.barrier(1).await;
    // Lowered to nothing, on one lane at the lagging watermark: the
    // leading group's minute is gone at its own, 150 seconds.
    let s = stores();
    let mut run = run
        .restore(TUMBLE, 0, &s, (2, 1, 1), &snaps, 70 * SEC)
        .await;
    run.rows(&[(lead, 1, 80 * SEC)]).await;
    assert_eq!(run.rejected, 1);
    let snaps = run.barrier(2).await;
    let s = stores();
    let mut run = run
        .restore(TUMBLE, 3 * MIN, &s, (3, 2, 2), &snaps, 70 * SEC)
        .await;
    run.rows(&[(lead, 1, 90 * SEC)]).await;
    assert_eq!(run.rejected, 2);
    run.advance(i64::MAX).await;
    let k = format!("1970-01-01T00:01:00Z|{lead}");
    assert!(
        run.table.0[&k].ends_with("total=1 n=1 hi=1"),
        "{:?}",
        run.table
    );
    assert_eq!(run.writes[&k], 1);
    assert_eq!(run.writes[&format!("1970-01-01T00:01:00Z|{lag}")], 1);
}

/// A window closed and kept at a raise takes the new lateness: its expiry
/// by the old one passes, and a row of it still updates its row.
#[tokio::test]
async fn a_window_kept_at_a_raised_lateness_takes_it() {
    let bucket = MemoryBucket::new();
    let mut run = Run::new(TUMBLE, MIN, &bucket, 1, 1, 1, &[]).await;
    run.rows(&[("eu", 5, 1)]).await;
    run.advance(MIN + 5).await;
    let snaps = run.barrier(1).await;
    let mut run = run
        .restore(TUMBLE, 3 * MIN, &bucket, (2, 1, 2), &snaps, MIN + 5)
        .await;
    run.advance(2 * MIN + 5).await;
    run.rows(&[("eu", 2, 10)]).await;
    let first = "1970-01-01T00:00:00Z|eu";
    assert!(
        run.table.0[first].contains("total=7 n=2"),
        "{:?}",
        run.table
    );
    run.advance(4 * MIN).await;
    run.rows(&[("eu", 2, 20)]).await;
    assert_eq!(run.rejected, 1);
    assert_eq!(run.writes[first], 2);
    assert_eq!(run.dropped(), 1);
}

/// The floor that keeps windows gone is in the window's snapshots by key
/// group, even when no window is left and they name no run. Kills between
/// the change and the late rows, with restores onto other numbers of
/// lanes, keep it; a window that ends after it takes rows by the new
/// lateness, and once the watermark less the new lateness passes it, the
/// floor no longer matters.
#[tokio::test]
async fn the_floor_survives_kills_onto_other_lanes() {
    let objects = Arc::new(ObjectStorage::new(
        Arc::new(object_store::memory::InMemory::new()),
        object_store::path::Path::from("state"),
    ));
    let stores = || RunStores::new(objects.clone(), RunConfig::default());
    let s = stores();
    let mut run = Run::new(TUMBLE, 0, &s, 1, 1, 2, &[]).await;
    let firsts: Vec<Row> = REGIONS.iter().map(|r| (*r, 1, 10 * SEC)).collect();
    run.rows(&firsts).await;
    run.advance(MIN + 5).await;
    assert_eq!(run.table.0.len(), REGIONS.len());
    let snaps = run.barrier(1).await;
    assert_eq!(snaps.len(), 2);
    assert!(
        snaps.iter().all(|sn| runs(sn).unwrap().is_empty()),
        "every window has gone"
    );
    // Raised, and killed before a row comes; then again on 1 lane.
    let s = stores();
    let mut run = run
        .restore(TUMBLE, 3 * MIN, &s, (2, 1, 3), &snaps, MIN + 5)
        .await;
    let snaps = run.barrier(2).await;
    assert_eq!(snaps.len(), 3);
    let s = stores();
    let mut run = run
        .restore(TUMBLE, 3 * MIN, &s, (3, 2, 1), &snaps, MIN + 5)
        .await;
    let lates: Vec<Row> = REGIONS.iter().map(|r| (*r, 7, 50 * SEC)).collect();
    run.rows(&lates).await;
    assert_eq!(run.rejected, REGIONS.len());
    // A window that ends after the floor, and none of whose rows came
    // before, takes one within the new lateness.
    run.advance(3 * MIN).await;
    run.rows(&[("eu", 2, MIN + SEC)]).await;
    assert_eq!(run.rejected, REGIONS.len());
    let snaps = run.barrier(3).await;
    let s = stores();
    let mut run = run
        .restore(TUMBLE, 3 * MIN, &s, (4, 3, 2), &snaps, 3 * MIN)
        .await;
    // The watermark less the new lateness has passed the floor: a row is
    // late by the lateness alone.
    run.advance(5 * MIN).await;
    run.rows(&[("eu", 1, 2 * MIN + SEC), ("eu", 1, 2 * MIN - 1)])
        .await;
    assert_eq!(run.rejected, REGIONS.len() + 1);
    run.advance(i64::MAX).await;
    assert_eq!(run.table.0.len(), REGIONS.len() + 2, "{:?}", run.table);
    for r in REGIONS {
        let k = format!("1970-01-01T00:00:00Z|{r}");
        assert_eq!(run.writes[&k], 1, "{k}");
        assert!(run.table.0[&k].contains("total=1 n=1"), "{:?}", run.table);
    }
    assert!(run.table.0["1970-01-01T00:02:00Z|eu"].contains("total=1 n=1"));
}

/// A lowered lateness takes rows only within itself, at once, and touches
/// no row the output has: a window past it keeps its row, and a row of it
/// is late. A session past it is dropped without a delete, and a row that
/// would have joined it makes a session of its own.
#[tokio::test]
async fn a_lowered_lateness_takes_rows_only_within_it() {
    let bucket = MemoryBucket::new();
    let mut run = Run::new(TUMBLE, 2 * MIN, &bucket, 1, 1, 1, &[]).await;
    run.rows(&[("eu", 5, 1), ("eu", 1, MIN + 1)]).await;
    run.advance(MIN + 5).await;
    run.rows(&[("eu", 2, 10)]).await;
    let first = "1970-01-01T00:00:00Z|eu";
    assert!(
        run.table.0[first].contains("total=7 n=2"),
        "{:?}",
        run.table
    );
    let snaps = run.barrier(1).await;
    let mut run = run
        .restore(TUMBLE, 30 * SEC, &bucket, (2, 1, 3), &snaps, MIN + 5)
        .await;
    run.rows(&[("eu", 1, 20)]).await;
    assert_eq!(run.rejected, 0, "within the new lateness");
    run.advance(MIN + 30 * SEC).await;
    run.rows(&[("eu", 100, 30)]).await;
    assert_eq!(run.rejected, 1);
    assert!(
        run.table.0[first].contains("total=8 n=3"),
        "{:?}",
        run.table
    );
    run.advance(i64::MAX).await;
    assert_eq!(run.writes[first], 3, "{:?}", run.writes);
    assert_eq!(run.dropped(), 2, "both minutes' state went");

    let mut run = Run::new(SESSION, 2 * MIN, &bucket, 3, 1, 1, &[]).await;
    run.rows(&[("eu", 5, 0)]).await;
    run.advance(40 * SEC).await;
    let snaps = run.barrier(1).await;
    let mut run = run
        .restore(SESSION, 0, &bucket, (4, 1, 2), &snaps, 40 * SEC)
        .await;
    run.rows(&[("eu", 2, 25 * SEC)]).await;
    assert_eq!(run.rejected, 0);
    assert_eq!(run.dropped(), 1, "the gone session went");
    run.advance(i64::MAX).await;
    assert_eq!(
        run.table.0.values().cloned().collect::<Vec<_>>(),
        [
            "window_start=1970-01-01T00:00:00Z window_end=1970-01-01T00:00:30Z region=eu total=5",
            "window_start=1970-01-01T00:00:25Z window_end=1970-01-01T00:00:55Z region=eu total=2",
        ]
    );
    assert_eq!(run.writes["1970-01-01T00:00:00Z|eu"], 1);
}

/// A raised lateness keeps a session gone too: a row whose session, with
/// the key's sessions kept, ends at or before the floor is late, so none
/// starts again under the gone one's start. A row whose own session
/// reaches past the floor makes a session, as it would without the change.
#[tokio::test]
async fn a_session_gone_before_a_raised_lateness_stays_gone() {
    let bucket = MemoryBucket::new();
    let sql = "INSERT INTO o SELECT window_start, window_end, region, sum(amount) AS total \
               FROM orders GROUP BY session(at, INTERVAL '30 seconds'), region";
    let mut run = Run::new(sql, 0, &bucket, 1, 1, 1, &[]).await;
    run.rows(&[("eu", 5, 0)]).await;
    run.advance(40 * SEC).await;
    let snaps = run.barrier(1).await;
    let mut run = run
        .restore(sql, 2 * MIN, &bucket, (2, 1, 3), &snaps, 40 * SEC)
        .await;
    run.rows(&[("eu", 4, 0), ("eu", 1, 9 * SEC)]).await;
    assert_eq!(run.rejected, 2);
    run.rows(&[("eu", 2, 20 * SEC)]).await;
    assert_eq!(run.rejected, 2);
    run.advance(i64::MAX).await;
    assert_eq!(
        run.table.0.values().cloned().collect::<Vec<_>>(),
        [
            "window_start=1970-01-01T00:00:00Z window_end=1970-01-01T00:00:30Z region=eu total=5",
            "window_start=1970-01-01T00:00:20Z window_end=1970-01-01T00:00:50Z region=eu total=2",
        ]
    );
    assert_eq!(run.writes["1970-01-01T00:00:00Z|eu"], 1);
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
    assert_eq!(out.failed[0].2.code, openetl_normalize::Code::Aggregate);
    let late = batch(&run.ops[0], &[("us", 1, MIN + 2)]);
    let out = run.ops[0]
        .rows(&mut run.layers[0], &late, None)
        .await
        .unwrap();
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
    let out = run.ops[0]
        .rows(&mut run.layers[0], &late, None)
        .await
        .unwrap();
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

/// A delete of a window's row doesn't evaluate again an output column
/// that reads the clock: one that gave its row before a moment and would
/// fail after it gives a delete, with its key and its other values.
#[tokio::test]
async fn a_delete_doesnt_evaluate_the_clock_again() {
    // At least 2 seconds away: the moment is in whole seconds, and the
    // window's row must be given before it, however slow the machine.
    let soon = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3;
    let sql = format!(
        "INSERT INTO o SELECT window_start, region, count(*) AS n, \
         CAST(CASE WHEN now() < to_timestamp({soon}) THEN '1' ELSE 'bad' END AS BIGINT) AS r \
         FROM orders GROUP BY tumble(at, INTERVAL '1 minute'), region HAVING count(*) = 1"
    );
    let bucket = MemoryBucket::new();
    let mut run = Run::new(&sql, MIN, &bucket, 1, 1, 1, &[]).await;
    run.rows(&[("eu", 1, 1)]).await;
    run.advance(MIN).await;
    assert_eq!(
        run.table.0.values().cloned().collect::<Vec<_>>(),
        ["window_start=1970-01-01T00:00:00Z region=eu n=1 r=1"]
    );
    while std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        < soon
    {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    // A late row: the window no longer passes HAVING.
    let late = batch(&run.ops[0], &[("eu", 1, 2)]);
    let out = run.ops[0]
        .rows(&mut run.layers[0], &late, None)
        .await
        .unwrap();
    assert!(out.failed.is_empty(), "{:?}", out.failed);
    let t = out.rows.unwrap();
    let ops: Vec<Option<&str>> = t
        .control
        .op
        .as_ref()
        .unwrap()
        .as_string::<i32>()
        .iter()
        .collect();
    assert_eq!(ops, [Some("delete")]);
    let cell = |name: &str| {
        let c = t.batch.column(t.batch.schema().index_of(name).unwrap());
        arrow_cast::display::array_value_to_string(c, 0).unwrap()
    };
    assert_eq!(
        [cell("window_start"), cell("region"), cell("n")],
        ["1970-01-01T00:00:00Z", "eu", "1"]
    );
}

pub(crate) fn next(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *seed;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

pub(crate) const REGIONS: [&str; 5] = ["eu", "us", "apac", "latam", "mena"];

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
        assert!(want.ops[0].opened > 0, "{sql}");
        assert_eq!(
            want.kept(),
            0,
            "{sql}: every window's state goes at the end"
        );
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
            // The windows kept as of the last barrier, as a record would
            // count them across restores.
            let mut kept = 0;
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
                    kept += run.kept();
                    assert!(kept >= 0, "{sql}, seed {seed}");
                    epoch += 1;
                    if next(&mut s).is_multiple_of(2) {
                        // The attempt dies after the record; another
                        // restores it on other lanes.
                        generation += 1;
                        lanes = 1 + u32::try_from(next(&mut s) % 4).unwrap();
                        let (table, rejected) = (run.table.clone(), run.rejected);
                        let watermark = run.ops[0].watermark;
                        run = Run::new(sql, 0, &bucket, generation, epoch, lanes, &snaps).await;
                        run.key = key.clone();
                        (run.table, run.rejected) = (table, rejected);
                        run.advance(watermark).await;
                    }
                }
            }
            run.advance(i64::MAX).await;
            assert_eq!(kept + run.kept(), 0, "{sql}, seed {seed}");
            assert_eq!(run.rejected, 0, "{sql}, seed {seed}");
            assert_eq!(run.table, want.table, "{sql}, seed {seed}");
        }
    }
}

/// Each section the snapshots of an epoch name.
fn named_sections(snaps: &[Snapshot]) -> usize {
    snaps
        .iter()
        .map(|s| {
            runs(s)
                .unwrap()
                .iter()
                .map(|r| r.sections.len())
                .sum::<usize>()
        })
        .sum()
}

/// What [`epochs`] feeds a job, and how its runs are kept.
#[derive(Clone, Copy)]
struct Load {
    epochs: u64,
    /// Rows each epoch, of this many keys.
    rows: usize,
    keys: u64,
    drop: bool,
    /// The block cache's bytes.
    cache: usize,
}

/// What one job of [`epochs`] gave: its rows, and for each epoch, the
/// sections its lanes named, and the requests its reads made, of lookups
/// and of restores.
struct Epochs {
    table: Table,
    named: Vec<usize>,
    lookups: Vec<u64>,
    restores: Vec<u64>,
}

/// Epochs of 20 seconds of rows each, the watermark 10 seconds behind, a
/// barrier at each, and the attempt restored onto another number of lanes
/// every 10 epochs; then the end of input.
async fn epochs(sql: &str, lateness: i64, load: Load) -> Epochs {
    let objects = Arc::new(ObjectStorage::new(
        Arc::new(object_store::memory::InMemory::new()),
        object_store::path::Path::from("state"),
    ));
    let config = RunConfig {
        drop: load.drop,
        cache_bytes: load.cache,
        ..RunConfig::default()
    };
    let keys: Vec<&'static str> = (0..load.keys)
        .map(|k| &*Box::leak(format!("k{k:04}").into_boxed_str()))
        .collect();
    let mut stores = RunStores::new(objects.clone(), config.clone());
    let mut counted = Counts::default();
    let mut run = Run::new(sql, lateness, &stores, 1, 1, 2, &[]).await;
    let (mut named, mut lookups, mut restores) = (Vec::new(), Vec::new(), Vec::new());
    let mut seed = 99;
    for epoch in 1..=load.epochs {
        let from = i64::try_from(epoch).unwrap() * 20 * SEC;
        let rows: Vec<Row> = (0..load.rows)
            .map(|_| {
                let k = keys[usize::try_from(next(&mut seed) % load.keys).unwrap()];
                let a = i64::try_from(next(&mut seed) % 50).unwrap();
                let t = from + i64::try_from(next(&mut seed) % 20).unwrap() * SEC;
                (k, a, t)
            })
            .collect();
        run.rows(&rows).await;
        let watermark = from + 10 * SEC;
        run.advance(watermark).await;
        let before = stores.counts();
        let snaps = run.barrier(epoch).await;
        assert_eq!(
            (stores.counts() - before).lookup.count,
            0,
            "a barrier looks nothing up"
        );
        named.push(named_sections(&snaps));
        let c = stores.counts() - counted;
        lookups.push(c.lookup.count);
        restores.push(c.restore.count);
        counted = stores.counts();
        if epoch % 10 == 0 {
            // The attempt dies after the record, and the next restores it
            // on another number of lanes, with caches of its own.
            stores = RunStores::new(objects.clone(), config.clone());
            counted = Counts::default();
            let lanes = [1, 3, 2][usize::try_from(epoch / 10 % 3).unwrap()];
            let (table, rejected) = (run.table.clone(), run.rejected);
            run = Run::new(sql, lateness, &stores, epoch + 1, epoch + 1, lanes, &snaps).await;
            (run.table, run.rejected) = (table, rejected);
            run.advance(watermark).await;
        }
    }
    run.advance(i64::MAX).await;
    let end = run.barrier(load.epochs + 1).await;
    named.push(named_sections(&end));
    assert_eq!(run.rejected, 0, "{sql}: rows late, across every attempt");
    Epochs {
        table: run.table,
        named,
        lookups,
        restores,
    }
}

const HOP: &str = "INSERT INTO o SELECT window_start, region, count(*) AS n, sum(amount) AS total \
     FROM orders GROUP BY hop(at, INTERVAL '1 minute', INTERVAL '3 minutes'), region";

/// A tumbling window without lateness, a hopping one with 30 seconds of it
/// and sessions with a minute.
const WINDOWS: [(&str, i64); 3] = [(TUMBLE, 0), (HOP, 30 * SEC), (SESSION, MIN)];

/// As windows close, what a job's snapshots name stays bounded, where a
/// job that drops nothing names more at each epoch; both give the same
/// rows, through restores onto 3, 2 and 1 lanes. With every window closed
/// at the end of input, nothing is named.
#[tokio::test]
async fn named_sections_stay_bounded_as_windows_close() {
    let mut compared = 0;
    for (sql, lateness) in WINDOWS {
        let load = |drop| Load {
            epochs: 40,
            rows: 24,
            keys: 16,
            drop,
            cache: 64 << 20,
        };
        let kept = epochs(sql, lateness, load(false)).await;
        let dropped = epochs(sql, lateness, load(true)).await;
        assert!(!kept.table.0.is_empty());
        assert_eq!(dropped.table, kept.table, "{sql}");
        compared += 1;
        // On one lane, from epoch 31, the dropping job names no more than
        // it did on two in its first epochs; the other names more and more.
        let first = *dropped.named[..10].iter().max().unwrap();
        let last = *dropped.named[30..40].iter().max().unwrap();
        assert!(last <= first, "{sql}: {:?}", dropped.named);
        let kept_last = kept.named[39];
        assert!(
            kept_last > 10 * dropped.named[39].max(1),
            "{sql}: {:?}",
            kept.named
        );
        assert_eq!(dropped.named[40], 0, "{sql}: all closed");
    }
    assert_eq!(compared, 3);
}

/// The sections each epoch names and the requests its reads make, with no
/// block cache, over 20 epochs of 400 rows of 2,000 keys, by a job that
/// drops dead sections and one that doesn't.
#[tokio::test]
#[ignore = "a measurement: run it with --ignored --nocapture"]
async fn sections_and_reads_by_epoch() {
    for (sql, lateness) in WINDOWS {
        let load = |drop| Load {
            epochs: 20,
            rows: 400,
            keys: 2000,
            drop,
            cache: 0,
        };
        let kept = epochs(sql, lateness, load(false)).await;
        let dropped = epochs(sql, lateness, load(true)).await;
        assert_eq!(dropped.table, kept.table, "{sql}");
        println!("{sql}");
        println!("epoch | sections named | lookup GETs | restore GETs (kept / dropped)");
        for e in 0..20 {
            println!(
                "{:>5} | {:>6} / {:<6} | {:>6} / {:<6} | {:>4} / {:<4}",
                e + 1,
                kept.named[e],
                dropped.named[e],
                kept.lookups[e],
                dropped.lookups[e],
                kept.restores[e],
                dropped.restores[e]
            );
        }
    }
}

/// A restored operator goes on from its state's horizon, though the
/// record's watermark it's restored at is lower: a row of a window the
/// horizon passed is late, and doesn't open a window whose timer would be
/// gone at once.
#[tokio::test]
async fn a_restored_window_goes_on_from_its_states_horizon() {
    let objects = Arc::new(ObjectStorage::new(
        Arc::new(object_store::memory::InMemory::new()),
        object_store::path::Path::from("state"),
    ));
    let stores = RunStores::new(objects.clone(), RunConfig::default());
    let mut run = Run::new(TUMBLE, 0, &stores, 1, 1, 1, &[]).await;
    run.rows(&[("eu", 1, 1), ("eu", 1, 2 * MIN + 1)]).await;
    run.advance(2 * MIN).await;
    let snaps = run.barrier(1).await;
    let stores = RunStores::new(objects, RunConfig::default());
    let mut back = Run::new(TUMBLE, 0, &stores, 2, 2, 1, &snaps).await;
    back.advance(MIN / 2).await;
    back.rows(&[("eu", 5, MIN + 1)]).await;
    assert_eq!(
        back.rejected, 1,
        "the second minute closed before the record"
    );
    back.advance(i64::MAX).await;
    let rows: Vec<String> = back.table.0.values().cloned().collect();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert!(
        rows[0].contains("window_start=1970-01-01T00:02:00Z"),
        "{rows:?}"
    );
}

/// Two lanes whose state's horizons differ restore onto one lane, each key
/// group keeping its own: a window that the lagging group's horizon hadn't
/// passed closes once, on time, and a row of a window that the leading
/// group's horizon passed is late.
#[tokio::test]
async fn groups_restored_with_different_horizons_each_keep_theirs() {
    let objects = Arc::new(ObjectStorage::new(
        Arc::new(object_store::memory::InMemory::new()),
        object_store::path::Path::from("state"),
    ));
    let stores = RunStores::new(objects.clone(), RunConfig::default());
    let mut run = Run::new(TUMBLE, 0, &stores, 1, 1, 2, &[]).await;
    let op = &run.ops[0];
    let group = |region: &'static str| -> u32 {
        key_groups(&batch(op, &[(region, 1, 1)]), &op.plan.need, GROUPS).unwrap()[0]
    };
    const NAMES: [&str; 12] = [
        "eu", "us", "apac", "latam", "mena", "a", "b", "c", "d", "e", "f", "g",
    ];
    let lead = *NAMES.iter().find(|n| group(n) < GROUPS / 2).unwrap();
    let lag = *NAMES.iter().find(|n| group(n) >= GROUPS / 2).unwrap();
    run.rows(&[
        (lead, 1, 10 * SEC),
        (lead, 1, 70 * SEC),
        (lag, 1, 10 * SEC),
        (lag, 1, 70 * SEC),
    ])
    .await;
    // The leading lane has closed both minutes, the lagging one the first.
    run.advance_lane(0, 2 * MIN).await;
    run.advance_lane(1, MIN + SEC).await;
    let snaps = run.barrier(1).await;
    let (table, writes) = (run.table.clone(), run.writes.clone());
    let stores = RunStores::new(objects, RunConfig::default());
    let mut back = Run::new(TUMBLE, 0, &stores, 2, 2, 1, &snaps).await;
    (back.table, back.writes) = (table, writes);
    // Restored at the least of the lanes' watermarks.
    back.advance(MIN + SEC).await;
    back.rows(&[(lag, 1, 80 * SEC), (lead, 1, 80 * SEC)]).await;
    assert_eq!(back.rejected, 1, "the leading group's second minute closed");
    back.advance(2 * MIN).await;
    back.advance(i64::MAX).await;
    assert_eq!(back.writes.len(), 4, "{:?}", back.writes);
    assert!(back.writes.values().all(|n| *n == 1), "{:?}", back.writes);
    let second = &back.table.0[&format!("1970-01-01T00:01:00Z|{lag}")];
    assert!(second.ends_with("total=2 n=2 hi=1"), "{second}");
}
