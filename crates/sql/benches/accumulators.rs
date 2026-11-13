//! Reading aggregate state per group at a barrier without resetting it, for
//! count, sum, min, max and avg, three ways:
//!
//! - emit and merge back: DataFusion's `GroupsAccumulator`, whose `state`
//!   empties it, then `merge_batch` of what it gave;
//! - a row accumulator per group: DataFusion's `Accumulator`, read with
//!   `state` group by group;
//! - columnar accumulators of our own, copied at the barrier.
//!
//! Then a tumbling window's count and sum over 100K keys, planned from SQL,
//! and the copy and encoding of a large state.
//!
//! cargo bench -p openetl-sql --bench accumulators [-- GROUPS]

use std::collections::HashMap;
use std::error::Error;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, TimestampMicrosecondArray,
};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion_common::ScalarValue;
use datafusion_expr::{Accumulator, AggregateUDF, EmitTo, GroupsAccumulator};
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::col;
use openetl_sql::{Catalog, Operator, Sql, Stream, Window};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

const BATCH: usize = 8192;
const FUNCTIONS: [&str; 5] = ["count", "sum", "min", "max", "avg"];

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// avg takes floats: SQL planning casts an integer argument to Float64.
fn takes_floats(function: &str) -> bool {
    function == "avg"
}

fn schema(function: &str) -> SchemaRef {
    let t = match takes_floats(function) {
        true => DataType::Float64,
        false => DataType::Int64,
    };
    Arc::new(Schema::new(vec![Field::new("v", t, true)]))
}

fn floats(v: &ArrayRef) -> ArrayRef {
    Arc::new(Float64Array::from_iter(
        v.as_primitive::<Int64Type>()
            .iter()
            .map(|x| x.map(|x| x as f64)),
    ))
}

fn udaf(name: &str) -> Result<Arc<AggregateUDF>> {
    datafusion_functions_aggregate::all_default_aggregate_functions()
        .into_iter()
        .find(|f| f.name() == name)
        .ok_or_else(|| format!("no aggregate {name}").into())
}

fn expr(name: &str) -> Result<datafusion_physical_expr::aggregate::AggregateFunctionExpr> {
    let s = schema(name);
    Ok(AggregateExprBuilder::new(udaf(name)?, vec![col("v", &s)?])
        .schema(s)
        .alias(name)
        .build()?)
}

/// Batches of values and each row's group. The first batches name every
/// group in turn: DataFusion's groups accumulators take every group below
/// `total_num_groups` to have had a row, as an operator assigning group
/// indices at a group's first row gives them.
fn input(groups: usize, batches: usize, seed: u64) -> Vec<(ArrayRef, Vec<usize>)> {
    let mut r = Rng(seed);
    (0..batches)
        .map(|b| {
            let v: ArrayRef = Arc::new(Int64Array::from_iter_values(
                (0..BATCH).map(|_| (r.next() % 10_000) as i64 - 5_000),
            ));
            let g = (0..BATCH)
                .map(|i| match b * BATCH + i {
                    n if n < groups => n,
                    _ => (r.next() % groups as u64) as usize,
                })
                .collect();
            (v, g)
        })
        .collect()
}

fn rate(rows: usize, d: Duration) -> String {
    format!("{:.2} M rows/s", rows as f64 / d.as_secs_f64() / 1e6)
}

fn ms(d: Duration) -> String {
    format!("{:.1} ms", d.as_secs_f64() * 1e3)
}

const RUNS: usize = 3;

/// Emit and merge back, with DataFusion's groups accumulators. The best of
/// three runs.
fn emit_and_merge(groups: usize, data: &[(ArrayRef, Vec<usize>)]) -> Result<Vec<ArrayRef>> {
    let (mut update, mut barrier, mut bytes) = (Duration::MAX, Duration::MAX, 0);
    let mut out = Vec::new();
    for _ in 0..RUNS {
        let mut accs: Vec<Box<dyn GroupsAccumulator>> = FUNCTIONS
            .iter()
            .map(|f| expr(f)?.create_groups_accumulator().map_err(Into::into))
            .collect::<Result<_>>()?;
        let start = Instant::now();
        for (v, g) in data {
            let f = floats(v);
            for (a, name) in accs.iter_mut().zip(FUNCTIONS) {
                let values = if takes_floats(name) { &f } else { v };
                a.update_batch(std::slice::from_ref(values), g, None, groups)?;
            }
        }
        update = update.min(start.elapsed());
        let start = Instant::now();
        let all: Vec<usize> = (0..groups).collect();
        bytes = 0;
        for a in &mut accs {
            let state = a.state(EmitTo::All)?;
            bytes += state
                .iter()
                .map(|s| s.get_array_memory_size())
                .sum::<usize>();
            a.merge_batch(&state, &all, groups)?;
        }
        barrier = barrier.min(start.elapsed());
        out = accs
            .iter_mut()
            .map(|a| a.evaluate(EmitTo::All).map_err(Into::into))
            .collect::<Result<_>>()?;
    }
    println!(
        "  emit and merge back: update {}, barrier {} for {} groups ({:.0} MB of state)",
        rate(data.len() * BATCH, update),
        ms(barrier),
        groups,
        bytes as f64 / 1e6
    );
    Ok(out)
}

/// A row accumulator per group, updated with each group's rows of a batch.
fn row_per_group(groups: usize, data: &[(ArrayRef, Vec<usize>)]) -> Result<Vec<ArrayRef>> {
    let exprs: Vec<_> = FUNCTIONS.iter().map(|f| expr(f)).collect::<Result<_>>()?;
    let mut accs: Vec<Vec<Box<dyn Accumulator>>> = exprs
        .iter()
        .map(|e| (0..groups).map(|_| e.create_accumulator()).collect())
        .collect::<std::result::Result<_, _>>()?;
    let start = Instant::now();
    let mut order: Vec<u32> = Vec::with_capacity(BATCH);
    for (v, g) in data {
        order.clear();
        order.extend(0..g.len() as u32);
        order.sort_unstable_by_key(|&i| g[i as usize]);
        let mut at = 0;
        while at < order.len() {
            let group = g[order[at] as usize];
            let end = at + order[at..].partition_point(|&i| g[i as usize] == group);
            let rows = arrow_array::UInt32Array::from(order[at..end].to_vec());
            let values = arrow_select::take::take(v, &rows, None)?;
            let f = floats(&values);
            for (a, name) in accs.iter_mut().zip(FUNCTIONS) {
                let values = if takes_floats(name) { &f } else { &values };
                a[group].update_batch(std::slice::from_ref(values))?;
            }
            at = end;
        }
    }
    let update = start.elapsed();
    let start = Instant::now();
    for a in &mut accs {
        let states: Vec<Vec<ScalarValue>> = a
            .iter_mut()
            .map(|acc| acc.state())
            .collect::<std::result::Result<_, _>>()?;
        let width = states.first().map_or(0, Vec::len);
        for i in 0..width {
            ScalarValue::iter_to_array(states.iter().map(|s| s[i].clone()))?;
        }
    }
    let barrier = start.elapsed();
    println!(
        "  row accumulator per group: update {}, barrier {} for {} groups",
        rate(data.len() * BATCH, update),
        ms(barrier),
        groups
    );
    // state() twice gives the same, and evaluate still has every row.
    let mut again = Vec::new();
    for a in &mut accs {
        let first: Vec<_> = a[0].state()?;
        let second: Vec<_> = a[0].state()?;
        if first != second {
            return Err("a row accumulator's state() reset it".into());
        }
        let values: Vec<ScalarValue> = a
            .iter_mut()
            .map(|acc| acc.evaluate())
            .collect::<std::result::Result<_, _>>()?;
        again.push(ScalarValue::iter_to_array(values)?);
    }
    Ok(again)
}

/// Count, sum, min, max and avg of Int64 per group, in columns, and which
/// groups changed since the last barrier.
#[derive(Default)]
struct Ours {
    count: Vec<i64>,
    sum: Vec<i64>,
    min: Vec<i64>,
    max: Vec<i64>,
    /// avg's sum, as DataFusion's avg of an integer keeps it.
    fsum: Vec<f64>,
    seen: Vec<bool>,
    changed: Vec<u64>,
}

impl Ours {
    fn grow(&mut self, groups: usize) {
        if self.count.len() < groups {
            self.count.resize(groups, 0);
            self.sum.resize(groups, 0);
            self.min.resize(groups, i64::MAX);
            self.max.resize(groups, i64::MIN);
            self.fsum.resize(groups, 0.0);
            self.seen.resize(groups, false);
            self.changed.resize(groups.div_ceil(64), 0);
        }
    }

    /// A pass per aggregate, as DataFusion's groups accumulators make.
    fn update(&mut self, v: &Int64Array, groups: &[usize]) {
        let values = v.values();
        if v.null_count() == 0 {
            let rows = || groups.iter().copied().zip(values.iter().copied());
            for g in groups {
                self.count[*g] += 1;
            }
            for (g, x) in rows() {
                self.sum[g] = self.sum[g].wrapping_add(x);
            }
            for (g, x) in rows() {
                self.min[g] = self.min[g].min(x);
            }
            for (g, x) in rows() {
                self.max[g] = self.max[g].max(x);
            }
            for (g, x) in rows() {
                self.fsum[g] += x as f64;
            }
            for g in groups {
                self.seen[*g] = true;
                self.changed[*g / 64] |= 1 << (*g % 64);
            }
            return;
        }
        for (i, &g) in groups.iter().enumerate() {
            if v.is_null(i) {
                continue;
            }
            let x = values[i];
            self.count[g] += 1;
            self.sum[g] = self.sum[g].wrapping_add(x);
            self.min[g] = self.min[g].min(x);
            self.max[g] = self.max[g].max(x);
            self.fsum[g] += x as f64;
            self.seen[g] = true;
            self.changed[g / 64] |= 1 << (g % 64);
        }
    }

    fn nulls(&self, rows: &[usize]) -> Option<NullBuffer> {
        Some(NullBuffer::new(BooleanBuffer::from_iter(
            rows.iter().map(|&g| self.seen[g]),
        )))
    }

    /// Copies the state of every group, or of the groups that changed since
    /// the last snapshot, as Arrow columns: group, count, sum, min, max and
    /// avg's sum. The accumulators keep it.
    fn snapshot(&mut self, only_changed: bool) -> Vec<ArrayRef> {
        if !only_changed {
            self.changed.fill(0);
            let nulls = || Some(NullBuffer::new(BooleanBuffer::from(self.seen.as_slice())));
            return vec![
                Arc::new(Int64Array::from_iter_values(0..self.count.len() as i64)),
                Arc::new(Int64Array::from(self.count.clone())),
                Arc::new(Int64Array::new(self.sum.clone().into(), nulls())),
                Arc::new(Int64Array::new(self.min.clone().into(), nulls())),
                Arc::new(Int64Array::new(self.max.clone().into(), nulls())),
                Arc::new(Float64Array::from(self.fsum.clone())),
            ];
        }
        let rows: Vec<usize> = (0..self.count.len())
            .filter(|&g| self.changed[g / 64] & (1 << (g % 64)) != 0)
            .collect();
        let pick = |c: &[i64]| -> Vec<i64> { rows.iter().map(|&g| c[g]).collect() };
        let out: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|&g| g as i64))),
            Arc::new(Int64Array::from(pick(&self.count))),
            Arc::new(Int64Array::new(pick(&self.sum).into(), self.nulls(&rows))),
            Arc::new(Int64Array::new(pick(&self.min).into(), self.nulls(&rows))),
            Arc::new(Int64Array::new(pick(&self.max).into(), self.nulls(&rows))),
            Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|&g| self.fsum[g]),
            )),
        ];
        self.changed.fill(0);
        out
    }

    fn evaluate(&self) -> Vec<ArrayRef> {
        let all: Vec<usize> = (0..self.count.len()).collect();
        let avg = Float64Array::from_iter(
            all.iter()
                .map(|&g| (self.count[g] > 0).then(|| self.fsum[g] / self.count[g] as f64)),
        );
        vec![
            Arc::new(Int64Array::from(self.count.clone())),
            Arc::new(Int64Array::new(self.sum.clone().into(), self.nulls(&all))),
            Arc::new(Int64Array::new(self.min.clone().into(), self.nulls(&all))),
            Arc::new(Int64Array::new(self.max.clone().into(), self.nulls(&all))),
            Arc::new(avg),
        ]
    }
}

/// Ours, the best of three runs.
fn ours(groups: usize, data: &[(ArrayRef, Vec<usize>)]) -> Result<Vec<ArrayRef>> {
    let (mut update, mut all, mut some) = (Duration::MAX, Duration::MAX, Duration::MAX);
    let (mut bytes, mut changed) = (0, Vec::new());
    for _ in 0..RUNS {
        let mut acc = Ours::default();
        acc.grow(groups);
        let start = Instant::now();
        for (v, g) in data {
            acc.update(v.as_primitive::<Int64Type>(), g);
        }
        update = update.min(start.elapsed());
        let start = Instant::now();
        let snap = acc.snapshot(false);
        all = all.min(start.elapsed());
        bytes = snap.iter().map(|s| s.get_array_memory_size()).sum();
        // One more batch, then only what it changed.
        if let Some((v, g)) = data.last() {
            acc.update(v.as_primitive::<Int64Type>(), g);
        }
        let start = Instant::now();
        changed = acc.snapshot(true);
        some = some.min(start.elapsed());
    }
    println!(
        "  ours: update {}, barrier {} for {} groups ({:.0} MB), {} for the {} one batch changed",
        rate(data.len() * BATCH, update),
        ms(all),
        groups,
        bytes as f64 / 1e6,
        ms(some),
        changed[0].len()
    );
    // The batch added above, taken out again for the comparison.
    let mut check = Ours::default();
    check.grow(groups);
    for (v, g) in data {
        check.update(v.as_primitive::<Int64Type>(), g);
    }
    Ok(check.evaluate())
}

fn same(a: &[ArrayRef], b: &[ArrayRef], what: &str) -> Result<()> {
    for (i, (x, y)) in a.iter().zip(b).enumerate() {
        let equal = match (x.data_type(), y.data_type()) {
            (DataType::Float64, DataType::Float64) => {
                let (x, y) = (
                    x.as_primitive::<Float64Type>(),
                    y.as_primitive::<Float64Type>(),
                );
                x.len() == y.len()
                    && x.iter().zip(y.iter()).all(|(p, q)| match (p, q) {
                        (Some(p), Some(q)) => (p - q).abs() <= 1e-9 * p.abs().max(1.0),
                        (p, q) => p == q,
                    })
            }
            _ => x.as_ref() == y.as_ref(),
        };
        if !equal {
            let first = (0..x.len().min(y.len()))
                .find(|&r| x.slice(r, 1).as_ref() != y.slice(r, 1).as_ref())
                .map(|r| format!("row {r}: {:?} against {:?}", x.slice(r, 1), y.slice(r, 1)));
            return Err(format!(
                "{what}: {} differs ({} {} and {} {}); {first:?}",
                FUNCTIONS[i],
                x.len(),
                x.data_type(),
                y.len(),
                y.data_type()
            )
            .into());
        }
    }
    Ok(())
}

/// A fast hash of a 16-byte key, for the window prototype.
#[derive(Default)]
struct Mix(u64);

impl Hasher for Mix {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.write_u64(u64::from(*b));
        }
    }

    fn write_i64(&mut self, i: i64) {
        self.write_u64(i as u64);
    }

    fn write_u64(&mut self, i: u64) {
        self.0 = (self.0.rotate_left(5) ^ i).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
    }
}

/// `count(*)` and `sum(amount)` per minute and key, over 100K keys, planned
/// from SQL: the window's start and the key evaluated by the plan, groups
/// found in a hash map, and the accumulators updated.
fn tumbling(rows_per_window: usize, windows: usize) -> Result<()> {
    let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let schema = Arc::new(Schema::new(vec![
        Field::new("created_at", ts, true),
        Field::new("key", DataType::Int64, true),
        Field::new("amount", DataType::Int64, true),
    ]));
    let sql = Sql::parse(
        "INSERT INTO out SELECT window_start, key, count(*) AS n, sum(amount) AS total
         FROM t GROUP BY tumble(created_at, INTERVAL '1 minute'), key",
    )?;
    let catalog = Catalog::default().with(
        "t",
        Stream {
            schema: schema.clone(),
            key: Vec::new(),
            changelog: false,
        },
    );
    let Operator::GroupWindow(g) = sql.bind(0, &catalog)?.operator else {
        return Err("not a group window".into());
    };
    let Window::Tumble { start, .. } = &g.window;
    let mut r = Rng(42);
    let total = rows_per_window * windows;
    let step = 60_000_000 / rows_per_window as i64;
    let batches: Vec<RecordBatch> = (0..total / BATCH)
        .map(|b| {
            let base = (b * BATCH) as i64 * step;
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(
                        TimestampMicrosecondArray::from_iter_values(
                            (0..BATCH as i64).map(|i| base + i * step),
                        )
                        .with_timezone("UTC"),
                    ),
                    Arc::new(Int64Array::from_iter_values(
                        (0..BATCH).map(|_| (r.next() % 100_000) as i64),
                    )),
                    Arc::new(Int64Array::from_iter_values(
                        (0..BATCH).map(|_| (r.next() % 1000) as i64),
                    )),
                ],
            )
            .map_err(Into::into)
        })
        .collect::<Result<_>>()?;
    let mut groups: HashMap<(i64, i64), usize, BuildHasherDefault<Mix>> = HashMap::default();
    let mut acc = Ours::default();
    let mut idx = Vec::with_capacity(BATCH);
    let started = Instant::now();
    for b in &batches {
        let starts = start.eval(b)?;
        let starts = starts.as_primitive::<arrow_array::types::TimestampMicrosecondType>();
        let keys = g.keys[0].expr.eval(b)?;
        let keys = keys.as_primitive::<Int64Type>();
        let amount = g.aggregates[1].args[0].eval(b)?;
        idx.clear();
        for (w, k) in starts.values().iter().zip(keys.values()) {
            let next = groups.len();
            idx.push(*groups.entry((*w, *k)).or_insert(next));
        }
        acc.grow(groups.len());
        acc.update(amount.as_primitive::<Int64Type>(), &idx);
    }
    let took = started.elapsed();
    println!(
        "tumbling count and sum, 100K keys, {} windows of {} rows: {} ({} groups)",
        windows,
        rows_per_window,
        rate(batches.len() * BATCH, took),
        groups.len()
    );
    Ok(())
}

/// Whether emitting and merging back gives each aggregate what it had, for
/// a group with values, one with only nulls and one with no rows.
fn round_trip() -> Result<()> {
    let v: ArrayRef = Arc::new(Int64Array::from(vec![Some(4), None, Some(6)]));
    let groups = [0, 1, 0];
    for name in FUNCTIONS {
        let values = if takes_floats(name) {
            floats(&v)
        } else {
            v.clone()
        };
        let mut before = expr(name)?.create_groups_accumulator()?;
        let mut after = expr(name)?.create_groups_accumulator()?;
        for a in [&mut before, &mut after] {
            a.update_batch(std::slice::from_ref(&values), &groups, None, 3)?;
        }
        let state = after.state(EmitTo::All)?;
        after.merge_batch(&state, &[0, 1, 2], 3)?;
        let (b, a) = (before.evaluate(EmitTo::All)?, after.evaluate(EmitTo::All)?);
        let show = |x: &ArrayRef| -> Vec<String> {
            (0..x.len())
                .map(|i| match x.is_null(i) {
                    true => "null".into(),
                    false => format!("{:?}", ScalarValue::try_from_array(x, i)),
                })
                .collect()
        };
        let verdict = if a.as_ref() == b.as_ref() {
            "same"
        } else {
            "CHANGED"
        };
        println!(
            "  {name}: before {:?}, after emit and merge back {:?}: {verdict}",
            show(&b),
            show(&a)
        );
    }
    Ok(())
}

/// Copying a state of `groups` groups out of our accumulators and encoding
/// it as Arrow IPC, as a snapshot would.
fn snapshot(groups: usize) -> Result<()> {
    let mut acc = Ours::default();
    acc.grow(groups);
    let mut r = Rng(7);
    let v = Int64Array::from_iter_values((0..groups).map(|_| (r.next() % 1000) as i64));
    let all: Vec<usize> = (0..groups).collect();
    acc.update(&v, &all);
    let start = Instant::now();
    let cols = acc.snapshot(false);
    let copy = start.elapsed();
    let fields: Vec<Field> = ["group", "count", "sum", "min", "max", "fsum"]
        .iter()
        .zip(&cols)
        .map(|(n, c)| Field::new(*n, c.data_type().clone(), true))
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?;
    let bytes = batch.get_array_memory_size();
    let start = Instant::now();
    let mut out = Vec::with_capacity(bytes + (1 << 20));
    {
        let mut w = arrow_ipc::writer::StreamWriter::try_new(&mut out, &batch.schema())?;
        w.write(&batch)?;
        w.finish()?;
    }
    let encode = start.elapsed();
    let gb = bytes as f64 / 1e9;
    println!(
        "snapshot of {groups} groups, {:.2} GB: copy {} ({:.1} s per GB), IPC encode {} ({:.1} s per GB)",
        gb,
        ms(copy),
        copy.as_secs_f64() / gb,
        ms(encode),
        encode.as_secs_f64() / gb
    );
    Ok(())
}

fn main() -> Result<()> {
    let arg = std::env::args().nth(1).filter(|a| !a.starts_with('-'));
    let sizes: Vec<usize> = match arg {
        Some(a) => vec![a.parse()?],
        None => vec![100_000, 1_000_000],
    };
    println!("one group with values, one with only nulls, one with no rows");
    round_trip()?;
    for groups in sizes {
        let data = input(groups, 2_000_000 / BATCH * 5, groups as u64);
        println!(
            "{groups} groups, {} rows, {}",
            data.len() * BATCH,
            FUNCTIONS.join(", ")
        );
        let df = emit_and_merge(groups, &data)?;
        let ours = ours(groups, &data)?;
        same(&df, &ours, "ours against emit and merge back")?;
        if groups <= 100_000 {
            let some = &data[..data.len() / 10];
            println!("{groups} groups, {} rows", some.len() * BATCH);
            let rows = row_per_group(groups, some)?;
            let mut check = Ours::default();
            check.grow(groups);
            for (v, g) in some {
                check.update(v.as_primitive::<Int64Type>(), g);
            }
            same(&check.evaluate(), &rows, "row accumulators against ours")?;
        }
    }
    tumbling(1_000_000, 10)?;
    snapshot(8_000_000)?;
    Ok(())
}
