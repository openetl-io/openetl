//! The normalizer's work per row, on typed batches like the boundary
//! benchmark's Postgres snapshot and on the same rows as raw JSON. It prints
//! the median and fastest of its runs in nanoseconds per row.
//!
//! cargo bench -p openetl-normalize --bench shape -- --runs 21 --rows 1000000

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::builder::{
    BooleanBuilder, Decimal128Builder, FixedSizeBinaryBuilder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use openetl_normalize::naming::Naming;
use openetl_normalize::{
    EXTENSION_METADATA_KEY, EXTENSION_NAME_KEY, Identity, Policy, Shaper, TypeOptions,
    UUID_EXTENSION,
};

/// The engine's allocator.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

const BATCH_ROWS: usize = 100_000;
const RAW_ROWS: usize = 10_000;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let flag = |name: &str, default: usize| -> usize {
        match args.iter().position(|a| a == name) {
            Some(i) => args
                .get(i + 1)
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| panic!("{name} takes a number")),
            None => default,
        }
    };
    let runs = flag("--runs", 15);
    let rows = flag("--rows", 1_000_000);
    let batches = typed(rows);
    report("typed batches", runs, rows, || {
        let mut s = shaper();
        for b in &batches {
            black_box(s.batch(b).unwrap_or_else(|e| panic!("{e}")));
        }
    });
    let raw = json(&batches);
    report("raw rows", runs, rows, || {
        let mut s = shaper();
        for chunk in raw.chunks(RAW_ROWS) {
            let out = s.rows(chunk.iter().map(Vec::as_slice));
            black_box(out.unwrap_or_else(|e| panic!("{e}")));
        }
    });
}

fn report(name: &str, runs: usize, rows: usize, mut f: impl FnMut()) {
    f();
    let mut ns: Vec<f64> = (0..runs.max(1))
        .map(|_| {
            let start = Instant::now();
            f();
            start.elapsed().as_nanos() as f64 / rows as f64
        })
        .collect();
    ns.sort_by(f64::total_cmp);
    println!(
        "{name:<14} median {:>8.1} ns/row   fastest {:>8.1} ns/row   ({} runs of {rows} rows)",
        ns[ns.len() / 2],
        ns[0],
        ns.len(),
    );
}

fn shaper() -> Shaper {
    let identity = Identity {
        stream: "items".into(),
        key: vec!["id".into()],
        cursor: None,
        read_start: 1_700_000_000_000_000,
    };
    Shaper::new(
        Naming::default(),
        "items".into(),
        identity,
        Policy::AllowAll,
        None,
        None,
        TypeOptions::default(),
    )
    .unwrap_or_else(|e| panic!("{e}"))
}

/// A deterministic stream of numbers.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 11
    }

    fn text(&mut self, len: usize) -> String {
        (0..len)
            .map(|_| char::from(b'a' + (self.next() % 26) as u8))
            .collect()
    }
}

/// The boundary benchmark's table: a bigint key, integer, smallint, two
/// texts (the second 10% null), numeric(12,2), double (14% null), boolean,
/// timestamptz and uuid.
fn typed(rows: usize) -> Vec<RecordBatch> {
    let uuid =
        Field::new("uid", DataType::FixedSizeBinary(16), true).with_metadata(HashMap::from([
            (EXTENSION_NAME_KEY.to_owned(), UUID_EXTENSION.to_owned()),
            (EXTENSION_METADATA_KEY.to_owned(), String::new()),
        ]));
    let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("qty", DataType::Int32, true),
        Field::new("kind", DataType::Int16, true),
        Field::new("code", DataType::Utf8, true),
        Field::new("note", DataType::Utf8, true),
        Field::new("price", DataType::Decimal128(12, 2), true),
        Field::new("score", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
        Field::new("created_at", ts, true),
        uuid,
    ]));
    let mut r = Lcg(42);
    let mut out = Vec::new();
    let mut id = 0i64;
    while (id as usize) < rows {
        let n = BATCH_ROWS.min(rows - id as usize);
        let mut ids = Int64Builder::with_capacity(n);
        let mut qty = Int32Builder::with_capacity(n);
        let mut kind = Int16Builder::with_capacity(n);
        let mut code = StringBuilder::with_capacity(n, n * 12);
        let mut note = StringBuilder::with_capacity(n, n * 64);
        let mut price =
            Decimal128Builder::with_capacity(n).with_data_type(DataType::Decimal128(12, 2));
        let mut score = Float64Builder::with_capacity(n);
        let mut active = BooleanBuilder::with_capacity(n);
        let mut at = TimestampMicrosecondBuilder::with_capacity(n).with_timezone("UTC");
        let mut uid = FixedSizeBinaryBuilder::with_capacity(n, 16);
        for _ in 0..n {
            id += 1;
            ids.append_value(id);
            qty.append_value((r.next() % 1_000) as i32);
            kind.append_value((r.next() % 10) as i16);
            code.append_value(r.text(12));
            match r.next() % 10 {
                0 => note.append_null(),
                _ => note.append_value(r.text(64)),
            }
            price.append_value((r.next() % 10_000_000) as i128);
            match r.next() % 100 < 14 {
                true => score.append_null(),
                false => score.append_value((r.next() % 1_000_000) as f64 / 7.0),
            }
            active.append_value(r.next().is_multiple_of(2));
            at.append_value(1_700_000_000_000_000 + (r.next() % 100_000_000_000) as i64);
            let mut u = [0u8; 16];
            u[..8].copy_from_slice(&r.next().to_le_bytes());
            u[8..].copy_from_slice(&r.next().to_le_bytes());
            uid.append_value(u).unwrap_or_else(|e| panic!("{e}"));
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(ids.finish()),
            Arc::new(qty.finish()),
            Arc::new(kind.finish()),
            Arc::new(code.finish()),
            Arc::new(note.finish()),
            Arc::new(price.finish()),
            Arc::new(score.finish()),
            Arc::new(active.finish()),
            Arc::new(at.finish()),
            Arc::new(uid.finish()),
        ];
        out.push(RecordBatch::try_new(schema.clone(), columns).unwrap_or_else(|e| panic!("{e}")));
    }
    out
}

/// The batches' rows as JSON objects, as a source without Arrow sends them.
fn json(batches: &[RecordBatch]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for b in batches {
        for row in 0..b.num_rows() {
            out.push(openetl_normalize::render_row(b, row).into_bytes());
        }
    }
    out
}
