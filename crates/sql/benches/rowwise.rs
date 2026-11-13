//! Rows per second on one thread for row-wise statements on 20-column rows,
//! and the time and memory to plan the docs' example.
//!
//! cargo bench -p openetl-sql --bench rowwise [-- ROWS_PER_BATCH]

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use openetl_sql::{Catalog, Operator, RowWise, Sql, Stream};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

/// 1 id, 5 ints, 4 floats, 5 strings of 21 bytes, 3 timestamps and 2
/// booleans.
fn batch(rows: usize, seed: u64) -> Result<RecordBatch> {
    let mut x = seed;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let mut fields = vec![Field::new("id", DataType::Int64, false)];
    let mut cols: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(0..rows as i64))];
    for i in 1..=5 {
        fields.push(Field::new(format!("i{i}"), DataType::Int64, true));
        cols.push(Arc::new(Int64Array::from_iter_values(
            (0..rows).map(|_| (next() % 1_000_000) as i64),
        )));
    }
    for i in 1..=4 {
        fields.push(Field::new(format!("f{i}"), DataType::Float64, true));
        cols.push(Arc::new(Float64Array::from_iter_values(
            (0..rows).map(|_| (next() % 100_000) as f64 / 100.0),
        )));
    }
    for i in 1..=5 {
        fields.push(Field::new(format!("s{i}"), DataType::Utf8, true));
        cols.push(Arc::new(StringArray::from_iter_values((0..rows).map(
            |_| {
                let n = next();
                format!("User{:06}@Example{}.com", n % 1_000_000, n % 10)
            },
        ))));
    }
    let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    for i in 1..=3 {
        fields.push(Field::new(format!("t{i}"), ts.clone(), true));
        cols.push(Arc::new(
            TimestampMicrosecondArray::from_iter_values(
                (0..rows).map(|_| 1_700_000_000_000_000 + (next() % 1_000_000_000) as i64),
            )
            .with_timezone("UTC"),
        ));
    }
    for i in 1..=2 {
        fields.push(Field::new(format!("b{i}"), DataType::Boolean, true));
        cols.push(Arc::new(BooleanArray::from_iter(
            (0..rows).map(|_| Some(next() % 2 == 0)),
        )));
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?)
}

const PROJECT: &str = "SELECT id, i1, i2 AS i2_renamed, i3, i4, i5, f1, f2, f3, f4 * 100 AS f4_pct, \
     lower(s1) AS s1, s2, s3 AS s3_renamed, s4, s5, t1, t2, t3, b1, b2, 'eu' AS region FROM t";

fn statement(sql: &str, catalog: &Catalog) -> Result<RowWise> {
    let s = Sql::parse(&format!("INSERT INTO t {sql}"))?.bind(0, catalog)?;
    match s.operator {
        Operator::RowWise(op) => Ok(op),
        Operator::GroupWindow(_) => Err("not row-wise".into()),
    }
}

/// Rows in per second, the median of 5 runs of at least a second each, and
/// rows out per row in.
fn measure(op: &RowWise, input: &[RecordBatch]) -> Result<(f64, f64)> {
    let mut rates = Vec::new();
    let mut kept = 0.0;
    for _ in 0..5 {
        let (mut rows_in, mut rows_out) = (0usize, 0usize);
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(1) {
            for b in input {
                let out = op.eval(b, &[])?;
                rows_in += b.num_rows();
                rows_out += out.num_rows();
            }
        }
        rates.push(rows_in as f64 / start.elapsed().as_secs_f64());
        kept = rows_out as f64 / rows_in as f64;
    }
    rates.sort_by(f64::total_cmp);
    Ok((rates[2], kept))
}

/// This process's resident memory in MB, from ps.
fn rss() -> Result<f64> {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()?;
    Ok(String::from_utf8(out.stdout)?.trim().parse::<f64>()? / 1024.0)
}

/// Plans the docs' two statements: first in a fresh process, then again.
fn planning() -> Result<()> {
    let before = rss()?;
    let shop = Sql::parse(
        "INSERT INTO customers SELECT id, lower(email) AS email, 'eu' AS region, created_at FROM customers;
         INSERT INTO revenue_by_minute SELECT window_start, region, sum(amount) AS revenue, count(*) AS orders
         FROM orders GROUP BY tumble(created_at, INTERVAL '1 minute'), region;",
    )?;
    let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let catalog = Catalog::default()
        .with(
            "customers",
            Stream {
                schema: Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("email", DataType::Utf8, true),
                    Field::new("created_at", ts.clone(), true),
                ])),
                key: vec!["id".into()],
                changelog: false,
            },
        )
        .with(
            "orders",
            Stream {
                schema: Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("region", DataType::Utf8, true),
                    Field::new("amount", DataType::Decimal128(12, 2), true),
                    Field::new("created_at", ts, true),
                ])),
                key: vec!["id".into()],
                changelog: false,
            },
        );
    let first = Instant::now();
    let plan = shop.plan(&catalog)?;
    let first = first.elapsed();
    let after = rss()?;
    drop(plan);
    let n = 1000;
    let start = Instant::now();
    for _ in 0..n {
        shop.plan(&catalog)?;
    }
    let each = start.elapsed();
    println!(
        "planning the docs' two statements: first {:.2} ms, then {:.3} ms each",
        first.as_secs_f64() * 1e3,
        each.as_secs_f64() * 1e3 / n as f64,
    );
    println!(
        "resident memory: {before:.1} MB before, {after:.1} MB after the first plan, {:.1} MB after {n} more",
        rss()?
    );
    Ok(())
}

fn main() -> Result<()> {
    planning()?;
    let rows: usize = match std::env::args().nth(1).filter(|a| !a.starts_with('-')) {
        Some(a) => a.parse()?,
        None => 8192,
    };
    let input: Vec<RecordBatch> = (0..16)
        .map(|i| batch(rows, 0x9E37_79B9_7F4A_7C15 ^ i))
        .collect::<Result<_>>()?;
    let catalog = Catalog::default().with(
        "t",
        Stream {
            schema: input[0].schema(),
            key: vec!["id".into()],
            changelog: false,
        },
    );
    let cases = [
        ("select * (passes 20 columns)", "SELECT * FROM t".to_owned()),
        (
            "projection: 18 kept or renamed, lower(), *100, a literal",
            PROJECT.to_owned(),
        ),
        (
            "filter: WHERE i1 % 2 = 0, all 20 columns",
            "SELECT * FROM t WHERE i1 % 2 = 0".to_owned(),
        ),
        (
            "filter: WHERE lower(s2) LIKE '%example3.com', all 20 columns",
            "SELECT * FROM t WHERE lower(s2) LIKE '%example3.com'".to_owned(),
        ),
        (
            "projection and filter (i1 % 2 = 0)",
            format!("{PROJECT} WHERE i1 % 2 = 0"),
        ),
    ];
    println!("{rows} rows per batch, one thread");
    for (name, sql) in cases {
        let op = statement(&sql, &catalog)?;
        let (rate, kept) = measure(&op, &input)?;
        println!(
            "{name:<62} {:>7.2} M rows/s in, {:.0}% kept",
            rate / 1e6,
            kept * 100.0
        );
    }
    Ok(())
}
