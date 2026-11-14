//! The suites' queries over generated rows: each file's tables filled with
//! rows drawn from a seed, its views, then each query both engines run,
//! through ours and through DuckDB, and the results compared.
//!
//! Values are drawn from small pools that hold the edges of each type, and
//! strings from the file's own literals too, so its filters keep some rows
//! and drop others. Where a row fails in ours, which quarantines it, DuckDB
//! has to fail the query.

use std::path::Path;

use sqllogictest::{DB as _, QueryExpect, Record, StatementExpect};

use super::duckdb::DuckDb;
use super::ours::{Ours, statement};
use super::rows::{self, Kind, Table, Tok};

/// Rows per table.
const ROWS: usize = 24;

/// Queries whose results differ from DuckDB's on some generated rows, by
/// file and a part of their SQL, and why. Each must differ on one of the
/// seeds at least, or the test fails.
pub(super) const DIFFERS: &[(&str, &str, &str)] = &[
    (
        "functions.slt",
        "ln(abs(x))",
        "the logarithm of zero is -Infinity in ours, as DataFusion gives it; DuckDB, as Postgres, fails",
    ),
    (
        "functions.slt",
        "asin(x / 4)",
        "outside its domain a function is NaN in ours, as DataFusion gives it; DuckDB, as Postgres, fails",
    ),
    (
        "functions.slt",
        "cot(x)",
        "the cotangent of zero is Infinity in ours, as DataFusion gives it; DuckDB fails",
    ),
    (
        "functions.slt",
        "array_sort(nums)",
        "nulls sort first in ours, as in DataFusion; DuckDB puts them last",
    ),
    (
        "functions.slt",
        "array_to_string(tags",
        "a list of nulls joins to an empty string in ours, as in Postgres; DuckDB gives null",
    ),
    (
        "projection.slt",
        "score + 1",
        "an integer literal is BIGINT in ours, as DataFusion types it, so INTEGER + 1 is a BIGINT; DuckDB, as Postgres, adds INTEGERs and overflows",
    ),
    (
        "types.slt",
        "TRY_CAST(s AS BIGINT)",
        "text with a fraction isn't a BIGINT in ours, as in Postgres; DuckDB rounds it",
    ),
];

/// A small, seeded generator (splitmix64).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % (n.max(1) as u64)).unwrap_or_default()
    }

    fn pick<'a>(&mut self, xs: &[&'a str]) -> &'a str {
        xs[self.below(xs.len())]
    }
}

/// What one query gave in one engine: its rows, sorted, how many rows it
/// quarantined, or its error.
#[derive(Debug, PartialEq)]
enum Got {
    Rows(Vec<Vec<String>>, usize),
    Failed(String),
}

impl Got {
    fn agrees(&self, duck: &Got) -> bool {
        match (self, duck) {
            (Got::Rows(a, 0), Got::Rows(b, _)) => a == b,
            // DuckDB fails a query where ours quarantines a row.
            (Got::Rows(_, q), Got::Failed(_)) => *q > 0,
            (Got::Failed(_), Got::Failed(_)) => true,
            _ => false,
        }
    }

    fn shown(&self) -> String {
        match self {
            Got::Rows(r, q) => {
                let mut s = r
                    .iter()
                    .take(8)
                    .map(|row| row.join(" | "))
                    .collect::<Vec<_>>()
                    .join("\n    ");
                if r.len() > 8 {
                    s.push_str(&format!("\n    … {} rows", r.len()));
                }
                if *q > 0 {
                    s.push_str(&format!("\n    and {q} quarantined"));
                }
                s
            }
            Got::Failed(e) => format!("failed: {e}"),
        }
    }
}

/// Rows sorted, each cell's runs of whitespace made one space, as the
/// suites' text compares them.
fn sorted(rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = rows
        .into_iter()
        .map(|r| {
            r.into_iter()
                .map(|c| c.split_whitespace().collect::<Vec<_>>().join(" "))
                .collect()
        })
        .collect();
    rows.sort();
    rows
}

/// A query both engines run, with where it's written.
struct Query {
    line: u32,
    sql: String,
}

/// The mismatches of a file's queries over each seed's rows: one string
/// each, and the exceptions of [`DIFFERS`] that a mismatch used.
pub(super) fn compare(
    file: &Path,
    bin: &Path,
    seeds: &[u64],
) -> Result<(Vec<String>, Vec<usize>), String> {
    let name = file
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let records = sqllogictest::parse_file::<sqllogictest::DefaultColumnType>(file)
        .map_err(|e| e.to_string())?;
    let text = std::fs::read_to_string(file).map_err(|e| e.to_string())?;
    let words = strings(&text);
    // Tables and views, in order, and the queries.
    let mut setup = Vec::new();
    let mut queries = Vec::new();
    for r in records {
        match r {
            Record::Statement {
                conditions,
                sql,
                expected: StatementExpect::Ok | StatementExpect::Count(_),
                ..
            } if conditions.is_empty() => {
                let sql = statement(&sql).to_owned();
                match rows::kind(&sql) {
                    Kind::Table | Kind::View => setup.push(sql),
                    _ => {}
                }
            }
            Record::Query {
                conditions,
                sql,
                expected: QueryExpect::Results { .. },
                loc,
                ..
            } if conditions.is_empty() && rows::kind(statement(&sql)) == Kind::Query => {
                queries.push(Query {
                    line: loc.line(),
                    sql: statement(&sql).to_owned(),
                });
            }
            _ => {}
        }
    }
    let mut mismatches = Vec::new();
    let mut used = Vec::new();
    for &seed in seeds {
        let mut rng = Rng(seed);
        let mut ours = Ours::default();
        let mut duck = DuckDb::new(bin)?;
        for sql in &setup {
            ours.run(sql)
                .map_err(|e| format!("{name}: {sql}: ours: {e}"))?;
            duck.define(sql)
                .map_err(|e| format!("{name}: {sql}: DuckDB: {e}"))?;
            if rows::kind(sql) == Kind::Table {
                let t = rows::table(sql)?;
                let insert = generate(&t, &mut rng, &words);
                ours.run(&insert)
                    .map_err(|e| format!("{name}: {insert}: ours: {e}"))?;
                duck.define(&insert)
                    .map_err(|e| format!("{name}: {insert}: DuckDB: {e}"))?;
            }
        }
        let theirs: Vec<Got> = std::thread::scope(|s| {
            let duck = &duck;
            let handles: Vec<_> = queries
                .chunks(queries.len().div_ceil(4).max(1))
                .map(|chunk| {
                    s.spawn(move || {
                        chunk
                            .iter()
                            .map(|q| match duck.query(&q.sql) {
                                Ok(rows) => Got::Rows(sorted(rows), 0),
                                Err(e) => Got::Failed(e),
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|h| h.join().unwrap_or_else(|e| std::panic::resume_unwind(e)))
                .collect()
        });
        for (q, duck) in queries.iter().zip(theirs) {
            let ours = match ours.query(&q.sql, true) {
                Ok(ran) => Got::Rows(sorted(ran.rows), ran.quarantined.len()),
                Err(e) => Got::Failed(e.0),
            };
            if ours.agrees(&duck) {
                continue;
            }
            match DIFFERS
                .iter()
                .position(|(f, part, _)| *f == name && q.sql.contains(part))
            {
                Some(i) => used.push(i),
                None => mismatches.push(format!(
                    "{name}:{} with seed {seed}:\n  {}\n  ours:\n    {}\n  DuckDB:\n    {}",
                    q.line,
                    q.sql.replace('\n', "\n  "),
                    ours.shown(),
                    duck.shown()
                )),
            }
        }
    }
    Ok((mismatches, used))
}

/// The string literals of a file.
fn strings(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in text.lines() {
        if let Ok(t) = rows::tokens(line) {
            for tok in t {
                if let Tok::Str(s) = tok
                    && !out.contains(&s)
                {
                    out.push(s);
                }
            }
        }
    }
    out
}

/// An `INSERT` of [`ROWS`] generated rows into `t`, which both engines run.
fn generate(t: &Table, rng: &mut Rng, words: &[String]) -> String {
    let mut rows = Vec::with_capacity(ROWS);
    for i in 0..ROWS {
        let values: Vec<String> = t
            .schema
            .fields()
            .iter()
            .map(|f| match t.key.contains(f.name()) {
                true => key(f.data_type(), i),
                false if f.is_nullable() && rng.below(7) == 0 => "NULL".into(),
                false => value(f.data_type(), rows::is_json(f), rng, words),
            })
            .collect();
        rows.push(format!("({})", values.join(", ")));
    }
    format!(
        "INSERT INTO \"{}\" VALUES {}",
        t.name.replace('"', "\"\""),
        rows.join(", ")
    )
}

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// The `i`th row's key value: distinct for each row.
fn key(t: &arrow_schema::DataType, i: usize) -> String {
    use arrow_schema::DataType as T;
    match t {
        T::Int16 | T::Int32 | T::Int64 => (i * 3 + 1).to_string(),
        T::Date32 => quote(&format!("2024-01-{:02}", i % 28 + 1)),
        _ => quote(&format!("k{i}")),
    }
}

fn value(t: &arrow_schema::DataType, json: bool, rng: &mut Rng, words: &[String]) -> String {
    use arrow_schema::{DataType as T, TimeUnit};
    match t {
        T::Boolean => rng.pick(&["true", "false"]).into(),
        T::Int16 => rng
            .pick(&[
                "0", "1", "-1", "2", "3", "7", "10", "42", "100", "-100", "32767",
            ])
            .into(),
        T::Int32 => rng
            .pick(&[
                "0",
                "1",
                "-1",
                "2",
                "3",
                "7",
                "10",
                "42",
                "100",
                "-100",
                "65536",
                "2147483647",
                "-2147483648",
            ])
            .into(),
        T::Int64 => rng
            .pick(&[
                "0",
                "1",
                "-1",
                "2",
                "3",
                "7",
                "10",
                "42",
                "100",
                "-100",
                "1000",
                "65536",
                "2147483648",
                "-9007199254740993",
            ])
            .into(),
        T::Float32 | T::Float64 => rng
            .pick(&[
                "0.0", "1.5", "-2.25", "3.0", "0.1", "100.125", "1e10", "-0.5", "2.5", "7.0",
            ])
            .into(),
        T::Decimal128(p, s) => {
            let whole = usize::from(p.saturating_sub(s.unsigned_abs())).min(6);
            let mut v = String::new();
            if rng.below(4) == 0 {
                v.push('-');
            }
            let digits = rng.below(whole + 1).max(1);
            for _ in 0..digits {
                v.push(char::from(b'0' + u8::try_from(rng.below(10)).unwrap_or(0)));
            }
            if *s > 0 {
                v.push('.');
                for _ in 0..*s {
                    v.push(char::from(b'0' + u8::try_from(rng.below(10)).unwrap_or(0)));
                }
            }
            v
        }
        T::Utf8 if json => quote(rng.pick(&[
            r#"{"a": 1, "b": "x", "c": [1, 2, 3], "d": {"e": null}}"#,
            r#"{"name": "n", "n": 7, "tags": ["p", "q"]}"#,
            r#"{"name": null, "n": "8", "user": {"email": "e@x.io"}}"#,
            r#"[1, "two", 3.5, null]"#,
            r#"{"a": {"b": {"c": true}}}"#,
            r#""text""#,
            "7",
            "null",
            "{}",
            "[]",
        ])),
        T::Utf8 | T::Binary => {
            let pool = [
                "",
                "a",
                "A",
                "abc",
                "Hello World",
                "x,y,z",
                "  padded  ",
                "ünïcödé",
                "日本語",
                "50%",
                "a_b",
                "MiXeD",
                "user@example.com",
                "12",
                "-7",
            ];
            let w = match words.is_empty() || rng.below(2) == 0 {
                true => rng.pick(&pool).to_owned(),
                false => words[rng.below(words.len())].clone(),
            };
            // DuckDB reads a BLOB's literal as ASCII, with escapes.
            match t {
                T::Binary if !w.is_ascii() || w.contains('\\') => quote("bytes"),
                _ => quote(&w),
            }
        }
        T::Date32 => quote(rng.pick(&[
            "1970-01-01",
            "1999-12-31",
            "2000-02-29",
            "2024-02-29",
            "2026-11-14",
            "2038-01-19",
        ])),
        T::Timestamp(_, tz) => {
            let at = rng.pick(&[
                "1970-01-01 00:00:00",
                "1999-12-31 23:59:59",
                "2024-02-29 12:30:45.123456",
                "2026-11-14 08:00:00",
                "2024-01-02 03:04:05.5",
            ]);
            match tz {
                Some(_) => quote(&format!("{at}+00:00")),
                None => quote(at),
            }
        }
        T::Time64(TimeUnit::Microsecond) => {
            quote(rng.pick(&["00:00:00", "12:34:56.789", "23:59:59", "08:00:00"]))
        }
        T::List(f) => {
            let n = rng.below(4);
            let items: Vec<String> = (0..n)
                .map(|_| match rng.below(6) {
                    0 => "NULL".into(),
                    _ => value(f.data_type(), false, rng, words),
                })
                .collect();
            format!("[{}]", items.join(", "))
        }
        _ => "NULL".into(),
    }
}

mod tests {
    use super::*;

    fn row(cells: &[&str]) -> Vec<String> {
        cells.iter().map(|c| (*c).to_owned()).collect()
    }

    #[test]
    fn rows_compare_cell_by_cell() {
        let a = Got::Rows(sorted(vec![row(&["a b", "c"])]), 0);
        let b = Got::Rows(sorted(vec![row(&["a", "b c"])]), 0);
        assert!(!a.agrees(&b));
        let spaced = Got::Rows(sorted(vec![row(&["a  b", "c"])]), 0);
        assert!(a.agrees(&spaced));
    }

    #[test]
    fn a_value_that_reads_quarantined_is_a_value() {
        let mut ours = Ours::default();
        for sql in [
            "CREATE TABLE t (id BIGINT NOT NULL, n TEXT, PRIMARY KEY (id))",
            "INSERT INTO t VALUES (1, 'quarantined'), (2, 'x')",
        ] {
            ours.run(sql).unwrap_or_else(|e| panic!("{e}"));
        }
        let ran = ours
            .query("SELECT n, CAST(id AS TEXT) AS id FROM t", true)
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(ran.rows.len(), 2);
        assert!(ran.quarantined.is_empty());
        let ran = ours
            .query("SELECT id, CAST(n AS BIGINT) AS n FROM t", true)
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(ran.quarantined, [(None, 1), (None, 2)]);
    }
}
