//! The SQL suites in `tests/sql/*.slt`, in sqllogictest's format: tier 1's
//! statements run through our planner and the row-wise operator a lane
//! runs, and through DuckDB where its CLI is found, against the same
//! expected results; then each query both run, over generated rows, with
//! the two results compared.
//!
//! - `OPENETL_SQL_REWRITE=1` writes what ours gives into the files.
//! - `OPENETL_DUCKDB` names the CLI, which must run; without it, `duckdb`
//!   on the path, and the DuckDB tests skip where there's none.
//! - `OPENETL_SQL_SEEDS` sets how many seeds generate rows (3).
//!
//! A record that one engine skips says why in a comment just above it, and
//! a record that expects an error is ours alone: DuckDB runs what we
//! refuse. Where the two differ on purpose, a file has both results, each
//! under `onlyif` its engine.

mod duckdb;
mod generate;
mod ours;
mod rows;

use std::path::{Path, PathBuf};

use sqllogictest::{
    Condition, DefaultColumnType, QueryExpect, Record, Runner, StatementExpect,
    default_column_validator, default_normalizer, default_validator,
};

use self::duckdb::DuckDb;
use self::ours::{Failure, Ours};

/// Files only ours runs, and why.
const OURS_ONLY: &[(&str, &str)] = &[
    (
        "changelog.slt",
        "changelogs are ours: a DuckDB table has rows, not inserts, updates and deletes of a key",
    ),
    (
        "names.slt",
        "a stream's whole name, dotted or quoted, is ours: DuckDB reads a dotted name as a schema's table",
    ),
    (
        "refusals.slt",
        "refusals are ours: DuckDB runs what transforms refuse",
    ),
];

fn files() -> Vec<PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/sql");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "slt"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no suites in {}", dir.display());
    files
}

fn file_name(p: &Path) -> String {
    p.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn ours_only(p: &Path) -> bool {
    OURS_ONLY.iter().any(|(f, _)| *f == file_name(p))
}

/// Runs `f` on each file in a thread of its own, and gives the failures.
fn each(files: &[PathBuf], f: impl Fn(&Path) -> Result<(), String> + Sync) -> Vec<String> {
    std::thread::scope(|s| {
        let handles: Vec<_> = files
            .iter()
            .map(|p| {
                let f = &f;
                s.spawn(move || f(p).err())
            })
            .collect();
        handles
            .into_iter()
            .filter_map(|h| h.join().unwrap_or_else(|e| std::panic::resume_unwind(e)))
            .collect()
    })
}

#[test]
fn suites_run_through_our_planner_and_operator() {
    let rewrite = std::env::var_os("OPENETL_SQL_REWRITE").is_some();
    let failed = each(&files(), |p| {
        let mut runner = Runner::new(|| async { Ok::<_, Failure>(Ours::default()) });
        if rewrite {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .map_err(|e| e.to_string())?;
            return rt
                .block_on(runner.update_test_file(
                    p,
                    " ",
                    default_validator,
                    default_normalizer,
                    default_column_validator,
                ))
                .map_err(|e| e.to_string());
        }
        runner.run_file(p).map_err(|e| e.display(false).to_string())
    });
    assert!(
        failed.is_empty(),
        "{} suites fail; OPENETL_SQL_REWRITE=1 writes what ours gives:\n\n{}",
        failed.len(),
        failed.join("\n\n")
    );
}

/// The DuckDB CLI, or None where there's none and the test should skip.
fn duckdb_bin() -> Option<PathBuf> {
    match duckdb::find() {
        Ok(Some(bin)) => Some(bin),
        Ok(None) => {
            eprintln!(
                "no duckdb on the path, and no OPENETL_DUCKDB: the comparison with DuckDB is skipped"
            );
            None
        }
        Err(e) => panic!("{e}"),
    }
}

#[test]
fn suites_give_what_duckdb_gives() {
    let Some(bin) = duckdb_bin() else { return };
    let files: Vec<PathBuf> = files().into_iter().filter(|p| !ours_only(p)).collect();
    let failed = each(&files, |p| {
        let records: Vec<Record<DefaultColumnType>> =
            sqllogictest::parse_file(p).map_err(|e| e.to_string())?;
        let records = records.into_iter().filter(|r| !expects_error(r));
        let bin = bin.clone();
        let mut runner = Runner::new(move || {
            let db = DuckDb::new(&bin).map_err(Failure);
            async move { db }
        });
        let failed: Vec<String> = records
            .filter_map(|r| runner.run(r).err())
            .map(|e| e.display(false).to_string())
            .collect();
        match failed.is_empty() {
            true => Ok(()),
            false => Err(failed.join("\n")),
        }
    });
    assert!(
        failed.is_empty(),
        "{} suites give other results in DuckDB:\n\n{}",
        failed.len(),
        failed.join("\n\n")
    );
}

#[test]
fn queries_over_generated_rows_give_what_duckdb_gives() {
    let Some(bin) = duckdb_bin() else { return };
    let n: u64 = std::env::var("OPENETL_SQL_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    let seeds: Vec<u64> = (1..=n).collect();
    let files: Vec<PathBuf> = files().into_iter().filter(|p| !ours_only(p)).collect();
    let results = std::thread::scope(|s| {
        let handles: Vec<_> = files
            .iter()
            .map(|p| {
                let (bin, seeds) = (&bin, &seeds);
                s.spawn(move || generate::compare(p, bin, seeds))
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|e| std::panic::resume_unwind(e)))
            .collect::<Vec<_>>()
    });
    let mut mismatches = Vec::new();
    let mut used = vec![false; generate::DIFFERS.len()];
    for r in results {
        let (m, u) = r.unwrap_or_else(|e| panic!("{e}"));
        mismatches.extend(m);
        for i in u {
            used[i] = true;
        }
    }
    for (i, (file, part, why)) in generate::DIFFERS.iter().enumerate() {
        if !used[i] && n >= 3 {
            mismatches.push(format!(
                "{file}: the query with {part:?} agrees with DuckDB now, so it's no longer an exception ({why})"
            ));
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} queries differ from DuckDB over generated rows:\n\n{}",
        mismatches.len(),
        mismatches.join("\n\n")
    );
}

fn expects_error(r: &Record<DefaultColumnType>) -> bool {
    matches!(
        r,
        Record::Statement {
            expected: StatementExpect::Error(_),
            ..
        } | Record::Query {
            expected: QueryExpect::Error(_),
            ..
        }
    )
}

/// Each function in `crates/sql/functions.txt`, by its name or an alias, is
/// called in a suite.
#[test]
fn every_function_is_called_in_a_suite() {
    let list = Path::new(env!("CARGO_MANIFEST_DIR")).join("../sql/functions.txt");
    let list = std::fs::read_to_string(&list).unwrap_or_else(|e| panic!("{}: {e}", list.display()));
    let suites: String = files()
        .iter()
        .map(|p| {
            std::fs::read_to_string(p)
                .unwrap_or_default()
                .to_lowercase()
        })
        .collect();
    // Called as SQL's keywords are, without parentheses.
    let bare = ["current_date", "current_timestamp"];
    let called = |name: &str| {
        let call = match bare.contains(&name) {
            true => name.to_owned(),
            false => format!("{name}("),
        };
        suites.match_indices(&call).any(|(i, _)| {
            !suites[..i]
                .chars()
                .next_back()
                .is_some_and(|c| c.is_alphanumeric() || c == '_')
        })
    };
    let mut missing = Vec::new();
    for line in list
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
    {
        let names: Vec<&str> = line
            .split([' ', '(', ')', ','])
            .filter(|w| !w.is_empty() && *w != "not" && *w != "deterministic")
            .collect();
        if !names.iter().any(|n| called(n)) {
            missing.push(names[0]);
        }
    }
    assert!(missing.is_empty(), "no suite calls {}", missing.join(", "));
}

/// A record that runs on one engine says why, in a comment just above it,
/// unless it's the second of a pair, the same SQL under each engine, or one
/// only ours runs by its kind: DESCRIBE, a transform's statements as
/// written, and a refusal.
#[test]
fn a_record_one_engine_skips_says_why() {
    let mut unexplained = Vec::new();
    for p in files() {
        let records: Vec<Record<DefaultColumnType>> =
            sqllogictest::parse_file(&p).unwrap_or_else(|e| panic!("{}: {e}", p.display()));
        let mut commented = false;
        // A pair's second record shares the first's comment.
        let mut last: Option<&str> = None;
        for r in &records {
            let (conditions, loc, sql) = match r {
                Record::Comment(_) => {
                    commented = true;
                    continue;
                }
                Record::Condition(_) => continue,
                Record::Statement {
                    conditions,
                    loc,
                    sql,
                    ..
                }
                | Record::Query {
                    conditions,
                    loc,
                    sql,
                    ..
                } => (conditions, loc, sql.as_str()),
                _ => {
                    commented = false;
                    continue;
                }
            };
            let skips = conditions.iter().any(|c| match c {
                Condition::OnlyIf { label } | Condition::SkipIf { label } => {
                    label == "openetl" || label == "duckdb"
                }
            });
            let ours_by_kind = matches!(
                rows::kind(ours::statement(sql)),
                rows::Kind::Describe | rows::Kind::Transform
            );
            let explained = commented || last == Some(sql) || ours_by_kind;
            if skips && !explained && !expects_error(r) && !ours_only(&p) {
                unexplained.push(format!("{}:{}", file_name(&p), loc.line()));
            }
            commented = false;
            last = Some(sql);
        }
    }
    assert!(
        unexplained.is_empty(),
        "these run on one engine without a comment saying why: {}",
        unexplained.join(", ")
    );
}
