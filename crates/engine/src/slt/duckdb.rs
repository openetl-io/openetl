//! DuckDB, the suites' oracle, through its CLI: each statement runs in a
//! process of its own, after the tables, rows and views before it, and a
//! query's result comes back as Parquet, which [`super::rows::cell`]
//! writes as it writes ours.

use std::fs::File;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sqllogictest::{DBOutput, DefaultColumnType};

use super::ours::{Failure, Output, statement};
use super::rows::{self, Kind};

/// The CLI to run: `OPENETL_DUCKDB`, which must run, or else `duckdb` on
/// the path. None without either.
pub(super) fn find() -> Result<Option<PathBuf>, String> {
    if let Some(p) = std::env::var_os("OPENETL_DUCKDB") {
        let p = PathBuf::from(p);
        let ok = Command::new(&p)
            .arg("-version")
            .output()
            .is_ok_and(|o| o.status.success());
        return match ok {
            true => Ok(Some(p)),
            false => Err(format!(
                "OPENETL_DUCKDB is {}, which doesn't run",
                p.display()
            )),
        };
    }
    let path = std::env::var_os("PATH").unwrap_or_default();
    Ok(std::env::split_paths(&path)
        .map(|d| d.join("duckdb"))
        .find(|p| p.is_file()))
}

pub(super) struct DuckDb {
    bin: PathBuf,
    /// The statements so far that make its tables, rows and views.
    setup: Vec<String>,
    dir: tempfile::TempDir,
    files: AtomicUsize,
}

impl DuckDb {
    pub(super) fn new(bin: &Path) -> Result<Self, String> {
        let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
        File::create(dir.path().join("init.sql")).map_err(|e| e.to_string())?;
        Ok(Self {
            bin: bin.to_owned(),
            setup: Vec::new(),
            dir,
            files: AtomicUsize::new(0),
        })
    }

    /// Runs the setup, then `last`, in UTC; the error is what DuckDB said.
    fn exec(&self, last: &str) -> Result<(), String> {
        let mut script = String::from("SET TimeZone = 'UTC';\n");
        for s in &self.setup {
            script.push_str(s);
            script.push_str(";\n");
        }
        script.push_str(last);
        script.push_str(";\n");
        // Its own empty init file, so no ~/.duckdbrc applies.
        let mut child = Command::new(&self.bin)
            .arg("-bail")
            .arg("-init")
            .arg(self.dir.path().join("init.sql"))
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("{}: {e}", self.bin.display()))?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(script.as_bytes())
                .map_err(|e| e.to_string())?;
        }
        let out = child.wait_with_output().map_err(|e| e.to_string())?;
        let stderr = String::from_utf8_lossy(&out.stderr).trim().to_owned();
        match out.status.success() && stderr.is_empty() {
            true => Ok(()),
            false => Err(stderr),
        }
    }

    /// The rows a query gives, each value written by [`rows::cell`].
    pub(super) fn query(&self, sql: &str) -> Result<Vec<Vec<String>>, String> {
        let n = self.files.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.path().join(format!("q{n}.parquet"));
        let quoted = path.display().to_string().replace('\'', "''");
        self.exec(&format!("COPY ({sql}) TO '{quoted}' (FORMAT parquet)"))?;
        let file = File::open(&path).map_err(|e| e.to_string())?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .and_then(|b| b.build())
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for b in reader {
            out.extend(rows::rows(&b.map_err(|e| e.to_string())?));
        }
        let _ = std::fs::remove_file(&path);
        Ok(out)
    }

    /// Runs a statement, and keeps it for the statements after it.
    pub(super) fn define(&mut self, sql: &str) -> Result<(), String> {
        self.exec(sql)?;
        self.setup.push(sql.to_owned());
        Ok(())
    }
}

impl sqllogictest::DB for DuckDb {
    type Error = Failure;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<Output, Failure> {
        let sql = statement(sql);
        match rows::kind(sql) {
            Kind::Query => Ok(DBOutput::Rows {
                types: Vec::new(),
                rows: self.query(sql)?,
            }),
            Kind::Table | Kind::Values | Kind::View => {
                self.define(sql)?;
                Ok(DBOutput::StatementComplete(0))
            }
            Kind::Describe | Kind::Transform => Err(Failure(format!(
                "only our engine runs this; mark it onlyif openetl: {sql}"
            ))),
        }
    }

    fn engine_name(&self) -> &str {
        "duckdb"
    }
}
