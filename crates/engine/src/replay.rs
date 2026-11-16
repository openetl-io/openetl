//! Replays a stream's held values: reads its held files in
//! order, from where an earlier attempt's replay got, a chunk at a time.

use std::collections::HashSet;

use arrow_array::RecordBatch;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use openetl_normalize::hold::{CELL, HOLD_KIND_KEY};
use openetl_proto::connector::v1::ErrorCode;
use openetl_proto::engine::v1::ReplayProgress;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

use crate::error::JobError;
use crate::plan::ReplayPlan;
use crate::store::Location;

/// Reads a whole held file at a time: the staging writer ends files at
/// 256 MiB.
pub struct Replayer {
    location: Location,
    plan: ReplayPlan,
    reading: Option<ParquetRecordBatchReader>,
}

impl Replayer {
    pub fn open(url: &str, plan: ReplayPlan) -> Result<Self, JobError> {
        Ok(Self {
            location: Location::open(url, "held URL")?,
            plan,
            reading: None,
        })
    }

    /// The released columns that `held`, a batch of held file `file`, is
    /// read for: a batch holds cells, or columns whole.
    pub fn columns(&self, file: usize, held: &RecordBatch) -> HashSet<String> {
        let cells = held.schema().fields().iter().any(|f| {
            f.metadata()
                .get(HOLD_KIND_KEY)
                .is_some_and(|k| k.as_str() == CELL)
        });
        let first = match cells {
            true => &self.plan.first_cells,
            false => &self.plan.first,
        };
        let read = |c: &&String| first.get(*c).is_none_or(|first| *first <= file);
        self.plan.columns.iter().filter(read).cloned().collect()
    }

    pub fn done(&self) -> bool {
        self.plan.file >= self.plan.files.len()
    }

    pub fn progress(&self) -> ReplayProgress {
        ReplayProgress {
            file: self.plan.file as u32,
            row: self.plan.row,
            done: self.done(),
            still_held: self.plan.still_held,
        }
    }

    /// Counts cells read that still don't fit.
    pub fn still_held(&mut self, n: u64) {
        self.plan.still_held += n;
    }

    /// The next chunk: whole batches of held values, each with its file's
    /// index, until it holds about the plan's chunk rows or the files end.
    pub async fn chunk(&mut self) -> Result<Vec<(usize, RecordBatch)>, JobError> {
        let mut out = Vec::new();
        let mut rows = 0;
        while rows < self.plan.chunk_rows && !self.done() {
            if self.reading.is_none() {
                let rel = Path::parse(&self.plan.files[self.plan.file])
                    .map_err(|e| JobError::internal(format!("a held file's path: {e}")))?;
                let path = Path::from_iter(self.location.prefix.parts().chain(rel.parts()));
                let bytes = fetch(self.location.store.clone(), path)
                    .await
                    .map_err(|e| self.fetch_error(e))?;
                let batch = self.plan.chunk_rows.clamp(1, 8192) as usize;
                let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
                    .and_then(|b| {
                        b.with_batch_size(batch)
                            .with_offset(self.plan.row as usize)
                            .build()
                    })
                    .map_err(|e| self.corrupt(e))?;
                self.reading = Some(reader);
            }
            match self.reading.as_mut().and_then(Iterator::next) {
                Some(b) => {
                    let b = b.map_err(|e| self.corrupt(e))?;
                    self.plan.row += b.num_rows() as u64;
                    rows += b.num_rows() as u64;
                    out.push((self.plan.file, b));
                }
                None => {
                    self.reading = None;
                    self.plan.file += 1;
                    self.plan.row = 0;
                }
            }
        }
        Ok(out)
    }

    /// A held file that's gone or unreadable never comes back, so only the
    /// store's other failures are retried.
    fn fetch_error(&self, e: object_store::Error) -> JobError {
        use object_store::Error as E;
        match &e {
            E::NotFound { .. } => self.corrupt(e),
            E::PermissionDenied { .. } | E::Unauthenticated { .. } => {
                self.error(ErrorCode::PermissionDenied, e)
            }
            _ => self.error(ErrorCode::Transient, e),
        }
    }

    /// The file's bytes are in memory, so reading them fails only on what
    /// they hold.
    fn corrupt(&self, e: impl std::fmt::Display) -> JobError {
        self.error(
            ErrorCode::CheckpointCorrupt,
            format!("{e}: restore the file below _held, then resume the pipeline"),
        )
    }

    fn error(&self, code: ErrorCode, e: impl std::fmt::Display) -> JobError {
        JobError::new(
            code,
            format!("held file {}: {e}", self.plan.files[self.plan.file]),
        )
    }
}

async fn fetch(
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    path: Path,
) -> Result<bytes::Bytes, object_store::Error> {
    store.get(&path).await?.bytes().await
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use openetl_normalize::hold::COLUMN;

    use super::*;

    fn replayer(dir: &std::path::Path) -> Replayer {
        let plan = ReplayPlan {
            files: vec!["1/2/s/part-00000.parquet".into()],
            chunk_rows: 10,
            ..ReplayPlan::default()
        };
        Replayer::open(&format!("file://{}", dir.display()), plan).unwrap()
    }

    #[test]
    fn a_column_is_read_from_its_first_file_on() {
        let plan = ReplayPlan {
            files: vec!["1/1/s/part-00000.parquet".into(); 3],
            columns: HashSet::from(["a".to_owned(), "b".to_owned()]),
            first: HashMap::from([("a".to_owned(), 2)]),
            first_cells: HashMap::from([("b".to_owned(), 3)]),
            ..ReplayPlan::default()
        };
        let held = |kind: &str| {
            let f = Field::new("a", DataType::Utf8, true)
                .with_metadata(HashMap::from([(HOLD_KIND_KEY.to_owned(), kind.to_owned())]));
            RecordBatch::new_empty(Arc::new(Schema::new(vec![f])))
        };
        let names = |c: HashSet<String>| {
            let mut c: Vec<String> = c.into_iter().collect();
            c.sort();
            c
        };
        let dir = tempfile::tempdir().unwrap();
        let r = Replayer::open(&format!("file://{}", dir.path().display()), plan).unwrap();
        assert_eq!(names(r.columns(1, &held(COLUMN))), ["b"]);
        assert_eq!(names(r.columns(2, &held(COLUMN))), ["a", "b"]);
        assert_eq!(names(r.columns(2, &held(CELL))), ["a"]);
        assert_eq!(names(r.columns(3, &held(CELL))), ["a", "b"]);
    }

    #[tokio::test]
    async fn a_held_file_thats_gone_isnt_retried() {
        let dir = tempfile::tempdir().unwrap();
        let err = replayer(dir.path()).chunk().await.unwrap_err();
        assert_eq!(err.code, ErrorCode::CheckpointCorrupt, "{err:?}");
        assert!(
            err.message
                .ends_with("restore the file below _held, then resume the pipeline"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_held_file_that_isnt_parquet_isnt_retried() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("1/2/s/part-00000.parquet");
        std::fs::create_dir_all(f.parent().unwrap()).unwrap();
        std::fs::write(&f, b"not parquet").unwrap();
        let err = replayer(dir.path()).chunk().await.unwrap_err();
        assert_eq!(err.code, ErrorCode::CheckpointCorrupt, "{err:?}");
        assert!(
            err.message
                .ends_with("restore the file below _held, then resume the pipeline"),
            "{err:?}"
        );
    }
}
