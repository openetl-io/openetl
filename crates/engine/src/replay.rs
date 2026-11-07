//! Replays a stream's held values: reads its held files in
//! order, from where an earlier attempt's replay got, a chunk at a time.

use arrow_array::RecordBatch;
use object_store::ObjectStoreExt;
use object_store::path::Path;
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

    pub fn columns(&self) -> &std::collections::HashSet<String> {
        &self.plan.columns
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

    /// The next chunk: whole batches of held values, until it holds about
    /// the plan's chunk rows or the files end.
    pub async fn chunk(&mut self) -> Result<Vec<RecordBatch>, JobError> {
        let mut out = Vec::new();
        let mut rows = 0;
        while rows < self.plan.chunk_rows && !self.done() {
            if self.reading.is_none() {
                let rel = Path::parse(&self.plan.files[self.plan.file])
                    .map_err(|e| JobError::internal(format!("a held file's path: {e}")))?;
                let path = Path::from_iter(self.location.prefix.parts().chain(rel.parts()));
                let bytes = fetch(self.location.store.clone(), path)
                    .await
                    .map_err(|e| self.error(e))?;
                let batch = self.plan.chunk_rows.clamp(1, 8192) as usize;
                let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
                    .and_then(|b| {
                        b.with_batch_size(batch)
                            .with_offset(self.plan.row as usize)
                            .build()
                    })
                    .map_err(|e| self.error(e))?;
                self.reading = Some(reader);
            }
            match self.reading.as_mut().and_then(Iterator::next) {
                Some(b) => {
                    let b = b.map_err(|e| self.error(e))?;
                    self.plan.row += b.num_rows() as u64;
                    rows += b.num_rows() as u64;
                    out.push(b);
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

    fn error(&self, e: impl std::fmt::Display) -> JobError {
        JobError::new(
            ErrorCode::Transient,
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
