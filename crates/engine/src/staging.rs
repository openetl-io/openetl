//! Staging for a destination that loads Parquet files (DELIVERY_STAGED), and
//! for a Spark transform's inputs. Each stream's batches in epoch N go to
//! `{g}/{N}/{stream}/part-{k}.parquet` below the sink's URL, or
//! `{g}/{N}/{dir}/{stream}/part-{k}.parquet` with a dir, with the stream's
//! name percent-encoded, and the epoch's EpochReady lists them.

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use object_store::ObjectStoreExt;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use openetl_proto::connector::v1::{ErrorCode, StagedFile};
use openetl_proto::engine::v1::StagedStream;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;

use crate::error::JobError;
use crate::store::Location;

const MAX_ROW_GROUP_BYTES: usize = 64 << 20;
/// A file ends once it holds this much, so a reader's range requests and a
/// delivery cursor's row groups stay small.
const MAX_FILE_BYTES: usize = 256 << 20;

pub struct Staging {
    location: Location,
    /// Empty for none.
    dir: String,
    generation: u64,
    props: WriterProperties,
    streams: Vec<Stream>,
    sealed: BTreeMap<u64, Vec<StagedStream>>,
}

struct Stream {
    name: String,
    epoch: u64,
    part: Option<Part>,
    /// Closed files of the current epoch.
    files: Vec<StagedFile>,
}

struct Part {
    writer: AsyncArrowWriter<BufWriter>,
    schema: SchemaRef,
    /// Below the sink's URL.
    rel: Path,
    rows: u64,
}

impl Staging {
    pub fn open(
        url: &str,
        dir: &str,
        generation: u64,
        first_epoch: u64,
        streams: &[String],
    ) -> Result<Self, JobError> {
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_bytes(Some(MAX_ROW_GROUP_BYTES))
            .build();
        Ok(Self {
            location: Location::open(url, "staging URL")?,
            dir: dir.to_owned(),
            generation,
            props,
            streams: streams
                .iter()
                .map(|name| Stream {
                    name: name.clone(),
                    epoch: first_epoch,
                    part: None,
                    files: Vec::new(),
                })
                .collect(),
            sealed: BTreeMap::new(),
        })
    }

    /// Adds the batch to the stream's current epoch. A batch whose schema
    /// differs from the open file's starts a new file, and so does one that
    /// fills it.
    pub async fn write(&mut self, stream: usize, batch: &RecordBatch) -> Result<(), JobError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let st = &mut self.streams[stream];
        if let Some(p) = st.part.take_if(|p| p.schema != batch.schema()) {
            st.files.push(finish(p).await?);
        }
        let part = match &mut st.part {
            Some(p) => p,
            None => st.part.insert(open(
                &self.location,
                &self.dir,
                self.generation,
                &self.props,
                st,
                batch.schema(),
            )?),
        };
        part.writer
            .write(batch)
            .await
            .map_err(|e| stage_error(&part.rel, e))?;
        part.rows += batch.num_rows() as u64;
        if part.writer.bytes_written() + part.writer.in_progress_size() >= MAX_FILE_BYTES
            && let Some(p) = st.part.take()
        {
            st.files.push(finish(p).await?);
        }
        Ok(())
    }

    /// Makes the stream's files for its current epoch durable, and moves it
    /// to the next epoch. Returns the bytes staged.
    pub async fn seal(&mut self, stream: usize) -> Result<u64, JobError> {
        let st = &mut self.streams[stream];
        if let Some(p) = st.part.take() {
            st.files.push(finish(p).await?);
        }
        let epoch = st.epoch;
        st.epoch += 1;
        if st.files.is_empty() {
            return Ok(0);
        }
        let files = std::mem::take(&mut st.files);
        let bytes = files.iter().map(|f| f.bytes).sum();
        self.sealed.entry(epoch).or_default().push(StagedStream {
            stream: st.name.clone(),
            files,
        });
        Ok(bytes)
    }

    /// Ends the stream. Rows after its last barrier are never committed, so
    /// their files are removed, and an open file is never completed.
    pub async fn end(&mut self, stream: usize) -> Result<(), JobError> {
        let st = &mut self.streams[stream];
        if let Some(p) = st.part.take() {
            let rel = p.rel.clone();
            p.writer
                .into_inner()
                .abort()
                .await
                .map_err(|e| transient(&rel, &e))?;
        }
        for f in st.files.drain(..) {
            let rel = Path::parse(&f.path).map_err(|e| JobError::internal(e.to_string()))?;
            let path = Path::from_iter(self.location.prefix.parts().chain(rel.parts()));
            match self.location.store.delete(&path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(transient(&path, &e)),
            }
        }
        Ok(())
    }

    /// Hands over what every stream sealed for `epoch`.
    pub fn take(&mut self, epoch: u64) -> Vec<StagedStream> {
        self.sealed.remove(&epoch).unwrap_or_default()
    }
}

fn open(
    location: &Location,
    dir: &str,
    generation: u64,
    props: &WriterProperties,
    st: &Stream,
    schema: SchemaRef,
) -> Result<Part, JobError> {
    let mut parts = vec![generation.to_string(), st.epoch.to_string()];
    if !dir.is_empty() {
        parts.push(dir.to_owned());
    }
    parts.push(st.name.clone());
    parts.push(format!("part-{:05}.parquet", st.files.len()));
    let rel = Path::from_iter(parts);
    let path = Path::from_iter(location.prefix.parts().chain(rel.parts()));
    let out = BufWriter::new(location.store.clone(), path);
    let writer = AsyncArrowWriter::try_new(out, schema.clone(), Some(props.clone()))
        .map_err(|e| stage_error(&rel, e))?;
    Ok(Part {
        writer,
        schema,
        rel,
        rows: 0,
    })
}

async fn finish(mut p: Part) -> Result<StagedFile, JobError> {
    p.writer
        .finish()
        .await
        .map_err(|e| stage_error(&p.rel, e))?;
    Ok(StagedFile {
        path: p.rel.to_string(),
        rows: p.rows,
        bytes: p.writer.bytes_written() as u64,
    })
}

/// The store's failures are retried; encoding failures aren't.
fn stage_error(path: &Path, e: ParquetError) -> JobError {
    match e {
        ParquetError::External(e) => transient(path, &e),
        e => JobError::internal(format!("staging {path}: {e}")),
    }
}

fn transient(path: &Path, e: &dyn std::fmt::Display) -> JobError {
    JobError::new(ErrorCode::Transient, format!("staging {path}: {e}"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;

    fn batch(ids: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids.to_vec()))]).unwrap()
    }

    fn rows(dir: &std::path::Path, f: &StagedFile) -> Vec<i64> {
        let file = std::fs::File::open(dir.join(&f.path)).unwrap();
        assert_eq!(file.metadata().unwrap().len(), f.bytes);
        ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap()
            .flat_map(|b| {
                let b = b.unwrap();
                let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                ids.values().to_vec()
            })
            .collect()
    }

    #[tokio::test]
    async fn stages_each_streams_epochs_and_drops_what_no_barrier_ends() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("file://{}/s", dir.path().display());
        let mut st = Staging::open(&url, "", 3, 5, &["a/b".into(), "c".into()]).unwrap();
        st.write(0, &batch(&[1, 2])).await.unwrap();
        st.write(0, &batch(&[3])).await.unwrap();
        // A new schema starts a new file.
        let wide = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Utf8, true),
        ]));
        let b = RecordBatch::try_new(
            wide,
            vec![
                Arc::new(Int64Array::from(vec![4])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        )
        .unwrap();
        st.write(0, &b).await.unwrap();
        assert!(st.seal(0).await.unwrap() > 0);
        assert_eq!(st.seal(1).await.unwrap(), 0);
        st.write(1, &batch(&[9])).await.unwrap();
        st.write(0, &batch(&[5])).await.unwrap();
        st.end(0).await.unwrap();
        st.seal(1).await.unwrap();

        let five = st.take(5);
        assert_eq!(five.len(), 1);
        assert_eq!(five[0].stream, "a/b");
        let paths: Vec<_> = five[0].files.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(
            paths,
            [
                "3/5/a%2Fb/part-00000.parquet",
                "3/5/a%2Fb/part-00001.parquet"
            ]
        );
        let root = dir.path().join("s");
        assert_eq!(rows(&root, &five[0].files[0]), [1, 2, 3]);
        assert_eq!(rows(&root, &five[0].files[1]), [4]);
        assert_eq!(five[0].files.iter().map(|f| f.rows).sum::<u64>(), 4);

        let six = st.take(6);
        assert_eq!(six.len(), 1);
        assert_eq!(six[0].stream, "c");
        assert_eq!(rows(&root, &six[0].files[0]), [9]);
        // a/b's rows after its last barrier left nothing behind.
        assert!(
            !root.join("3/6/a%2Fb").exists()
                || std::fs::read_dir(root.join("3/6/a%2Fb"))
                    .unwrap()
                    .next()
                    .is_none()
        );
        assert!(st.take(7).is_empty());
    }

    #[tokio::test]
    async fn stages_in_each_epochs_dir() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("file://{}/s", dir.path().display());
        let mut st = Staging::open(&url, "in", 2, 7, &["a".into()]).unwrap();
        st.write(0, &batch(&[1])).await.unwrap();
        st.seal(0).await.unwrap();
        let seven = st.take(7);
        assert_eq!(seven[0].files[0].path, "2/7/in/a/part-00000.parquet");
        assert_eq!(rows(&dir.path().join("s"), &seven[0].files[0]), [1]);
    }
}
