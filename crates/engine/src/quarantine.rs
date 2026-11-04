//! Records the engine can't take. Each stream's are kept per epoch as JSON
//! lines in `{g}/{N}/{stream}.jsonl` below the job's quarantine URL, and the
//! epoch's checkpoint lists the files.

use std::sync::Arc;

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use openetl_proto::connector::v1::ErrorCode;

use crate::error::JobError;
use crate::store::Location;

#[derive(Debug)]
pub struct Quarantine {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    /// The URL that prefix is at.
    base: String,
}

impl Quarantine {
    /// Opens location as store::Location does.
    pub fn open(location: &str) -> Result<Self, JobError> {
        let l = Location::open(location, "quarantine URL")?;
        Ok(Self {
            store: l.store,
            prefix: l.prefix,
            base: l.base,
        })
    }

    /// Writes one stream's lines for an epoch, and returns the file's URL.
    pub async fn put(
        &self,
        generation: u64,
        epoch: u64,
        stream: &str,
        lines: Vec<u8>,
    ) -> Result<String, JobError> {
        let rel = Path::from_iter([
            generation.to_string(),
            epoch.to_string(),
            format!("{stream}.jsonl"),
        ]);
        let path = Path::from_iter(self.prefix.parts().chain(rel.parts()));
        self.store
            .put(&path, PutPayload::from(lines))
            .await
            .map_err(|e| JobError::new(ErrorCode::Transient, format!("quarantine {path}: {e}")))?;
        Ok(format!("{}/{rel}", self.base.trim_end_matches('/')))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn keeps_lines_per_stream_and_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let q = Quarantine::open(&format!("file://{}/q", dir.path().display())).unwrap();
        let url = q.put(2, 7, "a/b", b"{}\n".to_vec()).await.unwrap();
        assert!(url.ends_with("/q/2/7/a%2Fb.jsonl"), "{url}");
        let file = dir.path().join("q/2/7/a%2Fb.jsonl");
        assert_eq!(std::fs::read(file).unwrap(), b"{}\n");
    }

    #[test]
    fn rejects_other_locations() {
        for bad in [
            "relative/path",
            "ftp://bucket/p",
            "s3:///p",
            "gs:///p",
            "azblob:///p",
        ] {
            assert!(Quarantine::open(bad).is_err(), "{bad}");
        }
    }
}
