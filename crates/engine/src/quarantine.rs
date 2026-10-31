//! Records the engine can't take. Each stream's are kept per epoch as JSON
//! lines in `{g}/{N}/{stream}.jsonl` below the job's quarantine URL, and the
//! epoch's checkpoint lists the files.

use std::sync::Arc;

use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use openetl_proto::connector::v1::ErrorCode;
use url::Url;

use crate::error::JobError;

#[derive(Debug)]
pub struct Quarantine {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    /// The URL that prefix is at.
    base: String,
}

impl Quarantine {
    /// S3 credentials come from AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and
    /// AWS_SESSION_TOKEN; without them, requests are unsigned.
    pub fn open(location: &str) -> Result<Self, JobError> {
        let config = |msg: String| JobError::config(format!("quarantine URL {location:?}: {msg}"));
        let url = Url::parse(location).map_err(|e| config(e.to_string()))?;
        match url.scheme() {
            "file" => {
                let dir = url
                    .to_file_path()
                    .map_err(|_| config("not an absolute path".into()))?;
                std::fs::create_dir_all(&dir).map_err(|e| config(e.to_string()))?;
                let store =
                    LocalFileSystem::new_with_prefix(&dir).map_err(|e| config(e.to_string()))?;
                Ok(Self {
                    store: Arc::new(store),
                    prefix: Path::default(),
                    base: format!("file://{}", dir.display()),
                })
            }
            "s3" => {
                let bucket = url
                    .host_str()
                    .filter(|b| !b.is_empty())
                    .ok_or_else(|| config("no bucket".into()))?;
                let mut b = AmazonS3Builder::new()
                    .with_bucket_name(bucket)
                    .with_region("us-east-1");
                for (k, v) in url.query_pairs() {
                    match k.as_ref() {
                        "region" => b = b.with_region(v.as_ref()),
                        "endpoint" => {
                            b = b
                                .with_allow_http(v.starts_with("http://"))
                                .with_endpoint(v.as_ref())
                        }
                        _ => {}
                    }
                }
                b = match std::env::var("AWS_ACCESS_KEY_ID") {
                    Ok(id) if !id.is_empty() => {
                        let mut b = b.with_access_key_id(id).with_secret_access_key(
                            std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default(),
                        );
                        if let Ok(t) = std::env::var("AWS_SESSION_TOKEN") {
                            b = b.with_token(t);
                        }
                        b
                    }
                    _ => b.with_skip_signature(true),
                };
                let store = b.build().map_err(|e| config(e.to_string()))?;
                let prefix = Path::from(url.path().trim_matches('/'));
                Ok(Self {
                    store: Arc::new(store),
                    base: format!("s3://{bucket}/{prefix}"),
                    prefix,
                })
            }
            other => Err(config(format!("{other}:// isn't supported"))),
        }
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
        for bad in ["relative/path", "gs://bucket/p", "s3:///p"] {
            assert!(Quarantine::open(bad).is_err(), "{bad}");
        }
    }
}
