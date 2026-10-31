//! Records the engine can't take. Each stream's are kept per epoch as JSON
//! lines in `{g}/{N}/{stream}.jsonl` below the job's quarantine URL, and the
//! epoch's checkpoint lists the files.

use std::sync::Arc;

use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::{GcpCredential, GoogleCloudStorageBuilder};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    ClientOptions, ObjectStore, ObjectStoreExt, PutPayload, StaticCredentialProvider,
};
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
    /// Credentials come from the environment, as the runner's do:
    /// AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN for S3
    /// (unsigned without them); Google's application default credentials
    /// for GCS, or none with STORAGE_EMULATOR_HOST; AZURE_STORAGE_KEY for
    /// Azure, or Azure's default credential chain.
    pub fn open(location: &str) -> Result<Self, JobError> {
        let config = |msg: String| JobError::config(format!("quarantine URL {location:?}: {msg}"));
        let url = Url::parse(location).map_err(|e| config(e.to_string()))?;
        if url.scheme() == "file" {
            let dir = url
                .to_file_path()
                .map_err(|_| config("not an absolute path".into()))?;
            std::fs::create_dir_all(&dir).map_err(|e| config(e.to_string()))?;
            let store =
                LocalFileSystem::new_with_prefix(&dir).map_err(|e| config(e.to_string()))?;
            return Ok(Self {
                store: Arc::new(store),
                prefix: Path::default(),
                base: format!("file://{}", dir.display()),
            });
        }
        let bucket = url
            .host_str()
            .filter(|b| !b.is_empty())
            .ok_or_else(|| config("no bucket".into()))?;
        let query = |key: &str| {
            url.query_pairs()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.into_owned())
        };
        let store: Arc<dyn ObjectStore> = match url.scheme() {
            "s3" => {
                let mut b = AmazonS3Builder::new()
                    .with_bucket_name(bucket)
                    .with_region(query("region").unwrap_or_else(|| "us-east-1".into()));
                if let Some(endpoint) = query("endpoint") {
                    b = b
                        .with_allow_http(endpoint.starts_with("http://"))
                        .with_endpoint(endpoint);
                }
                b = match env("AWS_ACCESS_KEY_ID") {
                    Some(id) => {
                        let mut b = b.with_access_key_id(id).with_secret_access_key(
                            env("AWS_SECRET_ACCESS_KEY").unwrap_or_default(),
                        );
                        if let Some(t) = env("AWS_SESSION_TOKEN") {
                            b = b.with_token(t);
                        }
                        b
                    }
                    None => b.with_skip_signature(true),
                };
                Arc::new(b.build().map_err(|e| config(e.to_string()))?)
            }
            "gs" => {
                let mut b = GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket);
                if let Some(host) = env("STORAGE_EMULATOR_HOST") {
                    let base = if host.contains("://") {
                        host
                    } else {
                        format!("http://{host}")
                    };
                    // Unsigned: skip_signature still fetches a token for writes.
                    b = b
                        .with_base_url(&base)
                        .with_credentials(Arc::new(StaticCredentialProvider::new(GcpCredential {
                            bearer: String::new(),
                        })))
                        .with_client_options(ClientOptions::new().with_allow_http(true));
                }
                Arc::new(b.build().map_err(|e| config(e.to_string()))?)
            }
            "azblob" => {
                let account = query("account")
                    .or_else(|| env("AZURE_STORAGE_ACCOUNT"))
                    .ok_or_else(|| {
                        config("no account, and AZURE_STORAGE_ACCOUNT is unset".into())
                    })?;
                let mut b = MicrosoftAzureBuilder::from_env()
                    .with_container_name(bucket)
                    .with_account(account);
                if let Some(endpoint) = query("endpoint") {
                    b = b
                        .with_allow_http(endpoint.starts_with("http://"))
                        .with_endpoint(endpoint);
                }
                if let Some(key) = env("AZURE_STORAGE_KEY") {
                    b = b.with_access_key(key);
                }
                Arc::new(b.build().map_err(|e| config(e.to_string()))?)
            }
            other => return Err(config(format!("{other}:// isn't supported"))),
        };
        let prefix = Path::from(url.path().trim_matches('/'));
        Ok(Self {
            store,
            base: format!("{}://{bucket}/{prefix}", url.scheme()),
            prefix,
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

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
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
