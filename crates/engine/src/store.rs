//! Object stores the engine writes to, named by URL.

use std::sync::Arc;

use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::{GcpCredential, GoogleCloudStorageBuilder};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ClientOptions, ObjectStore, StaticCredentialProvider};
use url::Url;

use crate::error::JobError;

/// A store, the prefix a URL names in it, and that prefix's URL.
#[derive(Debug)]
pub struct Location {
    pub store: Arc<dyn ObjectStore>,
    pub prefix: Path,
    pub base: String,
}

impl Location {
    /// Opens file:// and an absolute path, or s3://bucket/prefix with region,
    /// endpoint and anonymous as query parameters, gs://bucket/prefix, or
    /// azblob://container/prefix with account and endpoint. `what` names
    /// the URL in errors. An s3:// URL without a region uses AWS_REGION,
    /// AWS_DEFAULT_REGION or us-east-1; the runner gives the engine URLs
    /// with the bucket's region.
    ///
    /// Credentials come from the environment, as the runner's do: for S3,
    /// AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN, else
    /// a web identity, the container's role or the instance's, and none
    /// with anonymous=true; the runner reads AWS profiles too, the engine
    /// doesn't. Google's application default credentials for GCS, or none
    /// with STORAGE_EMULATOR_HOST; AZURE_STORAGE_KEY for Azure, or Azure's
    /// default credential chain.
    pub fn open(location: &str, what: &str) -> Result<Self, JobError> {
        let config = |msg: String| JobError::config(format!("{what} {location:?}: {msg}"));
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
                let region = query("region")
                    .or_else(|| env("AWS_REGION"))
                    .or_else(|| env("AWS_DEFAULT_REGION"))
                    .unwrap_or_else(|| "us-east-1".into());
                let mut b = AmazonS3Builder::new()
                    .with_bucket_name(bucket)
                    .with_region(region);
                if let Some(endpoint) = query("endpoint") {
                    b = b
                        .with_allow_http(endpoint.starts_with("http://"))
                        .with_endpoint(endpoint);
                }
                let anonymous = match query("anonymous").as_deref() {
                    None | Some("false") => false,
                    Some("true") => true,
                    Some(v) => {
                        return Err(config(format!("anonymous={v} is neither true nor false")));
                    }
                };
                b = match env("AWS_ACCESS_KEY_ID") {
                    _ if anonymous => b.with_skip_signature(true),
                    Some(id) => {
                        let mut b = b.with_access_key_id(id).with_secret_access_key(
                            env("AWS_SECRET_ACCESS_KEY").unwrap_or_default(),
                        );
                        if let Some(t) = env("AWS_SESSION_TOKEN") {
                            b = b.with_token(t);
                        }
                        b
                    }
                    // The builder signs with the first of a web identity,
                    // an ECS task's role and EKS Pod Identity these set,
                    // or else with the instance's role.
                    None => AWS_ROLE_ENV
                        .iter()
                        .fold(b, |b, &(var, key)| match env(var) {
                            Some(v) => b.with_config(key, v),
                            None => b,
                        }),
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
}

/// What AWS's SDKs read for a web identity (EKS IRSA), an ECS task's role
/// and EKS Pod Identity.
const AWS_ROLE_ENV: [(&str, AmazonS3ConfigKey); 6] = [
    (
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        AmazonS3ConfigKey::WebIdentityTokenFile,
    ),
    ("AWS_ROLE_ARN", AmazonS3ConfigKey::RoleArn),
    ("AWS_ROLE_SESSION_NAME", AmazonS3ConfigKey::RoleSessionName),
    (
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        AmazonS3ConfigKey::ContainerCredentialsRelativeUri,
    ),
    (
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        AmazonS3ConfigKey::ContainerCredentialsFullUri,
    ),
    (
        "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
        AmazonS3ConfigKey::ContainerAuthorizationTokenFile,
    ),
];

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn anonymous_is_true_or_false() {
        for ok in [
            "s3://b/p?endpoint=http://127.0.0.1:1",
            "s3://b/p?anonymous=true&endpoint=http://127.0.0.1:1",
            "s3://b/p?anonymous=false&region=eu-west-1",
        ] {
            let l = Location::open(ok, "test").unwrap();
            assert_eq!(l.base, "s3://b/p", "{ok}");
        }
        assert!(Location::open("s3://b/p?anonymous=yes", "test").is_err());
    }
}
