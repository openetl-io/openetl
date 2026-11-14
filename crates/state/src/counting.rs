//! An object store that counts requests and bytes, adds a fixed latency to
//! each request, and does conditional puts itself, for a local store whose
//! filesystem can't (no hard links). For measuring, not for production.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyMode, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    UploadPart,
};
use tokio::sync::Mutex;

/// Requests and bytes, by kind.
#[derive(Default, Debug)]
pub struct Counters {
    pub puts: AtomicU64,
    pub put_bytes: AtomicU64,
    pub gets: AtomicU64,
    pub get_bytes: AtomicU64,
    pub heads: AtomicU64,
    pub lists: AtomicU64,
    pub deletes: AtomicU64,
    pub copies: AtomicU64,
    /// Of the puts and gets, those of a manifest or compaction state: a path
    /// with `manifest` or `compactions` in it.
    pub meta_puts: AtomicU64,
    pub meta_gets: AtomicU64,
}

/// A copy of [`Counters`] at a moment.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct Counts {
    pub puts: u64,
    pub put_bytes: u64,
    pub gets: u64,
    pub get_bytes: u64,
    pub heads: u64,
    pub lists: u64,
    pub deletes: u64,
    pub copies: u64,
    pub meta_puts: u64,
    pub meta_gets: u64,
}

impl Counts {
    pub fn requests(&self) -> u64 {
        self.puts + self.gets + self.heads + self.lists + self.deletes + self.copies
    }
}

impl std::ops::Sub for Counts {
    type Output = Counts;
    fn sub(self, o: Counts) -> Counts {
        Counts {
            puts: self.puts - o.puts,
            put_bytes: self.put_bytes - o.put_bytes,
            gets: self.gets - o.gets,
            get_bytes: self.get_bytes - o.get_bytes,
            heads: self.heads - o.heads,
            lists: self.lists - o.lists,
            deletes: self.deletes - o.deletes,
            copies: self.copies - o.copies,
            meta_puts: self.meta_puts - o.meta_puts,
            meta_gets: self.meta_gets - o.meta_gets,
        }
    }
}

impl std::ops::Add for Counts {
    type Output = Counts;
    fn add(self, o: Counts) -> Counts {
        Counts {
            puts: self.puts + o.puts,
            put_bytes: self.put_bytes + o.put_bytes,
            gets: self.gets + o.gets,
            get_bytes: self.get_bytes + o.get_bytes,
            heads: self.heads + o.heads,
            lists: self.lists + o.lists,
            deletes: self.deletes + o.deletes,
            copies: self.copies + o.copies,
            meta_puts: self.meta_puts + o.meta_puts,
            meta_gets: self.meta_gets + o.meta_gets,
        }
    }
}

impl Counters {
    pub fn counts(&self) -> Counts {
        let l = |a: &AtomicU64| a.load(Ordering::Relaxed);
        Counts {
            puts: l(&self.puts),
            put_bytes: l(&self.put_bytes),
            gets: l(&self.gets),
            get_bytes: l(&self.get_bytes),
            heads: l(&self.heads),
            lists: l(&self.lists),
            deletes: l(&self.deletes),
            copies: l(&self.copies),
            meta_puts: l(&self.meta_puts),
            meta_gets: l(&self.meta_gets),
        }
    }
}

fn add(a: &AtomicU64, n: u64) {
    a.fetch_add(n, Ordering::Relaxed);
}

fn meta(p: &Path) -> bool {
    let p = p.as_ref();
    p.contains("manifest") || p.contains("compactions")
}

/// Wraps a store. Every wrapper of one inner store shares its lock for
/// conditional puts, so give them the same [`Shared`].
#[derive(Clone)]
pub struct Counting {
    inner: Arc<dyn ObjectStore>,
    counters: Arc<Counters>,
    latency: Duration,
    shared: Arc<Shared>,
}

/// What wrappers of one inner store share.
#[derive(Default)]
pub struct Shared {
    lock: Mutex<()>,
}

impl Counting {
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        latency: Duration,
        shared: Arc<Shared>,
    ) -> (Arc<Self>, Arc<Counters>) {
        let counters = Arc::new(Counters::default());
        let s = Arc::new(Self {
            inner,
            counters: counters.clone(),
            latency,
            shared,
        });
        (s, counters)
    }

    async fn wait(&self) {
        if !self.latency.is_zero() {
            tokio::time::sleep(self.latency).await;
        }
    }
}

impl fmt::Debug for Counting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Counting({})", self.inner)
    }
}

impl fmt::Display for Counting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Counting({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for Counting {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.wait().await;
        add(&self.counters.puts, 1);
        add(&self.counters.put_bytes, payload.content_length() as u64);
        if meta(location) {
            add(&self.counters.meta_puts, 1);
        }
        let plain = PutOptions {
            mode: PutMode::Overwrite,
            tags: opts.tags.clone(),
            ..Default::default()
        };
        match &opts.mode {
            PutMode::Overwrite => self.inner.put_opts(location, payload, plain).await,
            PutMode::Create => {
                let _g = self.shared.lock.lock().await;
                match self.inner.head(location).await {
                    Ok(_) => Err(object_store::Error::AlreadyExists {
                        path: location.to_string(),
                        source: "exists".into(),
                    }),
                    Err(object_store::Error::NotFound { .. }) => {
                        self.inner.put_opts(location, payload, plain).await
                    }
                    Err(e) => Err(e),
                }
            }
            PutMode::Update(v) => {
                let _g = self.shared.lock.lock().await;
                let meta = self.inner.head(location).await?;
                if meta.e_tag != v.e_tag {
                    return Err(object_store::Error::Precondition {
                        path: location.to_string(),
                        source: "e_tag changed".into(),
                    });
                }
                self.inner.put_opts(location, payload, plain).await
            }
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.wait().await;
        add(&self.counters.puts, 1);
        let opts = PutMultipartOptions {
            tags: opts.tags,
            ..Default::default()
        };
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(Upload {
            inner,
            counters: self.counters.clone(),
            latency: self.latency,
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.wait().await;
        add(&self.counters.gets, 1);
        if meta(location) {
            add(&self.counters.meta_gets, 1);
        }
        let head = options.head;
        let r = self.inner.get_opts(location, options).await?;
        if !head {
            add(&self.counters.get_bytes, r.range.end - r.range.start);
        }
        Ok(r)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let counters = self.counters.clone();
        let counted = locations
            .map(move |p| {
                add(&counters.deletes, 1);
                p
            })
            .boxed();
        self.inner.delete_stream(counted)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        add(&self.counters.lists, 1);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.wait().await;
        add(&self.counters.lists, 1);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.wait().await;
        add(&self.counters.copies, 1);
        match options.mode {
            CopyMode::Overwrite => self.inner.copy_opts(from, to, options).await,
            CopyMode::Create => {
                let _g = self.shared.lock.lock().await;
                match self.inner.head(to).await {
                    Ok(_) => Err(object_store::Error::AlreadyExists {
                        path: to.to_string(),
                        source: "exists".into(),
                    }),
                    Err(object_store::Error::NotFound { .. }) => {
                        let data = self.inner.get(from).await?.bytes().await?;
                        self.inner.put(to, data.into()).await.map(|_| ())
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }
}

#[derive(Debug)]
struct Upload {
    inner: Box<dyn MultipartUpload>,
    counters: Arc<Counters>,
    latency: Duration,
}

#[async_trait]
impl MultipartUpload for Upload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        add(&self.counters.puts, 1);
        add(&self.counters.put_bytes, data.content_length() as u64);
        let part = self.inner.put_part(data);
        let latency = self.latency;
        Box::pin(async move {
            if !latency.is_zero() {
                tokio::time::sleep(latency).await;
            }
            part.await
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        if !self.latency.is_zero() {
            tokio::time::sleep(self.latency).await;
        }
        add(&self.counters.puts, 1);
        self.inner.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}
