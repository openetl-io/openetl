//! SlateDB as the store: one database per lane's key-group range and
//! generation, at `kg{first}-{last}/g{generation}/`, without its WAL.
//!
//! - A barrier writes the epoch's changes, flushes the memtable and creates a
//!   checkpoint (`CheckpointScope::All`, no lifetime). The record names the
//!   database's path and the checkpoint.
//! - A restore clones the record's checkpoints into a new database for the
//!   new generation: one source on the same layout, and the sources whose
//!   ranges overlap the lane's, each projected to that overlap, on another.
//!   Writes after the record are in no clone, and an older attempt writes
//!   only to its own database.
//! - Set entries that expired by the watermark are dropped by a compaction
//!   filter. Retention deletes the checkpoints of released records.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use object_store::ObjectStore;
use object_store::path::Path;
use slatedb::admin::{Admin, AdminBuilder};
use slatedb::config::{
    CheckpointOptions, CheckpointScope, CompactorOptions, GarbageCollectorDirectoryOptions,
    GarbageCollectorOptions, Settings,
};
use slatedb::db_cache::moka::{MokaCache, MokaCacheOptions};
use slatedb::db_cache::{DbCache, SplitCache};
use slatedb::{
    CloneSourceSpec, CompactionFilter, CompactionFilterDecision, CompactionFilterError,
    CompactionFilterSupplier, CompactionJobContext, CompactorBuilder, Db, GarbageCollectorBuilder,
    RowEntry, ValueDeletable, WriteBatch,
};
use uuid::Uuid;

use crate::key::{self, Kind};
use crate::{Change, Error, KeyGroupRange, Result, Snapshot, Store};

#[derive(Clone, Debug)]
pub struct SlateConfig {
    pub block_cache_bytes: u64,
    pub meta_cache_bytes: u64,
    pub concurrency: usize,
    /// How often its garbage collector runs, and how old a file must be.
    pub gc_interval: Duration,
    pub gc_min_age: Duration,
    /// How often the compactor looks for work.
    pub compactor_poll: Duration,
    /// L0 tables before a flush waits for the compactor.
    pub l0_max_ssts: usize,
}

impl Default for SlateConfig {
    fn default() -> Self {
        Self {
            block_cache_bytes: 64 << 20,
            meta_cache_bytes: 128 << 20,
            concurrency: 64,
            gc_interval: Duration::from_secs(600),
            gc_min_age: Duration::from_secs(300),
            compactor_poll: Duration::from_secs(5),
            l0_max_ssts: 8,
        }
    }
}

/// The stores SlateDB's parts use: the same objects, counted apart.
#[derive(Clone)]
pub struct Stores {
    pub db: Arc<dyn ObjectStore>,
    pub compactor: Arc<dyn ObjectStore>,
    pub gc: Arc<dyn ObjectStore>,
}

pub struct SlateStore {
    db: Db,
    admin: Admin,
    path: Path,
    range: KeyGroupRange,
    watermark: Arc<AtomicI64>,
    /// The checkpoint each kept record names, by epoch.
    checkpoints: BTreeMap<u64, Uuid>,
    concurrency: usize,
}

impl SlateStore {
    pub async fn open(
        stores: Stores,
        base: &Path,
        generation: u64,
        range: KeyGroupRange,
        restore: &[Snapshot],
        config: SlateConfig,
    ) -> Result<Self> {
        let path = base
            .clone()
            .join(format!("kg{}-{}", range.first, range.last))
            .join(format!("g{generation}"));
        let admin = AdminBuilder::new(path.clone(), stores.db.clone()).build();
        let sources: Vec<(KeyGroupRange, Path, Uuid)> = restore
            .iter()
            .filter(|s| s.range.overlaps(&range))
            .map(|s| decode(&s.data).map(|(p, id)| (s.range, p, id)))
            .collect::<Result<_>>()?;
        if !sources.is_empty() {
            let spec = |(r, p, id): &(KeyGroupRange, Path, Uuid)| {
                let spec = CloneSourceSpec::with_checkpoint(p.clone(), *id);
                if r.first >= range.first && r.last <= range.last {
                    return spec;
                }
                let first = r.first.max(range.first);
                let last = r.last.min(range.last);
                let (s, e) = key::group_range(first, last);
                let end = if e.is_empty() {
                    Bound::Unbounded
                } else {
                    Bound::Excluded(e)
                };
                spec.with_projection_range((Bound::Included(s), end))
            };
            let mut b = admin.create_clone_builder_from_source(spec(&sources[0]));
            for s in &sources[1..] {
                b = b.with_source(spec(s));
            }
            b.build().await?;
        }
        let watermark = Arc::new(AtomicI64::new(i64::MIN));
        let compactor = CompactorBuilder::new(path.clone(), stores.compactor.clone())
            .with_options(CompactorOptions {
                poll_interval: config.compactor_poll,
                ..Default::default()
            })
            .with_compaction_filter_supplier(Arc::new(Expiry {
                watermark: watermark.clone(),
            }));
        let dir = Some(GarbageCollectorDirectoryOptions {
            interval: Some(config.gc_interval),
            min_age: config.gc_min_age,
            dry_run: false,
        });
        let gc = GarbageCollectorBuilder::new(path.clone(), stores.gc.clone()).with_options(
            GarbageCollectorOptions {
                manifest_options: dir,
                wal_options: dir,
                compacted_options: dir,
                compactions_options: dir,
                ..Default::default()
            },
        );
        let settings = Settings {
            wal_enabled: false,
            l0_max_ssts: config.l0_max_ssts,
            compactor_options: None,
            garbage_collector_options: None,
            ..Default::default()
        };
        let cache: Arc<dyn DbCache> = Arc::new(
            SplitCache::new()
                .with_block_cache(Some(Arc::new(MokaCache::new_with_opts(MokaCacheOptions {
                    max_capacity: config.block_cache_bytes,
                    time_to_live: None,
                    time_to_idle: None,
                }))))
                .with_meta_cache(Some(Arc::new(MokaCache::new_with_opts(MokaCacheOptions {
                    max_capacity: config.meta_cache_bytes,
                    time_to_live: None,
                    time_to_idle: None,
                }))))
                .build(),
        );
        let db = Db::builder(path.clone(), stores.db.clone())
            .with_settings(settings)
            .with_db_cache(cache, u64::from(range.first))
            .with_compactor_builder(compactor)
            .with_gc_builder(gc)
            .build()
            .await?;
        Ok(Self {
            db,
            admin,
            path,
            range,
            watermark,
            checkpoints: BTreeMap::new(),
            concurrency: config.concurrency,
        })
    }

    async fn get(&self, k: &Bytes) -> Result<Option<Bytes>> {
        Ok(self.db.get(k).await?)
    }

    pub async fn close(&self) -> Result<()> {
        self.db.close().await?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn range(&self) -> KeyGroupRange {
        self.range
    }

    /// Tables in its current manifest: L0, sorted runs, and checkpoints.
    pub fn footprint(&self) -> (usize, usize, usize) {
        let m = self.db.manifest();
        (m.l0().len(), m.compacted().len(), m.checkpoints().len())
    }
}

impl Store for SlateStore {
    async fn get_many(&self, keys: Vec<Bytes>) -> Result<Vec<Option<Bytes>>> {
        let gets: Vec<_> = keys.iter().map(|k| self.get(k)).collect();
        crate::unordered(gets, self.concurrency).await
    }

    async fn scan(&self, start: Bytes, end: Bytes) -> Result<Vec<(Bytes, Bytes)>> {
        let mut it = if end.is_empty() {
            self.db.scan(start..).await?
        } else {
            self.db.scan(start..end).await?
        };
        let mut out = Vec::new();
        while let Some(kv) = it.next().await? {
            out.push((kv.key, kv.value));
        }
        Ok(out)
    }

    async fn commit(
        &mut self,
        epoch: u64,
        watermark: i64,
        changes: Vec<Change>,
    ) -> Result<Vec<Snapshot>> {
        self.watermark.fetch_max(watermark, Ordering::Relaxed);
        if !changes.is_empty() {
            let mut batch = WriteBatch::new();
            for (k, v) in changes {
                match v {
                    Some(v) => batch.put_bytes(k, v),
                    None => batch.delete(k),
                }
            }
            self.db.write(batch).await?;
        }
        let cp = self
            .db
            .begin_checkpoint(
                CheckpointScope::All,
                &CheckpointOptions {
                    lifetime: None,
                    source: None,
                    name: Some(format!("e{epoch}")),
                },
            )
            .await?
            .wait()
            .await?;
        self.checkpoints.insert(epoch, cp.id);
        Ok(vec![Snapshot {
            range: self.range,
            data: encode(&self.path, cp.id),
        }])
    }

    async fn release(&mut self, epoch: u64) -> Result<()> {
        let old: Vec<(u64, Uuid)> = self
            .checkpoints
            .range(..epoch)
            .map(|(e, id)| (*e, *id))
            .collect();
        for (e, id) in old {
            self.admin.delete_checkpoint(id).await?;
            self.checkpoints.remove(&e);
        }
        Ok(())
    }
}

fn encode(path: &Path, id: Uuid) -> Bytes {
    let p = path.as_ref().as_bytes();
    let mut b = BytesMut::with_capacity(2 + p.len() + 16);
    b.put_u16(u16::try_from(p.len()).unwrap_or(u16::MAX));
    b.put_slice(p);
    b.put_slice(id.as_bytes());
    b.freeze()
}

fn decode(data: &Bytes) -> Result<(Path, Uuid)> {
    let corrupt = || Error::Corrupt("a SlateDB snapshot runs past its end".into());
    let mut b = data.clone();
    if b.len() < 2 {
        return Err(corrupt());
    }
    let n = usize::from(b.get_u16());
    if b.len() != n + 16 {
        return Err(corrupt());
    }
    let p = String::from_utf8(b.split_to(n).to_vec()).map_err(|_| corrupt())?;
    let id = Uuid::from_slice(&b).map_err(|_| corrupt())?;
    Ok((Path::from(p), id))
}

/// Drops set entries that expired by the watermark of the newest epoch.
struct Expiry {
    watermark: Arc<AtomicI64>,
}

#[async_trait]
impl CompactionFilterSupplier for Expiry {
    async fn create_compaction_filter(
        &self,
        context: &CompactionJobContext,
    ) -> std::result::Result<Box<dyn CompactionFilter>, CompactionFilterError> {
        Ok(Box::new(ExpiryFilter {
            watermark: self.watermark.load(Ordering::Relaxed),
            last_run: context.is_dest_last_run,
        }))
    }
}

struct ExpiryFilter {
    watermark: i64,
    last_run: bool,
}

#[async_trait]
impl CompactionFilter for ExpiryFilter {
    async fn filter(
        &mut self,
        entry: &RowEntry,
    ) -> std::result::Result<CompactionFilterDecision, CompactionFilterError> {
        if key::kind_of(&entry.key) != Some(Kind::Set) {
            return Ok(CompactionFilterDecision::Keep);
        }
        match &entry.value {
            ValueDeletable::Value(v) if key::parse_expiry(v) <= self.watermark => {
                Ok(if self.last_run {
                    CompactionFilterDecision::Drop
                } else {
                    CompactionFilterDecision::Modify(ValueDeletable::Tombstone)
                })
            }
            _ => Ok(CompactionFilterDecision::Keep),
        }
    }

    async fn on_compaction_end(&mut self) -> std::result::Result<(), CompactionFilterError> {
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::KeyRef;
    use object_store::memory::InMemory;

    fn k(g: u16, s: &str) -> Bytes {
        key::value(
            0,
            KeyRef {
                group: g,
                key: s.as_bytes(),
            },
        )
    }

    fn put(g: u16, s: &str, v: &str) -> Change {
        (k(g, s), Some(Bytes::from(v.to_string())))
    }

    async fn get(s: &SlateStore, key: Bytes) -> Option<Bytes> {
        s.get_many(vec![key]).await.unwrap().remove(0)
    }

    #[tokio::test]
    async fn a_restore_reads_the_recorded_epoch_on_any_lanes() {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let stores = Stores {
            db: os.clone(),
            compactor: os.clone(),
            gc: os.clone(),
        };
        let base = Path::from("state");
        let config = SlateConfig::default();
        let mut lanes = Vec::new();
        for r in KeyGroupRange::split(256, 2) {
            lanes.push(
                SlateStore::open(stores.clone(), &base, 1, r, &[], config.clone())
                    .await
                    .unwrap(),
            );
        }
        let mut record = Vec::new();
        for (i, l) in lanes.iter_mut().enumerate() {
            let g = l.range().first;
            l.commit(1, 0, vec![put(g, "a", "1"), put(g + 100, "b", "1")])
                .await
                .unwrap();
            let snaps = l
                .commit(2, 0, vec![put(g, "a", &format!("2-{i}"))])
                .await
                .unwrap();
            record.extend(snaps);
            // An epoch whose record is never written.
            l.commit(3, 0, vec![put(g, "a", "3")]).await.unwrap();
        }
        let mut restored = Vec::new();
        for r in KeyGroupRange::split(256, 3) {
            restored.push(
                SlateStore::open(stores.clone(), &base, 2, r, &record, config.clone())
                    .await
                    .unwrap(),
            );
        }
        // The older generation goes on writing; no clone sees it.
        lanes[0].commit(4, 0, vec![put(0, "a", "4")]).await.unwrap();
        assert_eq!(get(&restored[0], k(0, "a")).await, Some(Bytes::from("2-0")));
        assert_eq!(get(&restored[0], k(100, "b")).await, None);
        // Groups 100 and 128 were each half's, and are now the second third's.
        assert_eq!(get(&restored[1], k(100, "b")).await, Some(Bytes::from("1")));
        assert_eq!(
            get(&restored[1], k(128, "a")).await,
            Some(Bytes::from("2-1"))
        );
        // Projected away: group 228 is the last third's alone.
        assert_eq!(get(&restored[1], k(228, "b")).await, None);
        assert_eq!(get(&restored[2], k(228, "b")).await, Some(Bytes::from("1")));
        for s in lanes.iter().chain(&restored) {
            s.close().await.unwrap();
        }
    }
}
