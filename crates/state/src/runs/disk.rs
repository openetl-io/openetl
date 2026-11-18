//! The disk cache, state's third tier: what lanes read from object
//! storage, and the runs they write, kept in files on the pod's local disk,
//! so that a block is fetched once in the cache's life.
//!
//! Nothing on it is ever the truth. It starts empty, in a directory it
//! empties first, and only the process that wrote an entry knows where it
//! is. An entry that reads short, or whose checksum isn't the one written,
//! is a miss, and a file that can't be written is skipped: a lane then
//! reads object storage, as with no cache. Entries are appended to segment
//! files; once the cache passes its bound, its oldest segment goes whole,
//! and an entry read from the oldest is written again to the newest, so
//! what's read stays longest.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use twox_hash::{XxHash3_64, XxHash3_128};

/// The most one segment file holds before the next starts, unless one
/// entry is larger.
const MAX_SEGMENT: u64 = 64 << 20;

/// An entry's key: the hash of its namespace, object, offset and length,
/// 128 bits so that no two share one.
type Key = u128;

/// Where an entry is.
#[derive(Clone, Copy)]
struct Entry {
    segment: u64,
    offset: u64,
    len: u32,
    sum: u64,
}

struct Segment {
    file: Arc<File>,
    path: PathBuf,
    len: u64,
    keys: Vec<Key>,
    /// It failed a write, so it takes no more.
    closed: bool,
}

#[derive(Default)]
struct Inner {
    index: HashMap<Key, Entry>,
    /// By number, oldest first; the last is written to.
    segments: BTreeMap<u64, Segment>,
    next: u64,
    bytes: u64,
}

/// What a [`DiskCache`] did since it opened.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DiskCounts {
    /// Reads it answered, and their bytes.
    pub hits: u64,
    pub hit_bytes: u64,
    /// Reads it couldn't: not held, evicted, or corrupt.
    pub misses: u64,
    /// Entries that read short or whose checksum failed.
    pub corrupt: u64,
    /// Entries written, and their bytes.
    pub written: u64,
    pub written_bytes: u64,
    /// Segments dropped to keep under the bound.
    pub evicted: u64,
}

#[derive(Default)]
struct Counters {
    hits: AtomicU64,
    hit_bytes: AtomicU64,
    misses: AtomicU64,
    corrupt: AtomicU64,
    written: AtomicU64,
    written_bytes: AtomicU64,
    evicted: AtomicU64,
}

fn add(a: &AtomicU64, n: u64) {
    a.fetch_add(n, Ordering::Relaxed);
}

/// A cache of byte ranges of immutable objects, in files of a directory of
/// its own, within a bound of bytes. Its lanes share it, each store under
/// a namespace of its own. The directory goes when it's dropped.
pub struct DiskCache {
    dir: PathBuf,
    limit: u64,
    segment_bytes: u64,
    inner: Mutex<Inner>,
    counters: Counters,
    /// Writes an entry at an offset of a segment: [`write_at`], but in a
    /// test.
    writer: fn(&File, &[u8], u64) -> io::Result<()>,
}

impl std::fmt::Debug for DiskCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskCache")
            .field("dir", &self.dir)
            .field("limit", &self.limit)
            .finish()
    }
}

impl DiskCache {
    /// A cache in `dir`, which it makes, or empties of what an earlier
    /// process left, holding up to about `limit` bytes.
    pub fn open(dir: &Path, limit: u64) -> io::Result<Arc<DiskCache>> {
        match fs::remove_dir_all(dir) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
            _ => {}
        }
        fs::create_dir_all(dir)?;
        Ok(Arc::new(DiskCache {
            dir: dir.to_owned(),
            limit,
            segment_bytes: (limit / 8).clamp(1, MAX_SEGMENT),
            inner: Mutex::new(Inner::default()),
            counters: Counters::default(),
            writer: write_at,
        }))
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The bytes its segment files hold.
    pub fn bytes(&self) -> u64 {
        self.lock().bytes
    }

    pub fn counts(&self) -> DiskCounts {
        let l = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let c = &self.counters;
        DiskCounts {
            hits: l(&c.hits),
            hit_bytes: l(&c.hit_bytes),
            misses: l(&c.misses),
            corrupt: l(&c.corrupt),
            written: l(&c.written),
            written_bytes: l(&c.written_bytes),
            evicted: l(&c.evicted),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn key(space: &str, object: &str, offset: u64, len: u32) -> Key {
        let mut b = Vec::with_capacity(space.len() + object.len() + 14);
        b.extend_from_slice(space.as_bytes());
        b.push(0);
        b.extend_from_slice(object.as_bytes());
        b.push(0);
        b.extend_from_slice(&offset.to_le_bytes());
        b.extend_from_slice(&len.to_le_bytes());
        XxHash3_128::oneshot(&b)
    }

    /// Bytes `offset` to `offset + len` of `object` in `space`, if it holds
    /// them as they were written.
    pub fn get(&self, space: &str, object: &str, offset: u64, len: u32) -> Option<Bytes> {
        let key = Self::key(space, object, offset, len);
        let (entry, file, oldest) = {
            let inner = self.lock();
            let found = inner.index.get(&key).copied().and_then(|e| {
                let seg = inner.segments.get(&e.segment)?;
                let oldest = inner.segments.len() > 1
                    && inner
                        .segments
                        .first_key_value()
                        .is_some_and(|(n, _)| *n == e.segment);
                Some((e, seg.file.clone(), oldest))
            });
            match found {
                Some(f) => f,
                None => {
                    add(&self.counters.misses, 1);
                    return None;
                }
            }
        };
        let mut buf = vec![0u8; entry.len as usize];
        let read = read_at(&file, &mut buf, entry.offset);
        if read.is_err() || XxHash3_64::oneshot(&buf) != entry.sum {
            add(&self.counters.corrupt, 1);
            add(&self.counters.misses, 1);
            self.lock().index.remove(&key);
            return None;
        }
        add(&self.counters.hits, 1);
        add(&self.counters.hit_bytes, u64::from(entry.len));
        if oldest {
            self.lock().index.remove(&key);
            self.write(key, &buf);
        }
        Some(Bytes::from(buf))
    }

    /// Keeps `data`, bytes `offset` on of `object` in `space`, unless it
    /// holds them or they're larger than its bound.
    pub fn put(&self, space: &str, object: &str, offset: u64, data: &[u8]) {
        let Ok(len) = u32::try_from(data.len()) else {
            return;
        };
        let key = Self::key(space, object, offset, len);
        if self.lock().index.contains_key(&key) {
            return;
        }
        self.write(key, data);
    }

    fn write(&self, key: Key, data: &[u8]) {
        let len = data.len() as u64;
        if len == 0 || len > self.limit {
            return;
        }
        let Ok(len32) = u32::try_from(data.len()) else {
            return;
        };
        let mut inner = self.lock();
        if inner.index.contains_key(&key) {
            return;
        }
        let full = inner
            .segments
            .last_key_value()
            .is_none_or(|(_, s)| s.closed || s.len > 0 && s.len + len > self.segment_bytes);
        if full && self.start_segment(&mut inner).is_err() {
            return;
        }
        let Some((&n, seg)) = inner.segments.last_key_value() else {
            return;
        };
        let (file, offset) = (seg.file.clone(), seg.len);
        if (self.writer)(&file, data, offset).is_err() {
            // A segment that can't be written takes nothing more. What the
            // write left of the entry goes, or else counts, so the bound
            // still holds.
            if let Some(seg) = inner.segments.get_mut(&n) {
                seg.closed = true;
                if file.set_len(offset).is_err() {
                    seg.len += len;
                    inner.bytes += len;
                }
            }
            self.evict(&mut inner);
            return;
        }
        let entry = Entry {
            segment: n,
            offset,
            len: len32,
            sum: XxHash3_64::oneshot(data),
        };
        inner.index.insert(key, entry);
        if let Some(seg) = inner.segments.get_mut(&n) {
            seg.len += len;
            seg.keys.push(key);
        }
        inner.bytes += len;
        add(&self.counters.written, 1);
        add(&self.counters.written_bytes, len);
        self.evict(&mut inner);
    }

    /// Drops the oldest segments while it holds more than its bound.
    fn evict(&self, inner: &mut Inner) {
        while inner.bytes > self.limit && inner.segments.len() > 1 {
            let Some((n, seg)) = inner.segments.pop_first() else {
                break;
            };
            for k in &seg.keys {
                if inner.index.get(k).is_some_and(|e| e.segment == n) {
                    inner.index.remove(k);
                }
            }
            inner.bytes = inner.bytes.saturating_sub(seg.len);
            let _ = fs::remove_file(&seg.path);
            add(&self.counters.evicted, 1);
        }
    }

    fn start_segment(&self, inner: &mut Inner) -> io::Result<()> {
        let n = inner.next;
        inner.next += 1;
        let path = self.dir.join(format!("{n:08}.seg"));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        // A segment that took nothing goes: none of its keys is indexed.
        if let Some((&last, s)) = inner.segments.last_key_value()
            && s.keys.is_empty()
        {
            let (path, len) = (s.path.clone(), s.len);
            inner.segments.remove(&last);
            inner.bytes = inner.bytes.saturating_sub(len);
            let _ = fs::remove_file(path);
        }
        inner.segments.insert(
            n,
            Segment {
                file: Arc::new(file),
                path,
                len: 0,
                keys: Vec::new(),
                closed: false,
            },
        );
        Ok(())
    }
}

impl Drop for DiskCache {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

#[cfg(unix)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, buf, offset)
}

#[cfg(unix)]
fn write_at(file: &File, data: &[u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, data, offset)
}

#[cfg(not(unix))]
fn read_at(_: &File, _: &mut [u8], _: u64) -> io::Result<()> {
    Err(io::Error::from(io::ErrorKind::Unsupported))
}

#[cfg(not(unix))]
fn write_at(_: &File, _: &[u8], _: u64) -> io::Result<()> {
    Err(io::Error::from(io::ErrorKind::Unsupported))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn block(i: u32, n: usize) -> Vec<u8> {
        (0..n).map(|j| (i as usize * 31 + j) as u8).collect()
    }

    #[test]
    fn it_gives_back_what_it_kept_and_empties_its_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("cache");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("left-by-a-dead-one"), b"x").unwrap();
        let c = DiskCache::open(&dir, 1 << 20).unwrap();
        assert!(!dir.join("left-by-a-dead-one").exists());
        c.put("op", "g1/e1/kg0-3.run", 0, &block(1, 100));
        c.put("op", "g1/e1/kg0-3.run", 100, &block(2, 50));
        assert_eq!(
            c.get("op", "g1/e1/kg0-3.run", 0, 100).as_deref(),
            Some(&block(1, 100)[..])
        );
        assert_eq!(
            c.get("op", "g1/e1/kg0-3.run", 100, 50).as_deref(),
            Some(&block(2, 50)[..])
        );
        // Another length, object or space is another entry.
        assert!(c.get("op", "g1/e1/kg0-3.run", 0, 99).is_none());
        assert!(c.get("op", "g1/e2/kg0-3.run", 0, 100).is_none());
        assert!(c.get("other", "g1/e1/kg0-3.run", 0, 100).is_none());
        let n = c.counts();
        assert_eq!((n.hits, n.misses, n.written), (2, 3, 2));
        drop(c);
        assert!(!dir.exists());
    }

    #[test]
    fn a_corrupt_or_short_entry_is_a_miss() {
        let tmp = tempfile::tempdir().unwrap();
        let c = DiskCache::open(tmp.path(), 1 << 20).unwrap();
        c.put("op", "r", 0, &block(1, 100));
        c.put("op", "r", 100, &block(2, 100));
        let seg = tmp.path().join(format!("{:08}.seg", 0));
        let mut bytes = fs::read(&seg).unwrap();
        bytes[10] ^= 0xff;
        fs::write(&seg, &bytes[..150]).unwrap();
        assert!(c.get("op", "r", 0, 100).is_none());
        assert!(c.get("op", "r", 100, 100).is_none());
        assert_eq!(c.counts().corrupt, 2);
        // Once a miss, it's written again.
        c.put("op", "r", 0, &block(1, 100));
        assert_eq!(
            c.get("op", "r", 0, 100).as_deref(),
            Some(&block(1, 100)[..])
        );
    }

    /// Writes half of an entry, then fails.
    fn half(file: &File, data: &[u8], offset: u64) -> io::Result<()> {
        write_at(file, &data[..data.len() / 2], offset)?;
        Err(io::Error::other("disk full"))
    }

    #[test]
    fn a_failed_write_leaves_nothing_uncounted() {
        let tmp = tempfile::tempdir().unwrap();
        let mut c = DiskCache::open(tmp.path(), 8 * 1000).unwrap();
        let on_disk = |c: &DiskCache| -> u64 {
            fs::read_dir(c.dir())
                .unwrap()
                .map(|e| e.unwrap().metadata().unwrap().len())
                .sum()
        };
        for i in 0..40u32 {
            Arc::get_mut(&mut c).unwrap().writer = if i % 2 == 0 { write_at } else { half };
            c.put("op", "r", u64::from(i) * 100, &block(i, 100));
            assert!(
                on_disk(&c) <= c.bytes(),
                "{} on disk, {} counted",
                on_disk(&c),
                c.bytes()
            );
            assert!(c.bytes() <= 8 * 1000 + c.segment_bytes);
        }
        // The entries written whole read back, the others are misses.
        let (mut hits, mut misses) = (0, 0);
        for i in 30..40u32 {
            match c.get("op", "r", u64::from(i) * 100, 100) {
                Some(b) => {
                    assert_eq!(&b[..], &block(i, 100)[..]);
                    assert_eq!(i % 2, 0);
                    hits += 1;
                }
                None => misses += 1,
            }
        }
        assert!(hits > 0 && misses >= 5, "{hits} hits, {misses} misses");
    }

    #[test]
    fn it_keeps_within_its_bound_and_keeps_what_is_read() {
        let tmp = tempfile::tempdir().unwrap();
        let limit = 8 * 1000;
        let c = DiskCache::open(tmp.path(), limit).unwrap();
        for i in 0..100u32 {
            c.put("op", "r", u64::from(i) * 100, &block(i, 100));
            // Block 0 is read all along, so it stays.
            assert!(c.get("op", "r", 0, 100).is_some(), "block 0 after {i}");
            assert!(c.bytes() <= limit + c.segment_bytes, "{}", c.bytes());
        }
        assert!(c.counts().evicted > 0);
        assert!(c.get("op", "r", 100, 100).is_none());
        assert_eq!(
            c.get("op", "r", 9900, 100).as_deref(),
            Some(&block(99, 100)[..])
        );
        let files = fs::read_dir(tmp.path()).unwrap().count() as u64;
        assert!(files <= limit / c.segment_bytes + 2, "{files} files");
        // Larger than the bound: never kept.
        c.put("op", "big", 0, &vec![1u8; limit as usize + 1]);
        assert!(c.get("op", "big", 0, limit as u32 + 1).is_none());
    }
}
