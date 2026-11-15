//! Blocks read or written, uncompressed, by run and offset, the least
//! recently used dropped first to keep under a bound of bytes.

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;

/// A block: its run's ID and its offset in the run.
pub type BlockId = (u64, u64);

/// What an entry costs beyond its bytes, about.
const OVERHEAD: usize = 96;

pub struct BlockCache {
    limit: usize,
    bytes: usize,
    tick: u64,
    blocks: HashMap<BlockId, (Bytes, u64)>,
    by_use: BTreeMap<u64, BlockId>,
}

impl BlockCache {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            bytes: 0,
            tick: 0,
            blocks: HashMap::new(),
            by_use: BTreeMap::new(),
        }
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn get(&mut self, id: BlockId) -> Option<Bytes> {
        self.tick += 1;
        let (b, used) = self.blocks.get_mut(&id)?;
        self.by_use.remove(used);
        *used = self.tick;
        self.by_use.insert(self.tick, id);
        Some(b.clone())
    }

    pub fn put(&mut self, id: BlockId, b: Bytes) {
        let size = b.len() + OVERHEAD;
        if size > self.limit {
            return;
        }
        self.tick += 1;
        if let Some((old, used)) = self.blocks.insert(id, (b, self.tick)) {
            self.by_use.remove(&used);
            self.bytes -= old.len() + OVERHEAD;
        }
        self.by_use.insert(self.tick, id);
        self.bytes += size;
        while self.bytes > self.limit {
            let Some((_, oldest)) = self.by_use.pop_first() else {
                break;
            };
            if let Some((old, _)) = self.blocks.remove(&oldest) {
                self.bytes -= old.len() + OVERHEAD;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_keeps_under_its_bound_and_drops_the_least_used() {
        let block = |n: usize| Bytes::from(vec![0u8; n]);
        let mut c = BlockCache::new(3 * (100 + OVERHEAD));
        c.put((1, 0), block(100));
        c.put((1, 100), block(100));
        c.put((2, 0), block(100));
        assert!(c.get((1, 0)).is_some());
        c.put((3, 0), block(100));
        assert!(c.get((1, 100)).is_none());
        assert!(c.get((1, 0)).is_some() && c.get((2, 0)).is_some());
        assert!(c.bytes() <= 3 * (100 + OVERHEAD));
        c.put((4, 0), block(10_000));
        assert!(c.get((4, 0)).is_none());
        c.put((2, 0), block(50));
        assert_eq!(c.bytes(), 50 + 100 + 100 + 3 * OVERHEAD);
    }
}
