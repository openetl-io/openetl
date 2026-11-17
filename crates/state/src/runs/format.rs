//! A run's format, version [`FORMAT`]: the changes of one lane's epoch for
//! its range of key groups, in one object.
//!
//! ```text
//! header  magic | format u32 | 0 u32
//! blocks  each section's data blocks, a section after another
//! metas   each section's index and filters
//! footer  format u32 | sections u8 | (kind u8 | meta offset | meta length)*
//!         | footer length u32 | checksum u64 | magic
//! ```
//!
//! Fixed-width numbers are big-endian, but a filter's words, which are
//! little-endian; offsets, lengths and counts are LEB128 varints. Every
//! checksum is XXH3-64 of what it follows.
//!
//! A block is its entries, each `key length | key | value length + 1 |
//! value`, 0 for a delete, compressed with LZ4 when that's smaller, then
//! `codec u8 | checksum u64`: codec 0 is raw, 1 LZ4.
//!
//! A section's meta is `kind u8 | entries | raw bytes | first group | last
//! group | has times u8 (| least i64 | greatest i64) | blocks | (offset |
//! stored length | raw length | entries | first key length | first key |
//! last key length | last key)* | bits a key u8 | filters | (group |
//! probes u8 | words | words)* | checksum u64`. A section with 0 bits a key has no filters; otherwise it
//! has one for each key group it holds a key of, and none for any other.
//! Its kind's keys are those [`layout::section_of`] gives it, sorted, and
//! its filters hold what [`layout::filtered`] keeps of them.

use bytes::{BufMut, Bytes, BytesMut};

use super::RunConfig;
use super::codec::{Reader, checksum, put_bytes, put_varint, seal};
use super::filter::{self, Bloom};
use crate::layout::{self, SectionKind};
use crate::store::Change;
use crate::{Error, KeyGroupRange, Result};

pub const FORMAT: u32 = 1;

const MAGIC: &[u8; 8] = b"oetl-run";
const HEADER: usize = 16;
/// A footer's length, checksum and magic.
const TRAILER: usize = 4 + 8 + 8;
const RAW: u8 = 0;
const LZ4: u8 = 1;

/// A block, as its section's index has it.
#[derive(Clone, Debug)]
pub struct BlockMeta {
    pub offset: u64,
    pub stored: u32,
    pub raw: u32,
    pub entries: u32,
    pub first: Bytes,
    pub last: Bytes,
}

impl BlockMeta {
    pub fn end(&self) -> u64 {
        self.offset + u64::from(self.stored)
    }
}

/// What a section is, as its meta and the record give it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SectionInfo {
    pub kind: SectionKind,
    pub entries: u64,
    pub raw_bytes: u64,
    pub stored_bytes: u64,
    /// The groups it holds keys of.
    pub groups: KeyGroupRange,
    pub times: Option<(i64, i64)>,
    pub meta: (u64, u64),
}

/// A section's index and filters in memory, for the key groups a lane
/// reads it for.
#[derive(Debug)]
pub struct OpenSection {
    pub blocks: Vec<BlockMeta>,
    /// Bits a key: 0 for none.
    bits: u8,
    filters: Vec<(u16, Bloom)>,
}

impl OpenSection {
    /// Whether the section may hold a key of `group` whose
    /// [`layout::filtered`] part hashes to `h`.
    pub fn may_contain(&self, group: u16, h: u64) -> bool {
        if self.bits == 0 {
            return true;
        }
        match self.filters.binary_search_by_key(&group, |(g, _)| *g) {
            Ok(i) => self.filters[i].1.may_contain(h),
            Err(_) => false,
        }
    }

    /// The block that would hold `k`.
    pub fn block_for(&self, k: &[u8]) -> Option<usize> {
        let i = self.blocks.partition_point(|b| b.last.as_ref() < k);
        let b = self.blocks.get(i)?;
        (b.first.as_ref() <= k).then_some(i)
    }

    /// The blocks that may hold a key in `[start, end)`.
    pub fn blocks_between(&self, start: &[u8], end: &[u8]) -> std::ops::Range<usize> {
        let first = self.blocks.partition_point(|b| b.last.as_ref() < start);
        let past = self.blocks.partition_point(|b| b.first.as_ref() < end);
        first..past.max(first)
    }

    pub fn memory_bytes(&self) -> usize {
        let blocks: usize = self
            .blocks
            .iter()
            .map(|b| b.first.len() + b.last.len() + std::mem::size_of::<BlockMeta>())
            .sum();
        let filters: usize = self.filters.iter().map(|(_, f)| f.memory_bytes() + 8).sum();
        blocks + filters + std::mem::size_of::<Self>()
    }
}

/// A run, built: its bytes, and each section's handle, index, filters and
/// raw blocks.
pub struct Built {
    pub bytes: Bytes,
    pub sections: Vec<BuiltSection>,
}

pub struct BuiltSection {
    pub info: SectionInfo,
    pub open: OpenSection,
    /// Each block's offset and its entries, uncompressed.
    pub blocks: Vec<(u64, Bytes)>,
    /// The operators whose state it holds, in order.
    pub operators: Vec<u32>,
}

/// Builds the run of `changes`, sorted by key and each key once. Sections
/// without a change are left out.
pub fn build(changes: &[Change], config: &RunConfig) -> Result<Built> {
    let mut by_kind: [Vec<&Change>; 4] = Default::default();
    for c in changes {
        let kind = layout::section_of(&c.0)
            .ok_or_else(|| Error::Invalid("a change whose key isn't of the layout".into()))?;
        by_kind[kind.index()].push(c);
    }
    let mut w = RunWriter::new(config, true, changes.len() * 32);
    for kind in SectionKind::ALL {
        let part = &by_kind[kind.index()];
        if part.is_empty() {
            continue;
        }
        w.section(kind)?;
        for (k, v) in part.iter().map(|c| (&c.0, &c.1)) {
            w.add(k, v.as_deref())?;
        }
    }
    w.finish()
}

/// A run written a section at a time, each section's entries in key order
/// and each key once. A section without an entry is left out.
pub struct RunWriter<'c> {
    config: &'c RunConfig,
    /// Keeps each block's entries, uncompressed, for the cache.
    keep_raw: bool,
    out: BytesMut,
    done: Vec<SectionWriter>,
    open: Option<SectionWriter>,
}

impl<'c> RunWriter<'c> {
    pub fn new(config: &'c RunConfig, keep_raw: bool, capacity: usize) -> Self {
        let mut out = BytesMut::with_capacity(HEADER + capacity);
        out.put_slice(MAGIC);
        out.put_u32(FORMAT);
        out.put_u32(0);
        Self {
            config,
            keep_raw,
            out,
            done: Vec::new(),
            open: None,
        }
    }

    /// Starts the section of `kind`, after the last one's.
    pub fn section(&mut self, kind: SectionKind) -> Result<()> {
        self.close()?;
        let mut w = SectionWriter::new(kind, self.config);
        w.keep_raw = self.keep_raw;
        self.open = Some(w);
        Ok(())
    }

    /// Adds an entry to the open section; `None` is a delete.
    pub fn add(&mut self, k: &[u8], v: Option<&[u8]>) -> Result<()> {
        match &mut self.open {
            Some(w) => w.add(&mut self.out, k, v),
            None => Err(Error::Invalid("an entry before its section".into())),
        }
    }

    /// The bytes it holds so far, about.
    pub fn bytes(&self) -> usize {
        self.out.len() + self.open.as_ref().map_or(0, SectionWriter::held)
    }

    fn close(&mut self) -> Result<()> {
        if let Some(mut w) = self.open.take() {
            w.end_block(&mut self.out)?;
            if w.entries > 0 {
                self.done.push(w);
            }
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<Built> {
        self.close()?;
        let out = &mut self.out;
        let mut sections = Vec::with_capacity(self.done.len());
        for w in self.done {
            let at = out.len();
            let section = w.finish(out);
            let meta = (at as u64, (out.len() - at) as u64);
            sections.push(BuiltSection {
                info: SectionInfo {
                    meta,
                    ..section.info
                },
                ..section
            });
        }
        let footer = out.len();
        out.put_u32(FORMAT);
        out.put_u8(sections.len() as u8);
        for s in &sections {
            out.put_u8(s.info.kind.code());
            put_varint(out, s.info.meta.0);
            put_varint(out, s.info.meta.1);
        }
        let len = u32::try_from(out.len() - footer)
            .map_err(|_| Error::Invalid("a footer over 4 GiB".into()))?;
        let sum = checksum(&out[footer..]);
        out.put_u32(len);
        out.put_u64(sum);
        out.put_slice(MAGIC);
        Ok(Built {
            bytes: self.out.freeze(),
            sections,
        })
    }
}

struct SectionWriter {
    kind: SectionKind,
    block_bytes: usize,
    compress: bool,
    keep_raw: bool,
    bits: u8,
    block: BytesMut,
    block_entries: u32,
    first: Vec<u8>,
    last: Vec<u8>,
    blocks: Vec<BlockMeta>,
    raw_blocks: Vec<(u64, Bytes)>,
    /// Each group's hashes, for its filter.
    hashes: std::collections::BTreeMap<u16, Vec<u64>>,
    entries: u64,
    raw_bytes: u64,
    groups: Option<(u16, u16)>,
    times: Option<(i64, i64)>,
    operators: std::collections::BTreeSet<u32>,
}

impl SectionWriter {
    fn new(kind: SectionKind, config: &RunConfig) -> Self {
        Self {
            kind,
            block_bytes: config.block_bytes.max(1),
            compress: config.compress,
            keep_raw: true,
            bits: config.bits(kind),
            block: BytesMut::with_capacity(config.block_bytes + 256),
            block_entries: 0,
            first: Vec::new(),
            last: Vec::new(),
            blocks: Vec::new(),
            raw_blocks: Vec::new(),
            hashes: std::collections::BTreeMap::new(),
            entries: 0,
            raw_bytes: 0,
            groups: None,
            times: None,
            operators: std::collections::BTreeSet::new(),
        }
    }

    fn add(&mut self, out: &mut BytesMut, k: &[u8], v: Option<&[u8]>) -> Result<()> {
        let group =
            layout::group_of(k).ok_or_else(|| Error::Invalid("a key without a group".into()))?;
        let op = layout::operator_of(k)
            .ok_or_else(|| Error::Invalid("a key without an operator".into()))?;
        self.operators.insert(op.0);
        let time = match self.kind {
            SectionKind::Values => None,
            SectionKind::Rows => layout::parse_row(k).map(|r| r.1),
            SectionKind::Timers => layout::parse_timer(k).map(|t| t.0),
            SectionKind::Set => v.and_then(layout::parse_time_value),
        };
        if let Some(t) = time {
            self.times = Some(match self.times {
                Some((lo, hi)) => (lo.min(t), hi.max(t)),
                None => (t, t),
            });
        }
        self.groups = Some(match self.groups {
            Some((lo, hi)) => (lo.min(group), hi.max(group)),
            None => (group, group),
        });
        if self.bits > 0 {
            self.hashes
                .entry(group)
                .or_default()
                .push(filter::hash(layout::filtered(k)));
        }
        let before = self.block.len();
        put_bytes(&mut self.block, k);
        match v {
            Some(v) => {
                put_varint(&mut self.block, v.len() as u64 + 1);
                self.block.put_slice(v);
            }
            None => put_varint(&mut self.block, 0),
        }
        self.raw_bytes += (self.block.len() - before) as u64;
        self.entries += 1;
        if self.block_entries == 0 {
            self.first.clear();
            self.first.extend_from_slice(k);
        }
        self.block_entries += 1;
        self.last.clear();
        self.last.extend_from_slice(k);
        if self.block.len() >= self.block_bytes {
            self.end_block(out)?;
        }
        Ok(())
    }

    fn end_block(&mut self, out: &mut BytesMut) -> Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        let raw = Bytes::copy_from_slice(&self.block);
        self.block.clear();
        let offset = out.len();
        let packed = match self.compress {
            true => lz4_flex::block::compress(&raw),
            false => Vec::new(),
        };
        match self.compress && packed.len() < raw.len() {
            true => {
                out.put_slice(&packed);
                out.put_u8(LZ4);
            }
            false => {
                out.put_slice(&raw);
                out.put_u8(RAW);
            }
        }
        seal(out, offset);
        let too_big = |_| Error::Invalid("a block over 4 GiB".into());
        self.blocks.push(BlockMeta {
            offset: offset as u64,
            stored: u32::try_from(out.len() - offset).map_err(too_big)?,
            raw: u32::try_from(raw.len()).map_err(too_big)?,
            entries: self.block_entries,
            first: Bytes::copy_from_slice(&self.first),
            last: Bytes::copy_from_slice(&self.last),
        });
        if self.keep_raw {
            self.raw_blocks.push((offset as u64, raw));
        }
        self.block_entries = 0;
        Ok(())
    }

    /// What it holds besides the run's bytes, about: its open block, its
    /// index and its filters' hashes.
    fn held(&self) -> usize {
        let index: usize = self
            .blocks
            .iter()
            .map(|b| b.first.len() + b.last.len() + std::mem::size_of::<BlockMeta>())
            .sum();
        let raw: usize = self.raw_blocks.iter().map(|(_, b)| b.len()).sum();
        self.block.len() + index + raw + self.entries as usize * 8
    }

    /// Writes its meta to `out`.
    fn finish(self, out: &mut BytesMut) -> BuiltSection {
        let at = out.len();
        let (first, last) = self.groups.unwrap_or((0, 0));
        out.put_u8(self.kind.code());
        put_varint(out, self.entries);
        put_varint(out, self.raw_bytes);
        put_varint(out, first.into());
        put_varint(out, last.into());
        match self.times {
            Some((lo, hi)) => {
                out.put_u8(1);
                out.put_i64(lo);
                out.put_i64(hi);
            }
            None => out.put_u8(0),
        }
        put_varint(out, self.blocks.len() as u64);
        for b in &self.blocks {
            put_varint(out, b.offset);
            put_varint(out, b.stored.into());
            put_varint(out, b.raw.into());
            put_varint(out, b.entries.into());
            put_bytes(out, &b.first);
            put_bytes(out, &b.last);
        }
        let filters: Vec<(u16, Bloom)> = self
            .hashes
            .into_iter()
            .map(|(g, hs)| (g, Bloom::build(&hs, self.bits.into())))
            .collect();
        out.put_u8(self.bits);
        if self.bits > 0 {
            put_varint(out, filters.len() as u64);
            for (g, f) in &filters {
                put_varint(out, (*g).into());
                f.encode(out);
            }
        }
        seal(out, at);
        let stored_bytes = self.blocks.iter().map(|b| u64::from(b.stored)).sum();
        BuiltSection {
            info: SectionInfo {
                kind: self.kind,
                entries: self.entries,
                raw_bytes: self.raw_bytes,
                stored_bytes,
                groups: KeyGroupRange::new(first.into(), last.into()),
                times: self.times,
                meta: (0, 0),
            },
            open: OpenSection {
                blocks: self.blocks,
                bits: self.bits,
                filters,
            },
            blocks: self.raw_blocks,
            operators: self.operators.into_iter().collect(),
        }
    }
}

/// The sections a run's footer lists: their kinds and where their metas
/// are. `tail` is the run's last bytes, from anywhere before the footer.
pub fn footer(tail: &[u8]) -> Result<Vec<(SectionKind, u64, u64)>> {
    let corrupt = |what: &str| Error::Corrupt(format!("a run's footer: {what}"));
    let n = tail.len();
    if n < TRAILER || &tail[n - 8..] != MAGIC {
        return Err(corrupt("no magic at its end"));
    }
    let mut r = Reader::new(&tail[n - TRAILER..n - 8], "a run's footer");
    let len = usize::try_from(r.u32()?).map_err(|_| corrupt("too long"))?;
    let sum = r.u64()?;
    let start = (n - TRAILER)
        .checked_sub(len)
        .ok_or_else(|| corrupt("longer than what was read"))?;
    let body = &tail[start..n - TRAILER];
    if checksum(body) != sum {
        return Err(corrupt("its checksum doesn't match"));
    }
    let mut r = Reader::new(body, "a run's footer");
    let format = r.u32()?;
    if format != FORMAT {
        return Err(Error::Snapshot(format!(
            "a run of format {format}, where this engine reads {FORMAT}"
        )));
    }
    let mut out = Vec::new();
    for _ in 0..r.u8()? {
        let kind = SectionKind::from_code(r.u8()?).ok_or_else(|| corrupt("a kind"))?;
        out.push((kind, r.varint()?, r.varint()?));
    }
    Ok(out)
}

/// A section's meta, as [`SectionWriter::finish`] wrote it at `at` in its
/// run, keeping the blocks and filters of the key groups in `keep`. Its
/// blocks must end by `at`: every block of a run precedes its metas.
pub fn open_section(
    meta: &[u8],
    at: u64,
    keep: &[KeyGroupRange],
) -> Result<(SectionInfo, OpenSection)> {
    let mut r = Reader::sealed(meta, "a section's meta")?;
    let kind = SectionKind::from_code(r.u8()?)
        .ok_or_else(|| Error::Corrupt("a section's meta: its kind".into()))?;
    let entries = r.varint()?;
    let raw_bytes = r.varint()?;
    let groups = KeyGroupRange::new(r.group()?, r.group()?);
    let times = match r.u8()? {
        0 => None,
        _ => Some((r.i64()?, r.i64()?)),
    };
    let kept = |a: u32, b: u32| keep.iter().any(|k| k.overlaps(&KeyGroupRange::new(a, b)));
    let mut blocks = Vec::new();
    let mut stored_bytes = 0u64;
    let group = |k: &[u8]| {
        layout::group_of(k)
            .map(u32::from)
            .ok_or_else(|| Error::Corrupt("a block's key has no group".into()))
    };
    for _ in 0..r.count()? {
        let offset = r.varint()?;
        let stored = narrow(r.varint()?)?;
        let raw = narrow(r.varint()?)?;
        let n = narrow(r.varint()?)?;
        let first = r.bytes()?;
        let last = r.bytes()?;
        if offset
            .checked_add(u64::from(stored))
            .is_none_or(|end| end > at)
        {
            return Err(Error::Corrupt(format!(
                "a block of {stored} bytes at {offset}, past its section's meta at {at}"
            )));
        }
        stored_bytes += u64::from(stored);
        let (from, to) = (group(first)?, group(last)?);
        // Timers sort by time first, so any block may hold any group's.
        if kind == SectionKind::Timers || kept(from.min(to), to.max(from)) {
            blocks.push(BlockMeta {
                offset,
                stored,
                raw,
                entries: n,
                first: Bytes::copy_from_slice(first),
                last: Bytes::copy_from_slice(last),
            });
        }
    }
    let bits = r.u8()?;
    let mut filters = Vec::new();
    if bits > 0 {
        for _ in 0..r.count()? {
            let g = r.group()?;
            match kept(g, g) {
                true => {
                    let g = u16::try_from(g).unwrap_or(u16::MAX);
                    filters.push((g, Bloom::decode(&mut r)?));
                }
                false => Bloom::skip(&mut r)?,
            }
        }
    }
    if !r.is_empty() {
        return Err(Error::Corrupt(
            "a section's meta: bytes past its end".into(),
        ));
    }
    let info = SectionInfo {
        kind,
        entries,
        raw_bytes,
        stored_bytes,
        groups,
        times,
        meta: (0, meta.len() as u64),
    };
    let open = OpenSection {
        blocks,
        bits,
        filters,
    };
    Ok((info, open))
}

fn narrow(v: u64) -> Result<u32> {
    u32::try_from(v).map_err(|_| Error::Corrupt(format!("a block's size, {v}")))
}

/// A block's entries, from what the run stores of it.
pub fn decode_block(stored: &[u8], raw_len: u32) -> Result<Bytes> {
    let corrupt = |what: &str| Error::Corrupt(format!("a block: {what}"));
    let body = stored
        .len()
        .checked_sub(8)
        .ok_or_else(|| corrupt("shorter than its trailer"))?;
    let mut r = Reader::sealed(stored, "a block")?;
    let payload = r.take(body.checked_sub(1).ok_or_else(|| corrupt("no codec"))?)?;
    let codec = r.u8()?;
    let raw_len = usize::try_from(raw_len).map_err(|_| corrupt("too long"))?;
    let raw = match codec {
        RAW => Bytes::copy_from_slice(payload),
        LZ4 => Bytes::from(
            lz4_flex::block::decompress(payload, raw_len)
                .map_err(|e| corrupt(&format!("LZ4: {e}")))?,
        ),
        c => return Err(corrupt(&format!("codec {c}"))),
    };
    if raw.len() != raw_len {
        return Err(corrupt(&format!("{} bytes, not {raw_len}", raw.len())));
    }
    Ok(raw)
}

/// The entry of a block at `*at`, which it moves past it.
pub fn entry(block: &Bytes, at: &mut usize) -> Result<(Bytes, Option<Bytes>)> {
    let rest = block.get(*at..).unwrap_or_default();
    let mut r = Reader::new(rest, "a block's entry");
    let k = r.bytes()?;
    let v = match r.varint()? {
        0 => None,
        n => {
            let n = usize::try_from(n - 1).map_err(|_| r.corrupt())?;
            Some(r.take(n)?)
        }
    };
    let entry = (block.slice_ref(k), v.map(|v| block.slice_ref(v)));
    *at = block.len() - r.remaining();
    Ok(entry)
}

/// The value of `k` in a block, `Some(None)` for its delete, or `None`.
pub fn find(block: &Bytes, k: &[u8]) -> Result<Option<Option<Bytes>>> {
    let mut at = 0;
    while at < block.len() {
        let (key, v) = entry(block, &mut at)?;
        match key.as_ref().cmp(k) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => return Ok(Some(v)),
            std::cmp::Ordering::Greater => break,
        }
    }
    Ok(None)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::OperatorId;

    const OP: OperatorId = OperatorId(2);
    const ALL: KeyGroupRange = KeyGroupRange {
        first: 0,
        last: 65_535,
    };

    fn config(block_bytes: usize) -> RunConfig {
        RunConfig {
            block_bytes,
            ..Default::default()
        }
    }

    fn changes() -> Vec<Change> {
        let mut c: Vec<Change> = Vec::new();
        for g in 0..40u16 {
            let t = i64::from(g);
            let v = Bytes::from(format!("value of group {g}, repeated, repeated"));
            c.push((layout::value(g, OP, b"k"), Some(v)));
            c.push((layout::value(g, OP, b"gone"), None));
            let row = Bytes::from(format!("row {g}"));
            c.push((layout::row(g, OP, t - 20, b"k", 1), Some(row)));
            let expiry = layout::time_value(100 + t);
            c.push((layout::set(g, OP, b"id"), Some(expiry)));
            c.push((layout::timer(OP, t * 3, g, b"k"), Some(Bytes::new())));
        }
        c.sort();
        c
    }

    fn stored<'a>(run: &'a [u8], b: &BlockMeta) -> &'a [u8] {
        &run[b.offset as usize..b.end() as usize]
    }

    fn meta(run: &[u8], (off, len): (u64, u64)) -> &[u8] {
        &run[off as usize..(off + len) as usize]
    }

    /// Every entry of a section, in order, reading its blocks from `run`.
    fn entries(run: &Bytes, open: &OpenSection) -> Vec<(Bytes, Option<Bytes>)> {
        let mut out = Vec::new();
        for b in &open.blocks {
            let block = decode_block(stored(run, b), b.raw).unwrap();
            let mut at = 0;
            let mut n = 0;
            while at < block.len() {
                out.push(entry(&block, &mut at).unwrap());
                n += 1;
            }
            assert_eq!(n, b.entries);
            assert_eq!(out[out.len() - n as usize].0, b.first);
            assert_eq!(out.last().unwrap().0, b.last);
        }
        out
    }

    #[test]
    fn a_run_reads_back_by_section() {
        let changes = changes();
        for block_bytes in [1, 100, 4096] {
            let built = build(&changes, &config(block_bytes)).unwrap();
            let run = built.bytes.clone();
            let listed = footer(&run[run.len() - 64..]).unwrap();
            assert_eq!(listed.len(), 4);
            let mut all = Vec::new();
            for (s, (kind, off, len)) in built.sections.iter().zip(&listed) {
                assert_eq!((s.info.kind, s.info.meta), (*kind, (*off, *len)));
                let (info, open) = open_section(meta(&run, (*off, *len)), *off, &[ALL]).unwrap();
                let want = SectionInfo {
                    meta: (0, *len),
                    ..s.info.clone()
                };
                assert_eq!(info, want);
                assert_eq!(open.blocks.len(), s.open.blocks.len());
                let got = entries(&run, &open);
                assert!(
                    got.iter()
                        .all(|(k, _)| layout::section_of(k) == Some(*kind))
                );
                all.extend(got);
            }
            all.sort();
            assert_eq!(all, changes);
        }
    }

    #[test]
    fn sections_know_their_groups_and_times() {
        let built = build(&changes(), &config(4096)).unwrap();
        let info: Vec<_> = built
            .sections
            .iter()
            .map(|s| (s.info.kind, s.info.groups, s.info.times, s.info.entries))
            .collect();
        let groups = KeyGroupRange::new(0, 39);
        assert_eq!(
            info,
            [
                (SectionKind::Values, groups, None, 80),
                (SectionKind::Rows, groups, Some((-20, 19)), 40),
                (SectionKind::Set, groups, Some((100, 139)), 40),
                (SectionKind::Timers, groups, Some((0, 117)), 40),
            ]
        );
    }

    /// A section opened for some key groups keeps their filters and the
    /// blocks that may hold their keys, and finds every key of them.
    #[test]
    fn a_section_opened_for_some_groups_keeps_theirs() {
        let changes = changes();
        let built = build(&changes, &config(60)).unwrap();
        let run = built.bytes;
        let keep = [KeyGroupRange::new(10, 14), KeyGroupRange::new(30, 30)];
        for s in &built.sections {
            let (_, open) = open_section(meta(&run, s.info.meta), s.info.meta.0, &keep).unwrap();
            assert!(open.memory_bytes() < s.open.memory_bytes());
            for (k, v) in &changes {
                if layout::section_of(k) != Some(s.info.kind) {
                    continue;
                }
                let g = layout::group_of(k).unwrap();
                let kept = keep.iter().any(|r| r.contains(g.into()));
                let h = filter::hash(layout::filtered(k));
                assert_eq!(open.may_contain(g, h), kept, "{g}");
                if kept {
                    let b = &open.blocks[open.block_for(k).unwrap()];
                    let block = decode_block(stored(&run, b), b.raw).unwrap();
                    assert_eq!(find(&block, k).unwrap(), Some(v.clone()));
                }
            }
        }
    }

    /// A meta whose checksum holds but whose blocks reach past it, or past
    /// the end of numbers, is refused.
    #[test]
    fn a_section_refuses_blocks_past_its_meta() {
        let built = build(&changes(), &config(4096)).unwrap();
        let s = &built.sections[0];
        let at = s.info.meta.0;
        let good = meta(&built.bytes, s.info.meta);
        assert!(open_section(good, at, &[ALL]).is_ok());
        assert!(matches!(
            open_section(good, s.open.blocks[0].end() - 1, &[ALL]),
            Err(Error::Corrupt(_))
        ));
        // The same section, its first block moved to the last offset.
        let mut lying = BytesMut::new();
        lying.put_u8(s.info.kind.code());
        for v in [s.info.entries, s.info.raw_bytes, 0, 39] {
            put_varint(&mut lying, v);
        }
        lying.put_u8(0);
        put_varint(&mut lying, 1);
        for v in [u64::MAX, 9, 9, 1] {
            put_varint(&mut lying, v);
        }
        let k = layout::value(0, OP, b"k");
        put_bytes(&mut lying, &k);
        put_bytes(&mut lying, &k);
        lying.put_u8(0);
        seal(&mut lying, 0);
        let got = open_section(&lying, at, &[ALL]);
        assert!(matches!(got, Err(Error::Corrupt(_))), "{got:?}");
    }

    #[test]
    fn a_block_refuses_damage() {
        let built = build(&changes(), &config(4096)).unwrap();
        let b = &built.sections[0].open.blocks[0];
        let good = stored(&built.bytes, b).to_vec();
        assert!(decode_block(&good, b.raw).is_ok());
        for at in [0, good.len() / 2, good.len() - 9, good.len() - 1] {
            let mut bad = good.clone();
            bad[at] ^= 1;
            let got = decode_block(&bad, b.raw);
            assert!(matches!(got, Err(Error::Corrupt(_))), "{at}");
        }
        assert!(decode_block(&good, b.raw + 1).is_err());
        assert!(decode_block(&good[..5], b.raw).is_err());
    }

    /// The runs recorded under tests/testdata hold every kind, in blocks of
    /// both codecs, so the test that restores them reads all of it.
    #[test]
    fn the_recorded_runs_hold_every_kind_and_codec() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/testdata/runs-v1");
        let mut kinds = std::collections::BTreeSet::new();
        let mut codecs = std::collections::BTreeSet::new();
        for name in ["g1/e1/kg0-7.run", "g1/e1/kg8-15.run"] {
            let run = Bytes::from(std::fs::read(dir.join(name)).unwrap());
            for (kind, off, len) in footer(&run).unwrap() {
                kinds.insert(kind);
                let (_, open) = open_section(meta(&run, (off, len)), off, &[ALL]).unwrap();
                for b in &open.blocks {
                    codecs.insert(run[b.end() as usize - 9]);
                }
                entries(&run, &open);
            }
        }
        assert_eq!(kinds.len(), 4);
        assert_eq!(codecs.into_iter().collect::<Vec<_>>(), [RAW, LZ4]);
    }
}
