//! What the checkpoint record keeps of runs: a lane's snapshot names each
//! section it reads, by run, so a section can be replaced or dropped
//! without touching its run's others.
//!
//! A snapshot's data is `tag u8 | version u8 | runs | (name | format |
//! size | sections | (kind u8 | epoch | first group | last group | entries
//! | raw bytes | stored bytes | has times u8 (| least i64 | greatest i64) |
//! meta offset | meta length)*)*`, in the run format's encoding.

use std::collections::BTreeSet;
use std::fmt;

use bytes::{BufMut, Bytes, BytesMut};

use super::codec::{Reader, put_bytes, put_varint};
use crate::layout::SectionKind;
use crate::store::Snapshot;
use crate::{Error, KeyGroupRange, Result};

const TAG: u8 = b'R';
const VERSION: u8 = 1;

/// A section of a run, as a snapshot names it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SectionRef {
    pub kind: SectionKind,
    /// The epoch of its newest change: a key group's sections are read
    /// newest first.
    pub epoch: u64,
    /// The key groups it counts for. Its keys of other groups are another
    /// snapshot's to name, or no one's.
    pub groups: KeyGroupRange,
    /// Its entries, deletes among them, in all of its key groups.
    pub entries: u64,
    /// Its entries' bytes, uncompressed.
    pub raw_bytes: u64,
    /// Its blocks' bytes in the run.
    pub stored_bytes: u64,
    /// The least and greatest time it holds, in all of its key groups:
    /// rows' times, timers', or the set's expiries. `None` for values.
    pub times: Option<(i64, i64)>,
    /// Where its index and filters are in the run: an offset and a length.
    pub meta: (u64, u64),
}

/// A run, and those of its sections a snapshot names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunRef {
    /// The run's object, under the store's prefix.
    pub name: String,
    /// Its format's version.
    pub format: u32,
    /// Its bytes.
    pub size: u64,
    pub sections: Vec<SectionRef>,
}

/// The name of the run a lane writes at a barrier:
/// `g{generation}/e{epoch}/kg{first}-{last}.run`. No two attempts or epochs
/// write the same name, and none is written twice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunName {
    pub generation: u64,
    pub epoch: u64,
    pub range: KeyGroupRange,
}

impl fmt::Display for RunName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "g{}/e{}/kg{}-{}.run",
            self.generation, self.epoch, self.range.first, self.range.last
        )
    }
}

impl RunName {
    pub fn parse(name: &str) -> Option<RunName> {
        let mut parts = name.split('/');
        let generation = parts.next()?.strip_prefix('g')?.parse().ok()?;
        let epoch = parts.next()?.strip_prefix('e')?.parse().ok()?;
        let groups = parts.next()?.strip_prefix("kg")?.strip_suffix(".run")?;
        let (first, last) = groups.split_once('-')?;
        if parts.next().is_some() {
            return None;
        }
        let range = KeyGroupRange::new(first.parse().ok()?, last.parse().ok()?);
        let parsed = RunName {
            generation,
            epoch,
            range,
        };
        (parsed.to_string() == name).then_some(parsed)
    }
}

pub fn encode(runs: &[RunRef]) -> Bytes {
    let mut b = BytesMut::new();
    b.put_u8(TAG);
    b.put_u8(VERSION);
    put_varint(&mut b, runs.len() as u64);
    for r in runs {
        put_bytes(&mut b, r.name.as_bytes());
        put_varint(&mut b, r.format.into());
        put_varint(&mut b, r.size);
        put_varint(&mut b, r.sections.len() as u64);
        for s in &r.sections {
            b.put_u8(s.kind.code());
            put_varint(&mut b, s.epoch);
            put_varint(&mut b, s.groups.first.into());
            put_varint(&mut b, s.groups.last.into());
            put_varint(&mut b, s.entries);
            put_varint(&mut b, s.raw_bytes);
            put_varint(&mut b, s.stored_bytes);
            match s.times {
                Some((lo, hi)) => {
                    b.put_u8(1);
                    b.put_i64(lo);
                    b.put_i64(hi);
                }
                None => b.put_u8(0),
            }
            put_varint(&mut b, s.meta.0);
            put_varint(&mut b, s.meta.1);
        }
    }
    b.freeze()
}

/// The runs a snapshot of [`super::RunStores`] names.
pub fn runs(snapshot: &Snapshot) -> Result<Vec<RunRef>> {
    decode(&snapshot.data).map_err(|e| match e {
        Error::Corrupt(m) => Error::Snapshot(m),
        e => e,
    })
}

fn decode(data: &[u8]) -> Result<Vec<RunRef>> {
    let mut r = Reader::new(data, "a snapshot of runs");
    if r.u8()? != TAG {
        return Err(Error::Snapshot("not a snapshot of runs".into()));
    }
    let version = r.u8()?;
    if version != VERSION {
        return Err(Error::Snapshot(format!(
            "a snapshot of runs of version {version}, where this engine reads {VERSION}"
        )));
    }
    let mut out = Vec::new();
    for _ in 0..r.count()? {
        let name = String::from_utf8(r.bytes()?.to_vec())
            .map_err(|_| Error::Snapshot("a run's name isn't UTF-8".into()))?;
        let format = u32::try_from(r.varint()?).map_err(|_| r.corrupt())?;
        let size = r.varint()?;
        let mut sections = Vec::new();
        for _ in 0..r.count()? {
            let kind = SectionKind::from_code(r.u8()?)
                .ok_or_else(|| Error::Snapshot("a section of an unknown kind".into()))?;
            let epoch = r.varint()?;
            let groups = KeyGroupRange::new(r.group()?, r.group()?);
            if groups.first > groups.last {
                return Err(Error::Snapshot(
                    "a section's key groups are reversed".into(),
                ));
            }
            let (entries, raw_bytes, stored_bytes) = (r.varint()?, r.varint()?, r.varint()?);
            let times = match r.u8()? {
                0 => None,
                _ => Some((r.i64()?, r.i64()?)),
            };
            let meta = (r.varint()?, r.varint()?);
            sections.push(SectionRef {
                kind,
                epoch,
                groups,
                entries,
                raw_bytes,
                stored_bytes,
                times,
                meta,
            });
        }
        out.push(RunRef {
            name,
            format,
            size,
            sections,
        });
    }
    if !r.is_empty() {
        return Err(Error::Snapshot("bytes past a snapshot's end".into()));
    }
    Ok(out)
}

/// The runs `snapshots` name: a run no kept record's snapshots name can be
/// deleted, once no barrier still to be recorded can name it either.
pub fn named_runs(snapshots: &[Snapshot]) -> Result<BTreeSet<String>> {
    let mut out = BTreeSet::new();
    for s in snapshots {
        out.extend(runs(s)?.into_iter().map(|r| r.name));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_round_trip_and_nothing_else_parses() {
        let n = RunName {
            generation: 3,
            epoch: 40,
            range: KeyGroupRange::new(0, 63),
        };
        assert_eq!(n.to_string(), "g3/e40/kg0-63.run");
        assert_eq!(RunName::parse("g3/e40/kg0-63.run"), Some(n));
        for bad in [
            "g3/e40/kg0-63",
            "g3/e40/kg0-63.run/x",
            "g03/e40/kg0-63.run",
            "e40/kg0-63.run",
            "g3/e40/kg063.run",
        ] {
            assert_eq!(RunName::parse(bad), None, "{bad}");
        }
    }

    #[test]
    fn a_snapshot_round_trips_and_refuses_others() {
        let runs = vec![RunRef {
            name: "g1/e2/kg0-3.run".into(),
            format: 1,
            size: 999,
            sections: vec![SectionRef {
                kind: SectionKind::Set,
                epoch: 2,
                groups: KeyGroupRange::new(1, 3),
                entries: 5,
                raw_bytes: 100,
                stored_bytes: 80,
                times: Some((-4, i64::MAX)),
                meta: (500, 40),
            }],
        }];
        let data = encode(&runs);
        let snap = |data: Bytes| Snapshot {
            range: KeyGroupRange::new(0, 3),
            layout: crate::LAYOUT,
            data,
        };
        assert_eq!(super::runs(&snap(data.clone())).ok(), Some(runs));
        for bad in [
            data.slice(..data.len() - 1),
            Bytes::from_static(b"g1/e1/kg0-3"),
            Bytes::from_static(b"R\x02"),
        ] {
            assert!(matches!(super::runs(&snap(bad)), Err(Error::Snapshot(_))));
        }
    }
}
