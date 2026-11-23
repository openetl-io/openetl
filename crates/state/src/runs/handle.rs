//! What the checkpoint record keeps of runs: a lane's snapshot names each
//! section it reads, by run, so a section can be replaced or dropped
//! without touching its run's others, and its operators' horizons.
//!
//! A snapshot's data is `tag u8 | version u8 | horizons | (operator |
//! first group | last group | horizon i64)* | runs | (name | format | size
//! | sections | (kind u8 | epoch | first group | last group | entries | raw
//! bytes | stored bytes | has times u8 (| least i64 | greatest i64) | meta
//! offset | meta length | operators + 1 | operator*)*)*`, in the run
//! format's encoding; operators + 1 is 0 where they aren't known. Version 1
//! has no horizons and no operators.

use std::collections::BTreeSet;
use std::fmt;

use bytes::{BufMut, Bytes, BytesMut};

use super::codec::{Reader, put_bytes, put_varint};
use crate::layout::SectionKind;
use crate::store::{Horizons, Snapshot};
use crate::{Error, KeyGroupRange, OperatorId, Result};

const TAG: u8 = b'R';
/// The snapshot's encoding: 2, with the horizons and each section's
/// operators. Version 1 reads too.
pub const VERSION: u8 = 2;

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
    /// The operators whose state it holds, in order; `None` where a
    /// snapshot of version 1 named it.
    pub operators: Option<Vec<u32>>,
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

/// The name of a run a lane writes: `g{generation}/e{epoch}/kg{first}-
/// {last}.run` at a barrier, of that epoch's changes; `….early{n}.run`
/// for its `n`th early flush, from 1, of the epoch's changes until then,
/// which the barrier's run follows; and `….merge.run` for a merge's
/// output, which the barrier of `epoch` names first. No two attempts or
/// epochs write the same name, and none is written twice: a lane names at
/// most one merge at a barrier, and counts its early flushes on across an
/// epoch that it opens again, and across a store opened again on its key
/// groups.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunName {
    pub generation: u64,
    pub epoch: u64,
    pub range: KeyGroupRange,
    /// A merge's output, whose sections hold earlier epochs'.
    pub merged: bool,
    /// An early flush's number in its epoch, from 1; `None` for the
    /// barrier's run and a merge's.
    pub early: Option<u32>,
}

impl fmt::Display for RunName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "g{}/e{}/kg{}-{}",
            self.generation, self.epoch, self.range.first, self.range.last
        )?;
        if let Some(n) = self.early {
            write!(f, "{EARLY}{n}")?;
        }
        f.write_str(if self.merged { MERGED } else { BARRIER })
    }
}

const BARRIER: &str = ".run";
const MERGED: &str = ".merge.run";
const EARLY: &str = ".early";

impl RunName {
    pub fn parse(name: &str) -> Option<RunName> {
        let mut parts = name.split('/');
        let generation = parts.next()?.strip_prefix('g')?.parse().ok()?;
        let epoch = parts.next()?.strip_prefix('e')?.parse().ok()?;
        let file = parts.next()?.strip_prefix("kg")?;
        let (groups, merged) = match file.strip_suffix(MERGED) {
            Some(g) => (g, true),
            None => (file.strip_suffix(BARRIER)?, false),
        };
        let (groups, early) = match groups.split_once(EARLY) {
            Some((g, n)) if !merged => (g, Some(n.parse().ok().filter(|n| *n > 0)?)),
            Some(_) => return None,
            None => (groups, None),
        };
        let (first, last) = groups.split_once('-')?;
        if parts.next().is_some() {
            return None;
        }
        let range = KeyGroupRange::new(first.parse().ok()?, last.parse().ok()?);
        let parsed = RunName {
            generation,
            epoch,
            range,
            merged,
            early,
        };
        (parsed.to_string() == name).then_some(parsed)
    }
}

/// An operator's horizon in a range of key groups, as a snapshot keeps it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Horizon {
    pub operator: OperatorId,
    pub groups: KeyGroupRange,
    pub at: i64,
}

/// What a snapshot of [`super::RunStores`] holds: the runs it names, with
/// the sections of each it reads, and its operators' horizons.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Named {
    pub runs: Vec<RunRef>,
    pub horizons: Vec<Horizon>,
}

impl Named {
    /// The horizons, by operator and group, of the key groups in `range`.
    pub fn horizons_in(&self, range: KeyGroupRange) -> Horizons {
        let mut out = Horizons::new();
        for h in &self.horizons {
            let (first, last) = (
                h.groups.first.max(range.first),
                h.groups.last.min(range.last),
            );
            for g in first..=last {
                out.insert((h.operator, g), h.at);
            }
        }
        out
    }
}

/// Horizons as ranges of key groups with one horizon each.
pub fn ranges_of(horizons: &Horizons) -> Vec<Horizon> {
    let mut out: Vec<Horizon> = Vec::new();
    for (&(operator, g), &at) in horizons {
        match out.last_mut() {
            Some(h)
                if h.operator == operator
                    && h.at == at
                    && h.groups.last.checked_add(1) == Some(g) =>
            {
                h.groups.last = g;
            }
            _ => out.push(Horizon {
                operator,
                groups: KeyGroupRange::new(g, g),
                at,
            }),
        }
    }
    out
}

/// A snapshot's data that names `runs`, with no horizons.
pub fn encode(runs: &[RunRef]) -> Bytes {
    encode_named(&Named {
        runs: runs.to_vec(),
        horizons: Vec::new(),
    })
}

pub fn encode_named(named: &Named) -> Bytes {
    let runs = &named.runs;
    let mut b = BytesMut::new();
    b.put_u8(TAG);
    b.put_u8(VERSION);
    put_varint(&mut b, named.horizons.len() as u64);
    for h in &named.horizons {
        put_varint(&mut b, h.operator.0.into());
        put_varint(&mut b, h.groups.first.into());
        put_varint(&mut b, h.groups.last.into());
        b.put_i64(h.at);
    }
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
            match &s.operators {
                Some(ops) => {
                    put_varint(&mut b, ops.len() as u64 + 1);
                    for op in ops {
                        put_varint(&mut b, (*op).into());
                    }
                }
                None => put_varint(&mut b, 0),
            }
        }
    }
    b.freeze()
}

/// The runs a snapshot of [`super::RunStores`] names.
pub fn runs(snapshot: &Snapshot) -> Result<Vec<RunRef>> {
    Ok(named(snapshot)?.runs)
}

/// What a snapshot of [`super::RunStores`] holds.
pub fn named(snapshot: &Snapshot) -> Result<Named> {
    decode(&snapshot.data).map_err(|e| match e {
        Error::Corrupt(m) => Error::Snapshot(m),
        e => e,
    })
}

fn decode(data: &[u8]) -> Result<Named> {
    let mut r = Reader::new(data, "a snapshot of runs");
    if r.u8()? != TAG {
        return Err(Error::Snapshot("not a snapshot of runs".into()));
    }
    let version = r.u8()?;
    if version != 1 && version != VERSION {
        return Err(Error::Snapshot(format!(
            "a snapshot of runs of version {version}, where this engine reads 1 to {VERSION}"
        )));
    }
    let mut horizons = Vec::new();
    if version >= 2 {
        for _ in 0..r.count()? {
            let operator = OperatorId(u32::try_from(r.varint()?).map_err(|_| r.corrupt())?);
            let groups = KeyGroupRange::new(r.group()?, r.group()?);
            if groups.first > groups.last {
                return Err(Error::Snapshot(
                    "a horizon's key groups are reversed".into(),
                ));
            }
            horizons.push(Horizon {
                operator,
                groups,
                at: r.i64()?,
            });
        }
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
            let operators = match version {
                1 => None,
                _ => match r.count()? {
                    0 => None,
                    n => {
                        let mut ops = Vec::new();
                        for _ in 1..n {
                            ops.push(u32::try_from(r.varint()?).map_err(|_| r.corrupt())?);
                        }
                        Some(ops)
                    }
                },
            };
            sections.push(SectionRef {
                kind,
                epoch,
                groups,
                entries,
                raw_bytes,
                stored_bytes,
                times,
                meta,
                operators,
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
    Ok(Named {
        runs: out,
        horizons,
    })
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
            merged: false,
            early: None,
        };
        assert_eq!(n.to_string(), "g3/e40/kg0-63.run");
        assert_eq!(RunName::parse("g3/e40/kg0-63.run"), Some(n));
        let m = RunName { merged: true, ..n };
        assert_eq!(m.to_string(), "g3/e40/kg0-63.merge.run");
        assert_eq!(RunName::parse("g3/e40/kg0-63.merge.run"), Some(m));
        let e = RunName {
            early: Some(2),
            ..n
        };
        assert_eq!(e.to_string(), "g3/e40/kg0-63.early2.run");
        assert_eq!(RunName::parse("g3/e40/kg0-63.early2.run"), Some(e));
        for bad in [
            "g3/e40/kg0-63.early0.run",
            "g3/e40/kg0-63.early02.run",
            "g3/e40/kg0-63.early.run",
            "g3/e40/kg0-63.early2.merge.run",
            "g3/e40/kg0-63",
            "g3/e40/kg0-63.run/x",
            "g03/e40/kg0-63.run",
            "e40/kg0-63.run",
            "g3/e40/kg063.run",
            "g3/e40/kg0-63.merge",
            "g3/e40/kg0-63.x.run",
        ] {
            assert_eq!(RunName::parse(bad), None, "{bad}");
        }
    }

    fn section(operators: Option<Vec<u32>>) -> SectionRef {
        SectionRef {
            kind: SectionKind::Set,
            epoch: 2,
            groups: KeyGroupRange::new(1, 3),
            entries: 5,
            raw_bytes: 100,
            stored_bytes: 80,
            times: Some((-4, i64::MAX)),
            meta: (500, 40),
            operators,
        }
    }

    #[test]
    fn a_snapshot_round_trips_and_refuses_others() {
        let named = Named {
            runs: vec![RunRef {
                name: "g1/e2/kg0-3.run".into(),
                format: 1,
                size: 999,
                sections: vec![section(Some(vec![7, u32::MAX])), section(None)],
            }],
            horizons: vec![
                Horizon {
                    operator: OperatorId(7),
                    groups: KeyGroupRange::new(0, 1),
                    at: -3,
                },
                Horizon {
                    operator: OperatorId(u32::MAX),
                    groups: KeyGroupRange::new(2, 3),
                    at: i64::MAX,
                },
            ],
        };
        let data = encode_named(&named);
        let snap = |data: Bytes| Snapshot {
            range: KeyGroupRange::new(0, 3),
            layout: crate::LAYOUT,
            data,
            pending: Bytes::new(),
        };
        assert_eq!(super::named(&snap(data.clone())).ok(), Some(named.clone()));
        assert_eq!(super::runs(&snap(data.clone())).ok(), Some(named.runs));
        for bad in [
            data.slice(..data.len() - 1),
            Bytes::from_static(b"g1/e1/kg0-3"),
            Bytes::from_static(b"R\x02"),
            Bytes::from_static(b"R\x03\x00\x00"),
        ] {
            assert!(matches!(super::runs(&snap(bad)), Err(Error::Snapshot(_))));
        }
    }

    /// Horizons go into a snapshot as ranges of groups, and come out as
    /// they went in, of the groups asked for.
    #[test]
    fn horizons_go_by_ranges_of_groups() {
        let mut h = Horizons::new();
        for g in 0..10 {
            h.insert((OperatorId(1), g), if g < 6 { 5 } else { 9 });
        }
        h.insert((OperatorId(2), 3), 1);
        h.insert((OperatorId(2), 5), 1);
        let ranges = ranges_of(&h);
        assert_eq!(
            ranges
                .iter()
                .map(|r| (r.operator.0, r.groups.first, r.groups.last, r.at))
                .collect::<Vec<_>>(),
            [(1, 0, 5, 5), (1, 6, 9, 9), (2, 3, 3, 1), (2, 5, 5, 1)]
        );
        let named = Named {
            runs: Vec::new(),
            horizons: ranges,
        };
        assert_eq!(named.horizons_in(KeyGroupRange::new(0, 65_535)), h);
        let part = named.horizons_in(KeyGroupRange::new(4, 6));
        let want: Horizons = h
            .into_iter()
            .filter(|((_, g), _)| (4..=6).contains(g))
            .collect();
        assert_eq!(part, want);
    }
}
