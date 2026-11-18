//! A lane's pending set as a snapshot carries it: its changes since it last
//! committed them to its store, which the checkpoint record holds in place
//! of a run while they're small.
//!
//! The encoding is `tag u8 | version u8 | entries | (key | 0, or 1 and
//! value)* | checksum u64`, with lengths and counts as the run format's
//! varints and keys and values as a length and bytes; the checksum is
//! XXH3-64 of all before it. Keys are sorted, each once, and all in the
//! snapshot's key groups. No entries is no bytes.

use bytes::{BufMut, Bytes, BytesMut};

use crate::layout;
use crate::runs::codec::{Reader, put_bytes, put_varint, seal};
use crate::{Error, KeyGroupRange, Result};

const TAG: u8 = b'P';
/// The pending set's encoding.
pub const VERSION: u8 = 1;
/// The tag, the version and the checksum.
const FRAME: usize = 2 + 8;

fn varint_len(v: u64) -> usize {
    let bits = 64 - (v | 1).leading_zeros() as usize;
    bits.div_ceil(7)
}

fn entry_len(k: &[u8], v: Option<&[u8]>) -> usize {
    let key = varint_len(k.len() as u64) + k.len();
    let value = v.map_or(0, |v| varint_len(v.len() as u64) + v.len());
    key + 1 + value
}

/// The bytes [`encode`] gives for `entries`.
pub(crate) fn encoded_len<'a>(
    entries: impl ExactSizeIterator<Item = (&'a Bytes, &'a Option<Bytes>)>,
) -> usize {
    let n = entries.len();
    if n == 0 {
        return 0;
    }
    let body: usize = entries.map(|(k, v)| entry_len(k, v.as_deref())).sum();
    FRAME + varint_len(n as u64) + body
}

/// `entries`, sorted by key with each once, as a snapshot carries them.
pub(crate) fn encode<'a>(
    entries: impl ExactSizeIterator<Item = (&'a Bytes, &'a Option<Bytes>)>,
) -> Bytes {
    let n = entries.len();
    if n == 0 {
        return Bytes::new();
    }
    let mut b = BytesMut::new();
    b.put_u8(TAG);
    b.put_u8(VERSION);
    put_varint(&mut b, n as u64);
    for (k, v) in entries {
        put_bytes(&mut b, k);
        match v {
            Some(v) => {
                b.put_u8(1);
                put_bytes(&mut b, v);
            }
            None => b.put_u8(0),
        }
    }
    seal(&mut b, 0);
    b.freeze()
}

/// The entries a snapshot of `range` carries in `data`.
pub(crate) fn decode(data: &Bytes, range: KeyGroupRange) -> Result<Vec<(Bytes, Option<Bytes>)>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let bad = |m: String| Error::Snapshot(format!("a pending set: {m}"));
    let mut r = Reader::sealed(data, "a pending set").map_err(|e| match e {
        Error::Corrupt(m) => Error::Snapshot(m),
        e => e,
    })?;
    let header = (r.u8(), r.u8());
    match header {
        (Ok(TAG), Ok(VERSION)) => {}
        (Ok(TAG), Ok(v)) => {
            return Err(bad(format!(
                "version {v}, where this engine reads {VERSION}"
            )));
        }
        _ => return Err(bad("not a pending set".into())),
    }
    let corrupt = |e: Error| match e {
        Error::Corrupt(m) => Error::Snapshot(m),
        e => e,
    };
    let n = r.count().map_err(corrupt)?;
    let mut out: Vec<(Bytes, Option<Bytes>)> = Vec::with_capacity(n.min(r.remaining()));
    for _ in 0..n {
        let k = data.slice_ref(r.bytes().map_err(corrupt)?);
        let v = match r.u8().map_err(corrupt)? {
            0 => None,
            1 => Some(data.slice_ref(r.bytes().map_err(corrupt)?)),
            f => return Err(bad(format!("an entry flagged {f}"))),
        };
        if out.last().is_some_and(|(p, _)| *p >= k) {
            return Err(bad("keys out of order".into()));
        }
        match layout::group_of(&k) {
            Some(g) if range.contains(g.into()) => {}
            Some(g) => {
                return Err(bad(format!(
                    "a key of key group {g}, in a snapshot of key groups {} to {}",
                    range.first, range.last
                )));
            }
            None => return Err(bad("a key of no key group".into())),
        }
        out.push((k, v));
    }
    if !r.is_empty() {
        return Err(bad("bytes past its entries".into()));
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::OperatorId;

    fn entries() -> BTreeMap<Bytes, Option<Bytes>> {
        let op = OperatorId(7);
        let mut m = BTreeMap::new();
        m.insert(layout::value(3, op, b"a"), Some(Bytes::from_static(b"1")));
        m.insert(layout::value(3, op, b"b"), None);
        m.insert(layout::timer(op, -5, 4, b"k"), Some(Bytes::new()));
        m.insert(
            layout::value(4, op, &[0xff; 300]),
            Some(Bytes::from(vec![9; 200])),
        );
        m
    }

    #[test]
    fn a_pending_set_round_trips_and_its_length_is_known() {
        let m = entries();
        let data = encode(m.iter());
        assert_eq!(data.len(), encoded_len(m.iter()));
        let back = decode(&data, KeyGroupRange::new(0, 15)).unwrap();
        assert_eq!(back, m.into_iter().collect::<Vec<_>>());
        let none = BTreeMap::new();
        assert!(encode(none.iter()).is_empty());
        assert_eq!(encoded_len(none.iter()), 0);
        assert!(
            decode(&Bytes::new(), KeyGroupRange::new(0, 0))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn a_pending_set_that_doesnt_read_is_refused() {
        let m = entries();
        let data = encode(m.iter());
        let range = KeyGroupRange::new(0, 15);
        let mut flipped = data.to_vec();
        flipped[5] ^= 1;
        let mut newer = data.to_vec();
        newer[1] = VERSION + 1;
        let at = newer.len() - 8;
        let sum = crate::runs::codec::checksum(&newer[..at]);
        newer[at..].copy_from_slice(&sum.to_be_bytes());
        for bad in [
            data.slice(..data.len() - 1),
            Bytes::from(flipped),
            Bytes::from(newer),
            Bytes::from_static(b"P"),
        ] {
            assert!(
                matches!(decode(&bad, range), Err(Error::Snapshot(_))),
                "{bad:?}"
            );
        }
        // A key outside the snapshot's key groups.
        let err = decode(&data, KeyGroupRange::new(0, 3)).unwrap_err();
        assert!(err.to_string().contains("key group 4"), "{err}");
    }
}
