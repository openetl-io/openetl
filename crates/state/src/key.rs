//! How the primitives' keys are laid out in a store: key group first, so a
//! range of key groups is a range of keys, then the operator and the kind.
//!
//! `group u16 | operator u16 | kind u8 | rest`, big-endian, where rest is:
//! - a value: the key;
//! - a row: the key's length as a u32, the key, and the row's seq as a u64;
//! - a set's id: the id;
//! - a timer: its time as a u64 with the sign bit flipped, then the key.

use bytes::{BufMut, Bytes, BytesMut};

use crate::{KeyGroup, KeyRef};

/// A stateful operator's index in the plan's list of operator IDs.
pub type OperatorId = u16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    Value = b'v',
    Row = b'r',
    Set = b's',
    Timer = b't',
}

const HEAD: usize = 5;

fn head(group: KeyGroup, op: OperatorId, kind: Kind, extra: usize) -> BytesMut {
    let mut b = BytesMut::with_capacity(HEAD + extra);
    b.put_u16(group);
    b.put_u16(op);
    b.put_u8(kind as u8);
    b
}

pub fn value(op: OperatorId, k: KeyRef<'_>) -> Bytes {
    let mut b = head(k.group, op, Kind::Value, k.key.len());
    b.put_slice(k.key);
    b.freeze()
}

/// The prefix of every row of a key.
pub fn rows(op: OperatorId, k: KeyRef<'_>) -> Bytes {
    let mut b = head(k.group, op, Kind::Row, 4 + k.key.len() + 8);
    b.put_u32(len32(k.key.len()));
    b.put_slice(k.key);
    b.freeze()
}

pub fn row(op: OperatorId, k: KeyRef<'_>, seq: u64) -> Bytes {
    let mut b = BytesMut::from(&rows(op, k)[..]);
    b.put_u64(seq);
    b.freeze()
}

/// A row key's seq.
pub fn row_seq(key: &[u8]) -> u64 {
    let mut s = [0u8; 8];
    if key.len() >= 8 {
        s.copy_from_slice(&key[key.len() - 8..]);
    }
    u64::from_be_bytes(s)
}

pub fn set(op: OperatorId, k: KeyRef<'_>) -> Bytes {
    let mut b = head(k.group, op, Kind::Set, k.key.len());
    b.put_slice(k.key);
    b.freeze()
}

pub fn timer(op: OperatorId, time: i64, k: KeyRef<'_>) -> Bytes {
    let mut b = head(k.group, op, Kind::Timer, 8 + k.key.len());
    b.put_u64(flip(time));
    b.put_slice(k.key);
    b.freeze()
}

/// A timer key's time, group and key.
pub fn parse_timer(key: &[u8]) -> Option<(i64, KeyGroup, Bytes)> {
    if key.len() < HEAD + 8 || key[4] != Kind::Timer as u8 {
        return None;
    }
    let group = u16::from_be_bytes([key[0], key[1]]);
    let mut t = [0u8; 8];
    t.copy_from_slice(&key[HEAD..HEAD + 8]);
    let time = (u64::from_be_bytes(t) ^ (1 << 63)) as i64;
    Some((time, group, Bytes::copy_from_slice(&key[HEAD + 8..])))
}

/// The keys of one kind of one operator in one group: `[start, end)`.
pub fn kind_range(group: KeyGroup, op: OperatorId, kind: Kind) -> (Bytes, Bytes) {
    let start = head(group, op, kind, 0).freeze();
    (start.clone(), successor(&start))
}

/// The first key of a group, and the first past `last`, so `[start, end)`
/// covers groups `first..=last`. `end` is empty past the last group.
pub fn group_range(first: KeyGroup, last: KeyGroup) -> (Bytes, Bytes) {
    let start = Bytes::copy_from_slice(&first.to_be_bytes());
    let end = match last.checked_add(1) {
        Some(n) => Bytes::copy_from_slice(&n.to_be_bytes()),
        None => Bytes::new(),
    };
    (start, end)
}

/// The group a store key belongs to.
pub fn group_of(key: &[u8]) -> KeyGroup {
    if key.len() < 2 {
        return 0;
    }
    u16::from_be_bytes([key[0], key[1]])
}

/// The kind of a store key.
pub fn kind_of(key: &[u8]) -> Option<Kind> {
    match key.get(4)? {
        b'v' => Some(Kind::Value),
        b'r' => Some(Kind::Row),
        b's' => Some(Kind::Set),
        b't' => Some(Kind::Timer),
        _ => None,
    }
}

/// The smallest key greater than every key with this prefix.
pub fn successor(prefix: &[u8]) -> Bytes {
    let mut b = prefix.to_vec();
    while let Some(last) = b.pop() {
        if last < u8::MAX {
            b.push(last + 1);
            return Bytes::from(b);
        }
    }
    Bytes::new()
}

fn flip(t: i64) -> u64 {
    (t as u64) ^ (1 << 63)
}

fn len32(n: usize) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

/// A set entry's value: when it expires.
pub fn expiry(expires: i64) -> Bytes {
    Bytes::copy_from_slice(&expires.to_le_bytes())
}

pub fn parse_expiry(v: &[u8]) -> i64 {
    let mut b = [0u8; 8];
    if v.len() == 8 {
        b.copy_from_slice(v);
    }
    i64::from_le_bytes(b)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn timers_sort_by_time_and_parse_back() {
        let k = KeyRef {
            group: 7,
            key: b"abc",
        };
        let a = timer(1, -5, k);
        let b = timer(1, 3, k);
        assert!(a < b);
        assert_eq!(parse_timer(&b).unwrap(), (3, 7, Bytes::from_static(b"abc")));
    }

    #[test]
    fn a_group_range_covers_its_groups_only() {
        let (s, e) = group_range(3, 4);
        let k = |g| {
            value(
                0,
                KeyRef {
                    group: g,
                    key: b"x",
                },
            )
        };
        assert!(k(2) < s);
        assert!(s <= k(3) && k(4) < e);
        assert!(k(5) >= e);
        assert!(group_range(0, u16::MAX).1.is_empty());
    }
}
