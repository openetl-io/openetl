//! How state is laid out as a store's keys, layout [`LAYOUT`]. Fields are
//! big-endian, so keys sort as their fields do, and a time has its sign bit
//! flipped, so times sort as signed numbers. A key's length goes before it
//! wherever more fields follow, so no key's fields are a prefix of
//! another's.
//!
//! Keyed state goes key group first, so a range of key groups is a range of
//! keys: `0x00 | group u16 | operator u32 | kind u8 | rest`, where rest is
//! - for a value (`v`): the key;
//! - for a set's id (`s`): the id, with its expiry as the value;
//! - for a set's horizon (`h`): nothing, with the horizon as the value.
//!   Stores once kept a set's horizon so; a store now keeps an operator's
//!   horizon beside its versions, and a layer reads this one as well;
//! - for a row (`r`): `time | key length u32 | key | seq u64`. A group's
//!   rows sort by the time they fall due, so the rows a watermark releases
//!   are one range.
//!
//! Timers go time first, so the timers a watermark makes due are one range
//! across the store's key groups, and a restore reads none of them before
//! one is due: `0x01 | operator u32 | t | time | group u16 | key length u32
//! | key`.

use bytes::{BufMut, Bytes, BytesMut};

use crate::OperatorId;

/// The layout's version, which every snapshot names.
pub const LAYOUT: u32 = 1;

const KEYED: u8 = 0x00;
const TIMED: u8 = 0x01;
/// The space, the group, the operator and the kind.
const KEYED_HEAD: usize = 1 + 2 + 4 + 1;
/// The space, the operator, the kind and the time.
const TIMED_HEAD: usize = 1 + 4 + 1 + 8;

#[derive(Clone, Copy)]
#[repr(u8)]
enum Kind {
    Value = b'v',
    Set = b's',
    Horizon = b'h',
    Row = b'r',
    Timer = b't',
}

fn keyed(group: u16, op: OperatorId, kind: Kind, extra: usize) -> BytesMut {
    let mut b = BytesMut::with_capacity(KEYED_HEAD + extra);
    b.put_u8(KEYED);
    b.put_u16(group);
    b.put_u32(op.0);
    b.put_u8(kind as u8);
    b
}

fn timed(op: OperatorId, time: i64, extra: usize) -> BytesMut {
    let mut b = BytesMut::with_capacity(TIMED_HEAD + extra);
    b.put_u8(TIMED);
    b.put_u32(op.0);
    b.put_u8(Kind::Timer as u8);
    b.put_u64(flip(time));
    b
}

pub fn value(group: u16, op: OperatorId, key: &[u8]) -> Bytes {
    let mut b = keyed(group, op, Kind::Value, key.len());
    b.put_slice(key);
    b.freeze()
}

pub fn set(group: u16, op: OperatorId, id: &[u8]) -> Bytes {
    let mut b = keyed(group, op, Kind::Set, id.len());
    b.put_slice(id);
    b.freeze()
}

/// The keys of an operator's set in a group: `[start, end)`.
pub fn set_range(group: u16, op: OperatorId) -> (Bytes, Bytes) {
    let start = keyed(group, op, Kind::Set, 0).freeze();
    let end = successor(&start);
    (start, end)
}

pub fn horizon(group: u16, op: OperatorId) -> Bytes {
    keyed(group, op, Kind::Horizon, 0).freeze()
}

/// A horizon key's group and operator.
pub fn parse_horizon(key: &[u8]) -> Option<(u16, OperatorId)> {
    if key.len() != KEYED_HEAD || key[0] != KEYED || key[7] != Kind::Horizon as u8 {
        return None;
    }
    Some((u16_at(key, 1)?, OperatorId(u32_at(key, 3)?)))
}

/// The prefix of a key's rows at a time.
pub fn row_slot(group: u16, op: OperatorId, time: i64, key: &[u8]) -> Bytes {
    let mut b = keyed(group, op, Kind::Row, 8 + 4 + key.len() + 8);
    b.put_u64(flip(time));
    b.put_u32(len32(key.len()));
    b.put_slice(key);
    b.freeze()
}

pub fn row(group: u16, op: OperatorId, time: i64, key: &[u8], seq: u64) -> Bytes {
    let mut b = BytesMut::from(&row_slot(group, op, time, key)[..]);
    b.put_u64(seq);
    b.freeze()
}

/// The keys of an operator's rows in a group at or before `watermark`:
/// `[start, end)`.
pub fn rows_due(group: u16, op: OperatorId, watermark: i64) -> (Bytes, Bytes) {
    rows_due_after(group, op, i64::MIN, watermark)
}

/// As [`rows_due`], of the rows after `after`; `i64::MIN` for all of them.
pub fn rows_due_after(group: u16, op: OperatorId, after: i64, watermark: i64) -> (Bytes, Bytes) {
    let at = |t: Option<i64>| match t {
        Some(t) => {
            let mut b = keyed(group, op, Kind::Row, 8);
            b.put_u64(flip(t));
            b.freeze()
        }
        None => successor(&keyed(group, op, Kind::Row, 0)),
    };
    let end = at(watermark.checked_add(1));
    let start = match after {
        i64::MIN => keyed(group, op, Kind::Row, 0).freeze(),
        a => at(a.checked_add(1)),
    };
    (start, end)
}

/// A row key's group, time, key and seq.
pub fn parse_row(k: &[u8]) -> Option<(u16, i64, &[u8], u64)> {
    if k.len() < KEYED_HEAD + 8 + 4 + 8 || k[0] != KEYED || k[7] != Kind::Row as u8 {
        return None;
    }
    let group = u16_at(k, 1)?;
    let time = unflip(u64_at(k, KEYED_HEAD)?);
    let len = usize::try_from(u32_at(k, KEYED_HEAD + 8)?).ok()?;
    let key_at = KEYED_HEAD + 8 + 4;
    if k.len() != key_at + len + 8 {
        return None;
    }
    let seq = u64_at(k, key_at + len)?;
    Some((group, time, &k[key_at..key_at + len], seq))
}

pub fn timer(op: OperatorId, time: i64, group: u16, key: &[u8]) -> Bytes {
    let mut b = timed(op, time, 2 + 4 + key.len());
    b.put_u16(group);
    b.put_u32(len32(key.len()));
    b.put_slice(key);
    b.freeze()
}

/// The keys of an operator's timers at or before `watermark`: `[start, end)`.
pub fn timers_due(op: OperatorId, watermark: i64) -> (Bytes, Bytes) {
    timers_due_after(op, i64::MIN, watermark)
}

/// As [`timers_due`], of the timers after `after`; `i64::MIN` for all of
/// them.
pub fn timers_due_after(op: OperatorId, after: i64, watermark: i64) -> (Bytes, Bytes) {
    let at = |t: Option<i64>| match t {
        Some(t) => timed(op, t, 0).freeze(),
        None => successor(&timed(op, i64::MIN, 0)[..TIMED_HEAD - 8]),
    };
    let end = at(watermark.checked_add(1));
    let start = match after {
        i64::MIN => timed(op, i64::MIN, 0).freeze(),
        a => at(a.checked_add(1)),
    };
    (start, end)
}

/// The operator of any key of this layout.
pub fn operator_of(k: &[u8]) -> Option<OperatorId> {
    match *k.first()? {
        KEYED => u32_at(k, 3).map(OperatorId),
        TIMED => u32_at(k, 1).map(OperatorId),
        _ => None,
    }
}

/// A timer key's time, group and key.
pub fn parse_timer(k: &[u8]) -> Option<(i64, u16, &[u8])> {
    if k.len() < TIMED_HEAD + 2 + 4 || k[0] != TIMED || k[5] != Kind::Timer as u8 {
        return None;
    }
    let time = unflip(u64_at(k, 6)?);
    let group = u16_at(k, TIMED_HEAD)?;
    let len = usize::try_from(u32_at(k, TIMED_HEAD + 2)?).ok()?;
    let key_at = TIMED_HEAD + 2 + 4;
    if k.len() != key_at + len {
        return None;
    }
    Some((time, group, &k[key_at..]))
}

/// The kind of state a key holds, and so the section of a run it goes in:
/// each section has an index and a filter of its own, and ages as its
/// kind does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SectionKind {
    /// Values per key, and the set's horizons.
    Values,
    Rows,
    Set,
    Timers,
}

impl SectionKind {
    pub const ALL: [SectionKind; 4] = [
        SectionKind::Values,
        SectionKind::Rows,
        SectionKind::Set,
        SectionKind::Timers,
    ];

    pub(crate) fn index(self) -> usize {
        self as usize
    }

    pub(crate) fn code(self) -> u8 {
        match self {
            SectionKind::Values => b'v',
            SectionKind::Rows => b'r',
            SectionKind::Set => b's',
            SectionKind::Timers => b't',
        }
    }

    pub(crate) fn from_code(c: u8) -> Option<SectionKind> {
        SectionKind::ALL.into_iter().find(|k| k.code() == c)
    }
}

pub fn section_of(k: &[u8]) -> Option<SectionKind> {
    match *k.first()? {
        KEYED if k.len() >= KEYED_HEAD => kind_section(k[7]),
        TIMED if k.len() >= TIMED_HEAD && k[5] == Kind::Timer as u8 => Some(SectionKind::Timers),
        _ => None,
    }
}

fn kind_section(kind: u8) -> Option<SectionKind> {
    match kind {
        k if k == Kind::Value as u8 || k == Kind::Horizon as u8 => Some(SectionKind::Values),
        k if k == Kind::Row as u8 => Some(SectionKind::Rows),
        k if k == Kind::Set as u8 => Some(SectionKind::Set),
        _ => None,
    }
}

/// Which sections can hold a key in `[start, end)`, by
/// [`SectionKind::index`]: exactly the one for a scan within one kind of
/// one operator's group, as the layer's are, and more for a wider one.
pub fn sections_between(start: &[u8], end: &[u8]) -> [bool; 4] {
    let mut out = [false; 4];
    if start >= end {
        return out;
    }
    out[SectionKind::Timers.index()] = end > &[TIMED][..] && start < &[TIMED + 1][..];
    if start >= &[TIMED][..] {
        return out;
    }
    let same = start.len() >= KEYED_HEAD - 1
        && end.len() > KEYED_HEAD - 1
        && start[0] == KEYED
        && start[..KEYED_HEAD - 1] == end[..KEYED_HEAD - 1];
    if !same {
        for k in [SectionKind::Values, SectionKind::Rows, SectionKind::Set] {
            out[k.index()] = true;
        }
        return out;
    }
    let lo = start.get(KEYED_HEAD - 1).copied().unwrap_or(0);
    let hi = end[KEYED_HEAD - 1];
    // An end of exactly a kind's prefix holds none of that kind.
    let below = |kind: Kind| {
        let k = kind as u8;
        lo <= k && (k < hi || (k == hi && end.len() > KEYED_HEAD))
    };
    for kind in [Kind::Value, Kind::Horizon, Kind::Row, Kind::Set] {
        if below(kind)
            && let Some(s) = kind_section(kind as u8)
        {
            out[s.index()] = true;
        }
    }
    out
}

/// What a section's filter holds of a key: for a row, its slot, so a read
/// of a key's rows at a time asks the filter; any other key whole.
pub fn filtered(k: &[u8]) -> &[u8] {
    match parse_row(k) {
        Some(_) => &k[..k.len() - 8],
        None => k,
    }
}

/// Whether `[start, end)` is exactly one row slot's rows, which a filter of
/// slots can rule out.
pub fn is_slot(start: &[u8], end: &[u8]) -> bool {
    if start.len() < KEYED_HEAD + 8 + 4 || start[0] != KEYED || start[7] != Kind::Row as u8 {
        return false;
    }
    let Some(len) = u32_at(start, KEYED_HEAD + 8).and_then(|n| usize::try_from(n).ok()) else {
        return false;
    };
    start.len() == KEYED_HEAD + 8 + 4 + len && successor(start)[..] == *end
}

/// The key group of any key of this layout.
pub fn group_of(k: &[u8]) -> Option<u16> {
    match *k.first()? {
        KEYED => u16_at(k, 1),
        TIMED => u16_at(k, TIMED_HEAD),
        _ => None,
    }
}

/// The group of a key in keyed state, or of a bound of a scan of it.
pub fn keyed_group(k: &[u8]) -> Option<u16> {
    match *k.first()? {
        KEYED => u16_at(k, 1),
        _ => None,
    }
}

/// The smallest key greater than every key that starts with `prefix`, which
/// starts with a space, so the key exists.
pub fn successor(prefix: &[u8]) -> Bytes {
    let mut b = prefix.to_vec();
    while let Some(last) = b.pop() {
        if last < u8::MAX {
            b.push(last + 1);
            break;
        }
    }
    Bytes::from(b)
}

/// The smallest key greater than `k`.
pub fn after(k: &[u8]) -> Bytes {
    let mut b = Vec::with_capacity(k.len() + 1);
    b.extend_from_slice(k);
    b.push(0);
    Bytes::from(b)
}

/// An expiry or a horizon as a value.
pub fn time_value(t: i64) -> Bytes {
    Bytes::copy_from_slice(&t.to_be_bytes())
}

pub fn parse_time_value(v: &[u8]) -> Option<i64> {
    Some(i64::from_be_bytes(v.try_into().ok()?))
}

fn flip(t: i64) -> u64 {
    u64::from_be_bytes(t.to_be_bytes()) ^ (1 << 63)
}

fn unflip(u: u64) -> i64 {
    i64::from_be_bytes((u ^ (1 << 63)).to_be_bytes())
}

/// A key's length; keys are under 4 GiB.
fn len32(n: usize) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

fn u16_at(k: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_be_bytes(k.get(at..at + 2)?.try_into().ok()?))
}

fn u32_at(k: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_be_bytes(k.get(at..at + 4)?.try_into().ok()?))
}

fn u64_at(k: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_be_bytes(k.get(at..at + 8)?.try_into().ok()?))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const OP: OperatorId = OperatorId(7);
    const TIMES: [i64; 7] = [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX];
    const KEYS: [&[u8]; 6] = [b"", b"\x00", b"a", b"b", b"ab", b"\xff\xff"];

    fn sorted<T: Ord + Clone>(v: &[T]) -> bool {
        v.windows(2).all(|w| w[0] < w[1])
    }

    #[test]
    fn times_sort_as_signed_numbers() {
        let rows: Vec<Bytes> = TIMES.iter().map(|&t| row(3, OP, t, b"k", 0)).collect();
        let timers: Vec<Bytes> = TIMES.iter().map(|&t| timer(OP, t, 3, b"k")).collect();
        assert!(sorted(&rows));
        assert!(sorted(&timers));
        for (&t, r) in TIMES.iter().zip(&rows) {
            assert_eq!(parse_row(r).unwrap().1, t);
        }
        for (&t, k) in TIMES.iter().zip(&timers) {
            assert_eq!(parse_timer(k).unwrap().0, t);
        }
    }

    /// Keys by length, then bytewise: the order rows and timers come out in.
    fn by_length(a: &&[u8], b: &&[u8]) -> std::cmp::Ordering {
        a.len().cmp(&b.len()).then(a.cmp(b))
    }

    #[test]
    fn rows_sort_by_time_then_key_then_seq_within_a_group() {
        let mut want = Vec::new();
        let mut keys = KEYS.to_vec();
        keys.sort_by(by_length);
        for t in [-5, 0, 9] {
            for k in &keys {
                for seq in [0, 1, u64::MAX] {
                    want.push(row(2, OP, t, k, seq));
                }
            }
        }
        assert!(sorted(&want));
        for (i, r) in want.iter().enumerate() {
            let (g, _, k, _) = parse_row(r).unwrap();
            assert_eq!(g, 2);
            assert_eq!(k, keys[(i / 3) % keys.len()]);
        }
    }

    #[test]
    fn a_slot_holds_only_its_keys_rows() {
        for a in KEYS {
            let slot = row_slot(1, OP, 4, a);
            let end = successor(&slot);
            for b in KEYS {
                for t in [3, 4, 5] {
                    let r = row(1, OP, t, b, 9);
                    let inside = slot <= r && r < end;
                    assert_eq!(inside, a == b && t == 4, "{a:?} {b:?} {t}");
                }
            }
        }
    }

    #[test]
    fn rows_due_are_those_at_or_before_the_watermark() {
        for w in TIMES {
            let (start, end) = rows_due(5, OP, w);
            for t in TIMES {
                for k in KEYS {
                    let r = row(5, OP, t, k, 1);
                    assert_eq!(start <= r && r < end, t <= w, "{t} {w}");
                    assert!(row(4, OP, t, k, 1) < start && row(6, OP, t, k, 1) >= end);
                    assert!(row(5, OperatorId(8), t, k, 1) >= end);
                    assert!(value(5, OP, k) >= end && set(5, OP, k) >= end);
                    assert!(horizon(5, OP) < start);
                }
            }
        }
    }

    /// After a horizon, the rows and timers due are those past it, and
    /// `i64::MIN` is no horizon.
    #[test]
    fn what_is_due_after_a_horizon_is_past_it() {
        for after in TIMES {
            for w in TIMES {
                let (rs, re) = rows_due_after(5, OP, after, w);
                let (ts, te) = timers_due_after(OP, after, w);
                for t in TIMES {
                    let due = t <= w && (after == i64::MIN || t > after);
                    let r = row(5, OP, t, b"k", 1);
                    assert_eq!(rs <= r && r < re, due, "row {t} after {after} to {w}");
                    let k = timer(OP, t, 5, b"k");
                    assert_eq!(ts <= k && k < te, due, "timer {t} after {after} to {w}");
                }
            }
        }
        assert_eq!(operator_of(&row(1, OP, 0, b"k", 0)), Some(OP));
        assert_eq!(operator_of(&timer(OP, 0, 1, b"k")), Some(OP));
        assert_eq!(operator_of(&value(1, OP, b"")), Some(OP));
        assert_eq!(operator_of(b"\x02"), None);
    }

    #[test]
    fn timers_sort_by_time_then_group_then_key() {
        let mut keys = KEYS.to_vec();
        keys.sort_by(by_length);
        let mut want = Vec::new();
        for t in [-1, 0, 7] {
            for g in [0, 1, u16::MAX] {
                for k in &keys {
                    want.push(timer(OP, t, g, k));
                }
            }
        }
        assert!(sorted(&want));
        let (start, end) = timers_due(OP, 0);
        for k in &want {
            let (t, g, key) = parse_timer(k).unwrap();
            assert_eq!(start <= *k && *k < end, t <= 0);
            assert_eq!(group_of(k), Some(g));
            assert!(keys.contains(&key));
        }
        let (start, end) = timers_due(OP, i64::MAX);
        assert!(want.iter().all(|k| start <= *k && *k < end));
        assert!(timer(OperatorId(6), i64::MAX, 0, b"") < start);
        assert!(timer(OperatorId(8), i64::MIN, 0, b"") >= end);
    }

    #[test]
    fn key_groups_lead_keyed_state_and_kinds_keep_apart() {
        let keys = |g: u16| {
            [
                value(g, OP, b"x"),
                set(g, OP, b"x"),
                horizon(g, OP),
                row(g, OP, 0, b"x", 0),
                value(g, OperatorId(u32::MAX), b""),
            ]
        };
        for g in [0, 1, 255, u16::MAX - 1] {
            for a in keys(g) {
                for b in keys(g + 1) {
                    assert!(a < b);
                }
                assert_eq!(group_of(&a), Some(g));
                // Every keyed key sorts before every timer.
                assert!(a < timer(OperatorId(0), i64::MIN, 0, b""));
            }
        }
        let (s, e) = set_range(3, OP);
        assert!(s <= set(3, OP, b"") && set(3, OP, b"\xff\xff") < e);
        assert!(horizon(3, OP) < s || horizon(3, OP) >= e);
        assert!(value(3, OP, b"") < s || value(3, OP, b"") >= e);
        assert_eq!(parse_horizon(&horizon(9, OP)), Some((9, OP)));
        assert_eq!(parse_horizon(&set(9, OP, b"")), None);
    }

    #[test]
    fn parsing_refuses_other_keys() {
        assert_eq!(parse_row(&value(1, OP, b"0123456789abcdefghijk")), None);
        assert_eq!(parse_row(&row(1, OP, 1, b"k", 2)[..20]), None);
        assert_eq!(parse_timer(&row(1, OP, 1, b"k", 2)), None);
        assert_eq!(parse_timer(&timer(OP, 1, 1, b"key")[..20]), None);
        assert_eq!(group_of(b"\x02abc"), None);
        assert_eq!(group_of(b""), None);
        assert_eq!(parse_time_value(b"short"), None);
        assert_eq!(parse_time_value(&time_value(-3)), Some(-3));
    }

    fn kinds(start: &[u8], end: &[u8]) -> Vec<SectionKind> {
        let between = sections_between(start, end);
        SectionKind::ALL
            .into_iter()
            .filter(|k| between[k.index()])
            .collect()
    }

    /// Each scan the layer makes reads one kind of section; any other scan
    /// reads every kind its range can hold.
    #[test]
    fn a_scan_reads_the_sections_its_range_can_hold() {
        use SectionKind::*;
        for w in [i64::MIN, -1, 0, i64::MAX] {
            let (s, e) = rows_due(4, OP, w);
            assert_eq!(kinds(&s, &e), [Rows]);
            let (s, e) = timers_due(OP, w);
            assert_eq!(kinds(&s, &e), [Timers]);
        }
        let slot = row_slot(4, OP, 7, b"k");
        assert_eq!(kinds(&slot, &successor(&slot)), [Rows]);
        let (s, e) = set_range(4, OP);
        assert_eq!(kinds(&s, &e), [Set]);
        let all = kinds(b"", b"\x02");
        assert_eq!(all, SectionKind::ALL);
        assert_eq!(
            kinds(&value(1, OP, b""), &value(2, OP, b"")),
            [Values, Rows, Set]
        );
        assert_eq!(
            kinds(&value(1, OP, b"a"), &successor(&value(1, OP, b""))),
            [Values]
        );
        assert_eq!(
            kinds(&horizon(1, OP), &value(1, OP, b"")),
            [Values, Rows, Set]
        );
        assert!(kinds(&value(1, OP, b"b"), &value(1, OP, b"a")).is_empty());
        // Every key a range holds is of a kind it names.
        let keys = [
            value(1, OP, b"k"),
            horizon(1, OP),
            set(1, OP, b"k"),
            row(1, OP, 3, b"k", 0),
            timer(OP, 3, 1, b"k"),
        ];
        for a in &keys {
            for b in &keys {
                let named = kinds(a, b);
                for k in keys.iter().filter(|k| a <= *k && *k < b) {
                    assert!(named.contains(&section_of(k).unwrap()), "{a:?} {b:?} {k:?}");
                }
            }
        }
    }

    #[test]
    fn filters_hold_a_rows_slot() {
        let r = row(3, OP, 9, b"key", 77);
        let slot = row_slot(3, OP, 9, b"key");
        assert_eq!(filtered(&r), &slot[..]);
        assert_eq!(filtered(&value(3, OP, b"x")), &value(3, OP, b"x")[..]);
        assert!(is_slot(&slot, &successor(&slot)));
        assert!(!is_slot(&r, &successor(&r)));
        assert!(!is_slot(&slot, &after(&slot)));
        let (s, e) = rows_due(3, OP, 9);
        assert!(!is_slot(&s, &e));
        assert_eq!(section_of(&slot), Some(SectionKind::Rows));
        assert_eq!(section_of(b"\x00ab"), None);
        assert_eq!(section_of(&timer(OP, 1, 2, b"")), Some(SectionKind::Timers));
    }

    #[test]
    fn successor_and_after() {
        assert_eq!(&successor(b"\x00\x01\xff\xff")[..], b"\x00\x02");
        let k = b"\x00ab";
        assert!(after(k).as_ref() > &k[..] && after(k) < successor(k));
        assert!(after(k) < Bytes::from_static(b"\x00ab\x00\x00"));
    }
}
