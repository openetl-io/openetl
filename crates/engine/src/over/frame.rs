//! What `OVER` keeps of a key's rows for each function, and the function's
//! value for each row. A key's rows come in event-time order, a time's
//! peers together: rows of one event time, in the order of their record
//! IDs. A `RANGE` frame holds every peer of the current row, so peers get
//! one value; a `ROWS` frame, `lag` and `first_value` over `ROWS` take them
//! one at a time.
//!
//! An unbounded aggregate keeps its accumulator, and `first_value` of an
//! unbounded frame the first row's value. A bounded frame keeps the rows it
//! may still reach: the last `n` of `ROWS n PRECEDING`, those within the
//! offset of the latest time for `RANGE`, and `lag`'s last `offset`.

use std::collections::VecDeque;

use openetl_sql::{AggregateKind, Frame, OverFunction, OverKind};

use super::row::put;
use crate::window::accumulate::{self, Acc, Spec};
use crate::window::codec::{self, Value, take};

/// A function as the operator runs it.
#[derive(Debug, Clone)]
pub struct Func {
    kind: Kind,
    frame: Option<Frame>,
    /// `count(*)`: every row counts, with no argument.
    star: bool,
}

#[derive(Debug, Clone)]
enum Kind {
    Agg(Spec),
    First,
    Lag(u64),
}

/// A row a frame keeps: its event time, its argument's value, and whether
/// an aggregate's FILTER takes it.
#[derive(Clone, Debug, PartialEq)]
pub struct Entry {
    time: i64,
    value: Option<Value>,
    counts: bool,
}

/// What a function keeps of one key's rows.
#[derive(Clone, Debug, PartialEq)]
pub enum Kept {
    Acc(Acc),
    /// The frame's first row's value, once a row has come.
    First(Option<Option<Value>>),
    /// The rows it may still reach, oldest first.
    Recent(VecDeque<Entry>),
}

const ACC: u8 = 1;
const FIRST: u8 = 2;
const RECENT: u8 = 3;

/// A row of a key's peers: its argument's value, and whether it counts.
pub type Peer = (Option<Value>, bool);

/// A function's value for a row; `Err` where it doesn't fit its type.
pub type Got = Result<Option<Value>, String>;

impl Func {
    pub fn of(f: &OverFunction) -> Self {
        let agg = |kind| {
            Kind::Agg(Spec {
                kind,
                arg: f.arg.as_ref().map(|e| e.data_type().clone()),
                out: f.data_type.clone(),
            })
        };
        let kind = match &f.kind {
            OverKind::Count => agg(AggregateKind::Count),
            OverKind::Sum => agg(AggregateKind::Sum),
            OverKind::Min => agg(AggregateKind::Min),
            OverKind::Max => agg(AggregateKind::Max),
            OverKind::Avg => agg(AggregateKind::Avg),
            OverKind::FirstValue => Kind::First,
            OverKind::Lag { offset, .. } => Kind::Lag(*offset),
        };
        Self {
            kind,
            frame: f.frame,
            star: f.arg.is_none(),
        }
    }

    fn unbounded(&self) -> bool {
        matches!(
            self.frame,
            Some(Frame::Rows { preceding: None } | Frame::Range { preceding: None })
        )
    }

    /// What it keeps of a key before its first row.
    pub fn start(&self) -> Kept {
        match &self.kind {
            Kind::Agg(spec) if self.unbounded() => Kept::Acc(Acc::new(spec)),
            Kind::First if self.unbounded() => Kept::First(None),
            _ => Kept::Recent(VecDeque::new()),
        }
    }

    /// Row `r`'s value of its argument, `a`, as the function takes it:
    /// `count`'s is whether the row has one, of whatever type, and the
    /// others' the value, of the types they take.
    pub fn value(&self, a: &dyn arrow_array::Array, r: usize) -> Option<Value> {
        match &self.kind {
            Kind::Agg(spec) if spec.kind == AggregateKind::Count => accumulate::present(a, r),
            _ => Value::read(a, r),
        }
    }

    fn arg(&self, v: &Option<Value>) -> Option<Option<Value>> {
        match self.star {
            true => None,
            false => Some(v.clone()),
        }
    }

    /// The value of each of a key's peers, rows at time `t` in their
    /// order, which `kept` then holds: `default` is `lag`'s where there's
    /// no row that far back.
    pub fn peers(
        &self,
        kept: &mut Kept,
        t: i64,
        rows: &[Peer],
        default: Option<&Value>,
    ) -> Vec<Got> {
        let entry = |(v, c): &Peer| Entry {
            time: t,
            value: v.clone(),
            counts: *c,
        };
        let over = |spec: &Spec, recent: &VecDeque<Entry>| {
            let mut acc = Acc::new(spec);
            for e in recent.iter().filter(|e| e.counts) {
                acc.take(self.arg(&e.value));
            }
            acc.value(spec)
        };
        let n = rows.len();
        match (&self.kind, self.frame, kept) {
            (Kind::Agg(spec), Some(Frame::Range { preceding: None }), Kept::Acc(acc)) => {
                for (v, _) in rows.iter().filter(|(_, c)| *c) {
                    acc.take(self.arg(v));
                }
                vec![acc.value(spec); n]
            }
            (Kind::Agg(spec), Some(Frame::Rows { preceding: None }), Kept::Acc(acc)) => rows
                .iter()
                .map(|(v, c)| {
                    if *c {
                        acc.take(self.arg(v));
                    }
                    acc.value(spec)
                })
                .collect(),
            (Kind::Agg(spec), Some(Frame::Rows { preceding: Some(p) }), Kept::Recent(recent)) => {
                rows.iter()
                    .map(|row| {
                        recent.push_back(entry(row));
                        let got = over(spec, recent);
                        keep_last(recent, p);
                        got
                    })
                    .collect()
            }
            (Kind::Agg(spec), Some(Frame::Range { preceding: Some(p) }), Kept::Recent(recent)) => {
                recent.extend(rows.iter().map(entry));
                keep_since(recent, t.saturating_sub(p));
                vec![over(spec, recent); n]
            }
            (Kind::First, _, Kept::First(first)) => {
                let v = first.get_or_insert_with(|| rows.first().and_then(|r| r.0.clone()));
                vec![Ok(v.clone()); n]
            }
            (Kind::First, Some(Frame::Rows { preceding: Some(p) }), Kept::Recent(recent)) => rows
                .iter()
                .map(|row| {
                    recent.push_back(entry(row));
                    let got = recent.front().and_then(|e| e.value.clone());
                    keep_last(recent, p);
                    Ok(got)
                })
                .collect(),
            (Kind::First, Some(Frame::Range { preceding: Some(p) }), Kept::Recent(recent)) => {
                recent.extend(rows.iter().map(entry));
                keep_since(recent, t.saturating_sub(p));
                let got = recent.front().and_then(|e| e.value.clone());
                vec![Ok(got); n]
            }
            (Kind::Lag(k), None, Kept::Recent(recent)) => rows
                .iter()
                .map(|row| {
                    let back = usize::try_from(*k)
                        .ok()
                        .and_then(|k| recent.len().checked_sub(k));
                    let got = match back {
                        Some(i) => recent.get(i).and_then(|e| e.value.clone()),
                        None => default.cloned(),
                    };
                    recent.push_back(entry(row));
                    keep_last(recent, *k);
                    Ok(got)
                })
                .collect(),
            _ => vec![Err("a function whose state isn't of its frame".to_owned()); n],
        }
    }
}

/// Keeps the last `n` rows, those a `ROWS n PRECEDING` frame or `lag(…, n)`
/// of the next row reaches.
fn keep_last(recent: &mut VecDeque<Entry>, n: u64) {
    while u64::try_from(recent.len()).is_ok_and(|l| l > n) {
        recent.pop_front();
    }
}

/// Keeps the rows at or after `from`, which a later row's `RANGE` frame
/// may reach.
fn keep_since(recent: &mut VecDeque<Entry>, from: i64) {
    while recent.front().is_some_and(|e| e.time < from) {
        recent.pop_front();
    }
}

impl Kept {
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Kept::Acc(a) => {
                out.push(ACC);
                a.encode(out);
            }
            Kept::First(f) => {
                out.push(FIRST);
                match f {
                    None => out.push(0),
                    Some(v) => {
                        out.push(1);
                        put(v.as_ref(), out);
                    }
                }
            }
            Kept::Recent(recent) => {
                out.push(RECENT);
                out.extend_from_slice(&codec::len32(recent.len()).to_be_bytes());
                for e in recent {
                    out.extend_from_slice(&e.time.to_be_bytes());
                    out.push(u8::from(e.counts));
                    put(e.value.as_ref(), out);
                }
            }
        }
    }

    pub fn decode(buf: &mut &[u8]) -> Result<Kept, String> {
        let [kind] = take::<1>(buf)?;
        Ok(match kind {
            ACC => Kept::Acc(Acc::decode(buf)?),
            FIRST => match take::<1>(buf)? {
                [0] => Kept::First(None),
                _ => Kept::First(Some(codec::decode(buf)?)),
            },
            RECENT => {
                let n = u32::from_be_bytes(take(buf)?);
                let mut recent = VecDeque::new();
                for _ in 0..n {
                    let time = i64::from_be_bytes(take(buf)?);
                    let [counts] = take::<1>(buf)?;
                    recent.push_back(Entry {
                        time,
                        counts: counts == 1,
                        value: codec::decode(buf)?,
                    });
                }
                Kept::Recent(recent)
            }
            other => return Err(format!("a frame's state of kind {other}")),
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    fn func(kind: Kind, frame: Option<Frame>) -> Func {
        Func {
            kind,
            frame,
            star: false,
        }
    }

    fn sum() -> Kind {
        Kind::Agg(Spec {
            kind: AggregateKind::Sum,
            arg: Some(DataType::Int64),
            out: DataType::Int64,
        })
    }

    fn ints(vs: &[i64]) -> Vec<Peer> {
        vs.iter().map(|v| (Some(Value::Int(*v)), true)).collect()
    }

    fn got(vs: Vec<Got>) -> Vec<Option<i64>> {
        vs.into_iter()
            .map(|v| match v.unwrap() {
                Some(Value::Int(i)) => Some(i),
                None => None,
                other => panic!("{other:?}"),
            })
            .collect()
    }

    /// Runs `f` over `(time, values)` peer groups in turn, writing and
    /// reading its state back between them.
    fn run(f: &Func, groups: &[(i64, &[i64])], default: Option<i64>) -> Vec<Option<i64>> {
        let mut kept = f.start();
        let mut out = Vec::new();
        let default = default.map(Value::Int);
        for (t, vs) in groups {
            out.extend(got(f.peers(&mut kept, *t, &ints(vs), default.as_ref())));
            let mut b = Vec::new();
            kept.encode(&mut b);
            let mut at = b.as_slice();
            assert_eq!(Kept::decode(&mut at).unwrap(), kept);
            assert!(at.is_empty());
        }
        out
    }

    /// RANGE takes every peer, so peers share a value; ROWS takes them one
    /// at a time.
    #[test]
    fn range_takes_peers_together_and_rows_one_at_a_time() {
        let groups: &[(i64, &[i64])] = &[(1, &[1, 2]), (2, &[4]), (5, &[8, 16])];
        let range = func(sum(), Some(Frame::Range { preceding: None }));
        assert_eq!(run(&range, groups, None), [3, 3, 7, 31, 31].map(Some));
        let rows = func(sum(), Some(Frame::Rows { preceding: None }));
        assert_eq!(run(&rows, groups, None), [1, 3, 7, 15, 31].map(Some));
        let two = func(sum(), Some(Frame::Rows { preceding: Some(1) }));
        assert_eq!(run(&two, groups, None), [1, 3, 6, 12, 24].map(Some));
        // Times 2 to 5, and 4 to 5: the offset is of event time.
        let within = func(sum(), Some(Frame::Range { preceding: Some(3) }));
        assert_eq!(run(&within, groups, None), [3, 3, 7, 28, 28].map(Some));
        let one = func(sum(), Some(Frame::Range { preceding: Some(1) }));
        assert_eq!(run(&one, groups, None), [3, 3, 7, 24, 24].map(Some));
    }

    #[test]
    fn lag_and_first_value_take_rows_in_order() {
        let groups: &[(i64, &[i64])] = &[(1, &[1, 2]), (2, &[4]), (5, &[8, 16])];
        let lag = func(Kind::Lag(2), None);
        assert_eq!(
            run(&lag, groups, Some(-1)),
            [Some(-1), Some(-1), Some(1), Some(2), Some(4)]
        );
        let first = func(Kind::First, Some(Frame::Range { preceding: None }));
        assert_eq!(run(&first, groups, None), [1, 1, 1, 1, 1].map(Some));
        let rows = func(Kind::First, Some(Frame::Rows { preceding: Some(1) }));
        assert_eq!(run(&rows, groups, None), [1, 1, 2, 4, 8].map(Some));
        let range = func(Kind::First, Some(Frame::Range { preceding: Some(3) }));
        assert_eq!(run(&range, groups, None), [1, 1, 1, 4, 4].map(Some));
    }

    /// count of an argument counts a row where it isn't null, of any type,
    /// as a list; the others take its value.
    #[test]
    fn count_takes_whether_a_row_has_its_argument() {
        let mut b =
            arrow_array::builder::ListBuilder::new(arrow_array::builder::Int64Builder::new());
        b.append(true);
        b.append_null();
        let lists = b.finish();
        let count = Func {
            kind: Kind::Agg(Spec {
                kind: AggregateKind::Count,
                arg: Some(arrow_array::Array::data_type(&lists).clone()),
                out: DataType::Int64,
            }),
            frame: Some(Frame::Range { preceding: None }),
            star: false,
        };
        assert_eq!(count.value(&lists, 0), Some(Value::Bool(true)));
        assert_eq!(count.value(&lists, 1), None);
        let mut kept = count.start();
        let rows: Vec<Peer> = (0..2).map(|r| (count.value(&lists, r), true)).collect();
        assert_eq!(
            got(count.peers(&mut kept, 1, &rows, None)),
            [Some(1), Some(1)]
        );
        let ints = arrow_array::Int64Array::from(vec![Some(7), None]);
        let sum = func(sum(), Some(Frame::Rows { preceding: None }));
        assert_eq!(sum.value(&ints, 0), Some(Value::Int(7)));
        assert_eq!(sum.value(&ints, 1), None);
    }

    /// A row its FILTER leaves out takes a ROWS frame's place, and adds
    /// nothing.
    #[test]
    fn a_filtered_row_holds_its_place_in_a_frame() {
        let f = Func {
            kind: Kind::Agg(Spec {
                kind: AggregateKind::Count,
                arg: None,
                out: DataType::Int64,
            }),
            frame: Some(Frame::Rows { preceding: Some(1) }),
            star: true,
        };
        let mut kept = f.start();
        let rows: Vec<Peer> = vec![(None, true), (None, false), (None, true), (None, true)];
        assert_eq!(
            got(f.peers(&mut kept, 1, &rows, None)),
            [1, 1, 1, 2].map(Some)
        );
    }
}
