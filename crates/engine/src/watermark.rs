//! Watermarks: how far a stream's event time has got. Times are
//! microseconds since the Unix epoch.
//!
//! A split's watermark is the latest event time it has read less the
//! stream's delay, or what the runner's Split frames raise it to, and never
//! goes back. A lane's is the least of its splits' ([`Splits`]). Whatever
//! takes a stream from several senders, the lanes before an exchange in this
//! engine or another's, takes the least of theirs, every sender counting
//! from the start whether it has sent yet or not ([`Senders`]). Watermarks
//! travel in band, so what a lane has at a barrier is what the record keeps,
//! and a restore starts every split from it.

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, TimeUnit};
use openetl_proto::engine::v1::EventTime as EventTimePlan;

/// Before any time: no watermark yet.
pub const NONE: i64 = i64::MIN;
/// After every time: the end of input.
pub const END: i64 = i64::MAX;

const MICROS_PER_DAY: i64 = 86_400_000_000;

/// How a stream's watermarks are made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventTime {
    /// Under source names.
    pub column: String,
    pub delay: i64,
    /// Its rows aren't read in time order: only Split frames move a split.
    pub held: bool,
    /// Where a receiver of the stream's outputs starts.
    pub floor: i64,
}

impl EventTime {
    pub fn of(p: &EventTimePlan) -> Result<Self, String> {
        if p.column.is_empty() {
            return Err("event time names no column".into());
        }
        if p.delay_micros < 0 {
            return Err(format!("a watermark delay of {}µs", p.delay_micros));
        }
        Ok(Self {
            column: p.column.clone(),
            delay: p.delay_micros,
            held: p.held,
            floor: p.floor.unwrap_or(NONE),
        })
    }
}

/// The latest time in a batch's column named `column`, matched without
/// regard to case where it isn't there exactly; None if the batch has no
/// such column or only nulls in it.
pub fn latest(batch: &RecordBatch, column: &str) -> Result<Option<i64>, String> {
    let schema = batch.schema();
    let i = match schema.index_of(column) {
        Ok(i) => i,
        Err(_) => match schema
            .fields()
            .iter()
            .position(|f| f.name().eq_ignore_ascii_case(column))
        {
            Some(i) => i,
            None => return Ok(None),
        },
    };
    let a = batch.column(i);
    let max = |values: &mut dyn Iterator<Item = Option<i64>>| values.flatten().max();
    let micros = match a.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => {
            max(&mut a.as_primitive::<TimestampSecondType>().iter())
                .map(|v| v.saturating_mul(1_000_000))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            max(&mut a.as_primitive::<TimestampMillisecondType>().iter())
                .map(|v| v.saturating_mul(1_000))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            max(&mut a.as_primitive::<TimestampMicrosecondType>().iter())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            max(&mut a.as_primitive::<TimestampNanosecondType>().iter())
                .map(|v| v.div_euclid(1_000))
        }
        DataType::Date32 => max(&mut a
            .as_primitive::<Date32Type>()
            .iter()
            .map(|v| v.map(i64::from)))
        .map(|v| v.saturating_mul(MICROS_PER_DAY)),
        DataType::Date64 => {
            max(&mut a.as_primitive::<Date64Type>().iter()).map(|v| v.saturating_mul(1_000))
        }
        _ if a.null_count() == a.len() => None,
        t => {
            return Err(format!(
                "its event time column {} is {t}, not a timestamp or a date",
                schema.field(i).name()
            ));
        }
    };
    Ok(micros)
}

/// One lane's splits of a stream, and the lane's watermark of it.
#[derive(Debug, Default)]
pub struct Splits {
    /// In the order they joined.
    splits: Vec<(String, i64)>,
    /// The split the next batch is of.
    current: Option<usize>,
    /// The lane's watermark as last given; None before it has one.
    sent: Option<i64>,
    /// The lane has passed a barrier, so a lane without splits has none to
    /// wait for.
    settled: bool,
    /// Splits joined since the lane's watermark was last worked out.
    joined: bool,
}

impl Splits {
    /// The split the batches after it are of is `id`, whose watermark is at
    /// least `at`. A split new to the lane joins it at no less than the
    /// lane's watermark, so the lane's never goes back, and the lane's is
    /// worked out again only at what comes next of the stream but another
    /// split's joining: the splits a read starts with join together. Returns
    /// the lane's watermark if it advanced.
    pub fn split(&mut self, id: &str, at: Option<i64>) -> Option<i64> {
        let at = at.unwrap_or(NONE);
        match self.splits.iter().position(|(s, _)| s == id) {
            Some(i) => {
                self.current = Some(i);
                let wm = &mut self.splits[i].1;
                *wm = (*wm).max(at);
                self.advance()
            }
            None => {
                let wm = at.max(self.sent.unwrap_or(NONE));
                self.splits.push((id.to_owned(), wm));
                self.current = Some(self.splits.len() - 1);
                self.joined = true;
                None
            }
        }
    }

    /// Before a batch: the lane's watermark, if splits that joined since it
    /// was last worked out advance it.
    pub fn settle(&mut self) -> Option<i64> {
        match self.joined {
            true => self.advance(),
            false => None,
        }
    }

    /// The current split read rows whose latest time is `latest`, of a
    /// stream whose watermark trails it by `delay`. Returns the lane's
    /// watermark if it advanced. A batch before any Split frame is of no
    /// split the lane knows, and moves nothing.
    pub fn read(&mut self, latest: i64, delay: i64) -> Option<i64> {
        let i = self.current?;
        let wm = &mut self.splits[i].1;
        *wm = (*wm).max(latest.saturating_sub(delay));
        self.advance()
    }

    /// At each of the stream's barriers on the lane. From the first one, a
    /// lane without splits holds none back. Returns the lane's watermark if
    /// it advanced.
    pub fn barrier(&mut self) -> Option<i64> {
        self.settled = true;
        self.advance()
    }

    /// Each split's watermark, in the order they joined.
    pub fn marks(&self) -> impl Iterator<Item = (&str, i64)> {
        self.splits.iter().map(|(s, wm)| (s.as_str(), *wm))
    }

    /// The lane's watermark; None before it has one.
    #[cfg(test)]
    pub fn watermark(&self) -> Option<i64> {
        self.sent
    }

    fn advance(&mut self) -> Option<i64> {
        self.joined = false;
        let wm = match self.splits.iter().map(|(_, wm)| *wm).min() {
            Some(wm) => wm,
            None if self.settled => END,
            None => return None,
        };
        if wm == NONE || self.sent.is_some_and(|s| s >= wm) {
            return None;
        }
        self.sent = Some(wm);
        Some(wm)
    }
}

/// A sender's watermark went back.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("sender {sender}'s watermark went back from {from} to {to}")]
pub struct WentBack {
    pub sender: usize,
    pub from: i64,
    pub to: i64,
}

/// What takes a stream from several senders: their watermarks, and its own,
/// the least of theirs, never below where it started. A sender that hasn't
/// sent one holds it at that.
#[derive(Debug, Clone)]
pub struct Senders {
    marks: Vec<i64>,
    current: i64,
}

impl Senders {
    /// `senders` of them, every one the assignment names, starting at
    /// `floor`.
    pub fn new(senders: usize, floor: i64) -> Self {
        Self {
            marks: vec![NONE; senders],
            current: floor,
        }
    }

    /// Sender `i`'s watermark is `wm`. Returns the receiver's if it
    /// advanced.
    pub fn take(&mut self, i: usize, wm: i64) -> Result<Option<i64>, WentBack> {
        let from = self.marks[i];
        if wm < from {
            return Err(WentBack {
                sender: i,
                from,
                to: wm,
            });
        }
        self.marks[i] = wm;
        let least = self.marks.iter().copied().min().unwrap_or(END);
        if least <= self.current {
            return Ok(None);
        }
        self.current = least;
        Ok(Some(least))
    }

    /// The receiver's watermark: NONE before it has one.
    pub fn watermark(&self) -> i64 {
        self.current
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        Date32Array, Int64Array, StringArray, TimestampMicrosecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    use arrow_schema::{Field, Schema};

    use super::*;

    /// SplitMix64.
    pub(crate) struct Rng(pub u64);

    impl Rng {
        pub fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        }

        pub fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    fn batch(name: &str, a: Arc<dyn Array>) -> RecordBatch {
        let s = Schema::new(vec![Field::new(name, a.data_type().clone(), true)]);
        RecordBatch::try_new(Arc::new(s), vec![a]).unwrap()
    }

    #[test]
    fn the_latest_time_is_in_microseconds_whatever_the_type() {
        let cases: Vec<(Arc<dyn Array>, Option<i64>)> = vec![
            (
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(5),
                    None,
                    Some(3),
                ])),
                Some(5),
            ),
            (
                Arc::new(TimestampSecondArray::from(vec![Some(2), Some(1)]).with_timezone("UTC")),
                Some(2_000_000),
            ),
            (
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(-1_500),
                    Some(-2_500),
                ])),
                Some(-2),
            ),
            (
                Arc::new(Date32Array::from(vec![Some(1), None])),
                Some(MICROS_PER_DAY),
            ),
            (
                Arc::new(TimestampMicrosecondArray::from(vec![None::<i64>, None])),
                None,
            ),
            (Arc::new(StringArray::from(vec![None::<&str>])), None),
        ];
        for (a, want) in cases {
            let t = a.data_type().clone();
            assert_eq!(latest(&batch("at", a), "at").unwrap(), want, "{t}");
        }
        let b = batch(
            "Created_At",
            Arc::new(TimestampMicrosecondArray::from(vec![7])),
        );
        assert_eq!(latest(&b, "created_at").unwrap(), Some(7));
        assert_eq!(latest(&b, "other").unwrap(), None);
        let e = latest(&batch("at", Arc::new(Int64Array::from(vec![1]))), "at").unwrap_err();
        assert!(e.contains("not a timestamp"), "{e}");
    }

    #[test]
    fn a_lane_is_at_the_least_of_its_splits() {
        let mut s = Splits::default();
        assert_eq!(s.split("a", Some(10)), None, "a joins");
        assert_eq!(s.settle(), Some(10));
        assert_eq!(s.split("b", None), None, "b joins at the lane's 10");
        assert_eq!(s.settle(), None);
        assert_eq!(s.read(30, 5), None, "a holds b back at 10");
        assert_eq!(s.split("a", None), None);
        assert_eq!(s.read(40, 5), Some(25));
        assert_eq!(s.split("b", None), None);
        assert_eq!(s.read(22, 5), None, "a split's never goes back");
        assert_eq!(s.marks().collect::<Vec<_>>(), vec![("a", 35), ("b", 25)]);
        assert_eq!(s.split("b", Some(END)), Some(35));
        assert_eq!(s.split("a", Some(END)), Some(END));
        assert_eq!(s.watermark(), Some(END));
    }

    #[test]
    fn a_lane_without_splits_holds_none_back_from_its_first_barrier() {
        let mut s = Splits::default();
        assert_eq!(s.read(10, 0), None, "a batch of no split moves nothing");
        assert_eq!(s.watermark(), None);
        assert_eq!(s.barrier(), Some(END));
        assert_eq!(s.barrier(), None);
        assert_eq!(s.split("late", Some(3)), None, "it joins at the lane's");
        assert_eq!(s.settle(), None);
        assert_eq!(s.marks().collect::<Vec<_>>(), vec![("late", END)]);
    }

    #[test]
    fn the_splits_a_read_starts_with_join_together() {
        let mut s = Splits::default();
        assert_eq!(s.split("a", Some(80)), None);
        assert_eq!(s.split("b", Some(70)), None);
        assert_eq!(s.settle(), Some(70));
        assert_eq!(s.marks().collect::<Vec<_>>(), vec![("a", 80), ("b", 70)]);
    }

    #[test]
    fn a_split_without_a_watermark_holds_the_lane_back() {
        let mut s = Splits::default();
        assert_eq!(s.split("a", None), None, "no watermark yet");
        assert_eq!(s.split("b", Some(10)), None, "a holds it");
        assert_eq!(s.barrier(), None);
        assert_eq!(s.read(30, 0), None);
        assert_eq!(s.split("a", None), None);
        assert_eq!(s.read(100, 10), Some(30));
    }

    #[test]
    fn a_receiver_waits_for_every_sender_from_its_floor() {
        let mut r = Senders::new(3, 50);
        assert_eq!(r.watermark(), 50);
        assert_eq!(r.take(0, 100), Ok(None));
        assert_eq!(r.take(1, 40), Ok(None), "below the floor");
        assert_eq!(r.take(2, 70), Ok(None), "sender 1 holds it");
        assert_eq!(r.take(1, 80), Ok(Some(70)));
        assert_eq!(r.take(2, 200), Ok(Some(80)));
        assert_eq!(
            r.take(1, 60),
            Err(WentBack {
                sender: 1,
                from: 80,
                to: 60
            })
        );
        assert_eq!(r.take(1, END), Ok(Some(100)));
        assert_eq!(r.take(0, END), Ok(Some(200)));
        assert_eq!(r.take(2, END), Ok(Some(END)));
    }

    /// Splits spread over lanes, each lane's rows and Split frames in an
    /// order of their own, its watermark sent to a receiver: under any
    /// interleaving, a split's, a lane's and the receiver's never go back;
    /// each lane is the least of its splits; the receiver is the least of
    /// the lanes, once each has sent; and every split's rows are at or
    /// after its own watermark, so none is late at the receiver, which ends
    /// at END once every split's input does.
    #[test]
    fn watermarks_hold_under_any_interleaving() {
        for seed in 0..500 {
            let mut rng = Rng(seed);
            let lanes = 1 + rng.below(4);
            let splits = 1 + rng.below(6);
            let delay = rng.below(4) as i64;
            // Each split's times rise, but within the delay of the latest.
            let rows: Vec<Vec<i64>> = (0..splits)
                .map(|_| {
                    let mut top = rng.below(50) as i64;
                    (0..rng.below(20))
                        .map(|_| {
                            top += rng.below(5) as i64;
                            top - rng.below(delay as usize + 1) as i64
                        })
                        .collect()
                })
                .collect();
            let lane_of: Vec<usize> = (0..splits).map(|s| s % lanes).collect();
            let mut at = vec![0; splits];
            let mut marks: Vec<Splits> = (0..lanes).map(|_| Splits::default()).collect();
            let mut recv = Senders::new(lanes, NONE);
            let mut prev_split = vec![NONE; splits];
            let mut prev_lane = vec![NONE; lanes];
            let mut prev_recv = NONE;
            for (s, l) in lane_of.iter().enumerate() {
                assert_eq!(marks[*l].split(&s.to_string(), None), None);
            }
            // At the first barrier, lanes without splits hold none back.
            for (l, m) in marks.iter_mut().enumerate() {
                if let Some(wm) = m.barrier() {
                    assert_eq!(wm, END);
                    prev_lane[l] = wm;
                    recv.take(l, wm).unwrap();
                }
            }
            let mut ended = vec![false; splits];
            while ended.iter().any(|e| !e) {
                let s = rng.below(splits);
                if ended[s] {
                    continue;
                }
                let l = lane_of[s];
                let m = &mut marks[l];
                let sent = match at[s] < rows[s].len() {
                    true => {
                        let t = rows[s][at[s]];
                        at[s] += 1;
                        assert!(
                            t >= recv.watermark() || recv.watermark() == NONE,
                            "seed {seed}: split {s}'s row at {t} is late at {}",
                            recv.watermark()
                        );
                        m.split(&s.to_string(), None);
                        m.read(t, delay)
                    }
                    false => {
                        ended[s] = true;
                        m.split(&s.to_string(), Some(END))
                    }
                };
                for (id, wm) in m.marks() {
                    let id: usize = id.parse().unwrap();
                    assert!(wm >= prev_split[id], "seed {seed}: split {id} went back");
                    prev_split[id] = wm;
                }
                let least = m.marks().map(|(_, wm)| wm).min().unwrap();
                if let Some(wm) = sent {
                    assert!(wm > prev_lane[l], "seed {seed}: lane {l} went back");
                    assert_eq!(wm, least, "seed {seed}: lane {l} isn't its least");
                    prev_lane[l] = wm;
                    if let Some(r) = recv.take(l, wm).unwrap() {
                        assert!(r > prev_recv, "seed {seed}: the receiver went back");
                        prev_recv = r;
                    }
                }
                let lanes_least = marks
                    .iter()
                    .map(|m| m.watermark().unwrap_or(NONE))
                    .min()
                    .unwrap();
                assert_eq!(recv.watermark(), lanes_least, "seed {seed}");
            }
            assert_eq!(recv.watermark(), END, "seed {seed}");
        }
    }
}
