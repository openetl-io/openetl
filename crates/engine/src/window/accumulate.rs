//! A group window's accumulators: ours, so their state is read at a
//! barrier without resetting it, and kept in a layout of ours. Each gives
//! the same value for the same rows in any order, so windows don't depend
//! on how rows interleave: float sums are exact until rounded once, and
//! distinct counts and percentiles keep what they've seen, not an order.

use std::collections::BTreeMap;

use arrow_array::{Array, BooleanArray};
use arrow_schema::DataType;
use openetl_sql::AggregateKind;
use twox_hash::XxHash3_64;

use super::codec::{self, Value, take};

/// What an aggregate is, as its accumulator needs it.
#[derive(Clone, Debug)]
pub struct Spec {
    pub kind: AggregateKind,
    /// Its argument's type; `None` for `count(*)`.
    pub arg: Option<DataType>,
    /// Its value's.
    pub out: DataType,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Acc {
    Count(i64),
    /// An integer's or a decimal's sum, exact, and how many values it has.
    Exact {
        sum: i128,
        n: i64,
    },
    Float {
        sum: Fsum,
        n: i64,
    },
    Min(Option<Value>),
    Max(Option<Value>),
    Distinct(Distinct),
    /// Each value and how many times it came, by its order.
    Values(BTreeMap<Ordered, u64>),
}

/// A value of a percentile's, in an order that sorts as the values do.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Ordered {
    Float(u64),
    Decimal(i128),
}

impl Ordered {
    fn of_float(f: f64) -> Self {
        let b = codec::canonical(f).to_bits();
        Ordered::Float(match b >> 63 {
            1 => !b,
            _ => b | (1 << 63),
        })
    }

    fn float(self) -> f64 {
        match self {
            Ordered::Float(b) => f64::from_bits(match b >> 63 {
                1 => b & !(1 << 63),
                _ => !b,
            }),
            Ordered::Decimal(d) => d as f64,
        }
    }
}

const COUNT: u8 = 1;
const EXACT: u8 = 2;
const FLOAT: u8 = 3;
const MIN: u8 = 4;
const MAX: u8 = 5;
const DISTINCT: u8 = 6;
const VALUES: u8 = 7;

impl Acc {
    pub fn new(spec: &Spec) -> Acc {
        let float = matches!(spec.arg, Some(DataType::Float32 | DataType::Float64));
        match spec.kind {
            AggregateKind::Count => Acc::Count(0),
            AggregateKind::Sum | AggregateKind::Avg if float => Acc::Float {
                sum: Fsum::default(),
                n: 0,
            },
            AggregateKind::Sum | AggregateKind::Avg => Acc::Exact { sum: 0, n: 0 },
            AggregateKind::Min => Acc::Min(None),
            AggregateKind::Max => Acc::Max(None),
            AggregateKind::ApproxDistinct => Acc::Distinct(Distinct::default()),
            AggregateKind::Percentile(_) => Acc::Values(BTreeMap::new()),
        }
    }

    /// Takes row `i`: of `arg`, the argument's values, unless `count(*)`.
    pub fn update(&mut self, arg: Option<&dyn Array>, i: usize) {
        let v = arg.map(|a| Value::read(a, i));
        match (self, v) {
            (Acc::Count(n), None) => *n += 1,
            (_, None | Some(None)) => {}
            (Acc::Count(n), Some(Some(_))) => *n += 1,
            (Acc::Exact { sum, n }, Some(Some(v))) => {
                match v {
                    Value::Int(i) => *sum = sum.saturating_add(i128::from(i)),
                    Value::Decimal(d) => *sum = sum.saturating_add(d),
                    _ => return,
                }
                *n += 1;
            }
            (Acc::Float { sum, n }, Some(Some(Value::Float(f)))) => {
                sum.add(f);
                *n += 1;
            }
            (Acc::Min(m), Some(Some(v))) => {
                if m.as_ref().is_none_or(|m| v.cmp(m).is_lt()) {
                    *m = Some(v);
                }
            }
            (Acc::Max(m), Some(Some(v))) => {
                if m.as_ref().is_none_or(|m| v.cmp(m).is_gt()) {
                    *m = Some(v);
                }
            }
            (Acc::Distinct(d), Some(Some(v))) => d.insert(&v),
            (Acc::Values(m), Some(Some(v))) => {
                let k = match v {
                    Value::Float(f) => Ordered::of_float(f),
                    Value::Decimal(d) => Ordered::Decimal(d),
                    _ => return,
                };
                *m.entry(k).or_default() += 1;
            }
            _ => {}
        }
    }

    /// Takes what `other` took, of the same aggregate.
    pub fn merge(&mut self, other: Acc) {
        match (self, other) {
            (Acc::Count(a), Acc::Count(b)) => *a += b,
            (Acc::Exact { sum, n }, Acc::Exact { sum: s, n: m }) => {
                *sum = sum.saturating_add(s);
                *n += m;
            }
            (Acc::Float { sum, n }, Acc::Float { sum: s, n: m }) => {
                sum.merge(&s);
                *n += m;
            }
            (Acc::Min(a), Acc::Min(Some(b))) => {
                if a.as_ref().is_none_or(|a| b.cmp(a).is_lt()) {
                    *a = Some(b);
                }
            }
            (Acc::Max(a), Acc::Max(Some(b))) => {
                if a.as_ref().is_none_or(|a| b.cmp(a).is_gt()) {
                    *a = Some(b);
                }
            }
            (Acc::Distinct(a), Acc::Distinct(b)) => a.merge(&b),
            (Acc::Values(a), Acc::Values(b)) => {
                for (k, c) in b {
                    *a.entry(k).or_default() += c;
                }
            }
            _ => {}
        }
    }

    /// Its value, of the aggregate's type; `Err` where it doesn't fit it.
    pub fn value(&self, spec: &Spec) -> Result<Option<Value>, String> {
        Ok(match self {
            Acc::Count(n) => Some(Value::Int(*n)),
            Acc::Exact { n: 0, .. } | Acc::Float { n: 0, .. } => None,
            Acc::Exact { sum, n } => match (&spec.kind, &spec.out) {
                (AggregateKind::Sum, DataType::Int64) => {
                    Some(Value::Int(i64::try_from(*sum).map_err(|_| {
                        "sum overflows BIGINT: CAST its argument AS DECIMAL(38, 0)".to_owned()
                    })?))
                }
                (AggregateKind::Sum, DataType::Decimal128(p, _)) => {
                    fits(*sum, *p)?;
                    Some(Value::Decimal(*sum))
                }
                (AggregateKind::Avg, DataType::Decimal128(p, s)) => {
                    let from = match &spec.arg {
                        Some(DataType::Decimal128(_, s)) => *s,
                        _ => *s,
                    };
                    let up = u32::try_from(i32::from(*s) - i32::from(from)).unwrap_or(0);
                    let scaled = 10i128
                        .checked_pow(up)
                        .and_then(|m| sum.checked_mul(m))
                        .ok_or_else(|| format!("avg overflows DECIMAL({p}, {s})"))?;
                    let avg = divide(scaled, i128::from(*n));
                    fits(avg, *p)?;
                    Some(Value::Decimal(avg))
                }
                (AggregateKind::Avg, DataType::Float64) => {
                    Some(Value::Float(*sum as f64 / *n as f64))
                }
                (k, t) => return Err(format!("{} of {t}", k.name())),
            },
            Acc::Float { sum, n } => match spec.kind {
                AggregateKind::Avg => Some(Value::Float(sum.total() / *n as f64)),
                _ => Some(Value::Float(sum.total())),
            },
            Acc::Min(m) | Acc::Max(m) => m.clone(),
            Acc::Distinct(d) => Some(Value::Int(i64::try_from(d.estimate()).unwrap_or(i64::MAX))),
            Acc::Values(m) => {
                let AggregateKind::Percentile(p) = spec.kind else {
                    return Err("values kept for an aggregate that isn't a percentile".into());
                };
                percentile(m, p, &spec.out)?
            }
        })
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Acc::Count(n) => {
                out.push(COUNT);
                out.extend_from_slice(&n.to_be_bytes());
            }
            Acc::Exact { sum, n } => {
                out.push(EXACT);
                out.extend_from_slice(&sum.to_be_bytes());
                out.extend_from_slice(&n.to_be_bytes());
            }
            Acc::Float { sum, n } => {
                out.push(FLOAT);
                sum.encode(out);
                out.extend_from_slice(&n.to_be_bytes());
            }
            Acc::Min(m) => {
                out.push(MIN);
                codec::encode(m.as_ref(), out);
            }
            Acc::Max(m) => {
                out.push(MAX);
                codec::encode(m.as_ref(), out);
            }
            Acc::Distinct(d) => {
                out.push(DISTINCT);
                d.encode(out);
            }
            Acc::Values(m) => {
                out.push(VALUES);
                out.extend_from_slice(&codec::len32(m.len()).to_be_bytes());
                for (k, c) in m {
                    match k {
                        Ordered::Float(b) => {
                            out.push(0);
                            out.extend_from_slice(&b.to_be_bytes());
                        }
                        Ordered::Decimal(d) => {
                            out.push(1);
                            out.extend_from_slice(&d.to_be_bytes());
                        }
                    }
                    out.extend_from_slice(&c.to_be_bytes());
                }
            }
        }
    }

    pub fn decode(buf: &mut &[u8]) -> Result<Acc, String> {
        let [kind] = take::<1>(buf)?;
        Ok(match kind {
            COUNT => Acc::Count(i64::from_be_bytes(take(buf)?)),
            EXACT => Acc::Exact {
                sum: i128::from_be_bytes(take(buf)?),
                n: i64::from_be_bytes(take(buf)?),
            },
            FLOAT => Acc::Float {
                sum: Fsum::decode(buf)?,
                n: i64::from_be_bytes(take(buf)?),
            },
            MIN => Acc::Min(codec::decode(buf)?),
            MAX => Acc::Max(codec::decode(buf)?),
            DISTINCT => Acc::Distinct(Distinct::decode(buf)?),
            VALUES => {
                let n = u32::from_be_bytes(take(buf)?);
                let mut m = BTreeMap::new();
                for _ in 0..n {
                    let [t] = take::<1>(buf)?;
                    let k = match t {
                        0 => Ordered::Float(u64::from_be_bytes(take(buf)?)),
                        1 => Ordered::Decimal(i128::from_be_bytes(take(buf)?)),
                        other => return Err(format!("a percentile's value of kind {other}")),
                    };
                    m.insert(k, u64::from_be_bytes(take(buf)?));
                }
                Acc::Values(m)
            }
            other => return Err(format!("an accumulator of kind {other}")),
        })
    }
}

/// Whether row `i` counts for an aggregate with a `FILTER`: its predicate
/// is true there.
pub fn passes(filter: Option<&BooleanArray>, i: usize) -> bool {
    filter.is_none_or(|f| f.is_valid(i) && f.values().value(i))
}

fn fits(v: i128, precision: u8) -> Result<(), String> {
    match 10i128.checked_pow(u32::from(precision)) {
        Some(max) if v.unsigned_abs() < max.unsigned_abs() => Ok(()),
        Some(_) => Err(format!(
            "the value overflows its DECIMAL's {precision} digits"
        )),
        None => Ok(()),
    }
}

/// `a / b`, rounded half away from zero.
fn divide(a: i128, b: i128) -> i128 {
    let (q, r) = (a / b, a % b);
    match r.unsigned_abs() * 2 >= b.unsigned_abs() {
        true if (a < 0) != (b < 0) => q - 1,
        true => q + 1,
        false => q,
    }
}

/// The value at fraction `p` of the values in order, between the two
/// nearest as a continuous percentile is.
fn percentile(m: &BTreeMap<Ordered, u64>, p: f64, out: &DataType) -> Result<Option<Value>, String> {
    let n: u64 = m.values().sum();
    if n == 0 {
        return Ok(None);
    }
    let pos = p * (n - 1) as f64;
    let (lo, hi) = (pos.floor() as u64, pos.ceil() as u64);
    let at = |i: u64| {
        let mut seen = 0;
        for (k, c) in m {
            seen += c;
            if i < seen {
                return *k;
            }
        }
        *m.keys().next_back().unwrap_or(&Ordered::Float(0))
    };
    let (a, b) = (at(lo), at(hi));
    let frac = pos - lo as f64;
    Ok(Some(match (a, b, out) {
        (Ordered::Decimal(a), Ordered::Decimal(b), DataType::Decimal128(..)) => {
            Value::Decimal(match lo == hi {
                true => a,
                false => (a & b) + ((a ^ b) >> 1),
            })
        }
        (a, b, _) => {
            let (a, b) = (a.float(), b.float());
            Value::Float(match lo == hi {
                true => a,
                false => a * (1.0 - frac) + b * frac,
            })
        }
    }))
}

/// An exact float sum, rounded once when it's read: Shewchuk's partials,
/// as Python's `math.fsum` keeps them. Infinities and NaNs are summed
/// apart.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Fsum {
    partials: Vec<f64>,
    /// The sum of the values that aren't finite, and of the infinities.
    special: f64,
    infinities: f64,
}

impl Fsum {
    pub fn add(&mut self, x: f64) {
        if !x.is_finite() {
            if x.is_infinite() {
                self.infinities += x;
            }
            self.special += x;
            return;
        }
        let mut x = x;
        let mut i = 0;
        for j in 0..self.partials.len() {
            let mut y = self.partials[j];
            if x.abs() < y.abs() {
                std::mem::swap(&mut x, &mut y);
            }
            let hi = x + y;
            let lo = y - (hi - x);
            if lo != 0.0 {
                self.partials[i] = lo;
                i += 1;
            }
            x = hi;
        }
        self.partials.truncate(i);
        if !x.is_finite() {
            // Finite values whose partial sum overflows.
            self.infinities += x;
            self.special += x;
            return;
        }
        if x != 0.0 {
            self.partials.push(x);
        }
    }

    pub fn merge(&mut self, other: &Fsum) {
        self.special += other.special;
        self.infinities += other.infinities;
        for &p in &other.partials {
            self.add(p);
        }
    }

    /// The sum, correctly rounded.
    pub fn total(&self) -> f64 {
        if self.special != 0.0 || self.special.is_nan() {
            return match self.infinities.is_nan() {
                true => f64::NAN,
                false => self.special,
            };
        }
        let p = &self.partials;
        let mut n = p.len();
        let mut hi = 0.0;
        if n > 0 {
            n -= 1;
            hi = p[n];
            let mut lo = 0.0;
            while n > 0 {
                let x = hi;
                n -= 1;
                let y = p[n];
                hi = x + y;
                let yr = hi - x;
                lo = y - yr;
                if lo != 0.0 {
                    break;
                }
            }
            if n > 0 && ((lo < 0.0 && p[n - 1] < 0.0) || (lo > 0.0 && p[n - 1] > 0.0)) {
                let y = lo * 2.0;
                let x = hi + y;
                if y == x - hi {
                    hi = x;
                }
            }
        }
        hi
    }

    fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&codec::len32(self.partials.len()).to_be_bytes());
        for p in &self.partials {
            out.extend_from_slice(&p.to_bits().to_be_bytes());
        }
        out.extend_from_slice(&self.special.to_bits().to_be_bytes());
        out.extend_from_slice(&self.infinities.to_bits().to_be_bytes());
    }

    fn decode(buf: &mut &[u8]) -> Result<Fsum, String> {
        let n = u32::from_be_bytes(take(buf)?);
        let mut partials = Vec::new();
        for _ in 0..n {
            partials.push(f64::from_bits(u64::from_be_bytes(take(buf)?)));
        }
        Ok(Fsum {
            partials,
            special: f64::from_bits(u64::from_be_bytes(take(buf)?)),
            infinities: f64::from_bits(u64::from_be_bytes(take(buf)?)),
        })
    }
}

/// How many distinct values have come: their hashes, exactly, up to
/// [`EXACT_UP_TO`], and past that a HyperLogLog of 2^[`PRECISION`]
/// registers, about 1.6% off. What it holds depends only on the values,
/// not their order.
#[derive(Clone, Debug, PartialEq)]
pub enum Distinct {
    /// The hashes, sorted.
    Exact(Vec<u64>),
    Registers(Box<[u8]>),
}

const EXACT_UP_TO: usize = 256;
const PRECISION: u32 = 12;
const REGISTERS: usize = 1 << PRECISION;

impl Default for Distinct {
    fn default() -> Self {
        Distinct::Exact(Vec::new())
    }
}

impl Distinct {
    fn insert(&mut self, v: &Value) {
        let mut b = Vec::new();
        codec::encode(Some(v), &mut b);
        self.hash(XxHash3_64::oneshot(&b));
    }

    fn hash(&mut self, h: u64) {
        match self {
            Distinct::Exact(hs) => {
                if let Err(at) = hs.binary_search(&h) {
                    hs.insert(at, h);
                }
                if hs.len() > EXACT_UP_TO {
                    let mut r = vec![0u8; REGISTERS].into_boxed_slice();
                    for &h in hs.iter() {
                        register(&mut r, h);
                    }
                    *self = Distinct::Registers(r);
                }
            }
            Distinct::Registers(r) => register(r, h),
        }
    }

    fn merge(&mut self, other: &Distinct) {
        match other {
            Distinct::Exact(hs) => {
                for &h in hs {
                    self.hash(h);
                }
            }
            Distinct::Registers(theirs) => {
                if let Distinct::Exact(hs) = self {
                    let mut r = vec![0u8; REGISTERS].into_boxed_slice();
                    for &h in hs.iter() {
                        register(&mut r, h);
                    }
                    *self = Distinct::Registers(r);
                }
                if let Distinct::Registers(r) = self {
                    for (a, b) in r.iter_mut().zip(theirs.iter()) {
                        *a = (*a).max(*b);
                    }
                }
            }
        }
    }

    fn estimate(&self) -> u64 {
        let r = match self {
            Distinct::Exact(hs) => return hs.len() as u64,
            Distinct::Registers(r) => r,
        };
        let m = REGISTERS as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let sum: f64 = r.iter().map(|&x| 2f64.powi(-i32::from(x))).sum();
        let zeros = r.iter().filter(|&&x| x == 0).count();
        let mut e = alpha * m * m / sum;
        if e <= 2.5 * m && zeros > 0 {
            let z = zeros as f64;
            e = m * (m / z).ln();
        }
        e.round() as u64
    }

    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Distinct::Exact(hs) => {
                out.push(0);
                out.extend_from_slice(&codec::len32(hs.len()).to_be_bytes());
                for h in hs {
                    out.extend_from_slice(&h.to_be_bytes());
                }
            }
            Distinct::Registers(r) => {
                out.push(1);
                out.extend_from_slice(r);
            }
        }
    }

    fn decode(buf: &mut &[u8]) -> Result<Distinct, String> {
        let [t] = take::<1>(buf)?;
        Ok(match t {
            0 => {
                let n = u32::from_be_bytes(take(buf)?);
                let mut hs = Vec::new();
                for _ in 0..n {
                    hs.push(u64::from_be_bytes(take(buf)?));
                }
                Distinct::Exact(hs)
            }
            1 => {
                if buf.len() < REGISTERS {
                    return Err("registers past the end".into());
                }
                let (r, rest) = buf.split_at(REGISTERS);
                *buf = rest;
                Distinct::Registers(r.into())
            }
            other => return Err(format!("a distinct count of kind {other}")),
        })
    }
}

/// Sets the register `h` falls in to the run of zeros it starts with.
fn register(r: &mut [u8], h: u64) {
    let i = usize::try_from(h >> (64 - PRECISION)).unwrap_or_default();
    let w = (h << PRECISION) | (1 << (PRECISION - 1));
    let rank = u8::try_from(w.leading_zeros() + 1).unwrap_or(u8::MAX);
    if let Some(x) = r.get_mut(i) {
        *x = (*x).max(rank);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn next(seed: &mut u64) -> u64 {
        *seed = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = *seed;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// The exact sum of floats doesn't depend on their order, where a sum
    /// as they come does.
    #[test]
    fn a_float_sum_is_the_same_in_any_order() {
        let values = [1e16, 1.0, -1e16, 0.1, 0.2, -0.3, 3.5e-12, 7.25, -1e-300];
        let mut seed = 3;
        let mut first = None;
        for _ in 0..200 {
            let mut v = values.to_vec();
            for i in (1..v.len()).rev() {
                let j = usize::try_from(next(&mut seed) % (i as u64 + 1)).unwrap();
                v.swap(i, j);
            }
            let mut s = Fsum::default();
            for x in &v {
                s.add(*x);
            }
            let t = s.total();
            assert_eq!(*first.get_or_insert(t), t, "{v:?}");
        }
        let mut split = Fsum::default();
        let mut rest = Fsum::default();
        for (i, x) in values.iter().enumerate() {
            match i % 2 {
                0 => split.add(*x),
                _ => rest.add(*x),
            }
        }
        split.merge(&rest);
        assert_eq!(Some(split.total()), first);
        let mut inf = Fsum::default();
        for x in [1.0, f64::INFINITY, 2.0] {
            inf.add(x);
        }
        assert_eq!(inf.total(), f64::INFINITY);
        inf.add(f64::NEG_INFINITY);
        assert!(inf.total().is_nan());
    }

    #[test]
    fn distinct_counts_are_exact_then_close() {
        let mut d = Distinct::default();
        for i in 0..200i64 {
            d.insert(&Value::Int(i % 150));
        }
        assert_eq!(d.estimate(), 150);
        for i in 0..20_000i64 {
            d.insert(&Value::Int(i));
        }
        let e = d.estimate();
        assert!((19_000..21_000).contains(&e), "{e}");
        let mut a = Distinct::default();
        let mut b = Distinct::default();
        for i in 0..20_000i64 {
            match i % 3 {
                0 => a.insert(&Value::Int(i)),
                _ => b.insert(&Value::Int(i)),
            }
        }
        a.merge(&b);
        assert_eq!(a, d);
    }

    #[test]
    fn accumulators_read_back_as_written() {
        let specs = [
            (AggregateKind::Count, None, DataType::Int64),
            (AggregateKind::Sum, Some(DataType::Int64), DataType::Int64),
            (
                AggregateKind::Avg,
                Some(DataType::Float64),
                DataType::Float64,
            ),
            (AggregateKind::Min, Some(DataType::Utf8), DataType::Utf8),
            (
                AggregateKind::ApproxDistinct,
                Some(DataType::Int64),
                DataType::UInt64,
            ),
            (
                AggregateKind::Percentile(0.5),
                Some(DataType::Float64),
                DataType::Float64,
            ),
        ];
        let ints = arrow_array::Int64Array::from(vec![Some(3), None, Some(-1)]);
        let floats = arrow_array::Float64Array::from(vec![Some(0.5), None, Some(2.0)]);
        let text = arrow_array::StringArray::from(vec![Some("b"), None, Some("a")]);
        for (kind, arg, out) in specs {
            let spec = Spec {
                kind,
                arg: arg.clone(),
                out,
            };
            let mut acc = Acc::new(&spec);
            let a: Option<&dyn Array> = match arg {
                None => None,
                Some(DataType::Int64) => Some(&ints),
                Some(DataType::Float64) => Some(&floats),
                _ => Some(&text),
            };
            for i in 0..3 {
                acc.update(a, i);
            }
            let mut buf = Vec::new();
            acc.encode(&mut buf);
            let mut at = buf.as_slice();
            assert_eq!(Acc::decode(&mut at).unwrap(), acc);
            assert!(at.is_empty());
        }
    }

    #[test]
    fn percentiles_interpolate_and_decimals_round() {
        let mut m = BTreeMap::new();
        for f in [1.0, 2.0, 3.0, 4.0] {
            *m.entry(Ordered::of_float(f)).or_default() += 1;
        }
        let at = |p| percentile(&m, p, &DataType::Float64).unwrap();
        assert_eq!(at(0.5), Some(Value::Float(2.5)));
        assert_eq!(at(0.0), Some(Value::Float(1.0)));
        assert_eq!(at(1.0), Some(Value::Float(4.0)));
        assert_eq!(divide(5, 2), 3);
        assert_eq!(divide(-5, 2), -3);
        assert_eq!(divide(4, 3), 1);
        for f in [-2.0, -0.0, 0.0, 1.5, f64::NEG_INFINITY] {
            assert_eq!(Ordered::of_float(f).float(), codec::canonical(f));
        }
        assert!(Ordered::of_float(-1.0) < Ordered::of_float(0.5));
    }

    /// A decimal median is the floor of the middle two's mean, at the
    /// type's extremes too.
    #[test]
    fn a_decimal_median_rounds_down_without_overflowing() {
        let median = |values: &[i128]| {
            let mut m = BTreeMap::new();
            for &v in values {
                *m.entry(Ordered::Decimal(v)).or_default() += 1;
            }
            percentile(&m, 0.5, &DataType::Decimal128(38, 0)).unwrap()
        };
        let big = 9 * 10i128.pow(37);
        assert_eq!(median(&[-big, big]), Some(Value::Decimal(0)));
        assert_eq!(median(&[big - 1, big]), Some(Value::Decimal(big - 1)));
        assert_eq!(median(&[-2, -1]), Some(Value::Decimal(-2)));
        assert_eq!(median(&[1, 4]), Some(Value::Decimal(2)));
    }
}
