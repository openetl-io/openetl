//! Bloom filters: a section keeps one per key group it holds, so a lane
//! that owns part of a run keeps only its groups' filters.

use bytes::{BufMut, BytesMut};
use twox_hash::XxHash3_64;

use super::codec::{Reader, put_varint};
use crate::Result;

/// The hash a filter takes of what [`crate::layout::filtered`] gives.
pub fn hash(k: &[u8]) -> u64 {
    XxHash3_64::oneshot_with_seed(0x6f70_656e_6574_6c31, k)
}

#[derive(Clone, Debug)]
pub struct Bloom {
    probes: u8,
    words: Box<[u64]>,
}

impl Bloom {
    /// A filter of `hashes` at about `bits` bits a key, at least 64 bits.
    pub fn build(hashes: &[u64], bits: u32) -> Bloom {
        let want = (hashes.len() as u64)
            .saturating_mul(u64::from(bits))
            .max(64);
        let words = usize::try_from(want.div_ceil(64)).unwrap_or(usize::MAX);
        let mut b = Bloom {
            probes: probes(bits),
            words: vec![0; words].into_boxed_slice(),
        };
        for &h in hashes {
            for bit in b.bits(h) {
                b.words[bit / 64] |= 1 << (bit % 64);
            }
        }
        b
    }

    fn bits(&self, h: u64) -> impl Iterator<Item = usize> + use<> {
        let m = self.words.len() as u64 * 64;
        let delta = h.rotate_left(31) | 1;
        let mut x = h;
        (0..self.probes).map(move |_| {
            let bit = x % m;
            x = x.wrapping_add(delta);
            bit as usize
        })
    }

    pub fn may_contain(&self, h: u64) -> bool {
        self.bits(h)
            .all(|bit| self.words[bit / 64] & (1 << (bit % 64)) != 0)
    }

    pub fn memory_bytes(&self) -> usize {
        self.words.len() * 8 + std::mem::size_of::<Self>()
    }

    pub fn encode(&self, b: &mut BytesMut) {
        b.put_u8(self.probes);
        put_varint(b, self.words.len() as u64);
        for w in &self.words {
            b.put_u64_le(*w);
        }
    }

    pub fn decode(r: &mut Reader<'_>) -> Result<Bloom> {
        let probes = r.u8()?;
        let words = r.count()?;
        if !(1..=30).contains(&probes) || words == 0 {
            return Err(r.corrupt());
        }
        let raw = r.take(words.checked_mul(8).ok_or_else(|| r.corrupt())?)?;
        let words = raw
            .chunks_exact(8)
            .map(|w| u64::from_le_bytes(w.try_into().unwrap_or_default()))
            .collect();
        Ok(Bloom { probes, words })
    }

    /// Skips an encoded filter.
    pub fn skip(r: &mut Reader<'_>) -> Result<()> {
        r.u8()?;
        let words = r.count()?;
        r.take(words.checked_mul(8).ok_or_else(|| r.corrupt())?)?;
        Ok(())
    }
}

/// The probes that minimise false positives at `bits` bits a key.
fn probes(bits: u32) -> u8 {
    let k = (f64::from(bits) * std::f64::consts::LN_2).round();
    k.clamp(1.0, 30.0) as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rate(bits: u32) -> f64 {
        let keys: Vec<u64> = (0..20_000u64).map(|i| hash(&i.to_be_bytes())).collect();
        let b = Bloom::build(&keys, bits);
        assert!(keys.iter().all(|h| b.may_contain(*h)));
        let tries = 200_000u64;
        let hits = (1_000_000..1_000_000 + tries)
            .filter(|i: &u64| b.may_contain(hash(&i.to_be_bytes())))
            .count();
        hits as f64 / tries as f64
    }

    /// No false negatives, and false positives near what the bits give:
    /// about 0.8% at 10 bits a key, under 0.01% at 20.
    #[test]
    fn false_positives_follow_the_bits() {
        let ten = rate(10);
        let twenty = rate(20);
        assert!(ten > 0.004 && ten < 0.015, "{ten}");
        assert!(twenty < 0.0003, "{twenty}");
    }

    #[test]
    fn a_filter_round_trips() {
        let keys: Vec<u64> = (0..100u64).map(|i| hash(&i.to_le_bytes())).collect();
        let b = Bloom::build(&keys, 16);
        let mut out = BytesMut::new();
        b.encode(&mut out);
        let mut r = Reader::new(&out, "a filter");
        let d = Bloom::decode(&mut r).unwrap_or_else(|e| panic!("{e}"));
        assert!(r.is_empty());
        assert_eq!(d.words, b.words);
        assert_eq!(d.probes, b.probes);
        let mut r = Reader::new(&out[..out.len() - 1], "a filter");
        assert!(Bloom::decode(&mut r).is_err());
    }
}
