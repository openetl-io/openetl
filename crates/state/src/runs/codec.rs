//! Varints, fixed-width numbers and checksums, as runs and snapshots encode
//! them.

use bytes::{BufMut, BytesMut};
use twox_hash::XxHash3_64;

use crate::{Error, Result};

pub fn checksum(b: &[u8]) -> u64 {
    XxHash3_64::oneshot(b)
}

pub fn put_varint(b: &mut BytesMut, mut v: u64) {
    while v >= 0x80 {
        b.put_u8((v as u8) | 0x80);
        v >>= 7;
    }
    b.put_u8(v as u8);
}

pub fn put_bytes(b: &mut BytesMut, v: &[u8]) {
    put_varint(b, v.len() as u64);
    b.put_slice(v);
}

/// Appends a checksum of everything in `b` from `from` on.
pub fn seal(b: &mut BytesMut, from: usize) {
    let sum = checksum(&b[from..]);
    b.put_u64(sum);
}

/// A reader of what [`put_varint`] and the rest wrote. Every read past the
/// end is [`Error::Corrupt`], naming `what`.
pub struct Reader<'a> {
    b: &'a [u8],
    what: &'static str,
}

impl<'a> Reader<'a> {
    pub fn new(b: &'a [u8], what: &'static str) -> Self {
        Self { b, what }
    }

    /// The part of `b` before its checksum, if the checksum holds.
    pub fn sealed(b: &'a [u8], what: &'static str) -> Result<Self> {
        let body = b
            .len()
            .checked_sub(8)
            .ok_or_else(|| Error::Corrupt(format!("{what}: shorter than its checksum")))?;
        let (body, sum) = b.split_at(body);
        if u64::from_be_bytes(sum.try_into().unwrap_or_default()) != checksum(body) {
            return Err(Error::Corrupt(format!(
                "{what}: its checksum doesn't match"
            )));
        }
        Ok(Self::new(body, what))
    }

    pub fn corrupt(&self) -> Error {
        Error::Corrupt(format!("{} runs past its end", self.what))
    }

    pub fn is_empty(&self) -> bool {
        self.b.is_empty()
    }

    pub fn remaining(&self) -> usize {
        self.b.len()
    }

    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.b.len() < n {
            return Err(self.corrupt());
        }
        let (head, rest) = self.b.split_at(n);
        self.b = rest;
        Ok(head)
    }

    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes(b.try_into().unwrap_or_default()))
    }

    pub fn u64(&mut self) -> Result<u64> {
        let b = self.take(8)?;
        Ok(u64::from_be_bytes(b.try_into().unwrap_or_default()))
    }

    pub fn i64(&mut self) -> Result<i64> {
        let b = self.take(8)?;
        Ok(i64::from_be_bytes(b.try_into().unwrap_or_default()))
    }

    pub fn varint(&mut self) -> Result<u64> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = self.u8()?;
            // The tenth byte holds the 64th bit only.
            if shift == 63 && byte > 1 {
                return Err(Error::Corrupt(format!(
                    "{}: a varint past 64 bits",
                    self.what
                )));
            }
            v |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(v);
            }
        }
        Err(Error::Corrupt(format!("{}: a varint too long", self.what)))
    }

    /// A varint that must fit a `usize`, as a length.
    pub fn count(&mut self) -> Result<usize> {
        let v = self.varint()?;
        usize::try_from(v).map_err(|_| self.corrupt())
    }

    /// A varint that must fit a key group.
    pub fn group(&mut self) -> Result<u32> {
        let v = self.varint()?;
        match u16::try_from(v) {
            Ok(g) => Ok(g.into()),
            Err(_) => Err(Error::Corrupt(format!("{}: key group {v}", self.what))),
        }
    }

    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.count()?;
        self.take(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(b: &[u8]) -> Result<u64> {
        let mut r = Reader::new(b, "a varint");
        let v = r.varint()?;
        assert!(r.is_empty());
        Ok(v)
    }

    #[test]
    fn varints_round_trip_to_64_bits_and_no_further() {
        for v in [0, 1, 127, 128, 300, 1 << 63, (1 << 63) - 1, u64::MAX] {
            let mut b = BytesMut::new();
            put_varint(&mut b, v);
            assert!(b.len() <= 10);
            assert_eq!(read(&b).ok(), Some(v), "{v}");
        }
        let mut max = vec![0xff; 9];
        max.push(0x01);
        assert_eq!(read(&max).ok(), Some(u64::MAX));
        for tenth in [0x02, 0x7f, 0x81] {
            let mut over = vec![0x80; 9];
            over.push(tenth);
            assert!(matches!(read(&over), Err(Error::Corrupt(_))), "{tenth:#x}");
        }
        let eleven = [0x80; 11];
        assert!(matches!(read(&eleven), Err(Error::Corrupt(_))));
        assert!(matches!(read(&[0x80, 0x80]), Err(Error::Corrupt(_))));
    }
}
