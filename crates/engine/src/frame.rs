//! Frames on a data edge: a 12-byte header, then the payload.

use std::io;

use openetl_proto::engine::v1::FrameKind;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const HEADER_LEN: usize = 12;
/// Larger lengths mean a corrupt header.
pub const MAX_PAYLOAD: usize = 1 << 30;

#[derive(Debug)]
pub struct Frame {
    pub kind: FrameKind,
    /// An index into the edge's streams.
    pub stream: u32,
    pub payload: Vec<u8>,
}

pub fn header(kind: FrameKind, stream: u32, len: u32) -> [u8; HEADER_LEN] {
    let mut h = [0u8; HEADER_LEN];
    h[0] = kind as u8;
    h[4..8].copy_from_slice(&stream.to_le_bytes());
    h[8..12].copy_from_slice(&len.to_le_bytes());
    h
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// Reads the next frame, or `None` if the edge closed between frames.
pub async fn read<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<Option<Frame>> {
    let mut h = [0u8; HEADER_LEN];
    let mut filled = 0;
    while filled < HEADER_LEN {
        match r.read(&mut h[filled..]).await? {
            0 if filled == 0 => return Ok(None),
            0 => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "edge closed inside a frame header",
                ));
            }
            n => filled += n,
        }
    }
    let kind = FrameKind::try_from(i32::from(h[0]))
        .ok()
        .filter(|k| *k != FrameKind::Unspecified)
        .ok_or_else(|| invalid(format!("unknown frame kind {}", h[0])))?;
    if h[1..4] != [0, 0, 0] {
        return Err(invalid("reserved header bytes are set"));
    }
    let stream = u32::from_le_bytes([h[4], h[5], h[6], h[7]]);
    let len = u32::from_le_bytes([h[8], h[9], h[10], h[11]]) as usize;
    if len > MAX_PAYLOAD {
        return Err(invalid(format!("frame payload of {len} bytes")));
    }
    let mut payload = Vec::with_capacity(len);
    r.take(len as u64).read_to_end(&mut payload).await?;
    if payload.len() != len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "edge closed inside a frame payload",
        ));
    }
    Ok(Some(Frame {
        kind,
        stream,
        payload,
    }))
}

pub async fn write<W: AsyncWrite + Unpin>(
    w: &mut W,
    kind: FrameKind,
    stream: u32,
    payload: &[u8],
) -> io::Result<()> {
    let len = u32::try_from(payload.len())
        .ok()
        .filter(|n| *n as usize <= MAX_PAYLOAD)
        .ok_or_else(|| invalid(format!("frame payload of {} bytes", payload.len())))?;
    w.write_all(&header(kind, stream, len)).await?;
    w.write_all(payload).await
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // internal/engineclient checks the same bytes.
    #[test]
    fn header_layout() {
        assert_eq!(
            header(FrameKind::Batch, 3, 5),
            [2, 0, 0, 0, 3, 0, 0, 0, 5, 0, 0, 0]
        );
    }

    #[tokio::test]
    async fn round_trip_and_clean_eof() {
        let mut buf = Vec::new();
        write(&mut buf, FrameKind::Rows, 7, b"abc").await.unwrap();
        write(&mut buf, FrameKind::EndOfStream, 7, b"")
            .await
            .unwrap();

        let mut r = buf.as_slice();
        let f = read(&mut r).await.unwrap().unwrap();
        assert_eq!(
            (f.kind, f.stream, f.payload.as_slice()),
            (FrameKind::Rows, 7, b"abc".as_slice())
        );
        let f = read(&mut r).await.unwrap().unwrap();
        assert_eq!((f.kind, f.payload.len()), (FrameKind::EndOfStream, 0));
        assert!(read(&mut r).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn rejects_truncated_and_corrupt_frames() {
        let mut buf = Vec::new();
        write(&mut buf, FrameKind::Batch, 0, b"abcdef")
            .await
            .unwrap();

        let err = read(&mut &buf[..5]).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        let err = read(&mut &buf[..15]).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);

        let mut bad = buf.clone();
        bad[0] = 99;
        assert_eq!(
            read(&mut bad.as_slice()).await.unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        let mut bad = buf.clone();
        bad[2] = 1;
        assert_eq!(
            read(&mut bad.as_slice()).await.unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
