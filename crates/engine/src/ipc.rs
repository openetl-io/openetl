//! Arrow payloads. Each is a standalone Arrow IPC stream.

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use arrow_ipc::writer::{StreamEncoder, StreamWriter};
use arrow_schema::{ArrowError, Schema};

/// Decodes a stream holding exactly one batch.
pub fn decode_batch(payload: Vec<u8>) -> Result<RecordBatch, ArrowError> {
    let mut buf = Buffer::from_vec(payload);
    let mut decoder = StreamDecoder::new();
    let mut batch = None;
    while !buf.is_empty() {
        if let Some(b) = decoder.decode(&mut buf)?
            && batch.replace(b).is_some()
        {
            return Err(ArrowError::IpcError(
                "a BATCH payload holds more than one batch".into(),
            ));
        }
    }
    decoder.finish()?;
    batch.ok_or_else(|| ArrowError::IpcError("a BATCH payload holds no batch".into()))
}

pub fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>, ArrowError> {
    let parts = encode_batch_parts(batch)?;
    let mut out = Vec::with_capacity(parts.iter().map(Buffer::len).sum());
    for p in &parts {
        out.extend_from_slice(p);
    }
    Ok(out)
}

/// The stream `encode_batch` makes, in parts: its metadata and padding,
/// and between them the batch's own buffers, not copied.
pub fn encode_batch_parts(batch: &RecordBatch) -> Result<Vec<Buffer>, ArrowError> {
    let mut e = StreamEncoder::try_new(&batch.schema())?;
    let mut parts = e.encode(batch)?;
    parts.extend(e.finish()?);
    Ok(parts)
}

/// Encodes a stream holding only the schema.
pub fn encode_schema(schema: &Schema) -> Result<Vec<u8>, ArrowError> {
    let mut w = StreamWriter::try_new(Vec::new(), schema)?;
    w.finish()?;
    w.into_inner()
}

pub fn decode_schema(payload: &[u8]) -> Result<arrow_schema::SchemaRef, ArrowError> {
    let r = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(payload), None)?;
    Ok(r.schema())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field};

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None])),
                Arc::new(StringArray::from(vec![Some("x"), Some("y")])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn batch_round_trip() {
        let b = batch();
        assert_eq!(decode_batch(encode_batch(&b).unwrap()).unwrap(), b);
    }

    /// The parts make the bytes a stream writer makes, of a batch whose
    /// arrays are slices too.
    #[test]
    fn parts_are_a_writers_stream() {
        let b = batch();
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let long = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter(
                    (0..1000).map(|i| (i % 7 != 0).then_some(i)),
                )),
                Arc::new(StringArray::from_iter(
                    (0..1000).map(|i| (i % 5 != 0).then(|| "v".repeat(i % 13))),
                )),
            ],
        )
        .unwrap();
        for b in [b, long.clone(), long.slice(3, 500), long.slice(999, 1)] {
            let mut w = StreamWriter::try_new(Vec::new(), &b.schema()).unwrap();
            w.write(&b).unwrap();
            w.finish().unwrap();
            let written = w.into_inner().unwrap();
            assert_eq!(encode_batch(&b).unwrap(), written);
            let parts: Vec<u8> = encode_batch_parts(&b)
                .unwrap()
                .iter()
                .flat_map(|p| p.as_slice().to_vec())
                .collect();
            assert_eq!(parts, written);
        }
    }

    #[test]
    fn schema_round_trip() {
        let b = batch();
        assert_eq!(
            decode_schema(&encode_schema(&b.schema()).unwrap()).unwrap(),
            b.schema()
        );
    }

    #[test]
    fn rejects_payloads_without_exactly_one_batch() {
        let b = batch();
        assert!(decode_batch(encode_schema(&b.schema()).unwrap()).is_err());

        let mut w = StreamWriter::try_new(Vec::new(), &b.schema()).unwrap();
        w.write(&b).unwrap();
        w.write(&b).unwrap();
        w.finish().unwrap();
        assert!(decode_batch(w.into_inner().unwrap()).is_err());
    }
}
