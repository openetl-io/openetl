//! Arrow payloads. Each is a standalone Arrow IPC stream.

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use arrow_ipc::writer::StreamWriter;
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
    let mut w = StreamWriter::try_new(Vec::new(), &batch.schema())?;
    w.write(batch)?;
    w.finish()?;
    w.into_inner()
}

/// Encodes a stream holding only the schema.
pub fn encode_schema(schema: &Schema) -> Result<Vec<u8>, ArrowError> {
    let mut w = StreamWriter::try_new(Vec::new(), schema)?;
    w.finish()?;
    w.into_inner()
}

#[cfg(test)]
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
