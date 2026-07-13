//! Apache Arrow IPC stream encoding for fetched [`RecordBatch`] values.

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;

use super::Result;

/// Serializes one [`RecordBatch`] as a self-describing Arrow IPC stream.
///
/// The returned bytes contain the schema followed by exactly one record batch
/// and the stream terminator. This is transport-neutral: callers remain
/// responsible for applying any egress policy before sending the bytes.
pub fn record_batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, batch.schema().as_ref())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
    use arrow_ipc::reader::StreamReader;
    use arrow_schema::{DataType, Field, Schema};

    use super::record_batch_to_ipc;

    #[test]
    fn record_batch_round_trips_through_ipc_stream() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![7, 9])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("Ada"), None])) as ArrayRef,
            ],
        )
        .expect("fixture batch is valid");

        let bytes = record_batch_to_ipc(&batch).expect("IPC encoding succeeds");
        let mut reader =
            StreamReader::try_new(Cursor::new(bytes), None).expect("IPC stream header is readable");
        let decoded = reader
            .next()
            .expect("stream contains one batch")
            .expect("batch decodes");

        assert_eq!(decoded, batch);
        assert!(reader.next().is_none(), "stream contains exactly one batch");
    }
}
