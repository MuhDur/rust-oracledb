#![no_main]
//! Fuzz target: borrowed fetch/query response parser plus row walker.
//!
//! Entry point:
//! `oracledb_protocol::thin::parse_query_response_borrowed_with_limits`.
//! The selector byte derives a small previous-cursor column state and optional
//! previous row seed; successful parses are walked through `for_each_row_ref`
//! to exercise the lazy borrowed scalar decoders, not just the framing pass.
use libfuzzer_sys::fuzz_target;
use oracledb_protocol::thin::{
    parse_query_response_borrowed_with_limits, ClientCapabilities, ColumnMetadata, QueryValue,
    QueryValueRef, CS_FORM_IMPLICIT, CS_FORM_NCHAR, ORA_TYPE_NUM_BINARY_DOUBLE,
    ORA_TYPE_NUM_BINARY_FLOAT, ORA_TYPE_NUM_BINARY_INTEGER, ORA_TYPE_NUM_BOOLEAN,
    ORA_TYPE_NUM_CHAR, ORA_TYPE_NUM_DATE, ORA_TYPE_NUM_LONG, ORA_TYPE_NUM_NUMBER, ORA_TYPE_NUM_RAW,
    ORA_TYPE_NUM_TIMESTAMP, ORA_TYPE_NUM_VARCHAR,
};
use oracledb_protocol::wire::ProtocolLimits;
use oracledb_protocol::ProtocolError;

fn limits() -> ProtocolLimits {
    ProtocolLimits {
        max_packet_bytes: 1_048_576,
        max_frame_bytes: 1_048_576,
        max_response_bytes: 1_048_576,
        max_columns: 8,
        max_binds: 64,
        max_batch_rows: 64,
        max_object_depth: 32,
        max_object_elements: 4096,
        max_vector_dimensions: 4096,
        max_lob_chunks: 4096,
        max_length_prefixed_elements: 4096,
    }
}

fn caps(selector: u8) -> ClientCapabilities {
    ClientCapabilities {
        ttc_field_version: 24 - (selector & 0x07),
        ..ClientCapabilities::default()
    }
}

fn columns(selector: u8) -> Vec<ColumnMetadata> {
    const TYPES: [u8; 8] = [
        ORA_TYPE_NUM_VARCHAR,
        ORA_TYPE_NUM_NUMBER,
        ORA_TYPE_NUM_RAW,
        ORA_TYPE_NUM_BOOLEAN,
        ORA_TYPE_NUM_DATE,
        ORA_TYPE_NUM_BINARY_FLOAT,
        ORA_TYPE_NUM_BINARY_DOUBLE,
        ORA_TYPE_NUM_TIMESTAMP,
    ];
    let count = usize::from(selector & 0x03) + 1;
    let offset = usize::from(selector >> 2);
    (0..count)
        .map(|index| {
            let ora_type_num = TYPES[(offset + index) % TYPES.len()];
            let csfrm = if selector & 0x40 != 0
                && matches!(
                    ora_type_num,
                    ORA_TYPE_NUM_VARCHAR | ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_LONG
                ) {
                CS_FORM_NCHAR
            } else {
                CS_FORM_IMPLICIT
            };
            ColumnMetadata::new(format!("C{index}"), ora_type_num)
                .with_csfrm(csfrm)
                .with_buffer_size(4096)
                .with_max_size(4096)
                .with_nulls_allowed(true)
        })
        .collect()
}

fn previous_row_for(columns: &[ColumnMetadata]) -> Vec<Option<QueryValue>> {
    columns
        .iter()
        .map(|column| match column.ora_type_num() {
            ORA_TYPE_NUM_VARCHAR | ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_LONG => {
                Some(QueryValue::Text("fuzz".to_string()))
            }
            ORA_TYPE_NUM_RAW => Some(QueryValue::Raw(vec![0xde, 0xad])),
            ORA_TYPE_NUM_NUMBER | ORA_TYPE_NUM_BINARY_INTEGER => {
                Some(QueryValue::number_from_text("1", true))
            }
            ORA_TYPE_NUM_BOOLEAN => Some(QueryValue::Boolean(false)),
            ORA_TYPE_NUM_DATE | ORA_TYPE_NUM_TIMESTAMP => Some(QueryValue::DateTime {
                year: 2026,
                month: 1,
                day: 1,
                hour: 0,
                minute: 0,
                second: 0,
                nanosecond: 0,
            }),
            ORA_TYPE_NUM_BINARY_FLOAT | ORA_TYPE_NUM_BINARY_DOUBLE => {
                Some(QueryValue::BinaryDouble("1.0".to_string()))
            }
            _ => None,
        })
        .collect()
}

fuzz_target!(|data: &[u8]| {
    if data.len() > 1_000_000 {
        return;
    }
    let (selector, payload) = data.split_first().map_or((0u8, data), |(v, r)| (*v, r));
    let capabilities = caps(selector);
    let columns = columns(selector);
    let previous_row = (selector & 0x80 != 0).then(|| previous_row_for(&columns));

    if let Ok(parsed) = parse_query_response_borrowed_with_limits(
        payload,
        capabilities,
        &columns,
        previous_row.as_deref(),
        limits(),
    ) {
        let walk: Result<(), ProtocolError> = parsed.batch.for_each_row_ref(|row| {
            for value in row.iter().flatten() {
                match value {
                    QueryValueRef::Text(text) => {
                        let _ = text.len();
                    }
                    QueryValueRef::Raw(bytes) => {
                        let _ = bytes.len();
                    }
                    _ => {
                        let _ = value.to_owned_value();
                    }
                }
            }
            Ok(())
        });
        let _ = walk;
    }
});
