#![no_main]
//! Fuzz target: typed TTC decode boundary.
//!
//! This composes the scalar codecs, direct OSON/VECTOR decoders, owned fetch,
//! borrowed fetch, and DefineMetadata LOB paths under one small adversarial
//! payload. The target is intentionally capped below the broad query target so
//! it can run in CI smoke and short local campaigns without becoming an OOM
//! amplifier.
use libfuzzer_sys::fuzz_target;
use oracledb_protocol::fuzz_api::fuzz_scalar_codecs;
use oracledb_protocol::oson::decode_oson_with_limits;
use oracledb_protocol::thin::{
    parse_define_fetch_response_borrowed_with_limits,
    parse_define_fetch_response_with_context_and_limits,
    parse_fetch_response_with_context_and_limits, parse_query_response_borrowed_with_limits,
    parse_query_response_with_limits, ClientCapabilities, ColumnMetadata, LobValue, QueryValue,
    CS_FORM_IMPLICIT, CS_FORM_NCHAR, ORA_TYPE_NUM_BINARY_DOUBLE, ORA_TYPE_NUM_BINARY_FLOAT,
    ORA_TYPE_NUM_BINARY_INTEGER, ORA_TYPE_NUM_BLOB, ORA_TYPE_NUM_BOOLEAN, ORA_TYPE_NUM_CHAR,
    ORA_TYPE_NUM_CLOB, ORA_TYPE_NUM_DATE, ORA_TYPE_NUM_INTERVAL_DS, ORA_TYPE_NUM_INTERVAL_YM,
    ORA_TYPE_NUM_JSON, ORA_TYPE_NUM_LONG, ORA_TYPE_NUM_LONG_RAW, ORA_TYPE_NUM_NUMBER,
    ORA_TYPE_NUM_RAW, ORA_TYPE_NUM_TIMESTAMP, ORA_TYPE_NUM_TIMESTAMP_LTZ,
    ORA_TYPE_NUM_TIMESTAMP_TZ, ORA_TYPE_NUM_VARCHAR, ORA_TYPE_NUM_VECTOR,
};
use oracledb_protocol::vector::{decode_vector_with_limits, VECTOR_FORMAT_FLOAT32};
use oracledb_protocol::wire::ProtocolLimits;
use oracledb_protocol::ProtocolError;

const MAX_INPUT_BYTES: usize = 65_536;

fn limits() -> ProtocolLimits {
    ProtocolLimits {
        max_packet_bytes: MAX_INPUT_BYTES,
        max_frame_bytes: MAX_INPUT_BYTES,
        max_response_bytes: MAX_INPUT_BYTES,
        max_columns: 8,
        max_binds: 32,
        max_batch_rows: 64,
        max_object_depth: 64,
        max_object_elements: 4096,
        max_vector_dimensions: 4096,
        max_lob_chunks: 1024,
        max_length_prefixed_elements: 4096,
    }
}

fn caps(selector: u8) -> ClientCapabilities {
    let ttc_field_version = match selector & 0x03 {
        0 => 24,
        1 => 23,
        2 => 20,
        _ => 18,
    };
    ClientCapabilities {
        ttc_field_version,
        max_string_size: if selector & 0x10 == 0 { 4000 } else { 32_767 },
        charset_id: if selector & 0x20 == 0 { 873 } else { 2000 },
    }
}

fn columns(selector: u8) -> Vec<ColumnMetadata> {
    const TYPES: [u8; 16] = [
        ORA_TYPE_NUM_VARCHAR,
        ORA_TYPE_NUM_NUMBER,
        ORA_TYPE_NUM_RAW,
        ORA_TYPE_NUM_BOOLEAN,
        ORA_TYPE_NUM_DATE,
        ORA_TYPE_NUM_TIMESTAMP,
        ORA_TYPE_NUM_TIMESTAMP_TZ,
        ORA_TYPE_NUM_TIMESTAMP_LTZ,
        ORA_TYPE_NUM_INTERVAL_DS,
        ORA_TYPE_NUM_INTERVAL_YM,
        ORA_TYPE_NUM_BINARY_FLOAT,
        ORA_TYPE_NUM_BINARY_DOUBLE,
        ORA_TYPE_NUM_CLOB,
        ORA_TYPE_NUM_BLOB,
        ORA_TYPE_NUM_VECTOR,
        ORA_TYPE_NUM_JSON,
    ];

    let count = usize::from(selector & 0x07) + 1;
    let offset = usize::from(selector >> 3);
    (0..count)
        .map(|index| {
            let ora_type_num = TYPES
                .iter()
                .copied()
                .cycle()
                .nth(offset + index)
                .unwrap_or(ORA_TYPE_NUM_VARCHAR);
            let csfrm = if selector & 0x80 != 0
                && matches!(
                    ora_type_num,
                    ORA_TYPE_NUM_VARCHAR
                        | ORA_TYPE_NUM_CHAR
                        | ORA_TYPE_NUM_LONG
                        | ORA_TYPE_NUM_CLOB
                ) {
                CS_FORM_NCHAR
            } else {
                CS_FORM_IMPLICIT
            };

            ColumnMetadata::new(format!("D{index}"), ora_type_num)
                .with_csfrm(csfrm)
                .with_buffer_size(4096)
                .with_max_size(4096)
                .with_nulls_allowed(true)
                .with_is_json(ora_type_num == ORA_TYPE_NUM_JSON)
                .with_is_oson(ora_type_num == ORA_TYPE_NUM_JSON)
                .with_vector_dimensions((ora_type_num == ORA_TYPE_NUM_VECTOR).then_some(4))
                .with_vector_format(if ora_type_num == ORA_TYPE_NUM_VECTOR {
                    VECTOR_FORMAT_FLOAT32
                } else {
                    0
                })
        })
        .collect()
}

fn previous_row_for(columns: &[ColumnMetadata]) -> Vec<Option<QueryValue>> {
    columns
        .iter()
        .map(|column| match column.ora_type_num() {
            ORA_TYPE_NUM_VARCHAR | ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_LONG => {
                Some(QueryValue::Text("seed".to_string()))
            }
            ORA_TYPE_NUM_RAW | ORA_TYPE_NUM_LONG_RAW => Some(QueryValue::Raw(vec![0xde, 0xad])),
            ORA_TYPE_NUM_NUMBER | ORA_TYPE_NUM_BINARY_INTEGER => {
                Some(QueryValue::number_from_text("1", true))
            }
            ORA_TYPE_NUM_BOOLEAN => Some(QueryValue::Boolean(false)),
            ORA_TYPE_NUM_BINARY_FLOAT | ORA_TYPE_NUM_BINARY_DOUBLE => {
                Some(QueryValue::BinaryDouble("1.0".to_string()))
            }
            ORA_TYPE_NUM_INTERVAL_DS => Some(QueryValue::IntervalDS {
                days: 0,
                hours: 0,
                minutes: 0,
                seconds: 0,
                fseconds: 0,
            }),
            ORA_TYPE_NUM_INTERVAL_YM => Some(QueryValue::IntervalYM {
                years: 0,
                months: 0,
            }),
            ORA_TYPE_NUM_TIMESTAMP_TZ => Some(QueryValue::TimestampTz {
                year: 2026,
                month: 1,
                day: 1,
                hour: 0,
                minute: 0,
                second: 0,
                nanosecond: 0,
                offset_minutes: 0,
            }),
            ORA_TYPE_NUM_DATE | ORA_TYPE_NUM_TIMESTAMP | ORA_TYPE_NUM_TIMESTAMP_LTZ => {
                Some(QueryValue::DateTime {
                    year: 2026,
                    month: 1,
                    day: 1,
                    hour: 0,
                    minute: 0,
                    second: 0,
                    nanosecond: 0,
                })
            }
            ORA_TYPE_NUM_CLOB | ORA_TYPE_NUM_BLOB => Some(QueryValue::Lob(Box::new(LobValue {
                ora_type_num: column.ora_type_num(),
                csfrm: column.csfrm(),
                locator: vec![1, 2, 3, 4],
                size: 0,
                chunk_size: 0,
            }))),
            _ => None,
        })
        .collect()
}

fn walk_borrowed<T>(parsed: Result<T, ProtocolError>)
where
    T: BorrowedRows,
{
    if let Ok(parsed) = parsed {
        let _ = parsed.walk_rows();
    }
}

trait BorrowedRows {
    fn walk_rows(&self) -> Result<(), ProtocolError>;
}

impl BorrowedRows for oracledb_protocol::thin::BorrowedFetchResult {
    fn walk_rows(&self) -> Result<(), ProtocolError> {
        self.batch.for_each_row_ref(|row| {
            for value in row.iter().flatten() {
                let _ = value.to_owned_value();
            }
            Ok(())
        })
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }

    let (selector, rest) = data
        .split_first()
        .map_or((0, data), |(value, rest)| (*value, rest));
    let (shape, payload) = rest
        .split_first()
        .map_or((0, rest), |(value, rest)| (*value, rest));
    let split = if payload.is_empty() {
        0
    } else {
        usize::from(selector) % (payload.len() + 1)
    };
    let (left, right) = payload.split_at(split);
    let capabilities = caps(selector);
    let columns = columns(shape);
    let previous_row = (selector & 0x40 != 0).then(|| previous_row_for(&columns));
    let limits = limits();

    fuzz_scalar_codecs(left);
    let _ = decode_vector_with_limits(left, limits);
    let _ = decode_oson_with_limits(right, limits);
    let _ = parse_query_response_with_limits(payload, capabilities, limits);
    let _ = parse_fetch_response_with_context_and_limits(
        payload,
        capabilities,
        &columns,
        previous_row.as_deref(),
        limits,
    );
    let _ = parse_define_fetch_response_with_context_and_limits(
        payload,
        capabilities,
        &columns,
        previous_row.as_deref(),
        limits,
    );
    walk_borrowed(parse_query_response_borrowed_with_limits(
        payload,
        capabilities,
        &columns,
        previous_row.as_deref(),
        limits,
    ));
    walk_borrowed(parse_define_fetch_response_borrowed_with_limits(
        payload,
        capabilities,
        &columns,
        previous_row.as_deref(),
        limits,
    ));
});
