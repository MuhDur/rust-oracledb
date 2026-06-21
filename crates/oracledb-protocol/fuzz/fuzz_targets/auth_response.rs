#![no_main]
//! Fuzz target: authentication response decoder.
//!
//! Entry point:
//! `oracledb_protocol::thin::parse_auth_response_with_limits(&[u8], limits)`.
//! Auth responses carry protocol-info, data-types, key/value return parameters,
//! piggybacks, and server errors; malformed server payloads must fail closed.
use libfuzzer_sys::fuzz_target;
use oracledb_protocol::thin::parse_auth_response_with_limits;
use oracledb_protocol::wire::ProtocolLimits;

fn limits() -> ProtocolLimits {
    ProtocolLimits {
        max_packet_bytes: 1_048_576,
        max_frame_bytes: 1_048_576,
        max_response_bytes: 1_048_576,
        max_columns: 64,
        max_binds: 64,
        max_batch_rows: 64,
        max_object_depth: 32,
        max_object_elements: 4096,
        max_vector_dimensions: 4096,
        max_lob_chunks: 4096,
        max_length_prefixed_elements: 4096,
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() > 1_000_000 {
        return;
    }
    let _ = parse_auth_response_with_limits(data, limits());
});
