#![no_main]
//! Fuzz target: one OAC notification record parser.
//!
//! Entry point:
//! `oracledb_protocol::thin::try_parse_oac_record_with_limits(&[u8], ...)`.
//! This is the byte-slice boundary around `parse_oac_record`; incomplete data
//! returns `Ok(None)` and complete malformed records must fail closed.
use libfuzzer_sys::fuzz_target;
use oracledb_protocol::thin::try_parse_oac_record_with_limits;
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
    let (selector, payload) = data.split_first().map_or((0u8, data), |(v, r)| (*v, r));
    let namespace = u32::from(selector >> 4);
    let public_qos = u32::from((selector >> 2) & 0x03);
    let db_name = (selector & 0x01 != 0).then_some("FUZZDB");

    let _ = try_parse_oac_record_with_limits(payload, namespace, public_qos, db_name, limits());
});
