#![no_main]
//! Fuzz target: sessionless transaction and TPC response parsers.
//!
//! Entry point: the public sessionless/TPC response parser family in
//! `crates/oracledb-protocol/src/thin/sessionless.rs`: transaction-state bits,
//! sessionless transaction switch, full XA switch, and TPC change-state.
use libfuzzer_sys::fuzz_target;
use oracledb_protocol::thin::{
    decode_sessionless_txn_state, parse_tpc_change_state_response_with_limits,
    parse_tpc_switch_response_with_limits, parse_tpc_txn_switch_response_with_limits,
    ClientCapabilities,
};
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

fn caps(selector: u8) -> ClientCapabilities {
    ClientCapabilities {
        ttc_field_version: 24 - (selector & 0x07),
        ..ClientCapabilities::default()
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() > 1_000_000 {
        return;
    }
    let (selector, payload) = data.split_first().map_or((0u8, data), |(v, r)| (*v, r));
    let capabilities = caps(selector);

    let _ = decode_sessionless_txn_state(payload);
    let _ = parse_tpc_txn_switch_response_with_limits(payload, capabilities, limits());
    let _ = parse_tpc_switch_response_with_limits(payload, capabilities, limits());
    let _ = parse_tpc_change_state_response_with_limits(payload, capabilities, limits());
});
