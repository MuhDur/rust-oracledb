#![no_main]
//! Fuzz target 7: Direct Path Load response parsers.
//!
//! Entry points: `parse_direct_path_prepare_response(&[u8], caps)` and
//! `parse_direct_path_simple_response(&[u8], caps)` (function 129/130). Both
//! run a TTC message-type dispatch loop that reads column metadata, return
//! parameters, and the shared server-error trailer. Must fail closed.
use libfuzzer_sys::fuzz_target;
use oracledb_protocol::dpl::{
    parse_direct_path_prepare_response, parse_direct_path_simple_response,
};
use oracledb_protocol::thin::ClientCapabilities;

fuzz_target!(|data: &[u8]| {
    if data.len() > 1_000_000 {
        return;
    }
    let (ttc_field_version, payload) = data
        .split_first()
        .map_or((24u8, data), |(v, rest)| (*v, rest));
    let caps = ClientCapabilities {
        ttc_field_version,
        ..ClientCapabilities::default()
    };
    let _ = parse_direct_path_prepare_response(payload, caps);
    let _ = parse_direct_path_simple_response(payload, caps);
});
