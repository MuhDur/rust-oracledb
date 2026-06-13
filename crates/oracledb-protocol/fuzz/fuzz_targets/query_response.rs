#![no_main]
//! Fuzz target 2: query/fetch response + column + row value parsing.
//!
//! Entry point: `oracledb_protocol::thin::parse_query_response(&[u8], caps)`.
//! This is the widest decoder surface: the TTC message-type dispatch loop, the
//! describe-info / column-metadata parser, row-data + bit-vector handling, and
//! every per-column scalar codec (NUMBER, datetime, intervals, VECTOR, OSON,
//! LOB locators, ...). It must fail closed on any adversarial server payload.
use libfuzzer_sys::fuzz_target;
use oracledb_protocol::thin::{parse_query_response, ClientCapabilities};

fuzz_target!(|data: &[u8]| {
    if data.len() > 1_000_000 {
        return;
    }
    // Derive the negotiated TTC field version from the first byte so the fuzzer
    // can reach the version-gated branches (12.2 / 23.1 / 23.4 metadata fields).
    let (ttc_field_version, payload) = data
        .split_first()
        .map_or((24u8, data), |(v, rest)| (*v, rest));
    let caps = ClientCapabilities {
        ttc_field_version,
        ..ClientCapabilities::default()
    };
    let _ = parse_query_response(payload, caps);
});
