#![no_main]
//! Fuzz target: TNS ACCEPT packet payload decoder.
//!
//! Entry point: `oracledb_protocol::thin::parse_accept_payload(&[u8])`.
//! The accept payload parser walks fixed offsets and version-gated fields from
//! the server's connection handshake response. Any truncated or adversarial
//! payload must return `Err`, never panic.
use libfuzzer_sys::fuzz_target;
use oracledb_protocol::thin::parse_accept_payload;

fuzz_target!(|data: &[u8]| {
    if data.len() > 65_536 {
        return;
    }
    let _ = parse_accept_payload(data);
});
