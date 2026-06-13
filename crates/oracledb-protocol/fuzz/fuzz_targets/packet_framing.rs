#![no_main]
//! Fuzz target 1: TNS packet framing on arbitrary bytes.
//!
//! Entry point: `oracledb_protocol::packet::TnsPacket::parse(&[u8])`.
//! A hostile/buggy server (or a MITM) could send any bytes for the 8-byte TNS
//! header + body. `parse` must always return a `Result` — never panic, loop,
//! or OOM.
use libfuzzer_sys::fuzz_target;
use oracledb_protocol::packet::TnsPacket;

fuzz_target!(|data: &[u8]| {
    if data.len() > 1_000_000 {
        return;
    }
    // Err is fine (truncated/incomplete/oversize); a panic is the bug.
    let _ = TnsPacket::parse(data);
});
