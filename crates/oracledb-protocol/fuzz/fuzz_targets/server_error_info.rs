#![no_main]
//! Fuzz target 6: server-error-info trailer parsing.
//!
//! Entry point: `oracledb_protocol::fuzz_api::fuzz_parse_server_error_info`
//! (a `#[cfg(fuzzing)]`-only wrapper over the `pub(crate)`
//! `parse_server_error_info`). The TTC error trailer has batch-error / offset /
//! message sub-arrays whose counts come straight from the wire, plus a
//! version-gated 20.1+ tail. The first input byte selects `ttc_field_version`.
//! Also exercises the server-side piggyback skipper.
use libfuzzer_sys::fuzz_target;
use oracledb_protocol::fuzz_api::{fuzz_parse_server_error_info, fuzz_skip_server_side_piggyback};

fuzz_target!(|data: &[u8]| {
    if data.len() > 1_000_000 {
        return;
    }
    let _ = fuzz_parse_server_error_info(data);
    let _ = fuzz_skip_server_side_piggyback(data);
});
