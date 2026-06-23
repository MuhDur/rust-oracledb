#![no_main]
//! Fuzz target: the `ALTER SESSION SET <key> = <value>` value extractor
//! (`sql::parse_alter_session_value`), driven via
//! `oracledb_protocol::fuzz_api::fuzz_alter_session_value(&str)`.
//!
//! This pure string parser tracks session state (current_schema / edition) the
//! server reflects back without a round trip. It consumes statement text and
//! must NEVER panic / slice across a UTF-8 boundary — only return `Some`/`None`.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(text) = core::str::from_utf8(data) {
        oracledb_protocol::fuzz_api::fuzz_alter_session_value(text);
    }
});
