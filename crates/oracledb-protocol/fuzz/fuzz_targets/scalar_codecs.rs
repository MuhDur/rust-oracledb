#![no_main]
//! Fuzz target 5: the NUMBER / datetime / interval / binary scalar codecs.
//!
//! Entry point: `oracledb_protocol::fuzz_api::fuzz_scalar_codecs(&[u8])`
//! (a `#[cfg(fuzzing)]`-only wrapper that drives `decode_number_value`,
//! `decode_datetime_value`, `decode_interval_ds`, `decode_interval_ym`,
//! `decode_binary_float`, and `decode_binary_double` from one input).
//!
//! These take raw on-wire column bytes and do offset/century/exponent
//! arithmetic; with overflow-checks on, any arithmetic overflow is a panic the
//! decoder must instead reject as a `ProtocolError`.
use libfuzzer_sys::fuzz_target;
use oracledb_protocol::fuzz_api::fuzz_scalar_codecs;

fuzz_target!(|data: &[u8]| {
    if data.len() > 4096 {
        return;
    }
    fuzz_scalar_codecs(data);
});
