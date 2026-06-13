#![no_main]
//! Fuzz target 4: VECTOR image decoder (`decode_values` and the header).
//!
//! Entry point: `oracledb_protocol::vector::decode_vector(&[u8])`.
//! The image header carries a u32 element count that drives a value-reading
//! loop (and the dense path's pre-allocation). Must fail closed on bad magic,
//! version, format, or a count that outruns the buffer — without OOM.
use libfuzzer_sys::fuzz_target;
use oracledb_protocol::vector::decode_vector;

fuzz_target!(|data: &[u8]| {
    if data.len() > 1_000_000 {
        return;
    }
    let _ = decode_vector(data);
});
