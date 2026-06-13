#![no_main]
//! Fuzz target 3: OSON (binary JSON) decoder on arbitrary bytes.
//!
//! Entry point: `oracledb_protocol::oson::decode_oson(&[u8])`.
//! OSON is a self-describing, offset-indexed node graph: the decoder seeks to
//! absolute tree-segment positions encoded in the image, which makes it the
//! highest-risk parser for malformed input (out-of-range offsets, cyclic /
//! deeply-nested containers, bogus field-id tables). Must fail closed.
use libfuzzer_sys::fuzz_target;
use oracledb_protocol::oson::decode_oson;

fuzz_target!(|data: &[u8]| {
    if data.len() > 1_000_000 {
        return;
    }
    let _ = decode_oson(data);
});
