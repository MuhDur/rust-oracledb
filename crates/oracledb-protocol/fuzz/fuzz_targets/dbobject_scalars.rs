#![no_main]
//! Fuzz target: DbObject scalar, text, XMLTYPE, LOB-text, and locator decoders.
//!
//! Entry point: `oracledb_protocol::fuzz_api::fuzz_dbobject_scalars(&[u8])`,
//! which forwards into the DbObject text/XMLTYPE/BFILE/binary-float decoders
//! plus the crate-private BINARY_INTEGER parser.
use libfuzzer_sys::fuzz_target;
use oracledb_protocol::fuzz_api::fuzz_dbobject_scalars;

fuzz_target!(|data: &[u8]| {
    if data.len() > 65_536 {
        return;
    }
    fuzz_dbobject_scalars(data);
});
