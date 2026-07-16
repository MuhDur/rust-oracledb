#![no_main]
//! Fuzz target: DbObject scalar, text, XMLTYPE, LOB-text, locator, and temporal
//! descriptor-normalizer paths.
//!
//! Entry point: `oracledb_protocol::fuzz_api::fuzz_dbobject_scalars(&[u8])`,
//! which forwards into the DbObject text/XMLTYPE/BFILE/binary-float decoders,
//! the crate-private BINARY_INTEGER parser, and the bounded UTF-8 descriptor
//! normalizer used for TIMESTAMP(+TZ) DbObject attribute metadata.
use libfuzzer_sys::fuzz_target;
use oracledb_protocol::fuzz_api::fuzz_dbobject_scalars;

fuzz_target!(|data: &[u8]| {
    if data.len() > 65_536 {
        return;
    }
    fuzz_dbobject_scalars(data);
});
