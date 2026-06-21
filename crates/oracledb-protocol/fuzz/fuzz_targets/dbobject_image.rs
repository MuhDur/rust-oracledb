#![no_main]
//! Fuzz target: DbObject packed-image reader walk.
//!
//! Entry point: `oracledb_protocol::fuzz_api::fuzz_dbobject_image_walk(&[u8])`,
//! which drives `DbObjectPackedReader` header, length, value, atomic-null, and
//! bounded-allocation helpers from one deterministic operation stream.
use libfuzzer_sys::fuzz_target;
use oracledb_protocol::fuzz_api::fuzz_dbobject_image_walk;

fuzz_target!(|data: &[u8]| {
    if data.len() > 65_536 {
        return;
    }
    fuzz_dbobject_image_walk(data);
});
