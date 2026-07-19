#![no_main]
//! Fuzz the public SQL bind-name surface. Arbitrary UTF-8, including a lone
//! quote, must never panic or slice through a code-point boundary.

use libfuzzer_sys::fuzz_target;
use oracledb_protocol::sql;

fuzz_target!(|data: &[u8]| {
    if let Ok(text) = core::str::from_utf8(data) {
        let _ = sql::public_bind_name(text);
        let _ = sql::is_quoted_bind_name(text);
        let _ = sql::scan_bind_names(text);
        let _ = sql::unique_bind_names(text);
        let _ = sql::bind_names_per_occurrence(text);
    }
});
