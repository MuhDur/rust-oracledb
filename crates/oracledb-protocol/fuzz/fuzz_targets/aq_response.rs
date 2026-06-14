#![no_main]
//! Fuzz target 8: Advanced Queuing response decoders.
//!
//! Entry point: `oracledb_protocol::fuzz_api::fuzz_aq_responses(&[u8])`, which
//! drives `parse_aq_enq_response`, `parse_aq_deq_response`, and
//! `parse_aq_array_response` from one input (the leading byte selects the TTC
//! field version, payload kind, array operation, and props count). These
//! decoders were added by the AQ thin-mode merge and parse adversarial server
//! payloads off a bounded `TtcReader`; they must fail closed (never panic /
//! over-read / over-allocate) on any input.
use libfuzzer_sys::fuzz_target;
use oracledb_protocol::fuzz_api::fuzz_aq_responses;

fuzz_target!(|data: &[u8]| {
    if data.len() > 1_000_000 {
        return;
    }
    fuzz_aq_responses(data);
});
