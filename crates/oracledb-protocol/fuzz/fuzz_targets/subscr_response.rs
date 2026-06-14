#![no_main]
//! Fuzz target 9: subscription / CQN notification decoders.
//!
//! Entry point: `oracledb_protocol::fuzz_api::fuzz_subscr_responses(&[u8])`,
//! which drives `parse_subscribe_response` and `parse_notification_stream`
//! (the OAC-record / grouping-notification parser) from one input. The leading
//! byte selects the TTC field version, namespace, and QoS flags. These decoders
//! were added by the CQN/subscription thin-mode merge and parse adversarial
//! server payloads off a bounded `TtcReader`; they must fail closed on any
//! input.
use libfuzzer_sys::fuzz_target;
use oracledb_protocol::fuzz_api::fuzz_subscr_responses;

fuzz_target!(|data: &[u8]| {
    if data.len() > 1_000_000 {
        return;
    }
    fuzz_subscr_responses(data);
});
