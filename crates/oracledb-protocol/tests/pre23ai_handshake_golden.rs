#![forbid(unsafe_code)]

//! Sans-I/O wire regression tests for the pre-23ai classic handshake
//! (bead rust-oracledb-pre23ai-connect-z47u.4), in the spirit of
//! `pipeline_golden.rs` / `dbobject_golden.rs`.
//!
//! Fixtures under `tests/golden/pre23ai_*.hex` are LAB CAPTURES ONLY (gvenzl
//! XE 11 / XE 18 / FREE 23ai containers, throwaway lab users — see
//! `golden/pre23ai_handshake.meta.txt`). They pin the wire contract that
//! shipped broken in 0.5.x (fast-auth-only, 23ai-only) and its 0.6.0 fixes:
//!
//! - the 8-byte RESEND packet answered before ACCEPT,
//! - the pre-23ai ACCEPT payload without fast-auth / END_OF_RESPONSE flags,
//! - the BELOW-FLOOR ACCEPT (Oracle 11g, version 314) that must be refused
//!   with the structured `UnsupportedVersion` error naming the floor —
//!   never a misleading decode error,
//! - classic protocol-negotiation / data-types / auth responses that
//!   terminate at their terminal TTC message WITHOUT any END_OF_RESPONSE
//!   framing (and the incomplete-prefix "keep reading" loop behavior).

use oracledb_protocol::packet::TnsPacket;
use oracledb_protocol::thin::{
    classic_connect_response_is_complete, parse_accept_payload, parse_auth_response,
    TNS_PACKET_TYPE_RESEND,
};
use oracledb_protocol::wire::ProtocolLimits;
use oracledb_protocol::{ProtocolError, TNS_VERSION_MIN_ACCEPTED};

fn fixture(name: &str) -> Vec<u8> {
    let hex = match name {
        "resend_packet" => include_str!("golden/pre23ai_resend_packet.hex"),
        "xe11_accept" => include_str!("golden/pre23ai_xe11_accept_payload.hex"),
        "xe18_accept" => include_str!("golden/pre23ai_xe18_accept_payload.hex"),
        "free23_accept" => include_str!("golden/free23_accept_payload.hex"),
        "protocol_negotiation" => {
            include_str!("golden/pre23ai_xe18_protocol_negotiation_response.hex")
        }
        "data_types" => include_str!("golden/pre23ai_xe18_data_types_response.hex"),
        "auth_phase_one" => include_str!("golden/pre23ai_xe18_auth_phase_one_response.hex"),
        "auth_phase_two" => include_str!("golden/pre23ai_xe18_auth_phase_two_response.hex"),
        other => panic!("unknown fixture {other}"),
    };
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let cleaned: String = hex.split_whitespace().collect();
    let mut chars = cleaned.chars();
    while let (Some(hi), Some(lo)) = (chars.next(), chars.next()) {
        let byte = u8::from_str_radix(&format!("{hi}{lo}"), 16)
            .unwrap_or_else(|_| panic!("bad hex in fixture {name}"));
        bytes.push(byte);
    }
    assert!(!bytes.is_empty(), "fixture {name} is empty");
    bytes
}

/// A classic connect-phase response must be complete at its full length and
/// incomplete ("read another DATA packet") at EVERY strict prefix — the
/// terminate-without-END_OF_RESPONSE loop contract.
fn assert_complete_only_at_full_length(name: &str, payload: &[u8]) {
    assert!(
        classic_connect_response_is_complete(payload, ProtocolLimits::DEFAULT)
            .unwrap_or_else(|err| panic!("{name}: completion probe failed: {err}")),
        "{name}: full captured response must be complete"
    );
    for end in 1..payload.len() {
        let prefix = &payload[..end];
        let complete = classic_connect_response_is_complete(prefix, ProtocolLimits::DEFAULT)
            .unwrap_or_else(|err| panic!("{name}: prefix len {end} errored: {err}"));
        assert!(
            !complete,
            "{name}: prefix of {end}/{} bytes must NOT be complete",
            payload.len()
        );
    }
}

#[test]
fn resend_packet_is_the_eight_byte_resend() {
    let raw = fixture("resend_packet");
    assert_eq!(raw.len(), 8, "RESEND is a bare 8-byte header");
    let packet = TnsPacket::parse(&raw).expect("RESEND packet parses");
    assert_eq!(packet.packet_type, TNS_PACKET_TYPE_RESEND);
    assert_eq!(packet.flags, 0);
    assert!(packet.payload.is_empty(), "RESEND carries no payload");
}

#[test]
fn xe11_below_floor_accept_is_refused_with_the_structured_error() {
    // Oracle 11g answers ACCEPT with protocol version 314 and a 24-byte
    // pre-12.1 payload. The refusal must be the STRUCTURED error naming both
    // the offered version and the floor — the exact parity contract with
    // python-oracledb's ERR_SERVER_VERSION_NOT_SUPPORTED (DPY-3010).
    let payload = fixture("xe11_accept");
    let err = parse_accept_payload(&payload).expect_err("version 314 must be refused");
    match err {
        ProtocolError::UnsupportedVersion { version, minimum } => {
            assert_eq!(version, 314, "11g negotiates TNS version 314");
            assert_eq!(minimum, TNS_VERSION_MIN_ACCEPTED);
            assert_eq!(minimum, 315, "floor is the reference's 12.1 minimum");
        }
        other => panic!("expected UnsupportedVersion, got {other:?}"),
    }
    // The message must be self-explanatory: it names the version and floor.
    let message = parse_accept_payload(&payload)
        .expect_err("version 314 must be refused")
        .to_string();
    assert!(
        message.contains("314") && message.contains("315"),
        "refusal must name the offered version and the floor: {message}"
    );
}

#[test]
fn below_floor_refusal_precedes_payload_decoding() {
    // Only the 2-byte version field: the floor check must fire BEFORE the
    // parser touches the (short, pre-12.1-layout) remainder. Regression pin
    // for the original failure mode, a misleading "truncated TTC payload".
    let payload = &fixture("xe11_accept")[..2];
    assert!(
        matches!(
            parse_accept_payload(payload),
            Err(ProtocolError::UnsupportedVersion {
                version: 314,
                minimum: 315,
            })
        ),
        "refusal must not depend on the rest of the payload"
    );
}

#[test]
fn xe18_accept_negotiates_classic_without_fast_auth_flags() {
    let info = parse_accept_payload(&fixture("xe18_accept")).expect("XE 18 ACCEPT parses");
    assert_eq!(info.protocol_version, 317, "XE 18 negotiates version 317");
    assert!(
        !info.supports_fast_auth,
        "pre-23ai ACCEPT must not advertise fast auth"
    );
    assert!(
        !info.supports_end_of_response,
        "pre-23ai ACCEPT must not advertise END_OF_RESPONSE framing"
    );
    assert_eq!(info.sdu, 8192);
}

#[test]
fn accept_at_exactly_the_floor_is_accepted() {
    // Patch the captured XE 18 payload down to exactly the floor (315): the
    // boundary version must connect, one below must not.
    let mut payload = fixture("xe18_accept");
    payload[0] = 0x01;
    payload[1] = 0x3b; // 315
    let info = parse_accept_payload(&payload).expect("version 315 is the lowest accepted");
    assert_eq!(info.protocol_version, 315);
    assert!(!info.supports_fast_auth);
    assert!(!info.supports_end_of_response);

    payload[1] = 0x3a; // 314
    assert!(matches!(
        parse_accept_payload(&payload),
        Err(ProtocolError::UnsupportedVersion {
            version: 314,
            minimum: 315,
        })
    ));
}

#[test]
fn free23_accept_is_the_fast_auth_contrast_pin() {
    // The 23ai ACCEPT proves the same parser distinguishes the eras: version
    // 319 with fast-auth and END_OF_RESPONSE flags set.
    let info = parse_accept_payload(&fixture("free23_accept")).expect("FREE 23ai ACCEPT parses");
    assert_eq!(info.protocol_version, 319);
    assert!(info.supports_fast_auth);
    assert!(info.supports_end_of_response);
    assert_eq!(info.sdu, 8192);
}

#[test]
fn classic_protocol_negotiation_response_terminates_at_its_message() {
    let payload = fixture("protocol_negotiation");
    assert_complete_only_at_full_length("protocol negotiation", &payload);
    // The connect flow parses this response for capabilities: database charset
    // and the ttc field version negotiated DOWN to the client's ceiling.
    let parsed = parse_auth_response(&payload).expect("protocol negotiation response parses");
    let caps = parsed
        .capabilities
        .expect("protocol negotiation carries compile/runtime capabilities");
    assert_eq!(caps.charset_id, 873, "lab database charset is AL32UTF8");
    assert!(
        caps.ttc_field_version > 0,
        "server must report a ttc field version"
    );
}

#[test]
fn classic_data_types_response_terminates_at_its_message() {
    let payload = fixture("data_types");
    assert_complete_only_at_full_length("data types", &payload);
    parse_auth_response(&payload).expect("data types response parses");
}

#[test]
fn classic_auth_phase_one_response_carries_verifier_data() {
    let payload = fixture("auth_phase_one");
    assert_complete_only_at_full_length("auth phase one", &payload);
    let parsed = parse_auth_response(&payload).expect("auth phase one response parses");
    assert!(
        parsed.verifier_type.is_some(),
        "phase one must carry the password verifier type"
    );
    for key in ["AUTH_VFR_DATA", "AUTH_SESSKEY"] {
        assert!(
            parsed.session_data.contains_key(key),
            "phase one session data must contain {key}, got keys {:?}",
            parsed.session_data.keys().collect::<Vec<_>>()
        );
    }
}

#[test]
fn classic_auth_phase_two_response_carries_session_data() {
    let payload = fixture("auth_phase_two");
    assert_complete_only_at_full_length("auth phase two", &payload);
    let parsed = parse_auth_response(&payload).expect("auth phase two response parses");
    for key in ["AUTH_SESSION_ID", "AUTH_SERIAL_NUM", "AUTH_VERSION_STRING"] {
        assert!(
            parsed.session_data.contains_key(key),
            "phase two session data must contain {key}"
        );
    }
}
