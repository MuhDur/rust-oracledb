//! Regression guard for the captured `.tns-cassette` fixture's framing shape.
//!
//! The replay path relies on two facts that this test pins down, so a
//! re-captured fixture that violates them fails loudly here rather than with a
//! confusing decode error in the offline replay test:
//!
//! 1. The fixture is a valid `.tns-cassette` with BOTH directions captured.
//! 2. The server side captures a TNS packet as (at least) two reads — an 8-byte
//!    header read followed by a body read — so frame boundaries are NOT packet
//!    boundaries. The replay reader must reassemble across frames via
//!    `read_exact`, which is exactly what the offline replay test does.

#![cfg(feature = "cassette")]

use oracledb_protocol::net::cassette::{decode_all, Direction};

fn fixture_bytes() -> Vec<u8> {
    let path = format!(
        "{}/tests/fixtures/cassettes/select_7_plus_5.tns-cassette",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read(&path).unwrap_or_else(|e| {
        panic!("missing cassette fixture {path} ({e}); capture it via the live record test")
    })
}

#[test]
fn fixture_is_a_real_bidirectional_cassette() {
    let frames = decode_all(&fixture_bytes()).expect("fixture must be a valid .tns-cassette");
    assert!(frames.len() >= 4, "a full session has many frames");
    let c2s = frames
        .iter()
        .filter(|f| f.direction == Direction::ClientToServer)
        .count();
    let s2c = frames
        .iter()
        .filter(|f| f.direction == Direction::ServerToClient)
        .count();
    assert!(c2s > 0, "must contain captured C->S writes");
    assert!(s2c > 0, "must contain captured S->C reads");
}

#[test]
fn server_packets_are_captured_as_header_then_body_reads() {
    let frames = decode_all(&fixture_bytes()).expect("valid cassette");
    // The very first S->C frame is the ACCEPT packet's 8-byte header. Its legacy
    // 16-bit length (bytes 0..2) exceeds 8, proving a separate body read follows
    // in the next S->C frame — i.e. one packet spans multiple frames.
    let first_s2c = frames
        .iter()
        .find(|f| f.direction == Direction::ServerToClient)
        .expect("a server frame");
    assert_eq!(first_s2c.bytes.len(), 8, "header read is exactly 8 bytes");
    let declared = u16::from_be_bytes([first_s2c.bytes[0], first_s2c.bytes[1]]);
    assert!(
        usize::from(declared) > first_s2c.bytes.len(),
        "ACCEPT declares a body beyond the header, so the packet spans frames"
    );
}
