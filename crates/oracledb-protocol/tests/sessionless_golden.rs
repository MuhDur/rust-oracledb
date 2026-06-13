//! Golden-wire tests for sessionless transactions (TTC FUNC 103
//! TPC_TXN_SWITCH for begin/suspend/resume, the sessionless PIGGYBACK that
//! rides on a deferred begin/resume execute, and the SYNC server-side
//! piggyback that reports the transaction state back).
//!
//! `tests/golden/sessionless_session.txt` is a raw PYO_DEBUG_PACKETS dump
//! produced by the REAL python-oracledb (thin mode, sync) running
//! `tests/golden/capture_sessionless.py` against a local Oracle 23.6+
//! container (server 23.26). Nothing in the capture is scrubbed (throwaway
//! container, fixed transaction id `golden_8700_txn_id`).
//!
//! Masking policy: none of the compared bytes are masked. The session-specific
//! TTC sequence numbers are read from the capture itself and fed to our
//! builders; everything else must match byte-for-byte.

use oracledb_protocol::thin::{
    build_sessionless_piggyback, build_tpc_txn_switch_payload_with_seq,
    decode_sessionless_txn_state, SessionlessTxnState, TNS_FUNC_TPC_TXN_SWITCH,
    TNS_MSG_TYPE_PIGGYBACK, TNS_TPC_TXN_DETACH, TNS_TPC_TXN_POST_DETACH, TNS_TPC_TXN_START,
    TPC_TXN_FLAGS_NEW, TPC_TXN_FLAGS_RESUME, TPC_TXN_FLAGS_SESSIONLESS,
};

const TNS_PACKET_TYPE_DATA: u8 = 6;
const TNS_MSG_TYPE_FUNCTION: u8 = 3;
const TXN_ID: &[u8] = b"golden_8700_txn_id";

#[derive(Clone, Debug)]
struct CapturedPacket {
    sending: bool,
    bytes: Vec<u8>,
}

impl CapturedPacket {
    fn packet_type(&self) -> u8 {
        self.bytes[4]
    }

    /// TTC payload of a data packet (skips 8-byte header + 2-byte data flags).
    fn data_payload(&self) -> &[u8] {
        assert_eq!(
            self.packet_type(),
            TNS_PACKET_TYPE_DATA,
            "not a data packet"
        );
        &self.bytes[10..]
    }
}

/// Parses a PYO_DEBUG_PACKETS dump (same format as pipeline_golden.rs): a
/// header line per packet then hex lines (`NNNN : HH HH .. |ascii|`).
fn parse_capture(text: &str) -> Vec<CapturedPacket> {
    let mut packets: Vec<CapturedPacket> = Vec::new();
    for line in text.lines() {
        if line.contains(" Sending packet [op ") || line.contains(" Sending data [op ") {
            packets.push(CapturedPacket {
                sending: true,
                bytes: Vec::new(),
            });
            continue;
        }
        if line.contains(" Receiving packet [op ") || line.contains(" Receiving data [op ") {
            packets.push(CapturedPacket {
                sending: false,
                bytes: Vec::new(),
            });
            continue;
        }
        let Some((offset, rest)) = line.split_once(" : ") else {
            continue;
        };
        if offset.len() != 4 || !offset.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        let Some(packet) = packets.last_mut() else {
            continue;
        };
        let hex_part = rest.split('|').next().unwrap_or("");
        for byte_hex in hex_part.split_whitespace() {
            let byte = u8::from_str_radix(byte_hex, 16).expect("valid hex byte in capture");
            packet.bytes.push(byte);
        }
    }
    packets
}

fn load_capture() -> Vec<CapturedPacket> {
    let text = include_str!("golden/sessionless_session.txt");
    let packets = parse_capture(text);
    assert!(
        packets.len() >= 12,
        "expected a full session capture, got {} packets",
        packets.len()
    );
    packets
}

/// All sent data-packet payloads that open with the FUNC 103 TPC_TXN_SWITCH
/// either as a direct FUNCTION message or as a PIGGYBACK at the head of the
/// payload, in capture order.
fn func_103_payloads(packets: &[CapturedPacket]) -> Vec<Vec<u8>> {
    packets
        .iter()
        .filter(|p| p.sending && p.packet_type() == TNS_PACKET_TYPE_DATA)
        .map(CapturedPacket::data_payload)
        .filter(|payload| {
            payload.len() >= 2
                && (payload[0] == TNS_MSG_TYPE_FUNCTION || payload[0] == TNS_MSG_TYPE_PIGGYBACK)
                && payload[1] == TNS_FUNC_TPC_TXN_SWITCH
        })
        .map(<[u8]>::to_vec)
        .collect()
}

/// The capture exercises four FUNC 103 operations in order:
///   0. begin  (immediate FUNCTION): START, flags NEW|SESSIONLESS, the XID
///   1. suspend (immediate FUNCTION): DETACH, flags SESSIONLESS, no XID
///   2. resume (deferred PIGGYBACK on the next execute): START,
///      flags RESUME|SESSIONLESS, the XID
///   3. resume (immediate FUNCTION): START, flags RESUME|SESSIONLESS, the XID
#[test]
fn tpc_txn_switch_payloads_match_reference_wire() {
    let packets = load_capture();
    let payloads = func_103_payloads(&packets);
    assert_eq!(
        payloads.len(),
        4,
        "expected begin + suspend + deferred-resume + resume FUNC 103 operations"
    );

    // 0: immediate begin — direct FUNCTION message, seq from the capture.
    let begin = &payloads[0];
    assert_eq!(begin[0], TNS_MSG_TYPE_FUNCTION);
    let begin_seq = begin[2];
    assert_eq!(
        &build_tpc_txn_switch_payload_with_seq(
            begin_seq,
            0,
            TNS_TPC_TXN_START,
            TPC_TXN_FLAGS_NEW | TPC_TXN_FLAGS_SESSIONLESS,
            15,
            Some(TXN_ID),
        ),
        begin,
        "begin TPC_TXN_SWITCH payload mismatch"
    );

    // 1: immediate suspend — direct FUNCTION message, no XID, timeout 0.
    let suspend = &payloads[1];
    assert_eq!(suspend[0], TNS_MSG_TYPE_FUNCTION);
    let suspend_seq = suspend[2];
    assert_eq!(
        &build_tpc_txn_switch_payload_with_seq(
            suspend_seq,
            0,
            TNS_TPC_TXN_DETACH,
            TPC_TXN_FLAGS_SESSIONLESS,
            0,
            None,
        ),
        suspend,
        "suspend TPC_TXN_SWITCH payload mismatch"
    );

    // 2: deferred resume — rides as a PIGGYBACK at the head of the next
    // execute. That execute used `suspend_on_success=True`, so the driver
    // folds POST_DETACH into the pending resume's operation (START|POST_DETACH).
    // Compare just the piggyback prefix our builder produces.
    let deferred = &payloads[2];
    assert_eq!(deferred[0], TNS_MSG_TYPE_PIGGYBACK);
    let deferred_seq = deferred[2];
    let piggyback = build_sessionless_piggyback(
        deferred_seq,
        0,
        TNS_TPC_TXN_START | TNS_TPC_TXN_POST_DETACH,
        TPC_TXN_FLAGS_RESUME | TPC_TXN_FLAGS_SESSIONLESS,
        5,
        Some(TXN_ID),
    );
    assert_eq!(
        &deferred[..piggyback.len()],
        piggyback.as_slice(),
        "deferred-resume sessionless piggyback prefix mismatch"
    );

    // 3: immediate resume — direct FUNCTION message.
    let resume = &payloads[3];
    assert_eq!(resume[0], TNS_MSG_TYPE_FUNCTION);
    let resume_seq = resume[2];
    // the final resume used the default timeout of 60s (no timeout kwarg)
    assert_eq!(
        &build_tpc_txn_switch_payload_with_seq(
            resume_seq,
            0,
            TNS_TPC_TXN_START,
            TPC_TXN_FLAGS_RESUME | TPC_TXN_FLAGS_SESSIONLESS,
            60,
            Some(TXN_ID),
        ),
        resume,
        "resume TPC_TXN_SWITCH payload mismatch"
    );
}

/// The begin/resume responses carry the transaction id and a SYNC piggyback
/// whose `TRANSACTION_ID` keyword binary value packs the sessionless state in
/// its last two bytes (state mask + sync version). Decode the exact bytes seen
/// on the wire for a SET and an UNSET update.
#[test]
fn sessionless_state_decode_matches_reference_bits() {
    // SET on the client (started by python-oracledb): state 0x40 (SYNC_SET),
    // sync version 1.
    assert_eq!(
        decode_sessionless_txn_state(&[0x40, 0x01]).expect("decode SET"),
        Some(SessionlessTxnState::Set {
            started_on_server: false
        }),
    );
    // SET started on the server via DBMS_TRANSACTION: 0x40 | 0x01 = 0x41.
    assert_eq!(
        decode_sessionless_txn_state(&[0x41, 0x01]).expect("decode server SET"),
        Some(SessionlessTxnState::Set {
            started_on_server: true
        }),
    );
    // UNSET (suspended / ended): 0x80.
    assert_eq!(
        decode_sessionless_txn_state(&[0x80, 0x01]).expect("decode UNSET"),
        Some(SessionlessTxnState::Unset),
    );
    // a transaction id prefix is allowed before the trailing state bytes
    assert_eq!(
        decode_sessionless_txn_state(b"\x01\x02\x03\x40\x01").expect("decode prefixed SET"),
        Some(SessionlessTxnState::Set {
            started_on_server: false
        }),
    );
    // an unknown sync version is rejected
    assert!(decode_sessionless_txn_state(&[0x40, 0x02]).is_err());
}
