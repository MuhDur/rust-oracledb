//! Golden-wire tests for XA / two-phase commit (TPC) transactions: TTC FUNC 103
//! TPC_TXN_SWITCH (begin START / end DETACH) and FUNC 104 TPC_TXN_CHANGE_STATE
//! (prepare / commit / rollback).
//!
//! `tests/golden/tpc_session.txt` is a raw PYO_DEBUG_PACKETS dump produced by
//! the REAL python-oracledb (thin mode, sync) running
//! `tests/golden/capture_tpc.py` against a local Oracle 23.6+ container
//! (server 23.26). Nothing in the capture is scrubbed (throwaway container,
//! fixed XID format_id 4400 / gtid "txn4400" / bqual "branchId" and a second
//! XID 4406 / "txn4406" / "branch4").
//!
//! Masking policy: the begin/end/prepare/commit/rollback REQUEST payloads are
//! compared byte-for-byte against our builders. The only session-specific input
//! fed from the capture is the TTC sequence number (a per-call counter) and the
//! ~168-byte transaction context the server returned on begin (captured from
//! the begin RESPONSE and echoed on the following operations). Everything else
//! must match the reference wire exactly.

use oracledb_protocol::thin::{
    build_tpc_change_state_payload_with_seq, build_tpc_switch_payload_with_seq,
    parse_tpc_change_state_response, parse_tpc_switch_response, ClientCapabilities, TpcXid,
    TNS_FUNC_TPC_TXN_CHANGE_STATE, TNS_FUNC_TPC_TXN_SWITCH, TNS_TPC_TXN_ABORT, TNS_TPC_TXN_COMMIT,
    TNS_TPC_TXN_DETACH, TNS_TPC_TXN_PREPARE, TNS_TPC_TXN_START, TNS_TPC_TXN_STATE_ABORTED,
    TNS_TPC_TXN_STATE_COMMITTED, TNS_TPC_TXN_STATE_FORGOTTEN, TNS_TPC_TXN_STATE_PREPARE,
    TNS_TPC_TXN_STATE_REQUIRES_COMMIT, TPC_TXN_FLAGS_NEW,
};

const TNS_PACKET_TYPE_DATA: u8 = 6;
const TNS_MSG_TYPE_FUNCTION: u8 = 3;

const FORMAT_ID: u32 = 4400;
const GTID: &[u8] = b"txn4400";
const BQUAL: &[u8] = b"branchId";
const FORMAT_ID_2: u32 = 4406;
const GTID_2: &[u8] = b"txn4406";
const BQUAL_2: &[u8] = b"branch4";

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

/// Parses a PYO_DEBUG_PACKETS dump (same format as sessionless_golden.rs): a
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
    let text = include_str!("golden/tpc_session.txt");
    let packets = parse_capture(text);
    assert!(
        packets.len() >= 18,
        "expected a full TPC session capture, got {} packets",
        packets.len()
    );
    packets
}

/// Sent data-packet payloads that open with a FUNCTION message for `func`, in
/// capture order.
fn function_payloads(packets: &[CapturedPacket], func: u8) -> Vec<Vec<u8>> {
    packets
        .iter()
        .filter(|p| p.sending && p.packet_type() == TNS_PACKET_TYPE_DATA)
        .map(CapturedPacket::data_payload)
        .filter(|payload| {
            payload.len() >= 2 && payload[0] == TNS_MSG_TYPE_FUNCTION && payload[1] == func
        })
        .map(<[u8]>::to_vec)
        .collect()
}

/// Received data-packet payloads in capture order (for parsing responses).
fn received_payloads(packets: &[CapturedPacket]) -> Vec<Vec<u8>> {
    packets
        .iter()
        .filter(|p| !p.sending && p.packet_type() == TNS_PACKET_TYPE_DATA)
        .map(|p| p.data_payload().to_vec())
        .collect()
}

/// The transaction contexts the server returned on each FUNC 103 *begin*
/// (START) response, in capture order. A begin response is the received data
/// packet immediately following a START switch request; DETACH (end) responses
/// also carry a (cleared) context and are skipped.
fn begin_contexts(packets: &[CapturedPacket]) -> Vec<Vec<u8>> {
    let mut contexts = Vec::new();
    let mut pending_begin = false;
    for packet in packets {
        let is_data = packet.packet_type() == TNS_PACKET_TYPE_DATA;
        if packet.sending && is_data {
            let payload = packet.data_payload();
            // operation is the ub4 right after [3][func][seq] + token ub8(0).
            // START = [1, 1]; a START switch request begins a new transaction.
            pending_begin = payload.len() >= 6
                && payload[0] == TNS_MSG_TYPE_FUNCTION
                && payload[1] == TNS_FUNC_TPC_TXN_SWITCH
                && payload[4] == 1
                && payload[5] == TNS_TPC_TXN_START as u8;
        } else if pending_begin && !packet.sending && is_data {
            let response =
                parse_tpc_switch_response(packet.data_payload(), ClientCapabilities::default())
                    .expect("begin response should parse");
            contexts.push(response.context);
            pending_begin = false;
        }
    }
    contexts
}

#[test]
fn tpc_begin_and_end_switch_payloads_match_reference_wire() {
    let packets = load_capture();
    let switch_payloads = function_payloads(&packets, TNS_FUNC_TPC_TXN_SWITCH);
    // begin(xid), begin(xid2), end(xid2) = three FUNC 103 operations.
    assert_eq!(
        switch_payloads.len(),
        3,
        "expected begin + begin2 + end FUNC 103 operations, got {}",
        switch_payloads.len()
    );
    let contexts = begin_contexts(&packets);
    assert_eq!(
        contexts.len(),
        2,
        "two begins (xid1, xid2) each return a context"
    );
    assert!(
        contexts.iter().all(|context| !context.is_empty()),
        "begin must return a transaction context to echo"
    );

    // 0: begin(xid1) — START, flags NEW, the XID, no context, default timeout 0.
    let begin = &switch_payloads[0];
    let begin_seq = begin[2];
    let xid = TpcXid {
        format_id: FORMAT_ID,
        global_transaction_id: GTID,
        branch_qualifier: BQUAL,
    };
    assert_eq!(
        &build_tpc_switch_payload_with_seq(
            begin_seq,
            TNS_TPC_TXN_START,
            TPC_TXN_FLAGS_NEW,
            0,
            Some(&xid),
            None,
        ),
        begin,
        "begin TPC_TXN_SWITCH payload mismatch"
    );

    // 2: end(xid2) — DETACH, echoes xid2's begin context, flags 0, timeout 0.
    let end = &switch_payloads[2];
    let end_seq = end[2];
    let xid2 = TpcXid {
        format_id: FORMAT_ID_2,
        global_transaction_id: GTID_2,
        branch_qualifier: BQUAL_2,
    };
    assert_eq!(
        &build_tpc_switch_payload_with_seq(
            end_seq,
            TNS_TPC_TXN_DETACH,
            0,
            0,
            Some(&xid2),
            Some(&contexts[1]),
        ),
        end,
        "end TPC_TXN_SWITCH payload mismatch"
    );
}

#[test]
fn tpc_change_state_payloads_match_reference_wire() {
    let packets = load_capture();
    let change_payloads = function_payloads(&packets, TNS_FUNC_TPC_TXN_CHANGE_STATE);
    // prepare(), commit(), rollback(xid2) = three FUNC 104 operations.
    assert_eq!(
        change_payloads.len(),
        3,
        "expected prepare + commit + rollback FUNC 104 operations, got {}",
        change_payloads.len()
    );
    let contexts = begin_contexts(&packets);
    assert_eq!(contexts.len(), 2, "two begins each return a context");

    // 0: prepare() — PREPARE, requested state 0, no XID, echoes xid1's context.
    let prepare = &change_payloads[0];
    let prepare_seq = prepare[2];
    assert_eq!(
        &build_tpc_change_state_payload_with_seq(
            prepare_seq,
            TNS_TPC_TXN_PREPARE,
            TNS_TPC_TXN_STATE_PREPARE,
            0,
            None,
            Some(&contexts[0]),
        ),
        prepare,
        "prepare TPC_TXN_CHANGE_STATE payload mismatch"
    );

    // 1: commit() (two-phase) — COMMIT, requested COMMITTED(2), echoes xid1's
    // context. After this the driver clears `_transaction_context` to None.
    let commit = &change_payloads[1];
    let commit_seq = commit[2];
    assert_eq!(
        &build_tpc_change_state_payload_with_seq(
            commit_seq,
            TNS_TPC_TXN_COMMIT,
            TNS_TPC_TXN_STATE_COMMITTED,
            0,
            None,
            Some(&contexts[0]),
        ),
        commit,
        "two-phase commit TPC_TXN_CHANGE_STATE payload mismatch"
    );

    // 2: rollback(xid2) — ABORT, requested ABORTED(3), XID set, NO context: the
    // preceding end(xid2) cleared `_transaction_context` to None.
    let rollback = &change_payloads[2];
    let rollback_seq = rollback[2];
    let xid2 = TpcXid {
        format_id: FORMAT_ID_2,
        global_transaction_id: GTID_2,
        branch_qualifier: BQUAL_2,
    };
    assert_eq!(
        &build_tpc_change_state_payload_with_seq(
            rollback_seq,
            TNS_TPC_TXN_ABORT,
            TNS_TPC_TXN_STATE_ABORTED,
            0,
            Some(&xid2),
            None,
        ),
        rollback,
        "rollback TPC_TXN_CHANGE_STATE payload mismatch"
    );
}

#[test]
fn tpc_change_state_responses_decode_reference_out_states() {
    let packets = load_capture();
    let received = received_payloads(&packets);

    // Find the change-state responses by parsing every received data packet
    // that decodes with an out state, in order: prepare -> REQUIRES_COMMIT(1),
    // two-phase commit -> FORGOTTEN(5), rollback -> ABORTED(3).
    let mut out_states: Vec<u32> = Vec::new();
    // Pair each FUNC 104 request with the next received packet.
    let mut expect_response = false;
    for packet in &packets {
        let is_data = packet.packet_type() == TNS_PACKET_TYPE_DATA;
        if packet.sending && is_data {
            let payload = packet.data_payload();
            expect_response = payload.len() >= 2
                && payload[0] == TNS_MSG_TYPE_FUNCTION
                && payload[1] == TNS_FUNC_TPC_TXN_CHANGE_STATE;
        } else if expect_response && !packet.sending && is_data {
            let response = parse_tpc_change_state_response(
                packet.data_payload(),
                ClientCapabilities::default(),
            )
            .expect("change-state response should parse");
            out_states.push(response.state);
            expect_response = false;
        }
    }

    assert_eq!(
        out_states,
        vec![
            TNS_TPC_TXN_STATE_REQUIRES_COMMIT,
            TNS_TPC_TXN_STATE_FORGOTTEN,
            TNS_TPC_TXN_STATE_ABORTED,
        ],
        "prepare/commit/rollback out states must match the reference wire"
    );
    // sanity: the received payloads vector is non-trivial
    assert!(received.len() >= 6);
}
