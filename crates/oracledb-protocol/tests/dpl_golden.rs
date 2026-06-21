//! Golden-wire tests for the direct path load protocol.
//!
//! `tests/golden/dpl_session.txt` is a raw PYO_DEBUG_PACKETS dump produced by
//! the REAL python-oracledb 4.0.1 (thin mode) running
//! `tests/golden/capture_dpl.py` against a local Oracle Free 23ai container.
//! Nothing in the capture is scrubbed (throwaway test container).
//!
//! Masking policy: none of the compared bytes are masked. Instead, the two
//! session-specific inputs are *extracted from the capture itself* and fed to
//! our builders:
//!   * the TTC sequence number (payload byte 2 of each function message),
//!   * the direct path cursor id (parsed from the prepare response by the
//!     code under test).
//!
//! Everything else must match byte-for-byte.

use oracledb_protocol::dpl::{
    build_direct_path_load_stream_payload, build_direct_path_op_payload,
    build_direct_path_prepare_payload, encode_direct_path_rows, parse_direct_path_prepare_response,
    parse_direct_path_simple_response, BatchLoadState, DirectPathColumnValue, TNS_DP_OP_ABORT,
    TNS_DP_OP_FINISH, TNS_FUNC_DIRECT_PATH_LOAD_STREAM, TNS_FUNC_DIRECT_PATH_OP,
    TNS_FUNC_DIRECT_PATH_PREPARE,
};
use oracledb_protocol::thin::{
    ClientCapabilities, ORA_TYPE_NUM_BINARY_DOUBLE, ORA_TYPE_NUM_DATE, ORA_TYPE_NUM_NUMBER,
    ORA_TYPE_NUM_RAW, ORA_TYPE_NUM_TIMESTAMP, ORA_TYPE_NUM_VARCHAR,
};

const TNS_PACKET_TYPE_DATA: u8 = 6;

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

    fn is_function(&self, function_code: u8) -> bool {
        self.sending
            && self.packet_type() == TNS_PACKET_TYPE_DATA
            && self.bytes.len() > 12
            && self.bytes[10] == 3
            && self.bytes[11] == function_code
    }

    fn seq_num(&self) -> u8 {
        self.bytes[12]
    }
}

/// Parses a PYO_DEBUG_PACKETS dump: a header line per packet
/// (`<date> <time> Sending/Receiving packet [op N] on socket M`) followed by
/// hex lines (`NNNN : HH HH .. |ascii|`, 8 bytes per line, decimal offsets).
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

fn load_dpl_capture() -> Vec<CapturedPacket> {
    let text = include_str!("golden/dpl_session.txt");
    let packets = parse_capture(text);
    assert!(
        packets.len() >= 40,
        "expected a full session capture, got {} packets",
        packets.len()
    );
    packets
}

/// The response that follows a sent function message (the next received data
/// packet). DPL responses in the capture are single-packet.
fn response_after(packets: &[CapturedPacket], index: usize) -> &CapturedPacket {
    let response = packets[index + 1..]
        .iter()
        .find(|p| !p.sending)
        .expect("response packet after request");
    assert_eq!(response.packet_type(), TNS_PACKET_TYPE_DATA);
    response
}

fn find_function(packets: &[CapturedPacket], function_code: u8, nth: usize) -> usize {
    packets
        .iter()
        .enumerate()
        .filter(|(_, p)| p.is_function(function_code))
        .map(|(i, _)| i)
        .nth(nth)
        .unwrap_or_else(|| panic!("function {function_code} occurrence {nth} not in capture"))
}

fn dpl_golden_column_names() -> Vec<String> {
    [
        "id", "name", "salary", "hired", "updated", "payload", "rating",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

/// Rows loaded by capture 1 of capture_dpl.py, in driver intermediate form.
fn capture_one_rows() -> Vec<Vec<DirectPathColumnValue>> {
    vec![
        vec![
            DirectPathColumnValue::Number("1".into()),
            DirectPathColumnValue::Bytes(b"alpha".to_vec()),
            DirectPathColumnValue::Number("1234.56".into()),
            DirectPathColumnValue::DateTime {
                year: 2024,
                month: 1,
                day: 2,
                hour: 3,
                minute: 4,
                second: 5,
                nanosecond: 0,
            },
            DirectPathColumnValue::DateTime {
                year: 2024,
                month: 1,
                day: 2,
                hour: 3,
                minute: 4,
                second: 5,
                nanosecond: 123_456_000,
            },
            DirectPathColumnValue::Bytes(vec![1, 2, 3]),
            DirectPathColumnValue::BinaryDouble(2.5),
        ],
        vec![
            DirectPathColumnValue::Number("2".into()),
            DirectPathColumnValue::Bytes(b"beta".to_vec()),
            DirectPathColumnValue::Null,
            DirectPathColumnValue::Null,
            DirectPathColumnValue::Null,
            DirectPathColumnValue::Null,
            DirectPathColumnValue::Null,
        ],
        vec![
            DirectPathColumnValue::Number("3".into()),
            DirectPathColumnValue::Bytes(b"gamma".to_vec()),
            DirectPathColumnValue::Number("-0.01".into()),
            DirectPathColumnValue::DateTime {
                year: 1988,
                month: 12,
                day: 31,
                hour: 23,
                minute: 59,
                second: 58,
                nanosecond: 0,
            },
            DirectPathColumnValue::DateTime {
                year: 1988,
                month: 12,
                day: 31,
                hour: 23,
                minute: 59,
                second: 58,
                nanosecond: 999_999_000,
            },
            DirectPathColumnValue::Bytes(vec![0xff; 16]),
            DirectPathColumnValue::BinaryDouble(-1.5),
        ],
    ]
}

#[test]
fn prepare_payload_byte_matches_reference_client() {
    let packets = load_dpl_capture();
    let index = find_function(&packets, TNS_FUNC_DIRECT_PATH_PREPARE, 0);
    let captured = &packets[index];
    let built = build_direct_path_prepare_payload(
        "pythontest",
        "dpl_golden",
        &dpl_golden_column_names(),
        captured.seq_num(),
    )
    .expect("prepare payload should build");
    assert_eq!(captured.data_payload(), built.as_slice());
}

#[test]
fn prepare_response_parses_and_overrides_metadata() {
    let packets = load_dpl_capture();
    let index = find_function(&packets, TNS_FUNC_DIRECT_PATH_PREPARE, 0);
    let response = response_after(&packets, index);
    let result =
        parse_direct_path_prepare_response(response.data_payload(), ClientCapabilities::default())
            .expect("prepare response should parse");

    let names: Vec<&str> = result.column_metadata.iter().map(|c| c.name()).collect();
    assert_eq!(
        names,
        ["ID", "NAME", "SALARY", "HIRED", "UPDATED", "PAYLOAD", "RATING"]
    );
    let types: Vec<u8> = result
        .column_metadata
        .iter()
        .map(|c| c.ora_type_num())
        .collect();
    assert_eq!(
        types,
        [
            ORA_TYPE_NUM_NUMBER,
            ORA_TYPE_NUM_VARCHAR,
            ORA_TYPE_NUM_NUMBER,
            ORA_TYPE_NUM_DATE,
            ORA_TYPE_NUM_TIMESTAMP,
            ORA_TYPE_NUM_RAW,
            ORA_TYPE_NUM_BINARY_DOUBLE,
        ]
    );
    assert!(!result.column_metadata[0].nulls_allowed(), "id is NOT NULL");
    assert!(
        !result.column_metadata[1].nulls_allowed(),
        "name is NOT NULL"
    );
    assert!(result.column_metadata[2].nulls_allowed());
    assert_eq!(result.column_metadata[1].max_size(), 100);
    assert_eq!(result.column_metadata[2].precision(), 9);
    assert_eq!(result.column_metadata[2].scale(), 2);
    assert!(result.cursor_id > 0);
}

#[test]
fn load_stream_payload_byte_matches_reference_client() {
    let packets = load_dpl_capture();
    let prepare_index = find_function(&packets, TNS_FUNC_DIRECT_PATH_PREPARE, 0);
    let prepare = parse_direct_path_prepare_response(
        response_after(&packets, prepare_index).data_payload(),
        ClientCapabilities::default(),
    )
    .expect("prepare response should parse");

    let stream_index = find_function(&packets, TNS_FUNC_DIRECT_PATH_LOAD_STREAM, 0);
    let captured = &packets[stream_index];

    let stream = encode_direct_path_rows(&prepare.column_metadata, &capture_one_rows(), 1)
        .expect("rows should encode");
    let built =
        build_direct_path_load_stream_payload(prepare.cursor_id, &stream, captured.seq_num())
            .expect("load stream payload should build");
    assert_eq!(captured.data_payload(), built.as_slice());

    parse_direct_path_simple_response(
        response_after(&packets, stream_index).data_payload(),
        ClientCapabilities::default(),
    )
    .expect("load stream response should parse");
}

#[test]
fn finish_and_abort_op_payloads_byte_match_reference_client() {
    let packets = load_dpl_capture();
    let prepare_index = find_function(&packets, TNS_FUNC_DIRECT_PATH_PREPARE, 0);
    let prepare = parse_direct_path_prepare_response(
        response_after(&packets, prepare_index).data_payload(),
        ClientCapabilities::default(),
    )
    .expect("prepare response should parse");

    // first op message in the capture is the FINISH of capture 1
    let finish_index = find_function(&packets, TNS_FUNC_DIRECT_PATH_OP, 0);
    let captured_finish = &packets[finish_index];
    let built = build_direct_path_op_payload(
        prepare.cursor_id,
        TNS_DP_OP_FINISH,
        captured_finish.seq_num(),
    );
    assert_eq!(captured_finish.data_payload(), built.as_slice());
    parse_direct_path_simple_response(
        response_after(&packets, finish_index).data_payload(),
        ClientCapabilities::default(),
    )
    .expect("op response should parse");

    // the last op message is the ABORT from capture 4 (DPY-8001 row)
    let abort_index = find_function(&packets, TNS_FUNC_DIRECT_PATH_OP, 3);
    let captured_abort = &packets[abort_index];
    let abort_prepare_index = find_function(&packets, TNS_FUNC_DIRECT_PATH_PREPARE, 3);
    let abort_prepare = parse_direct_path_prepare_response(
        response_after(&packets, abort_prepare_index).data_payload(),
        ClientCapabilities::default(),
    )
    .expect("prepare response should parse");
    let built = build_direct_path_op_payload(
        abort_prepare.cursor_id,
        TNS_DP_OP_ABORT,
        captured_abort.seq_num(),
    );
    assert_eq!(captured_abort.data_payload(), built.as_slice());
}

#[test]
fn batched_load_stream_payloads_byte_match_reference_client() {
    // capture 2: 4 rows with batch_size=2 -> two load stream messages
    let packets = load_dpl_capture();
    let prepare_index = find_function(&packets, TNS_FUNC_DIRECT_PATH_PREPARE, 1);
    let prepare = parse_direct_path_prepare_response(
        response_after(&packets, prepare_index).data_payload(),
        ClientCapabilities::default(),
    )
    .expect("prepare response should parse");

    let rows: Vec<Vec<DirectPathColumnValue>> = (10..14)
        .map(|i| {
            vec![
                DirectPathColumnValue::Number(i.to_string()),
                DirectPathColumnValue::Bytes(format!("r{i}").into_bytes()),
                DirectPathColumnValue::Number(format!("{}.0", i - 9)),
                DirectPathColumnValue::Null,
                DirectPathColumnValue::Null,
                DirectPathColumnValue::Null,
                DirectPathColumnValue::Null,
            ]
        })
        .collect();

    let mut state = BatchLoadState::for_rows(rows.len() as u64, 2).expect("state should build");
    let mut row_num = 1u64;
    for nth in [1usize, 2] {
        assert!(!state.is_done(), "manager must still have rows");
        let start = state.offset() as usize;
        let batch = &rows[start..start + state.num_rows() as usize];
        let stream = encode_direct_path_rows(&prepare.column_metadata, batch, row_num)
            .expect("rows should encode");
        row_num += batch.len() as u64;

        let stream_index = find_function(&packets, TNS_FUNC_DIRECT_PATH_LOAD_STREAM, nth);
        let captured = &packets[stream_index];
        let built =
            build_direct_path_load_stream_payload(prepare.cursor_id, &stream, captured.seq_num())
                .expect("load stream payload should build");
        assert_eq!(
            captured.data_payload(),
            built.as_slice(),
            "batch {nth} mismatch"
        );
        state.next_batch();
    }
    assert!(state.is_done());
}

#[test]
fn long_segment_load_stream_byte_matches_reference_client() {
    // capture 3: 600-byte VARCHAR2 value -> 0xfe chunked segment
    let packets = load_dpl_capture();
    let prepare_index = find_function(&packets, TNS_FUNC_DIRECT_PATH_PREPARE, 2);
    let captured_prepare = &packets[prepare_index];
    let built = build_direct_path_prepare_payload(
        "pythontest",
        "dpl_golden_wide",
        &["id".to_string(), "wide".to_string()],
        captured_prepare.seq_num(),
    )
    .expect("prepare payload should build");
    assert_eq!(captured_prepare.data_payload(), built.as_slice());

    let prepare = parse_direct_path_prepare_response(
        response_after(&packets, prepare_index).data_payload(),
        ClientCapabilities::default(),
    )
    .expect("prepare response should parse");

    let wide_value: String = (0..600)
        .map(|i| char::from(b'a' + (i % 26) as u8))
        .collect();
    let rows = vec![vec![
        DirectPathColumnValue::Number("100".into()),
        DirectPathColumnValue::Bytes(wide_value.into_bytes()),
    ]];
    let stream =
        encode_direct_path_rows(&prepare.column_metadata, &rows, 1).expect("rows should encode");

    let stream_index = find_function(&packets, TNS_FUNC_DIRECT_PATH_LOAD_STREAM, 3);
    let captured = &packets[stream_index];
    let built =
        build_direct_path_load_stream_payload(prepare.cursor_id, &stream, captured.seq_num())
            .expect("load stream payload should build");
    assert_eq!(captured.data_payload(), built.as_slice());
}
