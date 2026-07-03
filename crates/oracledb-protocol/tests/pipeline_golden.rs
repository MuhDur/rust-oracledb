//! Golden-wire tests for the pipelining protocol (BEGIN_PIPELINE piggyback,
//! FUNC 199/200, TOKEN message 33, END_OF_REQUEST framing).
//!
//! `tests/golden/pipeline_session.txt` is a raw PYO_DEBUG_PACKETS dump
//! produced by the REAL python-oracledb 4.0.1 (thin mode, async) running
//! `tests/golden/capture_pipeline.py` against a local Oracle Free 23ai
//! container. Nothing in the capture is scrubbed (throwaway test container).
//!
//! Masking policy: none of the compared bytes are masked. The session-specific
//! TTC sequence numbers are extracted from the capture itself and fed to our
//! builders; everything else must match byte-for-byte.

use oracledb_protocol::thin::{
    build_begin_pipeline_piggyback, build_end_pipeline_payload_with_seq,
    build_execute_payload_with_bind_rows_with_seq_and_token,
    build_function_payload_with_seq_and_token, parse_query_response, BindValue, ClientCapabilities,
    QueryValue, TNS_FUNC_COMMIT, TNS_FUNC_PIPELINE_END, TNS_MSG_TYPE_END_OF_RESPONSE,
    TNS_MSG_TYPE_FUNCTION, TNS_PIPELINE_MODE_ABORT_ON_ERROR, TNS_PIPELINE_MODE_CONTINUE_ON_ERROR,
};

const TNS_PACKET_TYPE_DATA: u8 = 6;
const TNS_PACKET_TYPE_MARKER: u8 = 12;
const TNS_DATA_FLAGS_END_OF_REQUEST: u16 = 0x0800;
const TNS_DATA_FLAGS_BEGIN_PIPELINE: u16 = 0x1000;
const TNS_DATA_FLAGS_END_OF_RESPONSE: u16 = 0x2000;

#[derive(Clone, Debug)]
struct CapturedPacket {
    sending: bool,
    bytes: Vec<u8>,
}

impl CapturedPacket {
    fn packet_type(&self) -> u8 {
        self.bytes[4]
    }

    fn data_flags(&self) -> u16 {
        u16::from_be_bytes([self.bytes[8], self.bytes[9]])
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

fn load_pipeline_capture() -> Vec<CapturedPacket> {
    let text = include_str!("golden/pipeline_session.txt");
    let packets = parse_capture(text);
    assert!(
        packets.len() >= 35,
        "expected a full session capture, got {} packets",
        packets.len()
    );
    packets
}

struct PipelineFrames {
    /// Payloads of the operation messages, in token order. The first one
    /// still carries the begin-pipeline piggyback prefix.
    op_payloads: Vec<Vec<u8>>,
    end_payload: Vec<u8>,
    /// One reassembled response payload per operation plus the end-pipeline
    /// response, in arrival order.
    responses: Vec<Vec<u8>>,
}

/// Extracts the nth pipeline exchange: the run of sent packets starting at
/// the nth BEGIN_PIPELINE-flagged packet (each op packet carries
/// END_OF_REQUEST), the end-pipeline message that follows, and the N+1
/// boundary-delimited responses (received markers are dropped, exactly as the
/// in-pipeline transport does).
fn extract_pipeline(packets: &[CapturedPacket], nth: usize) -> PipelineFrames {
    let begin_index = packets
        .iter()
        .enumerate()
        .filter(|(_, p)| {
            p.sending
                && p.packet_type() == TNS_PACKET_TYPE_DATA
                && p.data_flags() & TNS_DATA_FLAGS_BEGIN_PIPELINE != 0
        })
        .map(|(i, _)| i)
        .nth(nth)
        .unwrap_or_else(|| panic!("begin-pipeline packet occurrence {nth} not in capture"));

    let mut op_payloads = Vec::new();
    let mut index = begin_index;
    while packets[index].sending
        && packets[index].packet_type() == TNS_PACKET_TYPE_DATA
        && packets[index].data_flags() & TNS_DATA_FLAGS_END_OF_REQUEST != 0
    {
        op_payloads.push(packets[index].data_payload().to_vec());
        index += 1;
    }

    let end_packet = &packets[index];
    assert!(end_packet.sending, "end-pipeline message must follow ops");
    let end_payload = end_packet.data_payload().to_vec();
    assert_eq!(
        &end_payload[..2],
        &[TNS_MSG_TYPE_FUNCTION, TNS_FUNC_PIPELINE_END],
        "expected end-pipeline function message"
    );
    index += 1;

    let mut responses = Vec::new();
    let mut current = Vec::new();
    while responses.len() < op_payloads.len() + 1 {
        let packet = &packets[index];
        index += 1;
        assert!(
            !packet.sending,
            "no client packets between pipeline responses"
        );
        if packet.packet_type() == TNS_PACKET_TYPE_MARKER {
            // dropped without a reset exchange while in a pipeline
            continue;
        }
        current.extend_from_slice(packet.data_payload());
        if packet.data_flags() & TNS_DATA_FLAGS_END_OF_RESPONSE != 0
            || current.last() == Some(&TNS_MSG_TYPE_END_OF_RESPONSE)
        {
            responses.push(std::mem::take(&mut current));
        }
    }

    PipelineFrames {
        op_payloads,
        end_payload,
        responses,
    }
}

fn caps() -> ClientCapabilities {
    ClientCapabilities::default()
}

/// Splits the first op payload into (piggyback bytes, function message bytes)
/// using the built piggyback's own length.
fn split_first_payload(payload: &[u8], pipeline_mode: u8) -> (Vec<u8>, Vec<u8>) {
    // the piggyback's seq num is its third byte
    let piggyback = build_begin_pipeline_piggyback(payload[2], 1, pipeline_mode);
    assert!(payload.len() > piggyback.len());
    (
        payload[..piggyback.len()].to_vec(),
        payload[piggyback.len()..].to_vec(),
    )
}

#[test]
fn abort_mode_request_payloads_byte_match_reference_client() {
    let packets = load_pipeline_capture();
    let frames = extract_pipeline(&packets, 0);
    assert_eq!(frames.op_payloads.len(), 4, "capture A has four operations");

    // op 1: begin piggyback (mode 2 = abort on error, token 1) + insert
    let (captured_piggyback, captured_insert) =
        split_first_payload(&frames.op_payloads[0], TNS_PIPELINE_MODE_ABORT_ON_ERROR);
    let built_piggyback =
        build_begin_pipeline_piggyback(captured_piggyback[2], 1, TNS_PIPELINE_MODE_ABORT_ON_ERROR);
    assert_eq!(captured_piggyback, built_piggyback);
    let built = build_execute_payload_with_bind_rows_with_seq_and_token(
        "insert into pipe_golden values (1, 'one')",
        1,
        captured_insert[2],
        false,
        &[],
        1,
        ClientCapabilities::default().ttc_field_version,
    )
    .expect("insert payload should build");
    assert_eq!(captured_insert, built);

    // op 2: plain insert, token 2
    let captured = &frames.op_payloads[1];
    let built = build_execute_payload_with_bind_rows_with_seq_and_token(
        "insert into pipe_golden values (2, 'two')",
        1,
        captured[2],
        false,
        &[],
        2,
        ClientCapabilities::default().ttc_field_version,
    )
    .expect("insert payload should build");
    assert_eq!(captured, &built);

    // op 3: commit, token 3
    let captured = &frames.op_payloads[2];
    let built = build_function_payload_with_seq_and_token(
        TNS_FUNC_COMMIT,
        captured[2],
        3,
        ClientCapabilities::default().ttc_field_version,
    );
    assert_eq!(captured, &built);

    // op 4: fetchall (prefetchrows = arraysize = 100), token 4
    let captured = &frames.op_payloads[3];
    let built = build_execute_payload_with_bind_rows_with_seq_and_token(
        "select id, val from pipe_golden order by id",
        100,
        captured[2],
        true,
        &[],
        4,
        ClientCapabilities::default().ttc_field_version,
    )
    .expect("select payload should build");
    assert_eq!(captured, &built);

    // end-pipeline message
    let built = build_end_pipeline_payload_with_seq(frames.end_payload[2]);
    assert_eq!(frames.end_payload, built);
}

#[test]
fn abort_mode_responses_parse_with_tokens() {
    let packets = load_pipeline_capture();
    let frames = extract_pipeline(&packets, 0);
    assert_eq!(
        frames.responses.len(),
        5,
        "four ops + end-pipeline response"
    );

    for (index, expected_rows) in [(0usize, 1u64), (1, 1)] {
        let result =
            parse_query_response(&frames.responses[index], caps()).expect("insert response");
        assert_eq!(result.token_num, Some(index as u64 + 1));
        assert_eq!(result.row_count, expected_rows);
    }

    let commit = parse_query_response(&frames.responses[2], caps()).expect("commit response");
    assert_eq!(commit.token_num, Some(3));

    let fetched = parse_query_response(&frames.responses[3], caps()).expect("fetchall response");
    assert_eq!(fetched.token_num, Some(4));
    let names: Vec<&str> = fetched.columns.iter().map(|c| c.name()).collect();
    assert_eq!(names, ["ID", "VAL"]);
    let rows: Vec<(String, String)> = fetched
        .rows
        .iter()
        .map(|row| {
            let id = match &row[0] {
                Some(v @ QueryValue::Number(_)) => v.as_number_text().unwrap().into_owned(),
                other => panic!("unexpected id value: {other:?}"),
            };
            let val = match &row[1] {
                Some(QueryValue::Text(text)) => text.clone(),
                other => panic!("unexpected val value: {other:?}"),
            };
            (id, val)
        })
        .collect();
    assert_eq!(
        rows,
        [
            ("1".to_string(), "one".to_string()),
            ("2".to_string(), "two".to_string())
        ]
    );

    // the end-pipeline response is a plain status without a token
    let end = parse_query_response(&frames.responses[4], caps()).expect("end-pipeline response");
    assert_eq!(end.token_num, None);
}

#[test]
fn continue_mode_pipeline_carries_mode_and_surfaces_midstream_error() {
    let packets = load_pipeline_capture();
    let frames = extract_pipeline(&packets, 1);
    assert_eq!(
        frames.op_payloads.len(),
        3,
        "capture B has three operations"
    );

    let (captured_piggyback, _) =
        split_first_payload(&frames.op_payloads[0], TNS_PIPELINE_MODE_CONTINUE_ON_ERROR);
    let built_piggyback = build_begin_pipeline_piggyback(
        captured_piggyback[2],
        1,
        TNS_PIPELINE_MODE_CONTINUE_ON_ERROR,
    );
    assert_eq!(captured_piggyback, built_piggyback);

    assert_eq!(frames.responses.len(), 4);
    let first = parse_query_response(&frames.responses[0], caps()).expect("insert response");
    assert_eq!(first.token_num, Some(1));
    assert_eq!(first.row_count, 1);

    // the second op targets a missing table; its own response carries the
    // server error while later operations still receive answers
    let error = parse_query_response(&frames.responses[1], caps())
        .expect_err("missing-table response is an error");
    assert!(
        error.to_string().contains("ORA-00942"),
        "unexpected error: {error}"
    );

    let count = parse_query_response(&frames.responses[2], caps()).expect("fetchone response");
    assert_eq!(count.token_num, Some(3));
    match &count.rows[0][0] {
        Some(v @ QueryValue::Number(_)) => assert_eq!(v.as_number_text().unwrap(), "3"),
        other => panic!("unexpected count value: {other:?}"),
    }

    let end = parse_query_response(&frames.responses[3], caps()).expect("end-pipeline response");
    assert_eq!(end.token_num, None);
}

#[test]
fn bound_execute_in_pipeline_byte_matches_reference_client() {
    let packets = load_pipeline_capture();
    let frames = extract_pipeline(&packets, 2);
    assert_eq!(frames.op_payloads.len(), 2, "capture C has two operations");

    // the first payload carries begin-pipeline + close-cursors piggybacks
    // before the function message; locate the execute function header and
    // compare the bound insert from there
    let payload = &frames.op_payloads[0];
    let piggyback_len =
        build_begin_pipeline_piggyback(payload[2], 1, TNS_PIPELINE_MODE_ABORT_ON_ERROR).len();
    let function_offset = (piggyback_len..payload.len())
        .find(|&offset| payload[offset] == TNS_MSG_TYPE_FUNCTION && payload[offset + 1] == 0x5e)
        .expect("execute function header after piggybacks");
    let captured = &payload[function_offset..];
    let built = build_execute_payload_with_bind_rows_with_seq_and_token(
        "insert into pipe_golden values (:1, :2)",
        1,
        captured[2],
        false,
        &[vec![
            BindValue::Number("4".to_string()),
            BindValue::Text("four".to_string()),
        ]],
        1,
        ClientCapabilities::default().ttc_field_version,
    )
    .expect("bound insert payload should build");
    assert_eq!(captured, built.as_slice());

    // fetchall response: ids 1..4
    let fetched = parse_query_response(&frames.responses[1], caps()).expect("fetchall response");
    assert_eq!(fetched.token_num, Some(2));
    let ids: Vec<String> = fetched
        .rows
        .iter()
        .map(|row| match &row[0] {
            Some(v @ QueryValue::Number(_)) => v.as_number_text().unwrap().into_owned(),
            other => panic!("unexpected id value: {other:?}"),
        })
        .collect();
    assert_eq!(ids, ["1", "2", "3", "4"]);
}
