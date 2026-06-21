#![forbid(unsafe_code)]

//! Fault injection over representative thin-protocol decode phases.
//!
//! This is the malformed-input companion to `fragmentation_invariance.rs`.
//! The fragmentation suite re-splits valid response streams; this test corrupts
//! golden and synthetic buffers at packet, TTC message, type-tag, marker, and
//! length-prefix boundaries and asserts the public sans-io decoders fail closed:
//! an `Err` or bounded `Ok` is clean, while a panic or length-driven allocation
//! spike is a test failure.

use std::any::Any;
use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Mutex, MutexGuard, OnceLock};

use hex::FromHex;
use oracledb_protocol::dpl::{
    parse_direct_path_prepare_response_with_limits, parse_direct_path_simple_response_with_limits,
    TNS_FUNC_DIRECT_PATH_LOAD_STREAM, TNS_FUNC_DIRECT_PATH_OP, TNS_FUNC_DIRECT_PATH_PREPARE,
};
use oracledb_protocol::oson::{decode_oson_with_limits, encode_oson, OsonValue};
use oracledb_protocol::packet::TnsPacket;
use oracledb_protocol::thin::{
    decode_dbobject_text, decode_lob_text, decode_sessionless_txn_state, encode_lob_text,
    parse_accept_payload, parse_auth_response_with_limits,
    parse_fetch_response_with_context_and_limits, parse_lob_free_temp_response_with_limits,
    parse_lob_read_response_with_limits, parse_plain_function_response_with_limits,
    parse_query_response_borrowed_with_limits, parse_query_response_with_limits,
    parse_tpc_change_state_response_with_limits, parse_tpc_switch_response_with_limits,
    parse_tpc_txn_switch_response_with_limits, ClientCapabilities, ColumnMetadata, QueryValue,
    CS_FORM_IMPLICIT, ORA_TYPE_NUM_NUMBER, TNS_DATA_FLAGS_END_OF_RESPONSE, TNS_FUNC_AUTH_PHASE_ONE,
    TNS_FUNC_AUTH_PHASE_TWO, TNS_FUNC_EXECUTE, TNS_FUNC_FETCH, TNS_FUNC_LOB_OP,
    TNS_FUNC_TPC_TXN_CHANGE_STATE, TNS_FUNC_TPC_TXN_SWITCH, TNS_MSG_TYPE_BIT_VECTOR,
    TNS_MSG_TYPE_DATA_TYPES, TNS_MSG_TYPE_DESCRIBE_INFO, TNS_MSG_TYPE_END_OF_RESPONSE,
    TNS_MSG_TYPE_ERROR, TNS_MSG_TYPE_FUNCTION, TNS_MSG_TYPE_LOB_DATA, TNS_MSG_TYPE_PARAMETER,
    TNS_MSG_TYPE_PIGGYBACK, TNS_MSG_TYPE_PROTOCOL, TNS_MSG_TYPE_ROW_DATA, TNS_MSG_TYPE_ROW_HEADER,
    TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK, TNS_MSG_TYPE_STATUS, TNS_MSG_TYPE_TOKEN,
    TNS_PACKET_TYPE_ACCEPT, TNS_PACKET_TYPE_DATA,
};
use oracledb_protocol::vector::{decode_vector_with_limits, encode_vector, Vector, VectorValues};
use oracledb_protocol::wire::{encode_packet, PacketLengthWidth, ProtocolLimits, TtcWriter};
use oracledb_protocol::ProtocolError;
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

const PROPTEST_CASES: u32 = 1_024;
const TNS_PACKET_TYPE_MARKER: u8 = 12;
const ALLOCATION_COUNT_CEILING: u64 = 8_192;
const ALLOCATION_BYTES_CEILING: u64 = 8 * 1024 * 1024;

static DECODER_CASES: OnceLock<Vec<DecoderCase>> = OnceLock::new();
static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn config() -> ProptestConfig {
    ProptestConfig {
        cases: PROPTEST_CASES,
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

fn test_lock() -> MutexGuard<'static, ()> {
    match TEST_LOCK.get_or_init(|| Mutex::new(())).lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[derive(Clone, Debug)]
struct CapturedPacket {
    sending: bool,
    bytes: Vec<u8>,
}

impl CapturedPacket {
    fn packet_type(&self) -> Option<u8> {
        self.bytes.get(4).copied()
    }

    fn data_flags(&self) -> Option<u16> {
        if self.packet_type()? != TNS_PACKET_TYPE_DATA {
            return None;
        }
        let flags = self.bytes.get(8..10)?;
        let bytes: [u8; 2] = flags.try_into().expect("two bytes for data flags");
        Some(u16::from_be_bytes(bytes))
    }

    fn tns_payload(&self) -> Option<&[u8]> {
        self.bytes.get(8..)
    }

    fn data_payload(&self) -> Option<&[u8]> {
        if self.packet_type()? != TNS_PACKET_TYPE_DATA {
            return None;
        }
        self.bytes.get(10..)
    }

    fn is_function(&self, function_code: u8) -> bool {
        self.sending
            && self.packet_type() == Some(TNS_PACKET_TYPE_DATA)
            && self.data_payload().is_some_and(|payload| {
                payload.len() >= 2
                    && payload[0] == TNS_MSG_TYPE_FUNCTION
                    && payload[1] == function_code
            })
    }

    fn data_payload_contains(&self, needle: &[u8]) -> bool {
        self.sending
            && self.packet_type() == Some(TNS_PACKET_TYPE_DATA)
            && self
                .data_payload()
                .is_some_and(|payload| payload.windows(needle.len()).any(|w| w == needle))
    }
}

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
        let hex_part = rest
            .split('|')
            .next()
            .expect("split always yields the first field");
        for byte_hex in hex_part.split_whitespace() {
            let byte = u8::from_str_radix(byte_hex, 16).expect("valid hex byte in capture");
            packet.bytes.push(byte);
        }
    }
    packets
}

fn first_packet_by_type(packets: &[CapturedPacket], packet_type: u8, sending: bool) -> Vec<u8> {
    let Some(packet) = packets
        .iter()
        .find(|packet| packet.sending == sending && packet.packet_type() == Some(packet_type))
    else {
        panic!("packet type {packet_type} sending={sending} not found in capture");
    };
    packet.bytes.clone()
}

fn first_tns_payload_by_type(
    packets: &[CapturedPacket],
    packet_type: u8,
    sending: bool,
) -> Vec<u8> {
    let Some(packet) = packets
        .iter()
        .find(|packet| packet.sending == sending && packet.packet_type() == Some(packet_type))
    else {
        panic!("packet type {packet_type} sending={sending} not found in capture");
    };
    packet
        .tns_payload()
        .expect("packet has an 8-byte TNS header")
        .to_vec()
}

fn response_payload_after_function(
    packets: &[CapturedPacket],
    function_code: u8,
    nth: usize,
) -> Vec<u8> {
    let mut seen = 0usize;
    let mut index = None;
    for (i, packet) in packets.iter().enumerate() {
        if packet.is_function(function_code) {
            if seen == nth {
                index = Some(i);
                break;
            }
            seen += 1;
        }
    }
    let Some(index) = index else {
        panic!("function {function_code} occurrence {nth} not found in capture");
    };
    collect_response_payload_after(packets, index)
}

fn response_payload_after_sent_packet_containing(
    packets: &[CapturedPacket],
    needle: &[u8],
) -> Vec<u8> {
    let Some(index) = packets
        .iter()
        .position(|packet| packet.data_payload_contains(needle))
    else {
        panic!("sent data packet containing {needle:?} not found in capture");
    };
    collect_response_payload_after(packets, index)
}

fn collect_response_payload_after(packets: &[CapturedPacket], request_index: usize) -> Vec<u8> {
    let mut payload = Vec::new();
    for packet in &packets[request_index + 1..] {
        if packet.sending {
            break;
        }
        if packet.packet_type() == Some(TNS_PACKET_TYPE_DATA) {
            payload.extend_from_slice(
                packet
                    .data_payload()
                    .expect("data packet should expose TTC payload"),
            );
        }
    }
    assert!(
        !payload.is_empty(),
        "no response payload after request index {request_index}"
    );
    payload
}

struct PipelineFrames {
    responses: Vec<Vec<u8>>,
}

fn extract_pipeline(packets: &[CapturedPacket], nth: usize) -> PipelineFrames {
    let mut seen = 0usize;
    let mut begin_index = None;
    for (index, packet) in packets.iter().enumerate() {
        if packet.sending
            && packet.packet_type() == Some(TNS_PACKET_TYPE_DATA)
            && packet.data_flags().is_some_and(|flags| flags & 0x1000 != 0)
        {
            if seen == nth {
                begin_index = Some(index);
                break;
            }
            seen += 1;
        }
    }
    let Some(mut index) = begin_index else {
        panic!("begin-pipeline occurrence {nth} not found in capture");
    };

    let mut op_count = 0usize;
    while index < packets.len()
        && packets[index].sending
        && packets[index].packet_type() == Some(TNS_PACKET_TYPE_DATA)
        && packets[index]
            .data_flags()
            .is_some_and(|flags| flags & 0x0800 != 0)
    {
        op_count += 1;
        index += 1;
    }

    assert!(
        index < packets.len() && packets[index].sending,
        "end-pipeline message should follow pipeline ops"
    );
    index += 1;

    let mut responses = Vec::new();
    let mut current = Vec::new();
    while index < packets.len() && responses.len() < op_count + 1 {
        let packet = &packets[index];
        index += 1;
        if packet.packet_type() == Some(TNS_PACKET_TYPE_MARKER) {
            continue;
        }
        if packet.packet_type() != Some(TNS_PACKET_TYPE_DATA) {
            continue;
        }
        current.extend_from_slice(
            packet
                .data_payload()
                .expect("pipeline data packet should expose TTC payload"),
        );
        let flags_end = packet
            .data_flags()
            .is_some_and(|flags| flags & TNS_DATA_FLAGS_END_OF_RESPONSE != 0);
        if flags_end || current.last() == Some(&TNS_MSG_TYPE_END_OF_RESPONSE) {
            responses.push(std::mem::take(&mut current));
        }
    }
    assert_eq!(
        responses.len(),
        op_count + 1,
        "pipeline response count should match operation count plus end response"
    );
    PipelineFrames { responses }
}

#[derive(Clone, Debug)]
enum DecoderKind {
    TnsPacket,
    FramedQueryWire,
    Accept,
    Auth,
    Query,
    BorrowedQuery {
        columns: Vec<ColumnMetadata>,
    },
    Fetch {
        columns: Vec<ColumnMetadata>,
        previous_row: Option<Vec<Option<QueryValue>>>,
    },
    DplPrepare,
    DplSimple,
    TpcTxnSwitch,
    TpcSwitch,
    TpcChange,
    SessionlessState,
    LobRead {
        locator: Vec<u8>,
    },
    LobFreeTemp {
        returned_parameter_len: usize,
    },
    PlainFunction,
    Vector,
    Oson,
    DbObjectText {
        dbtype_name: &'static str,
    },
    LobText {
        csfrm: u8,
    },
}

#[derive(Clone, Debug)]
struct DecoderCase {
    phase: &'static str,
    name: &'static str,
    bytes: Vec<u8>,
    kind: DecoderKind,
}

impl DecoderCase {
    fn decode(&self, input: &[u8]) -> Result<(), ProtocolError> {
        let limits = limits_for(input.len().max(self.bytes.len()));
        let caps = ClientCapabilities::default();
        match &self.kind {
            DecoderKind::TnsPacket => TnsPacket::parse_with_limits(input, limits).map(|_| ()),
            DecoderKind::FramedQueryWire => decode_framed_query_wire(input, limits),
            DecoderKind::Accept => parse_accept_payload(input).map(|_| ()),
            DecoderKind::Auth => parse_auth_response_with_limits(input, limits).map(|_| ()),
            DecoderKind::Query => parse_query_response_with_limits(input, caps, limits).map(|_| ()),
            DecoderKind::BorrowedQuery { columns } => {
                let result =
                    parse_query_response_borrowed_with_limits(input, caps, columns, None, limits)?;
                result
                    .batch
                    .for_each_row_ref(|_| Ok::<(), ProtocolError>(()))?;
                Ok(())
            }
            DecoderKind::Fetch {
                columns,
                previous_row,
            } => parse_fetch_response_with_context_and_limits(
                input,
                caps,
                columns,
                previous_row.as_deref(),
                limits,
            )
            .map(|_| ()),
            DecoderKind::DplPrepare => {
                parse_direct_path_prepare_response_with_limits(input, caps, limits).map(|_| ())
            }
            DecoderKind::DplSimple => {
                parse_direct_path_simple_response_with_limits(input, caps, limits).map(|_| ())
            }
            DecoderKind::TpcTxnSwitch => {
                parse_tpc_txn_switch_response_with_limits(input, caps, limits).map(|_| ())
            }
            DecoderKind::TpcSwitch => {
                parse_tpc_switch_response_with_limits(input, caps, limits).map(|_| ())
            }
            DecoderKind::TpcChange => {
                parse_tpc_change_state_response_with_limits(input, caps, limits).map(|_| ())
            }
            DecoderKind::SessionlessState => decode_sessionless_txn_state(input).map(|_| ()),
            DecoderKind::LobRead { locator } => {
                parse_lob_read_response_with_limits(input, caps, locator, limits).map(|_| ())
            }
            DecoderKind::LobFreeTemp {
                returned_parameter_len,
            } => parse_lob_free_temp_response_with_limits(
                input,
                caps,
                *returned_parameter_len,
                limits,
            )
            .map(|_| ()),
            DecoderKind::PlainFunction => {
                parse_plain_function_response_with_limits(input, caps, limits).map(|_| ())
            }
            DecoderKind::Vector => decode_vector_with_limits(input, limits).map(|_| ()),
            DecoderKind::Oson => decode_oson_with_limits(input, limits).map(|_| ()),
            DecoderKind::DbObjectText { dbtype_name } => {
                decode_dbobject_text(input, dbtype_name).map(|_| ())
            }
            DecoderKind::LobText { csfrm } => decode_lob_text(input, *csfrm, None).map(|_| ()),
        }
    }
}

fn limits_for(payload_len: usize) -> ProtocolLimits {
    let byte_limit = payload_len.max(512).saturating_add(64);
    ProtocolLimits {
        max_packet_bytes: byte_limit,
        max_frame_bytes: byte_limit,
        max_response_bytes: byte_limit,
        max_columns: 512,
        max_binds: 512,
        max_batch_rows: 16_384,
        max_object_depth: 64,
        max_object_elements: 8_192,
        max_vector_dimensions: 8_192,
        max_lob_chunks: 1_024,
        max_length_prefixed_elements: 8_192,
    }
    .validate()
    .expect("test protocol limits should be valid")
}

fn decoder_cases() -> &'static [DecoderCase] {
    DECODER_CASES.get_or_init(build_decoder_cases)
}

fn build_decoder_cases() -> Vec<DecoderCase> {
    let mut out = Vec::new();

    let fetch_packets = parse_capture(include_str!("golden/fetch_df_session.txt"));
    push_case(
        &mut out,
        DecoderCase {
            phase: "connect",
            name: "accept-payload",
            bytes: first_tns_payload_by_type(&fetch_packets, TNS_PACKET_TYPE_ACCEPT, false),
            kind: DecoderKind::Accept,
        },
    );
    push_case(
        &mut out,
        DecoderCase {
            phase: "framing",
            name: "accept-tns-packet",
            bytes: first_packet_by_type(&fetch_packets, TNS_PACKET_TYPE_ACCEPT, false),
            kind: DecoderKind::TnsPacket,
        },
    );
    push_case(
        &mut out,
        DecoderCase {
            phase: "auth",
            name: "phase-one-response",
            bytes: response_payload_after_sent_packet_containing(
                &fetch_packets,
                b"python-oracledb",
            ),
            kind: DecoderKind::Auth,
        },
    );
    push_case(
        &mut out,
        DecoderCase {
            phase: "auth",
            name: "phase-two-response",
            bytes: response_payload_after_function(&fetch_packets, TNS_FUNC_AUTH_PHASE_TWO, 0),
            kind: DecoderKind::Auth,
        },
    );

    let query_payload =
        response_payload_after_sent_packet_containing(&fetch_packets, b"select * from fdf_golden");
    let query_result = parse_query_response_with_limits(
        &query_payload,
        ClientCapabilities::default(),
        limits_for(query_payload.len()),
    )
    .expect("fetch_df golden response should parse");
    push_case(
        &mut out,
        DecoderCase {
            phase: "execute",
            name: "fetch-df-query-response",
            bytes: query_payload.clone(),
            kind: DecoderKind::Query,
        },
    );
    push_case(
        &mut out,
        DecoderCase {
            phase: "fetch",
            name: "fetch-df-borrowed-response",
            bytes: query_payload.clone(),
            kind: DecoderKind::BorrowedQuery {
                columns: query_result.columns.clone(),
            },
        },
    );
    push_case(
        &mut out,
        DecoderCase {
            phase: "framing",
            name: "fetch-df-framed-multipacket-response",
            bytes: frame_query_response_wire(&query_payload),
            kind: DecoderKind::FramedQueryWire,
        },
    );

    let fetch_columns = vec![
        ColumnMetadata::new("INTCOL", ORA_TYPE_NUM_NUMBER),
        ColumnMetadata::new("NUMBERCOL", ORA_TYPE_NUM_NUMBER),
    ];
    let fetch_previous_row = vec![
        Some(QueryValue::number_from_text("2", true)),
        Some(QueryValue::number_from_text("0.5", false)),
    ];
    push_case(
        &mut out,
        DecoderCase {
            phase: "fetch",
            name: "context-fetch-response",
            bytes: Vec::from_hex("06020101000205dc0001010101000702c1041d")
                .expect("valid fetch response hex"),
            kind: DecoderKind::Fetch {
                columns: fetch_columns,
                previous_row: Some(fetch_previous_row),
            },
        },
    );

    let dbobject_packets = parse_capture(include_str!("golden/dbobject_session.txt"));
    push_case(
        &mut out,
        DecoderCase {
            phase: "dbobject",
            name: "get-string-response",
            bytes: response_payload_after_sent_packet_containing(
                &dbobject_packets,
                &[0x84, 0x01, 0xfe],
            ),
            kind: DecoderKind::Query,
        },
    );
    push_case(
        &mut out,
        DecoderCase {
            phase: "dbobject",
            name: "nchar-text-scalar",
            bytes: vec![0, b'A', 0, b'B'],
            kind: DecoderKind::DbObjectText {
                dbtype_name: "DB_TYPE_NCHAR",
            },
        },
    );

    let dpl_packets = parse_capture(include_str!("golden/dpl_session.txt"));
    push_case(
        &mut out,
        DecoderCase {
            phase: "dpl",
            name: "prepare-response",
            bytes: response_payload_after_function(&dpl_packets, TNS_FUNC_DIRECT_PATH_PREPARE, 0),
            kind: DecoderKind::DplPrepare,
        },
    );
    push_case(
        &mut out,
        DecoderCase {
            phase: "dpl",
            name: "load-stream-response",
            bytes: response_payload_after_function(
                &dpl_packets,
                TNS_FUNC_DIRECT_PATH_LOAD_STREAM,
                0,
            ),
            kind: DecoderKind::DplSimple,
        },
    );
    push_case(
        &mut out,
        DecoderCase {
            phase: "dpl",
            name: "finish-op-response",
            bytes: response_payload_after_function(&dpl_packets, TNS_FUNC_DIRECT_PATH_OP, 0),
            kind: DecoderKind::DplSimple,
        },
    );

    let pipeline_packets = parse_capture(include_str!("golden/pipeline_session.txt"));
    let abort_pipeline = extract_pipeline(&pipeline_packets, 0);
    push_case(
        &mut out,
        DecoderCase {
            phase: "pipeline",
            name: "insert-token-response",
            bytes: abort_pipeline.responses[0].clone(),
            kind: DecoderKind::Query,
        },
    );
    push_case(
        &mut out,
        DecoderCase {
            phase: "pipeline",
            name: "fetchall-token-response",
            bytes: abort_pipeline.responses[3].clone(),
            kind: DecoderKind::Query,
        },
    );
    push_case(
        &mut out,
        DecoderCase {
            phase: "pipeline",
            name: "end-pipeline-response",
            bytes: abort_pipeline.responses[4].clone(),
            kind: DecoderKind::Query,
        },
    );
    let continue_pipeline = extract_pipeline(&pipeline_packets, 1);
    push_case(
        &mut out,
        DecoderCase {
            phase: "pipeline",
            name: "missing-table-error-response",
            bytes: continue_pipeline.responses[1].clone(),
            kind: DecoderKind::Query,
        },
    );

    let sessionless_packets = parse_capture(include_str!("golden/sessionless_session.txt"));
    push_case(
        &mut out,
        DecoderCase {
            phase: "sessionless",
            name: "txn-switch-response",
            bytes: response_payload_after_function(
                &sessionless_packets,
                TNS_FUNC_TPC_TXN_SWITCH,
                0,
            ),
            kind: DecoderKind::TpcTxnSwitch,
        },
    );
    push_case(
        &mut out,
        DecoderCase {
            phase: "sessionless",
            name: "txn-state-keyword-binary",
            bytes: b"golden_8700_txn_id\x40\x01".to_vec(),
            kind: DecoderKind::SessionlessState,
        },
    );

    let tpc_packets = parse_capture(include_str!("golden/tpc_session.txt"));
    push_case(
        &mut out,
        DecoderCase {
            phase: "tpc",
            name: "switch-response",
            bytes: response_payload_after_function(&tpc_packets, TNS_FUNC_TPC_TXN_SWITCH, 0),
            kind: DecoderKind::TpcSwitch,
        },
    );
    push_case(
        &mut out,
        DecoderCase {
            phase: "tpc",
            name: "change-state-response",
            bytes: response_payload_after_function(&tpc_packets, TNS_FUNC_TPC_TXN_CHANGE_STATE, 0),
            kind: DecoderKind::TpcChange,
        },
    );

    let (lob_read_payload, lob_locator) = synthetic_lob_read_response();
    push_case(
        &mut out,
        DecoderCase {
            phase: "lob",
            name: "read-response",
            bytes: lob_read_payload,
            kind: DecoderKind::LobRead {
                locator: lob_locator,
            },
        },
    );
    push_case(
        &mut out,
        DecoderCase {
            phase: "lob",
            name: "free-temp-response",
            bytes: Vec::from_hex(concat!(
                "0800260000020080000002ee5500000044000000030369000a000000000002",
                "5295f656000000010000040101021a390000000000000000000000000000",
                "00000000000a000000000000000000001d",
            ))
            .expect("valid LOB free-temp response hex"),
            kind: DecoderKind::LobFreeTemp {
                returned_parameter_len: 40,
            },
        },
    );
    push_case(
        &mut out,
        DecoderCase {
            phase: "lob",
            name: "clob-text-scalar",
            bytes: encode_lob_text("fault lob text", CS_FORM_IMPLICIT, None),
            kind: DecoderKind::LobText {
                csfrm: CS_FORM_IMPLICIT,
            },
        },
    );
    push_case(
        &mut out,
        DecoderCase {
            phase: "plain-function",
            name: "commit-response",
            bytes: response_payload_after_function(&pipeline_packets, TNS_FUNC_EXECUTE, 0),
            kind: DecoderKind::PlainFunction,
        },
    );

    push_case(
        &mut out,
        DecoderCase {
            phase: "vector",
            name: "dense-f32-image",
            bytes: encode_vector(&Vector::Dense(VectorValues::Float32(vec![1.0, -2.5, 3.25]))),
            kind: DecoderKind::Vector,
        },
    );
    push_case(
        &mut out,
        DecoderCase {
            phase: "oson",
            name: "object-image",
            bytes: encode_oson(
                &OsonValue::Object(vec![
                    ("phase".to_string(), OsonValue::String("fault".to_string())),
                    ("count".to_string(), OsonValue::Number("3".to_string())),
                ]),
                false,
            )
            .expect("OSON fixture should encode"),
            kind: DecoderKind::Oson,
        },
    );

    out
}

fn push_case(cases: &mut Vec<DecoderCase>, case: DecoderCase) {
    if let Err(message) = decode_clean(&case, &case.bytes, "baseline fixture") {
        panic!("{message}");
    }
    cases.push(case);
}

fn synthetic_lob_read_response() -> (Vec<u8>, Vec<u8>) {
    let locator = vec![0x55; 40];
    let mut writer = TtcWriter::new();
    writer.write_u8(TNS_MSG_TYPE_LOB_DATA);
    writer
        .write_bytes_with_length(b"lob-payload")
        .expect("LOB test payload length should encode");
    writer.write_u8(TNS_MSG_TYPE_PARAMETER);
    writer.write_raw(&locator);
    // Oracle signed ub8 representation for the positive amount 11.
    writer.write_raw(&[1, 11]);
    writer.write_u8(TNS_MSG_TYPE_END_OF_RESPONSE);
    (writer.into_bytes(), locator)
}

fn frame_query_response_wire(payload: &[u8]) -> Vec<u8> {
    let cuts = if payload.len() > 3 {
        vec![payload.len() / 3, (payload.len() * 2) / 3]
    } else {
        Vec::new()
    };
    let mut bounds = vec![0usize];
    bounds.extend(
        cuts.into_iter()
            .filter(|&cut| cut > 0 && cut < payload.len()),
    );
    bounds.push(payload.len());
    bounds.sort_unstable();
    bounds.dedup();

    let mut out = Vec::new();
    let last = bounds.len().saturating_sub(2);
    for (index, window) in bounds.windows(2).enumerate() {
        let segment = &payload[window[0]..window[1]];
        let flags = if index == last {
            TNS_DATA_FLAGS_END_OF_RESPONSE
        } else {
            0
        };
        let packet = encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(flags),
            segment,
            PacketLengthWidth::Legacy16,
        )
        .expect("framed query packet should encode");
        out.extend_from_slice(&packet);
    }
    out
}

fn decode_framed_query_wire(input: &[u8], limits: ProtocolLimits) -> Result<(), ProtocolError> {
    let mut offset = 0usize;
    let mut payload = Vec::new();
    while offset < input.len() {
        let packet = TnsPacket::parse_with_limits(&input[offset..], limits)?;
        let declared = declared_legacy_packet_len(&input[offset..])?;
        if packet.packet_type != TNS_PACKET_TYPE_DATA {
            return Err(ProtocolError::UnknownMessageType {
                message_type: packet.packet_type,
                position: offset + 4,
            });
        }
        if packet.payload.len() < 2 {
            return Err(ProtocolError::TtcDecode("data packet missing data flags"));
        }
        let flags = u16::from_be_bytes([packet.payload[0], packet.payload[1]]);
        let ttc_payload = &packet.payload[2..];
        payload.extend_from_slice(ttc_payload);
        offset = offset
            .checked_add(declared)
            .ok_or(ProtocolError::TtcDecode("packet offset overflow"))?;
        if flags & TNS_DATA_FLAGS_END_OF_RESPONSE != 0
            || (declared == 11 && ttc_payload == [TNS_MSG_TYPE_END_OF_RESPONSE])
        {
            break;
        }
    }
    if payload.is_empty() {
        return Err(ProtocolError::TtcDecode("empty framed response"));
    }
    parse_query_response_with_limits(&payload, ClientCapabilities::default(), limits).map(|_| ())
}

fn declared_legacy_packet_len(input: &[u8]) -> Result<usize, ProtocolError> {
    let bytes = input
        .get(..2)
        .ok_or(ProtocolError::TruncatedHeader { got: input.len() })?;
    let len_bytes: [u8; 2] = bytes.try_into().expect("two bytes for packet length");
    Ok(usize::from(u16::from_be_bytes(len_bytes)))
}

fn decode_clean(case: &DecoderCase, input: &[u8], fault: &str) -> Result<(), String> {
    let caught = panic::catch_unwind(AssertUnwindSafe(|| case.decode(input)));
    match caught {
        Ok(_) => Ok(()),
        Err(payload) => Err(format!(
            "{}:{} panicked under {fault}: {}; len={}; bytes={}",
            case.phase,
            case.name,
            panic_payload_message(payload.as_ref()),
            input.len(),
            hex_preview(input)
        )),
    }
}

fn assert_clean(case: &DecoderCase, input: &[u8], fault: &str) {
    if let Err(message) = decode_clean(case, input, fault) {
        panic!("{message}");
    }
}

fn panic_payload_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_string();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "non-string panic payload".to_string()
}

fn hex_preview(bytes: &[u8]) -> String {
    let mut out = String::new();
    for byte in bytes.iter().take(96) {
        write!(&mut out, "{byte:02x}").expect("write to String should not fail");
    }
    if bytes.len() > 96 {
        out.push_str("...");
    }
    out
}

#[derive(Clone, Debug)]
enum ByteMutation {
    Substitute(u8),
    FlipBit(u8),
    FlipAll,
    MarkerOrTag,
}

impl ByteMutation {
    fn apply(&self, original: u8) -> u8 {
        let replacement = match *self {
            Self::Substitute(value) => value,
            Self::FlipBit(bit) => original ^ (1 << (bit % 8)),
            Self::FlipAll => !original,
            Self::MarkerOrTag => {
                if original == TNS_MSG_TYPE_END_OF_RESPONSE {
                    0
                } else {
                    TNS_MSG_TYPE_END_OF_RESPONSE
                }
            }
        };
        if replacement == original {
            original ^ 0x80
        } else {
            replacement
        }
    }
}

fn byte_mutation_strategy() -> impl Strategy<Value = ByteMutation> {
    prop_oneof![
        any::<u8>().prop_map(ByteMutation::Substitute),
        (0u8..8).prop_map(ByteMutation::FlipBit),
        Just(ByteMutation::FlipAll),
        Just(ByteMutation::MarkerOrTag),
    ]
}

fn length_boundary_offsets(bytes: &[u8]) -> Vec<usize> {
    let mut offsets = BTreeSet::new();
    let len = bytes.len();
    if len <= 1024 {
        offsets.extend(0..len);
    } else {
        offsets.extend(0..256.min(len));
        offsets.extend(len.saturating_sub(256)..len);
        offsets.extend((0..len).step_by(8));
    }

    let interesting = [
        0,
        1,
        2,
        3,
        4,
        8,
        0xfc,
        0xfd,
        0xfe,
        0xff,
        TNS_MSG_TYPE_PROTOCOL,
        TNS_MSG_TYPE_DATA_TYPES,
        TNS_MSG_TYPE_FUNCTION,
        TNS_MSG_TYPE_ERROR,
        TNS_MSG_TYPE_ROW_HEADER,
        TNS_MSG_TYPE_ROW_DATA,
        TNS_MSG_TYPE_PARAMETER,
        TNS_MSG_TYPE_STATUS,
        TNS_MSG_TYPE_LOB_DATA,
        TNS_MSG_TYPE_DESCRIBE_INFO,
        TNS_MSG_TYPE_PIGGYBACK,
        TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK,
        TNS_MSG_TYPE_END_OF_RESPONSE,
        TNS_MSG_TYPE_TOKEN,
        TNS_MSG_TYPE_BIT_VECTOR,
        TNS_FUNC_AUTH_PHASE_ONE,
        TNS_FUNC_AUTH_PHASE_TWO,
        TNS_FUNC_EXECUTE,
        TNS_FUNC_FETCH,
        TNS_FUNC_LOB_OP,
        TNS_FUNC_TPC_TXN_SWITCH,
        TNS_FUNC_TPC_TXN_CHANGE_STATE,
        TNS_FUNC_DIRECT_PATH_PREPARE,
        TNS_FUNC_DIRECT_PATH_LOAD_STREAM,
        TNS_FUNC_DIRECT_PATH_OP,
    ];

    for (offset, byte) in bytes.iter().copied().enumerate() {
        if interesting.contains(&byte) {
            for nearby in offset.saturating_sub(4)..(offset + 5).min(len) {
                offsets.insert(nearby);
            }
        }
    }

    offsets.into_iter().collect()
}

fn length_prefix_mutants(bytes: &[u8], offset: usize) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    if offset >= bytes.len() {
        return out;
    }

    let original = bytes[offset];
    for value in [
        0,
        1,
        original.saturating_sub(1),
        original.saturating_add(1),
        0xfc,
        0xfd,
        0xfe,
        0xff,
    ] {
        let mut mutated = bytes.to_vec();
        mutated[offset] = value;
        out.push((
            format!("byte length/tag at {offset} -> {value:#04x}"),
            mutated,
        ));
    }

    if offset + 2 <= bytes.len() {
        let original = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]);
        for value in [
            0,
            1,
            original.saturating_sub(1),
            original.saturating_add(1),
            u16::MAX,
        ] {
            let mut mutated = bytes.to_vec();
            mutated[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
            out.push((format!("u16 length at {offset} -> {value}"), mutated));
        }
    }

    if offset + 4 <= bytes.len() {
        let original = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        for value in [
            0,
            1,
            original.saturating_sub(1),
            original.saturating_add(1),
            bytes.len() as u32,
            bytes.len().saturating_add(1) as u32,
            u32::MAX,
        ] {
            let mut mutated = bytes.to_vec();
            mutated[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
            out.push((format!("u32 length at {offset} -> {value}"), mutated));
        }
    }

    if offset + 5 <= bytes.len() {
        let mut mutated = bytes.to_vec();
        mutated[offset] = 4;
        mutated[offset + 1..offset + 5].copy_from_slice(&u32::MAX.to_be_bytes());
        out.push((format!("Oracle ub4 at {offset} -> u32::MAX"), mutated));
    }

    out
}

fn marker_flag_type_offsets(bytes: &[u8]) -> Vec<usize> {
    let tags = [
        TNS_MSG_TYPE_PROTOCOL,
        TNS_MSG_TYPE_DATA_TYPES,
        TNS_MSG_TYPE_FUNCTION,
        TNS_MSG_TYPE_ERROR,
        TNS_MSG_TYPE_ROW_HEADER,
        TNS_MSG_TYPE_ROW_DATA,
        TNS_MSG_TYPE_PARAMETER,
        TNS_MSG_TYPE_STATUS,
        TNS_MSG_TYPE_LOB_DATA,
        TNS_MSG_TYPE_DESCRIBE_INFO,
        TNS_MSG_TYPE_PIGGYBACK,
        TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK,
        TNS_MSG_TYPE_END_OF_RESPONSE,
        TNS_MSG_TYPE_TOKEN,
        TNS_MSG_TYPE_BIT_VECTOR,
    ];
    let mut offsets = BTreeSet::new();
    for (offset, byte) in bytes.iter().copied().enumerate() {
        if tags.contains(&byte) {
            offsets.insert(offset);
        }
    }
    for header in [4usize, 5, 8, 9] {
        if header < bytes.len() {
            offsets.insert(header);
        }
    }
    offsets.into_iter().collect()
}

#[test]
fn decoder_never_panics_under_truncation_at_any_offset() {
    let _guard = test_lock();
    for case in decoder_cases() {
        for cut in 0..=case.bytes.len() {
            assert_clean(
                case,
                &case.bytes[..cut],
                &format!("truncation at offset {cut}"),
            );
        }
    }
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn decoder_never_panics_under_single_byte_corruption(
        case_seed in any::<usize>(),
        offset_seed in any::<usize>(),
        mutation in byte_mutation_strategy(),
    ) {
        let _guard = test_lock();
        let cases = decoder_cases();
        let case = &cases[case_seed % cases.len()];
        prop_assume!(!case.bytes.is_empty());
        let offset = offset_seed % case.bytes.len();
        let mut mutated = case.bytes.clone();
        let original = mutated[offset];
        mutated[offset] = mutation.apply(original);
        if let Err(message) = decode_clean(
            case,
            &mutated,
            &format!("single-byte corruption at offset {offset}: {mutation:?}"),
        ) {
            return Err(TestCaseError::fail(message));
        }
    }
}

#[test]
fn corrupt_length_prefix_never_allocates_unbounded_or_overreads() {
    let _guard = test_lock();
    for case in decoder_cases() {
        for offset in length_boundary_offsets(&case.bytes) {
            for (description, mutated) in length_prefix_mutants(&case.bytes, offset) {
                let mut failure = None;
                let measured = allocation_counter::measure(|| {
                    if let Err(message) = decode_clean(case, &mutated, &description) {
                        failure = Some(message);
                    }
                });
                if let Some(message) = failure {
                    panic!("{message}");
                }
                assert!(
                    measured.count_total <= ALLOCATION_COUNT_CEILING,
                    "{}:{} allocated {} times for {description}; input len {}; bytes {}",
                    case.phase,
                    case.name,
                    measured.count_total,
                    mutated.len(),
                    hex_preview(&mutated)
                );
                assert!(
                    measured.bytes_total <= ALLOCATION_BYTES_CEILING,
                    "{}:{} allocated {} bytes for {description}; input len {}; bytes {}",
                    case.phase,
                    case.name,
                    measured.bytes_total,
                    mutated.len(),
                    hex_preview(&mutated)
                );
            }
        }
    }
}

#[test]
fn premature_eof_midframe_is_clean_err_not_hang() {
    let _guard = test_lock();
    for case in decoder_cases().iter().filter(|case| {
        matches!(
            case.kind,
            DecoderKind::TnsPacket | DecoderKind::FramedQueryWire
        )
    }) {
        for cut in 0..case.bytes.len() {
            assert_clean(
                case,
                &case.bytes[..cut],
                &format!("premature EOF in framed packet stream at offset {cut}"),
            );
        }
    }
}

#[test]
fn marker_flag_and_type_tag_corruption_is_clean() {
    let _guard = test_lock();
    for case in decoder_cases() {
        for offset in marker_flag_type_offsets(&case.bytes) {
            for value in [0x00, 0x1d, 0x7f, 0xff] {
                let mut mutated = case.bytes.clone();
                mutated[offset] = value;
                assert_clean(
                    case,
                    &mutated,
                    &format!("marker/flag/type-tag corruption at {offset} -> {value:#04x}"),
                );
            }
        }
    }
}
