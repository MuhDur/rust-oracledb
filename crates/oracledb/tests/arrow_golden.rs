//! Golden-wire test for the fetch->Arrow path.
//!
//! `oracledb-protocol/tests/golden/fetch_df_session.txt` is a raw
//! PYO_DEBUG_PACKETS dump of the REAL python-oracledb 4.0.1 running
//! `fetch_df_all` against a local Oracle Free 23ai container; the matching
//! `.meta.txt` sidecar records the arrow schema and values the reference
//! produced. This test feeds the captured server responses through our query
//! parser and arrow builder and asserts the same schema and values.
//!
//! Known reference quirk (verified live, not replicated here): for a column
//! that is a literal NULL (`select null as n from dual`) the reference's
//! arrow path appends nothing, yielding a ZERO-row dataframe even though the
//! row path returns one row of None — and `select null as n, 1 as x from
//! dual` produces a corrupt table whose columns have differing lengths. We
//! materialize a one-row batch with a null instead.
#![cfg(feature = "arrow")]

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Decimal128Type, Float32Type, Float64Type, Int64Type, TimestampMicrosecondType,
    TimestampSecondType,
};
use arrow_array::Array;
use arrow_schema::{DataType, TimeUnit};
use oracledb::arrow::{build_record_batch, ArrowFetchOptions};
use oracledb_protocol::thin::{parse_query_response, ClientCapabilities};

struct CapturedPacket {
    sending: bool,
    bytes: Vec<u8>,
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
        let hex_part = rest.split('|').next().unwrap_or("");
        for byte_hex in hex_part.split_whitespace() {
            let byte = u8::from_str_radix(byte_hex, 16).expect("valid hex byte in capture");
            packet.bytes.push(byte);
        }
    }
    packets
}

/// Returns the concatenated TTC payload of the response to the sent data
/// packet whose payload contains `needle` (header + data flags stripped from
/// each response packet).
fn response_to_request_containing(packets: &[CapturedPacket], needle: &[u8]) -> Vec<u8> {
    let index = packets
        .iter()
        .position(|p| {
            p.sending
                && p.bytes.len() > 12
                && p.bytes[4] == 6
                && p.bytes[10] == 3
                && p.bytes.windows(needle.len()).any(|w| w == needle)
        })
        .unwrap_or_else(|| {
            panic!(
                "no sent function packet contains {:?}",
                String::from_utf8_lossy(needle)
            )
        });
    let mut payload = Vec::new();
    for packet in &packets[index + 1..] {
        if packet.sending {
            break;
        }
        assert_eq!(packet.bytes[4], 6, "expected a data packet response");
        payload.extend_from_slice(&packet.bytes[10..]);
    }
    assert!(!payload.is_empty(), "no response captured for request");
    payload
}

fn load_capture() -> Vec<CapturedPacket> {
    parse_capture(include_str!(
        "../../oracledb-protocol/tests/golden/fetch_df_session.txt"
    ))
}

#[test]
fn captured_fetch_df_all_response_builds_reference_schema_and_values() {
    let packets = load_capture();
    let payload = response_to_request_containing(&packets, b"select * from fdf_golden");
    let result = parse_query_response(&payload, ClientCapabilities::default())
        .expect("captured execute response should parse");
    assert_eq!(result.columns.len(), 11);
    assert_eq!(result.rows.len(), 3);

    let batch = build_record_batch(&result.columns, &result.rows, &ArrowFetchOptions::default())
        .expect("captured rows should convert to arrow");

    // schema recorded in fetch_df_session.meta.txt
    let expected_types: Vec<(&str, DataType)> = vec![
        ("ID", DataType::Int64),
        ("BIG", DataType::Float64),
        ("PRICE", DataType::Float64),
        ("ANYNUM", DataType::Float64),
        ("NAME", DataType::LargeUtf8),
        ("FIXED", DataType::LargeUtf8),
        ("HIRED", DataType::Timestamp(TimeUnit::Second, None)),
        ("UPDATED", DataType::Timestamp(TimeUnit::Microsecond, None)),
        ("PAYLOAD", DataType::LargeBinary),
        ("RATING", DataType::Float64),
        ("SCORE", DataType::Float32),
    ];
    for (index, (name, data_type)) in expected_types.iter().enumerate() {
        let field = batch.schema().field(index).clone();
        assert_eq!(field.name(), name, "column {index} name");
        assert_eq!(field.data_type(), data_type, "column {name} type");
    }

    // row 2 (index 1) is the all-NULL row
    for index in 1..11 {
        assert!(batch.column(index).is_null(1), "column {index} row 2 null");
    }

    let ids = batch.column(0).as_primitive::<Int64Type>();
    assert_eq!(
        (0..3).map(|i| ids.value(i)).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let big = batch.column(1).as_primitive::<Float64Type>();
    assert_eq!(big.value(0), 12345678901234.0);
    assert_eq!(big.value(2), -42.0);
    let price = batch.column(2).as_primitive::<Float64Type>();
    assert_eq!(price.value(0), 12.34);
    assert_eq!(price.value(2), -99.99);
    let anynum = batch.column(3).as_primitive::<Float64Type>();
    assert_eq!(anynum.value(0), 1.5);
    assert_eq!(anynum.value(2), -0.25);
    let names = batch.column(4).as_string::<i64>();
    assert_eq!(names.value(0), "alpha");
    assert_eq!(names.value(2), "gamma");
    let fixed = batch.column(5).as_string::<i64>();
    assert_eq!(fixed.value(0), "ab   ", "CHAR keeps its blank padding");
    assert_eq!(fixed.value(2), "xyz  ");
    let hired = batch.column(6).as_primitive::<TimestampSecondType>();
    assert_eq!(hired.value(0), 1_704_164_645); // 2024-01-02T03:04:05
    assert_eq!(hired.value(2), 599_615_998); // 1988-12-31T23:59:58
    let updated = batch.column(7).as_primitive::<TimestampMicrosecondType>();
    assert_eq!(updated.value(0), 1_704_164_645_123_456);
    assert_eq!(updated.value(2), 599_615_998_999_999);
    let payload_col = batch.column(8).as_binary::<i64>();
    assert_eq!(payload_col.value(0), &[1, 2]);
    assert_eq!(payload_col.value(2), &[0xff; 8]);
    let rating = batch.column(9).as_primitive::<Float64Type>();
    assert_eq!(rating.value(0), 2.5);
    assert_eq!(rating.value(2), -1.5);
    let score = batch.column(10).as_primitive::<Float32Type>();
    assert_eq!(score.value(0), 0.5);
    assert_eq!(score.value(2), -2.0);
}

#[test]
fn captured_fetch_decimals_response_builds_decimal128_columns() {
    let packets = load_capture();
    let payload = response_to_request_containing(&packets, b"select id, price, anynum");
    let result = parse_query_response(&payload, ClientCapabilities::default())
        .expect("captured execute response should parse");
    assert_eq!(result.columns.len(), 3);
    assert_eq!(result.rows.len(), 3);

    let options = ArrowFetchOptions {
        fetch_decimals: true,
        ..ArrowFetchOptions::default()
    };
    let batch = build_record_batch(&result.columns, &result.rows, &options)
        .expect("captured rows should convert to arrow");
    // schema recorded in fetch_df_session.meta.txt (fetch_decimals=True)
    assert_eq!(
        batch.schema().field(0).data_type(),
        &DataType::Decimal128(9, 0)
    );
    assert_eq!(
        batch.schema().field(1).data_type(),
        &DataType::Decimal128(9, 2)
    );
    assert_eq!(batch.schema().field(2).data_type(), &DataType::Float64);

    let id = batch.column(0).as_primitive::<Decimal128Type>();
    assert_eq!(
        (0..3).map(|i| id.value(i)).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let price = batch.column(1).as_primitive::<Decimal128Type>();
    assert_eq!(price.value(0), 1234, "12.34 at scale 2");
    assert!(price.is_null(1));
    assert_eq!(price.value(2), -9999, "-99.99 at scale 2");
}
