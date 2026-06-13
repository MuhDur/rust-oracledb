//! Golden-wire test for the DbObject (PL/SQL record) IN-bind packed image.
//!
//! `tests/golden/dbobject_session.txt` is a raw PYO_DEBUG_PACKETS dump produced
//! by the REAL python-oracledb 4.0.1 (thin mode) running
//! `tests/golden/capture_dbobject.py` against a local Oracle Free 23ai
//! container (lane 1524). Nothing in the capture is scrubbed (throwaway
//! container).
//!
//! The capture binds `PKG_TESTRECORDS.UDT_RECORD` (the exact values used by
//! `test_3211`) into `pkg_TestRecords.GetStringRep`. This test extracts the
//! 62-byte packed object image from the execute send packet (op 12, right after
//! the `>>>MARKER GetStringRep<<<` line) and asserts our encoder reproduces it
//! byte-for-byte from the same attribute values.

use oracledb_protocol::thin::{
    image_begin, image_finalize, image_write_value_bytes, pack_bindvalue_into_image, BindValue,
    CS_FORM_IMPLICIT, ORA_TYPE_NUM_TIMESTAMP,
};

/// Parses every `xxxx : HH HH ... |....|` hex-dump line that follows the named
/// marker until the next packet header or marker, returning the concatenated
/// bytes of the *first* send packet after the marker.
fn extract_marker_send_packet(session: &str, marker: &str) -> Vec<u8> {
    let mut lines = session.lines();
    // advance past the marker
    for line in lines.by_ref() {
        if line.contains(marker) {
            break;
        }
    }
    // the next "Sending packet" begins the execute frame
    for line in lines.by_ref() {
        if line.contains("Sending packet") {
            break;
        }
    }
    let mut bytes = Vec::new();
    for line in lines {
        if line.contains("packet [op") || line.contains(">>>MARKER") {
            break;
        }
        // format: "0400 : 04 01 01 02 0F 47 01 07 |.....G..|"
        let Some((_, rest)) = line.split_once(" : ") else {
            continue;
        };
        let Some((hex, _)) = rest.split_once('|') else {
            continue;
        };
        for token in hex.split_whitespace() {
            if let Ok(byte) = u8::from_str_radix(token, 16) {
                bytes.push(byte);
            }
        }
    }
    bytes
}

/// Locates the packed object image inside a captured execute packet. The outer
/// frame writes the toid (`00 22 02 08` + oid + extent-oid); the image starts at
/// the record header `84 01 FE` and runs for the BE-u32 size at offset 3.
fn extract_object_image(packet: &[u8]) -> Vec<u8> {
    let toid_start = packet
        .windows(4)
        .position(|window| window == [0x00, 0x22, 0x02, 0x08])
        .expect("toid prefix not found in packet");
    let after_toid = &packet[toid_start + 36..]; // 4 + 16 oid + 16 extent
    let header_rel = after_toid
        .windows(3)
        .position(|window| window == [0x84, 0x01, 0xFE])
        .expect("record image header not found");
    let image = &after_toid[header_rel..];
    let total = u32::from_be_bytes([image[3], image[4], image[5], image[6]]) as usize;
    image[..total].to_vec()
}

#[test]
fn dbobject_record_image_matches_reference() {
    let session = include_str!("golden/dbobject_session.txt");
    let packet = extract_marker_send_packet(session, ">>>MARKER GetStringRep<<<");
    let golden = extract_object_image(&packet);

    // Sanity: the documented golden image.
    let expected_hex = "8401fe0000003e02c113144120737472696e6720696e2061207265636f7264\
077874020f010101077874020c0f1a25040000000004000000150400000005";
    assert_eq!(
        hex_encode(&golden),
        expected_hex,
        "golden image extracted from the session does not match the documented capture"
    );

    // Reconstruct the image from the same record values (test_3211). Attribute
    // order is the declared order: NUMBER, STRING, DATE, TIMESTAMP, BOOLEAN,
    // PLS_INTEGER, BINARY_INTEGER.
    let mut image = image_begin(false);
    let attrs: [(BindValue, u8); 7] = [
        (BindValue::Number("18".into()), CS_FORM_IMPLICIT),
        (
            BindValue::Text("A string in a record".into()),
            CS_FORM_IMPLICIT,
        ),
        (
            BindValue::DateTime {
                year: 2016,
                month: 2,
                day: 15,
                hour: 0,
                minute: 0,
                second: 0,
            },
            CS_FORM_IMPLICIT,
        ),
        (
            BindValue::Timestamp {
                ora_type_num: ORA_TYPE_NUM_TIMESTAMP,
                year: 2016,
                month: 2,
                day: 12,
                hour: 14,
                minute: 25,
                second: 36,
                nanosecond: 0,
            },
            CS_FORM_IMPLICIT,
        ),
        (BindValue::Boolean(false), CS_FORM_IMPLICIT),
        (BindValue::BinaryInteger("21".into()), CS_FORM_IMPLICIT),
        (BindValue::BinaryInteger("5".into()), CS_FORM_IMPLICIT),
    ];
    for (value, csfrm) in &attrs {
        pack_bindvalue_into_image(&mut image, value, *csfrm).unwrap();
    }
    image_finalize(&mut image).unwrap();

    assert_eq!(
        hex_encode(&image),
        expected_hex,
        "encoder output does not match the reference packed image byte-for-byte"
    );

    // image_write_value_bytes round-trips a short value with the 252 short form.
    let mut probe = Vec::new();
    image_write_value_bytes(&mut probe, b"abc").unwrap();
    assert_eq!(probe, vec![3, b'a', b'b', b'c']);
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
