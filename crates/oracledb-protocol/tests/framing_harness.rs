//! Multi-packet / chunk-boundary framing harness — the wide-row bug class.
//!
//! A query response is split across many TNS DATA packets on the wire; the
//! driver concatenates each packet's TTC payload (after the 8-byte header and
//! 2-byte data flags) into one buffer and decides where the response ends, then
//! the sans-io codec (`parse_query_response`) decodes the whole buffer.
//!
//! The correctness net here is SPLIT-INVARIANCE: the decoded row set must be
//! identical no matter WHERE the packet boundaries fall. This is exactly the
//! wide-row reassembly bug class (bead rust-oracledb-n2s): for a wide,
//! multi-packet result, an ordinary payload byte that happens to equal
//! `TNS_MSG_TYPE_END_OF_RESPONSE` (29 / 0x1d) at a packet boundary made the
//! naive reassembler stop mid-stream, truncating the buffer; the TTC decoder
//! then mis-framed the continuation ("encoded NUMBER too long" / "truncated TTC
//! payload").
//!
//! Reference end-of-response rule (`impl/thin/packet.pyx::Packet.
//! has_end_of_response`, lines 58-73): the END_OF_RESPONSE / EOF data flag, OR a
//! trailing 0x1d byte that arrives as its OWN minimal packet — a DATA packet
//! whose entire post-flags payload is exactly that one byte
//! (`packet_size == PACKET_HEADER_SIZE + 3`). The size guard is load-bearing.
//!
//! This is a sans-io harness: it re-frames a real captured 11-column / 3-row
//! query response (golden/fetch_df_session.txt — the test_8000 dataframe
//! sentinel's wire) at every possible split offset into two and three DATA-
//! packet segments and asserts the re-parsed `QueryResult` is byte-identical to
//! the whole-buffer parse, regardless of split position.

use oracledb_protocol::thin::{
    parse_query_response, ClientCapabilities, QueryResult, TNS_DATA_FLAGS_END_OF_RESPONSE,
    TNS_MSG_TYPE_END_OF_RESPONSE,
};

// ---------------------------------------------------------------------------
// Capture parsing (PYO_DEBUG_PACKETS dump) — same format the golden tests use.
// ---------------------------------------------------------------------------

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

/// Concatenated TTC payload (header + data flags stripped) of the response to
/// the sent function packet whose bytes contain `needle`.
fn response_payload_for(packets: &[CapturedPacket], needle: &[u8]) -> Vec<u8> {
    let index = packets
        .iter()
        .position(|p| {
            p.sending
                && p.bytes.len() > 12
                && p.bytes[4] == 6
                && p.bytes[10] == 3
                && p.bytes.windows(needle.len()).any(|w| w == needle)
        })
        .unwrap_or_else(|| panic!("no sent function packet contains {needle:?}"));
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

/// The golden 11-column / 3-row query response payload (reassembled), and the
/// `QueryResult` of parsing it whole — the oracle every re-framing must match.
fn golden_response() -> (Vec<u8>, QueryResult) {
    let packets = parse_capture(include_str!("golden/fetch_df_session.txt"));
    let payload = response_payload_for(&packets, b"select * from fdf_golden");
    let result = parse_query_response(&payload, ClientCapabilities::default())
        .expect("whole-buffer parse of the captured response");
    assert_eq!(result.columns.len(), 11, "11-column wide row expected");
    assert_eq!(result.rows.len(), 3, "3 rows expected");
    (payload, result)
}

// ---------------------------------------------------------------------------
// Sans-io reassembler under test.
// ---------------------------------------------------------------------------

const TNS_HEADER_LEN: usize = 8;
const TNS_DATA_PACKET_TYPE: u8 = 6;

/// The reference-correct end-of-response predicate
/// (`Packet.has_end_of_response`, packet.pyx:58-73). `whole_packet_len` is the
/// full on-wire DATA packet length (header + 2 flag bytes + post-flags payload).
fn has_end_of_response_correct(
    flags: u16,
    payload_after_flags: &[u8],
    whole_packet_len: usize,
) -> bool {
    if flags & TNS_DATA_FLAGS_END_OF_RESPONSE != 0 {
        return true;
    }
    // A lone trailing 0x1d is the end-of-response ONLY when it arrived as its
    // own minimal packet: header + 2 flag bytes + 1 payload byte.
    whole_packet_len == TNS_HEADER_LEN + 3 && payload_after_flags == [TNS_MSG_TYPE_END_OF_RESPONSE]
}

/// The buggy predicate (bead rust-oracledb-n2s): treats ANY DATA packet whose
/// post-flags payload merely ENDS in 0x1d as the end of the response, ignoring
/// the packet size. Kept here so the harness can demonstrate the exact
/// mis-framing the size guard fixes.
fn has_end_of_response_naive(flags: u16, payload_after_flags: &[u8]) -> bool {
    if flags & TNS_DATA_FLAGS_END_OF_RESPONSE != 0 {
        return true;
    }
    payload_after_flags.last() == Some(&TNS_MSG_TYPE_END_OF_RESPONSE)
}

/// One framed DATA packet: full on-wire bytes (header + 2 flag bytes + payload).
fn frame_data_packet(flags: u16, payload: &[u8]) -> Vec<u8> {
    let len = TNS_HEADER_LEN + 2 + payload.len();
    let mut out = Vec::with_capacity(len);
    // 2-byte length (legacy) — only the low byte path matters for the harness
    // since we read packets from a Vec, not by length; we still write a header.
    out.extend_from_slice(&(len as u32).to_be_bytes());
    out.push(TNS_DATA_PACKET_TYPE);
    out.push(0); // flags
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&flags.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Reassemble a sequence of framed DATA packets into the single TTC payload the
/// codec consumes, using the given end-of-response predicate. Returns the
/// reassembled buffer (the bytes accumulated until the predicate fired).
fn reassemble(
    framed_packets: &[Vec<u8>],
    end_of_response: impl Fn(u16, &[u8], usize) -> bool,
) -> Vec<u8> {
    let mut buffer = Vec::new();
    for framed in framed_packets {
        let flags = u16::from_be_bytes([framed[TNS_HEADER_LEN], framed[TNS_HEADER_LEN + 1]]);
        let payload = &framed[TNS_HEADER_LEN + 2..];
        buffer.extend_from_slice(payload);
        if end_of_response(flags, payload, framed.len()) {
            break;
        }
    }
    buffer
}

/// Split a response payload into `n` DATA-packet segments at the given offsets,
/// frame each segment as a DATA packet, and set the END_OF_RESPONSE flag on the
/// last segment (the way the server signals the final packet). The original
/// trailing 0x1d in the payload stays where it is — interior segments end on
/// whatever byte the split lands on, exercising the boundary case.
fn frame_split(payload: &[u8], cut_points: &[usize]) -> Vec<Vec<u8>> {
    let mut bounds = vec![0usize];
    bounds.extend_from_slice(cut_points);
    bounds.push(payload.len());
    bounds.sort_unstable();
    bounds.dedup();
    let segments: Vec<&[u8]> = bounds.windows(2).map(|w| &payload[w[0]..w[1]]).collect();
    let last = segments.len() - 1;
    segments
        .iter()
        .enumerate()
        .map(|(i, seg)| {
            let flags = if i == last {
                TNS_DATA_FLAGS_END_OF_RESPONSE
            } else {
                0
            };
            frame_data_packet(flags, seg)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// THE HARNESS: split at every offset, reassemble, assert identical.
// ---------------------------------------------------------------------------

/// Split the golden wide-row response at EVERY offset 1..len into two segments,
/// reassemble with the reference-correct rule, and assert the re-parsed
/// `QueryResult` equals the whole-buffer parse. The server flags the last
/// segment END_OF_RESPONSE, so the correct rule never stops early — the decoded
/// row set must be split-invariant.
#[test]
fn wide_row_two_way_split_at_every_offset_is_invariant() {
    let (payload, golden) = golden_response();
    let caps = ClientCapabilities::default();
    for cut in 1..payload.len() {
        let framed = frame_split(&payload, &[cut]);
        let reassembled = reassemble(&framed, has_end_of_response_correct);
        let result = parse_query_response(&reassembled, caps).unwrap_or_else(|e| {
            panic!("split at offset {cut} failed to parse: {e:?}");
        });
        assert_eq!(
            result, golden,
            "two-way split at offset {cut} decoded a different result"
        );
    }
}

/// Same, but split into THREE segments at a representative sweep of offset
/// pairs (every offset paired with a few others) — the cross-product of all
/// pairs is O(n^2) parses, so we stride the second cut to keep it fast while
/// still covering boundary interactions across two interior packet edges.
#[test]
fn wide_row_three_way_split_is_invariant() {
    let (payload, golden) = golden_response();
    let caps = ClientCapabilities::default();
    let len = payload.len();
    for a in (1..len).step_by(7) {
        for b in ((a + 1)..len).step_by(11) {
            let framed = frame_split(&payload, &[a, b]);
            let reassembled = reassemble(&framed, has_end_of_response_correct);
            let result = parse_query_response(&reassembled, caps).unwrap_or_else(|e| {
                panic!("three-way split at offsets {a},{b} failed to parse: {e:?}");
            });
            assert_eq!(
                result, golden,
                "three-way split at offsets {a},{b} decoded a different result"
            );
        }
    }
}

/// Frame the real golden wide-row response the way the server sends a
/// MULTI-PACKET result, with the body split across interior DATA packets and
/// the final `0x1d` END_OF_RESPONSE marker arriving as its OWN minimal packet
/// (header + 2 flag bytes + 1 byte). This is the faithful framing for which the
/// reference's size-guarded rule is correct. The reassembled-correct buffer is
/// exactly the original golden payload, so it still parses to the golden 3 rows.
fn golden_framed_multipacket(interior_cut: usize) -> (Vec<Vec<u8>>, Vec<u8>, QueryResult) {
    let (payload, golden) = golden_response();
    assert_eq!(payload.last(), Some(&TNS_MSG_TYPE_END_OF_RESPONSE));
    let body = &payload[..payload.len() - 1]; // everything before the final marker
    let cut = interior_cut.clamp(1, body.len() - 1);
    let framed = vec![
        frame_data_packet(0, &body[..cut]),
        frame_data_packet(0, &body[cut..]),
        // the real end-of-response, as its own minimal packet (header + 3 bytes)
        frame_data_packet(0, &[TNS_MSG_TYPE_END_OF_RESPONSE]),
    ];
    (framed, payload, golden)
}

/// Sanity: with the correct size-guarded rule, framing the golden response as
/// body-packets + a minimal final marker packet reassembles to exactly the
/// original payload and parses to the golden 3 rows — for ANY interior cut.
#[test]
fn minimal_marker_packet_reassembles_to_golden() {
    let caps = ClientCapabilities::default();
    let body_len = golden_response().0.len() - 1; // payload minus the final marker
    for cut in 1..body_len {
        let (framed, expected, golden) = golden_framed_multipacket(cut);
        let reassembled = reassemble(&framed, has_end_of_response_correct);
        assert_eq!(
            reassembled, expected,
            "cut {cut}: correct rule keeps full buffer"
        );
        let result = parse_query_response(&reassembled, caps)
            .unwrap_or_else(|e| panic!("cut {cut}: parse failed {e:?}"));
        assert_eq!(result, golden, "cut {cut}: golden rows");
    }
}

// ---------------------------------------------------------------------------
// n2s demonstration: the NAIVE reassembler mis-frames. Marked #[ignore] with a
// bead reference; it flips to passing once the size-guard rule is adopted in the
// driver's reassembler (bead rust-oracledb-n2s owns that fix).
// ---------------------------------------------------------------------------

/// CHARACTERIZATION of the n2s bug (now FIXED in the driver). We frame a
/// multi-packet response where an INTERIOR DATA packet's post-flags payload ends
/// in an ordinary `0x1d` byte — exactly what happens past ~1500 rows when a
/// packet boundary lands on a normal row-data byte equal to
/// `TNS_MSG_TYPE_END_OF_RESPONSE`. The body bytes are held constant; only the
/// FRAMING varies, so this models the wire faithfully (the reassembler
/// concatenates post-flags payloads verbatim).
///
/// The NAIVE rule — which mirrored the pre-n2s driver reassembler
/// (`data_packet_ends_response` before its `packet_size == header + 3` size
/// guard) — TRUNCATES at the interior packet. This test pins that failure mode
/// so the contrast is documented and the regression cannot silently reappear.
/// Its companion `interior_0x1d_is_handled_by_size_guarded_rule` proves the
/// reference-correct (size-guarded) rule consumes the whole response — that is
/// the behavior bead n2s shipped to the driver (guarded live by the driver
/// crate's `wide_row_multipacket` test and `data_packet_ends_response` units).
#[test]
fn naive_rule_truncates_on_interior_0x1d_n2s_characterization() {
    let (payload, _golden) = golden_response();
    let body = &payload[..payload.len() - 1];

    // Frame so the FIRST interior packet's payload ends in an ordinary 0x1d.
    let k = body.len() / 2;
    let mut first = body[..k].to_vec();
    first.push(TNS_MSG_TYPE_END_OF_RESPONSE); // ordinary byte at the boundary
    let second = body[k..].to_vec();
    let framed = vec![
        frame_data_packet(0, &first),
        frame_data_packet(0, &second),
        // the real end-of-response, as its own minimal packet (header + 3)
        frame_data_packet(0, &[TNS_MSG_TYPE_END_OF_RESPONSE]),
    ];
    let full_len = first.len() + second.len() + 1;

    // Characterization: the naive rule stops at the first interior packet whose
    // payload ends in 0x1d, truncating the response. This is the n2s bug; the
    // driver now uses the size-guarded rule (see the companion test). Pinning the
    // truncation keeps the contrast explicit and the regression visible.
    let reassembled = reassemble(&framed, |flags, payload, _len| {
        has_end_of_response_naive(flags, payload)
    });
    assert!(
        reassembled.len() < full_len,
        "characterization (n2s): the naive rule must truncate on an interior \
         0x1d — got {} of {} bytes; if this no longer truncates the naive copy \
         has drifted from the documented pre-fix behavior",
        reassembled.len(),
        full_len
    );
}

/// The same invariant, but asserted against the REFERENCE-CORRECT rule — this
/// one PASSES today and is the green guard. It proves the size-guarded rule
/// handles an interior 0x1d at a packet boundary without truncating, which is
/// precisely the behavior bead n2s brings to the driver's reassembler.
#[test]
fn interior_0x1d_is_handled_by_size_guarded_rule() {
    let (payload, _golden) = golden_response();
    let body = &payload[..payload.len() - 1];
    let k = body.len() / 2;
    let mut first = body[..k].to_vec();
    first.push(TNS_MSG_TYPE_END_OF_RESPONSE);
    let second = body[k..].to_vec();
    let framed = vec![
        frame_data_packet(0, &first),
        frame_data_packet(0, &second),
        frame_data_packet(0, &[TNS_MSG_TYPE_END_OF_RESPONSE]),
    ];
    let full_len = first.len() + second.len() + 1;
    let reassembled = reassemble(&framed, has_end_of_response_correct);
    assert_eq!(
        reassembled.len(),
        full_len,
        "size-guarded rule must NOT stop on an interior 0x1d"
    );
}
