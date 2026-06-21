//! Fragmentation-invariance properties on the pure (sans-io) decode path.
//!
//! INVARIANT UNDER TEST (bead W3-E5.2): "every legal split yields the same
//! decoded result." A query response arrives on the wire as a stream of TNS
//! DATA packets; the driver concatenates each packet's post-flags TTC payload
//! into one buffer (stopping at the end-of-response marker) and then the
//! sans-io codec (`parse_query_response` / `parse_query_response_borrowed`)
//! decodes the whole buffer in one shot. The protocol crate has NO incremental
//! reader — decoding is one-shot over a full buffer — so the layer where
//! fragment boundaries actually matter is the packet REASSEMBLY: bytes ->
//! packets -> concatenated body -> decode. The property is therefore
//! split-INVARIANCE of reassembly + decode:
//!
//!     decode(reassemble(split(stream, P))) == decode(stream)   for all P.
//!
//! This is the randomized, strategy-driven sibling of the example-based
//! `framing_harness.rs` (which sweeps a single golden payload at every offset
//! with deterministic loops). Here proptest generates an ARBITRARY set of split
//! points and asserts byte-for-byte / semantic identity regardless of where the
//! splits fall. The reference end-of-response rule is the size-guarded predicate
//! (`impl/thin/packet.pyx::Packet.has_end_of_response`, lines 58-73): the
//! END_OF_RESPONSE / EOF data flag, OR a lone trailing `0x1d` that arrives as
//! its OWN minimal packet (`packet_size == header + 3`). The size guard is the
//! n2s fix (bead rust-oracledb-n2s): an ordinary payload byte equal to `0x1d`
//! landing on an interior packet boundary must NOT be mistaken for the marker.
//!
//! Method (skill: testing-metamorphic): each property is a split-INVARIANCE
//! metamorphic relation with no external oracle — the whole-buffer decode is the
//! oracle, and the relation is that any legal fragmentation reproduces it.
//!
//! NON-TRIVIALITY: the reassembler does NOT merely buffer everything before
//! decoding — it stops the moment the end-of-response predicate fires, so the
//! framing is load-bearing. The captured body happens to contain no interior
//! `0x1d` byte (only the trailing marker), so the flag-terminated split
//! properties below prove that framing carries no semantic information but do
//! not by themselves discriminate the size guard. Two further properties close
//! that gap: `lone_marker_wire_form_*` delivers the terminator as its own
//! minimal packet (exercising the guard's POSITIVE branch under arbitrary
//! splits), and `interior_marker_byte_does_not_truncate` FORCES an ordinary
//! `0x1d` onto an interior packet boundary and pins that the size-guarded rule
//! keeps going where the naive pre-n2s last-byte rule would truncate — the
//! exact n2s mis-frame a buffer-everything reader would mask. (A mutation of
//! `has_end_of_response_correct` to the naive rule makes that property fail,
//! confirming it is not a tautology.)

use oracledb_protocol::thin::{
    parse_query_response, parse_query_response_borrowed, ClientCapabilities, ColumnMetadata,
    QueryResult, QueryValue, QueryValueRef, TNS_DATA_FLAGS_END_OF_RESPONSE,
    TNS_MSG_TYPE_END_OF_RESPONSE,
};
use proptest::prelude::*;

/// Budget mirrors `codec_properties.rs` (the established split-invariance
/// precedent in this crate). Each case reassembles + decodes a real 11-column /
/// 3-row capture, so this stays well under a second.
const CASES: u32 = 1_024;

fn config() -> ProptestConfig {
    ProptestConfig {
        cases: CASES,
        ..ProptestConfig::default()
    }
}

// ---------------------------------------------------------------------------
// Capture parsing (PYO_DEBUG_PACKETS dump) — identical format to the golden
// tests and `framing_harness.rs`; kept local so this file is self-contained.
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

/// The golden 11-column / 3-row query-response payload (reassembled whole), and
/// the `QueryResult` of parsing it in one shot — the oracle every re-framing
/// must reproduce.
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
// Sans-io reassembler under test (mirrors the driver's reassembly: concatenate
// post-flags payloads, stop at the size-guarded end-of-response marker).
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

/// One framed DATA packet: full on-wire bytes (header + 2 flag bytes + payload).
fn frame_data_packet(flags: u16, payload: &[u8]) -> Vec<u8> {
    let len = TNS_HEADER_LEN + 2 + payload.len();
    let mut out = Vec::with_capacity(len);
    out.extend_from_slice(&(len as u32).to_be_bytes());
    out.push(TNS_DATA_PACKET_TYPE);
    out.push(0); // header flags
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&flags.to_be_bytes()); // 2-byte TTC data flags
    out.extend_from_slice(payload);
    out
}

/// Reassemble framed DATA packets into the single TTC payload the codec
/// consumes, stopping at the first packet for which `end_of_response` fires.
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

/// Normalize an arbitrary `Vec<usize>` of split seeds into a sorted, deduped set
/// of cut points strictly inside `1..len` (a cut at 0 or `len` is a no-op edge).
/// This is what turns the proptest's free-form seed vector into a LEGAL set of
/// interior packet boundaries.
fn cut_points(len: usize, seeds: &[usize]) -> Vec<usize> {
    if len == 0 {
        return Vec::new();
    }
    let mut cuts: Vec<usize> = seeds
        .iter()
        .map(|&seed| 1 + (seed % len)) // map into 1..=len
        .filter(|&c| c < len) // keep interior only
        .collect();
    cuts.sort_unstable();
    cuts.dedup();
    cuts
}

/// Split `payload` into DATA-packet segments at `cuts`, frame each as a DATA
/// packet, and set END_OF_RESPONSE on the LAST segment (how the server flags the
/// final packet of a response). Interior segments carry a 0 data flag and end on
/// whatever byte the cut lands on — exercising the boundary case.
fn frame_split(payload: &[u8], cuts: &[usize]) -> Vec<Vec<u8>> {
    let mut bounds = vec![0usize];
    bounds.extend_from_slice(cuts);
    bounds.push(payload.len());
    bounds.sort_unstable();
    bounds.dedup();
    let segments: Vec<&[u8]> = bounds.windows(2).map(|w| &payload[w[0]..w[1]]).collect();
    let last = segments.len().saturating_sub(1);
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

/// Split `payload` into `0`-flagged DATA-packet segments at `cuts` (NO
/// END_OF_RESPONSE flag on any segment). Used to model the body of a faithful
/// multi-packet response whose terminator arrives as a separate lone-marker
/// packet, so reassembly stops only via the size guard, never via the flag.
fn frame_split_no_terminator(payload: &[u8], cuts: &[usize]) -> Vec<Vec<u8>> {
    let mut bounds = vec![0usize];
    bounds.extend_from_slice(cuts);
    bounds.push(payload.len());
    bounds.sort_unstable();
    bounds.dedup();
    bounds
        .windows(2)
        .map(|w| frame_data_packet(0, &payload[w[0]..w[1]]))
        .collect()
}

/// Materialize the borrowed read path's rows as owned values so they can be
/// compared cell-for-cell against the owned `parse_query_response` rows. The
/// borrowed path (`parse_query_response_borrowed` + `for_each_row_ref`) is the
/// zero-copy fetch decoder; this asserts it decodes a fragmented-then-reassembled
/// buffer to exactly the same rows as the owned path.
fn borrowed_rows(
    payload: &[u8],
    columns: &[ColumnMetadata],
) -> Result<Vec<Vec<Option<QueryValue>>>, oracledb_protocol::ProtocolError> {
    let fetch =
        parse_query_response_borrowed(payload, ClientCapabilities::default(), columns, None)?;
    let mut rows: Vec<Vec<Option<QueryValue>>> = Vec::new();
    fetch
        .batch
        .for_each_row_ref(|cells: &[Option<QueryValueRef<'_>>]| {
            rows.push(
                cells
                    .iter()
                    .map(|c| c.map(|v| v.to_owned_value()))
                    .collect(),
            );
            Ok::<(), oracledb_protocol::ProtocolError>(())
        })?;
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Strategies.
// ---------------------------------------------------------------------------

/// An arbitrary set of split seeds. Each seed is reduced modulo the payload
/// length to a legal interior cut, so this generates 0..=16 split points
/// anywhere in the stream. 0 splits = the whole buffer in one packet (the
/// degenerate but legal case); 16 = a heavily fragmented stream.
fn split_seeds() -> impl Strategy<Value = Vec<usize>> {
    prop::collection::vec(0usize..100_000, 0..=16)
}

// ---------------------------------------------------------------------------
// PROPERTIES.
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(config())]

    /// SPLIT-INVARIANCE OF REASSEMBLY (byte level). For any set of split points,
    /// framing the golden payload into that many DATA packets (last flagged
    /// END_OF_RESPONSE) and reassembling with the size-guarded rule reproduces
    /// the original payload BYTE-FOR-BYTE. The size-guarded rule never stops on
    /// an interior ordinary `0x1d`, so reassembly is split-invariant: the wire
    /// invariant is that packet framing carries no semantic information — only
    /// the flags and the lone-marker packet do.
    #[test]
    fn reassembly_is_byte_identical_under_arbitrary_splits(seeds in split_seeds()) {
        let (payload, _golden) = golden_response();
        let cuts = cut_points(payload.len(), &seeds);
        let framed = frame_split(&payload, &cuts);
        let reassembled = reassemble(&framed, has_end_of_response_correct);
        prop_assert_eq!(
            &reassembled, &payload,
            "reassembly differs from the whole stream for cuts {:?}", cuts
        );
    }

    /// SPLIT-INVARIANCE OF THE OWNED DECODE. The decoded `QueryResult` of the
    /// reassembled-from-fragments buffer equals the whole-buffer decode for any
    /// split set. This is the headline property: "every legal split yields the
    /// same decoded result." `QueryResult` derives `PartialEq`, so the columns,
    /// every row, the cursor id, and the row count must all match.
    #[test]
    fn owned_decode_is_invariant_under_arbitrary_splits(seeds in split_seeds()) {
        let (payload, golden) = golden_response();
        let caps = ClientCapabilities::default();
        let cuts = cut_points(payload.len(), &seeds);
        let framed = frame_split(&payload, &cuts);
        let reassembled = reassemble(&framed, has_end_of_response_correct);
        let result = parse_query_response(&reassembled, caps)
            .unwrap_or_else(|e| panic!("decode failed for cuts {cuts:?}: {e:?}"));
        prop_assert_eq!(
            result, golden,
            "owned decode differs from the whole-stream decode for cuts {:?}", cuts
        );
    }

    /// SPLIT-INVARIANCE OF THE BORROWED (zero-copy) READ PATH. The borrowed
    /// fetch decoder (`parse_query_response_borrowed` + `for_each_row_ref`)
    /// decodes the reassembled-from-fragments buffer to exactly the rows of the
    /// whole-buffer OWNED decode. This couples two independent properties in one
    /// assertion: (a) fragmentation invariance, and (b) borrowed/owned decode
    /// agreement — neither the splitting nor the zero-copy fast path may change a
    /// single cell. Compared as owned values cell-for-cell.
    #[test]
    fn borrowed_decode_is_invariant_under_arbitrary_splits(seeds in split_seeds()) {
        let (payload, golden) = golden_response();
        let cuts = cut_points(payload.len(), &seeds);
        let framed = frame_split(&payload, &cuts);
        let reassembled = reassemble(&framed, has_end_of_response_correct);
        let rows = borrowed_rows(&reassembled, &golden.columns)
            .unwrap_or_else(|e| panic!("borrowed decode failed for cuts {cuts:?}: {e:?}"));
        prop_assert_eq!(
            rows, golden.rows.clone(),
            "borrowed decode differs from the owned whole-stream rows for cuts {:?}", cuts
        );
    }

    /// SPLIT-INVARIANCE UNDER THE FAITHFUL LONE-MARKER WIRE FORM. Instead of the
    /// flag shortcut, deliver the real end-of-response the way a multi-packet
    /// server does: the BODY (payload minus its trailing `0x1d`) split across
    /// arbitrary `0`-flagged interior DATA packets, then the marker as its OWN
    /// minimal packet (header + 3 bytes). This exercises the size guard's
    /// POSITIVE branch — `whole_packet_len == header + 3 && payload == [0x1d]`
    /// must stop reassembly — under arbitrary split points (the flag-terminated
    /// properties above never reach that branch, since the captured body has no
    /// interior `0x1d`). The reassembled buffer must equal the original payload
    /// and decode to the golden rows regardless of where the body is cut.
    #[test]
    fn lone_marker_wire_form_is_invariant_under_arbitrary_splits(seeds in split_seeds()) {
        let (payload, golden) = golden_response();
        prop_assert_eq!(payload.last(), Some(&TNS_MSG_TYPE_END_OF_RESPONSE),
            "fixture payload ends in the real marker");
        let body = &payload[..payload.len() - 1];
        let cuts = cut_points(body.len(), &seeds);

        // Frame the body as 0-flagged interior packets at arbitrary cuts...
        let mut framed = frame_split_no_terminator(body, &cuts);
        // ...then append the real end-of-response as its own minimal packet.
        framed.push(frame_data_packet(0, &[TNS_MSG_TYPE_END_OF_RESPONSE]));

        let reassembled = reassemble(&framed, has_end_of_response_correct);
        prop_assert_eq!(
            &reassembled, &payload,
            "lone-marker reassembly differs from the whole stream for cuts {:?}", cuts
        );
        let result = parse_query_response(&reassembled, ClientCapabilities::default())
            .unwrap_or_else(|e| panic!("lone-marker decode failed for cuts {cuts:?}: {e:?}"));
        prop_assert_eq!(
            result, golden,
            "lone-marker decode differs from the whole-stream decode for cuts {:?}", cuts
        );
    }

    /// INTERIOR-MARKER-BYTE REASSEMBLY GUARD (the n2s mis-frame trigger). Frame
    /// the golden BODY across two interior DATA packets cut at an arbitrary
    /// point, where the first interior packet's post-flags payload ends in an
    /// ordinary `0x1d` (`TNS_MSG_TYPE_END_OF_RESPONSE`) — exactly what happens
    /// past ~1500 rows when a packet boundary lands on a normal row byte equal
    /// to the marker — then send the real end-of-response as its OWN minimal
    /// packet (header + 3 bytes). The body bytes are held constant; only the
    /// FRAMING varies, so this models the wire faithfully.
    ///
    /// The invariant is REASSEMBLY split-invariance: the size-guarded rule must
    /// NOT stop at the interior `0x1d`, so the reassembled buffer is byte-for-
    /// byte the full body + the interior marker byte + the lone trailing marker
    /// — never truncated. (The naive pre-n2s rule, which keyed only on the last
    /// byte, would stop at the first interior packet here and truncate. A reader
    /// that simply buffered everything would mask the bug; this property forces
    /// the boundary, so it genuinely exercises the size guard.) We assert on the
    /// reassembled BYTES, not on a re-decode: the injected interior marker byte
    /// is not part of the captured TTC message, so re-decoding it has no defined
    /// golden — the framing invariant is the byte-level one. The split point is
    /// arbitrary, so every interior position is exercised.
    #[test]
    fn interior_marker_byte_does_not_truncate(seed in 0usize..100_000) {
        let (payload, _golden) = golden_response();
        prop_assert_eq!(payload.last(), Some(&TNS_MSG_TYPE_END_OF_RESPONSE),
            "fixture payload ends in the real marker");
        let body = &payload[..payload.len() - 1]; // everything before the final marker
        prop_assert!(body.len() >= 2, "need an interior boundary");
        let cut = 1 + (seed % (body.len() - 1)); // 1..=body.len()-1

        // First interior packet's payload ends in an ORDINARY 0x1d at the
        // boundary; the body content is otherwise unchanged.
        let mut first = body[..cut].to_vec();
        first.push(TNS_MSG_TYPE_END_OF_RESPONSE);
        let second = body[cut..].to_vec();
        let framed = vec![
            frame_data_packet(0, &first),
            frame_data_packet(0, &second),
            // the real end-of-response, as its own minimal packet (header + 3)
            frame_data_packet(0, &[TNS_MSG_TYPE_END_OF_RESPONSE]),
        ];

        // Expected reassembly: every post-flags payload concatenated verbatim,
        // with NO early stop — first packet (body[..cut] + interior 0x1d), then
        // second packet (body[cut..]), then the lone trailing marker.
        let mut expected = Vec::new();
        expected.extend_from_slice(&first);
        expected.extend_from_slice(&second);
        expected.push(TNS_MSG_TYPE_END_OF_RESPONSE); // the lone trailing marker

        let reassembled = reassemble(&framed, has_end_of_response_correct);
        prop_assert_eq!(
            &reassembled, &expected,
            "size-guarded rule truncated at an interior 0x1d (cut {})", cut
        );

        // CONTRAST: the naive last-byte rule WOULD stop at the first interior
        // packet (it ends in 0x1d), truncating the response — pinning that the
        // size guard is load-bearing, not incidental.
        let naive = reassemble(&framed, |flags, payload, _len| {
            flags & TNS_DATA_FLAGS_END_OF_RESPONSE != 0
                || payload.last() == Some(&TNS_MSG_TYPE_END_OF_RESPONSE)
        });
        prop_assert!(
            naive.len() < reassembled.len(),
            "naive last-byte rule must truncate where the size guard does not (cut {})", cut
        );
    }
}

// ---------------------------------------------------------------------------
// Anchor (example) tests: the degenerate endpoints, so a regression in the
// fixture or helpers surfaces without waiting on a proptest shrink.
// ---------------------------------------------------------------------------

/// Zero splits (whole payload in one END_OF_RESPONSE packet) is the identity.
#[test]
fn zero_split_is_identity() {
    let (payload, golden) = golden_response();
    let framed = frame_split(&payload, &[]);
    let reassembled = reassemble(&framed, has_end_of_response_correct);
    assert_eq!(reassembled, payload, "single-packet reassembly != payload");
    let result = parse_query_response(&reassembled, ClientCapabilities::default())
        .expect("single-packet decode");
    assert_eq!(result, golden, "single-packet decode != whole decode");
}

/// Maximal fragmentation: one byte per packet. Every byte of the body is its
/// own interior DATA packet; the size-guarded rule must still reassemble the
/// whole stream and decode to the golden rows. This is the densest legal split.
#[test]
fn one_byte_per_packet_is_invariant() {
    let (payload, golden) = golden_response();
    let cuts: Vec<usize> = (1..payload.len()).collect();
    let framed = frame_split(&payload, &cuts);
    assert_eq!(framed.len(), payload.len(), "expected one packet per byte");
    let reassembled = reassemble(&framed, has_end_of_response_correct);
    assert_eq!(
        reassembled, payload,
        "byte-per-packet reassembly != payload"
    );
    let result = parse_query_response(&reassembled, ClientCapabilities::default())
        .expect("byte-per-packet decode");
    assert_eq!(result, golden, "byte-per-packet decode != whole decode");

    // The borrowed path agrees on the maximally-fragmented stream too.
    let rows = borrowed_rows(&reassembled, &golden.columns).expect("byte-per-packet borrowed");
    assert_eq!(rows, golden.rows, "byte-per-packet borrowed != owned rows");
}
