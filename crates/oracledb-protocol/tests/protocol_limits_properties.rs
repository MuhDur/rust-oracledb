//! Property tests for [`ProtocolLimits`] oversized-length rejection (W1-T5-fup).
//!
//! W1-T5 landed boundary *unit* tests (one `observed = max + 1` case per family)
//! in `src/wire.rs`. These properties generalize the boundary to **arbitrary
//! oversized inputs**: for every resource family, feeding a length/count larger
//! than the configured bound must be rejected with a typed
//! [`ProtocolError::ResourceLimit`] — never a panic, never an OOM, and crucially
//! **before** the decoder reserves memory for the lie. This also seeds the W3-E2
//! fuzz corpus (each shrunk counterexample, if any, lands under
//! `tests/protocol_limits_properties.proptest-regressions`).
//!
//! Two complementary guarantees are exercised:
//!
//! 1. **Family-limit predicate** ([`ProtocolLimits::check_*`]): an `observed`
//!    above `maximum` returns the exact typed error with `observed`/`maximum`
//!    preserved, for every one of the eleven families.
//! 2. **Allocate-after-checking** (the [`BoundedReader`] primitives and the real
//!    `*_with_limits` decoders): an oversized declared count/length cannot drive
//!    an allocation larger than the bytes actually present. We prove the "before
//!    allocating" half with the `allocation-counter` global allocator — the
//!    rejection path must stay within a tiny constant of allocations regardless
//!    of how large the declared count is.
//!
//! The `allocation-counter` crate keeps its `unsafe GlobalAlloc` inside itself,
//! so this test (and the whole workspace) stays `#![forbid(unsafe_code)]`-clean.

use oracledb_protocol::packet::TnsPacket;
use oracledb_protocol::vector::{decode_vector_with_limits, TNS_VECTOR_MAGIC_BYTE};
use oracledb_protocol::wire::{
    BoundedReader, ProtocolLimits, TtcReader, TNS_LONG_LENGTH_INDICATOR,
};
use oracledb_protocol::ProtocolError;
use proptest::prelude::*;

const CASES: u32 = 2_048;

fn config() -> ProptestConfig {
    ProptestConfig {
        cases: CASES,
        ..ProptestConfig::default()
    }
}

/// Assert a decode result is the exact typed resource-limit error for `family`,
/// with `observed`/`maximum` preserved verbatim (callers classify on these).
/// Returns a `TestCaseError` so a property body can propagate it with `?`.
fn assert_limit<T: std::fmt::Debug>(
    result: Result<T, ProtocolError>,
    family: &str,
    observed: usize,
    maximum: usize,
) -> Result<(), TestCaseError> {
    match result {
        Err(ProtocolError::ResourceLimit {
            limit,
            observed: obs,
            maximum: max,
        }) if limit == family && obs == observed && max == maximum => Ok(()),
        other => Err(TestCaseError::fail(format!(
            "expected ResourceLimit {{ limit: {family}, observed: {observed}, maximum: {maximum} }}, got {other:?}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// 1. Family-limit predicate: every check_* rejects every oversized observed.
// ---------------------------------------------------------------------------
//
// One ProtocolLimits with every family pinned to a small `maximum`, then a
// property per family over an `observed` strategy that is always > maximum (up
// to usize::MAX). The predicate must return the typed error with the exact
// fields and never panic. This is the boundary unit test
// (`protocol_limits_check_helpers_return_typed_resource_limit_errors`)
// generalized from `max + 1` to the whole oversized range.

/// A validated limits value with every family pinned to a distinct small max,
/// honoring the `packet <= frame <= response` byte hierarchy `validate()` wants.
fn tiny_limits() -> ProtocolLimits {
    ProtocolLimits {
        max_packet_bytes: 8,
        max_frame_bytes: 16,
        max_response_bytes: 32,
        max_columns: 2,
        max_binds: 3,
        max_batch_rows: 4,
        max_object_depth: 5,
        max_object_elements: 6,
        max_vector_dimensions: 7,
        max_lob_chunks: 8,
        max_length_prefixed_elements: 9,
    }
    .validate()
    .expect("tiny limits are valid")
}

proptest! {
    #![proptest_config(config())]

    /// Each family's predicate, over an arbitrary observed strictly above its
    /// max, returns the exact typed error. The `family` index selects which of
    /// the eleven `check_*` helpers to drive, so one property covers them all.
    #[test]
    fn check_helpers_reject_every_oversized_observed(
        // family index 0..11 and an "overshoot" added on top of that family's max.
        family in 0usize..11,
        overshoot in 1usize..=usize::MAX,
    ) {
        let limits = tiny_limits();
        // (name, max, call) for each family, in named_limits() order.
        let max = [8usize, 16, 32, 2, 3, 4, 5, 6, 7, 8, 9][family];
        let observed = max.saturating_add(overshoot);
        // `observed` is guaranteed > max (overshoot >= 1, saturating keeps it so).
        prop_assume!(observed > max);

        let (name, result) = match family {
            0 => ("packet_bytes", limits.check_packet_bytes(observed)),
            1 => ("frame_bytes", limits.check_frame_bytes(observed)),
            2 => ("response_bytes", limits.check_response_bytes(observed)),
            3 => ("columns", limits.check_columns(observed)),
            4 => ("binds", limits.check_binds(observed)),
            5 => ("batch_rows", limits.check_batch_rows(observed)),
            6 => ("object_depth", limits.check_object_depth(observed)),
            7 => ("object_elements", limits.check_object_elements(observed)),
            8 => ("vector_dimensions", limits.check_vector_dimensions(observed)),
            9 => ("lob_chunks", limits.check_lob_chunks(observed)),
            _ => (
                "length_prefixed_elements",
                limits.check_length_prefixed_elements(observed),
            ),
        };
        assert_limit(result, name, observed, max)?;
    }

    /// The dual: any observed at or below the max is accepted (no false
    /// rejection). Pins that the predicate is an inequality, not a constant.
    #[test]
    fn check_helpers_accept_within_limit(family in 0usize..11) {
        let limits = tiny_limits();
        let max = [8usize, 16, 32, 2, 3, 4, 5, 6, 7, 8, 9][family];
        for observed in 0..=max {
            let ok = match family {
                0 => limits.check_packet_bytes(observed),
                1 => limits.check_frame_bytes(observed),
                2 => limits.check_response_bytes(observed),
                3 => limits.check_columns(observed),
                4 => limits.check_binds(observed),
                5 => limits.check_batch_rows(observed),
                6 => limits.check_object_depth(observed),
                7 => limits.check_object_elements(observed),
                8 => limits.check_vector_dimensions(observed),
                9 => limits.check_lob_chunks(observed),
                _ => limits.check_length_prefixed_elements(observed),
            };
            prop_assert!(ok.is_ok(), "family {family} rejected in-bounds {observed}");
        }
    }
}

// ---------------------------------------------------------------------------
// 2. Allocate-after-checking: the BoundedReader primitives.
// ---------------------------------------------------------------------------
//
// alloc_count_checked and with_capacity_bounded are the single place every
// count-driven decode reserves memory. The invariant (bead l2p): a count read
// off the wire can NEVER drive an allocation larger than the bytes remaining.
// We feed arbitrary oversized counts against a small buffer and prove (a) the
// early-error form rejects, and (b) the cap-and-grow form never reserves more
// than remaining()/per_elem, measured by the allocation counter.

proptest! {
    #![proptest_config(config())]

    /// `alloc_count_checked` fails closed for any count that cannot fit, and the
    /// failing call allocates essentially nothing (no Vec of `count` is built).
    #[test]
    fn alloc_count_checked_rejects_oversized_without_allocating(
        buf_len in 0usize..64,
        // A count far larger than any byte budget the small buffer could honor.
        count in (1usize << 20)..=usize::MAX,
        per_elem in 1usize..=8,
    ) {
        let bytes = vec![0u8; buf_len];
        let reader = TtcReader::new(&bytes);
        let measured = allocation_counter::measure(|| {
            let r = reader.alloc_count_checked(count, per_elem);
            // count * per_elem >> remaining, so this must be Err — either the
            // length_prefixed_elements family limit (DEFAULT = 1e6) or the
            // buffer-ceiling TtcDecode. Both are fail-closed, no panic.
            assert!(r.is_err(), "oversized count {count} must be rejected");
        });
        // The rejection path must not allocate a buffer for `count` elements.
        // A couple of bookkeeping allocations are tolerable; `count` (>= 1<<20)
        // elements would dwarf this bound.
        prop_assert!(
            measured.count_total <= 8,
            "alloc_count_checked allocated {} times rejecting an oversized count (must reject before allocating)",
            measured.count_total
        );
    }

    /// `with_capacity_bounded` caps the speculative reservation by the buffer:
    /// the returned Vec's capacity is never more than remaining()/per_elem,
    /// however large the declared count.
    #[test]
    fn with_capacity_bounded_never_exceeds_buffer(
        buf_len in 0usize..4096,
        count in any::<usize>(),
        per_elem in 1usize..=16,
    ) {
        let bytes = vec![0u8; buf_len];
        let reader = TtcReader::new(&bytes);
        let ceiling = buf_len / per_elem;
        let measured = allocation_counter::measure(|| {
            let v: Vec<u64> = reader.with_capacity_bounded(count, per_elem);
            // Capacity is bounded by the buffer ceiling, never by `count`.
            assert!(
                v.capacity() <= ceiling,
                "capacity {} exceeded buffer ceiling {ceiling}",
                v.capacity()
            );
            std::hint::black_box(v);
        });
        // u64 is 8 bytes; the reservation can be at most ceiling * 8 bytes. Pin
        // that the bytes reserved track the buffer, not the (possibly huge) count.
        prop_assert!(
            measured.bytes_total <= (ceiling.saturating_mul(8) as u64) + 64,
            "with_capacity_bounded reserved {} bytes for ceiling {ceiling} (count {count})",
            measured.bytes_total
        );
    }
}

// ---------------------------------------------------------------------------
// 3. Real decoders: oversized length prefix on the wire is rejected before the
//    big allocation. One representative decode entry per byte/count family.
// ---------------------------------------------------------------------------

/// Build a VECTOR image header declaring `num_elements` dense FLOAT32 dims but
/// carrying no value bytes, so a faithful decoder must reject the dimension
/// count before reserving space for it.
fn vector_header_with_dims(num_elements: u32) -> Vec<u8> {
    let mut img = vec![
        TNS_VECTOR_MAGIC_BYTE,
        0, // version 0 (no sparse)
        0,
        0, // flags = 0 (no norm, not sparse)
        2, // VECTOR_FORMAT_FLOAT32
    ];
    img.extend_from_slice(&num_elements.to_be_bytes());
    img
}

proptest! {
    #![proptest_config(config())]

    /// `decode_vector_with_limits` rejects an oversized dimension count with a
    /// typed `vector_dimensions` ResourceLimit, and the rejecting decode does
    /// not allocate a buffer for the declared dimensions.
    #[test]
    fn decode_vector_rejects_oversized_dimension_count(
        // Above the small per-test limit but within u32 so it reaches the family
        // check (the image carries no value bytes, so any positive count is a lie).
        num_elements in 33u32..=u32::MAX,
    ) {
        let limits = ProtocolLimits {
            max_vector_dimensions: 32,
            ..ProtocolLimits::DEFAULT
        };
        let img = vector_header_with_dims(num_elements);
        let measured = allocation_counter::measure(|| {
            let result = decode_vector_with_limits(&img, limits);
            assert!(result.is_err(), "oversized vector dims must be rejected");
        });
        // Decode again (outside the measure) to assert on the typed error.
        let result = decode_vector_with_limits(&img, limits);
        assert_limit(result, "vector_dimensions", num_elements as usize, 32)?;
        // The reject path must not reserve `num_elements` f32 slots.
        prop_assert!(
            measured.count_total <= 8,
            "decode_vector allocated {} times rejecting {num_elements} dims",
            measured.count_total
        );
    }

    /// `TtcReader::read_raw` rejects an oversized length against the
    /// response_bytes budget without reading/allocating that many bytes.
    #[test]
    fn read_raw_rejects_oversized_length(
        max_response in 1usize..=64,
        overshoot in 1usize..=(usize::MAX >> 1),
    ) {
        let limits = ProtocolLimits {
            max_packet_bytes: max_response,
            max_frame_bytes: max_response,
            max_response_bytes: max_response,
            ..ProtocolLimits::DEFAULT
        }
        .validate()
        .expect("byte hierarchy holds (all equal)");
        let bytes = vec![0u8; max_response];
        let mut reader = TtcReader::with_limits(&bytes, limits).expect("valid limits");
        let len = max_response.saturating_add(overshoot);
        prop_assume!(len > max_response);
        assert_limit(reader.read_raw(len), "response_bytes", len, max_response)?;
    }

    /// The chunked long form (`0xfe`) bounded by `lob_chunks`: a stream of more
    /// chunks than allowed is rejected with the typed lob_chunks error. We build
    /// `n` single-byte chunks and set the limit below `n`.
    #[test]
    fn read_bytes_rejects_too_many_lob_chunks(
        max_chunks in 1usize..=16,
        extra in 1usize..=16,
    ) {
        let n = max_chunks + extra;
        let limits = ProtocolLimits {
            max_lob_chunks: max_chunks,
            ..ProtocolLimits::DEFAULT
        };
        // 0xfe then `n` chunks, each a 1-byte payload. A chunk length is a `ub4`
        // (length byte, then that many value bytes), so chunk_len = 1 encodes as
        // [0x01, 0x01]; the payload byte follows. A ub4 of 0 ([0x00]) terminates.
        let mut bytes = vec![TNS_LONG_LENGTH_INDICATOR];
        for i in 0..n {
            bytes.push(0x01); // ub4 length byte: 1 value byte follows
            bytes.push(0x01); // ub4 value: chunk_len = 1
            bytes.push(i as u8); // the 1-byte chunk payload
        }
        bytes.push(0x00); // ub4 value 0: chunk terminator
        let mut reader = TtcReader::with_limits(&bytes, limits).expect("valid limits");
        // The decoder counts chunks and trips check_lob_chunks at chunk
        // (max_chunks + 1); observed is that chunk index, maximum is max_chunks.
        match reader.read_bytes() {
            Err(ProtocolError::ResourceLimit { limit, observed, maximum }) => {
                prop_assert_eq!(limit, "lob_chunks");
                prop_assert_eq!(maximum, max_chunks);
                prop_assert!(observed > maximum, "observed {observed} must exceed max {maximum}");
            }
            other => prop_assert!(false, "expected lob_chunks ResourceLimit, got {other:?}"),
        }
    }

    /// `TnsPacket::parse_with_limits` rejects a declared packet length over the
    /// packet_bytes budget with the typed error, before slicing a payload.
    #[test]
    fn packet_parse_rejects_oversized_declared_length(
        max_packet in 8usize..=64,
        declared in 0u16..=u16::MAX,
    ) {
        // Keep the byte hierarchy valid: frame/response >= packet.
        let limits = ProtocolLimits {
            max_packet_bytes: max_packet,
            max_frame_bytes: max_packet.max(64),
            max_response_bytes: max_packet.max(64),
            ..ProtocolLimits::DEFAULT
        }
        .validate()
        .expect("byte hierarchy holds");
        // Only exercise declared lengths that are (a) a structurally valid header
        // length (>= 8) and (b) strictly over the packet budget.
        prop_assume!(usize::from(declared) > max_packet);
        let mut header = declared.to_be_bytes().to_vec();
        header.extend_from_slice(&[0u8; 6]); // pad to the 8-byte TNS header
        assert_limit(
            TnsPacket::parse_with_limits(&header, limits),
            "packet_bytes",
            usize::from(declared),
            max_packet,
        )?;
    }
}
