//! Property + metamorphic + boundary tests for the `pub` sans-io codecs:
//! VECTOR (vector.rs), OSON/JSON (oson.rs), and LOB text chunk math
//! (dbobject.rs). These reach the public API, so they live as an integration
//! test (the scalar `pub(crate)` codecs are covered in-crate by
//! `src/thin/proptests.rs`).
//!
//! Method (skill: testing-metamorphic): each property is either a ROUND-TRIP
//! (`decode(encode(x)) == x`) or a METAMORPHIC relation that holds with no
//! external oracle (idempotence, split-invariance, set-preservation). Every
//! property cites the reference `.pyx` (python-oracledb v4.0.1) or the Oracle
//! wire invariant it enforces; none are tautologies.

use oracledb_protocol::oson::{decode_oson, encode_oson, OsonValue};
use oracledb_protocol::thin::{decode_lob_text, encode_lob_text};
use oracledb_protocol::vector::{
    decode_vector, encode_vector, Vector, VectorValues, VECTOR_FORMAT_BINARY,
};
use proptest::prelude::*;

const CASES: u32 = 1_024;

fn config() -> ProptestConfig {
    ProptestConfig {
        cases: CASES,
        ..ProptestConfig::default()
    }
}

// ---------------------------------------------------------------------------
// VECTOR — round-trip every format + sparse index-set preservation
// ---------------------------------------------------------------------------
//
// encode_vector / decode_vector (vector.rs, port of impl/base/vector.pyx
// VectorEncoder/VectorDecoder). Dense f32/f64/int8/binary and sparse all share
// one image layout; the round-trip must reproduce the value exactly. Floats use
// Oracle's BINARY_FLOAT/DOUBLE sign transform, so we compare on bits to make the
// signed-zero / NaN behavior explicit.

/// Strategy for a small dense f32 vector. f32 values restricted to finite
/// values (normals, subnormals, signed zeros) so the bit-exact comparison is
/// meaningful (NaN payloads are checked in the scalar suite).
fn f32_vec() -> impl Strategy<Value = Vec<f32>> {
    let elem = prop::num::f32::NORMAL
        | prop::num::f32::SUBNORMAL
        | prop::num::f32::ZERO
        | prop::num::f32::NEGATIVE
        | prop::num::f32::POSITIVE;
    prop::collection::vec(elem, 0..=64)
}

fn f64_vec() -> impl Strategy<Value = Vec<f64>> {
    let elem = prop::num::f64::NORMAL
        | prop::num::f64::SUBNORMAL
        | prop::num::f64::ZERO
        | prop::num::f64::NEGATIVE
        | prop::num::f64::POSITIVE;
    prop::collection::vec(elem, 0..=64)
}

fn bits_eq_f32(a: &[f32], b: &[f32]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits())
}

fn bits_eq_f64(a: &[f64], b: &[f64]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits())
}

proptest! {
    #![proptest_config(config())]

    /// ROUND-TRIP dense FLOAT32 (bit-exact).
    #[test]
    fn vector_dense_f32_round_trip(values in f32_vec()) {
        let v = Vector::Dense(VectorValues::Float32(values.clone()));
        let decoded = decode_vector(&encode_vector(&v)).expect("decode f32 vector");
        let Vector::Dense(VectorValues::Float32(out)) = decoded else {
            panic!("wrong variant");
        };
        prop_assert!(bits_eq_f32(&values, &out), "f32 vector bits differ");
    }

    /// ROUND-TRIP dense FLOAT64 (bit-exact).
    #[test]
    fn vector_dense_f64_round_trip(values in f64_vec()) {
        let v = Vector::Dense(VectorValues::Float64(values.clone()));
        let decoded = decode_vector(&encode_vector(&v)).expect("decode f64 vector");
        let Vector::Dense(VectorValues::Float64(out)) = decoded else {
            panic!("wrong variant");
        };
        prop_assert!(bits_eq_f64(&values, &out), "f64 vector bits differ");
    }

    /// ROUND-TRIP dense INT8.
    #[test]
    fn vector_dense_int8_round_trip(values in prop::collection::vec(any::<i8>(), 0..=128)) {
        let v = Vector::Dense(VectorValues::Int8(values.clone()));
        let decoded = decode_vector(&encode_vector(&v)).expect("decode int8 vector");
        prop_assert_eq!(decoded, Vector::Dense(VectorValues::Int8(values)));
    }

    /// ROUND-TRIP dense BINARY. The header encodes the *bit* count (len * 8); the
    /// decoder divides back by 8 (vector.rs:141-143). A non-byte-aligned bug
    /// would drop or add bytes.
    #[test]
    fn vector_dense_binary_round_trip(values in prop::collection::vec(any::<u8>(), 0..=64)) {
        let v = Vector::Dense(VectorValues::Binary(values.clone()));
        let image = encode_vector(&v);
        // METAMORPHIC: the num_elements header field equals 8 * byte-count.
        let num_elements = u32::from_be_bytes([image[5], image[6], image[7], image[8]]);
        prop_assert_eq!(num_elements as usize, values.len() * 8);
        prop_assert_eq!(image[4], VECTOR_FORMAT_BINARY);
        let decoded = decode_vector(&image).expect("decode binary vector");
        prop_assert_eq!(decoded, Vector::Dense(VectorValues::Binary(values)));
    }

    /// ROUND-TRIP sparse + METAMORPHIC index-set preservation: the decoded
    /// sparse vector carries exactly the same (index -> value) mapping. The
    /// number of dimensions, the index list, and the parallel value list must
    /// all survive (vector.rs sparse branch).
    #[test]
    fn vector_sparse_round_trip_preserves_index_set(
        num_dimensions in 1u32..=4096,
        entries in prop::collection::vec((0u32..4096, prop::num::f64::NORMAL), 0..=32),
    ) {
        // The wire form lists indices then values in parallel; build matching
        // arrays. Duplicate indices are allowed by the codec (it does not
        // dedupe), so we keep them as-is to test faithful preservation.
        let indices: Vec<u32> = entries.iter().map(|(i, _)| *i).collect();
        let values: Vec<f64> = entries.iter().map(|(_, v)| *v).collect();
        let v = Vector::Sparse {
            num_dimensions,
            indices: indices.clone(),
            values: VectorValues::Float64(values.clone()),
        };
        let decoded = decode_vector(&encode_vector(&v)).expect("decode sparse vector");
        let Vector::Sparse { num_dimensions: nd, indices: di, values: dv } = decoded else {
            panic!("expected sparse");
        };
        prop_assert_eq!(nd, num_dimensions, "num_dimensions changed");
        prop_assert_eq!(di, indices, "sparse index set not preserved");
        let VectorValues::Float64(dv) = dv else { panic!("value format changed") };
        prop_assert!(bits_eq_f64(&values, &dv), "sparse values differ");
    }
}

/// Boundary VECTOR cases: empty dense, single element, and the documented
/// golden-style values.
#[test]
fn vector_boundary_cases() {
    let cases = [
        Vector::Dense(VectorValues::Float32(vec![])),
        Vector::Dense(VectorValues::Float64(vec![f64::MAX, f64::MIN])),
        Vector::Dense(VectorValues::Int8(vec![i8::MIN, 0, i8::MAX])),
        Vector::Dense(VectorValues::Binary(vec![0x00, 0xFF])),
        Vector::Sparse {
            num_dimensions: 8,
            indices: vec![],
            values: VectorValues::Float32(vec![]),
        },
        Vector::Sparse {
            num_dimensions: 1,
            indices: vec![0],
            values: VectorValues::Int8(vec![-1]),
        },
    ];
    for v in cases {
        let decoded = decode_vector(&encode_vector(&v)).expect("decode boundary vector");
        assert_eq!(decoded, v, "vector boundary round-trip");
    }
}

// ---------------------------------------------------------------------------
// OSON / JSON — metamorphic re-encode idempotence + nesting-depth round-trip
// ---------------------------------------------------------------------------
//
// encode_oson / decode_oson (oson.rs, port of impl/base/oson.pyx
// OsonEncoder/OsonDecoder). The image is the binary the Oracle server stores
// for a native JSON column.

/// Recursive strategy for an OsonValue tree. Leaves are the scalar variants
/// that round-trip exactly (Number-as-canonical-text, Bool, Null, String,
/// BinaryFloat/Double). Numbers are generated as canonical integer/decimal text
/// the decoder would itself emit, so the round-trip is identity. Object keys are
/// short ASCII so the short-field-name path (version 1) is exercised.
fn oson_leaf() -> impl Strategy<Value = OsonValue> {
    prop_oneof![
        Just(OsonValue::Null),
        any::<bool>().prop_map(OsonValue::Bool),
        (-1_000_000i64..1_000_000).prop_map(|n| OsonValue::Number(n.to_string())),
        ".{0,32}".prop_map(OsonValue::String),
        prop::num::f32::NORMAL.prop_map(OsonValue::BinaryFloat),
        prop::num::f64::NORMAL.prop_map(OsonValue::BinaryDouble),
    ]
}

fn oson_tree() -> impl Strategy<Value = OsonValue> {
    oson_leaf().prop_recursive(6, 64, 8, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..=8).prop_map(OsonValue::Array),
            prop::collection::vec(("[a-z]{1,8}", inner), 0..=8).prop_map(|pairs| {
                // De-duplicate keys (object semantics keep the last write; the
                // decoder would too, so dup keys are not a faithful round-trip).
                let mut seen = std::collections::BTreeSet::new();
                let unique: Vec<(String, OsonValue)> = pairs
                    .into_iter()
                    .filter(|(k, _)| seen.insert(k.clone()))
                    .collect();
                OsonValue::Object(unique)
            }),
        ]
    })
}

fn bits_eq_oson(a: &OsonValue, b: &OsonValue) -> bool {
    // Compare structurally; BinaryFloat/Double compared on bits so signed zeros
    // are distinguished, matching the codec's exactness.
    match (a, b) {
        (OsonValue::BinaryFloat(x), OsonValue::BinaryFloat(y)) => x.to_bits() == y.to_bits(),
        (OsonValue::BinaryDouble(x), OsonValue::BinaryDouble(y)) => x.to_bits() == y.to_bits(),
        (OsonValue::Array(x), OsonValue::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(a, b)| bits_eq_oson(a, b))
        }
        (OsonValue::Object(x), OsonValue::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y)
                    .all(|((kx, vx), (ky, vy))| kx == ky && bits_eq_oson(vx, vy))
        }
        _ => a == b,
    }
}

proptest! {
    #![proptest_config(config())]

    /// ROUND-TRIP: decode(encode(tree)) == tree for arbitrary JSON trees.
    #[test]
    fn oson_round_trip(value in oson_tree()) {
        let encoded = encode_oson(&value, false).expect("encode oson");
        let decoded = decode_oson(&encoded).expect("decode oson");
        prop_assert!(bits_eq_oson(&value, &decoded), "oson tree changed");
    }

    /// METAMORPHIC — RE-ENCODE IDEMPOTENCE: decode, re-encode, decode again
    /// yields an equal tree. A stable encoder must reach a fixed point after one
    /// round; if the second decode differs, the encoder is non-deterministic or
    /// the decoder is lossy. (This is strictly stronger than the round-trip: it
    /// also catches an encoder that emits a *different but still decodable* image
    /// on re-encode.)
    #[test]
    fn oson_reencode_idempotent(value in oson_tree()) {
        let img1 = encode_oson(&value, false).expect("encode 1");
        let tree1 = decode_oson(&img1).expect("decode 1");
        let img2 = encode_oson(&tree1, false).expect("encode 2");
        let tree2 = decode_oson(&img2).expect("decode 2");
        prop_assert!(bits_eq_oson(&tree1, &tree2), "re-encode not idempotent at the tree level");
        // The image bytes themselves must also be stable (byte-for-byte fixed
        // point) — the server relies on a canonical encoding.
        prop_assert_eq!(img1, img2, "re-encode not byte-stable");
    }

    /// METAMORPHIC — CONTAINER NESTING DEPTH ROUND-TRIPS: an array nested to a
    /// generated depth decodes back to exactly that depth. The OSON tree segment
    /// stores child offsets; an off-by-one in the depth/offset handling would
    /// drop or add a level.
    #[test]
    fn oson_nesting_depth_round_trips(depth in 0usize..=40) {
        let mut v = OsonValue::Number("7".into());
        for _ in 0..depth {
            v = OsonValue::Array(vec![v]);
        }
        let decoded = decode_oson(&encode_oson(&v, false).expect("encode")).expect("decode");
        // Measure the decoded depth.
        let mut d = 0usize;
        let mut cur = &decoded;
        while let OsonValue::Array(items) = cur {
            prop_assert_eq!(items.len(), 1, "nesting array width changed");
            cur = &items[0];
            d += 1;
        }
        prop_assert_eq!(d, depth, "nesting depth changed");
        prop_assert_eq!(cur, &OsonValue::Number("7".into()), "leaf changed");
    }
}

// ---------------------------------------------------------------------------
// LOB chunk math — split-invariant reassembly (the wide-row sibling class)
// ---------------------------------------------------------------------------
//
// A CLOB read returns the column text in chunks; the driver concatenates the
// raw bytes and then decodes once (decode_lob_text, dbobject.rs). The
// correctness net here is split-INVARIANCE: decoding the whole byte buffer must
// equal decoding it as any sequence of chunks split on CHARACTER boundaries.
// This is the LOB analog of the multi-packet wide-row reassembly bug class
// (bead rust-oracledb-n2s): a value split across read chunks must reassemble
// identically regardless of where the splits fall.

/// CS_FORM for a single-byte (UTF-8) CLOB. csfrm 1 == CS_FORM_IMPLICIT.
const CS_FORM_IMPLICIT: u8 = 1;
/// CS_FORM for an NCHAR/NCLOB (drives the UTF-16 path in decode_lob_text).
const CS_FORM_NCHAR: u8 = 2;

proptest! {
    #![proptest_config(config())]

    /// METAMORPHIC — UTF-8 CLOB split-invariance on codepoint boundaries:
    /// for any split of the encoded bytes at character boundaries, the
    /// concatenation of the per-chunk decodes equals the whole-buffer decode.
    #[test]
    fn lob_utf8_chunked_decode_matches_whole(s in ".{0,256}", splits in prop::collection::vec(0usize..256, 0..8)) {
        let bytes = encode_lob_text(&s, CS_FORM_IMPLICIT, None);
        let whole = decode_lob_text(&bytes, CS_FORM_IMPLICIT, None).expect("decode whole utf8 lob");

        // Build character-boundary split points (byte offsets that fall between
        // codepoints), then chunk the buffer there. Splitting only on char
        // boundaries models the reference's per-chunk char-aware reads.
        let mut boundaries: Vec<usize> = s
            .char_indices()
            .map(|(i, _)| i)
            .chain(std::iter::once(s.len()))
            .collect();
        // Map the random split seeds onto valid boundary offsets.
        let mut cut_points: Vec<usize> = splits
            .iter()
            .map(|seed| boundaries[seed % boundaries.len()])
            .collect();
        cut_points.push(0);
        cut_points.push(bytes.len());
        cut_points.sort_unstable();
        cut_points.dedup();

        let mut reassembled = String::new();
        for window in cut_points.windows(2) {
            let chunk = &bytes[window[0]..window[1]];
            reassembled.push_str(&decode_lob_text(chunk, CS_FORM_IMPLICIT, None)
                .expect("decode utf8 lob chunk"));
        }
        boundaries.clear();
        prop_assert_eq!(reassembled, whole, "chunked utf8 LOB decode != whole decode");
    }

    /// METAMORPHIC — exhaustive: split a UTF-8 CLOB at EVERY codepoint boundary
    /// into two halves and assert reassembly equals the whole. This is the dense
    /// version of the wide-row split test for the LOB path: every legal split
    /// point is tried, not just random ones.
    #[test]
    fn lob_utf8_every_boundary_two_way_split(s in ".{0,64}") {
        let bytes = encode_lob_text(&s, CS_FORM_IMPLICIT, None);
        let whole = decode_lob_text(&bytes, CS_FORM_IMPLICIT, None).expect("decode whole");
        let boundaries: Vec<usize> = s
            .char_indices()
            .map(|(i, _)| i)
            .chain(std::iter::once(s.len()))
            .collect();
        for &cut in &boundaries {
            let left = decode_lob_text(&bytes[..cut], CS_FORM_IMPLICIT, None)
                .expect("decode left");
            let right = decode_lob_text(&bytes[cut..], CS_FORM_IMPLICIT, None)
                .expect("decode right");
            prop_assert_eq!(format!("{left}{right}"), whole.clone(),
                "two-way split at byte {} != whole", cut);
        }
    }

    /// METAMORPHIC — NCHAR/NCLOB (UTF-16) split-invariance on code-unit
    /// boundaries: splitting the UTF-16BE byte buffer at 2-byte code-unit
    /// boundaries reassembles identically. (A surrogate pair split between its
    /// two units would fail to decode — the reference reads whole code units —
    /// so we split only on even 2-byte boundaries between *characters*, matching
    /// how chunked NCLOB reads land.)
    #[test]
    fn lob_nchar_utf16_chunked_decode_matches_whole(s in ".{0,128}") {
        let bytes = encode_lob_text(&s, CS_FORM_NCHAR, None);
        let whole = decode_lob_text(&bytes, CS_FORM_NCHAR, None).expect("decode whole nchar lob");
        // Character boundaries in the UTF-16 byte stream: 2 bytes per code unit,
        // and a char is 1 or 2 code units. Cut between characters only.
        let mut byte_off = 0usize;
        let mut boundaries = vec![0usize];
        for ch in s.chars() {
            byte_off += ch.len_utf16() * 2;
            boundaries.push(byte_off);
        }
        let mut reassembled = String::new();
        for window in boundaries.windows(2) {
            let chunk = &bytes[window[0]..window[1]];
            reassembled.push_str(&decode_lob_text(chunk, CS_FORM_NCHAR, None)
                .expect("decode nchar lob chunk"));
        }
        prop_assert_eq!(reassembled, whole, "chunked nchar LOB decode != whole decode");
    }
}

/// Boundary LOB cases: empty, single char, multibyte, and the round-trip
/// through encode_lob_text/decode_lob_text for both charset forms.
#[test]
fn lob_text_boundary_round_trip() {
    for s in ["", "a", "é", "€", "𝄞", "mixed aé€𝄞 text", &"x".repeat(8192)] {
        for csfrm in [CS_FORM_IMPLICIT, CS_FORM_NCHAR] {
            let bytes = encode_lob_text(s, csfrm, None);
            let decoded = decode_lob_text(&bytes, csfrm, None).expect("decode lob boundary");
            assert_eq!(decoded, s, "LOB round-trip csfrm={csfrm} s={s:?}");
        }
    }
}
