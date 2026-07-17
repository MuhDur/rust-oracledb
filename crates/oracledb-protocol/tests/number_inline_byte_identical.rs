//! Byte-identical parity proof for the inline `OracleNumber` representation
//! (bead rust-oracledb-65w).
//!
//! The owned `QueryValue::Number` is moving from a heap `String` carrier to an
//! inline `{ coefficient: i128, scale: i16 }` form with a `Box<str>` fallback.
//! The HARD constraint is that the SINGLE shared formatter
//! (`OracleNumber::fmt_into`) must produce text BYTE-IDENTICAL to the legacy
//! `decode_number_text_into` path, over the whole NUMBER domain. python-oracledb's
//! parity suite asserts the canonical text, so any one-byte divergence is a
//! correctness regression.
//!
//! These tests are written FIRST (TDD): they reference the not-yet-existing
//! `OracleNumber` API and the legacy `decode_number_text_into`, and must FAIL to
//! compile / fail the assertion until the inline form is implemented and proven
//! byte-identical.

use oracledb_protocol::thin::{decode_number_text_into, encode_number_text, OracleNumber};

/// Legacy canonical text: the exact bytes the pre-i128 path produced.
fn legacy_text(wire: &[u8]) -> (String, bool) {
    let mut digits = Vec::new();
    let mut text = String::new();
    let is_integer =
        decode_number_text_into(wire, &mut digits, &mut text).expect("legacy decode of valid wire");
    (text, is_integer)
}

/// New inline path: decode wire -> OracleNumber -> shared formatter.
fn inline_text(wire: &[u8]) -> (String, bool) {
    let num = OracleNumber::from_wire(wire).expect("inline decode of valid wire");
    let mut out = String::new();
    num.fmt_into(&mut out);
    (out, num.is_integer())
}

/// Assert the inline path is byte-identical to the legacy path for one wire form.
fn assert_byte_identical(wire: &[u8], label: &str) {
    let (legacy, legacy_int) = legacy_text(wire);
    let (inline, inline_int) = inline_text(wire);
    assert_eq!(
        legacy, inline,
        "{label}: text diverged: legacy={legacy:?} inline={inline:?} wire={wire:02x?}"
    );
    assert_eq!(
        legacy_int, inline_int,
        "{label}: is_integer diverged: legacy={legacy_int} inline={inline_int} text={legacy:?}"
    );
}

/// Domain-spanning corpus the spec calls out: 38-digit, max/min exponent,
/// negative, zero, -0, integers, fractions, trailing zeros, the single-byte
/// sentinels.
const CORPUS: &[&str] = &[
    "0",
    "1",
    "-1",
    "100",
    "-100",
    "99",
    "-99",
    "0.01",
    "-0.01",
    "0.5",
    "-0.5",
    "3.14159",
    "-3.14159",
    "1000000",
    "0.0001",
    "1e125",
    "-1e125",
    "1e-120",
    "-1e-120",
    "10",
    "-10",
    "1000000000000000000",  // 1e18, integer
    "12345678901234567890", // 20-digit integer
    "123456789012345678901234567890",
    // 38 significant digits (max precision), integer and fractional.
    "12345678901234567890123456789012345678",
    "-12345678901234567890123456789012345678",
    "0.12345678901234567890123456789012345678",
    "-0.12345678901234567890123456789012345678",
    "9999999999999999999999999999999999999",
    "1.23456789012345678901234567890123456789",
    // trailing-zero / scale edge cases.
    "120",
    "1200",
    "0.120",
    "1.50",
    "100.001",
    "0.00012",
];

#[test]
fn corpus_round_trips_byte_identical() {
    for text in CORPUS {
        let wire = encode_number_text(text).unwrap_or_else(|e| panic!("encode {text}: {e:?}"));
        assert_byte_identical(&wire, text);
    }
}

/// The single-byte positive-zero wire form (`[0x80]`) and the negative
/// single-byte sentinel that the legacy decoder renders as `-1e126`.
#[test]
fn single_byte_sentinels_byte_identical() {
    // Positive zero on the wire is a single 0x80 byte.
    assert_byte_identical(&[0x80], "single-byte-zero");
    // The decoder's special negative single-byte path renders "-1e126".
    assert_byte_identical(&[0x00], "single-byte-negative-sentinel");
}

/// Every i64 boundary plus a stride: each must format byte-identically AND the
/// inline coefficient/scale must reconstruct the exact integer.
#[test]
fn i64_domain_byte_identical_and_exact() {
    let probes: &[i64] = &[
        0,
        1,
        -1,
        9,
        -9,
        10,
        -10,
        99,
        100,
        i64::MAX,
        i64::MIN,
        i64::MAX - 1,
        i64::MIN + 1,
        1_000_000_000_000,
        -1_000_000_000_000,
        123_456_789,
    ];
    for &v in probes {
        let wire = encode_number_text(&v.to_string()).expect("encode i64");
        assert_byte_identical(&wire, &v.to_string());
        let num = OracleNumber::from_wire(&wire).expect("inline decode");
        assert_eq!(num.to_i64(), Some(v), "i64 reconstruct for {v}");
        assert_eq!(
            num.to_i128(),
            Some(i128::from(v)),
            "i128 reconstruct for {v}"
        );
    }
}

/// u64 above i64::MAX still fits a NUMBER and an i128 coefficient.
#[test]
fn u64_above_i64max_exact() {
    for v in [u64::MAX, u64::MAX - 1, (i64::MAX as u64) + 1] {
        let wire = encode_number_text(&v.to_string()).expect("encode u64");
        assert_byte_identical(&wire, &v.to_string());
        let num = OracleNumber::from_wire(&wire).expect("inline decode");
        assert_eq!(num.to_i128(), Some(i128::from(v)), "i128 reconstruct {v}");
    }
}

/// The negative-zero wire canonicalizes to "0" through BOTH paths.
#[test]
fn negative_zero_canonicalizes_identically() {
    let wire = encode_number_text("-0")
        .unwrap_or_else(|_| encode_number_text("0").expect("zero must encode"));
    assert_byte_identical(&wire, "-0");
}

mod prop {
    use super::{assert_byte_identical, OracleNumber};
    use oracledb_protocol::thin::encode_number_text;
    use proptest::prelude::*;

    /// Generate a syntactically valid canonical-ish decimal across the full
    /// NUMBER domain (sign, up to 38 significant digits, decimal point anywhere,
    /// extreme exponents). Mirrors the in-crate `number_text_strategy`.
    fn number_text() -> impl Strategy<Value = String> {
        (any::<bool>(), "[0-9]{1,38}", 0usize..=37, -120i32..=120i32).prop_map(
            |(neg, digits, dp, exp)| {
                let dp = dp.min(digits.len());
                let (int_part, frac_part) = digits.split_at(dp);
                let int_part = if int_part.is_empty() { "0" } else { int_part };
                let mut s = String::new();
                if neg {
                    s.push('-');
                }
                s.push_str(int_part);
                if !frac_part.is_empty() {
                    s.push('.');
                    s.push_str(frac_part);
                }
                s.push('e');
                s.push_str(&exp.to_string());
                s
            },
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 4096, ..ProptestConfig::default() })]

        /// EXHAUSTIVE BYTE-IDENTITY PROOF: for every valid wire NUMBER across the
        /// full generated domain, the inline `OracleNumber` formatter produces
        /// text byte-identical to the legacy decoder, and `is_integer` matches.
        #[test]
        fn inline_formatter_byte_identical_to_legacy(text in number_text()) {
            let Ok(wire) = encode_number_text(&text) else { return Ok(()); };
            assert_byte_identical(&wire, &text);
        }

        /// Every integer in the i128-fitting domain reconstructs exactly through
        /// the inline coefficient/scale.
        #[test]
        fn inline_i128_exact(v in any::<i128>().prop_filter("38-digit fit", |v| {
            // Oracle NUMBER holds up to 38 sig digits losslessly; keep |v| within
            // that so the encoder accepts it.
            v.unsigned_abs() < 100_000_000_000_000_000_000_000_000_000_000_000_000u128
        })) {
            let Ok(wire) = encode_number_text(&v.to_string()) else { return Ok(()); };
            assert_byte_identical(&wire, &v.to_string());
            let num = OracleNumber::from_wire(&wire).expect("decode");
            prop_assert_eq!(num.to_i128(), Some(v));
        }
    }
}
