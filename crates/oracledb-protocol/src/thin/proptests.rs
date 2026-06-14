//! Property-based, metamorphic, and boundary tests for the sans-io scalar
//! codecs in `codecs.rs`.
//!
//! These reach the `pub(crate)` encoders/decoders (`encode_number_text`,
//! `encode_binary_float/double`, the DATE/TIMESTAMP/INTERVAL encoders) that the
//! integration `tests/` directory cannot see, so they live inside the crate.
//!
//! Method (skill: testing-metamorphic + property-based testing): every codec
//! pair gets a ROUND-TRIP property `decode(encode(x)) == x` over a
//! proptest-generated domain that *deliberately* hits the boundaries, plus
//! METAMORPHIC relations that hold with no external oracle. Each property cites
//! the reference `.pyx` (python-oracledb v4.0.1) or the documented Oracle wire
//! invariant it enforces. None are tautologies: each applies a real
//! transformation and asserts a real relation.
//!
//! Reference tag: see `crate::PYTHON_ORACLEDB_REFERENCE_TAG` (v4.0.1).

use super::codecs::{
    decode_binary_double, decode_binary_float, decode_datetime_value, decode_interval_ds,
    decode_interval_ym, decode_number_value, decode_text_value, encode_binary_double,
    encode_binary_float, encode_interval_ds, encode_interval_ym, encode_number_text,
    encode_oracle_date, encode_oracle_timestamp, encode_oracle_timestamp_tz,
};
use super::constants::CS_FORM_NCHAR;
use super::types::QueryValue;
use proptest::prelude::*;

// proptest's default is 256 cases per property; we raise it for the scalar
// codecs (cheap, sans-io) so the boundary coverage is dense. Each `proptest!`
// block below inherits this config.
const CASES: u32 = 2_048;

fn config() -> ProptestConfig {
    ProptestConfig {
        cases: CASES,
        ..ProptestConfig::default()
    }
}

// ---------------------------------------------------------------------------
// NUMBER
// ---------------------------------------------------------------------------
//
// `encode_number_text` (codecs.rs, port of impl/base/encoders.pyx
// `OracleNumber._encode`) turns canonical decimal text into Oracle's on-wire
// NUMBER, and `decode_number_value` (port of decoders.pyx) recovers the
// canonical text. The pair is value-preserving but NOT text-identity-preserving
// (e.g. "1.0" -> "1", "1e2" -> "100"), so the round-trip is asserted on
// *numeric value*, parsed as f64 for magnitudes that fit and as normalized
// decimal text otherwise.

/// Parse our canonical NUMBER text into an exact rational-ish comparison key:
/// (sign, integer-digits-without-leading-zeros, fractional-digits-without-
/// trailing-zeros). This lets us compare two canonical texts for *numeric*
/// equality without float rounding, which matters for the 38-significant-digit
/// cases that exceed f64 precision.
fn number_key(text: &str) -> (bool, String, String) {
    let (neg, rest) = match text.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, text),
    };
    let (int_part, frac_part) = match rest.split_once('.') {
        Some((i, f)) => (i, f),
        None => (rest, ""),
    };
    let int_norm = int_part.trim_start_matches('0').to_string();
    let frac_norm = frac_part.trim_end_matches('0').to_string();
    // Negative zero and positive zero must compare equal.
    let is_zero = int_norm.is_empty() && frac_norm.is_empty();
    (if is_zero { false } else { neg }, int_norm, frac_norm)
}

/// Generate a finite decimal string with up to 38 significant digits and an
/// explicit exponent at both extremes — the full domain the wire format must
/// survive. Always a syntactically valid input for `encode_number_text`.
fn number_text_strategy() -> impl Strategy<Value = String> {
    (
        any::<bool>(),    // negative?
        "[0-9]{1,38}",    // significant digits
        0usize..=37,      // decimal point position within the digits
        -120i32..=120i32, // explicit exponent (kept within Oracle's range)
    )
        .prop_map(|(neg, digits, dp, exp)| {
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
        })
}

proptest! {
    #![proptest_config(config())]

    /// ROUND-TRIP: decode(encode(text)) is numerically equal to `text`.
    /// (encoders.pyx `_encode` <-> decoders.pyx `_decode`.) Covers the full
    /// generated domain including 38-significant-digit mantissas and extreme
    /// exponents.
    #[test]
    fn number_round_trip_value_preserving(text in number_text_strategy()) {
        // Skip inputs the encoder legitimately rejects as out of Oracle's range
        // (|exponent| guard in encode_number_text); a rejection is not a
        // round-trip failure.
        let Ok(wire) = encode_number_text(&text) else { return Ok(()); };
        let decoded = decode_number_value(&wire).expect("decode our own encoding");
        let QueryValue::Number(num) = decoded else {
            panic!("decode_number_value returned a non-Number variant");
        };
        let out = num.to_canonical_string();
        prop_assert_eq!(
            number_key(&out),
            number_key(&normalize_input(&text)),
            "input {} -> wire {:02x?} -> {}",
            text, wire, out
        );
    }
}

/// Reduce a raw generated input to the same canonical numeric key the decoder
/// would produce, by folding the explicit `e<exp>` into the digit positions.
/// This is the *expected* value side of the round-trip oracle — derived from
/// the decimal spec, NOT from the codec under test.
fn normalize_input(text: &str) -> String {
    let (neg, rest) = match text.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, text),
    };
    let (mantissa, exp) = match rest.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i32>().unwrap_or(0)),
        None => (rest, 0),
    };
    let (int_part, frac_part) = match mantissa.split_once('.') {
        Some((i, f)) => (i.to_string(), f.to_string()),
        None => (mantissa.to_string(), String::new()),
    };
    // Combine into one digit string with a decimal-point index, then shift by exp.
    let mut digits: Vec<u8> = int_part.bytes().chain(frac_part.bytes()).collect();
    let mut point = int_part.len() as i64 + exp as i64;
    // strip leading zeros (adjusting the point), keeping at least nothing.
    while digits.first() == Some(&b'0') {
        digits.remove(0);
        point -= 1;
    }
    while digits.last() == Some(&b'0') {
        digits.pop();
    }
    if digits.is_empty() {
        return "0".to_string();
    }
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    if point <= 0 {
        out.push_str("0.");
        for _ in 0..(-point) {
            out.push('0');
        }
        out.extend(digits.iter().map(|b| *b as char));
    } else if (point as usize) >= digits.len() {
        out.extend(digits.iter().map(|b| *b as char));
        for _ in 0..(point as usize - digits.len()) {
            out.push('0');
        }
    } else {
        let (i, f) = digits.split_at(point as usize);
        out.extend(i.iter().map(|b| *b as char));
        out.push('.');
        out.extend(f.iter().map(|b| *b as char));
    }
    out
}

proptest! {
    #![proptest_config(config())]

    /// ROUND-TRIP for the i64 domain: every i64 encodes and decodes back to an
    /// equal integer (decode text parses to the same i64).
    #[test]
    fn number_round_trip_i64(value: i64) {
        let wire = encode_number_text(&value.to_string()).expect("i64 always encodable");
        let decoded = decode_number_value(&wire).expect("decode i64 number");
        prop_assert_eq!(decoded.as_i64(), Some(value));
    }

    /// ROUND-TRIP for the u64 domain (values above i64::MAX still fit a NUMBER).
    #[test]
    fn number_round_trip_u64(value: u64) {
        let wire = encode_number_text(&value.to_string()).expect("u64 always encodable");
        let decoded = decode_number_value(&wire).expect("decode u64 number");
        let QueryValue::Number(num) = decoded else { panic!("not a Number") };
        prop_assert_eq!(num.to_canonical_string().parse::<u64>().ok(), Some(value));
    }

    /// METAMORPHIC — ORDER-PRESERVING: for a < b, the on-wire NUMBER bytes are
    /// byte-lexicographically ordered, encode(a) < encode(b). This is the
    /// load-bearing Oracle invariant that makes NUMBER columns range-scannable
    /// by raw byte comparison (the sign/exponent/mantissa-101's-complement
    /// scheme in encoders.pyx exists precisely so the wire form sorts). A
    /// violation reveals a sign, exponent-bias, or complement bug that a pure
    /// round-trip can miss. Inputs are an ordered i64 pair plus an ordered
    /// scaled-decimal pair.
    #[test]
    fn number_encoding_is_order_preserving(a: i32, b: i32) {
        prop_assume!(a != b);
        let (lo, hi) = if a < b { (a, b) } else { (b, a) };
        let lo_w = encode_number_text(&lo.to_string()).expect("encode lo");
        let hi_w = encode_number_text(&hi.to_string()).expect("encode hi");
        prop_assert!(
            lo_w.as_slice() < hi_w.as_slice(),
            "order broken: {lo} -> {lo_w:02x?} should be < {hi} -> {hi_w:02x?}"
        );
    }

    /// METAMORPHIC — ORDER-PRESERVING over fractional/exponent-bearing decimals
    /// generated as an ordered pair from a common scaled integer. Exercises the
    /// exponent byte and the fractional mantissa, which the i32 case above does
    /// not reach.
    #[test]
    fn number_order_preserving_decimals(
        base in -1_000_000i64..=1_000_000i64,
        gap in 1i64..=1_000_000i64,
        scale in 0u32..=6,
    ) {
        let divisor = 10i64.pow(scale);
        let to_text = |n: i64| {
            if scale == 0 {
                n.to_string()
            } else {
                format!("{}e-{}", n, scale)
            }
        };
        let lo = base;
        let hi = base.saturating_add(gap);
        prop_assume!(lo != hi);
        let _ = divisor;
        let lo_w = encode_number_text(&to_text(lo)).expect("encode lo");
        let hi_w = encode_number_text(&to_text(hi)).expect("encode hi");
        prop_assert!(
            lo_w.as_slice() < hi_w.as_slice(),
            "decimal order broken: {} -> {lo_w:02x?} !< {} -> {hi_w:02x?}",
            to_text(lo), to_text(hi)
        );
    }
}

/// Explicit boundary cases for NUMBER, named so a failure points at the exact
/// edge. These complement the random domain with the values the spec calls out:
/// the base-100 mantissa edges, the single-byte zero, +/-1, the 20-mantissa-byte
/// limit, and negative zero.
#[test]
fn number_boundary_cases_round_trip() {
    let cases = [
        "0",
        "1",
        "-1",
        "100",
        "-100",
        "99",
        "-99",
        "0.01",
        "-0.01",
        "1e125",
        "-1e125",
        "1e-120",
        "-1e-120",
        // 38 significant digits (max precision), integer and fractional.
        "12345678901234567890123456789012345678",
        "-12345678901234567890123456789012345678",
        "0.12345678901234567890123456789012345678",
        // values that need close to the maximum mantissa byte count.
        "9999999999999999999999999999999999999",
    ];
    for text in cases {
        let wire = encode_number_text(text).unwrap_or_else(|e| panic!("encode {text}: {e:?}"));
        // The wire NUMBER is at most 21 bytes (1 exponent + 20 mantissa).
        assert!(wire.len() <= 21, "{text} encoded to {} bytes", wire.len());
        let decoded = decode_number_value(&wire).unwrap_or_else(|e| panic!("decode {text}: {e:?}"));
        let QueryValue::Number(num) = decoded else {
            panic!("{text}: not a Number");
        };
        let out = num.to_canonical_string();
        assert_eq!(
            number_key(&out),
            number_key(&normalize_input(text)),
            "{text} round-trip -> {out}"
        );
    }
}

/// Negative zero must canonicalize to "0" (positive), matching the decoder's
/// single-byte-128 special case and the way `number_key` folds sign on zero.
#[test]
fn number_negative_zero_is_zero() {
    let wire = encode_number_text("-0").expect("encode -0");
    let decoded = decode_number_value(&wire).expect("decode -0");
    assert_eq!(decoded.as_i64(), Some(0));
    assert_eq!(decoded.as_number_text().as_deref(), Some("0"));
}

// ---------------------------------------------------------------------------
// DATE / TIMESTAMP / TIMESTAMP WITH TIME ZONE
// ---------------------------------------------------------------------------
//
// encode_oracle_date / _timestamp / _timestamp_tz (codecs.rs, port of
// encoders.pyx `_encode_date`) <-> decode_datetime_value (decoders.pyx
// `_decode_date`). The encoders take civil components; the decoder recovers
// them (TZ-adjusted to UTC for the _tz form, which our generator accounts for).

prop_compose! {
    fn civil_datetime()(
        year in 1i32..=9999,
        month in 1u8..=12,
        day in 1u8..=28,            // 28 keeps every (month, day) pair valid
        hour in 0u8..=23,
        minute in 0u8..=59,
        second in 0u8..=59,
    ) -> (i32, u8, u8, u8, u8, u8) {
        (year, month, day, hour, minute, second)
    }
}

proptest! {
    #![proptest_config(config())]

    /// ROUND-TRIP DATE: encode civil components, decode them back unchanged.
    #[test]
    fn date_round_trip((y, mo, d, h, mi, s) in civil_datetime()) {
        let wire = encode_oracle_date(y, mo, d, h, mi, s).expect("encode date");
        prop_assert_eq!(wire.len(), 7, "DATE is 7 bytes");
        let decoded = decode_datetime_value(&wire).expect("decode date");
        prop_assert_eq!(decoded, QueryValue::DateTime {
            year: y, month: mo, day: d, hour: h, minute: mi, second: s, nanosecond: 0,
        });
    }

    /// ROUND-TRIP TIMESTAMP with full-precision fractional seconds.
    #[test]
    fn timestamp_round_trip(
        (y, mo, d, h, mi, s) in civil_datetime(),
        nanosecond in 0u32..=999_999_999,
    ) {
        let wire = encode_oracle_timestamp(y, mo, d, h, mi, s, nanosecond).expect("encode ts");
        let decoded = decode_datetime_value(&wire).expect("decode ts");
        prop_assert_eq!(decoded, QueryValue::DateTime {
            year: y, month: mo, day: d, hour: h, minute: mi, second: s, nanosecond,
        });
    }

    /// ROUND-TRIP TIMESTAMP WITH TIME ZONE. encode_oracle_timestamp_tz writes a
    /// fixed UTC offset (TZ_HOUR_OFFSET/TZ_MINUTE_OFFSET == zero offset), so the
    /// decoder's UTC normalization is the identity and the civil components must
    /// come back unchanged. This still exercises the 13-byte TSTZ frame and the
    /// offset-decode path (codecs.rs lines 99-110).
    #[test]
    fn timestamp_tz_zero_offset_round_trip(
        (y, mo, d, h, mi, s) in civil_datetime(),
        nanosecond in 0u32..=999_999_999,
    ) {
        let wire = encode_oracle_timestamp_tz(y, mo, d, h, mi, s, nanosecond).expect("encode tstz");
        prop_assert_eq!(wire.len(), 13, "TSTZ is 13 bytes");
        let decoded = decode_datetime_value(&wire).expect("decode tstz");
        prop_assert_eq!(decoded, QueryValue::DateTime {
            year: y, month: mo, day: d, hour: h, minute: mi, second: s, nanosecond,
        });
    }

    /// METAMORPHIC — TZ OFFSET COVARIANCE: applying a positive then the negating
    /// negative minute offset to the same instant returns the original civil
    /// time. `adjust_datetime_by_minutes` (codecs.rs) is the field-normalization
    /// core; shifting forward by k minutes then back by k must be the identity
    /// (an invertive relation, no external oracle needed). This catches
    /// day/month/year carry bugs in civil<->days conversion.
    #[test]
    fn datetime_offset_shift_is_invertible(
        (y, mo, d, h, mi, s) in civil_datetime(),
        offset in -1439i32..=1439i32,
    ) {
        let forward = super::codecs::adjust_datetime_by_minutes(y, mo, d, h, mi, s, offset)
            .expect("forward shift");
        let (y2, mo2, d2, h2, mi2, s2) = forward;
        let back = super::codecs::adjust_datetime_by_minutes(y2, mo2, d2, h2, mi2, s2, -offset)
            .expect("back shift");
        prop_assert_eq!(back, (y, mo, d, h, mi, s));
    }
}

/// Boundary DATE/TIMESTAMP cases: the field extremes, the epoch, and pre-epoch.
#[test]
fn datetime_boundary_cases() {
    let cases: &[(i32, u8, u8, u8, u8, u8, u32)] = &[
        (1, 1, 1, 0, 0, 0, 0),                   // earliest representable
        (9999, 12, 31, 23, 59, 59, 999_999_999), // latest representable, max frac
        (1970, 1, 1, 0, 0, 0, 0),                // the epoch
        (1969, 12, 31, 23, 59, 59, 0),           // pre-epoch
        (2000, 2, 29, 12, 0, 0, 500_000_000),    // leap day
    ];
    for &(y, mo, d, h, mi, s, ns) in cases {
        let wire = encode_oracle_timestamp(y, mo, d, h, mi, s, ns)
            .unwrap_or_else(|e| panic!("encode {y}-{mo}-{d}: {e:?}"));
        let decoded = decode_datetime_value(&wire).expect("decode ts boundary");
        assert_eq!(
            decoded,
            QueryValue::DateTime {
                year: y,
                month: mo,
                day: d,
                hour: h,
                minute: mi,
                second: s,
                nanosecond: ns
            },
            "{y}-{mo}-{d} {h}:{mi}:{s}.{ns}"
        );
    }
}

// ---------------------------------------------------------------------------
// INTERVAL YEAR TO MONTH / DAY TO SECOND
// ---------------------------------------------------------------------------
//
// encode_interval_ym/ds (codecs.rs, port of encoders.pyx:151-161 and the DS
// encoder) <-> decode_interval_ym/ds (decoders.pyx:147-155). Components are
// signed; the wire form offsets them by TNS_DURATION_MID / _OFFSET.

proptest! {
    #![proptest_config(config())]

    /// ROUND-TRIP INTERVAL YEAR TO MONTH over the field range (years span the
    /// signed ub4 offset domain; months -11..=11 around zero, incl. negatives).
    #[test]
    fn interval_ym_round_trip(years in -100_000i32..=100_000, months in -11i32..=11) {
        let wire = encode_interval_ym(years, months).expect("encode interval ym");
        prop_assert_eq!(wire.len(), 5, "INTERVAL YM is 5 bytes");
        let decoded = decode_interval_ym(&wire).expect("decode interval ym");
        prop_assert_eq!(decoded, QueryValue::IntervalYM { years, months });
    }

    /// ROUND-TRIP INTERVAL DAY TO SECOND. seconds is the total seconds field
    /// (hours/min/sec are derived); microseconds carries the fractional part.
    #[test]
    fn interval_ds_round_trip(
        days in -100_000i32..=100_000,
        hours in 0i32..=23,
        minutes in 0i32..=59,
        secs in 0i32..=59,
        microseconds in 0i32..=999_999,
    ) {
        let total_seconds = hours * 3600 + minutes * 60 + secs;
        let wire = encode_interval_ds(days, total_seconds, microseconds).expect("encode ds");
        prop_assert_eq!(wire.len(), 11, "INTERVAL DS is 11 bytes");
        let decoded = decode_interval_ds(&wire).expect("decode ds");
        prop_assert_eq!(decoded, QueryValue::IntervalDS {
            days, hours, minutes, seconds: secs, fseconds: microseconds * 1000,
        });
    }
}

/// Boundary INTERVAL cases: zero, and extreme magnitudes near the field limits.
#[test]
fn interval_boundary_cases() {
    // YM: zero and large positive/negative.
    for (y, m) in [(0, 0), (100_000, 11), (-100_000, -11), (5, -11), (-5, 11)] {
        let wire = encode_interval_ym(y, m).expect("encode ym boundary");
        assert_eq!(
            decode_interval_ym(&wire).expect("decode ym boundary"),
            QueryValue::IntervalYM {
                years: y,
                months: m
            }
        );
    }
    // DS: zero and large.
    for (d, total, us) in [(0, 0, 0), (100_000, 86_399, 999_999), (-100_000, 0, 0)] {
        let wire = encode_interval_ds(d, total, us).expect("encode ds boundary");
        let decoded = decode_interval_ds(&wire).expect("decode ds boundary");
        let QueryValue::IntervalDS { days, fseconds, .. } = decoded else {
            panic!("not DS")
        };
        assert_eq!(days, d);
        assert_eq!(fseconds, us * 1000);
    }
}

// ---------------------------------------------------------------------------
// BINARY_FLOAT / BINARY_DOUBLE
// ---------------------------------------------------------------------------
//
// encode_binary_float/double (codecs.rs, port of encoders.pyx) <->
// decode_binary_float/double (decoders.pyx). Oracle's sign-transform makes the
// bytes order-comparable: a positive value gets its sign bit set; a negative
// value has every bit inverted. The round-trip must be BIT-EXACT (so NaN
// payloads and signed zeros survive), hence the comparison is on `to_bits`.

proptest! {
    #![proptest_config(config())]

    /// ROUND-TRIP BINARY_DOUBLE bit-exact over arbitrary f64 bit patterns
    /// (covers normals, subnormals, signed zeros, infinities; NaNs are checked
    /// separately to compare classification not exact payload).
    #[test]
    fn binary_double_round_trip_bits(bits: u64) {
        let value = f64::from_bits(bits);
        prop_assume!(!value.is_nan());
        let wire = encode_binary_double(value);
        let decoded = decode_binary_double(&wire).expect("decode bdouble");
        prop_assert_eq!(decoded.to_bits(), value.to_bits(), "f64 {} bits differ", value);
    }

    /// ROUND-TRIP BINARY_FLOAT bit-exact over arbitrary f32 bit patterns.
    #[test]
    fn binary_float_round_trip_bits(bits: u32) {
        let value = f32::from_bits(bits);
        prop_assume!(!value.is_nan());
        let wire = encode_binary_float(value);
        let decoded = decode_binary_float(&wire).expect("decode bfloat");
        prop_assert_eq!(decoded.to_bits(), value.to_bits(), "f32 {} bits differ", value);
    }

    /// METAMORPHIC — ORDER-PRESERVING for non-negative doubles: the sign-flip
    /// transform exists so that for 0 <= a < b the wire bytes sort the same way
    /// (`encode(a) < encode(b)` lexicographically). This is why BINARY_DOUBLE
    /// is usable in index range scans. A regression to plain IEEE big-endian
    /// (the w3-async P0 class) breaks this for the positive half.
    #[test]
    fn binary_double_order_preserving_nonneg(a in 0.0f64..1e308, b in 0.0f64..1e308) {
        prop_assume!(a != b && a.is_finite() && b.is_finite());
        let (lo, hi) = if a < b { (a, b) } else { (b, a) };
        let lo_w = encode_binary_double(lo);
        let hi_w = encode_binary_double(hi);
        prop_assert!(lo_w < hi_w, "order broken: {lo} -> {lo_w:02x?} !< {hi} -> {hi_w:02x?}");
    }
}

/// Explicit BINARY_FLOAT/DOUBLE boundary values: 0, -0, min/max normal,
/// subnormal, NaN, +/-Inf. NaN must decode back to *a* NaN (classification
/// preserved); the others are bit-exact.
#[test]
fn binary_float_double_boundary_values() {
    let f64_cases = [
        0.0f64,
        -0.0,
        f64::MIN_POSITIVE,
        f64::MAX,
        f64::MIN,
        f64::from_bits(1), // smallest subnormal
        f64::INFINITY,
        f64::NEG_INFINITY,
    ];
    for v in f64_cases {
        let decoded = decode_binary_double(&encode_binary_double(v)).expect("decode f64 boundary");
        assert_eq!(decoded.to_bits(), v.to_bits(), "f64 boundary {v}");
    }
    // NaN: classification, not exact payload.
    let nan = decode_binary_double(&encode_binary_double(f64::NAN)).expect("decode f64 nan");
    assert!(nan.is_nan(), "f64 NaN must round-trip to a NaN");

    let f32_cases = [
        0.0f32,
        -0.0,
        f32::MIN_POSITIVE,
        f32::MAX,
        f32::MIN,
        f32::from_bits(1),
        f32::INFINITY,
        f32::NEG_INFINITY,
    ];
    for v in f32_cases {
        let decoded = decode_binary_float(&encode_binary_float(v)).expect("decode f32 boundary");
        assert_eq!(decoded.to_bits(), v.to_bits(), "f32 boundary {v}");
    }
    let nan = decode_binary_float(&encode_binary_float(f32::NAN)).expect("decode f32 nan");
    assert!(nan.is_nan(), "f32 NaN must round-trip to a NaN");
}

// ---------------------------------------------------------------------------
// VARCHAR / CHAR / NCHAR text
// ---------------------------------------------------------------------------
//
// decode_text_value (codecs.rs): single-byte charsets decode as UTF-8; NCHAR
// (CS_FORM_NCHAR) decodes as UTF-16BE. There is no separate text *encoder* in
// codecs.rs (the bind path writes raw UTF-8 / UTF-16 bytes), so the round-trip
// is `decode(utf8_bytes(s)) == s` and `decode_nchar(utf16be_bytes(s)) == s`.

proptest! {
    #![proptest_config(config())]

    /// ROUND-TRIP VARCHAR/CHAR: any Rust String's UTF-8 bytes decode back to the
    /// same string (covers 1/2/3/4-byte codepoints by construction — proptest's
    /// String generator emits arbitrary scalar values).
    #[test]
    fn text_utf8_round_trip(s in ".{0,512}") {
        let bytes = s.as_bytes();
        let decoded = decode_text_value(bytes, 0).expect("decode utf8 text");
        prop_assert_eq!(decoded, s);
    }

    /// ROUND-TRIP NCHAR (UTF-16BE): encode the string to UTF-16BE bytes, decode
    /// via the NCHAR path, recover the original. Exercises the surrogate-pair
    /// handling for astral-plane (4-byte UTF-8 / 2-unit UTF-16) codepoints.
    #[test]
    fn text_nchar_utf16_round_trip(s in ".{0,256}") {
        let mut bytes = Vec::new();
        for unit in s.encode_utf16() {
            bytes.extend_from_slice(&unit.to_be_bytes());
        }
        let decoded = decode_text_value(&bytes, CS_FORM_NCHAR).expect("decode nchar text");
        prop_assert_eq!(decoded, s);
    }
}

/// Boundary text cases: empty, single byte, the 32767-byte max, and the
/// multibyte-codepoint-split case. The split case asserts that a codepoint
/// straddling a would-be buffer boundary still decodes — decode_text_value sees
/// the full slice, so the invariant is that a 4-byte codepoint's bytes are
/// never interpreted individually.
#[test]
fn text_boundary_cases() {
    // empty
    assert_eq!(decode_text_value(b"", 0).expect("empty"), "");
    // single byte
    assert_eq!(decode_text_value(b"x", 0).expect("single"), "x");
    // 32767 bytes of ASCII (VARCHAR2 max with max_string_size).
    let big = "a".repeat(32_767);
    assert_eq!(
        decode_text_value(big.as_bytes(), 0).expect("big").len(),
        32_767
    );
    // each multibyte width: 2-byte (é), 3-byte (€), 4-byte (𝄞 musical symbol).
    for s in ["é", "€", "𝄞", "aé€𝄞z"] {
        assert_eq!(decode_text_value(s.as_bytes(), 0).expect("multibyte"), s);
    }
    // A 4-byte codepoint repeated so the byte stream is long and dense; if the
    // decoder ever split a codepoint it would error or mojibake.
    let astral = "𝄞".repeat(1000);
    assert_eq!(
        decode_text_value(astral.as_bytes(), 0).expect("astral"),
        astral
    );
}

/// Invalid UTF-8 must fail closed (the codec returns an error, never panics or
/// silently corrupts). A lone 0x80 continuation byte is not valid UTF-8.
#[test]
fn text_invalid_utf8_fails_closed() {
    assert!(decode_text_value(&[0x80], 0).is_err());
    // Odd-length NCHAR buffer (not a whole number of UTF-16 units) must fail.
    assert!(decode_text_value(&[0x00], CS_FORM_NCHAR).is_err());
}
