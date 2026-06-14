#![forbid(unsafe_code)]

//! Inline, lossless Oracle `NUMBER` representation (bead rust-oracledb-65w).
//!
//! Oracle `NUMBER` is up to 40 significant decimal digits (the wire form carries
//! up to 20 base-100 mantissa bytes) with a decimal exponent in roughly
//! `-130..=125`. The common case — a value with at most 38 significant digits —
//! fits losslessly in an `i128` coefficient plus an `i16` scale, allocating
//! nothing. The owned [`crate::thin::QueryValue::Number`] used to carry a heap
//! `String` per cell; this module replaces that inline payload so a NUMBER-heavy
//! row stops doing one `malloc` per NUMBER column.
//!
//! ## Losslessness
//!
//! Some wire forms cannot be represented exactly inline:
//!
//! - A 39- or 40-digit integer can exceed `i128::MAX` (`~1.7e38`, 39 digits).
//! - The decoder's special single-byte negative sentinel renders as the literal
//!   text `-1e126`, which is not a plain `coefficient × 10^-scale` decimal.
//!
//! For any such value the representation FALLS BACK to a boxed canonical-text
//! carrier ([`OracleNumber::Text`]) so correctness is never sacrificed. The
//! fallback is boxed (`Box<str>`) so the enum — and therefore
//! [`crate::thin::QueryValue`] — stays within its 32-byte budget.
//!
//! ## Single shared formatter
//!
//! [`OracleNumber::fmt_into`] is the ONE canonical formatter. It is BYTE-IDENTICAL
//! to the legacy [`super::codecs::decode_number_text_into`] text path (proven by
//! `tests/number_inline_byte_identical.rs` over the whole NUMBER domain). Every
//! consumer — `Display`, `FromSql<String>`, the OSON/JSON number text, and the
//! borrowed `QueryValueRef::Number` arena path — routes through it, so the owned
//! and borrowed decode paths can never diverge by even one byte.

use crate::{ProtocolError, Result};

/// Upper bound on the significant decimal digits the wire NUMBER digit walk can
/// emit into a stack buffer. Oracle NUMBER carries at most 40 significant
/// digits (20 base-100 mantissa bytes); +2 slack covers the `first_digit == 10`
/// base-100 carry the legacy walk can append.
pub(crate) const MAX_DIGITS: usize = 42;

/// Stack-decoded parts of a wire NUMBER (no heap allocation). Mirror of
/// [`DecodedNumber`] but with digits written into a caller stack buffer.
pub(crate) enum DecodedNumberStack {
    /// A single-byte sentinel whose canonical text is fixed.
    Sentinel {
        text: &'static str,
        is_integer: bool,
    },
    /// The decoded parts; `digit_len` significant digits were written to the
    /// caller's stack buffer.
    Parts {
        digit_len: usize,
        is_negative: bool,
        decimal_point_index: i16,
        is_integer: bool,
    },
}

/// Inline, lossless decimal carrier for an Oracle `NUMBER`.
///
/// The common case is [`OracleNumber::Inline`] (`coefficient × 10^-scale`,
/// allocation-free). Values that cannot be represented exactly inline fall back
/// to [`OracleNumber::Text`] (a boxed canonical-text carrier).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OracleNumber {
    /// `value == coefficient × 10^-scale`, with the sign carried in
    /// `coefficient`. `scale` may be negative (the value has trailing zeros to
    /// the left of the implied point). `is_integer` mirrors the legacy decoder's
    /// flag — whether the canonical text contains a decimal point — so the
    /// Python int-vs-float dispatch is preserved exactly.
    ///
    /// The coefficient is stored as its little-endian `i128` bytes rather than a
    /// bare `i128` field: a bare `i128` forces 16-byte alignment, which rounds
    /// the enum up to 32 bytes and would blow `QueryValue`'s 32-byte budget once
    /// the discriminant is added. The `[u8; 16]` form keeps 8-byte alignment so
    /// `OracleNumber` is 24 bytes. Access via [`OracleNumber::coefficient`].
    Inline {
        coefficient_le: [u8; 16],
        scale: i16,
        is_integer: bool,
    },
    /// Defensive fallback for values that do not fit the inline form exactly
    /// (39–40 significant digit integers that overflow `i128`, or the `-1e126`
    /// single-byte sentinel). Boxed so the enum stays small.
    Text { text: Box<str>, is_integer: bool },
}

impl OracleNumber {
    /// Build the inline variant from a real `i128` coefficient (stored as its
    /// little-endian bytes to keep the enum 8-byte aligned).
    fn inline(coefficient: i128, scale: i16, is_integer: bool) -> Self {
        OracleNumber::Inline {
            coefficient_le: coefficient.to_le_bytes(),
            scale,
            is_integer,
        }
    }

    /// The inline coefficient as an `i128`, or `None` for the boxed-text
    /// fallback. `value == coefficient × 10^-scale`.
    pub fn coefficient(&self) -> Option<i128> {
        match self {
            OracleNumber::Inline { coefficient_le, .. } => {
                Some(i128::from_le_bytes(*coefficient_le))
            }
            OracleNumber::Text { .. } => None,
        }
    }

    /// The inline scale, or `None` for the boxed-text fallback.
    pub fn scale(&self) -> Option<i16> {
        match self {
            OracleNumber::Inline { scale, .. } => Some(*scale),
            OracleNumber::Text { .. } => None,
        }
    }

    /// Decode an Oracle `NUMBER` wire form into the inline representation,
    /// falling back to a boxed canonical-text carrier when the value cannot be
    /// represented exactly inline. The canonical text — whether produced inline
    /// or stored in the fallback — is byte-identical to the legacy decoder.
    ///
    /// ZERO-ALLOCATION for the common inline case: the digit walk writes into a
    /// fixed stack buffer (Oracle NUMBER has at most 40 significant digits), and
    /// the inline coefficient/scale is folded directly — no scratch `Vec`/`String`
    /// is heap-allocated. Only the rare text fallback (sentinel / i128 overflow)
    /// touches the heap, and only then.
    pub fn from_wire(bytes: &[u8]) -> Result<Self> {
        // Stack scratch: up to 40 significant decimal digits + slack for the
        // base-100 carry the digit walk can append.
        let mut digit_buf = [0u8; MAX_DIGITS];
        match super::codecs::decode_number_parts_stack(bytes, &mut digit_buf)? {
            // Single-byte sentinels: format their canonical text once.
            DecodedNumberStack::Sentinel { text, is_integer } => Ok(OracleNumber::Text {
                text: text.into(),
                is_integer,
            }),
            DecodedNumberStack::Parts {
                digit_len,
                is_negative,
                decimal_point_index,
                is_integer,
            } => {
                let digits = &digit_buf[..digit_len];
                // Fold the decimal digits into an i128 coefficient. `digits` is
                // the significant-digit run (up to 40); >38 may overflow i128.
                match digits_to_i128(digits, is_negative) {
                    Some(coefficient) => {
                        // scale = len - decimal_point_index (implied fractional
                        // positions; may be negative for trailing-zero integers).
                        let len = i32::try_from(digits.len()).unwrap_or(i32::MAX);
                        let scale_i32 = len - i32::from(decimal_point_index);
                        match i16::try_from(scale_i32) {
                            Ok(scale) => Ok(OracleNumber::inline(coefficient, scale, is_integer)),
                            // Scale out of i16 range (cannot happen for valid
                            // Oracle NUMBER, but stay defensive): keep the text.
                            Err(_) => Ok(Self::spill_text(
                                digits,
                                is_negative,
                                decimal_point_index,
                                is_integer,
                            )),
                        }
                    }
                    // i128 overflow (39–40 digit value): spill to boxed text.
                    None => Ok(Self::spill_text(
                        digits,
                        is_negative,
                        decimal_point_index,
                        is_integer,
                    )),
                }
            }
        }
    }

    /// Format the digits into a boxed-text fallback (the rare path: i128 overflow
    /// or out-of-range scale). Uses the SAME formatter fragment as the inline
    /// path, so the text is byte-identical.
    fn spill_text(
        digits: &[u8],
        is_negative: bool,
        decimal_point_index: i16,
        is_integer: bool,
    ) -> Self {
        let mut text = String::new();
        super::codecs::format_number_digits(digits, is_negative, decimal_point_index, &mut text);
        OracleNumber::Text {
            text: text.into_boxed_str(),
            is_integer,
        }
    }

    /// Construct from already-canonical decimal text (the bind / parse path).
    /// Parses the text into the inline form when it fits, else keeps it boxed.
    /// The text MUST already be canonical Oracle `NUMBER` text (the form the
    /// decoder emits); this does not re-canonicalize.
    pub fn from_canonical_text(text: &str) -> Self {
        Self::from_canonical_text_with_flag(text, !text.contains('.'))
    }

    /// Like [`Self::from_canonical_text`] but with the caller-supplied
    /// `is_integer` flag (the borrowed fetch path already decoded it from the
    /// wire, so it is authoritative — preserve it verbatim).
    pub fn from_canonical_text_with_flag(text: &str, is_integer: bool) -> Self {
        match parse_canonical_inline(text) {
            Some((coefficient, scale)) => OracleNumber::inline(coefficient, scale, is_integer),
            None => OracleNumber::Text {
                text: text.into(),
                is_integer,
            },
        }
    }

    /// Borrow the canonical text when it is stored as boxed text (the fallback
    /// form), else `None` — the inline numeric form synthesizes its text on
    /// demand and has no `&str` to lend.
    pub fn as_borrowed_text(&self) -> Option<&str> {
        match self {
            OracleNumber::Text { text, .. } => Some(text),
            OracleNumber::Inline { .. } => None,
        }
    }

    /// Whether the canonical text is integral (carries no decimal point).
    /// Mirrors the legacy `is_integer` flag exactly.
    pub fn is_integer(&self) -> bool {
        match self {
            OracleNumber::Inline { is_integer, .. } | OracleNumber::Text { is_integer, .. } => {
                *is_integer
            }
        }
    }

    /// THE single shared canonical formatter. Appends the canonical decimal text
    /// to `out`. Byte-identical to [`super::codecs::decode_number_text_into`].
    pub fn fmt_into(&self, out: &mut String) {
        match self {
            OracleNumber::Text { text, .. } => out.push_str(text),
            OracleNumber::Inline {
                coefficient_le,
                scale,
                ..
            } => fmt_inline_into(i128::from_le_bytes(*coefficient_le), *scale, out),
        }
    }

    /// Canonical decimal text as an owned `String`.
    pub fn to_canonical_string(&self) -> String {
        let mut out = String::new();
        self.fmt_into(&mut out);
        out
    }

    /// Canonical decimal text as a `Cow`: borrowed for the boxed-text fallback
    /// (zero allocation), owned for the inline form (formatted once on demand).
    pub fn to_canonical_cow(&self) -> std::borrow::Cow<'_, str> {
        match self {
            OracleNumber::Text { text, .. } => std::borrow::Cow::Borrowed(text),
            OracleNumber::Inline { .. } => std::borrow::Cow::Owned(self.to_canonical_string()),
        }
    }

    /// Exact `i64` when the value is an integer that fits; else `None`.
    pub fn to_i64(&self) -> Option<i64> {
        match self {
            OracleNumber::Inline {
                coefficient_le,
                scale,
                ..
            } => inline_to_i128(i128::from_le_bytes(*coefficient_le), *scale)
                .and_then(|v| i64::try_from(v).ok()),
            OracleNumber::Text { text, .. } => text.parse::<i64>().ok(),
        }
    }

    /// Exact `i128` when the value is an integer that fits; else `None`.
    pub fn to_i128(&self) -> Option<i128> {
        match self {
            OracleNumber::Inline {
                coefficient_le,
                scale,
                ..
            } => inline_to_i128(i128::from_le_bytes(*coefficient_le), *scale),
            OracleNumber::Text { text, .. } => text.parse::<i128>().ok(),
        }
    }
}

/// Outcome of the wire digit walk: either a sentinel/overflow case that must be
/// kept as text, or the decoded parts the inline form is built from.
pub(crate) enum DecodedNumber {
    /// The canonical text is already in `text`; keep it verbatim (the special
    /// single-byte sentinel cases that are not plain `coeff × 10^-scale`).
    Text { is_integer: bool },
    /// Parts to fold into the inline coefficient/scale form.
    Parts {
        is_negative: bool,
        decimal_point_index: i16,
        is_integer: bool,
    },
}

/// Fold the significant decimal `digits` (each 0..=9) into an `i128` coefficient
/// with the given sign, returning `None` on overflow (39–40 digit values that
/// exceed `i128`).
fn digits_to_i128(digits: &[u8], is_negative: bool) -> Option<i128> {
    let mut acc: i128 = 0;
    for &d in digits {
        acc = acc.checked_mul(10)?.checked_add(i128::from(d))?;
    }
    if is_negative {
        Some(-acc)
    } else {
        Some(acc)
    }
}

/// Reconstruct an exact integer `i128` from the inline form, or `None` if the
/// value is fractional or the scaling overflows.
fn inline_to_i128(coefficient: i128, scale: i16) -> Option<i128> {
    match scale.cmp(&0) {
        std::cmp::Ordering::Equal => Some(coefficient),
        // Negative scale: value = coefficient × 10^(-scale), an integer.
        std::cmp::Ordering::Less => {
            let mut v = coefficient;
            for _ in 0..(-(i32::from(scale))) {
                v = v.checked_mul(10)?;
            }
            Some(v)
        }
        // Positive scale: integral only if the trailing `scale` digits are zero.
        std::cmp::Ordering::Greater => {
            let mut divisor: i128 = 1;
            for _ in 0..i32::from(scale) {
                divisor = divisor.checked_mul(10)?;
            }
            if coefficient % divisor == 0 {
                Some(coefficient / divisor)
            } else {
                None
            }
        }
    }
}

/// Format the inline `coefficient × 10^-scale` form into canonical Oracle
/// `NUMBER` text, BYTE-IDENTICAL to the legacy `decode_number_text_into`.
///
/// The legacy formatter works from `digits` (significant decimal digits, no
/// leading/trailing zeros except as positioned) and `decimal_point_index`. Here
/// the equivalent inputs are recovered as: the absolute coefficient's decimal
/// digits, and `decimal_point_index = digit_count - scale`.
fn fmt_inline_into(coefficient: i128, scale: i16, out: &mut String) {
    // Zero is always rendered "0" (matches the legacy single-byte-zero path and
    // the negative-zero canonicalization).
    if coefficient == 0 {
        out.push('0');
        return;
    }

    let is_negative = coefficient < 0;
    // Build the significant-digit string of |coefficient|. unsigned_abs avoids
    // the i128::MIN overflow trap.
    let mut buf = [0u8; 40];
    let mut mag = coefficient.unsigned_abs();
    let mut idx = buf.len();
    while mag > 0 {
        idx -= 1;
        buf[idx] = b'0' + (mag % 10) as u8;
        mag /= 10;
    }
    let digits = &buf[idx..];
    let digit_count = digits.len() as i32;
    let decimal_point_index = digit_count - i32::from(scale);

    if is_negative {
        out.push('-');
    }

    if decimal_point_index <= 0 {
        // "0." + (-decimal_point_index) zeros + all digits.
        out.push_str("0.");
        for _ in decimal_point_index..0 {
            out.push('0');
        }
        for &d in digits {
            out.push(d as char);
        }
        return;
    }

    // decimal_point_index > 0: emit digits, inserting '.' at the point, and pad
    // trailing zeros when the point is past the last digit.
    for (i, &d) in digits.iter().enumerate() {
        if i as i32 == decimal_point_index {
            out.push('.');
        }
        out.push(d as char);
    }
    if decimal_point_index > digit_count {
        for _ in digit_count..decimal_point_index {
            out.push('0');
        }
    }
}

/// Parse already-canonical Oracle `NUMBER` text into `(coefficient, scale)`,
/// returning `None` if it does not fit `i128`/`i16` (then the caller keeps the
/// text). The input is the decoder's canonical form: an optional `-`, digits,
/// an optional single `.`, no exponent (except the `-1e126` sentinel, which has
/// an `e` and is therefore rejected here -> text fallback).
fn parse_canonical_inline(text: &str) -> Option<(i128, i16)> {
    let (is_negative, rest) = match text.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, text),
    };
    if rest.is_empty() {
        return None;
    }
    let (int_part, frac_part) = match rest.split_once('.') {
        Some((i, f)) => (i, f),
        None => (rest, ""),
    };
    // Canonical text never contains an exponent or any non-digit beyond one '.'.
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let mut acc: i128 = 0;
    for b in int_part.bytes().chain(frac_part.bytes()) {
        acc = acc.checked_mul(10)?.checked_add(i128::from(b - b'0'))?;
    }
    let coefficient = if is_negative { acc.checked_neg()? } else { acc };
    let scale = i16::try_from(frac_part.len()).ok()?;
    Some((coefficient, scale))
}

impl std::fmt::Display for OracleNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::new();
        self.fmt_into(&mut s);
        f.write_str(&s)
    }
}

/// Map a [`ProtocolError`] kind into the decode error used for malformed wire.
#[allow(dead_code)]
fn _decode_err() -> ProtocolError {
    ProtocolError::TtcDecode("invalid NUMBER")
}
