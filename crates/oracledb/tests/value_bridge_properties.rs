//! Property tests for the `FromSql` / `ToSql` value bridge
//! (`crates/oracledb/src/sql_convert.rs`).
//!
//! Method (skill: testing-metamorphic / testing-fuzzing): one property per
//! `FromSql`/`ToSql` pair. Each is either a BRIDGE ROUND-TRIP or a one-sided
//! invariant, never a tautology. The "bridge round trip" models what actually
//! happens end-to-end: a Rust value `x` is turned into a [`BindValue`] by
//! [`ToSql`], the server echoes it back as the [`QueryValue`] of the natural
//! column type, and [`FromSql`] reconstructs `x'`. We assert the *verified*
//! relation between `x` and `x'` — exact equality where the path is lossless,
//! and the documented lossy relation where it is not.
//!
//! The echo step (`BindValue -> QueryValue`) is implemented by hand here,
//! type-by-type, to match what Oracle round-trips: a `NUMBER` bind comes back as
//! `QueryValue::Number` carrying the same canonical text
//! (`number_from_text`); a `BINARY_DOUBLE`/`BINARY_FLOAT` bind comes back as
//! `QueryValue::BinaryDouble(text)`; `Text`/`Raw`/`Boolean`/`Vector` echo to
//! their `QueryValue` twins; a `Timestamp`/`DateTime` bind echoes to
//! `QueryValue::DateTime`. The asymmetries this surfaces are real and encoded as
//! the correct relation rather than papered over:
//!
//! - `f64`/`f32` bind as `BINARY_DOUBLE`/`BINARY_FLOAT` (an IEEE float), echoed
//!   as text and reparsed. Rust's `{}` float formatter is round-trip-exact for
//!   finite values, so the relation is *bit-exact for finite* inputs; NaN/±Inf
//!   have no canonical Oracle text and are excluded.
//! - `i64`/`i128`/`Decimal` bind as `NUMBER` text, which is lossless, so the
//!   round-trip is exact within the type's own range.
//! - `NaiveDateTime` carries nanoseconds; Oracle `TIMESTAMP` resolves to the
//!   nanosecond, so the round-trip is exact for non-leap times.
//! - `serde_json::Value` binds as its `to_string()` text and is reparsed, so the
//!   round-trip is exact at the `Value` level (JSON's own canonicalization).
//! - VECTOR float32 elements round-trip bit-exact.

use oracledb::{FromSql, ToSql};
use oracledb_protocol::thin::{BindValue, QueryValue};
use proptest::prelude::*;

/// Per-property case budget. Matches the protocol crate's codec_properties.rs
/// convention of an explicit, generous budget on a cheap pure-CPU property.
const CASES: u32 = 1_024;

fn config() -> ProptestConfig {
    ProptestConfig {
        cases: CASES,
        ..ProptestConfig::default()
    }
}

// ---------------------------------------------------------------------------
// Echo helpers: map a `BindValue` to the `QueryValue` the server would return
// for that bind's natural column type. These encode the WIRE shape, not a
// re-statement of the conversion under test — they are deliberately the inverse
// of the column's storage, so a `ToSql` bug and a `FromSql` bug cannot cancel.
// ---------------------------------------------------------------------------

/// A `NUMBER` bind (`BindValue::Number(text)`) is stored losslessly and echoed
/// as `QueryValue::Number` carrying the same canonical decimal text.
fn echo_number(bind: &BindValue) -> QueryValue {
    let BindValue::Number(text) = bind else {
        panic!("expected BindValue::Number, got {bind:?}");
    };
    QueryValue::number_from_text(text, !text.contains('.'))
}

/// A `BINARY_DOUBLE` / `BINARY_FLOAT` bind is an IEEE float; the server echoes
/// it as text on the `QueryValue::BinaryDouble(text)` path. We format with
/// Rust's round-trip-exact `{}` so a finite value reparses bit-for-bit (this is
/// the *documented* float asymmetry: NUMBER is lossless text, but the binary
/// float column carries the IEEE value, surfaced to the driver as text).
fn echo_binary_float(bind: &BindValue) -> QueryValue {
    let v = match bind {
        BindValue::BinaryDouble(v) | BindValue::BinaryFloat(v) => *v,
        other => panic!("expected BindValue::Binary{{Double,Float}}, got {other:?}"),
    };
    QueryValue::BinaryDouble(v.to_string())
}

// ---------------------------------------------------------------------------
// Core integer scalars: NUMBER is lossless text -> exact bridge round-trip.
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(config())]

    /// BRIDGE ROUND-TRIP i64: `i64 -ToSql-> NUMBER text -echo-> NUMBER
    /// -FromSql-> i64` is exact over the whole i64 range. NUMBER carries the
    /// integer's decimal text with no rounding (sql_convert.rs i64 ToSql/FromSql).
    #[test]
    fn i64_bridge_round_trip_exact(x in any::<i64>()) {
        let bind = x.to_sql();
        prop_assert_eq!(&bind, &BindValue::Number(x.to_string()));
        let echoed = echo_number(&bind);
        prop_assert_eq!(i64::from_sql(&echoed).expect("NUMBER -> i64"), x);
    }

    /// BRIDGE ROUND-TRIP i32: same lossless NUMBER path, narrowed to i32. The
    /// i32 `FromSql` goes through i64 then `try_from` (sql_convert.rs:244).
    #[test]
    fn i32_bridge_round_trip_exact(x in any::<i32>()) {
        let echoed = echo_number(&x.to_sql());
        prop_assert_eq!(i32::from_sql(&echoed).expect("NUMBER -> i32"), x);
    }

    /// BRIDGE ROUND-TRIP u32: u32 fits i64 exactly, so the NUMBER path is exact
    /// across the full u32 range (sql_convert.rs:254).
    #[test]
    fn u32_bridge_round_trip_exact(x in any::<u32>()) {
        let echoed = echo_number(&x.to_sql());
        prop_assert_eq!(u32::from_sql(&echoed).expect("NUMBER -> u32"), x);
    }

    /// FromSql i128 from NUMBER is exact across the full i128 range — this is the
    /// "i128 vs NUMBER range" asymmetry: i128 has no `ToSql`, but every i128
    /// value's canonical text decodes back exactly (OracleNumber inline holds an
    /// i128 coefficient; sql_convert.rs:227). We also assert the i64-overflow
    /// boundary errors (typed `OutOfRange`, never a panic).
    #[test]
    fn i128_from_number_text_exact(x in any::<i128>()) {
        let echoed = QueryValue::number_from_text(&x.to_string(), true);
        prop_assert_eq!(i128::from_sql(&echoed).expect("NUMBER -> i128"), x);
        // i64 narrowing: exact iff in range, else a typed OutOfRange.
        match i64::from_sql(&echoed) {
            Ok(v) => prop_assert_eq!(i128::from(v), x),
            Err(e) => {
                prop_assert!(i64::try_from(x).is_err());
                let is_out_of_range = matches!(e, oracledb::ConversionError::OutOfRange { .. });
                prop_assert!(is_out_of_range, "expected OutOfRange, got {:?}", e);
            }
        }
    }

    /// FromSql i32/u32 narrowing boundary: a NUMBER outside the target range is a
    /// typed `OutOfRange`, and one inside reconstructs exactly. Drawn from a
    /// window that straddles both bounds so both arms are hit.
    #[test]
    fn i32_u32_narrowing_boundary(x in -10_000_000_000i64..=10_000_000_000) {
        let echoed = QueryValue::number_from_text(&x.to_string(), true);
        match i32::from_sql(&echoed) {
            Ok(v) => prop_assert_eq!(i64::from(v), x),
            Err(e) => {
                prop_assert!(i32::try_from(x).is_err());
                let is_out_of_range = matches!(e, oracledb::ConversionError::OutOfRange { .. });
                prop_assert!(is_out_of_range, "expected OutOfRange, got {:?}", e);
            }
        }
        match u32::from_sql(&echoed) {
            Ok(v) => prop_assert_eq!(i64::from(v), x),
            Err(e) => {
                prop_assert!(u32::try_from(x).is_err());
                let is_out_of_range = matches!(e, oracledb::ConversionError::OutOfRange { .. });
                prop_assert!(is_out_of_range, "expected OutOfRange, got {:?}", e);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Float scalars: binary-float column carries the IEEE value as text. The
// documented asymmetry is "finite values round-trip bit-exact; NaN/Inf are not
// representable as Oracle NUMBER/binary-float text" — so we constrain to finite.
// ---------------------------------------------------------------------------

fn finite_f64() -> impl Strategy<Value = f64> {
    prop::num::f64::NORMAL
        | prop::num::f64::SUBNORMAL
        | prop::num::f64::ZERO
        | prop::num::f64::NEGATIVE
        | prop::num::f64::POSITIVE
}

fn finite_f32() -> impl Strategy<Value = f32> {
    prop::num::f32::NORMAL
        | prop::num::f32::SUBNORMAL
        | prop::num::f32::ZERO
        | prop::num::f32::NEGATIVE
        | prop::num::f32::POSITIVE
}

proptest! {
    #![proptest_config(config())]

    /// BRIDGE ROUND-TRIP f64 (finite): `f64 -ToSql-> BINARY_DOUBLE -echo as
    /// text-> FromSql-> f64` is BIT-EXACT for finite values. `ToSql` for f64 is
    /// `BindValue::BinaryDouble(self)` (sql_convert.rs:971); the binary-float
    /// column carries the IEEE value, surfaced as text that Rust's round-trip
    /// formatter reparses exactly. (Signed zero is preserved: -0.0 formats to
    /// "-0" and reparses to -0.0.)
    #[test]
    fn f64_bridge_round_trip_bit_exact(x in finite_f64()) {
        let bind = x.to_sql();
        prop_assert_eq!(&bind, &BindValue::BinaryDouble(x));
        let echoed = echo_binary_float(&bind);
        let back = f64::from_sql(&echoed).expect("BINARY_DOUBLE text -> f64");
        prop_assert_eq!(back.to_bits(), x.to_bits(), "f64 {} did not round-trip bit-exact", x);
    }

    /// BRIDGE ROUND-TRIP f32 (finite): `f32 -ToSql-> BINARY_FLOAT(f64::from(f32))
    /// -echo as text-> FromSql-> f32`. `f64::from(f32)` widens exactly and the
    /// f32 `FromSql` narrows back with `as f32` (sql_convert.rs:288), so a finite
    /// f32 returns bit-exact. This pins the f32 widening/narrowing asymmetry: the
    /// VALUE survives because every f32 is exactly representable in f64.
    #[test]
    fn f32_bridge_round_trip_bit_exact(x in finite_f32()) {
        let bind = x.to_sql();
        prop_assert_eq!(&bind, &BindValue::BinaryFloat(f64::from(x)));
        let echoed = echo_binary_float(&bind);
        let back = f32::from_sql(&echoed).expect("BINARY_FLOAT text -> f32");
        prop_assert_eq!(back.to_bits(), x.to_bits(), "f32 {} did not round-trip bit-exact", x);
    }
}

// ---------------------------------------------------------------------------
// bool, String/str, Vec<u8>/[u8]: 1:1 variant maps -> exact round-trip.
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(config())]

    /// BRIDGE ROUND-TRIP bool: `bool -ToSql-> Boolean -echo-> Boolean -FromSql->
    /// bool`, exact. Also pins the NUMBER(1) flag representation the bool
    /// `FromSql` accepts (0/1) and rejects everything else (sql_convert.rs:294).
    #[test]
    fn bool_bridge_round_trip_and_number_flag(b in any::<bool>(), n in any::<i64>()) {
        let echoed = QueryValue::Boolean(b);
        prop_assert_eq!(bool::from_sql(&echoed).expect("BOOLEAN -> bool"), b);
        prop_assert_eq!(b.to_sql(), BindValue::Boolean(b));

        // NUMBER(1) flag: exactly 0 -> false, 1 -> true; anything else OutOfRange.
        let num = QueryValue::number_from_text(&n.to_string(), true);
        match bool::from_sql(&num) {
            Ok(v) => prop_assert!((n == 0 && !v) || (n == 1 && v)),
            Err(e) => {
                prop_assert!(n != 0 && n != 1);
                let is_out_of_range = matches!(e, oracledb::ConversionError::OutOfRange { .. });
                prop_assert!(is_out_of_range, "expected OutOfRange, got {:?}", e);
            }
        }
    }

    /// BRIDGE ROUND-TRIP String/str: `&str -ToSql-> Text -echo-> Text -FromSql->
    /// String` returns the original bytes for any UTF-8 string. Both `str` and
    /// `String` `ToSql` produce the identical `Text` bind (sql_convert.rs:989/995).
    #[test]
    fn string_bridge_round_trip_exact(s in ".{0,256}") {
        let from_str = s.as_str().to_sql();
        let from_string = s.clone().to_sql();
        prop_assert_eq!(&from_str, &from_string);
        prop_assert_eq!(&from_str, &BindValue::Text(s.clone()));
        let echoed = QueryValue::Text(s.clone());
        prop_assert_eq!(String::from_sql(&echoed).expect("Text -> String"), s);
    }

    /// BRIDGE ROUND-TRIP Vec<u8>/[u8]: `&[u8] -ToSql-> Raw -echo-> Raw -FromSql->
    /// Vec<u8>` is byte-exact for any RAW payload. Slice and Vec `ToSql` produce
    /// the same `Raw` bind (sql_convert.rs:1001/1007).
    #[test]
    fn bytes_bridge_round_trip_exact(bytes in prop::collection::vec(any::<u8>(), 0..=256)) {
        let from_slice = bytes.as_slice().to_sql();
        let from_vec = bytes.clone().to_sql();
        prop_assert_eq!(&from_slice, &from_vec);
        prop_assert_eq!(&from_slice, &BindValue::Raw(bytes.clone()));
        let echoed = QueryValue::Raw(bytes.clone());
        prop_assert_eq!(Vec::<u8>::from_sql(&echoed).expect("Raw -> Vec<u8>"), bytes);
    }

    /// Option<T> NULL semantics: `None -ToSql-> BindValue::Null`, and a `Some(x)`
    /// binds as `x` does (sql_convert.rs:1019). On the read side `Option<T>` is
    /// total over any value `T` accepts (it maps to `Some`); the NULL->None path
    /// is exercised by the FromRow tests in-crate.
    #[test]
    fn option_bind_null_and_some(x in any::<i64>()) {
        let none: Option<i64> = None;
        prop_assert_eq!(none.to_sql(), BindValue::Null);
        prop_assert_eq!(Some(x).to_sql(), x.to_sql());
        let echoed = QueryValue::number_from_text(&x.to_string(), true);
        prop_assert_eq!(Option::<i64>::from_sql(&echoed).expect("NUMBER -> Option<i64>"), Some(x));
    }
}

// ---------------------------------------------------------------------------
// chrono (feature-gated): TIMESTAMP carries nanoseconds; DATE is y/m/d only.
// ---------------------------------------------------------------------------

#[cfg(feature = "chrono")]
mod chrono_props {
    use super::{config, BindValue, FromSql, QueryValue, ToSql};
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use proptest::prelude::*;

    /// A `NaiveDateTime` whose time is NOT in a leap second. chrono encodes a
    /// leap second as `second == 59` plus `nanosecond >= 1_000_000_000`; the
    /// driver's `ToSql` emits the raw `nanosecond()` and `FromSql` feeds it to
    /// `NaiveTime::from_hms_nano_opt`, which only accepts a leap nanosecond when
    /// `second == 59`. The echo path drops the second/nanosecond split, so we
    /// exclude leaps (a documented, narrow non-round-tripping corner, not a bug
    /// in the scalar path). The date range stays well inside chrono's bounds.
    fn non_leap_datetime() -> impl Strategy<Value = NaiveDateTime> {
        (
            -262_000i32..=262_000, // days from the common epoch, comfortably in range
            0u32..86_400,          // seconds-of-day
            0u32..1_000_000_000,   // sub-second nanos, strictly below a leap
        )
            .prop_map(|(day_off, secs, nanos)| {
                let date = NaiveDate::from_ymd_opt(2000, 1, 1)
                    .expect("2000-01-01 is valid")
                    .checked_add_signed(chrono::Duration::days(i64::from(day_off)))
                    .expect("day offset stays in range");
                let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
                    .expect("secs<86400, nanos<1e9 is a valid non-leap time");
                NaiveDateTime::new(date, time)
            })
    }

    proptest! {
        #![proptest_config(config())]

        /// BRIDGE ROUND-TRIP NaiveDateTime (non-leap): `dt -ToSql-> Timestamp
        /// -echo-> DateTime -FromSql-> dt` is EXACT to the nanosecond. Oracle
        /// TIMESTAMP resolves to the nanosecond and the bind carries every
        /// component (sql_convert.rs:1034). This encodes the "sub-second
        /// precision" asymmetry as *no loss* for the non-leap domain.
        #[test]
        fn datetime_bridge_round_trip_to_nanosecond(dt in non_leap_datetime()) {
            let bind = dt.to_sql();
            let BindValue::Timestamp {
                ora_type_num, year, month, day, hour, minute, second, nanosecond,
            } = bind else {
                panic!("expected Timestamp bind, got {bind:?}");
            };
            prop_assert_eq!(ora_type_num, 180); // DB_TYPE_TIMESTAMP
            let echoed = QueryValue::DateTime {
                year, month, day, hour, minute, second, nanosecond,
            };
            prop_assert_eq!(
                NaiveDateTime::from_sql(&echoed).expect("DateTime -> NaiveDateTime"),
                dt
            );
        }

        /// BRIDGE ROUND-TRIP NaiveDate: `date -ToSql-> DateTime(00:00:00) -echo->
        /// DateTime -FromSql-> date` is exact. The date `ToSql` zeroes the time
        /// fields (sql_convert.rs:1050) and the date `FromSql` reads only y/m/d
        /// (sql_convert.rs:398), so the day survives regardless of any time part.
        #[test]
        fn date_bridge_round_trip_exact(day_off in -262_000i32..=262_000) {
            let date = NaiveDate::from_ymd_opt(2000, 1, 1)
                .expect("2000-01-01 is valid")
                .checked_add_signed(chrono::Duration::days(i64::from(day_off)))
                .expect("day offset stays in range");
            let bind = date.to_sql();
            let BindValue::DateTime { year, month, day, hour, minute, second } = bind else {
                panic!("expected DateTime bind, got {bind:?}");
            };
            prop_assert_eq!((hour, minute, second), (0, 0, 0), "date bind must zero the time");
            let echoed = QueryValue::DateTime {
                year, month, day, hour, minute, second, nanosecond: 0,
            };
            prop_assert_eq!(NaiveDate::from_sql(&echoed).expect("DateTime -> NaiveDate"), date);
        }
    }
}

// ---------------------------------------------------------------------------
// uuid (feature-gated): RAW(16) carrier, plus the canonical-text FromSql path.
// ---------------------------------------------------------------------------

#[cfg(feature = "uuid")]
mod uuid_props {
    use super::{config, BindValue, FromSql, QueryValue, ToSql};
    use proptest::prelude::*;
    use uuid::Uuid;

    proptest! {
        #![proptest_config(config())]

        /// BRIDGE ROUND-TRIP Uuid via RAW(16): `uuid -ToSql-> Raw(16) -echo->
        /// Raw -FromSql-> uuid` is byte-exact for any 128-bit value
        /// (sql_convert.rs:1070 / :425). The bytes are the canonical big-endian
        /// layout, so no endianness surprise.
        #[test]
        fn uuid_bridge_round_trip_raw(bytes in any::<[u8; 16]>()) {
            let id = Uuid::from_bytes(bytes);
            let bind = id.to_sql();
            prop_assert_eq!(&bind, &BindValue::Raw(id.as_bytes().to_vec()));
            let echoed = QueryValue::Raw(id.as_bytes().to_vec());
            prop_assert_eq!(Uuid::from_sql(&echoed).expect("RAW(16) -> Uuid"), id);
        }

        /// FromSql Uuid from canonical TEXT: a UUID stored as a hyphenated string
        /// column parses back to the same value (sql_convert.rs:439). This is the
        /// second, text-shaped read path the RAW round-trip does not cover.
        #[test]
        fn uuid_from_canonical_text(bytes in any::<[u8; 16]>()) {
            let id = Uuid::from_bytes(bytes);
            let echoed = QueryValue::Text(id.to_string());
            prop_assert_eq!(Uuid::from_sql(&echoed).expect("UUID text -> Uuid"), id);
        }
    }
}

// ---------------------------------------------------------------------------
// serde_json (feature-gated): Value binds as text and is reparsed.
// ---------------------------------------------------------------------------

#[cfg(feature = "serde_json")]
mod serde_json_props {
    use super::{config, BindValue, FromSql, QueryValue, ToSql};
    use proptest::prelude::*;
    use serde_json::{json, Map, Value};

    /// A finite-number-only JSON tree. JSON has no NaN/Inf, and `serde_json`
    /// rejects non-finite floats at serialization time, so we keep numbers to
    /// i64 and finite f64 that survive `to_string()`/`from_str()` exactly.
    fn json_leaf() -> impl Strategy<Value = Value> {
        prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(|n| json!(n)),
            ".{0,32}".prop_map(Value::String),
        ]
    }

    fn json_tree() -> impl Strategy<Value = Value> {
        json_leaf().prop_recursive(4, 32, 6, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..=6).prop_map(Value::Array),
                prop::collection::vec(("[a-z]{1,8}", inner), 0..=6).prop_map(|pairs| {
                    // Object semantics keep the last write per key; reparsing
                    // would too, so de-dup keys to make the round-trip an
                    // identity rather than asserting on dup-key collapse.
                    let mut map = Map::new();
                    for (k, v) in pairs {
                        map.insert(k, v);
                    }
                    Value::Object(map)
                }),
            ]
        })
    }

    proptest! {
        #![proptest_config(config())]

        /// BRIDGE ROUND-TRIP serde_json::Value: `value -ToSql-> Text(to_string)
        /// -echo-> Text -FromSql-> value` reconstructs the identical `Value`.
        /// `ToSql` serializes to JSON text (sql_convert.rs:1083) and the text
        /// `FromSql` path parses it back (sql_convert.rs:541). This encodes the
        /// "JSON canonicalization" asymmetry as an identity at the `Value` level:
        /// the textual form may differ, but `Value == Value` holds.
        #[test]
        fn json_value_bridge_round_trip(value in json_tree()) {
            let bind = value.to_sql();
            let BindValue::Text(text) = &bind else {
                panic!("expected Text bind, got {bind:?}");
            };
            prop_assert_eq!(text, &value.to_string());
            let echoed = QueryValue::Text(text.clone());
            prop_assert_eq!(Value::from_sql(&echoed).expect("JSON text -> Value"), value);
        }
    }
}

// ---------------------------------------------------------------------------
// rust_decimal (feature-gated): NUMBER text is lossless for Decimal's domain.
// ---------------------------------------------------------------------------

#[cfg(feature = "rust_decimal")]
mod rust_decimal_props {
    use super::{config, BindValue, FromSql, QueryValue, ToSql};
    use proptest::prelude::*;
    use rust_decimal::Decimal;

    /// An arbitrary `Decimal` built from a full 96-bit mantissa and a legal scale
    /// (0..=28). This spans the whole representable domain — exactly the values
    /// `ToSql`/`FromSql` claim to carry losslessly through Oracle NUMBER.
    fn any_decimal() -> impl Strategy<Value = Decimal> {
        (any::<i64>(), any::<u32>(), 0u32..=28).prop_map(|(hi, lo, scale)| {
            // Pack a wide-ish mantissa from two halves, then place the point.
            let mantissa = (i128::from(hi) << 32) | i128::from(lo);
            // Decimal::from_i128_with_scale panics if the mantissa exceeds 96
            // bits; clamp into range by masking to 96 bits and re-signing.
            let sign = if mantissa < 0 { -1i128 } else { 1i128 };
            let mag = (mantissa.unsigned_abs() & ((1u128 << 96) - 1)) as i128;
            Decimal::from_i128_with_scale(sign * mag, scale)
        })
    }

    proptest! {
        #![proptest_config(config())]

        /// BRIDGE ROUND-TRIP Decimal: `dec -ToSql-> NUMBER text -echo-> NUMBER
        /// -FromSql-> dec` is EXACT across Decimal's full domain. The canonical
        /// decimal text carries every digit with no float rounding
        /// (sql_convert.rs:1098 / :564). This is the lossless counterpart to the
        /// f64 path: where f64 binds an IEEE float, Decimal binds exact text.
        #[test]
        fn decimal_bridge_round_trip_exact(dec in any_decimal()) {
            let bind = dec.to_sql();
            prop_assert_eq!(&bind, &BindValue::Number(dec.to_string()));
            let echoed = QueryValue::number_from_text(&dec.to_string(), !dec.to_string().contains('.'));
            let back = Decimal::from_sql(&echoed).expect("NUMBER -> Decimal");
            // Compare numerically (normalize): "1.50" and "1.5" are equal values
            // even if scale-normalized differently by the carrier.
            prop_assert_eq!(back.normalize(), dec.normalize(),
                "Decimal {} did not round-trip (got {})", dec, back);
        }
    }
}

// ---------------------------------------------------------------------------
// VECTOR element vectors. Vec<f32>/[f32] are ToSql+FromSql (full bridge);
// Vec<f64> is FromSql-only, so its float64 read path is tested directly along
// with the documented cross-format coercions.
// ---------------------------------------------------------------------------

mod vector_props {
    use super::{config, BindValue, FromSql, QueryValue, ToSql};
    use oracledb_protocol::vector::{Vector, VectorValues};
    use proptest::prelude::*;

    fn finite_f32_vec() -> impl Strategy<Value = Vec<f32>> {
        let elem = prop::num::f32::NORMAL
            | prop::num::f32::SUBNORMAL
            | prop::num::f32::ZERO
            | prop::num::f32::NEGATIVE
            | prop::num::f32::POSITIVE;
        prop::collection::vec(elem, 0..=64)
    }

    proptest! {
        #![proptest_config(config())]

        /// BRIDGE ROUND-TRIP Vec<f32>/[f32]: `Vec<f32> -ToSql-> Vector(Dense
        /// Float32) -echo-> Vector -FromSql-> Vec<f32>` is BIT-EXACT. VECTOR
        /// float32 elements are carried verbatim (sql_convert.rs:1108/:604); slice
        /// and Vec `ToSql` produce the same dense-float32 bind.
        #[test]
        fn vec_f32_bridge_round_trip_bit_exact(values in finite_f32_vec()) {
            let from_vec = values.clone().to_sql();
            let from_slice = values.as_slice().to_sql();
            prop_assert_eq!(&from_vec, &from_slice);
            let BindValue::Vector(Vector::Dense(VectorValues::Float32(carried))) = &from_vec else {
                panic!("expected dense float32 vector bind, got {from_vec:?}");
            };
            prop_assert!(
                carried.iter().zip(&values).all(|(a, b)| a.to_bits() == b.to_bits()),
                "ToSql changed the float32 bits"
            );
            let echoed = QueryValue::Vector(Box::new(Vector::Dense(VectorValues::Float32(
                carried.clone(),
            ))));
            let back = Vec::<f32>::from_sql(&echoed).expect("VECTOR -> Vec<f32>");
            prop_assert!(
                back.iter().zip(&values).all(|(a, b)| a.to_bits() == b.to_bits())
                    && back.len() == values.len(),
                "Vec<f32> vector did not round-trip bit-exact"
            );
        }

        /// FromSql Vec<f64> from a dense FLOAT64 VECTOR is bit-exact (Vec<f64> has
        /// no `ToSql`, so this is its read-side invariant; sql_convert.rs:613).
        #[test]
        fn vec_f64_from_float64_vector_bit_exact(
            values in prop::collection::vec(
                prop::num::f64::NORMAL | prop::num::f64::ZERO | prop::num::f64::NEGATIVE,
                0..=64,
            ),
        ) {
            let echoed = QueryValue::Vector(Box::new(Vector::Dense(VectorValues::Float64(
                values.clone(),
            ))));
            let back = Vec::<f64>::from_sql(&echoed).expect("VECTOR -> Vec<f64>");
            prop_assert!(
                back.iter().zip(&values).all(|(a, b)| a.to_bits() == b.to_bits())
                    && back.len() == values.len(),
                "Vec<f64> vector did not round-trip bit-exact"
            );
        }

        /// METAMORPHIC cross-format coercion: an INT8 VECTOR reads identically as
        /// Vec<f32> and Vec<f64> (each element widened from the same i8). This
        /// pins the documented "VECTOR element types" asymmetry — the element
        /// FORMAT can differ from the requested Rust element type, and the driver
        /// widens losslessly (sql_convert.rs:636/648).
        #[test]
        fn int8_vector_widens_to_f32_and_f64(values in prop::collection::vec(any::<i8>(), 0..=64)) {
            let make = || QueryValue::Vector(Box::new(Vector::Dense(VectorValues::Int8(
                values.clone(),
            ))));
            let as_f32 = Vec::<f32>::from_sql(&make()).expect("INT8 VECTOR -> Vec<f32>");
            let as_f64 = Vec::<f64>::from_sql(&make()).expect("INT8 VECTOR -> Vec<f64>");
            for (i, &v) in values.iter().enumerate() {
                // Compare on bits: each i8 widens to an exactly-representable float.
                prop_assert_eq!(as_f32[i].to_bits(), f32::from(v).to_bits());
                prop_assert_eq!(as_f64[i].to_bits(), f64::from(v).to_bits());
            }
        }
    }
}
