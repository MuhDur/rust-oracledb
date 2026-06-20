//! Typed conversions between Rust values and Oracle wire values.
//!
//! This module is the ergonomic read/write surface that makes the driver feel
//! native to Rust:
//!
//! - [`FromSql`] converts a fetched [`QueryValue`] into a concrete Rust type
//!   (`row.get::<i64>(0, 1)?`), so callers stop matching the full value enum
//!   and unwrapping `Option`s by hand.
//! - [`ToSql`] converts a Rust value into a [`BindValue`] for a placeholder
//!   bind, and the [`params!`](crate::params) macro / tuple impls let
//!   `(40, "alice")` and `params!{ ":id" => 40 }` flow straight into the
//!   execute helpers.
//!
//! Core scalar conversions (`i64`/`i32`/`u32`/`f64`/`f32`/`bool`/`String`/
//! `Vec<u8>`) are always available. Conversions for `chrono`, `uuid`,
//! `serde_json`, `rust_decimal`, and vector element vectors (`Vec<f32>` /
//! `Vec<f64>`) sit behind the matching crate feature so the default build pulls
//! in no extra dependencies.
//!
//! # Lossless NUMBER
//!
//! Oracle NUMBER is carried losslessly as an inline `{ i128 coefficient, i16
//! scale }` form (see [`oracledb_protocol::thin::OracleNumber`]); its canonical
//! decimal text is synthesized on demand by a single shared formatter. The
//! `rust_decimal::Decimal` conversion builds *directly* from the inline
//! coefficient/scale when it fits, so a value round-trips *exactly* to the full
//! precision `rust_decimal::Decimal` can represent (~28-29 significant digits) —
//! no float rounding anywhere on the path. `i64`/`i128` reconstruct directly
//! from the coefficient with no string parse. python-oracledb hands you a lossy
//! `float` (~15-17 digits) unless you opt into `decimal.Decimal` per column. For
//! values exceeding `Decimal`'s range, read the canonical text with
//! [`QueryValue::as_number_text`](oracledb_protocol::thin::QueryValue::as_number_text)
//! and bind it back as `BindValue::Number`, which carries Oracle's full digits.

use std::borrow::Cow;

use oracledb_protocol::thin::{BindValue, QueryResult, QueryValue};

/// Why a typed [`FromSql`] conversion could not be performed.
///
/// Surfaced as [`crate::Error::Conversion`]. The variants distinguish the three
/// failure shapes a caller actually wants to branch on: the cell was SQL
/// `NULL`, the Oracle type does not map to the requested Rust type, or the
/// value was the right type but out of the Rust type's range / unparseable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConversionError {
    /// The cell was SQL `NULL` but the requested type is not nullable. Convert
    /// into `Option<T>` to accept nulls.
    UnexpectedNull,
    /// The Oracle value's variant has no conversion to the requested Rust type
    /// (e.g. asking for `i64` from a `RAW` column).
    TypeMismatch {
        /// The Rust type that was requested.
        expected: &'static str,
        /// A short description of the Oracle value that was present.
        found: &'static str,
    },
    /// The value was of a convertible variant but did not fit the target type
    /// (out of range, non-integral where an integer was asked, bad UTF-8, an
    /// unparseable NUMBER, a vector of the wrong element format, ...).
    OutOfRange {
        /// The Rust type that was requested.
        expected: &'static str,
        /// What went wrong.
        detail: String,
    },
}

impl std::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConversionError::UnexpectedNull => {
                write!(f, "value is SQL NULL but the target type is not Option<_>")
            }
            ConversionError::TypeMismatch { expected, found } => {
                write!(f, "cannot convert Oracle {found} into {expected}")
            }
            ConversionError::OutOfRange { expected, detail } => {
                write!(f, "value does not fit {expected}: {detail}")
            }
        }
    }
}

impl std::error::Error for ConversionError {}

impl From<ConversionError> for crate::Error {
    fn from(err: ConversionError) -> Self {
        crate::Error::Conversion(err)
    }
}

/// A short, stable description of a [`QueryValue`]'s Oracle type, for error
/// messages.
fn value_kind(value: &QueryValue) -> &'static str {
    match value {
        QueryValue::Text(_) => "VARCHAR2/CHAR text",
        QueryValue::TextRaw { .. } => "undecodable character data",
        QueryValue::Raw(_) => "RAW",
        QueryValue::Rowid(_) => "ROWID",
        QueryValue::BinaryDouble(_) => "BINARY_DOUBLE/BINARY_FLOAT",
        QueryValue::IntervalDS { .. } => "INTERVAL DAY TO SECOND",
        QueryValue::IntervalYM { .. } => "INTERVAL YEAR TO MONTH",
        QueryValue::Number(_) => "NUMBER",
        QueryValue::Boolean(_) => "BOOLEAN",
        QueryValue::Cursor(_) => "REF CURSOR",
        QueryValue::DateTime { .. } => "DATE/TIMESTAMP",
        QueryValue::Object(_) => "object/ADT",
        QueryValue::Lob(_) => "LOB locator",
        QueryValue::Vector(_) => "VECTOR",
        QueryValue::Json(_) => "JSON",
        QueryValue::Array(_) => "collection",
    }
}

fn mismatch<T>(expected: &'static str, value: &QueryValue) -> Result<T, ConversionError> {
    Err(ConversionError::TypeMismatch {
        expected,
        found: value_kind(value),
    })
}

/// Convert a fetched Oracle [`QueryValue`] into a concrete Rust type.
///
/// Implemented for the core scalars unconditionally and for `chrono`, `uuid`,
/// `serde_json`, `rust_decimal`, and vector element vectors behind their crate
/// features. Use it through [`QueryResultExt::get`] /
/// [`QueryResultExt::get_by_name`] rather than calling [`FromSql::from_sql`]
/// directly in most code.
///
/// `Option<T>` is implemented for every `T: FromSql`, so a nullable column maps
/// to `Option<T>` and a `NULL` cell yields `None`.
pub trait FromSql: Sized {
    /// Convert `value` into `Self`, or fail with a [`ConversionError`].
    fn from_sql(value: &QueryValue) -> Result<Self, ConversionError>;
}

// ---------------------------------------------------------------------------
// Core scalar conversions (always available)
// ---------------------------------------------------------------------------

impl FromSql for i64 {
    fn from_sql(value: &QueryValue) -> Result<Self, ConversionError> {
        match value {
            // Exact: reconstruct directly from the inline coefficient/scale (no
            // string parse) when the value is an integer that fits i64.
            QueryValue::Number(num) => num.to_i64().ok_or_else(|| ConversionError::OutOfRange {
                expected: "i64",
                detail: format!(
                    "NUMBER {:?} is not an integer that fits i64",
                    num.to_canonical_string()
                ),
            }),
            QueryValue::Boolean(b) => Ok(i64::from(*b)),
            other => mismatch("i64", other),
        }
    }
}

impl FromSql for i128 {
    fn from_sql(value: &QueryValue) -> Result<Self, ConversionError> {
        match value {
            // Exact i128 reconstruct from the inline coefficient/scale.
            QueryValue::Number(num) => num.to_i128().ok_or_else(|| ConversionError::OutOfRange {
                expected: "i128",
                detail: format!(
                    "NUMBER {:?} is not an integer that fits i128",
                    num.to_canonical_string()
                ),
            }),
            QueryValue::Boolean(b) => Ok(i128::from(*b)),
            other => mismatch("i128", other),
        }
    }
}

impl FromSql for i32 {
    fn from_sql(value: &QueryValue) -> Result<Self, ConversionError> {
        let wide = i64::from_sql(value)?;
        i32::try_from(wide).map_err(|_| ConversionError::OutOfRange {
            expected: "i32",
            detail: format!("{wide} is out of range for i32"),
        })
    }
}

impl FromSql for u32 {
    fn from_sql(value: &QueryValue) -> Result<Self, ConversionError> {
        let wide = i64::from_sql(value)?;
        u32::try_from(wide).map_err(|_| ConversionError::OutOfRange {
            expected: "u32",
            detail: format!("{wide} is out of range for u32"),
        })
    }
}

impl FromSql for f64 {
    fn from_sql(value: &QueryValue) -> Result<Self, ConversionError> {
        match value {
            QueryValue::Number(num) => {
                let text = num.to_canonical_string();
                text.parse::<f64>()
                    .map_err(|_| ConversionError::OutOfRange {
                        expected: "f64",
                        detail: format!("{text:?} is not a finite f64"),
                    })
            }
            QueryValue::BinaryDouble(text) => {
                text.trim()
                    .parse::<f64>()
                    .map_err(|_| ConversionError::OutOfRange {
                        expected: "f64",
                        detail: format!("{text:?} is not a finite f64"),
                    })
            }
            other => mismatch("f64", other),
        }
    }
}

impl FromSql for f32 {
    fn from_sql(value: &QueryValue) -> Result<Self, ConversionError> {
        Ok(f64::from_sql(value)? as f32)
    }
}

impl FromSql for bool {
    fn from_sql(value: &QueryValue) -> Result<Self, ConversionError> {
        match value {
            QueryValue::Boolean(b) => Ok(*b),
            // A NUMBER(1) flag column round-trips as 0/1.
            QueryValue::Number(num) => match num.to_i64() {
                Some(0) => Ok(false),
                Some(1) => Ok(true),
                _ => Err(ConversionError::OutOfRange {
                    expected: "bool",
                    detail: format!("NUMBER {:?} is neither 0 nor 1", num.to_canonical_string()),
                }),
            },
            other => mismatch("bool", other),
        }
    }
}

impl FromSql for String {
    fn from_sql(value: &QueryValue) -> Result<Self, ConversionError> {
        match value {
            QueryValue::Text(s) => Ok(s.clone()),
            QueryValue::Rowid(s) => Ok(s.clone()),
            QueryValue::Number(num) => Ok(num.to_canonical_string()),
            QueryValue::BinaryDouble(text) => Ok(text.clone()),
            other => mismatch("String", other),
        }
    }
}

impl FromSql for Vec<u8> {
    fn from_sql(value: &QueryValue) -> Result<Self, ConversionError> {
        match value {
            QueryValue::Raw(bytes) => Ok(bytes.clone()),
            QueryValue::TextRaw { bytes, .. } => Ok(bytes.clone()),
            other => mismatch("Vec<u8>", other),
        }
    }
}

impl<T: FromSql> FromSql for Option<T> {
    fn from_sql(value: &QueryValue) -> Result<Self, ConversionError> {
        T::from_sql(value).map(Some)
    }
}

// ---------------------------------------------------------------------------
// chrono (feature-gated)
// ---------------------------------------------------------------------------

#[cfg(feature = "chrono")]
mod chrono_impls {
    use super::{mismatch, ConversionError, FromSql};
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use oracledb_protocol::thin::QueryValue;

    fn naive_from_components(
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        nanosecond: u32,
    ) -> Result<NaiveDateTime, ConversionError> {
        let date =
            NaiveDate::from_ymd_opt(year, u32::from(month), u32::from(day)).ok_or_else(|| {
                ConversionError::OutOfRange {
                    expected: "chrono::NaiveDateTime",
                    detail: format!("invalid date {year:04}-{month:02}-{day:02}"),
                }
            })?;
        let time = NaiveTime::from_hms_nano_opt(
            u32::from(hour),
            u32::from(minute),
            u32::from(second),
            nanosecond,
        )
        .ok_or_else(|| ConversionError::OutOfRange {
            expected: "chrono::NaiveDateTime",
            detail: format!("invalid time {hour:02}:{minute:02}:{second:02}.{nanosecond:09}"),
        })?;
        Ok(NaiveDateTime::new(date, time))
    }

    impl FromSql for NaiveDateTime {
        fn from_sql(value: &QueryValue) -> Result<Self, ConversionError> {
            match value {
                QueryValue::DateTime {
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    nanosecond,
                } => {
                    naive_from_components(*year, *month, *day, *hour, *minute, *second, *nanosecond)
                }
                other => mismatch("chrono::NaiveDateTime", other),
            }
        }
    }

    impl FromSql for NaiveDate {
        fn from_sql(value: &QueryValue) -> Result<Self, ConversionError> {
            match value {
                QueryValue::DateTime {
                    year, month, day, ..
                } => NaiveDate::from_ymd_opt(*year, u32::from(*month), u32::from(*day)).ok_or_else(
                    || ConversionError::OutOfRange {
                        expected: "chrono::NaiveDate",
                        detail: format!("invalid date {year:04}-{month:02}-{day:02}"),
                    },
                ),
                other => mismatch("chrono::NaiveDate", other),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// uuid (feature-gated): from RAW(16) or canonical / hyphenated text
// ---------------------------------------------------------------------------

#[cfg(feature = "uuid")]
mod uuid_impls {
    use super::{mismatch, ConversionError, FromSql};
    use oracledb_protocol::thin::QueryValue;
    use uuid::Uuid;

    impl FromSql for Uuid {
        fn from_sql(value: &QueryValue) -> Result<Self, ConversionError> {
            match value {
                QueryValue::Raw(bytes) => {
                    let array: [u8; 16] =
                        bytes
                            .as_slice()
                            .try_into()
                            .map_err(|_| ConversionError::OutOfRange {
                                expected: "uuid::Uuid",
                                detail: format!("RAW length {} is not 16 bytes", bytes.len()),
                            })?;
                    Ok(Uuid::from_bytes(array))
                }
                QueryValue::Text(text) => {
                    Uuid::parse_str(text.trim()).map_err(|err| ConversionError::OutOfRange {
                        expected: "uuid::Uuid",
                        detail: format!("text {text:?} is not a UUID: {err}"),
                    })
                }
                other => mismatch("uuid::Uuid", other),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// serde_json (feature-gated): from the eager OSON tree (near-free, lossless)
// ---------------------------------------------------------------------------

#[cfg(feature = "serde_json")]
mod serde_json_impls {
    use super::{mismatch, ConversionError, FromSql};
    use oracledb_protocol::oson::OsonValue;
    use oracledb_protocol::thin::QueryValue;
    use serde_json::{Map, Number, Value};

    /// Convert one OSON node into a `serde_json::Value`. NUMBER text is parsed
    /// into the widest JSON number that fits (i64 / u64 / f64); values outside
    /// f64 range fall back to a JSON string so nothing is silently dropped.
    fn oson_to_json(node: &OsonValue) -> Value {
        match node {
            OsonValue::Null => Value::Null,
            OsonValue::Bool(b) => Value::Bool(*b),
            OsonValue::Number(text) => number_to_json(text),
            OsonValue::BinaryFloat(v) => f64_to_json(f64::from(*v)),
            OsonValue::BinaryDouble(v) => f64_to_json(*v),
            OsonValue::String(s) => Value::String(s.clone()),
            OsonValue::Raw(bytes) => Value::String(hex_encode(bytes)),
            OsonValue::DateTime {
                year,
                month,
                day,
                hour,
                minute,
                second,
                nanosecond,
            } => Value::String(format!(
                "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{nanosecond:09}"
            )),
            OsonValue::IntervalDS {
                days,
                hours,
                minutes,
                seconds,
                fseconds,
            } => Value::String(format!(
                "P{days}DT{hours}H{minutes}M{seconds}.{fseconds:09}S"
            )),
            OsonValue::Vector(_) => Value::String("<vector>".to_string()),
            OsonValue::Array(items) => Value::Array(items.iter().map(oson_to_json).collect()),
            OsonValue::Object(entries) => {
                let mut map = Map::with_capacity(entries.len());
                for (key, val) in entries {
                    map.insert(key.clone(), oson_to_json(val));
                }
                Value::Object(map)
            }
        }
    }

    fn number_to_json(text: &str) -> Value {
        let trimmed = text.trim();
        if let Ok(i) = trimmed.parse::<i64>() {
            return Value::Number(Number::from(i));
        }
        if let Ok(u) = trimmed.parse::<u64>() {
            return Value::Number(Number::from(u));
        }
        if let Ok(f) = trimmed.parse::<f64>() {
            if let Some(n) = Number::from_f64(f) {
                return Value::Number(n);
            }
        }
        // Preserve the exact text rather than lose precision.
        Value::String(trimmed.to_string())
    }

    fn f64_to_json(v: f64) -> Value {
        Number::from_f64(v).map_or_else(|| Value::String(v.to_string()), Value::Number)
    }

    fn hex_encode(bytes: &[u8]) -> String {
        let mut out = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            out.push_str(&format!("{byte:02x}"));
        }
        out
    }

    impl FromSql for Value {
        fn from_sql(value: &QueryValue) -> Result<Self, ConversionError> {
            match value {
                QueryValue::Json(oson) => Ok(oson_to_json(oson)),
                // A JSON document stored in a VARCHAR2/CLOB column comes back as
                // text; parse it so callers get a real Value either way.
                QueryValue::Text(text) => {
                    serde_json::from_str(text).map_err(|err| ConversionError::OutOfRange {
                        expected: "serde_json::Value",
                        detail: format!("text is not valid JSON: {err}"),
                    })
                }
                other => mismatch("serde_json::Value", other),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// rust_decimal (feature-gated): LOSSLESS from the text-NUMBER carrier
// ---------------------------------------------------------------------------

#[cfg(feature = "rust_decimal")]
mod rust_decimal_impls {
    use super::{mismatch, ConversionError, FromSql};
    use oracledb_protocol::thin::QueryValue;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    impl FromSql for Decimal {
        fn from_sql(value: &QueryValue) -> Result<Self, ConversionError> {
            match value {
                QueryValue::Number(num) => {
                    // EXACT path: build directly from the inline coefficient and a
                    // non-negative scale that `rust_decimal` accepts (0..=28). This
                    // avoids a string round-trip and is lossless for the full
                    // 28-significant-digit domain `Decimal` can hold.
                    if let (Some(coefficient), Some(scale)) = (num.coefficient(), num.scale()) {
                        if (0..=28).contains(&scale) {
                            if let Ok(dec) = Decimal::try_from_i128_with_scale(
                                coefficient,
                                u32::from(scale as u16),
                            ) {
                                return Ok(dec);
                            }
                        }
                    }
                    // Fallback for negative scale / out-of-range / boxed-text: go
                    // through the canonical text (still lossless for valid values).
                    let text = num.to_canonical_string();
                    Decimal::from_str(&text).or_else(|_| {
                        Decimal::from_scientific(&text).map_err(|err| ConversionError::OutOfRange {
                            expected: "rust_decimal::Decimal",
                            detail: format!("NUMBER {text:?} does not fit Decimal: {err}"),
                        })
                    })
                }
                other => mismatch("rust_decimal::Decimal", other),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Vector element vectors (feature-gated under their own logic — no extra dep)
// ---------------------------------------------------------------------------

use oracledb_protocol::vector::{Vector, VectorValues};

impl FromSql for Vec<f32> {
    fn from_sql(value: &QueryValue) -> Result<Self, ConversionError> {
        match value {
            QueryValue::Vector(vector) => vector_to_f32(vector),
            other => mismatch("Vec<f32>", other),
        }
    }
}

impl FromSql for Vec<f64> {
    fn from_sql(value: &QueryValue) -> Result<Self, ConversionError> {
        match value {
            QueryValue::Vector(vector) => vector_to_f64(vector),
            other => mismatch("Vec<f64>", other),
        }
    }
}

fn dense_values(vector: &Vector) -> Result<&VectorValues, ConversionError> {
    match vector {
        Vector::Dense(values) => Ok(values),
        Vector::Sparse { .. } => Err(ConversionError::OutOfRange {
            expected: "Vec<element>",
            detail: "sparse VECTOR cannot be read as a dense Vec".to_string(),
        }),
    }
}

fn vector_to_f32(vector: &Vector) -> Result<Vec<f32>, ConversionError> {
    match dense_values(vector)? {
        VectorValues::Float32(v) => Ok(v.clone()),
        VectorValues::Float64(v) => Ok(v.iter().map(|x| *x as f32).collect()),
        VectorValues::Int8(v) => Ok(v.iter().map(|x| f32::from(*x)).collect()),
        VectorValues::Binary(_) => Err(ConversionError::OutOfRange {
            expected: "Vec<f32>",
            detail: "BINARY-format VECTOR has no float elements".to_string(),
        }),
    }
}

fn vector_to_f64(vector: &Vector) -> Result<Vec<f64>, ConversionError> {
    match dense_values(vector)? {
        VectorValues::Float64(v) => Ok(v.clone()),
        VectorValues::Float32(v) => Ok(v.iter().map(|x| f64::from(*x)).collect()),
        VectorValues::Int8(v) => Ok(v.iter().map(|x| f64::from(*x)).collect()),
        VectorValues::Binary(_) => Err(ConversionError::OutOfRange {
            expected: "Vec<f64>",
            detail: "BINARY-format VECTOR has no float elements".to_string(),
        }),
    }
}

// ---------------------------------------------------------------------------
// Typed access onto a fetched QueryResult
// ---------------------------------------------------------------------------

/// Typed accessors layered onto [`QueryResult`] so callers can pull a cell out
/// as a concrete Rust type by index or by column name.
///
/// This is an extension trait (the [`QueryResult`] type lives in the protocol
/// crate, which stays dependency-lean) — bring it into scope with
/// `use oracledb::QueryResultExt;`.
pub trait QueryResultExt {
    /// Convert the cell at `(row, col)` into `T`. A SQL `NULL` cell yields a
    /// [`ConversionError::UnexpectedNull`] unless `T` is `Option<_>`; an
    /// out-of-range index yields [`ConversionError::OutOfRange`].
    fn get<T: FromSql>(&self, row: usize, col: usize) -> crate::Result<T>;

    /// Convert the cell at `(row, column_name)` into `T`, resolving the column
    /// name case-insensitively (Oracle folds unquoted identifiers to upper
    /// case). An unknown column name yields [`ConversionError::OutOfRange`].
    fn get_by_name<T: FromSql>(&self, row: usize, name: &str) -> crate::Result<T>;

    /// Borrow row `row` as a [`TypedRow`] for repeated typed `get` calls
    /// without re-passing the row index.
    fn typed_row(&self, row: usize) -> TypedRow<'_>;

    /// Map **every** fetched row into a value of `T` (a struct deriving
    /// [`FromRow`]), returning them in order.
    ///
    /// Each row is converted through `T::from_row`, which goes through the real
    /// [`FromSql`] conversion per field. A conversion failure on any row aborts
    /// with that row's [`ConversionError`] (surfaced as [`crate::Error`]).
    ///
    /// ```no_run
    /// use oracledb::{FromRow, QueryResultExt};
    /// # use oracledb::protocol::thin::QueryResult;
    ///
    /// #[derive(FromRow)]
    /// struct Emp { id: i64, name: String, hired: Option<String> }
    ///
    /// # fn demo(result: QueryResult) -> oracledb::Result<()> {
    /// let emps: Vec<Emp> = result.rows_as::<Emp>()?;
    /// # let _ = emps;
    /// # Ok(())
    /// # }
    /// ```
    fn rows_as<T: FromRow>(&self) -> crate::Result<Vec<T>>;
}

fn convert_cell<T: FromSql>(cell: Option<&Option<QueryValue>>, what: String) -> crate::Result<T> {
    convert_cell_ce(cell, what).map_err(crate::Error::Conversion)
}

/// Like [`convert_cell`] but yields the bare [`ConversionError`] rather than
/// wrapping it in [`crate::Error`]. The `#[derive(FromRow)]` code maps each
/// **non-nullable** field through this, so a `NULL` cell is a hard
/// [`ConversionError::UnexpectedNull`]. `Option<T>` fields take the dedicated
/// NULL-tolerant path below instead.
fn convert_cell_ce<T: FromSql>(
    cell: Option<&Option<QueryValue>>,
    what: String,
) -> Result<T, ConversionError> {
    match cell {
        None => Err(ConversionError::OutOfRange {
            expected: std::any::type_name::<T>(),
            detail: what,
        }),
        Some(None) => Err(ConversionError::UnexpectedNull),
        Some(Some(value)) => T::from_sql(value),
    }
}

/// Like [`convert_cell_ce`] but turns a SQL `NULL` cell into `None` rather than
/// erroring. This is the path `#[derive(FromRow)]` takes for `Option<T>` fields,
/// so a nullable column maps to `Option<T>` with `NULL` -> `None`. A *missing*
/// column is still an error (a mistyped `#[oracledb(column = ...)]` should not
/// silently become `None`).
fn convert_cell_opt_ce<T: FromSql>(
    cell: Option<&Option<QueryValue>>,
    what: String,
) -> Result<Option<T>, ConversionError> {
    match cell {
        None => Err(ConversionError::OutOfRange {
            expected: std::any::type_name::<Option<T>>(),
            detail: what,
        }),
        Some(None) => Ok(None),
        Some(Some(value)) => T::from_sql(value).map(Some),
    }
}

impl QueryResultExt for QueryResult {
    fn get<T: FromSql>(&self, row: usize, col: usize) -> crate::Result<T> {
        let cell = self.rows.get(row).and_then(|r| r.get(col));
        convert_cell(cell, format!("no cell at (row {row}, col {col})"))
    }

    fn get_by_name<T: FromSql>(&self, row: usize, name: &str) -> crate::Result<T> {
        match self.column_index(name) {
            Some(col) => self.get(row, col),
            None => Err(crate::Error::Conversion(ConversionError::OutOfRange {
                expected: std::any::type_name::<T>(),
                detail: format!("no column named {name:?}"),
            })),
        }
    }

    fn typed_row(&self, row: usize) -> TypedRow<'_> {
        TypedRow { result: self, row }
    }

    fn rows_as<T: FromRow>(&self) -> crate::Result<Vec<T>> {
        let mut out = Vec::with_capacity(self.rows.len());
        for row in 0..self.rows.len() {
            out.push(T::from_row(&self.typed_row(row))?);
        }
        Ok(out)
    }
}

/// A borrowed view of one row of a [`QueryResult`] that converts cells to typed
/// Rust values. Obtain it with [`QueryResultExt::typed_row`].
#[derive(Clone, Copy)]
pub struct TypedRow<'a> {
    result: &'a QueryResult,
    row: usize,
}

impl TypedRow<'_> {
    /// Convert the cell in this row at column index `col` into `T`.
    pub fn get<T: FromSql>(&self, col: usize) -> crate::Result<T> {
        self.result.get(self.row, col)
    }

    /// Convert the cell in this row at column `name` (case-insensitive) into
    /// `T`.
    pub fn get_by_name<T: FromSql>(&self, name: &str) -> crate::Result<T> {
        self.result.get_by_name(self.row, name)
    }

    /// Borrow the raw cell at column index `col` of this row: `None` if the
    /// index is out of range, `Some(None)` for a SQL `NULL`, `Some(Some(v))`
    /// for a present value.
    fn cell_at(&self, col: usize) -> Option<&Option<QueryValue>> {
        self.result.rows.get(self.row).and_then(|r| r.get(col))
    }

    /// Convert the cell in this row at column index `col` into `T`, yielding the
    /// bare [`ConversionError`] on failure (unlike [`TypedRow::get`], which
    /// wraps it in [`crate::Error`]). A SQL `NULL` cell is rejected with
    /// [`ConversionError::UnexpectedNull`]; use [`TypedRow::try_get_opt`] for a
    /// nullable column.
    ///
    /// This is the accessor the `#[derive(FromRow)]`-generated code uses for
    /// non-`Option` tuple-struct fields. It is `pub` so generated code can call
    /// it; hand-written code usually wants [`TypedRow::get`].
    pub fn try_get<T: FromSql>(&self, col: usize) -> Result<T, ConversionError> {
        convert_cell_ce(
            self.cell_at(col),
            format!("no cell at (row {}, col {col})", self.row),
        )
    }

    /// Like [`TypedRow::try_get`] but for an `Option<T>` field: a SQL `NULL`
    /// cell becomes `None`. The accessor `#[derive(FromRow)]` uses for
    /// `Option<T>` tuple-struct fields.
    pub fn try_get_opt<T: FromSql>(&self, col: usize) -> Result<Option<T>, ConversionError> {
        convert_cell_opt_ce(
            self.cell_at(col),
            format!("no cell at (row {}, col {col})", self.row),
        )
    }

    /// Convert the cell in this row at column `name` (case-insensitive) into
    /// `T`, yielding the bare [`ConversionError`] on failure (unlike
    /// [`TypedRow::get_by_name`], which wraps it in [`crate::Error`]). A SQL
    /// `NULL` cell is rejected with [`ConversionError::UnexpectedNull`]; use
    /// [`TypedRow::try_get_by_name_opt`] for a nullable column.
    ///
    /// This is the accessor the `#[derive(FromRow)]`-generated code uses for
    /// non-`Option` named-field structs. It is `pub` so generated code can call
    /// it; hand-written code usually wants [`TypedRow::get_by_name`].
    pub fn try_get_by_name<T: FromSql>(&self, name: &str) -> Result<T, ConversionError> {
        match self.result.column_index(name) {
            Some(col) => self.try_get(col),
            None => Err(ConversionError::OutOfRange {
                expected: std::any::type_name::<T>(),
                detail: format!("no column named {name:?}"),
            }),
        }
    }

    /// Like [`TypedRow::try_get_by_name`] but for an `Option<T>` field: a SQL
    /// `NULL` cell becomes `None`. The accessor `#[derive(FromRow)]` uses for
    /// `Option<T>` named-field structs.
    pub fn try_get_by_name_opt<T: FromSql>(
        &self,
        name: &str,
    ) -> Result<Option<T>, ConversionError> {
        match self.result.column_index(name) {
            Some(col) => self.try_get_opt(col),
            None => Err(ConversionError::OutOfRange {
                expected: std::any::type_name::<Option<T>>(),
                detail: format!("no column named {name:?}"),
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// FromRow: a whole row -> a user struct (bead 4bv, the #[derive(FromRow)] target)
// ---------------------------------------------------------------------------

/// Map a fetched query row into a concrete Rust struct, with compile-time
/// checked field types.
///
/// You almost never implement this by hand. Instead derive it:
///
/// ```no_run
/// use oracledb::FromRow;
///
/// #[derive(FromRow)]
/// struct Emp {
///     id: i64,
///     name: String,
///     // a nullable column maps to Option<T>; a NULL cell becomes None
///     manager_id: Option<i64>,
/// }
/// ```
///
/// The derive maps each field **by column name** (the field name by default;
/// override per field with `#[oracledb(column = "...")]` or rename the whole
/// struct with `#[oracledb(rename_all = "...")]`), pulling it out through the
/// real [`FromSql`] conversion. Tuple structs map their fields **by position**.
/// Then [`QueryResultExt::rows_as`] turns a whole result set into a `Vec<T>`:
///
/// ```no_run
/// # use oracledb::{FromRow, QueryResultExt};
/// # use oracledb::protocol::thin::QueryResult;
/// # #[derive(FromRow)]
/// # struct Emp { id: i64, name: String }
/// # fn demo(result: QueryResult) -> oracledb::Result<()> {
/// let emps: Vec<Emp> = result.rows_as::<Emp>()?;
/// # let _ = emps;
/// # Ok(())
/// # }
/// ```
pub trait FromRow: Sized {
    /// Build `Self` from one borrowed [`TypedRow`], or fail with a
    /// [`ConversionError`].
    fn from_row(row: &TypedRow<'_>) -> Result<Self, ConversionError>;
}

// ===========================================================================
// ToSql: Rust value -> BindValue (feature 3, bead zjd)
// ===========================================================================

/// Convert a Rust value into an Oracle [`BindValue`] for a placeholder bind.
///
/// Implemented for the same scalar set as [`FromSql`] (core unconditionally;
/// `chrono`/`uuid`/`serde_json`/`rust_decimal`/`Vec<f32>` behind their
/// features). Each impl is a 1:1 map to an existing [`BindValue`] variant, so
/// `(40, "alice")` and [`params!`](crate::params) flow straight into the
/// execute helpers. `Option<T>` binds `None` as SQL `NULL`.
pub trait ToSql {
    /// Produce the [`BindValue`] this value binds as.
    fn to_sql(&self) -> BindValue;
}

impl ToSql for i64 {
    fn to_sql(&self) -> BindValue {
        BindValue::Number(self.to_string())
    }
}

impl ToSql for i32 {
    fn to_sql(&self) -> BindValue {
        BindValue::Number(self.to_string())
    }
}

impl ToSql for u32 {
    fn to_sql(&self) -> BindValue {
        BindValue::Number(self.to_string())
    }
}

impl ToSql for f64 {
    fn to_sql(&self) -> BindValue {
        BindValue::BinaryDouble(*self)
    }
}

impl ToSql for f32 {
    fn to_sql(&self) -> BindValue {
        BindValue::BinaryFloat(f64::from(*self))
    }
}

impl ToSql for bool {
    fn to_sql(&self) -> BindValue {
        BindValue::Boolean(*self)
    }
}

impl ToSql for str {
    fn to_sql(&self) -> BindValue {
        BindValue::Text(self.to_string())
    }
}

impl ToSql for String {
    fn to_sql(&self) -> BindValue {
        BindValue::Text(self.clone())
    }
}

impl ToSql for [u8] {
    fn to_sql(&self) -> BindValue {
        BindValue::Raw(self.to_vec())
    }
}

impl ToSql for Vec<u8> {
    fn to_sql(&self) -> BindValue {
        BindValue::Raw(self.clone())
    }
}

impl<T: ToSql + ?Sized> ToSql for &T {
    fn to_sql(&self) -> BindValue {
        (**self).to_sql()
    }
}

impl<T: ToSql> ToSql for Option<T> {
    fn to_sql(&self) -> BindValue {
        match self {
            Some(value) => value.to_sql(),
            None => BindValue::Null,
        }
    }
}

#[cfg(feature = "chrono")]
mod chrono_to_sql {
    use super::ToSql;
    use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
    use oracledb_protocol::thin::BindValue;

    impl ToSql for NaiveDateTime {
        fn to_sql(&self) -> BindValue {
            BindValue::Timestamp {
                // DB_TYPE_TIMESTAMP
                ora_type_num: 180,
                year: self.year(),
                month: self.month() as u8,
                day: self.day() as u8,
                hour: self.hour() as u8,
                minute: self.minute() as u8,
                second: self.second() as u8,
                nanosecond: self.nanosecond(),
            }
        }
    }

    impl ToSql for NaiveDate {
        fn to_sql(&self) -> BindValue {
            BindValue::DateTime {
                year: self.year(),
                month: self.month() as u8,
                day: self.day() as u8,
                hour: 0,
                minute: 0,
                second: 0,
            }
        }
    }
}

#[cfg(feature = "uuid")]
mod uuid_to_sql {
    use super::ToSql;
    use oracledb_protocol::thin::BindValue;
    use uuid::Uuid;

    impl ToSql for Uuid {
        fn to_sql(&self) -> BindValue {
            BindValue::Raw(self.as_bytes().to_vec())
        }
    }
}

#[cfg(feature = "serde_json")]
mod serde_json_to_sql {
    use super::ToSql;
    use oracledb_protocol::thin::BindValue;
    use serde_json::Value;

    impl ToSql for Value {
        fn to_sql(&self) -> BindValue {
            // Bind a JSON document as text; callers wanting native DB_TYPE_JSON
            // can encode OSON and bind BindValue::Json directly.
            BindValue::Text(self.to_string())
        }
    }
}

#[cfg(feature = "rust_decimal")]
mod rust_decimal_to_sql {
    use super::ToSql;
    use oracledb_protocol::thin::BindValue;
    use rust_decimal::Decimal;

    impl ToSql for Decimal {
        fn to_sql(&self) -> BindValue {
            // LOSSLESS: the canonical decimal text carries Decimal's full
            // ~28-digit precision straight into Oracle's text-NUMBER path with
            // no float rounding.
            BindValue::Number(self.to_string())
        }
    }
}

impl ToSql for Vec<f32> {
    fn to_sql(&self) -> BindValue {
        BindValue::Vector(Vector::Dense(VectorValues::Float32(self.clone())))
    }
}

impl ToSql for [f32] {
    fn to_sql(&self) -> BindValue {
        BindValue::Vector(Vector::Dense(VectorValues::Float32(self.to_vec())))
    }
}

// ---------------------------------------------------------------------------
// Single-row bind payload: positional or named
// ---------------------------------------------------------------------------

/// Single-row bind payload for the operation-family APIs.
///
/// `Params` keeps named binds first-class while preserving the existing
/// positional [`IntoBinds`] and [`params!`](crate::params) conveniences:
/// tuples/arrays/`Vec<T: ToSql>` become [`Params::Positional`], the named
/// `params!` arm becomes [`Params::Named`], and raw `BindValue` slices can be
/// borrowed without moving.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum Params<'a> {
    /// No binds.
    #[default]
    None,
    /// Positional binds (`:1`, `:2`, ...).
    Positional(Cow<'a, [BindValue]>),
    /// Named binds; execution reorders these to SQL placeholder first-use.
    Named(Cow<'a, [(String, BindValue)]>),
}

impl<'a, T: IntoBinds> From<T> for Params<'a> {
    fn from(value: T) -> Self {
        Params::Positional(Cow::Owned(value.into_binds()))
    }
}

impl<'a> From<&'a [BindValue]> for Params<'a> {
    fn from(value: &'a [BindValue]) -> Self {
        Params::Positional(Cow::Borrowed(value))
    }
}

impl<'a> From<&'a Vec<BindValue>> for Params<'a> {
    fn from(value: &'a Vec<BindValue>) -> Self {
        Params::from(value.as_slice())
    }
}

impl<'a> From<Vec<(String, BindValue)>> for Params<'a> {
    fn from(value: Vec<(String, BindValue)>) -> Self {
        Params::Named(Cow::Owned(value))
    }
}

impl<'a> From<&'a [(String, BindValue)]> for Params<'a> {
    fn from(value: &'a [(String, BindValue)]) -> Self {
        Params::Named(Cow::Borrowed(value))
    }
}

impl<'a> From<&'a Vec<(String, BindValue)>> for Params<'a> {
    fn from(value: &'a Vec<(String, BindValue)>) -> Self {
        Params::from(value.as_slice())
    }
}

// ---------------------------------------------------------------------------
// Positional bind sources: tuples, arrays, Vec
// ---------------------------------------------------------------------------

/// A source of positional binds (`:1`, `:2`, ...) for the ergonomic execute
/// helpers. Implemented for tuples up to arity 12, for `[T]` / `Vec<T>` of a
/// single [`ToSql`] type, and for `Vec<BindValue>` (the raw form).
pub trait IntoBinds {
    /// Materialize the positional bind values in order.
    fn into_binds(self) -> Vec<BindValue>;
}

impl IntoBinds for Vec<BindValue> {
    fn into_binds(self) -> Vec<BindValue> {
        self
    }
}

impl IntoBinds for () {
    fn into_binds(self) -> Vec<BindValue> {
        Vec::new()
    }
}

impl<T: ToSql> IntoBinds for Vec<T> {
    fn into_binds(self) -> Vec<BindValue> {
        self.iter().map(ToSql::to_sql).collect()
    }
}

impl<T: ToSql, const N: usize> IntoBinds for [T; N] {
    fn into_binds(self) -> Vec<BindValue> {
        self.iter().map(ToSql::to_sql).collect()
    }
}

macro_rules! impl_into_binds_tuple {
    ($($name:ident),+) => {
        impl<$($name: ToSql),+> IntoBinds for ($($name,)+) {
            fn into_binds(self) -> Vec<BindValue> {
                #[allow(non_snake_case)]
                let ($($name,)+) = self;
                vec![$($name.to_sql()),+]
            }
        }
    };
}

impl_into_binds_tuple!(A);
impl_into_binds_tuple!(A, B);
impl_into_binds_tuple!(A, B, C);
impl_into_binds_tuple!(A, B, C, D);
impl_into_binds_tuple!(A, B, C, D, E);
impl_into_binds_tuple!(A, B, C, D, E, F);
impl_into_binds_tuple!(A, B, C, D, E, F, G);
impl_into_binds_tuple!(A, B, C, D, E, F, G, H);
impl_into_binds_tuple!(A, B, C, D, E, F, G, H, I);
impl_into_binds_tuple!(A, B, C, D, E, F, G, H, I, J);
impl_into_binds_tuple!(A, B, C, D, E, F, G, H, I, J, K);
impl_into_binds_tuple!(A, B, C, D, E, F, G, H, I, J, K, L);

/// Build a positional bind list from a heterogeneous set of [`ToSql`] values.
///
/// `params![40, "alice", true]` is sugar for an [`IntoBinds`] list and produces
/// a `Vec<BindValue>` directly. For *named* binds, pass `name => value` pairs;
/// this produces a `Vec<(String, BindValue)>` for the named-execute helpers.
///
/// ```
/// use oracledb::params;
/// // positional
/// let binds = params![40, "alice"];
/// assert_eq!(binds.len(), 2);
/// // named
/// let named = params!{ ":id" => 40, ":name" => "alice" };
/// assert_eq!(named.len(), 2);
/// ```
#[macro_export]
macro_rules! params {
    // named form: ":name" => value, ...
    ($($name:expr => $value:expr),+ $(,)?) => {{
        let binds: ::std::vec::Vec<(::std::string::String, $crate::protocol::thin::BindValue)> =
            ::std::vec![$(
                (::std::string::String::from($name), $crate::ToSql::to_sql(&$value))
            ),+];
        binds
    }};
    // positional form: value, ...
    ($($value:expr),+ $(,)?) => {{
        let binds: ::std::vec::Vec<$crate::protocol::thin::BindValue> =
            ::std::vec![$( $crate::ToSql::to_sql(&$value) ),+];
        binds
    }};
}

/// Order a named-bind list into the positional order the driver expects.
///
/// Oracle placeholders fill positionally in *first-appearance* order in the SQL
/// text, so `params!{ ":b" => 2, ":a" => 1 }` against `... :a ... :b ...` must
/// be reordered to `[1, 2]`. This scans the SQL for `:name` placeholders
/// (ignoring those inside single-quoted string literals), then emits each
/// supplied bind value once, in the order its name first appears. A name in the
/// list that never appears in the SQL is appended at the end (the server will
/// report the mismatch, matching the raw positional path).
pub(crate) fn order_named_binds(sql: &str, named: Vec<(String, BindValue)>) -> Vec<BindValue> {
    let order = placeholder_order(sql);
    let mut remaining = named;
    let mut out = Vec::with_capacity(remaining.len());
    for placeholder in &order {
        if let Some(pos) = remaining
            .iter()
            .position(|(name, _)| name_matches(name, placeholder))
        {
            let (_, value) = remaining.remove(pos);
            out.push(value);
        }
    }
    // Any leftover named binds the SQL did not reference, in their given order.
    for (_, value) in remaining {
        out.push(value);
    }
    out
}

/// Resolve a public [`Params`] payload into the positional bind vector the
/// current wire path expects.
pub(crate) fn resolve_params(sql: &str, params: Params<'_>) -> Vec<BindValue> {
    match params {
        Params::None => Vec::new(),
        Params::Positional(binds) => binds.into_owned(),
        Params::Named(named) => order_named_binds(sql, named.into_owned()),
    }
}

/// `:id` in `params!` vs `id` scanned from the SQL: compare case-insensitively
/// after stripping a single leading colon from either side.
fn name_matches(supplied: &str, scanned: &str) -> bool {
    supplied
        .trim_start_matches(':')
        .eq_ignore_ascii_case(scanned.trim_start_matches(':'))
}

/// Scan `sql` for bind placeholders (`:name` or `:1`), returning each distinct
/// placeholder name in first-appearance order. Quoted string literals and
/// `--`/`/* */` comments are skipped so a colon inside text is not mistaken for
/// a placeholder.
fn placeholder_order(sql: &str) -> Vec<String> {
    let bytes = sql.as_bytes();
    let mut seen: Vec<String> = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                // skip a single-quoted literal (doubled '' stays inside)
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            b'"' => {
                // skip a quoted identifier
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += 1;
                }
                i += 1;
            }
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i += 2;
            }
            b':' => {
                let start = i + 1;
                let mut j = start;
                while j < bytes.len()
                    && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_' || bytes[j] == b'$')
                {
                    j += 1;
                }
                if j > start {
                    let name = sql[start..j].to_string();
                    if !seen.iter().any(|n| n.eq_ignore_ascii_case(&name)) {
                        seen.push(name);
                    }
                }
                i = j;
            }
            _ => i += 1,
        }
    }
    seen
}

#[cfg(test)]
mod tests {
    use super::*;
    use oracledb_protocol::thin::ColumnMetadata;

    fn num(text: &str) -> QueryValue {
        QueryValue::number_from_text(text, !text.contains('.'))
    }

    #[test]
    fn core_from_sql_scalars() {
        assert_eq!(i64::from_sql(&num("42")).unwrap(), 42);
        assert_eq!(i32::from_sql(&num("42")).unwrap(), 42);
        assert_eq!(u32::from_sql(&num("42")).unwrap(), 42);
        assert_eq!(f64::from_sql(&num("2.5")).unwrap(), 2.5);
        assert_eq!(f32::from_sql(&num("2.5")).unwrap(), 2.5_f32);
        assert!(bool::from_sql(&QueryValue::Boolean(true)).unwrap());
        assert!(bool::from_sql(&num("1")).unwrap());
        assert_eq!(
            String::from_sql(&QueryValue::Text("hi".into())).unwrap(),
            "hi"
        );
        assert_eq!(
            Vec::<u8>::from_sql(&QueryValue::Raw(vec![1, 2, 3])).unwrap(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn i128_from_sql_is_exact_beyond_i64() {
        // A 30-digit integer that exceeds i64 but fits i128 must reconstruct
        // exactly from the inline coefficient (no string round-trip loss).
        let big = "123456789012345678901234567890";
        assert_eq!(
            i128::from_sql(&num(big)).unwrap(),
            123_456_789_012_345_678_901_234_567_890_i128
        );
        // i64 of the same value is out of range (typed error, not a panic).
        assert!(matches!(
            i64::from_sql(&num(big)).unwrap_err(),
            ConversionError::OutOfRange { .. }
        ));
        // A fractional NUMBER is not an integer -> typed OutOfRange.
        assert!(matches!(
            i128::from_sql(&num("3.14")).unwrap_err(),
            ConversionError::OutOfRange { .. }
        ));
    }

    #[test]
    fn string_from_sql_is_canonical_byte_exact() {
        // The String conversion must yield the exact canonical NUMBER text.
        for text in ["0", "-1", "2.5", "100", "0.001", "12345678901234567890"] {
            assert_eq!(String::from_sql(&num(text)).unwrap(), text);
        }
    }

    #[test]
    fn from_sql_errors_are_typed() {
        // type mismatch
        let err = i64::from_sql(&QueryValue::Text("x".into())).unwrap_err();
        assert!(matches!(err, ConversionError::TypeMismatch { .. }));
        // out of range
        let err = i32::from_sql(&num("9999999999")).unwrap_err();
        assert!(matches!(err, ConversionError::OutOfRange { .. }));
    }

    #[test]
    fn option_accepts_any() {
        let v: Option<i64> = Option::<i64>::from_sql(&num("7")).unwrap();
        assert_eq!(v, Some(7));
    }

    #[test]
    fn core_to_sql_scalars() {
        assert_eq!(40_i64.to_sql(), BindValue::Number("40".into()));
        assert_eq!(40_i32.to_sql(), BindValue::Number("40".into()));
        assert_eq!(2.5_f64.to_sql(), BindValue::BinaryDouble(2.5));
        assert_eq!(true.to_sql(), BindValue::Boolean(true));
        assert_eq!("alice".to_sql(), BindValue::Text("alice".into()));
        assert_eq!(vec![1u8, 2, 3].to_sql(), BindValue::Raw(vec![1, 2, 3]));
        let none: Option<i64> = None;
        assert_eq!(none.to_sql(), BindValue::Null);
    }

    #[test]
    fn into_binds_tuple_and_slice() {
        let binds = (40_i64, "alice").into_binds();
        assert_eq!(
            binds,
            vec![
                BindValue::Number("40".into()),
                BindValue::Text("alice".into())
            ]
        );
        let binds = [1_i64, 2, 3].into_binds();
        assert_eq!(binds.len(), 3);
    }

    #[test]
    fn params_macro_positional_and_named() {
        let positional = params![40_i64, "alice"];
        assert_eq!(
            positional,
            vec![
                BindValue::Number("40".into()),
                BindValue::Text("alice".into())
            ]
        );
        let named = params! { ":id" => 40_i64, ":name" => "alice" };
        assert_eq!(named.len(), 2);
        assert_eq!(named[0].0, ":id");
        assert_eq!(named[0].1, BindValue::Number("40".into()));
        assert_eq!(named[1].0, ":name");
    }

    #[test]
    fn params_from_positional_sources() {
        assert_eq!(Params::default(), Params::None);
        assert_eq!(Params::from(()), Params::Positional(Cow::Owned(Vec::new())));

        let from_tuple = Params::from((40_i64, "alice"));
        assert_eq!(
            from_tuple,
            Params::Positional(Cow::Owned(vec![
                BindValue::Number("40".into()),
                BindValue::Text("alice".into())
            ]))
        );

        let raw = vec![BindValue::Number("7".into()), BindValue::Boolean(true)];
        assert_eq!(
            Params::from(raw.clone()),
            Params::Positional(Cow::Owned(raw.clone()))
        );
        assert_eq!(
            Params::from(raw.as_slice()),
            Params::Positional(Cow::Borrowed(raw.as_slice()))
        );
        assert_eq!(
            Params::from(&raw),
            Params::Positional(Cow::Borrowed(raw.as_slice()))
        );

        let empty: &[BindValue] = &[];
        assert_eq!(
            Params::from(empty),
            Params::Positional(Cow::Borrowed(empty))
        );
    }

    #[test]
    fn params_from_named_sources() {
        let named = params! { ":id" => 40_i64, ":name" => "alice" };

        assert_eq!(
            Params::from(named.clone()),
            Params::Named(Cow::Owned(named.clone()))
        );
        assert_eq!(
            Params::from(named.as_slice()),
            Params::Named(Cow::Borrowed(named.as_slice()))
        );
        assert_eq!(
            Params::from(&named),
            Params::Named(Cow::Borrowed(named.as_slice()))
        );

        let empty: Vec<(String, BindValue)> = Vec::new();
        assert_eq!(
            Params::from(empty.clone()),
            Params::Named(Cow::Owned(empty.clone()))
        );
        assert_eq!(
            Params::from(empty.as_slice()),
            Params::Named(Cow::Borrowed(empty.as_slice()))
        );
    }

    #[test]
    fn resolve_params_reuses_named_ordering() {
        let named = params! { ":b" => 2_i64, ":a" => 1_i64 };
        let ordered = resolve_params(
            "select * from t where a = :a and b = :b",
            Params::from(named),
        );
        assert_eq!(
            ordered,
            vec![BindValue::Number("1".into()), BindValue::Number("2".into())]
        );

        assert!(resolve_params("select 1 from dual", Params::None).is_empty());
    }

    #[test]
    fn query_result_typed_get() {
        let result = QueryResult {
            columns: vec![
                ColumnMetadata {
                    name: "ID".into(),
                    ..Default::default()
                },
                ColumnMetadata {
                    name: "NAME".into(),
                    ..Default::default()
                },
            ],
            rows: vec![vec![Some(num("7")), Some(QueryValue::Text("bob".into()))]],
            ..Default::default()
        };
        assert_eq!(result.get::<i64>(0, 0).unwrap(), 7);
        assert_eq!(result.get_by_name::<i64>(0, "id").unwrap(), 7);
        assert_eq!(result.get_by_name::<String>(0, "name").unwrap(), "bob");
        let row = result.typed_row(0);
        assert_eq!(row.get::<i64>(0).unwrap(), 7);
        assert_eq!(row.get_by_name::<String>("NAME").unwrap(), "bob");
        // missing column name -> typed conversion error
        assert!(result.get_by_name::<i64>(0, "nope").is_err());
    }

    #[cfg(feature = "rust_decimal")]
    #[test]
    fn decimal_roundtrip_is_lossless_to_full_precision() {
        use rust_decimal::Decimal;
        use std::str::FromStr;
        // `rust_decimal::Decimal` holds a 96-bit mantissa (~28-29 significant
        // digits). At the *full* precision the type can represent, the
        // text-NUMBER carrier preserves every digit exactly in both
        // directions — no float rounding anywhere. (python-oracledb hands you a
        // lossy f64 of ~15-17 digits unless you opt into decimal.Decimal.)
        let text = "7922816251426433759354.395033"; // 28 significant digits
        let dec = Decimal::from_str(text).unwrap();
        // ToSql carries the exact text into the NUMBER bind...
        let bind = dec.to_sql();
        assert_eq!(bind, BindValue::Number(text.to_string()));
        // ...and FromSql recovers the exact Decimal from the NUMBER carrier.
        let back = Decimal::from_sql(&num(text)).unwrap();
        assert_eq!(back, dec);
        assert_eq!(back.to_string(), text);

        // An f64 would corrupt this 28-digit value; Decimal does not.
        let as_f64: f64 = text.parse().unwrap();
        assert_ne!(
            as_f64.to_string(),
            text,
            "f64 must lose precision here, proving Decimal is the lossless path"
        );
    }

    #[cfg(feature = "chrono")]
    #[test]
    fn chrono_from_and_to_sql() {
        use chrono::{NaiveDate, NaiveDateTime};
        let dt = QueryValue::DateTime {
            year: 2026,
            month: 6,
            day: 14,
            hour: 12,
            minute: 30,
            second: 45,
            nanosecond: 123_456_789,
        };
        let parsed = NaiveDateTime::from_sql(&dt).unwrap();
        assert_eq!(parsed.to_string(), "2026-06-14 12:30:45.123456789");
        let date = NaiveDate::from_sql(&dt).unwrap();
        assert_eq!(date, NaiveDate::from_ymd_opt(2026, 6, 14).unwrap());
        // ToSql produces a Timestamp bind carrying the same components.
        match parsed.to_sql() {
            BindValue::Timestamp {
                year, nanosecond, ..
            } => {
                assert_eq!(year, 2026);
                assert_eq!(nanosecond, 123_456_789);
            }
            other => panic!("expected Timestamp bind, got {other:?}"),
        }
    }

    #[cfg(feature = "uuid")]
    #[test]
    fn uuid_from_raw_and_text() {
        use uuid::Uuid;
        let id = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        // from RAW(16)
        let raw = QueryValue::Raw(id.as_bytes().to_vec());
        assert_eq!(Uuid::from_sql(&raw).unwrap(), id);
        // from text
        let text = QueryValue::Text(id.to_string());
        assert_eq!(Uuid::from_sql(&text).unwrap(), id);
        // ToSql -> RAW(16)
        assert_eq!(id.to_sql(), BindValue::Raw(id.as_bytes().to_vec()));
    }

    #[cfg(feature = "serde_json")]
    #[test]
    fn serde_json_from_oson_tree() {
        use oracledb_protocol::oson::OsonValue;
        use serde_json::json;
        let oson = OsonValue::Object(vec![
            ("id".into(), OsonValue::Number("7".into())),
            ("name".into(), OsonValue::String("bob".into())),
            ("active".into(), OsonValue::Bool(true)),
            (
                "tags".into(),
                OsonValue::Array(vec![
                    OsonValue::String("a".into()),
                    OsonValue::String("b".into()),
                ]),
            ),
        ]);
        let value = serde_json::Value::from_sql(&QueryValue::Json(Box::new(oson))).unwrap();
        assert_eq!(
            value,
            json!({"id": 7, "name": "bob", "active": true, "tags": ["a", "b"]})
        );
    }

    #[test]
    fn named_binds_reorder_to_first_appearance() {
        // :a appears before :b in the SQL, but the params! list is in the
        // opposite order. order_named_binds must reorder to [a=1, b=2].
        let named = params! { ":b" => 2_i64, ":a" => 1_i64 };
        let ordered = order_named_binds("select * from t where a = :a and b = :b", named);
        assert_eq!(
            ordered,
            vec![BindValue::Number("1".into()), BindValue::Number("2".into())]
        );
    }

    #[test]
    fn named_binds_repeated_placeholder_counts_once() {
        // :id used twice should appear once in the ordering.
        let named = params! { ":id" => 5_i64, ":name" => "x" };
        let ordered = order_named_binds("select :id from t where id = :id and name = :name", named);
        assert_eq!(
            ordered,
            vec![BindValue::Number("5".into()), BindValue::Text("x".into())]
        );
    }

    #[test]
    fn named_binds_ignore_colon_in_string_literal() {
        // The ':not_a_bind' inside the literal must not be treated as a
        // placeholder; only :real is.
        let named = params! { ":real" => 9_i64 };
        let ordered = order_named_binds("select 'time is 12:30' as t, :real from dual", named);
        assert_eq!(ordered, vec![BindValue::Number("9".into())]);
    }

    #[test]
    fn vector_from_sql_f32_f64() {
        let vector = QueryValue::Vector(Box::new(Vector::Dense(VectorValues::Float32(vec![
            1.0, 2.0, 3.0,
        ]))));
        assert_eq!(Vec::<f32>::from_sql(&vector).unwrap(), vec![1.0, 2.0, 3.0]);
        assert_eq!(Vec::<f64>::from_sql(&vector).unwrap(), vec![1.0, 2.0, 3.0]);
        // ToSql round-trips Vec<f32> into a dense float32 vector bind.
        assert_eq!(
            vec![1.0_f32, 2.0, 3.0].to_sql(),
            BindValue::Vector(Vector::Dense(VectorValues::Float32(vec![1.0, 2.0, 3.0])))
        );
    }
}
