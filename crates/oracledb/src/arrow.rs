//! Fetch-to-Arrow conversion (foundation for `fetch_df_all` /
//! `fetch_df_batches` / DataFrame ingestion).
//!
//! Pure conversion layer: turns fetched rows (`QueryValue`) plus column
//! metadata into [`RecordBatch`]es following the python-oracledb v4.0.1
//! reference type mapping (impl/base/metadata.pyx `_create_arrow_schema`,
//! impl/base/converters.pyx). Errors mirror the reference's DPY-coded
//! messages so a Python-facing layer can map them one-to-one.
//!
//! No pyo3 here by design: the PyCapsule export of these batches lives in the
//! shim crate.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Date64Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, Int8Builder, LargeBinaryBuilder, LargeStringBuilder, StringBuilder,
    UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow_array::types::{
    ArrowTimestampType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use arrow_array::{Array, ArrayRef, PrimitiveArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

use oracledb_protocol::dpl::DirectPathColumnValue;
use oracledb_protocol::thin::{
    ColumnMetadata, QueryValue, CS_FORM_NCHAR, ORA_TYPE_NUM_BINARY_DOUBLE,
    ORA_TYPE_NUM_BINARY_FLOAT, ORA_TYPE_NUM_BLOB, ORA_TYPE_NUM_BOOLEAN, ORA_TYPE_NUM_CHAR,
    ORA_TYPE_NUM_CLOB, ORA_TYPE_NUM_DATE, ORA_TYPE_NUM_LONG, ORA_TYPE_NUM_LONG_RAW,
    ORA_TYPE_NUM_NUMBER, ORA_TYPE_NUM_RAW, ORA_TYPE_NUM_TIMESTAMP, ORA_TYPE_NUM_TIMESTAMP_LTZ,
    ORA_TYPE_NUM_TIMESTAMP_TZ, ORA_TYPE_NUM_VARCHAR, TNS_MAX_LONG_LENGTH,
};

const ORA_TYPE_NUM_VECTOR: u8 = 127;
const ORA_TYPE_NUM_JSON: u8 = 119;
const ORA_TYPE_NUM_INTERVAL_YM: u8 = 182;
const ORA_TYPE_NUM_INTERVAL_DS: u8 = 183;

/// Errors raised by the fetch->Arrow and Arrow->bind conversion paths.
///
/// Messages are prefixed with the python-oracledb error number they
/// correspond to so the shim layer can surface exact reference errors.
#[derive(Debug, thiserror::Error)]
pub enum ArrowConversionError {
    #[error(
        "DPY-3030: conversion from Oracle Database type {db_type_name} \
         to Apache Arrow format is not supported"
    )]
    UnsupportedDataType { db_type_name: String },
    #[error(
        "DPY-3031: flexible vector formats are not supported. Only fixed 'FLOAT32', \
         'FLOAT64', 'INT8' or 'BINARY' formats are supported"
    )]
    UnsupportedVectorFormat,
    #[error(
        "DPY-2069: requested schema has {num_schema_columns} columns defined \
         but {num_fetched_columns} are being fetched"
    )]
    WrongRequestedSchemaLength {
        num_schema_columns: usize,
        num_fetched_columns: usize,
    },
    #[error("DPY-3038: database type \"{db_type}\" cannot be converted to Apache Arrow type \"{arrow_type}\"")]
    CannotConvertToArrow { arrow_type: String, db_type: String },
    #[error("DPY-3039: Apache Arrow type \"{arrow_type}\" cannot be converted to database type \"{db_type}\"")]
    CannotConvertFromArrow { arrow_type: String, db_type: String },
    #[error("DPY-4036: {value} cannot be converted to an Apache Arrow integer")]
    CannotConvertToInteger { value: String },
    #[error("DPY-4037: {value} cannot be converted to an Apache Arrow double")]
    CannotConvertToDouble { value: String },
    #[error("DPY-4039: {value} cannot be converted to an Apache Arrow float")]
    CannotConvertToFloat { value: String },
    #[error("value cannot be represented as Arrow Decimal128: {value}")]
    DecimalOutOfRange { value: String },
    #[error("column \"{column_name}\": {reason}")]
    InvalidValue { column_name: String, reason: String },
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),
    #[error(transparent)]
    Arrow(#[from] arrow_schema::ArrowError),
}

type Result<T> = std::result::Result<T, ArrowConversionError>;

/// Options controlling the fetch->Arrow conversion.
#[derive(Clone, Debug, Default)]
pub struct ArrowFetchOptions {
    /// `fetch_decimals` semantics: NUMBER columns with `0 < precision <= 38`
    /// become `decimal128(precision, scale)` instead of int64/float64.
    pub fetch_decimals: bool,
    /// Caller-requested output schema (`fetch_df_*(requested_schema=...)`).
    /// Must have exactly one field per fetched column; renames the output
    /// columns and coerces values per the reference conversion matrix.
    pub requested_schema: Option<SchemaRef>,
}

/// Oracle VECTOR storage formats (reference `VECTOR_FORMAT_*`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VectorFormat {
    Float32,
    Float64,
    Int8,
    Binary,
}

/// Arrow type for a VECTOR column of the given format (metadata.pyx:84-99).
/// Sparse vectors map to a struct of num_dimensions/indices/values; dense
/// vectors map to `list<element>`.
pub fn vector_arrow_type(format: VectorFormat, sparse: bool) -> DataType {
    let element = match format {
        VectorFormat::Float32 => DataType::Float32,
        VectorFormat::Float64 => DataType::Float64,
        VectorFormat::Int8 => DataType::Int8,
        VectorFormat::Binary => DataType::UInt8,
    };
    let values = Arc::new(Field::new("item", element, true));
    if sparse {
        DataType::Struct(
            vec![
                Field::new("num_dimensions", DataType::Int64, true),
                Field::new(
                    "indices",
                    DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))),
                    true,
                ),
                Field::new("values", DataType::List(values), true),
            ]
            .into(),
        )
    } else {
        DataType::List(values)
    }
}

/// Reference-style `DB_TYPE_*` name for a fetched column (used in DPY-3030 /
/// DPY-3038 message parity).
pub fn db_type_name(column: &ColumnMetadata) -> String {
    let nchar = column.csfrm == CS_FORM_NCHAR;
    let name = match column.ora_type_num {
        ORA_TYPE_NUM_VARCHAR if nchar => "DB_TYPE_NVARCHAR",
        ORA_TYPE_NUM_VARCHAR => "DB_TYPE_VARCHAR",
        ORA_TYPE_NUM_NUMBER => "DB_TYPE_NUMBER",
        3 => "DB_TYPE_BINARY_INTEGER",
        ORA_TYPE_NUM_LONG if nchar => "DB_TYPE_LONG_NVARCHAR",
        ORA_TYPE_NUM_LONG => "DB_TYPE_LONG",
        11 | 208 => "DB_TYPE_ROWID",
        ORA_TYPE_NUM_DATE => "DB_TYPE_DATE",
        ORA_TYPE_NUM_RAW => "DB_TYPE_RAW",
        ORA_TYPE_NUM_LONG_RAW => "DB_TYPE_LONG_RAW",
        ORA_TYPE_NUM_CHAR if nchar => "DB_TYPE_NCHAR",
        ORA_TYPE_NUM_CHAR => "DB_TYPE_CHAR",
        ORA_TYPE_NUM_BINARY_FLOAT => "DB_TYPE_BINARY_FLOAT",
        ORA_TYPE_NUM_BINARY_DOUBLE => "DB_TYPE_BINARY_DOUBLE",
        102 => "DB_TYPE_CURSOR",
        109 => "DB_TYPE_OBJECT",
        ORA_TYPE_NUM_CLOB if nchar => "DB_TYPE_NCLOB",
        ORA_TYPE_NUM_CLOB => "DB_TYPE_CLOB",
        ORA_TYPE_NUM_BLOB => "DB_TYPE_BLOB",
        114 => "DB_TYPE_BFILE",
        ORA_TYPE_NUM_JSON => "DB_TYPE_JSON",
        ORA_TYPE_NUM_VECTOR => "DB_TYPE_VECTOR",
        ORA_TYPE_NUM_TIMESTAMP => "DB_TYPE_TIMESTAMP",
        ORA_TYPE_NUM_TIMESTAMP_TZ => "DB_TYPE_TIMESTAMP_TZ",
        ORA_TYPE_NUM_TIMESTAMP_LTZ => "DB_TYPE_TIMESTAMP_LTZ",
        ORA_TYPE_NUM_INTERVAL_YM => "DB_TYPE_INTERVAL_YM",
        ORA_TYPE_NUM_INTERVAL_DS => "DB_TYPE_INTERVAL_DS",
        _ => "DB_TYPE_UNKNOWN",
    };
    name.to_string()
}

/// Nanoarrow-style type name for error message parity with the reference.
pub fn arrow_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "na".to_string(),
        DataType::Boolean => "bool".to_string(),
        DataType::Int8 => "int8".to_string(),
        DataType::Int16 => "int16".to_string(),
        DataType::Int32 => "int32".to_string(),
        DataType::Int64 => "int64".to_string(),
        DataType::UInt8 => "uint8".to_string(),
        DataType::UInt16 => "uint16".to_string(),
        DataType::UInt32 => "uint32".to_string(),
        DataType::UInt64 => "uint64".to_string(),
        DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Utf8 => "string".to_string(),
        DataType::LargeUtf8 => "large_string".to_string(),
        DataType::Utf8View => "string_view".to_string(),
        DataType::Binary => "binary".to_string(),
        DataType::LargeBinary => "large_binary".to_string(),
        DataType::BinaryView => "binary_view".to_string(),
        DataType::FixedSizeBinary(_) => "fixed_size_binary".to_string(),
        DataType::Decimal128(_, _) => "decimal128".to_string(),
        DataType::Date32 => "date32".to_string(),
        DataType::Date64 => "date64".to_string(),
        DataType::Timestamp(_, _) => "timestamp".to_string(),
        DataType::List(_) => "list".to_string(),
        DataType::FixedSizeList(_, _) => "fixed_size_list".to_string(),
        DataType::Struct(_) => "struct".to_string(),
        other => format!("{other:?}").to_lowercase(),
    }
}

fn is_char_like(column: &ColumnMetadata) -> bool {
    matches!(
        column.ora_type_num,
        ORA_TYPE_NUM_VARCHAR | ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_LONG
    )
}

fn is_datetime_like(column: &ColumnMetadata) -> bool {
    matches!(
        column.ora_type_num,
        ORA_TYPE_NUM_DATE
            | ORA_TYPE_NUM_TIMESTAMP
            | ORA_TYPE_NUM_TIMESTAMP_TZ
            | ORA_TYPE_NUM_TIMESTAMP_LTZ
    )
}

/// Default DB->Arrow type for one fetched column
/// (metadata.pyx `_create_arrow_schema`).
fn default_arrow_type(column: &ColumnMetadata, options: &ArrowFetchOptions) -> Result<DataType> {
    match column.ora_type_num {
        ORA_TYPE_NUM_NUMBER => {
            if options.fetch_decimals && (1..=38).contains(&column.precision) {
                Ok(DataType::Decimal128(column.precision as u8, column.scale))
            } else if !options.fetch_decimals
                && column.scale == 0
                && (1..=18).contains(&column.precision)
            {
                Ok(DataType::Int64)
            } else {
                Ok(DataType::Float64)
            }
        }
        ORA_TYPE_NUM_VARCHAR | ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_LONG => Ok(DataType::LargeUtf8),
        ORA_TYPE_NUM_RAW | ORA_TYPE_NUM_LONG_RAW => Ok(DataType::LargeBinary),
        ORA_TYPE_NUM_BINARY_FLOAT => Ok(DataType::Float32),
        ORA_TYPE_NUM_BINARY_DOUBLE => Ok(DataType::Float64),
        ORA_TYPE_NUM_BOOLEAN => Ok(DataType::Boolean),
        ORA_TYPE_NUM_DATE
        | ORA_TYPE_NUM_TIMESTAMP
        | ORA_TYPE_NUM_TIMESTAMP_TZ
        | ORA_TYPE_NUM_TIMESTAMP_LTZ => {
            // DATE describes with scale 0 -> seconds; fractional-second scale
            // picks ms/us/ns (metadata.pyx:65-75)
            let unit = match column.scale {
                1..=3 => TimeUnit::Millisecond,
                4..=6 => TimeUnit::Microsecond,
                7..=9 => TimeUnit::Nanosecond,
                _ => TimeUnit::Second,
            };
            Ok(DataType::Timestamp(unit, None))
        }
        ORA_TYPE_NUM_VECTOR => Err(ArrowConversionError::NotImplemented(
            "VECTOR columns require vector format metadata that the describe \
             parser does not yet capture; see vector_arrow_type for the mapping",
        )),
        _ => Err(ArrowConversionError::UnsupportedDataType {
            db_type_name: db_type_name(column),
        }),
    }
}

/// Reference conversion matrix for `requested_schema`
/// (metadata.pyx `check_convert_to_arrow`). Note that string_view /
/// binary_view are NOT accepted on the fetch side.
fn check_convert_to_arrow(column: &ColumnMetadata, requested: &DataType) -> Result<()> {
    let ok = match column.ora_type_num {
        ORA_TYPE_NUM_NUMBER => matches!(
            requested,
            DataType::Decimal128(_, _)
                | DataType::Float64
                | DataType::Float32
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        ),
        ORA_TYPE_NUM_RAW | ORA_TYPE_NUM_LONG_RAW | ORA_TYPE_NUM_BLOB => matches!(
            requested,
            DataType::Binary | DataType::FixedSizeBinary(_) | DataType::LargeBinary
        ),
        ORA_TYPE_NUM_BOOLEAN => matches!(requested, DataType::Boolean),
        ORA_TYPE_NUM_DATE
        | ORA_TYPE_NUM_TIMESTAMP
        | ORA_TYPE_NUM_TIMESTAMP_TZ
        | ORA_TYPE_NUM_TIMESTAMP_LTZ => matches!(
            requested,
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None)
        ),
        ORA_TYPE_NUM_BINARY_FLOAT | ORA_TYPE_NUM_BINARY_DOUBLE => {
            matches!(requested, DataType::Float32 | DataType::Float64)
        }
        ORA_TYPE_NUM_VARCHAR | ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_LONG | ORA_TYPE_NUM_CLOB => {
            matches!(requested, DataType::Utf8 | DataType::LargeUtf8)
        }
        _ => false,
    };
    if ok {
        Ok(())
    } else {
        Err(ArrowConversionError::CannotConvertToArrow {
            arrow_type: arrow_type_name(requested),
            db_type: db_type_name(column),
        })
    }
}

/// Computes the Arrow schema produced by [`build_record_batch`] for the
/// given fetch metadata and options.
pub fn arrow_schema_for_columns(
    columns: &[ColumnMetadata],
    options: &ArrowFetchOptions,
) -> Result<Schema> {
    if let Some(requested) = &options.requested_schema {
        if requested.fields().len() != columns.len() {
            return Err(ArrowConversionError::WrongRequestedSchemaLength {
                num_schema_columns: requested.fields().len(),
                num_fetched_columns: columns.len(),
            });
        }
        let mut fields = Vec::with_capacity(columns.len());
        for (column, requested_field) in columns.iter().zip(requested.fields()) {
            check_convert_to_arrow(column, requested_field.data_type())?;
            fields.push(Field::new(
                requested_field.name(),
                requested_field.data_type().clone(),
                true,
            ));
        }
        return Ok(Schema::new(fields));
    }
    let mut fields = Vec::with_capacity(columns.len());
    for column in columns {
        let data_type = default_arrow_type(column, options)?;
        fields.push(Field::new(&column.name, data_type, true));
    }
    Ok(Schema::new(fields))
}

/// Define-type coercions applied when fetching for Arrow: LOBs are inlined
/// (CLOB -> LONG, NCLOB -> LONG with NCHAR form, BLOB -> LONG RAW) so values
/// arrive as inline text/raw instead of locators (cursor.pyx:224-233).
pub fn arrow_define_columns(columns: &[ColumnMetadata]) -> Vec<ColumnMetadata> {
    columns
        .iter()
        .map(|column| {
            let mut column = column.clone();
            match column.ora_type_num {
                ORA_TYPE_NUM_CLOB => {
                    column.ora_type_num = ORA_TYPE_NUM_LONG;
                    column.buffer_size = TNS_MAX_LONG_LENGTH;
                    column.max_size = TNS_MAX_LONG_LENGTH;
                }
                ORA_TYPE_NUM_BLOB => {
                    column.ora_type_num = ORA_TYPE_NUM_LONG_RAW;
                    column.csfrm = 0;
                    column.buffer_size = TNS_MAX_LONG_LENGTH;
                    column.max_size = TNS_MAX_LONG_LENGTH;
                }
                _ => {}
            }
            column
        })
        .collect()
}

/// Builds one [`RecordBatch`] from fetched rows.
///
/// Every row must have one value per column. An empty `rows` slice produces a
/// zero-length array per column (required for empty result sets).
pub fn build_record_batch(
    columns: &[ColumnMetadata],
    rows: &[Vec<Option<QueryValue>>],
    options: &ArrowFetchOptions,
) -> Result<RecordBatch> {
    let schema = Arc::new(arrow_schema_for_columns(columns, options)?);
    build_record_batch_with_schema(schema, columns, rows)
}

/// As [`build_record_batch`] but with a precomputed schema (one fetch can
/// produce many batches; compute the schema once).
pub fn build_record_batch_with_schema(
    schema: SchemaRef,
    columns: &[ColumnMetadata],
    rows: &[Vec<Option<QueryValue>>],
) -> Result<RecordBatch> {
    for row in rows {
        if row.len() != columns.len() {
            return Err(ArrowConversionError::InvalidValue {
                column_name: String::new(),
                reason: format!(
                    "row has {} values but {} columns were described",
                    row.len(),
                    columns.len()
                ),
            });
        }
    }
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for (index, field) in schema.fields().iter().enumerate() {
        let column = &columns[index];
        let cells = rows.iter().map(move |row| row[index].as_ref());
        arrays.push(build_column_array(
            field.data_type(),
            column,
            cells,
            rows.len(),
        )?);
    }
    RecordBatch::try_new(schema, arrays).map_err(ArrowConversionError::from)
}

fn invalid_value(column: &ColumnMetadata, reason: impl Into<String>) -> ArrowConversionError {
    ArrowConversionError::InvalidValue {
        column_name: column.name.clone(),
        reason: reason.into(),
    }
}

/// Text form of a numeric value (NUMBER text or BINARY_DOUBLE/FLOAT repr).
fn numeric_text<'a>(column: &ColumnMetadata, value: &'a QueryValue) -> Result<&'a str> {
    match value {
        QueryValue::Number { text, .. } => Ok(text),
        QueryValue::BinaryDouble(text) => Ok(text),
        _ => Err(invalid_value(column, "expected a numeric value")),
    }
}

/// Mirrors C `strtoll` as used by converters.pyx:432-516: parses the leading
/// integer part and ignores a trailing fraction ("1.5" -> 1). Unlike strtoll,
/// a value without any leading digits is an error (fail-closed).
fn parse_number_i64(text: &str) -> Option<i64> {
    let (negative, rest) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let digits_len = rest.bytes().take_while(|b| b.is_ascii_digit()).count();
    if digits_len == 0 {
        return None;
    }
    let mut value: i64 = 0;
    for byte in rest[..digits_len].bytes() {
        value = value.checked_mul(10)?.checked_add(i64::from(byte - b'0'))?;
    }
    if negative {
        Some(-value)
    } else {
        Some(value)
    }
}

fn parse_number_u64(text: &str) -> Option<u64> {
    let rest = text.strip_prefix('+').unwrap_or(text);
    let digits_len = rest.bytes().take_while(|b| b.is_ascii_digit()).count();
    if digits_len == 0 {
        return None;
    }
    let mut value: u64 = 0;
    for byte in rest[..digits_len].bytes() {
        value = value.checked_mul(10)?.checked_add(u64::from(byte - b'0'))?;
    }
    Some(value)
}

/// Decimal digits of a NUMBER text as an unscaled i128 for a decimal128
/// column of the given scale (converters.pyx:231-280): the digit string
/// loses its decimal point and is right-padded with zeros up to the array
/// scale. Rejects values with more than 38 digits and the special
/// max-negative value (-1e126).
fn decimal128_from_number_text(text: &str, scale: i8) -> Option<i128> {
    if text.contains(['e', 'E']) {
        return None; // covers the -1e126 max-negative marker
    }
    let (negative, rest) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        _ => (false, text),
    };
    let (int_part, frac_part) = match rest.split_once('.') {
        Some((i, f)) => (i, f),
        None => (rest, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let scale_digits = usize::try_from(scale.max(0)).ok()?;
    if frac_part.len() > scale_digits {
        return None;
    }
    let mut digits = String::with_capacity(int_part.len() + scale_digits);
    digits.push_str(int_part);
    digits.push_str(frac_part);
    for _ in frac_part.len()..scale_digits {
        digits.push('0');
    }
    let trimmed = digits.trim_start_matches('0');
    if trimmed.len() > 38 {
        return None;
    }
    let mut value: i128 = 0;
    for byte in digits.bytes() {
        value = value
            .checked_mul(10)?
            .checked_add(i128::from(byte - b'0'))?;
    }
    Some(if negative { -value } else { value })
}

/// Days between civil date and the Unix epoch (Howard Hinnant's algorithm).
fn days_from_civil(year: i32, month: u8, day: u8) -> i64 {
    let year = i64::from(year) - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month = i64::from(month);
    let day = i64::from(day);
    let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

struct EpochParts {
    seconds: i64,
    nanos: u32,
}

fn epoch_parts(column: &ColumnMetadata, value: &QueryValue) -> Result<EpochParts> {
    let QueryValue::DateTime {
        year,
        month,
        day,
        hour,
        minute,
        second,
        nanosecond,
    } = value
    else {
        return Err(invalid_value(column, "expected a datetime value"));
    };
    let days = days_from_civil(*year, *month, *day);
    let seconds =
        days * 86_400 + i64::from(*hour) * 3_600 + i64::from(*minute) * 60 + i64::from(*second);
    Ok(EpochParts {
        seconds,
        nanos: *nanosecond,
    })
}

fn timestamp_epoch_value(parts: &EpochParts, unit: TimeUnit) -> Result<i64> {
    let overflow = || ArrowConversionError::InvalidValue {
        column_name: String::new(),
        reason: "timestamp out of range for the requested unit".to_string(),
    };
    match unit {
        // reference truncates sub-second parts for second resolution
        TimeUnit::Second => Ok(parts.seconds),
        TimeUnit::Millisecond => parts
            .seconds
            .checked_mul(1_000)
            .and_then(|v| v.checked_add(i64::from(parts.nanos) / 1_000_000))
            .ok_or_else(overflow),
        TimeUnit::Microsecond => parts
            .seconds
            .checked_mul(1_000_000)
            .and_then(|v| v.checked_add(i64::from(parts.nanos) / 1_000))
            .ok_or_else(overflow),
        TimeUnit::Nanosecond => parts
            .seconds
            .checked_mul(1_000_000_000)
            .and_then(|v| v.checked_add(i64::from(parts.nanos)))
            .ok_or_else(overflow),
    }
}

macro_rules! build_int_column {
    ($builder:ty, $target:ty, $column:expr, $cells:expr, $capacity:expr) => {{
        let mut builder = <$builder>::with_capacity($capacity);
        for cell in $cells {
            match cell {
                None => builder.append_null(),
                Some(value) => {
                    let text = numeric_text($column, value)?;
                    let wide = parse_number_i64(text).ok_or_else(|| {
                        ArrowConversionError::CannotConvertToInteger {
                            value: text.to_string(),
                        }
                    })?;
                    let narrowed = <$target>::try_from(wide).map_err(|_| {
                        ArrowConversionError::CannotConvertToInteger {
                            value: text.to_string(),
                        }
                    })?;
                    builder.append_value(narrowed);
                }
            }
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }};
}

macro_rules! build_uint_column {
    ($builder:ty, $target:ty, $column:expr, $cells:expr, $capacity:expr) => {{
        let mut builder = <$builder>::with_capacity($capacity);
        for cell in $cells {
            match cell {
                None => builder.append_null(),
                Some(value) => {
                    let text = numeric_text($column, value)?;
                    let wide = parse_number_u64(text).ok_or_else(|| {
                        ArrowConversionError::CannotConvertToInteger {
                            value: text.to_string(),
                        }
                    })?;
                    let narrowed = <$target>::try_from(wide).map_err(|_| {
                        ArrowConversionError::CannotConvertToInteger {
                            value: text.to_string(),
                        }
                    })?;
                    builder.append_value(narrowed);
                }
            }
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }};
}

fn build_timestamp_column<'a, T: ArrowTimestampType>(
    column: &ColumnMetadata,
    cells: impl Iterator<Item = Option<&'a QueryValue>>,
    capacity: usize,
    unit: TimeUnit,
) -> Result<ArrayRef> {
    let mut values: Vec<Option<i64>> = Vec::with_capacity(capacity);
    for cell in cells {
        match cell {
            None => values.push(None),
            Some(value) => {
                let parts = epoch_parts(column, value)?;
                let epoch = timestamp_epoch_value(&parts, unit).map_err(|_| {
                    invalid_value(column, "timestamp out of range for the requested unit")
                })?;
                values.push(Some(epoch));
            }
        }
    }
    Ok(Arc::new(PrimitiveArray::<T>::from_iter(values)) as ArrayRef)
}

fn build_column_array<'a>(
    data_type: &DataType,
    column: &ColumnMetadata,
    cells: impl Iterator<Item = Option<&'a QueryValue>>,
    capacity: usize,
) -> Result<ArrayRef> {
    match data_type {
        DataType::Int8 => build_int_column!(Int8Builder, i8, column, cells, capacity),
        DataType::Int16 => build_int_column!(Int16Builder, i16, column, cells, capacity),
        DataType::Int32 => build_int_column!(Int32Builder, i32, column, cells, capacity),
        DataType::Int64 => build_int_column!(Int64Builder, i64, column, cells, capacity),
        DataType::UInt8 => build_uint_column!(UInt8Builder, u8, column, cells, capacity),
        DataType::UInt16 => build_uint_column!(UInt16Builder, u16, column, cells, capacity),
        DataType::UInt32 => build_uint_column!(UInt32Builder, u32, column, cells, capacity),
        DataType::UInt64 => build_uint_column!(UInt64Builder, u64, column, cells, capacity),
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(capacity);
            for cell in cells {
                match cell {
                    None => builder.append_null(),
                    Some(value) => {
                        let text = numeric_text(column, value)?;
                        let parsed = text.parse::<f64>().map_err(|_| {
                            ArrowConversionError::CannotConvertToDouble {
                                value: text.to_string(),
                            }
                        })?;
                        builder.append_value(parsed);
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float32 => {
            let mut builder = Float32Builder::with_capacity(capacity);
            for cell in cells {
                match cell {
                    None => builder.append_null(),
                    Some(value) => {
                        let text = numeric_text(column, value)?;
                        let parsed = text.parse::<f32>().map_err(|_| {
                            ArrowConversionError::CannotConvertToFloat {
                                value: text.to_string(),
                            }
                        })?;
                        builder.append_value(parsed);
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Decimal128(precision, scale) => {
            let mut builder = Decimal128Builder::with_capacity(capacity)
                .with_precision_and_scale(*precision, *scale)?;
            for cell in cells {
                match cell {
                    None => builder.append_null(),
                    Some(value) => {
                        let text = numeric_text(column, value)?;
                        let unscaled =
                            decimal128_from_number_text(text, *scale).ok_or_else(|| {
                                ArrowConversionError::DecimalOutOfRange {
                                    value: text.to_string(),
                                }
                            })?;
                        builder.append_value(unscaled);
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(capacity);
            for cell in cells {
                match cell {
                    None => builder.append_null(),
                    Some(value) => {
                        // BOOLEAN columns surface as NUMBER 0/1 in QueryValue
                        let text = numeric_text(column, value)?;
                        builder.append_value(text != "0");
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for cell in cells {
                match cell {
                    None => builder.append_null(),
                    Some(QueryValue::Text(text)) | Some(QueryValue::Rowid(text)) => {
                        builder.append_value(text)
                    }
                    Some(_) => return Err(invalid_value(column, "expected a text value")),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::LargeUtf8 => {
            let mut builder = LargeStringBuilder::new();
            for cell in cells {
                match cell {
                    None => builder.append_null(),
                    Some(QueryValue::Text(text)) | Some(QueryValue::Rowid(text)) => {
                        builder.append_value(text)
                    }
                    Some(_) => return Err(invalid_value(column, "expected a text value")),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::new();
            for cell in cells {
                match cell {
                    None => builder.append_null(),
                    Some(QueryValue::Raw(bytes)) => builder.append_value(bytes),
                    Some(_) => return Err(invalid_value(column, "expected a raw value")),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::LargeBinary => {
            let mut builder = LargeBinaryBuilder::new();
            for cell in cells {
                match cell {
                    None => builder.append_null(),
                    Some(QueryValue::Raw(bytes)) => builder.append_value(bytes),
                    Some(_) => return Err(invalid_value(column, "expected a raw value")),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::FixedSizeBinary(size) => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(capacity, *size);
            for cell in cells {
                match cell {
                    None => builder.append_null(),
                    Some(QueryValue::Raw(bytes)) => {
                        builder.append_value(bytes)?;
                    }
                    Some(_) => return Err(invalid_value(column, "expected a raw value")),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Timestamp(TimeUnit::Second, None) => {
            build_timestamp_column::<TimestampSecondType>(column, cells, capacity, TimeUnit::Second)
        }
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            build_timestamp_column::<TimestampMillisecondType>(
                column,
                cells,
                capacity,
                TimeUnit::Millisecond,
            )
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            build_timestamp_column::<TimestampMicrosecondType>(
                column,
                cells,
                capacity,
                TimeUnit::Microsecond,
            )
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            build_timestamp_column::<TimestampNanosecondType>(
                column,
                cells,
                capacity,
                TimeUnit::Nanosecond,
            )
        }
        DataType::Date32 => {
            let mut builder = Date32Builder::with_capacity(capacity);
            for cell in cells {
                match cell {
                    None => builder.append_null(),
                    Some(value) => {
                        let parts = epoch_parts(column, value)?;
                        // floor division matches python timedelta.days
                        let days = parts.seconds.div_euclid(86_400);
                        let days = i32::try_from(days)
                            .map_err(|_| invalid_value(column, "date out of range for date32"))?;
                        builder.append_value(days);
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Date64 => {
            let mut builder = Date64Builder::with_capacity(capacity);
            for cell in cells {
                match cell {
                    None => builder.append_null(),
                    Some(value) => {
                        let parts = epoch_parts(column, value)?;
                        let millis = timestamp_epoch_value(&parts, TimeUnit::Millisecond)
                            .map_err(|_| invalid_value(column, "date out of range for date64"))?;
                        builder.append_value(millis);
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        other => Err(ArrowConversionError::CannotConvertToArrow {
            arrow_type: arrow_type_name(other),
            db_type: db_type_name(column),
        }),
    }
}

/// Reference conversion matrix for the ingestion direction
/// (metadata.pyx `check_convert_from_arrow`, DPY-3039).
pub fn check_convert_from_arrow(arrow_type: &DataType, column: &ColumnMetadata) -> Result<()> {
    let ok = match arrow_type {
        DataType::Null => true,
        DataType::Binary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary => matches!(
            column.ora_type_num,
            ORA_TYPE_NUM_RAW | ORA_TYPE_NUM_LONG_RAW
        ),
        DataType::Boolean => column.ora_type_num == ORA_TYPE_NUM_BOOLEAN,
        DataType::Decimal128(_, _)
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => column.ora_type_num == ORA_TYPE_NUM_NUMBER,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None) => {
            is_datetime_like(column)
        }
        DataType::Float32 | DataType::Float64 => matches!(
            column.ora_type_num,
            ORA_TYPE_NUM_BINARY_DOUBLE | ORA_TYPE_NUM_BINARY_FLOAT | ORA_TYPE_NUM_NUMBER
        ),
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => is_char_like(column),
        _ => false,
    };
    if ok {
        Ok(())
    } else {
        Err(ArrowConversionError::CannotConvertFromArrow {
            arrow_type: arrow_type_name(arrow_type),
            db_type: db_type_name(column),
        })
    }
}

/// Converts a [`RecordBatch`] into direct path load rows
/// (`convert_arrow_to_oracle_data`, converters.pyx:32-148).
///
/// `column_metadata` is the server metadata from the direct path prepare
/// response; each Arrow column must be convertible to its database column
/// (DPY-3039 otherwise). Notable reference semantics that are mirrored:
/// zero-length strings/binary become NULL; floats/doubles bound for NUMBER
/// columns go through shortest-roundtrip decimal text; LONG_NVARCHAR targets
/// are re-encoded as UTF-16BE.
pub fn record_batch_to_direct_path_rows(
    batch: &RecordBatch,
    column_metadata: &[ColumnMetadata],
) -> Result<Vec<Vec<DirectPathColumnValue>>> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{
        Date32Type, Date64Type, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type,
        Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    };

    if batch.num_columns() != column_metadata.len() {
        return Err(ArrowConversionError::InvalidValue {
            column_name: String::new(),
            reason: format!(
                "record batch has {} columns but the table has {}",
                batch.num_columns(),
                column_metadata.len()
            ),
        });
    }
    let num_rows = batch.num_rows();
    let mut rows: Vec<Vec<DirectPathColumnValue>> = (0..num_rows)
        .map(|_| Vec::with_capacity(column_metadata.len()))
        .collect();

    for (array, column) in batch.columns().iter().zip(column_metadata) {
        check_convert_from_arrow(array.data_type(), column)?;
        let number_target = column.ora_type_num == ORA_TYPE_NUM_NUMBER;
        let utf16_target =
            column.ora_type_num == ORA_TYPE_NUM_LONG && column.csfrm == CS_FORM_NCHAR;
        for (row_index, row) in rows.iter_mut().enumerate() {
            if array.is_null(row_index) {
                row.push(DirectPathColumnValue::Null);
                continue;
            }
            let value = match array.data_type() {
                DataType::Null => DirectPathColumnValue::Null,
                DataType::Int8 => DirectPathColumnValue::Number(
                    array
                        .as_primitive::<Int8Type>()
                        .value(row_index)
                        .to_string(),
                ),
                DataType::Int16 => DirectPathColumnValue::Number(
                    array
                        .as_primitive::<Int16Type>()
                        .value(row_index)
                        .to_string(),
                ),
                DataType::Int32 => DirectPathColumnValue::Number(
                    array
                        .as_primitive::<Int32Type>()
                        .value(row_index)
                        .to_string(),
                ),
                DataType::Int64 => DirectPathColumnValue::Number(
                    array
                        .as_primitive::<Int64Type>()
                        .value(row_index)
                        .to_string(),
                ),
                DataType::UInt8 => DirectPathColumnValue::Number(
                    array
                        .as_primitive::<UInt8Type>()
                        .value(row_index)
                        .to_string(),
                ),
                DataType::UInt16 => DirectPathColumnValue::Number(
                    array
                        .as_primitive::<UInt16Type>()
                        .value(row_index)
                        .to_string(),
                ),
                DataType::UInt32 => DirectPathColumnValue::Number(
                    array
                        .as_primitive::<UInt32Type>()
                        .value(row_index)
                        .to_string(),
                ),
                DataType::UInt64 => DirectPathColumnValue::Number(
                    array
                        .as_primitive::<UInt64Type>()
                        .value(row_index)
                        .to_string(),
                ),
                DataType::Decimal128(_, _) => {
                    let value = array
                        .as_primitive::<Decimal128Type>()
                        .value_as_string(row_index);
                    DirectPathColumnValue::Number(value)
                }
                DataType::Float64 => {
                    let value = array.as_primitive::<Float64Type>().value(row_index);
                    if number_target {
                        DirectPathColumnValue::Number(float_to_number_text(value))
                    } else if column.ora_type_num == ORA_TYPE_NUM_BINARY_FLOAT {
                        DirectPathColumnValue::BinaryFloat(value as f32)
                    } else {
                        DirectPathColumnValue::BinaryDouble(value)
                    }
                }
                DataType::Float32 => {
                    let value = array.as_primitive::<Float32Type>().value(row_index);
                    if number_target {
                        DirectPathColumnValue::Number(float_to_number_text(f64::from(value)))
                    } else if column.ora_type_num == ORA_TYPE_NUM_BINARY_DOUBLE {
                        DirectPathColumnValue::BinaryDouble(f64::from(value))
                    } else {
                        DirectPathColumnValue::BinaryFloat(value)
                    }
                }
                DataType::Boolean => {
                    DirectPathColumnValue::Boolean(array.as_boolean().value(row_index))
                }
                DataType::Utf8 => string_direct_path_value(
                    array.as_string::<i32>().value(row_index),
                    utf16_target,
                ),
                DataType::LargeUtf8 => string_direct_path_value(
                    array.as_string::<i64>().value(row_index),
                    utf16_target,
                ),
                DataType::Utf8View => {
                    string_direct_path_value(array.as_string_view().value(row_index), utf16_target)
                }
                DataType::Binary => {
                    bytes_direct_path_value(array.as_binary::<i32>().value(row_index))
                }
                DataType::LargeBinary => {
                    bytes_direct_path_value(array.as_binary::<i64>().value(row_index))
                }
                DataType::BinaryView => {
                    bytes_direct_path_value(array.as_binary_view().value(row_index))
                }
                DataType::FixedSizeBinary(_) => {
                    bytes_direct_path_value(array.as_fixed_size_binary().value(row_index))
                }
                DataType::Date32 => {
                    let days = array.as_primitive::<Date32Type>().value(row_index);
                    datetime_from_epoch(i64::from(days) * 86_400, 0)?
                }
                DataType::Date64 => {
                    let millis = array.as_primitive::<Date64Type>().value(row_index);
                    datetime_from_epoch(
                        millis.div_euclid(1_000),
                        u32::try_from(millis.rem_euclid(1_000)).unwrap_or(0) * 1_000_000,
                    )?
                }
                DataType::Timestamp(unit, None) => {
                    let raw = timestamp_raw_value(array, row_index)?;
                    let (seconds, nanos) = match unit {
                        TimeUnit::Second => (raw, 0u32),
                        TimeUnit::Millisecond => (
                            raw.div_euclid(1_000),
                            u32::try_from(raw.rem_euclid(1_000)).unwrap_or(0) * 1_000_000,
                        ),
                        TimeUnit::Microsecond => (
                            raw.div_euclid(1_000_000),
                            u32::try_from(raw.rem_euclid(1_000_000)).unwrap_or(0) * 1_000,
                        ),
                        TimeUnit::Nanosecond => (
                            raw.div_euclid(1_000_000_000),
                            u32::try_from(raw.rem_euclid(1_000_000_000)).unwrap_or(0),
                        ),
                    };
                    datetime_from_epoch(seconds, nanos)?
                }
                other => {
                    return Err(ArrowConversionError::CannotConvertFromArrow {
                        arrow_type: arrow_type_name(other),
                        db_type: db_type_name(column),
                    })
                }
            };
            row.push(value);
        }
    }
    Ok(rows)
}

/// Shortest round-trip decimal text for a float bound to a NUMBER column
/// (reference uses Python `str(float)`; Rust's `Display` is also shortest
/// round-trip).
fn float_to_number_text(value: f64) -> String {
    format!("{value}")
}

fn string_direct_path_value(text: &str, utf16_target: bool) -> DirectPathColumnValue {
    // zero-length strings are NULL in Oracle semantics (converters.pyx:108-110)
    if text.is_empty() {
        return DirectPathColumnValue::Null;
    }
    if utf16_target {
        let mut bytes = Vec::with_capacity(text.len() * 2);
        for unit in text.encode_utf16() {
            bytes.extend_from_slice(&unit.to_be_bytes());
        }
        DirectPathColumnValue::Bytes(bytes)
    } else {
        DirectPathColumnValue::Bytes(text.as_bytes().to_vec())
    }
}

fn bytes_direct_path_value(bytes: &[u8]) -> DirectPathColumnValue {
    if bytes.is_empty() {
        DirectPathColumnValue::Null
    } else {
        DirectPathColumnValue::Bytes(bytes.to_vec())
    }
}

fn timestamp_raw_value(array: &ArrayRef, row_index: usize) -> Result<i64> {
    use arrow_array::cast::AsArray;
    let value = match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => {
            array.as_primitive::<TimestampSecondType>().value(row_index)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => array
            .as_primitive::<TimestampMillisecondType>()
            .value(row_index),
        DataType::Timestamp(TimeUnit::Microsecond, _) => array
            .as_primitive::<TimestampMicrosecondType>()
            .value(row_index),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => array
            .as_primitive::<TimestampNanosecondType>()
            .value(row_index),
        other => {
            return Err(ArrowConversionError::CannotConvertFromArrow {
                arrow_type: arrow_type_name(other),
                db_type: "DB_TYPE_TIMESTAMP".to_string(),
            })
        }
    };
    Ok(value)
}

fn civil_from_days(days: i64) -> (i32, u8, u8) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    (year as i32, month as u8, day as u8)
}

fn datetime_from_epoch(seconds: i64, nanos: u32) -> Result<DirectPathColumnValue> {
    let days = seconds.div_euclid(86_400);
    let seconds_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    Ok(DirectPathColumnValue::DateTime {
        year,
        month,
        day,
        hour: (seconds_of_day / 3_600) as u8,
        minute: ((seconds_of_day % 3_600) / 60) as u8,
        second: (seconds_of_day % 60) as u8,
        nanosecond: nanos,
    })
}

/// Incremental record-batch fetch over an open cursor: the asupersync
/// equivalent of `fetch_df_batches`. Obtain via
/// [`crate::Connection::fetch_record_batches`], then pull batches with
/// [`RecordBatchFetch::next_batch`].
///
/// Mirrors impl/base/cursor.pyx:590-609: the first batch is whatever the
/// execute round trip prefetched (possibly zero rows — an empty result still
/// yields exactly one zero-length batch), then one batch per fetch round trip
/// of `batch_size` rows.
#[derive(Debug)]
pub struct RecordBatchFetch {
    schema: SchemaRef,
    columns: Vec<ColumnMetadata>,
    cursor_id: u32,
    batch_size: u32,
    pending: Option<Vec<Vec<Option<QueryValue>>>>,
    last_row: Option<Vec<Option<QueryValue>>>,
    more_rows: bool,
}

impl RecordBatchFetch {
    /// Schema shared by every batch this fetch yields.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Fetches the next record batch; `None` once the result set is drained.
    pub async fn next_batch(
        &mut self,
        cx: &asupersync::Cx,
        connection: &mut crate::Connection,
    ) -> crate::Result<Option<RecordBatch>> {
        if let Some(rows) = self.pending.take() {
            self.last_row = rows.last().cloned();
            let batch = build_record_batch_with_schema(self.schema.clone(), &self.columns, &rows)?;
            return Ok(Some(batch));
        }
        while self.more_rows {
            let result = connection
                .fetch_rows_with_columns(
                    cx,
                    self.cursor_id,
                    self.batch_size,
                    &self.columns,
                    self.last_row.as_deref(),
                )
                .await?;
            self.more_rows = result.more_rows;
            if result.rows.is_empty() {
                continue;
            }
            self.last_row = result.rows.last().cloned();
            let batch =
                build_record_batch_with_schema(self.schema.clone(), &self.columns, &result.rows)?;
            return Ok(Some(batch));
        }
        Ok(None)
    }
}

fn require_result_set(columns: &[ColumnMetadata]) -> Result<()> {
    if columns.is_empty() {
        return Err(ArrowConversionError::InvalidValue {
            column_name: String::new(),
            reason: "statement did not return a result set".to_string(),
        });
    }
    Ok(())
}

impl crate::Connection {
    /// Executes `sql` and returns the full result as a single
    /// [`RecordBatch`] (the `fetch_df_all` shape). `fetch_array_size` tunes
    /// the prefetch/fetch round trips, exactly like `arraysize`.
    pub async fn fetch_all_record_batch(
        &mut self,
        cx: &asupersync::Cx,
        sql: &str,
        fetch_array_size: u32,
        options: &ArrowFetchOptions,
    ) -> crate::Result<RecordBatch> {
        let size = fetch_array_size.max(1);
        let mut result = self.execute_query(cx, sql, size).await?;
        require_result_set(&result.columns)?;
        let columns = std::mem::take(&mut result.columns);
        let cursor_id = result.cursor_id;
        let mut rows = std::mem::take(&mut result.rows);
        let mut more_rows = result.more_rows;
        while more_rows {
            let previous = rows.last().cloned();
            let fetched = self
                .fetch_rows_with_columns(cx, cursor_id, size, &columns, previous.as_deref())
                .await?;
            more_rows = fetched.more_rows;
            rows.extend(fetched.rows);
        }
        Ok(build_record_batch(&columns, &rows, options)?)
    }

    /// Executes `sql` and returns an incremental batch fetch yielding
    /// [`RecordBatch`]es of (at most) `batch_size` rows each (the
    /// `fetch_df_batches` shape).
    pub async fn fetch_record_batches(
        &mut self,
        cx: &asupersync::Cx,
        sql: &str,
        batch_size: u32,
        options: &ArrowFetchOptions,
    ) -> crate::Result<RecordBatchFetch> {
        let size = batch_size.max(1);
        let result = self.execute_query(cx, sql, size).await?;
        require_result_set(&result.columns)?;
        let schema = Arc::new(arrow_schema_for_columns(&result.columns, options)?);
        Ok(RecordBatchFetch {
            schema,
            columns: result.columns,
            cursor_id: result.cursor_id,
            batch_size: size,
            pending: Some(result.rows),
            last_row: None,
            more_rows: result.more_rows,
        })
    }
}

impl crate::BlockingConnection {
    pub fn fetch_all_record_batch(
        connection: &mut crate::Connection,
        sql: &str,
        fetch_array_size: u32,
        options: &ArrowFetchOptions,
    ) -> crate::Result<RecordBatch> {
        crate::block_on_connection(move |cx| async move {
            connection
                .fetch_all_record_batch(&cx, sql, fetch_array_size, options)
                .await
        })
    }

    pub fn fetch_record_batches(
        connection: &mut crate::Connection,
        sql: &str,
        batch_size: u32,
        options: &ArrowFetchOptions,
    ) -> crate::Result<RecordBatchFetch> {
        crate::block_on_connection(move |cx| async move {
            connection
                .fetch_record_batches(&cx, sql, batch_size, options)
                .await
        })
    }

    pub fn next_record_batch(
        connection: &mut crate::Connection,
        fetch: &mut RecordBatchFetch,
    ) -> crate::Result<Option<RecordBatch>> {
        crate::block_on_connection(move |cx| async move { fetch.next_batch(&cx, connection).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{
        Date32Type, Decimal128Type, Float32Type, Float64Type, Int64Type, TimestampMicrosecondType,
        TimestampSecondType, UInt16Type,
    };

    fn column(name: &str, ora_type_num: u8, precision: i8, scale: i8) -> ColumnMetadata {
        ColumnMetadata {
            name: name.to_string(),
            ora_type_num,
            csfrm: 0,
            precision,
            scale,
            buffer_size: 0,
            max_size: 0,
            nulls_allowed: true,
            is_json: false,
            is_oson: false,
            object_schema: None,
            object_type_name: None,
            is_array: false,
        }
    }

    fn number(text: &str) -> Option<QueryValue> {
        Some(QueryValue::Number {
            text: text.to_string(),
            is_integer: !text.contains('.'),
        })
    }

    fn datetime(
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        nanosecond: u32,
    ) -> Option<QueryValue> {
        Some(QueryValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
        })
    }

    #[test]
    fn number_with_small_precision_and_zero_scale_maps_to_int64() {
        let columns = vec![column("ID", ORA_TYPE_NUM_NUMBER, 9, 0)];
        let rows = vec![vec![number("1")], vec![None], vec![number("-42")]];
        let batch =
            build_record_batch(&columns, &rows, &ArrowFetchOptions::default()).expect("batch");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        let array = batch.column(0).as_primitive::<Int64Type>();
        assert_eq!(array.value(0), 1);
        assert!(array.is_null(1));
        assert_eq!(array.value(2), -42);
    }

    #[test]
    fn number_with_wide_precision_maps_to_float64() {
        // number(19) exceeds the 18-digit int64 rule (reference capture: BIG)
        let columns = vec![column("BIG", ORA_TYPE_NUM_NUMBER, 19, 0)];
        let rows = vec![vec![number("12345678901234")]];
        let batch =
            build_record_batch(&columns, &rows, &ArrowFetchOptions::default()).expect("batch");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Float64);
        assert_eq!(
            batch.column(0).as_primitive::<Float64Type>().value(0),
            12345678901234.0
        );
    }

    #[test]
    fn unconstrained_number_maps_to_float64() {
        let columns = vec![column("ANYNUM", ORA_TYPE_NUM_NUMBER, 0, -127)];
        let rows = vec![vec![number("1.5")], vec![number("-0.25")]];
        let batch =
            build_record_batch(&columns, &rows, &ArrowFetchOptions::default()).expect("batch");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Float64);
        let array = batch.column(0).as_primitive::<Float64Type>();
        assert_eq!(array.value(0), 1.5);
        assert_eq!(array.value(1), -0.25);
    }

    #[test]
    fn max_negative_number_converts_to_minus_1e126() {
        let columns = vec![column("N", ORA_TYPE_NUM_NUMBER, 0, -127)];
        let rows = vec![vec![number("-1e126")]];
        let batch =
            build_record_batch(&columns, &rows, &ArrowFetchOptions::default()).expect("batch");
        assert_eq!(
            batch.column(0).as_primitive::<Float64Type>().value(0),
            -1.0e126
        );
    }

    #[test]
    fn fetch_decimals_maps_constrained_number_to_decimal128() {
        let options = ArrowFetchOptions {
            fetch_decimals: true,
            ..ArrowFetchOptions::default()
        };
        let columns = vec![
            column("ID", ORA_TYPE_NUM_NUMBER, 9, 0),
            column("PRICE", ORA_TYPE_NUM_NUMBER, 9, 2),
            column("ANYNUM", ORA_TYPE_NUM_NUMBER, 0, -127),
        ];
        let rows = vec![
            vec![number("1"), number("12.34"), number("1.5")],
            vec![number("3"), number("-99.99"), None],
            vec![number("7"), number("5"), number("-0.25")],
        ];
        let batch = build_record_batch(&columns, &rows, &options).expect("batch");
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Decimal128(9, 0)
        );
        assert_eq!(
            batch.schema().field(1).data_type(),
            &DataType::Decimal128(9, 2)
        );
        // fetch_decimals with precision 0 stays double (test_8018)
        assert_eq!(batch.schema().field(2).data_type(), &DataType::Float64);
        let price = batch.column(1).as_primitive::<Decimal128Type>();
        assert_eq!(price.value(0), 1234);
        assert_eq!(price.value(1), -9999);
        assert_eq!(price.value(2), 500, "scale padding adds trailing zeros");
    }

    #[test]
    fn decimal128_rejects_max_negative_and_overflow() {
        assert_eq!(decimal128_from_number_text("-1e126", 0), None);
        assert_eq!(
            decimal128_from_number_text("1234567890123456789012345678901234567890", 0),
            None,
            ">38 digits must be rejected"
        );
        assert_eq!(decimal128_from_number_text("12.34", 2), Some(1234));
        assert_eq!(decimal128_from_number_text("-0.01", 2), Some(-1));
        assert_eq!(decimal128_from_number_text("5", 2), Some(500));
    }

    #[test]
    fn varchar_long_and_rowid_map_to_large_utf8() {
        let columns = vec![
            column("NAME", ORA_TYPE_NUM_VARCHAR, 0, 0),
            column("FIXED", ORA_TYPE_NUM_CHAR, 0, 0),
            column("WIDE", ORA_TYPE_NUM_LONG, 0, 0),
        ];
        let rows = vec![
            vec![
                Some(QueryValue::Text("alpha".into())),
                Some(QueryValue::Text("ab   ".into())),
                Some(QueryValue::Text("long text".into())),
            ],
            vec![None, None, None],
        ];
        let batch =
            build_record_batch(&columns, &rows, &ArrowFetchOptions::default()).expect("batch");
        for index in 0..3 {
            assert_eq!(
                batch.schema().field(index).data_type(),
                &DataType::LargeUtf8
            );
            assert!(batch.column(index).is_null(1));
        }
        assert_eq!(batch.column(0).as_string::<i64>().value(0), "alpha");
        assert_eq!(batch.column(1).as_string::<i64>().value(0), "ab   ");
    }

    #[test]
    fn raw_maps_to_large_binary() {
        let columns = vec![column("PAYLOAD", ORA_TYPE_NUM_RAW, 0, 0)];
        let rows = vec![vec![Some(QueryValue::Raw(vec![1, 2, 3]))], vec![None]];
        let batch =
            build_record_batch(&columns, &rows, &ArrowFetchOptions::default()).expect("batch");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::LargeBinary);
        assert_eq!(batch.column(0).as_binary::<i64>().value(0), &[1, 2, 3]);
        assert!(batch.column(0).is_null(1));
    }

    #[test]
    fn binary_float_and_double_map_to_float32_and_float64() {
        let columns = vec![
            column("SCORE", ORA_TYPE_NUM_BINARY_FLOAT, 0, 0),
            column("RATING", ORA_TYPE_NUM_BINARY_DOUBLE, 0, 0),
        ];
        let rows = vec![vec![
            Some(QueryValue::BinaryDouble("0.5".into())),
            Some(QueryValue::BinaryDouble("-1.5".into())),
        ]];
        let batch =
            build_record_batch(&columns, &rows, &ArrowFetchOptions::default()).expect("batch");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Float32);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Float64);
        assert_eq!(batch.column(0).as_primitive::<Float32Type>().value(0), 0.5);
        assert_eq!(batch.column(1).as_primitive::<Float64Type>().value(0), -1.5);
    }

    #[test]
    fn boolean_column_maps_from_number_zero_one() {
        let columns = vec![column("FLAG", 252, 0, 0)];
        let rows = vec![vec![number("1")], vec![number("0")], vec![None]];
        let batch =
            build_record_batch(&columns, &rows, &ArrowFetchOptions::default()).expect("batch");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Boolean);
        let array = batch.column(0).as_boolean();
        assert!(array.value(0));
        assert!(!array.value(1));
        assert!(array.is_null(2));
    }

    #[test]
    fn date_maps_to_timestamp_seconds_and_timestamp6_to_microseconds() {
        let columns = vec![
            column("HIRED", ORA_TYPE_NUM_DATE, 0, 0),
            column("UPDATED", ORA_TYPE_NUM_TIMESTAMP, 0, 6),
        ];
        let rows = vec![
            vec![
                datetime(2024, 1, 2, 3, 4, 5, 0),
                datetime(2024, 1, 2, 3, 4, 5, 123_456_000),
            ],
            vec![
                datetime(1969, 12, 31, 23, 59, 59, 0),
                datetime(1988, 12, 31, 23, 59, 58, 999_999_000),
            ],
        ];
        let batch =
            build_record_batch(&columns, &rows, &ArrowFetchOptions::default()).expect("batch");
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Second, None)
        );
        assert_eq!(
            batch.schema().field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        let hired = batch.column(0).as_primitive::<TimestampSecondType>();
        assert_eq!(hired.value(0), 1_704_164_645); // 2024-01-02T03:04:05Z
        assert_eq!(hired.value(1), -1); // one second before the epoch
        let updated = batch.column(1).as_primitive::<TimestampMicrosecondType>();
        assert_eq!(updated.value(0), 1_704_164_645_123_456);
        assert_eq!(updated.value(1), 599_615_998_999_999);
    }

    #[test]
    fn timestamp_scale_9_maps_to_nanoseconds() {
        let columns = vec![column("TS9", ORA_TYPE_NUM_TIMESTAMP, 0, 9)];
        let rows = vec![vec![datetime(2024, 1, 2, 3, 4, 5, 123_456_789)]];
        let batch =
            build_record_batch(&columns, &rows, &ArrowFetchOptions::default()).expect("batch");
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<TimestampNanosecondType>()
                .value(0),
            1_704_164_645_123_456_789
        );
    }

    #[test]
    fn null_only_column_keeps_described_type() {
        // `select null from dual` describes as VARCHAR2 -> large_string nulls
        let columns = vec![column("N", ORA_TYPE_NUM_VARCHAR, 0, 0)];
        let rows = vec![vec![None], vec![None]];
        let batch =
            build_record_batch(&columns, &rows, &ArrowFetchOptions::default()).expect("batch");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::LargeUtf8);
        assert_eq!(batch.column(0).null_count(), 2);
    }

    #[test]
    fn empty_result_produces_zero_length_arrays() {
        let columns = vec![
            column("A", ORA_TYPE_NUM_NUMBER, 9, 0),
            column("B", ORA_TYPE_NUM_VARCHAR, 0, 0),
        ];
        let batch =
            build_record_batch(&columns, &[], &ArrowFetchOptions::default()).expect("batch");
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn unsupported_types_raise_dpy_3030() {
        for (name, ora_type) in [("CUR", 102u8), ("OBJ", 109), ("J", 119), ("IYM", 182)] {
            let columns = vec![column(name, ora_type, 0, 0)];
            let err = build_record_batch(&columns, &[], &ArrowFetchOptions::default())
                .expect_err("unsupported type must error");
            assert!(
                err.to_string().starts_with("DPY-3030:"),
                "unexpected error for {name}: {err}"
            );
        }
    }

    #[test]
    fn vector_columns_error_until_describe_captures_format() {
        let columns = vec![column("V", 127, 0, 0)];
        let err = build_record_batch(&columns, &[], &ArrowFetchOptions::default())
            .expect_err("vector must error for now");
        assert!(matches!(err, ArrowConversionError::NotImplemented(_)));
    }

    #[test]
    fn vector_arrow_type_mapping_matches_reference() {
        assert_eq!(
            vector_arrow_type(VectorFormat::Float32, false),
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true)))
        );
        assert_eq!(
            vector_arrow_type(VectorFormat::Binary, false),
            DataType::List(Arc::new(Field::new("item", DataType::UInt8, true)))
        );
        let DataType::Struct(fields) = vector_arrow_type(VectorFormat::Float64, true) else {
            panic!("sparse vector must map to a struct");
        };
        assert_eq!(fields[0].name(), "num_dimensions");
        assert_eq!(fields[1].name(), "indices");
        assert_eq!(fields[2].name(), "values");
    }

    #[test]
    fn requested_schema_renames_and_coerces_columns() {
        let requested = Arc::new(Schema::new(vec![
            Field::new("INT_COL", DataType::Int16, true),
            Field::new("STR_COL", DataType::Utf8, true),
            Field::new("DAY", DataType::Date32, true),
        ]));
        let options = ArrowFetchOptions {
            requested_schema: Some(requested),
            ..ArrowFetchOptions::default()
        };
        let columns = vec![
            column("N", ORA_TYPE_NUM_NUMBER, 9, 0),
            column("S", ORA_TYPE_NUM_VARCHAR, 0, 0),
            column("D", ORA_TYPE_NUM_DATE, 0, 0),
        ];
        let rows = vec![vec![
            number("123"),
            Some(QueryValue::Text("x".into())),
            datetime(2024, 1, 2, 3, 4, 5, 0),
        ]];
        let batch = build_record_batch(&columns, &rows, &options).expect("batch");
        assert_eq!(batch.schema().field(0).name(), "INT_COL");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int16);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Utf8);
        assert_eq!(batch.column(1).as_string::<i32>().value(0), "x");
        // date32: time of day is floored away
        assert_eq!(
            batch.column(2).as_primitive::<Date32Type>().value(0),
            19_724
        );
    }

    #[test]
    fn requested_schema_uint_and_truncation_semantics() {
        let requested = Arc::new(Schema::new(vec![Field::new("U", DataType::UInt16, true)]));
        let options = ArrowFetchOptions {
            requested_schema: Some(requested),
            ..ArrowFetchOptions::default()
        };
        let columns = vec![column("N", ORA_TYPE_NUM_NUMBER, 0, -127)];
        // strtoll semantics: "1.9" truncates to 1
        let rows = vec![vec![number("1.9")], vec![number("65535")]];
        let batch = build_record_batch(&columns, &rows, &options).expect("batch");
        let array = batch.column(0).as_primitive::<UInt16Type>();
        assert_eq!(array.value(0), 1);
        assert_eq!(array.value(1), 65_535);
    }

    #[test]
    fn requested_schema_out_of_range_integer_errors() {
        let requested = Arc::new(Schema::new(vec![Field::new("I", DataType::Int8, true)]));
        let options = ArrowFetchOptions {
            requested_schema: Some(requested),
            ..ArrowFetchOptions::default()
        };
        let columns = vec![column("N", ORA_TYPE_NUM_NUMBER, 9, 0)];
        let rows = vec![vec![number("300")]];
        let err = build_record_batch(&columns, &rows, &options).expect_err("must overflow");
        assert!(err.to_string().starts_with("DPY-4036:"), "{err}");
    }

    #[test]
    fn requested_schema_length_mismatch_raises_dpy_2069() {
        let requested = Arc::new(Schema::new(vec![Field::new("A", DataType::Int64, true)]));
        let options = ArrowFetchOptions {
            requested_schema: Some(requested),
            ..ArrowFetchOptions::default()
        };
        let columns = vec![
            column("A", ORA_TYPE_NUM_NUMBER, 9, 0),
            column("B", ORA_TYPE_NUM_NUMBER, 9, 0),
        ];
        let err = build_record_batch(&columns, &[], &options).expect_err("length mismatch");
        assert!(err.to_string().starts_with("DPY-2069:"), "{err}");
        assert!(err.to_string().contains("1 columns defined but 2"), "{err}");
    }

    #[test]
    fn requested_schema_bad_pairing_raises_dpy_3038() {
        let requested = Arc::new(Schema::new(vec![Field::new("S", DataType::Utf8, true)]));
        let options = ArrowFetchOptions {
            requested_schema: Some(requested),
            ..ArrowFetchOptions::default()
        };
        let columns = vec![column("N", ORA_TYPE_NUM_NUMBER, 9, 0)];
        let err = build_record_batch(&columns, &[], &options).expect_err("bad pairing");
        assert!(err.to_string().starts_with("DPY-3038:"), "{err}");
        assert!(
            err.to_string().contains("DB_TYPE_NUMBER") && err.to_string().contains("\"string\""),
            "{err}"
        );
    }

    #[test]
    fn requested_schema_timestamp_unit_overrides_column_scale() {
        let requested = Arc::new(Schema::new(vec![Field::new(
            "TS",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));
        let options = ArrowFetchOptions {
            requested_schema: Some(requested),
            ..ArrowFetchOptions::default()
        };
        let columns = vec![column("D", ORA_TYPE_NUM_DATE, 0, 0)];
        let rows = vec![vec![datetime(2024, 1, 2, 3, 4, 5, 0)]];
        let batch = build_record_batch(&columns, &rows, &options).expect("batch");
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<TimestampNanosecondType>()
                .value(0),
            1_704_164_645_000_000_000
        );
    }

    #[test]
    fn arrow_define_columns_inline_lobs() {
        let columns = vec![
            column("DOC", ORA_TYPE_NUM_CLOB, 0, 0),
            column("BIN", ORA_TYPE_NUM_BLOB, 0, 0),
            column("KEEP", ORA_TYPE_NUM_VARCHAR, 0, 0),
        ];
        let defined = arrow_define_columns(&columns);
        assert_eq!(defined[0].ora_type_num, ORA_TYPE_NUM_LONG);
        assert_eq!(defined[1].ora_type_num, ORA_TYPE_NUM_LONG_RAW);
        assert_eq!(defined[2].ora_type_num, ORA_TYPE_NUM_VARCHAR);
    }

    #[test]
    fn lob_values_are_rejected_with_clear_error() {
        let columns = vec![column("DOC", ORA_TYPE_NUM_CLOB, 0, 0)];
        let err = build_record_batch(&columns, &[], &ArrowFetchOptions::default())
            .expect_err("CLOB without define coercion must error");
        assert!(err.to_string().starts_with("DPY-3030:"), "{err}");
    }

    #[test]
    fn record_batch_round_trips_to_direct_path_rows() {
        let columns = vec![
            column("ID", ORA_TYPE_NUM_NUMBER, 9, 0),
            column("NAME", ORA_TYPE_NUM_VARCHAR, 0, 0),
            column("RATING", ORA_TYPE_NUM_BINARY_DOUBLE, 0, 0),
            column("HIRED", ORA_TYPE_NUM_DATE, 0, 0),
        ];
        let rows = vec![
            vec![
                number("1"),
                Some(QueryValue::Text("alpha".into())),
                Some(QueryValue::BinaryDouble("2.5".into())),
                datetime(2024, 1, 2, 3, 4, 5, 0),
            ],
            vec![number("2"), None, None, None],
        ];
        // build an arrow batch via the fetch path, then convert it back into
        // direct path rows
        let batch =
            build_record_batch(&columns, &rows, &ArrowFetchOptions::default()).expect("batch");
        let dpl_rows = record_batch_to_direct_path_rows(&batch, &columns).expect("dpl rows");
        assert_eq!(dpl_rows.len(), 2);
        assert_eq!(dpl_rows[0][0], DirectPathColumnValue::Number("1".into()));
        assert_eq!(
            dpl_rows[0][1],
            DirectPathColumnValue::Bytes(b"alpha".to_vec())
        );
        assert_eq!(dpl_rows[0][2], DirectPathColumnValue::BinaryDouble(2.5));
        assert_eq!(
            dpl_rows[0][3],
            DirectPathColumnValue::DateTime {
                year: 2024,
                month: 1,
                day: 2,
                hour: 3,
                minute: 4,
                second: 5,
                nanosecond: 0,
            }
        );
        assert_eq!(dpl_rows[1][1], DirectPathColumnValue::Null);
        assert_eq!(dpl_rows[1][3], DirectPathColumnValue::Null);
    }

    #[test]
    fn empty_strings_ingest_as_null() {
        use arrow_array::StringArray;
        let schema = Arc::new(Schema::new(vec![Field::new("NAME", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![Some(""), Some("x")])) as ArrayRef],
        )
        .expect("batch");
        let columns = vec![column("NAME", ORA_TYPE_NUM_VARCHAR, 0, 0)];
        let rows = record_batch_to_direct_path_rows(&batch, &columns).expect("rows");
        assert_eq!(rows[0][0], DirectPathColumnValue::Null);
        assert_eq!(rows[1][0], DirectPathColumnValue::Bytes(b"x".to_vec()));
    }

    #[test]
    fn ingestion_bad_pairing_raises_dpy_3039() {
        use arrow_array::Int64Array;
        let schema = Arc::new(Schema::new(vec![Field::new("N", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1i64])) as ArrayRef],
        )
        .expect("batch");
        let columns = vec![column("NAME", ORA_TYPE_NUM_VARCHAR, 0, 0)];
        let err = record_batch_to_direct_path_rows(&batch, &columns).expect_err("bad pairing");
        assert!(err.to_string().starts_with("DPY-3039:"), "{err}");
    }

    #[test]
    fn ingestion_floats_to_number_use_shortest_repr() {
        use arrow_array::Float64Array;
        let schema = Arc::new(Schema::new(vec![Field::new("N", DataType::Float64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(vec![0.1f64, 2.0])) as ArrayRef],
        )
        .expect("batch");
        let columns = vec![column("N", ORA_TYPE_NUM_NUMBER, 0, -127)];
        let rows = record_batch_to_direct_path_rows(&batch, &columns).expect("rows");
        assert_eq!(rows[0][0], DirectPathColumnValue::Number("0.1".into()));
        assert_eq!(rows[1][0], DirectPathColumnValue::Number("2".into()));
    }

    #[test]
    fn ingestion_sliced_arrays_respect_offsets() {
        use arrow_array::Int64Array;
        let schema = Arc::new(Schema::new(vec![Field::new("N", DataType::Int64, true)]));
        let full = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![10i64, 20, 30, 40])) as ArrayRef],
        )
        .expect("batch");
        let sliced = full.slice(1, 2);
        let columns = vec![column("N", ORA_TYPE_NUM_NUMBER, 0, -127)];
        let rows = record_batch_to_direct_path_rows(&sliced, &columns).expect("rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], DirectPathColumnValue::Number("20".into()));
        assert_eq!(rows[1][0], DirectPathColumnValue::Number("30".into()));
    }

    #[test]
    fn ingestion_timestamp_units_convert_to_datetime() {
        use arrow_array::TimestampMicrosecondArray;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "TS",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampMicrosecondArray::from(vec![
                1_704_164_645_123_456i64,
            ])) as ArrayRef],
        )
        .expect("batch");
        let columns = vec![column("TS", ORA_TYPE_NUM_TIMESTAMP, 0, 6)];
        let rows = record_batch_to_direct_path_rows(&batch, &columns).expect("rows");
        assert_eq!(
            rows[0][0],
            DirectPathColumnValue::DateTime {
                year: 2024,
                month: 1,
                day: 2,
                hour: 3,
                minute: 4,
                second: 5,
                nanosecond: 123_456_000,
            }
        );
    }
}
