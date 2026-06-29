use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Date64Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, Int8Builder, LargeBinaryBuilder, LargeStringBuilder, ListBuilder, StringBuilder,
    UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow_array::types::{
    ArrowTimestampType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use arrow_array::{ArrayRef, PrimitiveArray, RecordBatch, StructArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};

use oracledb_protocol::thin::{BorrowedRowBatch, ColumnMetadata, QueryValue, QueryValueRef};
use oracledb_protocol::vector::{Vector, VectorValues};

use super::{
    arrow_schema_for_columns, arrow_type_name, db_type_name, ArrowConversionError,
    ArrowFetchOptions, Result,
};

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
        column_name: column.name().to_string(),
        reason: reason.into(),
    }
}

/// Text form of a numeric value (NUMBER text or BINARY_DOUBLE/FLOAT repr).
/// NUMBER synthesizes its canonical text on demand from the inline form via the
/// shared formatter, so this returns a `Cow` (owned for NUMBER, borrowed for the
/// already-text BINARY_DOUBLE / BOOLEAN cases).
fn numeric_text<'a>(
    column: &ColumnMetadata,
    value: &'a QueryValue,
) -> Result<std::borrow::Cow<'a, str>> {
    use std::borrow::Cow;
    match value {
        QueryValue::Number(num) => Ok(Cow::Owned(num.to_canonical_string())),
        QueryValue::BinaryDouble(text) => Ok(Cow::Borrowed(text)),
        // A native DB_TYPE_BOOLEAN is materialized into an arrow numeric column
        // as 1/0 (it has no dedicated arrow boolean column type here).
        QueryValue::Boolean(value) => Ok(Cow::Borrowed(if *value { "1" } else { "0" })),
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
/// Formats a Decimal128 (unscaled `i128` + `scale`) as a decimal string, the
/// inverse of `decimal128_from_number_text`. Used when converting an Arrow
/// decimal cell back to a `decimal.Decimal` for the bind path.
pub fn decimal128_to_string(unscaled: i128, scale: i8) -> String {
    if scale <= 0 {
        // No fractional digits; trailing zeros for negative scale.
        let mut text = unscaled.to_string();
        for _ in 0..(-scale as i64) {
            text.push('0');
        }
        return text;
    }
    let scale = scale as usize;
    let negative = unscaled < 0;
    let digits = unscaled.unsigned_abs().to_string();
    let text = if digits.len() <= scale {
        let zeros = "0".repeat(scale - digits.len());
        format!("0.{zeros}{digits}")
    } else {
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    if negative {
        format!("-{text}")
    } else {
        text
    }
}

pub(super) fn decimal128_from_number_text(text: &str, scale: i8) -> Option<i128> {
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

fn epoch_parts_from_components(
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    nanosecond: u32,
    offset_minutes: i32,
) -> EpochParts {
    let days = days_from_civil(year, month, day);
    let seconds = days * 86_400 + i64::from(hour) * 3_600 + i64::from(minute) * 60
        - i64::from(offset_minutes) * 60
        + i64::from(second);
    EpochParts {
        seconds,
        nanos: nanosecond,
    }
}

fn epoch_parts(column: &ColumnMetadata, value: &QueryValue) -> Result<EpochParts> {
    match value {
        QueryValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
        } => Ok(epoch_parts_from_components(
            *year,
            *month,
            *day,
            *hour,
            *minute,
            *second,
            *nanosecond,
            0,
        )),
        QueryValue::TimestampTz {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
            offset_minutes,
        } => Ok(epoch_parts_from_components(
            *year,
            *month,
            *day,
            *hour,
            *minute,
            *second,
            *nanosecond,
            *offset_minutes,
        )),
        _ => Err(invalid_value(column, "expected a datetime value")),
    }
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
    ($builder:ty, $target:ty, $arrow_type:expr, $column:expr, $cells:expr, $capacity:expr) => {{
        let mut builder = <$builder>::with_capacity($capacity);
        for cell in $cells {
            match cell {
                None => builder.append_null(),
                Some(value) => {
                    let text = numeric_text($column, value)?;
                    // A non-integer text (fractional / non-numeric) is DPY-4036;
                    // an in-range integer that overflows the narrower Arrow width
                    // is DPY-4038 (matching the reference distinction).
                    let wide = parse_number_i64(text.as_ref()).ok_or_else(|| {
                        ArrowConversionError::CannotConvertToInteger {
                            value: text.to_string(),
                        }
                    })?;
                    let narrowed = <$target>::try_from(wide).map_err(|_| {
                        ArrowConversionError::InvalidInteger {
                            value: text.to_string(),
                            arrow_type: $arrow_type.to_string(),
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
    ($builder:ty, $target:ty, $arrow_type:expr, $column:expr, $cells:expr, $capacity:expr) => {{
        let mut builder = <$builder>::with_capacity($capacity);
        for cell in $cells {
            match cell {
                None => builder.append_null(),
                Some(value) => {
                    let text = numeric_text($column, value)?;
                    // Parse as the widest unsigned, or fall back to a signed
                    // parse so a valid-but-negative integer surfaces as DPY-4038
                    // (out of range) rather than DPY-4036 (not an integer).
                    let invalid = || ArrowConversionError::InvalidInteger {
                        value: text.to_string(),
                        arrow_type: $arrow_type.to_string(),
                    };
                    let wide = match parse_number_u64(text.as_ref()) {
                        Some(wide) => wide,
                        None => {
                            // A valid integer that is simply out of the unsigned
                            // range is DPY-4038; anything else is DPY-4036.
                            if parse_number_i64(text.as_ref()).is_some() {
                                return Err(invalid());
                            }
                            return Err(ArrowConversionError::CannotConvertToInteger {
                                value: text.to_string(),
                            });
                        }
                    };
                    let narrowed = <$target>::try_from(wide).map_err(|_| invalid())?;
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
        DataType::Int8 => build_int_column!(Int8Builder, i8, "int8", column, cells, capacity),
        DataType::Int16 => build_int_column!(Int16Builder, i16, "int16", column, cells, capacity),
        DataType::Int32 => build_int_column!(Int32Builder, i32, "int32", column, cells, capacity),
        DataType::Int64 => build_int_column!(Int64Builder, i64, "int64", column, cells, capacity),
        DataType::UInt8 => build_uint_column!(UInt8Builder, u8, "uint8", column, cells, capacity),
        DataType::UInt16 => {
            build_uint_column!(UInt16Builder, u16, "uint16", column, cells, capacity)
        }
        DataType::UInt32 => {
            build_uint_column!(UInt32Builder, u32, "uint32", column, cells, capacity)
        }
        DataType::UInt64 => {
            build_uint_column!(UInt64Builder, u64, "uint64", column, cells, capacity)
        }
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
                        let unscaled = decimal128_from_number_text(text.as_ref(), *scale)
                            .ok_or_else(|| ArrowConversionError::DecimalOutOfRange {
                                value: text.to_string(),
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
                        builder.append_value(text.as_ref() != "0");
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
            let fixed_size_len = usize::try_from(*size).unwrap_or(0);
            let mut builder = FixedSizeBinaryBuilder::with_capacity(capacity, *size);
            for cell in cells {
                match cell {
                    None => builder.append_null(),
                    Some(QueryValue::Raw(bytes)) => {
                        // A byte length that doesn't match the fixed Arrow width
                        // is DPY-4040 (not a raw Arrow "Invalid argument" error).
                        if bytes.len() != fixed_size_len {
                            return Err(ArrowConversionError::FixedSizeBinaryViolated {
                                actual_len: bytes.len(),
                                fixed_size_len,
                            });
                        }
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
        // VECTOR columns are the only Oracle type mapping to List / Struct
        // (dense -> List<child>, sparse -> Struct{num_dimensions,indices,values}).
        DataType::List(item) => build_vector_list_column(item, column, cells, capacity),
        DataType::Struct(fields) => build_vector_struct_column(fields, column, cells, capacity),
        other => Err(ArrowConversionError::CannotConvertToArrow {
            arrow_type: arrow_type_name(other),
            db_type: db_type_name(column),
        }),
    }
}

/// Pushes one row of dense VECTOR element values into a typed `ListBuilder`
/// (converters.pyx `convert_vector_to_arrow` -> array.pyx `append_vector`).
/// The list's child arrow type is decided by `vector_data_type`; a values
/// variant that disagrees with it is a server inconsistency (DPY message via
/// `invalid_value`). For BINARY each stored byte is one UInt8 element (the
/// reference does not bit-unpack — test_9103 expects `[3,2,3]` from 3 bytes).
fn push_vector_values(
    column: &ColumnMetadata,
    builder: &mut VectorListBuilder,
    values: &VectorValues,
) -> Result<()> {
    match (builder, values) {
        (VectorListBuilder::Float32(b), VectorValues::Float32(v)) => {
            b.values().append_slice(v);
            b.append(true);
        }
        (VectorListBuilder::Float64(b), VectorValues::Float64(v)) => {
            b.values().append_slice(v);
            b.append(true);
        }
        (VectorListBuilder::Int8(b), VectorValues::Int8(v)) => {
            b.values().append_slice(v);
            b.append(true);
        }
        (VectorListBuilder::UInt8(b), VectorValues::Binary(v)) => {
            b.values().append_slice(v);
            b.append(true);
        }
        _ => return Err(invalid_value(column, "vector format mismatch")),
    }
    Ok(())
}

/// A `ListBuilder` specialized to the VECTOR element type, so dense lists and
/// the sparse `values` list share a single push path.
enum VectorListBuilder {
    Float32(ListBuilder<Float32Builder>),
    Float64(ListBuilder<Float64Builder>),
    Int8(ListBuilder<Int8Builder>),
    UInt8(ListBuilder<UInt8Builder>),
}

impl VectorListBuilder {
    /// Builder for the child arrow type carried by a vector `List` field.
    fn for_item(item: &DataType) -> Result<Self> {
        Ok(match item {
            DataType::Float32 => {
                VectorListBuilder::Float32(ListBuilder::new(Float32Builder::new()))
            }
            DataType::Float64 => {
                VectorListBuilder::Float64(ListBuilder::new(Float64Builder::new()))
            }
            DataType::Int8 => VectorListBuilder::Int8(ListBuilder::new(Int8Builder::new())),
            DataType::UInt8 => VectorListBuilder::UInt8(ListBuilder::new(UInt8Builder::new())),
            _ => {
                return Err(ArrowConversionError::NotImplemented(
                    "unsupported vector list element type",
                ))
            }
        })
    }

    fn append_null(&mut self) {
        match self {
            VectorListBuilder::Float32(b) => b.append(false),
            VectorListBuilder::Float64(b) => b.append(false),
            VectorListBuilder::Int8(b) => b.append(false),
            VectorListBuilder::UInt8(b) => b.append(false),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            VectorListBuilder::Float32(b) => Arc::new(b.finish()),
            VectorListBuilder::Float64(b) => Arc::new(b.finish()),
            VectorListBuilder::Int8(b) => Arc::new(b.finish()),
            VectorListBuilder::UInt8(b) => Arc::new(b.finish()),
        }
    }
}

/// Builds the Arrow `List<child>` array for a dense VECTOR column. A NULL cell
/// (or a SQL NULL vector) becomes a NULL list element.
fn build_vector_list_column<'a>(
    item: &Arc<Field>,
    column: &ColumnMetadata,
    cells: impl Iterator<Item = Option<&'a QueryValue>>,
    _capacity: usize,
) -> Result<ArrayRef> {
    let mut builder = VectorListBuilder::for_item(item.data_type())?;
    for cell in cells {
        match cell {
            None => builder.append_null(),
            Some(QueryValue::Vector(vector)) => match vector.as_ref() {
                Vector::Dense(values) => {
                    push_vector_values(column, &mut builder, values)?;
                }
                Vector::Sparse { .. } => {
                    // A sparse vector value reaching a dense `List` column means the
                    // column described with flexible dimensions (mixed num_dimensions
                    // across rows) so could not be typed as a fixed sparse struct.
                    // Reference: append_sparse_vector -> ERR_ARROW_SPARSE_VECTOR_NOT_ALLOWED.
                    return Err(ArrowConversionError::SparseVectorNotAllowed);
                }
            },
            Some(_) => return Err(invalid_value(column, "expected a vector value")),
        }
    }
    Ok(builder.finish())
}

/// Builds the Arrow `Struct{num_dimensions,indices,values}` array for a sparse
/// VECTOR column (converters.pyx `append_sparse_vector`). The three children
/// are built in lockstep and a NULL cell yields a NULL struct element with all
/// three children NULL at that row.
fn build_vector_struct_column<'a>(
    fields: &Fields,
    column: &ColumnMetadata,
    cells: impl Iterator<Item = Option<&'a QueryValue>>,
    _capacity: usize,
) -> Result<ArrayRef> {
    // The child arrow type of the `values` list (fields[2]) decides the element
    // builder; `vector_arrow_type` always lays the struct out as
    // [num_dimensions: Int64, indices: List<UInt32>, values: List<child>].
    let values_item = match fields[2].data_type() {
        DataType::List(item) => item.data_type().clone(),
        _ => return Err(invalid_value(column, "sparse vector values must be a list")),
    };

    let mut num_dimensions = Int64Builder::new();
    let mut indices = ListBuilder::new(UInt32Builder::new());
    let mut values = VectorListBuilder::for_item(&values_item)?;
    let mut validity: Vec<bool> = Vec::new();

    for cell in cells {
        match cell {
            None => {
                num_dimensions.append_null();
                indices.append(false);
                values.append_null();
                validity.push(false);
            }
            Some(QueryValue::Vector(vector)) => match vector.as_ref() {
                Vector::Sparse {
                    num_dimensions: dims,
                    indices: idx,
                    values: vals,
                } => {
                    num_dimensions.append_value(i64::from(*dims));
                    indices.values().append_slice(idx);
                    indices.append(true);
                    push_vector_values(column, &mut values, vals)?;
                    validity.push(true);
                }
                Vector::Dense(_) => {
                    return Err(invalid_value(
                        column,
                        "expected a sparse vector but received a dense vector",
                    ));
                }
            },
            Some(_) => return Err(invalid_value(column, "expected a vector value")),
        }
    }

    let children: Vec<ArrayRef> = vec![
        Arc::new(num_dimensions.finish()) as ArrayRef,
        Arc::new(indices.finish()) as ArrayRef,
        values.finish(),
    ];
    let nulls = NullBuffer::from(validity);
    let array = StructArray::try_new(fields.clone(), children, Some(nulls))?;
    Ok(Arc::new(array) as ArrayRef)
}

// ===========================================================================
// COLUMNAR fetch->Arrow (bead rust-oracledb-wf7): decode the borrowed fetch
// batch DIRECTLY into per-column Arrow builders (transpose-during-parse),
// skipping the `Vec<Vec<Option<QueryValue>>>` row materialization AND the
// `build_record_batch` transpose pass.
//
// The wire is row-major; this path streams each borrowed row's cells straight
// into the matching column builder, so:
//   * no per-row `Vec<Option<QueryValue>>` is ever allocated,
//   * VARCHAR2/CHAR/RAW cells borrow the wire buffer (zero copy) and are copied
//     once into the Arrow value buffer,
//   * NUMBER canonical text lands in the borrowed batch's amortized per-row
//     arena (no per-cell `String` malloc), then converts to int64/float64/
//     decimal128 with the SAME helpers the row path uses,
//   * NULLs go straight into the builder's NullBuffer.
//
// CORRECTNESS: every cell is appended through the SAME conversion helpers
// (`numeric_text_ref` -> `parse_number_i64`/`decimal128_from_number_text`/
// `parse::<f64>`, `epoch_parts_ref` -> `timestamp_epoch_value`, etc.) the
// row-major `build_column_array` uses, on byte-identical canonical text (the
// shared NUMBER formatter), so the produced RecordBatch is byte-identical to
// `build_record_batch`. The `arrow_columnar_equals_row_path` differential test
// asserts this cell-for-cell over a mixed-type many-row result.
// ===========================================================================

/// Canonical numeric text of a borrowed cell (mirror of [`numeric_text`] for the
/// borrowed path). NUMBER borrows the per-row arena; BINARY_DOUBLE / BOOLEAN
/// borrow their already-text form. The returned `&str` is valid for the cell's
/// lifetime (the borrowed batch owns the backing arena/buffer).
fn numeric_text_ref<'a>(
    column: &ColumnMetadata,
    value: &QueryValueRef<'a>,
) -> Result<std::borrow::Cow<'a, str>> {
    use std::borrow::Cow;
    match *value {
        QueryValueRef::Number { text, .. } => Ok(Cow::Borrowed(text)),
        QueryValueRef::Boolean(b) => Ok(Cow::Borrowed(if b { "1" } else { "0" })),
        QueryValueRef::Owned(owned) => numeric_text(column, owned),
        _ => Err(invalid_value(column, "expected a numeric value")),
    }
}

/// `EpochParts` for a borrowed datetime cell (mirror of [`epoch_parts`]).
fn epoch_parts_ref(column: &ColumnMetadata, value: &QueryValueRef<'_>) -> Result<EpochParts> {
    match *value {
        QueryValueRef::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
        } => Ok(epoch_parts_from_components(
            year, month, day, hour, minute, second, nanosecond, 0,
        )),
        QueryValueRef::TimestampTz {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
            offset_minutes,
        } => Ok(epoch_parts_from_components(
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
            offset_minutes,
        )),
        QueryValueRef::Owned(owned) => epoch_parts(column, owned),
        _ => Err(invalid_value(column, "expected a datetime value")),
    }
}

/// Borrow a text cell's `&str` (VARCHAR2/CHAR/LONG/ROWID) for the string
/// builders (mirror of the `Text`/`Rowid` arms in [`build_column_array`]). The
/// `Owned(Text/Rowid)` arm is the cold fallback (UTF-16 NCHAR, synthesized
/// ROWID): the `&String` lives in the batch's owned arena and is valid for the
/// cell lifetime `'a`.
fn text_ref<'a>(column: &ColumnMetadata, value: &QueryValueRef<'a>) -> Result<&'a str> {
    match *value {
        QueryValueRef::Text(text) => Ok(text),
        QueryValueRef::Owned(QueryValue::Text(text)) => Ok(text.as_str()),
        QueryValueRef::Owned(QueryValue::Rowid(text)) => Ok(text.as_str()),
        _ => Err(invalid_value(column, "expected a text value")),
    }
}

/// Borrow a raw cell's bytes (RAW/LONG_RAW) for the binary builders.
fn raw_ref<'a>(column: &ColumnMetadata, value: &QueryValueRef<'a>) -> Result<&'a [u8]> {
    match *value {
        QueryValueRef::Raw(bytes) => Ok(bytes),
        QueryValueRef::Owned(QueryValue::Raw(bytes)) => Ok(bytes.as_slice()),
        _ => Err(invalid_value(column, "expected a raw value")),
    }
}

/// One Arrow column builder, type-erased over the scalar Arrow types this
/// columnar path supports. Each variant appends one borrowed cell at a time
/// (`append_ref`), reusing the row path's conversion helpers so the output is
/// byte-identical. VECTOR (List/Struct) is intentionally NOT handled here (it
/// stays on the row-materialize path); see `build_record_batch_columnar`.
enum ColumnBuilder {
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt8(UInt8Builder),
    UInt16(UInt16Builder),
    UInt32(UInt32Builder),
    UInt64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    /// The builder plus its scale (arrow's `Decimal128Builder` does not expose
    /// the scale back out, and we need it for `decimal128_from_number_text`).
    Decimal128(Decimal128Builder, i8),
    Boolean(BooleanBuilder),
    Utf8(StringBuilder),
    LargeUtf8(LargeStringBuilder),
    Binary(BinaryBuilder),
    LargeBinary(LargeBinaryBuilder),
    FixedSizeBinary(FixedSizeBinaryBuilder, i32),
    TimestampSecond(Vec<Option<i64>>),
    TimestampMilli(Vec<Option<i64>>),
    TimestampMicro(Vec<Option<i64>>),
    TimestampNano(Vec<Option<i64>>),
    Date32(Date32Builder),
    Date64(Date64Builder),
}

impl ColumnBuilder {
    /// Build the column builder for `data_type`, preallocating `capacity` rows.
    /// Returns `None` for the List/Struct (VECTOR) types, which the columnar
    /// entry routes to the row-materialize fallback.
    fn new(data_type: &DataType, capacity: usize) -> Option<Self> {
        Some(match data_type {
            DataType::Int8 => ColumnBuilder::Int8(Int8Builder::with_capacity(capacity)),
            DataType::Int16 => ColumnBuilder::Int16(Int16Builder::with_capacity(capacity)),
            DataType::Int32 => ColumnBuilder::Int32(Int32Builder::with_capacity(capacity)),
            DataType::Int64 => ColumnBuilder::Int64(Int64Builder::with_capacity(capacity)),
            DataType::UInt8 => ColumnBuilder::UInt8(UInt8Builder::with_capacity(capacity)),
            DataType::UInt16 => ColumnBuilder::UInt16(UInt16Builder::with_capacity(capacity)),
            DataType::UInt32 => ColumnBuilder::UInt32(UInt32Builder::with_capacity(capacity)),
            DataType::UInt64 => ColumnBuilder::UInt64(UInt64Builder::with_capacity(capacity)),
            DataType::Float32 => ColumnBuilder::Float32(Float32Builder::with_capacity(capacity)),
            DataType::Float64 => ColumnBuilder::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Decimal128(precision, scale) => ColumnBuilder::Decimal128(
                Decimal128Builder::with_capacity(capacity)
                    .with_precision_and_scale(*precision, *scale)
                    .ok()?,
                *scale,
            ),
            DataType::Boolean => ColumnBuilder::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::Utf8 => ColumnBuilder::Utf8(StringBuilder::with_capacity(capacity, 0)),
            DataType::LargeUtf8 => {
                ColumnBuilder::LargeUtf8(LargeStringBuilder::with_capacity(capacity, 0))
            }
            DataType::Binary => ColumnBuilder::Binary(BinaryBuilder::with_capacity(capacity, 0)),
            DataType::LargeBinary => {
                ColumnBuilder::LargeBinary(LargeBinaryBuilder::with_capacity(capacity, 0))
            }
            DataType::FixedSizeBinary(size) => ColumnBuilder::FixedSizeBinary(
                FixedSizeBinaryBuilder::with_capacity(capacity, *size),
                *size,
            ),
            DataType::Timestamp(TimeUnit::Second, None) => {
                ColumnBuilder::TimestampSecond(Vec::with_capacity(capacity))
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                ColumnBuilder::TimestampMilli(Vec::with_capacity(capacity))
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                ColumnBuilder::TimestampMicro(Vec::with_capacity(capacity))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                ColumnBuilder::TimestampNano(Vec::with_capacity(capacity))
            }
            DataType::Date32 => ColumnBuilder::Date32(Date32Builder::with_capacity(capacity)),
            DataType::Date64 => ColumnBuilder::Date64(Date64Builder::with_capacity(capacity)),
            // List / Struct (VECTOR) and anything else: not columnar-handled.
            _ => return None,
        })
    }

    /// Append one borrowed cell, mirroring `build_column_array`'s per-cell
    /// conversion exactly. `None` is a SQL NULL.
    fn append_ref(
        &mut self,
        column: &ColumnMetadata,
        cell: Option<QueryValueRef<'_>>,
    ) -> Result<()> {
        macro_rules! int_arm {
            ($builder:expr, $target:ty, $arrow:literal) => {{
                match cell {
                    None => $builder.append_null(),
                    Some(value) => {
                        let text = numeric_text_ref(column, &value)?;
                        let wide = parse_number_i64(text.as_ref()).ok_or_else(|| {
                            ArrowConversionError::CannotConvertToInteger {
                                value: text.to_string(),
                            }
                        })?;
                        let narrowed = <$target>::try_from(wide).map_err(|_| {
                            ArrowConversionError::InvalidInteger {
                                value: text.to_string(),
                                arrow_type: $arrow.to_string(),
                            }
                        })?;
                        $builder.append_value(narrowed);
                    }
                }
            }};
        }
        macro_rules! uint_arm {
            ($builder:expr, $target:ty, $arrow:literal) => {{
                match cell {
                    None => $builder.append_null(),
                    Some(value) => {
                        let text = numeric_text_ref(column, &value)?;
                        let invalid = || ArrowConversionError::InvalidInteger {
                            value: text.to_string(),
                            arrow_type: $arrow.to_string(),
                        };
                        let wide = match parse_number_u64(text.as_ref()) {
                            Some(wide) => wide,
                            None => {
                                if parse_number_i64(text.as_ref()).is_some() {
                                    return Err(invalid());
                                }
                                return Err(ArrowConversionError::CannotConvertToInteger {
                                    value: text.to_string(),
                                });
                            }
                        };
                        let narrowed = <$target>::try_from(wide).map_err(|_| invalid())?;
                        $builder.append_value(narrowed);
                    }
                }
            }};
        }

        match self {
            ColumnBuilder::Int8(b) => int_arm!(b, i8, "int8"),
            ColumnBuilder::Int16(b) => int_arm!(b, i16, "int16"),
            ColumnBuilder::Int32(b) => int_arm!(b, i32, "int32"),
            ColumnBuilder::Int64(b) => int_arm!(b, i64, "int64"),
            ColumnBuilder::UInt8(b) => uint_arm!(b, u8, "uint8"),
            ColumnBuilder::UInt16(b) => uint_arm!(b, u16, "uint16"),
            ColumnBuilder::UInt32(b) => uint_arm!(b, u32, "uint32"),
            ColumnBuilder::UInt64(b) => uint_arm!(b, u64, "uint64"),
            ColumnBuilder::Float64(b) => match cell {
                None => b.append_null(),
                Some(value) => {
                    let text = numeric_text_ref(column, &value)?;
                    let parsed = text.parse::<f64>().map_err(|_| {
                        ArrowConversionError::CannotConvertToDouble {
                            value: text.to_string(),
                        }
                    })?;
                    b.append_value(parsed);
                }
            },
            ColumnBuilder::Float32(b) => match cell {
                None => b.append_null(),
                Some(value) => {
                    let text = numeric_text_ref(column, &value)?;
                    let parsed = text.parse::<f32>().map_err(|_| {
                        ArrowConversionError::CannotConvertToFloat {
                            value: text.to_string(),
                        }
                    })?;
                    b.append_value(parsed);
                }
            },
            ColumnBuilder::Decimal128(b, scale) => {
                match cell {
                    None => b.append_null(),
                    Some(value) => {
                        // Build the unscaled i128 from the canonical NUMBER text with
                        // the SAME helper the row path uses, so the result is
                        // byte-identical. (The borrowed text is the canonical form
                        // straight from the shared formatter; no per-cell String.)
                        let text = numeric_text_ref(column, &value)?;
                        let unscaled = decimal128_from_number_text(text.as_ref(), *scale)
                            .ok_or_else(|| ArrowConversionError::DecimalOutOfRange {
                                value: text.to_string(),
                            })?;
                        b.append_value(unscaled);
                    }
                }
            }
            ColumnBuilder::Boolean(b) => match cell {
                None => b.append_null(),
                Some(value) => {
                    let text = numeric_text_ref(column, &value)?;
                    b.append_value(text.as_ref() != "0");
                }
            },
            ColumnBuilder::Utf8(b) => match cell {
                None => b.append_null(),
                Some(value) => b.append_value(text_ref(column, &value)?),
            },
            ColumnBuilder::LargeUtf8(b) => match cell {
                None => b.append_null(),
                Some(value) => b.append_value(text_ref(column, &value)?),
            },
            ColumnBuilder::Binary(b) => match cell {
                None => b.append_null(),
                Some(value) => b.append_value(raw_ref(column, &value)?),
            },
            ColumnBuilder::LargeBinary(b) => match cell {
                None => b.append_null(),
                Some(value) => b.append_value(raw_ref(column, &value)?),
            },
            ColumnBuilder::FixedSizeBinary(b, size) => match cell {
                None => b.append_null(),
                Some(value) => {
                    let bytes = raw_ref(column, &value)?;
                    let fixed = usize::try_from(*size).unwrap_or(0);
                    if bytes.len() != fixed {
                        return Err(ArrowConversionError::FixedSizeBinaryViolated {
                            actual_len: bytes.len(),
                            fixed_size_len: fixed,
                        });
                    }
                    b.append_value(bytes)?;
                }
            },
            ColumnBuilder::TimestampSecond(values) => {
                push_timestamp_ref(column, cell, values, TimeUnit::Second)?
            }
            ColumnBuilder::TimestampMilli(values) => {
                push_timestamp_ref(column, cell, values, TimeUnit::Millisecond)?
            }
            ColumnBuilder::TimestampMicro(values) => {
                push_timestamp_ref(column, cell, values, TimeUnit::Microsecond)?
            }
            ColumnBuilder::TimestampNano(values) => {
                push_timestamp_ref(column, cell, values, TimeUnit::Nanosecond)?
            }
            ColumnBuilder::Date32(b) => match cell {
                None => b.append_null(),
                Some(value) => {
                    let parts = epoch_parts_ref(column, &value)?;
                    let days = parts.seconds.div_euclid(86_400);
                    let days = i32::try_from(days)
                        .map_err(|_| invalid_value(column, "date out of range for date32"))?;
                    b.append_value(days);
                }
            },
            ColumnBuilder::Date64(b) => match cell {
                None => b.append_null(),
                Some(value) => {
                    let parts = epoch_parts_ref(column, &value)?;
                    let millis = timestamp_epoch_value(&parts, TimeUnit::Millisecond)
                        .map_err(|_| invalid_value(column, "date out of range for date64"))?;
                    b.append_value(millis);
                }
            },
        }
        Ok(())
    }

    /// Finalize this builder into an Arrow array.
    fn finish(self) -> ArrayRef {
        match self {
            ColumnBuilder::Int8(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Int16(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Int32(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Int64(mut b) => Arc::new(b.finish()),
            ColumnBuilder::UInt8(mut b) => Arc::new(b.finish()),
            ColumnBuilder::UInt16(mut b) => Arc::new(b.finish()),
            ColumnBuilder::UInt32(mut b) => Arc::new(b.finish()),
            ColumnBuilder::UInt64(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Float32(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Float64(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Decimal128(mut b, _) => Arc::new(b.finish()),
            ColumnBuilder::Boolean(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Utf8(mut b) => Arc::new(b.finish()),
            ColumnBuilder::LargeUtf8(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Binary(mut b) => Arc::new(b.finish()),
            ColumnBuilder::LargeBinary(mut b) => Arc::new(b.finish()),
            ColumnBuilder::FixedSizeBinary(mut b, _) => Arc::new(b.finish()),
            ColumnBuilder::TimestampSecond(values) => {
                Arc::new(PrimitiveArray::<TimestampSecondType>::from_iter(values))
            }
            ColumnBuilder::TimestampMilli(values) => Arc::new(PrimitiveArray::<
                TimestampMillisecondType,
            >::from_iter(values)),
            ColumnBuilder::TimestampMicro(values) => Arc::new(PrimitiveArray::<
                TimestampMicrosecondType,
            >::from_iter(values)),
            ColumnBuilder::TimestampNano(values) => {
                Arc::new(PrimitiveArray::<TimestampNanosecondType>::from_iter(values))
            }
            ColumnBuilder::Date32(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Date64(mut b) => Arc::new(b.finish()),
        }
    }
}

/// Push one borrowed datetime cell as the epoch value for a timestamp column
/// (mirror of `build_timestamp_column`'s per-cell body).
fn push_timestamp_ref(
    column: &ColumnMetadata,
    cell: Option<QueryValueRef<'_>>,
    values: &mut Vec<Option<i64>>,
    unit: TimeUnit,
) -> Result<()> {
    match cell {
        None => values.push(None),
        Some(value) => {
            let parts = epoch_parts_ref(column, &value)?;
            let epoch = timestamp_epoch_value(&parts, unit).map_err(|_| {
                invalid_value(column, "timestamp out of range for the requested unit")
            })?;
            values.push(Some(epoch));
        }
    }
    Ok(())
}

/// Whether the columnar path can handle every column of this schema. VECTOR
/// (List/Struct) columns fall back to the row-materialize path so the cold,
/// rarely-fetched vector types keep their fully-tested converter.
pub(super) fn columnar_supported(schema: &Schema) -> bool {
    schema
        .fields()
        .iter()
        .all(|f| !matches!(f.data_type(), DataType::List(_) | DataType::Struct(_)))
}

/// Accumulating columnar batch builder: holds one [`ColumnBuilder`] per column
/// and appends rows from multiple fetch pages (borrowed or owned) before a
/// single [`finish`](Self::finish) produces the [`RecordBatch`]. This is the
/// multi-page columnar entry the `Connection::fetch_all_record_batch_columnar`
/// driver feeds — every page streams straight into the builders, so no fetched
/// page is ever materialized into a `Vec<Vec<Option<QueryValue>>>`.
pub(super) struct ColumnarBatchBuilder {
    schema: SchemaRef,
    columns: Vec<ColumnMetadata>,
    builders: Vec<ColumnBuilder>,
}

impl ColumnarBatchBuilder {
    /// Create builders for `schema`/`columns`, preallocating `capacity` rows.
    /// Returns `None` if any column's Arrow type is not columnar-supported
    /// (VECTOR List/Struct) — the caller falls back to the row path.
    pub(super) fn new(
        schema: SchemaRef,
        columns: Vec<ColumnMetadata>,
        capacity: usize,
    ) -> Option<Self> {
        let mut builders = Vec::with_capacity(columns.len());
        for field in schema.fields() {
            builders.push(ColumnBuilder::new(field.data_type(), capacity)?);
        }
        Some(Self {
            schema,
            columns,
            builders,
        })
    }

    /// Append every row of a borrowed fetch page straight into the builders,
    /// returning the page's last row materialized as owned values (or `None` for
    /// an empty page) for the next page's duplicate-column seed. Capturing the
    /// last row owned costs one allocation per page — the same as the row path's
    /// `rows.last().cloned()` — and only happens once per page, not per row.
    pub(super) fn append_borrowed(
        &mut self,
        batch: &BorrowedRowBatch,
    ) -> Result<Option<Vec<Option<QueryValue>>>> {
        let columns = &self.columns;
        let builders = &mut self.builders;
        let last_index = batch.row_count().checked_sub(1);
        let mut row_index = 0usize;
        let mut last: Option<Vec<Option<QueryValue>>> = None;
        batch.for_each_row_ref(|row: &[Option<QueryValueRef<'_>>]| {
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
            for (index, cell) in row.iter().enumerate() {
                builders[index].append_ref(&columns[index], *cell)?;
            }
            // Snapshot ONLY the final row owned, once, for the next page's
            // duplicate-column seed (matches the row path's `rows.last().cloned()`
            // — one allocation per page, not per row).
            if Some(row_index) == last_index {
                last = Some(row.iter().map(|c| c.map(|v| v.to_owned_value())).collect());
            }
            row_index += 1;
            Ok::<(), ArrowConversionError>(())
        })?;
        Ok(last)
    }

    /// Append owned rows (the first execute page arrives owned) by wrapping each
    /// cell as a borrowed `QueryValueRef::Owned`, so the SAME `append_ref`
    /// conversion runs — no separate owned code path to keep in sync.
    pub(super) fn append_owned(&mut self, rows: &[Vec<Option<QueryValue>>]) -> Result<()> {
        for row in rows {
            if row.len() != self.columns.len() {
                return Err(ArrowConversionError::InvalidValue {
                    column_name: String::new(),
                    reason: format!(
                        "row has {} values but {} columns were described",
                        row.len(),
                        self.columns.len()
                    ),
                });
            }
            for (index, cell) in row.iter().enumerate() {
                let cell_ref = cell.as_ref().map(QueryValueRef::Owned);
                self.builders[index].append_ref(&self.columns[index], cell_ref)?;
            }
        }
        Ok(())
    }

    /// Finalize all builders into one [`RecordBatch`].
    pub(super) fn finish(self) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = self
            .builders
            .into_iter()
            .map(ColumnBuilder::finish)
            .collect();
        RecordBatch::try_new(self.schema, arrays).map_err(ArrowConversionError::from)
    }
}

/// Builds one [`RecordBatch`] DIRECTLY from a borrowed fetch batch, transposing
/// during parse into per-column Arrow builders (bead wf7). The produced batch is
/// byte-identical to `build_record_batch_with_schema` over the same rows, but
/// allocates only the Arrow value buffers (no per-row `Vec<Option<QueryValue>>`,
/// no transpose pass, no per-cell `String` for scalar cells). Each builder is
/// preallocated to the batch row count.
pub fn build_record_batch_columnar(
    schema: SchemaRef,
    columns: &[ColumnMetadata],
    batch: &BorrowedRowBatch,
) -> Result<RecordBatch> {
    let capacity = batch.row_count();
    let mut builder = ColumnarBatchBuilder::new(schema, columns.to_vec(), capacity).ok_or(
        ArrowConversionError::NotImplemented("columnar path does not support this column type"),
    )?;
    builder.append_borrowed(batch)?;
    builder.finish()
}
