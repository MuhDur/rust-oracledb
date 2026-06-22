use arrow_array::types::{
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, TimeUnit};

use oracledb_protocol::dpl::DirectPathColumnValue;
use oracledb_protocol::thin::{
    ColumnMetadata, CS_FORM_NCHAR, ORA_TYPE_NUM_BINARY_DOUBLE, ORA_TYPE_NUM_BINARY_FLOAT,
    ORA_TYPE_NUM_LONG, ORA_TYPE_NUM_NUMBER,
};

use super::{
    arrow_type_name, check_convert_from_arrow, db_type_name, ArrowConversionError, Result,
};

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
        let number_target = column.ora_type_num() == ORA_TYPE_NUM_NUMBER;
        let utf16_target =
            column.ora_type_num() == ORA_TYPE_NUM_LONG && column.csfrm() == CS_FORM_NCHAR;
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
                    } else if column.ora_type_num() == ORA_TYPE_NUM_BINARY_FLOAT {
                        DirectPathColumnValue::BinaryFloat(value as f32)
                    } else {
                        DirectPathColumnValue::BinaryDouble(value)
                    }
                }
                DataType::Float32 => {
                    let value = array.as_primitive::<Float32Type>().value(row_index);
                    if number_target {
                        DirectPathColumnValue::Number(float_to_number_text(f64::from(value)))
                    } else if column.ora_type_num() == ORA_TYPE_NUM_BINARY_DOUBLE {
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
