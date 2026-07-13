#![forbid(unsafe_code)]

use super::*;

pub(crate) fn encode_oracle_date(
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
) -> Result<[u8; ORA_TYPE_SIZE_DATE as usize]> {
    if !(1..=9999).contains(&year)
        || !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return Err(ProtocolError::TtcDecode("invalid DATE bind"));
    }
    let century = year / 100 + 100;
    let year_in_century = year % 100 + 100;
    Ok([
        u8::try_from(century).map_err(|_| ProtocolError::TtcDecode("invalid DATE century"))?,
        u8::try_from(year_in_century).map_err(|_| ProtocolError::TtcDecode("invalid DATE year"))?,
        month,
        day,
        hour + 1,
        minute + 1,
        second + 1,
    ])
}

pub(crate) fn encode_oracle_timestamp(
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    nanosecond: u32,
) -> Result<Vec<u8>> {
    if nanosecond > 999_999_999 {
        return Err(ProtocolError::TtcDecode("invalid TIMESTAMP fraction"));
    }
    let date = encode_oracle_date(year, month, day, hour, minute, second)?;
    if nanosecond == 0 {
        return Ok(date.to_vec());
    }
    let mut bytes = Vec::with_capacity(ORA_TYPE_SIZE_TIMESTAMP as usize);
    bytes.extend_from_slice(&date);
    bytes.extend_from_slice(&nanosecond.to_be_bytes());
    Ok(bytes)
}

pub(crate) fn encode_oracle_timestamp_tz(
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    nanosecond: u32,
) -> Result<Vec<u8>> {
    encode_oracle_timestamp_tz_with_offset(year, month, day, hour, minute, second, nanosecond, 0)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn encode_oracle_timestamp_tz_with_offset(
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    nanosecond: u32,
    offset_minutes: i32,
) -> Result<Vec<u8>> {
    if nanosecond > 999_999_999 {
        return Err(ProtocolError::TtcDecode(
            "invalid TIMESTAMP WITH TIME ZONE fraction",
        ));
    }
    if !valid_tz_offset_minutes(offset_minutes) {
        return Err(ProtocolError::TtcDecode(
            "invalid TIMESTAMP WITH TIME ZONE offset",
        ));
    }
    let offset_hours = offset_minutes / 60;
    let offset_minute_part = offset_minutes % 60;
    let encoded_hour = offset_hours + i32::from(TZ_HOUR_OFFSET);
    let encoded_minute = offset_minute_part + i32::from(TZ_MINUTE_OFFSET);
    let encoded_hour = u8::try_from(encoded_hour)
        .map_err(|_| ProtocolError::TtcDecode("invalid TIMESTAMP WITH TIME ZONE offset hour"))?;
    let encoded_minute = u8::try_from(encoded_minute)
        .map_err(|_| ProtocolError::TtcDecode("invalid TIMESTAMP WITH TIME ZONE offset minute"))?;
    let mut bytes = Vec::with_capacity(ORA_TYPE_SIZE_TIMESTAMP_TZ as usize);
    let date = encode_oracle_date(year, month, day, hour, minute, second)?;
    bytes.extend_from_slice(&date);
    bytes.extend_from_slice(&nanosecond.to_be_bytes());
    bytes.push(encoded_hour);
    bytes.push(encoded_minute);
    Ok(bytes)
}

pub fn decode_datetime_value(bytes: &[u8]) -> Result<QueryValue> {
    if !matches!(
        bytes.len(),
        len if len == ORA_TYPE_SIZE_DATE as usize
            || len == ORA_TYPE_SIZE_TIMESTAMP as usize
            || len == ORA_TYPE_SIZE_TIMESTAMP_TZ as usize
    ) {
        return Err(ProtocolError::TtcDecode("invalid DATE/TIMESTAMP length"));
    }
    let year = (i32::from(bytes[0]) - 100) * 100 + i32::from(bytes[1]) - 100;
    if !(1..=9999).contains(&year) {
        return Err(ProtocolError::TtcDecode("invalid DATE year"));
    }
    let month = bytes[2];
    if !(1..=12).contains(&month) {
        return Err(ProtocolError::TtcDecode("invalid DATE month"));
    }
    let day = bytes[3];
    if !(1..=31).contains(&day) {
        return Err(ProtocolError::TtcDecode("invalid DATE day"));
    }
    let hour = decode_offset_time_byte(bytes[4], 23, "hour")?;
    let minute = decode_offset_time_byte(bytes[5], 59, "minute")?;
    let second = decode_offset_time_byte(bytes[6], 59, "second")?;
    let nanosecond = if bytes.len() >= ORA_TYPE_SIZE_TIMESTAMP as usize {
        let value = u32::from_be_bytes(
            bytes[7..11]
                .try_into()
                .map_err(|_| ProtocolError::TtcDecode("invalid TIMESTAMP fraction"))?,
        );
        if value > 999_999_999 {
            return Err(ProtocolError::TtcDecode("invalid TIMESTAMP fraction"));
        }
        value
    } else {
        0
    };
    if bytes.len() == ORA_TYPE_SIZE_TIMESTAMP_TZ as usize {
        if bytes[11] == 0 || bytes[12] == 0 {
            return Err(ProtocolError::TtcDecode(
                "invalid TIMESTAMP WITH TIME ZONE offset",
            ));
        }
        if bytes[11] & TNS_HAS_REGION_ID != 0 {
            return Err(ProtocolError::UnsupportedFeature(
                "named TIMESTAMP WITH TIME ZONE region",
            ));
        }
        if !(1..(TZ_MINUTE_OFFSET * 2)).contains(&bytes[12]) {
            return Err(ProtocolError::TtcDecode(
                "invalid TIMESTAMP WITH TIME ZONE offset minute",
            ));
        }
        let offset_minutes = (i32::from(bytes[11]) - i32::from(TZ_HOUR_OFFSET)) * 60
            + i32::from(bytes[12])
            - i32::from(TZ_MINUTE_OFFSET);
        if !valid_tz_offset_minutes(offset_minutes) {
            return Err(ProtocolError::TtcDecode(
                "invalid TIMESTAMP WITH TIME ZONE offset",
            ));
        }
        return Ok(QueryValue::TimestampTz {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
            offset_minutes,
        });
    }
    Ok(QueryValue::DateTime {
        year,
        month,
        day,
        hour,
        minute,
        second,
        nanosecond,
    })
}

fn valid_tz_offset_minutes(offset_minutes: i32) -> bool {
    (-1439..=1439).contains(&offset_minutes)
}

fn decode_offset_time_byte(byte: u8, max: u8, field: &'static str) -> Result<u8> {
    let Some(value) = byte.checked_sub(1) else {
        return Err(ProtocolError::TtcDecode(match field {
            "hour" => "invalid DATE hour",
            "minute" => "invalid DATE minute",
            _ => "invalid DATE second",
        }));
    };
    if value > max {
        return Err(ProtocolError::TtcDecode(match field {
            "hour" => "invalid DATE hour",
            "minute" => "invalid DATE minute",
            _ => "invalid DATE second",
        }));
    }
    Ok(value)
}

pub(crate) fn adjust_datetime_by_minutes(
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    offset_minutes: i32,
) -> Result<(i32, u8, u8, u8, u8, u8)> {
    let days = days_from_civil(year, month, day)?;
    let seconds_of_day = i64::from(hour) * 3_600 + i64::from(minute) * 60 + i64::from(second);
    let total_seconds = days
        .checked_mul(86_400)
        .and_then(|value| value.checked_add(seconds_of_day))
        .and_then(|value| value.checked_add(i64::from(offset_minutes) * 60))
        .ok_or(ProtocolError::TtcDecode(
            "TIMESTAMP WITH TIME ZONE offset overflow",
        ))?;
    let adjusted_days = total_seconds.div_euclid(86_400);
    let adjusted_seconds = total_seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(adjusted_days)?;
    let hour = u8::try_from(adjusted_seconds / 3_600)
        .map_err(|_| ProtocolError::TtcDecode("invalid adjusted TIMESTAMP hour"))?;
    let minute = u8::try_from((adjusted_seconds % 3_600) / 60)
        .map_err(|_| ProtocolError::TtcDecode("invalid adjusted TIMESTAMP minute"))?;
    let second = u8::try_from(adjusted_seconds % 60)
        .map_err(|_| ProtocolError::TtcDecode("invalid adjusted TIMESTAMP second"))?;
    Ok((year, month, day, hour, minute, second))
}

pub(crate) fn days_from_civil(year: i32, month: u8, day: u8) -> Result<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(ProtocolError::TtcDecode("invalid TIMESTAMP date"));
    }
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month = i32::from(month);
    let day = i32::from(day);
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    Ok(i64::from(era) * 146_097 + i64::from(day_of_era) - 719_468)
}

pub(crate) fn civil_from_days(days: i64) -> Result<(i32, u8, u8)> {
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
    Ok((
        i32::try_from(year)
            .map_err(|_| ProtocolError::TtcDecode("invalid adjusted TIMESTAMP year"))?,
        u8::try_from(month)
            .map_err(|_| ProtocolError::TtcDecode("invalid adjusted TIMESTAMP month"))?,
        u8::try_from(day)
            .map_err(|_| ProtocolError::TtcDecode("invalid adjusted TIMESTAMP day"))?,
    ))
}

pub(crate) fn encode_binary_double(value: f64) -> [u8; 8] {
    let mut bytes = value.to_bits().to_be_bytes();
    if bytes[0] & 0x80 == 0 {
        bytes[0] |= 0x80;
    } else {
        for byte in &mut bytes {
            *byte = !*byte;
        }
    }
    bytes
}

pub(crate) fn encode_binary_float(value: f32) -> [u8; 4] {
    let mut bytes = value.to_bits().to_be_bytes();
    if bytes[0] & 0x80 == 0 {
        bytes[0] |= 0x80;
    } else {
        for byte in &mut bytes {
            *byte = !*byte;
        }
    }
    bytes
}

pub(crate) fn decode_binary_float(bytes: &[u8]) -> Result<f32> {
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| ProtocolError::TtcDecode("invalid BINARY_FLOAT length"))?;
    let mut decoded = bytes;
    if decoded[0] & 0x80 != 0 {
        decoded[0] &= 0x7f;
    } else {
        for byte in &mut decoded {
            *byte = !*byte;
        }
    }
    Ok(f32::from_bits(u32::from_be_bytes(decoded)))
}

pub(crate) fn encode_interval_ds(days: i32, seconds: i32, nanoseconds: i32) -> Result<[u8; 11]> {
    let mut bytes = [0u8; 11];
    let wire_days = u32::try_from(i64::from(days) + TNS_DURATION_MID)
        .map_err(|_| ProtocolError::TtcDecode("INTERVAL DS days out of range"))?;
    bytes[..4].copy_from_slice(&wire_days.to_be_bytes());
    let to_offset_byte = |value: i32| -> Result<u8> {
        u8::try_from(value + TNS_DURATION_OFFSET)
            .map_err(|_| ProtocolError::TtcDecode("INTERVAL DS component out of range"))
    };
    bytes[4] = to_offset_byte(seconds / 3600)?;
    bytes[5] = to_offset_byte((seconds % 3600) / 60)?;
    bytes[6] = to_offset_byte(seconds % 60)?;
    let fseconds = i64::from(nanoseconds);
    let wire_fseconds = u32::try_from(fseconds + TNS_DURATION_MID)
        .map_err(|_| ProtocolError::TtcDecode("INTERVAL DS fractional seconds out of range"))?;
    bytes[7..].copy_from_slice(&wire_fseconds.to_be_bytes());
    Ok(bytes)
}

pub(crate) fn decode_interval_ds(bytes: &[u8]) -> Result<QueryValue> {
    if bytes.len() < 11 {
        return Err(ProtocolError::TtcDecode("invalid INTERVAL DS length"));
    }
    let days_wire = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    let fseconds_wire = u32::from_be_bytes([bytes[7], bytes[8], bytes[9], bytes[10]]);
    let to_component = |value: i64| -> Result<i32> {
        i32::try_from(value).map_err(|_| ProtocolError::TtcDecode("INTERVAL DS out of range"))
    };
    Ok(QueryValue::IntervalDS {
        days: to_component(i64::from(days_wire) - TNS_DURATION_MID)?,
        hours: i32::from(bytes[4]) - TNS_DURATION_OFFSET,
        minutes: i32::from(bytes[5]) - TNS_DURATION_OFFSET,
        seconds: i32::from(bytes[6]) - TNS_DURATION_OFFSET,
        fseconds: to_component(i64::from(fseconds_wire) - TNS_DURATION_MID)?,
    })
}

/// Encodes an INTERVAL YEAR TO MONTH value (reference
/// impl/base/encoders.pyx:151-161): big-endian years offset by
/// TNS_DURATION_MID followed by months offset by TNS_DURATION_OFFSET.
pub(crate) fn encode_interval_ym(years: i32, months: i32) -> Result<[u8; 5]> {
    let mut bytes = [0u8; 5];
    let wire_years = u32::try_from(i64::from(years) + TNS_DURATION_MID)
        .map_err(|_| ProtocolError::TtcDecode("INTERVAL YM years out of range"))?;
    bytes[..4].copy_from_slice(&wire_years.to_be_bytes());
    bytes[4] = u8::try_from(months + TNS_DURATION_OFFSET)
        .map_err(|_| ProtocolError::TtcDecode("INTERVAL YM months out of range"))?;
    Ok(bytes)
}

/// Decodes an INTERVAL YEAR TO MONTH value (reference
/// impl/base/decoders.pyx:147-155). Components are signed: negative
/// intervals subtract below the offsets.
pub(crate) fn decode_interval_ym(bytes: &[u8]) -> Result<QueryValue> {
    if bytes.len() < 5 {
        return Err(ProtocolError::TtcDecode("invalid INTERVAL YM length"));
    }
    let years_wire = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    let years = i32::try_from(i64::from(years_wire) - TNS_DURATION_MID)
        .map_err(|_| ProtocolError::TtcDecode("INTERVAL YM out of range"))?;
    Ok(QueryValue::IntervalYM {
        years,
        months: i32::from(bytes[4]) - TNS_DURATION_OFFSET,
    })
}

pub(crate) fn decode_binary_double(bytes: &[u8]) -> Result<f64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| ProtocolError::TtcDecode("invalid BINARY_DOUBLE length"))?;
    let mut decoded = bytes;
    if decoded[0] & 0x80 != 0 {
        decoded[0] &= 0x7f;
    } else {
        for byte in &mut decoded {
            *byte = !*byte;
        }
    }
    Ok(f64::from_bits(u64::from_be_bytes(decoded)))
}

/// Encode a canonical decimal `value` into the Oracle `NUMBER` wire form
/// (the inverse of [`decode_number_value`]). Public so benches / parity
/// harnesses can synthesize fetch payloads. Reference
/// impl/base/encoders.pyx.
pub fn encode_number_text(value: &str) -> Result<Vec<u8>> {
    let value = value.as_bytes();
    if value.is_empty() {
        return Err(ProtocolError::TtcDecode("empty NUMBER bind"));
    }
    if value.len() > NUMBER_AS_TEXT_CHARS {
        return Err(ProtocolError::TtcDecode("NUMBER bind text too long"));
    }

    let mut pos = 0;
    let mut is_negative = false;
    if matches!(value.first(), Some(&b'-')) {
        is_negative = true;
        pos += 1;
    }

    let mut digits = Vec::with_capacity(NUMBER_AS_TEXT_CHARS);
    while let Some(byte) = value.get(pos).copied() {
        if matches!(byte, b'.' | b'e' | b'E') {
            break;
        }
        if !byte.is_ascii_digit() {
            return Err(ProtocolError::TtcDecode("invalid NUMBER bind"));
        }
        let digit = byte - b'0';
        pos += 1;
        if digit == 0 && digits.is_empty() {
            continue;
        }
        digits.push(digit);
    }
    let mut decimal_point_index = i32::try_from(digits.len()).unwrap_or(i32::MAX);

    if matches!(value.get(pos), Some(&b'.')) {
        pos += 1;
        while let Some(byte) = value.get(pos).copied() {
            if matches!(byte, b'e' | b'E') {
                break;
            }
            if !byte.is_ascii_digit() {
                return Err(ProtocolError::TtcDecode("invalid NUMBER bind"));
            }
            let digit = byte - b'0';
            pos += 1;
            if digit == 0 && digits.is_empty() {
                decimal_point_index -= 1;
                continue;
            }
            digits.push(digit);
        }
    }

    if matches!(value.get(pos).copied(), Some(b'e' | b'E')) {
        pos += 1;
        let mut exponent_is_negative = false;
        if let Some(byte) = value.get(pos).copied() {
            if byte == b'-' {
                exponent_is_negative = true;
                pos += 1;
            } else if byte == b'+' {
                pos += 1;
            }
        }
        let exponent_start = pos;
        while let Some(byte) = value.get(pos).copied() {
            if !byte.is_ascii_digit() {
                return Err(ProtocolError::TtcDecode("invalid NUMBER exponent"));
            }
            pos += 1;
        }
        if exponent_start == pos {
            return Err(ProtocolError::TtcDecode("empty NUMBER exponent"));
        }
        let exponent_text = std::str::from_utf8(&value[exponent_start..pos])
            .map_err(|_| ProtocolError::TtcDecode("invalid NUMBER exponent"))?;
        let mut exponent = exponent_text
            .parse::<i32>()
            .map_err(|_| ProtocolError::TtcDecode("invalid NUMBER exponent"))?;
        if exponent_is_negative {
            exponent = -exponent;
        }
        // `exponent` is parsed as a full i32 (the sign is stripped before
        // parsing, so it is in [0, i32::MAX] before negation) while the
        // reference treats it as int16_t. A crafted bind such as
        // "1"*160 + "e+2147483647" (within NUMBER_AS_TEXT_CHARS) would overflow
        // this add — panicking in debug builds — so add checked and reject
        // out-of-range like the reference's range check does (encoders.pyx).
        decimal_point_index = decimal_point_index
            .checked_add(exponent)
            .ok_or(ProtocolError::TtcDecode("NUMBER bind out of range"))?;
    }

    if pos < value.len() {
        return Err(ProtocolError::TtcDecode("invalid NUMBER bind suffix"));
    }

    while digits.last().is_some_and(|digit| *digit == 0) {
        digits.pop();
    }
    if digits.len() > NUMBER_MAX_DIGITS || !(-129..=126).contains(&decimal_point_index) {
        return Err(ProtocolError::TtcDecode("NUMBER bind out of range"));
    }

    let mut prepend_zero = false;
    if decimal_point_index % 2 != 0 {
        prepend_zero = true;
        if !digits.is_empty() {
            digits.push(0);
            decimal_point_index += 1;
        }
    }
    if digits.len() % 2 == 1 {
        digits.push(0);
    }

    if digits.is_empty() {
        return Ok(vec![128]);
    }

    let mut encoded = Vec::with_capacity(digits.len() / 2 + 2);
    let exponent_on_wire = decimal_point_index / 2 + 192;
    if !(0..=255).contains(&exponent_on_wire) {
        return Err(ProtocolError::TtcDecode(
            "NUMBER bind exponent out of range",
        ));
    }
    let exponent_byte = exponent_on_wire as u8;
    encoded.push(if is_negative {
        !exponent_byte
    } else {
        exponent_byte
    });

    let mut digit_pos = 0;
    for pair_num in 0..(digits.len() / 2) {
        let mut digit = if pair_num == 0 && prepend_zero {
            let digit = digits[digit_pos];
            digit_pos += 1;
            digit
        } else {
            let digit = digits[digit_pos] * 10 + digits[digit_pos + 1];
            digit_pos += 2;
            digit
        };
        if is_negative {
            digit = 101 - digit;
        } else {
            digit += 1;
        }
        encoded.push(digit);
    }

    if is_negative && digits.len() < NUMBER_MAX_DIGITS {
        encoded.push(102);
    }

    Ok(encoded)
}

pub fn decode_number_value(bytes: &[u8]) -> Result<QueryValue> {
    Ok(QueryValue::Number(super::number::OracleNumber::from_wire(
        bytes,
    )?))
}

/// Decode the Oracle `NUMBER` wire form into canonical decimal text, **appending
/// to `text`** and returning whether the value is integral. `digits` is a
/// caller-owned scratch buffer (cleared on entry) so a tight decode loop can
/// reuse one allocation across many values — this is the allocation-free core
/// the borrowed fetch path drives, writing straight into its per-row arena.
/// [`decode_number_value`] is the owning convenience wrapper.
///
/// Implemented in terms of `decode_number_parts` plus the shared formatter
/// fragment below, so the borrowed-arena text and the owned inline
/// [`super::number::OracleNumber`] are byte-identical by construction (they walk
/// the same digits and format with the same code).
pub fn decode_number_text_into(
    bytes: &[u8],
    digits: &mut Vec<u8>,
    text: &mut String,
) -> Result<bool> {
    match decode_number_parts(bytes, digits, text)? {
        // The single-byte sentinels already wrote their canonical text.
        super::number::DecodedNumber::Text { is_integer } => Ok(is_integer),
        super::number::DecodedNumber::Parts {
            is_negative,
            decimal_point_index,
            is_integer,
        } => {
            format_number_digits(digits, is_negative, decimal_point_index, text);
            Ok(is_integer)
        }
    }
}

/// Walk the Oracle `NUMBER` wire form into `digits` (significant decimal digits,
/// each 0..=9) and report the parts needed to format the canonical text and to
/// build the inline [`super::number::OracleNumber`]. The single-byte sentinels
/// (positive zero, the `-1e126` negative sentinel) write their canonical text
/// directly into `text` and return [`super::number::DecodedNumber::Text`].
///
/// This is the SINGLE digit-decoding source of truth: both the owned inline
/// representation and the borrowed-arena text path drive it.
pub(crate) fn decode_number_parts(
    bytes: &[u8],
    digits: &mut Vec<u8>,
    text: &mut String,
) -> Result<super::number::DecodedNumber> {
    use super::number::DecodedNumber;

    if bytes.len() > 21 {
        return Err(ProtocolError::TtcDecode("encoded NUMBER too long"));
    }
    let Some(&first) = bytes.first() else {
        return Err(ProtocolError::TtcDecode("empty NUMBER"));
    };
    let is_positive = first & 0x80 != 0;
    digits.clear();
    if bytes.len() == 1 {
        if is_positive {
            text.push('0');
        } else {
            text.push_str("-1e126");
        }
        return Ok(DecodedNumber::Text { is_integer: true });
    }

    let exponent_byte = if is_positive { first } else { !first };
    let exponent = i16::from(exponent_byte) - 193;
    let mut decimal_point_index = exponent * 2 + 2;
    let mut end = bytes.len();
    if !is_positive && bytes[end - 1] == 102 {
        end -= 1;
    }

    for (index, encoded) in bytes.iter().enumerate().take(end).skip(1) {
        let value = if is_positive {
            encoded.saturating_sub(1)
        } else {
            101u8.saturating_sub(*encoded)
        };

        let first_digit = value / 10;
        if first_digit == 0 && digits.is_empty() {
            decimal_point_index -= 1;
        } else if first_digit == 10 {
            digits.push(1);
            digits.push(0);
            decimal_point_index += 1;
        } else if first_digit != 0 || index > 0 {
            digits.push(first_digit);
        }

        let second_digit = value % 10;
        if second_digit != 0 || index < end - 1 {
            digits.push(second_digit);
        }
    }

    // `is_integer` is true unless the canonical text gets a decimal point: that
    // happens when `decimal_point_index <= 0` (leading "0.") or the point falls
    // strictly inside the significant digits (`0 < dpi < len`).
    let len = i16::try_from(digits.len()).unwrap_or(i16::MAX);
    let is_integer = decimal_point_index > 0 && decimal_point_index >= len;

    Ok(DecodedNumber::Parts {
        is_negative: !is_positive,
        decimal_point_index,
        is_integer,
    })
}

/// Stack-buffer twin of [`decode_number_parts`]: walks the wire NUMBER digits
/// into `digit_buf` (no heap allocation) and reports the parts needed to build
/// the inline [`super::number::OracleNumber`]. The single-byte sentinels return
/// their fixed canonical text. The owned per-cell NUMBER decode drives this so a
/// NUMBER-heavy row allocates nothing per cell.
///
/// `digit_buf` MUST be at least [`super::number::MAX_DIGITS`] long. This shares
/// the exact digit-walk logic with [`decode_number_parts`]; keep them aligned.
pub(crate) fn decode_number_parts_stack(
    bytes: &[u8],
    digit_buf: &mut [u8],
) -> Result<super::number::DecodedNumberStack> {
    use super::number::DecodedNumberStack;

    if bytes.len() > 21 {
        return Err(ProtocolError::TtcDecode("encoded NUMBER too long"));
    }
    let Some(&first) = bytes.first() else {
        return Err(ProtocolError::TtcDecode("empty NUMBER"));
    };
    let is_positive = first & 0x80 != 0;
    if bytes.len() == 1 {
        return Ok(DecodedNumberStack::Sentinel {
            text: if is_positive { "0" } else { "-1e126" },
            is_integer: true,
        });
    }

    let exponent_byte = if is_positive { first } else { !first };
    let exponent = i16::from(exponent_byte) - 193;
    let mut decimal_point_index = exponent * 2 + 2;
    let mut end = bytes.len();
    if !is_positive && bytes[end - 1] == 102 {
        end -= 1;
    }

    let mut len = 0usize;
    // FUSED i128 coefficient (bead rust-oracledb-shh): folded as each significant
    // digit is emitted, removing the second `digits_to_i128` walk over the digit
    // buffer for the common in-range NUMBER. `Some(acc)` accumulates `acc*10 + d`
    // over the SAME digit sequence, in the SAME order, that `digits_to_i128`
    // walks — so the result is byte-identical. On overflow it latches to `None`
    // and the digit buffer (still filled below) drives the unchanged spill path.
    let mut coeff: Option<i128> = Some(0);
    // The digit count is provably <= MAX_DIGITS for valid wire forms; guard
    // defensively so a crafted oversize input cannot index out of bounds. Each
    // emitted digit is also folded into the i128 accumulator.
    let push = |buf: &mut [u8], d: u8, len: &mut usize, coeff: &mut Option<i128>| {
        if *len < buf.len() {
            buf[*len] = d;
            *len += 1;
        }
        *coeff = coeff
            .and_then(|acc| acc.checked_mul(10))
            .and_then(|acc| acc.checked_add(i128::from(d)));
    };

    for (index, encoded) in bytes.iter().enumerate().take(end).skip(1) {
        let value = if is_positive {
            encoded.saturating_sub(1)
        } else {
            101u8.saturating_sub(*encoded)
        };

        let first_digit = value / 10;
        if first_digit == 0 && len == 0 {
            decimal_point_index -= 1;
        } else if first_digit == 10 {
            push(digit_buf, 1, &mut len, &mut coeff);
            push(digit_buf, 0, &mut len, &mut coeff);
            decimal_point_index += 1;
        } else if first_digit != 0 || index > 0 {
            push(digit_buf, first_digit, &mut len, &mut coeff);
        }

        let second_digit = value % 10;
        if second_digit != 0 || index < end - 1 {
            push(digit_buf, second_digit, &mut len, &mut coeff);
        }
    }

    let len_i16 = i16::try_from(len).unwrap_or(i16::MAX);
    let is_integer = decimal_point_index > 0 && decimal_point_index >= len_i16;

    // Apply the sign to the fused coefficient, matching `digits_to_i128`'s
    // `if is_negative { -acc }`. Negating a non-overflowed magnitude can itself
    // never overflow i128 here (the magnitude already fit), so this preserves the
    // exact spill boundary.
    let coefficient = coeff.map(|acc| if is_positive { acc } else { -acc });

    Ok(DecodedNumberStack::Parts {
        digit_len: len,
        is_negative: !is_positive,
        decimal_point_index,
        is_integer,
        coefficient,
    })
}

/// Append the canonical decimal text for `digits` (significant decimal digits,
/// each 0..=9) positioned by `decimal_point_index`, with the given sign. This is
/// the legacy `decode_number_text_into` formatting tail, factored out so the
/// inline [`super::number::OracleNumber`] formatter is the same logic. Keep it
/// byte-for-byte aligned with `super::number::fmt_inline_into`.
pub(crate) fn format_number_digits(
    digits: &[u8],
    is_negative: bool,
    decimal_point_index: i16,
    text: &mut String,
) {
    if is_negative {
        text.push('-');
    }
    if decimal_point_index <= 0 {
        text.push_str("0.");
        for _ in decimal_point_index..0 {
            text.push('0');
        }
    }
    for (index, digit) in digits.iter().enumerate() {
        if index > 0
            && matches!(
                i16::try_from(index)
                    .unwrap_or(i16::MAX)
                    .cmp(&decimal_point_index),
                std::cmp::Ordering::Equal
            )
        {
            text.push('.');
        }
        text.push(char::from(b'0' + *digit));
    }
    if decimal_point_index > i16::try_from(digits.len()).unwrap_or(i16::MAX) {
        for _ in i16::try_from(digits.len()).unwrap_or(i16::MAX)..decimal_point_index {
            text.push('0');
        }
    }
}

pub(crate) fn decode_text_value(bytes: &[u8], csfrm: u8) -> Result<String> {
    if csfrm == CS_FORM_NCHAR {
        let units = bytes
            .chunks_exact(2)
            .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
            .collect::<Vec<_>>();
        if units.len() * 2 != bytes.len() {
            return Err(ProtocolError::TtcDecode("invalid UTF-16 text length"));
        }
        String::from_utf16(&units).map_err(|_| ProtocolError::TtcDecode("invalid UTF-16 text"))
    } else {
        String::from_utf8(bytes.to_vec())
            .map_err(|_| ProtocolError::TtcDecode("invalid UTF-8 text"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Regression: bead rust-oracledb-jmc. A crafted BindValue::Number whose
    // text packs many leading digits and a huge exponent (within the
    // NUMBER_AS_TEXT_CHARS cap) used to overflow `decimal_point_index +=
    // exponent`, panicking in debug builds. The reference rejects such values;
    // we must too (clean Err, never a panic).
    #[test]
    fn number_text_huge_exponent_rejected_not_panicked() {
        // 160 digits + "e+2147483647" == 172 bytes == NUMBER_AS_TEXT_CHARS.
        let crafted = format!("{}e+2147483647", "1".repeat(160));
        assert_eq!(crafted.len(), NUMBER_AS_TEXT_CHARS);
        assert!(encode_number_text(&crafted).is_err());

        // Negative-exponent counterpart must also reject without panicking.
        let crafted_neg = format!("0.{}e-2147483647", "0".repeat(158));
        assert!(encode_number_text(&crafted_neg).is_err());
    }

    #[test]
    fn number_text_ordinary_values_still_encode() {
        for ok in [
            "0",
            "1",
            "-1",
            "3.14159",
            "1e10",
            "-2.5e-3",
            "12345678901234567890",
        ] {
            assert!(encode_number_text(ok).is_ok(), "expected {ok} to encode");
        }
    }

    #[test]
    fn interval_ds_roundtrip_preserves_nanoseconds() {
        let wire = encode_interval_ds(2, 3 * 3600 + 4 * 60 + 5, 123_456_789)
            .expect("encode nanosecond interval");
        assert_eq!(
            decode_interval_ds(&wire).expect("decode nanosecond interval"),
            QueryValue::IntervalDS {
                days: 2,
                hours: 3,
                minutes: 4,
                seconds: 5,
                fseconds: 123_456_789,
            }
        );
    }

    #[test]
    fn interval_ds_rejects_truncated_wire_value() {
        let wire = encode_interval_ds(-2, -(3 * 3600 + 4 * 60 + 5), -123_456_789)
            .expect("encode negative interval");
        assert!(decode_interval_ds(&wire[..10]).is_err());
    }

    #[test]
    fn datetime_decode_rejects_malformed_wire_fields() {
        let valid_date = encode_oracle_date(2026, 7, 13, 12, 34, 56).expect("valid date");
        for (index, value, label) in [
            (0, 99, "year"),
            (2, 0, "month zero"),
            (2, 13, "month high"),
            (3, 0, "day zero"),
            (3, 32, "day high"),
            (4, 0, "hour zero"),
            (4, 25, "hour high"),
            (5, 0, "minute zero"),
            (5, 61, "minute high"),
            (6, 0, "second zero"),
            (6, 61, "second high"),
        ] {
            let mut malformed = valid_date;
            malformed[index] = value;
            assert!(
                decode_datetime_value(&malformed).is_err(),
                "{label} must fail closed"
            );
        }
    }

    #[test]
    fn timestamp_decode_rejects_invalid_length_and_fraction() {
        let timestamp =
            encode_oracle_timestamp(2026, 7, 13, 12, 34, 56, 123_456_789).expect("timestamp");
        for len in [6, 8, 10, 12, 14] {
            let mut malformed = timestamp.clone();
            malformed.resize(len, 0);
            assert!(
                decode_datetime_value(&malformed).is_err(),
                "length {len} must fail closed"
            );
        }

        let mut malformed = timestamp;
        malformed[7..11].copy_from_slice(&1_000_000_000_u32.to_be_bytes());
        assert!(
            decode_datetime_value(&malformed).is_err(),
            "nanosecond above Oracle range must fail closed"
        );
    }

    #[test]
    fn timestamp_tz_decode_rejects_missing_or_malformed_offset_bytes() {
        let valid = encode_oracle_timestamp_tz_with_offset(2026, 7, 13, 12, 34, 56, 0, -330)
            .expect("timestamp with time zone");
        for (hour_byte, minute_byte, label) in [
            (0, valid[12], "missing offset hour"),
            (valid[11], 0, "missing offset minute"),
            (0, 0, "missing full offset"),
            (valid[11], TZ_MINUTE_OFFSET * 2, "invalid offset minute"),
            (TZ_HOUR_OFFSET + 24, TZ_MINUTE_OFFSET, "invalid +24h offset"),
            (127, TZ_MINUTE_OFFSET, "absurd offset hour"),
        ] {
            let mut malformed = valid.clone();
            malformed[11] = hour_byte;
            malformed[12] = minute_byte;
            assert!(
                decode_datetime_value(&malformed).is_err(),
                "{label} must fail closed"
            );
        }

        for offset_minutes in [-1440, 1440, 6420] {
            assert!(
                encode_oracle_timestamp_tz_with_offset(2026, 7, 13, 12, 34, 56, 0, offset_minutes,)
                    .is_err(),
                "outbound offset {offset_minutes} must fail closed"
            );
        }
    }

    #[test]
    fn number_decode_heap_and_stack_paths_match_edge_corpus() {
        for (text, is_integer) in [
            ("0", true),
            ("0.01", false),
            ("0.1", false),
            ("0.99", false),
            ("9.9", false),
            ("10", true),
            ("99", true),
            ("100", true),
            ("101", true),
            ("-0.01", false),
            ("-0.1", false),
            ("-0.99", false),
            ("-9.9", false),
            ("-10", true),
            ("-99", true),
            ("-100", true),
            ("-101", true),
            ("12345678901234567890123456789012345678", true),
            ("-12345678901234567890123456789012345678", true),
        ] {
            let wire = encode_number_text(text).expect("encode NUMBER edge");
            let mut digits = Vec::new();
            let mut decoded = String::new();
            let decoded_is_integer =
                decode_number_text_into(&wire, &mut digits, &mut decoded).expect("decode text");
            let owned = decode_number_value(&wire).expect("decode owned NUMBER");

            assert_eq!(decoded, text, "canonical NUMBER text for wire {wire:02x?}");
            assert_eq!(
                decoded_is_integer, is_integer,
                "integer flag for NUMBER wire {wire:02x?}"
            );
            assert_eq!(
                owned.as_number_text().as_deref(),
                Some(decoded.as_str()),
                "owned stack decode must match heap text decode for {text}"
            );
        }
    }

    #[test]
    fn number_part_decoders_emit_exact_digits_decimal_and_coefficient() {
        use super::super::number::{DecodedNumber, DecodedNumberStack, MAX_DIGITS};

        struct Case<'a> {
            wire: &'a [u8],
            digits: &'a [u8],
            is_negative: bool,
            decimal_point_index: i16,
            is_integer: bool,
            coefficient: Option<i128>,
            text: &'a str,
        }

        let cases = [
            // Forces the `first_digit == 10` carry branch. The wire is unusual
            // but valid for the decoder contract: it must produce canonical
            // "100", not "10", "1.0", or a zero-length stack result.
            Case {
                wire: &[193, 101],
                digits: &[1, 0],
                is_negative: false,
                decimal_point_index: 3,
                is_integer: true,
                coefficient: Some(10),
                text: "100",
            },
            // Leading zero base-100 group: decimal point moves left before any
            // significant digit is emitted, but the intermediate zero digit is
            // still retained because another group follows.
            Case {
                wire: &[193, 1, 2],
                digits: &[0, 0, 1],
                is_negative: false,
                decimal_point_index: 1,
                is_integer: false,
                coefficient: Some(1),
                text: "0.01",
            },
            // Negative terminator byte must be excluded from the digit walk.
            Case {
                wire: &[62, 100, 102],
                digits: &[1],
                is_negative: true,
                decimal_point_index: 1,
                is_integer: true,
                coefficient: Some(-1),
                text: "-1",
            },
            // Long digit sequence: the final zero of the last base-100 group is
            // suppressed, and the fused coefficient must match the emitted
            // digit stream exactly.
            Case {
                wire: &[
                    211, 13, 35, 57, 79, 91, 13, 35, 57, 79, 91, 13, 35, 57, 79, 91, 13, 35, 57,
                    79, 91,
                ],
                digits: &[
                    1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6,
                    7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
                ],
                is_negative: false,
                decimal_point_index: 38,
                is_integer: false,
                coefficient: Some(123456789012345678901234567890123456789),
                text: "12345678901234567890123456789012345678.9",
            },
        ];

        for case in cases {
            let mut heap_digits = Vec::new();
            let mut text = String::new();
            let decoded = decode_number_parts(case.wire, &mut heap_digits, &mut text)
                .expect("heap NUMBER part decode");
            assert!(
                matches!(decoded, DecodedNumber::Parts { .. }),
                "expected heap NUMBER parts for {:02x?}",
                case.wire
            );
            if let DecodedNumber::Parts {
                is_negative,
                decimal_point_index,
                is_integer,
            } = decoded
            {
                assert_eq!(
                    heap_digits, case.digits,
                    "heap digits for {:02x?}",
                    case.wire
                );
                assert_eq!(
                    is_negative, case.is_negative,
                    "heap sign for {:02x?}",
                    case.wire
                );
                assert_eq!(
                    decimal_point_index, case.decimal_point_index,
                    "heap decimal point for {:02x?}",
                    case.wire
                );
                assert_eq!(
                    is_integer, case.is_integer,
                    "heap integer flag for {:02x?}",
                    case.wire
                );
                format_number_digits(&heap_digits, is_negative, decimal_point_index, &mut text);
                assert_eq!(text, case.text, "heap text for {:02x?}", case.wire);
            }

            let mut stack_digits = [0u8; MAX_DIGITS];
            let decoded = decode_number_parts_stack(case.wire, &mut stack_digits)
                .expect("stack NUMBER part decode");
            assert!(
                matches!(decoded, DecodedNumberStack::Parts { .. }),
                "expected stack NUMBER parts for {:02x?}",
                case.wire
            );
            if let DecodedNumberStack::Parts {
                digit_len,
                is_negative,
                decimal_point_index,
                is_integer,
                coefficient,
            } = decoded
            {
                assert_eq!(
                    &stack_digits[..digit_len],
                    case.digits,
                    "stack digits for {:02x?}",
                    case.wire
                );
                assert_eq!(
                    is_negative, case.is_negative,
                    "stack sign for {:02x?}",
                    case.wire
                );
                assert_eq!(
                    decimal_point_index, case.decimal_point_index,
                    "stack decimal point for {:02x?}",
                    case.wire
                );
                assert_eq!(
                    is_integer, case.is_integer,
                    "stack integer flag for {:02x?}",
                    case.wire
                );
                assert_eq!(
                    coefficient, case.coefficient,
                    "stack coefficient for {:02x?}",
                    case.wire
                );
                let mut stack_text = String::new();
                format_number_digits(
                    &stack_digits[..digit_len],
                    is_negative,
                    decimal_point_index,
                    &mut stack_text,
                );
                assert_eq!(stack_text, case.text, "stack text for {:02x?}", case.wire);
            }
        }
    }
}
