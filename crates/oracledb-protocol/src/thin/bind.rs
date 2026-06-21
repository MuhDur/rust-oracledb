#![forbid(unsafe_code)]

use super::*;

impl BindValue {
    pub(crate) fn is_output_only(&self) -> bool {
        matches!(self, BindValue::Output { .. })
            || matches!(self, BindValue::ReturnOutput { .. })
            || matches!(self, BindValue::ObjectOutput { .. })
            || matches!(self, BindValue::Array { values, .. } if values.is_empty())
    }

    pub(crate) fn is_return_output(&self) -> bool {
        matches!(self, BindValue::ReturnOutput { .. })
            || matches!(
                self,
                BindValue::ObjectOutput {
                    is_return: true,
                    ..
                }
            )
    }
}

pub(crate) fn write_bind_metadata_with_type(
    writer: &mut TtcWriter,
    value: &BindValue,
    ora_type_num: u8,
    csfrm: u8,
    buffer_size: u32,
) -> Result<()> {
    let (flags, max_elements) = match value {
        BindValue::Array { max_elements, .. } => {
            (TNS_BIND_USE_INDICATORS | TNS_BIND_ARRAY, *max_elements)
        }
        _ => (TNS_BIND_USE_INDICATORS, 0),
    };
    // JSON binds advertise a TNS_JSON_MAX_LENGTH prefetch buffer (reference
    // base.pyx:1398-1400) so a returned/out OSON image streams inline.
    let buffer_size = if ora_type_num == ORA_TYPE_NUM_JSON {
        TNS_JSON_MAX_LENGTH
    } else {
        buffer_size
    };
    writer.write_u8(ora_type_num);
    writer.write_u8(flags);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub4(buffer_size);
    writer.write_ub4(max_elements);
    let cont_flags = if matches!(
        ora_type_num,
        ORA_TYPE_NUM_CLOB | ORA_TYPE_NUM_BLOB | ORA_TYPE_NUM_VECTOR | ORA_TYPE_NUM_JSON
    ) {
        TNS_LOB_PREFETCH_FLAG
    } else {
        0
    };
    writer.write_ub8(cont_flags);
    if let BindValue::ObjectOutput { oid, version, .. }
    | BindValue::ObjectInput { oid, version, .. } = value
    {
        writer.write_bytes_with_two_lengths(Some(oid))?;
        writer.write_ub4(*version);
    } else {
        writer.write_ub4(0);
        writer.write_ub2(0);
    }
    if csfrm != 0 {
        writer.write_ub2(TNS_CHARSET_UTF8);
    } else {
        writer.write_ub2(0);
    }
    writer.write_u8(csfrm);
    // max chars (LOB prefetch length): VECTOR advertises TNS_VECTOR_MAX_LENGTH
    // so the server prefetches the image inline (reference base.pyx)
    let lob_prefetch_length = match ora_type_num {
        ORA_TYPE_NUM_VECTOR => TNS_VECTOR_MAX_LENGTH,
        ORA_TYPE_NUM_JSON => TNS_JSON_MAX_LENGTH,
        _ => 0,
    };
    writer.write_ub4(lob_prefetch_length);
    writer.write_ub4(0);
    Ok(())
}

pub fn bind_value_type_info(value: &BindValue) -> Option<BindTypeInfo> {
    let (ora_type_num, csfrm, buffer_size) = match value {
        BindValue::Null => return None,
        BindValue::TypedNull {
            ora_type_num,
            csfrm,
            buffer_size,
        }
        | BindValue::Output {
            ora_type_num,
            csfrm,
            buffer_size,
        }
        | BindValue::ReturnOutput {
            ora_type_num,
            csfrm,
            buffer_size,
        } => (*ora_type_num, *csfrm, (*buffer_size).max(1)),
        BindValue::ObjectOutput { buffer_size, .. }
        | BindValue::ObjectInput { buffer_size, .. } => {
            (ORA_TYPE_NUM_OBJECT, 0, (*buffer_size).max(1))
        }
        // values larger than 32767 bytes keep the VARCHAR/RAW bind type with
        // a large buffer size; the chunked length encoding carries the data
        // (reference always derives VARCHAR/RAW from str/bytes values —
        // metadata.pyx from_value — and never switches the bind type to LONG,
        // which the server rejects for PL/SQL LOB parameters with ORA-01460)
        BindValue::Text(value) => {
            let buffer_size = u32::try_from(value.chars().count())
                .unwrap_or(u32::MAX)
                .saturating_mul(4)
                .max(1);
            (ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT, buffer_size)
        }
        BindValue::Raw(value) => {
            let buffer_size = u32::try_from(value.len()).unwrap_or(u32::MAX).max(1);
            (ORA_TYPE_NUM_RAW, 0, buffer_size)
        }
        BindValue::Lob {
            ora_type_num,
            csfrm,
            ..
        } => (*ora_type_num, *csfrm, 1),
        BindValue::Number(_) => (ORA_TYPE_NUM_NUMBER, 0, ORA_TYPE_SIZE_NUMBER),
        BindValue::BinaryInteger(_) => (ORA_TYPE_NUM_BINARY_INTEGER, 0, ORA_TYPE_SIZE_NUMBER),
        BindValue::Boolean(_) => (ORA_TYPE_NUM_BOOLEAN, 0, ORA_TYPE_SIZE_BOOLEAN),
        BindValue::BinaryDouble(_) => (ORA_TYPE_NUM_BINARY_DOUBLE, 0, ORA_TYPE_SIZE_BINARY_DOUBLE),
        BindValue::BinaryFloat(_) => (ORA_TYPE_NUM_BINARY_FLOAT, 0, ORA_TYPE_SIZE_BINARY_FLOAT),
        BindValue::IntervalDS { .. } => (ORA_TYPE_NUM_INTERVAL_DS, 0, ORA_TYPE_SIZE_INTERVAL_DS),
        BindValue::IntervalYM { .. } => (ORA_TYPE_NUM_INTERVAL_YM, 0, ORA_TYPE_SIZE_INTERVAL_YM),
        BindValue::DateTime { .. } => (ORA_TYPE_NUM_DATE, 0, ORA_TYPE_SIZE_DATE),
        BindValue::Timestamp { ora_type_num, .. } => (
            *ora_type_num,
            0,
            if *ora_type_num == ORA_TYPE_NUM_TIMESTAMP_TZ {
                ORA_TYPE_SIZE_TIMESTAMP_TZ
            } else {
                ORA_TYPE_SIZE_TIMESTAMP
            },
        ),
        BindValue::Array {
            ora_type_num,
            csfrm,
            buffer_size,
            ..
        } => (*ora_type_num, *csfrm, (*buffer_size).max(1)),
        // reference base.pyx _write_column_metadata: VECTOR binds advertise a
        // TNS_VECTOR_MAX_LENGTH prefetch buffer and the LOB-prefetch cont flag
        BindValue::Vector(_) => (ORA_TYPE_NUM_VECTOR, 0, TNS_VECTOR_MAX_LENGTH),
        // JSON binds: the reference DB_TYPE_JSON var has buffer_size_factor 0, so
        // its metadata buffer_size is small and the OSON value is written inline
        // (not deferred to the "long" bind section). The TNS_JSON_MAX_LENGTH
        // prefetch buffer is applied only in the wire metadata writer, not here,
        // so the long/non-long bind-data ordering matches the reference.
        BindValue::Json(_) => (ORA_TYPE_NUM_JSON, 0, 1),
        BindValue::Cursor { .. } => (ORA_TYPE_NUM_CURSOR, 0, 4),
    };
    Some(BindTypeInfo {
        ora_type_num,
        csfrm,
        buffer_size,
    })
}

pub fn define_metadata_from_bind(source: &ColumnMetadata, value: &BindValue) -> ColumnMetadata {
    let Some(mut info) = bind_value_type_info(value) else {
        return source.clone();
    };
    if source.ora_type_num == ORA_TYPE_NUM_CLOB
        && matches!(
            info.ora_type_num,
            ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_LONG | ORA_TYPE_NUM_VARCHAR
        )
    {
        info.ora_type_num = ORA_TYPE_NUM_LONG;
        if source.csfrm != 0 {
            info.csfrm = source.csfrm;
        }
    }
    let mut metadata = source.clone();
    metadata.ora_type_num = info.ora_type_num;
    metadata.csfrm = info.csfrm;
    if info.ora_type_num == ORA_TYPE_NUM_LONG {
        metadata.buffer_size = TNS_MAX_LONG_LENGTH;
        metadata.max_size = 0;
    } else {
        metadata.buffer_size = info.buffer_size.max(1);
        metadata.max_size = info.buffer_size.max(1);
    }
    metadata
}

/// When the same query is re-executed after a column's data type changed to
/// CLOB/BLOB but the previous execution fetched the column as a char/raw
/// type, the server streams the data as LONG/LONG RAW (same as a define of
/// CLOB/BLOB as string/bytes); the fetch metadata must follow (reference
/// impl/thin/messages/base.pyx:820-845 `_adjust_metadata`). Returns `true`
/// when the metadata was adjusted.
pub fn adjust_refetch_metadata(previous: &ColumnMetadata, current: &mut ColumnMetadata) -> bool {
    if current.ora_type_num == ORA_TYPE_NUM_CLOB
        && matches!(
            previous.ora_type_num,
            ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_LONG | ORA_TYPE_NUM_VARCHAR
        )
    {
        current.ora_type_num = ORA_TYPE_NUM_LONG;
        current.csfrm = previous.csfrm;
        current.buffer_size = TNS_MAX_LONG_LENGTH;
        current.max_size = 0;
        return true;
    }
    if current.ora_type_num == ORA_TYPE_NUM_BLOB
        && matches!(
            previous.ora_type_num,
            ORA_TYPE_NUM_RAW | ORA_TYPE_NUM_LONG_RAW
        )
    {
        current.ora_type_num = ORA_TYPE_NUM_LONG_RAW;
        current.csfrm = 0;
        current.buffer_size = TNS_MAX_LONG_LENGTH;
        current.max_size = 0;
        return true;
    }
    false
}

pub fn output_bind(value: BindValue) -> BindValue {
    match value {
        BindValue::ObjectOutput {
            schema,
            type_name,
            oid,
            version,
            buffer_size,
            ..
        } => BindValue::ObjectOutput {
            schema,
            type_name,
            oid,
            version,
            buffer_size: buffer_size.max(1),
            is_return: false,
        },
        value => {
            let info = bind_value_type_info(&value).unwrap_or(BindTypeInfo {
                ora_type_num: ORA_TYPE_NUM_VARCHAR,
                csfrm: CS_FORM_IMPLICIT,
                buffer_size: 1,
            });
            BindValue::Output {
                ora_type_num: info.ora_type_num,
                csfrm: info.csfrm,
                buffer_size: info.buffer_size,
            }
        }
    }
}

pub fn returning_output_bind(value: BindValue) -> BindValue {
    match value {
        BindValue::ObjectOutput {
            schema,
            type_name,
            oid,
            version,
            buffer_size,
            ..
        } => BindValue::ObjectOutput {
            schema,
            type_name,
            oid,
            version,
            buffer_size: buffer_size.max(1),
            is_return: true,
        },
        value => {
            let info = bind_value_type_info(&value).unwrap_or(BindTypeInfo {
                ora_type_num: ORA_TYPE_NUM_VARCHAR,
                csfrm: CS_FORM_IMPLICIT,
                buffer_size: 1,
            });
            BindValue::ReturnOutput {
                ora_type_num: info.ora_type_num,
                csfrm: info.csfrm,
                buffer_size: info.buffer_size,
            }
        }
    }
}

pub fn cursor_bind_template() -> BindValue {
    BindValue::TypedNull {
        ora_type_num: ORA_TYPE_NUM_CURSOR,
        csfrm: 0,
        buffer_size: 4,
    }
}

pub fn is_cursor_bind_template(value: &BindValue) -> bool {
    matches!(
        value,
        BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_CURSOR,
            ..
        }
    )
}

pub fn public_dbtype_name_from_type_name(type_name: &str) -> &'static str {
    match type_name {
        "NUMBER" | "DB_TYPE_NUMBER" | "int" | "float" | "Decimal" => "DB_TYPE_NUMBER",
        "NATIVE_INT" | "DB_TYPE_BINARY_INTEGER" => "DB_TYPE_BINARY_INTEGER",
        "NATIVE_FLOAT" | "DB_TYPE_BINARY_DOUBLE" => "DB_TYPE_BINARY_DOUBLE",
        "DB_TYPE_BINARY_FLOAT" | "BINARY_FLOAT" => "DB_TYPE_BINARY_FLOAT",
        "DB_TYPE_BOOLEAN" | "BOOLEAN" | "bool" => "DB_TYPE_BOOLEAN",
        "DB_TYPE_INTERVAL_DS" | "INTERVAL DAY TO SECOND" | "timedelta" => "DB_TYPE_INTERVAL_DS",
        "DB_TYPE_INTERVAL_YM" | "INTERVAL YEAR TO MONTH" | "IntervalYM" => "DB_TYPE_INTERVAL_YM",
        "DB_TYPE_BFILE" | "BFILE" => "DB_TYPE_BFILE",
        "DB_TYPE_JSON" | "JSON" => "DB_TYPE_JSON",
        "STRING" | "DB_TYPE_VARCHAR" | "str" => "DB_TYPE_VARCHAR",
        "DB_TYPE_CHAR" => "DB_TYPE_CHAR",
        "DB_TYPE_NCHAR" => "DB_TYPE_NCHAR",
        "DB_TYPE_NVARCHAR" => "DB_TYPE_NVARCHAR",
        "DB_TYPE_CLOB" | "CLOB" => "DB_TYPE_CLOB",
        "DB_TYPE_NCLOB" | "NCLOB" => "DB_TYPE_NCLOB",
        "DB_TYPE_BLOB" | "BLOB" => "DB_TYPE_BLOB",
        "DB_TYPE_LONG" | "LONG" | "LONG_STRING" => "DB_TYPE_LONG",
        "DB_TYPE_LONG_NVARCHAR" | "LONG NVARCHAR" => "DB_TYPE_LONG_NVARCHAR",
        "DB_TYPE_LONG_RAW" | "LONG RAW" | "LONG_BINARY" => "DB_TYPE_LONG_RAW",
        "DB_TYPE_RAW" | "BINARY" | "bytes" => "DB_TYPE_RAW",
        "ROWID" | "DB_TYPE_ROWID" => "DB_TYPE_ROWID",
        "DB_TYPE_UROWID" => "DB_TYPE_UROWID",
        "DATETIME" | "DB_TYPE_DATE" | "date" | "datetime" => "DB_TYPE_DATE",
        "DB_TYPE_TIMESTAMP" | "TIMESTAMP" => "DB_TYPE_TIMESTAMP",
        "DB_TYPE_TIMESTAMP_LTZ" | "TIMESTAMP WITH LOCAL TIME ZONE" => "DB_TYPE_TIMESTAMP_LTZ",
        "DB_TYPE_TIMESTAMP_TZ" | "TIMESTAMP WITH TIME ZONE" => "DB_TYPE_TIMESTAMP_TZ",
        "DB_TYPE_CURSOR" | "CURSOR" => "DB_TYPE_CURSOR",
        "DB_TYPE_VECTOR" | "VECTOR" => "DB_TYPE_VECTOR",
        _ => "DB_TYPE_VARCHAR",
    }
}

pub fn column_metadata_is_xmltype(metadata: &ColumnMetadata) -> bool {
    metadata
        .object_schema
        .as_deref()
        .is_some_and(|schema| schema.eq_ignore_ascii_case("SYS"))
        && metadata
            .object_type_name
            .as_deref()
            .is_some_and(|name| name.eq_ignore_ascii_case("XMLTYPE"))
}

pub fn public_dbtype_name_from_column_metadata(metadata: &ColumnMetadata) -> &'static str {
    if column_metadata_is_xmltype(metadata) {
        return "DB_TYPE_XMLTYPE";
    }
    match (metadata.ora_type_num, metadata.csfrm) {
        (ORA_TYPE_NUM_LONG, CS_FORM_NCHAR) => "DB_TYPE_LONG_NVARCHAR",
        (ORA_TYPE_NUM_LONG, _) => "DB_TYPE_LONG",
        (ORA_TYPE_NUM_LONG_RAW, _) => "DB_TYPE_LONG_RAW",
        (ORA_TYPE_NUM_VARCHAR, CS_FORM_NCHAR) => "DB_TYPE_NVARCHAR",
        (ORA_TYPE_NUM_CHAR, CS_FORM_NCHAR) => "DB_TYPE_NCHAR",
        (ORA_TYPE_NUM_CHAR, _) => "DB_TYPE_CHAR",
        (ORA_TYPE_NUM_VARCHAR, _) => "DB_TYPE_VARCHAR",
        (ORA_TYPE_NUM_RAW, _) => "DB_TYPE_RAW",
        (ORA_TYPE_NUM_ROWID, _) => "DB_TYPE_ROWID",
        (ORA_TYPE_NUM_UROWID, _) => "DB_TYPE_UROWID",
        (ORA_TYPE_NUM_BINARY_DOUBLE, _) => "DB_TYPE_BINARY_DOUBLE",
        (ORA_TYPE_NUM_BINARY_FLOAT, _) => "DB_TYPE_BINARY_FLOAT",
        (ORA_TYPE_NUM_BINARY_INTEGER, _) => "DB_TYPE_BINARY_INTEGER",
        (ORA_TYPE_NUM_NUMBER, _) => "DB_TYPE_NUMBER",
        (ORA_TYPE_NUM_CURSOR, _) => "DB_TYPE_CURSOR",
        (ORA_TYPE_NUM_OBJECT, _) => "DB_TYPE_OBJECT",
        (ORA_TYPE_NUM_CLOB, CS_FORM_NCHAR) => "DB_TYPE_NCLOB",
        (ORA_TYPE_NUM_CLOB, _) => "DB_TYPE_CLOB",
        (ORA_TYPE_NUM_BLOB, _) => "DB_TYPE_BLOB",
        (ORA_TYPE_NUM_BFILE, _) => "DB_TYPE_BFILE",
        (ORA_TYPE_NUM_DATE, _) => "DB_TYPE_DATE",
        (ORA_TYPE_NUM_TIMESTAMP, _) => "DB_TYPE_TIMESTAMP",
        (ORA_TYPE_NUM_TIMESTAMP_LTZ, _) => "DB_TYPE_TIMESTAMP_LTZ",
        (ORA_TYPE_NUM_TIMESTAMP_TZ, _) => "DB_TYPE_TIMESTAMP_TZ",
        (ORA_TYPE_NUM_INTERVAL_DS, _) => "DB_TYPE_INTERVAL_DS",
        (ORA_TYPE_NUM_INTERVAL_YM, _) => "DB_TYPE_INTERVAL_YM",
        (ORA_TYPE_NUM_BOOLEAN, _) => "DB_TYPE_BOOLEAN",
        (ORA_TYPE_NUM_VECTOR, _) => "DB_TYPE_VECTOR",
        (ORA_TYPE_NUM_JSON, _) => "DB_TYPE_JSON",
        _ => "DB_TYPE_VARCHAR",
    }
}

/// Mirrors the reference `DbType.default_size` / `_buffer_size_factor` table
/// (reference impl/base/types.pyx:120-440). Returns
/// `(default_size, buffer_size_factor)` for a public database type name.
pub fn public_dbtype_size_info(dbtype_name: &str) -> (u32, u32) {
    match dbtype_name {
        "DB_TYPE_BFILE" => (0, 4000),
        "DB_TYPE_BINARY_DOUBLE" => (0, ORA_TYPE_SIZE_BINARY_DOUBLE),
        "DB_TYPE_BINARY_FLOAT" => (0, ORA_TYPE_SIZE_BINARY_FLOAT),
        "DB_TYPE_BINARY_INTEGER" | "DB_TYPE_NUMBER" => (0, ORA_TYPE_SIZE_NUMBER),
        "DB_TYPE_BLOB" | "DB_TYPE_CLOB" | "DB_TYPE_NCLOB" => (0, 112),
        "DB_TYPE_BOOLEAN" => (0, ORA_TYPE_SIZE_BOOLEAN),
        "DB_TYPE_CHAR" | "DB_TYPE_NCHAR" => (2000, 4),
        "DB_TYPE_CURSOR" => (0, 4),
        "DB_TYPE_DATE" => (0, ORA_TYPE_SIZE_DATE),
        "DB_TYPE_INTERVAL_DS" => (0, ORA_TYPE_SIZE_INTERVAL_DS),
        "DB_TYPE_INTERVAL_YM" => (0, 5),
        "DB_TYPE_LONG" | "DB_TYPE_LONG_NVARCHAR" | "DB_TYPE_LONG_RAW" => (0, TNS_MAX_LONG_LENGTH),
        "DB_TYPE_NVARCHAR" | "DB_TYPE_VARCHAR" => (4000, 4),
        "DB_TYPE_RAW" => (4000, 1),
        "DB_TYPE_ROWID" => (0, ORA_TYPE_SIZE_ROWID),
        "DB_TYPE_TIMESTAMP" | "DB_TYPE_TIMESTAMP_LTZ" => (0, ORA_TYPE_SIZE_TIMESTAMP),
        "DB_TYPE_TIMESTAMP_TZ" => (0, ORA_TYPE_SIZE_TIMESTAMP_TZ),
        "DB_TYPE_JSON" | "DB_TYPE_VECTOR" => (0, 1),
        _ => (0, 0),
    }
}

/// Mirrors the reference fetch-conversion legality matrix
/// (reference impl/base/var.pyx:113-248 `_check_fetch_conversion`). Given the
/// metadata of the column being fetched and the Oracle type requested by an
/// output type handler variable, returns the metadata that should be used for
/// the wire define. Conversions that only affect the Python materialization
/// keep the original wire metadata; LOB and JSON sources adjust the define so
/// the server sends inline data. Unsupported pairs return `None` and the
/// caller is expected to raise `DPY-4007`.
pub fn check_fetch_conversion(
    source: &ColumnMetadata,
    to_ora_type_num: u8,
    to_csfrm: u8,
) -> Option<ColumnMetadata> {
    const CHAR_TYPES: [u8; 3] = [ORA_TYPE_NUM_CHAR, ORA_TYPE_NUM_LONG, ORA_TYPE_NUM_VARCHAR];
    let from = source.ora_type_num;
    let to = to_ora_type_num;
    if from == to {
        return Some(source.clone());
    }
    let supported = match from {
        ORA_TYPE_NUM_BINARY_DOUBLE | ORA_TYPE_NUM_BINARY_FLOAT => {
            matches!(
                to,
                ORA_TYPE_NUM_BINARY_INTEGER
                    | ORA_TYPE_NUM_BINARY_DOUBLE
                    | ORA_TYPE_NUM_BINARY_FLOAT
                    | ORA_TYPE_NUM_NUMBER
            ) || CHAR_TYPES.contains(&to)
        }
        ORA_TYPE_NUM_BINARY_INTEGER => to == ORA_TYPE_NUM_NUMBER || CHAR_TYPES.contains(&to),
        ORA_TYPE_NUM_BLOB => {
            if matches!(to, ORA_TYPE_NUM_RAW | ORA_TYPE_NUM_LONG_RAW) {
                let mut metadata = source.clone();
                metadata.ora_type_num = ORA_TYPE_NUM_LONG_RAW;
                metadata.csfrm = 0;
                metadata.buffer_size = TNS_MAX_LONG_LENGTH;
                metadata.max_size = 0;
                return Some(metadata);
            }
            false
        }
        ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_LONG | ORA_TYPE_NUM_VARCHAR => {
            matches!(
                to,
                ORA_TYPE_NUM_BINARY_DOUBLE
                    | ORA_TYPE_NUM_BINARY_FLOAT
                    | ORA_TYPE_NUM_NUMBER
                    | ORA_TYPE_NUM_BINARY_INTEGER
            ) || CHAR_TYPES.contains(&to)
        }
        ORA_TYPE_NUM_CLOB => {
            if CHAR_TYPES.contains(&to) {
                let mut metadata = source.clone();
                metadata.ora_type_num = ORA_TYPE_NUM_LONG;
                metadata.buffer_size = TNS_MAX_LONG_LENGTH;
                metadata.max_size = 0;
                return Some(metadata);
            }
            false
        }
        ORA_TYPE_NUM_DATE
        | ORA_TYPE_NUM_TIMESTAMP
        | ORA_TYPE_NUM_TIMESTAMP_LTZ
        | ORA_TYPE_NUM_TIMESTAMP_TZ => {
            matches!(
                to,
                ORA_TYPE_NUM_DATE
                    | ORA_TYPE_NUM_TIMESTAMP
                    | ORA_TYPE_NUM_TIMESTAMP_LTZ
                    | ORA_TYPE_NUM_TIMESTAMP_TZ
            ) || CHAR_TYPES.contains(&to)
        }
        ORA_TYPE_NUM_INTERVAL_DS | ORA_TYPE_NUM_INTERVAL_YM | ORA_TYPE_NUM_ROWID => {
            CHAR_TYPES.contains(&to)
        }
        ORA_TYPE_NUM_NUMBER => {
            matches!(
                to,
                ORA_TYPE_NUM_BINARY_INTEGER
                    | ORA_TYPE_NUM_BINARY_DOUBLE
                    | ORA_TYPE_NUM_BINARY_FLOAT
            ) || CHAR_TYPES.contains(&to)
        }
        ORA_TYPE_NUM_JSON => {
            // Native JSON (DB_TYPE_JSON) fetched as a character type via an
            // output type handler. The reference defines the column to the
            // server as VARCHAR but decodes the returned bytes as LONG: "the
            // server won't accept LONG being defined but even so it still sends
            // back LONG data" (reference impl/base/var.pyx:208-215, where
            // `_fetch_metadata.dbtype = DB_TYPE_LONG` and `return
            // DB_TYPE_VARCHAR`).
            //
            // Our wire define writer keys the VARCHAR ora_type_num off this
            // metadata, so the server accepts the define and streams the OSON
            // image inline as text. The returned data is then decoded through
            // the same `read_bytes` path used for VARCHAR/CHAR/LONG (all three
            // share identical framing in `parse_column_value`), and the
            // non-zero LONG-sized `buffer_size` set here keeps the
            // null-by-describe shortcut from firing — exactly the effect the
            // reference obtains by decoding as LONG. The handler's outconverter
            // (e.g. `json.loads`) then materializes the Python value.
            if matches!(to, ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_VARCHAR) {
                let mut metadata = source.clone();
                metadata.ora_type_num = ORA_TYPE_NUM_VARCHAR;
                metadata.csfrm = CS_FORM_IMPLICIT;
                metadata.buffer_size = TNS_MAX_LONG_LENGTH;
                metadata.max_size = 0;
                return Some(metadata);
            }
            // JSON fetched as RAW/bytes decodes the OSON image bytes directly
            // (reference var.pyx:216-218 sets `_fetch_metadata.dbtype =
            // DB_TYPE_RAW`).
            if to == ORA_TYPE_NUM_RAW {
                let mut metadata = source.clone();
                metadata.ora_type_num = ORA_TYPE_NUM_RAW;
                metadata.csfrm = 0;
                metadata.buffer_size = TNS_MAX_LONG_LENGTH;
                metadata.max_size = 0;
                return Some(metadata);
            }
            false
        }
        ORA_TYPE_NUM_VECTOR => {
            // VECTOR fetched as a character type streams its JSON text via a
            // LONG wire define; VECTOR fetched as a CLOB streams via a CLOB
            // locator (reference var.pyx:234-243).
            if CHAR_TYPES.contains(&to) {
                let mut metadata = source.clone();
                metadata.ora_type_num = ORA_TYPE_NUM_LONG;
                metadata.csfrm = CS_FORM_IMPLICIT;
                metadata.buffer_size = TNS_MAX_LONG_LENGTH;
                metadata.max_size = 0;
                return Some(metadata);
            }
            if to == ORA_TYPE_NUM_CLOB {
                let mut metadata = source.clone();
                metadata.ora_type_num = ORA_TYPE_NUM_CLOB;
                return Some(metadata);
            }
            false
        }
        _ => false,
    };
    let _ = to_csfrm;
    if supported {
        Some(source.clone())
    } else {
        None
    }
}

pub fn public_dbtype_name_from_oracle_type_name(type_name: &str) -> &'static str {
    let upper = type_name.to_ascii_uppercase();
    if upper.starts_with("TIMESTAMP") {
        if upper.contains("LOCAL TIME ZONE") || upper.contains("LOCAL TZ") {
            return "DB_TYPE_TIMESTAMP_LTZ";
        }
        if upper.contains("TIME ZONE") || upper.contains("WITH TZ") {
            return "DB_TYPE_TIMESTAMP_TZ";
        }
        return "DB_TYPE_TIMESTAMP";
    }
    match upper.as_str() {
        "CHAR" => "DB_TYPE_CHAR",
        "NCHAR" => "DB_TYPE_NCHAR",
        "VARCHAR2" | "VARCHAR" => "DB_TYPE_VARCHAR",
        "NVARCHAR2" | "NVARCHAR" => "DB_TYPE_NVARCHAR",
        "RAW" => "DB_TYPE_RAW",
        "DATE" => "DB_TYPE_DATE",
        "TIMESTAMP" => "DB_TYPE_TIMESTAMP",
        "TIMESTAMP WITH TIME ZONE" | "TIMESTAMP WITH TZ" => "DB_TYPE_TIMESTAMP_TZ",
        "TIMESTAMP WITH LOCAL TIME ZONE" | "TIMESTAMP WITH LOCAL TZ" => "DB_TYPE_TIMESTAMP_LTZ",
        "CLOB" => "DB_TYPE_CLOB",
        "NCLOB" => "DB_TYPE_NCLOB",
        "BLOB" => "DB_TYPE_BLOB",
        "XMLTYPE" => "DB_TYPE_XMLTYPE",
        "BINARY_FLOAT" => "DB_TYPE_BINARY_FLOAT",
        "BINARY_DOUBLE" => "DB_TYPE_BINARY_DOUBLE",
        "NUMBER" | "INTEGER" | "SMALLINT" | "REAL" | "DOUBLE PRECISION" | "FLOAT" => {
            "DB_TYPE_NUMBER"
        }
        // PL/SQL scalar attribute/element type names returned verbatim by the
        // type catalog. Without these arms they would fall through to the ADT
        // fallback below and be misclassified as nested objects (reference
        // impl/base/types.pyx:154-175,451-455 db_type_by_ora_name).
        "BOOLEAN" | "PL/SQL BOOLEAN" => "DB_TYPE_BOOLEAN",
        "BINARY_INTEGER" | "PLS_INTEGER" | "PL/SQL BINARY INTEGER" | "PL/SQL PLS INTEGER" => {
            "DB_TYPE_BINARY_INTEGER"
        }
        "LONG" => "DB_TYPE_LONG",
        "LONG RAW" => "DB_TYPE_LONG_RAW",
        "ROWID" => "DB_TYPE_ROWID",
        "UROWID" => "DB_TYPE_UROWID",
        "BFILE" => "DB_TYPE_BFILE",
        "JSON" => "DB_TYPE_JSON",
        "VECTOR" => "DB_TYPE_VECTOR",
        "INTERVAL DAY TO SECOND" => "DB_TYPE_INTERVAL_DS",
        "INTERVAL YEAR TO MONTH" => "DB_TYPE_INTERVAL_YM",
        // An unknown name IS a nested object type (mirrors reference
        // _create_attr only calling get_type_for_info when type_owner is set).
        _ => "DB_TYPE_OBJECT",
    }
}

pub fn dbobject_attr_precision_scale(
    type_name: &str,
    precision: Option<i8>,
    scale: Option<i8>,
) -> (i8, i8) {
    match type_name.to_ascii_uppercase().as_str() {
        "NUMBER" => (
            precision.unwrap_or(if scale == Some(0) { 38 } else { 0 }),
            scale.unwrap_or(-127),
        ),
        "INTEGER" | "SMALLINT" => (precision.unwrap_or(38), scale.unwrap_or(0)),
        "REAL" => (precision.unwrap_or(63), scale.unwrap_or(-127)),
        "DOUBLE PRECISION" | "FLOAT" => (precision.unwrap_or(126), scale.unwrap_or(-127)),
        _ => (0, 0),
    }
}

pub fn dbobject_attr_max_size(type_name: &str, length: Option<u32>) -> u32 {
    let length = length.unwrap_or(0);
    match type_name.to_ascii_uppercase().as_str() {
        "NCHAR" | "NVARCHAR2" | "NVARCHAR" => length.saturating_mul(2),
        _ => length,
    }
}

pub fn dbobject_rowtype_attr_max_size(
    type_name: &str,
    data_length: Option<u32>,
    char_length: Option<u32>,
) -> u32 {
    match type_name.to_ascii_uppercase().as_str() {
        "CHAR" | "VARCHAR" | "VARCHAR2" | "RAW" => data_length.unwrap_or(0),
        "NCHAR" | "NVARCHAR" | "NVARCHAR2" => dbobject_attr_max_size(
            type_name,
            char_length.filter(|length| *length > 0).or(data_length),
        ),
        _ => 0,
    }
}

pub fn public_dbtype_name_from_bind(value: &BindValue) -> &'static str {
    match value {
        BindValue::TypedNull {
            ora_type_num,
            csfrm,
            ..
        }
        | BindValue::Output {
            ora_type_num,
            csfrm,
            ..
        }
        | BindValue::ReturnOutput {
            ora_type_num,
            csfrm,
            ..
        }
        | BindValue::Array {
            ora_type_num,
            csfrm,
            ..
        } => public_dbtype_name_from_type_info(*ora_type_num, *csfrm),
        BindValue::ObjectOutput { .. } | BindValue::ObjectInput { .. } => "DB_TYPE_OBJECT",
        BindValue::Text(_) => "DB_TYPE_VARCHAR",
        BindValue::Raw(_) => "DB_TYPE_RAW",
        BindValue::Lob {
            ora_type_num,
            csfrm,
            ..
        } => match (*ora_type_num, *csfrm) {
            (ORA_TYPE_NUM_BLOB, _) => "DB_TYPE_BLOB",
            (ORA_TYPE_NUM_CLOB, CS_FORM_NCHAR) => "DB_TYPE_NCLOB",
            (ORA_TYPE_NUM_CLOB, _) => "DB_TYPE_CLOB",
            _ => "DB_TYPE_CLOB",
        },
        BindValue::Number(_) => "DB_TYPE_NUMBER",
        BindValue::BinaryInteger(_) => "DB_TYPE_BINARY_INTEGER",
        BindValue::BinaryDouble(_) => "DB_TYPE_BINARY_DOUBLE",
        BindValue::BinaryFloat(_) => "DB_TYPE_BINARY_FLOAT",
        BindValue::Boolean(_) => "DB_TYPE_BOOLEAN",
        BindValue::IntervalDS { .. } => "DB_TYPE_INTERVAL_DS",
        BindValue::IntervalYM { .. } => "DB_TYPE_INTERVAL_YM",
        BindValue::DateTime { .. } => "DB_TYPE_DATE",
        BindValue::Timestamp { ora_type_num, .. } => match *ora_type_num {
            ORA_TYPE_NUM_TIMESTAMP_LTZ => "DB_TYPE_TIMESTAMP_LTZ",
            ORA_TYPE_NUM_TIMESTAMP_TZ => "DB_TYPE_TIMESTAMP_TZ",
            _ => "DB_TYPE_TIMESTAMP",
        },
        BindValue::Vector(_) => "DB_TYPE_VECTOR",
        BindValue::Json(_) => "DB_TYPE_JSON",
        BindValue::Cursor { .. } => "DB_TYPE_CURSOR",
        BindValue::Null => "DB_TYPE_VARCHAR",
    }
}

pub fn bind_template_from_type_name(type_name: &str, size: u32) -> BindValue {
    let text_buffer_size = if size == 0 { 4000 } else { size.max(1) };
    let nchar_buffer_size = text_buffer_size.saturating_mul(4);
    match type_name {
        "NUMBER" | "DB_TYPE_NUMBER" | "int" | "float" | "Decimal" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_NUMBER,
            csfrm: 0,
            buffer_size: ORA_TYPE_SIZE_NUMBER,
        },
        "NATIVE_INT" | "DB_TYPE_BINARY_INTEGER" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_BINARY_INTEGER,
            csfrm: 0,
            buffer_size: ORA_TYPE_SIZE_NUMBER,
        },
        "NATIVE_FLOAT" | "DB_TYPE_BINARY_DOUBLE" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_BINARY_DOUBLE,
            csfrm: 0,
            buffer_size: ORA_TYPE_SIZE_BINARY_DOUBLE,
        },
        "DB_TYPE_BINARY_FLOAT" | "BINARY_FLOAT" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_BINARY_FLOAT,
            csfrm: 0,
            buffer_size: ORA_TYPE_SIZE_BINARY_FLOAT,
        },
        "DB_TYPE_BOOLEAN" | "BOOLEAN" | "bool" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_BOOLEAN,
            csfrm: 0,
            buffer_size: ORA_TYPE_SIZE_BOOLEAN,
        },
        "DB_TYPE_INTERVAL_DS" | "INTERVAL DAY TO SECOND" | "timedelta" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_INTERVAL_DS,
            csfrm: 0,
            buffer_size: ORA_TYPE_SIZE_INTERVAL_DS,
        },
        "DB_TYPE_INTERVAL_YM" | "INTERVAL YEAR TO MONTH" | "IntervalYM" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_INTERVAL_YM,
            csfrm: 0,
            buffer_size: ORA_TYPE_SIZE_INTERVAL_YM,
        },
        "STRING" | "DB_TYPE_VARCHAR" | "DB_TYPE_CHAR" | "str" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_VARCHAR,
            csfrm: CS_FORM_IMPLICIT,
            buffer_size: text_buffer_size,
        },
        "DB_TYPE_NCHAR" | "DB_TYPE_NVARCHAR" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_VARCHAR,
            csfrm: CS_FORM_NCHAR,
            buffer_size: nchar_buffer_size,
        },
        "DB_TYPE_CLOB" | "CLOB" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_LONG,
            csfrm: CS_FORM_IMPLICIT,
            buffer_size: TNS_MAX_LONG_LENGTH,
        },
        "DB_TYPE_NCLOB" | "NCLOB" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_LONG,
            csfrm: CS_FORM_NCHAR,
            buffer_size: TNS_MAX_LONG_LENGTH,
        },
        "DB_TYPE_BLOB" | "BLOB" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_LONG_RAW,
            csfrm: 0,
            buffer_size: TNS_MAX_LONG_LENGTH,
        },
        "DB_TYPE_LONG" | "LONG" | "LONG_STRING" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_LONG,
            csfrm: CS_FORM_IMPLICIT,
            buffer_size: TNS_MAX_LONG_LENGTH,
        },
        "DB_TYPE_LONG_NVARCHAR" | "LONG NVARCHAR" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_LONG,
            csfrm: CS_FORM_NCHAR,
            buffer_size: TNS_MAX_LONG_LENGTH,
        },
        "DB_TYPE_LONG_RAW" | "LONG RAW" | "LONG_BINARY" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_LONG_RAW,
            csfrm: 0,
            buffer_size: TNS_MAX_LONG_LENGTH,
        },
        "DB_TYPE_RAW" | "BINARY" | "bytes" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_RAW,
            csfrm: 0,
            buffer_size: size.max(1).max(4000),
        },
        "ROWID" | "DB_TYPE_ROWID" | "DB_TYPE_UROWID" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_VARCHAR,
            csfrm: CS_FORM_IMPLICIT,
            buffer_size: 5267,
        },
        "DATETIME" | "DB_TYPE_DATE" | "date" | "datetime" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_DATE,
            csfrm: 0,
            buffer_size: ORA_TYPE_SIZE_DATE,
        },
        "DB_TYPE_TIMESTAMP" | "TIMESTAMP" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_TIMESTAMP,
            csfrm: 0,
            buffer_size: ORA_TYPE_SIZE_TIMESTAMP,
        },
        "DB_TYPE_TIMESTAMP_LTZ" | "TIMESTAMP WITH LOCAL TIME ZONE" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_TIMESTAMP_LTZ,
            csfrm: 0,
            buffer_size: ORA_TYPE_SIZE_TIMESTAMP,
        },
        "DB_TYPE_TIMESTAMP_TZ" | "TIMESTAMP WITH TIME ZONE" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_TIMESTAMP_TZ,
            csfrm: 0,
            buffer_size: ORA_TYPE_SIZE_TIMESTAMP_TZ,
        },
        "DB_TYPE_CURSOR" | "CURSOR" => cursor_bind_template(),
        "DB_TYPE_VECTOR" | "VECTOR" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_VECTOR,
            csfrm: 0,
            buffer_size: TNS_VECTOR_MAX_LENGTH,
        },
        "DB_TYPE_JSON" | "JSON" => BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_JSON,
            csfrm: 0,
            buffer_size: TNS_VECTOR_MAX_LENGTH,
        },
        _ => BindValue::Null,
    }
}

pub fn dbobject_element_bind_type_info(dbtype_name: &str, max_size: u32) -> BindTypeInfo {
    let buffer_size = max_size.max(1);
    let (ora_type_num, csfrm, buffer_size) = match dbtype_name {
        "DB_TYPE_NUMBER" => (ORA_TYPE_NUM_NUMBER, 0, ORA_TYPE_SIZE_NUMBER),
        "DB_TYPE_RAW" | "DB_TYPE_BLOB" => (ORA_TYPE_NUM_RAW, 0, buffer_size.max(4000)),
        "DB_TYPE_NCHAR" | "DB_TYPE_NVARCHAR" | "DB_TYPE_NCLOB" => {
            (ORA_TYPE_NUM_VARCHAR, CS_FORM_NCHAR, buffer_size.max(4000))
        }
        "DB_TYPE_DATE" => (ORA_TYPE_NUM_DATE, 0, ORA_TYPE_SIZE_DATE),
        "DB_TYPE_TIMESTAMP" => (ORA_TYPE_NUM_TIMESTAMP, 0, ORA_TYPE_SIZE_TIMESTAMP),
        "DB_TYPE_TIMESTAMP_LTZ" => (ORA_TYPE_NUM_TIMESTAMP_LTZ, 0, ORA_TYPE_SIZE_TIMESTAMP),
        "DB_TYPE_TIMESTAMP_TZ" => (ORA_TYPE_NUM_TIMESTAMP_TZ, 0, ORA_TYPE_SIZE_TIMESTAMP_TZ),
        _ => (
            ORA_TYPE_NUM_VARCHAR,
            CS_FORM_IMPLICIT,
            buffer_size.max(4000),
        ),
    };
    BindTypeInfo {
        ora_type_num,
        csfrm,
        buffer_size,
    }
}

pub(crate) fn public_dbtype_name_from_type_info(ora_type_num: u8, csfrm: u8) -> &'static str {
    match (ora_type_num, csfrm) {
        (ORA_TYPE_NUM_BINARY_DOUBLE, _) => "DB_TYPE_BINARY_DOUBLE",
        (ORA_TYPE_NUM_BINARY_FLOAT, _) => "DB_TYPE_BINARY_FLOAT",
        (ORA_TYPE_NUM_INTERVAL_DS, _) => "DB_TYPE_INTERVAL_DS",
        (ORA_TYPE_NUM_INTERVAL_YM, _) => "DB_TYPE_INTERVAL_YM",
        (ORA_TYPE_NUM_BOOLEAN, _) => "DB_TYPE_BOOLEAN",
        (ORA_TYPE_NUM_BINARY_INTEGER, _) => "DB_TYPE_BINARY_INTEGER",
        (ORA_TYPE_NUM_NUMBER, _) => "DB_TYPE_NUMBER",
        (ORA_TYPE_NUM_CHAR, CS_FORM_NCHAR) | (ORA_TYPE_NUM_VARCHAR, CS_FORM_NCHAR) => {
            "DB_TYPE_NVARCHAR"
        }
        (ORA_TYPE_NUM_CHAR, _) => "DB_TYPE_CHAR",
        (ORA_TYPE_NUM_VARCHAR, _) => "DB_TYPE_VARCHAR",
        (ORA_TYPE_NUM_LONG, CS_FORM_NCHAR) => "DB_TYPE_LONG_NVARCHAR",
        (ORA_TYPE_NUM_LONG, _) => "DB_TYPE_LONG",
        (ORA_TYPE_NUM_LONG_RAW, _) => "DB_TYPE_LONG_RAW",
        (ORA_TYPE_NUM_RAW, _) => "DB_TYPE_RAW",
        (ORA_TYPE_NUM_DATE, _) => "DB_TYPE_DATE",
        (ORA_TYPE_NUM_TIMESTAMP, _) => "DB_TYPE_TIMESTAMP",
        (ORA_TYPE_NUM_TIMESTAMP_LTZ, _) => "DB_TYPE_TIMESTAMP_LTZ",
        (ORA_TYPE_NUM_TIMESTAMP_TZ, _) => "DB_TYPE_TIMESTAMP_TZ",
        (ORA_TYPE_NUM_CURSOR, _) => "DB_TYPE_CURSOR",
        (ORA_TYPE_NUM_OBJECT, _) => "DB_TYPE_OBJECT",
        (ORA_TYPE_NUM_VECTOR, _) => "DB_TYPE_VECTOR",
        (ORA_TYPE_NUM_JSON, _) => "DB_TYPE_JSON",
        _ => "DB_TYPE_VARCHAR",
    }
}

pub(crate) fn bind_metadata(value: &BindValue) -> (u8, u8, u32) {
    bind_value_type_info(value)
        .map(|info| (info.ora_type_num, info.csfrm, info.buffer_size))
        .unwrap_or((ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT, 1))
}

pub(crate) fn write_bind_value(writer: &mut TtcWriter, value: &BindValue, csfrm: u8) -> Result<()> {
    match value {
        BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_CURSOR,
            ..
        } => {
            writer.write_u8(1);
            writer.write_u8(0);
            Ok(())
        }
        // A NULL BOOLEAN bind is encoded as the two raw bytes
        // [TNS_ESCAPE_CHAR, 1], not the usual single 0 null indicator; sending
        // a plain 0 makes the server reject a PL/SQL BOOLEAN parameter with
        // PLS-00306 (reference messages/base.pyx _write_bind_params_column).
        BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_BOOLEAN,
            ..
        } => {
            writer.write_u8(TNS_ESCAPE_CHAR);
            writer.write_u8(1);
            Ok(())
        }
        BindValue::Null | BindValue::TypedNull { .. } => {
            writer.write_u8(0);
            Ok(())
        }
        BindValue::Output { .. } | BindValue::ReturnOutput { .. } => {
            writer.write_u8(0);
            Ok(())
        }
        BindValue::ObjectOutput { .. } => {
            // NULL object image (empty OUT bind): reference messages/base.pyx
            // 1462-1468.
            writer.write_ub4(0);
            writer.write_ub4(0);
            writer.write_ub4(0);
            writer.write_ub2(0);
            writer.write_ub4(0);
            writer.write_ub4(TNS_OBJ_TOP_LEVEL);
            Ok(())
        }
        BindValue::ObjectInput { oid, image, .. } => write_dbobject_bind(writer, oid, image),
        BindValue::Text(value) => {
            let bytes = encode_text_value(value, csfrm);
            writer.write_bytes_with_length(&bytes)
        }
        BindValue::Raw(value) => writer.write_bytes_with_length(value),
        BindValue::Lob { locator, .. } => writer.write_bytes_with_two_lengths(Some(locator)),
        BindValue::Number(value) | BindValue::BinaryInteger(value) => {
            let bytes = encode_number_text(value)?;
            writer.write_bytes_with_length(&bytes)
        }
        // reference encode_boolean (impl/base/encoders.pyx:99-111): true is
        // the two bytes [1, 1]; false is the single byte [0]
        BindValue::Boolean(value) => {
            let bytes: &[u8] = if *value { &[1, 1] } else { &[0] };
            writer.write_bytes_with_length(bytes)
        }
        BindValue::BinaryDouble(value) => {
            let bytes = encode_binary_double(*value);
            writer.write_bytes_with_length(&bytes)
        }
        BindValue::BinaryFloat(value) => {
            let bytes = encode_binary_float(*value as f32);
            writer.write_bytes_with_length(&bytes)
        }
        BindValue::IntervalDS {
            days,
            seconds,
            microseconds,
        } => {
            let nanoseconds = microseconds
                .checked_mul(1000)
                .ok_or(ProtocolError::TtcDecode(
                    "INTERVAL DS fractional seconds out of range",
                ))?;
            let bytes = encode_interval_ds(*days, *seconds, nanoseconds)?;
            writer.write_bytes_with_length(&bytes)
        }
        BindValue::IntervalYM { years, months } => {
            let bytes = encode_interval_ym(*years, *months)?;
            writer.write_bytes_with_length(&bytes)
        }
        BindValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
        } => {
            let bytes = encode_oracle_date(*year, *month, *day, *hour, *minute, *second)?;
            writer.write_bytes_with_length(&bytes)
        }
        BindValue::Timestamp {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
            ora_type_num,
        } => {
            let bytes = if matches!(*ora_type_num, ORA_TYPE_NUM_TIMESTAMP_TZ) {
                encode_oracle_timestamp_tz(
                    *year,
                    *month,
                    *day,
                    *hour,
                    *minute,
                    *second,
                    *nanosecond,
                )?
            } else {
                encode_oracle_timestamp(*year, *month, *day, *hour, *minute, *second, *nanosecond)?
            };
            writer.write_bytes_with_length(&bytes)
        }
        BindValue::Array {
            values,
            csfrm: array_csfrm,
            ..
        } => {
            writer.write_ub4(u32::try_from(values.len()).map_err(|_| {
                ProtocolError::InvalidPacketLength {
                    length: values.len(),
                    minimum: 0,
                }
            })?);
            for value in values {
                match value {
                    Some(value) => write_bind_value(writer, value, *array_csfrm)?,
                    None => writer.write_u8(0),
                }
            }
            Ok(())
        }
        // reference WriteBuffer.write_vector: a QLocator carrying the image
        // length, then the image bytes-with-length
        BindValue::Vector(vector) => {
            let image = crate::vector::encode_vector(vector);
            crate::vector::write_vector_image(writer, &image)
        }
        // reference WriteBuffer.write_oson: a QLocator carrying the OSON image
        // length, then the image bytes-with-length (same framing as VECTOR).
        BindValue::Json(image) => crate::vector::write_vector_image(writer, image),
        BindValue::Cursor { cursor_id } => {
            if *cursor_id == 0 {
                writer.write_u8(1);
                writer.write_u8(0);
            } else {
                writer.write_ub4(1);
                writer.write_ub4(*cursor_id);
            }
            Ok(())
        }
    }
}

pub(crate) fn encode_text_value(value: &str, csfrm: u8) -> Vec<u8> {
    if csfrm == CS_FORM_NCHAR {
        let mut bytes = Vec::with_capacity(value.len().saturating_mul(2));
        for unit in value.encode_utf16() {
            bytes.extend_from_slice(&unit.to_be_bytes());
        }
        bytes
    } else {
        value.as_bytes().to_vec()
    }
}
