use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, TimeUnit};

use oracledb_protocol::thin::{
    ColumnMetadata, CS_FORM_NCHAR, ORA_TYPE_NUM_BINARY_DOUBLE, ORA_TYPE_NUM_BINARY_FLOAT,
    ORA_TYPE_NUM_BLOB, ORA_TYPE_NUM_BOOLEAN, ORA_TYPE_NUM_CHAR, ORA_TYPE_NUM_CLOB,
    ORA_TYPE_NUM_DATE, ORA_TYPE_NUM_LONG, ORA_TYPE_NUM_LONG_RAW, ORA_TYPE_NUM_NUMBER,
    ORA_TYPE_NUM_RAW, ORA_TYPE_NUM_TIMESTAMP, ORA_TYPE_NUM_TIMESTAMP_LTZ,
    ORA_TYPE_NUM_TIMESTAMP_TZ, ORA_TYPE_NUM_VARCHAR, TNS_MAX_LONG_LENGTH,
};
use oracledb_protocol::vector::{
    VECTOR_FORMAT_BINARY, VECTOR_FORMAT_FLOAT32, VECTOR_FORMAT_FLOAT64, VECTOR_FORMAT_INT8,
};

use super::{ArrowConversionError, ArrowFetchOptions, Result, VectorFormat};

pub(super) const ORA_TYPE_NUM_VECTOR: u8 = 127;
const ORA_TYPE_NUM_JSON: u8 = 119;
const ORA_TYPE_NUM_INTERVAL_YM: u8 = 182;
const ORA_TYPE_NUM_INTERVAL_DS: u8 = 183;

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

/// describe `vector_flags` bit set for sparse vectors (reference
/// `VECTOR_META_FLAG_SPARSE_VECTOR`, constants.py:96). `0x01` is
/// `VECTOR_META_FLAG_FLEXIBLE_DIM` (handled by `List` allowing varying lengths).
pub(super) const VECTOR_META_FLAG_SPARSE_VECTOR: u8 = 0x02;

/// Maps a VECTOR column's describe metadata to its Arrow `DataType`
/// (metadata.pyx `_create_arrow_schema`, lines 83-97).
///
/// `vector_format == 0` is the flexible (unspecified) format Oracle reports
/// when a query produces vectors of differing element formats; the reference
/// raises `ERR_ARROW_UNSUPPORTED_VECTOR_FORMAT` (our DPY-3031).
fn vector_data_type(column: &ColumnMetadata, options: &ArrowFetchOptions) -> Result<DataType> {
    let format = match column.vector_format() {
        VECTOR_FORMAT_FLOAT32 => VectorFormat::Float32,
        VECTOR_FORMAT_FLOAT64 => VectorFormat::Float64,
        VECTOR_FORMAT_INT8 => VectorFormat::Int8,
        VECTOR_FORMAT_BINARY => VectorFormat::Binary,
        // 0 == flexible format -> DPY-3031.
        _ => return Err(ArrowConversionError::UnsupportedVectorFormat),
    };
    let sparse = column.vector_flags() & VECTOR_META_FLAG_SPARSE_VECTOR != 0;
    // Opt-in FixedSizeList upgrade (bead a4-0mk): only dense, concretely-sized
    // vectors qualify. `vector_dimensions()` is `Some(dim)` only when the server
    // described a fixed dimension (flexible-dim columns report `None`); sparse
    // vectors keep the struct mapping. Anything ineligible falls through to the
    // default `List`/`Struct` mapping, so the default behavior is unchanged.
    if options.vector_fixed_size_list() && !sparse {
        if let Some(dim) = column.vector_dimensions() {
            if dim > 0 {
                if let Ok(len) = i32::try_from(dim) {
                    return Ok(vector_fixed_size_list_type(format, len));
                }
            }
        }
    }
    Ok(vector_arrow_type(format, sparse))
}

/// Arrow `FixedSizeList(element, dim)` for a dense fixed-dimension VECTOR column
/// (bead a4-0mk). The element type matches [`vector_arrow_type`]'s dense child
/// so values decode identically to the `List` path; only the outer list type
/// differs (fixed vs variable length).
pub fn vector_fixed_size_list_type(format: VectorFormat, dim: i32) -> DataType {
    let element = match format {
        VectorFormat::Float32 => DataType::Float32,
        VectorFormat::Float64 => DataType::Float64,
        VectorFormat::Int8 => DataType::Int8,
        VectorFormat::Binary => DataType::UInt8,
    };
    DataType::FixedSizeList(Arc::new(Field::new("item", element, true)), dim)
}

/// Reference-style `DB_TYPE_*` name for a fetched column (used in DPY-3030 /
/// DPY-3038 message parity).
pub fn db_type_name(column: &ColumnMetadata) -> String {
    let nchar = column.csfrm() == CS_FORM_NCHAR;
    let name = match column.ora_type_num() {
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
        column.ora_type_num(),
        ORA_TYPE_NUM_VARCHAR | ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_LONG
    )
}

fn is_datetime_like(column: &ColumnMetadata) -> bool {
    matches!(
        column.ora_type_num(),
        ORA_TYPE_NUM_DATE
            | ORA_TYPE_NUM_TIMESTAMP
            | ORA_TYPE_NUM_TIMESTAMP_TZ
            | ORA_TYPE_NUM_TIMESTAMP_LTZ
    )
}

/// Default DB->Arrow type for one fetched column
/// (metadata.pyx `_create_arrow_schema`).
fn default_arrow_type(column: &ColumnMetadata, options: &ArrowFetchOptions) -> Result<DataType> {
    match column.ora_type_num() {
        ORA_TYPE_NUM_NUMBER => {
            if options.fetch_decimals && (1..=38).contains(&column.precision()) {
                Ok(DataType::Decimal128(
                    column.precision() as u8,
                    column.scale(),
                ))
            } else if !options.fetch_decimals
                && column.scale() == 0
                && (1..=18).contains(&column.precision())
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
            let unit = match column.scale() {
                1..=3 => TimeUnit::Millisecond,
                4..=6 => TimeUnit::Microsecond,
                7..=9 => TimeUnit::Nanosecond,
                _ => TimeUnit::Second,
            };
            Ok(DataType::Timestamp(unit, None))
        }
        ORA_TYPE_NUM_VECTOR => vector_data_type(column, options),
        _ => Err(ArrowConversionError::UnsupportedDataType {
            db_type_name: db_type_name(column),
        }),
    }
}

/// Reference conversion matrix for `requested_schema`
/// (metadata.pyx `check_convert_to_arrow`). Note that string_view /
/// binary_view are NOT accepted on the fetch side.
fn check_convert_to_arrow(column: &ColumnMetadata, requested: &DataType) -> Result<()> {
    let ok = match column.ora_type_num() {
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

/// Computes the Arrow schema produced by
/// [`build_record_batch`](crate::arrow::build_record_batch) for the given fetch
/// metadata and options.
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
        fields.push(Field::new(column.name(), data_type, true));
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
            match column.ora_type_num() {
                ORA_TYPE_NUM_CLOB => {
                    column = column
                        .with_ora_type_num(ORA_TYPE_NUM_LONG)
                        .with_buffer_size(TNS_MAX_LONG_LENGTH)
                        .with_max_size(TNS_MAX_LONG_LENGTH);
                }
                ORA_TYPE_NUM_BLOB => {
                    column = column
                        .with_ora_type_num(ORA_TYPE_NUM_LONG_RAW)
                        .with_csfrm(0)
                        .with_buffer_size(TNS_MAX_LONG_LENGTH)
                        .with_max_size(TNS_MAX_LONG_LENGTH);
                }
                _ => {}
            }
            column
        })
        .collect()
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
            column.ora_type_num(),
            ORA_TYPE_NUM_RAW | ORA_TYPE_NUM_LONG_RAW
        ),
        DataType::Boolean => column.ora_type_num() == ORA_TYPE_NUM_BOOLEAN,
        DataType::Decimal128(_, _)
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => column.ora_type_num() == ORA_TYPE_NUM_NUMBER,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None) => {
            is_datetime_like(column)
        }
        DataType::Float32 | DataType::Float64 => matches!(
            column.ora_type_num(),
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
