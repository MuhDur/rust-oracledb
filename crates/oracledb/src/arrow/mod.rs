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

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use oracledb_protocol::thin::{ColumnMetadata, QueryValue};

mod builders;
mod direct_path;
mod ipc;
mod schema;

pub use builders::{
    build_record_batch, build_record_batch_columnar, build_record_batch_with_schema,
    decimal128_to_string,
};
pub use direct_path::record_batch_to_direct_path_rows;
pub use ipc::record_batch_to_ipc;
pub use schema::{
    arrow_define_columns, arrow_schema_for_columns, arrow_type_name, check_convert_from_arrow,
    db_type_name, vector_arrow_type, vector_fixed_size_list_type,
};

use builders::{columnar_supported, ColumnarBatchBuilder};

/// Errors raised by the fetch->Arrow and Arrow->bind conversion paths.
///
/// Messages are prefixed with the python-oracledb error number they
/// correspond to so the shim layer can surface exact reference errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
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
        "DPY-2065: Apache Arrow format does not support sparse vectors with \
         flexible dimensions"
    )]
    SparseVectorNotAllowed,
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
    #[error("DPY-4038: integer {value} cannot be represented as Apache Arrow type {arrow_type}")]
    InvalidInteger { value: String, arrow_type: String },
    #[error(
        "DPY-4040: value of length {actual_len} does not match the Apache Arrow \
         fixed size binary length of {fixed_size_len}"
    )]
    FixedSizeBinaryViolated {
        actual_len: usize,
        fixed_size_len: usize,
    },
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
    /// A wire-decode failure surfaced while streaming the borrowed fetch batch
    /// into the columnar Arrow builders (`build_record_batch_columnar`). The
    /// `for_each_row_ref` decode is generic over `E: From<ProtocolError>`; this
    /// carries that error into the Arrow conversion error type.
    #[error(transparent)]
    Protocol(#[from] oracledb_protocol::ProtocolError),
}

type Result<T> = std::result::Result<T, ArrowConversionError>;

/// Options controlling the fetch->Arrow conversion.
#[derive(Clone, Debug, Default)]
pub struct ArrowFetchOptions {
    /// `fetch_decimals` semantics: NUMBER columns with `0 < precision <= 38`
    /// become `decimal128(precision, scale)` instead of int64/float64.
    fetch_decimals: bool,
    /// Caller-requested output schema (`fetch_df_*(requested_schema=...)`).
    /// Must have exactly one field per fetched column; renames the output
    /// columns and coerces values per the reference conversion matrix.
    requested_schema: Option<SchemaRef>,
    /// Opt-in: represent dense, fixed-dimension VECTOR columns as an Arrow
    /// `FixedSizeList(element, dim)` instead of the default `List(element)`.
    ///
    /// OFF by default so the schema stays byte-identical to python-oracledb
    /// (which always emits `List`). When ON, it only upgrades columns that the
    /// server described with a concrete `vector_dimensions` AND that are dense
    /// (non-sparse) with a fixed element format; flexible-dimension, sparse, or
    /// flexible-format vectors keep their existing `List`/`Struct` mapping.
    vector_fixed_size_list: bool,
}

impl ArrowFetchOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn fetch_decimals(&self) -> bool {
        self.fetch_decimals
    }

    #[must_use]
    pub fn with_fetch_decimals(mut self, enabled: bool) -> Self {
        self.fetch_decimals = enabled;
        self
    }

    pub fn requested_schema(&self) -> Option<&SchemaRef> {
        self.requested_schema.as_ref()
    }

    #[must_use]
    pub fn with_requested_schema(mut self, schema: SchemaRef) -> Self {
        self.requested_schema = Some(schema);
        self
    }

    pub fn vector_fixed_size_list(&self) -> bool {
        self.vector_fixed_size_list
    }

    /// Opt into `FixedSizeList(element, dim)` for dense fixed-dimension VECTOR
    /// columns. See [`ArrowFetchOptions::vector_fixed_size_list`] for the exact
    /// eligibility rules; the default (`false`) preserves the `List` mapping.
    #[must_use]
    pub fn with_vector_fixed_size_list(mut self, enabled: bool) -> Self {
        self.vector_fixed_size_list = enabled;
        self
    }
}

/// Oracle VECTOR storage formats (reference `VECTOR_FORMAT_*`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VectorFormat {
    Float32,
    Float64,
    Int8,
    Binary,
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
        // Capture warmth BEFORE the execute: a cold (freshly parsed) VECTOR/JSON/
        // LOB cursor needs a define-fetch first (bead a4-0mk), while a warm cached
        // cursor keeps the server-side define from an earlier fetch.
        let warm = self.statement_has_cached_cursor(sql);
        let mut result = self
            .execute_query_with_bind_rows_and_options_core(
                cx,
                sql,
                size,
                &[],
                crate::ExecuteOptions::default(),
            )
            .await?;
        require_result_set(&result.columns)?;
        self.establish_cold_define(cx, warm, &mut result, size)
            .await?;
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
        // Release the fully-drained cursor back to the statement cache so a
        // re-execute of the same SQL reuses the open server cursor (a repeated
        // `fetch_df_all` previously parsed a fresh copy each call and never
        // released it, leaking one cursor per call -> ORA-01000 over a long run).
        self.release_cursor(cursor_id);
        Ok(build_record_batch(&columns, &rows, options)?)
    }

    /// Columnar `fetch_df_all` (bead rust-oracledb-wf7): executes `sql` and
    /// returns the full result as a single [`RecordBatch`], decoded DIRECTLY
    /// into per-column Arrow builders — the first execute page (owned) plus every
    /// subsequent fetch page (borrowed, zero-copy) stream straight into the
    /// builders, so no page is ever materialized into a
    /// `Vec<Vec<Option<QueryValue>>>` and there is no transpose pass.
    ///
    /// The produced batch is byte-identical to
    /// [`fetch_all_record_batch`](Self::fetch_all_record_batch) (the row path);
    /// see the `arrow_columnar_equals_row_path` differential test. Dense fixed-
    /// dimension VECTOR columns (mapped to `FixedSizeList`) decode DIRECTLY into a
    /// contiguous child buffer on this path (bead rust-oracledb-0mk). Only
    /// flexible-dimension (`List`) or sparse (`Struct`) VECTOR columns fall back
    /// to the row path — callers always get the same `RecordBatch` either way.
    pub async fn fetch_all_record_batch_columnar(
        &mut self,
        cx: &asupersync::Cx,
        sql: &str,
        fetch_array_size: u32,
        options: &ArrowFetchOptions,
    ) -> crate::Result<RecordBatch> {
        let size = fetch_array_size.max(1);
        // See `fetch_all_record_batch`: a cold define-requiring query (VECTOR,
        // which falls back to the row path below) needs its client-side define
        // established before the first fetch (bead a4-0mk).
        let warm = self.statement_has_cached_cursor(sql);
        let mut result = self
            .execute_query_with_bind_rows_and_options_core(
                cx,
                sql,
                size,
                &[],
                crate::ExecuteOptions::default(),
            )
            .await?;
        require_result_set(&result.columns)?;
        self.establish_cold_define(cx, warm, &mut result, size)
            .await?;
        let columns = std::mem::take(&mut result.columns);
        let cursor_id = result.cursor_id;
        let schema = Arc::new(arrow_schema_for_columns(&columns, options)?);

        // Dense fixed-dimension VECTOR columns (FixedSizeList) ARE columnar-
        // handled (bead rust-oracledb-0mk): they stream straight into a
        // contiguous child buffer. Only flexible-dimension (List) or sparse
        // (Struct) VECTOR columns are not columnar-handled and fall back to the
        // fully-tested row path for the whole query so the result is identical.
        if !columnar_supported(&schema) {
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
            return Ok(build_record_batch_with_schema(schema, &columns, &rows)?);
        }

        let capacity = result.rows.len().max(size as usize);
        // `columnar_supported` upstream guarantees every column builds; surface a
        // future guard/builder mismatch as an error instead of panicking.
        let mut builder = ColumnarBatchBuilder::new(schema, columns.clone(), capacity).ok_or(
            ArrowConversionError::NotImplemented("columnar path does not support this column type"),
        )?;

        // First page arrived owned from the execute round trip.
        builder.append_owned(&result.rows)?;
        let mut more_rows = result.more_rows;
        // Track the last owned row so duplicate-column compression on the next
        // page resolves against it (mirrors the row path's `previous` seed). The
        // borrowed fetch carries the seed internally via `previous_row`.
        let mut previous: Option<Vec<Option<QueryValue>>> = result.rows.last().cloned();
        while more_rows && cursor_id != 0 {
            let page = self
                .fetch_rows_ref(cx, cursor_id, size, previous.as_deref())
                .await?;
            more_rows = page.more_rows;
            // Stream the page into the builders; `append_borrowed` returns the
            // page's last row owned (one alloc/page) for the next page's seed.
            if let Some(last) = builder.append_borrowed(&page.batch)? {
                previous = Some(last);
            }
        }
        // The cursor is now fully drained (`more_rows == false`): release it back
        // to the statement cache so a re-execute of the same SQL reuses the open
        // server cursor instead of parsing a fresh copy (mirrors the `fetch_all`
        // drain helper; keeps long-running callers from exhausting open_cursors).
        self.release_cursor(cursor_id);
        Ok(builder.finish()?)
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
        // See `fetch_all_record_batch`: establish the client-side define for a
        // cold define-requiring query before the first batch (bead a4-0mk).
        let warm = self.statement_has_cached_cursor(sql);
        let mut result = self
            .execute_query_with_bind_rows_and_options_core(
                cx,
                sql,
                size,
                &[],
                crate::ExecuteOptions::default(),
            )
            .await?;
        require_result_set(&result.columns)?;
        self.establish_cold_define(cx, warm, &mut result, size)
            .await?;
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

    /// Synchronous columnar `fetch_df_all` (bead wf7). Byte-identical to
    /// [`fetch_all_record_batch`](Self::fetch_all_record_batch) but decodes
    /// straight into per-column Arrow builders (no row materialization). Falls
    /// back to the row path transparently for VECTOR columns.
    pub fn fetch_all_record_batch_columnar(
        connection: &mut crate::Connection,
        sql: &str,
        fetch_array_size: u32,
        options: &ArrowFetchOptions,
    ) -> crate::Result<RecordBatch> {
        crate::block_on_connection(move |cx| async move {
            connection
                .fetch_all_record_batch_columnar(&cx, sql, fetch_array_size, options)
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
    use super::builders::decimal128_from_number_text;
    use super::schema::VECTOR_META_FLAG_SPARSE_VECTOR;
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{
        Date32Type, Decimal128Type, Float32Type, Float64Type, Int64Type, TimestampMicrosecondType,
        TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt8Type,
    };
    use arrow_array::{Array, ArrayRef, StructArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use oracledb_protocol::dpl::DirectPathColumnValue;
    use oracledb_protocol::thin::{
        ORA_TYPE_NUM_BINARY_DOUBLE, ORA_TYPE_NUM_BINARY_FLOAT, ORA_TYPE_NUM_BLOB,
        ORA_TYPE_NUM_CHAR, ORA_TYPE_NUM_CLOB, ORA_TYPE_NUM_DATE, ORA_TYPE_NUM_LONG,
        ORA_TYPE_NUM_LONG_RAW, ORA_TYPE_NUM_NUMBER, ORA_TYPE_NUM_RAW, ORA_TYPE_NUM_TIMESTAMP,
        ORA_TYPE_NUM_TIMESTAMP_TZ, ORA_TYPE_NUM_VARCHAR,
    };
    use oracledb_protocol::vector::{
        Vector, VectorValues, VECTOR_FORMAT_BINARY, VECTOR_FORMAT_FLOAT32, VECTOR_FORMAT_FLOAT64,
    };

    fn column(name: &str, ora_type_num: u8, precision: i8, scale: i8) -> ColumnMetadata {
        ColumnMetadata::new(name, ora_type_num)
            .with_precision(precision)
            .with_scale(scale)
            .with_nulls_allowed(true)
    }

    fn number(text: &str) -> Option<QueryValue> {
        Some(QueryValue::number_from_text(text, !text.contains('.')))
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

    fn timestamp_tz(
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        nanosecond: u32,
        offset_minutes: i32,
    ) -> Option<QueryValue> {
        Some(QueryValue::TimestampTz {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
            offset_minutes,
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
        let options = ArrowFetchOptions::new().with_fetch_decimals(true);
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
    fn timestamp_tz_maps_to_arrow_epoch_once() {
        let columns = vec![column("TSTZ", ORA_TYPE_NUM_TIMESTAMP_TZ, 0, 9)];
        let rows = vec![vec![timestamp_tz(2024, 1, 2, 3, 4, 5, 123_456_789, -330)]];
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
            1_704_184_445_123_456_789
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

    fn vector_column(name: &str, vector_format: u8, vector_flags: u8) -> ColumnMetadata {
        column(name, 127, 0, 0)
            .with_vector_format(vector_format)
            .with_vector_flags(vector_flags)
    }

    #[test]
    fn flexible_vector_format_raises_dpy_3031() {
        // vector_format == 0 is the flexible format Oracle reports when a query
        // yields vectors of differing element formats (test_9107).
        let columns = vec![vector_column("V", 0, 0)];
        let err = build_record_batch(&columns, &[], &ArrowFetchOptions::default())
            .expect_err("flexible vector format must error");
        assert!(
            matches!(err, ArrowConversionError::UnsupportedVectorFormat),
            "expected DPY-3031, got {err}"
        );
        assert!(err.to_string().starts_with("DPY-3031:"));
    }

    #[test]
    fn dense_float32_vector_builds_list_array_with_nulls() {
        let columns = vec![vector_column("V", VECTOR_FORMAT_FLOAT32, 0)];
        let rows = vec![
            vec![Some(QueryValue::Vector(Box::new(Vector::Dense(
                VectorValues::Float32(vec![34.6, 77.8]),
            ))))],
            vec![None],
            vec![Some(QueryValue::Vector(Box::new(Vector::Dense(
                VectorValues::Float32(vec![34.6, 77.8, 55.9]),
            ))))],
        ];
        let batch =
            build_record_batch(&columns, &rows, &ArrowFetchOptions::default()).expect("batch");
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::List(Arc::new(Field::new("item", DataType::Float32, true)))
        );
        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .expect("list array");
        assert_eq!(list.len(), 3);
        assert!(list.is_null(1));
        let row0 = list.value(0);
        let row0 = row0.as_primitive::<Float32Type>();
        assert_eq!(row0.values(), &[34.6_f32, 77.8]);
        let row2 = list.value(2);
        let row2 = row2.as_primitive::<Float32Type>();
        assert_eq!(row2.values(), &[34.6_f32, 77.8, 55.9]);
    }

    #[test]
    fn fixed_size_list_opt_in_upgrades_dense_fixed_dim_vector() {
        use arrow_array::FixedSizeListArray;

        // Dense float32 vector column the server described with a fixed dim of 3.
        let columns =
            vec![vector_column("V", VECTOR_FORMAT_FLOAT32, 0).with_vector_dimensions(Some(3))];
        let rows = vec![
            vec![Some(QueryValue::Vector(Box::new(Vector::Dense(
                VectorValues::Float32(vec![1.0, 2.0, 3.0]),
            ))))],
            vec![None],
            vec![Some(QueryValue::Vector(Box::new(Vector::Dense(
                VectorValues::Float32(vec![4.0, 5.0, 6.0]),
            ))))],
        ];

        // Default: unchanged List mapping (python-oracledb parity).
        let default_batch =
            build_record_batch(&columns, &rows, &ArrowFetchOptions::default()).expect("batch");
        assert_eq!(
            default_batch.schema().field(0).data_type(),
            &DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            "default must keep List mapping"
        );

        // Opt-in: FixedSizeList(Float32, 3).
        let options = ArrowFetchOptions::new().with_vector_fixed_size_list(true);
        let batch = build_record_batch(&columns, &rows, &options).expect("batch");
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3)
        );

        let fsl = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("fixed size list array");
        assert_eq!(fsl.len(), 3);
        assert!(fsl.is_null(1));
        // Values round-trip identical to the row (List) path, element for element.
        let list = default_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .expect("list array");
        for row in [0usize, 2] {
            let fsl_row = fsl.value(row);
            let list_row = list.value(row);
            assert_eq!(
                fsl_row.as_primitive::<Float32Type>().values(),
                list_row.as_primitive::<Float32Type>().values(),
                "row {row} values must match the List path"
            );
        }
        assert_eq!(
            fsl.value(0).as_primitive::<Float32Type>().values(),
            &[1.0_f32, 2.0, 3.0]
        );
        assert_eq!(
            fsl.value(2).as_primitive::<Float32Type>().values(),
            &[4.0_f32, 5.0, 6.0]
        );
    }

    #[test]
    fn fixed_size_list_opt_in_keeps_list_for_flexible_dim() {
        // Flag ON but the column has no concrete dimension (flexible-dim vector):
        // the mapping must stay List, never a fixed-size list.
        let columns = vec![vector_column("V", VECTOR_FORMAT_FLOAT32, 0)];
        let options = ArrowFetchOptions::new().with_vector_fixed_size_list(true);
        let schema = arrow_schema_for_columns(&columns, &options).expect("schema");
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            "flexible-dim vector must keep List even with the flag on"
        );
    }

    #[test]
    fn fixed_size_list_opt_in_rejects_wrong_length_vector() {
        // A stored vector whose element count disagrees with the described fixed
        // dimension is a server inconsistency: fail closed, never pad/truncate.
        let columns =
            vec![vector_column("V", VECTOR_FORMAT_FLOAT32, 0).with_vector_dimensions(Some(3))];
        let rows = vec![vec![Some(QueryValue::Vector(Box::new(Vector::Dense(
            VectorValues::Float32(vec![1.0, 2.0]),
        ))))]];
        let options = ArrowFetchOptions::new().with_vector_fixed_size_list(true);
        let err =
            build_record_batch(&columns, &rows, &options).expect_err("length mismatch must error");
        assert!(
            err.to_string().contains("fixed dimension"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn sparse_float64_vector_builds_struct_array_with_nulls() {
        let columns = vec![vector_column(
            "V",
            VECTOR_FORMAT_FLOAT64,
            VECTOR_META_FLAG_SPARSE_VECTOR,
        )];
        let rows = vec![
            vec![Some(QueryValue::Vector(Box::new(Vector::Sparse {
                num_dimensions: 8,
                indices: vec![0, 7],
                values: VectorValues::Float64(vec![34.6, 77.8]),
            })))],
            vec![None],
            vec![Some(QueryValue::Vector(Box::new(Vector::Sparse {
                num_dimensions: 8,
                indices: vec![0, 7],
                values: VectorValues::Float64(vec![34.6, 9.1]),
            })))],
        ];
        let batch =
            build_record_batch(&columns, &rows, &ArrowFetchOptions::default()).expect("batch");
        let st = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct array");
        assert_eq!(st.len(), 3);
        assert!(st.is_null(1));
        let dims = st
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("num_dimensions");
        assert_eq!(dims.value(0), 8);
        assert!(dims.is_null(1));
        let idx = st
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .expect("indices");
        let idx0 = idx.value(0);
        assert_eq!(idx0.as_primitive::<UInt32Type>().values(), &[0_u32, 7]);
        let vals = st
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .expect("values");
        let vals2 = vals.value(2);
        assert_eq!(vals2.as_primitive::<Float64Type>().values(), &[34.6, 9.1]);
    }

    #[test]
    fn binary_vector_builds_uint8_list_per_byte() {
        // BINARY vector bytes are NOT bit-unpacked: 3 bytes -> 3 UInt8 elements
        // (test_9103 expects [3, 2, 3] from a 24-bit binary vector).
        let columns = vec![vector_column("V", VECTOR_FORMAT_BINARY, 0)];
        let rows = vec![vec![Some(QueryValue::Vector(Box::new(Vector::Dense(
            VectorValues::Binary(vec![3, 2, 3]),
        ))))]];
        let batch =
            build_record_batch(&columns, &rows, &ArrowFetchOptions::default()).expect("batch");
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::List(Arc::new(Field::new("item", DataType::UInt8, true)))
        );
        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .expect("list array");
        let row0 = list.value(0);
        assert_eq!(row0.as_primitive::<UInt8Type>().values(), &[3_u8, 2, 3]);
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
        let options = ArrowFetchOptions::new().with_requested_schema(requested);
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
        let options = ArrowFetchOptions::new().with_requested_schema(requested);
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
        let options = ArrowFetchOptions::new().with_requested_schema(requested);
        let columns = vec![column("N", ORA_TYPE_NUM_NUMBER, 9, 0)];
        let rows = vec![vec![number("300")]];
        let err = build_record_batch(&columns, &rows, &options).expect_err("must overflow");
        // A valid integer out of range for the narrower Arrow width is DPY-4038
        // (the reference reserves DPY-4036 for values that are not integers).
        assert!(err.to_string().starts_with("DPY-4038:"), "{err}");
    }

    #[test]
    fn requested_schema_length_mismatch_raises_dpy_2069() {
        let requested = Arc::new(Schema::new(vec![Field::new("A", DataType::Int64, true)]));
        let options = ArrowFetchOptions::new().with_requested_schema(requested);
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
        let options = ArrowFetchOptions::new().with_requested_schema(requested);
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
        let options = ArrowFetchOptions::new().with_requested_schema(requested);
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
        assert_eq!(defined[0].ora_type_num(), ORA_TYPE_NUM_LONG);
        assert_eq!(defined[1].ora_type_num(), ORA_TYPE_NUM_LONG_RAW);
        assert_eq!(defined[2].ora_type_num(), ORA_TYPE_NUM_VARCHAR);
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
