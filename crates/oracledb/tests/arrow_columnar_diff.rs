//! Differential correctness gate for the columnar fetch->Arrow path (bead
//! rust-oracledb-wf7): the columnar [`Connection::fetch_all_record_batch_columnar`]
//! must produce a `RecordBatch` that is BYTE-IDENTICAL, cell-for-cell, to the
//! row-materialize-then-`build_record_batch` path
//! ([`Connection::fetch_all_record_batch`]) over a mixed-type many-row result.
//!
//! Two layers:
//!   1. A SYNTHETIC unit test (no container) drives both the owned decoder
//!      (`parse_fetch_response_with_context`) and the borrowed decoder
//!      (`parse_query_response_borrowed`) over one hand-built fetch frame of
//!      NUMBER / VARCHAR2 / RAW / NULL columns, then asserts
//!      `build_record_batch(owned) == build_record_batch_columnar(borrowed)`.
//!   2. A LIVE differential test (gated on the container env) runs the SAME
//!      mixed-type query (NUMBER int / NUMBER decimal / VARCHAR2 / DATE /
//!      NULLs) through both real fetch paths and asserts the two `RecordBatch`es
//!      are equal — the end-to-end wire-decode gate.
//!
//! Compiled only under `--features arrow`.
#![cfg(feature = "arrow")]

mod common;

use oracledb::arrow::{
    arrow_schema_for_columns, build_record_batch, build_record_batch_columnar, ArrowFetchOptions,
};
use oracledb::protocol::thin::{
    encode_number_text, parse_fetch_response_with_context, parse_query_response_borrowed,
    ClientCapabilities, ColumnMetadata,
};
use oracledb::protocol::wire::TtcWriter;
use std::sync::Arc;

const ORA_TYPE_NUM_VARCHAR: u8 = 1;
const ORA_TYPE_NUM_NUMBER: u8 = 2;
const ORA_TYPE_NUM_RAW: u8 = 23;
const CS_FORM_IMPLICIT: u8 = 1;
const TNS_MSG_TYPE_ROW_DATA: u8 = 7;
const TNS_MSG_TYPE_END_OF_RESPONSE: u8 = 29;

fn col(name: &str, ora_type_num: u8, precision: i8, scale: i8, buffer_size: u32) -> ColumnMetadata {
    ColumnMetadata::new(name, ora_type_num)
        .with_csfrm(CS_FORM_IMPLICIT)
        .with_precision(precision)
        .with_scale(scale)
        .with_buffer_size(buffer_size)
}

/// Build a fetch-response frame of `num_rows` rows of
/// `[NUMBER(int), NUMBER(2dp), VARCHAR2, RAW]`, injecting NULLs on a schedule so
/// the NullBuffer paths are exercised. Every Nth row's VARCHAR2 and NUMBER are
/// NULL. Returns (payload, columns).
fn build_mixed_frame(num_rows: usize) -> (Vec<u8>, Vec<ColumnMetadata>) {
    let columns = vec![
        col("ID", ORA_TYPE_NUM_NUMBER, 9, 0, 22),
        col("AMOUNT", ORA_TYPE_NUM_NUMBER, 18, 2, 22),
        col("LABEL", ORA_TYPE_NUM_VARCHAR, 0, 0, 4000),
        col("BLOB_ISH", ORA_TYPE_NUM_RAW, 0, 0, 2000),
    ];
    let mut w = TtcWriter::new();
    for i in 0..num_rows {
        w.write_u8(TNS_MSG_TYPE_ROW_DATA);
        // NUMBER id (NULL every 7th row): zero-length is the SQL NULL marker.
        if i % 7 == 0 {
            w.write_bytes_with_length(&[]).unwrap();
        } else {
            let id = encode_number_text(&i.to_string()).unwrap();
            w.write_bytes_with_length(&id).unwrap();
        }
        // NUMBER amount with 2 decimal places.
        let amount = encode_number_text(&format!("{}.{:02}", i, i % 100)).unwrap();
        w.write_bytes_with_length(&amount).unwrap();
        // VARCHAR2 label (NULL every 5th row).
        if i % 5 == 0 {
            w.write_bytes_with_length(&[]).unwrap();
        } else {
            w.write_bytes_with_length(format!("label-{i:08}").as_bytes())
                .unwrap();
        }
        // RAW bytes.
        let raw = [(i & 0xff) as u8, ((i >> 8) & 0xff) as u8, 0xAB, 0xCD];
        w.write_bytes_with_length(&raw).unwrap();
    }
    w.write_u8(TNS_MSG_TYPE_END_OF_RESPONSE);
    (w.into_bytes(), columns)
}

#[test]
fn columnar_synthetic_frame_byte_identical_to_row_path() {
    const NUM_ROWS: usize = 1234;
    let (payload, columns) = build_mixed_frame(NUM_ROWS);
    let caps = ClientCapabilities::default();
    let options = ArrowFetchOptions::default();

    // Owned -> row-path RecordBatch.
    let owned = parse_fetch_response_with_context(&payload, caps, &columns, None).unwrap();
    assert_eq!(owned.rows.len(), NUM_ROWS, "owned decodes all rows");
    let row_batch = build_record_batch(&columns, &owned.rows, &options).expect("row batch");

    // Borrowed -> columnar RecordBatch.
    let borrowed = parse_query_response_borrowed(&payload, caps, &columns, None).unwrap();
    assert_eq!(
        borrowed.batch.row_count(),
        NUM_ROWS,
        "borrowed decodes all rows"
    );
    let schema = Arc::new(arrow_schema_for_columns(&columns, &options).expect("schema"));
    let columnar_batch =
        build_record_batch_columnar(schema, &columns, &borrowed.batch).expect("columnar batch");

    assert_eq!(
        row_batch, columnar_batch,
        "columnar RecordBatch must equal the row path cell-for-cell"
    );
    // Sanity: the schemas match and the row count is preserved.
    assert_eq!(row_batch.schema(), columnar_batch.schema());
    assert_eq!(columnar_batch.num_rows(), NUM_ROWS);
}

#[test]
fn columnar_empty_frame_byte_identical_to_row_path() {
    let (payload, columns) = build_mixed_frame(0);
    let caps = ClientCapabilities::default();
    let options = ArrowFetchOptions::default();
    let owned = parse_fetch_response_with_context(&payload, caps, &columns, None).unwrap();
    let row_batch = build_record_batch(&columns, &owned.rows, &options).expect("row batch");
    let borrowed = parse_query_response_borrowed(&payload, caps, &columns, None).unwrap();
    let schema = Arc::new(arrow_schema_for_columns(&columns, &options).expect("schema"));
    let columnar_batch =
        build_record_batch_columnar(schema, &columns, &borrowed.batch).expect("columnar batch");
    assert_eq!(row_batch, columnar_batch, "empty result must also match");
    assert_eq!(columnar_batch.num_rows(), 0);
}

#[test]
fn columnar_fetch_decimals_byte_identical_to_row_path() {
    // fetch_decimals=true maps NUMBER(p,s) -> decimal128(p,s); exercise the
    // Decimal128 builder in BOTH paths and assert identity.
    const NUM_ROWS: usize = 777;
    let (payload, columns) = build_mixed_frame(NUM_ROWS);
    let caps = ClientCapabilities::default();
    let options = ArrowFetchOptions::new().with_fetch_decimals(true);
    let owned = parse_fetch_response_with_context(&payload, caps, &columns, None).unwrap();
    let row_batch = build_record_batch(&columns, &owned.rows, &options).expect("row batch");
    let borrowed = parse_query_response_borrowed(&payload, caps, &columns, None).unwrap();
    let schema = Arc::new(arrow_schema_for_columns(&columns, &options).expect("schema"));
    let columnar_batch =
        build_record_batch_columnar(schema, &columns, &borrowed.batch).expect("columnar batch");
    assert_eq!(
        row_batch, columnar_batch,
        "decimal128 columnar must equal the row path"
    );
}

/// Build a frame of `num_rows` rows where every one of `num_cols` VARCHAR2
/// columns is SQL NULL (zero-length wire field). The columns describe with
/// buffer_size 0, mimicking a "null by describe" column such as `SELECT null`.
fn build_all_null_frame(num_rows: usize, num_cols: usize) -> (Vec<u8>, Vec<ColumnMetadata>) {
    let columns: Vec<ColumnMetadata> = (0..num_cols)
        .map(|i| col(&format!("C{}", i + 1), ORA_TYPE_NUM_VARCHAR, 0, 0, 0))
        .collect();
    let mut w = TtcWriter::new();
    for _ in 0..num_rows {
        w.write_u8(TNS_MSG_TYPE_ROW_DATA);
        for _ in 0..num_cols {
            // Zero-length is the SQL NULL marker (null by describe).
            w.write_bytes_with_length(&[]).unwrap();
        }
    }
    w.write_u8(TNS_MSG_TYPE_END_OF_RESPONSE);
    (w.into_bytes(), columns)
}

/// Regression for upstream python-oracledb #597 (ac575331fe3b): fetching a
/// column that is NULL by describe into Arrow must append one Arrow null PER ROW
/// on the columnar fast path, so every column keeps the same length and the
/// batch never desyncs (upstream segfaulted here). Exercises the exact upstream
/// repro shape `SELECT null as c1, null as c2 FROM dual CONNECT BY LEVEL <= 3`
/// (3 rows of (None, None)) plus a single-null-column and a wider case.
#[test]
fn columnar_all_null_described_columns_stay_row_synced() {
    use arrow_array::Array;

    for (num_rows, num_cols) in [(3usize, 2usize), (5, 1), (1, 3), (0, 2)] {
        let (payload, columns) = build_all_null_frame(num_rows, num_cols);
        let caps = ClientCapabilities::default();
        let options = ArrowFetchOptions::default();

        let owned = parse_fetch_response_with_context(&payload, caps, &columns, None).unwrap();
        assert_eq!(owned.rows.len(), num_rows);
        let row_batch = build_record_batch(&columns, &owned.rows, &options).expect("row batch");

        let borrowed = parse_query_response_borrowed(&payload, caps, &columns, None).unwrap();
        assert_eq!(borrowed.batch.row_count(), num_rows);
        let schema = Arc::new(arrow_schema_for_columns(&columns, &options).expect("schema"));
        let columnar_batch =
            build_record_batch_columnar(schema, &columns, &borrowed.batch).expect("columnar batch");

        assert_eq!(
            row_batch, columnar_batch,
            "columnar must equal row for {num_cols} all-null column(s) x {num_rows} rows"
        );
        assert_eq!(columnar_batch.num_rows(), num_rows);
        for c in 0..num_cols {
            let column = columnar_batch.column(c);
            assert_eq!(
                column.len(),
                num_rows,
                "column {c} length must equal row count"
            );
            assert_eq!(
                column.null_count(),
                num_rows,
                "column {c} must be entirely null"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// LIVE differential: same query through both real fetch paths against the
// container. Self-skips cleanly when PYO_TEST_* is not configured.
// ---------------------------------------------------------------------------

mod live {
    use oracledb::arrow::ArrowFetchOptions;
    use oracledb::protocol::ClientIdentity;
    use oracledb::{BlockingConnection, ConnectOptions};

    fn connect_options() -> Option<ConnectOptions> {
        let crate::common::LiveCreds {
            connect_string,
            user,
            password,
        } = crate::common::live_creds_opt()?;
        let identity = ClientIdentity::new(
            "rust-oracledb-coldiff",
            "coldiff-machine",
            "coldiff-osuser",
            "coldiff-terminal",
            "rust-oracledb thn : 0.0.0",
        )
        .ok()?;
        Some(ConnectOptions::new(
            connect_string,
            user,
            password,
            identity,
        ))
    }

    /// A wide mixed-type result with NULLs: NUMBER int, NUMBER decimal, VARCHAR2,
    /// DATE, a NULLable column, and a small int. 12_000 rows / arraysize 1000 =>
    /// ~12 pages so multi-page paging is exercised in both fetch paths.
    const MIXED_SQL: &str = "select \
        level as id, \
        cast(level * 1.25 as number(18,4)) as amount, \
        rpad('row', 32, to_char(mod(level, 9))) as label, \
        date '2020-01-01' + numtodsinterval(level, 'second') as ts, \
        case when mod(level, 6) = 0 then null else to_char(level) end as maybe_null, \
        mod(level, 1000) as bucket \
        from dual connect by level <= 12000";

    fn run_diff(options_arrow: &ArrowFetchOptions) {
        let Some(opts) = connect_options() else {
            eprintln!("skipped live columnar diff: PYO_TEST_* not configured");
            return;
        };
        let mut conn = BlockingConnection::connect(opts).expect("connect");

        let row_batch =
            BlockingConnection::fetch_all_record_batch(&mut conn, MIXED_SQL, 1000, options_arrow)
                .expect("row-path fetch_df_all");
        let columnar_batch = BlockingConnection::fetch_all_record_batch_columnar(
            &mut conn,
            MIXED_SQL,
            1000,
            options_arrow,
        )
        .expect("columnar fetch_df_all");

        assert_eq!(
            row_batch.num_rows(),
            12_000,
            "the mixed query returns 12000 rows"
        );
        assert_eq!(
            row_batch.schema(),
            columnar_batch.schema(),
            "schemas must match"
        );
        assert_eq!(
            row_batch, columnar_batch,
            "LIVE columnar RecordBatch must equal the row path cell-for-cell"
        );

        BlockingConnection::close(conn).expect("close");
    }

    #[test]
    fn live_columnar_equals_row_path_default() {
        run_diff(&ArrowFetchOptions::default());
    }

    #[test]
    fn live_columnar_equals_row_path_fetch_decimals() {
        run_diff(&ArrowFetchOptions::new().with_fetch_decimals(true));
    }

    /// The exact upstream #597 repro (ac575331fe3b): both fetch paths must
    /// produce a 3-row batch of two all-null columns with equal lengths and no
    /// desync/panic.
    #[test]
    fn live_null_by_describe_columns_stay_row_synced() {
        use arrow_array::Array;

        let Some(opts) = connect_options() else {
            eprintln!("skipped live null-by-describe: PYO_TEST_* not configured");
            return;
        };
        let mut conn = BlockingConnection::connect(opts).expect("connect");
        let sql = "select null as c1, null as c2 from dual connect by level <= 3";
        let options = ArrowFetchOptions::default();

        let row_batch = BlockingConnection::fetch_all_record_batch(&mut conn, sql, 100, &options)
            .expect("row-path null fetch");
        let columnar_batch =
            BlockingConnection::fetch_all_record_batch_columnar(&mut conn, sql, 100, &options)
                .expect("columnar null fetch");

        assert_eq!(row_batch.num_rows(), 3);
        assert_eq!(
            row_batch, columnar_batch,
            "columnar must equal row for null-by-describe"
        );
        for c in 0..columnar_batch.num_columns() {
            let column = columnar_batch.column(c);
            assert_eq!(column.len(), 3, "column {c} length");
            assert_eq!(column.null_count(), 3, "column {c} all-null");
        }

        BlockingConnection::close(conn).expect("close");
    }
}

#[cfg(test)]
mod leak_probe {
    use oracledb::arrow::ArrowFetchOptions;
    use oracledb::protocol::ClientIdentity;
    use oracledb::{BlockingConnection, ConnectOptions};

    fn opts() -> Option<ConnectOptions> {
        let crate::common::LiveCreds {
            connect_string: cs,
            user: u,
            password: p,
        } = crate::common::live_creds_opt()?;
        let id =
            ClientIdentity::new("leakprobe", "m", "o", "t", "rust-oracledb thn : 0.0.0").ok()?;
        Some(ConnectOptions::new(cs, u, p, id))
    }

    #[test]
    fn columnar_does_not_leak_cursors_over_250_calls() {
        let Some(o) = opts() else {
            eprintln!("skip leak probe");
            return;
        };
        let mut conn = BlockingConnection::connect(o).expect("connect");
        let sql = "select level as id, to_char(level) as code from dual connect by level <= 3000";
        let options = ArrowFetchOptions::default();
        for i in 0..250 {
            let b =
                BlockingConnection::fetch_all_record_batch_columnar(&mut conn, sql, 1000, &options)
                    .unwrap_or_else(|e| panic!("columnar call {i} failed (cursor leak?): {e}"));
            assert_eq!(b.num_rows(), 3000);
        }
        BlockingConnection::close(conn).expect("close");
    }

    #[test]
    fn row_path_does_not_leak_cursors_over_250_calls() {
        let Some(o) = opts() else {
            eprintln!("skip leak probe");
            return;
        };
        let mut conn = BlockingConnection::connect(o).expect("connect");
        let sql = "select level as id, to_char(level) as code from dual connect by level <= 3000";
        let options = ArrowFetchOptions::default();
        for i in 0..250 {
            let b = BlockingConnection::fetch_all_record_batch(&mut conn, sql, 1000, &options)
                .unwrap_or_else(|e| panic!("row-path call {i} failed (cursor leak?): {e}"));
            assert_eq!(b.num_rows(), 3000);
        }
        BlockingConnection::close(conn).expect("close");
    }
}
