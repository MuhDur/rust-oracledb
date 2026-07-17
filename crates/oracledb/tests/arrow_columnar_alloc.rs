// Assertion-heavy test code intentionally panics on invariant violations.
#![allow(clippy::unwrap_used)]

//! Allocation + timing measurement for the columnar fetch->Arrow path
//! (bead rust-oracledb-wf7): row-materialize-then-`build_record_batch` vs the
//! columnar `build_record_batch_columnar`, over a wide many-row analytics batch.
//!
//! Both paths START from the SAME wire fetch-response frame and END at a
//! `RecordBatch`, so the measured allocations are the full client-side cost of
//! turning the wire bytes into Arrow columns:
//!   * ROW path  = `parse_fetch_response_with_context` (owned rows: one
//!     `Vec<Option<QueryValue>>` per row + a `String` per text cell + an
//!     `OracleNumber` per number cell) THEN `build_record_batch` (the transpose
//!     pass re-reads every cell into the column builders).
//!   * COLUMNAR path = `parse_query_response_borrowed` (zero-copy borrowed rows,
//!     amortized NUMBER text arena) THEN `build_record_batch_columnar` (streams
//!     each borrowed cell straight into the column builder — no row Vec, no
//!     transpose).
//!
//! Allocations are counted with the `allocation-counter` crate (its unsafe
//! GlobalAlloc lives inside that crate, so this crate stays
//! `#![forbid(unsafe_code)]`-clean). The headline metric is the allocation
//! reduction; the timing is informational (it varies with host load).
//!
//! Compiled only under `--features arrow`.
#![cfg(feature = "arrow")]

use std::sync::Arc;
use std::time::Instant;

use oracledb::arrow::{
    arrow_schema_for_columns, build_record_batch, build_record_batch_columnar, ArrowFetchOptions,
};
use oracledb::protocol::thin::{
    encode_number_text, parse_fetch_response_with_context, parse_query_response_borrowed,
    ClientCapabilities, ColumnMetadata,
};
use oracledb::protocol::wire::TtcWriter;

const ORA_TYPE_NUM_VARCHAR: u8 = 1;
const ORA_TYPE_NUM_NUMBER: u8 = 2;
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

/// A wide analytics frame: 10 typed columns (NUMBER int x4, NUMBER decimal x2,
/// VARCHAR2 x4), mirroring the perf-map's `fetch_wide_analytics` shape. NULLs on
/// a schedule exercise the NullBuffer paths.
fn build_wide_analytics_frame(num_rows: usize) -> (Vec<u8>, Vec<ColumnMetadata>) {
    let columns = vec![
        col("ID", ORA_TYPE_NUM_NUMBER, 9, 0, 22),
        col("AMOUNT", ORA_TYPE_NUM_NUMBER, 18, 4, 22),
        col("LABEL", ORA_TYPE_NUM_VARCHAR, 0, 0, 4000),
        col("BUCKET", ORA_TYPE_NUM_NUMBER, 5, 0, 22),
        col("SQ", ORA_TYPE_NUM_NUMBER, 18, 0, 22),
        col("CODE", ORA_TYPE_NUM_VARCHAR, 0, 0, 4000),
        col("PRICE", ORA_TYPE_NUM_NUMBER, 18, 2, 22),
        col("FLAG", ORA_TYPE_NUM_NUMBER, 1, 0, 22),
        col("CAT", ORA_TYPE_NUM_VARCHAR, 0, 0, 4000),
        col("NOTE", ORA_TYPE_NUM_VARCHAR, 0, 0, 4000),
    ];
    let mut w = TtcWriter::new();
    let num = |s: &str| encode_number_text(s).unwrap();
    for i in 0..num_rows {
        w.write_u8(TNS_MSG_TYPE_ROW_DATA);
        w.write_bytes_with_length(&num(&i.to_string())).unwrap();
        w.write_bytes_with_length(&num(&format!("{}.{:04}", i, i % 10000)))
            .unwrap();
        w.write_bytes_with_length(format!("label-{i:08}").as_bytes())
            .unwrap();
        w.write_bytes_with_length(&num(&(i % 7).to_string()))
            .unwrap();
        w.write_bytes_with_length(&num(&(i as u64 * i as u64).to_string()))
            .unwrap();
        w.write_bytes_with_length(format!("code-{i}").as_bytes())
            .unwrap();
        w.write_bytes_with_length(&num(&format!("{}.{:02}", i, i % 100)))
            .unwrap();
        w.write_bytes_with_length(&num(&(i % 2).to_string()))
            .unwrap();
        // NULL CAT every 5th row.
        if i % 5 == 0 {
            w.write_bytes_with_length(&[]).unwrap();
        } else {
            w.write_bytes_with_length(format!("category-{}", i % 13).as_bytes())
                .unwrap();
        }
        w.write_bytes_with_length(format!("note for row {i}").as_bytes())
            .unwrap();
    }
    w.write_u8(TNS_MSG_TYPE_END_OF_RESPONSE);
    (w.into_bytes(), columns)
}

#[test]
fn columnar_fetch_cuts_allocations_versus_row_path() {
    const NUM_ROWS: usize = 5000;
    let (payload, columns) = build_wide_analytics_frame(NUM_ROWS);
    let caps = ClientCapabilities::default();
    let options = ArrowFetchOptions::default();
    let schema = Arc::new(arrow_schema_for_columns(&columns, &options).expect("schema"));

    // Correctness first: both end-to-end paths produce equal batches.
    {
        let owned = parse_fetch_response_with_context(&payload, caps, &columns, None).unwrap();
        let row_batch = build_record_batch(&columns, &owned.rows, &options).unwrap();
        let borrowed = parse_query_response_borrowed(&payload, caps, &columns, None).unwrap();
        let columnar_batch =
            build_record_batch_columnar(schema.clone(), &columns, &borrowed.batch).unwrap();
        assert_eq!(
            row_batch, columnar_batch,
            "alloc test prerequisite: paths must be byte-identical"
        );
        assert_eq!(row_batch.num_rows(), NUM_ROWS);
    }

    // ROW path: owned decode + build_record_batch (transpose). Project the batch
    // so the optimizer cannot elide it.
    let mut row_rows = 0usize;
    let row = allocation_counter::measure(|| {
        let owned = parse_fetch_response_with_context(&payload, caps, &columns, None).unwrap();
        let batch = build_record_batch(&columns, &owned.rows, &options).unwrap();
        row_rows = std::hint::black_box(batch.num_rows());
    });

    // COLUMNAR path: borrowed decode + build_record_batch_columnar.
    let mut col_rows = 0usize;
    let columnar = allocation_counter::measure(|| {
        let borrowed = parse_query_response_borrowed(&payload, caps, &columns, None).unwrap();
        let batch = build_record_batch_columnar(schema.clone(), &columns, &borrowed.batch).unwrap();
        col_rows = std::hint::black_box(batch.num_rows());
    });
    assert_eq!(row_rows, col_rows, "both build the same row count");

    // Timing (informational).
    let time_row = {
        let start = Instant::now();
        for _ in 0..50 {
            let owned = parse_fetch_response_with_context(&payload, caps, &columns, None).unwrap();
            let batch = build_record_batch(&columns, &owned.rows, &options).unwrap();
            std::hint::black_box(batch.num_rows());
        }
        start.elapsed() / 50
    };
    let time_columnar = {
        let start = Instant::now();
        for _ in 0..50 {
            let borrowed = parse_query_response_borrowed(&payload, caps, &columns, None).unwrap();
            let batch =
                build_record_batch_columnar(schema.clone(), &columns, &borrowed.batch).unwrap();
            std::hint::black_box(batch.num_rows());
        }
        start.elapsed() / 50
    };

    let row_allocs = row.count_total;
    let col_allocs = columnar.count_total;
    let alloc_reduction = 100.0 * (row_allocs as f64 - col_allocs as f64) / row_allocs as f64;
    let bytes_reduction =
        100.0 * (row.bytes_total as f64 - columnar.bytes_total as f64) / row.bytes_total as f64;

    println!(
        "\n===== columnar fetch->Arrow allocation measurement ({NUM_ROWS} rows x 10 cols) ====="
    );
    println!(
        "row path (owned + transpose): {row_allocs:>9} allocs ({:.2}/row), {:>11} bytes",
        row_allocs as f64 / NUM_ROWS as f64,
        row.bytes_total
    );
    println!(
        "columnar (borrowed, direct) : {col_allocs:>9} allocs ({:.2}/row), {:>11} bytes",
        col_allocs as f64 / NUM_ROWS as f64,
        columnar.bytes_total
    );
    println!(
        "allocation reduction: {alloc_reduction:>6.1}%   bytes reduction: {bytes_reduction:>6.1}%"
    );
    println!("row path  decode+build time/batch: {time_row:?}");
    println!("columnar  decode+build time/batch: {time_columnar:?}");
    println!(
        "==================================================================================\n"
    );

    // Hard floor: the columnar path must allocate materially less. The row path
    // allocates a `Vec<Option<QueryValue>>` per row plus a `String` per text
    // cell plus the transpose; the columnar path allocates only the Arrow value
    // buffers plus the amortized NUMBER arena. We require at least a 3x reduction
    // in allocation COUNT (the dominant cost) to guard against regressions.
    assert!(
        col_allocs * 3 < row_allocs,
        "columnar path must cut allocations >=3x: row={row_allocs}, columnar={col_allocs}"
    );
}
