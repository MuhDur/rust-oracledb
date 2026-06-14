//! Allocation-count + timing measurement: owned fetch decode vs the zero-copy
//! borrowed fetch decode, over a wide many-row synthetic batch.
//!
//! Allocations are counted with the `allocation-counter` crate, whose unsafe
//! `GlobalAlloc` lives inside that crate — this test (and the whole workspace)
//! stays `#![forbid(unsafe_code)]`-clean. The synthetic payload is a real
//! fetch-response frame: N rows of `[NUMBER, VARCHAR2, VARCHAR2, NUMBER]` column
//! values, the exact shape both `parse_fetch_response_with_context` (owned) and
//! `parse_query_response_borrowed` (borrowed) walk. Honest numbers: the counter
//! sees every allocation, and the borrowed path is exercised through a real
//! consumer (`for_each_row_ref`) that touches every cell.

use std::time::Instant;

use oracledb_protocol::thin::{
    encode_number_text, parse_fetch_response_with_context, parse_query_response_borrowed,
    ClientCapabilities, ColumnMetadata, QueryValueRef,
};
use oracledb_protocol::wire::TtcWriter;
use oracledb_protocol::ProtocolError;

const ORA_TYPE_NUM_VARCHAR: u8 = 1;
const ORA_TYPE_NUM_NUMBER: u8 = 2;
const CS_FORM_IMPLICIT: u8 = 1;
const TNS_MSG_TYPE_ROW_DATA: u8 = 7;
const TNS_MSG_TYPE_END_OF_RESPONSE: u8 = 29;

// The error type the borrowed callback returns (decode errors convert into it).
type DecodeErr = ProtocolError;

fn col(name: &str, ora_type_num: u8, buffer_size: u32) -> ColumnMetadata {
    ColumnMetadata {
        name: name.to_string(),
        ora_type_num,
        csfrm: CS_FORM_IMPLICIT,
        buffer_size,
        ..ColumnMetadata::default()
    }
}

/// Build a fetch-response frame of `num_rows` rows of
/// `[NUMBER, VARCHAR2, VARCHAR2, NUMBER]`, returning (payload, columns).
fn build_wide_batch(num_rows: usize) -> (Vec<u8>, Vec<ColumnMetadata>) {
    let columns = vec![
        col("ID", ORA_TYPE_NUM_NUMBER, 22),
        col("NAME", ORA_TYPE_NUM_VARCHAR, 4000),
        col("CATEGORY", ORA_TYPE_NUM_VARCHAR, 4000),
        col("SCORE", ORA_TYPE_NUM_NUMBER, 22),
    ];
    let mut writer = TtcWriter::new();
    for i in 0..num_rows {
        writer.write_u8(TNS_MSG_TYPE_ROW_DATA);
        // NUMBER id
        let id = encode_number_text(&i.to_string()).unwrap();
        writer.write_bytes_with_length(&id).unwrap();
        // VARCHAR2 name
        writer
            .write_bytes_with_length(format!("customer-name-{i:08}").as_bytes())
            .unwrap();
        // VARCHAR2 category
        writer
            .write_bytes_with_length(format!("category-{}", i % 32).as_bytes())
            .unwrap();
        // NUMBER score
        let score = encode_number_text(&format!("{}", i % 1000)).unwrap();
        writer.write_bytes_with_length(&score).unwrap();
    }
    writer.write_u8(TNS_MSG_TYPE_END_OF_RESPONSE);
    (writer.into_bytes(), columns)
}

#[test]
fn borrowed_fetch_cuts_allocations_versus_owned_fetch() {
    const NUM_ROWS: usize = 5000;
    let (payload, columns) = build_wide_batch(NUM_ROWS);
    let caps = ClientCapabilities::default();

    // Warm + correctness: both paths decode the same row count.
    let owned_warm = parse_fetch_response_with_context(&payload, caps, &columns, None).unwrap();
    assert_eq!(owned_warm.rows.len(), NUM_ROWS, "owned decodes all rows");
    let borrowed_warm = parse_query_response_borrowed(&payload, caps, &columns, None).unwrap();
    assert_eq!(
        borrowed_warm.batch.row_count(),
        NUM_ROWS,
        "borrowed decodes all rows"
    );

    // --- Owned path: decode the whole batch into owned QueryValues, projecting
    //     each cell's length so the optimizer cannot elide the decode. ---
    let mut owned_sum = 0usize;
    let owned = allocation_counter::measure(|| {
        let result = parse_fetch_response_with_context(&payload, caps, &columns, None).unwrap();
        let mut sum = 0usize;
        for row in &result.rows {
            for cell in row.iter().flatten() {
                sum += cell
                    .as_number_text()
                    .map_or_else(|| cell.as_text().map_or(0, str::len), str::len);
            }
        }
        owned_sum = std::hint::black_box(sum);
    });

    // --- Borrowed path: decode + iterate every cell through for_each_row_ref,
    //     doing the same projection. Scalar cells borrow the buffer. ---
    let mut borrowed_sum = 0usize;
    let borrowed = allocation_counter::measure(|| {
        let result = parse_query_response_borrowed(&payload, caps, &columns, None).unwrap();
        let mut sum = 0usize;
        result
            .batch
            .for_each_row_ref(|row| {
                for cell in row.iter().flatten() {
                    sum += match cell {
                        QueryValueRef::Text(t) => t.len(),
                        QueryValueRef::Number { text, .. } => text.len(),
                        _ => 0,
                    };
                }
                Ok::<(), DecodeErr>(())
            })
            .unwrap();
        borrowed_sum = std::hint::black_box(sum);
    });

    assert_eq!(owned_sum, borrowed_sum, "both paths see the same data");

    // --- Timing (informational; allocation count is the headline metric). ---
    let time_owned = {
        let start = Instant::now();
        for _ in 0..100 {
            let result = parse_fetch_response_with_context(&payload, caps, &columns, None).unwrap();
            std::hint::black_box(&result.rows);
        }
        start.elapsed() / 100
    };
    let time_borrowed = {
        let start = Instant::now();
        for _ in 0..100 {
            let result = parse_query_response_borrowed(&payload, caps, &columns, None).unwrap();
            let mut sum = 0usize;
            result
                .batch
                .for_each_row_ref(|row| {
                    for cell in row.iter().flatten() {
                        if let QueryValueRef::Text(t) = cell {
                            sum += t.len();
                        }
                    }
                    Ok::<(), DecodeErr>(())
                })
                .unwrap();
            std::hint::black_box(sum);
        }
        start.elapsed() / 100
    };

    let owned_allocs = owned.count_total;
    let borrowed_allocs = borrowed.count_total;
    let reduction_pct =
        100.0 * (owned_allocs as f64 - borrowed_allocs as f64) / owned_allocs as f64;
    let bytes_reduction_pct =
        100.0 * (owned.bytes_total as f64 - borrowed.bytes_total as f64) / owned.bytes_total as f64;

    println!("\n===== borrowed-fetch allocation measurement ({NUM_ROWS} rows x 4 cols) =====");
    println!(
        "owned    decode: {owned_allocs:>8} allocs ({:.2}/row), {:>9} bytes",
        owned_allocs as f64 / NUM_ROWS as f64,
        owned.bytes_total
    );
    println!(
        "borrowed decode: {borrowed_allocs:>8} allocs ({:.2}/row), {:>9} bytes",
        borrowed_allocs as f64 / NUM_ROWS as f64,
        borrowed.bytes_total
    );
    println!("allocation reduction: {reduction_pct:>6.1}%   bytes reduction: {bytes_reduction_pct:>6.1}%");
    println!("owned    decode time/batch: {time_owned:?}");
    println!("borrowed decode time/batch: {time_borrowed:?}");
    println!("======================================================================\n");

    // Hard floor: the borrowed path must allocate dramatically less. The owned
    // path allocates a String per NUMBER (2/row) + per Text (2/row) + the per-row
    // Vec + the rows Vec; the borrowed path's Text cells are zero-copy and only
    // the NUMBER text lands in an amortized per-row arena.
    assert!(
        borrowed_allocs * 2 < owned_allocs,
        "borrowed path must at least halve allocations: owned={owned_allocs}, borrowed={borrowed_allocs}"
    );
}
