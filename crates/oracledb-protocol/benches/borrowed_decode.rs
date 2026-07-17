//! Microbenchmark for the borrowed fetch decode hot loop (bead rust-oracledb-myx
//! gate). Decodes a wide-analytics page through `for_each_row_ref`, touching
//! every cell — the path where the per-cell NULL guard + csfrm check + ora_type
//! match runs N_rows x N_cols times. This is the A/B harness for the
//! ColumnDecodePlan experiment: if hoisting that per-column dispatch into a
//! once-per-column plan reduces branch/cache misses, it shows here as faster
//! wall time on this decode-bound, allocation-light workload.
//!
//! Run:
//!   cargo bench -p oracledb-protocol --bench borrowed_decode

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use oracledb_protocol::thin::{
    encode_number_text, parse_query_response_borrowed, ClientCapabilities, ColumnMetadata,
    QueryValueRef,
};
use oracledb_protocol::wire::TtcWriter;
use oracledb_protocol::ProtocolError;

const ORA_VARCHAR: u8 = 1;
const ORA_NUMBER: u8 = 2;
const CS_IMPLICIT: u8 = 1;
const ROW_DATA: u8 = 7;
const END_OF_RESPONSE: u8 = 29;

fn col(name: &str, ora_type_num: u8, buffer_size: u32) -> ColumnMetadata {
    ColumnMetadata::new(name, ora_type_num)
        .with_csfrm(CS_IMPLICIT)
        .with_buffer_size(buffer_size)
}

/// A wide-analytics page: `rows` rows of 10 columns alternating NUMBER and
/// VARCHAR2 (the canonical analytics shape — every NUMBER cell forces a
/// base-100 decode, every VARCHAR2 cell a UTF-8 validation, and each cell runs
/// the per-cell type dispatch).
fn build_page(rows: usize) -> (Vec<u8>, Vec<ColumnMetadata>) {
    let columns = vec![
        col("ID", ORA_NUMBER, 22),
        col("NAME", ORA_VARCHAR, 4000),
        col("SCORE", ORA_NUMBER, 22),
        col("CATEGORY", ORA_VARCHAR, 4000),
        col("QTY", ORA_NUMBER, 22),
        col("LABEL", ORA_VARCHAR, 4000),
        col("RATIO", ORA_NUMBER, 22),
        col("CODE", ORA_VARCHAR, 4000),
        col("TOTAL", ORA_NUMBER, 22),
        col("NOTE", ORA_VARCHAR, 4000),
    ];
    let mut w = TtcWriter::new();
    for i in 0..rows {
        w.write_u8(ROW_DATA);
        for c in 0..10 {
            if c % 2 == 0 {
                let n = encode_number_text(&format!("{}", i * 7 + c))
                    .expect("benchmark NUMBER fixture must encode");
                w.write_bytes_with_length(&n)
                    .expect("benchmark NUMBER fixture must fit the TTC writer");
            } else {
                w.write_bytes_with_length(format!("value-{i:06}-{c}").as_bytes())
                    .expect("benchmark text fixture must fit the TTC writer");
            }
        }
    }
    w.write_u8(END_OF_RESPONSE);
    (w.into_bytes(), columns)
}

fn bench(c: &mut Criterion) {
    let rows = 20_000;
    let (payload, columns) = build_page(rows);
    let caps = ClientCapabilities::default();
    let cell_bytes = payload.len() as u64;

    let mut group = c.benchmark_group("borrowed_decode_wide_analytics");
    group.throughput(Throughput::Bytes(cell_bytes));
    group.bench_function("decode_20k_x_10", |b| {
        b.iter(|| {
            let result = parse_query_response_borrowed(black_box(&payload), caps, &columns, None)
                .expect("benchmark fixture must decode");
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
                    Ok::<(), ProtocolError>(())
                })
                .expect("benchmark decoded rows must iterate");
            black_box(sum)
        })
    });
    group.finish();
}

criterion_group!(borrowed, bench);
criterion_main!(borrowed);
