//! Microbenchmark for the wire NUMBER decode (bead rust-oracledb-shh).
//!
//! `OracleNumber::from_wire` walks Oracle's base-100 mantissa/exponent bytes into
//! the inline `{ i128 coefficient, i16 scale }` form. Bead shh FUSES the i128
//! coefficient accumulation into that single digit walk, removing the second
//! `digits_to_i128` pass over the digit buffer for the common in-range NUMBER.
//! This bench measures the decode of a realistic page of NUMBER cells across the
//! value widths a fetch sees: small integers, mid-range integers, decimals, and
//! the wide (near-i128, max-precision) values that exercise the full digit walk.
//!
//! Run (record a baseline on master, then compare on the branch):
//!   cargo bench -p oracledb-protocol --bench number_decode

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use oracledb_protocol::thin::{encode_number_text, OracleNumber};

/// Cells decoded per benchmark iteration (a fetch-page scale).
const CELLS: usize = 1000;

/// Encode `CELLS` wire NUMBERs from a text generator, returning the wire forms.
fn wire_cells(gen: impl Fn(usize) -> String) -> Vec<Vec<u8>> {
    (0..CELLS)
        .map(|i| encode_number_text(&gen(i)).expect("encode NUMBER"))
        .collect()
}

#[inline]
fn decode_all(cells: &[Vec<u8>]) -> i128 {
    // Sum the coefficients so the optimizer cannot elide the decode; the inline
    // coefficient is the field the fusion produces.
    let mut acc: i128 = 0;
    for cell in cells {
        let n = OracleNumber::from_wire(cell).expect("decode NUMBER");
        acc = acc.wrapping_add(n.coefficient().unwrap_or(0));
    }
    acc
}

fn bench_set(c: &mut Criterion, label: &str, cells: Vec<Vec<u8>>) {
    let total: usize = cells.iter().map(Vec::len).sum();
    let mut group = c.benchmark_group(label);
    group.throughput(Throughput::Bytes(total as u64));
    group.bench_function("from_wire", |b| {
        b.iter_batched(
            || &cells,
            |cells| black_box(decode_all(black_box(cells))),
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn benches(c: &mut Criterion) {
    // Small integers (1–3 digits): IDs, counts, the most common case.
    bench_set(
        c,
        "number_small_int",
        wire_cells(|i| (i % 1000).to_string()),
    );
    // Mid-range integers (~10 digits).
    bench_set(
        c,
        "number_mid_int",
        wire_cells(|i| (1_000_000_000u64 + i as u64).to_string()),
    );
    // Decimals (scale-bearing).
    bench_set(
        c,
        "number_decimal",
        wire_cells(|i| format!("{}.{:04}", i % 1000, i % 9999)),
    );
    // Wide, max-precision (38-digit) values: the longest digit walk, where a
    // second pass costs the most.
    bench_set(
        c,
        "number_wide_38digit",
        wire_cells(|i| format!("1234567890123456789012345678901234567{}", i % 10)),
    );
}

criterion_group!(number, benches);
criterion_main!(number);
