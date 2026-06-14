//! Microbenchmark for the borrowed-fetch UTF-8 validation decode path
//! (bead rust-oracledb-63o).
//!
//! The hot text path in `parse_column_slot` validates each borrowed VARCHAR/CHAR
//! cell as UTF-8 before lending it as a `&str`. This bench measures that one
//! operation in isolation, on a realistic *batch* of cells (so the per-call SIMD
//! setup cost is amortized the way it is in a real fetch page), for two cell
//! widths:
//!
//!   * VARCHAR2(40)   — the canonical short-string workload (names, codes).
//!   * VARCHAR2(2000)  — wide text (descriptions, serialized blobs).
//!
//! It runs BOTH validators (`core::str::from_utf8` and, when the `simd-decode`
//! feature is enabled, `simdutf8::basic::from_utf8`) side by side so the win is a
//! direct, same-binary head-to-head — no cross-build comparison. The validators
//! have identical accept/reject semantics (both decline the same invalid input),
//! so this measures pure throughput, not behavior.
//!
//! Run:
//!   cargo bench -p oracledb-protocol --bench utf8_validate --features simd-decode

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};

/// Rows-per-fetch-page scale: validate this many cells per benchmark iteration so
/// the measured cost reflects a full decode page, not a single 40-byte call.
const CELLS_PER_PAGE: usize = 1000;

/// Build `CELLS_PER_PAGE` ASCII text cells of the given byte width (the common
/// all-ASCII VARCHAR2 case — the fast path both validators take).
fn ascii_cells(width: usize) -> Vec<Vec<u8>> {
    (0..CELLS_PER_PAGE)
        .map(|i| {
            // Deterministic printable-ASCII filler; vary the leading bytes per
            // cell so the optimizer cannot hoist a single validation.
            let mut cell = Vec::with_capacity(width);
            let seed = (i % 26) as u8;
            for j in 0..width {
                cell.push(b'A' + ((seed + (j % 26) as u8) % 26));
            }
            cell
        })
        .collect()
}

/// Build cells with a sprinkling of multi-byte UTF-8 (CJK) so the validators
/// must walk the continuation-byte logic, not just the ASCII fast lane.
fn mixed_utf8_cells(width: usize) -> Vec<Vec<u8>> {
    (0..CELLS_PER_PAGE)
        .map(|i| {
            let mut s = String::with_capacity(width);
            while s.len() < width {
                if (i + s.len()) % 7 == 0 {
                    s.push('\u{4e2d}'); // CJK, 3 bytes
                } else {
                    s.push('a');
                }
            }
            // Truncate on a char boundary to the target width.
            while s.len() > width {
                s.pop();
            }
            s.into_bytes()
        })
        .collect()
}

#[inline]
fn validate_std(cells: &[Vec<u8>]) -> usize {
    let mut ok = 0usize;
    for cell in cells {
        if core::str::from_utf8(cell).is_ok() {
            ok += 1;
        }
    }
    ok
}

#[cfg(feature = "simd-decode")]
#[inline]
fn validate_simd(cells: &[Vec<u8>]) -> usize {
    let mut ok = 0usize;
    for cell in cells {
        if simdutf8::basic::from_utf8(cell).is_ok() {
            ok += 1;
        }
    }
    ok
}

fn bench_width(c: &mut Criterion, label: &str, cells: Vec<Vec<u8>>, width: usize) {
    let total_bytes = (width * CELLS_PER_PAGE) as u64;
    let mut group = c.benchmark_group(label);
    group.throughput(Throughput::Bytes(total_bytes));

    group.bench_function("std_from_utf8", |b| {
        b.iter_batched(
            || &cells,
            |cells| black_box(validate_std(black_box(cells))),
            BatchSize::SmallInput,
        )
    });

    #[cfg(feature = "simd-decode")]
    group.bench_function("simdutf8_basic", |b| {
        b.iter_batched(
            || &cells,
            |cells| black_box(validate_simd(black_box(cells))),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn benches(c: &mut Criterion) {
    // Canonical short-string workload.
    bench_width(c, "varchar2_40_ascii", ascii_cells(40), 40);
    bench_width(c, "varchar2_40_mixed", mixed_utf8_cells(40), 40);
    // Wide-text workload.
    bench_width(c, "varchar2_2000_ascii", ascii_cells(2000), 2000);
    bench_width(c, "varchar2_2000_mixed", mixed_utf8_cells(2000), 2000);
}

criterion_group!(utf8, benches);
criterion_main!(utf8);
