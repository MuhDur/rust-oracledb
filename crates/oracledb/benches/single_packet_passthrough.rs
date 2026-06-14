//! Microbenchmark + allocation count for the single-packet response passthrough
//! (bead rust-oracledb-0n0).
//!
//! When a fetch/data response is ONE terminal DATA packet, the reassembly loop
//! can MOVE the packet's owned payload buffer into the response and strip the
//! 2 flag bytes in place, instead of allocating a fresh `Vec` and copying the
//! whole payload into it. This bench isolates that exact operation on a freshly
//! allocated packet buffer (as `read_packet` produces one), comparing:
//!
//!   * legacy:      `Vec::new()` + `extend_from_slice(&payload[2..])`
//!   * passthrough: move the buffer + `drain(..2)`
//!
//! across the single-packet payload sizes the optimization actually applies to
//! (a response that fits one DATA packet — small queries, single-row fetches,
//! login/handshake responses). It reports both wall time (criterion) and the
//! allocation delta (counting allocator), so the win is measured, not assumed.
//!
//! Run:
//!   cargo bench -p oracledb --bench single_packet_passthrough

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};

/// A fresh owned packet buffer of `size` bytes (the first 2 are data flags), as
/// `read_packet` allocates one per packet. Filled so the copy is real work.
fn fresh_packet(size: usize) -> Vec<u8> {
    let mut v = vec![0u8; size];
    for (i, b) in v.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    v
}

/// Legacy reassembly: allocate a fresh response Vec and copy the flag-stripped
/// payload into it.
#[inline]
fn legacy(packet_payload: Vec<u8>) -> Vec<u8> {
    let mut response = Vec::new();
    response.extend_from_slice(&packet_payload[2..]);
    response
}

/// Passthrough: move the owned buffer and strip the 2 leading flag bytes in
/// place — no second allocation, no copy into a separate buffer.
#[inline]
fn passthrough(packet_payload: Vec<u8>) -> Vec<u8> {
    let mut response = packet_payload;
    response.drain(..2);
    response
}

fn bench_size(c: &mut Criterion, size: usize) {
    let label = format!("single_packet_{size}b");
    let mut group = c.benchmark_group(&label);
    group.throughput(Throughput::Bytes((size - 2) as u64));

    group.bench_function("legacy_extend", |b| {
        b.iter_batched(
            || fresh_packet(size),
            |p| black_box(legacy(black_box(p))),
            BatchSize::SmallInput,
        )
    });
    group.bench_function("passthrough_drain", |b| {
        b.iter_batched(
            || fresh_packet(size),
            |p| black_box(passthrough(black_box(p))),
            BatchSize::SmallInput,
        )
    });
    group.finish();

    // Allocation count over many iterations (counting allocator). Each `legacy`
    // call allocates one response Vec; each `passthrough` call allocates none
    // (it reuses the input buffer, which the caller already paid for).
    const ITERS: usize = 10_000;
    let legacy_allocs = allocation_counter::measure(|| {
        let mut sink = 0usize;
        for _ in 0..ITERS {
            let r = legacy(fresh_packet(size));
            sink = sink.wrapping_add(r.len());
        }
        black_box(sink);
    });
    let passthrough_allocs = allocation_counter::measure(|| {
        let mut sink = 0usize;
        for _ in 0..ITERS {
            let r = passthrough(fresh_packet(size));
            sink = sink.wrapping_add(r.len());
        }
        black_box(sink);
    });
    // Subtract the shared `fresh_packet` allocation (1 per iter) to isolate the
    // reassembly-side allocations.
    let legacy_reassembly = legacy_allocs.count_total as i64 - ITERS as i64;
    let passthrough_reassembly = passthrough_allocs.count_total as i64 - ITERS as i64;
    println!(
        "[{label}] reassembly allocs over {ITERS} iters — legacy: {legacy_reassembly}, \
         passthrough: {passthrough_reassembly} (bytes: legacy {}, passthrough {})",
        legacy_allocs.bytes_total, passthrough_allocs.bytes_total
    );
}

fn benches(c: &mut Criterion) {
    // Sizes that fit a single DATA packet (default SDU 8KB):
    bench_size(c, 64); // tiny: login / status response
    bench_size(c, 512); // small query result
    bench_size(c, 2048); // a few rows
    bench_size(c, 8000); // near a full single packet
}

criterion_group!(passthrough_bench, benches);
criterion_main!(passthrough_bench);
