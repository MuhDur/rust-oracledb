// Assertion-heavy test code intentionally panics on invariant violations.
#![allow(clippy::unwrap_used)]

//! Allocation-count probe for the execute-payload build (STEP 3 micro-opt
//! target): how many heap allocations does building one `select 1 from dual`
//! EXECUTE payload cost? The payload is built into a `TtcWriter`'s `Vec<u8>`;
//! starting from zero capacity, the small `write_*` pushes grow the Vec through
//! several doublings, each a separate allocation. Preallocating the writer cuts
//! those growth reallocs to one.
//!
//! This is a measurement-only test (it asserts a sane upper bound so a future
//! regression is caught), counted with the `allocation-counter` crate.

use oracledb_protocol::thin::{build_execute_payload_with_seq, ClientCapabilities};

#[test]
fn execute_payload_build_allocations() {
    let sql = "select 1 from dual";
    let ttc_field_version = ClientCapabilities::default().ttc_field_version;
    // Warm (touch the path once so any lazy statics are initialized).
    let _ = build_execute_payload_with_seq(sql, 1, 1, true, ttc_field_version).unwrap();

    let mut len = 0usize;
    let measured = allocation_counter::measure(|| {
        let payload = build_execute_payload_with_seq(sql, 1, 1, true, ttc_field_version).unwrap();
        len = std::hint::black_box(payload.len());
    });

    println!("\n===== execute payload build (select 1 from dual) =====");
    println!("payload bytes : {len}");
    println!(
        "allocations   : {} allocs, {} bytes",
        measured.count_total, measured.bytes_total
    );
    println!("======================================================\n");

    // The build is a single owned Vec<u8>; with preallocation it should be ONE
    // allocation (plus possibly the final shrink/no-op). Guard against the
    // growth-realloc regression: a zero-capacity writer grows through ~6
    // doublings = ~6 allocs for this payload.
    assert!(
        measured.count_total <= 2,
        "execute payload build should be <=2 allocations (got {})",
        measured.count_total
    );
}
