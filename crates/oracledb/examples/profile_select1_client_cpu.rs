//! Profiling-only (STEP 3): isolate the CLIENT-CPU slice of the select-1 hot
//! path — the synchronous facade the PyO3 shim drives — so the micro-opt has a
//! target. `select 1 from dual` is round-trip-bound (STEP 1: ~145us/call, ~all
//! the one server round trip), so the only BEATABLE part is the per-call client
//! work: the `BlockingConnection` dispatch (build_io_runtime TLS borrow +
//! Runtime::clone + block_on + Cx::current) plus any per-call allocation in the
//! execute-encode / one-row-decode.
//!
//! This harness reports, on a warm connection:
//!   * the per-call wall (the round-trip floor we cannot beat), and
//!   * the per-call ALLOCATION count (the beatable client work — every malloc
//!     here is pure client CPU the server never sees).
//!
//! Run:
//! ```sh
//! eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1523 \
//!         ORACLEDB_HOST_PORT=1523 scripts/container.sh env)"
//! cargo run -p oracledb --example profile_select1_client_cpu --release
//! ```

use std::time::Instant;

use oracledb::protocol::ClientIdentity;
use oracledb::{BlockingConnection, ConnectOptions};

fn connect_options() -> Option<ConnectOptions> {
    let connect_string = std::env::var("PYO_TEST_CONNECT_STRING").ok()?;
    let user = std::env::var("PYO_TEST_MAIN_USER").ok()?;
    let password = std::env::var("PYO_TEST_MAIN_PASSWORD").ok()?;
    let identity = ClientIdentity::new(
        "rust-oracledb-sel1",
        "sel1-machine",
        "sel1-osuser",
        "sel1-terminal",
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

fn main() {
    let Some(options) = connect_options() else {
        eprintln!("skipped profile_select1_client_cpu: PYO_TEST_* not set");
        return;
    };
    let mut conn = BlockingConnection::connect(options).expect("connect");

    // Warm the statement cache + server-side parse.
    for _ in 0..500 {
        let r = BlockingConnection::execute_query_with_bind_rows(
            &mut conn,
            "select 1 from dual",
            1,
            &[],
        )
        .expect("warm");
        conn.release_cursor(r.cursor_id);
    }

    // --- Per-call ALLOCATION count: the beatable client work. ---
    // Measure one steady-state call (warm) so the count reflects the per-call
    // client allocations, not first-time cache growth.
    let measured = allocation_counter::measure(|| {
        let r = BlockingConnection::execute_query_with_bind_rows(
            &mut conn,
            "select 1 from dual",
            1,
            &[],
        )
        .expect("select 1");
        std::hint::black_box(r.cursor_id);
        // NOTE: release_cursor is a synchronous bookkeeping call (no round trip);
        // include it so the count reflects the full shim per-call path.
        conn.release_cursor(r.cursor_id);
    });

    // --- Per-call wall (the round-trip floor). ---
    let iters = 4000u32;
    let mut samples = Vec::with_capacity(iters as usize);
    for _ in 0..iters {
        let t0 = Instant::now();
        let r = BlockingConnection::execute_query_with_bind_rows(
            &mut conn,
            "select 1 from dual",
            1,
            &[],
        )
        .expect("select 1");
        let dt = t0.elapsed();
        conn.release_cursor(r.cursor_id);
        samples.push(dt.as_nanos() as f64 / 1e3); // us
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p50 = samples[samples.len() / 2];
    let p10 = samples[samples.len() / 10];
    let p90 = samples[samples.len() * 9 / 10];

    println!("=== select-1 client-CPU profile (warm BlockingConnection) ===");
    println!("per-call wall  p10/p50/p90 : {p10:.1} / {p50:.1} / {p90:.1} us");
    println!(
        "per-call allocations         : {} allocs, {} bytes  <- BEATABLE client work",
        measured.count_total, measured.bytes_total
    );
    println!(
        "  (the wall is ~all the one server round trip — unbeatable; the allocations are pure client CPU)"
    );

    BlockingConnection::close(conn).expect("close");
}
