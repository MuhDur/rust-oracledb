//! Concurrent-throughput benchmark for the `oracledb` thin-mode driver.
//!
//! This is the missing half of `docs/PERFORMANCE.md`: that comparison is
//! single-connection and serial, and it says so. It measures per-operation
//! latency on one warm connection and explicitly disclaims any throughput or
//! concurrency claim. This harness measures the thing that comparison cannot:
//! how aggregate decode throughput scales when N worker threads each drive their
//! own connection in parallel.
//!
//! The hypothesis under test is the one honest gap in the docs. The Rust driver
//! has no GIL: each [`BlockingConnection`] runs on its own current-thread
//! Asupersync runtime (one epoll reactor + one worker OS thread, cached in a
//! `thread_local`), with its own TCP socket, and decodes rows on the calling
//! thread. N worker threads therefore have N fully independent I/O + codec
//! pipelines sharing nothing, so a CPU-bound decode workload should scale close
//! to linearly with cores. python-oracledb thin, by contrast, runs its
//! pure-Python protocol/codec under the CPython GIL, so the same CPU-bound
//! decode cannot run two threads' worth of codec at once. `benches/
//! compare_concurrent_python.py` measures that side; this file measures Rust.
//!
//! ## The workload is deliberately decode-bound, not server-bound
//!
//! A throughput comparison is only meaningful if the bottleneck is client-side
//! CPU (where the GIL bites), not the server or the wire (where neither driver
//! can help). The first cut of this bench used a `connect by level` row source,
//! and that turned out to be a trap: generating 5000 rows × 20 expression
//! columns is *server* CPU, and on a single container it serialized — aggregate
//! throughput peaked at 4 workers and then collapsed while the container, not
//! the client, sat pinned at multiple cores. That is measuring the database, not
//! the driver, so it was discarded.
//!
//! The honest workload pre-populates a small wide table ([`WORKLOAD_ROWS`] rows
//! × [`WORKLOAD_COLS`] columns of `NUMBER` + `VARCHAR2`) exactly once, warms it
//! into the server buffer cache, and then each worker repeatedly scans it with
//! `select *`. Now the server side is a buffer-cache block read plus wire
//! serialization — cheap, and (verified by a no-GIL multi-process probe) it
//! scales across parallel sessions to ~6× at 8 sessions before the single
//! container tails off. The expensive part is on the *client*: every `NUMBER`
//! cell forces a parse of Oracle's base-100 mantissa/exponent bytes into
//! lossless decimal text, and every `VARCHAR2` cell forces a UTF-8 validation +
//! `String` build. With the server able to feed parallel clients, the client
//! decoder is the bottleneck — which is precisely where the GIL decides whether
//! throughput scales.
//!
//! The table is sized so each scan returns in a single fetch batch. Past roughly
//! 1500 of these 20-column rows the `select *` payload spans several network
//! packets, and the current thin decoder mis-frames that multi-packet
//! wide-row continuation (`encoded NUMBER too long` / `truncated TTC payload`).
//! That is a real driver limitation, recorded in `docs/PERFORMANCE.md`; the
//! bench stays inside the single-batch envelope so it measures decode
//! throughput rather than that bug.
//!
//! Each worker scans the table in a tight loop for a fixed wall-clock window and
//! counts the rows it decoded. Aggregate throughput is the sum across workers;
//! the scaling factor is throughput(N) / throughput(1).
//!
//! ## Running
//!
//! ```sh
//! eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1526 \
//!         ORACLEDB_HOST_PORT=1526 scripts/container.sh env)"
//! CARGO_TARGET_DIR=/path/to/target \
//!   cargo bench -p oracledb --bench concurrent_throughput
//! ```
//!
//! When the container environment is absent the harness prints a skip notice and
//! returns cleanly, so `cargo bench` stays green offline. It is a plain `main`
//! (criterion is the wrong tool for fixed-N aggregate throughput across OS
//! threads), wired through `harness = false`.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use oracledb::protocol::thin::{QueryResult, QueryValue};
use oracledb::{BlockingConnection, ConnectOptions, Connection};
use oracledb_protocol::ClientIdentity;

const PROGRAM: &str = "rust-oracledb-concbench";
const MACHINE: &str = "bench-machine";
const OSUSER: &str = "bench-osuser";
const TERMINAL: &str = "bench-terminal";
const DRIVER: &str = "rust-oracledb thn : 0.0.0";

/// Worker counts to sweep. The hypothesis is that aggregate rows/sec rises with
/// N (no GIL) where python-oracledb threads plateau.
const WORKER_COUNTS: &[usize] = &[1, 2, 4, 8, 16];

/// Rows in the pre-populated scan table. Small enough to stay fully in the
/// server buffer cache (so scans are memory reads, not disk I/O) and to return
/// in a single fetch batch, large enough that one scan is dominated by decode
/// rather than per-statement overhead (verified: per-row throughput is flat from
/// 100 to 1000 rows, so decode, not statement setup, is the cost).
///
/// It is also deliberately *below* the point where the driver's multi-packet
/// wide-row reassembly desyncs. A `select *` of this 20-column row spans several
/// network packets past ~1500 rows, and the current thin decoder mis-frames the
/// continuation there (`encoded NUMBER too long` / `truncated TTC payload`).
/// That is a real driver limitation on wide multi-packet result sets, noted in
/// `docs/PERFORMANCE.md`; this bench stays inside the single-batch envelope so
/// it measures decode throughput, not that bug. Fixing the decoder is out of
/// scope for an additive benchmark.
const WORKLOAD_ROWS: u32 = 1_000;

/// Total columns per row: the first half `NUMBER`, the second half `VARCHAR2`.
/// Must be even. More columns = more client decode per row fetched, pushing the
/// bottleneck onto the codec. Kept in sync with the Python harness.
const WORKLOAD_COLS: usize = 20;

/// Name of the pre-populated scan table. Created and dropped by this harness;
/// nothing else in the schema is touched. Matches the Python harness so a run of
/// either side leaves the same well-known object.
const SCAN_TABLE: &str = "PERFTEST_CONC";

/// Arraysize for the scan. Set above [`WORKLOAD_ROWS`] so the whole table comes
/// back in one fetch batch (no paging, which also avoids the multi-packet
/// continuation path noted above). The per-iteration cost is then purely
/// execute + single-batch decode.
const WORKLOAD_ARRAYSIZE: u32 = WORKLOAD_ROWS + 500;

/// Measured window per worker count. Each worker fetches in a loop until the
/// window elapses; throughput is rows-decoded / elapsed.
const MEASURE_SECS: u64 = 6;

/// Unmeasured warmup before the window opens (statement cache, JIT of the decode
/// path, TCP window ramp).
const WARMUP_SECS: u64 = 2;

/// The decode-heavy SELECT every worker runs in its loop: a full scan of the
/// pre-populated table. The server reads buffer-cached blocks and serializes
/// them; the client decodes every cell. `select *` (rather than naming columns)
/// keeps the statement short and matches the Python harness.
fn scan_sql() -> String {
    format!("select * from {SCAN_TABLE}")
}

/// Number of `NUMBER` columns (first half of the row); the rest are `VARCHAR2`.
const NUM_COLS: usize = WORKLOAD_COLS / 2;

/// `create table` DDL for the scan table: `NUM_COLS` `NUMBER` columns followed
/// by the same count of `VARCHAR2(40)` columns.
fn create_table_sql() -> String {
    let mut cols = Vec::with_capacity(WORKLOAD_COLS);
    for i in 0..NUM_COLS {
        cols.push(format!("n{i} number"));
    }
    for i in 0..NUM_COLS {
        cols.push(format!("v{i} varchar2(40)"));
    }
    format!("create table {SCAN_TABLE} ({})", cols.join(", "))
}

/// `insert ... select` DDL that fills the table from a `connect by level` source
/// in a single statement. This runs *once* during setup (not in the measured
/// loop), so its server cost does not enter the throughput numbers. Each
/// `NUMBER` is a distinct multi-digit value and each `VARCHAR2` a distinct
/// ~40-char string, so the per-cell decode on every later scan is real work, not
/// a degenerate single byte.
fn populate_sql() -> String {
    let mut exprs = Vec::with_capacity(WORKLOAD_COLS);
    for i in 0..NUM_COLS {
        // Large, distinct integers so the base-100 NUMBER decoder walks several
        // mantissa bytes per cell.
        exprs.push(format!(
            "level * {} + {}",
            1_000_003 + i as i64 * 17,
            i * 31
        ));
    }
    for i in 0..NUM_COLS {
        exprs.push(format!("rpad('row' || to_char(level) || '-c{i}', 40, 'x')"));
    }
    format!(
        "insert /*+ append */ into {SCAN_TABLE} select {} from dual connect by level <= {WORKLOAD_ROWS}",
        exprs.join(", ")
    )
}

/// Build connect options from the harness container environment, or `None` so
/// the bench can self-skip when the container is not configured.
fn connect_options() -> Option<ConnectOptions> {
    let connect_string = std::env::var("PYO_TEST_CONNECT_STRING").ok()?;
    let user = std::env::var("PYO_TEST_MAIN_USER").ok()?;
    let password = std::env::var("PYO_TEST_MAIN_PASSWORD").ok()?;
    let identity = ClientIdentity::new(PROGRAM, MACHINE, OSUSER, TERMINAL, DRIVER).ok()?;
    Some(ConnectOptions::new(
        connect_string,
        user,
        password,
        identity,
    ))
}

/// Drain one execution of `sql` fully, decoding every cell, and return the row
/// count. `release_cursor` returns the open server cursor to the statement cache
/// so the next iteration reuses it (one parse, not one per loop) — matching how
/// python-oracledb reuses a cursor object.
///
/// Crucially this *touches* every cell (`as_i64` / `as_text`) so the optimizer
/// cannot elide the decode the benchmark exists to measure.
fn run_workload_once(conn: &mut Connection, sql: &str) -> u64 {
    let first =
        BlockingConnection::execute_query_with_bind_rows(conn, sql, WORKLOAD_ARRAYSIZE, &[])
            .expect("decode-heavy execute");
    let cursor_id = first.cursor_id;
    let mut total = touch_all_cells(&first);
    let mut more_rows = first.more_rows;
    let mut previous_row: Option<Vec<Option<QueryValue>>> = first.rows.last().cloned();
    while more_rows && cursor_id != 0 {
        let batch = BlockingConnection::fetch_rows(
            conn,
            cursor_id,
            WORKLOAD_ARRAYSIZE,
            previous_row.as_deref(),
        )
        .expect("decode-heavy fetch page");
        total += touch_all_cells(&batch);
        more_rows = batch.more_rows;
        if let Some(last) = batch.rows.last().cloned() {
            previous_row = Some(last);
        }
    }
    conn.release_cursor(cursor_id);
    total
}

/// Read every cell of every row through the typed accessors so the decoded
/// `NUMBER` text and `VARCHAR2` string are actually consumed. Returns the row
/// count. `black_box` on the folded result prevents the whole loop from being
/// optimized away.
fn touch_all_cells(result: &QueryResult) -> u64 {
    let mut sink: u64 = 0;
    for row in 0..result.rows.len() {
        for col in 0..WORKLOAD_COLS {
            if let Some(value) = result.cell(row, col) {
                if let Some(n) = value.as_i64() {
                    sink = sink.wrapping_add(n as u64);
                } else if let Some(text) = value.as_text() {
                    sink = sink.wrapping_add(text.len() as u64);
                }
            }
        }
    }
    std::hint::black_box(sink);
    result.rows.len() as u64
}

/// Create and populate the scan table once, on a dedicated connection, before
/// any worker starts. Drops a stale table first so reruns are idempotent. The
/// table is small enough to live in the server buffer cache; this setup is the
/// only place its rows are generated, so the generation cost never enters the
/// measured throughput.
fn setup_table(options: &ConnectOptions) -> Result<(), oracledb::Error> {
    let mut conn = BlockingConnection::connect(options.clone())?;
    // Best-effort drop: a missing table is fine.
    let _ =
        BlockingConnection::execute_query(&mut conn, &format!("drop table {SCAN_TABLE} purge"), 1);
    BlockingConnection::execute_query(&mut conn, &create_table_sql(), 1)?;
    BlockingConnection::execute_query(&mut conn, &populate_sql(), 1)?;
    BlockingConnection::commit(&mut conn)?;
    // Warm the buffer cache so the first measured scans are memory reads.
    let scan = scan_sql();
    for _ in 0..3 {
        run_workload_once(&mut conn, &scan);
    }
    BlockingConnection::close(conn)?;
    Ok(())
}

/// Drop the scan table. Best-effort: a failure here does not invalidate the
/// numbers already printed.
fn teardown_table(options: &ConnectOptions) {
    if let Ok(mut conn) = BlockingConnection::connect(options.clone()) {
        let _ = BlockingConnection::execute_query(
            &mut conn,
            &format!("drop table {SCAN_TABLE} purge"),
            1,
        );
        let _ = BlockingConnection::close(conn);
    }
}

/// Per-worker result: rows decoded and the wall-clock the worker spent inside
/// the measured window.
struct WorkerResult {
    rows: u64,
    elapsed: Duration,
}

/// Spawn `n` worker threads, each with its own connection, all fetching the
/// decode-heavy workload in lockstep. A [`Barrier`] aligns the start so every
/// worker is in its loop before the clock starts; a shared deadline flag stops
/// them together. Returns each worker's `(rows, elapsed)`.
fn run_worker_set(options: &ConnectOptions, sql: &str, n: usize) -> Vec<WorkerResult> {
    let barrier = Arc::new(Barrier::new(n + 1));
    let stop = Arc::new(AtomicBool::new(false));
    // Track how many workers actually have a live connection; if any failed to
    // connect we still release the barrier so we never deadlock.
    let ready = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let options = options.clone();
        let sql = sql.to_string();
        let barrier = Arc::clone(&barrier);
        let stop = Arc::clone(&stop);
        let ready = Arc::clone(&ready);
        handles.push(thread::spawn(move || -> WorkerResult {
            // Each worker owns its connection (and, on first BlockingConnection
            // call on this thread, its own cached current-thread runtime: one
            // epoll reactor + one worker OS thread). No state is shared between
            // workers, which is the whole point.
            let mut conn = match BlockingConnection::connect(options) {
                Ok(conn) => conn,
                Err(err) => {
                    eprintln!("worker connect failed: {err}");
                    // Still join the barrier so the coordinator does not hang.
                    barrier.wait();
                    return WorkerResult {
                        rows: 0,
                        elapsed: Duration::ZERO,
                    };
                }
            };
            ready.fetch_add(1, Ordering::Relaxed);

            // Warmup: prime the statement cache and the decode path before the
            // window opens, outside the measured count.
            let warm_deadline = Instant::now() + Duration::from_secs(WARMUP_SECS);
            while Instant::now() < warm_deadline {
                run_workload_once(&mut conn, &sql);
            }

            // Align all workers, then measure until the shared stop flips.
            barrier.wait();
            let start = Instant::now();
            let mut rows: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                rows += run_workload_once(&mut conn, &sql);
            }
            let elapsed = start.elapsed();

            let _ = BlockingConnection::close(conn);
            WorkerResult { rows, elapsed }
        }));
    }

    // Wait for every worker to finish warmup and reach the barrier, then time
    // the window from the coordinator side and signal stop.
    barrier.wait();
    thread::sleep(Duration::from_secs(MEASURE_SECS));
    stop.store(true, Ordering::Relaxed);

    let connected = ready.load(Ordering::Relaxed);
    if connected != n as u64 {
        eprintln!(
            "warning: only {connected}/{n} workers connected; throughput for N={n} is understated"
        );
    }

    handles
        .into_iter()
        .map(|h| h.join().expect("worker thread joins"))
        .collect()
}

fn main() {
    let Some(options) = connect_options() else {
        eprintln!(
            "skipped concurrent_throughput bench: PYO_TEST_* environment not configured \
             (source scripts/container.sh env to run against the container)"
        );
        return;
    };

    // One-time setup: create, populate, and warm the scan table. Its row
    // generation cost stays out of the measured loop.
    if let Err(err) = setup_table(&options) {
        eprintln!("skipped concurrent_throughput bench: table setup failed: {err}");
        return;
    }

    let sql = scan_sql();
    let host_threads = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);

    println!("rust-oracledb concurrent throughput (thin mode)");
    println!(
        "host logical CPUs: {host_threads}   workload: scan of {SCAN_TABLE} \
         ({WORKLOAD_ROWS} rows x {WORKLOAD_COLS} cols, half NUMBER half VARCHAR2), \
         arraysize {WORKLOAD_ARRAYSIZE}"
    );
    println!("measure window: {MEASURE_SECS}s per N (after {WARMUP_SECS}s warmup)\n");
    println!(
        "{:>3}  {:>16}  {:>14}  {:>10}  {:>12}",
        "N", "rows/sec (aggregate)", "rows/sec/worker", "scaling", "efficiency"
    );

    let mut baseline_throughput: Option<f64> = None;
    for &n in WORKER_COUNTS {
        let results = run_worker_set(&options, &sql, n);
        // Aggregate throughput: total rows decoded across all workers divided by
        // the longest worker window (they start together via the barrier and
        // stop together via the flag, so the windows are within a fetch of each
        // other; using the max is the conservative, honest denominator).
        let total_rows: u64 = results.iter().map(|r| r.rows).sum();
        let max_elapsed = results
            .iter()
            .map(|r| r.elapsed)
            .max()
            .unwrap_or(Duration::from_secs(MEASURE_SECS))
            .as_secs_f64()
            .max(f64::MIN_POSITIVE);
        let throughput = total_rows as f64 / max_elapsed;
        let per_worker = throughput / n as f64;

        let base = *baseline_throughput.get_or_insert(throughput);
        let scaling = throughput / base;
        let efficiency = scaling / n as f64;

        println!(
            "{n:>3}  {throughput:>16.0}  {per_worker:>14.0}  {scaling:>9.2}x  {:>11.0}%",
            efficiency * 100.0
        );
    }

    println!(
        "\nscaling = throughput(N) / throughput(1); efficiency = scaling / N \
         (100% = perfect linear, no-GIL ideal)."
    );

    teardown_table(&options);
}
