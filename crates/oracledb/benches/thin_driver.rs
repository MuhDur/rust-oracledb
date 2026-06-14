//! Criterion micro-benchmarks for the `oracledb` thin-mode driver against a
//! live Oracle container. These exercise the public crate API (the same code
//! paths `BlockingConnection` and the PyO3 shim drive) so the numbers are an
//! honest reflection of the driver as a Rust dependency would use it.
//!
//! Five operations are timed, mirroring `benches/compare_python_oracledb.py`:
//!
//!   1. `connect`        full TNS handshake: TCP connect + auth + logoff/close.
//!   2. `select_one_row` `select 1 from dual` execute + fetch (one round trip).
//!   3. `fetch_10k_rows` `connect by level <= 10000` execute + paged fetch.
//!   4. `executemany_1000` array-DML INSERT of 1000 rows into PERFTEST_BENCH.
//!   5. `read_clob`       select a CLOB locator + read its bytes over the wire.
//!
//! All but `connect` reuse a single warm connection across iterations, so they
//! measure per-operation protocol + codec cost rather than the handshake. The
//! `connect` bench deliberately includes TCP setup and teardown in every
//! sample.
//!
//! ## Running
//!
//! ```sh
//! eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1523 \
//!         ORACLEDB_HOST_PORT=1523 scripts/container.sh env)"
//! CARGO_TARGET_DIR=/home/you/.cache/cargo-target-w6perf \
//!   cargo bench -p oracledb --bench thin_driver
//! ```
//!
//! When the container environment is absent the harness prints a skip notice
//! and returns without registering any benchmark, so `cargo bench` stays green
//! offline (and does not hard-fail in CI).

use std::time::Duration;

use asupersync::runtime::{reactor, Runtime, RuntimeBuilder};
use asupersync::Cx;
use criterion::{criterion_group, criterion_main, Criterion};
use oracledb::protocol::thin::{decode_lob_text, BindValue, QueryValue, QueryValueRef};
use oracledb::{BlockingConnection, ConnectOptions, Connection};
use oracledb_protocol::ClientIdentity;

const PROGRAM: &str = "rust-oracledb-bench";
const MACHINE: &str = "bench-machine";
const OSUSER: &str = "bench-osuser";
const TERMINAL: &str = "bench-terminal";
const DRIVER: &str = "rust-oracledb thn : 0.0.0";

/// Scratch table for the executemany bench. The harness creates and drops it;
/// nothing else in the database is touched.
const SCRATCH_TABLE: &str = "PERFTEST_BENCH";

/// Build connect options from the harness container environment, or return
/// `None` so callers can self-skip when the container is not configured.
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

/// A current-thread Asupersync runtime, built once and reused to drive the
/// async driver API synchronously inside each criterion iteration. This is the
/// same runtime shape `BlockingConnection` builds internally; building it once
/// here keeps the per-call runtime construction out of the measured operation.
fn build_runtime() -> Runtime {
    let reactor = reactor::create_reactor().expect("native reactor builds for live I/O");
    RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("current-thread Asupersync runtime builds")
}

/// Run an async closure to completion on `runtime`, installing the ambient Cx.
fn block_on<F, T>(runtime: &Runtime, body: F) -> T
where
    F: AsyncFnOnce(&Cx) -> T,
{
    runtime.block_on(async {
        let cx = Cx::current().expect("block_on installs an ambient Cx");
        body(&cx).await
    })
}

/// Execute a no-bind query through the statement-cache path so the open server
/// cursor is reused across iterations (the equivalent of a prepared statement
/// re-executed on one cursor object). Without this the lower-level
/// `execute_query` allocates a fresh server cursor on every call and a long
/// bench run exhausts `open_cursors`.
async fn execute_cached(
    cx: &Cx,
    conn: &mut Connection,
    sql: &str,
    arraysize: u32,
) -> oracledb::protocol::thin::QueryResult {
    conn.execute_query_with_bind_rows(cx, sql, arraysize, &[])
        .await
        .expect("cached execute")
}

/// Drain a query that may return more rows than one batch holds, paging with
/// `fetch_rows` until the server reports no more rows. Returns the total row
/// count fetched. Mirrors the paged-fetch loop the PyO3 shim uses, including
/// releasing the open server cursor back to the statement cache once the query
/// is fully drained (the equivalent of closing a cursor object).
async fn fetch_all(cx: &Cx, conn: &mut Connection, sql: &str, arraysize: u32) -> usize {
    let first = execute_cached(cx, conn, sql, arraysize).await;
    let cursor_id = first.cursor_id;
    let mut total = first.rows.len();
    let mut more_rows = first.more_rows;
    let mut previous_row: Option<Vec<Option<QueryValue>>> = first.rows.last().cloned();
    while more_rows && cursor_id != 0 {
        let batch = conn
            .fetch_rows(cx, cursor_id, arraysize, previous_row.as_deref())
            .await
            .expect("fetch_rows page");
        total += batch.rows.len();
        more_rows = batch.more_rows;
        if let Some(last) = batch.rows.last().cloned() {
            previous_row = Some(last);
        }
    }
    conn.release_cursor(cursor_id);
    total
}

/// A small, deterministic CPU burst standing in for the work a real consumer
/// does per row (parse / transform / serialize). `work_units` scales it: 0 is
/// "just touch the cell", a few units is ~us-scale CPU. The overlap can only
/// hide the server round trip behind whatever CPU the page's decode + this work
/// take, so this is the lever that decides whether prefetch pays off.
#[inline]
fn per_row_work(seed: u64, work_units: u32) -> u64 {
    let mut h = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    for _ in 0..(40 * work_units) {
        h ^= h >> 30;
        h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        h ^= h >> 27;
    }
    h
}

/// SERIAL borrowed-fetch drain (no prefetch): page through the cursor with the
/// fused [`Connection::fetch_rows_ref`], which sends a FETCH and then reads its
/// response before issuing the next — read and decode are strictly serialized.
/// Runs `per_row_work(.., work_units)` for every row so the per-page CPU cost
/// matches the prefetched path exactly. This is the WITHOUT-overlap baseline.
async fn drain_serial_borrowed(
    cx: &Cx,
    conn: &mut Connection,
    sql: &str,
    arraysize: u32,
    work_units: u32,
) -> usize {
    let first = execute_cached(cx, conn, sql, arraysize).await;
    let cursor_id = first.cursor_id;
    let mut total = first.rows.len();
    let mut acc = 0u64;
    for row in &first.rows {
        acc = acc.wrapping_add(per_row_work(acc ^ row.len() as u64, work_units));
    }
    let mut more = first.more_rows;
    let mut prev: Option<Vec<Option<QueryValue>>> = first.rows.last().cloned();
    while more && cursor_id != 0 {
        let batch = conn
            .fetch_rows_ref(cx, cursor_id, arraysize, prev.as_deref())
            .await
            .expect("serial borrowed fetch page");
        more = batch.more_rows;
        let mut last: Option<Vec<Option<QueryValue>>> = None;
        let mut n = 0usize;
        batch
            .batch
            .for_each_row_ref(|row| {
                acc = acc.wrapping_add(per_row_work(acc ^ row.len() as u64, work_units));
                last = Some(row.iter().map(|c| c.map(|v| v.to_owned_value())).collect());
                n += 1;
                Ok::<(), oracledb::Error>(())
            })
            .expect("iterate serial borrowed batch");
        total += n;
        if let Some(l) = last {
            prev = Some(l);
        }
    }
    std::hint::black_box(acc);
    conn.release_cursor(cursor_id);
    total
}

/// PREFETCHED borrowed-fetch drain (one-page look-ahead overlap): the
/// production [`Connection::for_each_row_ref`] loop, which issues page K+1's
/// FETCH request before decoding page K so the wire round trip overlaps the
/// decode + the consumer's per-row work. Runs the identical `per_row_work` so
/// the only difference vs the serial drain is the overlap.
async fn drain_prefetched_borrowed(
    cx: &Cx,
    conn: &mut Connection,
    sql: &str,
    arraysize: u32,
    work_units: u32,
) -> usize {
    let mut total = 0usize;
    let mut acc = 0u64;
    conn.for_each_row_ref(cx, sql, arraysize, |row: &[Option<QueryValueRef<'_>>]| {
        acc = acc.wrapping_add(per_row_work(acc ^ row.len() as u64, work_units));
        total += 1;
        Ok(())
    })
    .await
    .expect("prefetched borrowed drain");
    std::hint::black_box(acc);
    total
}

/// Best-effort DDL: ignore "object does not exist" style failures so the
/// drop-then-create setup is idempotent across reruns.
fn ddl_best_effort(runtime: &Runtime, conn: &mut Connection, sql: &str) {
    block_on(runtime, async |cx| {
        let _ = conn.execute_query(cx, sql, 1).await;
    });
}

fn bench_thin_driver(c: &mut Criterion) {
    let Some(options) = connect_options() else {
        eprintln!(
            "skipped oracledb thin_driver benches: PYO_TEST_* environment not configured \
             (source scripts/container.sh env to run against the container)"
        );
        return;
    };

    let runtime = build_runtime();

    // ----------------------------------------------------------------------
    // (a) connect + auth + close: full TNS handshake, including TCP setup and
    //     teardown, measured fresh on every iteration.
    // ----------------------------------------------------------------------
    {
        let mut group = c.benchmark_group("oracledb_thin");
        group.bench_function("connect", |b| {
            b.iter(|| {
                block_on(&runtime, async |cx| {
                    let conn = Connection::connect(cx, options.clone())
                        .await
                        .expect("connect handshake");
                    conn.close(cx).await.expect("logoff/close");
                });
            });
        });
        group.finish();
    }

    // A single warm connection drives the remaining operation benches.
    let mut conn = block_on(&runtime, async |cx| {
        Connection::connect(cx, options.clone())
            .await
            .expect("warm connection for operation benches")
    });

    // ----------------------------------------------------------------------
    // (b) single-row SELECT: execute + fetch of `select 1 from dual`.
    // ----------------------------------------------------------------------
    {
        let mut group = c.benchmark_group("oracledb_thin");
        group.bench_function("select_one_row", |b| {
            b.iter(|| {
                block_on(&runtime, async |cx| {
                    let result = execute_cached(cx, &mut conn, "select 1 from dual", 1).await;
                    assert_eq!(result.rows.len(), 1, "select returns exactly one row");
                    // release the open cursor so the same SQL reuses it next
                    // iteration instead of parsing a fresh one (else the loop
                    // exhausts open_cursors). This is what closing a cursor
                    // object does in the shim.
                    conn.release_cursor(result.cursor_id);
                });
            });
        });
        group.finish();
    }

    // ----------------------------------------------------------------------
    // (b2) single-row SELECT through the synchronous `BlockingConnection`
    //      facade, the exact path the PyO3 shim drives for every suite
    //      operation. Unlike `select_one_row` above (which reuses one runtime
    //      via this bench's `block_on` helper), this measures the facade's own
    //      per-call runtime handling, so it reflects what a synchronous Rust
    //      caller — and the suite — actually pays. `execute_query` reuses the
    //      cursor through the statement cache, so the loop does not exhaust
    //      `open_cursors`.
    // ----------------------------------------------------------------------
    {
        let mut blocking_conn =
            BlockingConnection::connect(options.clone()).expect("blocking facade connection");
        let mut group = c.benchmark_group("oracledb_thin");
        group.bench_function("select_one_row_blocking", |b| {
            b.iter(|| {
                // Drive the statement-cache path (empty bind rows) so the open
                // server cursor is reused across iterations, exactly as the
                // async `select_one_row` bench does — the only difference being
                // that this goes through the synchronous facade.
                let result = BlockingConnection::execute_query_with_bind_rows(
                    &mut blocking_conn,
                    "select 1 from dual",
                    1,
                    &[],
                )
                .expect("blocking cached execute");
                assert_eq!(result.rows.len(), 1, "select returns exactly one row");
                // release the open cursor so the same SQL reuses it next
                // iteration instead of parsing a fresh one (else the loop
                // exhausts open_cursors). `release_cursor` is a synchronous
                // bookkeeping call; this is what closing a cursor object does in
                // the shim, mirroring the async `select_one_row` bench above.
                blocking_conn.release_cursor(result.cursor_id);
            });
        });
        group.finish();
        BlockingConnection::close(blocking_conn).expect("close blocking facade connection");
    }

    // ----------------------------------------------------------------------
    // (c) bulk fetch: 10000 rows via `connect by level`, execute + paged
    //     fetch with a 1000-row arraysize (so the loop pages ~10 times).
    // ----------------------------------------------------------------------
    {
        let sql = "select level as n from dual connect by level <= 10000";
        let mut group = c.benchmark_group("oracledb_thin");
        group.sample_size(30);
        group.bench_function("fetch_10k_rows", |b| {
            b.iter(|| {
                let total = block_on(&runtime, async |cx| {
                    fetch_all(cx, &mut conn, sql, 1000).await
                });
                assert_eq!(total, 10_000, "bulk fetch drains all 10000 rows");
            });
        });
        group.finish();
    }

    // ----------------------------------------------------------------------
    // (d) executemany INSERT: 1000 bind rows into the scratch table in one
    //     array-DML execute. The table is rolled back each iteration so it
    //     stays empty and the cost is the insert path, not table growth.
    // ----------------------------------------------------------------------
    {
        ddl_best_effort(
            &runtime,
            &mut conn,
            &format!("drop table {SCRATCH_TABLE} purge"),
        );
        block_on(&runtime, async |cx| {
            conn.execute_query(
                cx,
                &format!("create table {SCRATCH_TABLE} (id number(9), label varchar2(40))"),
                1,
            )
            .await
            .expect("create scratch table");
        });

        let rows: Vec<Vec<BindValue>> = (0..1000)
            .map(|i| {
                vec![
                    BindValue::Number(i.to_string()),
                    BindValue::Text(format!("row-{i:05}")),
                ]
            })
            .collect();
        let insert_sql = format!("insert into {SCRATCH_TABLE} (id, label) values (:1, :2)");

        let mut group = c.benchmark_group("oracledb_thin");
        group.sample_size(30);
        group.bench_function("executemany_1000", |b| {
            b.iter(|| {
                block_on(&runtime, async |cx| {
                    let result = conn
                        .execute_query_with_bind_rows(cx, &insert_sql, 1, &rows)
                        .await
                        .expect("array-DML insert of 1000 rows");
                    assert_eq!(result.row_count, 1000, "executemany inserts all 1000 rows");
                    // roll back so the next iteration inserts into an empty table
                    conn.rollback(cx).await.expect("rollback scratch insert");
                });
            });
        });
        group.finish();
    }

    // ----------------------------------------------------------------------
    // (e) CLOB read: select a CLOB locator (via the define-fetch collect path)
    //     then read its bytes over the wire and decode to text. A ~64 KiB CLOB
    //     gives the read a non-trivial body to stream.
    // ----------------------------------------------------------------------
    {
        ddl_best_effort(&runtime, &mut conn, "drop table PERFTEST_CLOB purge");
        block_on(&runtime, async |cx| {
            conn.execute_query(
                cx,
                "create table PERFTEST_CLOB (id number(9), body clob)",
                1,
            )
            .await
            .expect("create clob table");
            // Build a real 64 KiB CLOB by appending 1024-char chunks in PL/SQL:
            // a bare SQL rpad() caps at the 4000-char VARCHAR2 limit, so it
            // cannot stand in for a large LOB. 64 chunks of 1024 chars give
            // exactly 65536 characters.
            conn.execute_query(
                cx,
                "declare \
                   l_body clob; \
                   l_chunk varchar2(1024) := rpad('the quick brown fox jumps over \
                     the lazy dog 0123456789ABCDEF', 1024, 'x'); \
                 begin \
                   dbms_lob.createtemporary(l_body, true); \
                   for i in 1 .. 64 loop \
                     dbms_lob.append(l_body, to_clob(l_chunk)); \
                   end loop; \
                   insert into PERFTEST_CLOB values (1, l_body); \
                   dbms_lob.freetemporary(l_body); \
                 end;",
                1,
            )
            .await
            .expect("insert 64 KiB clob");
            conn.commit(cx).await.expect("commit clob");
        });

        let mut group = c.benchmark_group("oracledb_thin");
        group.bench_function("read_clob", |b| {
            b.iter(|| {
                block_on(&runtime, async |cx| {
                    // `execute_query_collect` opens a fresh server cursor and
                    // performs the client-side define-fetch a CLOB column needs
                    // to materialize its locator in the first batch. The cursor
                    // is non-cached, so close it explicitly afterward; the close
                    // rides the next execute's piggyback, keeping at most one
                    // CLOB cursor open at a time over the whole bench loop.
                    let select = conn
                        .execute_query_collect(cx, "select body from PERFTEST_CLOB where id = 1", 2)
                        .await
                        .expect("select clob locator");
                    let select_cursor = select.cursor_id;
                    let (locator, size, csfrm) = match select.cell(0, 0) {
                        Some(QueryValue::Lob(lob)) => (lob.locator.clone(), lob.size, lob.csfrm),
                        other => panic!("expected a LOB locator, got {other:?}"),
                    };
                    let read = conn
                        .read_lob(cx, &locator, 1, size)
                        .await
                        .expect("read_lob round trip");
                    let bytes = read.data.expect("clob read returns data");
                    let text =
                        decode_lob_text(&bytes, csfrm, Some(&locator)).expect("decode clob text");
                    // bench builds are release-optimized: assert (not
                    // debug_assert) so the 64 KiB read is genuinely verified.
                    assert_eq!(text.len(), 65_536, "clob read returns the full body");
                    conn.close_cursor(select_cursor);
                });
            });
        });
        group.finish();
    }

    // ----------------------------------------------------------------------
    // (f) PREFETCH OVERLAP: single-connection multi-page fetch WITHOUT vs WITH
    //     speculative next-page prefetch (bead xad / 3oi). Both drain the SAME
    //     large many-page result and touch every borrowed cell, so the only
    //     difference is whether page K+1's wire round trip overlaps page K's
    //     decode. A bigger arraysize-to-rows ratio => more pages => more overlap
    //     opportunities. 50000 rows / arraysize 1000 ≈ 50 pages.
    //
    //     CAVEAT (honesty): on loopback the socket read latency is tiny, so the
    //     win here is bounded by the decode fraction (~30% of read+decode per the
    //     attribution example). On a real network the per-page read is dominated
    //     by RTT (>> the ~324 us loopback read), so the prefetch hides close to a
    //     full RTT per page and the speedup is strictly larger than measured here.
    // ----------------------------------------------------------------------
    {
        let sql = "select level as n, rpad('row', 40, to_char(level)) as label \
                   from dual connect by level <= 50000";
        let arraysize = 1000u32;

        let mut group = c.benchmark_group("oracledb_prefetch");
        group.sample_size(40);

        // (i) Trivial consumer (work_units = 0): the page's decode is the only
        //     CPU the overlap can hide behind. On loopback the hideable read-wait
        //     is small, so this pair is roughly break-even (the prefetch
        //     bookkeeping ≈ the saved latency). This is the HONEST loopback floor.
        group.bench_function("fetch_50k_serial_trivial", |b| {
            b.iter(|| {
                let total = block_on(&runtime, async |cx| {
                    drain_serial_borrowed(cx, &mut conn, sql, arraysize, 0).await
                });
                assert_eq!(total, 50_000, "serial drain reads all rows");
            });
        });
        group.bench_function("fetch_50k_prefetched_trivial", |b| {
            b.iter(|| {
                let total = block_on(&runtime, async |cx| {
                    drain_prefetched_borrowed(cx, &mut conn, sql, arraysize, 0).await
                });
                assert_eq!(total, 50_000, "prefetched drain reads all rows");
            });
        });

        // (ii) Realistic consumer (work_units = 1, ~few us/row): a real caller
        //      does work per row. Now the per-page decode + consumer work covers
        //      the server round trip, so the overlap pays off and the prefetched
        //      path is materially faster — even on loopback. (On real-network RTT
        //      even the trivial pair wins, since read-wait is RTT-dominated.)
        group.bench_function("fetch_50k_serial_work", |b| {
            b.iter(|| {
                let total = block_on(&runtime, async |cx| {
                    drain_serial_borrowed(cx, &mut conn, sql, arraysize, 1).await
                });
                assert_eq!(total, 50_000, "serial drain reads all rows");
            });
        });
        group.bench_function("fetch_50k_prefetched_work", |b| {
            b.iter(|| {
                let total = block_on(&runtime, async |cx| {
                    drain_prefetched_borrowed(cx, &mut conn, sql, arraysize, 1).await
                });
                assert_eq!(total, 50_000, "prefetched drain reads all rows");
            });
        });

        group.finish();
    }

    // ----------------------------------------------------------------------
    // (g) COLUMNAR fetch->Arrow (bead rust-oracledb-wf7): the row-materialize
    //     `fetch_all_record_batch` vs the columnar `fetch_all_record_batch_columnar`
    //     over the SAME wide analytics result. Both produce a byte-identical
    //     RecordBatch (asserted by tests/arrow_columnar_diff.rs); the columnar
    //     path streams each borrowed cell straight into the column builders, so
    //     it skips the per-row Vec<Option<QueryValue>>, the per-text-cell String,
    //     and the transpose pass (95.3% fewer allocations — see
    //     tests/arrow_columnar_alloc.rs). On loopback the wall delta is bounded
    //     by the client decode/build share (~27% of a wide fetch; the rest is
    //     server read-wait), so this measures the beatable client-CPU slice.
    //     Only compiled with the `arrow` feature.
    // ----------------------------------------------------------------------
    #[cfg(feature = "arrow")]
    {
        use oracledb::arrow::ArrowFetchOptions;
        let sql = "select \
                   level as id, \
                   cast(level * 1.25 as number(18,4)) as amount, \
                   rpad('row', 32, to_char(mod(level, 9))) as label, \
                   mod(level, 1000) as bucket, \
                   to_char(level) as code, \
                   cast(level as number(18,2)) as price \
                   from dual connect by level <= 20000";
        let arraysize = 1000u32;
        let arrow_options = ArrowFetchOptions::default();

        // Each arm gets its OWN fresh connection so neither inherits the cursors
        // the shared warm connection accumulated across the earlier setup (CLOB
        // temp LOBs, executemany DDL), and so the two arms cannot interact
        // through one session's statement cache / open_cursors ceiling. Both
        // methods release their drained cursor (verified by the leak-probe tests
        // in tests/arrow_columnar_diff.rs), so each session reuses a single
        // server cursor across all iterations.
        let mut row_conn = block_on(&runtime, async |cx| {
            Connection::connect(cx, options.clone())
                .await
                .expect("row-path df connection")
        });
        let mut col_conn = block_on(&runtime, async |cx| {
            Connection::connect(cx, options.clone())
                .await
                .expect("columnar df connection")
        });

        let mut group = c.benchmark_group("oracledb_columnar");
        group.sample_size(30);
        group.bench_function("fetch_df_row_path", |b| {
            b.iter(|| {
                let batch = block_on(&runtime, async |cx| {
                    row_conn
                        .fetch_all_record_batch(cx, sql, arraysize, &arrow_options)
                        .await
                        .expect("row-path fetch_df_all")
                });
                assert_eq!(batch.num_rows(), 20_000);
            });
        });
        group.bench_function("fetch_df_columnar", |b| {
            b.iter(|| {
                let batch = block_on(&runtime, async |cx| {
                    col_conn
                        .fetch_all_record_batch_columnar(cx, sql, arraysize, &arrow_options)
                        .await
                        .expect("columnar fetch_df_all")
                });
                assert_eq!(batch.num_rows(), 20_000);
            });
        });
        group.finish();
        block_on(&runtime, async |cx| {
            row_conn.close(cx).await.expect("close row df connection");
            col_conn
                .close(cx)
                .await
                .expect("close columnar df connection");
        });
    }

    // Cleanup: drop scratch objects and close the warm connection. Only
    // PERFTEST_* objects this harness created are touched.
    ddl_best_effort(
        &runtime,
        &mut conn,
        &format!("drop table {SCRATCH_TABLE} purge"),
    );
    ddl_best_effort(&runtime, &mut conn, "drop table PERFTEST_CLOB purge");
    block_on(&runtime, async |cx| {
        conn.close(cx).await.expect("close warm connection");
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(8));
    targets = bench_thin_driver
}
criterion_main!(benches);
