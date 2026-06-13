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
use oracledb::protocol::thin::{decode_lob_text, BindValue, QueryValue};
use oracledb::{ConnectOptions, Connection};
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
                        Some(QueryValue::Lob {
                            locator,
                            size,
                            csfrm,
                            ..
                        }) => (locator.clone(), *size, *csfrm),
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
