//! Profiling-only (STEP 1 of the perf-push lane): attribute the five canonical
//! driver operations into server-round-trip (unbeatable) vs client-CPU
//! (decode/encode/alloc/runtime block_on, beatable) on a stable warm base.
//!
//! This is the input to the columnar-Arrow (wf7/0mk) and micro-opt (STEP 3)
//! work: it decides where client CPU actually lives. It is NOT a benchmark of
//! the driver vs python-oracledb (see `benches/thin_driver.rs` and
//! `docs/PERFORMANCE.md` for that); it is an internal attribution map.
//!
//! Operations measured (warm connection, statement cache hot, loopback):
//!   1. select_one_row     `select 1 from dual` (one round trip) — RT-bound floor
//!   2. fetch_10k_rows     `connect by level <= 10000`, arraysize 1000
//!   3. fetch_wide_analytics  10 typed columns x many rows (NUMBER/VARCHAR/DATE)
//!   4. executemany_1000   array-DML INSERT of 1000 rows + rollback
//!   5. (connect is measured separately and may be skipped under listener churn)
//!
//! For the fetch ops we additionally read the crate's `fetch_profile_*`
//! read-wait/decode counters to split socket-read-wait from decode-CPU
//! explicitly (loopback: read-wait is small RTT, decode-CPU is the beatable
//! slice; off-loopback the read term grows with RTT and the split shifts).
//!
//! Run:
//! ```sh
//! eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1523 \
//!         ORACLEDB_HOST_PORT=1523 scripts/container.sh env)"
//! cargo run -p oracledb --example perf_attribution_map --release
//! ```

use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::Cx;
use oracledb::protocol::thin::{BindValue, QueryValue};
use oracledb::{ConnectOptions, Connection};
use oracledb_protocol::ClientIdentity;

fn connect_options() -> Option<ConnectOptions> {
    let connect_string = std::env::var("PYO_TEST_CONNECT_STRING").ok()?;
    let user = std::env::var("PYO_TEST_MAIN_USER").ok()?;
    let password = std::env::var("PYO_TEST_MAIN_PASSWORD").ok()?;
    let identity = ClientIdentity::new(
        "rust-oracledb-attrib",
        "attrib-machine",
        "attrib-osuser",
        "attrib-terminal",
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

use std::time::Instant;

/// Median of a vector of f64 (mutates: sorts in place).
fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    v[v.len() / 2]
}

/// Paged owned-row drain of one query (the row-materialization path).
async fn drain(cx: &Cx, conn: &mut Connection, sql: &str, arraysize: u32) -> usize {
    let first = conn
        .execute_raw(
            cx,
            sql,
            arraysize,
            &[],
            oracledb::protocol::thin::ExecuteOptions::default(),
            None,
        )
        .await
        .expect("execute");
    let cursor_id = first.cursor_id;
    let mut total = first.rows.len();
    let mut more = first.more_rows;
    let mut prev: Option<Vec<Option<QueryValue>>> = first.rows.last().cloned();
    while more && cursor_id != 0 {
        let batch = conn
            .fetch_rows(cx, cursor_id, arraysize, prev.as_deref())
            .await
            .expect("fetch page");
        total += batch.rows.len();
        more = batch.more_rows;
        if let Some(last) = batch.rows.last().cloned() {
            prev = Some(last);
        }
    }
    conn.release_cursor(cursor_id);
    total
}

fn main() {
    let Some(options) = connect_options() else {
        eprintln!("skipped perf_attribution_map: PYO_TEST_* not set");
        return;
    };
    let reactor = reactor::create_reactor().expect("reactor");
    let runtime = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("runtime");

    runtime.block_on(async {
        let cx = Cx::current().expect("cx");
        let mut conn = Connection::connect(&cx, options).await.expect("connect");

        println!("=== perf attribution map (warm connection, loopback) ===\n");

        // ---- (1) select 1 from dual: the single round-trip floor. -----------
        // Pure RT-bound. Client CPU here is the execute encode + the one-row
        // decode; everything else is the server round trip we cannot beat.
        {
            let warm = 200u32;
            let iters = 4000u32;
            for _ in 0..warm {
                let r = conn
                    .execute_raw(
                        &cx,
                        "select 1 from dual",
                        1,
                        &[],
                        oracledb::protocol::thin::ExecuteOptions::default(),
                        None,
                    )
                    .await
                    .expect("warm");
                conn.release_cursor(r.cursor_id);
            }
            let mut samples = Vec::with_capacity(iters as usize);
            for _ in 0..iters {
                let t0 = Instant::now();
                let r = conn
                    .execute_raw(
                        &cx,
                        "select 1 from dual",
                        1,
                        &[],
                        oracledb::protocol::thin::ExecuteOptions::default(),
                        None,
                    )
                    .await
                    .expect("select 1");
                let dt = t0.elapsed();
                conn.release_cursor(r.cursor_id);
                samples.push(dt.as_nanos() as f64 / 1e3); // us
            }
            let med = median(&mut samples);
            println!("(1) select_one_row  (one round trip)");
            println!("    median wall : {med:.2} us/call  (server RT-bound: the one round trip dominates)");
            println!("    attribution : ~all wall is the round trip; client CPU = execute-encode + 1-row decode (sub-us)\n");
        }

        // ---- (2) fetch 10k rows, arraysize 1000 (~10 pages) -----------------
        {
            let sql = "select level as n from dual connect by level <= 10000";
            let arraysize = 1000u32;
            let _ = drain(&cx, &mut conn, sql, arraysize).await; // warm
            oracledb::fetch_profile_arm(true);
            oracledb::fetch_profile_reset();
            let iters = 60u32;
            let t0 = Instant::now();
            for _ in 0..iters {
                let _ = drain(&cx, &mut conn, sql, arraysize).await;
            }
            let wall = t0.elapsed().as_nanos() as f64 / 1e6 / f64::from(iters); // ms/iter
            oracledb::fetch_profile_arm(false);
            let (read_ns, decode_ns) = oracledb::fetch_profile_read_decode_ns();
            let total = (read_ns + decode_ns) as f64;
            println!("(2) fetch_10k_rows  (single NUMBER column, ~10 pages)");
            println!("    wall/iter   : {wall:.3} ms");
            println!(
                "    read-wait   : {:.1}%   decode-CPU : {:.1}%   (read+decode instrumented)",
                100.0 * read_ns as f64 / total,
                100.0 * decode_ns as f64 / total
            );
            println!("    beatable    : the {:.1}% decode-CPU slice (+ row Vec alloc); read-wait is server/socket\n",
                100.0 * decode_ns as f64 / total);
        }

        // ---- (3) wide analytics fetch: 10 typed columns x many rows ---------
        // This is the columnar-Arrow target: decode-heavy, allocation-heavy on
        // the row path (one Vec<Option<QueryValue>> per row, String per text
        // cell, OracleNumber per number cell).
        {
            let sql = "select \
                       level as id, \
                       level * 1.5 as amount, \
                       rpad('row', 40, to_char(level)) as label, \
                       mod(level, 7) as bucket, \
                       sysdate + level as ts, \
                       level * level as sq, \
                       to_char(level) as code, \
                       cast(level as number(18,4)) as price, \
                       mod(level,2) as flag, \
                       'category-' || mod(level, 13) as cat \
                       from dual connect by level <= 20000";
            let arraysize = 1000u32;
            let _ = drain(&cx, &mut conn, sql, arraysize).await; // warm
            oracledb::fetch_profile_arm(true);
            oracledb::fetch_profile_reset();
            let iters = 30u32;
            let t0 = Instant::now();
            let mut rows = 0usize;
            for _ in 0..iters {
                rows = drain(&cx, &mut conn, sql, arraysize).await;
            }
            let wall = t0.elapsed().as_nanos() as f64 / 1e6 / f64::from(iters);
            oracledb::fetch_profile_arm(false);
            let (read_ns, decode_ns) = oracledb::fetch_profile_read_decode_ns();
            let total = (read_ns + decode_ns) as f64;
            println!("(3) fetch_wide_analytics  ({rows} rows x 10 typed cols: NUMBER/VARCHAR/DATE)");
            println!("    wall/iter   : {wall:.3} ms");
            println!(
                "    read-wait   : {:.1}%   decode-CPU : {:.1}%",
                100.0 * read_ns as f64 / total,
                100.0 * decode_ns as f64 / total
            );
            println!("    COLUMNAR TARGET: decode-CPU + per-row Vec<Option<QueryValue>> + transpose-on-build are\n                     the beatable client slice; columnar decode skips the row Vec entirely.\n");
        }

        // ---- (4) executemany 1000 rows: array-DML encode path ---------------
        {
            let _ = conn
                .execute_raw(
                    &cx,
                    "drop table PERFATTR_BENCH purge",
                    1,
                    &[],
                    oracledb::protocol::thin::ExecuteOptions::default(),
                    None,
                )
                .await;
            conn.execute_raw(
                &cx,
                "create table PERFATTR_BENCH (id number(9), label varchar2(40))",
                1,
                &[],
                oracledb::protocol::thin::ExecuteOptions::default(),
                None,
            )
            .await
            .expect("create");
            let rows: Vec<Vec<BindValue>> = (0..1000)
                .map(|i| {
                    vec![
                        BindValue::Number(i.to_string()),
                        BindValue::Text(format!("row-{i:05}")),
                    ]
                })
                .collect();
            let sql = "insert into PERFATTR_BENCH (id, label) values (:1, :2)";
            // warm
            for _ in 0..5 {
                let r = conn
                    .execute_raw(
                        &cx,
                        sql,
                        1,
                        &rows,
                        oracledb::protocol::thin::ExecuteOptions::default(),
                        None,
                    )
                    .await
                    .expect("warm insert");
                assert_eq!(r.row_count, 1000);
                conn.rollback(&cx).await.expect("rollback");
            }
            let iters = 100u32;
            let mut samples = Vec::with_capacity(iters as usize);
            for _ in 0..iters {
                let t0 = Instant::now();
                let r = conn
                    .execute_raw(
                        &cx,
                        sql,
                        1,
                        &rows,
                        oracledb::protocol::thin::ExecuteOptions::default(),
                        None,
                    )
                    .await
                    .expect("insert");
                let dt = t0.elapsed();
                conn.rollback(&cx).await.expect("rollback");
                assert_eq!(r.row_count, 1000);
                samples.push(dt.as_nanos() as f64 / 1e6); // ms
            }
            let med = median(&mut samples);
            println!("(4) executemany_1000  (array-DML INSERT, one round trip)");
            println!("    median wall : {med:.3} ms/call");
            println!("    attribution : one round trip carries all 1000 rows; client CPU = bind-encode of 1000 rows.");
            println!("                  Server executes the array DML; the RT + server work dominate.\n");
            let _ = conn
                .execute_raw(
                    &cx,
                    "drop table PERFATTR_BENCH purge",
                    1,
                    &[],
                    oracledb::protocol::thin::ExecuteOptions::default(),
                    None,
                )
                .await;
        }

        conn.close(&cx).await.expect("close");
        println!("=== end attribution map ===");
    });
}
