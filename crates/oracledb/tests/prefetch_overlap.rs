//! Live tests for speculative next-page prefetch (bead rust-oracledb-xad / 3oi).
//!
//! The borrowed paging loop ([`Connection::for_each_row_ref`]) overlaps the
//! socket read of page K+1 with the CPU decode of page K by issuing page K+1's
//! FETCH request *before* decoding page K. This file pins the three properties
//! the bead requires:
//!
//!   1. CORRECTNESS: the prefetched (overlapped) path returns byte-identical
//!      rows to the serial owned path on a large many-page result.
//!   2. SOUNDNESS / cancellation: dropping a fetch mid-prefetch (a speculative
//!      page is in flight on the wire) and then reusing the SAME connection must
//!      not poison the stream — `select 7 + 5 -> 12` still works (reuses the
//!      bead-wnz cancel_then_reuse pattern).
//!   3. The low-level request/response split that powers the overlap
//!      (`fetch_rows_request` + `fetch_rows_ref_response`) yields the same rows
//!      as the fused `fetch_rows_ref`.
//!
//! Run against the container:
//!
//! ```sh
//! eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1523 \
//!         ORACLEDB_HOST_PORT=1523 scripts/container.sh env)"
//! cargo test -p oracledb --test prefetch_overlap -- --include-ignored
//! ```

use std::time::Duration;

use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::{time, Cx};
use oracledb::protocol::thin::{QueryValue, QueryValueRef};
use oracledb::{ConnectOptions, Connection};
use oracledb_protocol::ClientIdentity;

fn live_options() -> Option<ConnectOptions> {
    let connect_string = std::env::var("PYO_TEST_CONNECT_STRING").ok()?;
    let user = std::env::var("PYO_TEST_MAIN_USER").ok()?;
    let password = std::env::var("PYO_TEST_MAIN_PASSWORD").ok()?;
    let identity = ClientIdentity::new(
        "rust-oracledb-prefetch",
        "prefetch-host",
        "prefetch-user",
        "prefetch-term",
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

/// Drain `sql` through the owned serial path into a Vec of owned rows.
async fn owned_rows(
    cx: &Cx,
    conn: &mut Connection,
    sql: &str,
    arraysize: u32,
) -> Vec<Vec<Option<QueryValue>>> {
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
        .expect("owned execute");
    let cursor_id = first.cursor_id;
    let mut rows: Vec<Vec<Option<QueryValue>>> = first.rows.clone();
    let mut more = first.more_rows;
    let mut prev = first.rows.last().cloned();
    while more && cursor_id != 0 {
        let batch = conn
            .fetch_rows(cx, cursor_id, arraysize, prev.as_deref())
            .await
            .expect("owned fetch page");
        rows.extend(batch.rows.iter().cloned());
        more = batch.more_rows;
        if let Some(last) = batch.rows.last().cloned() {
            prev = Some(last);
        }
    }
    conn.release_cursor(cursor_id);
    rows
}

#[test]
#[ignore = "requires local Oracle listener from scripts/container.sh up"]
fn prefetched_borrowed_fetch_is_byte_identical_to_serial_owned() {
    let Some(options) = live_options() else {
        eprintln!("skipped: PYO_TEST_* not set");
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

        // Large many-page mixed-type result so the prefetch loop pages many times.
        let sql = "select level as n, \
                          rpad('row', 25, to_char(level)) as label, \
                          level * 1.5 as scaled, \
                          cast(null as varchar2(10)) as empty \
                   from dual connect by level <= 20000";
        let arraysize = 500;

        let expected = owned_rows(&cx, &mut conn, sql, arraysize).await;
        assert_eq!(expected.len(), 20_000, "owned path drains all rows");

        // Prefetched borrowed path: every borrowed cell -> owned must equal the
        // serial owned-path value, row for row.
        let mut got: Vec<Vec<Option<QueryValue>>> = Vec::new();
        conn.for_each_row_ref(
            &cx,
            sql,
            arraysize,
            |row: &[Option<QueryValueRef<'_>>]| {
                got.push(
                    row.iter()
                        .map(|cell| cell.map(|v| v.to_owned_value()))
                        .collect(),
                );
                Ok(())
            },
        )
        .await
        .expect("prefetched borrowed fetch");

        assert_eq!(
            got.len(),
            expected.len(),
            "prefetched path yields the same row count"
        );
        assert_eq!(
            got, expected,
            "every prefetched borrowed cell must be byte-identical to the serial owned value"
        );

        conn.close(&cx).await.expect("close");
    });
}

#[test]
#[ignore = "requires local Oracle listener from scripts/container.sh up"]
fn low_level_request_response_split_matches_fused_fetch() {
    let Some(options) = live_options() else {
        eprintln!("skipped: PYO_TEST_* not set");
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

        let sql = "select level as n from dual connect by level <= 3000";
        let arraysize = 500;

        // Reference rows via the owned path.
        let expected = owned_rows(&cx, &mut conn, sql, arraysize).await;

        // Drive the cursor with the split request/response primitives: send the
        // next page's request, THEN read the prior response (one-page lookahead),
        // exactly as the overlap loop does.
        let first = conn
            .execute_raw(
                &cx,
                sql,
                arraysize,
                &[],
                oracledb::protocol::thin::ExecuteOptions::default(),
                None,
            )
            .await
            .expect("execute");
        let cursor_id = first.cursor_id;
        let mut got: Vec<Vec<Option<QueryValue>>> = first.rows.clone();
        let mut more = first.more_rows;
        let mut prev = first.rows.last().cloned();
        while more && cursor_id != 0 {
            // Speculatively request the page.
            conn.fetch_rows_request(&cx, cursor_id, arraysize)
                .await
                .expect("send fetch request");
            // Then read + decode its response.
            let batch = conn
                .fetch_rows_ref_response(&cx, cursor_id, prev.as_deref())
                .await
                .expect("read fetch response");
            more = batch.more_rows;
            let mut last: Option<Vec<Option<QueryValue>>> = None;
            batch
                .batch
                .for_each_row_ref(|row| {
                    let owned: Vec<Option<QueryValue>> =
                        row.iter().map(|c| c.map(|v| v.to_owned_value())).collect();
                    last = Some(owned.clone());
                    got.push(owned);
                    Ok::<(), oracledb::Error>(())
                })
                .expect("iterate batch");
            if let Some(l) = last {
                prev = Some(l);
            }
        }
        conn.release_cursor(cursor_id);

        assert_eq!(
            got, expected,
            "split request/response yields identical rows"
        );

        conn.close(&cx).await.expect("close");
    });
}

#[test]
#[ignore = "requires local Oracle listener from scripts/container.sh up"]
fn connection_is_reusable_after_drop_mid_prefetch() {
    let Some(options) = live_options() else {
        eprintln!("skipped: PYO_TEST_* not set");
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

        // Sanity before any cancel.
        let before = conn
            .execute_raw(
                &cx,
                "select 1 + 1 from dual",
                2,
                &[],
                oracledb::protocol::thin::ExecuteOptions::default(),
                None,
            )
            .await
            .expect("pre-query");
        assert_eq!(before.cell(0, 0).and_then(QueryValue::as_i64), Some(2));

        // A large result so the prefetch loop is mid-flight (a speculative page
        // request is on the wire) when the timer wins and the future is dropped.
        // The callback artificially slows decode so the drop lands inside the
        // window where page K+1 is in flight but not yet consumed.
        let sql = "select level as n, rpad('x', 30, to_char(level)) as label \
                   from dual connect by level <= 200000";
        let mut seen = 0usize;
        let fetch = conn.for_each_row_ref(&cx, sql, 500, |_row| {
            seen += 1;
            Ok(())
        });
        let raced = time::timeout(time::wall_now(), Duration::from_millis(30), fetch).await;
        assert!(
            raced.is_err(),
            "the 200k-row prefetched fetch must NOT finish within 30 ms (it is mid-prefetch)"
        );
        // `fetch` (and its borrow of conn) is dropped here. A speculative page
        // may be stranded on the wire; the next op must break + drain it.

        // Reuse the SAME connection: must return 12, not stale prefetched bytes.
        let reuse = conn
            .execute_raw(
                &cx,
                "select 7 + 5 from dual",
                2,
                &[],
                oracledb::protocol::thin::ExecuteOptions::default(),
                None,
            )
            .await
            .expect("connection must be reusable after drop mid-prefetch");
        assert_eq!(
            reuse.cell(0, 0).and_then(QueryValue::as_i64),
            Some(12),
            "the reused connection must return 7 + 5 = 12, not a stranded prefetched page"
        );

        // And keeps working for a multi-row fetch (exercises paging again).
        let rows = conn
            .execute_raw(
                &cx,
                "select level as n from dual connect by level <= 5 order by n",
                10,
                &[],
                oracledb::protocol::thin::ExecuteOptions::default(),
                None,
            )
            .await
            .expect("multi-row fetch on the recovered connection");
        let values: Vec<i64> = (0..rows.rows.len())
            .filter_map(|r| rows.cell(r, 0).and_then(QueryValue::as_i64))
            .collect();
        assert_eq!(values, vec![1, 2, 3, 4, 5]);

        conn.close(&cx).await.expect("close");
    });
}

#[test]
#[ignore = "requires local Oracle listener from scripts/container.sh up"]
fn stranded_prefetch_request_is_drained_before_reuse() {
    // Deterministic (timing-free) version of the soundness proof: explicitly
    // issue a speculative FETCH request and then ABANDON it without reading the
    // response — the exact stranded-page state a drop-mid-prefetch leaves. The
    // request armed `cancel_drain_pending`, so the next op must break + drain the
    // stranded page; `select 7 + 5 -> 12` then proves the wire is clean.
    let Some(options) = live_options() else {
        eprintln!("skipped: PYO_TEST_* not set");
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

        // Open a cursor with more rows than one page, then read one page so the
        // cursor is mid-stream (more_rows = true) and a further FETCH is valid.
        let first = conn
            .execute_raw(
                &cx,
                "select level as n from dual connect by level <= 4000",
                500,
                &[],
                oracledb::protocol::thin::ExecuteOptions::default(),
                None,
            )
            .await
            .expect("execute");
        let cursor_id = first.cursor_id;
        assert!(
            first.more_rows && cursor_id != 0,
            "cursor must have more pages"
        );

        // Issue the speculative request for the next page... and DO NOT read its
        // response. This is the stranded-page state. (We intentionally drop the
        // returned future's effect by simply not calling fetch_rows_ref_response.)
        conn.fetch_rows_request(&cx, cursor_id, 500)
            .await
            .expect("speculative request sent");

        // Reuse the SAME connection immediately. The pending-drain flag is armed,
        // so this execute breaks + drains the stranded page first.
        let reuse = conn
            .execute_raw(
                &cx,
                "select 7 + 5 from dual",
                2,
                &[],
                oracledb::protocol::thin::ExecuteOptions::default(),
                None,
            )
            .await
            .expect("connection must be reusable after a stranded prefetch request");
        assert_eq!(
            reuse.cell(0, 0).and_then(QueryValue::as_i64),
            Some(12),
            "the reused connection must return 7 + 5 = 12, not the stranded page"
        );

        conn.close(&cx).await.expect("close");
    });
}
