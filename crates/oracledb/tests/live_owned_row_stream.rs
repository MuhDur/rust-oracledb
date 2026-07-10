//! Live parity + lifecycle tests for the owning row-by-row stream
//! ([`Connection::into_row_stream`] / [`Connection::into_query_stream`],
//! yielding [`oracledb::OwnedRowStream`]).
//!
//! The owning stream must be byte-identical to the eager [`Connection::query_all`]
//! path across single-page, multi-page, empty, and mid-stream-error results, and
//! it must hand the connection back cleanly after a full drain or an early stop.
//!
//! Gated behind `#[ignore]` like the other live suites: run with the container
//! environment sourced (`scripts/container.sh env`), e.g. against free23
//! (`localhost:1522/FREEPDB1`, pythontest) or xe21 (`localhost:1520/XEPDB1`,
//! testuser). Without a listener the suite self-skips (it is `#[ignore]`d).

use std::future::poll_fn;
use std::num::NonZeroU32;
use std::pin::Pin;

use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::Cx;
use futures_core::Stream;
use oracledb::protocol::thin::QueryValue;
use oracledb::{ConnectOptions, Connection, OwnedRowStream, Query};
use oracledb_protocol::ClientIdentity;

mod common;

fn live_options() -> ConnectOptions {
    let identity = ClientIdentity::new(
        "rust-oracledb",
        "rusthost",
        "rustuser",
        "rustterm",
        "rust-oracledb thn : 0.0.0",
    )
    .expect("test identity should be valid");
    ConnectOptions::new(
        common::live_conn_string_or(common::FREE23_CONNECT_STRING),
        common::live_user_or(common::FREE23_USER),
        std::env::var("PYO_TEST_MAIN_PASSWORD")
            .expect("PYO_TEST_MAIN_PASSWORD must be set for ignored live test"),
        identity,
    )
}

fn block_on_live<F, T>(body: F) -> T
where
    F: std::future::Future<Output = T>,
{
    let reactor = reactor::create_reactor().expect("native reactor should build for live I/O");
    let runtime = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("current-thread Asupersync runtime should build");
    runtime.block_on(body)
}

/// Pull every remaining row of the stream via a real waker (`poll_fn`), stopping
/// at the first error or at end-of-stream. `OwnedRowStream` is `Unpin`, so
/// `Pin::new(&mut _)` needs no `unsafe`.
async fn collect_stream(
    stream: &mut OwnedRowStream,
) -> oracledb::Result<Vec<Vec<Option<QueryValue>>>> {
    let mut rows = Vec::new();
    while let Some(item) = poll_fn(|task_cx| Pin::new(&mut *stream).poll_next(task_cx)).await {
        rows.push(item?);
    }
    Ok(rows)
}

#[test]
#[ignore = "requires local Oracle listener from scripts/container.sh up"]
fn streamed_rows_match_eager_query_all() {
    block_on_live(async {
        let cx = Cx::current().expect("block_on installs an ambient Cx");
        let mut conn = Connection::connect(&cx, live_options())
            .await
            .expect("connect");

        // A wide, mixed-type, many-row result paged across many small fetches.
        let sql = "select level as n, \
                          rpad('row', 20, to_char(level)) as label, \
                          level * 1.5 as scaled, \
                          cast(null as varchar2(10)) as empty \
                   from dual connect by level <= 2500";

        // Eager reference via query_all.
        let eager: Vec<Vec<Option<QueryValue>>> = conn
            .query_all(&cx, sql, ())
            .await
            .expect("eager query_all")
            .into_iter()
            .map(oracledb::Row::into_values)
            .collect();

        // Owning stream over the same SQL (default arraysize 100 pages ~25 times).
        let mut stream = conn
            .into_query_stream(&cx, sql, ())
            .await
            .expect("into_query_stream");
        let streamed = collect_stream(&mut stream).await.expect("stream drains");
        let conn = stream.into_connection().expect("recover connection");

        assert_eq!(eager.len(), 2500, "eager path drains all rows");
        assert_eq!(
            streamed.len(),
            eager.len(),
            "stream yields the same row count"
        );
        assert_eq!(
            streamed, eager,
            "every streamed row must equal the eager query_all value byte-for-byte"
        );

        conn.close(&cx).await.expect("close");
    });
}

#[test]
#[ignore = "requires local Oracle listener from scripts/container.sh up"]
fn small_arraysize_multipage_matches_eager() {
    block_on_live(async {
        let cx = Cx::current().expect("block_on installs an ambient Cx");
        let mut conn = Connection::connect(&cx, live_options())
            .await
            .expect("connect");

        let sql = "select level as n from dual connect by level <= 1000";

        let eager: Vec<Vec<Option<QueryValue>>> = conn
            .query_all(&cx, sql, ())
            .await
            .expect("eager")
            .into_iter()
            .map(oracledb::Row::into_values)
            .collect();

        // arraysize = 3 forces hundreds of continuation fetches through the
        // move-out/move-back connection path.
        let query = Query::new(sql).arraysize(NonZeroU32::new(3).expect("nonzero"));
        let mut stream = conn.into_row_stream(&cx, query).await.expect("stream");
        let streamed = collect_stream(&mut stream).await.expect("drain");
        let conn = stream.into_connection().expect("recover");

        assert_eq!(streamed.len(), 1000);
        assert_eq!(streamed, eager, "multi-page stream equals eager");

        conn.close(&cx).await.expect("close");
    });
}

#[test]
#[ignore = "requires local Oracle listener from scripts/container.sh up"]
fn empty_result_stream_yields_nothing_and_recovers() {
    block_on_live(async {
        let cx = Cx::current().expect("block_on installs an ambient Cx");
        let conn = Connection::connect(&cx, live_options())
            .await
            .expect("connect");

        let mut stream = conn
            .into_query_stream(&cx, "select 1 as n from dual where 1 = 0", ())
            .await
            .expect("stream");
        let rows = collect_stream(&mut stream).await.expect("drain");
        assert!(rows.is_empty(), "empty result yields no rows");

        // The connection is recoverable and reusable after an empty stream.
        let mut conn = stream.into_connection().expect("recover");
        let n: i64 = conn
            .query_one(&cx, "select 42 from dual", ())
            .await
            .expect("reuse")
            .get(0)
            .expect("value");
        assert_eq!(n, 42);
        conn.close(&cx).await.expect("close");
    });
}

#[test]
#[ignore = "requires local Oracle listener from scripts/container.sh up"]
fn midstream_error_propagates_then_stream_terminates() {
    block_on_live(async {
        let cx = Cx::current().expect("block_on installs an ambient Cx");
        let conn = Connection::connect(&cx, live_options())
            .await
            .expect("connect");

        // Row 700 divides by zero -> ORA-01476 raised DURING a continuation
        // fetch (arraysize 50 puts it well past the first page). Rows before it
        // stream fine, then the error surfaces and the stream terminates.
        let sql = "select level as n, \
                          case when level = 700 then 1/0 else level end as v \
                   from dual connect by level <= 2000";
        let query = Query::new(sql).arraysize(NonZeroU32::new(50).expect("nonzero"));
        let mut stream = conn.into_row_stream(&cx, query).await.expect("stream");

        let mut yielded = 0usize;
        let mut saw_error = false;
        loop {
            match poll_fn(|task_cx| Pin::new(&mut stream).poll_next(task_cx)).await {
                Some(Ok(_)) => yielded += 1,
                Some(Err(err)) => {
                    saw_error = true;
                    assert_eq!(
                        err.ora_code(),
                        Some(1476),
                        "mid-stream failure should surface ORA-01476, got {err:?}"
                    );
                    assert!(
                        !err.is_connection_lost(),
                        "a divide-by-zero is a clean server error, not a lost connection"
                    );
                    break;
                }
                None => break,
            }
        }
        assert!(saw_error, "the divide-by-zero row must surface an error");
        assert!(yielded > 0, "rows before the failing row still streamed");

        // After the error the stream is terminal: it yields None, not more errors.
        assert!(
            poll_fn(|task_cx| Pin::new(&mut stream).poll_next(task_cx))
                .await
                .is_none(),
            "stream is terminal after a mid-stream error"
        );

        // The connection came back with the error; it is recoverable and the
        // failed cursor's stale cache/registry state cannot poison the next SQL.
        let mut conn = stream.into_connection().expect("recover after error");
        let answer: i64 = conn
            .query_one(&cx, "select 42 from dual", ())
            .await
            .expect("connection remains reusable after a continuation error")
            .get(0)
            .expect("fresh query returns its value");
        assert_eq!(answer, 42);
        conn.close(&cx).await.expect("close");
    });
}

#[test]
#[ignore = "requires local Oracle listener from scripts/container.sh up"]
fn materialized_lob_define_failure_leaves_connection_reusable() {
    block_on_live(async {
        let cx = Cx::current().expect("block_on installs an ambient Cx");
        let mut conn = Connection::connect(&cx, live_options())
            .await
            .expect("connect");

        // A CLOB projection is describe-only on the initial execute and is
        // materialized by query_with's follow-up DEFINE/FETCH. The expression
        // raises while Oracle produces that row, exercising the bootstrap's
        // error lifecycle rather than a successfully returned Rows facade.
        let sql = "select to_clob(case when :1 = 1 then to_char(1/0) else '42' end) \
                   as payload from dual";
        let err = conn
            .query(&cx, sql, (1_i64,))
            .await
            .expect_err("materialized CLOB expression must raise ORA-01476");
        assert_eq!(
            err.ora_code(),
            Some(1476),
            "DEFINE/FETCH failure must surface the server error, got {err:?}"
        );

        // Re-execute the identical cached SQL with a non-failing bind. A stale
        // in-use/cached cursor from the failed DEFINE/FETCH must not poison it.
        let rows = conn
            .query_all(&cx, sql, (0_i64,))
            .await
            .expect("same SQL reparses cleanly after failed DEFINE/FETCH");
        assert_eq!(rows.len(), 1);
        conn.close(&cx).await.expect("close");
    });
}

#[test]
#[ignore = "requires local Oracle listener from scripts/container.sh up"]
fn early_stop_recovers_connection_for_reuse() {
    block_on_live(async {
        let cx = Cx::current().expect("block_on installs an ambient Cx");
        let conn = Connection::connect(&cx, live_options())
            .await
            .expect("connect");

        // arraysize 5 over 1000 rows: after pulling 12 rows the server cursor is
        // still open with more rows. Stopping early and recovering must leave the
        // connection clean and reusable.
        let query = Query::new("select level as n from dual connect by level <= 1000")
            .arraysize(NonZeroU32::new(5).expect("nonzero"));
        let mut stream = conn.into_row_stream(&cx, query).await.expect("stream");

        let mut pulled = 0;
        while pulled < 12 {
            match poll_fn(|task_cx| Pin::new(&mut stream).poll_next(task_cx)).await {
                Some(Ok(_)) => pulled += 1,
                other => panic!("unexpected early item: {other:?}"),
            }
        }
        assert_eq!(pulled, 12, "pulled a partial prefix");
        assert!(stream.cursor().is_none(), "no ref cursor for a plain query");

        // Recover the connection mid-stream (an open cursor is abandoned).
        let mut conn = stream.into_connection().expect("recover mid-stream");

        // The recovered connection decodes a fresh query correctly.
        let rows: Vec<Vec<Option<QueryValue>>> = conn
            .query_all(&cx, "select level from dual connect by level <= 4", ())
            .await
            .expect("reuse query")
            .into_iter()
            .map(oracledb::Row::into_values)
            .collect();
        assert_eq!(rows.len(), 4, "recovered connection runs a fresh query");

        conn.close(&cx).await.expect("close");
    });
}

#[test]
#[ignore = "requires local Oracle listener from scripts/container.sh up"]
fn drop_midstream_does_not_hang() {
    block_on_live(async {
        let cx = Cx::current().expect("block_on installs an ambient Cx");
        let conn = Connection::connect(&cx, live_options())
            .await
            .expect("connect");

        let query = Query::new("select level as n from dual connect by level <= 1000")
            .arraysize(NonZeroU32::new(5).expect("nonzero"));
        let mut stream = conn.into_row_stream(&cx, query).await.expect("stream");

        // Pull a couple of rows, then drop the whole stream while the server
        // cursor is still open. Drop releases the cursor and tears down the owned
        // connection without a hang or panic.
        for _ in 0..2 {
            let _ = poll_fn(|task_cx| Pin::new(&mut stream).poll_next(task_cx)).await;
        }
        drop(stream);
        // Reaching here means Drop completed cleanly.
    });
}
