//! Live test for configurable statement-cache size (bead 5ah, oraclemcp #10).
//! Exercises the eviction path (tiny cache) and the disabled path (size 0)
//! end to end: many distinct statements must keep working as cursors are evicted
//! and closed via the piggyback queue, and a disabled cache must not retain or
//! reuse anything.
//!
//! Run: PYO_TEST_CONNECT_STRING=localhost:1522/FREEPDB1 PYO_TEST_MAIN_USER=pythontest \
//!      PYO_TEST_MAIN_PASSWORD=pythontest \
//!      cargo test -p oracledb --test live_statement_cache -- --ignored --nocapture
use oracledb::protocol::thin::{QueryResult, QueryValue};
use oracledb::protocol::ClientIdentity;
use oracledb::{BlockingConnection, ConnectOptions};

mod common;

fn options() -> ConnectOptions {
    let common::LiveCreds {
        connect_string: cs,
        user,
        password: pw,
    } = common::live_creds_required();
    let id = ClientIdentity::new("stmtcache", "host", "user", "term", "rust").unwrap();
    ConnectOptions::new(cs, user, pw, id)
}

fn execute_raw(
    conn: &mut oracledb::Connection,
    sql: &str,
    prefetch_rows: u32,
) -> oracledb::Result<QueryResult> {
    BlockingConnection::execute_raw(
        conn,
        sql,
        prefetch_rows,
        &[],
        oracledb::protocol::thin::ExecuteOptions::default(),
        None,
    )
}

fn run_n_distinct(conn: &mut oracledb::Connection, n: i64) {
    for i in 1..=n {
        // Each statement is distinct SQL text, so each takes a cache slot.
        let sql = format!("select {i} + level from dual connect by level <= 2");
        let r = execute_raw(conn, &sql, 10).unwrap();
        let first = r.cell(0, 0).and_then(QueryValue::as_i64).unwrap();
        assert_eq!(first, i + 1, "row 0 of stmt {i}");
        assert_eq!(r.rows.len(), 2);
    }
}

#[test]
#[ignore]
fn tiny_cache_evicts_and_keeps_working() {
    // Cache of 3, but 30 distinct statements -> heavy eviction + cursor close
    // piggybacking. Every statement must still execute correctly.
    let mut c = BlockingConnection::connect(options().with_statement_cache_size(3)).unwrap();
    run_n_distinct(&mut c, 30);
    // Re-run an earlier statement (long evicted) — must re-prepare cleanly.
    let r = execute_raw(
        &mut c,
        "select 1 + level from dual connect by level <= 2",
        10,
    )
    .unwrap();
    assert_eq!(r.cell(0, 0).and_then(QueryValue::as_i64), Some(2));
    BlockingConnection::close(c).ok();
}

#[test]
#[ignore]
fn disabled_cache_size_zero_keeps_working() {
    // Caching disabled: each statement's cursor is closed after use, never
    // retained. Repeated execution of the same SQL must still work.
    let mut c = BlockingConnection::connect(options().with_statement_cache_size(0)).unwrap();
    for _ in 0..10 {
        let r = execute_raw(&mut c, "select 42 from dual", 1).unwrap();
        assert_eq!(r.cell(0, 0).and_then(QueryValue::as_i64), Some(42));
    }
    run_n_distinct(&mut c, 10);
    BlockingConnection::close(c).ok();
}

/// With caching disabled (size 0) the executed cursor is queued for close, yet a
/// query whose result spans multiple fetch batches must still drain correctly:
/// the close-cursors piggyback only rides the next *execute*, never a `fetch`, so
/// the still-open cursor survives until the caller is done with it. (Guards the
/// size-0 disable path against closing an in-use cursor mid-fetch.)
#[test]
#[ignore]
fn disabled_cache_multibatch_fetch_survives() {
    let mut c = BlockingConnection::connect(options().with_statement_cache_size(0)).unwrap();
    // prefetch 5 over 100 rows -> first batch leaves more_rows set, cursor open.
    let first = execute_raw(&mut c, "select level from dual connect by level <= 100", 5).unwrap();
    assert!(
        first.more_rows,
        "expected an unfinished cursor for the test"
    );
    assert_ne!(first.cursor_id, 0);
    let prev = first.rows.last().cloned();
    let rest =
        BlockingConnection::fetch_rows(&mut c, first.cursor_id, 100, prev.as_deref()).unwrap();
    assert_eq!(
        first.rows.len() + rest.rows.len(),
        100,
        "all rows fetched despite size-0 caching"
    );
    BlockingConnection::close(c).ok();
}

/// Regression for bead rust-oracledb-ilel: re-executing a CACHED statement with
/// a bind of a DIFFERENT type must re-describe the statement instead of letting
/// the server coerce through the stale cached bind metadata (ORA-01722 when a
/// text bind hits a cursor parsed with a NUMBER bind). The reference driver
/// tracks the bound metadata per statement and falls back to a full re-parse
/// when the shape changes (thin/statement.pyx `_set_var` -> `_binds_changed`).
/// One SQL text, NUMBER -> TEXT -> RAW -> NUMBER, all on the same connection.
#[test]
#[ignore]
fn rebind_type_change_on_cached_statement_redescribes() {
    let mut c = BlockingConnection::connect(options()).unwrap();
    let sql = "select :1 from dual";
    // 1) NUMBER: parses the statement and caches its cursor with NUMBER
    //    bind metadata.
    let n: i64 = BlockingConnection::query_one(&mut c, sql, (42i64,))
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(n, 42);
    // 2) TEXT on the SAME cached SQL: without re-describe the server converts
    //    the text through the stale NUMBER bind metadata -> ORA-01722.
    let unicode = "naïve-Ω-δοκιμή-支持-🎯";
    let t: String = BlockingConnection::query_one(&mut c, sql, (unicode,))
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(t, unicode);
    // 3) RAW: a third distinct bind type on the same cached statement.
    let raw_in: Vec<u8> = vec![0xde, 0xad, 0xbe, 0xef, 0x00, 0x7f];
    let raw_out: Vec<u8> = BlockingConnection::query_one(&mut c, sql, (raw_in.clone(),))
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(raw_out, raw_in);
    // 4) Back to NUMBER: the cache entry must still be usable after the
    //    type churn.
    let n2: i64 = BlockingConnection::query_one(&mut c, sql, (7i64,))
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(n2, 7);
    BlockingConnection::close(c).ok();
}
