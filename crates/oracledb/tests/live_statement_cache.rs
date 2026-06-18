//! Live test for configurable statement-cache size (bead 5ah, oraclemcp #10).
//! Exercises the eviction path (tiny cache) and the disabled path (size 0)
//! end to end: many distinct statements must keep working as cursors are evicted
//! and closed via the piggyback queue, and a disabled cache must not retain or
//! reuse anything.
//!
//! Run: PYO_TEST_CONNECT_STRING=localhost:1522/FREEPDB1 PYO_TEST_MAIN_USER=pythontest \
//!      PYO_TEST_MAIN_PASSWORD=pythontest \
//!      cargo test -p oracledb --test live_statement_cache -- --ignored --nocapture
use oracledb::protocol::thin::QueryValue;
use oracledb::protocol::ClientIdentity;
use oracledb::{BlockingConnection, ConnectOptions};

fn options() -> ConnectOptions {
    let cs = std::env::var("PYO_TEST_CONNECT_STRING").unwrap();
    let user = std::env::var("PYO_TEST_MAIN_USER").unwrap();
    let pw = std::env::var("PYO_TEST_MAIN_PASSWORD").unwrap();
    let id = ClientIdentity::new("stmtcache", "host", "user", "term", "rust").unwrap();
    ConnectOptions::new(cs, user, pw, id)
}

fn run_n_distinct(conn: &mut oracledb::Connection, n: i64) {
    for i in 1..=n {
        // Each statement is distinct SQL text, so each takes a cache slot.
        let sql = format!("select {i} + level from dual connect by level <= 2");
        let r = BlockingConnection::execute_query(conn, &sql, 10).unwrap();
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
    let r = BlockingConnection::execute_query(
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
        let r = BlockingConnection::execute_query(&mut c, "select 42 from dual", 1).unwrap();
        assert_eq!(r.cell(0, 0).and_then(QueryValue::as_i64), Some(42));
    }
    run_n_distinct(&mut c, 10);
    BlockingConnection::close(c).ok();
}
