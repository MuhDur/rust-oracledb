// Assertion-heavy test code intentionally panics on invariant violations.
#![allow(clippy::unwrap_used)]

//! Live test for timeout-aware named-bind queries (bead b85, oraclemcp #11):
//! `Query::timeout` gives named binds the same per-call timeout parity
//! the positional path already had. Verifies (a) the success path with binds
//! reordered to placeholder first-appearance order, and (b) that the timeout
//! actually applies and surfaces as a typed `CallTimeout`.
//!
//! Run: PYO_TEST_CONNECT_STRING=localhost:1522/FREEPDB1 PYO_TEST_MAIN_USER=pythontest \
//!      PYO_TEST_MAIN_PASSWORD=pythontest \
//!      cargo test -p oracledb --test live_named_bind_timeout -- --ignored --nocapture
use std::time::Duration;

use oracledb::protocol::thin::QueryValue;
use oracledb::protocol::ClientIdentity;
use oracledb::{params, BlockingConnection, ConnectOptions, Error, Execute, Query};

mod common;

fn connect() -> oracledb::Connection {
    let common::LiveCreds {
        connect_string: cs,
        user,
        password: pw,
    } = common::live_creds_required();
    let id = ClientIdentity::new("namedtimeout", "host", "user", "term", "rust").unwrap();
    BlockingConnection::connect(ConnectOptions::new(cs, user, pw, id)).unwrap()
}

#[test]
#[ignore]
fn named_bind_with_generous_timeout_succeeds() {
    let mut c = connect();
    // `:b` appears before `:a` in the SQL, but the params are given a-then-b:
    // the driver must reorder by first-appearance, so (100 - 1) = 99.
    let r = BlockingConnection::query_with(
        &mut c,
        Query::new("select :b - :a from dual")
            .bind(params! { ":a" => 1, ":b" => 100 })
            .timeout(Duration::from_millis(10_000)),
    )
    .expect("named bind query with generous timeout")
    .one()
    .expect("one row");
    assert_eq!(r.value(0).and_then(QueryValue::as_i64), Some(99));
    BlockingConnection::close(c).ok();
}

#[test]
#[ignore]
fn named_bind_timeout_fires_as_typed_error() {
    let mut c = connect();
    // A 2 s server-side sleep driven by a named bind, capped at 500 ms: the
    // timeout must fire and surface as a typed CallTimeout carrying the bound.
    let timed_out = BlockingConnection::execute_with(
        &mut c,
        Execute::new("begin dbms_session.sleep(:secs); end;")
            .bind(params! { ":secs" => 2 })
            .timeout(Duration::from_millis(500)),
    );
    match timed_out {
        Err(Error::CallTimeout(ms)) => assert_eq!(ms, 500, "reports the timeout we set"),
        Err(other) => panic!("expected CallTimeout, got: {other:?}"),
        Ok(_) => panic!("the 2s sleep should have timed out at 500ms"),
    }
    // The connection survives a plain call timeout — a follow-up query works.
    let after = BlockingConnection::query(&mut c, "select :x from dual", params! { ":x" => 7 })
        .expect("follow-up named bind query")
        .one()
        .expect("one row");
    assert_eq!(after.value(0).and_then(QueryValue::as_i64), Some(7));
    BlockingConnection::close(c).ok();
}
