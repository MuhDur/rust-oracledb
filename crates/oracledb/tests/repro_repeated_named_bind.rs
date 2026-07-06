//! Regression test for bead `qp0`: a named placeholder that occurs more than
//! once in plain SQL, bound ONCE by name via `query`, must bind correctly.
//!
//! Investigation outcome: NOT a bug. python-oracledb thin (and our pyshim) bind
//! per-occurrence for SQL, but Oracle binds repeated placeholders BY NAME, so our
//! once-per-distinct-name `order_named_binds` is correct — and it also sidesteps
//! python-oracledb #422 (per-occurrence binds confusing the optimizer). Verified
//! live: `select :v + :v` with v=5 returns 10 (not ORA-01008). This test guards
//! that behavior against regressions.
//!
//! Run: PYO_TEST_CONNECT_STRING=localhost:1522/FREEPDB1 PYO_TEST_MAIN_USER=pythontest \
//!      PYO_TEST_MAIN_PASSWORD=pythontest \
//!      cargo test -p oracledb --test repro_repeated_named_bind -- --ignored --nocapture
use oracledb::protocol::thin::QueryValue;
use oracledb::protocol::ClientIdentity;
use oracledb::{params, BlockingConnection, ConnectOptions};

mod common;

fn connect() -> oracledb::Connection {
    let common::LiveCreds {
        connect_string: cs,
        user,
        password: pw,
    } = common::live_creds_required();
    let id = ClientIdentity::new("repro", "host", "user", "term", "rust").unwrap();
    BlockingConnection::connect(ConnectOptions::new(cs, user, pw, id)).unwrap()
}

#[test]
#[ignore]
fn repeated_named_bind_in_sql() {
    let mut c = connect();

    // :v occurs TWICE; bound ONCE by name via the native query API.
    let res = BlockingConnection::query_one(
        &mut c,
        "select :v + :v as s from dual",
        params! { ":v" => 5_i64 },
    )
    .expect("repeated named bind must not under-bind (ORA-01008)");
    assert_eq!(
        res.value(0).and_then(QueryValue::as_i64),
        Some(10),
        "Oracle binds the repeated :v by name; both occurrences see 5"
    );

    // Single-occurrence control.
    let res1 = BlockingConnection::query_one(
        &mut c,
        "select :v as s from dual",
        params! { ":v" => 7_i64 },
    )
    .expect("single named bind");
    assert_eq!(res1.value(0).and_then(QueryValue::as_i64), Some(7));

    // Positional control where the user supplies both occurrences.
    let row2 = BlockingConnection::query_one(
        &mut c,
        "select :1 + :2 as s from dual",
        params![5_i64, 5_i64],
    )
    .expect("positional binds");
    assert_eq!(row2.value(0).and_then(QueryValue::as_i64), Some(10));

    BlockingConnection::close(c).ok();
}
