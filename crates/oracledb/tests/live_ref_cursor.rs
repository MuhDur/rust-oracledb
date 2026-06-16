//! Live test for ergonomic REF CURSOR / implicit result-set consumption
//! (bead za5). `Connection::fetch_cursor` drains a returned, self-describing
//! cursor (with column metadata), bounded by max_rows, releasing it at the end.
//!
//! Run: PYO_TEST_CONNECT_STRING=localhost:1522/FREEPDB1 PYO_TEST_MAIN_USER=pythontest \
//!      PYO_TEST_MAIN_PASSWORD=pythontest \
//!      cargo test -p oracledb --test live_ref_cursor -- --ignored --nocapture
use oracledb::protocol::thin::QueryValue;
use oracledb::protocol::ClientIdentity;
use oracledb::{BlockingConnection, ConnectOptions};

fn connect() -> oracledb::Connection {
    let cs = std::env::var("PYO_TEST_CONNECT_STRING").unwrap();
    let user = std::env::var("PYO_TEST_MAIN_USER").unwrap();
    let pw = std::env::var("PYO_TEST_MAIN_PASSWORD").unwrap();
    let id = ClientIdentity::new("refcursor", "host", "user", "term", "rust").unwrap();
    BlockingConnection::connect(ConnectOptions::new(cs, user, pw, id)).unwrap()
}

const RETURN_N: &str = "declare rc sys_refcursor; begin \
     open rc for select level as n from dual connect by level <= :lim; \
     dbms_sql.return_result(rc); end;";

#[test]
#[ignore]
fn implicit_result_set_full_fetch() {
    let mut c = connect();
    let res = BlockingConnection::execute_query(&mut c, &RETURN_N.replace(":lim", "5"), 0).unwrap();

    let cursors = res
        .implicit_resultsets
        .as_ref()
        .expect("the RETURN_RESULT block must surface an implicit result set");
    assert_eq!(cursors.len(), 1);

    let cv = match &cursors[0] {
        QueryValue::Cursor(cv) => cv.as_ref(),
        other => panic!("expected a cursor, got {other:?}"),
    };
    // The returned cursor is self-describing.
    assert_eq!(
        cv.columns.len(),
        1,
        "child cursor exposes its column metadata"
    );

    let out = BlockingConnection::fetch_cursor(&mut c, cv, 1000).unwrap();
    assert_eq!(out.columns.len(), 1);
    let vals: Vec<i64> = out
        .rows
        .iter()
        .map(|r| r[0].as_ref().and_then(QueryValue::as_i64).unwrap())
        .collect();
    assert_eq!(vals, vec![1, 2, 3, 4, 5]);

    BlockingConnection::close(c).ok();
}

#[test]
#[ignore]
fn cursor_fetch_is_bounded() {
    let mut c = connect();
    let res =
        BlockingConnection::execute_query(&mut c, &RETURN_N.replace(":lim", "100"), 0).unwrap();
    let cv = match &res.implicit_resultsets.as_ref().unwrap()[0] {
        QueryValue::Cursor(cv) => cv.as_ref(),
        other => panic!("expected cursor, got {other:?}"),
    };
    let out = BlockingConnection::fetch_cursor(&mut c, cv, 10).unwrap();
    assert_eq!(out.rows.len(), 10, "fetch must be bounded to max_rows");
    BlockingConnection::close(c).ok();
}
