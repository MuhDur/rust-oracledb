//! Regression: a connection must remain USABLE after a `call_timeout` fires
//! (bead rust-oracledb-2vx).
//!
//! When an operation exceeds the connection's `call_timeout`, the driver sends
//! a BREAK marker to interrupt the server. Before the fix it then returned
//! immediately WITHOUT draining the server's response, so the in-flight reply,
//! the RESET handshake, and the trailing error packet were left unread in the
//! socket. The next operation on the (reused) connection misread those stale
//! bytes -> wrong rows / TtcDecode error / protocol desync.
//!
//! python-oracledb deliberately keeps the connection alive after a call timeout
//! (`DPY-4024` is NOT `is_session_dead`, errors.py:124-125): on the timeout it
//! breaks, then drains the response + RESET marker + error packet via
//! `_break_external()` -> `_receive_packet()` (-> `_reset()`), leaving the wire
//! clean (protocol.pyx:449-451, 507-557). The fix mirrors that: every timeout
//! path now calls `break_and_drain` before returning, so the connection can be
//! reused.
//!
//! This test forces a real call timeout (a 2 s `dbms_session.sleep` under a
//! 500 ms timeout), confirms it surfaces a timeout error, and then runs an
//! ordinary `select 7 + 5 from dual` on the SAME connection and asserts it
//! returns 12. Against the pre-fix code the follow-up query misframes/errors;
//! after the fix it succeeds.
//!
//! Self-skips when the container environment is absent, like the rest of the
//! integration suite. Run against the container with:
//!
//! ```sh
//! eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1526 \
//!         ORACLEDB_HOST_PORT=1526 scripts/container.sh env)"
//! cargo test -p oracledb --test reuse_after_call_timeout
//! ```

use oracledb::protocol::thin::QueryValue;
use oracledb::{BlockingConnection, ConnectOptions, Connection, Error};
use oracledb_protocol::ClientIdentity;

const PROGRAM: &str = "rust-oracledb-reuse-to";
const MACHINE: &str = "reuse-to-machine";
const OSUSER: &str = "reuse-to-osuser";
const TERMINAL: &str = "reuse-to-terminal";
const DRIVER: &str = "rust-oracledb thn : 0.0.0";

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

#[test]
fn connection_is_reusable_after_call_timeout() {
    let Some(options) = connect_options() else {
        eprintln!("skipped connection_is_reusable_after_call_timeout: PYO_TEST_* not set");
        return;
    };
    let mut conn = BlockingConnection::connect(options).expect("connect to test container");

    // Sanity: an ordinary query works BEFORE the timeout, so any post-timeout
    // failure is attributable to the timeout recovery, not a broken connection.
    let before = BlockingConnection::execute_query(&mut conn, "select 1 + 1 from dual", 2)
        .expect("pre-query");
    assert_eq!(
        before.cell(0, 0).and_then(QueryValue::as_i64),
        Some(2),
        "the connection works before the timeout"
    );

    // Force a call timeout: a 2 s server-side sleep capped at 500 ms. The server
    // is still sleeping when the timeout fires, so its response is genuinely
    // in-flight -- exactly the condition that poisoned the wire before the fix.
    let slow = "begin dbms_session.sleep(2); end;";
    let timed_out = BlockingConnection::execute_query_with_timeout(&mut conn, slow, 1, Some(500));
    match timed_out {
        Err(Error::CallTimeout(ms)) => {
            assert_eq!(ms, 500, "the reported timeout is the one we set");
        }
        Err(other) => panic!("expected a CallTimeout, got: {other:?}"),
        Ok(_) => panic!("the slow statement should have timed out, not completed"),
    }

    // A plain call timeout must leave the connection USABLE (not connection-lost):
    // mirrors python-oracledb DPY-4024 keeping the session alive.
    assert!(
        !Error::CallTimeout(500).is_connection_lost(),
        "a call timeout is not connection-lost"
    );

    // THE REGRESSION: run an ordinary query on the SAME connection. Before the
    // fix this misframed onto the stale in-flight bytes (wrong value / decode
    // error / desync). After the fix the wire is clean and 7 + 5 == 12.
    let reuse = BlockingConnection::execute_query(&mut conn, "select 7 + 5 from dual", 2)
        .expect("the connection must be reusable after a call timeout");
    assert_eq!(
        reuse.cell(0, 0).and_then(QueryValue::as_i64),
        Some(12),
        "the reused connection must return the correct result (7 + 5 = 12), \
         not stale bytes from the timed-out call"
    );

    // And it keeps working for more than one follow-up round trip.
    let again = BlockingConnection::execute_query(&mut conn, "select 'reused' from dual", 2)
        .expect("second reuse");
    assert_eq!(
        again
            .cell(0, 0)
            .and_then(|v| v.as_text())
            .map(str::to_owned),
        Some("reused".to_string()),
        "a second reuse round trip also succeeds"
    );

    drop_table_select_count_roundtrip(&mut conn);

    BlockingConnection::close(conn).expect("close connection");
}

/// One more multi-row round trip to exercise the fetch path (not just scalar
/// `dual` selects) on the recovered connection.
fn drop_table_select_count_roundtrip(conn: &mut Connection) {
    let rows = BlockingConnection::execute_query_collect(
        conn,
        "select level as n from dual connect by level <= 5 order by n",
        10,
    )
    .expect("multi-row fetch on the recovered connection");
    let values: Vec<i64> = (0..rows.rows.len())
        .filter_map(|r| rows.cell(r, 0).and_then(QueryValue::as_i64))
        .collect();
    assert_eq!(
        values,
        vec![1, 2, 3, 4, 5],
        "the recovered connection fetches a multi-row result correctly"
    );
}
