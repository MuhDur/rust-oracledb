//! Live: an explicit `Connection::cancel()` (and the Scope-based cancel-on-drop)
//! must TRULY cancel an in-flight operation AND leave the connection in a clean,
//! reusable state (bead rust-oracledb-wnz).
//!
//! This builds on the call-timeout break+drain machinery (bead rust-oracledb-2vx
//! / 3mr): on a cancel the driver sends a BREAK and then drains the server's
//! entire cancel response — any in-flight DATA response of the cancelled call,
//! the break-ack MARKER, the RESET handshake, and the trailing `ORA-01013`
//! "user requested cancel" — leaving the wire at a clean boundary, exactly as
//! python-oracledb's `Connection.cancel()` does (`_break_external()` +
//! `_reset()`, protocol.pyx:533-557). A cancel keeps the session ALIVE (mirrors
//! `DPY-4024`: NOT connection-lost), so the SAME connection is reusable.
//!
//! Two scenarios, both ending in `select 7 + 5 -> 12` on the SAME connection:
//!
//!   1. EXPLICIT cancel: a slow `dbms_session.sleep` is raced by a short timer;
//!      when the timer wins the slow future is dropped and `conn.cancel(cx)`
//!      breaks + drains. Then the reuse query must return 12.
//!   2. DROP cancel: the slow future is dropped (its `CancelDrainGuard` arms the
//!      pending-drain flag) and the next fetch auto-drains the stranded call
//!      before issuing its own request. Then the reuse query must return 12.
//!
//! Self-skips when the container environment is absent, like
//! `reuse_after_call_timeout.rs`. Run against the container with:
//!
//! ```sh
//! eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1523 \
//!         ORACLEDB_HOST_PORT=1523 scripts/container.sh env)"
//! cargo test -p oracledb --test cancel_then_reuse
//! ```

use std::time::Duration;

use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::{time, Cx};
use oracledb::protocol::thin::QueryValue;
use oracledb::{ConnectOptions, Connection};
use oracledb_protocol::ClientIdentity;

mod common;

const PROGRAM: &str = "rust-oracledb-cancel";
const MACHINE: &str = "cancel-machine";
const OSUSER: &str = "cancel-osuser";
const TERMINAL: &str = "cancel-terminal";
const DRIVER: &str = "rust-oracledb thn : 0.0.0";

fn connect_options() -> Option<ConnectOptions> {
    let common::LiveCreds {
        connect_string,
        user,
        password,
    } = common::live_creds_opt()?;
    let identity = ClientIdentity::new(PROGRAM, MACHINE, OSUSER, TERMINAL, DRIVER).ok()?;
    Some(ConnectOptions::new(
        connect_string,
        user,
        password,
        identity,
    ))
}

async fn assert_select_seven_plus_five(conn: &mut Connection, cx: &Cx, context: &str) {
    let reuse = conn
        .execute_raw(
            cx,
            "select 7 + 5 from dual",
            2,
            &[],
            oracledb::protocol::thin::ExecuteOptions::default(),
            None,
        )
        .await
        .unwrap_or_else(|err| panic!("{context}: the connection must be reusable, got {err:?}"));
    assert_eq!(
        reuse.cell(0, 0).and_then(QueryValue::as_i64),
        Some(12),
        "{context}: the reused connection must return 7 + 5 = 12, not stale bytes"
    );
}

#[test]
fn connection_is_reusable_after_explicit_cancel_and_after_drop_cancel() {
    let Some(options) = connect_options() else {
        eprintln!(
            "skipped connection_is_reusable_after_explicit_cancel_and_after_drop_cancel: \
             PYO_TEST_* not set"
        );
        return;
    };

    let reactor = reactor::create_reactor().expect("native reactor should build for live I/O");
    let runtime = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("current-thread Asupersync runtime should build");

    runtime.block_on(async {
        let cx = Cx::current().expect("block_on installs an ambient Cx");
        let mut conn = Connection::connect(&cx, options)
            .await
            .expect("connect to test container");

        // Sanity: the connection works BEFORE any cancel, so a later failure is
        // attributable to cancel recovery, not a broken connection.
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
        assert_eq!(
            before.cell(0, 0).and_then(QueryValue::as_i64),
            Some(2),
            "the connection works before any cancel"
        );

        // ----- Scenario 1: EXPLICIT cancel -----
        // Race a 3 s server-side sleep against a 400 ms timer. The slow future is
        // genuinely in-flight (the server is still sleeping) when the timer wins
        // and the future is dropped; then cancel() breaks + drains the wire.
        let slow = "begin dbms_session.sleep(3); end;";
        let raced = time::timeout(
            time::wall_now(),
            Duration::from_millis(400),
            conn.execute_raw(
                &cx,
                slow,
                1,
                &[],
                oracledb::protocol::thin::ExecuteOptions::default(),
                None,
            ),
        )
        .await;
        assert!(
            raced.is_err(),
            "the 3 s sleep must NOT complete within the 400 ms race window"
        );
        // The slow future has been dropped. Explicitly cancel: break + drain so
        // the still-running server call is interrupted and the wire is clean.
        conn.cancel(&cx)
            .await
            .expect("explicit cancel must break + drain and leave the connection usable");
        assert!(
            !conn.is_dead(),
            "an explicit cancel must NOT mark the connection dead (session is alive)"
        );
        assert_select_seven_plus_five(&mut conn, &cx, "after explicit cancel").await;

        // ----- Scenario 2: DROP cancel (auto-drain on next op) -----
        // Drop the slow future again, but THIS time do not call cancel(); the
        // next operation must auto-break+drain via the armed CancelDrainGuard.
        let raced = time::timeout(
            time::wall_now(),
            Duration::from_millis(400),
            conn.execute_raw(
                &cx,
                slow,
                1,
                &[],
                oracledb::protocol::thin::ExecuteOptions::default(),
                None,
            ),
        )
        .await;
        assert!(
            raced.is_err(),
            "the 3 s sleep must NOT complete within the 400 ms race window"
        );
        // No explicit cancel() here: the next query is responsible for cleaning
        // up the stranded call before issuing its own request.
        assert_select_seven_plus_five(&mut conn, &cx, "after drop cancel (auto-drain)").await;

        // And it keeps working for more than one follow-up round trip, including
        // a multi-row fetch (exercises the paging path, not just scalar dual).
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
        assert_eq!(
            values,
            vec![1, 2, 3, 4, 5],
            "the recovered connection fetches a multi-row result correctly"
        );

        conn.close(&cx).await.expect("close connection");
    });
}
