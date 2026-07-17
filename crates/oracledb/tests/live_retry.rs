//! Live integration test for the idempotency-gated retry executor
//! (bead a4-r9a / iec3.1.23): reconnect-then-retry across a real server-side
//! session kill.
//!
//! `#[ignore]`d by default. Needs a lane container up, the `PYO_TEST_*` app
//! vars set, and a DBA login to kill the session — supplied out of band so no
//! secret is committed:
//!
//! ```text
//! PYO_TEST_CONNECT_STRING=localhost:1522/FREEPDB1 \
//! PYO_TEST_MAIN_USER=pythontest PYO_TEST_MAIN_PASSWORD=testpw \
//! PYO_TEST_SYSTEM_USER=system PYO_TEST_SYSTEM_PASSWORD=... \
//!   cargo test -p oracledb --test live_retry -- --ignored --nocapture
//! ```
//!
//! The scenario: connect an app session, capture its SID, kill it from a
//! separate DBA session, then run an idempotent `SELECT` through
//! `run_with_retry_reconnecting`. The first attempt fails connection-lost; the
//! executor reconnects and the retry succeeds. A non-idempotent operation would
//! never be replayed — that gate is proven exhaustively offline in
//! `src/retry.rs`; here we prove the transient-recovery path end to end.

use std::cell::{Cell, RefCell};

use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::Cx;
use oracledb::retry::{run_with_retry_reconnecting, Idempotency, RetryPolicy};
use oracledb::{ConnectOptions, Connection};
use oracledb_protocol::ClientIdentity;

mod common;

fn identity() -> ClientIdentity {
    ClientIdentity::new(
        "rust-oracledb",
        "rusthost",
        "rustuser",
        "rustterm",
        "rust-oracledb thn : 0.0.0",
    )
    .expect("identity")
}

fn app_options() -> ConnectOptions {
    ConnectOptions::new(
        common::live_conn_string_or(common::FREE23_CONNECT_STRING),
        common::live_user_or(common::FREE23_USER),
        common::live_password_or(common::FREE23_PASSWORD),
        identity(),
    )
}

/// DBA login used only to kill the app session. Read from the environment so no
/// secret lands in the committed test; the connect string is shared with the app.
fn system_options() -> ConnectOptions {
    let user = std::env::var("PYO_TEST_SYSTEM_USER").unwrap_or_else(|_| "system".to_string());
    let password = std::env::var("PYO_TEST_SYSTEM_PASSWORD")
        .expect("PYO_TEST_SYSTEM_PASSWORD must be set for the live session-kill retry test");
    ConnectOptions::new(
        common::live_conn_string_or(common::FREE23_CONNECT_STRING),
        user,
        password,
        identity(),
    )
}

fn run<F: std::future::Future<Output = ()>>(body: impl FnOnce(Cx) -> F) {
    let reactor = reactor::create_reactor().expect("reactor");
    let runtime = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let cx = Cx::current().expect("cx");
        body(cx).await;
    });
}

#[test]
#[ignore = "requires live Oracle container + DBA login (PYO_TEST_SYSTEM_PASSWORD)"]
fn reconnect_then_retry_across_server_side_session_kill() {
    run(|cx| async move {
        // App session under test, kept behind a cell so the reconnect hook can
        // swap in a fresh connection between attempts.
        let app = Connection::connect(&cx, app_options())
            .await
            .expect("app connect");
        let app = RefCell::new(Some(app));

        // Baseline: the session works, and capture its numeric SID.
        let sid: i64 = {
            let mut conn = { app.borrow_mut().take().expect("app present") };
            let sid_result = async {
                let baseline = conn.query_one(&cx, "select 7 from dual", ()).await?;
                assert_eq!(
                    baseline.get::<i64>(0)?,
                    7,
                    "baseline query must return its scalar"
                );
                conn.query_one(
                    &cx,
                    "select to_number(sys_context('userenv','sid')) from dual",
                    (),
                )
                .await?
                .get::<i64>(0)
            }
            .await;
            *app.borrow_mut() = Some(conn);
            sid_result.expect("baseline and SID queries must succeed")
        };

        // Kill the app session from a separate DBA session.
        {
            let mut sys = Connection::connect(&cx, system_options())
                .await
                .expect("system connect");
            let serial: i64 = sys
                .query_one(&cx, "select serial# from v$session where sid = :1", (sid,))
                .await
                .expect("serial# query")
                .get::<i64>(0)
                .expect("serial# scalar");
            let kill = format!("alter system kill session '{sid},{serial}' immediate");
            sys.execute(&cx, &kill, ()).await.expect("kill session");
            sys.close(&cx).await.ok();
        }

        // Idempotent SELECT through the reconnecting retry executor: the first
        // attempt hits the killed session (connection-lost), the hook reconnects,
        // and the retry succeeds.
        let reconnects = Cell::new(0usize);
        let out: oracledb::Result<i64> = run_with_retry_reconnecting(
            &cx,
            &RetryPolicy::default(),
            Idempotency::Idempotent,
            || async {
                let mut conn = { app.borrow_mut().take().expect("app present") };
                let query_result = async {
                    let row = conn.query_one(&cx, "select 7 from dual", ()).await?;
                    row.get::<i64>(0)
                }
                .await;
                *app.borrow_mut() = Some(conn);
                query_result
            },
            || async {
                reconnects.set(reconnects.get() + 1);
                let fresh = Connection::connect(&cx, app_options()).await?;
                *app.borrow_mut() = Some(fresh);
                Ok(())
            },
        )
        .await;

        assert_eq!(out.expect("retry recovered the killed session"), 7);
        assert!(
            reconnects.get() >= 1,
            "the killed session must have forced at least one reconnect"
        );

        let final_conn = app.borrow_mut().take();
        if let Some(conn) = final_conn {
            conn.close(&cx).await.ok();
        }
    });
}
