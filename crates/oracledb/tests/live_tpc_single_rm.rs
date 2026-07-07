//! Live single-resource-manager two-phase-commit demonstration (bead iec3.1.30).
//!
//! The XA/TPC wire path is proven exhaustively offline in
//! `oracledb_protocol`'s `tpc_golden` / `sessionless_golden` suites, and end to
//! end (cross-connection, ORA-24756 behaviour) in `e2e_live.rs`. This suite is a
//! Rust-native *documentation-value* demonstration of the ordinary single-RM
//! flow, driving the driver's public async TPC API on [`Connection`] directly
//! (not through the pyshim):
//!
//!   begin(Xid) -> DML -> tpc_end -> tpc_prepare -> tpc_commit (persists), and
//!   begin(Xid) -> DML -> tpc_end -> tpc_prepare -> tpc_rollback (discards).
//!
//! Like every live suite it is `#[ignore]`d by default and self-skips when the
//! `PYO_TEST_*` lane environment is not configured. Run against a container:
//!
//! ```sh
//! cargo test -p oracledb --test live_tpc_single_rm -- --ignored --nocapture
//! ```
use std::future::Future;
use std::pin::Pin;

use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::Cx;
use oracledb::protocol::thin::{ExecuteOptions, QueryResult, QueryValue, TPC_TXN_FLAGS_NEW};
use oracledb::{ConnectOptions, Connection};
use oracledb_protocol::ClientIdentity;

mod common;

/// `Some(options)` only when all three `PYO_TEST_*` variables are set; `None`
/// (the self-skip path) otherwise. Mirrors `prefetch_overlap.rs::live_options`.
fn live_options() -> Option<ConnectOptions> {
    let common::LiveCreds {
        connect_string,
        user,
        password,
    } = common::live_creds_opt()?;
    let identity = ClientIdentity::new(
        "rust-oracledb-tpc",
        "tpc-host",
        "tpc-user",
        "tpc-term",
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

/// Run `body` on a fresh current-thread runtime + live connection, closing it
/// afterwards. Mirrors `live_lob_stream.rs::with_conn`.
fn with_live_conn<F>(options: ConnectOptions, body: F)
where
    F: for<'a> FnOnce(&'a mut Connection, &'a Cx) -> Pin<Box<dyn Future<Output = ()> + 'a>>,
{
    let reactor = reactor::create_reactor().expect("reactor");
    let runtime = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let cx = Cx::current().expect("cx");
        let mut conn = Connection::connect(&cx, options).await.expect("connect");
        body(&mut conn, &cx).await;
        conn.close(&cx).await.expect("close");
    });
}

/// Execute a statement with no binds and no prefetch fanout.
async fn exec(conn: &mut Connection, cx: &Cx, sql: &str) -> QueryResult {
    conn.execute_raw(cx, sql, 1, &[], ExecuteOptions::default(), None)
        .await
        .unwrap_or_else(|e| panic!("execute `{sql}`: {e:?}"))
}

/// `drop table` that tolerates a not-yet-existing table (ORA-00942).
async fn drop_table_if_exists(conn: &mut Connection, cx: &Cx, table: &str) {
    let sql = format!(
        "begin execute immediate 'drop table {table}'; \
         exception when others then null; end;"
    );
    exec(conn, cx, &sql).await;
}

/// `select count(*) from <table>` as an `i64`.
async fn count_rows(conn: &mut Connection, cx: &Cx, table: &str) -> i64 {
    let r = exec(conn, cx, &format!("select count(*) from {table}")).await;
    r.cell(0, 0)
        .and_then(QueryValue::as_i64)
        .expect("count(*) scalar")
}

#[test]
#[ignore = "requires live Oracle container (free23/xe21) with PYO_TEST_* set"]
fn single_rm_two_phase_commit_persists_row() {
    let Some(options) = live_options() else {
        eprintln!("skipped: PYO_TEST_* not set");
        return;
    };
    with_live_conn(options, |conn, cx| {
        Box::pin(async move {
            let table = "rust_tpc_srm_commit";
            // Table lives outside the branch (DDL implicitly commits server-side).
            drop_table_if_exists(conn, cx, table).await;
            exec(
                conn,
                cx,
                &format!("create table {table} (id number primary key)"),
            )
            .await;

            // A synthetic global transaction id: (format_id, gtrid, bqual).
            let format_id: u32 = 0x0510_5f31;
            let gtrid = b"rust-oracledb-single-rm-commit";
            let bqual = b"branch-commit";

            conn.tpc_begin(cx, format_id, gtrid, bqual, TPC_TXN_FLAGS_NEW, 0)
                .await
                .expect("tpc_begin new branch");
            exec(conn, cx, &format!("insert into {table} values (1)")).await;
            conn.tpc_end(cx, None, 0).await.expect("tpc_end detach");

            let needs_commit = conn
                .tpc_prepare(cx, Some((format_id, gtrid.as_slice(), bqual.as_slice())))
                .await
                .expect("tpc_prepare");
            assert!(
                needs_commit,
                "prepared branch with a DML must require commit"
            );

            // Two-phase commit (one_phase = false): server returns FORGOTTEN.
            conn.tpc_commit(
                cx,
                Some((format_id, gtrid.as_slice(), bqual.as_slice())),
                false,
            )
            .await
            .expect("tpc_commit two-phase");

            assert_eq!(
                count_rows(conn, cx, table).await,
                1,
                "committed row persists"
            );
            drop_table_if_exists(conn, cx, table).await;
        })
    });
}

#[test]
#[ignore = "requires live Oracle container (free23/xe21) with PYO_TEST_* set"]
fn single_rm_prepared_branch_rollback_discards_row() {
    let Some(options) = live_options() else {
        eprintln!("skipped: PYO_TEST_* not set");
        return;
    };
    with_live_conn(options, |conn, cx| {
        Box::pin(async move {
            let table = "rust_tpc_srm_rollback";
            drop_table_if_exists(conn, cx, table).await;
            exec(
                conn,
                cx,
                &format!("create table {table} (id number primary key)"),
            )
            .await;

            let format_id: u32 = 0x0510_5f32;
            let gtrid = b"rust-oracledb-single-rm-rollback";
            let bqual = b"branch-rollback";

            conn.tpc_begin(cx, format_id, gtrid, bqual, TPC_TXN_FLAGS_NEW, 0)
                .await
                .expect("tpc_begin new branch");
            exec(conn, cx, &format!("insert into {table} values (1)")).await;
            conn.tpc_end(cx, None, 0).await.expect("tpc_end detach");

            let needs_commit = conn
                .tpc_prepare(cx, Some((format_id, gtrid.as_slice(), bqual.as_slice())))
                .await
                .expect("tpc_prepare");
            assert!(
                needs_commit,
                "prepared branch with a DML must require commit"
            );

            // Abort the prepared branch instead of committing it.
            conn.tpc_rollback(cx, Some((format_id, gtrid.as_slice(), bqual.as_slice())))
                .await
                .expect("tpc_rollback");

            assert_eq!(
                count_rows(conn, cx, table).await,
                0,
                "rolled-back branch leaves no row"
            );
            drop_table_if_exists(conn, cx, table).await;
        })
    });
}
