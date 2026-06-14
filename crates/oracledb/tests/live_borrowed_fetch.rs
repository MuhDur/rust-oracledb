//! Live parity test for the zero-copy borrowed fetch path
//! ([`Connection::for_each_row_ref`]): every borrowed cell, converted back to an
//! owned [`QueryValue`] via [`QueryValueRef::to_owned_value`], must equal the
//! value the existing owned fetch path produces for the same query. Exercised
//! over a wide, many-row, mixed-type result so the common scalar grid
//! (Text / Number / NULL) is the bulk of the work.
//!
//! Gated behind `#[ignore]` like the other live tests: run with the container
//! environment sourced (`scripts/container.sh env`).

use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::Cx;
use oracledb::protocol::thin::{QueryValue, QueryValueRef};
use oracledb::{ConnectOptions, Connection};
use oracledb_protocol::ClientIdentity;

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
        std::env::var("PYO_TEST_CONNECT_STRING")
            .unwrap_or_else(|_| "localhost:1522/FREEPDB1".into()),
        std::env::var("PYO_TEST_MAIN_USER").unwrap_or_else(|_| "pythontest".into()),
        std::env::var("PYO_TEST_MAIN_PASSWORD")
            .expect("PYO_TEST_MAIN_PASSWORD must be set for ignored live test"),
        identity,
    )
}

#[test]
#[ignore = "requires local Oracle listener from scripts/container.sh up"]
fn borrowed_fetch_matches_owned_fetch_for_wide_many_row_result() {
    let reactor = reactor::create_reactor().expect("native reactor should build for live I/O");
    let runtime = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("current-thread Asupersync runtime should build");

    runtime.block_on(async {
        let cx = Cx::current().expect("block_on installs an ambient Cx");
        let mut conn = Connection::connect(&cx, live_options())
            .await
            .expect("connect");

        // A wide, mixed-type, 5000-row result: a NUMBER, a VARCHAR2, a second
        // NUMBER, and a NULL column, paged across many fetch batches.
        let sql = "select level as n, \
                          rpad('row', 20, to_char(level)) as label, \
                          level * 1.5 as scaled, \
                          cast(null as varchar2(10)) as empty \
                   from dual connect by level <= 5000";
        let arraysize = 500;

        // --- Owned path: collect every row as owned QueryValues. ---
        let owned_rows = {
            let first = conn
                .execute_query_with_bind_rows(&cx, sql, arraysize, &[])
                .await
                .expect("owned execute");
            let cursor_id = first.cursor_id;
            let mut rows: Vec<Vec<Option<QueryValue>>> = first.rows.clone();
            let mut more = first.more_rows;
            let mut prev = first.rows.last().cloned();
            while more && cursor_id != 0 {
                let batch = conn
                    .fetch_rows(&cx, cursor_id, arraysize, prev.as_deref())
                    .await
                    .expect("owned fetch page");
                rows.extend(batch.rows.iter().cloned());
                more = batch.more_rows;
                if let Some(last) = batch.rows.last().cloned() {
                    prev = Some(last);
                }
            }
            conn.release_cursor(cursor_id);
            rows
        };

        // --- Borrowed path: collect each borrowed cell as owned via to_owned. ---
        let mut borrowed_rows: Vec<Vec<Option<QueryValue>>> = Vec::new();
        conn.for_each_row_ref(
            &cx,
            sql,
            arraysize,
            |row: &[Option<QueryValueRef<'_>>]| {
                borrowed_rows.push(
                    row.iter()
                        .map(|cell| cell.map(|v| v.to_owned_value()))
                        .collect(),
                );
                Ok(())
            },
        )
        .await
        .expect("borrowed fetch");

        assert_eq!(owned_rows.len(), 5000, "owned path drains all rows");
        assert_eq!(
            borrowed_rows.len(),
            owned_rows.len(),
            "borrowed path yields the same row count"
        );
        assert_eq!(
            borrowed_rows, owned_rows,
            "every borrowed cell to_owned() must equal the owned-path value"
        );

        conn.close(&cx).await.expect("close");
    });
}
