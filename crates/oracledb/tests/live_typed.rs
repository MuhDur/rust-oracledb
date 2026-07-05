#![forbid(unsafe_code)]

//! Live integration tests for the typed read/write surface (beads qxn + zjd).
//!
//! Exercises [`FromSql`] / [`QueryResultExt::get`] (typed extraction), [`ToSql`]
//! / the [`params!`] macro / `query` (ergonomic binds), and the
//! LOSSLESS `rust_decimal::Decimal` NUMBER round trip against the real
//! container.
//!
//! Self-skips cleanly when the container environment is absent. Run with:
//!
//! ```sh
//! eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1525 \
//!         ORACLEDB_HOST_PORT=1525 scripts/container.sh env)"
//! cargo test -p oracledb --test live_typed \
//!   --features "chrono uuid serde_json rust_decimal" -- --nocapture
//! ```

use std::num::NonZeroU32;

use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::Cx;
use oracledb::protocol::oson::OsonValue;
use oracledb::protocol::thin::{
    decode_lob_text, BindValue, ExecuteOptions, QueryResult, QueryValue, CS_FORM_IMPLICIT,
    CS_FORM_NCHAR, ORA_TYPE_NUM_BINARY_DOUBLE, ORA_TYPE_NUM_BINARY_FLOAT, ORA_TYPE_NUM_BLOB,
    ORA_TYPE_NUM_BOOLEAN, ORA_TYPE_NUM_CHAR, ORA_TYPE_NUM_CLOB, ORA_TYPE_NUM_DATE,
    ORA_TYPE_NUM_INTERVAL_DS, ORA_TYPE_NUM_INTERVAL_YM, ORA_TYPE_NUM_JSON, ORA_TYPE_NUM_LONG,
    ORA_TYPE_NUM_LONG_RAW, ORA_TYPE_NUM_NUMBER, ORA_TYPE_NUM_RAW, ORA_TYPE_NUM_ROWID,
    ORA_TYPE_NUM_TIMESTAMP, ORA_TYPE_NUM_TIMESTAMP_LTZ, ORA_TYPE_NUM_TIMESTAMP_TZ,
    ORA_TYPE_NUM_VARCHAR, ORA_TYPE_NUM_VECTOR, SUBSCR_QOS_QUERY, TNS_SUBSCR_NAMESPACE_DBCHANGE,
};
use oracledb::protocol::vector::{Vector, VectorValues};
use oracledb::{
    params, Batch, BlockingConnection, ConnectOptions, Connection, Error, Execute, FromRow, Query,
    QueryResultExt, Registration, Row,
};
use oracledb_protocol::ClientIdentity;

const PROGRAM: &str = "rust-oracledb-typed-itest";
const MACHINE: &str = "itest-machine";
const OSUSER: &str = "itest-osuser";
const TERMINAL: &str = "itest-terminal";
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

fn with_connection(test: &str, body: impl FnOnce(&mut Connection)) {
    let Some(options) = connect_options() else {
        eprintln!("skipped {test}: PYO_TEST_* environment not configured");
        return;
    };
    let mut conn = BlockingConnection::connect(options).expect("connect to test container");
    body(&mut conn);
    BlockingConnection::close(conn).expect("close connection");
}

fn execute_raw(
    conn: &mut Connection,
    sql: &str,
    prefetch_rows: u32,
) -> oracledb::Result<QueryResult> {
    BlockingConnection::execute_raw(
        conn,
        sql,
        prefetch_rows,
        &[],
        ExecuteOptions::default(),
        None,
    )
}

fn execute_raw_collect(
    conn: &mut Connection,
    sql: &str,
    prefetch_rows: u32,
) -> oracledb::Result<QueryResult> {
    let mut result = execute_raw(conn, sql, prefetch_rows)?;
    if result.cursor_id != 0 && result.rows.is_empty() {
        let cursor_id = result.cursor_id;
        let columns = result.columns.clone();
        let fetched = BlockingConnection::define_and_fetch_rows_with_columns(
            conn,
            cursor_id,
            prefetch_rows.max(1),
            &columns,
            None,
        )?;
        result.rows = fetched.rows;
        result.more_rows = fetched.more_rows;
        if !fetched.columns.is_empty() {
            result.columns = fetched.columns;
        }
    }
    Ok(result)
}

/// `query` with a positional tuple of typed Rust values, then typed `get`.
#[test]
fn query_positional_tuple_and_typed_get() {
    with_connection("query_positional_tuple_and_typed_get", |conn| {
        // (40, 2) binds :1, :2 — no manual BindValue::Number any more.
        let row = BlockingConnection::query(conn, "select :1 + :2 from dual", (40_i64, 2_i64))
            .expect("query with tuple binds");
        let sum: i64 = row.one().expect("one row").get(0).expect("typed get i64");
        assert_eq!(sum, 42);

        // mixed-type tuple: number + string, read back by typed accessors
        let row = BlockingConnection::query(
            conn,
            "select :1 as id, :2 as name from dual",
            (7_i64, "alice"),
        )
        .expect("mixed tuple binds")
        .one()
        .expect("one row");
        assert_eq!(row.get::<i64>(0).unwrap(), 7);
        assert_eq!(row.get_by_name::<String>("NAME").unwrap(), "alice");
        eprintln!(
            "positional ok: id={} name={}",
            row.get::<i64>(0).unwrap(),
            row.get_by_name::<String>("name").unwrap()
        );
    });
}

/// `params!` positional form feeds `query` just like a tuple.
#[test]
fn params_macro_positional() {
    with_connection("params_macro_positional", |conn| {
        let row = BlockingConnection::query_one(
            conn,
            "select :1 + :2 + :3 from dual",
            params![10_i64, 20_i64, 12_i64],
        )
        .expect("params! positional");
        assert_eq!(row.get::<i64>(0).unwrap(), 42);
    });
}

/// `query` with `params!{ ":a" => .., ":b" => .. }` — the names are
/// reordered to placeholder first-appearance order, so swapping the param order
/// still binds correctly.
#[test]
fn query_named_reorders_correctly() {
    with_connection("query_named_reorders_correctly", |conn| {
        // :a appears first in the SQL; pass the params in the opposite order to
        // prove the reorder. 100 - 1 = 99 (not 1 - 100).
        let result = BlockingConnection::query_one(
            conn,
            "select :a - :b as diff from dual",
            params! { ":b" => 1_i64, ":a" => 100_i64 },
        )
        .expect("named binds");
        let diff: i64 = result.get_by_name("DIFF").expect("DIFF column");
        assert_eq!(diff, 99, "named binds must map by name, not order given");
        eprintln!("named ok: diff={diff}");
    });
}

#[test]
fn async_query_family_eager_drains_and_checks_cardinality() {
    let Some(options) = connect_options() else {
        eprintln!("skipped async_query_family_eager_drains_and_checks_cardinality: PYO_TEST_* environment not configured");
        return;
    };
    let reactor = reactor::create_reactor().expect("native reactor should build for live I/O");
    let runtime = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("current-thread Asupersync runtime should build");

    runtime.block_on(async {
        let cx = Cx::current().expect("Runtime::block_on should install an ambient Cx");
        let mut conn = Connection::connect(&cx, options)
            .await
            .expect("connect to test container");

        let rows = conn
            .query_with(
                &cx,
                Query::new("select level as n from dual connect by level <= 105")
                    .arraysize(NonZeroU32::new(25).expect("non-zero"))
                    .prefetch(25),
            )
            .await
            .expect("query_with")
            .collect(&cx)
            .await
            .expect("collect");
        assert_eq!(rows.len(), 105);
        assert_eq!(rows[0].get_by_name::<i64>("N").unwrap(), 1);
        assert_eq!(rows[104].get::<i64>(0).unwrap(), 105);

        {
            let mut streamed = conn
                .query_with(
                    &cx,
                    Query::new("select level as n from dual connect by level <= 105")
                        .arraysize(NonZeroU32::new(25).expect("non-zero"))
                        .prefetch(25),
                )
                .await
                .expect("streamed query_with");
            let mut seen = Vec::new();
            loop {
                seen.extend(
                    streamed
                        .batch()
                        .iter()
                        .map(|row| row.get_by_name::<i64>("N").unwrap()),
                );
                if !streamed.next_batch(&cx).await.expect("next_batch") {
                    break;
                }
            }
            assert_eq!(seen.len(), 105);
            assert_eq!(seen[104], 105);
        }

        let all = conn
            .query_all(
                &cx,
                "select level as n from dual connect by level <= 105",
                (),
            )
            .await
            .expect("query_all eager drain");
        assert_eq!(all.len(), 105);
        let first_all = all[0].clone();
        let last_all = all[104].clone();

        let one = conn
            .query_one(&cx, "select :1 + :2 as n from dual", (40_i64, 2_i64))
            .await
            .expect("query_one");

        let opt = conn
            .query_opt(&cx, "select 1 as n from dual where 1 = 0", ())
            .await
            .expect("query_opt none");
        assert!(opt.is_none());

        let err = conn
            .query_one(&cx, "select level as n from dual connect by level <= 2", ())
            .await
            .expect_err("query_one must reject >1 row");
        assert!(matches!(err, Error::TooManyRows));

        conn.close(&cx).await.expect("close connection");

        assert_eq!(first_all.get::<i64>(0).unwrap(), 1);
        assert_eq!(first_all.get::<i64>("N").unwrap(), 1);
        assert_eq!(first_all.try_get::<i64>(0).unwrap(), Some(1));
        assert_eq!(first_all.try_get::<i64>("N").unwrap(), Some(1));
        assert_eq!(first_all.value(0).and_then(QueryValue::as_i64), Some(1));
        assert_eq!(first_all.value("N").and_then(QueryValue::as_i64), Some(1));
        assert_eq!(last_all.get::<i64>(0).unwrap(), 105);
        assert_eq!(last_all.get::<i64>("N").unwrap(), 105);
        assert_eq!(one.get::<i64>(0).unwrap(), 42);
        assert_eq!(one.get::<i64>("N").unwrap(), 42);
        assert_eq!(one.get_by_name::<i64>("N").unwrap(), 42);
    });
}

#[test]
fn async_rows_into_typed_drains_all_batches() {
    #[derive(Debug, PartialEq, FromRow)]
    struct NumberRow {
        n: i64,
    }

    let Some(options) = connect_options() else {
        eprintln!("skipped async_rows_into_typed_drains_all_batches: PYO_TEST_* environment not configured");
        return;
    };
    let reactor = reactor::create_reactor().expect("native reactor should build for live I/O");
    let runtime = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("current-thread Asupersync runtime should build");

    runtime.block_on(async {
        let cx = Cx::current().expect("Runtime::block_on should install an ambient Cx");
        let mut conn = Connection::connect(&cx, options)
            .await
            .expect("connect to test container");

        let rows: Vec<NumberRow> = conn
            .query_with(
                &cx,
                Query::new("select level as n from dual connect by level <= 105 order by n")
                    .arraysize(NonZeroU32::new(25).expect("non-zero"))
                    .prefetch(25),
            )
            .await
            .expect("query_with")
            .into_typed(&cx)
            .await
            .expect("into_typed drains all batches");

        assert_eq!(rows.len(), 105);
        assert_eq!(rows.first().expect("first row").n, 1);
        assert_eq!(rows.last().expect("last row").n, 105);

        conn.close(&cx).await.expect("close connection");
    });
}

#[test]
fn async_execute_family_surfaces_outcome() {
    let Some(options) = connect_options() else {
        eprintln!(
            "skipped async_execute_family_surfaces_outcome: PYO_TEST_* environment not configured"
        );
        return;
    };
    let reactor = reactor::create_reactor().expect("native reactor should build for live I/O");
    let runtime = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("current-thread Asupersync runtime should build");

    runtime.block_on(async {
        let cx = Cx::current().expect("Runtime::block_on should install an ambient Cx");
        let mut conn = Connection::connect(&cx, options)
            .await
            .expect("connect to test container");

        let _ = conn.execute(&cx, "drop table rust_execute_outcome_t", ()).await;
        conn.execute(
            &cx,
            "create table rust_execute_outcome_t (id number primary key, name varchar2(30))",
            (),
        )
        .await
        .expect("create execute outcome table");

        let insert = conn
            .execute(
                &cx,
                "insert into rust_execute_outcome_t (id, name) values (:1, :2)",
                (1_i64, "alice"),
            )
            .await
            .expect("insert via execute");
        assert_eq!(insert.rows_affected(), 1);
        assert!(insert.out_binds().is_empty());
        assert!(insert.returning().is_empty());

        let out = conn
            .execute_with(
                &cx,
                Execute::new("begin :1 := 'out-value'; end;").bind(vec![BindValue::Output {
                    ora_type_num: ORA_TYPE_NUM_VARCHAR,
                    csfrm: CS_FORM_IMPLICIT,
                    buffer_size: 30,
                }]),
            )
            .await
            .expect("PL/SQL OUT bind");
        assert_eq!(
            out.out_binds()
                .get(0)
                .and_then(Option::as_ref)
                .and_then(|value| value.as_text()),
            Some("out-value")
        );

        let returning = conn
            .execute_with(
                &cx,
                Execute::new(
                    "update rust_execute_outcome_t set name = :1 where id = :2 returning name into :3",
                )
                .bind(vec![
                    BindValue::Text("bob".to_string()),
                    BindValue::Number("1".to_string()),
                    BindValue::ReturnOutput {
                        ora_type_num: ORA_TYPE_NUM_VARCHAR,
                        csfrm: CS_FORM_IMPLICIT,
                        buffer_size: 30,
                    },
                ]),
            )
            .await
            .expect("DML RETURNING");
        assert_eq!(returning.rows_affected(), 1);
        assert_eq!(
            returning
                .returning()
                .rows_for(2)
                .and_then(|rows| rows.first())
                .and_then(Option::as_ref)
                .and_then(|value| value.as_text()),
            Some("bob")
        );

        let parsed = conn
            .execute_with(
                &cx,
                Execute::new("select 1 from dual")
                    .raw_options(ExecuteOptions::default().with_parse_only(true)),
            )
            .await
            .expect("parse-only via raw options");
        assert_eq!(parsed.rows_affected(), 0);

        conn.close(&cx).await.expect("close connection");
    });
}

#[test]
fn async_execute_many_family_surfaces_batch_outcome() {
    let Some(options) = connect_options() else {
        eprintln!(
            "skipped async_execute_many_family_surfaces_batch_outcome: PYO_TEST_* environment not configured"
        );
        return;
    };
    let reactor = reactor::create_reactor().expect("native reactor should build for live I/O");
    let runtime = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("current-thread Asupersync runtime should build");

    runtime.block_on(async {
        let cx = Cx::current().expect("Runtime::block_on should install an ambient Cx");
        let mut conn = Connection::connect(&cx, options)
            .await
            .expect("connect to test container");

        let _ = conn.execute(&cx, "drop table rust_batch_outcome_t purge", ()).await;
        conn.execute(
            &cx,
            "create table rust_batch_outcome_t (id number primary key, name varchar2(30))",
            (),
        )
        .await
        .expect("create batch outcome table");

        let rows = vec![
            vec![
                BindValue::Number("1".to_string()),
                BindValue::Text("alice".to_string()),
            ],
            vec![
                BindValue::Number("2".to_string()),
                BindValue::Text("bob".to_string()),
            ],
            vec![
                BindValue::Number("3".to_string()),
                BindValue::Text("carol".to_string()),
            ],
        ];
        let inserted = conn
            .execute_many(
                &cx,
                "insert into rust_batch_outcome_t (id, name) values (:1, :2)",
                &rows,
            )
            .await
            .expect("array DML insert via execute_many");
        assert_eq!(inserted.rows_affected(), 3);
        assert_eq!(inserted.per_row_counts(), None);
        assert!(inserted.errors().is_empty());
        assert!(inserted.returning().is_empty());

        let delete_rows = vec![
            vec![BindValue::Number("1".to_string())],
            vec![BindValue::Number("2".to_string())],
            vec![BindValue::Number("99".to_string())],
        ];
        let deleted = conn
            .execute_many_with(
                &cx,
                Batch::new(
                    "delete from rust_batch_outcome_t where id = :1",
                    &delete_rows,
                )
                .row_counts(),
            )
            .await
            .expect("array DML delete row counts");
        assert_eq!(deleted.rows_affected(), 2);
        assert_eq!(deleted.per_row_counts(), Some([1, 1, 0].as_slice()));
        assert!(deleted.errors().is_empty());

        let error_rows = vec![
            vec![
                BindValue::Number("3".to_string()),
                BindValue::Text("duplicate".to_string()),
            ],
            vec![
                BindValue::Number("4".to_string()),
                BindValue::Text("dana".to_string()),
            ],
        ];
        let with_error = conn
            .execute_many_with(
                &cx,
                Batch::new(
                    "insert into rust_batch_outcome_t (id, name) values (:1, :2)",
                    &error_rows,
                )
                .collect_errors(),
            )
            .await
            .expect("array DML collect_errors");
        assert_eq!(with_error.errors().len(), 1);
        assert_eq!(with_error.errors()[0].row_index(), 0);
        assert_eq!(with_error.errors()[0].code(), 1);
        assert!(
            !with_error.errors()[0].message().is_empty(),
            "batch errors should carry the server message"
        );

        let returning_rows = vec![
            vec![
                BindValue::Text("cora".to_string()),
                BindValue::Number("3".to_string()),
                BindValue::ReturnOutput {
                    ora_type_num: ORA_TYPE_NUM_VARCHAR,
                    csfrm: CS_FORM_IMPLICIT,
                    buffer_size: 30,
                },
            ],
            vec![
                BindValue::Text("dana2".to_string()),
                BindValue::Number("4".to_string()),
                BindValue::ReturnOutput {
                    ora_type_num: ORA_TYPE_NUM_VARCHAR,
                    csfrm: CS_FORM_IMPLICIT,
                    buffer_size: 30,
                },
            ],
        ];
        let returning = conn
            .execute_many_with(
                &cx,
                Batch::new(
                    "update rust_batch_outcome_t set name = :1 where id = :2 returning name into :3",
                    &returning_rows,
                )
                .row_counts(),
            )
            .await
            .expect("array DML RETURNING");
        assert_eq!(returning.rows_affected(), 2);
        assert_eq!(returning.per_row_counts(), Some([1, 1].as_slice()));
        let returned: Vec<&str> = returning
            .returning()
            .rows_for(2)
            .expect("returning bind index")
            .iter()
            .map(|value| {
                value
                    .as_ref()
                    .and_then(|value| value.as_text())
                    .expect("returned text")
            })
            .collect();
        assert_eq!(returned, vec!["cora", "dana2"]);

        conn.execute(&cx, "drop table rust_batch_outcome_t purge", ())
            .await
            .expect("drop batch outcome table");
        conn.close(&cx).await.expect("close connection");
    });
}

#[test]
fn async_register_query_surfaces_query_id_when_cqn_available() {
    let Some(options) = connect_options() else {
        eprintln!(
            "skipped async_register_query_surfaces_query_id_when_cqn_available: PYO_TEST_* environment not configured"
        );
        return;
    };
    let reactor = reactor::create_reactor().expect("native reactor should build for live I/O");
    let runtime = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("current-thread Asupersync runtime should build");

    runtime.block_on(async {
        let cx = Cx::current().expect("Runtime::block_on should install an ambient Cx");
        let mut conn = Connection::connect(&cx, options)
            .await
            .expect("connect to test container");

        let _ = conn
            .execute(&cx, "drop table rust_register_query_t purge", ())
            .await;
        conn.execute(
            &cx,
            "create table rust_register_query_t (id number primary key, name varchar2(30))",
            (),
        )
        .await
        .expect("create CQN registration table");

        let subscription = match conn
            .subscribe_register(
                &cx,
                TNS_SUBSCR_NAMESPACE_DBCHANGE,
                None,
                SUBSCR_QOS_QUERY,
                0,
                30,
                0,
                0,
                0,
            )
            .await
        {
            Ok(subscription) => subscription,
            Err(err) => {
                eprintln!(
                    "skipped async_register_query_surfaces_query_id_when_cqn_available: CQN subscribe unavailable: {err}"
                );
                let _ = conn
                    .execute(&cx, "drop table rust_register_query_t purge", ())
                    .await;
                conn.close(&cx).await.expect("close connection");
                return;
            }
        };

        // CQN register_query is a 21c+ thin-mode extension (no python-oracledb
        // thin parity — DPY-3001); pre-21c CQN registration semantics differ
        // (18c: ORA-29970). Gate on the server version — bead
        // rust-oracledb-cqn18c.
        if conn.server_version_tuple().is_none_or(|v| v.0 < 21) {
            eprintln!(
                "[live_typed] SKIP register_query: 21c+ thin-mode extension, pre-21c differs (18c ORA-29970)"
            );
        } else {
            let registered = conn
                .register_query(
                    &cx,
                    Registration::new(
                        "select id, name from rust_register_query_t where id > :1",
                        subscription.registration_id,
                    )
                    .bind((0_i64,)),
                )
                .await
                .expect("register query");
            assert!(
                matches!(registered.query_id(), Some(id) if id > 0),
                "CQN register_query should surface a positive query id, got {:?}",
                registered.query_id()
            );
            // Only unsubscribe on 21c+ — pre-21c returns ORA-29970 for the same
            // registration id (bead rust-oracledb-cqn18c).
            if let Some(client_id) = subscription.client_id.as_deref() {
                conn.subscribe_unregister(
                    &cx,
                    subscription.registration_id,
                    client_id,
                    TNS_SUBSCR_NAMESPACE_DBCHANGE,
                    None,
                    SUBSCR_QOS_QUERY,
                    0,
                    30,
                    0,
                    0,
                    0,
                )
                .await
                .expect("unsubscribe CQN");
            }
        }

        conn.execute(&cx, "drop table rust_register_query_t purge", ())
            .await
            .expect("drop CQN registration table");
        conn.close(&cx).await.expect("close connection");
    });
}

#[test]
fn blocking_connection_mirrors_four_operation_families() {
    with_connection(
        "blocking_connection_mirrors_four_operation_families",
        |conn| {
            let _ =
                BlockingConnection::execute(conn, "drop table rust_blocking_family_t purge", ());
            BlockingConnection::execute(
                conn,
                "create table rust_blocking_family_t (id number primary key, name varchar2(30))",
                (),
            )
            .expect("create blocking family table");

            let insert = BlockingConnection::execute(
                conn,
                "insert into rust_blocking_family_t (id, name) values (:1, :2)",
                (1_i64, "alice"),
            )
            .expect("execute insert");
            assert_eq!(insert.rows_affected(), 1);

            let batch_rows = vec![
                vec![
                    BindValue::Number("2".to_string()),
                    BindValue::Text("bob".to_string()),
                ],
                vec![
                    BindValue::Number("3".to_string()),
                    BindValue::Text("carol".to_string()),
                ],
            ];
            let batch = BlockingConnection::execute_many(
                conn,
                "insert into rust_blocking_family_t (id, name) values (:1, :2)",
                &batch_rows,
            )
            .expect("execute_many insert");
            assert_eq!(batch.rows_affected(), 2);

            let all = BlockingConnection::query_all(
                conn,
                "select id, name from rust_blocking_family_t order by id",
                (),
            )
            .expect("query_all");
            assert_eq!(all.len(), 3);
            assert_eq!(all[0].get::<i64>("ID").unwrap(), 1);

            let one = BlockingConnection::query_one(
                conn,
                "select name from rust_blocking_family_t where id = :1",
                (2_i64,),
            )
            .expect("query_one");
            assert_eq!(one.get::<String>(0).unwrap(), "bob");

            let none = BlockingConnection::query_opt(
                conn,
                "select name from rust_blocking_family_t where id = :1",
                (99_i64,),
            )
            .expect("query_opt");
            assert!(none.is_none());

            let mut rows = BlockingConnection::query_with(
                conn,
                Query::new("select id from rust_blocking_family_t order by id")
                    .arraysize(NonZeroU32::new(1).expect("non-zero"))
                    .prefetch(1),
            )
            .expect("query_with");
            assert_eq!(rows.batch()[0].get::<i64>(0).unwrap(), 1);
            assert!(rows.next_batch().expect("next blocking batch"));
            assert_eq!(rows.batch()[0].get::<i64>(0).unwrap(), 2);
            assert!(rows.next_batch().expect("final blocking batch"));
            assert_eq!(rows.batch()[0].get::<i64>(0).unwrap(), 3);
            assert!(!rows.next_batch().expect("cursor exhausted"));
            drop(rows);

            let out = BlockingConnection::execute_with(
                conn,
                Execute::new("begin :1 := 'mirror'; end;").bind(vec![BindValue::Output {
                    ora_type_num: ORA_TYPE_NUM_VARCHAR,
                    csfrm: CS_FORM_IMPLICIT,
                    buffer_size: 30,
                }]),
            )
            .expect("execute_with output bind");
            assert_eq!(
                out.out_binds()
                    .get(0)
                    .and_then(Option::as_ref)
                    .and_then(|value| value.as_text()),
                Some("mirror")
            );

            let delete_rows = vec![
                vec![BindValue::Number("1".to_string())],
                vec![BindValue::Number("2".to_string())],
                vec![BindValue::Number("99".to_string())],
            ];
            let deleted = BlockingConnection::execute_many_with(
                conn,
                Batch::new(
                    "delete from rust_blocking_family_t where id = :1",
                    &delete_rows,
                )
                .row_counts(),
            )
            .expect("execute_many_with row counts");
            assert_eq!(deleted.rows_affected(), 2);
            assert_eq!(deleted.per_row_counts(), Some([1, 1, 0].as_slice()));

            BlockingConnection::execute(conn, "commit", ()).expect("commit before CQN register");

            match BlockingConnection::subscribe_register(
                conn,
                TNS_SUBSCR_NAMESPACE_DBCHANGE,
                None,
                SUBSCR_QOS_QUERY,
                0,
                30,
                0,
                0,
                0,
            ) {
                Ok(subscription) => {
                    // CQN register_query is a 21c+ thin-mode extension (no
                    // python-oracledb thin parity — DPY-3001); pre-21c CQN
                    // registration semantics differ (18c: ORA-29970). Gate on
                    // the server version — bead rust-oracledb-cqn18c.
                    if conn.server_version_tuple().is_none_or(|v| v.0 < 21) {
                        eprintln!(
                            "[live_typed] SKIP blocking register_query: 21c+ thin-mode extension, pre-21c differs (18c ORA-29970)"
                        );
                    } else {
                        let registered = BlockingConnection::register_query(
                            conn,
                            Registration::new(
                                "select id, name from rust_blocking_family_t where id > :1",
                                subscription.registration_id,
                            )
                            .bind((0_i64,)),
                        )
                        .expect("register_query");
                        assert!(
                            matches!(registered.query_id(), Some(id) if id > 0),
                            "blocking register_query should surface a positive query id, got {:?}",
                            registered.query_id()
                        );
                        // Only unsubscribe on 21c+ — pre-21c returns ORA-29970
                        // for the same registration id (bead
                        // rust-oracledb-cqn18c).
                        if let Some(client_id) = subscription.client_id.as_deref() {
                            BlockingConnection::subscribe_unregister(
                                conn,
                                subscription.registration_id,
                                client_id,
                                TNS_SUBSCR_NAMESPACE_DBCHANGE,
                                None,
                                SUBSCR_QOS_QUERY,
                                0,
                                30,
                                0,
                                0,
                                0,
                            )
                            .expect("unsubscribe CQN");
                        }
                    }
                }
                Err(err) => {
                    eprintln!("skipped blocking register_query assertion: CQN unavailable: {err}");
                }
            }

            BlockingConnection::execute(conn, "drop table rust_blocking_family_t purge", ())
                .expect("drop blocking family table");
        },
    );
}

/// Typed extraction of several scalar types out of one row.
#[test]
fn typed_extraction_scalars() {
    with_connection("typed_extraction_scalars", |conn| {
        let result = execute_raw(conn, "select 42 as n, 2.5 as d, 'hello' as s from dual", 1)
            .expect("scalar select");
        let row = result.typed_row(0);
        assert_eq!(row.get::<i64>(0).unwrap(), 42);
        assert_eq!(row.get::<f64>(1).unwrap(), 2.5);
        assert_eq!(row.get::<String>(2).unwrap(), "hello");
        assert_eq!(row.get_by_name::<i32>("N").unwrap(), 42);
    });
}

/// The LOSSLESS Decimal proof against the real database: bind a high-precision
/// Decimal, store it in a NUMBER column, read it back, and assert it is exactly
/// equal — no float rounding anywhere on the wire round trip.
#[cfg(feature = "rust_decimal")]
#[test]
fn decimal_roundtrip_lossless_live() {
    use rust_decimal::Decimal;
    use std::str::FromStr;

    with_connection("decimal_roundtrip_lossless_live", |conn| {
        let _ = execute_raw(conn, "drop table dec_rt_t", 1);
        execute_raw(conn, "create table dec_rt_t (v number)", 1).expect("create dec table");

        // 28 significant digits — the full precision rust_decimal can hold.
        let text = "7922816251426433759354.395033";
        let dec = Decimal::from_str(text).unwrap();

        // bind the Decimal directly via ToSql (query / params!)
        BlockingConnection::execute(conn, "insert into dec_rt_t values (:1)", (dec,))
            .expect("insert decimal");
        execute_raw(conn, "commit", 1).expect("commit");

        let result = execute_raw(conn, "select v from dec_rt_t", 1).expect("select");
        let back: Decimal = result.get(0, 0).expect("typed get Decimal");
        eprintln!("decimal lossless: in={dec} out={back}");
        assert_eq!(back, dec, "Decimal must round-trip exactly through NUMBER");
        assert_eq!(back.to_string(), text, "all 28 digits preserved");

        // And the canonical NUMBER text (synthesized from the inline form via
        // the shared formatter) is byte-exact.
        if let Some(cell @ QueryValue::Number(_)) = result.cell(0, 0) {
            assert_eq!(
                cell.as_number_text().unwrap(),
                text,
                "canonical NUMBER text is byte-exact"
            );
        } else {
            panic!("expected a NUMBER cell");
        }

        let _ = execute_raw(conn, "drop table dec_rt_t", 1);
    });
}

/// chrono NaiveDate / NaiveDateTime bind + extract against a real DATE column.
#[cfg(feature = "chrono")]
#[test]
fn chrono_roundtrip_live() {
    use chrono::{NaiveDate, NaiveDateTime};

    with_connection("chrono_roundtrip_live", |conn| {
        let _ = execute_raw(conn, "drop table chrono_rt_t", 1);
        execute_raw(conn, "create table chrono_rt_t (d date)", 1).expect("create chrono table");

        let dt = NaiveDate::from_ymd_opt(2026, 6, 14)
            .unwrap()
            .and_hms_opt(13, 45, 30)
            .unwrap();
        BlockingConnection::execute(conn, "insert into chrono_rt_t values (:1)", (dt,))
            .expect("insert datetime");
        execute_raw(conn, "commit", 1).expect("commit");

        let result = execute_raw(conn, "select d from chrono_rt_t", 1).expect("select date");
        let back: NaiveDateTime = result.get(0, 0).expect("typed get NaiveDateTime");
        eprintln!("chrono roundtrip: in={dt} out={back}");
        assert_eq!(back, dt, "DATE must round-trip to the second");
        // and as a bare date
        let date: NaiveDate = result.get(0, 0).expect("typed get NaiveDate");
        assert_eq!(date, NaiveDate::from_ymd_opt(2026, 6, 14).unwrap());

        let _ = execute_raw(conn, "drop table chrono_rt_t", 1);
    });
}

/// uuid bind as RAW(16) + extract back.
#[cfg(feature = "uuid")]
#[test]
fn uuid_roundtrip_live() {
    use uuid::Uuid;

    with_connection("uuid_roundtrip_live", |conn| {
        let _ = execute_raw(conn, "drop table uuid_rt_t", 1);
        execute_raw(conn, "create table uuid_rt_t (id raw(16))", 1).expect("create uuid table");

        let id = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        BlockingConnection::execute(conn, "insert into uuid_rt_t values (:1)", (id,))
            .expect("insert uuid");
        execute_raw(conn, "commit", 1).expect("commit");

        let result = execute_raw(conn, "select id from uuid_rt_t", 1).expect("select uuid");
        let back: Uuid = result.get(0, 0).expect("typed get Uuid");
        eprintln!("uuid roundtrip: in={id} out={back}");
        assert_eq!(back, id, "RAW(16) must round-trip the UUID");

        let _ = execute_raw(conn, "drop table uuid_rt_t", 1);
    });
}

/// serde_json::Value extracted from a native JSON column (the eager OSON tree
/// converts near-free).
#[cfg(feature = "serde_json")]
#[test]
fn serde_json_from_native_json_live() {
    use serde_json::json;

    with_connection("serde_json_from_native_json_live", |conn| {
        let _ = execute_raw(conn, "drop table json_rt_t", 1);
        // 23ai native JSON type
        if execute_raw(conn, "create table json_rt_t (doc json)", 1).is_err() {
            eprintln!("skipped serde_json test: native JSON type unavailable");
            return;
        }

        execute_raw(
            conn,
            "insert into json_rt_t values (json('{\"id\": 7, \"name\": \"bob\", \"tags\": [\"a\", \"b\"]}'))",
            1,
        )
        .expect("insert json");
        execute_raw(conn, "commit", 1).expect("commit");

        // JSON streams through a client-side define; use explicit collect.
        let result =
            execute_raw_collect(conn, "select doc from json_rt_t", 1).expect("select json");
        let value: serde_json::Value = result.get(0, 0).expect("typed get serde_json::Value");
        eprintln!("serde_json from native JSON: {value}");
        assert_eq!(value["id"], json!(7));
        assert_eq!(value["name"], json!("bob"));
        assert_eq!(value["tags"], json!(["a", "b"]));

        let _ = execute_raw(conn, "drop table json_rt_t", 1);
    });
}

/// Vec<f32> extracted from a VECTOR column, and bound back via ToSql.
#[test]
fn vector_roundtrip_live() {
    with_connection("vector_roundtrip_live", |conn| {
        let _ = execute_raw(conn, "drop table vec_rt_t", 1);
        if execute_raw(
            conn,
            "create table vec_rt_t (embedding vector(3, float32))",
            1,
        )
        .is_err()
        {
            eprintln!("skipped vector test: VECTOR type unavailable");
            return;
        }

        let embedding: Vec<f32> = vec![1.5, -2.0, 3.25];
        BlockingConnection::execute(
            conn,
            "insert into vec_rt_t values (:1)",
            (embedding.clone(),),
        )
        .expect("insert vector");
        execute_raw(conn, "commit", 1).expect("commit");

        // VECTOR streams through a client-side define; use explicit collect.
        let result =
            execute_raw_collect(conn, "select embedding from vec_rt_t", 1).expect("select vector");
        let back: Vec<f32> = result.get(0, 0).expect("typed get Vec<f32>");
        eprintln!("vector roundtrip: in={embedding:?} out={back:?}");
        assert_eq!(back, embedding, "VECTOR(float32) must round-trip exactly");

        let _ = execute_raw(conn, "drop table vec_rt_t", 1);
    });
}

fn query_one_with_binds(conn: &mut Connection, sql: &str, binds: Vec<BindValue>) -> Row {
    try_query_one_with_binds(conn, sql, binds)
        .unwrap_or_else(|err| panic!("query one with explicit binds failed for {sql}: {err}"))
}

fn try_query_one_with_binds(
    conn: &mut Connection,
    sql: &str,
    binds: Vec<BindValue>,
) -> Result<Row, Error> {
    BlockingConnection::query_with(conn, Query::new(sql).bind(binds))?.one()
}

fn query_all_one_with_binds(conn: &mut Connection, sql: &str, binds: Vec<BindValue>) -> Row {
    let rows = BlockingConnection::query_with(conn, Query::new(sql).bind(binds))
        .unwrap_or_else(|err| panic!("query with explicit binds failed for {sql}: {err}"))
        .collect()
        .unwrap_or_else(|err| panic!("collect explicit-bind query failed for {sql}: {err}"));
    assert_eq!(rows.len(), 1, "expected one row for {sql}");
    rows.into_iter().next().expect("one collected row")
}

fn execute_with_binds(conn: &mut Connection, sql: &str, binds: Vec<BindValue>) {
    BlockingConnection::execute_with(conn, Execute::new(sql).bind(binds))
        .expect("execute with explicit binds");
}

fn drop_live_table(conn: &mut Connection, name: &str) {
    let sql = format!("drop table {name} purge");
    let _ = BlockingConnection::execute(conn, &sql, ());
}

fn create_optional_table(conn: &mut Connection, name: &str, ddl: &str, label: &str) -> bool {
    drop_live_table(conn, name);
    match BlockingConnection::execute(conn, ddl, ()) {
        Ok(_) => {
            drop_live_table(conn, name);
            true
        }
        Err(err) => {
            eprintln!("skipped {label}: database type unavailable: {err}");
            false
        }
    }
}

fn typed_null(ora_type_num: u8, csfrm: u8, buffer_size: u32) -> BindValue {
    BindValue::TypedNull {
        ora_type_num,
        csfrm,
        buffer_size,
    }
}

fn assert_typed_null(conn: &mut Connection, label: &str, bind: BindValue) {
    let row = query_one_with_binds(conn, "select :1 as v from dual", vec![bind]);
    assert!(
        row.value(0).is_none(),
        "{label} NULL should fetch as None, got {:?}",
        row.value(0)
    );
}

fn assert_cast_null(conn: &mut Connection, label: &str, sql_type: &str) {
    let sql = format!("select cast(null as {sql_type}) as v from dual");
    let row = BlockingConnection::query_one(conn, &sql, ()).expect("select cast null");
    assert!(
        row.value(0).is_none(),
        "{label} NULL should fetch as None, got {:?}",
        row.value(0)
    );
}

fn cell<'a>(row: &'a Row, label: &str) -> &'a QueryValue {
    row.value(0)
        .unwrap_or_else(|| panic!("{label} should not be NULL"))
}

fn assert_number_text(row: &Row, label: &str, expected: &str) {
    let text = cell(row, label)
        .as_number_text()
        .unwrap_or_else(|| panic!("{label} should fetch as NUMBER"));
    assert_eq!(text.as_ref(), expected, "{label} NUMBER text mismatch");
}

fn assert_text(row: &Row, label: &str, expected: &str) {
    assert_eq!(
        row.get::<String>(0)
            .unwrap_or_else(|err| panic!("{label} should fetch as String: {err}")),
        expected,
        "{label} text mismatch"
    );
}

fn assert_raw(row: &Row, label: &str, expected: &[u8]) {
    assert_eq!(
        row.get::<Vec<u8>>(0)
            .unwrap_or_else(|err| panic!("{label} should fetch as Vec<u8>: {err}")),
        expected,
        "{label} bytes mismatch"
    );
}

fn read_lob_bytes(conn: &mut Connection, row: &Row, label: &str) -> (u8, Vec<u8>, Vec<u8>) {
    match cell(row, label) {
        QueryValue::Lob(lob) => {
            let amount = lob.size.max(1);
            let read = BlockingConnection::read_lob(conn, &lob.locator, 1, amount)
                .unwrap_or_else(|err| panic!("{label} LOB read failed: {err}"));
            (
                lob.csfrm,
                lob.locator.clone(),
                read.data
                    .unwrap_or_else(|| panic!("{label} LOB read returned no data")),
            )
        }
        other => panic!("{label} should fetch as LOB locator, got {other:?}"),
    }
}

fn assert_lob_text(conn: &mut Connection, row: &Row, label: &str, expected: &str) {
    match cell(row, label) {
        QueryValue::Text(text) => assert_eq!(text, expected, "{label} text mismatch"),
        QueryValue::Lob(_) => {
            let (csfrm, locator, data) = read_lob_bytes(conn, row, label);
            let text = decode_lob_text(&data, csfrm, Some(&locator))
                .unwrap_or_else(|err| panic!("{label} LOB text decode failed: {err}"));
            assert_eq!(text, expected, "{label} streamed text mismatch");
        }
        other => panic!("{label} should fetch as Text or LOB locator, got {other:?}"),
    }
}

fn assert_lob_raw(conn: &mut Connection, row: &Row, label: &str, expected: &[u8]) {
    match cell(row, label) {
        QueryValue::Raw(bytes) => assert_eq!(bytes.as_slice(), expected, "{label} bytes mismatch"),
        QueryValue::Lob(_) => {
            let (_, _, data) = read_lob_bytes(conn, row, label);
            assert_eq!(data, expected, "{label} streamed bytes mismatch");
        }
        other => panic!("{label} should fetch as Raw or LOB locator, got {other:?}"),
    }
}

fn assert_datetime(row: &Row, label: &str, expected: (i32, u8, u8, u8, u8, u8, u32)) {
    match cell(row, label) {
        QueryValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
        } => assert_eq!(
            (*year, *month, *day, *hour, *minute, *second, *nanosecond),
            expected,
            "{label} datetime mismatch"
        ),
        other => panic!("{label} should fetch as DateTime, got {other:?}"),
    }
}

fn assert_timestamp_tz(row: &Row, label: &str, expected: (i32, u8, u8, u8, u8, u8, u32, i32)) {
    let value = cell(row, label);
    match value {
        QueryValue::TimestampTz {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
            offset_minutes,
        } => assert_eq!(
            (
                *year,
                *month,
                *day,
                *hour,
                *minute,
                *second,
                *nanosecond,
                *offset_minutes,
            ),
            expected,
            "{label} timestamp with time zone mismatch"
        ),
        _ => assert!(
            matches!(value, QueryValue::TimestampTz { .. }),
            "{label} should fetch as TimestampTz, got {value:?}"
        ),
    }
}

fn assert_binary_double_bits(row: &Row, label: &str, expected: f64) {
    let actual = row
        .get::<f64>(0)
        .unwrap_or_else(|err| panic!("{label} should fetch as f64: {err}"));
    assert_eq!(
        actual.to_bits(),
        expected.to_bits(),
        "{label} f64 bits mismatch"
    );
}

fn assert_binary_float_bits(row: &Row, label: &str, expected: f32) {
    let actual = row
        .get::<f32>(0)
        .unwrap_or_else(|err| panic!("{label} should fetch as f32: {err}"));
    assert_eq!(
        actual.to_bits(),
        expected.to_bits(),
        "{label} f32 bits mismatch"
    );
}

fn assert_vector_f32(row: &Row, expected: &[f32]) {
    let typed = row
        .get::<Vec<f32>>(0)
        .expect("VECTOR should fetch through typed Vec<f32>");
    assert_eq!(
        typed
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>(),
        expected
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>(),
        "typed VECTOR(float32) bits mismatch"
    );

    match cell(row, "VECTOR") {
        QueryValue::Vector(vector) => match vector.as_ref() {
            Vector::Dense(VectorValues::Float32(values)) => assert_eq!(
                values
                    .iter()
                    .map(|value| value.to_bits())
                    .collect::<Vec<_>>(),
                expected
                    .iter()
                    .map(|value| value.to_bits())
                    .collect::<Vec<_>>(),
                "raw VECTOR(float32) bits mismatch"
            ),
            other => panic!("VECTOR should fetch as dense float32, got {other:?}"),
        },
        other => panic!("VECTOR should fetch as QueryValue::Vector, got {other:?}"),
    }
}

fn json_field<'a>(entries: &'a [(String, OsonValue)], name: &str) -> &'a OsonValue {
    entries
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value)
        .unwrap_or_else(|| panic!("JSON object should contain field {name:?}"))
}

fn exercise_number_matrix(conn: &mut Connection) {
    for (label, text) in [
        ("NUMBER zero", "0"),
        ("NUMBER negative", "-42"),
        ("NUMBER decimal", "7922816251426433759354.395033"),
    ] {
        let row = query_one_with_binds(
            conn,
            "select :1 as v from dual",
            vec![BindValue::Number(text.to_string())],
        );
        assert_number_text(&row, label, text);
    }

    let large_i128_text = "99999999999999999999999999999999999999";
    let large_i128 = large_i128_text
        .parse::<i128>()
        .expect("large NUMBER fixture fits i128");
    let row = query_one_with_binds(
        conn,
        "select :1 as v from dual",
        vec![BindValue::Number(large_i128_text.to_string())],
    );
    assert_eq!(
        row.get::<i128>(0).expect("NUMBER should fetch as i128"),
        large_i128,
        "large i128 NUMBER mismatch"
    );
    assert_number_text(&row, "NUMBER large i128", large_i128_text);

    for (label, value) in [
        ("NUMBER rejects NaN", f64::NAN),
        ("NUMBER rejects +Inf", f64::INFINITY),
        ("NUMBER rejects -Inf", f64::NEG_INFINITY),
    ] {
        let result = try_query_one_with_binds(
            conn,
            "select cast(:1 as number) as v from dual",
            vec![BindValue::BinaryDouble(value)],
        );
        assert!(
            result.is_err(),
            "{label}: non-finite BINARY_DOUBLE should not cast to NUMBER"
        );
    }

    assert_typed_null(conn, "NUMBER", typed_null(ORA_TYPE_NUM_NUMBER, 0, 22));
}

fn exercise_binary_float_matrix(conn: &mut Connection) {
    let row = query_one_with_binds(
        conn,
        "select cast(:1 as binary_float) as v from dual",
        vec![BindValue::BinaryFloat(f64::from(-0.0_f32))],
    );
    assert_binary_float_bits(&row, "BINARY_FLOAT negative zero", -0.0);

    let row = query_one_with_binds(
        conn,
        "select cast(:1 as binary_float) as v from dual",
        vec![BindValue::BinaryFloat(f64::from(3.25_f32))],
    );
    assert_binary_float_bits(&row, "BINARY_FLOAT finite", 3.25);

    let row = query_one_with_binds(
        conn,
        "select cast(:1 as binary_double) as v from dual",
        vec![BindValue::BinaryDouble(-42.5)],
    );
    assert_binary_double_bits(&row, "BINARY_DOUBLE finite", -42.5);

    let row = query_one_with_binds(
        conn,
        "select cast(:1 as binary_double) as v from dual",
        vec![BindValue::BinaryDouble(-0.0)],
    );
    assert_binary_double_bits(&row, "BINARY_DOUBLE negative zero", -0.0);

    assert_typed_null(
        conn,
        "BINARY_FLOAT",
        typed_null(ORA_TYPE_NUM_BINARY_FLOAT, 0, 4),
    );
    assert_typed_null(
        conn,
        "BINARY_DOUBLE",
        typed_null(ORA_TYPE_NUM_BINARY_DOUBLE, 0, 8),
    );
}

fn exercise_character_matrix(conn: &mut Connection) {
    let row = query_one_with_binds(
        conn,
        "select cast(:1 as varchar2(80 char)) as v from dual",
        vec![BindValue::Text("hello varchar2".to_string())],
    );
    assert_text(&row, "VARCHAR2", "hello varchar2");

    let ntext = "nchar-\u{03b4}-\u{0444}";
    let row = query_one_with_binds(
        conn,
        "select cast(:1 as nvarchar2(80)) as v from dual",
        vec![BindValue::Text(ntext.to_string())],
    );
    assert_text(&row, "NVARCHAR2", ntext);

    let row = query_one_with_binds(
        conn,
        "select cast(:1 as char(6)) as v from dual",
        vec![BindValue::Text("xy".to_string())],
    );
    // Oracle CHAR is blank-padded to the declared width; assert the documented
    // server-side asymmetry rather than trimming silently.
    assert_text(&row, "CHAR padded", "xy    ");

    assert_typed_null(
        conn,
        "VARCHAR2",
        typed_null(ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT, 80),
    );
    assert_typed_null(
        conn,
        "NVARCHAR2",
        typed_null(ORA_TYPE_NUM_VARCHAR, CS_FORM_NCHAR, 80),
    );
    assert_typed_null(
        conn,
        "CHAR",
        typed_null(ORA_TYPE_NUM_CHAR, CS_FORM_IMPLICIT, 6),
    );
}

fn exercise_lob_and_raw_matrix(conn: &mut Connection) {
    let clob_text = "clob round trip\nsecond line";
    let row = query_one_with_binds(
        conn,
        "select to_clob(:1) as v from dual",
        vec![BindValue::Text(clob_text.to_string())],
    );
    assert_lob_text(conn, &row, "CLOB", clob_text);

    let nclob_text = "nclob-\u{05d0}-\u{03a9}";
    let row = query_one_with_binds(
        conn,
        "select to_nclob(:1) as v from dual",
        vec![BindValue::Text(nclob_text.to_string())],
    );
    assert_lob_text(conn, &row, "NCLOB", nclob_text);

    let bytes = vec![0, 1, 2, 0x7f, 0x80, 0xfe, 0xff];
    let row = query_one_with_binds(
        conn,
        "select cast(:1 as raw(16)) as v from dual",
        vec![BindValue::Raw(bytes.clone())],
    );
    assert_raw(&row, "RAW", &bytes);

    let row = query_one_with_binds(
        conn,
        "select to_blob(:1) as v from dual",
        vec![BindValue::Raw(bytes.clone())],
    );
    assert_lob_raw(conn, &row, "BLOB", &bytes);

    assert_typed_null(
        conn,
        "CLOB",
        typed_null(ORA_TYPE_NUM_CLOB, CS_FORM_IMPLICIT, 4096),
    );
    assert_typed_null(
        conn,
        "NCLOB",
        typed_null(ORA_TYPE_NUM_CLOB, CS_FORM_NCHAR, 4096),
    );
    assert_typed_null(conn, "BLOB", typed_null(ORA_TYPE_NUM_BLOB, 0, 4096));
    assert_typed_null(conn, "RAW", typed_null(ORA_TYPE_NUM_RAW, 0, 16));
}

fn exercise_long_matrix(conn: &mut Connection) {
    drop_live_table(conn, "rust_e12_long_t");
    BlockingConnection::execute(
        conn,
        "create table rust_e12_long_t (id number primary key, v long)",
        (),
    )
    .expect("create LONG table");

    let text = "long text through table";
    execute_with_binds(
        conn,
        "insert into rust_e12_long_t (id, v) values (:1, :2)",
        vec![
            BindValue::Number("1".to_string()),
            BindValue::Text(text.to_string()),
        ],
    );
    let row = query_all_one_with_binds(
        conn,
        "select v from rust_e12_long_t where id = :1",
        vec![BindValue::Number("1".to_string())],
    );
    assert_text(&row, "LONG", text);
    // Regression for rust-oracledb-hzz2: query_one over a single-row LONG result
    // must return the row, not Error::TooManyRows. The per-row LONG define-fetch
    // leaves `more_rows` set on a single row; query_one now fetches ahead to
    // confirm cardinality before reporting TooManyRows.
    let one_row = query_one_with_binds(
        conn,
        "select v from rust_e12_long_t where id = :1",
        vec![BindValue::Number("1".to_string())],
    );
    assert_text(&one_row, "LONG via query_one", text);
    assert_typed_null(
        conn,
        "LONG",
        typed_null(ORA_TYPE_NUM_LONG, CS_FORM_IMPLICIT, 4096),
    );
    drop_live_table(conn, "rust_e12_long_t");

    drop_live_table(conn, "rust_e12_long_raw_t");
    BlockingConnection::execute(
        conn,
        "create table rust_e12_long_raw_t (id number primary key, v long raw)",
        (),
    )
    .expect("create LONG RAW table");
    execute_with_binds(
        conn,
        "insert into rust_e12_long_raw_t (id, v) values (:1, hextoraw(:2))",
        vec![
            BindValue::Number("1".to_string()),
            BindValue::Text("0001027F80FEFF".to_string()),
        ],
    );
    let row = query_all_one_with_binds(
        conn,
        "select v from rust_e12_long_raw_t where id = :1",
        vec![BindValue::Number("1".to_string())],
    );
    assert_raw(&row, "LONG RAW", &[0, 1, 2, 0x7f, 0x80, 0xfe, 0xff]);
    // Regression for rust-oracledb-hzz2 (LONG RAW variant).
    let one_row = query_one_with_binds(
        conn,
        "select v from rust_e12_long_raw_t where id = :1",
        vec![BindValue::Number("1".to_string())],
    );
    assert_raw(
        &one_row,
        "LONG RAW via query_one",
        &[0, 1, 2, 0x7f, 0x80, 0xfe, 0xff],
    );
    assert_typed_null(conn, "LONG RAW", typed_null(ORA_TYPE_NUM_LONG_RAW, 0, 4096));
    drop_live_table(conn, "rust_e12_long_raw_t");
}

fn exercise_datetime_interval_matrix(conn: &mut Connection) {
    BlockingConnection::execute(conn, "alter session set time_zone = '+00:00'", ())
        .expect("set deterministic session time zone");

    let date_bind = BindValue::DateTime {
        year: 2026,
        month: 6,
        day: 21,
        hour: 13,
        minute: 45,
        second: 30,
    };
    let row = query_one_with_binds(
        conn,
        "select cast(:1 as date) as v from dual",
        vec![date_bind],
    );
    assert_datetime(&row, "DATE", (2026, 6, 21, 13, 45, 30, 0));

    let timestamp_bind = BindValue::Timestamp {
        ora_type_num: ORA_TYPE_NUM_TIMESTAMP,
        year: 2026,
        month: 6,
        day: 21,
        hour: 14,
        minute: 5,
        second: 6,
        nanosecond: 987_654_321,
    };
    let row = query_one_with_binds(
        conn,
        "select cast(:1 as timestamp(9)) as v from dual",
        vec![timestamp_bind],
    );
    assert_datetime(&row, "TIMESTAMP", (2026, 6, 21, 14, 5, 6, 987_654_321));

    let timestamp_tz_bind = BindValue::Timestamp {
        ora_type_num: ORA_TYPE_NUM_TIMESTAMP_TZ,
        year: 2026,
        month: 6,
        day: 21,
        hour: 15,
        minute: 6,
        second: 7,
        nanosecond: 123_456_789,
    };
    let row = query_one_with_binds(
        conn,
        "select cast(:1 as timestamp(9) with time zone) as v from dual",
        vec![timestamp_tz_bind],
    );
    // The legacy raw Timestamp bind still supplies a +00:00 offset; fetch now
    // preserves that explicit fixed offset as QueryValue::TimestampTz.
    assert_timestamp_tz(
        &row,
        "TIMESTAMP WITH TIME ZONE",
        (2026, 6, 21, 15, 6, 7, 123_456_789, 0),
    );

    let timestamp_ltz_bind = BindValue::Timestamp {
        ora_type_num: ORA_TYPE_NUM_TIMESTAMP_LTZ,
        year: 2026,
        month: 6,
        day: 21,
        hour: 16,
        minute: 7,
        second: 8,
        nanosecond: 555_000_111,
    };
    let row = query_one_with_binds(
        conn,
        "select cast(:1 as timestamp(9) with local time zone) as v from dual",
        vec![timestamp_ltz_bind],
    );
    // With the session time zone fixed at +00:00, TIMESTAMP WITH LOCAL TIME ZONE
    // comes back in that session-local wall clock.
    assert_datetime(
        &row,
        "TIMESTAMP WITH LOCAL TIME ZONE",
        (2026, 6, 21, 16, 7, 8, 555_000_111),
    );

    let row = query_one_with_binds(
        conn,
        "select :1 as v from dual",
        vec![BindValue::IntervalYM {
            years: -2,
            months: 7,
        }],
    );
    assert_eq!(
        cell(&row, "INTERVAL YEAR TO MONTH"),
        &QueryValue::IntervalYM {
            years: -2,
            months: 7,
        },
        "INTERVAL YEAR TO MONTH mismatch"
    );

    let row = query_one_with_binds(
        conn,
        "select :1 as v from dual",
        vec![BindValue::IntervalDS {
            days: 3,
            seconds: 4 * 3600 + 5 * 60 + 6,
            microseconds: 456_789,
        }],
    );
    assert_eq!(
        cell(&row, "INTERVAL DAY TO SECOND"),
        &QueryValue::IntervalDS {
            days: 3,
            hours: 4,
            minutes: 5,
            seconds: 6,
            fseconds: 456_789_000,
        },
        "INTERVAL DAY TO SECOND mismatch"
    );

    assert_typed_null(conn, "DATE", typed_null(ORA_TYPE_NUM_DATE, 0, 7));
    assert_typed_null(conn, "TIMESTAMP", typed_null(ORA_TYPE_NUM_TIMESTAMP, 0, 11));
    assert_typed_null(
        conn,
        "TIMESTAMP WITH TIME ZONE",
        typed_null(ORA_TYPE_NUM_TIMESTAMP_TZ, 0, 13),
    );
    assert_typed_null(
        conn,
        "TIMESTAMP WITH LOCAL TIME ZONE",
        typed_null(ORA_TYPE_NUM_TIMESTAMP_LTZ, 0, 11),
    );
    assert_typed_null(
        conn,
        "INTERVAL YEAR TO MONTH",
        typed_null(ORA_TYPE_NUM_INTERVAL_YM, 0, 5),
    );
    assert_typed_null(
        conn,
        "INTERVAL DAY TO SECOND",
        typed_null(ORA_TYPE_NUM_INTERVAL_DS, 0, 11),
    );
}

fn exercise_boolean_matrix(conn: &mut Connection) {
    if !create_optional_table(
        conn,
        "rust_e12_boolean_probe_t",
        "create table rust_e12_boolean_probe_t (v boolean)",
        "BOOLEAN",
    ) {
        return;
    }

    let row = query_one_with_binds(
        conn,
        "select :1 as v from dual",
        vec![BindValue::Boolean(true)],
    );
    assert_eq!(
        row.get::<bool>(0).expect("BOOLEAN should fetch as bool"),
        true,
        "BOOLEAN true mismatch"
    );
    assert_typed_null(conn, "BOOLEAN", typed_null(ORA_TYPE_NUM_BOOLEAN, 0, 4));
}

fn exercise_json_matrix(conn: &mut Connection) {
    if !create_optional_table(
        conn,
        "rust_e12_json_probe_t",
        "create table rust_e12_json_probe_t (doc json)",
        "JSON",
    ) {
        return;
    }

    drop_live_table(conn, "rust_e12_json_t");
    BlockingConnection::execute(
        conn,
        "create table rust_e12_json_t (id number primary key, doc json)",
        (),
    )
    .expect("create JSON table");

    execute_with_binds(
        conn,
        "insert into rust_e12_json_t (id, doc) values (:1, json(:2))",
        vec![
            BindValue::Number("1".to_string()),
            BindValue::Text(r#"{"id":7,"name":"bob","tags":["a","b"]}"#.to_string()),
        ],
    );
    execute_with_binds(
        conn,
        "insert into rust_e12_json_t (id, doc) values (:1, :2)",
        vec![
            BindValue::Number("2".to_string()),
            typed_null(ORA_TYPE_NUM_JSON, 0, 1_048_576),
        ],
    );

    let row = query_one_with_binds(
        conn,
        "select doc from rust_e12_json_t where id = :1",
        vec![BindValue::Number("1".to_string())],
    );
    match cell(&row, "JSON") {
        QueryValue::Json(value) => match value.as_ref() {
            OsonValue::Object(entries) => {
                assert_eq!(
                    json_field(entries, "id"),
                    &OsonValue::Number("7".to_string())
                );
                assert_eq!(
                    json_field(entries, "name"),
                    &OsonValue::String("bob".to_string())
                );
                assert_eq!(
                    json_field(entries, "tags"),
                    &OsonValue::Array(vec![
                        OsonValue::String("a".to_string()),
                        OsonValue::String("b".to_string()),
                    ])
                );
            }
            other => panic!("JSON should fetch as OSON object, got {other:?}"),
        },
        other => panic!("JSON should fetch as QueryValue::Json, got {other:?}"),
    }

    let null_row = query_one_with_binds(
        conn,
        "select doc from rust_e12_json_t where id = :1",
        vec![BindValue::Number("2".to_string())],
    );
    assert!(
        null_row.value(0).is_none(),
        "JSON NULL should fetch as None, got {:?}",
        null_row.value(0)
    );

    drop_live_table(conn, "rust_e12_json_t");
}

fn exercise_vector_matrix(conn: &mut Connection) {
    if !create_optional_table(
        conn,
        "rust_e12_vector_probe_t",
        "create table rust_e12_vector_probe_t (v vector(3, float32))",
        "VECTOR",
    ) {
        return;
    }

    drop_live_table(conn, "rust_e12_vector_t");
    BlockingConnection::execute(
        conn,
        "create table rust_e12_vector_t (id number primary key, v vector(3, float32))",
        (),
    )
    .expect("create VECTOR table");

    let embedding = vec![1.5_f32, -2.0, 3.25];
    BlockingConnection::execute(
        conn,
        "insert into rust_e12_vector_t (id, v) values (:1, :2)",
        (1_i64, embedding.clone()),
    )
    .expect("insert VECTOR");
    execute_with_binds(
        conn,
        "insert into rust_e12_vector_t (id, v) values (:1, :2)",
        vec![
            BindValue::Number("2".to_string()),
            typed_null(ORA_TYPE_NUM_VECTOR, 0, 1_048_576),
        ],
    );

    let row = query_one_with_binds(
        conn,
        "select v from rust_e12_vector_t where id = :1",
        vec![BindValue::Number("1".to_string())],
    );
    assert_vector_f32(&row, &embedding);

    let null_row = query_one_with_binds(
        conn,
        "select v from rust_e12_vector_t where id = :1",
        vec![BindValue::Number("2".to_string())],
    );
    assert!(
        null_row.value(0).is_none(),
        "VECTOR NULL should fetch as None, got {:?}",
        null_row.value(0)
    );

    drop_live_table(conn, "rust_e12_vector_t");
}

fn exercise_rowid_matrix(conn: &mut Connection) {
    drop_live_table(conn, "rust_e12_rowid_t");
    BlockingConnection::execute(
        conn,
        "create table rust_e12_rowid_t (id number primary key, txt varchar2(20))",
        (),
    )
    .expect("create ROWID table");
    BlockingConnection::execute(
        conn,
        "insert into rust_e12_rowid_t (id, txt) values (:1, :2)",
        (1_i64, "rowid"),
    )
    .expect("insert ROWID fixture");

    let source = BlockingConnection::query_one(
        conn,
        "select rowid from rust_e12_rowid_t where id = :1",
        (1_i64,),
    )
    .expect("select source ROWID");
    let rowid = match cell(&source, "ROWID source") {
        QueryValue::Rowid(rowid) => rowid.clone(),
        other => panic!("ROWID source should fetch as Rowid, got {other:?}"),
    };
    assert!(!rowid.is_empty(), "ROWID should not be empty");

    let echoed = query_one_with_binds(
        conn,
        "select rowid from rust_e12_rowid_t where rowid = chartorowid(:1)",
        vec![BindValue::Text(rowid.clone())],
    );
    assert_eq!(
        echoed
            .get::<String>(0)
            .expect("ROWID should fetch as String"),
        rowid,
        "ROWID string mismatch"
    );
    assert_eq!(
        cell(&echoed, "ROWID"),
        &QueryValue::Rowid(rowid),
        "ROWID raw value mismatch"
    );
    assert_cast_null(conn, "ROWID", "rowid");
    assert_typed_null(
        conn,
        "ROWID typed bind",
        typed_null(ORA_TYPE_NUM_ROWID, 0, 18),
    );
    drop_live_table(conn, "rust_e12_rowid_t");
}

/// Wave-3 E1.2: live typed round-trip matrix against a real Oracle database.
///
/// Ignored by default so the normal test suite remains offline. Run with:
/// `eval "$(scripts/container.sh env)" cargo test -p oracledb --test live_typed -- --ignored --nocapture`.
#[test]
#[ignore]
fn live_typed_roundtrip_matrix_e12() {
    with_connection("live_typed_roundtrip_matrix_e12", |conn| {
        exercise_number_matrix(conn);
        exercise_binary_float_matrix(conn);
        exercise_character_matrix(conn);
        exercise_lob_and_raw_matrix(conn);
        exercise_long_matrix(conn);
        exercise_datetime_interval_matrix(conn);
        exercise_boolean_matrix(conn);
        exercise_json_matrix(conn);
        exercise_vector_matrix(conn);
        exercise_rowid_matrix(conn);

        eprintln!(
            "covered live typed matrix: NUMBER, BINARY_FLOAT, BINARY_DOUBLE, VARCHAR2, NVARCHAR2, CHAR, CLOB, NCLOB, BLOB, RAW, LONG, LONG RAW, DATE, TIMESTAMP, TIMESTAMP WITH TIME ZONE, TIMESTAMP WITH LOCAL TIME ZONE, INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND, BOOLEAN when available, JSON when available, VECTOR when available, ROWID, and NULL checks"
        );
    });
}
