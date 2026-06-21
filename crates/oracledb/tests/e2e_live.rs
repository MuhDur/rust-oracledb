#![forbid(unsafe_code)]

//! Driver-native, operator-readable live E2E scenarios for the public
//! `oracledb` API.
//!
//! Self-skips when the live container environment is absent. Run with:
//!
//! ```sh
//! eval "$(scripts/container.sh env)"
//! cargo test -p oracledb --test e2e_live -- --ignored --nocapture
//! ```

use std::fmt::Write as _;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use asupersync::runtime::{reactor, Runtime, RuntimeBuilder};
use asupersync::Cx;
use oracledb::pool::{
    AcquireOptions, BlockingPool, Pool, PoolBackend, PoolConfig, PoolError, PoolStats,
    POOL_GETMODE_TIMEDWAIT, POOL_GETMODE_WAIT,
};
use oracledb::protocol::thin::{
    decode_lob_text, encode_lob_text, BindValue, QueryValue, CS_FORM_IMPLICIT, ORA_TYPE_NUM_CLOB,
    ORA_TYPE_NUM_VARCHAR, SUBSCR_QOS_QUERY, TNS_SUBSCR_NAMESPACE_DBCHANGE,
};
use oracledb::protocol::ClientIdentity;
use oracledb::{
    Batch, BlockingConnection, ConnectOptions, Connection, Error, Execute, Query, Registration, Row,
};

const PROGRAM: &str = "rust-oracledb-e2e";
const MACHINE: &str = "e2e-machine";
const OSUSER: &str = "e2e-osuser";
const TERMINAL: &str = "e2e-terminal";
const DRIVER: &str = "rust-oracledb thn : 0.0.0";

const RUN: &str = "cargo test -p oracledb --test e2e_live -- --ignored --nocapture";

#[derive(Clone)]
struct E2eLog {
    scenario: &'static str,
    start: Instant,
}

impl E2eLog {
    fn new(scenario: &'static str) -> Self {
        Self {
            scenario,
            start: Instant::now(),
        }
    }

    fn step(&self, phase: &str, detail: &str) {
        eprintln!(
            "[e2e] scenario={} phase={} elapsed={}ms detail={}",
            self.scenario,
            phase,
            self.start.elapsed().as_millis(),
            detail
        );
    }

    fn skip(&self, scenario: &str, reason: &str) {
        eprintln!(
            "[e2e] SKIP {scenario}: {reason} elapsed={}ms",
            self.start.elapsed().as_millis()
        );
    }

    fn ok(&self) {
        eprintln!("[e2e] SCENARIO {} OK", self.scenario);
    }
}

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

fn tcps_connect_options() -> Option<ConnectOptions> {
    let connect_string = env_first(&[
        "ORACLEDB_E2E_TCPS_CONNECT_STRING",
        "PYO_TEST_TCPS_CONNECT_STRING",
    ])?;
    let user = env_first(&[
        "ORACLEDB_E2E_TCPS_USER",
        "PYO_TEST_TCPS_MAIN_USER",
        "PYO_TEST_MAIN_USER",
    ])?;
    let password = env_first(&[
        "ORACLEDB_E2E_TCPS_PASSWORD",
        "PYO_TEST_TCPS_MAIN_PASSWORD",
        "PYO_TEST_MAIN_PASSWORD",
    ])?;
    let identity =
        ClientIdentity::new("rust-oracledb-e2e-tcps", MACHINE, OSUSER, TERMINAL, DRIVER).ok()?;
    let mut options = ConnectOptions::new(connect_string, user, password, identity);
    if let Ok(wallet) = std::env::var("ORACLEDB_E2E_TCPS_WALLET_LOCATION") {
        options = options.with_wallet_location(wallet);
    }
    if let Ok(wallet_password) = std::env::var("ORACLEDB_E2E_TCPS_WALLET_PASSWORD") {
        options = options.with_wallet_password(wallet_password);
    }
    if let Ok(dn) = std::env::var("ORACLEDB_E2E_TCPS_SERVER_DN") {
        options = options.with_ssl_server_cert_dn(dn);
    }
    if let Ok(value) = std::env::var("ORACLEDB_E2E_TCPS_DN_MATCH") {
        options = options.with_ssl_server_dn_match(parse_bool(&value));
    }
    if let Ok(value) = std::env::var("ORACLEDB_E2E_TCPS_USE_SNI") {
        options = options.with_use_sni(parse_bool(&value));
    }
    Some(options)
}

fn env_first(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| std::env::var(name).ok())
}

fn parse_bool(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn build_runtime() -> Runtime {
    let reactor = reactor::create_reactor().expect("native reactor should build for live I/O");
    RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("current-thread Asupersync runtime should build")
}

fn with_connection(test: &'static str, body: impl FnOnce(&E2eLog, &mut Connection)) {
    let log = E2eLog::new(test);
    let Some(options) = connect_options() else {
        log.skip(test, "PYO_TEST_* environment not configured");
        return;
    };
    log.step(
        "connect",
        &format!(
            "connect_string={} password=redacted run=\"{}\"",
            options.connect_string(),
            RUN
        ),
    );
    let mut conn = BlockingConnection::connect(options).expect("connect to test container");
    body(&log, &mut conn);
    log.step(
        "logoff",
        &format!(
            "connection_disposition=clean session_id={} dead={}",
            conn.session_id(),
            conn.is_dead()
        ),
    );
    BlockingConnection::close(conn).expect("clean logoff");
    log.ok();
}

fn drop_table_if_exists(conn: &mut Connection, table: &str) {
    let _ = BlockingConnection::execute(conn, &format!("drop table {table} purge"), ());
}

fn row_i64(row: &Row, index: usize) -> i64 {
    row.get::<i64>(index)
        .expect("row column should decode as i64")
}

fn row_text(row: &Row, index: usize) -> String {
    row.get::<String>(index)
        .expect("row column should decode as String")
}

fn value_shape(value: &QueryValue) -> &'static str {
    match value {
        QueryValue::Text(_) => "Text",
        QueryValue::TextRaw { .. } => "TextRaw",
        QueryValue::Raw(_) => "Raw",
        QueryValue::Rowid(_) => "Rowid",
        QueryValue::BinaryDouble(_) => "BinaryDouble",
        QueryValue::IntervalDS { .. } => "IntervalDS",
        QueryValue::IntervalYM { .. } => "IntervalYM",
        QueryValue::Number(_) => "Number",
        QueryValue::Boolean(_) => "Boolean",
        QueryValue::Cursor(_) => "Cursor",
        QueryValue::DateTime { .. } => "DateTime",
        QueryValue::Object(_) => "Object",
        QueryValue::Lob(_) => "Lob",
        QueryValue::Vector(_) => "Vector",
        QueryValue::Json(_) => "Json",
        QueryValue::Array(_) => "Array",
        _ => "Unknown",
    }
}

fn returning_shape(values: &[Option<QueryValue>]) -> String {
    let mut detail = format!("count={}", values.len());
    for (idx, value) in values.iter().enumerate() {
        let shape = value.as_ref().map(value_shape).unwrap_or("Null");
        write!(&mut detail, " idx{idx}={shape}").expect("write shape detail");
    }
    detail
}

fn batch_error_detail(errors: &[oracledb::BatchError]) -> String {
    let mut detail = format!("count={}", errors.len());
    for error in errors {
        write!(
            &mut detail,
            " row_index={} code={} message_len={}",
            error.row_index(),
            error.code(),
            error.message().len()
        )
        .expect("write batch error detail");
    }
    detail
}

#[test]
#[ignore = "requires live Oracle container env from scripts/container.sh"]
fn connect_auth_and_optional_tcps_handshake_are_logged() {
    with_connection("connect_auth", |log, conn| {
        let charset_row =
            BlockingConnection::query_one(conn, "select userenv('language') from dual", ())
                .expect("fetch session charset");
        let charset = row_text(&charset_row, 0);
        let auth_row = BlockingConnection::query_one(
            conn,
            "select sys_context('USERENV','AUTHENTICATED_IDENTITY'), \
                    sys_context('USERENV','SERVICE_NAME') \
             from dual",
            (),
        )
        .expect("fetch auth context");
        log.step(
            "authenticated",
            &format!(
                "server_version={} version_tuple={:?} charset={} session_id={} serial={} \
                 service={} connection_class=direct-none supports_oob={} password=redacted",
                conn.server_version().unwrap_or("unknown"),
                conn.server_version_tuple(),
                charset,
                conn.session_id(),
                conn.serial_num(),
                row_text(&auth_row, 1),
                conn.supports_oob()
            ),
        );
        assert!(conn.session_id() > 0, "session id should be assigned");
        assert!(conn.serial_num() > 0, "serial number should be assigned");
        assert_eq!(conn.identity().program, PROGRAM);
        assert!(!row_text(&auth_row, 0).is_empty(), "authenticated identity");

        match tcps_connect_options() {
            Some(options) => {
                log.step(
                    "tcps_connect",
                    &format!(
                        "connect_string={} password=redacted wallet_configured={} dn_match={}",
                        options.connect_string(),
                        options.wallet_location().is_some(),
                        options.ssl_server_dn_match()
                    ),
                );
                let mut tcps = BlockingConnection::connect(options)
                    .expect("connect to configured TCPS endpoint");
                log.step(
                    "tcps_handshake",
                    &format!(
                        "transport=tcps negotiated_protocol=tcps/tls session_id={} server_version={}",
                        tcps.session_id(),
                        tcps.server_version().unwrap_or("unknown")
                    ),
                );
                let one = BlockingConnection::query_one(&mut tcps, "select 1 from dual", ())
                    .expect("TCPS query after handshake");
                assert_eq!(row_i64(&one, 0), 1);
                BlockingConnection::close(tcps).expect("close TCPS connection");
            }
            None => {
                log.skip(
                    "connect_auth_tls",
                    "ORACLEDB_E2E_TCPS_CONNECT_STRING/PYO_TEST_TCPS_CONNECT_STRING not set",
                );
            }
        }
    });
}

#[test]
#[ignore = "requires live Oracle container env from scripts/container.sh"]
fn query_family_streaming_and_cardinality_are_logged() {
    let log = E2eLog::new("query_family");
    let Some(options) = connect_options() else {
        log.skip("query_family", "PYO_TEST_* environment not configured");
        return;
    };
    let runtime = build_runtime();
    runtime.block_on(async {
        let cx = Cx::current().expect("Runtime::block_on should install an ambient Cx");
        let mut conn = Connection::connect(&cx, options)
            .await
            .expect("connect to test container");
        log.step(
            "connect",
            &format!(
                "session_id={} server_version={} password=redacted",
                conn.session_id(),
                conn.server_version().unwrap_or("unknown")
            ),
        );

        let single = conn
            .query(&cx, "select 42 as n from dual", ())
            .await
            .expect("query single row")
            .one()
            .expect("one row");
        assert_eq!(row_i64(&single, 0), 42);
        log.step(
            "query_single",
            "rows=1 columns=1 cursor_id=not_exposed_by_public_rows_api sql=\"select 42 as n from dual\"",
        );

        {
            let mut rows = conn
                .query_with(
                    &cx,
                    Query::new("select level as n from dual connect by level <= 105")
                        .arraysize(NonZeroU32::new(25).expect("non-zero arraysize"))
                        .prefetch(25)
                        .timeout(Duration::from_secs(10)),
                )
                .await
                .expect("query_with multi-batch");
            let mut total = 0usize;
            let mut batches = 0usize;
            loop {
                let batch_len = rows.batch().len();
                total += batch_len;
                batches += 1;
                log.step(
                    "query_batch",
                    &format!(
                        "batch_index={} rows={} total_rows={} deadline_ms=10000 \
                         cursor_id=not_exposed_by_public_rows_api",
                        batches, batch_len, total
                    ),
                );
                if !rows.next_batch(&cx).await.expect("fetch next batch") {
                    break;
                }
            }
            assert_eq!(total, 105);
            assert!(
                batches >= 2,
                "small arraysize should force multiple next_batch round trips"
            );
        }

        let one = conn
            .query_one(&cx, "select 7 + 5 as n from dual", ())
            .await
            .expect("query_one");
        assert_eq!(row_i64(&one, 0), 12);
        log.step("query_one", "cardinality=one rows=1");

        let none = conn
            .query_opt(&cx, "select 1 as n from dual where 1 = 0", ())
            .await
            .expect("query_opt none");
        assert!(none.is_none(), "query_opt should surface zero rows as None");
        log.step("query_opt_zero", "cardinality=zero rows=0");

        let many = conn
            .query_all(
                &cx,
                "select level as n from dual connect by level <= 18 order by n",
                (),
            )
            .await
            .expect("query_all many rows");
        assert_eq!(many.len(), 18);
        assert_eq!(row_i64(&many[17], 0), 18);
        log.step("query_all_many", "rows=18 first=1 last=18");

        let err = conn
            .query_one(&cx, "select level as n from dual connect by level <= 2", ())
            .await
            .expect_err("query_one should reject many rows");
        assert!(matches!(err, Error::TooManyRows));
        log.step(
            "query_one_many",
            &format!("outcome=TooManyRows connection_disposition=alive dead={}", conn.is_dead()),
        );

        let _ = conn.execute(&cx, "drop table rust_e2e_long_t purge", ()).await;
        conn.execute(
            &cx,
            "create table rust_e2e_long_t (id number primary key, v long)",
            (),
        )
        .await
        .expect("create LONG regression table");
        conn.execute(
            &cx,
            "insert into rust_e2e_long_t (id, v) values (:1, :2)",
            (1_i64, "single-row long regression"),
        )
        .await
        .expect("insert LONG regression row");
        let long_row = conn
            .query_one(
                &cx,
                "select v from rust_e2e_long_t where id = :1",
                (1_i64,),
            )
            .await
            .expect("single-row LONG query_one should not report TooManyRows");
        assert_eq!(row_text(&long_row, 0), "single-row long regression");
        log.step(
            "query_one_long",
            "rows=1 column_shape=Text regression=long_single_row_cardinality",
        );
        conn.execute(&cx, "drop table rust_e2e_long_t purge", ())
            .await
            .expect("drop LONG regression table");

        conn.close(&cx).await.expect("close connection");
    });
    log.ok();
}

#[test]
#[ignore = "requires live Oracle container env from scripts/container.sh"]
fn execute_execute_many_and_register_query_are_logged() {
    let log = E2eLog::new("four_families_execute");
    let Some(options) = connect_options() else {
        log.skip(
            "four_families_execute",
            "PYO_TEST_* environment not configured",
        );
        return;
    };
    let runtime = build_runtime();
    runtime.block_on(async {
        let cx = Cx::current().expect("Runtime::block_on should install an ambient Cx");
        let mut conn = Connection::connect(&cx, options)
            .await
            .expect("connect to test container");
        log.step(
            "connect",
            &format!("session_id={} password=redacted", conn.session_id()),
        );

        let _ = conn.execute(&cx, "drop table rust_e2e_dml_t purge", ()).await;
        conn.execute(
            &cx,
            "create table rust_e2e_dml_t (id number primary key, name varchar2(30))",
            (),
        )
        .await
        .expect("create DML table");
        log.step("execute_ddl", "sql=create table rows_affected=0");

        let rolled_back = conn
            .execute(
                &cx,
                "insert into rust_e2e_dml_t (id, name) values (:1, :2)",
                (900_i64, "rollback"),
            )
            .await
            .expect("insert rollback probe");
        assert_eq!(rolled_back.rows_affected(), 1);
        conn.rollback(&cx).await.expect("rollback probe transaction");
        let absent = conn
            .query_opt(
                &cx,
                "select name from rust_e2e_dml_t where id = :1",
                (900_i64,),
            )
            .await
            .expect("query rolled back row");
        assert!(absent.is_none(), "rollback should remove uncommitted row");
        log.step(
            "transaction_rollback",
            "inserted_rows=1 visible_after_rollback=false bind_values=redacted",
        );

        let insert = conn
            .execute(
                &cx,
                "insert into rust_e2e_dml_t (id, name) values (:1, :2)",
                (1_i64, "alice"),
            )
            .await
            .expect("insert via execute");
        assert_eq!(insert.rows_affected(), 1);
        conn.commit(&cx).await.expect("commit inserted row");
        log.step(
            "execute_dml_commit",
            &format!(
                "rows_affected={} last_rowid_present={} out_binds=0 returning=0",
                insert.rows_affected(),
                insert.last_rowid().is_some()
            ),
        );

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
        assert_eq!(out.out_binds().len(), 1);
        assert_eq!(
            out.out_binds()
                .get(0)
                .and_then(Option::as_ref)
                .and_then(QueryValue::as_text),
            Some("out-value")
        );
        log.step(
            "execute_out_bind",
            "out_binds=count=1 idx0=Text value_redacted=true rows_affected=0",
        );

        let returning = conn
            .execute_with(
                &cx,
                Execute::new(
                    "update rust_e2e_dml_t set name = :1 where id = :2 returning name into :3",
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
        let returned = returning
            .returning()
            .rows_for(2)
            .expect("returning bind index");
        assert_eq!(
            returned
                .first()
                .and_then(Option::as_ref)
                .and_then(QueryValue::as_text),
            Some("bob")
        );
        log.step(
            "execute_returning",
            &format!(
                "rows_affected={} last_rowid_present={} returning_{} bind_values=redacted",
                returning.rows_affected(),
                returning.last_rowid().is_some(),
                returning_shape(returned)
            ),
        );

        let implicit = conn
            .execute(
                &cx,
                "declare rc sys_refcursor; begin \
                 open rc for select level as n from dual connect by level <= 3; \
                 dbms_sql.return_result(rc); end;",
                (),
            )
            .await
            .expect("implicit result set execute");
        assert_eq!(implicit.implicit_results().len(), 1);
        let cursor_id = implicit.implicit_results()[0].cursor_id;
        let fetched = conn
            .fetch_cursor(&cx, &implicit.implicit_results()[0], 100)
            .await
            .expect("fetch implicit cursor");
        assert_eq!(fetched.rows.len(), 3);
        log.step(
            "execute_implicit_result",
            &format!(
                "implicit_results=1 cursor_id={} rows_fetched={} columns={}",
                cursor_id,
                fetched.rows.len(),
                fetched.columns.len()
            ),
        );

        let batch_rows = vec![
            vec![
                BindValue::Number("2".to_string()),
                BindValue::Text("carol".to_string()),
            ],
            vec![
                BindValue::Number("3".to_string()),
                BindValue::Text("dana".to_string()),
            ],
        ];
        let inserted = conn
            .execute_many(
                &cx,
                "insert into rust_e2e_dml_t (id, name) values (:1, :2)",
                &batch_rows,
            )
            .await
            .expect("execute_many insert");
        assert_eq!(inserted.rows_affected(), 2);
        log.step(
            "execute_many_insert",
            "rows_affected=2 row_count=2 bind_width=2 bind_values=redacted",
        );

        let error_rows = vec![
            vec![
                BindValue::Number("3".to_string()),
                BindValue::Text("duplicate".to_string()),
            ],
            vec![
                BindValue::Number("4".to_string()),
                BindValue::Text("erin".to_string()),
            ],
        ];
        let with_error = conn
            .execute_many_with(
                &cx,
                Batch::new(
                    "insert into rust_e2e_dml_t (id, name) values (:1, :2)",
                    &error_rows,
                )
                .collect_errors(),
            )
            .await
            .expect("execute_many collect_errors");
        assert_eq!(with_error.errors().len(), 1);
        assert_eq!(with_error.errors()[0].row_index(), 0);
        assert_eq!(with_error.errors()[0].code(), 1);
        log.step(
            "execute_many_collect_errors",
            &format!(
                "rows_affected={} batch_errors={} bind_values=redacted",
                with_error.rows_affected(),
                batch_error_detail(with_error.errors())
            ),
        );

        let returning_rows = vec![
            vec![
                BindValue::Text("carol2".to_string()),
                BindValue::Number("2".to_string()),
                BindValue::ReturnOutput {
                    ora_type_num: ORA_TYPE_NUM_VARCHAR,
                    csfrm: CS_FORM_IMPLICIT,
                    buffer_size: 30,
                },
            ],
            vec![
                BindValue::Text("erin2".to_string()),
                BindValue::Number("4".to_string()),
                BindValue::ReturnOutput {
                    ora_type_num: ORA_TYPE_NUM_VARCHAR,
                    csfrm: CS_FORM_IMPLICIT,
                    buffer_size: 30,
                },
            ],
        ];
        let returning_many = conn
            .execute_many_with(
                &cx,
                Batch::new(
                    "update rust_e2e_dml_t set name = :1 where id = :2 returning name into :3",
                    &returning_rows,
                )
                .row_counts(),
            )
            .await
            .expect("execute_many returning");
        assert_eq!(returning_many.rows_affected(), 2);
        assert_eq!(returning_many.per_row_counts(), Some([1, 1].as_slice()));
        let returned_many = returning_many
            .returning()
            .rows_for(2)
            .expect("returning bind index");
        log.step(
            "execute_many_returning_probe",
            &format!(
                "expected_returned_rows=2 actual_returned_rows={} returning_bind_groups={} returning_{} bind_values=redacted",
                returned_many.len(),
                returning_many.returning().len(),
                returning_shape(returned_many)
            ),
        );
        let execute_many_returning_bug = returned_many.len() != 2;
        let actual_returned_many_len = returned_many.len();
        if !execute_many_returning_bug {
            log.step(
                "execute_many_returning",
                &format!(
                    "rows_affected={} per_row_counts={:?} returning_{} bind_values=redacted",
                    returning_many.rows_affected(),
                    returning_many.per_row_counts(),
                    returning_shape(returned_many)
                ),
            );
        }

        let _ = conn
            .execute(&cx, "drop table rust_e2e_cqn_t purge", ())
            .await;
        conn.execute(
            &cx,
            "create table rust_e2e_cqn_t (id number primary key, name varchar2(30))",
            (),
        )
        .await
        .expect("create CQN table");
        match conn
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
            Ok(subscription) => {
                let registered = conn
                    .register_query(
                        &cx,
                        Registration::new(
                            "select id, name from rust_e2e_cqn_t where id > :1",
                            subscription.registration_id,
                        )
                        .bind((0_i64,))
                        .timeout(Duration::from_secs(10)),
                    )
                    .await
                    .expect("register_query");
                let query_id = registered.query_id().expect("CQN query id should be present");
                assert!(query_id > 0);
                log.step(
                    "register_query",
                    &format!(
                        "registration_id={} query_id={} sql_shape=select bind_count=1 bind_values=redacted",
                        subscription.registration_id, query_id
                    ),
                );
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
            Err(err) => {
                log.skip(
                    "register_query",
                    &format!("CQN subscribe unavailable: {err}"),
                );
            }
        }
        conn.execute(&cx, "drop table rust_e2e_cqn_t purge", ())
            .await
            .expect("drop CQN table");
        conn.execute(&cx, "drop table rust_e2e_dml_t purge", ())
            .await
            .expect("drop DML table");
        if execute_many_returning_bug {
            log.step(
                "execute_many_returning_bug",
                &format!(
                    "api=execute_many_with expected_returned_rows=2 actual_returned_rows={} rows_affected=2 per_row_counts=[1,1]",
                    actual_returned_many_len
                ),
            );
            conn.close(&cx)
                .await
                .expect("close connection after execute_many returning bug");
            panic!(
                "execute_many RETURNING should aggregate one returned value per affected input row; got {actual_returned_many_len}, expected 2"
            );
        }
        conn.close(&cx).await.expect("close connection");
    });
    log.ok();
}

#[derive(Clone)]
struct LiveBackend {
    options: ConnectOptions,
    created: Arc<AtomicU64>,
    closed: Arc<AtomicU64>,
    pinged: Arc<AtomicU64>,
    log: E2eLog,
}

struct PooledLiveConn {
    conn: Mutex<Option<Connection>>,
}

impl PoolBackend for LiveBackend {
    type Conn = PooledLiveConn;

    fn create_connection(&self, id: u64, cclass: Option<&str>) -> Result<Self::Conn, String> {
        self.log.step(
            "pool_backend_create",
            &format!(
                "pool_conn_id={} cclass={} password=redacted",
                id,
                cclass.unwrap_or("none")
            ),
        );
        let conn =
            BlockingConnection::connect(self.options.clone()).map_err(|err| err.to_string())?;
        self.created.fetch_add(1, Ordering::SeqCst);
        Ok(PooledLiveConn {
            conn: Mutex::new(Some(conn)),
        })
    }

    fn ping_connection(&self, conn: &Self::Conn, ping_timeout_ms: u32) -> bool {
        self.pinged.fetch_add(1, Ordering::SeqCst);
        let mut guard = conn.conn.lock().expect("pooled connection lock");
        let Some(connection) = guard.as_mut() else {
            return false;
        };
        let ok = BlockingConnection::ping_with_timeout(connection, ping_timeout_ms).is_ok();
        self.log.step(
            "pool_backend_ping",
            &format!("ping_timeout_ms={} healthy={}", ping_timeout_ms, ok),
        );
        ok
    }

    fn close_connection(&self, id: u64, conn: Self::Conn) {
        let mut guard = conn.conn.lock().expect("pooled connection lock");
        if let Some(connection) = guard.take() {
            let disposition = if BlockingConnection::close(connection).is_ok() {
                "clean"
            } else {
                "close_error"
            };
            self.closed.fetch_add(1, Ordering::SeqCst);
            self.log.step(
                "pool_backend_close",
                &format!("pool_conn_id={} connection_disposition={}", id, disposition),
            );
        }
    }

    fn connection_is_open(&self, conn: &Self::Conn) -> bool {
        conn.conn
            .lock()
            .expect("pooled connection lock")
            .as_ref()
            .is_some_and(|conn| !conn.is_dead())
    }
}

fn pool_stats_detail(stats: PoolStats) -> String {
    format!(
        "idle={} busy={} open={} opening={} validating={} retiring={} waiters={}",
        stats.idle_count(),
        stats.busy_count(),
        stats.open_count(),
        stats.opening_count(),
        stats.validating_count(),
        stats.retiring_count(),
        stats.waiter_count()
    )
}

fn log_pool_stats(pool: &BlockingPool<LiveBackend>, log: &E2eLog, phase: &str) -> PoolStats {
    let stats = pool.stats().expect("pool stats");
    log.step(phase, &pool_stats_detail(stats));
    stats
}

fn wait_until(label: &str, timeout: Duration, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("timed out waiting for {label}");
}

#[test]
#[ignore = "requires live Oracle container env from scripts/container.sh"]
fn pool_live_stress_logs_state_transitions() {
    let log = E2eLog::new("pool_live_stress");
    let Some(options) = connect_options() else {
        log.skip("pool_live_stress", "PYO_TEST_* environment not configured");
        return;
    };
    let backend = LiveBackend {
        options,
        created: Arc::new(AtomicU64::new(0)),
        closed: Arc::new(AtomicU64::new(0)),
        pinged: Arc::new(AtomicU64::new(0)),
        log: log.clone(),
    };
    let config = PoolConfig::new(1, 2, 1)
        .with_getmode(POOL_GETMODE_WAIT)
        .with_wait_timeout_ms(2_000)
        .with_timeout_secs(1)
        .with_ping_interval_secs(0)
        .with_ping_timeout_ms(5_000)
        .with_creation_cclass("e2e-pool");
    let pool_async = Pool::start(backend.clone(), config).expect("pool starts");
    let pool = pool_async.blocking();
    wait_until("pool min connection", Duration::from_secs(10), || {
        pool.stats()
            .map(|stats| stats.open_count() >= 1)
            .unwrap_or(false)
    });
    log_pool_stats(&pool, &log, "pool_started");

    let workers = 2usize;
    let barrier = Arc::new(Barrier::new(workers));
    let (tx, rx) = mpsc::channel();
    let mut handles = Vec::new();
    for worker in 0..workers {
        let worker_pool = pool.clone();
        let worker_barrier = Arc::clone(&barrier);
        let worker_tx = tx.clone();
        handles.push(std::thread::spawn(move || {
            worker_barrier.wait();
            let guard = worker_pool
                .acquire(AcquireOptions::default())
                .expect("concurrent acquire");
            worker_tx
                .send((worker, guard.id()))
                .expect("send concurrent acquire id");
            std::thread::sleep(Duration::from_millis(250));
            guard.release().expect("release concurrent guard");
        }));
    }
    drop(tx);
    let mut acquired_ids = Vec::new();
    for _ in 0..workers {
        acquired_ids.push(
            rx.recv_timeout(Duration::from_secs(10))
                .expect("worker should report acquired id"),
        );
    }
    log.step(
        "pool_concurrent_acquire",
        &format!("workers={} acquired_ids={:?}", workers, acquired_ids),
    );
    let busy = log_pool_stats(&pool, &log, "pool_during_concurrent_acquire").busy_count();
    assert_eq!(busy, workers as u32);
    for handle in handles {
        handle.join().expect("concurrent acquire worker joins");
    }
    let after_release = log_pool_stats(&pool, &log, "pool_after_concurrent_release");
    assert_eq!(after_release.busy_count(), 0);

    pool.set_getmode(POOL_GETMODE_TIMEDWAIT)
        .expect("set timedwait getmode");
    pool.set_wait_timeout_ms(50)
        .expect("set short wait timeout");
    let hold_a = pool
        .acquire(AcquireOptions::default())
        .expect("hold first pool slot");
    let hold_b = pool
        .acquire(AcquireOptions::default())
        .expect("hold second pool slot");
    log_pool_stats(&pool, &log, "pool_saturated_before_timedwait");
    let started = Instant::now();
    let err = pool
        .acquire(AcquireOptions::default())
        .map(|guard| guard.id())
        .expect_err("saturated timedwait acquire should fail");
    assert!(matches!(err, PoolError::NoConnectionAvailable));
    log.step(
        "pool_timedwait_timeout",
        &format!(
            "expected_code=DPY-4005 elapsed_ms={} error_variant=NoConnectionAvailable",
            started.elapsed().as_millis()
        ),
    );
    hold_a.release().expect("release hold_a");
    hold_b.release().expect("release hold_b");

    pool.set_getmode(POOL_GETMODE_WAIT)
        .expect("restore wait getmode");
    let held_cancel_a = pool
        .acquire(AcquireOptions::default())
        .expect("hold cancel slot a");
    let held_cancel_b = pool
        .acquire(AcquireOptions::default())
        .expect("hold cancel slot b");
    log_pool_stats(&pool, &log, "pool_before_cancelled_acquire");
    let pool_for_async = pool_async.clone();
    let runtime = build_runtime();
    let err = runtime.block_on(async {
        let cx = Cx::current().expect("Runtime::block_on should install an ambient Cx");
        let cancel_cx = cx.clone();
        let returner = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(75));
            cancel_cx.cancel_fast(asupersync::CancelKind::User);
            held_cancel_a
                .release()
                .expect("release cancelled-acquire slot");
        });
        let err = pool_for_async
            .acquire(&cx, AcquireOptions::default())
            .await
            .map(|guard| guard.id())
            .expect_err("cancelled acquire should not succeed");
        returner.join().expect("cancel returner joins");
        err
    });
    assert!(matches!(err, PoolError::Cancelled(_)));
    log.step(
        "pool_cancel_during_acquire",
        &format!(
            "error_variant=Cancelled no_slot_leak=true error_display=\"{}\"",
            err
        ),
    );
    let stats_after_cancel = log_pool_stats(&pool, &log, "pool_after_cancelled_acquire");
    assert_eq!(stats_after_cancel.busy_count(), 1);
    held_cancel_b
        .release()
        .expect("release cancelled-acquire survivor");
    let reacquired_a = pool
        .acquire(AcquireOptions::default())
        .expect("reacquire after cancelled waiter");
    let reacquired_b = pool
        .acquire(AcquireOptions::default())
        .expect("second reacquire after cancelled waiter");
    assert_ne!(
        reacquired_a.id(),
        reacquired_b.id(),
        "cancelled waiter must not double-hand out one slot"
    );
    log.step(
        "pool_reacquire_after_cancel",
        &format!(
            "ids=[{},{}] double_handout=false",
            reacquired_a.id(),
            reacquired_b.id()
        ),
    );
    reacquired_a.release().expect("release reacquired a");
    reacquired_b.release().expect("release reacquired b");

    pool.set_timeout_secs(1)
        .expect("wake idle reaper with one-second timeout");
    log_pool_stats(&pool, &log, "pool_before_idle_reaper_wait");
    let reaper_deadline = Instant::now() + Duration::from_secs(15);
    let mut reaper_ready = false;
    while Instant::now() < reaper_deadline {
        let stats = pool
            .stats()
            .expect("poll pool stats during idle reaper wait");
        if stats.open_count() <= 1 && stats.busy_count() == 0 {
            reaper_ready = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    if !reaper_ready {
        let stats = log_pool_stats(&pool, &log, "pool_idle_reaper_timeout_state");
        panic!(
            "timed out waiting for idle expiry reaps extra; {}",
            pool_stats_detail(stats)
        );
    }
    let reaped = log_pool_stats(&pool, &log, "pool_after_idle_reaper");
    assert!(reaped.open_count() <= 1);
    let before_ping = backend.pinged.load(Ordering::SeqCst);
    let pinged = pool
        .acquire(AcquireOptions::default())
        .expect("acquire after idle for ping");
    let after_ping = backend.pinged.load(Ordering::SeqCst);
    assert!(
        after_ping > before_ping,
        "ping_interval_secs=0 should force ping on idle acquire"
    );
    log.step(
        "pool_ping_on_acquire",
        &format!(
            "pool_conn_id={} ping_count_delta={} healthy=true",
            pinged.id(),
            after_ping - before_ping
        ),
    );
    pinged.release().expect("release pinged connection");
    pool.close(false).expect("graceful close pool");
    log.step(
        "pool_graceful_close",
        &format!(
            "created={} closed={} pinged={}",
            backend.created.load(Ordering::SeqCst),
            backend.closed.load(Ordering::SeqCst),
            backend.pinged.load(Ordering::SeqCst)
        ),
    );

    let force_backend = LiveBackend {
        log: E2eLog::new("pool_force_close_probe"),
        ..backend
    };
    let force_pool = Pool::start(
        force_backend.clone(),
        PoolConfig::new(1, 1, 1)
            .with_getmode(POOL_GETMODE_WAIT)
            .with_ping_interval_secs(-1),
    )
    .expect("force-close pool starts")
    .blocking();
    wait_until("force pool min connection", Duration::from_secs(10), || {
        force_pool
            .stats()
            .map(|stats| stats.open_count() >= 1)
            .unwrap_or(false)
    });
    let busy_guard = force_pool
        .acquire(AcquireOptions::default())
        .expect("hold busy force-close slot");
    let graceful_err = force_pool
        .close(false)
        .expect_err("graceful close with busy connections should fail");
    assert!(matches!(graceful_err, PoolError::HasBusyConnections));
    log.step(
        "pool_graceful_close_busy",
        "error_variant=HasBusyConnections expected_code=DPY-1005",
    );
    drop(busy_guard);
    force_pool.drain().expect("drain dropped busy guard");
    force_pool.close(true).expect("forced close succeeds");
    log.step(
        "pool_forced_close",
        "force=true connection_disposition=closed",
    );
    log.ok();
}

#[test]
#[ignore = "requires live Oracle container env from scripts/container.sh"]
fn cancel_timeout_and_user_cancel_recovery_are_logged() {
    let log = E2eLog::new("cancel_recovery");
    let Some(options) = connect_options() else {
        log.skip("cancel_recovery", "PYO_TEST_* environment not configured");
        return;
    };
    let runtime = build_runtime();
    runtime.block_on(async {
        let cx = Cx::current().expect("Runtime::block_on should install an ambient Cx");
        let mut conn = Connection::connect(&cx, options.clone())
            .await
            .expect("connect to test container");
        log.step(
            "connect_timeout_probe",
            &format!("session_id={} password=redacted", conn.session_id()),
        );
        let started = Instant::now();
        let err = conn
            .execute_with(
                &cx,
                Execute::new("begin dbms_session.sleep(3); end;")
                    .timeout(Duration::from_millis(500)),
            )
            .await
            .expect_err("sleep should hit call timeout");
        match err {
            Error::CallTimeout(ms) => assert_eq!(ms, 500),
            other => panic!("expected CallTimeout, got {other:?}"),
        }
        assert!(
            !conn.is_dead(),
            "timeout recovery should keep session alive"
        );
        log.step(
            "call_timeout",
            &format!(
                "elapsed_ms={} error=CallTimeout timeout_ms=500 break_drain=true dead={}",
                started.elapsed().as_millis(),
                conn.is_dead()
            ),
        );
        let reused = conn
            .query_one(&cx, "select 7 + 5 as n from dual", ())
            .await
            .expect("reuse after call timeout");
        assert_eq!(row_i64(&reused, 0), 12);
        log.step(
            "timeout_recovery_reuse",
            "rows=1 value_shape=Number expected=12 session_state=Ready dead=false",
        );
        conn.close(&cx).await.expect("close timeout probe");
    });

    let runtime = build_runtime();
    let mut cancelled_conn = runtime.block_on(async {
        let cx = Cx::current().expect("Runtime::block_on should install an ambient Cx");
        let mut conn = Connection::connect(&cx, options)
            .await
            .expect("connect to test container");
        log.step(
            "connect_user_cancel_probe",
            &format!("session_id={} password=redacted", conn.session_id()),
        );
        let started = Instant::now();
        let mut rows = conn
            .query_with(
                &cx,
                Query::new("select level as n from dual connect by level <= 20 order by n")
                    .arraysize(NonZeroU32::new(1).expect("non-zero arraysize"))
                    .timeout(Duration::from_secs(10)),
            )
            .await
            .expect("open cancellable continuation cursor");
        assert_eq!(rows.batch().len(), 1);
        cx.cancel_fast(asupersync::CancelKind::User);
        let err = rows
            .next_batch(&cx)
            .await
            .expect_err("user cancel should surface Error::Cancelled");
        assert!(matches!(err, Error::Cancelled));
        drop(rows);
        assert!(!conn.is_dead(), "user cancel should keep session reusable");
        log.step(
            "user_cancel",
            &format!(
                "elapsed_ms={} error=Cancelled ora_code=1013 checkpoint=fetch_continuation rows_before_cancel=1 dead={}",
                started.elapsed().as_millis(),
                conn.is_dead()
            ),
        );
        conn
    });
    let reused =
        BlockingConnection::query_one(&mut cancelled_conn, "select 12 * 2 as n from dual", ())
            .expect("reuse after user cancel with fresh context");
    assert_eq!(row_i64(&reused, 0), 24);
    log.step(
        "user_cancel_reuse",
        "rows=1 value_shape=Number expected=24 session_state=Ready dead=false",
    );
    BlockingConnection::close(cancelled_conn).expect("close user-cancel probe");
    log.ok();
}

#[test]
#[ignore = "requires live Oracle container env from scripts/container.sh"]
fn lob_streaming_and_transactions_are_logged() {
    with_connection("lob_streaming", |log, conn| {
        drop_table_if_exists(conn, "rust_e2e_lob_t");
        BlockingConnection::execute(
            conn,
            "create table rust_e2e_lob_t (id number primary key, body clob)",
            (),
        )
        .expect("create LOB table");
        let inserted = BlockingConnection::execute(
            conn,
            "insert into rust_e2e_lob_t (id, body) values (:1, to_clob(:2))",
            (1_i64, "persistent clob payload"),
        )
        .expect("insert LOB row");
        assert_eq!(inserted.rows_affected(), 1);
        BlockingConnection::commit(conn).expect("commit LOB row");
        log.step(
            "lob_setup_commit",
            "rows_affected=1 transaction=commit bind_values=redacted",
        );

        log.step(
            "lob_stream_lobs_locator_mode",
            "stream_lobs=true expected_shape=Lob read_amount=inserted_text_chars",
        );
        let expected_text = "persistent clob payload";
        let lob_rows = BlockingConnection::query_with(
            conn,
            Query::new("select body from rust_e2e_lob_t where id = 1")
                .stream_lobs()
                .arraysize(NonZeroU32::new(1).expect("non-zero arraysize")),
        )
        .expect("query LOB locator")
        .collect()
        .expect("collect LOB locator rows");
        assert_eq!(lob_rows.len(), 1);
        let row = lob_rows.first().expect("one LOB row");
        let lob = match row.value(0).expect("LOB column value") {
            QueryValue::Lob(lob) => lob.as_ref(),
            other => panic!("expected LOB locator, got {other:?}"),
        };
        log.step(
            "lob_locator",
            &format!(
                "shape=Lob size={} chunk_size={} locator_digest=len:{}",
                lob.size,
                lob.chunk_size,
                lob.locator.len()
            ),
        );

        let mut offset = 1u64;
        let mut total = Vec::new();
        let chunk = 8u64;
        let mut remaining_chars = u64::try_from(expected_text.chars().count())
            .expect("expected text char count fits u64");
        while remaining_chars > 0 {
            let amount = remaining_chars.min(chunk);
            let read = BlockingConnection::read_lob(conn, &lob.locator, offset, amount)
                .expect("read LOB chunk");
            let data = read.data.expect("LOB read returns data");
            log.step(
                "lob_read_chunk",
                &format!(
                    "offset={} requested={} bytes={} locator_digest=len:{}",
                    offset,
                    amount,
                    data.len(),
                    lob.locator.len()
                ),
            );
            total.extend_from_slice(&data);
            offset += amount;
            remaining_chars -= amount;
        }
        let text = decode_lob_text(&total, lob.csfrm, Some(&lob.locator))
            .expect("decode streamed CLOB text");
        assert_eq!(text, expected_text);
        log.step(
            "lob_read_complete",
            &format!(
                "chunks={} bytes={} text_redacted=true",
                total.len().div_ceil(8),
                total.len()
            ),
        );

        let temp = BlockingConnection::create_temp_lob(conn, ORA_TYPE_NUM_CLOB, CS_FORM_IMPLICIT)
            .expect("create temporary CLOB");
        let mut locator = temp.locator;
        let chunks = ["alpha-", "beta-", "gamma"];
        let mut write_offset = 1u64;
        for chunk_text in chunks {
            let encoded = encode_lob_text(chunk_text, CS_FORM_IMPLICIT, Some(&locator));
            let written = BlockingConnection::write_lob(conn, &locator, write_offset, &encoded)
                .expect("write temp LOB chunk");
            if !written.locator.is_empty() {
                locator = written.locator;
            }
            log.step(
                "lob_write_chunk",
                &format!(
                    "offset={} bytes={} locator_digest=len:{} data_redacted=true",
                    write_offset,
                    encoded.len(),
                    locator.len()
                ),
            );
            write_offset += chunk_text.len() as u64;
        }
        let read_back =
            BlockingConnection::read_lob(conn, &locator, 1, write_offset.saturating_sub(1))
                .expect("read temp LOB after writes");
        let temp_bytes = read_back.data.expect("temp LOB read returns data");
        let temp_text = decode_lob_text(&temp_bytes, CS_FORM_IMPLICIT, Some(&locator))
            .expect("decode temp CLOB");
        assert_eq!(temp_text, "alpha-beta-gamma");
        log.step(
            "lob_write_verify",
            &format!(
                "chunks={} bytes={} text_redacted=true",
                chunks.len(),
                temp_bytes.len()
            ),
        );
        BlockingConnection::free_temp_lobs(conn, &[locator]).expect("free temp LOB");
        log.step("lob_free_temp", "locators=1 connection_disposition=alive");
        drop_table_if_exists(conn, "rust_e2e_lob_t");
    });
}
