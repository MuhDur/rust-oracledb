//! Standalone-library integration suite for the `oracledb` crate.
//!
//! These tests drive the PUBLIC crate API directly (`BlockingConnection`,
//! `Connection`, `ConnectOptions`, the `pool` facade) against a live Oracle
//! container, with no PyO3 shim and no Python in the loop. Each test is a real
//! round trip: connect, execute, fetch, commit. The point is to prove that the
//! driver is usable as an ordinary Rust dependency.
//!
//! Every test self-skips cleanly (prints `skipped: ...` and returns) when the
//! container environment is absent, so `cargo test` stays green offline. Run
//! against the container with:
//!
//! ```sh
//! eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1524 \
//!         ORACLEDB_HOST_PORT=1524 scripts/container.sh env)"
//! cargo test -p oracledb --test integration_library
//! ```

use std::sync::atomic::{AtomicU64, Ordering};

use oracledb::pool::{AcquireOptions, Pool, PoolBackend, PoolConfig, POOL_GETMODE_WAIT};
use oracledb::protocol::oson::OsonValue;
use oracledb::protocol::thin::{BindValue, ExecuteOptions, QueryValue};
use oracledb::protocol::vector::{Vector, VectorValues};
use oracledb::{BlockingConnection, ConnectOptions, Connection};
use oracledb_protocol::ClientIdentity;

/// Connection identity used by these tests. The fields are deliberately
/// distinctive so a `v$session` lookup can prove the caller-set masquerade.
const PROGRAM: &str = "rust-oracledb-itest";
const MACHINE: &str = "itest-machine";
const OSUSER: &str = "itest-osuser";
const TERMINAL: &str = "itest-terminal";
const DRIVER: &str = "rust-oracledb thn : 0.0.0";

/// Build connect options from the harness container environment, or return
/// `None` so the caller can self-skip when the container is not configured.
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

/// Run `body` with a freshly connected `BlockingConnection`, or print a skip
/// notice and return when the container environment is absent.
fn with_connection(test: &str, body: impl FnOnce(&mut Connection)) {
    let Some(options) = connect_options() else {
        eprintln!("skipped {test}: PYO_TEST_* environment not configured");
        return;
    };
    let mut conn = BlockingConnection::connect(options).expect("connect to test container");
    body(&mut conn);
    BlockingConnection::close(conn).expect("close connection");
}

/// Best-effort DDL that ignores "object does not exist" style failures so a
/// drop-then-create setup is idempotent across reruns.
fn drop_if_exists(conn: &mut Connection, ddl: &str) {
    let _ = BlockingConnection::execute_query(conn, ddl, 1);
}

// ---------------------------------------------------------------------------
// connect + identity masquerade (the headline differentiator)
// ---------------------------------------------------------------------------

#[test]
fn connect_reports_session_and_server_version() {
    with_connection("connect_reports_session_and_server_version", |conn| {
        assert!(conn.session_id() > 0, "session id should be assigned");
        assert!(conn.serial_num() > 0, "serial number should be assigned");
        // identity round-trips back through the accessor
        assert_eq!(conn.identity().program, PROGRAM);
        assert_eq!(conn.identity().osuser, OSUSER);
    });
}

/// The differentiator: the connection advertises a CALLER-SET program / osuser
/// / machine / terminal, and the database records exactly those values in
/// `v$session`. A normal OCI client cannot lie about these; this thin driver
/// can. We read the session's own `v$session` row (no admin grant required).
#[test]
fn identity_masquerade_is_visible_in_v_session() {
    with_connection("identity_masquerade_is_visible_in_v_session", |conn| {
        let result = BlockingConnection::execute_query(
            conn,
            "select program, osuser, machine, terminal \
             from v$session where sid = sys_context('USERENV','SID')",
            2,
        )
        .expect("v$session self-lookup should fetch");
        assert_eq!(result.rows.len(), 1, "exactly one own-session row");
        assert_eq!(
            result.cell(0, 0).and_then(QueryValue::as_text),
            Some(PROGRAM),
            "v$session.program must reflect the caller-set program"
        );
        assert_eq!(
            result.cell(0, 1).and_then(QueryValue::as_text),
            Some(OSUSER),
            "v$session.osuser must reflect the caller-set osuser"
        );
        assert_eq!(
            result.cell(0, 2).and_then(QueryValue::as_text),
            Some(MACHINE),
            "v$session.machine must reflect the caller-set machine"
        );
        assert_eq!(
            result.cell(0, 3).and_then(QueryValue::as_text),
            Some(TERMINAL),
            "v$session.terminal must reflect the caller-set terminal"
        );
    });
}

// ---------------------------------------------------------------------------
// SELECT with typed fetch across the scalar type spectrum
// ---------------------------------------------------------------------------

#[test]
fn typed_scalar_fetch_covers_core_types() {
    with_connection("typed_scalar_fetch_covers_core_types", |conn| {
        let result = BlockingConnection::execute_query(
            conn,
            "select \
                cast(1234567890123456789 as number(19))      as num_int, \
                cast(2.5 as number(10,4))                    as num_dec, \
                cast('hello' as varchar2(20))                as vc, \
                date '2024-06-13'                            as dt, \
                timestamp '2024-06-13 08:09:10.123456'       as ts, \
                hextoraw('DEADBEEF')                         as rw, \
                cast(6.0221409e23 as binary_double)          as bd \
             from dual",
            2,
        )
        .expect("scalar select should fetch");
        assert_eq!(result.rows.len(), 1);

        // NUMBER: lossless integer via the canonical text
        assert_eq!(
            result.cell(0, 0).and_then(QueryValue::as_i64),
            Some(1_234_567_890_123_456_789),
        );
        // NUMBER with scale: text preserves the decimal exactly
        assert_eq!(
            result
                .cell(0, 1)
                .and_then(QueryValue::as_number_text)
                .as_deref(),
            Some("2.5"),
        );
        // VARCHAR2
        assert_eq!(
            result.cell(0, 2).and_then(QueryValue::as_text),
            Some("hello")
        );
        // DATE
        assert!(matches!(
            result.cell(0, 3),
            Some(QueryValue::DateTime {
                year: 2024,
                month: 6,
                day: 13,
                ..
            })
        ));
        // TIMESTAMP with fractional seconds
        assert!(matches!(
            result.cell(0, 4),
            Some(QueryValue::DateTime {
                year: 2024, month: 6, day: 13, hour: 8, minute: 9, second: 10, nanosecond
            }) if *nanosecond == 123_456_000
        ));
        // RAW
        assert_eq!(
            result.cell(0, 5).and_then(QueryValue::as_raw),
            Some([0xDE, 0xAD, 0xBE, 0xEF].as_slice()),
        );
        // BINARY_DOUBLE
        let bd = result
            .cell(0, 6)
            .and_then(QueryValue::as_f64)
            .expect("binary_double parses as f64");
        assert!((bd - 6.0221409e23).abs() / 6.0221409e23 < 1e-9);
    });
}

#[test]
fn rowid_and_boolean_fetch() {
    with_connection("rowid_and_boolean_fetch", |conn| {
        // ROWID: every heap row has one; fetch dual's pseudo-rowid
        let rowid = BlockingConnection::execute_query(conn, "select rowid from dual", 2)
            .expect("rowid select should fetch");
        let rid = rowid
            .cell(0, 0)
            .and_then(QueryValue::as_rowid)
            .expect("dual rowid is a ROWID value");
        assert!(!rid.is_empty(), "rowid text should be non-empty");

        // BINARY (native DB_TYPE_BOOLEAN) requires 23ai; the container is 23ai.
        let boolean = BlockingConnection::execute_query(conn, "select true, false from dual", 2);
        match boolean {
            Ok(result) => {
                assert_eq!(result.cell(0, 0).and_then(QueryValue::as_bool), Some(true));
                assert_eq!(result.cell(0, 1).and_then(QueryValue::as_bool), Some(false));
            }
            Err(err) => {
                // Older databases lack the SQL boolean literal; that is fine.
                eprintln!("boolean literal not supported on this server: {err}");
            }
        }
    });
}

// ---------------------------------------------------------------------------
// positional + named binds
// ---------------------------------------------------------------------------

#[test]
fn positional_and_named_binds() {
    with_connection("positional_and_named_binds", |conn| {
        // positional binds (:1, :2)
        let positional = BlockingConnection::execute_query_with_binds(
            conn,
            "select :1 + :2 as total from dual",
            2,
            &[
                BindValue::Number("40".to_string()),
                BindValue::Number("2".to_string()),
            ],
        )
        .expect("positional bind select should fetch");
        assert_eq!(positional.cell(0, 0).and_then(QueryValue::as_i64), Some(42));

        // named binds (:lo, :hi) bound in declaration order
        let named = BlockingConnection::execute_query_with_binds(
            conn,
            "select :greeting || ' ' || :who as msg from dual",
            2,
            &[
                BindValue::Text("hello".to_string()),
                BindValue::Text("world".to_string()),
            ],
        )
        .expect("named bind select should fetch");
        assert_eq!(
            named.cell(0, 0).and_then(QueryValue::as_text),
            Some("hello world"),
        );
    });
}

// ---------------------------------------------------------------------------
// INSERT / UPDATE / DELETE + commit / rollback
// ---------------------------------------------------------------------------

#[test]
fn dml_with_commit_and_rollback() {
    with_connection("dml_with_commit_and_rollback", |conn| {
        drop_if_exists(conn, "drop table rust_itest_dml purge");
        BlockingConnection::execute_query(
            conn,
            "create table rust_itest_dml (id number(9) primary key, val varchar2(40))",
            1,
        )
        .expect("create table");

        // INSERT then commit -> visible after a fresh select
        let ins = BlockingConnection::execute_query_with_binds(
            conn,
            "insert into rust_itest_dml (id, val) values (:1, :2)",
            1,
            &[
                BindValue::Number("1".to_string()),
                BindValue::Text("first".to_string()),
            ],
        )
        .expect("insert");
        assert_eq!(ins.row_count, 1);
        BlockingConnection::commit(conn).expect("commit");

        // UPDATE
        let upd = BlockingConnection::execute_query_with_binds(
            conn,
            "update rust_itest_dml set val = :1 where id = :2",
            1,
            &[
                BindValue::Text("updated".to_string()),
                BindValue::Number("1".to_string()),
            ],
        )
        .expect("update");
        assert_eq!(upd.row_count, 1);
        BlockingConnection::commit(conn).expect("commit update");

        // DELETE without commit, then rollback -> row reappears
        let del =
            BlockingConnection::execute_query(conn, "delete from rust_itest_dml where id = 1", 1)
                .expect("delete");
        assert_eq!(del.row_count, 1);
        BlockingConnection::rollback(conn).expect("rollback");

        let after = BlockingConnection::execute_query(
            conn,
            "select val from rust_itest_dml where id = 1",
            2,
        )
        .expect("select after rollback");
        assert_eq!(
            after.cell(0, 0).and_then(QueryValue::as_text),
            Some("updated"),
            "rollback must restore the deleted, previously-updated row"
        );

        drop_if_exists(conn, "drop table rust_itest_dml purge");
    });
}

// ---------------------------------------------------------------------------
// executemany / array DML (multiple bind rows in one execute)
// ---------------------------------------------------------------------------

#[test]
fn executemany_array_dml() {
    with_connection("executemany_array_dml", |conn| {
        drop_if_exists(conn, "drop table rust_itest_many purge");
        BlockingConnection::execute_query(
            conn,
            "create table rust_itest_many (id number(9), label varchar2(20))",
            1,
        )
        .expect("create table");

        // four bind rows -> one array-DML execute -> four inserted rows
        let rows: Vec<Vec<BindValue>> = (1..=4)
            .map(|i| {
                vec![
                    BindValue::Number(i.to_string()),
                    BindValue::Text(format!("row{i}")),
                ]
            })
            .collect();
        let result = BlockingConnection::execute_query_with_bind_rows(
            conn,
            "insert into rust_itest_many (id, label) values (:1, :2)",
            1,
            &rows,
        )
        .expect("array DML insert");
        assert_eq!(result.row_count, 4, "array DML inserts all rows");
        BlockingConnection::commit(conn).expect("commit");

        let count =
            BlockingConnection::execute_query(conn, "select count(*) from rust_itest_many", 2)
                .expect("count");
        assert_eq!(count.cell(0, 0).and_then(QueryValue::as_i64), Some(4));

        // arraydmlrowcounts: ask the server for a per-iteration row count vector
        let counted = BlockingConnection::execute_query_with_bind_rows_options_and_timeout(
            conn,
            "delete from rust_itest_many where id = :1",
            1,
            &[
                vec![BindValue::Number("1".to_string())],
                vec![BindValue::Number("2".to_string())],
            ],
            ExecuteOptions {
                arraydmlrowcounts: true,
                ..ExecuteOptions::default()
            },
            None,
        )
        .expect("array DML delete with row counts");
        assert_eq!(
            counted.array_dml_row_counts.as_deref(),
            Some([1u64, 1u64].as_slice()),
            "each delete iteration removed exactly one row"
        );
        BlockingConnection::commit(conn).expect("commit delete");

        drop_if_exists(conn, "drop table rust_itest_many purge");
    });
}

// ---------------------------------------------------------------------------
// LOB read
// ---------------------------------------------------------------------------

#[test]
fn clob_read_round_trip() {
    with_connection("clob_read_round_trip", |conn| {
        drop_if_exists(conn, "drop table rust_itest_lob purge");
        BlockingConnection::execute_query(
            conn,
            "create table rust_itest_lob (id number(9), body clob)",
            1,
        )
        .expect("create table");
        BlockingConnection::execute_query(
            conn,
            "insert into rust_itest_lob values (1, to_clob('the quick brown fox'))",
            1,
        )
        .expect("insert clob");
        BlockingConnection::commit(conn).expect("commit");

        // selecting a CLOB returns a locator; read its bytes over the wire.
        // `execute_query_collect` performs the client-side define-fetch that a
        // CLOB column needs, materializing the locator in the first batch.
        let select = BlockingConnection::execute_query_collect(
            conn,
            "select body from rust_itest_lob where id = 1",
            2,
        )
        .expect("select clob locator");
        let (locator, size, csfrm) = match select.cell(0, 0) {
            Some(QueryValue::Lob(lob)) => (lob.locator.clone(), lob.size, lob.csfrm),
            other => panic!("expected a LOB locator, got {other:?}"),
        };
        assert!(size > 0, "clob reports a non-zero length");

        let read =
            BlockingConnection::read_lob(conn, &locator, 1, size).expect("read_lob round trip");
        let bytes = read.data.expect("clob read returns data");
        // decode the raw LOB bytes per the column character-set form (the
        // server streams CLOB content in its own charset; the public
        // `decode_lob_text` helper applies the UTF-8 / UTF-16 policy)
        let text = oracledb::protocol::thin::decode_lob_text(&bytes, csfrm, Some(&locator))
            .expect("decode clob text");
        assert_eq!(text, "the quick brown fox");

        drop_if_exists(conn, "drop table rust_itest_lob purge");
    });
}

// ---------------------------------------------------------------------------
// object type fetch
// ---------------------------------------------------------------------------

#[test]
fn object_type_fetch() {
    with_connection("object_type_fetch", |conn| {
        drop_if_exists(conn, "drop table rust_itest_obj purge");
        drop_if_exists(conn, "drop type rust_itest_point force");
        BlockingConnection::execute_query(
            conn,
            "create or replace type rust_itest_point as object (x number, y number)",
            1,
        )
        .expect("create type");
        BlockingConnection::execute_query(
            conn,
            "create table rust_itest_obj (id number(9), p rust_itest_point)",
            1,
        )
        .expect("create table");
        BlockingConnection::execute_query(
            conn,
            "insert into rust_itest_obj values (1, rust_itest_point(3, 4))",
            1,
        )
        .expect("insert object");
        BlockingConnection::commit(conn).expect("commit");

        let select =
            BlockingConnection::execute_query(conn, "select p from rust_itest_obj where id = 1", 2)
                .expect("select object column");
        // the column metadata identifies the object type
        let column = &select.columns[0];
        assert_eq!(
            column.object_type_name.as_deref().map(str::to_uppercase),
            Some("RUST_ITEST_POINT".to_string()),
        );
        // the value is a packed object image (decoding the pickle is the shim's
        // job; the crate surfaces the schema + raw image)
        match select.cell(0, 0) {
            Some(QueryValue::Object(object)) => {
                assert_eq!(
                    object.type_name.as_deref().map(str::to_uppercase),
                    Some("RUST_ITEST_POINT".to_string())
                );
                assert!(!object.packed_data.is_empty(), "object image carries data");
            }
            other => panic!("expected an Object value, got {other:?}"),
        }

        drop_if_exists(conn, "drop table rust_itest_obj purge");
        drop_if_exists(conn, "drop type rust_itest_point force");
    });
}

// ---------------------------------------------------------------------------
// VECTOR round trip (23ai)
// ---------------------------------------------------------------------------

#[test]
fn vector_round_trip() {
    with_connection("vector_round_trip", |conn| {
        drop_if_exists(conn, "drop table rust_itest_vec purge");
        let created = BlockingConnection::execute_query(
            conn,
            "create table rust_itest_vec (id number(9), embedding vector(3, float32))",
            1,
        );
        if let Err(err) = created {
            eprintln!("skipping vector_round_trip: server lacks VECTOR support: {err}");
            return;
        }

        // bind a dense float32 vector IN, then fetch it back OUT
        let embedding = Vector::Dense(VectorValues::Float32(vec![1.5, -2.0, 3.25]));
        BlockingConnection::execute_query_with_binds(
            conn,
            "insert into rust_itest_vec (id, embedding) values (1, :1)",
            1,
            &[BindValue::Vector(embedding.clone())],
        )
        .expect("insert vector");
        BlockingConnection::commit(conn).expect("commit");

        // VECTOR columns need the client-side define-fetch that
        // `execute_query_collect` runs automatically.
        let select = BlockingConnection::execute_query_collect(
            conn,
            "select embedding from rust_itest_vec where id = 1",
            2,
        )
        .expect("select vector");
        match select.cell(0, 0) {
            Some(QueryValue::Vector(vector)) => match vector.as_ref() {
                Vector::Dense(VectorValues::Float32(values)) => {
                    assert_eq!(values.as_slice(), &[1.5, -2.0, 3.25]);
                }
                other => panic!("expected a dense float32 vector, got {other:?}"),
            },
            other => panic!("expected a dense float32 vector, got {other:?}"),
        }

        drop_if_exists(conn, "drop table rust_itest_vec purge");
    });
}

// ---------------------------------------------------------------------------
// JSON / OSON round trip (23ai native DB_TYPE_JSON)
// ---------------------------------------------------------------------------

#[test]
fn json_oson_round_trip() {
    with_connection("json_oson_round_trip", |conn| {
        drop_if_exists(conn, "drop table rust_itest_json purge");
        let created = BlockingConnection::execute_query(
            conn,
            "create table rust_itest_json (id number(9), doc json)",
            1,
        );
        if let Err(err) = created {
            eprintln!("skipping json_oson_round_trip: server lacks native JSON: {err}");
            return;
        }

        // build an OSON object, encode it, bind it IN as DB_TYPE_JSON
        let doc = OsonValue::Object(vec![
            ("name".to_string(), OsonValue::String("widget".to_string())),
            ("qty".to_string(), OsonValue::Number("7".to_string())),
            ("active".to_string(), OsonValue::Bool(true)),
        ]);
        let image = oracledb::protocol::oson::encode_oson(&doc, true).expect("encode oson");
        BlockingConnection::execute_query_with_binds(
            conn,
            "insert into rust_itest_json (id, doc) values (1, :1)",
            1,
            &[BindValue::Json(image)],
        )
        .expect("insert json");
        BlockingConnection::commit(conn).expect("commit");

        // native JSON columns need the client-side define-fetch that
        // `execute_query_collect` runs automatically.
        let select = BlockingConnection::execute_query_collect(
            conn,
            "select doc from rust_itest_json where id = 1",
            2,
        )
        .expect("select json");
        match select.cell(0, 0) {
            Some(QueryValue::Json(json)) => match json.as_ref() {
                OsonValue::Object(fields) => {
                    let get = |key: &str| fields.iter().find(|(k, _)| k == key).map(|(_, v)| v);
                    assert_eq!(get("name"), Some(&OsonValue::String("widget".to_string())));
                    assert_eq!(get("qty"), Some(&OsonValue::Number("7".to_string())));
                    assert_eq!(get("active"), Some(&OsonValue::Bool(true)));
                }
                other => panic!("expected a JSON object, got {other:?}"),
            },
            other => panic!("expected a JSON object, got {other:?}"),
        }

        drop_if_exists(conn, "drop table rust_itest_json purge");
    });
}

// ---------------------------------------------------------------------------
// connection pool acquire / release
// ---------------------------------------------------------------------------

/// A real `PoolBackend` that creates blocking `Connection`s against the
/// container. The pool engine drives this synchronously on its background
/// worker thread. The backend is cloneable (shared state behind an `Arc`) so
/// the test can keep observing the create/close counters after handing a clone
/// to the engine.
#[derive(Clone)]
struct LiveBackend {
    options: ConnectOptions,
    created: std::sync::Arc<AtomicU64>,
    closed: std::sync::Arc<AtomicU64>,
}

struct LivePooledConn {
    conn: std::sync::Mutex<Option<Connection>>,
}

impl PoolBackend for LiveBackend {
    type Conn = LivePooledConn;

    fn create_connection(&self, _id: u64, _cclass: Option<&str>) -> Result<Self::Conn, String> {
        let conn = BlockingConnection::connect(self.options.clone()).map_err(|e| e.to_string())?;
        self.created.fetch_add(1, Ordering::SeqCst);
        Ok(LivePooledConn {
            conn: std::sync::Mutex::new(Some(conn)),
        })
    }

    fn ping_connection(&self, conn: &Self::Conn, _ping_timeout_ms: u32) -> bool {
        let mut guard = conn.conn.lock().expect("pooled conn lock");
        match guard.as_mut() {
            Some(c) => BlockingConnection::ping(c).is_ok(),
            None => false,
        }
    }

    fn close_connection(&self, _id: u64, conn: Self::Conn) {
        if let Some(c) = conn.conn.lock().expect("pooled conn lock").take() {
            let _ = BlockingConnection::close(c);
            self.closed.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn connection_is_open(&self, conn: &Self::Conn) -> bool {
        conn.conn
            .lock()
            .expect("pooled conn lock")
            .as_ref()
            .is_some_and(|c| !c.is_dead())
    }
}

#[test]
fn connection_pool_acquire_release() {
    let Some(options) = connect_options() else {
        eprintln!("skipped connection_pool_acquire_release: PYO_TEST_* not configured");
        return;
    };
    let backend = LiveBackend {
        options,
        created: std::sync::Arc::new(AtomicU64::new(0)),
        closed: std::sync::Arc::new(AtomicU64::new(0)),
    };
    let config = PoolConfig {
        min: 1,
        max: 2,
        increment: 1,
        getmode: POOL_GETMODE_WAIT,
        wait_timeout_ms: 10_000,
        timeout_secs: 0,
        max_lifetime_session_secs: 0,
        ping_interval_secs: -1,
        ping_timeout_ms: 5_000,
        creation_cclass: None,
    };
    let pool = Pool::start(backend.clone(), config)
        .expect("pool starts")
        .blocking();

    // acquire two distinct connections up to max
    let a = pool.acquire(AcquireOptions::default()).expect("acquire a");
    let b = pool.acquire(AcquireOptions::default()).expect("acquire b");
    let a_id = a.id();
    let b_id = b.id();
    assert_ne!(
        a_id, b_id,
        "two acquires up to max yield distinct connections"
    );
    assert_eq!(pool.busy_count().expect("busy count"), 2);

    // release one and re-acquire: the freed connection is reused (LIFO)
    a.release_blocking().expect("return a");
    assert_eq!(pool.busy_count().expect("busy count"), 1);
    let c = pool.acquire(AcquireOptions::default()).expect("re-acquire");
    assert_eq!(c.id(), a_id, "released connection is reused");

    b.release_blocking().expect("return b");
    c.release_blocking().expect("return c");
    pool.close(false).expect("close pool");

    assert!(backend.created.load(Ordering::SeqCst) >= 2);
    assert!(backend.closed.load(Ordering::SeqCst) >= 2);
}
