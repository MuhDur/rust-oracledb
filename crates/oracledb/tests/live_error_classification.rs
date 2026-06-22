//! Live integration test for the structured error taxonomy (bead 7bo).
//!
//! Triggers real ORA errors against the container and asserts the public
//! classification accessors ([`Error::ora_code`], [`Error::offset`],
//! [`Error::is_transient`], [`Error::is_connection_lost`],
//! [`Error::is_retryable`]) report what python-oracledb only gives you as a
//! bare `.code` int.
//!
//! Self-skips cleanly when the container environment is absent. Run with:
//!
//! ```sh
//! eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1525 \
//!         ORACLEDB_HOST_PORT=1525 scripts/container.sh env)"
//! cargo test -p oracledb --test live_error_classification -- --nocapture
//! ```

use oracledb::{BlockingConnection, ConnectOptions, Connection, Error};
use oracledb_protocol::ClientIdentity;

const PROGRAM: &str = "rust-oracledb-err-itest";
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
) -> oracledb::Result<oracledb::protocol::thin::QueryResult> {
    BlockingConnection::execute_raw(
        conn,
        sql,
        prefetch_rows,
        &[],
        oracledb::protocol::thin::ExecuteOptions::default(),
        None,
    )
}

/// ORA-00942 (table or view does not exist) is a *permanent* error: not
/// transient, not connection-lost, not retryable. The accessors must report
/// the code and (for a parse error) a non-zero offset.
#[test]
fn missing_object_is_permanent_and_carries_code() {
    with_connection("missing_object_is_permanent_and_carries_code", |conn| {
        let err = execute_raw(conn, "select * from a_table_that_does_not_exist_42", 1)
            .expect_err("selecting a missing table must error");

        eprintln!(
            "ORA error: code={:?} offset={:?} msg={err}",
            err.ora_code(),
            err.offset()
        );

        assert_eq!(err.ora_code(), Some(942), "ORA-00942 code surfaced");
        // a parse error reports the 1-based offset of the offending token
        assert!(
            err.offset().is_some(),
            "parse error should carry a server offset"
        );
        assert!(!err.is_transient(), "ORA-00942 is not transient");
        assert!(
            !err.is_connection_lost(),
            "ORA-00942 is not connection-lost"
        );
        assert!(!err.is_retryable(), "ORA-00942 is permanent — do not retry");
    });
}

/// ORA-00904 (invalid identifier) is likewise a permanent parse error; it must
/// surface its code and not be classified as retryable.
#[test]
fn invalid_identifier_is_permanent() {
    with_connection("invalid_identifier_is_permanent", |conn| {
        let err = execute_raw(conn, "select not_a_column from dual", 1)
            .expect_err("selecting a missing column must error");
        eprintln!(
            "ORA error: code={:?} offset={:?} msg={err}",
            err.ora_code(),
            err.offset()
        );
        assert_eq!(err.ora_code(), Some(904), "ORA-00904 code surfaced");
        assert!(!err.is_retryable(), "ORA-00904 is permanent");
    });
}

/// ORA-00001 (unique constraint violated) is a permanent application error:
/// the classification must not flag it for retry (retrying the same insert
/// would just fail again).
#[test]
fn unique_violation_is_permanent() {
    with_connection("unique_violation_is_permanent", |conn| {
        let _ = execute_raw(conn, "drop table err_uq_t", 1);
        execute_raw(conn, "create table err_uq_t (id number primary key)", 1)
            .expect("create table");
        execute_raw(conn, "insert into err_uq_t values (1)", 1).expect("first insert");

        let err = execute_raw(conn, "insert into err_uq_t values (1)", 1)
            .expect_err("duplicate key must error");
        eprintln!("ORA error: code={:?} msg={err}", err.ora_code());
        assert_eq!(err.ora_code(), Some(1), "ORA-00001 code surfaced");
        assert!(
            !err.is_retryable(),
            "ORA-00001 unique violation is permanent"
        );

        let _ = execute_raw(conn, "drop table err_uq_t", 1);
    });
}

/// Sanity check that a deliberately divided-by-zero (ORA-01476) error is
/// reported with its code through the taxonomy, exercising the structured path
/// end to end against the real server.
#[test]
fn divide_by_zero_reports_code() {
    with_connection("divide_by_zero_reports_code", |conn| {
        let err =
            execute_raw(conn, "select 1/0 from dual", 1).expect_err("divide by zero must error");
        eprintln!("ORA error: code={:?} msg={err}", err.ora_code());
        assert_eq!(err.ora_code(), Some(1476), "ORA-01476 code surfaced");
        // a downstream caller can branch on the taxonomy without substring matching
        let _: bool = err.is_retryable();
    });
}

/// Confirm an `Error` constructed from a curated transient code classifies as
/// transient + retryable (no DB needed for the classification itself, but we
/// run it inside the live suite so the taxonomy is exercised alongside the real
/// errors above). Uses a real ORA-00060 deadlock message shape.
#[test]
fn curated_transient_code_classifies() {
    // This does not require a connection; it asserts the public taxonomy on a
    // representative server-error message.
    let err = Error::Protocol(oracledb_protocol::ProtocolError::ServerError(
        "ORA-00060: deadlock detected while waiting for resource".to_string(),
    ));
    assert_eq!(err.ora_code(), Some(60));
    assert!(err.is_transient());
    assert!(err.is_retryable());
    assert!(!err.is_connection_lost());
}
