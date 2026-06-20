//! Live connect-string integration test (bead rust-oracledb-hk6).
//!
//! Proves the real connect-string parser is load-bearing end-to-end: it parses
//! BOTH a full TNS connect descriptor (`(DESCRIPTION=(ADDRESS=...)...)`) AND an
//! EZConnect string derived from the same listener, then actually CONNECTs with
//! each and runs `select 7 + 5 from dual` expecting 12.
//!
//! Self-skips cleanly when the container environment is absent. Run with:
//!
//! ```sh
//! eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1527 \
//!         ORACLEDB_HOST_PORT=1527 scripts/container.sh env)"
//! cargo test -p oracledb --test live_connect_string -- --nocapture
//! ```

use oracledb::{BlockingConnection, ConnectOptions, Connection};
use oracledb_protocol::net::EasyConnect;
use oracledb_protocol::ClientIdentity;

const PROGRAM: &str = "rust-oracledb-hk6-itest";
const MACHINE: &str = "itest-machine";
const OSUSER: &str = "itest-osuser";
const TERMINAL: &str = "itest-terminal";
const DRIVER: &str = "rust-oracledb thn : 0.0.0";

fn identity() -> Option<ClientIdentity> {
    ClientIdentity::new(PROGRAM, MACHINE, OSUSER, TERMINAL, DRIVER).ok()
}

/// Connects with `connect_string`, runs `select 7 + 5 from dual`, asserts 12.
fn connect_and_add(label: &str, connect_string: String) {
    let (Some(user), Some(password), Some(identity)) = (
        std::env::var("PYO_TEST_MAIN_USER").ok(),
        std::env::var("PYO_TEST_MAIN_PASSWORD").ok(),
        identity(),
    ) else {
        eprintln!("skipped {label}: PYO_TEST_* environment not configured");
        return;
    };

    // Confirm the string parses through the real parser before connecting, and
    // print the resolved topology for troubleshooting evidence.
    let descriptor = EasyConnect::parse_descriptor(&connect_string)
        .unwrap_or_else(|e| panic!("{label}: connect string {connect_string:?} must parse: {e}"));
    eprintln!("{label} resolved:\n{}", descriptor.describe());

    let options = ConnectOptions::new(connect_string, user, password, identity);
    let mut conn: Connection =
        BlockingConnection::connect(options).unwrap_or_else(|e| panic!("{label}: connect: {e:?}"));
    let row = BlockingConnection::query_one(&mut conn, "select 7 + 5 from dual", ())
        .unwrap_or_else(|e| panic!("{label}: query: {e:?}"));
    let sum: i64 = row.get(0).expect("typed get i64");
    assert_eq!(sum, 12, "{label}: 7 + 5 should be 12");
    BlockingConnection::close(conn).expect("close connection");
    eprintln!("{label}: 7 + 5 = {sum} OK");
}

#[test]
fn live_connect_with_full_description_and_easy_connect() {
    let Some(env_cs) = std::env::var("PYO_TEST_CONNECT_STRING").ok() else {
        eprintln!("skipped: PYO_TEST_CONNECT_STRING not set");
        return;
    };

    // Derive the listener's host/port/service from the lane connect string.
    let resolved = EasyConnect::parse(&env_cs)
        .unwrap_or_else(|e| panic!("lane connect string {env_cs:?} must parse: {e}"));

    // Form 1: an EZConnect string (host:port/service).
    let easy_connect = format!(
        "{}:{}/{}",
        resolved.host, resolved.port, resolved.service_name
    );

    // Form 2: a full TNS connect descriptor for the same listener.
    let description = format!(
        "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST={})(PORT={}))\
         (CONNECT_DATA=(SERVICE_NAME={})))",
        resolved.host, resolved.port, resolved.service_name
    );

    connect_and_add("ezconnect", easy_connect);
    connect_and_add("full-description", description);
}
