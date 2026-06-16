//! Live test for edition selection (bead jr9). `ConnectOptions::with_edition`
//! sends `AUTH_ORA_EDITION` during authentication (reference messages/auth.pyx),
//! so `SYS_CONTEXT('USERENV','CURRENT_EDITION_NAME')` reflects the chosen edition.
//!
//! Setup (done by the test runner / CI before this runs):
//!   CREATE EDITION e_test;  GRANT USE ON EDITION e_test TO <user>;
//!
//! Run: PYO_TEST_CONNECT_STRING=localhost:1522/FREEPDB1 PYO_TEST_MAIN_USER=pythontest \
//!      PYO_TEST_MAIN_PASSWORD=pythontest \
//!      cargo test -p oracledb --test live_edition -- --ignored --nocapture
use oracledb::protocol::thin::QueryValue;
use oracledb::protocol::ClientIdentity;
use oracledb::{BlockingConnection, ConnectOptions};

fn env() -> (String, String, String) {
    (
        std::env::var("PYO_TEST_CONNECT_STRING").unwrap(),
        std::env::var("PYO_TEST_MAIN_USER").unwrap(),
        std::env::var("PYO_TEST_MAIN_PASSWORD").unwrap(),
    )
}

fn identity() -> ClientIdentity {
    ClientIdentity::new("edition", "host", "user", "term", "rust").unwrap()
}

fn current_edition(conn: &mut oracledb::Connection) -> String {
    let r = BlockingConnection::execute_query(
        conn,
        "select sys_context('USERENV','CURRENT_EDITION_NAME') from dual",
        1,
    )
    .unwrap();
    r.cell(0, 0)
        .and_then(QueryValue::as_text)
        .unwrap_or("")
        .to_string()
}

#[test]
#[ignore]
fn non_default_edition_is_applied() {
    let (cs, user, pw) = env();

    // Control: no edition selected -> the database default (ORA$BASE).
    let mut base =
        BlockingConnection::connect(ConnectOptions::new(&cs, &user, &pw, identity())).unwrap();
    assert_eq!(current_edition(&mut base), "ORA$BASE");
    BlockingConnection::close(base).ok();

    // The feature: select E_TEST -> the session runs under it.
    let mut c = BlockingConnection::connect(
        ConnectOptions::new(&cs, &user, &pw, identity()).with_edition("E_TEST"),
    )
    .expect("connect with edition E_TEST");
    assert_eq!(
        current_edition(&mut c),
        "E_TEST",
        "AUTH_ORA_EDITION must put the session in the selected edition"
    );
    BlockingConnection::close(c).ok();
}

#[test]
#[ignore]
fn invalid_edition_is_a_typed_error() {
    let (cs, user, pw) = env();
    let err = BlockingConnection::connect(
        ConnectOptions::new(&cs, &user, &pw, identity()).with_edition("NO_SUCH_EDITION_XYZ"),
    )
    .expect_err("an unknown edition must fail at connect, not silently succeed");
    // Proves the edition reached the server: it rejects an unknown one
    // (ORA-38802 edition does not exist) with a typed, classifiable code.
    assert!(
        err.ora_code().is_some(),
        "expected a typed ORA error, got: {err}"
    );
    eprintln!("invalid edition -> ORA-{:?}: {err}", err.ora_code());
}
