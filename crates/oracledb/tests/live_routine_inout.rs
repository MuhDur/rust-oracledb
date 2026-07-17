// Assertion-heavy test code intentionally panics on invariant violations.
#![allow(clippy::unwrap_used)]

//! Live IN OUT bind round-trip for [`RoutineCall`] (bead `iec3.1.31`).
//!
//! `RoutineCall::arg_in_out` rides the combined `BindValue::InOut` bind: the
//! driver sends the input value, the server flags the position IN OUT
//! (`TNS_BIND_DIR_INPUT_OUTPUT` = 48) in the IO vector, and the (routine-
//! modified) value is read back into the `RoutineOutcome`. These tests create a
//! session-local procedure with an IN OUT parameter, call it through the typed
//! surface, and assert the returned value — the `p := p * 2` case the bead calls
//! out, plus a VARCHAR case that returns a value longer than the input to prove
//! the OUT slot is sized for the output.
//!
//! Self-skips (returns early) when the `PYO_TEST_*` live environment is not
//! configured, and is `#[ignore]`d so a plain `cargo test` never runs it. Run:
//!   PYO_TEST_CONNECT_STRING=localhost:1522/FREEPDB1 PYO_TEST_MAIN_USER=pythontest \
//!   PYO_TEST_MAIN_PASSWORD=pythontest \
//!   cargo test -p oracledb --test live_routine_inout -- --ignored --nocapture
use oracledb::protocol::ClientIdentity;
use oracledb::{BlockingConnection, ConnectOptions, OutType, RoutineCall};

mod common;

/// `Some(conn)` when the live env is configured, else `None` so the caller
/// returns and the test self-skips.
fn connect() -> Option<oracledb::Connection> {
    let common::LiveCreds {
        connect_string,
        user,
        password,
    } = common::live_creds_opt()?;
    let id = ClientIdentity::new("inout", "host", "user", "term", "rust").unwrap();
    Some(
        BlockingConnection::connect(ConnectOptions::new(connect_string, user, password, id))
            .unwrap(),
    )
}

#[test]
#[ignore]
fn in_out_number_doubles() {
    let Some(mut c) = connect() else {
        return;
    };
    let proc = "rust_inout_double_test";
    BlockingConnection::execute(
        &mut c,
        &format!("CREATE OR REPLACE PROCEDURE {proc}(p IN OUT NUMBER) IS BEGIN p := p * 2; END;"),
        (),
    )
    .unwrap();

    let outcome = BlockingConnection::call_routine(
        &mut c,
        RoutineCall::procedure(proc).arg_in_out(21i64, OutType::Number),
    )
    .unwrap();
    let doubled: Option<i64> = outcome.out_as(0).unwrap();
    assert_eq!(
        doubled,
        Some(42),
        "IN OUT NUMBER read back after p := p * 2"
    );

    BlockingConnection::execute(&mut c, &format!("DROP PROCEDURE {proc}"), ()).ok();
    BlockingConnection::close(c).ok();
}

#[test]
#[ignore]
fn in_out_varchar_returns_value_longer_than_input() {
    let Some(mut c) = connect() else {
        return;
    };
    let proc = "rust_inout_concat_test";
    BlockingConnection::execute(
        &mut c,
        &format!(
            "CREATE OR REPLACE PROCEDURE {proc}(s IN OUT VARCHAR2) IS BEGIN s := s || '-done'; END;"
        ),
        (),
    )
    .unwrap();

    let outcome = BlockingConnection::call_routine(
        &mut c,
        // Input "job" is 3 bytes; the returned "job-done" is 8. The 200-byte OUT
        // slot must hold the longer returned value.
        RoutineCall::procedure(proc).arg_in_out("job", OutType::Varchar { buffer_size: 200 }),
    )
    .unwrap();
    let returned: Option<String> = outcome.out_as(0).unwrap();
    assert_eq!(returned.as_deref(), Some("job-done"));

    BlockingConnection::execute(&mut c, &format!("DROP PROCEDURE {proc}"), ()).ok();
    BlockingConnection::close(c).ok();
}
