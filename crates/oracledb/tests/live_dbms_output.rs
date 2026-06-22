//! Live test for the DBMS_OUTPUT capture helper (bead acj). Verifies the helper
//! enables buffering, captures lines emitted on the SAME session, bounds output
//! by max_lines (setting `truncated`), and drains cleanly to the end.
//!
//! Run: PYO_TEST_CONNECT_STRING=localhost:1522/FREEPDB1 PYO_TEST_MAIN_USER=pythontest \
//!      PYO_TEST_MAIN_PASSWORD=pythontest \
//!      cargo test -p oracledb --test live_dbms_output -- --ignored --nocapture
use oracledb::protocol::ClientIdentity;
use oracledb::{BlockingConnection, ConnectOptions};

fn connect() -> oracledb::Connection {
    let cs = std::env::var("PYO_TEST_CONNECT_STRING").unwrap();
    let user = std::env::var("PYO_TEST_MAIN_USER").unwrap();
    let pw = std::env::var("PYO_TEST_MAIN_PASSWORD").unwrap();
    let id = ClientIdentity::new("dbmsout", "host", "user", "term", "rust").unwrap();
    BlockingConnection::connect(ConnectOptions::new(cs, user, pw, id)).unwrap()
}

#[test]
#[ignore]
fn dbms_output_capture_roundtrip() {
    let mut c = connect();
    BlockingConnection::enable_dbms_output(&mut c, Some(20000)).unwrap();

    // Emit three lines from this exact session.
    BlockingConnection::execute(
        &mut c,
        "begin dbms_output.put_line('alpha'); \
         dbms_output.put_line('beta'); \
         dbms_output.put_line('gamma'); end;",
        (),
    )
    .unwrap();

    let out = BlockingConnection::read_dbms_output(&mut c, 1000, 100_000).unwrap();
    assert_eq!(out.lines(), vec!["alpha", "beta", "gamma"]);
    assert_eq!(out.line_count(), 3);
    assert_eq!(out.char_count(), 14); // 5 + 4 + 5
    assert!(!out.truncated(), "drained to the end");

    // The buffer is now consumed; a second read yields nothing.
    let empty = BlockingConnection::read_dbms_output(&mut c, 1000, 100_000).unwrap();
    assert!(empty.lines().is_empty() && !empty.truncated());

    BlockingConnection::close(c).ok();
}

#[test]
#[ignore]
fn dbms_output_respects_max_lines() {
    let mut c = connect();
    BlockingConnection::enable_dbms_output(&mut c, None).unwrap();
    BlockingConnection::execute(
        &mut c,
        "begin for i in 1..10 loop dbms_output.put_line('line ' || i); end loop; end;",
        (),
    )
    .unwrap();

    let out = BlockingConnection::read_dbms_output(&mut c, 3, 100_000).unwrap();
    assert_eq!(out.lines().len(), 3);
    assert_eq!(out.lines()[0], "line 1");
    assert!(out.truncated(), "7 lines remained buffered -> truncated");

    BlockingConnection::close(c).ok();
}

/// Output that ends *exactly* at `max_lines` must report `truncated == false`:
/// the read drained the buffer, nothing remained. (Regression guard for the
/// top-of-loop check that used to over-report truncation at the boundary.)
#[test]
#[ignore]
fn dbms_output_exact_boundary_is_not_truncated() {
    let mut c = connect();
    BlockingConnection::enable_dbms_output(&mut c, None).unwrap();
    BlockingConnection::execute(
        &mut c,
        "begin for i in 1..5 loop dbms_output.put_line('row ' || i); end loop; end;",
        (),
    )
    .unwrap();

    // Exactly 5 lines buffered, asked for exactly 5 -> drained, not truncated.
    let out = BlockingConnection::read_dbms_output(&mut c, 5, 100_000).unwrap();
    assert_eq!(out.lines().len(), 5);
    assert!(
        !out.truncated(),
        "buffer drained exactly at max_lines must not report truncated"
    );

    BlockingConnection::close(c).ok();
}
