//! End-to-end smoke test binary for `oracledb`, built to prove the
//! single-static-binary deployment story (musl + `FROM scratch`).
//!
//! It connects to an Oracle listener using the pure-Rust thin driver (no
//! Instant Client, no OCI, no Python interpreter), runs `select 7+5 from dual`
//! plus one small typed query, prints the results, and exits `0` on success or
//! a non-zero code on any failure. Because it goes through
//! [`BlockingConnection`], it is an ordinary synchronous `main` with no visible
//! async runtime.
//!
//! Connection parameters come from the environment, with optional positional
//! CLI overrides:
//!
//! ```text
//! smoke [CONNECT_STRING] [USER] [PASSWORD]
//! ```
//!
//! Environment variables (used when the matching arg is absent):
//! - `PYO_TEST_CONNECT_STRING` — EasyConnect `host:port/service` (default
//!   `localhost:1525/FREEPDB1`)
//! - `PYO_TEST_MAIN_USER` — database user (default `pythontest`)
//! - `PYO_TEST_MAIN_PASSWORD` — database password (default `pythontest`)
//!
//! Run it directly, or via the scratch image (see `docker/Dockerfile.scratch`
//! and `scripts/smoke-static.sh`).

use std::process::ExitCode;

use oracledb::protocol::thin::QueryValue;
use oracledb::protocol::ClientIdentity;
use oracledb::{BlockingConnection, ConnectOptions};

/// Resolve a connection parameter from (in order) a positional CLI argument,
/// an environment variable, then a baked-in default.
fn resolve(arg: Option<String>, env_key: &str, default: &str) -> String {
    arg.or_else(|| std::env::var(env_key).ok())
        .unwrap_or_else(|| default.to_string())
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let connect_string = resolve(
        args.next(),
        "PYO_TEST_CONNECT_STRING",
        "localhost:1525/FREEPDB1",
    );
    let user = resolve(args.next(), "PYO_TEST_MAIN_USER", "pythontest");
    let password = resolve(args.next(), "PYO_TEST_MAIN_PASSWORD", "pythontest");

    // The session identity the database records in v$session. A static binary
    // running in a FROM-scratch container still gets to choose exactly what the
    // DBA sees, regardless of the (empty) container OS environment.
    let identity = ClientIdentity::new(
        "oracledb-smoke",
        "scratch-container",
        "static-binary",
        "musl",
        "rust-oracledb static smoke",
    )?;

    eprintln!("[smoke] connecting to {connect_string} as {user} ...");
    let mut conn = BlockingConnection::connect(ConnectOptions::new(
        connect_string,
        user,
        password,
        identity,
    ))?;
    eprintln!(
        "[smoke] connected: session_id={} serial={}",
        conn.session_id(),
        conn.serial_num()
    );

    // Headline arithmetic round trip: the literal proof the wire protocol works.
    let arithmetic = BlockingConnection::execute_query(&mut conn, "select 7+5 from dual", 1)?;
    let sum = arithmetic
        .cell(0, 0)
        .and_then(QueryValue::as_i64)
        .ok_or("expected an integer result from select 7+5 from dual")?;
    println!("{sum}");
    if sum != 12 {
        return Err(format!("arithmetic check failed: expected 12, got {sum}").into());
    }

    // A small typed query: fetch a VARCHAR2 to exercise text describe + decode.
    let typed = BlockingConnection::execute_query(
        &mut conn,
        "select cast('rust-oracledb' as varchar2(32)) as label from dual",
        1,
    )?;
    let label = typed
        .cell(0, 0)
        .and_then(QueryValue::as_text)
        .ok_or("expected a text result from the typed query")?;
    eprintln!("[smoke] typed query returned label={label:?}");
    if label != "rust-oracledb" {
        return Err(
            format!("typed check failed: expected \"rust-oracledb\", got {label:?}").into(),
        );
    }

    BlockingConnection::close(conn)?;
    eprintln!("[smoke] OK — connected, ran 2 queries, closed cleanly");
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("[smoke] FAILED: {err}");
            ExitCode::FAILURE
        }
    }
}
