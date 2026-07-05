//! Regression guard for the connect-handshake trace (bead
//! `rust-oracledb-connect-trace-mode-vdr0`).
//!
//! Two properties, proven against a live listener:
//!
//!   1. The trace actually emits protocol steps. The field-triage complaint was
//!      *zero* protocol detail, because the trace is gated on
//!      `ORACLEDB_TRACE_CONNECT`, not `RUST_LOG`. This test asserts the
//!      milestones (and a hex-dumped packet) are present when the env var is on.
//!   2. The account password never appears in the trace — neither as ASCII nor
//!      as its hex form — so an operator can safely share a captured handshake.
//!      The password is O5LOGON-encrypted (`generate_verifier`) before it ever
//!      reaches a traced payload, so the plaintext is structurally absent; this
//!      pins that invariant against a future edit. (The token-auth path can't be
//!      live-tested without a token source; it is covered deterministically by
//!      `scripts/check_trace_secret_exclusion.sh`.)
//!
//! Test shape: the parent re-execs the test binary as a child with
//! `ORACLEDB_TRACE_CONNECT=1`, the child performs a real connect (the handshake
//! trace goes to its stderr), and the parent captures that stderr and inspects
//! it. This avoids any unsafe fd redirection (the crate is `forbid(unsafe)`).
//!
//! Live-gated (`#[ignore]`). Run against a lane whose password DIFFERS from the
//! username (else `AUTH_USER` in the trace trivially violates "password
//! absent"). The xe18 lane fits (`testuser` / `testpw`):
//!
//! ```text
//! PYO_TEST_CONNECT_STRING=localhost:1518/XEPDB1 \
//! PYO_TEST_MAIN_USER=testuser PYO_TEST_MAIN_PASSWORD=testpw \
//!   cargo test -p oracledb --test connect_trace_secret -- --ignored --nocapture
//! ```

use std::process::Command;

/// Set on the re-exec'd child so it performs the connect instead of spawning.
const CHILD_ENV: &str = "ORACLEDB_TRACE_SECRET_CHILD";

#[test]
#[ignore = "requires a live listener + PYO_TEST_MAIN_PASSWORD; use a lane whose password != username (e.g. xe18 testuser/testpw)"]
fn password_absent_from_connect_trace() {
    if std::env::var_os(CHILD_ENV).is_some() {
        // Child role: perform the real connect with the trace already enabled by
        // the parent. The handshake trace lands on this process's stderr.
        run_child_connect();
        return;
    }

    let password = std::env::var("PYO_TEST_MAIN_PASSWORD")
        .expect("PYO_TEST_MAIN_PASSWORD must be set for this ignored live test");
    let user = std::env::var("PYO_TEST_MAIN_USER").unwrap_or_else(|_| "testuser".to_string());
    assert_ne!(
        password, user,
        "pick a lane whose password differs from the username, otherwise 'password absent' \
         is trivially violated by AUTH_USER appearing in the trace"
    );

    let exe = std::env::current_exe().expect("current test executable path");
    let output = Command::new(exe)
        .args([
            "--exact",
            "--ignored",
            "--nocapture",
            "password_absent_from_connect_trace",
        ])
        .env(CHILD_ENV, "1")
        .env("ORACLEDB_TRACE_CONNECT", "1")
        .output()
        .expect("spawn child test process");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "child connect failed (status {:?})\n--- child stderr ---\n{stderr}\n--- child stdout ---\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
    );

    // (i) The trace WORKS: protocol milestones are present (the field complaint
    //     was that RUST_LOG=trace produced none of these).
    for needle in [
        "oracledb::connect: tcp connect",
        "oracledb::connect: send CONNECT",
        "oracledb::connect: read ACCEPT",
        "oracledb::connect: ACCEPT", // negotiated-capabilities line (fast-auth visible)
        "oracledb::connect: send AUTH phase one",
        "oracledb::connect: session established",
    ] {
        assert!(
            stderr.contains(needle),
            "expected handshake milestone `{needle}` in the trace; got:\n{stderr}"
        );
    }
    // A hex-dumped packet must be present (packet-level, PYO_DEBUG_PACKETS parity).
    assert!(
        stderr.contains(" hex="),
        "expected at least one hex-dumped packet in the trace:\n{stderr}"
    );

    // (ii) The password is ABSENT — neither its ASCII form nor its hex encoding
    //      appears anywhere in the captured trace.
    assert!(
        !stderr.contains(password.as_str()),
        "SECURITY REGRESSION: plaintext password leaked into the connect trace"
    );
    let password_hex: String = password
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert!(
        !stderr.to_ascii_lowercase().contains(&password_hex),
        "SECURITY REGRESSION: password bytes (hex {password_hex}) leaked into the trace hex dump"
    );
}

/// The child half: open one real connection with the trace on, then close it.
/// A connect+close exercises the entire handshake — CONNECT/ACCEPT, protocol
/// negotiation (or fast auth), and both auth phases — which is exactly the byte
/// range the secret must stay out of.
fn run_child_connect() {
    use asupersync::runtime::{reactor, RuntimeBuilder};
    use asupersync::Cx;
    use oracledb::{ConnectOptions, Connection};
    use oracledb_protocol::ClientIdentity;

    let reactor = reactor::create_reactor().expect("native reactor should build for live I/O");
    let runtime = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("current-thread Asupersync runtime should build");

    runtime.block_on(async {
        let cx = Cx::current().expect("Runtime::block_on should install an ambient Cx");
        let identity = ClientIdentity::new(
            "rust-oracledb",
            "rusthost",
            "rustuser",
            "rustterm",
            "rust-oracledb thn : 0.0.0",
        )
        .expect("test identity should be valid");
        let options = ConnectOptions::new(
            std::env::var("PYO_TEST_CONNECT_STRING")
                .unwrap_or_else(|_| "localhost:1518/XEPDB1".to_string()),
            std::env::var("PYO_TEST_MAIN_USER").unwrap_or_else(|_| "testuser".to_string()),
            std::env::var("PYO_TEST_MAIN_PASSWORD")
                .expect("PYO_TEST_MAIN_PASSWORD must be set for the child connect"),
            identity,
        );
        let conn = Connection::connect(&cx, options)
            .await
            .expect("Rust thin connection should authenticate");
        assert!(conn.session_id() > 0, "server should assign a session id");
        conn.close(&cx)
            .await
            .expect("Rust thin logoff should round-trip");
    });
}
