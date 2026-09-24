//! Regression guard for the connect-handshake trace (bead
//! `rust-oracledb-connect-trace-mode-vdr0`).
//!
//! Two properties, proven against a live listener:
//!
//!   1. Structured connect milestones are emitted under
//!      `ORACLEDB_TRACE_CONNECT=1`, independent of `RUST_LOG`.
//!   2. Default traces omit all payload bytes, so password, user, and token
//!      material cannot be recovered from an AUTH hex dump.
//!
//! Test shape: the parent re-execs the test binary as a child with
//! `ORACLEDB_TRACE_CONNECT=1`, the child performs a real connect (the handshake
//! trace goes to its stderr), and the parent captures that stderr and inspects
//! it. This avoids any unsafe fd redirection (the crate is `forbid(unsafe)`).
//!
//! Live-gated (`#[ignore]`). Defaults to the local FREE23 lane; environment
//! overrides can target another configured lab lane:
//!
//! ```text
//! PYO_TEST_CONNECT_STRING=localhost:1522/FREEPDB1 \
//! PYO_TEST_MAIN_USER=pythontest PYO_TEST_MAIN_PASSWORD=pythontest \
//!   cargo test -p oracledb --test connect_trace_secret -- --ignored --nocapture
//! ```

extern crate oraclemcp_driver_cx as oracledb;

mod common;

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

    let password = common::live_password_or(common::FREE23_PASSWORD);
    let user = common::live_user_or(common::FREE23_USER);

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
        "step=tcp connect",
        "step=send CONNECT",
        "step=read ACCEPT",
        "step=ACCEPT capabilities",
        "step=send AUTH phase one",
        "step=session established",
    ] {
        assert!(
            stderr.contains(needle),
            "expected handshake milestone `{needle}` in the trace; got:\n{stderr}"
        );
    }
    let ordered_phases = [
        "phase=dns",
        "phase=tcp",
        "phase=connect",
        "phase=accept",
        "phase=auth_phase_one",
        "phase=auth_phase_two",
        "phase=session",
    ];
    let mut previous = None;
    for phase in ordered_phases {
        let position = stderr
            .find(phase)
            .unwrap_or_else(|| panic!("missing structured phase `{phase}` in trace:\n{stderr}"));
        assert!(
            previous.is_none_or(|prior| prior < position),
            "connect phases were out of order: {stderr}"
        );
        previous = Some(position);
    }
    assert!(
        !stderr.contains(" hex="),
        "default trace must omit payload hex"
    );

    // (ii) The configured password is absent from the default trace.
    assert!(
        !stderr.contains(password.as_str()),
        "SECURITY REGRESSION: plaintext password leaked into the connect trace"
    );
    assert!(
        !stderr.contains(user.as_str()),
        "SECURITY REGRESSION: username leaked into the connect trace"
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
            common::live_conn_string_or(common::FREE23_CONNECT_STRING),
            common::live_user_or(common::FREE23_USER),
            common::live_password_or(common::FREE23_PASSWORD),
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
