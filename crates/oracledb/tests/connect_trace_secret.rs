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
const CONNECT_CANARY_USER: &str = "creamleopard-connect-user-canary";
const CONNECT_CANARY_PASSWORD: &str = "creamleopard-connect-password-canary";
const CONNECT_CANARY_TOKEN: &str = "creamleopard-connect-token-canary";

/// Drive the real Connection::connect auth path against a loopback listener,
/// while a child process lets the parent inspect the actual stderr bytes.
/// A second ConnectOptions value plants an access token and verifies the
/// driver's fail-closed refusal over plain TCP without printing the token.
#[test]
fn connect_trace_redacts_secrets_on_the_real_connect_path() {
    if let Some(mode) = std::env::var_os(CHILD_ENV) {
        run_loopback_auth_and_token_refusal();
        let _ = mode;
        return;
    }

    for mode in ["1", "raw"] {
        let output = Command::new(std::env::current_exe().expect("test executable"))
            .args([
                "--exact",
                "connect_trace_redacts_secrets_on_the_real_connect_path",
                "--nocapture",
            ])
            .env(CHILD_ENV, mode)
            .env("ORACLEDB_TRACE_CONNECT", mode)
            .output()
            .expect("spawn loopback connect child");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "loopback connect child failed: {:?}\n{stderr}\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout)
        );

        if mode == "1" {
            for canary in [
                CONNECT_CANARY_USER,
                CONNECT_CANARY_PASSWORD,
                CONNECT_CANARY_TOKEN,
            ] {
                assert!(
                    !stderr.contains(canary),
                    "default real-connect trace leaked {canary}:\n{stderr}"
                );
                let encoded = canary
                    .as_bytes()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>();
                assert!(
                    !stderr.contains(&encoded),
                    "default real-connect trace leaked encoded {canary}:\n{stderr}"
                );
            }
            assert!(
                !stderr.contains(" hex="),
                "default mode emitted raw AUTH bytes"
            );
        } else {
            let mut warning_for_next_hex = false;
            let mut raw_auth_lines = 0;
            for line in stderr.lines() {
                if line.contains("WARNING raw trace includes credential-derived material") {
                    warning_for_next_hex = true;
                }
                if line.contains(" hex=") {
                    assert!(
                        warning_for_next_hex,
                        "raw AUTH bytes appeared before their warning: {line}\n{stderr}"
                    );
                    warning_for_next_hex = false;
                    raw_auth_lines += 1;
                }
            }
            assert!(
                raw_auth_lines >= 2,
                "expected real phase-one and phase-two AUTH bytes:\n{stderr}"
            );
        }
    }
}

fn run_loopback_auth_and_token_refusal() {
    use asupersync::runtime::{reactor, RuntimeBuilder};
    use asupersync::Cx;
    use oracledb::{ConnectOptions, Connection};
    use oracledb_protocol::wire::{encode_packet, PacketLengthWidth};
    use oracledb_protocol::{
        thin::{TNS_DATA_FLAGS_END_OF_RESPONSE, TNS_PACKET_TYPE_ACCEPT, TNS_PACKET_TYPE_DATA},
        ClientIdentity,
    };
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Duration;

    fn read_packet(socket: &mut std::net::TcpStream, large: bool) -> std::io::Result<()> {
        let mut header = [0u8; 8];
        socket.read_exact(&mut header)?;
        let declared = if large {
            usize::try_from(u32::from_be_bytes(
                header[..4].try_into().expect("four bytes"),
            ))
            .unwrap_or(usize::MAX)
        } else {
            usize::from(u16::from_be_bytes([header[0], header[1]]))
        };
        let mut payload = vec![0; declared.saturating_sub(header.len())];
        socket.read_exact(&mut payload)
    }

    fn golden(hex: &str) -> Vec<u8> {
        let digits: Vec<_> = hex
            .bytes()
            .filter(|byte| !byte.is_ascii_whitespace())
            .collect();
        digits
            .chunks_exact(2)
            .map(|pair| {
                let high = (pair[0] as char).to_digit(16).expect("hex digit");
                let low = (pair[1] as char).to_digit(16).expect("hex digit");
                ((high << 4) | low) as u8
            })
            .collect()
    }

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind synthetic listener");
    let addr = listener.local_addr().expect("listener address");
    let server = std::thread::spawn(move || -> std::io::Result<()> {
        let (mut socket, _) = listener.accept()?;
        socket.set_read_timeout(Some(Duration::from_secs(5)))?;
        read_packet(&mut socket, false)?; // CONNECT
        let accept = encode_packet(
            TNS_PACKET_TYPE_ACCEPT,
            0,
            None,
            &golden(include_str!(
                "../../oracledb-protocol/tests/golden/free23_accept_payload.hex"
            )),
            PacketLengthWidth::Legacy16,
        )
        .expect("encode synthetic ACCEPT");
        socket.write_all(&accept)?;
        read_packet(&mut socket, true)?; // AUTH phase one
        let auth_one = encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(TNS_DATA_FLAGS_END_OF_RESPONSE),
            &golden(include_str!(
                "../../oracledb-protocol/tests/golden/pre23ai_xe18_auth_phase_one_response.hex"
            )),
            PacketLengthWidth::Large32,
        )
        .expect("encode synthetic AUTH challenge");
        socket.write_all(&auth_one)?;
        read_packet(&mut socket, true)?; // AUTH phase two
        Ok(()) // close before a server response
    });

    let reactor = reactor::create_reactor().expect("native reactor");
    let runtime = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("Asupersync runtime");
    runtime.block_on(async {
        let cx = Cx::current().expect("ambient Cx");
        let identity = || {
            ClientIdentity::new(
                "rust-oracledb",
                "synthetic-host",
                "synthetic-osuser",
                "synthetic-terminal",
                "rust-oracledb test",
            )
            .expect("synthetic identity")
        };
        let options = ConnectOptions::new(
            format!(
                "127.0.0.1:{}/FREEPDB1?transport_connect_timeout=2",
                addr.port()
            ),
            CONNECT_CANARY_USER,
            CONNECT_CANARY_PASSWORD,
            identity(),
        );
        assert!(Connection::connect(&cx, options).await.is_err());

        let token_options = ConnectOptions::new(
            "127.0.0.1:1/FREEPDB1?transport_connect_timeout=1",
            CONNECT_CANARY_USER,
            CONNECT_CANARY_PASSWORD,
            identity(),
        )
        .with_access_token(CONNECT_CANARY_TOKEN);
        assert!(Connection::connect(&cx, token_options).await.is_err());
    });
    server
        .join()
        .expect("loopback peer joins")
        .expect("wire exchange");
}

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
    eprintln!(
        "live connect trace reached phases: {}",
        ordered_phases.join(" -> ")
    );
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
