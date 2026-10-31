//! Live TCPS regression (oraclemcp-t44cz).
//!
//! An Oracle listener that hands a TCPS session to a dedicated server process
//! answers CONNECT with RESEND carrying the TLS-renegotiation packet flag, and
//! the server process then starts a fresh TLS handshake on the same socket. A
//! client that keeps its first TLS session fails at ACCEPT with "cannot decrypt
//! peer's message".
//!
//! Runs when `ORACLEDB_E2E_TCPS_CONNECT_STRING`, `ORACLEDB_E2E_TCPS_USER` and
//! `ORACLEDB_E2E_TCPS_PASSWORD` are set (plus `ORACLEDB_E2E_TCPS_WALLET_LOCATION`
//! for a private CA); otherwise it prints a SKIP, and with
//! `ORACLEDB_E2E_REQUIRE_TCPS=1` an incomplete environment is a failure.
//!
//! ```text
//! cargo test -p oracledb --test tcps_renegotiation_live -- --nocapture
//! ```

extern crate oraclemcp_driver_cx as oracledb;

use std::time::{Duration, Instant};

use oracledb::protocol::ClientIdentity;
use oracledb::{BlockingConnection, ConnectOptions};

const PROGRAM: &str = "rust-oracledb-t44cz";
const MACHINE: &str = "t44cz-machine";
const OSUSER: &str = "t44cz-osuser";
const TERMINAL: &str = "t44cz-terminal";
const DRIVER: &str = "rust-oracledb thn : 0.0.0";

fn tcps_options() -> Option<ConnectOptions> {
    let names = [
        "ORACLEDB_E2E_TCPS_CONNECT_STRING",
        "ORACLEDB_E2E_TCPS_USER",
        "ORACLEDB_E2E_TCPS_PASSWORD",
    ];
    let values: Vec<String> = names
        .iter()
        .filter_map(|name| std::env::var(name).ok().filter(|value| !value.is_empty()))
        .collect();
    if values.len() != names.len() {
        assert_ne!(
            std::env::var("ORACLEDB_E2E_REQUIRE_TCPS").as_deref(),
            Ok("1"),
            "ORACLEDB_E2E_REQUIRE_TCPS=1 but {} is incomplete",
            names.join("/")
        );
        eprintln!(
            "[t44cz] SKIP: set {} to run the live TCPS regression",
            names.join(", ")
        );
        return None;
    }
    let identity = ClientIdentity::new(PROGRAM, MACHINE, OSUSER, TERMINAL, DRIVER)
        .expect("valid client identity");
    let mut options = ConnectOptions::new(
        values[0].clone(),
        values[1].clone(),
        values[2].clone(),
        identity,
    );
    if let Ok(wallet) = std::env::var("ORACLEDB_E2E_TCPS_WALLET_LOCATION") {
        options = options.with_wallet_location(wallet);
    }
    Some(options)
}

#[test]
fn tcps_connect_completes_when_the_server_renegotiates_tls() {
    let Some(options) = tcps_options() else {
        return;
    };
    let started = Instant::now();
    let mut conn = BlockingConnection::connect(options)
        .expect("TCPS connect must complete after the server's TLS renegotiation");
    let row = BlockingConnection::query_one(
        &mut conn,
        "select sys_context('USERENV','NETWORK_PROTOCOL') from dual",
        (),
    )
    .expect("query on the renegotiated TCPS session");
    assert_eq!(
        row.get::<String>(0)
            .expect("protocol text")
            .to_ascii_lowercase(),
        "tcps",
        "the session must really run over TCPS"
    );
    BlockingConnection::close(conn).expect("close TCPS connection");
    assert!(
        started.elapsed() < Duration::from_secs(60),
        "TCPS connect took {:?}",
        started.elapsed()
    );
}
