//! Golden fixture tests for the connect-descriptor parser
//! (`oracledb_protocol::net::connectstring::parse`).
//!
//! Unlike the live-capture goldens elsewhere in this crate (`dpl_golden.rs`,
//! `pipeline_golden.rs`, ...), nothing here talks to a database or replays a
//! PYO_DEBUG_PACKETS dump: every fixture is a static connect-string literal
//! and every expected value is a Rust struct literal, so the whole suite is
//! deterministic and offline. The point is to pin the *parsed* side of the
//! connect descriptor exactly (full `Descriptor`/`Description`/`Security`
//! equality, not just a substring check) so a parser regression is caught
//! immediately, with a diff that names the drifted field.
//!
//! Companion coverage lives in `crates/oracledb/src/lib.rs` (inline tests,
//! since the descriptor *builder* functions are crate-private there): those
//! pin the outgoing wire bytes the driver builds per auth mode. This file
//! pins the other half of the round trip — parsing a descriptor back into
//! its typed fields — with particular attention to two fields that landed
//! recently:
//!
//!   * **DC2** (`SSL_SERVER_CERT_DN`) — bead F-DC2/F-DC3 fixed a DSN-only
//!     pinned cert DN silently failing to reach the TLS verifier. The DN is
//!     never wrapped/escaped, so this also proves an unquoted, comma-bearing
//!     DN value survives the descriptor tokenizer unmangled (the tokenizer
//!     only special-cases `"`, `(`, `)`, and whitespace-before-first-token).
//!   * **DC4** (`CONNECT_TIMEOUT` / `TRANSPORT_CONNECT_TIMEOUT`) — bead F-DC4
//!     made the descriptor's own connect timeout actually bound a TLS
//!     handshake end to end. This pins the parsed `tcp_connect_timeout`
//!     seconds value exactly, across the canonical key, the
//!     `TRANSPORT_CONNECT_TIMEOUT` alias, unit suffixes (`ms`/`min`), and the
//!     unset default.

use oracledb_protocol::net::connectstring::{
    parse, Address, AddressList, ConnectData, Description, Descriptor, Protocol, Security,
    DEFAULT_TCP_CONNECT_TIMEOUT,
};

fn parse_ok(input: &str) -> Descriptor {
    parse(input)
        .unwrap_or_else(|e| panic!("parse({input:?}) should succeed but failed: {e}"))
        .unwrap_or_else(|| panic!("parse({input:?}) should be a descriptor, not a tns alias"))
}

// ---------------------------------------------------------------------------
// DC2 — SSL_SERVER_CERT_DN / SSL_SERVER_DN_MATCH
// ---------------------------------------------------------------------------

#[test]
fn golden_dc2_ssl_server_cert_dn_and_dn_match_pin_exact_security() {
    let dn_match_on = parse_ok(
        "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(HOST=adb.example.test)(PORT=2484))\
         (CONNECT_DATA=(SERVICE_NAME=adbsvc))\
         (SECURITY=(SSL_SERVER_DN_MATCH=on)\
         (SSL_SERVER_CERT_DN=CN=adb.example.test,O=ExampleCorp,C=US)))",
    );
    assert_eq!(
        dn_match_on.first_description().security,
        Security {
            ssl_server_dn_match: true,
            ssl_server_cert_dn: Some("CN=adb.example.test,O=ExampleCorp,C=US".to_string()),
            wallet_location: None,
            extra: Vec::new(),
        },
        "bead DC2: an unquoted, comma-bearing SSL_SERVER_CERT_DN must parse verbatim \
         alongside an explicit SSL_SERVER_DN_MATCH=on"
    );

    let dn_match_off = parse_ok(
        "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(HOST=adb.example.test)(PORT=2484))\
         (CONNECT_DATA=(SERVICE_NAME=adbsvc))\
         (SECURITY=(SSL_SERVER_DN_MATCH=off)\
         (SSL_SERVER_CERT_DN=CN=adb.example.test,O=ExampleCorp,C=US)))",
    );
    assert_eq!(
        dn_match_off.first_description().security,
        Security {
            ssl_server_dn_match: false,
            ssl_server_cert_dn: Some("CN=adb.example.test,O=ExampleCorp,C=US".to_string()),
            wallet_location: None,
            extra: Vec::new(),
        },
        "bead DC2: SSL_SERVER_DN_MATCH=off must be honored alongside a pinned cert DN"
    );

    let no_cert_dn = parse_ok(
        "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(HOST=adb.example.test)(PORT=2484))\
         (CONNECT_DATA=(SERVICE_NAME=adbsvc)))",
    );
    assert_eq!(
        no_cert_dn.first_description().security,
        Security::default(),
        "bead DC2: no SECURITY clause at all must leave ssl_server_cert_dn unset and \
         ssl_server_dn_match at its true default"
    );
}

// ---------------------------------------------------------------------------
// DC4 — CONNECT_TIMEOUT / TRANSPORT_CONNECT_TIMEOUT
// ---------------------------------------------------------------------------

#[test]
fn golden_dc4_connect_timeout_parses_to_exact_seconds() {
    let explicit_seconds = parse_ok(
        "(DESCRIPTION=(CONNECT_TIMEOUT=7.5)\
         (ADDRESS=(PROTOCOL=tcps)(HOST=h)(PORT=1))(CONNECT_DATA=(SERVICE_NAME=s)))",
    );
    assert!(
        (explicit_seconds.first_description().tcp_connect_timeout - 7.5).abs() < 1e-9,
        "bead DC4: CONNECT_TIMEOUT=7.5 must parse to exactly 7.5 seconds, got {}",
        explicit_seconds.first_description().tcp_connect_timeout
    );

    let ms_alias = parse_ok(
        "(DESCRIPTION=(TRANSPORT_CONNECT_TIMEOUT=250ms)\
         (ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1))(CONNECT_DATA=(SERVICE_NAME=s)))",
    );
    assert!(
        (ms_alias.first_description().tcp_connect_timeout - 0.25).abs() < 1e-9,
        "bead DC4: TRANSPORT_CONNECT_TIMEOUT=250ms must normalize to 0.25 seconds, got {}",
        ms_alias.first_description().tcp_connect_timeout
    );

    let connect_timeout_minutes = parse_ok(
        "(DESCRIPTION=(CONNECT_TIMEOUT=2min)\
         (ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1))(CONNECT_DATA=(SERVICE_NAME=s)))",
    );
    assert!(
        (connect_timeout_minutes
            .first_description()
            .tcp_connect_timeout
            - 120.0)
            .abs()
            < 1e-9,
        "bead DC4/F1: CONNECT_TIMEOUT=2min (the driver's connect_timeout alias to \
         tcp_connect_timeout) must normalize to 120 seconds, got {}",
        connect_timeout_minutes
            .first_description()
            .tcp_connect_timeout
    );

    let unset = parse_ok(
        "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1))(CONNECT_DATA=(SERVICE_NAME=s)))",
    );
    assert!(
        (unset.first_description().tcp_connect_timeout - DEFAULT_TCP_CONNECT_TIMEOUT).abs() < 1e-9,
        "bead DC4: an absent CONNECT_TIMEOUT must fall back to DEFAULT_TCP_CONNECT_TIMEOUT, got {}",
        unset.first_description().tcp_connect_timeout
    );
}

// ---------------------------------------------------------------------------
// Per-auth-mode topology goldens (parser side): password/plain TCP,
// wallet-TCPS, and the IAM-token-style TCPS shape (OCI_IAM_HOST passthrough
// plus SSL_SERVER_DN_MATCH=off — TOKEN_AUTH itself is injected by the driver
// at build time, never present in an inbound DSN, so the parser-side golden
// pins the DSN shape an IAM/ADB connect string actually carries).
// ---------------------------------------------------------------------------

#[test]
fn golden_password_plain_tcp_topology_pins_full_descriptor() {
    let parsed = parse_ok(
        "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=db.example.test)(PORT=1521))\
         (CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))",
    );
    let expected = Descriptor {
        descriptions: vec![Description {
            address_lists: vec![AddressList {
                addresses: vec![Address {
                    host: Some("db.example.test".to_string()),
                    port: 1521,
                    protocol: Protocol::Tcp,
                    ..Address::default()
                }],
                failover: true,
                ..AddressList::default()
            }],
            connect_data: ConnectData {
                service_name: Some("FREEPDB1".to_string()),
                ..ConnectData::default()
            },
            ..Description::default()
        }],
        load_balance: false,
        failover: true,
        source_route: false,
    };
    assert_eq!(
        parsed, expected,
        "password/plain-TCP connect descriptor topology drifted from the golden fixture"
    );
}

#[test]
fn golden_wallet_tcps_topology_with_cert_dn_pins_full_descriptor() {
    let parsed = parse_ok(
        "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(HOST=adb.example.test)(PORT=2484))\
         (CONNECT_DATA=(SERVICE_NAME=adbsvc))\
         (SECURITY=(SSL_SERVER_DN_MATCH=on)\
         (SSL_SERVER_CERT_DN=CN=adb.example.test,O=ExampleCorp,C=US)\
         (MY_WALLET_DIRECTORY=/opt/wallets/adb)))",
    );
    let expected = Descriptor {
        descriptions: vec![Description {
            address_lists: vec![AddressList {
                addresses: vec![Address {
                    host: Some("adb.example.test".to_string()),
                    port: 2484,
                    protocol: Protocol::Tcps,
                    ..Address::default()
                }],
                failover: true,
                ..AddressList::default()
            }],
            connect_data: ConnectData {
                service_name: Some("adbsvc".to_string()),
                ..ConnectData::default()
            },
            security: Security {
                ssl_server_dn_match: true,
                ssl_server_cert_dn: Some("CN=adb.example.test,O=ExampleCorp,C=US".to_string()),
                wallet_location: Some("/opt/wallets/adb".to_string()),
                extra: Vec::new(),
            },
            ..Description::default()
        }],
        load_balance: false,
        failover: true,
        source_route: false,
    };
    assert_eq!(
        parsed, expected,
        "wallet-TCPS connect descriptor topology (incl. DC2 cert DN + MY_WALLET_DIRECTORY -> \
         wallet_location aliasing) drifted from the golden fixture"
    );
}

#[test]
fn golden_iam_token_style_tcps_topology_pins_oci_iam_host_passthrough() {
    let parsed = parse_ok(
        "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(HOST=adb.example.test)(PORT=2484))\
         (CONNECT_DATA=(SERVICE_NAME=adbsvc))\
         (SECURITY=(SSL_SERVER_DN_MATCH=off)(OCI_IAM_HOST=private-endpoint)))",
    );
    let expected = Descriptor {
        descriptions: vec![Description {
            address_lists: vec![AddressList {
                addresses: vec![Address {
                    host: Some("adb.example.test".to_string()),
                    port: 2484,
                    protocol: Protocol::Tcps,
                    ..Address::default()
                }],
                failover: true,
                ..AddressList::default()
            }],
            connect_data: ConnectData {
                service_name: Some("adbsvc".to_string()),
                ..ConnectData::default()
            },
            security: Security {
                ssl_server_dn_match: false,
                ssl_server_cert_dn: None,
                wallet_location: None,
                extra: vec![("OCI_IAM_HOST".to_string(), "private-endpoint".to_string())],
            },
            ..Description::default()
        }],
        load_balance: false,
        failover: true,
        source_route: false,
    };
    assert_eq!(
        parsed, expected,
        "IAM-token-style TCPS connect descriptor topology (OCI_IAM_HOST passthrough, \
         SSL_SERVER_DN_MATCH=off, no cert DN) drifted from the golden fixture — the driver \
         injects TOKEN_AUTH=OCI_TOKEN itself at build time, so it never appears in the DSN"
    );
}
