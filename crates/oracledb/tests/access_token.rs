//! Tests for access-token authentication (bead 5bh): secret redaction (always)
//! and the TLS-required guard (live, env-gated).
//!
//! Successful token auth needs an OCI/ADB endpoint that accepts IAM/OAuth tokens,
//! which this suite has no access to; the wire encoding is pinned separately by
//! the deterministic `token_auth_tests` cassette in `oracledb-protocol`. Here we
//! prove the two client-side guarantees we *can* prove end to end: the token is
//! never rendered, and using one over plain TCP fails with a precise typed error.
//!
//! Live guard run: PYO_TEST_CONNECT_STRING=localhost:1522/FREEPDB1 \
//!   PYO_TEST_MAIN_USER=pythontest PYO_TEST_MAIN_PASSWORD=pythontest \
//!   cargo test -p oracledb --test access_token -- --ignored --nocapture
use oracledb::protocol::ClientIdentity;
use oracledb::{
    AccessToken, AuthModeKind, AuthModeSupport, BlockingConnection, ConnectOptions, Error,
    IamPrivateKey,
};

// A synthetic PEM-looking sentinel used only to prove redaction; it is never
// parsed or sent here (the real signing path is exercised offline by the
// protocol crate's OpenSSL signature-vector test). NEVER real OCI key material.
const IAM_PRIVATE_KEY_REDACTION_VALUE: &str =
    "-----BEGIN PRIVATE KEY-----REDACTION-SENTINEL-KEY-----END PRIVATE KEY-----";

const OPAQUE_AUTH_VALUE: &str = "opaque-redaction-value";
const LOGIN_REDACTION_VALUE: &str = "login-redaction-value";
const WALLET_REDACTION_VALUE: &str = "wallet-redaction-value";
const SAMPLE_WALLET_PATH: &str = "/secure/wallet/redaction-location";
const SAMPLE_CERT_DN: &str = "CN=redaction-db";
const SAMPLE_KERBEROS_PRINCIPAL: &str = "service/redaction-host@EXAMPLE.COM";
const SAMPLE_KERBEROS_KEYTAB: &str = "/secure/keytabs/redaction.keytab";
const SAMPLE_RADIUS_CHALLENGE: &str = "redaction-radius-challenge";

#[test]
fn access_token_is_redacted_in_debug() {
    let tok = AccessToken::new(OPAQUE_AUTH_VALUE);
    let shown = format!("{tok:?}");
    assert!(
        !shown.contains(OPAQUE_AUTH_VALUE),
        "token must never appear in Debug"
    );
    assert!(shown.contains("redacted"));

    // ...and it stays redacted when nested inside ConnectOptions, so logging
    // the options can't leak the credential.
    let id = ClientIdentity::new("a", "h", "u", "t", "r").expect("test identity should be valid");
    let opts = ConnectOptions::new("localhost/FREEPDB1", "scott", "", id)
        .with_access_token(OPAQUE_AUTH_VALUE);
    let shown = format!("{opts:?}");
    assert!(
        !shown.contains(OPAQUE_AUTH_VALUE),
        "ConnectOptions Debug must not leak the access token"
    );
    assert!(opts.access_token().is_some());
}

#[test]
fn iam_private_key_is_redacted_in_debug() {
    // The newtype itself never renders the key.
    let key = IamPrivateKey::new(IAM_PRIVATE_KEY_REDACTION_VALUE);
    let shown = format!("{key:?}");
    assert!(
        !shown.contains("SENTINEL"),
        "IamPrivateKey must never appear in Debug"
    );
    assert!(shown.contains("redacted"));

    // ...and it stays redacted (along with the token) nested inside ConnectOptions,
    // so logging the options can't leak either credential.
    let id = ClientIdentity::new("a", "h", "u", "t", "r").expect("test identity should be valid");
    let opts = ConnectOptions::new("adb.example.com:1522/svc_high", "scott", "", id)
        .with_iam_token(OPAQUE_AUTH_VALUE, IAM_PRIVATE_KEY_REDACTION_VALUE);
    let shown = format!("{opts:?}");
    assert!(
        !shown.contains("SENTINEL"),
        "ConnectOptions Debug must not leak the IAM private key"
    );
    assert!(
        !shown.contains(OPAQUE_AUTH_VALUE),
        "ConnectOptions Debug must not leak the IAM token"
    );
    assert!(
        shown.contains("iam_private_key") && shown.contains("***redacted***"),
        "Debug keeps the field name but redacts the value"
    );
    assert!(opts.iam_private_key().is_some());
    assert!(opts.access_token().is_some());
    assert_eq!(opts.auth_mode().kind(), AuthModeKind::IamToken);
}

#[test]
fn iam_token_over_plain_tcp_is_typed_error_before_io() {
    // A signed IAM token must be refused over a non-TLS transport before any bytes
    // leave the client (reuse of the AccessTokenRequiresTcps guard), and neither
    // the token nor the key may appear in the error.
    let id = ClientIdentity::new("a", "h", "u", "t", "r").expect("test identity should be valid");
    let opts = ConnectOptions::new("127.0.0.1:1/svc_high", "scott", "", id)
        .with_iam_token(OPAQUE_AUTH_VALUE, IAM_PRIVATE_KEY_REDACTION_VALUE);
    let err = BlockingConnection::connect(opts)
        .expect_err("signed IAM token over plain TCP must fail closed");
    assert!(
        matches!(err, Error::AccessTokenRequiresTcps),
        "expected AccessTokenRequiresTcps, got: {err:?}"
    );
    let display = format!("{err}");
    let debug = format!("{err:?}");
    assert!(!display.contains(OPAQUE_AUTH_VALUE) && !debug.contains(OPAQUE_AUTH_VALUE));
    assert!(!display.contains("SENTINEL") && !debug.contains("SENTINEL"));
}

#[test]
fn connect_options_debug_redacts_passwords() {
    let id = ClientIdentity::new("a", "h", "u", "t", "r").expect("test identity should be valid");
    let opts = ConnectOptions::new("localhost/FREEPDB1", "scott", LOGIN_REDACTION_VALUE, id)
        .with_wallet_location(SAMPLE_WALLET_PATH)
        .with_wallet_password(WALLET_REDACTION_VALUE)
        .with_ssl_server_cert_dn(SAMPLE_CERT_DN)
        .with_access_token(OPAQUE_AUTH_VALUE);

    let shown = format!("{opts:?}");
    assert!(
        !shown.contains(LOGIN_REDACTION_VALUE),
        "ConnectOptions Debug must not leak the login password"
    );
    assert!(
        !shown.contains(WALLET_REDACTION_VALUE),
        "ConnectOptions Debug must not leak the wallet password"
    );
    assert!(
        !shown.contains(OPAQUE_AUTH_VALUE),
        "ConnectOptions Debug must not leak the access token"
    );
    assert!(
        !shown.contains(SAMPLE_WALLET_PATH),
        "ConnectOptions Debug must not leak the wallet path"
    );
    assert!(
        !shown.contains(SAMPLE_CERT_DN),
        "ConnectOptions Debug must not leak certificate identity material"
    );
    assert!(
        shown.contains("***redacted***"),
        "Debug should show explicit redaction"
    );
    assert!(
        shown.contains("password") && shown.contains("wallet_password"),
        "Debug should keep field names for diagnostics"
    );
}

#[test]
fn unsupported_auth_modes_are_typed_and_redacted() {
    let id = ClientIdentity::new("a", "h", "u", "t", "r").expect("test identity should be valid");
    let external = ConnectOptions::external_auth("localhost/FREEPDB1", id.clone());
    assert_eq!(external.auth_mode().kind(), AuthModeKind::External);
    assert_eq!(
        external.auth_capabilities().support(AuthModeKind::External),
        AuthModeSupport::UnsupportedInThin
    );

    let err = BlockingConnection::connect(external)
        .expect_err("external auth is a typed unsupported mode in this thin build");
    assert!(
        matches!(err, Error::UnsupportedAuthMode(_)),
        "expected UnsupportedAuthMode(External), got: {err:?}"
    );
    let Error::UnsupportedAuthMode(reason) = err else {
        return;
    };
    assert!(matches!(reason.mode(), AuthModeKind::External));

    let kerberos = ConnectOptions::kerberos_auth(
        "localhost/FREEPDB1",
        SAMPLE_KERBEROS_PRINCIPAL,
        SAMPLE_KERBEROS_KEYTAB,
        id.clone(),
    );
    let shown = format!("{kerberos:?}");
    assert!(!shown.contains(SAMPLE_KERBEROS_PRINCIPAL));
    assert!(!shown.contains(SAMPLE_KERBEROS_KEYTAB));
    assert_eq!(kerberos.auth_mode().kind(), AuthModeKind::Kerberos);

    let radius = ConnectOptions::radius_auth("localhost/FREEPDB1", SAMPLE_RADIUS_CHALLENGE, id);
    let shown = format!("{radius:?}");
    assert!(!shown.contains(SAMPLE_RADIUS_CHALLENGE));
    assert_eq!(radius.auth_mode().kind(), AuthModeKind::Radius);
}

#[test]
fn kerberos_and_radius_reject_before_network_io() {
    let id = ClientIdentity::new("a", "h", "u", "t", "r").expect("test identity should be valid");
    let kerberos = ConnectOptions::kerberos_auth(
        "127.0.0.1:1/FREEPDB1",
        SAMPLE_KERBEROS_PRINCIPAL,
        SAMPLE_KERBEROS_KEYTAB,
        id.clone(),
    );
    assert_unsupported_mode_before_io(
        kerberos,
        AuthModeKind::Kerberos,
        &[SAMPLE_KERBEROS_PRINCIPAL, SAMPLE_KERBEROS_KEYTAB],
    );

    let radius = ConnectOptions::radius_auth("127.0.0.1:1/FREEPDB1", SAMPLE_RADIUS_CHALLENGE, id);
    assert_unsupported_mode_before_io(radius, AuthModeKind::Radius, &[SAMPLE_RADIUS_CHALLENGE]);
}

fn assert_unsupported_mode_before_io(
    options: ConnectOptions,
    expected: AuthModeKind,
    secret_values: &[&str],
) {
    let err = BlockingConnection::connect(options)
        .expect_err("unsupported auth mode should fail before connecting to 127.0.0.1:1");
    let reason = if let Error::UnsupportedAuthMode(reason) = &err {
        reason
    } else {
        assert!(
            matches!(&err, Error::UnsupportedAuthMode(_)),
            "expected UnsupportedAuthMode({expected:?}) before network I/O, got {err:?}"
        );
        return;
    };
    assert_eq!(reason.mode(), expected);
    let display = format!("{err}");
    let debug = format!("{err:?}");
    for secret in secret_values {
        assert!(!display.contains(secret));
        assert!(!debug.contains(secret));
    }
}

#[test]
#[ignore]
fn access_token_over_plain_tcp_is_typed_error() {
    let cs = std::env::var("PYO_TEST_CONNECT_STRING")
        .expect("PYO_TEST_CONNECT_STRING must be set for ignored live test");
    let user = std::env::var("PYO_TEST_MAIN_USER")
        .expect("PYO_TEST_MAIN_USER must be set for ignored live test");
    let id = ClientIdentity::new("tok", "host", "user", "term", "rust")
        .expect("test identity should be valid");

    // The local endpoint is plain TCP. A token there must be refused *before* any
    // bytes leave the client — with the precise, machine-classifiable variant,
    // not a generic connection failure (and never echoing the token).
    let err = BlockingConnection::connect(
        ConnectOptions::new(&cs, &user, "", id).with_access_token(OPAQUE_AUTH_VALUE),
    )
    .expect_err("token auth over plain TCP must fail");

    assert!(
        matches!(err, Error::AccessTokenRequiresTcps),
        "expected AccessTokenRequiresTcps, got: {err:?}"
    );
    assert!(
        !format!("{err}").contains(OPAQUE_AUTH_VALUE)
            && !format!("{err:?}").contains(OPAQUE_AUTH_VALUE),
        "the token must not appear in the error"
    );
}
