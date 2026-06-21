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
use oracledb::{AccessToken, BlockingConnection, ConnectOptions, Error};

const SECRET: &str = "eyJ-this-is-a-fake-jwt-SECRET-value.signature";

#[test]
fn access_token_is_redacted_in_debug() {
    let tok = AccessToken::new(SECRET);
    let shown = format!("{tok:?}");
    assert!(!shown.contains(SECRET), "token must never appear in Debug");
    assert!(shown.contains("redacted"));

    // ...and it stays redacted when nested inside ConnectOptions (which derives
    // Debug), so logging the options can't leak the credential.
    let id = ClientIdentity::new("a", "h", "u", "t", "r").unwrap();
    let opts = ConnectOptions::new("localhost/FREEPDB1", "scott", "", id).with_access_token(SECRET);
    let shown = format!("{opts:?}");
    assert!(
        !shown.contains(SECRET),
        "ConnectOptions Debug must not leak the access token"
    );
    assert!(opts.access_token().is_some());
}

#[test]
#[ignore]
fn access_token_over_plain_tcp_is_typed_error() {
    let cs = std::env::var("PYO_TEST_CONNECT_STRING").unwrap();
    let user = std::env::var("PYO_TEST_MAIN_USER").unwrap();
    let id = ClientIdentity::new("tok", "host", "user", "term", "rust").unwrap();

    // The local endpoint is plain TCP. A token there must be refused *before* any
    // bytes leave the client — with the precise, machine-classifiable variant,
    // not a generic connection failure (and never echoing the token).
    let err = BlockingConnection::connect(
        ConnectOptions::new(&cs, &user, "", id).with_access_token(SECRET),
    )
    .expect_err("token auth over plain TCP must fail");

    assert!(
        matches!(err, Error::AccessTokenRequiresTcps),
        "expected AccessTokenRequiresTcps, got: {err:?}"
    );
    assert!(
        !format!("{err}").contains(SECRET) && !format!("{err:?}").contains(SECRET),
        "the token must not appear in the error"
    );
}
