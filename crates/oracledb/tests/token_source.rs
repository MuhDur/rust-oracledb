//! Tests for the pluggable OCI IAM / OAuth2 [`TokenSource`] seam (bead A3).
//!
//! Autonomous coverage of the three guarantees we can prove offline:
//!   1. [`TokenSourceError`] is fully redacted — no token or provider detail can
//!      appear in `Debug`/`Display`, nor when wrapped in [`Error`].
//!   2. A token source on a **plaintext** descriptor is refused with the exact
//!      typed error *before the source is ever consulted* (fail closed before
//!      fetch — a token is never produced for a transport that can't carry it).
//!   3. A source **failure** over a TCPS descriptor maps to the redacted
//!      [`Error::TokenSource`] before any socket is dialled, and a source's
//!      token frames correctly as `AUTH_TOKEN`.
//!
//! Real-cloud token *acceptance* is an operator smoke test (C5-smoke); the
//! over-the-wire `AUTH_TOKEN` framing across a real TLS transport is pinned by
//! the C3 lane test in `tls_handshake.rs`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use oracledb::protocol::ClientIdentity;
use oracledb::{
    BlockingConnection, BoxFuture, ConnectOptions, Error, TokenSource, TokenSourceError,
};

/// A secret-shaped token value used to prove it never leaks through any error.
const SECRET_TOKEN: &str = "HEADER.PAYLOAD.super-secret-signature-value";

#[derive(Clone)]
enum Outcome {
    Token(String),
    Fail(TokenSourceError),
}

/// A local mock token source that records how many times it was consulted, so a
/// test can assert the driver refuses *before* fetching on a plaintext lane.
struct MockTokenSource {
    calls: Arc<AtomicUsize>,
    outcome: Outcome,
}

impl MockTokenSource {
    fn new(outcome: Outcome) -> (Arc<Self>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let source = Arc::new(Self {
            calls: calls.clone(),
            outcome,
        });
        (source, calls)
    }
}

impl TokenSource for MockTokenSource {
    fn get_token(&self) -> BoxFuture<'_, Result<String, TokenSourceError>> {
        let calls = self.calls.clone();
        let outcome = self.outcome.clone();
        Box::pin(async move {
            calls.fetch_add(1, Ordering::SeqCst);
            match outcome {
                Outcome::Token(token) => Ok(token),
                Outcome::Fail(err) => Err(err),
            }
        })
    }
}

fn identity() -> ClientIdentity {
    ClientIdentity::new("tok", "host", "user", "term", "rust")
        .expect("test identity should be valid")
}

#[test]
fn token_source_error_is_fully_redacted() {
    for variant in [
        TokenSourceError::Exec,
        TokenSourceError::Invalid,
        TokenSourceError::Timeout,
        TokenSourceError::Other,
    ] {
        let display = format!("{variant}");
        let debug = format!("{variant:?}");
        // A stable, non-secret class label is exposed; nothing else.
        assert!(display.starts_with("token source"), "display: {display}");
        assert!(debug.starts_with("TokenSourceError::"), "debug: {debug}");
        // Wrapped in the driver Error, still nothing beyond the class.
        let wrapped = Error::TokenSource(variant);
        let wrapped_display = format!("{wrapped}");
        let wrapped_debug = format!("{wrapped:?}");
        for rendered in [&display, &debug, &wrapped_display, &wrapped_debug] {
            assert!(
                !rendered.contains(SECRET_TOKEN),
                "a token source error must never carry a token: {rendered}"
            );
        }
        // The variant carries no payload, so there is nothing to leak.
        assert!(!variant.as_str().is_empty());
    }
}

#[test]
fn token_source_over_plaintext_is_refused_before_fetch() {
    let (source, calls) = MockTokenSource::new(Outcome::Token(SECRET_TOKEN.to_string()));

    // Plaintext (tcp://) descriptor + a token source: must fail closed with the
    // precise typed error, and — critically — the source must NOT be consulted,
    // so no token is ever fetched for a transport that could not carry it.
    let err = BlockingConnection::connect(
        ConnectOptions::new("127.0.0.1:1/FREEPDB1", "OCITESTUSER", "", identity())
            .with_token_source(source),
    )
    .expect_err("a token source over plaintext must be refused");

    assert!(
        matches!(err, Error::AccessTokenRequiresTcps),
        "expected AccessTokenRequiresTcps, got: {err:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "the token source must NOT be consulted on a plaintext descriptor"
    );
    assert!(
        !format!("{err}").contains(SECRET_TOKEN) && !format!("{err:?}").contains(SECRET_TOKEN),
        "the token must never appear in the error"
    );
}

#[test]
fn token_source_failure_over_tcps_maps_to_redacted_error_before_dial() {
    let (source, calls) = MockTokenSource::new(Outcome::Fail(TokenSourceError::Exec));

    // A TCPS descriptor passes the transport guard, so the source IS consulted;
    // its failure surfaces as the redacted `Error::TokenSource` — and because
    // the token is resolved before the socket is dialled, the unroutable
    // 127.0.0.1:1 is never contacted (a connection error would classify as
    // Network, not Authentication).
    let err = BlockingConnection::connect(
        ConnectOptions::new("tcps://127.0.0.1:1/FREEPDB1", "OCITESTUSER", "", identity())
            .with_token_source(source),
    )
    .expect_err("a failing token source must fail the connect");

    assert!(
        matches!(err, Error::TokenSource(TokenSourceError::Exec)),
        "expected Error::TokenSource(Exec), got: {err:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the token source must be consulted exactly once on a TCPS descriptor"
    );
    assert_eq!(err.kind(), oracledb::ErrorKind::Authentication);
}

#[test]
fn token_source_token_frames_as_auth_token() {
    // The source's token must be exactly what the driver puts in `AUTH_TOKEN`.
    // Resolve it on the runtime and pin the framing (the over-the-TLS-transport
    // proof lives in the C3 lane test).
    let (source, _calls) = MockTokenSource::new(Outcome::Token(SECRET_TOKEN.to_string()));

    let reactor = asupersync::runtime::reactor::create_reactor().expect("reactor");
    let rt = asupersync::runtime::RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("runtime");
    let token = rt.block_on(async move {
        source
            .get_token()
            .await
            .expect("mock source yields a token")
    });
    assert_eq!(token, SECRET_TOKEN);

    let payload = oracledb_protocol::thin::build_fast_auth_token_payload(
        "OCITESTUSER",
        &token,
        "rust-oracledb",
        300_000_000,
        "cs",
        None,
    )
    .expect("build fast-auth token payload");

    let contains = |needle: &[u8]| payload.windows(needle.len()).any(|w| w == needle);
    assert!(contains(b"AUTH_TOKEN"), "the token frames as AUTH_TOKEN");
    assert!(
        contains(SECRET_TOKEN.as_bytes()),
        "the resolved token value is carried on the wire"
    );
    assert!(
        !contains(b"AUTH_PASSWORD") && !contains(b"AUTH_SESSKEY"),
        "token auth carries no password material"
    );
}
