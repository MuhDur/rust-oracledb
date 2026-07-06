//! End-to-end TLS (TCPS) handshake tests against a REAL rustls server.
//!
//! These prove the actual TLS code path — the Oracle `ServerCertVerifier`
//! (name-unbound chain validation + Oracle DN/SAN/CN match), the Oracle SNI
//! string, the wallet trust anchors, and the `OracleReadHalf`/`OracleWriteHalf`
//! transport — all against a live rustls server presenting the CA-signed leaf
//! certificate from the fixtures. No mocks of our own code; rustls does the
//! real handshake on both sides.
//!
//! The server runs on a blocking std thread (`rustls::ServerConnection` +
//! `rustls::Stream`); the client runs on the asupersync runtime exactly as the
//! driver does, compiling the crate-local TLS module directly because it is not
//! part of the public API.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Arc;

use asupersync::io::{AsyncReadExt, AsyncWriteExt};
use asupersync::net::TcpStream;
use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::Cx;
use oracledb_protocol::net::EasyConnect;
use oracledb_protocol::tls::wallet::parse_ewallet_pem;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig, ServerConnection};

use oracledb::protocol::ClientIdentity;
pub use oracledb::Error;
use oracledb::{BlockingConnection, ConnectOptions};

// `tls.rs` calls the crate's `obs_warn!` (defined in `src/obs.rs`, brought into
// crate scope by `#[macro_use] mod obs;` in `lib.rs`). When we `#[path]`-include
// `tls.rs` here as a standalone module of the test binary that macro is not in
// scope, so we supply the same no-op expansion the non-`tracing` build uses. It
// must be declared textually before the `mod tls;` include to be in scope for it.
macro_rules! obs_warn {
    ($($args:tt)+) => {{}};
}

#[allow(dead_code)]
#[path = "../src/tls.rs"]
mod tls;

use tls::TlsParams;

/// Build the asupersync I/O runtime the same way the driver does.
fn io_runtime() -> asupersync::runtime::Runtime {
    let reactor = reactor::create_reactor().expect("native reactor");
    RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("io runtime")
}

fn fixture(name: &str) -> Vec<u8> {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("fixtures");
    p.push("tls");
    p.push(name);
    std::fs::read(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

/// Build a rustls `ServerConfig` presenting the CA-signed leaf cert + key.
fn server_config() -> Arc<ServerConfig> {
    let leaf = fixture("leaf.crt");
    let key = fixture("leaf.key");
    let ca = fixture("ca.crt");

    let mut leaf_reader = std::io::BufReader::new(&leaf[..]);
    let mut chain: Vec<CertificateDer<'static>> = rustls_pemfile_certs(&mut leaf_reader);
    // Include the CA so the client (which trusts the CA) sees a full chain.
    let mut ca_reader = std::io::BufReader::new(&ca[..]);
    chain.extend(rustls_pemfile_certs(&mut ca_reader));

    let mut key_reader = std::io::BufReader::new(&key[..]);
    let key_der = rustls_pemfile::private_key(&mut key_reader)
        .expect("read key")
        .expect("a private key");

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(chain, key_der)
        .expect("server config");
    Arc::new(config)
}

fn rustls_pemfile_certs(reader: &mut dyn std::io::BufRead) -> Vec<CertificateDer<'static>> {
    rustls_pemfile::certs(reader)
        .filter_map(Result::ok)
        .collect()
}

/// Spawn a one-shot blocking rustls echo server; returns its bound port and the
/// join handle. The server completes one TLS handshake, echoes one line, closes.
fn spawn_tls_server() -> (u16, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let config = server_config();
    let handle = std::thread::spawn(move || {
        if let Ok((mut sock, _)) = listener.accept() {
            let mut conn = ServerConnection::new(config).expect("server conn");
            let mut tls = rustls::Stream::new(&mut conn, &mut sock);
            // Drive the handshake + echo. Read up to a newline, echo it back.
            let mut buf = [0u8; 64];
            match tls.read(&mut buf) {
                Ok(n) if n > 0 => {
                    let _ = tls.write_all(&buf[..n]);
                    let _ = tls.flush();
                }
                _ => {}
            }
        }
    });
    (port, handle)
}

fn descriptor(port: u16) -> EasyConnect {
    EasyConnect::parse(&format!("tcps://127.0.0.1:{port}/FREEPDB1")).expect("parse tcps descriptor")
}

/// Wallet that trusts the CA (verify-only): the client validates the server's
/// leaf against this CA.
fn ca_trust_params(expected_host: &str, dn_match: bool, cert_dn: Option<&str>) -> TlsParams {
    let ca_pem = fixture("ca_wallet.pem");
    let wallet = parse_ewallet_pem(&ca_pem, None).expect("parse ca wallet");
    TlsParams {
        wallet: Some(wallet),
        dn_match,
        server_cert_dn: cert_dn.map(str::to_string),
        expected_host: expected_host.to_string(),
        use_sni: false,
    }
}

#[test]
fn tcps_handshake_succeeds_with_ca_wallet_and_name_match() {
    let (port, server) = spawn_tls_server();
    // The leaf SAN/CN is db.example.com but also 127.0.0.1 (IP SAN). We match on
    // the host name; the leaf carries DNS:localhost and IP:127.0.0.1, so use
    // "localhost" which the leaf SAN includes.
    let params = ca_trust_params("localhost", true, None);
    let desc = descriptor(port);

    let rt = io_runtime();
    let echoed: Vec<u8> = rt.block_on(async move {
        let _cx = Cx::current().expect("ambient cx");
        let tcp = TcpStream::connect((desc.host.clone(), desc.port))
            .await
            .expect("tcp connect");
        let mut tls_stream = tls::tls_handshake(&desc, None, &params, tcp)
            .await
            .expect("TCPS handshake must succeed against CA-trusted server");
        tls_stream.write_all(b"ping\n").await.expect("write");
        tls_stream.flush().await.expect("flush");
        let mut buf = vec![0u8; 5];
        tls_stream.read_exact(&mut buf).await.expect("read echo");
        buf
    });
    assert_eq!(
        &echoed, b"ping\n",
        "TLS echo must round-trip through the transport"
    );
    server.join().expect("server thread");
}

#[test]
fn tcps_handshake_rejects_on_dn_mismatch() {
    let (port, server) = spawn_tls_server();
    // expected_host that the leaf cert does NOT carry => DN/name match fails.
    let params = ca_trust_params("not-in-cert.example.org", true, None);
    let desc = descriptor(port);

    let rt = io_runtime();
    let result = rt.block_on(async move {
        let _cx = Cx::current().expect("ambient cx");
        let tcp = TcpStream::connect((desc.host.clone(), desc.port))
            .await
            .expect("tcp connect");
        tls::tls_handshake(&desc, None, &params, tcp).await
    });
    assert!(
        result.is_err(),
        "handshake must FAIL when the server name is not in the certificate"
    );
    // Best-effort: the server thread may error on the aborted handshake.
    let _ = server.join();
}

#[test]
fn tcps_handshake_accepts_explicit_cert_dn() {
    let (port, server) = spawn_tls_server();
    // The leaf subject DN is "CN=db.example.com,O=ExampleDB,C=US".
    let params = ca_trust_params(
        "ignored-when-cert-dn-set",
        true,
        Some("CN=db.example.com,O=ExampleDB,C=US"),
    );
    let desc = descriptor(port);

    let rt = io_runtime();
    let ok = rt.block_on(async move {
        let _cx = Cx::current().expect("ambient cx");
        let tcp = TcpStream::connect((desc.host.clone(), desc.port))
            .await
            .expect("tcp connect");
        tls::tls_handshake(&desc, None, &params, tcp).await.is_ok()
    });
    assert!(ok, "explicit matching ssl_server_cert_dn must be accepted");
    let _ = server.join();
}

// ---------------------------------------------------------------------------
// C2 — local TCPS lane over the C1 synthetic wallets.
//
// gvenzl Oracle images cannot speak TCPS (no orapki/Java/openssl, TCP 1521
// only), so the client TLS path is proven against the local rustls listener
// above, driven by the synthetic wallet fixtures under `fixtures/tls/synthetic/`
// (fictional `CN=oracle-test.invalid`, C1 / bead …3.3.1). Everything here is
// fully offline and deterministic: no container, no cloud, no infra.
//
// Coverage (the C2 DoD): handshake · DN/name match (+negatives) · wallet
// trust-anchor precedence · mutual TLS (+ the no-client-cert negative control).
// The synthetic leaf carries CN=oracle-test.invalid and NO SAN, so the name
// branch exercises the CN fallback and the explicit-DN branch exercises
// `ssl_server_cert_dn`.
// ---------------------------------------------------------------------------

/// The synthetic leaf/CA subject DN (RFC4514). `check_cert_dn` is
/// order-independent, so the attribute order here is not load-bearing.
const SYNTHETIC_DN: &str = "CN=oracle-test.invalid,O=Oracle Synthetic Test,C=US";
/// The synthetic leaf's CN — the name-match branch target (there is no SAN).
const SYNTHETIC_CN: &str = "oracle-test.invalid";

/// Read a fixture from `tests/fixtures/tls/synthetic/`.
fn synthetic_fixture(name: &str) -> Vec<u8> {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("fixtures");
    p.push("tls");
    p.push("synthetic");
    p.push(name);
    std::fs::read(&p).unwrap_or_else(|e| panic!("read synthetic fixture {}: {e}", p.display()))
}

/// The synthetic server identity for a rustls `ServerConfig`: the leaf from
/// `ewallet.pem` (leaf + key), with `ca.pem` appended so the presented chain is
/// complete for a client that trusts the CA.
fn synthetic_server_identity() -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    let ewallet = synthetic_fixture("ewallet.pem");
    let ca = synthetic_fixture("ca.pem");

    let mut chain: Vec<CertificateDer<'static>> =
        rustls_pemfile_certs(&mut std::io::BufReader::new(&ewallet[..]));
    chain.extend(rustls_pemfile_certs(&mut std::io::BufReader::new(&ca[..])));

    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(&ewallet[..]))
        .expect("read synthetic server key")
        .expect("synthetic ewallet.pem carries a private key");
    (chain, key)
}

/// Build a rustls `ServerConfig` presenting the synthetic leaf. When
/// `require_client_auth` is set the server demands + verifies a client
/// certificate against the synthetic CA (mutual TLS).
fn synthetic_server_config(require_client_auth: bool) -> Arc<ServerConfig> {
    let (chain, key) = synthetic_server_identity();

    let builder = ServerConfig::builder();
    let config = if require_client_auth {
        let mut roots = RootCertStore::empty();
        for ca in rustls_pemfile_certs(&mut std::io::BufReader::new(
            &synthetic_fixture("ca.pem")[..],
        )) {
            roots
                .add(ca)
                .expect("add synthetic CA to client-auth roots");
        }
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .expect("build synthetic client-cert verifier");
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain, key)
            .expect("synthetic mTLS server config")
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .expect("synthetic server config")
    };
    Arc::new(config)
}

/// Spawn a one-shot blocking rustls echo server presenting the synthetic leaf.
fn spawn_synthetic_tls_server(require_client_auth: bool) -> (u16, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let config = synthetic_server_config(require_client_auth);
    let handle = std::thread::spawn(move || {
        if let Ok((mut sock, _)) = listener.accept() {
            let mut conn = match ServerConnection::new(config) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[C2] synthetic server conn setup failed: {e}");
                    return;
                }
            };
            let mut tls = rustls::Stream::new(&mut conn, &mut sock);
            let mut buf = [0u8; 64];
            match tls.read(&mut buf) {
                Ok(n) if n > 0 => {
                    let _ = tls.write_all(&buf[..n]);
                    let _ = tls.flush();
                }
                _ => {}
            }
        }
    });
    (port, handle)
}

/// Verify-only client params that trust ONLY the synthetic CA (`ca.pem` alone →
/// `ca_certificates=[ca]`, no client identity).
fn synthetic_ca_trust_params(expected_host: &str, cert_dn: Option<&str>) -> TlsParams {
    let wallet = parse_ewallet_pem(&synthetic_fixture("ca.pem"), None)
        .expect("parse synthetic ca.pem into a verify-only wallet");
    assert!(
        !wallet.has_client_identity(),
        "ca.pem alone must be verify-only (no client key)"
    );
    TlsParams {
        wallet: Some(wallet),
        dn_match: true,
        server_cert_dn: cert_dn.map(str::to_string),
        expected_host: expected_host.to_string(),
        use_sni: false,
    }
}

/// Client params carrying BOTH the synthetic CA trust anchor AND the synthetic
/// client identity — the shape a real Oracle `ewallet.pem` has (client leaf
/// first, then the CA). Built in-memory from the two committed fixtures so the
/// real [`parse_ewallet_pem`] path is exercised end-to-end.
fn synthetic_mtls_params(expected_host: &str) -> TlsParams {
    let mut pem = synthetic_fixture("ewallet.pem"); // client leaf + key
    pem.extend_from_slice(&synthetic_fixture("ca.pem")); // trust anchor
    let wallet = parse_ewallet_pem(&pem, None).expect("parse synthetic mTLS wallet");
    assert!(
        wallet.has_client_identity(),
        "combined ewallet.pem+ca.pem must yield a client identity for mTLS"
    );
    TlsParams {
        wallet: Some(wallet),
        dn_match: true,
        server_cert_dn: None,
        expected_host: expected_host.to_string(),
        use_sni: false,
    }
}

#[test]
fn synthetic_tcps_handshake_succeeds_with_ca_wallet_cn_match() {
    // Handshake: the synthetic-CA wallet validates the synthetic leaf and the
    // name branch matches the CN (the leaf carries no SAN).
    let (port, server) = spawn_synthetic_tls_server(false);
    let params = synthetic_ca_trust_params(SYNTHETIC_CN, None);
    let desc = descriptor(port);

    let rt = io_runtime();
    let echoed: Vec<u8> = rt.block_on(async move {
        let _cx = Cx::current().expect("ambient cx");
        let tcp = TcpStream::connect((desc.host.clone(), desc.port))
            .await
            .expect("tcp connect");
        let mut tls_stream = tls::tls_handshake(&desc, None, &params, tcp)
            .await
            .expect("TCPS handshake must succeed against the synthetic CA-trusted server");
        tls_stream.write_all(b"ping\n").await.expect("write");
        tls_stream.flush().await.expect("flush");
        let mut buf = vec![0u8; 5];
        tls_stream.read_exact(&mut buf).await.expect("read echo");
        buf
    });
    assert_eq!(&echoed, b"ping\n", "synthetic TLS echo must round-trip");
    eprintln!("[C2] handshake+CN-match OK against {SYNTHETIC_CN}; echo round-tripped");
    server.join().expect("server thread");
}

#[test]
fn synthetic_tcps_handshake_rejects_on_name_mismatch() {
    // DN/name negative: a host the synthetic leaf does not carry fails closed.
    let (port, server) = spawn_synthetic_tls_server(false);
    let params = synthetic_ca_trust_params("not-in-cert.example.org", None);
    let desc = descriptor(port);

    let rt = io_runtime();
    let err = rt.block_on(async move {
        let _cx = Cx::current().expect("ambient cx");
        let tcp = TcpStream::connect((desc.host.clone(), desc.port))
            .await
            .expect("tcp connect");
        tls::tls_handshake(&desc, None, &params, tcp)
            .await
            .expect_err("handshake must FAIL when the host is not in the synthetic cert")
    });
    eprintln!("[C2] name-mismatch correctly rejected: {err}");
    let _ = server.join();
}

#[test]
fn synthetic_tcps_handshake_accepts_explicit_cert_dn() {
    // DN branch (positive): the explicit ssl_server_cert_dn equals the synthetic
    // subject DN (order-independent), so the match succeeds.
    let (port, server) = spawn_synthetic_tls_server(false);
    let params = synthetic_ca_trust_params("ignored-when-cert-dn-set", Some(SYNTHETIC_DN));
    let desc = descriptor(port);

    let rt = io_runtime();
    let ok = rt.block_on(async move {
        let _cx = Cx::current().expect("ambient cx");
        let tcp = TcpStream::connect((desc.host.clone(), desc.port))
            .await
            .expect("tcp connect");
        tls::tls_handshake(&desc, None, &params, tcp).await.is_ok()
    });
    assert!(
        ok,
        "explicit matching synthetic ssl_server_cert_dn must be accepted"
    );
    eprintln!("[C2] explicit-DN match OK for {SYNTHETIC_DN}");
    let _ = server.join();
}

#[test]
fn synthetic_tcps_handshake_rejects_on_cert_dn_mismatch() {
    // DN branch (negative): a non-matching ssl_server_cert_dn fails closed even
    // though the chain itself is trusted — the Oracle DN check is enforced.
    let (port, server) = spawn_synthetic_tls_server(false);
    let params = synthetic_ca_trust_params(
        "ignored-when-cert-dn-set",
        Some("CN=someone-else.invalid,C=US"),
    );
    let desc = descriptor(port);

    let rt = io_runtime();
    let err = rt.block_on(async move {
        let _cx = Cx::current().expect("ambient cx");
        let tcp = TcpStream::connect((desc.host.clone(), desc.port))
            .await
            .expect("tcp connect");
        tls::tls_handshake(&desc, None, &params, tcp)
            .await
            .expect_err("a non-matching ssl_server_cert_dn must fail closed")
    });
    eprintln!("[C2] cert-DN mismatch correctly rejected: {err}");
    let _ = server.join();
}

#[test]
fn synthetic_tcps_wallet_trust_anchor_governs() {
    // Trust-anchor precedence: the wallet's CAs govern chain validation. The
    // synthetic CA is self-signed and absent from any system trust store, so a
    // wallet carrying an UNRELATED CA (the db.example.com `ca_wallet.pem`) must
    // reject the synthetic leaf — proving the wallet's trust anchors, not the
    // system roots, decide the outcome. The positive counterpart is
    // `synthetic_tcps_handshake_succeeds_with_ca_wallet_cn_match`.
    let (port, server) = spawn_synthetic_tls_server(false);
    // A wallet that trusts a DIFFERENT CA than the one that signed the leaf.
    let wrong_ca = fixture("ca_wallet.pem");
    let wallet = parse_ewallet_pem(&wrong_ca, None).expect("parse unrelated CA wallet");
    let params = TlsParams {
        wallet: Some(wallet),
        dn_match: true,
        server_cert_dn: None,
        expected_host: SYNTHETIC_CN.to_string(),
        use_sni: false,
    };
    let desc = descriptor(port);

    let rt = io_runtime();
    let err = rt.block_on(async move {
        let _cx = Cx::current().expect("ambient cx");
        let tcp = TcpStream::connect((desc.host.clone(), desc.port))
            .await
            .expect("tcp connect");
        tls::tls_handshake(&desc, None, &params, tcp)
            .await
            .expect_err("an unrelated wallet CA must reject the synthetic leaf")
    });
    eprintln!("[C2] wallet trust-anchor precedence enforced (wrong CA rejected): {err}");
    let _ = server.join();
}

#[test]
fn synthetic_tcps_mutual_tls_succeeds() {
    // mTLS (positive): the server demands a client cert and verifies it against
    // the synthetic CA; the client presents the synthetic identity from the
    // combined ewallet.pem+ca.pem wallet. Full mutual handshake + echo.
    let (port, server) = spawn_synthetic_tls_server(true);
    let params = synthetic_mtls_params(SYNTHETIC_CN);
    let desc = descriptor(port);

    let rt = io_runtime();
    let echoed: Vec<u8> = rt.block_on(async move {
        let _cx = Cx::current().expect("ambient cx");
        let tcp = TcpStream::connect((desc.host.clone(), desc.port))
            .await
            .expect("tcp connect");
        let mut tls_stream = tls::tls_handshake(&desc, None, &params, tcp)
            .await
            .expect("mutual-TLS handshake must succeed with the synthetic client identity");
        tls_stream.write_all(b"ping\n").await.expect("write");
        tls_stream.flush().await.expect("flush");
        let mut buf = vec![0u8; 5];
        tls_stream.read_exact(&mut buf).await.expect("read echo");
        buf
    });
    assert_eq!(&echoed, b"ping\n", "mTLS echo must round-trip");
    eprintln!("[C2] mutual TLS OK: client identity accepted, echo round-tripped");
    server.join().expect("server thread");
}

#[test]
fn synthetic_tcps_mutual_tls_rejects_without_client_cert() {
    // mTLS (negative control): the server demands a client cert but the client
    // presents a verify-only wallet (no identity). The server aborts and the
    // client handshake fails closed.
    let (port, server) = spawn_synthetic_tls_server(true);
    let params = synthetic_ca_trust_params(SYNTHETIC_CN, None); // verify-only
    assert!(
        params
            .wallet
            .as_ref()
            .is_some_and(|w| !w.has_client_identity()),
        "this negative control must present NO client identity"
    );
    let desc = descriptor(port);

    // Under TLS 1.3 the client's handshake future can resolve before the
    // server's "certificate required" alert arrives, so the rejection is
    // observed on the first application read/write. Drive a full round-trip and
    // require the sequence as a whole to fail closed.
    let rt = io_runtime();
    let outcome: Result<(), oracledb::Error> = rt.block_on(async move {
        let _cx = Cx::current().expect("ambient cx");
        let tcp = TcpStream::connect((desc.host.clone(), desc.port))
            .await
            .expect("tcp connect");
        let mut tls_stream = tls::tls_handshake(&desc, None, &params, tcp).await?;
        tls_stream
            .write_all(b"ping\n")
            .await
            .map_err(|e| oracledb::Error::Tls(format!("write: {e}")))?;
        tls_stream
            .flush()
            .await
            .map_err(|e| oracledb::Error::Tls(format!("flush: {e}")))?;
        let mut buf = vec![0u8; 5];
        tls_stream
            .read_exact(&mut buf)
            .await
            .map_err(|e| oracledb::Error::Tls(format!("read: {e}")))?;
        Ok(())
    });
    let err = outcome.expect_err("a client with no certificate must be rejected by an mTLS server");
    eprintln!("[C2] mTLS without a client cert correctly rejected: {err}");
    let _ = server.join();
}

// ---------------------------------------------------------------------------
// C3 — mock OCI IAM token source over the C2 TCPS lane.
//
// Covers OCI Layer 3 (the token wire path + the non-TCPS refusal) autonomously:
// no real IAM, no cloud creds, no signing. A local provider returns a
// throwaway JWT-shaped token; the test drives the driver's real `AUTH_TOKEN`
// fast-auth framing over the synthetic C2 TCPS lane and asserts (a) the token
// travels as `AUTH_TOKEN` (never as password material) across an actual TLS
// transport, and (b) the same token on a plaintext descriptor is refused with
// the exact typed error — before any bytes leave the process.
//
// The `MockIamTokenSource` here is the test-only provider that bead A3's
// `TokenSource` trait later formalizes; C3 pins the wire behaviour it must
// produce.
// ---------------------------------------------------------------------------

/// A mock OCI IAM token source. Returns a deterministic, throwaway JWT-shaped
/// token — three base64url segments (`header.payload.signature`) — that is NOT a
/// credential and never touches any cloud service. Purely local.
struct MockIamTokenSource {
    token: String,
}

impl MockIamTokenSource {
    fn new() -> Self {
        // Fake, self-contained JWT shape. base64url charset only; the segments
        // decode to obviously-synthetic JSON but are never verified. No secret.
        let header = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9";
        let payload = "eyJzdWIiOiJvY2lkMS51c2VyLm9jMS4uc3ludGhldGljIiwiZXhwIjoyNTM0MDIzMDB9";
        let signature = "c3ludGhldGljLXNpZ25hdHVyZS1ub3QtcmVhbA";
        Self {
            token: format!("{header}.{payload}.{signature}"),
        }
    }

    /// Return the current token (A3's `TokenSource::get_token` analogue).
    fn get_token(&self) -> String {
        self.token.clone()
    }
}

fn byte_window_contains(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty() && haystack.windows(needle.len()).any(|w| w == needle)
}

/// Spawn a one-shot blocking rustls server on the synthetic leaf that, after the
/// TLS handshake, reads one length-prefixed frame (`u32` big-endian length +
/// body) and echoes the body back. Lets the client prove that its `AUTH_TOKEN`
/// payload traverses a real TLS transport byte-for-byte.
fn spawn_synthetic_capture_server() -> (u16, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let config = synthetic_server_config(false);
    let handle = std::thread::spawn(move || {
        if let Ok((mut sock, _)) = listener.accept() {
            let mut conn = match ServerConnection::new(config) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[C3] synthetic capture server conn setup failed: {e}");
                    return;
                }
            };
            let mut tls = rustls::Stream::new(&mut conn, &mut sock);
            let mut len_buf = [0u8; 4];
            if tls.read_exact(&mut len_buf).is_ok() {
                let n = u32::from_be_bytes(len_buf) as usize;
                let mut body = vec![0u8; n];
                if tls.read_exact(&mut body).is_ok() {
                    let _ = tls.write_all(&body);
                    let _ = tls.flush();
                }
            }
        }
    });
    (port, handle)
}

#[test]
fn c3_mock_iam_token_frames_auth_token_over_tcps_lane() {
    let source = MockIamTokenSource::new();
    let token = source.get_token();
    assert_eq!(
        token.split('.').count(),
        3,
        "the mock IAM token must be JWT-shaped (header.payload.signature)"
    );

    // The exact fast-auth token bundle the driver puts on the wire for token
    // (OCI IAM / OAuth2) auth. This is the real driver framing, not a re-encode.
    let payload = oracledb_protocol::thin::build_fast_auth_token_payload(
        "OCITESTUSER",
        &token,
        "rust-oracledb",
        300_000_000,
        "cs",
        None,
    )
    .expect("build fast-auth token payload");

    let (port, server) = spawn_synthetic_capture_server();
    let params = synthetic_ca_trust_params(SYNTHETIC_CN, None);
    let desc = descriptor(port);

    let rt = io_runtime();
    let sent = payload.clone();
    let echoed: Vec<u8> = rt.block_on(async move {
        let _cx = Cx::current().expect("ambient cx");
        let tcp = TcpStream::connect((desc.host.clone(), desc.port))
            .await
            .expect("tcp connect");
        let mut tls_stream = tls::tls_handshake(&desc, None, &params, tcp)
            .await
            .expect("TCPS handshake must succeed for the C3 token lane");
        let len = u32::try_from(sent.len()).expect("payload fits u32");
        tls_stream
            .write_all(&len.to_be_bytes())
            .await
            .expect("write frame length");
        tls_stream
            .write_all(&sent)
            .await
            .expect("write auth payload");
        tls_stream.flush().await.expect("flush");
        let mut buf = vec![0u8; sent.len()];
        tls_stream
            .read_exact(&mut buf)
            .await
            .expect("read echoed auth payload");
        buf
    });

    // The AUTH_TOKEN framing traversed a real TLS transport intact.
    assert_eq!(
        echoed, payload,
        "the AUTH_TOKEN fast-auth payload must round-trip byte-identically over the TCPS lane"
    );
    assert!(
        byte_window_contains(&echoed, b"AUTH_TOKEN"),
        "the wire frame must carry the AUTH_TOKEN key"
    );
    assert!(
        byte_window_contains(&echoed, token.as_bytes()),
        "the mock JWT must be carried inside AUTH_TOKEN"
    );
    assert!(
        !byte_window_contains(&echoed, b"AUTH_PASSWORD")
            && !byte_window_contains(&echoed, b"AUTH_SESSKEY"),
        "token auth must carry no password/verifier material"
    );
    eprintln!(
        "[C3] mock IAM token framed as AUTH_TOKEN and round-tripped over the C2 TCPS lane ({} bytes)",
        echoed.len()
    );
    server.join().expect("server thread");
}

#[test]
fn c3_mock_iam_token_over_plaintext_refused_before_io() {
    let source = MockIamTokenSource::new();
    let token = source.get_token();
    let id = ClientIdentity::new("tok", "host", "user", "term", "rust")
        .expect("test identity should be valid");

    // A plaintext (tcp://) descriptor carrying an access token must fail closed
    // with the precise typed variant BEFORE any bytes leave the process — the
    // guard fires on the descriptor, so the unroutable 127.0.0.1:1 is never even
    // dialled. Were the guard missing this would instead attempt a connection.
    let err = BlockingConnection::connect(
        ConnectOptions::new("127.0.0.1:1/FREEPDB1", "OCITESTUSER", "", id)
            .with_access_token(token.as_str()),
    )
    .expect_err("token auth over a plaintext descriptor must be refused");

    assert!(
        matches!(err, Error::AccessTokenRequiresTcps),
        "expected AccessTokenRequiresTcps, got: {err:?}"
    );
    assert!(
        !format!("{err}").contains(&token) && !format!("{err:?}").contains(&token),
        "the token must never appear in the error"
    );
    eprintln!("[C3] mock IAM token over plaintext correctly refused: {err}");
}

// ---------------------------------------------------------------------------
// A2.3 — offline wallet + TCPS handshake validation.
//
// The C1 wallet-parse matrix (3DES/PBES2 p12, encrypted/plain pem, sso, and the
// wrong-password typed-error negatives) is covered by
// `oracledb-protocol/tests/tls_wallet.rs`; the A2.2 fallthrough negative
// controls (undecryptable-only preserves the exact typed error, never a
// fallthrough mention) by the `tls::tests` unit tests in `src/tls.rs`. Here we
// add the handshake-side proofs that tie those parsers to the C2 lane:
//   * the A2.1 legacy-3DES-decrypted client key is *cryptographically* usable in
//     a real mutual-TLS handshake (the key must actually sign the
//     CertificateVerify — structural PKCS#8 validity alone would not catch a
//     mis-decrypted key), and
//   * an undecryptable wallet fed to the handshake input preserves the exact
//     typed `Pkcs12` error, redacted and with no fallthrough wording.
// ---------------------------------------------------------------------------

/// Password for the wallets under `fixtures/tls/synthetic/`
/// (see `scripts/gen_test_wallets.sh`).
const SYNTHETIC_WALLET_PASSWORD: &str = "oracle-test-wallet-16";

/// An mTLS client wallet that MERGES the identity decrypted from the synthetic
/// legacy-3DES `ewallet_3des_openssl.p12` (A2.1) with the synthetic CA trust
/// anchor from `ca.pem`. Exercising a handshake with it proves the decrypted
/// private key matches the leaf public key.
fn synthetic_3des_mtls_params() -> TlsParams {
    use oracledb_protocol::tls::wallet::{parse_ewallet_p12, WalletContents};
    let p12 = synthetic_fixture("ewallet_3des_openssl.p12");
    let identity = parse_ewallet_p12(&p12, Some(SYNTHETIC_WALLET_PASSWORD))
        .expect("legacy 3DES ewallet.p12 must decrypt with the wallet password");
    assert!(
        identity.has_client_identity(),
        "the 3DES p12 must carry a client identity"
    );
    let ca = parse_ewallet_pem(&synthetic_fixture("ca.pem"), None).expect("parse synthetic CA");
    let wallet = WalletContents {
        ca_certificates: ca.ca_certificates,
        client_cert_chain: identity.client_cert_chain,
        client_private_key: identity.client_private_key,
    };
    assert!(
        wallet.has_client_identity(),
        "merged 3DES identity + CA must be usable for mTLS"
    );
    TlsParams {
        wallet: Some(wallet),
        dn_match: true,
        server_cert_dn: None,
        expected_host: SYNTHETIC_CN.to_string(),
        use_sni: false,
    }
}

#[test]
fn a23_mtls_handshake_with_decrypted_3des_p12_identity() {
    // The server demands a client cert and verifies it against the synthetic CA;
    // the client presents the identity whose key was recovered by the A2.1 3DES
    // path. A successful mutual handshake + echo proves the decrypted key is the
    // real private key for the leaf (rustls signs the handshake with it).
    let (port, server) = spawn_synthetic_tls_server(true);
    let params = synthetic_3des_mtls_params();
    let desc = descriptor(port);

    let rt = io_runtime();
    let echoed: Vec<u8> = rt.block_on(async move {
        let _cx = Cx::current().expect("ambient cx");
        let tcp = TcpStream::connect((desc.host.clone(), desc.port))
            .await
            .expect("tcp connect");
        let mut tls_stream = tls::tls_handshake(&desc, None, &params, tcp)
            .await
            .expect("mTLS handshake with the 3DES-decrypted identity must succeed");
        tls_stream.write_all(b"ping\n").await.expect("write");
        tls_stream.flush().await.expect("flush");
        let mut buf = vec![0u8; 5];
        tls_stream.read_exact(&mut buf).await.expect("read echo");
        buf
    });
    assert_eq!(
        &echoed, b"ping\n",
        "mTLS echo with 3DES identity must round-trip"
    );
    eprintln!("[A2.3] mTLS handshake OK with the A2.1 legacy-3DES-decrypted client identity");
    server.join().expect("server thread");
}

#[test]
fn a23_undecryptable_3des_wallet_preserves_typed_error_no_fallthrough() {
    // Handshake-input negative control: a wrong wallet password against the
    // legacy-3DES p12 (with no cwallet.sso to fall through to) must surface the
    // parser's own typed `Pkcs12` error verbatim — redacted, and never rewritten
    // to mention the auto-login fallthrough.
    use oracledb_protocol::tls::wallet::{parse_ewallet_p12, WalletError};
    let p12 = synthetic_fixture("ewallet_3des_openssl.p12");
    let err = parse_ewallet_p12(&p12, Some("not-the-password!"))
        .expect_err("a wrong wallet password must fail closed");
    assert!(
        matches!(&err, WalletError::Pkcs12(_)),
        "expected the original typed Pkcs12 error, got {err:?}"
    );
    for rendered in [format!("{err}"), format!("{err:?}")] {
        assert!(
            !rendered.contains("not-the-password!"),
            "the wallet password must not leak: {rendered}"
        );
        assert!(
            !rendered.contains(SYNTHETIC_WALLET_PASSWORD),
            "the real wallet password must not leak: {rendered}"
        );
        let lower = rendered.to_ascii_lowercase();
        assert!(
            !lower.contains("fall") && !lower.contains("auto-login") && !lower.contains("sso"),
            "the preserved error must not mention the fallthrough: {rendered}"
        );
    }
    eprintln!("[A2.3] undecryptable 3DES wallet preserved its typed Pkcs12 error (no fallthrough)");
}
