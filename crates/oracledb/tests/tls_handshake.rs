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

pub use oracledb::Error;

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
