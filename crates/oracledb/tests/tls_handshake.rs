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
use std::time::{Duration, Instant};

use asupersync::io::{AsyncReadExt, AsyncWriteExt};
use asupersync::net::TcpStream;
use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::Cx;
use oracledb_protocol::net::EasyConnect;
use oracledb_protocol::tls::wallet::parse_ewallet_pem;
use ring::signature::{UnparsedPublicKey, RSA_PKCS1_2048_8192_SHA256, RSA_PSS_2048_8192_SHA256};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, UnixTime};
use rustls::server::{
    danger::{ClientCertVerified, ClientCertVerifier},
    ClientHello, ResolvesServerCert, WebPkiClientVerifier,
};
use rustls::sign::CertifiedKey;
use rustls::{
    DigitallySignedStruct, Error as RustlsError, ProtocolVersion, RootCertStore, ServerConfig,
    ServerConnection, SignatureScheme,
};
use x509_cert::der::{Decode, Encode};

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

fn fixture_dir() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("fixtures");
    p.push("tls");
    p
}

fn fixture(name: &str) -> Vec<u8> {
    let mut p = fixture_dir();
    p.push(name);
    std::fs::read(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

/// Load the CA-signed leaf certificate chain and matching private key used by
/// the local rustls servers.
fn fixture_server_credentials() -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
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

    (chain, key_der)
}

/// Build a rustls `ServerConfig` presenting the CA-signed leaf cert + key.
fn server_config() -> Arc<ServerConfig> {
    let (chain, key_der) = fixture_server_credentials();
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(chain, key_der)
        .expect("server config");
    Arc::new(config)
}

/// A test-only v1 client verifier. rustls-webpki deliberately rejects v1
/// end-entity certificates, so this verifies an exact v1 leaf using ring
/// directly: its certificate signature must come from the supplied issuer and
/// its TLS CertificateVerify signature must prove possession of the leaf key.
#[derive(Debug)]
struct V1ClientCertVerifier {
    expected_leaf: Vec<u8>,
    ca_public_key: Vec<u8>,
    tls13_signature_scheme: Arc<std::sync::Mutex<Option<SignatureScheme>>>,
}

impl V1ClientCertVerifier {
    fn fixture_ca_public_key() -> Vec<u8> {
        let ca_pem = fixture("ca.crt");
        let ca_der = rustls_pemfile_certs(&mut std::io::BufReader::new(&ca_pem[..]))
            .into_iter()
            .next()
            .expect("fixture CA must contain a certificate");
        let ca = x509_cert::Certificate::from_der(ca_der.as_ref())
            .expect("fixture CA must be parseable");
        ca.tbs_certificate
            .subject_public_key_info
            .subject_public_key
            .raw_bytes()
            .to_vec()
    }

    fn client_leaf_public_key(cert: &CertificateDer<'_>) -> Result<Vec<u8>, RustlsError> {
        let leaf = x509_cert::Certificate::from_der(cert.as_ref())
            .map_err(|e| RustlsError::General(format!("invalid v1 client certificate: {e}")))?;
        if leaf.tbs_certificate.version != x509_cert::Version::V1 {
            return Err(RustlsError::General(
                "v1 regression verifier received a non-v1 certificate".to_string(),
            ));
        }
        Ok(leaf
            .tbs_certificate
            .subject_public_key_info
            .subject_public_key
            .raw_bytes()
            .to_vec())
    }

    fn verify_signature(
        algorithm: &'static dyn ring::signature::VerificationAlgorithm,
        public_key: &[u8],
        message: &[u8],
        signature: &[u8],
        context: &str,
    ) -> Result<(), RustlsError> {
        UnparsedPublicKey::new(algorithm, public_key)
            .verify(message, signature)
            .map_err(|_| RustlsError::General(format!("v1 fixture {context} verification failed")))
    }
}

impl ClientCertVerifier for V1ClientCertVerifier {
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, RustlsError> {
        if end_entity.as_ref() != self.expected_leaf.as_slice() {
            return Err(RustlsError::General(
                "v1 regression verifier received an unexpected client certificate".to_string(),
            ));
        }
        let leaf = x509_cert::Certificate::from_der(end_entity.as_ref())
            .map_err(|e| RustlsError::General(format!("invalid v1 client certificate: {e}")))?;
        if leaf.tbs_certificate.version != x509_cert::Version::V1 {
            return Err(RustlsError::General(
                "v1 regression verifier received a non-v1 certificate".to_string(),
            ));
        }
        let tbs = leaf.tbs_certificate.to_der().map_err(|e| {
            RustlsError::General(format!("v1 client certificate re-encode failed: {e}"))
        })?;
        Self::verify_signature(
            &RSA_PKCS1_2048_8192_SHA256,
            &self.ca_public_key,
            &tbs,
            leaf.signature.raw_bytes(),
            "CA signature",
        )?;
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, RustlsError> {
        self.verify_tls13_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, RustlsError> {
        if let Ok(mut seen_scheme) = self.tls13_signature_scheme.lock() {
            *seen_scheme = Some(dss.scheme);
        }
        let public_key = Self::client_leaf_public_key(cert)?;
        let algorithm: &'static dyn ring::signature::VerificationAlgorithm = match dss.scheme {
            SignatureScheme::RSA_PSS_SHA256 => &RSA_PSS_2048_8192_SHA256,
            SignatureScheme::RSA_PKCS1_SHA256 => &RSA_PKCS1_2048_8192_SHA256,
            scheme => {
                return Err(RustlsError::General(format!(
                    "unexpected v1 fixture client signature scheme: {scheme:?}"
                )));
            }
        };
        Self::verify_signature(
            algorithm,
            &public_key,
            message,
            dss.signature(),
            "CertificateVerify",
        )?;
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PKCS1_SHA256,
        ]
    }
}

/// Build a rustls server which requires an exact X.509 v1 client identity.
fn server_config_requiring_v1_client_auth_with(
    chain: Vec<CertificateDer<'static>>,
    key_der: PrivateKeyDer<'static>,
    expected_leaf: Vec<u8>,
    ca_public_key: Vec<u8>,
) -> (
    Arc<ServerConfig>,
    Arc<std::sync::Mutex<Option<SignatureScheme>>>,
) {
    let tls13_signature_scheme = Arc::new(std::sync::Mutex::new(None));
    let verifier = Arc::new(V1ClientCertVerifier {
        expected_leaf,
        ca_public_key,
        tls13_signature_scheme: Arc::clone(&tls13_signature_scheme),
    });
    (
        Arc::new(
            ServerConfig::builder()
                .with_client_cert_verifier(verifier)
                .with_single_cert(chain, key_der)
                .expect("mTLS server config"),
        ),
        tls13_signature_scheme,
    )
}

fn rustls_pemfile_certs(reader: &mut dyn std::io::BufRead) -> Vec<CertificateDer<'static>> {
    rustls_pemfile::certs(reader)
        .filter_map(Result::ok)
        .collect()
}

#[derive(Debug)]
struct RecordingServerCertResolver {
    key: Arc<CertifiedKey>,
    sni_tx: std::sync::mpsc::Sender<Option<String>>,
}

impl ResolvesServerCert for RecordingServerCertResolver {
    fn resolve(&self, client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let _ = self
            .sni_tx
            .send(client_hello.server_name().map(str::to_owned));
        Some(Arc::clone(&self.key))
    }
}

/// Build a rustls server that records the parsed ClientHello SNI while using
/// the normal fixture certificate.
fn recording_server_config(sni_tx: std::sync::mpsc::Sender<Option<String>>) -> Arc<ServerConfig> {
    let (chain, key_der) = fixture_server_credentials();
    let key = CertifiedKey::from_der(chain, key_der, &rustls::crypto::ring::default_provider())
        .expect("fixture certificate and key match");

    Arc::new(
        ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(RecordingServerCertResolver {
                key: Arc::new(key),
                sni_tx,
            })),
    )
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

/// Accept one TCP connection but never send a TLS ServerHello. This is a real
/// socket-level stalled handshake, not a mocked future.
fn spawn_stalled_tls_peer(hold: Duration) -> (u16, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let handle = std::thread::spawn(move || {
        if let Ok((_socket, _)) = listener.accept() {
            std::thread::sleep(hold);
        }
    });
    (port, handle)
}

type V1MtlsEvidence = (Option<ProtocolVersion>, Option<SignatureScheme>);
type V1MtlsServer = (
    u16,
    std::sync::mpsc::Receiver<V1MtlsEvidence>,
    std::thread::JoinHandle<()>,
);

/// Spawn a one-shot blocking rustls echo server which requires the v1 fixture
/// client certificate and reports its negotiated protocol version.
fn spawn_tls_server_requiring_client_auth() -> V1MtlsServer {
    let (chain, key_der) = fixture_server_credentials();
    let expected_leaf =
        rustls_pemfile_certs(&mut std::io::BufReader::new(&fixture("client_v1.crt")[..]))
            .into_iter()
            .next()
            .expect("v1 client fixture must contain a certificate")
            .as_ref()
            .to_vec();
    spawn_tls_server_requiring_v1_client_auth_with(
        chain,
        key_der,
        expected_leaf,
        V1ClientCertVerifier::fixture_ca_public_key(),
    )
}

fn spawn_tls_server_requiring_v1_client_auth_with(
    chain: Vec<CertificateDer<'static>>,
    key_der: PrivateKeyDer<'static>,
    expected_leaf: Vec<u8>,
    ca_public_key: Vec<u8>,
) -> V1MtlsServer {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let (config, tls13_signature_scheme) =
        server_config_requiring_v1_client_auth_with(chain, key_der, expected_leaf, ca_public_key);
    let (protocol_tx, protocol_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        if let Ok((mut sock, _)) = listener.accept() {
            let mut conn = ServerConnection::new(config).expect("server conn");
            {
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
            let signature_scheme = tls13_signature_scheme
                .lock()
                .ok()
                .and_then(|seen_scheme| *seen_scheme);
            let _ = protocol_tx.send((conn.protocol_version(), signature_scheme));
        }
    });
    (port, protocol_rx, handle)
}

/// Spawn a one-shot TLS echo server and report the SNI it received after the
/// ClientHello is processed.
fn spawn_tls_server_recording_sni() -> (
    u16,
    std::sync::mpsc::Receiver<Option<String>>,
    std::thread::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let (sni_tx, sni_rx) = std::sync::mpsc::channel();
    let config = recording_server_config(sni_tx);
    let handle = std::thread::spawn(move || {
        if let Ok((mut sock, _)) = listener.accept() {
            let mut conn = ServerConnection::new(config).expect("server conn");
            {
                let mut tls = rustls::Stream::new(&mut conn, &mut sock);
                let mut buf = [0u8; 64];
                if let Ok(n) = tls.read(&mut buf) {
                    if n > 0 {
                        let _ = tls.write_all(&buf[..n]);
                        let _ = tls.flush();
                    }
                }
            }
        }
    });
    (port, sni_rx, handle)
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

/// Wallet parameters for the CA-signed X.509 v1 client fixture. Its private
/// key is the fixture `leaf.key`; the appended CA is both the server trust
/// anchor and the client-chain issuer, just like an Oracle wallet.
fn v1_client_wallet_params(expected_host: &str) -> TlsParams {
    let mut pem = fixture("client_v1.crt");
    pem.extend_from_slice(&fixture("leaf.key"));
    pem.extend_from_slice(&fixture("ca.crt"));
    let wallet = parse_ewallet_pem(&pem, None).expect("parse X.509 v1 client wallet");
    assert!(
        wallet.has_client_identity(),
        "v1 wallet must carry an identity"
    );
    TlsParams {
        wallet: Some(wallet),
        dn_match: true,
        server_cert_dn: None,
        expected_host: expected_host.to_string(),
        use_sni: false,
    }
}

/// Return the public key of the certificate that issued the wallet's client
/// leaf. `ewallet.pem` contains the identity chain in leaf-first order, so the
/// matching issuer is available locally without contacting an ADB endpoint.
fn client_leaf_issuer_public_key(client_cert_chain: &[Vec<u8>]) -> Vec<u8> {
    let leaf_der = client_cert_chain
        .first()
        .expect("real wallet must include a client certificate");
    let leaf = x509_cert::Certificate::from_der(leaf_der)
        .expect("real wallet client certificate must parse as X.509");

    client_cert_chain
        .iter()
        .skip(1)
        .find_map(|candidate_der| {
            let candidate = x509_cert::Certificate::from_der(candidate_der).ok()?;
            (candidate.tbs_certificate.subject == leaf.tbs_certificate.issuer).then(|| {
                candidate
                    .tbs_certificate
                    .subject_public_key_info
                    .subject_public_key
                    .raw_bytes()
                    .to_vec()
            })
        })
        .expect("real wallet must include the issuer of its client certificate")
}

/// Build an mTLS client configuration from a real OCI wallet selected only at
/// test time. The wallet itself is deliberately never copied into this
/// repository. We replace only its server-trust anchors with the committed
/// synthetic CA so the client can complete a completely local handshake while
/// retaining the wallet's actual X.509 v1 certificate and private key.
fn real_v1_wallet_mtls_params() -> (TlsParams, Vec<u8>, Vec<u8>) {
    let wallet_dir = std::env::var_os("ORACLEDB_REAL_V1_WALLET_DIR")
        .map(PathBuf::from)
        .expect("set ORACLEDB_REAL_V1_WALLET_DIR to a retained OCI wallet directory");
    let pem_path = wallet_dir.join("ewallet.pem");
    let pem =
        std::fs::read(&pem_path).unwrap_or_else(|e| panic!("read real wallet ewallet.pem: {e}"));
    let password_file = std::env::var_os("ORACLEDB_REAL_V1_WALLET_PASSWORD_FILE")
        .map(PathBuf::from)
        .expect("set ORACLEDB_REAL_V1_WALLET_PASSWORD_FILE for the retained OCI wallet");
    let password = std::fs::read_to_string(&password_file)
        .unwrap_or_else(|e| panic!("read real wallet password file: {e}"));
    let password = password.trim_end_matches(['\r', '\n']);
    let mut wallet = parse_ewallet_pem(&pem, Some(password))
        .expect("parse real OCI ewallet.pem with its password");
    assert!(
        wallet.has_client_identity(),
        "real OCI wallet must contain a client identity"
    );

    let expected_leaf = wallet
        .client_cert_chain
        .first()
        .expect("real wallet must include a client certificate")
        .clone();
    let leaf = x509_cert::Certificate::from_der(&expected_leaf)
        .expect("real wallet client certificate must parse as X.509");
    assert_eq!(
        leaf.tbs_certificate.version,
        x509_cert::Version::V1,
        "this regression requires the OCI wallet's real X.509 v1 client certificate"
    );
    let issuer_public_key = client_leaf_issuer_public_key(&wallet.client_cert_chain);

    let synthetic_trust = parse_ewallet_pem(&synthetic_fixture("ca.pem"), None)
        .expect("parse local synthetic server trust anchor");
    wallet.ca_certificates = synthetic_trust.ca_certificates;
    (
        TlsParams {
            wallet: Some(wallet),
            dn_match: true,
            server_cert_dn: None,
            expected_host: SYNTHETIC_CN.to_string(),
            use_sni: false,
        },
        expected_leaf,
        issuer_public_key,
    )
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
fn configured_connect_timeout_bounds_a_stalled_tls_handshake() {
    let (port, server) = spawn_stalled_tls_peer(Duration::from_millis(350));
    let identity =
        ClientIdentity::new("tls-timeout", "host", "user", "term", "rust").expect("test identity");
    let options = ConnectOptions::new(
        format!("tcps://127.0.0.1:{port}/FREEPDB1?transport_connect_timeout=75ms"),
        "user",
        "password",
        identity,
    )
    // A committed local fixture makes TLS configuration deterministic; the
    // peer stalls before presenting a certificate, so identity matching is
    // not involved in this timeout regression.
    .with_wallet_location(fixture_dir().display().to_string())
    .with_ssl_server_dn_match(false);

    let started = Instant::now();
    let error = BlockingConnection::connect(options)
        .expect_err("the configured 75ms budget must stop a stalled handshake");
    let elapsed = started.elapsed();
    assert!(
        matches!(error, Error::CallTimeout(75)),
        "the outer configured connect budget must own classification, got {error:?}"
    );
    assert!(
        elapsed < Duration::from_millis(300),
        "the former fixed 20s TLS cap must not preempt the configured 75ms budget: {elapsed:?}"
    );
    server.join().expect("stalled peer thread");
}

#[test]
fn tcps_hard_close_delivers_complete_plaintext_before_missing_close_notify() {
    // `spawn_tls_server` deliberately drops the TCP socket after flushing the
    // echo; it does not queue a rustls close_notify. This is the Oracle session
    // shutdown shape: complete application bytes followed by a bare TCP FIN.
    let (port, server) = spawn_tls_server();
    let params = ca_trust_params("localhost", true, None);
    let desc = descriptor(port);

    let rt = io_runtime();
    let error = rt.block_on(async move {
        let _cx = Cx::current().expect("ambient cx");
        let tcp = TcpStream::connect((desc.host.clone(), desc.port))
            .await
            .expect("tcp connect");
        let mut tls_stream = tls::tls_handshake(&desc, None, &params, tcp)
            .await
            .expect("TCPS handshake");
        tls_stream.write_all(b"ping\n").await.expect("write");
        tls_stream.flush().await.expect("flush");

        let mut complete = [0u8; 5];
        tls_stream
            .read_exact(&mut complete)
            .await
            .expect("complete plaintext must be delivered before transport EOF");
        assert_eq!(&complete, b"ping\n");

        let mut next = [0u8; 1];
        tls_stream
            .read(&mut next)
            .await
            .expect_err("bare TCP close must retain Asupersync's typed TLS EOF")
    });
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    assert_eq!(
        error.to_string(),
        "tls connection closed without close_notify"
    );
    server.join().expect("server thread");
}

#[test]
fn tcps_hard_close_before_expected_plaintext_remains_unexpected_eof() {
    let (port, server) = spawn_tls_server();
    let params = ca_trust_params("localhost", true, None);
    let desc = descriptor(port);

    let rt = io_runtime();
    let error = rt.block_on(async move {
        let _cx = Cx::current().expect("ambient cx");
        let tcp = TcpStream::connect((desc.host.clone(), desc.port))
            .await
            .expect("tcp connect");
        let mut tls_stream = tls::tls_handshake(&desc, None, &params, tcp)
            .await
            .expect("TCPS handshake");
        tls_stream.write_all(b"ping\n").await.expect("write");
        tls_stream.flush().await.expect("flush");

        let mut truncated = [0u8; 6];
        tls_stream
            .read_exact(&mut truncated)
            .await
            .expect_err("a missing application byte must not become clean EOF")
    });
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    assert_eq!(
        error.to_string(),
        "tls connection closed without close_notify"
    );
    server.join().expect("server thread");
}

#[test]
fn tcps_x509_v1_wallet_client_cert_builds_and_handshakes() {
    // OCI Autonomous Database wallets may contain an X.509 v1 client leaf.
    // `with_client_auth_cert` rejects it while parsing with webpki; the driver
    // must instead retain the raw DER in a CertifiedKey resolver and complete
    // mTLS against a server that requires the fixture CA's client identity.
    let params = v1_client_wallet_params("localhost");
    tls::build_client_config(&params)
        .expect("X.509 v1 client certificate must build a rustls client config");

    let (port, protocol_rx, server) = spawn_tls_server_requiring_client_auth();
    let desc = descriptor(port);
    let rt = io_runtime();
    let echoed: Vec<u8> = rt.block_on(async move {
        let _cx = Cx::current().expect("ambient cx");
        let tcp = TcpStream::connect((desc.host.clone(), desc.port))
            .await
            .expect("tcp connect");
        let mut tls_stream = tls::tls_handshake(&desc, None, &params, tcp)
            .await
            .expect("mTLS handshake must accept the X.509 v1 wallet identity");
        tls_stream.write_all(b"ping\n").await.expect("write");
        tls_stream.flush().await.expect("flush");
        let mut buf = vec![0u8; 5];
        tls_stream.read_exact(&mut buf).await.expect("read echo");
        buf
    });
    assert_eq!(&echoed, b"ping\n", "v1-wallet mTLS echo must round-trip");
    server.join().expect("server thread");
    let (protocol_version, signature_scheme) = protocol_rx
        .recv()
        .expect("v1 mTLS server must report its protocol version and CertificateVerify scheme");
    assert_eq!(
        protocol_version,
        Some(ProtocolVersion::TLSv1_3),
        "the v1-wallet regression must exercise TLS 1.3 client authentication"
    );
    assert_eq!(
        signature_scheme,
        Some(SignatureScheme::RSA_PSS_SHA256),
        "TLS 1.3 v1-wallet client authentication must prove the key with RSA-PSS"
    );
}

/// Regression proof for the precise live OCI wallet shape. This requires an
/// operator-retained OCI wallet because its v1 private key must never be added
/// to test fixtures. The local rustls server sends `CertificateRequest`,
/// verifies the real wallet leaf's issuer signature, and verifies its TLS 1.3
/// RSA-PSS `CertificateVerify`; it makes no network connection to OCI.
#[test]
#[ignore = "requires ORACLEDB_REAL_V1_WALLET_DIR and ORACLEDB_REAL_V1_WALLET_PASSWORD_FILE"]
fn tcps_real_v1_wallet_tls13_mtls_handshakes() {
    let (params, expected_leaf, issuer_public_key) = real_v1_wallet_mtls_params();
    tls::build_client_config(&params)
        .expect("real X.509 v1 client certificate must build a rustls client config");

    let (server_chain, server_key) = synthetic_server_identity();
    let (port, protocol_rx, server) = spawn_tls_server_requiring_v1_client_auth_with(
        server_chain,
        server_key,
        expected_leaf,
        issuer_public_key,
    );
    let desc = descriptor(port);
    let rt = io_runtime();
    let echoed: Vec<u8> = rt.block_on(async move {
        let _cx = Cx::current().expect("ambient cx");
        let tcp = TcpStream::connect((desc.host.clone(), desc.port))
            .await
            .expect("tcp connect");
        let mut tls_stream = tls::tls_handshake(&desc, None, &params, tcp)
            .await
            .expect("mTLS handshake must accept the real X.509 v1 wallet identity");
        tls_stream.write_all(b"ping\n").await.expect("write");
        tls_stream.flush().await.expect("flush");
        let mut buf = vec![0u8; 5];
        tls_stream.read_exact(&mut buf).await.expect("read echo");
        buf
    });
    assert_eq!(
        &echoed, b"ping\n",
        "real-v1-wallet TLS 1.3 mTLS echo must round-trip"
    );
    server.join().expect("server thread");
    let (protocol_version, signature_scheme) = protocol_rx.recv().expect(
        "real-v1 mTLS server must report its protocol version and CertificateVerify scheme",
    );
    assert_eq!(
        protocol_version,
        Some(ProtocolVersion::TLSv1_3),
        "the real OCI wallet regression must exercise TLS 1.3 client authentication"
    );
    assert_eq!(
        signature_scheme,
        Some(SignatureScheme::RSA_PSS_SHA256),
        "the real OCI wallet must prove its TLS 1.3 client key with RSA-PSS"
    );
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

#[test]
fn dsn_only_cert_dn_rejects_a_san_matching_subject_mismatch() {
    let (port, server) = spawn_tls_server();
    // The fixture certificate carries DNS:localhost, so ordinary hostname/SAN
    // verification would accept it. The pin exists only in the descriptor
    // inputs and deliberately names a different subject; the shared resolver
    // must install that pin in the real rustls verifier and reject the peer.
    let resolved = tls::resolve_tls_security(
        None,
        None,
        true,
        Some("CN=not-the-server.example.test,O=Acme,C=US"),
    );
    let params = ca_trust_params(
        "localhost",
        resolved.dn_match,
        resolved.server_cert_dn.as_deref(),
    );
    let desc = descriptor(port);

    let rt = io_runtime();
    let error = rt.block_on(async move {
        let _cx = Cx::current().expect("ambient cx");
        let tcp = TcpStream::connect((desc.host.clone(), desc.port))
            .await
            .expect("tcp connect");
        tls::tls_handshake(&desc, None, &params, tcp)
            .await
            .expect_err("a DSN-only mismatched certificate DN must fail closed")
    });
    let rendered = error.to_string();
    assert!(
        rendered.contains("distinguished name (DN)") && rendered.contains("does not match"),
        "expected the real verifier's certificate-DN mismatch, got {error}"
    );
    let _ = server.join();
}

#[test]
fn dsn_dn_match_off_disables_only_name_matching() {
    let resolved = tls::resolve_tls_security(None, None, false, None);
    assert!(!resolved.dn_match, "DSN-only OFF must survive resolution");

    // The trusted leaf does not name this host, but disabling the Oracle name
    // check intentionally permits it after the chain validates.
    let (port, server) = spawn_tls_server();
    let params = ca_trust_params(
        "not-in-cert.example.org",
        resolved.dn_match,
        resolved.server_cert_dn.as_deref(),
    );
    let desc = descriptor(port);
    let rt = io_runtime();
    let echoed: Vec<u8> = rt.block_on(async move {
        let _cx = Cx::current().expect("ambient cx");
        let tcp = TcpStream::connect((desc.host.clone(), desc.port))
            .await
            .expect("tcp connect");
        let mut tls_stream = tls::tls_handshake(&desc, None, &params, tcp)
            .await
            .expect("DN_MATCH=OFF should bypass only the SAN/CN check");
        tls_stream.write_all(b"ping\n").await.expect("write");
        tls_stream.flush().await.expect("flush");
        let mut buf = vec![0u8; 5];
        tls_stream.read_exact(&mut buf).await.expect("read echo");
        buf
    });
    assert_eq!(&echoed, b"ping\n");
    server.join().expect("server thread");

    // Turning name matching off must not weaken certificate-chain validation.
    // The synthetic CA is unrelated to the regular fixture server's chain.
    let unrelated_wallet = parse_ewallet_pem(&synthetic_fixture("ca.pem"), None)
        .expect("parse unrelated synthetic CA");
    let (port, server) = spawn_tls_server();
    let params = TlsParams {
        wallet: Some(unrelated_wallet),
        dn_match: resolved.dn_match,
        server_cert_dn: None,
        expected_host: "not-in-cert.example.org".to_string(),
        use_sni: false,
    };
    let desc = descriptor(port);
    let error = rt.block_on(async move {
        let _cx = Cx::current().expect("ambient cx");
        let tcp = TcpStream::connect((desc.host.clone(), desc.port))
            .await
            .expect("tcp connect");
        tls::tls_handshake(&desc, None, &params, tcp)
            .await
            .expect_err("DN_MATCH=OFF must still reject an untrusted chain")
    });
    assert!(
        error.to_string().contains("certificate"),
        "expected a chain-validation failure, got {error}"
    );
    let _ = server.join();
}

#[test]
fn tcps_handshake_emits_oci_endpoint_host_sni_and_keeps_explicit_dn_check() {
    let (port, received_sni, server) = spawn_tls_server_recording_sni();
    let endpoint_host = "adb.eu-frankfurt-1.oraclecloud.com";
    let desc = EasyConnect::parse(&format!(
        "tcps://127.0.0.1:{port}/g2bb4261a88e318_myadb_high.adb.oraclecloud.com"
    ))
    .expect("parse OCI-shaped descriptor");
    assert_eq!(
        desc.service_name,
        "g2bb4261a88e318_myadb_high.adb.oraclecloud.com"
    );
    // The test listener's certificate names `db.example.com`, not the OCI
    // endpoint. An explicit matching Oracle DN keeps post-handshake
    // verification active while `expected_host` drives the selected SNI.
    let mut params = ca_trust_params(
        endpoint_host,
        true,
        Some("CN=db.example.com,O=ExampleDB,C=US"),
    );
    params.use_sni = true;
    assert_eq!(
        tls::decide_sni(true, &params.expected_host, &desc.service_name, None)
            .expect("OCI host SNI is selectable"),
        Some(endpoint_host.to_string())
    );

    let rt = io_runtime();
    let echoed: Vec<u8> = rt.block_on(async move {
        let _cx = Cx::current().expect("ambient cx");
        let tcp = TcpStream::connect((desc.host.clone(), desc.port))
            .await
            .expect("tcp connect");
        let mut tls_stream = tls::tls_handshake(&desc, None, &params, tcp)
            .await
            .expect("OCI endpoint-host SNI handshake must succeed");
        tls_stream.write_all(b"ping\n").await.expect("write");
        tls_stream.flush().await.expect("flush");
        let mut buf = vec![0u8; 5];
        tls_stream.read_exact(&mut buf).await.expect("read echo");
        buf
    });

    assert_eq!(&echoed, b"ping\n");
    assert_eq!(
        received_sni
            .recv()
            .expect("server must report the received SNI"),
        Some(endpoint_host.to_string())
    );
    server.join().expect("server thread");
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

type RecordedClientIdentity = (Vec<Vec<u8>>, Option<ProtocolVersion>);
type SyntheticMtlsRecordingServer = (
    u16,
    std::sync::mpsc::Receiver<RecordedClientIdentity>,
    std::thread::JoinHandle<()>,
);

/// Spawn the synthetic mTLS echo server and return the exact client certificate
/// chain rustls observed after it completed the handshake. This makes the
/// positive mTLS test prove more than successful config construction: the peer
/// verified and actually received the wallet identity on the wire.
fn spawn_synthetic_mtls_server_recording_client_cert() -> SyntheticMtlsRecordingServer {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let config = synthetic_server_config(true);
    let (client_chain_tx, client_chain_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        if let Ok((mut sock, _)) = listener.accept() {
            let mut conn = match ServerConnection::new(config) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[C2] synthetic mTLS server conn setup failed: {e}");
                    return;
                }
            };
            {
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
            let client_chain = conn
                .peer_certificates()
                .map(|certs| certs.iter().map(|cert| cert.as_ref().to_vec()).collect())
                .unwrap_or_default();
            let _ = client_chain_tx.send((client_chain, conn.protocol_version()));
        }
    });
    (port, client_chain_rx, handle)
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
fn synthetic_tcps_mutual_tls_presents_wallet_client_cert() {
    // mTLS (positive): the server demands a client cert and verifies it against
    // the synthetic CA; the client presents the synthetic identity from the
    // combined ewallet.pem+ca.pem wallet. Full mutual handshake + echo, then
    // assert rustls on the server actually observed that identity.
    let (port, client_chain_rx, server) = spawn_synthetic_mtls_server_recording_client_cert();
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
    server.join().expect("server thread");
    let (client_chain, protocol_version) = client_chain_rx
        .recv()
        .expect("mTLS server must report the peer certificate chain");
    assert_eq!(
        protocol_version,
        Some(ProtocolVersion::TLSv1_3),
        "the default rustls client/server configurations must negotiate TLS 1.3"
    );
    let expected_leaf = rustls_pemfile_certs(&mut std::io::BufReader::new(
        &synthetic_fixture("ewallet.pem")[..],
    ))
    .into_iter()
    .next()
    .expect("synthetic ewallet.pem carries its client leaf");
    assert_eq!(
        client_chain.first().map(Vec::as_slice),
        Some(expected_leaf.as_ref()),
        "mTLS server must receive the client leaf from the wallet"
    );
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
