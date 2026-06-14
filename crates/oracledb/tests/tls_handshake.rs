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
//! driver does, calling the real `oracledb::tls` + `oracledb::transport` code.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Arc;

use asupersync::io::{AsyncReadExt, AsyncWriteExt};
use asupersync::net::TcpStream;
use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::Cx;
use oracledb::tls::{self, TlsParams};
use oracledb::transport;
use oracledb_protocol::net::EasyConnect;
use oracledb_protocol::tls::parse_ewallet_pem;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ServerConfig, ServerConnection};

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
        .with_single_cert(chain, PrivateKeyDer::from(key_der))
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
        let cx = Cx::current().expect("ambient cx");
        let tcp = TcpStream::connect((desc.host.clone(), desc.port))
            .await
            .expect("tcp connect");
        let tls_stream = tls::tls_handshake(&desc, None, &params, tcp)
            .await
            .expect("TCPS handshake must succeed against CA-trusted server");
        let (mut read, write) = transport::tls_split(tls_stream);
        let write = Arc::new(asupersync::sync::Mutex::with_name("w", write));
        {
            let mut g = write.lock(&cx).await.expect("lock");
            g.write_all(b"ping\n").await.expect("write");
            g.flush().await.expect("flush");
        }
        let mut buf = vec![0u8; 5];
        read.read_exact(&mut buf).await.expect("read echo");
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
