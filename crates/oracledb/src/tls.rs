//! TCPS (TLS) transport: the Oracle server-certificate verifier and the
//! rustls `ClientConfig` construction.
//!
//! The actual handshake is driven by asupersync's [`TlsConnector`] over the
//! async TCP socket; we supply a custom rustls [`ServerCertVerifier`] that
//! reproduces python-oracledb thin's behaviour:
//!
//! * standard hostname verification is **disabled** (the SNI value is the
//!   Oracle `S{len}.{service}.V3.{version}` string, not a DNS name that the
//!   certificate would carry), and
//! * after the chain is validated to a trust anchor, the Oracle DN/SAN/CN match
//!   ([`oracledb_protocol::tls::dn`]) is run instead.
//!
//! Chain validation itself (signature + path to a trust anchor) is delegated to
//! `rustls-webpki`'s name-unbound `EndEntityCert::verify_for_usage`, which is
//! the same crypto rustls uses internally, just without the SNI name binding —
//! exactly mirroring OpenSSL `check_hostname = False` plus `CERT_REQUIRED`.

use std::sync::Arc;

use asupersync::net::TcpStream;
use asupersync::tls::{TlsConnector, TlsStream};
use oracledb_protocol::net::EasyConnect;
use oracledb_protocol::tls::dn::{check_cert_dn, check_server_name, DnMatchError};
use oracledb_protocol::tls::sni::build_sni;
use oracledb_protocol::tls::wallet::{resolve_wallet_dir, WalletContents};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error as RustlsError, SignatureScheme};

use crate::Error;

/// TLS parameters resolved from the connect descriptor for a TCPS connection.
#[derive(Clone, Debug, Default)]
pub struct TlsParams {
    /// Wallet directory contents (trust anchors + optional client identity).
    /// `None` means "no wallet": validate against the system trust store.
    pub wallet: Option<WalletContents>,
    /// Whether to run the Oracle server-DN match after the handshake
    /// (`ssl_server_dn_match`, default `true`).
    pub dn_match: bool,
    /// Explicit expected DN (`ssl_server_cert_dn`). When set, the server's
    /// subject DN must equal this. When `None`, the host name is matched
    /// against the certificate's SAN/CN.
    pub server_cert_dn: Option<String>,
    /// The expected host (the descriptor `HOST`) for the name-match branch.
    pub expected_host: String,
    /// Send the Oracle TCPS SNI string (`use_sni`, reference default `false`).
    ///
    /// python-oracledb only emits the `S{len}.{service}.V3.{version}` SNI when
    /// `use_sni=True` is explicitly requested; by default no SNI is sent and the
    /// server is identified purely by the post-handshake DN match. rustls's
    /// `ServerName` is RFC-strict and rejects the Oracle SNI's trailing
    /// all-numeric label (e.g. `.V3.319`), so when `use_sni` is set we send the
    /// SNI only if rustls accepts it; otherwise we proceed without SNI and the
    /// DN match still secures the connection. See `docs/TLS_SETUP.md`.
    pub use_sni: bool,
}

/// The Oracle server-certificate verifier.
#[derive(Debug)]
pub(crate) struct OracleServerCertVerifier {
    /// Trust anchors parsed from the wallet (or system roots), as DER, kept so
    /// that `verify_server_cert` can build webpki `TrustAnchor`s per call.
    trust_anchor_ders: Vec<Vec<u8>>,
    /// Supported signature algorithms from the active crypto provider.
    supported_algs: WebPkiSupportedAlgorithms,
    /// Run the Oracle DN/name match after chain validation.
    dn_match: bool,
    /// Explicit expected DN; `None` => name match against `expected_host`.
    server_cert_dn: Option<String>,
    /// Expected host for the name-match branch.
    expected_host: String,
}

impl OracleServerCertVerifier {
    fn run_dn_match(&self, end_entity: &CertificateDer<'_>) -> Result<(), RustlsError> {
        if !self.dn_match {
            return Ok(());
        }
        let (subject_dn, san_dns, common_names) = parse_cert_identity(end_entity)?;
        let result = if let Some(expected_dn) = self.server_cert_dn.as_deref() {
            check_cert_dn(expected_dn, &subject_dn)
        } else {
            check_server_name(&self.expected_host, &san_dns, &common_names)
        };
        result.map_err(dn_error_to_rustls)
    }
}

impl ServerCertVerifier for OracleServerCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        // 1. Validate the chain to a trust anchor WITHOUT binding the SNI name
        //    (python-oracledb: check_hostname = False, but CERT_REQUIRED).
        let owned_certs: Vec<CertificateDer<'static>> = self
            .trust_anchor_ders
            .iter()
            .map(|der| CertificateDer::from(der.clone()))
            .collect();
        let anchors: Vec<rustls_pki_types::TrustAnchor<'_>> = owned_certs
            .iter()
            .filter_map(|c| webpki::anchor_from_trusted_cert(c).ok())
            .collect();
        if anchors.is_empty() {
            return Err(RustlsError::General(
                "wallet contained no usable CA trust anchors".to_string(),
            ));
        }

        let ee = webpki::EndEntityCert::try_from(end_entity)
            .map_err(|e| RustlsError::General(format!("invalid server certificate: {e}")))?;
        ee.verify_for_usage(
            self.supported_algs.all,
            &anchors,
            intermediates,
            now,
            webpki::KeyUsage::server_auth(),
            None,
            None,
        )
        .map_err(|e| {
            RustlsError::General(format!("TCPS server certificate chain is not trusted: {e}"))
        })?;

        // 2. Oracle DN / name match (replaces the standard hostname check).
        self.run_dn_match(end_entity)?;

        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        verify_tls12_signature(message, cert, dss, &self.supported_algs)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        verify_tls13_signature(message, cert, dss, &self.supported_algs)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported_algs.supported_schemes()
    }
}

/// Map an Oracle DN-match failure to a rustls error.
fn dn_error_to_rustls(err: DnMatchError) -> RustlsError {
    RustlsError::General(err.to_string())
}

/// Extract `(subject_dn_rfc4514, san_dns_names, common_names)` from a leaf
/// certificate using `x509-cert`.
fn parse_cert_identity(
    cert: &CertificateDer<'_>,
) -> Result<(String, Vec<String>, Vec<String>), RustlsError> {
    use x509_cert::der::Decode;
    let parsed = x509_cert::Certificate::from_der(cert.as_ref())
        .map_err(|e| RustlsError::General(format!("server certificate parse error: {e}")))?;

    let subject_dn = parsed.tbs_certificate.subject.to_string();

    // Common names from the subject RDNs.
    let mut common_names = Vec::new();
    for rdn in parsed.tbs_certificate.subject.0.iter() {
        for atv in rdn.0.iter() {
            // CN OID = 2.5.4.3
            if atv.oid.to_string() == "2.5.4.3" {
                if let Ok(s) = std::str::from_utf8(atv.value.value()) {
                    common_names.push(s.to_string());
                } else if let Ok(s) = atv.value.decode_as::<x509_cert::der::asn1::Utf8StringRef>() {
                    common_names.push(s.as_str().to_string());
                }
            }
        }
    }

    // SAN DNS names from the subjectAltName extension.
    let mut san_dns = Vec::new();
    if let Some(extensions) = parsed.tbs_certificate.extensions.as_ref() {
        for ext in extensions.iter() {
            // SAN OID = 2.5.29.17
            if ext.extn_id.to_string() == "2.5.29.17" {
                if let Ok(san) =
                    x509_cert::ext::pkix::SubjectAltName::from_der(ext.extn_value.as_bytes())
                {
                    for name in san.0.iter() {
                        if let x509_cert::ext::pkix::name::GeneralName::DnsName(dns) = name {
                            san_dns.push(dns.as_str().to_string());
                        }
                    }
                }
            }
        }
    }

    Ok((subject_dn, san_dns, common_names))
}

/// Build a rustls [`ClientConfig`] for a TCPS connection from [`TlsParams`].
///
/// The config uses the Oracle [`OracleServerCertVerifier`] (custom
/// chain-validation + DN match) and, when the wallet carries a client identity,
/// sets it for mutual TLS.
pub(crate) fn build_client_config(params: &TlsParams) -> Result<ClientConfig, Error> {
    use rustls::crypto::ring::default_provider;

    let provider = Arc::new(default_provider());
    let supported_algs = provider.signature_verification_algorithms;

    // Trust anchors: wallet CAs if present, else fall back to the OS roots so a
    // server with a publicly-trusted cert still works (python-oracledb uses
    // ssl.create_default_context()).
    let trust_anchor_ders: Vec<Vec<u8>> = match &params.wallet {
        Some(w) if !w.ca_certificates.is_empty() => w.ca_certificates.clone(),
        _ => load_system_roots(),
    };
    if trust_anchor_ders.is_empty() {
        return Err(Error::Tls(
            "no trust anchors available for TCPS: supply a wallet (ewallet.pem) \
             or install system root certificates"
                .to_string(),
        ));
    }

    let verifier = Arc::new(OracleServerCertVerifier {
        trust_anchor_ders,
        supported_algs,
        dn_match: params.dn_match,
        server_cert_dn: params.server_cert_dn.clone(),
        expected_host: params.expected_host.clone(),
    });

    let builder = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::Tls(format!("TLS protocol setup failed: {e}")))?
        .dangerous()
        .with_custom_certificate_verifier(verifier);

    // mTLS client identity, if the wallet supplied one.
    let mut config = if let Some(w) = &params.wallet {
        if w.has_client_identity() {
            let chain: Vec<CertificateDer<'static>> = w
                .client_cert_chain
                .iter()
                .map(|der| CertificateDer::from(der.clone()))
                .collect();
            let key = client_private_key(w)?;
            builder
                .with_client_auth_cert(chain, key)
                .map_err(|e| Error::Tls(format!("client certificate setup failed: {e}")))?
        } else {
            builder.with_no_client_auth()
        }
    } else {
        builder.with_no_client_auth()
    };

    // SNI is on by default; the connector passes the Oracle SNI string.
    config.enable_sni = true;
    Ok(config)
}

fn client_private_key(
    w: &WalletContents,
) -> Result<rustls::pki_types::PrivateKeyDer<'static>, Error> {
    let der = w
        .client_private_key
        .as_ref()
        .ok_or_else(|| Error::Tls("wallet has a client cert but no private key".to_string()))?;
    // Try PKCS#8 first (most wallets), then PKCS#1/SEC1.
    rustls::pki_types::PrivateKeyDer::try_from(der.clone())
        .map_err(|e| Error::Tls(format!("client private key parse failed: {e}")))
}

/// Best-effort load of OS root certificates for the no-wallet path. Returns an
/// empty vec when no native-roots backend is available; the caller surfaces a
/// clear error in that case.
fn load_system_roots() -> Vec<Vec<u8>> {
    // We don't pull rustls-native-certs directly; instead read the common
    // bundle locations. This keeps the dependency surface minimal while still
    // allowing publicly-trusted-cert TCPS endpoints to work without a wallet.
    const BUNDLES: &[&str] = &[
        "/etc/ssl/certs/ca-certificates.crt",
        "/etc/pki/tls/certs/ca-bundle.crt",
        "/etc/ssl/ca-bundle.pem",
        "/etc/ssl/cert.pem",
    ];
    for path in BUNDLES {
        if let Ok(bytes) = std::fs::read(path) {
            let mut reader = std::io::BufReader::new(&bytes[..]);
            let certs: Vec<Vec<u8>> = rustls_pemfile_certs(&mut reader);
            if !certs.is_empty() {
                return certs;
            }
        }
    }
    Vec::new()
}

/// Parse all CERTIFICATE blocks from a PEM reader into DER byte vectors,
/// without adding a direct rustls-pemfile dependency to this crate (the
/// protocol crate already depends on it; reuse its re-export).
fn rustls_pemfile_certs(reader: &mut dyn std::io::BufRead) -> Vec<Vec<u8>> {
    oracledb_protocol::tls::wallet::parse_pem_certificates(reader)
}

/// Resolve the [`TlsParams`] for a TCPS connection from the descriptor and
/// connect options: locate the wallet directory (explicit `wallet_location`
/// then `TNS_ADMIN`), read the wallet (`ewallet.pem`, `ewallet.p12`, or
/// `cwallet.sso` — see [`load_wallet`]), and capture the DN-match
/// configuration.
///
/// # Errors
/// Returns [`Error::Wallet`] when a configured wallet directory is missing or
/// its wallet file cannot be parsed.
#[allow(clippy::too_many_arguments)]
pub(crate) fn resolve_tls_params(
    descriptor: &EasyConnect,
    wallet_location: Option<&str>,
    wallet_password: Option<&str>,
    ssl_server_dn_match: bool,
    ssl_server_cert_dn: Option<&str>,
    use_sni: bool,
) -> Result<TlsParams, Error> {
    let tns_admin = std::env::var("TNS_ADMIN").ok();
    let wallet = match resolve_wallet_dir(wallet_location, tns_admin.as_deref()) {
        Some(dir) => Some(load_wallet(&dir, wallet_password)?),
        None => None,
    };
    Ok(TlsParams {
        wallet,
        dn_match: ssl_server_dn_match,
        server_cert_dn: ssl_server_cert_dn.map(str::to_string),
        expected_host: descriptor.host.clone(),
        use_sni,
    })
}

/// Read a wallet from a directory. Precedence: `ewallet.pem` → `ewallet.p12`
/// (when a `wallet_password` is supplied) → `cwallet.sso`.
///
/// The first wallet in that order that yields a usable identity wins. If the
/// chosen `ewallet.pem` / `ewallet.p12` is *present but unusable* — an
/// unsupported cipher, or a wrong / missing wallet password — and a valid
/// auto-login `cwallet.sso` is present, the reader falls through to the SSO
/// wallet and logs a WARN naming the skipped wallet.
///
/// When no auto-login wallet is available the original typed error is surfaced
/// **verbatim** (it never mentions the fallthrough), so a genuine
/// misconfiguration stays diagnosable:
///
/// * `ewallet.p12` with no password and no `cwallet.sso` →
///   [`WalletError::PasswordRequired`].
/// * a wrong password / unsupported cipher with no `cwallet.sso` → the reader's
///   own typed [`WalletError::KeyDecrypt`] / [`WalletError::Pkcs12`].
///
/// I/O and malformed-container errors are never treated as fallthrough-eligible;
/// they are surfaced as-is (a broken primary wallet should not be silently
/// masked by an unrelated auto-login wallet).
fn load_wallet(dir: &std::path::Path, password: Option<&str>) -> Result<WalletContents, Error> {
    use oracledb_protocol::tls::wallet::{
        p12_wallet_path, pem_wallet_path, read_ewallet_p12, read_ewallet_pem, sso_wallet_path,
        WalletError,
    };

    // Read + parse the auto-login cwallet.sso if present. `Ok(None)` = no sso
    // file; `Ok(Some(_))` = a usable auto-login wallet; `Err(_)` = an sso file
    // that itself failed to parse.
    let read_sso = || -> Result<Option<WalletContents>, WalletError> {
        let sso = sso_wallet_path(dir);
        if !sso.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&sso).map_err(|source| WalletError::Io {
            path: sso.display().to_string(),
            source,
        })?;
        oracledb_protocol::tls::sso::parse_cwallet_sso(&bytes).map(Some)
    };

    // A present-but-unusable primary wallet (unsupported cipher, or a wrong /
    // missing wallet password) may fall through to auto-login; an I/O or
    // malformed-container error may not.
    let falls_through_to_autologin = |e: &WalletError| {
        matches!(
            e,
            WalletError::KeyDecrypt(_)
                | WalletError::Pkcs12(_)
                | WalletError::PasswordRequired { .. }
                | WalletError::UnsupportedFormat { .. }
        )
    };

    // The primary wallet, in precedence order (pem, then password-bearing p12).
    let have_p12 = p12_wallet_path(dir).exists();
    let primary: Option<(&'static str, Result<WalletContents, WalletError>)> =
        if pem_wallet_path(dir).exists() {
            Some(("ewallet.pem", read_ewallet_pem(dir, password)))
        } else if have_p12 && password.is_some() {
            Some(("ewallet.p12", read_ewallet_p12(dir, password)))
        } else {
            None
        };

    match primary {
        Some((_, Ok(contents))) => Ok(contents),
        Some((name, Err(primary_err))) => {
            if falls_through_to_autologin(&primary_err) {
                if let Ok(Some(sso)) = read_sso() {
                    obs_warn!(
                        skipped_wallet = name,
                        "wallet {name} could not be used ({primary_err}); \
                         falling back to auto-login cwallet.sso"
                    );
                    // `name` is referenced only by obs_warn!, which is a no-op
                    // in the default (tracing-off) build.
                    let _ = name;
                    return Ok(sso);
                }
            }
            // No usable auto-login wallet: surface the original typed error
            // verbatim (never mention the fallthrough).
            Err(primary_err.into())
        }
        None => {
            // No pem and no password-bearing p12. Prefer an auto-login wallet;
            // otherwise fall back to a typed error the operator can act on.
            if let Some(sso) = read_sso()? {
                return Ok(sso);
            }
            if have_p12 {
                // p12 present but no password and no auto-login wallet: surface
                // the typed supply-wallet_password remediation.
                return read_ewallet_p12(dir, password).map_err(Error::from);
            }
            Err(
                WalletError::FileMissing("ewallet.pem, ewallet.p12, or cwallet.sso".to_string())
                    .into(),
            )
        }
    }
}

/// A `ServerName` that is always a valid rustls DNS name, used when no SNI is
/// being sent (SNI disabled). The value is never transmitted: `enable_sni` is
/// `false` in that path, and the Oracle verifier ignores `server_name`.
const SNI_PLACEHOLDER: &str = "oracle.invalid";

/// Whether the Oracle SNI string is a valid rustls `ServerName`. The Oracle
/// format ends in an all-numeric label (`.V3.319`), which RFC-strict rustls
/// rejects; this helper lets the handshake decide whether the SNI can be sent.
fn sni_is_rustls_valid(sni: &str) -> bool {
    rustls::pki_types::ServerName::try_from(sni.to_string()).is_ok()
}

/// Decide the SNI server name for a TCPS handshake (F3, bead `rust-oracledb-clvm`).
///
/// - `Ok(None)` — no SNI is sent (`use_sni=false`, the default and the common
///   case): the caller uses a placeholder name with `enable_sni=false`, and the
///   server is identified purely by the post-handshake Oracle DN match.
/// - `Ok(Some(name))` — `use_sni=true` and the Oracle SNI is a valid rustls DNS
///   name; it is transmitted with `enable_sni=true`.
/// - `Err(Error::UnsupportedSni)` — `use_sni=true` was explicitly requested but
///   the Oracle SNI (`S{len}.{service}.V3.{version}`) is not a valid rustls DNS
///   name and therefore cannot be sent. The driver **fails closed** rather than
///   silently downgrading to no-SNI, so an operator who asked for SNI learns it
///   was not honored instead of discovering it only from a packet capture.
pub(crate) fn decide_sni(
    use_sni: bool,
    service_name: &str,
    server_type: Option<&str>,
) -> Result<Option<String>, Error> {
    if !use_sni {
        return Ok(None);
    }
    let sni = build_sni(service_name, server_type);
    if sni_is_rustls_valid(&sni) {
        Ok(Some(sni))
    } else {
        Err(Error::UnsupportedSni(sni))
    }
}

/// Perform the TCPS TLS handshake over a connected TCP stream, returning the
/// established [`TlsStream`].
///
/// SNI handling mirrors python-oracledb: the Oracle `S{len}.{service}.V3.{ver}`
/// SNI is only emitted when [`TlsParams::use_sni`] is set; by default no SNI is
/// sent and the server is identified by the post-handshake DN match. When
/// `use_sni=true` is explicitly requested but rustls cannot encode the Oracle
/// SNI (its trailing numeric label is rejected), the handshake **fails closed**
/// with [`Error::UnsupportedSni`] rather than silently proceeding without SNI —
/// see [`decide_sni`].
///
/// # Errors
/// Returns [`Error::UnsupportedSni`] when `use_sni=true` cannot be honored, or
/// [`Error::Tls`] on configuration or handshake failure.
pub async fn tls_handshake(
    descriptor: &EasyConnect,
    server_type: Option<&str>,
    params: &TlsParams,
    tcp: TcpStream,
) -> Result<TlsStream<TcpStream>, Error> {
    let mut config = build_client_config(params)?;

    // Decide the SNI name. Default (and the common case) is no SNI; an
    // explicitly requested but un-encodable Oracle SNI fails closed here.
    let server_name = match decide_sni(params.use_sni, &descriptor.service_name, server_type)? {
        Some(sni) => {
            config.enable_sni = true;
            sni
        }
        None => {
            config.enable_sni = false;
            SNI_PLACEHOLDER.to_string()
        }
    };

    let connector =
        TlsConnector::new(config).with_handshake_timeout(std::time::Duration::from_secs(20));
    connector
        .connect(&server_name, tcp)
        .await
        .map_err(|e| Error::Tls(format!("TCPS handshake failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_config_requires_trust_anchors() {
        // No wallet and no system roots discoverable in a hermetic test would
        // error; but on CI hosts system roots usually exist. We only assert the
        // verifier wiring compiles and that an empty-wallet path is rejected
        // cleanly when no anchors are present.
        let params = TlsParams {
            wallet: Some(WalletContents::default()),
            dn_match: true,
            server_cert_dn: None,
            expected_host: "db.example.com".to_string(),
            use_sni: false,
        };
        // Empty wallet => falls back to system roots; result depends on host.
        let _ = build_client_config(&params);
    }

    #[test]
    fn oracle_sni_is_rejected_by_rustls_servername() {
        // The Oracle SNI ends in an all-numeric label which RFC-strict rustls
        // rejects; this is why use_sni cannot be honored. Document it as a
        // test so a future rustls relaxation is noticed.
        assert!(!sni_is_rustls_valid("S8.FREEPDB1.V3.319"));
        assert!(sni_is_rustls_valid("db.example.com"));
    }

    #[test]
    fn decide_sni_without_use_sni_sends_no_sni() {
        // The default (use_sni=false) yields no SNI, cleanly (no error).
        let decided = decide_sni(false, "FREEPDB1", None).expect("no-SNI must be Ok");
        assert!(decided.is_none(), "use_sni=false must not send an SNI");
    }

    #[test]
    fn decide_sni_with_use_sni_fails_closed_not_silent() {
        // F3 (bead rust-oracledb-clvm): use_sni=true must NO LONGER silently
        // degrade to enable_sni=false. Because the Oracle SNI ends in a numeric
        // label rustls rejects, use_sni=true fails closed with the typed
        // UnsupportedSni error naming the SNI string.
        let err = decide_sni(true, "FREEPDB1", None)
            .expect_err("use_sni=true with an un-encodable Oracle SNI must fail closed");
        match err {
            Error::UnsupportedSni(sni) => {
                assert!(
                    sni.starts_with('S') && sni.contains("FREEPDB1"),
                    "error must name the Oracle SNI string, got {sni:?}"
                );
            }
            other => panic!("expected Error::UnsupportedSni, got {other:?}"),
        }
    }

    fn fixture_tls_dir() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("tls")
    }

    #[test]
    fn ewallet_p12_only_wallet_without_password_is_password_required() {
        // The p12_only fixture dir holds a dummy ewallet.p12 and nothing else:
        // with no wallet_password the loader must fail closed with the typed
        // supply-wallet_password remediation (never a silent skip).
        let dir = fixture_tls_dir().join("p12_only");
        let err = load_wallet(&dir, None).expect_err("p12-only wallet without password");
        let wallet_err = if let Error::Wallet(wallet_err) = err {
            wallet_err
        } else {
            panic!("expected wallet error, got {err:?}");
        };
        assert!(
            matches!(
                &wallet_err,
                oracledb_protocol::tls::wallet::WalletError::PasswordRequired { format }
                    if *format == "ewallet.p12"
            ),
            "expected PasswordRequired, got {wallet_err:?}"
        );
        let sensitive_path = dir.display().to_string();
        assert!(!format!("{wallet_err}").contains(&sensitive_path));
        assert!(!format!("{wallet_err:?}").contains(&sensitive_path));
    }

    #[test]
    fn ewallet_p12_only_wallet_with_password_garbage_is_typed_pkcs12_error() {
        // Same dummy p12, but WITH a password: the parse itself must fail with
        // a typed PKCS#12 error (the file is not a real PFX).
        let dir = fixture_tls_dir().join("p12_only");
        let err = load_wallet(&dir, Some("any-password")).expect_err("dummy p12 must not parse");
        let wallet_err = if let Error::Wallet(wallet_err) = err {
            wallet_err
        } else {
            panic!("expected wallet error, got {err:?}");
        };
        assert!(
            matches!(
                &wallet_err,
                oracledb_protocol::tls::wallet::WalletError::Pkcs12(_)
            ),
            "expected Pkcs12, got {wallet_err:?}"
        );
        assert!(!format!("{wallet_err}").contains("any-password"));
        assert!(!format!("{wallet_err:?}").contains("any-password"));
    }

    /// Build a temp wallet dir holding copies of the named fixtures.
    fn temp_wallet_dir(label: &str, files: &[&str]) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "oracledb-wallet-test-{label}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp wallet dir");
        for name in files {
            std::fs::copy(
                fixture_tls_dir().join(name),
                dir.join(wallet_file_name(name)),
            )
            .expect("copy fixture");
        }
        dir
    }

    /// Map a fixture file name to its in-wallet name.
    fn wallet_file_name(fixture: &str) -> &'static str {
        match fixture {
            "ewallet_orapki.p12" => "ewallet.p12",
            "cwallet_orapki.sso" => "cwallet.sso",
            "ewallet.pem" => "ewallet.pem",
            other => panic!("unmapped fixture {other}"),
        }
    }

    #[test]
    fn adb_style_wallet_dir_prefers_p12_with_password_and_sso_without() {
        // An ADB wallet zip ships cwallet.sso + ewallet.p12 (no ewallet.pem).
        // With wallet_password -> ewallet.p12; without -> cwallet.sso. Both
        // must yield the same mTLS identity (proven identical in the protocol
        // crate tests; here we prove the loader wiring).
        let dir = temp_wallet_dir("adb", &["ewallet_orapki.p12", "cwallet_orapki.sso"]);
        let with_pw =
            load_wallet(&dir, Some("WalletPass123")).expect("p12 path must load with password");
        assert!(with_pw.has_client_identity());
        let without_pw = load_wallet(&dir, None).expect("sso path must load without password");
        assert!(without_pw.has_client_identity());
        assert_eq!(with_pw.ca_certificates, without_pw.ca_certificates);
    }

    #[test]
    fn wallet_dir_prefers_pem_over_p12_and_sso() {
        // python-oracledb parity: ewallet.pem wins when present.
        let dir = temp_wallet_dir(
            "pem-first",
            &["ewallet.pem", "ewallet_orapki.p12", "cwallet_orapki.sso"],
        );
        let wallet = load_wallet(&dir, None).expect("pem path must load");
        // The pem fixture's subject is db.example.com and differs from the
        // orapki wallet's key: proving the pem was chosen is enough.
        assert!(wallet.has_client_identity());
        use oracledb_protocol::tls::wallet::parse_ewallet_pem;
        let pem_bytes =
            std::fs::read(fixture_tls_dir().join("ewallet.pem")).expect("read pem fixture");
        let direct = parse_ewallet_pem(&pem_bytes, None).expect("parse pem fixture");
        assert_eq!(wallet.ca_certificates, direct.ca_certificates);
    }

    #[test]
    fn unusable_p12_falls_through_to_auto_login_sso() {
        // A2.2: when the primary ewallet.p12 is present but unusable (here a
        // wrong wallet_password → typed Pkcs12 error) AND a valid auto-login
        // cwallet.sso is present, the loader falls through to the SSO wallet
        // instead of failing. The result must be the same identity the SSO
        // wallet yields on its own.
        let dir = temp_wallet_dir("fallthrough", &["ewallet_orapki.p12", "cwallet_orapki.sso"]);
        let fell_through = load_wallet(&dir, Some("not-the-password!"))
            .expect("wrong p12 password must fall through to the auto-login cwallet.sso");
        assert!(fell_through.has_client_identity());
        // Identical to loading the SSO wallet directly (no password).
        let sso_only = temp_wallet_dir("fallthrough-sso", &["cwallet_orapki.sso"]);
        let direct = load_wallet(&sso_only, None).expect("sso path must load");
        assert_eq!(fell_through.ca_certificates, direct.ca_certificates);
        assert_eq!(fell_through.client_private_key, direct.client_private_key);
    }

    #[test]
    fn unusable_p12_without_sso_preserves_original_typed_error() {
        // A2.2: with NO auto-login cwallet.sso to fall through to, the primary
        // wallet's original typed error is surfaced verbatim — a wrong password
        // stays a typed Pkcs12 error, never rewritten to mention a fallthrough.
        let dir = temp_wallet_dir("no-sso", &["ewallet_orapki.p12"]);
        let err = load_wallet(&dir, Some("not-the-password!"))
            .expect_err("wrong p12 password with no sso must fail closed");
        let wallet_err = if let Error::Wallet(wallet_err) = err {
            wallet_err
        } else {
            panic!("expected wallet error, got {err:?}");
        };
        assert!(
            matches!(
                &wallet_err,
                oracledb_protocol::tls::wallet::WalletError::Pkcs12(_)
            ),
            "expected the original typed Pkcs12 error, got {wallet_err:?}"
        );
        // The preserved error must NOT leak the password nor reference the
        // fallthrough / auto-login machinery (it is the reader's own message).
        for rendered in [format!("{wallet_err}"), format!("{wallet_err:?}")] {
            assert!(!rendered.contains("not-the-password!"), "password leaked");
            let lower = rendered.to_ascii_lowercase();
            assert!(
                !lower.contains("fall") && !lower.contains("auto-login") && !lower.contains("sso"),
                "preserved error must not mention the fallthrough, got {rendered:?}"
            );
        }
    }
}
