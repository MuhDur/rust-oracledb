//! TCPS (TLS) transport: the Oracle server-certificate verifier and the
//! rustls `ClientConfig` construction.
//!
//! The actual handshake is driven by asupersync's [`TlsConnector`] over the
//! async TCP socket; we supply a custom rustls [`ServerCertVerifier`] that
//! reproduces python-oracledb thin's behaviour:
//!
//! * standard hostname verification is **disabled** (the SNI value is an
//!   Oracle routing name, not necessarily a certificate identity), and
//! * after the chain is validated to a trust anchor, the Oracle DN/SAN/CN match
//!   ([`oracledb_protocol::tls::dn`]) is run instead.
//!
//! Chain validation itself (signature + path to a trust anchor) is delegated to
//! `rustls-webpki`'s name-unbound `EndEntityCert::verify_for_usage`, which is
//! the same crypto rustls uses internally, just without the SNI name binding —
//! exactly mirroring OpenSSL `check_hostname = False` plus `CERT_REQUIRED`.

use std::sync::Arc;
use std::time::Duration;

use asupersync::net::TcpStream;
use asupersync::tls::{TlsConnector, TlsStream};
use oracledb_protocol::net::EasyConnect;
use oracledb_protocol::tls::dn::{check_cert_dn, check_server_name, DnMatchError};
use oracledb_protocol::tls::sni::build_sni;
use oracledb_protocol::tls::wallet::{resolve_wallet_dir, WalletContents};
use rustls::client::{
    danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    ResolvesClientCert,
};
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::sign::CertifiedKey;
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
    /// Request the Oracle TCPS SNI fast path (`use_sni`, reference default
    /// `false`).
    ///
    /// python-oracledb only emits the `S{len}.{service}.V3.{version}` SNI when
    /// `use_sni=True` is explicitly requested; by default no SNI is sent and the
    /// server is identified purely by the post-handshake DN match. See
    /// [`decide_sni`] for the strict SNI-selection rules.
    pub use_sni: bool,
}

/// Fully validated TLS setup for one connect descriptor.
///
/// Construction performs every deterministic operation that can fail before
/// network I/O (client-config construction, wallet key validation, and SNI
/// selection). A prepared value can therefore be reused across address
/// failover attempts without allowing a configuration error to masquerade as
/// a transient handshake failure.
pub(crate) struct PreparedTlsHandshake {
    config: ClientConfig,
    server_name: String,
}

/// A failure produced only after a prepared TLS configuration starts its
/// network handshake.
///
/// Keeping this distinct from [`Error::Tls`] internally makes the address
/// failover boundary stage-aware without changing the public error enum.
#[derive(Debug)]
pub(crate) struct TlsHandshakeError(String);

impl TlsHandshakeError {
    pub(crate) fn new(message: String) -> Self {
        Self(message)
    }

    pub(crate) fn into_driver_error(self) -> Error {
        Error::Tls(format!("TCPS handshake failed: {}", self.0))
    }
}

impl std::fmt::Display for TlsHandshakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TLS/TCPS error: TCPS handshake failed: {}", self.0)
    }
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

/// A wallet identity selected for every certificate request.
///
/// Oracle Autonomous Database wallets can contain an X.509 v1 client
/// certificate. rustls's [`ClientConfig::with_client_auth_cert`] convenience
/// constructor rejects that certificate while checking the private-key match,
/// because the webpki parser it uses only accepts v3 certificates. The TLS
/// protocol itself transmits the client chain as DER and needs only the private
/// key to sign the handshake. This resolver therefore keeps the parsed key and
/// original wallet DER together without parsing the client certificate through
/// webpki. The peer still performs the authoritative client-certificate chain
/// validation, while our independent server-certificate and Oracle DN checks
/// remain unchanged.
#[derive(Debug)]
struct StaticClientCert(Arc<CertifiedKey>);

impl ResolvesClientCert for StaticClientCert {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(&self.0))
    }

    fn has_certs(&self) -> bool {
        true
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
    let key_provider = provider.key_provider;
    let supported_algs = provider.signature_verification_algorithms;

    // python-oracledb starts with ssl.create_default_context() and then adds
    // the wallet's CAs. Keep that union: an ADB wallet can supply mTLS and
    // private roots while the server chain still terminates at a public root.
    let trust_anchor_ders = union_trust_anchors(load_system_roots(), params.wallet.as_ref());
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
            let signing_key = key_provider
                .load_private_key(key)
                .map_err(|e| Error::Tls(format!("client private key rejected: {e}")))?;
            let certified = Arc::new(CertifiedKey::new(chain, signing_key));
            builder.with_client_cert_resolver(Arc::new(StaticClientCert(certified)))
        } else {
            builder.with_no_client_auth()
        }
    } else {
        builder.with_no_client_auth()
    };

    // SNI is on by default; the connector selects the per-descriptor SNI name.
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

/// Best-effort load of platform root certificates.
///
/// `rustls-native-certs` consults the native Windows/macOS/Linux store and
/// honors `SSL_CERT_FILE` / `SSL_CERT_DIR`. The legacy Unix bundle locations
/// remain a fallback for hosts whose native loader yields no roots, but never
/// override an explicit environment-root selection.
fn load_system_roots() -> Vec<Vec<u8>> {
    let native = rustls_native_certs::load_native_certs();
    let native_roots: Vec<Vec<u8>> = native
        .certs
        .into_iter()
        .map(|certificate| certificate.as_ref().to_vec())
        .collect();
    if !native_roots.is_empty() || explicit_root_override_is_set() {
        return native_roots;
    }

    // Fallback for Unix hosts where native discovery has no usable roots.
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

fn explicit_root_override_is_set() -> bool {
    std::env::var_os("SSL_CERT_FILE").is_some() || std::env::var_os("SSL_CERT_DIR").is_some()
}

fn union_trust_anchors(
    mut system_roots: Vec<Vec<u8>>,
    wallet: Option<&WalletContents>,
) -> Vec<Vec<u8>> {
    if let Some(wallet) = wallet {
        system_roots.extend(wallet.ca_certificates.iter().cloned());
    }
    system_roots
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
/// F-DC3: `ssl_server_dn_match=false` deliberately disables the Oracle
/// server-certificate identity check (`OracleServerCertVerifier::run_dn_match`
/// short-circuits to `Ok` — chain-of-trust validation still applies, only the
/// DN/SAN/CN match is skipped). That is a real security posture change for
/// this connection, so it is surfaced as an audited WARN rather than a silent
/// downgrade: an operator grepping logs can see exactly when and where DN
/// matching was turned off, instead of discovering it only by noticing that
/// impersonation would have gone undetected. This only fires when the
/// *effective* (options-merged-with-descriptor) value is off, so it reflects
/// the same resolved setting the verifier actually uses — never an
/// approximation that could itself drift from what is enforced.
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
    if !ssl_server_dn_match {
        // Explicit, audited: SSL_SERVER_DN_MATCH=OFF (or with_ssl_server_dn_match(false))
        // must never be an invisible bypass. A pinned SSL_SERVER_CERT_DN, if
        // any, is named here too, since it is what would have been enforced.
        obs_warn!(
            server.host = %descriptor.host,
            server.port = descriptor.port as u64,
            pinned_cert_dn_configured = ssl_server_cert_dn.is_some(),
            "TCPS SSL_SERVER_DN_MATCH=OFF: the Oracle server-certificate identity check \
             (DN/SAN/CN match) is disabled for this connection; TLS chain-of-trust \
             validation still applies, but the peer's identity is not verified. This must be \
             a deliberate, explicit choice, never a default."
        );
    }
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
    resolve_wallet_inner(dir, password).map(|(_, contents)| contents)
}

/// Which wallet file in a wallet directory supplied the resolved TLS identity.
///
/// The precedence order the driver applies (mirroring python-oracledb) is
/// `ewallet.pem` → `ewallet.p12` → `cwallet.sso`; this is the same order used
/// by the driver's internal wallet loader.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WalletFile {
    /// `ewallet.pem` (PEM trust anchors + optional client identity).
    Pem,
    /// `ewallet.p12` (password-bearing PKCS#12 wallet).
    P12,
    /// `cwallet.sso` (SSO auto-login wallet).
    Sso,
}

impl WalletFile {
    /// The on-disk file name of this wallet file (`ewallet.pem`,
    /// `ewallet.p12`, or `cwallet.sso`).
    #[must_use]
    pub fn file_name(self) -> &'static str {
        use oracledb_protocol::tls::wallet::{
            P12_WALLET_FILE_NAME, PEM_WALLET_FILE_NAME, SSO_WALLET_FILE_NAME,
        };
        match self {
            WalletFile::Pem => PEM_WALLET_FILE_NAME,
            WalletFile::P12 => P12_WALLET_FILE_NAME,
            WalletFile::Sso => SSO_WALLET_FILE_NAME,
        }
    }
}

/// The outcome of wallet-file precedence resolution in a wallet directory.
///
/// [`resolve_wallet`] returns this so a caller (e.g. a server doctor) can report
/// exactly which wallet file won — and whether resolution fell through the
/// precedence chain to the auto-login wallet — instead of re-deriving the
/// driver's precedence by hand and risking drift. It is the *same* decision the
/// internal wallet loader makes (both go through the one resolver), just without
/// the parsed key material.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalletResolution {
    /// The wallet file that supplied the resolved identity.
    pub chosen: WalletFile,
    /// The primary wallet (`ewallet.pem` or `ewallet.p12`) the driver attempted
    /// before any fallthrough, or `None` when no primary was present and the
    /// auto-login `cwallet.sso` was chosen directly.
    pub attempted_primary: Option<WalletFile>,
    /// `true` when the primary wallet was present but unusable and resolution
    /// fell through to the auto-login `cwallet.sso`. Implies `chosen == Sso`
    /// and `attempted_primary.is_some()`.
    pub fell_through: bool,
    /// Whether the attempted primary's failure was *fallthrough-eligible* — an
    /// unusable-but-present classification (unsupported cipher, or a wrong /
    /// missing wallet password): `WalletError::{KeyDecrypt, Pkcs12,
    /// PasswordRequired, UnsupportedFormat}`. An I/O or malformed-container
    /// error is never eligible. `false` when there was no primary, or the
    /// primary loaded cleanly.
    pub fallthrough_eligible: bool,
}

/// Resolve which wallet file in `dir` wins the precedence chain and report the
/// [`WalletResolution`] outcome, without exposing the parsed key material.
///
/// This is the public, drift-free accessor for the precedence the internal
/// wallet loader applies. Both go through the same resolver, so the reported
/// decision cannot drift from the one the connection actually uses. Resolution
/// must genuinely read and parse the wallet files (that is the only way to know
/// whether a primary is usable and whether a fallthrough occurred); the parsed
/// [`WalletContents`] is then discarded and only the decision is returned.
/// Offline — no connection or network I/O is involved.
///
/// # Errors
/// Returns the same typed [`Error::Wallet`] the internal loader would surface when no
/// usable wallet is found. When the primary is present but unusable and there is
/// no auto-login `cwallet.sso` to fall through to, the primary's original typed
/// error is preserved verbatim (it never mentions the fallthrough).
pub fn resolve_wallet(
    dir: &std::path::Path,
    password: Option<&str>,
) -> Result<WalletResolution, Error> {
    resolve_wallet_inner(dir, password).map(|(resolution, _)| resolution)
}

/// The single wallet-precedence resolver. Returns both the [`WalletResolution`]
/// decision and the parsed [`WalletContents`]; [`load_wallet`] keeps the
/// contents, [`resolve_wallet`] keeps the decision. Precedence: `ewallet.pem` →
/// password-bearing `ewallet.p12` → auto-login `cwallet.sso`, with a
/// present-but-unusable primary falling through to `cwallet.sso` only for the
/// fallthrough-eligible error classes.
fn resolve_wallet_inner(
    dir: &std::path::Path,
    password: Option<&str>,
) -> Result<(WalletResolution, WalletContents), Error> {
    use oracledb_protocol::tls::wallet::{
        p12_wallet_path, pem_wallet_path, read_ewallet_p12, read_ewallet_pem, read_wallet_file,
        sso_wallet_path, WalletError,
    };

    // Read + parse the auto-login cwallet.sso if present. `Ok(None)` = no sso
    // file; `Ok(Some(_))` = a usable auto-login wallet; `Err(_)` = an sso file
    // that itself failed to parse.
    let read_sso = || -> Result<Option<WalletContents>, WalletError> {
        let sso = sso_wallet_path(dir);
        if !sso.exists() {
            return Ok(None);
        }
        let bytes = read_wallet_file(&sso)?;
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
    let primary: Option<(WalletFile, Result<WalletContents, WalletError>)> =
        if pem_wallet_path(dir).exists() {
            Some((WalletFile::Pem, read_ewallet_pem(dir, password)))
        } else if have_p12 && password.is_some() {
            Some((WalletFile::P12, read_ewallet_p12(dir, password)))
        } else {
            None
        };

    match primary {
        Some((file, Ok(contents))) => Ok((
            WalletResolution {
                chosen: file,
                attempted_primary: Some(file),
                fell_through: false,
                fallthrough_eligible: false,
            },
            contents,
        )),
        Some((file, Err(primary_err))) => {
            if falls_through_to_autologin(&primary_err) {
                if let Ok(Some(sso)) = read_sso() {
                    let name = file.file_name();
                    obs_warn!(
                        skipped_wallet = name,
                        "wallet {name} could not be used ({primary_err}); \
                         falling back to auto-login cwallet.sso"
                    );
                    // `name` is referenced only by obs_warn!, which is a no-op
                    // in the default (tracing-off) build.
                    let _ = name;
                    return Ok((
                        WalletResolution {
                            chosen: WalletFile::Sso,
                            attempted_primary: Some(file),
                            fell_through: true,
                            fallthrough_eligible: true,
                        },
                        sso,
                    ));
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
                return Ok((
                    WalletResolution {
                        chosen: WalletFile::Sso,
                        attempted_primary: None,
                        fell_through: false,
                        fallthrough_eligible: false,
                    },
                    sso,
                ));
            }
            if have_p12 {
                // p12 present but no password and no auto-login wallet: surface
                // the typed supply-wallet_password remediation.
                return read_ewallet_p12(dir, password)
                    .map(|contents| {
                        (
                            WalletResolution {
                                chosen: WalletFile::P12,
                                attempted_primary: Some(WalletFile::P12),
                                fell_through: false,
                                fallthrough_eligible: false,
                            },
                            contents,
                        )
                    })
                    .map_err(Error::from);
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

/// Whether `name` is a DNS name rustls can encode in ClientHello SNI.
///
/// `ServerName` also accepts IP addresses, but rustls deliberately omits the
/// SNI extension for them. Require its DNS variant here so `Some(name)` always
/// means the extension will actually be transmitted.
fn sni_is_rustls_valid(sni: &str) -> bool {
    matches!(
        rustls::pki_types::ServerName::try_from(sni.to_string()),
        Ok(ServerName::DnsName(_))
    )
}

/// Whether `host` looks like an OCI Autonomous Database listener hostname.
///
/// Covered shapes (case-insensitive):
/// - Shared / public LB: `adb.<region>.oraclecloud.com`
/// - Private endpoint / node-qualified: `<label>.adb.<region>.oraclecloud.com`
/// - Sovereign clouds: the same patterns under `.oraclecloud.eu`,
///   `.oraclegovcloud.com`, and `.oraclecloud.com.au`
///
/// Rejects bare IPs, customer custom DNS, and non-OCI hosts. The SNI host
/// fallback is only a ClientHello label — the post-handshake Oracle DN/name
/// match remains authoritative.
fn is_oci_adb_host(host: &str) -> bool {
    let host = host.to_ascii_lowercase();
    // Reject anything that is not a plain hostname (IPs, empty, wildcards).
    if host.is_empty() || host.contains(':') || host.starts_with('.') || host.ends_with('.') {
        return false;
    }
    let Some(rest) = strip_oci_cloud_suffix(&host) else {
        return false;
    };
    // rest is everything before the cloud suffix, e.g.
    //   "adb.eu-frankfurt-1" or "pe1.adb.us-ashburn-1"
    rest == "adb" || rest.starts_with("adb.") || rest.ends_with(".adb") || rest.contains(".adb.")
}

/// Whether `service_name` is an OCI ADB service name (`*.adb.<region>.…`).
fn is_oci_adb_service(service_name: &str) -> bool {
    let service_name = service_name.to_ascii_lowercase();
    let Some(rest) = strip_oci_cloud_suffix(&service_name) else {
        return false;
    };
    // Shared and PE services both end as `<db>_<tier>.adb.<region>` before the
    // cloud suffix → rest contains ".adb." (or ends with ".adb" for pathological
    // short names; require the ".adb." form in practice).
    rest.contains(".adb.") || rest.ends_with(".adb")
}

/// Strip a known OCI commercial / sovereign cloud DNS suffix.
///
/// Returns the hostname prefix before the suffix, or `None` if no known suffix
/// matches.
fn strip_oci_cloud_suffix(name: &str) -> Option<&str> {
    // Longest match first so `.oraclecloud.com.au` wins over `.oraclecloud.com`.
    const SUFFIXES: &[&str] = &[
        ".oraclecloud.com.au",
        ".oraclegovcloud.com",
        ".oraclecloud.eu",
        ".oraclecloud.com",
    ];
    for suffix in SUFFIXES {
        if let Some(rest) = name.strip_suffix(suffix) {
            if !rest.is_empty() {
                return Some(rest);
            }
        }
    }
    None
}

/// Whether this descriptor is an OCI Autonomous Database shape eligible for
/// the host-as-SNI fallback when the Oracle service-form SNI token is not a
/// rustls-valid DNS name.
///
/// OCI wallet descriptors separate the listener endpoint hostname from the
/// database service (`*.adb.<region>.oraclecloud.com`). Only this family may
/// use the endpoint as an SNI fallback; all other unencodable Oracle
/// service-form SNI values remain fail-closed with [`Error::UnsupportedSni`].
fn is_oci_adb_endpoint(host: &str, service_name: &str) -> bool {
    is_oci_adb_host(host) && is_oci_adb_service(service_name)
}

/// Decide the SNI server name for a TCPS handshake (F3, bead `rust-oracledb-clvm`).
///
/// - `Ok(None)` — no SNI is sent (`use_sni=false`, the default and the common
///   case): the caller uses a placeholder name with `enable_sni=false`, and the
///   server is identified purely by the post-handshake Oracle DN match.
/// - `Ok(Some(name))` — `use_sni=true` and `name` is a DNS name rustls can
///   transmit with `enable_sni=true`. The OCI Autonomous Database shape uses
///   its valid descriptor host when the Oracle service-form SNI cannot be
///   represented by rustls (host-as-SNI carve-out; see [`is_oci_adb_endpoint`]).
/// - `Err(Error::UnsupportedSni)` — `use_sni=true` was explicitly requested but
///   the Oracle SNI (`S{len}.{service}.V3.{version}`) is not a valid rustls DNS
///   name and therefore cannot be sent. The driver **fails closed** rather than
///   silently downgrading to no-SNI, so an operator who asked for SNI learns it
///   was not honored instead of discovering it only from a packet capture.
pub(crate) fn decide_sni(
    use_sni: bool,
    host: &str,
    service_name: &str,
    server_type: Option<&str>,
) -> Result<Option<String>, Error> {
    if !use_sni {
        return Ok(None);
    }
    let sni = build_sni(service_name, server_type);
    if sni_is_rustls_valid(&sni) {
        return Ok(Some(sni));
    }

    // OCI ADB endpoints are valid DNS names even though the service-form SNI
    // has the required all-numeric `.V3.<version>` suffix (and often underscores
    // in the service label). The custom verifier still validates the chain and
    // then the Oracle DN/name against `TlsParams::expected_host`; SNI never
    // replaces either. Host-as-SNI completes the handshake but does **not**
    // trigger Oracle's one-negotiation routing fast-path (documented structural
    // parity limit vs python-oracledb; see docs/PARITY_LEDGER.md).
    if is_oci_adb_endpoint(host, service_name) && sni_is_rustls_valid(host) {
        return Ok(Some(host.to_string()));
    }

    Err(Error::UnsupportedSni(sni))
}

/// Validate and prepare all deterministic TLS configuration before a transport
/// address is dialled.
pub(crate) fn prepare_tls_handshake(
    descriptor: &EasyConnect,
    server_type: Option<&str>,
    params: &TlsParams,
) -> Result<PreparedTlsHandshake, Error> {
    let mut config = build_client_config(params)?;

    // Decide the SNI name before any address attempt. Default (and the common
    // case) is no SNI; an explicitly requested but un-encodable Oracle SNI
    // fails closed here without consuming failover retries or the call budget.
    let server_name = match decide_sni(
        params.use_sni,
        &params.expected_host,
        &descriptor.service_name,
        server_type,
    )? {
        Some(sni) => {
            config.enable_sni = true;
            sni
        }
        None => {
            config.enable_sni = false;
            SNI_PLACEHOLDER.to_string()
        }
    };

    Ok(PreparedTlsHandshake {
        config,
        server_name,
    })
}

/// Perform a network TLS handshake from already-validated configuration.
pub(crate) async fn tls_handshake_prepared(
    prepared: &PreparedTlsHandshake,
    tcp: TcpStream,
    handshake_timeout: Duration,
) -> Result<TlsStream<TcpStream>, TlsHandshakeError> {
    let connector =
        TlsConnector::new(prepared.config.clone()).with_handshake_timeout(handshake_timeout);
    connector
        .connect(&prepared.server_name, tcp)
        .await
        .map_err(|error| TlsHandshakeError::new(error.to_string()))
}

/// Perform the TCPS TLS handshake over a connected TCP stream, returning the
/// established [`TlsStream`].
///
/// SNI handling mirrors python-oracledb: the Oracle `S{len}.{service}.V3.{ver}`
/// SNI is only emitted when [`TlsParams::use_sni`] is set; by default no SNI is
/// sent and the server is identified by the post-handshake DN match. The public
/// OCI Autonomous Database descriptor shape instead uses its valid endpoint
/// host when rustls cannot encode that service form. Other unencodable SNI
/// values fail closed with [`Error::UnsupportedSni`] rather than silently
/// proceeding without SNI — see [`decide_sni`].
///
/// `handshake_timeout` bounds the handshake itself. Callers should pass the
/// same configured connect timeout that already bounds the preceding TCP
/// dial (the DSN `TRANSPORT_CONNECT_TIMEOUT`/`CONNECT_TIMEOUT`, resolved
/// identically for easy-connect and full-descriptor DSN forms) rather than a
/// separate hard-coded value, so a short (or long) configured connect
/// timeout is actually honored on the TLS phase, not just the TCP dial.
///
/// # Errors
/// Returns [`Error::UnsupportedSni`] when `use_sni=true` cannot be honored, or
/// [`Error::Tls`] on configuration or handshake failure.
#[cfg(test)]
#[allow(dead_code)] // Direct entry point for the TLS integration-test harness.
pub async fn tls_handshake(
    descriptor: &EasyConnect,
    server_type: Option<&str>,
    params: &TlsParams,
    tcp: TcpStream,
    handshake_timeout: Duration,
) -> Result<TlsStream<TcpStream>, Error> {
    let prepared = prepare_tls_handshake(descriptor, server_type, params)?;
    tls_handshake_prepared(&prepared, tcp, handshake_timeout)
        .await
        .map_err(TlsHandshakeError::into_driver_error)
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
    fn platform_and_wallet_trust_anchors_are_unioned() {
        let platform_root = vec![0x30, 0x01];
        let wallet_root = vec![0x30, 0x02];
        let wallet = WalletContents {
            ca_certificates: vec![wallet_root.clone()],
            ..WalletContents::default()
        };

        assert_eq!(
            union_trust_anchors(vec![platform_root.clone()], Some(&wallet)),
            vec![platform_root, wallet_root]
        );
    }

    #[test]
    fn system_root_loader_honors_ssl_cert_file() {
        assert_system_root_loader_honors_explicit_override(
            "file",
            "system_root_loader_honors_ssl_cert_file",
        );
    }

    #[test]
    fn system_root_loader_honors_ssl_cert_dir() {
        assert_system_root_loader_honors_explicit_override(
            "dir",
            "system_root_loader_honors_ssl_cert_dir",
        );
    }

    #[test]
    fn tls_configuration_failure_is_typed_before_any_transport_attempt() {
        let descriptor =
            EasyConnect::parse("tcps://db.example.com:2484/FREEPDB1").expect("descriptor");
        let params = TlsParams {
            wallet: Some(WalletContents {
                // Any non-empty anchor set keeps this test independent of the
                // host's system-root installation. The bad client key fails
                // before certificate-chain verification is relevant.
                ca_certificates: vec![vec![0x30, 0x00]],
                client_cert_chain: vec![vec![0x30, 0x00]],
                client_private_key: Some(vec![0xff]),
            }),
            dn_match: true,
            server_cert_dn: None,
            expected_host: "db.example.com".to_string(),
            use_sni: false,
        };

        let error = match prepare_tls_handshake(&descriptor, None, &params) {
            Ok(_) => panic!("an invalid client private key must fail TLS preparation"),
            Err(error) => error,
        };
        assert!(
            matches!(error, Error::Tls(ref detail) if detail.contains("private key")),
            "configuration failure must remain the original typed TLS error, got {error:?}"
        );
    }

    #[test]
    fn oracle_service_form_sni_rejection_is_the_terminal_numeric_label() {
        // rustls-pki-types permits underscores in DNS labels. The exact OCI
        // service-form SNI fails because `.319` is an all-numeric *final*
        // label, not because the service name contains an underscore.
        let service_form = "S11.myadb_high.V3.319";
        let err = ServerName::try_from(service_form.to_string())
            .expect_err("an Oracle service-form SNI currently is not a rustls ServerName");
        assert_eq!(err.to_string(), "invalid dns name");

        assert!(ServerName::try_from("myadb_high".to_string()).is_ok());
        assert!(ServerName::try_from("S11.myadb_high.V3.name".to_string()).is_ok());
        assert!(!sni_is_rustls_valid(service_form));
        assert!(!sni_is_rustls_valid("192.0.2.1"));
        assert!(sni_is_rustls_valid("db.example.com"));
    }

    #[test]
    fn decide_sni_without_use_sni_sends_no_sni() {
        // The default (use_sni=false) yields no SNI, cleanly (no error).
        let decided =
            decide_sni(false, "db.example.com", "FREEPDB1", None).expect("no-SNI must be Ok");
        assert!(decided.is_none(), "use_sni=false must not send an SNI");
    }

    #[test]
    fn decide_sni_with_use_sni_fails_closed_not_silent() {
        // F3 (bead rust-oracledb-clvm): use_sni=true must NO LONGER silently
        // degrade to enable_sni=false. Because the Oracle SNI ends in a numeric
        // label rustls rejects, use_sni=true fails closed with the typed
        // UnsupportedSni error naming the SNI string.
        let err = decide_sni(true, "db.example.com", "FREEPDB1", None)
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

    #[test]
    fn decide_sni_for_oci_adb_uses_the_valid_endpoint_host() {
        let host = "adb.eu-frankfurt-1.oraclecloud.com";
        let service_name = "g2bb4261a88e318_myadb_high.adb.oraclecloud.com";

        assert_eq!(
            decide_sni(true, host, service_name, None).expect("OCI host SNI is encodable"),
            Some(host.to_string())
        );
    }

    #[test]
    fn only_the_oci_adb_descriptor_shape_gets_the_host_sni_fallback() {
        // Shared public LB shape.
        assert!(is_oci_adb_endpoint(
            "adb.eu-frankfurt-1.oraclecloud.com",
            "g2bb4261a88e318_myadb_high.adb.oraclecloud.com"
        ));
        // Private-endpoint / node-qualified host (the 0.8.4 predicate miss).
        assert!(is_oci_adb_endpoint(
            "pe1.adb.us-ashburn-1.oraclecloud.com",
            "g2bb4261a88e318_myadb_high.adb.oraclecloud.com"
        ));
        // Sovereign EU commercial cloud.
        assert!(is_oci_adb_endpoint(
            "adb.eu-frankfurt-1.oraclecloud.eu",
            "g2bb4261a88e318_myadb_high.adb.eu-frankfurt-1.oraclecloud.eu"
        ));
        // Case-insensitive.
        assert!(is_oci_adb_endpoint(
            "ADB.US-ASHBURN-1.ORACLECLOUD.COM",
            "G2BB4261A88E318_MYADB_HIGH.ADB.ORACLECLOUD.COM"
        ));

        // Negative: non-OCI host with an ADB-looking service.
        assert!(!is_oci_adb_endpoint(
            "db.example.com",
            "g2bb4261a88e318_myadb_high.adb.oraclecloud.com"
        ));
        // Negative: ADB host with a non-ADB service (local Free, etc.).
        assert!(!is_oci_adb_endpoint(
            "adb.eu-frankfurt-1.oraclecloud.com",
            "FREEPDB1"
        ));
        // Negative: bare IP — never a host-SNI carve-out.
        assert!(!is_oci_adb_endpoint(
            "203.0.113.10",
            "g2bb4261a88e318_myadb_high.adb.oraclecloud.com"
        ));
        // Negative: customer custom DNS, even with ADB-ish service.
        assert!(!is_oci_adb_endpoint(
            "db.customer.example",
            "g2bb4261a88e318_myadb_high.adb.oraclecloud.com"
        ));
    }

    #[test]
    fn decide_sni_private_endpoint_adb_uses_host_fallback() {
        let host = "abcd1234.adb.us-phoenix-1.oraclecloud.com";
        let service = "g2bb4261a88e318_myadb_tp.adb.oraclecloud.com";
        // Service-form SNI is never rustls-valid (numeric .V3.319 + underscores).
        let service_form = build_sni(service, None);
        assert!(
            !sni_is_rustls_valid(&service_form),
            "fixture must stay unencodable as rustls DNS: {service_form}"
        );
        assert_eq!(
            decide_sni(true, host, service, None).expect("PE ADB host SNI"),
            Some(host.to_string())
        );
    }

    #[test]
    fn decide_sni_sovereign_cloud_adb_uses_host_fallback() {
        let host = "adb.uk-london-1.oraclegovcloud.com";
        let service = "govdb_high.adb.uk-london-1.oraclegovcloud.com";
        assert_eq!(
            decide_sni(true, host, service, None).expect("gov ADB host SNI"),
            Some(host.to_string())
        );
    }

    fn fixture_tls_dir() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("tls")
    }

    /// Assert the production root loader honors the two OpenSSL-compatible
    /// overrides. Run the environment-mutating half in a child test process:
    /// Cargo's unit tests are parallel, while process environment is global.
    fn assert_system_root_loader_honors_explicit_override(mode: &str, test_name: &str) {
        const CHILD_MODE: &str = "ORACLEDB_TEST_SYSTEM_ROOT_LOADER_MODE";

        if let Some(child_mode) = std::env::var_os(CHILD_MODE) {
            let (configured_path, expected_cert_file) = match child_mode.to_string_lossy().as_ref()
            {
                "file" => {
                    let cert = fixture_tls_dir().join("ca_wallet.pem");
                    (cert.clone(), cert)
                }
                "dir" => {
                    let dir = fixture_tls_dir().join("invalid_client_key");
                    (dir.clone(), dir.join("ewallet.pem"))
                }
                other => panic!("unexpected child root-loader mode {other:?}"),
            };
            let expected = std::fs::File::open(&expected_cert_file)
                .map(std::io::BufReader::new)
                .map(|mut reader| rustls_pemfile_certs(&mut reader))
                .expect("read fixture certificate");
            assert!(
                !expected.is_empty(),
                "fixture must provide an explicit trust anchor"
            );

            let actual = load_system_roots();
            assert_eq!(
                actual,
                expected,
                "load_system_roots must use the {child_mode:?} SSL_CERT_* override at {}",
                configured_path.display()
            );
            return;
        }

        let mut child = std::process::Command::new(std::env::current_exe().expect("test binary"));
        child
            .arg("--exact")
            .arg(format!("tls::tests::{test_name}"))
            .arg("--nocapture")
            .env(CHILD_MODE, mode)
            .env_remove("SSL_CERT_FILE")
            .env_remove("SSL_CERT_DIR");
        match mode {
            "file" => {
                child.env("SSL_CERT_FILE", fixture_tls_dir().join("ca_wallet.pem"));
            }
            "dir" => {
                child.env("SSL_CERT_DIR", fixture_tls_dir().join("invalid_client_key"));
            }
            other => panic!("unexpected root-loader mode {other:?}"),
        }
        let status = child.status().expect("run isolated root-loader test");
        assert!(status.success(), "isolated {mode} root-loader test failed");
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

    #[test]
    fn resolve_wallet_reports_pem_first_no_fallthrough() {
        // With ewallet.pem present it wins the precedence outright.
        let dir = temp_wallet_dir(
            "resolve-pem-first",
            &["ewallet.pem", "ewallet_orapki.p12", "cwallet_orapki.sso"],
        );
        let res = resolve_wallet(&dir, None).expect("pem must resolve");
        assert_eq!(res.chosen, WalletFile::Pem);
        assert_eq!(res.attempted_primary, Some(WalletFile::Pem));
        assert!(!res.fell_through);
        assert!(!res.fallthrough_eligible);
        assert_eq!(res.chosen.file_name(), "ewallet.pem");
    }

    #[test]
    fn resolve_wallet_adb_dir_reports_p12_with_password_sso_without() {
        // ADB-style dir (p12 + sso, no pem): password -> p12, none -> sso.
        let dir = temp_wallet_dir("resolve-adb", &["ewallet_orapki.p12", "cwallet_orapki.sso"]);
        let with_pw =
            resolve_wallet(&dir, Some("WalletPass123")).expect("p12 resolves with password");
        assert_eq!(with_pw.chosen, WalletFile::P12);
        assert_eq!(with_pw.attempted_primary, Some(WalletFile::P12));
        assert!(!with_pw.fell_through);
        assert!(!with_pw.fallthrough_eligible);

        let without_pw = resolve_wallet(&dir, None).expect("sso resolves without password");
        assert_eq!(without_pw.chosen, WalletFile::Sso);
        assert_eq!(without_pw.attempted_primary, None);
        assert!(!without_pw.fell_through);
        assert!(!without_pw.fallthrough_eligible);
        assert_eq!(without_pw.chosen.file_name(), "cwallet.sso");
    }

    #[test]
    fn resolve_wallet_unusable_p12_reports_fallthrough_to_sso() {
        // A wrong p12 password with a valid cwallet.sso present -> the outcome
        // records the fallthrough: chosen == Sso, primary attempted was P12,
        // and the failure was fallthrough-eligible.
        let dir = temp_wallet_dir(
            "resolve-fallthrough",
            &["ewallet_orapki.p12", "cwallet_orapki.sso"],
        );
        let res = resolve_wallet(&dir, Some("not-the-password!"))
            .expect("wrong p12 password falls through to cwallet.sso");
        assert_eq!(res.chosen, WalletFile::Sso);
        assert_eq!(res.attempted_primary, Some(WalletFile::P12));
        assert!(res.fell_through);
        assert!(res.fallthrough_eligible);
    }

    #[test]
    fn resolve_wallet_does_not_drift_from_load_wallet() {
        // The public accessor and the private loader go through the one
        // resolver: when resolve_wallet says a wallet file won, load_wallet
        // yields the identity from that same resolution.
        let dir = temp_wallet_dir(
            "resolve-parity",
            &["ewallet_orapki.p12", "cwallet_orapki.sso"],
        );
        let res = resolve_wallet(&dir, Some("WalletPass123")).expect("resolve with password");
        assert_eq!(res.chosen, WalletFile::P12);
        let loaded = load_wallet(&dir, Some("WalletPass123")).expect("load with password");
        assert!(loaded.has_client_identity());
    }

    #[test]
    fn resolve_wallet_empty_dir_is_typed_wallet_error() {
        // No wallet file at all: the same typed Wallet error load_wallet
        // surfaces (never a panic, never a spurious success).
        let dir = temp_wallet_dir("resolve-empty", &[]);
        let err = resolve_wallet(&dir, None).expect_err("empty wallet dir must error");
        assert!(
            matches!(err, Error::Wallet(_)),
            "expected wallet error, got {err:?}"
        );
    }
}
