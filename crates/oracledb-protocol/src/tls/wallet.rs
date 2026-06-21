//! Oracle wallet readers and wallet-location resolution.
//!
//! Two wallet shapes are supported:
//!
//! * **`ewallet.pem`** — a single PEM file holding the trust-anchor
//!   certificate(s) and, for mTLS, the client certificate chain plus the
//!   client private key (optionally encrypted with a wallet password). This is
//!   the format python-oracledb thin loads
//!   (`transport.pyx::create_ssl_context`: `load_verify_locations(ewallet.pem)`
//!   then a best-effort `load_cert_chain(ewallet.pem, password=...)`). Fully
//!   supported here.
//!
//! * **`cwallet.sso`** — the SSO auto-login wallet (proprietary Oracle
//!   container wrapping a PKCS#12). Parsing is gated behind the `experimental`
//!   feature; see [`super::sso`].
//!
//! All parsed certificates and keys are returned as DER bytes so the I/O crate
//! can hand them to rustls without this (sans-I/O) crate depending on the async
//! TLS stack.

use std::io::BufRead;
use std::path::{Path, PathBuf};

/// File name of the PEM wallet (python-oracledb `PEM_WALLET_FILE_NAME`).
pub const PEM_WALLET_FILE_NAME: &str = "ewallet.pem";
/// File name of the SSO auto-login wallet.
pub const SSO_WALLET_FILE_NAME: &str = "cwallet.sso";

/// Errors raised while resolving or reading a wallet.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WalletError {
    /// The wallet directory did not contain the expected file.
    #[error("wallet file is missing: {0}")]
    FileMissing(String),
    /// An I/O error occurred reading the wallet.
    #[error("failed to read wallet {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    /// The PEM content could not be parsed.
    #[error("failed to parse wallet PEM: {0}")]
    Pem(String),
    /// The wallet contained no usable trust-anchor certificates.
    #[error("wallet contained no certificates")]
    NoCertificates,
    /// SSO (cwallet.sso) parsing failure (experimental).
    #[error("cwallet.sso parse error: {0}")]
    Sso(String),
    /// SSO support is not compiled in.
    #[error(
        "cwallet.sso support is experimental and not enabled; rebuild with \
         --features experimental, or convert the wallet to ewallet.pem"
    )]
    SsoNotEnabled,
}

/// Parsed contents of an Oracle wallet, as DER bytes ready for rustls.
#[derive(Debug, Clone, Default)]
pub struct WalletContents {
    /// Trust-anchor / CA certificates used to verify the server (DER).
    pub ca_certificates: Vec<Vec<u8>>,
    /// Client certificate chain for mTLS, leaf first (DER). Empty if the
    /// wallet is verify-only.
    pub client_cert_chain: Vec<Vec<u8>>,
    /// Client private key for mTLS (DER, PKCS#8 or PKCS#1/SEC1). `None` if the
    /// wallet is verify-only.
    pub client_private_key: Option<Vec<u8>>,
}

impl WalletContents {
    /// Returns `true` if a client identity (cert chain + key) is present, i.e.
    /// the wallet can be used for mutual TLS.
    #[must_use]
    pub fn has_client_identity(&self) -> bool {
        !self.client_cert_chain.is_empty() && self.client_private_key.is_some()
    }
}

/// Resolve the wallet directory the way python-oracledb does.
///
/// Precedence (first non-`None`/non-`SYSTEM` wins):
/// 1. An explicit `wallet_location` (from the connect descriptor's
///    `MY_WALLET_DIRECTORY`/`wallet_location` param). The special value
///    `SYSTEM` (case-insensitive) is treated as "no wallet" — the system trust
///    store is used (reference: 23ai `SYSTEM` keyword).
/// 2. The `TNS_ADMIN` environment variable (python-oracledb `config_dir`).
///
/// Returns `None` when neither yields a directory (the caller should then fall
/// back to system roots).
#[must_use]
pub fn resolve_wallet_dir(
    wallet_location: Option<&str>,
    tns_admin: Option<&str>,
) -> Option<PathBuf> {
    if let Some(loc) = wallet_location {
        if !loc.is_empty() && !loc.eq_ignore_ascii_case("SYSTEM") {
            return Some(PathBuf::from(loc));
        }
        // Explicit SYSTEM => no wallet directory.
        if loc.eq_ignore_ascii_case("SYSTEM") {
            return None;
        }
    }
    tns_admin.filter(|s| !s.is_empty()).map(PathBuf::from)
}

/// Returns the path to `ewallet.pem` inside a wallet directory.
#[must_use]
pub fn pem_wallet_path(dir: &Path) -> PathBuf {
    dir.join(PEM_WALLET_FILE_NAME)
}

/// Returns the path to `cwallet.sso` inside a wallet directory.
#[must_use]
pub fn sso_wallet_path(dir: &Path) -> PathBuf {
    dir.join(SSO_WALLET_FILE_NAME)
}

/// Parse an `ewallet.pem` byte buffer into [`WalletContents`].
///
/// Mirrors python-oracledb: every certificate block is loaded as a trust
/// anchor (`load_verify_locations`), and additionally — if a private key and at
/// least one certificate are present — they form the client identity for mTLS
/// (`load_cert_chain`). A wallet without a private key is verify-only, which is
/// the common server-verification case.
///
/// The `wallet_password` is accepted for API symmetry with python-oracledb but
/// is only meaningful for encrypted private keys; rustls-pemfile handles
/// unencrypted PKCS#8/PKCS#1/SEC1 keys. Encrypted keys are reported via
/// [`WalletError::Pem`] so the caller can surface a clear message rather than
/// silently producing a verify-only wallet.
///
/// # Errors
/// Returns [`WalletError::Pem`] on malformed PEM and
/// [`WalletError::NoCertificates`] if no certificate blocks are found.
pub fn parse_ewallet_pem(
    pem: &[u8],
    _wallet_password: Option<&str>,
) -> Result<WalletContents, WalletError> {
    let mut reader = std::io::BufReader::new(pem);
    let mut contents = WalletContents::default();
    let mut all_certs: Vec<Vec<u8>> = Vec::new();
    let mut keys: Vec<Vec<u8>> = Vec::new();
    let mut saw_encrypted_key = false;

    loop {
        match rustls_pemfile::read_one(&mut reader) {
            Ok(Some(item)) => match item {
                rustls_pemfile::Item::X509Certificate(der) => {
                    all_certs.push(der.as_ref().to_vec());
                }
                rustls_pemfile::Item::Pkcs8Key(der) => {
                    keys.push(der.secret_pkcs8_der().to_vec());
                }
                rustls_pemfile::Item::Pkcs1Key(der) => {
                    keys.push(der.secret_pkcs1_der().to_vec());
                }
                rustls_pemfile::Item::Sec1Key(der) => {
                    keys.push(der.secret_sec1_der().to_vec());
                }
                // Encrypted private keys are not handled by rustls-pemfile;
                // they appear as Crl/Csr-less "other" items the iterator skips.
                // We detect the PEM marker separately below.
                _ => {}
            },
            Ok(None) => break,
            Err(e) => return Err(WalletError::Pem(e.to_string())),
        }
    }

    // rustls-pemfile silently skips ENCRYPTED PRIVATE KEY blocks; detect them so
    // we can tell the operator their key needs decrypting rather than pretend
    // the wallet is verify-only.
    if keys.is_empty() && pem_contains_encrypted_key(pem) {
        saw_encrypted_key = true;
    }

    if all_certs.is_empty() {
        return Err(WalletError::NoCertificates);
    }

    // Every certificate is a candidate trust anchor (python-oracledb loads the
    // whole PEM via load_verify_locations).
    contents.ca_certificates = all_certs.clone();

    // If a private key is present, treat the certs as the client chain for
    // mTLS as well (python-oracledb's best-effort load_cert_chain). The leaf is
    // the first cert in the file by Oracle wallet convention.
    if let Some(key) = keys.into_iter().next() {
        contents.client_cert_chain = all_certs;
        contents.client_private_key = Some(key);
    } else if saw_encrypted_key {
        return Err(WalletError::Pem(
            "wallet private key is encrypted; supply a wallet with an \
             unencrypted ewallet.pem (orapki ... -auto_login) or use cwallet.sso"
                .to_string(),
        ));
    }

    Ok(contents)
}

/// Parse all `CERTIFICATE` blocks from a PEM reader into DER byte vectors.
///
/// Exposed so the I/O crate can load OS root bundles (for the no-wallet TCPS
/// path) without taking its own `rustls-pemfile` dependency.
pub fn parse_pem_certificates(reader: &mut dyn BufRead) -> Vec<Vec<u8>> {
    rustls_pemfile::certs(reader)
        .filter_map(Result::ok)
        .map(|der| der.as_ref().to_vec())
        .collect()
}

/// Heuristic: does this PEM buffer contain an encrypted private-key block?
fn pem_contains_encrypted_key(pem: &[u8]) -> bool {
    let mut reader = std::io::BufReader::new(pem);
    let mut line = String::new();
    while let Ok(n) = reader.read_line(&mut line) {
        if n == 0 {
            break;
        }
        if line.contains("ENCRYPTED PRIVATE KEY") || line.contains("Proc-Type: 4,ENCRYPTED") {
            return true;
        }
        line.clear();
    }
    false
}

/// Read and parse `ewallet.pem` from a wallet directory.
///
/// # Errors
/// Returns [`WalletError::FileMissing`] if the file does not exist,
/// [`WalletError::Io`] on a read error, and parse errors from
/// [`parse_ewallet_pem`].
pub fn read_ewallet_pem(
    dir: &Path,
    wallet_password: Option<&str>,
) -> Result<WalletContents, WalletError> {
    let path = pem_wallet_path(dir);
    if !path.exists() {
        return Err(WalletError::FileMissing(path.display().to_string()));
    }
    let bytes = std::fs::read(&path).map_err(|source| WalletError::Io {
        path: path.display().to_string(),
        source,
    })?;
    parse_ewallet_pem(&bytes, wallet_password)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_prefers_explicit_location() {
        let dir = resolve_wallet_dir(Some("/wallets/db1"), Some("/etc/tns"));
        assert_eq!(dir, Some(PathBuf::from("/wallets/db1")));
    }

    #[test]
    fn resolve_system_means_no_wallet() {
        assert_eq!(resolve_wallet_dir(Some("SYSTEM"), Some("/etc/tns")), None);
        assert_eq!(resolve_wallet_dir(Some("system"), None), None);
    }

    #[test]
    fn resolve_falls_back_to_tns_admin() {
        assert_eq!(
            resolve_wallet_dir(None, Some("/etc/tns")),
            Some(PathBuf::from("/etc/tns"))
        );
    }

    #[test]
    fn resolve_none_when_nothing_set() {
        assert_eq!(resolve_wallet_dir(None, None), None);
        assert_eq!(resolve_wallet_dir(Some(""), None), None);
    }

    #[test]
    fn parse_rejects_empty_pem() {
        let err = parse_ewallet_pem(b"", None).unwrap_err();
        assert!(matches!(err, WalletError::NoCertificates));
    }
}
