//! Oracle wallet readers and wallet-location resolution.
//!
//! Three wallet shapes are supported:
//!
//! * **`ewallet.pem`** — a single PEM file holding the trust-anchor
//!   certificate(s) and, for mTLS, the client certificate chain plus the
//!   client private key (optionally encrypted with a wallet password). This is
//!   the format python-oracledb thin loads
//!   (`transport.pyx::create_ssl_context`: `load_verify_locations(ewallet.pem)`
//!   then a best-effort `load_cert_chain(ewallet.pem, password=...)`).
//!   Encrypted `ENCRYPTED PRIVATE KEY` (PKCS#8 PBES2) blocks are decrypted
//!   when a wallet password is supplied.
//!
//! * **`ewallet.p12`** — the standard PKCS#12 wallet (the file `orapki wallet
//!   create` produces and Autonomous Database wallet zips ship). Requires the
//!   wallet password. Modern PBES2/PBKDF2/AES-CBC wallets are supported;
//!   legacy 3DES/RC2 wallets return a typed error.
//!
//! * **`cwallet.sso`** — the SSO auto-login wallet (proprietary Oracle
//!   container wrapping a PKCS#12); see [`super::sso`].
//!
//! All parsed certificates and keys are returned as DER bytes so the I/O crate
//! can hand them to rustls without this (sans-I/O) crate depending on the async
//! TLS stack.

use std::io::BufRead;
use std::path::{Path, PathBuf};

/// File name of the PEM wallet (python-oracledb `PEM_WALLET_FILE_NAME`).
pub const PEM_WALLET_FILE_NAME: &str = "ewallet.pem";
/// File name of the standalone PKCS#12 wallet.
pub const P12_WALLET_FILE_NAME: &str = "ewallet.p12";
/// File name of the SSO auto-login wallet.
pub const SSO_WALLET_FILE_NAME: &str = "cwallet.sso";

/// Errors raised while resolving or reading a wallet.
#[derive(thiserror::Error)]
#[non_exhaustive]
pub enum WalletError {
    /// The wallet directory did not contain the expected file.
    #[error("wallet file is missing")]
    FileMissing(String),
    /// An I/O error occurred reading the wallet.
    #[error("failed to read wallet file: {source}")]
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
    /// SSO (cwallet.sso) outer-container parsing failure.
    #[error("cwallet.sso parse error: {0}")]
    Sso(String),
    /// Historical: SSO support compiled out. No longer returned as of 0.7.x
    /// (the `cwallet.sso` reader is always available); the variant is kept so
    /// existing `match` arms keep compiling.
    #[error(
        "cwallet.sso support is not enabled in this build; convert the wallet \
         to ewallet.pem"
    )]
    SsoNotEnabled,
    /// PKCS#12 (`ewallet.p12`, or the PKCS#12 embedded in `cwallet.sso`)
    /// parsing or decryption failure. The message names OIDs/structures only —
    /// never paths or passwords.
    #[error("PKCS#12 wallet parse error: {0}")]
    Pkcs12(String),
    /// An encrypted private key could not be decrypted (wrong wallet password,
    /// or an unsupported encryption scheme — only PKCS#8 PBES2 with
    /// PBKDF2-HMAC-SHA1/SHA256 + AES-CBC is supported).
    #[error("wallet private key decryption failed: {0}")]
    KeyDecrypt(String),
    /// The wallet (or its private key) is encrypted and requires a wallet
    /// password, but none was supplied. Machine-classifiable remediation:
    /// supply `wallet_password`, or use an auto-login `cwallet.sso` /
    /// unencrypted `ewallet.pem` wallet.
    #[error(
        "wallet {format} is encrypted and requires a wallet password; supply \
         wallet_password (or use an auto-login cwallet.sso or unencrypted \
         ewallet.pem wallet)"
    )]
    PasswordRequired { format: &'static str },
    /// A recognized wallet file is present but this thin build does not support
    /// the format.
    #[error("wallet format {format} is not supported by this thin build")]
    UnsupportedFormat { format: &'static str },
}

impl std::fmt::Debug for WalletError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const REDACTED_PATH: &str = "***redacted***";
        let redacted = |_: &String| REDACTED_PATH;
        match self {
            Self::FileMissing(path) => f.debug_tuple("FileMissing").field(&redacted(path)).finish(),
            Self::Io { path, source } => f
                .debug_struct("Io")
                .field("path", &redacted(path))
                .field("source", source)
                .finish(),
            Self::Pem(message) => f.debug_tuple("Pem").field(message).finish(),
            Self::NoCertificates => f.write_str("NoCertificates"),
            Self::Sso(message) => f.debug_tuple("Sso").field(message).finish(),
            Self::SsoNotEnabled => f.write_str("SsoNotEnabled"),
            Self::Pkcs12(message) => f.debug_tuple("Pkcs12").field(message).finish(),
            Self::KeyDecrypt(message) => f.debug_tuple("KeyDecrypt").field(message).finish(),
            Self::PasswordRequired { format } => f
                .debug_struct("PasswordRequired")
                .field("format", format)
                .finish(),
            Self::UnsupportedFormat { format } => f
                .debug_struct("UnsupportedFormat")
                .field("format", format)
                .finish(),
        }
    }
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

/// Returns the path to `ewallet.p12` inside a wallet directory.
#[must_use]
pub fn p12_wallet_path(dir: &Path) -> PathBuf {
    dir.join(P12_WALLET_FILE_NAME)
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
/// When the private key is an `ENCRYPTED PRIVATE KEY` (PKCS#8
/// `EncryptedPrivateKeyInfo`) block — the shape Autonomous Database wallet
/// downloads produce — it is decrypted with `wallet_password` (PBES2 /
/// PBKDF2-HMAC-SHA1/SHA256 / AES-CBC, the scheme `openssl pkcs8 -topk8` and
/// Oracle wallet exports emit). A missing password yields
/// [`WalletError::PasswordRequired`]; a wrong password or unsupported scheme
/// yields [`WalletError::KeyDecrypt`]. Legacy OpenSSL PEM-level encryption
/// (`Proc-Type: 4,ENCRYPTED`) is rejected with a typed remediation.
///
/// # Errors
/// Returns [`WalletError::Pem`] on malformed PEM,
/// [`WalletError::NoCertificates`] if no certificate blocks are found, and the
/// encrypted-key errors described above.
pub fn parse_ewallet_pem(
    pem: &[u8],
    wallet_password: Option<&str>,
) -> Result<WalletContents, WalletError> {
    // Legacy OpenSSL PEM-level encryption scrambles the base64 payload of a
    // PKCS#1 block; rustls-pemfile would surface it as a garbage key. Reject it
    // up front with a typed remediation (fail closed).
    if pem_contains_legacy_encryption(pem) {
        return Err(WalletError::KeyDecrypt(
            "legacy OpenSSL PEM encryption (Proc-Type: 4,ENCRYPTED) is not \
             supported; re-export the key as PKCS#8 with \
             `openssl pkcs8 -topk8` (optionally encrypted, then supply \
             wallet_password)"
                .to_string(),
        ));
    }

    let mut reader = std::io::BufReader::new(pem);
    let mut contents = WalletContents::default();
    let mut all_certs: Vec<Vec<u8>> = Vec::new();
    let mut keys: Vec<Vec<u8>> = Vec::new();

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
                // ENCRYPTED PRIVATE KEY blocks are not handled by
                // rustls-pemfile; they are extracted and decrypted below.
                _ => {}
            },
            Ok(None) => break,
            Err(e) => return Err(WalletError::Pem(e.to_string())),
        }
    }

    if all_certs.is_empty() {
        return Err(WalletError::NoCertificates);
    }

    // Decrypt an ENCRYPTED PRIVATE KEY block when no plaintext key was found.
    if keys.is_empty() {
        let encrypted_blocks = extract_encrypted_key_pem_blocks(pem);
        if !encrypted_blocks.is_empty() {
            let Some(password) = wallet_password else {
                return Err(WalletError::PasswordRequired {
                    format: PEM_WALLET_FILE_NAME,
                });
            };
            // Oracle wallets carry a single client key; decrypt the first block
            // and surface its error directly (never silently degrade to a
            // verify-only wallet).
            let block = &encrypted_blocks[0];
            keys.push(decrypt_encrypted_pem_key(block, password)?);
        }
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
    }

    Ok(contents)
}

/// Parse a standalone `ewallet.p12` (PKCS#12) wallet into [`WalletContents`].
///
/// This is the wallet file `orapki wallet create` produces and Autonomous
/// Database wallet zips ship. Only the modern PBES2 / PBKDF2 / AES-CBC scheme
/// is supported (orapki 19c+, `openssl pkcs12 -export` defaults); legacy
/// 3DES/RC2 wallets return a typed [`WalletError::Pkcs12`] naming the
/// unsupported OID.
///
/// # Errors
/// Returns [`WalletError::PasswordRequired`] when `wallet_password` is `None`
/// (Oracle PKCS#12 wallets are always password-protected), and
/// [`WalletError::Pkcs12`] on parse/decrypt failure (including a wrong
/// password).
pub fn parse_ewallet_p12(
    data: &[u8],
    wallet_password: Option<&str>,
) -> Result<WalletContents, WalletError> {
    let Some(password) = wallet_password else {
        return Err(WalletError::PasswordRequired {
            format: P12_WALLET_FILE_NAME,
        });
    };
    super::pfx::parse_pfx(data, password.as_bytes())
}

/// Extract the raw text of every `ENCRYPTED PRIVATE KEY` PEM block.
fn extract_encrypted_key_pem_blocks(pem: &[u8]) -> Vec<String> {
    const BEGIN: &str = "-----BEGIN ENCRYPTED PRIVATE KEY-----";
    const END: &str = "-----END ENCRYPTED PRIVATE KEY-----";
    let text = String::from_utf8_lossy(pem);
    let mut blocks = Vec::new();
    let mut rest: &str = &text;
    while let Some(start) = rest.find(BEGIN) {
        let Some(end_rel) = rest[start..].find(END) else {
            break;
        };
        let stop = start + end_rel + END.len();
        blocks.push(rest[start..stop].to_string());
        rest = &rest[stop..];
    }
    blocks
}

/// Decode one `ENCRYPTED PRIVATE KEY` PEM block and decrypt it to plaintext
/// PKCS#8 `PrivateKeyInfo` DER.
fn decrypt_encrypted_pem_key(block: &str, password: &str) -> Result<Vec<u8>, WalletError> {
    let (label, doc) = der::Document::from_pem(block)
        .map_err(|e| WalletError::Pem(format!("ENCRYPTED PRIVATE KEY block: {e}")))?;
    if label != "ENCRYPTED PRIVATE KEY" {
        return Err(WalletError::Pem(format!(
            "expected ENCRYPTED PRIVATE KEY PEM label, got {label}"
        )));
    }
    super::pfx::decrypt_encrypted_private_key_info(doc.as_bytes(), password.as_bytes())
}

/// Heuristic: does this PEM buffer use legacy OpenSSL PEM-level encryption?
fn pem_contains_legacy_encryption(pem: &[u8]) -> bool {
    let mut reader = std::io::BufReader::new(pem);
    let mut line = String::new();
    while let Ok(n) = reader.read_line(&mut line) {
        if n == 0 {
            break;
        }
        if line.contains("Proc-Type: 4,ENCRYPTED") {
            return true;
        }
        line.clear();
    }
    false
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

/// Read and parse `ewallet.p12` from a wallet directory.
///
/// # Errors
/// Returns [`WalletError::FileMissing`] if the file does not exist,
/// [`WalletError::Io`] on a read error, and parse errors from
/// [`parse_ewallet_p12`].
pub fn read_ewallet_p12(
    dir: &Path,
    wallet_password: Option<&str>,
) -> Result<WalletContents, WalletError> {
    let path = p12_wallet_path(dir);
    if !path.exists() {
        return Err(WalletError::FileMissing(path.display().to_string()));
    }
    let bytes = std::fs::read(&path).map_err(|source| WalletError::Io {
        path: path.display().to_string(),
        source,
    })?;
    parse_ewallet_p12(&bytes, wallet_password)
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

    #[test]
    fn wallet_errors_redact_paths_in_display_and_debug() {
        let sensitive_path = "/private/wallet/ewallet.pem";
        let err = WalletError::FileMissing(sensitive_path.to_string());
        assert!(!format!("{err}").contains(sensitive_path));
        assert!(!format!("{err:?}").contains(sensitive_path));

        let err = WalletError::Io {
            path: sensitive_path.to_string(),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "missing"),
        };
        assert!(!format!("{err}").contains(sensitive_path));
        assert!(!format!("{err:?}").contains(sensitive_path));
    }
}
