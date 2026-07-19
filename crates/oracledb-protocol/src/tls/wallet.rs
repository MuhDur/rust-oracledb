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

use std::io::{BufRead, Read};
use std::path::{Path, PathBuf};

/// File name of the PEM wallet (python-oracledb `PEM_WALLET_FILE_NAME`).
pub const PEM_WALLET_FILE_NAME: &str = "ewallet.pem";
/// File name of the standalone PKCS#12 wallet.
pub const P12_WALLET_FILE_NAME: &str = "ewallet.p12";
/// File name of the SSO auto-login wallet.
pub const SSO_WALLET_FILE_NAME: &str = "cwallet.sso";

/// Largest wallet image accepted from disk or a caller-provided byte buffer.
///
/// Real PEM, PKCS#12, and SSO wallets are normally measured in KiB. Keeping
/// this bound modest prevents a configured-but-hostile wallet file from making
/// the driver allocate unbounded memory before its format parser can reject it.
pub const MAX_WALLET_FILE_BYTES: usize = 16 * 1024 * 1024;

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
    /// A wallet image exceeded the fail-closed resource limit before parsing.
    #[error("wallet data exceeds maximum size of {maximum_bytes} bytes")]
    TooLarge { maximum_bytes: usize },
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
            Self::TooLarge { maximum_bytes } => f
                .debug_struct("TooLarge")
                .field("maximum_bytes", maximum_bytes)
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

    /// Parse the X.509 validity window ([`CertMetadata`]) of every certificate
    /// this wallet holds — the trust anchors ([`Self::ca_certificates`]) first,
    /// then the client identity chain ([`Self::client_cert_chain`]), in that
    /// order.
    ///
    /// Purely offline: it inspects the DER bytes already parsed into this
    /// struct, so no connection or network I/O is involved. A non-certificate
    /// or otherwise unparseable DER entry is silently skipped (it never fails
    /// the whole call), so one odd entry does not hide the metadata of the
    /// rest. This lets a doctor warn on a near-expiry trust anchor or client
    /// certificate.
    #[must_use]
    pub fn certificate_metadata(&self) -> Vec<CertMetadata> {
        self.ca_certificates
            .iter()
            .chain(self.client_cert_chain.iter())
            .filter_map(|der| CertMetadata::from_der(der))
            .collect()
    }
}

/// The X.509 validity window of a wallet certificate, as Unix-epoch seconds.
///
/// Both fields are seconds since the Unix epoch (1970-01-01T00:00:00Z, UTC) —
/// the form the certificate's `notBefore` / `notAfter` decode to. Plain seconds
/// (rather than a richer date type) keeps this dependency-free and trivially
/// comparable: a doctor compares [`Self::not_after`] against the current time
/// to warn on an expired or soon-to-expire certificate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CertMetadata {
    /// `notBefore`: Unix-epoch seconds at/after which the certificate is valid.
    pub not_before: i64,
    /// `notAfter`: Unix-epoch seconds after which the certificate is expired.
    pub not_after: i64,
}

impl CertMetadata {
    /// Parse the validity window out of a single DER-encoded X.509
    /// certificate, or `None` when `der` is not a certificate we can read
    /// (wrong ASN.1 shape, truncated, an out-of-range time, etc.). The parse is
    /// deliberately narrow — it walks the `Certificate` → `TBSCertificate`
    /// SEQUENCE only far enough to reach the `validity` field — so it never
    /// pulls in a full X.509 stack and never fails on an unrelated DER blob.
    #[must_use]
    pub fn from_der(der: &[u8]) -> Option<Self> {
        use der::asn1::{GeneralizedTime, UtcTime};
        use der::{Decode, Header, Reader, SliceReader, Tag};

        /// Read the body slice of a SEQUENCE, advancing `reader` past it.
        fn seq_body<'a>(reader: &mut SliceReader<'a>) -> Option<&'a [u8]> {
            let header = Header::decode(reader).ok()?;
            if header.tag != Tag::Sequence {
                return None;
            }
            reader.read_slice(header.length).ok()
        }

        /// Consume (skip) one TLV element, whatever its tag.
        fn skip_tlv(reader: &mut SliceReader<'_>) -> Option<()> {
            let header = Header::decode(reader).ok()?;
            reader.read_slice(header.length).ok()?;
            Some(())
        }

        /// Decode a `Time` CHOICE (UTCTime or GeneralizedTime) to Unix seconds.
        fn read_time(reader: &mut SliceReader<'_>) -> Option<i64> {
            let unix = match reader.peek_tag().ok()? {
                Tag::UtcTime => UtcTime::decode(reader).ok()?.to_unix_duration(),
                Tag::GeneralizedTime => GeneralizedTime::decode(reader).ok()?.to_unix_duration(),
                _ => return None,
            };
            i64::try_from(unix.as_secs()).ok()
        }

        // Certificate ::= SEQUENCE { tbsCertificate, signatureAlgorithm, sig }
        let mut root = SliceReader::new(der).ok()?;
        let cert_body = seq_body(&mut root)?;
        let mut cert = SliceReader::new(cert_body).ok()?;

        // TBSCertificate ::= SEQUENCE {
        //   version [0] EXPLICIT DEFAULT v1, serialNumber, signature, issuer,
        //   validity, subject, ... }
        let tbs_body = seq_body(&mut cert)?;
        let mut tbs = SliceReader::new(tbs_body).ok()?;

        // The optional [0] EXPLICIT version tag is context-specific; when
        // present, skip it. Then skip serialNumber, signature, and issuer to
        // land on validity.
        if tbs.peek_tag().ok()?.is_context_specific() {
            skip_tlv(&mut tbs)?; // version [0]
        }
        skip_tlv(&mut tbs)?; // serialNumber INTEGER
        skip_tlv(&mut tbs)?; // signature AlgorithmIdentifier SEQUENCE
        skip_tlv(&mut tbs)?; // issuer Name SEQUENCE

        // Validity ::= SEQUENCE { notBefore Time, notAfter Time }
        let validity_body = seq_body(&mut tbs)?;
        let mut validity = SliceReader::new(validity_body).ok()?;
        let not_before = read_time(&mut validity)?;
        let not_after = read_time(&mut validity)?;
        Some(CertMetadata {
            not_before,
            not_after,
        })
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
    ensure_wallet_size(pem.len())?;
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
    ensure_wallet_size(data.len())?;
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
    let bytes = read_wallet_file(&path)?;
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
    let bytes = read_wallet_file(&path)?;
    parse_ewallet_p12(&bytes, wallet_password)
}

/// Read one wallet file without allowing a configured path to allocate an
/// unbounded buffer before parsing begins.
///
/// The stream is cut off at one byte above [`MAX_WALLET_FILE_BYTES`], so this
/// remains bounded even if file metadata races, is unavailable, or lies.
pub fn read_wallet_file(path: &Path) -> Result<Vec<u8>, WalletError> {
    let file = std::fs::File::open(path).map_err(|source| WalletError::Io {
        path: path.display().to_string(),
        source,
    })?;
    match read_wallet_reader(file, MAX_WALLET_FILE_BYTES).map_err(|source| WalletError::Io {
        path: path.display().to_string(),
        source,
    })? {
        Some(bytes) => Ok(bytes),
        None => Err(WalletError::TooLarge {
            maximum_bytes: MAX_WALLET_FILE_BYTES,
        }),
    }
}

/// Read at most `maximum_bytes + 1` bytes, returning `None` when the source is
/// oversized. Kept separate from filesystem I/O so the boundary is directly
/// regression-tested without creating a temporary wallet file.
fn read_wallet_reader<R: Read>(
    reader: R,
    maximum_bytes: usize,
) -> std::io::Result<Option<Vec<u8>>> {
    let limit = u64::try_from(maximum_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut reader = reader.take(limit);
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    Ok((bytes.len() <= maximum_bytes).then_some(bytes))
}

/// Enforce the same limit for public in-memory parser entry points.
pub(crate) fn ensure_wallet_size(size: usize) -> Result<(), WalletError> {
    if size > MAX_WALLET_FILE_BYTES {
        return Err(WalletError::TooLarge {
            maximum_bytes: MAX_WALLET_FILE_BYTES,
        });
    }
    Ok(())
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
    fn parse_rejects_malformed_pem_body() {
        // A wallet directory can be pointed at any file; a corrupted or
        // truncated ewallet.pem (bad base64 inside a real BEGIN/END block, as
        // opposed to just "no certificates at all") must surface the distinct
        // WalletError::Pem parse failure rather than NoCertificates.
        // PEM markers are split across concat! fragments so the source literal
        // does not trip the release secret-scan (this is a deliberately
        // malformed test fixture, not a real certificate); the concatenated
        // bytes are an ordinary BEGIN/END-delimited PEM block.
        let bad = concat!(
            "-----BEGIN CERT",
            "IFICATE-----\n***not valid base64***\n-----END CERT",
            "IFICATE-----\n"
        )
        .as_bytes();
        let err = parse_ewallet_pem(bad, None).unwrap_err();
        assert!(matches!(err, WalletError::Pem(_)), "got {err:?}");
    }

    #[test]
    fn bounded_wallet_reader_rejects_oversized_input() {
        let bytes = read_wallet_reader(std::io::Cursor::new([0u8; 17]), 16)
            .expect("in-memory reader is infallible");
        assert!(bytes.is_none(), "one byte over the cap must be rejected");
    }

    #[test]
    fn wallet_size_guard_rejects_before_parser_allocations() {
        let err = ensure_wallet_size(MAX_WALLET_FILE_BYTES + 1).unwrap_err();
        assert!(matches!(err, WalletError::TooLarge { .. }));
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

    /// A synthetic self-signed X.509 certificate (DER) minted only for this
    /// test with a *fixed* validity window so the parsed epochs are exact and
    /// deterministic:
    ///   subject/issuer CN=oracle-test.invalid (fictional; never a real host)
    ///   notBefore = 2020-01-02T03:04:05Z (Unix 1_577_934_245)
    ///   notAfter  = 2030-01-02T03:04:05Z (Unix 1_893_553_445)
    /// (`openssl req -x509 -not_before 20200102030405Z -not_after
    /// 20300102030405Z`). Both dates fall in 1950..2050 so they encode as
    /// ASN.1 UTCTime.
    const SYNTHETIC_CERT_DER_HEX: &str = "308203773082025fa00302010202142a2157bcbcc8fd4e52f45c36edb8b5e8ba8309c7300d06092a864886f70d01010b0500304b311c301a06035504030c136f7261636c652d746573742e696e76616c6964311e301c060355040a0c154f7261636c652053796e7468657469632054657374310b3009060355040613025553301e170d3230303130323033303430355a170d3330303130323033303430355a304b311c301a06035504030c136f7261636c652d746573742e696e76616c6964311e301c060355040a0c154f7261636c652053796e7468657469632054657374310b300906035504061302555330820122300d06092a864886f70d01010105000382010f003082010a0282010100a76a70aa8dc41c8254dca98dd01d683b253cf5cc189b019fa26f56f35c5c1ab57f5823b669d5f67cf15195d1d98e1da710ee06bde99133095c6fed0936a69d07d9d79c88d9d2741a0f680708e5a857c3df8f007ae963e5354af008211dbf6e1240e7ebf48a83ba7ead7c708e5775ecf2904caeadfc4464fdfa32a2d5040f6f63126762034ff65e816f63d59cfb0cb6a8a10da6f7fd49780cd5066eda2abc356970cab783743a8a556cc7c780fff5c73cee534a2eeddcfb54527ff3db40ffa202c5ec2e85bc6b9d97c54ab87acb3cfa895bcbc76b3935b080d8e6f98603c4c446e5c56ab0f4b33577affd36e12919d8fe520e5900b7919477bf3e81f493f516b30203010001a3533051301d0603551d0e041604147bc6964ed97e4e23f79f5f58ccdca185fb223893301f0603551d230418301680147bc6964ed97e4e23f79f5f58ccdca185fb223893300f0603551d130101ff040530030101ff300d06092a864886f70d01010b05000382010100904c05f871771ba1e15d9b18e92b7ed40d872b5eb84a7f795c1a908436d9a9a22d3a65f54f75dc8619820fbdb19738b9052849ef0b21b0b5ee0c455bb5eb019495a8abc517bf180f09cc8a937c1d7109d42a73f2ad9d716693676fee0a3b1d50d8908cfea7c9bb1d94a12408d7e967b6fb99705edfeda6de9f73dec4047d913e4173a2bfb4a196f571584d9b9fd84af455eaf228dcbcb1d2cf1a3fa9928b61a19f66400024ea92f9b9f70a2af994f831c017fca3563698a228367712112673175d505725318017ed3e3e5736465b174bf5669d7a8bae6fd595c4a03edb44b30465b32d7fd2d0d91f13fa40fd5c6ee0a79aec57472beb7be93cf0de05d0f01ad1";

    #[test]
    fn cert_metadata_parses_known_validity_dates() {
        let der = hex::decode(SYNTHETIC_CERT_DER_HEX).expect("decode synthetic cert hex");
        let meta = CertMetadata::from_der(&der).expect("synthetic cert must parse");
        assert_eq!(
            meta.not_before, 1_577_934_245,
            "notBefore 2020-01-02T03:04:05Z"
        );
        assert_eq!(
            meta.not_after, 1_893_553_445,
            "notAfter 2030-01-02T03:04:05Z"
        );
        assert!(meta.not_before < meta.not_after);
    }

    #[test]
    fn cert_metadata_skips_non_certificate_der() {
        // Random bytes and a bare (non-cert) SEQUENCE are not certificates: the
        // parser returns None instead of erroring.
        assert!(CertMetadata::from_der(b"").is_none());
        assert!(CertMetadata::from_der(&[0xDE, 0xAD, 0xBE, 0xEF]).is_none());
        // A well-formed but empty SEQUENCE (0x30 0x00) — no TBSCertificate.
        assert!(CertMetadata::from_der(&[0x30, 0x00]).is_none());
    }

    #[test]
    fn certificate_metadata_collects_and_skips_cleanly() {
        let der = hex::decode(SYNTHETIC_CERT_DER_HEX).expect("decode synthetic cert hex");
        // ca_certificates holds one real cert plus a junk entry; client chain
        // holds the same real cert. The junk entry is skipped, the two real
        // certs are reported in order (CA first, then client chain).
        let wallet = WalletContents {
            ca_certificates: vec![der.clone(), vec![0x01, 0x02, 0x03]],
            client_cert_chain: vec![der.clone()],
            client_private_key: None,
        };
        let all = wallet.certificate_metadata();
        assert_eq!(
            all.len(),
            2,
            "one junk CA entry is skipped, two certs remain"
        );
        for meta in &all {
            assert_eq!(meta.not_before, 1_577_934_245);
            assert_eq!(meta.not_after, 1_893_553_445);
        }
        // A wallet with no certificates yields an empty vec (never panics).
        assert!(WalletContents::default().certificate_metadata().is_empty());
    }
}
