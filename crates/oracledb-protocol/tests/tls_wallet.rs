//! Integration tests for the sans-I/O TLS wallet readers, SNI builder and DN
//! matcher, exercised against REAL fixtures (see
//! `crates/oracledb/tests/fixtures/tls/` and `docs/TLS_SETUP.md`): openssl-3
//! generated PEM/PKCS#12 material plus a genuine `orapki` 23c wallet
//! (`ewallet_orapki.p12` + `cwallet_orapki.sso`, synthetic self-signed
//! lab-only content).
//!
//! These prove the actual parsing/crypto paths — not mocks.

use std::path::PathBuf;

use oracledb_protocol::tls::dn::{check_cert_dn, check_server_name, name_matches};
use oracledb_protocol::tls::sni::build_sni;
use oracledb_protocol::tls::wallet::{
    parse_ewallet_p12, parse_ewallet_pem, resolve_wallet_dir, WalletError,
};

/// Wallet password used when generating the encrypted fixtures (see
/// `docs/TLS_SETUP.md` §5). Lab-only synthetic material.
const FIXTURE_WALLET_PASSWORD: &str = "WalletPassword16";
/// Wallet password of the orapki-generated wallet fixtures.
const ORAPKI_WALLET_PASSWORD: &str = "WalletPass123";

fn fixture_dir() -> PathBuf {
    // crates/oracledb-protocol/tests -> crates/oracledb/tests/fixtures/tls
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop(); // crates/
    p.push("oracledb");
    p.push("tests");
    p.push("fixtures");
    p.push("tls");
    p
}

fn read_fixture(name: &str) -> Vec<u8> {
    let path = fixture_dir().join(name);
    std::fs::read(&path).expect("read TLS wallet fixture")
}

#[test]
fn ewallet_pem_parses_real_cert_and_key() {
    // ewallet.pem = self-signed server cert + private key (mTLS-capable wallet).
    let pem = read_fixture("ewallet.pem");
    let wallet = parse_ewallet_pem(&pem, None).expect("ewallet.pem should parse");
    assert!(
        !wallet.ca_certificates.is_empty(),
        "wallet must expose at least one CA/trust-anchor cert"
    );
    assert!(
        wallet.has_client_identity(),
        "ewallet.pem with a private key must yield a client identity"
    );
    // The first CA cert DER must be a valid X.509 (parse with x509-cert).
    use x509_cert::der::Decode;
    let cert = x509_cert::Certificate::from_der(&wallet.ca_certificates[0])
        .expect("parsed cert DER must be valid X.509");
    let subject = cert.tbs_certificate.subject.to_string();
    assert!(
        subject.contains("db.example.com"),
        "subject was {subject:?}"
    );
}

#[test]
fn ca_only_wallet_is_verify_only() {
    // ca_wallet.pem holds only the CA cert, no key => verify-only.
    let pem = read_fixture("ca_wallet.pem");
    let wallet = parse_ewallet_pem(&pem, None).expect("ca_wallet.pem should parse");
    assert_eq!(wallet.ca_certificates.len(), 1);
    assert!(
        !wallet.has_client_identity(),
        "a CA-only wallet must NOT present a client identity"
    );
}

/// Assert an error's Display and Debug never leak the wallet password or the
/// fixture path.
fn assert_redacted(err: &WalletError, password: &str) {
    let display = format!("{err}");
    let debug = format!("{err:?}");
    assert!(!display.contains(password), "password leaked: {display}");
    assert!(!debug.contains(password), "password leaked: {debug}");
    let sensitive_path = fixture_dir().display().to_string();
    assert!(!display.contains(&sensitive_path), "path leaked: {display}");
    assert!(!debug.contains(&sensitive_path), "path leaked: {debug}");
}

/// Assert decrypted wallet contents carry a usable mTLS identity whose key is
/// valid PKCS#8 (or PKCS#1) DER.
fn assert_client_identity(wallet: &oracledb_protocol::tls::wallet::WalletContents) {
    assert!(!wallet.ca_certificates.is_empty(), "trust anchors expected");
    assert!(wallet.has_client_identity(), "client identity expected");
    use x509_cert::der::Decode;
    let key_der = wallet.client_private_key.as_ref().expect("key present");
    let pkcs8_ok = pkcs8::PrivateKeyInfo::from_der(key_der).is_ok();
    // PKCS#1 keys (BEGIN RSA PRIVATE KEY) are also accepted by rustls.
    assert!(
        pkcs8_ok || key_der.first() == Some(&0x30),
        "decrypted key must be DER"
    );
    let _cert = x509_cert::Certificate::from_der(&wallet.client_cert_chain[0])
        .expect("client chain leaf must be X.509");
}

#[test]
fn encrypted_ewallet_pem_decrypts_with_password_sha256_prf() {
    // ADB-style ewallet.pem: certs + PKCS#8 ENCRYPTED PRIVATE KEY
    // (PBES2 / PBKDF2-HMAC-SHA256 / AES-256-CBC).
    let pem = read_fixture("ewallet_encrypted.pem");
    let wallet = parse_ewallet_pem(&pem, Some(FIXTURE_WALLET_PASSWORD))
        .expect("encrypted ewallet.pem must decrypt with the wallet password");
    assert_client_identity(&wallet);
    assert_eq!(wallet.ca_certificates.len(), 2, "leaf + CA certs");
}

#[test]
fn encrypted_ewallet_pem_decrypts_with_password_sha1_prf() {
    // PBES2 / PBKDF2-HMAC-SHA1 / AES-128-CBC variant.
    let pem = read_fixture("ewallet_encrypted_sha1.pem");
    let wallet = parse_ewallet_pem(&pem, Some(FIXTURE_WALLET_PASSWORD))
        .expect("SHA1-PRF encrypted ewallet.pem must decrypt");
    assert_client_identity(&wallet);
}

#[test]
fn encrypted_ewallet_pem_without_password_is_password_required() {
    let pem = read_fixture("ewallet_encrypted.pem");
    let err = parse_ewallet_pem(&pem, None)
        .expect_err("encrypted key without a password must fail closed");
    assert!(
        matches!(err, WalletError::PasswordRequired { format } if format == "ewallet.pem"),
        "expected PasswordRequired, got {err:?}"
    );
    assert_redacted(&err, FIXTURE_WALLET_PASSWORD);
}

#[test]
fn encrypted_ewallet_pem_wrong_password_is_typed_key_decrypt() {
    let pem = read_fixture("ewallet_encrypted.pem");
    let err = parse_ewallet_pem(&pem, Some("not-the-password!"))
        .expect_err("wrong password must fail, never degrade to verify-only");
    assert!(
        matches!(&err, WalletError::KeyDecrypt(_)),
        "expected KeyDecrypt, got {err:?}"
    );
    assert!(format!("{err}").contains("wallet_password"));
    assert_redacted(&err, "not-the-password!");
}

#[test]
fn encrypted_ewallet_pem_scrypt_kdf_is_typed_unsupported() {
    // openssl pkcs8 -scrypt: an unsupported KDF must produce a typed error
    // naming the unsupported OID — not a panic, not a silent verify-only wallet.
    let pem = read_fixture("ewallet_encrypted_scrypt.pem");
    let err = parse_ewallet_pem(&pem, Some(FIXTURE_WALLET_PASSWORD))
        .expect_err("scrypt KDF is not supported and must fail closed");
    assert!(
        matches!(&err, WalletError::KeyDecrypt(_)),
        "expected KeyDecrypt, got {err:?}"
    );
    assert_redacted(&err, FIXTURE_WALLET_PASSWORD);
}

#[test]
fn legacy_openssl_encrypted_pem_is_typed_unsupported() {
    // Proc-Type: 4,ENCRYPTED (PEM-level encryption) — typed remediation
    // pointing at `openssl pkcs8 -topk8`, even when a password is supplied.
    let pem = read_fixture("ewallet_encrypted_legacy.pem");
    let err = parse_ewallet_pem(&pem, Some(FIXTURE_WALLET_PASSWORD))
        .expect_err("legacy PEM encryption must fail closed");
    let message = if let WalletError::KeyDecrypt(message) = &err {
        message.clone()
    } else {
        panic!("expected KeyDecrypt, got {err:?}");
    };
    assert!(message.contains("pkcs8"), "remediation must name pkcs8");
    assert_redacted(&err, FIXTURE_WALLET_PASSWORD);
}

#[test]
fn orapki_ewallet_p12_parses_with_password() {
    // A REAL `orapki wallet create` + `wallet add -self_signed` ewallet.p12
    // (oraclepki 23.26): whole-safe encryptedData, PBES2 / PBKDF2-HMAC-SHA256 /
    // AES-256-CBC.
    let p12 = read_fixture("ewallet_orapki.p12");
    let wallet = parse_ewallet_p12(&p12, Some(ORAPKI_WALLET_PASSWORD))
        .expect("real orapki ewallet.p12 must parse");
    assert_client_identity(&wallet);
    use x509_cert::der::Decode;
    let cert = x509_cert::Certificate::from_der(&wallet.ca_certificates[0])
        .expect("orapki cert must be X.509");
    let subject = cert.tbs_certificate.subject.to_string();
    assert!(
        subject.contains("db.example.com"),
        "subject was {subject:?}"
    );
}

#[test]
fn openssl_default_ewallet_p12_parses_with_password() {
    // `openssl pkcs12 -export` (OpenSSL 3 defaults): PKCS8ShroudedKeyBag +
    // encryptedData cert safe, PBES2/AES-256-CBC.
    let p12 = read_fixture("ewallet_openssl.p12");
    let wallet = parse_ewallet_p12(&p12, Some(FIXTURE_WALLET_PASSWORD))
        .expect("openssl-default ewallet.p12 must parse");
    assert_client_identity(&wallet);
}

#[test]
fn ewallet_p12_without_password_is_password_required() {
    let p12 = read_fixture("ewallet_orapki.p12");
    let err = parse_ewallet_p12(&p12, None).expect_err("p12 without password must fail closed");
    assert!(
        matches!(err, WalletError::PasswordRequired { format } if format == "ewallet.p12"),
        "expected PasswordRequired, got {err:?}"
    );
    assert_redacted(&err, ORAPKI_WALLET_PASSWORD);
}

#[test]
fn ewallet_p12_wrong_password_is_typed_pkcs12_error() {
    let p12 = read_fixture("ewallet_orapki.p12");
    let err = parse_ewallet_p12(&p12, Some("not-the-password!"))
        .expect_err("wrong p12 password must fail");
    assert!(
        matches!(&err, WalletError::Pkcs12(_)),
        "expected Pkcs12, got {err:?}"
    );
    assert_redacted(&err, "not-the-password!");
}

#[test]
fn sni_string_matches_reference_format() {
    // python-oracledb _calc_sni_data: S{len}.{service}.V3.{version}
    assert_eq!(build_sni("FREEPDB1", None), "S8.FREEPDB1.V3.319");
    assert_eq!(build_sni("svc", Some("dedicated")), "S3.svc.T1.d.V3.319");
}

#[test]
fn dn_match_accepts_matching_subject_dn() {
    // The real leaf subject is "C=US, O=ExampleDB, CN=db.example.com".
    assert!(check_cert_dn(
        "CN=db.example.com,O=ExampleDB,C=US",
        "C=US, O=ExampleDB, CN=db.example.com"
    )
    .is_ok());
}

#[test]
fn dn_match_rejects_wrong_subject_dn() {
    assert!(check_cert_dn(
        "CN=evil.example.com,O=ExampleDB,C=US",
        "C=US, O=ExampleDB, CN=db.example.com"
    )
    .is_err());
}

#[test]
fn name_match_accepts_san_and_wildcard() {
    assert!(check_server_name("db.example.com", &["db.example.com".into()], &[]).is_ok());
    assert!(name_matches("anything.example.com", "*.example.com"));
    assert!(check_server_name("db.example.com", &["evil.example.com".into()], &[]).is_err());
}

#[test]
fn resolve_wallet_dir_precedence() {
    assert_eq!(
        resolve_wallet_dir(Some("/w"), Some("/t")),
        Some("/w".into())
    );
    assert_eq!(resolve_wallet_dir(Some("SYSTEM"), Some("/t")), None);
    assert_eq!(resolve_wallet_dir(None, Some("/t")), Some("/t".into()));
}

#[test]
fn cwallet_sso_orapki_real_wallet_parses() {
    // The genuine `orapki wallet create -auto_login` cwallet.sso (oraclepki
    // 23.26) that pairs with ewallet_orapki.p12: outer container magic
    // A1F84E / version '6' / sub-type 6 (AES-128-CBC auto-login password),
    // inner PKCS#12 PBES2/AES-256. Its contents must match the p12's.
    let sso = read_fixture("cwallet_orapki.sso");
    let wallet = oracledb_protocol::tls::sso::parse_cwallet_sso(&sso)
        .expect("real orapki cwallet.sso must parse end to end");
    assert_client_identity(&wallet);
    let p12 = read_fixture("ewallet_orapki.p12");
    let from_p12 = parse_ewallet_p12(&p12, Some(ORAPKI_WALLET_PASSWORD))
        .expect("paired ewallet.p12 must parse");
    assert_eq!(
        wallet.ca_certificates, from_p12.ca_certificates,
        "sso and p12 must yield identical certificates"
    );
    assert_eq!(
        wallet.client_private_key, from_p12.client_private_key,
        "sso and p12 must yield the identical private key"
    );
}

#[test]
fn cwallet_sso_parses_real_oracle_format_wallet() {
    // A genuine cwallet.sso (Oracle SSO outer container wrapping a real
    // PBES2/PBKDF2/AES-256-CBC PKCS#12), generated by docs/TLS_SETUP.md steps.
    let sso = read_fixture("cwallet.sso");
    let wallet = oracledb_protocol::tls::sso::parse_cwallet_sso(&sso)
        .expect("real cwallet.sso should parse end to end");
    assert!(
        !wallet.ca_certificates.is_empty() || !wallet.client_cert_chain.is_empty(),
        "SSO wallet must yield at least one certificate"
    );
    // Validate the extracted cert DER is real X.509.
    use x509_cert::der::Decode;
    let der = wallet
        .ca_certificates
        .first()
        .or_else(|| wallet.client_cert_chain.first())
        .expect("at least one cert");
    let _cert = x509_cert::Certificate::from_der(der).expect("extracted SSO cert must be X.509");
    // The unencrypted keyBag private key must have been extracted.
    assert!(
        wallet.client_private_key.is_some(),
        "SSO wallet with a keyBag must yield the private key"
    );
}

#[test]
fn cwallet_sso_parses_shrouded_key_wallet() {
    // A cwallet.sso whose inner PKCS#12 stores the private key in a PBES2/AES
    // *shrouded* key bag (the orapki / openssl default), proving the
    // shrouded-key decryption path.
    let sso = read_fixture("cwallet_shrouded.sso");
    let wallet = oracledb_protocol::tls::sso::parse_cwallet_sso(&sso)
        .expect("shrouded-key cwallet.sso should parse");
    assert!(
        wallet.client_private_key.is_some(),
        "shrouded key bag must be decrypted and extracted"
    );
    assert!(
        !wallet.ca_certificates.is_empty(),
        "shrouded-key SSO wallet must still yield certs"
    );
    // The decrypted key must be a valid PKCS#8 PrivateKeyInfo.
    use x509_cert::der::Decode;
    let key_der = wallet
        .client_private_key
        .as_ref()
        .expect("client private key present");
    pkcs8::PrivateKeyInfo::from_der(key_der).expect("decrypted key must be valid PKCS#8");
}
