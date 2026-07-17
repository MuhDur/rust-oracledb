#![forbid(unsafe_code)]
//! OCI IAM database-token **proof-of-possession** (PoP) signing.
//!
//! An OCI IAM database token (`oci iam db-token get`) is bound to an RSA key
//! pair: the token embeds the public key (its `jwk` claim) and the caller holds
//! the matching PKCS#8 private key (`oci_db_key.pem`). Authenticating with the
//! token is therefore a proof of possession — presenting the bearer token alone
//! is refused by the database with `ORA-01017`. The client must sign an
//! HTTP-Signatures header with the private key and send it alongside the token
//! as `AUTH_HEADER` / `AUTH_SIGNATURE`; the server verifies the signature
//! against the public key embedded in the token.
//!
//! This mirrors python-oracledb thin mode exactly
//! (`impl/thin/messages/auth.pyx` `_write_message` + `impl/thin/crypto.pyx`
//! `get_signature`): the signed string is
//! `date: <IMF-fixdate>\n(request-target): <service_name>\nhost: <host>:<port>`,
//! signed with RSA-PKCS1v15 + SHA-256 and base64-encoded. The signing and key
//! parsing use `ring` (already the crypto backend behind rustls) so no new
//! crypto engine — and no `unsafe` in our crates — is introduced.

use std::time::SystemTime;

use base64::Engine as _;
use ring::rand::SystemRandom;
use ring::signature::{RsaKeyPair, RSA_PKCS1_SHA256};

use crate::{Error, Result};

/// Builds the HTTP-Signatures signing string for the token proof-of-possession,
/// byte-for-byte as the reference constructs it before signing. `now` is the
/// current wall-clock time (the `date:` field is validated by the server for
/// freshness, so it must be current UTC).
pub(crate) fn build_signing_header(
    service_name: &str,
    host: &str,
    port: u16,
    now: SystemTime,
) -> String {
    // `httpdate::fmt_http_date` emits the IMF-fixdate form
    // `Fri, 17 Jul 2026 07:00:00 GMT`, identical to the reference's
    // `datetime.strftime("%a, %d %b %Y %H:%M:%S GMT")` on a UTC datetime.
    let date = httpdate::fmt_http_date(now);
    format!("date: {date}\n(request-target): {service_name}\nhost: {host}:{port}")
}

/// Signs `header` with the token's bound PKCS#8 RSA private key using
/// RSA-PKCS1v15 + SHA-256 and returns the base64-encoded signature. The key
/// material, the token, and the header never appear in the returned error.
pub(crate) fn sign_signing_header(private_key_pkcs8_pem: &str, header: &str) -> Result<String> {
    let der = pkcs8_pem_to_der(private_key_pkcs8_pem)?;
    let key_pair = RsaKeyPair::from_pkcs8(&der).map_err(|_| Error::IamTokenProofOfPossession)?;
    let rng = SystemRandom::new();
    let mut signature = vec![0u8; key_pair.public().modulus_len()];
    key_pair
        .sign(&RSA_PKCS1_SHA256, &rng, header.as_bytes(), &mut signature)
        .map_err(|_| Error::IamTokenProofOfPossession)?;
    Ok(base64::engine::general_purpose::STANDARD.encode(&signature))
}

/// Extracts the DER body of the first PKCS#8 `PRIVATE KEY` PEM block, using the
/// same `rustls-pemfile` reader the wallet loader uses (`Item::Pkcs8Key`). OCI
/// IAM database tokens ship an unencrypted PKCS#8 RSA key; a PKCS#1
/// (`RSA PRIVATE KEY`) or encrypted key is refused with the redacted PoP error.
fn pkcs8_pem_to_der(pem: &str) -> Result<Vec<u8>> {
    let mut reader = std::io::BufReader::new(pem.as_bytes());
    loop {
        match rustls_pemfile::read_one(&mut reader).map_err(|_| Error::IamTokenProofOfPossession)? {
            Some(rustls_pemfile::Item::Pkcs8Key(der)) => {
                return Ok(der.secret_pkcs8_der().to_vec())
            }
            // Skip any non-key blocks (e.g. a stray certificate) and keep looking.
            Some(_) => continue,
            None => return Err(Error::IamTokenProofOfPossession),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::signature::{UnparsedPublicKey, RSA_PKCS1_2048_8192_SHA256};

    // Throwaway 2048-bit RSA key in PKCS#8 PEM — test-only, never a real secret.
    // Kept in the exempted `tests/fixtures/tls/` dir (secret_scan skips it there)
    // and pulled in only under `#[cfg(test)]`, so it never ships in the packaged
    // crate (`exclude = ["tests/"]`) and never trips the private-key heuristic on
    // this source file.
    const TEST_PKCS8_PEM: &str = include_str!("../tests/fixtures/tls/iam_pop_test_key_pkcs8.pem");

    #[test]
    fn header_matches_reference_format() {
        // 2026-07-17T07:00:00Z → the reference's IMF-fixdate `date:` field.
        let now = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_784_271_600);
        let header = build_signing_header("adb.example.db.example.com", "10.0.0.5", 1522, now);
        assert_eq!(
            header,
            "date: Fri, 17 Jul 2026 07:00:00 GMT\n\
             (request-target): adb.example.db.example.com\n\
             host: 10.0.0.5:1522"
        );
    }

    #[test]
    fn signature_verifies_against_the_public_key() {
        let now = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_784_271_600);
        let header = build_signing_header("svc", "h", 1522, now);
        let sig_b64 = sign_signing_header(TEST_PKCS8_PEM, &header).expect("sign");
        let sig = base64::engine::general_purpose::STANDARD
            .decode(sig_b64.as_bytes())
            .expect("base64");

        // Recover the public key from the same key pair and verify the signature
        // is a valid RSA-PKCS1v15-SHA256 signature over the exact header bytes —
        // exactly the check the OCI database performs with the token's `jwk`.
        let der = pkcs8_pem_to_der(TEST_PKCS8_PEM).expect("der");
        let key_pair = RsaKeyPair::from_pkcs8(&der).expect("keypair");
        let public =
            UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, key_pair.public().as_ref());
        public
            .verify(header.as_bytes(), &sig)
            .expect("proof-of-possession signature must verify");

        // A tampered header must NOT verify.
        assert!(
            public.verify(b"date: tampered", &sig).is_err(),
            "signature must not verify a different header"
        );
    }

    #[test]
    fn rejects_non_pkcs8_key_material_without_leaking() {
        // Not a PKCS#8 PEM block — the reader finds no key item and fails closed.
        let err = sign_signing_header("unusable-key-material-xyzzy\n", "hdr").unwrap_err();
        assert!(matches!(err, Error::IamTokenProofOfPossession));
        // The fixed diagnostic must never echo the supplied material.
        assert!(!format!("{err}").contains("xyzzy"));
    }
}
