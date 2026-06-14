//! Minimal PKCS#12 (PFX) reader for the SSO wallet's inner container —
//! EXPERIMENTAL, only the PBES2/PBKDF2/AES-CBC scheme.
//!
//! This is intentionally narrow: it understands exactly the structure Oracle's
//! modern (AES) `cwallet.sso` produces — a PFX whose AuthenticatedSafe holds an
//! `encryptedData` content encrypted with PBES2 (PBKDF2 + AES-CBC), wrapping a
//! `SafeContents` of key/cert SafeBags. Anything outside that path returns an
//! explicit error (see [`super::sso`] for the supported/unsupported matrix).
//!
//! Ported from go-ora `v3/configurations/wallet.go` + `wallet_algo.go`.

#![cfg(feature = "experimental")]

use crate::tls::wallet::{WalletContents, WalletError};
use der::asn1::ObjectIdentifier;
use der::{Decode, Reader, SliceReader, Tag};

// --- OIDs -------------------------------------------------------------------
const OID_DATA: &str = "1.2.840.113549.1.7.1";
const OID_ENCRYPTED_DATA: &str = "1.2.840.113549.1.7.6";
const OID_PBES2: &str = "1.2.840.113549.1.5.13";
const OID_PBKDF2: &str = "1.2.840.113549.1.5.12";
const OID_HMAC_SHA256: &str = "1.2.840.113549.2.9";
const OID_HMAC_SHA1: &str = "1.2.840.113549.2.7";
const OID_AES128_CBC: &str = "2.16.840.1.101.3.4.1.2";
const OID_AES192_CBC: &str = "2.16.840.1.101.3.4.1.22";
const OID_AES256_CBC: &str = "2.16.840.1.101.3.4.1.42";
const OID_KEY_BAG: &str = "1.2.840.113549.1.12.10.1.1";
const OID_PKCS8_SHROUDED_KEY_BAG: &str = "1.2.840.113549.1.12.10.1.2";
const OID_CERT_BAG: &str = "1.2.840.113549.1.12.10.1.3";

fn sso(msg: impl Into<String>) -> WalletError {
    WalletError::Sso(msg.into())
}

fn read_oid(reader: &mut SliceReader<'_>) -> Result<ObjectIdentifier, WalletError> {
    ObjectIdentifier::decode(reader).map_err(|e| sso(format!("OID decode: {e}")))
}

/// Read a single TLV, returning (tag, value-bytes), advancing the reader.
fn read_tlv<'a>(reader: &mut SliceReader<'a>) -> Result<(Tag, &'a [u8]), WalletError> {
    let header = der::Header::decode(reader).map_err(|e| sso(format!("TLV header: {e}")))?;
    let len = usize::try_from(header.length).map_err(|_| sso("length overflow"))?;
    let bytes = reader
        .read_slice(header.length)
        .map_err(|e| sso(format!("TLV body ({len} bytes): {e}")))?;
    Ok((header.tag, bytes))
}

/// Open a constructed value and return a reader over its contents.
fn into_seq<'a>(bytes: &'a [u8]) -> Result<SliceReader<'a>, WalletError> {
    SliceReader::new(bytes).map_err(|e| sso(format!("subreader: {e}")))
}

/// Top-level entry: parse a PFX and extract certs + key.
pub(super) fn parse_pfx(data: &[u8], password: &[u8]) -> Result<WalletContents, WalletError> {
    let mut root = into_seq(data)?;
    // PFX ::= SEQUENCE { version INTEGER, authSafe ContentInfo, macData OPTIONAL }
    let (tag, pfx_body) = read_tlv(&mut root)?;
    if tag != Tag::Sequence {
        return Err(sso("PFX: expected outer SEQUENCE"));
    }
    let mut pfx = into_seq(pfx_body)?;
    // version INTEGER
    let _ = read_tlv(&mut pfx)?;
    // authSafe ContentInfo ::= SEQUENCE { contentType OID, content [0] EXPLICIT }
    let (tag, ci_body) = read_tlv(&mut pfx)?;
    if tag != Tag::Sequence {
        return Err(sso("PFX authSafe: expected ContentInfo SEQUENCE"));
    }
    let auth_safe_data = read_content_info_data(ci_body, OID_DATA)?;

    // authSafe content (data) is an OCTET STRING wrapping DER AuthenticatedSafe
    // ::= SEQUENCE OF ContentInfo.
    let mut as_reader = into_seq(auth_safe_data)?;
    let (tag, authsafe_seq) = read_tlv(&mut as_reader)?;
    if tag != Tag::Sequence {
        return Err(sso("AuthenticatedSafe: expected SEQUENCE OF ContentInfo"));
    }

    let mut decrypted_safe_contents: Vec<u8> = Vec::new();
    let mut plain_safe_contents: Vec<Vec<u8>> = Vec::new();
    let mut inner = into_seq(authsafe_seq)?;
    while !inner.is_finished() {
        let (tag, ci) = read_tlv(&mut inner)?;
        if tag != Tag::Sequence {
            return Err(sso("AuthenticatedSafe element: expected ContentInfo"));
        }
        let mut ci_reader = into_seq(ci)?;
        let content_type = read_oid(&mut ci_reader)?;
        let ct = content_type.to_string();
        // content [0] EXPLICIT
        let (ctx_tag, content_bytes) = read_tlv(&mut ci_reader)?;
        if ctx_tag.is_context_specific() {
            // unwrap the explicit [0]
            let mut ctx = into_seq(content_bytes)?;
            if ct == OID_ENCRYPTED_DATA {
                let dec = decrypt_encrypted_data(&mut ctx, password)?;
                decrypted_safe_contents = dec;
            } else if ct == OID_DATA {
                // plain data: OCTET STRING wrapping SafeContents
                let (t, os) = read_tlv(&mut ctx)?;
                if t == Tag::OctetString {
                    plain_safe_contents.push(os.to_vec());
                }
            }
        }
    }

    let mut contents = WalletContents::default();
    if !decrypted_safe_contents.is_empty() {
        read_safe_contents(&decrypted_safe_contents, password, &mut contents)?;
    }
    for sc in &plain_safe_contents {
        read_safe_contents(sc, password, &mut contents)?;
    }

    if contents.ca_certificates.is_empty() && contents.client_cert_chain.is_empty() {
        return Err(sso("PFX produced no certificates"));
    }
    // Mirror ewallet behaviour: all certs are trust anchors; if a key exists,
    // the certs form the client chain too.
    if contents.client_private_key.is_some() && contents.client_cert_chain.is_empty() {
        contents.client_cert_chain = contents.ca_certificates.clone();
    }
    Ok(contents)
}

/// From a ContentInfo body, verify the contentType OID and return the inner
/// `data` OCTET STRING bytes (unwrapping the `[0] EXPLICIT` and OCTET STRING).
fn read_content_info_data<'a>(
    ci_body: &'a [u8],
    expected_oid: &str,
) -> Result<&'a [u8], WalletError> {
    let mut reader = into_seq(ci_body)?;
    let oid = read_oid(&mut reader)?;
    if oid.to_string() != expected_oid {
        return Err(sso(format!(
            "ContentInfo: expected {expected_oid}, got {oid}"
        )));
    }
    let (ctx_tag, content) = read_tlv(&mut reader)?;
    if !ctx_tag.is_context_specific() {
        return Err(sso("ContentInfo: expected [0] EXPLICIT content"));
    }
    let mut ctx = into_seq(content)?;
    let (t, os) = read_tlv(&mut ctx)?;
    if t != Tag::OctetString {
        return Err(sso("ContentInfo content: expected OCTET STRING"));
    }
    Ok(os)
}

/// Decrypt an EncryptedData (already unwrapped from its `[0] EXPLICIT`).
///
/// EncryptedData ::= SEQUENCE { version, EncryptedContentInfo }
/// EncryptedContentInfo ::= SEQUENCE {
///   contentType OID, contentEncryptionAlgorithm AlgorithmIdentifier,
///   encryptedContent [0] IMPLICIT OCTET STRING }
fn decrypt_encrypted_data(
    ctx: &mut SliceReader<'_>,
    password: &[u8],
) -> Result<Vec<u8>, WalletError> {
    let (tag, ed_body) = read_tlv(ctx)?;
    if tag != Tag::Sequence {
        return Err(sso("EncryptedData: expected SEQUENCE"));
    }
    let mut ed = into_seq(ed_body)?;
    let _ = read_tlv(&mut ed)?; // version
    let (tag, eci_body) = read_tlv(&mut ed)?;
    if tag != Tag::Sequence {
        return Err(sso("EncryptedContentInfo: expected SEQUENCE"));
    }
    let mut eci = into_seq(eci_body)?;
    let content_type = read_oid(&mut eci)?;
    if content_type.to_string() != OID_DATA {
        return Err(sso("EncryptedContentInfo: content type must be data"));
    }
    // contentEncryptionAlgorithm AlgorithmIdentifier ::= SEQUENCE { algo, params }
    let (tag, alg_body) = read_tlv(&mut eci)?;
    if tag != Tag::Sequence {
        return Err(sso("EncryptionAlgorithm: expected SEQUENCE"));
    }
    let (key, iv) = derive_pbes2(alg_body, password)?;

    // encryptedContent [0] IMPLICIT OCTET STRING
    let (ctag, enc_content) = read_tlv(&mut eci)?;
    if !ctag.is_context_specific() {
        return Err(sso("encryptedContent: expected [0] IMPLICIT"));
    }
    aes_cbc_decrypt(&key, &iv, enc_content)
}

/// Parse the PBES2 AlgorithmIdentifier and derive (key, iv).
///
/// algId.algorithm == PBES2; params ::= SEQUENCE { keyDerivationFunc, encScheme }.
fn derive_pbes2(alg_body: &[u8], password: &[u8]) -> Result<(Vec<u8>, Vec<u8>), WalletError> {
    let mut alg = into_seq(alg_body)?;
    let algo = read_oid(&mut alg)?;
    if algo.to_string() != OID_PBES2 {
        return Err(sso(format!(
            "unsupported PFX encryption algorithm {algo}; only PBES2 (AES) is \
             supported in experimental mode — convert wallet to ewallet.pem"
        )));
    }
    let (tag, params) = read_tlv(&mut alg)?;
    if tag != Tag::Sequence {
        return Err(sso("PBES2 params: expected SEQUENCE"));
    }
    let mut p = into_seq(params)?;
    // keyDerivationFunc AlgorithmIdentifier
    let (tag, kdf_body) = read_tlv(&mut p)?;
    if tag != Tag::Sequence {
        return Err(sso("PBES2 KDF: expected SEQUENCE"));
    }
    // encryptionScheme AlgorithmIdentifier
    let (tag, enc_body) = read_tlv(&mut p)?;
    if tag != Tag::Sequence {
        return Err(sso("PBES2 encScheme: expected SEQUENCE"));
    }

    // --- KDF (PBKDF2) ---
    let mut kdf = into_seq(kdf_body)?;
    let kdf_oid = read_oid(&mut kdf)?;
    if kdf_oid.to_string() != OID_PBKDF2 {
        return Err(sso(format!("unsupported KDF {kdf_oid}; only PBKDF2")));
    }
    let (tag, pbkdf2_params) = read_tlv(&mut kdf)?;
    if tag != Tag::Sequence {
        return Err(sso("PBKDF2 params: expected SEQUENCE"));
    }
    let mut pk = into_seq(pbkdf2_params)?;
    // salt OCTET STRING
    let (tag, salt) = read_tlv(&mut pk)?;
    if tag != Tag::OctetString {
        return Err(sso("PBKDF2 salt: expected OCTET STRING"));
    }
    // iterationCount INTEGER
    let (tag, iter_bytes) = read_tlv(&mut pk)?;
    if tag != Tag::Integer {
        return Err(sso("PBKDF2 iterations: expected INTEGER"));
    }
    let iterations = be_uint(iter_bytes)?;
    // optional keyLength INTEGER and prf AlgorithmIdentifier
    let mut key_len: Option<usize> = None;
    let mut prf = PrfHash::Sha1; // default per RFC 8018
    while !pk.is_finished() {
        let (tag, body) = read_tlv(&mut pk)?;
        if tag == Tag::Integer {
            key_len = Some(usize::try_from(be_uint(body)?).map_err(|_| sso("keylen overflow"))?);
        } else if tag == Tag::Sequence {
            let mut prf_reader = into_seq(body)?;
            let prf_oid = read_oid(&mut prf_reader)?;
            prf = match prf_oid.to_string().as_str() {
                OID_HMAC_SHA256 => PrfHash::Sha256,
                OID_HMAC_SHA1 => PrfHash::Sha1,
                other => return Err(sso(format!("unsupported PBKDF2 PRF {other}"))),
            };
        }
    }

    // --- encryption scheme (AES-CBC) ---
    let mut enc = into_seq(enc_body)?;
    let enc_oid = read_oid(&mut enc)?;
    let derived_key_len = match enc_oid.to_string().as_str() {
        OID_AES128_CBC => 16,
        OID_AES192_CBC => 24,
        OID_AES256_CBC => 32,
        other => {
            return Err(sso(format!(
                "unsupported PBES2 cipher {other}; only AES-CBC"
            )))
        }
    };
    let (tag, iv) = read_tlv(&mut enc)?;
    if tag != Tag::OctetString {
        return Err(sso("AES-CBC IV: expected OCTET STRING"));
    }
    if iv.len() != 16 {
        return Err(sso("AES-CBC IV must be 16 bytes"));
    }
    // The optional PBKDF2 keyLength, when present, MUST equal the AES cipher's
    // key size (RFC 8018 §6.2). Reject a mismatch rather than trusting the
    // wallet-declared length: an unbounded keyLength would otherwise flow into
    // `vec![0u8; key_len]` in pbkdf2_derive and let a malicious cwallet.sso
    // request an arbitrarily large allocation (OOM/DoS). This bounds key_len to
    // {16,24,32} by construction (bead rust-oracledb-exz).
    let key_len = match key_len {
        Some(specified) if specified != derived_key_len => {
            return Err(sso(format!(
                "PBES2 keyLength {specified} does not match AES key size {derived_key_len}"
            )));
        }
        Some(specified) => specified,
        None => derived_key_len,
    };
    let key = pbkdf2_derive(password, salt, iterations, key_len, prf)?;
    Ok((key, iv.to_vec()))
}

#[derive(Clone, Copy)]
enum PrfHash {
    Sha1,
    Sha256,
}

/// Largest PBKDF2 derived-key length we will ever allocate. Real keys are tiny
/// (AES-256 = 32 bytes); this guards `vec![0u8; key_len]` against a malicious
/// wallet-declared length even if a future caller forgets the cipher-size check
/// in derive_pbes2 (defense-in-depth for bead rust-oracledb-exz).
const MAX_PBKDF2_KEY_LEN: usize = 1024;

fn pbkdf2_derive(
    password: &[u8],
    salt: &[u8],
    iterations: u64,
    key_len: usize,
    prf: PrfHash,
) -> Result<Vec<u8>, WalletError> {
    use hmac::Hmac;
    if key_len > MAX_PBKDF2_KEY_LEN {
        return Err(sso(format!(
            "PBKDF2 keyLength {key_len} exceeds maximum {MAX_PBKDF2_KEY_LEN}"
        )));
    }
    let iters = u32::try_from(iterations).unwrap_or(u32::MAX);
    let mut out = vec![0u8; key_len];
    match prf {
        PrfHash::Sha1 => {
            pbkdf2::pbkdf2::<Hmac<sha1::Sha1>>(password, salt, iters, &mut out).unwrap_or_default();
        }
        PrfHash::Sha256 => {
            pbkdf2::pbkdf2::<Hmac<sha2::Sha256>>(password, salt, iters, &mut out)
                .unwrap_or_default();
        }
    }
    Ok(out)
}

fn aes_cbc_decrypt(key: &[u8], iv: &[u8], ct: &[u8]) -> Result<Vec<u8>, WalletError> {
    use aes::cipher::{block_padding::Pkcs7, BlockDecryptMut, KeyIvInit};
    let mut buf = ct.to_vec();
    macro_rules! run {
        ($aes:ty) => {{
            type Dec = cbc::Decryptor<$aes>;
            let dec = Dec::new_from_slices(key, iv).map_err(|e| sso(format!("AES init: {e}")))?;
            let pt = dec
                .decrypt_padded_mut::<Pkcs7>(&mut buf)
                .map_err(|e| sso(format!("AES decrypt/unpad: {e}")))?;
            Ok(pt.to_vec())
        }};
    }
    match key.len() {
        16 => run!(aes::Aes128),
        24 => run!(aes::Aes192),
        32 => run!(aes::Aes256),
        n => Err(sso(format!("bad AES key length {n}"))),
    }
}

/// Parse a SafeContents (`SEQUENCE OF SafeBag`) and collect certs/keys.
///
/// SafeBag ::= SEQUENCE { bagId OID, bagValue [0] EXPLICIT, bagAttributes OPTIONAL }
fn read_safe_contents(
    data: &[u8],
    password: &[u8],
    out: &mut WalletContents,
) -> Result<(), WalletError> {
    let mut reader = into_seq(data)?;
    let (tag, seq) = read_tlv(&mut reader)?;
    if tag != Tag::Sequence {
        return Err(sso("SafeContents: expected SEQUENCE OF SafeBag"));
    }
    let mut bags = into_seq(seq)?;
    while !bags.is_finished() {
        let (tag, bag) = read_tlv(&mut bags)?;
        if tag != Tag::Sequence {
            continue;
        }
        let mut bag_reader = into_seq(bag)?;
        let bag_id = read_oid(&mut bag_reader)?;
        let (ctx_tag, value) = read_tlv(&mut bag_reader)?;
        if !ctx_tag.is_context_specific() {
            continue;
        }
        match bag_id.to_string().as_str() {
            OID_KEY_BAG => {
                // bagValue is a PKCS#8 PrivateKeyInfo (DER) directly.
                out.client_private_key = Some(value.to_vec());
            }
            OID_PKCS8_SHROUDED_KEY_BAG => {
                // bagValue [0] EXPLICIT wraps an EncryptedPrivateKeyInfo
                // ::= SEQUENCE { encryptionAlgorithm AlgorithmIdentifier,
                //                encryptedData OCTET STRING }.
                // Only the PBES2/AES scheme is supported (the modern wallet
                // format); other schemes return an explicit error.
                let mut bv = into_seq(value)?;
                let (epki_tag, epki_body) = read_tlv(&mut bv)?;
                if epki_tag != Tag::Sequence {
                    continue;
                }
                let mut epki = into_seq(epki_body)?;
                let (alg_tag, alg_body) = read_tlv(&mut epki)?;
                if alg_tag != Tag::Sequence {
                    return Err(sso("shrouded key: expected AlgorithmIdentifier"));
                }
                let (key, iv) = derive_pbes2(alg_body, password)?;
                let (ct_tag, ct) = read_tlv(&mut epki)?;
                if ct_tag != Tag::OctetString {
                    return Err(sso("shrouded key: expected encrypted OCTET STRING"));
                }
                let pkcs8 = aes_cbc_decrypt(&key, &iv, ct)?;
                out.client_private_key = Some(pkcs8);
            }
            OID_CERT_BAG => {
                // bagValue [0] EXPLICIT wraps a CertBag SEQUENCE; unwrap it.
                // CertBag ::= SEQUENCE { certId OID, certValue [0] EXPLICIT OCTET STRING }
                let mut bv = into_seq(value)?;
                let (cb_tag, cb_body) = read_tlv(&mut bv)?;
                if cb_tag != Tag::Sequence {
                    continue;
                }
                let mut cb = into_seq(cb_body)?;
                let _cert_id = read_oid(&mut cb)?;
                let (ct_tag, cv) = read_tlv(&mut cb)?;
                if ct_tag.is_context_specific() {
                    let mut cvr = into_seq(cv)?;
                    let (t, der) = read_tlv(&mut cvr)?;
                    if t == Tag::OctetString {
                        out.ca_certificates.push(der.to_vec());
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Decode a big-endian unsigned integer from DER INTEGER content bytes.
fn be_uint(bytes: &[u8]) -> Result<u64, WalletError> {
    if bytes.len() > 8 {
        return Err(sso("integer too large"));
    }
    let mut v: u64 = 0;
    for &b in bytes {
        v = (v << 8) | u64::from(b);
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Regression (bead rust-oracledb-exz): a malicious wallet declaring a huge
    // PBKDF2 keyLength must be rejected, never allocated. Before the bound,
    // `vec![0u8; key_len]` with key_len up to u64::MAX caused OOM/DoS.
    #[test]
    fn pbkdf2_rejects_oversized_key_len_instead_of_allocating() {
        let huge = pbkdf2_derive(b"pw", b"saltsalt", 1000, usize::MAX, PrfHash::Sha256);
        assert!(huge.is_err(), "oversized key_len must error, not allocate");
        let over = pbkdf2_derive(
            b"pw",
            b"saltsalt",
            1000,
            MAX_PBKDF2_KEY_LEN + 1,
            PrfHash::Sha256,
        );
        assert!(over.is_err(), "key_len just past the cap must error");
        // A real AES-256 key length still derives successfully.
        let ok = pbkdf2_derive(b"pw", b"saltsalt", 1000, 32, PrfHash::Sha256);
        assert!(ok.is_ok() && ok.unwrap().len() == 32);
    }
}
