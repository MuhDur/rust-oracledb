#![forbid(unsafe_code)]

use aes::cipher::{BlockDecrypt, BlockEncrypt, KeyInit};
use aes::Aes256;
use hex::FromHex;
use md5::Md5;
use pbkdf2::pbkdf2_hmac;
use rand::rngs::OsRng;
use rand::RngCore;
use sha1::Sha1;
use sha2::{Digest, Sha512};

use crate::thin::{TNS_VERIFIER_TYPE_11G_1, TNS_VERIFIER_TYPE_11G_2, TNS_VERIFIER_TYPE_12C};
use crate::{ProtocolError, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncryptedPassword {
    pub session_key: String,
    pub speedy_key: Option<String>,
    pub password: String,
    pub combo_key: Vec<u8>,
}

pub fn generate_verifier(
    password: &[u8],
    session_data: &std::collections::BTreeMap<String, String>,
    verifier_type: u32,
) -> Result<EncryptedPassword> {
    let verifier_data = decode_session_hex(session_data, "AUTH_VFR_DATA")?;
    let (key_len, password_hash, password_key) = match verifier_type {
        TNS_VERIFIER_TYPE_12C => {
            let iterations = parse_session_u32(session_data, "AUTH_PBKDF2_VGEN_COUNT")?;
            let mut salt = verifier_data.clone();
            salt.extend_from_slice(b"AUTH_PBKDF2_SPEEDY_KEY");
            let mut password_key = [0u8; 64];
            pbkdf2_hmac::<Sha512>(password, &salt, iterations, &mut password_key);
            let mut hasher = Sha512::new();
            hasher.update(password_key);
            hasher.update(&verifier_data);
            (
                32usize,
                hasher.finalize()[..32].to_vec(),
                Some(password_key.to_vec()),
            )
        }
        TNS_VERIFIER_TYPE_11G_1 | TNS_VERIFIER_TYPE_11G_2 => {
            let mut hasher = Sha1::new();
            hasher.update(password);
            hasher.update(&verifier_data);
            let mut hash = hasher.finalize().to_vec();
            hash.extend_from_slice(&[0, 0, 0, 0]);
            (24usize, hash, None)
        }
        other => {
            return Err(ProtocolError::UnsupportedVerifier {
                verifier_type: other,
            })
        }
    };

    let encoded_server_key = decode_session_hex(session_data, "AUTH_SESSKEY")?;
    let session_key_part_a = decrypt_cbc_raw(&password_hash, &encoded_server_key)?;
    let mut session_key_part_b = vec![0u8; session_key_part_a.len()];
    OsRng.fill_bytes(&mut session_key_part_b);
    let encoded_client_key = encrypt_cbc(&password_hash, &session_key_part_b, false)?;

    let (session_key, combo_key) = if session_key_part_a.len() == 48 {
        let session_key = hex_upper_truncated(&encoded_client_key, 96);
        let mut mixed = [0u8; 24];
        for ix in 16..40 {
            mixed[ix - 16] = session_key_part_a[ix] ^ session_key_part_b[ix];
        }
        let mut first = Md5::new();
        first.update(&mixed[..16]);
        let mut second = Md5::new();
        second.update(&mixed[16..]);
        let mut combo = first.finalize().to_vec();
        combo.extend_from_slice(&second.finalize());
        combo.truncate(key_len);
        (session_key, combo)
    } else {
        let session_key = hex_upper_truncated(&encoded_client_key, 64);
        let salt = decode_session_hex(session_data, "AUTH_PBKDF2_CSK_SALT")?;
        let iterations = parse_session_u32(session_data, "AUTH_PBKDF2_SDER_COUNT")?;
        if session_key_part_a.len() < key_len || session_key_part_b.len() < key_len {
            return Err(ProtocolError::TtcDecode("session key too short"));
        }
        let mut temp_key = session_key_part_b[..key_len].to_vec();
        temp_key.extend_from_slice(&session_key_part_a[..key_len]);
        let temp_key_hex = hex_upper(&temp_key);
        let mut combo_key = vec![0u8; key_len];
        pbkdf2_hmac::<Sha512>(temp_key_hex.as_bytes(), &salt, iterations, &mut combo_key);
        (session_key, combo_key)
    };

    let speedy_key = if matches!(verifier_type, TNS_VERIFIER_TYPE_12C) {
        let password_key =
            password_key.ok_or(ProtocolError::TtcDecode("missing 12c password key"))?;
        let mut salt = [0u8; 16];
        OsRng.fill_bytes(&mut salt);
        let mut plain = salt.to_vec();
        plain.extend_from_slice(&password_key);
        let encrypted = encrypt_cbc(&combo_key, &plain, false)?;
        Some(hex_upper_truncated(&encrypted, 160))
    } else {
        None
    };

    let mut salt = [0u8; 16];
    OsRng.fill_bytes(&mut salt);
    let mut password_with_salt = salt.to_vec();
    password_with_salt.extend_from_slice(password);
    let encrypted_password = encrypt_cbc(&combo_key, &password_with_salt, false)?;

    Ok(EncryptedPassword {
        session_key,
        speedy_key,
        password: hex_upper(&encrypted_password),
        combo_key,
    })
}

/// Encrypt the old/new password pair with the session combo key for the
/// change-password call (reference messages/auth.pyx `_encrypt_passwords`:
/// one shared random salt, AES-CBC under `_combo_key`, upper-hex encoding).
pub fn encrypt_change_password_pair(
    combo_key: &[u8],
    old_password: &[u8],
    new_password: &[u8],
) -> Result<(String, String)> {
    let mut salt = [0u8; 16];
    OsRng.fill_bytes(&mut salt);
    let mut old_with_salt = salt.to_vec();
    old_with_salt.extend_from_slice(old_password);
    let encrypted_old = encrypt_cbc(combo_key, &old_with_salt, false)?;
    let mut new_with_salt = salt.to_vec();
    new_with_salt.extend_from_slice(new_password);
    let encrypted_new = encrypt_cbc(combo_key, &new_with_salt, false)?;
    Ok((hex_upper(&encrypted_old), hex_upper(&encrypted_new)))
}

pub fn verify_server_response(
    combo_key: &[u8],
    session_data: &std::collections::BTreeMap<String, String>,
) -> Result<()> {
    let Some(response_hex) = session_data.get("AUTH_SVR_RESPONSE") else {
        return Err(ProtocolError::InvalidServerResponse);
    };
    let encoded_response =
        Vec::from_hex(response_hex).map_err(|_| ProtocolError::InvalidServerResponse)?;
    let response = decrypt_cbc_raw(combo_key, &encoded_response)?;
    if response.get(16..32) == Some(b"SERVER_TO_CLIENT") {
        Ok(())
    } else {
        Err(ProtocolError::InvalidServerResponse)
    }
}

fn decode_session_hex(
    session_data: &std::collections::BTreeMap<String, String>,
    key: &'static str,
) -> Result<Vec<u8>> {
    let value = session_data
        .get(key)
        .ok_or(ProtocolError::MissingAuthParameter { key })?;
    Vec::from_hex(value).map_err(|_| ProtocolError::MissingAuthParameter { key })
}

fn parse_session_u32(
    session_data: &std::collections::BTreeMap<String, String>,
    key: &'static str,
) -> Result<u32> {
    session_data
        .get(key)
        .ok_or(ProtocolError::MissingAuthParameter { key })?
        .parse()
        .map_err(|_| ProtocolError::MissingAuthParameter { key })
}

fn encrypt_cbc(key: &[u8], plain_text: &[u8], zeros: bool) -> Result<Vec<u8>> {
    let mut data = plain_text.to_vec();
    let pad_len = 16 - (data.len() % 16);
    if zeros {
        data.extend(std::iter::repeat_n(0, pad_len));
    } else {
        data.extend(std::iter::repeat_n(pad_len as u8, pad_len));
    }

    let cipher = Aes256::new_from_slice(key).map_err(|_| ProtocolError::InvalidAesKey)?;
    let mut previous = [0u8; 16];
    for block in data.chunks_mut(16) {
        for (byte, prev) in block.iter_mut().zip(previous) {
            *byte ^= prev;
        }
        let block_ref = aes::cipher::generic_array::GenericArray::from_mut_slice(block);
        cipher.encrypt_block(block_ref);
        previous.copy_from_slice(block_ref);
    }
    Ok(data)
}

fn decrypt_cbc_raw(key: &[u8], encrypted_text: &[u8]) -> Result<Vec<u8>> {
    if !encrypted_text.len().is_multiple_of(16) {
        return Err(ProtocolError::TtcDecode(
            "AES-CBC ciphertext length is not block aligned",
        ));
    }
    let cipher = Aes256::new_from_slice(key).map_err(|_| ProtocolError::InvalidAesKey)?;
    let mut out = encrypted_text.to_vec();
    let mut previous = [0u8; 16];
    for block in out.chunks_mut(16) {
        let current: [u8; 16] = block
            .try_into()
            .map_err(|_| ProtocolError::TtcDecode("invalid AES block"))?;
        let block_ref = aes::cipher::generic_array::GenericArray::from_mut_slice(block);
        cipher.decrypt_block(block_ref);
        for (byte, prev) in block.iter_mut().zip(previous) {
            *byte ^= prev;
        }
        previous = current;
    }
    Ok(out)
}

/// Sign `signing_string` with an RSA private key using PKCS#1 v1.5 padding over
/// SHA-256, returning the standard base64 encoding of the signature.
///
/// This is the OCI IAM instance/resource-principal request-signing primitive
/// (reference crypto.pyx `get_signature`): the Python reference calls
/// `private_key.sign(text.encode(), padding.PKCS1v15(), hashes.SHA256())` and
/// base64-encodes the result. PKCS#1 v1.5 with a fixed hash is deterministic —
/// the output is byte-identical to `openssl dgst -sha256 -sign key.pem`.
///
/// `private_key_pem` may be either a PKCS#8 (`-----BEGIN PRIVATE KEY-----`) or a
/// PKCS#1 (`-----BEGIN RSA PRIVATE KEY-----`) PEM block; both are accepted, as
/// python-oracledb's `serialization.load_pem_private_key` accepts both.
pub fn iam_signature(private_key_pem: &str, signing_string: &str) -> Result<String> {
    use base64::Engine as _;
    use rsa::pkcs1::DecodeRsaPrivateKey as _;
    use rsa::pkcs1v15::SigningKey;
    use rsa::pkcs8::DecodePrivateKey as _;
    use rsa::sha2::Sha256;
    use rsa::signature::{SignatureEncoding as _, Signer as _};
    use rsa::RsaPrivateKey;

    // Accept PKCS#8 first (the OCI-issued key format), falling back to PKCS#1.
    let private_key = RsaPrivateKey::from_pkcs8_pem(private_key_pem)
        .or_else(|_| RsaPrivateKey::from_pkcs1_pem(private_key_pem))
        .map_err(|_| ProtocolError::InvalidIamPrivateKey)?;

    // SigningKey::<Sha256> hashes the message with SHA-256 and applies the
    // DigestInfo-prefixed PKCS#1 v1.5 padding — exactly the OpenSSL/cryptography
    // `PKCS1v15() + SHA256` scheme. `Signer::try_sign` uses no RNG, so the
    // signature is deterministic.
    let signing_key = SigningKey::<Sha256>::new(private_key);
    let signature = signing_key
        .try_sign(signing_string.as_bytes())
        .map_err(|_| ProtocolError::IamSignatureFailed)?;
    Ok(base64::engine::general_purpose::STANDARD.encode(signature.to_bytes()))
}

fn hex_upper(bytes: &[u8]) -> String {
    hex::encode_upper(bytes)
}

fn hex_upper_truncated(bytes: &[u8], chars: usize) -> String {
    let mut text = hex_upper(bytes);
    text.truncate(chars);
    text
}

#[cfg(test)]
mod iam_signature_tests {
    use super::*;

    // Lab/synthetic RSA-2048 key — generated offline with OpenSSL 3.5 solely for
    // this test vector; it protects nothing and is NEVER real OCI key material.
    // The same key is provided in both PKCS#8 and PKCS#1 PEM form to prove both
    // parse paths and that they yield the identical (deterministic) signature.
    const TEST_KEY_PKCS8: &str = "-----BEGIN PRIVATE KEY-----\n\
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDs0WHBTqUp45Je\n\
660ynwJK4Evp7ipmT88kOEzWpL9/cm5yVXbd9NavwarrYVRO3uhfDYswL1kzD7MO\n\
nQiIqggqi0Xy8sfKltBR9UfC+QwWABOPjs9m3VFdBmLHfQjDYo92ekYzAUS6qGcn\n\
O+rkJdDxsnfWXEzdgF2bINZsgnEbW2rsYLjkS6HaphdgYWHYf5dDnO44ZUz818jc\n\
DcPHG3p2mEjaQBCmTiE6qknYtQwA2w5wc6/XrS1Q0R5NKql6ijUpsndw3aOeXVV1\n\
PGhGH+mrPLn1w0j3TxXMcM0EVBnMoBBvcq78lWO9jXmWm5rKzLbqsMfka6sVieiK\n\
kQkpHq8jAgMBAAECggEAYqovoLKr9GJYgsyNCeies2R0rF9qcdrxceU0+j6EfExI\n\
QMFjt0DBS1OUauHPltafGK8TXP11G+WAE+gP/KRep58EAx7sZ3bjDypyfgR16Rqa\n\
C7cTqQcvVWMKN5Pb2U+QduuloK14HY54/6sih3BL988Dx58H9Ub8eNu7ClVkW2Ez\n\
X25nlwoC0BHGu2UZdLt7/etSZ1FgeVY5haH8JIe2ZJ+Qgi/f+GnJm2fPtP/nvQ0n\n\
8uRvCiYDil86z8VOlSPKDHYBdmqT7ygVZOnYYFWGYpSjScADrtdUlSPgCwlJ9Y7v\n\
PtR7OEL7IBsAuHnIo8kZzYPs40lv8b74dY1mgn0dTQKBgQD570KYSs2LCwVk2Ia9\n\
4RSRO6gSwhS/PDqoj+h/C6M8yj7lf1lv8rszNMeHRsUzaOr1aoUR9FlpAfm5SAsA\n\
WwzzLgp8hrWrPMMSdi4Js8maf+lTCUPgApnx9C3PNyWqc/LVG5lPFecv8fFGyQPJ\n\
v0eAQquf3KA4BAlQ3V3ZrZs9hQKBgQDykKIO1BEJWfYX8RpiBOHXGzwYiNHbR0aV\n\
rawIaJu0k/VlAe5M4S4JhmGQ4C5dsSVDTXmNRf4K6b/kgIMVGDXRR5D91M1wgEek\n\
ZXETFMX+6XE1ccL9sHm40iiRn/8ey86gE6kgZs65IILQtHOyCLlhGpqJTA+n5iK3\n\
RvylbPmmhwKBgHBoNjhONEFbtJJVe8w8RcbH4OCG31Pe37bd+j/hWQpHH6CR9QSP\n\
U7hE/ZQOtTRY9Itp2+1mSywaEllNWH1RdfKM+3RkFaKtEkLkWfJNJNfSvXp2CUvf\n\
f6M9Ibk3YT5XJZjI79uMy0xZ8RzX3VdIKv05fvzH+lsXHaV72fDDzrUNAoGAWRwh\n\
LOljSNgvvCPq2U+J2Ag9T5YT4kaQ+7PNm1kyumgQoobPEJc43m7rsNlqryjA07TG\n\
jsvDxCaTRFKh1UILo1JzRYzD3CyEJTTPEX90LW19FXExfRkz3l32YWkxxBKDWfNf\n\
EnQyRXhYFmv0hNcHo1CurPmwVrII1uPRNMSQAkkCgYEA6dqDV1xmp5LhO6mQoUGr\n\
/pDAWtkwUwneYEU7bciZAvLcJG0d8bXTAdFAVwNJxtY7FPv7O7e79YUkkm1Lw6Qp\n\
YpKlmxnxE9F3x+G0KwrzCYxmU0WkhC+rqHoYeUEiy/XoWEOD4UNih2J06it+Eq1+\n\
F6I+AuaPEZ7ZAnCLeAmxhjg=\n\
-----END PRIVATE KEY-----\n";

    const TEST_KEY_PKCS1: &str = "-----BEGIN RSA PRIVATE KEY-----\n\
MIIEowIBAAKCAQEA7NFhwU6lKeOSXuutMp8CSuBL6e4qZk/PJDhM1qS/f3JuclV2\n\
3fTWr8Gq62FUTt7oXw2LMC9ZMw+zDp0IiKoIKotF8vLHypbQUfVHwvkMFgATj47P\n\
Zt1RXQZix30Iw2KPdnpGMwFEuqhnJzvq5CXQ8bJ31lxM3YBdmyDWbIJxG1tq7GC4\n\
5Euh2qYXYGFh2H+XQ5zuOGVM/NfI3A3Dxxt6dphI2kAQpk4hOqpJ2LUMANsOcHOv\n\
160tUNEeTSqpeoo1KbJ3cN2jnl1VdTxoRh/pqzy59cNI908VzHDNBFQZzKAQb3Ku\n\
/JVjvY15lpuaysy26rDH5GurFYnoipEJKR6vIwIDAQABAoIBAGKqL6Cyq/RiWILM\n\
jQnonrNkdKxfanHa8XHlNPo+hHxMSEDBY7dAwUtTlGrhz5bWnxivE1z9dRvlgBPo\n\
D/ykXqefBAMe7Gd24w8qcn4Edekamgu3E6kHL1VjCjeT29lPkHbrpaCteB2OeP+r\n\
IodwS/fPA8efB/VG/HjbuwpVZFthM19uZ5cKAtARxrtlGXS7e/3rUmdRYHlWOYWh\n\
/CSHtmSfkIIv3/hpyZtnz7T/570NJ/LkbwomA4pfOs/FTpUjygx2AXZqk+8oFWTp\n\
2GBVhmKUo0nAA67XVJUj4AsJSfWO7z7UezhC+yAbALh5yKPJGc2D7ONJb/G++HWN\n\
ZoJ9HU0CgYEA+e9CmErNiwsFZNiGveEUkTuoEsIUvzw6qI/ofwujPMo+5X9Zb/K7\n\
MzTHh0bFM2jq9WqFEfRZaQH5uUgLAFsM8y4KfIa1qzzDEnYuCbPJmn/pUwlD4AKZ\n\
8fQtzzclqnPy1RuZTxXnL/HxRskDyb9HgEKrn9ygOAQJUN1d2a2bPYUCgYEA8pCi\n\
DtQRCVn2F/EaYgTh1xs8GIjR20dGla2sCGibtJP1ZQHuTOEuCYZhkOAuXbElQ015\n\
jUX+Cum/5ICDFRg10UeQ/dTNcIBHpGVxExTF/ulxNXHC/bB5uNIokZ//HsvOoBOp\n\
IGbOuSCC0LRzsgi5YRqaiUwPp+Yit0b8pWz5pocCgYBwaDY4TjRBW7SSVXvMPEXG\n\
x+Dght9T3t+23fo/4VkKRx+gkfUEj1O4RP2UDrU0WPSLadvtZkssGhJZTVh9UXXy\n\
jPt0ZBWirRJC5FnyTSTX0r16dglL33+jPSG5N2E+VyWYyO/bjMtMWfEc191XSCr9\n\
OX78x/pbFx2le9nww861DQKBgFkcISzpY0jYL7wj6tlPidgIPU+WE+JGkPuzzZtZ\n\
MrpoEKKGzxCXON5u67DZaq8owNO0xo7Lw8Qmk0RSodVCC6NSc0WMw9wshCU0zxF/\n\
dC1tfRVxMX0ZM95d9mFpMcQSg1nzXxJ0MkV4WBZr9ITXB6NQrqz5sFayCNbj0TTE\n\
kAJJAoGBAOnag1dcZqeS4TupkKFBq/6QwFrZMFMJ3mBFO23ImQLy3CRtHfG10wHR\n\
QFcDScbWOxT7+zu3u/WFJJJtS8OkKWKSpZsZ8RPRd8fhtCsK8wmMZlNFpIQvq6h6\n\
GHlBIsv16FhDg+FDYodidOorfhKtfheiPgLmjxGe2QJwi3gJsYY4\n\
-----END RSA PRIVATE KEY-----\n";

    // The signing string is the reference header layout (date/(request-target)/host
    // joined by single `\n`), byte-identical to `thin::iam_signing_string`.
    const SIGNING_STRING: &str = "date: Wed, 04 Jul 2026 12:34:56 GMT\n\
(request-target): salesdb_high\n\
host: adb.us-ashburn-1.oraclecloud.com:1522";

    // Golden signature computed offline, independent of this code, with:
    //   printf '<signing string>' > s.txt
    //   openssl dgst -sha256 -sign key.pem s.txt | openssl base64 -A
    const EXPECTED_SIGNATURE_B64: &str = "QJwKj0zdGCIDFtiBmJj5Q9kfaKdX/6Vhylhwlv6UejcGu4DiUrn8fuTxLSvNjLxLJWs7hi4pEgfWd9ub9XehB8mlld6bkm2FkvsjdOr78SpWAG1lAhTrHnHHU3ser1lyVlraOk1Z5nWtGnlbOHmmwwarlZ+sAjCodX2oqjC+zLaUN7lOc5vj/rUl6jXrvKpYl9EWOio34y1UrEQtKYPNCNFARdHl7CMykr8sHMfNgZKVqEC5WznKMW6MIrOr3SNWJgw9h+mVX3gx5YIKQ2WIRkoufLbFkY957V78bXWIMNCJtJVfab1609jthpUl42JWI9p8zQMb2vHlSojIcmGAvg==";

    /// The core evidence: our RSA PKCS#1 v1.5 SHA-256 signature is byte-for-byte
    /// identical to the one OpenSSL's `dgst -sha256 -sign` produced offline.
    #[test]
    fn signature_matches_openssl_vector() {
        let sig = iam_signature(TEST_KEY_PKCS8, SIGNING_STRING).expect("PKCS#8 key must sign");
        assert_eq!(
            sig, EXPECTED_SIGNATURE_B64,
            "signature must match the OpenSSL-computed golden vector byte-for-byte"
        );
    }

    /// The PKCS#1 (`BEGIN RSA PRIVATE KEY`) encoding of the same key parses and
    /// yields the identical deterministic signature.
    #[test]
    fn pkcs1_and_pkcs8_agree() {
        let from_pkcs1 = iam_signature(TEST_KEY_PKCS1, SIGNING_STRING).expect("PKCS#1 must sign");
        assert_eq!(from_pkcs1, EXPECTED_SIGNATURE_B64);
    }

    /// Signing is deterministic: repeated calls produce identical output.
    #[test]
    fn signature_is_deterministic() {
        let a = iam_signature(TEST_KEY_PKCS8, SIGNING_STRING).unwrap();
        let b = iam_signature(TEST_KEY_PKCS8, SIGNING_STRING).unwrap();
        assert_eq!(a, b);
    }

    /// A non-PEM / malformed key is a typed error, never a panic.
    #[test]
    fn invalid_key_is_typed_error() {
        let err = iam_signature("not a pem key", SIGNING_STRING).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidIamPrivateKey));
    }
}
