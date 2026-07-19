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

fn hex_upper(bytes: &[u8]) -> String {
    hex::encode_upper(bytes)
}

fn hex_upper_truncated(bytes: &[u8], chars: usize) -> String {
    let mut text = hex_upper(bytes);
    text.truncate(chars);
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    // This module had zero tests before H5: every error variant this file can
    // raise (MissingAuthParameter, UnsupportedVerifier, InvalidAesKey,
    // InvalidServerResponse) is server-response-driven — a hostile or buggy
    // listener controls `session_data`, so a missing/malformed AUTH_* key must
    // fail closed with the right typed error, never panic.

    #[test]
    fn generate_verifier_rejects_missing_auth_vfr_data() {
        let session_data = std::collections::BTreeMap::new();
        let err = generate_verifier(b"password", &session_data, TNS_VERIFIER_TYPE_12C)
            .expect_err("missing AUTH_VFR_DATA must be rejected");
        assert!(
            matches!(
                err,
                ProtocolError::MissingAuthParameter {
                    key: "AUTH_VFR_DATA"
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn generate_verifier_rejects_non_hex_auth_vfr_data() {
        let mut session_data = std::collections::BTreeMap::new();
        session_data.insert("AUTH_VFR_DATA".to_string(), "not-hex!!".to_string());
        let err = generate_verifier(b"password", &session_data, TNS_VERIFIER_TYPE_12C)
            .expect_err("non-hex AUTH_VFR_DATA must be rejected");
        assert!(
            matches!(
                err,
                ProtocolError::MissingAuthParameter {
                    key: "AUTH_VFR_DATA"
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn generate_verifier_rejects_unsupported_verifier_type() {
        let mut session_data = std::collections::BTreeMap::new();
        session_data.insert("AUTH_VFR_DATA".to_string(), "aabbcc".to_string());
        let bogus_verifier_type = 0x9999;
        let err = generate_verifier(b"password", &session_data, bogus_verifier_type)
            .expect_err("unknown verifier type must be rejected");
        assert!(
            matches!(
                err,
                ProtocolError::UnsupportedVerifier {
                    verifier_type: 0x9999
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn verify_server_response_rejects_missing_auth_svr_response() {
        let session_data = std::collections::BTreeMap::new();
        let err = verify_server_response(&[0u8; 32], &session_data)
            .expect_err("missing AUTH_SVR_RESPONSE must be rejected");
        assert!(
            matches!(err, ProtocolError::InvalidServerResponse),
            "got {err:?}"
        );
    }

    #[test]
    fn verify_server_response_rejects_non_hex_auth_svr_response() {
        let mut session_data = std::collections::BTreeMap::new();
        session_data.insert("AUTH_SVR_RESPONSE".to_string(), "zz-not-hex".to_string());
        let err = verify_server_response(&[0u8; 32], &session_data)
            .expect_err("non-hex AUTH_SVR_RESPONSE must be rejected");
        assert!(
            matches!(err, ProtocolError::InvalidServerResponse),
            "got {err:?}"
        );
    }

    #[test]
    fn verify_server_response_rejects_content_mismatch() {
        // A server response that decrypts cleanly under the shared combo key
        // but does not carry the "SERVER_TO_CLIENT" marker at bytes 16..32
        // must still be rejected — a server that can complete the handshake
        // crypto but skips the marker (or a MITM without the key trying
        // garbage) does not get treated as verified.
        let combo_key = [0x42u8; 32];
        let plain = b"0123456789ABCDEFNOT_SERVER_RESP!"; // 33 bytes; [16..32] != marker
        let ciphertext = encrypt_cbc(&combo_key, plain, true).expect("encrypt fixture");
        let mut session_data = std::collections::BTreeMap::new();
        session_data.insert("AUTH_SVR_RESPONSE".to_string(), hex_upper(&ciphertext));
        let err = verify_server_response(&combo_key, &session_data)
            .expect_err("mismatched server response content must be rejected");
        assert!(
            matches!(err, ProtocolError::InvalidServerResponse),
            "got {err:?}"
        );
    }

    #[test]
    fn verify_server_response_accepts_the_matching_marker() {
        // Positive control for the mismatch test above: proves the assertion
        // is discriminating (the marker really does flip the outcome) rather
        // than `verify_server_response` always failing.
        let combo_key = [0x24u8; 32];
        let mut plain = vec![0u8; 16];
        plain.extend_from_slice(b"SERVER_TO_CLIENT");
        let ciphertext = encrypt_cbc(&combo_key, &plain, true).expect("encrypt fixture");
        let mut session_data = std::collections::BTreeMap::new();
        session_data.insert("AUTH_SVR_RESPONSE".to_string(), hex_upper(&ciphertext));
        verify_server_response(&combo_key, &session_data)
            .expect("matching marker must be accepted");
    }

    #[test]
    fn decrypt_cbc_raw_rejects_wrong_length_aes_key() {
        // AES-256 requires an exact 32-byte key; a combo key of any other
        // length (corrupted derivation, or a hostile/buggy server driving
        // this path with unexpected session data) must fail closed with a
        // typed error instead of panicking inside the `aes` crate.
        let short_key = [0u8; 10];
        let block_aligned_ciphertext = [0u8; 16];
        let err = decrypt_cbc_raw(&short_key, &block_aligned_ciphertext)
            .expect_err("wrong-length AES key must be rejected");
        assert!(matches!(err, ProtocolError::InvalidAesKey), "got {err:?}");
    }
}
