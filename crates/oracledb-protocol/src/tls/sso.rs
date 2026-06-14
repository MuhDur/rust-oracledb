//! `cwallet.sso` (SSO auto-login wallet) reader — EXPERIMENTAL.
//!
//! `cwallet.sso` is Oracle's auto-login wallet: a proprietary binary container
//! that wraps a standard PKCS#12 (PFX). The outer container holds a 3-byte
//! magic, a small header, and an obfuscated password; the password unlocks the
//! embedded PKCS#12 which in turn holds the certificate chain and private key.
//!
//! This implementation is ported from go-ora
//! (`v3/configurations/wallet.go`, `wallet_algo.go`, `wallet_utils.go`) which
//! is the established open-source prior art for reading `cwallet.sso`.
//!
//! ## Scope and honesty
//!
//! This is gated behind the `experimental` feature. What is implemented:
//!
//! * Outer container: magic `A1 F8 4E`, magic version `6`/`7`/`8`, header
//!   version `6`, and the **`num3 == 6` AES-128-CBC auto-login** sub-type
//!   (the common modern case), including the auto-login-local (`magic
//!   version 8`) host/user HMAC-SHA1 key re-derivation.
//! * Inner PKCS#12 with the **PBES2 / PBKDF2 / AES-CBC** scheme (modern
//!   AES wallets). Salt, IV, iteration count and key length are read from the
//!   wallet; PRF HMAC-SHA256 / HMAC-SHA1 are supported.
//!
//! What is **not** implemented (returns an explicit
//! [`super::wallet::WalletError::Sso`] error rather than guessing):
//!
//! * `num3 == 0x35` single-DES outer sub-type and `num3 == 5` no-key sub-type.
//! * Inner PKCS#12 PBE-with-SHA-and-3DES (`1.2.840.113549.1.12.1.3`) and
//!   RC2 schemes.
//!
//! Operators whose wallet hits an unsupported branch should convert it to
//! `ewallet.pem` (fully supported, see [`super::wallet`]).

use crate::tls::wallet::{WalletContents, WalletError};

/// Outer SSO container magic (`{0xA1, 0xF8, 0x4E}`), from go-ora.
const SSO_MAGIC: [u8; 3] = [0xA1, 0xF8, 0x4E];
/// Hardcoded AES-128-CBC IV for the auto-login password block (`num3 == 6`).
const SSO_AES_IV: [u8; 16] = [
    0xC0, 0x34, 0xD8, 0x31, 0x1C, 0x02, 0xCE, 0xF8, 0x51, 0xF0, 0x14, 0x4B, 0x81, 0xED, 0x4B, 0xF2,
];

#[cfg(feature = "experimental")]
mod imp {
    use super::{SSO_AES_IV, SSO_MAGIC};
    use crate::tls::wallet::{WalletContents, WalletError};
    use aes::cipher::{BlockDecryptMut, KeyIvInit};
    use hmac::{Hmac, Mac};
    use sha1::Sha1;

    type Aes128CbcDec = cbc::Decryptor<aes::Aes128>;
    type HmacSha1 = Hmac<Sha1>;

    /// Decrypt the outer SSO container's auto-login password (the `num3 == 6`
    /// AES-128-CBC branch). Returns the password used to unlock the inner
    /// PKCS#12 plus the offset where the PKCS#12 begins.
    pub(super) fn decode_outer(data: &[u8]) -> Result<(Vec<u8>, usize), WalletError> {
        if data.len() < 13 {
            return Err(WalletError::Sso("file too short for SSO header".into()));
        }
        if data[0..3] != SSO_MAGIC {
            return Err(WalletError::Sso("invalid SSO wallet magic".into()));
        }
        let magic_version = data[3];
        let auto_login_local = match magic_version {
            54 | 55 => false,           // '6' / '7'
            56 => true,                 // '8' — machine-bound auto-login-local
            other => {
                return Err(WalletError::Sso(format!(
                    "invalid SSO magic version: {other}"
                )))
            }
        };
        let num1 = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        if num1 != 6 {
            return Err(WalletError::Sso(format!(
                "invalid wallet header version: {num1}"
            )));
        }
        let size = u32::from_be_bytes([data[8], data[9], data[10], data[11]]) as usize;
        let num3 = data[12];
        let mut index = 13usize;

        let mut password: Vec<u8> = match num3 {
            6 => {
                // 16-byte AES key follows, then the encrypted password.
                if data.len() < index + 16 {
                    return Err(WalletError::Sso("truncated SSO AES key".into()));
                }
                let key: [u8; 16] = data[index..index + 16]
                    .try_into()
                    .map_err(|_| WalletError::Sso("bad AES key slice".into()))?;
                index += 16;
                // passwordLen = size - 1 - 16 (go-ora v3).
                let password_len = size
                    .checked_sub(1 + 16)
                    .ok_or_else(|| WalletError::Sso("invalid SSO size field".into()))?;
                if data.len() < index + password_len {
                    return Err(WalletError::Sso("truncated SSO password block".into()));
                }
                let mut buf = data[index..index + password_len].to_vec();
                index += password_len;
                let dec = Aes128CbcDec::new(&key.into(), &SSO_AES_IV.into());
                // The password block length is a multiple of the AES block; the
                // go-ora reference does not strip padding here — the raw
                // decrypted bytes become the PKCS#12 password.
                decrypt_cbc_nopad(dec, &mut buf)
                    .map_err(|e| WalletError::Sso(format!("SSO AES decrypt: {e}")))?;
                buf
            }
            5 => {
                return Err(WalletError::Sso(
                    "SSO sub-type 5 (empty-password) is not supported; convert to ewallet.pem"
                        .into(),
                ))
            }
            0x35 => {
                return Err(WalletError::Sso(
                    "SSO sub-type 0x35 (single-DES) is not supported; convert to ewallet.pem"
                        .into(),
                ))
            }
            other => {
                return Err(WalletError::Sso(format!(
                    "invalid SSO header sub-type: {other}"
                )))
            }
        };

        if auto_login_local {
            password = derive_auto_login_local(&password)?;
        }

        Ok((password, index))
    }

    /// Re-derive the auto-login-local password (magic version 8): HMAC-SHA1 of
    /// the decrypted password keyed by `hostname + username`, byte-mapped into
    /// `[1, 127]`, first 16 bytes. Faithful to go-ora.
    fn derive_auto_login_local(password: &[u8]) -> Result<Vec<u8>, WalletError> {
        let mut hostname = hostname_short();
        let user = current_user();
        hostname.push_str(&user);
        let mut mac = <HmacSha1 as Mac>::new_from_slice(hostname.as_bytes())
            .map_err(|e| WalletError::Sso(format!("hmac init: {e}")))?;
        mac.update(password);
        let mut temp = mac.finalize().into_bytes().to_vec();
        for b in &mut temp {
            // (b + 128) % 128 % 127 + 1
            *b = ((((u16::from(*b) + 128) % 128) % 127) as u8) + 1;
        }
        temp.truncate(16);
        Ok(temp)
    }

    fn hostname_short() -> String {
        let host = std::env::var("HOSTNAME")
            .ok()
            .or_else(|| {
                std::fs::read_to_string("/proc/sys/kernel/hostname")
                    .ok()
                    .map(|s| s.trim().to_string())
            })
            .unwrap_or_default();
        match host.split_once('.') {
            Some((short, _)) => short.to_string(),
            None => host,
        }
    }

    fn current_user() -> String {
        std::env::var("USER")
            .or_else(|_| std::env::var("LOGNAME"))
            .unwrap_or_default()
    }

    /// CBC decrypt in place with NO padding removal (caller-controlled).
    fn decrypt_cbc_nopad(dec: Aes128CbcDec, buf: &mut [u8]) -> Result<(), String> {
        use aes::cipher::block_padding::NoPadding;
        let len = buf.len();
        dec.decrypt_padded_mut::<NoPadding>(&mut buf[..len])
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    /// Parse the inner PKCS#12 (PFX) and extract the cert chain + private key.
    pub(super) fn parse_pkcs12(
        data: &[u8],
        password: &[u8],
    ) -> Result<WalletContents, WalletError> {
        crate::tls::pfx::parse_pfx(data, password)
    }
}

/// Parse a `cwallet.sso` byte buffer into [`WalletContents`].
///
/// # Errors
/// Returns [`WalletError::SsoNotEnabled`] unless the `experimental` feature is
/// enabled, and [`WalletError::Sso`] for parse / unsupported-branch failures.
#[cfg(feature = "experimental")]
pub fn parse_cwallet_sso(data: &[u8]) -> Result<WalletContents, WalletError> {
    let (password, pkcs12_offset) = imp::decode_outer(data)?;
    imp::parse_pkcs12(&data[pkcs12_offset..], &password)
}

/// Parse a `cwallet.sso` byte buffer (disabled build).
///
/// # Errors
/// Always returns [`WalletError::SsoNotEnabled`].
#[cfg(not(feature = "experimental"))]
pub fn parse_cwallet_sso(_data: &[u8]) -> Result<WalletContents, WalletError> {
    let _ = (&SSO_MAGIC, &SSO_AES_IV);
    Err(WalletError::SsoNotEnabled)
}

#[cfg(all(test, feature = "experimental"))]
mod tests {
    use super::*;

    #[test]
    fn rejects_bad_magic() {
        let err = parse_cwallet_sso(&[0, 0, 0, 0, 0]).unwrap_err();
        assert!(matches!(err, WalletError::Sso(_)));
    }

    #[test]
    fn rejects_truncated() {
        let err = parse_cwallet_sso(&SSO_MAGIC).unwrap_err();
        assert!(matches!(err, WalletError::Sso(_)));
    }

    #[test]
    fn rejects_bad_magic_version() {
        // magic ok, but magic version byte invalid (not 6/7/8).
        let mut data = SSO_MAGIC.to_vec();
        data.push(99);
        data.extend_from_slice(&[0u8; 16]);
        let err = parse_cwallet_sso(&data).unwrap_err();
        assert!(matches!(err, WalletError::Sso(_)));
    }
}

#[cfg(not(feature = "experimental"))]
#[cfg(test)]
mod disabled_tests {
    use super::*;

    #[test]
    fn sso_disabled_returns_not_enabled() {
        let err = parse_cwallet_sso(&[0xA1, 0xF8, 0x4E]).unwrap_err();
        assert!(matches!(err, WalletError::SsoNotEnabled));
    }
}
