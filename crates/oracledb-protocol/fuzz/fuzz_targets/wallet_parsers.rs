#![no_main]
//! Fuzz target: wallet and distinguished-name parsers.
//!
//! Entry points:
//! `oracledb_protocol::tls::wallet::parse_ewallet_pem`,
//! `oracledb_protocol::tls::wallet::parse_ewallet_p12`,
//! `oracledb_protocol::tls::sso::parse_cwallet_sso`, and
//! `oracledb_protocol::tls::dn::parse_dn`.
use libfuzzer_sys::fuzz_target;
use oracledb_protocol::tls::dn::parse_dn;
use oracledb_protocol::tls::sso::parse_cwallet_sso;
use oracledb_protocol::tls::wallet::{parse_ewallet_p12, parse_ewallet_pem};

fuzz_target!(|data: &[u8]| {
    if data.len() > 65_536 {
        return;
    }
    let (selector, payload) = data.split_first().map_or((0u8, data), |(v, r)| (*v, r));
    let password_len = payload.len().min(64);
    let password = (selector & 0x01 != 0)
        .then(|| core::str::from_utf8(&payload[..password_len]).ok())
        .flatten();

    let _ = parse_ewallet_pem(payload, password);
    let _ = parse_ewallet_p12(payload, password);
    let _ = parse_cwallet_sso(payload);
    let dn = String::from_utf8_lossy(payload);
    let _ = parse_dn(&dn);
});
