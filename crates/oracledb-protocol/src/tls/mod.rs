//! Sans-I/O TLS/TCPS support: SNI construction, server-DN matching, and
//! Oracle wallet readers.
//!
//! These pieces are pure (no async, no sockets) so they can be unit-tested
//! directly and reused by the I/O crate, which drives the actual rustls
//! handshake over the asupersync transport. The split mirrors python-oracledb's
//! division between `transport.pyx`/`crypto.pyx` (the algorithms) and the
//! socket layer (the I/O).
//!
//! * [`sni`] — the Oracle TCPS SNI string (`S{len}.{service}.V3.{version}`).
//! * [`dn`] — server-certificate DN / SAN / CN matching (`check_server_dn`).
//! * [`wallet`] — `ewallet.pem` reader and wallet-location resolution.
//! * [`sso`] — `cwallet.sso` reader (experimental).

#![forbid(unsafe_code)]

pub mod dn;
#[cfg(feature = "experimental")]
mod pfx;
pub mod sni;
pub mod sso;
pub mod wallet;

pub use dn::{check_cert_dn, check_server_name, name_matches, parse_dn, DnMatchError};
pub use sni::build_sni;
pub use sso::parse_cwallet_sso;
pub use wallet::{
    parse_ewallet_pem, read_ewallet_pem, resolve_wallet_dir, WalletContents, WalletError,
    PEM_WALLET_FILE_NAME, SSO_WALLET_FILE_NAME,
};
