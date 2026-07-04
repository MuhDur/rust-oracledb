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
//! * [`wallet`] — `ewallet.pem` / `ewallet.p12` readers and wallet-location
//!   resolution (including encrypted-PKCS#8 private-key decryption).
//! * [`sso`] — `cwallet.sso` (auto-login wallet) reader.

#![forbid(unsafe_code)]

pub mod dn;
mod pfx;
pub mod sni;
pub mod sso;
pub mod wallet;

// The TLS helpers live in their submodules (`dn`, `sni`, `sso`, `wallet`), which
// are themselves the public surface (`tls::dn::check_cert_dn`,
// `tls::wallet::WalletContents`, ...). We deliberately do NOT re-export them flat
// at `tls::`: a second public path per item is the module-coherence smell W1-T9
// closes (one obvious path per type).
