//! TCPS SNI string construction.
//!
//! Oracle's TCPS transport uses a specially-formatted SNI (Server Name
//! Indication) value that lets the server bypass one of the TLS
//! negotiations. The format is taken verbatim from python-oracledb thin
//! (`impl/thin/transport.pyx::_calc_sni_data`):
//!
//! ```text
//! S{len(service_name)}.{service_name}[.T1.{server_type[0]}].V3.{TNS_VERSION_DESIRED}
//! ```
//!
//! * `S{len}.` — the literal `S`, the byte length of the service name, a dot.
//! * `{service_name}` — the connect-data service name verbatim.
//! * `.T1.{c}` — present only when a `server_type` is set; `{c}` is the first
//!   character of `server_type` (e.g. `D` for `dedicated`).
//! * `.V3.{version}` — the literal `.V3.` and the desired TNS protocol version
//!   (`TNS_VERSION_DESIRED`, currently 319).
//!
//! The value is passed to rustls as the `ServerName`. python-oracledb disables
//! standard hostname verification (`check_hostname = False`) and instead runs
//! the Oracle DN-match algorithm after the handshake (see [`super::dn`]), so the
//! SNI string does not need to be a resolvable DNS name from rustls's point of
//! view — but it must still be a syntactically valid `ServerName`. The Oracle
//! SNI format (dotted ASCII labels, digits and letters only) satisfies that.

use crate::TNS_VERSION_DESIRED;

/// Build the Oracle TCPS SNI string for the given service name and optional
/// server type, matching python-oracledb's `_calc_sni_data` exactly.
///
/// `server_type` is the `(SERVER=...)` value from the connect descriptor
/// (`dedicated`, `shared`, `pooled`, `emon`, ...). When present, only its first
/// character is encoded, as `.T1.{c}`.
#[must_use]
pub fn build_sni(service_name: &str, server_type: Option<&str>) -> String {
    let server_type_part = match server_type {
        Some(st) if !st.is_empty() => {
            // python-oracledb: f".T1.{description.server_type[:1]}"
            let first = st.chars().next().unwrap_or_default();
            format!(".T1.{first}")
        }
        _ => String::new(),
    };
    format!(
        "S{}.{}{}.V3.{}",
        service_name.len(),
        service_name,
        server_type_part,
        TNS_VERSION_DESIRED
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sni_basic_service_only() {
        // python-oracledb: S{len}.{service}.V3.{version}
        // "FREEPDB1" is 8 chars; TNS_VERSION_DESIRED is 319.
        assert_eq!(build_sni("FREEPDB1", None), "S8.FREEPDB1.V3.319");
    }

    #[test]
    fn sni_with_server_type_uses_first_char() {
        assert_eq!(build_sni("svc", Some("dedicated")), "S3.svc.T1.d.V3.319");
    }

    #[test]
    fn sni_with_emon_server_type() {
        assert_eq!(build_sni("svc", Some("emon")), "S3.svc.T1.e.V3.319");
    }

    #[test]
    fn sni_empty_server_type_is_omitted() {
        assert_eq!(build_sni("svc", Some("")), "S3.svc.V3.319");
    }

    #[test]
    fn sni_length_is_byte_count_not_padded() {
        // The length is the service-name length, decimal, not zero-padded.
        let svc = "a".repeat(12);
        assert_eq!(build_sni(&svc, None), format!("S12.{svc}.V3.319"));
    }

    #[test]
    fn sni_version_matches_protocol_constant() {
        let sni = build_sni("x", None);
        assert!(sni.ends_with(&format!(".V3.{TNS_VERSION_DESIRED}")));
    }
}
