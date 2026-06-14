#![forbid(unsafe_code)]

pub mod connectstring;

/// `.tns-cassette` record/replay wire format (sans-I/O framing). Gated behind
/// the `cassette` feature so the default build is byte-identical.
#[cfg(feature = "cassette")]
pub mod cassette;

use crate::{ProtocolError, Result};

/// Transport protocol for the connection (the EZConnect `protocol://` prefix).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub enum Protocol {
    /// Plain TCP (default).
    #[default]
    Tcp,
    /// TLS-encrypted TCP (TCPS); default port 2484.
    Tcps,
}

impl Protocol {
    /// Default listener port for this protocol.
    #[must_use]
    pub fn default_port(self) -> u16 {
        match self {
            Self::Tcp => 1521,
            Self::Tcps => 2484,
        }
    }

    /// Returns whether this protocol requires a TLS handshake.
    #[must_use]
    pub fn is_tls(self) -> bool {
        matches!(self, Self::Tcps)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EasyConnect {
    pub host: String,
    pub port: u16,
    pub service_name: String,
    /// Transport protocol parsed from a `tcp://` / `tcps://` prefix (default
    /// [`Protocol::Tcp`]).
    pub protocol: Protocol,
}

impl From<connectstring::Protocol> for Protocol {
    fn from(value: connectstring::Protocol) -> Self {
        match value {
            connectstring::Protocol::Tcp => Self::Tcp,
            connectstring::Protocol::Tcps => Self::Tcps,
        }
    }
}

impl EasyConnect {
    /// Resolves a connect string into the single primary endpoint used by the
    /// thin connection path: host, port, service name, and transport protocol.
    ///
    /// This now delegates to the full [`connectstring`] parser, so it accepts
    /// not only EZConnect / EZConnect-Plus strings but also complete TNS
    /// connect descriptors (`(DESCRIPTION=...)`), `DESCRIPTION_LIST`s, and
    /// multi-address `ADDRESS_LIST`s — selecting the first address that has a
    /// host and the first description's `SERVICE_NAME`.
    pub fn parse(input: &str) -> Result<Self> {
        let descriptor = connectstring::parse(input)?.ok_or_else(|| {
            ProtocolError::InvalidConnectDescriptor(format!(
                "\"{input}\" is not a connect descriptor or EZConnect string \
                 (it may be a tnsnames.ora alias requiring a config directory)"
            ))
        })?;

        let address = descriptor.first_address().ok_or_else(|| {
            ProtocolError::InvalidConnectDescriptor(
                "connect descriptor defines no usable address (host is required)".to_string(),
            )
        })?;
        let host = address.host.clone().ok_or_else(|| {
            ProtocolError::InvalidConnectDescriptor("host is required".to_string())
        })?;
        let service_name = descriptor
            .first_description()
            .connect_data
            .service_name
            .clone()
            .ok_or_else(|| {
                ProtocolError::InvalidConnectDescriptor("service name is required".to_string())
            })?;

        Ok(Self {
            host,
            port: address.port,
            service_name,
            protocol: address.protocol.into(),
        })
    }

    /// Parses a connect string into the full resolved [`connectstring::Descriptor`],
    /// exposing the entire address topology and connect data (for diagnostics or
    /// callers that need more than the single primary endpoint).
    pub fn parse_descriptor(input: &str) -> Result<connectstring::Descriptor> {
        connectstring::parse(input)?.ok_or_else(|| {
            ProtocolError::InvalidConnectDescriptor(format!(
                "\"{input}\" is not a connect descriptor or EZConnect string"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_easy_connect_with_default_port() {
        let parsed = EasyConnect::parse("localhost/FREEPDB1")
            .expect("default-port EZConnect descriptor should parse");
        assert_eq!(parsed.host, "localhost");
        assert_eq!(parsed.port, 1521);
        assert_eq!(parsed.service_name, "FREEPDB1");
    }

    #[test]
    fn parses_easy_connect_with_explicit_port() {
        let parsed = EasyConnect::parse("db.example.test:1522/service")
            .expect("explicit-port EZConnect descriptor should parse");
        assert_eq!(parsed.host, "db.example.test");
        assert_eq!(parsed.port, 1522);
        assert_eq!(parsed.service_name, "service");
        assert_eq!(parsed.protocol, Protocol::Tcp);
    }

    #[test]
    fn parses_tcps_prefix_defaults_to_2484() {
        let parsed = EasyConnect::parse("tcps://db.example.test/FREEPDB1")
            .expect("tcps EZConnect descriptor should parse");
        assert_eq!(parsed.host, "db.example.test");
        assert_eq!(parsed.port, 2484);
        assert_eq!(parsed.service_name, "FREEPDB1");
        assert_eq!(parsed.protocol, Protocol::Tcps);
        assert!(parsed.protocol.is_tls());
    }

    #[test]
    fn parses_tcps_prefix_with_explicit_port() {
        let parsed = EasyConnect::parse("tcps://host:2484/svc").expect("should parse");
        assert_eq!(parsed.port, 2484);
        assert_eq!(parsed.protocol, Protocol::Tcps);
    }

    #[test]
    fn parses_tcp_prefix_explicitly() {
        let parsed = EasyConnect::parse("tcp://host/svc").expect("should parse");
        assert_eq!(parsed.port, 1521);
        assert_eq!(parsed.protocol, Protocol::Tcp);
    }

    #[test]
    fn parses_full_tns_descriptor_via_easy_connect() {
        // EasyConnect::parse now delegates to the real connect-string parser,
        // so it must resolve the first address of a full TNS descriptor.
        let parsed = EasyConnect::parse(
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(HOST=db.example.test)(PORT=2484))\
             (CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))",
        )
        .expect("full TNS descriptor should parse via EasyConnect");
        assert_eq!(parsed.host, "db.example.test");
        assert_eq!(parsed.port, 2484);
        assert_eq!(parsed.service_name, "FREEPDB1");
        assert_eq!(parsed.protocol, Protocol::Tcps);
    }

    #[test]
    fn picks_first_address_of_address_list() {
        let parsed = EasyConnect::parse(
            "(DESCRIPTION=(ADDRESS_LIST=\
             (ADDRESS=(PROTOCOL=tcp)(HOST=primary)(PORT=1521))\
             (ADDRESS=(PROTOCOL=tcp)(HOST=standby)(PORT=1522)))\
             (CONNECT_DATA=(SERVICE_NAME=svc)))",
        )
        .expect("address-list descriptor should parse");
        assert_eq!(parsed.host, "primary");
        assert_eq!(parsed.port, 1521);
    }
}
