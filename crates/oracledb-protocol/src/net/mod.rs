#![forbid(unsafe_code)]

pub mod connectstring;

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

impl EasyConnect {
    pub fn parse(input: &str) -> Result<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(ProtocolError::InvalidConnectDescriptor(
                "connect descriptor must not be empty".to_string(),
            ));
        }

        // Optional `protocol://` prefix (python-oracledb EZConnect syntax).
        let (protocol, rest) = if let Some(after) = trimmed.strip_prefix("tcps://") {
            (Protocol::Tcps, after)
        } else if let Some(after) = trimmed.strip_prefix("tcp://") {
            (Protocol::Tcp, after)
        } else {
            (Protocol::Tcp, trimmed)
        };

        let (host_port, service_name) = rest.split_once('/').ok_or_else(|| {
            ProtocolError::InvalidConnectDescriptor(
                "EZConnect descriptor must contain a service name".to_string(),
            )
        })?;
        let (host, port) = match host_port.rsplit_once(':') {
            Some((host, port)) => {
                let parsed_port = port.parse::<u16>().map_err(|_| {
                    ProtocolError::InvalidConnectDescriptor(format!("invalid port: {port}"))
                })?;
                (host, parsed_port)
            }
            None => (host_port, protocol.default_port()),
        };

        if host.is_empty() || service_name.is_empty() {
            return Err(ProtocolError::InvalidConnectDescriptor(
                "host and service name are required".to_string(),
            ));
        }

        Ok(Self {
            host: host.to_string(),
            port,
            service_name: service_name.to_string(),
            protocol,
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
}
