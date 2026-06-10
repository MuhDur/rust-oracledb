#![forbid(unsafe_code)]

use crate::{ProtocolError, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EasyConnect {
    pub host: String,
    pub port: u16,
    pub service_name: String,
}

impl EasyConnect {
    pub fn parse(input: &str) -> Result<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(ProtocolError::InvalidConnectDescriptor(
                "connect descriptor must not be empty".to_string(),
            ));
        }

        let (host_port, service_name) = trimmed.split_once('/').ok_or_else(|| {
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
            None => (host_port, 1521),
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
    }
}
