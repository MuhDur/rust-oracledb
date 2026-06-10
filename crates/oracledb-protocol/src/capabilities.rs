#![forbid(unsafe_code)]

use crate::{ProtocolError, Result, TNS_VERSION_DESIRED, TNS_VERSION_MIN};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TnsVersion(u16);

impl TnsVersion {
    pub fn negotiate(server_version: u16) -> Result<Self> {
        if server_version < TNS_VERSION_MIN {
            return Err(ProtocolError::UnsupportedVersion {
                version: server_version,
            });
        }

        Ok(Self(server_version.min(TNS_VERSION_DESIRED)))
    }

    pub fn as_u16(self) -> u16 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caps_high_versions_to_reference_desired_version() {
        assert_eq!(
            TnsVersion::negotiate(999)
                .expect("999 should negotiate down to desired reference version")
                .as_u16(),
            TNS_VERSION_DESIRED
        );
    }

    #[test]
    fn rejects_versions_below_reference_floor() {
        assert!(TnsVersion::negotiate(TNS_VERSION_MIN - 1).is_err());
    }
}
