#![forbid(unsafe_code)]

use crate::{ProtocolError, Result, TNS_VERSION_DESIRED, TNS_VERSION_MIN_ACCEPTED};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TnsVersion(u16);

impl TnsVersion {
    pub fn negotiate(server_version: u16) -> Result<Self> {
        // The refusal floor is TNS_VERSION_MIN_ACCEPTED (315, Oracle 12.1),
        // not TNS_VERSION_MIN (300): the CONNECT packet advertises 300 like
        // the reference, but the reference refuses any ACCEPT below 315
        // (connect.pyx ERR_SERVER_VERSION_NOT_SUPPORTED). Oracle 11g answers
        // with 314.
        if server_version < TNS_VERSION_MIN_ACCEPTED {
            return Err(ProtocolError::UnsupportedVersion {
                version: server_version,
                minimum: TNS_VERSION_MIN_ACCEPTED,
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
        // 314 is exactly what Oracle 11g negotiates; the reference refuses
        // anything below TNS_VERSION_MIN_ACCEPTED = 315 (12.1).
        assert!(matches!(
            TnsVersion::negotiate(TNS_VERSION_MIN_ACCEPTED - 1),
            Err(ProtocolError::UnsupportedVersion {
                version: 314,
                minimum: 315,
            })
        ));
        assert!(TnsVersion::negotiate(TNS_VERSION_MIN_ACCEPTED).is_ok());
    }
}
