#![forbid(unsafe_code)]

pub mod capabilities;
pub mod crypto;
pub mod dpl;
pub mod net;
pub mod packet;
pub mod sql;
pub mod thin;
pub mod wire;

use std::borrow::Cow;

pub const PYTHON_ORACLEDB_REFERENCE_TAG: &str = "v4.0.1";
pub const PYTHON_ORACLEDB_REFERENCE_COMMIT: &str = "3daef052904e41668bb862e6fa40f43c22a81beb";
pub const TNS_VERSION_MIN: u16 = 300;
pub const TNS_VERSION_DESIRED: u16 = 319;

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("truncated packet header: got {got} bytes")]
    TruncatedHeader { got: usize },
    #[error("invalid packet length {length}; expected at least {minimum}")]
    InvalidPacketLength { length: usize, minimum: usize },
    #[error("packet length {declared} exceeds available bytes {available}")]
    IncompletePacket { declared: usize, available: usize },
    #[error("packet length {length} exceeds TNS two-byte length field")]
    PacketTooLarge { length: usize },
    #[error("unsupported TNS version {version}")]
    UnsupportedVersion { version: u16 },
    #[error("invalid client identity field {field}: {reason}")]
    InvalidClientIdentity {
        field: &'static str,
        reason: Cow<'static, str>,
    },
    #[error("invalid connect descriptor: {0}")]
    InvalidConnectDescriptor(String),
    #[error("TTC decode failed: {0}")]
    TtcDecode(&'static str),
    #[error("unknown TTC message type {message_type} at position {position}")]
    UnknownMessageType { message_type: u8, position: usize },
    #[error("server returned Oracle error: {0}")]
    ServerError(String),
    #[error("server returned Oracle error: {message}")]
    ServerErrorWithRowCount { message: String, row_count: u64 },
    #[error("unsupported feature: {0}")]
    UnsupportedFeature(&'static str),
    #[error("missing authentication parameter {key}")]
    MissingAuthParameter { key: &'static str },
    #[error("unsupported password verifier type {verifier_type:#x}")]
    UnsupportedVerifier { verifier_type: u32 },
    #[error("invalid AES key length")]
    InvalidAesKey,
    #[error("invalid server authentication response")]
    InvalidServerResponse,
    // The next three mirror python-oracledb error numbers DPY-8000, DPY-8001
    // and DPY-4041 so a Python-facing layer can map them one-to-one.
    // "exeeds" reproduces the reference's spelling (errors.py ERR_VALUE_TOO_LARGE).
    #[error(
        "DPY-8000: value of size {actual_size} exeeds maximum allowed size of \
         {max_size} for column \"{column_name}\" of row {row_num}"
    )]
    ValueTooLarge {
        actual_size: usize,
        max_size: u32,
        column_name: String,
        row_num: u64,
    },
    #[error("DPY-8001: value for column \"{column_name}\" may not be null on row {row_num}")]
    NullsNotAllowed { column_name: String, row_num: u64 },
    #[error("DPY-4041: the maximum size of a Direct Path load has been exceeded")]
    DirectPathLoadTooMuchData,
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),
}

pub type Result<T> = std::result::Result<T, ProtocolError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientIdentity {
    pub program: String,
    pub machine: String,
    pub osuser: String,
    pub terminal: String,
    pub driver_name: String,
}

impl ClientIdentity {
    pub fn new(
        program: impl Into<String>,
        machine: impl Into<String>,
        osuser: impl Into<String>,
        terminal: impl Into<String>,
        driver_name: impl Into<String>,
    ) -> Result<Self> {
        Ok(Self {
            program: sanitize_identity_field("program", program.into())?,
            machine: sanitize_identity_field("machine", machine.into())?,
            osuser: sanitize_identity_field("osuser", osuser.into())?,
            terminal: sanitize_identity_field("terminal", terminal.into())?,
            driver_name: sanitize_identity_field("driver_name", driver_name.into())?,
        })
    }
}

fn sanitize_identity_field(field: &'static str, value: String) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ProtocolError::InvalidClientIdentity {
            field,
            reason: Cow::Borrowed("value must not be empty"),
        });
    }

    let mut out = String::with_capacity(trimmed.len().min(30));
    for ch in trimmed.chars() {
        if ch.is_control() {
            return Err(ProtocolError::InvalidClientIdentity {
                field,
                reason: Cow::Borrowed("control characters are not allowed"),
            });
        }
        if out.len() + ch.len_utf8() > 30 {
            break;
        }
        out.push(ch);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_fields_are_trimmed_and_bounded() {
        let identity = ClientIdentity::new(
            "  program-name-longer-than-thirty-bytes  ",
            "machine",
            "user",
            "terminal",
            "driver",
        )
        .expect("valid identity fields should sanitize");

        assert_eq!(identity.program, "program-name-longer-than-thirt");
        assert_eq!(identity.machine, "machine");
    }

    #[test]
    fn identity_rejects_empty_fields() {
        let err = ClientIdentity::new("", "machine", "user", "terminal", "driver")
            .expect_err("empty program should be rejected");
        assert!(matches!(
            err,
            ProtocolError::InvalidClientIdentity {
                field: "program",
                ..
            }
        ));
    }
}
