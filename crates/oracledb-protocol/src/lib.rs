#![forbid(unsafe_code)]

pub mod capabilities;
pub mod crypto;
pub mod dpl;
pub mod net;
pub mod oson;
pub mod packet;
pub mod sql;
pub mod thin;
pub mod tls;
pub mod vector;
pub mod wire;

use std::borrow::Cow;

pub const PYTHON_ORACLEDB_REFERENCE_TAG: &str = "v4.0.1";
pub const PYTHON_ORACLEDB_REFERENCE_COMMIT: &str = "3daef052904e41668bb862e6fa40f43c22a81beb";
pub const TNS_VERSION_MIN: u16 = 300;
/// Lowest server TNS protocol version this driver will talk to (12.1-era wire
/// format). The CONNECT packet still advertises [`TNS_VERSION_MIN`] like the
/// reference, but an ACCEPT below this floor is refused outright (reference
/// constants.pxi `TNS_VERSION_MIN_ACCEPTED` / connect.pyx raising
/// `ERR_SERVER_VERSION_NOT_SUPPORTED`, DPY-3010). Oracle 11g answers ACCEPT
/// with protocol version 314 and an older, shorter payload layout; without
/// this floor the parser would surface a misleading "truncated TTC payload"
/// decode error instead of a clean, self-explanatory refusal.
pub const TNS_VERSION_MIN_ACCEPTED: u16 = 315;
pub const TNS_VERSION_DESIRED: u16 = 319;

/// Structured details for a protocol resource-limit violation.
///
/// The limit names are stable policy keys from [`wire::ProtocolLimits`], so
/// callers can classify or log the exact bound that rejected a payload without
/// parsing the display string.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceLimit {
    pub limit: &'static str,
    pub observed: usize,
    pub maximum: usize,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProtocolError {
    #[error("truncated packet header: got {got} bytes")]
    TruncatedHeader { got: usize },
    #[error("invalid packet length {length}; expected at least {minimum}")]
    InvalidPacketLength { length: usize, minimum: usize },
    #[error("packet length {declared} exceeds available bytes {available}")]
    IncompletePacket { declared: usize, available: usize },
    #[error("packet length {length} exceeds TNS two-byte length field")]
    PacketTooLarge { length: usize },
    #[error(
        "server TNS protocol version {version} is below the minimum {minimum} supported by \
         this driver (Oracle Database 12.1 wire format); connections to this database server \
         version are not supported (mirrors python-oracledb DPY-3010)"
    )]
    UnsupportedVersion { version: u16, minimum: u16 },
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
    #[error("protocol resource limit exceeded: {limit} observed {observed}, maximum {maximum}")]
    ResourceLimit {
        limit: &'static str,
        observed: usize,
        maximum: usize,
    },
    #[error("server returned Oracle error: {0}")]
    ServerError(String),
    #[error("server returned Oracle error: {message}")]
    ServerErrorWithRowCount { message: String, row_count: u64 },
    #[error("server returned Oracle error: {}", .0.message)]
    ServerErrorInfo(Box<ServerErrorDetails>),
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
    // OSON / DB_TYPE_JSON. These mirror python-oracledb error numbers so the
    // Python-facing layer can map them one-to-one:
    //   DPY-5004 ERR_OSON_NODE_TYPE_NOT_SUPPORTED is *not* this; 5004 is the
    //   "not previously encoded" case (bad magic/version) and 5006 is a
    //   structurally invalid OSON image (truncation / bad offset).
    #[error("DPY-5004: input data is not in the OSON format: {0}")]
    OsonNotEncoded(&'static str),
    #[error("DPY-5006: invalid OSON data: {0}")]
    OsonInvalid(&'static str),
    /// A JSON scalar node decoded to an Oracle type with no Python mapping
    /// (e.g. INTERVAL YEAR TO MONTH). Mirrors DPY-3007 / ERR_DB_TYPE_NOT_SUPPORTED.
    #[error("DPY-3007: the data type {0} is not supported")]
    OsonTypeNotSupported(&'static str),
}

impl ProtocolError {
    pub fn resource_limit(&self) -> Option<ResourceLimit> {
        match self {
            Self::ResourceLimit {
                limit,
                observed,
                maximum,
            } => Some(ResourceLimit {
                limit,
                observed: *observed,
                maximum: *maximum,
            }),
            _ => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, ProtocolError>;

/// Structured server error information parsed from the TTC error trailer
/// (reference impl/thin/messages/base.pyx `_process_error_info`).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ServerErrorDetails {
    pub message: String,
    /// ORA error number (extended field).
    pub code: u32,
    /// Error position / parse offset (sb2; 0 when not reported).
    pub pos: i32,
    /// Server-reported row count at the time of the error.
    pub row_count: u64,
    /// Encoded rowid of the last affected row, if any.
    pub rowid: Option<String>,
    /// Row counts received before the error when
    /// `executemany(arraydmlrowcounts=True)` was requested.
    pub array_dml_row_counts: Option<Vec<u64>>,
}

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

/// Fuzz-only thin wrappers over `pub(crate)` decoder entry points.
///
/// This module is compiled **only** under `--cfg fuzzing` (set automatically
/// by `cargo-fuzz`). It exposes the crate-internal decode functions that take
/// adversarial server bytes — the server-error trailer parser and the
/// `pub(crate)` scalar codecs — so the `fuzz/` targets can call them directly
/// without widening the normal public API. Each wrapper is a zero-logic
/// forward to the real function; the goal is to prove these never panic on
/// malformed input (they must fail closed with a [`ProtocolError`]).
#[cfg(fuzzing)]
pub mod fuzz_api {
    use crate::wire::{BoundedReader, TtcReader};
    use crate::Result;

    /// Fuzz the server-error trailer parser (`parse_server_error_info`).
    /// `ttc_field_version` is taken from the first input byte so the fuzzer
    /// can explore both the legacy and 20.1+ trailer layouts.
    pub fn fuzz_parse_server_error_info(data: &[u8]) -> Result<()> {
        let (ttc_field_version, rest) = data.split_first().map_or((24u8, data), |(v, r)| (*v, r));
        let mut reader = TtcReader::new(rest);
        crate::thin::parse_server_error_info(&mut reader, ttc_field_version).map(|_| ())
    }

    /// Fuzz the server-side piggyback skipper (`skip_server_side_piggyback`).
    pub fn fuzz_skip_server_side_piggyback(data: &[u8]) -> Result<()> {
        let mut reader = TtcReader::new(data);
        crate::thin::skip_server_side_piggyback(&mut reader).map(|_| ())
    }

    /// Fuzz every `pub(crate)` scalar codec that decodes raw column bytes.
    /// Drives them all from one input so a single target covers the full
    /// scalar surface (NUMBER, datetime, intervals, binary float/double).
    pub fn fuzz_scalar_codecs(data: &[u8]) {
        let _ = crate::thin::decode_number_value(data);
        let _ = crate::thin::decode_datetime_value(data);
        let _ = crate::thin::decode_interval_ds(data);
        let _ = crate::thin::decode_interval_ym(data);
        let _ = crate::thin::decode_binary_float(data);
        let _ = crate::thin::decode_binary_double(data);
    }

    /// Fuzz the DbObject packed-image reader by walking arbitrary image bytes
    /// through the same length/header/value readers used by ADT and collection
    /// decoding. The selector bytes choose a bounded sequence of operations;
    /// expected decode failures are ignored, but panics/OOMs are bugs.
    pub fn fuzz_dbobject_image_walk(data: &[u8]) {
        let (ops, payload) = data.split_at(data.len().min(64));
        let mut reader = crate::thin::DbObjectPackedReader::new(payload);
        for op in ops {
            match op % 7 {
                0 => {
                    let _ = reader.read_u8();
                }
                1 => {
                    let _ = reader.read_i32be();
                }
                2 => {
                    let _ = reader.read_length();
                }
                3 => {
                    let _ = reader.read_value_bytes();
                }
                4 => {
                    let _ = reader.read_header();
                }
                5 => {
                    let _ = reader.read_atomic_null(op & 0x80 != 0);
                }
                _ => {
                    let count = usize::from(*op);
                    let _ = reader.alloc_count_checked(count, 1);
                    let _: Vec<u8> = reader.with_capacity_bounded(count, 1);
                }
            }
            if reader.remaining() == 0 {
                break;
            }
        }
    }

    /// Fuzz DbObject scalar/image-adjacent decoders that are not all reachable
    /// through one public parser boundary. This includes text, XMLTYPE, BFILE
    /// locator names, LOB text decoding, binary float/double, and the
    /// crate-private BINARY_INTEGER text parser.
    pub fn fuzz_dbobject_scalars(data: &[u8]) {
        let (selector, payload) = data.split_first().map_or((0u8, data), |(v, r)| (*v, r));
        let dbtype_name = match selector & 0x03 {
            0 => "DB_TYPE_VARCHAR",
            1 => "DB_TYPE_NVARCHAR",
            2 => "DB_TYPE_CHAR",
            _ => "DB_TYPE_NCHAR",
        };
        let csfrm = if selector & 0x04 == 0 {
            crate::thin::CS_FORM_IMPLICIT
        } else {
            crate::thin::CS_FORM_NCHAR
        };
        let locator = (selector & 0x08 != 0).then_some(payload);

        let _ = crate::thin::decode_dbobject_text(payload, dbtype_name);
        let _ = crate::thin::decode_dbobject_xmltype_text(payload);
        let _ = crate::thin::decode_lob_text(payload, csfrm, locator);
        let _ = crate::thin::decode_bfile_locator_name(payload);
        let _ = crate::thin::decode_dbobject_binary_float(payload);
        let _ = crate::thin::decode_dbobject_binary_double(payload);
        if let Ok(text) = core::str::from_utf8(payload) {
            let _ = crate::thin::parse_binary_integer_u32(text);
        }
    }

    /// Fuzz the Advanced Queuing response decoders (enqueue / dequeue / array).
    /// The first input byte selects the negotiated TTC field version and the
    /// payload kind so the fuzzer can reach the RAW / JSON / Object branches;
    /// the rest is the adversarial server payload. All three AQ parsers must
    /// fail closed on any malformed input (they only `read_*` from a bounded
    /// `TtcReader`, never index raw bytes).
    pub fn fuzz_aq_responses(data: &[u8]) {
        use crate::thin::aq::{
            parse_aq_array_response, parse_aq_deq_response, parse_aq_enq_response, AqPayloadKind,
        };
        let (selector, payload) = data.split_first().map_or((0u8, data), |(v, r)| (*v, r));
        let caps = crate::thin::ClientCapabilities {
            ttc_field_version: 24 - (selector & 0x07),
            ..crate::thin::ClientCapabilities::default()
        };
        let kind = match (selector >> 3) % 3 {
            0 => AqPayloadKind::Raw,
            1 => AqPayloadKind::Json,
            _ => AqPayloadKind::Object,
        };
        let _ = parse_aq_enq_response(payload, caps);
        let _ = parse_aq_deq_response(payload, caps, &kind);
        // `operation` and `props_count` are derived from the selector so the
        // array decoder explores both the dequeue-array and enqueue-array shapes.
        let operation = i32::from(selector >> 6);
        let props_count = u32::from(selector & 0x0f);
        let _ = parse_aq_array_response(payload, caps, operation, props_count, &kind);
    }

    /// Fuzz the subscription (CQN/AQ-notification) response + notification
    /// stream decoders. The first input byte drives the TTC field version, the
    /// namespace, and the QoS flags so the fuzzer reaches the OAC-record and
    /// grouping-notification branches. Both parsers must fail closed.
    pub fn fuzz_subscr_responses(data: &[u8]) {
        use crate::thin::{
            parse_notification_stream, parse_subscribe_response, ClientCapabilities,
        };
        let (selector, payload) = data.split_first().map_or((0u8, data), |(v, r)| (*v, r));
        let caps = ClientCapabilities {
            ttc_field_version: 24 - (selector & 0x07),
            ..ClientCapabilities::default()
        };
        let _ = parse_subscribe_response(payload, caps);
        let namespace = u32::from(selector >> 4);
        let public_qos = u32::from((selector >> 2) & 0x03);
        let _ = parse_notification_stream(payload, namespace, public_qos, None);
        let _ = parse_notification_stream(payload, namespace, public_qos, Some("FUZZDB"));
    }

    /// Fuzz the connect-string parsers on one untrusted string: the TNS
    /// connect-descriptor / EZConnect-Plus parser
    /// ([`crate::net::connectstring::parse`]) and the in-memory tnsnames.ora
    /// lexer (`tnsnames::fuzz_parse_file`).
    ///
    /// Both consume untrusted env / config / user input and must *never*
    /// panic / OOM / overflow the stack — only return `Err` (or, for the
    /// descriptor case, `Ok(None)` meaning "this is a tnsnames alias"). The
    /// descriptor recursion-depth DoS was fixed in bead `uf8`
    /// (`MAX_DESCRIPTOR_DEPTH`); this entry point guards that fix and hunts
    /// siblings in the EZConnect quote/host/port lexer and the tnsnames
    /// comment / multi-line / paren-balancing tokenizer.
    pub fn fuzz_connect_string(input: &str) {
        let _ = crate::net::connectstring::parse(input);
        let _ = crate::net::connectstring::tnsnames::fuzz_parse_file(input);
    }

    /// Drive `sql::parse_alter_session_value` — the `ALTER SESSION SET <key> =
    /// <value>` value extractor used to track session state (current_schema /
    /// edition) the server reflects back. It must never panic on arbitrary
    /// statement text, including non-UTF-8-boundary keys/values. The first byte
    /// selects the lookup key so the fuzzer exercises the matched + unmatched
    /// branches.
    pub fn fuzz_alter_session_value(input: &str) {
        let keys = ["current_schema", "edition", "time_zone", ""];
        let key = keys[input.as_bytes().first().copied().unwrap_or(0) as usize % keys.len()];
        let _ = crate::sql::parse_alter_session_value(input, key);
    }
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

    #[test]
    fn resource_limit_accessor_returns_typed_details() {
        let err = ProtocolError::ResourceLimit {
            limit: "response_bytes",
            observed: 33,
            maximum: 32,
        };
        assert_eq!(
            err.resource_limit(),
            Some(ResourceLimit {
                limit: "response_bytes",
                observed: 33,
                maximum: 32,
            })
        );
        assert_eq!(ProtocolError::TtcDecode("bad").resource_limit(), None);
    }
}
