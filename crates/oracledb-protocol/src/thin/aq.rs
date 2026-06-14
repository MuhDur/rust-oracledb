#![forbid(unsafe_code)]

//! Sans-io codecs for Oracle Advanced Queuing (AQ) enqueue/dequeue operations.
//!
//! Mirrors the reference thin driver's `impl/thin/messages/aq_*.pyx`:
//! - [`build_aq_enq_payload`] / [`parse_aq_enq_response`]  — FUNC 121 (single enqueue)
//! - [`build_aq_deq_payload`] / [`parse_aq_deq_response`]  — FUNC 122 (single dequeue)
//! - [`build_aq_array_enq_payload`] / [`build_aq_array_deq_payload`]
//!   / [`parse_aq_array_response`]                          — FUNC 145 (bulk enqueue/dequeue)
//!
//! The message-property / payload codecs are shared between all three so the
//! wire encoding is byte-identical to python-oracledb (golden traces under
//! `tests/golden/aq_*.txt`). Object payloads reuse [`super::dbobject`] and JSON
//! payloads reuse [`crate::oson`].

use super::*;
use crate::oson::{decode_oson, encode_oson, OsonValue};

/// Payload classification for a queue. Determines the TOID sentinel and the
/// payload-encoding branch taken during enqueue/dequeue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AqPayloadKind {
    /// RAW / bytes payload. TOID sentinel `bytes([0]*15 + [0x17])`.
    Raw,
    /// JSON payload (OSON). TOID sentinel `bytes([0]*15 + [0x47])`.
    Json,
    /// Object payload of a named type. TOID is the type's OID.
    Object,
}

/// Static description of an AQ queue, used by both enqueue and dequeue.
#[derive(Clone, Debug)]
pub struct AqQueueDesc {
    pub name: String,
    pub kind: AqPayloadKind,
    /// TOID of the payload: 16-byte sentinel for RAW/JSON, the type OID for
    /// object queues.
    pub payload_toid: Vec<u8>,
}

impl AqQueueDesc {
    /// Builds the queue descriptor, deriving the payload TOID from the kind.
    /// For object queues `object_oid` must carry the type's OID.
    pub fn new(name: String, kind: AqPayloadKind, object_oid: Option<Vec<u8>>) -> Self {
        let payload_toid = match kind {
            AqPayloadKind::Raw => raw_payload_toid(),
            AqPayloadKind::Json => json_payload_toid(),
            AqPayloadKind::Object => object_oid.unwrap_or_default(),
        };
        Self {
            name,
            kind,
            payload_toid,
        }
    }
}

fn raw_payload_toid() -> Vec<u8> {
    let mut toid = vec![0u8; 15];
    toid.push(0x17);
    toid
}

fn json_payload_toid() -> Vec<u8> {
    let mut toid = vec![0u8; 15];
    toid.push(0x47);
    toid
}

/// The payload value carried by a message being enqueued.
#[derive(Clone, Debug)]
pub enum AqPayloadValue {
    /// RAW bytes (already UTF-8 encoded if originally a string).
    Raw(Vec<u8>),
    /// JSON value encoded as OSON.
    Json(OsonValue),
    /// Pre-packed object image (the body produced by `DbObjectImpl::pack_image`).
    Object { oid: Vec<u8>, image: Vec<u8> },
}

/// Mutable message properties (reference `ThinMsgPropsImpl`). Defaults match
/// the reference: `delay=0`, `expiration=-1`, `priority=0`, `state=0`.
#[derive(Clone, Debug)]
pub struct AqMsgProps {
    pub priority: i32,
    pub delay: i32,
    pub expiration: i32,
    pub correlation: Option<String>,
    pub exception_queue: Option<String>,
    pub state: i32,
    pub enq_txn_id: Option<Vec<u8>>,
    /// Recipient names (multi-consumer enqueue). `None` => no recipient list.
    pub recipients: Option<Vec<String>>,
    /// Payload to enqueue. Required for enqueue; ignored for dequeue requests
    /// (where defaults are written).
    pub payload: Option<AqPayloadValue>,
}

impl Default for AqMsgProps {
    fn default() -> Self {
        Self {
            priority: 0,
            delay: 0,
            expiration: -1,
            correlation: None,
            exception_queue: None,
            state: 0,
            enq_txn_id: None,
            // Reference `ThinMsgPropsImpl.__init__` defaults recipients to an
            // empty list (not None): an empty list still writes pointer=1 with
            // a zero count, whereas None writes pointer=0.
            recipients: Some(Vec::new()),
            payload: None,
        }
    }
}

/// Enqueue options (reference `ThinEnqOptionsImpl`). `visibility` defaults to
/// `ENQ_ON_COMMIT (2)`, `delivery_mode` to `PERSISTENT (1)`.
#[derive(Clone, Debug)]
pub struct AqEnqOptions {
    pub visibility: u32,
    pub delivery_mode: u16,
}

impl Default for AqEnqOptions {
    fn default() -> Self {
        Self {
            visibility: 2,
            delivery_mode: TNS_AQ_MSG_PERSISTENT,
        }
    }
}

/// Dequeue options (reference `ThinDeqOptionsImpl`). Defaults: `mode=REMOVE(3)`,
/// `navigation=NEXT_MSG(3)`, `visibility=ON_COMMIT(2)`, `wait=WAIT_FOREVER`,
/// `delivery_mode=PERSISTENT(1)`.
#[derive(Clone, Debug)]
pub struct AqDeqOptions {
    pub condition: Option<String>,
    pub consumer_name: Option<String>,
    pub correlation: Option<String>,
    pub delivery_mode: u16,
    pub mode: i32,
    pub msgid: Option<Vec<u8>>,
    pub navigation: i32,
    pub visibility: i32,
    pub wait: u32,
}

impl Default for AqDeqOptions {
    fn default() -> Self {
        Self {
            condition: None,
            consumer_name: None,
            correlation: None,
            delivery_mode: TNS_AQ_MSG_PERSISTENT,
            mode: 3,
            msgid: None,
            navigation: 3,
            visibility: 2,
            wait: 0xFFFF_FFFF,
        }
    }
}

/// A message returned by dequeue (reference fields read by `_process_msg_props`
/// / `_process_payload`).
#[derive(Clone, Debug, Default)]
pub struct AqDeqMessage {
    pub priority: i32,
    pub delay: i32,
    pub expiration: i32,
    pub correlation: Option<String>,
    pub num_attempts: i32,
    pub exception_queue: Option<String>,
    pub state: i32,
    /// Oracle enqueue time decoded to a naive datetime, or `None`.
    pub enq_time: Option<QueryValue>,
    pub delivery_mode: u16,
    pub msgid: Option<Vec<u8>>,
    /// Decoded payload. `None` for an empty-payload message.
    pub payload: Option<AqDeqPayload>,
}

/// A decoded dequeue payload.
#[derive(Clone, Debug)]
pub enum AqDeqPayload {
    /// RAW bytes (may be empty for `DEQ_REMOVE_NODATA`).
    Raw(Vec<u8>),
    /// JSON decoded from OSON.
    Json(OsonValue),
    /// Object payload: the raw packed image (unpacked by the shim against the
    /// queue's payload type).
    Object(Vec<u8>),
}

// ---------------------------------------------------------------------------
// Shared message-property and payload codecs (reference aq_base.pyx).
// ---------------------------------------------------------------------------

/// Writes the TTC function-code preamble: message-type/function/seq plus the
/// `token_num` ub8 the server expects when the negotiated field version is at
/// least `23.1 EXT 1` (reference `Message._write_function_code`).
fn write_aq_function_code(
    writer: &mut TtcWriter,
    function_code: u8,
    seq_num: u8,
    ttc_field_version: u8,
) {
    writer.write_function_code_with_seq(function_code, seq_num);
    if ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1_EXT_1 {
        writer.write_ub8(0); // token_num
    }
}

fn write_value_with_length(writer: &mut TtcWriter, value: Option<&[u8]>) -> Result<()> {
    match value {
        None => {
            writer.write_ub4(0);
            Ok(())
        }
        Some(bytes) => writer.write_bytes_with_two_lengths(Some(bytes)),
    }
}

/// Writes the AQ message-property block (`_write_msg_props`).
fn write_msg_props(
    writer: &mut TtcWriter,
    props: &AqMsgProps,
    ttc_field_version: u8,
) -> Result<()> {
    writer.write_ub4(props.priority as u32);
    writer.write_ub4(props.delay as u32);
    writer.write_sb4(props.expiration);
    write_value_with_length(writer, props.correlation.as_deref().map(str::as_bytes))?;
    writer.write_ub4(0); // number of attempts
    write_value_with_length(writer, props.exception_queue.as_deref().map(str::as_bytes))?;
    writer.write_ub4(props.state as u32);
    writer.write_ub4(0); // enqueue time length
    write_value_with_length(writer, props.enq_txn_id.as_deref())?;
    writer.write_ub4(4); // number of extensions
    writer.write_u8(0x0e); // unknown extra byte
    writer.write_keyword_value_pair(None, None, TNS_AQ_EXT_KEYWORD_AGENT_NAME)?;
    writer.write_keyword_value_pair(None, None, TNS_AQ_EXT_KEYWORD_AGENT_ADDRESS)?;
    writer.write_keyword_value_pair(None, Some(b"\x00"), TNS_AQ_EXT_KEYWORD_AGENT_PROTOCOL)?;
    writer.write_keyword_value_pair(None, None, TNS_AQ_EXT_KEYWORD_ORIGINAL_MSGID)?;
    writer.write_ub4(0); // user property
    writer.write_ub4(0); // cscn
    writer.write_ub4(0); // dscn
    writer.write_ub4(0); // flags
    if ttc_field_version >= TNS_CCAP_FIELD_VERSION_21_1 {
        writer.write_ub4(0xFFFF_FFFF); // shard id
    }
    Ok(())
}

/// Writes the recipient-list key/value pairs (`_write_recipients`).
fn write_recipients(writer: &mut TtcWriter, recipients: &[String]) -> Result<()> {
    let mut index: u16 = 0;
    for recipient in recipients {
        writer.write_keyword_value_pair(Some(recipient.as_bytes()), None, index)?;
        writer.write_keyword_value_pair(None, None, index + 1)?;
        writer.write_keyword_value_pair(None, Some(b"\x00"), index + 2)?;
        index += 3;
    }
    Ok(())
}

/// Writes the message payload (`_write_payload`).
fn write_payload(
    writer: &mut TtcWriter,
    payload: &AqPayloadValue,
    supports_oson_long_fnames: bool,
) -> Result<()> {
    match payload {
        AqPayloadValue::Json(value) => {
            // write_oson(..., write_length=False): a QLocator (no chunk-length
            // prefix) followed by the OSON image as a length-prefixed chunk.
            let image = encode_oson(value, supports_oson_long_fnames)?;
            crate::vector::write_oson_aq_payload(writer, &image)
        }
        AqPayloadValue::Object { oid, image } => write_dbobject_bind(writer, oid, image),
        AqPayloadValue::Raw(bytes) => {
            writer.write_raw(bytes);
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// FUNC 121 — single enqueue
// ---------------------------------------------------------------------------

/// Builds the AQ enqueue (FUNC 121) request payload (reference
/// `AqEnqMessage._write_message`).
pub fn build_aq_enq_payload(
    queue: &AqQueueDesc,
    props: &AqMsgProps,
    enq_options: &AqEnqOptions,
    seq_num: u8,
    ttc_field_version: u8,
    supports_oson_long_fnames: bool,
) -> Result<Vec<u8>> {
    let payload = props
        .payload
        .as_ref()
        .ok_or(ProtocolError::TtcDecode("AQ enqueue has no payload"))?;
    let queue_name = queue.name.as_bytes();
    let mut writer = TtcWriter::new();
    write_aq_function_code(&mut writer, TNS_FUNC_AQ_ENQ, seq_num, ttc_field_version);
    writer.write_u8(1); // queue name (pointer)
    writer.write_ub4(queue_name.len() as u32);
    write_msg_props(&mut writer, props, ttc_field_version)?;
    match props.recipients.as_ref() {
        None => {
            writer.write_u8(0); // recipients (pointer)
            writer.write_ub4(0); // number of key/value pairs
        }
        Some(recipients) => {
            writer.write_u8(1);
            writer.write_ub4(3 * recipients.len() as u32);
        }
    }
    writer.write_ub4(enq_options.visibility);
    writer.write_u8(0); // relative message id (pointer)
    writer.write_ub4(0); // relative message length
    writer.write_ub4(0); // sequence deviation
    writer.write_u8(1); // TOID of payload (pointer)
    writer.write_ub4(16); // TOID of payload length
    writer.write_ub2(TNS_AQ_MESSAGE_VERSION);
    match queue.kind {
        AqPayloadKind::Json => {
            writer.write_u8(0); // payload (pointer)
            writer.write_u8(0); // RAW payload (pointer)
            writer.write_ub4(0); // RAW payload length
        }
        AqPayloadKind::Object => {
            writer.write_u8(1); // payload (pointer)
            writer.write_u8(0); // RAW payload (pointer)
            writer.write_ub4(0); // RAW payload length
        }
        AqPayloadKind::Raw => {
            let raw_len = match payload {
                AqPayloadValue::Raw(bytes) => bytes.len() as u32,
                _ => return Err(ProtocolError::TtcDecode("RAW queue requires RAW payload")),
            };
            writer.write_u8(0); // payload (pointer)
            writer.write_u8(1); // RAW payload (pointer)
            writer.write_ub4(raw_len);
        }
    }
    writer.write_u8(1); // return message id (pointer)
    writer.write_ub4(TNS_AQ_MESSAGE_ID_LENGTH as u32);
    let mut enq_flags = 0u32;
    if enq_options.delivery_mode == TNS_AQ_MSG_BUFFERED {
        enq_flags |= TNS_KPD_AQ_BUFMSG;
    }
    writer.write_ub4(enq_flags); // enqueue flags
    writer.write_u8(0); // extensions 1 (pointer)
    writer.write_ub4(0); // number of extensions 1
    writer.write_u8(0); // extensions 2 (pointer)
    writer.write_ub4(0); // number of extensions 2
    writer.write_u8(0); // source sequence number
    writer.write_ub4(0); // source sequence length
    writer.write_u8(0); // max sequence number
    writer.write_ub4(0); // max sequence length
    writer.write_u8(0); // output ack length
    writer.write_u8(0); // correlation (pointer)
    writer.write_ub4(0); // correlation length
    writer.write_u8(0); // sender name (pointer)
    writer.write_ub4(0); // sender name length
    writer.write_u8(0); // sender address (pointer)
    writer.write_ub4(0); // sender address length
    writer.write_u8(0); // sender charset id (pointer)
    writer.write_u8(0); // sender ncharset id (pointer)
    if ttc_field_version >= TNS_CCAP_FIELD_VERSION_20_1 {
        // JSON payload (pointer)
        writer.write_u8(u8::from(queue.kind == AqPayloadKind::Json));
    }

    writer.write_bytes_with_length(queue_name)?;
    if let Some(recipients) = props.recipients.as_ref() {
        write_recipients(&mut writer, recipients)?;
    }
    writer.write_raw(&queue.payload_toid);
    write_payload(&mut writer, payload, supports_oson_long_fnames)?;
    Ok(writer.into_bytes())
}

/// Parses an AQ enqueue (FUNC 121) response, returning the assigned 16-byte
/// message id (reference `AqEnqMessage._process_return_parameters`).
pub fn parse_aq_enq_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
) -> Result<Option<Vec<u8>>> {
    let mut reader = TtcReader::new(payload);
    let mut msgid: Option<Vec<u8>> = None;
    while reader.remaining() > 0 {
        let message_type = reader.read_u8()?;
        match message_type {
            0 => {}
            TNS_MSG_TYPE_PARAMETER => {
                let id = reader.read_raw(TNS_AQ_MESSAGE_ID_LENGTH)?.to_vec();
                let _ext_len = reader.read_ub2()?;
                msgid = Some(id);
            }
            TNS_MSG_TYPE_STATUS => {
                let _call_status = reader.read_ub4()?;
                let _seq = reader.read_ub2()?;
            }
            TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK => {
                let _ = skip_server_side_piggyback(&mut reader)?;
            }
            TNS_MSG_TYPE_END_OF_RESPONSE => break,
            TNS_MSG_TYPE_ERROR => {
                let info = parse_server_error_info(&mut reader, capabilities.ttc_field_version)?;
                if info.number != 0 {
                    return Err(ProtocolError::ServerErrorInfo(Box::new(
                        info.into_details(),
                    )));
                }
            }
            _ => {
                return Err(ProtocolError::UnknownMessageType {
                    message_type,
                    position: reader.position().saturating_sub(1),
                })
            }
        }
    }
    Ok(msgid)
}

// ---------------------------------------------------------------------------
// FUNC 122 — single dequeue
// ---------------------------------------------------------------------------

/// Builds the AQ dequeue (FUNC 122) request payload (reference
/// `AqDeqMessage._write_message`).
pub fn build_aq_deq_payload(
    queue: &AqQueueDesc,
    deq_options: &AqDeqOptions,
    seq_num: u8,
    ttc_field_version: u8,
) -> Result<Vec<u8>> {
    let queue_name = queue.name.as_bytes();
    let mut writer = TtcWriter::new();
    write_aq_function_code(&mut writer, TNS_FUNC_AQ_DEQ, seq_num, ttc_field_version);
    writer.write_u8(1); // queue name (pointer)
    writer.write_ub4(queue_name.len() as u32);
    writer.write_u8(1); // message properties
    writer.write_u8(1); // msg props length
    writer.write_u8(1); // recipient list
    writer.write_u8(1); // recipient list length
    let consumer_name = deq_options
        .consumer_name
        .as_ref()
        .filter(|name| !name.is_empty());
    match consumer_name {
        Some(name) => {
            writer.write_u8(1);
            writer.write_ub4(name.len() as u32);
        }
        None => {
            writer.write_u8(0);
            writer.write_ub4(0);
        }
    }
    writer.write_sb4(deq_options.mode);
    writer.write_sb4(deq_options.navigation);
    writer.write_sb4(deq_options.visibility);
    writer.write_sb4(deq_options.wait as i32);
    let msgid = deq_options.msgid.as_ref().filter(|id| !id.is_empty());
    match msgid {
        Some(_) => {
            writer.write_u8(1);
            writer.write_ub4(TNS_AQ_MESSAGE_ID_LENGTH as u32);
        }
        None => {
            writer.write_u8(0);
            writer.write_ub4(0);
        }
    }
    let correlation = deq_options.correlation.as_ref().filter(|c| !c.is_empty());
    match correlation {
        Some(c) => {
            writer.write_u8(1);
            writer.write_ub4(c.len() as u32);
        }
        None => {
            writer.write_u8(0);
            writer.write_ub4(0);
        }
    }
    writer.write_u8(1); // toid of payload
    writer.write_ub4(16); // toid length
    writer.write_ub2(TNS_AQ_MESSAGE_VERSION);
    writer.write_u8(1); // payload
    writer.write_u8(1); // return msg id
    writer.write_ub4(TNS_AQ_MESSAGE_ID_LENGTH as u32);
    let mut deq_flags = 0u32;
    match deq_options.delivery_mode {
        TNS_AQ_MSG_BUFFERED => deq_flags |= TNS_KPD_AQ_BUFMSG,
        TNS_AQ_MSG_PERSISTENT_OR_BUFFERED => deq_flags |= TNS_KPD_AQ_EITHER,
        _ => {}
    }
    writer.write_ub4(deq_flags);
    let condition = deq_options.condition.as_ref().filter(|c| !c.is_empty());
    match condition {
        Some(c) => {
            writer.write_u8(1);
            writer.write_ub4(c.len() as u32);
        }
        None => {
            writer.write_u8(0);
            writer.write_ub4(0);
        }
    }
    writer.write_u8(0); // extensions
    writer.write_ub4(0); // number of extensions
    if ttc_field_version >= TNS_CCAP_FIELD_VERSION_20_1 {
        writer.write_u8(0); // JSON payload
    }
    if ttc_field_version >= TNS_CCAP_FIELD_VERSION_21_1 {
        writer.write_ub4(0xFFFF_FFFF); // shard id (-1)
    }

    writer.write_bytes_with_length(queue_name)?;
    if let Some(name) = consumer_name {
        writer.write_bytes_with_length(name.as_bytes())?;
    }
    if let Some(id) = msgid {
        let mut id = id.clone();
        id.truncate(16);
        if id.len() < 16 {
            id.resize(16, 0);
        }
        writer.write_raw(&id);
    }
    if let Some(c) = correlation {
        writer.write_bytes_with_length(c.as_bytes())?;
    }
    writer.write_raw(&queue.payload_toid);
    if let Some(c) = condition {
        writer.write_bytes_with_length(c.as_bytes())?;
    }
    Ok(writer.into_bytes())
}

/// Outcome of a single dequeue.
#[derive(Clone, Debug, Default)]
pub struct AqDeqResult {
    /// The dequeued message, or `None` when the queue was empty (ORA-25228).
    pub message: Option<AqDeqMessage>,
}

/// Parses an AQ dequeue (FUNC 122) response (reference
/// `AqDeqMessage._process_return_parameters`).
pub fn parse_aq_deq_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
    kind: &AqPayloadKind,
) -> Result<AqDeqResult> {
    let mut reader = TtcReader::new(payload);
    let mut result = AqDeqResult::default();
    let mut no_msg_found = false;
    while reader.remaining() > 0 {
        let message_type = reader.read_u8()?;
        match message_type {
            0 => {}
            TNS_MSG_TYPE_PARAMETER => {
                let num_bytes = reader.read_ub4()?;
                if num_bytes > 0 {
                    let mut message = AqDeqMessage::default();
                    process_msg_props(&mut reader, &mut message, capabilities.ttc_field_version)?;
                    process_recipients(&mut reader)?;
                    message.payload = process_payload(&mut reader, kind)?;
                    message.msgid = Some(process_msg_id(&mut reader)?);
                    result.message = Some(message);
                }
            }
            TNS_MSG_TYPE_STATUS => {
                let _call_status = reader.read_ub4()?;
                let _seq = reader.read_ub2()?;
            }
            TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK => {
                let _ = skip_server_side_piggyback(&mut reader)?;
            }
            TNS_MSG_TYPE_END_OF_RESPONSE => break,
            TNS_MSG_TYPE_ERROR => {
                let info = parse_server_error_info(&mut reader, capabilities.ttc_field_version)?;
                if info.number == TNS_ERR_NO_MESSAGES_FOUND as u32 {
                    no_msg_found = true;
                } else if info.number != 0 {
                    return Err(ProtocolError::ServerErrorInfo(Box::new(
                        info.into_details(),
                    )));
                }
            }
            _ => {
                return Err(ProtocolError::UnknownMessageType {
                    message_type,
                    position: reader.position().saturating_sub(1),
                })
            }
        }
    }
    if no_msg_found {
        result.message = None;
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// FUNC 145 — bulk enqueue / dequeue
// ---------------------------------------------------------------------------

/// Builds the AQ array enqueue (FUNC 145, op=ENQ) request payload (reference
/// `AqArrayMessage._write_message` + `_write_array_enq`).
pub fn build_aq_array_enq_payload(
    queue: &AqQueueDesc,
    props_list: &[AqMsgProps],
    enq_options: &AqEnqOptions,
    seq_num: u8,
    ttc_field_version: u8,
    supports_oson_long_fnames: bool,
) -> Result<Vec<u8>> {
    let num_iters = props_list.len() as u32;
    let queue_name = queue.name.as_bytes();
    let mut writer = TtcWriter::new();
    write_aq_function_code(&mut writer, TNS_FUNC_AQ_ARRAY, seq_num, ttc_field_version);
    writer.write_u8(0); // input params (pointer)
    writer.write_ub4(0); // length
    writer.write_ub4(TNS_AQ_ARRAY_FLAGS_RETURN_MESSAGE_ID);
    writer.write_u8(1); // output params (pointer)
    writer.write_u8(0); // length
    writer.write_sb4(TNS_AQ_ARRAY_ENQ);
    writer.write_u8(1); // num iters (pointer)
    if ttc_field_version >= TNS_CCAP_FIELD_VERSION_21_1 {
        writer.write_ub4(0xFFFF); // shard id
    }
    writer.write_ub4(num_iters);

    let mut flags = 0u32;
    if enq_options.delivery_mode == TNS_AQ_MSG_BUFFERED {
        flags |= TNS_KPD_AQ_BUFMSG;
    }
    writer.write_ub4(0); // rel msgid len
    writer.write_u8(TNS_MSG_TYPE_ROW_HEADER);
    writer.write_bytes_with_two_lengths(Some(queue_name))?;
    writer.write_raw(&queue.payload_toid);
    writer.write_ub2(TNS_AQ_MESSAGE_VERSION);
    writer.write_ub4(flags);
    for props in props_list {
        let payload = props
            .payload
            .as_ref()
            .ok_or(ProtocolError::TtcDecode("AQ array enqueue has no payload"))?;
        writer.write_u8(TNS_MSG_TYPE_ROW_DATA);
        writer.write_ub4(flags); // aqi flags
        write_msg_props(&mut writer, props, ttc_field_version)?;
        match props.recipients.as_ref() {
            None => writer.write_ub4(0),
            Some(recipients) => {
                writer.write_ub4(3 * recipients.len() as u32);
                write_recipients(&mut writer, recipients)?;
            }
        }
        writer.write_sb4(enq_options.visibility as i32);
        writer.write_ub4(0); // relative msg id
        writer.write_sb4(0); // seq deviation
        if matches!(queue.kind, AqPayloadKind::Raw) {
            let raw_len = match payload {
                AqPayloadValue::Raw(bytes) => bytes.len() as u32,
                _ => return Err(ProtocolError::TtcDecode("RAW queue requires RAW payload")),
            };
            writer.write_ub4(raw_len);
        }
        write_payload(&mut writer, payload, supports_oson_long_fnames)?;
    }
    writer.write_u8(TNS_MSG_TYPE_STATUS);
    Ok(writer.into_bytes())
}

/// Builds the AQ array dequeue (FUNC 145, op=DEQ) request payload (reference
/// `AqArrayMessage._write_message` + `_write_array_deq`).
pub fn build_aq_array_deq_payload(
    queue: &AqQueueDesc,
    deq_options: &AqDeqOptions,
    num_iters: u32,
    seq_num: u8,
    ttc_field_version: u8,
) -> Result<Vec<u8>> {
    let queue_name = queue.name.as_bytes();
    let mut writer = TtcWriter::new();
    write_aq_function_code(&mut writer, TNS_FUNC_AQ_ARRAY, seq_num, ttc_field_version);
    writer.write_u8(1); // input params (pointer)
    writer.write_ub4(num_iters);
    writer.write_ub4(TNS_AQ_ARRAY_FLAGS_RETURN_MESSAGE_ID);
    writer.write_u8(1); // output params (pointer)
    writer.write_u8(1); // length
    writer.write_sb4(TNS_AQ_ARRAY_DEQ);
    writer.write_u8(0); // num iters (pointer)
    if ttc_field_version >= TNS_CCAP_FIELD_VERSION_21_1 {
        writer.write_ub4(0xFFFF); // shard id
    }

    let mut flags = 0u32;
    match deq_options.delivery_mode {
        TNS_AQ_MSG_BUFFERED => flags |= TNS_KPD_AQ_BUFMSG,
        TNS_AQ_MSG_PERSISTENT_OR_BUFFERED => flags |= TNS_KPD_AQ_EITHER,
        _ => {}
    }
    let consumer_name = deq_options
        .consumer_name
        .as_ref()
        .filter(|name| !name.is_empty())
        .map(|name| name.as_bytes());
    let correlation = deq_options
        .correlation
        .as_ref()
        .filter(|c| !c.is_empty())
        .map(|c| c.as_bytes());
    let condition = deq_options
        .condition
        .as_ref()
        .filter(|c| !c.is_empty())
        .map(|c| c.as_bytes());
    let props = AqMsgProps::default();
    for _ in 0..num_iters {
        writer.write_bytes_with_two_lengths(Some(queue_name))?;
        write_msg_props(&mut writer, &props, ttc_field_version)?;
        writer.write_ub4(0); // num recipients
        write_value_with_length(&mut writer, consumer_name)?;
        writer.write_sb4(deq_options.mode);
        writer.write_sb4(deq_options.navigation);
        writer.write_sb4(deq_options.visibility);
        writer.write_sb4(deq_options.wait as i32);
        write_value_with_length(&mut writer, deq_options.msgid.as_deref())?;
        write_value_with_length(&mut writer, correlation)?;
        write_value_with_length(&mut writer, condition)?;
        writer.write_ub4(0); // extensions
        writer.write_ub4(0); // rel msg id
        writer.write_sb4(0); // seq deviation
        writer.write_bytes_with_two_lengths(Some(&queue.payload_toid))?;
        writer.write_ub2(TNS_AQ_MESSAGE_VERSION);
        writer.write_ub4(0); // payload length
        writer.write_ub4(0); // raw pay length
        writer.write_ub4(0);
        writer.write_ub4(flags);
        writer.write_ub4(0); // extensions len
        writer.write_ub4(0); // source seq len
    }
    Ok(writer.into_bytes())
}

/// Result of a bulk operation: enqueue returns assigned msgids per message;
/// dequeue returns the dequeued messages (already truncated to `num_iters`).
#[derive(Clone, Debug, Default)]
pub struct AqArrayResult {
    /// For enqueue: assigned msgid per input message, in order.
    pub enq_msgids: Vec<Vec<u8>>,
    /// For dequeue: the dequeued messages.
    pub deq_messages: Vec<AqDeqMessage>,
}

/// Parses an AQ array (FUNC 145) response for either operation (reference
/// `AqArrayMessage._process_return_parameters`).
///
/// `props_count` is the number of message-property slots prepared client-side
/// (`num_iters` for enqueue, `max_num_messages` for dequeue).
pub fn parse_aq_array_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
    operation: i32,
    props_count: u32,
    kind: &AqPayloadKind,
) -> Result<AqArrayResult> {
    let mut reader = TtcReader::new(payload);
    let mut result = AqArrayResult::default();
    let mut messages: Vec<AqDeqMessage> = Vec::new();
    let mut enq_msgid_blob: Option<Vec<u8>> = None;
    let mut response_num_iters: u32 = 0;
    let mut no_msg_found = false;
    while reader.remaining() > 0 {
        let message_type = reader.read_u8()?;
        match message_type {
            0 => {}
            TNS_MSG_TYPE_PARAMETER => {
                let num_iters = reader.read_ub4()?;
                response_num_iters = num_iters;
                for i in 0..num_iters {
                    let mut message = AqDeqMessage::default();
                    let props_len = reader.read_ub2()?;
                    if props_len > 0 {
                        reader.read_u8()?; // skip_ub1
                        process_msg_props(
                            &mut reader,
                            &mut message,
                            capabilities.ttc_field_version,
                        )?;
                    }
                    process_recipients(&mut reader)?;
                    let payload_len = reader.read_ub2()?;
                    if payload_len > 0 {
                        message.payload = process_payload(&mut reader, kind)?;
                    }
                    let msgid = reader.read_bytes_with_length()?.unwrap_or_default();
                    if operation == TNS_AQ_ARRAY_ENQ {
                        enq_msgid_blob = Some(msgid);
                    } else {
                        message.msgid = Some(msgid);
                    }
                    let ext_len = reader.read_ub2()?;
                    if ext_len > 0 {
                        return Err(ProtocolError::UnsupportedFeature("AQ array extensions"));
                    }
                    let _output_ack = reader.read_ub2()?;
                    if operation != TNS_AQ_ARRAY_ENQ {
                        let _ = i;
                        messages.push(message);
                    }
                }
                if operation == TNS_AQ_ARRAY_ENQ {
                    response_num_iters = reader.read_ub4()?;
                }
            }
            TNS_MSG_TYPE_STATUS => {
                let _call_status = reader.read_ub4()?;
                let _seq = reader.read_ub2()?;
            }
            TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK => {
                let _ = skip_server_side_piggyback(&mut reader)?;
            }
            TNS_MSG_TYPE_END_OF_RESPONSE => break,
            TNS_MSG_TYPE_ERROR => {
                let info = parse_server_error_info(&mut reader, capabilities.ttc_field_version)?;
                if info.number == TNS_ERR_NO_MESSAGES_FOUND as u32 {
                    no_msg_found = true;
                } else if info.number != 0 {
                    return Err(ProtocolError::ServerErrorInfo(Box::new(
                        info.into_details(),
                    )));
                }
            }
            _ => {
                return Err(ProtocolError::UnknownMessageType {
                    message_type,
                    position: reader.position().saturating_sub(1),
                })
            }
        }
    }
    if operation == TNS_AQ_ARRAY_ENQ {
        if let Some(blob) = enq_msgid_blob {
            let count = props_count as usize;
            result.enq_msgids = (0..count)
                .map(|j| {
                    let start = j * 16;
                    let end = start + 16;
                    blob.get(start..end).map(<[u8]>::to_vec).unwrap_or_default()
                })
                .collect();
        }
    } else if no_msg_found {
        result.deq_messages = Vec::new();
    } else {
        let keep = response_num_iters as usize;
        messages.truncate(keep);
        result.deq_messages = messages;
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Shared response decoders (reference aq_base.pyx).
// ---------------------------------------------------------------------------

fn process_msg_props(
    reader: &mut TtcReader<'_>,
    message: &mut AqDeqMessage,
    ttc_field_version: u8,
) -> Result<()> {
    message.priority = reader.read_sb4()?;
    message.delay = reader.read_sb4()?;
    message.expiration = reader.read_sb4()?;
    message.correlation = reader.read_string_with_length()?;
    message.num_attempts = reader.read_sb4()?;
    message.exception_queue = reader.read_string_with_length()?;
    message.state = reader.read_sb4()?;
    message.enq_time = process_date(reader)?;
    let _enq_txn_id = reader.read_bytes_with_length()?;
    process_extensions(reader)?;
    let user_props = reader.read_ub4()?;
    if user_props > 0 {
        return Err(ProtocolError::UnsupportedFeature("AQ user properties"));
    }
    let _csn = reader.read_ub4()?;
    let _dsn = reader.read_ub4()?;
    let flags = reader.read_ub4()?;
    message.delivery_mode = if flags == TNS_KPD_AQ_BUFMSG {
        TNS_AQ_MSG_BUFFERED
    } else {
        TNS_AQ_MSG_PERSISTENT
    };
    if ttc_field_version >= TNS_CCAP_FIELD_VERSION_21_1 {
        let _shard = reader.read_ub4()?;
    }
    Ok(())
}

/// Reads an Oracle date the way the reference `_process_date` does: a ub4
/// presence flag, then `read_raw_bytes_and_length` (a single u8 length byte
/// followed by that many raw date bytes).
fn process_date(reader: &mut TtcReader<'_>) -> Result<Option<QueryValue>> {
    let num_bytes = reader.read_ub4()?;
    if num_bytes == 0 {
        return Ok(None);
    }
    let len = usize::from(reader.read_u8()?);
    if len == 0 {
        return Ok(None);
    }
    let bytes = reader.read_raw(len)?;
    Ok(Some(decode_datetime_value(bytes)?))
}

fn process_extensions(reader: &mut TtcReader<'_>) -> Result<()> {
    let num_extensions = reader.read_ub4()?;
    if num_extensions > 0 {
        reader.read_u8()?; // skip_ub1
        for _ in 0..num_extensions {
            let _text = reader.read_bytes_with_length()?;
            let _binary = reader.read_bytes_with_length()?;
            let _keyword = reader.read_ub2()?;
        }
    }
    Ok(())
}

fn process_recipients(reader: &mut TtcReader<'_>) -> Result<()> {
    let count = reader.read_ub4()?;
    if count > 0 {
        return Err(ProtocolError::UnsupportedFeature(
            "AQ recipients on dequeue",
        ));
    }
    Ok(())
}

fn process_msg_id(reader: &mut TtcReader<'_>) -> Result<Vec<u8>> {
    Ok(reader.read_raw(TNS_AQ_MESSAGE_ID_LENGTH)?.to_vec())
}

/// Decodes the payload of a dequeued message (reference `_process_payload`).
fn process_payload(
    reader: &mut TtcReader<'_>,
    kind: &AqPayloadKind,
) -> Result<Option<AqDeqPayload>> {
    if matches!(kind, AqPayloadKind::Object) {
        // Object branch (reference `read_dbobject`): TOID/OID/snapshot
        // (length-prefixed each), version ub2, image-len ub4, flags ub2, then
        // the packed image as a bare length-prefixed chunk (`read_bytes`).
        let _toid = reader.read_bytes_with_length()?;
        let _oid = reader.read_bytes_with_length()?;
        let _snapshot = reader.read_bytes_with_length()?;
        let _version = reader.read_ub2()?;
        let image_length = reader.read_ub4()?;
        let _flags = reader.read_ub2()?;
        if image_length == 0 {
            return Ok(None);
        }
        let image = reader
            .read_bytes()?
            .ok_or(ProtocolError::TtcDecode("AQ object payload missing"))?;
        return Ok(Some(AqDeqPayload::Object(image)));
    }
    // RAW / JSON branch.
    let _toid = reader.read_bytes_with_length()?;
    let _oid = reader.read_bytes_with_length()?;
    let _snapshot = reader.read_bytes_with_length()?;
    let _version = reader.read_ub2()?;
    let image_length = reader.read_ub4()? as usize;
    let _flags = reader.read_ub2()?;
    if image_length > 0 {
        // reference: payload = read_bytes()[4:image_length]
        let raw = reader
            .read_bytes()?
            .ok_or(ProtocolError::TtcDecode("AQ payload missing"))?;
        let end = image_length.min(raw.len());
        let start = 4.min(end);
        let payload = raw.get(start..end).unwrap_or_default().to_vec();
        if matches!(kind, AqPayloadKind::Json) {
            let value = decode_oson(&payload)?;
            return Ok(Some(AqDeqPayload::Json(value)));
        }
        return Ok(Some(AqDeqPayload::Raw(payload)));
    }
    if matches!(kind, AqPayloadKind::Raw) {
        return Ok(Some(AqDeqPayload::Raw(Vec::new())));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Field version negotiated against the 23ai container (lane-1523). >= EXT_1
    // (18) so token_num is emitted; >= 21_1 (16) so shard id is emitted.
    const FV: u8 = 24;

    fn caps() -> ClientCapabilities {
        ClientCapabilities {
            ttc_field_version: FV,
            max_string_size: 32767,
            charset_id: 873,
        }
    }

    // Golden RAW enqueue request (FUNC 121), captured from python-oracledb 4.0.1
    // against lane-1523: msgproperties(payload=b"sample raw data 1",
    // correlation="CORR1", priority=2); enqone. TTC payload (offset 10 past the
    // packet header), seq_num=4. See tests/golden/aq_raw.txt.
    const GOLDEN_RAW_ENQ: &[u8] = &[
        0x03, 0x79, 0x04, 0x00, 0x01, 0x01, 0x0e, 0x01, 0x02, 0x00, 0x81, 0x01, 0x01, 0x05, 0x05,
        0x43, 0x4f, 0x52, 0x52, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x04, 0x0e, 0x00, 0x00,
        0x01, 0x40, 0x00, 0x00, 0x01, 0x41, 0x00, 0x01, 0x01, 0x01, 0x00, 0x01, 0x42, 0x00, 0x00,
        0x01, 0x45, 0x00, 0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff, 0xff, 0x01, 0x00, 0x01, 0x02,
        0x00, 0x00, 0x00, 0x01, 0x01, 0x10, 0x01, 0x01, 0x00, 0x01, 0x01, 0x11, 0x01, 0x01, 0x10,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x0e, 0x54, 0x45, 0x53, 0x54, 0x5f, 0x52, 0x41, 0x57, 0x5f, 0x51,
        0x55, 0x45, 0x55, 0x45, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x17, 0x73, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x20, 0x72, 0x61, 0x77,
        0x20, 0x64, 0x61, 0x74, 0x61, 0x20, 0x31,
    ];

    // Golden RAW dequeue request (FUNC 122): deqoptions wait=NO_WAIT,
    // navigation=DEQ_FIRST_MSG; deqone. seq_num=6.
    const GOLDEN_RAW_DEQ: &[u8] = &[
        0x03, 0x7a, 0x06, 0x00, 0x01, 0x01, 0x0e, 0x01, 0x01, 0x01, 0x01, 0x00, 0x00, 0x01, 0x03,
        0x01, 0x01, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x10, 0x01, 0x01, 0x01,
        0x01, 0x01, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff, 0xff, 0x0e,
        0x54, 0x45, 0x53, 0x54, 0x5f, 0x52, 0x41, 0x57, 0x5f, 0x51, 0x55, 0x45, 0x55, 0x45, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17,
    ];

    #[test]
    fn raw_enqueue_request_matches_golden() {
        let queue = AqQueueDesc::new("TEST_RAW_QUEUE".to_string(), AqPayloadKind::Raw, None);
        let props = AqMsgProps {
            priority: 2,
            correlation: Some("CORR1".to_string()),
            payload: Some(AqPayloadValue::Raw(b"sample raw data 1".to_vec())),
            ..AqMsgProps::default()
        };
        let bytes = build_aq_enq_payload(&queue, &props, &AqEnqOptions::default(), 4, FV, false)
            .expect("build enqueue");
        assert_eq!(bytes, GOLDEN_RAW_ENQ);
    }

    #[test]
    fn raw_dequeue_request_matches_golden() {
        let queue = AqQueueDesc::new("TEST_RAW_QUEUE".to_string(), AqPayloadKind::Raw, None);
        let deq = AqDeqOptions {
            wait: 0,
            navigation: 1,
            ..AqDeqOptions::default()
        };
        let bytes = build_aq_deq_payload(&queue, &deq, 6, FV).expect("build dequeue");
        assert_eq!(bytes, GOLDEN_RAW_DEQ);
    }

    #[test]
    fn empty_queue_dequeue_yields_no_message() {
        // ORA-25228 is cleared and surfaces as no message. We can't synthesize a
        // full error packet trivially here, so just confirm an empty response
        // (status-only) yields None without error.
        let caps = caps();
        let res = parse_aq_deq_response(&[], caps, &AqPayloadKind::Raw).expect("parse");
        assert!(res.message.is_none());
    }

    // Golden JSON enqueue (FUNC 121): msgproperties(payload=dict(name="John",
    // age=30, city="NY")); enqone against TEST_JSON_QUEUE, seq_num=4. The OSON
    // image (after `ff 4a 5a 01`) also exercises encode_oson byte-parity.
    const GOLDEN_JSON_ENQ: &[u8] = &[
        0x03, 0x79, 0x04, 0x00, 0x01, 0x01, 0x0f, 0x00, 0x00, 0x81, 0x01, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x01, 0x04, 0x0e, 0x00, 0x00, 0x01, 0x40, 0x00, 0x00, 0x01, 0x41, 0x00, 0x01,
        0x01, 0x01, 0x00, 0x01, 0x42, 0x00, 0x00, 0x01, 0x45, 0x00, 0x00, 0x00, 0x00, 0x04, 0xff,
        0xff, 0xff, 0xff, 0x01, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x01, 0x01, 0x10, 0x01, 0x01,
        0x00, 0x00, 0x00, 0x01, 0x01, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x0f, 0x54, 0x45, 0x53, 0x54,
        0x5f, 0x4a, 0x53, 0x4f, 0x4e, 0x5f, 0x51, 0x55, 0x45, 0x55, 0x45, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x47, 0x01, 0x28, 0x00,
        0x26, 0x00, 0x04, 0x61, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x43, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x43, 0xff, 0x4a, 0x5a, 0x01, 0x21,
        0x02, 0x03, 0x00, 0x0e, 0x00, 0x1f, 0x00, 0x00, 0x42, 0x9c, 0xe6, 0x00, 0x09, 0x00, 0x05,
        0x00, 0x00, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x03, 0x61, 0x67, 0x65, 0x04, 0x63, 0x69, 0x74,
        0x79, 0xa4, 0x03, 0x03, 0x02, 0x01, 0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x17, 0x00,
        0x00, 0x00, 0x1b, 0x33, 0x04, 0x4a, 0x6f, 0x68, 0x6e, 0x34, 0x02, 0xc1, 0x1f, 0x33, 0x02,
        0x4e, 0x59,
    ];

    #[test]
    fn json_enqueue_request_matches_golden() {
        let queue = AqQueueDesc::new("TEST_JSON_QUEUE".to_string(), AqPayloadKind::Json, None);
        // Insertion-ordered object {name, age, city}.
        let value = OsonValue::Object(vec![
            ("name".to_string(), OsonValue::String("John".to_string())),
            ("age".to_string(), OsonValue::Number("30".to_string())),
            ("city".to_string(), OsonValue::String("NY".to_string())),
        ]);
        let props = AqMsgProps {
            payload: Some(AqPayloadValue::Json(value)),
            ..AqMsgProps::default()
        };
        // Container negotiates >= 23ai so supports_oson_long_fnames = true.
        let bytes = build_aq_enq_payload(&queue, &props, &AqEnqOptions::default(), 4, FV, true)
            .expect("build json enqueue");
        assert_eq!(bytes, GOLDEN_JSON_ENQ);
    }
}
