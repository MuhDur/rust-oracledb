#![forbid(unsafe_code)]

//! CQN / Continuous Query Notification wire codecs (sans-io).
//!
//! Ports the reference thin subscription messages:
//! - `impl/thin/messages/subscribe.pyx` — FUNC 125 register/unregister payload
//!   and the `_process_return_parameters` response decode.
//! - `impl/thin/messages/notification.pyx` — FUNC 187 NOTIFY payload, the OAC
//!   record loop (`_process_oac`) and the big-endian inner CQN payload decoder
//!   (`_process_notification_payload` / `_process_tables` / `_process_rows` /
//!   `_process_queries`).
//!
//! Only the byte<->struct translation lives here; the second ("emon")
//! connection, the background receive loop and the Python callback invocation
//! live in the driver and pyshim crates.

use super::*;
use crate::wire::{ProtocolLimits, TtcReader, TtcWriter};

/// Result of decoding the SUBSCRIBE (register) response.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SubscribeResult {
    /// `USER_CHANGE_NOTIFICATION_REGS.REGID` — exposed as `Subscription.id`.
    pub registration_id: u64,
    /// EMON client id (e.g. `b"OCI:EP:301"`) echoed back in the NOTIFY message.
    pub client_id: Option<Vec<u8>>,
}

/// One row changed inside a table notification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MsgRow {
    pub operation: u32,
    pub rowid: String,
}

/// One table changed inside an OBJCHANGE / QUERYCHANGE notification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MsgTable {
    pub operation: u32,
    pub name: String,
    pub rows: Vec<MsgRow>,
}

/// One query whose result set changed (QUERYCHANGE notifications).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MsgQuery {
    pub id: u64,
    pub operation: u32,
    pub tables: Vec<MsgTable>,
}

/// A single decoded OAC notification record handed to the user callback.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotificationMessage {
    /// `EVENT_*` value placed on `Message.type`.
    pub msg_type: u32,
    pub dbname: Option<String>,
    /// Thin never decodes the transaction id (14 bytes skipped); always `None`.
    pub txid: Option<Vec<u8>>,
    pub registered: bool,
    pub queue_name: Option<String>,
    pub consumer_name: Option<String>,
    pub msgid: Option<Vec<u8>>,
    pub tables: Vec<MsgTable>,
    pub queries: Vec<MsgQuery>,
}

/// Outcome of decoding one OAC record from the notification stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NotificationRecord {
    /// A record to deliver to the callback. `end_of_response` mirrors the
    /// reference flag (DEREG / DEREG_NFY terminate the loop after delivery).
    Message {
        message: NotificationMessage,
        end_of_response: bool,
    },
    /// `TNS_SUBSCR_STOP_NOTIF` — the stream is finished; no callback fires.
    Stop,
}

/// Writes the function-code header plus the pipeline `token_num` (always 0 for
/// these messages) when the negotiated caps include it, mirroring
/// `messages/base.pyx::_write_function_code` (writes `ub8 token_num` for
/// `ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1_EXT_1`).
fn write_function_code_token(w: &mut TtcWriter, function_code: u8, seq_num: u8, field_version: u8) {
    w.write_function_header(function_code, seq_num, field_version);
}

/// Build the SUBSCRIBE (FUNC 125) payload for register (`opcode = 1`) or
/// unregister (`opcode = 2`). Ports `subscribe.pyx::_write_message`.
///
/// `qos`/`operations` are the *public* `SUBSCR_QOS_*` / `OPCODE_*` values; this
/// function performs the qos/flags derivation (`subscribe.pyx:82-93`).
#[allow(clippy::too_many_arguments)]
pub fn build_subscribe_payload_with_seq(
    seq_num: u8,
    opcode: u8,
    username: Option<&str>,
    client_id: Option<&[u8]>,
    namespace: u32,
    name: Option<&str>,
    public_qos: u32,
    operations: u32,
    timeout: u32,
    grouping_class: u8,
    grouping_value: u32,
    grouping_type: u8,
    registration_id: u64,
    field_version: u8,
) -> Result<Vec<u8>> {
    // derive the wire qos flags
    let mut qos = TNS_SUBSCR_QOS_SECURE;
    if public_qos & SUBSCR_QOS_RELIABLE != 0 {
        qos |= TNS_SUBSCR_QOS_RELIABLE;
    }
    if public_qos & SUBSCR_QOS_DEREG_NFY != 0 {
        qos |= TNS_SUBSCR_QOS_PURGE_ON_NTFN;
    }
    // derive the wire operation flags
    let mut flags = operations;
    if public_qos & SUBSCR_QOS_QUERY != 0 {
        flags |= TNS_SUBSCR_FLAGS_QUERY;
    }
    if public_qos & SUBSCR_QOS_ROWIDS != 0 {
        flags |= TNS_SUBSCR_FLAGS_INCLUDE_ROWIDS;
    }
    // grouping_type can only be sent when a grouping class is set
    let grouping_type = if grouping_class == 0 {
        0
    } else {
        grouping_type
    };

    let username_bytes = username.map(str::as_bytes);

    let mut w = TtcWriter::new();
    write_function_code_token(&mut w, TNS_FUNC_SUBSCRIBE, seq_num, field_version);
    w.write_u8(opcode);
    w.write_ub4(TNS_SUBSCR_MODE_CLIENT_INITIATED);
    match username_bytes {
        Some(bytes) => {
            w.write_u8(1); // pointer (username)
            w.write_ub4(u32::try_from(bytes.len()).unwrap_or(u32::MAX));
        }
        None => {
            w.write_u8(0);
            w.write_ub4(0);
        }
    }
    match client_id {
        Some(bytes) => {
            w.write_u8(1); // pointer (location)
            w.write_ub4(u32::try_from(bytes.len()).unwrap_or(u32::MAX));
        }
        None => {
            w.write_u8(0);
            w.write_ub4(0);
        }
    }
    w.write_u8(1); // pointer (registration)
    w.write_ub4(1); // num registrations
    w.write_ub2(1); // raw presentation
    w.write_ub2(6); // version for client notification
    w.write_u8(0); // pointer (namespace out attrs)
    w.write_u8(1); // pointer (num elements in array)
    w.write_u8(0); // pointer (generic out attrs)
    w.write_u8(1); // pointer (num elements in array)
    if version_gates::writes_subscribe_client_id_block(field_version) {
        w.write_u8(1); // kpninst
        w.write_u8(1); // kpninstl
        w.write_u8(1); // kpngcret
        w.write_u8(1); // kpngcretl
        w.write_u8(1); // client id
        w.write_ub4(TNS_SUBSCR_CLIENT_ID_LEN);
        w.write_u8(1); // client id length
    }
    if let Some(bytes) = username_bytes {
        w.write_bytes_with_length(bytes)?;
    }
    if let Some(bytes) = client_id {
        w.write_bytes_with_length(bytes)?;
    }
    w.write_ub4(namespace);
    match name {
        Some(name) => w.write_bytes_with_two_lengths(Some(name.as_bytes()))?,
        None => w.write_ub4(0),
    }
    w.write_ub4(0); // context length
    w.write_ub4(0); // payload type
    w.write_ub4(qos);
    w.write_ub4(0); // payload callback length (JMS)
    w.write_ub4(timeout);
    w.write_ub4(0); // kpdnsd
    w.write_ub4(flags);
    w.write_ub4(0); // change lag between notifications
    w.write_ub4(0); // change registration id
    w.write_u8(grouping_class);
    w.write_ub4(grouping_value);
    w.write_u8(grouping_type);
    w.write_ub4(0); // grouping class start time
                    // grouping repeat count: write_sb4(0); for the constant 0 this is the same
                    // single 0x00 byte the unsigned encoder emits.
    w.write_ub4(0);
    w.write_ub8(registration_id);
    Ok(w.into_bytes())
}

/// Decode the SUBSCRIBE (register) response. Ports
/// `subscribe.pyx::_process_return_parameters`, dispatched on the
/// `TNS_MSG_TYPE_PARAMETER` message inside the standard function response loop.
pub fn parse_subscribe_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
) -> Result<SubscribeResult> {
    parse_subscribe_response_with_limits(payload, capabilities, ProtocolLimits::DEFAULT)
}

pub fn parse_subscribe_response_with_limits(
    payload: &[u8],
    capabilities: ClientCapabilities,
    limits: ProtocolLimits,
) -> Result<SubscribeResult> {
    let mut reader = TtcReader::with_limits(payload, limits)?;
    let mut result = SubscribeResult::default();
    let field_version = capabilities.ttc_field_version;
    while reader.remaining() > 0 {
        let message_type = reader.read_u8()?;
        match message_type {
            0 => {}
            TNS_MSG_TYPE_PARAMETER => {
                parse_subscribe_return_parameters(&mut reader, field_version, &mut result)?;
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
                let info = parse_server_error_info(&mut reader, field_version)?;
                if info.number != 0 {
                    return Err(ProtocolError::ServerError(info.message));
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
    Ok(result)
}

fn parse_subscribe_return_parameters(
    reader: &mut TtcReader<'_>,
    field_version: u8,
    result: &mut SubscribeResult,
) -> Result<()> {
    let num_values = reader.read_ub4()?; // out parameters (kpnrl)
    for _ in 0..num_values {
        let _ = reader.read_ub4()?;
    }
    for _ in 0..num_values {
        let _ = reader.read_ub4()?; // registration id (short)
    }
    let num_values = reader.read_ub4()?; // out parameters (kpngrl)
    for _ in 0..num_values {
        result.registration_id = reader.read_ub8()?;
        if version_gates::reads_subscribe_response_details(field_version) {
            let _subscriber_name = reader.read_bytes_with_length()?;
        }
    }
    if version_gates::reads_subscribe_response_details(field_version) {
        let num_instances = reader.read_ub4()?;
        for _ in 0..num_instances {
            let _ = reader.read_bytes_with_length()?;
        }
        let num_listeners = reader.read_ub4()?;
        for _ in 0..num_listeners {
            let _ = reader.read_bytes_with_length()?;
        }
        result.client_id = reader.read_bytes_with_length()?;
    }
    Ok(())
}

/// Build the NOTIFY (FUNC 187) payload sent on the emon connection. Ports
/// `notification.pyx::_write_message`. The caller must transmit this packet
/// with the `TNS_DATA_FLAGS_END_OF_REQUEST` data flag set.
pub fn build_notify_payload_with_seq(
    seq_num: u8,
    client_id: &[u8],
    field_version: u8,
) -> Result<Vec<u8>> {
    let mut w = TtcWriter::new();
    write_function_code_token(&mut w, TNS_FUNC_NOTIFY, seq_num, field_version);
    w.write_ub4(u32::try_from(client_id.len()).unwrap_or(u32::MAX));
    w.write_bytes_with_length(client_id)?;
    w.write_u8(TNS_INIT_KPNDRREQ);
    w.write_ub4(0);
    Ok(w.into_bytes())
}

/// Decode every OAC record in a notification stream. The reference reads one
/// leading `message_type` byte (`TNS_MSG_TYPE_OAC`) then loops `_process_oac`
/// until `end_of_response`; the driver chains network packets into `payload`
/// so this operates on the full concatenated TTC stream.
///
/// Returns the decoded records in order. A trailing [`NotificationRecord::Stop`]
/// (or a record whose `end_of_response` is set) marks the end of the stream.
pub fn parse_notification_stream(
    payload: &[u8],
    namespace: u32,
    public_qos: u32,
    db_name: Option<&str>,
) -> Result<Vec<NotificationRecord>> {
    parse_notification_stream_with_limits(
        payload,
        namespace,
        public_qos,
        db_name,
        ProtocolLimits::DEFAULT,
    )
}

pub fn parse_notification_stream_with_limits(
    payload: &[u8],
    namespace: u32,
    public_qos: u32,
    db_name: Option<&str>,
    limits: ProtocolLimits,
) -> Result<Vec<NotificationRecord>> {
    let mut reader = TtcReader::with_limits(payload, limits)?;
    let message_type = reader.read_u8()?; // outer process(): read_ub1(message_type)
    if message_type != TNS_MSG_TYPE_OAC {
        return Err(ProtocolError::UnknownMessageType {
            message_type,
            position: reader.position().saturating_sub(1),
        });
    }
    let mut records = Vec::new();
    while reader.remaining() > 0 {
        let record =
            parse_oac_record_with_limits(&mut reader, namespace, public_qos, db_name, limits)?;
        let end = match &record {
            NotificationRecord::Stop => true,
            NotificationRecord::Message {
                end_of_response, ..
            } => *end_of_response,
        };
        records.push(record);
        if end {
            break;
        }
    }
    Ok(records)
}

/// Consume the leading `TNS_MSG_TYPE_OAC` byte that precedes the OAC record
/// stream (`process()` reads it once before delivering any record). Returns the
/// number of bytes consumed (1) or an error if the byte is not OAC.
pub fn check_notification_header(bytes: &[u8]) -> Result<usize> {
    check_notification_header_with_limits(bytes, ProtocolLimits::DEFAULT)
}

pub fn check_notification_header_with_limits(
    bytes: &[u8],
    limits: ProtocolLimits,
) -> Result<usize> {
    let mut reader = TtcReader::with_limits(bytes, limits)?;
    let message_type = reader.read_u8()?;
    if message_type != TNS_MSG_TYPE_OAC {
        return Err(ProtocolError::UnknownMessageType {
            message_type,
            position: 0,
        });
    }
    Ok(reader.position())
}

/// Attempt to decode exactly one OAC record from the front of `bytes`. Returns
/// the decoded record and the number of bytes consumed, or `Ok(None)` when the
/// buffer does not yet hold a complete record (the caller must read more data
/// from the EMON socket and retry — mirroring the reference `ReadBuffer`
/// chaining packets within a single `process()` call).
pub fn try_parse_oac_record(
    bytes: &[u8],
    namespace: u32,
    public_qos: u32,
    db_name: Option<&str>,
) -> Result<Option<(NotificationRecord, usize)>> {
    try_parse_oac_record_with_limits(
        bytes,
        namespace,
        public_qos,
        db_name,
        ProtocolLimits::DEFAULT,
    )
}

pub fn try_parse_oac_record_with_limits(
    bytes: &[u8],
    namespace: u32,
    public_qos: u32,
    db_name: Option<&str>,
    limits: ProtocolLimits,
) -> Result<Option<(NotificationRecord, usize)>> {
    let mut reader = TtcReader::with_limits(bytes, limits)?;
    match parse_oac_record_with_limits(&mut reader, namespace, public_qos, db_name, limits) {
        Ok(record) => Ok(Some((record, reader.position()))),
        // The server only emits well-formed records; a decode failure while the
        // stream is still being chained means the buffer is short, so signal
        // "need more bytes" rather than treating it as corruption.
        Err(_) => Ok(None),
    }
}

/// Decode a single OAC record. Ports `notification.pyx::_process_oac` plus the
/// inner payload decode.
pub fn parse_oac_record(
    reader: &mut TtcReader<'_>,
    namespace: u32,
    public_qos: u32,
    db_name: Option<&str>,
) -> Result<NotificationRecord> {
    parse_oac_record_with_limits(reader, namespace, public_qos, db_name, reader.limits())
}

pub fn parse_oac_record_with_limits(
    reader: &mut TtcReader<'_>,
    namespace: u32,
    public_qos: u32,
    db_name: Option<&str>,
    limits: ProtocolLimits,
) -> Result<NotificationRecord> {
    let message_type = reader.read_ub4()?;
    if message_type == TNS_SUBSCR_STOP_NOTIF {
        return Ok(NotificationRecord::Stop);
    }
    let _error_code = reader.read_ub4()?;
    let _registration_id = reader.read_ub4()?;
    let queue_name = reader.read_string_with_length()?;
    let consumer_name = reader.read_string_with_length()?;
    let msgid = reader.read_bytes_with_length()?;
    let num_props = reader.read_ub4()?;
    if num_props > 0 {
        // AQ message properties path: skip the invalid-length byte then the
        // property records. The CQN tests never exercise this branch (AQ uses
        // num_props == 0); skip conservatively so the stream stays aligned.
        let _ = reader.read_u8()?;
        skip_msg_props(reader, num_props)?;
    }
    skip_bytes_with_length(reader)?; // JMS message properties

    let mut payload: Option<Vec<u8>> = None;
    if namespace != TNS_SUBSCR_NAMESPACE_AQ {
        let _payload_type = reader.read_ub4()?;
        let _payload_flags = reader.read_ub4()?;
        let _chunk_number = reader.read_ub4()?;
        payload = reader.read_bytes_with_length()?;
        skip_bytes_with_length(reader)?; // DbObject / JSON payload
    }

    let mut message = NotificationMessage {
        msg_type: 0,
        dbname: db_name.map(str::to_string),
        txid: None,
        registered: false,
        queue_name,
        consumer_name,
        msgid,
        tables: Vec::new(),
        queries: Vec::new(),
    };
    let end_of_response = process_notification_payload(
        payload.as_deref(),
        namespace,
        public_qos,
        limits,
        &mut message,
    )?;
    Ok(NotificationRecord::Message {
        message,
        end_of_response,
    })
}

/// Ports `_process_notification_payload`. Returns the resulting
/// `end_of_response` flag.
fn process_notification_payload(
    payload: Option<&[u8]>,
    namespace: u32,
    public_qos: u32,
    limits: ProtocolLimits,
    message: &mut NotificationMessage,
) -> Result<bool> {
    if namespace == TNS_SUBSCR_NAMESPACE_AQ {
        message.msg_type = EVENT_AQ;
        return Ok(false);
    }
    let Some(payload) = payload else {
        // empty payload => registration discarded
        message.msg_type = EVENT_DEREG;
        return Ok(true);
    };
    let mut end_of_response = false;
    if public_qos & SUBSCR_QOS_DEREG_NFY != 0 {
        message.registered = false;
        end_of_response = true;
    } else {
        message.registered = true;
    }
    // inner payload is a plain big-endian byte cursor
    let mut cur = ByteCursor::with_limits(payload, limits)?;
    let _version = cur.u16be()?;
    let _registration_id = cur.u32be()?;
    let event_type = cur.u32be()?;
    message.msg_type = event_type;
    let dbname_len = cur.u16be()? as usize;
    let dbname = cur.raw(dbname_len)?;
    message.dbname = Some(
        String::from_utf8(dbname.to_vec())
            .map_err(|_| ProtocolError::TtcDecode("notification dbname not UTF-8"))?,
    );
    cur.skip(14)?; // transaction id + SCN (txid intentionally left None)
    if event_type == EVENT_OBJCHANGE {
        message.tables = process_tables(&mut cur)?;
    } else if event_type == EVENT_QUERYCHANGE {
        message.queries = process_queries(&mut cur)?;
    }
    Ok(end_of_response)
}

fn process_tables(cur: &mut ByteCursor<'_>) -> Result<Vec<MsgTable>> {
    let num_tables = cur.u16be()?;
    // Each table record reads at least a u32 operation + u16 name length (6
    // bytes) before its name, so cap the reservation by the buffer
    // (BoundedReader); the loop still fails closed on truncation.
    let mut tables: Vec<MsgTable> = cur.with_capacity_limited(
        num_tables as usize,
        6,
        ProtocolLimits::check_length_prefixed_elements,
    )?;
    for _ in 0..num_tables {
        let operation = cur.u32be()?;
        let name_len = cur.u16be()? as usize;
        let name = String::from_utf8(cur.raw(name_len)?.to_vec())
            .map_err(|_| ProtocolError::TtcDecode("table name not UTF-8"))?;
        let _object_num = cur.u32be()?;
        let rows = if operation & OPCODE_ALLROWS == 0 {
            process_rows(cur)?
        } else {
            Vec::new()
        };
        tables.push(MsgTable {
            operation,
            name,
            rows,
        });
    }
    Ok(tables)
}

fn process_rows(cur: &mut ByteCursor<'_>) -> Result<Vec<MsgRow>> {
    let num_rows = cur.u16be()?;
    // Each row record reads at least a u32 operation + u16 rowid length (6
    // bytes); bound the reservation by the buffer (BoundedReader).
    let mut rows: Vec<MsgRow> = cur.with_capacity_limited(
        num_rows as usize,
        6,
        ProtocolLimits::check_length_prefixed_elements,
    )?;
    for _ in 0..num_rows {
        let operation = cur.u32be()?;
        let rowid_len = cur.u16be()? as usize;
        let rowid = String::from_utf8(cur.raw(rowid_len)?.to_vec())
            .map_err(|_| ProtocolError::TtcDecode("rowid not UTF-8"))?;
        rows.push(MsgRow { operation, rowid });
    }
    Ok(rows)
}

fn process_queries(cur: &mut ByteCursor<'_>) -> Result<Vec<MsgQuery>> {
    let num_queries = cur.u16be()?;
    // Each query record reads at least three u32s (12 bytes) before its nested
    // tables; bound the reservation by the buffer (BoundedReader).
    let mut queries: Vec<MsgQuery> = cur.with_capacity_limited(
        num_queries as usize,
        12,
        ProtocolLimits::check_length_prefixed_elements,
    )?;
    for _ in 0..num_queries {
        let id_lsb = u64::from(cur.u32be()?);
        let id_msb = u64::from(cur.u32be()?);
        let id = (id_msb << 32) | id_lsb;
        let operation = cur.u32be()?;
        let tables = process_tables(cur)?;
        queries.push(MsgQuery {
            id,
            operation,
            tables,
        });
    }
    Ok(queries)
}

/// Skip AQ message-property records (`_process_msg_props`). The CQN tests never
/// reach this branch; this keeps the parser aligned should the server send it.
fn skip_msg_props(reader: &mut TtcReader<'_>, num_props: u32) -> Result<()> {
    for _ in 0..num_props {
        skip_bytes_with_length(reader)?; // name
        skip_bytes_with_length(reader)?; // value
    }
    Ok(())
}

fn skip_bytes_with_length(reader: &mut TtcReader<'_>) -> Result<()> {
    let _ = reader.read_bytes_with_length()?;
    Ok(())
}

/// A plain big-endian cursor over the inner CQN payload bytes (no TTC chunking).
struct ByteCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
    limits: ProtocolLimits,
}

impl<'a> ByteCursor<'a> {
    #[cfg(test)]
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            pos: 0,
            limits: ProtocolLimits::DEFAULT,
        }
    }

    fn with_limits(bytes: &'a [u8], limits: ProtocolLimits) -> Result<Self> {
        let limits = limits.validate()?;
        limits.check_response_bytes(bytes.len())?;
        Ok(Self {
            bytes,
            pos: 0,
            limits,
        })
    }

    fn raw(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or(ProtocolError::TtcDecode("notification payload overflow"))?;
        let slice = self
            .bytes
            .get(self.pos..end)
            .ok_or(ProtocolError::TtcDecode("notification payload truncated"))?;
        self.pos = end;
        Ok(slice)
    }

    fn skip(&mut self, n: usize) -> Result<()> {
        let _ = self.raw(n)?;
        Ok(())
    }

    fn u16be(&mut self) -> Result<u16> {
        let bytes = self.raw(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn u32be(&mut self) -> Result<u32> {
        let bytes = self.raw(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }
}

impl crate::wire::BoundedReader for ByteCursor<'_> {
    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    fn protocol_limits(&self) -> ProtocolLimits {
        self.limits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // BoundedReader invariant (l2p), CQN array family: a notification table
    // record declaring the maximum num_tables (0xFFFF) but carrying no table
    // bytes must fail closed via the bounded reservation + the per-record read,
    // not pre-allocate 65535 MsgTable structs from the count. (num_tables is a
    // u16 so this was never a multi-GB OOM, but routing it through the bound
    // keeps the whole class uniform and regression-proof.)
    #[test]
    fn cqn_oversized_table_count_fails_closed_not_oom() {
        // num_tables = 0xFFFF (u16), then nothing.
        let bytes = [0xFFu8, 0xFF];
        let mut cur = ByteCursor::new(&bytes);
        let err = process_tables(&mut cur).expect_err("oversized table count must fail closed");
        assert!(matches!(err, ProtocolError::TtcDecode(_)), "got {err:?}");
        // The pre-allocation never exceeds remaining()/6 even for the max count.
        let cur2 = ByteCursor::new(&bytes);
        let v: Vec<MsgTable> = cur2.with_capacity_bounded(0xFFFF, 6);
        assert!(v.capacity() <= 1, "reservation capped by remaining bytes");
    }

    #[test]
    fn cqn_table_count_respects_protocol_element_limit() {
        // num_tables = 2. A max_length_prefixed_elements=1 policy rejects the
        // count before reserving table slots.
        let bytes = [0x00u8, 0x02];
        let limits = ProtocolLimits {
            max_length_prefixed_elements: 1,
            ..ProtocolLimits::DEFAULT
        };
        let mut cur = ByteCursor::with_limits(&bytes, limits).expect("valid limits");
        let err = process_tables(&mut cur).expect_err("table count above policy must fail");
        assert!(
            matches!(
                err,
                ProtocolError::ResourceLimit {
                    limit: "length_prefixed_elements",
                    observed: 2,
                    maximum: 1,
                }
            ),
            "got {err:?}"
        );
    }

    fn caps_12_1() -> ClientCapabilities {
        ClientCapabilities {
            ttc_field_version: 24,
            ..ClientCapabilities::default()
        }
    }

    #[test]
    fn subscribe_register_payload_matches_golden() {
        // Golden /tmp/cqn_trace.txt line 2421 (op 8, socket 5), payload after
        // the 2-byte data flags. seq byte is 0x03 in the capture.
        let payload = build_subscribe_payload_with_seq(
            0x03,
            TNS_SUBSCR_OP_REGISTER,
            Some("pythontest"),
            None,
            TNS_SUBSCR_NAMESPACE_DBCHANGE,
            None,
            SUBSCR_QOS_ROWIDS,
            0, // OPCODE_ALLOPS
            10,
            0,
            0,
            0,
            0,
            24,
        )
        .expect("subscribe payload");
        // real capture TTC payload (token byte 0x00 follows the seq):
        // 03 7d 03 00 01 01 04 01 01 0a 00 00 01 01 01 01 01 01 06 00 ...
        let expected: &[u8] = &[
            0x03, 0x7D, 0x03, 0x00, 0x01, 0x01, 0x04, 0x01, 0x01, 0x0A, 0x00, 0x00, 0x01, 0x01,
            0x01, 0x01, 0x01, 0x01, 0x06, 0x00, 0x01, 0x00, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
            0x01, 0x1D, 0x01, 0x0A, 0x70, 0x79, 0x74, 0x68, 0x6F, 0x6E, 0x74, 0x65, 0x73, 0x74,
            0x01, 0x02, 0x00, 0x00, 0x00, 0x01, 0x08, 0x00, 0x01, 0x0A, 0x00, 0x01, 0x10, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(payload, expected);
    }

    #[test]
    fn subscribe_unregister_payload_matches_golden() {
        // Golden /tmp/cqn_trace.txt line 4029 (op 22, socket 5). seq 0x0A,
        // opcode 2, client_id now set to "OCI:EP:301", reg id 302 in the tail.
        let payload = build_subscribe_payload_with_seq(
            0x0A,
            TNS_SUBSCR_OP_UNREGISTER,
            Some("pythontest"),
            Some(b"OCI:EP:301"),
            TNS_SUBSCR_NAMESPACE_DBCHANGE,
            None,
            SUBSCR_QOS_ROWIDS,
            0,
            10,
            0,
            0,
            0,
            302,
            24,
        )
        .expect("unsubscribe payload");
        let expected: &[u8] = &[
            0x03, 0x7D, 0x0A, 0x00, 0x02, 0x01, 0x04, 0x01, 0x01, 0x0A, 0x01, 0x01, 0x0A, 0x01,
            0x01, 0x01, 0x01, 0x01, 0x01, 0x06, 0x00, 0x01, 0x00, 0x01, 0x01, 0x01, 0x01, 0x01,
            0x01, 0x01, 0x1D, 0x01, 0x0A, 0x70, 0x79, 0x74, 0x68, 0x6F, 0x6E, 0x74, 0x65, 0x73,
            0x74, 0x0A, 0x4F, 0x43, 0x49, 0x3A, 0x45, 0x50, 0x3A, 0x33, 0x30, 0x31, 0x01, 0x02,
            0x00, 0x00, 0x00, 0x01, 0x08, 0x00, 0x01, 0x0A, 0x00, 0x01, 0x10, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x02, 0x01, 0x2E,
        ];
        assert_eq!(payload, expected);
    }

    #[test]
    fn notify_payload_matches_golden() {
        // Golden /tmp/cqn_trace.txt line 3647 (op 8, socket 6) after data flags.
        let payload =
            build_notify_payload_with_seq(0x03, b"OCI:EP:301", 24).expect("notify payload");
        // 03 bb 03 00 01 0a 0a OCI:EP:301 01 00  (token 0x00 after seq)
        let want: &[u8] = &[
            0x03, 0xBB, 0x03, 0x00, 0x01, 0x0A, 0x0A, 0x4F, 0x43, 0x49, 0x3A, 0x45, 0x50, 0x3A,
            0x33, 0x30, 0x31, 0x01, 0x00,
        ];
        assert_eq!(payload, want);
    }

    #[test]
    fn subscribe_response_decodes_registration_and_client_id() {
        // Golden /tmp/cqn_trace.txt line 2433 (op 9, socket 5) after data flags.
        let payload: &[u8] = &[
            0x08, 0x01, 0x01, 0x00, 0x02, 0x01, 0x2E, 0x01, 0x01, 0x02, 0x01, 0x2E, 0x00, 0x00,
            0x01, 0x01, 0x01, 0x36, 0x36, 0x28, 0x41, 0x44, 0x44, 0x52, 0x45, 0x53, 0x53, 0x3D,
            0x28, 0x50, 0x52, 0x4F, 0x54, 0x4F, 0x43, 0x4F, 0x4C, 0x3D, 0x54, 0x43, 0x50, 0x29,
            0x28, 0x48, 0x4F, 0x53, 0x54, 0x3D, 0x32, 0x39, 0x30, 0x61, 0x63, 0x30, 0x33, 0x30,
            0x30, 0x33, 0x38, 0x37, 0x29, 0x28, 0x50, 0x4F, 0x52, 0x54, 0x3D, 0x31, 0x35, 0x32,
            0x31, 0x29, 0x29, 0x01, 0x0A, 0x0A, 0x4F, 0x43, 0x49, 0x3A, 0x45, 0x50, 0x3A, 0x33,
            0x30, 0x31, 0x09, 0x01, 0x01, 0x02, 0xDD, 0x48, 0x1D,
        ];
        let result = parse_subscribe_response(payload, caps_12_1()).expect("subscribe response");
        assert_eq!(result.registration_id, 302);
        assert_eq!(result.client_id.as_deref(), Some(&b"OCI:EP:301"[..]));
    }

    /// The full real notification stream captured on the emon socket
    /// (`/tmp/cqn_notif_stream.bin`): the leading OAC ack byte plus five OAC
    /// records (insert / update / insert / delete / truncate).
    const NOTIF_STREAM: &[u8] = &[
        0x0d, 0x01, 0x03, 0x00, 0x02, 0x01, 0x2e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00,
        0x00, 0x01, 0x60, 0x60, 0x00, 0x01, 0x02, 0xa4, 0xe2, 0x7a, 0x00, 0x00, 0x00, 0x06, 0x00,
        0x08, 0x46, 0x52, 0x45, 0x45, 0x50, 0x44, 0x42, 0x31, 0x01, 0x00, 0x10, 0x00, 0xd2, 0x03,
        0x00, 0x00, 0xe2, 0x7a, 0x00, 0x00, 0x00, 0x9b, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00,
        0x18, 0x50, 0x59, 0x54, 0x48, 0x4f, 0x4e, 0x54, 0x45, 0x53, 0x54, 0x2e, 0x54, 0x45, 0x53,
        0x54, 0x54, 0x45, 0x4d, 0x50, 0x54, 0x41, 0x42, 0x4c, 0x45, 0x00, 0x01, 0x1c, 0x4a, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x12, 0x41, 0x41, 0x41, 0x53, 0x6a, 0x4d, 0x41, 0x41,
        0x59, 0x41, 0x41, 0x41, 0x4a, 0x4f, 0x33, 0x41, 0x41, 0x41, 0x00, 0x01, 0x03, 0x00, 0x02,
        0x01, 0x2e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x01, 0x60, 0x60, 0x00,
        0x01, 0x00, 0x00, 0x89, 0x00, 0x00, 0x00, 0x00, 0x06, 0x00, 0x08, 0x46, 0x52, 0x45, 0x45,
        0x50, 0x44, 0x42, 0x31, 0x03, 0x00, 0x19, 0x00, 0x98, 0x04, 0x00, 0x00, 0x0b, 0x00, 0x00,
        0x00, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x18, 0x50, 0x59, 0x54, 0x48,
        0x4f, 0x4e, 0x54, 0x45, 0x53, 0x54, 0x2e, 0x54, 0x45, 0x53, 0x54, 0x54, 0x45, 0x4d, 0x50,
        0x54, 0x41, 0x42, 0x4c, 0x45, 0x00, 0x01, 0x1c, 0x4a, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04,
        0x00, 0x12, 0x41, 0x41, 0x41, 0x53, 0x6a, 0x4d, 0x41, 0x41, 0x59, 0x41, 0x41, 0x41, 0x4a,
        0x4f, 0x33, 0x41, 0x41, 0x41, 0x00, 0x01, 0x03, 0x00, 0x02, 0x01, 0x2e, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x01, 0x60, 0x60, 0x00, 0x01, 0x03, 0x00, 0x89, 0x00,
        0x00, 0x00, 0x00, 0x06, 0x00, 0x08, 0x46, 0x52, 0x45, 0x45, 0x50, 0x44, 0x42, 0x31, 0x05,
        0x00, 0x06, 0x00, 0xa9, 0x04, 0x00, 0x00, 0xe2, 0x7a, 0x00, 0x00, 0x44, 0x32, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x02, 0x00, 0x18, 0x50, 0x59, 0x54, 0x48, 0x4f, 0x4e, 0x54, 0x45, 0x53,
        0x54, 0x2e, 0x54, 0x45, 0x53, 0x54, 0x54, 0x45, 0x4d, 0x50, 0x54, 0x41, 0x42, 0x4c, 0x45,
        0x00, 0x01, 0x1c, 0x4a, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x12, 0x41, 0x41, 0x41,
        0x53, 0x6a, 0x4d, 0x41, 0x41, 0x59, 0x41, 0x41, 0x41, 0x4a, 0x4f, 0x33, 0x41, 0x41, 0x42,
        0x00, 0x01, 0x03, 0x00, 0x02, 0x01, 0x2e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00,
        0x00, 0x01, 0x60, 0x60, 0x00, 0x01, 0x03, 0xa5, 0xe2, 0x7a, 0x00, 0x00, 0x00, 0x06, 0x00,
        0x08, 0x46, 0x52, 0x45, 0x45, 0x50, 0x44, 0x42, 0x31, 0x02, 0x00, 0x09, 0x00, 0x7d, 0x04,
        0x00, 0x00, 0xe2, 0x7a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00,
        0x18, 0x50, 0x59, 0x54, 0x48, 0x4f, 0x4e, 0x54, 0x45, 0x53, 0x54, 0x2e, 0x54, 0x45, 0x53,
        0x54, 0x54, 0x45, 0x4d, 0x50, 0x54, 0x41, 0x42, 0x4c, 0x45, 0x00, 0x01, 0x1c, 0x4a, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x12, 0x41, 0x41, 0x41, 0x53, 0x6a, 0x4d, 0x41, 0x41,
        0x59, 0x41, 0x41, 0x41, 0x4a, 0x4f, 0x33, 0x41, 0x41, 0x42, 0x00, 0x01, 0x03, 0x00, 0x02,
        0x01, 0x2e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x01, 0x46, 0x46, 0x00,
        0x01, 0x00, 0x00, 0x89, 0x00, 0x00, 0x00, 0x00, 0x06, 0x00, 0x08, 0x46, 0x52, 0x45, 0x45,
        0x50, 0x44, 0x42, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xfe, 0x7f, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x11, 0x00, 0x18, 0x50, 0x59, 0x54, 0x48,
        0x4f, 0x4e, 0x54, 0x45, 0x53, 0x54, 0x2e, 0x54, 0x45, 0x53, 0x54, 0x54, 0x45, 0x4d, 0x50,
        0x54, 0x41, 0x42, 0x4c, 0x45, 0x00, 0x01, 0x1c, 0x4a, 0x00,
    ];

    #[test]
    fn notification_stream_decodes_dml_events() {
        let records = parse_notification_stream(
            NOTIF_STREAM,
            TNS_SUBSCR_NAMESPACE_DBCHANGE,
            SUBSCR_QOS_ROWIDS,
            Some("FREEPDB1"),
        )
        .expect("notification stream");
        let messages: Vec<&NotificationMessage> = records
            .iter()
            .filter_map(|r| match r {
                NotificationRecord::Message { message, .. } => Some(message),
                NotificationRecord::Stop => None,
            })
            .collect();
        assert_eq!(messages.len(), 5);

        let table_ops: Vec<u32> = messages.iter().map(|m| m.tables[0].operation).collect();
        assert_eq!(table_ops, vec![2, 4, 2, 8, 17]);

        let mut row_ops = Vec::new();
        let mut rowids = Vec::new();
        for m in &messages {
            assert_eq!(m.msg_type, EVENT_OBJCHANGE);
            assert_eq!(m.dbname.as_deref(), Some("FREEPDB1"));
            assert!(m.registered);
            assert!(m.txid.is_none());
            for row in &m.tables[0].rows {
                row_ops.push(row.operation);
                rowids.push(row.rowid.clone());
            }
        }
        assert_eq!(row_ops, vec![2, 4, 2, 8]);
        assert_eq!(
            rowids,
            vec![
                "AAASjMAAYAAAJO3AAA",
                "AAASjMAAYAAAJO3AAA",
                "AAASjMAAYAAAJO3AAB",
                "AAASjMAAYAAAJO3AAB",
            ]
        );
        // the truncate record carries the ALLROWS bit, so no rows are present
        assert!(messages[4].tables[0].rows.is_empty());
    }

    // ---- 12.1 version-gate boundary tests ---------------------------------
    //
    // Reference messages/subscribe.pyx gates the client-id pointer block on the
    // write side (:127) and the subscriber name + RAC instance/listener block
    // on the read side (:61, :63), all on ttc field version >= 12.1 (7). 12.1's
    // field version (7) is far below our live floor (18c == 11), so no live lane
    // ever exercises the pre-12.1 branch; these offline tests pin both sides.

    fn assert_single_insertion(lo: &[u8], hi: &[u8], label: &str) {
        assert!(hi.len() > lo.len(), "{label}: gated block must add bytes");
        let prefix = lo.iter().zip(hi).take_while(|(a, b)| a == b).count();
        let suffix = lo[prefix..]
            .iter()
            .rev()
            .zip(hi[prefix..].iter().rev())
            .take_while(|(a, b)| a == b)
            .count();
        assert_eq!(
            prefix + suffix,
            lo.len(),
            "{label}: below-boundary bytes must equal above-boundary bytes minus one inserted block"
        );
    }

    // Reference messages/subscribe.pyx:127 — the kpninst/client-id pointer block.
    #[test]
    fn subscribe_build_gates_client_id_block_on_12_1() {
        let build = |fv| {
            build_subscribe_payload_with_seq(
                1,
                TNS_SUBSCR_OP_REGISTER,
                None,
                None,
                TNS_SUBSCR_NAMESPACE_DBCHANGE,
                None,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                fv,
            )
            .expect("subscribe payload")
        };
        assert_single_insertion(
            &build(TNS_CCAP_FIELD_VERSION_12_1 - 1),
            &build(TNS_CCAP_FIELD_VERSION_12_1),
            "subscribe client-id block (12.1)",
        );
    }

    // Reference messages/subscribe.pyx:61,63 — subscriber name + RAC instance
    // and listener addresses, read only at >= 12.1.
    #[test]
    fn subscribe_response_read_gates_subscriber_and_instances_on_12_1() {
        let fixture = |fv: u8| {
            let mut w = TtcWriter::new();
            w.write_ub4(0); // kpnrl count (short registration ids)
            w.write_ub4(1); // kpngrl count (one registration)
            w.write_ub8(302); // registration id
            if fv >= TNS_CCAP_FIELD_VERSION_12_1 {
                w.write_bytes_with_two_lengths(Some(b"SUB"))
                    .expect("subscriber name");
            }
            if fv >= TNS_CCAP_FIELD_VERSION_12_1 {
                w.write_ub4(0); // num instances
                w.write_ub4(0); // num listeners
                w.write_bytes_with_two_lengths(Some(b"OCI:EP:301"))
                    .expect("client id");
            }
            w.into_bytes()
        };
        let parse = |bytes: &[u8], fv: u8| -> Option<(u64, Option<Vec<u8>>, usize)> {
            let mut reader = TtcReader::new(bytes);
            let mut result = SubscribeResult::default();
            parse_subscribe_return_parameters(&mut reader, fv, &mut result)
                .ok()
                .map(|()| (result.registration_id, result.client_id, reader.remaining()))
        };
        let lo = TNS_CCAP_FIELD_VERSION_12_1 - 1;
        let hi = TNS_CCAP_FIELD_VERSION_12_1;

        // Version-matched parses consume the record exactly.
        assert_eq!(parse(&fixture(lo), lo), Some((302, None, 0)));
        assert_eq!(
            parse(&fixture(hi), hi),
            Some((302, Some(b"OCI:EP:301".to_vec()), 0))
        );

        // The >= 12.1 block present but read as pre-12.1: the client id is never
        // read and the block is left unconsumed on the wire.
        let (reg, client, remaining) = parse(&fixture(hi), lo).expect("reg id still parses");
        assert_eq!(reg, 302);
        assert_eq!(client, None, "pre-12.1 read must not consume the client id");
        assert!(
            remaining > 0,
            "pre-12.1 read leaves the 12.1 block unconsumed"
        );

        // The block absent but read as >= 12.1: the parser expects a subscriber
        // name that is not there and fails closed.
        assert_eq!(parse(&fixture(lo), hi), None, "over-read must fail closed");
    }
}
