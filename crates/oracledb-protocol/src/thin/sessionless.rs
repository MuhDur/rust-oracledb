#![forbid(unsafe_code)]

use super::*;

/// Body of the transaction-switch message (reference impl/thin/messages/
/// tpc_switch.pyx `_write_message`), shared by the direct function call and the
/// piggyback forms. `xid` is the (format_id, global_txn_id) of a sessionless
/// transaction being started; `None` for a suspend/detach which carries no XID.
pub(crate) fn write_tpc_txn_switch_body(
    writer: &mut TtcWriter,
    operation: u32,
    flags: u32,
    timeout: u32,
    xid: Option<&[u8]>,
) {
    writer.write_ub4(operation);
    writer.write_u8(0); // pointer (transaction context)
    writer.write_ub4(0); // transaction context length
    if let Some(global_txn_id) = xid {
        // sessionless transactions send only a global transaction id; the
        // branch qualifier is empty and the combined value is right-padded
        // with zero bytes to 128 bytes (tpc_switch.pyx:80-81).
        let mut xid_bytes = global_txn_id.to_vec();
        xid_bytes.resize(128, 0);
        writer.write_ub4(SESSIONLESS_FORMAT_ID);
        writer.write_ub4(u32::try_from(global_txn_id.len()).unwrap_or(0)); // global txn id len
        writer.write_ub4(0); // branch qualifier length
        writer.write_u8(1); // pointer (XID)
        writer.write_ub4(u32::try_from(xid_bytes.len()).unwrap_or(0));
        writer.write_ub4(flags);
        writer.write_ub4(timeout);
        writer.write_u8(1); // pointer (application value)
        writer.write_u8(1); // pointer (return context)
        writer.write_u8(1); // pointer (return context length)
        writer.write_u8(0); // pointer (internal name)
        writer.write_ub4(0); // length of internal name
        writer.write_u8(0); // pointer (external name)
        writer.write_ub4(0); // length of external name
        writer.write_raw(&xid_bytes);
        writer.write_ub4(0); // application value
    } else {
        writer.write_ub4(0); // format id
        writer.write_ub4(0); // global transaction id length
        writer.write_ub4(0); // branch qualifier length
        writer.write_u8(0); // pointer (XID)
        writer.write_ub4(0); // XID length
        writer.write_ub4(flags);
        writer.write_ub4(timeout);
        writer.write_u8(1); // pointer (application value)
        writer.write_u8(1); // pointer (return context)
        writer.write_u8(1); // pointer (return context length)
        writer.write_u8(0); // pointer (internal name)
        writer.write_ub4(0); // length of internal name
        writer.write_u8(0); // pointer (external name)
        writer.write_ub4(0); // length of external name
        writer.write_ub4(0); // application value
    }
}

/// Direct (non-deferred) transaction-switch function call used to begin/resume
/// (`TNS_TPC_TXN_START` + new/resume flag, with `xid`) or suspend
/// (`TNS_TPC_TXN_DETACH`, no `xid`) a sessionless transaction. Reference
/// impl/thin/connection.pyx `begin/resume/suspend_sessionless_transaction`.
pub fn build_tpc_txn_switch_payload_with_seq(
    seq_num: u8,
    token_num: u64,
    operation: u32,
    flags: u32,
    timeout: u32,
    xid: Option<&[u8]>,
) -> Vec<u8> {
    let mut writer = TtcWriter::new();
    writer.write_function_code_with_seq(TNS_FUNC_TPC_TXN_SWITCH, seq_num);
    writer.write_ub8(token_num);
    write_tpc_txn_switch_body(&mut writer, operation, flags, timeout, xid);
    writer.into_bytes()
}

/// Sessionless transaction-switch piggyback, prepended to the next execute
/// message's payload (reference messages/base.pyx `_write_sessionless_piggyback`
/// — the same message body written with a `TNS_MSG_TYPE_PIGGYBACK` header). Used
/// for a deferred begin/resume (`defer_round_trip=True`) and for the
/// `suspend_on_success` post-detach. `operation` already encodes whether a
/// post-detach is folded in (`TNS_TPC_TXN_START | TNS_TPC_TXN_POST_DETACH`).
pub fn build_sessionless_piggyback(
    seq_num: u8,
    token_num: u64,
    operation: u32,
    flags: u32,
    timeout: u32,
    xid: Option<&[u8]>,
) -> Vec<u8> {
    let mut writer = TtcWriter::new();
    writer.write_u8(TNS_MSG_TYPE_PIGGYBACK);
    writer.write_u8(TNS_FUNC_TPC_TXN_SWITCH);
    writer.write_u8(seq_num);
    writer.write_ub8(token_num);
    write_tpc_txn_switch_body(&mut writer, operation, flags, timeout, xid);
    writer.into_bytes()
}

/// Decode the sessionless state bits packed in the transaction-id key/value
/// binary payload (reference `_update_sessionless_txn_state`). The last two
/// bytes are the state mask and the sync version; the leading bytes are the
/// transaction id itself.
pub fn decode_sessionless_txn_state(binary: &[u8]) -> Result<Option<SessionlessTxnState>> {
    if binary.len() < 2 {
        return Err(ProtocolError::TtcDecode("short sessionless txn state"));
    }
    let state = binary[binary.len() - 2];
    let sync_version = binary[binary.len() - 1];
    if sync_version != 1 {
        return Err(ProtocolError::TtcDecode("unknown transaction sync version"));
    }
    if state & TNS_TPC_TXNID_SYNC_UNSET != 0 {
        Ok(Some(SessionlessTxnState::Unset))
    } else if state & TNS_TPC_TXNID_SYNC_SET != 0 {
        Ok(Some(SessionlessTxnState::Set {
            started_on_server: state & TNS_TPC_TXNID_SYNC_SERVER != 0,
        }))
    } else {
        Ok(None)
    }
}

/// Parse a transaction-switch response (reference tpc_switch.pyx
/// `_process_return_parameters` plus base.pyx message loop). Returns any
/// sessionless state update carried by a transaction-id key/value pair; server
/// errors (e.g. ORA-25351 / ORA-26217) are surfaced as `ProtocolError`.
pub fn parse_tpc_txn_switch_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
) -> Result<Option<SessionlessTxnState>> {
    let mut reader = TtcReader::new(payload);
    let mut state = None;
    while reader.remaining() > 0 {
        let message_type = reader.read_u8()?;
        match message_type {
            0 => {}
            TNS_MSG_TYPE_STATUS => {
                let _call_status = reader.read_ub4()?;
                let _seq = reader.read_ub2()?;
            }
            TNS_MSG_TYPE_PARAMETER => {
                // tpc_switch.pyx `_process_return_parameters`: application value
                // (ub4) then the return transaction context (ub2 length + bytes).
                let _application_value = reader.read_ub4()?;
                let context_len = reader.read_ub2()?;
                if context_len > 0 {
                    reader.skip(usize::from(context_len))?;
                }
            }
            TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK => {
                if let Some(update) = skip_server_side_piggyback(&mut reader)? {
                    state = Some(update);
                }
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
            _ => break,
        }
    }
    Ok(state)
}

/// Begin-pipeline piggyback (messages/base.pyx `_write_begin_pipeline_piggyback`
/// and `_write_piggyback_code`): prepended to the first pipelined message's
/// payload. The packet carrying it must set [`TNS_DATA_FLAGS_BEGIN_PIPELINE`].
///
/// `token_num` is the token of the message the piggyback rides on (1 for the
/// first pipeline operation); `pipeline_mode` is one of
/// [`TNS_PIPELINE_MODE_CONTINUE_ON_ERROR`] / [`TNS_PIPELINE_MODE_ABORT_ON_ERROR`].
pub fn build_begin_pipeline_piggyback(seq_num: u8, token_num: u64, pipeline_mode: u8) -> Vec<u8> {
    let mut writer = TtcWriter::new();
    writer.write_u8(TNS_MSG_TYPE_PIGGYBACK);
    writer.write_u8(TNS_FUNC_PIPELINE_BEGIN);
    writer.write_u8(seq_num);
    writer.write_ub8(token_num);
    writer.write_ub2(0); // error set ID
    writer.write_u8(0); // error set mode
    writer.write_u8(pipeline_mode);
    writer.into_bytes()
}

/// End-pipeline message (messages/end_pipeline.pyx): function 200 plus an
/// unused ub4 identifier. Sent after every pipelined operation message; its
/// packet carries no END_OF_REQUEST flag and its response is the final
/// (N+1th) boundary-delimited response of the pipeline.
pub fn build_end_pipeline_payload_with_seq(seq_num: u8) -> Vec<u8> {
    let mut writer = TtcWriter::new();
    writer.write_function_code_with_seq(TNS_FUNC_PIPELINE_END, seq_num);
    writer.write_ub8(0); // token (the end-pipeline message itself has none)
    writer.write_ub4(0); // error set ID (unused)
    writer.into_bytes()
}

/// A two-phase-commit transaction id (reference `Xid` namedtuple). The
/// `global_transaction_id` and `branch_qualifier` are the raw (already
/// UTF-8 encoded) byte values; the shim coerces `str` members before calling.
#[derive(Clone, Debug)]
pub struct TpcXid<'a> {
    pub format_id: u32,
    pub global_transaction_id: &'a [u8],
    pub branch_qualifier: &'a [u8],
}

/// Writes the XID descriptor + the 128-byte zero-padded XID block, shared by
/// the full-XA switch (func 103) and change-state (func 104) messages. The
/// descriptor (`format_id`, gtid length, bqual length, pointer, block length)
/// is written at the caller-specified position; the 128-byte block itself is
/// written by [`write_xid_block_bytes`] later in the message body, after the
/// context bytes (reference tpc_switch.pyx / tpc_change_state.pyx).
fn write_xid_descriptor(writer: &mut TtcWriter, xid: Option<&TpcXid<'_>>) {
    match xid {
        Some(xid) => {
            writer.write_ub4(xid.format_id);
            writer.write_ub4(u32::try_from(xid.global_transaction_id.len()).unwrap_or(0));
            writer.write_ub4(u32::try_from(xid.branch_qualifier.len()).unwrap_or(0));
            writer.write_u8(1); // pointer (XID)
            writer.write_ub4(128); // length of the XID block
        }
        None => {
            writer.write_ub4(0); // format id
            writer.write_ub4(0); // global transaction id length
            writer.write_ub4(0); // branch qualifier length
            writer.write_u8(0); // pointer (XID)
            writer.write_ub4(0); // XID length
        }
    }
}

/// The 128-byte XID block: `global_transaction_id + branch_qualifier`,
/// right-zero-padded to exactly 128 bytes (reference tpc_switch.pyx:80-81).
fn write_xid_block_bytes(writer: &mut TtcWriter, xid: &TpcXid<'_>) {
    let mut xid_bytes = Vec::with_capacity(128);
    xid_bytes.extend_from_slice(xid.global_transaction_id);
    xid_bytes.extend_from_slice(xid.branch_qualifier);
    xid_bytes.resize(128, 0);
    writer.write_raw(&xid_bytes);
}

/// Full-XA transaction-switch payload (func 103), used by `tpc_begin`
/// (`operation = TNS_TPC_TXN_START`) and `tpc_end` (`operation =
/// TNS_TPC_TXN_DETACH`). Unlike [`build_tpc_txn_switch_payload_with_seq`] (the
/// sessionless special case) this carries a real `format_id`, a non-empty
/// branch qualifier, and the captured transaction `context` to echo back.
/// Reference messages/tpc_switch.pyx `_write_message`.
pub fn build_tpc_switch_payload_with_seq(
    seq_num: u8,
    operation: u32,
    flags: u32,
    timeout: u32,
    xid: Option<&TpcXid<'_>>,
    context: Option<&[u8]>,
) -> Vec<u8> {
    let mut writer = TtcWriter::new();
    writer.write_function_code_with_seq(TNS_FUNC_TPC_TXN_SWITCH, seq_num);
    writer.write_ub8(0); // token
    writer.write_ub4(operation);
    match context {
        Some(context) => {
            writer.write_u8(1); // pointer (transaction context)
            writer.write_ub4(u32::try_from(context.len()).unwrap_or(0));
        }
        None => {
            writer.write_u8(0); // pointer (transaction context)
            writer.write_ub4(0); // transaction context length
        }
    }
    write_xid_descriptor(&mut writer, xid);
    writer.write_ub4(flags);
    writer.write_ub4(timeout);
    writer.write_u8(1); // pointer (application value)
    writer.write_u8(1); // pointer (return context)
    writer.write_u8(1); // pointer (return context length)
    writer.write_u8(0); // pointer (internal name)
    writer.write_ub4(0); // length of internal name
    writer.write_u8(0); // pointer (external name)
    writer.write_ub4(0); // length of external name
    if let Some(context) = context {
        writer.write_raw(context);
    }
    if let Some(xid) = xid {
        write_xid_block_bytes(&mut writer, xid);
    }
    writer.write_ub4(0); // application value
    writer.into_bytes()
}

/// TPC transaction change-state payload (func 104), used by `tpc_prepare`
/// (`operation = TNS_TPC_TXN_PREPARE`), `tpc_commit` (`TNS_TPC_TXN_COMMIT`) and
/// `tpc_rollback` (`TNS_TPC_TXN_ABORT`). `requested_state` is the desired state
/// (0 for prepare; READ_ONLY/COMMITTED for commit; ABORTED for rollback).
/// Reference messages/tpc_change_state.pyx `_write_message`.
pub fn build_tpc_change_state_payload_with_seq(
    seq_num: u8,
    operation: u32,
    requested_state: u32,
    flags: u32,
    xid: Option<&TpcXid<'_>>,
    context: Option<&[u8]>,
) -> Vec<u8> {
    let mut writer = TtcWriter::new();
    writer.write_function_code_with_seq(TNS_FUNC_TPC_TXN_CHANGE_STATE, seq_num);
    writer.write_ub8(0); // token
    writer.write_ub4(operation);
    match context {
        Some(context) => {
            writer.write_u8(1); // pointer (context)
            writer.write_ub4(u32::try_from(context.len()).unwrap_or(0));
        }
        None => {
            writer.write_u8(0); // pointer (context)
            writer.write_ub4(0); // context length
        }
    }
    write_xid_descriptor(&mut writer, xid);
    writer.write_ub4(0); // timeout (always 0)
    writer.write_ub4(requested_state);
    writer.write_u8(1); // pointer (out state)
    writer.write_ub4(flags);
    if let Some(context) = context {
        writer.write_raw(context);
    }
    if let Some(xid) = xid {
        write_xid_block_bytes(&mut writer, xid);
    }
    writer.into_bytes()
}

/// Parse a full-XA transaction-switch response (reference tpc_switch.pyx
/// `_process_return_parameters` plus the base.pyx message loop). Captures the
/// returned transaction context (PARAMETER message) and the txn-in-progress bit
/// (last call status). Server errors are surfaced as `ProtocolError`.
pub fn parse_tpc_switch_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
) -> Result<TpcSwitchResponse> {
    let mut reader = TtcReader::new(payload);
    let mut response = TpcSwitchResponse::default();
    while reader.remaining() > 0 {
        let message_type = reader.read_u8()?;
        match message_type {
            0 => {}
            TNS_MSG_TYPE_STATUS => {
                let call_status = reader.read_ub4()?;
                let _seq = reader.read_ub2()?;
                response.txn_in_progress = call_status & TNS_EOCS_FLAGS_TXN_IN_PROGRESS != 0;
            }
            TNS_MSG_TYPE_PARAMETER => {
                // tpc_switch.pyx `_process_return_parameters`: application value
                // (ub4) then the return transaction context (ub2 length + bytes).
                let _application_value = reader.read_ub4()?;
                let context_len = reader.read_ub2()?;
                let context = reader.read_raw(usize::from(context_len))?;
                response.context = context.to_vec();
            }
            TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK => {
                if let Some(update) = skip_server_side_piggyback(&mut reader)? {
                    response.sessionless_state = Some(update);
                }
            }
            TNS_MSG_TYPE_END_OF_RESPONSE => break,
            TNS_MSG_TYPE_ERROR => {
                // On a server error the reference raises before
                // `_process_call_status` runs, so `_txn_in_progress` keeps its
                // prior value; we likewise leave `txn_in_progress` untouched and
                // surface the error.
                let info = parse_server_error_info(&mut reader, capabilities.ttc_field_version)?;
                if info.number != 0 {
                    return Err(ProtocolError::ServerErrorInfo(Box::new(
                        info.into_details(),
                    )));
                }
            }
            _ => break,
        }
    }
    Ok(response)
}

/// Parse a TPC change-state response (reference tpc_change_state.pyx
/// `_process_return_parameters` plus the base.pyx message loop). Reads the out
/// state from the PARAMETER message and the txn-in-progress bit from the last
/// call status. Server errors are surfaced as `ProtocolError`.
pub fn parse_tpc_change_state_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
) -> Result<TpcChangeStateResponse> {
    let mut reader = TtcReader::new(payload);
    let mut response = TpcChangeStateResponse::default();
    while reader.remaining() > 0 {
        let message_type = reader.read_u8()?;
        match message_type {
            0 => {}
            TNS_MSG_TYPE_STATUS => {
                let call_status = reader.read_ub4()?;
                let _seq = reader.read_ub2()?;
                response.txn_in_progress = call_status & TNS_EOCS_FLAGS_TXN_IN_PROGRESS != 0;
            }
            TNS_MSG_TYPE_PARAMETER => {
                // tpc_change_state.pyx `_process_return_parameters` reads the
                // out state (ub4).
                response.state = reader.read_ub4()?;
            }
            TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK => {
                skip_server_side_piggyback(&mut reader)?;
            }
            TNS_MSG_TYPE_END_OF_RESPONSE => break,
            TNS_MSG_TYPE_ERROR => {
                // On a server error the reference raises before
                // `_process_call_status` runs, so `_txn_in_progress` keeps its
                // prior value; we likewise leave `txn_in_progress` untouched and
                // surface the error.
                let info = parse_server_error_info(&mut reader, capabilities.ttc_field_version)?;
                if info.number != 0 {
                    return Err(ProtocolError::ServerErrorInfo(Box::new(
                        info.into_details(),
                    )));
                }
            }
            _ => break,
        }
    }
    Ok(response)
}

pub(crate) fn skip_keyword_value_pairs(reader: &mut TtcReader<'_>, num_pairs: u16) -> Result<()> {
    read_keyword_value_pairs_for_txn_state(reader, num_pairs).map(|_| ())
}

/// Like [`skip_keyword_value_pairs`] but extracts the sessionless transaction
/// state carried by the `TRANSACTION_ID` keyword (201). Reference
/// `_process_keyword_value_pairs` calls `_update_sessionless_txn_state` on the
/// binary value of that keyword.
pub(crate) fn read_keyword_value_pairs_for_txn_state(
    reader: &mut TtcReader<'_>,
    num_pairs: u16,
) -> Result<Option<SessionlessTxnState>> {
    let mut state = None;
    for _ in 0..num_pairs {
        if reader.read_ub2()? > 0 {
            let _text_value = reader.read_bytes()?;
        }
        let mut binary_value = None;
        if reader.read_ub2()? > 0 {
            binary_value = reader.read_bytes()?;
        }
        let keyword_num = reader.read_ub2()?;
        if keyword_num == TNS_KEYWORD_NUM_TRANSACTION_ID {
            if let Some(binary) = binary_value.as_deref() {
                if let Some(update) = decode_sessionless_txn_state(binary)? {
                    state = Some(update);
                }
            }
        }
    }
    Ok(state)
}
