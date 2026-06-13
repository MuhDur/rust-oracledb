#![forbid(unsafe_code)]

use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ServerErrorInfo {
    pub(crate) number: u32,
    pub(crate) message: String,
    pub(crate) cursor_id: u16,
    pub(crate) pos: i32,
    pub(crate) row_count: u64,
    pub(crate) rowid: Option<String>,
    pub(crate) batch_errors: Vec<BatchServerError>,
    pub(crate) compilation_error_warning: bool,
    /// End-of-call status (reference `_process_error_info` reads
    /// `self.call_status`). On a successful round trip the response's final
    /// message is an ERROR with `number = 0` whose call status carries the
    /// transaction-in-progress bit.
    pub(crate) call_status: u32,
}

impl ServerErrorInfo {
    pub(crate) fn into_details(self) -> crate::ServerErrorDetails {
        crate::ServerErrorDetails {
            message: self.message,
            code: self.number,
            pos: self.pos,
            row_count: self.row_count,
            rowid: self.rowid,
            array_dml_row_counts: None,
        }
    }
}

/// Encodes a physical rowid the way the reference driver does
/// (impl/thin/utils.pyx `_encode_rowid`/`_convert_base64`).
pub(crate) fn encode_rowid(
    rba: u32,
    partition_id: u16,
    block_num: u32,
    slot_num: u16,
) -> Option<String> {
    if rba == 0 && partition_id == 0 && block_num == 0 && slot_num == 0 {
        return None;
    }
    let mut out = String::with_capacity(18);
    encode_rowid_component(rba, 6, &mut out);
    encode_rowid_component(u32::from(partition_id), 3, &mut out);
    encode_rowid_component(block_num, 6, &mut out);
    encode_rowid_component(u32::from(slot_num), 3, &mut out);
    Some(out)
}

pub(crate) fn parse_server_error(
    reader: &mut TtcReader<'_>,
    ttc_field_version: u8,
) -> Result<Option<String>> {
    let info = parse_server_error_info(reader, ttc_field_version)?;
    if info.number == 0 {
        Ok(None)
    } else if info.message.is_empty() {
        Ok(Some(format!("ORA-{:05}", info.number)))
    } else {
        Ok(Some(info.message))
    }
}

pub(crate) fn parse_server_error_info(
    reader: &mut TtcReader<'_>,
    ttc_field_version: u8,
) -> Result<ServerErrorInfo> {
    let call_status = reader.read_ub4()?;
    let _seq = reader.read_ub2()?;
    let _current_row = reader.read_ub4()?;
    let _error_number = reader.read_ub2()?;
    let _array_elem_error_1 = reader.read_ub2()?;
    let _array_elem_error_2 = reader.read_ub2()?;
    let cursor_id = reader.read_ub2()?;
    let error_pos = reader.read_sb4()?; // sb2 error position (same wire shape)
    reader.skip(5)?;
    let warning_flags = reader.read_u8()?;
    let rowid = read_rowid(reader)?;
    let _os_error = reader.read_ub4()?;
    reader.skip(2)?;
    let _padding = reader.read_ub2()?;
    let _success_iters = reader.read_ub4()?;
    reader.read_bytes_with_length()?;

    let mut batch_errors: Vec<BatchServerError> = Vec::new();
    let batch_error_count = reader.read_ub2()?;
    if batch_error_count > 0 {
        let first_byte = reader.read_u8()?;
        for _ in 0..batch_error_count {
            if first_byte == crate::wire::TNS_LONG_LENGTH_INDICATOR {
                let _chunk_len = reader.read_ub4()?;
            }
            let code = reader.read_ub2()?;
            batch_errors.push(BatchServerError {
                code: u32::from(code),
                ..BatchServerError::default()
            });
        }
        if first_byte == crate::wire::TNS_LONG_LENGTH_INDICATOR {
            reader.skip(1)?;
        }
    }

    let batch_offset_count = reader.read_ub4()?;
    if batch_offset_count > 0 {
        let first_byte = reader.read_u8()?;
        for index in 0..batch_offset_count {
            if first_byte == crate::wire::TNS_LONG_LENGTH_INDICATOR {
                let _chunk_len = reader.read_ub4()?;
            }
            let offset = reader.read_ub4()?;
            if let Some(entry) = batch_errors.get_mut(index as usize) {
                entry.offset = offset;
            }
        }
        if first_byte == crate::wire::TNS_LONG_LENGTH_INDICATOR {
            reader.skip(1)?;
        }
    }

    let batch_message_count = reader.read_ub2()?;
    if batch_message_count > 0 {
        reader.skip(1)?; // packed size
        for index in 0..batch_message_count {
            let _chunk_len = reader.read_ub2()?;
            let message = reader
                .read_bytes()?
                .map(|bytes| String::from_utf8_lossy(&bytes).trim_end().to_string())
                .unwrap_or_default();
            if let Some(entry) = batch_errors.get_mut(usize::from(index)) {
                entry.message = message;
            }
            reader.skip(2)?; // end marker
        }
    }

    let error_number = reader.read_ub4()?;
    let row_count = reader.read_ub8()?;
    if ttc_field_version >= TNS_CCAP_FIELD_VERSION_20_1
        || (reader.remaining() > 2 && reader.peek_u8()? == 0)
    {
        let _sql_type = reader.read_ub4()?;
        let _server_checksum = reader.read_ub4()?;
    }
    let message = if error_number != 0 {
        reader
            .read_bytes()?
            .map(|bytes| String::from_utf8_lossy(&bytes).trim().to_string())
            .unwrap_or_else(|| format!("ORA-{error_number:05}"))
    } else {
        String::new()
    };

    Ok(ServerErrorInfo {
        number: error_number,
        message,
        cursor_id,
        pos: if error_pos > 0 { error_pos } else { 0 },
        row_count,
        rowid,
        batch_errors,
        compilation_error_warning: warning_flags & 0x20 != 0,
        call_status,
    })
}

pub(crate) fn read_rowid(reader: &mut TtcReader<'_>) -> Result<Option<String>> {
    let rba = reader.read_ub4()?;
    let partition_id = reader.read_ub2()?;
    reader.skip(1)?;
    let block_num = reader.read_ub4()?;
    let slot_num = reader.read_ub2()?;
    Ok(encode_rowid(rba, partition_id, block_num, slot_num))
}

/// Process a server-side piggyback, returning any sessionless transaction
/// state update carried by the SYNC piggyback's `TRANSACTION_ID` keyword
/// (reference messages/base.pyx `_process_server_side_piggyback`). Most callers
/// discard the result with `?;`.
pub(crate) fn skip_server_side_piggyback(
    reader: &mut TtcReader<'_>,
) -> Result<Option<SessionlessTxnState>> {
    let opcode = reader.read_u8()?;
    let mut txn_state = None;
    match opcode {
        TNS_SERVER_PIGGYBACK_LTXID => {
            let _ltxid = reader.read_bytes_with_length()?;
        }
        TNS_SERVER_PIGGYBACK_QUERY_CACHE_INVALIDATION | TNS_SERVER_PIGGYBACK_TRACE_EVENT => {}
        TNS_SERVER_PIGGYBACK_OS_PID_MTS => {
            let _pid = reader.read_ub2()?;
            let _mts = reader.read_bytes()?;
        }
        TNS_SERVER_PIGGYBACK_SYNC => {
            let _num_dtys = reader.read_ub2()?;
            reader.skip(1)?;
            let num_elements = reader.read_ub2()?;
            reader.skip(1)?;
            txn_state = read_keyword_value_pairs_for_txn_state(reader, num_elements)?;
            let _flags = reader.read_ub4()?;
        }
        TNS_SERVER_PIGGYBACK_EXT_SYNC => {
            let _num_dtys = reader.read_ub2()?;
            reader.skip(1)?;
        }
        TNS_SERVER_PIGGYBACK_AC_REPLAY_CONTEXT => {
            let _num_dtys = reader.read_ub2()?;
            reader.skip(1)?;
            let _flags = reader.read_ub4()?;
            let _error_code = reader.read_ub4()?;
            reader.skip(1)?;
            let _replay_context = reader.read_bytes_with_length()?;
        }
        TNS_SERVER_PIGGYBACK_SESS_RET => {
            let _num_dtys = reader.read_ub2()?;
            reader.skip(1)?;
            let num_elements = reader.read_ub2()?;
            if num_elements > 0 {
                reader.skip(1)?;
                for _ in 0..num_elements {
                    if reader.read_ub2()? > 0 {
                        let _key = reader.read_bytes()?;
                    }
                    if reader.read_ub2()? > 0 {
                        let _value = reader.read_bytes()?;
                    }
                    let _flags = reader.read_ub2()?;
                }
            }
            let _flags = reader.read_ub4()?;
            let _session_id = reader.read_ub4()?;
            let _serial_num = reader.read_ub2()?;
        }
        TNS_SERVER_PIGGYBACK_SESS_SIGNATURE => {
            let _num_dtys = reader.read_ub2()?;
            reader.skip(1)?;
            let _signature_flags = reader.read_ub8()?;
            let _client_signature = reader.read_ub8()?;
            let _server_signature = reader.read_ub8()?;
        }
        _ => return Err(ProtocolError::UnsupportedFeature("server-side piggyback")),
    }
    Ok(txn_state)
}

pub(crate) fn has_u8_flag(flags: u8, mask: u8) -> bool {
    flags & mask > 0
}

pub(crate) fn has_u32_flag(flags: u32, mask: u32) -> bool {
    flags & mask > 0
}
