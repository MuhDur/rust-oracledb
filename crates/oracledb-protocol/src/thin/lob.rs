#![forbid(unsafe_code)]

use super::*;
use crate::wire::ProtocolLimits;

pub fn build_lob_read_payload_with_seq(
    locator: &[u8],
    offset: u64,
    amount: u64,
    seq_num: u8,
    ttc_field_version: u8,
) -> Result<Vec<u8>> {
    let locator_len =
        u32::try_from(locator.len()).map_err(|_| ProtocolError::InvalidPacketLength {
            length: locator.len(),
            minimum: 0,
        })?;
    let mut writer = TtcWriter::new();
    writer.write_function_header(TNS_FUNC_LOB_OP, seq_num, ttc_field_version);
    writer.write_u8(1);
    writer.write_ub4(locator_len);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub4(TNS_LOB_OP_READ);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub8(offset);
    writer.write_ub8(0);
    writer.write_u8(1);
    for _ in 0..3 {
        writer.write_u16be(0);
    }
    writer.write_raw(locator);
    writer.write_ub8(amount);
    Ok(writer.into_bytes())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn write_lob_op_header(
    writer: &mut TtcWriter,
    locator: &[u8],
    seq_num: u8,
    ttc_field_version: u8,
    operation: u32,
    dest_length: u32,
    source_offset: u64,
    dest_offset: u64,
    pointer_charset: bool,
    pointer_null_lob: bool,
    send_amount: bool,
) -> Result<()> {
    let locator_len =
        u32::try_from(locator.len()).map_err(|_| ProtocolError::InvalidPacketLength {
            length: locator.len(),
            minimum: 0,
        })?;
    writer.write_function_header(TNS_FUNC_LOB_OP, seq_num, ttc_field_version);
    writer.write_u8(1);
    writer.write_ub4(locator_len);
    writer.write_u8(0);
    writer.write_ub4(dest_length);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_u8(u8::from(pointer_charset));
    writer.write_u8(0);
    writer.write_u8(u8::from(pointer_null_lob));
    writer.write_ub4(operation);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub8(source_offset);
    writer.write_ub8(dest_offset);
    writer.write_u8(u8::from(send_amount));
    for _ in 0..3 {
        writer.write_u16be(0);
    }
    writer.write_raw(locator);
    Ok(())
}

pub fn build_lob_create_temp_payload_with_seq(
    ora_type_num: u8,
    csfrm: u8,
    seq_num: u8,
    ttc_field_version: u8,
) -> Result<Vec<u8>> {
    let mut writer = TtcWriter::new();
    write_lob_op_header(
        &mut writer,
        &[0; 40],
        seq_num,
        ttc_field_version,
        TNS_LOB_OP_CREATE_TEMP,
        TNS_DURATION_SESSION,
        u64::from(csfrm),
        u64::from(ora_type_num),
        true,
        true,
        false,
    )?;
    writer.write_ub4(TNS_CHARSET_UTF8.into());
    Ok(writer.into_bytes())
}

pub fn build_lob_write_payload_with_seq(
    locator: &[u8],
    offset: u64,
    data: &[u8],
    seq_num: u8,
    ttc_field_version: u8,
) -> Result<Vec<u8>> {
    let mut writer = TtcWriter::new();
    write_lob_op_header(
        &mut writer,
        locator,
        seq_num,
        ttc_field_version,
        TNS_LOB_OP_WRITE,
        0,
        offset,
        0,
        false,
        false,
        false,
    )?;
    writer.write_u8(TNS_MSG_TYPE_LOB_DATA);
    writer.write_bytes_with_length(data)?;
    Ok(writer.into_bytes())
}

pub fn build_lob_trim_payload_with_seq(
    locator: &[u8],
    new_size: u64,
    seq_num: u8,
    ttc_field_version: u8,
) -> Result<Vec<u8>> {
    let mut writer = TtcWriter::new();
    write_lob_op_header(
        &mut writer,
        locator,
        seq_num,
        ttc_field_version,
        TNS_LOB_OP_TRIM,
        0,
        0,
        0,
        false,
        false,
        true,
    )?;
    writer.write_ub8(new_size);
    Ok(writer.into_bytes())
}

pub fn lob_locator_is_temporary(locator: &[u8]) -> bool {
    locator
        .get(TNS_LOB_LOC_OFFSET_FLAG_1)
        .is_some_and(|flags| flags & TNS_LOB_LOC_FLAGS_ABSTRACT != 0)
        || locator
            .get(TNS_LOB_LOC_OFFSET_FLAG_4)
            .is_some_and(|flags| flags & TNS_LOB_LOC_FLAGS_TEMP != 0)
}

pub fn build_lob_free_temp_payload_with_seq(
    locators: &[Vec<u8>],
    seq_num: u8,
    ttc_field_version: u8,
) -> Result<Vec<u8>> {
    let total_size = locators.iter().try_fold(0u32, |total, locator| {
        let locator_len =
            u32::try_from(locator.len()).map_err(|_| ProtocolError::InvalidPacketLength {
                length: locator.len(),
                minimum: 0,
            })?;
        total
            .checked_add(locator_len)
            .ok_or(ProtocolError::PacketTooLarge { length: usize::MAX })
    })?;
    let mut writer = TtcWriter::new();
    writer.write_function_header(TNS_FUNC_LOB_OP, seq_num, ttc_field_version);
    writer.write_u8(1);
    writer.write_ub4(total_size);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub4(TNS_LOB_OP_FREE_TEMP | TNS_LOB_OP_ARRAY);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_ub8(0);
    writer.write_ub8(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_u8(0);
    writer.write_ub4(0);
    for locator in locators {
        writer.write_raw(locator);
    }
    Ok(writer.into_bytes())
}

pub fn parse_lob_read_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
    locator: &[u8],
) -> Result<LobReadResult> {
    parse_lob_read_response_with_limits(payload, capabilities, locator, ProtocolLimits::DEFAULT)
}

pub fn parse_lob_read_response_with_limits(
    payload: &[u8],
    capabilities: ClientCapabilities,
    locator: &[u8],
    limits: ProtocolLimits,
) -> Result<LobReadResult> {
    parse_lob_op_response_with_limits(payload, capabilities, locator, false, true, limits)
}

pub fn parse_lob_create_temp_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
) -> Result<LobReadResult> {
    parse_lob_create_temp_response_with_limits(payload, capabilities, ProtocolLimits::DEFAULT)
}

pub fn parse_lob_create_temp_response_with_limits(
    payload: &[u8],
    capabilities: ClientCapabilities,
    limits: ProtocolLimits,
) -> Result<LobReadResult> {
    parse_lob_op_response_with_limits(payload, capabilities, &[0; 40], true, false, limits)
}

pub fn parse_lob_write_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
    locator: &[u8],
) -> Result<LobReadResult> {
    parse_lob_write_response_with_limits(payload, capabilities, locator, ProtocolLimits::DEFAULT)
}

pub fn parse_lob_write_response_with_limits(
    payload: &[u8],
    capabilities: ClientCapabilities,
    locator: &[u8],
    limits: ProtocolLimits,
) -> Result<LobReadResult> {
    parse_lob_op_response_with_limits(payload, capabilities, locator, false, false, limits)
}

pub fn parse_lob_trim_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
    locator: &[u8],
) -> Result<LobReadResult> {
    parse_lob_trim_response_with_limits(payload, capabilities, locator, ProtocolLimits::DEFAULT)
}

pub fn parse_lob_trim_response_with_limits(
    payload: &[u8],
    capabilities: ClientCapabilities,
    locator: &[u8],
    limits: ProtocolLimits,
) -> Result<LobReadResult> {
    parse_lob_op_response_with_limits(payload, capabilities, locator, false, true, limits)
}

pub fn parse_lob_free_temp_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
    returned_parameter_len: usize,
) -> Result<()> {
    parse_lob_free_temp_response_with_limits(
        payload,
        capabilities,
        returned_parameter_len,
        ProtocolLimits::DEFAULT,
    )
}

pub fn parse_lob_free_temp_response_with_limits(
    payload: &[u8],
    capabilities: ClientCapabilities,
    returned_parameter_len: usize,
    limits: ProtocolLimits,
) -> Result<()> {
    limits.check_frame_bytes(returned_parameter_len)?;
    let mut reader = TtcReader::with_limits(payload, limits)?;
    while reader.remaining() > 0 {
        let message_type = reader.read_u8()?;
        match message_type {
            0 => {}
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
                    return Err(ProtocolError::ServerError(info.message));
                }
            }
            TNS_MSG_TYPE_PARAMETER => reader.skip(returned_parameter_len)?,
            _ => {
                return Err(ProtocolError::UnknownMessageType {
                    message_type,
                    position: reader.position().saturating_sub(1),
                })
            }
        }
    }
    Ok(())
}

/// Scan a plain function response (ping/commit/rollback) for a server error.
/// Unknown message types end the scan without error so payload shapes that
/// were previously tolerated (responses used to go unparsed) keep working.
/// Returns whether a server-side transaction is in progress, sampled from the
/// final end-of-call status bit (reference protocol.pyx `_process_call_status`).
pub fn parse_plain_function_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
) -> Result<bool> {
    parse_plain_function_response_with_limits(payload, capabilities, ProtocolLimits::DEFAULT)
}

pub fn parse_plain_function_response_with_limits(
    payload: &[u8],
    capabilities: ClientCapabilities,
    limits: ProtocolLimits,
) -> Result<bool> {
    let mut reader = TtcReader::with_limits(payload, limits)?;
    let mut txn_in_progress = false;
    while reader.remaining() > 0 {
        let message_type = reader.read_u8()?;
        match message_type {
            0 => {}
            TNS_MSG_TYPE_STATUS => {
                let call_status = reader.read_ub4()?;
                let _seq = reader.read_ub2()?;
                txn_in_progress = call_status & TNS_EOCS_FLAGS_TXN_IN_PROGRESS != 0;
            }
            TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK => {
                let _ = skip_server_side_piggyback(&mut reader)?;
            }
            TNS_MSG_TYPE_END_OF_RESPONSE => break,
            TNS_MSG_TYPE_ERROR => {
                let info = parse_server_error_info(&mut reader, capabilities.ttc_field_version)?;
                // The end-of-call ERROR (number 0 on success) carries the
                // end-of-call status; sample the transaction-in-progress bit.
                txn_in_progress = info.call_status & TNS_EOCS_FLAGS_TXN_IN_PROGRESS != 0;
                if info.number != 0 {
                    return Err(ProtocolError::ServerError(info.message));
                }
            }
            _ => break,
        }
    }
    Ok(txn_in_progress)
}

pub(crate) fn parse_lob_op_response_with_limits(
    payload: &[u8],
    capabilities: ClientCapabilities,
    locator: &[u8],
    is_create_temp: bool,
    read_amount: bool,
    limits: ProtocolLimits,
) -> Result<LobReadResult> {
    let mut reader = TtcReader::with_limits(payload, limits)?;
    let mut result = LobReadResult {
        locator: locator.to_vec(),
        ..LobReadResult::default()
    };
    while reader.remaining() > 0 {
        let message_type = reader.read_u8()?;
        match message_type {
            0 => {}
            TNS_MSG_TYPE_LOB_DATA => {
                result.data = reader.read_bytes()?;
            }
            TNS_MSG_TYPE_PARAMETER => {
                if !result.locator.is_empty() {
                    result.locator = reader.read_raw(result.locator.len())?.to_vec();
                }
                if is_create_temp {
                    let _charset = reader.read_ub2()?;
                    reader.skip(1)?;
                } else if read_amount {
                    let amount = reader.read_sb8()?;
                    if amount > 0 {
                        result.amount = amount as u64;
                    }
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
