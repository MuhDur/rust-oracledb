#![forbid(unsafe_code)]

use super::*;

pub fn build_connect_packet_payload(connect_data: &str, sdu: u16) -> Result<Vec<u8>> {
    let connect_bytes = connect_data.as_bytes();
    let connect_len =
        u16::try_from(connect_bytes.len()).map_err(|_| ProtocolError::PacketTooLarge {
            length: connect_bytes.len(),
        })?;

    let mut writer = TtcWriter::new();
    writer.write_u16be(TNS_VERSION_DESIRED);
    writer.write_u16be(TNS_VERSION_MIN);
    writer.write_u16be(TNS_GSO_DONT_CARE);
    writer.write_u16be(sdu);
    writer.write_u16be(sdu);
    writer.write_u16be(TNS_PROTOCOL_CHARACTERISTICS);
    writer.write_u16be(0);
    writer.write_u16be(1);
    writer.write_u16be(connect_len);
    writer.write_u16be(74);
    writer.write_u32be(0);
    let nsi_flags = TNS_NSI_SUPPORT_SECURITY_RENEG | TNS_NSI_DISABLE_NA;
    writer.write_u8(nsi_flags);
    writer.write_u8(nsi_flags);
    writer.write_u64be(0);
    writer.write_u64be(0);
    writer.write_u64be(0);
    writer.write_u32be(u32::from(sdu));
    writer.write_u32be(u32::from(sdu));
    writer.write_u32be(0);
    writer.write_u32be(0);
    writer.write_raw(connect_bytes);
    Ok(writer.into_bytes())
}

pub fn parse_accept_payload(payload: &[u8]) -> Result<AcceptInfo> {
    let mut reader = TtcReader::new(payload);
    let protocol_version = reader.read_u16be()?;
    let protocol_options = reader.read_u16be()?;
    reader.skip(10)?;
    let flags1 = reader.read_u8()?;
    if has_u8_flag(flags1, TNS_NSI_NA_REQUIRED) {
        return Err(ProtocolError::UnsupportedFeature(
            "Native Network Encryption and Data Integrity",
        ));
    }
    reader.skip(9)?;
    let sdu = reader.read_u32be()?;
    let mut flags2 = 0;
    if protocol_version >= 318 {
        reader.skip(5)?;
        flags2 = reader.read_u32be()?;
    }

    Ok(AcceptInfo {
        protocol_version,
        protocol_options,
        sdu,
        supports_fast_auth: has_u32_flag(flags2, TNS_ACCEPT_FLAG_FAST_AUTH),
        supports_oob_check: has_u32_flag(flags2, TNS_ACCEPT_FLAG_CHECK_OOB),
        // Reference: Capabilities.supports_oob = protocol_options &
        // TNS_GSO_CAN_RECV_ATTENTION (capabilities.pyx:121).
        supports_oob: protocol_options & TNS_GSO_CAN_RECV_ATTENTION != 0,
        supports_end_of_response: protocol_version >= 319
            && has_u32_flag(flags2, TNS_ACCEPT_FLAG_HAS_END_OF_RESPONSE),
    })
}

pub fn build_fast_auth_phase_one_payload(
    user: &str,
    program: &str,
    machine: &str,
    osuser: &str,
    terminal: &str,
    pid: u32,
) -> Result<Vec<u8>> {
    let mut out = Vec::from_hex(FAST_AUTH_PREFIX_HEX)
        .map_err(|_| ProtocolError::TtcDecode("invalid static fast-auth prefix"))?;
    append_auth_phase_one(&mut out, user, program, machine, osuser, terminal, pid)?;
    Ok(out)
}

/// Fast-auth bundle for **token authentication**: the same static
/// protocol/data-types prefix as [`build_fast_auth_phase_one_payload`], but with
/// a phase-two `AUTH_TOKEN` message appended (no verifier round-trip). The caller
/// sends this once and reads a single auth response.
pub fn build_fast_auth_token_payload(
    user: &str,
    token: &str,
    driver_name: &str,
    version_num: u32,
    connect_string: &str,
    edition: Option<&str>,
) -> Result<Vec<u8>> {
    let mut out = Vec::from_hex(FAST_AUTH_PREFIX_HEX)
        .map_err(|_| ProtocolError::TtcDecode("invalid static fast-auth prefix"))?;
    append_auth_phase_two_token(
        &mut out,
        user,
        token,
        driver_name,
        version_num,
        connect_string,
        edition,
    )?;
    Ok(out)
}

pub fn build_function_payload(function_code: u8) -> Vec<u8> {
    build_function_payload_with_seq(function_code, 1)
}

pub fn build_function_payload_with_seq(function_code: u8, seq_num: u8) -> Vec<u8> {
    build_function_payload_with_seq_and_token(function_code, seq_num, 0)
}

/// Bare function message with an explicit pipeline token (messages/base.pyx
/// `_write_function_code` writes `ub8 token_num` for field version >= 23.1
/// ext 1; non-pipelined messages carry 0).
pub fn build_function_payload_with_seq_and_token(
    function_code: u8,
    seq_num: u8,
    token_num: u64,
) -> Vec<u8> {
    let mut writer = TtcWriter::new();
    writer.write_function_code_with_seq(function_code, seq_num);
    writer.write_ub8(token_num);
    writer.into_bytes()
}

pub(crate) fn skip_protocol_message(
    reader: &mut TtcReader<'_>,
) -> Result<Option<ClientCapabilities>> {
    let _server_version = reader.read_u8()?;
    reader.skip(1)?;
    loop {
        if reader.read_u8()? == 0 {
            break;
        }
    }
    let charset_id = reader.read_u16le()?;
    let _server_flags = reader.read_u8()?;
    let num_elem = reader.read_u16le()?;
    reader.skip(usize::from(num_elem) * 5)?;
    let fdo_len = reader.read_u16be()?;
    reader.skip(usize::from(fdo_len))?;
    let compile_caps = reader.read_bytes()?;
    let runtime_caps = reader.read_bytes()?;
    let Some(compile_caps) = compile_caps else {
        return Ok(None);
    };
    let server_ttc_field_version = compile_caps
        .get(TNS_CCAP_FIELD_VERSION)
        .copied()
        .unwrap_or_else(|| ClientCapabilities::default().ttc_field_version);
    let ttc_field_version =
        server_ttc_field_version.max(ClientCapabilities::default().ttc_field_version);
    let max_string_size = if runtime_caps
        .as_deref()
        .and_then(|caps| caps.get(TNS_RCAP_TTC))
        .is_some_and(|flags| flags & TNS_RCAP_TTC_32K != 0)
    {
        32_767
    } else {
        4_000
    };
    Ok(Some(ClientCapabilities {
        ttc_field_version,
        max_string_size,
        charset_id,
    }))
}

pub(crate) fn skip_data_types_response(reader: &mut TtcReader<'_>) -> Result<()> {
    loop {
        let data_type = reader.read_u16be()?;
        if data_type == 0 {
            break;
        }
        let conv_data_type = reader.read_u16be()?;
        if conv_data_type != 0 {
            reader.skip(4)?;
        }
    }
    Ok(())
}
