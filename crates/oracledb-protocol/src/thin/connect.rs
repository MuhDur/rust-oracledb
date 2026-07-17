#![forbid(unsafe_code)]

use super::*;

/// Whether the connect data travels inline in the CONNECT packet. Longer
/// descriptors must be sent in a separate DATA packet right after the CONNECT
/// packet (reference messages/connect.pyx `ConnectMessage.send`); the caller
/// owns that follow-up send.
pub fn connect_data_fits_inline(connect_data: &str) -> bool {
    connect_data.len() <= TNS_MAX_CONNECT_DATA
}

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
    // Connect data above TNS_MAX_CONNECT_DATA is carried in a separate DATA
    // packet; the header still advertises the full length either way.
    if connect_data_fits_inline(connect_data) {
        writer.write_raw(connect_bytes);
    }
    Ok(writer.into_bytes())
}

pub fn parse_accept_payload(payload: &[u8]) -> Result<AcceptInfo> {
    let mut reader = TtcReader::new(payload);
    let protocol_version = reader.read_u16be()?;
    // Refuse below-floor servers BEFORE touching the rest of the payload
    // (reference messages/connect.pyx: `if protocol_version <
    // TNS_VERSION_MIN_ACCEPTED: ERR_SERVER_VERSION_NOT_SUPPORTED`). Pre-12.1
    // servers use an older, shorter ACCEPT layout — Oracle 11g (version 314)
    // sends 24 payload bytes, so parsing on would die with a misleading
    // "truncated TTC payload" instead of naming the real problem.
    if protocol_version < TNS_VERSION_MIN_ACCEPTED {
        return Err(ProtocolError::UnsupportedVersion {
            version: protocol_version,
            minimum: TNS_VERSION_MIN_ACCEPTED,
        });
    }
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

// Byte layout of FAST_AUTH_PREFIX_HEX (mirrors reference
// messages/fast_auth.pyx `FastAuthMessage._write_message`):
//   [0..4)    fast-auth envelope: msg type 34, version 1, char-conv flags
//   [4..23)   embedded protocol-negotiation message (msg type 1, version 6,
//             terminator, driver name string + NUL)
//   [23..29)  envelope glue: unused server charset/ncharset placeholders +
//             the pinned ttc field version byte
//   [29..)    embedded data-types message (msg type 2, UTF8 charsets,
//             encoding flags, compile/runtime caps, static type table)
// The classic (non-fast-auth) handshake sends the same two embedded messages
// as standalone round trips, so pre-23ai servers negotiate byte-identically
// to the reference implementation.
const FAST_AUTH_PROTOCOL_MSG_START: usize = 4;
const FAST_AUTH_PROTOCOL_MSG_END: usize = 23;
const FAST_AUTH_DATA_TYPES_MSG_START: usize = 29;

fn fast_auth_prefix_slice(start: usize, end: Option<usize>) -> Result<Vec<u8>> {
    let prefix = Vec::from_hex(FAST_AUTH_PREFIX_HEX)
        .map_err(|_| ProtocolError::TtcDecode("invalid static fast-auth prefix"))?;
    let slice = match end {
        Some(end) => prefix.get(start..end),
        None => prefix.get(start..),
    };
    slice
        .map(<[u8]>::to_vec)
        .ok_or(ProtocolError::TtcDecode("fast-auth prefix too short"))
}

/// Standalone TTC protocol-negotiation message (msg type 1) for the classic
/// pre-23ai handshake. Byte-identical to the copy embedded in the fast-auth
/// bundle (reference messages/protocol.pyx `ProtocolMessage._write_message`).
pub fn build_protocol_negotiation_payload() -> Result<Vec<u8>> {
    let payload = fast_auth_prefix_slice(
        FAST_AUTH_PROTOCOL_MSG_START,
        Some(FAST_AUTH_PROTOCOL_MSG_END),
    )?;
    debug_assert_eq!(payload.first(), Some(&TNS_MSG_TYPE_PROTOCOL));
    Ok(payload)
}

/// Standalone TTC data-types message (msg type 2) for the classic pre-23ai
/// handshake. Byte-identical to the copy embedded in the fast-auth bundle
/// (reference messages/data_types.pyx `DataTypesMessage._write_message`).
pub fn build_data_types_payload() -> Result<Vec<u8>> {
    let payload = fast_auth_prefix_slice(FAST_AUTH_DATA_TYPES_MSG_START, None)?;
    debug_assert_eq!(payload.first(), Some(&TNS_MSG_TYPE_DATA_TYPES));
    Ok(payload)
}

/// Standalone auth phase-one function message for the classic pre-23ai
/// handshake — the same message [`build_fast_auth_phase_one_payload`] appends
/// after the fast-auth bundle, sent on its own round trip instead.
pub fn build_auth_phase_one_payload(
    user: &str,
    program: &str,
    machine: &str,
    osuser: &str,
    terminal: &str,
    pid: u32,
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
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
    pop: Option<TokenPop<'_>>,
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
        pop,
    )?;
    Ok(out)
}

/// Builds the standalone classic-auth phase-two token message.
///
/// Unlike [`build_fast_auth_token_payload`], this does not prepend the
/// fast-auth envelope. Pre-23ai servers receive protocol negotiation and data
/// types as separate round trips, then this self-contained `AUTH_TOKEN`
/// phase-two message.
pub fn build_auth_phase_two_token_payload(
    user: &str,
    token: &str,
    driver_name: &str,
    version_num: u32,
    connect_string: &str,
    edition: Option<&str>,
    pop: Option<TokenPop<'_>>,
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    append_auth_phase_two_token(
        &mut out,
        user,
        token,
        driver_name,
        version_num,
        connect_string,
        edition,
        pop,
    )?;
    Ok(out)
}

pub fn build_function_payload(function_code: u8, ttc_field_version: u8) -> Vec<u8> {
    build_function_payload_with_seq(function_code, 1, ttc_field_version)
}

pub fn build_function_payload_with_seq(
    function_code: u8,
    seq_num: u8,
    ttc_field_version: u8,
) -> Vec<u8> {
    build_function_payload_with_seq_and_token(function_code, seq_num, 0, ttc_field_version)
}

/// Bare function message with an explicit pipeline token (messages/base.pyx
/// `_write_function_code` writes `ub8 token_num` for field version >= 23.1
/// ext 1; non-pipelined messages carry 0). On a pre-23.1-ext-1 connection the
/// token field does not exist on the wire at all; pipelining (nonzero tokens)
/// only happens on 23ai-negotiated connections, so no token is ever dropped.
pub fn build_function_payload_with_seq_and_token(
    function_code: u8,
    seq_num: u8,
    token_num: u64,
    ttc_field_version: u8,
) -> Vec<u8> {
    let mut writer = TtcWriter::new();
    writer.write_function_code_with_seq(function_code, seq_num);
    if version_gates::writes_pipeline_token(ttc_field_version) {
        writer.write_ub8(token_num);
    } else {
        debug_assert_eq!(
            token_num, 0,
            "pipeline tokens require a 23ai-negotiated connection"
        );
    }
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
    // The effective field version is the LOWER of what the server reports and
    // what this client supports (reference capabilities.pyx
    // `_adjust_for_server_compile_caps`: "if server < client: client =
    // server"). Taking the max would over-claim 23ai-era field formats against
    // pre-23ai servers.
    let ttc_field_version =
        server_ttc_field_version.min(ClientCapabilities::default().ttc_field_version);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classic_handshake_messages_slice_the_fast_auth_prefix() {
        let protocol = build_protocol_negotiation_payload().expect("protocol payload");
        assert_eq!(protocol[0], TNS_MSG_TYPE_PROTOCOL);
        assert_eq!(protocol[1], 6, "protocol version byte (8.1 and higher)");
        assert_eq!(protocol[2], 0, "array terminator");
        assert!(
            protocol.ends_with(b"python-oracledb\0"),
            "driver name string with NUL terminator"
        );

        let data_types = build_data_types_payload().expect("data types payload");
        assert_eq!(data_types[0], TNS_MSG_TYPE_DATA_TYPES);
        // UTF8 charset (873) little-endian for charset and ncharset.
        assert_eq!(&data_types[1..5], &[0x69, 0x03, 0x69, 0x03]);

        // The two standalone messages are exact slices of the fast-auth bundle,
        // so classic and fast-auth handshakes can never drift apart.
        let full = Vec::from_hex(FAST_AUTH_PREFIX_HEX).expect("prefix decodes");
        assert_eq!(full[0], TNS_MSG_TYPE_FAST_AUTH);
        assert_eq!(
            &full[FAST_AUTH_PROTOCOL_MSG_START..FAST_AUTH_PROTOCOL_MSG_END],
            &protocol[..]
        );
        assert_eq!(&full[FAST_AUTH_DATA_TYPES_MSG_START..], &data_types[..]);
    }

    #[test]
    fn classic_token_payload_is_the_fast_auth_phase_two_suffix() {
        let classic = build_auth_phase_two_token_payload(
            "scott",
            "token-secret",
            "rust-oracledb",
            4_000_000_000,
            "db.example.com/service",
            Some("MY_EDITION"),
            None,
        )
        .expect("classic token payload");
        let fast = build_fast_auth_token_payload(
            "scott",
            "token-secret",
            "rust-oracledb",
            4_000_000_000,
            "db.example.com/service",
            Some("MY_EDITION"),
            None,
        )
        .expect("fast token payload");
        let prefix = Vec::from_hex(FAST_AUTH_PREFIX_HEX).expect("prefix decodes");

        assert_eq!(&fast[..prefix.len()], prefix.as_slice());
        assert_eq!(&fast[prefix.len()..], classic.as_slice());
        assert_eq!(classic[0], TNS_MSG_TYPE_FUNCTION);
        assert_eq!(classic[1], TNS_FUNC_AUTH_PHASE_TWO);
        assert_eq!(
            classic[2], 1,
            "standalone phase two is the first TTC function"
        );
    }

    // ---- ACCEPT protocol-version gate boundary tests ----------------------
    //
    // parse_accept_payload mirrors three reference gates keyed on the ACCEPT's
    // protocol_version / protocol_options (references connect.pyx:65/75/111,
    // capabilities.pyx:126, protocol.pyx:262). The live matrix crosses these
    // (all live servers are >= 318, 23ai advertises end-of-response), but this
    // offline test pins each boundary exactly.

    /// A full (>= 318 layout) ACCEPT payload with a caller-controlled
    /// protocol_version field, so the same trailing flags2 bytes can be parsed
    /// on either side of the 318/319 gates.
    fn accept_bytes(version: u16, options: u16, flags2: u32) -> Vec<u8> {
        let mut w = TtcWriter::new();
        w.write_u16be(version); // protocol version
        w.write_u16be(options); // protocol options
        w.write_raw(&[0u8; 10]); // skip(10)
        w.write_u8(0); // flags1 (no NA_REQUIRED)
        w.write_raw(&[0u8; 9]); // skip(9)
        w.write_u32be(8192); // sdu
        w.write_raw(&[0u8; 5]); // skip(5) before flags2
        w.write_u32be(flags2); // flags2 (only read when version >= 318)
        w.into_bytes()
    }

    #[test]
    fn accept_parsing_gates_capabilities_on_protocol_version() {
        // protocol.pyx:262 — supports_oob is a plain flag on protocol_options,
        // independent of protocol_version.
        assert!(
            !parse_accept_payload(&accept_bytes(319, 0, 0))
                .unwrap()
                .supports_oob,
            "no CAN_RECV_ATTENTION bit => supports_oob false"
        );
        assert!(
            parse_accept_payload(&accept_bytes(319, TNS_GSO_CAN_RECV_ATTENTION, 0))
                .unwrap()
                .supports_oob,
            "CAN_RECV_ATTENTION bit => supports_oob true"
        );

        // connect.pyx:75 (MIN_OOB_CHECK, >= 318) — flags2 (and everything it
        // carries) is only read at/above 318. Same trailing bytes, version off
        // by one, must flip the derived capability.
        let flags2 = TNS_ACCEPT_FLAG_FAST_AUTH | TNS_ACCEPT_FLAG_CHECK_OOB;
        assert!(
            !parse_accept_payload(&accept_bytes(317, 0, flags2))
                .unwrap()
                .supports_fast_auth,
            "below 318 flags2 is not read"
        );
        let at_318 = parse_accept_payload(&accept_bytes(318, 0, flags2)).unwrap();
        assert!(at_318.supports_fast_auth, "at 318 flags2 is read");
        assert!(
            at_318.supports_oob_check,
            "at 318 the CHECK_OOB flag is read"
        );

        // capabilities.pyx:126 (MIN_END_OF_RESPONSE) — end-of-response requires
        // BOTH protocol_version >= 319 AND the flag. Prove the version gate and
        // the flag gate independently.
        assert!(
            !parse_accept_payload(&accept_bytes(318, 0, TNS_ACCEPT_FLAG_HAS_END_OF_RESPONSE))
                .unwrap()
                .supports_end_of_response,
            "318 < 319: no end-of-response even with the flag"
        );
        assert!(
            parse_accept_payload(&accept_bytes(319, 0, TNS_ACCEPT_FLAG_HAS_END_OF_RESPONSE))
                .unwrap()
                .supports_end_of_response,
            "319 + flag: end-of-response negotiated"
        );
        assert!(
            !parse_accept_payload(&accept_bytes(319, 0, 0))
                .unwrap()
                .supports_end_of_response,
            "319 without the flag: no end-of-response"
        );
    }
}
