#![forbid(unsafe_code)]

use super::*;

pub fn append_auth_phase_one(
    out: &mut Vec<u8>,
    user: &str,
    program: &str,
    machine: &str,
    osuser: &str,
    terminal: &str,
    pid: u32,
) -> Result<()> {
    let mut writer = TtcWriter::new();
    writer.write_function_code(TNS_FUNC_AUTH_PHASE_ONE);
    write_auth_header(&mut writer, user, TNS_AUTH_MODE_LOGON, 5)?;
    write_key_value(&mut writer, "AUTH_TERMINAL", terminal, 0)?;
    write_key_value(&mut writer, "AUTH_PROGRAM_NM", program, 0)?;
    write_key_value(&mut writer, "AUTH_MACHINE", machine, 0)?;
    write_key_value(&mut writer, "AUTH_PID", &pid.to_string(), 0)?;
    write_key_value(&mut writer, "AUTH_SID", osuser, 0)?;
    out.extend_from_slice(&writer.into_bytes());
    Ok(())
}

/// Appends the auth message for **token authentication** (OCI IAM database
/// token / OAuth2) to the fast-auth bundle. Unlike password auth there is no
/// verifier challenge: the reference sends auth phase TWO directly, carrying the
/// token in `AUTH_TOKEN` with no `AUTH_SESSKEY`/`AUTH_PASSWORD` and auth mode
/// `LOGON` (no `WITH_PASSWORD`); it never resends (messages/auth.pyx
/// `_set_params`/`_write_message`, messages/fast_auth.pyx). Because this message
/// lives inside the fast-auth bundle (ttc field version 19.1), the function code
/// carries no `ub8` token-num — exactly like [`append_auth_phase_one`].
pub fn append_auth_phase_two_token(
    out: &mut Vec<u8>,
    user: &str,
    token: &str,
    driver_name: &str,
    version_num: u32,
    connect_string: &str,
) -> Result<()> {
    let mut writer = TtcWriter::new();
    writer.write_function_code(TNS_FUNC_AUTH_PHASE_TWO);
    // AUTH_TOKEN + the four mandatory session pairs, plus AUTH_CONNECT_STRING.
    let mut num_pairs = 5u32;
    if !connect_string.is_empty() {
        num_pairs += 1;
    }
    write_auth_header(&mut writer, user, TNS_AUTH_MODE_LOGON, num_pairs)?;
    write_key_value(&mut writer, "AUTH_TOKEN", token, 0)?;
    write_key_value(&mut writer, "SESSION_CLIENT_CHARSET", "873", 0)?;
    write_key_value(&mut writer, "SESSION_CLIENT_DRIVER_NAME", driver_name, 0)?;
    write_key_value(
        &mut writer,
        "SESSION_CLIENT_VERSION",
        &version_num.to_string(),
        0,
    )?;
    write_key_value(
        &mut writer,
        "AUTH_ALTER_SESSION",
        "ALTER SESSION SET TIME_ZONE='+00:00'\0",
        1,
    )?;
    if !connect_string.is_empty() {
        write_key_value(&mut writer, "AUTH_CONNECT_STRING", connect_string, 0)?;
    }
    out.extend_from_slice(&writer.into_bytes());
    Ok(())
}

pub fn build_auth_phase_two_payload(
    user: &str,
    encrypted: &crate::crypto::EncryptedPassword,
    driver_name: &str,
    version_num: u32,
    connect_string: &str,
) -> Result<Vec<u8>> {
    build_auth_phase_two_payload_with_seq(
        user,
        encrypted,
        driver_name,
        version_num,
        connect_string,
        1,
    )
}

pub fn build_auth_phase_two_payload_with_seq(
    user: &str,
    encrypted: &crate::crypto::EncryptedPassword,
    driver_name: &str,
    version_num: u32,
    connect_string: &str,
    seq_num: u8,
) -> Result<Vec<u8>> {
    build_auth_phase_two_payload_with_context_with_seq(
        user,
        encrypted,
        driver_name,
        version_num,
        connect_string,
        seq_num,
        &[],
    )
}

pub fn build_auth_phase_two_payload_with_context_with_seq(
    user: &str,
    encrypted: &crate::crypto::EncryptedPassword,
    driver_name: &str,
    version_num: u32,
    connect_string: &str,
    seq_num: u8,
    app_context: &[(String, String, String)],
) -> Result<Vec<u8>> {
    build_auth_phase_two_payload_with_proxy_with_seq(
        user,
        encrypted,
        driver_name,
        version_num,
        connect_string,
        seq_num,
        app_context,
        None,
        None,
    )
}

/// Phase-two auth payload with optional proxy authentication: the reference
/// writes `PROXY_CLIENT_NAME` as the first key/value pair when the connect
/// user is of the form `user[proxy_user]` (messages/auth.pyx).
#[allow(clippy::too_many_arguments)]
pub fn build_auth_phase_two_payload_with_proxy_with_seq(
    user: &str,
    encrypted: &crate::crypto::EncryptedPassword,
    driver_name: &str,
    version_num: u32,
    connect_string: &str,
    seq_num: u8,
    app_context: &[(String, String, String)],
    proxy_user: Option<&str>,
    edition: Option<&str>,
) -> Result<Vec<u8>> {
    let mut writer = TtcWriter::new();
    writer.write_function_code_with_seq(TNS_FUNC_AUTH_PHASE_TWO, seq_num);
    writer.write_ub8(0);
    let mut num_pairs = 6u32;
    if encrypted.speedy_key.is_some() {
        num_pairs += 1;
    }
    if proxy_user.is_some() {
        num_pairs += 1;
    }
    if !connect_string.is_empty() {
        num_pairs += 1;
    }
    if edition.is_some() {
        num_pairs += 1;
    }
    let app_context_pairs =
        app_context
            .len()
            .checked_mul(3)
            .ok_or(ProtocolError::InvalidPacketLength {
                length: app_context.len(),
                minimum: 0,
            })?;
    num_pairs +=
        u32::try_from(app_context_pairs).map_err(|_| ProtocolError::InvalidPacketLength {
            length: app_context.len(),
            minimum: 0,
        })?;
    write_auth_header(
        &mut writer,
        user,
        TNS_AUTH_MODE_LOGON | TNS_AUTH_MODE_WITH_PASSWORD,
        num_pairs,
    )?;
    if let Some(proxy_user) = proxy_user {
        write_key_value(&mut writer, "PROXY_CLIENT_NAME", proxy_user, 0)?;
    }
    write_key_value(&mut writer, "AUTH_SESSKEY", &encrypted.session_key, 1)?;
    if let Some(speedy_key) = &encrypted.speedy_key {
        write_key_value(&mut writer, "AUTH_PBKDF2_SPEEDY_KEY", speedy_key, 0)?;
    }
    write_key_value(&mut writer, "AUTH_PASSWORD", &encrypted.password, 0)?;
    write_key_value(&mut writer, "SESSION_CLIENT_CHARSET", "873", 0)?;
    write_key_value(&mut writer, "SESSION_CLIENT_DRIVER_NAME", driver_name, 0)?;
    write_key_value(
        &mut writer,
        "SESSION_CLIENT_VERSION",
        &version_num.to_string(),
        0,
    )?;
    write_key_value(
        &mut writer,
        "AUTH_ALTER_SESSION",
        "ALTER SESSION SET TIME_ZONE='+00:00'\0",
        1,
    )?;
    // Edition-Based Redefinition: select the session edition during auth, exactly
    // as the reference does (messages/auth.pyx writes `AUTH_ORA_EDITION` when
    // `params.edition is not None`). Applied before any user SQL.
    if let Some(edition) = edition {
        write_key_value(&mut writer, "AUTH_ORA_EDITION", edition, 0)?;
    }
    for (namespace, name, value) in app_context {
        write_key_value(&mut writer, "AUTH_APPCTX_NSPACE\0", namespace, 0)?;
        write_key_value(&mut writer, "AUTH_APPCTX_ATTR\0", name, 0)?;
        write_key_value(&mut writer, "AUTH_APPCTX_VALUE\0", value, 0)?;
    }
    if !connect_string.is_empty() {
        write_key_value(&mut writer, "AUTH_CONNECT_STRING", connect_string, 0)?;
    }
    Ok(writer.into_bytes())
}

/// Change-password payload: an AUTH_PHASE_TWO message carrying only the
/// combo-key-encrypted old/new passwords (reference
/// connection.pyx `_create_change_password_message` + messages/auth.pyx
/// `_write_message`: auth mode WITH_PASSWORD|CHANGE_PASSWORD, two pairs).
pub fn build_change_password_payload_with_seq(
    user: &str,
    encoded_password: &str,
    encoded_newpassword: &str,
    seq_num: u8,
) -> Result<Vec<u8>> {
    let mut writer = TtcWriter::new();
    writer.write_function_code_with_seq(TNS_FUNC_AUTH_PHASE_TWO, seq_num);
    writer.write_ub8(0);
    write_auth_header(
        &mut writer,
        user,
        TNS_AUTH_MODE_WITH_PASSWORD | TNS_AUTH_MODE_CHANGE_PASSWORD,
        2,
    )?;
    write_key_value(&mut writer, "AUTH_PASSWORD", encoded_password, 0)?;
    write_key_value(&mut writer, "AUTH_NEWPASSWORD", encoded_newpassword, 0)?;
    Ok(writer.into_bytes())
}

pub fn parse_auth_response(payload: &[u8]) -> Result<AuthResponse> {
    let mut reader = TtcReader::new(payload);
    let mut response = AuthResponse::default();
    while reader.remaining() > 0 {
        let message_type = reader.read_u8()?;
        match message_type {
            TNS_MSG_TYPE_PROTOCOL => {
                if let Some(capabilities) = skip_protocol_message(&mut reader)? {
                    response.capabilities = Some(capabilities);
                }
            }
            TNS_MSG_TYPE_DATA_TYPES => skip_data_types_response(&mut reader)?,
            TNS_MSG_TYPE_PARAMETER => {
                let mut parsed = parse_return_parameters(&mut reader)?;
                response.session_data.append(&mut parsed.session_data);
                if parsed.verifier_type.is_some() {
                    response.verifier_type = parsed.verifier_type;
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
                if let Some(message) = parse_server_error(&mut reader, 13)? {
                    return Err(ProtocolError::ServerError(message));
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
    Ok(response)
}

pub(crate) fn write_auth_header(
    writer: &mut TtcWriter,
    user: &str,
    auth_mode: u32,
    num_pairs: u32,
) -> Result<()> {
    let user_bytes = user.as_bytes();
    writer.write_u8(u8::from(!user_bytes.is_empty()));
    writer.write_ub4(u32::try_from(user_bytes.len()).map_err(|_| {
        ProtocolError::InvalidPacketLength {
            length: user_bytes.len(),
            minimum: 0,
        }
    })?);
    writer.write_ub4(auth_mode);
    writer.write_u8(1);
    writer.write_ub4(num_pairs);
    writer.write_u8(1);
    writer.write_u8(1);
    if !user_bytes.is_empty() {
        writer.write_bytes_with_length(user_bytes)?;
    }
    Ok(())
}

pub(crate) fn write_key_value(
    writer: &mut TtcWriter,
    key: &str,
    value: &str,
    flags: u32,
) -> Result<()> {
    writer.write_str_two_lengths(key)?;
    writer.write_str_two_lengths(value)?;
    writer.write_ub4(flags);
    Ok(())
}

pub(crate) fn parse_return_parameters(reader: &mut TtcReader<'_>) -> Result<AuthResponse> {
    let num_params = reader.read_ub2()?;
    let mut response = AuthResponse::default();
    for _ in 0..num_params {
        let key = reader
            .read_string_with_length()?
            .ok_or(ProtocolError::TtcDecode("missing auth response key"))?;
        let value = reader.read_string_with_length()?.unwrap_or_default();
        if key == "AUTH_VFR_DATA" {
            response.verifier_type = Some(reader.read_ub4()?);
        } else {
            let _flags = reader.read_ub4()?;
        }
        response.session_data.insert(key, value);
    }
    Ok(response)
}

#[cfg(test)]
mod token_auth_tests {
    use super::*;

    /// Decode an Oracle `ub4` at `*pos`, advancing it (see `WriteBuffer::write_ub4`).
    fn read_ub4(bytes: &[u8], pos: &mut usize) -> u32 {
        let len = bytes[*pos] as usize;
        *pos += 1;
        let mut value = 0u32;
        for _ in 0..len {
            value = (value << 8) | u32::from(bytes[*pos]);
            *pos += 1;
        }
        value
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    /// The token auth message must encode the token as `AUTH_TOKEN`, in auth mode
    /// `LOGON` (never `WITH_PASSWORD`), with no `AUTH_SESSKEY`/`AUTH_PASSWORD` and
    /// the correct key/value-pair count. This is the deterministic "cassette" that
    /// pins the wire format against the reference (messages/auth.pyx).
    #[test]
    fn token_message_carries_auth_token_not_password() {
        let mut out = Vec::new();
        append_auth_phase_two_token(
            &mut out,
            "scott",
            "HEADER.PAYLOAD.SIG",
            "drv",
            300_000_000,
            "cs",
        )
        .unwrap();

        // Function header: TTC function message, phase two, then the auth header.
        assert_eq!(out[0], TNS_MSG_TYPE_FUNCTION);
        assert_eq!(out[1], TNS_FUNC_AUTH_PHASE_TWO);
        // out[2] is the sequence byte; out[3] is the has_user flag.
        assert_eq!(out[3], 1, "user is present");
        let mut pos = 4;
        assert_eq!(read_ub4(&out, &mut pos), 5, "user length = len(\"scott\")");
        assert_eq!(
            read_ub4(&out, &mut pos),
            TNS_AUTH_MODE_LOGON,
            "token auth uses LOGON only — never the WITH_PASSWORD bit"
        );
        assert_eq!(out[pos], 1); // authivl pointer
        pos += 1;
        assert_eq!(
            read_ub4(&out, &mut pos),
            6,
            "AUTH_TOKEN + 4 session pairs + AUTH_CONNECT_STRING"
        );

        assert!(contains(&out, b"AUTH_TOKEN"));
        assert!(
            contains(&out, b"HEADER.PAYLOAD.SIG"),
            "the token value is sent"
        );
        assert!(contains(&out, b"AUTH_CONNECT_STRING"));
        assert!(
            !contains(&out, b"AUTH_PASSWORD") && !contains(&out, b"AUTH_SESSKEY"),
            "token auth must not send any password material"
        );
    }

    /// Without a connect string the pair count drops to exactly the token + the
    /// four mandatory session pairs.
    #[test]
    fn token_message_pair_count_without_connect_string() {
        let mut out = Vec::new();
        append_auth_phase_two_token(&mut out, "u", "tok", "drv", 1, "").unwrap();
        let mut pos = 4;
        let _user_len = read_ub4(&out, &mut pos);
        let _auth_mode = read_ub4(&out, &mut pos);
        pos += 1; // authivl pointer
        assert_eq!(read_ub4(&out, &mut pos), 5, "AUTH_TOKEN + 4 session pairs");
        assert!(!contains(&out, b"AUTH_CONNECT_STRING"));
    }
}
