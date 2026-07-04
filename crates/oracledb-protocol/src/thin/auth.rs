#![forbid(unsafe_code)]

use super::*;
use crate::wire::ProtocolLimits;

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
    edition: Option<&str>,
) -> Result<()> {
    append_auth_phase_two_token_inner(
        out,
        user,
        token,
        driver_name,
        version_num,
        connect_string,
        edition,
        None,
    )
}

/// The signing string covered by the IAM request signature, in the exact layout
/// the reference builds inside `AuthMessage._write_message` (messages/auth.pyx):
/// three pseudo-headers — `date`, `(request-target)` and `host` — joined by
/// single `\n` separators (no trailing newline). `date` must be an RFC 1123 GMT
/// timestamp (`%a, %d %b %Y %H:%M:%S GMT`), `request_target` is the connection's
/// service name, and `host` is `host:port` of the live transport.
///
/// This is a pure function so the caller signs exactly the bytes it sends and so
/// the layout can be pinned by a deterministic test without a clock.
pub fn iam_signing_string(date: &str, request_target: &str, host: &str, port: u16) -> String {
    format!("date: {date}\n(request-target): {request_target}\nhost: {host}:{port}")
}

/// The IAM (instance/resource-principal) variant of [`append_auth_phase_two_token`]:
/// in addition to the token, it writes the `AUTH_HEADER` (the signing string) and
/// `AUTH_SIGNATURE` (base64 RSA signature) key/value pairs and OR's
/// `TNS_AUTH_MODE_IAM_TOKEN` into the auth mode (reference messages/auth.pyx: the
/// `self.private_key is not None` branch and `auth_mode |= TNS_AUTH_MODE_IAM_TOKEN`).
/// `auth_header` and `auth_signature` are produced by [`iam_signing_string`] and
/// [`crate::crypto::iam_signature`].
#[allow(clippy::too_many_arguments)]
pub fn append_auth_phase_two_token_iam(
    out: &mut Vec<u8>,
    user: &str,
    token: &str,
    driver_name: &str,
    version_num: u32,
    connect_string: &str,
    edition: Option<&str>,
    auth_header: &str,
    auth_signature: &str,
) -> Result<()> {
    append_auth_phase_two_token_inner(
        out,
        user,
        token,
        driver_name,
        version_num,
        connect_string,
        edition,
        Some((auth_header, auth_signature)),
    )
}

#[allow(clippy::too_many_arguments)]
fn append_auth_phase_two_token_inner(
    out: &mut Vec<u8>,
    user: &str,
    token: &str,
    driver_name: &str,
    version_num: u32,
    connect_string: &str,
    edition: Option<&str>,
    iam_signature: Option<(&str, &str)>,
) -> Result<()> {
    let mut writer = TtcWriter::new();
    writer.write_function_code(TNS_FUNC_AUTH_PHASE_TWO);
    // AUTH_TOKEN + the four mandatory session pairs, plus the optional
    // AUTH_HEADER/AUTH_SIGNATURE pair, AUTH_ORA_EDITION and AUTH_CONNECT_STRING.
    let mut num_pairs = 5u32;
    if iam_signature.is_some() {
        num_pairs += 2;
    }
    if edition.is_some() {
        num_pairs += 1;
    }
    if !connect_string.is_empty() {
        num_pairs += 1;
    }
    // A present private key adds the IAM_TOKEN bit to the LOGON mode
    // (reference `auth_mode |= TNS_AUTH_MODE_IAM_TOKEN`).
    let auth_mode = if iam_signature.is_some() {
        TNS_AUTH_MODE_LOGON | TNS_AUTH_MODE_IAM_TOKEN
    } else {
        TNS_AUTH_MODE_LOGON
    };
    write_auth_header(&mut writer, user, auth_mode, num_pairs)?;
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
    // The IAM signature pair sits between AUTH_ALTER_SESSION and AUTH_ORA_EDITION,
    // exactly as the reference writes it (messages/auth.pyx `_write_message`).
    if let Some((auth_header, auth_signature)) = iam_signature {
        write_key_value(&mut writer, "AUTH_HEADER", auth_header, 0)?;
        write_key_value(&mut writer, "AUTH_SIGNATURE", auth_signature, 0)?;
    }
    // Edition-Based Redefinition applies to token auth too — the reference writes
    // AUTH_ORA_EDITION after AUTH_ALTER_SESSION on both auth paths (messages/auth.pyx
    // `_write_message`); omitting it here silently ran token sessions under the
    // default edition.
    if let Some(edition) = edition {
        write_key_value(&mut writer, "AUTH_ORA_EDITION", edition, 0)?;
    }
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
        ClientCapabilities::default().ttc_field_version,
    )
}

/// Phase-two auth payload with optional proxy authentication: the reference
/// writes `PROXY_CLIENT_NAME` as the first key/value pair when the connect
/// user is of the form `user[proxy_user]` (messages/auth.pyx).
///
/// `ttc_field_version` is the field version negotiated with THIS server: the
/// ub8 pipeline-token in the function header is a 23.1+ field (reference
/// messages/base.pyx `_write_function_code`); a pre-23ai server parses the
/// stray byte as part of the auth header and breaks the connection with a
/// MARKER (observed live against Oracle XE 18c).
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
    ttc_field_version: u8,
) -> Result<Vec<u8>> {
    let mut writer = TtcWriter::new();
    writer.write_function_code_with_seq(TNS_FUNC_AUTH_PHASE_TWO, seq_num);
    if ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1_EXT_1 {
        writer.write_ub8(0);
    }
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
    ttc_field_version: u8,
) -> Result<Vec<u8>> {
    let mut writer = TtcWriter::new();
    writer.write_function_code_with_seq(TNS_FUNC_AUTH_PHASE_TWO, seq_num);
    if ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1_EXT_1 {
        writer.write_ub8(0);
    }
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
    parse_auth_response_with_limits(payload, ProtocolLimits::DEFAULT)
}

/// Whether an accumulated classic (pre-END_OF_RESPONSE) connect-phase response
/// is complete.
///
/// Servers that did not negotiate END_OF_RESPONSE framing (protocol version
/// below 319, i.e. everything before 23ai) never set the end-of-response DATA
/// flag; the response instead ends when its *terminal message* has been read
/// (reference messages/base.pyx `Message.process`: loop until
/// `end_of_response`). The terminal messages for the connect-phase round trips
/// are:
///
/// - protocol negotiation (msg 1): ends the response once processed
///   (messages/protocol.pyx),
/// - data types (msg 2): same (messages/data_types.pyx),
/// - STATUS (msg 9): ends any response when END_OF_RESPONSE framing is off
///   (messages/base.pyx),
/// - ERROR (msg 4): carries the failure that the real parse will surface.
///
/// Returns `Ok(false)` when the payload runs out mid-message — the caller must
/// read the next DATA packet and try again, exactly like the reference's
/// `ReadBuffer` blocking for more packets mid-parse. Unknown message types
/// propagate as errors.
pub fn classic_connect_response_is_complete(
    payload: &[u8],
    limits: ProtocolLimits,
) -> Result<bool> {
    let Ok(mut reader) = TtcReader::with_limits(payload, limits) else {
        return Ok(false);
    };
    while reader.remaining() > 0 {
        let message_type = reader.read_u8()?;
        let terminal = match message_type {
            TNS_MSG_TYPE_PROTOCOL => match skip_protocol_message(&mut reader) {
                Ok(_) => true,
                Err(ProtocolError::TtcDecode(_)) => return Ok(false),
                Err(err) => return Err(err),
            },
            TNS_MSG_TYPE_DATA_TYPES => match skip_data_types_response(&mut reader) {
                Ok(()) => true,
                Err(ProtocolError::TtcDecode(_)) => return Ok(false),
                Err(err) => return Err(err),
            },
            TNS_MSG_TYPE_PARAMETER => match parse_return_parameters(&mut reader) {
                Ok(_) => false,
                Err(ProtocolError::TtcDecode(_)) => return Ok(false),
                Err(err) => return Err(err),
            },
            TNS_MSG_TYPE_STATUS => {
                let complete = reader.read_ub4().and_then(|_| reader.read_ub2());
                match complete {
                    Ok(_) => true,
                    Err(ProtocolError::TtcDecode(_)) => return Ok(false),
                    Err(err) => return Err(err),
                }
            }
            TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK => match skip_server_side_piggyback(&mut reader) {
                Ok(_) => false,
                Err(ProtocolError::TtcDecode(_)) => return Ok(false),
                Err(err) => return Err(err),
            },
            TNS_MSG_TYPE_END_OF_RESPONSE => true,
            TNS_MSG_TYPE_ERROR => match parse_server_error(&mut reader, 13) {
                Ok(_) | Err(ProtocolError::ServerError(_)) => true,
                Err(ProtocolError::TtcDecode(_)) => return Ok(false),
                Err(err) => return Err(err),
            },
            _ => {
                return Err(ProtocolError::UnknownMessageType {
                    message_type,
                    position: reader.position().saturating_sub(1),
                })
            }
        };
        if terminal {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn parse_auth_response_with_limits(
    payload: &[u8],
    limits: ProtocolLimits,
) -> Result<AuthResponse> {
    let mut reader = TtcReader::with_limits(payload, limits)?;
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
    reader
        .limits()
        .check_length_prefixed_elements(usize::from(num_params))?;
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
            None,
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
        append_auth_phase_two_token(&mut out, "u", "tok", "drv", 1, "", None).unwrap();
        let mut pos = 4;
        let _user_len = read_ub4(&out, &mut pos);
        let _auth_mode = read_ub4(&out, &mut pos);
        pos += 1; // authivl pointer
        assert_eq!(read_ub4(&out, &mut pos), 5, "AUTH_TOKEN + 4 session pairs");
        assert!(!contains(&out, b"AUTH_CONNECT_STRING"));
    }

    /// Edition-Based Redefinition must reach the server on the token path too:
    /// `AUTH_ORA_EDITION` is written and counted, exactly as on the password path
    /// (regression guard for the 0.2.0 bug where token auth dropped the edition).
    #[test]
    fn token_message_carries_edition() {
        let mut out = Vec::new();
        append_auth_phase_two_token(&mut out, "u", "tok", "drv", 1, "", Some("E_TEST")).unwrap();
        let mut pos = 4;
        let _user_len = read_ub4(&out, &mut pos);
        let _auth_mode = read_ub4(&out, &mut pos);
        pos += 1; // authivl pointer
        assert_eq!(
            read_ub4(&out, &mut pos),
            6,
            "AUTH_TOKEN + 4 session pairs + AUTH_ORA_EDITION"
        );
        assert!(contains(&out, b"AUTH_ORA_EDITION"));
        assert!(contains(&out, b"E_TEST"), "the edition value is sent");

        // With both an edition and a connect string the count rises to 7.
        let mut out2 = Vec::new();
        append_auth_phase_two_token(&mut out2, "u", "tok", "drv", 1, "cs", Some("E_TEST")).unwrap();
        let mut p = 4;
        let _ = read_ub4(&out2, &mut p);
        let _ = read_ub4(&out2, &mut p);
        p += 1;
        assert_eq!(read_ub4(&out2, &mut p), 7, "+ AUTH_CONNECT_STRING");
    }

    /// The signing string is the reference header layout: three pseudo-headers
    /// joined by single `\n`, no trailing newline (messages/auth.pyx).
    #[test]
    fn iam_signing_string_layout() {
        let s = iam_signing_string(
            "Wed, 04 Jul 2026 12:34:56 GMT",
            "salesdb_high",
            "adb.us-ashburn-1.oraclecloud.com",
            1522,
        );
        assert_eq!(
            s,
            "date: Wed, 04 Jul 2026 12:34:56 GMT\n\
             (request-target): salesdb_high\n\
             host: adb.us-ashburn-1.oraclecloud.com:1522"
        );
        // Exactly two newline separators, none trailing.
        assert_eq!(s.matches('\n').count(), 2);
        assert!(!s.ends_with('\n'));
    }

    /// The IAM message must carry AUTH_HEADER + AUTH_SIGNATURE, set the
    /// LOGON|IAM_TOKEN auth mode, and keep the reference key order (the signature
    /// pair sits between AUTH_ALTER_SESSION and AUTH_ORA_EDITION / AUTH_CONNECT_STRING).
    /// This is the deterministic cassette pinning the signed-token wire format.
    #[test]
    fn iam_message_carries_header_and_signature() {
        let mut out = Vec::new();
        append_auth_phase_two_token_iam(
            &mut out,
            "scott",
            "HEADER.PAYLOAD.SIG",
            "drv",
            300_000_000,
            "cs",
            None,
            "date: X\n(request-target): svc\nhost: h:1522",
            "QkFTRTY0U0lHTg==",
        )
        .unwrap();

        assert_eq!(out[0], TNS_MSG_TYPE_FUNCTION);
        assert_eq!(out[1], TNS_FUNC_AUTH_PHASE_TWO);
        assert_eq!(out[3], 1, "user is present");
        let mut pos = 4;
        assert_eq!(read_ub4(&out, &mut pos), 5, "user length = len(\"scott\")");
        assert_eq!(
            read_ub4(&out, &mut pos),
            TNS_AUTH_MODE_LOGON | TNS_AUTH_MODE_IAM_TOKEN,
            "a private key OR's the IAM_TOKEN bit into the LOGON mode"
        );
        assert_eq!(out[pos], 1); // authivl pointer
        pos += 1;
        assert_eq!(
            read_ub4(&out, &mut pos),
            8,
            "AUTH_TOKEN + 4 session pairs + AUTH_HEADER + AUTH_SIGNATURE + AUTH_CONNECT_STRING"
        );

        assert!(contains(&out, b"AUTH_TOKEN"));
        assert!(contains(&out, b"AUTH_HEADER"));
        assert!(contains(&out, b"AUTH_SIGNATURE"));
        assert!(
            contains(&out, b"QkFTRTY0U0lHTg=="),
            "the signature value is sent"
        );
        assert!(contains(&out, b"AUTH_CONNECT_STRING"));
        assert!(
            !contains(&out, b"AUTH_PASSWORD") && !contains(&out, b"AUTH_SESSKEY"),
            "signed-token auth must not send any password material"
        );

        // Ordering: AUTH_HEADER/AUTH_SIGNATURE come after AUTH_ALTER_SESSION and
        // before AUTH_CONNECT_STRING (reference _write_message order).
        let idx = |needle: &[u8]| out.windows(needle.len()).position(|w| w == needle).unwrap();
        assert!(idx(b"AUTH_ALTER_SESSION") < idx(b"AUTH_HEADER"));
        assert!(idx(b"AUTH_HEADER") < idx(b"AUTH_SIGNATURE"));
        assert!(idx(b"AUTH_SIGNATURE") < idx(b"AUTH_CONNECT_STRING"));
    }

    /// Without a connect string or edition the signed-token pair count is exactly
    /// the token + four session pairs + the two signature pairs.
    #[test]
    fn iam_message_pair_count_minimal() {
        let mut out = Vec::new();
        append_auth_phase_two_token_iam(&mut out, "u", "tok", "drv", 1, "", None, "hdr", "sig")
            .unwrap();
        let mut pos = 4;
        let _user_len = read_ub4(&out, &mut pos);
        assert_eq!(
            read_ub4(&out, &mut pos),
            TNS_AUTH_MODE_LOGON | TNS_AUTH_MODE_IAM_TOKEN
        );
        pos += 1; // authivl pointer
        assert_eq!(
            read_ub4(&out, &mut pos),
            7,
            "AUTH_TOKEN + 4 session pairs + AUTH_HEADER + AUTH_SIGNATURE"
        );
        assert!(!contains(&out, b"AUTH_CONNECT_STRING"));
    }

    /// The non-IAM token path is unchanged: it never emits the signature pairs
    /// and never sets the IAM_TOKEN bit (regression guard for the shared inner fn).
    #[test]
    fn plain_token_path_has_no_signature() {
        let mut out = Vec::new();
        append_auth_phase_two_token(&mut out, "u", "tok", "drv", 1, "cs", None).unwrap();
        let mut pos = 4;
        let _user_len = read_ub4(&out, &mut pos);
        assert_eq!(
            read_ub4(&out, &mut pos),
            TNS_AUTH_MODE_LOGON,
            "the bare token path must not set the IAM_TOKEN bit"
        );
        assert!(!contains(&out, b"AUTH_HEADER"));
        assert!(!contains(&out, b"AUTH_SIGNATURE"));
    }
}
