//! Secure transport record/replay fixtures.
//!
//! The committed cassette fixture is synthetic: no live login, password
//! verifier, session key, salt, token, hostname-bearing server banner, or
//! production capture is embedded. The fixture still drives real TNS packet
//! framing and real TTC decoders offline.

#![cfg(feature = "cassette")]

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use oracledb_protocol::net::cassette::{self, CassetteError, Direction};
use oracledb_protocol::thin::{
    build_connect_packet_payload, build_execute_payload_with_bind_rows_and_options_with_seq,
    build_fetch_payload_with_seq, build_function_payload_with_seq,
    parse_fetch_response_with_context, parse_query_response, ClientCapabilities, ColumnMetadata,
    ExecuteOptions, QueryValue, TNS_DATA_FLAGS_END_OF_RESPONSE, TNS_DATA_FLAGS_EOF,
    TNS_FUNC_LOGOFF, TNS_FUNC_ROLLBACK, TNS_MSG_TYPE_END_OF_RESPONSE, TNS_PACKET_TYPE_ACCEPT,
    TNS_PACKET_TYPE_CONNECT, TNS_PACKET_TYPE_DATA,
};
use oracledb_protocol::wire::{encode_packet, PacketLengthWidth};
use sha2::{Digest, Sha256};

type TestResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

const FIXTURE_NAME: &str = "select_7_plus_5.tns-cassette";
const MANIFEST_SCHEMA_VERSION: &str = "1";
const CASSETTE_FORMAT_VERSION: &str = "1";
const SANITIZER_VERSION: &str = "e6.2.0";
const SOURCE_COMMIT: &str = include_str!("../../../docs/baseline/source_commit.txt");
const EXECUTE_SQL: &str = "select value from synthetic_fixture";
const SYNTHETIC_CURSOR_ID: u32 = 42;
const TNS_PACKET_HEADER_LEN: usize = 8;

/// A committed synthetic fixture plus its sidecar manifest.
struct SyntheticFixture {
    cassette: Vec<u8>,
    manifest: String,
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("cassettes")
}

fn fixture_path() -> PathBuf {
    fixture_dir().join(FIXTURE_NAME)
}

fn manifest_path() -> PathBuf {
    fixture_dir().join(format!("{FIXTURE_NAME}.manifest"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut out, "{byte:02x}").expect("writing to String cannot fail");
    }
    out
}

fn hex_value(byte: u8) -> TestResult<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(format!("invalid fixture hex byte {byte:#04x}").into()),
    }
}

fn decode_hex_fixture(hex: &str) -> TestResult<Vec<u8>> {
    let clean = hex
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    if clean.len() % 2 != 0 {
        return Err("fixture hex must contain an even number of digits".into());
    }
    let mut out = Vec::with_capacity(clean.len() / 2);
    for pair in clean.chunks_exact(2) {
        out.push((hex_value(pair[0])? << 4) | hex_value(pair[1])?);
    }
    Ok(out)
}

fn number_columns() -> Vec<ColumnMetadata> {
    vec![
        ColumnMetadata::new("INTCOL", oracledb_protocol::thin::ORA_TYPE_NUM_NUMBER)
            .with_csfrm(oracledb_protocol::thin::CS_FORM_IMPLICIT)
            .with_buffer_size(22)
            .with_max_size(22)
            .with_nulls_allowed(true),
        ColumnMetadata::new("NUMBERCOL", oracledb_protocol::thin::ORA_TYPE_NUM_NUMBER)
            .with_csfrm(oracledb_protocol::thin::CS_FORM_IMPLICIT)
            .with_buffer_size(22)
            .with_max_size(22)
            .with_nulls_allowed(true),
    ]
}

fn previous_fetch_row() -> Vec<Option<QueryValue>> {
    vec![
        Some(QueryValue::number_from_text("2", true)),
        Some(QueryValue::number_from_text("0.5", false)),
    ]
}

fn synthetic_execute_response_payload() -> TestResult<Vec<u8>> {
    decode_hex_fixture(concat!(
        "101710740fb986350b6010fbcb6e06a74ed0787e060a110328014001018201800000",
        "014000000000020369010140023ffe010501050556414c554500000000000000000000",
        "010707787e060a110b1000021fe8010a010a00062201010001020000000708414c33",
        "32555446380801060323a4d500010100000000000004010102013b010102057b0000",
        "01010003000000000000000000000000030001010000000002057b0101010300194f",
        "52412d30313430333a206e6f206461746120666f756e640a1d",
    ))
}

fn synthetic_fetch_response_payload() -> TestResult<Vec<u8>> {
    decode_hex_fixture("06020101000205dc0001010101000702c1041d")
}

fn synthetic_connect_packet() -> TestResult<Vec<u8>> {
    let payload = build_connect_packet_payload(
        "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=fixture-host)(PORT=0))\
         (CONNECT_DATA=(SERVICE_NAME=SYNTHETIC)(CID=(PROGRAM=rust-oracledb)\
         (HOST=fixture-host)(USER=fixture-user))))",
        8192,
    )?;
    Ok(encode_packet(
        TNS_PACKET_TYPE_CONNECT,
        0,
        None,
        &payload,
        PacketLengthWidth::Legacy16,
    )?)
}

fn synthetic_accept_packet() -> TestResult<Vec<u8>> {
    Ok(encode_packet(
        TNS_PACKET_TYPE_ACCEPT,
        0,
        None,
        b"SYNTHETIC-ACCEPT",
        PacketLengthWidth::Legacy16,
    )?)
}

fn data_packet(message: &[u8], end_of_response: bool) -> TestResult<Vec<u8>> {
    let flags = if end_of_response {
        TNS_DATA_FLAGS_END_OF_RESPONSE
    } else {
        0
    };
    Ok(encode_packet(
        TNS_PACKET_TYPE_DATA,
        0,
        Some(flags),
        message,
        PacketLengthWidth::Large32,
    )?)
}

fn eof_packet() -> TestResult<Vec<u8>> {
    Ok(encode_packet(
        TNS_PACKET_TYPE_DATA,
        0,
        Some(TNS_DATA_FLAGS_EOF),
        &[],
        PacketLengthWidth::Large32,
    )?)
}

fn execute_packet() -> TestResult<Vec<u8>> {
    let payload = build_execute_payload_with_bind_rows_and_options_with_seq(
        EXECUTE_SQL,
        2,
        1,
        true,
        &[],
        ExecuteOptions::default(),
        ClientCapabilities::default().ttc_field_version,
    )?;
    Ok(encode_packet(
        TNS_PACKET_TYPE_DATA,
        0,
        Some(0),
        &payload,
        PacketLengthWidth::Large32,
    )?)
}

fn fetch_packet() -> TestResult<Vec<u8>> {
    let payload = build_fetch_payload_with_seq(
        SYNTHETIC_CURSOR_ID,
        2,
        2,
        ClientCapabilities::default().ttc_field_version,
    );
    Ok(encode_packet(
        TNS_PACKET_TYPE_DATA,
        0,
        Some(0),
        &payload,
        PacketLengthWidth::Large32,
    )?)
}

fn function_packet(function_code: u8, seq_num: u8) -> TestResult<Vec<u8>> {
    let payload = build_function_payload_with_seq(
        function_code,
        seq_num,
        ClientCapabilities::default().ttc_field_version,
    );
    Ok(encode_packet(
        TNS_PACKET_TYPE_DATA,
        0,
        Some(0),
        &payload,
        PacketLengthWidth::Large32,
    )?)
}

fn write_server_packet_split(out: &mut Vec<u8>, micros: u64, packet: &[u8]) -> TestResult<()> {
    let (header, body) = packet
        .split_at_checked(TNS_PACKET_HEADER_LEN)
        .ok_or("synthetic TNS packet shorter than header")?;
    cassette::write_frame(out, Direction::ServerToClient, micros, header);
    cassette::write_frame(out, Direction::ServerToClient, micros + 1, body);
    Ok(())
}

fn build_synthetic_fixture() -> TestResult<SyntheticFixture> {
    let connect = synthetic_connect_packet()?;
    let execute = execute_packet()?;
    let fetch = fetch_packet()?;
    let rollback = function_packet(TNS_FUNC_ROLLBACK, 3)?;
    let logoff = function_packet(TNS_FUNC_LOGOFF, 4)?;
    let eof = eof_packet()?;
    let expected_writes = [&connect, &execute, &fetch, &rollback, &logoff, &eof];

    let execute_response = data_packet(&synthetic_execute_response_payload()?, true)?;
    let fetch_response = data_packet(&synthetic_fetch_response_payload()?, true)?;
    let function_response = data_packet(&[TNS_MSG_TYPE_END_OF_RESPONSE], true)?;

    let mut cassette = Vec::new();
    cassette::write_header(&mut cassette);
    cassette::write_frame(&mut cassette, Direction::ClientToServer, 0, &connect);
    write_server_packet_split(&mut cassette, 1, &synthetic_accept_packet()?)?;
    cassette::write_frame(&mut cassette, Direction::ClientToServer, 10, &execute);
    write_server_packet_split(&mut cassette, 11, &execute_response)?;
    cassette::write_frame(&mut cassette, Direction::ClientToServer, 20, &fetch);
    write_server_packet_split(&mut cassette, 21, &fetch_response)?;
    cassette::write_frame(&mut cassette, Direction::ClientToServer, 30, &rollback);
    write_server_packet_split(&mut cassette, 31, &function_response)?;
    cassette::write_frame(&mut cassette, Direction::ClientToServer, 40, &logoff);
    write_server_packet_split(&mut cassette, 41, &function_response)?;
    cassette::write_frame(&mut cassette, Direction::ClientToServer, 50, &eof);

    let expected_write_sha256 = expected_writes
        .iter()
        .map(|bytes| sha256_hex(bytes))
        .collect::<Vec<_>>()
        .join(",");
    let manifest = format!(
        concat!(
            "schema_version = {}\n",
            "format_version = {}\n",
            "commit = \"{}\"\n",
            "profile = \"synthetic-post-auth\"\n",
            "charset = \"AL32UTF8\"\n",
            "timezone = \"+00:00\"\n",
            "scenario = \"synthetic_connect_execute_fetch_close\"\n",
            "sanitizer_version = \"{}\"\n",
            "checksum_sha256 = \"{}\"\n",
            "expected_writes = {}\n",
            "expected_write_sha256 = \"{}\"\n",
            "sanitized = true\n",
        ),
        MANIFEST_SCHEMA_VERSION,
        CASSETTE_FORMAT_VERSION,
        SOURCE_COMMIT.trim(),
        SANITIZER_VERSION,
        sha256_hex(&cassette),
        expected_writes.len(),
        expected_write_sha256,
    );

    Ok(SyntheticFixture { cassette, manifest })
}

fn parse_manifest(text: &str) -> BTreeMap<String, String> {
    text.lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (key, value) = line.split_once('=')?;
            Some((
                key.trim().to_string(),
                value.trim().trim_matches('"').to_string(),
            ))
        })
        .collect()
}

fn require_manifest_value(
    manifest: &BTreeMap<String, String>,
    key: &str,
    expected: &str,
) -> TestResult<()> {
    match manifest.get(key).map(String::as_str) {
        Some(actual) if actual == expected => Ok(()),
        Some(actual) => {
            Err(format!("manifest {key} mismatch: expected {expected}, got {actual}").into())
        }
        None => Err(format!("manifest missing {key}").into()),
    }
}

fn assert_manifest_valid(cassette_bytes: &[u8], manifest_text: &str) -> TestResult<()> {
    let manifest = parse_manifest(manifest_text);
    require_manifest_value(&manifest, "schema_version", MANIFEST_SCHEMA_VERSION)?;
    require_manifest_value(&manifest, "format_version", CASSETTE_FORMAT_VERSION)?;
    require_manifest_value(&manifest, "profile", "synthetic-post-auth")?;
    require_manifest_value(&manifest, "sanitizer_version", SANITIZER_VERSION)?;
    require_manifest_value(&manifest, "sanitized", "true")?;
    require_manifest_value(&manifest, "checksum_sha256", &sha256_hex(cassette_bytes))?;

    let frames = cassette::decode_all(cassette_bytes)?;
    let write_hashes = frames
        .iter()
        .filter(|frame| frame.direction == Direction::ClientToServer)
        .map(|frame| sha256_hex(&frame.bytes))
        .collect::<Vec<_>>();
    let expected_writes = write_hashes.len().to_string();
    require_manifest_value(&manifest, "expected_writes", &expected_writes)?;
    require_manifest_value(&manifest, "expected_write_sha256", &write_hashes.join(","))?;
    Ok(())
}

fn contains_ignore_ascii_case(haystack: &[u8], needle: &str) -> bool {
    String::from_utf8_lossy(haystack)
        .to_ascii_lowercase()
        .contains(&needle.to_ascii_lowercase())
}

fn secret_field_name(bytes: &[u8]) -> Option<&'static str> {
    [
        "AUTH_PASSWORD",
        "AUTH_SESSKEY",
        "AUTH_VFR_DATA",
        "AUTH_PBKDF2_CSK_SALT",
        "AUTH_PBKDF2_SPEEDY_KEY",
        "AUTH_TOKEN",
        "SESSION_TOKEN",
        "SESSION_KEY",
        "ACCESS_TOKEN",
        "REFRESH_TOKEN",
        "PRIVATE_KEY",
        "AUTH_SC_SERVER_HOST",
        "AUTH_VERSION_STRING",
    ]
    .into_iter()
    .find(|field| contains_ignore_ascii_case(bytes, field))
}

fn printable_tokens(bytes: &[u8]) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = Vec::new();
    for byte in bytes {
        if byte.is_ascii_graphic() {
            current.push(*byte);
        } else if !current.is_empty() {
            if current.len() >= 20 {
                tokens.push(String::from_utf8_lossy(&current).to_string());
            }
            current.clear();
        }
    }
    if current.len() >= 20 {
        tokens.push(String::from_utf8_lossy(&current).to_string());
    }
    tokens
}

fn shannon_entropy(token: &str) -> f64 {
    let mut counts = [0usize; 256];
    for byte in token.bytes() {
        counts[usize::from(byte)] += 1;
    }
    let len = token.len() as f64;
    counts
        .iter()
        .filter(|count| **count > 0)
        .map(|count| {
            let p = *count as f64 / len;
            -p * p.log2()
        })
        .sum()
}

fn looks_like_secret_token(token: &str) -> bool {
    if token.len() < 24 {
        return false;
    }
    if token
        .bytes()
        .any(|byte| matches!(byte, b'(' | b')' | b'=' | b',' | b';' | b':'))
    {
        return false;
    }
    let secret_alphabet = token
        .bytes()
        .filter(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'_' | b'-' | b'@' | b'=')
        })
        .count();
    let has_alpha = token.bytes().any(|byte| byte.is_ascii_alphabetic());
    let has_digit = token.bytes().any(|byte| byte.is_ascii_digit());
    let alphabet_ratio = secret_alphabet as f64 / token.len() as f64;
    alphabet_ratio > 0.85 && has_alpha && has_digit && shannon_entropy(token) >= 4.2
}

fn scan_for_leaks(bytes: &[u8]) -> Vec<String> {
    let mut findings = Vec::new();
    if let Some(field) = secret_field_name(bytes) {
        findings.push(format!("known secret-bearing field name `{field}`"));
    }
    for token in printable_tokens(bytes) {
        if looks_like_secret_token(&token) {
            findings.push(format!(
                "high-entropy printable token (len {}, entropy {:.2})",
                token.len(),
                shannon_entropy(&token)
            ));
        }
    }
    findings
}

fn scrub_secret_bearing_frames(data: &[u8]) -> std::result::Result<Vec<u8>, CassetteError> {
    let frames = cassette::decode_all(data)?;
    let mut scrubbed = Vec::new();
    cassette::write_header(&mut scrubbed);
    for frame in frames {
        if secret_field_name(&frame.bytes).is_some() {
            cassette::write_frame(
                &mut scrubbed,
                frame.direction,
                frame.micros,
                b"[scrubbed-frame]",
            );
        } else {
            cassette::write_frame(&mut scrubbed, frame.direction, frame.micros, &frame.bytes);
        }
    }
    Ok(scrubbed)
}

#[test]
fn scrubber_strips_secret_bearing_fields() -> TestResult<()> {
    let mut raw = Vec::new();
    cassette::write_header(&mut raw);
    cassette::write_frame(
        &mut raw,
        Direction::ClientToServer,
        0,
        b"AUTH_PASSWORD @@ABCDEF0123456789ABCDEF0123456789",
    );
    cassette::write_frame(
        &mut raw,
        Direction::ServerToClient,
        1,
        b"AUTH_SC_SERVER_HOST prod-db-01 AUTH_SESSKEY @@0123456789ABCDEF0123456789ABCDEF",
    );
    cassette::write_frame(
        &mut raw,
        Direction::ServerToClient,
        2,
        b"safe synthetic frame",
    );

    let scrubbed = scrub_secret_bearing_frames(&raw)?;
    let findings = scan_for_leaks(&scrubbed);
    assert!(
        findings.is_empty(),
        "scrubbed fixture should have no leak findings: {findings:?}"
    );
    let text = String::from_utf8_lossy(&scrubbed);
    assert!(!text.contains("AUTH_PASSWORD"));
    assert!(!text.contains("prod-db-01"));
    assert!(text.contains("safe synthetic frame"));
    Ok(())
}

#[test]
fn committed_fixture_manifest_and_leak_scan_pass() -> TestResult<()> {
    let bytes = fs::read(fixture_path())?;
    let manifest = fs::read_to_string(manifest_path())?;
    assert_manifest_valid(&bytes, &manifest)?;

    let findings = scan_for_leaks(&bytes);
    assert!(
        findings.is_empty(),
        "cassette fixture must not contain secrets: {findings:?}"
    );
    let manifest_findings = scan_for_leaks(manifest.as_bytes());
    assert!(
        manifest_findings.is_empty(),
        "cassette manifest must not contain secrets: {manifest_findings:?}"
    );
    Ok(())
}

#[test]
fn replay_synthetic_fixture_decodes_execute_and_fetch_offline() -> TestResult<()> {
    let frames = cassette::decode_all(&fs::read(fixture_path())?)?;
    assert!(
        frames
            .iter()
            .any(|frame| frame.direction == Direction::ClientToServer),
        "fixture must contain expected C->S writes"
    );
    assert!(
        frames
            .iter()
            .any(|frame| frame.direction == Direction::ServerToClient),
        "fixture must contain replayed S->C reads"
    );

    let server_bytes = frames
        .iter()
        .filter(|frame| frame.direction == Direction::ServerToClient)
        .flat_map(|frame| frame.bytes.iter().copied())
        .collect::<Vec<_>>();
    let responses = reassemble_responses(&server_bytes);
    assert!(
        responses.len() >= 4,
        "execute, fetch, rollback, and logoff responses must replay offline"
    );

    let execute = parse_query_response(&responses[0], ClientCapabilities::default())?;
    assert_eq!(
        execute.cell(0, 0).and_then(QueryValue::as_text),
        Some("AL32UTF8")
    );

    let previous = previous_fetch_row();
    let fetch = parse_fetch_response_with_context(
        &responses[1],
        ClientCapabilities::default(),
        &number_columns(),
        Some(&previous),
    )?;
    assert_eq!(
        fetch
            .cell(0, 0)
            .and_then(QueryValue::as_number_text)
            .as_deref(),
        Some("3")
    );
    assert_eq!(
        fetch
            .cell(0, 1)
            .and_then(QueryValue::as_number_text)
            .as_deref(),
        Some("0.5")
    );
    Ok(())
}

#[test]
fn altered_manifest_checksum_fails_validation() -> TestResult<()> {
    let bytes = fs::read(fixture_path())?;
    let mut manifest = fs::read_to_string(manifest_path())?;
    manifest = manifest.replace("checksum_sha256 = \"", "checksum_sha256 = \"00");
    let err = assert_manifest_valid(&bytes, &manifest)
        .expect_err("altered checksum must fail manifest validation");
    assert!(
        err.to_string().contains("checksum_sha256"),
        "checksum mismatch error should name the manifest checksum field"
    );
    Ok(())
}

#[test]
fn truncated_and_unsupported_cassettes_fail() -> TestResult<()> {
    let bytes = fs::read(fixture_path())?;
    let truncated = &bytes[..bytes.len().saturating_sub(1)];
    assert!(
        cassette::decode_all(truncated).is_err(),
        "truncated cassette must fail decode"
    );

    let mut bad_version = bytes;
    let version = bad_version
        .get_mut(cassette::CASSETTE_MAGIC.len())
        .ok_or("cassette header missing version byte")?;
    *version = version.saturating_add(1);
    assert!(
        matches!(
            cassette::decode_all(&bad_version),
            Err(CassetteError::UnsupportedVersion(_))
        ),
        "unsupported cassette version must fail decode"
    );
    Ok(())
}

/// Regenerate the committed fixture from deterministic synthetic bytes.
///
/// This is ignored because normal CI should validate fixtures, not rewrite
/// them. It is intentionally synthetic-only and writes no live capture data.
#[test]
#[ignore = "regenerates the synthetic cassette fixture and manifest"]
fn write_synthetic_connect_execute_fetch_close_fixture() -> TestResult<()> {
    let fixture = build_synthetic_fixture()?;
    fs::write(fixture_path(), fixture.cassette)?;
    fs::write(manifest_path(), fixture.manifest)?;
    Ok(())
}

#[derive(Clone, Copy)]
enum LenWidth {
    Legacy16,
    Large32,
}

fn read_tns_packet(read: &mut std::io::Cursor<&[u8]>, width: LenWidth) -> Option<(u8, Vec<u8>)> {
    use std::io::Read as _;

    let mut header = [0u8; TNS_PACKET_HEADER_LEN];
    read.read_exact(&mut header).ok()?;
    let declared = match width {
        LenWidth::Legacy16 => usize::from(u16::from_be_bytes([header[0], header[1]])),
        LenWidth::Large32 => {
            u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize
        }
    };
    if declared < header.len() {
        return None;
    }
    let mut body = vec![0u8; declared - header.len()];
    read.read_exact(&mut body).ok()?;
    Some((header[4], body))
}

fn reassemble_responses(server_bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut responses = Vec::new();
    let mut current: Vec<u8> = Vec::new();
    let mut read = std::io::Cursor::new(server_bytes);
    let mut width = LenWidth::Legacy16;
    while let Some((packet_type, body)) = read_tns_packet(&mut read, width) {
        width = LenWidth::Large32;
        if packet_type != TNS_PACKET_TYPE_DATA {
            continue;
        }
        let Some((flags_bytes, payload)) = body.split_at_checked(2) else {
            continue;
        };
        let flags = u16::from_be_bytes([flags_bytes[0], flags_bytes[1]]);
        current.extend_from_slice(payload);
        if flags & (TNS_DATA_FLAGS_END_OF_RESPONSE | TNS_DATA_FLAGS_EOF) != 0 {
            responses.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        responses.push(current);
    }
    responses
}
