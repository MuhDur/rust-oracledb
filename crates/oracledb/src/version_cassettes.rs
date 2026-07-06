//! L2 version cassettes (bead rust-oracledb-xver-parity-so3w.3).
//!
//! The live version matrix (L3) records the REAL TTC wire exchange against a
//! Docker fleet of every supported Oracle generation, but it is slow and needs
//! containers, so it only runs nightly. L2 records that exchange **once** per
//! version into a committed `.tns-cassette` and **replays it offline** in unit
//! CI, so a cross-version wire regression (a version gate that flips the emitted
//! request bytes or mis-decodes a real server response) fails on **every PR** in
//! seconds with no database and no network.
//!
//! # Scope of this first cassette set
//!
//! The captured scenario is the **connect negotiation handshake**: the client's
//! `CONNECT` packet (plus any `RESEND` retries) and the server's `ACCEPT`
//! response, up to — but **not including** — authentication. This is the
//! richest *version-gated, secret-free, byte-deterministic* surface on the wire:
//!
//! * The negotiated `protocol_version` and the `fast_auth` / `end_of_response` /
//!   `oob` capability flags are exactly the gate inputs the parity epic keys on
//!   (`parse_accept_payload`, `capabilities.pyx` gates). Each server generation
//!   emits a different ACCEPT layout — Oracle 11g (below the protocol floor)
//!   even uses the short pre-12.1 24-byte layout — so replaying the REAL bytes
//!   pins the decoder against ground truth, not against a hand-crafted fixture.
//! * The handshake carries **no secrets** (no password verifier, session key,
//!   salt, or token — those only appear in the auth phase, which we never
//!   capture) and **no client randomness** (the auth session key from
//!   `OsRng` is the only non-deterministic wire input, and it lives in the auth
//!   phase). So the `CONNECT` request is byte-reproducible and the cassette is
//!   safe to commit.
//!
//! The capture uses a **fixed synthetic connect descriptor** (a placeholder CID
//! with no real machine hostname / OS user) so the recorded `CONNECT` bytes
//! leak no local identity and are reproducible offline.
//!
//! # Post-auth query cassettes (bead `cwsr`)
//!
//! A second cassette set reaches a **post-auth typed query**. The auth phase is
//! non-deterministic (client `OsRng` session key) and carries secrets, so it is
//! never committed; instead a full `connect + auth + execute` session is
//! captured, the connect+auth prefix (and trailing logoff) are sliced off, and
//! only the deterministic, secret-free execute request/response frames are
//! committed (`<lane>-postauth.tns-cassette`). Offline replay rebuilds a loopback
//! [`crate::Connection`] *seeded* from the manifest with the negotiated caps and
//! the post-auth `ttc_seq_num` — both shape the execute request bytes — so the
//! recorded request replays byte-exact under `ReplayWriteMode::Check`. See the
//! `record_postauth_query_cassettes` / `replay_postauth_query_cassettes_offline`
//! pair below. LOB / AQ / DPL / CQN round-trips remain a follow-up.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use asupersync::net::TcpStream;
use asupersync::Cx;
use sha2::{Digest, Sha256};

use oracledb_protocol::net::cassette::{self, Direction};
use oracledb_protocol::thin::{
    build_connect_packet_payload, parse_accept_payload, TNS_PACKET_TYPE_ACCEPT,
    TNS_PACKET_TYPE_CONNECT, TNS_PACKET_TYPE_RESEND,
};
use oracledb_protocol::wire::{encode_packet, PacketLengthWidth, ProtocolLimits};
use oracledb_protocol::ProtocolError;

use crate::transport::{self, ReplayWriteMode};
use crate::{
    build_io_runtime, ConnectionCore, DriverTransport, Error, IncomingPacket, Result,
    MAX_CONNECT_RESEND_ROUNDS,
};

/// SDU advertised in every capture/replay CONNECT packet. Fixed so the request
/// bytes are reproducible offline.
const ADVERTISED_SDU: u16 = 8192;

/// Below-floor protocol version Oracle 11g negotiates (12.1 = 315 is the floor).
const ORACLE_11G_PROTOCOL_VERSION: u16 = 314;

const MANIFEST_SCHEMA_VERSION: &str = "1";
const CASSETTE_FORMAT_VERSION: &str = "1";
const SOURCE_COMMIT: &str = include_str!("../../../docs/baseline/source_commit.txt");

/// A version lane whose connect-negotiation handshake we cassette.
struct Lane {
    /// Short lane id, used in the cassette file name.
    id: &'static str,
    /// Default `host:port/service` used both to dial (capture) and to name the
    /// service in the CONNECT descriptor (capture + replay). Overridable at
    /// capture time via the `ORACLEDB_CASSETTE_<ID>` env var.
    default_connect: &'static str,
    /// Expected negotiation outcome, asserted on offline replay.
    outcome: Outcome,
}

/// The version-gated result the driver derives from the real ACCEPT bytes.
enum Outcome {
    /// Below the protocol floor: `parse_accept_payload` must refuse with a
    /// structured `UnsupportedVersion` naming this version and the floor.
    Refusal { version: u16 },
    /// At/above the floor: `parse_accept_payload` succeeds; these are the
    /// version-gated capability flags it must derive from the real bytes.
    Accept {
        supports_fast_auth: bool,
        supports_end_of_response: bool,
    },
}

/// Owned copy of [`Outcome`] moved into the scoped replay future.
enum ExpectedOutcome {
    Refusal {
        version: u16,
    },
    Accept {
        supports_fast_auth: bool,
        supports_end_of_response: bool,
    },
}

/// The lanes covered by the first version-cassette set. The connect strings
/// mirror `scripts/version_matrix.sh` (xe11 is the below-floor refusal lane).
fn lanes() -> Vec<Lane> {
    vec![
        Lane {
            id: "xe11",
            default_connect: "localhost:1511/XE",
            outcome: Outcome::Refusal {
                version: ORACLE_11G_PROTOCOL_VERSION,
            },
        },
        Lane {
            id: "xe18",
            default_connect: "localhost:1518/XEPDB1",
            outcome: Outcome::Accept {
                supports_fast_auth: false,
                supports_end_of_response: false,
            },
        },
        Lane {
            id: "xe21",
            default_connect: "localhost:1520/XEPDB1",
            outcome: Outcome::Accept {
                supports_fast_auth: false,
                supports_end_of_response: false,
            },
        },
        Lane {
            id: "free23",
            default_connect: "localhost:1522/FREEPDB1",
            outcome: Outcome::Accept {
                supports_fast_auth: true,
                supports_end_of_response: true,
            },
        },
    ]
}

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("cassettes")
}

fn cassette_path(lane_id: &str) -> PathBuf {
    fixtures_dir().join(format!("{lane_id}-connect.tns-cassette"))
}

fn manifest_path(lane_id: &str) -> PathBuf {
    fixtures_dir().join(format!("{lane_id}-connect.tns-cassette.manifest"))
}

/// Split `host:port/service` into `(host, port, service)`.
fn split_connect(connect: &str) -> Result<(String, u16, String)> {
    let (addr, service) = connect
        .rsplit_once('/')
        .ok_or_else(|| Error::Runtime(format!("connect string {connect:?} has no /service")))?;
    let (host, port) = addr
        .rsplit_once(':')
        .ok_or_else(|| Error::Runtime(format!("address {addr:?} has no :port")))?;
    let port: u16 = port
        .parse()
        .map_err(|_| Error::Runtime(format!("bad port in {addr:?}")))?;
    Ok((host.to_string(), port, service.to_string()))
}

/// A fixed synthetic connect descriptor for `service`. The CID carries only
/// placeholder identity (no real hostname / OS user), so the recorded CONNECT
/// bytes are reproducible offline and leak nothing about the capture host.
fn capture_connect_descriptor(service: &str) -> String {
    format!(
        "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=cassette-capture)(PORT=0))\
         (CONNECT_DATA=(SERVICE_NAME={service})(CID=(PROGRAM=rust-oracledb-cassette)\
         (HOST=cassette-capture)(USER=cassette))))"
    )
}

/// Drive the connect-negotiation handshake (CONNECT / RESEND* / ACCEPT) over
/// `core`, returning the ACCEPT packet. Byte-identical to the loop in
/// `Connection::connect`, but with the fixed capture descriptor and stopping at
/// ACCEPT (before auth). Used for BOTH live capture and offline replay, so the
/// request bytes match by construction.
async fn drive_connect_handshake(
    core: &mut ConnectionCore<DriverTransport>,
    cx: &Cx,
    connect_data: &str,
) -> Result<IncomingPacket> {
    let mut resend_rounds = 0u8;
    loop {
        let payload = build_connect_packet_payload(connect_data, ADVERTISED_SDU)?;
        let packet = encode_packet(
            TNS_PACKET_TYPE_CONNECT,
            0,
            None,
            &payload,
            PacketLengthWidth::Legacy16,
        )?;
        core.write_all(cx, &packet).await?;
        // The fixed descriptor is short (< TNS_MAX_CONNECT_DATA), so it travels
        // inline in the CONNECT packet — no follow-up DATA packet to send.
        let reply = core.read_packet(PacketLengthWidth::Legacy16).await?;
        match reply.packet_type {
            TNS_PACKET_TYPE_ACCEPT => return Ok(reply),
            TNS_PACKET_TYPE_RESEND => {
                resend_rounds += 1;
                if resend_rounds > MAX_CONNECT_RESEND_ROUNDS {
                    return Err(Error::ConnectResendLoop(resend_rounds));
                }
                continue;
            }
            other => return Err(Error::UnexpectedPacket(other)),
        }
    }
}

// ---- secret / sanitization guard ------------------------------------------

/// Known auth-phase field names that must NEVER appear in a committed cassette.
/// The connect-negotiation capture stops before auth, so this is a belt-and-
/// suspenders assertion: if any appears, we refuse to write the fixture.
const SECRET_FIELD_NAMES: &[&str] = &[
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
];

fn scan_for_secret_fields(bytes: &[u8]) -> Vec<&'static str> {
    let haystack = String::from_utf8_lossy(bytes).to_ascii_uppercase();
    SECRET_FIELD_NAMES
        .iter()
        .copied()
        .filter(|field| haystack.contains(field))
        .collect()
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

fn write_frame_hashes(cassette_bytes: &[u8]) -> Result<Vec<String>> {
    let frames = cassette::decode_all(cassette_bytes)
        .map_err(|err| Error::Runtime(format!("cassette decode: {err}")))?;
    Ok(frames
        .iter()
        .filter(|frame| frame.direction == Direction::ClientToServer)
        .map(|frame| sha256_hex(&frame.bytes))
        .collect())
}

// ---- manifest --------------------------------------------------------------

fn build_manifest(
    lane: &Lane,
    service: &str,
    cassette_bytes: &[u8],
    accept_payload: &[u8],
) -> Result<String> {
    let (outcome, version, fast_auth, eor) = describe_accept(accept_payload);
    let write_hashes = write_frame_hashes(cassette_bytes)?;
    Ok(format!(
        concat!(
            "schema_version = {}\n",
            "format_version = {}\n",
            "commit = \"{}\"\n",
            "profile = \"connect-negotiation\"\n",
            "lane = \"{}\"\n",
            "service = \"{}\"\n",
            "scenario = \"connect_accept\"\n",
            "outcome = \"{}\"\n",
            "protocol_version = {}\n",
            "supports_fast_auth = {}\n",
            "supports_end_of_response = {}\n",
            "sanitized = true\n",
            "checksum_sha256 = \"{}\"\n",
            "expected_writes = {}\n",
            "expected_write_sha256 = \"{}\"\n",
        ),
        MANIFEST_SCHEMA_VERSION,
        CASSETTE_FORMAT_VERSION,
        SOURCE_COMMIT.trim(),
        lane.id,
        service,
        outcome,
        version,
        fast_auth,
        eor,
        sha256_hex(cassette_bytes),
        write_hashes.len(),
        write_hashes.join(","),
    ))
}

/// Decode the ACCEPT payload into the manifest's version-gated fields. Refusal
/// (below-floor) is reported with the raw version and no capabilities.
fn describe_accept(payload: &[u8]) -> (&'static str, u16, bool, bool) {
    match parse_accept_payload(payload) {
        Ok(info) => (
            "accept",
            info.protocol_version,
            info.supports_fast_auth,
            info.supports_end_of_response,
        ),
        Err(ProtocolError::UnsupportedVersion { version, .. }) => {
            ("refusal", version, false, false)
        }
        Err(_) => ("unknown", 0, false, false),
    }
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

// ---- CAPTURE (live; #[ignore]) --------------------------------------------

/// Record the connect-negotiation cassette for one lane against a live server.
/// Returns `Ok(())` after writing the cassette + manifest, or an error the
/// caller aggregates.
fn record_lane(lane: &Lane) -> Result<()> {
    let connect = std::env::var(format!("ORACLEDB_CASSETTE_{}", lane.id.to_uppercase()))
        .unwrap_or_else(|_| lane.default_connect.to_string());
    let (host, port, service) = split_connect(&connect)?;
    let connect_data = capture_connect_descriptor(&service);

    let runtime = build_io_runtime()?;
    let cassette_bytes = runtime.block_on(async move {
        let cx = Cx::current()
            .ok_or_else(|| Error::Runtime("missing ambient Cx in capture runtime".into()))?;
        let stream =
            TcpStream::connect_timeout((host.clone(), port), std::time::Duration::from_secs(15))
                .await
                .map_err(|err| Error::Runtime(format!("dial {host}:{port}: {err}")))?;
        stream.set_nodelay(true).ok();

        // Install the recorder BEFORE splitting so plain_split auto-tees.
        let scope = transport::capture_scope();
        let (read, write) = transport::plain_split(stream);
        let mut core = ConnectionCore::<DriverTransport>::from_halves(read, write, "cassette");
        core.set_protocol_limits(ProtocolLimits::DEFAULT)?;

        // Drive to ACCEPT and stop — never send the auth phase.
        let accept = drive_connect_handshake(&mut core, &cx, &connect_data).await?;
        Ok::<_, Error>((scope.to_cassette_bytes(), accept.payload))
    })?;
    let (cassette_bytes, accept_payload) = cassette_bytes;

    // Sanitization gate: refuse to persist anything with an auth field name.
    let leaks = scan_for_secret_fields(&cassette_bytes);
    if !leaks.is_empty() {
        return Err(Error::Runtime(format!(
            "REFUSING to write {}: secret field(s) present: {leaks:?}",
            lane.id
        )));
    }

    let manifest = build_manifest(lane, &service, &cassette_bytes, &accept_payload)?;
    let out_dir = std::env::var("ORACLEDB_CASSETTE_RECORD")
        .map(PathBuf::from)
        .unwrap_or_else(|_| fixtures_dir());
    fs::create_dir_all(&out_dir).map_err(|e| Error::Runtime(e.to_string()))?;
    let cass = out_dir.join(format!("{}-connect.tns-cassette", lane.id));
    let man = out_dir.join(format!("{}-connect.tns-cassette.manifest", lane.id));
    fs::write(&cass, &cassette_bytes).map_err(|e| Error::Runtime(e.to_string()))?;
    fs::write(&man, manifest).map_err(|e| Error::Runtime(e.to_string()))?;
    eprintln!(
        "recorded {} ({} bytes) -> {}",
        lane.id,
        cassette_bytes.len(),
        cass.display()
    );
    Ok(())
}

/// Record every lane's connect-negotiation cassette against the live version
/// fleet. Ignored by default (needs the Docker lanes up); run explicitly:
///
/// ```text
/// cargo test -p oracledb --features cassette \
///   record_version_connect_cassettes -- --ignored --nocapture
/// ```
///
/// Per-lane connect strings default to the `version_matrix.sh` ports and are
/// overridable via `ORACLEDB_CASSETTE_XE11` / `_XE18` / `_XE21` / `_FREE23`.
/// No credentials are needed — capture stops before authentication.
#[test]
#[ignore = "records the live version-connect cassettes; needs the Docker lanes"]
fn record_version_connect_cassettes() {
    let mut failures = Vec::new();
    for lane in lanes() {
        match record_lane(&lane) {
            Ok(()) => {}
            Err(err) => failures.push(format!("{}: {err}", lane.id)),
        }
    }
    assert!(failures.is_empty(), "capture failures: {failures:?}");
}

// ---- REPLAY (offline; runs in cargo test) ---------------------------------

/// Replay one committed cassette offline and assert the driver re-derives the
/// recorded, version-specific negotiation outcome — with the CONNECT request
/// bytes byte-matching the recording (`ReplayWriteMode::Check`) and the cassette
/// consumed exactly (`ReplayAudit`).
fn replay_lane(lane: &Lane) -> Result<()> {
    let cassette_bytes =
        fs::read(cassette_path(lane.id)).map_err(|e| Error::Runtime(e.to_string()))?;
    let manifest_text =
        fs::read_to_string(manifest_path(lane.id)).map_err(|e| Error::Runtime(e.to_string()))?;
    let manifest = parse_manifest(&manifest_text);

    // Integrity: the committed cassette must match its manifest checksum, so a
    // silent edit to either side fails loudly.
    let expected_checksum = manifest
        .get("checksum_sha256")
        .ok_or_else(|| Error::Runtime("manifest missing checksum_sha256".into()))?;
    if &sha256_hex(&cassette_bytes) != expected_checksum {
        return Err(Error::Runtime(format!(
            "{}: cassette checksum != manifest",
            lane.id
        )));
    }
    // Sanitization holds for the committed artifact too.
    let leaks = scan_for_secret_fields(&cassette_bytes);
    if !leaks.is_empty() {
        return Err(Error::Runtime(format!(
            "{}: secret leak {leaks:?}",
            lane.id
        )));
    }

    let service = manifest
        .get("service")
        .cloned()
        .ok_or_else(|| Error::Runtime("manifest missing service".into()))?;
    let connect_data = capture_connect_descriptor(&service);

    let (read, write, audit) =
        transport::replay_split_with_audit(&cassette_bytes, ReplayWriteMode::Check)
            .map_err(|err| Error::Runtime(format!("invalid replay cassette: {err}")))?;
    let mut core = ConnectionCore::<DriverTransport>::from_halves(read, write, "replay");
    core.set_protocol_limits(ProtocolLimits::DEFAULT)?;

    // Move only owned/Copy values into the (scoped) block_on future.
    let lane_id = lane.id.to_string();
    let outcome = match &lane.outcome {
        Outcome::Refusal { version } => ExpectedOutcome::Refusal { version: *version },
        Outcome::Accept {
            supports_fast_auth,
            supports_end_of_response,
        } => ExpectedOutcome::Accept {
            supports_fast_auth: *supports_fast_auth,
            supports_end_of_response: *supports_end_of_response,
        },
    };
    let expected_version = manifest.get("protocol_version").cloned();

    let runtime = build_io_runtime()?;
    runtime.block_on(async move {
        let cx = Cx::current()
            .ok_or_else(|| Error::Runtime("missing ambient Cx in replay runtime".into()))?;
        let accept = drive_connect_handshake(&mut core, &cx, &connect_data).await?;
        let payload = accept.payload.as_slice();
        match outcome {
            ExpectedOutcome::Refusal { version } => match parse_accept_payload(payload) {
                Err(ProtocolError::UnsupportedVersion {
                    version: got,
                    minimum,
                }) => {
                    assert_eq!(got, version, "{lane_id}: refused version");
                    assert_eq!(
                        minimum,
                        oracledb_protocol::TNS_VERSION_MIN_ACCEPTED,
                        "{lane_id}: floor"
                    );
                }
                other => {
                    return Err(Error::Runtime(format!(
                        "{lane_id}: expected UnsupportedVersion, got {other:?}"
                    )));
                }
            },
            ExpectedOutcome::Accept {
                supports_fast_auth,
                supports_end_of_response,
            } => {
                let info = parse_accept_payload(payload)
                    .map_err(|e| Error::Runtime(format!("{lane_id}: parse ACCEPT: {e}")))?;
                assert_eq!(
                    info.supports_fast_auth, supports_fast_auth,
                    "{lane_id}: fast_auth"
                );
                assert_eq!(
                    info.supports_end_of_response, supports_end_of_response,
                    "{lane_id}: end_of_response"
                );
                // Manifest and decoded bytes must agree on the protocol version.
                if let Some(expected) = expected_version {
                    assert_eq!(
                        info.protocol_version.to_string(),
                        expected,
                        "{lane_id}: protocol_version"
                    );
                }
            }
        }
        Ok::<_, Error>(())
    })?;

    audit
        .assert_finished()
        .map_err(|err| Error::Runtime(format!("{}: {err}", lane.id)))?;
    Ok(())
}

/// Offline, no-database replay of every committed version-connect cassette.
/// This is the per-PR cross-version regression gate: it runs in ordinary
/// `cargo test --features cassette`, reconstructs each version's CONNECT request
/// and asserts it byte-matches the recording, then decodes the REAL server
/// ACCEPT and asserts the version-gated outcome.
#[test]
fn replay_version_connect_cassettes_offline() {
    let mut failures = Vec::new();
    for lane in lanes() {
        // A lane whose cassette has not been recorded yet is skipped rather than
        // failing the suite (the capture is operator-run against live lanes).
        if !cassette_path(lane.id).exists() {
            eprintln!("skip {}: no committed cassette", lane.id);
            continue;
        }
        if let Err(err) = replay_lane(&lane) {
            failures.push(err.to_string());
        }
    }
    assert!(failures.is_empty(), "replay failures: {failures:?}");
}

// ---- POST-AUTH validation (cwsr) ------------------------------------------
//
// The first cassette set stops at ACCEPT because the auth phase is
// non-deterministic (client `OsRng` session key) and carries secrets. To reach
// a post-auth typed query we use the "slice + loopback" approach (bead
// `rust-oracledb-cwsr`, option b): capture a full live session, drop the
// connect+auth prefix, and replay ONLY the deterministic, secret-free post-auth
// frames against a loopback [`Connection`] that is *seeded* with the negotiated
// caps and the post-auth `ttc_seq_num` the live session had.
//
// The reason seeding is required — and why a naive "start a fresh connection at
// the query" fails — is that the execute request bytes are shaped by
// `ttc_field_version` (a negotiated cap) AND by the `ttc_seq_num` counter, which
// the auth phase has already advanced by a lane-specific amount (fast-auth vs
// classic send a different number of TTC messages). Server-assigned values
// (cursor id, SCN) travel back in the recorded responses, so the replay
// reproduces them for free; only the client-side counter and caps must be
// carried across the slice.
//
// This test is the empirical proof that the post-auth C->S request bytes are
// byte-reproducible offline. It self-validates in one process (capture then
// replay) so it needs no committed fixture; the committed-cassette harness is
// built on top of a green result here.

/// Build a loopback [`Connection`] over a replay `core`, seeded with the
/// negotiated caps and post-auth `ttc_seq_num` of a captured live session, so
/// its post-auth request bytes reproduce the recording byte-for-byte.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn loopback_for_replay(
    core: ConnectionCore<DriverTransport>,
    capabilities: oracledb_protocol::thin::ClientCapabilities,
    ttc_seq_num: u8,
    supports_end_of_response: bool,
    supports_oob: bool,
    sdu: usize,
) -> crate::Connection {
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
    crate::Connection {
        descriptor: crate::EasyConnect::parse("127.0.0.1:1521/FREEPDB1")
            .expect("loopback descriptor parses"),
        identity: oracledb_protocol::ClientIdentity::new(
            "cassette-replay",
            "cassette-replay",
            "cassette",
            "unknown",
            "rust-oracledb-cassette",
        )
        .expect("loopback identity is valid"),
        core,
        protocol_limits: ProtocolLimits::DEFAULT,
        session_id: 0,
        serial_num: 0,
        server_version: None,
        server_version_tuple: None,
        capabilities,
        ttc_seq_num,
        sdu,
        supports_end_of_response,
        supports_oob,
        cursor_columns: BTreeMap::new(),
        fetch_metadata_by_sql: HashMap::new(),
        fetch_metadata_order: VecDeque::new(),
        dead: false,
        user: "cassette-replay".into(),
        combo_key: Vec::new(),
        statement_cache: Vec::new(),
        statement_cache_size: crate::STATEMENT_CACHE_SIZE,
        in_use_cursors: HashSet::new(),
        lob_prefetch_cursors: BTreeSet::new(),
        copied_cursors: HashSet::new(),
        cursors_to_close: Vec::new(),
        sessionless_data: None,
        notification_buffer: Vec::new(),
        notification_header_consumed: false,
        transaction_context: None,
        txn_in_progress: false,
    }
}

/// Re-encode `frames` into a fresh cassette byte stream, normalizing every
/// frame timestamp to `0` so the slice is deterministic regardless of capture
/// timing (replay compares bytes, not timing).
fn reencode_frames(frames: &[cassette::Frame]) -> Vec<u8> {
    let mut out = Vec::new();
    cassette::write_header(&mut out);
    for frame in frames {
        cassette::write_frame(&mut out, frame.direction, 0, &frame.bytes);
    }
    out
}

/// The typed query captured post-auth: a bind-free scalar whose request bytes
/// are fully determined by the negotiated caps + `ttc_seq_num`, and whose result
/// (`12`) is trivially assertable offline. No literal contains a `"` or `=`, so
/// it round-trips through the manifest cleanly.
const POSTAUTH_SQL: &str = "select cast(7 + 5 as number(6)) as v from dual";
/// The decoded scalar the post-auth replay must reproduce.
const POSTAUTH_EXPECTED_VALUE: &str = "12";

/// A post-auth lane. Unlike the connect lanes these authenticate, so they carry
/// per-lane default credentials — lab-only, and never written into a cassette
/// (the slice starts after auth).
struct PostAuthLane {
    id: &'static str,
    default_connect: &'static str,
    default_user: &'static str,
    default_password: &'static str,
}

/// The lanes covered by the post-auth query cassettes. xe11 is absent: it is
/// below the protocol floor and never authenticates, so it has no post-auth
/// phase to record.
fn postauth_lanes() -> Vec<PostAuthLane> {
    vec![
        PostAuthLane {
            id: "xe18",
            default_connect: "localhost:1518/XEPDB1",
            default_user: "testuser",
            default_password: "testpw",
        },
        PostAuthLane {
            id: "xe21",
            default_connect: "localhost:1520/XEPDB1",
            default_user: "testuser",
            default_password: "testpw",
        },
        PostAuthLane {
            id: "free23",
            default_connect: "localhost:1522/FREEPDB1",
            default_user: "pythontest",
            default_password: "pythontest",
        },
    ]
}

fn postauth_cassette_path(lane_id: &str) -> PathBuf {
    fixtures_dir().join(format!("{lane_id}-postauth.tns-cassette"))
}

fn postauth_manifest_path(lane_id: &str) -> PathBuf {
    fixtures_dir().join(format!("{lane_id}-postauth.tns-cassette.manifest"))
}

/// The secret-free post-auth execute slice plus the negotiated state a loopback
/// [`Connection`] must be seeded with to reproduce the recorded request bytes.
struct PostAuthCapture {
    service: String,
    sliced: Vec<u8>,
    capabilities: oracledb_protocol::thin::ClientCapabilities,
    ttc_seq_num: u8,
    supports_end_of_response: bool,
    supports_oob: bool,
    sdu: usize,
    value: Option<String>,
}

/// Capture `connect + auth + execute` against a live lane, then slice off the
/// connect+auth prefix and the trailing logoff, keeping only the deterministic,
/// secret-free execute request/response frames. Errors (never panics) so the
/// record loop can aggregate per-lane failures.
fn capture_postauth(connect: &str, user: &str, password: &str) -> Result<PostAuthCapture> {
    use oracledb_protocol::thin::{ExecuteOptions, QueryValue};

    let (_, _, service) = split_connect(connect)?;
    let runtime = build_io_runtime()?;
    let (full_bytes, prefix_frames, seq0, caps, eor, oob, sdu, value) =
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("missing ambient Cx in capture runtime".into()))?;
            let scope = transport::capture_scope();
            let identity = oracledb_protocol::ClientIdentity::new(
                "cassette-postauth",
                "cassette-capture",
                "cassette",
                "unknown",
                "rust-oracledb-cassette",
            )
            .map_err(|e| Error::Runtime(e.to_string()))?;
            let options = crate::ConnectOptions::new(
                connect.to_string(),
                user.to_string(),
                password.to_string(),
                identity,
            );
            let mut conn = crate::Connection::connect(&cx, options).await?;

            // Boundary: every frame so far is connect + auth (the sliced-off prefix).
            // The counter/caps captured here are what the loopback must be seeded
            // with, because the execute request bytes depend on both.
            let prefix_frames = scope.recorder().frame_count();
            let seq0 = conn.ttc_seq_num;
            let caps = conn.capabilities;
            let eor = conn.supports_end_of_response;
            let oob = conn.supports_oob;
            let sdu = conn.sdu;

            let exec = conn
                .execute_raw(&cx, POSTAUTH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await?;
            let value = exec
                .cell(0, 0)
                .and_then(QueryValue::as_number_text)
                .map(|c| c.to_string());

            let full = scope.to_cassette_bytes();
            // Best-effort logoff so the capture leaves the session clean; its frames
            // are past the execute slice and are dropped below.
            conn.close(&cx).await.ok();
            Ok::<_, Error>((full, prefix_frames, seq0, caps, eor, oob, sdu, value))
        })?;

    let all_frames = cassette::decode_all(&full_bytes)
        .map_err(|e| Error::Runtime(format!("decode capture: {e}")))?;
    if prefix_frames >= all_frames.len() {
        return Err(Error::Runtime(format!(
            "no post-auth frames after the connect+auth prefix ({prefix_frames} of {})",
            all_frames.len()
        )));
    }
    // Keep post-auth frames until the client's SECOND write: the first is the
    // execute request; the second is the close/logoff piggyback, which we drop.
    let post = &all_frames[prefix_frames..];
    let mut end = post.len();
    let mut seen_client_write = false;
    for (idx, frame) in post.iter().enumerate() {
        if frame.direction == cassette::Direction::ClientToServer {
            if seen_client_write {
                end = idx;
                break;
            }
            seen_client_write = true;
        }
    }
    let sliced = reencode_frames(&post[..end]);

    // Belt-and-suspenders: the slice must carry NO auth-phase secret field name.
    let leaks = scan_for_secret_fields(&sliced);
    if !leaks.is_empty() {
        return Err(Error::Runtime(format!(
            "post-auth slice leaked secrets: {leaks:?}"
        )));
    }

    Ok(PostAuthCapture {
        service,
        sliced,
        capabilities: caps,
        ttc_seq_num: seq0,
        supports_end_of_response: eor,
        supports_oob: oob,
        sdu,
        value,
    })
}

/// Replay a sliced post-auth cassette against a loopback seeded from `caps` /
/// `ttc_seq_num`, returning the decoded scalar. `ReplayWriteMode::Check` asserts
/// every client byte matches the recording; the audit asserts full consumption.
#[allow(clippy::too_many_arguments)]
fn replay_postauth(
    sliced: &[u8],
    capabilities: oracledb_protocol::thin::ClientCapabilities,
    ttc_seq_num: u8,
    supports_end_of_response: bool,
    supports_oob: bool,
    sdu: usize,
) -> Result<Option<String>> {
    use oracledb_protocol::thin::{ExecuteOptions, QueryValue};

    let (read, write, audit) = transport::replay_split_with_audit(sliced, ReplayWriteMode::Check)
        .map_err(|e| Error::Runtime(format!("replay split: {e}")))?;
    let core = ConnectionCore::<DriverTransport>::from_halves(read, write, "postauth_replay");
    let mut conn = loopback_for_replay(
        core,
        capabilities,
        ttc_seq_num,
        supports_end_of_response,
        supports_oob,
        sdu,
    );

    let runtime = build_io_runtime()?;
    let value = runtime.block_on(async {
        let cx = Cx::current()
            .ok_or_else(|| Error::Runtime("missing ambient Cx in replay runtime".into()))?;
        let exec = conn
            .execute_raw(&cx, POSTAUTH_SQL, 2, &[], ExecuteOptions::default(), None)
            .await?;
        Ok::<_, Error>(
            exec.cell(0, 0)
                .and_then(QueryValue::as_number_text)
                .map(|c| c.to_string()),
        )
    })?;

    audit
        .assert_finished()
        .map_err(|e| Error::Runtime(format!("post-auth replay audit: {e}")))?;
    Ok(value)
}

fn build_postauth_manifest(lane_id: &str, cap: &PostAuthCapture) -> Result<String> {
    let write_hashes = write_frame_hashes(&cap.sliced)?;
    Ok(format!(
        concat!(
            "schema_version = {}\n",
            "format_version = {}\n",
            "commit = \"{}\"\n",
            "profile = \"post-auth-query\"\n",
            "lane = \"{}\"\n",
            "service = \"{}\"\n",
            "scenario = \"execute_select\"\n",
            "sql = \"{}\"\n",
            "ttc_field_version = {}\n",
            "charset_id = {}\n",
            "max_string_size = {}\n",
            "ttc_seq_num = {}\n",
            "supports_end_of_response = {}\n",
            "supports_oob = {}\n",
            "sdu = {}\n",
            "expected_value = \"{}\"\n",
            "sanitized = true\n",
            "checksum_sha256 = \"{}\"\n",
            "expected_writes = {}\n",
            "expected_write_sha256 = \"{}\"\n",
        ),
        MANIFEST_SCHEMA_VERSION,
        CASSETTE_FORMAT_VERSION,
        SOURCE_COMMIT.trim(),
        lane_id,
        cap.service,
        POSTAUTH_SQL,
        cap.capabilities.ttc_field_version,
        cap.capabilities.charset_id,
        cap.capabilities.max_string_size,
        cap.ttc_seq_num,
        cap.supports_end_of_response,
        cap.supports_oob,
        cap.sdu,
        cap.value.as_deref().unwrap_or(""),
        sha256_hex(&cap.sliced),
        write_hashes.len(),
        write_hashes.join(","),
    ))
}

/// Record every lane's post-auth query cassette against the live version fleet.
/// Ignored by default (needs the Docker lanes up); run explicitly:
///
/// ```text
/// cargo test -p oracledb --features cassette \
///   record_postauth_query_cassettes -- --ignored --nocapture
/// ```
///
/// Per-lane connect strings default to the `version_matrix.sh` ports and are
/// overridable via `ORACLEDB_CASSETTE_XE18` / `_XE21` / `_FREE23`; credentials
/// via `ORACLEDB_CASSETTE_<ID>_USER` / `_PASSWORD`. Each capture is re-verified
/// offline before its fixture is written.
#[test]
#[ignore = "records the live post-auth query cassettes; needs the Docker lanes"]
fn record_postauth_query_cassettes() {
    let out_dir = std::env::var("ORACLEDB_CASSETTE_RECORD")
        .map(PathBuf::from)
        .unwrap_or_else(|_| fixtures_dir());
    fs::create_dir_all(&out_dir).expect("create fixtures dir");

    let mut failures = Vec::new();
    for lane in postauth_lanes() {
        let up = lane.id.to_uppercase();
        let connect = std::env::var(format!("ORACLEDB_CASSETTE_{up}"))
            .unwrap_or_else(|_| lane.default_connect.to_string());
        let user = std::env::var(format!("ORACLEDB_CASSETTE_{up}_USER"))
            .unwrap_or_else(|_| lane.default_user.to_string());
        let password = std::env::var(format!("ORACLEDB_CASSETTE_{up}_PASSWORD"))
            .unwrap_or_else(|_| lane.default_password.to_string());

        let cap = match capture_postauth(&connect, &user, &password) {
            Ok(cap) => cap,
            Err(err) => {
                failures.push(format!("{}: {err}", lane.id));
                continue;
            }
        };
        // Re-verify offline against a seeded loopback before committing.
        match replay_postauth(
            &cap.sliced,
            cap.capabilities,
            cap.ttc_seq_num,
            cap.supports_end_of_response,
            cap.supports_oob,
            cap.sdu,
        ) {
            Ok(v) if v.as_deref() == Some(POSTAUTH_EXPECTED_VALUE) => {}
            Ok(v) => {
                failures.push(format!(
                    "{}: replay value {v:?} != {POSTAUTH_EXPECTED_VALUE:?}",
                    lane.id
                ));
                continue;
            }
            Err(err) => {
                failures.push(format!("{}: pre-commit replay {err}", lane.id));
                continue;
            }
        }
        let manifest = match build_postauth_manifest(lane.id, &cap) {
            Ok(m) => m,
            Err(err) => {
                failures.push(format!("{}: manifest {err}", lane.id));
                continue;
            }
        };
        let cass = out_dir.join(format!("{}-postauth.tns-cassette", lane.id));
        let man = out_dir.join(format!("{}-postauth.tns-cassette.manifest", lane.id));
        if let Err(err) = fs::write(&cass, &cap.sliced) {
            failures.push(format!("{}: write cassette {err}", lane.id));
            continue;
        }
        if let Err(err) = fs::write(&man, manifest) {
            failures.push(format!("{}: write manifest {err}", lane.id));
            continue;
        }
        eprintln!(
            "recorded {} post-auth ({} bytes) -> {}",
            lane.id,
            cap.sliced.len(),
            cass.display()
        );
    }
    assert!(
        failures.is_empty(),
        "post-auth capture failures: {failures:?}"
    );
}

/// Replay one committed post-auth cassette offline and assert the seeded
/// loopback re-derives the recorded scalar with byte-matching request bytes.
fn replay_postauth_lane_offline(lane_id: &str) -> Result<()> {
    let cassette_bytes =
        fs::read(postauth_cassette_path(lane_id)).map_err(|e| Error::Runtime(e.to_string()))?;
    let manifest_text = fs::read_to_string(postauth_manifest_path(lane_id))
        .map_err(|e| Error::Runtime(e.to_string()))?;
    let manifest = parse_manifest(&manifest_text);

    // Integrity + sanitization hold for the committed artifact.
    let expected_checksum = manifest
        .get("checksum_sha256")
        .ok_or_else(|| Error::Runtime("manifest missing checksum_sha256".into()))?;
    if &sha256_hex(&cassette_bytes) != expected_checksum {
        return Err(Error::Runtime(format!(
            "{lane_id}: cassette checksum != manifest"
        )));
    }
    let leaks = scan_for_secret_fields(&cassette_bytes);
    if !leaks.is_empty() {
        return Err(Error::Runtime(format!("{lane_id}: secret leak {leaks:?}")));
    }

    let need = |key: &str| -> Result<String> {
        manifest
            .get(key)
            .cloned()
            .ok_or_else(|| Error::Runtime(format!("{lane_id}: manifest missing {key}")))
    };
    let parse_u8 = |key: &str| -> Result<u8> {
        need(key)?
            .parse()
            .map_err(|_| Error::Runtime(format!("{lane_id}: bad {key}")))
    };
    let ttc_field_version = parse_u8("ttc_field_version")?;
    let charset_id: u16 = need("charset_id")?
        .parse()
        .map_err(|_| Error::Runtime(format!("{lane_id}: bad charset_id")))?;
    let max_string_size: u32 = need("max_string_size")?
        .parse()
        .map_err(|_| Error::Runtime(format!("{lane_id}: bad max_string_size")))?;
    let ttc_seq_num = parse_u8("ttc_seq_num")?;
    let eor: bool = need("supports_end_of_response")?
        .parse()
        .map_err(|_| Error::Runtime(format!("{lane_id}: bad supports_end_of_response")))?;
    let oob: bool = need("supports_oob")?
        .parse()
        .map_err(|_| Error::Runtime(format!("{lane_id}: bad supports_oob")))?;
    let sdu: usize = need("sdu")?
        .parse()
        .map_err(|_| Error::Runtime(format!("{lane_id}: bad sdu")))?;
    let expected_value = need("expected_value")?;

    let caps = oracledb_protocol::thin::ClientCapabilities {
        ttc_field_version,
        max_string_size,
        charset_id,
    };
    let value = replay_postauth(&cassette_bytes, caps, ttc_seq_num, eor, oob, sdu)?;
    if value.as_deref() != Some(expected_value.as_str()) {
        return Err(Error::Runtime(format!(
            "{lane_id}: replay value {value:?} != {expected_value:?}"
        )));
    }
    Ok(())
}

/// Offline, no-database replay of every committed post-auth query cassette.
/// This is the per-PR gate: in ordinary `cargo test --features cassette` it
/// reconstructs each lane's seeded loopback, replays the recorded execute with
/// `ReplayWriteMode::Check` (so a request-byte regression fails here), and
/// asserts the decoded scalar. Lanes without a committed cassette are skipped.
#[test]
fn replay_postauth_query_cassettes_offline() {
    let mut failures = Vec::new();
    for lane in postauth_lanes() {
        if !postauth_cassette_path(lane.id).exists() {
            eprintln!("skip {}: no committed post-auth cassette", lane.id);
            continue;
        }
        if let Err(err) = replay_postauth_lane_offline(lane.id) {
            failures.push(err.to_string());
        }
    }
    assert!(
        failures.is_empty(),
        "post-auth replay failures: {failures:?}"
    );
}

// ---- Substrate integrity guard (a4-1s2) -----------------------------------
//
// The deterministic cassette-replay CI is only a force-multiplier if EVERY
// committed cassette is actually exercised offline. A `.tns-cassette` that is
// added to the fixtures directory but never wired to a replay lane would rot
// silently — the per-lane replay tests skip *missing* cassettes, but nothing
// catches an *orphan* cassette that no test reads. This guard closes that gap
// from the other direction: it enumerates the committed cassettes and fails if
// any is not covered by a known replay path (connect lane, post-auth lane, or
// the synthetic record/replay fixture). New cassettes (e.g. the a4-nnnz LOB/AQ
// post-auth set) must be registered on a lane here to pass.

/// The synthetic fixture exercised by `tests/cassette_record_replay.rs`.
const SYNTHETIC_FIXTURE: &str = "select_7_plus_5.tns-cassette";

/// Every committed `.tns-cassette` that a replay test is expected to cover.
fn expected_cassette_files() -> std::collections::BTreeSet<String> {
    let mut expected = std::collections::BTreeSet::new();
    for lane in lanes() {
        expected.insert(format!("{}-connect.tns-cassette", lane.id));
    }
    for lane in postauth_lanes() {
        expected.insert(format!("{}-postauth.tns-cassette", lane.id));
    }
    expected.insert(SYNTHETIC_FIXTURE.to_string());
    expected
}

/// Fail if any committed `.tns-cassette` is not wired to a replay lane, and if
/// any lane's committed cassette is empty (a truncated capture that would
/// otherwise replay as a vacuous pass).
#[test]
fn every_committed_cassette_is_covered_by_a_replay_lane() {
    let dir = fixtures_dir();
    let expected = expected_cassette_files();

    let mut orphans = Vec::new();
    let mut empty = Vec::new();
    for entry in fs::read_dir(&dir).expect("fixtures/cassettes dir is readable") {
        let entry = entry.expect("dir entry is readable");
        let name = entry.file_name().to_string_lossy().into_owned();
        // Cassettes only; skip the `.manifest` sidecars and anything else.
        if !name.ends_with(".tns-cassette") {
            continue;
        }
        if !expected.contains(&name) {
            orphans.push(name.clone());
        }
        let len = fs::metadata(entry.path())
            .expect("cassette metadata is readable")
            .len();
        if len == 0 {
            empty.push(name);
        }
    }

    assert!(
        orphans.is_empty(),
        "orphan cassette(s) with no replay lane: {orphans:?}; \
         wire them onto a lane in version_cassettes.rs (or the synthetic fixture set)"
    );
    assert!(empty.is_empty(), "empty committed cassette(s): {empty:?}");
}
