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
//! Broader per-version op coverage — a post-auth typed query, and LOB / AQ /
//! DPL / CQN round-trips — is tracked as a follow-up (needs the auth phase, so
//! it must slice+scrub a full capture; see the discovered-from bead).

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
