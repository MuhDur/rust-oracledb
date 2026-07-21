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
//! pair below.
//!
//! # LOB post-auth cassettes (bead a4-nnnz)
//!
//! The same slice+loopback machinery is generalized over a [`PostAuthScenario`]
//! so it can carry more than a typed scalar. The first extra surface is a temp
//! **BLOB** round-trip — `create_temp_lob` + `write_lob` + `read_lob` — committed
//! as `<lane>-lob.tns-cassette`. Its three LOB TTC calls are byte-deterministic:
//! the server-assigned locator travels back in the recorded create response and
//! the client only echoes it, so write/read replay byte-exact under
//! `ReplayWriteMode::Check`.
//!
//! # AQ post-auth cassettes + decoded-assert replay (bead iec3.1.32)
//!
//! AQ (Advanced Queuing: `DBMS_AQ` enqueue / dequeue) and DPL (direct-path load)
//! do **not** replay byte-exact across separate captures the way the scalar / LOB
//! scenarios do. Their request bytes embed a **server-assigned id** that the
//! server picks fresh on every run — AQ dequeue-by-message-id echoes the 16-byte
//! message id the enqueue returned (and enqueue may carry a 16-byte transaction
//! id); DPL echoes the server-assigned direct-path cursor id. A recording made in
//! run #1 (id = X) and a recording made in run #2 (id = Y) differ **only** in
//! those id bytes, so a byte-exact [`ReplayWriteMode::Check`] would reject run #2
//! against run #1 even though the two are semantically identical.
//!
//! [`ReplayMode::DecodedAssert`] is the replay model for those surfaces. It
//! relaxes the byte-exact write check to [`ReplayWriteMode::Ignore`] and proves
//! fidelity two ways instead: (1) the driver's decoded semantic return value
//! (the dequeued payload) must equal the recording, and (2) the driver's
//! re-issued request stream must equal the recording after **masking every
//! server-assigned id byte-run** — i.e. the ONLY tolerated differences are
//! id-shaped runs whose length is a known id length (16 for an AQ message /
//! transaction id). Any other request divergence still fails. The offline
//! `decoded_assert_survives_server_id_divergence_that_check_rejects` test proves
//! the model with no database: it takes a committed cassette whose request echoes
//! a server-assigned id, mutates only those id bytes (simulating a second capture
//! run), and shows the mutated cassette replays green under `DecodedAssert` but
//! is rejected under `Check`.
//!
//! Both AQ and DPL are captured this way. DPL (direct-path load) echoes the
//! server-assigned **direct-path cursor id** in its load-stream and finish
//! requests — the same class of id — so it too replays under `DecodedAssert`
//! (`PostAuthScenario::DplLoadReadback`, `<lane>-dpl.tns-cassette`). CQN (change
//! notification) remains a follow-up: its registration id is the same class of
//! server-assigned id, so the model already covers it — only the capture scenario
//! is unwritten.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use asupersync::net::TcpStream;
use asupersync::Cx;
use sha2::{Digest, Sha256};

use oracledb_protocol::net::cassette::{self, Direction};
use oracledb_protocol::thin::{
    build_connect_packet_payload, parse_accept_payload, TNS_AQ_MESSAGE_ID_LENGTH,
    TNS_PACKET_TYPE_ACCEPT, TNS_PACKET_TYPE_CONNECT, TNS_PACKET_TYPE_RESEND,
};
use oracledb_protocol::wire::{encode_packet, PacketLengthWidth, ProtocolLimits, TtcWriter};
use oracledb_protocol::ProtocolError;

use crate::transport::{self, scan_for_secret_fields, ReplayWriteMode};
use crate::{
    build_io_runtime, ConnectionCore, DriverTransport, Error, IncomingPacket, Result,
    MAX_CONNECT_RESEND_ROUNDS,
};

/// SDU advertised in every capture/replay CONNECT packet. Fixed so the request
/// bytes are reproducible offline.
const ADVERTISED_SDU: u16 = 8192;

/// Below-floor protocol version Oracle 11g negotiates (12.1 = 315 is the floor).
const ORACLE_11G_PROTOCOL_VERSION: u16 = 314;

/// D11's offline 19c-shaped profile. This is a reference-derived fixture, not
/// an assertion about a particular live 19c server's ACCEPT bytes.
const SYNTHETIC_19C_PROTOCOL_VERSION: u16 = 318;

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

// The auth-phase secret-field scanner (`SECRET_FIELD_NAMES` +
// `scan_for_secret_fields`) is the single source of truth in
// `crate::transport` (bead K6); it is imported above and reused here so the
// refuse gate can never drift between the two capture paths.

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

/// Build the smallest ACCEPT layout that selects the 19c protocol branch:
/// protocol 318 reads flags2 but does not enable end-of-response (which starts
/// at 319). The fixture deliberately advertises neither optional flag.
fn synthetic_19c_accept_payload() -> Vec<u8> {
    let mut writer = TtcWriter::new();
    writer.write_u16be(SYNTHETIC_19C_PROTOCOL_VERSION);
    writer.write_u16be(0); // protocol options: no OOB attention
    writer.write_raw(&[0; 10]);
    writer.write_u8(0); // flags1: no native network encryption requirement
    writer.write_raw(&[0; 9]);
    writer.write_u32be(u32::from(ADVERTISED_SDU));
    writer.write_raw(&[0; 5]);
    writer.write_u32be(0); // flags2: no fast auth, no OOB check, no EOR
    writer.into_bytes()
}

/// A full secret-free `.tns-cassette` exchange for the synthetic 19c profile.
/// It uses the same packet writer and replay transport as recorded lanes, but
/// never needs a database or a committed binary fixture.
fn synthetic_19c_caps_cassette() -> Result<Vec<u8>> {
    let connect_data = capture_connect_descriptor("NINETEEN_C_PROFILE");
    let connect_payload = build_connect_packet_payload(&connect_data, ADVERTISED_SDU)?;
    let connect_packet = encode_packet(
        TNS_PACKET_TYPE_CONNECT,
        0,
        None,
        &connect_payload,
        PacketLengthWidth::Legacy16,
    )?;
    let accept_packet = encode_packet(
        TNS_PACKET_TYPE_ACCEPT,
        0,
        None,
        &synthetic_19c_accept_payload(),
        PacketLengthWidth::Legacy16,
    )?;

    let mut cassette = Vec::new();
    cassette::write_header(&mut cassette);
    cassette::write_frame(&mut cassette, Direction::ClientToServer, 0, &connect_packet);
    cassette::write_frame(&mut cassette, Direction::ServerToClient, 1, &accept_packet);
    Ok(cassette)
}

/// Offline 19c-caps lane: strict replay reissues the normal CONNECT request,
/// consumes the synthetic ACCEPT, and proves the protocol-318 selection stays
/// `fast_auth=false` / `end_of_response=false`.
#[test]
fn replay_synthetic_19c_caps_cassette_offline() -> Result<()> {
    let cassette_bytes = synthetic_19c_caps_cassette()?;
    assert!(
        scan_for_secret_fields(&cassette_bytes).is_empty(),
        "synthetic handshake must remain secret-free"
    );
    let (read, write, audit) =
        transport::replay_split_with_audit(&cassette_bytes, ReplayWriteMode::Check)
            .map_err(|err| Error::Runtime(format!("invalid 19c profile cassette: {err}")))?;
    let mut core = ConnectionCore::<DriverTransport>::from_halves(read, write, "19c-profile");
    core.set_protocol_limits(ProtocolLimits::DEFAULT)?;
    let connect_data = capture_connect_descriptor("NINETEEN_C_PROFILE");

    let runtime = build_io_runtime()?;
    runtime.block_on(async move {
        let cx = Cx::current()
            .ok_or_else(|| Error::Runtime("missing ambient Cx in 19c replay runtime".into()))?;
        let accept = drive_connect_handshake(&mut core, &cx, &connect_data).await?;
        let info = parse_accept_payload(&accept.payload)?;
        assert_eq!(info.protocol_version, SYNTHETIC_19C_PROTOCOL_VERSION);
        assert!(!info.supports_fast_auth);
        assert!(!info.supports_end_of_response);
        Ok::<_, Error>(())
    })?;
    audit
        .assert_finished()
        .map_err(|err| Error::Runtime(format!("synthetic 19c replay: {err}")))?;
    Ok(())
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
        db_unique_name: None,
        capabilities,
        ttc_seq_num,
        sdu,
        protocol_version: 0,
        supports_fast_auth: false,
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
        shape_cache: std::sync::Arc::new(crate::StatementShapeCache::new()),
        capture_guard: None,
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

/// Keep the frames of a post-auth scenario that emits `client_writes` client
/// requests: every frame up to (but not including) the `client_writes + 1`-th
/// client write — that next write is the trailing close/logoff, which is
/// dropped. If the capture ends before that many writes (e.g. the logoff was
/// piggybacked or absent), the whole tail is kept.
fn slice_scenario_frames(post: &[cassette::Frame], client_writes: usize) -> &[cassette::Frame] {
    let mut seen = 0usize;
    for (idx, frame) in post.iter().enumerate() {
        if frame.direction == cassette::Direction::ClientToServer {
            seen += 1;
            if seen > client_writes {
                return &post[..idx];
            }
        }
    }
    post
}

/// The typed query captured post-auth: a bind-free scalar whose request bytes
/// are fully determined by the negotiated caps + `ttc_seq_num`, and whose result
/// (`12`) is trivially assertable offline. No literal contains a `"` or `=`, so
/// it round-trips through the manifest cleanly.
const POSTAUTH_SQL: &str = "select cast(7 + 5 as number(6)) as v from dual";
/// The decoded scalar the post-auth replay must reproduce.
const POSTAUTH_EXPECTED_VALUE: &str = "12";

/// The LOB post-auth scenario (bead a4-nnnz): create a temporary BLOB, write a
/// fixed payload into it, and read it back. This is the first post-auth cassette
/// that goes *beyond a typed scalar* — it exercises three locator-bearing LOB
/// TTC calls (create-temp, write, read). Each round-trip's request bytes are
/// deterministic given the negotiated caps + `ttc_seq_num`, plus the temp-LOB
/// locator, which the server hands back in the recorded create response and the
/// client only echoes — so write/read bytes are reproduced for free on replay.
/// A BLOB (binary) sidesteps CLOB wire-charset encoding entirely: the bytes
/// written are the bytes read.
const LOB_SCENARIO_DESC: &str = "lob: create_temp_lob + write_lob + read_lob (blob)";
/// The payload written then read back; the replay must reproduce it exactly.
/// Pure ASCII, and free of `"`/`=`, so it round-trips the manifest cleanly.
const LOB_EXPECTED_VALUE: &str = "rust-oracledb cassette blob payload";
/// Bytes requested per `read_lob` — comfortably larger than the payload so a
/// single read drains the whole BLOB.
const LOB_READ_AMOUNT: u64 = 4000;

/// The AQ post-auth scenario (bead iec3.1.32): enqueue one RAW message into a
/// single-consumer queue, then dequeue it **by message id**. The dequeue request
/// echoes the 16-byte server-assigned message id the enqueue returned, so — unlike
/// the LOB locator, which is reproduced byte-for-byte from the recorded response —
/// two independent captures differ in those id bytes and cannot share a byte-exact
/// `Check` replay. This is the archetypal [`ReplayMode::DecodedAssert`] surface.
///
/// The queue is pre-provisioned by `scripts/bootstrap_live_schema.sh`
/// (`DBMS_AQADM.create_queue_table` / `create_queue` / `start_queue`), so the
/// captured slice is just the two enqueue/dequeue round-trips — no DDL round-trips
/// leak into the cassette.
const AQ_QUEUE_NAME: &str = "RUST_CASS_RAWQ";
/// Human-readable scenario description recorded in the manifest `sql` field.
/// Free of `"`/`=` so it round-trips the manifest cleanly.
const AQ_SCENARIO_DESC: &str = "aq: raw enqueue + dequeue-by-msgid (single-consumer)";
/// The RAW payload enqueued then dequeued; the replay must reproduce it exactly.
/// Pure ASCII, free of `"`/`=`, so it round-trips the manifest cleanly.
const AQ_EXPECTED_VALUE: &str = "rust-oracledb cassette aq raw payload";

/// The DPL (direct-path load) post-auth scenario (bead iec3.1.32): direct-path
/// load one single-column NUMBER row, then read it back. The load's server-
/// assigned **direct-path cursor id** (a ub2 the server picks in the PREPARE
/// response) is echoed in the load-stream and FINISH requests — the same class of
/// server-assigned id as the AQ message id — so this too is a
/// [`ReplayMode::DecodedAssert`] surface. Offline replay is still byte-exact (the
/// cursor id is reproduced from the recorded PREPARE response, like the LOB
/// locator); DecodedAssert bounds a *re-capture* divergence to the cursor-id
/// field. The table is pre-provisioned by `scripts/bootstrap_live_schema.sh`.
const DPL_TABLE_NAME: &str = "RUST_CASS_DPL";
/// The schema the direct-path load targets. Fixed (not `conn.user`) so the
/// PREPARE request bytes are identical on capture and on the loopback replay —
/// this is the synthetic free23 lab schema, the only lane DPL is captured on.
const DPL_SCHEMA: &str = "PYTHONTEST";
/// Single NUMBER column loaded and read back.
const DPL_COLUMN: &str = "v";
/// Read-back query (returns the loaded value). Every loaded row carries the same
/// value, so `rownum = 1` is deterministic even after a re-capture appends rows.
const DPL_READBACK_SQL: &str = "select v from RUST_CASS_DPL where rownum = 1";
/// Human-readable scenario description recorded in the manifest `sql` field.
const DPL_SCENARIO_DESC: &str = "dpl: direct-path load one number row + read-back";
/// The loaded (and read-back) NUMBER value. Free of `"`/`=` for the manifest.
const DPL_EXPECTED_VALUE: &str = "4242";

/// How an offline post-auth replay validates the driver's re-issued request bytes.
///
/// The two modes differ only in how strictly the *client write* stream is checked;
/// both always assert the decoded semantic return value. See the module docs.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ReplayMode {
    /// Byte-exact ([`ReplayWriteMode::Check`]): every re-issued client byte must
    /// equal the recording. Used by scenarios whose request bytes carry no
    /// server-assigned ids — the scalar select, and LOB (whose locator is echoed
    /// from the recorded response, so it too reproduces byte-for-byte).
    Check,
    /// Decoded-assert ([`ReplayWriteMode::Ignore`] plus a masked request compare):
    /// the request stream embeds server-assigned ids that differ run-to-run, so
    /// the byte-exact check is relaxed. Fidelity is proven by the decoded return
    /// value AND by requiring the re-issued request stream to equal the recording
    /// after masking every server-assigned id byte-run: the only tolerated
    /// differences are runs whose length is one of `id_lengths`.
    DecodedAssert { id_lengths: Vec<usize> },
}

/// A deterministic-or-decoded, secret-free post-auth flow. Request bytes are
/// fully determined by the negotiated caps + `ttc_seq_num` (plus, for LOB, the
/// server-assigned locator echoed back from the recorded execute response; for
/// AQ, the server-assigned message id — see [`ReplayMode`]).
///
/// The same [`drive_scenario`] runs in both capture and replay, so the client
/// byte-stream is identical on both sides. Byte-exact scenarios prove that under
/// `ReplayWriteMode::Check`; AQ proves it under [`ReplayMode::DecodedAssert`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PostAuthScenario {
    /// A bind-free scalar select (one execute round-trip).
    ExecuteSelect,
    /// A temp BLOB round-trip: create-temp, write, read (three LOB TTC calls).
    LobBlob,
    /// A RAW AQ round-trip: enqueue, then dequeue-by-message-id (two TTC calls).
    /// The dequeue request embeds the server-assigned message id, so this replays
    /// under [`ReplayMode::DecodedAssert`], not byte-exact `Check`.
    AqRawRoundTrip,
    /// A direct-path load of one NUMBER row, then a read-back (four TTC calls:
    /// prepare, load-stream, finish, select). The load-stream and finish requests
    /// embed the server-assigned direct-path cursor id, so this too replays under
    /// [`ReplayMode::DecodedAssert`].
    DplLoadReadback,
}

impl PostAuthScenario {
    /// Fixture filename suffix: `{lane}-{suffix}.tns-cassette`.
    fn suffix(self) -> &'static str {
        match self {
            PostAuthScenario::ExecuteSelect => "postauth",
            PostAuthScenario::LobBlob => "lob",
            PostAuthScenario::AqRawRoundTrip => "aq",
            PostAuthScenario::DplLoadReadback => "dpl",
        }
    }

    fn sql(self) -> &'static str {
        match self {
            PostAuthScenario::ExecuteSelect => POSTAUTH_SQL,
            PostAuthScenario::LobBlob => LOB_SCENARIO_DESC,
            PostAuthScenario::AqRawRoundTrip => AQ_SCENARIO_DESC,
            PostAuthScenario::DplLoadReadback => DPL_SCENARIO_DESC,
        }
    }

    fn expected_value(self) -> &'static str {
        match self {
            PostAuthScenario::ExecuteSelect => POSTAUTH_EXPECTED_VALUE,
            PostAuthScenario::LobBlob => LOB_EXPECTED_VALUE,
            PostAuthScenario::AqRawRoundTrip => AQ_EXPECTED_VALUE,
            PostAuthScenario::DplLoadReadback => DPL_EXPECTED_VALUE,
        }
    }

    /// Manifest `scenario` tag.
    fn tag(self) -> &'static str {
        match self {
            PostAuthScenario::ExecuteSelect => "execute_select",
            PostAuthScenario::LobBlob => "temp_lob_create_write_read",
            PostAuthScenario::AqRawRoundTrip => "aq_raw_enq_deq_by_msgid",
            PostAuthScenario::DplLoadReadback => "dpl_load_readback",
        }
    }

    /// How many client writes the scenario emits before the trailing logoff.
    /// The slice keeps exactly this many (and their responses), dropping the
    /// close. `ExecuteSelect` = 1 (unchanged from the original slicer);
    /// `LobBlob` = 3 (create-temp, write, read); `AqRawRoundTrip` = 2 (enqueue,
    /// dequeue); `DplLoadReadback` = 4 (prepare, load-stream, finish, select).
    fn client_writes(self) -> usize {
        match self {
            PostAuthScenario::ExecuteSelect => 1,
            PostAuthScenario::LobBlob => 3,
            PostAuthScenario::AqRawRoundTrip => 2,
            PostAuthScenario::DplLoadReadback => 4,
        }
    }

    /// The replay model this scenario's committed cassette is validated under.
    /// Byte-deterministic scenarios use `Check`; AQ and DPL use `DecodedAssert`
    /// because their requests embed a server-assigned id (AQ's 16-byte message
    /// id; DPL's ub2 direct-path cursor id).
    fn replay_mode(self) -> ReplayMode {
        match self {
            PostAuthScenario::ExecuteSelect | PostAuthScenario::LobBlob => ReplayMode::Check,
            PostAuthScenario::AqRawRoundTrip => ReplayMode::DecodedAssert {
                id_lengths: vec![TNS_AQ_MESSAGE_ID_LENGTH],
            },
            // The direct-path cursor id is a ub2, wire-encoded as a 1-3 byte
            // length-prefixed field. Offline replay reproduces it byte-for-byte
            // (echoed from the recorded PREPARE response), so these tolerances
            // only bound a re-capture divergence.
            PostAuthScenario::DplLoadReadback => ReplayMode::DecodedAssert {
                id_lengths: vec![1, 2, 3],
            },
        }
    }

    /// Whether this scenario is captured on `lane_id`. AQ needs a pre-provisioned
    /// queue + `aq_administrator_role`, and DPL a pre-provisioned target table,
    /// which only the free23 lane's schema has — so both are captured there
    /// alone; the byte-exact scenarios run every lane.
    fn applies_to_lane(self, lane_id: &str) -> bool {
        match self {
            PostAuthScenario::AqRawRoundTrip | PostAuthScenario::DplLoadReadback => {
                lane_id == "free23"
            }
            _ => true,
        }
    }
}

/// Drive a post-auth scenario on `conn`, returning the decoded assertion value.
/// Shared verbatim by capture and replay so the emitted client bytes match.
#[cfg(test)]
async fn drive_scenario(
    conn: &mut crate::Connection,
    cx: &Cx,
    scenario: PostAuthScenario,
) -> Result<Option<String>> {
    match scenario {
        PostAuthScenario::ExecuteSelect => {
            use oracledb_protocol::thin::{ExecuteOptions, QueryValue};
            let exec = conn
                .execute_raw(cx, scenario.sql(), 2, &[], ExecuteOptions::default(), None)
                .await?;
            Ok(exec
                .cell(0, 0)
                .and_then(QueryValue::as_number_text)
                .map(|c| c.to_string()))
        }
        PostAuthScenario::LobBlob => {
            use oracledb_protocol::thin::{CS_FORM_IMPLICIT, ORA_TYPE_NUM_BLOB};
            // Create a temp BLOB, write the payload at byte offset 1, read it back.
            // The locator comes back in the create response; write/read only echo
            // it, so all three request byte-streams are deterministic on replay.
            // BLOB is binary — the bytes read are exactly the bytes written.
            let temp = conn
                .create_temp_lob(cx, ORA_TYPE_NUM_BLOB, CS_FORM_IMPLICIT)
                .await?;
            let locator = temp.locator;
            conn.write_lob(cx, &locator, 1, LOB_EXPECTED_VALUE.as_bytes())
                .await?;
            let read = conn.read_lob(cx, &locator, 1, LOB_READ_AMOUNT).await?;
            let bytes = read.data.unwrap_or_default();
            let text = String::from_utf8(bytes)
                .map_err(|e| Error::Runtime(format!("BLOB read not UTF-8: {e}")))?;
            Ok(Some(text))
        }
        PostAuthScenario::AqRawRoundTrip => {
            use oracledb_protocol::thin::aq::{
                AqDeqOptions, AqDeqPayload, AqEnqOptions, AqMsgProps, AqPayloadKind,
                AqPayloadValue, AqQueueDesc,
            };
            // Enqueue one RAW message with IMMEDIATE visibility (visibility=1) so
            // it is committed without a separate COMMIT round-trip and is
            // dequeuable in the same session. The enqueue returns the 16-byte
            // server-assigned message id.
            let queue = AqQueueDesc::new(AQ_QUEUE_NAME.to_string(), AqPayloadKind::Raw, None);
            let props = AqMsgProps {
                payload: Some(AqPayloadValue::Raw(AQ_EXPECTED_VALUE.as_bytes().to_vec())),
                ..AqMsgProps::default()
            };
            let enq_options = AqEnqOptions {
                visibility: 1,
                ..AqEnqOptions::default()
            };
            let msgid = conn
                .aq_enq_one(cx, &queue, &props, &enq_options)
                .await?
                .ok_or_else(|| Error::Runtime("AQ enqueue returned no message id".into()))?;
            // Dequeue BY the server-assigned message id: the request embeds those
            // 16 bytes, so it is NOT byte-reproducible across independent captures
            // — hence DecodedAssert. IMMEDIATE visibility (visibility=1) removes
            // the message in an autonomous transaction, no COMMIT round-trip.
            let deq_options = AqDeqOptions {
                visibility: 1,
                msgid: Some(msgid),
                ..AqDeqOptions::default()
            };
            let result = conn.aq_deq_one(cx, &queue, &deq_options).await?;
            let text = match result.message.and_then(|m| m.payload) {
                Some(AqDeqPayload::Raw(bytes)) => Some(
                    String::from_utf8(bytes)
                        .map_err(|e| Error::Runtime(format!("AQ RAW payload not UTF-8: {e}")))?,
                ),
                Some(_) => {
                    return Err(Error::Runtime("AQ dequeue returned non-RAW payload".into()))
                }
                None => None,
            };
            Ok(text)
        }
        PostAuthScenario::DplLoadReadback => {
            use oracledb_protocol::dpl::DirectPathColumnValue;
            use oracledb_protocol::thin::{ExecuteOptions, QueryValue};
            // Direct-path load one row (prepare + one load-stream + finish). The
            // server-assigned direct-path cursor id from the PREPARE response is
            // echoed in the load-stream and finish requests. `batch_size` is a
            // large upper bound so the single row streams in one message.
            let columns = [DPL_COLUMN.to_string()];
            let rows = [vec![DirectPathColumnValue::Number(
                DPL_EXPECTED_VALUE.to_string(),
            )]];
            conn.direct_path_load(cx, DPL_SCHEMA, DPL_TABLE_NAME, &columns, &rows, 1000)
                .await?;
            // Read the loaded value back — a real decoded semantic assertion that
            // the direct-path load committed and is queryable.
            let exec = conn
                .execute_raw(
                    cx,
                    DPL_READBACK_SQL,
                    2,
                    &[],
                    ExecuteOptions::default(),
                    None,
                )
                .await?;
            Ok(exec
                .cell(0, 0)
                .and_then(QueryValue::as_number_text)
                .map(|c| c.to_string()))
        }
    }
}

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

fn postauth_cassette_path(lane_id: &str, scenario: PostAuthScenario) -> PathBuf {
    fixtures_dir().join(format!("{lane_id}-{}.tns-cassette", scenario.suffix()))
}

fn postauth_manifest_path(lane_id: &str, scenario: PostAuthScenario) -> PathBuf {
    fixtures_dir().join(format!(
        "{lane_id}-{}.tns-cassette.manifest",
        scenario.suffix()
    ))
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
fn capture_postauth(
    connect: &str,
    user: &str,
    password: &str,
    scenario: PostAuthScenario,
) -> Result<PostAuthCapture> {
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

            let value = drive_scenario(&mut conn, &cx, scenario).await?;

            let full = scope.to_cassette_bytes();
            // Best-effort logoff so the capture leaves the session clean; its frames
            // are past the scenario slice and are dropped below.
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
    // Keep the scenario's post-auth frames — its `client_writes()` request(s) and
    // their responses — and drop everything from the trailing close/logoff write
    // onward. `ExecuteSelect` keeps 1 write (identical to the original slicer);
    // `LobBlob` keeps 3 (create-temp, write, read).
    let post = &all_frames[prefix_frames..];
    let sliced = reencode_frames(slice_scenario_frames(post, scenario.client_writes()));

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

/// The concatenated client-to-server byte stream of a cassette (all `C->S`
/// frame payloads in order). Used by the decoded-assert masked write compare.
fn client_write_stream(cassette_bytes: &[u8]) -> Result<Vec<u8>> {
    let frames = cassette::decode_all(cassette_bytes)
        .map_err(|e| Error::Runtime(format!("cassette decode: {e}")))?;
    let mut out = Vec::new();
    for frame in frames {
        if frame.direction == Direction::ClientToServer {
            out.extend_from_slice(&frame.bytes);
        }
    }
    Ok(out)
}

/// Assert the driver's re-issued client-write stream (`produced`) equals the
/// recorded client-write stream (`recorded`) after masking every server-assigned
/// id byte-run. The two concatenations must be the same length (a server id
/// substitution never changes length), and every maximal run of differing bytes
/// must have a length in `id_lengths`. Any other divergence — a length mismatch,
/// or a differing run of an unexpected length — is a real request regression, not
/// id noise, and fails. This is what makes [`ReplayMode::DecodedAssert`] stricter
/// than a blind `Ignore`: it tolerates ONLY the volatile id fields.
fn assert_masked_writes_match(
    produced: &[u8],
    recorded: &[u8],
    id_lengths: &[usize],
) -> Result<()> {
    if produced.len() != recorded.len() {
        return Err(Error::Runtime(format!(
            "decoded-assert: re-issued client writes are {} bytes, recording is {} — a \
             server-assigned id substitution never changes length, so this is a real \
             request regression",
            produced.len(),
            recorded.len()
        )));
    }
    let mut i = 0usize;
    while i < produced.len() {
        if produced[i] == recorded[i] {
            i += 1;
            continue;
        }
        let run_start = i;
        while i < produced.len() && produced[i] != recorded[i] {
            i += 1;
        }
        let run_len = i - run_start;
        if !id_lengths.contains(&run_len) {
            return Err(Error::Runtime(format!(
                "decoded-assert: re-issued client writes differ from the recording in a \
                 {run_len}-byte run at offset {run_start}, whose length is not a known \
                 server-assigned id length ({id_lengths:?}) — a real request regression, not \
                 id noise"
            )));
        }
    }
    Ok(())
}

/// Replay a sliced post-auth cassette against a loopback seeded from `caps` /
/// `ttc_seq_num`, returning the decoded scalar.
///
/// * [`ReplayMode::Check`] asserts every re-issued client byte matches the
///   recording (`ReplayWriteMode::Check`) and the audit asserts full consumption.
/// * [`ReplayMode::DecodedAssert`] relaxes the byte check to
///   `ReplayWriteMode::Ignore`, tees the driver's re-issued writes, and asserts
///   they equal the recording after masking the server-assigned id byte-runs (see
///   [`assert_masked_writes_match`]). Server responses are still consumed exactly.
#[allow(clippy::too_many_arguments)]
fn replay_postauth(
    sliced: &[u8],
    capabilities: oracledb_protocol::thin::ClientCapabilities,
    ttc_seq_num: u8,
    supports_end_of_response: bool,
    supports_oob: bool,
    sdu: usize,
    scenario: PostAuthScenario,
    mode: &ReplayMode,
) -> Result<Option<String>> {
    let write_mode = match mode {
        ReplayMode::Check => ReplayWriteMode::Check,
        ReplayMode::DecodedAssert { .. } => ReplayWriteMode::Ignore,
    };

    // For DecodedAssert, tee the driver's replay writes so we can compare them
    // (masked) against the recording. The scope must be installed before the
    // halves are wrapped; it records on whichever thread `block_on` runs (the
    // recorder is cloned into the halves, so it is thread-independent).
    let capture = matches!(mode, ReplayMode::DecodedAssert { .. }).then(transport::capture_scope);

    let (read, write, audit) = transport::replay_split_with_audit(sliced, write_mode)
        .map_err(|e| Error::Runtime(format!("replay split: {e}")))?;
    let (read, write) = if capture.is_some() {
        transport::wrap_if_capturing((read, write))
    } else {
        (read, write)
    };
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
        drive_scenario(&mut conn, &cx, scenario).await
    })?;

    if let ReplayMode::DecodedAssert { id_lengths } = mode {
        let scope = capture.expect("DecodedAssert installs a capture scope");
        let produced = client_write_stream(&scope.to_cassette_bytes())?;
        let recorded = client_write_stream(sliced)?;
        assert_masked_writes_match(&produced, &recorded, id_lengths)?;
    }

    audit
        .assert_finished()
        .map_err(|e| Error::Runtime(format!("post-auth replay audit: {e}")))?;
    Ok(value)
}

fn build_postauth_manifest(
    lane_id: &str,
    cap: &PostAuthCapture,
    scenario: PostAuthScenario,
) -> Result<String> {
    let write_hashes = write_frame_hashes(&cap.sliced)?;
    Ok(format!(
        concat!(
            "schema_version = {}\n",
            "format_version = {}\n",
            "commit = \"{}\"\n",
            "profile = \"post-auth-query\"\n",
            "lane = \"{}\"\n",
            "service = \"{}\"\n",
            "scenario = \"{}\"\n",
            "sql = \"{}\"\n",
            "ttc_field_version = {}\n",
            "charset_id = {}\n",
            "max_string_size = {}\n",
            "ttc_seq_num = {}\n",
            "supports_end_of_response = {}\n",
            "supports_oob = {}\n",
            "sdu = {}\n",
            "expected_value = \"{}\"\n",
            "replay_mode = \"{}\"\n",
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
        scenario.tag(),
        scenario.sql(),
        cap.capabilities.ttc_field_version,
        cap.capabilities.charset_id,
        cap.capabilities.max_string_size,
        cap.ttc_seq_num,
        cap.supports_end_of_response,
        cap.supports_oob,
        cap.sdu,
        cap.value.as_deref().unwrap_or(""),
        match scenario.replay_mode() {
            ReplayMode::Check => "check",
            ReplayMode::DecodedAssert { .. } => "decoded_assert",
        },
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
    record_postauth_scenario(PostAuthScenario::ExecuteSelect);
}

/// Record every lane's LOB post-auth cassette (bead a4-nnnz) against the live
/// fleet. Same driver as the query recorder; run explicitly:
///
/// ```text
/// cargo test -p oracledb --features cassette \
///   record_postauth_lob_cassettes -- --ignored --nocapture
/// ```
#[test]
#[ignore = "records the live LOB post-auth cassettes; needs the Docker lanes"]
fn record_postauth_lob_cassettes() {
    record_postauth_scenario(PostAuthScenario::LobBlob);
}

/// Record the AQ post-auth cassette (bead iec3.1.32) against the live free23
/// lane. Needs the queue provisioned by `scripts/bootstrap_live_schema.sh`
/// (`DBMS_AQADM` create-queue-table / create-queue / start-queue) and the
/// `pythontest` user's `aq_administrator_role`. Run explicitly:
///
/// ```text
/// cargo test -p oracledb --features cassette \
///   record_postauth_aq_cassettes -- --ignored --nocapture
/// ```
///
/// The pre-commit re-verify runs under [`ReplayMode::DecodedAssert`] (the
/// dequeue request embeds a server-assigned message id), so a non-deterministic
/// non-id divergence still fails here rather than committing a bad cassette.
#[test]
#[ignore = "records the live AQ post-auth cassette; needs free23 + a provisioned queue"]
fn record_postauth_aq_cassettes() {
    record_postauth_scenario(PostAuthScenario::AqRawRoundTrip);
}

/// Record the DPL (direct-path load) post-auth cassette (bead iec3.1.32) against
/// the live free23 lane. Needs the target table provisioned by
/// `scripts/bootstrap_live_schema.sh`. Run explicitly:
///
/// ```text
/// cargo test -p oracledb --features cassette \
///   record_postauth_dpl_cassettes -- --ignored --nocapture
/// ```
#[test]
#[ignore = "records the live DPL post-auth cassette; needs free23 + a provisioned table"]
fn record_postauth_dpl_cassettes() {
    record_postauth_scenario(PostAuthScenario::DplLoadReadback);
}

/// Capture, pre-verify offline, and commit every lane's cassette for `scenario`.
/// Shared by the query and LOB recorders. Each capture is replayed against a
/// seeded loopback before its fixture is written, so a non-deterministic capture
/// fails here rather than committing a cassette that cannot replay.
fn record_postauth_scenario(scenario: PostAuthScenario) {
    let out_dir = std::env::var("ORACLEDB_CASSETTE_RECORD")
        .map(PathBuf::from)
        .unwrap_or_else(|_| fixtures_dir());
    fs::create_dir_all(&out_dir).expect("create fixtures dir");

    let mut failures = Vec::new();
    for lane in postauth_lanes() {
        // A scenario that does not apply to this lane (AQ is free23-only) is
        // skipped rather than attempted against a lane with no queue.
        if !scenario.applies_to_lane(lane.id) {
            continue;
        }
        let up = lane.id.to_uppercase();
        let connect = std::env::var(format!("ORACLEDB_CASSETTE_{up}"))
            .unwrap_or_else(|_| lane.default_connect.to_string());
        let user = std::env::var(format!("ORACLEDB_CASSETTE_{up}_USER"))
            .unwrap_or_else(|_| lane.default_user.to_string());
        let password = std::env::var(format!("ORACLEDB_CASSETTE_{up}_PASSWORD"))
            .unwrap_or_else(|_| lane.default_password.to_string());

        let cap = match capture_postauth(&connect, &user, &password, scenario) {
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
            scenario,
            &scenario.replay_mode(),
        ) {
            Ok(v) if v.as_deref() == Some(scenario.expected_value()) => {}
            Ok(v) => {
                failures.push(format!(
                    "{}: replay value {v:?} != {:?}",
                    lane.id,
                    scenario.expected_value()
                ));
                continue;
            }
            Err(err) => {
                failures.push(format!("{}: pre-commit replay {err}", lane.id));
                continue;
            }
        }
        let manifest = match build_postauth_manifest(lane.id, &cap, scenario) {
            Ok(m) => m,
            Err(err) => {
                failures.push(format!("{}: manifest {err}", lane.id));
                continue;
            }
        };
        let cass = out_dir.join(format!("{}-{}.tns-cassette", lane.id, scenario.suffix()));
        let man = out_dir.join(format!(
            "{}-{}.tns-cassette.manifest",
            lane.id,
            scenario.suffix()
        ));
        if let Err(err) = fs::write(&cass, &cap.sliced) {
            failures.push(format!("{}: write cassette {err}", lane.id));
            continue;
        }
        if let Err(err) = fs::write(&man, manifest) {
            failures.push(format!("{}: write manifest {err}", lane.id));
            continue;
        }
        eprintln!(
            "recorded {} {} ({} bytes) -> {}",
            lane.id,
            scenario.suffix(),
            cap.sliced.len(),
            cass.display()
        );
    }
    assert!(
        failures.is_empty(),
        "{} capture failures: {failures:?}",
        scenario.suffix()
    );
}

/// A committed post-auth cassette plus the loopback seed parsed from its manifest.
struct PostAuthFixture {
    cassette_bytes: Vec<u8>,
    caps: oracledb_protocol::thin::ClientCapabilities,
    ttc_seq_num: u8,
    supports_end_of_response: bool,
    supports_oob: bool,
    sdu: usize,
    expected_value: String,
}

/// Read a committed post-auth cassette + manifest, verify integrity and
/// sanitization, and parse the loopback seed. Shared by the offline replay gate
/// and the decoded-assert proof.
fn read_postauth_fixture(lane_id: &str, scenario: PostAuthScenario) -> Result<PostAuthFixture> {
    let cassette_bytes = fs::read(postauth_cassette_path(lane_id, scenario))
        .map_err(|e| Error::Runtime(e.to_string()))?;
    let manifest_text = fs::read_to_string(postauth_manifest_path(lane_id, scenario))
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

    Ok(PostAuthFixture {
        cassette_bytes,
        caps: oracledb_protocol::thin::ClientCapabilities {
            ttc_field_version,
            max_string_size,
            charset_id,
        },
        ttc_seq_num,
        supports_end_of_response: eor,
        supports_oob: oob,
        sdu,
        expected_value,
    })
}

/// Replay one committed post-auth cassette offline and assert the seeded
/// loopback re-derives the recorded value under the scenario's replay mode.
fn replay_postauth_lane_offline(lane_id: &str, scenario: PostAuthScenario) -> Result<()> {
    let fx = read_postauth_fixture(lane_id, scenario)?;
    let value = replay_postauth(
        &fx.cassette_bytes,
        fx.caps,
        fx.ttc_seq_num,
        fx.supports_end_of_response,
        fx.supports_oob,
        fx.sdu,
        scenario,
        &scenario.replay_mode(),
    )?;
    if value.as_deref() != Some(fx.expected_value.as_str()) {
        return Err(Error::Runtime(format!(
            "{lane_id}: replay value {value:?} != {:?}",
            fx.expected_value
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
    replay_postauth_scenario_offline(PostAuthScenario::ExecuteSelect);
}

/// Offline, no-database replay of every committed LOB post-auth cassette (bead
/// a4-nnnz). Same gate as the query replay: `ReplayWriteMode::Check` proves the
/// create/write/read request bytes are byte-reproducible, and the decoded BLOB
/// text is asserted. Lanes without a committed cassette are skipped.
#[test]
fn replay_postauth_lob_cassettes_offline() {
    replay_postauth_scenario_offline(PostAuthScenario::LobBlob);
}

/// Offline, no-database replay of the committed AQ post-auth cassette (bead
/// iec3.1.32). Unlike the byte-exact scenarios this runs under
/// [`ReplayMode::DecodedAssert`]: the dequeue request embeds the server-assigned
/// message id, so the gate asserts the decoded RAW payload AND that the re-issued
/// requests match the recording after masking the 16-byte id runs. Self-skips
/// when the cassette is not committed (the capture is operator-run against a live
/// queue).
#[test]
fn replay_postauth_aq_cassettes_offline() {
    replay_postauth_scenario_offline(PostAuthScenario::AqRawRoundTrip);
}

/// Offline, no-database replay of the committed DPL post-auth cassette (bead
/// iec3.1.32) under [`ReplayMode::DecodedAssert`]. The gate asserts the decoded
/// read-back value AND that the re-issued prepare/load-stream/finish/select
/// requests match the recording after masking the direct-path cursor-id runs.
/// Self-skips when the cassette is not committed.
#[test]
fn replay_postauth_dpl_cassettes_offline() {
    replay_postauth_scenario_offline(PostAuthScenario::DplLoadReadback);
}

/// Replay every committed cassette for `scenario` offline, aggregating failures.
fn replay_postauth_scenario_offline(scenario: PostAuthScenario) {
    let mut failures = Vec::new();
    for lane in postauth_lanes() {
        if !scenario.applies_to_lane(lane.id) {
            continue;
        }
        if !postauth_cassette_path(lane.id, scenario).exists() {
            eprintln!(
                "skip {}: no committed {} cassette",
                lane.id,
                scenario.suffix()
            );
            continue;
        }
        if let Err(err) = replay_postauth_lane_offline(lane.id, scenario) {
            failures.push(err.to_string());
        }
    }
    assert!(
        failures.is_empty(),
        "{} replay failures: {failures:?}",
        scenario.suffix()
    );
}

// ---- decoded-assert proof (bead iec3.1.32, offline, no database) ----------
//
// The `DecodedAssert` model exists so an AQ / DPL cassette — whose request bytes
// embed a server-assigned id the server picks fresh each run — can still be
// replayed even though two independent captures differ in those id bytes. The
// test below proves the model with NO database, entirely offline, on a committed
// cassette whose request echoes a server-assigned id: it mutates ONLY those id
// bytes (simulating a second capture run) and shows the mutated cassette replays
// green under `DecodedAssert` yet is rejected under byte-exact `Check`.

/// The fixed length of the server-assigned id the proof mutates. 16 bytes is the
/// AQ message-id / transaction-id length, and a 16-byte sub-window of the longer
/// LOB locator serves identically when AQ is not captured.
const ECHOED_ID_LEN: usize = TNS_AQ_MESSAGE_ID_LENGTH;

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    needle.len() <= haystack.len() && haystack.windows(needle.len()).any(|w| w == needle)
}

/// Find a [`ECHOED_ID_LEN`]-byte window inside a client-to-server frame that also
/// appears in an EARLIER server-to-client frame — i.e. a server-assigned id the
/// server returned and the client echoed back in a later request. Returns the
/// `(frame_index, offset)` of the first such window, skipping constant (all-equal
/// byte) windows like zero padding. This locates the AQ message id in the
/// dequeue-by-msgid request (or a window of the LOB locator in write/read).
fn find_echoed_id_region(frames: &[cassette::Frame]) -> Option<(usize, usize)> {
    for (i, frame) in frames.iter().enumerate() {
        if frame.direction != Direction::ClientToServer || frame.bytes.len() < ECHOED_ID_LEN {
            continue;
        }
        let earlier_server: Vec<&[u8]> = frames[..i]
            .iter()
            .filter(|f| f.direction == Direction::ServerToClient)
            .map(|f| f.bytes.as_slice())
            .collect();
        for offset in 0..=(frame.bytes.len() - ECHOED_ID_LEN) {
            let window = &frame.bytes[offset..offset + ECHOED_ID_LEN];
            if window.iter().all(|&b| b == window[0]) {
                continue; // constant run (padding) — not a server id
            }
            if earlier_server.iter().any(|s| contains_subslice(s, window)) {
                return Some((i, offset));
            }
        }
    }
    None
}

/// Build a copy of `cassette_bytes` with a single server-assigned id run flipped
/// (XOR 0xFF, so every byte differs) inside ONE client-to-server frame, leaving
/// all server responses intact. Returns the mutated cassette and the mutated
/// frame index. Errors if no echoed id is present.
fn mutate_echoed_id(cassette_bytes: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut frames = cassette::decode_all(cassette_bytes)
        .map_err(|e| Error::Runtime(format!("cassette decode: {e}")))?;
    let (frame_index, offset) = find_echoed_id_region(&frames).ok_or_else(|| {
        Error::Runtime("cassette has no server-assigned id echoed in a request frame".into())
    })?;
    for byte in &mut frames[frame_index].bytes[offset..offset + ECHOED_ID_LEN] {
        *byte ^= 0xFF;
    }
    Ok((reencode_frames(&frames), frame_index))
}

/// The committed cassette used to prove the decoded-assert model: prefer the AQ
/// cassette (its dequeue request literally echoes a server-assigned message id);
/// otherwise fall back to a LOB cassette (whose write/read requests echo the
/// server-assigned temp-lob locator — the same class of value). One of these is
/// always committed, so the proof exercises real driver code rather than skipping.
fn id_echo_proof_lane() -> Option<(PostAuthScenario, &'static str)> {
    for lane in postauth_lanes() {
        let aq = PostAuthScenario::AqRawRoundTrip;
        if aq.applies_to_lane(lane.id) && postauth_cassette_path(lane.id, aq).exists() {
            return Some((aq, lane.id));
        }
    }
    for lane in postauth_lanes() {
        let lob = PostAuthScenario::LobBlob;
        if postauth_cassette_path(lane.id, lob).exists() {
            return Some((lob, lane.id));
        }
    }
    None
}

/// Offline proof of [`ReplayMode::DecodedAssert`] (bead iec3.1.32). With no
/// database: take a committed cassette whose request echoes a server-assigned id,
/// prove the pristine cassette replays byte-exact under `Check`, then mutate ONLY
/// the id bytes (a second capture run differs there and nowhere else) and prove
/// the mutated cassette is REJECTED under `Check` but replays GREEN under
/// `DecodedAssert`, with the decoded payload intact.
#[test]
fn decoded_assert_survives_server_id_divergence_that_check_rejects() {
    let Some((scenario, lane_id)) = id_echo_proof_lane() else {
        // Neither AQ nor LOB cassette committed — nothing to prove against. The
        // LOB set is normally committed, so this only trips on a bare checkout.
        eprintln!("skip: no committed id-echo cassette (AQ or LOB) available");
        return;
    };
    let fx = read_postauth_fixture(lane_id, scenario).expect("id-echo fixture loads");

    let replay = |bytes: &[u8], mode: &ReplayMode| {
        replay_postauth(
            bytes,
            fx.caps,
            fx.ttc_seq_num,
            fx.supports_end_of_response,
            fx.supports_oob,
            fx.sdu,
            scenario,
            mode,
        )
    };

    // Baseline: the pristine committed cassette replays byte-exact under Check —
    // the driver reproduces the recorded request bytes (the server id is echoed
    // from the recorded response), so byte-exact holds run-over-run in-process.
    let pristine = replay(&fx.cassette_bytes, &ReplayMode::Check)
        .expect("pristine cassette replays byte-exact under Check");
    assert_eq!(
        pristine.as_deref(),
        Some(fx.expected_value.as_str()),
        "{lane_id}/{}: pristine decoded value",
        scenario.suffix()
    );

    // Simulate a SECOND capture run: mutate ONLY the server-assigned id a request
    // echoes (responses untouched). The driver, reading the untouched response,
    // re-issues the ORIGINAL id, so its request now diverges from the recording
    // in exactly those id bytes — "two runs differ ONLY in the server-assigned ids".
    let (mutated, _frame) =
        mutate_echoed_id(&fx.cassette_bytes).expect("cassette echoes a server id to mutate");
    assert_ne!(
        mutated, fx.cassette_bytes,
        "mutation must actually change the cassette"
    );

    // (1) Byte-exact Check REJECTS the divergent id run.
    let checked = replay(&mutated, &ReplayMode::Check);
    assert!(
        checked.is_err(),
        "{lane_id}/{}: byte-exact Check must reject a request whose server-id bytes diverge \
         from the recording, but it passed: {checked:?}",
        scenario.suffix()
    );

    // (2) DecodedAssert ACCEPTS it: the only difference is one 16-byte id run, so
    // masking id-length runs leaves the requests equal and the decoded payload is
    // intact. (If the mutation had touched a non-id byte, the masked compare would
    // still fail — proving DecodedAssert is stricter than a blind Ignore.)
    let decoded = replay(
        &mutated,
        &ReplayMode::DecodedAssert {
            id_lengths: vec![ECHOED_ID_LEN],
        },
    )
    .expect("DecodedAssert masks the server-id run and replays green");
    assert_eq!(
        decoded.as_deref(),
        Some(fx.expected_value.as_str()),
        "{lane_id}/{}: decoded-assert decoded value",
        scenario.suffix()
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
        // The LOB post-auth set (bead a4-nnnz) shares the same lanes.
        expected.insert(format!("{}-lob.tns-cassette", lane.id));
        // The AQ + DPL post-auth sets (bead iec3.1.32) are captured only where the
        // scenario applies (free23 has the provisioned queue + AQ role + table).
        if PostAuthScenario::AqRawRoundTrip.applies_to_lane(lane.id) {
            expected.insert(format!("{}-aq.tns-cassette", lane.id));
        }
        if PostAuthScenario::DplLoadReadback.applies_to_lane(lane.id) {
            expected.insert(format!("{}-dpl.tns-cassette", lane.id));
        }
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
