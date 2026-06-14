//! Transport record/replay seam: live capture + offline replay.
//!
//! Two halves, both gated behind the `cassette` feature:
//!
//! * [`record_select_7_plus_5_session`] is a LIVE test (`#[ignore]`, needs the
//!   lane container). It installs a [`transport::capture_scope`], runs a real
//!   `Connection::connect` + `select 7+5 from dual` + fetch + close, and writes
//!   the captured `.tns-cassette` to the test fixture directory. This proves the
//!   seam tees a real Oracle session through to a cassette file.
//!
//! * [`replay_select_7_plus_5_offline`] is an OFFLINE test (NO database). It
//!   loads the captured fixture, builds a socket-free [`transport::replay_split`]
//!   transport, drives the REAL TNS packet framing over the [`ReplayRead`] half,
//!   and decodes the execute response with the REAL `parse_query_response`. It
//!   asserts the decoded value is exactly 12 — reproducing the recorded session
//!   with no socket, no clock, and no DB.
//!
//! python-oracledb has no equivalent: there is no way to capture a raw thin-mode
//! wire session and replay it offline to drive the decoder.

#![cfg(feature = "cassette")]

use std::path::PathBuf;

use asupersync::io::{AsyncRead, AsyncReadExt};
use oracledb::transport::{self, OracleReadHalf, ReplayWriteMode};
use oracledb_protocol::thin::{
    parse_query_response, ClientCapabilities, QueryValue, TNS_DATA_FLAGS_END_OF_RESPONSE,
    TNS_DATA_FLAGS_EOF,
};

/// Where the captured fixture lives. Checked in so the offline replay test runs
/// in CI with no container.
fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("cassettes")
        .join("select_7_plus_5.tns-cassette")
}

const TNS_PACKET_TYPE_DATA: u8 = 6;

/// TNS packet length-field width. The driver reads the connect/accept handshake
/// packets with the legacy 16-bit length (bytes 0..2) and everything after with
/// the 32-bit length (bytes 0..4); replay must mirror that to stay byte-aligned.
#[derive(Clone, Copy)]
enum LenWidth {
    Legacy16,
    Large32,
}

/// Read one TNS packet (8-byte header + body) from an [`AsyncRead`], mirroring
/// the driver's private `read_packet` framing. `width` selects the length field
/// (the type byte is always at offset 4). Returns `(packet_type, body)` or
/// `None` at end of stream.
async fn read_tns_packet<R: AsyncRead + Unpin>(
    read: &mut R,
    width: LenWidth,
) -> Option<(u8, Vec<u8>)> {
    let mut header = [0u8; 8];
    read.read_exact(&mut header).await.ok()?;
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
    read.read_exact(&mut body).await.ok()?;
    Some((header[4], body))
}

/// Replay the cassette's S->C stream and reassemble each DATA response (the
/// driver concatenates the post-flags payload of each DATA packet until the
/// END_OF_RESPONSE / EOF data flag). Returns one reassembled payload per
/// response boundary — exactly the byte stream `parse_query_response` consumes.
async fn reassemble_responses(read: &mut OracleReadHalf) -> Vec<Vec<u8>> {
    let mut responses = Vec::new();
    let mut current: Vec<u8> = Vec::new();
    // The first server packet is the ACCEPT handshake (legacy 16-bit length);
    // every packet after it is a 32-bit-length DATA/MARKER packet.
    let mut width = LenWidth::Legacy16;
    while let Some((packet_type, body)) = read_tns_packet(read, width).await {
        width = LenWidth::Large32;
        if packet_type != TNS_PACKET_TYPE_DATA {
            continue; // skip CONNECT/ACCEPT/etc.; only DATA carries TTC.
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

/// Scan decoded responses for a row whose single value is the integer 12.
fn decoded_value_is_twelve(responses: &[Vec<u8>]) -> bool {
    for payload in responses {
        let Ok(result) = parse_query_response(payload, ClientCapabilities::default()) else {
            continue;
        };
        for row in &result.rows {
            for value in row {
                if let Some(QueryValue::Number(num)) = value {
                    if num.to_canonical_string() == "12" && num.is_integer() {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// OFFLINE replay: NO database. Loads the captured fixture, builds a socket-free
/// replay transport, drives the real TNS framing + real decoder, asserts 12.
#[test]
fn replay_select_7_plus_5_offline() {
    let path = fixture_path();
    let bytes = std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "missing cassette fixture {} ({e}); run the ignored \
             record_select_7_plus_5_session against the lane container to capture it",
            path.display()
        )
    });

    // Sanity: it is a real .tns-cassette with both directions captured.
    let frames = oracledb_protocol::net::cassette::decode_all(&bytes)
        .expect("fixture must be a valid .tns-cassette");
    assert!(
        frames
            .iter()
            .any(|f| f.direction == oracledb_protocol::net::cassette::Direction::ClientToServer),
        "cassette must contain captured C->S writes"
    );
    assert!(
        frames
            .iter()
            .any(|f| f.direction == oracledb_protocol::net::cassette::Direction::ServerToClient),
        "cassette must contain captured S->C reads"
    );

    // Build the socket-free replay transport and drive the real read path.
    let (mut read, _write) = transport::replay_split(&bytes, ReplayWriteMode::Ignore)
        .expect("captured fixture should replay");

    // A tiny single-thread runtime just to poll the in-memory ReplayRead; it
    // never touches the network (the replay half has no socket).
    let reactor =
        asupersync::runtime::reactor::create_reactor().expect("reactor builds for in-memory poll");
    let runtime = asupersync::runtime::RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("current-thread runtime builds");

    let responses = runtime.block_on(async { reassemble_responses(&mut read).await });
    assert!(
        decoded_value_is_twelve(&responses),
        "offline replay of the captured `select 7+5 from dual` session must \
         decode the result 12 with no database; got {} reassembled responses",
        responses.len()
    );
}

/// LIVE capture: records a real session against the lane container and writes
/// the cassette fixture. Ignored by default (needs the DB); run explicitly to
/// (re)generate the fixture the offline replay test consumes.
#[test]
#[ignore = "requires the lane Oracle container; records the cassette fixture"]
fn record_select_7_plus_5_session() {
    use oracledb::{ConnectOptions, Connection};
    use oracledb_protocol::ClientIdentity;

    let reactor = asupersync::runtime::reactor::create_reactor()
        .expect("native reactor should build for live I/O");
    let runtime = asupersync::runtime::RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("current-thread runtime should build");

    let cassette_bytes = runtime.block_on(async {
        let cx = asupersync::Cx::current().expect("block_on installs an ambient Cx");
        let identity = ClientIdentity::new(
            "rust-oracledb",
            "rusthost",
            "rustuser",
            "rustterm",
            "rust-oracledb thn : 0.0.0",
        )
        .expect("identity should be valid");
        let options = ConnectOptions::new(
            std::env::var("PYO_TEST_CONNECT_STRING")
                .unwrap_or_else(|_| "localhost:1526/FREEPDB1".to_string()),
            std::env::var("PYO_TEST_MAIN_USER").unwrap_or_else(|_| "pythontest".to_string()),
            std::env::var("PYO_TEST_MAIN_PASSWORD")
                .expect("PYO_TEST_MAIN_PASSWORD must be set for the live capture test"),
            identity,
        );

        // Install the capture scope BEFORE connect so the full session — connect,
        // auth, execute, fetch, close — is teed into the recorder.
        let scope = transport::capture_scope();

        let mut conn = Connection::connect(&cx, options)
            .await
            .expect("live connect should authenticate");
        let result = conn
            .execute_query(&cx, "select 7+5 from dual", 2)
            .await
            .expect("live `select 7+5 from dual` should execute and fetch");
        // Confirm the LIVE result is 12 before we trust the recording.
        assert_eq!(result.rows.len(), 1);
        let cell = result.rows[0][0].as_ref().expect("NUMBER cell");
        assert_eq!(cell.as_number_text().as_deref(), Some("12"));
        assert!(cell.as_number().expect("number").is_integer());
        conn.close(&cx)
            .await
            .expect("live logoff should round-trip");

        let bytes = scope.to_cassette_bytes();
        drop(scope);
        bytes
    });

    assert!(
        !cassette_bytes.is_empty(),
        "capture scope should have recorded a non-empty session"
    );
    let path = fixture_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("fixture dir should be creatable");
    }
    std::fs::write(&path, &cassette_bytes).expect("cassette fixture should be writable");
    eprintln!(
        "wrote {} bytes of .tns-cassette to {}",
        cassette_bytes.len(),
        path.display()
    );
}
