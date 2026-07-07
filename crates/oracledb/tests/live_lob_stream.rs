//! Live integration tests for lazy LOB streaming (bead a4-bbx / iec3.1.22).
//!
//! These are `#[ignore]`d by default; run with a lane container up and the
//! `PYO_TEST_*` env vars set (see scripts/version_matrix.sh). They exercise the
//! lazy streaming reader/writer end to end against a real server:
//!
//! ```text
//! cargo test -p oracledb --test live_lob_stream -- --ignored --nocapture
//! ```
//!
//! * a large BLOB round-trip: write ~256 KiB through [`LobWriter`] in 8 KiB
//!   chunks, then drain it back through [`LobReader`] in 4 KiB chunks and assert
//!   byte-identical content across the many round trips;
//! * a CLOB carrying astral (surrogate-pair) codepoints, streamed back through
//!   [`ClobReader`] in tiny character chunks, asserting the text decodes
//!   identically. (The adversarial mid-codepoint / mid-surrogate byte splits are
//!   proven exhaustively offline in `oracledb_protocol`'s `LobTextDecoder`
//!   tests; here we prove the streaming path is wired to it correctly.)

use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::Cx;
use oracledb::protocol::thin::{
    encode_lob_text, LobValue, CS_FORM_IMPLICIT, ORA_TYPE_NUM_BLOB, ORA_TYPE_NUM_CLOB,
};
use oracledb::{ClobReader, ConnectOptions, Connection, LobReader, LobWriter};
use oracledb_protocol::ClientIdentity;

mod common;

fn connect_options() -> ConnectOptions {
    let identity = ClientIdentity::new(
        "rust-oracledb",
        "rusthost",
        "rustuser",
        "rustterm",
        "rust-oracledb thn : 0.0.0",
    )
    .expect("identity");
    ConnectOptions::new(
        common::live_conn_string_or(common::FREE23_CONNECT_STRING),
        common::live_user_or(common::FREE23_USER),
        common::live_password_or(common::FREE23_PASSWORD),
        identity,
    )
}

/// Run an async test body on a fresh current-thread runtime + connection.
fn with_conn<F>(body: F)
where
    F: for<'a> FnOnce(
        &'a mut Connection,
        &'a Cx,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>>,
{
    let reactor = reactor::create_reactor().expect("reactor");
    let runtime = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let cx = Cx::current().expect("cx");
        let mut conn = Connection::connect(&cx, connect_options())
            .await
            .expect("connect");
        body(&mut conn, &cx).await;
        conn.close(&cx).await.expect("close");
    });
}

#[test]
#[ignore = "requires live Oracle container (any lane)"]
fn large_blob_stream_round_trip() {
    with_conn(|conn, cx| {
        Box::pin(async move {
            // 256 KiB of deterministic pseudo-random bytes (spans many chunks).
            let payload: Vec<u8> = (0u32..256 * 1024)
                .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
                .collect();

            let temp = conn
                .create_temp_lob(cx, ORA_TYPE_NUM_BLOB, CS_FORM_IMPLICIT)
                .await
                .expect("create temp BLOB");

            let mut writer = LobWriter::new(temp.locator);
            for chunk in payload.chunks(8 * 1024) {
                writer
                    .write_chunk(conn, cx, chunk)
                    .await
                    .expect("write chunk");
            }
            let locator = writer.into_locator();

            let mut reader = LobReader::from_parts(locator.clone(), payload.len() as u64, 4 * 1024);
            let back = reader.read_to_end(conn, cx).await.expect("read BLOB");

            assert_eq!(back.len(), payload.len(), "streamed BLOB length must match");
            assert_eq!(
                back, payload,
                "streamed BLOB content must be byte-identical"
            );

            conn.free_temp_lobs(cx, &[locator]).await.ok();
        })
    });
}

#[test]
#[ignore = "requires live Oracle container (any lane)"]
fn clob_astral_codepoints_stream_round_trip() {
    with_conn(|conn, cx| {
        Box::pin(async move {
            // Astral codepoints (surrogate pairs), BMP CJK, Latin-1 and ASCII.
            let text = "emoji 😀 party 🎉🎊 漢字 café ✓ end \u{10FFFF}";

            let temp = conn
                .create_temp_lob(cx, ORA_TYPE_NUM_CLOB, CS_FORM_IMPLICIT)
                .await
                .expect("create temp CLOB");
            let mut locator = temp.locator;

            // Encode in the temp LOB's character-set form and write in one shot
            // (single write keeps the character offset bookkeeping trivial).
            let encoded = encode_lob_text(text, CS_FORM_IMPLICIT, Some(&locator));
            let written = conn
                .write_lob(cx, &locator, 1, &encoded)
                .await
                .expect("write CLOB");
            if !written.locator.is_empty() {
                locator = written.locator;
            }

            // Stream it back in tiny character chunks through the decoding reader.
            // Oracle CLOB length is measured in UTF-16 code units (an astral
            // codepoint counts as 2), matching AL16UTF16 storage — not scalar
            // count. A tiny chunk therefore splits surrogate pairs across reads.
            let lob = LobValue {
                ora_type_num: ORA_TYPE_NUM_CLOB,
                csfrm: CS_FORM_IMPLICIT,
                locator: locator.clone(),
                size: text.encode_utf16().count() as u64,
                chunk_size: 0,
            };
            let got = ClobReader::new(&lob, 3)
                .read_to_string(conn, cx)
                .await
                .expect("stream-read CLOB");

            assert_eq!(got, text, "streamed CLOB text must decode identically");

            conn.free_temp_lobs(cx, &[locator]).await.ok();
        })
    });
}
