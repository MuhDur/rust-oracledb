//! DIFFERENTIATOR metamorphic property tests (bead oraclemcp-release-073 D6.1b
//! / rust-oracledb iec3.4.8).
//!
//! Skill: `testing-metamorphic`. Each test expresses a driver differentiator's
//! correctness as a METAMORPHIC RELATION — a property that must hold under an
//! input transformation even though we cannot cheaply compute the exact expected
//! output — driven by property-based input generation (`proptest`). Three of the
//! four differentiator MRs live here (the fourth, j1w, is an in-crate unit test
//! beside its `pub(crate)` surface in `src/rows.rs::continuation_mr`):
//!
//!   * MR1 — x3s stream == collect (EQUIVALENCE): the zero-copy borrowed fetch
//!     (`parse_query_response_borrowed` + `BorrowedRowBatch::for_each_row_ref`,
//!     the surface behind `Connection::for_each_row_ref`) decodes a fetch frame
//!     to values byte-identical to the eager owned collect
//!     (`parse_fetch_response_with_context`). Two independent decode paths over
//!     the same wire bytes must agree row-by-row.
//!   * MR2 — 0mk VECTOR round-trip (INVERTIVE): a VECTOR carried as a
//!     `QueryValue::Vector` cell survives encode∘decode through the row path
//!     (bind image `encode_vector` → fetch image `decode_vector`) bit-identically,
//!     and its `FromSql` projection (`Vec<f32>`/`Vec<f64>`, sql_convert.rs) is
//!     preserved.
//!   * MR4 — bbx LOB round-trip (INVERTIVE): a LOB written then read back is
//!     identical. Offline core: the public CLOB/NCLOB text codec
//!     (`encode_lob_text`∘`decode_lob_text`). Live core (self-skipping without a
//!     lane): the streaming `LobWriter`→`LobReader` / `ClobReader` write∘read.
//!
//! Each MR is MUTATION-VALIDATED by a companion `*_planted_mutant_is_killed`
//! test: it feeds the SAME relation a deliberately-broken mirror of the surface
//! (a realistic bug: a dropped row / a truncated vector element / a lost LOB
//! byte) and asserts the relation then FAILS — proving the MR is non-vacuous and
//! fault-sensitive. (This repo is `#![forbid(unsafe_code)]` and the bead is
//! TEST-ONLY, so we cannot flip the shipped code in place; the planted mutant is
//! the sanctioned alternative to a `cargo-mutants` run on the module.) Every
//! proptest case logs its generated input shape + the relation checked so a
//! shrunk failure is diagnosable.

use oracledb::protocol::thin::{
    decode_lob_text, encode_lob_text, encode_number_text, parse_fetch_response_with_context,
    parse_query_response_borrowed, ClientCapabilities, ColumnMetadata, QueryValue,
};
use oracledb::protocol::vector::{decode_vector, encode_vector, Vector, VectorValues};
use oracledb::protocol::wire::TtcWriter;
use oracledb::protocol::ProtocolError;
use oracledb::FromSql;
use proptest::prelude::*;

mod common;

// Wire tags + type numbers the fetch-frame parsers walk (kept local, mirroring
// the offline synthetic-frame tests in `oracledb_protocol`).
const ORA_TYPE_NUM_VARCHAR: u8 = 1;
const ORA_TYPE_NUM_NUMBER: u8 = 2;
const ORA_TYPE_NUM_RAW: u8 = 23;
const CS_FORM_IMPLICIT: u8 = 1;
const CS_FORM_NCHAR: u8 = 2;
const TNS_MSG_TYPE_ROW_DATA: u8 = 7;
const TNS_MSG_TYPE_END_OF_RESPONSE: u8 = 29;

const CASES: u32 = 256;

fn config() -> ProptestConfig {
    ProptestConfig {
        cases: CASES,
        ..ProptestConfig::default()
    }
}

// ===========================================================================
// MR1 — x3s stream == collect (EQUIVALENCE)
// ===========================================================================
//
// Transform: decode the SAME fetch-response frame two ways — the eager owned
// collect (`parse_fetch_response_with_context`, materializes owned rows) and the
// streaming borrowed fetch (`parse_query_response_borrowed` +
// `BorrowedRowBatch::for_each_row_ref`, the zero-copy path behind
// `Connection::for_each_row_ref`). Relation: the two row grids are equal,
// row-by-row, cell-by-cell, after materializing each borrowed cell with
// `to_owned_value()`. An equivalence MR needs no external oracle: it does not
// matter what a length-0 cell *means* (NULL vs empty), only that both paths
// agree — so it catches any divergence between the borrowed and owned decoders
// (a mis-sized read, a dropped duplicate column, a NUMBER-arena off-by-one).
//
// The schema is a fixed, representative 5-column grid exercising every decode
// class the fast path cares about — NUMBER (per-row scratch arena), VARCHAR2
// (zero-copy borrow), RAW (zero-copy borrow) — with NULLs interleaved. The
// property is over the ROW VALUES, which is the axis that drives the decoders.

/// One generated row of `[NUMBER, VARCHAR2, RAW, VARCHAR2, NUMBER]`; `None`
/// means a wire NULL (length byte 0).
type GenRow = (
    Option<i64>,
    Option<String>,
    Option<Vec<u8>>,
    Option<String>,
    Option<i64>,
);

fn rows_strategy() -> impl Strategy<Value = Vec<GenRow>> {
    let row = (
        prop::option::of(any::<i64>()),
        prop::option::of(".{0,24}"),
        prop::option::of(prop::collection::vec(any::<u8>(), 0..=16)),
        prop::option::of(".{0,24}"),
        prop::option::of(any::<i64>()),
    );
    prop::collection::vec(row, 0..=12)
}

fn mr1_columns() -> Vec<ColumnMetadata> {
    vec![
        ColumnMetadata::new("N1", ORA_TYPE_NUM_NUMBER).with_buffer_size(22),
        ColumnMetadata::new("T1", ORA_TYPE_NUM_VARCHAR)
            .with_csfrm(CS_FORM_IMPLICIT)
            .with_buffer_size(4000),
        ColumnMetadata::new("R1", ORA_TYPE_NUM_RAW).with_buffer_size(2000),
        ColumnMetadata::new("T2", ORA_TYPE_NUM_VARCHAR)
            .with_csfrm(CS_FORM_IMPLICIT)
            .with_buffer_size(4000),
        ColumnMetadata::new("N2", ORA_TYPE_NUM_NUMBER).with_buffer_size(22),
    ]
}

fn write_num_cell(writer: &mut TtcWriter, cell: &Option<i64>) {
    match cell {
        Some(n) => {
            let image = encode_number_text(&n.to_string()).expect("encode NUMBER text");
            writer
                .write_bytes_with_length(&image)
                .expect("frame NUMBER cell");
        }
        None => writer.write_u8(0),
    }
}

fn write_bytes_cell(writer: &mut TtcWriter, cell: Option<&[u8]>) {
    match cell {
        Some(bytes) => writer
            .write_bytes_with_length(bytes)
            .expect("frame bytes cell"),
        None => writer.write_u8(0),
    }
}

/// Build a fetch-response frame for the fixed 5-column schema.
fn build_fetch_frame(rows: &[GenRow]) -> Vec<u8> {
    let mut writer = TtcWriter::new();
    for (n1, t1, r1, t2, n2) in rows {
        writer.write_u8(TNS_MSG_TYPE_ROW_DATA);
        write_num_cell(&mut writer, n1);
        write_bytes_cell(&mut writer, t1.as_deref().map(str::as_bytes));
        write_bytes_cell(&mut writer, r1.as_deref());
        write_bytes_cell(&mut writer, t2.as_deref().map(str::as_bytes));
        write_num_cell(&mut writer, n2);
    }
    writer.write_u8(TNS_MSG_TYPE_END_OF_RESPONSE);
    writer.into_bytes()
}

/// Collect the streaming borrowed path into owned rows via `to_owned_value()`.
fn stream_collect(payload: &[u8], columns: &[ColumnMetadata]) -> Vec<Vec<Option<QueryValue>>> {
    let borrowed =
        parse_query_response_borrowed(payload, ClientCapabilities::default(), columns, None)
            .expect("borrowed decode");
    let mut streamed: Vec<Vec<Option<QueryValue>>> = Vec::new();
    borrowed
        .batch
        .for_each_row_ref(|row| {
            streamed.push(
                row.iter()
                    .map(|cell| cell.map(|v| v.to_owned_value()))
                    .collect(),
            );
            Ok::<(), ProtocolError>(())
        })
        .expect("stream decode");
    streamed
}

proptest! {
    #![proptest_config(config())]

    /// MR1: streaming (borrowed) decode == eager (owned) collect, row-by-row.
    #[test]
    fn mr1_stream_equals_collect(rows in rows_strategy()) {
        let columns = mr1_columns();
        let payload = build_fetch_frame(&rows);

        let owned = parse_fetch_response_with_context(
            &payload,
            ClientCapabilities::default(),
            &columns,
            None,
        )
        .expect("owned collect decode");
        let streamed = stream_collect(&payload, &columns);

        println!(
            "[MR1] rows={} cols=5 frame_bytes={} -> collect={} stream={}",
            rows.len(),
            payload.len(),
            owned.rows.len(),
            streamed.len(),
        );

        prop_assert_eq!(owned.rows.len(), streamed.len(), "row count differs (stream vs collect)");
        prop_assert_eq!(owned.rows, streamed, "stream != collect (row values diverged)");
    }
}

/// MR1 mutation-validation: the equivalence relation must reject a "collect"
/// that silently drops the last row — a realistic paging/off-by-one bug in the
/// eager path. Proves MR1 is fault-sensitive, not a tautology.
#[test]
fn mr1_planted_mutant_is_killed() {
    let rows: Vec<GenRow> = vec![
        (
            Some(1),
            Some("alpha".into()),
            Some(vec![0xDE, 0xAD]),
            None,
            Some(-7),
        ),
        (
            None,
            Some("β-row".into()),
            Some(vec![]),
            Some("z".into()),
            Some(42),
        ),
        (
            Some(i64::MIN),
            None,
            Some(vec![0xFF, 0x00, 0x10]),
            Some("last".into()),
            None,
        ),
    ];
    let columns = mr1_columns();
    let payload = build_fetch_frame(&rows);
    let owned =
        parse_fetch_response_with_context(&payload, ClientCapabilities::default(), &columns, None)
            .expect("owned decode");
    let streamed = stream_collect(&payload, &columns);

    // Real surface: the relation HOLDS.
    assert_eq!(owned.rows, streamed, "real stream==collect must hold");

    // Planted mutant: a collect that drops the final row.
    let mutant: Vec<Vec<Option<QueryValue>>> = owned.rows[..owned.rows.len() - 1].to_vec();
    assert_ne!(
        mutant, streamed,
        "planted mutant (dropped last row) must break stream==collect"
    );
}

// ===========================================================================
// MR2 — 0mk VECTOR round-trip (INVERTIVE)
// ===========================================================================
//
// Transform: encode∘decode a VECTOR through the row-path images (bind uses
// `encode_vector`, fetch uses `decode_vector`) while it is carried as the
// `QueryValue::Vector` cell the row path actually produces. Relation: identity —
// the decoded vector is bit-identical to the original, AND its `FromSql`
// projection to `Vec<f32>`/`Vec<f64>` (sql_convert.rs) is preserved. Floats are
// compared on BITS so a signed-zero / element-order bug is visible (derive-`Eq`
// on f32 would treat +0.0 == -0.0 and mask it).

fn f32_vec() -> impl Strategy<Value = Vec<f32>> {
    let elem = prop::num::f32::NORMAL
        | prop::num::f32::SUBNORMAL
        | prop::num::f32::ZERO
        | prop::num::f32::NEGATIVE
        | prop::num::f32::POSITIVE;
    prop::collection::vec(elem, 0..=48)
}

fn f64_vec() -> impl Strategy<Value = Vec<f64>> {
    let elem = prop::num::f64::NORMAL
        | prop::num::f64::SUBNORMAL
        | prop::num::f64::ZERO
        | prop::num::f64::NEGATIVE
        | prop::num::f64::POSITIVE;
    prop::collection::vec(elem, 0..=48)
}

fn vector_strategy() -> impl Strategy<Value = Vector> {
    prop_oneof![
        f32_vec().prop_map(|v| Vector::Dense(VectorValues::Float32(v))),
        f64_vec().prop_map(|v| Vector::Dense(VectorValues::Float64(v))),
        prop::collection::vec(any::<i8>(), 0..=64)
            .prop_map(|v| Vector::Dense(VectorValues::Int8(v))),
        prop::collection::vec(any::<u8>(), 0..=32)
            .prop_map(|v| Vector::Dense(VectorValues::Binary(v))),
        (
            1u32..=4096,
            prop::collection::vec((0u32..4096, prop::num::f64::NORMAL), 0..=24)
        )
            .prop_map(|(num_dimensions, entries)| {
                let indices = entries.iter().map(|(i, _)| *i).collect();
                let values = entries.iter().map(|(_, v)| *v).collect();
                Vector::Sparse {
                    num_dimensions,
                    indices,
                    values: VectorValues::Float64(values),
                }
            }),
    ]
}

/// Bit-exact vector equality (signed-zero / NaN-aware for floats).
fn vectors_bit_eq(a: &Vector, b: &Vector) -> bool {
    fn values_bit_eq(a: &VectorValues, b: &VectorValues) -> bool {
        match (a, b) {
            (VectorValues::Float32(x), VectorValues::Float32(y)) => {
                x.len() == y.len() && x.iter().zip(y).all(|(p, q)| p.to_bits() == q.to_bits())
            }
            (VectorValues::Float64(x), VectorValues::Float64(y)) => {
                x.len() == y.len() && x.iter().zip(y).all(|(p, q)| p.to_bits() == q.to_bits())
            }
            (VectorValues::Int8(x), VectorValues::Int8(y)) => x == y,
            (VectorValues::Binary(x), VectorValues::Binary(y)) => x == y,
            _ => false,
        }
    }
    match (a, b) {
        (Vector::Dense(x), Vector::Dense(y)) => values_bit_eq(x, y),
        (
            Vector::Sparse {
                num_dimensions: nd1,
                indices: i1,
                values: v1,
            },
            Vector::Sparse {
                num_dimensions: nd2,
                indices: i2,
                values: v2,
            },
        ) => nd1 == nd2 && i1 == i2 && values_bit_eq(v1, v2),
        _ => false,
    }
}

proptest! {
    #![proptest_config(config())]

    /// MR2: `QueryValue::Vector` cell survives encode∘decode bit-identically and
    /// keeps its `FromSql` float projection.
    #[test]
    fn mr2_vector_row_path_round_trip(vector in vector_strategy()) {
        // Row-path images: encode as the bind side does, decode as the fetch
        // side does, and wrap back into the row cell the fetch path yields.
        let decoded = decode_vector(&encode_vector(&vector)).expect("decode vector image");
        let cell = QueryValue::Vector(Box::new(decoded.clone()));

        let (kind, dims) = match &vector {
            Vector::Dense(v) => ("dense", v.len()),
            Vector::Sparse { indices, .. } => ("sparse", indices.len()),
        };
        println!("[MR2] kind={kind} elems={dims} -> identity check");

        prop_assert!(vectors_bit_eq(&vector, &decoded), "VECTOR encode∘decode not identity");

        // Row path: the FromSql projection onto the decoded cell must reproduce
        // the original float elements (bit-exact) for the float formats.
        match &vector {
            Vector::Dense(VectorValues::Float32(orig)) => {
                let got = <Vec<f32> as FromSql>::from_sql(&cell).expect("project Vec<f32>");
                prop_assert_eq!(got.len(), orig.len());
                prop_assert!(
                    orig.iter().zip(&got).all(|(a, b)| a.to_bits() == b.to_bits()),
                    "Vec<f32> projection diverged"
                );
            }
            Vector::Dense(VectorValues::Float64(orig)) => {
                let got = <Vec<f64> as FromSql>::from_sql(&cell).expect("project Vec<f64>");
                prop_assert_eq!(got.len(), orig.len());
                prop_assert!(
                    orig.iter().zip(&got).all(|(a, b)| a.to_bits() == b.to_bits()),
                    "Vec<f64> projection diverged"
                );
            }
            _ => {}
        }
    }
}

/// MR2 mutation-validation: the identity relation must reject a decoder that
/// truncates the last element — a realistic length/off-by-one bug in the VECTOR
/// image decode.
#[test]
fn mr2_planted_mutant_is_killed() {
    let vector = Vector::Dense(VectorValues::Float32(vec![1.0, -2.0, 3.5, -0.0]));
    let decoded = decode_vector(&encode_vector(&vector)).expect("decode");

    // Real surface: identity HOLDS.
    assert!(
        vectors_bit_eq(&vector, &decoded),
        "real VECTOR identity must hold"
    );

    // Planted mutant: drop the last element of the decoded vector.
    let mutant = match decoded {
        Vector::Dense(VectorValues::Float32(mut v)) => {
            v.pop();
            Vector::Dense(VectorValues::Float32(v))
        }
        other => other,
    };
    assert!(
        !vectors_bit_eq(&vector, &mutant),
        "planted mutant (dropped last element) must break VECTOR identity"
    );
}

// ===========================================================================
// MR4 — bbx LOB round-trip (INVERTIVE)
// ===========================================================================
//
// Offline core: the public CLOB/NCLOB text codec round-trips (write = encode,
// read = decode) for both charset forms. Live core (self-skipping without a
// lane): the streaming `LobWriter`→`LobReader` (BLOB, byte-identical) and
// `ClobReader` (CLOB, surrogate-pair-aware, text-identical) write∘read.

proptest! {
    #![proptest_config(config())]

    /// MR4 (offline): `decode_lob_text(encode_lob_text(s)) == s` for the
    /// single-byte (UTF-8) and NCHAR (UTF-16) charset forms.
    #[test]
    fn mr4_lob_text_write_read_identity(
        s in ".{0,256}",
        nchar in any::<bool>(),
    ) {
        let csfrm = if nchar { CS_FORM_NCHAR } else { CS_FORM_IMPLICIT };
        let written = encode_lob_text(&s, csfrm, None);
        let read = decode_lob_text(&written, csfrm, None).expect("decode lob text");
        println!(
            "[MR4] csfrm={csfrm} chars={} written_bytes={}",
            s.chars().count(),
            written.len(),
        );
        prop_assert_eq!(read, s, "LOB text write∘read not identity");
    }
}

/// MR4 mutation-validation: the write∘read identity must reject a read that
/// loses the final byte of the stored buffer — a realistic truncated-last-chunk
/// bug. (Killing = the mutated read either errors on the partial unit or returns
/// a different string.)
#[test]
fn mr4_planted_mutant_is_killed() {
    let s = "hello 😀 world café ✓";
    let written = encode_lob_text(s, CS_FORM_IMPLICIT, None);
    let read = decode_lob_text(&written, CS_FORM_IMPLICIT, None).expect("decode");

    // Real surface: identity HOLDS.
    assert_eq!(read, s, "real LOB write∘read must hold");

    // Planted mutant: a read that drops the final byte.
    let truncated = &written[..written.len() - 1];
    let killed = match decode_lob_text(truncated, CS_FORM_IMPLICIT, None) {
        Ok(mutant) => mutant != s,
        Err(_) => true,
    };
    assert!(
        killed,
        "planted mutant (dropped last byte) must break LOB write∘read identity"
    );
}

/// MR4 (live, self-skipping): stream a BLOB and a CLOB through the real
/// `LobWriter`/`LobReader`/`ClobReader` against a lane, asserting the read-back
/// is identical to what was written. Runs only when `PYO_TEST_*` is configured
/// (try gvenzl free23 localhost:1522/FREEPDB1 or xe21 localhost:1520/XEPDB1);
/// otherwise it self-skips, matching the repo's live-suite gate.
#[test]
fn mr4_live_lob_stream_write_read_identity() {
    use asupersync::runtime::{reactor, RuntimeBuilder};
    use asupersync::Cx;
    use oracledb::protocol::thin::{LobValue, ORA_TYPE_NUM_BLOB, ORA_TYPE_NUM_CLOB};
    use oracledb::protocol::ClientIdentity;
    use oracledb::{ClobReader, ConnectOptions, Connection, LobReader, LobWriter};

    let Some(creds) = common::live_creds_opt() else {
        println!("[MR4-live] skipped: no live DB (PYO_TEST_* unset)");
        return;
    };

    let identity = ClientIdentity::new(
        "rust-oracledb",
        "rusthost",
        "rustuser",
        "rustterm",
        "rust-oracledb thn : 0.0.0",
    )
    .expect("identity");
    let options = ConnectOptions::new(creds.connect_string, creds.user, creds.password, identity);

    let reactor = reactor::create_reactor().expect("reactor");
    let runtime = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let cx = Cx::current().expect("cx");
        let mut conn = Connection::connect(&cx, options).await.expect("connect");

        // BLOB: 64 KiB deterministic pseudo-random payload spanning many chunks.
        let payload: Vec<u8> = (0u32..64 * 1024)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect();
        let temp = conn
            .create_temp_lob(&cx, ORA_TYPE_NUM_BLOB, CS_FORM_IMPLICIT)
            .await
            .expect("create temp BLOB");
        let mut writer = LobWriter::new(temp.locator);
        for chunk in payload.chunks(8 * 1024) {
            writer
                .write_chunk(&mut conn, &cx, chunk)
                .await
                .expect("write chunk");
        }
        let locator = writer.into_locator();
        let mut reader = LobReader::from_parts(locator.clone(), payload.len() as u64, 4 * 1024);
        let back = reader.read_to_end(&mut conn, &cx).await.expect("read BLOB");
        println!(
            "[MR4-live] BLOB bytes written={} read={}",
            payload.len(),
            back.len()
        );
        assert_eq!(
            back, payload,
            "streamed BLOB write∘read must be byte-identical"
        );
        conn.free_temp_lobs(&cx, &[locator]).await.ok();

        // CLOB: astral (surrogate-pair) codepoints streamed back in tiny chunks.
        let text = "emoji 😀 party 🎉🎊 漢字 café ✓ end \u{10FFFF}";
        let temp = conn
            .create_temp_lob(&cx, ORA_TYPE_NUM_CLOB, CS_FORM_IMPLICIT)
            .await
            .expect("create temp CLOB");
        let mut locator = temp.locator;
        let encoded = encode_lob_text(text, CS_FORM_IMPLICIT, Some(&locator));
        let written = conn
            .write_lob(&cx, &locator, 1, &encoded)
            .await
            .expect("write CLOB");
        if !written.locator.is_empty() {
            locator = written.locator;
        }
        let lob = LobValue {
            ora_type_num: ORA_TYPE_NUM_CLOB,
            csfrm: CS_FORM_IMPLICIT,
            locator: locator.clone(),
            size: text.encode_utf16().count() as u64,
            chunk_size: 0,
        };
        let got = ClobReader::new(&lob, 3)
            .read_to_string(&mut conn, &cx)
            .await
            .expect("stream-read CLOB");
        println!(
            "[MR4-live] CLOB chars written={} read={}",
            text.chars().count(),
            got.chars().count()
        );
        assert_eq!(
            got, text,
            "streamed CLOB write∘read must decode identically"
        );
        conn.free_temp_lobs(&cx, &[locator]).await.ok();

        conn.close(&cx).await.expect("close");
    });
}
