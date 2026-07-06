//! Regression: multi-packet WIDE-row fetch reassembly (bead rust-oracledb-n2s).
//!
//! The thin response reassembler concatenates every DATA packet of a response
//! into one buffer and then decides where the response ends. Before the fix it
//! terminated that loop whenever the *last byte* of any DATA packet happened to
//! equal `TNS_MSG_TYPE_END_OF_RESPONSE` (29 / 0x1d), regardless of packet size.
//! For a wide (20-column NUMBER/VARCHAR2) single fetch of ~1500+ rows the result
//! spans several network packets, and a perfectly ordinary payload byte at a
//! packet boundary is 0x1d often enough that the loop stopped mid-stream. The
//! truncated buffer then mis-framed in the TTC decoder, surfacing as
//! "encoded NUMBER too long" / "truncated TTC payload".
//!
//! The reference (`impl/thin/packet.pyx::Packet.has_end_of_response`) only
//! treats a trailing 0x1d as the end-of-response marker when the whole DATA
//! packet is exactly `PACKET_HEADER_SIZE + 3` bytes (header + 2-byte data flags
//! + the lone END_OF_RESPONSE message byte). This test fetches a wide,
//! multi-packet result in one batch and checks that every row arrives intact.
//!
//! Self-skips when the container environment is absent, like the rest of the
//! integration suite. Run against the container with:
//!
//! ```sh
//! eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1526 \
//!         ORACLEDB_HOST_PORT=1526 scripts/container.sh env)"
//! cargo test -p oracledb --test wide_row_multipacket
//! ```

use oracledb::protocol::thin::{BindValue, ExecuteOptions, QueryResult, QueryValue};
use oracledb::{BlockingConnection, ConnectOptions, Connection};
use oracledb_protocol::ClientIdentity;

mod common;

const PROGRAM: &str = "rust-oracledb-widerow";
const MACHINE: &str = "widerow-machine";
const OSUSER: &str = "widerow-osuser";
const TERMINAL: &str = "widerow-terminal";
const DRIVER: &str = "rust-oracledb thn : 0.0.0";

/// 10 NUMBER + 10 VARCHAR2 columns: the shape the concurrent benchmark found
/// mis-framing on. More columns => one row is wider => the result crosses a
/// packet boundary at a lower row count.
const NUM_COLS: usize = 10;
const STR_COLS: usize = 10;
const TOTAL_COLS: usize = NUM_COLS + STR_COLS;

/// Enough rows that a single `select *` fetch spans several TNS packets. The
/// benchmark observed the desync past ~1500 rows; 3000 keeps us well past it.
const ROW_COUNT: i64 = 3000;

const TABLE: &str = "RUST_WIDE_MULTIPACKET";

fn connect_options() -> Option<ConnectOptions> {
    let common::LiveCreds {
        connect_string,
        user,
        password,
    } = common::live_creds_opt()?;
    let identity = ClientIdentity::new(PROGRAM, MACHINE, OSUSER, TERMINAL, DRIVER).ok()?;
    // Request the smallest legal SDU so the wide result splits into many small
    // packets. With small packets a mid-stream packet boundary lands on a 0x1d
    // byte reliably (the string columns deliberately contain 0x1d), which is the
    // exact mis-reassembly trigger. Under the default 8 KiB SDU the boundary
    // would only occasionally hit 0x1d, making the live trigger flaky; the small
    // SDU makes it deterministic without changing what is being tested.
    Some(ConnectOptions::new(connect_string, user, password, identity).with_sdu(512))
}

/// Deterministic per-cell value generator. `id` is the row's primary key; the
/// numeric columns get distinct integer values derived from it, the string
/// columns get distinct text. The exact values do not matter for the bug — what
/// matters is that the row is wide and the fetch is multi-packet — but pinning
/// them lets the reader verify every cell, not just the count.
fn num_value(id: i64, col: usize) -> i64 {
    // Spread digits so cells differ in width; modest magnitudes keep NUMBER
    // encoding short and predictable.
    (id * 31 + col as i64 * 7) % 1_000_000
}

fn str_value(id: i64, col: usize) -> String {
    // Embed the byte 0x1d (U+001D, the TNS_MSG_TYPE_END_OF_RESPONSE marker value
    // 29) into the data, padded to varying lengths. Combined with the small SDU,
    // this guarantees that many mid-stream DATA packets end on a 0x1d byte -- the
    // exact false-positive that truncated the response before the fix. 0x1d is a
    // legal single-byte UTF-8 character and round-trips through VARCHAR2.
    let pad = (id as usize + col) % 13;
    format!("r{id}c{col}\u{1d}{}", "\u{1d}".repeat(pad))
}

fn drop_if_exists(conn: &mut Connection, ddl: &str) {
    let _ = execute_raw(conn, ddl, 1);
}

fn execute_raw(
    conn: &mut Connection,
    sql: &str,
    prefetch_rows: u32,
) -> oracledb::Result<QueryResult> {
    BlockingConnection::execute_raw(
        conn,
        sql,
        prefetch_rows,
        &[],
        ExecuteOptions::default(),
        None,
    )
}

#[test]
fn wide_row_multipacket_fetch_reassembles_every_row() {
    let Some(options) = connect_options() else {
        eprintln!("skipped wide_row_multipacket_fetch_reassembles_every_row: PYO_TEST_* not set");
        return;
    };
    let mut conn = BlockingConnection::connect(options).expect("connect to test container");

    drop_if_exists(&mut conn, &format!("drop table {TABLE} purge"));

    let num_defs: String = (0..NUM_COLS)
        .map(|i| format!("n{i} number(12)"))
        .collect::<Vec<_>>()
        .join(", ");
    let str_defs: String = (0..STR_COLS)
        .map(|i| format!("s{i} varchar2(64)"))
        .collect::<Vec<_>>()
        .join(", ");
    let create =
        format!("create table {TABLE} (id number(12) primary key, {num_defs}, {str_defs})");
    execute_raw(&mut conn, &create, 1).expect("create wide table");

    // Build the parameterized INSERT once.
    let placeholders: Vec<String> = (1..=(1 + TOTAL_COLS)).map(|i| format!(":{i}")).collect();
    let col_names: Vec<String> = std::iter::once("id".to_string())
        .chain((0..NUM_COLS).map(|i| format!("n{i}")))
        .chain((0..STR_COLS).map(|i| format!("s{i}")))
        .collect();
    let insert = format!(
        "insert into {TABLE} ({}) values ({})",
        col_names.join(", "),
        placeholders.join(", ")
    );

    // Insert in array-DML chunks to keep each round trip reasonable.
    const CHUNK: i64 = 500;
    let mut id = 1i64;
    while id <= ROW_COUNT {
        let end = (id + CHUNK - 1).min(ROW_COUNT);
        let bind_rows: Vec<Vec<BindValue>> = (id..=end)
            .map(|row_id| {
                let mut row = Vec::with_capacity(1 + TOTAL_COLS);
                row.push(BindValue::Number(row_id.to_string()));
                for c in 0..NUM_COLS {
                    row.push(BindValue::Number(num_value(row_id, c).to_string()));
                }
                for c in 0..STR_COLS {
                    row.push(BindValue::Text(str_value(row_id, c)));
                }
                row
            })
            .collect();
        BlockingConnection::execute_raw(
            &mut conn,
            &insert,
            1,
            &bind_rows,
            ExecuteOptions::default(),
            None,
        )
        .expect("array DML insert chunk");
        id = end + 1;
    }
    BlockingConnection::commit(&mut conn).expect("commit inserts");

    // Sanity: server-side count matches what we inserted.
    let count =
        execute_raw(&mut conn, &format!("select count(*) from {TABLE}"), 2).expect("count rows");
    assert_eq!(
        count.cell(0, 0).and_then(QueryValue::as_i64),
        Some(ROW_COUNT),
        "all rows should have been inserted"
    );

    // The bug: fetch all rows in ONE batch ordered by id. A large prefetch makes
    // the server return the whole result set in a single multi-packet response,
    // which is exactly the reassembly path that mis-framed. We also drain any
    // residual rows via the cursor to be robust to the server's own batch caps.
    let select = format!(
        "select id, {}, {} from {TABLE} order by id",
        (0..NUM_COLS)
            .map(|i| format!("n{i}"))
            .collect::<Vec<_>>()
            .join(", "),
        (0..STR_COLS)
            .map(|i| format!("s{i}"))
            .collect::<Vec<_>>()
            .join(", "),
    );

    let arraysize = (ROW_COUNT as u32) + 100;
    let first = execute_raw(&mut conn, &select, arraysize)
        .expect("wide multi-packet fetch must reassemble");

    assert_eq!(
        first.columns.len(),
        1 + TOTAL_COLS,
        "describe should report id + {TOTAL_COLS} data columns"
    );

    // Collect every row, draining the cursor if the server paged.
    let mut all_rows: Vec<Vec<Option<QueryValue>>> = first.rows.clone();
    let mut more = first.more_rows;
    let cursor_id = first.cursor_id;
    let mut prev = all_rows.last().cloned();
    while more {
        let batch =
            BlockingConnection::fetch_rows(&mut conn, cursor_id, arraysize, prev.as_deref())
                .expect("drain remaining wide rows");
        more = batch.more_rows;
        prev = batch.rows.last().cloned().or(prev);
        all_rows.extend(batch.rows);
    }

    assert_eq!(
        all_rows.len() as i64,
        ROW_COUNT,
        "every wide row must be fetched (multi-packet reassembly)"
    );

    // Verify EVERY cell of EVERY row: a mis-framed continuation corrupts values,
    // not just the count, so a full content check is the real proof.
    for row in &all_rows {
        assert_eq!(row.len(), 1 + TOTAL_COLS, "each row has all columns");
        let id = row[0]
            .as_ref()
            .and_then(QueryValue::as_i64)
            .expect("id present");
        assert!((1..=ROW_COUNT).contains(&id), "id in range: {id}");
        for c in 0..NUM_COLS {
            let got = row[1 + c]
                .as_ref()
                .and_then(QueryValue::as_i64)
                .unwrap_or_else(|| panic!("num col {c} present for id {id}"));
            assert_eq!(got, num_value(id, c), "num col {c} for id {id}");
        }
        for c in 0..STR_COLS {
            let got = row[1 + NUM_COLS + c]
                .as_ref()
                .and_then(|v| v.as_text())
                .unwrap_or_else(|| panic!("str col {c} present for id {id}"));
            assert_eq!(got, str_value(id, c), "str col {c} for id {id}");
        }
    }

    // Ids must be exactly 1..=ROW_COUNT with no gaps/dupes (ordering guaranteed
    // by `order by id`): catches silent row loss from an early loop break.
    for (idx, row) in all_rows.iter().enumerate() {
        let id = row[0].as_ref().and_then(QueryValue::as_i64).unwrap();
        assert_eq!(id, idx as i64 + 1, "row {idx} should have id {}", idx + 1);
    }

    drop_if_exists(&mut conn, &format!("drop table {TABLE} purge"));
    BlockingConnection::close(conn).expect("close connection");
}
