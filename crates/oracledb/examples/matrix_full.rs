//! Deep live matrix suite for `scripts/version_matrix.sh full` (bead
//! rust-oracledb-pre23ai-connect-z47u.5).
//!
//! Where `smoke.rs` proves "a connection works", this binary proves the wire
//! protocol against ONE server generation with VALUE assertions — every check
//! compares actual fetched values, not exit codes. It is run once per matrix
//! lane (XE 18, XE 21, FREE 23ai) and covers: session identity, multi-packet
//! fetch (600 rows, every value verified), wide rows (multiple DATA packets
//! per fetch page), bind DML with `rows_affected` checks, rollback/commit
//! semantics (commit verified from a second connection), CLOB/BLOB write +
//! readback above one LOB chunk, describe/metadata correctness, NULL
//! handling, NUMBER/VARCHAR2/DATE/TIMESTAMP round-trips, and deliberate error
//! paths (bad SQL, unknown table, wrong password).
//!
//! Usage (same argument style as `smoke.rs`):
//!
//! ```text
//! matrix_full [--expect-version-refusal] [CONNECT_STRING] [USER] [PASSWORD]
//! ```
//!
//! `--expect-version-refusal` is the below-floor lane (Oracle 11g / XE 11):
//! instead of running the suite, it asserts that the driver REFUSES the server
//! with the structured `UnsupportedVersion` error naming the protocol floor
//! (TNS_VERSION_MIN_ACCEPTED = 315, reference DPY-3010) — never a hang, never
//! a misleading decode error. A watchdog converts any hang into a hard exit.
//!
//! Environment fallbacks match `smoke.rs`: `PYO_TEST_CONNECT_STRING`,
//! `PYO_TEST_MAIN_USER`, `PYO_TEST_MAIN_PASSWORD`.

use std::process::ExitCode;
use std::time::Duration;

use oracledb::prelude::*;
use oracledb::protocol::thin::{
    decode_lob_text, encode_lob_text, ORA_TYPE_NUM_BLOB, ORA_TYPE_NUM_CLOB, ORA_TYPE_NUM_DATE,
    ORA_TYPE_NUM_NUMBER, ORA_TYPE_NUM_VARCHAR,
};
use oracledb::protocol::{ProtocolError, TNS_VERSION_MIN_ACCEPTED};
use oracledb::{Connection, Query};

type Suite = Result<(), Box<dyn std::error::Error>>;

const DML_TABLE: &str = "matrix_full_dml";
const LOB_TABLE: &str = "matrix_full_lob";

fn resolve(arg: Option<String>, env_key: &str, default: &str) -> String {
    arg.or_else(|| std::env::var(env_key).ok())
        .unwrap_or_else(|| default.to_string())
}

fn identity() -> Result<ClientIdentity, oracledb::Error> {
    Ok(ClientIdentity::new(
        "oracledb-matrix-full",
        "matrix-lane",
        "matrix-runner",
        "matrix",
        "rust-oracledb version matrix full suite",
    )?)
}

/// A failed assertion, with enough context to locate it without a debugger.
macro_rules! ensure {
    ($cond:expr, $($arg:tt)+) => {
        if !$cond {
            return Err(format!("assertion failed: {}", format_args!($($arg)+)).into());
        }
    };
}

fn section(name: &str) {
    eprintln!("[matrix-full] === {name} ===");
}

fn pass(name: &str, detail: &str) {
    eprintln!("[matrix-full] {name}: OK ({detail})");
}

fn drop_table_if_exists(conn: &mut Connection, table: &str) {
    // ORA-00942 (table does not exist) is the expected steady state on a
    // fresh lane; anything else surfaces on the create that follows.
    let _ = BlockingConnection::execute(conn, &format!("drop table {table} purge"), ());
}

// ---------------------------------------------------------------------------
// suite sections
// ---------------------------------------------------------------------------

fn check_session_identity(conn: &mut Connection, user: &str) -> Suite {
    section("connect + session identity");
    ensure!(conn.session_id() > 0, "session_id must be > 0");
    let row = BlockingConnection::query_one(
        conn,
        "select sys_context('userenv', 'session_user'), \
                sys_context('userenv', 'current_schema') from dual",
        (),
    )?;
    let session_user: String = row.get(0)?;
    let current_schema: String = row.get(1)?;
    ensure!(
        session_user == user.to_uppercase(),
        "session_user: expected {expected:?}, got {session_user:?}",
        expected = user.to_uppercase()
    );
    ensure!(
        current_schema == user.to_uppercase(),
        "current_schema: expected {expected:?}, got {current_schema:?}",
        expected = user.to_uppercase()
    );
    let version = conn
        .server_version()
        .ok_or("server_version missing after auth")?
        .to_string();
    pass(
        "session identity",
        &format!(
            "session_id={} serial={} session_user={session_user} server_version={version}",
            conn.session_id(),
            conn.serial_num()
        ),
    );
    Ok(())
}

fn check_multi_packet_fetch(conn: &mut Connection) -> Suite {
    section("multi-packet fetch (600 rows, every value verified)");
    let rows = BlockingConnection::query_all(
        conn,
        "select level as n, 'row-' || to_char(level) as label, mod(level * 7, 97) as m \
         from dual connect by level <= 600 order by level",
        (),
    )?;
    ensure!(rows.len() == 600, "expected 600 rows, got {}", rows.len());
    for (i, row) in rows.iter().enumerate() {
        let expected_n = i as i64 + 1;
        let n: i64 = row.get(0)?;
        let label: String = row.get(1)?;
        let m: i64 = row.get(2)?;
        ensure!(n == expected_n, "row {i}: n expected {expected_n}, got {n}");
        ensure!(
            label == format!("row-{expected_n}"),
            "row {i}: label expected row-{expected_n}, got {label:?}"
        );
        ensure!(
            m == (expected_n * 7) % 97,
            "row {i}: m expected {}, got {m}",
            (expected_n * 7) % 97
        );
    }
    pass("multi-packet fetch", "600 rows x 3 columns verified");
    Ok(())
}

fn check_wide_rows(conn: &mut Connection) -> Suite {
    section("wide rows (3 x 4000-char columns, rows larger than one SDU)");
    let rows = BlockingConnection::query_all(
        conn,
        "select rpad('a', 4000, 'a') as c1, \
                rpad(to_char(level), 4000, 'x') as c2, \
                rpad('z', 4000, 'z') as c3, \
                level as n \
         from dual connect by level <= 25 order by level",
        (),
    )?;
    ensure!(rows.len() == 25, "expected 25 rows, got {}", rows.len());
    let all_a = "a".repeat(4000);
    let all_z = "z".repeat(4000);
    for (i, row) in rows.iter().enumerate() {
        let level = i + 1;
        let c1: String = row.get(0)?;
        let c2: String = row.get(1)?;
        let c3: String = row.get(2)?;
        let n: i64 = row.get(3)?;
        let mut expected_c2 = level.to_string();
        expected_c2.push_str(&"x".repeat(4000 - expected_c2.len()));
        ensure!(c1 == all_a, "row {level}: c1 mismatch (len={})", c1.len());
        ensure!(
            c2 == expected_c2,
            "row {level}: c2 mismatch (len={})",
            c2.len()
        );
        ensure!(c3 == all_z, "row {level}: c3 mismatch (len={})", c3.len());
        ensure!(n == level as i64, "row {level}: n mismatch, got {n}");
    }
    pass("wide rows", "25 rows x 12000 chars verified byte-for-byte");
    Ok(())
}

fn check_bind_dml_rollback_commit(conn: &mut Connection, options: &ConnectOptions) -> Suite {
    section("bind DML + rollback + commit");
    drop_table_if_exists(conn, DML_TABLE);
    BlockingConnection::execute(
        conn,
        &format!(
            "create table {DML_TABLE} (\
                 id number(10), \
                 label varchar2(64), \
                 amount number(12,2), \
                 created date)"
        ),
        (),
    )?;

    // Single-row bind inserts, one rows_affected check each.
    for (id, label, amount) in [
        (1i64, "alpha", 10.25f64),
        (2, "beta", -3.5),
        (3, "gamma", 0.0),
        (4, "delta", 99999.25),
    ] {
        let outcome = BlockingConnection::execute(
            conn,
            &format!(
                "insert into {DML_TABLE} (id, label, amount, created) \
                 values (:1, :2, :3, to_date(:4, 'YYYY-MM-DD HH24:MI:SS'))"
            ),
            (id, label, amount, "2026-07-04 08:30:00"),
        )?;
        ensure!(
            outcome.rows_affected() == 1,
            "insert id={id}: rows_affected expected 1, got {}",
            outcome.rows_affected()
        );
    }

    // Array DML.
    let batch_rows: Vec<Vec<BindValue>> = (5i64..=7)
        .map(|id| {
            vec![
                BindValue::Number(id.to_string()),
                BindValue::Text(format!("bulk-{id}")),
                BindValue::Number(format!("{}.5", id * 10)),
            ]
        })
        .collect();
    let batch = BlockingConnection::execute_many(
        conn,
        &format!("insert into {DML_TABLE} (id, label, amount) values (:1, :2, :3)"),
        &batch_rows,
    )?;
    ensure!(
        batch.rows_affected() == 3,
        "execute_many: rows_affected expected 3, got {}",
        batch.rows_affected()
    );

    // Read back one row completely and check every column value.
    let row = BlockingConnection::query_one(
        conn,
        &format!(
            "select id, label, amount, to_char(created, 'YYYY-MM-DD HH24:MI:SS') \
             from {DML_TABLE} where id = :1"
        ),
        (2i64,),
    )?;
    let id: i64 = row.get(0)?;
    let label: String = row.get(1)?;
    let amount: f64 = row.get(2)?;
    let created: String = row.get(3)?;
    ensure!(id == 2, "readback id: expected 2, got {id}");
    ensure!(label == "beta", "readback label: got {label:?}");
    ensure!(
        amount == -3.5,
        "readback amount: expected -3.5, got {amount}"
    );
    ensure!(
        created == "2026-07-04 08:30:00",
        "readback created: got {created:?}"
    );

    // Bind UPDATE and DELETE with rows_affected checks.
    let update = BlockingConnection::execute(
        conn,
        &format!("update {DML_TABLE} set amount = amount + :1 where id <= :2"),
        (1i64, 4i64),
    )?;
    ensure!(
        update.rows_affected() == 4,
        "update: rows_affected expected 4, got {}",
        update.rows_affected()
    );
    let updated: i64 = BlockingConnection::query_one(
        conn,
        &format!("select count(*) from {DML_TABLE} where amount = 11.25"),
        (),
    )?
    .get(0)?;
    ensure!(updated == 1, "update value check: expected 1 row at 11.25");
    let delete = BlockingConnection::execute(
        conn,
        &format!("delete from {DML_TABLE} where id = :1"),
        (7i64,),
    )?;
    ensure!(
        delete.rows_affected() == 1,
        "delete: rows_affected expected 1, got {}",
        delete.rows_affected()
    );

    // Commit the DML baseline so the rollback probe below rolls back ONLY the
    // probe row (everything since `create table` is one open transaction).
    BlockingConnection::commit(conn)?;

    // Rollback semantics: an uncommitted insert is visible to this session,
    // gone after rollback.
    let count_before: i64 =
        BlockingConnection::query_one(conn, &format!("select count(*) from {DML_TABLE}"), ())?
            .get(0)?;
    BlockingConnection::execute(
        conn,
        &format!("insert into {DML_TABLE} (id, label) values (:1, :2)"),
        (100i64, "uncommitted"),
    )?;
    let count_dirty: i64 =
        BlockingConnection::query_one(conn, &format!("select count(*) from {DML_TABLE}"), ())?
            .get(0)?;
    ensure!(
        count_dirty == count_before + 1,
        "uncommitted insert must be visible in-session: {count_before} -> {count_dirty}"
    );
    BlockingConnection::rollback(conn)?;
    let count_after: i64 =
        BlockingConnection::query_one(conn, &format!("select count(*) from {DML_TABLE}"), ())?
            .get(0)?;
    ensure!(
        count_after == count_before,
        "rollback must remove the insert: expected {count_before}, got {count_after}"
    );

    // Commit semantics, verified from a SECOND connection.
    BlockingConnection::execute(
        conn,
        &format!("insert into {DML_TABLE} (id, label) values (:1, :2)"),
        (99i64, "committed"),
    )?;
    BlockingConnection::commit(conn)?;
    let mut observer = BlockingConnection::connect(ConnectOptions::new(
        options.connect_string().to_string(),
        options.user().to_string(),
        options.password().to_string(),
        identity()?,
    ))?;
    let observed: String = BlockingConnection::query_one(
        &mut observer,
        &format!("select label from {DML_TABLE} where id = :1"),
        (99i64,),
    )?
    .get(0)?;
    BlockingConnection::close(observer)?;
    ensure!(
        observed == "committed",
        "second connection must see the committed row, got {observed:?}"
    );

    pass(
        "bind DML",
        "7 bind inserts, update/delete rows_affected, rollback + cross-connection commit verified",
    );
    Ok(())
}

fn check_lob_roundtrip(conn: &mut Connection) -> Suite {
    section("LOB write + readback (CLOB and BLOB above one chunk)");
    drop_table_if_exists(conn, LOB_TABLE);
    BlockingConnection::execute(
        conn,
        &format!("create table {LOB_TABLE} (id number(5), c clob, b blob)"),
        (),
    )?;
    BlockingConnection::execute(
        conn,
        &format!("insert into {LOB_TABLE} (id, c, b) values (1, empty_clob(), empty_blob())"),
        (),
    )?;

    // Deterministic payloads, both far above one LOB chunk (~8K) and above one
    // SDU, so both write and readback split across multiple DATA packets.
    // ASCII only: for a CLOB, offsets/amounts are in characters.
    let clob_text: String = (0..4000)
        .map(|i| format!("clob-{i:05}-abcdefghijklmnopqrst\n"))
        .collect();
    let blob_bytes: Vec<u8> = (0..90_000u32).map(|i| ((i * 31 + 7) % 251) as u8).collect();

    // Fetch the empty-LOB locators (stream_lobs keeps them as locators).
    // collect() rather than one(): a single-row LOB query can leave the
    // server's more-rows flag set until the next fetch round trip.
    let locator_rows = BlockingConnection::query_with(
        conn,
        Query::new("select c, b from matrix_full_lob where id = 1 for update").stream_lobs(),
    )?
    .collect()?;
    ensure!(
        locator_rows.len() == 1,
        "locator query: expected 1 row, got {}",
        locator_rows.len()
    );
    let locator_row = &locator_rows[0];
    let clob = match locator_row.value(0) {
        Some(QueryValue::Lob(lob)) => lob.clone(),
        other => return Err(format!("expected CLOB locator, got {other:?}").into()),
    };
    let blob = match locator_row.value(1) {
        Some(QueryValue::Lob(lob)) => lob.clone(),
        other => return Err(format!("expected BLOB locator, got {other:?}").into()),
    };

    // Chunked writes through the driver's LOB write path.
    let mut clob_locator = clob.locator.clone();
    let chars: Vec<char> = clob_text.chars().collect();
    let mut offset = 1u64;
    for chunk in chars.chunks(32_000) {
        let chunk_text: String = chunk.iter().collect();
        let encoded = encode_lob_text(&chunk_text, clob.csfrm, Some(&clob_locator));
        let written = BlockingConnection::write_lob(conn, &clob_locator, offset, &encoded)?;
        if !written.locator.is_empty() {
            clob_locator = written.locator;
        }
        offset += chunk.len() as u64;
    }
    let mut blob_locator = blob.locator.clone();
    let mut offset = 1u64;
    for chunk in blob_bytes.chunks(32_768) {
        let written = BlockingConnection::write_lob(conn, &blob_locator, offset, chunk)?;
        if !written.locator.is_empty() {
            blob_locator = written.locator;
        }
        offset += chunk.len() as u64;
    }
    BlockingConnection::commit(conn)?;

    // Server-side length checks.
    let row = BlockingConnection::query_one(
        conn,
        &format!(
            "select dbms_lob.getlength(c), dbms_lob.getlength(b) from {LOB_TABLE} where id = 1"
        ),
        (),
    )?;
    let clob_len: i64 = row.get(0)?;
    let blob_len: i64 = row.get(1)?;
    ensure!(
        clob_len == chars.len() as i64,
        "CLOB length: expected {}, got {clob_len}",
        chars.len()
    );
    ensure!(
        blob_len == blob_bytes.len() as i64,
        "BLOB length: expected {}, got {blob_len}",
        blob_bytes.len()
    );

    // Full readback through the driver's LOB read path, compared exactly.
    let readback_rows = BlockingConnection::query_with(
        conn,
        Query::new("select c, b from matrix_full_lob where id = 1").stream_lobs(),
    )?
    .collect()?;
    ensure!(
        readback_rows.len() == 1,
        "readback query: expected 1 row, got {}",
        readback_rows.len()
    );
    let readback_row = &readback_rows[0];
    let read_clob = match readback_row.value(0) {
        Some(QueryValue::Lob(lob)) => lob.clone(),
        other => return Err(format!("readback: expected CLOB locator, got {other:?}").into()),
    };
    let read_blob = match readback_row.value(1) {
        Some(QueryValue::Lob(lob)) => lob.clone(),
        other => return Err(format!("readback: expected BLOB locator, got {other:?}").into()),
    };
    let clob_read = BlockingConnection::read_lob(conn, &read_clob.locator, 1, read_clob.size)?
        .data
        .ok_or("CLOB read returned no data")?;
    let clob_back = decode_lob_text(&clob_read, read_clob.csfrm, Some(&read_clob.locator))?;
    ensure!(
        clob_back == clob_text,
        "CLOB readback mismatch: {} chars back vs {} written",
        clob_back.chars().count(),
        chars.len()
    );
    let blob_back = BlockingConnection::read_lob(conn, &read_blob.locator, 1, read_blob.size)?
        .data
        .ok_or("BLOB read returned no data")?;
    ensure!(
        blob_back == blob_bytes,
        "BLOB readback mismatch: {} bytes back vs {} written",
        blob_back.len(),
        blob_bytes.len()
    );

    pass(
        "LOB roundtrip",
        &format!(
            "CLOB {} chars + BLOB {} bytes written in chunks, read back byte-identical",
            chars.len(),
            blob_bytes.len()
        ),
    );
    Ok(())
}

fn check_describe_metadata(conn: &mut Connection) -> Suite {
    section("describe / column metadata");
    // Zero-row query: everything below comes from the DESCRIBE response only.
    let rows = BlockingConnection::query(
        conn,
        &format!("select id, label, amount, created from {DML_TABLE} where 1 = 0"),
        (),
    )?;
    let columns = rows.columns().to_vec();
    drop(rows);
    ensure!(
        columns.len() == 4,
        "expected 4 columns, got {}",
        columns.len()
    );
    let expect = [
        ("ID", ORA_TYPE_NUM_NUMBER, 10i8, 0i8),
        ("LABEL", ORA_TYPE_NUM_VARCHAR, 0, 0),
        ("AMOUNT", ORA_TYPE_NUM_NUMBER, 12, 2),
        ("CREATED", ORA_TYPE_NUM_DATE, 0, 0),
    ];
    for (i, (name, ora_type, precision, scale)) in expect.iter().enumerate() {
        let col = &columns[i];
        ensure!(
            col.name() == *name,
            "column {i}: name expected {name}, got {:?}",
            col.name()
        );
        ensure!(
            col.ora_type_num() == *ora_type,
            "column {name}: ora_type_num expected {ora_type}, got {}",
            col.ora_type_num()
        );
        if *precision != 0 {
            ensure!(
                col.precision() == *precision,
                "column {name}: precision expected {precision}, got {}",
                col.precision()
            );
            ensure!(
                col.scale() == *scale,
                "column {name}: scale expected {scale}, got {}",
                col.scale()
            );
        }
    }
    let lob_rows = BlockingConnection::query(
        conn,
        &format!("select c, b from {LOB_TABLE} where 1 = 0"),
        (),
    )?;
    let lob_columns = lob_rows.columns().to_vec();
    drop(lob_rows);
    ensure!(
        lob_columns[0].ora_type_num() == ORA_TYPE_NUM_CLOB,
        "CLOB column: ora_type_num expected {ORA_TYPE_NUM_CLOB}, got {}",
        lob_columns[0].ora_type_num()
    );
    ensure!(
        lob_columns[1].ora_type_num() == ORA_TYPE_NUM_BLOB,
        "BLOB column: ora_type_num expected {ORA_TYPE_NUM_BLOB}, got {}",
        lob_columns[1].ora_type_num()
    );
    pass(
        "describe",
        "names, ora_type_num, precision/scale for NUMBER/VARCHAR2/DATE/CLOB/BLOB",
    );
    Ok(())
}

fn check_null_handling(conn: &mut Connection) -> Suite {
    section("NULL handling");
    let row = BlockingConnection::query_one(
        conn,
        "select cast(null as varchar2(10)), cast(null as number), cast(null as date) from dual",
        (),
    )?;
    for i in 0..3 {
        ensure!(
            row.value(i).is_none(),
            "column {i}: expected NULL, got {:?}",
            row.value(i)
        );
    }
    ensure!(
        row.try_get::<String>(0)?.is_none(),
        "try_get on NULL VARCHAR2 must be None"
    );
    ensure!(
        row.try_get::<i64>(1)?.is_none(),
        "try_get on NULL NUMBER must be None"
    );
    ensure!(
        row.get::<String>(0).is_err(),
        "get::<String> on NULL must be a hard error, not a default"
    );

    // NULL travels correctly in binds too (Option::None -> NULL in, NULL out).
    BlockingConnection::execute(
        conn,
        &format!("insert into {DML_TABLE} (id, label) values (:1, :2)"),
        (200i64, Option::<String>::None),
    )?;
    let back = BlockingConnection::query_one(
        conn,
        &format!("select label from {DML_TABLE} where id = :1"),
        (200i64,),
    )?;
    ensure!(
        back.try_get::<String>(0)?.is_none(),
        "NULL bind roundtrip: expected NULL label back"
    );
    BlockingConnection::rollback(conn)?;
    pass(
        "NULL handling",
        "NULL select, try_get/get contracts, NULL bind roundtrip",
    );
    Ok(())
}

fn check_scalar_roundtrips(conn: &mut Connection) -> Suite {
    section("NUMBER / VARCHAR2 / DATE / TIMESTAMP round-trips");

    // NUMBER: exact integers (including one beyond f64's 2^53 mantissa).
    for value in [0i64, -1, 42, 9_007_199_254_740_993, i64::MAX, i64::MIN] {
        let back: i64 = BlockingConnection::query_one(conn, "select :1 from dual", (value,))
            .map_err(|err| format!("NUMBER i64 roundtrip for {value}: {err}"))?
            .get(0)?;
        ensure!(
            back == value,
            "NUMBER i64 roundtrip: sent {value}, got {back}"
        );
    }
    // NUMBER: decimal text kept lossless through the text-NUMBER path.
    let decimal: String = BlockingConnection::query_one(
        conn,
        "select to_char(to_number(:1)) from dual",
        ("-123456789.25",),
    )
    .map_err(|err| format!("NUMBER decimal text roundtrip: {err}"))?
    .get(0)?;
    ensure!(
        decimal == "-123456789.25",
        "NUMBER decimal roundtrip: got {decimal:?}"
    );
    // NUMBER: f64 through a scaled cast.
    let amount: f64 = BlockingConnection::query_one(
        conn,
        "select cast(:1 as number(10,2)) from dual",
        (-12.75f64,),
    )
    .map_err(|err| format!("NUMBER f64 cast roundtrip: {err}"))?
    .get(0)?;
    ensure!(amount == -12.75, "NUMBER f64 roundtrip: got {amount}");

    // VARCHAR2: multi-byte text must survive bind + fetch byte-identically.
    // Distinct SQL text from the NUMBER probes above: the statement cache is
    // keyed by SQL text and re-executing a cached cursor with a DIFFERENT bind
    // type reuses the old bind metadata (tracked as a separate driver issue;
    // the reference re-describes on bind-type change).
    let unicode = "naïve-Ω-δοκιμή-支持-🎯";
    let text: String =
        BlockingConnection::query_one(conn, "select :1 as v_text from dual", (unicode,))
            .map_err(|err| format!("VARCHAR2 unicode roundtrip: {err}"))?
            .get(0)?;
    ensure!(text == unicode, "VARCHAR2 unicode roundtrip: got {text:?}");

    // DATE: full second precision, decoded into the structured DateTime value.
    let row = BlockingConnection::query_one(
        conn,
        "select to_date('2026-07-04 12:34:56', 'YYYY-MM-DD HH24:MI:SS') from dual",
        (),
    )
    .map_err(|err| format!("DATE literal decode: {err}"))?;
    match row.value(0) {
        Some(QueryValue::DateTime {
            year: 2026,
            month: 7,
            day: 4,
            hour: 12,
            minute: 34,
            second: 56,
            nanosecond: 0,
        }) => {}
        other => return Err(format!("DATE decode: unexpected value {other:?}").into()),
    }
    // DATE round-trip through a bind (text in, structured value out, text back).
    let date_text: String = BlockingConnection::query_one(
        conn,
        "select to_char(to_date(:1, 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS') from dual",
        ("1999-12-31 23:59:59",),
    )
    .map_err(|err| format!("DATE bind roundtrip: {err}"))?
    .get(0)?;
    ensure!(
        date_text == "1999-12-31 23:59:59",
        "DATE bind roundtrip: got {date_text:?}"
    );

    // TIMESTAMP: fractional seconds preserved.
    let row = BlockingConnection::query_one(
        conn,
        "select timestamp '2026-07-04 12:34:56.123456' from dual",
        (),
    )
    .map_err(|err| format!("TIMESTAMP literal decode: {err}"))?;
    match row.value(0) {
        Some(QueryValue::DateTime {
            year: 2026,
            month: 7,
            day: 4,
            hour: 12,
            minute: 34,
            second: 56,
            nanosecond: 123_456_000,
        }) => {}
        other => return Err(format!("TIMESTAMP decode: unexpected value {other:?}").into()),
    }

    pass(
        "scalar roundtrips",
        "NUMBER (i64/decimal/f64), VARCHAR2 unicode, DATE, TIMESTAMP fractional seconds",
    );
    Ok(())
}

fn check_error_paths(conn: &mut Connection, options: &ConnectOptions) -> Suite {
    section("deliberate error paths");

    // Bad SQL: a real ORA parse error with its code surfaced, session intact.
    let bad_sql = BlockingConnection::query_all(conn, "select from where", ());
    let err = match bad_sql {
        Err(err) => err,
        Ok(_) => return Err("bad SQL must fail".into()),
    };
    let code = err
        .ora_code()
        .ok_or_else(|| format!("bad SQL error must carry an ORA code, got: {err}"))?;
    ensure!(
        [900, 903, 923, 933, 936].contains(&code),
        "bad SQL: expected an ORA parse-error code, got ORA-{code:05}: {err}"
    );

    // Unknown table: ORA-00942 exactly.
    let missing = BlockingConnection::query_all(conn, "select * from matrix_full_missing_t", ());
    let err = match missing {
        Err(err) => err,
        Ok(_) => return Err("select from unknown table must fail".into()),
    };
    ensure!(
        err.ora_code() == Some(942),
        "unknown table: expected ORA-00942, got: {err}"
    );

    // The session must survive both server errors.
    let alive: i64 = BlockingConnection::query_one(conn, "select 42 from dual", ())?.get(0)?;
    ensure!(alive == 42, "session must stay usable after ORA errors");

    // Wrong password: a clean ORA-01017 refusal, not a protocol error or hang.
    let denied = BlockingConnection::connect(ConnectOptions::new(
        options.connect_string().to_string(),
        options.user().to_string(),
        "definitely-wrong-password".to_string(),
        identity()?,
    ));
    let err = match denied {
        Err(err) => err,
        Ok(conn) => {
            let _ = BlockingConnection::close(conn);
            return Err("wrong password must be rejected".into());
        }
    };
    ensure!(
        err.ora_code() == Some(1017),
        "wrong password: expected ORA-01017, got: {err}"
    );

    pass(
        "error paths",
        &format!("bad SQL ORA-{code:05}, unknown table ORA-00942, wrong password ORA-01017"),
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// modes
// ---------------------------------------------------------------------------

fn run_full(connect_string: &str, user: &str, password: &str) -> Suite {
    eprintln!("[matrix-full] connecting to {connect_string} as {user} ...");
    let options = ConnectOptions::new(
        connect_string.to_string(),
        user.to_string(),
        password.to_string(),
        identity()?,
    );
    let mut conn = BlockingConnection::connect(ConnectOptions::new(
        connect_string.to_string(),
        user.to_string(),
        password.to_string(),
        identity()?,
    ))?;

    let result = (|| -> Suite {
        check_session_identity(&mut conn, user)?;
        check_multi_packet_fetch(&mut conn)?;
        check_wide_rows(&mut conn)?;
        check_bind_dml_rollback_commit(&mut conn, &options)?;
        check_lob_roundtrip(&mut conn)?;
        check_describe_metadata(&mut conn)?;
        check_null_handling(&mut conn)?;
        check_scalar_roundtrips(&mut conn)?;
        check_error_paths(&mut conn, &options)?;
        Ok(())
    })();

    // Best-effort cleanup either way; the verdict is `result`.
    drop_table_if_exists(&mut conn, DML_TABLE);
    drop_table_if_exists(&mut conn, LOB_TABLE);
    let _ = BlockingConnection::close(conn);
    result?;
    eprintln!("[matrix-full] ALL SECTIONS PASSED");
    Ok(())
}

fn run_expect_version_refusal(connect_string: &str, user: &str, password: &str) -> Suite {
    eprintln!(
        "[matrix-full] below-floor lane: expecting a structured protocol-version refusal \
         from {connect_string}"
    );
    let attempt = BlockingConnection::connect(ConnectOptions::new(
        connect_string.to_string(),
        user.to_string(),
        password.to_string(),
        identity()?,
    ));
    match attempt {
        Ok(conn) => {
            let _ = BlockingConnection::close(conn);
            Err("connect unexpectedly SUCCEEDED against a below-floor server".into())
        }
        Err(oracledb::Error::Protocol(ProtocolError::UnsupportedVersion { version, minimum })) => {
            ensure!(
                minimum == TNS_VERSION_MIN_ACCEPTED,
                "refusal must name the floor {TNS_VERSION_MIN_ACCEPTED}, named {minimum}"
            );
            ensure!(
                version < minimum,
                "refused version {version} must be below the floor {minimum}"
            );
            eprintln!(
                "[matrix-full] structured refusal verified: server protocol version {version} \
                 refused against floor {minimum} (reference parity: DPY-3010)"
            );
            Ok(())
        }
        Err(other) => {
            Err(format!("expected the structured UnsupportedVersion refusal, got: {other}").into())
        }
    }
}

fn main() -> ExitCode {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let expect_refusal = args
        .first()
        .is_some_and(|arg| arg == "--expect-version-refusal");
    if expect_refusal {
        args.remove(0);
    }
    let mut args = args.into_iter();
    let connect_string = resolve(
        args.next(),
        "PYO_TEST_CONNECT_STRING",
        "localhost:1525/FREEPDB1",
    );
    let user = resolve(args.next(), "PYO_TEST_MAIN_USER", "pythontest");
    let password = resolve(args.next(), "PYO_TEST_MAIN_PASSWORD", "pythontest");

    // Watchdog: this binary's whole point is "never hang". If the suite (or
    // the refusal handshake) wedges, convert the hang into a hard, attributable
    // failure instead of stalling the matrix lane forever.
    let budget = if expect_refusal {
        Duration::from_secs(60)
    } else {
        Duration::from_secs(600)
    };
    std::thread::spawn(move || {
        std::thread::sleep(budget);
        eprintln!(
            "[matrix-full] WATCHDOG: exceeded {}s budget — treating as hang",
            budget.as_secs()
        );
        std::process::exit(3);
    });

    let result = if expect_refusal {
        run_expect_version_refusal(&connect_string, &user, &password)
    } else {
        run_full(&connect_string, &user, &password)
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("[matrix-full] FAILED: {err}");
            ExitCode::FAILURE
        }
    }
}
