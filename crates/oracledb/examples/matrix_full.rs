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
use oracledb::retry::{run_with_retry, Idempotency, RetryPolicy};
use oracledb::{Batch, Connection, Execute, PipelineRequest, Query, StatementShapeCache};

use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::Cx;

type Suite = Result<(), Box<dyn std::error::Error>>;

const DML_TABLE: &str = "matrix_full_dml";
const LOB_TABLE: &str = "matrix_full_lob";
const PIPE_TABLE: &str = "matrix_full_pipe";
const BATCH_TABLE: &str = "matrix_full_batch";
const SHAPE_TABLE: &str = "matrix_full_shape";
const RETRY_TABLE: &str = "matrix_full_retry";
#[cfg(feature = "arrow")]
const VECTOR_TABLE: &str = "matrix_full_vector";

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
    // Deliberately the SAME SQL text as the NUMBER probes above: a rebind
    // with a different TYPE on a cached statement must re-parse instead of
    // coercing through the stale NUMBER bind metadata (bead
    // rust-oracledb-ilel; regression test in tests/live_statement_cache.rs).
    let unicode = "naïve-Ω-δοκιμή-支持-🎯";
    let text: String = BlockingConnection::query_one(conn, "select :1 from dual", (unicode,))
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
// 0.7.3 differentiator sections (per-capability VALUE asserts)
//
// Beyond the wire-correctness suite above, these prove the 0.7.3 surpass
// features against THIS server generation, asserting the actual capability
// behavior (not just "connects"). Version-specific capabilities gate on the
// negotiated server major version: SODA at 21c+, VECTOR at 23ai. The rest
// (pipelining, batch errors, call timeout, shape-cache self-heal, LOB
// streaming, the retry executor) work identically across 18c/21c/23ai and are
// asserted to do so on every working lane.
// ---------------------------------------------------------------------------

/// Pipelining (A8 / a4-pipeline): N ops collapsed into ONE server round trip.
/// Version-specific — pipelining needs END_OF_RESPONSE framing (23ai+); on
/// pre-23ai servers the driver refuses it with a typed `UnsupportedFeature`.
fn check_pipeline_single_round_trip(conn: &mut Connection, major: u8) -> Suite {
    section("pipelining: N ops collapsed into one round trip (A8; 23ai-only)");

    if major < 23 {
        // Version gate: pipelining requires END_OF_RESPONSE framing, negotiated
        // only by 23ai+. The driver must refuse it with the typed feature error.
        let probe = [PipelineRequest::execute(
            "select 1 from dual".to_string(),
            Vec::new(),
            1,
        )];
        let err = match BlockingConnection::run_pipeline(conn, &probe, false) {
            Err(err) => err,
            Ok(_) => return Err("pipelining unexpectedly succeeded on a pre-23ai server".into()),
        };
        ensure!(
            matches!(
                err,
                oracledb::Error::Protocol(oracledb::protocol::ProtocolError::UnsupportedFeature(_))
            ),
            "pre-23ai pipelining must fail with a typed UnsupportedFeature error, got: {err}"
        );
        pass(
            "pipelining gate",
            &format!("version={major}c: pipelining refused (UnsupportedFeature); needs 23ai END_OF_RESPONSE framing"),
        );
        return Ok(());
    }

    drop_table_if_exists(conn, PIPE_TABLE);
    BlockingConnection::execute(
        conn,
        &format!("create table {PIPE_TABLE} (id number(5), val varchar2(16))"),
        (),
    )?;

    // `run_pipeline`/`run_pipeline_decoded` both actually EXECUTE the ops, so
    // the single-round-trip collapse is proven with a READ-ONLY pipeline (safe
    // to run for the raw-payload count) and the per-op decode + mutation is
    // proven once with a decoded pipeline.

    // Collapse proof: three independent SELECTs return one payload per op plus
    // the trailing end-pipeline marker (N + 1). Un-pipelined, this would be N
    // separate round trips with no end marker.
    let read_only = [
        PipelineRequest::execute("select 1 from dual".to_string(), Vec::new(), 1),
        PipelineRequest::execute("select 2 from dual".to_string(), Vec::new(), 1),
        PipelineRequest::execute("select 3 from dual".to_string(), Vec::new(), 1),
    ];
    let raw = BlockingConnection::run_pipeline(conn, &read_only, false)?;
    ensure!(
        raw.len() == read_only.len() + 1,
        "pipeline must return one response per op plus the end-pipeline marker: \
         expected {}, got {}",
        read_only.len() + 1,
        raw.len()
    );

    // Decode proof: a single mutating pipeline (2 inserts + commit + select),
    // run exactly once. Value-assert each op's decoded outcome.
    let mutating = [
        PipelineRequest::execute(
            format!("insert into {PIPE_TABLE} values (1, 'one')"),
            Vec::new(),
            1,
        ),
        PipelineRequest::execute(
            format!("insert into {PIPE_TABLE} values (:1, :2)"),
            vec![vec![
                BindValue::Number("2".to_string()),
                BindValue::Text("two".to_string()),
            ]],
            1,
        ),
        PipelineRequest::commit(),
        PipelineRequest::execute(
            format!("select id, val from {PIPE_TABLE} order by id"),
            Vec::new(),
            100,
        ),
    ];
    let decoded = BlockingConnection::run_pipeline_decoded(conn, &mutating, false)?;
    ensure!(
        decoded.len() == mutating.len(),
        "decoded pipeline results: expected {}, got {}",
        mutating.len(),
        decoded.len()
    );
    for op in [0usize, 1] {
        let insert = decoded[op]
            .as_ref()
            .map_err(|e| format!("pipeline insert op {op}: {e}"))?;
        ensure!(
            insert.row_count == 1,
            "pipeline insert op {op}: row_count expected 1, got {}",
            insert.row_count
        );
    }
    let select = decoded[3]
        .as_ref()
        .map_err(|e| format!("pipeline select op: {e}"))?;
    ensure!(
        select.rows.len() == 2,
        "pipeline select: expected 2 rows, got {}",
        select.rows.len()
    );
    for (i, expected) in ["1", "2"].iter().enumerate() {
        let id_ok = matches!(
            &select.rows[i][0],
            Some(v) if v.as_number_text().as_deref() == Some(*expected)
        );
        ensure!(
            id_ok,
            "pipeline select row {i}: expected id {expected}, got {:?}",
            select.rows[i][0]
        );
    }

    drop_table_if_exists(conn, PIPE_TABLE);
    pass(
        "pipelining",
        "3-op read pipeline collapses to N+1 responses; 4-op mutating pipeline decoded + verified",
    );
    Ok(())
}

/// Batch executemany (a4-j1w): per-row `BatchError` continuation — the good
/// rows still apply while the offending row is reported by index + ORA code.
fn check_batch_errors(conn: &mut Connection) -> Suite {
    section("batch executemany: per-row BatchError continuation (a4-j1w)");
    drop_table_if_exists(conn, BATCH_TABLE);
    BlockingConnection::execute(
        conn,
        &format!("create table {BATCH_TABLE} (id number(5) primary key, label varchar2(32))"),
        (),
    )?;
    // Seed id=2 so the middle row of the batch collides with the primary key.
    BlockingConnection::execute(
        conn,
        &format!("insert into {BATCH_TABLE} values (2, 'seed')"),
        (),
    )?;
    BlockingConnection::commit(conn)?;

    let rows: Vec<Vec<BindValue>> = [1i64, 2, 3]
        .iter()
        .map(|id| {
            vec![
                BindValue::Number(id.to_string()),
                BindValue::Text(format!("row-{id}")),
            ]
        })
        .collect();
    let outcome = BlockingConnection::execute_many_with(
        conn,
        Batch::new(
            &format!("insert into {BATCH_TABLE} (id, label) values (:1, :2)"),
            &rows,
        )
        .collect_errors(),
    )?;
    ensure!(
        outcome.errors().len() == 1,
        "batch must report exactly one row error, got {}",
        outcome.errors().len()
    );
    let err = &outcome.errors()[0];
    ensure!(
        err.row_index() == 1,
        "batch error row_index: expected 1 (the duplicate), got {}",
        err.row_index()
    );
    ensure!(
        err.code() == 1,
        "batch error code: expected ORA-00001 (unique constraint), got ORA-{:05}",
        err.code()
    );
    // Continuation: the two good rows (0 and 2) were applied despite the middle
    // failure — seed(2) + row(1) + row(3) = 3.
    let count: i64 =
        BlockingConnection::query_one(conn, &format!("select count(*) from {BATCH_TABLE}"), ())?
            .get(0)?;
    ensure!(
        count == 3,
        "batch continuation: expected 3 rows (seed + 2 good), got {count}"
    );

    BlockingConnection::rollback(conn)?;
    drop_table_if_exists(conn, BATCH_TABLE);
    pass(
        "batch errors",
        "row 1 -> ORA-00001 at correct index; rows 0 and 2 continued and applied",
    );
    Ok(())
}

/// Call timeout (A1.1 / GH#14): a slow server call surfaces the typed
/// `Error::CallTimeout` at the configured budget, and the session survives it.
fn check_call_timeout(conn: &mut Connection) -> Suite {
    section("call timeout: slow call -> Error::CallTimeout, session survives (A1.1 / GH#14)");
    let slow = "begin dbms_session.sleep(3); end;";
    let outcome = BlockingConnection::execute_with(
        conn,
        Execute::new(slow).timeout(Duration::from_millis(500)),
    );
    match outcome {
        Err(oracledb::Error::CallTimeout(ms)) => {
            ensure!(
                ms == 500,
                "CallTimeout must report the configured 500ms budget, got {ms}"
            );
        }
        Err(other) => return Err(format!("expected Error::CallTimeout, got: {other}").into()),
        Ok(_) => return Err("the 3s sleep must have timed out at 500ms".into()),
    }
    // The connection must remain usable: a call timeout drains the server
    // response and leaves the session intact (not connection-lost).
    let alive: i64 = BlockingConnection::query_one(conn, "select 42 from dual", ())?.get(0)?;
    ensure!(alive == 42, "session must survive a call timeout");
    pass(
        "call timeout",
        "3s sleep interrupted at 500ms as Error::CallTimeout(500); session reusable",
    );
    Ok(())
}

/// Cross-connection statement-shape cache + DDL-invalidation self-heal
/// (a4-8pp): a real describe on connection A records generation 1; DDL from a
/// SECOND connection changes the shape; A re-describes and the cache self-heals
/// to generation 2, flagging that a rebind is required.
fn check_shape_cache_self_heal(conn: &mut Connection, options: &ConnectOptions) -> Suite {
    section("statement-shape cache: DDL-invalidation self-heal (a4-8pp)");
    drop_table_if_exists(conn, SHAPE_TABLE);
    BlockingConnection::execute(
        conn,
        &format!("create table {SHAPE_TABLE} (id number(10), label varchar2(32))"),
        (),
    )?;
    let sql = format!("select * from {SHAPE_TABLE}");
    let cache = StatementShapeCache::new();

    // Connection A: describe the pre-DDL 2-column shape (generation 1).
    let rows_v1 = BlockingConnection::query(conn, &sql, ())?;
    let cols_v1 = rows_v1.columns().len();
    let obs1 = cache.observe(&sql, rows_v1.columns());
    drop(rows_v1);
    ensure!(
        cols_v1 == 2,
        "pre-DDL shape must have 2 columns, got {cols_v1}"
    );
    ensure!(
        obs1.first_seen && !obs1.self_healed && obs1.generation == 1,
        "first observation must be generation 1, first-seen, not self-healed: {obs1:?}"
    );

    // A SECOND connection alters the table shape (adds a column).
    let mut other = BlockingConnection::connect(ConnectOptions::new(
        options.connect_string().to_string(),
        options.user().to_string(),
        options.password().to_string(),
        identity()?,
    ))?;
    BlockingConnection::execute(
        &mut other,
        &format!("alter table {SHAPE_TABLE} add (extra number(3))"),
        (),
    )?;
    BlockingConnection::close(other)?;

    // Connection A re-describes: the cache self-heals to the 3-column shape.
    let rows_v2 = BlockingConnection::query(conn, &sql, ())?;
    let cols_v2 = rows_v2.columns().len();
    let obs2 = cache.observe(&sql, rows_v2.columns());
    drop(rows_v2);
    ensure!(
        cols_v2 == 3,
        "post-DDL shape must have 3 columns, got {cols_v2}"
    );
    ensure!(
        obs2.self_healed && obs2.requires_rebind() && obs2.generation == 2,
        "post-DDL observation must self-heal to generation 2 requiring rebind: {obs2:?}"
    );
    let (gen, _) = cache
        .current(&sql)
        .ok_or("shape must be cached after observe")?;
    ensure!(
        gen == 2,
        "cached generation must be 2 after self-heal, got {gen}"
    );

    drop_table_if_exists(conn, SHAPE_TABLE);
    pass(
        "shape cache",
        "gen1 (2 cols) -> cross-connection DDL -> self-heal gen2 (3 cols), rebind required",
    );
    Ok(())
}

/// VECTOR -> Arrow `FixedSizeList` columnar fast path (a4-0mk). 23ai-only: on
/// pre-23ai lanes the VECTOR type does not exist, which the gate asserts as a
/// real server error; on 23ai it fetches dense VECTORs into a
/// `FixedSizeList(Float32, dim)` and value-checks the elements.
#[cfg(feature = "arrow")]
fn check_vector_fixed_size_list(conn: &mut Connection, major: u8) -> Suite {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float32Type;
    use arrow_schema::DataType;
    use oracledb::arrow::ArrowFetchOptions;

    section("VECTOR -> Arrow FixedSizeList columnar fast path (a4-0mk; 23ai-only)");
    drop_table_if_exists(conn, VECTOR_TABLE);
    let create = BlockingConnection::execute(
        conn,
        &format!("create table {VECTOR_TABLE} (id number(5), embedding vector(3, float32))"),
        (),
    );

    if major < 23 {
        // Version gate: the VECTOR datatype does not exist before 23ai.
        let err = match create {
            Err(err) => err,
            Ok(_) => {
                drop_table_if_exists(conn, VECTOR_TABLE);
                return Err(
                    "VECTOR table create unexpectedly succeeded on a pre-23ai server".into(),
                );
            }
        };
        let code = err
            .ora_code()
            .ok_or_else(|| format!("pre-23ai VECTOR create must be a real server error: {err}"))?;
        pass(
            "VECTOR gate",
            &format!(
                "version={major}c: VECTOR type unavailable (ORA-{code:05}); \
                 FixedSizeList fast path N/A (23ai-only)"
            ),
        );
        return Ok(());
    }

    create?;
    BlockingConnection::execute(
        conn,
        &format!("insert into {VECTOR_TABLE} values (1, to_vector('[1.5, 2.5, 3.5]'))"),
        (),
    )?;
    BlockingConnection::execute(
        conn,
        &format!("insert into {VECTOR_TABLE} values (2, to_vector('[4.5, 5.5, 6.5]'))"),
        (),
    )?;
    BlockingConnection::commit(conn)?;

    let select = format!("select embedding from {VECTOR_TABLE} order by id");

    // COLD Arrow fetch FIRST — no prior query on this statement (bead a4-0mk).
    // A VECTOR column comes back from the execute as describe-only metadata; the
    // Arrow fetch path establishes the client-side define before its first fetch,
    // so `fetch_all_record_batch` works standalone (it used to desync with
    // "invalid ub8 length" unless a prior query had warmed the cursor's define).
    let options = ArrowFetchOptions::new().with_vector_fixed_size_list(true);
    let batch = BlockingConnection::fetch_all_record_batch(conn, &select, 100, &options)?;
    ensure!(
        batch.num_rows() == 2,
        "expected 2 VECTOR rows, got {}",
        batch.num_rows()
    );
    match batch.schema().field(0).data_type() {
        DataType::FixedSizeList(field, 3) => {
            ensure!(
                matches!(field.data_type(), DataType::Float32),
                "FixedSizeList element type must be Float32, got {:?}",
                field.data_type()
            );
        }
        other => {
            return Err(
                format!("VECTOR must map to FixedSizeList(Float32, 3), got {other:?}").into(),
            )
        }
    }
    let list = batch.column(0).as_fixed_size_list();
    let expected = [[1.5f32, 2.5, 3.5], [4.5, 5.5, 6.5]];
    for (i, want) in expected.iter().enumerate() {
        let cell = list.value(i);
        let got: Vec<f32> = cell.as_primitive::<Float32Type>().values().to_vec();
        ensure!(
            got == want,
            "VECTOR row {i}: expected {want:?}, got {got:?}"
        );
    }

    // Also prove the standard row fetch path decodes VECTOR correctly (now warm).
    let std_rows = BlockingConnection::query_all(conn, &select, ())?;
    ensure!(
        std_rows.len() == 2,
        "standard VECTOR fetch: expected 2 rows, got {}",
        std_rows.len()
    );
    let v0: Vec<f32> = std_rows[0].get(0)?;
    ensure!(
        v0 == vec![1.5f32, 2.5, 3.5],
        "standard VECTOR decode row 0: expected [1.5, 2.5, 3.5], got {v0:?}"
    );

    drop_table_if_exists(conn, VECTOR_TABLE);
    pass(
        "VECTOR FixedSizeList",
        &format!("version={major}c: 2 dense VECTORs fetched as FixedSizeList(Float32, 3), values verified"),
    );
    Ok(())
}

// --- async-surface differentiators (LOB streaming, retry executor, SODA) ----
//
// These three capabilities live on the async `Connection` surface, so they run
// on a dedicated current-thread asupersync runtime with their own connection —
// the same shape the driver's own live integration tests use.

/// Lazy LOB streaming reader/writer + CLOB UTF-16 boundary (a4-bbx).
async fn check_lob_stream(conn: &mut Connection, cx: &Cx) -> Suite {
    use oracledb::protocol::thin::{LobValue, CS_FORM_IMPLICIT};
    use oracledb::{ClobReader, LobReader, LobWriter};

    section("lazy LOB streaming reader/writer + CLOB UTF-16 boundary (a4-bbx)");

    // BLOB: stream-write in 8 KiB chunks, stream-read back in 4 KiB chunks,
    // byte-identical across the many round trips.
    let payload: Vec<u8> = (0u32..64 * 1024)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();
    let temp = conn
        .create_temp_lob(cx, ORA_TYPE_NUM_BLOB, CS_FORM_IMPLICIT)
        .await
        .map_err(|e| format!("create temp BLOB: {e}"))?;
    let mut writer = LobWriter::new(temp.locator);
    for chunk in payload.chunks(8 * 1024) {
        writer
            .write_chunk(conn, cx, chunk)
            .await
            .map_err(|e| format!("LOB write_chunk: {e}"))?;
    }
    let blob_locator = writer.into_locator();
    let mut reader = LobReader::from_parts(blob_locator.clone(), payload.len() as u64, 4 * 1024);
    let back = reader
        .read_to_end(conn, cx)
        .await
        .map_err(|e| format!("LOB read_to_end: {e}"))?;
    ensure!(
        back == payload,
        "streamed BLOB must round-trip byte-identical: {} back vs {} written",
        back.len(),
        payload.len()
    );
    conn.free_temp_lobs(cx, &[blob_locator]).await.ok();

    // CLOB carrying astral (surrogate-pair) codepoints: Oracle measures CLOB
    // length in UTF-16 code units, and a tiny character chunk splits surrogate
    // pairs across reads — the reader must reassemble them.
    let text = "emoji 😀 party 🎉🎊 漢字 café ✓ end \u{10FFFF}";
    let temp = conn
        .create_temp_lob(cx, ORA_TYPE_NUM_CLOB, CS_FORM_IMPLICIT)
        .await
        .map_err(|e| format!("create temp CLOB: {e}"))?;
    let mut clob_locator = temp.locator;
    let encoded = encode_lob_text(text, CS_FORM_IMPLICIT, Some(&clob_locator));
    let written = conn
        .write_lob(cx, &clob_locator, 1, &encoded)
        .await
        .map_err(|e| format!("CLOB write_lob: {e}"))?;
    if !written.locator.is_empty() {
        clob_locator = written.locator;
    }
    let lob = LobValue {
        ora_type_num: ORA_TYPE_NUM_CLOB,
        csfrm: CS_FORM_IMPLICIT,
        locator: clob_locator.clone(),
        size: text.encode_utf16().count() as u64,
        chunk_size: 0,
    };
    let got = ClobReader::new(&lob, 3)
        .read_to_string(conn, cx)
        .await
        .map_err(|e| format!("CLOB stream read: {e}"))?;
    ensure!(
        got == text,
        "streamed CLOB must decode identically across surrogate-splitting chunks"
    );
    conn.free_temp_lobs(cx, &[clob_locator]).await.ok();

    pass(
        "LOB streaming",
        "64 KiB BLOB chunked round-trip + astral CLOB via UTF-16-aware ClobReader",
    );
    Ok(())
}

/// Idempotency-gated retry executor over the live ORA taxonomy (a4-r9a).
///
/// Proven deterministically against a REAL server error: a row locked by a
/// second session makes `SELECT ... FOR UPDATE NOWAIT` fail with ORA-00054
/// (which the taxonomy classifies retry-same-connection). We assert (a) the
/// executor runs an idempotent op through to success, (b) the idempotency gate
/// surfaces a real retryable failure without replay when the op is
/// non-idempotent, and (c) an idempotent op is genuinely re-run up to the
/// budget on that same real retryable error.
//
// The retry executor's op is an `FnMut` factory whose future must borrow the
// connection each attempt, so the connection lives behind a `RefCell` and the
// borrow is held across the `await` inside the op — exactly the pattern the
// driver's own `tests/live_retry.rs` executor uses. The borrow is single-
// threaded and re-entrancy-free (the op future is the only thing running), so
// the `await_holding_refcell_ref` lint is not a real hazard here.
#[allow(clippy::await_holding_refcell_ref)]
async fn check_retry_executor(
    conn: &mut Connection,
    cx: &Cx,
    cs: &str,
    user: &str,
    password: &str,
) -> Suite {
    use std::cell::{Cell, RefCell};

    section("idempotency-gated retry executor over the live ORA taxonomy (a4-r9a)");

    // Fixture row, then hold an exclusive lock on it from a SECOND session.
    conn.execute(cx, &format!("drop table {RETRY_TABLE} purge"), ())
        .await
        .ok();
    conn.execute(
        cx,
        &format!("create table {RETRY_TABLE} (id number(5))"),
        (),
    )
    .await
    .map_err(|e| format!("create retry fixture: {e}"))?;
    conn.execute(cx, &format!("insert into {RETRY_TABLE} values (1)"), ())
        .await
        .map_err(|e| format!("seed retry fixture: {e}"))?;
    conn.commit(cx)
        .await
        .map_err(|e| format!("commit seed: {e}"))?;

    let mut locker = Connection::connect(
        cx,
        ConnectOptions::new(
            cs.to_string(),
            user.to_string(),
            password.to_string(),
            identity()?,
        ),
    )
    .await
    .map_err(|e| format!("locker connect: {e}"))?;
    locker
        .query_one(
            cx,
            &format!("select id from {RETRY_TABLE} where id = 1 for update"),
            (),
        )
        .await
        .map_err(|e| format!("locker lock: {e}"))?;

    let cell = RefCell::new(conn);
    let contended = format!("select id from {RETRY_TABLE} where id = 1 for update nowait");

    // (a) happy path: an idempotent op runs through the executor exactly once.
    {
        let calls = Cell::new(0usize);
        let out: oracledb::Result<i64> = run_with_retry(
            cx,
            &RetryPolicy::default(),
            Idempotency::Idempotent,
            || async {
                calls.set(calls.get() + 1);
                let mut g = cell.borrow_mut();
                g.query_one(cx, "select 7 from dual", ())
                    .await?
                    .get::<i64>(0)
            },
        )
        .await;
        ensure!(
            out.map_err(|e| format!("retry happy path: {e}"))? == 7,
            "retry executor must return the idempotent op's real value"
        );
        ensure!(
            calls.get() == 1,
            "a succeeding op must run exactly once, ran {}",
            calls.get()
        );
    }

    // (b) idempotency gate: a REAL retryable failure (ORA-00054) is surfaced
    // WITHOUT replay when the operation is non-idempotent.
    {
        let calls = Cell::new(0usize);
        let out: oracledb::Result<i64> = run_with_retry(
            cx,
            &RetryPolicy::default(),
            Idempotency::NonIdempotent,
            || async {
                calls.set(calls.get() + 1);
                let mut g = cell.borrow_mut();
                g.query_one(cx, &contended, ()).await?.get::<i64>(0)
            },
        )
        .await;
        let err = out
            .err()
            .ok_or("non-idempotent contended select must fail")?;
        ensure!(
            err.ora_code() == Some(54),
            "gate must surface the real ORA-00054, got: {err}"
        );
        ensure!(
            calls.get() == 1,
            "the idempotency gate must NOT replay a non-idempotent op (ran {})",
            calls.get()
        );
    }

    // (c) idempotent retry loop: the SAME real ORA-00054 is classified
    // retryable, so an idempotent op is re-run up to the budget (1 initial + 2
    // retries) before surfacing.
    {
        let calls = Cell::new(0usize);
        let policy = RetryPolicy {
            max_retries: 2,
            base_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        };
        let out: oracledb::Result<i64> =
            run_with_retry(cx, &policy, Idempotency::Idempotent, || async {
                calls.set(calls.get() + 1);
                let mut g = cell.borrow_mut();
                g.query_one(cx, &contended, ()).await?.get::<i64>(0)
            })
            .await;
        let err = out
            .err()
            .ok_or("still-contended select must fail after the budget")?;
        ensure!(
            err.ora_code() == Some(54),
            "loop must surface the real ORA-00054 after exhausting the budget, got: {err}"
        );
        ensure!(
            calls.get() == 3,
            "idempotent op must be retried to the budget (1 + 2 = 3 runs), ran {}",
            calls.get()
        );
    }

    // Release the lock and drop the fixture.
    locker.rollback(cx).await.ok();
    locker.close(cx).await.ok();
    let conn = cell.into_inner();
    conn.execute(cx, &format!("drop table {RETRY_TABLE} purge"), ())
        .await
        .ok();

    pass(
        "retry executor",
        "idempotent success (1 run); ORA-00054 gate surfaces non-idempotent (1 run); \
         idempotent retries to budget (3 runs)",
    );
    Ok(())
}

/// Thin-mode SODA: gated on pre-21c, works on 21c+ (a4-h74 / a4-soda-pre21c).
///
/// `JSON_SERIALIZE` is the exact SQL primitive the thin SODA write path depends
/// on and is the real 21c boundary; it is asserted on every lane (privilege
/// free). On 21c+ where the connecting user holds SODA_APP, a real
/// create/insert/get/drop round trip additionally value-asserts the stored
/// document content.
#[cfg(feature = "soda")]
async fn check_soda_version_gate(conn: &mut Connection, cx: &Cx, major: u8) -> Suite {
    use oracledb::protocol::oson::OsonValue;
    use oracledb::soda::{SodaDatabase, SodaDocument, SodaError, SodaOperation};

    fn oson_name(doc: &SodaDocument) -> Option<String> {
        match doc.content_as_oson()? {
            OsonValue::Object(entries) => {
                entries
                    .iter()
                    .find(|(k, _)| k == "name")
                    .and_then(|(_, v)| match v {
                        OsonValue::String(s) => Some(s.clone()),
                        _ => None,
                    })
            }
            _ => None,
        }
    }

    section("thin-mode SODA: gated <21c, works 21c+ (a4-h74 / a4-soda-pre21c)");

    let probe = conn
        .query_one(
            cx,
            "select json_serialize('{\"a\":1}' returning varchar2) from dual",
            (),
        )
        .await;

    let db = SodaDatabase::new();
    db.drop_collection(conn, cx, "MatrixFullSoda").await.ok();
    conn.commit(cx).await.ok();
    let create = db
        .create_collection(conn, cx, Some("MatrixFullSoda"), None, false)
        .await;

    if major < 21 {
        ensure!(probe.is_err(), "JSON_SERIALIZE must NOT resolve before 21c");
        let err = match create {
            Err(err) => err,
            Ok(_) => return Err("SODA create must fail before 21c".into()),
        };
        match &err {
            SodaError::Driver(driver) => ensure!(
                driver.ora_code() == Some(904),
                "pre-21c SODA create must fail with ORA-00904 (JSON_SERIALIZE invalid \
                 identifier), got: {driver}"
            ),
            other => {
                return Err(
                    format!("pre-21c SODA error must carry an ORA code, got: {other:?}").into(),
                )
            }
        }
        pass(
            "SODA gate",
            &format!(
                "version={major}c GATED: JSON_SERIALIZE absent, create_collection -> ORA-00904"
            ),
        );
        return Ok(());
    }

    // 21c+: the version-enabling primitive must resolve.
    probe.map_err(|e| format!("JSON_SERIALIZE must resolve on 21c+: {e}"))?;

    // If the connecting user has SODA_APP, prove the full round trip; otherwise
    // the JSON_SERIALIZE boundary above is the version proof and the full
    // surface is covered by the `versions` live_soda suite (which bootstraps
    // SODA_APP). Report honestly which path ran.
    let soda_app: i64 = conn
        .query_one(
            cx,
            "select count(*) from session_roles where role = 'SODA_APP'",
            (),
        )
        .await
        .and_then(|r| r.get::<i64>(0))
        .unwrap_or(0);
    if soda_app == 0 {
        // create may legitimately fail on a user without SODA_APP; do not treat
        // a privilege gap as a version failure.
        create.ok();
        pass(
            "SODA",
            &format!(
                "version={major}c: JSON_SERIALIZE available (21c+ boundary); SODA_APP not granted \
                 to this user — full round trip covered by `versions`/live_soda"
            ),
        );
        return Ok(());
    }

    let coll = create.map_err(|e| format!("SODA create on 21c+ with SODA_APP: {e}"))?;
    let stored = coll
        .insert_one(
            conn,
            cx,
            &SodaDocument::from_bytes(br#"{"name":"George","age":47}"#.to_vec(), None, None),
            None,
            true,
        )
        .await
        .map_err(|e| format!("SODA insert_one: {e}"))?
        .ok_or("insert_one must return the stored document")?;
    let key = stored
        .key
        .clone()
        .ok_or("stored document must carry a key")?;
    let op = SodaOperation {
        key: Some(key),
        ..Default::default()
    };
    let found = coll
        .get_one(conn, cx, &op)
        .await
        .map_err(|e| format!("SODA get_one: {e}"))?
        .ok_or("get_one by key must find the stored document")?;
    let name = oson_name(&found).ok_or("stored document must expose a string 'name' field")?;
    ensure!(
        name == "George",
        "SODA readback name: expected George, got {name:?}"
    );
    db.drop_collection(conn, cx, "MatrixFullSoda").await.ok();
    conn.commit(cx).await.ok();

    pass(
        "SODA",
        &format!("version={major}c: create/insert/get_one/drop round trip, content verified"),
    );
    Ok(())
}

/// Drive the async-surface differentiators on a dedicated runtime + connection.
fn run_async_differentiators(connect_string: &str, user: &str, password: &str, major: u8) -> Suite {
    let reactor = reactor::create_reactor().map_err(|e| format!("create reactor: {e}"))?;
    let runtime = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .map_err(|e| format!("build runtime: {e}"))?;
    let cs = connect_string.to_string();
    let user = user.to_string();
    let password = password.to_string();
    runtime.block_on(async move {
        let cx = Cx::current().ok_or("no ambient Cx in runtime")?;
        let mut conn = Connection::connect(
            &cx,
            ConnectOptions::new(cs.clone(), user.clone(), password.clone(), identity()?),
        )
        .await
        .map_err(|e| format!("async connect: {e}"))?;
        let result: Suite = async {
            check_lob_stream(&mut conn, &cx).await?;
            check_retry_executor(&mut conn, &cx, &cs, &user, &password).await?;
            #[cfg(feature = "soda")]
            check_soda_version_gate(&mut conn, &cx, major).await?;
            #[cfg(not(feature = "soda"))]
            {
                let _ = major;
                eprintln!(
                    "[matrix-full] SODA section compiled out (build with --features soda to \
                     assert the 21c gate)"
                );
            }
            Ok(())
        }
        .await;
        conn.close(&cx).await.ok();
        result
    })
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

    // Negotiated server major version, used to gate version-specific
    // differentiators (SODA at 21c+, VECTOR at 23ai).
    let major = conn.server_version_tuple().map(|(m, ..)| m).unwrap_or(0);

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
        // 0.7.3 differentiator value-asserts (blocking surface).
        check_pipeline_single_round_trip(&mut conn, major)?;
        check_batch_errors(&mut conn)?;
        check_call_timeout(&mut conn)?;
        check_shape_cache_self_heal(&mut conn, &options)?;
        #[cfg(feature = "arrow")]
        check_vector_fixed_size_list(&mut conn, major)?;
        #[cfg(not(feature = "arrow"))]
        eprintln!(
            "[matrix-full] VECTOR/Arrow section compiled out (build with --features arrow \
             to assert the 23ai FixedSizeList fast path)"
        );
        Ok(())
    })();

    // Best-effort cleanup either way; the verdict is `result`.
    drop_table_if_exists(&mut conn, DML_TABLE);
    drop_table_if_exists(&mut conn, LOB_TABLE);
    let _ = BlockingConnection::close(conn);
    result?;

    // 0.7.3 differentiator value-asserts on the async surface (own runtime +
    // connection): LOB streaming, the retry executor, and the SODA version gate.
    run_async_differentiators(connect_string, user, password, major)?;

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
