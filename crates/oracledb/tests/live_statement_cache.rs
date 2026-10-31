// Assertion-heavy test code intentionally panics on invariant violations.
#![allow(clippy::unwrap_used)]

//! Live test for configurable statement-cache size (bead 5ah, oraclemcp #10).
//! Exercises the eviction path (tiny cache) and the disabled path (size 0)
//! end to end: many distinct statements must keep working as cursors are evicted
//! and closed via the piggyback queue, and a disabled cache must not retain or
//! reuse anything.
//!
//! Run: PYO_TEST_CONNECT_STRING=localhost:1522/FREEPDB1 PYO_TEST_MAIN_USER=pythontest \
//!      PYO_TEST_MAIN_PASSWORD=pythontest \
//!      cargo test -p oracledb --test live_statement_cache -- --ignored --nocapture
extern crate oraclemcp_driver_cx as oracledb;

use oracledb::protocol::thin::{QueryResult, QueryValue};
use oracledb::protocol::ClientIdentity;
use oracledb::{BlockingConnection, ConnectOptions};

mod common;

fn options() -> ConnectOptions {
    let common::LiveCreds {
        connect_string: cs,
        user,
        password: pw,
    } = common::live_creds_required();
    let id = ClientIdentity::new("stmtcache", "host", "user", "term", "rust").unwrap();
    ConnectOptions::new(cs, user, pw, id)
}

fn execute_raw(
    conn: &mut oracledb::Connection,
    sql: &str,
    prefetch_rows: u32,
) -> oracledb::Result<QueryResult> {
    BlockingConnection::execute_raw(
        conn,
        sql,
        prefetch_rows,
        &[],
        oracledb::protocol::thin::ExecuteOptions::default(),
        None,
    )
}

/// Oracle's beta.4/beta.5 NUMBER bugs can survive a bind/fetch round trip when
/// both ends share the same defect. Compare the server's bytes and equality to
/// a SQL literal, as well as to an independent python-oracledb Decimal bind.
#[test]
#[ignore]
fn upstream_number_bind_bytes_match_python_decimal_and_oracle_literals() {
    use oracledb::protocol::thin::{BindValue, ExecuteOptions};
    let numbers = [
        "0", "0.5", "0.05", "0.005", "0.0005", "0.00005", "0.0123", "0.000123", "1E-4", "1E-130",
        "-0.05", "-0.005", "101", "100", "2500", "-100", "10000", "123.4500", "0.0500", "-2500.00",
    ];
    let reference = std::process::Command::new("python3")
        .args(["-c", r#"
import decimal, os, sys, oracledb
with oracledb.connect(user=os.environ['PYO_TEST_MAIN_USER'],
    password=os.environ['PYO_TEST_MAIN_PASSWORD'],
    dsn=os.environ['PYO_TEST_CONNECT_STRING']) as conn:
    with conn.cursor() as cur:
        for text in sys.argv[1:]:
            sql = f"SELECT DUMP(:1,16), DUMP({text},16), CASE WHEN :2={text} THEN 1 ELSE 0 END, TO_CHAR(:3,'TM9','NLS_NUMERIC_CHARACTERS=''.,''') FROM SYS.DUAL"
            value = decimal.Decimal(text)
            row = cur.execute(sql, [value, value, value]).fetchone()
            assert row[0] == row[1] and row[2] == 1, (text, row)
            print(text, *row, sep='\t')
"#])
        .args(numbers)
        .output().expect("python-oracledb reference must be installed");
    assert!(
        reference.status.success(),
        "python reference: {}",
        String::from_utf8_lossy(&reference.stderr)
    );
    let output = String::from_utf8(reference.stdout).unwrap();
    let rows: Vec<_> = output.lines().collect();
    assert_eq!(rows.len(), numbers.len());
    let mut conn = BlockingConnection::connect(options()).unwrap();
    for (number, reference) in numbers.into_iter().zip(rows) {
        let expected: Vec<_> = reference.split('\t').collect();
        assert_eq!(expected.len(), 5);
        assert_eq!(expected[0], number);
        let sql = format!(
            "SELECT DUMP(:1,16), DUMP({number},16), \
            CASE WHEN :2={number} THEN 1 ELSE 0 END, \
            TO_CHAR(:3,'TM9','NLS_NUMERIC_CHARACTERS=''.,''') FROM SYS.DUAL"
        );
        let result = BlockingConnection::execute_raw(
            &mut conn,
            &sql,
            2,
            &[vec![BindValue::Number(number.into()); 3]],
            ExecuteOptions::default(),
            Some(3000),
        )
        .unwrap_or_else(|err| panic!("NUMBER {number}: {err}"));
        assert_eq!(result.rows.len(), 1);
        for col in [0, 1, 3] {
            assert_eq!(
                result.cell(0, col).and_then(QueryValue::as_text),
                Some(expected[col + 1]),
                "NUMBER {number}, column {col}"
            );
        }
        assert_eq!(
            result.cell(0, 2).and_then(QueryValue::as_i64),
            Some(1),
            "NUMBER {number}"
        );
    }
    BlockingConnection::close(conn).unwrap();
}

#[test]
#[ignore]
fn upstream_repeated_and_null_columns_survive_prefetch_and_fetch_boundaries() {
    let mut conn = BlockingConnection::connect(options()).unwrap();
    for prefetch in [2, 3, 8, 50] {
        let mut batch = execute_raw(
            &mut conn,
            "SELECT 'repeat', CAST(NULL AS VARCHAR2(20)), level \
             FROM SYS.DUAL CONNECT BY level <= 43 ORDER BY level",
            prefetch,
        )
        .unwrap();
        assert_eq!(batch.rows.len(), usize::min(prefetch as usize, 43));
        let mut seen = 0;
        loop {
            for row in &batch.rows {
                seen += 1;
                assert_eq!(
                    row[0].as_ref().and_then(QueryValue::as_text),
                    Some("repeat")
                );
                assert_eq!(row[1], None);
                assert_eq!(row[2].as_ref().and_then(QueryValue::as_i64), Some(seen));
            }
            if !batch.more_rows {
                break;
            }
            assert!(!batch.rows.is_empty(), "fetch must make progress");
            batch = BlockingConnection::fetch_rows(
                &mut conn,
                batch.cursor_id,
                7,
                batch.rows.last().map(Vec::as_slice),
            )
            .unwrap();
        }
        assert_eq!(seen, 43, "prefetch {prefetch}");
    }
    BlockingConnection::close(conn).unwrap();
}

fn fixture_name(suffix: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    format!(
        "VOP_{suffix}_{:X}_{:X}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros()
    )
}

#[test]
#[ignore]
fn upstream_description_failover_authenticates_to_the_selected_service() {
    let creds = common::live_creds_required();
    let endpoint = oracledb::protocol::net::EasyConnect::parse(&creds.connect_string).unwrap();
    assert_eq!(endpoint.protocol, oracledb::protocol::net::Protocol::Tcp);
    let unavailable = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = unavailable.local_addr().unwrap().port();
    drop(unavailable);
    let dsn = format!(
        "(DESCRIPTION_LIST=(LOAD_BALANCE=OFF)\
        (DESCRIPTION=(CONNECT_TIMEOUT=3)(SDU=4096)\
        (ADDRESS=(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT={port}))\
        (CONNECT_DATA=(SERVICE_NAME=VOP_NOT_A_SERVICE)))\
        (DESCRIPTION=(SDU=16384)\
        (ADDRESS=(PROTOCOL=tcp)(HOST={})(PORT={}))\
        (CONNECT_DATA=(SERVICE_NAME={}))))",
        endpoint.host, endpoint.port, endpoint.service_name
    );
    let identity = ClientIdentity::new("vop-failover", "host", "user", "term", "rust").unwrap();
    let mut conn = BlockingConnection::connect(ConnectOptions::new(
        dsn,
        creds.user,
        creds.password,
        identity,
    ))
    .unwrap();
    let value: i64 = BlockingConnection::query_one(&mut conn, "SELECT 42 FROM SYS.DUAL", ())
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(value, 42);
    BlockingConnection::close(conn).unwrap();
}

#[test]
#[ignore]
fn upstream_failed_parse_is_reparsed_after_the_missing_object_is_created() {
    let mut conn = BlockingConnection::connect(options()).unwrap();
    let table = fixture_name("PARSE");
    let sql = format!("SELECT n FROM {table}");
    let err = execute_raw(&mut conn, &sql, 2).unwrap_err();
    assert_eq!(err.ora_code(), Some(942));
    BlockingConnection::execute(&mut conn, &format!("CREATE TABLE {table} (n NUMBER)"), ())
        .unwrap();
    BlockingConnection::execute(&mut conn, &format!("INSERT INTO {table} VALUES (42)"), ())
        .unwrap();
    let result = execute_raw(&mut conn, &sql, 2).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.cell(0, 0).and_then(QueryValue::as_i64), Some(42));
    BlockingConnection::rollback(&mut conn).unwrap();
    BlockingConnection::execute(&mut conn, &format!("DROP TABLE {table} PURGE"), ()).unwrap();
    BlockingConnection::close(conn).unwrap();
}

#[test]
#[ignore]
fn upstream_leading_line_comments_preserve_query_dml_and_plsql_execution() {
    let mut conn = BlockingConnection::connect(options()).unwrap();
    let table = fixture_name("COMMENT");
    BlockingConnection::execute(&mut conn, &format!("CREATE TABLE {table} (n NUMBER)"), ())
        .unwrap();
    for prefix in [
        "-- c\n",
        "--c\n",
        "\n-- c\n",
        "-- one\r\n-- two\n",
        "/* block */\n--line\n",
    ] {
        let result =
            execute_raw(&mut conn, &format!("{prefix}SELECT 42 FROM SYS.DUAL"), 2).unwrap();
        assert_eq!(result.cell(0, 0).and_then(QueryValue::as_i64), Some(42));
        BlockingConnection::execute(
            &mut conn,
            &format!("{prefix}INSERT INTO {table} VALUES (1)"),
            (),
        )
        .unwrap();
        BlockingConnection::execute(
            &mut conn,
            &format!("{prefix}BEGIN INSERT INTO {table} VALUES (2); END;"),
            (),
        )
        .unwrap();
    }
    let count: i64 =
        BlockingConnection::query_one(&mut conn, &format!("SELECT COUNT(*) FROM {table}"), ())
            .unwrap()
            .get(0)
            .unwrap();
    assert_eq!(count, 10);
    BlockingConnection::rollback(&mut conn).unwrap();
    BlockingConnection::execute(&mut conn, &format!("DROP TABLE {table} PURGE"), ()).unwrap();
    BlockingConnection::close(conn).unwrap();
}

#[test]
#[ignore]
fn upstream_returning_trigger_error_returns_oracle_error_and_keeps_session_usable() {
    use oracledb::protocol::thin::{BindValue, ExecuteOptions, ORA_TYPE_NUM_NUMBER};
    let mut conn = BlockingConnection::connect(options()).unwrap();
    let table = fixture_name("RETURN");
    let trigger = fixture_name("TRIGGER");
    BlockingConnection::execute(&mut conn, &format!("CREATE TABLE {table} (n NUMBER)"), ())
        .unwrap();
    BlockingConnection::execute(
        &mut conn,
        &format!(
            "CREATE TRIGGER {trigger} BEFORE INSERT ON {table} \
        BEGIN RAISE_APPLICATION_ERROR(-20001, 'VOP returning regression'); END;"
        ),
        (),
    )
    .unwrap();
    let sql = format!("INSERT INTO {table} VALUES (42) RETURNING n INTO :1");
    let binds = [vec![BindValue::ReturnOutput {
        ora_type_num: ORA_TYPE_NUM_NUMBER,
        csfrm: 0,
        buffer_size: 22,
    }]];
    let err = BlockingConnection::execute_raw(
        &mut conn,
        &sql,
        0,
        &binds,
        ExecuteOptions::default(),
        Some(3000),
    )
    .unwrap_err();
    // A client timeout would bound a hang but would not prove the server error
    // was decoded or that the response boundary was drained.
    assert_eq!(err.ora_code(), Some(20001), "{err}");
    assert!(!conn.is_dead());
    let count: i64 =
        BlockingConnection::query_one(&mut conn, &format!("SELECT COUNT(*) FROM {table}"), ())
            .unwrap()
            .get(0)
            .unwrap();
    assert_eq!(count, 0);
    BlockingConnection::execute(&mut conn, &format!("ALTER TRIGGER {trigger} DISABLE"), ())
        .unwrap();
    let result = BlockingConnection::execute_raw(
        &mut conn,
        &sql,
        0,
        &binds,
        ExecuteOptions::default(),
        Some(3000),
    )
    .unwrap();
    assert_eq!(result.row_count, 1);
    assert_eq!(result.return_values.len(), 1);
    assert_eq!(result.return_values[0].0, 0);
    assert_eq!(result.return_values[0].1.len(), 1);
    assert_eq!(
        result.return_values[0].1[0]
            .as_ref()
            .and_then(QueryValue::as_i64),
        Some(42)
    );
    BlockingConnection::rollback(&mut conn).unwrap();
    BlockingConnection::execute(&mut conn, &format!("DROP TABLE {table} PURGE"), ()).unwrap();
    BlockingConnection::close(conn).unwrap();
}

fn run_n_distinct(conn: &mut oracledb::Connection, n: i64) {
    for i in 1..=n {
        // Each statement is distinct SQL text, so each takes a cache slot.
        let sql = format!("select {i} + level from dual connect by level <= 2");
        let r = execute_raw(conn, &sql, 10).unwrap();
        let first = r.cell(0, 0).and_then(QueryValue::as_i64).unwrap();
        assert_eq!(first, i + 1, "row 0 of stmt {i}");
        assert_eq!(r.rows.len(), 2);
    }
}

#[test]
#[ignore]
fn tiny_cache_evicts_and_keeps_working() {
    // Cache of 3, but 30 distinct statements -> heavy eviction + cursor close
    // piggybacking. Every statement must still execute correctly.
    let mut c = BlockingConnection::connect(options().with_statement_cache_size(3)).unwrap();
    run_n_distinct(&mut c, 30);
    // Re-run an earlier statement (long evicted) — must re-prepare cleanly.
    let r = execute_raw(
        &mut c,
        "select 1 + level from dual connect by level <= 2",
        10,
    )
    .unwrap();
    assert_eq!(r.cell(0, 0).and_then(QueryValue::as_i64), Some(2));
    BlockingConnection::close(c).ok();
}

#[test]
#[ignore]
fn disabled_cache_size_zero_keeps_working() {
    // Caching disabled: each statement's cursor is closed after use, never
    // retained. Repeated execution of the same SQL must still work.
    let mut c = BlockingConnection::connect(options().with_statement_cache_size(0)).unwrap();
    for _ in 0..10 {
        let r = execute_raw(&mut c, "select 42 from dual", 1).unwrap();
        assert_eq!(r.cell(0, 0).and_then(QueryValue::as_i64), Some(42));
    }
    run_n_distinct(&mut c, 10);
    BlockingConnection::close(c).ok();
}

/// With caching disabled (size 0) the executed cursor is queued for close, yet a
/// query whose result spans multiple fetch batches must still drain correctly:
/// the close-cursors piggyback only rides the next *execute*, never a `fetch`, so
/// the still-open cursor survives until the caller is done with it. (Guards the
/// size-0 disable path against closing an in-use cursor mid-fetch.)
#[test]
#[ignore]
fn disabled_cache_multibatch_fetch_survives() {
    let mut c = BlockingConnection::connect(options().with_statement_cache_size(0)).unwrap();
    // prefetch 5 over 100 rows -> first batch leaves more_rows set, cursor open.
    let first = execute_raw(&mut c, "select level from dual connect by level <= 100", 5).unwrap();
    assert!(
        first.more_rows,
        "expected an unfinished cursor for the test"
    );
    assert_ne!(first.cursor_id, 0);
    let prev = first.rows.last().cloned();
    let rest =
        BlockingConnection::fetch_rows(&mut c, first.cursor_id, 100, prev.as_deref()).unwrap();
    assert_eq!(
        first.rows.len() + rest.rows.len(),
        100,
        "all rows fetched despite size-0 caching"
    );
    BlockingConnection::close(c).ok();
}

/// Regression for bead rust-oracledb-ilel: re-executing a CACHED statement with
/// a bind of a DIFFERENT type must re-describe the statement instead of letting
/// the server coerce through the stale cached bind metadata (ORA-01722 when a
/// text bind hits a cursor parsed with a NUMBER bind). The reference driver
/// tracks the bound metadata per statement and falls back to a full re-parse
/// when the shape changes (thin/statement.pyx `_set_var` -> `_binds_changed`).
/// One SQL text, NUMBER -> TEXT -> RAW -> NUMBER, all on the same connection.
#[test]
#[ignore]
fn rebind_type_change_on_cached_statement_redescribes() {
    let mut c = BlockingConnection::connect(options()).unwrap();
    let sql = "select :1 from dual";
    // 1) NUMBER: parses the statement and caches its cursor with NUMBER
    //    bind metadata.
    let n: i64 = BlockingConnection::query_one(&mut c, sql, (42i64,))
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(n, 42);
    // 2) TEXT on the SAME cached SQL: without re-describe the server converts
    //    the text through the stale NUMBER bind metadata -> ORA-01722.
    let unicode = "naïve-Ω-δοκιμή-支持-🎯";
    let t: String = BlockingConnection::query_one(&mut c, sql, (unicode,))
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(t, unicode);
    // 3) RAW: a third distinct bind type on the same cached statement.
    let raw_in: Vec<u8> = vec![0xde, 0xad, 0xbe, 0xef, 0x00, 0x7f];
    let raw_out: Vec<u8> = BlockingConnection::query_one(&mut c, sql, (raw_in.clone(),))
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(raw_out, raw_in);
    // 4) Back to NUMBER: the cache entry must still be usable after the
    //    type churn.
    let n2: i64 = BlockingConnection::query_one(&mut c, sql, (7i64,))
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(n2, 7);
    BlockingConnection::close(c).ok();
}

/// Requires an administrator in PYO_TEST_MAIN_USER. The observed application
/// user receives only CREATE SESSION; its cursor count is read by the separate
/// administrator so sampling cannot change the application session's cursors.
#[test]
#[ignore]
fn failed_parses_retire_returned_cursors_on_the_same_session(
) -> Result<(), Box<dyn std::error::Error>> {
    use std::time::{SystemTime, UNIX_EPOCH};

    fn cycle(c: &mut oracledb::Connection, index: usize) -> Result<(), Box<dyn std::error::Error>> {
        // Eight SQL texts exceed the two-entry cache and force close piggybacks.
        let value = (index % 8) as i64;
        let actual: i64 =
            BlockingConnection::query_one(c, &format!("select {value} from SYS.DUAL"), ())?
                .get(0)?;
        if actual != value {
            return Err(format!("successful query returned {actual}, expected {value}").into());
        }
        let missing = BlockingConnection::query_one(c, "select * from QXP_MISSING_TABLE", ())
            .err()
            .ok_or("missing-table parse unexpectedly succeeded")?;
        let denied = BlockingConnection::execute(c, "create table QXP_DENIED_TABLE (n number)", ())
            .err()
            .ok_or("CREATE TABLE unexpectedly succeeded for CREATE SESSION-only user")?;
        if missing.ora_code() != Some(942) || denied.ora_code() != Some(1031) {
            return Err(format!("expected ORA-00942/01031, got {missing}; {denied}").into());
        }
        Ok(())
    }

    fn opened_cursors(
        admin: &mut oracledb::Connection,
        sid: u32,
        serial: u16,
        user: &str,
    ) -> oracledb::Result<i64> {
        BlockingConnection::query_one(
            admin,
            "select st.value from SYS.V_$SESSTAT st \
             join SYS.V_$STATNAME n on n.statistic# = st.statistic# \
             join SYS.V_$SESSION s on s.sid = st.sid \
             where s.sid = :1 and s.serial# = :2 and s.username = :3 \
             and n.name = 'opened cursors current'",
            (i64::from(sid), i64::from(serial), user),
        )?
        .get(0)
    }

    let creds = common::live_creds_required();
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros();
    let user = format!("QXP{:X}{nonce:X}", std::process::id());
    let password = format!("Qxp9_{nonce:X}");
    let mut admin = BlockingConnection::connect(options())?;
    BlockingConnection::execute(
        &mut admin,
        &format!("create user {user} identified by \"{password}\""),
        (),
    )?;
    let result = (|| -> Result<_, Box<dyn std::error::Error>> {
        BlockingConnection::execute(&mut admin, &format!("grant create session to {user}"), ())?;
        let mut observations = Vec::new();
        for cache_size in [0, 2] {
            let identity =
                ClientIdentity::new("failed-parse-custody", "host", "user", "term", "rust")?;
            let mut app = BlockingConnection::connect(
                ConnectOptions::new(&creds.connect_string, &user, &password, identity)
                    .with_statement_cache_size(cache_size),
            )?;
            let sid = app.session_id();
            let serial = app.serial_num();
            let measured = (|| -> Result<_, Box<dyn std::error::Error>> {
                // Warm two complete SQL-text cycles. Each sample is taken at
                // the identical boundary: the final failed cursor awaits its
                // next execute's close piggyback. No tolerance is added.
                for index in 0..16 {
                    cycle(&mut app, index)?;
                }
                let baseline = opened_cursors(&mut admin, sid, serial, &user)?;
                let mut peak = baseline;
                for index in 16..80 {
                    cycle(&mut app, index)?;
                    peak = peak.max(opened_cursors(&mut admin, sid, serial, &user)?);
                }
                if (app.session_id(), app.serial_num()) != (sid, serial) {
                    return Err("application session changed during failed parses".into());
                }
                Ok((cache_size, sid, serial, baseline, peak))
            })();
            let closed = BlockingConnection::close(app);
            observations.push(measured?);
            closed?;
        }
        Ok(observations)
    })();
    // Always remove the owned disposable user, including on a driver error.
    let dropped = BlockingConnection::execute(&mut admin, &format!("drop user {user} cascade"), ());
    let closed = BlockingConnection::close(admin);
    dropped?;
    closed?;
    let observations = result?;
    for (cache_size, sid, serial, baseline, peak) in &observations {
        eprintln!("cache={cache_size} sid={sid} serial={serial} opened cursors current: baseline={baseline}, peak={peak}");
    }
    for (cache_size, _, _, baseline, peak) in observations {
        assert!(peak <= baseline, "failed parses leaked cursors with cache={cache_size}: baseline={baseline}, peak={peak}");
    }
    Ok(())
}
