//! Statement-suite ground-truth emitter (bead rust-oracledb-rwoh).
//!
//! Runs the FIXED statement corpus against a live lane and prints one
//! canonical JSON document to stdout. `scripts/statement_ground_truth.py` is
//! the python-oracledb twin: it runs the IDENTICAL corpus (same case ids, same
//! SQL text, same binds) and emits the same document shape, so the two outputs
//! can be diffed field-by-field (`scripts/statement_ground_truth.py --diff`).
//! Any mismatch is a driver bug (or a documented representational difference)
//! — never noise, because every cell is encoded canonically:
//!
//! * `null`                       — SQL NULL
//! * `s:<text>`                   — character data (VARCHAR2/NVARCHAR2/CHAR/CLOB)
//! * `n:<decimal>`                — NUMBER, compared as exact decimals
//! * `d:<16-hex>`                 — BINARY_DOUBLE/BINARY_FLOAT as IEEE-754 f64 bits
//! * `r:<hex>`                    — RAW / BLOB bytes
//! * `dt:YYYY-MM-DDTHH:MM:SS.ffffff`         — DATE / TIMESTAMP, and
//!   TIMESTAMP WITH TIME ZONE rendered the way python-oracledb returns it: a
//!   naive **wall-clock** datetime (the wire carries UTC + offset; the
//!   reference `convert_date_to_python` adds the offset and drops the tz)
//! * `b:true|false`               — BOOLEAN
//! * `rid:<rowid>`                — ROWID
//!
//! Error cases are `{"ok": false, "error": "ORA-NNNNN"}` (first ORA code).
//!
//! Usage: `statement_ground_truth [CONNECT_STRING] [USER] [PASSWORD]`, with
//! `PYO_TEST_CONNECT_STRING` / `PYO_TEST_MAIN_USER` / `PYO_TEST_MAIN_PASSWORD`
//! fallbacks (same convention as `smoke.rs` / `matrix_full.rs`).

use std::fmt::Write as _;
use std::process::ExitCode;

use oracledb::protocol::thin::{
    decode_lob_text, BindValue, QueryValue, CS_FORM_IMPLICIT, ORA_TYPE_NUM_BLOB,
    ORA_TYPE_NUM_NUMBER, ORA_TYPE_NUM_VARCHAR,
};
use oracledb::protocol::ClientIdentity;
use oracledb::{BlockingConnection, ConnectOptions, Connection, Execute, Query};

/// Corpus schema version; bump when cases change so twins can refuse to diff
/// mismatched corpora.
const CORPUS_VERSION: u32 = 1;

const DML_TABLE: &str = "gt_truth_dml";
const LOB_TABLE: &str = "gt_truth_lob";

fn resolve(arg: Option<String>, env_key: &str, default: &str) -> String {
    arg.or_else(|| std::env::var(env_key).ok())
        .unwrap_or_else(|| default.to_string())
}

// ---------------------------------------------------------------------------
// canonical cell encoding
// ---------------------------------------------------------------------------

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Encode a cell, materializing LOB locators through the connection (the
/// python twin does the same via `oracledb.defaults.fetch_lobs = False`).
fn encode_cell_with_conn(
    conn: &mut Connection,
    value: Option<&QueryValue>,
) -> Result<String, String> {
    if let Some(QueryValue::Lob(lob)) = value {
        if lob.size == 0 {
            return Ok(if lob.ora_type_num == ORA_TYPE_NUM_BLOB {
                "r:".to_string()
            } else {
                "s:".to_string()
            });
        }
        let read = BlockingConnection::read_lob(conn, &lob.locator, 1, lob.size)
            .map_err(|e| format!("read_lob failed: {e}"))?;
        let data = read.data.unwrap_or_default();
        return Ok(if lob.ora_type_num == ORA_TYPE_NUM_BLOB {
            format!("r:{}", hex(&data))
        } else {
            let text = decode_lob_text(&data, lob.csfrm, Some(&lob.locator))
                .map_err(|e| format!("CLOB decode failed: {e}"))?;
            format!("s:{text}")
        });
    }
    encode_cell(value)
}

fn encode_cell(value: Option<&QueryValue>) -> Result<String, String> {
    let Some(value) = value else {
        return Ok("null".to_string());
    };
    Ok(match value {
        QueryValue::Text(text) => format!("s:{text}"),
        QueryValue::Raw(bytes) => format!("r:{}", hex(bytes)),
        QueryValue::Rowid(rowid) => format!("rid:{rowid}"),
        QueryValue::Number(num) => format!("n:{}", num.to_canonical_cow()),
        QueryValue::Boolean(b) => format!("b:{b}"),
        QueryValue::BinaryDouble(text) => {
            let parsed: f64 = text
                .parse()
                .map_err(|e| format!("BinaryDouble text {text:?} did not parse: {e}"))?;
            format!("d:{:016x}", parsed.to_bits())
        }
        QueryValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
        } => format!(
            "dt:{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{:06}",
            nanosecond / 1_000
        ),
        QueryValue::TimestampTz {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
            offset_minutes,
        } => {
            // The wire fields are UTC; python-oracledb returns the wall-clock
            // time (UTC + offset) as a naive datetime. Mirror that exactly.
            let (year, month, day, hour, minute, second) = add_minutes(
                *year,
                *month,
                *day,
                *hour,
                *minute,
                *second,
                *offset_minutes,
            );
            format!(
                "dt:{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{:06}",
                nanosecond / 1_000
            )
        }
        other => return Err(format!("unencodable QueryValue variant: {other:?}")),
    })
}

/// Add `offset_minutes` to a civil datetime (Howard Hinnant days-from-civil
/// round trip; mirrors the protocol crate's internal offset normalization).
fn add_minutes(
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    offset_minutes: i32,
) -> (i32, u8, u8, u8, u8, u8) {
    let y = i64::from(year) - i64::from(month <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (i64::from(month) + 9) % 12;
    let doy = (153 * mp + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let total = days * 86_400
        + i64::from(hour) * 3_600
        + i64::from(minute) * 60
        + i64::from(second)
        + i64::from(offset_minutes) * 60;
    let days = total.div_euclid(86_400);
    let secs = total.rem_euclid(86_400);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u8;
    let month = (if mp < 10 { mp + 3 } else { mp - 9 }) as u8;
    let year = (y + i64::from(month <= 2)) as i32;
    (
        year,
        month,
        day,
        (secs / 3_600) as u8,
        ((secs % 3_600) / 60) as u8,
        (secs % 60) as u8,
    )
}

/// Minimal JSON string escaping (the emitter writes JSON by hand to avoid a
/// serde dependency in an example).
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// case results
// ---------------------------------------------------------------------------

enum CaseResult {
    /// Query rows: column names + encoded cells.
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    /// PL/SQL OUT binds, ordered by bind index.
    Out(Vec<String>),
    /// Error surface: the first ORA-NNNNN code (or a prefix of the message).
    Error(String),
}

fn ora_code(err: &oracledb::Error) -> String {
    let text = err.to_string();
    if let Some(pos) = text.find("ORA-") {
        let code: String = text[pos..].chars().take(9).collect();
        if code.len() == 9 && code[4..].chars().all(|c| c.is_ascii_digit()) {
            return code;
        }
    }
    // No ORA code: surface a stable prefix so the diff shows what happened.
    let mut prefix: String = text.chars().take(60).collect();
    prefix.insert_str(0, "noora:");
    prefix
}

fn run_query(conn: &mut Connection, sql: &str, binds: Vec<BindValue>) -> CaseResult {
    let query = if binds.is_empty() {
        Query::new(sql)
    } else {
        Query::new(sql).bind(binds)
    };
    let rows = match BlockingConnection::query_with(conn, query).and_then(|rows| rows.collect()) {
        Ok(rows) => rows,
        Err(err) => return CaseResult::Error(ora_code(&err)),
    };
    let mut columns: Vec<String> = Vec::new();
    let mut encoded_rows = Vec::with_capacity(rows.len());
    for row in &rows {
        if columns.is_empty() {
            columns = row.columns().iter().map(|c| c.name().to_string()).collect();
        }
        let mut cells = Vec::with_capacity(row.len());
        for i in 0..row.len() {
            match encode_cell_with_conn(conn, row.value(i)) {
                Ok(cell) => cells.push(cell),
                Err(e) => return CaseResult::Error(format!("encode:{e}")),
            }
        }
        encoded_rows.push(cells);
    }
    CaseResult::Rows {
        columns,
        rows: encoded_rows,
    }
}

/// Run a DML/DDL statement, returning its `rows_affected` as a one-cell row.
fn run_count(conn: &mut Connection, sql: &str) -> Result<Vec<String>, CaseResult> {
    match BlockingConnection::execute(conn, sql, ()) {
        Ok(outcome) => Ok(vec![format!("n:{}", outcome.rows_affected())]),
        Err(err) => Err(CaseResult::Error(ora_code(&err))),
    }
}

fn drop_if_exists(conn: &mut Connection, table: &str) {
    // ORA-00942 is the expected steady state on a fresh schema.
    let _ = BlockingConnection::execute(conn, &format!("drop table {table} purge"), ());
}

// ---------------------------------------------------------------------------
// the corpus — MUST stay in lock-step with scripts/statement_ground_truth.py
// ---------------------------------------------------------------------------

fn run_corpus(conn: &mut Connection) -> Vec<(&'static str, CaseResult)> {
    let mut results: Vec<(&'static str, CaseResult)> = Vec::new();

    // Session normalization: identical decode context on both sides.
    let _ = BlockingConnection::execute(conn, "alter session set time_zone = '+00:00'", ());

    let simple_queries: &[(&str, &str)] = &[
        (
            "num_int_edges",
            "select 0, 1, -1, 42, -42, 2147483647, -2147483648, 9223372036854775807, \
             to_number('99999999999999999999999999999999999999'), \
             to_number('-99999999999999999999999999999999999999') from dual",
        ),
        (
            "num_frac_edges",
            "select 0.5, -0.5, 0.1, -0.1, 123.456, -123.456, \
             0.000000000000000000000000000001, 1.5E125, -1.5E125, 1E-130, -1E-130 from dual",
        ),
        (
            "num_negative_scale",
            "select 12345678901234567890, 1E10, 123450000, 99999999999999999999999999999999999000 \
             from dual",
        ),
        (
            "str_basic",
            "select 'plain', 'trailing ', ' leading', 'MiXeD-42_!@#' from dual",
        ),
        (
            "str_unicode",
            "select 'üñíçødé', 'żółć', '日本語テキスト', '💾🚀', unistr('\\20AC') from dual",
        ),
        (
            "str_nvarchar",
            "select cast('nvalue' as nvarchar2(30)), n'unicode-ñ', cast('ab' as nchar(4)) from dual",
        ),
        ("str_char_pad", "select cast('ab' as char(5)) from dual"),
        (
            "empty_null",
            "select '', null, cast(null as number), cast(null as date), \
             cast(null as raw(10)) from dual",
        ),
        (
            "long_4000",
            "select rpad('x', 4000, 'x'), rpad('ab', 2000, 'ab') from dual",
        ),
        (
            "date_vals",
            "select date '2026-02-28', date '2000-02-29', date '1970-01-01', \
             to_date('1900-01-01 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), \
             to_date('0001-01-01', 'YYYY-MM-DD'), date '9999-12-31' from dual",
        ),
        (
            "ts_vals",
            "select timestamp '2026-07-04 12:34:56.123456', \
             timestamp '2026-07-04 00:00:00', \
             to_timestamp('2026-12-31 23:59:59.999999', 'YYYY-MM-DD HH24:MI:SS.FF6') from dual",
        ),
        (
            "tstz_vals",
            "select timestamp '2026-07-04 12:34:56.123456 +05:30', \
             timestamp '2026-07-04 12:34:56.123456 -08:00', \
             timestamp '2026-07-04 12:34:56.123456 +00:00' from dual",
        ),
        (
            "raw_vals",
            "select hextoraw('DEADBEEF00FF'), hextoraw(''), utl_raw.cast_to_raw('abc') from dual",
        ),
        (
            "float_native",
            "select cast(1.5 as binary_double), cast(-2.25 as binary_double), \
             cast(0.1 as binary_double), cast(1.5 as binary_float), \
             cast(0.1 as binary_float), binary_double_infinity, \
             -binary_double_infinity, binary_double_nan from dual",
        ),
        (
            "float_oracle",
            "select cast(2.5 as float(126)), cast(123.25 as number(10,2)), \
             cast(7 as integer) from dual",
        ),
        (
            "fetch_pages_99",
            "select level, 'r' || level from dual connect by level <= 99 order by level",
        ),
        (
            "fetch_pages_100",
            "select level, 'r' || level from dual connect by level <= 100 order by level",
        ),
        (
            "fetch_pages_101",
            "select level, 'r' || level from dual connect by level <= 101 order by level",
        ),
        (
            "fetch_pages_250",
            "select level, mod(level * 7, 97), rpad('p', 100, 'p') \
             from dual connect by level <= 250 order by level",
        ),
        ("err_no_table", "select * from gt_truth_missing_tbl"),
        ("err_bad_col", "select bogus_col from dual"),
        ("err_div_zero", "select 1/0 from dual"),
        ("err_syntax", "select from dual"),
    ];
    for (id, sql) in simple_queries {
        let result = run_query(conn, sql, Vec::new());
        results.push((id, result));
    }

    // Bind round-trips.
    results.push((
        "bind_roundtrip",
        run_query(
            conn,
            "select :1, :2, :3 from dual",
            vec![
                BindValue::Number("42".to_string()),
                BindValue::Text("text-bind".to_string()),
                BindValue::Null,
            ],
        ),
    ));
    results.push((
        "bind_exprs",
        run_query(
            conn,
            "select :1 * 2, upper(:2), nvl(:3, 'was-null') from dual",
            vec![
                BindValue::Number("21".to_string()),
                BindValue::Text("abc".to_string()),
                BindValue::Null,
            ],
        ),
    ));

    // PL/SQL block with OUT binds.
    results.push(("plsql_out", {
        let exec = Execute::new("begin :1 := 40 + 2; :2 := 'out-' || :3; end;").bind(vec![
            BindValue::Output {
                ora_type_num: ORA_TYPE_NUM_NUMBER,
                csfrm: 0,
                buffer_size: 22,
            },
            BindValue::Output {
                ora_type_num: ORA_TYPE_NUM_VARCHAR,
                csfrm: CS_FORM_IMPLICIT,
                buffer_size: 64,
            },
            BindValue::Text("x".to_string()),
        ]);
        match BlockingConnection::execute_with(conn, exec) {
            Ok(outcome) => {
                let mut cells = Vec::new();
                let mut failed = None;
                for (_, value) in outcome.out_binds().values() {
                    match encode_cell(value.as_ref()) {
                        Ok(cell) => cells.push(cell),
                        Err(e) => {
                            failed = Some(CaseResult::Error(format!("encode:{e}")));
                            break;
                        }
                    }
                }
                failed.unwrap_or(CaseResult::Out(cells))
            }
            Err(err) => CaseResult::Error(ora_code(&err)),
        }
    }));
    results.push(("err_plsql", {
        let exec = Execute::new("begin raise_application_error(-20001, 'boom'); end;");
        match BlockingConnection::execute_with(conn, exec) {
            Ok(_) => CaseResult::Error("noora:unexpected success".to_string()),
            Err(err) => CaseResult::Error(ora_code(&err)),
        }
    }));

    // DML rows_affected suite (each statement's count becomes one row).
    results.push(("dml_counts", {
        drop_if_exists(conn, DML_TABLE);
        let statements = [
            format!("create table {DML_TABLE} (id number(10), label varchar2(40))"),
            format!("insert into {DML_TABLE} (id, label) values (0, 'zero')"),
            format!(
                "insert into {DML_TABLE} (id, label) \
                 select level, 'row-' || level from dual connect by level <= 3"
            ),
            format!("update {DML_TABLE} set label = label || '!' where id >= 2"),
            format!("delete from {DML_TABLE} where id = 0"),
        ];
        let mut rows = Vec::new();
        let mut failed = None;
        for sql in &statements {
            match run_count(conn, sql) {
                Ok(row) => rows.push(row),
                Err(err_result) => {
                    failed = Some(err_result);
                    break;
                }
            }
        }
        if let Some(failed) = failed {
            failed
        } else {
            // Final contents check rides along as ordinary rows.
            match run_query(
                conn,
                &format!("select id, label from {DML_TABLE} order by id"),
                Vec::new(),
            ) {
                CaseResult::Rows {
                    rows: content_rows, ..
                } => {
                    rows.extend(content_rows);
                    let _ = BlockingConnection::execute(
                        conn,
                        &format!("drop table {DML_TABLE} purge"),
                        (),
                    );
                    CaseResult::Rows {
                        columns: vec!["DML".to_string()],
                        rows,
                    }
                }
                other => other,
            }
        }
    }));

    // LOB roundtrip: build a >1-chunk CLOB and BLOB server-side (identical on
    // both twins), then fetch them materialized and compare full contents.
    results.push(("lob_roundtrip", {
        drop_if_exists(conn, LOB_TABLE);
        let setup: Vec<String> = vec![
            format!("create table {LOB_TABLE} (id number(5), c clob, b blob)"),
            format!("insert into {LOB_TABLE} (id, c, b) values (1, empty_clob(), empty_blob())"),
        ];
        let plsql = format!(
            "declare \
               l_c clob; l_b blob; l_chunk varchar2(1000); \
             begin \
               select c, b into l_c, l_b from {LOB_TABLE} where id = 1 for update; \
               for i in 0..99 loop \
                 l_chunk := lpad(to_char(i), 10, '0') || rpad('abcdefghij', 990, 'k'); \
                 dbms_lob.writeappend(l_c, length(l_chunk), l_chunk); \
                 dbms_lob.writeappend(l_b, 500, utl_raw.cast_to_raw(substr(l_chunk, 1, 500))); \
               end loop; \
               commit; \
             end;"
        );
        let mut failed = None;
        for sql in &setup {
            if let Err(err_result) = run_count(conn, sql) {
                failed = Some(err_result);
                break;
            }
        }
        if failed.is_none() {
            if let Err(err) = BlockingConnection::execute_with(conn, Execute::new(&plsql)) {
                failed = Some(CaseResult::Error(ora_code(&err)));
            }
        }
        if let Some(failed) = failed {
            failed
        } else {
            let result = run_query(
                conn,
                &format!(
                    "select dbms_lob.getlength(c), dbms_lob.getlength(b), c, b \
                     from {LOB_TABLE} where id = 1"
                ),
                Vec::new(),
            );
            let _ = BlockingConnection::execute(conn, &format!("drop table {LOB_TABLE} purge"), ());
            result
        }
    }));

    results
}

// ---------------------------------------------------------------------------
// JSON emission
// ---------------------------------------------------------------------------

fn emit_json(results: &[(&'static str, CaseResult)]) -> String {
    let mut out = String::new();
    out.push_str("{\n");
    let _ = write!(
        out,
        "  \"harness\": \"statement-ground-truth\",\n  \"impl\": \"rust\",\n  \"corpus_version\": {CORPUS_VERSION},\n  \"cases\": {{\n"
    );
    for (i, (id, result)) in results.iter().enumerate() {
        let comma = if i + 1 == results.len() { "" } else { "," };
        match result {
            CaseResult::Rows { columns, rows } => {
                let cols = columns
                    .iter()
                    .map(|c| format!("\"{}\"", json_escape(c)))
                    .collect::<Vec<_>>()
                    .join(", ");
                let _ = write!(
                    out,
                    "    \"{id}\": {{\"ok\": true, \"columns\": [{cols}], \"rows\": ["
                );
                for (r, row) in rows.iter().enumerate() {
                    let cells = row
                        .iter()
                        .map(|c| format!("\"{}\"", json_escape(c)))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let rc = if r + 1 == rows.len() { "" } else { ", " };
                    let _ = write!(out, "[{cells}]{rc}");
                }
                let _ = write!(out, "]}}{comma}\n");
            }
            CaseResult::Out(cells) => {
                let cells = cells
                    .iter()
                    .map(|c| format!("\"{}\"", json_escape(c)))
                    .collect::<Vec<_>>()
                    .join(", ");
                let _ = write!(
                    out,
                    "    \"{id}\": {{\"ok\": true, \"out\": [{cells}]}}{comma}\n"
                );
            }
            CaseResult::Error(code) => {
                let _ = write!(
                    out,
                    "    \"{id}\": {{\"ok\": false, \"error\": \"{}\"}}{comma}\n",
                    json_escape(code)
                );
            }
        }
    }
    out.push_str("  }\n}\n");
    out
}

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let connect = resolve(
        args.next(),
        "PYO_TEST_CONNECT_STRING",
        "localhost:1522/FREEPDB1",
    );
    let user = resolve(args.next(), "PYO_TEST_MAIN_USER", "pythontest");
    let password = resolve(args.next(), "PYO_TEST_MAIN_PASSWORD", "pythontest");

    let identity = match ClientIdentity::new(
        "oracledb-ground-truth",
        "gt-lane",
        "gt-runner",
        "gt",
        "rust-oracledb statement ground truth",
    ) {
        Ok(identity) => identity,
        Err(err) => {
            eprintln!("[ground-truth] identity error: {err}");
            return ExitCode::FAILURE;
        }
    };
    let options = ConnectOptions::new(connect, user, password, identity);
    let mut conn = match BlockingConnection::connect(options) {
        Ok(conn) => conn,
        Err(err) => {
            eprintln!("[ground-truth] connect failed: {err}");
            return ExitCode::FAILURE;
        }
    };

    let results = run_corpus(&mut conn);
    print!("{}", emit_json(&results));
    let _ = BlockingConnection::close(conn);
    ExitCode::SUCCESS
}
