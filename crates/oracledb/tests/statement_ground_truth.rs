//! Statement-suite ground-truth differential (bead rust-oracledb-rwoh).
//!
//! Live, env-gated like the other `live_*` tests (`#[ignore]`, needs a lane
//! from `scripts/version_matrix.sh` / `scripts/container.sh` plus the
//! `PYO_TEST_*` variables). Two layers:
//!
//! 1. `ground_truth_differential_vs_python`: runs the FIXED corpus through
//!    BOTH drivers — the Rust emitter (`examples/statement_ground_truth.rs`,
//!    via `cargo run`) and python-oracledb (`scripts/statement_ground_truth.py`
//!    via the repo venv) — and diffs the two JSON documents field-by-field
//!    with the python twin's `--diff` mode. Any mismatch fails the test.
//!    Self-skips (with a loud message) when the python twin is unavailable.
//!
//! 2. `tstz_bind_fields_are_utc_on_the_wire`: pins the TIMESTAMP WITH TIME
//!    ZONE wire contract that bead rust-oracledb-97cj fixed: `BindValue::
//!    TimestampTz` fields are UTC and the offset is the display timezone, so
//!    the SERVER (the ultimate oracle) must render wall clock = fields +
//!    offset.

use std::path::PathBuf;
use std::process::Command;

extern crate oraclemcp_driver_cx as oracledb;

use oracledb::protocol::thin::{BindValue, QueryValue};
use oracledb::protocol::ClientIdentity;
use oracledb::{BlockingConnection, ConnectOptions, Query};

mod common;

fn lane() -> (String, String, String) {
    (
        common::live_conn_string_or(common::FREE23_CONNECT_STRING),
        common::live_user_or(common::FREE23_USER),
        std::env::var("PYO_TEST_MAIN_PASSWORD")
            .expect("PYO_TEST_MAIN_PASSWORD must be set for ignored live test"),
    )
}

fn repo_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop(); // crates/
    p.pop(); // repo root
    p
}

#[test]
#[ignore = "requires local Oracle lane and python-oracledb; credentials in PYO_TEST_*"]
fn named_region_tstz_matches_oracle_and_python_at_dst_edges() {
    let (connect, user, password) = lane();
    let python = python_with_oracledb().expect("python-oracledb reference is required");
    let identity =
        ClientIdentity::new("named-tstz", "local", "local", "local", "local").expect("identity");
    let mut conn = BlockingConnection::connect(ConnectOptions::new(
        connect.clone(),
        user.clone(),
        password.clone(),
        identity,
    ))
    .expect("connect");
    let versions = BlockingConnection::query_with(
        &mut conn,
        Query::new("SELECT VERSION FROM SYS.V_$TIMEZONE_FILE"),
    )
    .and_then(|rows| rows.collect())
    .expect("timezone-file version");
    let version = match versions[0].value(0) {
        Some(QueryValue::Number(value)) => {
            value.to_string().parse::<u32>().expect("version number")
        }
        other => panic!("timezone version is not a number: {other:?}"),
    };
    let mut cases = vec![
        (
            "2020-03-08 06:59:59.123456789",
            "America/New_York",
            "-05:00",
        ),
        (
            "2020-03-08 07:00:00.987654321",
            "America/New_York",
            "-04:00",
        ),
        (
            "2020-11-01 05:59:59.123456789",
            "America/New_York",
            "-04:00",
        ),
        (
            "2020-11-01 06:00:00.987654321",
            "America/New_York",
            "-05:00",
        ),
        (
            "2020-04-04 14:59:59.123456789",
            "Australia/Lord_Howe",
            "+11:00",
        ),
        (
            "2020-04-04 15:00:00.987654321",
            "Australia/Lord_Howe",
            "+10:30",
        ),
        (
            "2020-10-03 15:29:59.123456789",
            "Australia/Lord_Howe",
            "+10:30",
        ),
        (
            "2020-10-03 15:30:00.987654321",
            "Australia/Lord_Howe",
            "+11:00",
        ),
        ("2020-03-29 00:59:59.123456789", "Europe/Vienna", "+01:00"),
        ("2020-03-29 01:00:00.987654321", "Europe/Vienna", "+02:00"),
        ("2020-10-25 00:59:59.123456789", "Europe/Vienna", "+02:00"),
        ("2020-10-25 01:00:00.987654321", "Europe/Vienna", "+01:00"),
    ];
    if version >= 45 {
        cases.push((
            "2025-04-06 03:00:00.123456789",
            "America/Coyhaique",
            "-03:00",
        ));
    }
    let input = cases
        .iter()
        .map(|(utc, region, _)| format!("{utc}\t{region}"))
        .collect::<Vec<_>>()
        .join("\n");
    // Python thin cannot fetch native named-region values. It independently
    // asks Oracle for UTC/region/offset text, preserving all nine digits.
    let reference = Command::new(python).arg("-c").arg(r#"
import os, sys, oracledb
with oracledb.connect(user=os.environ['Z779C_USER'], password=os.environ['Z779C_PASSWORD'], dsn=os.environ['Z779C_DSN']) as conn:
    cur = conn.cursor()
    for line in sys.argv[1].splitlines():
        utc, region = line.split('\t')
        cur.execute("SELECT TO_CHAR(SYS_EXTRACT_UTC(t),'YYYY-MM-DD\"T\"HH24:MI:SS.FF9\"Z\"'), EXTRACT(TIMEZONE_REGION FROM t), TO_CHAR(t,'TZH:TZM') FROM (SELECT FROM_TZ(TO_TIMESTAMP(:1,'YYYY-MM-DD HH24:MI:SS.FF9'),'+00:00') AT TIME ZONE (:2) AS t FROM SYS.DUAL)", [utc, region])
        print('\t'.join(cur.fetchone()))
"#).arg(input).env("Z779C_DSN", &connect).env("Z779C_USER", &user)
        .env("Z779C_PASSWORD", &password).output().expect("python reference");
    assert!(
        reference.status.success(),
        "python reference failed: {}",
        String::from_utf8_lossy(&reference.stderr)
    );
    let output = String::from_utf8(reference.stdout).expect("reference UTF-8");
    let reference_rows: Vec<_> = output.lines().collect();
    assert_eq!(reference_rows.len(), cases.len());
    for ((utc, region_name, expected_offset), reference) in cases.iter().zip(reference_rows) {
        let sql = "SELECT t, TO_CHAR(SYS_EXTRACT_UTC(t),'YYYY-MM-DD\"T\"HH24:MI:SS.FF9\"Z\"'), EXTRACT(TIMEZONE_REGION FROM t), TO_CHAR(t,'TZH:TZM') FROM (SELECT FROM_TZ(TO_TIMESTAMP(:1,'YYYY-MM-DD HH24:MI:SS.FF9'),'+00:00') AT TIME ZONE (:2) AS t FROM SYS.DUAL)";
        let rows = BlockingConnection::query_with(
            &mut conn,
            Query::new(sql).bind(vec![
                BindValue::Text((*utc).into()),
                BindValue::Text((*region_name).into()),
            ]),
        )
        .and_then(|rows| rows.collect())
        .expect("native named-region query");
        assert_eq!(rows.len(), 1);
        let Some(QueryValue::TimestampTzRegion {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
            region,
        }) = rows[0].value(0)
        else {
            panic!("native region must remain distinct: {:?}", rows[0].value(0));
        };
        let decoded_utc = format!(
            "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{nanosecond:09}Z"
        );
        let expected_utc = format!("{}Z", utc.replace(' ', "T"));
        assert_eq!(decoded_utc, expected_utc, "{region_name}");
        assert_eq!(region.name().expect("known region name"), *region_name);
        let mut server = Vec::new();
        for index in 1..=3 {
            let Some(QueryValue::Text(text)) = rows[0].value(index) else {
                panic!("server projection must be text: {:?}", rows[0].value(index));
            };
            server.push(text.as_str());
        }
        assert_eq!(
            server,
            vec![decoded_utc.as_str(), *region_name, *expected_offset]
        );
        assert_eq!(server.join("\t"), reference, "independent python reference");
    }
    BlockingConnection::close(conn).expect("close");
}

/// Locate a python interpreter that can import oracledb: `ORACLEDB_GT_PYTHON`
/// overrides; default is the repo-pinned venv.
fn python_with_oracledb() -> Option<PathBuf> {
    let candidate = std::env::var("ORACLEDB_GT_PYTHON")
        .map(PathBuf::from)
        .unwrap_or_else(|_| repo_root().join(".venv-py313/bin/python"));
    let probe = Command::new(&candidate)
        .args(["-c", "import oracledb"])
        .output()
        .ok()?;
    probe.status.success().then_some(candidate)
}

#[test]
#[ignore = "requires local Oracle listener from scripts/version_matrix.sh up"]
fn ground_truth_differential_vs_python() {
    let (connect, user, password) = lane();
    let root = repo_root();
    let Some(python) = python_with_oracledb() else {
        eprintln!(
            "[ground-truth] SKIP: no python with python-oracledb found \
             (set ORACLEDB_GT_PYTHON or create .venv-py313)"
        );
        return;
    };

    let out_dir = std::env::temp_dir().join(format!("oracledb-gt-{}", std::process::id()));
    std::fs::create_dir_all(&out_dir).expect("create ground-truth output dir");
    let rust_json = out_dir.join("rust.json");
    let py_json = out_dir.join("python.json");

    // Rust emitter (the corpus lives in the example so the twins stay two
    // files total; `cargo run` reuses the already-built example in CI).
    let rust_out = Command::new(env!("CARGO"))
        .current_dir(&root)
        .args([
            "run",
            "-q",
            "-p",
            "oracledb",
            "--example",
            "statement_ground_truth",
            "--",
            &connect,
            &user,
            &password,
        ])
        .output()
        .expect("run rust ground-truth emitter");
    assert!(
        rust_out.status.success(),
        "rust emitter failed: {}",
        String::from_utf8_lossy(&rust_out.stderr)
    );
    std::fs::write(&rust_json, &rust_out.stdout).expect("write rust.json");

    let py_out = Command::new(&python)
        .current_dir(&root)
        .args([
            "scripts/statement_ground_truth.py",
            &connect,
            &user,
            &password,
        ])
        .output()
        .expect("run python ground-truth twin");
    assert!(
        py_out.status.success(),
        "python twin failed: {}",
        String::from_utf8_lossy(&py_out.stderr)
    );
    std::fs::write(&py_json, &py_out.stdout).expect("write python.json");

    let diff = Command::new(&python)
        .current_dir(&root)
        .args(["scripts/statement_ground_truth.py", "--diff"])
        .arg(&rust_json)
        .arg(&py_json)
        .output()
        .expect("run ground-truth diff");
    let report = String::from_utf8_lossy(&diff.stdout);
    eprintln!("{report}");
    assert!(
        diff.status.success(),
        "ground-truth mismatch between rust and python-oracledb:\n{report}"
    );
}

#[test]
#[ignore = "requires local Oracle listener from scripts/version_matrix.sh up"]
fn tstz_bind_fields_are_utc_on_the_wire() {
    let (connect, user, password) = lane();
    let identity = ClientIdentity::new(
        "oracledb-gt-tstz",
        "gt-lane",
        "gt-runner",
        "gt",
        "rust-oracledb tstz wire contract",
    )
    .expect("identity");
    let mut conn =
        BlockingConnection::connect(ConnectOptions::new(connect, user, password, identity))
            .expect("connect");

    // Fields are UTC 07:04:56.123456, display offset +05:30. The server must
    // render the wall clock 12:34:56.123456 +05:30 (reference semantics:
    // decoders.pyx stores UTC fields, converters.pyx adds the offset). This is
    // the contract the chrono conversions rely on (bead rust-oracledb-97cj).
    let bind = BindValue::TimestampTz {
        year: 2026,
        month: 7,
        day: 4,
        hour: 7,
        minute: 4,
        second: 56,
        nanosecond: 123_456_000,
        offset_minutes: 330,
    };
    let rows = BlockingConnection::query_with(
        &mut conn,
        Query::new(
            "select to_char(cast(:1 as timestamp(6) with time zone), \
             'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM') from dual",
        )
        .bind(vec![bind]),
    )
    .and_then(|rows| rows.collect())
    .expect("tstz bind query");
    assert_eq!(rows.len(), 1);
    let rendered = match rows[0].value(0) {
        Some(QueryValue::Text(text)) => text.clone(),
        other => panic!("expected text, got {other:?}"),
    };
    assert_eq!(
        rendered, "2026-07-04 12:34:56.123456 +05:30",
        "server-rendered TSTZ must be wall clock = UTC fields + offset"
    );

    // And the fetch direction: the same literal comes back with UTC fields.
    let rows = BlockingConnection::query_with(
        &mut conn,
        Query::new("select timestamp '2026-07-04 12:34:56.123456 +05:30' from dual"),
    )
    .and_then(|rows| rows.collect())
    .expect("tstz fetch query");
    match rows[0].value(0) {
        Some(QueryValue::TimestampTz {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
            offset_minutes,
        }) => {
            assert_eq!(
                (
                    *year,
                    *month,
                    *day,
                    *hour,
                    *minute,
                    *second,
                    *nanosecond,
                    *offset_minutes
                ),
                (2026, 7, 4, 7, 4, 56, 123_456_000, 330),
                "fetched TSTZ fields must be UTC + display offset"
            );
        }
        other => panic!("expected TimestampTz, got {other:?}"),
    }

    let _ = BlockingConnection::close(conn);
}

/// chrono end-to-end: the typed conversions must agree with the server. Only
/// compiled when the `chrono` feature is on (`cargo test -p oracledb
/// --features chrono -- --ignored`).
#[cfg(feature = "chrono")]
#[test]
#[ignore = "requires local Oracle listener from scripts/version_matrix.sh up"]
fn tstz_chrono_conversions_match_server_instant() {
    use chrono::{DateTime, FixedOffset, Utc};

    let (connect, user, password) = lane();
    let identity = ClientIdentity::new(
        "oracledb-gt-chrono",
        "gt-lane",
        "gt-runner",
        "gt",
        "rust-oracledb tstz chrono live check",
    )
    .expect("identity");
    let mut conn =
        BlockingConnection::connect(ConnectOptions::new(connect, user, password, identity))
            .expect("connect");

    let rows = BlockingConnection::query_with(
        &mut conn,
        Query::new("select timestamp '2026-07-04 12:34:56.123456 +05:30' from dual"),
    )
    .and_then(|rows| rows.collect())
    .expect("tstz chrono query");
    let fixed: DateTime<FixedOffset> = rows[0].get(0).expect("chrono FixedOffset conversion");
    assert_eq!(fixed.to_rfc3339(), "2026-07-04T12:34:56.123456+05:30");
    let utc: DateTime<Utc> = rows[0].get(0).expect("chrono Utc conversion");
    assert_eq!(utc.to_rfc3339(), "2026-07-04T07:04:56.123456+00:00");

    let _ = BlockingConnection::close(conn);
}
