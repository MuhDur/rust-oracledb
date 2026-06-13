//! Live wire test for `Connection::run_pipeline`: drives a real pipelined
//! batch (BEGIN_PIPELINE piggyback, END_OF_REQUEST framing, end-pipeline
//! function 200, N+1 boundary-delimited responses) against the disposable
//! local Oracle container. Skips silently when the container environment
//! (PYO_TEST_* variables) is not configured, so plain `cargo test` stays
//! green offline.

use oracledb::protocol::thin::{parse_query_response, BindValue, QueryValue};
use oracledb::{BlockingConnection, ConnectOptions, PipelineRequest};
use oracledb_protocol::ClientIdentity;

fn connect_options() -> Option<ConnectOptions> {
    let connect_string = std::env::var("PYO_TEST_CONNECT_STRING").ok()?;
    let user = std::env::var("PYO_TEST_MAIN_USER").ok()?;
    let password = std::env::var("PYO_TEST_MAIN_PASSWORD").ok()?;
    let identity = ClientIdentity::new(
        "pipeline_live",
        "localhost",
        "pipeline_live",
        "unknown",
        "rust-oracledb",
    )
    .ok()?;
    Some(ConnectOptions::new(
        connect_string,
        user,
        password,
        identity,
    ))
}

#[test]
fn pipeline_round_trips_against_local_container() {
    let Some(options) = connect_options() else {
        eprintln!("skipped: PYO_TEST_* environment not configured");
        return;
    };
    let mut conn = BlockingConnection::connect(options).expect("connect to test container");
    assert!(
        conn.supports_pipelining(),
        "23ai test container must negotiate END_OF_RESPONSE"
    );

    for ddl in [
        "drop table if exists pipe_live_rust purge",
        "create table pipe_live_rust (id number(9), val varchar2(50))",
    ] {
        BlockingConnection::execute_query(&mut conn, ddl, 1).expect("setup ddl");
    }

    // abort-on-error batch: insert, bound insert, commit, select
    let requests = [
        PipelineRequest::Execute {
            sql: "insert into pipe_live_rust values (1, 'one')".to_string(),
            bind_rows: Vec::new(),
            prefetch_rows: 1,
        },
        PipelineRequest::Execute {
            sql: "insert into pipe_live_rust values (:1, :2)".to_string(),
            bind_rows: vec![vec![
                BindValue::Number("2".to_string()),
                BindValue::Text("two".to_string()),
            ]],
            prefetch_rows: 1,
        },
        PipelineRequest::Commit,
        PipelineRequest::Execute {
            sql: "select id, val from pipe_live_rust order by id".to_string(),
            bind_rows: Vec::new(),
            prefetch_rows: 100,
        },
    ];
    let responses =
        BlockingConnection::run_pipeline(&mut conn, &requests, false).expect("pipeline runs");
    assert_eq!(responses.len(), 5, "four ops + end-pipeline response");

    let capabilities = oracledb_protocol::thin::ClientCapabilities::default();
    for (index, payload) in responses.iter().take(2).enumerate() {
        let result = parse_query_response(payload, capabilities).expect("insert response");
        assert_eq!(result.token_num, Some(index as u64 + 1));
        assert_eq!(result.row_count, 1);
    }
    let commit = parse_query_response(&responses[2], capabilities).expect("commit response");
    assert_eq!(commit.token_num, Some(3));
    let fetched = parse_query_response(&responses[3], capabilities).expect("select response");
    assert_eq!(fetched.token_num, Some(4));
    let rows: Vec<(String, String)> = fetched
        .rows
        .iter()
        .map(|row| {
            let id = match &row[0] {
                Some(QueryValue::Number { text, .. }) => text.clone(),
                other => panic!("unexpected id: {other:?}"),
            };
            let val = match &row[1] {
                Some(QueryValue::Text(text)) => text.clone(),
                other => panic!("unexpected val: {other:?}"),
            };
            (id, val)
        })
        .collect();
    assert_eq!(
        rows,
        [
            ("1".to_string(), "one".to_string()),
            ("2".to_string(), "two".to_string())
        ]
    );
    let end = parse_query_response(&responses[4], capabilities).expect("end-pipeline response");
    assert_eq!(end.token_num, None);

    // continue-on-error batch: a mid-pipeline server error (missing table)
    // must not wedge the connection; later operations still get answers
    let requests = [
        PipelineRequest::Execute {
            sql: "insert into pipe_live_rust values (3, 'three')".to_string(),
            bind_rows: Vec::new(),
            prefetch_rows: 1,
        },
        PipelineRequest::Execute {
            sql: "insert into pipe_live_rust_missing values (1)".to_string(),
            bind_rows: Vec::new(),
            prefetch_rows: 1,
        },
        PipelineRequest::Execute {
            sql: "select count(*) from pipe_live_rust".to_string(),
            bind_rows: Vec::new(),
            prefetch_rows: 2,
        },
    ];
    let responses = BlockingConnection::run_pipeline(&mut conn, &requests, true)
        .expect("continue-on-error pipeline runs");
    assert_eq!(responses.len(), 4);
    let first = parse_query_response(&responses[0], capabilities).expect("insert response");
    assert_eq!(first.token_num, Some(1));
    let error = parse_query_response(&responses[1], capabilities)
        .expect_err("missing table response is an error");
    assert!(
        error.to_string().contains("ORA-00942"),
        "unexpected error: {error}"
    );
    let count = parse_query_response(&responses[2], capabilities).expect("count response");
    assert_eq!(count.token_num, Some(3));
    match &count.rows[0][0] {
        Some(QueryValue::Number { text, .. }) => assert_eq!(text, "3"),
        other => panic!("unexpected count: {other:?}"),
    }

    // the connection must remain healthy for ordinary traffic afterwards
    let after =
        BlockingConnection::execute_query(&mut conn, "select max(id) from pipe_live_rust", 2)
            .expect("plain query after pipelines");
    match &after.rows[0][0] {
        Some(QueryValue::Number { text, .. }) => assert_eq!(text, "3"),
        other => panic!("unexpected max id: {other:?}"),
    }

    BlockingConnection::execute_query(&mut conn, "drop table pipe_live_rust purge", 1)
        .expect("cleanup ddl");
    BlockingConnection::close(conn).expect("close connection");
}
