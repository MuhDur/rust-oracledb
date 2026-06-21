#![allow(deprecated)]

use std::time::Duration;

use oracledb::protocol::thin::{BindValue, ExecuteOptions, LobReadResult, QueryResult};
use oracledb::{
    Batch, BatchOutcome, BlockingConnection, BlockingRows, Connection, Execute, ExecuteOutcome,
    NotificationOutcome, Query, Registration, RegistrationOutcome, Result, Row,
};

fn blocking_family_surface(conn: &mut Connection) {
    {
        let _: Result<BlockingRows<'_>> = BlockingConnection::query(conn, "select 1 from dual", ());
    }
    let _: Result<Row> = BlockingConnection::query_one(conn, "select 1 from dual", ());
    let _: Result<Option<Row>> = BlockingConnection::query_opt(conn, "select 1 from dual", ());
    let _: Result<Vec<Row>> = BlockingConnection::query_all(conn, "select 1 from dual", ());
    {
        let _: Result<BlockingRows<'_>> =
            BlockingConnection::query_with(conn, Query::new("select 1 from dual"));
    }

    let _: Result<ExecuteOutcome> = BlockingConnection::execute(conn, "begin null; end;", ());
    let _: Result<ExecuteOutcome> =
        BlockingConnection::execute_with(conn, Execute::new("begin null; end;"));

    let rows = vec![vec![BindValue::Number("1".to_string())]];
    let _: Result<BatchOutcome> =
        BlockingConnection::execute_many(conn, "insert into t values (:1)", &rows);
    let _: Result<BatchOutcome> =
        BlockingConnection::execute_many_with(conn, Batch::new("insert into t values (:1)", &rows));

    let _: Result<RegistrationOutcome> =
        BlockingConnection::register_query(conn, Registration::new("select * from t", 1));

    let _: Result<()> = BlockingConnection::cancel(conn);
    let _: Result<()> = BlockingConnection::notify_register(conn, b"client-id");
    let _: Result<NotificationOutcome> =
        BlockingConnection::recv_notification(conn, 0, 0, Duration::from_millis(1));

    let _: Result<QueryResult> = BlockingConnection::execute_query_with_bind_rows_and_options(
        conn,
        "insert into t values (:1)",
        1,
        &rows,
        ExecuteOptions::default(),
    );

    let locator = vec![0_u8; 16];
    let _: Result<LobReadResult> = BlockingConnection::trim_lob(conn, &locator, 0);
    let locators = vec![locator];
    let _: Result<()> = BlockingConnection::free_temp_lobs(conn, &locators);
}

fn main() {}
