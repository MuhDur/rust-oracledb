use oracledb::protocol::thin::BindValue;
use oracledb::{
    Batch, BatchOutcome, BlockingConnection, BlockingRows, Connection, Execute, ExecuteOutcome,
    Query, Registration, RegistrationOutcome, Result, Row,
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
}

fn main() {}
