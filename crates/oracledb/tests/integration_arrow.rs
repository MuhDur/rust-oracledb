//! Standalone-library Arrow integration test: fetch a real result set straight
//! into an Apache Arrow `RecordBatch` through the public crate API, with no
//! shim and no Python. Self-skips when the container environment is absent.
//!
//! Run with:
//!
//! ```sh
//! eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1524 \
//!         ORACLEDB_HOST_PORT=1524 scripts/container.sh env)"
//! cargo test -p oracledb --features arrow --test integration_arrow
//! ```
#![cfg(feature = "arrow")]

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_schema::DataType;
use oracledb::arrow::ArrowFetchOptions;
use oracledb::{BlockingConnection, ConnectOptions, Connection};
use oracledb_protocol::ClientIdentity;

fn connect_options() -> Option<ConnectOptions> {
    let connect_string = std::env::var("PYO_TEST_CONNECT_STRING").ok()?;
    let user = std::env::var("PYO_TEST_MAIN_USER").ok()?;
    let password = std::env::var("PYO_TEST_MAIN_PASSWORD").ok()?;
    let identity = ClientIdentity::new(
        "rust-oracledb-itest",
        "itest-machine",
        "itest-osuser",
        "itest-terminal",
        "rust-oracledb thn : 0.0.0",
    )
    .ok()?;
    Some(ConnectOptions::new(
        connect_string,
        user,
        password,
        identity,
    ))
}

fn with_connection(test: &str, body: impl FnOnce(&mut Connection)) {
    let Some(options) = connect_options() else {
        eprintln!("skipped {test}: PYO_TEST_* environment not configured");
        return;
    };
    let mut conn = BlockingConnection::connect(options).expect("connect to test container");
    body(&mut conn);
    BlockingConnection::close(conn).expect("close connection");
}

#[test]
fn fetch_record_batch_from_live_query() {
    with_connection("fetch_record_batch_from_live_query", |conn| {
        let _ = BlockingConnection::execute_query(conn, "drop table rust_itest_arrow purge", 1);
        BlockingConnection::execute_query(
            conn,
            "create table rust_itest_arrow (id number(9), amount number(12,2))",
            1,
        )
        .expect("create table");
        for (id, amount) in [(1, "10.50"), (2, "20.25"), (3, "30.00")] {
            BlockingConnection::execute_query(
                conn,
                &format!("insert into rust_itest_arrow values ({id}, {amount})"),
                1,
            )
            .expect("insert");
        }
        BlockingConnection::commit(conn).expect("commit");

        let batch = BlockingConnection::fetch_all_record_batch(
            conn,
            "select id, amount from rust_itest_arrow order by id",
            100,
            &ArrowFetchOptions::default(),
        )
        .expect("arrow fetch should produce a record batch");

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Float64);

        let ids = batch.column(0).as_primitive::<Int64Type>();
        assert_eq!(
            (0..3).map(|i| ids.value(i)).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let amounts = batch.column(1).as_primitive::<Float64Type>();
        assert_eq!(amounts.value(0), 10.50);
        assert_eq!(amounts.value(2), 30.00);

        let _ = BlockingConnection::execute_query(conn, "drop table rust_itest_arrow purge", 1);
    });
}
