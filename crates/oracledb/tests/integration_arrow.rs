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
use arrow_array::types::{Float32Type, Float64Type, Int64Type, IntervalMonthDayNanoType};
use arrow_schema::{DataType, IntervalUnit};
use oracledb::arrow::ArrowFetchOptions;
use oracledb::{BlockingConnection, ConnectOptions, Connection};
use oracledb_protocol::ClientIdentity;

mod common;

fn connect_options() -> Option<ConnectOptions> {
    let common::LiveCreds {
        connect_string,
        user,
        password,
    } = common::live_creds_opt()?;
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
        let _ = BlockingConnection::execute_raw(
            conn,
            "drop table rust_itest_arrow purge",
            1,
            &[],
            oracledb::protocol::thin::ExecuteOptions::default(),
            None,
        );
        BlockingConnection::execute_raw(
            conn,
            "create table rust_itest_arrow (id number(9), amount number(12,2))",
            1,
            &[],
            oracledb::protocol::thin::ExecuteOptions::default(),
            None,
        )
        .expect("create table");
        for (id, amount) in [(1, "10.50"), (2, "20.25"), (3, "30.00")] {
            BlockingConnection::execute_raw(
                conn,
                &format!("insert into rust_itest_arrow values ({id}, {amount})"),
                1,
                &[],
                oracledb::protocol::thin::ExecuteOptions::default(),
                None,
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

        let _ = BlockingConnection::execute_raw(
            conn,
            "drop table rust_itest_arrow purge",
            1,
            &[],
            oracledb::protocol::thin::ExecuteOptions::default(),
            None,
        );
    });
}

/// INTERVAL DAY TO SECOND / YEAR TO MONTH columns fetched into Arrow must map to
/// the `MonthDayNano` interval with the correct months/days/nanoseconds, and the
/// columnar fast path must agree with the row path
/// (bead rust-oracledb-upstream-sync-2026-07-13-etib.6).
#[test]
fn fetch_record_batch_interval_columns() {
    with_connection("fetch_record_batch_interval_columns", |conn| {
        let _ = BlockingConnection::execute_raw(
            conn,
            "drop table rust_itest_arrow_intvl purge",
            1,
            &[],
            oracledb::protocol::thin::ExecuteOptions::default(),
            None,
        );
        BlockingConnection::execute_raw(
            conn,
            "create table rust_itest_arrow_intvl (\
               ds interval day(2) to second(6), \
               ym interval year(4) to month)",
            1,
            &[],
            oracledb::protocol::thin::ExecuteOptions::default(),
            None,
        )
        .expect("create interval table");
        BlockingConnection::execute_raw(
            conn,
            "insert into rust_itest_arrow_intvl values (\
               interval '5 02:34:56.123456' day(2) to second(6), \
               interval '3-7' year(4) to month)",
            1,
            &[],
            oracledb::protocol::thin::ExecuteOptions::default(),
            None,
        )
        .expect("insert interval row");
        BlockingConnection::commit(conn).expect("commit");

        let sql = "select ds, ym from rust_itest_arrow_intvl";
        let options = ArrowFetchOptions::default();
        let batch = BlockingConnection::fetch_all_record_batch(conn, sql, 100, &options)
            .expect("interval arrow fetch");

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
        assert_eq!(
            batch.schema().field(1).data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );

        let ds = batch
            .column(0)
            .as_primitive::<IntervalMonthDayNanoType>()
            .value(0);
        assert_eq!(ds.months, 0);
        assert_eq!(ds.days, 5);
        // (2*3600 + 34*60 + 56) s = 9296 s; + 0.123456 s = 123_456_000 ns.
        assert_eq!(ds.nanoseconds, 9296 * 1_000_000_000 + 123_456_000);

        let ym = batch
            .column(1)
            .as_primitive::<IntervalMonthDayNanoType>()
            .value(0);
        assert_eq!(ym.months, 3 * 12 + 7);
        assert_eq!(ym.days, 0);
        assert_eq!(ym.nanoseconds, 0);

        // The columnar fast path must agree with the row path cell-for-cell.
        let columnar =
            BlockingConnection::fetch_all_record_batch_columnar(conn, sql, 100, &options)
                .expect("interval columnar fetch");
        assert_eq!(
            batch, columnar,
            "columnar interval batch must equal row path"
        );

        let _ = BlockingConnection::execute_raw(
            conn,
            "drop table rust_itest_arrow_intvl purge",
            1,
            &[],
            oracledb::protocol::thin::ExecuteOptions::default(),
            None,
        );
    });
}

/// A COLD `fetch_all_record_batch` on a VECTOR column — with NO prior query on
/// the statement — must return the correct `FixedSizeList(Float32, N)` batch on
/// 23ai with no desync (bead a4-0mk). The Arrow columnar fast path used to send
/// the execute with inline prefetch before the VECTOR client-side define was
/// established, so a cold fetch desynced with "invalid ub8 length"; it only
/// worked once a prior query had cached the describe. This proves the standalone
/// cold fetch works with no warm-up.
#[test]
fn cold_fetch_all_record_batch_vector_fixed_size_list() {
    with_connection(
        "cold_fetch_all_record_batch_vector_fixed_size_list",
        |conn| {
            // VECTOR is a 23ai-only datatype; skip on older lanes.
            let major = conn.server_version_tuple().map(|(m, ..)| m).unwrap_or(0);
            if major < 23 {
                eprintln!(
                    "skipped cold VECTOR arrow fetch: server major {major} < 23 (no VECTOR type)"
                );
                return;
            }

            let _ = BlockingConnection::execute_raw(
                conn,
                "drop table rust_itest_arrow_vec purge",
                1,
                &[],
                oracledb::protocol::thin::ExecuteOptions::default(),
                None,
            );
            BlockingConnection::execute_raw(
                conn,
                "create table rust_itest_arrow_vec (id number(5), embedding vector(3, float32))",
                1,
                &[],
                oracledb::protocol::thin::ExecuteOptions::default(),
                None,
            )
            .expect("create vector table");
            for (id, lit) in [(1, "[1.5, 2.5, 3.5]"), (2, "[4.5, 5.5, 6.5]")] {
                BlockingConnection::execute_raw(
                    conn,
                    &format!("insert into rust_itest_arrow_vec values ({id}, to_vector('{lit}'))"),
                    1,
                    &[],
                    oracledb::protocol::thin::ExecuteOptions::default(),
                    None,
                )
                .expect("insert vector");
            }
            BlockingConnection::commit(conn).expect("commit");

            // COLD arrow fetch: no prior query_all/query warm-up on this SQL, so the
            // statement's describe is not cached. This is exactly the standalone
            // path that used to desync.
            let options = ArrowFetchOptions::new().with_vector_fixed_size_list(true);
            let batch = BlockingConnection::fetch_all_record_batch(
                conn,
                "select embedding from rust_itest_arrow_vec order by id",
                100,
                &options,
            )
            .expect("cold arrow fetch on a VECTOR column should not desync");

            assert_eq!(batch.num_rows(), 2, "expected 2 VECTOR rows");
            match batch.schema().field(0).data_type() {
                DataType::FixedSizeList(field, 3) => {
                    assert_eq!(
                        field.data_type(),
                        &DataType::Float32,
                        "FixedSizeList element type must be Float32"
                    );
                }
                other => panic!("VECTOR must map to FixedSizeList(Float32, 3), got {other:?}"),
            }
            let list = batch.column(0).as_fixed_size_list();
            let expected = [[1.5f32, 2.5, 3.5], [4.5, 5.5, 6.5]];
            for (i, want) in expected.iter().enumerate() {
                let got: Vec<f32> = list
                    .value(i)
                    .as_primitive::<Float32Type>()
                    .values()
                    .to_vec();
                assert_eq!(&got, want, "VECTOR row {i} mismatch");
            }

            let _ = BlockingConnection::execute_raw(
                conn,
                "drop table rust_itest_arrow_vec purge",
                1,
                &[],
                oracledb::protocol::thin::ExecuteOptions::default(),
                None,
            );
        },
    );
}
