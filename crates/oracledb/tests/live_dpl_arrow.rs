//! Live end-to-end test for direct path load + arrow fetch against a local
//! Oracle Free container. Run with:
//!
//! ```sh
//! eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1526 \
//!         ORACLEDB_HOST_PORT=1526 scripts/container.sh env)"
//! cargo test -p oracledb --features arrow --test live_dpl_arrow -- --ignored
//! ```
#![cfg(feature = "arrow")]

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type, TimestampSecondType};
use arrow_array::Array;
use arrow_schema::{DataType, TimeUnit};
use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::Cx;
use oracledb::arrow::ArrowFetchOptions;
use oracledb::{ConnectOptions, Connection};
use oracledb_protocol::dpl::DirectPathColumnValue;
use oracledb_protocol::ClientIdentity;

mod common;

#[test]
#[ignore = "requires local Oracle listener from scripts/container.sh up"]
fn live_direct_path_load_then_arrow_fetch() {
    let reactor = reactor::create_reactor().expect("native reactor should build for live I/O");
    let runtime = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("current-thread Asupersync runtime should build");

    runtime.block_on(async {
        let cx = Cx::current().expect("Runtime::block_on should install an ambient Cx");
        let identity = ClientIdentity::new(
            "rust-oracledb",
            "rusthost",
            "rustuser",
            "rustterm",
            "rust-oracledb thn : 0.0.0",
        )
        .expect("test identity should be valid");
        let user = common::live_user_or(common::FREE23_USER);
        let options = ConnectOptions::new(
            common::live_conn_string_or("localhost:1526/FREEPDB1"),
            user.clone(),
            std::env::var("PYO_TEST_MAIN_PASSWORD")
                .expect("PYO_TEST_MAIN_PASSWORD must be set for ignored live test"),
            identity,
        );
        let mut conn = Connection::connect(&cx, options)
            .await
            .expect("Rust thin connection should authenticate");

        let _ = conn
            .execute_raw(
                &cx,
                "drop table rust_dpl_live purge",
                1,
                &[],
                oracledb::protocol::thin::ExecuteOptions::default(),
                None,
            )
            .await;
        conn.execute_raw(
            &cx,
            "create table rust_dpl_live (
                 id      number(9) not null,
                 name    varchar2(100) not null,
                 salary  number(9, 2),
                 hired   date
             )",
            1,
            &[],
            oracledb::protocol::thin::ExecuteOptions::default(),
            None,
        )
        .await
        .expect("create table should succeed");

        let rows: Vec<Vec<DirectPathColumnValue>> = (1..=5)
            .map(|i| {
                vec![
                    DirectPathColumnValue::Number(i.to_string()),
                    DirectPathColumnValue::Bytes(format!("name{i}").into_bytes()),
                    if i == 3 {
                        DirectPathColumnValue::Null
                    } else {
                        DirectPathColumnValue::Number(format!("{i}.5"))
                    },
                    DirectPathColumnValue::DateTime {
                        year: 2024,
                        month: 6,
                        day: i,
                        hour: 12,
                        minute: 0,
                        second: 0,
                        nanosecond: 0,
                    },
                ]
            })
            .collect();
        let columns = ["id", "name", "salary", "hired"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        // batch_size 2 forces three load stream round trips
        conn.direct_path_load(&cx, &user, "rust_dpl_live", &columns, &rows, 2)
            .await
            .expect("direct path load should succeed");

        // direct path FINISH commits server-side; the rows must be visible
        let batch = conn
            .fetch_all_record_batch(
                &cx,
                "select id, name, salary, hired from rust_dpl_live order by id",
                100,
                &ArrowFetchOptions::default(),
            )
            .await
            .expect("arrow fetch should succeed");
        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::LargeUtf8);
        assert_eq!(batch.schema().field(2).data_type(), &DataType::Float64);
        assert_eq!(
            batch.schema().field(3).data_type(),
            &DataType::Timestamp(TimeUnit::Second, None)
        );
        let ids = batch.column(0).as_primitive::<Int64Type>();
        assert_eq!(
            (0..5).map(|i| ids.value(i)).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        assert_eq!(batch.column(1).as_string::<i64>().value(0), "name1");
        let salaries = batch.column(2).as_primitive::<Float64Type>();
        assert_eq!(salaries.value(0), 1.5);
        assert!(salaries.is_null(2), "row 3 salary was loaded as NULL");
        let hired = batch.column(3).as_primitive::<TimestampSecondType>();
        // 2024-06-01T12:00:00Z
        assert_eq!(hired.value(0), 1_717_243_200);

        // batched fetch: 5 rows with batch size 2 -> 3 batches (2/2/1)
        let mut fetch = conn
            .fetch_record_batches(
                &cx,
                "select id from rust_dpl_live order by id",
                2,
                &ArrowFetchOptions::default(),
            )
            .await
            .expect("batched arrow fetch should start");
        let mut batch_sizes = Vec::new();
        while let Some(batch) = fetch
            .next_batch(&cx, &mut conn)
            .await
            .expect("next batch should fetch")
        {
            batch_sizes.push(batch.num_rows());
        }
        assert_eq!(batch_sizes, vec![2, 2, 1]);

        // empty result still yields exactly one zero-length batch
        let mut fetch = conn
            .fetch_record_batches(
                &cx,
                "select id from rust_dpl_live where id < 0",
                10,
                &ArrowFetchOptions::default(),
            )
            .await
            .expect("empty arrow fetch should start");
        let first = fetch
            .next_batch(&cx, &mut conn)
            .await
            .expect("first batch should fetch");
        assert_eq!(first.map(|b| b.num_rows()), Some(0));
        assert!(fetch
            .next_batch(&cx, &mut conn)
            .await
            .expect("second poll should succeed")
            .is_none());

        // a DPY-8001 failure mid-load must abort: no rows may stick
        let bad_rows = vec![vec![
            DirectPathColumnValue::Number("99".into()),
            DirectPathColumnValue::Null,
            DirectPathColumnValue::Null,
            DirectPathColumnValue::Null,
        ]];
        let err = conn
            .direct_path_load(&cx, &user, "rust_dpl_live", &columns, &bad_rows, 100)
            .await
            .expect_err("NULL into NOT NULL column must fail client-side");
        assert!(err.to_string().starts_with("DPY-8001:"), "{err}");
        let count = conn
            .execute_raw(
                &cx,
                "select count(*) from rust_dpl_live",
                10,
                &[],
                oracledb::protocol::thin::ExecuteOptions::default(),
                None,
            )
            .await
            .expect("count should fetch");
        assert_eq!(
            count.rows[0][0]
                .as_ref()
                .and_then(oracledb_protocol::thin::QueryValue::as_number_text)
                .as_deref(),
            Some("5"),
        );

        conn.execute_raw(
            &cx,
            "drop table rust_dpl_live purge",
            1,
            &[],
            oracledb::protocol::thin::ExecuteOptions::default(),
            None,
        )
        .await
        .expect("drop table should succeed");
        conn.close(&cx)
            .await
            .expect("Rust thin logoff should round-trip");
    });
}
