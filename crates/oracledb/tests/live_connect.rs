use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::Cx;
use oracledb::{ConnectOptions, Connection};
use oracledb_protocol::{thin::QueryValue, ClientIdentity};

mod common;

#[test]
#[ignore = "requires local Oracle listener from scripts/container.sh up"]
fn live_connect_ping_and_close() {
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
        let options = ConnectOptions::new(
            common::live_conn_string_or(common::FREE23_CONNECT_STRING),
            common::live_user_or(common::FREE23_USER),
            std::env::var("PYO_TEST_MAIN_PASSWORD")
                .expect("PYO_TEST_MAIN_PASSWORD must be set for ignored live test"),
            identity,
        );
        let mut conn = Connection::connect(&cx, options)
            .await
            .expect("Rust thin connection should authenticate");
        assert!(conn.session_id() > 0);
        assert!(conn.serial_num() > 0);
        let charset = conn
            .execute_raw(
                &cx,
                "select value from nls_database_parameters where parameter = 'NLS_CHARACTERSET'",
                2,
                &[],
                oracledb::protocol::thin::ExecuteOptions::default(),
                None,
            )
            .await
            .expect("Rust thin query should execute and fetch text");
        assert_eq!(charset.columns.len(), 1);
        assert_eq!(charset.rows.len(), 1);
        assert!(matches!(
            charset.rows[0][0],
            Some(QueryValue::Text(ref value)) if !value.is_empty()
        ));

        let ratios = conn
            .execute_raw(
                &cx,
                "select cast('X' as varchar2(1)), cast('Y' as nvarchar2(1)) from dual",
                2,
                &[],
                oracledb::protocol::thin::ExecuteOptions::default(),
                None,
            )
            .await
            .expect("Rust thin query should describe varchar and nvarchar");
        assert_eq!(ratios.columns.len(), 2);
        assert!(ratios.columns[0].buffer_size() > 0);
        assert!(ratios.columns[1].buffer_size() > 0);
        conn.ping(&cx)
            .await
            .expect("Rust thin ping should round-trip");
        conn.close(&cx)
            .await
            .expect("Rust thin logoff should round-trip");
    });
}
