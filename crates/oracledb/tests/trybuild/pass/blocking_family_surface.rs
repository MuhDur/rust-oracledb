#![allow(deprecated)]

use std::time::Duration;

use oracledb::protocol::thin::{BindValue, ExecuteOptions, LobReadResult, QueryResult};
use oracledb::protocol::{wire::ProtocolLimits, ClientIdentity};
use oracledb::{
    Batch, BatchOutcome, BlockingConnection, BlockingRows, ConnectOptions, Connection, Execute,
    ExecuteOutcome, NotificationOutcome, PipelineRequest, Query, Registration, RegistrationOutcome,
    Result, Row,
};
use oracledb::pool::{
    AcquireOptions, BlockingPool, BlockingPooledConnection, PoolBackend, PoolConfig, PoolError,
    PoolStats, POOL_GETMODE_TIMEDWAIT,
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

fn blocking_pool_surface<B: PoolBackend>(pool: &BlockingPool<B>) {
    let _: std::result::Result<BlockingPooledConnection<B>, PoolError> =
        pool.acquire(AcquireOptions::default());
    let _: std::result::Result<(), PoolError> = pool.drain();
    let _: std::result::Result<PoolStats, PoolError> = pool.stats();
    let _: std::result::Result<u32, PoolError> = pool.busy_count();
    let _: std::result::Result<u32, PoolError> = pool.open_count();
    let _: std::result::Result<(), PoolError> = pool.close(false);
}

fn blocking_pool_stats_surface(stats: PoolStats) {
    let _: u32 = stats.open_count();
    let _: u32 = stats.busy_count();
    let _: u32 = stats.idle_count();
    let _: u32 = stats.opening_count();
    let _: u32 = stats.validating_count();
    let _: u32 = stats.retiring_count();
    let _: u32 = stats.waiter_count();
}

fn option_config_surface() {
    let identity = ClientIdentity::new("program", "machine", "osuser", "terminal", "driver")
        .expect("identity");
    let connect = ConnectOptions::new("localhost/FREEPDB1", "scott", "tiger", identity)
        .with_app_context(vec![("ns".into(), "name".into(), "value".into())])
        .with_proxy_user(Some("proxy".into()))
        .with_sdu(4096)
        .with_server_type_emon(true)
        .with_wallet_location("/wallet")
        .with_wallet_password("secret")
        .with_edition("E1")
        .with_ssl_server_dn_match(false)
        .with_ssl_server_cert_dn("CN=db")
        .with_use_sni(true)
        .with_access_token("token")
        .with_statement_cache_size(7)
        .with_protocol_limits(ProtocolLimits::DEFAULT);
    let _: &str = connect.connect_string();
    let _: &str = connect.user();
    let _: &str = connect.password();
    let _: &ClientIdentity = connect.identity();
    let _: &[(String, String, String)] = connect.app_context();
    let _: u16 = connect.sdu();
    let _: Option<&str> = connect.proxy_user();
    let _: bool = connect.server_type_emon();
    let _: Option<&str> = connect.wallet_location();
    let _: Option<&str> = connect.wallet_password();
    let _: Option<&str> = connect.edition();
    let _: bool = connect.ssl_server_dn_match();
    let _: Option<&str> = connect.ssl_server_cert_dn();
    let _: bool = connect.use_sni();
    let _: Option<&oracledb::AccessToken> = connect.access_token();
    let _: usize = connect.statement_cache_size();
    let _: ProtocolLimits = connect.protocol_limits();

    let exec_options = ExecuteOptions::default()
        .with_batcherrors(true)
        .with_arraydmlrowcounts(true)
        .with_parse_only(true)
        .with_token_num(1)
        .with_cursor_id(2)
        .with_cache_statement(false)
        .with_scrollable(true)
        .with_fetch_orientation(3)
        .with_fetch_pos(4)
        .with_scroll_operation(true)
        .with_suspend_on_success(true)
        .with_no_prefetch(true)
        .with_registration_id(5);
    let _: bool = exec_options.batcherrors();
    let _: bool = exec_options.arraydmlrowcounts();
    let _: bool = exec_options.parse_only();
    let _: u64 = exec_options.token_num();
    let _: u32 = exec_options.cursor_id();
    let _: bool = exec_options.cache_statement();
    let _: bool = exec_options.scrollable();
    let _: u32 = exec_options.fetch_orientation();
    let _: u32 = exec_options.fetch_pos();
    let _: bool = exec_options.scroll_operation();
    let _: bool = exec_options.suspend_on_success();
    let _: bool = exec_options.no_prefetch();
    let _: u64 = exec_options.registration_id();

    let config = PoolConfig::new(1, 4, 1)
        .with_getmode(POOL_GETMODE_TIMEDWAIT)
        .with_wait_timeout_ms(100)
        .with_timeout_secs(30)
        .with_max_lifetime_session_secs(60)
        .with_ping_interval_secs(-1)
        .with_ping_timeout_ms(500)
        .with_creation_cclass("pool");
    let _: u32 = config.min();
    let _: u32 = config.max();
    let _: u32 = config.increment();
    let _: u32 = config.getmode();
    let _: u32 = config.wait_timeout_ms();
    let _: u32 = config.timeout_secs();
    let _: u32 = config.max_lifetime_session_secs();
    let _: i64 = config.ping_interval_secs();
    let _: u32 = config.ping_timeout_ms();
    let _: Option<&str> = config.creation_cclass();

    let acquire = AcquireOptions::new()
        .with_wants_new(true)
        .with_cclass("custom")
        .with_optional_cclass(Some("override".into()));
    let _: bool = acquire.wants_new();
    let _: Option<&str> = acquire.cclass();

    let query = Query::new("select :1 from dual")
        .bind(vec![BindValue::Number("1".into())])
        .arraysize(std::num::NonZeroU32::new(10).expect("non-zero"))
        .prefetch(12)
        .stream_lobs()
        .scrollable()
        .timeout(Duration::from_secs(1));
    let _: &str = query.sql();
    let _: &oracledb::Params<'_> = query.params();
    let _: std::num::NonZeroU32 = query.arraysize_value();
    let _: u32 = query.prefetch_rows();
    let _: bool = query.materialize_lobs();
    let _: bool = query.is_scrollable();
    let _: Option<Duration> = query.timeout_duration();

    let execute = Execute::new("begin null; end;")
        .bind(vec![BindValue::Number("1".into())])
        .timeout(Duration::from_secs(1))
        .parse_only()
        .raw_options(exec_options);
    let _: &str = execute.sql();
    let _: &oracledb::Params<'_> = execute.params();
    let _: Option<Duration> = execute.timeout_duration();
    let _: ExecuteOptions = execute.options();

    let rows = vec![vec![BindValue::Number("1".into())]];
    let batch = Batch::new("insert into t values (:1)", &rows)
        .collect_errors()
        .row_counts()
        .timeout(Duration::from_secs(1))
        .raw_options(exec_options);
    let _: &str = batch.sql();
    let _: &oracledb::BatchRows<'_> = batch.rows();
    let _: Option<Duration> = batch.timeout_duration();
    let _: ExecuteOptions = batch.options();

    let registration = Registration::new("select * from t", 42)
        .bind(vec![BindValue::Number("1".into())])
        .timeout(Duration::from_secs(1));
    let _: &str = registration.sql();
    let _: &oracledb::Params<'_> = registration.params();
    let _: u64 = registration.registration_id();
    let _: Option<Duration> = registration.timeout_duration();

    let pipeline =
        PipelineRequest::execute("insert into t values (:1)", rows.clone(), 1);
    let _: Option<&str> = pipeline.sql();
    let _: Option<&[Vec<BindValue>]> = pipeline.bind_rows();
    let _: Option<u32> = pipeline.prefetch_rows();
    let _: bool = PipelineRequest::commit().is_commit();
}

fn blocking_pooled_connection_release_surface<B: PoolBackend>(
    guard: BlockingPooledConnection<B>,
) {
    let _: u64 = guard.id();
    let _: std::result::Result<(), PoolError> = guard.release();
}

fn blocking_pooled_connection_drop_surface<B: PoolBackend>(
    guard: BlockingPooledConnection<B>,
) {
    let _: std::result::Result<(), PoolError> = guard.drop_from_pool();
}

fn main() {}
