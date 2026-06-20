//! Live integration tests for the typed read/write surface (beads qxn + zjd).
//!
//! Exercises [`FromSql`] / [`QueryResultExt::get`] (typed extraction), [`ToSql`]
//! / the [`params!`] macro / `query` / `query_named` (ergonomic binds), and the
//! LOSSLESS `rust_decimal::Decimal` NUMBER round trip against the real
//! container.
//!
//! Self-skips cleanly when the container environment is absent. Run with:
//!
//! ```sh
//! eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1525 \
//!         ORACLEDB_HOST_PORT=1525 scripts/container.sh env)"
//! cargo test -p oracledb --test live_typed \
//!   --features "chrono uuid serde_json rust_decimal" -- --nocapture
//! ```

use std::num::NonZeroU32;

use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::Cx;
use oracledb::{
    params, BlockingConnection, ConnectOptions, Connection, Error, Query, QueryResultExt,
};
use oracledb_protocol::ClientIdentity;

const PROGRAM: &str = "rust-oracledb-typed-itest";
const MACHINE: &str = "itest-machine";
const OSUSER: &str = "itest-osuser";
const TERMINAL: &str = "itest-terminal";
const DRIVER: &str = "rust-oracledb thn : 0.0.0";

fn connect_options() -> Option<ConnectOptions> {
    let connect_string = std::env::var("PYO_TEST_CONNECT_STRING").ok()?;
    let user = std::env::var("PYO_TEST_MAIN_USER").ok()?;
    let password = std::env::var("PYO_TEST_MAIN_PASSWORD").ok()?;
    let identity = ClientIdentity::new(PROGRAM, MACHINE, OSUSER, TERMINAL, DRIVER).ok()?;
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

/// `query` with a positional tuple of typed Rust values, then typed `get`.
#[test]
fn query_positional_tuple_and_typed_get() {
    with_connection("query_positional_tuple_and_typed_get", |conn| {
        // (40, 2) binds :1, :2 — no manual BindValue::Number any more.
        let result = BlockingConnection::query(conn, "select :1 + :2 from dual", (40_i64, 2_i64))
            .expect("query with tuple binds");
        let sum: i64 = result.get(0, 0).expect("typed get i64");
        assert_eq!(sum, 42);

        // mixed-type tuple: number + string, read back by typed accessors
        let result = BlockingConnection::query(
            conn,
            "select :1 as id, :2 as name from dual",
            (7_i64, "alice"),
        )
        .expect("mixed tuple binds");
        assert_eq!(result.get::<i64>(0, 0).unwrap(), 7);
        assert_eq!(result.get_by_name::<String>(0, "NAME").unwrap(), "alice");
        eprintln!(
            "positional ok: id={} name={}",
            result.get::<i64>(0, 0).unwrap(),
            result.get_by_name::<String>(0, "name").unwrap()
        );
    });
}

/// `params!` positional form feeds `query` just like a tuple.
#[test]
fn params_macro_positional() {
    with_connection("params_macro_positional", |conn| {
        let result = BlockingConnection::query(
            conn,
            "select :1 + :2 + :3 from dual",
            params![10_i64, 20_i64, 12_i64],
        )
        .expect("params! positional");
        assert_eq!(result.get::<i64>(0, 0).unwrap(), 42);
    });
}

/// `query_named` with `params!{ ":a" => .., ":b" => .. }` — the names are
/// reordered to placeholder first-appearance order, so swapping the param order
/// still binds correctly.
#[test]
fn query_named_reorders_correctly() {
    with_connection("query_named_reorders_correctly", |conn| {
        // :a appears first in the SQL; pass the params in the opposite order to
        // prove the reorder. 100 - 1 = 99 (not 1 - 100).
        let result = BlockingConnection::query_named(
            conn,
            "select :a - :b as diff from dual",
            params! { ":b" => 1_i64, ":a" => 100_i64 },
        )
        .expect("named binds");
        let diff: i64 = result.get_by_name(0, "DIFF").unwrap();
        assert_eq!(diff, 99, "named binds must map by name, not order given");
        eprintln!("named ok: diff={diff}");
    });
}

#[test]
fn async_query_family_eager_drains_and_checks_cardinality() {
    let Some(options) = connect_options() else {
        eprintln!("skipped async_query_family_eager_drains_and_checks_cardinality: PYO_TEST_* environment not configured");
        return;
    };
    let reactor = reactor::create_reactor().expect("native reactor should build for live I/O");
    let runtime = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("current-thread Asupersync runtime should build");

    runtime.block_on(async {
        let cx = Cx::current().expect("Runtime::block_on should install an ambient Cx");
        let mut conn = Connection::connect(&cx, options)
            .await
            .expect("connect to test container");

        let rows = conn
            .query_with(
                &cx,
                Query::new("select level as n from dual connect by level <= 105")
                    .arraysize(NonZeroU32::new(25).expect("non-zero"))
                    .prefetch(25),
            )
            .await
            .expect("query_with")
            .collect(&cx)
            .await
            .expect("collect");
        assert_eq!(rows.len(), 105);
        assert_eq!(rows[0].get_by_name::<i64>("N").unwrap(), 1);
        assert_eq!(rows[104].get::<i64>(0).unwrap(), 105);

        {
            let mut streamed = conn
                .query_with(
                    &cx,
                    Query::new("select level as n from dual connect by level <= 105")
                        .arraysize(NonZeroU32::new(25).expect("non-zero"))
                        .prefetch(25),
                )
                .await
                .expect("streamed query_with");
            let mut seen = Vec::new();
            loop {
                seen.extend(
                    streamed
                        .batch()
                        .iter()
                        .map(|row| row.get_by_name::<i64>("N").unwrap()),
                );
                if !streamed.next_batch(&cx).await.expect("next_batch") {
                    break;
                }
            }
            assert_eq!(seen.len(), 105);
            assert_eq!(seen[104], 105);
        }

        let all = conn
            .query_all(
                &cx,
                "select level as n from dual connect by level <= 105",
                (),
            )
            .await
            .expect("query_all eager drain");
        assert_eq!(all.len(), 105);

        let one = conn
            .query_one(&cx, "select :1 + :2 as n from dual", (40_i64, 2_i64))
            .await
            .expect("query_one");
        assert_eq!(one.get_by_name::<i64>("N").unwrap(), 42);

        let opt = conn
            .query_opt(&cx, "select 1 as n from dual where 1 = 0", ())
            .await
            .expect("query_opt none");
        assert!(opt.is_none());

        let err = conn
            .query_one(&cx, "select level as n from dual connect by level <= 2", ())
            .await
            .expect_err("query_one must reject >1 row");
        assert!(matches!(err, Error::TooManyRows));

        conn.close(&cx).await.expect("close connection");
    });
}

/// Typed extraction of several scalar types out of one row.
#[test]
fn typed_extraction_scalars() {
    with_connection("typed_extraction_scalars", |conn| {
        let result = BlockingConnection::execute_query(
            conn,
            "select 42 as n, 2.5 as d, 'hello' as s from dual",
            1,
        )
        .expect("scalar select");
        let row = result.typed_row(0);
        assert_eq!(row.get::<i64>(0).unwrap(), 42);
        assert_eq!(row.get::<f64>(1).unwrap(), 2.5);
        assert_eq!(row.get::<String>(2).unwrap(), "hello");
        assert_eq!(row.get_by_name::<i32>("N").unwrap(), 42);
    });
}

/// The LOSSLESS Decimal proof against the real database: bind a high-precision
/// Decimal, store it in a NUMBER column, read it back, and assert it is exactly
/// equal — no float rounding anywhere on the wire round trip.
#[cfg(feature = "rust_decimal")]
#[test]
fn decimal_roundtrip_lossless_live() {
    use rust_decimal::Decimal;
    use std::str::FromStr;

    with_connection("decimal_roundtrip_lossless_live", |conn| {
        let _ = BlockingConnection::execute_query(conn, "drop table dec_rt_t", 1);
        BlockingConnection::execute_query(conn, "create table dec_rt_t (v number)", 1)
            .expect("create dec table");

        // 28 significant digits — the full precision rust_decimal can hold.
        let text = "7922816251426433759354.395033";
        let dec = Decimal::from_str(text).unwrap();

        // bind the Decimal directly via ToSql (query / params!)
        BlockingConnection::query(conn, "insert into dec_rt_t values (:1)", (dec,))
            .expect("insert decimal");
        BlockingConnection::execute_query(conn, "commit", 1).expect("commit");

        let result =
            BlockingConnection::execute_query(conn, "select v from dec_rt_t", 1).expect("select");
        let back: Decimal = result.get(0, 0).expect("typed get Decimal");
        eprintln!("decimal lossless: in={dec} out={back}");
        assert_eq!(back, dec, "Decimal must round-trip exactly through NUMBER");
        assert_eq!(back.to_string(), text, "all 28 digits preserved");

        // And the canonical NUMBER text (synthesized from the inline form via
        // the shared formatter) is byte-exact.
        if let Some(cell @ QueryValue::Number(_)) = result.cell(0, 0) {
            assert_eq!(
                cell.as_number_text().unwrap(),
                text,
                "canonical NUMBER text is byte-exact"
            );
        } else {
            panic!("expected a NUMBER cell");
        }

        let _ = BlockingConnection::execute_query(conn, "drop table dec_rt_t", 1);
    });
}

/// chrono NaiveDate / NaiveDateTime bind + extract against a real DATE column.
#[cfg(feature = "chrono")]
#[test]
fn chrono_roundtrip_live() {
    use chrono::{NaiveDate, NaiveDateTime};

    with_connection("chrono_roundtrip_live", |conn| {
        let _ = BlockingConnection::execute_query(conn, "drop table chrono_rt_t", 1);
        BlockingConnection::execute_query(conn, "create table chrono_rt_t (d date)", 1)
            .expect("create chrono table");

        let dt = NaiveDate::from_ymd_opt(2026, 6, 14)
            .unwrap()
            .and_hms_opt(13, 45, 30)
            .unwrap();
        BlockingConnection::query(conn, "insert into chrono_rt_t values (:1)", (dt,))
            .expect("insert datetime");
        BlockingConnection::execute_query(conn, "commit", 1).expect("commit");

        let result = BlockingConnection::execute_query(conn, "select d from chrono_rt_t", 1)
            .expect("select date");
        let back: NaiveDateTime = result.get(0, 0).expect("typed get NaiveDateTime");
        eprintln!("chrono roundtrip: in={dt} out={back}");
        assert_eq!(back, dt, "DATE must round-trip to the second");
        // and as a bare date
        let date: NaiveDate = result.get(0, 0).expect("typed get NaiveDate");
        assert_eq!(date, NaiveDate::from_ymd_opt(2026, 6, 14).unwrap());

        let _ = BlockingConnection::execute_query(conn, "drop table chrono_rt_t", 1);
    });
}

/// uuid bind as RAW(16) + extract back.
#[cfg(feature = "uuid")]
#[test]
fn uuid_roundtrip_live() {
    use uuid::Uuid;

    with_connection("uuid_roundtrip_live", |conn| {
        let _ = BlockingConnection::execute_query(conn, "drop table uuid_rt_t", 1);
        BlockingConnection::execute_query(conn, "create table uuid_rt_t (id raw(16))", 1)
            .expect("create uuid table");

        let id = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        BlockingConnection::query(conn, "insert into uuid_rt_t values (:1)", (id,))
            .expect("insert uuid");
        BlockingConnection::execute_query(conn, "commit", 1).expect("commit");

        let result = BlockingConnection::execute_query(conn, "select id from uuid_rt_t", 1)
            .expect("select uuid");
        let back: Uuid = result.get(0, 0).expect("typed get Uuid");
        eprintln!("uuid roundtrip: in={id} out={back}");
        assert_eq!(back, id, "RAW(16) must round-trip the UUID");

        let _ = BlockingConnection::execute_query(conn, "drop table uuid_rt_t", 1);
    });
}

/// serde_json::Value extracted from a native JSON column (the eager OSON tree
/// converts near-free).
#[cfg(feature = "serde_json")]
#[test]
fn serde_json_from_native_json_live() {
    use serde_json::json;

    with_connection("serde_json_from_native_json_live", |conn| {
        let _ = BlockingConnection::execute_query(conn, "drop table json_rt_t", 1);
        // 23ai native JSON type
        if BlockingConnection::execute_query(conn, "create table json_rt_t (doc json)", 1).is_err()
        {
            eprintln!("skipped serde_json test: native JSON type unavailable");
            return;
        }

        BlockingConnection::execute_query(
            conn,
            "insert into json_rt_t values (json('{\"id\": 7, \"name\": \"bob\", \"tags\": [\"a\", \"b\"]}'))",
            1,
        )
        .expect("insert json");
        BlockingConnection::execute_query(conn, "commit", 1).expect("commit");

        // JSON streams through a client-side define; use execute_query_collect.
        let result =
            BlockingConnection::execute_query_collect(conn, "select doc from json_rt_t", 1)
                .expect("select json");
        let value: serde_json::Value = result.get(0, 0).expect("typed get serde_json::Value");
        eprintln!("serde_json from native JSON: {value}");
        assert_eq!(value["id"], json!(7));
        assert_eq!(value["name"], json!("bob"));
        assert_eq!(value["tags"], json!(["a", "b"]));

        let _ = BlockingConnection::execute_query(conn, "drop table json_rt_t", 1);
    });
}

/// Vec<f32> extracted from a VECTOR column, and bound back via ToSql.
#[test]
fn vector_roundtrip_live() {
    with_connection("vector_roundtrip_live", |conn| {
        let _ = BlockingConnection::execute_query(conn, "drop table vec_rt_t", 1);
        if BlockingConnection::execute_query(
            conn,
            "create table vec_rt_t (embedding vector(3, float32))",
            1,
        )
        .is_err()
        {
            eprintln!("skipped vector test: VECTOR type unavailable");
            return;
        }

        let embedding: Vec<f32> = vec![1.5, -2.0, 3.25];
        BlockingConnection::query(
            conn,
            "insert into vec_rt_t values (:1)",
            (embedding.clone(),),
        )
        .expect("insert vector");
        BlockingConnection::execute_query(conn, "commit", 1).expect("commit");

        // VECTOR streams through a client-side define; use execute_query_collect.
        let result =
            BlockingConnection::execute_query_collect(conn, "select embedding from vec_rt_t", 1)
                .expect("select vector");
        let back: Vec<f32> = result.get(0, 0).expect("typed get Vec<f32>");
        eprintln!("vector roundtrip: in={embedding:?} out={back:?}");
        assert_eq!(back, embedding, "VECTOR(float32) must round-trip exactly");

        let _ = BlockingConnection::execute_query(conn, "drop table vec_rt_t", 1);
    });
}
