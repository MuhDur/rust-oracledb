//! Tests for `#[derive(FromRow)]` (bead 4bv).
//!
//! Two layers:
//!
//! * **Synthetic, no DB** — construct a [`QueryResult`] by hand and prove the
//!   derived `from_row` / `rows_as` map it correctly through the real
//!   [`FromSql`] conversion, including `Option<T>` NULL handling, by-name
//!   mapping, `#[oracledb(column = ...)]` / `rename_all`, and tuple structs.
//!   These run on a plain `cargo test` with no container.
//! * **Live** — against the real container (self-skips when the `PYO_TEST_*`
//!   environment is absent): create a scratch table, insert rows including a
//!   NULL column and a `chrono` DATE, `select`, and map straight into a derived
//!   struct via `rows_as`, asserting exact values.

use oracledb::protocol::thin::{ColumnMetadata, QueryResult, QueryValue};
use oracledb::{FromRow, QueryResultExt};

// ---------------------------------------------------------------------------
// Synthetic-row helpers (no DB)
// ---------------------------------------------------------------------------

fn col(name: &str) -> ColumnMetadata {
    ColumnMetadata::new(name, 0)
}

fn num(text: &str) -> QueryValue {
    QueryValue::number_from_text(text, !text.contains('.'))
}

fn text(value: &str) -> QueryValue {
    QueryValue::Text(value.to_string())
}

/// Build a `QueryResult` from column names and a grid of optional cells.
fn synthetic(columns: &[&str], rows: Vec<Vec<Option<QueryValue>>>) -> QueryResult {
    QueryResult {
        columns: columns.iter().map(|c| col(c)).collect(),
        rows,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Derived structs under test
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, FromRow)]
struct Emp {
    id: i64,
    name: String,
    // nullable column -> Option<T>; a NULL cell becomes None
    manager_id: Option<i64>,
}

#[derive(Debug, PartialEq, FromRow)]
#[oracledb(rename_all = "SCREAMING_SNAKE_CASE")]
struct Renamed {
    employee_id: i64,
    full_name: String,
}

#[derive(Debug, PartialEq, FromRow)]
struct Overridden {
    #[oracledb(column = "EMPNO")]
    id: i64,
    #[oracledb(rename = "ENAME")]
    name: String,
}

#[derive(Debug, PartialEq, FromRow)]
struct Pair(i64, String);

// ---------------------------------------------------------------------------
// Synthetic tests
// ---------------------------------------------------------------------------

#[test]
fn maps_named_struct_by_column_name() {
    let result = synthetic(
        &["ID", "NAME", "MANAGER_ID"],
        vec![
            vec![Some(num("1")), Some(text("alice")), Some(num("7"))],
            // NULL manager_id in the second row
            vec![Some(num("2")), Some(text("bob")), None],
        ],
    );

    let emps: Vec<Emp> = result.rows_as::<Emp>().expect("map rows");
    assert_eq!(
        emps,
        vec![
            Emp {
                id: 1,
                name: "alice".to_string(),
                manager_id: Some(7),
            },
            Emp {
                id: 2,
                name: "bob".to_string(),
                manager_id: None,
            },
        ]
    );
}

#[test]
fn from_row_on_a_single_typed_row() {
    let result = synthetic(
        &["ID", "NAME", "MANAGER_ID"],
        vec![vec![Some(num("42")), Some(text("carol")), None]],
    );
    let emp = Emp::from_row(&result.typed_row(0)).expect("from_row");
    assert_eq!(
        emp,
        Emp {
            id: 42,
            name: "carol".to_string(),
            manager_id: None,
        }
    );
}

#[test]
fn column_order_is_irrelevant_mapping_is_by_name() {
    // Columns in a different order than the struct fields: by-name mapping wins.
    let result = synthetic(
        &["NAME", "MANAGER_ID", "ID"],
        vec![vec![Some(text("dave")), Some(num("9")), Some(num("3"))]],
    );
    let emps = result.rows_as::<Emp>().expect("map rows");
    assert_eq!(
        emps[0],
        Emp {
            id: 3,
            name: "dave".to_string(),
            manager_id: Some(9),
        }
    );
}

#[test]
fn rename_all_screaming_snake_case() {
    // struct fields employee_id / full_name -> columns EMPLOYEE_ID / FULL_NAME
    let result = synthetic(
        &["EMPLOYEE_ID", "FULL_NAME"],
        vec![vec![Some(num("11")), Some(text("erin"))]],
    );
    let rows = result.rows_as::<Renamed>().expect("map rows");
    assert_eq!(
        rows[0],
        Renamed {
            employee_id: 11,
            full_name: "erin".to_string(),
        }
    );
}

#[test]
fn per_field_column_override() {
    let result = synthetic(
        &["EMPNO", "ENAME"],
        vec![vec![Some(num("7369")), Some(text("smith"))]],
    );
    let rows = result.rows_as::<Overridden>().expect("map rows");
    assert_eq!(
        rows[0],
        Overridden {
            id: 7369,
            name: "smith".to_string(),
        }
    );
}

#[test]
fn tuple_struct_maps_by_position() {
    let result = synthetic(
        &["WHATEVER", "ALSO_WHATEVER"],
        vec![vec![Some(num("5")), Some(text("frank"))]],
    );
    let rows = result.rows_as::<Pair>().expect("map rows");
    assert_eq!(rows[0], Pair(5, "frank".to_string()));
}

#[test]
fn missing_column_is_a_conversion_error() {
    // No MANAGER_ID column at all -> OutOfRange (not a silent None).
    let result = synthetic(&["ID", "NAME"], vec![vec![Some(num("1")), Some(text("x"))]]);
    let err = result.rows_as::<Emp>().expect_err("must fail");
    let msg = err.to_string();
    assert!(
        msg.to_uppercase().contains("MANAGER_ID"),
        "error should name the missing column, got: {msg}"
    );
}

#[test]
fn null_in_a_non_optional_field_is_unexpected_null() {
    // NAME is NULL but the struct field is `String`, not `Option<String>`.
    let result = synthetic(
        &["ID", "NAME", "MANAGER_ID"],
        vec![vec![Some(num("1")), None, Some(num("2"))]],
    );
    let err = result.rows_as::<Emp>().expect_err("must fail");
    assert!(
        matches!(err, oracledb::Error::Conversion(_)),
        "expected a Conversion error, got: {err}"
    );
}

#[test]
fn empty_result_maps_to_empty_vec() {
    let result = synthetic(&["ID", "NAME", "MANAGER_ID"], vec![]);
    let emps = result.rows_as::<Emp>().expect("map rows");
    assert!(emps.is_empty());
}

// ---------------------------------------------------------------------------
// Live integration test
// ---------------------------------------------------------------------------

#[cfg(feature = "chrono")]
mod live {
    use chrono::NaiveDate;
    use oracledb::protocol::thin::{ExecuteOptions, QueryResult};
    use oracledb::{BlockingConnection, ConnectOptions, Connection, FromRow, QueryResultExt};
    use oracledb_protocol::ClientIdentity;

    const PROGRAM: &str = "rust-oracledb-fromrow-itest";
    const MACHINE: &str = "itest-machine";
    const OSUSER: &str = "itest-osuser";
    const TERMINAL: &str = "itest-terminal";
    const DRIVER: &str = "rust-oracledb thn : 0.0.0";

    #[derive(Debug, PartialEq, FromRow)]
    struct Emp {
        id: i64,
        name: String,
        // nullable DATE column + nullable manager column exercise Option<T>
        hired: Option<NaiveDate>,
        manager_id: Option<i64>,
    }

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

    fn execute_raw(
        conn: &mut Connection,
        sql: &str,
        prefetch_rows: u32,
    ) -> oracledb::Result<QueryResult> {
        BlockingConnection::execute_raw(
            conn,
            sql,
            prefetch_rows,
            &[],
            ExecuteOptions::default(),
            None,
        )
    }

    #[test]
    fn derive_from_row_live_roundtrip() {
        let Some(options) = connect_options() else {
            eprintln!("skipped derive_from_row_live_roundtrip: PYO_TEST_* not configured");
            return;
        };
        let mut conn = BlockingConnection::connect(options).expect("connect to test container");

        let _ = execute_raw(&mut conn, "drop table fromrow_emp_t", 1);
        execute_raw(
            &mut conn,
            "create table fromrow_emp_t (id number, name varchar2(40), hired date, manager_id number)",
            1,
        )
        .expect("create table");

        // Row 1: fully populated. Row 2: NULL hired AND NULL manager_id.
        execute_raw(
            &mut conn,
            "insert into fromrow_emp_t values (1, 'alice', DATE '2021-03-15', 7)",
            1,
        )
        .expect("insert row 1");
        execute_raw(
            &mut conn,
            "insert into fromrow_emp_t values (2, 'bob', NULL, NULL)",
            1,
        )
        .expect("insert row 2");
        execute_raw(&mut conn, "commit", 1).expect("commit");

        let result = execute_raw(
            &mut conn,
            "select id, name, hired, manager_id from fromrow_emp_t order by id",
            10,
        )
        .expect("select rows");

        // The headline ergonomic: a whole result set into Vec<Emp> in one call.
        let emps: Vec<Emp> = result.rows_as::<Emp>().expect("map rows into Vec<Emp>");
        eprintln!("derive live: {emps:?}");

        assert_eq!(
            emps,
            vec![
                Emp {
                    id: 1,
                    name: "alice".to_string(),
                    hired: Some(NaiveDate::from_ymd_opt(2021, 3, 15).unwrap()),
                    manager_id: Some(7),
                },
                Emp {
                    id: 2,
                    name: "bob".to_string(),
                    hired: None,
                    manager_id: None,
                },
            ],
            "derived FromRow must map real DB values, incl. NULL -> None"
        );

        let _ = execute_raw(&mut conn, "drop table fromrow_emp_t", 1);
        BlockingConnection::close(conn).expect("close connection");
    }
}
