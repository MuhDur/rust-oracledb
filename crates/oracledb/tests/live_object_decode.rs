//! Live test for structured ADT object decoding (bead vx6). Describes an object
//! type from the data dictionary, then decodes a returned object value into its
//! scalar attributes (handling NULLs).
//!
//! Fixture (created by the runner before this test):
//!   create type vx6_addr as object (street varchar2(40), zip number, ok number(1));
//!   create table vx6_people (id number, home vx6_addr);
//!   insert ... (1, vx6_addr('12 Oak St', 90210, 1)), (2, vx6_addr('  ', null, 0));
//!
//! Run: PYO_TEST_CONNECT_STRING=localhost:1522/FREEPDB1 PYO_TEST_MAIN_USER=pythontest \
//!      PYO_TEST_MAIN_PASSWORD=pythontest \
//!      cargo test -p oracledb --test live_object_decode -- --ignored --nocapture
use oracledb::protocol::thin::QueryValue;
use oracledb::protocol::ClientIdentity;
use oracledb::{decode_object, BlockingConnection, ConnectOptions};

mod common;

fn connect() -> oracledb::Connection {
    let common::LiveCreds {
        connect_string: cs,
        user,
        password: pw,
    } = common::live_creds_required();
    let id = ClientIdentity::new("objdecode", "host", "user", "term", "rust").unwrap();
    BlockingConnection::connect(ConnectOptions::new(cs, user, pw, id)).unwrap()
}

/// The schema that owns the `vx6_*` fixtures: the connecting user's own schema.
/// Kept portable across the version matrix (pythontest on free23, testuser on
/// the xe18/xe21 lanes) instead of a hard-coded owner. `describe_object_type`
/// matches the data-dictionary owner case-insensitively.
fn fixture_owner() -> String {
    std::env::var("PYO_TEST_MAIN_USER").unwrap()
}

#[test]
#[ignore]
fn describe_and_decode_simple_object() {
    let mut c = connect();

    // Describe (case-insensitive).
    let ty =
        BlockingConnection::describe_object_type(&mut c, &fixture_owner(), "vx6_addr").unwrap();
    let names: Vec<&str> = ty.attributes.iter().map(|a| a.name.as_str()).collect();
    assert_eq!(names, vec!["STREET", "ZIP", "OK"]);
    assert_eq!(ty.attributes[0].type_name, "VARCHAR2");
    assert_eq!(ty.attributes[1].type_name, "NUMBER");
    assert!(ty.attributes.iter().all(|a| a.type_owner.is_none()));

    let res = BlockingConnection::execute_raw(
        &mut c,
        "select home from vx6_people order by id",
        10,
        &[],
        oracledb::protocol::thin::ExecuteOptions::default(),
        None,
    )
    .unwrap();

    // Row 0: vx6_addr('12 Oak St', 90210, 1)
    let obj0 = match res.cell(0, 0) {
        Some(QueryValue::Object(o)) => o.as_ref(),
        other => panic!("expected an object value, got {other:?}"),
    };
    let d0 = decode_object(obj0, &ty).unwrap();
    assert_eq!(d0.type_name(), "VX6_ADDR");
    assert_eq!(d0.attributes()[0].0, "STREET");
    assert_eq!(
        d0.attributes()[0].1.as_ref().and_then(QueryValue::as_text),
        Some("12 Oak St")
    );
    assert_eq!(
        d0.attributes()[1].1.as_ref().and_then(QueryValue::as_i64),
        Some(90210)
    );
    assert_eq!(
        d0.attributes()[2].1.as_ref().and_then(QueryValue::as_i64),
        Some(1)
    );

    // Row 1: vx6_addr('  ', null, 0) — the NULL zip decodes to None.
    let obj1 = match res.cell(1, 0) {
        Some(QueryValue::Object(o)) => o.as_ref(),
        other => panic!("expected an object value, got {other:?}"),
    };
    let d1 = decode_object(obj1, &ty).unwrap();
    assert_eq!(d1.attributes()[1].1, None, "NULL attribute decodes to None");
    assert_eq!(
        d1.attributes()[2].1.as_ref().and_then(QueryValue::as_i64),
        Some(0)
    );

    BlockingConnection::close(c).ok();
}

/// Collection (VARRAY) decode: describe the element type, then decode each
/// returned collection value into its scalar elements (NULLs and empties too).
///
/// Fixture (created by the runner before this test):
///   create type vx6_nums as varray(10) of number;
///   create table vx6_coll (id number, vals vx6_nums);
///   insert ... (1, vx6_nums(10,20,30)), (2, vx6_nums(7,null,9)), (3, vx6_nums());
#[test]
#[ignore]
fn describe_and_decode_collection() {
    let mut c = connect();

    let ty =
        BlockingConnection::describe_object_type(&mut c, &fixture_owner(), "vx6_nums").unwrap();
    assert!(ty.attributes.is_empty(), "a collection has no attributes");
    let elem = ty
        .collection_element
        .as_ref()
        .expect("VX6_NUMS is a collection type, so it must carry element metadata");
    assert_eq!(elem.type_name, "NUMBER");
    assert!(elem.type_owner.is_none(), "scalar element has no owner");

    let res = BlockingConnection::execute_raw(
        &mut c,
        "select vals from vx6_coll order by id",
        10,
        &[],
        oracledb::protocol::thin::ExecuteOptions::default(),
        None,
    )
    .unwrap();

    let decode_row =
        |res: &oracledb::protocol::thin::QueryResult, row: usize| -> Vec<Option<i64>> {
            let obj = match res.cell(row, 0) {
                Some(QueryValue::Object(o)) => o.as_ref(),
                other => panic!("expected a collection object, got {other:?}"),
            };
            let d = decode_object(obj, &ty).unwrap();
            d.elements()
                .expect("a collection decodes into `elements`")
                .iter()
                .map(|e| e.as_ref().and_then(QueryValue::as_i64))
                .collect()
        };

    assert_eq!(decode_row(&res, 0), vec![Some(10), Some(20), Some(30)]);
    assert_eq!(
        decode_row(&res, 1),
        vec![Some(7), None, Some(9)],
        "NULL element decodes to None in order"
    );
    assert_eq!(
        decode_row(&res, 2),
        Vec::<Option<i64>>::new(),
        "empty varray"
    );

    BlockingConnection::close(c).ok();
}
