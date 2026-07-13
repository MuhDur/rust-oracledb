//! Live test: precision/scale for TIMESTAMP and INTERVAL attributes within a
//! DbObject (upstream python-oracledb commit 6cfd00aa642e).
//!
//! Our `describe_object_type` is `ALL_TYPE_ATTRS`-based, so the data-dictionary
//! precision/scale for timestamp/interval attributes is available at describe
//! time; the protocol helper `dbobject_attr_precision_scale` is what the pyshim
//! reports to python-oracledb's DbObject metadata. Before the fix that helper
//! discarded precision/scale for every non-NUMBER type (the `_ => (0, 0)` arm),
//! so a TIMESTAMP attribute reported (0, 0) instead of (0, 6). This test
//! describes a real object type carrying each timestamp/interval variant and
//! asserts the reported precision & scale via the same helper the driver uses.
//!
//! Fixture (created by this test): an object type with the five attribute
//! variants below. The test creates it, describes it, then drops it.
//!
//! Run: PYO_TEST_CONNECT_STRING=localhost:1522/FREEPDB1 PYO_TEST_MAIN_USER=pythontest \
//!      PYO_TEST_MAIN_PASSWORD=pythontest \
//!      cargo test -p oracledb --test live_object_precision_scale -- --ignored --nocapture
use oracledb::protocol::thin::dbobject_attr_precision_scale;
use oracledb::protocol::ClientIdentity;
use oracledb::{BlockingConnection, ConnectOptions};

mod common;

fn connect() -> oracledb::Connection {
    let common::LiveCreds {
        connect_string: cs,
        user,
        password: pw,
    } = common::live_creds_required();
    let id = ClientIdentity::new("objprecscale", "host", "user", "term", "rust")
        .expect("client identity");
    BlockingConnection::connect(ConnectOptions::new(cs, user, pw, id)).expect("connect")
}

fn fixture_owner() -> String {
    std::env::var("PYO_TEST_MAIN_USER").expect("PYO_TEST_MAIN_USER")
}

fn exec(c: &mut oracledb::Connection, sql: &str) {
    let _ = BlockingConnection::execute_raw(
        c,
        sql,
        0,
        &[],
        oracledb::protocol::thin::ExecuteOptions::default(),
        None,
    );
}

#[test]
#[ignore]
fn describe_timestamp_and_interval_precision_scale() {
    let mut c = connect();

    // Fresh fixture: drop-if-exists, then create the object type.
    exec(&mut c, "drop type ps6_stamps force");
    let create = "create type ps6_stamps as object (\
        ts      timestamp(6), \
        ts_tz   timestamp with time zone, \
        ts_ltz  timestamp with local time zone, \
        ids     interval day(2) to second(6), \
        iym     interval year(2) to month)";
    let created = BlockingConnection::execute_raw(
        &mut c,
        create,
        0,
        &[],
        oracledb::protocol::thin::ExecuteOptions::default(),
        None,
    );
    created.expect("create type ps6_stamps");

    let ty = BlockingConnection::describe_object_type(&mut c, &fixture_owner(), "ps6_stamps")
        .expect("describe ps6_stamps");

    let names: Vec<&str> = ty.attributes.iter().map(|a| a.name.as_str()).collect();
    assert_eq!(names, vec!["TS", "TS_TZ", "TS_LTZ", "IDS", "IYM"]);

    // The describe path exposes the Oracle attribute type name; feed each through
    // the same helper the driver/pyshim uses to report precision & scale. This
    // exercises the whole live describe -> type_name -> precision/scale path.
    let by_name = |n: &str| -> &oracledb::ObjectAttribute {
        ty.attributes
            .iter()
            .find(|a| a.name == n)
            .unwrap_or_else(|| panic!("attribute {n} present"))
    };

    // TIMESTAMP(6): precision 0, scale 6 (fractional seconds).
    assert_eq!(
        dbobject_attr_precision_scale(&by_name("TS").type_name, None, None),
        (0, 6),
        "TIMESTAMP"
    );
    // TIMESTAMP WITH TIME ZONE: precision 0, scale 6.
    assert_eq!(
        dbobject_attr_precision_scale(&by_name("TS_TZ").type_name, None, None),
        (0, 6),
        "TIMESTAMP WITH TIME ZONE"
    );
    // TIMESTAMP WITH LOCAL TIME ZONE: precision 0, scale 6.
    assert_eq!(
        dbobject_attr_precision_scale(&by_name("TS_LTZ").type_name, None, None),
        (0, 6),
        "TIMESTAMP WITH LOCAL TIME ZONE"
    );
    // INTERVAL DAY(2) TO SECOND(6): precision 2, scale 6.
    assert_eq!(
        dbobject_attr_precision_scale(&by_name("IDS").type_name, None, None),
        (2, 6),
        "INTERVAL DAY TO SECOND"
    );
    // INTERVAL YEAR(2) TO MONTH: precision 2, scale 0.
    assert_eq!(
        dbobject_attr_precision_scale(&by_name("IYM").type_name, None, None),
        (2, 0),
        "INTERVAL YEAR TO MONTH"
    );

    exec(&mut c, "drop type ps6_stamps force");
    BlockingConnection::close(c).ok();
}
