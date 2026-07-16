//! Live test: precision/scale for TIMESTAMP and INTERVAL attributes within a
//! DbObject (upstream python-oracledb commit 6cfd00aa642e).
//!
//! Our `describe_object_type` is `ALL_TYPE_ATTRS`-based, so the data-dictionary
//! precision/scale for timestamp/interval attributes is available at describe
//! time. Before the fix the protocol helper `dbobject_attr_precision_scale`
//! discarded every non-NUMBER descriptor as `(0, 0)`. This test queries the
//! real `ALL_TYPE_ATTRS` rows first (including their raw short `WITH TZ`
//! spellings and actual precision/scale), then proves the same values normalize
//! through the helper used by the driver.
//!
//! Fixture (created by this test): an object type with the captured 23ai
//! timestamp and interval descriptor family. The test creates it, describes
//! it, then drops it.
//!
//! Run: PYO_TEST_CONNECT_STRING=localhost:1522/FREEPDB1 PYO_TEST_MAIN_USER=pythontest \
//!      PYO_TEST_MAIN_PASSWORD=pythontest \
//!      cargo test -p oracledb --test live_object_precision_scale -- --ignored --nocapture
use oracledb::protocol::thin::dbobject_attr_precision_scale;
use oracledb::protocol::ClientIdentity;
use oracledb::{BlockingConnection, ConnectOptions};

mod common;

#[derive(Debug, Eq, PartialEq)]
struct RawAttribute {
    name: String,
    type_name: String,
    precision: Option<i64>,
    scale: Option<i64>,
}

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

    // Fresh fixture: match the full captured 23ai descriptor family, including
    // explicit fractional-seconds precision and the raw short TZ spellings.
    exec(&mut c, "drop type ps6_stamps force");
    let create = "create type ps6_stamps as object (\
        ts_default      timestamp, \
        ts_3            timestamp(3), \
        tstz_default    timestamp with time zone, \
        tstz_3          timestamp(3) with time zone, \
        tsltz_default   timestamp with local time zone, \
        tsltz_3         timestamp(3) with local time zone, \
        ids_default     interval day to second, \
        ids_9_3         interval day(9) to second(3), \
        iym_default     interval year to month, \
        iym_9           interval year(9) to month)";
    let created = BlockingConnection::execute_raw(
        &mut c,
        create,
        0,
        &[],
        oracledb::protocol::thin::ExecuteOptions::default(),
        None,
    );
    created.expect("create type ps6_stamps");

    // The direct dictionary query does not normalize bind values the way
    // `describe_object_type` does; dictionary owner names are uppercase.
    let owner = fixture_owner().to_ascii_uppercase();
    let raw = BlockingConnection::query_all(
        &mut c,
        "select attr_name, attr_type_name, precision, scale \
         from all_type_attrs where owner = :1 and type_name = 'PS6_STAMPS' order by attr_no",
        (owner.clone(),),
    )
    .expect("query ALL_TYPE_ATTRS for ps6_stamps")
    .into_iter()
    .map(|row| RawAttribute {
        name: row.get(0).expect("ATTR_NAME"),
        type_name: row.get(1).expect("ATTR_TYPE_NAME"),
        precision: row.try_get(2).expect("PRECISION"),
        scale: row.try_get(3).expect("SCALE"),
    })
    .collect::<Vec<_>>();

    let expected = [
        ("TS_DEFAULT", "TIMESTAMP", None, Some(6), (0, 6)),
        ("TS_3", "TIMESTAMP", None, Some(3), (0, 3)),
        ("TSTZ_DEFAULT", "TIMESTAMP WITH TZ", None, Some(6), (0, 6)),
        ("TSTZ_3", "TIMESTAMP WITH TZ", None, Some(3), (0, 3)),
        (
            "TSLTZ_DEFAULT",
            "TIMESTAMP WITH LOCAL TZ",
            None,
            Some(6),
            (0, 6),
        ),
        ("TSLTZ_3", "TIMESTAMP WITH LOCAL TZ", None, Some(3), (0, 3)),
        (
            "IDS_DEFAULT",
            "INTERVAL DAY TO SECOND",
            Some(2),
            Some(6),
            (2, 6),
        ),
        (
            "IDS_9_3",
            "INTERVAL DAY TO SECOND",
            Some(9),
            Some(3),
            (9, 3),
        ),
        (
            "IYM_DEFAULT",
            "INTERVAL YEAR TO MONTH",
            Some(2),
            None,
            (2, 0),
        ),
        ("IYM_9", "INTERVAL YEAR TO MONTH", Some(9), None, (9, 0)),
    ];
    assert_eq!(
        raw.len(),
        expected.len(),
        "raw ALL_TYPE_ATTRS rows: {raw:#?}"
    );

    let ty = BlockingConnection::describe_object_type(&mut c, &owner, "ps6_stamps")
        .expect("describe ps6_stamps");

    let names: Vec<&str> = ty.attributes.iter().map(|a| a.name.as_str()).collect();
    assert_eq!(
        names,
        expected.iter().map(|(name, ..)| *name).collect::<Vec<_>>(),
        "described attribute names"
    );

    let described = |name: &str| -> &oracledb::ObjectAttribute {
        ty.attributes
            .iter()
            .find(|attribute| attribute.name == name)
            .unwrap_or_else(|| panic!("attribute {name} present"))
    };

    for (name, raw_type_name, raw_precision, raw_scale, normalized) in expected {
        let raw_attribute = raw
            .iter()
            .find(|attribute| attribute.name == name)
            .unwrap_or_else(|| panic!("raw ALL_TYPE_ATTRS row {name} present; observed {raw:#?}"));
        assert_eq!(
            (
                raw_attribute.type_name.as_str(),
                raw_attribute.precision,
                raw_attribute.scale
            ),
            (raw_type_name, raw_precision, raw_scale),
            "raw ALL_TYPE_ATTRS descriptor for {name}; observed {raw_attribute:?}"
        );
        assert_eq!(
            described(name).type_name,
            raw_type_name,
            "describe path must preserve the raw descriptor name for {name}"
        );
        assert_eq!(
            dbobject_attr_precision_scale(
                &raw_attribute.type_name,
                raw_attribute.precision.map(|value| {
                    i8::try_from(value).unwrap_or_else(|_| {
                        panic!("ALL_TYPE_ATTRS precision {value} exceeds i8 for {raw_attribute:?}")
                    })
                }),
                raw_attribute.scale.map(|value| {
                    i8::try_from(value).unwrap_or_else(|_| {
                        panic!("ALL_TYPE_ATTRS scale {value} exceeds i8 for {raw_attribute:?}")
                    })
                }),
            ),
            normalized,
            "normalization from real ALL_TYPE_ATTRS metadata for {name}; raw {raw_attribute:?}"
        );
    }

    exec(&mut c, "drop type ps6_stamps force");
    BlockingConnection::close(c).ok();
}
