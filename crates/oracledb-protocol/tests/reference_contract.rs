use oracledb_protocol::{
    PYTHON_ORACLEDB_REFERENCE_COMMIT, PYTHON_ORACLEDB_REFERENCE_TAG, TNS_VERSION_DESIRED,
};

#[test]
fn reference_pin_is_part_of_the_protocol_contract() {
    assert_eq!(PYTHON_ORACLEDB_REFERENCE_TAG, "v4.0.1");
    assert_eq!(
        PYTHON_ORACLEDB_REFERENCE_COMMIT,
        "3daef052904e41668bb862e6fa40f43c22a81beb"
    );
    assert_eq!(TNS_VERSION_DESIRED, 319);
}
