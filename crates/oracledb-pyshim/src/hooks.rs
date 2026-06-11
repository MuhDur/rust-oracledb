use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};
use std::thread;

use asupersync::runtime::{reactor, Runtime, RuntimeBuilder};
use asupersync::Cx;
use oracledb::protocol::sql;
use oracledb::protocol::thin::{
    bind_template_from_type_name, bind_value_type_info, column_metadata_is_xmltype,
    cursor_bind_template, dbobject_attr_max_size, dbobject_attr_precision_scale,
    dbobject_element_bind_type_info, dbobject_rowtype_attr_max_size, decode_bfile_locator_name,
    decode_datetime_value, decode_dbobject_binary_double as protocol_decode_dbobject_binary_double,
    decode_dbobject_binary_float as protocol_decode_dbobject_binary_float,
    decode_dbobject_text as protocol_decode_dbobject_text, decode_dbobject_xmltype_text,
    decode_lob_text as protocol_decode_lob_text, decode_number_value, define_metadata_from_bind,
    encode_lob_text as protocol_encode_lob_text, is_cursor_bind_template, lob_locator_is_temporary,
    output_bind as output_only_bind, public_dbtype_name_from_bind,
    public_dbtype_name_from_column_metadata, public_dbtype_name_from_oracle_type_name,
    public_dbtype_name_from_type_name, returning_output_bind, BindValue, ColumnMetadata,
    DbObjectPackedReader, QueryResult, QueryValue, CS_FORM_IMPLICIT, CS_FORM_NCHAR,
    ORA_TYPE_NUM_BFILE, ORA_TYPE_NUM_BINARY_DOUBLE, ORA_TYPE_NUM_BINARY_INTEGER, ORA_TYPE_NUM_BLOB,
    ORA_TYPE_NUM_CLOB, ORA_TYPE_NUM_CURSOR, ORA_TYPE_NUM_NUMBER, ORA_TYPE_NUM_OBJECT,
    ORA_TYPE_NUM_RAW, ORA_TYPE_NUM_TIMESTAMP, ORA_TYPE_NUM_TIMESTAMP_LTZ,
    ORA_TYPE_NUM_TIMESTAMP_TZ, ORA_TYPE_NUM_VARCHAR,
};
use oracledb::protocol::{ClientIdentity, ProtocolError};
use oracledb::{
    BlockingConnection, CancelHandle, ConnectOptions, Connection as RustConnection,
    Error as DriverError,
};
use pyo3::exceptions::{PyIndexError, PyNotImplementedError, PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyBytesMethods, PyDict, PyList, PyString, PyTuple};

use crate::*;

pub(crate) static PASSWORD_OVERRIDES: OnceLock<Mutex<BTreeMap<String, String>>> = OnceLock::new();
pub(crate) static NEXT_CONNECT_ARGS: OnceLock<Mutex<VecDeque<ConnectArgs>>> = OnceLock::new();
pub(crate) static NEXT_CONNECT_ARGS_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Default)]
pub(crate) struct ConnectArgs {
    id: u64,
    pub(crate) password: Option<String>,
    pub(crate) new_password: Option<String>,
    pub(crate) invalid_user_dsn: bool,
}

pub(crate) fn password_overrides() -> &'static Mutex<BTreeMap<String, String>> {
    PASSWORD_OVERRIDES.get_or_init(|| Mutex::new(BTreeMap::new()))
}

pub(crate) fn next_connect_args_queue() -> &'static Mutex<VecDeque<ConnectArgs>> {
    NEXT_CONNECT_ARGS.get_or_init(|| Mutex::new(VecDeque::new()))
}

pub(crate) fn consume_next_connect_args() -> PyResult<ConnectArgs> {
    Ok(next_connect_args_queue()
        .lock()
        .map_err(runtime_error)?
        .pop_front()
        .unwrap_or_default())
}

pub(crate) fn password_override_for_user(user: &str) -> PyResult<Option<String>> {
    Ok(password_overrides()
        .lock()
        .map_err(runtime_error)?
        .get(&user.to_ascii_uppercase())
        .cloned())
}

pub(crate) fn set_password_override_for_user(user: &str, password: &str) -> PyResult<()> {
    password_overrides()
        .lock()
        .map_err(runtime_error)?
        .insert(user.to_ascii_uppercase(), password.to_string());
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (password=None, new_password=None, invalid_user_dsn=false))]
pub(crate) fn record_next_connect_args(
    password: Option<String>,
    new_password: Option<String>,
    invalid_user_dsn: bool,
) -> PyResult<u64> {
    let id = NEXT_CONNECT_ARGS_ID.fetch_add(1, Ordering::Relaxed);
    next_connect_args_queue()
        .lock()
        .map_err(runtime_error)?
        .push_back(ConnectArgs {
            id,
            password,
            new_password,
            invalid_user_dsn,
        });
    Ok(id)
}

#[pyfunction]
pub(crate) fn discard_pending_connect_args(id: u64) -> PyResult<bool> {
    let mut queue = next_connect_args_queue().lock().map_err(runtime_error)?;
    if let Some(pos) = queue.iter().position(|entry| entry.id == id) {
        queue.remove(pos);
        return Ok(true);
    }
    Ok(false)
}

pub(crate) fn env_password_for_user(user: &str) -> PyResult<String> {
    if let Some(password) = password_override_for_user(user)? {
        return Ok(password);
    }
    if let Ok(password) = std::env::var("ORACLEDB_SHIM_PASSWORD") {
        return Ok(password);
    }
    if std::env::var("PYO_TEST_MAIN_USER")
        .is_ok_and(|main_user| user.eq_ignore_ascii_case(&main_user))
    {
        return std::env::var("PYO_TEST_MAIN_PASSWORD")
            .or_else(|_| std::env::var("PYO_TEST_PASSWORD"))
            .map_err(|_| {
                PyRuntimeError::new_err(
                    "oracledb-pyshim cannot read password from ConnectParamsImpl; set PYO_TEST_MAIN_PASSWORD",
                )
            });
    }
    let proxy_user = std::env::var("PYO_TEST_PROXY_USER").unwrap_or_default();
    if !proxy_user.is_empty() && user.eq_ignore_ascii_case(&proxy_user) {
        return std::env::var("PYO_TEST_PROXY_PASSWORD")
            .or_else(|_| std::env::var("PYO_TEST_MAIN_PASSWORD"))
            .map_err(|_| {
                PyRuntimeError::new_err(
                    "oracledb-pyshim cannot read proxy password from ConnectParamsImpl; set PYO_TEST_PROXY_PASSWORD",
                )
            });
    }
    std::env::var("PYO_TEST_MAIN_PASSWORD").map_err(|_| {
        PyRuntimeError::new_err(
            "oracledb-pyshim cannot read password from ConnectParamsImpl; set ORACLEDB_SHIM_PASSWORD",
        )
    })
}
