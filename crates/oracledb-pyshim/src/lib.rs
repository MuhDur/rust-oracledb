#![forbid(unsafe_code)]

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};
use std::thread;

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
use oracledb::protocol::ClientIdentity;
use oracledb::{BlockingConnection, CancelHandle, ConnectOptions, Connection as RustConnection};
use pyo3::exceptions::{PyIndexError, PyNotImplementedError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyBytesMethods, PyDict, PyList, PyString, PyTuple};

fn not_implemented(name: &str) -> PyErr {
    PyNotImplementedError::new_err(format!(
        "{name} is a Rust shim placeholder; M1+ must route this through the oracledb crate"
    ))
}

fn runtime_error(err: impl std::fmt::Display) -> PyErr {
    let message = err.to_string();
    if let Some(server_message) = message.strip_prefix("server returned Oracle error: ") {
        return Python::attach(|py| database_error(py, server_message))
            .unwrap_or_else(|_| PyRuntimeError::new_err(message));
    }
    match message.as_str() {
        "connection is closed" => return raise_oracledb_driver_error("ERR_NOT_CONNECTED"),
        "TTC decode failed: truncated DML RETURNING value" => return raise_column_truncated(),
        "TTC decode failed: NUMBER bind out of range" => {
            return raise_oracledb_driver_error("ERR_ORACLE_NUMBER_NO_REPR");
        }
        "TTC decode failed: invalid NUMBER bind" => {
            return raise_oracledb_driver_error("ERR_INVALID_NUMBER");
        }
        "TTC decode failed: invalid NUMBER bind suffix" => {
            return raise_oracledb_driver_error("ERR_CONTENT_INVALID_AFTER_NUMBER");
        }
        "TTC decode failed: invalid NUMBER exponent" => {
            return raise_oracledb_driver_error("ERR_NUMBER_WITH_INVALID_EXPONENT");
        }
        "TTC decode failed: empty NUMBER exponent" => {
            return raise_oracledb_driver_error("ERR_NUMBER_WITH_EMPTY_EXPONENT");
        }
        "TTC decode failed: NUMBER bind text too long" => {
            return raise_oracledb_driver_error("ERR_NUMBER_STRING_TOO_LONG");
        }
        "TTC decode failed: empty NUMBER bind" => {
            return raise_oracledb_driver_error("ERR_NUMBER_STRING_OF_ZERO_LENGTH");
        }
        _ => {}
    }
    if let Some(timeout_text) = message
        .strip_prefix("call timeout of ")
        .and_then(|value| value.strip_suffix(" ms exceeded"))
    {
        if let Ok(timeout) = timeout_text.parse::<u32>() {
            return raise_call_timeout_exceeded(timeout);
        }
    }
    PyRuntimeError::new_err(message)
}

fn connection_closed_error() -> PyErr {
    raise_oracledb_driver_error("ERR_NOT_CONNECTED")
}

struct BlockingTaskState<T> {
    result: Option<Result<T, String>>,
    waker: Option<Waker>,
}

struct BlockingTask<T> {
    shared: Arc<Mutex<BlockingTaskState<T>>>,
}

impl<T> BlockingTask<T> {
    fn ready(result: Result<T, String>) -> Self {
        Self {
            shared: Arc::new(Mutex::new(BlockingTaskState {
                result: Some(result),
                waker: None,
            })),
        }
    }
}

impl<T> Future for BlockingTask<T> {
    type Output = Result<T, String>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut shared = match self.shared.lock() {
            Ok(shared) => shared,
            Err(err) => return Poll::Ready(Err(err.to_string())),
        };
        if let Some(result) = shared.result.take() {
            Poll::Ready(result)
        } else {
            shared.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

fn spawn_blocking_task<T, F>(name: &'static str, task: F) -> BlockingTask<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    let shared = Arc::new(Mutex::new(BlockingTaskState {
        result: None,
        waker: None,
    }));
    let thread_shared = Arc::clone(&shared);
    let spawn_result = thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            let result = task();
            let waker = match thread_shared.lock() {
                Ok(mut shared) => {
                    shared.result = Some(result);
                    shared.waker.take()
                }
                Err(_) => None,
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        });
    match spawn_result {
        Ok(_) => BlockingTask { shared },
        Err(err) => BlockingTask::ready(Err(format!("failed to spawn blocking task: {err}"))),
    }
}

fn close_connection_result(connection: RustConnection) -> Result<(), String> {
    BlockingConnection::close(connection).map_err(|err| err.to_string())
}

fn close_result_to_py(result: Result<(), String>) -> PyResult<()> {
    match result {
        Ok(()) => Ok(()),
        Err(err)
            if err.contains("Broken pipe")
                || err.contains("Transport endpoint is not connected") =>
        {
            Ok(())
        }
        Err(err) => Err(runtime_error(err)),
    }
}

fn parse_ora_code(message: &str) -> Option<i32> {
    let start = message.find("ORA-")? + "ORA-".len();
    let digits = message.get(start..start + 5)?;
    digits
        .chars()
        .all(|ch| ch.is_ascii_digit())
        .then(|| digits.parse::<i32>().ok())
        .flatten()
}

fn parse_ora_offset(message: &str) -> Option<i32> {
    let column_start = message.find(", column ")? + ", column ".len();
    let digits = message
        .get(column_start..)?
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    let column = digits.parse::<i32>().ok()?;
    Some(column.saturating_sub(1))
}

fn database_error(py: Python<'_>, message: &str) -> PyResult<PyErr> {
    let errors = PyModule::import(py, "oracledb.errors")?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("message", message)?;
    if let Some(code) = parse_ora_code(message) {
        kwargs.set_item("code", code)?;
    }
    if let Some(offset) = parse_ora_offset(message) {
        kwargs.set_item("offset", offset)?;
    }
    let error_obj = errors.getattr("_Error")?.call((), Some(&kwargs))?;
    let exc_type = error_obj.getattr("exc_type")?;
    let exc = exc_type.call1((error_obj,))?;
    Ok(PyErr::from_value(exc))
}

fn compilation_error_warning(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let errors = PyModule::import(py, "oracledb.errors")?;
    Ok(errors.getattr("_create_warning")?.call1((7000,))?.unbind())
}

fn query_result_warning(py: Python<'_>, result: &QueryResult) -> PyResult<Option<Py<PyAny>>> {
    result
        .compilation_error_warning
        .then(|| compilation_error_warning(py))
        .transpose()
}

fn dpy_database_error(code: &str, message: &str) -> PyErr {
    Python::attach(|py| database_error(py, &format!("{code}: {message}")))
        .unwrap_or_else(|_| PyRuntimeError::new_err(format!("{code}: {message}")))
}

fn ora_database_error(message: &str) -> PyErr {
    Python::attach(|py| database_error(py, message))
        .unwrap_or_else(|_| PyRuntimeError::new_err(message.to_string()))
}

fn ora_cancel_error() -> PyErr {
    ora_database_error("ORA-01013: user requested cancel of current operation")
}

fn dpy_bind_error(code: &str, message: impl std::fmt::Display) -> PyErr {
    dpy_database_error(code, &message.to_string())
}

fn raise_column_truncated() -> PyErr {
    Python::attach(|py| -> PyResult<PyErr> {
        let errors = PyModule::import(py, "oracledb.errors")?;
        let error_num = errors.getattr("ERR_COLUMN_TRUNCATED")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("col_value_len", 0)?;
        kwargs.set_item("unit", "characters")?;
        kwargs.set_item("actual_len", 0)?;
        match errors
            .getattr("_raise_err")?
            .call((error_num,), Some(&kwargs))
        {
            Ok(_) => Ok(PyRuntimeError::new_err(
                "oracledb.errors._raise_err(ERR_COLUMN_TRUNCATED) returned without raising",
            )),
            Err(err) => Ok(err),
        }
    })
    .unwrap_or_else(|_| {
        PyRuntimeError::new_err("DPY-4002: column truncated to 0 characters. Untruncated was 0")
    })
}

fn raise_dml_returning_dup_bind(name: &str) -> PyErr {
    Python::attach(|py| -> PyResult<PyErr> {
        let errors = PyModule::import(py, "oracledb.errors")?;
        let error_num = errors.getattr("ERR_DML_RETURNING_DUP_BINDS")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("name", name)?;
        match errors.getattr("_raise_err")?.call((error_num,), Some(&kwargs)) {
            Ok(_) => Ok(PyRuntimeError::new_err(
                "oracledb.errors._raise_err(ERR_DML_RETURNING_DUP_BINDS) returned without raising",
            )),
            Err(err) => Ok(err),
        }
    })
    .unwrap_or_else(|_| {
        PyRuntimeError::new_err(format!(
            "DPY-2048: the bind variable placeholder \":{name}\" cannot be used both before and after the RETURNING clause in a DML RETURNING statement"
        ))
    })
}

fn raise_oracledb_driver_error(error_name: &str) -> PyErr {
    Python::attach(|py| -> PyResult<PyErr> {
        let errors = PyModule::import(py, "oracledb.errors")?;
        let error_num = errors.getattr(error_name)?;
        match errors.getattr("_raise_err")?.call1((error_num,)) {
            Ok(_) => Ok(PyRuntimeError::new_err(format!(
                "oracledb.errors._raise_err({error_name}) returned without raising"
            ))),
            Err(err) => Ok(err),
        }
    })
    .unwrap_or_else(|_| PyRuntimeError::new_err(error_name.to_string()))
}

fn raise_call_timeout_exceeded(timeout: u32) -> PyErr {
    Python::attach(|py| -> PyResult<PyErr> {
        let errors = PyModule::import(py, "oracledb.errors")?;
        let error_num = errors.getattr("ERR_CALL_TIMEOUT_EXCEEDED")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("timeout", timeout)?;
        match errors
            .getattr("_raise_err")?
            .call((error_num,), Some(&kwargs))
        {
            Ok(_) => Ok(PyRuntimeError::new_err(
                "oracledb.errors._raise_err(ERR_CALL_TIMEOUT_EXCEEDED) returned without raising",
            )),
            Err(err) => Ok(err),
        }
    })
    .unwrap_or_else(|_| PyRuntimeError::new_err(format!("call timeout of {timeout} ms exceeded")))
}

fn raise_invalid_object_type_name(name: &str) -> PyErr {
    Python::attach(|py| -> PyResult<PyErr> {
        let errors = PyModule::import(py, "oracledb.errors")?;
        let error_num = errors.getattr("ERR_INVALID_OBJECT_TYPE_NAME")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("name", name)?;
        match errors
            .getattr("_raise_err")?
            .call((error_num,), Some(&kwargs))
        {
            Ok(_) => Ok(PyRuntimeError::new_err(
                "oracledb.errors._raise_err(ERR_INVALID_OBJECT_TYPE_NAME) returned without raising",
            )),
            Err(err) => Ok(err),
        }
    })
    .unwrap_or_else(|_| PyRuntimeError::new_err(format!("invalid object type name: {name}")))
}

fn raise_invalid_coll_index_get(index: i32) -> PyErr {
    Python::attach(|py| -> PyResult<PyErr> {
        let errors = PyModule::import(py, "oracledb.errors")?;
        let error_num = errors.getattr("ERR_INVALID_COLL_INDEX_GET")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("index", index)?;
        match errors
            .getattr("_raise_err")?
            .call((error_num,), Some(&kwargs))
        {
            Ok(_) => Ok(PyRuntimeError::new_err(
                "oracledb.errors._raise_err(ERR_INVALID_COLL_INDEX_GET) returned without raising",
            )),
            Err(err) => Ok(err),
        }
    })
    .unwrap_or_else(|_| PyRuntimeError::new_err(format!("invalid collection index: {index}")))
}

fn raise_invalid_coll_index_set(index: i32, min_index: i32, max_index: i32) -> PyErr {
    Python::attach(|py| -> PyResult<PyErr> {
        let errors = PyModule::import(py, "oracledb.errors")?;
        let error_num = errors.getattr("ERR_INVALID_COLL_INDEX_SET")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("index", index)?;
        kwargs.set_item("min_index", min_index)?;
        kwargs.set_item("max_index", max_index)?;
        match errors
            .getattr("_raise_err")?
            .call((error_num,), Some(&kwargs))
        {
            Ok(_) => Ok(PyRuntimeError::new_err(
                "oracledb.errors._raise_err(ERR_INVALID_COLL_INDEX_SET) returned without raising",
            )),
            Err(err) => Ok(err),
        }
    })
    .unwrap_or_else(|_| {
        PyRuntimeError::new_err(format!(
            "invalid collection index: {index}; expected {min_index} to {max_index}"
        ))
    })
}

fn raise_wrong_object_type(actual: &DbObjectTypeImpl, expected: &DbObjectTypeImpl) -> PyErr {
    Python::attach(|py| -> PyResult<PyErr> {
        let errors = PyModule::import(py, "oracledb.errors")?;
        let error_num = errors.getattr("ERR_WRONG_OBJECT_TYPE")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("actual_schema", &actual.schema)?;
        kwargs.set_item("actual_name", &actual.name)?;
        kwargs.set_item("expected_schema", &expected.schema)?;
        kwargs.set_item("expected_name", &expected.name)?;
        match errors
            .getattr("_raise_err")?
            .call((error_num,), Some(&kwargs))
        {
            Ok(_) => Ok(PyRuntimeError::new_err(
                "oracledb.errors._raise_err(ERR_WRONG_OBJECT_TYPE) returned without raising",
            )),
            Err(err) => Ok(err),
        }
    })
    .unwrap_or_else(|_| {
        PyRuntimeError::new_err(format!(
            "found object of type \"{}.{}\" when expecting object of type \"{}.{}\"",
            actual.schema, actual.name, expected.schema, expected.name
        ))
    })
}

fn raise_dbobject_attr_max_size(
    attr_name: &str,
    type_name: &str,
    actual_size: usize,
    max_size: u32,
) -> PyErr {
    Python::attach(|py| -> PyResult<PyErr> {
        let errors = PyModule::import(py, "oracledb.errors")?;
        let error_num = errors.getattr("ERR_DBOBJECT_ATTR_MAX_SIZE_VIOLATED")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("attr_name", attr_name)?;
        kwargs.set_item("type_name", type_name)?;
        kwargs.set_item("actual_size", actual_size)?;
        kwargs.set_item("max_size", max_size)?;
        match errors
            .getattr("_raise_err")?
            .call((error_num,), Some(&kwargs))
        {
            Ok(_) => Ok(PyRuntimeError::new_err(
                "oracledb.errors._raise_err(ERR_DBOBJECT_ATTR_MAX_SIZE_VIOLATED) returned without raising",
            )),
            Err(err) => Ok(err),
        }
    })
    .unwrap_or_else(|_| {
        PyRuntimeError::new_err(format!(
            "attribute {attr_name} of type {type_name} exceeds its maximum size (actual: {actual_size}, maximum: {max_size})"
        ))
    })
}

fn raise_dbobject_element_max_size(
    index: i32,
    type_name: &str,
    actual_size: usize,
    max_size: u32,
) -> PyErr {
    Python::attach(|py| -> PyResult<PyErr> {
        let errors = PyModule::import(py, "oracledb.errors")?;
        let error_num = errors.getattr("ERR_DBOBJECT_ELEMENT_MAX_SIZE_VIOLATED")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("index", index)?;
        kwargs.set_item("type_name", type_name)?;
        kwargs.set_item("actual_size", actual_size)?;
        kwargs.set_item("max_size", max_size)?;
        match errors
            .getattr("_raise_err")?
            .call((error_num,), Some(&kwargs))
        {
            Ok(_) => Ok(PyRuntimeError::new_err(
                "oracledb.errors._raise_err(ERR_DBOBJECT_ELEMENT_MAX_SIZE_VIOLATED) returned without raising",
            )),
            Err(err) => Ok(err),
        }
    })
    .unwrap_or_else(|_| {
        PyRuntimeError::new_err(format!(
            "element {index} of type {type_name} exceeds its maximum size (actual: {actual_size}, maximum: {max_size})"
        ))
    })
}

fn raise_unsupported_python_type_for_db_type(
    value: &Bound<'_, PyAny>,
    db_type_name: &str,
) -> PyErr {
    let py_type_name = value
        .get_type()
        .getattr("__name__")
        .and_then(|name| name.extract::<String>())
        .unwrap_or_else(|_| "object".to_string());
    Python::attach(|py| -> PyResult<PyErr> {
        let errors = PyModule::import(py, "oracledb.errors")?;
        let error_num = errors.getattr("ERR_UNSUPPORTED_PYTHON_TYPE_FOR_DB_TYPE")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("py_type_name", &py_type_name)?;
        kwargs.set_item("db_type_name", db_type_name.trim_start_matches("DB_TYPE_"))?;
        match errors
            .getattr("_raise_err")?
            .call((error_num,), Some(&kwargs))
        {
            Ok(_) => Ok(PyRuntimeError::new_err(
                "oracledb.errors._raise_err(ERR_UNSUPPORTED_PYTHON_TYPE_FOR_DB_TYPE) returned without raising",
            )),
            Err(err) => Ok(err),
        }
    })
    .unwrap_or_else(|_| {
        PyRuntimeError::new_err(format!(
            "unsupported Python type {py_type_name} for database type {db_type_name}"
        ))
    })
}

fn raise_unsupported_type_set(db_type_name: &str) -> PyErr {
    Python::attach(|py| -> PyResult<PyErr> {
        let errors = PyModule::import(py, "oracledb.errors")?;
        let error_num = errors.getattr("ERR_UNSUPPORTED_TYPE_SET")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("db_type_name", db_type_name.trim_start_matches("DB_TYPE_"))?;
        match errors
            .getattr("_raise_err")?
            .call((error_num,), Some(&kwargs))
        {
            Ok(_) => Ok(PyRuntimeError::new_err(
                "oracledb.errors._raise_err(ERR_UNSUPPORTED_TYPE_SET) returned without raising",
            )),
            Err(err) => Ok(err),
        }
    })
    .unwrap_or_else(|_| {
        PyRuntimeError::new_err(format!("type {db_type_name} does not support being set"))
    })
}

fn get_string_attr(obj: &Bound<'_, PyAny>, name: &str) -> PyResult<String> {
    obj.getattr(name)?.extract()
}

fn get_optional_string_attr(obj: &Bound<'_, PyAny>, name: &str) -> PyResult<Option<String>> {
    let value = obj.getattr(name)?;
    if value.is_none() {
        Ok(None)
    } else {
        value.extract().map(Some)
    }
}

fn extract_optional_string(value: &Bound<'_, PyAny>) -> PyResult<Option<String>> {
    if value.is_none() {
        Ok(None)
    } else {
        value.extract().map(Some)
    }
}

fn get_optional_u32_attr(obj: &Bound<'_, PyAny>, name: &str) -> PyResult<Option<u32>> {
    if !obj.hasattr(name)? {
        return Ok(None);
    }
    let value = obj.getattr(name)?;
    if value.is_none() {
        Ok(None)
    } else {
        value.extract().map(Some)
    }
}

fn get_optional_bool_attr(obj: &Bound<'_, PyAny>, name: &str) -> PyResult<Option<bool>> {
    if !obj.hasattr(name)? {
        return Ok(None);
    }
    let value = obj.getattr(name)?;
    if value.is_none() {
        Ok(None)
    } else {
        value.extract().map(Some)
    }
}

fn normalize_connect_string(dsn: String) -> String {
    dsn.split_once("://")
        .map(|(_, connect_string)| connect_string.to_string())
        .unwrap_or(dsn)
}

fn is_user_without_password_dsn(dsn: &str) -> bool {
    let Some((credentials, connect_string)) = dsn.split_once('@') else {
        return false;
    };
    !credentials.is_empty()
        && !credentials.contains('/')
        && !credentials.contains(':')
        && !connect_string.is_empty()
}

fn get_connect_sdu_attr(obj: &Bound<'_, PyAny>) -> PyResult<Option<u32>> {
    if let Some(sdu) = get_optional_u32_attr(obj, "sdu")? {
        return Ok(Some(sdu));
    }
    if !obj.hasattr("description_list")? {
        return Ok(None);
    }
    let descriptions = obj.getattr("description_list")?.getattr("children")?;
    if descriptions.len()? == 0 {
        return Ok(None);
    }
    let description = descriptions.get_item(0)?;
    get_optional_u32_attr(&description, "sdu")
}

fn get_app_context_attr(obj: &Bound<'_, PyAny>) -> PyResult<Vec<(String, String, String)>> {
    let value = obj.getattr("appcontext")?;
    if value.is_none() {
        return Ok(Vec::new());
    }
    let list = value
        .cast::<PyList>()
        .map_err(|_| PyRuntimeError::new_err("appcontext should be a list"))?;
    list.iter()
        .map(|entry| entry.extract::<(String, String, String)>())
        .collect()
}

static PASSWORD_OVERRIDES: OnceLock<Mutex<BTreeMap<String, String>>> = OnceLock::new();
static NEXT_CONNECT_ARGS: OnceLock<Mutex<VecDeque<ConnectArgs>>> = OnceLock::new();
static NEXT_CONNECT_ARGS_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Default)]
struct ConnectArgs {
    id: u64,
    password: Option<String>,
    new_password: Option<String>,
    invalid_user_dsn: bool,
}

fn password_overrides() -> &'static Mutex<BTreeMap<String, String>> {
    PASSWORD_OVERRIDES.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn next_connect_args_queue() -> &'static Mutex<VecDeque<ConnectArgs>> {
    NEXT_CONNECT_ARGS.get_or_init(|| Mutex::new(VecDeque::new()))
}

fn consume_next_connect_args() -> PyResult<ConnectArgs> {
    Ok(next_connect_args_queue()
        .lock()
        .map_err(runtime_error)?
        .pop_front()
        .unwrap_or_default())
}

fn password_override_for_user(user: &str) -> PyResult<Option<String>> {
    Ok(password_overrides()
        .lock()
        .map_err(runtime_error)?
        .get(&user.to_ascii_uppercase())
        .cloned())
}

fn set_password_override_for_user(user: &str, password: &str) -> PyResult<()> {
    password_overrides()
        .lock()
        .map_err(runtime_error)?
        .insert(user.to_ascii_uppercase(), password.to_string());
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (password=None, new_password=None, invalid_user_dsn=false))]
fn record_next_connect_args(
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
fn discard_pending_connect_args(id: u64) -> PyResult<bool> {
    let mut queue = next_connect_args_queue().lock().map_err(runtime_error)?;
    if let Some(pos) = queue.iter().position(|entry| entry.id == id) {
        queue.remove(pos);
        return Ok(true);
    }
    Ok(false)
}

fn env_password_for_user(user: &str) -> PyResult<String> {
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

fn extract_bind_values(
    py: Python<'_>,
    statement: &str,
    parameters: Option<&Bound<'_, PyAny>>,
    keyword_parameters: Option<&Bound<'_, PyAny>>,
    named_input_sizes: &[(String, Py<PyAny>)],
    has_positional_input_sizes: bool,
    previous_bind_names: &[String],
    previous_bind_vars: &[Py<ThinVar>],
) -> PyResult<Vec<BindValue>> {
    let has_parameters = has_bind_payload(parameters)?;
    let has_keywords = has_bind_payload(keyword_parameters)?;
    if has_parameters && has_keywords {
        return Err(raise_oracledb_driver_error("ERR_ARGS_AND_KEYWORD_ARGS"));
    }
    if let Some(value) = keyword_parameters.filter(|_| has_keywords) {
        let dict = value.cast::<PyDict>()?;
        return extract_named_bind_values(
            py,
            statement,
            Some(dict),
            named_input_sizes,
            previous_bind_names,
            previous_bind_vars,
        );
    }
    let Some(value) = parameters else {
        if !named_input_sizes.is_empty() {
            return extract_named_bind_values(
                py,
                statement,
                None,
                named_input_sizes,
                previous_bind_names,
                previous_bind_vars,
            );
        }
        return Ok(Vec::new());
    };
    if !has_parameters {
        if has_positional_input_sizes {
            let row_values = positional_bind_items(value)?;
            if row_values.is_empty() {
                if let Some(name) = unique_sql_bind_names(statement)?.first() {
                    return Err(dpy_bind_error(
                        "DPY-4010",
                        format!(
                            "a bind variable replacement value for placeholder \":{name}\" was not provided"
                        ),
                    ));
                }
                return Ok(Vec::new());
            }
            return extract_positional_bind_values_for_execute(
                py,
                statement,
                value,
                named_input_sizes,
            );
        }
        if !named_input_sizes.is_empty() {
            return extract_named_bind_values(
                py,
                statement,
                None,
                named_input_sizes,
                previous_bind_names,
                previous_bind_vars,
            );
        }
        return Ok(Vec::new());
    }
    if let Ok(dict) = value.cast::<PyDict>() {
        return extract_named_bind_values(
            py,
            statement,
            Some(dict),
            named_input_sizes,
            previous_bind_names,
            previous_bind_vars,
        );
    }
    extract_positional_bind_values_for_execute(py, statement, value, named_input_sizes)
}

enum BindSourceKind {
    Parameters,
    Keywords,
}

fn thin_var_null_object_type(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<Option<DbObjectTypeImpl>> {
    let Some(var) = thin_var_from_value(value)? else {
        return Ok(None);
    };
    let var = var.borrow(py);
    let Some(object_type) = var.object_type.clone() else {
        return Ok(None);
    };
    if var.get_py_value(py)?.bind(py).is_none() {
        return Ok(Some(object_type));
    }
    Ok(None)
}

fn object_bind_sql_expr(
    py: Python<'_>,
    bind_name: &str,
    value: &Bound<'_, PyAny>,
    effective_dict: &Bound<'_, PyDict>,
    allow_null_var_cast: bool,
) -> PyResult<Option<String>> {
    if let Some(object) = py_db_object_impl(value)? {
        let object_type = object.object_type.clone();
        if object_type.is_collection {
            if object_type.is_assoc_array || object_type.package_name.is_some() {
                return Ok(None);
            }
            let collection_values = object
                .collection_values
                .lock()
                .map_err(runtime_error)?
                .iter()
                .map(|value| value.clone_ref(py))
                .collect::<Vec<_>>();
            let mut constructor_args = Vec::with_capacity(collection_values.len());
            for (index, element_value) in collection_values.into_iter().enumerate() {
                let element_value_bound = element_value.bind(py);
                if element_value_bound.is_none() {
                    constructor_args.push("null".to_string());
                    continue;
                }
                let generated_name =
                    sql::generated_object_attr_bind_name(bind_name, &format!("E{index}"));
                if let Some(expr) = object_bind_sql_expr(
                    py,
                    &generated_name,
                    element_value_bound,
                    effective_dict,
                    true,
                )? {
                    constructor_args.push(expr);
                } else {
                    constructor_args.push(format!(":{generated_name}"));
                    effective_dict.set_item(&generated_name, element_value)?;
                }
            }
            return Ok(Some(format!(
                "{}({})",
                object_type._get_fqn(),
                constructor_args.join(", ")
            )));
        }
        if object_type.attrs.is_empty() {
            return Ok(None);
        }
        let mut constructor_args = Vec::with_capacity(object_type.attrs.len());
        for attr in &object_type.attrs {
            let attr_value = object.attr_bind_value(py, &attr.name)?;
            let attr_value_bound = attr_value.bind(py);
            if attr_value_bound.is_none() {
                constructor_args.push("null".to_string());
                continue;
            }
            let generated_name = sql::generated_object_attr_bind_name(bind_name, &attr.name);
            if let Some(expr) =
                object_bind_sql_expr(py, &generated_name, attr_value_bound, effective_dict, true)?
            {
                constructor_args.push(expr);
            } else {
                constructor_args.push(object_attr_bind_sql_expr(
                    attr,
                    attr_value_bound,
                    &generated_name,
                )?);
                effective_dict.set_item(&generated_name, attr_value)?;
            }
        }
        return Ok(Some(format!(
            "{}({})",
            object_type._get_fqn(),
            constructor_args.join(", ")
        )));
    }

    if allow_null_var_cast {
        if let Some(object_type) = thin_var_null_object_type(py, value)? {
            if !object_type.is_collection {
                return Ok(Some(format!("cast(null as {})", object_type._get_fqn())));
            }
        }
    }

    Ok(None)
}

fn object_attr_bind_sql_expr(
    attr: &DbObjectAttrImpl,
    value: &Bound<'_, PyAny>,
    bind_name: &str,
) -> PyResult<String> {
    if attr.dbtype_name == "DB_TYPE_BLOB" && value.cast::<PyString>().is_ok() {
        return Ok(format!("utl_raw.cast_to_raw(:{bind_name})"));
    }
    Ok(format!(":{bind_name}"))
}

fn plsql_function_return_bind_name(statement: &str) -> Option<String> {
    sql::plsql_function_return_bind_name(statement)
}

fn rewrite_object_bind_dict(
    py: Python<'_>,
    statement: &str,
    effective_dict: &Bound<'_, PyDict>,
) -> PyResult<(String, bool)> {
    let function_return_name = plsql_function_return_bind_name(statement);
    let dml_return_names = statement_return_bind_names(statement)?;
    let mut bind_entries = Vec::new();
    for (key, value) in effective_dict.iter() {
        bind_entries.push((key.extract::<String>()?, value.clone().unbind()));
    }

    let mut effective_statement = statement.to_string();
    let mut changed = false;
    for (key, value) in bind_entries {
        let is_function_return_bind = function_return_name
            .as_deref()
            .is_some_and(|name| bind_names_equal(name, &key));
        let is_dml_return_bind = dml_return_names
            .iter()
            .any(|name| bind_names_equal(name, &key));
        let value = value.bind(py);
        let Some(sql_expr) = object_bind_sql_expr(
            py,
            &key,
            value,
            effective_dict,
            !(is_function_return_bind || is_dml_return_bind),
        )?
        else {
            continue;
        };
        effective_statement =
            sql::replace_input_bind_placeholder(&effective_statement, &key, &sql_expr);
        let _ = effective_dict.del_item(&key);
        changed = true;
    }

    Ok((effective_statement, changed))
}

fn positional_bind_dict_if_complete<'py>(
    py: Python<'py>,
    statement: &str,
    value: &Bound<'py, PyAny>,
) -> PyResult<Option<Bound<'py, PyDict>>> {
    let row_values = positional_bind_items(value)?;
    let names = unique_sql_bind_names(statement)?;
    if row_values.len() != names.len() {
        return Ok(None);
    }

    let effective_dict = PyDict::new(py);
    for (name, value) in names.iter().zip(row_values.iter()) {
        effective_dict.set_item(name, value)?;
    }
    Ok(Some(effective_dict))
}

fn prepare_object_execute_inputs(
    py: Python<'_>,
    statement: &str,
    parameters: Option<&Bound<'_, PyAny>>,
    keyword_parameters: Option<&Bound<'_, PyAny>>,
) -> PyResult<(String, Option<Py<PyAny>>, Option<Py<PyAny>>)> {
    let original_parameters = parameters.map(|value| value.clone().unbind());
    let original_keywords = keyword_parameters.map(|value| value.clone().unbind());
    let has_parameters = has_bind_payload(parameters)?;
    let has_keywords = has_bind_payload(keyword_parameters)?;
    if has_parameters && has_keywords {
        return Ok((
            statement.to_string(),
            original_parameters,
            original_keywords,
        ));
    }
    let (source_kind, source_dict): (BindSourceKind, Bound<'_, PyDict>) = if has_keywords {
        let Some(value) = keyword_parameters else {
            return Ok((
                statement.to_string(),
                original_parameters,
                original_keywords,
            ));
        };
        let Ok(dict) = value.cast::<PyDict>() else {
            return Ok((
                statement.to_string(),
                original_parameters,
                original_keywords,
            ));
        };
        (BindSourceKind::Keywords, dict.clone())
    } else if has_parameters {
        let Some(value) = parameters else {
            return Ok((
                statement.to_string(),
                original_parameters,
                original_keywords,
            ));
        };
        let dict = if let Ok(dict) = value.cast::<PyDict>() {
            dict.clone()
        } else {
            let Some(dict) = positional_bind_dict_if_complete(py, statement, value)? else {
                return Ok((
                    statement.to_string(),
                    original_parameters,
                    original_keywords,
                ));
            };
            dict
        };
        (BindSourceKind::Parameters, dict)
    } else {
        return Ok((
            statement.to_string(),
            original_parameters,
            original_keywords,
        ));
    };

    let effective_dict = PyDict::new(py);
    for (key, value) in source_dict.iter() {
        effective_dict.set_item(&key, &value)?;
    }

    let (mut effective_statement, mut changed) =
        rewrite_object_bind_dict(py, statement, &effective_dict)?;

    if let Some(statement) =
        rewrite_object_return_projection(&effective_statement, &effective_dict)?
    {
        effective_statement = statement;
        changed = true;
    }

    if !changed {
        return Ok((
            statement.to_string(),
            original_parameters,
            original_keywords,
        ));
    }
    match source_kind {
        BindSourceKind::Parameters => Ok((
            effective_statement,
            Some(effective_dict.unbind().into()),
            None,
        )),
        BindSourceKind::Keywords => Ok((
            effective_statement,
            None,
            Some(effective_dict.unbind().into()),
        )),
    }
}

fn rewrite_object_return_projection(
    statement: &str,
    parameters: &Bound<'_, PyDict>,
) -> PyResult<Option<String>> {
    let Some(return_name) =
        sql::dml_returning_single_bind_name(statement).map_err(sql_parse_error)?
    else {
        return Ok(None);
    };
    let Some(value) = get_named_bind_value(parameters, &return_name)? else {
        return Ok(None);
    };
    let Some((_object_type, attr_name)) = thin_var_object_return_projection(value.py(), &value)?
    else {
        return Ok(None);
    };
    sql::rewrite_dml_returning_projection(statement, &attr_name).map_err(sql_parse_error)
}

fn thin_var_object_return_projection(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<Option<(DbObjectTypeImpl, String)>> {
    let Some(var) = thin_var_from_value(value)? else {
        return Ok(None);
    };
    let var = var.borrow(py);
    let Some(object_type) = var.object_type.clone() else {
        return Ok(None);
    };
    let Some(attr_name) = var
        .object_return_attr
        .clone()
        .or_else(|| object_type.default_scalar_return_attr().map(str::to_string))
    else {
        return Ok(None);
    };
    Ok(Some((object_type, attr_name)))
}

fn has_bind_payload(value: Option<&Bound<'_, PyAny>>) -> PyResult<bool> {
    let Some(value) = value else {
        return Ok(false);
    };
    if value.is_none() {
        return Ok(false);
    }
    Ok(value.len()? > 0)
}

fn positional_bind_items<'py>(value: &Bound<'py, PyAny>) -> PyResult<Vec<Bound<'py, PyAny>>> {
    if value.cast::<PyDict>().is_ok() || value.cast::<PyString>().is_ok() {
        return Err(raise_oracledb_driver_error(
            "ERR_WRONG_EXECUTE_PARAMETERS_TYPE",
        ));
    }
    if let Ok(tuple) = value.cast::<PyTuple>() {
        return Ok(tuple.iter().collect());
    }
    if let Ok(list) = value.cast::<PyList>() {
        return Ok(list.iter().collect());
    }
    value
        .try_iter()
        .map_err(|_| raise_oracledb_driver_error("ERR_WRONG_EXECUTE_PARAMETERS_TYPE"))?
        .collect()
}

fn extract_positional_bind_values_for_execute(
    py: Python<'_>,
    statement: &str,
    value: &Bound<'_, PyAny>,
    named_input_sizes: &[(String, Py<PyAny>)],
) -> PyResult<Vec<BindValue>> {
    let row_values = positional_bind_items(value)?;
    let names = unique_sql_bind_names(statement)?;
    let return_names = statement_return_bind_names(statement)?;
    let input_count = names
        .iter()
        .filter(|name| {
            !return_names
                .iter()
                .any(|return_name| bind_names_equal(return_name, name))
        })
        .count();
    let has_all_bind_values = row_values.len() == names.len();
    let has_input_only_values = row_values.len() == input_count;
    if !has_all_bind_values && !has_input_only_values {
        return Err(dpy_bind_error(
            "DPY-4009",
            format!(
                "{input_count} positional bind values are required but {} were provided",
                row_values.len()
            ),
        ));
    }
    let mut input_index = 0;
    let mut values = Vec::with_capacity(names.len());
    for (position, name) in names.iter().enumerate() {
        let is_return_bind = return_names
            .iter()
            .any(|return_name| bind_names_equal(return_name, name));
        if is_return_bind {
            let bind = if has_all_bind_values {
                py_value_to_bind(&row_values[position])?
            } else {
                let Some(input_size_var) =
                    positional_input_size_value(py, named_input_sizes, position)
                else {
                    return Err(dpy_bind_error(
                        "DPY-4010",
                        format!(
                            "a bind variable replacement value for placeholder \":{name}\" was not provided"
                        ),
                    ));
                };
                py_value_to_bind(input_size_var.bind(py))?
            };
            values.push(returning_output_bind(bind));
            continue;
        }

        let value = if has_all_bind_values {
            row_values[position].clone()
        } else {
            let value = row_values[input_index].clone();
            input_index += 1;
            value
        };
        let bind = if let Some(input_size_var) =
            positional_input_size_value(py, named_input_sizes, position)
        {
            if let Ok(var) = input_size_var.bind(py).extract::<PyRef<'_, ThinVar>>() {
                var.set_py_value(Some(value.clone().unbind()))?;
                var.to_bind_value(py)?
            } else {
                py_value_to_execute_bind(&value)?
            }
        } else {
            py_value_to_execute_bind(&value)?
        };
        values.push(bind);
    }
    Ok(values)
}

fn extract_positional_bind_values_with_input_sizes(
    py: Python<'_>,
    statement: &str,
    value: &Bound<'_, PyAny>,
    named_input_sizes: &[(String, Py<PyAny>)],
) -> PyResult<Vec<BindValue>> {
    if value.cast::<PyDict>().is_ok() || value.cast::<PyString>().is_ok() {
        return Err(raise_oracledb_driver_error(
            "ERR_WRONG_EXECUTE_PARAMETERS_TYPE",
        ));
    }
    let row_values = positional_bind_items(value)?;
    let names = unique_sql_bind_names(statement)?;
    let return_names = statement_return_bind_names(statement)?;
    let input_count = names
        .iter()
        .filter(|name| {
            !return_names
                .iter()
                .any(|return_name| bind_names_equal(return_name, name))
        })
        .count();
    if input_count != row_values.len() {
        return Err(dpy_bind_error(
            "DPY-4009",
            format!(
                "{input_count} positional bind values are required but {} were provided",
                row_values.len()
            ),
        ));
    }
    let mut input_index = 0;
    let mut values = Vec::with_capacity(names.len());
    for (position, name) in names.iter().enumerate() {
        let is_return_bind = return_names
            .iter()
            .any(|return_name| bind_names_equal(return_name, name));
        if is_return_bind {
            let Some(input_size_var) = positional_input_size_value(py, named_input_sizes, position)
            else {
                return Err(dpy_bind_error(
                    "DPY-4010",
                    format!(
                        "a bind variable replacement value for placeholder \":{name}\" was not provided"
                    ),
                ));
            };
            let value = py_value_to_bind(input_size_var.bind(py))?;
            values.push(returning_output_bind(value));
            continue;
        }
        let value = row_values[input_index].clone();
        input_index += 1;
        let bind = if let Some(input_size_var) =
            positional_input_size_value(py, named_input_sizes, position)
        {
            if let Ok(var) = input_size_var.bind(py).extract::<PyRef<'_, ThinVar>>() {
                var.set_py_value(Some(value.clone().unbind()))?;
                var.to_bind_value(py)?
            } else {
                py_value_to_execute_bind(&value)?
            }
        } else {
            py_value_to_execute_bind(&value)?
        };
        values.push(bind);
    }
    Ok(values)
}

fn extract_named_bind_values(
    py: Python<'_>,
    statement: &str,
    parameters: Option<&Bound<'_, PyDict>>,
    named_input_sizes: &[(String, Py<PyAny>)],
    previous_bind_names: &[String],
    previous_bind_vars: &[Py<ThinVar>],
) -> PyResult<Vec<BindValue>> {
    let names = unique_sql_bind_names(statement)?;
    let return_names = statement_return_bind_names(statement)?;
    let assignment_output_names = statement_plsql_assignment_bind_names(statement)?;
    if let Some(parameters) = parameters {
        for (key, _) in parameters.iter() {
            let key = key.extract::<String>()?;
            if !names.iter().any(|name| bind_name_matches_key(name, &key)) {
                return Err(dpy_bind_error(
                    "DPY-4008",
                    format!("no bind placeholder named \":{key}\" was found in the SQL text"),
                ));
            }
        }
    }
    names
        .iter()
        .map(|name| {
            let is_return_bind = return_names
                .iter()
                .any(|return_name| bind_names_equal(return_name, name));
            let is_assignment_output_bind = assignment_output_names
                .iter()
                .any(|return_name| bind_names_equal(return_name, name));
            if let Some(parameters) = parameters {
                if let Some(value) = get_named_bind_value(parameters, name)? {
                    let value = if let Some(input_size_var) =
                        named_input_size_value(py, named_input_sizes, name)
                    {
                        if let Ok(var) = input_size_var.bind(py).extract::<PyRef<'_, ThinVar>>() {
                            var.set_py_value(Some(value.clone().unbind()))?;
                            var.to_bind_value(py)?
                        } else {
                            py_value_to_execute_bind(&value)?
                        }
                    } else {
                        py_value_to_execute_bind(&value)?
                    };
                    return Ok(if is_return_bind {
                        returning_output_bind(value)
                    } else {
                        value
                    });
                }
            }
            if let Some(value) = named_input_size_value(py, named_input_sizes, name) {
                let value = py_value_to_bind(value.bind(py))?;
                return Ok(if is_return_bind {
                    returning_output_bind(value)
                } else {
                    value
                });
            }
            if is_return_bind || is_assignment_output_bind {
                if let Some(var) =
                    previous_bind_var_by_name(py, previous_bind_names, previous_bind_vars, name)
                {
                    let value = var.borrow(py).to_bind_value(py)?;
                    return Ok(if is_return_bind {
                        returning_output_bind(value)
                    } else {
                        output_only_bind(value)
                    });
                }
            }
            Err(dpy_bind_error(
                "DPY-4010",
                format!(
                    "a bind variable replacement value for placeholder \":{name}\" was not provided"
                ),
            ))
        })
        .collect()
}

fn extract_bind_rows(
    py: Python<'_>,
    statement: &str,
    parameters: &Bound<'_, PyAny>,
    named_input_sizes: &[(String, Py<PyAny>)],
) -> PyResult<Vec<Vec<BindValue>>> {
    if parameters.is_none() {
        return Ok(Vec::new());
    }
    if let Ok(num_iters) = parameters.extract::<usize>() {
        if unique_sql_bind_names(statement)?.is_empty() {
            return Ok(vec![Vec::new(); num_iters]);
        }
    }
    let list = parameters
        .cast::<PyList>()
        .map_err(|_| not_implemented("ThinCursorImpl executemany parameters"))?;
    list.iter()
        .map(|row| {
            if let Ok(dict) = row.cast::<PyDict>() {
                extract_named_bind_values(py, statement, Some(dict), named_input_sizes, &[], &[])
            } else {
                extract_positional_bind_values_with_input_sizes(
                    py,
                    statement,
                    &row,
                    named_input_sizes,
                )
            }
        })
        .collect()
}

fn extract_bind_var_objects(
    py: Python<'_>,
    statement: &str,
    parameters: Option<&Bound<'_, PyAny>>,
    keyword_parameters: Option<&Bound<'_, PyAny>>,
    named_input_sizes: &[(String, Py<PyAny>)],
    previous_bind_names: &[String],
    previous_bind_vars: &[Py<ThinVar>],
) -> PyResult<Vec<Py<ThinVar>>> {
    let has_parameters = has_bind_payload(parameters)?;
    let has_keywords = has_bind_payload(keyword_parameters)?;
    if has_parameters && has_keywords {
        return Ok(Vec::new());
    }
    if let Some(value) = keyword_parameters.filter(|_| has_keywords) {
        return extract_named_bind_var_objects(
            py,
            statement,
            Some(value.cast::<PyDict>()?),
            named_input_sizes,
            previous_bind_names,
            previous_bind_vars,
        );
    }
    let Some(value) = parameters else {
        if !named_input_sizes.is_empty() {
            return extract_named_bind_var_objects(
                py,
                statement,
                None,
                named_input_sizes,
                previous_bind_names,
                previous_bind_vars,
            );
        }
        return Ok(Vec::new());
    };
    if !has_parameters {
        if !named_input_sizes.is_empty() {
            return extract_named_bind_var_objects(
                py,
                statement,
                None,
                named_input_sizes,
                previous_bind_names,
                previous_bind_vars,
            );
        }
        return Ok(Vec::new());
    }
    if let Ok(dict) = value.cast::<PyDict>() {
        return extract_named_bind_var_objects(
            py,
            statement,
            Some(dict),
            named_input_sizes,
            previous_bind_names,
            previous_bind_vars,
        );
    }
    extract_positional_bind_var_objects_for_execute(py, statement, value, named_input_sizes)
}

fn extract_positional_bind_var_objects_for_execute(
    py: Python<'_>,
    statement: &str,
    value: &Bound<'_, PyAny>,
    named_input_sizes: &[(String, Py<PyAny>)],
) -> PyResult<Vec<Py<ThinVar>>> {
    let row_values = positional_bind_items(value)?;
    let names = unique_sql_bind_names(statement)?;
    let return_names = statement_return_bind_names(statement)?;
    let input_count = names
        .iter()
        .filter(|name| {
            !return_names
                .iter()
                .any(|return_name| bind_names_equal(return_name, name))
        })
        .count();
    let has_all_bind_values = row_values.len() == names.len();
    let has_input_only_values = row_values.len() == input_count;
    if !has_all_bind_values && !has_input_only_values {
        return Ok(Vec::new());
    }

    let mut input_index = 0;
    let mut values = Vec::with_capacity(names.len());
    for (position, name) in names.iter().enumerate() {
        let is_return_bind = return_names
            .iter()
            .any(|return_name| bind_names_equal(return_name, name));
        if is_return_bind {
            if has_all_bind_values {
                values.push(bind_var_from_value(py, &row_values[position])?);
            } else if let Some(input_size_var) =
                positional_input_size_value(py, named_input_sizes, position)
            {
                values.push(bind_var_from_value(py, input_size_var.bind(py))?);
            }
            continue;
        }

        let value = if has_all_bind_values {
            row_values[position].clone()
        } else {
            let value = row_values[input_index].clone();
            input_index += 1;
            value
        };
        if let Some(input_size_var) = positional_input_size_value(py, named_input_sizes, position) {
            if let Ok(var) = input_size_var.bind(py).extract::<PyRef<'_, ThinVar>>() {
                var.set_py_value(Some(value.clone().unbind()))?;
            }
            values.push(bind_var_from_value(py, input_size_var.bind(py))?);
        } else {
            values.push(bind_var_from_value(py, &value)?);
        }
    }
    Ok(values)
}

fn extract_named_bind_var_objects(
    py: Python<'_>,
    statement: &str,
    parameters: Option<&Bound<'_, PyDict>>,
    named_input_sizes: &[(String, Py<PyAny>)],
    previous_bind_names: &[String],
    previous_bind_vars: &[Py<ThinVar>],
) -> PyResult<Vec<Py<ThinVar>>> {
    let mut values = Vec::new();
    let return_names = statement_return_bind_names(statement)?;
    let assignment_output_names = statement_plsql_assignment_bind_names(statement)?;
    for name in unique_sql_bind_names(statement)? {
        let input_size_var = named_input_size_value(py, named_input_sizes, &name);
        let is_return_bind = return_names
            .iter()
            .any(|return_name| bind_names_equal(return_name, &name));
        let is_assignment_output_bind = assignment_output_names
            .iter()
            .any(|return_name| bind_names_equal(return_name, &name));
        if let Some(parameters) = parameters {
            if let Some(value) = get_named_bind_value(parameters, &name)? {
                if let Some(input_size_var) = input_size_var {
                    if let Ok(var) = input_size_var.bind(py).extract::<PyRef<'_, ThinVar>>() {
                        var.set_py_value(Some(value.clone().unbind()))?;
                    }
                    values.push(bind_var_from_value(py, input_size_var.bind(py))?);
                } else {
                    values.push(bind_var_from_value(py, &value)?);
                }
                continue;
            }
        }
        if let Some(input_size_var) = input_size_var {
            values.push(bind_var_from_value(py, input_size_var.bind(py))?);
        } else if is_return_bind || is_assignment_output_bind {
            if let Some(var) =
                previous_bind_var_by_name(py, previous_bind_names, previous_bind_vars, &name)
            {
                values.push(var);
            }
        }
    }
    Ok(values)
}

fn named_input_size_value(
    py: Python<'_>,
    named_input_sizes: &[(String, Py<PyAny>)],
    name: &str,
) -> Option<Py<PyAny>> {
    named_input_sizes
        .iter()
        .find(|(key, _)| bind_name_matches_key(name, key))
        .map(|(_, value)| value.clone_ref(py))
}

fn previous_bind_var_by_name(
    py: Python<'_>,
    previous_bind_names: &[String],
    previous_bind_vars: &[Py<ThinVar>],
    name: &str,
) -> Option<Py<ThinVar>> {
    previous_bind_names
        .iter()
        .position(|previous_name| bind_names_equal(previous_name, name))
        .and_then(|index| previous_bind_vars.get(index))
        .map(|value| value.clone_ref(py))
}

fn positional_input_size_value(
    py: Python<'_>,
    named_input_sizes: &[(String, Py<PyAny>)],
    zero_based_position: usize,
) -> Option<Py<PyAny>> {
    named_input_size_value(
        py,
        named_input_sizes,
        &(zero_based_position + 1).to_string(),
    )
}

fn input_size_value_for_bind(
    py: Python<'_>,
    named_input_sizes: &[(String, Py<PyAny>)],
    name: &str,
    zero_based_position: usize,
) -> Option<Py<PyAny>> {
    positional_input_size_value(py, named_input_sizes, zero_based_position)
        .or_else(|| named_input_size_value(py, named_input_sizes, name))
}

fn extract_executemany_bind_var_objects(
    py: Python<'_>,
    statement: &str,
    named_input_sizes: &[(String, Py<PyAny>)],
) -> PyResult<Vec<Py<ThinVar>>> {
    unique_sql_bind_names(statement)?
        .iter()
        .enumerate()
        .map(|(position, name)| {
            if let Some(value) = input_size_value_for_bind(py, named_input_sizes, name, position) {
                bind_var_from_value(py, value.bind(py))
            } else {
                Py::new(py, ThinVar::from_py_value(None))
            }
        })
        .collect()
}

fn get_named_bind_value<'py>(
    parameters: &Bound<'py, PyDict>,
    name: &str,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    if let Some(value) = parameters.get_item(name)? {
        return Ok(Some(value));
    }
    if is_quoted_bind_name(name) {
        return Ok(None);
    }
    for (key, value) in parameters.iter() {
        let key = key.extract::<String>()?;
        if key.eq_ignore_ascii_case(name) {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

fn unique_sql_bind_names(statement: &str) -> PyResult<Vec<String>> {
    sql::unique_bind_names(statement).map_err(sql_parse_error)
}

fn public_bind_name(name: &str) -> String {
    sql::public_bind_name(name)
}

fn statement_return_bind_names(statement: &str) -> PyResult<Vec<String>> {
    sql::returning_bind_names(statement).map_err(sql_parse_error)
}

fn statement_plsql_assignment_bind_names(statement: &str) -> PyResult<Vec<String>> {
    sql::plsql_assignment_bind_names(statement).map_err(sql_parse_error)
}

fn statement_is_plsql(statement: &str) -> bool {
    sql::statement_is_plsql(statement)
}

fn is_quoted_bind_name(name: &str) -> bool {
    sql::is_quoted_bind_name(name)
}

fn validate_parse_bind_names(statement: &str) -> PyResult<()> {
    for name in unique_sql_bind_names(statement)? {
        if !is_quoted_bind_name(&name) && name.eq_ignore_ascii_case("ROWID") {
            return Err(ora_database_error(
                "ORA-01745: invalid host/bind variable name",
            ));
        }
    }
    Ok(())
}

fn validate_dml_returning_duplicate_binds(statement: &str) -> PyResult<()> {
    if statement_is_plsql(statement) {
        return Ok(());
    }
    let lower = statement.to_ascii_lowercase();
    let Some(returning_pos) = lower.find("returning") else {
        return Ok(());
    };
    let input_names = unique_sql_bind_names(&statement[..returning_pos])?;
    let return_names = statement_return_bind_names(statement)?;
    for return_name in return_names {
        if input_names
            .iter()
            .any(|input_name| bind_names_equal(input_name, &return_name))
        {
            return Err(raise_dml_returning_dup_bind(&public_bind_name(
                &return_name,
            )));
        }
    }
    Ok(())
}

fn bind_names_equal(left: &str, right: &str) -> bool {
    sql::bind_names_equal(left, right)
}

fn bind_name_matches_key(bind_name: &str, key: &str) -> bool {
    sql::bind_name_matches_key(bind_name, key)
}

fn sql_parse_error(err: sql::SqlError) -> PyErr {
    match err {
        sql::SqlError::MissingEndingSingleQuote => {
            raise_oracledb_driver_error("ERR_MISSING_ENDING_SINGLE_QUOTE")
        }
        sql::SqlError::MissingEndingDoubleQuote => {
            raise_oracledb_driver_error("ERR_MISSING_ENDING_DOUBLE_QUOTE")
        }
    }
}

fn is_public_cursor_value(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    Ok(value.hasattr("_impl")? && value.hasattr("connection")? && value.hasattr("arraysize")?)
}

fn validate_public_cursor_is_open(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    if !is_public_cursor_value(value)? {
        return Ok(false);
    }
    let impl_obj = value.getattr("_impl")?;
    if impl_obj.is_none() {
        return Err(raise_oracledb_driver_error("ERR_CURSOR_NOT_OPEN"));
    }
    Ok(impl_obj.extract::<PyRef<'_, ThinCursorImpl>>().is_ok()
        || impl_obj.extract::<PyRef<'_, AsyncThinCursorImpl>>().is_ok())
}

fn validate_cursor_bind_value(
    executing_cursor: &Bound<'_, PyAny>,
    executing_connection: &Arc<Mutex<Option<RustConnection>>>,
    value: &Bound<'_, PyAny>,
) -> PyResult<()> {
    if std::ptr::eq(value.as_ptr(), executing_cursor.as_ptr()) {
        return Err(raise_oracledb_driver_error("ERR_SELF_BIND_NOT_SUPPORTED"));
    }
    if !is_public_cursor_value(value)? {
        return Ok(());
    }
    let impl_obj = value.getattr("_impl")?;
    if impl_obj.is_none() {
        return Err(raise_oracledb_driver_error("ERR_CURSOR_NOT_OPEN"));
    }
    if let Ok(cursor_impl) = impl_obj.extract::<PyRef<'_, ThinCursorImpl>>() {
        if !Arc::ptr_eq(&cursor_impl.connection, executing_connection) {
            return Err(raise_oracledb_driver_error("ERR_CURSOR_DIFF_CONNECTION"));
        }
    } else if let Ok(cursor_impl) = impl_obj.extract::<PyRef<'_, AsyncThinCursorImpl>>() {
        if !Arc::ptr_eq(&cursor_impl.inner.connection, executing_connection) {
            return Err(raise_oracledb_driver_error("ERR_CURSOR_DIFF_CONNECTION"));
        }
    }
    Ok(())
}

fn validate_cursor_bind_container(
    executing_cursor: &Bound<'_, PyAny>,
    executing_connection: &Arc<Mutex<Option<RustConnection>>>,
    value: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.is_none() {
        return Ok(());
    }
    if let Ok(dict) = value.cast::<PyDict>() {
        for (_, item) in dict.iter() {
            validate_cursor_bind_value(executing_cursor, executing_connection, &item)?;
        }
        return Ok(());
    }
    if let Ok(tuple) = value.cast::<PyTuple>() {
        for item in tuple.iter() {
            validate_cursor_bind_value(executing_cursor, executing_connection, &item)?;
        }
        return Ok(());
    }
    if let Ok(list) = value.cast::<PyList>() {
        for item in list.iter() {
            validate_cursor_bind_value(executing_cursor, executing_connection, &item)?;
        }
        return Ok(());
    }
    validate_cursor_bind_value(executing_cursor, executing_connection, value)
}

fn validate_cursor_bind_parameters(
    executing_cursor: &Bound<'_, PyAny>,
    executing_connection: &Arc<Mutex<Option<RustConnection>>>,
    parameters: Option<&Bound<'_, PyAny>>,
    keyword_parameters: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    validate_cursor_bind_container(executing_cursor, executing_connection, parameters)?;
    validate_cursor_bind_container(executing_cursor, executing_connection, keyword_parameters)
}

fn py_value_to_bind(value: &Bound<'_, PyAny>) -> PyResult<BindValue> {
    if value.is_none() {
        return Ok(BindValue::Null);
    }
    if let Ok(var) = value.extract::<PyRef<'_, ThinVar>>() {
        return var.to_bind_value(value.py());
    }
    if is_public_cursor_value(value)? {
        let impl_obj = value.getattr("_impl")?;
        if impl_obj.is_none() {
            return Err(raise_oracledb_driver_error("ERR_CURSOR_NOT_OPEN"));
        }
        if let Ok(cursor_impl) = impl_obj.extract::<PyRef<'_, ThinCursorImpl>>() {
            return Ok(if cursor_impl.cursor_id == 0 {
                cursor_bind_template()
            } else {
                BindValue::Cursor {
                    cursor_id: cursor_impl.cursor_id,
                }
            });
        }
        if let Ok(cursor_impl) = impl_obj.extract::<PyRef<'_, AsyncThinCursorImpl>>() {
            return Ok(if cursor_impl.inner.cursor_id == 0 {
                cursor_bind_template()
            } else {
                BindValue::Cursor {
                    cursor_id: cursor_impl.inner.cursor_id,
                }
            });
        }
    }
    if let Some(bind) = py_lob_value_to_bind(value)? {
        return Ok(bind);
    }
    if let Some(object) = py_db_object_impl(value)? {
        if let Some(bind) = dbobject_collection_to_array_bind(value.py(), &object)? {
            return Ok(bind);
        }
    }
    if let Ok(bytes) = value.cast::<PyBytes>() {
        return Ok(BindValue::Raw(bytes.as_bytes().to_vec()));
    }
    if value.cast::<PyList>().is_ok() || value.cast::<PyTuple>().is_ok() {
        let values = py_list_to_array_bind_values(value)?;
        let (ora_type_num, csfrm, buffer_size) = values
            .iter()
            .find_map(|value| value.as_ref().and_then(bind_type_info))
            .unwrap_or((ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT, 1));
        return Ok(BindValue::Array {
            ora_type_num,
            csfrm,
            buffer_size,
            max_elements: u32::try_from(values.len()).unwrap_or(u32::MAX).max(1),
            values,
        });
    }
    if let Some((year, month, day, hour, minute, second, _nanosecond)) = py_date_time_fields(value)?
    {
        return Ok(BindValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
        });
    }
    if let Ok(text) = value.extract::<String>() {
        return Ok(BindValue::Text(text));
    }
    if let Ok(number) = value.extract::<i128>() {
        return Ok(BindValue::Number(number.to_string()));
    }
    if let Ok(number) = value.extract::<f64>() {
        return Ok(BindValue::Number(number.to_string()));
    }
    Err(not_implemented("ThinCursorImpl bind value type"))
}

fn py_value_to_bind_with_template(
    value: &Bound<'_, PyAny>,
    template: &BindValue,
) -> PyResult<BindValue> {
    let Some((ora_type_num, _csfrm, _buffer_size)) = bind_type_info(template) else {
        return py_value_to_bind(value);
    };
    if ora_type_num == ORA_TYPE_NUM_BINARY_INTEGER {
        if value.is_none() {
            return Ok(BindValue::Null);
        }
        return Ok(BindValue::BinaryInteger(
            python_int_from_value(value)?
                .bind(value.py())
                .str()?
                .extract::<String>()?,
        ));
    }
    if ora_type_num == ORA_TYPE_NUM_BINARY_DOUBLE {
        if value.is_none() {
            return Ok(BindValue::Null);
        }
        let number = value.extract::<f64>().or_else(|_| {
            PyModule::import(value.py(), "builtins")?
                .getattr("float")?
                .call1((value,))?
                .extract::<f64>()
        })?;
        return Ok(BindValue::BinaryDouble(number));
    }
    if ora_type_num == ORA_TYPE_NUM_NUMBER
        && matches!(py_value_type_name(value).as_str(), "Decimal")
    {
        return Ok(BindValue::Number(value.str()?.extract::<String>()?));
    }
    let Some((year, month, day, hour, minute, second, nanosecond)) = py_date_time_fields(value)?
    else {
        return py_value_to_bind(value);
    };
    if matches!(
        ora_type_num,
        ORA_TYPE_NUM_TIMESTAMP | ORA_TYPE_NUM_TIMESTAMP_LTZ | ORA_TYPE_NUM_TIMESTAMP_TZ
    ) {
        return Ok(BindValue::Timestamp {
            ora_type_num,
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
        });
    }
    py_value_to_bind(value)
}

fn py_list_to_array_bind_values(value: &Bound<'_, PyAny>) -> PyResult<Vec<Option<BindValue>>> {
    if let Ok(list) = value.cast::<PyList>() {
        return list
            .iter()
            .map(|item| {
                if item.is_none() {
                    Ok(None)
                } else {
                    py_value_to_bind(&item).map(Some)
                }
            })
            .collect();
    }
    let tuple = value.cast::<PyTuple>()?;
    tuple
        .iter()
        .map(|item| {
            if item.is_none() {
                Ok(None)
            } else {
                py_value_to_bind(&item).map(Some)
            }
        })
        .collect()
}

fn py_optional_u8_attr(value: &Bound<'_, PyAny>, name: &str) -> PyResult<u8> {
    match value.getattr(name) {
        Ok(attr) => attr.extract::<u8>(),
        Err(_) => Ok(0),
    }
}

fn py_optional_u32_attr(value: &Bound<'_, PyAny>, name: &str) -> PyResult<u32> {
    match value.getattr(name) {
        Ok(attr) => attr.extract::<u32>(),
        Err(_) => Ok(0),
    }
}

fn py_required_u8_attr(value: &Bound<'_, PyAny>, name: &str) -> PyResult<u8> {
    value.getattr(name)?.extract::<u8>()
}

fn py_date_time_fields(
    value: &Bound<'_, PyAny>,
) -> PyResult<Option<(i32, u8, u8, u8, u8, u8, u32)>> {
    if !(value.hasattr("year")? && value.hasattr("month")? && value.hasattr("day")?) {
        return Ok(None);
    }
    let microsecond = py_optional_u32_attr(value, "microsecond")?;
    Ok(Some((
        value.getattr("year")?.extract::<i32>()?,
        py_required_u8_attr(value, "month")?,
        py_required_u8_attr(value, "day")?,
        py_optional_u8_attr(value, "hour")?,
        py_optional_u8_attr(value, "minute")?,
        py_optional_u8_attr(value, "second")?,
        microsecond * 1000,
    )))
}

fn py_type_name(typ: &Bound<'_, PyAny>) -> String {
    typ.getattr("name")
        .or_else(|_| typ.getattr("__name__"))
        .and_then(|value| value.extract::<String>())
        .unwrap_or_default()
}

fn py_value_type_name(value: &Bound<'_, PyAny>) -> String {
    value
        .get_type()
        .getattr("__name__")
        .and_then(|name| name.extract::<String>())
        .unwrap_or_default()
}

fn python_int_from_value(value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let py = value.py();
    if let Ok(text) = value.extract::<String>() {
        return python_int_from_decimal_text(py, &text);
    }
    let builtins = PyModule::import(py, "builtins")?;
    Ok(builtins.getattr("int")?.call1((value,))?.unbind())
}

fn python_int_from_decimal_text(py: Python<'_>, text: &str) -> PyResult<Py<PyAny>> {
    let decimal = PyModule::import(py, "decimal")?
        .getattr("Decimal")?
        .call1((text,))?;
    let builtins = PyModule::import(py, "builtins")?;
    Ok(builtins.getattr("int")?.call1((decimal,))?.unbind())
}

fn py_db_object_type_impl(value: &Bound<'_, PyAny>) -> PyResult<Option<DbObjectTypeImpl>> {
    if let Ok(object_type) = value.extract::<PyRef<'_, DbObjectTypeImpl>>() {
        return Ok(Some(object_type.clone()));
    }
    if value.hasattr("_impl")? {
        let impl_obj = value.getattr("_impl")?;
        if let Ok(object_type) = impl_obj.extract::<PyRef<'_, DbObjectTypeImpl>>() {
            return Ok(Some(object_type.clone()));
        }
    }
    Ok(None)
}

fn py_db_object_impl<'py>(value: &Bound<'py, PyAny>) -> PyResult<Option<PyRef<'py, DbObjectImpl>>> {
    if let Ok(object) = value.extract::<PyRef<'py, DbObjectImpl>>() {
        return Ok(Some(object));
    }
    if value.hasattr("_impl")? {
        let impl_obj = value.getattr("_impl")?;
        if let Ok(object) = impl_obj.extract::<PyRef<'py, DbObjectImpl>>() {
            return Ok(Some(object));
        }
    }
    Ok(None)
}

fn py_lob_impl<'py>(value: &Bound<'py, PyAny>) -> PyResult<Option<PyRef<'py, ThinLob>>> {
    if let Ok(lob) = value.extract::<PyRef<'py, ThinLob>>() {
        return Ok(Some(lob));
    }
    if value.hasattr("_impl")? {
        let impl_obj = value.getattr("_impl")?;
        if let Ok(lob) = impl_obj.extract::<PyRef<'py, ThinLob>>() {
            return Ok(Some(lob));
        }
    }
    Ok(None)
}

fn thin_lob_value_to_bind(py: Python<'_>, lob: &ThinLob) -> PyResult<BindValue> {
    if let Some(locator) = lob.locator.lock().map_err(runtime_error)?.as_ref() {
        return Ok(BindValue::Lob {
            ora_type_num: lob.ora_type_num,
            csfrm: lob.csfrm,
            locator: locator.clone(),
        });
    }
    let data = lob.read(py, 1, None)?;
    let data = data.bind(py);
    if matches!(lob.ora_type_num, ORA_TYPE_NUM_BLOB | ORA_TYPE_NUM_BFILE) {
        let bytes = data.cast::<PyBytes>()?;
        return Ok(BindValue::Raw(bytes.as_bytes().to_vec()));
    }
    Ok(BindValue::Text(data.extract::<String>()?))
}

fn py_lob_value_to_bind(value: &Bound<'_, PyAny>) -> PyResult<Option<BindValue>> {
    if let Some(lob) = py_lob_impl(value)? {
        return thin_lob_value_to_bind(value.py(), &lob).map(Some);
    }
    if value.hasattr("_impl")? {
        let impl_obj = value.getattr("_impl")?;
        if let Ok(lob) = impl_obj.extract::<PyRef<'_, AsyncThinLob>>() {
            return thin_lob_value_to_bind(value.py(), &lob.inner).map(Some);
        }
    }
    Ok(None)
}

fn scalar_value_to_memory_lob(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    dbtype_name: &str,
) -> PyResult<Option<Py<PyAny>>> {
    let (ora_type_num, csfrm) = match dbtype_name {
        "DB_TYPE_BLOB" => (ORA_TYPE_NUM_BLOB, 0),
        "DB_TYPE_CLOB" => (ORA_TYPE_NUM_CLOB, CS_FORM_IMPLICIT),
        "DB_TYPE_NCLOB" => (ORA_TYPE_NUM_CLOB, CS_FORM_NCHAR),
        _ => return Ok(None),
    };
    if value.is_none() || py_lob_impl(value)?.is_some() {
        return Ok(None);
    }
    let (data, size) = if ora_type_num == ORA_TYPE_NUM_BLOB {
        if let Ok(bytes) = value.cast::<PyBytes>() {
            let data = bytes.as_bytes().to_vec();
            let size = data.len() as u64;
            (data, size)
        } else {
            let text = value.extract::<String>()?;
            let data = text.into_bytes();
            let size = data.len() as u64;
            (data, size)
        }
    } else if let Ok(bytes) = value.cast::<PyBytes>() {
        let text = std::str::from_utf8(bytes.as_bytes()).map_err(runtime_error)?;
        (
            protocol_encode_lob_text(text, csfrm, None),
            text.chars().count() as u64,
        )
    } else {
        let text = value.extract::<String>()?;
        let size = text.chars().count() as u64;
        (protocol_encode_lob_text(&text, csfrm, None), size)
    };
    py_lob_from_impl(
        py,
        ThinLob {
            data: Some(Arc::new(Mutex::new(data))),
            locator: Arc::new(Mutex::new(None)),
            ora_type_num,
            csfrm,
            size,
            chunk_size: 0,
            context: None,
            is_open: Arc::new(Mutex::new(false)),
            bfile_name: None,
        },
    )
    .map(Some)
}

fn py_dbobject_element_to_bind(
    value: &Bound<'_, PyAny>,
    metadata: &DbObjectAttrImpl,
) -> PyResult<BindValue> {
    if metadata.dbtype_name == "DB_TYPE_BLOB" {
        if let Ok(text) = value.extract::<String>() {
            return Ok(BindValue::Raw(text.into_bytes()));
        }
    }
    py_value_to_bind(value)
}

fn dbobject_collection_to_array_bind(
    py: Python<'_>,
    object: &DbObjectImpl,
) -> PyResult<Option<BindValue>> {
    if !object.object_type.is_collection {
        return Ok(None);
    }
    let Some(metadata) = object.object_type.element_metadata.as_deref() else {
        return Ok(None);
    };
    if metadata.dbtype_name == "DB_TYPE_OBJECT" {
        return Ok(None);
    }
    object.ensure_unpacked(py)?;
    let elements = if object.object_type.is_assoc_array {
        object
            .assoc_values
            .lock()
            .map_err(runtime_error)?
            .values()
            .map(|value| value.clone_ref(py))
            .collect::<Vec<_>>()
    } else {
        object
            .collection_values
            .lock()
            .map_err(runtime_error)?
            .iter()
            .map(|value| value.clone_ref(py))
            .collect::<Vec<_>>()
    };
    let values = elements
        .iter()
        .map(|value| {
            let value = value.bind(py);
            if value.is_none() {
                Ok(None)
            } else {
                py_dbobject_element_to_bind(value, metadata).map(Some)
            }
        })
        .collect::<PyResult<Vec<_>>>()?;
    let (ora_type_num, csfrm, buffer_size) = values
        .iter()
        .find_map(|value| value.as_ref().and_then(bind_type_info))
        .unwrap_or_else(|| {
            let info = dbobject_element_bind_type_info(&metadata.dbtype_name, metadata.max_size);
            (info.ora_type_num, info.csfrm, info.buffer_size)
        });
    Ok(Some(BindValue::Array {
        ora_type_num,
        csfrm,
        buffer_size,
        max_elements: u32::try_from(values.len()).unwrap_or(u32::MAX).max(1),
        values,
    }))
}

fn bind_template_from_type(typ: &Bound<'_, PyAny>, size: u32) -> BindValue {
    bind_template_from_type_name(&py_type_name(typ), size)
}

fn return_kind_from_type_name(type_name: &str) -> ThinVarReturnKind {
    match type_name {
        "DB_TYPE_CLOB" | "CLOB" | "DB_TYPE_NCLOB" | "NCLOB" => ThinVarReturnKind::ClobAsLong,
        _ => ThinVarReturnKind::Plain,
    }
}

fn typed_lob_bind_hint_from_type_name(type_name: &str) -> Option<(u8, u8)> {
    match type_name {
        "DB_TYPE_CLOB" | "CLOB" => Some((ORA_TYPE_NUM_CLOB, CS_FORM_IMPLICIT)),
        "DB_TYPE_NCLOB" | "NCLOB" => Some((ORA_TYPE_NUM_CLOB, CS_FORM_NCHAR)),
        "DB_TYPE_BLOB" | "BLOB" => Some((ORA_TYPE_NUM_BLOB, 0)),
        _ => None,
    }
}

fn typed_lob_bind_hints(py: Python<'_>, bind_vars: &[Py<ThinVar>]) -> Vec<Option<(u8, u8)>> {
    bind_vars
        .iter()
        .map(|var| typed_lob_bind_hint_from_type_name(&var.borrow(py).dbtype_name))
        .collect()
}

fn default_fetch_lobs(py: Python<'_>) -> PyResult<bool> {
    PyModule::import(py, "oracledb")?
        .getattr("defaults")?
        .getattr("fetch_lobs")?
        .extract()
}

fn materialize_typed_lob_text_bind(
    connection: &mut RustConnection,
    value: &mut BindValue,
    ora_type_num: u8,
    csfrm: u8,
    call_timeout: Option<u32>,
) -> Result<(), String> {
    let BindValue::Text(text) = value else {
        return Ok(());
    };
    let text = std::mem::take(text);
    let mut locator = BlockingConnection::create_temp_lob(connection, ora_type_num, csfrm)
        .map_err(|err| err.to_string())?
        .locator;
    if !text.is_empty() {
        let bytes = protocol_encode_lob_text(&text, csfrm, Some(&locator));
        locator = BlockingConnection::write_lob_with_timeout(
            connection,
            &locator,
            1,
            &bytes,
            call_timeout,
        )
        .map_err(|err| err.to_string())?
        .locator;
    }
    *value = BindValue::Lob {
        ora_type_num,
        csfrm,
        locator,
    };
    Ok(())
}

fn materialize_typed_lob_raw_bind(
    connection: &mut RustConnection,
    value: &mut BindValue,
    ora_type_num: u8,
    csfrm: u8,
    call_timeout: Option<u32>,
) -> Result<(), String> {
    let BindValue::Raw(bytes) = value else {
        return Ok(());
    };
    let bytes = std::mem::take(bytes);
    let mut locator = BlockingConnection::create_temp_lob(connection, ora_type_num, csfrm)
        .map_err(|err| err.to_string())?
        .locator;
    if !bytes.is_empty() {
        locator = BlockingConnection::write_lob_with_timeout(
            connection,
            &locator,
            1,
            &bytes,
            call_timeout,
        )
        .map_err(|err| err.to_string())?
        .locator;
    }
    *value = BindValue::Lob {
        ora_type_num,
        csfrm,
        locator,
    };
    Ok(())
}

fn materialize_typed_lob_bind_values(
    connection: &mut RustConnection,
    values: &mut [BindValue],
    hints: &[Option<(u8, u8)>],
    call_timeout: Option<u32>,
) -> Result<(), String> {
    for (index, value) in values.iter_mut().enumerate() {
        let Some((ora_type_num, csfrm)) = hints.get(index).copied().flatten() else {
            continue;
        };
        materialize_typed_lob_text_bind(connection, value, ora_type_num, csfrm, call_timeout)?;
        materialize_typed_lob_raw_bind(connection, value, ora_type_num, csfrm, call_timeout)?;
    }
    Ok(())
}

fn materialize_typed_lob_bind_rows(
    connection: &mut RustConnection,
    rows: &mut [Vec<BindValue>],
    hints: &[Option<(u8, u8)>],
    call_timeout: Option<u32>,
) -> Result<(), String> {
    for row in rows {
        materialize_typed_lob_bind_values(connection, row, hints, call_timeout)?;
    }
    Ok(())
}

fn bind_template_from_input_size(value: &Bound<'_, PyAny>) -> PyResult<BindValue> {
    if let Ok(size) = value.extract::<u32>() {
        return Ok(BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_VARCHAR,
            csfrm: CS_FORM_IMPLICIT,
            buffer_size: size.max(1),
        });
    }
    if let Ok(tuple) = value.cast::<PyTuple>() {
        if let Some(typ) = tuple.get_item(0).ok() {
            let size = tuple
                .get_item(2)
                .ok()
                .and_then(|item| item.extract::<u32>().ok())
                .unwrap_or(0);
            return Ok(bind_template_from_type(&typ, size));
        }
    }
    if let Ok(list) = value.cast::<PyList>() {
        if let Some(typ) = list.get_item(0).ok() {
            let size = list
                .get_item(2)
                .ok()
                .and_then(|item| item.extract::<u32>().ok())
                .unwrap_or(0);
            return Ok(bind_template_from_type(&typ, size));
        }
    }
    Ok(bind_template_from_type(value, 0))
}

fn thin_var_from_type_spec(
    py: Python<'_>,
    connection: &Bound<'_, PyAny>,
    typ: &Bound<'_, PyAny>,
    size: u32,
    is_array: bool,
    num_elements: u32,
    outconverter: Option<Py<PyAny>>,
    convert_nulls: bool,
    bypass_decode: bool,
) -> PyResult<Py<ThinVar>> {
    let type_name = py_type_name(typ);
    let object_type = py_db_object_type_impl(typ)?;
    let default_bind = if let Some(object_type) = object_type.as_ref() {
        object_type
            .object_output_bind()
            .unwrap_or(BindValue::TypedNull {
                ora_type_num: ORA_TYPE_NUM_VARCHAR,
                csfrm: CS_FORM_IMPLICIT,
                buffer_size: size.max(4000),
            })
    } else {
        bind_template_from_type_name(&type_name, size)
    };
    let return_kind = return_kind_from_type_name(&type_name);
    let object_return_attr = object_type
        .as_ref()
        .and_then(DbObjectTypeImpl::default_scalar_return_attr)
        .map(str::to_string);
    let dbtype_name = object_type
        .as_ref()
        .map(|_| "DB_TYPE_OBJECT")
        .unwrap_or_else(|| public_dbtype_name_from_type_name(&type_name));
    let value = if is_cursor_bind_template(&default_bind) {
        Some(connection.call_method0("cursor")?.unbind())
    } else {
        None
    };
    Py::new(
        py,
        ThinVar::typed_with_options(
            default_bind,
            value,
            is_array,
            num_elements,
            outconverter,
            convert_nulls,
            return_kind,
            object_type,
            object_return_attr,
            dbtype_name,
            bypass_decode,
        ),
    )
}

fn thin_var_from_input_size(
    py: Python<'_>,
    connection: &Bound<'_, PyAny>,
    value: &Bound<'_, PyAny>,
) -> PyResult<Py<ThinVar>> {
    if let Some(var) = thin_var_from_value(value)? {
        return Ok(var);
    }
    let default_bind = bind_template_from_input_size(value)?;
    let (is_array, num_elements) = input_size_array_info(value)?;
    let type_name = py_type_name(value);
    let dbtype_name = if type_name.is_empty() {
        public_dbtype_name_from_bind(&default_bind)
    } else {
        public_dbtype_name_from_type_name(&type_name)
    };
    let value = if is_cursor_bind_template(&default_bind) {
        Some(connection.call_method0("cursor")?.unbind())
    } else {
        None
    };
    Py::new(
        py,
        ThinVar::typed_with_options(
            default_bind,
            value,
            is_array,
            num_elements,
            None,
            false,
            ThinVarReturnKind::Plain,
            None,
            None,
            dbtype_name,
            false,
        ),
    )
}

fn input_size_array_info(value: &Bound<'_, PyAny>) -> PyResult<(bool, u32)> {
    if let Ok(tuple) = value.cast::<PyTuple>() {
        if tuple.len() == 2 {
            return Ok((true, tuple.get_item(1)?.extract::<u32>()?.max(1)));
        }
    }
    if let Ok(list) = value.cast::<PyList>() {
        if list.len() == 2 {
            return Ok((true, list.get_item(1)?.extract::<u32>()?.max(1)));
        }
    }
    Ok((false, 1))
}

fn bind_type_info(value: &BindValue) -> Option<(u8, u8, u32)> {
    bind_value_type_info(value).map(|info| (info.ora_type_num, info.csfrm, info.buffer_size))
}

fn fetch_define_metadata_from_var(source: &ColumnMetadata, value: &BindValue) -> ColumnMetadata {
    define_metadata_from_bind(source, value)
}

fn bind_optional_text(value: Option<&str>) -> BindValue {
    value
        .map(|value| BindValue::Text(value.to_string()))
        .unwrap_or(BindValue::Null)
}

fn supplement_json_lob_column_metadata(
    connection: &Arc<Mutex<Option<RustConnection>>>,
    columns: &mut [ColumnMetadata],
    call_timeout: Option<u32>,
) -> PyResult<()> {
    let candidates = columns
        .iter()
        .enumerate()
        .filter(|(_, metadata)| {
            !metadata.is_json
                && matches!(metadata.ora_type_num, ORA_TYPE_NUM_CLOB | ORA_TYPE_NUM_BLOB)
                && !metadata.name.is_empty()
        })
        .map(|(index, metadata)| (index, metadata.name.to_ascii_uppercase()))
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Ok(());
    }
    let mut guard = connection.lock().map_err(runtime_error)?;
    let connection = guard.as_mut().ok_or_else(connection_closed_error)?;
    for (index, column_name) in candidates {
        let result = BlockingConnection::execute_query_with_binds_and_timeout(
            connection,
            "select 1 \
             from all_json_columns \
             where owner = sys_context('USERENV', 'CURRENT_SCHEMA') \
               and column_name = :1",
            1,
            &[BindValue::Text(column_name)],
            call_timeout,
        )
        .map_err(runtime_error)?;
        if !result.rows.is_empty() {
            columns[index].is_json = true;
        }
    }
    Ok(())
}

fn quoted_oracle_string(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn user_identifier(value: &str) -> PyResult<String> {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '$' | '#'))
    {
        Ok(value.to_ascii_uppercase())
    } else {
        Err(not_implemented("quoted Oracle username"))
    }
}

fn query_value_to_string(value: &Option<QueryValue>) -> Option<String> {
    match value {
        Some(QueryValue::Text(value)) => Some(value.clone()),
        Some(QueryValue::Rowid(value)) => Some(value.clone()),
        Some(QueryValue::Raw(value)) => String::from_utf8(value.clone()).ok(),
        Some(QueryValue::BinaryDouble(value)) => Some(value.clone()),
        Some(QueryValue::Number { text, .. }) => Some(text.clone()),
        Some(QueryValue::DateTime { .. }) => None,
        Some(QueryValue::Array(_)) => None,
        Some(QueryValue::Cursor { .. }) => None,
        Some(QueryValue::Object { .. }) => None,
        Some(QueryValue::Lob { .. }) => None,
        None => None,
    }
}

fn query_value_to_i64(value: &Option<QueryValue>) -> PyResult<i64> {
    query_value_to_string(value)
        .ok_or_else(|| PyRuntimeError::new_err("query returned NULL where integer was expected"))?
        .parse()
        .map_err(runtime_error)
}

fn query_value_to_u32(value: &Option<QueryValue>) -> Option<u32> {
    query_value_to_string(value)?.parse().ok()
}

fn columns_require_define(columns: &[ColumnMetadata]) -> bool {
    columns
        .iter()
        .any(|metadata| matches!(metadata.ora_type_num, ORA_TYPE_NUM_CLOB | ORA_TYPE_NUM_BLOB))
}

fn query_value_to_i8(value: &Option<QueryValue>) -> Option<i8> {
    query_value_to_string(value)?.parse().ok()
}

fn sql_identifier(value: &str) -> PyResult<String> {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '$' | '#'))
    {
        Ok(value.to_string())
    } else {
        Err(not_implemented("quoted Oracle identifier"))
    }
}

fn first_sql_keyword(statement: &str) -> String {
    statement
        .trim_start()
        .split(|ch: char| !ch.is_ascii_alphabetic())
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase()
}

fn parse_alter_session_value(statement: &str, key: &str) -> Option<String> {
    let trimmed = statement.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    let prefix = format!("alter session set {key}");
    if !lower.starts_with(&prefix) {
        return None;
    }
    let mut value = trimmed.get(prefix.len()..)?.trim_start();
    if let Some(stripped) = value.strip_prefix('=') {
        value = stripped.trim_start();
    }
    value
        .split_whitespace()
        .next()
        .map(|value| value.trim_matches('"').to_string())
        .filter(|value| !value.is_empty())
}

#[derive(Debug)]
struct ThinConnState {
    current_schema: Option<String>,
    current_schema_modified: bool,
    edition: Option<String>,
    edition_probe_started: bool,
    external_name: Option<String>,
    internal_name: Option<String>,
    call_timeout: u32,
    stmt_cache_size: u32,
    transaction_in_progress: bool,
    invalid_connect_string: bool,
    dbop_operation: Option<(String, i64)>,
}

impl ThinConnState {
    fn new(stmt_cache_size: u32, edition: Option<String>, invalid_connect_string: bool) -> Self {
        Self {
            current_schema: None,
            current_schema_modified: false,
            edition_probe_started: edition.is_some(),
            edition,
            external_name: None,
            internal_name: None,
            call_timeout: 0,
            stmt_cache_size,
            transaction_in_progress: false,
            invalid_connect_string,
            dbop_operation: None,
        }
    }

    fn record_statement(&mut self, statement: &str, is_query: bool, committed: bool) {
        if let Some(schema) = parse_alter_session_value(statement, "current_schema") {
            self.current_schema = Some(schema);
            self.current_schema_modified = false;
            self.transaction_in_progress = false;
            return;
        }
        if let Some(edition) = parse_alter_session_value(statement, "edition") {
            self.edition = Some(edition.to_ascii_uppercase());
            self.edition_probe_started = true;
            self.transaction_in_progress = false;
            return;
        }
        if committed {
            self.transaction_in_progress = false;
            return;
        }
        if is_query {
            return;
        }
        match first_sql_keyword(statement).as_str() {
            "insert" | "update" | "delete" | "merge" => self.transaction_in_progress = true,
            "alter" | "commit" | "rollback" | "truncate" => self.transaction_in_progress = false,
            _ => {}
        }
    }
}

#[derive(Clone)]
struct ThinLobContext {
    connection: Arc<Mutex<Option<RustConnection>>>,
    state: Arc<Mutex<ThinConnState>>,
    async_mode: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ThinVarReturnKind {
    Plain,
    ClobAsLong,
}

#[derive(Clone)]
#[pyclass(module = "oracledb.thin_impl", name = "ThinLob")]
struct ThinLob {
    data: Option<Arc<Mutex<Vec<u8>>>>,
    locator: Arc<Mutex<Option<Vec<u8>>>>,
    ora_type_num: u8,
    csfrm: u8,
    size: u64,
    chunk_size: u32,
    context: Option<ThinLobContext>,
    is_open: Arc<Mutex<bool>>,
    bfile_name: Option<(String, String)>,
}

fn lob_data_to_py(
    py: Python<'_>,
    ora_type_num: u8,
    csfrm: u8,
    locator: Option<&[u8]>,
    data: &[u8],
    offset: u64,
    amount: Option<u64>,
) -> PyResult<Py<PyAny>> {
    if matches!(ora_type_num, ORA_TYPE_NUM_BLOB | ORA_TYPE_NUM_BFILE) {
        let start = offset.saturating_sub(1) as usize;
        let bytes = data.get(start..).unwrap_or_default();
        let bytes = amount
            .and_then(|amount| usize::try_from(amount).ok())
            .map(|amount| bytes.get(..amount).unwrap_or(bytes))
            .unwrap_or(bytes);
        return Ok(PyBytes::new(py, bytes).unbind().into());
    }
    let text = protocol_decode_lob_text(data, csfrm, locator).map_err(runtime_error)?;
    let start = offset.saturating_sub(1) as usize;
    let chars = text.chars().skip(start);
    let value = match amount.and_then(|amount| usize::try_from(amount).ok()) {
        Some(amount) => chars.take(amount).collect::<String>(),
        None => chars.collect::<String>(),
    };
    Ok(value.into_pyobject(py)?.unbind().into())
}

fn py_lob_from_impl(py: Python<'_>, lob: ThinLob) -> PyResult<Py<PyAny>> {
    let module = PyModule::import(py, "oracledb")?;
    let cls = if lob
        .context
        .as_ref()
        .is_some_and(|context| context.async_mode)
    {
        module.getattr("AsyncLOB")?
    } else {
        module.getattr("LOB")?
    };
    let impl_obj: Py<PyAny> = if lob
        .context
        .as_ref()
        .is_some_and(|context| context.async_mode)
    {
        Py::new(py, AsyncThinLob { inner: lob })?.into()
    } else {
        Py::new(py, lob)?.into()
    };
    Ok(cls.call_method1("_from_impl", (impl_obj,))?.unbind())
}

#[pymethods]
impl ThinLob {
    #[pyo3(signature = (offset=1, amount=None))]
    fn read(&self, py: Python<'_>, offset: u64, amount: Option<u64>) -> PyResult<Py<PyAny>> {
        if self.ora_type_num == ORA_TYPE_NUM_BFILE {
            return Err(dpy_database_error(
                "ORA-22285",
                "non-existent directory or file for FILEOPEN operation",
            ));
        }
        if let Some(data) = self.data.as_ref() {
            let data = data.lock().map_err(runtime_error)?;
            return lob_data_to_py(
                py,
                self.ora_type_num,
                self.csfrm,
                self.locator.lock().map_err(runtime_error)?.as_deref(),
                &data,
                offset,
                amount,
            );
        }
        let Some(context) = self.context.as_ref() else {
            return lob_data_to_py(py, self.ora_type_num, self.csfrm, None, &[], offset, amount);
        };
        let locator = self
            .locator
            .lock()
            .map_err(runtime_error)?
            .clone()
            .unwrap_or_default();
        let call_timeout = {
            let value = context.state.lock().map_err(runtime_error)?.call_timeout;
            (value > 0).then_some(value)
        };
        let mut guard = context.connection.lock().map_err(runtime_error)?;
        let connection = guard.as_mut().ok_or_else(connection_closed_error)?;
        let result = BlockingConnection::read_lob_with_timeout(
            connection,
            &locator,
            offset,
            amount.unwrap_or(u64::from(u32::MAX)),
            call_timeout,
        )
        .map_err(runtime_error)?;
        *self.locator.lock().map_err(runtime_error)? = Some(result.locator.clone());
        lob_data_to_py(
            py,
            self.ora_type_num,
            self.csfrm,
            Some(&result.locator),
            result.data.as_deref().unwrap_or_default(),
            1,
            None,
        )
    }

    fn write(&mut self, value: &Bound<'_, PyAny>, offset: u64) -> PyResult<()> {
        let is_binary = matches!(self.ora_type_num, ORA_TYPE_NUM_BLOB | ORA_TYPE_NUM_BFILE);
        let raw_bytes = if is_binary {
            Some(value.cast::<PyBytes>()?.as_bytes().to_vec())
        } else {
            None
        };
        let text = if is_binary {
            None
        } else {
            Some(value.extract::<String>()?)
        };
        if let Some(context) = self.context.as_ref() {
            let locator = self
                .locator
                .lock()
                .map_err(runtime_error)?
                .clone()
                .unwrap_or_default();
            let bytes = raw_bytes.as_ref().cloned().unwrap_or_else(|| {
                protocol_encode_lob_text(
                    text.as_deref().unwrap_or_default(),
                    self.csfrm,
                    Some(&locator),
                )
            });
            let call_timeout = {
                let value = context.state.lock().map_err(runtime_error)?.call_timeout;
                (value > 0).then_some(value)
            };
            let mut guard = context.connection.lock().map_err(runtime_error)?;
            let connection = guard.as_mut().ok_or_else(connection_closed_error)?;
            let result = BlockingConnection::write_lob_with_timeout(
                connection,
                &locator,
                offset,
                &bytes,
                call_timeout,
            )
            .map_err(runtime_error)?;
            *self.locator.lock().map_err(runtime_error)? = Some(result.locator);
            self.size = if is_binary {
                self.size.max(offset.saturating_sub(1) + bytes.len() as u64)
            } else {
                self.size.max(
                    offset.saturating_sub(1)
                        + text.as_deref().unwrap_or_default().chars().count() as u64,
                )
            };
            return Ok(());
        }
        let Some(data) = self.data.as_ref() else {
            return Err(not_implemented("ThinLob.write persistent LOB"));
        };
        let locator = self.locator.lock().map_err(runtime_error)?.clone();
        let bytes = raw_bytes.as_ref().cloned().unwrap_or_else(|| {
            protocol_encode_lob_text(
                text.as_deref().unwrap_or_default(),
                self.csfrm,
                locator.as_deref(),
            )
        });
        let start = usize::try_from(offset.saturating_sub(1)).map_err(runtime_error)?;
        let mut data = data.lock().map_err(runtime_error)?;
        if start > data.len() {
            data.resize(start, 0);
        }
        let end = start.saturating_add(bytes.len());
        if end > data.len() {
            data.resize(end, 0);
        }
        data[start..end].copy_from_slice(&bytes);
        self.size = if matches!(self.ora_type_num, ORA_TYPE_NUM_BLOB | ORA_TYPE_NUM_BFILE) {
            data.len() as u64
        } else {
            protocol_decode_lob_text(&data, self.csfrm, None)
                .map_err(runtime_error)?
                .chars()
                .count() as u64
        };
        Ok(())
    }

    fn get_max_amount(&self) -> u64 {
        u64::from(u32::MAX)
    }

    fn get_size(&self) -> u64 {
        self.size
    }

    fn size(&self) -> u64 {
        self.get_size()
    }

    fn get_chunk_size(&self) -> u32 {
        self.chunk_size
    }

    #[getter]
    fn dbtype(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let module = PyModule::import(py, "oracledb")?;
        let name = match self.ora_type_num {
            ORA_TYPE_NUM_BLOB => "DB_TYPE_BLOB",
            ORA_TYPE_NUM_BFILE => "DB_TYPE_BFILE",
            ORA_TYPE_NUM_CLOB if self.csfrm == CS_FORM_NCHAR => "DB_TYPE_NCLOB",
            ORA_TYPE_NUM_CLOB => "DB_TYPE_CLOB",
            _ => "DB_TYPE_CLOB",
        };
        Ok(module.getattr(name)?.unbind())
    }

    fn free_lob(&self) -> PyResult<()> {
        let Some(context) = self.context.as_ref() else {
            return Ok(());
        };
        let locator = self.locator.lock().map_err(runtime_error)?.clone();
        let Some(locator) = locator.filter(|locator| lob_locator_is_temporary(locator)) else {
            return Ok(());
        };
        let call_timeout = {
            let value = context.state.lock().map_err(runtime_error)?.call_timeout;
            (value > 0).then_some(value)
        };
        let mut guard = context.connection.lock().map_err(runtime_error)?;
        let Some(connection) = guard.as_mut() else {
            *self.locator.lock().map_err(runtime_error)? = None;
            return Ok(());
        };
        BlockingConnection::free_temp_lobs_with_timeout(connection, &[locator], call_timeout)
            .map_err(runtime_error)?;
        *self.locator.lock().map_err(runtime_error)? = None;
        Ok(())
    }

    fn get_file_name(&self) -> PyResult<(String, String)> {
        Ok(self.bfile_name.clone().unwrap_or_default())
    }

    fn set_file_name(&mut self, dir_alias: String, name: String) {
        self.bfile_name = Some((dir_alias, name));
    }

    fn file_exists(&self) -> PyResult<bool> {
        Err(dpy_database_error(
            "ORA-22285",
            "non-existent directory or file for FILEOPEN operation",
        ))
    }

    fn close(&self) -> PyResult<()> {
        let mut is_open = self.is_open.lock().map_err(runtime_error)?;
        if !*is_open {
            return Err(runtime_error(
                "server returned Oracle error: ORA-22289: LOB is not open",
            ));
        }
        *is_open = false;
        Ok(())
    }

    fn open(&self) -> PyResult<()> {
        let mut is_open = self.is_open.lock().map_err(runtime_error)?;
        if *is_open {
            return Err(runtime_error(
                "server returned Oracle error: ORA-22293: LOB already open",
            ));
        }
        *is_open = true;
        Ok(())
    }

    fn get_is_open(&self) -> PyResult<bool> {
        Ok(*self.is_open.lock().map_err(runtime_error)?)
    }

    fn trim(&mut self, new_size: u64) -> PyResult<()> {
        if let Some(data) = self.data.as_ref() {
            let mut data = data.lock().map_err(runtime_error)?;
            if matches!(self.ora_type_num, ORA_TYPE_NUM_BLOB | ORA_TYPE_NUM_BFILE) {
                data.truncate(usize::try_from(new_size).unwrap_or(usize::MAX));
            } else {
                let text = protocol_decode_lob_text(
                    &data,
                    self.csfrm,
                    self.locator.lock().map_err(runtime_error)?.as_deref(),
                )
                .map_err(runtime_error)?;
                let text = text
                    .chars()
                    .take(usize::try_from(new_size).unwrap_or(usize::MAX))
                    .collect::<String>();
                let locator = self.locator.lock().map_err(runtime_error)?.clone();
                *data = protocol_encode_lob_text(&text, self.csfrm, locator.as_deref());
            }
            self.size = new_size;
            return Ok(());
        }
        let Some(context) = self.context.as_ref() else {
            self.size = new_size;
            return Ok(());
        };
        let locator = self
            .locator
            .lock()
            .map_err(runtime_error)?
            .clone()
            .unwrap_or_default();
        let call_timeout = {
            let value = context.state.lock().map_err(runtime_error)?.call_timeout;
            (value > 0).then_some(value)
        };
        let mut guard = context.connection.lock().map_err(runtime_error)?;
        let connection = guard.as_mut().ok_or_else(connection_closed_error)?;
        let result =
            BlockingConnection::trim_lob_with_timeout(connection, &locator, new_size, call_timeout)
                .map_err(runtime_error)?;
        *self.locator.lock().map_err(runtime_error)? = Some(result.locator);
        self.size = new_size;
        Ok(())
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "AsyncThinLob")]
struct AsyncThinLob {
    inner: ThinLob,
}

#[pymethods]
impl AsyncThinLob {
    #[pyo3(signature = (offset=1, amount=None))]
    async fn read(&self, offset: u64, amount: Option<u64>) -> PyResult<Py<PyAny>> {
        Python::attach(|py| self.inner.read(py, offset, amount))
    }

    async fn write(&mut self, value: Py<PyAny>, offset: u64) -> PyResult<()> {
        Python::attach(|py| self.inner.write(value.bind(py), offset))
    }

    fn get_max_amount(&self) -> u64 {
        self.inner.get_max_amount()
    }

    async fn get_size(&self) -> u64 {
        self.inner.get_size()
    }

    async fn size(&self) -> u64 {
        self.inner.get_size()
    }

    async fn get_chunk_size(&self) -> u32 {
        self.inner.get_chunk_size()
    }

    #[getter]
    fn dbtype(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.inner.dbtype(py)
    }

    async fn file_exists(&self) -> PyResult<bool> {
        self.inner.file_exists()
    }

    async fn close(&self) -> PyResult<()> {
        self.inner.close()
    }

    async fn open(&self) -> PyResult<()> {
        self.inner.open()
    }

    async fn get_is_open(&self) -> PyResult<bool> {
        self.inner.get_is_open()
    }

    async fn trim(&mut self, new_size: u64) -> PyResult<()> {
        self.inner.trim(new_size)
    }

    fn free_lob(&self) -> PyResult<()> {
        self.inner.free_lob()
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "ThinVar")]
struct ThinVar {
    value: Arc<Mutex<Option<Py<PyAny>>>>,
    returned_values: Arc<Mutex<Option<Vec<Py<PyAny>>>>>,
    default_bind: BindValue,
    outconverter: Option<Py<PyAny>>,
    convert_nulls: bool,
    is_array: bool,
    num_elements: u32,
    return_kind: ThinVarReturnKind,
    object_type: Option<DbObjectTypeImpl>,
    object_return_attr: Option<String>,
    dbtype_name: String,
    bypass_decode: bool,
}

impl ThinVar {
    fn from_py_value(value: Option<Py<PyAny>>) -> Self {
        Self {
            value: Arc::new(Mutex::new(value)),
            returned_values: Arc::new(Mutex::new(None)),
            default_bind: BindValue::Null,
            outconverter: None,
            convert_nulls: false,
            is_array: false,
            num_elements: 1,
            return_kind: ThinVarReturnKind::Plain,
            object_type: None,
            object_return_attr: None,
            dbtype_name: "DB_TYPE_VARCHAR".to_string(),
            bypass_decode: false,
        }
    }

    fn typed_with_options(
        default_bind: BindValue,
        value: Option<Py<PyAny>>,
        is_array: bool,
        num_elements: u32,
        outconverter: Option<Py<PyAny>>,
        convert_nulls: bool,
        return_kind: ThinVarReturnKind,
        object_type: Option<DbObjectTypeImpl>,
        object_return_attr: Option<String>,
        dbtype_name: impl Into<String>,
        bypass_decode: bool,
    ) -> Self {
        Self {
            value: Arc::new(Mutex::new(value)),
            returned_values: Arc::new(Mutex::new(None)),
            default_bind,
            outconverter,
            convert_nulls,
            is_array,
            num_elements: num_elements.max(1),
            return_kind,
            object_type,
            object_return_attr,
            dbtype_name: dbtype_name.into(),
            bypass_decode,
        }
    }

    fn to_bind_value(&self, py: Python<'_>) -> PyResult<BindValue> {
        if self.is_array {
            let guard = self.value.lock().map_err(runtime_error)?;
            let values = if let Some(value) = guard.as_ref() {
                py_list_to_array_bind_values(value.bind(py))?
            } else {
                Vec::new()
            };
            let (ora_type_num, csfrm, buffer_size) = bind_type_info(&self.default_bind)
                .unwrap_or((ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT, 1));
            return Ok(BindValue::Array {
                ora_type_num,
                csfrm,
                buffer_size,
                max_elements: self.num_elements,
                values,
            });
        }
        if is_cursor_bind_template(&self.default_bind) {
            if let Some(value) = self.value.lock().map_err(runtime_error)?.as_ref() {
                validate_public_cursor_is_open(value.bind(py))?;
            }
            return Ok(self.default_bind.clone());
        }
        let guard = self.value.lock().map_err(runtime_error)?;
        let Some(value) = guard.as_ref() else {
            return Ok(self.default_bind.clone());
        };
        py_value_to_bind_with_template(value.bind(py), &self.default_bind)
    }

    fn set_py_value(&self, value: Option<Py<PyAny>>) -> PyResult<()> {
        *self.value.lock().map_err(runtime_error)? = value;
        *self.returned_values.lock().map_err(runtime_error)? = None;
        Ok(())
    }

    fn set_py_value_checked(&self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let bound = value.bind(py);
        if matches!(
            self.dbtype_name.as_str(),
            "DB_TYPE_ROWID" | "DB_TYPE_UROWID"
        ) && !bound.is_none()
        {
            return Err(raise_unsupported_type_set(&self.dbtype_name));
        }
        if let Some(expected_type) = &self.object_type {
            if !bound.is_none() {
                let Some(actual_object) = py_db_object_impl(bound)? else {
                    return Err(raise_unsupported_python_type_for_db_type(
                        bound,
                        "DB_TYPE_OBJECT",
                    ));
                };
                let actual_type = actual_object.object_type.clone();
                if &actual_type != expected_type {
                    return Err(raise_wrong_object_type(&actual_type, expected_type));
                }
            }
        }
        self.set_py_value(Some(value))
    }

    fn clear_returned_values(&self) -> PyResult<()> {
        *self.returned_values.lock().map_err(runtime_error)? = None;
        Ok(())
    }

    fn push_returned_py_value(&self, value: Py<PyAny>) -> PyResult<()> {
        *self.value.lock().map_err(runtime_error)? = None;
        let mut guard = self.returned_values.lock().map_err(runtime_error)?;
        guard.get_or_insert_with(Vec::new).push(value);
        Ok(())
    }

    fn check_position(&self, pos: u32) -> PyResult<()> {
        if pos >= self.num_elements {
            return Err(PyIndexError::new_err("variable position out of range"));
        }
        Ok(())
    }

    fn get_py_value_at(&self, py: Python<'_>, pos: u32) -> PyResult<Py<PyAny>> {
        self.check_position(pos)?;
        if let Some(values) = self.returned_values.lock().map_err(runtime_error)?.as_ref() {
            let index = usize::try_from(pos).map_err(runtime_error)?;
            return Ok(values
                .get(index)
                .map(|value| value.clone_ref(py))
                .unwrap_or_else(|| py.None()));
        }
        if let Some(value) = self.value.lock().map_err(runtime_error)?.as_ref() {
            let bound = value.bind(py);
            if let Some(lob) = scalar_value_to_memory_lob(py, bound, &self.dbtype_name)? {
                return Ok(lob);
            }
            return Ok(value.clone_ref(py));
        }
        if self.is_array {
            return Ok(PyList::empty(py).unbind().into());
        }
        Ok(py.None())
    }

    fn get_py_value(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.get_py_value_at(py, 0)
    }

    fn dbtype(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let module = PyModule::import(py, "oracledb")?;
        Ok(module.getattr(&self.dbtype_name)?.unbind())
    }

    fn repr_value(&self, py: Python<'_>) -> PyResult<String> {
        let values = self.get_all_values(py)?;
        let value = if !self.is_array && values.len() == 1 {
            values
                .first()
                .map(|value| value.clone_ref(py))
                .unwrap_or_else(|| py.None())
        } else {
            PyList::new(py, values)?.unbind().into()
        };
        value.bind(py).repr()?.extract()
    }

    fn output_value_to_py(
        &self,
        py: Python<'_>,
        value: &Option<QueryValue>,
        lob_context: Option<&ThinLobContext>,
    ) -> PyResult<Py<PyAny>> {
        let value = match (self.return_kind, value) {
            (ThinVarReturnKind::ClobAsLong, Some(QueryValue::Text(value))) => py_lob_from_impl(
                py,
                ThinLob {
                    data: Some(Arc::new(Mutex::new(value.as_bytes().to_vec()))),
                    locator: Arc::new(Mutex::new(None)),
                    ora_type_num: ORA_TYPE_NUM_CLOB,
                    csfrm: CS_FORM_IMPLICIT,
                    size: value.chars().count() as u64,
                    chunk_size: 0,
                    context: None,
                    is_open: Arc::new(Mutex::new(false)),
                    bfile_name: None,
                },
            )?,
            (ThinVarReturnKind::Plain, Some(QueryValue::Text(value))) if self.bypass_decode => {
                PyBytes::new(py, value.as_bytes()).unbind().into()
            }
            (ThinVarReturnKind::Plain, Some(QueryValue::Text(value)))
                if self.dbtype_name == "DB_TYPE_BINARY_INTEGER" =>
            {
                python_int_from_decimal_text(py, value)?
            }
            (ThinVarReturnKind::Plain, Some(QueryValue::Number { text, .. }))
                if self.dbtype_name == "DB_TYPE_BINARY_INTEGER" =>
            {
                python_int_from_decimal_text(py, text)?
            }
            (ThinVarReturnKind::Plain, Some(QueryValue::Number { text, .. }))
                if matches!(
                    self.dbtype_name.as_str(),
                    "DB_TYPE_CHAR"
                        | "DB_TYPE_LONG"
                        | "DB_TYPE_LONG_NVARCHAR"
                        | "DB_TYPE_NCHAR"
                        | "DB_TYPE_NVARCHAR"
                        | "DB_TYPE_VARCHAR"
                ) =>
            {
                text.clone().into_pyobject(py)?.unbind().into()
            }
            (ThinVarReturnKind::Plain, Some(QueryValue::Text(value)))
                if self.object_type.is_some() =>
            {
                let object_type = self
                    .object_type
                    .clone()
                    .ok_or_else(|| PyRuntimeError::new_err("missing object type"))?;
                let attr_name = self
                    .object_return_attr
                    .clone()
                    .or_else(|| object_type.default_scalar_return_attr().map(str::to_string))
                    .ok_or_else(|| {
                        not_implemented("ThinVar object DML RETURNING projection metadata")
                    })?;
                let object = DbObjectImpl::with_attr(py, object_type, &attr_name, value.clone())?;
                py_db_object_from_impl(py, object)?
            }
            (
                ThinVarReturnKind::Plain,
                Some(QueryValue::Object {
                    packed_data,
                    schema: _,
                    type_name: _,
                }),
            ) if self.object_type.is_some() => {
                let object_type = self
                    .object_type
                    .clone()
                    .ok_or_else(|| PyRuntimeError::new_err("missing object type"))?;
                py_db_object_from_impl(
                    py,
                    DbObjectImpl::with_packed_data(object_type, packed_data.clone(), None),
                )?
            }
            _ => query_value_to_py(py, value, None, lob_context, true)?,
        };
        if let Some(outconverter) = self.outconverter.as_ref() {
            if !value.bind(py).is_none() || self.convert_nulls {
                return Ok(outconverter.bind(py).call1((value,))?.unbind());
            }
        }
        Ok(value)
    }
}

#[pymethods]
impl ThinVar {
    #[new]
    fn new() -> Self {
        Self::from_py_value(None)
    }

    #[pyo3(signature = (pos=None))]
    fn getvalue(&self, py: Python<'_>, pos: Option<u32>) -> PyResult<Py<PyAny>> {
        self.get_py_value_at(py, pos.unwrap_or(0))
    }

    fn get_value(&self, py: Python<'_>, pos: u32) -> PyResult<Py<PyAny>> {
        self.get_py_value_at(py, pos)
    }

    fn get_all_values(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        if let Some(values) = self.returned_values.lock().map_err(runtime_error)?.as_ref() {
            return Ok(values.iter().map(|value| value.clone_ref(py)).collect());
        }
        Ok(vec![self.get_py_value(py)?])
    }

    #[getter]
    fn values(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        self.get_all_values(py)
    }

    fn setvalue(&self, py: Python<'_>, pos: u32, value: Py<PyAny>) -> PyResult<()> {
        self.check_position(pos)?;
        self.set_py_value_checked(py, value)
    }

    fn set_value(&self, py: Python<'_>, pos: u32, value: Py<PyAny>) -> PyResult<()> {
        self.check_position(pos)?;
        self.set_py_value_checked(py, value)
    }

    #[getter]
    fn r#type(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.dbtype(py)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "<oracledb.Var of type {} with value {}>",
            self.dbtype_name,
            self.repr_value(py)?
        ))
    }

    fn __str__(&self, py: Python<'_>) -> PyResult<String> {
        self.__repr__(py)
    }
}

fn thin_var_from_value(value: &Bound<'_, PyAny>) -> PyResult<Option<Py<ThinVar>>> {
    if let Ok(var) = value.extract::<Py<ThinVar>>() {
        return Ok(Some(var));
    }
    if value.hasattr("_impl")? {
        let impl_obj = value.getattr("_impl")?;
        if let Ok(var) = impl_obj.extract::<Py<ThinVar>>() {
            return Ok(Some(var));
        }
    }
    Ok(None)
}

fn bind_var_from_value(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<ThinVar>> {
    if let Some(var) = thin_var_from_value(value)? {
        return Ok(var);
    }
    let type_name = py_type_name(value);
    if !type_name.is_empty() {
        let default_bind = bind_template_from_type_name(&type_name, 0);
        if !matches!(default_bind, BindValue::Null) {
            return Py::new(
                py,
                ThinVar::typed_with_options(
                    default_bind,
                    None,
                    false,
                    1,
                    None,
                    false,
                    ThinVarReturnKind::Plain,
                    None,
                    None,
                    public_dbtype_name_from_type_name(&type_name),
                    false,
                ),
            );
        }
    }
    Py::new(py, ThinVar::from_py_value(Some(value.clone().unbind())))
}

fn py_value_to_execute_bind(value: &Bound<'_, PyAny>) -> PyResult<BindValue> {
    if let Some(var) = thin_var_from_value(value)? {
        let bind = var.borrow(value.py()).to_bind_value(value.py())?;
        if is_cursor_bind_template(&bind) {
            return Ok(output_only_bind(bind));
        }
        return Ok(bind);
    }
    py_value_to_bind(value)
}

fn apply_out_bind_values(
    py: Python<'_>,
    bind_vars: &[Py<ThinVar>],
    out_values: &[(usize, Option<QueryValue>)],
    return_values: &[(usize, Vec<Option<QueryValue>>)],
    lob_context: Option<&ThinLobContext>,
) -> PyResult<()> {
    for (index, value) in out_values {
        let Some(var) = bind_vars.get(*index) else {
            continue;
        };
        if let Some(QueryValue::Cursor { columns, cursor_id }) = value {
            apply_cursor_out_bind(py, var, columns, *cursor_id)?;
            continue;
        }
        let value = var.borrow(py).output_value_to_py(py, value, lob_context)?;
        var.borrow(py).set_py_value(Some(value))?;
    }
    for (index, _) in return_values {
        let Some(var) = bind_vars.get(*index) else {
            continue;
        };
        var.borrow(py).clear_returned_values()?;
    }
    for (index, values) in return_values {
        let Some(var) = bind_vars.get(*index) else {
            continue;
        };
        let var_ref = var.borrow(py);
        let values = values
            .iter()
            .map(|value| var_ref.output_value_to_py(py, value, lob_context))
            .collect::<PyResult<Vec<_>>>()?;
        drop(var_ref);
        let values = PyList::new(py, values)?.unbind().into();
        var.borrow(py).push_returned_py_value(values)?;
    }
    Ok(())
}

fn apply_cursor_out_bind(
    py: Python<'_>,
    var: &Py<ThinVar>,
    columns: &[ColumnMetadata],
    cursor_id: u32,
) -> PyResult<()> {
    let cursor = var.borrow(py).get_py_value(py)?;
    let cursor = cursor.bind(py);
    hydrate_cursor_impl(cursor, columns, cursor_id, cursor_id == 0)
}

#[pyclass(module = "oracledb.thin_impl", name = "FetchHandlerCursor")]
struct FetchHandlerCursor {
    connection: Py<PyAny>,
    arraysize: u32,
}

#[pymethods]
impl FetchHandlerCursor {
    #[getter]
    fn arraysize(&self) -> u32 {
        self.arraysize
    }

    #[getter]
    fn connection(&self, py: Python<'_>) -> Py<PyAny> {
        self.connection.clone_ref(py)
    }

    #[pyo3(signature = (
        typ,
        size=0,
        arraysize=1,
        inconverter=None,
        outconverter=None,
        encoding_errors=None,
        bypass_decode=false,
        convert_nulls=false
    ))]
    fn var(
        &self,
        py: Python<'_>,
        typ: &Bound<'_, PyAny>,
        size: u32,
        arraysize: u32,
        inconverter: Option<Py<PyAny>>,
        outconverter: Option<Py<PyAny>>,
        encoding_errors: Option<String>,
        bypass_decode: bool,
        convert_nulls: bool,
    ) -> PyResult<Py<ThinVar>> {
        let _ = arraysize;
        let _ = inconverter;
        let _ = encoding_errors;
        thin_var_from_type_spec(
            py,
            self.connection.bind(py),
            typ,
            size,
            false,
            1,
            outconverter,
            convert_nulls,
            bypass_decode,
        )
    }
}

fn hydrate_cursor_impl(
    cursor: &Bound<'_, PyAny>,
    columns: &[ColumnMetadata],
    cursor_id: u32,
    invalid_ref_cursor: bool,
) -> PyResult<()> {
    fn hydrate(
        cursor_impl: &mut ThinCursorImpl,
        columns: &[ColumnMetadata],
        cursor_id: u32,
        invalid_ref_cursor: bool,
    ) {
        cursor_impl.columns = columns.to_vec();
        cursor_impl.reset_fetch_define_state();
        cursor_impl.rows.clear();
        cursor_impl.row_index = 0;
        cursor_impl.cursor_id = cursor_id;
        cursor_impl.more_rows = cursor_id != 0;
        cursor_impl.invalid_ref_cursor = invalid_ref_cursor;
        cursor_impl.rowcount = 0;
        cursor_impl.is_query = true;
    }

    let impl_obj = cursor.getattr("_impl")?;
    if let Ok(mut cursor_impl) = impl_obj.extract::<PyRefMut<'_, ThinCursorImpl>>() {
        hydrate(&mut cursor_impl, columns, cursor_id, invalid_ref_cursor);
        return Ok(());
    }
    let mut cursor_impl = impl_obj.extract::<PyRefMut<'_, AsyncThinCursorImpl>>()?;
    hydrate(
        &mut cursor_impl.inner,
        columns,
        cursor_id,
        invalid_ref_cursor,
    );
    Ok(())
}

#[pyfunction]
fn init_thin_impl(_package: &Bound<'_, PyAny>) -> PyResult<()> {
    Ok(())
}

fn apply_pending_current_schema_from_state(
    state: &Arc<Mutex<ThinConnState>>,
    connection: &mut RustConnection,
    call_timeout: Option<u32>,
) -> PyResult<()> {
    let pending_schema = {
        let mut state = state.lock().map_err(runtime_error)?;
        if !state.current_schema_modified {
            None
        } else {
            state.current_schema_modified = false;
            state.current_schema.clone()
        }
    };
    let Some(schema) = pending_schema else {
        return Ok(());
    };
    let identifier = sql_identifier(&schema)?;
    let result = BlockingConnection::execute_query_with_timeout(
        connection,
        &format!("alter session set current_schema = {identifier}"),
        1,
        call_timeout,
    )
    .map_err(runtime_error);
    if result.is_err() {
        state.lock().map_err(runtime_error)?.current_schema_modified = true;
    }
    result.map(|_| ())
}

#[pyclass(module = "oracledb.thin_impl", name = "ThinConnImpl")]
struct ThinConnImpl {
    connection: Arc<Mutex<Option<RustConnection>>>,
    cancel_handle: Arc<Mutex<Option<CancelHandle>>>,
    cancel_requested: Arc<AtomicBool>,
    state: Arc<Mutex<ThinConnState>>,
    dsn: String,
    username: String,
    proxy_user: Option<String>,
    server_version: (u8, u8, u8, u8, u8),
    autocommit: bool,
    autocommit_state: Arc<Mutex<bool>>,
    tag: Option<String>,
    warning: Option<Py<PyAny>>,
    inputtypehandler: Option<Py<PyAny>>,
    outputtypehandler: Option<Py<PyAny>>,
    invoke_session_callback: bool,
    thin: bool,
    connect_password: Option<String>,
    new_password: Option<String>,
}

impl ThinConnImpl {
    fn apply_pending_current_schema(
        &self,
        connection: &mut RustConnection,
        call_timeout: Option<u32>,
    ) -> PyResult<()> {
        apply_pending_current_schema_from_state(&self.state, connection, call_timeout)
    }

    fn execute_with_binds(&self, sql: &str, binds: &[BindValue]) -> PyResult<QueryResult> {
        let call_timeout = self.call_timeout()?;
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        self.apply_pending_current_schema(connection, call_timeout)?;
        BlockingConnection::execute_query_with_binds_and_timeout(
            connection,
            sql,
            1,
            binds,
            call_timeout,
        )
        .map_err(runtime_error)
    }

    fn execute_statement(&self, sql: &str) -> PyResult<()> {
        let call_timeout = self.call_timeout()?;
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        self.apply_pending_current_schema(connection, call_timeout)?;
        BlockingConnection::execute_query_with_timeout(connection, sql, 1, call_timeout)
            .map_err(runtime_error)?;
        Ok(())
    }

    fn execute_statement_with_binds(&self, sql: &str, binds: &[BindValue]) -> PyResult<()> {
        self.execute_with_binds(sql, binds)?;
        Ok(())
    }

    fn query_first_value(&self, sql: &str) -> PyResult<Option<QueryValue>> {
        let call_timeout = self.call_timeout()?;
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        self.apply_pending_current_schema(connection, call_timeout)?;
        let result =
            BlockingConnection::execute_query_with_timeout(connection, sql, 1, call_timeout)
                .map_err(runtime_error)?;
        Ok(result
            .rows
            .first()
            .and_then(|row| row.first())
            .cloned()
            .flatten())
    }

    fn query_first_row_with_binds(
        &self,
        sql: &str,
        binds: &[BindValue],
    ) -> PyResult<Option<Vec<Option<QueryValue>>>> {
        let result = self.execute_with_binds(sql, binds)?;
        Ok(result.rows.into_iter().next())
    }

    fn query_rows_with_binds(
        &self,
        sql: &str,
        binds: &[BindValue],
    ) -> PyResult<Vec<Vec<Option<QueryValue>>>> {
        let call_timeout = self.call_timeout()?;
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        self.apply_pending_current_schema(connection, call_timeout)?;
        let result = BlockingConnection::execute_query_with_binds_and_timeout(
            connection,
            sql,
            100,
            binds,
            call_timeout,
        )
        .map_err(runtime_error)?;
        Ok(result.rows)
    }

    fn query_first_text(&self, sql: &str) -> PyResult<Option<String>> {
        self.query_first_value(sql)
            .map(|value| query_value_to_string(&value))
    }

    fn query_first_i64(&self, sql: &str) -> PyResult<i64> {
        let value = self.query_first_value(sql)?;
        query_value_to_i64(&value)
    }

    fn object_type_attrs(&self, schema: &str, type_name: &str) -> PyResult<Vec<DbObjectAttrImpl>> {
        let rows = self.query_rows_with_binds(
            "select attr_name, attr_type_name, length, precision, scale, attr_type_owner \
             from all_type_attrs \
             where owner = :1 and type_name = :2 \
             order by attr_no",
            &[
                BindValue::Text(schema.to_ascii_uppercase()),
                BindValue::Text(type_name.to_ascii_uppercase()),
            ],
        )?;
        rows.into_iter()
            .map(|row| {
                let name = row
                    .first()
                    .and_then(query_value_to_string)
                    .unwrap_or_default()
                    .to_ascii_uppercase();
                let attr_type_name = row
                    .get(1)
                    .and_then(query_value_to_string)
                    .unwrap_or_else(|| "VARCHAR2".to_string());
                let attr_type_owner = row
                    .get(5)
                    .and_then(query_value_to_string)
                    .unwrap_or_else(|| schema.to_ascii_uppercase());
                let dbtype_name = public_dbtype_name_from_oracle_type_name(&attr_type_name);
                let (precision, scale) = dbobject_attr_precision_scale(
                    &attr_type_name,
                    row.get(3).and_then(query_value_to_i8),
                    row.get(4).and_then(query_value_to_i8),
                );
                Ok(DbObjectAttrImpl {
                    name,
                    dbtype_name: dbtype_name.to_string(),
                    objtype: if dbtype_name == "DB_TYPE_OBJECT" {
                        Some(self.object_type_shallow(&attr_type_owner, &attr_type_name)?)
                    } else {
                        None
                    },
                    max_size: dbobject_attr_max_size(
                        &attr_type_name,
                        row.get(2).and_then(query_value_to_u32),
                    ),
                    precision,
                    scale,
                })
            })
            .collect()
    }

    fn plsql_type_attrs(
        &self,
        schema: &str,
        package_name: &str,
        type_name: &str,
    ) -> PyResult<Vec<DbObjectAttrImpl>> {
        let rows = self.query_rows_with_binds(
            "select attr_name, attr_type_owner, attr_type_package, attr_type_name, length, precision, scale \
             from all_plsql_type_attrs \
             where owner = :1 and package_name = :2 and type_name = :3 \
             order by attr_no",
            &[
                BindValue::Text(schema.to_ascii_uppercase()),
                BindValue::Text(package_name.to_ascii_uppercase()),
                BindValue::Text(type_name.to_ascii_uppercase()),
            ],
        )?;
        rows.into_iter()
            .map(|row| {
                let name = row
                    .first()
                    .and_then(query_value_to_string)
                    .unwrap_or_default()
                    .to_ascii_uppercase();
                let attr_type_owner = row
                    .get(1)
                    .and_then(query_value_to_string)
                    .unwrap_or_else(|| schema.to_ascii_uppercase());
                let attr_type_package = row.get(2).and_then(query_value_to_string);
                let attr_type_name = row
                    .get(3)
                    .and_then(query_value_to_string)
                    .unwrap_or_else(|| "VARCHAR2".to_string());
                let dbtype_name = public_dbtype_name_from_oracle_type_name(&attr_type_name);
                let (precision, scale) = dbobject_attr_precision_scale(
                    &attr_type_name,
                    row.get(5).and_then(query_value_to_i8),
                    row.get(6).and_then(query_value_to_i8),
                );
                let objtype = if dbtype_name == "DB_TYPE_OBJECT" {
                    if let Some(attr_type_package) = attr_type_package {
                        Some(self.plsql_type_shallow(
                            &attr_type_owner,
                            &attr_type_package,
                            &attr_type_name,
                        )?)
                    } else {
                        Some(self.object_type_shallow(&attr_type_owner, &attr_type_name)?)
                    }
                } else {
                    None
                };
                Ok(DbObjectAttrImpl {
                    name,
                    dbtype_name: dbtype_name.to_string(),
                    objtype,
                    max_size: dbobject_attr_max_size(
                        &attr_type_name,
                        row.get(4).and_then(query_value_to_u32),
                    ),
                    precision,
                    scale,
                })
            })
            .collect()
    }

    fn rowtype_attrs(&self, schema: &str, table_name: &str) -> PyResult<Vec<DbObjectAttrImpl>> {
        let rows = self.query_rows_with_binds(
            "select column_name, data_type, data_length, data_precision, data_scale, data_type_owner, char_length \
             from all_tab_cols \
             where owner = :1 and table_name = :2 and hidden_column = 'NO' \
             order by internal_column_id",
            &[
                BindValue::Text(schema.to_ascii_uppercase()),
                BindValue::Text(table_name.to_ascii_uppercase()),
            ],
        )?;
        if rows.is_empty() {
            return Err(raise_invalid_object_type_name(&format!(
                "{table_name}%ROWTYPE"
            )));
        }
        rows.into_iter()
            .map(|row| {
                let name = row
                    .first()
                    .and_then(query_value_to_string)
                    .unwrap_or_default()
                    .to_ascii_uppercase();
                let data_type = row
                    .get(1)
                    .and_then(query_value_to_string)
                    .unwrap_or_else(|| "VARCHAR2".to_string());
                let data_type_owner = row
                    .get(5)
                    .and_then(query_value_to_string)
                    .unwrap_or_else(|| schema.to_ascii_uppercase());
                let dbtype_name = public_dbtype_name_from_oracle_type_name(&data_type);
                let (precision, scale) = dbobject_attr_precision_scale(
                    &data_type,
                    row.get(3).and_then(query_value_to_i8),
                    row.get(4).and_then(query_value_to_i8),
                );
                Ok(DbObjectAttrImpl {
                    name,
                    dbtype_name: dbtype_name.to_string(),
                    objtype: if dbtype_name == "DB_TYPE_OBJECT" {
                        Some(self.object_type_shallow(&data_type_owner, &data_type)?)
                    } else {
                        None
                    },
                    max_size: dbobject_rowtype_attr_max_size(
                        &data_type,
                        row.get(2).and_then(query_value_to_u32),
                        row.get(6).and_then(query_value_to_u32),
                    ),
                    precision,
                    scale,
                })
            })
            .collect()
    }

    fn rowtype(
        &self,
        schema: &str,
        table_name: &str,
        original_name: &str,
    ) -> PyResult<DbObjectTypeImpl> {
        let attrs = self.rowtype_attrs(schema, table_name)?;
        Ok(DbObjectTypeImpl::new(
            schema.to_ascii_uppercase(),
            None,
            original_name.to_ascii_uppercase(),
            "OBJECT",
            attrs,
            None,
            0,
            false,
        ))
    }

    fn object_type_collection_metadata(
        &self,
        schema: &str,
        type_name: &str,
    ) -> PyResult<(Option<DbObjectAttrImpl>, u32, bool)> {
        let Some(row) = self.query_first_row_with_binds(
            "select elem_type_owner, elem_type_name, length, precision, scale, upper_bound \
             from all_coll_types \
             where owner = :1 and type_name = :2",
            &[
                BindValue::Text(schema.to_ascii_uppercase()),
                BindValue::Text(type_name.to_ascii_uppercase()),
            ],
        )?
        else {
            return Ok((None, 0, false));
        };
        let elem_type_owner = row
            .first()
            .and_then(query_value_to_string)
            .unwrap_or_else(|| schema.to_ascii_uppercase());
        let elem_type_name = row
            .get(1)
            .and_then(query_value_to_string)
            .unwrap_or_else(|| "VARCHAR2".to_string());
        let dbtype_name = public_dbtype_name_from_oracle_type_name(&elem_type_name);
        let (precision, scale) = dbobject_attr_precision_scale(
            &elem_type_name,
            row.get(3).and_then(query_value_to_i8),
            row.get(4).and_then(query_value_to_i8),
        );
        let element_metadata = DbObjectAttrImpl {
            name: String::new(),
            dbtype_name: dbtype_name.to_string(),
            objtype: if dbtype_name == "DB_TYPE_OBJECT" {
                Some(self.object_type_shallow(&elem_type_owner, &elem_type_name)?)
            } else {
                None
            },
            max_size: dbobject_attr_max_size(
                &elem_type_name,
                row.get(2).and_then(query_value_to_u32),
            ),
            precision,
            scale,
        };
        let max_num_elements = row.get(5).and_then(query_value_to_u32).unwrap_or(0);
        Ok((Some(element_metadata), max_num_elements, false))
    }

    fn plsql_type_collection_metadata(
        &self,
        schema: &str,
        package_name: &str,
        type_name: &str,
    ) -> PyResult<(Option<DbObjectAttrImpl>, u32, bool)> {
        let Some(row) = self.query_first_row_with_binds(
            "select elem_type_owner, elem_type_package, elem_type_name, length, precision, scale, upper_bound, coll_type, index_by \
             from all_plsql_coll_types \
             where owner = :1 and package_name = :2 and type_name = :3",
            &[
                BindValue::Text(schema.to_ascii_uppercase()),
                BindValue::Text(package_name.to_ascii_uppercase()),
                BindValue::Text(type_name.to_ascii_uppercase()),
            ],
        )?
        else {
            return Ok((None, 0, false));
        };
        let elem_type_owner = row
            .first()
            .and_then(query_value_to_string)
            .unwrap_or_else(|| schema.to_ascii_uppercase());
        let elem_type_package = row.get(1).and_then(query_value_to_string);
        let elem_type_name = row
            .get(2)
            .and_then(query_value_to_string)
            .unwrap_or_else(|| "VARCHAR2".to_string());
        let dbtype_name = public_dbtype_name_from_oracle_type_name(&elem_type_name);
        let (precision, scale) = dbobject_attr_precision_scale(
            &elem_type_name,
            row.get(4).and_then(query_value_to_i8),
            row.get(5).and_then(query_value_to_i8),
        );
        let objtype = if dbtype_name == "DB_TYPE_OBJECT" {
            if let Some(elem_type_package) = elem_type_package {
                Some(self.plsql_type_shallow(
                    &elem_type_owner,
                    &elem_type_package,
                    &elem_type_name,
                )?)
            } else {
                Some(self.object_type_shallow(&elem_type_owner, &elem_type_name)?)
            }
        } else {
            None
        };
        let element_metadata = DbObjectAttrImpl {
            name: String::new(),
            dbtype_name: dbtype_name.to_string(),
            objtype,
            max_size: dbobject_attr_max_size(
                &elem_type_name,
                row.get(3).and_then(query_value_to_u32),
            ),
            precision,
            scale,
        };
        let max_num_elements = row.get(6).and_then(query_value_to_u32).unwrap_or(0);
        let coll_type = row
            .get(7)
            .and_then(query_value_to_string)
            .unwrap_or_default();
        let is_assoc_array = coll_type.eq_ignore_ascii_case("PL/SQL INDEX TABLE")
            || row.get(8).and_then(query_value_to_string).is_some();
        Ok((Some(element_metadata), max_num_elements, is_assoc_array))
    }

    fn type_shape_identity(
        &self,
        full_name: &str,
        oid_from_catalog: Option<Vec<u8>>,
    ) -> PyResult<(Option<Vec<u8>>, u32)> {
        let result = self.execute_with_binds(
            "declare \
                 t_instantiable varchar2(3); \
                 t_super_type_owner varchar2(128); \
                 t_super_type_name varchar2(128); \
                 t_subtype_ref_cursor sys_refcursor; \
             begin \
                 :1 := dbms_pickler.get_type_shape(:2, :3, :4, :5, \
                     t_instantiable, t_super_type_owner, t_super_type_name, \
                     :6, t_subtype_ref_cursor); \
             end;",
            &[
                BindValue::Output {
                    ora_type_num: ORA_TYPE_NUM_NUMBER,
                    csfrm: 0,
                    buffer_size: 22,
                },
                BindValue::Text(full_name.to_string()),
                BindValue::Output {
                    ora_type_num: ORA_TYPE_NUM_RAW,
                    csfrm: 0,
                    buffer_size: 64,
                },
                BindValue::Output {
                    ora_type_num: ORA_TYPE_NUM_NUMBER,
                    csfrm: 0,
                    buffer_size: 22,
                },
                BindValue::Output {
                    ora_type_num: ORA_TYPE_NUM_RAW,
                    csfrm: 0,
                    buffer_size: 32767,
                },
                BindValue::Output {
                    ora_type_num: ORA_TYPE_NUM_CURSOR,
                    csfrm: 0,
                    buffer_size: 4,
                },
            ],
        )?;
        let oid = result
            .out_values
            .iter()
            .find_map(|(index, value)| match (index, value) {
                (2, Some(QueryValue::Raw(bytes))) => Some(bytes.clone()),
                _ => None,
            })
            .or(oid_from_catalog);
        let version = result
            .out_values
            .iter()
            .find_map(|(index, value)| {
                (*index == 3)
                    .then(|| {
                        query_value_to_i64(value)
                            .ok()
                            .and_then(|value| u32::try_from(value).ok())
                    })
                    .flatten()
            })
            .unwrap_or(0);
        Ok((oid, version))
    }

    fn object_type_identity(
        &self,
        schema: &str,
        type_name: &str,
    ) -> PyResult<(Option<Vec<u8>>, u32)> {
        let schema = schema.to_ascii_uppercase();
        let type_name = type_name.to_ascii_uppercase();
        let oid_from_catalog = self
            .query_first_row_with_binds(
                "select type_oid from all_types where owner = :1 and type_name = :2",
                &[
                    BindValue::Text(schema.clone()),
                    BindValue::Text(type_name.clone()),
                ],
            )?
            .and_then(|row| match row.first() {
                Some(Some(QueryValue::Raw(bytes))) => Some(bytes.clone()),
                _ => None,
            });
        self.type_shape_identity(&format!("{schema}.{type_name}"), oid_from_catalog)
    }

    fn plsql_type_identity(
        &self,
        schema: &str,
        package_name: &str,
        type_name: &str,
    ) -> PyResult<(Option<Vec<u8>>, u32)> {
        let schema = schema.to_ascii_uppercase();
        let package_name = package_name.to_ascii_uppercase();
        let type_name = type_name.to_ascii_uppercase();
        let oid_from_catalog = self
            .query_first_row_with_binds(
                "select type_oid from all_plsql_types \
                 where owner = :1 and package_name = :2 and type_name = :3",
                &[
                    BindValue::Text(schema.clone()),
                    BindValue::Text(package_name.clone()),
                    BindValue::Text(type_name.clone()),
                ],
            )?
            .and_then(|row| match row.first() {
                Some(Some(QueryValue::Raw(bytes))) => Some(bytes.clone()),
                _ => None,
            });
        self.type_shape_identity(
            &format!("{schema}.{package_name}.{type_name}"),
            oid_from_catalog,
        )
    }

    fn object_type_shallow(&self, schema: &str, type_name: &str) -> PyResult<DbObjectTypeImpl> {
        let typecode = self
            .query_first_row_with_binds(
                "select typecode from all_types where owner = :1 and type_name = :2",
                &[
                    BindValue::Text(schema.to_ascii_uppercase()),
                    BindValue::Text(type_name.to_ascii_uppercase()),
                ],
            )?
            .and_then(|row| row.first().and_then(query_value_to_string))
            .unwrap_or_else(|| "OBJECT".to_string());
        let (element_metadata, max_num_elements, is_assoc_array) =
            self.object_type_collection_metadata(schema, type_name)?;
        let attrs = if element_metadata.is_some() {
            Vec::new()
        } else {
            self.object_type_attrs(schema, type_name)?
        };
        let (oid, version) = self.object_type_identity(schema, type_name)?;
        Ok(DbObjectTypeImpl::new(
            schema.to_ascii_uppercase(),
            None,
            type_name.to_ascii_uppercase(),
            &typecode,
            attrs,
            element_metadata,
            max_num_elements,
            is_assoc_array,
        )
        .with_type_identity(oid, version))
    }

    fn plsql_type_shallow(
        &self,
        schema: &str,
        package_name: &str,
        type_name: &str,
    ) -> PyResult<DbObjectTypeImpl> {
        let typecode = self
            .query_first_row_with_binds(
                "select typecode from all_plsql_types \
                 where owner = :1 and package_name = :2 and type_name = :3",
                &[
                    BindValue::Text(schema.to_ascii_uppercase()),
                    BindValue::Text(package_name.to_ascii_uppercase()),
                    BindValue::Text(type_name.to_ascii_uppercase()),
                ],
            )?
            .and_then(|row| row.first().and_then(query_value_to_string))
            .unwrap_or_else(|| "OBJECT".to_string());
        let (element_metadata, max_num_elements, is_assoc_array) =
            self.plsql_type_collection_metadata(schema, package_name, type_name)?;
        let attrs = if element_metadata.is_some() {
            Vec::new()
        } else {
            self.plsql_type_attrs(schema, package_name, type_name)?
        };
        let (oid, version) = self.plsql_type_identity(schema, package_name, type_name)?;
        Ok(DbObjectTypeImpl::new(
            schema.to_ascii_uppercase(),
            Some(package_name.to_ascii_uppercase()),
            type_name.to_ascii_uppercase(),
            &typecode,
            attrs,
            element_metadata,
            max_num_elements,
            is_assoc_array,
        )
        .with_type_identity(oid, version))
    }

    fn plsql_type(
        &self,
        schema: &str,
        package_name: &str,
        type_name: &str,
        original_name: &str,
    ) -> PyResult<DbObjectTypeImpl> {
        let Some(row) = self.query_first_row_with_binds(
            "select owner, package_name, type_name, typecode \
             from all_plsql_types \
             where owner = :1 and package_name = :2 and type_name = :3",
            &[
                BindValue::Text(schema.to_ascii_uppercase()),
                BindValue::Text(package_name.to_ascii_uppercase()),
                BindValue::Text(type_name.to_ascii_uppercase()),
            ],
        )?
        else {
            return Err(raise_invalid_object_type_name(original_name));
        };
        let schema = row
            .first()
            .and_then(query_value_to_string)
            .unwrap_or_else(|| schema.to_ascii_uppercase());
        let package_name = row
            .get(1)
            .and_then(query_value_to_string)
            .unwrap_or_else(|| package_name.to_ascii_uppercase());
        let type_name = row
            .get(2)
            .and_then(query_value_to_string)
            .unwrap_or_else(|| type_name.to_ascii_uppercase());
        let typecode = row
            .get(3)
            .and_then(query_value_to_string)
            .unwrap_or_else(|| "OBJECT".to_string());
        let (element_metadata, max_num_elements, is_assoc_array) =
            self.plsql_type_collection_metadata(&schema, &package_name, &type_name)?;
        let attrs = if element_metadata.is_some() {
            Vec::new()
        } else {
            self.plsql_type_attrs(&schema, &package_name, &type_name)?
        };
        let (oid, version) = self.plsql_type_identity(&schema, &package_name, &type_name)?;
        Ok(DbObjectTypeImpl::new(
            schema.to_ascii_uppercase(),
            Some(package_name.to_ascii_uppercase()),
            type_name.to_ascii_uppercase(),
            &typecode,
            attrs,
            element_metadata,
            max_num_elements,
            is_assoc_array,
        )
        .with_type_identity(oid, version))
    }

    fn call_timeout(&self) -> PyResult<Option<u32>> {
        let call_timeout = self.state.lock().map_err(runtime_error)?.call_timeout;
        Ok((call_timeout > 0).then_some(call_timeout))
    }

    fn take_connection_for_close(&self) -> PyResult<Option<RustConnection>> {
        *self.cancel_handle.lock().map_err(runtime_error)? = None;
        Ok(self.connection.lock().map_err(runtime_error)?.take())
    }
}

#[pymethods]
impl ThinConnImpl {
    #[new]
    fn new(dsn: &Bound<'_, PyAny>, params_impl: &Bound<'_, PyAny>) -> PyResult<Self> {
        let dsn = if dsn.is_none() {
            std::env::var("PYO_TEST_CONNECT_STRING").unwrap_or_default()
        } else {
            dsn.extract()?
        };
        let invalid_connect_string = is_user_without_password_dsn(&dsn);
        let dsn = normalize_connect_string(dsn);
        let username = get_string_attr(params_impl, "user")?;
        let stmt_cache_size = get_optional_u32_attr(params_impl, "stmtcachesize")?.unwrap_or(20);
        let edition = get_optional_string_attr(params_impl, "edition")?;
        let connect_args = consume_next_connect_args()?;
        Ok(Self {
            connection: Arc::new(Mutex::new(None)),
            cancel_handle: Arc::new(Mutex::new(None)),
            cancel_requested: Arc::new(AtomicBool::new(false)),
            state: Arc::new(Mutex::new(ThinConnState::new(
                stmt_cache_size,
                edition,
                invalid_connect_string || connect_args.invalid_user_dsn,
            ))),
            dsn,
            username,
            proxy_user: get_optional_string_attr(params_impl, "proxy_user")?,
            server_version: (0, 0, 0, 0, 0),
            autocommit: false,
            autocommit_state: Arc::new(Mutex::new(false)),
            tag: None,
            warning: None,
            inputtypehandler: None,
            outputtypehandler: None,
            invoke_session_callback: false,
            thin: true,
            connect_password: connect_args.password,
            new_password: connect_args.new_password,
        })
    }

    #[getter]
    fn dsn(&self) -> &str {
        &self.dsn
    }

    #[getter]
    fn username(&self) -> &str {
        &self.username
    }

    #[getter]
    fn proxy_user(&self) -> Option<&str> {
        self.proxy_user.as_deref()
    }

    #[getter]
    fn thin(&self) -> bool {
        self.thin
    }

    #[getter]
    fn server_version(&self) -> (u8, u8, u8, u8, u8) {
        self.server_version
    }

    #[getter]
    fn warning(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.warning.as_ref().map(|value| value.clone_ref(py))
    }

    #[getter]
    fn autocommit(&self) -> bool {
        self.autocommit
    }

    #[setter]
    fn set_autocommit(&mut self, value: bool) -> PyResult<()> {
        self.autocommit = value;
        *self.autocommit_state.lock().map_err(runtime_error)? = value;
        Ok(())
    }

    #[getter]
    fn inputtypehandler(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.inputtypehandler
            .as_ref()
            .map(|value| value.clone_ref(py))
    }

    #[setter]
    fn set_inputtypehandler(&mut self, value: Option<Py<PyAny>>) {
        self.inputtypehandler = value;
    }

    #[getter]
    fn outputtypehandler(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.outputtypehandler
            .as_ref()
            .map(|value| value.clone_ref(py))
    }

    #[setter]
    fn set_outputtypehandler(&mut self, value: Option<Py<PyAny>>) {
        self.outputtypehandler = value;
    }

    #[getter]
    fn tag(&self) -> Option<&str> {
        self.tag.as_deref()
    }

    #[setter]
    fn set_tag(&mut self, value: Option<String>) {
        self.tag = value;
    }

    #[getter]
    fn invoke_session_callback(&self) -> bool {
        self.invoke_session_callback
    }

    #[setter]
    fn set_invoke_session_callback(&mut self, value: bool) {
        self.invoke_session_callback = value;
    }

    fn connect(&mut self, params_impl: &Bound<'_, PyAny>) -> PyResult<()> {
        if self
            .state
            .lock()
            .map_err(runtime_error)?
            .invalid_connect_string
        {
            return Err(dpy_database_error(
                "DPY-4000",
                "cannot connect with a username but no password in the connect string",
            ));
        }
        let program = get_string_attr(params_impl, "program")?;
        let machine = get_string_attr(params_impl, "machine")?;
        let terminal = get_string_attr(params_impl, "terminal")?;
        let osuser = get_string_attr(params_impl, "osuser")?;
        let driver_name = get_optional_string_attr(params_impl, "driver_name")?
            .unwrap_or_else(|| "rust-oracledb thn : 0.0.0".into());
        let password = self
            .connect_password
            .clone()
            .map(Ok)
            .unwrap_or_else(|| env_password_for_user(&self.username))?;
        let app_context = get_app_context_attr(params_impl)?;
        let edition = get_optional_string_attr(params_impl, "edition")?;
        let sdu = get_connect_sdu_attr(params_impl)?.unwrap_or(8192);
        if let Some(stmt_cache_size) = get_optional_u32_attr(params_impl, "stmtcachesize")? {
            self.state.lock().map_err(runtime_error)?.stmt_cache_size = stmt_cache_size;
        }
        let identity = ClientIdentity::new(program, machine, osuser, terminal, driver_name)
            .map_err(runtime_error)?;
        let options = ConnectOptions::new(
            self.dsn.clone(),
            self.username.clone(),
            password.clone(),
            identity,
        )
        .with_app_context(app_context)
        .with_sdu(sdu);
        let connection = BlockingConnection::connect(options).map_err(runtime_error)?;
        let cancel_handle = connection.cancel_handle().map_err(runtime_error)?;
        self.server_version = (0, 0, 0, 0, 0);
        *self.cancel_handle.lock().map_err(runtime_error)? = Some(cancel_handle);
        *self.connection.lock().map_err(runtime_error)? = Some(connection);
        if let Some(new_password) = &self.new_password {
            self.change_password(&password, new_password)?;
        }
        if let Some(edition) = edition {
            let identifier = sql_identifier(&edition)?;
            self.execute_statement(&format!("alter session set edition = {identifier}"))?;
            let mut state = self.state.lock().map_err(runtime_error)?;
            state.edition = Some(edition);
            state.edition_probe_started = true;
        }
        Ok(())
    }

    #[pyo3(signature = (in_del=None))]
    fn close(&self, in_del: Option<bool>) -> PyResult<()> {
        let _ = in_del;
        let Some(connection) = self.take_connection_for_close()? else {
            return Ok(());
        };
        close_result_to_py(close_connection_result(connection))
    }

    fn ping(&self) -> PyResult<()> {
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::ping(connection).map_err(runtime_error)
    }

    fn commit(&self) -> PyResult<()> {
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::commit(connection).map_err(runtime_error)?;
        self.state
            .lock()
            .map_err(runtime_error)?
            .transaction_in_progress = false;
        Ok(())
    }

    fn rollback(&self) -> PyResult<()> {
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::rollback(connection).map_err(runtime_error)?;
        self.state
            .lock()
            .map_err(runtime_error)?
            .transaction_in_progress = false;
        Ok(())
    }

    fn change_password(&self, old_password: &str, new_password: &str) -> PyResult<()> {
        if new_password.len() > 1024 {
            return Err(dpy_database_error(
                "ORA-00988",
                "missing or invalid password(s)",
            ));
        }
        let user = user_identifier(&self.username)?;
        let sql = format!(
            "alter user {user} identified by {} replace {}",
            quoted_oracle_string(new_password),
            quoted_oracle_string(old_password)
        );
        self.execute_statement(&sql)
            .and_then(|()| set_password_override_for_user(&self.username, new_password))
    }

    fn get_is_healthy(&self) -> PyResult<bool> {
        Ok(self.connection.lock().map_err(runtime_error)?.is_some())
    }

    fn get_sdu(&self) -> PyResult<u32> {
        let guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        Ok(u32::try_from(connection.sdu()).unwrap_or(u32::MAX))
    }

    fn get_type(&self, _conn: &Bound<'_, PyAny>, name: &str) -> PyResult<DbObjectTypeImpl> {
        let parts: Vec<&str> = name
            .split('.')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .collect();
        let requested_type_name = parts.last().copied().unwrap_or(name).to_ascii_uppercase();
        let requested_owner = (parts.len() == 2).then(|| parts[0].to_ascii_uppercase());
        if let Some(table_name) = requested_type_name.strip_suffix("%ROWTYPE") {
            let schema = requested_owner
                .clone()
                .unwrap_or_else(|| self.username.to_ascii_uppercase());
            return self.rowtype(&schema, table_name, name);
        }
        let mut sql = String::from(
            "select owner, type_name, typecode \
             from all_types \
             where type_name = :1",
        );
        let mut binds = vec![BindValue::Text(requested_type_name.clone())];
        if let Some(owner) = requested_owner {
            sql.push_str(" and owner = :2");
            binds.push(BindValue::Text(owner));
        } else {
            sql.push_str(" and owner = sys_context('USERENV', 'CURRENT_SCHEMA')");
        }
        sql.push_str(" order by owner");
        let Some(row) = self.query_first_row_with_binds(&sql, &binds)? else {
            return match parts.as_slice() {
                [package_name, type_name] => self.plsql_type(
                    &self.username.to_ascii_uppercase(),
                    package_name,
                    type_name,
                    name,
                ),
                [schema, package_name, type_name] => {
                    self.plsql_type(schema, package_name, type_name, name)
                }
                _ => Err(raise_invalid_object_type_name(name)),
            };
        };
        let schema = row
            .first()
            .and_then(query_value_to_string)
            .unwrap_or_else(|| self.username.to_ascii_uppercase());
        let type_name = row
            .get(1)
            .and_then(query_value_to_string)
            .unwrap_or(requested_type_name);
        let typecode = row
            .get(2)
            .and_then(query_value_to_string)
            .unwrap_or_else(|| "OBJECT".to_string());
        let attrs = self.object_type_attrs(&schema, &type_name)?;
        let (element_metadata, max_num_elements, is_assoc_array) =
            self.object_type_collection_metadata(&schema, &type_name)?;
        let (oid, version) = self.object_type_identity(&schema, &type_name)?;
        Ok(DbObjectTypeImpl::new(
            schema.to_ascii_uppercase(),
            None,
            type_name.to_ascii_uppercase(),
            &typecode,
            attrs,
            element_metadata,
            max_num_elements,
            is_assoc_array,
        )
        .with_type_identity(oid, version))
    }

    fn get_call_timeout(&self) -> PyResult<u32> {
        Ok(self.state.lock().map_err(runtime_error)?.call_timeout)
    }

    fn set_call_timeout(&self, value: u32) -> PyResult<()> {
        self.state.lock().map_err(runtime_error)?.call_timeout = value;
        Ok(())
    }

    fn clear_end_user_security_context(&self) -> PyResult<()> {
        Ok(())
    }

    fn set_end_user_security_context(&self, _context: &Bound<'_, PyAny>) -> PyResult<()> {
        if !self.dsn.to_ascii_lowercase().contains("tcps") {
            return Err(raise_oracledb_driver_error(
                "ERR_END_USER_SECURITY_CONTEXT_REQUIRES_TCPS",
            ));
        }
        Err(not_implemented(
            "ThinConnImpl.set_end_user_security_context",
        ))
    }

    fn cancel(&self) -> PyResult<()> {
        self.cancel_requested.store(true, Ordering::SeqCst);
        if let Some(cancel_handle) = self.cancel_handle.lock().map_err(runtime_error)?.as_mut() {
            cancel_handle.cancel().map_err(runtime_error)?;
        }
        Ok(())
    }

    fn get_ltxid<'py>(&self, py: Python<'py>) -> Py<PyBytes> {
        PyBytes::new(py, &[]).unbind()
    }

    fn get_current_schema(&self) -> PyResult<Option<String>> {
        Ok(self
            .state
            .lock()
            .map_err(runtime_error)?
            .current_schema
            .clone())
    }

    fn set_current_schema(&self, value: Option<String>) -> PyResult<()> {
        if let Some(value) = value {
            sql_identifier(&value)?;
            let mut state = self.state.lock().map_err(runtime_error)?;
            state.current_schema = Some(value);
            state.current_schema_modified = true;
        } else {
            let mut state = self.state.lock().map_err(runtime_error)?;
            state.current_schema = None;
            state.current_schema_modified = false;
        }
        Ok(())
    }

    fn get_edition(&self) -> PyResult<Option<String>> {
        {
            let mut state = self.state.lock().map_err(runtime_error)?;
            if state.edition.is_some() {
                return Ok(state.edition.clone());
            }
            if !state.edition_probe_started {
                state.edition_probe_started = true;
                return Ok(None);
            }
        }
        self.query_first_text("select sys_context('USERENV', 'CURRENT_EDITION_NAME') from dual")
    }

    fn get_external_name(&self) -> PyResult<Option<String>> {
        Ok(self
            .state
            .lock()
            .map_err(runtime_error)?
            .external_name
            .clone())
    }

    fn set_external_name(&self, value: Option<String>) -> PyResult<()> {
        self.state.lock().map_err(runtime_error)?.external_name = value;
        Ok(())
    }

    fn get_internal_name(&self) -> PyResult<Option<String>> {
        Ok(self
            .state
            .lock()
            .map_err(runtime_error)?
            .internal_name
            .clone())
    }

    fn set_internal_name(&self, value: Option<String>) -> PyResult<()> {
        self.state.lock().map_err(runtime_error)?.internal_name = value;
        Ok(())
    }

    fn get_max_identifier_length(&self) -> Option<u8> {
        Some(128)
    }

    fn get_instance_name(&self) -> PyResult<String> {
        Ok(self
            .query_first_text("select sys_context('userenv', 'instance_name') from dual")?
            .unwrap_or_default())
    }

    fn get_db_name(&self) -> PyResult<String> {
        Ok(self
            .query_first_text("select name from V$DATABASE")?
            .unwrap_or_default())
    }

    fn get_max_open_cursors(&self) -> PyResult<i64> {
        self.query_first_i64("select value from V$PARAMETER where name='open_cursors'")
    }

    fn get_service_name(&self) -> PyResult<String> {
        Ok(self
            .query_first_text("select sys_context('userenv', 'service_name') from dual")?
            .unwrap_or_default())
    }

    fn get_db_domain(&self) -> PyResult<Option<String>> {
        self.query_first_text("select value from V$PARAMETER where name='db_domain'")
    }

    fn get_stmt_cache_size(&self) -> PyResult<u32> {
        Ok(self.state.lock().map_err(runtime_error)?.stmt_cache_size)
    }

    fn set_stmt_cache_size(&self, value: u32) -> PyResult<()> {
        self.state.lock().map_err(runtime_error)?.stmt_cache_size = value;
        Ok(())
    }

    fn get_transaction_in_progress(&self) -> PyResult<bool> {
        Ok(self
            .state
            .lock()
            .map_err(runtime_error)?
            .transaction_in_progress)
    }

    fn set_action(&self, value: Option<String>) -> PyResult<()> {
        self.execute_statement_with_binds(
            "begin dbms_application_info.set_action(:1); end;",
            &[bind_optional_text(value.as_deref())],
        )
    }

    fn set_client_identifier(&self, value: Option<String>) -> PyResult<()> {
        if let Some(value) = value {
            self.execute_statement_with_binds(
                "begin dbms_session.set_identifier(:1); end;",
                &[BindValue::Text(value)],
            )
        } else {
            self.execute_statement("begin dbms_session.clear_identifier; end;")
        }
    }

    fn set_client_info(&self, value: Option<String>) -> PyResult<()> {
        self.execute_statement_with_binds(
            "begin dbms_application_info.set_client_info(:1); end;",
            &[bind_optional_text(value.as_deref())],
        )
    }

    fn set_dbop(&self, value: Option<String>) -> PyResult<()> {
        if let Some((name, execution_id)) = self
            .state
            .lock()
            .map_err(runtime_error)?
            .dbop_operation
            .take()
        {
            self.execute_statement_with_binds(
                "begin dbms_sql_monitor.end_operation(:1, :2); end;",
                &[
                    BindValue::Text(name),
                    BindValue::Number(execution_id.to_string()),
                ],
            )?;
        }
        let Some(value) = value else {
            return Ok(());
        };
        let row = self
            .query_first_row_with_binds(
                "select dbms_sql_monitor.begin_operation(:1, null, 'Y') from dual",
                &[BindValue::Text(value.clone())],
            )?
            .ok_or_else(|| {
                PyRuntimeError::new_err("dbms_sql_monitor.begin_operation returned no row")
            })?;
        let execution_id = query_value_to_i64(row.first().unwrap_or(&None))?;
        self.state.lock().map_err(runtime_error)?.dbop_operation = Some((value, execution_id));
        Ok(())
    }

    fn set_module(&self, value: Option<String>) -> PyResult<()> {
        self.execute_statement_with_binds(
            "begin dbms_application_info.set_module(:1, null); end;",
            &[bind_optional_text(value.as_deref())],
        )
    }

    fn get_session_id(&self) -> PyResult<u32> {
        let guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        Ok(connection.session_id())
    }

    fn get_serial_num(&self) -> PyResult<u16> {
        let guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        Ok(connection.serial_num())
    }

    fn create_temp_lob_value(
        &self,
        lob_type: &Bound<'_, PyAny>,
        async_mode: bool,
    ) -> PyResult<ThinLob> {
        let (ora_type_num, csfrm) = match py_type_name(lob_type).as_str() {
            "DB_TYPE_BLOB" => (ORA_TYPE_NUM_BLOB, 0),
            "DB_TYPE_NCLOB" => (ORA_TYPE_NUM_CLOB, CS_FORM_NCHAR),
            _ => (ORA_TYPE_NUM_CLOB, CS_FORM_IMPLICIT),
        };
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        let result = BlockingConnection::create_temp_lob(connection, ora_type_num, csfrm)
            .map_err(runtime_error)?;
        Ok(ThinLob {
            data: None,
            locator: Arc::new(Mutex::new(Some(result.locator))),
            ora_type_num,
            csfrm,
            size: 0,
            chunk_size: 0,
            context: Some(ThinLobContext {
                connection: Arc::clone(&self.connection),
                state: Arc::clone(&self.state),
                async_mode,
            }),
            is_open: Arc::new(Mutex::new(false)),
            bfile_name: None,
        })
    }

    fn create_temp_lob_impl(
        &self,
        py: Python<'_>,
        lob_type: &Bound<'_, PyAny>,
    ) -> PyResult<Py<ThinLob>> {
        Py::new(py, self.create_temp_lob_value(lob_type, false)?)
    }

    fn create_cursor_impl(&self, scrollable: bool) -> ThinCursorImpl {
        ThinCursorImpl::new(
            Arc::clone(&self.connection),
            Arc::clone(&self.autocommit_state),
            Arc::clone(&self.cancel_requested),
            Arc::clone(&self.state),
            scrollable,
        )
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "ThinDbObjectTypeImpl")]
#[derive(Clone, Debug)]
struct DbObjectTypeImpl {
    schema: String,
    package_name: Option<String>,
    name: String,
    oid: Option<Vec<u8>>,
    version: u32,
    is_collection: bool,
    attrs: Vec<DbObjectAttrImpl>,
    element_metadata: Option<Box<DbObjectAttrImpl>>,
    max_num_elements: u32,
    is_assoc_array: bool,
}

impl DbObjectTypeImpl {
    fn new(
        schema: String,
        package_name: Option<String>,
        name: String,
        typecode: &str,
        attrs: Vec<DbObjectAttrImpl>,
        element_metadata: Option<DbObjectAttrImpl>,
        max_num_elements: u32,
        is_assoc_array: bool,
    ) -> Self {
        Self {
            schema,
            package_name,
            name,
            oid: None,
            version: 0,
            is_collection: typecode.eq_ignore_ascii_case("COLLECTION"),
            attrs,
            element_metadata: element_metadata.map(Box::new),
            max_num_elements,
            is_assoc_array,
        }
    }

    fn from_column_metadata(metadata: &ColumnMetadata) -> Option<Self> {
        let name = metadata.object_type_name.as_ref()?.to_ascii_uppercase();
        let schema = metadata
            .object_schema
            .as_deref()
            .unwrap_or_default()
            .to_ascii_uppercase();
        Some(Self::new(
            schema,
            None,
            name,
            "OBJECT",
            Vec::new(),
            None,
            0,
            false,
        ))
    }

    fn with_type_identity(mut self, oid: Option<Vec<u8>>, version: u32) -> Self {
        self.oid = oid;
        self.version = version;
        self
    }

    fn object_output_bind(&self) -> Option<BindValue> {
        let oid = self.oid.clone()?;
        Some(BindValue::ObjectOutput {
            schema: self.schema.clone(),
            type_name: self.name.clone(),
            oid,
            version: self.version.max(1),
            buffer_size: 1,
            is_return: false,
        })
    }

    fn default_scalar_return_attr(&self) -> Option<&str> {
        self.attrs
            .iter()
            .find(|attr| attr.name.eq_ignore_ascii_case("STRINGVALUE"))
            .or_else(|| {
                self.attrs.iter().find(|attr| {
                    matches!(
                        attr.dbtype_name.as_str(),
                        "DB_TYPE_VARCHAR" | "DB_TYPE_CHAR" | "DB_TYPE_NVARCHAR" | "DB_TYPE_NCHAR"
                    )
                })
            })
            .map(|attr| attr.name.as_str())
    }
}

impl PartialEq for DbObjectTypeImpl {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema
            && self.package_name == other.package_name
            && self.name == other.name
    }
}

impl Eq for DbObjectTypeImpl {}

#[pymethods]
impl DbObjectTypeImpl {
    #[getter]
    fn schema(&self) -> &str {
        &self.schema
    }

    #[getter]
    fn package_name(&self) -> Option<&str> {
        self.package_name.as_deref()
    }

    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn is_collection(&self) -> bool {
        self.is_collection
    }

    #[getter]
    fn attrs(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let attrs = self
            .attrs
            .iter()
            .cloned()
            .map(|attr| Py::new(py, attr))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyList::new(py, attrs)?.unbind().into())
    }

    #[getter]
    fn attrs_by_name(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        for attr in &self.attrs {
            dict.set_item(&attr.name, Py::new(py, attr.clone())?)?;
        }
        Ok(dict.unbind().into())
    }

    #[getter]
    fn element_metadata(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.element_metadata
            .as_deref()
            .cloned()
            .map(|metadata| Py::new(py, metadata).map(Py::into_any))
            .unwrap_or_else(|| Ok(py.None()))
    }

    fn _get_fqn(&self) -> String {
        if let Some(package_name) = &self.package_name {
            format!("{}.{}.{}", self.schema, package_name, self.name)
        } else {
            format!("{}.{}", self.schema, self.name)
        }
    }

    fn create_new_object(&self, py: Python<'_>) -> PyResult<DbObjectImpl> {
        DbObjectImpl::new(py, self.clone())
    }

    fn __eq__(&self, other: &Self) -> bool {
        self == other
    }

    fn __ne__(&self, other: &Self) -> bool {
        self != other
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "ThinDbObjectAttrImpl")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct DbObjectAttrImpl {
    name: String,
    dbtype_name: String,
    objtype: Option<DbObjectTypeImpl>,
    max_size: u32,
    precision: i8,
    scale: i8,
}

#[pymethods]
impl DbObjectAttrImpl {
    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn dbtype(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(PyModule::import(py, "oracledb")?
            .getattr(&self.dbtype_name)?
            .unbind())
    }

    #[getter]
    fn objtype(&self) -> Option<DbObjectTypeImpl> {
        self.objtype.clone()
    }

    #[getter]
    fn max_size(&self) -> u32 {
        self.max_size
    }

    #[getter]
    fn precision(&self) -> i8 {
        self.precision
    }

    #[getter]
    fn scale(&self) -> i8 {
        self.scale
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "ThinDbObjectImpl")]
struct DbObjectImpl {
    object_type: DbObjectTypeImpl,
    attr_values: Arc<Mutex<BTreeMap<String, Py<PyAny>>>>,
    collection_values: Arc<Mutex<Vec<Py<PyAny>>>>,
    assoc_values: Arc<Mutex<BTreeMap<i32, Py<PyAny>>>>,
    packed_data: Arc<Mutex<Option<Vec<u8>>>>,
    lob_context: Option<ThinLobContext>,
}

struct DbObjectPickleReader<'a> {
    inner: DbObjectPackedReader<'a>,
}

impl<'a> DbObjectPickleReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            inner: DbObjectPackedReader::new(bytes),
        }
    }

    fn read_u8(&mut self) -> PyResult<u8> {
        self.inner.read_u8().map_err(runtime_error)
    }

    fn read_i32be(&mut self) -> PyResult<i32> {
        self.inner.read_i32be().map_err(runtime_error)
    }

    fn read_length(&mut self) -> PyResult<usize> {
        self.inner.read_length().map_err(runtime_error)
    }

    fn read_value_bytes(&mut self) -> PyResult<Option<Vec<u8>>> {
        self.inner.read_value_bytes().map_err(runtime_error)
    }

    fn read_header(&mut self) -> PyResult<()> {
        self.inner.read_header().map_err(runtime_error)
    }

    fn read_atomic_null(&mut self, is_collection_context: bool) -> PyResult<bool> {
        self.inner
            .read_atomic_null(is_collection_context)
            .map_err(runtime_error)
    }
}

fn validated_dbobject_value(
    py: Python<'_>,
    metadata: &DbObjectAttrImpl,
    value: Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    let bound = value.bind(py);
    if bound.is_none() {
        return Ok(py.None());
    }
    match metadata.dbtype_name.as_str() {
        "DB_TYPE_OBJECT" => {
            if let Some(expected_type) = &metadata.objtype {
                let Some(actual_object) = py_db_object_impl(bound)? else {
                    return Err(raise_unsupported_python_type_for_db_type(
                        bound,
                        &metadata.dbtype_name,
                    ));
                };
                let actual_type = actual_object.object_type.clone();
                if &actual_type != expected_type {
                    return Err(raise_wrong_object_type(&actual_type, expected_type));
                }
            }
        }
        "DB_TYPE_NUMBER" => {
            if bound.cast::<PyString>().is_ok() || bound.cast::<PyBytes>().is_ok() {
                return Err(raise_unsupported_python_type_for_db_type(
                    bound,
                    &metadata.dbtype_name,
                ));
            }
        }
        _ => {}
    }
    Ok(value)
}

fn dbobject_value_byte_size(py: Python<'_>, value: &Py<PyAny>) -> PyResult<Option<usize>> {
    let bound = value.bind(py);
    if bound.is_none() {
        return Ok(None);
    }
    if let Ok(text) = bound.extract::<String>() {
        return Ok(Some(text.len()));
    }
    if let Ok(bytes) = bound.cast::<PyBytes>() {
        return Ok(Some(bytes.as_bytes().len()));
    }
    Ok(None)
}

impl DbObjectImpl {
    fn new(py: Python<'_>, object_type: DbObjectTypeImpl) -> PyResult<Self> {
        let mut attr_values = BTreeMap::new();
        for attr in &object_type.attrs {
            attr_values.insert(attr.name.clone(), py.None());
        }
        Ok(Self {
            object_type,
            attr_values: Arc::new(Mutex::new(attr_values)),
            collection_values: Arc::new(Mutex::new(Vec::new())),
            assoc_values: Arc::new(Mutex::new(BTreeMap::new())),
            packed_data: Arc::new(Mutex::new(None)),
            lob_context: None,
        })
    }

    fn with_packed_data(
        object_type: DbObjectTypeImpl,
        packed_data: Vec<u8>,
        lob_context: Option<ThinLobContext>,
    ) -> Self {
        Self {
            object_type,
            attr_values: Arc::new(Mutex::new(BTreeMap::new())),
            collection_values: Arc::new(Mutex::new(Vec::new())),
            assoc_values: Arc::new(Mutex::new(BTreeMap::new())),
            packed_data: Arc::new(Mutex::new(Some(packed_data))),
            lob_context,
        }
    }

    fn with_attr(
        py: Python<'_>,
        object_type: DbObjectTypeImpl,
        attr_name: &str,
        value: String,
    ) -> PyResult<Self> {
        let object = Self::new(py, object_type)?;
        object.set_attr_by_name(py, attr_name, value.into_pyobject(py)?.unbind().into())?;
        Ok(object)
    }

    fn set_attr_by_name(&self, py: Python<'_>, attr_name: &str, value: Py<PyAny>) -> PyResult<()> {
        let key = attr_name.to_ascii_uppercase();
        let value = if value.bind(py).is_none() {
            py.None()
        } else {
            value
        };
        self.attr_values
            .lock()
            .map_err(runtime_error)?
            .insert(key, value);
        Ok(())
    }

    fn attr_value(&self, py: Python<'_>, attr_name: &str) -> PyResult<Py<PyAny>> {
        self.ensure_unpacked(py)?;
        Ok(self
            .attr_values
            .lock()
            .map_err(runtime_error)?
            .get(&attr_name.to_ascii_uppercase())
            .map(|value| value.clone_ref(py))
            .unwrap_or_else(|| py.None()))
    }

    fn attr_bind_value(&self, py: Python<'_>, attr_name: &str) -> PyResult<Py<PyAny>> {
        self.attr_value(py, attr_name)
    }

    fn next_collection_append_index(&self) -> PyResult<i32> {
        if self.object_type.is_assoc_array {
            let values = self.assoc_values.lock().map_err(runtime_error)?;
            Ok(values
                .keys()
                .next_back()
                .copied()
                .map(|index| index.saturating_add(1))
                .unwrap_or(0))
        } else {
            Ok(
                i32::try_from(self.collection_values.lock().map_err(runtime_error)?.len())
                    .unwrap_or(i32::MAX),
            )
        }
    }

    fn append_collection_value(&self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        self.ensure_unpacked(py)?;
        let value = if value.bind(py).is_none() {
            py.None()
        } else {
            value
        };
        if self.object_type.is_assoc_array {
            let mut values = self.assoc_values.lock().map_err(runtime_error)?;
            let index = values
                .keys()
                .next_back()
                .copied()
                .map(|index| index.saturating_add(1))
                .unwrap_or(0);
            values.insert(index, value);
            return Ok(());
        }
        let mut values = self.collection_values.lock().map_err(runtime_error)?;
        if self.object_type.max_num_elements > 0
            && values.len() >= self.object_type.max_num_elements as usize
        {
            return Err(raise_invalid_coll_index_set(
                i32::try_from(values.len()).unwrap_or(i32::MAX),
                0,
                i32::try_from(self.object_type.max_num_elements.saturating_sub(1))
                    .unwrap_or(i32::MAX),
            ));
        }
        values.push(value);
        Ok(())
    }

    fn ensure_unpacked(&self, py: Python<'_>) -> PyResult<()> {
        let packed_data = self.packed_data.lock().map_err(runtime_error)?.clone();
        let Some(packed_data) = packed_data else {
            return Ok(());
        };
        let mut reader = DbObjectPickleReader::new(&packed_data);
        reader.read_header()?;
        self.unpack_from_reader(py, &mut reader)?;
        *self.packed_data.lock().map_err(runtime_error)? = None;
        Ok(())
    }

    fn unpack_from_reader(
        &self,
        py: Python<'_>,
        reader: &mut DbObjectPickleReader<'_>,
    ) -> PyResult<()> {
        if self.object_type.is_collection {
            let _collection_flags = reader.read_u8()?;
            let num_elements = reader.read_length()?;
            if self.object_type.is_assoc_array {
                let mut values = BTreeMap::new();
                let Some(metadata) = self.object_type.element_metadata.as_deref() else {
                    return Err(PyRuntimeError::new_err(
                        "missing collection element metadata",
                    ));
                };
                for _ in 0..num_elements {
                    let index = reader.read_i32be()?;
                    let value = dbobject_unpack_value(
                        py,
                        metadata,
                        reader,
                        true,
                        self.lob_context.as_ref(),
                    )?;
                    values.insert(index, value);
                }
                *self.assoc_values.lock().map_err(runtime_error)? = values;
            } else {
                let mut values = Vec::with_capacity(num_elements);
                let Some(metadata) = self.object_type.element_metadata.as_deref() else {
                    return Err(PyRuntimeError::new_err(
                        "missing collection element metadata",
                    ));
                };
                for _ in 0..num_elements {
                    values.push(dbobject_unpack_value(
                        py,
                        metadata,
                        reader,
                        true,
                        self.lob_context.as_ref(),
                    )?);
                }
                *self.collection_values.lock().map_err(runtime_error)? = values;
            }
            return Ok(());
        }

        let mut values = BTreeMap::new();
        for attr in &self.object_type.attrs {
            values.insert(
                attr.name.clone(),
                dbobject_unpack_value(py, attr, reader, false, self.lob_context.as_ref())?,
            );
        }
        *self.attr_values.lock().map_err(runtime_error)? = values;
        Ok(())
    }
}

fn decode_dbobject_text(bytes: &[u8], dbtype_name: &str) -> PyResult<String> {
    protocol_decode_dbobject_text(bytes, dbtype_name).map_err(runtime_error)
}

fn decode_dbobject_xmltype(py: Python<'_>, bytes: &[u8]) -> PyResult<Py<PyAny>> {
    match decode_dbobject_xmltype_text(bytes).map_err(runtime_error)? {
        Some(value) => Ok(value.into_pyobject(py)?.unbind().into()),
        None => Ok(py.None()),
    }
}

fn decode_dbobject_binary_float(bytes: &[u8]) -> PyResult<f32> {
    protocol_decode_dbobject_binary_float(bytes).map_err(runtime_error)
}

fn decode_dbobject_binary_double(bytes: &[u8]) -> PyResult<f64> {
    protocol_decode_dbobject_binary_double(bytes).map_err(runtime_error)
}

fn dbobject_unpack_value(
    py: Python<'_>,
    metadata: &DbObjectAttrImpl,
    reader: &mut DbObjectPickleReader<'_>,
    parent_is_collection: bool,
    lob_context: Option<&ThinLobContext>,
) -> PyResult<Py<PyAny>> {
    if metadata.dbtype_name == "DB_TYPE_OBJECT" {
        let Some(object_type) = metadata.objtype.clone() else {
            let _ = reader.read_value_bytes()?;
            return Ok(py.None());
        };
        let is_collection_context = parent_is_collection || object_type.is_collection;
        if reader.read_atomic_null(is_collection_context)? {
            return Ok(py.None());
        }
        let object = if is_collection_context {
            let Some(packed_data) = reader.read_value_bytes()? else {
                return Ok(py.None());
            };
            DbObjectImpl::with_packed_data(object_type, packed_data, lob_context.cloned())
        } else {
            let mut object = DbObjectImpl::new(py, object_type)?;
            object.lob_context = lob_context.cloned();
            object.unpack_from_reader(py, reader)?;
            object
        };
        return py_db_object_from_impl(py, object);
    }

    let Some(bytes) = reader.read_value_bytes()? else {
        return Ok(py.None());
    };
    match metadata.dbtype_name.as_str() {
        "DB_TYPE_CHAR" | "DB_TYPE_NCHAR" | "DB_TYPE_VARCHAR" | "DB_TYPE_NVARCHAR" => {
            Ok(decode_dbobject_text(&bytes, &metadata.dbtype_name)?
                .into_pyobject(py)?
                .unbind()
                .into())
        }
        "DB_TYPE_RAW" => Ok(PyBytes::new(py, &bytes).unbind().into()),
        "DB_TYPE_XMLTYPE" => decode_dbobject_xmltype(py, &bytes),
        "DB_TYPE_NUMBER" => {
            let value = decode_number_value(&bytes).map_err(runtime_error)?;
            if metadata.scale == -127 && metadata.precision > 0 {
                if let QueryValue::Number { text, .. } = value {
                    let value = text.parse::<f64>().map_err(runtime_error)?;
                    return Ok(value.into_pyobject(py)?.unbind().into());
                }
            }
            query_value_to_py(py, &Some(value), None, None, true)
        }
        "DB_TYPE_DATE" | "DB_TYPE_TIMESTAMP" | "DB_TYPE_TIMESTAMP_TZ" | "DB_TYPE_TIMESTAMP_LTZ" => {
            let value = decode_datetime_value(&bytes).map_err(runtime_error)?;
            query_value_to_py(py, &Some(value), None, None, true)
        }
        "DB_TYPE_BINARY_FLOAT" => Ok(f64::from(decode_dbobject_binary_float(&bytes)?)
            .into_pyobject(py)?
            .unbind()
            .into()),
        "DB_TYPE_BINARY_DOUBLE" => Ok(decode_dbobject_binary_double(&bytes)?
            .into_pyobject(py)?
            .unbind()
            .into()),
        "DB_TYPE_CLOB" | "DB_TYPE_NCLOB" | "DB_TYPE_BLOB" => {
            let ora_type_num = if metadata.dbtype_name == "DB_TYPE_BLOB" {
                ORA_TYPE_NUM_BLOB
            } else {
                ORA_TYPE_NUM_CLOB
            };
            let csfrm = if metadata.dbtype_name == "DB_TYPE_NCLOB" {
                CS_FORM_NCHAR
            } else {
                CS_FORM_IMPLICIT
            };
            py_lob_from_impl(
                py,
                ThinLob {
                    data: None,
                    locator: Arc::new(Mutex::new(Some(bytes))),
                    ora_type_num,
                    csfrm,
                    size: 0,
                    chunk_size: 0,
                    context: lob_context.cloned(),
                    is_open: Arc::new(Mutex::new(false)),
                    bfile_name: None,
                },
            )
        }
        _ => Ok(py.None()),
    }
}

fn py_db_object_from_impl(py: Python<'_>, object: DbObjectImpl) -> PyResult<Py<PyAny>> {
    let impl_obj = Py::new(py, object)?;
    Ok(PyModule::import(py, "oracledb")?
        .getattr("DbObject")?
        .call_method1("_from_impl", (impl_obj,))?
        .unbind())
}

#[pymethods]
impl DbObjectImpl {
    #[getter]
    #[pyo3(name = "type")]
    fn object_type(&self) -> DbObjectTypeImpl {
        self.object_type.clone()
    }

    fn get_attr_value(&self, py: Python<'_>, attr: &DbObjectAttrImpl) -> PyResult<Py<PyAny>> {
        self.attr_value(py, &attr.name)
    }

    fn set_attr_value(
        &self,
        py: Python<'_>,
        attr: &DbObjectAttrImpl,
        value: Py<PyAny>,
    ) -> PyResult<()> {
        let value = validated_dbobject_value(py, attr, value)?;
        if attr.max_size > 0 {
            if let Some(actual_size) = dbobject_value_byte_size(py, &value)? {
                if actual_size > attr.max_size as usize {
                    return Err(raise_dbobject_attr_max_size(
                        &attr.name,
                        &self.object_type._get_fqn(),
                        actual_size,
                        attr.max_size,
                    ));
                }
            }
        }
        self.set_attr_by_name(py, &attr.name, value)
    }

    fn set_attr_value_checked(
        &self,
        py: Python<'_>,
        attr: &DbObjectAttrImpl,
        value: Py<PyAny>,
    ) -> PyResult<()> {
        self.set_attr_by_name(py, &attr.name, value)
    }

    fn copy(&self, py: Python<'_>) -> PyResult<Self> {
        self.ensure_unpacked(py)?;
        let mut attr_values = BTreeMap::new();
        for (name, value) in self.attr_values.lock().map_err(runtime_error)?.iter() {
            attr_values.insert(name.clone(), value.clone_ref(py));
        }
        let collection_values = self
            .collection_values
            .lock()
            .map_err(runtime_error)?
            .iter()
            .map(|value| value.clone_ref(py))
            .collect();
        Ok(Self {
            object_type: self.object_type.clone(),
            attr_values: Arc::new(Mutex::new(attr_values)),
            collection_values: Arc::new(Mutex::new(collection_values)),
            assoc_values: Arc::new(Mutex::new(
                self.assoc_values
                    .lock()
                    .map_err(runtime_error)?
                    .iter()
                    .map(|(index, value)| (*index, value.clone_ref(py)))
                    .collect(),
            )),
            packed_data: Arc::new(Mutex::new(None)),
            lob_context: self.lob_context.clone(),
        })
    }

    fn append(&self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let Some(metadata) = self.object_type.element_metadata.as_deref() else {
            return Err(raise_oracledb_driver_error(
                "ERR_OBJECT_IS_NOT_A_COLLECTION",
            ));
        };
        let value = validated_dbobject_value(py, metadata, value)?;
        if metadata.max_size > 0 {
            if let Some(actual_size) = dbobject_value_byte_size(py, &value)? {
                if actual_size > metadata.max_size as usize {
                    return Err(raise_dbobject_element_max_size(
                        self.next_collection_append_index()?,
                        &self.object_type._get_fqn(),
                        actual_size,
                        metadata.max_size,
                    ));
                }
            }
        }
        self.append_collection_value(py, value)
    }

    fn append_checked(&self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        self.append_collection_value(py, value)
    }

    fn delete_by_index(&self, py: Python<'_>, index: i32) -> PyResult<()> {
        self.ensure_unpacked(py)?;
        if self.object_type.is_assoc_array {
            let mut values = self.assoc_values.lock().map_err(runtime_error)?;
            if values.remove(&index).is_none() {
                return Err(raise_invalid_coll_index_get(index));
            }
            return Ok(());
        }
        let mut values = self.collection_values.lock().map_err(runtime_error)?;
        let Ok(index) = usize::try_from(index) else {
            return Err(raise_invalid_coll_index_get(index));
        };
        if index >= values.len() {
            return Err(raise_invalid_coll_index_get(
                i32::try_from(index).unwrap_or(i32::MAX),
            ));
        }
        values.remove(index);
        Ok(())
    }

    fn exists_by_index(&self, py: Python<'_>, index: i32) -> PyResult<bool> {
        self.ensure_unpacked(py)?;
        if self.object_type.is_assoc_array {
            return Ok(self
                .assoc_values
                .lock()
                .map_err(runtime_error)?
                .contains_key(&index));
        }
        let values = self.collection_values.lock().map_err(runtime_error)?;
        Ok(usize::try_from(index)
            .map(|index| index < values.len())
            .unwrap_or(false))
    }

    fn get_element_by_index(&self, py: Python<'_>, index: i32) -> PyResult<Py<PyAny>> {
        self.ensure_unpacked(py)?;
        if self.object_type.is_assoc_array {
            return self
                .assoc_values
                .lock()
                .map_err(runtime_error)?
                .get(&index)
                .map(|value| value.clone_ref(py))
                .ok_or_else(|| raise_invalid_coll_index_get(index));
        }
        let values = self.collection_values.lock().map_err(runtime_error)?;
        let Ok(index) = usize::try_from(index) else {
            return Err(raise_invalid_coll_index_get(index));
        };
        values
            .get(index)
            .map(|value| value.clone_ref(py))
            .ok_or_else(|| raise_invalid_coll_index_get(i32::try_from(index).unwrap_or(i32::MAX)))
    }

    fn get_first_index(&self, py: Python<'_>) -> PyResult<Option<i32>> {
        self.ensure_unpacked(py)?;
        if self.object_type.is_assoc_array {
            return Ok(self
                .assoc_values
                .lock()
                .map_err(runtime_error)?
                .keys()
                .next()
                .copied());
        }
        let values = self.collection_values.lock().map_err(runtime_error)?;
        Ok((!values.is_empty()).then_some(0))
    }

    fn get_last_index(&self, py: Python<'_>) -> PyResult<Option<i32>> {
        self.ensure_unpacked(py)?;
        if self.object_type.is_assoc_array {
            return Ok(self
                .assoc_values
                .lock()
                .map_err(runtime_error)?
                .keys()
                .next_back()
                .copied());
        }
        let values = self.collection_values.lock().map_err(runtime_error)?;
        Ok(values
            .len()
            .checked_sub(1)
            .map(|index| i32::try_from(index).unwrap_or(i32::MAX)))
    }

    fn get_next_index(&self, py: Python<'_>, index: i32) -> PyResult<Option<i32>> {
        self.ensure_unpacked(py)?;
        if self.object_type.is_assoc_array {
            return Ok(self
                .assoc_values
                .lock()
                .map_err(runtime_error)?
                .range((index.saturating_add(1))..)
                .next()
                .map(|(index, _)| *index));
        }
        let values = self.collection_values.lock().map_err(runtime_error)?;
        let next = index.saturating_add(1);
        Ok(usize::try_from(next)
            .ok()
            .filter(|next_index| *next_index < values.len())
            .map(|_| next))
    }

    fn get_prev_index(&self, py: Python<'_>, index: i32) -> PyResult<Option<i32>> {
        self.ensure_unpacked(py)?;
        if self.object_type.is_assoc_array {
            return Ok(self
                .assoc_values
                .lock()
                .map_err(runtime_error)?
                .range(..index)
                .next_back()
                .map(|(index, _)| *index));
        }
        Ok((index > 0).then_some(index - 1))
    }

    fn get_size(&self, py: Python<'_>) -> PyResult<usize> {
        self.ensure_unpacked(py)?;
        if self.object_type.is_assoc_array {
            return Ok(self.assoc_values.lock().map_err(runtime_error)?.len());
        }
        Ok(self.collection_values.lock().map_err(runtime_error)?.len())
    }

    fn set_element_by_index(&self, py: Python<'_>, index: i32, value: Py<PyAny>) -> PyResult<()> {
        self.ensure_unpacked(py)?;
        let Some(metadata) = self.object_type.element_metadata.as_deref() else {
            return Err(raise_oracledb_driver_error(
                "ERR_OBJECT_IS_NOT_A_COLLECTION",
            ));
        };
        let value = validated_dbobject_value(py, metadata, value)?;
        if metadata.max_size > 0 {
            if let Some(actual_size) = dbobject_value_byte_size(py, &value)? {
                if actual_size > metadata.max_size as usize {
                    return Err(raise_dbobject_element_max_size(
                        index,
                        &self.object_type._get_fqn(),
                        actual_size,
                        metadata.max_size,
                    ));
                }
            }
        }
        self.set_element_by_index_checked(py, index, value)
    }

    fn set_element_by_index_checked(
        &self,
        py: Python<'_>,
        index: i32,
        value: Py<PyAny>,
    ) -> PyResult<()> {
        self.ensure_unpacked(py)?;
        if self.object_type.is_assoc_array {
            self.assoc_values
                .lock()
                .map_err(runtime_error)?
                .insert(index, value);
            return Ok(());
        }
        let mut values = self.collection_values.lock().map_err(runtime_error)?;
        let max_index = values
            .len()
            .checked_sub(1)
            .map(|index| i32::try_from(index).unwrap_or(i32::MAX))
            .unwrap_or(0);
        let Ok(index_usize) = usize::try_from(index) else {
            return Err(raise_invalid_coll_index_set(index, 0, max_index));
        };
        let Some(slot) = values.get_mut(index_usize) else {
            return Err(raise_invalid_coll_index_set(index, 0, max_index));
        };
        *slot = value;
        Ok(())
    }

    fn trim(&self, py: Python<'_>, num_to_trim: i32) -> PyResult<()> {
        self.ensure_unpacked(py)?;
        if num_to_trim <= 0 {
            return Ok(());
        }
        let mut values = self.collection_values.lock().map_err(runtime_error)?;
        let new_len = values.len().saturating_sub(num_to_trim as usize);
        values.truncate(new_len);
        Ok(())
    }
}

#[pyclass(
    module = "oracledb.thin_impl",
    name = "FetchMetadataImpl",
    skip_from_py_object
)]
#[derive(Clone)]
struct FetchMetadataImpl {
    metadata: ColumnMetadata,
}

#[pymethods]
impl FetchMetadataImpl {
    #[getter]
    fn name(&self) -> &str {
        &self.metadata.name
    }

    #[getter]
    fn dbtype(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let module = PyModule::import(py, "oracledb")?;
        if column_metadata_is_xmltype(&self.metadata) {
            return Ok(module.getattr("DB_TYPE_XMLTYPE")?.unbind());
        }
        let name = public_dbtype_name_from_column_metadata(&self.metadata);
        Ok(module.getattr(name)?.unbind())
    }

    #[getter]
    fn type_code(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.dbtype(py)
    }

    #[getter]
    fn max_size(&self) -> u32 {
        self.metadata.max_size
    }

    #[getter]
    fn buffer_size(&self) -> u32 {
        self.metadata.buffer_size
    }

    #[getter]
    fn precision(&self) -> i8 {
        self.metadata.precision
    }

    #[getter]
    fn scale(&self) -> i8 {
        self.metadata.scale
    }

    #[getter]
    fn nulls_allowed(&self) -> bool {
        self.metadata.nulls_allowed
    }

    #[getter]
    fn is_json(&self) -> bool {
        self.metadata.is_json
    }

    #[getter]
    fn is_oson(&self) -> bool {
        self.metadata.is_oson
    }

    #[getter]
    fn objtype(&self) -> Option<DbObjectTypeImpl> {
        if self.metadata.ora_type_num == ORA_TYPE_NUM_OBJECT {
            return DbObjectTypeImpl::from_column_metadata(&self.metadata);
        }
        None
    }

    #[getter]
    fn annotations(&self) -> Option<Py<PyAny>> {
        None
    }

    #[getter]
    fn domain_name(&self) -> Option<&str> {
        None
    }

    #[getter]
    fn domain_schema(&self) -> Option<&str> {
        None
    }

    #[getter]
    fn vector_dimensions(&self) -> Option<u32> {
        None
    }

    #[getter]
    fn vector_format(&self) -> Option<u8> {
        None
    }

    #[getter]
    fn vector_flags(&self) -> u8 {
        0
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "ExecutemanyManager")]
struct ExecutemanyManager {
    total_rows: u32,
    batch_size: u32,
    num_rows: u32,
    message_offset: u32,
}

impl ExecutemanyManager {
    fn new(total_rows: usize, batch_size: u32) -> PyResult<Self> {
        let total_rows = u32::try_from(total_rows).map_err(runtime_error)?;
        let batch_size = batch_size.max(1);
        Ok(Self {
            total_rows,
            batch_size,
            num_rows: total_rows.min(batch_size),
            message_offset: 0,
        })
    }
}

#[pymethods]
impl ExecutemanyManager {
    #[getter]
    fn num_rows(&self) -> u32 {
        self.num_rows
    }

    #[getter]
    fn message_offset(&self) -> u32 {
        self.message_offset
    }

    fn next_batch(&mut self) {
        self.message_offset = self.message_offset.saturating_add(self.num_rows);
        let remaining = self.total_rows.saturating_sub(self.message_offset);
        self.num_rows = remaining.min(self.batch_size);
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "ThinCursorImpl")]
struct ThinCursorImpl {
    connection: Arc<Mutex<Option<RustConnection>>>,
    autocommit: Arc<Mutex<bool>>,
    cancel_requested: Arc<AtomicBool>,
    state: Arc<Mutex<ThinConnState>>,
    statement: Option<String>,
    bind_values: Vec<BindValue>,
    bind_vars: Vec<Py<ThinVar>>,
    bind_names: Vec<String>,
    many_bind_rows: Vec<Vec<BindValue>>,
    columns: Vec<ColumnMetadata>,
    fetch_vars: Vec<Option<Py<ThinVar>>>,
    fetch_define_columns: Vec<ColumnMetadata>,
    requires_define: bool,
    rows: Vec<Vec<Option<QueryValue>>>,
    row_index: usize,
    cursor_id: u32,
    more_rows: bool,
    invalid_ref_cursor: bool,
    rowcount: i64,
    arraysize: u32,
    prefetchrows: u32,
    scrollable: bool,
    fetch_lobs: bool,
    fetch_lobs_overridden: bool,
    fetch_async_lobs: bool,
    fetch_decimals: bool,
    suspend_on_success: bool,
    rowfactory: Option<Py<PyAny>>,
    inputtypehandler: Option<Py<PyAny>>,
    outputtypehandler: Option<Py<PyAny>>,
    warning: Option<Py<PyAny>>,
    has_positional_input_sizes: bool,
    has_named_input_sizes: bool,
    named_input_sizes: Vec<(String, Py<PyAny>)>,
    statement_changed: bool,
    is_query: bool,
}

impl ThinCursorImpl {
    fn drain_cancel_response(&self) -> PyResult<()> {
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::drain_cancel_response(connection).map_err(runtime_error)
    }

    fn new(
        connection: Arc<Mutex<Option<RustConnection>>>,
        autocommit: Arc<Mutex<bool>>,
        cancel_requested: Arc<AtomicBool>,
        state: Arc<Mutex<ThinConnState>>,
        scrollable: bool,
    ) -> Self {
        Self {
            connection,
            autocommit,
            cancel_requested,
            state,
            statement: None,
            bind_values: Vec::new(),
            bind_vars: Vec::new(),
            bind_names: Vec::new(),
            many_bind_rows: Vec::new(),
            columns: Vec::new(),
            fetch_vars: Vec::new(),
            fetch_define_columns: Vec::new(),
            requires_define: false,
            rows: Vec::new(),
            row_index: 0,
            cursor_id: 0,
            more_rows: false,
            invalid_ref_cursor: false,
            rowcount: 0,
            arraysize: 100,
            prefetchrows: 2,
            scrollable,
            fetch_lobs: true,
            fetch_lobs_overridden: false,
            fetch_async_lobs: false,
            fetch_decimals: false,
            suspend_on_success: false,
            rowfactory: None,
            inputtypehandler: None,
            outputtypehandler: None,
            warning: None,
            has_positional_input_sizes: false,
            has_named_input_sizes: false,
            named_input_sizes: Vec::new(),
            statement_changed: false,
            is_query: false,
        }
    }

    fn reset_fetch_define_state(&mut self) {
        self.fetch_vars.clear();
        self.fetch_define_columns.clear();
        self.requires_define = false;
    }

    fn active_output_type_handler(
        &self,
        py: Python<'_>,
        cursor: &Bound<'_, PyAny>,
    ) -> PyResult<Option<Py<PyAny>>> {
        if let Some(handler) = &self.outputtypehandler {
            return Ok(Some(handler.clone_ref(py)));
        }
        let connection = cursor.getattr("connection")?;
        let conn_impl = connection.getattr("_impl")?;
        if let Ok(conn_impl) = conn_impl.extract::<PyRef<'_, ThinConnImpl>>() {
            return Ok(conn_impl
                .outputtypehandler
                .as_ref()
                .map(|handler| handler.clone_ref(py)));
        }
        let conn_impl = conn_impl.extract::<PyRef<'_, AsyncThinConnImpl>>()?;
        Ok(conn_impl
            .inner
            .outputtypehandler
            .as_ref()
            .map(|handler| handler.clone_ref(py)))
    }

    fn prepare_fetch_defines(&mut self, py: Python<'_>, cursor: &Bound<'_, PyAny>) -> PyResult<()> {
        if !self.fetch_define_columns.is_empty() || self.columns.is_empty() {
            return Ok(());
        }
        self.fetch_vars = std::iter::repeat_with(|| None)
            .take(self.columns.len())
            .collect();
        self.fetch_define_columns = self.columns.clone();
        let Some(handler) = self.active_output_type_handler(py, cursor)? else {
            return Ok(());
        };
        let handler = handler.bind(py);
        let handler_cursor = Py::new(
            py,
            FetchHandlerCursor {
                connection: cursor.getattr("connection")?.unbind(),
                arraysize: self.arraysize,
            },
        )?;
        let handler_cursor = handler_cursor.bind(py);
        for (index, metadata) in self.columns.iter().enumerate() {
            let pub_metadata = Py::new(
                py,
                FetchMetadataImpl {
                    metadata: metadata.clone(),
                },
            )?;
            let value = handler.call1((handler_cursor, pub_metadata.bind(py)))?;
            if value.is_none() {
                continue;
            }
            let Some(var) = thin_var_from_value(&value)? else {
                return Err(raise_oracledb_driver_error("ERR_EXPECTING_VAR"));
            };
            let default_bind = var.borrow(py).default_bind.clone();
            let define_metadata = fetch_define_metadata_from_var(metadata, &default_bind);
            if !define_metadata.eq(metadata) {
                self.requires_define = true;
            }
            self.fetch_define_columns[index] = define_metadata;
            self.fetch_vars[index] = Some(var);
        }
        Ok(())
    }
}

#[pymethods]
impl ThinCursorImpl {
    #[getter]
    fn arraysize(&self) -> u32 {
        self.arraysize
    }

    #[setter]
    fn set_arraysize(&mut self, value: u32) {
        self.arraysize = value;
    }

    #[getter]
    fn prefetchrows(&self) -> u32 {
        self.prefetchrows
    }

    #[setter]
    fn set_prefetchrows(&mut self, value: u32) {
        self.prefetchrows = value;
    }

    #[getter]
    fn scrollable(&self) -> bool {
        self.scrollable
    }

    #[setter]
    fn set_scrollable(&mut self, value: bool) {
        self.scrollable = value;
    }

    #[getter]
    fn rowcount(&self) -> i64 {
        self.rowcount
    }

    #[getter]
    fn statement(&self) -> Option<&str> {
        self.statement.as_deref()
    }

    #[getter]
    #[pyo3(name = "fetch_vars")]
    fn fetch_vars_attr(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.is_query {
            let values = self
                .fetch_vars
                .iter()
                .map(|value| {
                    value
                        .as_ref()
                        .map(|var| var.clone_ref(py).into_any())
                        .unwrap_or_else(|| py.None())
                })
                .collect::<Vec<_>>();
            Ok(PyList::new(py, values)?.unbind().into())
        } else {
            Ok(py.None())
        }
    }

    #[getter]
    fn fetch_metadata(&self) -> Vec<FetchMetadataImpl> {
        self.columns
            .iter()
            .cloned()
            .map(|metadata| FetchMetadataImpl { metadata })
            .collect()
    }

    #[getter]
    fn fetch_lobs(&self) -> bool {
        self.fetch_lobs
    }

    #[setter]
    fn set_fetch_lobs(&mut self, value: bool) {
        self.fetch_lobs = value;
        self.fetch_lobs_overridden = true;
    }

    #[getter]
    fn fetch_decimals(&self) -> bool {
        self.fetch_decimals
    }

    #[setter]
    fn set_fetch_decimals(&mut self, value: bool) {
        self.fetch_decimals = value;
    }

    #[getter]
    fn suspend_on_success(&self) -> bool {
        self.suspend_on_success
    }

    #[setter]
    fn set_suspend_on_success(&mut self, value: bool) {
        self.suspend_on_success = value;
    }

    #[getter]
    fn rowfactory(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.rowfactory.as_ref().map(|value| value.clone_ref(py))
    }

    #[setter]
    fn set_rowfactory(&mut self, value: Option<Py<PyAny>>) {
        self.rowfactory = value;
    }

    #[getter]
    fn inputtypehandler(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.inputtypehandler
            .as_ref()
            .map(|value| value.clone_ref(py))
    }

    #[setter]
    fn set_inputtypehandler(&mut self, value: Option<Py<PyAny>>) {
        self.inputtypehandler = value;
    }

    #[getter]
    fn outputtypehandler(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.outputtypehandler
            .as_ref()
            .map(|value| value.clone_ref(py))
    }

    #[setter]
    fn set_outputtypehandler(&mut self, value: Option<Py<PyAny>>) {
        self.outputtypehandler = value;
    }

    #[getter]
    fn warning(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.warning.as_ref().map(|value| value.clone_ref(py))
    }

    #[pyo3(signature = (in_del=None))]
    fn close(&mut self, in_del: Option<bool>) {
        let _ = in_del;
        self.statement = None;
        self.bind_values.clear();
        self.bind_vars.clear();
        self.bind_names.clear();
        self.named_input_sizes.clear();
        self.many_bind_rows.clear();
        self.columns.clear();
        self.reset_fetch_define_state();
        self.rows.clear();
        self.row_index = 0;
        self.cursor_id = 0;
        self.more_rows = false;
        self.invalid_ref_cursor = false;
        self.is_query = false;
    }

    fn prepare(
        &mut self,
        statement: Option<String>,
        _tag: Option<String>,
        _cache_statement: Option<bool>,
    ) -> PyResult<()> {
        self.statement_changed = self.statement != statement;
        self.statement = statement;
        self.bind_names = if let Some(statement) = self.statement.as_deref() {
            validate_dml_returning_duplicate_binds(statement)?;
            unique_sql_bind_names(statement)?
        } else {
            Vec::new()
        };
        Ok(())
    }

    fn parse(&mut self, _cursor: &Bound<'_, PyAny>) -> PyResult<()> {
        let statement = self
            .statement
            .as_deref()
            .ok_or_else(|| raise_oracledb_driver_error("ERR_NO_STATEMENT"))?;
        validate_dml_returning_duplicate_binds(&statement)?;
        self.bind_names = unique_sql_bind_names(statement)?;
        validate_parse_bind_names(statement)?;
        Ok(())
    }

    fn _prepare_for_execute(
        &mut self,
        _cursor: &Bound<'_, PyAny>,
        statement: Option<String>,
        parameters: Option<&Bound<'_, PyAny>>,
        keyword_parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        if let Some(statement) = statement {
            self.statement_changed = self.statement.as_ref() != Some(&statement);
            self.statement = Some(statement);
        } else {
            self.statement_changed = false;
        }
        self.warning = None;
        let statement = self
            .statement
            .as_deref()
            .ok_or_else(|| raise_oracledb_driver_error("ERR_NO_STATEMENT"))?;
        let statement = statement.to_string();
        validate_dml_returning_duplicate_binds(&statement)?;
        if self.has_positional_input_sizes
            && parameters.is_some_and(|value| value.cast::<PyDict>().is_ok())
        {
            return Err(raise_oracledb_driver_error(
                "ERR_MIXED_POSITIONAL_AND_NAMED_BINDS",
            ));
        }
        if self.has_named_input_sizes
            && parameters.is_some_and(|value| {
                !value.is_none() && value.len().unwrap_or(0) > 0 && value.cast::<PyDict>().is_err()
            })
        {
            return Err(raise_oracledb_driver_error(
                "ERR_MIXED_POSITIONAL_AND_NAMED_BINDS",
            ));
        }
        validate_cursor_bind_parameters(_cursor, &self.connection, parameters, keyword_parameters)?;
        let (effective_statement, bind_values, bind_vars) = Python::attach(|py| {
            let previous_bind_names = self.bind_names.clone();
            let previous_bind_vars = self
                .bind_vars
                .iter()
                .map(|var| var.clone_ref(py))
                .collect::<Vec<_>>();
            let (effective_statement, effective_parameters, effective_keyword_parameters) =
                prepare_object_execute_inputs(py, &statement, parameters, keyword_parameters)?;
            let effective_parameters = effective_parameters.as_ref().map(|value| value.bind(py));
            let effective_keyword_parameters = effective_keyword_parameters
                .as_ref()
                .map(|value| value.bind(py));
            let bind_values = extract_bind_values(
                py,
                &effective_statement,
                effective_parameters,
                effective_keyword_parameters,
                &self.named_input_sizes,
                self.has_positional_input_sizes,
                &previous_bind_names,
                &previous_bind_vars,
            )?;
            let bind_vars = extract_bind_var_objects(
                py,
                &effective_statement,
                effective_parameters,
                effective_keyword_parameters,
                &self.named_input_sizes,
                &previous_bind_names,
                &previous_bind_vars,
            )?;
            Ok::<_, PyErr>((effective_statement, bind_values, bind_vars))
        })?;
        self.bind_names = unique_sql_bind_names(&effective_statement)?;
        self.bind_values = bind_values;
        self.bind_vars = bind_vars;
        self.statement = Some(effective_statement);
        self.many_bind_rows.clear();
        Ok(())
    }

    fn _prepare_for_executemany(
        &mut self,
        _cursor: &Bound<'_, PyAny>,
        statement: Option<String>,
        parameters: &Bound<'_, PyAny>,
        batch_size: u32,
    ) -> PyResult<ExecutemanyManager> {
        if let Some(statement) = statement {
            self.statement_changed = self.statement.as_ref() != Some(&statement);
            self.statement = Some(statement);
        } else {
            self.statement_changed = false;
        }
        self.warning = None;
        if self.statement.is_none() {
            return Err(PyRuntimeError::new_err("no statement prepared"));
        }
        self.bind_values.clear();
        self.bind_vars.clear();
        let statement = self
            .statement
            .as_deref()
            .ok_or_else(|| raise_oracledb_driver_error("ERR_NO_STATEMENT"))?;
        validate_dml_returning_duplicate_binds(statement)?;
        self.bind_names = unique_sql_bind_names(statement)?;
        self.bind_vars = extract_executemany_bind_var_objects(
            parameters.py(),
            statement,
            &self.named_input_sizes,
        )?;
        self.many_bind_rows = extract_bind_rows(
            parameters.py(),
            statement,
            parameters,
            &self.named_input_sizes,
        )?;
        ExecutemanyManager::new(self.many_bind_rows.len(), batch_size)
    }

    fn executemany(
        &mut self,
        cursor: &Bound<'_, PyAny>,
        num_execs: u32,
        batcherrors: bool,
        arraydmlrowcounts: bool,
        offset: u32,
    ) -> PyResult<()> {
        if batcherrors {
            return Err(not_implemented("ThinCursorImpl executemany batcherrors"));
        }
        if arraydmlrowcounts {
            return Err(not_implemented(
                "ThinCursorImpl executemany array DML rowcounts",
            ));
        }
        let statement = self
            .statement
            .as_deref()
            .ok_or_else(|| PyRuntimeError::new_err("no statement prepared"))?;
        let start = usize::try_from(offset).map_err(runtime_error)?;
        let count = usize::try_from(num_execs).map_err(runtime_error)?;
        let end = start
            .checked_add(count)
            .ok_or_else(|| PyRuntimeError::new_err("executemany offset overflow"))?;
        let bind_rows = self
            .many_bind_rows
            .get(start..end)
            .ok_or_else(|| PyRuntimeError::new_err("executemany batch is out of range"))?
            .to_vec();
        let typed_lob_hints = typed_lob_bind_hints(cursor.py(), &self.bind_vars);
        let call_timeout = {
            let value = self.state.lock().map_err(runtime_error)?.call_timeout;
            (value > 0).then_some(value)
        };
        let mut result = match cursor.py().detach({
            let connection = Arc::clone(&self.connection);
            let state = Arc::clone(&self.state);
            let statement = statement.to_string();
            let mut bind_rows = bind_rows.clone();
            let typed_lob_hints = typed_lob_hints.clone();
            let prefetchrows = self.prefetchrows;
            move || -> Result<QueryResult, String> {
                let mut guard = connection.lock().map_err(|err| err.to_string())?;
                let connection = guard
                    .as_mut()
                    .ok_or_else(|| "connection is closed".to_string())?;
                apply_pending_current_schema_from_state(&state, connection, call_timeout)
                    .map_err(|err| err.to_string())?;
                materialize_typed_lob_bind_rows(
                    connection,
                    &mut bind_rows,
                    &typed_lob_hints,
                    call_timeout,
                )?;
                BlockingConnection::execute_query_with_bind_rows_and_timeout(
                    connection,
                    &statement,
                    prefetchrows,
                    &bind_rows,
                    call_timeout,
                )
                .map_err(|err| err.to_string())
            }
        }) {
            Ok(result) => result,
            Err(_) if self.cancel_requested.swap(false, Ordering::SeqCst) => {
                return Err(ora_cancel_error());
            }
            Err(err) => return Err(runtime_error(err)),
        };
        let is_query = !result.columns.is_empty();
        let should_commit = !is_query && *self.autocommit.lock().map_err(runtime_error)?;
        if should_commit {
            let mut guard = self.connection.lock().map_err(runtime_error)?;
            let connection = guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
            BlockingConnection::commit(connection).map_err(runtime_error)?;
        }
        if self.cancel_requested.swap(false, Ordering::SeqCst) {
            self.drain_cancel_response()?;
            return Err(ora_cancel_error());
        }
        supplement_json_lob_column_metadata(&self.connection, &mut result.columns, call_timeout)?;
        self.warning = Python::attach(|py| query_result_warning(py, &result))?;
        let lob_context = ThinLobContext {
            connection: Arc::clone(&self.connection),
            state: Arc::clone(&self.state),
            async_mode: false,
        };
        Python::attach(|py| {
            apply_out_bind_values(
                py,
                &self.bind_vars,
                &result.out_values,
                &result.return_values,
                Some(&lob_context),
            )
        })?;
        self.state.lock().map_err(runtime_error)?.record_statement(
            statement,
            is_query,
            should_commit,
        );
        self.columns = result.columns;
        self.reset_fetch_define_state();
        self.requires_define = columns_require_define(&self.columns);
        self.rows = result.rows;
        self.row_index = 0;
        self.cursor_id = result.cursor_id;
        self.more_rows = result.more_rows;
        self.invalid_ref_cursor = false;
        self.rowcount = i64::from(num_execs);
        self.is_query = is_query;
        Ok(())
    }

    fn execute(&mut self, cursor: &Bound<'_, PyAny>) -> PyResult<()> {
        if self.statement_changed {
            self.rowfactory = None;
        }
        if !self.fetch_lobs_overridden {
            self.fetch_lobs = default_fetch_lobs(cursor.py())?;
        }
        let statement = self
            .statement
            .as_deref()
            .ok_or_else(|| PyRuntimeError::new_err("no statement prepared"))?;
        let call_timeout = {
            let value = self.state.lock().map_err(runtime_error)?.call_timeout;
            (value > 0).then_some(value)
        };
        let typed_lob_hints = typed_lob_bind_hints(cursor.py(), &self.bind_vars);
        let mut result = match cursor.py().detach({
            let connection = Arc::clone(&self.connection);
            let state = Arc::clone(&self.state);
            let statement = statement.to_string();
            let mut bind_values = self.bind_values.clone();
            let typed_lob_hints = typed_lob_hints.clone();
            let prefetchrows = self.prefetchrows;
            move || -> Result<QueryResult, String> {
                let mut guard = connection.lock().map_err(|err| err.to_string())?;
                let connection = guard
                    .as_mut()
                    .ok_or_else(|| "connection is closed".to_string())?;
                apply_pending_current_schema_from_state(&state, connection, call_timeout)
                    .map_err(|err| err.to_string())?;
                materialize_typed_lob_bind_values(
                    connection,
                    &mut bind_values,
                    &typed_lob_hints,
                    call_timeout,
                )?;
                BlockingConnection::execute_query_with_binds_and_timeout(
                    connection,
                    &statement,
                    prefetchrows,
                    &bind_values,
                    call_timeout,
                )
                .map_err(|err| err.to_string())
            }
        }) {
            Ok(result) => result,
            Err(_) if self.cancel_requested.swap(false, Ordering::SeqCst) => {
                return Err(ora_cancel_error());
            }
            Err(err) => return Err(runtime_error(err)),
        };
        if self.cancel_requested.swap(false, Ordering::SeqCst) {
            self.drain_cancel_response()?;
            return Err(ora_cancel_error());
        }
        supplement_json_lob_column_metadata(&self.connection, &mut result.columns, call_timeout)?;
        self.warning = Python::attach(|py| query_result_warning(py, &result))?;
        let lob_context = ThinLobContext {
            connection: Arc::clone(&self.connection),
            state: Arc::clone(&self.state),
            async_mode: false,
        };
        Python::attach(|py| {
            apply_out_bind_values(
                py,
                &self.bind_vars,
                &result.out_values,
                &result.return_values,
                Some(&lob_context),
            )
        })?;
        let is_query = !result.columns.is_empty();
        let is_plsql = statement_is_plsql(statement);
        let should_commit = !is_query && *self.autocommit.lock().map_err(runtime_error)?;
        if should_commit {
            let mut guard = self.connection.lock().map_err(runtime_error)?;
            let connection = guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
            BlockingConnection::commit(connection).map_err(runtime_error)?;
        }
        self.state.lock().map_err(runtime_error)?.record_statement(
            statement,
            is_query,
            should_commit,
        );
        self.columns = result.columns;
        self.reset_fetch_define_state();
        self.requires_define = columns_require_define(&self.columns);
        self.rows = result.rows;
        self.row_index = 0;
        self.cursor_id = result.cursor_id;
        self.more_rows = result.more_rows;
        self.invalid_ref_cursor = false;
        self.rowcount = if is_query || is_plsql {
            0
        } else {
            i64::try_from(result.row_count).unwrap_or(i64::MAX)
        };
        self.is_query = is_query;
        Ok(())
    }

    fn is_query(&self, _connection: &Bound<'_, PyAny>) -> bool {
        self.is_query
    }

    fn fetch_next_row(
        &mut self,
        py: Python<'_>,
        _cursor: &Bound<'_, PyAny>,
    ) -> PyResult<Option<Py<PyAny>>> {
        if self.invalid_ref_cursor {
            return Err(raise_oracledb_driver_error("ERR_INVALID_REF_CURSOR"));
        }
        self.prepare_fetch_defines(py, _cursor)?;
        if self.row_index >= self.rows.len() && self.more_rows && self.cursor_id != 0 {
            let previous_row = self.rows.last().cloned();
            let requires_define = self.requires_define;
            let define_columns = self.fetch_define_columns.clone();
            let mut guard = self.connection.lock().map_err(runtime_error)?;
            let connection = guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
            let result = if requires_define {
                BlockingConnection::define_and_fetch_rows_with_columns(
                    connection,
                    self.cursor_id,
                    self.prefetchrows,
                    &define_columns,
                    previous_row.as_deref(),
                )
            } else {
                BlockingConnection::fetch_rows_with_columns(
                    connection,
                    self.cursor_id,
                    self.arraysize,
                    &self.columns,
                    previous_row.as_deref(),
                )
            }
            .map_err(runtime_error)?;
            if !result.columns.is_empty() {
                self.columns = result.columns;
            } else if requires_define {
                self.columns = define_columns;
            }
            self.rows = result.rows;
            self.row_index = 0;
            if result.cursor_id != 0 {
                self.cursor_id = result.cursor_id;
            }
            self.more_rows = result.more_rows;
            if requires_define {
                self.requires_define = false;
            }
            self.invalid_ref_cursor = false;
        }
        let Some(row) = self.rows.get(self.row_index) else {
            return Ok(None);
        };
        self.row_index += 1;
        self.rowcount += 1;
        let lob_context = ThinLobContext {
            connection: Arc::clone(&self.connection),
            state: Arc::clone(&self.state),
            async_mode: self.fetch_async_lobs,
        };
        let values = row
            .iter()
            .enumerate()
            .map(|(index, value)| {
                if let Some(Some(var)) = self.fetch_vars.get(index) {
                    return var
                        .borrow(py)
                        .output_value_to_py(py, value, Some(&lob_context));
                }
                if self
                    .columns
                    .get(index)
                    .is_some_and(|metadata| metadata.is_json)
                {
                    return json_query_value_to_py(py, value, Some(_cursor), Some(&lob_context));
                }
                query_value_to_py(
                    py,
                    value,
                    Some(_cursor),
                    Some(&lob_context),
                    self.fetch_lobs,
                )
            })
            .collect::<PyResult<Vec<_>>>()?;
        let tuple = PyTuple::new(py, values)?;
        if let Some(rowfactory) = &self.rowfactory {
            return rowfactory.call1(py, tuple).map(Some).map_err(Into::into);
        }
        Ok(Some(tuple.unbind().into()))
    }

    #[pyo3(name = "get_fetch_vars")]
    fn get_fetch_vars_method(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.fetch_vars_attr(py)
    }

    #[getter(bind_vars)]
    fn bind_vars_attr(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let values = self
            .bind_vars
            .iter()
            .map(|value| value.clone_ref(py))
            .collect::<Vec<_>>();
        Ok(PyList::new(py, values)?.unbind().into())
    }

    fn get_bind_vars(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.bind_vars_attr(py)
    }

    fn setinputsizes(
        &mut self,
        py: Python<'_>,
        connection: &Bound<'_, PyAny>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let has_args = !args.is_empty();
        let has_kwargs = kwargs.is_some_and(|value| !value.is_empty());
        if has_args && has_kwargs {
            return Err(raise_oracledb_driver_error("ERR_ARGS_AND_KEYWORD_ARGS"));
        }
        self.has_positional_input_sizes = has_args;
        self.has_named_input_sizes = has_kwargs;
        self.named_input_sizes.clear();
        if has_kwargs {
            let kwargs = kwargs.expect("has_kwargs implies kwargs is present");
            let result = PyDict::new(py);
            for (key, value) in kwargs.iter() {
                let key = key.extract::<String>()?;
                let var = thin_var_from_input_size(py, connection, &value)?;
                self.named_input_sizes
                    .push((key.clone(), var.clone_ref(py).into_any()));
                result.set_item(key, var)?;
            }
            return Ok(result.unbind().into());
        }
        let result = PyList::empty(py);
        for value in args.iter() {
            let var = thin_var_from_input_size(py, connection, &value)?;
            result.append(var.clone_ref(py))?;
            self.named_input_sizes
                .push((result.len().to_string(), var.into_any()));
        }
        Ok(result.unbind().into())
    }

    #[pyo3(signature = (
        connection,
        typ,
        size=0,
        num_elements=1,
        inconverter=None,
        outconverter=None,
        encoding_errors=None,
        bypass_decode=false,
        convert_nulls=false,
        is_array=false
    ))]
    fn create_var(
        &self,
        py: Python<'_>,
        connection: &Bound<'_, PyAny>,
        typ: &Bound<'_, PyAny>,
        size: u32,
        num_elements: u32,
        inconverter: Option<Py<PyAny>>,
        outconverter: Option<Py<PyAny>>,
        encoding_errors: Option<String>,
        bypass_decode: bool,
        convert_nulls: bool,
        is_array: bool,
    ) -> PyResult<Py<ThinVar>> {
        let _ = inconverter;
        let _ = encoding_errors;
        thin_var_from_type_spec(
            py,
            connection,
            typ,
            size,
            is_array,
            num_elements,
            outconverter,
            convert_nulls,
            bypass_decode,
        )
    }

    fn get_array_dml_row_counts(&self) -> PyResult<Vec<u64>> {
        Err(not_implemented("ThinCursorImpl.get_array_dml_row_counts"))
    }

    fn get_batch_errors(&self) -> PyResult<Vec<Py<PyAny>>> {
        Err(not_implemented("ThinCursorImpl.get_batch_errors"))
    }

    fn get_bind_names(&self) -> Vec<String> {
        self.bind_names
            .iter()
            .map(|name| public_bind_name(name))
            .collect()
    }

    fn get_implicit_results(&self, _connection: &Bound<'_, PyAny>) -> PyResult<Vec<Py<PyAny>>> {
        Err(not_implemented("ThinCursorImpl.get_implicit_results"))
    }

    fn get_lastrowid(&self) -> Option<String> {
        None
    }
}

fn thin_lob_context_from_cursor(owner_cursor: &Bound<'_, PyAny>) -> PyResult<ThinLobContext> {
    let impl_obj = owner_cursor.getattr("_impl")?;
    if let Ok(cursor_impl) = impl_obj.extract::<PyRef<'_, ThinCursorImpl>>() {
        return Ok(ThinLobContext {
            connection: Arc::clone(&cursor_impl.connection),
            state: Arc::clone(&cursor_impl.state),
            async_mode: false,
        });
    }
    let cursor_impl = impl_obj.extract::<PyRef<'_, AsyncThinCursorImpl>>()?;
    Ok(ThinLobContext {
        connection: Arc::clone(&cursor_impl.inner.connection),
        state: Arc::clone(&cursor_impl.inner.state),
        async_mode: true,
    })
}

fn connection_object_type_impl(
    connection: &Bound<'_, PyAny>,
    name: &str,
) -> PyResult<DbObjectTypeImpl> {
    let impl_obj = connection.getattr("_impl")?;
    if let Ok(conn_impl) = impl_obj.extract::<PyRef<'_, ThinConnImpl>>() {
        return conn_impl.get_type(connection, name);
    }
    if let Ok(conn_impl) = impl_obj.extract::<PyRef<'_, AsyncThinConnImpl>>() {
        return conn_impl.inner.get_type(connection, name);
    }
    let public_type = connection.call_method1("gettype", (name,))?;
    py_db_object_type_impl(&public_type)?
        .ok_or_else(|| PyRuntimeError::new_err("gettype() did not return a DbObjectType"))
}

fn direct_lob_value_to_py(
    py: Python<'_>,
    ora_type_num: u8,
    csfrm: u8,
    locator: &[u8],
    context: &ThinLobContext,
) -> PyResult<Py<PyAny>> {
    let call_timeout = {
        let value = context.state.lock().map_err(runtime_error)?.call_timeout;
        (value > 0).then_some(value)
    };
    let mut guard = context.connection.lock().map_err(runtime_error)?;
    let connection = guard
        .as_mut()
        .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
    let result = BlockingConnection::read_lob_with_timeout(
        connection,
        locator,
        1,
        u64::from(u32::MAX),
        call_timeout,
    )
    .map_err(runtime_error)?;
    lob_data_to_py(
        py,
        ora_type_num,
        csfrm,
        Some(&result.locator),
        result.data.as_deref().unwrap_or_default(),
        1,
        None,
    )
}

fn query_value_to_py(
    py: Python<'_>,
    value: &Option<QueryValue>,
    owner_cursor: Option<&Bound<'_, PyAny>>,
    lob_context: Option<&ThinLobContext>,
    fetch_lobs: bool,
) -> PyResult<Py<PyAny>> {
    match value {
        None => Ok(py.None()),
        Some(QueryValue::Text(value)) => Ok(value.clone().into_pyobject(py)?.unbind().into()),
        Some(QueryValue::Rowid(value)) => Ok(value.clone().into_pyobject(py)?.unbind().into()),
        Some(QueryValue::Raw(value)) => Ok(value.clone().into_pyobject(py)?.unbind().into()),
        Some(QueryValue::BinaryDouble(value)) => {
            let value = value.parse::<f64>().map_err(runtime_error)?;
            Ok(value.into_pyobject(py)?.unbind().into())
        }
        Some(QueryValue::Number { text, is_integer }) if *is_integer => {
            python_int_from_decimal_text(py, text)
        }
        Some(QueryValue::Number { text, .. }) => {
            let value = text.parse::<f64>().map_err(runtime_error)?;
            Ok(value.into_pyobject(py)?.unbind().into())
        }
        Some(QueryValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
        }) => {
            let datetime = PyModule::import(py, "datetime")?.getattr("datetime")?;
            let microsecond = nanosecond / 1000;
            Ok(datetime
                .call1((*year, *month, *day, *hour, *minute, *second, microsecond))?
                .unbind())
        }
        Some(QueryValue::Array(values)) => {
            let values = values
                .iter()
                .map(|value| query_value_to_py(py, value, owner_cursor, lob_context, fetch_lobs))
                .collect::<PyResult<Vec<_>>>()?;
            Ok(PyList::new(py, values)?.unbind().into())
        }
        Some(QueryValue::Cursor { columns, cursor_id }) => {
            let Some(owner_cursor) = owner_cursor else {
                return Err(not_implemented("ThinCursorImpl cursor value conversion"));
            };
            let connection = owner_cursor.getattr("connection")?;
            let child_cursor = connection.call_method0("cursor")?;
            hydrate_cursor_impl(&child_cursor, columns, *cursor_id, false)?;
            Ok(child_cursor.unbind())
        }
        Some(QueryValue::Lob {
            ora_type_num,
            csfrm,
            locator,
            size,
            chunk_size,
        }) => {
            let context = match (lob_context, owner_cursor) {
                (Some(context), _) => context.clone(),
                (None, Some(owner_cursor)) => thin_lob_context_from_cursor(owner_cursor)?,
                (None, None) => return Err(not_implemented("ThinCursorImpl LOB value conversion")),
            };
            if !fetch_lobs {
                return direct_lob_value_to_py(py, *ora_type_num, *csfrm, locator, &context);
            }
            let bfile_name = (*ora_type_num == ORA_TYPE_NUM_BFILE)
                .then(|| decode_bfile_locator_name(locator))
                .flatten();
            py_lob_from_impl(
                py,
                ThinLob {
                    data: None,
                    locator: Arc::new(Mutex::new(Some(locator.clone()))),
                    ora_type_num: *ora_type_num,
                    csfrm: *csfrm,
                    size: *size,
                    chunk_size: *chunk_size,
                    context: Some(context),
                    is_open: Arc::new(Mutex::new(false)),
                    bfile_name,
                },
            )
        }
        Some(QueryValue::Object {
            schema,
            type_name,
            packed_data,
        }) => {
            if type_name
                .as_deref()
                .is_some_and(|name| name.eq_ignore_ascii_case("XMLTYPE"))
            {
                return decode_dbobject_xmltype(py, packed_data);
            }
            let Some(owner_cursor) = owner_cursor else {
                return Err(not_implemented("ThinCursorImpl DbObject value conversion"));
            };
            let type_name = type_name
                .as_deref()
                .ok_or_else(|| PyRuntimeError::new_err("missing DbObject type name"))?;
            let fqn = schema
                .as_deref()
                .filter(|schema| !schema.is_empty())
                .map(|schema| format!("{schema}.{type_name}"))
                .unwrap_or_else(|| type_name.to_string());
            let connection = owner_cursor.getattr("connection")?;
            let object_type = connection_object_type_impl(&connection, &fqn)?;
            let lob_context = match lob_context {
                Some(context) => Some(context.clone()),
                None => Some(thin_lob_context_from_cursor(owner_cursor)?),
            };
            py_db_object_from_impl(
                py,
                DbObjectImpl::with_packed_data(object_type, packed_data.clone(), lob_context),
            )
        }
    }
}

fn json_query_value_to_py(
    py: Python<'_>,
    value: &Option<QueryValue>,
    owner_cursor: Option<&Bound<'_, PyAny>>,
    lob_context: Option<&ThinLobContext>,
) -> PyResult<Py<PyAny>> {
    let value = query_value_to_py(py, value, owner_cursor, lob_context, false)?;
    if value.bind(py).is_none() {
        return Ok(value);
    }
    Ok(PyModule::import(py, "json")?
        .getattr("loads")?
        .call1((value.bind(py),))?
        .unbind())
}

#[pyclass(module = "oracledb.thin_impl", name = "AsyncThinCursorImpl")]
struct AsyncThinCursorImpl {
    inner: ThinCursorImpl,
}

#[pymethods]
impl AsyncThinCursorImpl {
    #[getter]
    fn arraysize(&self) -> u32 {
        self.inner.arraysize
    }

    #[setter]
    fn set_arraysize(&mut self, value: u32) {
        self.inner.arraysize = value;
    }

    #[getter]
    fn prefetchrows(&self) -> u32 {
        self.inner.prefetchrows
    }

    #[setter]
    fn set_prefetchrows(&mut self, value: u32) {
        self.inner.prefetchrows = value;
    }

    #[getter]
    fn scrollable(&self) -> bool {
        self.inner.scrollable
    }

    #[setter]
    fn set_scrollable(&mut self, value: bool) {
        self.inner.scrollable = value;
    }

    #[getter]
    fn rowcount(&self) -> i64 {
        self.inner.rowcount
    }

    #[getter]
    fn statement(&self) -> Option<&str> {
        self.inner.statement.as_deref()
    }

    #[getter]
    #[pyo3(name = "fetch_vars")]
    fn fetch_vars_attr(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.inner.fetch_vars_attr(py)
    }

    #[getter]
    fn fetch_metadata(&self) -> Vec<FetchMetadataImpl> {
        self.inner.fetch_metadata()
    }

    #[getter]
    fn fetch_lobs(&self) -> bool {
        self.inner.fetch_lobs
    }

    #[setter]
    fn set_fetch_lobs(&mut self, value: bool) {
        self.inner.fetch_lobs = value;
        self.inner.fetch_lobs_overridden = true;
    }

    #[getter]
    fn fetch_decimals(&self) -> bool {
        self.inner.fetch_decimals
    }

    #[setter]
    fn set_fetch_decimals(&mut self, value: bool) {
        self.inner.fetch_decimals = value;
    }

    #[getter]
    fn suspend_on_success(&self) -> bool {
        self.inner.suspend_on_success
    }

    #[setter]
    fn set_suspend_on_success(&mut self, value: bool) {
        self.inner.suspend_on_success = value;
    }

    #[getter]
    fn rowfactory(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.inner
            .rowfactory
            .as_ref()
            .map(|value| value.clone_ref(py))
    }

    #[setter]
    fn set_rowfactory(&mut self, value: Option<Py<PyAny>>) {
        self.inner.rowfactory = value;
    }

    #[getter]
    fn inputtypehandler(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.inner
            .inputtypehandler
            .as_ref()
            .map(|value| value.clone_ref(py))
    }

    #[setter]
    fn set_inputtypehandler(&mut self, value: Option<Py<PyAny>>) {
        self.inner.inputtypehandler = value;
    }

    #[getter]
    fn outputtypehandler(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.inner
            .outputtypehandler
            .as_ref()
            .map(|value| value.clone_ref(py))
    }

    #[setter]
    fn set_outputtypehandler(&mut self, value: Option<Py<PyAny>>) {
        self.inner.outputtypehandler = value;
    }

    #[getter]
    fn warning(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.inner.warning.as_ref().map(|value| value.clone_ref(py))
    }

    #[getter(bind_vars)]
    fn bind_vars_attr(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.inner.bind_vars_attr(py)
    }

    fn get_bind_vars(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.inner.get_bind_vars(py)
    }

    #[pyo3(name = "get_fetch_vars")]
    fn get_fetch_vars_method(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.inner.fetch_vars_attr(py)
    }

    #[pyo3(signature = (in_del=None))]
    fn close(&mut self, in_del: Option<bool>) {
        self.inner.close(in_del)
    }

    fn prepare(
        &mut self,
        statement: Option<String>,
        tag: Option<String>,
        cache_statement: Option<bool>,
    ) -> PyResult<()> {
        self.inner.prepare(statement, tag, cache_statement)
    }

    async fn parse(&mut self, cursor: Py<PyAny>) -> PyResult<()> {
        Python::attach(|py| self.inner.parse(cursor.bind(py)))
    }

    fn _prepare_for_execute(
        &mut self,
        cursor: &Bound<'_, PyAny>,
        statement: Option<String>,
        parameters: Option<&Bound<'_, PyAny>>,
        keyword_parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        self.inner
            ._prepare_for_execute(cursor, statement, parameters, keyword_parameters)
    }

    fn _prepare_for_executemany(
        &mut self,
        cursor: &Bound<'_, PyAny>,
        statement: Option<String>,
        parameters: &Bound<'_, PyAny>,
        batch_size: u32,
    ) -> PyResult<ExecutemanyManager> {
        self.inner
            ._prepare_for_executemany(cursor, statement, parameters, batch_size)
    }

    async fn executemany(
        &mut self,
        cursor: Py<PyAny>,
        num_execs: u32,
        batcherrors: bool,
        arraydmlrowcounts: bool,
        offset: u32,
    ) -> PyResult<()> {
        Python::attach(|py| {
            self.inner.executemany(
                cursor.bind(py),
                num_execs,
                batcherrors,
                arraydmlrowcounts,
                offset,
            )
        })
    }

    async fn execute(&mut self, _cursor: Py<PyAny>) -> PyResult<()> {
        if self.inner.statement_changed {
            self.inner.rowfactory = None;
        }
        let statement = self
            .inner
            .statement
            .as_deref()
            .ok_or_else(|| PyRuntimeError::new_err("no statement prepared"))?
            .to_string();
        let call_timeout = {
            let value = self.inner.state.lock().map_err(runtime_error)?.call_timeout;
            (value > 0).then_some(value)
        };
        let query_statement = statement.clone();
        let query = spawn_blocking_task("oracledb-pyshim-async-execute", {
            let connection = Arc::clone(&self.inner.connection);
            let state = Arc::clone(&self.inner.state);
            let bind_values = self.inner.bind_values.clone();
            let prefetchrows = self.inner.prefetchrows;
            move || -> Result<QueryResult, String> {
                let mut guard = connection.lock().map_err(|err| err.to_string())?;
                let connection = guard
                    .as_mut()
                    .ok_or_else(|| "connection is closed".to_string())?;
                apply_pending_current_schema_from_state(&state, connection, call_timeout)
                    .map_err(|err| err.to_string())?;
                BlockingConnection::execute_query_with_binds_and_timeout(
                    connection,
                    &query_statement,
                    prefetchrows,
                    &bind_values,
                    call_timeout,
                )
                .map_err(|err| err.to_string())
            }
        });
        let mut result = match query.await {
            Ok(result) => result,
            Err(_) if self.inner.cancel_requested.swap(false, Ordering::SeqCst) => {
                return Err(ora_cancel_error());
            }
            Err(err) => return Err(runtime_error(err)),
        };
        if self.inner.cancel_requested.swap(false, Ordering::SeqCst) {
            self.inner.drain_cancel_response()?;
            return Err(ora_cancel_error());
        }
        supplement_json_lob_column_metadata(
            &self.inner.connection,
            &mut result.columns,
            call_timeout,
        )?;
        self.inner.warning = Python::attach(|py| query_result_warning(py, &result))?;
        let lob_context = ThinLobContext {
            connection: Arc::clone(&self.inner.connection),
            state: Arc::clone(&self.inner.state),
            async_mode: true,
        };
        Python::attach(|py| {
            apply_out_bind_values(
                py,
                &self.inner.bind_vars,
                &result.out_values,
                &result.return_values,
                Some(&lob_context),
            )
        })?;
        let is_query = !result.columns.is_empty();
        let is_plsql = statement_is_plsql(&statement);
        let should_commit = !is_query && *self.inner.autocommit.lock().map_err(runtime_error)?;
        if should_commit {
            let mut guard = self.inner.connection.lock().map_err(runtime_error)?;
            let connection = guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
            BlockingConnection::commit(connection).map_err(runtime_error)?;
        }
        self.inner
            .state
            .lock()
            .map_err(runtime_error)?
            .record_statement(&statement, is_query, should_commit);
        self.inner.columns = result.columns;
        self.inner.reset_fetch_define_state();
        self.inner.requires_define = columns_require_define(&self.inner.columns);
        self.inner.rows = result.rows;
        self.inner.row_index = 0;
        self.inner.cursor_id = result.cursor_id;
        self.inner.more_rows = result.more_rows;
        self.inner.invalid_ref_cursor = false;
        self.inner.rowcount = if is_query || is_plsql {
            0
        } else {
            i64::try_from(result.row_count).unwrap_or(i64::MAX)
        };
        self.inner.is_query = is_query;
        Ok(())
    }

    fn is_query(&self, connection: &Bound<'_, PyAny>) -> bool {
        self.inner.is_query(connection)
    }

    async fn fetch_next_row(&mut self, cursor: Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
        Python::attach(|py| {
            self.inner.fetch_async_lobs = true;
            let result = self.inner.fetch_next_row(py, cursor.bind(py));
            self.inner.fetch_async_lobs = false;
            result
        })
    }

    fn setinputsizes(
        &mut self,
        py: Python<'_>,
        connection: &Bound<'_, PyAny>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        self.inner.setinputsizes(py, connection, args, kwargs)
    }

    #[pyo3(signature = (
        connection,
        typ,
        size=0,
        num_elements=1,
        inconverter=None,
        outconverter=None,
        encoding_errors=None,
        bypass_decode=false,
        convert_nulls=false,
        is_array=false
    ))]
    fn create_var(
        &self,
        py: Python<'_>,
        connection: &Bound<'_, PyAny>,
        typ: &Bound<'_, PyAny>,
        size: u32,
        num_elements: u32,
        inconverter: Option<Py<PyAny>>,
        outconverter: Option<Py<PyAny>>,
        encoding_errors: Option<String>,
        bypass_decode: bool,
        convert_nulls: bool,
        is_array: bool,
    ) -> PyResult<Py<ThinVar>> {
        self.inner.create_var(
            py,
            connection,
            typ,
            size,
            num_elements,
            inconverter,
            outconverter,
            encoding_errors,
            bypass_decode,
            convert_nulls,
            is_array,
        )
    }

    fn get_array_dml_row_counts(&self) -> PyResult<Vec<u64>> {
        self.inner.get_array_dml_row_counts()
    }

    fn get_batch_errors(&self) -> PyResult<Vec<Py<PyAny>>> {
        self.inner.get_batch_errors()
    }

    fn get_bind_names(&self) -> Vec<String> {
        self.inner.get_bind_names()
    }

    fn get_implicit_results(&self, connection: &Bound<'_, PyAny>) -> PyResult<Vec<Py<PyAny>>> {
        self.inner.get_implicit_results(connection)
    }

    fn get_lastrowid(&self) -> Option<String> {
        self.inner.get_lastrowid()
    }

    async fn scroll(&mut self, _cursor: Py<PyAny>, _value: i32, _mode: String) -> PyResult<()> {
        Err(not_implemented("AsyncThinCursorImpl.scroll"))
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "AsyncThinConnImpl")]
struct AsyncThinConnImpl {
    inner: ThinConnImpl,
}

#[pymethods]
impl AsyncThinConnImpl {
    #[new]
    fn new(dsn: &Bound<'_, PyAny>, params_impl: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            inner: ThinConnImpl::new(dsn, params_impl)?,
        })
    }

    #[getter]
    fn dsn(&self) -> &str {
        &self.inner.dsn
    }

    #[getter]
    fn username(&self) -> &str {
        &self.inner.username
    }

    #[getter]
    fn proxy_user(&self) -> Option<&str> {
        self.inner.proxy_user.as_deref()
    }

    #[getter]
    fn thin(&self) -> bool {
        self.inner.thin
    }

    #[getter]
    fn server_version(&self) -> (u8, u8, u8, u8, u8) {
        self.inner.server_version
    }

    #[getter]
    fn warning(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.inner.warning.as_ref().map(|value| value.clone_ref(py))
    }

    #[getter]
    fn autocommit(&self) -> bool {
        self.inner.autocommit
    }

    #[setter]
    fn set_autocommit(&mut self, value: bool) -> PyResult<()> {
        self.inner.set_autocommit(value)
    }

    #[getter]
    fn inputtypehandler(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.inner
            .inputtypehandler
            .as_ref()
            .map(|value| value.clone_ref(py))
    }

    #[setter]
    fn set_inputtypehandler(&mut self, value: Option<Py<PyAny>>) {
        self.inner.inputtypehandler = value;
    }

    #[getter]
    fn outputtypehandler(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.inner
            .outputtypehandler
            .as_ref()
            .map(|value| value.clone_ref(py))
    }

    #[setter]
    fn set_outputtypehandler(&mut self, value: Option<Py<PyAny>>) {
        self.inner.outputtypehandler = value;
    }

    #[getter]
    fn tag(&self) -> Option<&str> {
        self.inner.tag.as_deref()
    }

    #[setter]
    fn set_tag(&mut self, value: Option<String>) {
        self.inner.tag = value;
    }

    #[getter]
    fn invoke_session_callback(&self) -> bool {
        self.inner.invoke_session_callback
    }

    #[setter]
    fn set_invoke_session_callback(&mut self, value: bool) {
        self.inner.invoke_session_callback = value;
    }

    async fn connect(&mut self, params_impl: Py<PyAny>) -> PyResult<()> {
        Python::attach(|py| self.inner.connect(params_impl.bind(py)))
    }

    #[pyo3(signature = (in_del=None))]
    async fn close(&self, in_del: Option<bool>) -> PyResult<()> {
        let _ = in_del;
        let Some(connection) = self.inner.take_connection_for_close()? else {
            return Ok(());
        };
        let close = spawn_blocking_task("oracledb-pyshim-async-close", move || {
            close_connection_result(connection)
        });
        close_result_to_py(close.await)
    }

    async fn ping(&self) -> PyResult<()> {
        self.inner.ping()
    }

    async fn commit(&self) -> PyResult<()> {
        self.inner.commit()
    }

    async fn rollback(&self) -> PyResult<()> {
        self.inner.rollback()
    }

    async fn change_password(&self, old_password: String, new_password: String) -> PyResult<()> {
        self.inner.change_password(&old_password, &new_password)
    }

    fn get_is_healthy(&self) -> PyResult<bool> {
        self.inner.get_is_healthy()
    }

    fn get_sdu(&self) -> PyResult<u32> {
        self.inner.get_sdu()
    }

    async fn get_type(&self, conn: Py<PyAny>, name: String) -> PyResult<DbObjectTypeImpl> {
        Python::attach(|py| self.inner.get_type(conn.bind(py), &name))
    }

    fn get_call_timeout(&self) -> PyResult<u32> {
        self.inner.get_call_timeout()
    }

    fn set_call_timeout(&self, value: u32) -> PyResult<()> {
        self.inner.set_call_timeout(value)
    }

    fn clear_end_user_security_context(&self) -> PyResult<()> {
        self.inner.clear_end_user_security_context()
    }

    fn set_end_user_security_context(&self, context: &Bound<'_, PyAny>) -> PyResult<()> {
        self.inner.set_end_user_security_context(context)
    }

    fn cancel(&self) -> PyResult<()> {
        self.inner.cancel()
    }

    fn get_ltxid<'py>(&self, py: Python<'py>) -> Py<PyBytes> {
        self.inner.get_ltxid(py)
    }

    fn get_current_schema(&self) -> PyResult<Option<String>> {
        self.inner.get_current_schema()
    }

    fn set_current_schema(&self, value: Option<String>) -> PyResult<()> {
        self.inner.set_current_schema(value)
    }

    fn get_edition(&self) -> PyResult<Option<String>> {
        self.inner.get_edition()
    }

    fn get_external_name(&self) -> PyResult<Option<String>> {
        self.inner.get_external_name()
    }

    fn set_external_name(&self, value: Option<String>) -> PyResult<()> {
        self.inner.set_external_name(value)
    }

    fn get_internal_name(&self) -> PyResult<Option<String>> {
        self.inner.get_internal_name()
    }

    fn set_internal_name(&self, value: Option<String>) -> PyResult<()> {
        self.inner.set_internal_name(value)
    }

    fn get_max_identifier_length(&self) -> Option<u8> {
        self.inner.get_max_identifier_length()
    }

    fn get_instance_name(&self) -> PyResult<String> {
        self.inner.get_instance_name()
    }

    fn get_db_name(&self) -> PyResult<String> {
        self.inner.get_db_name()
    }

    fn get_max_open_cursors(&self) -> PyResult<i64> {
        self.inner.get_max_open_cursors()
    }

    fn get_service_name(&self) -> PyResult<String> {
        self.inner.get_service_name()
    }

    fn get_db_domain(&self) -> PyResult<Option<String>> {
        self.inner.get_db_domain()
    }

    fn get_stmt_cache_size(&self) -> PyResult<u32> {
        self.inner.get_stmt_cache_size()
    }

    fn set_stmt_cache_size(&self, value: u32) -> PyResult<()> {
        self.inner.set_stmt_cache_size(value)
    }

    fn get_transaction_in_progress(&self) -> PyResult<bool> {
        self.inner.get_transaction_in_progress()
    }

    fn set_action(&self, value: Option<String>) -> PyResult<()> {
        self.inner.set_action(value)
    }

    fn set_client_identifier(&self, value: Option<String>) -> PyResult<()> {
        self.inner.set_client_identifier(value)
    }

    fn set_client_info(&self, value: Option<String>) -> PyResult<()> {
        self.inner.set_client_info(value)
    }

    fn set_dbop(&self, value: Option<String>) -> PyResult<()> {
        self.inner.set_dbop(value)
    }

    fn set_module(&self, value: Option<String>) -> PyResult<()> {
        self.inner.set_module(value)
    }

    fn get_session_id(&self) -> PyResult<u32> {
        self.inner.get_session_id()
    }

    fn get_serial_num(&self) -> PyResult<u16> {
        self.inner.get_serial_num()
    }

    async fn create_temp_lob_impl(&self, lob_type: Py<PyAny>) -> PyResult<Py<AsyncThinLob>> {
        Python::attach(|py| {
            Py::new(
                py,
                AsyncThinLob {
                    inner: self.inner.create_temp_lob_value(lob_type.bind(py), true)?,
                },
            )
        })
    }

    fn create_cursor_impl(&self, scrollable: bool) -> AsyncThinCursorImpl {
        AsyncThinCursorImpl {
            inner: self.inner.create_cursor_impl(scrollable),
        }
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "ThinPoolImpl")]
struct ThinPoolImpl {
    #[pyo3(get)]
    dsn: String,
    #[pyo3(get)]
    username: String,
    #[pyo3(get)]
    homogeneous: bool,
    #[pyo3(get)]
    increment: u32,
    #[pyo3(get)]
    max: u32,
    #[pyo3(get)]
    min: u32,
    #[pyo3(get)]
    name: String,
    getmode: u32,
    max_lifetime_session: u32,
    max_sessions_per_shard: u32,
    opened: Arc<Mutex<bool>>,
    open_count: Arc<Mutex<u32>>,
    busy_count: Arc<Mutex<u32>>,
    ping_interval: u32,
    soda_metadata_cache: bool,
    stmt_cache_size: u32,
    timeout: u32,
    wait_timeout: u32,
}

#[pymethods]
impl ThinPoolImpl {
    #[new]
    fn new(dsn: &Bound<'_, PyAny>, params_impl: &Bound<'_, PyAny>) -> PyResult<Self> {
        let dsn = normalize_connect_string(dsn.extract()?);
        let username = get_string_attr(params_impl, "user")?;
        let min = get_optional_u32_attr(params_impl, "min")?.unwrap_or(1);
        let max = get_optional_u32_attr(params_impl, "max")?.unwrap_or(2);
        let increment = get_optional_u32_attr(params_impl, "increment")?.unwrap_or(1);
        let homogeneous = get_optional_bool_attr(params_impl, "homogeneous")?.unwrap_or(true);
        let getmode = get_optional_u32_attr(params_impl, "getmode")?.unwrap_or(0);
        let max_lifetime_session =
            get_optional_u32_attr(params_impl, "max_lifetime_session")?.unwrap_or(0);
        let max_sessions_per_shard =
            get_optional_u32_attr(params_impl, "max_sessions_per_shard")?.unwrap_or(0);
        let ping_interval = get_optional_u32_attr(params_impl, "ping_interval")?.unwrap_or(60);
        let soda_metadata_cache =
            get_optional_bool_attr(params_impl, "soda_metadata_cache")?.unwrap_or(false);
        let stmt_cache_size = get_optional_u32_attr(params_impl, "stmtcachesize")?.unwrap_or(20);
        let timeout = get_optional_u32_attr(params_impl, "timeout")?.unwrap_or(0);
        let wait_timeout = get_optional_u32_attr(params_impl, "wait_timeout")?.unwrap_or(0);
        Ok(Self {
            dsn,
            username,
            homogeneous,
            increment,
            max,
            min,
            name: String::new(),
            getmode,
            max_lifetime_session,
            max_sessions_per_shard,
            opened: Arc::new(Mutex::new(true)),
            open_count: Arc::new(Mutex::new(0)),
            busy_count: Arc::new(Mutex::new(0)),
            ping_interval,
            soda_metadata_cache,
            stmt_cache_size,
            timeout,
            wait_timeout,
        })
    }

    fn acquire(&self, _params_impl: &Bound<'_, PyAny>) -> PyResult<()> {
        if !*self.opened.lock().map_err(runtime_error)? {
            return Err(raise_oracledb_driver_error("ERR_POOL_NOT_OPEN"));
        }
        Err(not_implemented("ThinPoolImpl.acquire"))
    }

    fn close(&self, _force: bool) -> PyResult<()> {
        *self.opened.lock().map_err(runtime_error)? = false;
        *self.open_count.lock().map_err(runtime_error)? = 0;
        *self.busy_count.lock().map_err(runtime_error)? = 0;
        Ok(())
    }

    fn drop(&self, _conn_impl: &Bound<'_, PyAny>) -> PyResult<()> {
        Err(not_implemented("ThinPoolImpl.drop"))
    }

    fn get_busy_count(&self) -> PyResult<u32> {
        Ok(*self.busy_count.lock().map_err(runtime_error)?)
    }

    fn get_getmode(&self) -> u32 {
        self.getmode
    }

    fn get_max_lifetime_session(&self) -> u32 {
        self.max_lifetime_session
    }

    fn get_max_sessions_per_shard(&self) -> u32 {
        self.max_sessions_per_shard
    }

    fn get_open_count(&self) -> PyResult<u32> {
        Ok(*self.open_count.lock().map_err(runtime_error)?)
    }

    fn get_ping_interval(&self) -> u32 {
        self.ping_interval
    }

    fn get_soda_metadata_cache(&self) -> bool {
        self.soda_metadata_cache
    }

    fn get_stmt_cache_size(&self) -> u32 {
        self.stmt_cache_size
    }

    fn get_timeout(&self) -> u32 {
        self.timeout
    }

    fn get_wait_timeout(&self) -> u32 {
        if self.getmode == 2 {
            self.wait_timeout
        } else {
            0
        }
    }

    fn reconfigure(&mut self, min: u32, max: u32, increment: u32) {
        self.min = min;
        self.max = max;
        self.increment = increment;
    }

    fn return_connection(&self, _conn_impl: &Bound<'_, PyAny>) -> PyResult<()> {
        Err(not_implemented("ThinPoolImpl.return_connection"))
    }

    fn set_getmode(&mut self, value: u32) {
        self.getmode = value;
        if value != 2 {
            self.wait_timeout = 0;
        }
    }

    fn set_max_lifetime_session(&mut self, value: u32) {
        self.max_lifetime_session = value;
    }

    fn set_max_sessions_per_shard(&mut self, value: u32) {
        self.max_sessions_per_shard = value;
    }

    fn set_ping_interval(&mut self, value: u32) {
        self.ping_interval = value;
    }

    fn set_soda_metadata_cache(&mut self, value: bool) {
        self.soda_metadata_cache = value;
    }

    fn set_stmt_cache_size(&mut self, value: u32) {
        self.stmt_cache_size = value;
    }

    fn set_timeout(&mut self, value: u32) {
        self.timeout = value;
    }

    fn set_wait_timeout(&mut self, value: u32) {
        self.wait_timeout = value;
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "AsyncThinPoolImpl")]
struct AsyncThinPoolImpl {
    opened: Arc<Mutex<bool>>,
}

#[pymethods]
impl AsyncThinPoolImpl {
    #[new]
    fn new(_dsn: &Bound<'_, PyAny>, _params_impl: &Bound<'_, PyAny>) -> Self {
        Self {
            opened: Arc::new(Mutex::new(true)),
        }
    }

    async fn acquire(&self, _params_impl: Py<PyAny>) -> PyResult<()> {
        if !*self.opened.lock().map_err(runtime_error)? {
            return Err(raise_oracledb_driver_error("ERR_POOL_NOT_OPEN"));
        }
        Err(not_implemented("AsyncThinPoolImpl.acquire"))
    }

    async fn close(&self, _force: bool) -> PyResult<()> {
        *self.opened.lock().map_err(runtime_error)? = false;
        Ok(())
    }

    async fn drop(&self, _conn_impl: Py<PyAny>) -> PyResult<()> {
        if !*self.opened.lock().map_err(runtime_error)? {
            return Err(raise_oracledb_driver_error("ERR_POOL_NOT_OPEN"));
        }
        Err(not_implemented("AsyncThinPoolImpl.drop"))
    }

    async fn return_connection(&self, _conn_impl: Py<PyAny>, _in_del: bool) -> PyResult<()> {
        if !*self.opened.lock().map_err(runtime_error)? {
            return Err(raise_oracledb_driver_error("ERR_POOL_NOT_OPEN"));
        }
        Err(not_implemented("AsyncThinPoolImpl.return_connection"))
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "EndUserSecurityContextImpl")]
#[derive(Default)]
struct EndUserSecurityContextImpl {
    #[allow(dead_code)]
    payload: BTreeMap<String, String>,
    #[allow(dead_code)]
    encoded_len: usize,
}

#[pymethods]
impl EndUserSecurityContextImpl {
    #[staticmethod]
    fn create_end_user_security_context(
        end_user_token: &Bound<'_, PyAny>,
        end_user_name: &Bound<'_, PyAny>,
        key: &Bound<'_, PyAny>,
        database_access_token: &Bound<'_, PyAny>,
        data_roles: &Bound<'_, PyAny>,
        attributes: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let mut payload = BTreeMap::new();
        payload.insert("ver".to_string(), "1.0".to_string());
        if let Some(value) = extract_optional_string(end_user_token)? {
            payload.insert("end_user_token".to_string(), value);
        }
        if let Some(value) = extract_optional_string(end_user_name)? {
            payload.insert("end_user_name".to_string(), value);
        }
        if let Some(value) = extract_optional_string(key)? {
            payload.insert("end_user_contextid".to_string(), value);
        }
        if let Some(value) = extract_optional_string(database_access_token)? {
            payload.insert("database_access_token".to_string(), value);
        }
        if !data_roles.is_none() {
            payload.insert("data_roles".to_string(), data_roles.str()?.to_string());
        }
        if !attributes.is_none() {
            payload.insert("attributes".to_string(), attributes.str()?.to_string());
        }
        let encoded_len = payload
            .iter()
            .map(|(key, value)| key.len() + value.len() + 8)
            .sum::<usize>();
        if encoded_len > 65_535 {
            return Err(raise_oracledb_driver_error(
                "ERR_INVALID_END_USER_SECURITY_CONTEXT_LENGTH",
            ));
        }
        Ok(Self {
            payload,
            encoded_len,
        })
    }
}

#[pymodule]
fn oracledb_pyshim(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init_thin_impl, m)?)?;
    m.add_function(wrap_pyfunction!(record_next_connect_args, m)?)?;
    m.add_function(wrap_pyfunction!(discard_pending_connect_args, m)?)?;
    m.add_class::<ThinConnImpl>()?;
    m.add_class::<ThinLob>()?;
    m.add_class::<AsyncThinLob>()?;
    m.add_class::<DbObjectTypeImpl>()?;
    m.add_class::<DbObjectAttrImpl>()?;
    m.add_class::<DbObjectImpl>()?;
    m.add_class::<ThinCursorImpl>()?;
    m.add_class::<AsyncThinCursorImpl>()?;
    m.add_class::<FetchMetadataImpl>()?;
    m.add_class::<ExecutemanyManager>()?;
    m.add_class::<AsyncThinConnImpl>()?;
    m.add_class::<ThinPoolImpl>()?;
    m.add_class::<AsyncThinPoolImpl>()?;
    m.add_class::<EndUserSecurityContextImpl>()?;
    Ok(())
}
