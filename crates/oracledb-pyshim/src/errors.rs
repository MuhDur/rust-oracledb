use std::sync::{Arc, Mutex};

use oracledb::protocol::thin::QueryResult;
use oracledb::protocol::ServerErrorDetails;
use oracledb::Connection as RustConnection;
use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::*;

pub(crate) fn not_implemented(name: &str) -> PyErr {
    PyNotImplementedError::new_err(format!(
        "{name} is a Rust shim placeholder; M1+ must route this through the oracledb crate"
    ))
}

pub(crate) fn runtime_error(err: impl std::fmt::Display) -> PyErr {
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

pub(crate) fn connection_closed_error() -> PyErr {
    raise_oracledb_driver_error("ERR_NOT_CONNECTED")
}

pub(crate) fn parse_ora_code(message: &str) -> Option<i32> {
    let start = message.find("ORA-")? + "ORA-".len();
    let digits = message.get(start..start + 5)?;
    digits
        .chars()
        .all(|ch| ch.is_ascii_digit())
        .then(|| digits.parse::<i32>().ok())
        .flatten()
}

pub(crate) fn parse_ora_offset(message: &str) -> Option<i32> {
    let column_start = message.find(", column ")? + ", column ".len();
    let digits = message
        .get(column_start..)?
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    let column = digits.parse::<i32>().ok()?;
    Some(column.saturating_sub(1))
}

pub(crate) fn database_error(py: Python<'_>, message: &str) -> PyResult<PyErr> {
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

/// Builds the Python `_Error`/exception for a structured server error and
/// reports whether the error marks the session dead (reference
/// messages/base.pyx `_check_and_raise_exception`).
pub(crate) fn server_error_details_to_pyerr(details: &ServerErrorDetails) -> (PyErr, bool) {
    Python::attach(|py| -> PyResult<(PyErr, bool)> {
        let errors = PyModule::import(py, "oracledb.errors")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("message", &details.message)?;
        if details.code != 0 {
            kwargs.set_item("code", details.code)?;
        } else if let Some(code) = parse_ora_code(&details.message) {
            kwargs.set_item("code", code)?;
        }
        if details.pos > 0 {
            kwargs.set_item("offset", details.pos)?;
        } else if let Some(offset) = parse_ora_offset(&details.message) {
            kwargs.set_item("offset", offset)?;
        }
        let error_obj = errors.getattr("_Error")?.call((), Some(&kwargs))?;
        let is_session_dead = error_obj.getattr("is_session_dead")?.extract::<bool>()?;
        let exc_type = error_obj.getattr("exc_type")?;
        let exc = exc_type.call1((error_obj,))?;
        Ok((PyErr::from_value(exc), is_session_dead))
    })
    .unwrap_or_else(|_| (PyRuntimeError::new_err(details.message.clone()), false))
}

/// Converts a task error to a Python exception. Structured server errors keep
/// their code/offset; a dead-session error force-disconnects the connection so
/// `is_healthy()` reports false (reference `_Error.is_session_dead` →
/// `protocol._disconnect()`).
pub(crate) fn raise_task_error(
    err: &TaskError,
    connection: &Arc<Mutex<Option<RustConnection>>>,
) -> PyErr {
    if let Some(details) = err.server_error_details() {
        let (pyerr, is_session_dead) = server_error_details_to_pyerr(details);
        if is_session_dead {
            if let Ok(mut guard) = connection.lock() {
                *guard = None;
            }
        }
        pyerr
    } else {
        runtime_error(err)
    }
}

pub(crate) fn compilation_error_warning(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let errors = PyModule::import(py, "oracledb.errors")?;
    Ok(errors.getattr("_create_warning")?.call1((7000,))?.unbind())
}

pub(crate) fn query_result_warning(
    py: Python<'_>,
    result: &QueryResult,
) -> PyResult<Option<Py<PyAny>>> {
    result
        .compilation_error_warning
        .then(|| compilation_error_warning(py))
        .transpose()
}

pub(crate) fn dpy_database_error(code: &str, message: &str) -> PyErr {
    Python::attach(|py| database_error(py, &format!("{code}: {message}")))
        .unwrap_or_else(|_| PyRuntimeError::new_err(format!("{code}: {message}")))
}

pub(crate) fn ora_database_error(message: &str) -> PyErr {
    Python::attach(|py| database_error(py, message))
        .unwrap_or_else(|_| PyRuntimeError::new_err(message.to_string()))
}

pub(crate) fn ora_cancel_error() -> PyErr {
    ora_database_error("ORA-01013: user requested cancel of current operation")
}

pub(crate) fn dpy_bind_error(code: &str, message: impl std::fmt::Display) -> PyErr {
    dpy_database_error(code, &message.to_string())
}

pub(crate) fn raise_column_truncated() -> PyErr {
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

pub(crate) fn raise_dml_returning_dup_bind(name: &str) -> PyErr {
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

pub(crate) fn raise_oracledb_driver_error(error_name: &str) -> PyErr {
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

pub(crate) fn raise_wrong_executemany_parameters_type() -> PyErr {
    raise_oracledb_driver_error("ERR_WRONG_EXECUTEMANY_PARAMETERS_TYPE")
}

pub(crate) fn raise_incorrect_var_arraysize(
    var_arraysize: usize,
    required_arraysize: usize,
) -> PyErr {
    Python::attach(|py| -> PyResult<PyErr> {
        let errors = PyModule::import(py, "oracledb.errors")?;
        let error_num = errors.getattr("ERR_INCORRECT_VAR_ARRAYSIZE")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("var_arraysize", var_arraysize)?;
        kwargs.set_item("required_arraysize", required_arraysize)?;
        match errors
            .getattr("_raise_err")?
            .call((error_num,), Some(&kwargs))
        {
            Ok(_) => Ok(PyRuntimeError::new_err(
                "oracledb.errors._raise_err(ERR_INCORRECT_VAR_ARRAYSIZE) returned without raising",
            )),
            Err(err) => Ok(err),
        }
    })
    .unwrap_or_else(|_| {
        PyRuntimeError::new_err(format!(
            "DPY-2016: variable array size of {var_arraysize} is too small (should be at least {required_arraysize})"
        ))
    })
}

pub(crate) fn raise_not_supported(feature: &str) -> PyErr {
    Python::attach(|py| -> PyResult<PyErr> {
        let errors = PyModule::import(py, "oracledb.errors")?;
        match errors.getattr("_raise_not_supported")?.call1((feature,)) {
            Ok(_) => Ok(PyRuntimeError::new_err(
                "oracledb.errors._raise_not_supported returned without raising",
            )),
            Err(err) => Ok(err),
        }
    })
    .unwrap_or_else(|_| {
        PyRuntimeError::new_err(format!(
            "DPY-3001: {feature} is only supported in python-oracledb thick mode"
        ))
    })
}

pub(crate) fn raise_python_type_not_supported(typ: &Bound<'_, PyAny>) -> PyErr {
    let py = typ.py();
    (|| -> PyResult<PyErr> {
        let errors = PyModule::import(py, "oracledb.errors")?;
        let error_num = errors.getattr("ERR_PYTHON_TYPE_NOT_SUPPORTED")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("typ", typ)?;
        match errors
            .getattr("_raise_err")?
            .call((error_num,), Some(&kwargs))
        {
            Ok(_) => Ok(PyRuntimeError::new_err(
                "oracledb.errors._raise_err(ERR_PYTHON_TYPE_NOT_SUPPORTED) returned without raising",
            )),
            Err(err) => Ok(err),
        }
    })()
    .unwrap_or_else(|_| PyRuntimeError::new_err("DPY-3003: Python type is not supported"))
}

pub(crate) fn raise_call_timeout_exceeded(timeout: u32) -> PyErr {
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

pub(crate) fn raise_invalid_object_type_name(name: &str) -> PyErr {
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

pub(crate) fn raise_invalid_coll_index_get(index: i32) -> PyErr {
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

pub(crate) fn raise_invalid_coll_index_set(index: i32, min_index: i32, max_index: i32) -> PyErr {
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

pub(crate) fn raise_wrong_object_type(
    actual: &DbObjectTypeImpl,
    expected: &DbObjectTypeImpl,
) -> PyErr {
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

pub(crate) fn raise_dbobject_attr_max_size(
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

pub(crate) fn raise_dbobject_element_max_size(
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

pub(crate) fn raise_unsupported_python_type_for_db_type(
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

pub(crate) fn raise_unsupported_type_set(db_type_name: &str) -> PyErr {
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
