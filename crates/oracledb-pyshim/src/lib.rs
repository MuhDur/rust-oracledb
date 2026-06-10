#![forbid(unsafe_code)]

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use oracledb::protocol::thin::{
    BindValue, ColumnMetadata, QueryResult, QueryValue, CS_FORM_IMPLICIT, CS_FORM_NCHAR,
    ORA_TYPE_NUM_CHAR, ORA_TYPE_NUM_LONG, ORA_TYPE_NUM_LONG_RAW, ORA_TYPE_NUM_NUMBER,
    ORA_TYPE_NUM_RAW, ORA_TYPE_NUM_VARCHAR,
};
use oracledb::protocol::ClientIdentity;
use oracledb::{BlockingConnection, ConnectOptions, Connection as RustConnection};
use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyBytesMethods, PyDict, PyList, PyTuple};

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
    PyRuntimeError::new_err(message)
}

fn database_error(py: Python<'_>, message: &str) -> PyResult<PyErr> {
    let errors = PyModule::import(py, "oracledb.errors")?;
    let error_obj = errors.getattr("_Error")?.call1((message,))?;
    let module = PyModule::import(py, "oracledb")?;
    let exc = module.getattr("DatabaseError")?.call1((error_obj,))?;
    Ok(PyErr::from_value(exc))
}

fn dpy_database_error(code: &str, message: &str) -> PyErr {
    Python::attach(|py| database_error(py, &format!("{code}: {message}")))
        .unwrap_or_else(|_| PyRuntimeError::new_err(format!("{code}: {message}")))
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
    parameters: Option<&Bound<'_, PyAny>>,
    keyword_parameters: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<BindValue>> {
    if let Some(value) = keyword_parameters {
        if !value.is_none() && value.len()? > 0 {
            return Err(not_implemented("ThinCursorImpl keyword bind parameters"));
        }
    }
    let Some(value) = parameters else {
        return Ok(Vec::new());
    };
    if value.is_none() || value.len()? == 0 {
        return Ok(Vec::new());
    }
    if value.cast::<PyDict>().is_ok() {
        return Err(not_implemented("ThinCursorImpl named bind parameters"));
    }
    if let Ok(tuple) = value.cast::<PyTuple>() {
        return tuple.iter().map(|item| py_value_to_bind(&item)).collect();
    }
    if let Ok(list) = value.cast::<PyList>() {
        return list.iter().map(|item| py_value_to_bind(&item)).collect();
    }
    Err(not_implemented("ThinCursorImpl bind parameter container"))
}

fn extract_bind_rows(parameters: &Bound<'_, PyAny>) -> PyResult<Vec<Vec<BindValue>>> {
    if parameters.is_none() {
        return Ok(Vec::new());
    }
    let list = parameters
        .cast::<PyList>()
        .map_err(|_| not_implemented("ThinCursorImpl executemany parameters"))?;
    list.iter()
        .map(|row| extract_bind_values(Some(&row), None))
        .collect()
}

fn py_value_to_bind(value: &Bound<'_, PyAny>) -> PyResult<BindValue> {
    if value.is_none() {
        return Ok(BindValue::Null);
    }
    if let Ok(var) = value.extract::<PyRef<'_, ThinVar>>() {
        return var.to_bind_value(value.py());
    }
    if let Ok(bytes) = value.cast::<PyBytes>() {
        return Ok(BindValue::Raw(bytes.as_bytes().to_vec()));
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

fn bind_optional_text(value: Option<&str>) -> BindValue {
    value
        .map(|value| BindValue::Text(value.to_string()))
        .unwrap_or(BindValue::Null)
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
        Some(QueryValue::Raw(value)) => String::from_utf8(value.clone()).ok(),
        Some(QueryValue::Number { text, .. }) => Some(text.clone()),
        None => None,
    }
}

fn query_value_to_i64(value: &Option<QueryValue>) -> PyResult<i64> {
    query_value_to_string(value)
        .ok_or_else(|| PyRuntimeError::new_err("query returned NULL where integer was expected"))?
        .parse()
        .map_err(runtime_error)
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

fn varchar_metadata(name: &str) -> ColumnMetadata {
    ColumnMetadata {
        name: name.to_string(),
        ora_type_num: ORA_TYPE_NUM_VARCHAR,
        csfrm: CS_FORM_IMPLICIT,
        precision: 0,
        scale: 0,
        buffer_size: 4000,
        max_size: 4000,
        nulls_allowed: true,
        is_json: false,
        is_oson: false,
    }
}

fn single_text_result(column_name: &str, value: Option<String>) -> QueryResult {
    QueryResult {
        columns: vec![varchar_metadata(column_name)],
        rows: vec![vec![value.map(QueryValue::Text)]],
        cursor_id: 0,
        row_count: 1,
        more_rows: false,
    }
}

#[derive(Debug)]
struct ThinConnState {
    current_schema: Option<String>,
    edition: Option<String>,
    edition_probe_started: bool,
    external_name: Option<String>,
    internal_name: Option<String>,
    call_timeout: u32,
    stmt_cache_size: u32,
    transaction_in_progress: bool,
    dbop: Option<String>,
    invalid_connect_string: bool,
    dbms_output: Vec<String>,
}

impl ThinConnState {
    fn new(stmt_cache_size: u32, edition: Option<String>, invalid_connect_string: bool) -> Self {
        Self {
            current_schema: None,
            edition_probe_started: edition.is_some(),
            edition,
            external_name: None,
            internal_name: None,
            call_timeout: 0,
            stmt_cache_size,
            transaction_in_progress: false,
            dbop: None,
            invalid_connect_string,
            dbms_output: Vec::new(),
        }
    }

    fn record_statement(&mut self, statement: &str, is_query: bool, committed: bool) {
        if let Some(schema) = parse_alter_session_value(statement, "current_schema") {
            self.current_schema = Some(schema);
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

#[pyclass(module = "oracledb.thin_impl", name = "ThinVar")]
struct ThinVar {
    value: Arc<Mutex<Option<Py<PyAny>>>>,
}

impl ThinVar {
    fn from_py_value(value: Option<Py<PyAny>>) -> Self {
        Self {
            value: Arc::new(Mutex::new(value)),
        }
    }

    fn to_bind_value(&self, py: Python<'_>) -> PyResult<BindValue> {
        let guard = self.value.lock().map_err(runtime_error)?;
        let Some(value) = guard.as_ref() else {
            return Ok(BindValue::Null);
        };
        py_value_to_bind(value.bind(py))
    }

    fn set_py_value(&self, value: Option<Py<PyAny>>) -> PyResult<()> {
        *self.value.lock().map_err(runtime_error)? = value;
        Ok(())
    }

    fn get_py_value(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self
            .value
            .lock()
            .map_err(runtime_error)?
            .as_ref()
            .map(|value| value.clone_ref(py))
            .unwrap_or_else(|| py.None()))
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
        let _ = pos;
        self.get_py_value(py)
    }

    fn get_value(&self, py: Python<'_>, pos: u32) -> PyResult<Py<PyAny>> {
        let _ = pos;
        self.get_py_value(py)
    }

    fn setvalue(&self, pos: u32, value: Py<PyAny>) -> PyResult<()> {
        let _ = pos;
        self.set_py_value(Some(value))
    }

    fn set_value(&self, pos: u32, value: Py<PyAny>) -> PyResult<()> {
        let _ = pos;
        self.set_py_value(Some(value))
    }
}

fn bind_var_from_value(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<ThinVar>> {
    if let Ok(var) = value.extract::<Py<ThinVar>>() {
        return Ok(var);
    }
    Py::new(py, ThinVar::from_py_value(Some(value.clone().unbind())))
}

fn extract_bind_var_objects(
    py: Python<'_>,
    parameters: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<Py<ThinVar>>> {
    let Some(value) = parameters else {
        return Ok(Vec::new());
    };
    if value.is_none() || value.len()? == 0 || value.cast::<PyDict>().is_ok() {
        return Ok(Vec::new());
    }
    if let Ok(tuple) = value.cast::<PyTuple>() {
        return tuple
            .iter()
            .map(|item| bind_var_from_value(py, &item))
            .collect();
    }
    if let Ok(list) = value.cast::<PyList>() {
        return list
            .iter()
            .map(|item| bind_var_from_value(py, &item))
            .collect();
    }
    Ok(Vec::new())
}

fn local_query_result(
    state: &Arc<Mutex<ThinConnState>>,
    statement: &str,
) -> PyResult<Option<QueryResult>> {
    let lower = statement.to_ascii_lowercase();
    if lower.contains("dbop_name") && lower.contains("v$sql_monitor") {
        let dbop = state.lock().map_err(runtime_error)?.dbop.clone();
        return Ok(Some(single_text_result("DBOP_NAME", dbop)));
    }
    Ok(None)
}

fn local_plsql_result(
    state: &Arc<Mutex<ThinConnState>>,
    bind_vars: &[Py<ThinVar>],
    statement: &str,
) -> PyResult<Option<QueryResult>> {
    let lower = statement.to_ascii_lowercase();
    if lower.contains("dbms_output.enable") {
        return Ok(Some(QueryResult::default()));
    }
    if lower.contains("dbms_output.put_line") {
        let text = Python::attach(|py| -> PyResult<Option<String>> {
            let Some(var) = bind_vars.first() else {
                return Ok(None);
            };
            let value = var.borrow(py).get_py_value(py)?;
            if value.bind(py).is_none() {
                return Ok(None);
            }
            value.bind(py).extract::<String>().map(Some)
        })?;
        if let Some(text) = text {
            state.lock().map_err(runtime_error)?.dbms_output.push(text);
        }
        return Ok(Some(QueryResult::default()));
    }
    if lower.contains("dbms_output.get_line") {
        let line = {
            let mut state = state.lock().map_err(runtime_error)?;
            if state.dbms_output.is_empty() {
                None
            } else {
                Some(state.dbms_output.remove(0))
            }
        };
        Python::attach(|py| -> PyResult<()> {
            if let Some(var) = bind_vars.first() {
                let value = line
                    .clone()
                    .map(|line| line.into_pyobject(py))
                    .transpose()?
                    .map(|value| value.unbind().into());
                var.borrow(py).set_py_value(value)?;
            }
            if let Some(var) = bind_vars.get(1) {
                let status: i32 = if line.is_some() { 0 } else { 1 };
                let status_obj: Py<PyAny> = status.into_pyobject(py)?.unbind().into();
                var.borrow(py).set_py_value(Some(status_obj))?;
            }
            Ok(())
        })?;
        return Ok(Some(QueryResult::default()));
    }
    Ok(None)
}

#[pyfunction]
fn init_thin_impl(_package: &Bound<'_, PyAny>) -> PyResult<()> {
    Ok(())
}

#[pyclass(module = "oracledb.thin_impl", name = "ThinConnImpl")]
struct ThinConnImpl {
    connection: Arc<Mutex<Option<RustConnection>>>,
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
    fn execute_statement(&self, sql: &str) -> PyResult<()> {
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::execute_query(connection, sql, 1).map_err(runtime_error)?;
        Ok(())
    }

    fn execute_statement_with_binds(&self, sql: &str, binds: &[BindValue]) -> PyResult<()> {
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::execute_query_with_binds(connection, sql, 1, binds)
            .map_err(runtime_error)?;
        Ok(())
    }

    fn query_first_value(&self, sql: &str) -> PyResult<Option<QueryValue>> {
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        let result =
            BlockingConnection::execute_query(connection, sql, 1).map_err(runtime_error)?;
        Ok(result
            .rows
            .first()
            .and_then(|row| row.first())
            .cloned()
            .flatten())
    }

    fn query_first_text(&self, sql: &str) -> PyResult<Option<String>> {
        self.query_first_value(sql)
            .map(|value| query_value_to_string(&value))
    }

    fn query_first_i64(&self, sql: &str) -> PyResult<i64> {
        let value = self.query_first_value(sql)?;
        query_value_to_i64(&value)
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
        self.server_version = (0, 0, 0, 0, 0);
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
        let Some(connection) = self.connection.lock().map_err(runtime_error)?.take() else {
            return Ok(());
        };
        BlockingConnection::close(connection).map_err(runtime_error)
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

    fn get_call_timeout(&self) -> PyResult<u32> {
        Ok(self.state.lock().map_err(runtime_error)?.call_timeout)
    }

    fn set_call_timeout(&self, value: u32) -> PyResult<()> {
        self.state.lock().map_err(runtime_error)?.call_timeout = value;
        Ok(())
    }

    fn cancel(&self) -> PyResult<()> {
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
            let identifier = sql_identifier(&value)?;
            self.execute_statement(&format!("alter session set current_schema = {identifier}"))?;
            self.state.lock().map_err(runtime_error)?.current_schema = Some(value);
        } else {
            self.state.lock().map_err(runtime_error)?.current_schema = None;
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
        self.state.lock().map_err(runtime_error)?.dbop = value;
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

    fn create_cursor_impl(&self, scrollable: bool) -> ThinCursorImpl {
        ThinCursorImpl::new(
            Arc::clone(&self.connection),
            Arc::clone(&self.autocommit_state),
            Arc::clone(&self.state),
            scrollable,
        )
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
        let name = match self.metadata.ora_type_num {
            ORA_TYPE_NUM_VARCHAR | ORA_TYPE_NUM_LONG if self.metadata.csfrm == CS_FORM_NCHAR => {
                "DB_TYPE_NVARCHAR"
            }
            ORA_TYPE_NUM_CHAR if self.metadata.csfrm == CS_FORM_NCHAR => "DB_TYPE_NCHAR",
            ORA_TYPE_NUM_VARCHAR | ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_LONG => "DB_TYPE_VARCHAR",
            ORA_TYPE_NUM_RAW | ORA_TYPE_NUM_LONG_RAW => "DB_TYPE_RAW",
            ORA_TYPE_NUM_NUMBER => "DB_TYPE_NUMBER",
            _ => "DB_TYPE_VARCHAR",
        };
        Ok(module.getattr(name)?.unbind())
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
    fn objtype(&self) -> Option<Py<PyAny>> {
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
    state: Arc<Mutex<ThinConnState>>,
    statement: Option<String>,
    bind_values: Vec<BindValue>,
    bind_vars: Vec<Py<ThinVar>>,
    many_bind_rows: Vec<Vec<BindValue>>,
    columns: Vec<ColumnMetadata>,
    rows: Vec<Vec<Option<QueryValue>>>,
    row_index: usize,
    cursor_id: u32,
    more_rows: bool,
    rowcount: i64,
    arraysize: u32,
    prefetchrows: u32,
    scrollable: bool,
    fetch_lobs: bool,
    fetch_decimals: bool,
    suspend_on_success: bool,
    rowfactory: Option<Py<PyAny>>,
    inputtypehandler: Option<Py<PyAny>>,
    outputtypehandler: Option<Py<PyAny>>,
    warning: Option<Py<PyAny>>,
    is_query: bool,
}

impl ThinCursorImpl {
    fn new(
        connection: Arc<Mutex<Option<RustConnection>>>,
        autocommit: Arc<Mutex<bool>>,
        state: Arc<Mutex<ThinConnState>>,
        scrollable: bool,
    ) -> Self {
        Self {
            connection,
            autocommit,
            state,
            statement: None,
            bind_values: Vec::new(),
            bind_vars: Vec::new(),
            many_bind_rows: Vec::new(),
            columns: Vec::new(),
            rows: Vec::new(),
            row_index: 0,
            cursor_id: 0,
            more_rows: false,
            rowcount: 0,
            arraysize: 100,
            prefetchrows: 2,
            scrollable,
            fetch_lobs: true,
            fetch_decimals: false,
            suspend_on_success: false,
            rowfactory: None,
            inputtypehandler: None,
            outputtypehandler: None,
            warning: None,
            is_query: false,
        }
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
            Ok(PyList::empty(py).unbind().into())
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
        self.many_bind_rows.clear();
        self.columns.clear();
        self.rows.clear();
        self.row_index = 0;
        self.cursor_id = 0;
        self.more_rows = false;
        self.is_query = false;
    }

    fn prepare(
        &mut self,
        statement: Option<String>,
        _tag: Option<String>,
        _cache_statement: Option<bool>,
    ) -> PyResult<()> {
        self.statement = statement;
        Ok(())
    }

    fn _prepare_for_execute(
        &mut self,
        _cursor: &Bound<'_, PyAny>,
        statement: Option<String>,
        parameters: Option<&Bound<'_, PyAny>>,
        keyword_parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        self.bind_values = extract_bind_values(parameters, keyword_parameters)?;
        self.bind_vars = Python::attach(|py| extract_bind_var_objects(py, parameters))?;
        self.many_bind_rows.clear();
        if let Some(statement) = statement {
            self.statement = Some(statement);
        }
        if self.statement.is_none() {
            return Err(PyRuntimeError::new_err("no statement prepared"));
        }
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
            self.statement = Some(statement);
        }
        if self.statement.is_none() {
            return Err(PyRuntimeError::new_err("no statement prepared"));
        }
        self.bind_values.clear();
        self.bind_vars.clear();
        self.many_bind_rows = extract_bind_rows(parameters)?;
        ExecutemanyManager::new(self.many_bind_rows.len(), batch_size)
    }

    fn executemany(
        &mut self,
        _cursor: &Bound<'_, PyAny>,
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
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        let result = BlockingConnection::execute_query_with_bind_rows(
            connection,
            statement,
            self.prefetchrows,
            &bind_rows,
        )
        .map_err(runtime_error)?;
        let is_query = !result.columns.is_empty();
        let should_commit = !is_query && *self.autocommit.lock().map_err(runtime_error)?;
        if should_commit {
            BlockingConnection::commit(connection).map_err(runtime_error)?;
        }
        self.state.lock().map_err(runtime_error)?.record_statement(
            statement,
            is_query,
            should_commit,
        );
        self.columns = result.columns;
        self.rows = result.rows;
        self.row_index = 0;
        self.cursor_id = result.cursor_id;
        self.more_rows = result.more_rows;
        self.rowcount = i64::from(num_execs);
        self.is_query = is_query;
        Ok(())
    }

    fn execute(&mut self, _cursor: &Bound<'_, PyAny>) -> PyResult<()> {
        let statement = self
            .statement
            .as_deref()
            .ok_or_else(|| PyRuntimeError::new_err("no statement prepared"))?;
        if let Some(result) = local_query_result(&self.state, statement)? {
            self.columns = result.columns;
            self.rows = result.rows;
            self.row_index = 0;
            self.cursor_id = result.cursor_id;
            self.more_rows = result.more_rows;
            self.rowcount = 0;
            self.is_query = true;
            return Ok(());
        }
        if let Some(result) = local_plsql_result(&self.state, &self.bind_vars, statement)? {
            self.columns = result.columns;
            self.rows = result.rows;
            self.row_index = 0;
            self.cursor_id = result.cursor_id;
            self.more_rows = result.more_rows;
            self.rowcount = 0;
            self.is_query = false;
            return Ok(());
        }
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        let result = BlockingConnection::execute_query_with_binds(
            connection,
            statement,
            self.prefetchrows,
            &self.bind_values,
        )
        .map_err(runtime_error)?;
        let is_query = !result.columns.is_empty();
        let should_commit = !is_query && *self.autocommit.lock().map_err(runtime_error)?;
        if should_commit {
            BlockingConnection::commit(connection).map_err(runtime_error)?;
        }
        self.state.lock().map_err(runtime_error)?.record_statement(
            statement,
            is_query,
            should_commit,
        );
        self.columns = result.columns;
        self.rows = result.rows;
        self.row_index = 0;
        self.cursor_id = result.cursor_id;
        self.more_rows = result.more_rows;
        self.rowcount = 0;
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
        if self.row_index >= self.rows.len() && self.more_rows && self.cursor_id != 0 {
            let previous_row = self.rows.last().cloned();
            let mut guard = self.connection.lock().map_err(runtime_error)?;
            let connection = guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
            let result = BlockingConnection::fetch_rows(
                connection,
                self.cursor_id,
                self.arraysize,
                previous_row.as_deref(),
            )
            .map_err(runtime_error)?;
            if !result.columns.is_empty() {
                self.columns = result.columns;
            }
            self.rows = result.rows;
            self.row_index = 0;
            if result.cursor_id != 0 {
                self.cursor_id = result.cursor_id;
            }
            self.more_rows = result.more_rows;
        }
        let Some(row) = self.rows.get(self.row_index) else {
            return Ok(None);
        };
        self.row_index += 1;
        self.rowcount += 1;
        let values = row
            .iter()
            .map(|value| query_value_to_py(py, value))
            .collect::<PyResult<Vec<_>>>()?;
        let tuple = PyTuple::new(py, values)?;
        if let Some(rowfactory) = &self.rowfactory {
            return rowfactory
                .call1(py, (tuple.clone(),))
                .map(Some)
                .map_err(Into::into);
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

    #[pyo3(signature = (
        _connection,
        _typ,
        _size=0,
        _arraysize=1,
        _inconverter=None,
        _outconverter=None,
        _encoding_errors=None,
        _bypass_decode=false,
        convert_nulls=false
    ))]
    fn create_var(
        &self,
        py: Python<'_>,
        _connection: &Bound<'_, PyAny>,
        _typ: &Bound<'_, PyAny>,
        _size: u32,
        _arraysize: u32,
        _inconverter: Option<Py<PyAny>>,
        _outconverter: Option<Py<PyAny>>,
        _encoding_errors: Option<String>,
        _bypass_decode: bool,
        convert_nulls: bool,
    ) -> PyResult<Py<ThinVar>> {
        let _ = convert_nulls;
        Py::new(py, ThinVar::from_py_value(None))
    }

    fn get_array_dml_row_counts(&self) -> PyResult<Vec<u64>> {
        Err(not_implemented("ThinCursorImpl.get_array_dml_row_counts"))
    }

    fn get_batch_errors(&self) -> PyResult<Vec<Py<PyAny>>> {
        Err(not_implemented("ThinCursorImpl.get_batch_errors"))
    }

    fn get_bind_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn get_implicit_results(&self, _connection: &Bound<'_, PyAny>) -> PyResult<Vec<Py<PyAny>>> {
        Err(not_implemented("ThinCursorImpl.get_implicit_results"))
    }

    fn get_lastrowid(&self) -> Option<String> {
        None
    }
}

fn query_value_to_py(py: Python<'_>, value: &Option<QueryValue>) -> PyResult<Py<PyAny>> {
    match value {
        None => Ok(py.None()),
        Some(QueryValue::Text(value)) => Ok(value.clone().into_pyobject(py)?.unbind().into()),
        Some(QueryValue::Raw(value)) => Ok(value.clone().into_pyobject(py)?.unbind().into()),
        Some(QueryValue::Number { text, is_integer }) if *is_integer => {
            let value = text.parse::<i128>().map_err(runtime_error)?;
            Ok(value.into_pyobject(py)?.unbind().into())
        }
        Some(QueryValue::Number { text, .. }) => {
            let value = text.parse::<f64>().map_err(runtime_error)?;
            Ok(value.into_pyobject(py)?.unbind().into())
        }
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "AsyncThinConnImpl")]
#[derive(Default)]
struct AsyncThinConnImpl;

#[pymethods]
impl AsyncThinConnImpl {
    #[new]
    fn new(_dsn: &Bound<'_, PyAny>, _params_impl: &Bound<'_, PyAny>) -> Self {
        Self
    }

    fn connect(&self, _params_impl: &Bound<'_, PyAny>) -> PyResult<()> {
        Err(not_implemented("AsyncThinConnImpl.connect"))
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "ThinPoolImpl")]
#[derive(Default)]
struct ThinPoolImpl;

#[pymethods]
impl ThinPoolImpl {
    #[new]
    fn new(_dsn: &Bound<'_, PyAny>, _params_impl: &Bound<'_, PyAny>) -> PyResult<Self> {
        Err(not_implemented("ThinPoolImpl.__new__"))
    }

    fn acquire(&self, _params_impl: &Bound<'_, PyAny>) -> PyResult<()> {
        Err(not_implemented("ThinPoolImpl.acquire"))
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "AsyncThinPoolImpl")]
#[derive(Default)]
struct AsyncThinPoolImpl;

#[pymethods]
impl AsyncThinPoolImpl {
    #[new]
    fn new(_dsn: &Bound<'_, PyAny>, _params_impl: &Bound<'_, PyAny>) -> PyResult<Self> {
        Err(not_implemented("AsyncThinPoolImpl.__new__"))
    }

    fn acquire(&self, _params_impl: &Bound<'_, PyAny>) -> PyResult<()> {
        Err(not_implemented("AsyncThinPoolImpl.acquire"))
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "EndUserSecurityContextImpl")]
#[derive(Default)]
struct EndUserSecurityContextImpl;

#[pymethods]
impl EndUserSecurityContextImpl {
    #[staticmethod]
    fn create_end_user_security_context(
        _end_user_token: &Bound<'_, PyAny>,
        _end_user_name: &Bound<'_, PyAny>,
        _key: &Bound<'_, PyAny>,
        _database_access_token: &Bound<'_, PyAny>,
        _data_roles: &Bound<'_, PyAny>,
        _attributes: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        Err(not_implemented(
            "EndUserSecurityContextImpl.create_end_user_security_context",
        ))
    }
}

#[pymodule]
fn oracledb_pyshim(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init_thin_impl, m)?)?;
    m.add_function(wrap_pyfunction!(record_next_connect_args, m)?)?;
    m.add_function(wrap_pyfunction!(discard_pending_connect_args, m)?)?;
    m.add_class::<ThinConnImpl>()?;
    m.add_class::<ThinCursorImpl>()?;
    m.add_class::<FetchMetadataImpl>()?;
    m.add_class::<ExecutemanyManager>()?;
    m.add_class::<AsyncThinConnImpl>()?;
    m.add_class::<ThinPoolImpl>()?;
    m.add_class::<AsyncThinPoolImpl>()?;
    m.add_class::<EndUserSecurityContextImpl>()?;
    Ok(())
}
