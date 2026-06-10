#![forbid(unsafe_code)]

use std::sync::{Arc, Mutex};

use oracledb::protocol::thin::{
    BindValue, ColumnMetadata, QueryValue, CS_FORM_NCHAR, ORA_TYPE_NUM_CHAR, ORA_TYPE_NUM_LONG,
    ORA_TYPE_NUM_LONG_RAW, ORA_TYPE_NUM_NUMBER, ORA_TYPE_NUM_RAW, ORA_TYPE_NUM_VARCHAR,
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

fn env_password_for_user(user: &str) -> PyResult<String> {
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

#[pyfunction]
fn init_thin_impl(_package: &Bound<'_, PyAny>) -> PyResult<()> {
    Ok(())
}

#[pyclass(module = "oracledb.thin_impl", name = "ThinConnImpl")]
struct ThinConnImpl {
    connection: Arc<Mutex<Option<RustConnection>>>,
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
        let username = get_string_attr(params_impl, "user")?;
        Ok(Self {
            connection: Arc::new(Mutex::new(None)),
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
        let program = get_string_attr(params_impl, "program")?;
        let machine = get_string_attr(params_impl, "machine")?;
        let terminal = get_string_attr(params_impl, "terminal")?;
        let osuser = get_string_attr(params_impl, "osuser")?;
        let driver_name = get_optional_string_attr(params_impl, "driver_name")?
            .unwrap_or_else(|| "rust-oracledb thn : 0.0.0".into());
        let password = env_password_for_user(&self.username)?;
        let app_context = get_app_context_attr(params_impl)?;
        let identity = ClientIdentity::new(program, machine, osuser, terminal, driver_name)
            .map_err(runtime_error)?;
        let options =
            ConnectOptions::new(self.dsn.clone(), self.username.clone(), password, identity)
                .with_app_context(app_context);
        let connection = BlockingConnection::connect(options).map_err(runtime_error)?;
        self.server_version = (0, 0, 0, 0, 0);
        *self.connection.lock().map_err(runtime_error)? = Some(connection);
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
        BlockingConnection::commit(connection).map_err(runtime_error)
    }

    fn rollback(&self) -> PyResult<()> {
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::rollback(connection).map_err(runtime_error)
    }

    fn get_is_healthy(&self) -> PyResult<bool> {
        Ok(self.connection.lock().map_err(runtime_error)?.is_some())
    }

    fn get_sdu(&self) -> u32 {
        8192
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
    statement: Option<String>,
    bind_values: Vec<BindValue>,
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
        scrollable: bool,
    ) -> Self {
        Self {
            connection,
            autocommit,
            statement: None,
            bind_values: Vec::new(),
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

    fn get_bind_vars(&self, py: Python<'_>) -> Py<PyAny> {
        py.None()
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
