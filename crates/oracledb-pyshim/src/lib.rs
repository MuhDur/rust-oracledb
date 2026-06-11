#![forbid(unsafe_code)]

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

mod errors;
mod async_bridge;
mod hooks;
mod pyutil;
mod binds;
mod convert;
mod lob;
mod var;
mod typehandler;
mod dbobject;
mod metadata;
mod conn;
mod cursor;

pub(crate) use errors::*;
pub(crate) use async_bridge::*;
pub(crate) use hooks::*;
pub(crate) use pyutil::*;
pub(crate) use binds::*;
pub(crate) use convert::*;
pub(crate) use lob::*;
pub(crate) use var::*;
pub(crate) use typehandler::*;
pub(crate) use dbobject::*;
pub(crate) use metadata::*;
pub(crate) use conn::*;
pub(crate) use cursor::*;

#[pyfunction]
fn init_thin_impl(_package: &Bound<'_, PyAny>) -> PyResult<()> {
    Ok(())
}

struct AsyncExecuteOutcome {
    result: QueryResult,
    should_commit: bool,
}

fn spawn_async_executemany_task(
    connection: Arc<Mutex<Option<RustConnection>>>,
    state: Arc<Mutex<ThinConnState>>,
    statement: String,
    mut bind_rows: Vec<Vec<BindValue>>,
    typed_lob_hints: Vec<Option<(u8, u8)>>,
    prefetchrows: u32,
    call_timeout: Option<u32>,
    autocommit: bool,
) -> BlockingTask<AsyncExecuteOutcome> {
    spawn_async_connection_task(
        "oracledb-pyshim-async-executemany",
        connection,
        move |cx, connection| {
            Box::pin(async move {
                apply_pending_current_schema_from_state_async(cx, &state, connection, call_timeout)
                    .await?;
                materialize_typed_lob_bind_rows_async(
                    cx,
                    connection,
                    &mut bind_rows,
                    &typed_lob_hints,
                    call_timeout,
                )
                .await?;
                if bind_rows.iter().all(Vec::is_empty)
                    || bind_rows_need_iterative_plsql(&statement, &bind_rows)
                {
                    let mut result = QueryResult::default();
                    let mut out_values: BTreeMap<usize, Vec<Option<QueryValue>>> = BTreeMap::new();
                    let mut return_values: BTreeMap<usize, Vec<Option<QueryValue>>> =
                        BTreeMap::new();
                    for row in &bind_rows {
                        let row_result = if row.is_empty() {
                            connection
                                .execute_query_with_timeout(
                                    cx,
                                    &statement,
                                    prefetchrows,
                                    call_timeout,
                                )
                                .await
                                .map_err(TaskError::from)?
                        } else {
                            let one_row = vec![row.clone()];
                            connection
                                .execute_query_with_bind_rows_and_timeout(
                                    cx,
                                    &statement,
                                    prefetchrows,
                                    &one_row,
                                    call_timeout,
                                )
                                .await
                                .map_err(TaskError::from)?
                        };
                        result.row_count = result.row_count.saturating_add(row_result.row_count);
                        result.compilation_error_warning |= row_result.compilation_error_warning;
                        for (index, value) in row_result.out_values {
                            out_values.entry(index).or_default().push(value);
                        }
                        for (index, values) in row_result.return_values {
                            return_values.entry(index).or_default().extend(values);
                        }
                    }
                    result.out_values = out_values
                        .into_iter()
                        .map(|(index, values)| (index, Some(QueryValue::Array(values))))
                        .collect();
                    result.return_values = return_values.into_iter().collect();
                    let should_commit = result.columns.is_empty() && autocommit;
                    if should_commit {
                        connection.commit(cx).await.map_err(TaskError::from)?;
                    }
                    return Ok(AsyncExecuteOutcome {
                        result,
                        should_commit,
                    });
                }
                let mut result = connection
                    .execute_query_with_bind_rows_and_timeout(
                        cx,
                        &statement,
                        prefetchrows,
                        &bind_rows,
                        call_timeout,
                    )
                    .await
                    .map_err(TaskError::from)?;
                supplement_json_lob_column_metadata_async(
                    cx,
                    connection,
                    &mut result.columns,
                    call_timeout,
                )
                .await?;
                let should_commit = result.columns.is_empty() && autocommit;
                if should_commit {
                    connection.commit(cx).await.map_err(TaskError::from)?;
                }
                Ok(AsyncExecuteOutcome {
                    result,
                    should_commit,
                })
            })
        },
    )
}

fn spawn_async_execute_task(
    connection: Arc<Mutex<Option<RustConnection>>>,
    state: Arc<Mutex<ThinConnState>>,
    statement: String,
    mut bind_values: Vec<BindValue>,
    typed_lob_hints: Vec<Option<(u8, u8)>>,
    prefetchrows: u32,
    call_timeout: Option<u32>,
    autocommit: bool,
) -> BlockingTask<AsyncExecuteOutcome> {
    spawn_blocking_task("oracledb-pyshim-async-execute", move || {
        let mut guard = connection.lock().map_err(|err| err.to_string())?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| "connection is closed".to_string())?;
        let runtime = build_pyshim_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| "asupersync did not install an ambient Cx".to_string())?;
            apply_pending_current_schema_from_state_async(&cx, &state, connection, call_timeout)
                .await?;
            materialize_typed_lob_bind_values_async(
                &cx,
                connection,
                &mut bind_values,
                &typed_lob_hints,
                call_timeout,
            )
            .await?;
            let mut result = connection
                .execute_query_with_binds_and_timeout(
                    &cx,
                    &statement,
                    prefetchrows,
                    &bind_values,
                    call_timeout,
                )
                .await
                .map_err(TaskError::from)?;
            supplement_json_lob_column_metadata_async(
                &cx,
                connection,
                &mut result.columns,
                call_timeout,
            )
            .await?;
            let should_commit = result.columns.is_empty() && autocommit;
            if should_commit {
                connection.commit(&cx).await.map_err(TaskError::from)?;
            }
            Ok(AsyncExecuteOutcome {
                result,
                should_commit,
            })
        })
    })
}

fn spawn_async_fetch_task(
    connection: Arc<Mutex<Option<RustConnection>>>,
    cursor_id: u32,
    arraysize: u32,
    prefetchrows: u32,
    columns: Vec<ColumnMetadata>,
    define_columns: Vec<ColumnMetadata>,
    previous_row: Option<Vec<Option<QueryValue>>>,
    requires_define: bool,
) -> BlockingTask<QueryResult> {
    spawn_blocking_task("oracledb-pyshim-async-fetch", move || {
        let mut guard = connection.lock().map_err(|err| err.to_string())?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| "connection is closed".to_string())?;
        let runtime = build_pyshim_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| "asupersync did not install an ambient Cx".to_string())?;
            if requires_define {
                connection
                    .define_and_fetch_rows_with_columns(
                        &cx,
                        cursor_id,
                        prefetchrows,
                        &define_columns,
                        previous_row.as_deref(),
                    )
                    .await
                    .map_err(TaskError::from)
            } else {
                connection
                    .fetch_rows_with_columns(
                        &cx,
                        cursor_id,
                        arraysize,
                        &columns,
                        previous_row.as_deref(),
                    )
                    .await
                    .map_err(TaskError::from)
            }
        })
    })
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
        _cursor: Py<PyAny>,
        num_execs: u32,
        batcherrors: bool,
        arraydmlrowcounts: bool,
        offset: u32,
    ) -> PyResult<()> {
        if batcherrors {
            return Err(not_implemented(
                "AsyncThinCursorImpl executemany batcherrors",
            ));
        }
        if arraydmlrowcounts {
            return Err(not_implemented(
                "AsyncThinCursorImpl executemany array DML rowcounts",
            ));
        }
        let statement = self
            .inner
            .statement
            .as_deref()
            .ok_or_else(|| PyRuntimeError::new_err("no statement prepared"))?
            .to_string();
        let start = usize::try_from(offset).map_err(runtime_error)?;
        let count = usize::try_from(num_execs).map_err(runtime_error)?;
        let end = start
            .checked_add(count)
            .ok_or_else(|| PyRuntimeError::new_err("executemany offset overflow"))?;
        let bind_rows = self
            .inner
            .many_bind_rows
            .get(start..end)
            .ok_or_else(|| PyRuntimeError::new_err("executemany batch is out of range"))?
            .to_vec();
        let typed_lob_hints = Python::attach(|py| typed_lob_bind_hints(py, &self.inner.bind_vars));
        let call_timeout = {
            let value = self.inner.state.lock().map_err(runtime_error)?.call_timeout;
            (value > 0).then_some(value)
        };
        let autocommit = *self.inner.autocommit.lock().map_err(runtime_error)?;
        let query = spawn_async_executemany_task(
            Arc::clone(&self.inner.connection),
            Arc::clone(&self.inner.state),
            statement.clone(),
            bind_rows,
            typed_lob_hints,
            self.inner.prefetchrows,
            call_timeout,
            autocommit,
        );
        let outcome = match query.await {
            Ok(outcome) => outcome,
            Err(_) if self.inner.cancel_requested.swap(false, Ordering::SeqCst) => {
                return Err(ora_cancel_error());
            }
            Err(err) => {
                if let Some(row_count) = err.server_row_count() {
                    self.inner.rowcount = i64::try_from(row_count).unwrap_or(i64::MAX);
                }
                return Err(runtime_error(err));
            }
        };
        if self.inner.cancel_requested.swap(false, Ordering::SeqCst) {
            self.inner.drain_cancel_response()?;
            return Err(ora_cancel_error());
        }
        let result = outcome.result;
        let should_commit = outcome.should_commit;
        let is_query = !result.columns.is_empty();
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
        self.inner.rowcount = if statement_is_plsql(&statement) {
            0
        } else {
            i64::from(num_execs)
        };
        self.inner.is_query = is_query;
        Ok(())
    }

    async fn execute(&mut self, _cursor: Py<PyAny>) -> PyResult<()> {
        if self.inner.statement_changed {
            self.inner.rowfactory = None;
        }
        if !self.inner.fetch_lobs_overridden {
            self.inner.fetch_lobs = Python::attach(default_fetch_lobs)?;
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
        let typed_lob_hints = Python::attach(|py| typed_lob_bind_hints(py, &self.inner.bind_vars));
        let autocommit = *self.inner.autocommit.lock().map_err(runtime_error)?;
        let query = spawn_async_execute_task(
            Arc::clone(&self.inner.connection),
            Arc::clone(&self.inner.state),
            statement.clone(),
            self.inner.bind_values.clone(),
            typed_lob_hints,
            self.inner.prefetchrows,
            call_timeout,
            autocommit,
        );
        let outcome = match query.await {
            Ok(outcome) => outcome,
            Err(_) if self.inner.cancel_requested.swap(false, Ordering::SeqCst) => {
                return Err(ora_cancel_error());
            }
            Err(err) => return Err(runtime_error(err)),
        };
        let result = outcome.result;
        let should_commit = outcome.should_commit;
        if self.inner.cancel_requested.swap(false, Ordering::SeqCst) {
            self.inner.drain_cancel_response()?;
            return Err(ora_cancel_error());
        }
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
        if self.inner.invalid_ref_cursor {
            return Err(raise_oracledb_driver_error("ERR_INVALID_REF_CURSOR"));
        }
        Python::attach(|py| self.inner.prepare_fetch_defines(py, cursor.bind(py)))?;
        if self.inner.row_index >= self.inner.rows.len()
            && self.inner.more_rows
            && self.inner.cursor_id != 0
        {
            let previous_row = self.inner.rows.last().cloned();
            let requires_define = self.inner.requires_define;
            let define_columns = self.inner.fetch_define_columns.clone();
            let fetch = spawn_async_fetch_task(
                Arc::clone(&self.inner.connection),
                self.inner.cursor_id,
                self.inner.arraysize,
                self.inner.prefetchrows,
                self.inner.columns.clone(),
                define_columns.clone(),
                previous_row,
                requires_define,
            );
            let result = fetch.await.map_err(runtime_error)?;
            if !result.columns.is_empty() {
                self.inner.columns = result.columns;
            } else if requires_define {
                self.inner.columns = define_columns;
            }
            self.inner.rows = result.rows;
            self.inner.row_index = 0;
            if result.cursor_id != 0 {
                self.inner.cursor_id = result.cursor_id;
            }
            self.inner.more_rows = result.more_rows;
            if requires_define {
                self.inner.requires_define = false;
            }
            self.inner.invalid_ref_cursor = false;
        }
        Python::attach(|py| {
            self.inner.fetch_async_lobs = true;
            let result = self.inner.fetch_buffered_next_row(py, cursor.bind(py));
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
        let prepared = Python::attach(|py| self.inner.prepare_connect(params_impl.bind(py)))?;
        let connection = spawn_async_connect_task(prepared.options)
            .await
            .map_err(runtime_error)?;
        let cancel_handle = connection.cancel_handle().map_err(runtime_error)?;
        self.inner.server_version = (0, 0, 0, 0, 0);
        *self.inner.cancel_handle.lock().map_err(runtime_error)? = Some(cancel_handle);
        *self.inner.connection.lock().map_err(runtime_error)? = Some(connection);
        if let Some(new_password) = prepared.new_password {
            self.change_password(prepared.password, new_password)
                .await?;
        }
        if let Some(edition) = prepared.edition {
            let identifier = sql_identifier(&edition)?;
            let sql = format!("alter session set edition = {identifier}");
            let call_timeout = self.inner.call_timeout()?;
            let task = spawn_async_connection_task(
                "oracledb-pyshim-async-set-edition",
                Arc::clone(&self.inner.connection),
                move |cx, connection| {
                    Box::pin(async move {
                        connection
                            .execute_query_with_timeout(cx, &sql, 1, call_timeout)
                            .await
                            .map(|_| ())
                            .map_err(TaskError::from)
                    })
                },
            );
            task.await.map_err(runtime_error)?;
            let mut state = self.inner.state.lock().map_err(runtime_error)?;
            state.edition = Some(edition);
            state.edition_probe_started = true;
        }
        Ok(())
    }

    #[pyo3(signature = (in_del=None))]
    async fn close(&self, in_del: Option<bool>) -> PyResult<()> {
        let _ = in_del;
        let Some(connection) = self.inner.take_connection_for_close()? else {
            return Ok(());
        };
        let close = spawn_async_close_task(connection);
        close_result_to_py(close.await)
    }

    async fn ping(&self) -> PyResult<()> {
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-ping",
            Arc::clone(&self.inner.connection),
            |cx, connection| {
                Box::pin(async move { connection.ping(cx).await.map_err(TaskError::from) })
            },
        );
        task.await.map_err(runtime_error)
    }

    async fn commit(&self) -> PyResult<()> {
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-commit",
            Arc::clone(&self.inner.connection),
            |cx, connection| {
                Box::pin(async move { connection.commit(cx).await.map_err(TaskError::from) })
            },
        );
        task.await.map_err(runtime_error)?;
        self.inner
            .state
            .lock()
            .map_err(runtime_error)?
            .transaction_in_progress = false;
        Ok(())
    }

    async fn rollback(&self) -> PyResult<()> {
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-rollback",
            Arc::clone(&self.inner.connection),
            |cx, connection| {
                Box::pin(async move { connection.rollback(cx).await.map_err(TaskError::from) })
            },
        );
        task.await.map_err(runtime_error)?;
        self.inner
            .state
            .lock()
            .map_err(runtime_error)?
            .transaction_in_progress = false;
        Ok(())
    }

    async fn change_password(&self, old_password: String, new_password: String) -> PyResult<()> {
        if new_password.len() > 1024 {
            return Err(dpy_database_error(
                "ORA-00988",
                "missing or invalid password(s)",
            ));
        }
        let user = user_identifier(&self.inner.username)?;
        let sql = format!(
            "alter user {user} identified by {} replace {}",
            quoted_oracle_string(&new_password),
            quoted_oracle_string(&old_password)
        );
        let call_timeout = {
            let value = self.inner.state.lock().map_err(runtime_error)?.call_timeout;
            (value > 0).then_some(value)
        };
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-change-password",
            Arc::clone(&self.inner.connection),
            move |cx, connection| {
                Box::pin(async move {
                    connection
                        .execute_query_with_timeout(cx, &sql, 1, call_timeout)
                        .await
                        .map(|_| ())
                        .map_err(TaskError::from)
                })
            },
        );
        task.await
            .map_err(runtime_error)
            .and_then(|()| set_password_override_for_user(&self.inner.username, &new_password))
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
        let (ora_type_num, csfrm) =
            Python::attach(|py| match py_type_name(lob_type.bind(py)).as_str() {
                "DB_TYPE_BLOB" => (ORA_TYPE_NUM_BLOB, 0),
                "DB_TYPE_NCLOB" => (ORA_TYPE_NUM_CLOB, CS_FORM_NCHAR),
                _ => (ORA_TYPE_NUM_CLOB, CS_FORM_IMPLICIT),
            });
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-create-temp-lob",
            Arc::clone(&self.inner.connection),
            move |cx, connection| {
                Box::pin(async move {
                    connection
                        .create_temp_lob(cx, ora_type_num, csfrm)
                        .await
                        .map_err(TaskError::from)
                })
            },
        );
        let result = task.await.map_err(runtime_error)?;
        Python::attach(|py| {
            Py::new(
                py,
                AsyncThinLob {
                    inner: ThinLob {
                        data: None,
                        locator: Arc::new(Mutex::new(Some(result.locator))),
                        ora_type_num,
                        csfrm,
                        size: 0,
                        chunk_size: 0,
                        context: Some(ThinLobContext {
                            connection: Arc::clone(&self.inner.connection),
                            state: Arc::clone(&self.inner.state),
                            async_mode: true,
                        }),
                        is_open: Arc::new(Mutex::new(false)),
                        bfile_name: None,
                    },
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
