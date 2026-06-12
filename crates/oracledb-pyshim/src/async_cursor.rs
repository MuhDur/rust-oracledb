use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use asupersync::Cx;
use oracledb::protocol::thin::{BindValue, ColumnMetadata, QueryResult, QueryValue};
use oracledb::Connection as RustConnection;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use crate::*;

pub(crate) struct AsyncExecuteOutcome {
    result: QueryResult,
    should_commit: bool,
}

// d49: migrate to oracledb (driver async futures)
#[allow(clippy::too_many_arguments)] // pre-existing lint at pre-split HEAD 978491a; not movement-induced
pub(crate) fn spawn_async_executemany_task(
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

// d49: migrate to oracledb (driver async futures)
#[allow(clippy::too_many_arguments)] // pre-existing lint at pre-split HEAD 978491a; not movement-induced
pub(crate) fn spawn_async_execute_task(
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

// d49: migrate to oracledb (driver async futures)
#[allow(clippy::too_many_arguments)] // pre-existing lint at pre-split HEAD 978491a; not movement-induced
pub(crate) fn spawn_async_fetch_task(
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
pub(crate) struct AsyncThinCursorImpl {
    pub(crate) inner: ThinCursorImpl,
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
        self.inner.fetch_decimals_overridden = true;
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
        if !self.inner.fetch_decimals_overridden {
            self.inner.fetch_decimals = Python::attach(default_fetch_decimals)?;
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
    #[allow(clippy::too_many_arguments)] // pre-existing lint at pre-split HEAD 978491a; not movement-induced
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
