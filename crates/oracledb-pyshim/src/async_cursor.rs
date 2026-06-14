use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use asupersync::Cx;
use oracledb::protocol::thin::{
    BindValue, ColumnMetadata, ExecuteOptions, QueryResult, QueryValue,
};
use oracledb::Connection as RustConnection;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use crate::*;

pub(crate) struct AsyncExecuteOutcome {
    result: QueryResult,
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
    row_offset: usize,
    exec_options: ExecuteOptions,
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
                let is_plsql = statement_is_plsql(&statement);
                if is_plsql {
                    for row in bind_rows.iter_mut() {
                        materialize_plsql_long_binds_async(cx, connection, row, call_timeout)
                            .await?;
                    }
                }
                if bind_rows.iter().all(Vec::is_empty)
                    || bind_rows_need_iterative_plsql(&statement, &bind_rows)
                {
                    let mut result = QueryResult::default();
                    let mut out_values: BTreeMap<usize, Vec<Option<QueryValue>>> = BTreeMap::new();
                    let mut return_values: BTreeMap<usize, Vec<Option<QueryValue>>> =
                        BTreeMap::new();
                    for (row_index, row) in bind_rows.iter().enumerate() {
                        let map_row_err = |err: oracledb::Error| {
                            let err = TaskError::from(err);
                            if is_plsql {
                                err.with_plsql_row_offset(row_offset + row_index)
                            } else {
                                err
                            }
                        };
                        let row_result = if row.is_empty() {
                            connection
                                .execute_query_with_timeout(
                                    cx,
                                    &statement,
                                    prefetchrows,
                                    call_timeout,
                                )
                                .await
                                .map_err(map_row_err)?
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
                                .map_err(map_row_err)?
                        };
                        result.row_count = result.row_count.saturating_add(row_result.row_count);
                        result.compilation_error_warning |= row_result.compilation_error_warning;
                        result.last_rowid = row_result.last_rowid;
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
                    return Ok(AsyncExecuteOutcome { result });
                }
                let mut result = connection
                    .execute_query_with_bind_rows_options_and_timeout(
                        cx,
                        &statement,
                        prefetchrows,
                        &bind_rows,
                        exec_options,
                        call_timeout,
                    )
                    .await
                    .map_err(|err| {
                        let err = TaskError::from(err);
                        if is_plsql {
                            err.with_plsql_row_offset(row_offset)
                        } else {
                            err
                        }
                    })?;
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
                Ok(AsyncExecuteOutcome { result })
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
    exec_options: ExecuteOptions,
    prior_cursor_id: u32,
) -> BlockingTask<AsyncExecuteOutcome> {
    spawn_blocking_task("oracledb-pyshim-async-execute", move || {
        let mut guard = connection.lock().map_err(|err| err.to_string())?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| "connection is closed".to_string())?;
        // Release the cursor's previously held server cursor before the
        // statement-cache lookup, mirroring the reference `_prepare` which
        // returns the old statement (in_use=False) before getting the new one.
        // This lets a cursor re-executing the same SQL reuse its own server
        // cursor, while a *different* cursor running the same SQL concurrently
        // still sees it as in use and gets a fresh one (ORA-01002 avoidance).
        connection.release_cursor(prior_cursor_id);
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
            if statement_is_plsql(&statement) {
                materialize_plsql_long_binds_async(&cx, connection, &mut bind_values, call_timeout)
                    .await?;
            }
            let bind_rows = if bind_values.is_empty() {
                Vec::new()
            } else {
                vec![bind_values.clone()]
            };
            let mut result = connection
                .execute_query_with_bind_rows_options_and_timeout(
                    &cx,
                    &statement,
                    prefetchrows,
                    &bind_rows,
                    exec_options,
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
            Ok(AsyncExecuteOutcome { result })
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

pub(crate) fn spawn_async_scroll_task(
    connection: Arc<Mutex<Option<RustConnection>>>,
    statement: String,
    cursor_id: u32,
    arraysize: u32,
    fetch_orientation: u32,
    fetch_pos: u32,
) -> BlockingTask<QueryResult> {
    spawn_blocking_task("oracledb-pyshim-async-scroll", move || {
        let mut guard = connection.lock().map_err(|err| err.to_string())?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| "connection is closed".to_string())?;
        let runtime = build_pyshim_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| "asupersync did not install an ambient Cx".to_string())?;
            connection
                .scroll_cursor(
                    &cx,
                    &statement,
                    cursor_id,
                    arraysize,
                    fetch_orientation,
                    fetch_pos,
                )
                .await
                .map_err(TaskError::from)
        })
    })
}

#[pyclass(module = "oracledb.thin_impl", name = "AsyncThinCursorImpl")]
pub(crate) struct AsyncThinCursorImpl {
    pub(crate) inner: ThinCursorImpl,
}

impl AsyncThinCursorImpl {
    /// Drains the executed cursor into a single Arrow [`RecordBatch`], mirroring
    /// the sync `build_arrow_batch` but using the async fetch task bridge. The
    /// CLOB/BLOB re-define case fetches inline; remaining locators (fully
    /// prefetched) are materialized by the sync `inline_lob_cells`.
    async fn drain_arrow_batch(&mut self, cursor: Py<PyAny>) -> PyResult<arrow_array::RecordBatch> {
        let (arrow_columns, mut pending_define) =
            Python::attach(|py| self.inner.arrow_drain_plan(py, cursor.bind(py)))?;
        let mut all_rows: Vec<Vec<Option<QueryValue>>> = std::mem::take(&mut self.inner.rows);
        self.inner.row_index = 0;
        while self.inner.more_rows && self.inner.cursor_id != 0 {
            let previous_row = all_rows.last().cloned();
            let fetch = spawn_async_fetch_task(
                Arc::clone(&self.inner.connection),
                self.inner.cursor_id,
                self.inner.arraysize,
                self.inner.arraysize,
                arrow_columns.clone(),
                arrow_columns.clone(),
                previous_row,
                pending_define,
            );
            let result = fetch.await.map_err(runtime_error)?;
            pending_define = false;
            self.inner.more_rows = result.more_rows;
            if result.cursor_id != 0 {
                self.inner.cursor_id = result.cursor_id;
            }
            all_rows.extend(result.rows);
        }
        self.inner.requires_define = false;
        Python::attach(|py| {
            self.inner.inline_lob_cells(&arrow_columns, &mut all_rows)?;
            self.inner.finish_arrow_batch(py, &arrow_columns, &all_rows)
        })
    }
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
    fn fetching_arrow(&self) -> bool {
        self.inner.fetching_arrow
    }

    #[setter]
    fn set_fetching_arrow(&mut self, value: bool) {
        self.inner.fetching_arrow = value;
    }

    #[getter]
    fn schema_impl(&self, py: Python<'_>) -> Option<Py<ArrowSchemaImpl>> {
        self.inner
            .schema_impl
            .as_ref()
            .map(|value| value.clone_ref(py))
    }

    #[setter]
    fn set_schema_impl(&mut self, value: Option<Py<ArrowSchemaImpl>>) {
        self.inner.schema_impl = value;
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
        let statement = self
            .inner
            .statement
            .as_deref()
            .ok_or_else(|| PyRuntimeError::new_err("no statement prepared"))?
            .to_string();
        // only DML statements may use the batch errors or array DML row
        // counts flags (reference thin/cursor.pyx:418-422)
        if (batcherrors || arraydmlrowcounts) && !statement_is_dml(&statement) {
            return Err(raise_oracledb_driver_error("ERR_EXECUTE_MODE_ONLY_FOR_DML"));
        }
        let exec_options = ExecuteOptions {
            batcherrors,
            arraydmlrowcounts,
            cache_statement: self.inner.cache_statement,
            suspend_on_success: self.inner.suspend_on_success,
            ..ExecuteOptions::default()
        };
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
        let is_plsql_statement = statement_is_plsql(&statement);
        let query = spawn_async_executemany_task(
            Arc::clone(&self.inner.connection),
            Arc::clone(&self.inner.state),
            statement.clone(),
            bind_rows,
            typed_lob_hints,
            self.inner.prefetchrows,
            call_timeout,
            autocommit,
            start,
            exec_options,
        );
        let outcome = match query.await {
            Ok(outcome) => outcome,
            Err(_) if self.inner.cancel_requested.swap(false, Ordering::SeqCst) => {
                return Err(ora_cancel_error());
            }
            Err(err) => {
                self.inner
                    .record_executemany_error_modes(&err, batcherrors, arraydmlrowcounts);
                return Err(self
                    .inner
                    .raise_execute_task_error(&err, is_plsql_statement));
            }
        };
        if self.inner.cancel_requested.swap(false, Ordering::SeqCst) {
            self.inner.drain_cancel_response()?;
            return Err(ora_cancel_error());
        }
        let mut result = outcome.result;
        self.inner.batch_errors_state =
            batcherrors.then(|| std::mem::take(&mut result.batch_errors));
        if arraydmlrowcounts {
            self.inner.dml_row_counts =
                Some(result.array_dml_row_counts.take().unwrap_or_default());
        }
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
            .record_statement(&statement);
        self.inner.record_implicit_resultsets(&mut result);
        self.inner.columns = result.columns;
        self.inner.reset_fetch_define_state();
        self.inner.requires_define = columns_require_define(&self.inner.columns);
        let execute_returned_rows = !result.rows.is_empty();
        self.inner.rows = result.rows;
        self.inner.row_index = 0;
        self.inner.cursor_id = result.cursor_id;
        self.inner.more_rows = result.more_rows;
        // a re-executed open VECTOR cursor streams its rows in the execute
        // response (server prefetch suppressed via ExecuteOptions::no_prefetch);
        // the active server define is already satisfied, so clear requires_define
        // to avoid a define-fetch landing out of sequence (ORA-01002)
        if execute_returned_rows && self.inner.requires_define {
            self.inner.requires_define = false;
        }
        self.inner.invalid_ref_cursor = false;
        self.inner.last_rowid = result.last_rowid;
        self.inner.rowcount = if is_plsql_statement {
            0
        } else {
            // reference sets rowcount from the server error-info trailer
            // (messages/base.pyx:1188-1189), not the iteration count
            i64::try_from(result.row_count).unwrap_or(i64::MAX)
        };
        self.inner.is_query = is_query;
        if self.inner.is_query {
            Python::attach(|py| self.inner.prepare_fetch_defines(py, _cursor.bind(py)))?;
        }
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
        // reference resets _batcherrors via _process_error_info on every
        // execute round trip
        self.inner.batch_errors_state = None;
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
        let mut typed_lob_hints =
            Python::attach(|py| typed_lob_bind_hints(py, &self.inner.bind_vars));
        promote_oversized_plsql_bind_hints(
            &statement,
            &self.inner.bind_values,
            &mut typed_lob_hints,
        );
        let autocommit = *self.inner.autocommit.lock().map_err(runtime_error)?;
        // a scrollable cursor primes the open result set with orientation
        // CURRENT at the first row (reference `_create_execute_message`)
        let exec_options = if self.inner.scrollable {
            ExecuteOptions {
                cache_statement: self.inner.cache_statement,
                scrollable: true,
                fetch_orientation: oracledb::protocol::thin::TNS_FETCH_ORIENTATION_CURRENT,
                fetch_pos: u32::try_from(self.inner.rowcount.max(0) + 1).unwrap_or(u32::MAX),
                suspend_on_success: self.inner.suspend_on_success,
                ..ExecuteOptions::default()
            }
        } else {
            ExecuteOptions {
                cache_statement: self.inner.cache_statement,
                suspend_on_success: self.inner.suspend_on_success,
                ..ExecuteOptions::default()
            }
        };
        let prior_cursor_id = self.inner.cursor_id;
        let query = spawn_async_execute_task(
            Arc::clone(&self.inner.connection),
            Arc::clone(&self.inner.state),
            statement.clone(),
            self.inner.bind_values.clone(),
            typed_lob_hints,
            self.inner.prefetchrows,
            call_timeout,
            autocommit,
            exec_options,
            prior_cursor_id,
        );
        let is_plsql = statement_is_plsql(&statement);
        let outcome = match query.await {
            Ok(outcome) => outcome,
            Err(_) if self.inner.cancel_requested.swap(false, Ordering::SeqCst) => {
                return Err(ora_cancel_error());
            }
            Err(err) => return Err(self.inner.raise_execute_task_error(&err, is_plsql)),
        };
        let mut result = outcome.result;
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
            )?;
            reset_cursor_bind_vars(py, &self.inner.bind_values, &self.inner.bind_vars)
        })?;
        let is_query = !result.columns.is_empty();
        self.inner
            .state
            .lock()
            .map_err(runtime_error)?
            .record_statement(&statement);
        self.inner.record_implicit_resultsets(&mut result);
        self.inner.columns = result.columns;
        self.inner.reset_fetch_define_state();
        self.inner.requires_define = columns_require_define(&self.inner.columns);
        let execute_returned_rows = !result.rows.is_empty();
        self.inner.rows = result.rows;
        self.inner.row_index = 0;
        self.inner.cursor_id = result.cursor_id;
        self.inner.more_rows = result.more_rows;
        // a re-executed open VECTOR cursor streams its rows in the execute
        // response (server prefetch suppressed via ExecuteOptions::no_prefetch);
        // the active server define is already satisfied, so clear requires_define
        // to avoid a define-fetch landing out of sequence (ORA-01002)
        if execute_returned_rows && self.inner.requires_define {
            self.inner.requires_define = false;
        }
        self.inner.invalid_ref_cursor = false;
        self.inner.last_rowid = result.last_rowid;
        self.inner.rowcount = if is_query || is_plsql {
            0
        } else {
            i64::try_from(result.row_count).unwrap_or(i64::MAX)
        };
        // the freshly fetched buffer starts one past the consumed rowcount
        // (reference `_fetch_rows`: `_buffer_min_row = rowcount + 1`)
        self.inner.refresh_buffer_window();
        self.inner.is_query = is_query;
        if self.inner.is_query {
            Python::attach(|py| self.inner.prepare_fetch_defines(py, _cursor.bind(py)))?;
            // A query whose select list has a LOB/JSON/VECTOR column cannot be
            // prefetched inline, so the execute returns the describe with rows
            // deferred. The reference resends the execute with a define that
            // performs the fetch during execute, so a per-row compute error
            // (e.g. ORA-01476 from 1/0) surfaces on execute, not only on the
            // later fetch. Prime the define buffer eagerly here to match, using
            // the define columns established by prepare_fetch_defines (so the
            // output type handler is honored). Test 6348. The arrow / DataFrame
            // fetch path performs its own define-fetch with arrow columns, so
            // skip the eager prime there.
            if self.inner.requires_define && !self.inner.fetching_arrow {
                self.fetch_next_buffer().await?;
            }
        }
        Ok(())
    }

    fn is_query(&self, connection: &Bound<'_, PyAny>) -> bool {
        self.inner.is_query(connection)
    }

    /// Fetches the next batch of rows from the open cursor into the buffer,
    /// applying the active define (LOB/JSON/VECTOR re-define) when required.
    /// Used both by the row iterator and by `execute` to prime the buffer
    /// eagerly. Returns without doing anything when the buffer still has rows or
    /// the result set is exhausted.
    async fn fetch_next_buffer(&mut self) -> PyResult<()> {
        if self.inner.row_index < self.inner.rows.len()
            || !self.inner.more_rows
            || self.inner.cursor_id == 0
        {
            return Ok(());
        }
        let previous_row = self.inner.rows.last().cloned();
        let requires_define = self.inner.requires_define;
        let define_columns = self.inner.fetch_define_columns.clone();
        // a scrollable cursor re-executes the open cursor with orientation
        // CURRENT instead of issuing a plain fetch (reference `_fetch_rows`)
        let result = if self.inner.scrollable {
            let scroll = spawn_async_scroll_task(
                Arc::clone(&self.inner.connection),
                self.inner.statement.clone().unwrap_or_default(),
                self.inner.cursor_id,
                self.inner.arraysize,
                oracledb::protocol::thin::TNS_FETCH_ORIENTATION_CURRENT,
                u32::try_from(self.inner.rowcount.max(0) + 1).unwrap_or(u32::MAX),
            );
            scroll.await.map_err(runtime_error)?
        } else {
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
            fetch.await.map_err(runtime_error)?
        };
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
        self.inner.refresh_buffer_window();
        Ok(())
    }

    async fn fetch_next_row(&mut self, cursor: Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
        if self.inner.invalid_ref_cursor {
            return Err(raise_oracledb_driver_error("ERR_INVALID_REF_CURSOR"));
        }
        Python::attach(|py| self.inner.prepare_fetch_defines(py, cursor.bind(py)))?;
        self.fetch_next_buffer().await?;
        Python::attach(|py| {
            self.inner.fetch_async_lobs = true;
            let result = self.inner.fetch_buffered_next_row(py, cursor.bind(py));
            self.inner.fetch_async_lobs = false;
            result
        })
    }

    /// Fetches all remaining rows and returns a public `DataFrame` (reference
    /// async `fetch_df_all`).
    async fn fetch_df_all(&mut self, cursor: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let batch = self.drain_arrow_batch(cursor).await?;
        Python::attach(|py| Ok(dataframe_from_batch(py, batch)?.unbind()))
    }

    /// Returns an async iterator yielding the result set as `DataFrame` batches
    /// (reference async `fetch_df_batches`, consumed via `async for`). The whole
    /// result set is drained eagerly here (one logical fetch) and sliced; the
    /// returned iterator hands the batches out. This must be a *sync* method so
    /// the caller's `async for` sees an async iterator rather than a coroutine.
    fn fetch_df_batches(
        &mut self,
        py: Python<'_>,
        cursor: &Bound<'_, PyAny>,
        batch_size: i64,
    ) -> PyResult<AsyncDataFrameBatchIter> {
        let batch = self.inner.build_arrow_batch(py, cursor)?;
        let frames = slice_batch_into_frames(py, batch, batch_size)?;
        Ok(AsyncDataFrameBatchIter::new(frames))
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

    fn get_batch_errors(&self, py: Python<'_>) -> PyResult<Option<Vec<Py<PyAny>>>> {
        self.inner.get_batch_errors(py)
    }

    fn get_bind_names(&self) -> Vec<String> {
        self.inner.get_bind_names()
    }

    fn get_implicit_results(&mut self, connection: &Bound<'_, PyAny>) -> PyResult<Vec<Py<PyAny>>> {
        self.inner.get_implicit_results(connection)
    }

    fn get_lastrowid(&self) -> Option<String> {
        self.inner.get_lastrowid()
    }

    fn get_handle(&self) -> PyResult<Py<PyAny>> {
        self.inner.get_handle()
    }

    #[pyo3(signature = (external_handle_capsule=None))]
    fn attach_external_handle(
        &self,
        external_handle_capsule: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        self.inner.attach_external_handle(external_handle_capsule)
    }

    async fn scroll(&mut self, _cursor: Py<PyAny>, value: i32, mode: String) -> PyResult<()> {
        let Some((orientation, fetch_pos)) = self.inner.resolve_scroll_target(value, &mode)? else {
            // an in-buffer reposition was applied without contacting the server
            return Ok(());
        };
        let scroll = spawn_async_scroll_task(
            Arc::clone(&self.inner.connection),
            self.inner.statement.clone().unwrap_or_default(),
            self.inner.cursor_id,
            self.inner.arraysize,
            orientation,
            fetch_pos,
        );
        let result = scroll.await.map_err(runtime_error)?;
        self.inner.apply_scroll_result(orientation, result)
    }
}
