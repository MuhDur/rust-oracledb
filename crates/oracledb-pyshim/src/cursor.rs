use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use oracledb::protocol::thin::{
    check_fetch_conversion, public_dbtype_name_from_column_metadata, BatchServerError, BindValue,
    ColumnMetadata, ExecuteOptions, QueryResult, QueryValue, CS_FORM_IMPLICIT,
    ORA_TYPE_NUM_VARCHAR,
};
use oracledb::{BlockingConnection, Connection as RustConnection};
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::*;

#[pyclass(module = "oracledb.thin_impl", name = "ExecutemanyManager")]
// d49: migrate to oracledb (executemany manager belongs on driver)
pub(crate) struct ExecutemanyManager {
    total_rows: u32,
    batch_size: u32,
    num_rows: u32,
    message_offset: u32,
}

impl ExecutemanyManager {
    fn new(total_rows: usize, batch_size: u32) -> PyResult<Self> {
        let total_rows = u32::try_from(total_rows).map_err(runtime_error)?;
        if batch_size == 0 {
            return Err(PyTypeError::new_err("batch_size must be greater than zero"));
        }
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
pub(crate) struct ThinCursorImpl {
    pub(crate) connection: Arc<Mutex<Option<RustConnection>>>,
    pub(crate) autocommit: Arc<Mutex<bool>>,
    pub(crate) cancel_requested: Arc<AtomicBool>,
    pub(crate) state: Arc<Mutex<ThinConnState>>,
    pub(crate) statement: Option<String>,
    pub(crate) bind_values: Vec<BindValue>,
    pub(crate) bind_vars: Vec<Py<ThinVar>>,
    bind_names: Vec<String>,
    pub(crate) many_bind_rows: Vec<Vec<BindValue>>,
    pub(crate) columns: Vec<ColumnMetadata>,
    fetch_vars: Vec<Option<Py<ThinVar>>>,
    fetch_value_vars: Vec<Option<Py<ThinVar>>>,
    pub(crate) fetch_define_columns: Vec<ColumnMetadata>,
    pub(crate) requires_define: bool,
    pub(crate) rows: Vec<Vec<Option<QueryValue>>>,
    pub(crate) row_index: usize,
    pub(crate) cursor_id: u32,
    pub(crate) more_rows: bool,
    pub(crate) invalid_ref_cursor: bool,
    pub(crate) rowcount: i64,
    pub(crate) arraysize: u32,
    pub(crate) prefetchrows: u32,
    pub(crate) scrollable: bool,
    pub(crate) fetch_lobs: bool,
    pub(crate) fetch_lobs_overridden: bool,
    pub(crate) fetch_async_lobs: bool,
    pub(crate) fetch_decimals: bool,
    pub(crate) fetch_decimals_overridden: bool,
    pub(crate) suspend_on_success: bool,
    pub(crate) rowfactory: Option<Py<PyAny>>,
    pub(crate) inputtypehandler: Option<Py<PyAny>>,
    pub(crate) outputtypehandler: Option<Py<PyAny>>,
    pub(crate) warning: Option<Py<PyAny>>,
    has_positional_input_sizes: bool,
    has_named_input_sizes: bool,
    named_input_sizes: Vec<(String, Py<PyAny>)>,
    input_size_bind_surface: Option<Py<PyAny>>,
    pub(crate) statement_changed: bool,
    pub(crate) is_query: bool,
    pub(crate) last_rowid: Option<String>,
    /// `Some` after `executemany(batcherrors=True)`; `None` otherwise
    /// (reference `_batcherrors`).
    pub(crate) batch_errors_state: Option<Vec<BatchServerError>>,
    /// `Some` after `executemany(arraydmlrowcounts=True)` (reference
    /// `_dmlrowcounts`).
    pub(crate) dml_row_counts: Option<Vec<u64>>,
    /// `QueryValue::Cursor` entries from `dbms_sql.return_result`
    /// (reference `_implicit_resultsets`; `None` until a statement returns
    /// implicit results).
    pub(crate) implicit_resultsets: Option<Vec<QueryValue>>,
    /// Lazily-built public cursors for `getimplicitresults()`.
    implicit_result_cursors: Option<Vec<Py<PyAny>>>,
    /// Whether the prepared statement may use the connection statement
    /// cache (reference `cursor.prepare(cache_statement=...)`).
    pub(crate) cache_statement: bool,
}

impl ThinCursorImpl {
    pub(crate) fn drain_cancel_response(&self) -> PyResult<()> {
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::drain_cancel_response(connection).map_err(runtime_error)
    }

    pub(crate) fn new(
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
            fetch_value_vars: Vec::new(),
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
            fetch_decimals_overridden: false,
            suspend_on_success: false,
            rowfactory: None,
            inputtypehandler: None,
            outputtypehandler: None,
            warning: None,
            has_positional_input_sizes: false,
            has_named_input_sizes: false,
            named_input_sizes: Vec::new(),
            input_size_bind_surface: None,
            statement_changed: false,
            is_query: false,
            last_rowid: None,
            batch_errors_state: None,
            dml_row_counts: None,
            implicit_resultsets: None,
            implicit_result_cursors: None,
            cache_statement: true,
        }
    }

    /// Adopts implicit resultsets from an execute response (reference
    /// `_process_implicit_result`).
    pub(crate) fn record_implicit_resultsets(&mut self, result: &mut QueryResult) {
        if let Some(resultsets) = result.implicit_resultsets.take() {
            self.implicit_resultsets = Some(resultsets);
            self.implicit_result_cursors = None;
        }
    }

    /// Mirrors reference `_process_error_info` mode bookkeeping when an
    /// executemany fails outright: batcherrors yields an empty list and the
    /// DML row counts gathered before the error are preserved.
    pub(crate) fn record_executemany_error_modes(
        &mut self,
        err: &TaskError,
        batcherrors: bool,
        arraydmlrowcounts: bool,
    ) {
        self.batch_errors_state = batcherrors.then(Vec::new);
        if arraydmlrowcounts {
            let counts = err
                .server_error_details()
                .and_then(|details| details.array_dml_row_counts.clone())
                .unwrap_or_default();
            self.dml_row_counts = Some(counts);
        }
    }

    /// Applies structured server-error side effects (rowcount, lastrowid,
    /// dead-session disconnect) before raising, mirroring the reference
    /// `_process_error_info`/`_check_and_raise_exception` pair.
    pub(crate) fn raise_execute_task_error(&mut self, err: &TaskError, is_plsql: bool) -> PyErr {
        if let Some(details) = err.server_error_details() {
            if !is_plsql {
                self.rowcount = i64::try_from(details.row_count).unwrap_or(i64::MAX);
            }
            self.last_rowid = details.rowid.clone();
        }
        raise_task_error(err, &self.connection)
    }

    pub(crate) fn reset_fetch_define_state(&mut self) {
        self.fetch_vars.clear();
        self.fetch_value_vars.clear();
        self.fetch_define_columns.clear();
        self.requires_define = false;
    }

    pub(crate) fn clear_input_sizes_state(&mut self) {
        self.has_positional_input_sizes = false;
        self.has_named_input_sizes = false;
        self.named_input_sizes.clear();
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

    /// Maintains per-column fetch vars holding the most recently fetched
    /// values so `cursor.fetchvars` exposes a Var per column (reference
    /// creates fetch var impls for every fetched column,
    /// impl/base/cursor.pyx `_create_fetch_var`).
    fn record_fetch_value_vars(&mut self, py: Python<'_>, values: &[Py<PyAny>]) -> PyResult<()> {
        if self.fetch_value_vars.len() < values.len() {
            self.fetch_value_vars
                .resize_with(values.len(), Default::default);
        }
        for (index, value) in values.iter().enumerate() {
            if let Some(Some(var)) = self.fetch_value_vars.get(index) {
                var.borrow(py)
                    .set_bind_py_value(py, Some(value.clone_ref(py)))?;
                continue;
            }
            let dbtype_name = self
                .columns
                .get(index)
                .map(public_dbtype_name_from_column_metadata)
                .unwrap_or("DB_TYPE_VARCHAR");
            let var = Py::new(py, ThinVar::for_fetch_value(dbtype_name))?;
            var.borrow(py)
                .set_bind_py_value(py, Some(value.clone_ref(py)))?;
            self.fetch_value_vars[index] = Some(var);
        }
        Ok(())
    }

    /// Invokes the output type handler (if any) and prepares the fetch
    /// defines. Mirrors the reference `_create_fetch_var` protocol
    /// (reference impl/base/cursor.pyx:146-240, 300-324, 484-494): the
    /// handler runs during execute response processing, receives the public
    /// cursor plus a `FetchInfo` (or the legacy 6-argument form), and any
    /// returned variable is validated (DPY-2015/DPY-2016) and checked for
    /// fetch-conversion legality (DPY-4007).
    pub(crate) fn prepare_fetch_defines(
        &mut self,
        py: Python<'_>,
        cursor: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
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
        let uses_metadata = handler_uses_metadata(py, handler);
        // a proxy stands in for the public cursor because the real cursor's
        // attribute access would re-borrow this (mutably borrowed) impl
        let handler_cursor = Py::new(
            py,
            FetchHandlerCursor {
                connection: cursor.getattr("connection")?.unbind(),
                arraysize: self.arraysize,
            },
        )?;
        let handler_cursor = handler_cursor.bind(py);
        let mut define_changed = false;
        for (index, metadata) in self.columns.iter().enumerate() {
            let impl_metadata = Py::new(
                py,
                FetchMetadataImpl {
                    metadata: metadata.clone(),
                },
            )?;
            let impl_metadata = impl_metadata.bind(py);
            let value = if uses_metadata {
                let fetch_info = PyModule::import(py, "oracledb.fetch_info")?
                    .getattr("FetchInfo")?
                    .call_method1("_from_impl", (impl_metadata,))?;
                handler.call1((handler_cursor, fetch_info))?
            } else {
                // legacy 6-argument handler signature
                // (cursor, name, default_type, size, precision, scale)
                handler.call1((
                    handler_cursor,
                    impl_metadata.getattr("name")?,
                    impl_metadata.getattr("dbtype")?,
                    impl_metadata.getattr("max_size")?,
                    impl_metadata.getattr("precision")?,
                    impl_metadata.getattr("scale")?,
                ))?
            };
            if value.is_none() {
                continue;
            }
            let Some(var) = thin_var_from_value(&value)? else {
                return Err(raise_oracledb_driver_error("ERR_EXPECTING_VAR"));
            };
            {
                let var_ref = var.borrow(py);
                if self.arraysize > var_ref.num_elements_value() {
                    return Err(raise_incorrect_var_arraysize(
                        var_ref.num_elements_value(),
                        self.arraysize,
                    ));
                }
                let fetch_dbtype = public_dbtype_name_from_column_metadata(metadata);
                if var_ref.dbtype_name != fetch_dbtype {
                    let (to_ora_type_num, to_csfrm, _) = bind_type_info(&var_ref.default_bind)
                        .unwrap_or((ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT, 1));
                    let Some(define_metadata) =
                        check_fetch_conversion(metadata, to_ora_type_num, to_csfrm)
                    else {
                        return Err(raise_inconsistent_datatypes(
                            fetch_dbtype,
                            &var_ref.dbtype_name,
                        ));
                    };
                    if !define_metadata.eq(metadata) {
                        self.requires_define = true;
                        define_changed = true;
                    }
                    self.fetch_define_columns[index] = define_metadata;
                }
            }
            self.fetch_vars[index] = Some(var);
        }
        // the reference discards rows prefetched with the previous defines
        // and re-executes when an output type handler changes the define
        // types (statement._requires_define)
        if define_changed && !self.rows.is_empty() && self.cursor_id != 0 {
            self.rows.clear();
            self.row_index = 0;
            self.more_rows = true;
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
    pub(crate) fn fetch_vars_attr(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.is_query {
            let count = self.fetch_vars.len().max(self.fetch_value_vars.len());
            let values = (0..count)
                .map(|index| {
                    self.fetch_vars
                        .get(index)
                        .and_then(|var| var.as_ref())
                        .or_else(|| {
                            self.fetch_value_vars
                                .get(index)
                                .and_then(|var| var.as_ref())
                        })
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
    pub(crate) fn fetch_metadata(&self) -> Vec<FetchMetadataImpl> {
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
        self.fetch_decimals_overridden = true;
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
    pub(crate) fn close(&mut self, in_del: Option<bool>) {
        let _ = in_del;
        self.statement = None;
        self.bind_values.clear();
        self.bind_vars.clear();
        self.bind_names.clear();
        self.named_input_sizes.clear();
        self.input_size_bind_surface = None;
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

    pub(crate) fn prepare(
        &mut self,
        statement: Option<String>,
        _tag: Option<String>,
        cache_statement: Option<bool>,
    ) -> PyResult<()> {
        self.statement_changed = self.statement != statement;
        self.statement = statement;
        self.cache_statement = cache_statement.unwrap_or(true);
        self.bind_names = if let Some(statement) = self.statement.as_deref() {
            validate_dml_returning_duplicate_binds(statement)?;
            unique_sql_bind_names(statement)?
        } else {
            Vec::new()
        };
        Ok(())
    }

    pub(crate) fn parse(&mut self, cursor: &Bound<'_, PyAny>) -> PyResult<()> {
        let statement = self
            .statement
            .as_deref()
            .ok_or_else(|| raise_oracledb_driver_error("ERR_NO_STATEMENT"))?;
        #[allow(clippy::needless_borrow)]
        // pre-existing lint at pre-split HEAD 978491a; not movement-induced
        validate_dml_returning_duplicate_binds(&statement)?;
        self.bind_names = unique_sql_bind_names(statement)?;
        validate_parse_bind_names(statement)?;
        // reference sends a parse-only ExecuteMessage so queries are
        // described and the cursor exposes fetch metadata
        // (thin/cursor.pyx:324-330, execute.pyx:89-92)
        let is_plsql = statement_is_plsql(statement);
        let call_timeout = {
            let value = self.state.lock().map_err(runtime_error)?.call_timeout;
            (value > 0).then_some(value)
        };
        let result = match cursor.py().detach({
            let connection = Arc::clone(&self.connection);
            let statement = statement.to_string();
            move || -> Result<QueryResult, TaskError> {
                let mut guard = connection
                    .lock()
                    .map_err(|err| TaskError::from(err.to_string()))?;
                let connection = guard
                    .as_mut()
                    .ok_or_else(|| TaskError::from("connection is closed"))?;
                BlockingConnection::execute_query_with_bind_rows_options_and_timeout(
                    connection,
                    &statement,
                    1,
                    &[],
                    ExecuteOptions {
                        parse_only: true,
                        ..ExecuteOptions::default()
                    },
                    call_timeout,
                )
                .map_err(TaskError::from)
            }
        }) {
            Ok(result) => result,
            Err(err) => return Err(self.raise_execute_task_error(&err, is_plsql)),
        };
        if !result.columns.is_empty() {
            self.columns = result.columns;
            self.reset_fetch_define_state();
            self.requires_define = columns_require_define(&self.columns);
            self.rows.clear();
            self.row_index = 0;
            self.more_rows = false;
            self.is_query = true;
        }
        Ok(())
    }

    pub(crate) fn _prepare_for_execute(
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
        // Reference resets fetch options from oracledb.defaults on every
        // prepare (impl/base/cursor.pyx:420-421); per-call overrides are
        // applied by cursor.py after prepare, before execute.
        self.fetch_lobs = default_fetch_lobs(_cursor.py())?;
        self.fetch_decimals = default_fetch_decimals(_cursor.py())?;
        let statement = self
            .statement
            .as_deref()
            .ok_or_else(|| raise_oracledb_driver_error("ERR_NO_STATEMENT"))?;
        let statement = statement.to_string();
        validate_dml_returning_duplicate_binds(&statement)?;
        if self.has_positional_input_sizes
            && parameters.is_some_and(|value| value.cast::<PyDict>().is_ok())
        {
            // Reference clears the input-size state when this error fires so
            // a subsequent execute succeeds (impl/base/cursor.pyx:400-417).
            self.clear_input_sizes_state();
            return Err(raise_oracledb_driver_error(
                "ERR_MIXED_POSITIONAL_AND_NAMED_BINDS",
            ));
        }
        if self.has_named_input_sizes
            && parameters.is_some_and(|value| {
                !value.is_none() && value.len().unwrap_or(0) > 0 && value.cast::<PyDict>().is_err()
            })
        {
            self.clear_input_sizes_state();
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
            // input type handler runs before any other bind processing
            // (reference impl/base/cursor.pyx bind_one: num_elements is 1
            // for single-row execution)
            let input_type_handler =
                active_input_type_handler(py, _cursor, self.inputtypehandler.as_ref())?;
            let (handled_parameters, handled_keyword_parameters) = if let Some(handler) =
                &input_type_handler
            {
                let handler = handler.bind(py);
                (
                    apply_input_type_handler(py, _cursor, handler, self.arraysize, parameters, 1)?,
                    apply_input_type_handler(
                        py,
                        _cursor,
                        handler,
                        self.arraysize,
                        keyword_parameters,
                        1,
                    )?,
                )
            } else {
                (None, None)
            };
            let parameters = handled_parameters.as_ref().or(parameters);
            let keyword_parameters = handled_keyword_parameters.as_ref().or(keyword_parameters);
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

    pub(crate) fn _prepare_for_executemany(
        &mut self,
        _cursor: &Bound<'_, PyAny>,
        statement: Option<String>,
        parameters: &Bound<'_, PyAny>,
        batch_size: u32,
    ) -> PyResult<ExecutemanyManager> {
        let statement_supplied = statement.is_some();
        if let Some(statement) = statement {
            self.statement_changed = self.statement.as_ref() != Some(&statement);
            self.statement = Some(statement);
        } else {
            self.statement_changed = false;
        }
        self.warning = None;
        self.fetch_lobs = default_fetch_lobs(_cursor.py())?;
        self.fetch_decimals = default_fetch_decimals(_cursor.py())?;
        if self.statement.is_none() {
            return Err(raise_oracledb_driver_error("ERR_NO_STATEMENT"));
        }
        // executemany(None, N) re-uses the previous call's bind rows/vars
        // (reference PrePopulatedBatchLoadManager,
        // impl/base/batch_load_manager.pyx:358-389) and raises DPY-2016 when
        // fewer rows were previously bound than iterations requested.
        if !statement_supplied && !self.many_bind_rows.is_empty() {
            if let Ok(num_iters) = parameters.extract::<usize>() {
                let statement = self
                    .statement
                    .as_deref()
                    .ok_or_else(|| raise_oracledb_driver_error("ERR_NO_STATEMENT"))?;
                if !unique_sql_bind_names(statement)?.is_empty()
                    && self.named_input_sizes.is_empty()
                {
                    if num_iters > self.many_bind_rows.len() {
                        return Err(raise_incorrect_var_arraysize(
                            u32::try_from(self.many_bind_rows.len()).unwrap_or(u32::MAX),
                            u32::try_from(num_iters).unwrap_or(u32::MAX),
                        ));
                    }
                    return ExecutemanyManager::new(num_iters, batch_size);
                }
            }
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
            parameters,
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
        let statement = self
            .statement
            .as_deref()
            .ok_or_else(|| PyRuntimeError::new_err("no statement prepared"))?;
        // only DML statements may use the batch errors or array DML row
        // counts flags (reference thin/cursor.pyx:302-305)
        if (batcherrors || arraydmlrowcounts) && !statement_is_dml(statement) {
            return Err(raise_oracledb_driver_error("ERR_EXECUTE_MODE_ONLY_FOR_DML"));
        }
        let exec_options = ExecuteOptions {
            batcherrors,
            arraydmlrowcounts,
            cache_statement: self.cache_statement,
            ..ExecuteOptions::default()
        };
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
        let is_plsql_statement = statement_is_plsql(statement);
        let mut result = match cursor.py().detach({
            let connection = Arc::clone(&self.connection);
            let state = Arc::clone(&self.state);
            let statement = statement.to_string();
            let mut bind_rows = bind_rows.clone();
            let typed_lob_hints = typed_lob_hints.clone();
            let prefetchrows = self.prefetchrows;
            move || -> Result<QueryResult, TaskError> {
                let mut guard = connection
                    .lock()
                    .map_err(|err| TaskError::from(err.to_string()))?;
                let connection = guard
                    .as_mut()
                    .ok_or_else(|| TaskError::from("connection is closed"))?;
                apply_pending_current_schema_from_state(&state, connection, call_timeout)
                    .map_err(|err| TaskError::from(err.to_string()))?;
                materialize_typed_lob_bind_rows(
                    connection,
                    &mut bind_rows,
                    &typed_lob_hints,
                    call_timeout,
                )?;
                let is_plsql = statement_is_plsql(&statement);
                if is_plsql {
                    for row in bind_rows.iter_mut() {
                        materialize_plsql_long_binds(connection, row, call_timeout)?;
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
                                err.with_plsql_row_offset(start + row_index)
                            } else {
                                err
                            }
                        };
                        let row_result = if row.is_empty() {
                            BlockingConnection::execute_query_with_timeout(
                                connection,
                                &statement,
                                prefetchrows,
                                call_timeout,
                            )
                            .map_err(map_row_err)?
                        } else {
                            let one_row = vec![row.clone()];
                            BlockingConnection::execute_query_with_bind_rows_and_timeout(
                                connection,
                                &statement,
                                prefetchrows,
                                &one_row,
                                call_timeout,
                            )
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
                    return Ok(result);
                }
                BlockingConnection::execute_query_with_bind_rows_options_and_timeout(
                    connection,
                    &statement,
                    prefetchrows,
                    &bind_rows,
                    exec_options,
                    call_timeout,
                )
                .map_err(|err| {
                    let err = TaskError::from(err);
                    if is_plsql {
                        err.with_plsql_row_offset(start)
                    } else {
                        err
                    }
                })
            }
        }) {
            Ok(result) => result,
            Err(_) if self.cancel_requested.swap(false, Ordering::SeqCst) => {
                return Err(ora_cancel_error());
            }
            Err(err) => {
                self.record_executemany_error_modes(&err, batcherrors, arraydmlrowcounts);
                return Err(self.raise_execute_task_error(&err, is_plsql_statement));
            }
        };
        self.batch_errors_state = batcherrors.then(|| std::mem::take(&mut result.batch_errors));
        if arraydmlrowcounts {
            self.dml_row_counts = Some(result.array_dml_row_counts.take().unwrap_or_default());
        }
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
        self.record_implicit_resultsets(&mut result);
        self.columns = result.columns;
        self.reset_fetch_define_state();
        self.requires_define = columns_require_define(&self.columns);
        self.rows = result.rows;
        self.row_index = 0;
        self.cursor_id = result.cursor_id;
        self.more_rows = result.more_rows;
        self.invalid_ref_cursor = false;
        self.last_rowid = result.last_rowid;
        self.rowcount = if is_plsql_statement {
            0
        } else {
            // reference sets rowcount from the server error-info trailer
            // (messages/base.pyx:1188-1189), not the iteration count
            i64::try_from(result.row_count).unwrap_or(i64::MAX)
        };
        self.is_query = is_query;
        if self.is_query {
            self.prepare_fetch_defines(cursor.py(), cursor)?;
        }
        Ok(())
    }

    fn execute(&mut self, cursor: &Bound<'_, PyAny>) -> PyResult<()> {
        if self.statement_changed {
            self.rowfactory = None;
        }
        if !self.fetch_lobs_overridden {
            self.fetch_lobs = default_fetch_lobs(cursor.py())?;
        }
        if !self.fetch_decimals_overridden {
            self.fetch_decimals = default_fetch_decimals(cursor.py())?;
        }
        // reference resets _batcherrors via _process_error_info on every
        // execute round trip
        self.batch_errors_state = None;
        let statement = self
            .statement
            .as_deref()
            .ok_or_else(|| PyRuntimeError::new_err("no statement prepared"))?;
        let call_timeout = {
            let value = self.state.lock().map_err(runtime_error)?.call_timeout;
            (value > 0).then_some(value)
        };
        let mut typed_lob_hints = typed_lob_bind_hints(cursor.py(), &self.bind_vars);
        promote_oversized_plsql_bind_hints(statement, &self.bind_values, &mut typed_lob_hints);
        let is_plsql = statement_is_plsql(statement);
        let exec_options = ExecuteOptions {
            cache_statement: self.cache_statement,
            ..ExecuteOptions::default()
        };
        let mut result = match cursor.py().detach({
            let connection = Arc::clone(&self.connection);
            let state = Arc::clone(&self.state);
            let statement = statement.to_string();
            let mut bind_values = self.bind_values.clone();
            let typed_lob_hints = typed_lob_hints.clone();
            let prefetchrows = self.prefetchrows;
            move || -> Result<QueryResult, TaskError> {
                let mut guard = connection
                    .lock()
                    .map_err(|err| TaskError::from(err.to_string()))?;
                let connection = guard
                    .as_mut()
                    .ok_or_else(|| TaskError::from("connection is closed"))?;
                apply_pending_current_schema_from_state(&state, connection, call_timeout)
                    .map_err(|err| TaskError::from(err.to_string()))?;
                materialize_typed_lob_bind_values(
                    connection,
                    &mut bind_values,
                    &typed_lob_hints,
                    call_timeout,
                )?;
                if statement_is_plsql(&statement) {
                    materialize_plsql_long_binds(connection, &mut bind_values, call_timeout)?;
                }
                let bind_rows = if bind_values.is_empty() {
                    Vec::new()
                } else {
                    vec![bind_values.clone()]
                };
                BlockingConnection::execute_query_with_bind_rows_options_and_timeout(
                    connection,
                    &statement,
                    prefetchrows,
                    &bind_rows,
                    exec_options,
                    call_timeout,
                )
                .map_err(TaskError::from)
            }
        }) {
            Ok(result) => result,
            Err(_) if self.cancel_requested.swap(false, Ordering::SeqCst) => {
                return Err(ora_cancel_error());
            }
            Err(err) => return Err(self.raise_execute_task_error(&err, is_plsql)),
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
        self.record_implicit_resultsets(&mut result);
        self.columns = result.columns;
        self.reset_fetch_define_state();
        self.requires_define = columns_require_define(&self.columns);
        self.rows = result.rows;
        self.row_index = 0;
        self.cursor_id = result.cursor_id;
        self.more_rows = result.more_rows;
        self.invalid_ref_cursor = false;
        self.last_rowid = result.last_rowid;
        self.rowcount = if is_query || is_plsql {
            0
        } else {
            i64::try_from(result.row_count).unwrap_or(i64::MAX)
        };
        self.is_query = is_query;
        if self.is_query {
            // output type handlers run during execute in the reference
            // implementation (DPY-2015/2016/4007 surface from execute and
            // cursor.fetchvars is populated immediately afterwards)
            self.prepare_fetch_defines(cursor.py(), cursor)?;
        }
        Ok(())
    }

    pub(crate) fn is_query(&self, _connection: &Bound<'_, PyAny>) -> bool {
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
        self.fetch_buffered_next_row(py, _cursor)
    }

    pub(crate) fn fetch_buffered_next_row(
        &mut self,
        py: Python<'_>,
        _cursor: &Bound<'_, PyAny>,
    ) -> PyResult<Option<Py<PyAny>>> {
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
                // Reference: NUMBER columns convert to decimal.Decimal when
                // fetch_decimals is in effect (impl/base/cursor.pyx:211-214).
                if self.fetch_decimals
                    && self.columns.get(index).is_some_and(|metadata| {
                        metadata.ora_type_num == oracledb::protocol::thin::ORA_TYPE_NUM_NUMBER
                    })
                {
                    if let Some(QueryValue::Number { text, .. }) = value {
                        return python_decimal_from_text(py, text);
                    }
                }
                query_value_to_py(
                    py,
                    value,
                    Some(_cursor),
                    Some(&lob_context),
                    self.fetch_lobs,
                    self.fetch_decimals,
                )
            })
            .collect::<PyResult<Vec<_>>>()?;
        self.record_fetch_value_vars(py, &values)?;
        let tuple = PyTuple::new(py, values)?;
        if let Some(rowfactory) = &self.rowfactory {
            #[allow(clippy::useless_conversion)]
            // pre-existing lint at pre-split HEAD 978491a; not movement-induced
            return rowfactory.call1(py, tuple).map(Some).map_err(Into::into);
        }
        Ok(Some(tuple.unbind().into()))
    }

    #[pyo3(name = "get_fetch_vars")]
    fn get_fetch_vars_method(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.fetch_vars_attr(py)
    }

    #[getter(bind_vars)]
    pub(crate) fn bind_vars_attr(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // After setinputsizes() and before any bind, the surface created by
        // setinputsizes (list with None placeholders or dict by name) is the
        // bindvars value (impl/base/cursor.pyx get_bind_vars).
        if self.bind_vars.is_empty() {
            if let Some(surface) = &self.input_size_bind_surface {
                return Ok(surface.clone_ref(py));
            }
        }
        let values = self
            .bind_vars
            .iter()
            .map(|value| value.clone_ref(py))
            .collect::<Vec<_>>();
        Ok(PyList::new(py, values)?.unbind().into())
    }

    pub(crate) fn get_bind_vars(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.bind_vars_attr(py)
    }

    pub(crate) fn setinputsizes(
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
        // None entries stay as placeholders (the bind value determines the
        // type later) but are still part of the bindvars surface
        // (impl/base/cursor.pyx setinputsizes + get_bind_vars).
        if has_kwargs {
            let kwargs = kwargs.expect("has_kwargs implies kwargs is present");
            let result = PyDict::new(py);
            for (key, value) in kwargs.iter() {
                let key = key.extract::<String>()?;
                if value.is_none() {
                    result.set_item(key, py.None())?;
                    continue;
                }
                let var = thin_var_from_input_size(py, connection, &value)?;
                self.named_input_sizes
                    .push((key.clone(), var.clone_ref(py).into_any()));
                result.set_item(key, py_public_var_from_impl(py, &var)?)?;
            }
            let result: Py<PyAny> = result.unbind().into();
            self.input_size_bind_surface = Some(result.clone_ref(py));
            return Ok(result);
        }
        let result = PyList::empty(py);
        for (index, value) in args.iter().enumerate() {
            if value.is_none() {
                result.append(py.None())?;
                // a None placeholder still occupies a bind position so the
                // reference DPY-4009 count includes it (impl/thin/var.pyx
                // :101-106); lookups treat the None value as absent
                self.named_input_sizes
                    .push(((index + 1).to_string(), py.None()));
                continue;
            }
            let var = thin_var_from_input_size(py, connection, &value)?;
            result.append(py_public_var_from_impl(py, &var)?)?;
            self.named_input_sizes
                .push(((index + 1).to_string(), var.into_any()));
        }
        let result: Py<PyAny> = result.unbind().into();
        self.input_size_bind_surface = Some(result.clone_ref(py));
        Ok(result)
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
    pub(crate) fn create_var(
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
        thin_var_from_type_spec(
            py,
            connection,
            typ,
            size,
            is_array,
            num_elements,
            inconverter,
            outconverter,
            encoding_errors,
            convert_nulls,
            bypass_decode,
        )
    }

    pub(crate) fn get_array_dml_row_counts(&self) -> PyResult<Vec<u64>> {
        // reference thin/cursor.pyx get_array_dml_row_counts: DPY-4006 when
        // the last executemany did not enable arraydmlrowcounts
        self.dml_row_counts
            .clone()
            .ok_or_else(|| raise_oracledb_driver_error("ERR_ARRAY_DML_ROW_COUNTS_NOT_ENABLED"))
    }

    pub(crate) fn get_batch_errors(&self, py: Python<'_>) -> PyResult<Option<Vec<Py<PyAny>>>> {
        let Some(batch_errors) = &self.batch_errors_state else {
            return Ok(None);
        };
        let errors_mod = PyModule::import(py, "oracledb.errors")?;
        let error_type = errors_mod.getattr("_Error")?;
        let mut result = Vec::with_capacity(batch_errors.len());
        for batch_error in batch_errors {
            let kwargs = PyDict::new(py);
            if !batch_error.message.is_empty() {
                kwargs.set_item("message", &batch_error.message)?;
            }
            kwargs.set_item("code", batch_error.code)?;
            kwargs.set_item("offset", batch_error.offset)?;
            result.push(error_type.call((), Some(&kwargs))?.unbind());
        }
        Ok(Some(result))
    }

    pub(crate) fn get_bind_names(&self) -> Vec<String> {
        self.bind_names
            .iter()
            .map(|name| public_bind_name(name))
            .collect()
    }

    pub(crate) fn get_implicit_results(
        &mut self,
        connection: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let py = connection.py();
        if let Some(cursors) = &self.implicit_result_cursors {
            return Ok(cursors.iter().map(|cursor| cursor.clone_ref(py)).collect());
        }
        // reference thin/cursor.pyx get_implicit_results: DPY-1004 until a
        // statement producing implicit results has been executed
        let Some(resultsets) = &self.implicit_resultsets else {
            return Err(raise_oracledb_driver_error("ERR_NO_STATEMENT_EXECUTED"));
        };
        let mut cursors = Vec::with_capacity(resultsets.len());
        for value in resultsets {
            let QueryValue::Cursor { columns, cursor_id } = value else {
                continue;
            };
            let child_cursor = connection.call_method0("cursor")?;
            hydrate_cursor_impl(&child_cursor, columns, *cursor_id, false)?;
            cursors.push(child_cursor.unbind());
        }
        self.implicit_result_cursors =
            Some(cursors.iter().map(|cursor| cursor.clone_ref(py)).collect());
        Ok(cursors)
    }

    pub(crate) fn get_lastrowid(&self) -> Option<String> {
        // reference thin/cursor.pyx get_lastrowid: only exposed when the
        // last statement affected at least one row
        if self.rowcount > 0 {
            self.last_rowid.clone()
        } else {
            None
        }
    }

    pub(crate) fn get_handle(&self) -> PyResult<Py<PyAny>> {
        Err(raise_not_supported("getting an OCIStmt handle"))
    }

    #[pyo3(signature = (external_handle_capsule=None))]
    pub(crate) fn attach_external_handle(
        &self,
        external_handle_capsule: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let _ = external_handle_capsule;
        Err(raise_not_supported("attaching an external OCIStmt handle"))
    }
}
