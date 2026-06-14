use std::sync::{Arc, Mutex};

use oracledb::protocol::thin::{
    CS_FORM_IMPLICIT, CS_FORM_NCHAR, ORA_TYPE_NUM_BLOB, ORA_TYPE_NUM_CLOB,
};
use oracledb::BlockingConnection;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};

use crate::*;

#[pyclass(module = "oracledb.thin_impl", name = "AsyncThinConnImpl")]
pub(crate) struct AsyncThinConnImpl {
    pub(crate) inner: ThinConnImpl,
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
        self.inner.server_version = connection.server_version_tuple().unwrap_or_default();
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
        // Mirror reference connection.pyx: set at the end of every connect.
        self.inner.invoke_session_callback = true;
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
        Ok(())
    }

    /// Async begin of a sessionless transaction (reference async connection.py
    /// `begin_sessionless_transaction`).
    async fn begin_sessionless_transaction(
        &self,
        transaction_id: Vec<u8>,
        timeout: u32,
        defer_round_trip: bool,
    ) -> PyResult<()> {
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-begin-sessionless",
            Arc::clone(&self.inner.connection),
            move |cx, connection| {
                Box::pin(async move {
                    connection
                        .begin_sessionless_transaction(
                            cx,
                            &transaction_id,
                            timeout,
                            defer_round_trip,
                        )
                        .await
                        .map_err(TaskError::from)
                })
            },
        );
        task.await.map_err(runtime_error)?;
        Ok(())
    }

    /// Async resume of a sessionless transaction (reference async
    /// `resume_sessionless_transaction`).
    async fn resume_sessionless_transaction(
        &self,
        transaction_id: Vec<u8>,
        timeout: u32,
        defer_round_trip: bool,
    ) -> PyResult<()> {
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-resume-sessionless",
            Arc::clone(&self.inner.connection),
            move |cx, connection| {
                Box::pin(async move {
                    connection
                        .resume_sessionless_transaction(
                            cx,
                            &transaction_id,
                            timeout,
                            defer_round_trip,
                        )
                        .await
                        .map_err(TaskError::from)
                })
            },
        );
        task.await.map_err(runtime_error)?;
        Ok(())
    }

    /// Async suspend of the active sessionless transaction (reference async
    /// `suspend_sessionless_transaction`).
    async fn suspend_sessionless_transaction(&self) -> PyResult<()> {
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-suspend-sessionless",
            Arc::clone(&self.inner.connection),
            |cx, connection| {
                Box::pin(async move {
                    connection
                        .suspend_sessionless_transaction(cx)
                        .await
                        .map_err(TaskError::from)
                })
            },
        );
        task.await.map_err(runtime_error)
    }

    /// Async begin of an XA global transaction (reference async `tpc_begin`).
    #[pyo3(signature = (xid, flags, timeout))]
    async fn tpc_begin(&self, xid: Py<PyAny>, flags: u32, timeout: u32) -> PyResult<()> {
        let (format_id, gtid, bqual) = Python::attach(|py| extract_xid(xid.bind(py)))?;
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-tpc-begin",
            Arc::clone(&self.inner.connection),
            move |cx, connection| {
                Box::pin(async move {
                    connection
                        .tpc_begin(cx, format_id, &gtid, &bqual, flags, timeout)
                        .await
                        .map_err(TaskError::from)
                })
            },
        );
        task.await.map_err(runtime_error)
    }

    /// Async end (detach) of an XA global transaction branch (reference async
    /// `tpc_end`).
    #[pyo3(signature = (xid, flags))]
    async fn tpc_end(&self, xid: Py<PyAny>, flags: u32) -> PyResult<()> {
        let xid = Python::attach(|py| extract_optional_xid(xid.bind(py)))?;
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-tpc-end",
            Arc::clone(&self.inner.connection),
            move |cx, connection| {
                Box::pin(async move {
                    connection
                        .tpc_end(cx, xid_as_refs(&xid), flags)
                        .await
                        .map_err(TaskError::from)
                })
            },
        );
        task.await.map_err(runtime_error)
    }

    /// Async prepare of an XA global transaction (reference async
    /// `tpc_prepare`). Returns `True` when a commit is needed.
    #[pyo3(signature = (xid))]
    async fn tpc_prepare(&self, xid: Py<PyAny>) -> PyResult<bool> {
        let xid = Python::attach(|py| extract_optional_xid(xid.bind(py)))?;
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-tpc-prepare",
            Arc::clone(&self.inner.connection),
            move |cx, connection| {
                Box::pin(async move {
                    connection
                        .tpc_prepare(cx, xid_as_refs(&xid))
                        .await
                        .map_err(TaskError::from)
                })
            },
        );
        task.await.map_err(runtime_error)
    }

    /// Async commit of an XA global transaction (reference async `tpc_commit`).
    #[pyo3(signature = (xid, one_phase))]
    async fn tpc_commit(&self, xid: Py<PyAny>, one_phase: bool) -> PyResult<()> {
        let xid = Python::attach(|py| extract_optional_xid(xid.bind(py)))?;
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-tpc-commit",
            Arc::clone(&self.inner.connection),
            move |cx, connection| {
                Box::pin(async move {
                    connection
                        .tpc_commit(cx, xid_as_refs(&xid), one_phase)
                        .await
                        .map_err(TaskError::from)
                })
            },
        );
        task.await.map_err(runtime_error)
    }

    /// Async rollback of an XA global transaction (reference async
    /// `tpc_rollback`).
    #[pyo3(signature = (xid))]
    async fn tpc_rollback(&self, xid: Py<PyAny>) -> PyResult<()> {
        let xid = Python::attach(|py| extract_optional_xid(xid.bind(py)))?;
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-tpc-rollback",
            Arc::clone(&self.inner.connection),
            move |cx, connection| {
                Box::pin(async move {
                    connection
                        .tpc_rollback(cx, xid_as_refs(&xid))
                        .await
                        .map_err(TaskError::from)
                })
            },
        );
        task.await.map_err(runtime_error)
    }

    /// Async forget of an XA global transaction. Thin mode does not support it;
    /// raises DPY-3001 (NotSupportedError) without a round trip (reference base
    /// impl `tpc_forget`).
    #[pyo3(signature = (xid))]
    async fn tpc_forget(&self, xid: Py<PyAny>) -> PyResult<()> {
        Python::attach(|py| extract_xid(xid.bind(py)))?;
        Err(raise_not_supported(
            "forgetting a TPC (two-phase commit) transaction",
        ))
    }

    /// Async Direct Path Load (reference thin/connection.pyx:1179). Mirrors the
    /// sync ThinConnImpl::direct_path_load but bridges each wire step through the
    /// async fetch-task runtime: materialize+verify (GIL) -> PREPARE -> convert
    /// against the prepared metadata (GIL) -> load+FINISH/ABORT.
    async fn direct_path_load(
        &self,
        schema_name: String,
        table_name: String,
        column_names: Vec<String>,
        data: Py<PyAny>,
        batch_size: u32,
    ) -> PyResult<()> {
        if batch_size == 0 {
            return Err(PyTypeError::new_err(
                "batch_size must be a positive integer",
            ));
        }
        let num_columns = column_names.len();
        let py_rows = Python::attach(|py| -> PyResult<Vec<Py<PyTuple>>> {
            let data = data.bind(py);
            let py_rows = direct_path_py_rows(data)?;
            verify_direct_path_widths(py, &py_rows, num_columns)?;
            Ok(py_rows)
        })?;

        // PREPARE.
        let prepare = {
            let schema_name = schema_name.clone();
            let table_name = table_name.clone();
            let column_names = column_names.clone();
            let task = spawn_async_connection_task(
                "oracledb-pyshim-async-dpl-prepare",
                Arc::clone(&self.inner.connection),
                move |cx, connection| {
                    Box::pin(async move {
                        connection
                            .direct_path_prepare(cx, &schema_name, &table_name, &column_names)
                            .await
                            .map_err(TaskError::from)
                    })
                },
            );
            task.await
                .map_err(|err| ora_database_error(&err.to_string()))?
        };

        // Convert against the prepared per-column metadata.
        let rows = Python::attach(|py| {
            direct_path_rows_from_py(py, &py_rows, &prepare.column_metadata, num_columns)
        })?;

        // LOAD + FINISH/ABORT.
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-dpl-load",
            Arc::clone(&self.inner.connection),
            move |cx, connection| {
                Box::pin(async move {
                    connection
                        .direct_path_load_prepared(cx, &prepare, &rows, batch_size)
                        .await
                        .map_err(TaskError::from)
                })
            },
        );
        // Surface DPL wire errors (DPY-8000/8001, ORA-*) as proper oracledb
        // DatabaseErrors carrying the embedded code, not a bare RuntimeError.
        task.await
            .map_err(|err| ora_database_error(&err.to_string()))?;
        Ok(())
    }

    async fn change_password(&self, old_password: String, new_password: String) -> PyResult<()> {
        let task = {
            let new_password = new_password.clone();
            spawn_async_connection_task(
                "oracledb-pyshim-async-change-password",
                Arc::clone(&self.inner.connection),
                move |cx, connection| {
                    Box::pin(async move {
                        connection
                            .change_password(cx, &old_password, &new_password)
                            .await
                            .map_err(TaskError::from)
                    })
                },
            )
        };
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

    fn create_cursor_impl(
        &self,
        py: Python<'_>,
        scrollable: bool,
    ) -> PyResult<AsyncThinCursorImpl> {
        Ok(AsyncThinCursorImpl {
            inner: self.inner.create_cursor_impl(py, scrollable)?,
        })
    }

    fn create_queue_impl(&self) -> crate::aq::AsyncThinQueueImpl {
        crate::aq::AsyncThinQueueImpl::new()
    }

    fn create_msg_props_impl(&self) -> crate::aq::ThinMsgPropsImpl {
        crate::aq::ThinMsgPropsImpl::new()
    }

    /// Pipeline contract (reference connection.py:2786-2796): the public layer
    /// routes a multi-op pipeline through `run_pipeline_with_pipelining` only
    /// when this reports `True`. The async surface advertises native pipelining
    /// because the native single-round-trip transport is wired in here
    /// (`run_pipeline_with_pipelining` -> `pipeline::run_pipeline_native`): a
    /// batch of independent statements is sent in ONE round trip
    /// (`oracledb::Connection::run_pipeline`: BEGIN_PIPELINE piggyback,
    /// END_OF_REQUEST framing, end-pipeline FUNC 200, N+1 boundary-delimited
    /// responses — proven byte-for-byte by `pipeline_golden.rs` and live by
    /// `pipeline_live.rs`) and each response is decoded back through the same
    /// `parse_query_response_*` decoders the ordinary execute path uses.
    ///
    /// The native runner handles the all-simple op subset (execute / executemany
    /// of array DML / commit / fetch of scalar binds) and falls back, per
    /// pipeline, to the sequential runner for any op that needs the connection
    /// interleaved with decoding (callfunc/callproc OUT binds, ref/nested
    /// cursors, PL/SQL execute with binds, DML returning, autocommit) — so the
    /// single-round-trip property and exact parity with the sequential path both
    /// hold. The sync surface keeps `supports_pipelining() == False` (sync
    /// pipelining is not the target).
    fn supports_pipelining(&self) -> bool {
        true
    }

    /// Native single-round-trip pipelining entry point (selected by the public
    /// layer when `supports_pipelining()` is true and the pipeline has >1 op).
    /// Drives the native `Connection::run_pipeline` for the all-simple op subset
    /// and falls back to the sequential runner per-pipeline otherwise — see
    /// `pipeline::run_pipeline_native`.
    fn run_pipeline_with_pipelining(
        &self,
        py: Python<'_>,
        conn: Py<PyAny>,
        results: Py<PyAny>,
        continue_on_error: bool,
    ) -> PyResult<Py<PyAny>> {
        run_pipeline_native(py, conn, results, continue_on_error)
    }

    fn run_pipeline_without_pipelining(
        &self,
        py: Python<'_>,
        conn: Py<PyAny>,
        results: Py<PyAny>,
        continue_on_error: bool,
    ) -> PyResult<Py<PyAny>> {
        run_pipeline_sequential(py, conn, results, continue_on_error)
    }

    /// Whether autocommit is enabled — consulted by the native pipeline runner's
    /// gate (an autocommit pipeline routes to the sequential fallback because
    /// the shim implements autocommit as a separate post-execute commit round
    /// trip, which a single-round-trip batch cannot express).
    fn get_autocommit_for_pipeline(&self) -> bool {
        self.inner.autocommit
    }

    /// Queue the native pipeline's open query cursors for close. The native
    /// decode path does not route these cursor_ids through the statement cache,
    /// so they must be explicitly retired or a run of pipelines that open query
    /// cursors leaks them server-side (ORA-01000). `close_cursor` only queues
    /// the id (no round trip); the close-cursors piggyback flushes on the next
    /// pipeline's first op. Called once at the end of the native runner, after
    /// all per-op fetches have completed. A `None` entry (COMMIT op) is skipped.
    fn _pipeline_close_cursors(
        &self,
        py: Python<'_>,
        cursors: Vec<Option<Py<PyAny>>>,
    ) -> PyResult<()> {
        let mut cursor_ids = Vec::new();
        for cursor in cursors.iter().flatten() {
            let impl_obj = cursor.bind(py).getattr("_impl")?;
            let cursor_impl = impl_obj.extract::<PyRef<'_, AsyncThinCursorImpl>>()?;
            let cursor_id = cursor_impl.inner.cursor_id;
            if cursor_id != 0 {
                cursor_ids.push(cursor_id);
            }
        }
        if cursor_ids.is_empty() {
            return Ok(());
        }
        let mut guard = self.inner.connection.lock().map_err(runtime_error)?;
        if let Some(connection) = guard.as_mut() {
            for cursor_id in cursor_ids {
                connection.close_cursor(cursor_id);
            }
        }
        Ok(())
    }

    /// Phase 2 of the native pipeline runner: drive `Connection::run_pipeline`
    /// for the prepared per-op cursors in ONE round trip and decode each
    /// response onto the matching cursor impl, returning a per-op error (or
    /// `None`) list so the Python runner can apply abort/continue semantics and
    /// then read each op's result through the public cursor API.
    ///
    /// `cursors` is the list of public cursor objects produced by the runner's
    /// prepare phase, one per operation; a `None` entry marks a COMMIT op (no
    /// cursor). Each cursor's `_impl` already carries its prepared statement and
    /// bind values.
    async fn _pipeline_native_execute(
        &self,
        cursors: Vec<Option<Py<PyAny>>>,
        continue_on_error: bool,
    ) -> PyResult<Py<PyAny>> {
        // Phase 2a (GIL): extract one PipelineRequest per op from the prepared
        // cursor impls (statement + bind values).
        let requests =
            Python::attach(|py| crate::pipeline::build_native_plan(py, &cursors))?.requests;

        // Phase 2b (off the event loop): one wire round trip + decode.
        let connection = Arc::clone(&self.inner.connection);
        let task = spawn_blocking_task("oracledb-pyshim-async-pipeline", move || {
            let mut guard = connection.lock().map_err(|err| err.to_string())?;
            let conn = guard
                .as_mut()
                .ok_or_else(|| "connection is closed".to_string())?;
            BlockingConnection::run_pipeline_decoded(conn, &requests, continue_on_error)
                .map_err(TaskError::from)
        });
        let decoded = task.await.map_err(runtime_error)?;

        // Phase 2c (GIL): populate each op's cursor impl from its decoded
        // QueryResult and collect the per-op error list returned to Python.
        let connection = Arc::clone(&self.inner.connection);
        Python::attach(|py| {
            crate::pipeline::populate_native_results(py, &cursors, &connection, decoded)
        })
    }
}
