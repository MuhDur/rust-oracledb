//! Thin CQN subscription impl exposed to the python `oracledb` layer.
//!
//! The shipped pure-python `Connection.subscribe()` calls
//! `self._impl.create_subscr_impl(...)` to build a [`ThinSubscrImpl`], then
//! `impl.subscribe(subscr, conn_impl)`; `Subscription` reads its attributes off
//! `self._impl.*`. This module satisfies that `_impl` contract and drives the
//! CQN two-connection notification model:
//!
//! 1. The **primary** connection (the user's `conn`) sends SUBSCRIBE (FUNC 125)
//!    and returns `(registration_id, client_id)`.
//! 2. A **background "emon" connection** is opened on a daemon thread by cloning
//!    the primary's connect options and injecting `(SERVER=emon)`. It sends one
//!    NOTIFY (FUNC 187), signals a ready event so `subscribe()` can return, then
//!    loops receiving server-pushed OAC notification packets and invoking the
//!    user callback (under the GIL) with a freshly built `Message`.
//!
//! Teardown (`unsubscribe`): the primary connection sends UNSUBSCRIBE (FUNC 125
//! opcode 2), then the background thread is signalled to stop and joined. The
//! background read is bounded, so the thread exits promptly and never hangs.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use asupersync::Cx;
use oracledb::protocol::thin::{MsgQuery, MsgTable, NotificationMessage, NotificationRecord};
use oracledb::{
    BlockingConnection, ConnectOptions, Connection as RustConnection, Error as DriverError,
};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::async_bridge::build_pyshim_io_runtime;
use crate::conn::ThinConnImpl;
use crate::errors::runtime_error;

/// How long each background socket read blocks before the loop re-checks the
/// shutdown flag. Small enough that teardown joins promptly, large enough to
/// avoid busy-spinning.
const NOTIFICATION_READ_TIMEOUT: Duration = Duration::from_millis(200);

/// Gate that lets `subscribe()` block until the background NOTIFY has been
/// written (or the background connect failed), mirroring the reference
/// `threading.Event` wait.
struct ReadyGate {
    state: Mutex<ReadyState>,
    condvar: Condvar,
}

struct ReadyState {
    ready: bool,
    error: Option<String>,
}

impl ReadyGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(ReadyState {
                ready: false,
                error: None,
            }),
            condvar: Condvar::new(),
        })
    }

    fn signal_ready(&self) {
        if let Ok(mut guard) = self.state.lock() {
            guard.ready = true;
            self.condvar.notify_all();
        }
    }

    fn signal_error(&self, error: String) {
        if let Ok(mut guard) = self.state.lock() {
            guard.ready = true;
            guard.error = Some(error);
            self.condvar.notify_all();
        }
    }

    /// Block until ready; returns any background-connect error message.
    fn wait(&self) -> Option<String> {
        let mut guard = match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        while !guard.ready {
            guard = match self.condvar.wait(guard) {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
        guard.error.clone()
    }
}

/// Live background-task state for an active subscription.
struct BgTask {
    handle: JoinHandle<()>,
    stop: Arc<AtomicBool>,
}

#[pyclass(module = "oracledb.thin_impl", name = "ThinSubscrImpl")]
pub(crate) struct ThinSubscrImpl {
    // attributes read by the python `Subscription` via `self._impl.*`
    callback: Option<Py<PyAny>>,
    connection: Py<PyAny>,
    namespace: u32,
    name: Option<String>,
    protocol: u32,
    ip_address: Option<String>,
    port: u32,
    timeout: u32,
    operations: u32,
    qos: u32,
    id: u64,
    grouping_class: u8,
    grouping_value: u32,
    grouping_type: u8,
    // runtime state
    client_id: Option<Vec<u8>>,
    bg_task: Mutex<Option<BgTask>>,
}

impl ThinSubscrImpl {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        connection: Py<PyAny>,
        callback: Option<Py<PyAny>>,
        namespace: u32,
        name: Option<String>,
        protocol: u32,
        ip_address: Option<String>,
        port: u32,
        timeout: u32,
        operations: u32,
        qos: u32,
        grouping_class: u8,
        grouping_value: u32,
        grouping_type: u8,
    ) -> Self {
        Self {
            callback,
            connection,
            namespace,
            name,
            protocol,
            ip_address,
            port,
            timeout,
            operations,
            qos,
            id: 0,
            grouping_class,
            grouping_value,
            grouping_type,
            client_id: None,
            bg_task: Mutex::new(None),
        }
    }
}

#[pymethods]
impl ThinSubscrImpl {
    #[getter]
    fn callback(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.callback.as_ref().map(|cb| cb.clone_ref(py))
    }

    #[getter]
    fn connection(&self, py: Python<'_>) -> Py<PyAny> {
        self.connection.clone_ref(py)
    }

    #[getter]
    fn namespace(&self) -> u32 {
        self.namespace
    }

    #[getter]
    fn name(&self) -> Option<String> {
        self.name.clone()
    }

    #[getter]
    fn protocol(&self) -> u32 {
        self.protocol
    }

    #[getter]
    fn ip_address(&self) -> Option<String> {
        self.ip_address.clone()
    }

    #[getter]
    fn port(&self) -> u32 {
        self.port
    }

    #[getter]
    fn timeout(&self) -> u32 {
        self.timeout
    }

    #[getter]
    fn operations(&self) -> u32 {
        self.operations
    }

    #[getter]
    fn qos(&self) -> u32 {
        self.qos
    }

    #[getter]
    fn id(&self) -> u64 {
        self.id
    }

    #[getter]
    fn grouping_class(&self) -> u8 {
        self.grouping_class
    }

    #[getter]
    fn grouping_value(&self) -> u32 {
        self.grouping_value
    }

    #[getter]
    fn grouping_type(&self) -> u8 {
        self.grouping_type
    }

    /// Register the subscription: FUNC 125 on the primary connection, then spawn
    /// the emon background notification connection. Mirrors
    /// `ThinSubscrImpl.subscribe`.
    fn subscribe(
        &mut self,
        py: Python<'_>,
        _subscr: Py<PyAny>,
        conn_impl: Py<ThinConnImpl>,
    ) -> PyResult<()> {
        // AQ namespace with qos==0 pre-sets the (public) secure flag so the
        // reference qos derivation is a no-op (subscr.pyx:114). We pass the
        // public qos straight through; the wire SECURE bit is always set by the
        // protocol builder, so nothing extra is required here.
        let (registration_id, client_id, emon_options) = {
            let conn_ref = conn_impl.borrow(py);
            let emon_options = conn_ref.emon_connect_options()?;
            let result = conn_ref.with_connection(|connection| {
                BlockingConnection::subscribe_register(
                    connection,
                    self.namespace,
                    self.name.as_deref(),
                    self.qos,
                    self.operations,
                    self.timeout,
                    self.grouping_class,
                    self.grouping_value,
                    self.grouping_type,
                )
            })?;
            (result.registration_id, result.client_id, emon_options)
        };
        self.id = registration_id;
        self.client_id = client_id.clone();

        let Some(client_id) = client_id else {
            return Err(runtime_error(
                "subscribe did not return an emon client id".to_string(),
            ));
        };

        // spawn the emon background task on a dedicated OS thread we own (so it
        // can be joined cleanly on teardown). The GIL is released while the
        // worker connects and sends NOTIFY so it can re-acquire it to invoke the
        // callback.
        let subscr_obj = _subscr.clone_ref(py);
        let namespace = self.namespace;
        let qos = self.qos;
        let stop = Arc::new(AtomicBool::new(false));
        let ready = ReadyGate::new();
        let task_stop = Arc::clone(&stop);
        let task_ready = Arc::clone(&ready);
        let handle = std::thread::Builder::new()
            .name("oracledb-pyshim-cqn-emon".to_string())
            .spawn(move || {
                run_emon_task(
                    emon_options,
                    client_id,
                    namespace,
                    qos,
                    &subscr_obj,
                    &task_stop,
                    &task_ready,
                );
            })
            .map_err(|err| runtime_error(format!("failed to spawn emon thread: {err}")))?;

        // wait until NOTIFY is on the wire (or the background connect failed),
        // mirroring the reference event.wait(). Release the GIL while waiting so
        // the worker thread can run.
        let error = py.detach(|| ready.wait());
        if let Some(error) = error {
            stop.store(true, Ordering::SeqCst);
            py.detach(|| {
                let _ = handle.join();
            });
            return Err(runtime_error(format!("DPY-6007: {error}")));
        }
        *self.bg_task.lock().map_err(runtime_error)? = Some(BgTask { handle, stop });
        Ok(())
    }

    /// Register a query against this subscription. Reference
    /// `ThinSubscrImpl.register_query`: prepare the statement, reject non-queries
    /// (DPY-1003), thread the registration id into the execute and return the
    /// query id (or `None` without SUBSCR_QOS_QUERY). The AQ guard (DPY-2071)
    /// lives in the python `subscr.py`.
    #[pyo3(signature = (sql, args=None))]
    fn register_query(
        &self,
        py: Python<'_>,
        sql: &str,
        args: Option<Py<PyAny>>,
    ) -> PyResult<Option<u64>> {
        let _ = args;
        if !statement_is_query(sql) {
            return Err(ora_not_a_query());
        }
        let conn_impl = self.conn_impl(py)?;
        let conn_ref = conn_impl.borrow(py);
        let query_id = conn_ref.with_connection(|connection| {
            BlockingConnection::execute_query_for_registration(connection, sql, self.id)
        })?;
        // without SUBSCR_QOS_QUERY the server returns query id 0 -> public None
        match query_id {
            Some(0) | None => Ok(None),
            Some(id) => Ok(Some(id)),
        }
    }

    /// Destroy the subscription: FUNC 125 opcode 2 on the primary connection,
    /// then stop and join the emon background thread. Mirrors
    /// `ThinSubscrImpl.unsubscribe`.
    fn unsubscribe(
        &mut self,
        py: Python<'_>,
        _subscr: Py<PyAny>,
        conn_impl: Py<ThinConnImpl>,
    ) -> PyResult<()> {
        let client_id = self.client_id.clone().unwrap_or_default();
        let registration_id = self.id;
        let namespace = self.namespace;
        let name = self.name.clone();
        let qos = self.qos;
        let operations = self.operations;
        let timeout = self.timeout;
        let grouping_class = self.grouping_class;
        let grouping_value = self.grouping_value;
        let grouping_type = self.grouping_type;
        {
            let conn_ref = conn_impl.borrow(py);
            conn_ref.with_connection(|connection| {
                BlockingConnection::subscribe_unregister(
                    connection,
                    registration_id,
                    &client_id,
                    namespace,
                    name.as_deref(),
                    qos,
                    operations,
                    timeout,
                    grouping_class,
                    grouping_value,
                    grouping_type,
                )
            })?;
        }
        self.stop_bg_task(py);
        Ok(())
    }
}

impl ThinSubscrImpl {
    fn conn_impl(&self, py: Python<'_>) -> PyResult<Py<ThinConnImpl>> {
        Ok(self
            .connection
            .getattr(py, "_impl")?
            .extract::<Py<ThinConnImpl>>(py)?)
    }

    /// Stop and join the background emon thread (releasing the GIL so the worker
    /// can re-acquire it to finish any in-flight callback).
    fn stop_bg_task(&mut self, py: Python<'_>) {
        let task = self.bg_task.lock().ok().and_then(|mut guard| guard.take());
        if let Some(task) = task {
            task.stop.store(true, Ordering::SeqCst);
            py.detach(|| {
                let _ = task.handle.join();
            });
        }
    }
}

impl Drop for ThinSubscrImpl {
    fn drop(&mut self) {
        let task = self.bg_task.lock().ok().and_then(|mut guard| guard.take());
        if let Some(task) = task {
            task.stop.store(true, Ordering::SeqCst);
            let _ = task.handle.join();
        }
    }
}

/// Background worker: connect the emon connection, send NOTIFY, signal ready,
/// then loop delivering pushed notification records to the user callback.
fn run_emon_task(
    options: ConnectOptions,
    client_id: Vec<u8>,
    namespace: u32,
    qos: u32,
    subscr: &Py<PyAny>,
    stop: &AtomicBool,
    ready: &ReadyGate,
) {
    let runtime = match build_pyshim_io_runtime() {
        Ok(runtime) => runtime,
        Err(err) => {
            ready.signal_error(err);
            return;
        }
    };
    let mut connection = match runtime.block_on(connect_emon(&options)) {
        Ok(connection) => connection,
        Err(err) => {
            ready.signal_error(err.to_string());
            return;
        }
    };
    // send NOTIFY then signal ready so subscribe() can return
    if let Err(err) = runtime.block_on(send_notify(&mut connection, &client_id)) {
        ready.signal_error(err.to_string());
        let _ = BlockingConnection::close(connection);
        return;
    }
    ready.signal_ready();

    // notification receive loop
    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        match runtime.block_on(recv_one(&mut connection, namespace, qos)) {
            Ok(oracledb::NotificationOutcome::Record(record)) => {
                let end = matches!(
                    &record,
                    NotificationRecord::Message {
                        end_of_response: true,
                        ..
                    }
                );
                if !deliver_record(subscr, &record) || end {
                    break;
                }
            }
            Ok(oracledb::NotificationOutcome::TimedOut) => {}
            // `NotificationOutcome` is `#[non_exhaustive]`: `Closed`, any
            // future terminal outcome, and a receive error all stop the loop.
            Ok(oracledb::NotificationOutcome::Closed) | Ok(_) | Err(_) => break,
        }
    }
    let _ = BlockingConnection::close(connection);
}

async fn connect_emon(options: &ConnectOptions) -> Result<RustConnection, DriverError> {
    let cx = Cx::current().ok_or_else(|| {
        DriverError::Runtime("asupersync did not install an ambient Cx".to_string())
    })?;
    RustConnection::connect(&cx, options.clone()).await
}

async fn send_notify(connection: &mut RustConnection, client_id: &[u8]) -> Result<(), DriverError> {
    let cx = Cx::current().ok_or_else(|| {
        DriverError::Runtime("asupersync did not install an ambient Cx".to_string())
    })?;
    connection.notify_register(&cx, client_id).await
}

async fn recv_one(
    connection: &mut RustConnection,
    namespace: u32,
    qos: u32,
) -> Result<oracledb::NotificationOutcome, DriverError> {
    let cx = Cx::current().ok_or_else(|| {
        DriverError::Runtime("asupersync did not install an ambient Cx".to_string())
    })?;
    connection
        .recv_notification(&cx, namespace, qos, NOTIFICATION_READ_TIMEOUT)
        .await
}

/// Build the python `Message` (and its children) under the GIL and invoke the
/// user callback. Returns `false` if delivery should stop (callback error or
/// missing callback).
fn deliver_record(subscr: &Py<PyAny>, record: &NotificationRecord) -> bool {
    let NotificationRecord::Message { message, .. } = record else {
        return true;
    };
    Python::attach(|py| match build_and_invoke(py, subscr, message) {
        Ok(()) => true,
        Err(err) => {
            // a callback that raises is reported to stderr (reference logs and
            // continues); stop the loop to avoid a tight error spin.
            err.print(py);
            false
        }
    })
}

fn build_and_invoke(
    py: Python<'_>,
    subscr: &Py<PyAny>,
    message: &NotificationMessage,
) -> PyResult<()> {
    let subscr_mod = PyModule::import(py, "oracledb.subscr")?;
    let subscr_bound = subscr.bind(py);
    let callback = subscr_bound.getattr("callback")?;
    if callback.is_none() {
        return Ok(());
    }
    let py_message = build_message(py, &subscr_mod, subscr_bound, message)?;
    callback.call1((py_message,))?;
    Ok(())
}

fn build_message<'py>(
    py: Python<'py>,
    subscr_mod: &Bound<'py, PyModule>,
    subscr: &Bound<'py, PyAny>,
    message: &NotificationMessage,
) -> PyResult<Bound<'py, PyAny>> {
    let py_message = subscr_mod.getattr("Message")?.call1((subscr,))?;
    py_message.setattr("_type", message.msg_type)?;
    py_message.setattr("_registered", message.registered)?;
    if let Some(dbname) = &message.dbname {
        py_message.setattr("_dbname", dbname)?;
    }
    match &message.txid {
        Some(txid) => py_message.setattr("_txid", PyBytes::new(py, txid))?,
        None => py_message.setattr("_txid", py.None())?,
    }
    set_optional_str(&py_message, "_queue_name", message.queue_name.as_deref())?;
    set_optional_str(
        &py_message,
        "_consumer_name",
        message.consumer_name.as_deref(),
    )?;
    match &message.msgid {
        Some(msgid) => py_message.setattr("_msgid", PyBytes::new(py, msgid))?,
        None => py_message.setattr("_msgid", py.None())?,
    }
    let tables = build_tables(py, subscr_mod, &message.tables)?;
    py_message.setattr("_tables", tables)?;
    let queries = build_queries(py, subscr_mod, &message.queries)?;
    py_message.setattr("_queries", queries)?;
    Ok(py_message)
}

fn build_tables<'py>(
    py: Python<'py>,
    subscr_mod: &Bound<'py, PyModule>,
    tables: &[MsgTable],
) -> PyResult<Bound<'py, PyAny>> {
    let list = pyo3::types::PyList::empty(py);
    for table in tables {
        let py_table = subscr_mod.getattr("MessageTable")?.call0()?;
        py_table.setattr("_operation", table.operation)?;
        py_table.setattr("_name", &table.name)?;
        let rows = pyo3::types::PyList::empty(py);
        for row in &table.rows {
            let py_row = subscr_mod.getattr("MessageRow")?.call0()?;
            py_row.setattr("_operation", row.operation)?;
            py_row.setattr("_rowid", &row.rowid)?;
            rows.append(py_row)?;
        }
        py_table.setattr("_rows", rows)?;
        list.append(py_table)?;
    }
    Ok(list.into_any())
}

fn build_queries<'py>(
    py: Python<'py>,
    subscr_mod: &Bound<'py, PyModule>,
    queries: &[MsgQuery],
) -> PyResult<Bound<'py, PyAny>> {
    let list = pyo3::types::PyList::empty(py);
    for query in queries {
        let py_query = subscr_mod.getattr("MessageQuery")?.call0()?;
        py_query.setattr("_id", query.id)?;
        py_query.setattr("_operation", query.operation)?;
        let tables = build_tables(py, subscr_mod, &query.tables)?;
        py_query.setattr("_tables", tables)?;
        list.append(py_query)?;
    }
    Ok(list.into_any())
}

fn set_optional_str(obj: &Bound<'_, PyAny>, attr: &str, value: Option<&str>) -> PyResult<()> {
    match value {
        Some(value) => obj.setattr(attr, value),
        None => obj.setattr(attr, obj.py().None()),
    }
}

/// Whether `sql` is a query for registerquery purposes. The reference treats a
/// leading `SELECT` or `WITH` keyword as a query (statement.pyx:363-364).
fn statement_is_query(sql: &str) -> bool {
    sql.trim_start()
        .split(|ch: char| !ch.is_ascii_alphabetic())
        .next()
        .is_some_and(|keyword| {
            keyword.eq_ignore_ascii_case("select") || keyword.eq_ignore_ascii_case("with")
        })
}

fn ora_not_a_query() -> PyErr {
    // DPY-1003 ERR_NOT_A_QUERY (reference errors.py:863)
    crate::errors::dpy_database_error("DPY-1003", "the executed statement does not return rows")
}
