//! Pool marshalling layer: maps the python-oracledb pool impl surface onto
//! the driver pool engine (`oracledb::pool`). The engine owns the state
//! machine; this module owns the Python objects (conn impls live in a
//! registry keyed by engine id) and performs connection creation, ping and
//! close work on behalf of the engine's background worker.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, OnceLock};

use oracledb::pool::{
    AcquireOptions, BlockingPool, BlockingPooledConnection, Pool, PoolBackend, PoolConfig,
    PoolError, PURITY_NEW,
};
use oracledb::{BlockingConnection, Connection as RustConnection};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::*;

// ---------------------------------------------------------------------------
// Pool creation-argument capture (password is unreadable from PoolParamsImpl;
// the harness records it via `record_next_pool_args` before pool creation).
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub(crate) struct PoolArgs {
    id: u64,
    pub(crate) password: Option<String>,
}

static NEXT_POOL_ARGS: OnceLock<Mutex<VecDeque<PoolArgs>>> = OnceLock::new();

fn next_pool_args_queue() -> &'static Mutex<VecDeque<PoolArgs>> {
    NEXT_POOL_ARGS.get_or_init(|| Mutex::new(VecDeque::new()))
}

fn consume_next_pool_args() -> PyResult<PoolArgs> {
    Ok(next_pool_args_queue()
        .lock()
        .map_err(runtime_error)?
        .pop_front()
        .unwrap_or_default())
}

#[pyfunction]
#[pyo3(signature = (password=None))]
pub(crate) fn record_next_pool_args(password: Option<String>) -> PyResult<u64> {
    let id = NEXT_CONNECT_ARGS_ID.fetch_add(1, Ordering::Relaxed);
    next_pool_args_queue()
        .lock()
        .map_err(runtime_error)?
        .push_back(PoolArgs { id, password });
    Ok(id)
}

#[pyfunction]
pub(crate) fn discard_pending_pool_args(entry_id: u64) -> PyResult<bool> {
    let mut queue = next_pool_args_queue().lock().map_err(runtime_error)?;
    if let Some(position) = queue.iter().position(|entry| entry.id.eq(&entry_id)) {
        queue.remove(position);
        return Ok(true);
    }
    Ok(false)
}

// ---------------------------------------------------------------------------
// Engine backend
// ---------------------------------------------------------------------------

/// Engine payload: shared handle onto the pooled conn impl's transport.
/// Closing the transport through `connection` is immediately visible to the
/// Python-side impl object, which shares the same `Arc`.
pub(crate) struct ConnHandle {
    connection: Arc<Mutex<Option<RustConnection>>>,
}

type Registry = Arc<Mutex<HashMap<u64, Py<PyAny>>>>;

pub(crate) struct ShimPoolBackend {
    dsn: String,
    creation_params: Py<PyAny>,
    password: Option<String>,
    is_async: bool,
    registry: Registry,
}

fn pyerr_to_message(err: PyErr) -> String {
    Python::attach(|py| {
        err.value(py)
            .str()
            .map(|value| value.to_string())
            .unwrap_or_else(|_| err.to_string())
    })
}

fn plain_identifier(value: &str) -> Result<&str, String> {
    if !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '$' | '#'))
    {
        Ok(value)
    } else {
        Err(format!("invalid Oracle identifier: {value}"))
    }
}

impl PoolBackend for ShimPoolBackend {
    type Conn = ConnHandle;

    fn create_connection(&self, id: u64, _cclass: Option<&str>) -> Result<ConnHandle, String> {
        // Phase 1 (GIL): build the conn impl and prepare connect options.
        let (mut conn_impl, prepared) = Python::attach(|py| {
            let params = self.creation_params.bind(py);
            let mut conn_impl =
                ThinConnImpl::new_for_pool(&self.dsn, params, self.password.clone(), id)?;
            let prepared = conn_impl.prepare_connect(params)?;
            Ok::<_, PyErr>((conn_impl, prepared))
        })
        .map_err(pyerr_to_message)?;

        // Phase 2 (no GIL): connect and apply the creation edition.
        let mut connection =
            BlockingConnection::connect(prepared.options).map_err(|err| err.to_string())?;
        let cancel_handle = connection.cancel_handle().map_err(|err| err.to_string())?;
        if let Some(edition) = &prepared.edition {
            let identifier = plain_identifier(edition)?;
            BlockingConnection::execute_with(
                &mut connection,
                execute_with_call_timeout(
                    &format!("alter session set edition = {identifier}"),
                    None,
                ),
            )
            .map_err(|err| err.to_string())?;
        }

        // Phase 3 (GIL): finalize the impl and register the Python object.
        Python::attach(|py| {
            conn_impl.server_version = connection.server_version_tuple().unwrap_or_default();
            *conn_impl.cancel_handle.lock().map_err(runtime_error)? = Some(cancel_handle);
            *conn_impl.connection.lock().map_err(runtime_error)? = Some(connection);
            if let Some(edition) = prepared.edition {
                let mut state = conn_impl.state.lock().map_err(runtime_error)?;
                state.edition = Some(edition);
                state.edition_probe_started = true;
            }
            conn_impl.invoke_session_callback = true;
            let handle = ConnHandle {
                connection: Arc::clone(&conn_impl.connection),
            };
            let obj: Py<PyAny> = if self.is_async {
                Py::new(py, AsyncThinConnImpl { inner: conn_impl })?.into_any()
            } else {
                Py::new(py, conn_impl)?.into_any()
            };
            self.registry.lock().map_err(runtime_error)?.insert(id, obj);
            Ok::<_, PyErr>(handle)
        })
        .map_err(pyerr_to_message)
    }

    fn ping_connection(&self, conn: &ConnHandle, ping_timeout_ms: u32) -> bool {
        let Ok(mut guard) = conn.connection.lock() else {
            return false;
        };
        let Some(connection) = guard.as_mut() else {
            return false;
        };
        BlockingConnection::ping_with_timeout(connection, ping_timeout_ms).is_ok()
    }

    fn close_connection(&self, id: u64, conn: ConnHandle) {
        let taken = conn
            .connection
            .lock()
            .ok()
            .and_then(|mut guard| guard.take());
        if let Some(connection) = taken {
            let _ = close_connection_result(connection);
        }
        Python::attach(|_py| {
            if let Ok(mut registry) = self.registry.lock() {
                registry.remove(&id);
            }
        });
    }

    fn connection_is_open(&self, conn: &ConnHandle) -> bool {
        conn.connection
            .lock()
            .ok()
            .map(|guard| guard.as_ref().is_some_and(|conn| !conn.is_dead()))
            .unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// Shared pool wrapper used by both the sync and async pool impl classes
// ---------------------------------------------------------------------------

pub(crate) struct ShimPool {
    dsn: String,
    username: String,
    homogeneous: bool,
    increment: u32,
    max: u32,
    min: u32,
    engine: BlockingPool<ShimPoolBackend>,
    registry: Registry,
    guards: Mutex<HashMap<u64, BlockingPooledConnection<ShimPoolBackend>>>,
    stmt_cache_size: Mutex<u32>,
}

fn pool_error_to_pyerr(err: PoolError) -> PyErr {
    match err {
        PoolError::Closed => raise_oracledb_driver_error("ERR_POOL_NOT_OPEN"),
        PoolError::NoConnectionAvailable => {
            raise_oracledb_driver_error("ERR_POOL_NO_CONNECTION_AVAILABLE")
        }
        PoolError::HasBusyConnections => {
            raise_oracledb_driver_error("ERR_POOL_HAS_BUSY_CONNECTIONS")
        }
        // Double-release / releasing a connection not checked out (upstream
        // 4b5aeb23d602): the reference raises DPY-1001 / ERR_NOT_CONNECTED.
        PoolError::ConnectionNotAcquired => raise_oracledb_driver_error("ERR_NOT_CONNECTED"),
        PoolError::Backend(message) => runtime_error(message),
        PoolError::Cancelled(message) => PyRuntimeError::new_err(message),
        PoolError::Internal(message) => PyRuntimeError::new_err(message),
        // `PoolError` is `#[non_exhaustive]`: a future pool failure with no
        // dedicated reference code surfaces as a generic runtime error.
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

fn raise_not_supported(feature: &str) -> PyErr {
    Python::attach(|py| -> PyResult<PyErr> {
        let errors = PyModule::import(py, "oracledb.errors")?;
        match errors.getattr("_raise_not_supported")?.call1((feature,)) {
            Ok(_) => Ok(PyRuntimeError::new_err(
                "oracledb.errors._raise_not_supported returned without raising",
            )),
            Err(err) => Ok(err),
        }
    })
    .unwrap_or_else(|_| PyRuntimeError::new_err(format!("{feature} is not supported")))
}

fn extract_acquire_options(params_impl: &Bound<'_, PyAny>) -> PyResult<AcquireOptions> {
    if let Ok(tag) = params_impl.getattr("tag") {
        if !tag.is_none() {
            return Err(raise_not_supported("session tagging"));
        }
    }
    let mut purity = 0u32;
    let mut cclass = None;
    if let Ok(description_list) = params_impl.getattr("description_list") {
        if let Ok(children) = description_list.getattr("children") {
            if children.len().unwrap_or(0) > 0 {
                let description = children.get_item(0)?;
                purity = get_optional_u32_attr(&description, "purity")?.unwrap_or(0);
                cclass = get_optional_string_attr(&description, "cclass")?;
            }
        }
    }
    Ok(AcquireOptions::new()
        .with_wants_new(purity == PURITY_NEW)
        .with_optional_cclass(cclass))
}

type PoolConnRefs = (
    u64,
    Arc<Mutex<Option<RustConnection>>>,
    Arc<Mutex<ThinConnState>>,
);

fn extract_pool_conn_refs(obj: &Bound<'_, PyAny>) -> PyResult<PoolConnRefs> {
    if let Ok(conn) = obj.extract::<PyRef<'_, ThinConnImpl>>() {
        let id = conn
            .pool_conn_id
            .ok_or_else(|| PyRuntimeError::new_err("connection is not owned by a pool"))?;
        return Ok((id, Arc::clone(&conn.connection), Arc::clone(&conn.state)));
    }
    let conn = obj.extract::<PyRef<'_, AsyncThinConnImpl>>()?;
    let id = conn
        .inner
        .pool_conn_id
        .ok_or_else(|| PyRuntimeError::new_err("connection is not owned by a pool"))?;
    Ok((
        id,
        Arc::clone(&conn.inner.connection),
        Arc::clone(&conn.inner.state),
    ))
}

impl ShimPool {
    fn new(
        dsn: &Bound<'_, PyAny>,
        params_impl: &Bound<'_, PyAny>,
        is_async: bool,
    ) -> PyResult<Arc<Self>> {
        let dsn = normalize_connect_string(dsn.extract()?);
        let username = get_string_attr(params_impl, "user")?;
        let min = get_optional_u32_attr(params_impl, "min")?.unwrap_or(1);
        let max = get_optional_u32_attr(params_impl, "max")?.unwrap_or(2);
        let increment = get_optional_u32_attr(params_impl, "increment")?.unwrap_or(1);
        let homogeneous = get_optional_bool_attr(params_impl, "homogeneous")?.unwrap_or(true);
        let getmode = get_optional_u32_attr(params_impl, "getmode")?.unwrap_or(0);
        let wait_timeout = get_optional_u32_attr(params_impl, "wait_timeout")?.unwrap_or(0);
        let timeout = get_optional_u32_attr(params_impl, "timeout")?.unwrap_or(0);
        let max_lifetime_session =
            get_optional_u32_attr(params_impl, "max_lifetime_session")?.unwrap_or(0);
        let ping_interval = get_optional_i64_attr(params_impl, "ping_interval")?.unwrap_or(60);
        let ping_timeout = get_optional_u32_attr(params_impl, "ping_timeout")?.unwrap_or(5000);
        let stmt_cache_size = get_optional_u32_attr(params_impl, "stmtcachesize")?.unwrap_or(20);
        let pool_args = consume_next_pool_args()?;
        let registry: Registry = Arc::new(Mutex::new(HashMap::new()));
        let backend = ShimPoolBackend {
            dsn: dsn.clone(),
            creation_params: params_impl.clone().unbind(),
            password: pool_args.password,
            is_async,
            registry: Arc::clone(&registry),
        };
        let config = PoolConfig::new(min, max, increment)
            .with_getmode(getmode)
            .with_wait_timeout_ms(wait_timeout)
            .with_timeout_secs(timeout)
            .with_max_lifetime_session_secs(max_lifetime_session)
            .with_ping_interval_secs(ping_interval)
            .with_ping_timeout_ms(ping_timeout);
        let engine = Pool::start(backend, config)
            .map_err(pool_error_to_pyerr)?
            .blocking();
        Ok(Arc::new(Self {
            dsn,
            username,
            homogeneous,
            increment,
            max,
            min,
            engine,
            registry,
            guards: Mutex::new(HashMap::new()),
            stmt_cache_size: Mutex::new(stmt_cache_size),
        }))
    }

    /// Blocking acquire; callers must hold neither the GIL nor any lock.
    fn acquire_blocking(&self, opts: AcquireOptions) -> Result<u64, PoolError> {
        let guard = self.engine.acquire(opts)?;
        let id = guard.id();
        self.guards
            .lock()
            .map_err(|err| PoolError::Internal(err.to_string()))?
            .insert(id, guard);
        Ok(id)
    }

    fn registered_conn(&self, py: Python<'_>, id: u64) -> PyResult<Py<PyAny>> {
        self.registry
            .lock()
            .map_err(runtime_error)?
            .get(&id)
            .map(|obj| obj.clone_ref(py))
            .ok_or_else(|| PyRuntimeError::new_err("pooled connection object not found"))
    }

    /// End-of-request handling plus engine return. Mirrors the reference:
    /// roll back an in-progress transaction; failures propagate (and leave
    /// the connection busy) unless called from `__del__`.
    fn release_blocking(&self, refs: &PoolConnRefs, in_del: bool) -> PyResult<()> {
        let (id, connection, _state) = refs;
        // Read the wire-derived transaction-in-progress flag from the driver
        // (reference protocol.pyx `_txn_in_progress`) to decide whether to roll
        // back a transaction before returning the connection to the pool.
        let in_txn = {
            let guard = connection.lock().map_err(runtime_error)?;
            guard
                .as_ref()
                .map(|conn| conn.transaction_in_progress())
                .unwrap_or(false)
        };
        if in_txn {
            let rollback_result = {
                let mut guard = connection.lock().map_err(runtime_error)?;
                match guard.as_mut() {
                    Some(conn) => BlockingConnection::rollback(conn).map_err(runtime_error),
                    None => Ok(()),
                }
            };
            match rollback_result {
                Ok(()) => {}
                Err(err) => {
                    if !in_del {
                        return Err(err);
                    }
                }
            }
        }
        let guard = self.guards.lock().map_err(runtime_error)?.remove(id);
        if let Some(guard) = guard {
            guard.release().map_err(pool_error_to_pyerr)
        } else if in_del {
            // GC / `__del__` cleanup: a connection whose guard is already gone
            // was released earlier; never raise on the finalizer path.
            Ok(())
        } else {
            // Explicit double-release (or releasing a connection already dropped
            // from the pool): the guard was consumed by the first release. The
            // reference raises DPY-1001 (ERR_NOT_CONNECTED) via its verify-
            // connected guard (upstream 4b5aeb23d602); match it instead of the
            // former silent no-op.
            Err(raise_oracledb_driver_error("ERR_NOT_CONNECTED"))
        }
    }

    fn drop_blocking(&self, conn_id: u64) -> Result<(), PoolError> {
        let guard = self
            .guards
            .lock()
            .map_err(|err| PoolError::Internal(err.to_string()))?
            .remove(&conn_id);
        if let Some(guard) = guard {
            guard.drop_from_pool()
        } else {
            Ok(())
        }
    }

    fn wait_timeout_object(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let value = self.engine.wait_timeout_ms().map_err(pool_error_to_pyerr)?;
        match value {
            // Reference quirk: the stored value is `wait_timeout / 1000`
            // (a Python float in seconds) and is returned verbatim.
            Some(ms) => Ok((f64::from(ms) / 1000.0)
                .into_pyobject(py)?
                .into_any()
                .unbind()),
            None => Ok(0u32.into_pyobject(py)?.into_any().unbind()),
        }
    }
}

fn get_optional_i64_attr(obj: &Bound<'_, PyAny>, name: &str) -> PyResult<Option<i64>> {
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

// ---------------------------------------------------------------------------
// Sync pool impl
// ---------------------------------------------------------------------------

#[pyclass(module = "oracledb.thin_impl", name = "ThinPoolImpl")]
pub(crate) struct ThinPoolImpl {
    pool: Arc<ShimPool>,
}

#[pymethods]
impl ThinPoolImpl {
    #[new]
    fn new(dsn: &Bound<'_, PyAny>, params_impl: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            pool: ShimPool::new(dsn, params_impl, false)?,
        })
    }

    #[getter]
    fn dsn(&self) -> &str {
        &self.pool.dsn
    }

    #[getter]
    fn username(&self) -> &str {
        &self.pool.username
    }

    #[getter]
    fn homogeneous(&self) -> bool {
        self.pool.homogeneous
    }

    #[getter]
    fn increment(&self) -> u32 {
        self.pool.increment
    }

    #[getter]
    fn max(&self) -> u32 {
        self.pool.max
    }

    #[getter]
    fn min(&self) -> u32 {
        self.pool.min
    }

    #[getter]
    fn name(&self) -> Option<String> {
        // Thin pools never receive a server-assigned name (reference
        // BasePoolImpl.name stays None in thin mode).
        None
    }

    fn acquire(&self, py: Python<'_>, params_impl: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let opts = extract_acquire_options(params_impl)?;
        let pool = Arc::clone(&self.pool);
        let id = py
            .detach(move || pool.acquire_blocking(opts))
            .map_err(pool_error_to_pyerr)?;
        self.pool.registered_conn(py, id)
    }

    fn close(&self, py: Python<'_>, force: bool) -> PyResult<()> {
        let pool = Arc::clone(&self.pool);
        py.detach(move || pool.engine.close(force))
            .map_err(pool_error_to_pyerr)
    }

    fn drop(&self, py: Python<'_>, conn_impl: &Bound<'_, PyAny>) -> PyResult<()> {
        let refs = extract_pool_conn_refs(conn_impl)?;
        let pool = Arc::clone(&self.pool);
        py.detach(move || pool.drop_blocking(refs.0))
            .map_err(pool_error_to_pyerr)
    }

    #[pyo3(signature = (conn_impl, in_del=false))]
    fn return_connection(
        &self,
        py: Python<'_>,
        conn_impl: &Bound<'_, PyAny>,
        in_del: bool,
    ) -> PyResult<()> {
        let refs = extract_pool_conn_refs(conn_impl)?;
        let pool = Arc::clone(&self.pool);
        py.detach(move || pool.release_blocking(&refs, in_del))
    }

    fn get_busy_count(&self) -> PyResult<u32> {
        self.pool.engine.busy_count().map_err(pool_error_to_pyerr)
    }

    fn get_open_count(&self) -> PyResult<u32> {
        self.pool.engine.open_count().map_err(pool_error_to_pyerr)
    }

    fn get_getmode(&self) -> PyResult<u32> {
        self.pool.engine.getmode().map_err(pool_error_to_pyerr)
    }

    fn set_getmode(&self, value: u32) -> PyResult<()> {
        self.pool
            .engine
            .set_getmode(value)
            .map_err(pool_error_to_pyerr)
    }

    fn get_wait_timeout(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.pool.wait_timeout_object(py)
    }

    fn set_wait_timeout(&self, value: u32) -> PyResult<()> {
        self.pool
            .engine
            .set_wait_timeout_ms(value)
            .map_err(pool_error_to_pyerr)
    }

    fn get_timeout(&self) -> PyResult<u32> {
        self.pool.engine.timeout_secs().map_err(pool_error_to_pyerr)
    }

    fn set_timeout(&self, value: u32) -> PyResult<()> {
        self.pool
            .engine
            .set_timeout_secs(value)
            .map_err(pool_error_to_pyerr)
    }

    fn get_max_lifetime_session(&self) -> PyResult<u32> {
        self.pool
            .engine
            .max_lifetime_session_secs()
            .map_err(pool_error_to_pyerr)
    }

    fn set_max_lifetime_session(&self, value: u32) -> PyResult<()> {
        self.pool
            .engine
            .set_max_lifetime_session_secs(value)
            .map_err(pool_error_to_pyerr)
    }

    fn get_ping_interval(&self) -> PyResult<i64> {
        self.pool
            .engine
            .ping_interval_secs()
            .map_err(pool_error_to_pyerr)
    }

    fn set_ping_interval(&self, value: i64) -> PyResult<()> {
        self.pool
            .engine
            .set_ping_interval_secs(value)
            .map_err(pool_error_to_pyerr)
    }

    fn get_stmt_cache_size(&self) -> PyResult<u32> {
        Ok(*self.pool.stmt_cache_size.lock().map_err(runtime_error)?)
    }

    fn set_stmt_cache_size(&self, value: u32) -> PyResult<()> {
        *self.pool.stmt_cache_size.lock().map_err(runtime_error)? = value;
        Ok(())
    }

    // Thick-mode-only attributes: reference impl/base/pool.pyx:58-111 raises
    // DPY-3001 and impl/thin/pool.pyx does not override these four methods.
    fn get_max_sessions_per_shard(&self) -> PyResult<u32> {
        Err(raise_not_supported(
            "getting the maximum sessions per shard in a pool",
        ))
    }

    fn set_max_sessions_per_shard(&self, value: u32) -> PyResult<()> {
        let _ = value;
        Err(raise_not_supported(
            "setting the maximum sessions per shard",
        ))
    }

    fn get_soda_metadata_cache(&self) -> PyResult<bool> {
        Err(raise_not_supported(
            "getting whether the SODA metadata cache is enabled",
        ))
    }

    fn set_soda_metadata_cache(&self, value: bool) -> PyResult<()> {
        let _ = value;
        Err(raise_not_supported(
            "setting whether the SODA metadata cache is enabled",
        ))
    }

    fn reconfigure(&self, _min: u32, _max: u32, _increment: u32) -> PyResult<()> {
        Err(raise_not_supported("reconfiguring a pool"))
    }
}

// ---------------------------------------------------------------------------
// Async pool impl: same engine; blocking entry points are awaited through
// dedicated threads so the event loop never blocks.
// ---------------------------------------------------------------------------

#[pyclass(module = "oracledb.thin_impl", name = "AsyncThinPoolImpl")]
pub(crate) struct AsyncThinPoolImpl {
    pool: Arc<ShimPool>,
}

#[pymethods]
impl AsyncThinPoolImpl {
    #[new]
    fn new(dsn: &Bound<'_, PyAny>, params_impl: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            pool: ShimPool::new(dsn, params_impl, true)?,
        })
    }

    #[getter]
    fn dsn(&self) -> &str {
        &self.pool.dsn
    }

    #[getter]
    fn username(&self) -> &str {
        &self.pool.username
    }

    #[getter]
    fn homogeneous(&self) -> bool {
        self.pool.homogeneous
    }

    #[getter]
    fn increment(&self) -> u32 {
        self.pool.increment
    }

    #[getter]
    fn max(&self) -> u32 {
        self.pool.max
    }

    #[getter]
    fn min(&self) -> u32 {
        self.pool.min
    }

    #[getter]
    fn name(&self) -> Option<String> {
        None
    }

    async fn acquire(&self, params_impl: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let opts = Python::attach(|py| extract_acquire_options(params_impl.bind(py)))?;
        let pool = Arc::clone(&self.pool);
        let task = spawn_blocking_task("oracledb-pyshim-pool-acquire", move || {
            Ok::<_, TaskError>(pool.acquire_blocking(opts))
        });
        let id = task
            .await
            .map_err(runtime_error)?
            .map_err(pool_error_to_pyerr)?;
        Python::attach(|py| self.pool.registered_conn(py, id))
    }

    async fn close(&self, force: bool) -> PyResult<()> {
        let pool = Arc::clone(&self.pool);
        let task = spawn_blocking_task("oracledb-pyshim-pool-close", move || {
            Ok::<_, TaskError>(pool.engine.close(force))
        });
        task.await
            .map_err(runtime_error)?
            .map_err(pool_error_to_pyerr)
    }

    async fn drop(&self, conn_impl: Py<PyAny>) -> PyResult<()> {
        let refs = Python::attach(|py| extract_pool_conn_refs(conn_impl.bind(py)))?;
        let pool = Arc::clone(&self.pool);
        let task = spawn_blocking_task("oracledb-pyshim-pool-drop", move || {
            Ok::<_, TaskError>(pool.drop_blocking(refs.0))
        });
        task.await
            .map_err(runtime_error)?
            .map_err(pool_error_to_pyerr)
    }

    #[pyo3(signature = (conn_impl, in_del=false))]
    async fn return_connection(&self, conn_impl: Py<PyAny>, in_del: bool) -> PyResult<()> {
        let refs = Python::attach(|py| extract_pool_conn_refs(conn_impl.bind(py)))?;
        let pool = Arc::clone(&self.pool);
        let task = spawn_blocking_task("oracledb-pyshim-pool-return", move || {
            Ok::<_, TaskError>(pool.release_blocking(&refs, in_del))
        });
        task.await.map_err(runtime_error)?
    }

    fn get_busy_count(&self) -> PyResult<u32> {
        self.pool.engine.busy_count().map_err(pool_error_to_pyerr)
    }

    fn get_open_count(&self) -> PyResult<u32> {
        self.pool.engine.open_count().map_err(pool_error_to_pyerr)
    }

    fn get_getmode(&self) -> PyResult<u32> {
        self.pool.engine.getmode().map_err(pool_error_to_pyerr)
    }

    fn set_getmode(&self, value: u32) -> PyResult<()> {
        self.pool
            .engine
            .set_getmode(value)
            .map_err(pool_error_to_pyerr)
    }

    fn get_wait_timeout(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.pool.wait_timeout_object(py)
    }

    fn set_wait_timeout(&self, value: u32) -> PyResult<()> {
        self.pool
            .engine
            .set_wait_timeout_ms(value)
            .map_err(pool_error_to_pyerr)
    }

    fn get_timeout(&self) -> PyResult<u32> {
        self.pool.engine.timeout_secs().map_err(pool_error_to_pyerr)
    }

    fn set_timeout(&self, value: u32) -> PyResult<()> {
        self.pool
            .engine
            .set_timeout_secs(value)
            .map_err(pool_error_to_pyerr)
    }

    fn get_max_lifetime_session(&self) -> PyResult<u32> {
        self.pool
            .engine
            .max_lifetime_session_secs()
            .map_err(pool_error_to_pyerr)
    }

    fn set_max_lifetime_session(&self, value: u32) -> PyResult<()> {
        self.pool
            .engine
            .set_max_lifetime_session_secs(value)
            .map_err(pool_error_to_pyerr)
    }

    fn get_ping_interval(&self) -> PyResult<i64> {
        self.pool
            .engine
            .ping_interval_secs()
            .map_err(pool_error_to_pyerr)
    }

    fn set_ping_interval(&self, value: i64) -> PyResult<()> {
        self.pool
            .engine
            .set_ping_interval_secs(value)
            .map_err(pool_error_to_pyerr)
    }

    fn get_stmt_cache_size(&self) -> PyResult<u32> {
        Ok(*self.pool.stmt_cache_size.lock().map_err(runtime_error)?)
    }

    fn set_stmt_cache_size(&self, value: u32) -> PyResult<()> {
        *self.pool.stmt_cache_size.lock().map_err(runtime_error)? = value;
        Ok(())
    }

    // Thick-mode-only attributes: reference impl/base/pool.pyx:58-111 raises
    // DPY-3001 and impl/thin/pool.pyx does not override these four methods.
    fn get_max_sessions_per_shard(&self) -> PyResult<u32> {
        Err(raise_not_supported(
            "getting the maximum sessions per shard in a pool",
        ))
    }

    fn set_max_sessions_per_shard(&self, value: u32) -> PyResult<()> {
        let _ = value;
        Err(raise_not_supported(
            "setting the maximum sessions per shard",
        ))
    }

    fn get_soda_metadata_cache(&self) -> PyResult<bool> {
        Err(raise_not_supported(
            "getting whether the SODA metadata cache is enabled",
        ))
    }

    fn set_soda_metadata_cache(&self, value: bool) -> PyResult<()> {
        let _ = value;
        Err(raise_not_supported(
            "setting whether the SODA metadata cache is enabled",
        ))
    }

    fn reconfigure(&self, _min: u32, _max: u32, _increment: u32) -> PyResult<()> {
        Err(raise_not_supported("reconfiguring a pool"))
    }
}
