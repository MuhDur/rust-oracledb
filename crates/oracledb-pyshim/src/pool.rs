use std::sync::{Arc, Mutex};

use pyo3::prelude::*;

use crate::*;

#[pyclass(module = "oracledb.thin_impl", name = "ThinPoolImpl")]
// d49: migrate to oracledb (driver pool module); shim keeps marshalling only
pub(crate) struct ThinPoolImpl {
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
    opened: Arc<Mutex<bool>>,
    open_count: Arc<Mutex<u32>>,
    busy_count: Arc<Mutex<u32>>,
    ping_interval: u32,
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
        let ping_interval = get_optional_u32_attr(params_impl, "ping_interval")?.unwrap_or(60);
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
            opened: Arc::new(Mutex::new(true)),
            open_count: Arc::new(Mutex::new(0)),
            busy_count: Arc::new(Mutex::new(0)),
            ping_interval,
            stmt_cache_size,
            timeout,
            wait_timeout,
        })
    }

    fn acquire(&self, params_impl: &Bound<'_, PyAny>) -> PyResult<()> {
        if !*self.opened.lock().map_err(runtime_error)? {
            return Err(raise_oracledb_driver_error("ERR_POOL_NOT_OPEN"));
        }
        // session tagging has not been implemented yet
        // (reference impl/thin/pool.pyx:631-633)
        if !params_impl.getattr("tag")?.is_none() {
            return Err(raise_not_supported("session tagging"));
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

    // Thick-mode-only attributes: reference impl/base/pool.pyx:58-111 raises
    // DPY-3001 and impl/thin/pool.pyx does not override these four methods.
    fn get_max_sessions_per_shard(&self) -> PyResult<u32> {
        Err(raise_not_supported(
            "getting the maximum sessions per shard in a pool",
        ))
    }

    fn get_open_count(&self) -> PyResult<u32> {
        Ok(*self.open_count.lock().map_err(runtime_error)?)
    }

    fn get_ping_interval(&self) -> u32 {
        self.ping_interval
    }

    fn get_soda_metadata_cache(&self) -> PyResult<bool> {
        Err(raise_not_supported(
            "getting whether the SODA metadata cache is enabled",
        ))
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

    fn set_max_sessions_per_shard(&mut self, value: u32) -> PyResult<()> {
        let _ = value;
        Err(raise_not_supported(
            "setting the maximum sessions per shard",
        ))
    }

    fn set_ping_interval(&mut self, value: u32) {
        self.ping_interval = value;
    }

    fn set_soda_metadata_cache(&mut self, value: bool) -> PyResult<()> {
        let _ = value;
        Err(raise_not_supported(
            "setting whether the SODA metadata cache is enabled",
        ))
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
pub(crate) struct AsyncThinPoolImpl {
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

    async fn acquire(&self, params_impl: Py<PyAny>) -> PyResult<()> {
        if !*self.opened.lock().map_err(runtime_error)? {
            return Err(raise_oracledb_driver_error("ERR_POOL_NOT_OPEN"));
        }
        // session tagging has not been implemented yet
        // (reference impl/thin/pool.pyx:827-829)
        let tag_set = Python::attach(|py| -> PyResult<bool> {
            Ok(!params_impl.bind(py).getattr("tag")?.is_none())
        })?;
        if tag_set {
            return Err(raise_not_supported("session tagging"));
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
