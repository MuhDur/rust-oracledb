#![forbid(unsafe_code)]

use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;

fn not_implemented(name: &str) -> PyErr {
    PyNotImplementedError::new_err(format!(
        "{name} is a Rust shim placeholder; M1+ must route this through the oracledb crate"
    ))
}

#[pyfunction]
fn init_thin_impl(_package: &Bound<'_, PyAny>) -> PyResult<()> {
    Ok(())
}

#[pyclass(module = "oracledb.thin_impl", name = "ThinConnImpl")]
#[derive(Default)]
struct ThinConnImpl;

#[pymethods]
impl ThinConnImpl {
    #[new]
    fn new(_dsn: &Bound<'_, PyAny>, _params_impl: &Bound<'_, PyAny>) -> Self {
        Self
    }

    fn connect(&self, _params_impl: &Bound<'_, PyAny>) -> PyResult<()> {
        Err(not_implemented("ThinConnImpl.connect"))
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
    m.add_class::<AsyncThinConnImpl>()?;
    m.add_class::<ThinPoolImpl>()?;
    m.add_class::<AsyncThinPoolImpl>()?;
    m.add_class::<EndUserSecurityContextImpl>()?;
    Ok(())
}
