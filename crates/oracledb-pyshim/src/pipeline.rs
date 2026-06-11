//! Sequential pipeline fallback (reference impl/thin/connection.pyx
//! `run_pipeline_without_pipelining`, lines 1280-1306, and
//! `_run_pipeline_op_without_pipelining`, lines 1045-1090).
//!
//! The fallback runner drives the *public* AsyncConnection/AsyncCursor
//! objects exactly like the reference does, so each operation goes through
//! the shim's normal execute/callproc/callfunc paths. The loop itself is
//! Python coroutine glue (await chains over public objects), so it is
//! expressed as an embedded Python helper; all database work still runs
//! through the Rust shim. True pipelined transport is owned by the
//! PIPELINING cluster (protocol + driver).

use std::ffi::CString;

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyModule;

#[pyclass(module = "oracledb.thin_impl", name = "PipelineOpResultShimImpl")]
pub(crate) struct PipelineOpResultShimImpl {
    #[pyo3(get)]
    operation: Py<PyAny>,
    #[pyo3(get, set)]
    error: Option<Py<PyAny>>,
    #[pyo3(get, set)]
    warning: Option<Py<PyAny>>,
    #[pyo3(get, set)]
    rows: Option<Py<PyAny>>,
    #[pyo3(get, set)]
    return_value: Option<Py<PyAny>>,
    #[pyo3(get, set)]
    fetch_metadata: Option<Py<PyAny>>,
}

#[pymethods]
impl PipelineOpResultShimImpl {
    #[new]
    fn new(operation: Py<PyAny>) -> Self {
        Self {
            operation,
            error: None,
            warning: None,
            rows: None,
            return_value: None,
            fetch_metadata: None,
        }
    }
}

const PIPELINE_RUNNER_SOURCE: &str = r#"
import oracledb
from oracledb import errors

_OP_CALL_FUNC = 1
_OP_CALL_PROC = 2
_OP_COMMIT = 3
_OP_EXECUTE = 4
_OP_EXECUTE_MANY = 5
_OP_FETCH_ALL = 6
_OP_FETCH_MANY = 7
_OP_FETCH_ONE = 8


async def _run_op(conn, op, shim):
    # mirrors impl/thin/connection.pyx _run_pipeline_op_without_pipelining
    if op.op_type == _OP_COMMIT:
        await conn.commit()
        return
    cursor = conn.cursor()
    if op.op_type == _OP_CALL_FUNC:
        shim.return_value = await cursor.callfunc(
            op.name, op.return_type, op.parameters, op.keyword_parameters
        )
    elif op.op_type == _OP_CALL_PROC:
        await cursor.callproc(op.name, op.parameters, op.keyword_parameters)
    elif op.op_type == _OP_EXECUTE:
        await cursor.execute(op.statement, op.parameters)
    elif op.op_type == _OP_EXECUTE_MANY:
        await cursor.executemany(op.statement, op.parameters)
    elif op.op_type == _OP_FETCH_ALL:
        await cursor.execute(op.statement, op.parameters)
        cursor.rowfactory = op.rowfactory
        shim.rows = await cursor.fetchall()
    elif op.op_type == _OP_FETCH_MANY:
        await cursor.execute(op.statement, op.parameters)
        cursor.rowfactory = op.rowfactory
        shim.rows = await cursor.fetchmany(op.num_rows)
    elif op.op_type == _OP_FETCH_ONE:
        await cursor.execute(op.statement, op.parameters)
        cursor.rowfactory = op.rowfactory
        shim.rows = await cursor.fetchmany(1)
    else:
        errors._raise_err(
            errors.ERR_UNSUPPORTED_PIPELINE_OPERATION, op_type=op.op_type
        )
    shim.warning = cursor.warning
    metadata = cursor._impl.fetch_metadata
    shim.fetch_metadata = metadata if metadata else None


def _capture_err(shim, exc):
    # mirrors impl/base/pipeline.pyx PipelineOpResultImpl._capture_err
    if isinstance(exc, oracledb.Error):
        shim.error = exc.args[0]
    else:
        shim.error = errors._create_err(
            errors.ERR_UNEXPECTED_PIPELINE_FAILURE, cause=exc
        )


async def run_pipeline_without_pipelining(
    result_shim_factory, conn, results, continue_on_error
):
    call_timeout = conn._impl.get_call_timeout()
    try:
        for result in results:
            op = result._impl.operation
            shim = result_shim_factory(op)
            result._impl = shim
            try:
                await _run_op(conn, op, shim)
            except Exception as exc:
                if not continue_on_error:
                    raise
                _capture_err(shim, exc)
    finally:
        conn._impl.set_call_timeout(call_timeout)
"#;

static PIPELINE_RUNNER_MODULE: PyOnceLock<Py<PyModule>> = PyOnceLock::new();

pub(crate) fn pipeline_runner_function<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    let module = PIPELINE_RUNNER_MODULE.get_or_try_init(py, || {
        let source = CString::new(PIPELINE_RUNNER_SOURCE)
            .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
        let file_name = CString::new("oracledb_pyshim_pipeline.py")
            .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
        let module_name = CString::new("oracledb_pyshim_pipeline")
            .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
        PyModule::from_code(py, &source, &file_name, &module_name).map(Bound::unbind)
    })?;
    module
        .bind(py)
        .getattr("run_pipeline_without_pipelining")
        .map(Bound::into_any)
}
