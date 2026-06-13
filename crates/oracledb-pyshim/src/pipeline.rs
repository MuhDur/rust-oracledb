use std::ffi::CString;

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyModule;

/// Sequential pipeline runner mirroring the reference thin driver's
/// `run_pipeline_without_pipelining` path (impl/thin/connection.pyx:1280-1304
/// wrapping `_run_pipeline_op_without_pipelining`, :1045-1089). The public
/// `AsyncConnection.run_pipeline` routes here whenever the conn impl reports
/// `supports_pipelining() == False` — the same path the reference itself takes
/// against servers without END_OF_RESPONSE support.
///
/// Each public `PipelineOpResult` arrives holding the genuine Cython
/// `PipelineOpResultImpl`, whose result attributes are readonly cdef fields
/// (base_impl.pxd) that only Cython code can assign. The runner therefore
/// substitutes a shim-owned, attribute-compatible result impl on
/// `result._impl` (a plain writable attribute of the pure-Python result
/// object) before running any operation.
///
/// Per-op execution is delegated to the unmodified public async cursor API,
/// which lands in this shim's native Rust execute/fetch paths; this module is
/// orchestration glue only.
const PIPELINE_RUNNER_SOURCE: &str = r#"
from oracledb import errors, exceptions
from oracledb.enums import PipelineOpType


class PipelineOpResultImpl:
    """Writable stand-in for the readonly Cython PipelineOpResultImpl."""

    def __init__(self, operation):
        self.operation = operation
        self.return_value = None
        self.rows = None
        self.error = None
        self.warning = None
        self.fetch_metadata = None

    def _capture_err(self, exc):
        # parity with impl/base/pipeline.pyx PipelineOpResultImpl._capture_err
        if isinstance(exc, exceptions.Error):
            self.error = exc.args[0]
        else:
            self.error = errors._create_err(
                errors.ERR_UNEXPECTED_PIPELINE_FAILURE, cause=exc
            )


async def _run_op(conn, result_impl):
    op_impl = result_impl.operation
    op_type = op_impl.op_type
    if op_type == PipelineOpType.COMMIT:
        await conn.commit()
        return
    cursor = conn.cursor()
    if op_type == PipelineOpType.CALL_FUNC:
        result_impl.return_value = await cursor.callfunc(
            op_impl.name,
            op_impl.return_type,
            op_impl.parameters,
            op_impl.keyword_parameters,
        )
    elif op_type == PipelineOpType.CALL_PROC:
        await cursor.callproc(
            op_impl.name, op_impl.parameters, op_impl.keyword_parameters
        )
    elif op_type == PipelineOpType.EXECUTE:
        await cursor.execute(op_impl.statement, op_impl.parameters)
    elif op_type == PipelineOpType.EXECUTE_MANY:
        await cursor.executemany(op_impl.statement, op_impl.parameters)
    elif op_type in (
        PipelineOpType.FETCH_ALL,
        PipelineOpType.FETCH_MANY,
        PipelineOpType.FETCH_ONE,
    ):
        if op_type == PipelineOpType.FETCH_ONE:
            num_rows = 1
        elif op_type == PipelineOpType.FETCH_MANY:
            num_rows = op_impl.num_rows
        else:
            num_rows = op_impl.arraysize
        # the reference copies these op attributes onto the cursor impl in its
        # with-pipelining message builder (impl/thin/connection.pyx
        # _create_message_for_pipeline_op) AFTER _prepare_for_execute, which
        # resets the fetch flags from oracledb.defaults (impl/base/cursor.pyx
        # 420-421). The tests assert that observable behavior, so the
        # sequential runner routes the op fetch flags through the public
        # execute() keywords, which the genuine cursor.py applies after
        # prepare (cursor.py async execute, fetch_lobs/fetch_decimals).
        cursor._impl.prefetchrows = num_rows
        cursor._impl.arraysize = num_rows
        await cursor.execute(
            op_impl.statement,
            op_impl.parameters,
            fetch_lobs=op_impl.fetch_lobs,
            fetch_decimals=op_impl.fetch_decimals,
        )
        cursor.rowfactory = op_impl.rowfactory
        if op_type == PipelineOpType.FETCH_ALL:
            result_impl.rows = await cursor.fetchall()
        elif op_type == PipelineOpType.FETCH_MANY:
            result_impl.rows = await cursor.fetchmany(num_rows)
        else:
            result_impl.rows = await cursor.fetchmany(1)
    else:
        errors._raise_err(
            errors.ERR_UNSUPPORTED_PIPELINE_OPERATION, op_type=op_type
        )
    result_impl.warning = cursor.warning
    # the genuine cursor impl reports None for non-queries; this shim's
    # fetch_metadata getter reports an empty list, which PipelineOpResult
    # .columns would render as [] instead of None
    fetch_metadata = cursor._impl.fetch_metadata
    result_impl.fetch_metadata = fetch_metadata if fetch_metadata else None


async def run_pipeline_sequential(conn, results, continue_on_error):
    conn_impl = conn._impl
    call_timeout = conn_impl.get_call_timeout()
    for result in results:
        if not isinstance(result._impl, PipelineOpResultImpl):
            result._impl = PipelineOpResultImpl(result._impl.operation)
    try:
        for result in results:
            try:
                await _run_op(conn, result._impl)
            except Exception as exc:
                if not continue_on_error:
                    raise
                result._impl._capture_err(exc)
    finally:
        conn_impl.set_call_timeout(call_timeout)
"#;

static PIPELINE_RUNNER: PyOnceLock<Py<PyModule>> = PyOnceLock::new();

fn pipeline_runner<'py>(py: Python<'py>) -> PyResult<&'py Bound<'py, PyModule>> {
    Ok(PIPELINE_RUNNER
        .get_or_try_init(py, || -> PyResult<Py<PyModule>> {
            let code = CString::new(PIPELINE_RUNNER_SOURCE)
                .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
            let module = PyModule::from_code(
                py,
                code.as_c_str(),
                c"oracledb_pyshim/_pipeline_runner.py",
                c"oracledb_pyshim._pipeline_runner",
            )?;
            Ok(module.unbind())
        })?
        .bind(py))
}

/// Returns the coroutine object for the sequential runner; the public layer
/// awaits it on its own event loop.
pub(crate) fn run_pipeline_sequential(
    py: Python<'_>,
    conn: Py<PyAny>,
    results: Py<PyAny>,
    continue_on_error: bool,
) -> PyResult<Py<PyAny>> {
    Ok(pipeline_runner(py)?
        .getattr("run_pipeline_sequential")?
        .call1((conn, results, continue_on_error))?
        .unbind())
}
