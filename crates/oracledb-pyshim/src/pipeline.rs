use std::ffi::CString;
use std::sync::{Arc, Mutex};

use oracledb::protocol::thin::{BindValue, QueryResult};
use oracledb::{Connection as RustConnection, Error as DriverError, PipelineRequest};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyList, PyModule};

use crate::{
    columns_require_define, query_result_warning, raise_task_error, AsyncThinCursorImpl, TaskError,
};

/// Test/diagnostic override for which pipeline path runs, and a log of which
/// path the last `run_pipeline_with_pipelining` actually took. The override is
/// `"auto"` (honour the native-vs-fallback gate), `"native"` (force the native
/// single-round-trip path; a non-native op still falls back per-pipeline so
/// parity holds), or `"sequential"` (force the per-op fallback). These exist so
/// the byte-identical regression test can drive the same batch down both paths
/// and confirm the native path actually ran.
static FORCE_PIPELINE_PATH: Mutex<Option<String>> = Mutex::new(None);
static LAST_PIPELINE_PATH: Mutex<Option<String>> = Mutex::new(None);

pub(crate) fn set_force_pipeline_path(mode: Option<String>) {
    *FORCE_PIPELINE_PATH.lock().expect("force-path mutex") = mode;
}

pub(crate) fn force_pipeline_path() -> Option<String> {
    FORCE_PIPELINE_PATH
        .lock()
        .expect("force-path mutex")
        .clone()
}

pub(crate) fn record_pipeline_path(path: &str) {
    *LAST_PIPELINE_PATH.lock().expect("path-log mutex") = Some(path.to_string());
}

pub(crate) fn last_pipeline_path() -> Option<String> {
    LAST_PIPELINE_PATH.lock().expect("path-log mutex").clone()
}

pub(crate) fn reset_pipeline_path_log() {
    *LAST_PIPELINE_PATH.lock().expect("path-log mutex") = None;
}

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


# --- native single-round-trip pipelining --------------------------------
#
# The reference's run_pipeline_with_pipelining (impl/thin/connection.pyx:1258)
# reuses the SAME per-op cursor/decoder objects that ordinary execute/fetch
# use: it builds N messages, sends them all in one round trip
# (protocol.end_pipeline), then finishes each op (_complete_pipeline_op). This
# shim has no Cython Message objects, so it mirrors that flow in three phases:
#
#   1. PREPARE (no wire): build one cursor per non-commit op and run the
#      reference _prepare_for_execute / _call_get_execute_args exactly as the
#      sequential runner's _run_op does (input type handlers, var binds, fetch
#      flags). This populates each cursor's bind_values / statement.
#   2. SINGLE ROUND TRIP: hand the prepared cursors to the Rust hook
#      conn_impl._pipeline_native_execute, which drives the native
#      Connection::run_pipeline (BEGIN_PIPELINE piggyback, END_OF_REQUEST
#      framing, FUNC 200 end-pipeline, N+1 boundary-delimited reads) and feeds
#      each boundary-delimited response back through the SAME decoders the
#      ordinary execute path uses (parse_query_response_*), populating each
#      cursor's rows/columns/cursor_id/rowcount — no decode reimplementation.
#   3. MATERIALIZE (tail round trips only as the reference also makes): read
#      each op's result through the public cursor API (fetchall/fetchone/
#      fetchmany with rowfactory + LOB materialization), so the per-op result
#      attributes are byte-identical to the sequential runner.
#
# Only the all-simple op subset (execute / executemany of array DML / commit /
# fetch of scalar binds) runs natively. Any op needing connection state
# interleaved with decoding — callfunc/callproc (OUT binds, ref cursors),
# PL/SQL execute with binds (OUT binds, ref/nested cursors), DML returning —
# routes the WHOLE pipeline to the sequential fallback so the single-round-trip
# property and exact parity both hold.

_NATIVE_OP_TYPES = frozenset(
    (
        PipelineOpType.COMMIT,
        PipelineOpType.EXECUTE,
        PipelineOpType.EXECUTE_MANY,
        PipelineOpType.FETCH_ALL,
        PipelineOpType.FETCH_MANY,
        PipelineOpType.FETCH_ONE,
    )
)


def _is_plsql(statement):
    if statement is None:
        return True
    s = statement.lstrip().lstrip("(").lstrip()
    head = s[:8].lower()
    return head.startswith("begin") or head.startswith("declare")


def _scalar_param(value):
    # A bind the native path can serialize without a Var/cursor/LOB object.
    # Vars (OUT/IN-OUT binds, ref cursors), nested cursors and LOB objects all
    # require the connection interleaved with decoding -> not native.
    return isinstance(
        value, (type(None), bool, int, float, str, bytes, bytearray)
    ) or hasattr(value, "isoformat")  # datetime/date


def _params_are_scalar(parameters):
    if parameters is None:
        return True
    if isinstance(parameters, dict):
        values = parameters.values()
    elif isinstance(parameters, (list, tuple)):
        # executemany: list of rows OR a single row
        if parameters and isinstance(parameters[0], (list, tuple, dict)):
            rows = parameters
        else:
            rows = [parameters]
        values = []
        for row in rows:
            values.extend(row.values() if isinstance(row, dict) else row)
    elif isinstance(parameters, int):
        return True  # executemany with iteration count
    else:
        return False
    return all(_scalar_param(v) for v in values)


def _op_is_native(op_impl):
    op_type = op_impl.op_type
    if op_type not in _NATIVE_OP_TYPES:
        return False
    if op_type == PipelineOpType.COMMIT:
        return True
    if _is_plsql(op_impl.statement):
        return False
    return _params_are_scalar(op_impl.parameters)


def _pipeline_is_native(results):
    return all(_op_is_native(r._impl.operation) for r in results)


async def _prepare_native_op(conn, result_impl):
    """Phase 1: build + prepare the cursor for one op without any wire I/O.

    Returns (cursor, op_type, num_rows) or (None, COMMIT, 0) for commit.
    """
    op_impl = result_impl.operation
    op_type = op_impl.op_type
    if op_type == PipelineOpType.COMMIT:
        return None, op_type, 0
    cursor = conn.cursor()
    if op_type == PipelineOpType.EXECUTE:
        cursor._prepare_for_execute(op_impl.statement, op_impl.parameters)
        num_rows = 0
    elif op_type == PipelineOpType.EXECUTE_MANY:
        # populate cursor._impl.many_bind_rows; array DML is sent as one message.
        cursor._impl._prepare_for_executemany(
            cursor, op_impl.statement, op_impl.parameters, 2 ** 32 - 1
        )
        num_rows = 0
    else:  # FETCH_ONE / FETCH_MANY / FETCH_ALL
        if op_type == PipelineOpType.FETCH_ONE:
            num_rows = 1
        elif op_type == PipelineOpType.FETCH_MANY:
            num_rows = op_impl.num_rows
        else:
            num_rows = op_impl.arraysize
        cursor._impl.prefetchrows = num_rows
        cursor._impl.arraysize = num_rows
        cursor._impl.fetch_lobs = op_impl.fetch_lobs
        cursor._impl.fetch_decimals = op_impl.fetch_decimals
        cursor._prepare_for_execute(op_impl.statement, op_impl.parameters)
        cursor._impl.prefetchrows = num_rows
        cursor._impl.arraysize = num_rows
        cursor._impl.fetch_lobs = op_impl.fetch_lobs
        cursor._impl.fetch_decimals = op_impl.fetch_decimals
        cursor.rowfactory = op_impl.rowfactory
    return cursor, op_type, num_rows


async def _materialize_native_op(result_impl, cursor, op_type, num_rows):
    """Phase 3: read one op's result through the public cursor API, after the
    native round trip has populated the cursor impl with decoded data."""
    if op_type == PipelineOpType.COMMIT:
        return
    if op_type == PipelineOpType.EXECUTE_MANY:
        op_impl = result_impl.operation
        # the reference applies executemany iteration count via the batch load
        # manager; for array DML the native single message already executed
        # every iteration, so nothing else is required here.
        pass
    if op_type == PipelineOpType.FETCH_ALL:
        result_impl.rows = await cursor.fetchall()
    elif op_type == PipelineOpType.FETCH_MANY:
        result_impl.rows = await cursor.fetchmany(num_rows)
    elif op_type == PipelineOpType.FETCH_ONE:
        result_impl.rows = await cursor.fetchmany(1)
    result_impl.warning = cursor.warning
    fetch_metadata = cursor._impl.fetch_metadata
    result_impl.fetch_metadata = fetch_metadata if fetch_metadata else None


async def run_pipeline_native(conn, results, continue_on_error):
    conn_impl = conn._impl
    call_timeout = conn_impl.get_call_timeout()
    for result in results:
        if not isinstance(result._impl, PipelineOpResultImpl):
            result._impl = PipelineOpResultImpl(result._impl.operation)

    forced = _force_pipeline_path()  # injected by the Rust module builder
    # autocommit is implemented as a separate post-execute commit round trip in
    # this shim, which a single-round-trip batch cannot express -> fall back.
    autocommit = conn_impl.get_autocommit_for_pipeline()
    if (
        forced == "sequential"
        or autocommit
        or not _pipeline_is_native(results)
    ):
        _record_pipeline_path("sequential")
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
        return

    _record_pipeline_path("native")
    try:
        # Phase 1: prepare every op's cursor (no wire I/O).
        prepared = []
        for result in results:
            cursor, op_type, num_rows = await _prepare_native_op(
                conn, result._impl
            )
            prepared.append((result, cursor, op_type, num_rows))

        # Phase 2: one round trip. The Rust hook drives Connection::run_pipeline
        # and decodes each response onto the matching cursor impl; it returns a
        # per-op error (or None) so we can apply abort/continue semantics here.
        cursors = [cursor for (_r, cursor, _t, _n) in prepared]
        op_errors = await conn_impl._pipeline_native_execute(
            cursors, continue_on_error
        )

        # Phase 3: per-op error attribution + result materialization.
        for (result, cursor, op_type, num_rows), op_error in zip(
            prepared, op_errors
        ):
            if op_error is not None:
                if not continue_on_error:
                    raise op_error
                result._impl._capture_err(op_error)
                continue
            try:
                await _materialize_native_op(
                    result._impl, cursor, op_type, num_rows
                )
            except Exception as exc:
                if not continue_on_error:
                    raise
                result._impl._capture_err(exc)
    finally:
        conn_impl.set_call_timeout(call_timeout)
"#;

static PIPELINE_RUNNER: PyOnceLock<Py<PyModule>> = PyOnceLock::new();

#[pyfunction]
fn _record_pipeline_path(path: &str) {
    record_pipeline_path(path);
}

#[pyfunction]
fn _force_pipeline_path() -> Option<String> {
    force_pipeline_path()
}

/// Diagnostic: force which async pipeline path runs. `"auto"` clears the
/// override (honour the native-vs-fallback gate); `"native"` / `"sequential"`
/// pin the path. Used by the byte-identical regression test.
#[pyfunction]
#[pyo3(name = "set_force_pipeline_path")]
pub(crate) fn set_force_pipeline_path_py(mode: &str) -> PyResult<()> {
    match mode {
        "auto" => set_force_pipeline_path(None),
        "native" | "sequential" => set_force_pipeline_path(Some(mode.to_string())),
        other => {
            return Err(PyRuntimeError::new_err(format!(
                "unknown pipeline path mode: {other:?} (expected auto/native/sequential)"
            )))
        }
    }
    Ok(())
}

/// Diagnostic: which path the last `run_pipeline_with_pipelining` actually took
/// (`"native"` or `"sequential"`), or `None` if none has run since the last
/// reset.
#[pyfunction]
#[pyo3(name = "last_pipeline_path")]
pub(crate) fn last_pipeline_path_py() -> Option<String> {
    last_pipeline_path()
}

/// Diagnostic: clear the last-path log.
#[pyfunction]
#[pyo3(name = "reset_pipeline_path_log")]
pub(crate) fn reset_pipeline_path_log_py() {
    reset_pipeline_path_log();
}

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
            // Inject the Rust path-log + force-path hooks so the runner can
            // honour the diagnostic override and record which path it took.
            module.add_function(wrap_pyfunction!(_record_pipeline_path, &module)?)?;
            module.add_function(wrap_pyfunction!(_force_pipeline_path, &module)?)?;
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

/// Returns the coroutine object for the native single-round-trip runner (which
/// itself falls back to the sequential path per-pipeline for non-native ops).
pub(crate) fn run_pipeline_native(
    py: Python<'_>,
    conn: Py<PyAny>,
    results: Py<PyAny>,
    continue_on_error: bool,
) -> PyResult<Py<PyAny>> {
    Ok(pipeline_runner(py)?
        .getattr("run_pipeline_native")?
        .call1((conn, results, continue_on_error))?
        .unbind())
}

// --- Rust side of the native runner (phases 2a / 2c) ---------------------

/// Per-op plan produced by [`build_native_plan`]: the wire request in token
/// order. A `Commit` request marks a COMMIT op (no cursor to populate).
pub(crate) struct NativePlan {
    pub(crate) requests: Vec<PipelineRequest>,
}

/// Phase 2a (GIL held): read each prepared cursor's statement + bind values and
/// build the wire `PipelineRequest` for it. A `None` cursor is a COMMIT op.
pub(crate) fn build_native_plan(
    py: Python<'_>,
    cursors: &[Option<Py<PyAny>>],
) -> PyResult<NativePlan> {
    let mut requests = Vec::with_capacity(cursors.len());
    for cursor in cursors {
        let request = match cursor {
            None => PipelineRequest::Commit,
            Some(cursor) => {
                let impl_obj = cursor.bind(py).getattr("_impl")?;
                let cursor_impl = impl_obj.extract::<PyRef<'_, AsyncThinCursorImpl>>()?;
                let inner = &cursor_impl.inner;
                let sql = inner
                    .statement
                    .clone()
                    .ok_or_else(|| PyRuntimeError::new_err("pipeline op has no statement"))?;
                // EXECUTE_MANY populated many_bind_rows; EXECUTE populated a
                // single bind_values row. An empty bind list is a no-bind op.
                let bind_rows: Vec<Vec<BindValue>> = if !inner.many_bind_rows.is_empty() {
                    inner.many_bind_rows.clone()
                } else if inner.bind_values.is_empty() {
                    Vec::new()
                } else {
                    vec![inner.bind_values.clone()]
                };
                PipelineRequest::Execute {
                    sql,
                    bind_rows,
                    prefetch_rows: inner.prefetchrows.max(1),
                }
            }
        };
        requests.push(request);
    }
    Ok(NativePlan { requests })
}

/// Phase 2c (GIL held): populate each op's cursor impl from its decoded
/// `QueryResult`, mirroring the async `execute()` materialization, and return a
/// Python list of per-op errors (`None` for success). The Python runner then
/// reads each op's result through the public cursor API.
pub(crate) fn populate_native_results(
    py: Python<'_>,
    cursors: &[Option<Py<PyAny>>],
    connection: &Arc<Mutex<Option<RustConnection>>>,
    decoded: Vec<Result<QueryResult, DriverError>>,
) -> PyResult<Py<PyAny>> {
    let errors = PyList::empty(py);
    for (cursor, outcome) in cursors.iter().zip(decoded) {
        match outcome {
            Err(err) => {
                // Build the proper Python DatabaseError (carries full_code) for
                // this op; the Python runner raises or captures it.
                let pyerr = raise_task_error(&TaskError::from(err), connection);
                errors.append(pyerr.into_value(py))?;
            }
            Ok(result) => {
                if let Some(cursor) = cursor {
                    populate_cursor_from_result(py, cursor, result)?;
                }
                errors.append(py.None())?;
            }
        }
    }
    Ok(errors.into_any().unbind())
}

/// Materialize a decoded `QueryResult` onto a cursor impl, mirroring the async
/// `execute()` tail so the public fetch/rowcount path observes identical state.
fn populate_cursor_from_result(
    py: Python<'_>,
    cursor: &Py<PyAny>,
    mut result: QueryResult,
) -> PyResult<()> {
    let impl_obj = cursor.bind(py).getattr("_impl")?;
    let mut cursor_impl = impl_obj.extract::<PyRefMut<'_, AsyncThinCursorImpl>>()?;
    let warning = query_result_warning(py, &result)?;
    let inner = &mut cursor_impl.inner;
    inner.warning = warning;
    inner.record_implicit_resultsets(&mut result);
    let is_query = !result.columns.is_empty();
    inner.columns = result.columns;
    inner.reset_fetch_define_state();
    inner.requires_define = columns_require_define(&inner.columns);
    let execute_returned_rows = !result.rows.is_empty();
    inner.rows = result.rows;
    inner.row_index = 0;
    inner.cursor_id = result.cursor_id;
    inner.more_rows = result.more_rows;
    if execute_returned_rows && inner.requires_define {
        inner.requires_define = false;
    }
    inner.invalid_ref_cursor = false;
    inner.last_rowid = result.last_rowid;
    inner.rowcount = if is_query {
        0
    } else {
        i64::try_from(result.row_count).unwrap_or(i64::MAX)
    };
    inner.refresh_buffer_window();
    inner.is_query = is_query;
    Ok(())
}
