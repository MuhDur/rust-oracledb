use oracledb::protocol::thin::ColumnMetadata;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::*;

/// Stand-in for the public cursor handed to output type handlers.
///
/// The reference implementation passes the real cursor, but the shim cannot:
/// handlers run while `ThinCursorImpl.execute(&mut self)` holds the pyo3
/// borrow, so any handler touching `cursor._impl` (e.g. `cursor.arraysize`)
/// would die with "Already mutably borrowed". The proxy snapshots the state
/// handlers may read and delegates `var()` to the reference `Cursor.var`
/// unbound method so the full public keyword surface (`typename`,
/// `encoding_errors`/`encodingErrors`, DPY-2014/DPY-2037 validation) behaves
/// exactly like the reference.
#[pyclass(module = "oracledb.thin_impl", name = "FetchHandlerCursor")]
pub(crate) struct FetchHandlerCursor {
    pub(crate) connection: Py<PyAny>,
    pub(crate) arraysize: u32,
}

#[pymethods]
impl FetchHandlerCursor {
    #[getter]
    fn arraysize(&self) -> u32 {
        self.arraysize
    }

    #[getter]
    fn connection(&self, py: Python<'_>) -> Py<PyAny> {
        self.connection.clone_ref(py)
    }

    fn _verify_open(&self) {}

    #[getter]
    fn _impl(&self, py: Python<'_>) -> PyResult<Py<FetchHandlerVarFactory>> {
        Py::new(py, FetchHandlerVarFactory)
    }

    #[pyo3(signature = (*args, **kwargs))]
    fn var(
        slf: &Bound<'_, Self>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let public_var = PyModule::import(py, "oracledb.cursor")?
            .getattr("Cursor")?
            .getattr("var")?;
        let mut call_args: Vec<Py<PyAny>> = Vec::with_capacity(args.len() + 1);
        call_args.push(slf.clone().into_any().unbind());
        call_args.extend(args.iter().map(|arg| arg.unbind()));
        let call_args = PyTuple::new(py, call_args)?;
        Ok(public_var.call(call_args, kwargs)?.unbind())
    }
}

/// Minimal `cursor._impl` stand-in exposing `create_var` for the public
/// `Cursor.var` code path used by `FetchHandlerCursor.var`.
#[pyclass(module = "oracledb.thin_impl", name = "FetchHandlerVarFactory")]
pub(crate) struct FetchHandlerVarFactory;

#[pymethods]
impl FetchHandlerVarFactory {
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
    #[allow(clippy::too_many_arguments)] // mirrors the reference create_var signature
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
}

/// Resolves the active input type handler: a handler set on the cursor
/// beats one set on the connection, mirroring the reference
/// `_get_input_type_handler` (reference impl/base/cursor.pyx:285-295).
pub(crate) fn active_input_type_handler(
    py: Python<'_>,
    cursor: &Bound<'_, PyAny>,
    cursor_level: Option<&Py<PyAny>>,
) -> PyResult<Option<Py<PyAny>>> {
    if let Some(handler) = cursor_level {
        return Ok(Some(handler.clone_ref(py)));
    }
    let conn_impl = cursor.getattr("connection")?.getattr("_impl")?;
    if let Ok(conn_impl) = conn_impl.extract::<PyRef<'_, ThinConnImpl>>() {
        return Ok(conn_impl
            .inputtypehandler
            .as_ref()
            .map(|handler| handler.clone_ref(py)));
    }
    if let Ok(conn_impl) = conn_impl.extract::<PyRef<'_, AsyncThinConnImpl>>() {
        return Ok(conn_impl
            .inner
            .inputtypehandler
            .as_ref()
            .map(|handler| handler.clone_ref(py)));
    }
    Ok(None)
}

/// Calls the input type handler for a single bind value, mirroring the
/// reference `_set_by_value` handler protocol
/// (reference impl/base/bind_var.pyx:146-162): values that already are
/// variables bypass the handler, a non-None handler result must be a Var
/// (DPY-2015 otherwise), and the original value is pushed through the
/// returned variable's checked setter so the inconverter and `_check_value`
/// coercion apply. Returns the replacement object or `None` when default
/// processing should continue.
fn input_handler_substitute<'py>(
    py: Python<'py>,
    handler: &Bound<'py, PyAny>,
    proxy: &Bound<'py, PyAny>,
    value: &Bound<'py, PyAny>,
    num_elements: u32,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    if thin_var_from_value(value)?.is_some() {
        return Ok(None);
    }
    // a non-callable handler raises TypeError here, exactly like the
    // reference which calls the handler unconditionally
    let result = handler.call1((proxy, value, num_elements))?;
    if result.is_none() {
        return Ok(None);
    }
    let Some(var) = thin_var_from_value(&result)? else {
        return Err(raise_oracledb_driver_error("ERR_EXPECTING_VAR"));
    };
    var.borrow(py).check_and_set_value(py, 0, value)?;
    Ok(Some(result))
}

/// Applies the input type handler across positional or named bind
/// parameters, substituting handler-returned variables for raw values.
/// Returns `None` when nothing was substituted (including parameter shapes
/// that are rejected later by bind extraction, so the error origin stays
/// unchanged).
pub(crate) fn apply_input_type_handler<'py>(
    py: Python<'py>,
    cursor: &Bound<'py, PyAny>,
    handler: &Bound<'py, PyAny>,
    arraysize: u32,
    parameters: Option<&Bound<'py, PyAny>>,
    num_elements: u32,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    let Some(parameters) = parameters else {
        return Ok(None);
    };
    if parameters.is_none() {
        return Ok(None);
    }
    // the same proxy used for output type handlers stands in for the public
    // cursor: the real cursor's `_impl` is mutably borrowed while binds are
    // prepared, and handlers only need `var()`/`arraysize`/`connection`
    let proxy = Py::new(
        py,
        FetchHandlerCursor {
            connection: cursor.getattr("connection")?.unbind(),
            arraysize,
        },
    )?
    .into_bound(py)
    .into_any();
    if let Ok(dict) = parameters.cast::<PyDict>() {
        let substituted = PyDict::new(py);
        for (key, value) in dict.iter() {
            match input_handler_substitute(py, handler, &proxy, &value, num_elements)? {
                Some(replacement) => substituted.set_item(key, replacement)?,
                None => substituted.set_item(key, value)?,
            }
        }
        return Ok(Some(substituted.into_any()));
    }
    let Ok(items) = positional_bind_items(parameters) else {
        return Ok(None);
    };
    let substituted = PyList::empty(py);
    for value in items {
        match input_handler_substitute(py, handler, &proxy, &value, num_elements)? {
            Some(replacement) => substituted.append(replacement)?,
            None => substituted.append(value)?,
        }
    }
    Ok(Some(substituted.into_any()))
}

/// Determines whether an output type handler uses the modern two-argument
/// signature `(cursor, metadata)` or the legacy six-argument signature,
/// mirroring the reference `_get_output_type_handler`
/// (reference impl/base/cursor.pyx:318-324).
pub(crate) fn handler_uses_metadata(py: Python<'_>, handler: &Bound<'_, PyAny>) -> bool {
    let count = || -> PyResult<usize> {
        let signature = PyModule::import(py, "inspect")?
            .getattr("signature")?
            .call1((handler,))?;
        signature.getattr("parameters")?.len()
    };
    count().map(|count| count == 2).unwrap_or(false)
}

pub(crate) fn hydrate_cursor_impl(
    cursor: &Bound<'_, PyAny>,
    columns: &[ColumnMetadata],
    cursor_id: u32,
    invalid_ref_cursor: bool,
) -> PyResult<()> {
    fn hydrate(
        cursor_impl: &mut ThinCursorImpl,
        columns: &[ColumnMetadata],
        cursor_id: u32,
        invalid_ref_cursor: bool,
    ) {
        cursor_impl.columns = columns.to_vec();
        cursor_impl.reset_fetch_define_state();
        cursor_impl.rows.clear();
        cursor_impl.row_index = 0;
        cursor_impl.cursor_id = cursor_id;
        cursor_impl.more_rows = cursor_id != 0;
        cursor_impl.invalid_ref_cursor = invalid_ref_cursor;
        cursor_impl.rowcount = 0;
        cursor_impl.is_query = true;
    }

    let impl_obj = cursor.getattr("_impl")?;
    if let Ok(mut cursor_impl) = impl_obj.extract::<PyRefMut<'_, ThinCursorImpl>>() {
        hydrate(&mut cursor_impl, columns, cursor_id, invalid_ref_cursor);
        return Ok(());
    }
    let mut cursor_impl = impl_obj.extract::<PyRefMut<'_, AsyncThinCursorImpl>>()?;
    hydrate(
        &mut cursor_impl.inner,
        columns,
        cursor_id,
        invalid_ref_cursor,
    );
    Ok(())
}
