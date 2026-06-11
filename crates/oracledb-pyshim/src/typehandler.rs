use oracledb::protocol::thin::ColumnMetadata;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

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
