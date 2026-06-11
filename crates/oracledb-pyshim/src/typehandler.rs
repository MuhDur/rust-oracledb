use oracledb::protocol::thin::ColumnMetadata;
use pyo3::prelude::*;

use crate::*;

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

    #[pyo3(signature = (
        typ,
        size=0,
        arraysize=1,
        inconverter=None,
        outconverter=None,
        encoding_errors=None,
        bypass_decode=false,
        convert_nulls=false
    ))]
    #[allow(clippy::too_many_arguments)] // pre-existing lint at pre-split HEAD 978491a; not movement-induced
    fn var(
        &self,
        py: Python<'_>,
        typ: &Bound<'_, PyAny>,
        size: u32,
        arraysize: u32,
        inconverter: Option<Py<PyAny>>,
        outconverter: Option<Py<PyAny>>,
        encoding_errors: Option<String>,
        bypass_decode: bool,
        convert_nulls: bool,
    ) -> PyResult<Py<ThinVar>> {
        let _ = arraysize;
        let _ = inconverter;
        let _ = encoding_errors;
        thin_var_from_type_spec(
            py,
            self.connection.bind(py),
            typ,
            size,
            false,
            1,
            outconverter,
            convert_nulls,
            bypass_decode,
        )
    }
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
