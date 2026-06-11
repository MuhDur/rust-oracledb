use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};
use std::thread;

use asupersync::runtime::{reactor, Runtime, RuntimeBuilder};
use asupersync::Cx;
use oracledb::protocol::sql;
use oracledb::protocol::thin::{
    bind_template_from_type_name, bind_value_type_info, column_metadata_is_xmltype,
    cursor_bind_template, dbobject_attr_max_size, dbobject_attr_precision_scale,
    dbobject_element_bind_type_info, dbobject_rowtype_attr_max_size, decode_bfile_locator_name,
    decode_datetime_value, decode_dbobject_binary_double as protocol_decode_dbobject_binary_double,
    decode_dbobject_binary_float as protocol_decode_dbobject_binary_float,
    decode_dbobject_text as protocol_decode_dbobject_text, decode_dbobject_xmltype_text,
    decode_lob_text as protocol_decode_lob_text, decode_number_value, define_metadata_from_bind,
    encode_lob_text as protocol_encode_lob_text, is_cursor_bind_template, lob_locator_is_temporary,
    output_bind as output_only_bind, public_dbtype_name_from_bind,
    public_dbtype_name_from_column_metadata, public_dbtype_name_from_oracle_type_name,
    public_dbtype_name_from_type_name, returning_output_bind, BindValue, ColumnMetadata,
    DbObjectPackedReader, QueryResult, QueryValue, CS_FORM_IMPLICIT, CS_FORM_NCHAR,
    ORA_TYPE_NUM_BFILE, ORA_TYPE_NUM_BINARY_DOUBLE, ORA_TYPE_NUM_BINARY_INTEGER, ORA_TYPE_NUM_BLOB,
    ORA_TYPE_NUM_CLOB, ORA_TYPE_NUM_CURSOR, ORA_TYPE_NUM_NUMBER, ORA_TYPE_NUM_OBJECT,
    ORA_TYPE_NUM_RAW, ORA_TYPE_NUM_TIMESTAMP, ORA_TYPE_NUM_TIMESTAMP_LTZ,
    ORA_TYPE_NUM_TIMESTAMP_TZ, ORA_TYPE_NUM_VARCHAR,
};
use oracledb::protocol::{ClientIdentity, ProtocolError};
use oracledb::{
    BlockingConnection, CancelHandle, ConnectOptions, Connection as RustConnection,
    Error as DriverError,
};
use pyo3::exceptions::{PyIndexError, PyNotImplementedError, PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyBytesMethods, PyDict, PyList, PyString, PyTuple};

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
