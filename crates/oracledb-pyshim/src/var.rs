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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ThinVarReturnKind {
    Plain,
    ClobAsLong,
}

pub(crate) fn thin_var_from_type_spec(
    py: Python<'_>,
    connection: &Bound<'_, PyAny>,
    typ: &Bound<'_, PyAny>,
    size: u32,
    is_array: bool,
    num_elements: u32,
    outconverter: Option<Py<PyAny>>,
    convert_nulls: bool,
    bypass_decode: bool,
) -> PyResult<Py<ThinVar>> {
    let type_name = py_type_name(typ);
    let object_type = py_db_object_type_impl(typ)?;
    let default_bind = if let Some(object_type) = object_type.as_ref() {
        object_type
            .object_output_bind()
            .unwrap_or(BindValue::TypedNull {
                ora_type_num: ORA_TYPE_NUM_VARCHAR,
                csfrm: CS_FORM_IMPLICIT,
                buffer_size: size.max(4000),
            })
    } else {
        bind_template_from_type_name(&type_name, size)
    };
    let return_kind = return_kind_from_type_name(&type_name);
    let object_return_attr = object_type
        .as_ref()
        .and_then(DbObjectTypeImpl::default_scalar_return_attr)
        .map(str::to_string);
    let dbtype_name = object_type
        .as_ref()
        .map(|_| "DB_TYPE_OBJECT")
        .unwrap_or_else(|| public_dbtype_name_from_type_name(&type_name));
    let value = if is_cursor_bind_template(&default_bind) {
        Some(connection.call_method0("cursor")?.unbind())
    } else {
        None
    };
    Py::new(
        py,
        ThinVar::typed_with_options(
            default_bind,
            value,
            is_array,
            num_elements,
            outconverter,
            convert_nulls,
            return_kind,
            object_type,
            object_return_attr,
            dbtype_name,
            bypass_decode,
        ),
    )
}

pub(crate) fn thin_var_from_input_size(
    py: Python<'_>,
    connection: &Bound<'_, PyAny>,
    value: &Bound<'_, PyAny>,
) -> PyResult<Py<ThinVar>> {
    if let Some(var) = thin_var_from_value(value)? {
        return Ok(var);
    }
    let default_bind = bind_template_from_input_size(value)?;
    let (is_array, num_elements) = input_size_array_info(value)?;
    let type_name = py_type_name(value);
    let dbtype_name = if type_name.is_empty() {
        public_dbtype_name_from_bind(&default_bind)
    } else {
        public_dbtype_name_from_type_name(&type_name)
    };
    let value = if is_cursor_bind_template(&default_bind) {
        Some(connection.call_method0("cursor")?.unbind())
    } else {
        None
    };
    Py::new(
        py,
        ThinVar::typed_with_options(
            default_bind,
            value,
            is_array,
            num_elements,
            None,
            false,
            ThinVarReturnKind::Plain,
            None,
            None,
            dbtype_name,
            false,
        ),
    )
}

pub(crate) fn input_size_array_info(value: &Bound<'_, PyAny>) -> PyResult<(bool, u32)> {
    if let Ok(tuple) = value.cast::<PyTuple>() {
        if tuple.len() == 2 {
            return Ok((true, tuple.get_item(1)?.extract::<u32>()?.max(1)));
        }
    }
    if let Ok(list) = value.cast::<PyList>() {
        if list.len() == 2 {
            return Ok((true, list.get_item(1)?.extract::<u32>()?.max(1)));
        }
    }
    Ok((false, 1))
}

#[pyclass(module = "oracledb.thin_impl", name = "ThinVar")]
pub(crate) struct ThinVar {
    value: Arc<Mutex<Option<Py<PyAny>>>>,
    returned_values: Arc<Mutex<Option<Vec<Py<PyAny>>>>>,
    pub(crate) default_bind: BindValue,
    outconverter: Option<Py<PyAny>>,
    convert_nulls: bool,
    is_array: bool,
    num_elements: u32,
    return_kind: ThinVarReturnKind,
    pub(crate) object_type: Option<DbObjectTypeImpl>,
    pub(crate) object_return_attr: Option<String>,
    pub(crate) dbtype_name: String,
    bypass_decode: bool,
}

impl ThinVar {
    pub(crate) fn from_py_value(value: Option<Py<PyAny>>) -> Self {
        Self {
            value: Arc::new(Mutex::new(value)),
            returned_values: Arc::new(Mutex::new(None)),
            default_bind: BindValue::Null,
            outconverter: None,
            convert_nulls: false,
            is_array: false,
            num_elements: 1,
            return_kind: ThinVarReturnKind::Plain,
            object_type: None,
            object_return_attr: None,
            dbtype_name: "DB_TYPE_VARCHAR".to_string(),
            bypass_decode: false,
        }
    }

    fn typed_with_options(
        default_bind: BindValue,
        value: Option<Py<PyAny>>,
        is_array: bool,
        num_elements: u32,
        outconverter: Option<Py<PyAny>>,
        convert_nulls: bool,
        return_kind: ThinVarReturnKind,
        object_type: Option<DbObjectTypeImpl>,
        object_return_attr: Option<String>,
        dbtype_name: impl Into<String>,
        bypass_decode: bool,
    ) -> Self {
        Self {
            value: Arc::new(Mutex::new(value)),
            returned_values: Arc::new(Mutex::new(None)),
            default_bind,
            outconverter,
            convert_nulls,
            is_array,
            num_elements: num_elements.max(1),
            return_kind,
            object_type,
            object_return_attr,
            dbtype_name: dbtype_name.into(),
            bypass_decode,
        }
    }

    pub(crate) fn to_bind_value(&self, py: Python<'_>) -> PyResult<BindValue> {
        if self.is_array {
            let guard = self.value.lock().map_err(runtime_error)?;
            let values = if let Some(value) = guard.as_ref() {
                py_list_to_array_bind_values(value.bind(py))?
            } else {
                Vec::new()
            };
            let (ora_type_num, csfrm, buffer_size) = bind_type_info(&self.default_bind)
                .unwrap_or((ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT, 1));
            return Ok(BindValue::Array {
                ora_type_num,
                csfrm,
                buffer_size,
                max_elements: self.num_elements,
                values,
            });
        }
        if is_cursor_bind_template(&self.default_bind) {
            if let Some(value) = self.value.lock().map_err(runtime_error)?.as_ref() {
                validate_public_cursor_is_open(value.bind(py))?;
            }
            return Ok(self.default_bind.clone());
        }
        let guard = self.value.lock().map_err(runtime_error)?;
        let Some(value) = guard.as_ref() else {
            return Ok(self.default_bind.clone());
        };
        py_value_to_bind_with_template(value.bind(py), &self.default_bind)
    }

    pub(crate) fn set_py_value(&self, value: Option<Py<PyAny>>) -> PyResult<()> {
        *self.value.lock().map_err(runtime_error)? = value;
        *self.returned_values.lock().map_err(runtime_error)? = None;
        Ok(())
    }

    pub(crate) fn set_bind_py_value(&self, value: Option<Py<PyAny>>) -> PyResult<()> {
        *self.value.lock().map_err(runtime_error)? = value;
        Ok(())
    }

    fn set_py_value_checked(&self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let bound = value.bind(py);
        if matches!(
            self.dbtype_name.as_str(),
            "DB_TYPE_ROWID" | "DB_TYPE_UROWID"
        ) && !bound.is_none()
        {
            return Err(raise_unsupported_type_set(&self.dbtype_name));
        }
        if let Some(expected_type) = &self.object_type {
            if !bound.is_none() {
                let Some(actual_object) = py_db_object_impl(bound)? else {
                    return Err(raise_unsupported_python_type_for_db_type(
                        bound,
                        "DB_TYPE_OBJECT",
                    ));
                };
                let actual_type = actual_object.object_type.clone();
                if &actual_type != expected_type {
                    return Err(raise_wrong_object_type(&actual_type, expected_type));
                }
            }
        }
        self.set_py_value(Some(value))
    }

    pub(crate) fn clear_returned_values(&self) -> PyResult<()> {
        *self.returned_values.lock().map_err(runtime_error)? = None;
        Ok(())
    }

    pub(crate) fn push_returned_py_value(&self, value: Py<PyAny>) -> PyResult<()> {
        *self.value.lock().map_err(runtime_error)? = None;
        let mut guard = self.returned_values.lock().map_err(runtime_error)?;
        guard.get_or_insert_with(Vec::new).push(value);
        Ok(())
    }

    fn check_position(&self, pos: u32) -> PyResult<()> {
        if pos >= self.num_elements {
            return Err(PyIndexError::new_err("variable position out of range"));
        }
        Ok(())
    }

    fn get_py_value_at(&self, py: Python<'_>, pos: u32) -> PyResult<Py<PyAny>> {
        self.check_position(pos)?;
        if let Some(values) = self.returned_values.lock().map_err(runtime_error)?.as_ref() {
            let index = usize::try_from(pos).map_err(runtime_error)?;
            return Ok(values
                .get(index)
                .map(|value| value.clone_ref(py))
                .unwrap_or_else(|| py.None()));
        }
        if let Some(value) = self.value.lock().map_err(runtime_error)?.as_ref() {
            let bound = value.bind(py);
            if let Some(lob) = scalar_value_to_memory_lob(py, bound, &self.dbtype_name)? {
                return Ok(lob);
            }
            return Ok(value.clone_ref(py));
        }
        if self.is_array {
            return Ok(PyList::empty(py).unbind().into());
        }
        Ok(py.None())
    }

    pub(crate) fn get_py_value(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.get_py_value_at(py, 0)
    }

    fn dbtype(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let module = PyModule::import(py, "oracledb")?;
        Ok(module.getattr(&self.dbtype_name)?.unbind())
    }

    fn repr_value(&self, py: Python<'_>) -> PyResult<String> {
        let values = self.get_all_values(py)?;
        let value = if !self.is_array && values.len() == 1 {
            values
                .first()
                .map(|value| value.clone_ref(py))
                .unwrap_or_else(|| py.None())
        } else {
            PyList::new(py, values)?.unbind().into()
        };
        value.bind(py).repr()?.extract()
    }

    pub(crate) fn output_value_to_py(
        &self,
        py: Python<'_>,
        value: &Option<QueryValue>,
        lob_context: Option<&ThinLobContext>,
    ) -> PyResult<Py<PyAny>> {
        let value = match (self.return_kind, value) {
            (ThinVarReturnKind::ClobAsLong, Some(QueryValue::Text(value))) => py_lob_from_impl(
                py,
                ThinLob {
                    data: Some(Arc::new(Mutex::new(value.as_bytes().to_vec()))),
                    locator: Arc::new(Mutex::new(None)),
                    ora_type_num: ORA_TYPE_NUM_CLOB,
                    csfrm: CS_FORM_IMPLICIT,
                    size: value.chars().count() as u64,
                    chunk_size: 0,
                    context: None,
                    is_open: Arc::new(Mutex::new(false)),
                    bfile_name: None,
                },
            )?,
            (ThinVarReturnKind::Plain, Some(QueryValue::Text(value))) if self.bypass_decode => {
                PyBytes::new(py, value.as_bytes()).unbind().into()
            }
            (ThinVarReturnKind::Plain, Some(QueryValue::Text(value)))
                if self.dbtype_name == "DB_TYPE_BINARY_INTEGER" =>
            {
                python_int_from_decimal_text(py, value)?
            }
            (ThinVarReturnKind::Plain, Some(QueryValue::Number { text, .. }))
                if self.dbtype_name == "DB_TYPE_BINARY_INTEGER" =>
            {
                python_int_from_decimal_text(py, text)?
            }
            (ThinVarReturnKind::Plain, Some(QueryValue::Number { text, .. }))
                if matches!(
                    self.dbtype_name.as_str(),
                    "DB_TYPE_CHAR"
                        | "DB_TYPE_LONG"
                        | "DB_TYPE_LONG_NVARCHAR"
                        | "DB_TYPE_NCHAR"
                        | "DB_TYPE_NVARCHAR"
                        | "DB_TYPE_VARCHAR"
                ) =>
            {
                text.clone().into_pyobject(py)?.unbind().into()
            }
            (ThinVarReturnKind::Plain, Some(QueryValue::Text(value)))
                if self.object_type.is_some() =>
            {
                let object_type = self
                    .object_type
                    .clone()
                    .ok_or_else(|| PyRuntimeError::new_err("missing object type"))?;
                let attr_name = self
                    .object_return_attr
                    .clone()
                    .or_else(|| object_type.default_scalar_return_attr().map(str::to_string))
                    .ok_or_else(|| {
                        not_implemented("ThinVar object DML RETURNING projection metadata")
                    })?;
                let object = DbObjectImpl::with_attr(py, object_type, &attr_name, value.clone())?;
                py_db_object_from_impl(py, object)?
            }
            (
                ThinVarReturnKind::Plain,
                Some(QueryValue::Object {
                    packed_data,
                    schema: _,
                    type_name: _,
                }),
            ) if self.object_type.is_some() => {
                let object_type = self
                    .object_type
                    .clone()
                    .ok_or_else(|| PyRuntimeError::new_err("missing object type"))?;
                py_db_object_from_impl(
                    py,
                    DbObjectImpl::with_packed_data(object_type, packed_data.clone(), None),
                )?
            }
            _ => query_value_to_py(py, value, None, lob_context, true)?,
        };
        if let Some(outconverter) = self.outconverter.as_ref() {
            if !value.bind(py).is_none() || self.convert_nulls {
                return Ok(outconverter.bind(py).call1((value,))?.unbind());
            }
        }
        Ok(value)
    }
}

#[pymethods]
impl ThinVar {
    #[new]
    fn new() -> Self {
        Self::from_py_value(None)
    }

    #[pyo3(signature = (pos=None))]
    fn getvalue(&self, py: Python<'_>, pos: Option<u32>) -> PyResult<Py<PyAny>> {
        self.get_py_value_at(py, pos.unwrap_or(0))
    }

    fn get_value(&self, py: Python<'_>, pos: u32) -> PyResult<Py<PyAny>> {
        self.get_py_value_at(py, pos)
    }

    fn get_all_values(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        if let Some(values) = self.returned_values.lock().map_err(runtime_error)?.as_ref() {
            return Ok(values.iter().map(|value| value.clone_ref(py)).collect());
        }
        Ok(vec![self.get_py_value(py)?])
    }

    #[getter]
    fn values(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        self.get_all_values(py)
    }

    fn setvalue(&self, py: Python<'_>, pos: u32, value: Py<PyAny>) -> PyResult<()> {
        self.check_position(pos)?;
        self.set_py_value_checked(py, value)
    }

    fn set_value(&self, py: Python<'_>, pos: u32, value: Py<PyAny>) -> PyResult<()> {
        self.check_position(pos)?;
        self.set_py_value_checked(py, value)
    }

    #[getter]
    fn r#type(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.dbtype(py)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "<oracledb.Var of type {} with value {}>",
            self.dbtype_name,
            self.repr_value(py)?
        ))
    }

    fn __str__(&self, py: Python<'_>) -> PyResult<String> {
        self.__repr__(py)
    }
}

pub(crate) fn thin_var_from_value(value: &Bound<'_, PyAny>) -> PyResult<Option<Py<ThinVar>>> {
    if let Ok(var) = value.extract::<Py<ThinVar>>() {
        return Ok(Some(var));
    }
    if value.hasattr("_impl")? {
        let impl_obj = value.getattr("_impl")?;
        if let Ok(var) = impl_obj.extract::<Py<ThinVar>>() {
            return Ok(Some(var));
        }
    }
    Ok(None)
}

pub(crate) fn bind_var_from_value(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<ThinVar>> {
    if let Some(var) = thin_var_from_value(value)? {
        return Ok(var);
    }
    let type_name = py_type_name(value);
    if !type_name.is_empty() {
        let default_bind = bind_template_from_type_name(&type_name, 0);
        if !matches!(default_bind, BindValue::Null) {
            return Py::new(
                py,
                ThinVar::typed_with_options(
                    default_bind,
                    None,
                    false,
                    1,
                    None,
                    false,
                    ThinVarReturnKind::Plain,
                    None,
                    None,
                    public_dbtype_name_from_type_name(&type_name),
                    false,
                ),
            );
        }
    }
    Py::new(py, ThinVar::from_py_value(Some(value.clone().unbind())))
}

pub(crate) fn py_value_to_execute_bind(value: &Bound<'_, PyAny>) -> PyResult<BindValue> {
    if let Some(var) = thin_var_from_value(value)? {
        let bind = var.borrow(value.py()).to_bind_value(value.py())?;
        if is_cursor_bind_template(&bind) {
            return Ok(output_only_bind(bind));
        }
        return Ok(bind);
    }
    py_value_to_bind(value)
}

pub(crate) fn apply_out_bind_values(
    py: Python<'_>,
    bind_vars: &[Py<ThinVar>],
    out_values: &[(usize, Option<QueryValue>)],
    return_values: &[(usize, Vec<Option<QueryValue>>)],
    lob_context: Option<&ThinLobContext>,
) -> PyResult<()> {
    for (index, value) in out_values {
        let Some(var) = bind_vars.get(*index) else {
            continue;
        };
        if let Some(QueryValue::Cursor { columns, cursor_id }) = value {
            apply_cursor_out_bind(py, var, columns, *cursor_id)?;
            continue;
        }
        if let Some(QueryValue::Array(values)) = value {
            let var_ref = var.borrow(py);
            let values = values
                .iter()
                .map(|value| var_ref.output_value_to_py(py, value, lob_context))
                .collect::<PyResult<Vec<_>>>()?;
            drop(var_ref);
            var.borrow(py).clear_returned_values()?;
            for value in values {
                var.borrow(py).push_returned_py_value(value)?;
            }
            continue;
        }
        let value = var.borrow(py).output_value_to_py(py, value, lob_context)?;
        var.borrow(py).set_py_value(Some(value))?;
    }
    for (index, _) in return_values {
        let Some(var) = bind_vars.get(*index) else {
            continue;
        };
        var.borrow(py).clear_returned_values()?;
    }
    for (index, values) in return_values {
        let Some(var) = bind_vars.get(*index) else {
            continue;
        };
        let var_ref = var.borrow(py);
        let values = values
            .iter()
            .map(|value| var_ref.output_value_to_py(py, value, lob_context))
            .collect::<PyResult<Vec<_>>>()?;
        drop(var_ref);
        let values = PyList::new(py, values)?.unbind().into();
        var.borrow(py).push_returned_py_value(values)?;
    }
    Ok(())
}

pub(crate) fn apply_cursor_out_bind(
    py: Python<'_>,
    var: &Py<ThinVar>,
    columns: &[ColumnMetadata],
    cursor_id: u32,
) -> PyResult<()> {
    let cursor = var.borrow(py).get_py_value(py)?;
    let cursor = cursor.bind(py);
    hydrate_cursor_impl(cursor, columns, cursor_id, cursor_id == 0)
}
