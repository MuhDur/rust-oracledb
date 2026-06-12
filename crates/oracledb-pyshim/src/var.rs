use std::sync::{Arc, Mutex};

use oracledb::protocol::thin::{
    bind_template_from_type_name, is_cursor_bind_template, output_bind as output_only_bind,
    public_dbtype_name_from_bind, public_dbtype_name_from_type_name, public_dbtype_size_info,
    BindValue, ColumnMetadata, QueryValue, CS_FORM_IMPLICIT, CS_FORM_NCHAR, ORA_TYPE_NUM_BFILE,
    ORA_TYPE_NUM_BLOB, ORA_TYPE_NUM_CLOB, ORA_TYPE_NUM_VARCHAR,
};
use pyo3::exceptions::{PyIndexError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyFloat, PyInt, PyList, PyString, PyTuple};

use crate::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ThinVarReturnKind {
    Plain,
    ClobAsLong,
}

/// Python materialization requested for fetched values, mirroring the
/// reference `OracleMetadata._py_type_num` overrides that matter for the shim
/// (reference impl/base/metadata.pyx:369-411).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ThinVarPyKind {
    Default,
    Float,
    Decimal,
}

pub(crate) struct ThinVarOptions {
    pub(crate) default_bind: BindValue,
    pub(crate) value: Option<Py<PyAny>>,
    pub(crate) is_array: bool,
    pub(crate) num_elements: u32,
    pub(crate) size: u32,
    pub(crate) inconverter: Option<Py<PyAny>>,
    pub(crate) outconverter: Option<Py<PyAny>>,
    pub(crate) encoding_errors: Option<String>,
    pub(crate) convert_nulls: bool,
    pub(crate) return_kind: ThinVarReturnKind,
    pub(crate) py_kind: ThinVarPyKind,
    pub(crate) object_type: Option<DbObjectTypeImpl>,
    pub(crate) object_return_attr: Option<String>,
    pub(crate) dbtype_name: String,
    pub(crate) bypass_decode: bool,
}

impl Default for ThinVarOptions {
    fn default() -> Self {
        Self {
            default_bind: BindValue::Null,
            value: None,
            is_array: false,
            num_elements: 1,
            size: 0,
            inconverter: None,
            outconverter: None,
            encoding_errors: None,
            convert_nulls: false,
            return_kind: ThinVarReturnKind::Plain,
            py_kind: ThinVarPyKind::Default,
            object_type: None,
            object_return_attr: None,
            dbtype_name: "DB_TYPE_VARCHAR".to_string(),
            bypass_decode: false,
        }
    }
}

pub(crate) fn py_kind_from_type_name(type_name: &str) -> ThinVarPyKind {
    match type_name {
        "Decimal" => ThinVarPyKind::Decimal,
        "float" => ThinVarPyKind::Float,
        _ => ThinVarPyKind::Default,
    }
}

#[allow(clippy::too_many_arguments)] // pre-existing lint at pre-split HEAD 978491a; not movement-induced
pub(crate) fn thin_var_from_type_spec(
    py: Python<'_>,
    connection: &Bound<'_, PyAny>,
    typ: &Bound<'_, PyAny>,
    size: u32,
    is_array: bool,
    num_elements: u32,
    inconverter: Option<Py<PyAny>>,
    outconverter: Option<Py<PyAny>>,
    encoding_errors: Option<String>,
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
        ThinVar::with_options(ThinVarOptions {
            default_bind,
            value,
            is_array,
            num_elements,
            size,
            inconverter,
            outconverter,
            encoding_errors,
            convert_nulls,
            return_kind,
            py_kind: py_kind_from_type_name(&type_name),
            object_type,
            object_return_attr,
            dbtype_name: dbtype_name.to_string(),
            bypass_decode,
        }),
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
        ThinVar::with_options(ThinVarOptions {
            default_bind,
            value,
            is_array,
            num_elements,
            dbtype_name: dbtype_name.to_string(),
            ..ThinVarOptions::default()
        }),
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
    values: Mutex<Vec<Option<Py<PyAny>>>>,
    returned_values: Arc<Mutex<Option<Vec<Py<PyAny>>>>>,
    pub(crate) default_bind: BindValue,
    pub(crate) inconverter_value: Option<Py<PyAny>>,
    outconverter_value: Option<Py<PyAny>>,
    pub(crate) encoding_errors: Option<String>,
    convert_nulls: bool,
    is_array: bool,
    num_elements: u32,
    num_elements_in_array: Mutex<u32>,
    max_size: Mutex<u32>,
    return_kind: ThinVarReturnKind,
    pub(crate) py_kind: ThinVarPyKind,
    pub(crate) object_type: Option<DbObjectTypeImpl>,
    pub(crate) object_return_attr: Option<String>,
    pub(crate) dbtype_name: String,
    bypass_decode: bool,
}

impl ThinVar {
    pub(crate) fn from_py_value(value: Option<Py<PyAny>>) -> Self {
        Self::with_options(ThinVarOptions {
            value,
            ..ThinVarOptions::default()
        })
    }

    pub(crate) fn with_options(options: ThinVarOptions) -> Self {
        let num_elements = options.num_elements.max(1);
        let mut values: Vec<Option<Py<PyAny>>> = Vec::with_capacity(num_elements as usize);
        values.push(options.value);
        values.resize_with(num_elements as usize, || None);
        // mirror reference OracleMetadata._finalize_init
        // (impl/base/metadata.pyx:112-133)
        let (default_size, _) = public_dbtype_size_info(&options.dbtype_name);
        let max_size = if default_size == 0 {
            0
        } else if options.size == 0 {
            default_size
        } else {
            options.size
        };
        Self {
            values: Mutex::new(values),
            returned_values: Arc::new(Mutex::new(None)),
            default_bind: options.default_bind,
            inconverter_value: options.inconverter,
            outconverter_value: options.outconverter,
            encoding_errors: options.encoding_errors,
            convert_nulls: options.convert_nulls,
            is_array: options.is_array,
            num_elements,
            num_elements_in_array: Mutex::new(0),
            max_size: Mutex::new(max_size),
            return_kind: options.return_kind,
            py_kind: options.py_kind,
            object_type: options.object_type,
            object_return_attr: options.object_return_attr,
            dbtype_name: options.dbtype_name,
            bypass_decode: options.bypass_decode,
        }
    }

    pub(crate) fn num_elements_value(&self) -> u32 {
        self.num_elements
    }

    pub(crate) fn to_bind_value(&self, py: Python<'_>) -> PyResult<BindValue> {
        if self.is_array {
            let count = *self.num_elements_in_array.lock().map_err(runtime_error)?;
            let guard = self.values.lock().map_err(runtime_error)?;
            let values = guard
                .iter()
                .take(count as usize)
                .map(|value| match value {
                    Some(value) if !value.bind(py).is_none() => {
                        py_value_to_bind_with_template(value.bind(py), &self.default_bind).map(Some)
                    }
                    _ => Ok(None),
                })
                .collect::<PyResult<Vec<_>>>()?;
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
            let guard = self.values.lock().map_err(runtime_error)?;
            if let Some(value) = guard.first().and_then(Option::as_ref) {
                validate_public_cursor_is_open(value.bind(py))?;
            }
            return Ok(self.default_bind.clone());
        }
        let guard = self.values.lock().map_err(runtime_error)?;
        let Some(value) = guard.first().and_then(Option::as_ref) else {
            return Ok(self.default_bind.clone());
        };
        py_value_to_bind_with_template(value.bind(py), &self.default_bind)
    }

    /// Stores a raw value without performing the public `_check_value`
    /// validation. Used by internal bind plumbing where the value has either
    /// already been validated or comes from the wire.
    pub(crate) fn set_py_value(&self, py: Python<'_>, value: Option<Py<PyAny>>) -> PyResult<()> {
        self.store_raw_value(py, value)?;
        *self.returned_values.lock().map_err(runtime_error)? = None;
        Ok(())
    }

    pub(crate) fn set_bind_py_value(
        &self,
        py: Python<'_>,
        value: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        self.store_raw_value(py, value)
    }

    fn store_raw_value(&self, py: Python<'_>, value: Option<Py<PyAny>>) -> PyResult<()> {
        if self.is_array {
            if let Some(value) = value.as_ref() {
                let bound = value.bind(py);
                if let Ok(list) = bound.cast::<PyList>() {
                    let mut guard = self.values.lock().map_err(runtime_error)?;
                    guard.iter_mut().for_each(|slot| *slot = None);
                    for (index, item) in list.iter().enumerate() {
                        if index >= guard.len() {
                            guard.push(None);
                        }
                        guard[index] = if item.is_none() {
                            None
                        } else {
                            Some(item.unbind())
                        };
                    }
                    *self.num_elements_in_array.lock().map_err(runtime_error)? =
                        u32::try_from(list.len()).unwrap_or(u32::MAX);
                    return Ok(());
                }
            }
        }
        let mut guard = self.values.lock().map_err(runtime_error)?;
        if guard.is_empty() {
            guard.push(None);
        }
        guard[0] = value;
        Ok(())
    }

    /// Mirrors the reference `_check_and_set_value`
    /// (impl/base/var.pyx:74-111): array variables require a list, the list
    /// must fit and each element is validated and coerced individually.
    pub(crate) fn check_and_set_value(
        &self,
        py: Python<'_>,
        pos: u32,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        if !self.is_array {
            return self.check_and_set_scalar_value(py, pos, value);
        }
        let Ok(list) = value.cast::<PyList>() else {
            return Err(raise_oracledb_driver_error(
                "ERR_EXPECTING_LIST_FOR_ARRAY_VAR",
            ));
        };
        let count = u32::try_from(list.len()).unwrap_or(u32::MAX);
        if count > self.num_elements {
            return Err(raise_incorrect_var_arraysize(self.num_elements, count));
        }
        for (index, element) in list.iter().enumerate() {
            self.check_and_set_scalar_value(
                py,
                u32::try_from(index).unwrap_or(u32::MAX),
                &element,
            )?;
        }
        *self.num_elements_in_array.lock().map_err(runtime_error)? = count;
        Ok(())
    }

    /// Mirrors the reference `_check_and_set_scalar_value`
    /// (impl/base/var.pyx:43-72): apply the inconverter, validate/coerce the
    /// value for the database type and resize the variable when a longer
    /// string/bytes value is supplied.
    fn check_and_set_scalar_value(
        &self,
        py: Python<'_>,
        pos: u32,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let converted;
        let value = if let Some(inconverter) = self.inconverter_value.as_ref() {
            converted = inconverter.bind(py).call1((value,))?;
            &converted
        } else {
            value
        };
        let value = self.check_value(py, value)?;
        let bound = value.bind(py);
        if !bound.is_none() {
            let (default_size, _) = public_dbtype_size_info(&self.dbtype_name);
            if default_size != 0 {
                if let Ok(size) = bound.len() {
                    let size = u32::try_from(size).unwrap_or(u32::MAX);
                    let mut max_size = self.max_size.lock().map_err(runtime_error)?;
                    if size > *max_size {
                        *max_size = size;
                    }
                }
            }
        }
        let mut guard = self.values.lock().map_err(runtime_error)?;
        let index = pos as usize;
        if index >= guard.len() {
            return Err(PyIndexError::new_err("position out of range"));
        }
        guard[index] = if bound.is_none() { None } else { Some(value) };
        drop(guard);
        *self.returned_values.lock().map_err(runtime_error)? = None;
        Ok(())
    }

    /// Port of the reference `_check_value` coercion matrix
    /// (impl/base/connection.pyx:39-171).
    fn check_value(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        if value.is_none() {
            return Ok(py.None());
        }
        let pass = || Ok(value.clone().unbind());
        let unsupported = || {
            Err(raise_unsupported_python_type_for_db_type(
                value,
                &self.dbtype_name,
            ))
        };
        match self.dbtype_name.as_str() {
            "DB_TYPE_NUMBER"
            | "DB_TYPE_BINARY_INTEGER"
            | "DB_TYPE_BINARY_DOUBLE"
            | "DB_TYPE_BINARY_FLOAT" => {
                let is_bool = value.is_instance_of::<PyBool>();
                let is_numeric = is_bool
                    || value.is_instance_of::<PyInt>()
                    || value.is_instance_of::<PyFloat>()
                    || is_decimal_value(value)?;
                if !is_numeric {
                    return unsupported();
                }
                let builtins = PyModule::import(py, "builtins")?;
                if matches!(
                    self.dbtype_name.as_str(),
                    "DB_TYPE_BINARY_DOUBLE" | "DB_TYPE_BINARY_FLOAT"
                ) {
                    return Ok(builtins.getattr("float")?.call1((value,))?.unbind());
                }
                if self.dbtype_name == "DB_TYPE_BINARY_INTEGER" || is_bool {
                    return Ok(builtins.getattr("int")?.call1((value,))?.unbind());
                }
                pass()
            }
            "DB_TYPE_CHAR"
            | "DB_TYPE_VARCHAR"
            | "DB_TYPE_NCHAR"
            | "DB_TYPE_NVARCHAR"
            | "DB_TYPE_LONG"
            | "DB_TYPE_LONG_NVARCHAR" => {
                if value.is_instance_of::<PyBytes>() {
                    return Ok(value.call_method0("decode")?.unbind());
                }
                if value.is_instance_of::<PyString>() {
                    return pass();
                }
                unsupported()
            }
            "DB_TYPE_RAW" | "DB_TYPE_LONG_RAW" => {
                if value.is_instance_of::<PyString>() {
                    return Ok(value.call_method0("encode")?.unbind());
                }
                if value.is_instance_of::<PyBytes>() {
                    return pass();
                }
                unsupported()
            }
            "DB_TYPE_DATE"
            | "DB_TYPE_TIMESTAMP"
            | "DB_TYPE_TIMESTAMP_LTZ"
            | "DB_TYPE_TIMESTAMP_TZ" => {
                let date_type = PyModule::import(py, "datetime")?.getattr("date")?;
                if value.is_instance(&date_type)? {
                    return pass();
                }
                unsupported()
            }
            "DB_TYPE_INTERVAL_DS" => {
                let timedelta_type = PyModule::import(py, "datetime")?.getattr("timedelta")?;
                if value.is_instance(&timedelta_type)? {
                    return pass();
                }
                unsupported()
            }
            "DB_TYPE_CLOB" | "DB_TYPE_NCLOB" | "DB_TYPE_BLOB" | "DB_TYPE_BFILE" => {
                if let Some(actual) = py_any_lob_dbtype_name(value)? {
                    if actual != self.dbtype_name {
                        return Err(raise_lob_of_wrong_type(&actual, &self.dbtype_name));
                    }
                    return pass();
                }
                if self.dbtype_name != "DB_TYPE_BFILE" {
                    if value.is_instance_of::<PyBytes>() {
                        if self.dbtype_name == "DB_TYPE_BLOB" {
                            return pass();
                        }
                        return Ok(value.call_method0("decode")?.unbind());
                    }
                    if value.is_instance_of::<PyString>() {
                        if self.dbtype_name == "DB_TYPE_BLOB" {
                            return Ok(value.call_method0("encode")?.unbind());
                        }
                        return pass();
                    }
                }
                unsupported()
            }
            "DB_TYPE_OBJECT" => {
                let Some(actual_object) = py_db_object_impl(value)? else {
                    return Err(raise_unsupported_python_type_for_db_type(
                        value,
                        "DB_TYPE_OBJECT",
                    ));
                };
                if let Some(expected_type) = &self.object_type {
                    let actual_type = actual_object.object_type.clone();
                    if &actual_type != expected_type {
                        return Err(raise_wrong_object_type(&actual_type, expected_type));
                    }
                }
                pass()
            }
            "DB_TYPE_CURSOR" => {
                if is_public_cursor_value(value)? {
                    validate_public_cursor_is_open(value)?;
                    return pass();
                }
                unsupported()
            }
            "DB_TYPE_BOOLEAN" => Ok(PyBool::new(py, value.is_truthy()?)
                .to_owned()
                .unbind()
                .into()),
            "DB_TYPE_JSON" => pass(),
            _ => Err(raise_unsupported_type_set(&self.dbtype_name)),
        }
    }

    pub(crate) fn clear_returned_values(&self) -> PyResult<()> {
        *self.returned_values.lock().map_err(runtime_error)? = None;
        Ok(())
    }

    pub(crate) fn push_returned_py_value(&self, value: Py<PyAny>) -> PyResult<()> {
        if let Some(slot) = self.values.lock().map_err(runtime_error)?.first_mut() {
            *slot = None;
        }
        let mut guard = self.returned_values.lock().map_err(runtime_error)?;
        guard.get_or_insert_with(Vec::new).push(value);
        Ok(())
    }

    fn check_position(&self, pos: u32) -> PyResult<()> {
        if pos >= self.num_elements {
            return Err(PyIndexError::new_err("position out of range"));
        }
        Ok(())
    }

    fn array_value(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        let count = *self.num_elements_in_array.lock().map_err(runtime_error)?;
        let guard = self.values.lock().map_err(runtime_error)?;
        guard
            .iter()
            .take(count as usize)
            .map(|value| self.materialize_value(py, value.as_ref()))
            .collect()
    }

    fn materialize_value(&self, py: Python<'_>, value: Option<&Py<PyAny>>) -> PyResult<Py<PyAny>> {
        let Some(value) = value else {
            return Ok(py.None());
        };
        let bound = value.bind(py);
        if let Some(lob) = scalar_value_to_memory_lob(py, bound, &self.dbtype_name)? {
            return Ok(lob);
        }
        Ok(value.clone_ref(py))
    }

    fn get_py_value_at(&self, py: Python<'_>, pos: u32) -> PyResult<Py<PyAny>> {
        if self.is_array {
            return Ok(PyList::new(py, self.array_value(py)?)?.unbind().into());
        }
        self.check_position(pos)?;
        if let Some(values) = self.returned_values.lock().map_err(runtime_error)?.as_ref() {
            let index = usize::try_from(pos).map_err(runtime_error)?;
            return Ok(values
                .get(index)
                .map(|value| value.clone_ref(py))
                .unwrap_or_else(|| py.None()));
        }
        let guard = self.values.lock().map_err(runtime_error)?;
        let value = guard.get(pos as usize).and_then(Option::as_ref);
        let value = value.map(|value| value.clone_ref(py));
        drop(guard);
        self.materialize_value(py, value.as_ref())
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
        let value = self.convert_output_value(py, value, lob_context)?;
        if let Some(outconverter) = self.outconverter_value.as_ref() {
            if !value.bind(py).is_none() || self.convert_nulls {
                return Ok(outconverter.bind(py).call1((value,))?.unbind());
            }
        }
        Ok(value)
    }

    /// Client-side fetch materialization keyed on the variable's database
    /// type and the wire value, mirroring the reference
    /// `convert_oracle_data_to_python` matrix
    /// (impl/base/converters.pyx:498-700).
    fn convert_output_value(
        &self,
        py: Python<'_>,
        value: &Option<QueryValue>,
        lob_context: Option<&ThinLobContext>,
    ) -> PyResult<Py<PyAny>> {
        let target_is_char = matches!(
            self.dbtype_name.as_str(),
            "DB_TYPE_CHAR"
                | "DB_TYPE_LONG"
                | "DB_TYPE_LONG_NVARCHAR"
                | "DB_TYPE_NCHAR"
                | "DB_TYPE_NVARCHAR"
                | "DB_TYPE_VARCHAR"
        );
        let target_is_float = matches!(
            self.dbtype_name.as_str(),
            "DB_TYPE_BINARY_DOUBLE" | "DB_TYPE_BINARY_FLOAT"
        ) || (self.dbtype_name == "DB_TYPE_NUMBER"
            && matches!(self.py_kind, ThinVarPyKind::Float));
        match (self.return_kind, value) {
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
            ),
            (_, Some(QueryValue::TextRaw { bytes, csfrm })) => {
                if self.bypass_decode {
                    return Ok(PyBytes::new(py, bytes).unbind().into());
                }
                text_raw_to_py_str(py, bytes, *csfrm, self.encoding_errors.as_deref())
            }
            (ThinVarReturnKind::Plain, Some(QueryValue::Text(value))) if self.bypass_decode => {
                Ok(PyBytes::new(py, value.as_bytes()).unbind().into())
            }
            (ThinVarReturnKind::Plain, Some(QueryValue::Text(value)))
                if self.dbtype_name == "DB_TYPE_BINARY_INTEGER" =>
            {
                python_int_from_decimal_text(py, value)
            }
            // char/LONG wire data requested as NUMBER/BINARY_DOUBLE/FLOAT
            // materializes as Python float (converters.pyx:613-634)
            (ThinVarReturnKind::Plain, Some(QueryValue::Text(value)))
                if self.dbtype_name == "DB_TYPE_NUMBER" || target_is_float =>
            {
                let builtins = PyModule::import(py, "builtins")?;
                Ok(builtins
                    .getattr("float")?
                    .call1((value.as_str(),))?
                    .unbind())
            }
            (ThinVarReturnKind::Plain, Some(QueryValue::Number { text, .. }))
                if self.dbtype_name == "DB_TYPE_BINARY_INTEGER" =>
            {
                python_int_from_decimal_text(py, text)
            }
            (ThinVarReturnKind::Plain, Some(QueryValue::Number { text, .. }))
                if matches!(self.py_kind, ThinVarPyKind::Decimal)
                    && self.dbtype_name == "DB_TYPE_NUMBER" =>
            {
                let decimal = PyModule::import(py, "decimal")?.getattr("Decimal")?;
                Ok(decimal.call1((text.as_str(),))?.unbind())
            }
            (ThinVarReturnKind::Plain, Some(QueryValue::Number { text, .. }))
                if target_is_float =>
            {
                let builtins = PyModule::import(py, "builtins")?;
                Ok(builtins.getattr("float")?.call1((text.as_str(),))?.unbind())
            }
            (ThinVarReturnKind::Plain, Some(QueryValue::Number { text, .. })) if target_is_char => {
                Ok(text.clone().into_pyobject(py)?.unbind().into())
            }
            (ThinVarReturnKind::Plain, Some(QueryValue::BinaryDouble(text)))
                if self.dbtype_name == "DB_TYPE_BINARY_INTEGER" =>
            {
                python_int_from_decimal_text(py, text)
            }
            // str(float) of a BINARY_DOUBLE/BINARY_FLOAT value
            // (converters.pyx:548-553)
            (ThinVarReturnKind::Plain, Some(QueryValue::BinaryDouble(text))) if target_is_char => {
                let value = text.parse::<f64>().map_err(runtime_error)?;
                let builtins = PyModule::import(py, "builtins")?;
                let py_float = value.into_pyobject(py)?;
                Ok(builtins.getattr("str")?.call1((py_float,))?.unbind())
            }
            // str(datetime) for DATE/TIMESTAMP wire data requested as a
            // character type (converters.pyx:556-562)
            (ThinVarReturnKind::Plain, Some(QueryValue::DateTime { .. })) if target_is_char => {
                let datetime = query_value_to_py(py, value, None, lob_context, true)?;
                let builtins = PyModule::import(py, "builtins")?;
                Ok(builtins.getattr("str")?.call1((datetime,))?.unbind())
            }
            // str(timedelta) for INTERVAL DS wire data requested as a
            // character type (converters.pyx:565-566)
            (ThinVarReturnKind::Plain, Some(QueryValue::IntervalDS { .. })) if target_is_char => {
                let interval = query_value_to_py(py, value, None, lob_context, true)?;
                let builtins = PyModule::import(py, "builtins")?;
                Ok(builtins.getattr("str")?.call1((interval,))?.unbind())
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
                py_db_object_from_impl(py, object)
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
                )
            }
            _ => query_value_to_py(py, value, None, lob_context, true),
        }
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
        if self.is_array {
            return self.array_value(py);
        }
        let values = {
            let guard = self.values.lock().map_err(runtime_error)?;
            guard
                .iter()
                .map(|value| value.as_ref().map(|value| value.clone_ref(py)))
                .collect::<Vec<_>>()
        };
        values
            .iter()
            .map(|value| self.materialize_value(py, value.as_ref()))
            .collect()
    }

    #[getter]
    fn values(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        self.get_all_values(py)
    }

    fn setvalue(&self, py: Python<'_>, pos: u32, value: &Bound<'_, PyAny>) -> PyResult<()> {
        self.set_value(py, pos, value)
    }

    /// Mirrors the reference impl `set_value` semantics
    /// (impl/base/var.pyx:391-399).
    fn set_value(&self, py: Python<'_>, pos: u32, value: &Bound<'_, PyAny>) -> PyResult<()> {
        if self.is_array {
            if pos > 0 {
                return Err(raise_oracledb_driver_error("ERR_ARRAYS_OF_ARRAYS"));
            }
        } else {
            self.check_position(pos)?;
        }
        self.check_and_set_value(py, pos, value)
    }

    #[getter]
    fn r#type(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.dbtype(py)
    }

    #[getter]
    fn size(&self) -> PyResult<u32> {
        Ok(*self.max_size.lock().map_err(runtime_error)?)
    }

    #[getter]
    fn buffer_size(&self) -> PyResult<u32> {
        let (default_size, factor) = public_dbtype_size_info(&self.dbtype_name);
        if default_size == 0 {
            return Ok(factor);
        }
        let max_size = *self.max_size.lock().map_err(runtime_error)?;
        Ok(max_size.saturating_mul(factor))
    }

    #[getter(bufferSize)]
    fn buffer_size_deprecated(&self) -> PyResult<u32> {
        self.buffer_size()
    }

    #[getter]
    fn num_elements(&self) -> u32 {
        self.num_elements
    }

    #[getter(numElements)]
    fn num_elements_deprecated(&self) -> u32 {
        self.num_elements
    }

    #[getter]
    fn num_elements_in_array(&self) -> PyResult<u32> {
        Ok(*self.num_elements_in_array.lock().map_err(runtime_error)?)
    }

    #[getter]
    fn actual_elements(&self) -> PyResult<u32> {
        if self.is_array {
            return self.num_elements_in_array();
        }
        Ok(self.num_elements)
    }

    #[getter(actualElements)]
    fn actual_elements_deprecated(&self) -> PyResult<u32> {
        self.actual_elements()
    }

    #[getter]
    fn is_array(&self) -> bool {
        self.is_array
    }

    #[getter]
    fn convert_nulls(&self) -> bool {
        self.convert_nulls
    }

    #[getter]
    fn inconverter(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.inconverter_value
            .as_ref()
            .map(|value| value.clone_ref(py))
    }

    #[getter]
    fn outconverter(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.outconverter_value
            .as_ref()
            .map(|value| value.clone_ref(py))
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

fn is_decimal_value(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    let decimal_type = PyModule::import(value.py(), "decimal")?.getattr("Decimal")?;
    value.is_instance(&decimal_type)
}

/// Returns the public database type name of a LOB value (either a bare
/// `ThinLob`/`AsyncThinLob` impl or a public LOB wrapper) or `None` when the
/// value is not a LOB.
fn py_any_lob_dbtype_name(value: &Bound<'_, PyAny>) -> PyResult<Option<String>> {
    let info = if let Some(lob) = py_lob_impl(value)? {
        Some((lob.ora_type_num, lob.csfrm))
    } else if value.hasattr("_impl")? {
        let impl_obj = value.getattr("_impl")?;
        impl_obj
            .extract::<PyRef<'_, AsyncThinLob>>()
            .ok()
            .map(|lob| (lob.inner.ora_type_num, lob.inner.csfrm))
    } else {
        None
    };
    Ok(info.map(|(ora_type_num, csfrm)| {
        match (ora_type_num, csfrm) {
            (ORA_TYPE_NUM_BLOB, _) => "DB_TYPE_BLOB",
            (ORA_TYPE_NUM_CLOB, CS_FORM_NCHAR) => "DB_TYPE_NCLOB",
            (ORA_TYPE_NUM_CLOB, _) => "DB_TYPE_CLOB",
            (ORA_TYPE_NUM_BFILE, _) => "DB_TYPE_BFILE",
            _ => "DB_TYPE_CLOB",
        }
        .to_string()
    }))
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

pub(crate) fn bind_var_from_value(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<Py<ThinVar>> {
    if let Some(var) = thin_var_from_value(value)? {
        return Ok(var);
    }
    let type_name = py_type_name(value);
    if !type_name.is_empty() {
        let default_bind = bind_template_from_type_name(&type_name, 0);
        if !matches!(default_bind, BindValue::Null) {
            return Py::new(
                py,
                ThinVar::with_options(ThinVarOptions {
                    default_bind,
                    dbtype_name: public_dbtype_name_from_type_name(&type_name).to_string(),
                    ..ThinVarOptions::default()
                }),
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
        var.borrow(py).set_py_value(py, Some(value))?;
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
