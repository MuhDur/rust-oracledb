// pyo3 emits deprecated HasAutomaticFromPyObject for Clone pyclasses (pre-existing at
// pre-split HEAD 978491a; not movement-induced). Item-level allows cannot reach the
// macro-generated siblings, so the allow must be file-scoped.
#![allow(deprecated)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use oracledb::protocol::thin::{
    decode_datetime_value, decode_dbobject_binary_double as protocol_decode_dbobject_binary_double,
    decode_dbobject_binary_float as protocol_decode_dbobject_binary_float,
    decode_dbobject_text as protocol_decode_dbobject_text, decode_dbobject_xmltype_text,
    decode_number_value, BindValue, ColumnMetadata, DbObjectPackedReader, QueryValue,
    CS_FORM_IMPLICIT, CS_FORM_NCHAR, ORA_TYPE_NUM_BLOB, ORA_TYPE_NUM_CLOB,
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyBytesMethods, PyDict, PyList, PyString};

use crate::*;

#[pyclass(module = "oracledb.thin_impl", name = "ThinDbObjectTypeImpl")]
#[derive(Clone, Debug)]
pub(crate) struct DbObjectTypeImpl {
    pub(crate) schema: String,
    pub(crate) package_name: Option<String>,
    pub(crate) name: String,
    oid: Option<Vec<u8>>,
    version: u32,
    pub(crate) is_collection: bool,
    pub(crate) attrs: Vec<DbObjectAttrImpl>,
    pub(crate) element_metadata: Option<Box<DbObjectAttrImpl>>,
    max_num_elements: u32,
    pub(crate) is_assoc_array: bool,
}

impl DbObjectTypeImpl {
    #[allow(clippy::too_many_arguments)] // pre-existing lint at pre-split HEAD 978491a; not movement-induced
    pub(crate) fn new(
        schema: String,
        package_name: Option<String>,
        name: String,
        typecode: &str,
        attrs: Vec<DbObjectAttrImpl>,
        element_metadata: Option<DbObjectAttrImpl>,
        max_num_elements: u32,
        is_assoc_array: bool,
    ) -> Self {
        Self {
            schema,
            package_name,
            name,
            oid: None,
            version: 0,
            is_collection: typecode.eq_ignore_ascii_case("COLLECTION"),
            attrs,
            element_metadata: element_metadata.map(Box::new),
            max_num_elements,
            is_assoc_array,
        }
    }

    pub(crate) fn from_column_metadata(metadata: &ColumnMetadata) -> Option<Self> {
        let name = metadata.object_type_name.as_ref()?.to_ascii_uppercase();
        let schema = metadata
            .object_schema
            .as_deref()
            .unwrap_or_default()
            .to_ascii_uppercase();
        Some(Self::new(
            schema,
            None,
            name,
            "OBJECT",
            Vec::new(),
            None,
            0,
            false,
        ))
    }

    pub(crate) fn with_type_identity(mut self, oid: Option<Vec<u8>>, version: u32) -> Self {
        self.oid = oid;
        self.version = version;
        self
    }

    pub(crate) fn object_output_bind(&self) -> Option<BindValue> {
        let oid = self.oid.clone()?;
        Some(BindValue::ObjectOutput {
            schema: self.schema.clone(),
            type_name: self.name.clone(),
            oid,
            version: self.version.max(1),
            buffer_size: 1,
            is_return: false,
        })
    }

    pub(crate) fn default_scalar_return_attr(&self) -> Option<&str> {
        self.attrs
            .iter()
            .find(|attr| attr.name.eq_ignore_ascii_case("STRINGVALUE"))
            .or_else(|| {
                self.attrs.iter().find(|attr| {
                    matches!(
                        attr.dbtype_name.as_str(),
                        "DB_TYPE_VARCHAR" | "DB_TYPE_CHAR" | "DB_TYPE_NVARCHAR" | "DB_TYPE_NCHAR"
                    )
                })
            })
            .map(|attr| attr.name.as_str())
    }
}

impl PartialEq for DbObjectTypeImpl {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema
            && self.package_name == other.package_name
            && self.name == other.name
    }
}

impl Eq for DbObjectTypeImpl {}

#[pymethods]
impl DbObjectTypeImpl {
    #[getter]
    fn schema(&self) -> &str {
        &self.schema
    }

    #[getter]
    fn package_name(&self) -> Option<&str> {
        self.package_name.as_deref()
    }

    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn is_collection(&self) -> bool {
        self.is_collection
    }

    #[getter]
    fn attrs(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let attrs = self
            .attrs
            .iter()
            .cloned()
            .map(|attr| Py::new(py, attr))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyList::new(py, attrs)?.unbind().into())
    }

    #[getter]
    fn attrs_by_name(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        for attr in &self.attrs {
            dict.set_item(&attr.name, Py::new(py, attr.clone())?)?;
        }
        Ok(dict.unbind().into())
    }

    #[getter]
    fn element_metadata(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.element_metadata
            .as_deref()
            .cloned()
            .map(|metadata| Py::new(py, metadata).map(Py::into_any))
            .unwrap_or_else(|| Ok(py.None()))
    }

    pub(crate) fn _get_fqn(&self) -> String {
        if let Some(package_name) = &self.package_name {
            format!("{}.{}.{}", self.schema, package_name, self.name)
        } else {
            format!("{}.{}", self.schema, self.name)
        }
    }

    fn create_new_object(&self, py: Python<'_>) -> PyResult<DbObjectImpl> {
        DbObjectImpl::new(py, self.clone())
    }

    fn __eq__(&self, other: &Self) -> bool {
        self == other
    }

    fn __ne__(&self, other: &Self) -> bool {
        self != other
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "ThinDbObjectAttrImpl")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DbObjectAttrImpl {
    pub(crate) name: String,
    pub(crate) dbtype_name: String,
    pub(crate) objtype: Option<DbObjectTypeImpl>,
    pub(crate) max_size: u32,
    pub(crate) precision: i8,
    pub(crate) scale: i8,
}

#[pymethods]
impl DbObjectAttrImpl {
    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn dbtype(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(PyModule::import(py, "oracledb")?
            .getattr(&self.dbtype_name)?
            .unbind())
    }

    #[getter]
    fn objtype(&self) -> Option<DbObjectTypeImpl> {
        self.objtype.clone()
    }

    #[getter]
    fn max_size(&self) -> u32 {
        self.max_size
    }

    #[getter]
    fn precision(&self) -> i8 {
        self.precision
    }

    #[getter]
    fn scale(&self) -> i8 {
        self.scale
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "ThinDbObjectImpl")]
pub(crate) struct DbObjectImpl {
    pub(crate) object_type: DbObjectTypeImpl,
    attr_values: Arc<Mutex<BTreeMap<String, Py<PyAny>>>>,
    pub(crate) collection_values: Arc<Mutex<Vec<Py<PyAny>>>>,
    pub(crate) assoc_values: Arc<Mutex<BTreeMap<i32, Py<PyAny>>>>,
    packed_data: Arc<Mutex<Option<Vec<u8>>>>,
    lob_context: Option<ThinLobContext>,
}

pub(crate) struct DbObjectPickleReader<'a> {
    inner: DbObjectPackedReader<'a>,
}

impl<'a> DbObjectPickleReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            inner: DbObjectPackedReader::new(bytes),
        }
    }

    fn read_u8(&mut self) -> PyResult<u8> {
        self.inner.read_u8().map_err(runtime_error)
    }

    fn read_i32be(&mut self) -> PyResult<i32> {
        self.inner.read_i32be().map_err(runtime_error)
    }

    fn read_length(&mut self) -> PyResult<usize> {
        self.inner.read_length().map_err(runtime_error)
    }

    fn read_value_bytes(&mut self) -> PyResult<Option<Vec<u8>>> {
        self.inner.read_value_bytes().map_err(runtime_error)
    }

    fn read_header(&mut self) -> PyResult<()> {
        self.inner.read_header().map_err(runtime_error)
    }

    fn read_atomic_null(&mut self, is_collection_context: bool) -> PyResult<bool> {
        self.inner
            .read_atomic_null(is_collection_context)
            .map_err(runtime_error)
    }
}

pub(crate) fn validated_dbobject_value(
    py: Python<'_>,
    metadata: &DbObjectAttrImpl,
    value: Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    let bound = value.bind(py);
    if bound.is_none() {
        return Ok(py.None());
    }
    match metadata.dbtype_name.as_str() {
        "DB_TYPE_OBJECT" => {
            if let Some(expected_type) = &metadata.objtype {
                let Some(actual_object) = py_db_object_impl(bound)? else {
                    return Err(raise_unsupported_python_type_for_db_type(
                        bound,
                        &metadata.dbtype_name,
                    ));
                };
                let actual_type = actual_object.object_type.clone();
                if &actual_type != expected_type {
                    return Err(raise_wrong_object_type(&actual_type, expected_type));
                }
            }
        }
        #[allow(clippy::collapsible_match)]
        // pre-existing lint at pre-split HEAD 978491a; not movement-induced
        "DB_TYPE_NUMBER" => {
            if bound.cast::<PyString>().is_ok() || bound.cast::<PyBytes>().is_ok() {
                return Err(raise_unsupported_python_type_for_db_type(
                    bound,
                    &metadata.dbtype_name,
                ));
            }
        }
        _ => {}
    }
    Ok(value)
}

pub(crate) fn dbobject_value_byte_size(
    py: Python<'_>,
    value: &Py<PyAny>,
) -> PyResult<Option<usize>> {
    let bound = value.bind(py);
    if bound.is_none() {
        return Ok(None);
    }
    if let Ok(text) = bound.extract::<String>() {
        return Ok(Some(text.len()));
    }
    if let Ok(bytes) = bound.cast::<PyBytes>() {
        return Ok(Some(bytes.as_bytes().len()));
    }
    Ok(None)
}

impl DbObjectImpl {
    fn new(py: Python<'_>, object_type: DbObjectTypeImpl) -> PyResult<Self> {
        let mut attr_values = BTreeMap::new();
        for attr in &object_type.attrs {
            attr_values.insert(attr.name.clone(), py.None());
        }
        Ok(Self {
            object_type,
            attr_values: Arc::new(Mutex::new(attr_values)),
            collection_values: Arc::new(Mutex::new(Vec::new())),
            assoc_values: Arc::new(Mutex::new(BTreeMap::new())),
            packed_data: Arc::new(Mutex::new(None)),
            lob_context: None,
        })
    }

    pub(crate) fn with_packed_data(
        object_type: DbObjectTypeImpl,
        packed_data: Vec<u8>,
        lob_context: Option<ThinLobContext>,
    ) -> Self {
        Self {
            object_type,
            attr_values: Arc::new(Mutex::new(BTreeMap::new())),
            collection_values: Arc::new(Mutex::new(Vec::new())),
            assoc_values: Arc::new(Mutex::new(BTreeMap::new())),
            packed_data: Arc::new(Mutex::new(Some(packed_data))),
            lob_context,
        }
    }

    pub(crate) fn with_attr(
        py: Python<'_>,
        object_type: DbObjectTypeImpl,
        attr_name: &str,
        value: String,
    ) -> PyResult<Self> {
        let object = Self::new(py, object_type)?;
        object.set_attr_by_name(py, attr_name, value.into_pyobject(py)?.unbind().into())?;
        Ok(object)
    }

    fn set_attr_by_name(&self, py: Python<'_>, attr_name: &str, value: Py<PyAny>) -> PyResult<()> {
        let key = attr_name.to_ascii_uppercase();
        let value = if value.bind(py).is_none() {
            py.None()
        } else {
            value
        };
        self.attr_values
            .lock()
            .map_err(runtime_error)?
            .insert(key, value);
        Ok(())
    }

    fn attr_value(&self, py: Python<'_>, attr_name: &str) -> PyResult<Py<PyAny>> {
        self.ensure_unpacked(py)?;
        Ok(self
            .attr_values
            .lock()
            .map_err(runtime_error)?
            .get(&attr_name.to_ascii_uppercase())
            .map(|value| value.clone_ref(py))
            .unwrap_or_else(|| py.None()))
    }

    pub(crate) fn attr_bind_value(&self, py: Python<'_>, attr_name: &str) -> PyResult<Py<PyAny>> {
        self.attr_value(py, attr_name)
    }

    fn next_collection_append_index(&self) -> PyResult<i32> {
        if self.object_type.is_assoc_array {
            let values = self.assoc_values.lock().map_err(runtime_error)?;
            Ok(values
                .keys()
                .next_back()
                .copied()
                .map(|index| index.saturating_add(1))
                .unwrap_or(0))
        } else {
            Ok(
                i32::try_from(self.collection_values.lock().map_err(runtime_error)?.len())
                    .unwrap_or(i32::MAX),
            )
        }
    }

    fn append_collection_value(&self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        self.ensure_unpacked(py)?;
        let value = if value.bind(py).is_none() {
            py.None()
        } else {
            value
        };
        if self.object_type.is_assoc_array {
            let mut values = self.assoc_values.lock().map_err(runtime_error)?;
            let index = values
                .keys()
                .next_back()
                .copied()
                .map(|index| index.saturating_add(1))
                .unwrap_or(0);
            values.insert(index, value);
            return Ok(());
        }
        let mut values = self.collection_values.lock().map_err(runtime_error)?;
        if self.object_type.max_num_elements > 0
            && values.len() >= self.object_type.max_num_elements as usize
        {
            return Err(raise_invalid_coll_index_set(
                i32::try_from(values.len()).unwrap_or(i32::MAX),
                0,
                i32::try_from(self.object_type.max_num_elements.saturating_sub(1))
                    .unwrap_or(i32::MAX),
            ));
        }
        values.push(value);
        Ok(())
    }

    pub(crate) fn ensure_unpacked(&self, py: Python<'_>) -> PyResult<()> {
        let packed_data = self.packed_data.lock().map_err(runtime_error)?.clone();
        let Some(packed_data) = packed_data else {
            return Ok(());
        };
        let mut reader = DbObjectPickleReader::new(&packed_data);
        reader.read_header()?;
        self.unpack_from_reader(py, &mut reader)?;
        *self.packed_data.lock().map_err(runtime_error)? = None;
        Ok(())
    }

    fn unpack_from_reader(
        &self,
        py: Python<'_>,
        reader: &mut DbObjectPickleReader<'_>,
    ) -> PyResult<()> {
        if self.object_type.is_collection {
            let _collection_flags = reader.read_u8()?;
            let num_elements = reader.read_length()?;
            if self.object_type.is_assoc_array {
                let mut values = BTreeMap::new();
                let Some(metadata) = self.object_type.element_metadata.as_deref() else {
                    return Err(PyRuntimeError::new_err(
                        "missing collection element metadata",
                    ));
                };
                for _ in 0..num_elements {
                    let index = reader.read_i32be()?;
                    let value = dbobject_unpack_value(
                        py,
                        metadata,
                        reader,
                        true,
                        self.lob_context.as_ref(),
                    )?;
                    values.insert(index, value);
                }
                *self.assoc_values.lock().map_err(runtime_error)? = values;
            } else {
                let mut values = Vec::with_capacity(num_elements);
                let Some(metadata) = self.object_type.element_metadata.as_deref() else {
                    return Err(PyRuntimeError::new_err(
                        "missing collection element metadata",
                    ));
                };
                for _ in 0..num_elements {
                    values.push(dbobject_unpack_value(
                        py,
                        metadata,
                        reader,
                        true,
                        self.lob_context.as_ref(),
                    )?);
                }
                *self.collection_values.lock().map_err(runtime_error)? = values;
            }
            return Ok(());
        }

        let mut values = BTreeMap::new();
        for attr in &self.object_type.attrs {
            values.insert(
                attr.name.clone(),
                dbobject_unpack_value(py, attr, reader, false, self.lob_context.as_ref())?,
            );
        }
        *self.attr_values.lock().map_err(runtime_error)? = values;
        Ok(())
    }
}

pub(crate) fn decode_dbobject_text(bytes: &[u8], dbtype_name: &str) -> PyResult<String> {
    protocol_decode_dbobject_text(bytes, dbtype_name).map_err(runtime_error)
}

pub(crate) fn decode_dbobject_xmltype(py: Python<'_>, bytes: &[u8]) -> PyResult<Py<PyAny>> {
    match decode_dbobject_xmltype_text(bytes).map_err(runtime_error)? {
        Some(value) => Ok(value.into_pyobject(py)?.unbind().into()),
        None => Ok(py.None()),
    }
}

pub(crate) fn decode_dbobject_binary_float(bytes: &[u8]) -> PyResult<f32> {
    protocol_decode_dbobject_binary_float(bytes).map_err(runtime_error)
}

pub(crate) fn decode_dbobject_binary_double(bytes: &[u8]) -> PyResult<f64> {
    protocol_decode_dbobject_binary_double(bytes).map_err(runtime_error)
}

pub(crate) fn dbobject_unpack_value(
    py: Python<'_>,
    metadata: &DbObjectAttrImpl,
    reader: &mut DbObjectPickleReader<'_>,
    parent_is_collection: bool,
    lob_context: Option<&ThinLobContext>,
) -> PyResult<Py<PyAny>> {
    if metadata.dbtype_name == "DB_TYPE_OBJECT" {
        let Some(object_type) = metadata.objtype.clone() else {
            let _ = reader.read_value_bytes()?;
            return Ok(py.None());
        };
        let is_collection_context = parent_is_collection || object_type.is_collection;
        if reader.read_atomic_null(is_collection_context)? {
            return Ok(py.None());
        }
        let object = if is_collection_context {
            let Some(packed_data) = reader.read_value_bytes()? else {
                return Ok(py.None());
            };
            DbObjectImpl::with_packed_data(object_type, packed_data, lob_context.cloned())
        } else {
            let mut object = DbObjectImpl::new(py, object_type)?;
            object.lob_context = lob_context.cloned();
            object.unpack_from_reader(py, reader)?;
            object
        };
        return py_db_object_from_impl(py, object);
    }

    let Some(bytes) = reader.read_value_bytes()? else {
        return Ok(py.None());
    };
    match metadata.dbtype_name.as_str() {
        "DB_TYPE_CHAR" | "DB_TYPE_NCHAR" | "DB_TYPE_VARCHAR" | "DB_TYPE_NVARCHAR" => {
            Ok(decode_dbobject_text(&bytes, &metadata.dbtype_name)?
                .into_pyobject(py)?
                .unbind()
                .into())
        }
        "DB_TYPE_RAW" => Ok(PyBytes::new(py, &bytes).unbind().into()),
        "DB_TYPE_XMLTYPE" => decode_dbobject_xmltype(py, &bytes),
        "DB_TYPE_NUMBER" => {
            let value = decode_number_value(&bytes).map_err(runtime_error)?;
            if metadata.scale == -127 && metadata.precision > 0 {
                if let QueryValue::Number { text, .. } = value {
                    let value = text.parse::<f64>().map_err(runtime_error)?;
                    return Ok(value.into_pyobject(py)?.unbind().into());
                }
            }
            query_value_to_py(py, &Some(value), None, None, true)
        }
        "DB_TYPE_DATE" | "DB_TYPE_TIMESTAMP" | "DB_TYPE_TIMESTAMP_TZ" | "DB_TYPE_TIMESTAMP_LTZ" => {
            let value = decode_datetime_value(&bytes).map_err(runtime_error)?;
            query_value_to_py(py, &Some(value), None, None, true)
        }
        "DB_TYPE_BINARY_FLOAT" => Ok(f64::from(decode_dbobject_binary_float(&bytes)?)
            .into_pyobject(py)?
            .unbind()
            .into()),
        "DB_TYPE_BINARY_DOUBLE" => Ok(decode_dbobject_binary_double(&bytes)?
            .into_pyobject(py)?
            .unbind()
            .into()),
        "DB_TYPE_CLOB" | "DB_TYPE_NCLOB" | "DB_TYPE_BLOB" => {
            let ora_type_num = if metadata.dbtype_name == "DB_TYPE_BLOB" {
                ORA_TYPE_NUM_BLOB
            } else {
                ORA_TYPE_NUM_CLOB
            };
            let csfrm = if metadata.dbtype_name == "DB_TYPE_NCLOB" {
                CS_FORM_NCHAR
            } else {
                CS_FORM_IMPLICIT
            };
            py_lob_from_impl(
                py,
                ThinLob {
                    data: None,
                    locator: Arc::new(Mutex::new(Some(bytes))),
                    ora_type_num,
                    csfrm,
                    size: 0,
                    chunk_size: 0,
                    context: lob_context.cloned(),
                    is_open: Arc::new(Mutex::new(false)),
                    bfile_name: None,
                },
            )
        }
        _ => Ok(py.None()),
    }
}

pub(crate) fn py_db_object_from_impl(py: Python<'_>, object: DbObjectImpl) -> PyResult<Py<PyAny>> {
    let impl_obj = Py::new(py, object)?;
    Ok(PyModule::import(py, "oracledb")?
        .getattr("DbObject")?
        .call_method1("_from_impl", (impl_obj,))?
        .unbind())
}

#[pymethods]
impl DbObjectImpl {
    #[getter]
    #[pyo3(name = "type")]
    fn object_type(&self) -> DbObjectTypeImpl {
        self.object_type.clone()
    }

    fn get_attr_value(&self, py: Python<'_>, attr: &DbObjectAttrImpl) -> PyResult<Py<PyAny>> {
        self.attr_value(py, &attr.name)
    }

    fn set_attr_value(
        &self,
        py: Python<'_>,
        attr: &DbObjectAttrImpl,
        value: Py<PyAny>,
    ) -> PyResult<()> {
        let value = validated_dbobject_value(py, attr, value)?;
        if attr.max_size > 0 {
            if let Some(actual_size) = dbobject_value_byte_size(py, &value)? {
                if actual_size > attr.max_size as usize {
                    return Err(raise_dbobject_attr_max_size(
                        &attr.name,
                        &self.object_type._get_fqn(),
                        actual_size,
                        attr.max_size,
                    ));
                }
            }
        }
        self.set_attr_by_name(py, &attr.name, value)
    }

    fn set_attr_value_checked(
        &self,
        py: Python<'_>,
        attr: &DbObjectAttrImpl,
        value: Py<PyAny>,
    ) -> PyResult<()> {
        self.set_attr_by_name(py, &attr.name, value)
    }

    fn copy(&self, py: Python<'_>) -> PyResult<Self> {
        self.ensure_unpacked(py)?;
        let mut attr_values = BTreeMap::new();
        for (name, value) in self.attr_values.lock().map_err(runtime_error)?.iter() {
            attr_values.insert(name.clone(), value.clone_ref(py));
        }
        let collection_values = self
            .collection_values
            .lock()
            .map_err(runtime_error)?
            .iter()
            .map(|value| value.clone_ref(py))
            .collect();
        Ok(Self {
            object_type: self.object_type.clone(),
            attr_values: Arc::new(Mutex::new(attr_values)),
            collection_values: Arc::new(Mutex::new(collection_values)),
            assoc_values: Arc::new(Mutex::new(
                self.assoc_values
                    .lock()
                    .map_err(runtime_error)?
                    .iter()
                    .map(|(index, value)| (*index, value.clone_ref(py)))
                    .collect(),
            )),
            packed_data: Arc::new(Mutex::new(None)),
            lob_context: self.lob_context.clone(),
        })
    }

    fn append(&self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let Some(metadata) = self.object_type.element_metadata.as_deref() else {
            return Err(raise_oracledb_driver_error(
                "ERR_OBJECT_IS_NOT_A_COLLECTION",
            ));
        };
        let value = validated_dbobject_value(py, metadata, value)?;
        if metadata.max_size > 0 {
            if let Some(actual_size) = dbobject_value_byte_size(py, &value)? {
                if actual_size > metadata.max_size as usize {
                    return Err(raise_dbobject_element_max_size(
                        self.next_collection_append_index()?,
                        &self.object_type._get_fqn(),
                        actual_size,
                        metadata.max_size,
                    ));
                }
            }
        }
        self.append_collection_value(py, value)
    }

    fn append_checked(&self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        self.append_collection_value(py, value)
    }

    fn delete_by_index(&self, py: Python<'_>, index: i32) -> PyResult<()> {
        self.ensure_unpacked(py)?;
        if self.object_type.is_assoc_array {
            let mut values = self.assoc_values.lock().map_err(runtime_error)?;
            if values.remove(&index).is_none() {
                return Err(raise_invalid_coll_index_get(index));
            }
            return Ok(());
        }
        let mut values = self.collection_values.lock().map_err(runtime_error)?;
        let Ok(index) = usize::try_from(index) else {
            return Err(raise_invalid_coll_index_get(index));
        };
        if index >= values.len() {
            return Err(raise_invalid_coll_index_get(
                i32::try_from(index).unwrap_or(i32::MAX),
            ));
        }
        values.remove(index);
        Ok(())
    }

    fn exists_by_index(&self, py: Python<'_>, index: i32) -> PyResult<bool> {
        self.ensure_unpacked(py)?;
        if self.object_type.is_assoc_array {
            return Ok(self
                .assoc_values
                .lock()
                .map_err(runtime_error)?
                .contains_key(&index));
        }
        let values = self.collection_values.lock().map_err(runtime_error)?;
        Ok(usize::try_from(index)
            .map(|index| index < values.len())
            .unwrap_or(false))
    }

    fn get_element_by_index(&self, py: Python<'_>, index: i32) -> PyResult<Py<PyAny>> {
        self.ensure_unpacked(py)?;
        if self.object_type.is_assoc_array {
            return self
                .assoc_values
                .lock()
                .map_err(runtime_error)?
                .get(&index)
                .map(|value| value.clone_ref(py))
                .ok_or_else(|| raise_invalid_coll_index_get(index));
        }
        let values = self.collection_values.lock().map_err(runtime_error)?;
        let Ok(index) = usize::try_from(index) else {
            return Err(raise_invalid_coll_index_get(index));
        };
        values
            .get(index)
            .map(|value| value.clone_ref(py))
            .ok_or_else(|| raise_invalid_coll_index_get(i32::try_from(index).unwrap_or(i32::MAX)))
    }

    fn get_first_index(&self, py: Python<'_>) -> PyResult<Option<i32>> {
        self.ensure_unpacked(py)?;
        if self.object_type.is_assoc_array {
            return Ok(self
                .assoc_values
                .lock()
                .map_err(runtime_error)?
                .keys()
                .next()
                .copied());
        }
        let values = self.collection_values.lock().map_err(runtime_error)?;
        Ok((!values.is_empty()).then_some(0))
    }

    fn get_last_index(&self, py: Python<'_>) -> PyResult<Option<i32>> {
        self.ensure_unpacked(py)?;
        if self.object_type.is_assoc_array {
            return Ok(self
                .assoc_values
                .lock()
                .map_err(runtime_error)?
                .keys()
                .next_back()
                .copied());
        }
        let values = self.collection_values.lock().map_err(runtime_error)?;
        Ok(values
            .len()
            .checked_sub(1)
            .map(|index| i32::try_from(index).unwrap_or(i32::MAX)))
    }

    fn get_next_index(&self, py: Python<'_>, index: i32) -> PyResult<Option<i32>> {
        self.ensure_unpacked(py)?;
        if self.object_type.is_assoc_array {
            return Ok(self
                .assoc_values
                .lock()
                .map_err(runtime_error)?
                .range((index.saturating_add(1))..)
                .next()
                .map(|(index, _)| *index));
        }
        let values = self.collection_values.lock().map_err(runtime_error)?;
        let next = index.saturating_add(1);
        Ok(usize::try_from(next)
            .ok()
            .filter(|next_index| *next_index < values.len())
            .map(|_| next))
    }

    fn get_prev_index(&self, py: Python<'_>, index: i32) -> PyResult<Option<i32>> {
        self.ensure_unpacked(py)?;
        if self.object_type.is_assoc_array {
            return Ok(self
                .assoc_values
                .lock()
                .map_err(runtime_error)?
                .range(..index)
                .next_back()
                .map(|(index, _)| *index));
        }
        Ok((index > 0).then_some(index - 1))
    }

    fn get_size(&self, py: Python<'_>) -> PyResult<usize> {
        self.ensure_unpacked(py)?;
        if self.object_type.is_assoc_array {
            return Ok(self.assoc_values.lock().map_err(runtime_error)?.len());
        }
        Ok(self.collection_values.lock().map_err(runtime_error)?.len())
    }

    fn set_element_by_index(&self, py: Python<'_>, index: i32, value: Py<PyAny>) -> PyResult<()> {
        self.ensure_unpacked(py)?;
        let Some(metadata) = self.object_type.element_metadata.as_deref() else {
            return Err(raise_oracledb_driver_error(
                "ERR_OBJECT_IS_NOT_A_COLLECTION",
            ));
        };
        let value = validated_dbobject_value(py, metadata, value)?;
        if metadata.max_size > 0 {
            if let Some(actual_size) = dbobject_value_byte_size(py, &value)? {
                if actual_size > metadata.max_size as usize {
                    return Err(raise_dbobject_element_max_size(
                        index,
                        &self.object_type._get_fqn(),
                        actual_size,
                        metadata.max_size,
                    ));
                }
            }
        }
        self.set_element_by_index_checked(py, index, value)
    }

    fn set_element_by_index_checked(
        &self,
        py: Python<'_>,
        index: i32,
        value: Py<PyAny>,
    ) -> PyResult<()> {
        self.ensure_unpacked(py)?;
        if self.object_type.is_assoc_array {
            self.assoc_values
                .lock()
                .map_err(runtime_error)?
                .insert(index, value);
            return Ok(());
        }
        let mut values = self.collection_values.lock().map_err(runtime_error)?;
        let max_index = values
            .len()
            .checked_sub(1)
            .map(|index| i32::try_from(index).unwrap_or(i32::MAX))
            .unwrap_or(0);
        let Ok(index_usize) = usize::try_from(index) else {
            return Err(raise_invalid_coll_index_set(index, 0, max_index));
        };
        let Some(slot) = values.get_mut(index_usize) else {
            return Err(raise_invalid_coll_index_set(index, 0, max_index));
        };
        *slot = value;
        Ok(())
    }

    fn trim(&self, py: Python<'_>, num_to_trim: i32) -> PyResult<()> {
        self.ensure_unpacked(py)?;
        if num_to_trim <= 0 {
            return Ok(());
        }
        let mut values = self.collection_values.lock().map_err(runtime_error)?;
        let new_len = values.len().saturating_sub(num_to_trim as usize);
        values.truncate(new_len);
        Ok(())
    }
}
