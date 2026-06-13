use oracledb::protocol::thin::{
    column_metadata_is_xmltype, public_dbtype_name_from_column_metadata, ColumnMetadata,
    ORA_TYPE_NUM_OBJECT,
};
use pyo3::prelude::*;

use crate::*;

#[pyclass(
    module = "oracledb.thin_impl",
    name = "FetchMetadataImpl",
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct FetchMetadataImpl {
    pub(crate) metadata: ColumnMetadata,
}

#[pymethods]
impl FetchMetadataImpl {
    #[getter]
    fn name(&self) -> &str {
        &self.metadata.name
    }

    #[getter]
    fn dbtype(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let module = PyModule::import(py, "oracledb")?;
        if column_metadata_is_xmltype(&self.metadata) {
            return Ok(module.getattr("DB_TYPE_XMLTYPE")?.unbind());
        }
        let name = public_dbtype_name_from_column_metadata(&self.metadata);
        Ok(module.getattr(name)?.unbind())
    }

    #[getter]
    fn type_code(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.dbtype(py)
    }

    #[getter]
    fn max_size(&self) -> u32 {
        self.metadata.max_size
    }

    #[getter]
    fn buffer_size(&self) -> u32 {
        self.metadata.buffer_size
    }

    #[getter]
    fn precision(&self) -> i8 {
        self.metadata.precision
    }

    #[getter]
    fn scale(&self) -> i8 {
        self.metadata.scale
    }

    #[getter]
    fn nulls_allowed(&self) -> bool {
        self.metadata.nulls_allowed
    }

    #[getter]
    fn is_json(&self) -> bool {
        self.metadata.is_json
    }

    #[getter]
    fn is_oson(&self) -> bool {
        self.metadata.is_oson
    }

    #[getter]
    fn objtype(&self) -> Option<DbObjectTypeImpl> {
        if self.metadata.ora_type_num == ORA_TYPE_NUM_OBJECT {
            return DbObjectTypeImpl::from_column_metadata(&self.metadata);
        }
        None
    }

    #[getter]
    fn annotations(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        match &self.metadata.annotations {
            None => Ok(None),
            Some(pairs) => {
                let dict = pyo3::types::PyDict::new(py);
                for (key, value) in pairs {
                    dict.set_item(key, value)?;
                }
                Ok(Some(dict.into_any().unbind()))
            }
        }
    }

    #[getter]
    fn domain_name(&self) -> Option<&str> {
        self.metadata.domain_name.as_deref()
    }

    #[getter]
    fn domain_schema(&self) -> Option<&str> {
        self.metadata.domain_schema.as_deref()
    }

    #[getter]
    fn vector_dimensions(&self) -> Option<u32> {
        self.metadata.vector_dimensions
    }

    #[getter]
    fn vector_format(&self) -> u8 {
        self.metadata.vector_format
    }

    #[getter]
    fn vector_flags(&self) -> u8 {
        self.metadata.vector_flags
    }
}
