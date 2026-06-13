use std::sync::{Arc, Mutex};

use asupersync::Cx;
use oracledb::protocol::oson::OsonValue;
use oracledb::protocol::thin::{
    bind_template_from_type_name, bind_value_type_info, cursor_bind_template,
    dbobject_element_bind_type_info, decode_bfile_locator_name,
    encode_lob_text as protocol_encode_lob_text, BindValue, ColumnMetadata, QueryValue,
    CS_FORM_IMPLICIT, CS_FORM_NCHAR, ORA_TYPE_NUM_BFILE, ORA_TYPE_NUM_BINARY_DOUBLE,
    ORA_TYPE_NUM_BINARY_INTEGER, ORA_TYPE_NUM_BLOB, ORA_TYPE_NUM_BOOLEAN, ORA_TYPE_NUM_CLOB,
    ORA_TYPE_NUM_JSON, ORA_TYPE_NUM_NUMBER, ORA_TYPE_NUM_TIMESTAMP, ORA_TYPE_NUM_TIMESTAMP_LTZ,
    ORA_TYPE_NUM_TIMESTAMP_TZ, ORA_TYPE_NUM_VARCHAR, ORA_TYPE_NUM_VECTOR,
};
use oracledb::{BlockingConnection, Connection as RustConnection};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyBytesMethods, PyDict, PyList, PyTuple};

use crate::*;

pub(crate) fn py_value_to_bind(value: &Bound<'_, PyAny>) -> PyResult<BindValue> {
    if value.is_none() {
        return Ok(BindValue::Null);
    }
    if let Ok(var) = value.extract::<PyRef<'_, ThinVar>>() {
        return var.to_bind_value(value.py());
    }
    // bool precedes the numeric extracts (it is an int subclass); reference
    // OracleMetadata.from_value maps bool to DB_TYPE_BOOLEAN
    // (impl/base/metadata.pyx:422-423)
    if value.is_instance_of::<PyBool>() {
        return Ok(BindValue::Boolean(value.extract::<bool>()?));
    }
    if is_public_cursor_value(value)? {
        let impl_obj = value.getattr("_impl")?;
        if impl_obj.is_none() {
            return Err(raise_oracledb_driver_error("ERR_CURSOR_NOT_OPEN"));
        }
        if let Ok(cursor_impl) = impl_obj.extract::<PyRef<'_, ThinCursorImpl>>() {
            return Ok(if cursor_impl.cursor_id == 0 {
                cursor_bind_template()
            } else {
                BindValue::Cursor {
                    cursor_id: cursor_impl.cursor_id,
                }
            });
        }
        if let Ok(cursor_impl) = impl_obj.extract::<PyRef<'_, AsyncThinCursorImpl>>() {
            return Ok(if cursor_impl.inner.cursor_id == 0 {
                cursor_bind_template()
            } else {
                BindValue::Cursor {
                    cursor_id: cursor_impl.inner.cursor_id,
                }
            });
        }
    }
    if let Some(bind) = py_lob_value_to_bind(value)? {
        return Ok(bind);
    }
    if let Some(object) = py_db_object_impl(value)? {
        return dbobject_to_bind(value.py(), &object);
    }
    if let Ok(bytes) = value.cast::<PyBytes>() {
        return Ok(BindValue::Raw(bytes.as_bytes().to_vec()));
    }
    // IntervalYM is a namedtuple and must be recognized before the generic
    // tuple -> array-bind branch (reference metadata.pyx:452-453; only the
    // concrete oracledb.IntervalYM class is accepted, not (int, int) tuples)
    if let Some(bind) = py_interval_ym_to_bind(value)? {
        return Ok(bind);
    }
    // array.array / SparseVector always map to DB_TYPE_VECTOR (reference
    // metadata.pyx from_value:450-451). A bad array typecode raises DPY-3013.
    if is_vector_value(value)? {
        return Ok(BindValue::Vector(py_to_vector(value, false)?));
    }
    if value.cast::<PyList>().is_ok() || value.cast::<PyTuple>().is_ok() {
        let values = py_list_to_array_bind_values(value)?;
        let (ora_type_num, csfrm, buffer_size) = values
            .iter()
            .find_map(|value| value.as_ref().and_then(bind_type_info))
            .unwrap_or((ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT, 1));
        return Ok(BindValue::Array {
            ora_type_num,
            csfrm,
            buffer_size,
            max_elements: u32::try_from(values.len()).unwrap_or(u32::MAX).max(1),
            values,
        });
    }
    if let Some((year, month, day, hour, minute, second, _nanosecond)) = py_date_time_fields(value)?
    {
        return Ok(BindValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
        });
    }
    if let Ok(text) = value.extract::<String>() {
        return Ok(BindValue::Text(text));
    }
    if let Ok(number) = value.extract::<i128>() {
        return Ok(BindValue::Number(number.to_string()));
    }
    if let Ok(number) = value.extract::<f64>() {
        return Ok(BindValue::Number(number.to_string()));
    }
    if let Some(bind) = py_timedelta_to_bind(value)? {
        return Ok(bind);
    }
    // unsupported Python value without an input type handler raises DPY-3002
    // (reference impl/base/metadata.pyx:455 via bind_var.pyx:167-169)
    Err(raise_python_value_not_supported(&py_value_type_name(value)))
}

/// Converts a Python value to an [`OsonValue`] for OSON encoding, mirroring the
/// reference `OsonTreeSegment.encode_node` (impl/base/oson.pyx). Unsupported
/// Python types raise DPY-3003 (ERR_PYTHON_TYPE_NOT_SUPPORTED), matching the
/// reference (test_3508).
pub(crate) fn py_value_to_oson(value: &Bound<'_, PyAny>) -> PyResult<OsonValue> {
    let py = value.py();
    if value.is_none() {
        return Ok(OsonValue::Null);
    }
    // bool precedes the numeric branch (bool is an int subclass).
    if value.is_instance_of::<PyBool>() {
        return Ok(OsonValue::Bool(value.extract::<bool>()?));
    }
    // int / float / decimal.Decimal -> Oracle NUMBER carried as its str().
    let decimal_type = PyModule::import(py, "decimal")?.getattr("Decimal")?;
    let is_int = value.is_instance_of::<pyo3::types::PyInt>();
    let is_float = value.is_instance_of::<pyo3::types::PyFloat>();
    let is_decimal = value.is_instance(&decimal_type)?;
    if is_int || is_float || is_decimal {
        // str() preserves arbitrary-precision ints and Decimals exactly, which
        // is what the reference does (`PyObject_Str(value)`).
        let text = value.str()?.extract::<String>()?;
        return Ok(OsonValue::Number(text));
    }
    if let Ok(bytes) = value.cast::<PyBytes>() {
        return Ok(OsonValue::Raw(bytes.as_bytes().to_vec()));
    }
    // array.array / SparseVector -> embedded VECTOR node.
    if is_vector_value(value)? {
        return Ok(OsonValue::Vector(py_to_vector(value, false)?));
    }
    // timedelta -> INTERVAL DAY TO SECOND (checked before the generic datetime
    // attribute probe, which it would not satisfy anyway).
    let timedelta_type = PyModule::import(py, "datetime")?.getattr("timedelta")?;
    if value.is_instance(&timedelta_type)? {
        let days = value.getattr("days")?.extract::<i32>()?;
        let seconds = value.getattr("seconds")?.extract::<i32>()?;
        let microseconds = value.getattr("microseconds")?.extract::<i32>()?;
        return Ok(OsonValue::IntervalDS {
            days,
            hours: seconds / 3600,
            minutes: (seconds % 3600) / 60,
            seconds: seconds % 60,
            fseconds: microseconds * 1000,
        });
    }
    // datetime.datetime / datetime.date -> DATE / TIMESTAMP.
    if let Some((year, month, day, hour, minute, second, nanosecond)) = py_date_time_fields(value)?
    {
        return Ok(OsonValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
        });
    }
    if let Ok(text) = value.extract::<String>() {
        return Ok(OsonValue::String(text));
    }
    if let Ok(items) = value.cast::<PyList>() {
        let mut out = Vec::with_capacity(items.len());
        for item in items.iter() {
            out.push(py_value_to_oson(&item)?);
        }
        return Ok(OsonValue::Array(out));
    }
    if let Ok(items) = value.cast::<PyTuple>() {
        let mut out = Vec::with_capacity(items.len());
        for item in items.iter() {
            out.push(py_value_to_oson(&item)?);
        }
        return Ok(OsonValue::Array(out));
    }
    if let Ok(dict) = value.cast::<PyDict>() {
        let mut entries = Vec::with_capacity(dict.len());
        for (key, child) in dict.iter() {
            let key = key
                .extract::<String>()
                .map_err(|_| raise_oracledb_driver_error("ERR_PYTHON_TYPE_NOT_SUPPORTED"))?;
            entries.push((key, py_value_to_oson(&child)?));
        }
        return Ok(OsonValue::Object(entries));
    }
    // Unsupported type (e.g. a bare `list` class object) raises DPY-3003.
    Err(raise_python_type_not_supported(
        &value.get_type().into_any(),
    ))
}

/// Builds a [`BindValue::Json`] from a Python value by encoding it to OSON.
/// Every Python type (including `bytes`, which becomes an OSON binary scalar
/// node) is encoded; the reference does not special-case pre-encoded images
/// (test_6906 binds OSON bytes and they are re-wrapped as a binary node, then
/// decoded again by the test). Long field names (>255 bytes) are permitted
/// (OSON version 3); the encoder still emits version 1 when no long name is
/// present, matching the live driver byte-for-byte.
pub(crate) fn py_value_to_json_bind(value: &Bound<'_, PyAny>) -> PyResult<BindValue> {
    let oson = py_value_to_oson(value)?;
    let image = oracledb::protocol::oson::encode_oson(&oson, true).map_err(runtime_error)?;
    Ok(BindValue::Json(image))
}

pub(crate) fn py_interval_ym_to_bind(value: &Bound<'_, PyAny>) -> PyResult<Option<BindValue>> {
    let interval_ym_type = PyModule::import(value.py(), "oracledb")?.getattr("IntervalYM")?;
    if !value.is_instance(&interval_ym_type)? {
        return Ok(None);
    }
    Ok(Some(BindValue::IntervalYM {
        years: value.getattr("years")?.extract::<i32>()?,
        months: value.getattr("months")?.extract::<i32>()?,
    }))
}

pub(crate) fn py_timedelta_to_bind(value: &Bound<'_, PyAny>) -> PyResult<Option<BindValue>> {
    let timedelta_type = PyModule::import(value.py(), "datetime")?.getattr("timedelta")?;
    if !value.is_instance(&timedelta_type)? {
        return Ok(None);
    }
    Ok(Some(BindValue::IntervalDS {
        days: value.getattr("days")?.extract::<i32>()?,
        seconds: value.getattr("seconds")?.extract::<i32>()?,
        microseconds: value.getattr("microseconds")?.extract::<i32>()?,
    }))
}

pub(crate) fn py_value_to_bind_with_template(
    value: &Bound<'_, PyAny>,
    template: &BindValue,
) -> PyResult<BindValue> {
    let Some((ora_type_num, _csfrm, _buffer_size)) = bind_type_info(template) else {
        return py_value_to_bind(value);
    };
    if ora_type_num == ORA_TYPE_NUM_BINARY_INTEGER {
        if value.is_none() {
            return Ok(BindValue::Null);
        }
        return Ok(BindValue::BinaryInteger(
            python_int_from_value(value)?
                .bind(value.py())
                .str()?
                .extract::<String>()?,
        ));
    }
    if ora_type_num == ORA_TYPE_NUM_BINARY_DOUBLE {
        if value.is_none() {
            return Ok(BindValue::Null);
        }
        let number = value.extract::<f64>().or_else(|_| {
            PyModule::import(value.py(), "builtins")?
                .getattr("float")?
                .call1((value,))?
                .extract::<f64>()
        })?;
        return Ok(BindValue::BinaryDouble(number));
    }
    // reference _check_value coerces any value bound through a BOOLEAN
    // variable with bool() (impl/base/connection.pyx:139-140)
    if ora_type_num == ORA_TYPE_NUM_BOOLEAN {
        if value.is_none() {
            // A NULL bound through a BOOLEAN variable must keep its
            // DB_TYPE_BOOLEAN bind type (the template is TypedNull BOOLEAN);
            // an untyped Null falls back to VARCHAR and PL/SQL rejects it with
            // PLS-00306 (test_3103).
            return Ok(template.clone());
        }
        return Ok(BindValue::Boolean(value.is_truthy()?));
    }
    if ora_type_num == ORA_TYPE_NUM_NUMBER && value.is_instance_of::<PyBool>() {
        // bool bound through a NUMBER variable becomes int
        // (impl/base/connection.pyx:64-68)
        return Ok(BindValue::Number(
            if value.is_truthy()? { "1" } else { "0" }.to_string(),
        ));
    }
    if ora_type_num == ORA_TYPE_NUM_NUMBER
        && matches!(py_value_type_name(value).as_str(), "Decimal")
    {
        return Ok(BindValue::Number(value.str()?.extract::<String>()?));
    }
    // a value bound through a DB_TYPE_VECTOR variable accepts array.array,
    // SparseVector, and (coerced to array('d')) a plain list
    if ora_type_num == ORA_TYPE_NUM_VECTOR {
        if value.is_none() {
            return Ok(BindValue::Null);
        }
        return Ok(BindValue::Vector(py_to_vector(value, true)?));
    }
    // A value bound through a DB_TYPE_JSON variable is encoded to OSON. Already-
    // encoded OSON `bytes` are passed through unchanged (reference accepts a
    // pre-encoded image; test_6906).
    if ora_type_num == ORA_TYPE_NUM_JSON {
        if value.is_none() {
            return Ok(BindValue::Null);
        }
        return py_value_to_json_bind(value);
    }
    let Some((year, month, day, hour, minute, second, nanosecond)) = py_date_time_fields(value)?
    else {
        return py_value_to_bind(value);
    };
    if matches!(
        ora_type_num,
        ORA_TYPE_NUM_TIMESTAMP | ORA_TYPE_NUM_TIMESTAMP_LTZ | ORA_TYPE_NUM_TIMESTAMP_TZ
    ) {
        return Ok(BindValue::Timestamp {
            ora_type_num,
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
        });
    }
    py_value_to_bind(value)
}

pub(crate) fn py_list_to_array_bind_values(
    value: &Bound<'_, PyAny>,
) -> PyResult<Vec<Option<BindValue>>> {
    if let Ok(list) = value.cast::<PyList>() {
        return list
            .iter()
            .map(|item| {
                if item.is_none() {
                    Ok(None)
                } else {
                    py_value_to_bind(&item).map(Some)
                }
            })
            .collect();
    }
    let tuple = value.cast::<PyTuple>()?;
    tuple
        .iter()
        .map(|item| {
            if item.is_none() {
                Ok(None)
            } else {
                py_value_to_bind(&item).map(Some)
            }
        })
        .collect()
}

pub(crate) fn py_db_object_type_impl(
    value: &Bound<'_, PyAny>,
) -> PyResult<Option<DbObjectTypeImpl>> {
    if let Ok(object_type) = value.extract::<PyRef<'_, DbObjectTypeImpl>>() {
        return Ok(Some(object_type.clone()));
    }
    if value.hasattr("_impl")? {
        let impl_obj = value.getattr("_impl")?;
        if let Ok(object_type) = impl_obj.extract::<PyRef<'_, DbObjectTypeImpl>>() {
            return Ok(Some(object_type.clone()));
        }
    }
    Ok(None)
}

pub(crate) fn py_db_object_impl<'py>(
    value: &Bound<'py, PyAny>,
) -> PyResult<Option<PyRef<'py, DbObjectImpl>>> {
    if let Ok(object) = value.extract::<PyRef<'py, DbObjectImpl>>() {
        return Ok(Some(object));
    }
    if value.hasattr("_impl")? {
        let impl_obj = value.getattr("_impl")?;
        if let Ok(object) = impl_obj.extract::<PyRef<'py, DbObjectImpl>>() {
            return Ok(Some(object));
        }
    }
    Ok(None)
}

pub(crate) fn py_lob_impl<'py>(value: &Bound<'py, PyAny>) -> PyResult<Option<PyRef<'py, ThinLob>>> {
    if let Ok(lob) = value.extract::<PyRef<'py, ThinLob>>() {
        return Ok(Some(lob));
    }
    if value.hasattr("_impl")? {
        let impl_obj = value.getattr("_impl")?;
        if let Ok(lob) = impl_obj.extract::<PyRef<'py, ThinLob>>() {
            return Ok(Some(lob));
        }
    }
    Ok(None)
}

pub(crate) fn thin_lob_value_to_bind(py: Python<'_>, lob: &ThinLob) -> PyResult<BindValue> {
    if let Some(locator) = lob.locator.lock().map_err(runtime_error)?.as_ref() {
        return Ok(BindValue::Lob {
            ora_type_num: lob.ora_type_num,
            csfrm: lob.csfrm,
            locator: locator.clone(),
        });
    }
    let data = lob.read(py, 1, None)?;
    let data = data.bind(py);
    if matches!(lob.ora_type_num, ORA_TYPE_NUM_BLOB | ORA_TYPE_NUM_BFILE) {
        let bytes = data.cast::<PyBytes>()?;
        return Ok(BindValue::Raw(bytes.as_bytes().to_vec()));
    }
    Ok(BindValue::Text(data.extract::<String>()?))
}

pub(crate) fn py_is_async_lob(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    if value.extract::<PyRef<'_, AsyncThinLob>>().is_ok() {
        return Ok(true);
    }
    if value.hasattr("_impl")? {
        let impl_obj = value.getattr("_impl")?;
        return Ok(impl_obj.extract::<PyRef<'_, AsyncThinLob>>().is_ok());
    }
    Ok(false)
}

pub(crate) fn py_lob_value_to_bind(value: &Bound<'_, PyAny>) -> PyResult<Option<BindValue>> {
    if let Some(lob) = py_lob_impl(value)? {
        return thin_lob_value_to_bind(value.py(), &lob).map(Some);
    }
    if value.hasattr("_impl")? {
        let impl_obj = value.getattr("_impl")?;
        if let Ok(lob) = impl_obj.extract::<PyRef<'_, AsyncThinLob>>() {
            return thin_lob_value_to_bind(value.py(), &lob.inner).map(Some);
        }
    }
    Ok(None)
}

pub(crate) fn scalar_value_to_memory_lob(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    dbtype_name: &str,
) -> PyResult<Option<Py<PyAny>>> {
    let (ora_type_num, csfrm) = match dbtype_name {
        "DB_TYPE_BLOB" => (ORA_TYPE_NUM_BLOB, 0),
        "DB_TYPE_CLOB" => (ORA_TYPE_NUM_CLOB, CS_FORM_IMPLICIT),
        "DB_TYPE_NCLOB" => (ORA_TYPE_NUM_CLOB, CS_FORM_NCHAR),
        _ => return Ok(None),
    };
    if value.is_none() || py_lob_impl(value)?.is_some() || py_is_async_lob(value)? {
        return Ok(None);
    }
    let (data, size) = if ora_type_num == ORA_TYPE_NUM_BLOB {
        if let Ok(bytes) = value.cast::<PyBytes>() {
            let data = bytes.as_bytes().to_vec();
            let size = data.len() as u64;
            (data, size)
        } else {
            let text = value.extract::<String>()?;
            let data = text.into_bytes();
            let size = data.len() as u64;
            (data, size)
        }
    } else if let Ok(bytes) = value.cast::<PyBytes>() {
        let text = std::str::from_utf8(bytes.as_bytes()).map_err(runtime_error)?;
        (
            protocol_encode_lob_text(text, csfrm, None),
            text.chars().count() as u64,
        )
    } else {
        let text = value.extract::<String>()?;
        let size = text.chars().count() as u64;
        (protocol_encode_lob_text(&text, csfrm, None), size)
    };
    py_lob_from_impl(
        py,
        ThinLob {
            data: Some(Arc::new(Mutex::new(data))),
            locator: Arc::new(Mutex::new(None)),
            ora_type_num,
            csfrm,
            size,
            chunk_size: 0,
            context: None,
            is_open: Arc::new(Mutex::new(false)),
            bfile_name: None,
        },
    )
    .map(Some)
}

pub(crate) fn py_dbobject_element_to_bind(
    value: &Bound<'_, PyAny>,
    metadata: &DbObjectAttrImpl,
) -> PyResult<BindValue> {
    if metadata.dbtype_name == "DB_TYPE_BLOB" {
        if let Ok(text) = value.extract::<String>() {
            return Ok(BindValue::Raw(text.into_bytes()));
        }
    }
    py_value_to_bind(value)
}

/// Builds the bind value for a DbObject IN/IN-OUT bind. Prefers the native
/// packed-image path (`BindValue::ObjectInput`), which is the parity-correct
/// wire shape the reference always uses for `ORA_TYPE_NUM_OBJECT` binds
/// (reference messages/base.pyx:1518-1519). Falls back to the PL/SQL-array
/// flattening only when the type has no oid (cannot frame a native object).
pub(crate) fn dbobject_to_bind(py: Python<'_>, object: &DbObjectImpl) -> PyResult<BindValue> {
    let object_type = &object.object_type;
    if let Some(oid) = object_type.oid_bytes() {
        let image = object.pack_image(py)?;
        let buffer_size = u32::try_from(image.len()).unwrap_or(u32::MAX).max(1);
        return Ok(BindValue::ObjectInput {
            schema: object_type.schema.clone(),
            type_name: object_type.name.clone(),
            oid,
            version: object_type.version(),
            image,
            buffer_size,
        });
    }
    // No oid: cannot bind natively. Keep the scalar-collection array workaround.
    if let Some(bind) = dbobject_collection_to_array_bind(py, object)? {
        return Ok(bind);
    }
    Err(raise_python_value_not_supported("DbObject"))
}

pub(crate) fn dbobject_collection_to_array_bind(
    py: Python<'_>,
    object: &DbObjectImpl,
) -> PyResult<Option<BindValue>> {
    if !object.object_type.is_collection {
        return Ok(None);
    }
    let Some(metadata) = object.object_type.element_metadata.as_deref() else {
        return Ok(None);
    };
    if metadata.dbtype_name == "DB_TYPE_OBJECT" {
        return Ok(None);
    }
    object.ensure_unpacked(py)?;
    let elements = if object.object_type.is_assoc_array {
        object
            .assoc_values
            .lock()
            .map_err(runtime_error)?
            .values()
            .map(|value| value.clone_ref(py))
            .collect::<Vec<_>>()
    } else {
        object
            .collection_values
            .lock()
            .map_err(runtime_error)?
            .iter()
            .map(|value| value.clone_ref(py))
            .collect::<Vec<_>>()
    };
    let values = elements
        .iter()
        .map(|value| {
            let value = value.bind(py);
            if value.is_none() {
                Ok(None)
            } else {
                py_dbobject_element_to_bind(value, metadata).map(Some)
            }
        })
        .collect::<PyResult<Vec<_>>>()?;
    let (ora_type_num, csfrm, buffer_size) = values
        .iter()
        .find_map(|value| value.as_ref().and_then(bind_type_info))
        .unwrap_or_else(|| {
            let info = dbobject_element_bind_type_info(&metadata.dbtype_name, metadata.max_size);
            (info.ora_type_num, info.csfrm, info.buffer_size)
        });
    Ok(Some(BindValue::Array {
        ora_type_num,
        csfrm,
        buffer_size,
        max_elements: u32::try_from(values.len()).unwrap_or(u32::MAX).max(1),
        values,
    }))
}

pub(crate) fn bind_template_from_type(typ: &Bound<'_, PyAny>, size: u32) -> BindValue {
    bind_template_from_type_name(&py_type_name(typ), size)
}

pub(crate) fn return_kind_from_type_name(type_name: &str) -> ThinVarReturnKind {
    match type_name {
        "DB_TYPE_CLOB" | "CLOB" | "DB_TYPE_NCLOB" | "NCLOB" => ThinVarReturnKind::ClobAsLong,
        _ => ThinVarReturnKind::Plain,
    }
}

pub(crate) fn typed_lob_bind_hint_from_type_name(type_name: &str) -> Option<(u8, u8)> {
    match type_name {
        "DB_TYPE_CLOB" | "CLOB" => Some((ORA_TYPE_NUM_CLOB, CS_FORM_IMPLICIT)),
        "DB_TYPE_NCLOB" | "NCLOB" => Some((ORA_TYPE_NUM_CLOB, CS_FORM_NCHAR)),
        "DB_TYPE_BLOB" | "BLOB" => Some((ORA_TYPE_NUM_BLOB, 0)),
        _ => None,
    }
}

pub(crate) fn typed_lob_bind_hints(
    py: Python<'_>,
    bind_vars: &[Py<ThinVar>],
) -> Vec<Option<(u8, u8)>> {
    bind_vars
        .iter()
        .map(|var| typed_lob_bind_hint_from_type_name(&var.borrow(py).dbtype_name))
        .collect()
}

fn defaults_attr<'py>(py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyAny>> {
    PyModule::import(py, "oracledb")?
        .getattr("defaults")?
        .getattr(name)
}

pub(crate) fn default_fetch_lobs(py: Python<'_>) -> PyResult<bool> {
    defaults_attr(py, "fetch_lobs")?.extract()
}

pub(crate) fn default_fetch_decimals(py: Python<'_>) -> PyResult<bool> {
    defaults_attr(py, "fetch_decimals")?.extract()
}

/// (arraysize, prefetchrows) read from the live ``oracledb.defaults``
/// singleton, mirroring base/connection.pyx:223-224 which copies
/// ``C_DEFAULTS.arraysize/prefetchrows`` onto every new cursor impl.
pub(crate) fn default_cursor_sizes(py: Python<'_>) -> PyResult<(u32, u32)> {
    Ok((
        defaults_attr(py, "arraysize")?.extract()?,
        defaults_attr(py, "prefetchrows")?.extract()?,
    ))
}

pub(crate) fn python_decimal_from_text(py: Python<'_>, text: &str) -> PyResult<Py<PyAny>> {
    Ok(PyModule::import(py, "decimal")?
        .getattr("Decimal")?
        .call1((text,))?
        .unbind())
}

/// PL/SQL statements cannot bind VARCHAR/RAW values longer than 32767 bytes;
/// the reference driver promotes them to temporary LOBs
/// (impl/thin/var.pyx:53-90). Marking the bind with a LOB hint reuses the
/// existing temp-LOB materialization machinery.
pub(crate) fn promote_oversized_plsql_bind_hints(
    statement: &str,
    bind_values: &[BindValue],
    hints: &mut Vec<Option<(u8, u8)>>,
) {
    if !statement_is_plsql(statement) {
        return;
    }
    if hints.len() < bind_values.len() {
        hints.resize(bind_values.len(), None);
    }
    for (index, value) in bind_values.iter().enumerate() {
        if hints[index].is_some() {
            continue;
        }
        match value {
            BindValue::Text(text) if text.len() > 32_767 => {
                hints[index] = Some((ORA_TYPE_NUM_CLOB, CS_FORM_IMPLICIT));
            }
            BindValue::Raw(bytes) if bytes.len() > 32_767 => {
                hints[index] = Some((ORA_TYPE_NUM_BLOB, 0));
            }
            _ => {}
        }
    }
}

pub(crate) fn materialize_typed_lob_text_bind(
    connection: &mut RustConnection,
    value: &mut BindValue,
    ora_type_num: u8,
    csfrm: u8,
    call_timeout: Option<u32>,
) -> Result<(), String> {
    let BindValue::Text(text) = value else {
        return Ok(());
    };
    let text = std::mem::take(text);
    let mut locator = BlockingConnection::create_temp_lob(connection, ora_type_num, csfrm)
        .map_err(|err| err.to_string())?
        .locator;
    if !text.is_empty() {
        let bytes = protocol_encode_lob_text(&text, csfrm, Some(&locator));
        locator = BlockingConnection::write_lob_with_timeout(
            connection,
            &locator,
            1,
            &bytes,
            call_timeout,
        )
        .map_err(|err| err.to_string())?
        .locator;
    }
    *value = BindValue::Lob {
        ora_type_num,
        csfrm,
        locator,
    };
    Ok(())
}

pub(crate) fn materialize_typed_lob_raw_bind(
    connection: &mut RustConnection,
    value: &mut BindValue,
    ora_type_num: u8,
    csfrm: u8,
    call_timeout: Option<u32>,
) -> Result<(), String> {
    let BindValue::Raw(bytes) = value else {
        return Ok(());
    };
    let bytes = std::mem::take(bytes);
    let mut locator = BlockingConnection::create_temp_lob(connection, ora_type_num, csfrm)
        .map_err(|err| err.to_string())?
        .locator;
    if !bytes.is_empty() {
        locator = BlockingConnection::write_lob_with_timeout(
            connection,
            &locator,
            1,
            &bytes,
            call_timeout,
        )
        .map_err(|err| err.to_string())?
        .locator;
    }
    *value = BindValue::Lob {
        ora_type_num,
        csfrm,
        locator,
    };
    Ok(())
}

pub(crate) fn materialize_typed_lob_bind_values(
    connection: &mut RustConnection,
    values: &mut [BindValue],
    hints: &[Option<(u8, u8)>],
    call_timeout: Option<u32>,
) -> Result<(), String> {
    for (index, value) in values.iter_mut().enumerate() {
        let Some((ora_type_num, csfrm)) = hints.get(index).copied().flatten() else {
            continue;
        };
        materialize_typed_lob_text_bind(connection, value, ora_type_num, csfrm, call_timeout)?;
        materialize_typed_lob_raw_bind(connection, value, ora_type_num, csfrm, call_timeout)?;
    }
    Ok(())
}

/// For PL/SQL blocks a string or bytes bind larger than 32767 bytes must be
/// converted to a temporary CLOB/BLOB before execution (reference
/// impl/thin/var.pyx:53-71); the server rejects oversized character binds in
/// PL/SQL with ORA-01460.
pub(crate) fn materialize_plsql_long_binds(
    connection: &mut RustConnection,
    values: &mut [BindValue],
    call_timeout: Option<u32>,
) -> Result<(), String> {
    for value in values.iter_mut() {
        match value {
            BindValue::Text(text) if text.len() > 32_767 => {
                materialize_typed_lob_text_bind(
                    connection,
                    value,
                    ORA_TYPE_NUM_CLOB,
                    CS_FORM_IMPLICIT,
                    call_timeout,
                )?;
            }
            BindValue::Raw(bytes) if bytes.len() > 32_767 => {
                materialize_typed_lob_raw_bind(
                    connection,
                    value,
                    ORA_TYPE_NUM_BLOB,
                    0,
                    call_timeout,
                )?;
            }
            _ => {}
        }
    }
    Ok(())
}

pub(crate) fn materialize_typed_lob_bind_rows(
    connection: &mut RustConnection,
    rows: &mut [Vec<BindValue>],
    hints: &[Option<(u8, u8)>],
    call_timeout: Option<u32>,
) -> Result<(), String> {
    for row in rows {
        materialize_typed_lob_bind_values(connection, row, hints, call_timeout)?;
    }
    Ok(())
}

pub(crate) async fn materialize_typed_lob_text_bind_async(
    cx: &Cx,
    connection: &mut RustConnection,
    value: &mut BindValue,
    ora_type_num: u8,
    csfrm: u8,
    call_timeout: Option<u32>,
) -> Result<(), String> {
    let BindValue::Text(text) = value else {
        return Ok(());
    };
    let text = std::mem::take(text);
    let mut locator = connection
        .create_temp_lob(cx, ora_type_num, csfrm)
        .await
        .map_err(|err| err.to_string())?
        .locator;
    if !text.is_empty() {
        let bytes = protocol_encode_lob_text(&text, csfrm, Some(&locator));
        locator = connection
            .write_lob_with_timeout(cx, &locator, 1, &bytes, call_timeout)
            .await
            .map_err(|err| err.to_string())?
            .locator;
    }
    *value = BindValue::Lob {
        ora_type_num,
        csfrm,
        locator,
    };
    Ok(())
}

pub(crate) async fn materialize_typed_lob_raw_bind_async(
    cx: &Cx,
    connection: &mut RustConnection,
    value: &mut BindValue,
    ora_type_num: u8,
    csfrm: u8,
    call_timeout: Option<u32>,
) -> Result<(), String> {
    let BindValue::Raw(bytes) = value else {
        return Ok(());
    };
    let bytes = std::mem::take(bytes);
    let mut locator = connection
        .create_temp_lob(cx, ora_type_num, csfrm)
        .await
        .map_err(|err| err.to_string())?
        .locator;
    if !bytes.is_empty() {
        locator = connection
            .write_lob_with_timeout(cx, &locator, 1, &bytes, call_timeout)
            .await
            .map_err(|err| err.to_string())?
            .locator;
    }
    *value = BindValue::Lob {
        ora_type_num,
        csfrm,
        locator,
    };
    Ok(())
}

pub(crate) async fn materialize_typed_lob_bind_values_async(
    cx: &Cx,
    connection: &mut RustConnection,
    values: &mut [BindValue],
    hints: &[Option<(u8, u8)>],
    call_timeout: Option<u32>,
) -> Result<(), String> {
    for (index, value) in values.iter_mut().enumerate() {
        let Some((ora_type_num, csfrm)) = hints.get(index).copied().flatten() else {
            continue;
        };
        materialize_typed_lob_text_bind_async(
            cx,
            connection,
            value,
            ora_type_num,
            csfrm,
            call_timeout,
        )
        .await?;
        materialize_typed_lob_raw_bind_async(
            cx,
            connection,
            value,
            ora_type_num,
            csfrm,
            call_timeout,
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn materialize_typed_lob_bind_rows_async(
    cx: &Cx,
    connection: &mut RustConnection,
    rows: &mut [Vec<BindValue>],
    hints: &[Option<(u8, u8)>],
    call_timeout: Option<u32>,
) -> Result<(), String> {
    for row in rows {
        materialize_typed_lob_bind_values_async(cx, connection, row, hints, call_timeout).await?;
    }
    Ok(())
}

/// Async twin of [`materialize_plsql_long_binds`].
pub(crate) async fn materialize_plsql_long_binds_async(
    cx: &Cx,
    connection: &mut RustConnection,
    values: &mut [BindValue],
    call_timeout: Option<u32>,
) -> Result<(), String> {
    for value in values.iter_mut() {
        match value {
            BindValue::Text(text) if text.len() > 32_767 => {
                materialize_typed_lob_text_bind_async(
                    cx,
                    connection,
                    value,
                    ORA_TYPE_NUM_CLOB,
                    CS_FORM_IMPLICIT,
                    call_timeout,
                )
                .await?;
            }
            BindValue::Raw(bytes) if bytes.len() > 32_767 => {
                materialize_typed_lob_raw_bind_async(
                    cx,
                    connection,
                    value,
                    ORA_TYPE_NUM_BLOB,
                    0,
                    call_timeout,
                )
                .await?;
            }
            _ => {}
        }
    }
    Ok(())
}

pub(crate) fn bind_template_from_input_size(value: &Bound<'_, PyAny>) -> PyResult<BindValue> {
    if let Ok(size) = value.extract::<u32>() {
        return Ok(BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_VARCHAR,
            csfrm: CS_FORM_IMPLICIT,
            buffer_size: size.max(1),
        });
    }
    #[allow(clippy::match_result_ok)]
    // pre-existing lint at pre-split HEAD 978491a; not movement-induced
    if let Ok(tuple) = value.cast::<PyTuple>() {
        if let Some(typ) = tuple.get_item(0).ok() {
            let size = tuple
                .get_item(2)
                .ok()
                .and_then(|item| item.extract::<u32>().ok())
                .unwrap_or(0);
            return Ok(bind_template_from_type(&typ, size));
        }
    }
    #[allow(clippy::match_result_ok)]
    // pre-existing lint at pre-split HEAD 978491a; not movement-induced
    if let Ok(list) = value.cast::<PyList>() {
        if let Some(typ) = list.get_item(0).ok() {
            let size = list
                .get_item(2)
                .ok()
                .and_then(|item| item.extract::<u32>().ok())
                .unwrap_or(0);
            return Ok(bind_template_from_type(&typ, size));
        }
    }
    Ok(bind_template_from_type(value, 0))
}

pub(crate) fn bind_type_info(value: &BindValue) -> Option<(u8, u8, u32)> {
    bind_value_type_info(value).map(|info| (info.ora_type_num, info.csfrm, info.buffer_size))
}

pub(crate) fn bind_optional_text(value: Option<&str>) -> BindValue {
    value
        .map(|value| BindValue::Text(value.to_string()))
        .unwrap_or(BindValue::Null)
}

// d49: migrate to oracledb-protocol (fetch metadata supplement)
pub(crate) fn supplement_json_lob_column_metadata(
    connection: &Arc<Mutex<Option<RustConnection>>>,
    columns: &mut [ColumnMetadata],
    call_timeout: Option<u32>,
) -> PyResult<()> {
    let candidates = columns
        .iter()
        .enumerate()
        .filter(|(_, metadata)| {
            !metadata.is_json
                && matches!(metadata.ora_type_num, ORA_TYPE_NUM_CLOB | ORA_TYPE_NUM_BLOB)
                && !metadata.name.is_empty()
        })
        .map(|(index, metadata)| (index, metadata.name.to_ascii_uppercase()))
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Ok(());
    }
    let mut guard = connection.lock().map_err(runtime_error)?;
    let connection = guard.as_mut().ok_or_else(connection_closed_error)?;
    for (index, column_name) in candidates {
        let result = BlockingConnection::execute_query_with_binds_and_timeout(
            connection,
            "select 1 \
             from all_json_columns \
             where owner = sys_context('USERENV', 'CURRENT_SCHEMA') \
               and column_name = :1",
            1,
            &[BindValue::Text(column_name)],
            call_timeout,
        )
        .map_err(runtime_error)?;
        if !result.rows.is_empty() {
            columns[index].is_json = true;
        }
    }
    Ok(())
}

pub(crate) async fn supplement_json_lob_column_metadata_async(
    cx: &Cx,
    connection: &mut RustConnection,
    columns: &mut [ColumnMetadata],
    call_timeout: Option<u32>,
) -> Result<(), String> {
    let candidates = columns
        .iter()
        .enumerate()
        .filter(|(_, metadata)| {
            !metadata.is_json
                && matches!(metadata.ora_type_num, ORA_TYPE_NUM_CLOB | ORA_TYPE_NUM_BLOB)
                && !metadata.name.is_empty()
        })
        .map(|(index, metadata)| (index, metadata.name.to_ascii_uppercase()))
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Ok(());
    }
    for (index, column_name) in candidates {
        let result = connection
            .execute_query_with_binds_and_timeout(
                cx,
                "select 1 \
                 from all_json_columns \
                 where owner = sys_context('USERENV', 'CURRENT_SCHEMA') \
                   and column_name = :1",
                1,
                &[BindValue::Text(column_name)],
                call_timeout,
            )
            .await
            .map_err(|err| err.to_string())?;
        if !result.rows.is_empty() {
            columns[index].is_json = true;
        }
    }
    Ok(())
}

pub(crate) fn thin_lob_context_from_cursor(
    owner_cursor: &Bound<'_, PyAny>,
) -> PyResult<ThinLobContext> {
    let impl_obj = owner_cursor.getattr("_impl")?;
    if let Ok(cursor_impl) = impl_obj.extract::<PyRef<'_, ThinCursorImpl>>() {
        return Ok(ThinLobContext {
            connection: Arc::clone(&cursor_impl.connection),
            state: Arc::clone(&cursor_impl.state),
            async_mode: false,
        });
    }
    let cursor_impl = impl_obj.extract::<PyRef<'_, AsyncThinCursorImpl>>()?;
    Ok(ThinLobContext {
        connection: Arc::clone(&cursor_impl.inner.connection),
        state: Arc::clone(&cursor_impl.inner.state),
        async_mode: true,
    })
}

pub(crate) fn connection_object_type_impl(
    connection: &Bound<'_, PyAny>,
    name: &str,
) -> PyResult<DbObjectTypeImpl> {
    let impl_obj = connection.getattr("_impl")?;
    if let Ok(conn_impl) = impl_obj.extract::<PyRef<'_, ThinConnImpl>>() {
        return conn_impl.get_type(connection, name);
    }
    if let Ok(conn_impl) = impl_obj.extract::<PyRef<'_, AsyncThinConnImpl>>() {
        return conn_impl.inner.get_type(connection, name);
    }
    let public_type = connection.call_method1("gettype", (name,))?;
    py_db_object_type_impl(&public_type)?
        .ok_or_else(|| PyRuntimeError::new_err("gettype() did not return a DbObjectType"))
}

pub(crate) fn direct_lob_value_to_py(
    py: Python<'_>,
    ora_type_num: u8,
    csfrm: u8,
    locator: &[u8],
    context: &ThinLobContext,
) -> PyResult<Py<PyAny>> {
    let call_timeout = {
        let value = context.state.lock().map_err(runtime_error)?.call_timeout;
        (value > 0).then_some(value)
    };
    let mut guard = context.connection.lock().map_err(runtime_error)?;
    let connection = guard
        .as_mut()
        .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
    let result = BlockingConnection::read_lob_with_timeout(
        connection,
        locator,
        1,
        u64::from(u32::MAX),
        call_timeout,
    )
    .map_err(runtime_error)?;
    lob_data_to_py(
        py,
        ora_type_num,
        csfrm,
        Some(&result.locator),
        result.data.as_deref().unwrap_or_default(),
        1,
        None,
    )
}

/// Decode character data that failed strict text decoding in the protocol
/// layer using Python's codec machinery so the configured `encoding_errors`
/// policy (or a genuine `UnicodeDecodeError` when none is set) is honored,
/// exactly as the reference does (reference impl/base/converters.pyx:421-429).
pub(crate) fn text_raw_to_py_str(
    py: Python<'_>,
    bytes: &[u8],
    csfrm: u8,
    encoding_errors: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let encoding = if csfrm == CS_FORM_NCHAR {
        "utf-16-be"
    } else {
        "utf-8"
    };
    let py_bytes = PyBytes::new(py, bytes);
    let decoded = match encoding_errors {
        Some(errors) => py_bytes.call_method1("decode", (encoding, errors))?,
        None => py_bytes.call_method1("decode", (encoding,))?,
    };
    Ok(decoded.unbind())
}

pub(crate) fn interval_ds_to_py(
    py: Python<'_>,
    days: i32,
    hours: i32,
    minutes: i32,
    seconds: i32,
    fseconds: i32,
) -> PyResult<Py<PyAny>> {
    let timedelta = PyModule::import(py, "datetime")?.getattr("timedelta")?;
    let total_seconds = i64::from(hours) * 3600 + i64::from(minutes) * 60 + i64::from(seconds);
    Ok(timedelta
        .call1((days, total_seconds, i64::from(fseconds) / 1000))?
        .unbind())
}

pub(crate) fn query_value_to_py(
    py: Python<'_>,
    value: &Option<QueryValue>,
    owner_cursor: Option<&Bound<'_, PyAny>>,
    lob_context: Option<&ThinLobContext>,
    fetch_lobs: bool,
    fetch_decimals: bool,
) -> PyResult<Py<PyAny>> {
    match value {
        None => Ok(py.None()),
        Some(QueryValue::Text(value)) => Ok(value.clone().into_pyobject(py)?.unbind().into()),
        Some(QueryValue::TextRaw { bytes, csfrm }) => text_raw_to_py_str(py, bytes, *csfrm, None),
        Some(QueryValue::IntervalDS {
            days,
            hours,
            minutes,
            seconds,
            fseconds,
        }) => interval_ds_to_py(py, *days, *hours, *minutes, *seconds, *fseconds),
        // reference converters.pyx:222-228: INTERVAL YEAR TO MONTH
        // materializes as the oracledb.IntervalYM namedtuple
        Some(QueryValue::IntervalYM { years, months }) => Ok(PyModule::import(py, "oracledb")?
            .getattr("IntervalYM")?
            .call1((*years, *months))?
            .unbind()),
        // Native DB_TYPE_BOOLEAN fetches as a Python bool.
        Some(QueryValue::Boolean(value)) => {
            Ok(PyBool::new(py, *value).to_owned().into_any().unbind())
        }
        Some(QueryValue::Rowid(value)) => Ok(value.clone().into_pyobject(py)?.unbind().into()),
        #[allow(clippy::useless_conversion)]
        // pre-existing lint at pre-split HEAD 978491a; not movement-induced
        Some(QueryValue::Raw(value)) => Ok(value.clone().into_pyobject(py)?.unbind().into()),
        Some(QueryValue::BinaryDouble(value)) => {
            let value = value.parse::<f64>().map_err(runtime_error)?;
            Ok(value.into_pyobject(py)?.unbind().into())
        }
        Some(QueryValue::Number { text, is_integer }) => {
            // base/cursor.pyx:212-214: NUMBER columns fetch as decimal.Decimal
            // when defaults.fetch_decimals (or the per-cursor flag) is set.
            if fetch_decimals {
                Ok(PyModule::import(py, "decimal")?
                    .getattr("Decimal")?
                    .call1((text.as_str(),))?
                    .unbind())
            } else if *is_integer {
                python_int_from_decimal_text(py, text)
            } else {
                let value = text.parse::<f64>().map_err(runtime_error)?;
                Ok(value.into_pyobject(py)?.unbind().into())
            }
        }
        Some(QueryValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
        }) => {
            let datetime = PyModule::import(py, "datetime")?.getattr("datetime")?;
            let microsecond = nanosecond / 1000;
            Ok(datetime
                .call1((*year, *month, *day, *hour, *minute, *second, microsecond))?
                .unbind())
        }
        Some(QueryValue::Array(values)) => {
            let values = values
                .iter()
                .map(|value| {
                    query_value_to_py(
                        py,
                        value,
                        owner_cursor,
                        lob_context,
                        fetch_lobs,
                        fetch_decimals,
                    )
                })
                .collect::<PyResult<Vec<_>>>()?;
            Ok(PyList::new(py, values)?.unbind().into())
        }
        // dense VECTOR -> array.array, sparse VECTOR -> oracledb.SparseVector
        Some(QueryValue::Vector(vector)) => vector_to_py(py, vector),
        Some(QueryValue::Cursor { columns, cursor_id }) => {
            let Some(owner_cursor) = owner_cursor else {
                return Err(not_implemented("ThinCursorImpl cursor value conversion"));
            };
            let connection = owner_cursor.getattr("connection")?;
            let child_cursor = connection.call_method0("cursor")?;
            hydrate_cursor_impl(&child_cursor, columns, *cursor_id, false)?;
            Ok(child_cursor.unbind())
        }
        Some(QueryValue::Lob {
            ora_type_num,
            csfrm,
            locator,
            size,
            chunk_size,
        }) => {
            let context = match (lob_context, owner_cursor) {
                (Some(context), _) => context.clone(),
                (None, Some(owner_cursor)) => thin_lob_context_from_cursor(owner_cursor)?,
                (None, None) => return Err(not_implemented("ThinCursorImpl LOB value conversion")),
            };
            if !fetch_lobs {
                return direct_lob_value_to_py(py, *ora_type_num, *csfrm, locator, &context);
            }
            let bfile_name = (*ora_type_num == ORA_TYPE_NUM_BFILE)
                .then(|| decode_bfile_locator_name(locator))
                .flatten();
            py_lob_from_impl(
                py,
                ThinLob {
                    data: None,
                    locator: Arc::new(Mutex::new(Some(locator.clone()))),
                    ora_type_num: *ora_type_num,
                    csfrm: *csfrm,
                    size: *size,
                    chunk_size: *chunk_size,
                    context: Some(context),
                    is_open: Arc::new(Mutex::new(false)),
                    bfile_name,
                },
            )
        }
        Some(QueryValue::Object {
            schema,
            type_name,
            packed_data,
        }) => {
            if type_name
                .as_deref()
                .is_some_and(|name| name.eq_ignore_ascii_case("XMLTYPE"))
            {
                return decode_dbobject_xmltype(py, packed_data);
            }
            let Some(owner_cursor) = owner_cursor else {
                return Err(not_implemented("ThinCursorImpl DbObject value conversion"));
            };
            let type_name = type_name
                .as_deref()
                .ok_or_else(|| PyRuntimeError::new_err("missing DbObject type name"))?;
            let fqn = schema
                .as_deref()
                .filter(|schema| !schema.is_empty())
                .map(|schema| format!("{schema}.{type_name}"))
                .unwrap_or_else(|| type_name.to_string());
            let connection = owner_cursor.getattr("connection")?;
            let object_type = connection_object_type_impl(&connection, &fqn)?;
            let lob_context = match lob_context {
                Some(context) => Some(context.clone()),
                None => Some(thin_lob_context_from_cursor(owner_cursor)?),
            };
            py_db_object_from_impl(
                py,
                DbObjectImpl::with_packed_data(object_type, packed_data.clone(), lob_context),
            )
        }
        // Native Oracle JSON (DB_TYPE_JSON): the OSON image was decoded by the
        // protocol layer into an OsonValue tree; marshal it to Python objects.
        Some(QueryValue::Json(value)) => oson_value_to_py(py, value),
    }
}

/// Converts a decoded [`OsonValue`] to its Python equivalent, matching
/// python-oracledb's OSON decode: numbers -> `decimal.Decimal` (lossless),
/// binary float/double -> `float`, strings -> `str`, raw -> `bytes`,
/// dates/timestamps -> `datetime.datetime`, INTERVAL DS -> `datetime.timedelta`,
/// objects -> `dict` (insertion order preserved), arrays -> `list`.
pub(crate) fn oson_value_to_py(py: Python<'_>, value: &OsonValue) -> PyResult<Py<PyAny>> {
    match value {
        OsonValue::Null => Ok(py.None()),
        OsonValue::Bool(value) => Ok(PyBool::new(py, *value).to_owned().unbind().into()),
        // OSON numbers always decode to decimal.Decimal in python-oracledb so
        // arbitrary precision is preserved (verified against the live driver).
        OsonValue::Number(text) => Ok(PyModule::import(py, "decimal")?
            .getattr("Decimal")?
            .call1((text.as_str(),))?
            .unbind()),
        OsonValue::BinaryFloat(value) => Ok(f64::from(*value).into_pyobject(py)?.unbind().into()),
        OsonValue::BinaryDouble(value) => Ok((*value).into_pyobject(py)?.unbind().into()),
        OsonValue::String(text) => Ok(text.clone().into_pyobject(py)?.unbind().into()),
        OsonValue::Raw(bytes) => Ok(PyBytes::new(py, bytes).unbind().into()),
        OsonValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
        } => {
            let datetime = PyModule::import(py, "datetime")?.getattr("datetime")?;
            let microsecond = nanosecond / 1000;
            Ok(datetime
                .call1((*year, *month, *day, *hour, *minute, *second, microsecond))?
                .unbind())
        }
        OsonValue::IntervalDS {
            days,
            hours,
            minutes,
            seconds,
            fseconds,
        } => interval_ds_to_py(py, *days, *hours, *minutes, *seconds, *fseconds),
        OsonValue::Vector(vector) => vector_to_py(py, vector),
        OsonValue::Array(values) => {
            let items = values
                .iter()
                .map(|item| oson_value_to_py(py, item))
                .collect::<PyResult<Vec<_>>>()?;
            Ok(PyList::new(py, items)?.unbind().into())
        }
        OsonValue::Object(entries) => {
            let dict = PyDict::new(py);
            for (key, child) in entries {
                dict.set_item(key, oson_value_to_py(py, child)?)?;
            }
            Ok(dict.unbind().into())
        }
    }
}

pub(crate) fn json_query_value_to_py(
    py: Python<'_>,
    value: &Option<QueryValue>,
    owner_cursor: Option<&Bound<'_, PyAny>>,
    lob_context: Option<&ThinLobContext>,
) -> PyResult<Py<PyAny>> {
    // Mirrors the reference is_json text converter
    // (impl/base/cursor.pyx `_build_json_converter_fn`): bytes are decoded to
    // text and an empty/false value yields None (the `if value:` guard) so that
    // an empty CLOB/BLOB column is not passed to json.loads (which would raise
    // "Expecting value"). fetch_lobs=false materializes any CLOB/BLOB to
    // str/bytes already, so there is no LOB object left to read here.
    let value = query_value_to_py(py, value, owner_cursor, lob_context, false, false)?;
    let mut value = value.into_bound(py);
    if value.is_none() {
        return Ok(value.unbind());
    }
    if let Ok(bytes) = value.cast::<PyBytes>() {
        value = bytes.call_method0("decode")?;
    }
    if !value.is_truthy()? {
        return Ok(py.None());
    }
    Ok(PyModule::import(py, "json")?
        .getattr("loads")?
        .call1((&value,))?
        .unbind())
}
