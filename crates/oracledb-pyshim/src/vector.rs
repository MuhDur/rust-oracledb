//! Marshalling between Python VECTOR surfaces and the protocol `Vector`.
//!
//! Python represents dense vectors as `array.array` with typecode `f`/`d`/`b`/
//! `B` (or a plain `list`, which the reference coerces to `array('d')`) and
//! sparse vectors as `oracledb.SparseVector`. The protocol layer owns the wire
//! encoding; this module only converts between the two value models, reading
//! `array.array` element bytes via the safe `tobytes()` / `frombytes()` API
//! (the crate forbids `unsafe`, so the buffer protocol is off-limits).

use oracledb::protocol::vector::{Vector, VectorValues};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyBytesMethods, PyList};

use crate::errors::{raise_invalid_vector, raise_unsupported_python_type_for_db_type};
use crate::pyutil::py_value_type_name;

const DB_TYPE_VECTOR: &str = "DB_TYPE_VECTOR";

/// True if `value` is an `array.array`.
fn is_array_array(value: &Bound<'_, PyAny>) -> bool {
    py_value_type_name(value) == "array"
}

/// True if `value` is an `oracledb.SparseVector`.
pub(crate) fn is_sparse_vector(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    let sparse_type = PyModule::import(value.py(), "oracledb")?.getattr("SparseVector")?;
    value.is_instance(&sparse_type)
}

/// True if `value` is any VECTOR-shaped Python value (array.array or
/// SparseVector). A plain `list` is *not* included here: lists only bind as a
/// vector when an explicit `DB_TYPE_VECTOR` input type is supplied.
pub(crate) fn is_vector_value(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    Ok(is_array_array(value) || is_sparse_vector(value)?)
}

/// Read an `array.array`'s native-endian bytes into typed `VectorValues`.
/// Rejects typecodes other than `f`/`d`/`b`/`B` with DPY-3013, matching the
/// reference `_check_value` for `DB_TYPE_VECTOR`.
fn array_to_vector_values(value: &Bound<'_, PyAny>) -> PyResult<VectorValues> {
    let typecode = value.getattr("typecode")?.extract::<String>()?;
    let bytes_obj = value.call_method0("tobytes")?;
    let raw = bytes_obj.cast::<PyBytes>()?;
    let raw = raw.as_bytes();
    match typecode.as_str() {
        "f" => {
            let mut out = Vec::with_capacity(raw.len() / 4);
            for chunk in raw.chunks_exact(4) {
                out.push(f32::from_ne_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
            }
            Ok(VectorValues::Float32(out))
        }
        "d" => {
            let mut out = Vec::with_capacity(raw.len() / 8);
            for chunk in raw.chunks_exact(8) {
                out.push(f64::from_ne_bytes([
                    chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
                ]));
            }
            Ok(VectorValues::Float64(out))
        }
        "b" => Ok(VectorValues::Int8(raw.iter().map(|b| *b as i8).collect())),
        "B" => Ok(VectorValues::Binary(raw.to_vec())),
        _ => Err(raise_unsupported_python_type_for_db_type(
            value,
            DB_TYPE_VECTOR,
        )),
    }
}

/// Convert a Python VECTOR value (`array.array` or `SparseVector`) to the
/// protocol model. `allow_list` enables the reference's list -> `array('d')`
/// coercion that only applies when binding through a `DB_TYPE_VECTOR` type.
pub(crate) fn py_to_vector(value: &Bound<'_, PyAny>, allow_list: bool) -> PyResult<Vector> {
    if is_sparse_vector(value)? {
        let num_dimensions = value.getattr("num_dimensions")?.extract::<u32>()?;
        let indices_obj = value.getattr("indices")?;
        let indices = indices_obj
            .try_iter()?
            .map(|item| item?.extract::<u32>())
            .collect::<PyResult<Vec<u32>>>()?;
        let values = array_to_vector_values(&value.getattr("values")?)?;
        // a SparseVector is never rejected client-side for emptiness: an empty
        // sparse vector with positive num_dimensions is a valid all-zero vector,
        // and a zero-dimension sparse vector is left to the server to reject
        // (ORA-51803/51862). The reference `_check_value` passes SparseVector
        // through unconditionally (connection.pyx:153-154).
        return Ok(Vector::Sparse {
            num_dimensions,
            indices,
            values,
        });
    }
    if is_array_array(value) {
        let values = array_to_vector_values(value)?;
        if values.is_empty() {
            return Err(raise_invalid_vector());
        }
        return Ok(Vector::Dense(values));
    }
    if allow_list {
        if let Ok(list) = value.cast::<PyList>() {
            if list.is_empty() {
                return Err(raise_invalid_vector());
            }
            // reference coerces a plain list to array('d')
            let floats = list
                .iter()
                .map(|item| item.extract::<f64>())
                .collect::<PyResult<Vec<f64>>>()?;
            return Ok(Vector::Dense(VectorValues::Float64(floats)));
        }
    }
    Err(raise_unsupported_python_type_for_db_type(
        value,
        DB_TYPE_VECTOR,
    ))
}

/// Build a Python `array.array(typecode, values)`.
fn build_array(py: Python<'_>, typecode: &str, bytes: &[u8]) -> PyResult<Py<PyAny>> {
    let array_mod = PyModule::import(py, "array")?;
    let arr = array_mod
        .getattr("array")?
        .call1((typecode, PyBytes::new(py, &[])))?;
    arr.call_method1("frombytes", (PyBytes::new(py, bytes),))?;
    Ok(arr.unbind())
}

/// Materialize `VectorValues` as the matching Python `array.array`.
fn values_to_array(py: Python<'_>, values: &VectorValues) -> PyResult<Py<PyAny>> {
    match values {
        VectorValues::Float32(v) => {
            let mut bytes = Vec::with_capacity(v.len() * 4);
            for value in v {
                bytes.extend_from_slice(&value.to_ne_bytes());
            }
            build_array(py, "f", &bytes)
        }
        VectorValues::Float64(v) => {
            let mut bytes = Vec::with_capacity(v.len() * 8);
            for value in v {
                bytes.extend_from_slice(&value.to_ne_bytes());
            }
            build_array(py, "d", &bytes)
        }
        VectorValues::Int8(v) => {
            let bytes: Vec<u8> = v.iter().map(|x| *x as u8).collect();
            build_array(py, "b", &bytes)
        }
        VectorValues::Binary(v) => build_array(py, "B", v),
    }
}

/// Convert a decoded protocol `Vector` to its Python surface: dense vectors
/// become `array.array`; sparse vectors become `oracledb.SparseVector`.
pub(crate) fn vector_to_py(py: Python<'_>, vector: &Vector) -> PyResult<Py<PyAny>> {
    match vector {
        Vector::Dense(values) => values_to_array(py, values),
        Vector::Sparse {
            num_dimensions,
            indices,
            values,
        } => {
            // The SparseVector constructor normalizes indices to its uint32
            // array typecode, so a plain list of ints is accepted directly.
            let indices_list = PyList::new(py, indices)?;
            let values_arr = values_to_array(py, values)?;
            let sparse_cls = PyModule::import(py, "oracledb")?.getattr("SparseVector")?;
            Ok(sparse_cls
                .call1((*num_dimensions, indices_list, values_arr))?
                .unbind())
        }
    }
}
