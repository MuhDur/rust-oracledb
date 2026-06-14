//! Arrow C Data Interface PyCapsule export for the DataFrame fetch surface.
//!
//! The public `oracledb.dataframe.DataFrame` / `oracledb.arrow_array.ArrowArray`
//! classes are duck-typed against an implementation object that exposes
//! `get_arrays()` / `get_stream_capsule()` (DataFrameImpl) and
//! `get_schema_capsule()` / `get_array_capsule()` / `get_data_type()` /
//! `get_name()` / `get_null_count()` / `get_num_rows()` (ArrowArrayImpl). The
//! `requested_schema` path additionally drives `ArrowSchemaImpl.from_arrow_schema`.
//!
//! This module is the *only* place the crate permits `unsafe`. It implements the
//! Arrow PyCapsule protocol
//! (<https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html>):
//! a heap-allocated `FFI_ArrowArray` / `FFI_ArrowSchema` / `FFI_ArrowArrayStream`
//! is moved into a `PyCapsule` named `"arrow_array"` / `"arrow_schema"` /
//! `"arrow_array_stream"`. The capsule destructor reclaims the `Box`; the arrow-rs
//! FFI struct's own `Drop` invokes the C release callback when the consumer has
//! not already moved (released) the struct, so ownership transfer is leak-free in
//! both the consumed and unconsumed cases.
#![allow(unsafe_code)]

use std::ffi::{c_void, CStr};
use std::ptr::NonNull;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::ffi::{to_ffi, FFI_ArrowArray};
use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::types::{
    Date32Type, Date64Type, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchIterator};
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use oracledb::arrow::arrow_type_name;
use pyo3::exceptions::PyValueError;
use pyo3::ffi as pyffi;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyList, PyTuple};
use pyo3::IntoPyObjectExt;

use crate::{dpy_database_error, runtime_error};

const CAP_SCHEMA: &CStr = c"arrow_schema";
const CAP_ARRAY: &CStr = c"arrow_array";
const CAP_STREAM: &CStr = c"arrow_array_stream";

/// Generic capsule destructor for a boxed arrow-rs FFI struct `T`.
///
/// # Safety
/// `capsule` must be a `PyCapsule` whose stored pointer was produced by
/// `Box::into_raw(Box::<T>::new(..))` under `name`, exactly as in
/// [`new_ffi_capsule`]. PyO3 guarantees this destructor is only invoked once,
/// when the capsule is collected. Reconstructing and dropping the `Box` runs
/// `T`'s `Drop`, which calls the Arrow C release callback iff the consumer has
/// not already moved the struct out (in which case `release` is null and the
/// drop is a no-op). The function must not unwind across the FFI boundary; all
/// operations here are non-panicking pointer reads and a `Box` drop.
unsafe extern "C" fn capsule_destructor<T>(capsule: *mut pyffi::PyObject, name: *const i8) {
    // SAFETY: `PyCapsule_GetPointer` returns the pointer we stored, or null on a
    // name mismatch (which cannot happen here as we pass the same name).
    let ptr = unsafe { pyffi::PyCapsule_GetPointer(capsule, name) };
    if ptr.is_null() {
        // Clear the error PyCapsule_GetPointer set; nothing to free.
        unsafe { pyffi::PyErr_Clear() };
        return;
    }
    // SAFETY: `ptr` was created via `Box::into_raw` of a `Box<T>`; reclaiming it
    // here transfers ownership back so the `Box` (and `T`'s release callback)
    // runs exactly once.
    drop(unsafe { Box::from_raw(ptr.cast::<T>()) });
}

unsafe extern "C" fn schema_destructor(capsule: *mut pyffi::PyObject) {
    // SAFETY: capsule stores a `Box<FFI_ArrowSchema>` under `CAP_SCHEMA`.
    unsafe { capsule_destructor::<FFI_ArrowSchema>(capsule, CAP_SCHEMA.as_ptr()) }
}

unsafe extern "C" fn array_destructor(capsule: *mut pyffi::PyObject) {
    // SAFETY: capsule stores a `Box<FFI_ArrowArray>` under `CAP_ARRAY`.
    unsafe { capsule_destructor::<FFI_ArrowArray>(capsule, CAP_ARRAY.as_ptr()) }
}

unsafe extern "C" fn stream_destructor(capsule: *mut pyffi::PyObject) {
    // SAFETY: capsule stores a `Box<FFI_ArrowArrayStream>` under `CAP_STREAM`.
    unsafe { capsule_destructor::<FFI_ArrowArrayStream>(capsule, CAP_STREAM.as_ptr()) }
}

/// Moves a boxed arrow-rs FFI struct into a named `PyCapsule`.
fn new_ffi_capsule<'py, T>(
    py: Python<'py>,
    value: T,
    name: &'static CStr,
    destructor: unsafe extern "C" fn(*mut pyffi::PyObject),
) -> PyResult<Bound<'py, PyCapsule>> {
    let ptr = NonNull::new(Box::into_raw(Box::new(value)).cast::<c_void>())
        .ok_or_else(|| runtime_error("failed to allocate Arrow FFI capsule"))?;
    // SAFETY: `ptr` is a freshly leaked `Box<T>` (non-null, properly aligned,
    // uniquely owned). `name` is a `'static CStr`. `destructor` reclaims exactly
    // this `Box<T>` under exactly this `name`. Ownership of the box transfers to
    // the capsule, whose destructor CPython invokes exactly once on collection.
    // If capsule creation itself fails (only on a Python `MemoryError`), the
    // destructor is never registered and the box leaks; that is a benign leak on
    // an OOM path, never a double-free or use-after-free.
    unsafe { PyCapsule::new_with_pointer_and_destructor(py, ptr, name, Some(destructor)) }
}

fn schema_capsule<'py>(
    py: Python<'py>,
    schema: FFI_ArrowSchema,
) -> PyResult<Bound<'py, PyCapsule>> {
    new_ffi_capsule(py, schema, CAP_SCHEMA, schema_destructor)
}

fn array_capsule<'py>(py: Python<'py>, array: FFI_ArrowArray) -> PyResult<Bound<'py, PyCapsule>> {
    new_ffi_capsule(py, array, CAP_ARRAY, array_destructor)
}

fn stream_capsule<'py>(
    py: Python<'py>,
    stream: FFI_ArrowArrayStream,
) -> PyResult<Bound<'py, PyCapsule>> {
    new_ffi_capsule(py, stream, CAP_STREAM, stream_destructor)
}

fn ffi_schema_for_field(field: &Field) -> PyResult<FFI_ArrowSchema> {
    FFI_ArrowSchema::try_from(field).map_err(runtime_error)
}

fn ffi_for_array(array: &ArrayRef) -> PyResult<(FFI_ArrowArray, FFI_ArrowSchema)> {
    to_ffi(&array.to_data()).map_err(runtime_error)
}

/// One fetched column: a named field plus its Arrow array. Backs the public
/// `ArrowArray` object (`ArrowArray._from_impl(impl)`).
#[pyclass(module = "oracledb.arrow_impl", name = "ArrowArrayImpl")]
pub(crate) struct ArrowArrayImpl {
    field: Arc<Field>,
    array: ArrayRef,
}

impl ArrowArrayImpl {
    pub(crate) fn new(field: Arc<Field>, array: ArrayRef) -> Self {
        Self { field, array }
    }
}

#[pymethods]
impl ArrowArrayImpl {
    /// Returns an `ArrowSchema` PyCapsule for this column (with its name).
    fn get_schema_capsule<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        schema_capsule(py, ffi_schema_for_field(&self.field)?)
    }

    /// Returns an `ArrowArray` PyCapsule holding the column data.
    fn get_array_capsule<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let (array, _schema) = ffi_for_array(&self.array)?;
        array_capsule(py, array)
    }

    /// nanoarrow-style type name (e.g. `int64`, `large_string`, `decimal128`).
    fn get_data_type(&self) -> String {
        arrow_type_name(self.field.data_type())
    }

    fn get_name(&self) -> String {
        self.field.name().clone()
    }

    fn get_null_count(&self) -> i64 {
        self.array.null_count() as i64
    }

    fn get_num_rows(&self) -> i64 {
        self.array.len() as i64
    }
}

/// A whole fetched DataFrame: the schema plus one array per column. Backs the
/// public `DataFrame` object (`DataFrame._from_impl(impl)`).
#[pyclass(module = "oracledb.arrow_impl", name = "DataFrameImpl")]
pub(crate) struct DataFrameImpl {
    batch: RecordBatch,
}

impl DataFrameImpl {
    pub(crate) fn new(batch: RecordBatch) -> Self {
        Self { batch }
    }
}

#[pymethods]
impl DataFrameImpl {
    /// One `ArrowArrayImpl` per column, in select-list order.
    fn get_arrays(&self, py: Python<'_>) -> PyResult<Vec<Py<ArrowArrayImpl>>> {
        let schema = self.batch.schema();
        let mut arrays = Vec::with_capacity(self.batch.num_columns());
        for (index, field) in schema.fields().iter().enumerate() {
            let impl_obj = ArrowArrayImpl::new(field.clone(), self.batch.column(index).clone());
            arrays.push(Py::new(py, impl_obj)?);
        }
        Ok(arrays)
    }

    /// An `ArrowArrayStream` PyCapsule streaming this single batch.
    fn get_stream_capsule<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let schema = self.batch.schema();
        let batch = self.batch.clone();
        let reader = RecordBatchIterator::new(std::iter::once(Ok(batch)), schema);
        let stream = FFI_ArrowArrayStream::new(Box::new(reader));
        stream_capsule(py, stream)
    }
}

/// A coercion schema supplied via `fetch_df_*(requested_schema=...)`. Backs the
/// cursor's `schema_impl`; the cursor reads [`ArrowSchemaImpl::schema`].
#[pyclass(module = "oracledb.arrow_impl", name = "ArrowSchemaImpl")]
pub(crate) struct ArrowSchemaImpl {
    schema: SchemaRef,
}

impl ArrowSchemaImpl {
    pub(crate) fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[pymethods]
impl ArrowSchemaImpl {
    /// Consumes an object implementing the Arrow PyCapsule schema interface
    /// (`__arrow_c_schema__`) into a Rust [`Schema`].
    #[staticmethod]
    fn from_arrow_schema(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        let schema = import_schema(obj)?;
        Ok(Self {
            schema: Arc::new(schema),
        })
    }
}

/// Imports an `__arrow_c_schema__` capsule into an arrow-rs [`Schema`].
///
/// The requested schema describes the *columns* of the result; pyarrow exports a
/// `struct` schema whose children are the per-column fields. We unwrap a single
/// top-level struct into a flat column schema (matching pyarrow.schema()).
fn import_schema(obj: &Bound<'_, PyAny>) -> PyResult<Schema> {
    let capsule_obj = obj.call_method0("__arrow_c_schema__")?;
    let capsule = capsule_obj
        .cast::<PyCapsule>()
        .map_err(|_| PyValueError::new_err("__arrow_c_schema__ did not return a PyCapsule"))?;
    // SAFETY: a well-formed `arrow_schema` capsule stores a pointer to a valid,
    // consumer-owned `FFI_ArrowSchema` (Arrow PyCapsule protocol). We read it by
    // reference and deep-clone via `Schema::try_from`; we do not take ownership
    // of the producer's struct, so we must not run its release callback.
    //
    // Lifetime: `&*ptr` produces a reference whose lifetime the borrow checker
    // does NOT tie to `capsule_obj` (it is reborrowed from a raw pointer). The
    // capsule owns the `FFI_ArrowSchema`, so the borrow is valid only while
    // `capsule_obj` is alive. We use `ffi_schema` solely below, before
    // `capsule_obj` (a function-scoped local) is dropped, so it never dangles;
    // the deep clone finishes before the capsule can be collected.
    let ffi_schema: &FFI_ArrowSchema = unsafe {
        let ptr = pyffi::PyCapsule_GetPointer(capsule.as_ptr(), CAP_SCHEMA.as_ptr());
        if ptr.is_null() {
            return Err(PyValueError::new_err("invalid arrow_schema capsule"));
        }
        &*ptr.cast::<FFI_ArrowSchema>()
    };
    match Schema::try_from(ffi_schema) {
        Ok(schema) => Ok(schema),
        Err(_) => {
            // pyarrow.schema(...) exports the column list as a top-level struct.
            let field = Field::try_from(ffi_schema).map_err(runtime_error)?;
            match field.data_type() {
                arrow_schema::DataType::Struct(children) => Ok(Schema::new(
                    children.iter().map(Arc::clone).collect::<Vec<_>>(),
                )),
                _ => Err(runtime_error("requested_schema is not a struct/schema")),
            }
        }
    }
}

/// True when `obj` implements the Arrow PyCapsule stream interface
/// (`__arrow_c_stream__`) — i.e. is a DataFrame / pyarrow.Table to ingest.
pub(crate) fn has_arrow_c_stream(obj: &Bound<'_, PyAny>) -> bool {
    obj.hasattr("__arrow_c_stream__").unwrap_or(false)
}

/// Imports an `__arrow_c_stream__`-bearing object into a list of [`RecordBatch`].
fn import_arrow_batches(obj: &Bound<'_, PyAny>) -> PyResult<Vec<RecordBatch>> {
    let capsule_obj = obj.call_method0("__arrow_c_stream__")?;
    let capsule = capsule_obj
        .cast::<PyCapsule>()
        .map_err(|_| PyValueError::new_err("__arrow_c_stream__ did not return a PyCapsule"))?;
    // SAFETY: a well-formed `arrow_array_stream` capsule stores a pointer to a
    // valid, consumer-owned `FFI_ArrowArrayStream` (Arrow PyCapsule protocol).
    // `ArrowArrayStreamReader::from_raw` moves the stream out (taking ownership of
    // the producer's release callback), leaving the capsule's struct released so
    // its own destructor is a no-op. We must do this exactly once per capsule.
    let mut reader = unsafe {
        let ptr = pyffi::PyCapsule_GetPointer(capsule.as_ptr(), CAP_STREAM.as_ptr());
        if ptr.is_null() {
            return Err(PyValueError::new_err("invalid arrow_array_stream capsule"));
        }
        ArrowArrayStreamReader::from_raw(ptr.cast::<FFI_ArrowArrayStream>())
            .map_err(runtime_error)?
    };
    let mut batches = Vec::new();
    for batch in reader.by_ref() {
        batches.push(batch.map_err(runtime_error)?);
    }
    Ok(batches)
}

/// Result of materializing an Arrow table for ingestion.
pub(crate) struct ArrowIngestRows<'py> {
    /// Row tuples of native Python values for the existing bind path.
    pub rows: Bound<'py, PyList>,
    /// Row count of each Arrow chunk/batch, so the executemany manager can keep
    /// batches from spanning chunk boundaries (reference BatchLoadManager).
    pub chunk_lengths: Vec<usize>,
    /// Column indices whose Arrow type is timestamp; their bind values must be
    /// encoded as TIMESTAMP (not DATE) so fractional seconds survive.
    pub timestamp_columns: Vec<usize>,
}

/// Converts an `__arrow_c_stream__` object (DataFrame / pyarrow.Table) into a
/// Python list of row tuples carrying native Python values, so the existing
/// executemany bind path (type inference + wire encode) consumes it unchanged.
pub(crate) fn arrow_table_to_py_rows<'py>(
    py: Python<'py>,
    obj: &Bound<'py, PyAny>,
) -> PyResult<ArrowIngestRows<'py>> {
    let batches = import_arrow_batches(obj)?;
    let rows = PyList::empty(py);
    let mut chunk_lengths = Vec::with_capacity(batches.len());
    let mut timestamp_columns = Vec::new();
    if let Some(first) = batches.first() {
        for (index, field) in first.schema().fields().iter().enumerate() {
            if matches!(field.data_type(), DataType::Timestamp(_, _)) {
                timestamp_columns.push(index);
            }
        }
    }
    for batch in &batches {
        if batch.num_rows() > 0 {
            chunk_lengths.push(batch.num_rows());
        }
        for row_index in 0..batch.num_rows() {
            let cells = (0..batch.num_columns())
                .map(|col| arrow_cell_to_py(py, batch.column(col), row_index))
                .collect::<PyResult<Vec<_>>>()?;
            rows.append(PyTuple::new(py, cells)?)?;
        }
    }
    Ok(ArrowIngestRows {
        rows,
        chunk_lengths,
        timestamp_columns,
    })
}

/// Converts a single Arrow cell to the native Python value the bind path
/// expects (int/float/Decimal/str/bytes/bool/datetime/None).
fn arrow_cell_to_py(py: Python<'_>, array: &ArrayRef, row: usize) -> PyResult<Py<PyAny>> {
    if array.is_null(row) {
        return Ok(py.None());
    }
    let value: Py<PyAny> = match array.data_type() {
        DataType::Null => py.None(),
        DataType::Boolean => array.as_boolean().value(row).into_py_any(py)?,
        DataType::Int8 => array
            .as_primitive::<Int8Type>()
            .value(row)
            .into_py_any(py)?,
        DataType::Int16 => array
            .as_primitive::<Int16Type>()
            .value(row)
            .into_py_any(py)?,
        DataType::Int32 => array
            .as_primitive::<Int32Type>()
            .value(row)
            .into_py_any(py)?,
        DataType::Int64 => array
            .as_primitive::<Int64Type>()
            .value(row)
            .into_py_any(py)?,
        DataType::UInt8 => array
            .as_primitive::<UInt8Type>()
            .value(row)
            .into_py_any(py)?,
        DataType::UInt16 => array
            .as_primitive::<UInt16Type>()
            .value(row)
            .into_py_any(py)?,
        DataType::UInt32 => array
            .as_primitive::<UInt32Type>()
            .value(row)
            .into_py_any(py)?,
        DataType::UInt64 => array
            .as_primitive::<UInt64Type>()
            .value(row)
            .into_py_any(py)?,
        DataType::Float32 => array
            .as_primitive::<Float32Type>()
            .value(row)
            .into_py_any(py)?,
        DataType::Float64 => array
            .as_primitive::<Float64Type>()
            .value(row)
            .into_py_any(py)?,
        DataType::Decimal128(_, scale) => decimal128_cell_to_py(py, array, row, *scale)?,
        DataType::Utf8 => array.as_string::<i32>().value(row).into_py_any(py)?,
        DataType::LargeUtf8 => array.as_string::<i64>().value(row).into_py_any(py)?,
        DataType::Utf8View => array.as_string_view().value(row).into_py_any(py)?,
        DataType::Binary => array.as_binary::<i32>().value(row).into_py_any(py)?,
        DataType::LargeBinary => array.as_binary::<i64>().value(row).into_py_any(py)?,
        DataType::BinaryView => array.as_binary_view().value(row).into_py_any(py)?,
        DataType::FixedSizeBinary(_) => array.as_fixed_size_binary().value(row).into_py_any(py)?,
        DataType::Date32 => {
            let days = array.as_primitive::<Date32Type>().value(row);
            date_from_days(py, i64::from(days))?
        }
        DataType::Date64 => {
            let millis = array.as_primitive::<Date64Type>().value(row);
            date_from_days(py, millis.div_euclid(86_400_000))?
        }
        DataType::Timestamp(unit, _) => timestamp_cell_to_py(py, array, row, *unit)?,
        // An Arrow list maps to a dense VECTOR: its primitive child becomes a
        // Python array.array (reference converters.pyx:138-139 get_vector), which
        // the bind path then encodes as DB_TYPE_VECTOR.
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            arrow_list_cell_to_dense_vector(py, array, row)?
        }
        // An Arrow struct maps to a sparse VECTOR carrying num_dimensions /
        // indices / values (reference converters.pyx:140-147 get_sparse_vector ->
        // SparseVector), which the bind path encodes as DB_TYPE_VECTOR.
        DataType::Struct(_) => arrow_struct_cell_to_sparse_vector(py, array, row)?,
        other => {
            return Err(dpy_database_error(
                "DPY-3032",
                &format!(
                    "conversion from Apache Arrow format \"{}\" to Oracle Database \
                     is not supported",
                    arrow_type_name(other)
                ),
            ));
        }
    };
    Ok(value)
}

/// Builds a Python `array.array(typecode, values)` from native-endian bytes.
fn build_py_array(py: Python<'_>, typecode: &str, bytes: &[u8]) -> PyResult<Py<PyAny>> {
    let array_mod = py.import("array")?;
    let arr = array_mod
        .getattr("array")?
        .call1((typecode, pyo3::types::PyBytes::new(py, &[])))?;
    arr.call_method1("frombytes", (pyo3::types::PyBytes::new(py, bytes),))?;
    Ok(arr.unbind())
}

/// Converts an Arrow numeric child array slice (`offset..offset+len`) into a
/// dense-vector Python `array.array`. Supported element types match the Oracle
/// VECTOR storage formats: int8 -> "b", float32 -> "f", float64 -> "d"
/// (reference get_vector / VectorEncoder formats).
fn arrow_numeric_child_to_py_array(
    py: Python<'_>,
    child: &ArrayRef,
    offset: usize,
    len: usize,
) -> PyResult<Py<PyAny>> {
    match child.data_type() {
        DataType::Int8 => {
            let values = child.as_primitive::<Int8Type>().values();
            let bytes: Vec<u8> = values[offset..offset + len]
                .iter()
                .map(|value| *value as u8)
                .collect();
            build_py_array(py, "b", &bytes)
        }
        DataType::Float32 => {
            let values = child.as_primitive::<Float32Type>().values();
            let mut bytes = Vec::with_capacity(len * 4);
            for value in &values[offset..offset + len] {
                bytes.extend_from_slice(&value.to_ne_bytes());
            }
            build_py_array(py, "f", &bytes)
        }
        DataType::Float64 => {
            let values = child.as_primitive::<Float64Type>().values();
            let mut bytes = Vec::with_capacity(len * 8);
            for value in &values[offset..offset + len] {
                bytes.extend_from_slice(&value.to_ne_bytes());
            }
            build_py_array(py, "d", &bytes)
        }
        other => Err(dpy_database_error(
            "DPY-3033",
            &format!(
                "conversion from Apache Arrow list with child format \"{}\" \
                 to Oracle Database vector is not supported",
                arrow_type_name(other)
            ),
        )),
    }
}

/// Converts an Arrow list cell into a dense-vector `array.array`. Resolves the
/// (offset, length) of the row's child slice for List / LargeList /
/// FixedSizeList, then materializes the numeric child as an `array.array`.
fn arrow_list_cell_to_dense_vector(
    py: Python<'_>,
    array: &ArrayRef,
    row: usize,
) -> PyResult<Py<PyAny>> {
    let (child, offset, len) = match array.data_type() {
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let offsets = list.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            (list.values().clone(), start, end - start)
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            let offsets = list.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            (list.values().clone(), start, end - start)
        }
        DataType::FixedSizeList(_, size) => {
            let list = array.as_fixed_size_list();
            let size = *size as usize;
            (list.values().clone(), row * size, size)
        }
        other => {
            return Err(dpy_database_error(
                "DPY-3033",
                &format!(
                    "conversion from Apache Arrow list with child format \"{}\" \
                     to Oracle Database vector is not supported",
                    arrow_type_name(other)
                ),
            ));
        }
    };
    arrow_numeric_child_to_py_array(py, &child, offset, len)
}

/// Converts an Arrow struct cell (fields `num_dimensions`, `indices`, `values`)
/// into an `oracledb.SparseVector` (reference converters.pyx:140-147). `indices`
/// is materialized as a Python list of ints and `values` as a dense
/// `array.array` whose typecode matches the struct's value field.
fn arrow_struct_cell_to_sparse_vector(
    py: Python<'_>,
    array: &ArrayRef,
    row: usize,
) -> PyResult<Py<PyAny>> {
    let st = array.as_struct();
    let field = |name: &str| -> PyResult<ArrayRef> {
        st.column_by_name(name).cloned().ok_or_else(|| {
            dpy_database_error(
                "DPY-3032",
                &format!(
                    "conversion from Apache Arrow struct without a \"{name}\" field \
                     to Oracle Database is not supported"
                ),
            )
        })
    };
    let num_dimensions_arr = field("num_dimensions")?;
    let num_dimensions: u32 = match num_dimensions_arr.data_type() {
        DataType::Int64 => num_dimensions_arr.as_primitive::<Int64Type>().value(row) as u32,
        DataType::Int32 => num_dimensions_arr.as_primitive::<Int32Type>().value(row) as u32,
        DataType::UInt32 => num_dimensions_arr.as_primitive::<UInt32Type>().value(row),
        DataType::UInt64 => num_dimensions_arr.as_primitive::<UInt64Type>().value(row) as u32,
        other => {
            return Err(dpy_database_error(
                "DPY-3032",
                &format!(
                    "conversion from Apache Arrow struct num_dimensions format \
                     \"{}\" to Oracle Database is not supported",
                    arrow_type_name(other)
                ),
            ));
        }
    };

    let indices_arr = field("indices")?;
    let (indices_child, idx_offset, idx_len) = list_child_slice(&indices_arr, row)?;
    let indices = arrow_integer_child_to_py_list(py, &indices_child, idx_offset, idx_len)?;

    let values_arr = field("values")?;
    let (values_child, val_offset, val_len) = list_child_slice(&values_arr, row)?;
    let values = arrow_numeric_child_to_py_array(py, &values_child, val_offset, val_len)?;

    let sparse_cls = py.import("oracledb")?.getattr("SparseVector")?;
    Ok(sparse_cls
        .call1((num_dimensions, indices, values))?
        .unbind())
}

/// Resolves the (child, offset, length) of a row's slice for a List / LargeList
/// / FixedSizeList array. Shared by the sparse-vector indices and values fields.
fn list_child_slice(array: &ArrayRef, row: usize) -> PyResult<(ArrayRef, usize, usize)> {
    match array.data_type() {
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let offsets = list.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            Ok((list.values().clone(), start, end - start))
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            let offsets = list.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            Ok((list.values().clone(), start, end - start))
        }
        DataType::FixedSizeList(_, size) => {
            let list = array.as_fixed_size_list();
            let size = *size as usize;
            Ok((list.values().clone(), row * size, size))
        }
        other => Err(dpy_database_error(
            "DPY-3033",
            &format!(
                "conversion from Apache Arrow list with child format \"{}\" \
                 to Oracle Database vector is not supported",
                arrow_type_name(other)
            ),
        )),
    }
}

/// Materializes an Arrow integer child slice as a Python list of ints (sparse
/// vector indices).
fn arrow_integer_child_to_py_list(
    py: Python<'_>,
    child: &ArrayRef,
    offset: usize,
    len: usize,
) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    for index in offset..offset + len {
        let value: i64 = match child.data_type() {
            DataType::Int8 => i64::from(child.as_primitive::<Int8Type>().value(index)),
            DataType::Int16 => i64::from(child.as_primitive::<Int16Type>().value(index)),
            DataType::Int32 => i64::from(child.as_primitive::<Int32Type>().value(index)),
            DataType::Int64 => child.as_primitive::<Int64Type>().value(index),
            DataType::UInt8 => i64::from(child.as_primitive::<UInt8Type>().value(index)),
            DataType::UInt16 => i64::from(child.as_primitive::<UInt16Type>().value(index)),
            DataType::UInt32 => i64::from(child.as_primitive::<UInt32Type>().value(index)),
            DataType::UInt64 => child.as_primitive::<UInt64Type>().value(index) as i64,
            other => {
                return Err(dpy_database_error(
                    "DPY-3032",
                    &format!(
                        "conversion from Apache Arrow struct indices format \"{}\" \
                         to Oracle Database is not supported",
                        arrow_type_name(other)
                    ),
                ));
            }
        };
        list.append(value)?;
    }
    Ok(list.into_any().unbind())
}

/// Converts a decimal128 cell to a `decimal.Decimal` (the type the NUMBER bind
/// path expects).
fn decimal128_cell_to_py(
    py: Python<'_>,
    array: &ArrayRef,
    row: usize,
    scale: i8,
) -> PyResult<Py<PyAny>> {
    let unscaled = array.as_primitive::<Decimal128Type>().value(row);
    let text = oracledb::arrow::decimal128_to_string(unscaled, scale);
    let decimal_cls = py.import("decimal")?.getattr("Decimal")?;
    Ok(decimal_cls.call1((text,))?.unbind())
}

/// Builds a `datetime.date` from a day count since the Unix epoch.
fn date_from_days(py: Python<'_>, days: i64) -> PyResult<Py<PyAny>> {
    let date_cls = py.import("datetime")?.getattr("date")?;
    let epoch = date_cls.call1((1970, 1, 1))?;
    let timedelta = py.import("datetime")?.getattr("timedelta")?;
    let delta = timedelta.call1((days,))?;
    Ok(epoch.call_method1("__add__", (delta,))?.unbind())
}

/// Converts a timestamp cell to a naive `datetime.datetime`.
fn timestamp_cell_to_py(
    py: Python<'_>,
    array: &ArrayRef,
    row: usize,
    unit: TimeUnit,
) -> PyResult<Py<PyAny>> {
    let micros = match unit {
        TimeUnit::Second => array.as_primitive::<TimestampSecondType>().value(row) * 1_000_000,
        TimeUnit::Millisecond => {
            array.as_primitive::<TimestampMillisecondType>().value(row) * 1_000
        }
        TimeUnit::Microsecond => array.as_primitive::<TimestampMicrosecondType>().value(row),
        TimeUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().value(row) / 1_000,
    };
    let datetime_mod = py.import("datetime")?;
    let epoch = datetime_mod
        .getattr("datetime")?
        .call1((1970, 1, 1, 0, 0, 0))?;
    let delta = datetime_mod.getattr("timedelta")?;
    let kwargs = pyo3::types::PyDict::new(py);
    kwargs.set_item("microseconds", micros)?;
    let delta = delta.call((), Some(&kwargs))?;
    Ok(epoch.call_method1("__add__", (delta,))?.unbind())
}

/// Helper so other modules can build a `DataFrame` python object from a batch.
pub(crate) fn dataframe_from_batch<'py>(
    py: Python<'py>,
    batch: RecordBatch,
) -> PyResult<Bound<'py, PyAny>> {
    let impl_obj = Py::new(py, DataFrameImpl::new(batch))?;
    let dataframe_mod = py.import("oracledb.dataframe")?;
    let dataframe_cls = dataframe_mod.getattr("DataFrame")?;
    dataframe_cls.call_method1("_from_impl", (impl_obj,))
}

/// Slices a [`RecordBatch`] into `batch_size`-row `DataFrame` objects (at least
/// one, even when empty).
pub(crate) fn slice_batch_into_frames(
    py: Python<'_>,
    batch: RecordBatch,
    batch_size: i64,
) -> PyResult<Vec<Py<PyAny>>> {
    let size = usize::try_from(batch_size.max(1))
        .unwrap_or(usize::MAX)
        .max(1);
    let total = batch.num_rows();
    let mut frames = Vec::new();
    if total == 0 {
        frames.push(dataframe_from_batch(py, batch)?.unbind());
    } else {
        let mut offset = 0usize;
        while offset < total {
            let len = size.min(total - offset);
            frames.push(dataframe_from_batch(py, batch.slice(offset, len))?.unbind());
            offset += len;
        }
    }
    Ok(frames)
}

/// An awaitable that resolves immediately to a stored value. Implements the
/// coroutine/iterator protocol (`__await__` -> self, `__next__` raises
/// `StopIteration(value)`) so it works under any async runtime without yielding
/// to the event loop. Used to wrap already-computed DataFrame batches for the
/// async `fetch_df_batches` iterator.
#[pyclass(module = "oracledb.thin_impl", name = "ImmediateAwaitable")]
pub(crate) struct ImmediateAwaitable {
    value: Option<Py<PyAny>>,
}

#[pymethods]
impl ImmediateAwaitable {
    fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Completes the await immediately by raising `StopIteration(value)`, which
    /// the interpreter interprets as the awaited result.
    fn __next__(&mut self, py: Python<'_>) -> PyResult<()> {
        match self.value.take() {
            Some(value) => Err(pyo3::exceptions::PyStopIteration::new_err(
                value.bind(py).clone().unbind(),
            )),
            None => Err(pyo3::exceptions::PyStopIteration::new_err(py.None())),
        }
    }
}

/// Async iterator over pre-built `DataFrame` batches, so the async
/// `fetch_df_batches` impl can satisfy `async for df in ...`. The whole result
/// set is materialized up front (a single drain) and sliced into batches; this
/// iterator just hands them out one at a time.
#[pyclass(module = "oracledb.thin_impl", name = "AsyncDataFrameBatchIter")]
pub(crate) struct AsyncDataFrameBatchIter {
    frames: Vec<Py<PyAny>>,
    index: usize,
}

impl AsyncDataFrameBatchIter {
    pub(crate) fn new(frames: Vec<Py<PyAny>>) -> Self {
        Self { frames, index: 0 }
    }
}

#[pymethods]
impl AsyncDataFrameBatchIter {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Returns an awaitable resolving to the next pre-built frame (the batches
    /// are already in memory so it resolves immediately), or `None` once
    /// exhausted, which the runtime turns into `StopAsyncIteration`. Python's
    /// `async for` requires `__anext__` to return an awaitable, so we hand back
    /// an [`ImmediateAwaitable`] rather than the value directly.
    fn __anext__(&mut self, py: Python<'_>) -> Option<ImmediateAwaitable> {
        let frame = self.frames.get(self.index)?.clone_ref(py);
        self.index += 1;
        Some(ImmediateAwaitable { value: Some(frame) })
    }
}

/// Builds the `(schema_capsule, array_capsule)` tuple the public
/// `ArrowArray.__arrow_c_array__` returns. Currently unused by Python (the
/// public layer calls the impl getters directly) but kept for symmetry/tests.
#[allow(dead_code)]
pub(crate) fn array_capsules<'py>(
    py: Python<'py>,
    field: &Field,
    array: &ArrayRef,
) -> PyResult<Bound<'py, PyTuple>> {
    let schema = schema_capsule(py, ffi_schema_for_field(field)?)?;
    let (ffi_array, _) = ffi_for_array(array)?;
    let array_cap = array_capsule(py, ffi_array)?;
    PyTuple::new(py, [schema.into_any(), array_cap.into_any()])
}

// ---------------------------------------------------------------------------
// Direct Path Load: Python data -> DirectPathColumnValue rows
// ---------------------------------------------------------------------------

use oracledb::protocol::dpl::DirectPathColumnValue;
use oracledb::protocol::thin::{
    ColumnMetadata, CS_FORM_NCHAR, ORA_TYPE_NUM_BINARY_DOUBLE, ORA_TYPE_NUM_BINARY_FLOAT,
    ORA_TYPE_NUM_BLOB, ORA_TYPE_NUM_NUMBER, ORA_TYPE_NUM_RAW,
};

/// Materializes `direct_path_load`'s `data` (a list of row sequences, or an
/// Arrow PyCapsule stream object such as pyarrow.Table / pandas.DataFrame) into
/// owned Python row tuples. Done before taking the connection lock so the lock
/// is held only for the wire exchange.
pub(crate) fn direct_path_py_rows(data: &Bound<'_, PyAny>) -> PyResult<Vec<Py<PyTuple>>> {
    let py = data.py();
    let owned;
    let row_iterable: Bound<'_, PyAny> = if has_arrow_c_stream(data) {
        owned = arrow_table_to_py_rows(py, data)?.rows.into_any();
        owned
    } else {
        data.clone()
    };
    let rows_iter = row_iterable.try_iter().map_err(|_| {
        dpy_database_error(
            "DPY-2004",
            "data must be a list of sequences or a DataFrame",
        )
    })?;
    let mut rows = Vec::new();
    for row in rows_iter {
        let row = row?;
        let cells: Vec<Bound<'_, PyAny>> = row.try_iter()?.collect::<PyResult<Vec<_>>>()?;
        rows.push(PyTuple::new(py, cells)?.unbind());
    }
    Ok(rows)
}

/// Verifies every materialized row has exactly `num_columns` cells, raising
/// DPY-4009 otherwise. Run before PREPARE so a width mismatch never leaves a
/// half-open direct-path cursor on the session.
pub(crate) fn verify_direct_path_widths(
    py: Python<'_>,
    py_rows: &[Py<PyTuple>],
    num_columns: usize,
) -> PyResult<()> {
    for py_row in py_rows {
        let len = py_row.bind(py).len();
        if len != num_columns {
            return Err(dpy_database_error(
                "DPY-4009",
                &format!(
                    "{num_columns} positional bind values are required but {len} were provided"
                ),
            ));
        }
    }
    Ok(())
}

/// Converts materialized Python rows into direct-path rows using the prepared
/// per-column Oracle metadata (so a Python `float` becomes BINARY_FLOAT,
/// BINARY_DOUBLE, or NUMBER as the target column requires).
pub(crate) fn direct_path_rows_from_py(
    py: Python<'_>,
    py_rows: &[Py<PyTuple>],
    column_metadata: &[ColumnMetadata],
    num_columns: usize,
) -> PyResult<Vec<Vec<DirectPathColumnValue>>> {
    let mut rows = Vec::with_capacity(py_rows.len());
    for py_row in py_rows {
        let row = py_row.bind(py);
        let mut converted = Vec::with_capacity(num_columns);
        for (index, cell) in row.iter().enumerate() {
            converted.push(py_value_to_direct_path(&cell, column_metadata.get(index))?);
        }
        rows.push(converted);
    }
    Ok(rows)
}

/// Converts a single Python value to a [`DirectPathColumnValue`]. The target
/// column metadata (when known) disambiguates numeric encodings and the text
/// charset (NCHAR-form columns are UTF-16BE on the wire).
fn py_value_to_direct_path(
    value: &Bound<'_, PyAny>,
    metadata: Option<&ColumnMetadata>,
) -> PyResult<DirectPathColumnValue> {
    let ora_type_num = metadata.map(|m| m.ora_type_num);
    if value.is_none() {
        return Ok(DirectPathColumnValue::Null);
    }
    if value.is_instance_of::<pyo3::types::PyBool>() && ora_type_num != Some(ORA_TYPE_NUM_NUMBER) {
        return Ok(DirectPathColumnValue::Boolean(value.is_truthy()?));
    }
    if let Ok(bytes) = value.cast::<pyo3::types::PyBytes>() {
        return Ok(DirectPathColumnValue::Bytes(bytes.as_bytes().to_vec()));
    }
    if let Ok(text) = value.extract::<String>() {
        // Oracle stores an empty VARCHAR/CHAR as NULL; a direct-path load of an
        // empty string into a NOT NULL column must therefore raise DPY-8001.
        if text.is_empty() {
            return Ok(DirectPathColumnValue::Null);
        }
        // NCHAR-form columns (incl. CLOBs streamed as LONG over a multi-byte DB
        // charset) carry UTF-16BE bytes on the direct-path wire; other character
        // columns are UTF-8.
        let is_nchar = metadata.is_some_and(|m| m.csfrm == CS_FORM_NCHAR);
        let encoded = if is_nchar {
            text.encode_utf16().flat_map(u16::to_be_bytes).collect()
        } else {
            text.into_bytes()
        };
        return Ok(DirectPathColumnValue::Bytes(encoded));
    }
    if let Some((year, month, day, hour, minute, second, nanosecond)) =
        crate::py_date_time_fields(value)?
    {
        return Ok(DirectPathColumnValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
        });
    }
    // Numeric value: pick the encoding from the target column type.
    match ora_type_num {
        Some(ORA_TYPE_NUM_BINARY_FLOAT) => {
            return Ok(DirectPathColumnValue::BinaryFloat(value.extract::<f32>()?));
        }
        Some(ORA_TYPE_NUM_BINARY_DOUBLE) => {
            return Ok(DirectPathColumnValue::BinaryDouble(value.extract::<f64>()?));
        }
        Some(ORA_TYPE_NUM_RAW) | Some(ORA_TYPE_NUM_BLOB) => {
            // RAW/BLOB from a non-bytes value is unsupported.
        }
        _ => {
            // int / Decimal / float bound to a NUMBER column all keep their
            // exact decimal text representation.
            if value.is_instance_of::<pyo3::types::PyInt>()
                || crate::py_value_type_name(value) == "Decimal"
                || value.extract::<f64>().is_ok()
            {
                return Ok(DirectPathColumnValue::Number(
                    value.str()?.extract::<String>()?,
                ));
            }
        }
    }
    Err(dpy_database_error(
        "DPY-3002",
        &format!(
            "Python value of type \"{}\" is not supported",
            crate::py_value_type_name(value)
        ),
    ))
}

/// Maps a driver direct-path-load error to a Python exception, preserving the
/// reference DPY-* / ORA-* codes the driver already embeds in its messages.
pub(crate) fn direct_path_error_to_py(err: oracledb::Error) -> PyErr {
    crate::ora_database_error(&err.to_string())
}
