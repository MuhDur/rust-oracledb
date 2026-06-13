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
    // the capsule for the rest of its lifetime.
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
        // Lists/structs are vector shapes; binding them needs VECTOR support that
        // the driver foundation does not yet provide. Surface the reference
        // DPY-3033 so the public layer errors cleanly instead of hanging.
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            return Err(dpy_database_error(
                "DPY-3033",
                &format!(
                    "conversion from Apache Arrow list with child format \"{}\" \
                     to Oracle Database vector is not supported",
                    arrow_type_name(array.data_type())
                ),
            ));
        }
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
