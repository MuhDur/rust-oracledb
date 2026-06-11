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

#[derive(Clone)]
pub(crate) struct ThinLobContext {
    pub(crate) connection: Arc<Mutex<Option<RustConnection>>>,
    pub(crate) state: Arc<Mutex<ThinConnState>>,
    pub(crate) async_mode: bool,
}

#[derive(Clone)]
#[pyclass(module = "oracledb.thin_impl", name = "ThinLob")]
pub(crate) struct ThinLob {
    pub(crate) data: Option<Arc<Mutex<Vec<u8>>>>,
    pub(crate) locator: Arc<Mutex<Option<Vec<u8>>>>,
    pub(crate) ora_type_num: u8,
    pub(crate) csfrm: u8,
    pub(crate) size: u64,
    pub(crate) chunk_size: u32,
    pub(crate) context: Option<ThinLobContext>,
    pub(crate) is_open: Arc<Mutex<bool>>,
    pub(crate) bfile_name: Option<(String, String)>,
}

pub(crate) fn lob_data_to_py(
    py: Python<'_>,
    ora_type_num: u8,
    csfrm: u8,
    locator: Option<&[u8]>,
    data: &[u8],
    offset: u64,
    amount: Option<u64>,
) -> PyResult<Py<PyAny>> {
    if matches!(ora_type_num, ORA_TYPE_NUM_BLOB | ORA_TYPE_NUM_BFILE) {
        let start = offset.saturating_sub(1) as usize;
        let bytes = data.get(start..).unwrap_or_default();
        let bytes = amount
            .and_then(|amount| usize::try_from(amount).ok())
            .map(|amount| bytes.get(..amount).unwrap_or(bytes))
            .unwrap_or(bytes);
        return Ok(PyBytes::new(py, bytes).unbind().into());
    }
    let text = protocol_decode_lob_text(data, csfrm, locator).map_err(runtime_error)?;
    let start = offset.saturating_sub(1) as usize;
    let chars = text.chars().skip(start);
    let value = match amount.and_then(|amount| usize::try_from(amount).ok()) {
        Some(amount) => chars.take(amount).collect::<String>(),
        None => chars.collect::<String>(),
    };
    Ok(value.into_pyobject(py)?.unbind().into())
}

pub(crate) fn py_lob_from_impl(py: Python<'_>, lob: ThinLob) -> PyResult<Py<PyAny>> {
    let module = PyModule::import(py, "oracledb")?;
    let cls = if lob
        .context
        .as_ref()
        .is_some_and(|context| context.async_mode)
    {
        module.getattr("AsyncLOB")?
    } else {
        module.getattr("LOB")?
    };
    let impl_obj: Py<PyAny> = if lob
        .context
        .as_ref()
        .is_some_and(|context| context.async_mode)
    {
        Py::new(py, AsyncThinLob { inner: lob })?.into()
    } else {
        Py::new(py, lob)?.into()
    };
    Ok(cls.call_method1("_from_impl", (impl_obj,))?.unbind())
}

#[pymethods]
impl ThinLob {
    #[pyo3(signature = (offset=1, amount=None))]
    pub(crate) fn read(&self, py: Python<'_>, offset: u64, amount: Option<u64>) -> PyResult<Py<PyAny>> {
        if self.ora_type_num == ORA_TYPE_NUM_BFILE {
            return Err(dpy_database_error(
                "ORA-22285",
                "non-existent directory or file for FILEOPEN operation",
            ));
        }
        if let Some(data) = self.data.as_ref() {
            let data = data.lock().map_err(runtime_error)?;
            return lob_data_to_py(
                py,
                self.ora_type_num,
                self.csfrm,
                self.locator.lock().map_err(runtime_error)?.as_deref(),
                &data,
                offset,
                amount,
            );
        }
        let Some(context) = self.context.as_ref() else {
            return lob_data_to_py(py, self.ora_type_num, self.csfrm, None, &[], offset, amount);
        };
        let locator = self
            .locator
            .lock()
            .map_err(runtime_error)?
            .clone()
            .unwrap_or_default();
        let call_timeout = {
            let value = context.state.lock().map_err(runtime_error)?.call_timeout;
            (value > 0).then_some(value)
        };
        let mut guard = context.connection.lock().map_err(runtime_error)?;
        let connection = guard.as_mut().ok_or_else(connection_closed_error)?;
        let result = BlockingConnection::read_lob_with_timeout(
            connection,
            &locator,
            offset,
            amount.unwrap_or(u64::from(u32::MAX)),
            call_timeout,
        )
        .map_err(runtime_error)?;
        *self.locator.lock().map_err(runtime_error)? = Some(result.locator.clone());
        lob_data_to_py(
            py,
            self.ora_type_num,
            self.csfrm,
            Some(&result.locator),
            result.data.as_deref().unwrap_or_default(),
            1,
            None,
        )
    }

    fn write(&mut self, value: &Bound<'_, PyAny>, offset: u64) -> PyResult<()> {
        let is_binary = matches!(self.ora_type_num, ORA_TYPE_NUM_BLOB | ORA_TYPE_NUM_BFILE);
        let raw_bytes = if is_binary {
            Some(value.cast::<PyBytes>()?.as_bytes().to_vec())
        } else {
            None
        };
        let text = if is_binary {
            None
        } else {
            Some(value.extract::<String>()?)
        };
        if let Some(context) = self.context.as_ref() {
            let locator = self
                .locator
                .lock()
                .map_err(runtime_error)?
                .clone()
                .unwrap_or_default();
            let bytes = raw_bytes.as_ref().cloned().unwrap_or_else(|| {
                protocol_encode_lob_text(
                    text.as_deref().unwrap_or_default(),
                    self.csfrm,
                    Some(&locator),
                )
            });
            let call_timeout = {
                let value = context.state.lock().map_err(runtime_error)?.call_timeout;
                (value > 0).then_some(value)
            };
            let mut guard = context.connection.lock().map_err(runtime_error)?;
            let connection = guard.as_mut().ok_or_else(connection_closed_error)?;
            let result = BlockingConnection::write_lob_with_timeout(
                connection,
                &locator,
                offset,
                &bytes,
                call_timeout,
            )
            .map_err(runtime_error)?;
            *self.locator.lock().map_err(runtime_error)? = Some(result.locator);
            self.size = if is_binary {
                self.size.max(offset.saturating_sub(1) + bytes.len() as u64)
            } else {
                self.size.max(
                    offset.saturating_sub(1)
                        + text.as_deref().unwrap_or_default().chars().count() as u64,
                )
            };
            return Ok(());
        }
        let Some(data) = self.data.as_ref() else {
            return Err(not_implemented("ThinLob.write persistent LOB"));
        };
        let locator = self.locator.lock().map_err(runtime_error)?.clone();
        let bytes = raw_bytes.as_ref().cloned().unwrap_or_else(|| {
            protocol_encode_lob_text(
                text.as_deref().unwrap_or_default(),
                self.csfrm,
                locator.as_deref(),
            )
        });
        let start = usize::try_from(offset.saturating_sub(1)).map_err(runtime_error)?;
        let mut data = data.lock().map_err(runtime_error)?;
        if start > data.len() {
            data.resize(start, 0);
        }
        let end = start.saturating_add(bytes.len());
        if end > data.len() {
            data.resize(end, 0);
        }
        data[start..end].copy_from_slice(&bytes);
        self.size = if matches!(self.ora_type_num, ORA_TYPE_NUM_BLOB | ORA_TYPE_NUM_BFILE) {
            data.len() as u64
        } else {
            protocol_decode_lob_text(&data, self.csfrm, None)
                .map_err(runtime_error)?
                .chars()
                .count() as u64
        };
        Ok(())
    }

    fn get_max_amount(&self) -> u64 {
        u64::from(u32::MAX)
    }

    fn get_size(&self) -> u64 {
        self.size
    }

    fn size(&self) -> u64 {
        self.get_size()
    }

    fn get_chunk_size(&self) -> u32 {
        self.chunk_size
    }

    #[getter]
    fn dbtype(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let module = PyModule::import(py, "oracledb")?;
        let name = match self.ora_type_num {
            ORA_TYPE_NUM_BLOB => "DB_TYPE_BLOB",
            ORA_TYPE_NUM_BFILE => "DB_TYPE_BFILE",
            ORA_TYPE_NUM_CLOB if self.csfrm == CS_FORM_NCHAR => "DB_TYPE_NCLOB",
            ORA_TYPE_NUM_CLOB => "DB_TYPE_CLOB",
            _ => "DB_TYPE_CLOB",
        };
        Ok(module.getattr(name)?.unbind())
    }

    fn free_lob(&self) -> PyResult<()> {
        let Some(context) = self.context.as_ref() else {
            return Ok(());
        };
        let locator = self.locator.lock().map_err(runtime_error)?.clone();
        let Some(locator) = locator.filter(|locator| lob_locator_is_temporary(locator)) else {
            return Ok(());
        };
        let call_timeout = {
            let value = context.state.lock().map_err(runtime_error)?.call_timeout;
            (value > 0).then_some(value)
        };
        let mut guard = context.connection.lock().map_err(runtime_error)?;
        let Some(connection) = guard.as_mut() else {
            *self.locator.lock().map_err(runtime_error)? = None;
            return Ok(());
        };
        BlockingConnection::free_temp_lobs_with_timeout(connection, &[locator], call_timeout)
            .map_err(runtime_error)?;
        *self.locator.lock().map_err(runtime_error)? = None;
        Ok(())
    }

    fn get_file_name(&self) -> PyResult<(String, String)> {
        Ok(self.bfile_name.clone().unwrap_or_default())
    }

    fn set_file_name(&mut self, dir_alias: String, name: String) {
        self.bfile_name = Some((dir_alias, name));
    }

    fn file_exists(&self) -> PyResult<bool> {
        Err(dpy_database_error(
            "ORA-22285",
            "non-existent directory or file for FILEOPEN operation",
        ))
    }

    fn close(&self) -> PyResult<()> {
        let mut is_open = self.is_open.lock().map_err(runtime_error)?;
        if !*is_open {
            return Err(runtime_error(
                "server returned Oracle error: ORA-22289: LOB is not open",
            ));
        }
        *is_open = false;
        Ok(())
    }

    fn open(&self) -> PyResult<()> {
        let mut is_open = self.is_open.lock().map_err(runtime_error)?;
        if *is_open {
            return Err(runtime_error(
                "server returned Oracle error: ORA-22293: LOB already open",
            ));
        }
        *is_open = true;
        Ok(())
    }

    fn get_is_open(&self) -> PyResult<bool> {
        Ok(*self.is_open.lock().map_err(runtime_error)?)
    }

    fn trim(&mut self, new_size: u64) -> PyResult<()> {
        if let Some(data) = self.data.as_ref() {
            let mut data = data.lock().map_err(runtime_error)?;
            if matches!(self.ora_type_num, ORA_TYPE_NUM_BLOB | ORA_TYPE_NUM_BFILE) {
                data.truncate(usize::try_from(new_size).unwrap_or(usize::MAX));
            } else {
                let text = protocol_decode_lob_text(
                    &data,
                    self.csfrm,
                    self.locator.lock().map_err(runtime_error)?.as_deref(),
                )
                .map_err(runtime_error)?;
                let text = text
                    .chars()
                    .take(usize::try_from(new_size).unwrap_or(usize::MAX))
                    .collect::<String>();
                let locator = self.locator.lock().map_err(runtime_error)?.clone();
                *data = protocol_encode_lob_text(&text, self.csfrm, locator.as_deref());
            }
            self.size = new_size;
            return Ok(());
        }
        let Some(context) = self.context.as_ref() else {
            self.size = new_size;
            return Ok(());
        };
        let locator = self
            .locator
            .lock()
            .map_err(runtime_error)?
            .clone()
            .unwrap_or_default();
        let call_timeout = {
            let value = context.state.lock().map_err(runtime_error)?.call_timeout;
            (value > 0).then_some(value)
        };
        let mut guard = context.connection.lock().map_err(runtime_error)?;
        let connection = guard.as_mut().ok_or_else(connection_closed_error)?;
        let result =
            BlockingConnection::trim_lob_with_timeout(connection, &locator, new_size, call_timeout)
                .map_err(runtime_error)?;
        *self.locator.lock().map_err(runtime_error)? = Some(result.locator);
        self.size = new_size;
        Ok(())
    }
}

impl ThinLob {
    async fn read_async(&self, offset: u64, amount: Option<u64>) -> PyResult<Py<PyAny>> {
        if self.ora_type_num == ORA_TYPE_NUM_BFILE || self.data.is_some() || self.context.is_none()
        {
            return Python::attach(|py| self.read(py, offset, amount));
        }
        let context = self.context.clone().ok_or_else(|| {
            PyRuntimeError::new_err("LOB has neither local data nor connection context")
        })?;
        let locator = self
            .locator
            .lock()
            .map_err(runtime_error)?
            .clone()
            .unwrap_or_default();
        let call_timeout = {
            let value = context.state.lock().map_err(runtime_error)?.call_timeout;
            (value > 0).then_some(value)
        };
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-lob-read",
            Arc::clone(&context.connection),
            move |cx, connection| {
                Box::pin(async move {
                    connection
                        .read_lob_with_timeout(
                            cx,
                            &locator,
                            offset,
                            amount.unwrap_or(u64::from(u32::MAX)),
                            call_timeout,
                        )
                        .await
                        .map_err(TaskError::from)
                })
            },
        );
        let result = task.await.map_err(runtime_error)?;
        *self.locator.lock().map_err(runtime_error)? = Some(result.locator.clone());
        Python::attach(|py| {
            lob_data_to_py(
                py,
                self.ora_type_num,
                self.csfrm,
                Some(&result.locator),
                result.data.as_deref().unwrap_or_default(),
                1,
                None,
            )
        })
    }

    async fn write_async(&mut self, value: Py<PyAny>, offset: u64) -> PyResult<()> {
        if self.context.is_none() {
            return Python::attach(|py| self.write(value.bind(py), offset));
        }
        let is_binary = matches!(self.ora_type_num, ORA_TYPE_NUM_BLOB | ORA_TYPE_NUM_BFILE);
        let (raw_bytes, text): (Option<Vec<u8>>, Option<String>) = Python::attach(|py| {
            if is_binary {
                Ok::<(Option<Vec<u8>>, Option<String>), PyErr>((
                    Some(value.bind(py).cast::<PyBytes>()?.as_bytes().to_vec()),
                    None,
                ))
            } else {
                Ok::<(Option<Vec<u8>>, Option<String>), PyErr>((
                    None,
                    Some(value.bind(py).extract::<String>()?),
                ))
            }
        })?;
        let context = self.context.clone().ok_or_else(|| {
            PyRuntimeError::new_err("LOB has neither local data nor connection context")
        })?;
        let locator = self
            .locator
            .lock()
            .map_err(runtime_error)?
            .clone()
            .unwrap_or_default();
        let bytes = raw_bytes.as_ref().cloned().unwrap_or_else(|| {
            protocol_encode_lob_text(
                text.as_deref().unwrap_or_default(),
                self.csfrm,
                Some(&locator),
            )
        });
        let call_timeout = {
            let value = context.state.lock().map_err(runtime_error)?.call_timeout;
            (value > 0).then_some(value)
        };
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-lob-write",
            Arc::clone(&context.connection),
            move |cx, connection| {
                Box::pin(async move {
                    connection
                        .write_lob_with_timeout(cx, &locator, offset, &bytes, call_timeout)
                        .await
                        .map_err(TaskError::from)
                })
            },
        );
        let result = task.await.map_err(runtime_error)?;
        *self.locator.lock().map_err(runtime_error)? = Some(result.locator);
        self.size = if is_binary {
            self.size.max(
                offset.saturating_sub(1)
                    + raw_bytes.as_ref().map(Vec::len).unwrap_or_default() as u64,
            )
        } else {
            self.size.max(
                offset.saturating_sub(1)
                    + text.as_deref().unwrap_or_default().chars().count() as u64,
            )
        };
        Ok(())
    }

    async fn trim_async(&mut self, new_size: u64) -> PyResult<()> {
        if self.data.is_some() || self.context.is_none() {
            return self.trim(new_size);
        }
        let context = self.context.clone().ok_or_else(|| {
            PyRuntimeError::new_err("LOB has neither local data nor connection context")
        })?;
        let locator = self
            .locator
            .lock()
            .map_err(runtime_error)?
            .clone()
            .unwrap_or_default();
        let call_timeout = {
            let value = context.state.lock().map_err(runtime_error)?.call_timeout;
            (value > 0).then_some(value)
        };
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-lob-trim",
            Arc::clone(&context.connection),
            move |cx, connection| {
                Box::pin(async move {
                    connection
                        .trim_lob_with_timeout(cx, &locator, new_size, call_timeout)
                        .await
                        .map_err(TaskError::from)
                })
            },
        );
        let result = task.await.map_err(runtime_error)?;
        *self.locator.lock().map_err(runtime_error)? = Some(result.locator);
        self.size = new_size;
        Ok(())
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "AsyncThinLob")]
pub(crate) struct AsyncThinLob {
    pub(crate) inner: ThinLob,
}

#[pymethods]
impl AsyncThinLob {
    #[pyo3(signature = (offset=1, amount=None))]
    async fn read(&self, offset: u64, amount: Option<u64>) -> PyResult<Py<PyAny>> {
        self.inner.read_async(offset, amount).await
    }

    async fn write(&mut self, value: Py<PyAny>, offset: u64) -> PyResult<()> {
        self.inner.write_async(value, offset).await
    }

    fn get_max_amount(&self) -> u64 {
        self.inner.get_max_amount()
    }

    async fn get_size(&self) -> u64 {
        self.inner.get_size()
    }

    async fn size(&self) -> u64 {
        self.inner.get_size()
    }

    async fn get_chunk_size(&self) -> u32 {
        self.inner.get_chunk_size()
    }

    #[getter]
    fn dbtype(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.inner.dbtype(py)
    }

    async fn file_exists(&self) -> PyResult<bool> {
        self.inner.file_exists()
    }

    fn get_file_name(&self) -> PyResult<(String, String)> {
        self.inner.get_file_name()
    }

    fn set_file_name(&mut self, dir_alias: String, name: String) {
        self.inner.set_file_name(dir_alias, name)
    }

    async fn close(&self) -> PyResult<()> {
        self.inner.close()
    }

    async fn open(&self) -> PyResult<()> {
        self.inner.open()
    }

    async fn get_is_open(&self) -> PyResult<bool> {
        self.inner.get_is_open()
    }

    async fn trim(&mut self, new_size: u64) -> PyResult<()> {
        self.inner.trim_async(new_size).await
    }

    fn free_lob(&self) -> PyResult<()> {
        self.inner.free_lob()
    }
}
