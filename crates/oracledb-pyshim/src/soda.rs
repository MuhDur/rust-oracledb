//! PyO3 impl classes for thin-mode SODA.
//!
//! python-oracledb's thin mode ships no SODA; this is the experimental surpass
//! surface. The public `oracledb.soda` Python classes (`SodaDatabase`,
//! `SodaCollection`, `SodaDocument`, `SodaDocCursor`, `SodaOperation`) are
//! imported unchanged from the reference package and delegate to the `*Impl`
//! classes defined here. The reference passes the *public* `SodaOperation`
//! object straight to the collection impl methods (`get_count(self)` etc.), so
//! those methods read its `_key` / `_keys` / `_filter` / ... attributes
//! directly rather than going through a dedicated op-impl class.
//!
//! All methods are synchronous (matching the reference SODA API). They lock the
//! shared connection, build a private Asupersync runtime, and drive the async
//! `oracledb::soda` domain layer to completion — the same shape as
//! `BlockingConnection`.

use std::sync::{Arc, Mutex};

use oracledb::soda::{SodaCollection, SodaDatabase, SodaDocument, SodaOperation};
use oracledb::Connection as RustConnection;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};

use crate::async_bridge::{build_pyshim_io_runtime, TaskError};
use crate::conn::ThinConnImpl;
use crate::convert::{oson_value_to_py, py_value_to_oson};
use crate::errors::{raise_task_error, runtime_error};

type ConnHandle = Arc<Mutex<Option<RustConnection>>>;

/// Lock the connection and drive an async SODA closure to completion on a
/// private runtime, releasing the GIL for the duration. Errors are mapped to
/// the reference Python exception types (so ORA-/DPY- codes surface).
fn run_soda<T, F>(py: Python<'_>, connection: &ConnHandle, op: F) -> PyResult<T>
where
    T: Send,
    F: for<'a> FnOnce(
            &'a asupersync::Cx,
            &'a mut RustConnection,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<T, TaskError>> + 'a>,
        > + Send,
{
    let result = py.detach(|| -> Result<T, TaskError> {
        let mut guard = connection.lock().map_err(|e| e.to_string())?;
        let conn = guard
            .as_mut()
            .ok_or_else(|| "connection is closed".to_string())?;
        let runtime = build_pyshim_io_runtime()?;
        runtime.block_on(async {
            let cx = asupersync::Cx::current()
                .ok_or_else(|| "asupersync did not install an ambient Cx".to_string())?;
            op(&cx, conn).await
        })
    });
    result.map_err(|e| raise_task_error(&e, connection))
}

// ---------------------------------------------------------------------------
// SodaDatabase
// ---------------------------------------------------------------------------

#[pyclass(module = "oracledb.thin_impl", name = "ThinSodaDbImpl")]
pub(crate) struct ThinSodaDbImpl {
    connection: ConnHandle,
    #[pyo3(get)]
    supports_json: bool,
}

impl ThinSodaDbImpl {
    pub(crate) fn new(connection: ConnHandle, supports_json: bool) -> Self {
        ThinSodaDbImpl {
            connection,
            supports_json,
        }
    }
}

#[pymethods]
impl ThinSodaDbImpl {
    /// Create (or open) a collection.
    fn create_collection(
        &self,
        py: Python<'_>,
        name: String,
        metadata: Option<String>,
        map_mode: bool,
    ) -> PyResult<ThinSodaCollImpl> {
        let conn = self.connection.clone();
        let coll = run_soda(py, &conn, move |cx, c| {
            let name = name.clone();
            let metadata = metadata.clone();
            Box::pin(async move {
                SodaDatabase::new()
                    .create_collection(c, cx, &name, metadata.as_deref(), map_mode)
                    .await
                    .map_err(soda_task_error)
            })
        })?;
        Ok(ThinSodaCollImpl {
            collection: coll,
            connection: self.connection.clone(),
        })
    }

    /// Open an existing collection; returns None if absent.
    fn open_collection(&self, py: Python<'_>, name: String) -> PyResult<Option<ThinSodaCollImpl>> {
        let conn = self.connection.clone();
        let coll = run_soda(py, &conn, move |cx, c| {
            let name = name.clone();
            Box::pin(async move {
                SodaDatabase::new()
                    .open_collection(c, cx, &name)
                    .await
                    .map_err(soda_task_error)
            })
        })?;
        Ok(coll.map(|collection| ThinSodaCollImpl {
            collection,
            connection: self.connection.clone(),
        }))
    }

    /// List collection names.
    fn get_collection_names(
        &self,
        py: Python<'_>,
        start_name: Option<String>,
        limit: u32,
    ) -> PyResult<Vec<String>> {
        let conn = self.connection.clone();
        run_soda(py, &conn, move |cx, c| {
            let start_name = start_name.clone();
            Box::pin(async move {
                SodaDatabase::new()
                    .get_collection_names(c, cx, start_name.as_deref(), limit)
                    .await
                    .map_err(soda_task_error)
            })
        })
    }

    /// Build a document from raw content bytes.
    fn create_document(
        &self,
        content: &[u8],
        key: Option<String>,
        media_type: Option<String>,
    ) -> PyResult<ThinSodaDocImpl> {
        Ok(ThinSodaDocImpl {
            inner: SodaDocument::from_bytes(content.to_vec(), key, media_type),
        })
    }

    /// Build a document from a Python value (dict/list/scalar) by encoding it to
    /// OSON, mirroring the native `create_json_document` path.
    fn create_json_document(
        &self,
        value: &Bound<'_, PyAny>,
        key: Option<String>,
    ) -> PyResult<ThinSodaDocImpl> {
        let oson = py_value_to_oson(value)?;
        Ok(ThinSodaDocImpl {
            inner: SodaDocument::from_oson(oson, key),
        })
    }
}

// ---------------------------------------------------------------------------
// SodaCollection
// ---------------------------------------------------------------------------

#[pyclass(module = "oracledb.thin_impl", name = "ThinSodaCollImpl")]
pub(crate) struct ThinSodaCollImpl {
    collection: SodaCollection,
    connection: ConnHandle,
}

#[pymethods]
impl ThinSodaCollImpl {
    #[getter]
    fn name(&self) -> String {
        self.collection.name().to_string()
    }

    /// The collection metadata as a JSON string (the public layer json.loads it).
    fn get_metadata(&self) -> PyResult<String> {
        let m = self.collection.metadata();
        // Re-emit a metadata document in the python-oracledb shape.
        let mut obj = serde_json::Map::new();
        obj.insert(
            "tableName".into(),
            serde_json::Value::String(m.table_name.clone()),
        );
        if let Some(s) = &m.schema_name {
            obj.insert("schemaName".into(), serde_json::Value::String(s.clone()));
        }
        let mut key = serde_json::Map::new();
        key.insert(
            "name".into(),
            serde_json::Value::String(m.key_column.clone()),
        );
        key.insert(
            "sqlType".into(),
            serde_json::Value::String(m.key_sql_type.clone()),
        );
        obj.insert("keyColumn".into(), serde_json::Value::Object(key));
        let mut content = serde_json::Map::new();
        content.insert(
            "name".into(),
            serde_json::Value::String(m.content_column.clone()),
        );
        obj.insert("contentColumn".into(), serde_json::Value::Object(content));
        obj.insert("readOnly".into(), serde_json::Value::Bool(m.read_only));
        serde_json::to_string(&serde_json::Value::Object(obj)).map_err(runtime_error)
    }

    fn insert_one(
        &self,
        py: Python<'_>,
        doc: &ThinSodaDocImpl,
        hint: Option<String>,
        return_doc: bool,
    ) -> PyResult<Option<ThinSodaDocImpl>> {
        let conn = self.connection.clone();
        let coll = self.collection.clone();
        let doc = doc.inner.clone();
        let out = run_soda(py, &conn, move |cx, c| {
            let coll = coll.clone();
            let doc = doc.clone();
            let hint = hint.clone();
            Box::pin(async move {
                coll.insert_one(c, cx, &doc, hint.as_deref(), return_doc)
                    .await
                    .map_err(soda_task_error)
            })
        })?;
        Ok(out.map(|inner| ThinSodaDocImpl { inner }))
    }

    fn insert_many(
        &self,
        py: Python<'_>,
        documents: Vec<Py<ThinSodaDocImpl>>,
        hint: Option<String>,
        return_docs: bool,
    ) -> PyResult<Option<Vec<ThinSodaDocImpl>>> {
        let docs: Vec<SodaDocument> = documents
            .iter()
            .map(|d| d.borrow(py).inner.clone())
            .collect();
        let conn = self.connection.clone();
        let coll = self.collection.clone();
        let out = run_soda(py, &conn, move |cx, c| {
            let coll = coll.clone();
            let docs = docs.clone();
            let hint = hint.clone();
            Box::pin(async move {
                coll.insert_many(c, cx, &docs, hint.as_deref(), return_docs)
                    .await
                    .map_err(soda_task_error)
            })
        })?;
        Ok(out.map(|v| {
            v.into_iter()
                .map(|inner| ThinSodaDocImpl { inner })
                .collect()
        }))
    }

    fn get_count(&self, py: Python<'_>, op: &Bound<'_, PyAny>) -> PyResult<u64> {
        let operation = op_from_py(op)?;
        let conn = self.connection.clone();
        let coll = self.collection.clone();
        run_soda(py, &conn, move |cx, c| {
            let coll = coll.clone();
            let operation = operation.clone();
            Box::pin(async move {
                coll.get_count(c, cx, &operation)
                    .await
                    .map_err(soda_task_error)
            })
        })
    }

    fn get_one(&self, py: Python<'_>, op: &Bound<'_, PyAny>) -> PyResult<Option<ThinSodaDocImpl>> {
        let operation = op_from_py(op)?;
        let conn = self.connection.clone();
        let coll = self.collection.clone();
        let out = run_soda(py, &conn, move |cx, c| {
            let coll = coll.clone();
            let operation = operation.clone();
            Box::pin(async move {
                coll.get_one(c, cx, &operation)
                    .await
                    .map_err(soda_task_error)
            })
        })?;
        Ok(out.map(|inner| ThinSodaDocImpl { inner }))
    }

    fn get_cursor(&self, py: Python<'_>, op: &Bound<'_, PyAny>) -> PyResult<ThinSodaDocCursorImpl> {
        // Materialise all matching documents up front; the cursor impl then
        // hands them out one at a time. This keeps the connection borrow short
        // and matches the reference's row-at-a-time iteration semantics for the
        // (small) collections the tests use.
        let operation = op_from_py(op)?;
        let conn = self.connection.clone();
        let coll = self.collection.clone();
        let docs = run_soda(py, &conn, move |cx, c| {
            let coll = coll.clone();
            let operation = operation.clone();
            Box::pin(async move {
                coll.get_documents(c, cx, &operation)
                    .await
                    .map_err(soda_task_error)
            })
        })?;
        Ok(ThinSodaDocCursorImpl {
            docs: docs
                .into_iter()
                .map(|inner| Some(ThinSodaDocImpl { inner }))
                .collect(),
            position: 0,
            open: true,
        })
    }

    fn remove(&self, py: Python<'_>, op: &Bound<'_, PyAny>) -> PyResult<u64> {
        let operation = op_from_py(op)?;
        let conn = self.connection.clone();
        let coll = self.collection.clone();
        run_soda(py, &conn, move |cx, c| {
            let coll = coll.clone();
            let operation = operation.clone();
            Box::pin(async move {
                coll.remove(c, cx, &operation)
                    .await
                    .map_err(soda_task_error)
            })
        })
    }

    fn replace_one(
        &self,
        py: Python<'_>,
        op: &Bound<'_, PyAny>,
        doc: &ThinSodaDocImpl,
        return_doc: bool,
    ) -> PyResult<Py<PyAny>> {
        let operation = op_from_py(op)?;
        let conn = self.connection.clone();
        let coll = self.collection.clone();
        let doc = doc.inner.clone();
        let (replaced, out) = run_soda(py, &conn, move |cx, c| {
            let coll = coll.clone();
            let operation = operation.clone();
            let doc = doc.clone();
            Box::pin(async move {
                coll.replace_one(c, cx, &operation, &doc, return_doc)
                    .await
                    .map_err(soda_task_error)
            })
        })?;
        if return_doc {
            // replaceOneAndGet -> the doc (or None)
            match out {
                Some(inner) => Ok(Py::new(py, ThinSodaDocImpl { inner })?.into_any()),
                None => Ok(py.None()),
            }
        } else {
            // replaceOne -> bool
            Ok(pyo3::types::PyBool::new(py, replaced)
                .to_owned()
                .into_any()
                .unbind())
        }
    }

    fn truncate(&self, py: Python<'_>) -> PyResult<()> {
        let conn = self.connection.clone();
        let coll = self.collection.clone();
        run_soda(py, &conn, move |cx, c| {
            let coll = coll.clone();
            Box::pin(async move { coll.truncate(c, cx).await.map_err(soda_task_error) })
        })
    }

    fn create_index(&self, py: Python<'_>, spec: String) -> PyResult<()> {
        let conn = self.connection.clone();
        let coll = self.collection.clone();
        run_soda(py, &conn, move |cx, c| {
            let coll = coll.clone();
            let spec = spec.clone();
            Box::pin(async move {
                coll.create_index(c, cx, &spec)
                    .await
                    .map_err(soda_task_error)
            })
        })
    }

    fn drop_index(&self, py: Python<'_>, name: String, force: bool) -> PyResult<bool> {
        let conn = self.connection.clone();
        let coll = self.collection.clone();
        run_soda(py, &conn, move |cx, c| {
            let coll = coll.clone();
            let name = name.clone();
            Box::pin(async move {
                coll.drop_index(c, cx, &name, force)
                    .await
                    .map_err(soda_task_error)
            })
        })
    }

    fn drop(&self, py: Python<'_>) -> PyResult<bool> {
        let conn = self.connection.clone();
        let name = self.collection.name().to_string();
        run_soda(py, &conn, move |cx, c| {
            let name = name.clone();
            Box::pin(async move {
                SodaDatabase::new()
                    .drop_collection(c, cx, &name)
                    .await
                    .map_err(soda_task_error)
            })
        })
    }

    /// getDataGuide is a documented thin gap (needs a JSON search index, which
    /// requires Oracle Text that is not present on the 23ai-free container).
    fn get_data_guide(&self) -> PyResult<Option<ThinSodaDocImpl>> {
        Err(runtime_error(
            "ORA-40582: getDataGuide() requires a JSON search index (Oracle Text); \
             not supported by thin-mode SODA on this database",
        ))
    }

    /// listIndexes is a documented thin gap.
    fn list_indexes(&self) -> PyResult<Vec<String>> {
        Err(runtime_error(
            "DPY-3001: listIndexes() is not supported by thin-mode SODA",
        ))
    }

    /// save / saveAndGet are documented thin gaps (UPSERT semantics).
    fn save(
        &self,
        _doc: &ThinSodaDocImpl,
        _hint: Option<String>,
        _return_doc: bool,
    ) -> PyResult<Option<ThinSodaDocImpl>> {
        Err(runtime_error(
            "DPY-3001: save()/saveAndGet() is not supported by thin-mode SODA",
        ))
    }
}

// ---------------------------------------------------------------------------
// SodaDocument
// ---------------------------------------------------------------------------

#[pyclass(module = "oracledb.thin_impl", name = "ThinSodaDocImpl")]
pub(crate) struct ThinSodaDocImpl {
    inner: SodaDocument,
}

#[pymethods]
impl ThinSodaDocImpl {
    fn get_key(&self) -> Option<String> {
        self.inner.key.clone()
    }

    fn get_version(&self) -> Option<String> {
        self.inner.version.clone()
    }

    fn get_media_type(&self) -> String {
        self.inner.media_type.clone()
    }

    fn get_created_on(&self) -> Option<String> {
        self.inner.created_on.clone()
    }

    fn get_last_modified(&self) -> Option<String> {
        self.inner.last_modified.clone()
    }

    /// Returns `(content, encoding)` per the reference SodaDocument.getContent:
    /// for native JSON content we return the decoded Python value and encoding
    /// None; for byte content we return the raw bytes and "UTF-8".
    fn get_content(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        if let Some(oson) = self.inner.content_as_oson() {
            let value = oson_value_to_py(py, oson)?;
            return Ok((value, py.None()));
        }
        if let Some(bytes) = self.inner.content_as_bytes() {
            let py_bytes = PyBytes::new(py, bytes).into_any().unbind();
            let enc = PyString::new(py, "UTF-8").into_any().unbind();
            return Ok((py_bytes, enc));
        }
        Ok((py.None(), py.None()))
    }
}

// ---------------------------------------------------------------------------
// SodaDocCursor
// ---------------------------------------------------------------------------

#[pyclass(module = "oracledb.thin_impl", name = "ThinSodaDocCursorImpl")]
pub(crate) struct ThinSodaDocCursorImpl {
    docs: Vec<Option<ThinSodaDocImpl>>,
    position: usize,
    open: bool,
}

#[pymethods]
impl ThinSodaDocCursorImpl {
    fn get_next_doc(&mut self) -> PyResult<Option<ThinSodaDocImpl>> {
        if !self.open {
            return Err(runtime_error("DPY-1006: cursor is not open"));
        }
        if self.position >= self.docs.len() {
            return Ok(None);
        }
        let doc = self.docs[self.position].take();
        self.position += 1;
        Ok(doc)
    }

    fn close(&mut self) -> PyResult<()> {
        if !self.open {
            return Err(runtime_error("DPY-1006: cursor is not open"));
        }
        self.open = false;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Operation extraction from the public Python SodaOperation
// ---------------------------------------------------------------------------

/// Build a [`SodaOperation`] from the public `oracledb.soda.SodaOperation`
/// Python object the reference passes to the collection impl methods.
fn op_from_py(op: &Bound<'_, PyAny>) -> PyResult<SodaOperation> {
    let get_str = |name: &str| -> PyResult<Option<String>> {
        match op.getattr(name) {
            Ok(v) if !v.is_none() => Ok(Some(v.extract::<String>()?)),
            _ => Ok(None),
        }
    };
    let get_keys = || -> PyResult<Option<Vec<String>>> {
        match op.getattr("_keys") {
            Ok(v) if !v.is_none() => Ok(Some(v.extract::<Vec<String>>()?)),
            _ => Ok(None),
        }
    };
    let get_u64 = |name: &str| -> PyResult<Option<u64>> {
        match op.getattr(name) {
            Ok(v) if !v.is_none() => Ok(Some(v.extract::<u64>()?)),
            _ => Ok(None),
        }
    };
    let get_u32 = |name: &str| -> PyResult<u32> {
        match op.getattr(name) {
            Ok(v) if !v.is_none() => Ok(v.extract::<u32>()?),
            _ => Ok(0),
        }
    };
    let get_bool = |name: &str| -> bool {
        op.getattr(name)
            .ok()
            .and_then(|v| v.extract::<bool>().ok())
            .unwrap_or(false)
    };

    Ok(SodaOperation {
        key: get_str("_key")?,
        keys: get_keys()?,
        filter: get_str("_filter")?,
        version: get_str("_version")?,
        skip: get_u64("_skip")?,
        limit: get_u64("_limit")?,
        fetch_array_size: get_u32("_fetch_array_size")?,
        hint: get_str("_hint")?,
        lock: get_bool("_lock"),
    })
}

/// Map a SODA domain error to a shim TaskError, preserving server error details
/// (ORA codes) so the Python exception carries the right code.
fn soda_task_error(err: oracledb::soda::SodaError) -> TaskError {
    match err {
        oracledb::soda::SodaError::Driver(e) => TaskError::from(e),
        other => TaskError::from(other.to_string()),
    }
}

// Expose a constructor for the conn impl to mint a db handle.
impl ThinConnImpl {
    pub(crate) fn build_soda_db_impl(&self) -> ThinSodaDbImpl {
        // 23ai stores documents as native JSON, so the shim supports the
        // create_json_document path.
        ThinSodaDbImpl::new(Arc::clone(&self.connection), true)
    }
}
