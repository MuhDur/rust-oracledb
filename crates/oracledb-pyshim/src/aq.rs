//! PyO3 impl classes for Advanced Queuing (AQ).
//!
//! The public `oracledb.aq` Python classes (`Queue`, `AsyncQueue`, `DeqOptions`,
//! `EnqOptions`, `MessageProperties`) are imported unchanged from the reference
//! package and delegate to the `*Impl` classes defined here, matching the
//! reference `impl/thin/queue.pyx` method surface:
//! - [`ThinQueueImpl`] / [`AsyncThinQueueImpl`]: `initialize`, `deq_one`,
//!   `deq_many`, `enq_one`, `enq_many`, `_supports_deq_many` + the
//!   `deq_options_impl` / `enq_options_impl` / `name` / `is_json` /
//!   `payload_type` attributes.
//! - [`ThinDeqOptionsImpl`] / [`ThinEnqOptionsImpl`] / [`ThinMsgPropsImpl`]:
//!   get/set accessors.

use std::sync::{Arc, Mutex};

use oracledb::protocol::thin::aq::{
    AqDeqMessage, AqDeqOptions, AqDeqPayload, AqEnqOptions, AqMsgProps, AqPayloadKind,
    AqPayloadValue, AqQueueDesc,
};
use oracledb::protocol::thin::QueryValue;
use oracledb::{BlockingConnection, Connection as RustConnection};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDateTime};

use crate::async_bridge::{spawn_async_connection_task, TaskError};
use crate::async_conn::AsyncThinConnImpl;
use crate::conn::ThinConnImpl;
use crate::convert::{oson_value_to_py, py_value_to_oson};
use crate::dbobject::{py_db_object_from_impl, DbObjectImpl, DbObjectTypeImpl};
use crate::errors::{raise_task_error, runtime_error};

type ConnHandle = Arc<Mutex<Option<RustConnection>>>;

// AQ delivery / option constants surfaced to the impl layer.
const TNS_AQ_MSG_PERSISTENT: u16 = 1;
const TNS_AQ_DEQ_REMOVE: u32 = 3;
const TNS_AQ_DEQ_NEXT_MSG: u32 = 3;
const TNS_AQ_DEQ_ON_COMMIT: u32 = 2;
const TNS_AQ_DEQ_WAIT_FOREVER: u32 = 0xFFFF_FFFF;
const TNS_AQ_ENQ_ON_COMMIT: u32 = 2;
const TNS_AQ_MSG_READY: i32 = 0;

// ---------------------------------------------------------------------------
// DeqOptions
// ---------------------------------------------------------------------------

#[pyclass(
    module = "oracledb.thin_impl",
    name = "ThinDeqOptionsImpl",
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct ThinDeqOptionsImpl {
    inner: Arc<Mutex<DeqOptionsState>>,
}

#[derive(Clone)]
struct DeqOptionsState {
    condition: Option<String>,
    consumer_name: Option<String>,
    correlation: Option<String>,
    delivery_mode: u16,
    mode: u32,
    msgid: Option<Vec<u8>>,
    navigation: u32,
    transformation: Option<String>,
    visibility: u32,
    wait: u32,
}

impl Default for DeqOptionsState {
    fn default() -> Self {
        Self {
            condition: None,
            consumer_name: None,
            correlation: None,
            delivery_mode: TNS_AQ_MSG_PERSISTENT,
            mode: TNS_AQ_DEQ_REMOVE,
            msgid: None,
            navigation: TNS_AQ_DEQ_NEXT_MSG,
            transformation: None,
            visibility: TNS_AQ_DEQ_ON_COMMIT,
            wait: TNS_AQ_DEQ_WAIT_FOREVER,
        }
    }
}

impl ThinDeqOptionsImpl {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(DeqOptionsState::default())),
        }
    }

    fn to_protocol(&self) -> PyResult<AqDeqOptions> {
        let state = self.inner.lock().map_err(runtime_error)?;
        Ok(AqDeqOptions {
            condition: state.condition.clone(),
            consumer_name: state.consumer_name.clone(),
            correlation: state.correlation.clone(),
            delivery_mode: state.delivery_mode,
            mode: state.mode as i32,
            msgid: state.msgid.clone(),
            navigation: state.navigation as i32,
            visibility: state.visibility as i32,
            wait: state.wait,
        })
    }
}

#[pymethods]
impl ThinDeqOptionsImpl {
    fn get_condition(&self) -> PyResult<Option<String>> {
        Ok(self.inner.lock().map_err(runtime_error)?.condition.clone())
    }

    fn get_consumer_name(&self) -> PyResult<Option<String>> {
        Ok(self
            .inner
            .lock()
            .map_err(runtime_error)?
            .consumer_name
            .clone())
    }

    fn get_correlation(&self) -> PyResult<Option<String>> {
        Ok(self
            .inner
            .lock()
            .map_err(runtime_error)?
            .correlation
            .clone())
    }

    fn get_message_id(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let state = self.inner.lock().map_err(runtime_error)?;
        Ok(match &state.msgid {
            Some(bytes) => PyBytes::new(py, bytes).into_any().unbind(),
            None => py.None(),
        })
    }

    fn get_mode(&self) -> PyResult<u32> {
        Ok(self.inner.lock().map_err(runtime_error)?.mode)
    }

    fn get_navigation(&self) -> PyResult<u32> {
        Ok(self.inner.lock().map_err(runtime_error)?.navigation)
    }

    fn get_transformation(&self) -> PyResult<Option<String>> {
        Ok(self
            .inner
            .lock()
            .map_err(runtime_error)?
            .transformation
            .clone())
    }

    fn get_visibility(&self) -> PyResult<u32> {
        Ok(self.inner.lock().map_err(runtime_error)?.visibility)
    }

    fn get_wait(&self) -> PyResult<u32> {
        Ok(self.inner.lock().map_err(runtime_error)?.wait)
    }

    fn set_condition(&self, value: Option<String>) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.condition = value;
        Ok(())
    }

    fn set_consumer_name(&self, value: Option<String>) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.consumer_name = value;
        Ok(())
    }

    fn set_correlation(&self, value: Option<String>) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.correlation = value;
        Ok(())
    }

    fn set_delivery_mode(&self, value: u16) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.delivery_mode = value;
        Ok(())
    }

    fn set_mode(&self, value: u32) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.mode = value;
        Ok(())
    }

    fn set_message_id(&self, value: Option<Vec<u8>>) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.msgid = value;
        Ok(())
    }

    fn set_navigation(&self, value: u32) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.navigation = value;
        Ok(())
    }

    fn set_transformation(&self, value: Option<String>) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.transformation = value;
        Ok(())
    }

    fn set_visibility(&self, value: u32) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.visibility = value;
        Ok(())
    }

    fn set_wait(&self, value: u32) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.wait = value;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// EnqOptions
// ---------------------------------------------------------------------------

#[pyclass(
    module = "oracledb.thin_impl",
    name = "ThinEnqOptionsImpl",
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct ThinEnqOptionsImpl {
    inner: Arc<Mutex<EnqOptionsState>>,
}

#[derive(Clone)]
struct EnqOptionsState {
    transformation: Option<String>,
    visibility: u32,
    delivery_mode: u16,
}

impl Default for EnqOptionsState {
    fn default() -> Self {
        Self {
            transformation: None,
            visibility: TNS_AQ_ENQ_ON_COMMIT,
            delivery_mode: TNS_AQ_MSG_PERSISTENT,
        }
    }
}

impl ThinEnqOptionsImpl {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(EnqOptionsState::default())),
        }
    }

    fn to_protocol(&self) -> PyResult<AqEnqOptions> {
        let state = self.inner.lock().map_err(runtime_error)?;
        Ok(AqEnqOptions {
            visibility: state.visibility,
            delivery_mode: state.delivery_mode,
        })
    }
}

#[pymethods]
impl ThinEnqOptionsImpl {
    fn get_transformation(&self) -> PyResult<Option<String>> {
        Ok(self
            .inner
            .lock()
            .map_err(runtime_error)?
            .transformation
            .clone())
    }

    fn get_visibility(&self) -> PyResult<u32> {
        Ok(self.inner.lock().map_err(runtime_error)?.visibility)
    }

    fn set_delivery_mode(&self, value: u16) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.delivery_mode = value;
        Ok(())
    }

    fn set_transformation(&self, value: Option<String>) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.transformation = value;
        Ok(())
    }

    fn set_visibility(&self, value: u32) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.visibility = value;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// MessageProperties
// ---------------------------------------------------------------------------

/// The typed payload set client-side for enqueue, mirroring the reference's
/// `set_payload_bytes` / `set_payload_object` / `set_payload_json`.
enum MsgPayload {
    None,
    Bytes(Vec<u8>),
    Object(Py<DbObjectImpl>),
    Json(Py<PyAny>),
}

struct MsgPropsState {
    priority: i32,
    delay: i32,
    expiration: i32,
    correlation: Option<String>,
    exception_queue: Option<String>,
    state: i32,
    num_attempts: i32,
    delivery_mode: u16,
    enq_time: Option<QueryValue>,
    msgid: Option<Vec<u8>>,
    recipients: Option<Vec<String>>,
    payload_obj: MsgPayload,
    /// The Python-side `payload` attribute (the original value or the decoded
    /// dequeue payload), mirroring `props._impl.payload`.
    payload_attr: Option<Py<PyAny>>,
}

impl Default for MsgPropsState {
    fn default() -> Self {
        Self {
            priority: 0,
            delay: 0,
            expiration: -1,
            correlation: None,
            exception_queue: None,
            state: TNS_AQ_MSG_READY,
            num_attempts: 0,
            // Reference `ThinMsgPropsImpl` leaves delivery_mode at the Cython
            // uint32_t default (0) until a dequeue sets it (test 7806).
            delivery_mode: 0,
            enq_time: None,
            msgid: None,
            recipients: None,
            payload_obj: MsgPayload::None,
            payload_attr: None,
        }
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "ThinMsgPropsImpl")]
pub(crate) struct ThinMsgPropsImpl {
    inner: Arc<Mutex<MsgPropsState>>,
}

impl ThinMsgPropsImpl {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MsgPropsState::default())),
        }
    }

    /// Builds the protocol-level message properties + payload for enqueue,
    /// packing object/JSON payloads against the queue's payload kind.
    fn to_protocol(&self, py: Python<'_>) -> PyResult<AqMsgProps> {
        let state = self.inner.lock().map_err(runtime_error)?;
        let payload = match &state.payload_obj {
            MsgPayload::None => None,
            MsgPayload::Bytes(bytes) => Some(AqPayloadValue::Raw(bytes.clone())),
            MsgPayload::Object(obj) => {
                let obj_ref = obj.borrow(py);
                let oid = obj_ref.object_type.oid_bytes().unwrap_or_default();
                let image = obj_ref.pack_image(py)?;
                Some(AqPayloadValue::Object { oid, image })
            }
            MsgPayload::Json(value) => {
                let oson = py_value_to_oson(value.bind(py))?;
                Some(AqPayloadValue::Json(oson))
            }
        };
        Ok(AqMsgProps {
            priority: state.priority,
            delay: state.delay,
            expiration: state.expiration,
            correlation: state.correlation.clone(),
            exception_queue: state.exception_queue.clone(),
            state: state.state,
            enq_txn_id: None,
            recipients: state.recipients.clone().or_else(|| Some(Vec::new())),
            payload,
        })
    }

    /// Constructs a fresh impl carrying a dequeued message's fields, decoding
    /// the payload against the queue's payload type.
    fn from_deq_message(
        py: Python<'_>,
        message: AqDeqMessage,
        payload_type: Option<&DbObjectTypeImpl>,
        is_json: bool,
    ) -> PyResult<Py<Self>> {
        let payload_attr = decode_payload(py, &message.payload, payload_type, is_json)?;
        let state = MsgPropsState {
            priority: message.priority,
            delay: message.delay,
            expiration: message.expiration,
            correlation: message.correlation,
            exception_queue: message.exception_queue,
            state: message.state,
            num_attempts: message.num_attempts,
            delivery_mode: message.delivery_mode,
            enq_time: message.enq_time,
            msgid: message.msgid,
            recipients: None,
            payload_obj: MsgPayload::None,
            payload_attr: Some(payload_attr),
        };
        Py::new(
            py,
            Self {
                inner: Arc::new(Mutex::new(state)),
            },
        )
    }
}

/// Decodes a dequeued payload into the Python object the `payload` attribute
/// returns (bytes, DbObject, or JSON value).
fn decode_payload(
    py: Python<'_>,
    payload: &Option<AqDeqPayload>,
    payload_type: Option<&DbObjectTypeImpl>,
    is_json: bool,
) -> PyResult<Py<PyAny>> {
    match payload {
        Some(AqDeqPayload::Raw(bytes)) => Ok(PyBytes::new(py, bytes).into_any().unbind()),
        Some(AqDeqPayload::Json(value)) => oson_value_to_py(py, value),
        Some(AqDeqPayload::Object(image)) => {
            let Some(type_impl) = payload_type else {
                return Ok(py.None());
            };
            let object = DbObjectImpl::with_packed_data(type_impl.clone(), image.clone(), None);
            py_db_object_from_impl(py, object)
        }
        None => {
            if is_json {
                Ok(py.None())
            } else if payload_type.is_some() {
                // Object queue with an empty payload: return a new empty object.
                let object =
                    DbObjectImpl::with_packed_data(payload_type.unwrap().clone(), Vec::new(), None);
                py_db_object_from_impl(py, object)
            } else {
                // RAW with empty image already mapped to Raw(empty); reaching here
                // means no payload at all.
                Ok(PyBytes::new(py, b"").into_any().unbind())
            }
        }
    }
}

#[pymethods]
impl ThinMsgPropsImpl {
    fn get_num_attempts(&self) -> PyResult<i32> {
        Ok(self.inner.lock().map_err(runtime_error)?.num_attempts)
    }

    fn get_correlation(&self) -> PyResult<Option<String>> {
        Ok(self
            .inner
            .lock()
            .map_err(runtime_error)?
            .correlation
            .clone())
    }

    fn get_delay(&self) -> PyResult<i32> {
        Ok(self.inner.lock().map_err(runtime_error)?.delay)
    }

    fn get_delivery_mode(&self) -> PyResult<u16> {
        Ok(self.inner.lock().map_err(runtime_error)?.delivery_mode)
    }

    fn get_enq_time(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let state = self.inner.lock().map_err(runtime_error)?;
        match &state.enq_time {
            Some(QueryValue::DateTime {
                year,
                month,
                day,
                hour,
                minute,
                second,
                nanosecond,
            }) => {
                let dt = PyDateTime::new(
                    py,
                    *year,
                    *month,
                    *day,
                    *hour,
                    *minute,
                    *second,
                    nanosecond / 1000,
                    None,
                )?;
                Ok(dt.into_any().unbind())
            }
            _ => Ok(py.None()),
        }
    }

    fn get_exception_queue(&self) -> PyResult<Option<String>> {
        Ok(self
            .inner
            .lock()
            .map_err(runtime_error)?
            .exception_queue
            .clone())
    }

    fn get_expiration(&self) -> PyResult<i32> {
        Ok(self.inner.lock().map_err(runtime_error)?.expiration)
    }

    fn get_message_id(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let state = self.inner.lock().map_err(runtime_error)?;
        Ok(match &state.msgid {
            Some(bytes) => PyBytes::new(py, bytes).into_any().unbind(),
            None => py.None(),
        })
    }

    fn get_priority(&self) -> PyResult<i32> {
        Ok(self.inner.lock().map_err(runtime_error)?.priority)
    }

    fn get_state(&self) -> PyResult<i32> {
        Ok(self.inner.lock().map_err(runtime_error)?.state)
    }

    fn set_correlation(&self, value: Option<String>) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.correlation = value;
        Ok(())
    }

    fn set_delay(&self, value: i32) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.delay = value;
        Ok(())
    }

    fn set_exception_queue(&self, value: Option<String>) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.exception_queue = value;
        Ok(())
    }

    fn set_expiration(&self, value: i32) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.expiration = value;
        Ok(())
    }

    fn set_payload_bytes(&self, value: Vec<u8>) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.payload_obj = MsgPayload::Bytes(value);
        Ok(())
    }

    fn set_payload_object(&self, value: Py<DbObjectImpl>) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.payload_obj = MsgPayload::Object(value);
        Ok(())
    }

    fn set_payload_json(&self, value: Py<PyAny>) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.payload_obj = MsgPayload::Json(value);
        Ok(())
    }

    fn set_priority(&self, value: i32) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.priority = value;
        Ok(())
    }

    fn set_recipients(&self, value: Option<Vec<String>>) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.recipients = value;
        Ok(())
    }

    /// The `payload` attribute (set directly by the Python `MessageProperties`
    /// class and read back as `props.payload`).
    #[getter]
    fn get_payload(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self
            .inner
            .lock()
            .map_err(runtime_error)?
            .payload_attr
            .as_ref()
            .map(|value| value.clone_ref(py))
            .unwrap_or_else(|| py.None()))
    }

    #[setter]
    fn set_payload(&self, value: Py<PyAny>) -> PyResult<()> {
        self.inner.lock().map_err(runtime_error)?.payload_attr = Some(value);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------------

struct QueueState {
    name: String,
    is_json: bool,
    payload_type: Option<DbObjectTypeImpl>,
}

impl QueueState {
    fn kind(&self) -> AqPayloadKind {
        if self.is_json {
            AqPayloadKind::Json
        } else if self.payload_type.is_some() {
            AqPayloadKind::Object
        } else {
            AqPayloadKind::Raw
        }
    }

    fn queue_desc(&self) -> AqQueueDesc {
        let object_oid = self
            .payload_type
            .as_ref()
            .and_then(DbObjectTypeImpl::oid_bytes);
        AqQueueDesc::new(self.name.clone(), self.kind(), object_oid)
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "ThinQueueImpl")]
pub(crate) struct ThinQueueImpl {
    connection: Option<ConnHandle>,
    state: Option<Arc<Mutex<QueueState>>>,
    deq_options: ThinDeqOptionsImpl,
    enq_options: ThinEnqOptionsImpl,
}

impl ThinQueueImpl {
    pub(crate) fn new() -> Self {
        Self {
            connection: None,
            state: None,
            deq_options: ThinDeqOptionsImpl::new(),
            enq_options: ThinEnqOptionsImpl::new(),
        }
    }
}

#[pymethods]
impl ThinQueueImpl {
    fn initialize(
        &mut self,
        conn_impl: &Bound<'_, PyAny>,
        name: String,
        payload_type: Option<DbObjectTypeImpl>,
        is_json: bool,
    ) -> PyResult<()> {
        let conn = conn_impl.extract::<PyRef<'_, ThinConnImpl>>()?;
        self.connection = Some(Arc::clone(&conn.connection));
        self.state = Some(Arc::new(Mutex::new(QueueState {
            name,
            is_json,
            payload_type,
        })));
        Ok(())
    }

    #[getter]
    fn name(&self) -> PyResult<String> {
        let state = self.state.as_ref().ok_or_else(uninitialized)?;
        Ok(state.lock().map_err(runtime_error)?.name.clone())
    }

    #[getter]
    fn is_json(&self) -> PyResult<bool> {
        let state = self.state.as_ref().ok_or_else(uninitialized)?;
        Ok(state.lock().map_err(runtime_error)?.is_json)
    }

    #[getter]
    fn payload_type(&self) -> PyResult<Option<DbObjectTypeImpl>> {
        let state = self.state.as_ref().ok_or_else(uninitialized)?;
        Ok(state.lock().map_err(runtime_error)?.payload_type.clone())
    }

    #[getter]
    fn deq_options_impl(&self) -> ThinDeqOptionsImpl {
        self.deq_options.clone()
    }

    #[getter]
    fn enq_options_impl(&self) -> ThinEnqOptionsImpl {
        self.enq_options.clone()
    }

    fn _supports_deq_many(&self, _conn_impl: &Bound<'_, PyAny>) -> bool {
        // Container is 23ai, which supports array dequeue for all payload types.
        true
    }

    fn enq_one(&self, py: Python<'_>, props: &Bound<'_, PyAny>) -> PyResult<()> {
        let connection = self.connection.as_ref().ok_or_else(uninitialized)?.clone();
        let queue = self.queue_desc()?;
        let props_impl = props.extract::<PyRef<ThinMsgPropsImpl>>()?;
        let proto_props = props_impl.to_protocol(py)?;
        let enq = self.enq_options.to_protocol()?;
        let msgid = py.detach(|| {
            let mut guard = connection.lock().map_err(|e| e.to_string())?;
            let connection = guard
                .as_mut()
                .ok_or_else(|| "connection is closed".to_string())?;
            BlockingConnection::aq_enq_one(connection, &queue, &proto_props, &enq)
                .map_err(TaskError::from)
        });
        let msgid = msgid.map_err(|e| raise_task_error(&e, &connection))?;
        if let Some(id) = msgid {
            props_impl.inner.lock().map_err(runtime_error)?.msgid = Some(id);
        }
        Ok(())
    }

    fn deq_one(&self, py: Python<'_>) -> PyResult<Option<Py<ThinMsgPropsImpl>>> {
        let connection = self.connection.as_ref().ok_or_else(uninitialized)?.clone();
        let queue = self.queue_desc()?;
        let deq = self.deq_options.to_protocol()?;
        let result = py.detach(|| {
            let mut guard = connection.lock().map_err(|e| e.to_string())?;
            let connection = guard
                .as_mut()
                .ok_or_else(|| "connection is closed".to_string())?;
            BlockingConnection::aq_deq_one(connection, &queue, &deq).map_err(TaskError::from)
        });
        let result = result.map_err(|e| raise_task_error(&e, &connection))?;
        match result.message {
            None => Ok(None),
            Some(message) => {
                let (payload_type, is_json) = self.payload_type_and_json()?;
                let props = ThinMsgPropsImpl::from_deq_message(
                    py,
                    message,
                    payload_type.as_ref(),
                    is_json,
                )?;
                Ok(Some(props))
            }
        }
    }

    fn enq_many(&self, py: Python<'_>, props_list: &Bound<'_, PyAny>) -> PyResult<()> {
        let connection = self.connection.as_ref().ok_or_else(uninitialized)?.clone();
        let queue = self.queue_desc()?;
        let enq = self.enq_options.to_protocol()?;
        let mut proto_props = Vec::new();
        let mut impls = Vec::new();
        for item in props_list.try_iter()? {
            let item = item?;
            let props_impl = item.extract::<Py<ThinMsgPropsImpl>>()?;
            proto_props.push(props_impl.borrow(py).to_protocol(py)?);
            impls.push(props_impl);
        }
        let msgids = py.detach(|| {
            let mut guard = connection.lock().map_err(|e| e.to_string())?;
            let connection = guard
                .as_mut()
                .ok_or_else(|| "connection is closed".to_string())?;
            BlockingConnection::aq_enq_many(connection, &queue, &proto_props, &enq)
                .map_err(TaskError::from)
        });
        let msgids = msgids.map_err(|e| raise_task_error(&e, &connection))?;
        for (impl_obj, msgid) in impls.iter().zip(msgids.into_iter()) {
            impl_obj
                .borrow(py)
                .inner
                .lock()
                .map_err(runtime_error)?
                .msgid = Some(msgid);
        }
        Ok(())
    }

    fn deq_many(
        &self,
        py: Python<'_>,
        max_num_messages: u32,
    ) -> PyResult<Vec<Py<ThinMsgPropsImpl>>> {
        let connection = self.connection.as_ref().ok_or_else(uninitialized)?.clone();
        let queue = self.queue_desc()?;
        let deq = self.deq_options.to_protocol()?;
        let messages = py.detach(|| {
            let mut guard = connection.lock().map_err(|e| e.to_string())?;
            let connection = guard
                .as_mut()
                .ok_or_else(|| "connection is closed".to_string())?;
            BlockingConnection::aq_deq_many(connection, &queue, &deq, max_num_messages)
                .map_err(TaskError::from)
        });
        let messages = messages.map_err(|e| raise_task_error(&e, &connection))?;
        let (payload_type, is_json) = self.payload_type_and_json()?;
        let mut out = Vec::with_capacity(messages.len());
        for message in messages {
            out.push(ThinMsgPropsImpl::from_deq_message(
                py,
                message,
                payload_type.as_ref(),
                is_json,
            )?);
        }
        Ok(out)
    }
}

impl ThinQueueImpl {
    fn queue_desc(&self) -> PyResult<AqQueueDesc> {
        let state = self.state.as_ref().ok_or_else(uninitialized)?;
        Ok(state.lock().map_err(runtime_error)?.queue_desc())
    }

    fn payload_type_and_json(&self) -> PyResult<(Option<DbObjectTypeImpl>, bool)> {
        let state = self.state.as_ref().ok_or_else(uninitialized)?;
        let state = state.lock().map_err(runtime_error)?;
        Ok((state.payload_type.clone(), state.is_json))
    }
}

// ---------------------------------------------------------------------------
// AsyncQueue
// ---------------------------------------------------------------------------

#[pyclass(module = "oracledb.thin_impl", name = "AsyncThinQueueImpl")]
pub(crate) struct AsyncThinQueueImpl {
    connection: Option<ConnHandle>,
    state: Option<Arc<Mutex<QueueState>>>,
    deq_options: ThinDeqOptionsImpl,
    enq_options: ThinEnqOptionsImpl,
}

impl AsyncThinQueueImpl {
    pub(crate) fn new() -> Self {
        Self {
            connection: None,
            state: None,
            deq_options: ThinDeqOptionsImpl::new(),
            enq_options: ThinEnqOptionsImpl::new(),
        }
    }

    fn connection_handle(&self) -> PyResult<ConnHandle> {
        Ok(self.connection.as_ref().ok_or_else(uninitialized)?.clone())
    }

    fn queue_desc(&self) -> PyResult<AqQueueDesc> {
        let state = self.state.as_ref().ok_or_else(uninitialized)?;
        Ok(state.lock().map_err(runtime_error)?.queue_desc())
    }

    fn payload_type_and_json(&self) -> PyResult<(Option<DbObjectTypeImpl>, bool)> {
        let state = self.state.as_ref().ok_or_else(uninitialized)?;
        let state = state.lock().map_err(runtime_error)?;
        Ok((state.payload_type.clone(), state.is_json))
    }
}

#[pymethods]
impl AsyncThinQueueImpl {
    fn initialize(
        &mut self,
        conn_impl: &Bound<'_, PyAny>,
        name: String,
        payload_type: Option<DbObjectTypeImpl>,
        is_json: bool,
    ) -> PyResult<()> {
        let conn = conn_impl.extract::<PyRef<'_, AsyncThinConnImpl>>()?;
        self.connection = Some(Arc::clone(&conn.inner.connection));
        self.state = Some(Arc::new(Mutex::new(QueueState {
            name,
            is_json,
            payload_type,
        })));
        Ok(())
    }

    #[getter]
    fn name(&self) -> PyResult<String> {
        let state = self.state.as_ref().ok_or_else(uninitialized)?;
        Ok(state.lock().map_err(runtime_error)?.name.clone())
    }

    #[getter]
    fn is_json(&self) -> PyResult<bool> {
        let state = self.state.as_ref().ok_or_else(uninitialized)?;
        Ok(state.lock().map_err(runtime_error)?.is_json)
    }

    #[getter]
    fn payload_type(&self) -> PyResult<Option<DbObjectTypeImpl>> {
        let state = self.state.as_ref().ok_or_else(uninitialized)?;
        Ok(state.lock().map_err(runtime_error)?.payload_type.clone())
    }

    #[getter]
    fn deq_options_impl(&self) -> ThinDeqOptionsImpl {
        self.deq_options.clone()
    }

    #[getter]
    fn enq_options_impl(&self) -> ThinEnqOptionsImpl {
        self.enq_options.clone()
    }

    fn _supports_deq_many(&self, _conn_impl: &Bound<'_, PyAny>) -> bool {
        true
    }

    async fn enq_one(&self, props: Py<ThinMsgPropsImpl>) -> PyResult<()> {
        let conn = self.connection_handle()?;
        let queue = self.queue_desc()?;
        let enq = self.enq_options.to_protocol()?;
        let proto_props = Python::attach(|py| props.borrow(py).to_protocol(py))?;
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-aq-enq-one",
            conn,
            move |cx, connection| {
                Box::pin(async move {
                    connection
                        .aq_enq_one(cx, &queue, &proto_props, &enq)
                        .await
                        .map_err(TaskError::from)
                })
            },
        );
        let msgid = task.await.map_err(runtime_error)?;
        if let Some(id) = msgid {
            Python::attach(|py| -> PyResult<()> {
                props.borrow(py).inner.lock().map_err(runtime_error)?.msgid = Some(id);
                Ok(())
            })?;
        }
        Ok(())
    }

    async fn deq_one(&self) -> PyResult<Option<Py<ThinMsgPropsImpl>>> {
        let conn = self.connection_handle()?;
        let queue = self.queue_desc()?;
        let deq = self.deq_options.to_protocol()?;
        let queue_for_task = queue.clone();
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-aq-deq-one",
            conn,
            move |cx, connection| {
                Box::pin(async move {
                    connection
                        .aq_deq_one(cx, &queue_for_task, &deq)
                        .await
                        .map_err(TaskError::from)
                })
            },
        );
        let result = task.await.map_err(runtime_error)?;
        match result.message {
            None => Ok(None),
            Some(message) => {
                let (payload_type, is_json) = self.payload_type_and_json()?;
                Python::attach(|py| {
                    ThinMsgPropsImpl::from_deq_message(py, message, payload_type.as_ref(), is_json)
                        .map(Some)
                })
            }
        }
    }

    async fn enq_many(&self, props_list: Vec<Py<ThinMsgPropsImpl>>) -> PyResult<()> {
        let conn = self.connection_handle()?;
        let queue = self.queue_desc()?;
        let enq = self.enq_options.to_protocol()?;
        let proto_props = Python::attach(|py| -> PyResult<Vec<AqMsgProps>> {
            props_list
                .iter()
                .map(|p| p.borrow(py).to_protocol(py))
                .collect()
        })?;
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-aq-enq-many",
            conn,
            move |cx, connection| {
                Box::pin(async move {
                    connection
                        .aq_enq_many(cx, &queue, &proto_props, &enq)
                        .await
                        .map_err(TaskError::from)
                })
            },
        );
        let msgids = task.await.map_err(runtime_error)?;
        Python::attach(|py| -> PyResult<()> {
            for (impl_obj, msgid) in props_list.iter().zip(msgids.into_iter()) {
                impl_obj
                    .borrow(py)
                    .inner
                    .lock()
                    .map_err(runtime_error)?
                    .msgid = Some(msgid);
            }
            Ok(())
        })?;
        Ok(())
    }

    async fn deq_many(&self, max_num_messages: u32) -> PyResult<Vec<Py<ThinMsgPropsImpl>>> {
        let conn = self.connection_handle()?;
        let queue = self.queue_desc()?;
        let deq = self.deq_options.to_protocol()?;
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-aq-deq-many",
            conn,
            move |cx, connection| {
                Box::pin(async move {
                    connection
                        .aq_deq_many(cx, &queue, &deq, max_num_messages)
                        .await
                        .map_err(TaskError::from)
                })
            },
        );
        let messages = task.await.map_err(runtime_error)?;
        let (payload_type, is_json) = self.payload_type_and_json()?;
        Python::attach(|py| -> PyResult<Vec<Py<ThinMsgPropsImpl>>> {
            messages
                .into_iter()
                .map(|message| {
                    ThinMsgPropsImpl::from_deq_message(py, message, payload_type.as_ref(), is_json)
                })
                .collect()
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn uninitialized() -> PyErr {
    runtime_error("queue is not initialized")
}
