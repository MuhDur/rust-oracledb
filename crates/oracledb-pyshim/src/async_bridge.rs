use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;

use asupersync::runtime::{reactor, Runtime, RuntimeBuilder};
use asupersync::Cx;
use oracledb::protocol::{ProtocolError, ServerErrorDetails};
use oracledb::{
    BlockingConnection, ConnectOptions, Connection as RustConnection, Error as DriverError,
};
use pyo3::prelude::*;

use crate::*;

#[derive(Debug)]
pub(crate) struct TaskError {
    message: String,
    server_error: Option<Box<ServerErrorDetails>>,
}

impl TaskError {
    fn from_driver_error(err: DriverError) -> Self {
        let server_error = match &err {
            DriverError::Protocol(ProtocolError::ServerErrorInfo(details)) => Some(details.clone()),
            DriverError::Protocol(ProtocolError::ServerErrorWithRowCount {
                message,
                row_count,
            }) => Some(Box::new(ServerErrorDetails {
                message: message.clone(),
                row_count: *row_count,
                ..ServerErrorDetails::default()
            })),
            _ => None,
        };
        Self {
            message: err.to_string(),
            server_error,
        }
    }

    pub(crate) fn server_error_details(&self) -> Option<&ServerErrorDetails> {
        self.server_error.as_deref()
    }

    /// PL/SQL statements report no parse offset; the reference substitutes
    /// `error_info.rowcount + message.offset` instead — the number of
    /// successful iterations plus the executemany batch offset
    /// (messages/execute.pyx `process`).
    pub(crate) fn with_plsql_row_offset(mut self, offset: usize) -> Self {
        if let Some(details) = self.server_error.as_deref_mut() {
            if details.pos == 0 {
                let offset = (offset as u64).saturating_add(details.row_count);
                details.pos = i32::try_from(offset).unwrap_or(i32::MAX);
            }
        }
        self
    }
}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message.fmt(f)
    }
}

impl From<String> for TaskError {
    fn from(message: String) -> Self {
        Self {
            message,
            server_error: None,
        }
    }
}

impl From<&str> for TaskError {
    fn from(message: &str) -> Self {
        message.to_string().into()
    }
}

impl From<DriverError> for TaskError {
    fn from(err: DriverError) -> Self {
        Self::from_driver_error(err)
    }
}

pub(crate) struct BlockingTaskState<T> {
    result: Option<Result<T, TaskError>>,
    waker: Option<Waker>,
}

pub(crate) struct BlockingTask<T> {
    shared: Arc<Mutex<BlockingTaskState<T>>>,
}

impl<T> BlockingTask<T> {
    fn ready(result: Result<T, TaskError>) -> Self {
        Self {
            shared: Arc::new(Mutex::new(BlockingTaskState {
                result: Some(result),
                waker: None,
            })),
        }
    }
}

impl<T> Future for BlockingTask<T> {
    type Output = Result<T, TaskError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut shared = match self.shared.lock() {
            Ok(shared) => shared,
            Err(err) => return Poll::Ready(Err(err.to_string().into())),
        };
        if let Some(result) = shared.result.take() {
            Poll::Ready(result)
        } else {
            shared.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

pub(crate) fn spawn_blocking_task<T, F>(name: &'static str, task: F) -> BlockingTask<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, TaskError> + Send + 'static,
{
    let shared = Arc::new(Mutex::new(BlockingTaskState {
        result: None,
        waker: None,
    }));
    let thread_shared = Arc::clone(&shared);
    let spawn_result = thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            let result = task();
            let waker = match thread_shared.lock() {
                Ok(mut shared) => {
                    shared.result = Some(result);
                    shared.waker.take()
                }
                Err(_) => None,
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        });
    match spawn_result {
        Ok(_) => BlockingTask { shared },
        Err(err) => {
            BlockingTask::ready(Err(format!("failed to spawn blocking task: {err}").into()))
        }
    }
}

pub(crate) fn build_pyshim_io_runtime() -> Result<Runtime, String> {
    let reactor = reactor::create_reactor().map_err(|err| err.to_string())?;
    RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .map_err(|err| err.to_string())
}

pub(crate) fn spawn_async_connection_task<T, F>(
    name: &'static str,
    connection: Arc<Mutex<Option<RustConnection>>>,
    task: F,
) -> BlockingTask<T>
where
    T: Send + 'static,
    F: for<'a> FnOnce(
            &'a Cx,
            &'a mut RustConnection,
        ) -> Pin<Box<dyn Future<Output = Result<T, TaskError>> + 'a>>
        + Send
        + 'static,
{
    spawn_blocking_task(name, move || {
        let mut guard = connection.lock().map_err(|err| err.to_string())?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| "connection is closed".to_string())?;
        let runtime = build_pyshim_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| "asupersync did not install an ambient Cx".to_string())?;
            task(&cx, connection).await
        })
    })
}

pub(crate) fn spawn_async_connect_task(options: ConnectOptions) -> BlockingTask<RustConnection> {
    spawn_blocking_task("oracledb-pyshim-async-connect", move || {
        let runtime = build_pyshim_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| "asupersync did not install an ambient Cx".to_string())?;
            RustConnection::connect(&cx, options)
                .await
                .map_err(TaskError::from)
        })
    })
}

pub(crate) fn spawn_async_close_task(connection: RustConnection) -> BlockingTask<()> {
    spawn_blocking_task("oracledb-pyshim-async-close", move || {
        let runtime = build_pyshim_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| "asupersync did not install an ambient Cx".to_string())?;
            connection.close(&cx).await.map_err(TaskError::from)
        })
    })
}

pub(crate) fn close_connection_result(connection: RustConnection) -> Result<(), String> {
    BlockingConnection::close(connection).map_err(|err| err.to_string())
}

pub(crate) fn close_result_to_py<E: std::fmt::Display>(result: Result<(), E>) -> PyResult<()> {
    match result {
        Ok(()) => Ok(()),
        Err(err) => {
            let message = err.to_string();
            if message.contains("Broken pipe")
                || message.contains("Transport endpoint is not connected")
            {
                Ok(())
            } else {
                Err(runtime_error(message))
            }
        }
    }
}
