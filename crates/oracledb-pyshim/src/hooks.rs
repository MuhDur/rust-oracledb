use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::*;

pub(crate) static PASSWORD_OVERRIDES: OnceLock<Mutex<BTreeMap<String, String>>> = OnceLock::new();
pub(crate) static NEXT_CONNECT_ARGS: OnceLock<Mutex<VecDeque<ConnectArgs>>> = OnceLock::new();
pub(crate) static NEXT_CONNECT_ARGS_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Default)]
pub(crate) struct ConnectArgs {
    id: u64,
    pub(crate) password: Option<String>,
    pub(crate) new_password: Option<String>,
    pub(crate) invalid_user_dsn: bool,
}

pub(crate) fn password_overrides() -> &'static Mutex<BTreeMap<String, String>> {
    PASSWORD_OVERRIDES.get_or_init(|| Mutex::new(BTreeMap::new()))
}

pub(crate) fn next_connect_args_queue() -> &'static Mutex<VecDeque<ConnectArgs>> {
    NEXT_CONNECT_ARGS.get_or_init(|| Mutex::new(VecDeque::new()))
}

pub(crate) fn consume_next_connect_args() -> PyResult<ConnectArgs> {
    Ok(next_connect_args_queue()
        .lock()
        .map_err(runtime_error)?
        .pop_front()
        .unwrap_or_default())
}

pub(crate) fn password_override_for_user(user: &str) -> PyResult<Option<String>> {
    Ok(password_overrides()
        .lock()
        .map_err(runtime_error)?
        .get(&user.to_ascii_uppercase())
        .cloned())
}

pub(crate) fn set_password_override_for_user(user: &str, password: &str) -> PyResult<()> {
    password_overrides()
        .lock()
        .map_err(runtime_error)?
        .insert(user.to_ascii_uppercase(), password.to_string());
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (password=None, new_password=None, invalid_user_dsn=false))]
pub(crate) fn record_next_connect_args(
    password: Option<String>,
    new_password: Option<String>,
    invalid_user_dsn: bool,
) -> PyResult<u64> {
    let id = NEXT_CONNECT_ARGS_ID.fetch_add(1, Ordering::Relaxed);
    next_connect_args_queue()
        .lock()
        .map_err(runtime_error)?
        .push_back(ConnectArgs {
            id,
            password,
            new_password,
            invalid_user_dsn,
        });
    Ok(id)
}

#[pyfunction]
pub(crate) fn discard_pending_connect_args(id: u64) -> PyResult<bool> {
    let mut queue = next_connect_args_queue().lock().map_err(runtime_error)?;
    if let Some(pos) = queue.iter().position(|entry| entry.id == id) {
        queue.remove(pos);
        return Ok(true);
    }
    Ok(false)
}

pub(crate) fn env_password_for_user(user: &str) -> PyResult<String> {
    if let Some(password) = password_override_for_user(user)? {
        return Ok(password);
    }
    if let Ok(password) = std::env::var("ORACLEDB_SHIM_PASSWORD") {
        return Ok(password);
    }
    if std::env::var("PYO_TEST_MAIN_USER")
        .is_ok_and(|main_user| user.eq_ignore_ascii_case(&main_user))
    {
        return std::env::var("PYO_TEST_MAIN_PASSWORD")
            .or_else(|_| std::env::var("PYO_TEST_PASSWORD"))
            .map_err(|_| {
                PyRuntimeError::new_err(
                    "oracledb-pyshim cannot read password from ConnectParamsImpl; set PYO_TEST_MAIN_PASSWORD",
                )
            });
    }
    let proxy_user = std::env::var("PYO_TEST_PROXY_USER").unwrap_or_default();
    if !proxy_user.is_empty() && user.eq_ignore_ascii_case(&proxy_user) {
        return std::env::var("PYO_TEST_PROXY_PASSWORD")
            .or_else(|_| std::env::var("PYO_TEST_MAIN_PASSWORD"))
            .map_err(|_| {
                PyRuntimeError::new_err(
                    "oracledb-pyshim cannot read proxy password from ConnectParamsImpl; set PYO_TEST_PROXY_PASSWORD",
                )
            });
    }
    std::env::var("PYO_TEST_MAIN_PASSWORD").map_err(|_| {
        PyRuntimeError::new_err(
            "oracledb-pyshim cannot read password from ConnectParamsImpl; set ORACLEDB_SHIM_PASSWORD",
        )
    })
}
