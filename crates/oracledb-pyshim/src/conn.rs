use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use asupersync::Cx;
use oracledb::protocol::thin::{
    dbobject_attr_max_size, dbobject_attr_precision_scale, dbobject_rowtype_attr_max_size,
    public_dbtype_name_from_oracle_type_name, BindValue, QueryValue, CS_FORM_IMPLICIT,
    CS_FORM_NCHAR, ORA_TYPE_NUM_BLOB, ORA_TYPE_NUM_CLOB, ORA_TYPE_NUM_CURSOR, ORA_TYPE_NUM_NUMBER,
    ORA_TYPE_NUM_RAW,
};
use oracledb::protocol::ClientIdentity;
use oracledb::{
    BlockingConnection, CancelHandle, ConnectOptions, Connection as RustConnection, Execute,
    ExecuteOutcome, Query,
};
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::*;

/// An owned TPC transaction id: `(format_id, gtid_bytes, bqual_bytes)`.
pub(crate) type OwnedXid = (u32, Vec<u8>, Vec<u8>);

/// Attach a per-call timeout to an [`Execute`] builder, matching the old
/// `execute_query_with_timeout(..)` semantics: a `None`/zero `call_timeout`
/// leaves the operation un-timed (no `Duration`), so it never adopts an
/// ambient deadline the old path ignored.
pub(crate) fn execute_with_call_timeout(sql: &str, call_timeout: Option<u32>) -> Execute<'_> {
    let execute = Execute::new(sql);
    match call_timeout {
        Some(ms) if ms > 0 => execute.timeout(std::time::Duration::from_millis(u64::from(ms))),
        _ => execute,
    }
}

/// Run a SELECT through the [`Query`] family and return ONLY the first fetch
/// batch as raw `QueryValue` rows, reproducing the old
/// `execute_query_with_binds_and_timeout(sql, prefetch, binds, ct)` shape used by
/// the connection's data-dictionary probes: the same `prefetch_rows`, no
/// continuation fetch (the old call read `result.rows` — the first batch only),
/// and `stream_lobs()` so define-requiring columns keep their describe-only
/// behavior instead of being materialized (the old path did not materialize).
fn query_first_batch_with_binds(
    connection: &mut RustConnection,
    sql: &str,
    prefetch_rows: u32,
    binds: Vec<BindValue>,
    call_timeout: Option<u32>,
) -> Result<Vec<Vec<Option<QueryValue>>>, oracledb::Error> {
    let mut query = Query::new(sql)
        .prefetch(prefetch_rows)
        .stream_lobs()
        .bind(binds);
    if let Some(ms) = call_timeout.filter(|value| *value > 0) {
        query = query.timeout(std::time::Duration::from_millis(u64::from(ms)));
    }
    let rows = BlockingConnection::query_with(connection, query)?;
    Ok(rows
        .batch()
        .iter()
        .map(|row| row.values().to_vec())
        .collect())
}

/// Extract an `Xid` (reference `Xid` namedtuple: `(format_id, gtid, bqual)`)
/// into `(format_id, gtid_bytes, bqual_bytes)`. The gtid / bqual members may be
/// `bytes` or `str`; `str` is UTF-8 encoded to mirror the reference message
/// writer (tpc_switch.pyx). A non-3-element or wrongly typed value raises
/// `TypeError`, matching the reference `_verify_xid`.
pub(crate) fn extract_xid(xid: &Bound<'_, PyAny>) -> PyResult<OwnedXid> {
    let len = xid
        .len()
        .map_err(|_| PyTypeError::new_err("xid must be a 3-element Xid"))?;
    if len != 3 {
        return Err(PyTypeError::new_err("xid must be a 3-element Xid"));
    }
    let format_id: u32 = xid
        .get_item(0)
        .and_then(|item| item.extract())
        .map_err(|_| PyTypeError::new_err("xid format_id must be an integer"))?;
    let gtid = extract_xid_member(&xid.get_item(1)?, "global_transaction_id")?;
    let bqual = extract_xid_member(&xid.get_item(2)?, "branch_qualifier")?;
    Ok((format_id, gtid, bqual))
}

/// Extract a single `Xid` member (`bytes` directly, or `str` UTF-8 encoded).
fn extract_xid_member(value: &Bound<'_, PyAny>, name: &str) -> PyResult<Vec<u8>> {
    if let Ok(bytes) = value.cast::<PyBytes>() {
        return Ok(bytes.as_bytes().to_vec());
    }
    if let Ok(text) = value.extract::<String>() {
        return Ok(text.into_bytes());
    }
    Err(PyTypeError::new_err(format!(
        "xid {name} must be bytes or str"
    )))
}

/// Extract an optional `Xid`: `None` (the implicit current transaction) or an
/// `Xid` namedtuple.
pub(crate) fn extract_optional_xid(xid: &Bound<'_, PyAny>) -> PyResult<Option<OwnedXid>> {
    if xid.is_none() {
        Ok(None)
    } else {
        extract_xid(xid).map(Some)
    }
}

/// Borrow an owned optional xid as the `(u32, &[u8], &[u8])` tuple the driver
/// methods accept.
pub(crate) fn xid_as_refs(xid: &Option<OwnedXid>) -> Option<(u32, &[u8], &[u8])> {
    xid.as_ref()
        .map(|(format_id, gtid, bqual)| (*format_id, gtid.as_slice(), bqual.as_slice()))
}

#[derive(Debug)]
// d49: migrate to oracledb (session state belongs on driver Connection)
pub(crate) struct ThinConnState {
    current_schema: Option<String>,
    current_schema_modified: bool,
    pub(crate) edition: Option<String>,
    pub(crate) edition_probe_started: bool,
    external_name: Option<String>,
    internal_name: Option<String>,
    pub(crate) call_timeout: u32,
    stmt_cache_size: u32,
    invalid_connect_string: bool,
    dbop_operation: Option<(String, i64)>,
}

impl ThinConnState {
    fn new(stmt_cache_size: u32, edition: Option<String>, invalid_connect_string: bool) -> Self {
        Self {
            current_schema: None,
            current_schema_modified: false,
            edition_probe_started: edition.is_some(),
            edition,
            external_name: None,
            internal_name: None,
            call_timeout: 0,
            stmt_cache_size,
            invalid_connect_string,
            dbop_operation: None,
        }
    }

    /// Records side effects of an executed statement on cached session state.
    /// The transaction-in-progress flag is now derived from the wire end-of-call
    /// status on the driver (reference protocol.pyx `_txn_in_progress`), so this
    /// only tracks the schema/edition that the reference reads back from
    /// `alter session` without a round trip.
    pub(crate) fn record_statement(&mut self, statement: &str) {
        if let Some(schema) = parse_alter_session_value(statement, "current_schema") {
            self.current_schema = Some(schema);
            self.current_schema_modified = false;
            return;
        }
        if let Some(edition) = parse_alter_session_value(statement, "edition") {
            self.edition = Some(edition.to_ascii_uppercase());
            self.edition_probe_started = true;
        }
    }
}

// d49: migrate to oracledb (session state belongs on driver Connection)
pub(crate) fn apply_pending_current_schema_from_state(
    state: &Arc<Mutex<ThinConnState>>,
    connection: &mut RustConnection,
    call_timeout: Option<u32>,
) -> PyResult<()> {
    let pending_schema = {
        let mut state = state.lock().map_err(runtime_error)?;
        if !state.current_schema_modified {
            None
        } else {
            state.current_schema_modified = false;
            state.current_schema.clone()
        }
    };
    let Some(schema) = pending_schema else {
        return Ok(());
    };
    let identifier = sql_identifier(&schema)?;
    let result = BlockingConnection::execute_with(
        connection,
        execute_with_call_timeout(
            &format!("alter session set current_schema = {identifier}"),
            call_timeout,
        ),
    )
    .map_err(runtime_error);
    if result.is_err() {
        state.lock().map_err(runtime_error)?.current_schema_modified = true;
    }
    result.map(|_| ())
}

pub(crate) async fn apply_pending_current_schema_from_state_async(
    cx: &Cx,
    state: &Arc<Mutex<ThinConnState>>,
    connection: &mut RustConnection,
    call_timeout: Option<u32>,
) -> Result<(), String> {
    let pending_schema = {
        let mut state = state.lock().map_err(|err| err.to_string())?;
        if !state.current_schema_modified {
            None
        } else {
            state.current_schema_modified = false;
            state.current_schema.clone()
        }
    };
    let Some(schema) = pending_schema else {
        return Ok(());
    };
    let identifier = match sql_identifier(&schema) {
        Ok(identifier) => identifier,
        Err(err) => {
            state
                .lock()
                .map_err(|err| err.to_string())?
                .current_schema_modified = true;
            return Err(err.to_string());
        }
    };
    let result = connection
        .execute_with(
            cx,
            execute_with_call_timeout(
                &format!("alter session set current_schema = {identifier}"),
                call_timeout,
            ),
        )
        .await
        .map_err(|err| err.to_string());
    if result.is_err() {
        state
            .lock()
            .map_err(|err| err.to_string())?
            .current_schema_modified = true;
    }
    result.map(|_| ())
}

#[pyclass(module = "oracledb.thin_impl", name = "ThinConnImpl")]
pub(crate) struct ThinConnImpl {
    pub(crate) connection: Arc<Mutex<Option<RustConnection>>>,
    pub(crate) cancel_handle: Arc<Mutex<Option<CancelHandle>>>,
    cancel_requested: Arc<AtomicBool>,
    pub(crate) state: Arc<Mutex<ThinConnState>>,
    pub(crate) dsn: String,
    pub(crate) username: String,
    pub(crate) proxy_user: Option<String>,
    pub(crate) server_version: (u8, u8, u8, u8, u8),
    pub(crate) autocommit: bool,
    autocommit_state: Arc<Mutex<bool>>,
    pub(crate) tag: Option<String>,
    pub(crate) warning: Option<Py<PyAny>>,
    pub(crate) inputtypehandler: Option<Py<PyAny>>,
    pub(crate) outputtypehandler: Option<Py<PyAny>>,
    pub(crate) invoke_session_callback: bool,
    pub(crate) thin: bool,
    connect_password: Option<String>,
    new_password: Option<String>,
    /// Engine identity when this connection is owned by a pool.
    pub(crate) pool_conn_id: Option<u64>,
    /// The [`ConnectOptions`] used for the primary connection, retained so the
    /// CQN background ("emon") connection can be built by cloning them and
    /// injecting `(SERVER=emon)`. Populated on `connect()`. (The DPY-1007
    /// double-unsubscribe guard lives in the shipped python `unsubscribe`,
    /// which checks `subscr._impl is None`, so no shim-side tracking is needed.)
    pub(crate) connect_options: Arc<Mutex<Option<ConnectOptions>>>,
}

pub(crate) struct PreparedConnect {
    pub(crate) options: ConnectOptions,
    pub(crate) password: String,
    pub(crate) new_password: Option<String>,
    pub(crate) edition: Option<String>,
}

impl ThinConnImpl {
    pub(crate) fn prepare_connect(
        &mut self,
        params_impl: &Bound<'_, PyAny>,
    ) -> PyResult<PreparedConnect> {
        if self
            .state
            .lock()
            .map_err(runtime_error)?
            .invalid_connect_string
        {
            return Err(dpy_database_error(
                "DPY-4000",
                "cannot connect with a username but no password in the connect string",
            ));
        }
        if self.dsn.trim().is_empty() {
            return Err(raise_not_supported("bequeath"));
        }
        let program = get_string_attr(params_impl, "program")?;
        let machine = get_string_attr(params_impl, "machine")?;
        let terminal = get_string_attr(params_impl, "terminal")?;
        let osuser = get_string_attr(params_impl, "osuser")?;
        let driver_name = get_optional_string_attr(params_impl, "driver_name")?
            .unwrap_or_else(|| "rust-oracledb thn : 0.0.0".into());
        let password = self
            .connect_password
            .clone()
            .map(Ok)
            .unwrap_or_else(|| env_password_for_user(&self.username))?;
        let app_context = get_app_context_attr(params_impl)?;
        let edition = get_optional_string_attr(params_impl, "edition")?;
        let sdu = get_connect_sdu_attr(params_impl)?.unwrap_or(8192);
        if let Some(stmt_cache_size) = get_optional_u32_attr(params_impl, "stmtcachesize")? {
            self.state.lock().map_err(runtime_error)?.stmt_cache_size = stmt_cache_size;
        }
        let identity = ClientIdentity::new(program, machine, osuser, terminal, driver_name)
            .map_err(runtime_error)?;
        let options = ConnectOptions::new(
            self.dsn.clone(),
            self.username.clone(),
            password.clone(),
            identity,
        )
        .with_app_context(app_context)
        .with_proxy_user(self.proxy_user.clone())
        .with_sdu(sdu);
        Ok(PreparedConnect {
            options,
            password,
            new_password: self.new_password.clone(),
            edition,
        })
    }

    /// Run `f` with the locked live connection, mapping a closed connection and
    /// any driver error to the appropriate Python exception. Used by the CQN
    /// subscription impl for the primary-connection round trips.
    pub(crate) fn with_connection<T>(
        &self,
        f: impl FnOnce(&mut RustConnection) -> Result<T, oracledb::Error>,
    ) -> PyResult<T> {
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        f(connection).map_err(runtime_error)
    }

    /// Build the connect options for the CQN background ("emon") connection:
    /// the retained primary options with `(SERVER=emon)` injected. Errors if the
    /// primary connect options were never recorded (connection not established).
    pub(crate) fn emon_connect_options(&self) -> PyResult<ConnectOptions> {
        let guard = self.connect_options.lock().map_err(runtime_error)?;
        let options = guard
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("connection is not established"))?;
        Ok(options.clone().with_server_type_emon(true))
    }

    fn apply_pending_current_schema(
        &self,
        connection: &mut RustConnection,
        call_timeout: Option<u32>,
    ) -> PyResult<()> {
        apply_pending_current_schema_from_state(&self.state, connection, call_timeout)
    }

    fn execute_with_binds(&self, sql: &str, binds: &[BindValue]) -> PyResult<ExecuteOutcome> {
        let call_timeout = self.call_timeout()?;
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        self.apply_pending_current_schema(connection, call_timeout)?;
        let execute = execute_with_call_timeout(sql, call_timeout).bind(binds.to_vec());
        BlockingConnection::execute_with(connection, execute).map_err(runtime_error)
    }

    fn execute_statement(&self, sql: &str) -> PyResult<()> {
        let call_timeout = self.call_timeout()?;
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        self.apply_pending_current_schema(connection, call_timeout)?;
        BlockingConnection::execute_with(connection, execute_with_call_timeout(sql, call_timeout))
            .map_err(runtime_error)?;
        Ok(())
    }

    fn execute_statement_with_binds(&self, sql: &str, binds: &[BindValue]) -> PyResult<()> {
        self.execute_with_binds(sql, binds)?;
        Ok(())
    }

    fn query_first_value(&self, sql: &str) -> PyResult<Option<QueryValue>> {
        let call_timeout = self.call_timeout()?;
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        self.apply_pending_current_schema(connection, call_timeout)?;
        let rows = query_first_batch_with_binds(connection, sql, 1, Vec::new(), call_timeout)
            .map_err(runtime_error)?;
        Ok(rows.first().and_then(|row| row.first()).cloned().flatten())
    }

    fn query_first_row_with_binds(
        &self,
        sql: &str,
        binds: &[BindValue],
    ) -> PyResult<Option<Vec<Option<QueryValue>>>> {
        let call_timeout = self.call_timeout()?;
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        self.apply_pending_current_schema(connection, call_timeout)?;
        let rows = query_first_batch_with_binds(connection, sql, 1, binds.to_vec(), call_timeout)
            .map_err(runtime_error)?;
        Ok(rows.into_iter().next())
    }

    fn query_rows_with_binds(
        &self,
        sql: &str,
        binds: &[BindValue],
    ) -> PyResult<Vec<Vec<Option<QueryValue>>>> {
        let call_timeout = self.call_timeout()?;
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        self.apply_pending_current_schema(connection, call_timeout)?;
        query_first_batch_with_binds(connection, sql, 100, binds.to_vec(), call_timeout)
            .map_err(runtime_error)
    }

    fn query_first_text(&self, sql: &str) -> PyResult<Option<String>> {
        self.query_first_value(sql)
            .map(|value| query_value_to_string(&value))
    }

    fn query_first_i64(&self, sql: &str) -> PyResult<i64> {
        let value = self.query_first_value(sql)?;
        query_value_to_i64(&value)
    }

    fn object_type_attrs(&self, schema: &str, type_name: &str) -> PyResult<Vec<DbObjectAttrImpl>> {
        let rows = self.query_rows_with_binds(
            "select attr_name, attr_type_name, length, precision, scale, attr_type_owner \
             from all_type_attrs \
             where owner = :1 and type_name = :2 \
             order by attr_no",
            &[
                BindValue::Text(schema.to_ascii_uppercase()),
                BindValue::Text(type_name.to_ascii_uppercase()),
            ],
        )?;
        rows.into_iter()
            .map(|row| {
                let name = row
                    .first()
                    .and_then(query_value_to_string)
                    .unwrap_or_default()
                    .to_ascii_uppercase();
                let attr_type_name = row
                    .get(1)
                    .and_then(query_value_to_string)
                    .unwrap_or_else(|| "VARCHAR2".to_string());
                let attr_type_owner = row
                    .get(5)
                    .and_then(query_value_to_string)
                    .unwrap_or_else(|| schema.to_ascii_uppercase());
                let dbtype_name = public_dbtype_name_from_oracle_type_name(&attr_type_name);
                let (precision, scale) = dbobject_attr_precision_scale(
                    &attr_type_name,
                    row.get(3).and_then(query_value_to_i8),
                    row.get(4).and_then(query_value_to_i8),
                );
                Ok(DbObjectAttrImpl {
                    name,
                    dbtype_name: dbtype_name.to_string(),
                    objtype: if dbtype_name == "DB_TYPE_OBJECT" {
                        Some(self.object_type_shallow(&attr_type_owner, &attr_type_name)?)
                    } else {
                        None
                    },
                    max_size: dbobject_attr_max_size(
                        &attr_type_name,
                        row.get(2).and_then(query_value_to_u32),
                    ),
                    precision,
                    scale,
                })
            })
            .collect()
    }

    fn plsql_type_attrs(
        &self,
        schema: &str,
        package_name: &str,
        type_name: &str,
    ) -> PyResult<Vec<DbObjectAttrImpl>> {
        let rows = self.query_rows_with_binds(
            "select attr_name, attr_type_owner, attr_type_package, attr_type_name, length, precision, scale \
             from all_plsql_type_attrs \
             where owner = :1 and package_name = :2 and type_name = :3 \
             order by attr_no",
            &[
                BindValue::Text(schema.to_ascii_uppercase()),
                BindValue::Text(package_name.to_ascii_uppercase()),
                BindValue::Text(type_name.to_ascii_uppercase()),
            ],
        )?;
        rows.into_iter()
            .map(|row| {
                let name = row
                    .first()
                    .and_then(query_value_to_string)
                    .unwrap_or_default()
                    .to_ascii_uppercase();
                let attr_type_owner = row
                    .get(1)
                    .and_then(query_value_to_string)
                    .unwrap_or_else(|| schema.to_ascii_uppercase());
                let attr_type_package = row.get(2).and_then(query_value_to_string);
                let attr_type_name = row
                    .get(3)
                    .and_then(query_value_to_string)
                    .unwrap_or_else(|| "VARCHAR2".to_string());
                let dbtype_name = public_dbtype_name_from_oracle_type_name(&attr_type_name);
                let (precision, scale) = dbobject_attr_precision_scale(
                    &attr_type_name,
                    row.get(5).and_then(query_value_to_i8),
                    row.get(6).and_then(query_value_to_i8),
                );
                let objtype = if dbtype_name == "DB_TYPE_OBJECT" {
                    if let Some(attr_type_package) = attr_type_package {
                        Some(self.plsql_type_shallow(
                            &attr_type_owner,
                            &attr_type_package,
                            &attr_type_name,
                        )?)
                    } else {
                        Some(self.object_type_shallow(&attr_type_owner, &attr_type_name)?)
                    }
                } else {
                    None
                };
                Ok(DbObjectAttrImpl {
                    name,
                    dbtype_name: dbtype_name.to_string(),
                    objtype,
                    max_size: dbobject_attr_max_size(
                        &attr_type_name,
                        row.get(4).and_then(query_value_to_u32),
                    ),
                    precision,
                    scale,
                })
            })
            .collect()
    }

    fn rowtype_attrs(&self, schema: &str, table_name: &str) -> PyResult<Vec<DbObjectAttrImpl>> {
        let rows = self.query_rows_with_binds(
            "select column_name, data_type, data_length, data_precision, data_scale, data_type_owner, char_length \
             from all_tab_cols \
             where owner = :1 and table_name = :2 and hidden_column = 'NO' \
             order by internal_column_id",
            &[
                BindValue::Text(schema.to_ascii_uppercase()),
                BindValue::Text(table_name.to_ascii_uppercase()),
            ],
        )?;
        if rows.is_empty() {
            return Err(raise_invalid_object_type_name(&format!(
                "{table_name}%ROWTYPE"
            )));
        }
        rows.into_iter()
            .map(|row| {
                let name = row
                    .first()
                    .and_then(query_value_to_string)
                    .unwrap_or_default()
                    .to_ascii_uppercase();
                let data_type = row
                    .get(1)
                    .and_then(query_value_to_string)
                    .unwrap_or_else(|| "VARCHAR2".to_string());
                let data_type_owner = row
                    .get(5)
                    .and_then(query_value_to_string)
                    .unwrap_or_else(|| schema.to_ascii_uppercase());
                let dbtype_name = public_dbtype_name_from_oracle_type_name(&data_type);
                let (precision, scale) = dbobject_attr_precision_scale(
                    &data_type,
                    row.get(3).and_then(query_value_to_i8),
                    row.get(4).and_then(query_value_to_i8),
                );
                Ok(DbObjectAttrImpl {
                    name,
                    dbtype_name: dbtype_name.to_string(),
                    objtype: if dbtype_name == "DB_TYPE_OBJECT" {
                        Some(self.object_type_shallow(&data_type_owner, &data_type)?)
                    } else {
                        None
                    },
                    max_size: dbobject_rowtype_attr_max_size(
                        &data_type,
                        row.get(2).and_then(query_value_to_u32),
                        row.get(6).and_then(query_value_to_u32),
                    ),
                    precision,
                    scale,
                })
            })
            .collect()
    }

    fn rowtype(
        &self,
        schema: &str,
        table_name: &str,
        original_name: &str,
    ) -> PyResult<DbObjectTypeImpl> {
        let attrs = self.rowtype_attrs(schema, table_name)?;
        Ok(DbObjectTypeImpl::new(
            schema.to_ascii_uppercase(),
            None,
            original_name.to_ascii_uppercase(),
            "OBJECT",
            attrs,
            None,
            0,
            false,
        ))
    }

    fn object_type_collection_metadata(
        &self,
        schema: &str,
        type_name: &str,
    ) -> PyResult<(Option<DbObjectAttrImpl>, u32, bool)> {
        let Some(row) = self.query_first_row_with_binds(
            "select elem_type_owner, elem_type_name, length, precision, scale, upper_bound \
             from all_coll_types \
             where owner = :1 and type_name = :2",
            &[
                BindValue::Text(schema.to_ascii_uppercase()),
                BindValue::Text(type_name.to_ascii_uppercase()),
            ],
        )?
        else {
            return Ok((None, 0, false));
        };
        let elem_type_owner = row
            .first()
            .and_then(query_value_to_string)
            .unwrap_or_else(|| schema.to_ascii_uppercase());
        let elem_type_name = row
            .get(1)
            .and_then(query_value_to_string)
            .unwrap_or_else(|| "VARCHAR2".to_string());
        let dbtype_name = public_dbtype_name_from_oracle_type_name(&elem_type_name);
        let (precision, scale) = dbobject_attr_precision_scale(
            &elem_type_name,
            row.get(3).and_then(query_value_to_i8),
            row.get(4).and_then(query_value_to_i8),
        );
        let element_metadata = DbObjectAttrImpl {
            name: String::new(),
            dbtype_name: dbtype_name.to_string(),
            objtype: if dbtype_name == "DB_TYPE_OBJECT" {
                Some(self.object_type_shallow(&elem_type_owner, &elem_type_name)?)
            } else {
                None
            },
            max_size: dbobject_attr_max_size(
                &elem_type_name,
                row.get(2).and_then(query_value_to_u32),
            ),
            precision,
            scale,
        };
        let max_num_elements = row.get(5).and_then(query_value_to_u32).unwrap_or(0);
        Ok((Some(element_metadata), max_num_elements, false))
    }

    fn plsql_type_collection_metadata(
        &self,
        schema: &str,
        package_name: &str,
        type_name: &str,
    ) -> PyResult<(Option<DbObjectAttrImpl>, u32, bool)> {
        let Some(row) = self.query_first_row_with_binds(
            "select elem_type_owner, elem_type_package, elem_type_name, length, precision, scale, upper_bound, coll_type, index_by \
             from all_plsql_coll_types \
             where owner = :1 and package_name = :2 and type_name = :3",
            &[
                BindValue::Text(schema.to_ascii_uppercase()),
                BindValue::Text(package_name.to_ascii_uppercase()),
                BindValue::Text(type_name.to_ascii_uppercase()),
            ],
        )?
        else {
            return Ok((None, 0, false));
        };
        let elem_type_owner = row
            .first()
            .and_then(query_value_to_string)
            .unwrap_or_else(|| schema.to_ascii_uppercase());
        let elem_type_package = row.get(1).and_then(query_value_to_string);
        let elem_type_name = row
            .get(2)
            .and_then(query_value_to_string)
            .unwrap_or_else(|| "VARCHAR2".to_string());
        let dbtype_name = public_dbtype_name_from_oracle_type_name(&elem_type_name);
        let (precision, scale) = dbobject_attr_precision_scale(
            &elem_type_name,
            row.get(4).and_then(query_value_to_i8),
            row.get(5).and_then(query_value_to_i8),
        );
        let objtype = if dbtype_name == "DB_TYPE_OBJECT" {
            if let Some(table_name) = elem_type_name.strip_suffix("%ROWTYPE") {
                // A collection of %ROWTYPE: the element type's attributes are the
                // referenced table's columns (reference resolves the rowtype's
                // attrs from the catalog, not a shallow ADT).
                Some(self.rowtype(&elem_type_owner, table_name, &elem_type_name)?)
            } else if let Some(elem_type_package) = elem_type_package {
                Some(self.plsql_type_shallow(
                    &elem_type_owner,
                    &elem_type_package,
                    &elem_type_name,
                )?)
            } else {
                Some(self.object_type_shallow(&elem_type_owner, &elem_type_name)?)
            }
        } else {
            None
        };
        let element_metadata = DbObjectAttrImpl {
            name: String::new(),
            dbtype_name: dbtype_name.to_string(),
            objtype,
            max_size: dbobject_attr_max_size(
                &elem_type_name,
                row.get(3).and_then(query_value_to_u32),
            ),
            precision,
            scale,
        };
        let max_num_elements = row.get(6).and_then(query_value_to_u32).unwrap_or(0);
        let coll_type = row
            .get(7)
            .and_then(query_value_to_string)
            .unwrap_or_default();
        let is_assoc_array = coll_type.eq_ignore_ascii_case("PL/SQL INDEX TABLE")
            || row.get(8).and_then(query_value_to_string).is_some();
        Ok((Some(element_metadata), max_num_elements, is_assoc_array))
    }

    fn type_shape_identity(
        &self,
        full_name: &str,
        oid_from_catalog: Option<Vec<u8>>,
    ) -> PyResult<(Option<Vec<u8>>, u32)> {
        let result = self.execute_with_binds(
            "declare \
                 t_instantiable varchar2(3); \
                 t_super_type_owner varchar2(128); \
                 t_super_type_name varchar2(128); \
                 t_subtype_ref_cursor sys_refcursor; \
             begin \
                 :1 := dbms_pickler.get_type_shape(:2, :3, :4, :5, \
                     t_instantiable, t_super_type_owner, t_super_type_name, \
                     :6, t_subtype_ref_cursor); \
             end;",
            &[
                BindValue::Output {
                    ora_type_num: ORA_TYPE_NUM_NUMBER,
                    csfrm: 0,
                    buffer_size: 22,
                },
                BindValue::Text(full_name.to_string()),
                BindValue::Output {
                    ora_type_num: ORA_TYPE_NUM_RAW,
                    csfrm: 0,
                    buffer_size: 64,
                },
                BindValue::Output {
                    ora_type_num: ORA_TYPE_NUM_NUMBER,
                    csfrm: 0,
                    buffer_size: 22,
                },
                BindValue::Output {
                    ora_type_num: ORA_TYPE_NUM_RAW,
                    csfrm: 0,
                    buffer_size: 32767,
                },
                BindValue::Output {
                    ora_type_num: ORA_TYPE_NUM_CURSOR,
                    csfrm: 0,
                    buffer_size: 4,
                },
            ],
        )?;
        let oid = result
            .out_binds()
            .values()
            .iter()
            .find_map(|(index, value)| match (index, value) {
                (2, Some(QueryValue::Raw(bytes))) => Some(bytes.clone()),
                _ => None,
            })
            .or(oid_from_catalog);
        let version = result
            .out_binds()
            .values()
            .iter()
            .find_map(|(index, value)| {
                (*index == 3)
                    .then(|| {
                        query_value_to_i64(value)
                            .ok()
                            .and_then(|value| u32::try_from(value).ok())
                    })
                    .flatten()
            })
            .unwrap_or(0);
        Ok((oid, version))
    }

    fn object_type_identity(
        &self,
        schema: &str,
        type_name: &str,
    ) -> PyResult<(Option<Vec<u8>>, u32)> {
        let schema = schema.to_ascii_uppercase();
        let type_name = type_name.to_ascii_uppercase();
        let oid_from_catalog = self
            .query_first_row_with_binds(
                "select type_oid from all_types where owner = :1 and type_name = :2",
                &[
                    BindValue::Text(schema.clone()),
                    BindValue::Text(type_name.clone()),
                ],
            )?
            .and_then(|row| match row.first() {
                Some(Some(QueryValue::Raw(bytes))) => Some(bytes.clone()),
                _ => None,
            });
        self.type_shape_identity(&format!("{schema}.{type_name}"), oid_from_catalog)
    }

    fn plsql_type_identity(
        &self,
        schema: &str,
        package_name: &str,
        type_name: &str,
    ) -> PyResult<(Option<Vec<u8>>, u32)> {
        let schema = schema.to_ascii_uppercase();
        let package_name = package_name.to_ascii_uppercase();
        let type_name = type_name.to_ascii_uppercase();
        let oid_from_catalog = self
            .query_first_row_with_binds(
                "select type_oid from all_plsql_types \
                 where owner = :1 and package_name = :2 and type_name = :3",
                &[
                    BindValue::Text(schema.clone()),
                    BindValue::Text(package_name.clone()),
                    BindValue::Text(type_name.clone()),
                ],
            )?
            .and_then(|row| match row.first() {
                Some(Some(QueryValue::Raw(bytes))) => Some(bytes.clone()),
                _ => None,
            });
        self.type_shape_identity(
            &format!("{schema}.{package_name}.{type_name}"),
            oid_from_catalog,
        )
    }

    fn object_type_shallow(&self, schema: &str, type_name: &str) -> PyResult<DbObjectTypeImpl> {
        let typecode = self
            .query_first_row_with_binds(
                "select typecode from all_types where owner = :1 and type_name = :2",
                &[
                    BindValue::Text(schema.to_ascii_uppercase()),
                    BindValue::Text(type_name.to_ascii_uppercase()),
                ],
            )?
            .and_then(|row| row.first().and_then(query_value_to_string))
            .unwrap_or_else(|| "OBJECT".to_string());
        let (element_metadata, max_num_elements, is_assoc_array) =
            self.object_type_collection_metadata(schema, type_name)?;
        let attrs = if element_metadata.is_some() {
            Vec::new()
        } else {
            self.object_type_attrs(schema, type_name)?
        };
        let (oid, version) = self.object_type_identity(schema, type_name)?;
        Ok(DbObjectTypeImpl::new(
            schema.to_ascii_uppercase(),
            None,
            type_name.to_ascii_uppercase(),
            &typecode,
            attrs,
            element_metadata,
            max_num_elements,
            is_assoc_array,
        )
        .with_type_identity(oid, version))
    }

    fn plsql_type_shallow(
        &self,
        schema: &str,
        package_name: &str,
        type_name: &str,
    ) -> PyResult<DbObjectTypeImpl> {
        let typecode = self
            .query_first_row_with_binds(
                "select typecode from all_plsql_types \
                 where owner = :1 and package_name = :2 and type_name = :3",
                &[
                    BindValue::Text(schema.to_ascii_uppercase()),
                    BindValue::Text(package_name.to_ascii_uppercase()),
                    BindValue::Text(type_name.to_ascii_uppercase()),
                ],
            )?
            .and_then(|row| row.first().and_then(query_value_to_string))
            .unwrap_or_else(|| "OBJECT".to_string());
        let (element_metadata, max_num_elements, is_assoc_array) =
            self.plsql_type_collection_metadata(schema, package_name, type_name)?;
        let attrs = if element_metadata.is_some() {
            Vec::new()
        } else {
            self.plsql_type_attrs(schema, package_name, type_name)?
        };
        let (oid, version) = self.plsql_type_identity(schema, package_name, type_name)?;
        Ok(DbObjectTypeImpl::new(
            schema.to_ascii_uppercase(),
            Some(package_name.to_ascii_uppercase()),
            type_name.to_ascii_uppercase(),
            &typecode,
            attrs,
            element_metadata,
            max_num_elements,
            is_assoc_array,
        )
        .with_type_identity(oid, version))
    }

    fn plsql_type(
        &self,
        schema: &str,
        package_name: &str,
        type_name: &str,
        original_name: &str,
    ) -> PyResult<DbObjectTypeImpl> {
        let Some(row) = self.query_first_row_with_binds(
            "select owner, package_name, type_name, typecode \
             from all_plsql_types \
             where owner = :1 and package_name = :2 and type_name = :3",
            &[
                BindValue::Text(schema.to_ascii_uppercase()),
                BindValue::Text(package_name.to_ascii_uppercase()),
                BindValue::Text(type_name.to_ascii_uppercase()),
            ],
        )?
        else {
            return Err(raise_invalid_object_type_name(original_name));
        };
        let schema = row
            .first()
            .and_then(query_value_to_string)
            .unwrap_or_else(|| schema.to_ascii_uppercase());
        let package_name = row
            .get(1)
            .and_then(query_value_to_string)
            .unwrap_or_else(|| package_name.to_ascii_uppercase());
        let type_name = row
            .get(2)
            .and_then(query_value_to_string)
            .unwrap_or_else(|| type_name.to_ascii_uppercase());
        let typecode = row
            .get(3)
            .and_then(query_value_to_string)
            .unwrap_or_else(|| "OBJECT".to_string());
        let (element_metadata, max_num_elements, is_assoc_array) =
            self.plsql_type_collection_metadata(&schema, &package_name, &type_name)?;
        let attrs = if element_metadata.is_some() {
            Vec::new()
        } else {
            self.plsql_type_attrs(&schema, &package_name, &type_name)?
        };
        let (oid, version) = self.plsql_type_identity(&schema, &package_name, &type_name)?;
        Ok(DbObjectTypeImpl::new(
            schema.to_ascii_uppercase(),
            Some(package_name.to_ascii_uppercase()),
            type_name.to_ascii_uppercase(),
            &typecode,
            attrs,
            element_metadata,
            max_num_elements,
            is_assoc_array,
        )
        .with_type_identity(oid, version))
    }

    pub(crate) fn call_timeout(&self) -> PyResult<Option<u32>> {
        let call_timeout = self.state.lock().map_err(runtime_error)?.call_timeout;
        Ok((call_timeout > 0).then_some(call_timeout))
    }

    pub(crate) fn take_connection_for_close(&self) -> PyResult<Option<RustConnection>> {
        *self.cancel_handle.lock().map_err(runtime_error)? = None;
        Ok(self.connection.lock().map_err(runtime_error)?.take())
    }

    /// Construct a connection implementation owned by a pool. Unlike
    /// [`ThinConnImpl::new`], this does not consume the global
    /// next-connect-args queue (which belongs to standalone connects); the
    /// pool supplies its captured password explicitly.
    pub(crate) fn new_for_pool(
        dsn: &str,
        params_impl: &Bound<'_, PyAny>,
        password: Option<String>,
        pool_conn_id: u64,
    ) -> PyResult<Self> {
        let username = get_string_attr(params_impl, "user")?;
        let stmt_cache_size = get_optional_u32_attr(params_impl, "stmtcachesize")?.unwrap_or(20);
        let edition = get_optional_string_attr(params_impl, "edition")?;
        Ok(Self {
            connection: Arc::new(Mutex::new(None)),
            cancel_handle: Arc::new(Mutex::new(None)),
            cancel_requested: Arc::new(AtomicBool::new(false)),
            state: Arc::new(Mutex::new(ThinConnState::new(
                stmt_cache_size,
                edition,
                false,
            ))),
            dsn: normalize_connect_string(dsn.to_string()),
            username,
            proxy_user: get_optional_string_attr(params_impl, "proxy_user")?,
            server_version: (0, 0, 0, 0, 0),
            autocommit: false,
            autocommit_state: Arc::new(Mutex::new(false)),
            tag: None,
            warning: None,
            inputtypehandler: None,
            outputtypehandler: None,
            invoke_session_callback: false,
            thin: true,
            connect_password: password,
            new_password: None,
            pool_conn_id: Some(pool_conn_id),
            connect_options: Arc::new(Mutex::new(None)),
        })
    }
}

#[pymethods]
impl ThinConnImpl {
    #[new]
    pub(crate) fn new(dsn: &Bound<'_, PyAny>, params_impl: &Bound<'_, PyAny>) -> PyResult<Self> {
        // dsn is None when no connect string could be resolved (bequeath);
        // prepare_connect raises DPY-3001 like the reference
        // (impl/thin/connection.pyx:450-454).
        let dsn: String = if dsn.is_none() {
            String::new()
        } else {
            dsn.extract()?
        };
        let invalid_connect_string = is_user_without_password_dsn(&dsn);
        let dsn = normalize_connect_string(dsn);
        let username = get_string_attr(params_impl, "user")?;
        let stmt_cache_size = get_optional_u32_attr(params_impl, "stmtcachesize")?.unwrap_or(20);
        let edition = get_optional_string_attr(params_impl, "edition")?;
        let connect_args = consume_next_connect_args()?;
        Ok(Self {
            connection: Arc::new(Mutex::new(None)),
            cancel_handle: Arc::new(Mutex::new(None)),
            cancel_requested: Arc::new(AtomicBool::new(false)),
            state: Arc::new(Mutex::new(ThinConnState::new(
                stmt_cache_size,
                edition,
                invalid_connect_string || connect_args.invalid_user_dsn,
            ))),
            dsn,
            username,
            proxy_user: get_optional_string_attr(params_impl, "proxy_user")?,
            server_version: (0, 0, 0, 0, 0),
            autocommit: false,
            autocommit_state: Arc::new(Mutex::new(false)),
            tag: None,
            warning: None,
            inputtypehandler: None,
            outputtypehandler: None,
            invoke_session_callback: false,
            thin: true,
            connect_password: connect_args.password,
            new_password: connect_args.new_password,
            pool_conn_id: None,
            connect_options: Arc::new(Mutex::new(None)),
        })
    }

    #[getter]
    fn dsn(&self) -> &str {
        &self.dsn
    }

    #[getter]
    fn username(&self) -> &str {
        &self.username
    }

    #[getter]
    fn proxy_user(&self) -> Option<&str> {
        self.proxy_user.as_deref()
    }

    #[getter]
    fn thin(&self) -> bool {
        self.thin
    }

    #[getter]
    fn server_version(&self) -> (u8, u8, u8, u8, u8) {
        self.server_version
    }

    #[getter]
    fn warning(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.warning.as_ref().map(|value| value.clone_ref(py))
    }

    #[getter]
    fn autocommit(&self) -> bool {
        self.autocommit
    }

    #[setter]
    pub(crate) fn set_autocommit(&mut self, value: bool) -> PyResult<()> {
        self.autocommit = value;
        *self.autocommit_state.lock().map_err(runtime_error)? = value;
        Ok(())
    }

    #[getter]
    fn inputtypehandler(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.inputtypehandler
            .as_ref()
            .map(|value| value.clone_ref(py))
    }

    #[setter]
    fn set_inputtypehandler(&mut self, value: Option<Py<PyAny>>) {
        self.inputtypehandler = value;
    }

    #[getter]
    fn outputtypehandler(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.outputtypehandler
            .as_ref()
            .map(|value| value.clone_ref(py))
    }

    #[setter]
    fn set_outputtypehandler(&mut self, value: Option<Py<PyAny>>) {
        self.outputtypehandler = value;
    }

    #[getter]
    fn tag(&self) -> Option<&str> {
        self.tag.as_deref()
    }

    #[setter]
    fn set_tag(&mut self, value: Option<String>) {
        self.tag = value;
    }

    #[getter]
    fn invoke_session_callback(&self) -> bool {
        self.invoke_session_callback
    }

    #[setter]
    fn set_invoke_session_callback(&mut self, value: bool) {
        self.invoke_session_callback = value;
    }

    fn connect(&mut self, params_impl: &Bound<'_, PyAny>) -> PyResult<()> {
        let prepared = self.prepare_connect(params_impl)?;
        // retain the connect options so a CQN subscription can spawn the emon
        // background connection (clone + (SERVER=emon))
        *self.connect_options.lock().map_err(runtime_error)? = Some(prepared.options.clone());
        let connection = BlockingConnection::connect(prepared.options).map_err(runtime_error)?;
        let cancel_handle = connection.cancel_handle().map_err(runtime_error)?;
        self.server_version = connection.server_version_tuple().unwrap_or_default();
        *self.cancel_handle.lock().map_err(runtime_error)? = Some(cancel_handle);
        *self.connection.lock().map_err(runtime_error)? = Some(connection);
        if let Some(new_password) = &prepared.new_password {
            self.change_password(&prepared.password, new_password)?;
        }
        if let Some(edition) = prepared.edition {
            let identifier = sql_identifier(&edition)?;
            self.execute_statement(&format!("alter session set edition = {identifier}"))?;
            let mut state = self.state.lock().map_err(runtime_error)?;
            state.edition = Some(edition);
            state.edition_probe_started = true;
        }
        // Reference impl/thin/connection.pyx sets this flag at the end of
        // every connect; the Python layer only consults it for pooled
        // connections.
        self.invoke_session_callback = true;
        Ok(())
    }

    #[pyo3(signature = (in_del=None))]
    fn close(&self, in_del: Option<bool>) -> PyResult<()> {
        let _ = in_del;
        let Some(connection) = self.take_connection_for_close()? else {
            return Ok(());
        };
        close_result_to_py(close_connection_result(connection))
    }

    fn ping(&self) -> PyResult<()> {
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::ping(connection).map_err(runtime_error)
    }

    fn commit(&self) -> PyResult<()> {
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::commit(connection).map_err(runtime_error)?;
        Ok(())
    }

    /// Loads data into a table via the Direct Path Load interface (reference
    /// thin/connection.pyx:589). `data` is a list of row sequences or an object
    /// implementing the Arrow PyCapsule stream interface (pyarrow.Table /
    /// pandas.DataFrame). A successful load commits server-side (FINISH op).
    fn direct_path_load(
        &self,
        schema_name: &str,
        table_name: &str,
        column_names: Vec<String>,
        data: &Bound<'_, PyAny>,
        batch_size: u32,
    ) -> PyResult<()> {
        if batch_size == 0 {
            return Err(PyTypeError::new_err(
                "batch_size must be a positive integer",
            ));
        }
        // Materialize the data into Python row tuples up front (Arrow/pandas are
        // consumed into native values), and validate the row widths *before*
        // PREPARE so a column-count mismatch (DPY-4009) never leaves a half-open
        // direct-path cursor behind.
        let py_rows = direct_path_py_rows(data)?;
        Python::attach(|py| verify_direct_path_widths(py, &py_rows, column_names.len()))?;
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        // PREPARE first so the per-column Oracle types are known; values are then
        // converted against that metadata (e.g. float -> BINARY_FLOAT vs NUMBER).
        let prepare = BlockingConnection::direct_path_prepare(
            connection,
            schema_name,
            table_name,
            &column_names,
        )
        .map_err(direct_path_error_to_py)?;
        let rows = Python::attach(|py| {
            direct_path_rows_from_py(py, &py_rows, &prepare.column_metadata, column_names.len())
        })?;
        BlockingConnection::direct_path_load_prepared(connection, &prepare, &rows, batch_size)
            .map_err(direct_path_error_to_py)?;
        Ok(())
    }

    fn rollback(&self) -> PyResult<()> {
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::rollback(connection).map_err(runtime_error)?;
        Ok(())
    }

    /// Begins a sessionless transaction (reference connection.py
    /// `begin_sessionless_transaction` -> `_impl.begin_sessionless_transaction`).
    /// The Python layer has already normalized `transaction_id` (bytes) and
    /// validated `timeout`.
    fn begin_sessionless_transaction(
        &self,
        transaction_id: &[u8],
        timeout: u32,
        defer_round_trip: bool,
    ) -> PyResult<()> {
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::begin_sessionless_transaction(
            connection,
            transaction_id,
            timeout,
            defer_round_trip,
        )
        .map_err(runtime_error)?;
        Ok(())
    }

    /// Resumes an existing sessionless transaction (reference
    /// `resume_sessionless_transaction`).
    fn resume_sessionless_transaction(
        &self,
        transaction_id: &[u8],
        timeout: u32,
        defer_round_trip: bool,
    ) -> PyResult<()> {
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::resume_sessionless_transaction(
            connection,
            transaction_id,
            timeout,
            defer_round_trip,
        )
        .map_err(runtime_error)?;
        Ok(())
    }

    /// Suspends the active sessionless transaction (reference
    /// `suspend_sessionless_transaction`).
    fn suspend_sessionless_transaction(&self) -> PyResult<()> {
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::suspend_sessionless_transaction(connection).map_err(runtime_error)
    }

    /// Begin an XA global transaction (reference connection.py `tpc_begin` ->
    /// `_impl.tpc_begin(xid, flags, timeout)`). The Python wrapper has validated
    /// the flags (DPY-2050) and the xid type before calling.
    #[pyo3(signature = (xid, flags, timeout))]
    fn tpc_begin(&self, xid: &Bound<'_, PyAny>, flags: u32, timeout: u32) -> PyResult<()> {
        let (format_id, gtid, bqual) = extract_xid(xid)?;
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::tpc_begin(connection, format_id, &gtid, &bqual, flags, timeout)
            .map_err(runtime_error)
    }

    /// End (detach) an XA global transaction branch (reference `tpc_end` ->
    /// `_impl.tpc_end(xid, flags)`). `xid` is `None` to detach the implicit
    /// current transaction.
    #[pyo3(signature = (xid, flags))]
    fn tpc_end(&self, xid: &Bound<'_, PyAny>, flags: u32) -> PyResult<()> {
        let xid = extract_optional_xid(xid)?;
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::tpc_end(connection, xid_as_refs(&xid), flags).map_err(runtime_error)
    }

    /// Prepare an XA global transaction for commit (reference `tpc_prepare` ->
    /// `_impl.tpc_prepare(xid)`). Returns `True` when a commit is needed.
    #[pyo3(signature = (xid))]
    fn tpc_prepare(&self, xid: &Bound<'_, PyAny>) -> PyResult<bool> {
        let xid = extract_optional_xid(xid)?;
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::tpc_prepare(connection, xid_as_refs(&xid)).map_err(runtime_error)
    }

    /// Commit an XA global transaction (reference `tpc_commit` ->
    /// `_impl.tpc_commit(xid, one_phase)`).
    #[pyo3(signature = (xid, one_phase))]
    fn tpc_commit(&self, xid: &Bound<'_, PyAny>, one_phase: bool) -> PyResult<()> {
        let xid = extract_optional_xid(xid)?;
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::tpc_commit(connection, xid_as_refs(&xid), one_phase)
            .map_err(runtime_error)
    }

    /// Roll back an XA global transaction (reference `tpc_rollback` ->
    /// `_impl.tpc_rollback(xid)`).
    #[pyo3(signature = (xid))]
    fn tpc_rollback(&self, xid: &Bound<'_, PyAny>) -> PyResult<()> {
        let xid = extract_optional_xid(xid)?;
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::tpc_rollback(connection, xid_as_refs(&xid)).map_err(runtime_error)
    }

    /// Forget an XA global transaction. Thin mode does not support this; the
    /// reference base impl raises DPY-3001 (NotSupportedError) and sends no
    /// packet.
    #[pyo3(signature = (xid))]
    fn tpc_forget(&self, xid: &Bound<'_, PyAny>) -> PyResult<()> {
        // Validate the xid type/shape so the error path mirrors the reference
        // (the public wrapper's `_verify_xid` already ran, but a bad value
        // would still be a TypeError before DPY-3001 in the reference).
        let _ = extract_xid(xid)?;
        Err(raise_not_supported(
            "forgetting a TPC (two-phase commit) transaction",
        ))
    }

    fn change_password(&self, old_password: &str, new_password: &str) -> PyResult<()> {
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::change_password(connection, old_password, new_password)
            .map_err(runtime_error)?;
        drop(guard);
        set_password_override_for_user(&self.username, new_password)
    }

    /// Encodes a Python value to OSON bytes (reference
    /// thin/connection.pyx `encode_oson`). Long field names (>255 bytes) are
    /// permitted on Oracle 23ai+ (OSON version 3); the encoder still emits
    /// version 1 when no long name is present.
    fn encode_oson<'py>(
        &self,
        py: Python<'py>,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<Py<PyBytes>> {
        let oson = py_value_to_oson(value)?;
        let supports_long_fnames = self.server_version.0 >= 23;
        let image = oracledb::protocol::oson::encode_oson(&oson, supports_long_fnames)
            .map_err(|err| oson_error_to_pyerr(&err))?;
        Ok(PyBytes::new(py, &image).unbind())
    }

    /// Decodes OSON bytes to a Python value (reference
    /// thin/connection.pyx `decode_oson`). Raises DPY-5004 when the input is not
    /// OSON and DPY-5006 when it is structurally invalid.
    fn decode_oson(&self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let bytes = data.extract::<Vec<u8>>()?;
        let value = oracledb::protocol::oson::decode_oson(&bytes)
            .map_err(|err| oson_error_to_pyerr(&err))?;
        oson_value_to_py(py, &value)
    }

    pub(crate) fn get_is_healthy(&self) -> PyResult<bool> {
        Ok(self.connection.lock().map_err(runtime_error)?.is_some())
    }

    /// Parity with the reference base connection impl
    /// (impl/base/connection.pyx:360-361): sync connections never pipeline.
    fn supports_pipelining(&self) -> bool {
        false
    }

    pub(crate) fn get_sdu(&self) -> PyResult<u32> {
        let guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        Ok(u32::try_from(connection.sdu()).unwrap_or(u32::MAX))
    }

    pub(crate) fn get_type(
        &self,
        _conn: &Bound<'_, PyAny>,
        name: &str,
    ) -> PyResult<DbObjectTypeImpl> {
        let parts: Vec<&str> = name
            .split('.')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .collect();
        let requested_type_name = parts.last().copied().unwrap_or(name).to_ascii_uppercase();
        let requested_owner = (parts.len() == 2).then(|| parts[0].to_ascii_uppercase());
        if let Some(table_name) = requested_type_name.strip_suffix("%ROWTYPE") {
            let schema = requested_owner
                .clone()
                .unwrap_or_else(|| self.username.to_ascii_uppercase());
            return self.rowtype(&schema, table_name, name);
        }
        let mut sql = String::from(
            "select owner, type_name, typecode \
             from all_types \
             where type_name = :1",
        );
        let mut binds = vec![BindValue::Text(requested_type_name.clone())];
        if let Some(owner) = requested_owner {
            sql.push_str(" and owner = :2");
            binds.push(BindValue::Text(owner));
        } else {
            sql.push_str(" and owner = sys_context('USERENV', 'CURRENT_SCHEMA')");
        }
        sql.push_str(" order by owner");
        let Some(row) = self.query_first_row_with_binds(&sql, &binds)? else {
            return match parts.as_slice() {
                [package_name, type_name] => self.plsql_type(
                    &self.username.to_ascii_uppercase(),
                    package_name,
                    type_name,
                    name,
                ),
                [schema, package_name, type_name] => {
                    self.plsql_type(schema, package_name, type_name, name)
                }
                _ => Err(raise_invalid_object_type_name(name)),
            };
        };
        let schema = row
            .first()
            .and_then(query_value_to_string)
            .unwrap_or_else(|| self.username.to_ascii_uppercase());
        let type_name = row
            .get(1)
            .and_then(query_value_to_string)
            .unwrap_or(requested_type_name);
        let typecode = row
            .get(2)
            .and_then(query_value_to_string)
            .unwrap_or_else(|| "OBJECT".to_string());
        let attrs = self.object_type_attrs(&schema, &type_name)?;
        let (element_metadata, max_num_elements, is_assoc_array) =
            self.object_type_collection_metadata(&schema, &type_name)?;
        let (oid, version) = self.object_type_identity(&schema, &type_name)?;
        Ok(DbObjectTypeImpl::new(
            schema.to_ascii_uppercase(),
            None,
            type_name.to_ascii_uppercase(),
            &typecode,
            attrs,
            element_metadata,
            max_num_elements,
            is_assoc_array,
        )
        .with_type_identity(oid, version))
    }

    pub(crate) fn get_call_timeout(&self) -> PyResult<u32> {
        Ok(self.state.lock().map_err(runtime_error)?.call_timeout)
    }

    pub(crate) fn set_call_timeout(&self, value: u32) -> PyResult<()> {
        self.state.lock().map_err(runtime_error)?.call_timeout = value;
        Ok(())
    }

    pub(crate) fn clear_end_user_security_context(&self) -> PyResult<()> {
        Ok(())
    }

    pub(crate) fn set_end_user_security_context(
        &self,
        _context: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        if !self.dsn.to_ascii_lowercase().contains("tcps") {
            return Err(raise_oracledb_driver_error(
                "ERR_END_USER_SECURITY_CONTEXT_REQUIRES_TCPS",
            ));
        }
        Err(not_implemented(
            "ThinConnImpl.set_end_user_security_context",
        ))
    }

    pub(crate) fn cancel(&self) -> PyResult<()> {
        self.cancel_requested.store(true, Ordering::SeqCst);
        if let Some(cancel_handle) = self.cancel_handle.lock().map_err(runtime_error)?.as_mut() {
            cancel_handle.cancel_blocking().map_err(runtime_error)?;
        }
        Ok(())
    }

    pub(crate) fn get_ltxid<'py>(&self, py: Python<'py>) -> Py<PyBytes> {
        PyBytes::new(py, &[]).unbind()
    }

    pub(crate) fn get_current_schema(&self) -> PyResult<Option<String>> {
        Ok(self
            .state
            .lock()
            .map_err(runtime_error)?
            .current_schema
            .clone())
    }

    pub(crate) fn set_current_schema(&self, value: Option<String>) -> PyResult<()> {
        if let Some(value) = value {
            sql_identifier(&value)?;
            let mut state = self.state.lock().map_err(runtime_error)?;
            state.current_schema = Some(value);
            state.current_schema_modified = true;
        } else {
            let mut state = self.state.lock().map_err(runtime_error)?;
            state.current_schema = None;
            state.current_schema_modified = false;
        }
        Ok(())
    }

    pub(crate) fn get_edition(&self) -> PyResult<Option<String>> {
        {
            let mut state = self.state.lock().map_err(runtime_error)?;
            if state.edition.is_some() {
                return Ok(state.edition.clone());
            }
            if !state.edition_probe_started {
                state.edition_probe_started = true;
                return Ok(None);
            }
        }
        self.query_first_text("select sys_context('USERENV', 'CURRENT_EDITION_NAME') from dual")
    }

    pub(crate) fn get_external_name(&self) -> PyResult<Option<String>> {
        Ok(self
            .state
            .lock()
            .map_err(runtime_error)?
            .external_name
            .clone())
    }

    pub(crate) fn set_external_name(&self, value: Option<String>) -> PyResult<()> {
        self.state.lock().map_err(runtime_error)?.external_name = value;
        Ok(())
    }

    pub(crate) fn get_internal_name(&self) -> PyResult<Option<String>> {
        Ok(self
            .state
            .lock()
            .map_err(runtime_error)?
            .internal_name
            .clone())
    }

    pub(crate) fn set_internal_name(&self, value: Option<String>) -> PyResult<()> {
        self.state.lock().map_err(runtime_error)?.internal_name = value;
        Ok(())
    }

    pub(crate) fn get_max_identifier_length(&self) -> Option<u8> {
        Some(128)
    }

    pub(crate) fn get_instance_name(&self) -> PyResult<String> {
        Ok(self
            .query_first_text("select sys_context('userenv', 'instance_name') from dual")?
            .unwrap_or_default())
    }

    pub(crate) fn get_db_name(&self) -> PyResult<String> {
        Ok(self
            .query_first_text("select name from V$DATABASE")?
            .unwrap_or_default())
    }

    pub(crate) fn get_max_open_cursors(&self) -> PyResult<i64> {
        self.query_first_i64("select value from V$PARAMETER where name='open_cursors'")
    }

    pub(crate) fn get_service_name(&self) -> PyResult<String> {
        Ok(self
            .query_first_text("select sys_context('userenv', 'service_name') from dual")?
            .unwrap_or_default())
    }

    pub(crate) fn get_db_domain(&self) -> PyResult<Option<String>> {
        self.query_first_text("select value from V$PARAMETER where name='db_domain'")
    }

    pub(crate) fn get_stmt_cache_size(&self) -> PyResult<u32> {
        Ok(self.state.lock().map_err(runtime_error)?.stmt_cache_size)
    }

    pub(crate) fn set_stmt_cache_size(&self, value: u32) -> PyResult<()> {
        self.state.lock().map_err(runtime_error)?.stmt_cache_size = value;
        Ok(())
    }

    pub(crate) fn get_transaction_in_progress(&self) -> PyResult<bool> {
        // Read the wire-derived flag from the driver (reference protocol.pyx
        // `_txn_in_progress`, sampled from the end-of-call status of every round
        // trip). On a closed connection there is no transaction in progress.
        let guard = self.connection.lock().map_err(runtime_error)?;
        Ok(guard
            .as_ref()
            .map(|connection| connection.transaction_in_progress())
            .unwrap_or(false))
    }

    pub(crate) fn set_action(&self, value: Option<String>) -> PyResult<()> {
        self.execute_statement_with_binds(
            "begin dbms_application_info.set_action(:1); end;",
            &[bind_optional_text(value.as_deref())],
        )
    }

    pub(crate) fn set_client_identifier(&self, value: Option<String>) -> PyResult<()> {
        if let Some(value) = value {
            self.execute_statement_with_binds(
                "begin dbms_session.set_identifier(:1); end;",
                &[BindValue::Text(value)],
            )
        } else {
            self.execute_statement("begin dbms_session.clear_identifier; end;")
        }
    }

    pub(crate) fn set_client_info(&self, value: Option<String>) -> PyResult<()> {
        self.execute_statement_with_binds(
            "begin dbms_application_info.set_client_info(:1); end;",
            &[bind_optional_text(value.as_deref())],
        )
    }

    pub(crate) fn set_dbop(&self, value: Option<String>) -> PyResult<()> {
        if let Some((name, execution_id)) = self
            .state
            .lock()
            .map_err(runtime_error)?
            .dbop_operation
            .take()
        {
            self.execute_statement_with_binds(
                "begin dbms_sql_monitor.end_operation(:1, :2); end;",
                &[
                    BindValue::Text(name),
                    BindValue::Number(execution_id.to_string()),
                ],
            )?;
        }
        let Some(value) = value else {
            return Ok(());
        };
        let row = self
            .query_first_row_with_binds(
                "select dbms_sql_monitor.begin_operation(:1, null, 'Y') from dual",
                &[BindValue::Text(value.clone())],
            )?
            .ok_or_else(|| {
                PyRuntimeError::new_err("dbms_sql_monitor.begin_operation returned no row")
            })?;
        let execution_id = query_value_to_i64(row.first().unwrap_or(&None))?;
        self.state.lock().map_err(runtime_error)?.dbop_operation = Some((value, execution_id));
        Ok(())
    }

    pub(crate) fn set_module(&self, value: Option<String>) -> PyResult<()> {
        self.execute_statement_with_binds(
            "begin dbms_application_info.set_module(:1, null); end;",
            &[bind_optional_text(value.as_deref())],
        )
    }

    pub(crate) fn get_session_id(&self) -> PyResult<u32> {
        let guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        Ok(connection.session_id())
    }

    pub(crate) fn get_serial_num(&self) -> PyResult<u16> {
        let guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        Ok(connection.serial_num())
    }

    fn create_temp_lob_value(
        &self,
        lob_type: &Bound<'_, PyAny>,
        async_mode: bool,
    ) -> PyResult<ThinLob> {
        let (ora_type_num, csfrm) = match py_type_name(lob_type).as_str() {
            "DB_TYPE_BLOB" => (ORA_TYPE_NUM_BLOB, 0),
            "DB_TYPE_NCLOB" => (ORA_TYPE_NUM_CLOB, CS_FORM_NCHAR),
            _ => (ORA_TYPE_NUM_CLOB, CS_FORM_IMPLICIT),
        };
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        let result = BlockingConnection::create_temp_lob(connection, ora_type_num, csfrm)
            .map_err(runtime_error)?;
        Ok(ThinLob {
            data: None,
            locator: Arc::new(Mutex::new(Some(result.locator))),
            ora_type_num,
            csfrm,
            size: 0,
            chunk_size: 0,
            context: Some(ThinLobContext {
                connection: Arc::clone(&self.connection),
                state: Arc::clone(&self.state),
                async_mode,
            }),
            is_open: Arc::new(Mutex::new(false)),
            bfile_name: None,
        })
    }

    fn create_temp_lob_impl(
        &self,
        py: Python<'_>,
        lob_type: &Bound<'_, PyAny>,
    ) -> PyResult<Py<ThinLob>> {
        Py::new(py, self.create_temp_lob_value(lob_type, false)?)
    }

    /// Build a CQN subscription impl. Mirrors `connection.pyx:559
    /// create_subscr_impl`: a server-initiated subscription (client_initiated
    /// false) is not supported in thin mode.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        conn, callback, namespace, name, protocol, ip_address, port, timeout,
        operations, qos, grouping_class, grouping_value, grouping_type,
        client_initiated
    ))]
    fn create_subscr_impl(
        &self,
        py: Python<'_>,
        conn: Py<PyAny>,
        callback: Option<Py<PyAny>>,
        namespace: u32,
        name: Option<String>,
        protocol: u32,
        ip_address: Option<String>,
        port: u32,
        timeout: u32,
        operations: u32,
        qos: u32,
        grouping_class: u8,
        grouping_value: u32,
        grouping_type: u8,
        client_initiated: bool,
    ) -> PyResult<Py<ThinSubscrImpl>> {
        if !client_initiated {
            return Err(raise_not_supported("server initiated subscription"));
        }
        let impl_ = ThinSubscrImpl::new(
            conn,
            callback,
            namespace,
            name,
            protocol,
            ip_address,
            port,
            timeout,
            operations,
            qos,
            grouping_class,
            grouping_value,
            grouping_type,
        );
        Py::new(py, impl_)
    }

    fn create_queue_impl(&self) -> crate::aq::ThinQueueImpl {
        crate::aq::ThinQueueImpl::new()
    }

    /// Build a SODA database impl for `getSodaDatabase()`. The `_conn` argument
    /// is the public Connection object (the reference passes `self`); we hold
    /// the connection handle already, so it is unused.
    fn create_soda_database_impl(&self, _conn: &Bound<'_, PyAny>) -> crate::soda::ThinSodaDbImpl {
        self.build_soda_db_impl()
    }

    fn create_msg_props_impl(&self) -> crate::aq::ThinMsgPropsImpl {
        crate::aq::ThinMsgPropsImpl::new()
    }

    pub(crate) fn create_cursor_impl(
        &self,
        py: Python<'_>,
        scrollable: bool,
    ) -> PyResult<ThinCursorImpl> {
        let mut cursor_impl = ThinCursorImpl::new(
            Arc::clone(&self.connection),
            Arc::clone(&self.autocommit_state),
            Arc::clone(&self.cancel_requested),
            Arc::clone(&self.state),
            scrollable,
        );
        // base/connection.pyx:223-224 sources arraysize/prefetchrows from the
        // live oracledb.defaults singleton at cursor creation.
        let (arraysize, prefetchrows) = default_cursor_sizes(py)?;
        cursor_impl.arraysize = arraysize;
        cursor_impl.prefetchrows = prefetchrows;
        Ok(cursor_impl)
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "EndUserSecurityContextImpl")]
#[derive(Default)]
pub(crate) struct EndUserSecurityContextImpl {
    #[allow(dead_code)]
    payload: BTreeMap<String, String>,
    #[allow(dead_code)]
    encoded_len: usize,
}

#[pymethods]
impl EndUserSecurityContextImpl {
    #[staticmethod]
    fn create_end_user_security_context(
        end_user_token: &Bound<'_, PyAny>,
        end_user_name: &Bound<'_, PyAny>,
        key: &Bound<'_, PyAny>,
        database_access_token: &Bound<'_, PyAny>,
        data_roles: &Bound<'_, PyAny>,
        attributes: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let mut payload = BTreeMap::new();
        payload.insert("ver".to_string(), "1.0".to_string());
        if let Some(value) = extract_optional_string(end_user_token)? {
            payload.insert("end_user_token".to_string(), value);
        }
        if let Some(value) = extract_optional_string(end_user_name)? {
            payload.insert("end_user_name".to_string(), value);
        }
        if let Some(value) = extract_optional_string(key)? {
            payload.insert("end_user_contextid".to_string(), value);
        }
        if let Some(value) = extract_optional_string(database_access_token)? {
            payload.insert("database_access_token".to_string(), value);
        }
        if !data_roles.is_none() {
            payload.insert("data_roles".to_string(), data_roles.str()?.to_string());
        }
        if !attributes.is_none() {
            payload.insert("attributes".to_string(), attributes.str()?.to_string());
        }
        let encoded_len = payload
            .iter()
            .map(|(key, value)| key.len() + value.len() + 8)
            .sum::<usize>();
        if encoded_len > 65_535 {
            return Err(raise_oracledb_driver_error(
                "ERR_INVALID_END_USER_SECURITY_CONTEXT_LENGTH",
            ));
        }
        Ok(Self {
            payload,
            encoded_len,
        })
    }
}
