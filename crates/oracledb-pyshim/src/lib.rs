#![forbid(unsafe_code)]

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

mod errors;
mod async_bridge;
mod hooks;
mod pyutil;
mod binds;
mod convert;
mod lob;
mod var;
mod typehandler;
mod dbobject;
mod metadata;

pub(crate) use errors::*;
pub(crate) use async_bridge::*;
pub(crate) use hooks::*;
pub(crate) use pyutil::*;
pub(crate) use binds::*;
pub(crate) use convert::*;
pub(crate) use lob::*;
pub(crate) use var::*;
pub(crate) use typehandler::*;
pub(crate) use dbobject::*;
pub(crate) use metadata::*;

#[derive(Debug)]
struct ThinConnState {
    current_schema: Option<String>,
    current_schema_modified: bool,
    edition: Option<String>,
    edition_probe_started: bool,
    external_name: Option<String>,
    internal_name: Option<String>,
    call_timeout: u32,
    stmt_cache_size: u32,
    transaction_in_progress: bool,
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
            transaction_in_progress: false,
            invalid_connect_string,
            dbop_operation: None,
        }
    }

    fn record_statement(&mut self, statement: &str, is_query: bool, committed: bool) {
        if let Some(schema) = parse_alter_session_value(statement, "current_schema") {
            self.current_schema = Some(schema);
            self.current_schema_modified = false;
            self.transaction_in_progress = false;
            return;
        }
        if let Some(edition) = parse_alter_session_value(statement, "edition") {
            self.edition = Some(edition.to_ascii_uppercase());
            self.edition_probe_started = true;
            self.transaction_in_progress = false;
            return;
        }
        if committed {
            self.transaction_in_progress = false;
            return;
        }
        if is_query {
            return;
        }
        match first_sql_keyword(statement).as_str() {
            "insert" | "update" | "delete" | "merge" => self.transaction_in_progress = true,
            "alter" | "commit" | "rollback" | "truncate" => self.transaction_in_progress = false,
            _ => {}
        }
    }
}

#[pyfunction]
fn init_thin_impl(_package: &Bound<'_, PyAny>) -> PyResult<()> {
    Ok(())
}

fn apply_pending_current_schema_from_state(
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
    let result = BlockingConnection::execute_query_with_timeout(
        connection,
        &format!("alter session set current_schema = {identifier}"),
        1,
        call_timeout,
    )
    .map_err(runtime_error);
    if result.is_err() {
        state.lock().map_err(runtime_error)?.current_schema_modified = true;
    }
    result.map(|_| ())
}

async fn apply_pending_current_schema_from_state_async(
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
        .execute_query_with_timeout(
            cx,
            &format!("alter session set current_schema = {identifier}"),
            1,
            call_timeout,
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
struct ThinConnImpl {
    connection: Arc<Mutex<Option<RustConnection>>>,
    cancel_handle: Arc<Mutex<Option<CancelHandle>>>,
    cancel_requested: Arc<AtomicBool>,
    state: Arc<Mutex<ThinConnState>>,
    dsn: String,
    username: String,
    proxy_user: Option<String>,
    server_version: (u8, u8, u8, u8, u8),
    autocommit: bool,
    autocommit_state: Arc<Mutex<bool>>,
    tag: Option<String>,
    warning: Option<Py<PyAny>>,
    inputtypehandler: Option<Py<PyAny>>,
    outputtypehandler: Option<Py<PyAny>>,
    invoke_session_callback: bool,
    thin: bool,
    connect_password: Option<String>,
    new_password: Option<String>,
}

struct PreparedConnect {
    options: ConnectOptions,
    password: String,
    new_password: Option<String>,
    edition: Option<String>,
}

impl ThinConnImpl {
    fn prepare_connect(&mut self, params_impl: &Bound<'_, PyAny>) -> PyResult<PreparedConnect> {
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
        .with_sdu(sdu);
        Ok(PreparedConnect {
            options,
            password,
            new_password: self.new_password.clone(),
            edition,
        })
    }

    fn apply_pending_current_schema(
        &self,
        connection: &mut RustConnection,
        call_timeout: Option<u32>,
    ) -> PyResult<()> {
        apply_pending_current_schema_from_state(&self.state, connection, call_timeout)
    }

    fn execute_with_binds(&self, sql: &str, binds: &[BindValue]) -> PyResult<QueryResult> {
        let call_timeout = self.call_timeout()?;
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        self.apply_pending_current_schema(connection, call_timeout)?;
        BlockingConnection::execute_query_with_binds_and_timeout(
            connection,
            sql,
            1,
            binds,
            call_timeout,
        )
        .map_err(runtime_error)
    }

    fn execute_statement(&self, sql: &str) -> PyResult<()> {
        let call_timeout = self.call_timeout()?;
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        self.apply_pending_current_schema(connection, call_timeout)?;
        BlockingConnection::execute_query_with_timeout(connection, sql, 1, call_timeout)
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
        let result =
            BlockingConnection::execute_query_with_timeout(connection, sql, 1, call_timeout)
                .map_err(runtime_error)?;
        Ok(result
            .rows
            .first()
            .and_then(|row| row.first())
            .cloned()
            .flatten())
    }

    fn query_first_row_with_binds(
        &self,
        sql: &str,
        binds: &[BindValue],
    ) -> PyResult<Option<Vec<Option<QueryValue>>>> {
        let result = self.execute_with_binds(sql, binds)?;
        Ok(result.rows.into_iter().next())
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
        let result = BlockingConnection::execute_query_with_binds_and_timeout(
            connection,
            sql,
            100,
            binds,
            call_timeout,
        )
        .map_err(runtime_error)?;
        Ok(result.rows)
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
            if let Some(elem_type_package) = elem_type_package {
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
            .out_values
            .iter()
            .find_map(|(index, value)| match (index, value) {
                (2, Some(QueryValue::Raw(bytes))) => Some(bytes.clone()),
                _ => None,
            })
            .or(oid_from_catalog);
        let version = result
            .out_values
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

    fn call_timeout(&self) -> PyResult<Option<u32>> {
        let call_timeout = self.state.lock().map_err(runtime_error)?.call_timeout;
        Ok((call_timeout > 0).then_some(call_timeout))
    }

    fn take_connection_for_close(&self) -> PyResult<Option<RustConnection>> {
        *self.cancel_handle.lock().map_err(runtime_error)? = None;
        Ok(self.connection.lock().map_err(runtime_error)?.take())
    }
}

#[pymethods]
impl ThinConnImpl {
    #[new]
    fn new(dsn: &Bound<'_, PyAny>, params_impl: &Bound<'_, PyAny>) -> PyResult<Self> {
        let dsn = if dsn.is_none() {
            std::env::var("PYO_TEST_CONNECT_STRING").unwrap_or_default()
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
    fn set_autocommit(&mut self, value: bool) -> PyResult<()> {
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
        let connection = BlockingConnection::connect(prepared.options).map_err(runtime_error)?;
        let cancel_handle = connection.cancel_handle().map_err(runtime_error)?;
        self.server_version = (0, 0, 0, 0, 0);
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
        self.state
            .lock()
            .map_err(runtime_error)?
            .transaction_in_progress = false;
        Ok(())
    }

    fn rollback(&self) -> PyResult<()> {
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::rollback(connection).map_err(runtime_error)?;
        self.state
            .lock()
            .map_err(runtime_error)?
            .transaction_in_progress = false;
        Ok(())
    }

    fn change_password(&self, old_password: &str, new_password: &str) -> PyResult<()> {
        if new_password.len() > 1024 {
            return Err(dpy_database_error(
                "ORA-00988",
                "missing or invalid password(s)",
            ));
        }
        let user = user_identifier(&self.username)?;
        let sql = format!(
            "alter user {user} identified by {} replace {}",
            quoted_oracle_string(new_password),
            quoted_oracle_string(old_password)
        );
        self.execute_statement(&sql)
            .and_then(|()| set_password_override_for_user(&self.username, new_password))
    }

    fn get_is_healthy(&self) -> PyResult<bool> {
        Ok(self.connection.lock().map_err(runtime_error)?.is_some())
    }

    fn get_sdu(&self) -> PyResult<u32> {
        let guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        Ok(u32::try_from(connection.sdu()).unwrap_or(u32::MAX))
    }

    fn get_type(&self, _conn: &Bound<'_, PyAny>, name: &str) -> PyResult<DbObjectTypeImpl> {
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

    fn get_call_timeout(&self) -> PyResult<u32> {
        Ok(self.state.lock().map_err(runtime_error)?.call_timeout)
    }

    fn set_call_timeout(&self, value: u32) -> PyResult<()> {
        self.state.lock().map_err(runtime_error)?.call_timeout = value;
        Ok(())
    }

    fn clear_end_user_security_context(&self) -> PyResult<()> {
        Ok(())
    }

    fn set_end_user_security_context(&self, _context: &Bound<'_, PyAny>) -> PyResult<()> {
        if !self.dsn.to_ascii_lowercase().contains("tcps") {
            return Err(raise_oracledb_driver_error(
                "ERR_END_USER_SECURITY_CONTEXT_REQUIRES_TCPS",
            ));
        }
        Err(not_implemented(
            "ThinConnImpl.set_end_user_security_context",
        ))
    }

    fn cancel(&self) -> PyResult<()> {
        self.cancel_requested.store(true, Ordering::SeqCst);
        if let Some(cancel_handle) = self.cancel_handle.lock().map_err(runtime_error)?.as_mut() {
            cancel_handle.cancel().map_err(runtime_error)?;
        }
        Ok(())
    }

    fn get_ltxid<'py>(&self, py: Python<'py>) -> Py<PyBytes> {
        PyBytes::new(py, &[]).unbind()
    }

    fn get_current_schema(&self) -> PyResult<Option<String>> {
        Ok(self
            .state
            .lock()
            .map_err(runtime_error)?
            .current_schema
            .clone())
    }

    fn set_current_schema(&self, value: Option<String>) -> PyResult<()> {
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

    fn get_edition(&self) -> PyResult<Option<String>> {
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

    fn get_external_name(&self) -> PyResult<Option<String>> {
        Ok(self
            .state
            .lock()
            .map_err(runtime_error)?
            .external_name
            .clone())
    }

    fn set_external_name(&self, value: Option<String>) -> PyResult<()> {
        self.state.lock().map_err(runtime_error)?.external_name = value;
        Ok(())
    }

    fn get_internal_name(&self) -> PyResult<Option<String>> {
        Ok(self
            .state
            .lock()
            .map_err(runtime_error)?
            .internal_name
            .clone())
    }

    fn set_internal_name(&self, value: Option<String>) -> PyResult<()> {
        self.state.lock().map_err(runtime_error)?.internal_name = value;
        Ok(())
    }

    fn get_max_identifier_length(&self) -> Option<u8> {
        Some(128)
    }

    fn get_instance_name(&self) -> PyResult<String> {
        Ok(self
            .query_first_text("select sys_context('userenv', 'instance_name') from dual")?
            .unwrap_or_default())
    }

    fn get_db_name(&self) -> PyResult<String> {
        Ok(self
            .query_first_text("select name from V$DATABASE")?
            .unwrap_or_default())
    }

    fn get_max_open_cursors(&self) -> PyResult<i64> {
        self.query_first_i64("select value from V$PARAMETER where name='open_cursors'")
    }

    fn get_service_name(&self) -> PyResult<String> {
        Ok(self
            .query_first_text("select sys_context('userenv', 'service_name') from dual")?
            .unwrap_or_default())
    }

    fn get_db_domain(&self) -> PyResult<Option<String>> {
        self.query_first_text("select value from V$PARAMETER where name='db_domain'")
    }

    fn get_stmt_cache_size(&self) -> PyResult<u32> {
        Ok(self.state.lock().map_err(runtime_error)?.stmt_cache_size)
    }

    fn set_stmt_cache_size(&self, value: u32) -> PyResult<()> {
        self.state.lock().map_err(runtime_error)?.stmt_cache_size = value;
        Ok(())
    }

    fn get_transaction_in_progress(&self) -> PyResult<bool> {
        Ok(self
            .state
            .lock()
            .map_err(runtime_error)?
            .transaction_in_progress)
    }

    fn set_action(&self, value: Option<String>) -> PyResult<()> {
        self.execute_statement_with_binds(
            "begin dbms_application_info.set_action(:1); end;",
            &[bind_optional_text(value.as_deref())],
        )
    }

    fn set_client_identifier(&self, value: Option<String>) -> PyResult<()> {
        if let Some(value) = value {
            self.execute_statement_with_binds(
                "begin dbms_session.set_identifier(:1); end;",
                &[BindValue::Text(value)],
            )
        } else {
            self.execute_statement("begin dbms_session.clear_identifier; end;")
        }
    }

    fn set_client_info(&self, value: Option<String>) -> PyResult<()> {
        self.execute_statement_with_binds(
            "begin dbms_application_info.set_client_info(:1); end;",
            &[bind_optional_text(value.as_deref())],
        )
    }

    fn set_dbop(&self, value: Option<String>) -> PyResult<()> {
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

    fn set_module(&self, value: Option<String>) -> PyResult<()> {
        self.execute_statement_with_binds(
            "begin dbms_application_info.set_module(:1, null); end;",
            &[bind_optional_text(value.as_deref())],
        )
    }

    fn get_session_id(&self) -> PyResult<u32> {
        let guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        Ok(connection.session_id())
    }

    fn get_serial_num(&self) -> PyResult<u16> {
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

    fn create_cursor_impl(&self, scrollable: bool) -> ThinCursorImpl {
        ThinCursorImpl::new(
            Arc::clone(&self.connection),
            Arc::clone(&self.autocommit_state),
            Arc::clone(&self.cancel_requested),
            Arc::clone(&self.state),
            scrollable,
        )
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "ExecutemanyManager")]
struct ExecutemanyManager {
    total_rows: u32,
    batch_size: u32,
    num_rows: u32,
    message_offset: u32,
}

impl ExecutemanyManager {
    fn new(total_rows: usize, batch_size: u32) -> PyResult<Self> {
        let total_rows = u32::try_from(total_rows).map_err(runtime_error)?;
        if batch_size == 0 {
            return Err(PyTypeError::new_err("batch_size must be greater than zero"));
        }
        Ok(Self {
            total_rows,
            batch_size,
            num_rows: total_rows.min(batch_size),
            message_offset: 0,
        })
    }
}

#[pymethods]
impl ExecutemanyManager {
    #[getter]
    fn num_rows(&self) -> u32 {
        self.num_rows
    }

    #[getter]
    fn message_offset(&self) -> u32 {
        self.message_offset
    }

    fn next_batch(&mut self) {
        self.message_offset = self.message_offset.saturating_add(self.num_rows);
        let remaining = self.total_rows.saturating_sub(self.message_offset);
        self.num_rows = remaining.min(self.batch_size);
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "ThinCursorImpl")]
struct ThinCursorImpl {
    connection: Arc<Mutex<Option<RustConnection>>>,
    autocommit: Arc<Mutex<bool>>,
    cancel_requested: Arc<AtomicBool>,
    state: Arc<Mutex<ThinConnState>>,
    statement: Option<String>,
    bind_values: Vec<BindValue>,
    bind_vars: Vec<Py<ThinVar>>,
    bind_names: Vec<String>,
    many_bind_rows: Vec<Vec<BindValue>>,
    columns: Vec<ColumnMetadata>,
    fetch_vars: Vec<Option<Py<ThinVar>>>,
    fetch_define_columns: Vec<ColumnMetadata>,
    requires_define: bool,
    rows: Vec<Vec<Option<QueryValue>>>,
    row_index: usize,
    cursor_id: u32,
    more_rows: bool,
    invalid_ref_cursor: bool,
    rowcount: i64,
    arraysize: u32,
    prefetchrows: u32,
    scrollable: bool,
    fetch_lobs: bool,
    fetch_lobs_overridden: bool,
    fetch_async_lobs: bool,
    fetch_decimals: bool,
    suspend_on_success: bool,
    rowfactory: Option<Py<PyAny>>,
    inputtypehandler: Option<Py<PyAny>>,
    outputtypehandler: Option<Py<PyAny>>,
    warning: Option<Py<PyAny>>,
    has_positional_input_sizes: bool,
    has_named_input_sizes: bool,
    named_input_sizes: Vec<(String, Py<PyAny>)>,
    statement_changed: bool,
    is_query: bool,
}

impl ThinCursorImpl {
    fn drain_cancel_response(&self) -> PyResult<()> {
        let mut guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        BlockingConnection::drain_cancel_response(connection).map_err(runtime_error)
    }

    fn new(
        connection: Arc<Mutex<Option<RustConnection>>>,
        autocommit: Arc<Mutex<bool>>,
        cancel_requested: Arc<AtomicBool>,
        state: Arc<Mutex<ThinConnState>>,
        scrollable: bool,
    ) -> Self {
        Self {
            connection,
            autocommit,
            cancel_requested,
            state,
            statement: None,
            bind_values: Vec::new(),
            bind_vars: Vec::new(),
            bind_names: Vec::new(),
            many_bind_rows: Vec::new(),
            columns: Vec::new(),
            fetch_vars: Vec::new(),
            fetch_define_columns: Vec::new(),
            requires_define: false,
            rows: Vec::new(),
            row_index: 0,
            cursor_id: 0,
            more_rows: false,
            invalid_ref_cursor: false,
            rowcount: 0,
            arraysize: 100,
            prefetchrows: 2,
            scrollable,
            fetch_lobs: true,
            fetch_lobs_overridden: false,
            fetch_async_lobs: false,
            fetch_decimals: false,
            suspend_on_success: false,
            rowfactory: None,
            inputtypehandler: None,
            outputtypehandler: None,
            warning: None,
            has_positional_input_sizes: false,
            has_named_input_sizes: false,
            named_input_sizes: Vec::new(),
            statement_changed: false,
            is_query: false,
        }
    }

    fn reset_fetch_define_state(&mut self) {
        self.fetch_vars.clear();
        self.fetch_define_columns.clear();
        self.requires_define = false;
    }

    fn active_output_type_handler(
        &self,
        py: Python<'_>,
        cursor: &Bound<'_, PyAny>,
    ) -> PyResult<Option<Py<PyAny>>> {
        if let Some(handler) = &self.outputtypehandler {
            return Ok(Some(handler.clone_ref(py)));
        }
        let connection = cursor.getattr("connection")?;
        let conn_impl = connection.getattr("_impl")?;
        if let Ok(conn_impl) = conn_impl.extract::<PyRef<'_, ThinConnImpl>>() {
            return Ok(conn_impl
                .outputtypehandler
                .as_ref()
                .map(|handler| handler.clone_ref(py)));
        }
        let conn_impl = conn_impl.extract::<PyRef<'_, AsyncThinConnImpl>>()?;
        Ok(conn_impl
            .inner
            .outputtypehandler
            .as_ref()
            .map(|handler| handler.clone_ref(py)))
    }

    fn prepare_fetch_defines(&mut self, py: Python<'_>, cursor: &Bound<'_, PyAny>) -> PyResult<()> {
        if !self.fetch_define_columns.is_empty() || self.columns.is_empty() {
            return Ok(());
        }
        self.fetch_vars = std::iter::repeat_with(|| None)
            .take(self.columns.len())
            .collect();
        self.fetch_define_columns = self.columns.clone();
        let Some(handler) = self.active_output_type_handler(py, cursor)? else {
            return Ok(());
        };
        let handler = handler.bind(py);
        let handler_cursor = Py::new(
            py,
            FetchHandlerCursor {
                connection: cursor.getattr("connection")?.unbind(),
                arraysize: self.arraysize,
            },
        )?;
        let handler_cursor = handler_cursor.bind(py);
        for (index, metadata) in self.columns.iter().enumerate() {
            let pub_metadata = Py::new(
                py,
                FetchMetadataImpl {
                    metadata: metadata.clone(),
                },
            )?;
            let value = handler.call1((handler_cursor, pub_metadata.bind(py)))?;
            if value.is_none() {
                continue;
            }
            let Some(var) = thin_var_from_value(&value)? else {
                return Err(raise_oracledb_driver_error("ERR_EXPECTING_VAR"));
            };
            let default_bind = var.borrow(py).default_bind.clone();
            let define_metadata = fetch_define_metadata_from_var(metadata, &default_bind);
            if !define_metadata.eq(metadata) {
                self.requires_define = true;
            }
            self.fetch_define_columns[index] = define_metadata;
            self.fetch_vars[index] = Some(var);
        }
        Ok(())
    }
}

#[pymethods]
impl ThinCursorImpl {
    #[getter]
    fn arraysize(&self) -> u32 {
        self.arraysize
    }

    #[setter]
    fn set_arraysize(&mut self, value: u32) {
        self.arraysize = value;
    }

    #[getter]
    fn prefetchrows(&self) -> u32 {
        self.prefetchrows
    }

    #[setter]
    fn set_prefetchrows(&mut self, value: u32) {
        self.prefetchrows = value;
    }

    #[getter]
    fn scrollable(&self) -> bool {
        self.scrollable
    }

    #[setter]
    fn set_scrollable(&mut self, value: bool) {
        self.scrollable = value;
    }

    #[getter]
    fn rowcount(&self) -> i64 {
        self.rowcount
    }

    #[getter]
    fn statement(&self) -> Option<&str> {
        self.statement.as_deref()
    }

    #[getter]
    #[pyo3(name = "fetch_vars")]
    fn fetch_vars_attr(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.is_query {
            let values = self
                .fetch_vars
                .iter()
                .map(|value| {
                    value
                        .as_ref()
                        .map(|var| var.clone_ref(py).into_any())
                        .unwrap_or_else(|| py.None())
                })
                .collect::<Vec<_>>();
            Ok(PyList::new(py, values)?.unbind().into())
        } else {
            Ok(py.None())
        }
    }

    #[getter]
    fn fetch_metadata(&self) -> Vec<FetchMetadataImpl> {
        self.columns
            .iter()
            .cloned()
            .map(|metadata| FetchMetadataImpl { metadata })
            .collect()
    }

    #[getter]
    fn fetch_lobs(&self) -> bool {
        self.fetch_lobs
    }

    #[setter]
    fn set_fetch_lobs(&mut self, value: bool) {
        self.fetch_lobs = value;
        self.fetch_lobs_overridden = true;
    }

    #[getter]
    fn fetch_decimals(&self) -> bool {
        self.fetch_decimals
    }

    #[setter]
    fn set_fetch_decimals(&mut self, value: bool) {
        self.fetch_decimals = value;
    }

    #[getter]
    fn suspend_on_success(&self) -> bool {
        self.suspend_on_success
    }

    #[setter]
    fn set_suspend_on_success(&mut self, value: bool) {
        self.suspend_on_success = value;
    }

    #[getter]
    fn rowfactory(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.rowfactory.as_ref().map(|value| value.clone_ref(py))
    }

    #[setter]
    fn set_rowfactory(&mut self, value: Option<Py<PyAny>>) {
        self.rowfactory = value;
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
    fn warning(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.warning.as_ref().map(|value| value.clone_ref(py))
    }

    #[pyo3(signature = (in_del=None))]
    fn close(&mut self, in_del: Option<bool>) {
        let _ = in_del;
        self.statement = None;
        self.bind_values.clear();
        self.bind_vars.clear();
        self.bind_names.clear();
        self.named_input_sizes.clear();
        self.many_bind_rows.clear();
        self.columns.clear();
        self.reset_fetch_define_state();
        self.rows.clear();
        self.row_index = 0;
        self.cursor_id = 0;
        self.more_rows = false;
        self.invalid_ref_cursor = false;
        self.is_query = false;
    }

    fn prepare(
        &mut self,
        statement: Option<String>,
        _tag: Option<String>,
        _cache_statement: Option<bool>,
    ) -> PyResult<()> {
        self.statement_changed = self.statement != statement;
        self.statement = statement;
        self.bind_names = if let Some(statement) = self.statement.as_deref() {
            validate_dml_returning_duplicate_binds(statement)?;
            unique_sql_bind_names(statement)?
        } else {
            Vec::new()
        };
        Ok(())
    }

    fn parse(&mut self, _cursor: &Bound<'_, PyAny>) -> PyResult<()> {
        let statement = self
            .statement
            .as_deref()
            .ok_or_else(|| raise_oracledb_driver_error("ERR_NO_STATEMENT"))?;
        validate_dml_returning_duplicate_binds(&statement)?;
        self.bind_names = unique_sql_bind_names(statement)?;
        validate_parse_bind_names(statement)?;
        Ok(())
    }

    fn _prepare_for_execute(
        &mut self,
        _cursor: &Bound<'_, PyAny>,
        statement: Option<String>,
        parameters: Option<&Bound<'_, PyAny>>,
        keyword_parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        if let Some(statement) = statement {
            self.statement_changed = self.statement.as_ref() != Some(&statement);
            self.statement = Some(statement);
        } else {
            self.statement_changed = false;
        }
        self.warning = None;
        let statement = self
            .statement
            .as_deref()
            .ok_or_else(|| raise_oracledb_driver_error("ERR_NO_STATEMENT"))?;
        let statement = statement.to_string();
        validate_dml_returning_duplicate_binds(&statement)?;
        if self.has_positional_input_sizes
            && parameters.is_some_and(|value| value.cast::<PyDict>().is_ok())
        {
            return Err(raise_oracledb_driver_error(
                "ERR_MIXED_POSITIONAL_AND_NAMED_BINDS",
            ));
        }
        if self.has_named_input_sizes
            && parameters.is_some_and(|value| {
                !value.is_none() && value.len().unwrap_or(0) > 0 && value.cast::<PyDict>().is_err()
            })
        {
            return Err(raise_oracledb_driver_error(
                "ERR_MIXED_POSITIONAL_AND_NAMED_BINDS",
            ));
        }
        validate_cursor_bind_parameters(_cursor, &self.connection, parameters, keyword_parameters)?;
        let (effective_statement, bind_values, bind_vars) = Python::attach(|py| {
            let previous_bind_names = self.bind_names.clone();
            let previous_bind_vars = self
                .bind_vars
                .iter()
                .map(|var| var.clone_ref(py))
                .collect::<Vec<_>>();
            let (effective_statement, effective_parameters, effective_keyword_parameters) =
                prepare_object_execute_inputs(py, &statement, parameters, keyword_parameters)?;
            let effective_parameters = effective_parameters.as_ref().map(|value| value.bind(py));
            let effective_keyword_parameters = effective_keyword_parameters
                .as_ref()
                .map(|value| value.bind(py));
            let bind_values = extract_bind_values(
                py,
                &effective_statement,
                effective_parameters,
                effective_keyword_parameters,
                &self.named_input_sizes,
                self.has_positional_input_sizes,
                &previous_bind_names,
                &previous_bind_vars,
            )?;
            let bind_vars = extract_bind_var_objects(
                py,
                &effective_statement,
                effective_parameters,
                effective_keyword_parameters,
                &self.named_input_sizes,
                &previous_bind_names,
                &previous_bind_vars,
            )?;
            Ok::<_, PyErr>((effective_statement, bind_values, bind_vars))
        })?;
        self.bind_names = unique_sql_bind_names(&effective_statement)?;
        self.bind_values = bind_values;
        self.bind_vars = bind_vars;
        self.statement = Some(effective_statement);
        self.many_bind_rows.clear();
        Ok(())
    }

    fn _prepare_for_executemany(
        &mut self,
        _cursor: &Bound<'_, PyAny>,
        statement: Option<String>,
        parameters: &Bound<'_, PyAny>,
        batch_size: u32,
    ) -> PyResult<ExecutemanyManager> {
        if let Some(statement) = statement {
            self.statement_changed = self.statement.as_ref() != Some(&statement);
            self.statement = Some(statement);
        } else {
            self.statement_changed = false;
        }
        self.warning = None;
        if self.statement.is_none() {
            return Err(raise_oracledb_driver_error("ERR_NO_STATEMENT"));
        }
        self.bind_values.clear();
        self.bind_vars.clear();
        let statement = self
            .statement
            .as_deref()
            .ok_or_else(|| raise_oracledb_driver_error("ERR_NO_STATEMENT"))?;
        validate_dml_returning_duplicate_binds(statement)?;
        self.bind_names = unique_sql_bind_names(statement)?;
        self.bind_vars = extract_executemany_bind_var_objects(
            parameters.py(),
            statement,
            parameters,
            &self.named_input_sizes,
        )?;
        self.many_bind_rows = extract_bind_rows(
            parameters.py(),
            statement,
            parameters,
            &self.named_input_sizes,
        )?;
        ExecutemanyManager::new(self.many_bind_rows.len(), batch_size)
    }

    fn executemany(
        &mut self,
        cursor: &Bound<'_, PyAny>,
        num_execs: u32,
        batcherrors: bool,
        arraydmlrowcounts: bool,
        offset: u32,
    ) -> PyResult<()> {
        if batcherrors {
            return Err(not_implemented("ThinCursorImpl executemany batcherrors"));
        }
        if arraydmlrowcounts {
            return Err(not_implemented(
                "ThinCursorImpl executemany array DML rowcounts",
            ));
        }
        let statement = self
            .statement
            .as_deref()
            .ok_or_else(|| PyRuntimeError::new_err("no statement prepared"))?;
        let start = usize::try_from(offset).map_err(runtime_error)?;
        let count = usize::try_from(num_execs).map_err(runtime_error)?;
        let end = start
            .checked_add(count)
            .ok_or_else(|| PyRuntimeError::new_err("executemany offset overflow"))?;
        let bind_rows = self
            .many_bind_rows
            .get(start..end)
            .ok_or_else(|| PyRuntimeError::new_err("executemany batch is out of range"))?
            .to_vec();
        let typed_lob_hints = typed_lob_bind_hints(cursor.py(), &self.bind_vars);
        let call_timeout = {
            let value = self.state.lock().map_err(runtime_error)?.call_timeout;
            (value > 0).then_some(value)
        };
        let mut result = match cursor.py().detach({
            let connection = Arc::clone(&self.connection);
            let state = Arc::clone(&self.state);
            let statement = statement.to_string();
            let mut bind_rows = bind_rows.clone();
            let typed_lob_hints = typed_lob_hints.clone();
            let prefetchrows = self.prefetchrows;
            move || -> Result<QueryResult, String> {
                let mut guard = connection.lock().map_err(|err| err.to_string())?;
                let connection = guard
                    .as_mut()
                    .ok_or_else(|| "connection is closed".to_string())?;
                apply_pending_current_schema_from_state(&state, connection, call_timeout)
                    .map_err(|err| err.to_string())?;
                materialize_typed_lob_bind_rows(
                    connection,
                    &mut bind_rows,
                    &typed_lob_hints,
                    call_timeout,
                )?;
                if bind_rows.iter().all(Vec::is_empty)
                    || bind_rows_need_iterative_plsql(&statement, &bind_rows)
                {
                    let mut result = QueryResult::default();
                    let mut out_values: BTreeMap<usize, Vec<Option<QueryValue>>> = BTreeMap::new();
                    let mut return_values: BTreeMap<usize, Vec<Option<QueryValue>>> =
                        BTreeMap::new();
                    for row in &bind_rows {
                        let row_result = if row.is_empty() {
                            BlockingConnection::execute_query_with_timeout(
                                connection,
                                &statement,
                                prefetchrows,
                                call_timeout,
                            )
                            .map_err(|err| err.to_string())?
                        } else {
                            let one_row = vec![row.clone()];
                            BlockingConnection::execute_query_with_bind_rows_and_timeout(
                                connection,
                                &statement,
                                prefetchrows,
                                &one_row,
                                call_timeout,
                            )
                            .map_err(|err| err.to_string())?
                        };
                        result.row_count = result.row_count.saturating_add(row_result.row_count);
                        result.compilation_error_warning |= row_result.compilation_error_warning;
                        for (index, value) in row_result.out_values {
                            out_values.entry(index).or_default().push(value);
                        }
                        for (index, values) in row_result.return_values {
                            return_values.entry(index).or_default().extend(values);
                        }
                    }
                    result.out_values = out_values
                        .into_iter()
                        .map(|(index, values)| (index, Some(QueryValue::Array(values))))
                        .collect();
                    result.return_values = return_values.into_iter().collect();
                    return Ok(result);
                }
                BlockingConnection::execute_query_with_bind_rows_and_timeout(
                    connection,
                    &statement,
                    prefetchrows,
                    &bind_rows,
                    call_timeout,
                )
                .map_err(|err| err.to_string())
            }
        }) {
            Ok(result) => result,
            Err(_) if self.cancel_requested.swap(false, Ordering::SeqCst) => {
                return Err(ora_cancel_error());
            }
            Err(err) => return Err(runtime_error(err)),
        };
        let is_query = !result.columns.is_empty();
        let should_commit = !is_query && *self.autocommit.lock().map_err(runtime_error)?;
        if should_commit {
            let mut guard = self.connection.lock().map_err(runtime_error)?;
            let connection = guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
            BlockingConnection::commit(connection).map_err(runtime_error)?;
        }
        if self.cancel_requested.swap(false, Ordering::SeqCst) {
            self.drain_cancel_response()?;
            return Err(ora_cancel_error());
        }
        supplement_json_lob_column_metadata(&self.connection, &mut result.columns, call_timeout)?;
        self.warning = Python::attach(|py| query_result_warning(py, &result))?;
        let lob_context = ThinLobContext {
            connection: Arc::clone(&self.connection),
            state: Arc::clone(&self.state),
            async_mode: false,
        };
        Python::attach(|py| {
            apply_out_bind_values(
                py,
                &self.bind_vars,
                &result.out_values,
                &result.return_values,
                Some(&lob_context),
            )
        })?;
        let is_plsql_statement = statement_is_plsql(statement);
        self.state.lock().map_err(runtime_error)?.record_statement(
            statement,
            is_query,
            should_commit,
        );
        self.columns = result.columns;
        self.reset_fetch_define_state();
        self.requires_define = columns_require_define(&self.columns);
        self.rows = result.rows;
        self.row_index = 0;
        self.cursor_id = result.cursor_id;
        self.more_rows = result.more_rows;
        self.invalid_ref_cursor = false;
        self.rowcount = if is_plsql_statement {
            0
        } else {
            i64::from(num_execs)
        };
        self.is_query = is_query;
        Ok(())
    }

    fn execute(&mut self, cursor: &Bound<'_, PyAny>) -> PyResult<()> {
        if self.statement_changed {
            self.rowfactory = None;
        }
        if !self.fetch_lobs_overridden {
            self.fetch_lobs = default_fetch_lobs(cursor.py())?;
        }
        let statement = self
            .statement
            .as_deref()
            .ok_or_else(|| PyRuntimeError::new_err("no statement prepared"))?;
        let call_timeout = {
            let value = self.state.lock().map_err(runtime_error)?.call_timeout;
            (value > 0).then_some(value)
        };
        let typed_lob_hints = typed_lob_bind_hints(cursor.py(), &self.bind_vars);
        let mut result = match cursor.py().detach({
            let connection = Arc::clone(&self.connection);
            let state = Arc::clone(&self.state);
            let statement = statement.to_string();
            let mut bind_values = self.bind_values.clone();
            let typed_lob_hints = typed_lob_hints.clone();
            let prefetchrows = self.prefetchrows;
            move || -> Result<QueryResult, String> {
                let mut guard = connection.lock().map_err(|err| err.to_string())?;
                let connection = guard
                    .as_mut()
                    .ok_or_else(|| "connection is closed".to_string())?;
                apply_pending_current_schema_from_state(&state, connection, call_timeout)
                    .map_err(|err| err.to_string())?;
                materialize_typed_lob_bind_values(
                    connection,
                    &mut bind_values,
                    &typed_lob_hints,
                    call_timeout,
                )?;
                BlockingConnection::execute_query_with_binds_and_timeout(
                    connection,
                    &statement,
                    prefetchrows,
                    &bind_values,
                    call_timeout,
                )
                .map_err(|err| err.to_string())
            }
        }) {
            Ok(result) => result,
            Err(_) if self.cancel_requested.swap(false, Ordering::SeqCst) => {
                return Err(ora_cancel_error());
            }
            Err(err) => return Err(runtime_error(err)),
        };
        if self.cancel_requested.swap(false, Ordering::SeqCst) {
            self.drain_cancel_response()?;
            return Err(ora_cancel_error());
        }
        supplement_json_lob_column_metadata(&self.connection, &mut result.columns, call_timeout)?;
        self.warning = Python::attach(|py| query_result_warning(py, &result))?;
        let lob_context = ThinLobContext {
            connection: Arc::clone(&self.connection),
            state: Arc::clone(&self.state),
            async_mode: false,
        };
        Python::attach(|py| {
            apply_out_bind_values(
                py,
                &self.bind_vars,
                &result.out_values,
                &result.return_values,
                Some(&lob_context),
            )
        })?;
        let is_query = !result.columns.is_empty();
        let is_plsql = statement_is_plsql(statement);
        let should_commit = !is_query && *self.autocommit.lock().map_err(runtime_error)?;
        if should_commit {
            let mut guard = self.connection.lock().map_err(runtime_error)?;
            let connection = guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
            BlockingConnection::commit(connection).map_err(runtime_error)?;
        }
        self.state.lock().map_err(runtime_error)?.record_statement(
            statement,
            is_query,
            should_commit,
        );
        self.columns = result.columns;
        self.reset_fetch_define_state();
        self.requires_define = columns_require_define(&self.columns);
        self.rows = result.rows;
        self.row_index = 0;
        self.cursor_id = result.cursor_id;
        self.more_rows = result.more_rows;
        self.invalid_ref_cursor = false;
        self.rowcount = if is_query || is_plsql {
            0
        } else {
            i64::try_from(result.row_count).unwrap_or(i64::MAX)
        };
        self.is_query = is_query;
        Ok(())
    }

    fn is_query(&self, _connection: &Bound<'_, PyAny>) -> bool {
        self.is_query
    }

    fn fetch_next_row(
        &mut self,
        py: Python<'_>,
        _cursor: &Bound<'_, PyAny>,
    ) -> PyResult<Option<Py<PyAny>>> {
        if self.invalid_ref_cursor {
            return Err(raise_oracledb_driver_error("ERR_INVALID_REF_CURSOR"));
        }
        self.prepare_fetch_defines(py, _cursor)?;
        if self.row_index >= self.rows.len() && self.more_rows && self.cursor_id != 0 {
            let previous_row = self.rows.last().cloned();
            let requires_define = self.requires_define;
            let define_columns = self.fetch_define_columns.clone();
            let mut guard = self.connection.lock().map_err(runtime_error)?;
            let connection = guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
            let result = if requires_define {
                BlockingConnection::define_and_fetch_rows_with_columns(
                    connection,
                    self.cursor_id,
                    self.prefetchrows,
                    &define_columns,
                    previous_row.as_deref(),
                )
            } else {
                BlockingConnection::fetch_rows_with_columns(
                    connection,
                    self.cursor_id,
                    self.arraysize,
                    &self.columns,
                    previous_row.as_deref(),
                )
            }
            .map_err(runtime_error)?;
            if !result.columns.is_empty() {
                self.columns = result.columns;
            } else if requires_define {
                self.columns = define_columns;
            }
            self.rows = result.rows;
            self.row_index = 0;
            if result.cursor_id != 0 {
                self.cursor_id = result.cursor_id;
            }
            self.more_rows = result.more_rows;
            if requires_define {
                self.requires_define = false;
            }
            self.invalid_ref_cursor = false;
        }
        self.fetch_buffered_next_row(py, _cursor)
    }

    fn fetch_buffered_next_row(
        &mut self,
        py: Python<'_>,
        _cursor: &Bound<'_, PyAny>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let Some(row) = self.rows.get(self.row_index) else {
            return Ok(None);
        };
        self.row_index += 1;
        self.rowcount += 1;
        let lob_context = ThinLobContext {
            connection: Arc::clone(&self.connection),
            state: Arc::clone(&self.state),
            async_mode: self.fetch_async_lobs,
        };
        let values = row
            .iter()
            .enumerate()
            .map(|(index, value)| {
                if let Some(Some(var)) = self.fetch_vars.get(index) {
                    return var
                        .borrow(py)
                        .output_value_to_py(py, value, Some(&lob_context));
                }
                if self
                    .columns
                    .get(index)
                    .is_some_and(|metadata| metadata.is_json)
                {
                    return json_query_value_to_py(py, value, Some(_cursor), Some(&lob_context));
                }
                query_value_to_py(
                    py,
                    value,
                    Some(_cursor),
                    Some(&lob_context),
                    self.fetch_lobs,
                )
            })
            .collect::<PyResult<Vec<_>>>()?;
        let tuple = PyTuple::new(py, values)?;
        if let Some(rowfactory) = &self.rowfactory {
            return rowfactory.call1(py, tuple).map(Some).map_err(Into::into);
        }
        Ok(Some(tuple.unbind().into()))
    }

    #[pyo3(name = "get_fetch_vars")]
    fn get_fetch_vars_method(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.fetch_vars_attr(py)
    }

    #[getter(bind_vars)]
    fn bind_vars_attr(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let values = self
            .bind_vars
            .iter()
            .map(|value| value.clone_ref(py))
            .collect::<Vec<_>>();
        Ok(PyList::new(py, values)?.unbind().into())
    }

    fn get_bind_vars(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.bind_vars_attr(py)
    }

    fn setinputsizes(
        &mut self,
        py: Python<'_>,
        connection: &Bound<'_, PyAny>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let has_args = !args.is_empty();
        let has_kwargs = kwargs.is_some_and(|value| !value.is_empty());
        if has_args && has_kwargs {
            return Err(raise_oracledb_driver_error("ERR_ARGS_AND_KEYWORD_ARGS"));
        }
        self.has_positional_input_sizes = has_args;
        self.has_named_input_sizes = has_kwargs;
        self.named_input_sizes.clear();
        if has_kwargs {
            let kwargs = kwargs.expect("has_kwargs implies kwargs is present");
            let result = PyDict::new(py);
            for (key, value) in kwargs.iter() {
                let key = key.extract::<String>()?;
                let var = thin_var_from_input_size(py, connection, &value)?;
                self.named_input_sizes
                    .push((key.clone(), var.clone_ref(py).into_any()));
                result.set_item(key, var)?;
            }
            return Ok(result.unbind().into());
        }
        let result = PyList::empty(py);
        for value in args.iter() {
            let var = thin_var_from_input_size(py, connection, &value)?;
            result.append(var.clone_ref(py))?;
            self.named_input_sizes
                .push((result.len().to_string(), var.into_any()));
        }
        Ok(result.unbind().into())
    }

    #[pyo3(signature = (
        connection,
        typ,
        size=0,
        num_elements=1,
        inconverter=None,
        outconverter=None,
        encoding_errors=None,
        bypass_decode=false,
        convert_nulls=false,
        is_array=false
    ))]
    fn create_var(
        &self,
        py: Python<'_>,
        connection: &Bound<'_, PyAny>,
        typ: &Bound<'_, PyAny>,
        size: u32,
        num_elements: u32,
        inconverter: Option<Py<PyAny>>,
        outconverter: Option<Py<PyAny>>,
        encoding_errors: Option<String>,
        bypass_decode: bool,
        convert_nulls: bool,
        is_array: bool,
    ) -> PyResult<Py<ThinVar>> {
        let _ = inconverter;
        let _ = encoding_errors;
        thin_var_from_type_spec(
            py,
            connection,
            typ,
            size,
            is_array,
            num_elements,
            outconverter,
            convert_nulls,
            bypass_decode,
        )
    }

    fn get_array_dml_row_counts(&self) -> PyResult<Vec<u64>> {
        Err(not_implemented("ThinCursorImpl.get_array_dml_row_counts"))
    }

    fn get_batch_errors(&self) -> PyResult<Vec<Py<PyAny>>> {
        Err(not_implemented("ThinCursorImpl.get_batch_errors"))
    }

    fn get_bind_names(&self) -> Vec<String> {
        self.bind_names
            .iter()
            .map(|name| public_bind_name(name))
            .collect()
    }

    fn get_implicit_results(&self, _connection: &Bound<'_, PyAny>) -> PyResult<Vec<Py<PyAny>>> {
        Err(not_implemented("ThinCursorImpl.get_implicit_results"))
    }

    fn get_lastrowid(&self) -> Option<String> {
        None
    }
}

struct AsyncExecuteOutcome {
    result: QueryResult,
    should_commit: bool,
}

fn spawn_async_executemany_task(
    connection: Arc<Mutex<Option<RustConnection>>>,
    state: Arc<Mutex<ThinConnState>>,
    statement: String,
    mut bind_rows: Vec<Vec<BindValue>>,
    typed_lob_hints: Vec<Option<(u8, u8)>>,
    prefetchrows: u32,
    call_timeout: Option<u32>,
    autocommit: bool,
) -> BlockingTask<AsyncExecuteOutcome> {
    spawn_async_connection_task(
        "oracledb-pyshim-async-executemany",
        connection,
        move |cx, connection| {
            Box::pin(async move {
                apply_pending_current_schema_from_state_async(cx, &state, connection, call_timeout)
                    .await?;
                materialize_typed_lob_bind_rows_async(
                    cx,
                    connection,
                    &mut bind_rows,
                    &typed_lob_hints,
                    call_timeout,
                )
                .await?;
                if bind_rows.iter().all(Vec::is_empty)
                    || bind_rows_need_iterative_plsql(&statement, &bind_rows)
                {
                    let mut result = QueryResult::default();
                    let mut out_values: BTreeMap<usize, Vec<Option<QueryValue>>> = BTreeMap::new();
                    let mut return_values: BTreeMap<usize, Vec<Option<QueryValue>>> =
                        BTreeMap::new();
                    for row in &bind_rows {
                        let row_result = if row.is_empty() {
                            connection
                                .execute_query_with_timeout(
                                    cx,
                                    &statement,
                                    prefetchrows,
                                    call_timeout,
                                )
                                .await
                                .map_err(TaskError::from)?
                        } else {
                            let one_row = vec![row.clone()];
                            connection
                                .execute_query_with_bind_rows_and_timeout(
                                    cx,
                                    &statement,
                                    prefetchrows,
                                    &one_row,
                                    call_timeout,
                                )
                                .await
                                .map_err(TaskError::from)?
                        };
                        result.row_count = result.row_count.saturating_add(row_result.row_count);
                        result.compilation_error_warning |= row_result.compilation_error_warning;
                        for (index, value) in row_result.out_values {
                            out_values.entry(index).or_default().push(value);
                        }
                        for (index, values) in row_result.return_values {
                            return_values.entry(index).or_default().extend(values);
                        }
                    }
                    result.out_values = out_values
                        .into_iter()
                        .map(|(index, values)| (index, Some(QueryValue::Array(values))))
                        .collect();
                    result.return_values = return_values.into_iter().collect();
                    let should_commit = result.columns.is_empty() && autocommit;
                    if should_commit {
                        connection.commit(cx).await.map_err(TaskError::from)?;
                    }
                    return Ok(AsyncExecuteOutcome {
                        result,
                        should_commit,
                    });
                }
                let mut result = connection
                    .execute_query_with_bind_rows_and_timeout(
                        cx,
                        &statement,
                        prefetchrows,
                        &bind_rows,
                        call_timeout,
                    )
                    .await
                    .map_err(TaskError::from)?;
                supplement_json_lob_column_metadata_async(
                    cx,
                    connection,
                    &mut result.columns,
                    call_timeout,
                )
                .await?;
                let should_commit = result.columns.is_empty() && autocommit;
                if should_commit {
                    connection.commit(cx).await.map_err(TaskError::from)?;
                }
                Ok(AsyncExecuteOutcome {
                    result,
                    should_commit,
                })
            })
        },
    )
}

fn spawn_async_execute_task(
    connection: Arc<Mutex<Option<RustConnection>>>,
    state: Arc<Mutex<ThinConnState>>,
    statement: String,
    mut bind_values: Vec<BindValue>,
    typed_lob_hints: Vec<Option<(u8, u8)>>,
    prefetchrows: u32,
    call_timeout: Option<u32>,
    autocommit: bool,
) -> BlockingTask<AsyncExecuteOutcome> {
    spawn_blocking_task("oracledb-pyshim-async-execute", move || {
        let mut guard = connection.lock().map_err(|err| err.to_string())?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| "connection is closed".to_string())?;
        let runtime = build_pyshim_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| "asupersync did not install an ambient Cx".to_string())?;
            apply_pending_current_schema_from_state_async(&cx, &state, connection, call_timeout)
                .await?;
            materialize_typed_lob_bind_values_async(
                &cx,
                connection,
                &mut bind_values,
                &typed_lob_hints,
                call_timeout,
            )
            .await?;
            let mut result = connection
                .execute_query_with_binds_and_timeout(
                    &cx,
                    &statement,
                    prefetchrows,
                    &bind_values,
                    call_timeout,
                )
                .await
                .map_err(TaskError::from)?;
            supplement_json_lob_column_metadata_async(
                &cx,
                connection,
                &mut result.columns,
                call_timeout,
            )
            .await?;
            let should_commit = result.columns.is_empty() && autocommit;
            if should_commit {
                connection.commit(&cx).await.map_err(TaskError::from)?;
            }
            Ok(AsyncExecuteOutcome {
                result,
                should_commit,
            })
        })
    })
}

fn spawn_async_fetch_task(
    connection: Arc<Mutex<Option<RustConnection>>>,
    cursor_id: u32,
    arraysize: u32,
    prefetchrows: u32,
    columns: Vec<ColumnMetadata>,
    define_columns: Vec<ColumnMetadata>,
    previous_row: Option<Vec<Option<QueryValue>>>,
    requires_define: bool,
) -> BlockingTask<QueryResult> {
    spawn_blocking_task("oracledb-pyshim-async-fetch", move || {
        let mut guard = connection.lock().map_err(|err| err.to_string())?;
        let connection = guard
            .as_mut()
            .ok_or_else(|| "connection is closed".to_string())?;
        let runtime = build_pyshim_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| "asupersync did not install an ambient Cx".to_string())?;
            if requires_define {
                connection
                    .define_and_fetch_rows_with_columns(
                        &cx,
                        cursor_id,
                        prefetchrows,
                        &define_columns,
                        previous_row.as_deref(),
                    )
                    .await
                    .map_err(TaskError::from)
            } else {
                connection
                    .fetch_rows_with_columns(
                        &cx,
                        cursor_id,
                        arraysize,
                        &columns,
                        previous_row.as_deref(),
                    )
                    .await
                    .map_err(TaskError::from)
            }
        })
    })
}

#[pyclass(module = "oracledb.thin_impl", name = "AsyncThinCursorImpl")]
struct AsyncThinCursorImpl {
    inner: ThinCursorImpl,
}

#[pymethods]
impl AsyncThinCursorImpl {
    #[getter]
    fn arraysize(&self) -> u32 {
        self.inner.arraysize
    }

    #[setter]
    fn set_arraysize(&mut self, value: u32) {
        self.inner.arraysize = value;
    }

    #[getter]
    fn prefetchrows(&self) -> u32 {
        self.inner.prefetchrows
    }

    #[setter]
    fn set_prefetchrows(&mut self, value: u32) {
        self.inner.prefetchrows = value;
    }

    #[getter]
    fn scrollable(&self) -> bool {
        self.inner.scrollable
    }

    #[setter]
    fn set_scrollable(&mut self, value: bool) {
        self.inner.scrollable = value;
    }

    #[getter]
    fn rowcount(&self) -> i64 {
        self.inner.rowcount
    }

    #[getter]
    fn statement(&self) -> Option<&str> {
        self.inner.statement.as_deref()
    }

    #[getter]
    #[pyo3(name = "fetch_vars")]
    fn fetch_vars_attr(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.inner.fetch_vars_attr(py)
    }

    #[getter]
    fn fetch_metadata(&self) -> Vec<FetchMetadataImpl> {
        self.inner.fetch_metadata()
    }

    #[getter]
    fn fetch_lobs(&self) -> bool {
        self.inner.fetch_lobs
    }

    #[setter]
    fn set_fetch_lobs(&mut self, value: bool) {
        self.inner.fetch_lobs = value;
        self.inner.fetch_lobs_overridden = true;
    }

    #[getter]
    fn fetch_decimals(&self) -> bool {
        self.inner.fetch_decimals
    }

    #[setter]
    fn set_fetch_decimals(&mut self, value: bool) {
        self.inner.fetch_decimals = value;
    }

    #[getter]
    fn suspend_on_success(&self) -> bool {
        self.inner.suspend_on_success
    }

    #[setter]
    fn set_suspend_on_success(&mut self, value: bool) {
        self.inner.suspend_on_success = value;
    }

    #[getter]
    fn rowfactory(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.inner
            .rowfactory
            .as_ref()
            .map(|value| value.clone_ref(py))
    }

    #[setter]
    fn set_rowfactory(&mut self, value: Option<Py<PyAny>>) {
        self.inner.rowfactory = value;
    }

    #[getter]
    fn inputtypehandler(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.inner
            .inputtypehandler
            .as_ref()
            .map(|value| value.clone_ref(py))
    }

    #[setter]
    fn set_inputtypehandler(&mut self, value: Option<Py<PyAny>>) {
        self.inner.inputtypehandler = value;
    }

    #[getter]
    fn outputtypehandler(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.inner
            .outputtypehandler
            .as_ref()
            .map(|value| value.clone_ref(py))
    }

    #[setter]
    fn set_outputtypehandler(&mut self, value: Option<Py<PyAny>>) {
        self.inner.outputtypehandler = value;
    }

    #[getter]
    fn warning(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.inner.warning.as_ref().map(|value| value.clone_ref(py))
    }

    #[getter(bind_vars)]
    fn bind_vars_attr(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.inner.bind_vars_attr(py)
    }

    fn get_bind_vars(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.inner.get_bind_vars(py)
    }

    #[pyo3(name = "get_fetch_vars")]
    fn get_fetch_vars_method(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.inner.fetch_vars_attr(py)
    }

    #[pyo3(signature = (in_del=None))]
    fn close(&mut self, in_del: Option<bool>) {
        self.inner.close(in_del)
    }

    fn prepare(
        &mut self,
        statement: Option<String>,
        tag: Option<String>,
        cache_statement: Option<bool>,
    ) -> PyResult<()> {
        self.inner.prepare(statement, tag, cache_statement)
    }

    async fn parse(&mut self, cursor: Py<PyAny>) -> PyResult<()> {
        Python::attach(|py| self.inner.parse(cursor.bind(py)))
    }

    fn _prepare_for_execute(
        &mut self,
        cursor: &Bound<'_, PyAny>,
        statement: Option<String>,
        parameters: Option<&Bound<'_, PyAny>>,
        keyword_parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        self.inner
            ._prepare_for_execute(cursor, statement, parameters, keyword_parameters)
    }

    fn _prepare_for_executemany(
        &mut self,
        cursor: &Bound<'_, PyAny>,
        statement: Option<String>,
        parameters: &Bound<'_, PyAny>,
        batch_size: u32,
    ) -> PyResult<ExecutemanyManager> {
        self.inner
            ._prepare_for_executemany(cursor, statement, parameters, batch_size)
    }

    async fn executemany(
        &mut self,
        _cursor: Py<PyAny>,
        num_execs: u32,
        batcherrors: bool,
        arraydmlrowcounts: bool,
        offset: u32,
    ) -> PyResult<()> {
        if batcherrors {
            return Err(not_implemented(
                "AsyncThinCursorImpl executemany batcherrors",
            ));
        }
        if arraydmlrowcounts {
            return Err(not_implemented(
                "AsyncThinCursorImpl executemany array DML rowcounts",
            ));
        }
        let statement = self
            .inner
            .statement
            .as_deref()
            .ok_or_else(|| PyRuntimeError::new_err("no statement prepared"))?
            .to_string();
        let start = usize::try_from(offset).map_err(runtime_error)?;
        let count = usize::try_from(num_execs).map_err(runtime_error)?;
        let end = start
            .checked_add(count)
            .ok_or_else(|| PyRuntimeError::new_err("executemany offset overflow"))?;
        let bind_rows = self
            .inner
            .many_bind_rows
            .get(start..end)
            .ok_or_else(|| PyRuntimeError::new_err("executemany batch is out of range"))?
            .to_vec();
        let typed_lob_hints = Python::attach(|py| typed_lob_bind_hints(py, &self.inner.bind_vars));
        let call_timeout = {
            let value = self.inner.state.lock().map_err(runtime_error)?.call_timeout;
            (value > 0).then_some(value)
        };
        let autocommit = *self.inner.autocommit.lock().map_err(runtime_error)?;
        let query = spawn_async_executemany_task(
            Arc::clone(&self.inner.connection),
            Arc::clone(&self.inner.state),
            statement.clone(),
            bind_rows,
            typed_lob_hints,
            self.inner.prefetchrows,
            call_timeout,
            autocommit,
        );
        let outcome = match query.await {
            Ok(outcome) => outcome,
            Err(_) if self.inner.cancel_requested.swap(false, Ordering::SeqCst) => {
                return Err(ora_cancel_error());
            }
            Err(err) => {
                if let Some(row_count) = err.server_row_count() {
                    self.inner.rowcount = i64::try_from(row_count).unwrap_or(i64::MAX);
                }
                return Err(runtime_error(err));
            }
        };
        if self.inner.cancel_requested.swap(false, Ordering::SeqCst) {
            self.inner.drain_cancel_response()?;
            return Err(ora_cancel_error());
        }
        let result = outcome.result;
        let should_commit = outcome.should_commit;
        let is_query = !result.columns.is_empty();
        self.inner.warning = Python::attach(|py| query_result_warning(py, &result))?;
        let lob_context = ThinLobContext {
            connection: Arc::clone(&self.inner.connection),
            state: Arc::clone(&self.inner.state),
            async_mode: true,
        };
        Python::attach(|py| {
            apply_out_bind_values(
                py,
                &self.inner.bind_vars,
                &result.out_values,
                &result.return_values,
                Some(&lob_context),
            )
        })?;
        self.inner
            .state
            .lock()
            .map_err(runtime_error)?
            .record_statement(&statement, is_query, should_commit);
        self.inner.columns = result.columns;
        self.inner.reset_fetch_define_state();
        self.inner.requires_define = columns_require_define(&self.inner.columns);
        self.inner.rows = result.rows;
        self.inner.row_index = 0;
        self.inner.cursor_id = result.cursor_id;
        self.inner.more_rows = result.more_rows;
        self.inner.invalid_ref_cursor = false;
        self.inner.rowcount = if statement_is_plsql(&statement) {
            0
        } else {
            i64::from(num_execs)
        };
        self.inner.is_query = is_query;
        Ok(())
    }

    async fn execute(&mut self, _cursor: Py<PyAny>) -> PyResult<()> {
        if self.inner.statement_changed {
            self.inner.rowfactory = None;
        }
        if !self.inner.fetch_lobs_overridden {
            self.inner.fetch_lobs = Python::attach(default_fetch_lobs)?;
        }
        let statement = self
            .inner
            .statement
            .as_deref()
            .ok_or_else(|| PyRuntimeError::new_err("no statement prepared"))?
            .to_string();
        let call_timeout = {
            let value = self.inner.state.lock().map_err(runtime_error)?.call_timeout;
            (value > 0).then_some(value)
        };
        let typed_lob_hints = Python::attach(|py| typed_lob_bind_hints(py, &self.inner.bind_vars));
        let autocommit = *self.inner.autocommit.lock().map_err(runtime_error)?;
        let query = spawn_async_execute_task(
            Arc::clone(&self.inner.connection),
            Arc::clone(&self.inner.state),
            statement.clone(),
            self.inner.bind_values.clone(),
            typed_lob_hints,
            self.inner.prefetchrows,
            call_timeout,
            autocommit,
        );
        let outcome = match query.await {
            Ok(outcome) => outcome,
            Err(_) if self.inner.cancel_requested.swap(false, Ordering::SeqCst) => {
                return Err(ora_cancel_error());
            }
            Err(err) => return Err(runtime_error(err)),
        };
        let result = outcome.result;
        let should_commit = outcome.should_commit;
        if self.inner.cancel_requested.swap(false, Ordering::SeqCst) {
            self.inner.drain_cancel_response()?;
            return Err(ora_cancel_error());
        }
        self.inner.warning = Python::attach(|py| query_result_warning(py, &result))?;
        let lob_context = ThinLobContext {
            connection: Arc::clone(&self.inner.connection),
            state: Arc::clone(&self.inner.state),
            async_mode: true,
        };
        Python::attach(|py| {
            apply_out_bind_values(
                py,
                &self.inner.bind_vars,
                &result.out_values,
                &result.return_values,
                Some(&lob_context),
            )
        })?;
        let is_query = !result.columns.is_empty();
        let is_plsql = statement_is_plsql(&statement);
        self.inner
            .state
            .lock()
            .map_err(runtime_error)?
            .record_statement(&statement, is_query, should_commit);
        self.inner.columns = result.columns;
        self.inner.reset_fetch_define_state();
        self.inner.requires_define = columns_require_define(&self.inner.columns);
        self.inner.rows = result.rows;
        self.inner.row_index = 0;
        self.inner.cursor_id = result.cursor_id;
        self.inner.more_rows = result.more_rows;
        self.inner.invalid_ref_cursor = false;
        self.inner.rowcount = if is_query || is_plsql {
            0
        } else {
            i64::try_from(result.row_count).unwrap_or(i64::MAX)
        };
        self.inner.is_query = is_query;
        Ok(())
    }

    fn is_query(&self, connection: &Bound<'_, PyAny>) -> bool {
        self.inner.is_query(connection)
    }

    async fn fetch_next_row(&mut self, cursor: Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
        if self.inner.invalid_ref_cursor {
            return Err(raise_oracledb_driver_error("ERR_INVALID_REF_CURSOR"));
        }
        Python::attach(|py| self.inner.prepare_fetch_defines(py, cursor.bind(py)))?;
        if self.inner.row_index >= self.inner.rows.len()
            && self.inner.more_rows
            && self.inner.cursor_id != 0
        {
            let previous_row = self.inner.rows.last().cloned();
            let requires_define = self.inner.requires_define;
            let define_columns = self.inner.fetch_define_columns.clone();
            let fetch = spawn_async_fetch_task(
                Arc::clone(&self.inner.connection),
                self.inner.cursor_id,
                self.inner.arraysize,
                self.inner.prefetchrows,
                self.inner.columns.clone(),
                define_columns.clone(),
                previous_row,
                requires_define,
            );
            let result = fetch.await.map_err(runtime_error)?;
            if !result.columns.is_empty() {
                self.inner.columns = result.columns;
            } else if requires_define {
                self.inner.columns = define_columns;
            }
            self.inner.rows = result.rows;
            self.inner.row_index = 0;
            if result.cursor_id != 0 {
                self.inner.cursor_id = result.cursor_id;
            }
            self.inner.more_rows = result.more_rows;
            if requires_define {
                self.inner.requires_define = false;
            }
            self.inner.invalid_ref_cursor = false;
        }
        Python::attach(|py| {
            self.inner.fetch_async_lobs = true;
            let result = self.inner.fetch_buffered_next_row(py, cursor.bind(py));
            self.inner.fetch_async_lobs = false;
            result
        })
    }

    fn setinputsizes(
        &mut self,
        py: Python<'_>,
        connection: &Bound<'_, PyAny>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        self.inner.setinputsizes(py, connection, args, kwargs)
    }

    #[pyo3(signature = (
        connection,
        typ,
        size=0,
        num_elements=1,
        inconverter=None,
        outconverter=None,
        encoding_errors=None,
        bypass_decode=false,
        convert_nulls=false,
        is_array=false
    ))]
    fn create_var(
        &self,
        py: Python<'_>,
        connection: &Bound<'_, PyAny>,
        typ: &Bound<'_, PyAny>,
        size: u32,
        num_elements: u32,
        inconverter: Option<Py<PyAny>>,
        outconverter: Option<Py<PyAny>>,
        encoding_errors: Option<String>,
        bypass_decode: bool,
        convert_nulls: bool,
        is_array: bool,
    ) -> PyResult<Py<ThinVar>> {
        self.inner.create_var(
            py,
            connection,
            typ,
            size,
            num_elements,
            inconverter,
            outconverter,
            encoding_errors,
            bypass_decode,
            convert_nulls,
            is_array,
        )
    }

    fn get_array_dml_row_counts(&self) -> PyResult<Vec<u64>> {
        self.inner.get_array_dml_row_counts()
    }

    fn get_batch_errors(&self) -> PyResult<Vec<Py<PyAny>>> {
        self.inner.get_batch_errors()
    }

    fn get_bind_names(&self) -> Vec<String> {
        self.inner.get_bind_names()
    }

    fn get_implicit_results(&self, connection: &Bound<'_, PyAny>) -> PyResult<Vec<Py<PyAny>>> {
        self.inner.get_implicit_results(connection)
    }

    fn get_lastrowid(&self) -> Option<String> {
        self.inner.get_lastrowid()
    }

    async fn scroll(&mut self, _cursor: Py<PyAny>, _value: i32, _mode: String) -> PyResult<()> {
        Err(not_implemented("AsyncThinCursorImpl.scroll"))
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "AsyncThinConnImpl")]
struct AsyncThinConnImpl {
    inner: ThinConnImpl,
}

#[pymethods]
impl AsyncThinConnImpl {
    #[new]
    fn new(dsn: &Bound<'_, PyAny>, params_impl: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            inner: ThinConnImpl::new(dsn, params_impl)?,
        })
    }

    #[getter]
    fn dsn(&self) -> &str {
        &self.inner.dsn
    }

    #[getter]
    fn username(&self) -> &str {
        &self.inner.username
    }

    #[getter]
    fn proxy_user(&self) -> Option<&str> {
        self.inner.proxy_user.as_deref()
    }

    #[getter]
    fn thin(&self) -> bool {
        self.inner.thin
    }

    #[getter]
    fn server_version(&self) -> (u8, u8, u8, u8, u8) {
        self.inner.server_version
    }

    #[getter]
    fn warning(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.inner.warning.as_ref().map(|value| value.clone_ref(py))
    }

    #[getter]
    fn autocommit(&self) -> bool {
        self.inner.autocommit
    }

    #[setter]
    fn set_autocommit(&mut self, value: bool) -> PyResult<()> {
        self.inner.set_autocommit(value)
    }

    #[getter]
    fn inputtypehandler(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.inner
            .inputtypehandler
            .as_ref()
            .map(|value| value.clone_ref(py))
    }

    #[setter]
    fn set_inputtypehandler(&mut self, value: Option<Py<PyAny>>) {
        self.inner.inputtypehandler = value;
    }

    #[getter]
    fn outputtypehandler(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.inner
            .outputtypehandler
            .as_ref()
            .map(|value| value.clone_ref(py))
    }

    #[setter]
    fn set_outputtypehandler(&mut self, value: Option<Py<PyAny>>) {
        self.inner.outputtypehandler = value;
    }

    #[getter]
    fn tag(&self) -> Option<&str> {
        self.inner.tag.as_deref()
    }

    #[setter]
    fn set_tag(&mut self, value: Option<String>) {
        self.inner.tag = value;
    }

    #[getter]
    fn invoke_session_callback(&self) -> bool {
        self.inner.invoke_session_callback
    }

    #[setter]
    fn set_invoke_session_callback(&mut self, value: bool) {
        self.inner.invoke_session_callback = value;
    }

    async fn connect(&mut self, params_impl: Py<PyAny>) -> PyResult<()> {
        let prepared = Python::attach(|py| self.inner.prepare_connect(params_impl.bind(py)))?;
        let connection = spawn_async_connect_task(prepared.options)
            .await
            .map_err(runtime_error)?;
        let cancel_handle = connection.cancel_handle().map_err(runtime_error)?;
        self.inner.server_version = (0, 0, 0, 0, 0);
        *self.inner.cancel_handle.lock().map_err(runtime_error)? = Some(cancel_handle);
        *self.inner.connection.lock().map_err(runtime_error)? = Some(connection);
        if let Some(new_password) = prepared.new_password {
            self.change_password(prepared.password, new_password)
                .await?;
        }
        if let Some(edition) = prepared.edition {
            let identifier = sql_identifier(&edition)?;
            let sql = format!("alter session set edition = {identifier}");
            let call_timeout = self.inner.call_timeout()?;
            let task = spawn_async_connection_task(
                "oracledb-pyshim-async-set-edition",
                Arc::clone(&self.inner.connection),
                move |cx, connection| {
                    Box::pin(async move {
                        connection
                            .execute_query_with_timeout(cx, &sql, 1, call_timeout)
                            .await
                            .map(|_| ())
                            .map_err(TaskError::from)
                    })
                },
            );
            task.await.map_err(runtime_error)?;
            let mut state = self.inner.state.lock().map_err(runtime_error)?;
            state.edition = Some(edition);
            state.edition_probe_started = true;
        }
        Ok(())
    }

    #[pyo3(signature = (in_del=None))]
    async fn close(&self, in_del: Option<bool>) -> PyResult<()> {
        let _ = in_del;
        let Some(connection) = self.inner.take_connection_for_close()? else {
            return Ok(());
        };
        let close = spawn_async_close_task(connection);
        close_result_to_py(close.await)
    }

    async fn ping(&self) -> PyResult<()> {
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-ping",
            Arc::clone(&self.inner.connection),
            |cx, connection| {
                Box::pin(async move { connection.ping(cx).await.map_err(TaskError::from) })
            },
        );
        task.await.map_err(runtime_error)
    }

    async fn commit(&self) -> PyResult<()> {
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-commit",
            Arc::clone(&self.inner.connection),
            |cx, connection| {
                Box::pin(async move { connection.commit(cx).await.map_err(TaskError::from) })
            },
        );
        task.await.map_err(runtime_error)?;
        self.inner
            .state
            .lock()
            .map_err(runtime_error)?
            .transaction_in_progress = false;
        Ok(())
    }

    async fn rollback(&self) -> PyResult<()> {
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-rollback",
            Arc::clone(&self.inner.connection),
            |cx, connection| {
                Box::pin(async move { connection.rollback(cx).await.map_err(TaskError::from) })
            },
        );
        task.await.map_err(runtime_error)?;
        self.inner
            .state
            .lock()
            .map_err(runtime_error)?
            .transaction_in_progress = false;
        Ok(())
    }

    async fn change_password(&self, old_password: String, new_password: String) -> PyResult<()> {
        if new_password.len() > 1024 {
            return Err(dpy_database_error(
                "ORA-00988",
                "missing or invalid password(s)",
            ));
        }
        let user = user_identifier(&self.inner.username)?;
        let sql = format!(
            "alter user {user} identified by {} replace {}",
            quoted_oracle_string(&new_password),
            quoted_oracle_string(&old_password)
        );
        let call_timeout = {
            let value = self.inner.state.lock().map_err(runtime_error)?.call_timeout;
            (value > 0).then_some(value)
        };
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-change-password",
            Arc::clone(&self.inner.connection),
            move |cx, connection| {
                Box::pin(async move {
                    connection
                        .execute_query_with_timeout(cx, &sql, 1, call_timeout)
                        .await
                        .map(|_| ())
                        .map_err(TaskError::from)
                })
            },
        );
        task.await
            .map_err(runtime_error)
            .and_then(|()| set_password_override_for_user(&self.inner.username, &new_password))
    }

    fn get_is_healthy(&self) -> PyResult<bool> {
        self.inner.get_is_healthy()
    }

    fn get_sdu(&self) -> PyResult<u32> {
        self.inner.get_sdu()
    }

    async fn get_type(&self, conn: Py<PyAny>, name: String) -> PyResult<DbObjectTypeImpl> {
        Python::attach(|py| self.inner.get_type(conn.bind(py), &name))
    }

    fn get_call_timeout(&self) -> PyResult<u32> {
        self.inner.get_call_timeout()
    }

    fn set_call_timeout(&self, value: u32) -> PyResult<()> {
        self.inner.set_call_timeout(value)
    }

    fn clear_end_user_security_context(&self) -> PyResult<()> {
        self.inner.clear_end_user_security_context()
    }

    fn set_end_user_security_context(&self, context: &Bound<'_, PyAny>) -> PyResult<()> {
        self.inner.set_end_user_security_context(context)
    }

    fn cancel(&self) -> PyResult<()> {
        self.inner.cancel()
    }

    fn get_ltxid<'py>(&self, py: Python<'py>) -> Py<PyBytes> {
        self.inner.get_ltxid(py)
    }

    fn get_current_schema(&self) -> PyResult<Option<String>> {
        self.inner.get_current_schema()
    }

    fn set_current_schema(&self, value: Option<String>) -> PyResult<()> {
        self.inner.set_current_schema(value)
    }

    fn get_edition(&self) -> PyResult<Option<String>> {
        self.inner.get_edition()
    }

    fn get_external_name(&self) -> PyResult<Option<String>> {
        self.inner.get_external_name()
    }

    fn set_external_name(&self, value: Option<String>) -> PyResult<()> {
        self.inner.set_external_name(value)
    }

    fn get_internal_name(&self) -> PyResult<Option<String>> {
        self.inner.get_internal_name()
    }

    fn set_internal_name(&self, value: Option<String>) -> PyResult<()> {
        self.inner.set_internal_name(value)
    }

    fn get_max_identifier_length(&self) -> Option<u8> {
        self.inner.get_max_identifier_length()
    }

    fn get_instance_name(&self) -> PyResult<String> {
        self.inner.get_instance_name()
    }

    fn get_db_name(&self) -> PyResult<String> {
        self.inner.get_db_name()
    }

    fn get_max_open_cursors(&self) -> PyResult<i64> {
        self.inner.get_max_open_cursors()
    }

    fn get_service_name(&self) -> PyResult<String> {
        self.inner.get_service_name()
    }

    fn get_db_domain(&self) -> PyResult<Option<String>> {
        self.inner.get_db_domain()
    }

    fn get_stmt_cache_size(&self) -> PyResult<u32> {
        self.inner.get_stmt_cache_size()
    }

    fn set_stmt_cache_size(&self, value: u32) -> PyResult<()> {
        self.inner.set_stmt_cache_size(value)
    }

    fn get_transaction_in_progress(&self) -> PyResult<bool> {
        self.inner.get_transaction_in_progress()
    }

    fn set_action(&self, value: Option<String>) -> PyResult<()> {
        self.inner.set_action(value)
    }

    fn set_client_identifier(&self, value: Option<String>) -> PyResult<()> {
        self.inner.set_client_identifier(value)
    }

    fn set_client_info(&self, value: Option<String>) -> PyResult<()> {
        self.inner.set_client_info(value)
    }

    fn set_dbop(&self, value: Option<String>) -> PyResult<()> {
        self.inner.set_dbop(value)
    }

    fn set_module(&self, value: Option<String>) -> PyResult<()> {
        self.inner.set_module(value)
    }

    fn get_session_id(&self) -> PyResult<u32> {
        self.inner.get_session_id()
    }

    fn get_serial_num(&self) -> PyResult<u16> {
        self.inner.get_serial_num()
    }

    async fn create_temp_lob_impl(&self, lob_type: Py<PyAny>) -> PyResult<Py<AsyncThinLob>> {
        let (ora_type_num, csfrm) =
            Python::attach(|py| match py_type_name(lob_type.bind(py)).as_str() {
                "DB_TYPE_BLOB" => (ORA_TYPE_NUM_BLOB, 0),
                "DB_TYPE_NCLOB" => (ORA_TYPE_NUM_CLOB, CS_FORM_NCHAR),
                _ => (ORA_TYPE_NUM_CLOB, CS_FORM_IMPLICIT),
            });
        let task = spawn_async_connection_task(
            "oracledb-pyshim-async-create-temp-lob",
            Arc::clone(&self.inner.connection),
            move |cx, connection| {
                Box::pin(async move {
                    connection
                        .create_temp_lob(cx, ora_type_num, csfrm)
                        .await
                        .map_err(TaskError::from)
                })
            },
        );
        let result = task.await.map_err(runtime_error)?;
        Python::attach(|py| {
            Py::new(
                py,
                AsyncThinLob {
                    inner: ThinLob {
                        data: None,
                        locator: Arc::new(Mutex::new(Some(result.locator))),
                        ora_type_num,
                        csfrm,
                        size: 0,
                        chunk_size: 0,
                        context: Some(ThinLobContext {
                            connection: Arc::clone(&self.inner.connection),
                            state: Arc::clone(&self.inner.state),
                            async_mode: true,
                        }),
                        is_open: Arc::new(Mutex::new(false)),
                        bfile_name: None,
                    },
                },
            )
        })
    }

    fn create_cursor_impl(&self, scrollable: bool) -> AsyncThinCursorImpl {
        AsyncThinCursorImpl {
            inner: self.inner.create_cursor_impl(scrollable),
        }
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "ThinPoolImpl")]
struct ThinPoolImpl {
    #[pyo3(get)]
    dsn: String,
    #[pyo3(get)]
    username: String,
    #[pyo3(get)]
    homogeneous: bool,
    #[pyo3(get)]
    increment: u32,
    #[pyo3(get)]
    max: u32,
    #[pyo3(get)]
    min: u32,
    #[pyo3(get)]
    name: String,
    getmode: u32,
    max_lifetime_session: u32,
    max_sessions_per_shard: u32,
    opened: Arc<Mutex<bool>>,
    open_count: Arc<Mutex<u32>>,
    busy_count: Arc<Mutex<u32>>,
    ping_interval: u32,
    soda_metadata_cache: bool,
    stmt_cache_size: u32,
    timeout: u32,
    wait_timeout: u32,
}

#[pymethods]
impl ThinPoolImpl {
    #[new]
    fn new(dsn: &Bound<'_, PyAny>, params_impl: &Bound<'_, PyAny>) -> PyResult<Self> {
        let dsn = normalize_connect_string(dsn.extract()?);
        let username = get_string_attr(params_impl, "user")?;
        let min = get_optional_u32_attr(params_impl, "min")?.unwrap_or(1);
        let max = get_optional_u32_attr(params_impl, "max")?.unwrap_or(2);
        let increment = get_optional_u32_attr(params_impl, "increment")?.unwrap_or(1);
        let homogeneous = get_optional_bool_attr(params_impl, "homogeneous")?.unwrap_or(true);
        let getmode = get_optional_u32_attr(params_impl, "getmode")?.unwrap_or(0);
        let max_lifetime_session =
            get_optional_u32_attr(params_impl, "max_lifetime_session")?.unwrap_or(0);
        let max_sessions_per_shard =
            get_optional_u32_attr(params_impl, "max_sessions_per_shard")?.unwrap_or(0);
        let ping_interval = get_optional_u32_attr(params_impl, "ping_interval")?.unwrap_or(60);
        let soda_metadata_cache =
            get_optional_bool_attr(params_impl, "soda_metadata_cache")?.unwrap_or(false);
        let stmt_cache_size = get_optional_u32_attr(params_impl, "stmtcachesize")?.unwrap_or(20);
        let timeout = get_optional_u32_attr(params_impl, "timeout")?.unwrap_or(0);
        let wait_timeout = get_optional_u32_attr(params_impl, "wait_timeout")?.unwrap_or(0);
        Ok(Self {
            dsn,
            username,
            homogeneous,
            increment,
            max,
            min,
            name: String::new(),
            getmode,
            max_lifetime_session,
            max_sessions_per_shard,
            opened: Arc::new(Mutex::new(true)),
            open_count: Arc::new(Mutex::new(0)),
            busy_count: Arc::new(Mutex::new(0)),
            ping_interval,
            soda_metadata_cache,
            stmt_cache_size,
            timeout,
            wait_timeout,
        })
    }

    fn acquire(&self, _params_impl: &Bound<'_, PyAny>) -> PyResult<()> {
        if !*self.opened.lock().map_err(runtime_error)? {
            return Err(raise_oracledb_driver_error("ERR_POOL_NOT_OPEN"));
        }
        Err(not_implemented("ThinPoolImpl.acquire"))
    }

    fn close(&self, _force: bool) -> PyResult<()> {
        *self.opened.lock().map_err(runtime_error)? = false;
        *self.open_count.lock().map_err(runtime_error)? = 0;
        *self.busy_count.lock().map_err(runtime_error)? = 0;
        Ok(())
    }

    fn drop(&self, _conn_impl: &Bound<'_, PyAny>) -> PyResult<()> {
        Err(not_implemented("ThinPoolImpl.drop"))
    }

    fn get_busy_count(&self) -> PyResult<u32> {
        Ok(*self.busy_count.lock().map_err(runtime_error)?)
    }

    fn get_getmode(&self) -> u32 {
        self.getmode
    }

    fn get_max_lifetime_session(&self) -> u32 {
        self.max_lifetime_session
    }

    fn get_max_sessions_per_shard(&self) -> u32 {
        self.max_sessions_per_shard
    }

    fn get_open_count(&self) -> PyResult<u32> {
        Ok(*self.open_count.lock().map_err(runtime_error)?)
    }

    fn get_ping_interval(&self) -> u32 {
        self.ping_interval
    }

    fn get_soda_metadata_cache(&self) -> bool {
        self.soda_metadata_cache
    }

    fn get_stmt_cache_size(&self) -> u32 {
        self.stmt_cache_size
    }

    fn get_timeout(&self) -> u32 {
        self.timeout
    }

    fn get_wait_timeout(&self) -> u32 {
        if self.getmode == 2 {
            self.wait_timeout
        } else {
            0
        }
    }

    fn reconfigure(&mut self, min: u32, max: u32, increment: u32) {
        self.min = min;
        self.max = max;
        self.increment = increment;
    }

    fn return_connection(&self, _conn_impl: &Bound<'_, PyAny>) -> PyResult<()> {
        Err(not_implemented("ThinPoolImpl.return_connection"))
    }

    fn set_getmode(&mut self, value: u32) {
        self.getmode = value;
        if value != 2 {
            self.wait_timeout = 0;
        }
    }

    fn set_max_lifetime_session(&mut self, value: u32) {
        self.max_lifetime_session = value;
    }

    fn set_max_sessions_per_shard(&mut self, value: u32) {
        self.max_sessions_per_shard = value;
    }

    fn set_ping_interval(&mut self, value: u32) {
        self.ping_interval = value;
    }

    fn set_soda_metadata_cache(&mut self, value: bool) {
        self.soda_metadata_cache = value;
    }

    fn set_stmt_cache_size(&mut self, value: u32) {
        self.stmt_cache_size = value;
    }

    fn set_timeout(&mut self, value: u32) {
        self.timeout = value;
    }

    fn set_wait_timeout(&mut self, value: u32) {
        self.wait_timeout = value;
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "AsyncThinPoolImpl")]
struct AsyncThinPoolImpl {
    opened: Arc<Mutex<bool>>,
}

#[pymethods]
impl AsyncThinPoolImpl {
    #[new]
    fn new(_dsn: &Bound<'_, PyAny>, _params_impl: &Bound<'_, PyAny>) -> Self {
        Self {
            opened: Arc::new(Mutex::new(true)),
        }
    }

    async fn acquire(&self, _params_impl: Py<PyAny>) -> PyResult<()> {
        if !*self.opened.lock().map_err(runtime_error)? {
            return Err(raise_oracledb_driver_error("ERR_POOL_NOT_OPEN"));
        }
        Err(not_implemented("AsyncThinPoolImpl.acquire"))
    }

    async fn close(&self, _force: bool) -> PyResult<()> {
        *self.opened.lock().map_err(runtime_error)? = false;
        Ok(())
    }

    async fn drop(&self, _conn_impl: Py<PyAny>) -> PyResult<()> {
        if !*self.opened.lock().map_err(runtime_error)? {
            return Err(raise_oracledb_driver_error("ERR_POOL_NOT_OPEN"));
        }
        Err(not_implemented("AsyncThinPoolImpl.drop"))
    }

    async fn return_connection(&self, _conn_impl: Py<PyAny>, _in_del: bool) -> PyResult<()> {
        if !*self.opened.lock().map_err(runtime_error)? {
            return Err(raise_oracledb_driver_error("ERR_POOL_NOT_OPEN"));
        }
        Err(not_implemented("AsyncThinPoolImpl.return_connection"))
    }
}

#[pyclass(module = "oracledb.thin_impl", name = "EndUserSecurityContextImpl")]
#[derive(Default)]
struct EndUserSecurityContextImpl {
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

#[pymodule]
fn oracledb_pyshim(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init_thin_impl, m)?)?;
    m.add_function(wrap_pyfunction!(record_next_connect_args, m)?)?;
    m.add_function(wrap_pyfunction!(discard_pending_connect_args, m)?)?;
    m.add_class::<ThinConnImpl>()?;
    m.add_class::<ThinLob>()?;
    m.add_class::<AsyncThinLob>()?;
    m.add_class::<DbObjectTypeImpl>()?;
    m.add_class::<DbObjectAttrImpl>()?;
    m.add_class::<DbObjectImpl>()?;
    m.add_class::<ThinCursorImpl>()?;
    m.add_class::<AsyncThinCursorImpl>()?;
    m.add_class::<FetchMetadataImpl>()?;
    m.add_class::<ExecutemanyManager>()?;
    m.add_class::<AsyncThinConnImpl>()?;
    m.add_class::<ThinPoolImpl>()?;
    m.add_class::<AsyncThinPoolImpl>()?;
    m.add_class::<EndUserSecurityContextImpl>()?;
    Ok(())
}
