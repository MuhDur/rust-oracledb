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
    pub(crate) transaction_in_progress: bool,
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

    pub(crate) fn record_statement(&mut self, statement: &str, is_query: bool, committed: bool) {
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
}

pub(crate) struct PreparedConnect {
    pub(crate) options: ConnectOptions,
    pub(crate) password: String,
    pub(crate) new_password: Option<String>,
    pub(crate) edition: Option<String>,
}

impl ThinConnImpl {
    pub(crate) fn prepare_connect(&mut self, params_impl: &Bound<'_, PyAny>) -> PyResult<PreparedConnect> {
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

    pub(crate) fn call_timeout(&self) -> PyResult<Option<u32>> {
        let call_timeout = self.state.lock().map_err(runtime_error)?.call_timeout;
        Ok((call_timeout > 0).then_some(call_timeout))
    }

    pub(crate) fn take_connection_for_close(&self) -> PyResult<Option<RustConnection>> {
        *self.cancel_handle.lock().map_err(runtime_error)? = None;
        Ok(self.connection.lock().map_err(runtime_error)?.take())
    }
}

#[pymethods]
impl ThinConnImpl {
    #[new]
    pub(crate) fn new(dsn: &Bound<'_, PyAny>, params_impl: &Bound<'_, PyAny>) -> PyResult<Self> {
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

    pub(crate) fn get_is_healthy(&self) -> PyResult<bool> {
        Ok(self.connection.lock().map_err(runtime_error)?.is_some())
    }

    pub(crate) fn get_sdu(&self) -> PyResult<u32> {
        let guard = self.connection.lock().map_err(runtime_error)?;
        let connection = guard
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("connection is closed"))?;
        Ok(u32::try_from(connection.sdu()).unwrap_or(u32::MAX))
    }

    pub(crate) fn get_type(&self, _conn: &Bound<'_, PyAny>, name: &str) -> PyResult<DbObjectTypeImpl> {
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

    pub(crate) fn set_end_user_security_context(&self, _context: &Bound<'_, PyAny>) -> PyResult<()> {
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
            cancel_handle.cancel().map_err(runtime_error)?;
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
        Ok(self
            .state
            .lock()
            .map_err(runtime_error)?
            .transaction_in_progress)
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

    pub(crate) fn create_cursor_impl(&self, scrollable: bool) -> ThinCursorImpl {
        ThinCursorImpl::new(
            Arc::clone(&self.connection),
            Arc::clone(&self.autocommit_state),
            Arc::clone(&self.cancel_requested),
            Arc::clone(&self.state),
            scrollable,
        )
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
