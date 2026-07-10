//! Entry point for SODA: create / open / list / drop collections.

use asupersync::Cx;
use oracledb_protocol::thin::{BindValue, QueryResult, QueryValue};

use crate::Connection;

use super::collection::{execute_with_binds_raw, finish_cursor_operation, SodaCollection};
use super::error::{Result, SodaError};
use super::metadata::parse_metadata;

/// The SODA database facade. Zero-sized; every operation borrows the
/// [`Connection`] explicitly.
#[derive(Debug, Clone, Copy, Default)]
pub struct SodaDatabase;

impl SodaDatabase {
    pub fn new() -> Self {
        SodaDatabase
    }

    /// Create (or open, if it already exists with matching metadata) a
    /// collection. `metadata` is an optional JSON string; `map_mode` maps to an
    /// existing table.
    ///
    /// `DBMS_SODA.CREATE_COLLECTION` returns an OPAQUE `SODA_COLLECTION_T` we
    /// cannot bind out over the thin protocol, so we call it inside an anonymous
    /// block that discards the return value, then read the canonical descriptor
    /// back from `USER_SODA_COLLECTIONS`.
    pub async fn create_collection(
        &self,
        conn: &mut Connection,
        cx: &Cx,
        name: Option<&str>,
        metadata: Option<&str>,
        map_mode: bool,
    ) -> Result<SodaCollection> {
        // CREATE_MODE: 1 = DDL (create table), 2 = MAP (map to existing table)
        // per DBMS_SODA.CREATE_MODE_DDL / CREATE_MODE_MAP.
        let create_mode = if map_mode { 2 } else { 1 };
        let sql = "DECLARE c SODA_COLLECTION_T; \
                   BEGIN c := DBMS_SODA.CREATE_COLLECTION(:1, :2, :3); END;";
        // A NULL name lets the server raise ORA-40658 (collection name cannot be
        // null), matching the reference error contract.
        let binds = vec![
            match name {
                Some(n) => BindValue::Text(n.to_string()),
                None => BindValue::Null,
            },
            match metadata {
                Some(m) => BindValue::Text(m.to_string()),
                None => BindValue::Null,
            },
            BindValue::Number(create_mode.to_string()),
        ];
        execute_with_binds_raw(conn, cx, sql, 0, &binds).await?;

        // Read the descriptor back.
        let name = name.unwrap_or_default();
        self.open_collection(conn, cx, name).await?.ok_or_else(|| {
            SodaError::InvalidMetadata(format!("collection {name} not found after create"))
        })
    }

    /// Open an existing collection. Returns `None` if it does not exist.
    pub async fn open_collection(
        &self,
        conn: &mut Connection,
        cx: &Cx,
        name: &str,
    ) -> Result<Option<SodaCollection>> {
        let sql = "SELECT JSON_SERIALIZE(JSON_DESCRIPTOR) \
                   FROM USER_SODA_COLLECTIONS WHERE URI_NAME = :1";
        let binds = vec![BindValue::Text(name.to_string())];
        let result = execute_with_binds_raw(conn, cx, sql, 2, &binds).await?;
        // The descriptor is consumed in this method, so no caller-owned cursor
        // survives the return. Release before local JSON/metadata decoding.
        conn.release_cursor(result.cursor_id);
        let Some(cell) = result.cell(0, 0) else {
            return Ok(None);
        };
        let descriptor_text = cell
            .as_text()
            .ok_or_else(|| SodaError::InvalidMetadata("descriptor is not text".to_string()))?;
        let descriptor: serde_json::Value = serde_json::from_str(descriptor_text)
            .map_err(|e| SodaError::InvalidMetadata(format!("bad descriptor JSON: {e}")))?;
        let meta = parse_metadata(&descriptor)?;
        Ok(Some(SodaCollection::new(name.to_string(), meta)))
    }

    /// List collection names (alphabetical), optionally starting at `start_name`
    /// and limited to `limit` (0 = no limit).
    pub async fn get_collection_names(
        &self,
        conn: &mut Connection,
        cx: &Cx,
        start_name: Option<&str>,
        limit: u32,
    ) -> Result<Vec<String>> {
        let mut sql = String::from("SELECT URI_NAME FROM USER_SODA_COLLECTIONS");
        let mut binds = Vec::new();
        if let Some(start) = start_name {
            sql.push_str(" WHERE URI_NAME >= :1");
            binds.push(BindValue::Text(start.to_string()));
        }
        sql.push_str(" ORDER BY URI_NAME");
        if limit > 0 {
            sql.push_str(&format!(" FETCH FIRST {limit} ROWS ONLY"));
        }
        let array_size = limit.max(100);
        let result = execute_with_binds_raw(conn, cx, &sql, array_size, &binds).await?;
        let result = collect_owned_query_result(conn, cx, array_size, result).await?;
        let mut names = Vec::with_capacity(result.rows.len());
        for row in &result.rows {
            if let Some(name) = row
                .first()
                .and_then(|c| c.as_ref())
                .and_then(QueryValue::as_text)
            {
                names.push(name.to_string());
            }
        }
        Ok(names)
    }

    /// Drop a collection. Returns whether it existed and was dropped.
    ///
    /// `DBMS_SODA.DROP_COLLECTION` returns a NUMBER (1 if dropped, 0 if not
    /// found) which we capture via a NUMBER OUT bind.
    pub async fn drop_collection(
        &self,
        conn: &mut Connection,
        cx: &Cx,
        name: &str,
    ) -> Result<bool> {
        let sql = "BEGIN :1 := DBMS_SODA.DROP_COLLECTION(:2); END;";
        let binds = vec![
            BindValue::Output {
                ora_type_num: 2, // NUMBER
                csfrm: 0,
                buffer_size: 22,
            },
            BindValue::Text(name.to_string()),
        ];
        let result = execute_with_binds_raw(conn, cx, sql, 0, &binds).await?;
        let dropped = result
            .out_values
            .first()
            .and_then(|(_, v)| v.as_ref())
            .and_then(QueryValue::as_i64)
            .map(|n| n != 0)
            .unwrap_or(false);
        Ok(dropped)
    }
}

/// Consume every page of an internally-owned query result, then return its
/// statement to the cache. Unlike the public streaming cursor, callers of this
/// helper expose only owned values and cannot transfer cursor ownership.
pub(super) async fn collect_owned_query_result(
    conn: &mut Connection,
    cx: &Cx,
    array_size: u32,
    mut result: QueryResult,
) -> Result<QueryResult> {
    let cursor_id = result.cursor_id;
    let columns = result.columns.clone();
    let mut previous_row = result.rows.last().cloned();
    while cursor_id != 0 && result.more_rows {
        // A cancellation observed here proves the continuation operation never
        // started, so the still-valid statement can be released to the cache.
        // Once FETCH starts, failures retire it fail-closed.
        if let Err(err) = crate::observe_cancellation_between_round_trips(cx) {
            conn.release_cursor(cursor_id);
            return Err(SodaError::Driver(err));
        }
        let fetched = conn
            .fetch_rows_with_columns(cx, cursor_id, array_size, &columns, previous_row.as_deref())
            .await;
        let fetched = finish_cursor_operation(conn, cursor_id, fetched)?;
        result.more_rows = fetched.more_rows;
        if let Some(last) = fetched.rows.last() {
            previous_row = Some(last.clone());
        }
        result.rows.extend(fetched.rows);
    }
    conn.release_cursor(cursor_id);
    Ok(result)
}
