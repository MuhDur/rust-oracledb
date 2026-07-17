//! A SODA collection and the document operations that run against it.
//!
//! All operations are async and take `&mut Connection` plus the Asupersync
//! `&Cx`, mirroring the rest of the driver. They generate SQL/PL-SQL and run it
//! through the existing execute/fetch surface.

use asupersync::Cx;
use oracledb_protocol::oson::{decode_oson, encode_oson, OsonValue};
use oracledb_protocol::thin::{
    BindValue, ColumnMetadata, QueryResult, QueryValue, ORA_TYPE_NUM_BLOB, ORA_TYPE_NUM_CLOB,
    ORA_TYPE_NUM_JSON, ORA_TYPE_NUM_VECTOR,
};

use crate::Connection;

use super::cursor::SodaCursor;
use super::document::SodaDocument;
use super::error::{Result, SodaError};
use super::metadata::{quote_ident, ContentSqlType, SodaCollectionMetadata, VersionMethod};
use super::operation::{self, SelectColumns, SodaOperation};

/// A handle to a SODA collection: its name plus parsed metadata.
#[derive(Debug, Clone)]
pub struct SodaCollection {
    pub name: String,
    pub metadata: SodaCollectionMetadata,
}

impl SodaCollection {
    pub fn new(name: String, metadata: SodaCollectionMetadata) -> Self {
        SodaCollection { name, metadata }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn metadata(&self) -> &SodaCollectionMetadata {
        &self.metadata
    }

    // --- inserts -----------------------------------------------------------

    /// Insert one document. When `return_doc` is true the inserted key/version/
    /// timestamps are read back via RETURNING and returned in a metadata-only
    /// document (no content, matching python-oracledb).
    pub async fn insert_one(
        &self,
        conn: &mut Connection,
        cx: &Cx,
        doc: &SodaDocument,
        hint: Option<&str>,
        return_doc: bool,
    ) -> Result<Option<SodaDocument>> {
        if self.metadata.read_only {
            return Err(read_only_err());
        }
        let (sql, mut binds, ret_layout) = self.build_insert_sql(doc, hint, return_doc)?;
        // Bind order: input binds (content [+ client key] [+ media type]), then
        // the RETURNING outputs.
        if return_doc {
            self.push_returning_binds(&mut binds, ret_layout.bind_count);
        }

        let result = execute_with_binds_raw(conn, cx, &sql, 0, &binds).await?;

        if return_doc {
            let doc = self.returning_to_doc(&result, &ret_layout)?;
            Ok(Some(doc))
        } else {
            Ok(None)
        }
    }

    /// Insert many documents in a single batch.
    pub async fn insert_many(
        &self,
        conn: &mut Connection,
        cx: &Cx,
        docs: &[SodaDocument],
        hint: Option<&str>,
        return_docs: bool,
    ) -> Result<Option<Vec<SodaDocument>>> {
        if self.metadata.read_only {
            return Err(read_only_err());
        }
        if docs.is_empty() {
            return Err(SodaError::Driver(server_like_err(
                "DPI-1031: no documents were provided to insertMany",
            )));
        }

        if return_docs {
            // RETURNING with array binds is awkward across drivers; run one
            // INSERT ... RETURNING per document to collect metadata reliably.
            let mut out = Vec::with_capacity(docs.len());
            for doc in docs {
                if let Some(d) = self.insert_one(conn, cx, doc, hint, true).await? {
                    out.push(d);
                }
            }
            Ok(Some(out))
        } else {
            // Build the statement from the first document (the column shape is
            // identical for every document) and a bind row per document.
            let (sql, first_binds, _ret) = self.build_insert_sql(&docs[0], hint, false)?;
            let mut bind_rows = Vec::with_capacity(docs.len());
            bind_rows.push(first_binds);
            for doc in &docs[1..] {
                let (_, binds, _) = self.build_insert_sql(doc, hint, false)?;
                bind_rows.push(binds);
            }
            execute_with_bind_rows_raw(conn, cx, &sql, 0, &bind_rows).await?;
            Ok(None)
        }
    }

    // --- reads -------------------------------------------------------------

    /// Count matching documents.
    pub async fn get_count(
        &self,
        conn: &mut Connection,
        cx: &Cx,
        op: &SodaOperation,
    ) -> Result<u64> {
        let (sql, binds) = op.build_count_sql(&self.metadata)?;
        let result = execute_with_binds_raw(conn, cx, &sql, 1, &binds).await?;
        // This API consumes the prefetched scalar rather than handing a
        // cursor to its caller. Return the statement to the cache before any
        // local decoding can fail.
        conn.release_cursor(result.cursor_id);
        let count = result
            .cell(0, 0)
            .and_then(QueryValue::as_i64)
            .unwrap_or(0)
            .max(0) as u64;
        Ok(count)
    }

    /// Fetch the first matching document, if any.
    ///
    /// We do NOT append `FETCH NEXT 1 ROWS ONLY` here: on this server a
    /// `WHERE raw_col = :raw_bind` predicate stops matching when combined with a
    /// row-limiting clause (a row-source transformation interaction). Instead we
    /// fetch the first batch and take row 0; callers that explicitly set a
    /// `limit` still get it honoured through `build_select_sql`.
    pub async fn get_one(
        &self,
        conn: &mut Connection,
        cx: &Cx,
        op: &SodaOperation,
    ) -> Result<Option<SodaDocument>> {
        let (sql, binds, layout) = op.build_select_sql(&self.metadata)?;
        let result = execute_collect_with_binds(conn, cx, &sql, 1, &binds).await?;
        // `get_one` owns the query result outright. There is no cursor handle
        // in its return type that could release this ownership later.
        conn.release_cursor(result.cursor_id);
        if result.rows.is_empty() {
            return Ok(None);
        }
        let doc = self.row_to_document(&result.rows[0], &layout)?;
        Ok(Some(doc))
    }

    /// Fetch all matching documents (used by getDocuments).
    pub async fn get_documents(
        &self,
        conn: &mut Connection,
        cx: &Cx,
        op: &SodaOperation,
    ) -> Result<Vec<SodaDocument>> {
        let mut cursor = self.open_cursor(conn, cx, op).await?;
        let mut out = Vec::new();
        loop {
            match cursor.next_doc(conn, cx).await {
                Ok(Some(doc)) => out.push(doc),
                Ok(None) => {
                    cursor.release(conn);
                    return Ok(out);
                }
                Err(err) => {
                    // Release a still-valid cursor on local conversion errors.
                    // Fetch failures already retired it through the fail-closed
                    // path, making this cleanup an intentional no-op.
                    cursor.release(conn);
                    return Err(err);
                }
            }
        }
    }

    /// Open a streaming cursor over matching documents.
    pub async fn open_cursor(
        &self,
        conn: &mut Connection,
        cx: &Cx,
        op: &SodaOperation,
    ) -> Result<SodaCursor> {
        let (sql, binds, layout) = op.build_select_sql(&self.metadata)?;
        let array_size = op.fetch_array_size();
        let result = execute_collect_with_binds(conn, cx, &sql, array_size, &binds).await?;
        Ok(SodaCursor::new(self.clone(), result, layout, array_size))
    }

    // --- writes ------------------------------------------------------------

    /// Remove matching documents; returns the number removed.
    pub async fn remove(&self, conn: &mut Connection, cx: &Cx, op: &SodaOperation) -> Result<u64> {
        let (sql, binds) = op.build_delete_sql(&self.metadata)?;
        let result = execute_with_binds_raw(conn, cx, &sql, 0, &binds).await?;
        Ok(result.row_count)
    }

    /// Replace a single document identified by the operation's key. Returns
    /// whether a row was replaced; when `return_doc` is true the new key/
    /// version is returned too.
    pub async fn replace_one(
        &self,
        conn: &mut Connection,
        cx: &Cx,
        op: &SodaOperation,
        doc: &SodaDocument,
        return_doc: bool,
    ) -> Result<(bool, Option<SodaDocument>)> {
        if self.metadata.read_only {
            return Err(read_only_err());
        }
        // replaceOne requires key() per the reference; keys() is rejected.
        if op.keys.is_some() {
            return Err(SodaError::Driver(server_like_err(
                "ORA-40734: key not specified for SODA replaceOne",
            )));
        }
        let (sql, mut binds, ret_layout) = self.build_replace_sql(op, doc, return_doc)?;
        if let Some(layout) = &ret_layout {
            self.push_returning_binds(&mut binds, layout.bind_count);
        }

        let result = execute_with_binds_raw(conn, cx, &sql, 0, &binds).await?;

        let replaced = result.row_count > 0;
        let out = if return_doc && replaced {
            if let Some(layout) = ret_layout {
                Some(self.returning_to_doc(&result, &layout)?)
            } else {
                None
            }
        } else {
            None
        };
        Ok((replaced, out))
    }

    // --- DDL / admin -------------------------------------------------------

    /// Truncate the collection (remove all documents).
    pub async fn truncate(&self, conn: &mut Connection, cx: &Cx) -> Result<()> {
        let sql = format!("TRUNCATE TABLE {}", self.metadata.quoted_table());
        execute_raw(conn, cx, &sql, 0).await?;
        Ok(())
    }

    /// Create an index from a SODA index spec via DBMS_SODA_ADMIN.CREATE_INDEX.
    pub async fn create_index(&self, conn: &mut Connection, cx: &Cx, spec: &str) -> Result<()> {
        let sql = "BEGIN DBMS_SODA_ADMIN.CREATE_INDEX(P_URI_NAME => :1, P_INDEX_SPEC => :2); END;";
        let binds = vec![
            BindValue::Text(self.name.clone()),
            BindValue::Text(spec.to_string()),
        ];
        execute_with_binds_raw(conn, cx, sql, 0, &binds).await?;
        Ok(())
    }

    /// Drop an index by name. Returns whether the index existed and was
    /// dropped.
    ///
    /// `DBMS_SODA_ADMIN` has no `DROP_INDEX` procedure on this server build, so
    /// the index is dropped via DDL. A SODA index becomes a real index with the
    /// same (case-sensitive) name, so we quote it. ORA-01418 (index does not
    /// exist) maps to a `false` return; `force` appends `FORCE`.
    pub async fn drop_index(
        &self,
        conn: &mut Connection,
        cx: &Cx,
        index_name: &str,
        force: bool,
    ) -> Result<bool> {
        let force_kw = if force { " FORCE" } else { "" };
        let sql = format!(
            "DROP INDEX {}{}",
            super::metadata::quote_ident(index_name),
            force_kw
        );
        match execute_raw(conn, cx, &sql, 0).await {
            Ok(_) => Ok(true),
            // ORA-01418: specified index does not exist -> not dropped.
            // ORA-00942: table/view does not exist (index gone) -> false.
            Err(SodaError::Driver(e)) if matches!(e.ora_code(), Some(1418) | Some(942)) => {
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    // --- helpers -----------------------------------------------------------

    /// Build the content bind value appropriate to the content column type.
    fn content_bind(&self, doc: &SodaDocument) -> Result<BindValue> {
        match self.metadata.content_sql_type {
            ContentSqlType::Json => {
                // A decoded value (create_json_document) is OSON-encoded. Raw
                // bytes (create_document) are bound as text so the server parses
                // and VALIDATES them — invalid JSON then raises a server error
                // (ORA-40441) instead of failing client-side, matching how the
                // database is the source of truth for JSON validity.
                if let Some(oson) = &doc.content_oson {
                    let image = encode_oson(oson, true)
                        .map_err(|e| SodaError::Driver(crate::Error::Protocol(e)))?;
                    return Ok(BindValue::Json(image));
                }
                if let Some(bytes) = &doc.content_bytes {
                    if let Ok(v) = decode_oson(bytes) {
                        let image = encode_oson(&v, true)
                            .map_err(|e| SodaError::Driver(crate::Error::Protocol(e)))?;
                        return Ok(BindValue::Json(image));
                    }
                    let text = String::from_utf8(bytes.clone()).map_err(|_| {
                        SodaError::Driver(server_like_err("ORA-40441: JSON syntax error"))
                    })?;
                    return Ok(BindValue::Text(text));
                }
                Err(SodaError::Qbe("document has no content".to_string()))
            }
            ContentSqlType::Blob | ContentSqlType::Clob | ContentSqlType::Raw => {
                let bytes = self.doc_to_bytes(doc)?;
                Ok(BindValue::Raw(bytes))
            }
            ContentSqlType::Varchar2 => {
                let bytes = self.doc_to_bytes(doc)?;
                let text = String::from_utf8(bytes).map_err(|_| {
                    SodaError::InvalidMetadata("VARCHAR2 content must be UTF-8".to_string())
                })?;
                Ok(BindValue::Text(text))
            }
        }
    }

    /// Produce raw bytes for a document destined for a BLOB/CLOB column.
    fn doc_to_bytes(&self, doc: &SodaDocument) -> Result<Vec<u8>> {
        if let Some(bytes) = &doc.content_bytes {
            return Ok(bytes.clone());
        }
        if let Some(oson) = &doc.content_oson {
            let value = oson_to_json(oson);
            return serde_json::to_vec(&value)
                .map_err(|e| SodaError::Qbe(format!("could not serialize content: {e}")));
        }
        Err(SodaError::Qbe("document has no content".to_string()))
    }

    /// Build the INSERT statement, its input binds, and the RETURNING layout.
    ///
    /// Input binds always start with the content (`:1`). For legacy collections
    /// a client-assigned key and/or a media type are appended as further binds.
    /// Server-generated key/version/timestamp columns use SQL expressions
    /// (SYS_GUID / SYSTIMESTAMP), not binds.
    fn build_insert_sql(
        &self,
        doc: &SodaDocument,
        hint: Option<&str>,
        with_returning: bool,
    ) -> Result<(String, Vec<BindValue>, ReturningLayout)> {
        let meta = &self.metadata;
        let mut columns = vec![quote_ident(&meta.content_column)];
        let mut values = vec![":1".to_string()];
        let mut input_binds: Vec<BindValue> = vec![self.content_bind(doc)?];

        // Non-native (legacy) collections have explicit key/version/timestamp
        // columns with no server-side default or trigger, so the driver fills
        // them. Native 23ai collections populate these automatically.
        if !meta.native {
            use super::metadata::KeyAssignment;
            match meta.key_assignment {
                KeyAssignment::Uuid | KeyAssignment::Guid => {
                    columns.push(quote_ident(&meta.key_column));
                    values.push("RAWTOHEX(SYS_GUID())".to_string());
                }
                KeyAssignment::Client => {
                    // Client must supply the key; bind it.
                    let key = doc.key.clone().ok_or_else(|| {
                        SodaError::Driver(server_like_err(
                            "ORA-40646: client-assigned key required",
                        ))
                    })?;
                    columns.push(quote_ident(&meta.key_column));
                    values.push(format!(":{}", input_binds.len() + 1));
                    input_binds.push(BindValue::Text(key));
                }
                KeyAssignment::Sequence | KeyAssignment::EmbeddedOid => {}
            }
            if let (Some(vc), VersionMethod::Uuid) = (&meta.version_column, &meta.version_method) {
                columns.push(quote_ident(vc));
                values.push("RAWTOHEX(SYS_GUID())".to_string());
            }
            if let Some(c) = &meta.creation_time_column {
                columns.push(quote_ident(c));
                values.push("SYSTIMESTAMP".to_string());
            }
            if let Some(c) = &meta.last_modified_column {
                columns.push(quote_ident(c));
                values.push("SYSTIMESTAMP".to_string());
            }
        }

        // Media-type column (mixed-media collections): bind the document's media
        // type so non-JSON content is round-tripped with its type. The column
        if let Some(mt_col) = &meta.media_type_column {
            columns.push(quote_ident(mt_col));
            values.push(format!(":{}", input_binds.len() + 1));
            input_binds.push(BindValue::Text(doc.media_type.clone()));
        }

        let hint_str = hint.map(|h| format!("/*+ {h} */ ")).unwrap_or_default();
        let mut sql = format!(
            "INSERT {hint_str}INTO {} ({}) VALUES ({})",
            meta.quoted_table(),
            columns.join(", "),
            values.join(", ")
        );

        let layout = if with_returning {
            let (clause, layout) = self.returning_clause(input_binds.len());
            sql.push_str(&clause);
            layout
        } else {
            ReturningLayout::default()
        };

        Ok((sql, input_binds, layout))
    }

    /// Build the UPDATE statement used by replaceOne().
    fn build_replace_sql(
        &self,
        op: &SodaOperation,
        doc: &SodaDocument,
        with_returning: bool,
    ) -> Result<(String, Vec<BindValue>, Option<ReturningLayout>)> {
        let key = op.key.as_ref().ok_or_else(|| {
            SodaError::Driver(server_like_err(
                "ORA-40734: key not specified for SODA replaceOne",
            ))
        })?;

        let content_bind = self.content_bind(doc)?;
        let meta = &self.metadata;
        let mut binds = vec![content_bind];
        let mut set_parts = vec![format!("{} = :1", quote_ident(&meta.content_column))];

        // bump last_modified if present
        if let Some(lm) = &meta.last_modified_column {
            set_parts.push(format!("{} = SYSTIMESTAMP", quote_ident(lm)));
        }
        // version: regenerate UUID for UUID method; leave server-managed otherwise
        if let (Some(vc), VersionMethod::Uuid) = (&meta.version_column, &meta.version_method) {
            set_parts.push(format!("{} = SYS_GUID()", quote_ident(vc)));
        }

        let mut next_bind = 2;
        // RAW key/version columns bind decoded bytes (see operation.rs note on
        // why HEXTORAW(:bind) does not match in a WHERE comparison).
        let key_is_raw = meta.key_sql_type.eq_ignore_ascii_case("RAW");
        if key_is_raw {
            binds.push(BindValue::Raw(operation::hex_decode(key)?));
        } else {
            binds.push(BindValue::Text(key.clone()));
        }
        let mut where_clause = format!("{} = :{next_bind}", quote_ident(&meta.key_column));
        next_bind += 1;
        if let Some(version) = &op.version {
            let vc = meta.version_column.as_ref().ok_or_else(|| {
                SodaError::NotSupported("collection has no version column".to_string())
            })?;
            if matches!(meta.version_method, VersionMethod::None) && meta.native {
                binds.push(BindValue::Raw(operation::hex_decode(version)?));
            } else {
                binds.push(BindValue::Text(version.clone()));
            }
            where_clause.push_str(&format!(" AND {} = :{next_bind}", quote_ident(vc)));
        }

        let mut sql = format!(
            "UPDATE {} SET {} WHERE {}",
            meta.quoted_table(),
            set_parts.join(", "),
            where_clause
        );

        let layout = if with_returning {
            let (clause, layout) = self.returning_clause(binds.len());
            sql.push_str(&clause);
            Some(layout)
        } else {
            None
        };

        Ok((sql, binds, layout))
    }

    /// Build a `RETURNING key[,version][,created][,lastmod] INTO ...` clause,
    /// numbering bind placeholders starting after `existing_binds`.
    fn returning_clause(&self, existing_binds: usize) -> (String, ReturningLayout) {
        let meta = &self.metadata;
        let mut ret_cols = Vec::new();
        let mut into = Vec::new();
        let mut layout = ReturningLayout::default();
        let mut n = existing_binds;
        let mut idx = 0;

        // key
        n += 1;
        if meta.key_sql_type.eq_ignore_ascii_case("RAW") {
            ret_cols.push(format!("RAWTOHEX({})", quote_ident(&meta.key_column)));
        } else {
            ret_cols.push(quote_ident(&meta.key_column));
        }
        into.push(format!(":{n}"));
        layout.key_idx = Some(idx);
        idx += 1;

        if let Some(vc) = &meta.version_column {
            n += 1;
            if matches!(meta.version_method, VersionMethod::None) && meta.native {
                ret_cols.push(format!("RAWTOHEX({})", quote_ident(vc)));
            } else {
                ret_cols.push(quote_ident(vc));
            }
            into.push(format!(":{n}"));
            layout.version_idx = Some(idx);
            idx += 1;
        }
        if let Some(cc) = &meta.creation_time_column {
            n += 1;
            ret_cols.push(format!(
                "TO_CHAR({}, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6')",
                quote_ident(cc)
            ));
            into.push(format!(":{n}"));
            layout.created_idx = Some(idx);
            idx += 1;
        }
        if let Some(lm) = &meta.last_modified_column {
            n += 1;
            ret_cols.push(format!(
                "TO_CHAR({}, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6')",
                quote_ident(lm)
            ));
            into.push(format!(":{n}"));
            layout.last_modified_idx = Some(idx);
            idx += 1;
        }

        let _ = idx;
        let clause = format!(
            " RETURNING {} INTO {}",
            ret_cols.join(", "),
            into.join(", ")
        );
        // Append the matching ReturnOutput binds onto the layout for the caller.
        layout.bind_count = ret_cols.len();
        (clause, layout)
    }

    /// Decode a fetched row into a SodaDocument per the select layout.
    pub(crate) fn row_to_document(
        &self,
        row: &[Option<QueryValue>],
        layout: &SelectColumns,
    ) -> Result<SodaDocument> {
        let key = row
            .get(layout.key_idx)
            .and_then(|c| c.as_ref())
            .and_then(QueryValue::as_text)
            .map(str::to_string);

        let mut doc = SodaDocument {
            key,
            content_bytes: None,
            content_oson: None,
            media_type: "application/json".to_string(),
            version: None,
            created_on: None,
            last_modified: None,
        };

        // content
        if let Some(cell) = row.get(layout.content_idx).and_then(|c| c.as_ref()) {
            match cell {
                QueryValue::Json(oson) => doc.content_oson = Some((**oson).clone()),
                QueryValue::Raw(bytes) => doc.content_bytes = Some(bytes.clone()),
                QueryValue::Text(s) => doc.content_bytes = Some(s.clone().into_bytes()),
                other => {
                    if let Some(bytes) = other.as_raw() {
                        doc.content_bytes = Some(bytes.to_vec());
                    }
                }
            }
        }

        if let Some(i) = layout.version_idx {
            doc.version = row
                .get(i)
                .and_then(|c| c.as_ref())
                .and_then(QueryValue::as_text)
                .map(str::to_string);
        }
        if let Some(i) = layout.created_idx {
            doc.created_on = row
                .get(i)
                .and_then(|c| c.as_ref())
                .and_then(QueryValue::as_text)
                .map(str::to_string);
        }
        if let Some(i) = layout.last_modified_idx {
            doc.last_modified = row
                .get(i)
                .and_then(|c| c.as_ref())
                .and_then(QueryValue::as_text)
                .map(str::to_string);
        }
        if let Some(i) = layout.media_type_idx {
            if let Some(mt) = row
                .get(i)
                .and_then(|c| c.as_ref())
                .and_then(QueryValue::as_text)
            {
                doc.media_type = mt.to_string();
            }
        }
        Ok(doc)
    }

    /// Build a metadata-only document from a RETURNING result.
    fn returning_to_doc(
        &self,
        result: &oracledb_protocol::thin::QueryResult,
        layout: &ReturningLayout,
    ) -> Result<SodaDocument> {
        let mut doc = SodaDocument {
            key: None,
            content_bytes: None,
            content_oson: None,
            media_type: "application/json".to_string(),
            version: None,
            created_on: None,
            last_modified: None,
        };
        // return_values: Vec<(col_idx, Vec<rows>)> in declaration order.
        let get = |pos: usize| -> Option<String> {
            result
                .return_values
                .get(pos)
                .and_then(|(_, rows)| rows.first())
                .and_then(|c| c.as_ref())
                .and_then(QueryValue::as_text)
                .map(str::to_string)
        };
        if let Some(i) = layout.key_idx {
            doc.key = get(i);
        }
        if let Some(i) = layout.version_idx {
            doc.version = get(i);
        }
        if let Some(i) = layout.created_idx {
            doc.created_on = get(i);
        }
        if let Some(i) = layout.last_modified_idx {
            doc.last_modified = get(i);
        }
        Ok(doc)
    }
}

/// Bind/column layout for a RETURNING clause.
#[derive(Debug, Default, Clone)]
struct ReturningLayout {
    key_idx: Option<usize>,
    version_idx: Option<usize>,
    created_idx: Option<usize>,
    last_modified_idx: Option<usize>,
    bind_count: usize,
}

fn read_only_err() -> SodaError {
    SodaError::Driver(server_like_err(
        "ORA-40663: cannot modify a read-only SODA collection",
    ))
}

/// Wrap a message as a structured server-style error so the shim error path can
/// surface the ORA/DPI/DPY code as the exception's `full_code`. The numeric code
/// is parsed from a leading `ORA-NNNNN` if present.
fn server_like_err(message: &str) -> crate::Error {
    let code = parse_ora_code(message).unwrap_or(0);
    crate::Error::Protocol(oracledb_protocol::ProtocolError::ServerErrorInfo(Box::new(
        oracledb_protocol::ServerErrorDetails {
            message: message.to_string(),
            code,
            pos: 0,
            row_count: 0,
            rowid: None,
            array_dml_row_counts: None,
        },
    )))
}

/// Parse the `ORA-NNNNN` code from a message, if present.
fn parse_ora_code(message: &str) -> Option<u32> {
    let start = message.find("ORA-")? + "ORA-".len();
    let digits: String = message[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse::<u32>().ok()
}

// Re-export for the collection's insert path: ReturningLayout's bind binds are
// appended by the caller (database/collection) using these helpers.
impl SodaCollection {
    /// Append the ReturnOutput binds for a RETURNING clause to `binds`.
    pub(crate) fn push_returning_binds(&self, binds: &mut Vec<BindValue>, count: usize) {
        for _ in 0..count {
            binds.push(BindValue::ReturnOutput {
                ora_type_num: 1, // VARCHAR2
                csfrm: 0,
                buffer_size: 4000,
            });
        }
    }
}

// These focused helpers must remain before the production collection operations.
#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use crate::soda::database::collect_owned_query_result;
    use crate::soda::metadata::KeyAssignment;
    use crate::soda::SodaDatabase;
    use asupersync::{net::TcpStream, CancelKind};
    use oracledb_protocol::thin::{
        CS_FORM_IMPLICIT, ORA_TYPE_NUM_NUMBER, ORA_TYPE_NUM_VARCHAR,
        TNS_DATA_FLAGS_END_OF_RESPONSE, TNS_MSG_TYPE_DESCRIBE_INFO, TNS_MSG_TYPE_ERROR,
        TNS_MSG_TYPE_ROW_DATA, TNS_MSG_TYPE_ROW_HEADER, TNS_PACKET_TYPE_DATA,
    };
    use oracledb_protocol::wire::{encode_packet, PacketLengthWidth, TtcWriter};
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    fn mixed_case_collection() -> SodaCollection {
        SodaCollection::new(
            "MixedCollection".to_string(),
            SodaCollectionMetadata {
                table_name: "MixedCollection".into(),
                schema_name: Some("PyTest".into()),
                key_column: "CamelKey".into(),
                key_sql_type: "VARCHAR2".into(),
                key_assignment: KeyAssignment::Client,
                key_path: None,
                content_column: "JsonDoc".into(),
                content_sql_type: ContentSqlType::Blob,
                version_column: Some("DocVersion".into()),
                version_method: VersionMethod::Uuid,
                last_modified_column: Some("LastModifiedAt".into()),
                creation_time_column: Some("CreatedAt".into()),
                media_type_column: Some("MimeType".into()),
                read_only: false,
                native: false,
            },
        )
    }

    fn document() -> SodaDocument {
        SodaDocument::from_bytes(
            br#"{"name":"Ada"}"#.to_vec(),
            Some("client-key".to_string()),
            Some("application/json".to_string()),
        )
    }

    fn response_packet(payload: &[u8]) -> Vec<u8> {
        encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(TNS_DATA_FLAGS_END_OF_RESPONSE),
            payload,
            PacketLengthWidth::Large32,
        )
        .expect("encode SODA test response")
    }

    fn read_one_packet(socket: &mut std::net::TcpStream) -> std::io::Result<()> {
        let mut header = [0u8; 8];
        socket.read_exact(&mut header)?;
        let declared = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
        if declared < header.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "test request packet shorter than its header",
            ));
        }
        let mut body = vec![0u8; declared - header.len()];
        socket.read_exact(&mut body)
    }

    fn write_test_column(writer: &mut TtcWriter, name: &[u8], ora_type_num: u8, position: u16) {
        // Full default-capability (field version 24) describe record. Keeping
        // this fixture in TTC form makes the test drive the real execute
        // decoder before it reaches the initial DEFINE-FETCH failure.
        writer.write_u8(ora_type_num);
        writer.write_u8(0); // flags
        writer.write_u8(0); // precision
        writer.write_u8(0); // scale
        writer.write_ub4(4000); // buffer size
        writer.write_ub4(0); // max array elements
        writer.write_ub8(0); // continuation flags
        writer
            .write_bytes_with_two_lengths(None)
            .expect("column oid");
        writer.write_ub2(0); // version
        writer.write_ub2(0); // server charset id
        writer.write_u8(CS_FORM_IMPLICIT);
        writer.write_ub4(4000); // max size
        writer.write_ub4(0); // oaccolid (12.2+)
        writer.write_u8(1); // nullable
        writer.write_u8(0); // flags
        writer
            .write_bytes_with_two_lengths(Some(name))
            .expect("column name");
        writer
            .write_bytes_with_two_lengths(None)
            .expect("object schema");
        writer
            .write_bytes_with_two_lengths(None)
            .expect("object type");
        writer.write_ub2(position);
        writer.write_ub4(0); // uds flags
        writer
            .write_bytes_with_two_lengths(None)
            .expect("domain schema");
        writer
            .write_bytes_with_two_lengths(None)
            .expect("domain name");
        writer.write_ub4(0); // annotation count
        writer.write_ub4(0); // vector dimensions
        writer.write_u8(0); // vector format
        writer.write_u8(0); // vector flags
    }

    fn write_error_info(
        writer: &mut TtcWriter,
        cursor_id: u16,
        number: u32,
        row_count: u64,
        message: &str,
    ) {
        writer.write_ub4(0); // call status
        writer.write_ub2(0); // sequence
        writer.write_ub4(0); // current row
        writer.write_ub2(0); // obsolete error number
        writer.write_ub2(0); // array element error 1
        writer.write_ub2(0); // array element error 2
        writer.write_ub2(cursor_id);
        writer.write_sb4(0); // error position
        writer.write_raw(&[0u8; 5]);
        writer.write_u8(0); // warning flags
        writer.write_ub4(0); // rowid rba
        writer.write_ub2(0); // rowid partition
        writer.write_u8(0);
        writer.write_ub4(0); // rowid block
        writer.write_ub2(0); // rowid slot
        writer.write_ub4(0); // os error
        writer.write_raw(&[0u8; 2]);
        writer.write_ub2(0); // padding
        writer.write_ub4(0); // successful iterations
        writer
            .write_bytes_with_two_lengths(None)
            .expect("diagnostic field");
        writer.write_ub2(0); // batch error count
        writer.write_ub4(0); // batch offset count
        writer.write_ub2(0); // batch message count
        writer.write_ub4(number);
        writer.write_ub8(row_count);
        writer.write_ub4(0); // SQL type (20.1+)
        writer.write_ub4(0); // server checksum
        if number != 0 {
            writer
                .write_bytes_with_length(message.as_bytes())
                .expect("server error message");
        }
    }

    fn write_success_error_info(writer: &mut TtcWriter, cursor_id: u16) {
        write_error_info(writer, cursor_id, 0, 0, "");
    }

    fn write_no_data_error_info(writer: &mut TtcWriter, cursor_id: u16, row_count: u64) {
        write_error_info(
            writer,
            cursor_id,
            1403,
            row_count,
            "ORA-01403: no data found",
        );
    }

    fn json_describe_execute_response(cursor_id: u16, terminal: bool) -> Vec<u8> {
        let mut writer = TtcWriter::new();
        writer.write_u8(TNS_MSG_TYPE_DESCRIBE_INFO);
        writer
            .write_bytes_with_length(b"soda initial define")
            .expect("describe name");
        writer.write_ub4(4096); // max row size
        writer.write_ub4(1); // column count
        writer.write_u8(0); // describe column marker
        write_test_column(&mut writer, b"DOC", ORA_TYPE_NUM_JSON, 1);
        writer
            .write_bytes_with_two_lengths(None)
            .expect("current date");
        writer.write_ub4(0); // dcbflag
        writer.write_ub4(0); // dcbmdbz
        writer.write_ub4(0); // dcbmnpr
        writer.write_ub4(0); // dcbmxpr
        writer.write_bytes_with_two_lengths(None).expect("dcbqcky");
        writer.write_u8(TNS_MSG_TYPE_ERROR);
        if terminal {
            write_no_data_error_info(&mut writer, cursor_id, 0);
        } else {
            write_success_error_info(&mut writer, cursor_id);
        }
        writer.into_bytes()
    }

    fn scalar_execute_response(
        cursor_id: u16,
        column_name: &[u8],
        ora_type_num: u8,
        value: &[u8],
    ) -> Vec<u8> {
        let mut writer = TtcWriter::new();
        writer.write_u8(TNS_MSG_TYPE_DESCRIBE_INFO);
        writer
            .write_bytes_with_length(b"soda one-shot scalar")
            .expect("describe name");
        writer.write_ub4(4096); // max row size
        writer.write_ub4(1); // column count
        writer.write_u8(0); // describe column marker
        write_test_column(&mut writer, column_name, ora_type_num, 1);
        writer
            .write_bytes_with_two_lengths(None)
            .expect("current date");
        writer.write_ub4(0); // dcbflag
        writer.write_ub4(0); // dcbmdbz
        writer.write_ub4(0); // dcbmnpr
        writer.write_ub4(0); // dcbmxpr
        writer.write_bytes_with_two_lengths(None).expect("dcbqcky");
        writer.write_u8(TNS_MSG_TYPE_ROW_HEADER);
        writer.write_u8(0); // flags
        writer.write_ub2(1); // request count
        writer.write_ub4(1); // iteration number
        writer.write_ub4(1); // iteration count
        writer.write_ub2(0); // buffer length
        writer.write_ub4(0); // no duplicate-column bit vector
        writer
            .write_bytes_with_two_lengths(None)
            .expect("row header id");
        writer.write_u8(TNS_MSG_TYPE_ROW_DATA);
        writer.write_bytes_with_length(value).expect("scalar value");
        writer.write_u8(TNS_MSG_TYPE_ERROR);
        write_no_data_error_info(&mut writer, cursor_id, 1);
        writer.into_bytes()
    }

    fn collection_name_rows_response(
        cursor_id: u16,
        values: &[String],
        include_describe: bool,
        terminal: bool,
    ) -> Vec<u8> {
        let mut writer = TtcWriter::new();
        if include_describe {
            writer.write_u8(TNS_MSG_TYPE_DESCRIBE_INFO);
            writer
                .write_bytes_with_length(b"soda collection names")
                .expect("describe name");
            writer.write_ub4(4096); // max row size
            writer.write_ub4(1); // column count
            writer.write_u8(0); // describe column marker
            write_test_column(&mut writer, b"URI_NAME", ORA_TYPE_NUM_VARCHAR, 1);
            writer
                .write_bytes_with_two_lengths(None)
                .expect("current date");
            writer.write_ub4(0); // dcbflag
            writer.write_ub4(0); // dcbmdbz
            writer.write_ub4(0); // dcbmnpr
            writer.write_ub4(0); // dcbmxpr
            writer.write_bytes_with_two_lengths(None).expect("dcbqcky");
        }
        writer.write_u8(TNS_MSG_TYPE_ROW_HEADER);
        writer.write_u8(0); // flags
        writer.write_ub2(1); // request count
        writer.write_ub4(1); // iteration number
        writer.write_ub4(u32::try_from(values.len()).expect("test row count fits u32"));
        writer.write_ub2(0); // buffer length
        writer.write_ub4(0); // no duplicate-column bit vector
        writer
            .write_bytes_with_two_lengths(None)
            .expect("row header id");
        for value in values {
            writer.write_u8(TNS_MSG_TYPE_ROW_DATA);
            writer
                .write_bytes_with_length(value.as_bytes())
                .expect("collection name");
        }
        writer.write_u8(TNS_MSG_TYPE_ERROR);
        if terminal {
            write_no_data_error_info(
                &mut writer,
                cursor_id,
                u64::try_from(values.len()).expect("test row count fits u64"),
            );
        } else {
            write_success_error_info(&mut writer, cursor_id);
        }
        writer.into_bytes()
    }

    fn one_shot_document_execute_response(cursor_id: u16, terminal: bool) -> Vec<u8> {
        let mut writer = TtcWriter::new();
        writer.write_u8(TNS_MSG_TYPE_DESCRIBE_INFO);
        writer
            .write_bytes_with_length(b"soda one-shot document")
            .expect("describe name");
        writer.write_ub4(4096); // max row size
        writer.write_ub4(2); // column count
        writer.write_u8(0); // describe column marker
        write_test_column(&mut writer, b"KEY", ORA_TYPE_NUM_VARCHAR, 1);
        write_test_column(&mut writer, b"DOC", ORA_TYPE_NUM_VARCHAR, 2);
        writer
            .write_bytes_with_two_lengths(None)
            .expect("current date");
        writer.write_ub4(0); // dcbflag
        writer.write_ub4(0); // dcbmdbz
        writer.write_ub4(0); // dcbmnpr
        writer.write_ub4(0); // dcbmxpr
        writer.write_bytes_with_two_lengths(None).expect("dcbqcky");
        writer.write_u8(TNS_MSG_TYPE_ROW_HEADER);
        writer.write_u8(0); // flags
        writer.write_ub2(1); // request count
        writer.write_ub4(1); // iteration number
        writer.write_ub4(1); // iteration count
        writer.write_ub2(0); // buffer length
        writer.write_ub4(0); // no duplicate-column bit vector
        writer
            .write_bytes_with_two_lengths(None)
            .expect("row header id");
        writer.write_u8(TNS_MSG_TYPE_ROW_DATA);
        writer
            .write_bytes_with_length(b"doc-key")
            .expect("document key");
        writer
            .write_bytes_with_length(br#"{"one":1}"#)
            .expect("document content");
        writer.write_u8(TNS_MSG_TYPE_ERROR);
        if terminal {
            write_no_data_error_info(&mut writer, cursor_id, 1);
        } else {
            write_success_error_info(&mut writer, cursor_id);
        }
        writer.into_bytes()
    }

    fn one_shot_collection() -> SodaCollection {
        SodaCollection::new(
            "OneShot".to_string(),
            SodaCollectionMetadata {
                table_name: "OneShot".to_string(),
                schema_name: None,
                key_column: "KEY".to_string(),
                key_sql_type: "VARCHAR2".to_string(),
                key_assignment: KeyAssignment::Client,
                key_path: None,
                content_column: "DOC".to_string(),
                content_sql_type: ContentSqlType::Varchar2,
                version_column: None,
                version_method: VersionMethod::None,
                last_modified_column: None,
                creation_time_column: None,
                media_type_column: None,
                read_only: false,
                native: false,
            },
        )
    }

    fn native_json_collection() -> SodaCollection {
        SodaCollection::new(
            "NativeEmpty".to_string(),
            SodaCollectionMetadata {
                table_name: "NativeEmpty".to_string(),
                schema_name: None,
                key_column: "RESID".to_string(),
                key_sql_type: "RAW".to_string(),
                key_assignment: KeyAssignment::EmbeddedOid,
                key_path: Some("_id".to_string()),
                content_column: "DOC".to_string(),
                content_sql_type: ContentSqlType::Json,
                version_column: None,
                version_method: VersionMethod::None,
                last_modified_column: None,
                creation_time_column: None,
                media_type_column: None,
                read_only: false,
                native: true,
            },
        )
    }

    #[test]
    fn failed_initial_define_fetch_retires_cursor_once() -> crate::Result<()> {
        const CURSOR_ID: u32 = 91;
        const SQL: &str = "select soda initial define probe";

        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<()> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;
            read_one_packet(&mut socket)?;
            socket.write_all(&response_packet(&json_describe_execute_response(
                u16::try_from(CURSOR_ID).expect("test cursor id fits u16"),
                false,
            )))?;
            socket.flush()?;
            read_one_packet(&mut socket)?;
            socket.write_all(&response_packet(&[0xff]))?;
            socket.flush()
        });

        let runtime = crate::build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| crate::Error::Runtime("missing ambient test Cx".to_string()))?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = crate::transport::plain_split(stream);
            let mut conn = crate::tests::loopback_connection(read, write);

            let err = execute_collect_with_binds(&mut conn, &cx, SQL, 1, &[])
                .await
                .expect_err("malformed initial DEFINE-FETCH must fail");
            assert!(matches!(err, SodaError::Driver(_)), "{err:?}");
            assert!(!conn.in_use_cursors.contains(&CURSOR_ID));
            assert!(conn
                .statement_cache
                .iter()
                .all(|entry| entry.cursor_id != CURSOR_ID));
            assert!(!conn.cursor_columns.contains_key(&CURSOR_ID));
            assert!(!conn.lob_prefetch_cursors.contains(&CURSOR_ID));

            let failed_again: crate::Result<QueryResult> =
                Err(crate::Error::Runtime("repeat failure".to_string()));
            finish_cursor_operation(&mut conn, CURSOR_ID, failed_again)
                .expect_err("repeated retirement remains an error");
            assert_eq!(
                conn.cursors_to_close
                    .iter()
                    .filter(|cursor_id| **cursor_id == CURSOR_ID)
                    .count(),
                1,
                "fail-closed cleanup must not queue duplicate closes"
            );
            Ok::<_, crate::Error>(())
        });

        server.join().expect("initial define mapper server joins")?;
        outcome
    }

    #[test]
    fn terminal_empty_native_json_query_skips_define_fetch() -> crate::Result<()> {
        const CURSOR_ID: u32 = 128;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<bool> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;
            read_one_packet(&mut socket)?;
            socket.write_all(&response_packet(&json_describe_execute_response(
                u16::try_from(CURSOR_ID).expect("test cursor id fits u16"),
                true,
            )))?;
            socket.flush()?;

            socket.set_read_timeout(Some(Duration::from_millis(300)))?;
            match read_one_packet(&mut socket) {
                Ok(()) => {
                    let mut payload = TtcWriter::new();
                    payload.write_u8(TNS_MSG_TYPE_ERROR);
                    write_no_data_error_info(
                        &mut payload,
                        u16::try_from(CURSOR_ID).expect("test cursor id fits u16"),
                        0,
                    );
                    socket.write_all(&response_packet(&payload.into_bytes()))?;
                    socket.flush()?;
                    Ok(true)
                }
                Err(err)
                    if matches!(
                        err.kind(),
                        std::io::ErrorKind::WouldBlock
                            | std::io::ErrorKind::TimedOut
                            | std::io::ErrorKind::UnexpectedEof
                    ) =>
                {
                    Ok(false)
                }
                Err(err) => Err(err),
            }
        });

        let runtime = crate::build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| crate::Error::Runtime("missing ambient test Cx".to_string()))?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = crate::transport::plain_split(stream);
            let mut conn = crate::tests::loopback_connection(read, write);

            let doc = native_json_collection()
                .get_one(&mut conn, &cx, &SodaOperation::default())
                .await
                .expect("empty native JSON query succeeds");
            assert!(doc.is_none());
            assert!(!conn.in_use_cursors.contains(&CURSOR_ID));
            assert!(conn
                .statement_cache
                .iter()
                .any(|entry| entry.cursor_id == CURSOR_ID));
            assert!(conn.cursors_to_close.is_empty());
            Ok::<_, crate::Error>(())
        });

        let sent_define_fetch = server.join().expect("empty JSON server joins")?;
        outcome?;
        assert!(
            !sent_define_fetch,
            "terminal ORA-01403 already proves there is nothing to define-fetch"
        );
        Ok(())
    }

    #[test]
    fn repeated_get_one_releases_query_cursor_ownership() -> crate::Result<()> {
        const CURSOR_ID: u32 = 117;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<()> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;
            for _ in 0..2 {
                read_one_packet(&mut socket)?;
                socket.write_all(&response_packet(&one_shot_document_execute_response(
                    u16::try_from(CURSOR_ID).expect("test cursor id fits u16"),
                    true,
                )))?;
                socket.flush()?;
            }
            Ok(())
        });

        let runtime = crate::build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| crate::Error::Runtime("missing ambient test Cx".to_string()))?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = crate::transport::plain_split(stream);
            let mut conn = crate::tests::loopback_connection(read, write);
            let collection = one_shot_collection();

            for _ in 0..2 {
                let doc = collection
                    .get_one(&mut conn, &cx, &SodaOperation::default())
                    .await
                    .expect("one-shot SODA query succeeds")
                    .expect("one document returned");
                assert_eq!(doc.key.as_deref(), Some("doc-key"));
                assert_eq!(doc.content_bytes.as_deref(), Some(&br#"{"one":1}"#[..]));
            }

            assert!(!conn.in_use_cursors.contains(&CURSOR_ID));
            assert!(conn.copied_cursors.is_empty());
            assert_eq!(
                conn.statement_cache
                    .iter()
                    .filter(|entry| entry.cursor_id == CURSOR_ID)
                    .count(),
                1,
                "repeated one-shot reads must reuse one released cached cursor"
            );
            assert!(conn.cursors_to_close.is_empty());
            Ok::<_, crate::Error>(())
        });

        server.join().expect("one-shot SODA server joins")?;
        outcome
    }

    #[test]
    fn repeated_get_count_releases_query_cursor_ownership() -> crate::Result<()> {
        const CURSOR_ID: u32 = 118;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<()> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;
            for _ in 0..2 {
                read_one_packet(&mut socket)?;
                socket.write_all(&response_packet(&scalar_execute_response(
                    u16::try_from(CURSOR_ID).expect("test cursor id fits u16"),
                    b"COUNT(*)",
                    ORA_TYPE_NUM_NUMBER,
                    &[0xc1, 0x08], // Oracle NUMBER 7
                )))?;
                socket.flush()?;
            }
            Ok(())
        });

        let runtime = crate::build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| crate::Error::Runtime("missing ambient test Cx".to_string()))?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = crate::transport::plain_split(stream);
            let mut conn = crate::tests::loopback_connection(read, write);
            let collection = one_shot_collection();

            for _ in 0..2 {
                assert_eq!(
                    collection
                        .get_count(&mut conn, &cx, &SodaOperation::default())
                        .await
                        .expect("one-shot count succeeds"),
                    7
                );
            }

            assert!(!conn.in_use_cursors.contains(&CURSOR_ID));
            assert!(conn.copied_cursors.is_empty());
            assert_eq!(
                conn.statement_cache
                    .iter()
                    .filter(|entry| entry.cursor_id == CURSOR_ID)
                    .count(),
                1,
                "repeated counts must reuse one released cached cursor"
            );
            assert!(conn.cursors_to_close.is_empty());
            Ok::<_, crate::Error>(())
        });

        server.join().expect("one-shot count server joins")?;
        outcome
    }

    #[test]
    fn get_documents_releases_drained_cursor_ownership() -> crate::Result<()> {
        const CURSOR_ID: u32 = 119;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<()> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;
            read_one_packet(&mut socket)?;
            socket.write_all(&response_packet(&one_shot_document_execute_response(
                u16::try_from(CURSOR_ID).expect("test cursor id fits u16"),
                true,
            )))?;
            socket.flush()
        });

        let runtime = crate::build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| crate::Error::Runtime("missing ambient test Cx".to_string()))?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = crate::transport::plain_split(stream);
            let mut conn = crate::tests::loopback_connection(read, write);

            let docs = one_shot_collection()
                .get_documents(&mut conn, &cx, &SodaOperation::default())
                .await
                .expect("getDocuments drains its cursor");
            assert_eq!(docs.len(), 1);
            assert_eq!(docs[0].key.as_deref(), Some("doc-key"));
            assert_eq!(docs[0].content_bytes.as_deref(), Some(&br#"{"one":1}"#[..]));
            assert!(!conn.in_use_cursors.contains(&CURSOR_ID));
            assert!(conn.copied_cursors.is_empty());
            assert!(conn
                .statement_cache
                .iter()
                .any(|entry| entry.cursor_id == CURSOR_ID));
            assert!(conn.cursors_to_close.is_empty());
            Ok::<_, crate::Error>(())
        });

        server.join().expect("getDocuments server joins")?;
        outcome
    }

    #[test]
    fn get_documents_fetch_failure_stays_retired() -> crate::Result<()> {
        const CURSOR_ID: u32 = 123;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<()> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;
            read_one_packet(&mut socket)?;
            socket.write_all(&response_packet(&one_shot_document_execute_response(
                u16::try_from(CURSOR_ID).expect("test cursor id fits u16"),
                false,
            )))?;
            socket.flush()?;
            read_one_packet(&mut socket)?;
            socket.write_all(&response_packet(&[0xff]))?;
            socket.flush()
        });

        let runtime = crate::build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| crate::Error::Runtime("missing ambient test Cx".to_string()))?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = crate::transport::plain_split(stream);
            let mut conn = crate::tests::loopback_connection(read, write);

            let err = one_shot_collection()
                .get_documents(&mut conn, &cx, &SodaOperation::default())
                .await
                .expect_err("malformed continuation fetch must fail");
            assert!(matches!(err, SodaError::Driver(_)), "{err:?}");
            assert!(!conn.in_use_cursors.contains(&CURSOR_ID));
            assert!(conn
                .statement_cache
                .iter()
                .all(|entry| entry.cursor_id != CURSOR_ID));
            assert_eq!(
                conn.cursors_to_close
                    .iter()
                    .filter(|cursor_id| **cursor_id == CURSOR_ID)
                    .count(),
                1,
                "getDocuments cleanup must not release a failed cursor back to cache"
            );
            Ok::<_, crate::Error>(())
        });

        server.join().expect("failed getDocuments server joins")?;
        outcome
    }

    #[test]
    fn open_cursor_retains_ownership_until_explicit_close() -> crate::Result<()> {
        const CURSOR_ID: u32 = 120;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<()> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;
            read_one_packet(&mut socket)?;
            socket.write_all(&response_packet(&one_shot_document_execute_response(
                u16::try_from(CURSOR_ID).expect("test cursor id fits u16"),
                true,
            )))?;
            socket.flush()
        });

        let runtime = crate::build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| crate::Error::Runtime("missing ambient test Cx".to_string()))?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = crate::transport::plain_split(stream);
            let mut conn = crate::tests::loopback_connection(read, write);

            let mut cursor = one_shot_collection()
                .open_cursor(&mut conn, &cx, &SodaOperation::default())
                .await
                .expect("openCursor succeeds");
            assert!(conn.in_use_cursors.contains(&CURSOR_ID));
            assert!(cursor
                .next_doc(&mut conn, &cx)
                .await
                .expect("first document decodes")
                .is_some());
            assert!(cursor
                .next_doc(&mut conn, &cx)
                .await
                .expect("cursor reaches end of data")
                .is_none());
            assert!(
                conn.in_use_cursors.contains(&CURSOR_ID),
                "draining a caller-owned cursor does not close it implicitly"
            );

            cursor
                .close(&mut conn, &cx)
                .await
                .expect("explicit close succeeds");
            assert!(cursor.is_closed());
            assert!(!conn.in_use_cursors.contains(&CURSOR_ID));
            assert!(conn
                .statement_cache
                .iter()
                .any(|entry| entry.cursor_id == CURSOR_ID));
            Ok::<_, crate::Error>(())
        });

        server.join().expect("openCursor server joins")?;
        outcome
    }

    #[test]
    fn repeated_database_reads_release_query_cursor_ownership() -> crate::Result<()> {
        const OPEN_CURSOR_ID: u32 = 121;
        const LIST_CURSOR_ID: u32 = 122;
        const DESCRIPTOR: &[u8] = br#"{"tableName":"OneShot","keyColumn":{"name":"KEY","sqlType":"VARCHAR2","assignmentMethod":"CLIENT"},"contentColumn":{"name":"DOC","sqlType":"VARCHAR2"}}"#;

        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<()> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;
            for _ in 0..2 {
                read_one_packet(&mut socket)?;
                socket.write_all(&response_packet(&scalar_execute_response(
                    u16::try_from(OPEN_CURSOR_ID).expect("test cursor id fits u16"),
                    b"JSON_DESCRIPTOR",
                    ORA_TYPE_NUM_VARCHAR,
                    DESCRIPTOR,
                )))?;
                socket.flush()?;
            }
            for _ in 0..2 {
                read_one_packet(&mut socket)?;
                socket.write_all(&response_packet(&scalar_execute_response(
                    u16::try_from(LIST_CURSOR_ID).expect("test cursor id fits u16"),
                    b"URI_NAME",
                    ORA_TYPE_NUM_VARCHAR,
                    b"OneShot",
                )))?;
                socket.flush()?;
            }
            Ok(())
        });

        let runtime = crate::build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| crate::Error::Runtime("missing ambient test Cx".to_string()))?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = crate::transport::plain_split(stream);
            let mut conn = crate::tests::loopback_connection(read, write);
            let database = SodaDatabase::new();

            for _ in 0..2 {
                let opened = database
                    .open_collection(&mut conn, &cx, "OneShot")
                    .await
                    .expect("openCollection succeeds")
                    .expect("collection exists");
                assert_eq!(opened.name(), "OneShot");
                assert_eq!(opened.metadata().content_sql_type, ContentSqlType::Varchar2);
            }
            for _ in 0..2 {
                assert_eq!(
                    database
                        .get_collection_names(&mut conn, &cx, None, 0)
                        .await
                        .expect("getCollectionNames succeeds"),
                    vec!["OneShot".to_string()]
                );
            }

            assert!(!conn.in_use_cursors.contains(&OPEN_CURSOR_ID));
            assert!(!conn.in_use_cursors.contains(&LIST_CURSOR_ID));
            assert!(conn.copied_cursors.is_empty());
            for cursor_id in [OPEN_CURSOR_ID, LIST_CURSOR_ID] {
                assert_eq!(
                    conn.statement_cache
                        .iter()
                        .filter(|entry| entry.cursor_id == cursor_id)
                        .count(),
                    1,
                    "each repeated database query reuses its released cursor"
                );
            }
            assert!(conn.cursors_to_close.is_empty());
            Ok::<_, crate::Error>(())
        });

        server.join().expect("database read server joins")?;
        outcome
    }

    #[test]
    fn open_collection_releases_cursor_before_local_decode_error() -> crate::Result<()> {
        const CURSOR_ID: u32 = 124;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<()> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;
            read_one_packet(&mut socket)?;
            socket.write_all(&response_packet(&scalar_execute_response(
                u16::try_from(CURSOR_ID).expect("test cursor id fits u16"),
                b"JSON_DESCRIPTOR",
                ORA_TYPE_NUM_VARCHAR,
                b"not valid JSON",
            )))?;
            socket.flush()
        });

        let runtime = crate::build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| crate::Error::Runtime("missing ambient test Cx".to_string()))?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = crate::transport::plain_split(stream);
            let mut conn = crate::tests::loopback_connection(read, write);

            let err = SodaDatabase::new()
                .open_collection(&mut conn, &cx, "Broken")
                .await
                .expect_err("invalid descriptor must fail locally");
            assert!(matches!(err, SodaError::InvalidMetadata(_)), "{err:?}");
            assert!(!conn.in_use_cursors.contains(&CURSOR_ID));
            assert!(conn
                .statement_cache
                .iter()
                .any(|entry| entry.cursor_id == CURSOR_ID));
            assert!(conn.copied_cursors.is_empty());
            assert!(conn.cursors_to_close.is_empty());
            Ok::<_, crate::Error>(())
        });

        server.join().expect("invalid descriptor server joins")?;
        outcome
    }

    #[test]
    fn unlimited_collection_names_fetches_beyond_initial_prefetch() -> crate::Result<()> {
        const CURSOR_ID: u32 = 125;
        let expected: Vec<String> = (0..=100).map(|n| format!("Collection{n:03}")).collect();
        let initial = expected[..100].to_vec();
        let continuation = expected[100..].to_vec();

        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<()> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;
            read_one_packet(&mut socket)?;
            socket.write_all(&response_packet(&collection_name_rows_response(
                u16::try_from(CURSOR_ID).expect("test cursor id fits u16"),
                &initial,
                true,
                false,
            )))?;
            socket.flush()?;
            read_one_packet(&mut socket)?;
            socket.write_all(&response_packet(&collection_name_rows_response(
                u16::try_from(CURSOR_ID).expect("test cursor id fits u16"),
                &continuation,
                false,
                true,
            )))?;
            socket.flush()
        });

        let runtime = crate::build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| crate::Error::Runtime("missing ambient test Cx".to_string()))?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = crate::transport::plain_split(stream);
            let mut conn = crate::tests::loopback_connection(read, write);

            let names = SodaDatabase::new()
                .get_collection_names(&mut conn, &cx, None, 0)
                .await
                .expect("unlimited collection-name query succeeds");
            assert_eq!(names, expected, "limit=0 must consume every fetch page");
            assert!(!conn.in_use_cursors.contains(&CURSOR_ID));
            assert!(conn
                .statement_cache
                .iter()
                .any(|entry| entry.cursor_id == CURSOR_ID));
            Ok::<_, crate::Error>(())
        });

        server.join().expect("unlimited names server joins")?;
        outcome
    }

    #[test]
    fn collection_names_failed_continuation_retires_cursor_once() -> crate::Result<()> {
        const CURSOR_ID: u32 = 126;
        let initial: Vec<String> = (0..100).map(|n| format!("Collection{n:03}")).collect();

        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<()> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;
            read_one_packet(&mut socket)?;
            socket.write_all(&response_packet(&collection_name_rows_response(
                u16::try_from(CURSOR_ID).expect("test cursor id fits u16"),
                &initial,
                true,
                false,
            )))?;
            socket.flush()?;
            read_one_packet(&mut socket)?;
            socket.write_all(&response_packet(&[0xff]))?;
            socket.flush()
        });

        let runtime = crate::build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| crate::Error::Runtime("missing ambient test Cx".to_string()))?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = crate::transport::plain_split(stream);
            let mut conn = crate::tests::loopback_connection(read, write);

            let err = SodaDatabase::new()
                .get_collection_names(&mut conn, &cx, None, 0)
                .await
                .expect_err("malformed continuation fetch must fail");
            assert!(matches!(err, SodaError::Driver(_)), "{err:?}");
            assert!(!conn.in_use_cursors.contains(&CURSOR_ID));
            assert!(conn
                .statement_cache
                .iter()
                .all(|entry| entry.cursor_id != CURSOR_ID));
            assert_eq!(
                conn.cursors_to_close
                    .iter()
                    .filter(|cursor_id| **cursor_id == CURSOR_ID)
                    .count(),
                1,
                "failed pagination must queue one fail-closed cursor retirement"
            );
            Ok::<_, crate::Error>(())
        });

        server.join().expect("failed names server joins")?;
        outcome
    }

    #[test]
    fn collection_names_precancel_releases_without_fetch() -> crate::Result<()> {
        const CURSOR_ID: u32 = 127;
        const SQL: &str = "select collection names cancellation probe";
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<Option<u8>> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_millis(300)))?;
            let mut byte = [0u8; 1];
            match socket.read(&mut byte) {
                Ok(0) => Ok(None),
                Ok(_) => Ok(Some(byte[0])),
                Err(err)
                    if matches!(
                        err.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    Ok(None)
                }
                Err(err) => Err(err),
            }
        });

        let runtime = crate::build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| crate::Error::Runtime("missing ambient test Cx".to_string()))?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = crate::transport::plain_split(stream);
            let mut conn = crate::tests::loopback_connection(read, write);
            let columns = vec![ColumnMetadata::new("URI_NAME", ORA_TYPE_NUM_VARCHAR)];
            conn.statement_cache_put(SQL, CURSOR_ID, Vec::new());
            conn.in_use_cursors.insert(CURSOR_ID);
            conn.cursor_columns.insert(CURSOR_ID, columns.clone());

            cx.cancel_fast(CancelKind::User);
            let err = collect_owned_query_result(
                &mut conn,
                &cx,
                100,
                QueryResult {
                    columns,
                    rows: vec![vec![Some(QueryValue::Text("Collection000".to_string()))]],
                    cursor_id: CURSOR_ID,
                    more_rows: true,
                    ..QueryResult::default()
                },
            )
            .await
            .expect_err("pending cancellation must stop before FETCH");
            assert!(matches!(err, SodaError::Driver(crate::Error::Cancelled)));
            assert!(!conn.in_use_cursors.contains(&CURSOR_ID));
            assert!(conn
                .statement_cache
                .iter()
                .any(|entry| entry.cursor_id == CURSOR_ID));
            assert!(conn.cursors_to_close.is_empty());
            Ok::<_, crate::Error>(())
        });

        assert_eq!(
            server.join().expect("pre-cancel names server joins")?,
            None,
            "a cancellation before continuation must not write FETCH"
        );
        outcome
    }

    #[test]
    fn mixed_case_descriptor_columns_are_quoted_in_insert_and_returning_sql() {
        let collection = mixed_case_collection();
        let (sql, binds, layout) = collection
            .build_insert_sql(&document(), None, true)
            .expect("build insert");

        assert!(sql.contains("INSERT INTO \"MixedCollection\""), "{sql}");
        assert!(sql.contains("\"JsonDoc\""), "{sql}");
        assert!(sql.contains("\"CamelKey\""), "{sql}");
        assert!(sql.contains("\"DocVersion\""), "{sql}");
        assert!(sql.contains("\"CreatedAt\""), "{sql}");
        assert!(sql.contains("\"LastModifiedAt\""), "{sql}");
        assert!(sql.contains("\"MimeType\""), "{sql}");
        assert!(sql.contains("RETURNING \"CamelKey\""), "{sql}");
        assert!(sql.contains("\"DocVersion\""), "{sql}");
        assert!(sql.contains("TO_CHAR(\"CreatedAt\""), "{sql}");
        assert!(sql.contains("TO_CHAR(\"LastModifiedAt\""), "{sql}");
        assert_eq!(binds.len(), 3);
        assert_eq!(layout.bind_count, 4);
    }

    #[test]
    fn mixed_case_descriptor_columns_are_quoted_in_replace_and_returning_sql() {
        let collection = mixed_case_collection();
        let op = SodaOperation {
            key: Some("client-key".into()),
            version: Some("doc-version".into()),
            ..Default::default()
        };
        let (sql, binds, layout) = collection
            .build_replace_sql(&op, &document(), true)
            .expect("build replace");

        assert!(sql.contains("UPDATE \"MixedCollection\" SET"), "{sql}");
        assert!(sql.contains("\"JsonDoc\" = :1"), "{sql}");
        assert!(sql.contains("\"LastModifiedAt\" = SYSTIMESTAMP"), "{sql}");
        assert!(sql.contains("\"DocVersion\" = SYS_GUID()"), "{sql}");
        assert!(sql.contains("WHERE \"CamelKey\" = :2"), "{sql}");
        assert!(sql.contains("\"DocVersion\" = :3"), "{sql}");
        assert!(sql.contains("RETURNING \"CamelKey\""), "{sql}");
        assert!(sql.contains("\"DocVersion\""), "{sql}");
        assert!(sql.contains("TO_CHAR(\"CreatedAt\""), "{sql}");
        assert!(sql.contains("TO_CHAR(\"LastModifiedAt\""), "{sql}");
        assert_eq!(binds.len(), 3);
        assert_eq!(layout.expect("returning layout").bind_count, 4);
    }

    #[test]
    fn replace_rejects_invalid_native_raw_key_and_version() {
        let mut collection = native_json_collection();
        collection.metadata.version_column = Some("ETAG".to_string());
        let doc = document();

        let invalid_key = SodaOperation {
            key: Some("AAzzBB".to_string()),
            ..Default::default()
        };
        assert!(matches!(
            collection.build_replace_sql(&invalid_key, &doc, false),
            Err(SodaError::InvalidArgument(_))
        ));

        let invalid_version = SodaOperation {
            key: Some("AABB".to_string()),
            version: Some("odd".to_string()),
            ..Default::default()
        };
        assert!(matches!(
            collection.build_replace_sql(&invalid_version, &doc, false),
            Err(SodaError::InvalidArgument(_))
        ));
    }

    #[test]
    fn replace_never_ignores_a_requested_version() {
        let collection = native_json_collection();
        let op = SodaOperation {
            key: Some("AABB".to_string()),
            version: Some("CCDD".to_string()),
            ..Default::default()
        };

        let err = collection
            .build_replace_sql(&op, &document(), false)
            .expect_err("optimistic locking requires a version column");
        assert!(matches!(err, SodaError::NotSupported(_)), "{err:?}");
    }
}

// --- OSON <-> serde_json bridges ------------------------------------------

/// Convert an OsonValue into a serde_json value (for serializing native content
/// to JSON text for a BLOB column).
pub(crate) fn oson_to_json(v: &OsonValue) -> serde_json::Value {
    match v {
        OsonValue::Null => serde_json::Value::Null,
        OsonValue::Bool(b) => serde_json::Value::Bool(*b),
        OsonValue::Number(n) => n
            .parse::<serde_json::Number>()
            .map(serde_json::Value::Number)
            .unwrap_or_else(|_| serde_json::Value::String(n.clone())),
        OsonValue::BinaryFloat(f) => serde_json::Number::from_f64(f64::from(*f))
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        OsonValue::BinaryDouble(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        OsonValue::String(s) => serde_json::Value::String(s.clone()),
        OsonValue::Raw(bytes) => serde_json::Value::String(hex_encode(bytes)),
        OsonValue::DateTime { .. } => serde_json::Value::String(format!("{v:?}")),
        OsonValue::IntervalDS { .. } => serde_json::Value::String(format!("{v:?}")),
        OsonValue::Vector(_) => serde_json::Value::Null,
        OsonValue::Array(a) => serde_json::Value::Array(a.iter().map(oson_to_json).collect()),
        OsonValue::Object(o) => serde_json::Value::Object(
            o.iter()
                .map(|(k, v)| (k.clone(), oson_to_json(v)))
                .collect(),
        ),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// True when any column needs a client-side define to stream its value
/// (`CLOB` / `BLOB` / `VECTOR` / native `JSON`). Such columns come back from the
/// initial execute as describe-only metadata; the value is delivered on a
/// follow-up define-fetch round trip. Mirrors the crate-private
/// `columns_require_define`.
fn columns_require_define(columns: &[ColumnMetadata]) -> bool {
    columns.iter().any(|c| {
        matches!(
            c.ora_type_num(),
            ORA_TYPE_NUM_CLOB | ORA_TYPE_NUM_BLOB | ORA_TYPE_NUM_VECTOR | ORA_TYPE_NUM_JSON
        )
    })
}

/// Finish an operation on an already-open SODA cursor. Once a fetch or define
/// operation fails, the server-side cursor state is no longer proven valid, so
/// it must be evicted from every local registry and queued for close instead of
/// being returned to the statement cache.
pub(super) fn finish_cursor_operation<T>(
    conn: &mut Connection,
    cursor_id: u32,
    result: crate::Result<T>,
) -> Result<T> {
    conn.close_cursor_on_error(cursor_id, result)
        .map_err(SodaError::Driver)
}

/// Execute a parameterised query and, if the result projects columns that
/// require a client-side define (native JSON / LOB / VECTOR), perform the
/// define-fetch round trip so the values are actually delivered.
///
/// `crate::Connection::execute_query_collect` does this for bind-free queries;
/// SODA needs the same behaviour with binds, which this helper provides.
pub(crate) async fn execute_collect_with_binds(
    conn: &mut Connection,
    cx: &Cx,
    sql: &str,
    prefetch_rows: u32,
    binds: &[BindValue],
) -> Result<QueryResult> {
    let mut result = execute_with_binds_raw(conn, cx, sql, prefetch_rows, binds).await?;
    if !columns_require_define(&result.columns) || result.cursor_id == 0 || !result.more_rows {
        return Ok(result);
    }
    if !result.rows.is_empty() {
        return Ok(result);
    }
    let cursor_id = result.cursor_id;
    let columns = result.columns.clone();
    let fetched_result = conn
        .define_and_fetch_rows_with_columns(cx, cursor_id, prefetch_rows.max(1), &columns, None)
        .await;
    let fetched = finish_cursor_operation(conn, cursor_id, fetched_result)?;
    result.rows = fetched.rows;
    result.more_rows = fetched.more_rows;
    if !fetched.columns.is_empty() {
        result.columns = fetched.columns;
    }
    if result.cursor_id == 0 {
        result.cursor_id = cursor_id;
    }
    Ok(result)
}

pub(crate) async fn execute_raw(
    conn: &mut Connection,
    cx: &Cx,
    sql: &str,
    prefetch_rows: u32,
) -> Result<QueryResult> {
    execute_with_bind_rows_raw(conn, cx, sql, prefetch_rows, &[]).await
}

pub(crate) async fn execute_with_binds_raw(
    conn: &mut Connection,
    cx: &Cx,
    sql: &str,
    prefetch_rows: u32,
    binds: &[BindValue],
) -> Result<QueryResult> {
    let bind_rows = if binds.is_empty() {
        Vec::new()
    } else {
        vec![binds.to_vec()]
    };
    execute_with_bind_rows_raw(conn, cx, sql, prefetch_rows, &bind_rows).await
}

pub(crate) async fn execute_with_bind_rows_raw(
    conn: &mut Connection,
    cx: &Cx,
    sql: &str,
    prefetch_rows: u32,
    bind_rows: &[Vec<BindValue>],
) -> Result<QueryResult> {
    conn.execute_query_with_bind_rows_and_options_core(
        cx,
        sql,
        prefetch_rows,
        bind_rows,
        crate::ExecuteOptions::default(),
    )
    .await
    .map_err(SodaError::Driver)
}
