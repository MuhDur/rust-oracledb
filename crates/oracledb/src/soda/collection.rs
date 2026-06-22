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
        while let Some(doc) = cursor.next_doc(conn, cx).await? {
            out.push(doc);
        }
        Ok(out)
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
            binds.push(BindValue::Raw(operation::hex_decode(key)));
        } else {
            binds.push(BindValue::Text(key.clone()));
        }
        let mut where_clause = format!("{} = :{next_bind}", quote_ident(&meta.key_column));
        next_bind += 1;
        if let Some(version) = &op.version {
            if let Some(vc) = &meta.version_column {
                if matches!(meta.version_method, VersionMethod::None) && meta.native {
                    binds.push(BindValue::Raw(operation::hex_decode(version)));
                } else {
                    binds.push(BindValue::Text(version.clone()));
                }
                where_clause.push_str(&format!(" AND {} = :{next_bind}", quote_ident(vc)));
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::soda::metadata::KeyAssignment;

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
    if !columns_require_define(&result.columns) || result.cursor_id == 0 {
        return Ok(result);
    }
    if !result.rows.is_empty() {
        return Ok(result);
    }
    let cursor_id = result.cursor_id;
    let columns = result.columns.clone();
    let fetched = conn
        .define_and_fetch_rows_with_columns(cx, cursor_id, prefetch_rows.max(1), &columns, None)
        .await
        .map_err(SodaError::Driver)?;
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
