//! SODA find/operation criteria and the SQL builders that turn them into
//! parameterised statements over a collection's backing table.

use oracledb_protocol::thin::BindValue;

use super::error::{Result, SodaError};
use super::metadata::{quote_ident, KeyAssignment, SodaCollectionMetadata};
use super::qbe;

/// Criteria accumulated by a `find()` chain.
#[derive(Debug, Clone, Default)]
pub struct SodaOperation {
    /// Single key filter.
    pub key: Option<String>,
    /// Multiple keys filter.
    pub keys: Option<Vec<String>>,
    /// QBE filter as a raw JSON string.
    pub filter: Option<String>,
    /// Version filter (optimistic locking).
    pub version: Option<String>,
    /// Number of matching docs to skip.
    pub skip: Option<u64>,
    /// Max number of docs to return.
    pub limit: Option<u64>,
    /// Internal fetch batch size (default 100).
    pub fetch_array_size: u32,
    /// SQL hint string (no comment markers).
    pub hint: Option<String>,
    /// SELECT ... FOR UPDATE.
    pub lock: bool,
}

impl SodaOperation {
    /// Default fetch array size when unset / zero.
    pub const DEFAULT_FETCH_ARRAY_SIZE: u32 = 100;

    pub fn fetch_array_size(&self) -> u32 {
        if self.fetch_array_size == 0 {
            Self::DEFAULT_FETCH_ARRAY_SIZE
        } else {
            self.fetch_array_size
        }
    }

    /// Build the WHERE clause + ordered binds for this operation.
    ///
    /// Precedence matches python-oracledb: `key` and `keys` are exclusive with
    /// each other; a `filter` may be combined with neither (SODA treats key/
    /// filter as alternatives — key wins if both are set, mirroring the
    /// reference which ignores filter once key/keys is chosen).
    fn build_where(&self, meta: &SodaCollectionMetadata) -> Result<(String, Vec<BindValue>)> {
        let mut binds = Vec::new();
        let mut clauses: Vec<String> = Vec::new();

        if let Some(key) = &self.key {
            clauses.push(key_predicate(meta, &mut binds, key));
            if let Some(version) = &self.version {
                clauses.push(version_predicate(meta, &mut binds, version)?);
            }
        } else if let Some(keys) = &self.keys {
            if keys.is_empty() {
                // No keys -> match nothing.
                clauses.push("1=0".to_string());
            } else {
                let mut placeholders = Vec::new();
                for k in keys {
                    placeholders.push(key_bind_placeholder(meta, &mut binds, k));
                }
                clauses.push(format!(
                    "{} IN ({})",
                    quote_ident(&meta.key_column),
                    placeholders.join(", ")
                ));
            }
        } else if let Some(filter) = &self.filter {
            let value: serde_json::Value = serde_json::from_str(filter)
                .map_err(|e| SodaError::Qbe(format!("invalid filter JSON: {e}")))?;
            let frag = qbe::qbe_to_where_clause(&value, &quote_ident(&meta.content_column))?;
            clauses.push(frag);
        }

        if clauses.is_empty() {
            clauses.push("1=1".to_string());
        }
        Ok((clauses.join(" AND "), binds))
    }

    /// Extract an ORDER BY fragment from the filter's `$orderby`, if any.
    fn order_by(&self, meta: &SodaCollectionMetadata) -> Result<Option<String>> {
        let Some(filter) = &self.filter else {
            return Ok(None);
        };
        let value: serde_json::Value = serde_json::from_str(filter)
            .map_err(|e| SodaError::Qbe(format!("invalid filter JSON: {e}")))?;
        qbe::extract_orderby(&value, &quote_ident(&meta.content_column))
    }

    /// Build the SELECT for fetching documents (content + metadata columns).
    ///
    /// Column order is fixed: key, content, version, created_on, last_modified,
    /// media_type — each present only if the collection has that column.
    pub fn build_select_sql(
        &self,
        meta: &SodaCollectionMetadata,
    ) -> Result<(String, Vec<BindValue>, SelectColumns)> {
        let (where_clause, binds) = self.build_where(meta)?;
        let (cols, layout) = select_column_list(meta);

        let mut sql = String::from("SELECT ");
        if let Some(hint) = &self.hint {
            sql.push_str(&format!("/*+ {hint} */ "));
        }
        sql.push_str(&cols);
        sql.push_str(&format!(
            " FROM {} WHERE {}",
            meta.quoted_table(),
            where_clause
        ));

        if let Some(order) = self.order_by(meta)? {
            sql.push_str(&format!(" ORDER BY {order}"));
        }

        // OFFSET / FETCH NEXT for skip/limit.
        if let Some(skip) = self.skip {
            sql.push_str(&format!(" OFFSET {skip} ROWS"));
        }
        if let Some(limit) = self.limit {
            sql.push_str(&format!(" FETCH NEXT {limit} ROWS ONLY"));
        }

        if self.lock {
            sql.push_str(" FOR UPDATE");
        }

        Ok((sql, binds, layout))
    }

    /// Build a COUNT(*) statement. Errors if skip/limit are set (matches
    /// ORA-40748 behaviour).
    pub fn build_count_sql(
        &self,
        meta: &SodaCollectionMetadata,
    ) -> Result<(String, Vec<BindValue>)> {
        if self.skip.is_some() || self.limit.is_some() {
            return Err(SodaError::Driver(crate::Error::Protocol(
                oracledb_protocol::ProtocolError::ServerError(
                    "ORA-40748: SKIP and LIMIT cannot be specified with count operation"
                        .to_string(),
                ),
            )));
        }
        let (where_clause, binds) = self.build_where(meta)?;
        let sql = format!(
            "SELECT COUNT(*) FROM {} WHERE {}",
            meta.quoted_table(),
            where_clause
        );
        Ok((sql, binds))
    }

    /// Build a DELETE statement for remove().
    pub fn build_delete_sql(
        &self,
        meta: &SodaCollectionMetadata,
    ) -> Result<(String, Vec<BindValue>)> {
        let (where_clause, binds) = self.build_where(meta)?;
        let sql = format!("DELETE FROM {} WHERE {}", meta.quoted_table(), where_clause);
        Ok((sql, binds))
    }
}

/// Which metadata columns the SELECT projects, and in what order, so the row
/// decoder knows how to map cells back to a [`super::document::SodaDocument`].
#[derive(Debug, Clone)]
pub struct SelectColumns {
    pub key_idx: usize,
    pub content_idx: usize,
    pub version_idx: Option<usize>,
    pub created_idx: Option<usize>,
    pub last_modified_idx: Option<usize>,
    pub media_type_idx: Option<usize>,
}

/// Build the projection list and its column layout.
pub(crate) fn select_column_list(meta: &SodaCollectionMetadata) -> (String, SelectColumns) {
    let mut cols: Vec<String> = Vec::new();
    let mut next = 0usize;
    let take = |expr: String, cols: &mut Vec<String>, next: &mut usize| -> usize {
        cols.push(expr);
        let i = *next;
        *next += 1;
        i
    };

    // Key: for RAW keys (native embedded OID) render as hex string so the key
    // round-trips as a portable string identifier.
    let key_expr = if meta.key_sql_type.eq_ignore_ascii_case("RAW") {
        format!("RAWTOHEX({})", quote_ident(&meta.key_column))
    } else {
        quote_ident(&meta.key_column)
    };
    let key_idx = take(key_expr, &mut cols, &mut next);

    // Content projection: native JSON comes back inline. BLOB/CLOB-stored JSON
    // is a LOB locator on a plain SELECT, so we serialize it to inline text via
    // JSON_SERIALIZE(... RETURNING VARCHAR2) — small SODA documents fit, and the
    // row decoder treats the text as JSON content bytes. This is only valid for
    // JSON-only collections; a media-type column means non-JSON content may be
    // present, which JSON_SERIALIZE would reject (those need a raw LOB read,
    // which is a documented thin-SODA gap).
    let json_only = meta.media_type_column.is_none();
    let content_expr = match meta.content_sql_type {
        super::metadata::ContentSqlType::Blob | super::metadata::ContentSqlType::Clob
            if json_only =>
        {
            format!(
                "JSON_SERIALIZE({} RETURNING VARCHAR2(32767))",
                quote_ident(&meta.content_column)
            )
        }
        _ => quote_ident(&meta.content_column),
    };
    let content_idx = take(content_expr, &mut cols, &mut next);

    let version_idx = meta.version_column.as_ref().map(|c| {
        let expr =
            if matches!(meta.version_method, super::metadata::VersionMethod::None) && meta.native {
                // ETAG is RAW; project as hex string.
                format!("RAWTOHEX({})", quote_ident(c))
            } else {
                quote_ident(c)
            };
        take(expr, &mut cols, &mut next)
    });
    let created_idx = meta
        .creation_time_column
        .as_ref()
        .map(|c| take(timestamp_iso(&quote_ident(c)), &mut cols, &mut next));
    let last_modified_idx = meta
        .last_modified_column
        .as_ref()
        .map(|c| take(timestamp_iso(&quote_ident(c)), &mut cols, &mut next));
    let media_type_idx = meta
        .media_type_column
        .as_ref()
        .map(|c| take(quote_ident(c), &mut cols, &mut next));

    (
        cols.join(", "),
        SelectColumns {
            key_idx,
            content_idx,
            version_idx,
            created_idx,
            last_modified_idx,
            media_type_idx,
        },
    )
}

/// Render a timestamp column as an ISO-8601 string.
fn timestamp_iso(col: &str) -> String {
    format!("TO_CHAR({col}, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6')")
}

/// Build a key equality predicate, pushing the bind for the key value.
fn key_predicate(meta: &SodaCollectionMetadata, binds: &mut Vec<BindValue>, key: &str) -> String {
    let placeholder = key_bind_placeholder(meta, binds, key);
    format!("{} = {}", quote_ident(&meta.key_column), placeholder)
}

/// Push a bind for a key value and return the SQL placeholder expression.
///
/// RAW keys (native embedded OID collections) are stored as a `RAW` column. We
/// bind the decoded bytes as a `RAW` value and compare `RESID = :n` directly:
/// binding the hex string and wrapping the placeholder in `HEXTORAW(:n)` does
/// NOT match in the WHERE clause on this server (the function is not folded to a
/// constant, so the implicit conversion silently fails to match), whereas a
/// `RAW` bind matches exactly.
fn key_bind_placeholder(
    meta: &SodaCollectionMetadata,
    binds: &mut Vec<BindValue>,
    key: &str,
) -> String {
    let n = binds.len() + 1;
    if meta.key_sql_type.eq_ignore_ascii_case("RAW") {
        binds.push(BindValue::Raw(hex_decode(key)));
    } else {
        binds.push(BindValue::Text(key.to_string()));
    }
    format!(":{n}")
}

/// Decode a hex string into bytes. Invalid pairs are skipped defensively (the
/// key always originates from `RAWTOHEX`, so it is well-formed in practice).
pub(crate) fn hex_decode(s: &str) -> Vec<u8> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut i = 0;
    while i + 1 < bytes.len() {
        let hi = (bytes[i] as char).to_digit(16);
        let lo = (bytes[i + 1] as char).to_digit(16);
        if let (Some(h), Some(l)) = (hi, lo) {
            out.push((h * 16 + l) as u8);
        }
        i += 2;
    }
    out
}

/// Build a version equality predicate.
fn version_predicate(
    meta: &SodaCollectionMetadata,
    binds: &mut Vec<BindValue>,
    version: &str,
) -> Result<String> {
    let col = meta
        .version_column
        .as_ref()
        .ok_or_else(|| SodaError::NotSupported("collection has no version column".to_string()))?;
    let n = binds.len() + 1;
    if matches!(meta.version_method, super::metadata::VersionMethod::None) && meta.native {
        // ETAG RAW: bind the decoded bytes (see key_bind_placeholder).
        binds.push(BindValue::Raw(hex_decode(version)));
    } else {
        binds.push(BindValue::Text(version.to_string()));
    }
    Ok(format!("{} = :{n}", quote_ident(col)))
}

/// True if the collection's key is client-assigned.
#[allow(dead_code)]
pub(crate) fn key_is_client_assigned(meta: &SodaCollectionMetadata) -> bool {
    matches!(meta.key_assignment, KeyAssignment::Client)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::soda::metadata::{ContentSqlType, VersionMethod};

    fn native_meta() -> SodaCollectionMetadata {
        SodaCollectionMetadata {
            table_name: "MYCOLL".into(),
            schema_name: Some("PYTHONTEST".into()),
            key_column: "RESID".into(),
            key_sql_type: "RAW".into(),
            key_assignment: KeyAssignment::EmbeddedOid,
            key_path: Some("_id".into()),
            content_column: "DATA".into(),
            content_sql_type: ContentSqlType::Json,
            version_column: Some("ETAG".into()),
            version_method: VersionMethod::None,
            last_modified_column: None,
            creation_time_column: None,
            media_type_column: None,
            read_only: false,
            native: true,
        }
    }

    fn legacy_meta() -> SodaCollectionMetadata {
        SodaCollectionMetadata {
            table_name: "MYLEGACY".into(),
            schema_name: Some("PYTHONTEST".into()),
            key_column: "ID".into(),
            key_sql_type: "VARCHAR2".into(),
            key_assignment: KeyAssignment::Uuid,
            key_path: None,
            content_column: "JSON_DOCUMENT".into(),
            content_sql_type: ContentSqlType::Blob,
            version_column: Some("VERSION".into()),
            version_method: VersionMethod::Uuid,
            last_modified_column: Some("LAST_MODIFIED".into()),
            creation_time_column: Some("CREATED_ON".into()),
            media_type_column: None,
            read_only: false,
            native: false,
        }
    }

    fn mixed_case_meta() -> SodaCollectionMetadata {
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
        }
    }

    #[test]
    fn native_select_by_key_uses_raw_bind_and_rawtohex_projection() {
        let op = SodaOperation {
            key: Some("0123ABCD".into()),
            ..Default::default()
        };
        let (sql, binds, layout) = op.build_select_sql(&native_meta()).expect("build select");
        // Key projection is hex text; the WHERE binds the decoded RAW bytes and
        // compares the RAW column directly (HEXTORAW(:bind) does not match).
        assert!(sql.contains("RAWTOHEX(\"RESID\")"), "{sql}");
        assert!(sql.contains("WHERE \"RESID\" = :1"), "{sql}");
        assert!(!sql.contains("HEXTORAW"), "{sql}");
        assert_eq!(binds.len(), 1);
        assert!(matches!(binds[0], BindValue::Raw(_)), "{binds:?}");
        assert_eq!(layout.key_idx, 0);
        assert_eq!(layout.content_idx, 1);
        assert_eq!(layout.version_idx, Some(2));
    }

    #[test]
    fn legacy_select_by_key_plain() {
        let op = SodaOperation {
            key: Some("uuid-key".into()),
            ..Default::default()
        };
        let (sql, binds, layout) = op.build_select_sql(&legacy_meta()).expect("build select");
        assert!(sql.contains("WHERE \"ID\" = :1"), "{sql}");
        assert!(sql.contains("FROM \"MYLEGACY\""), "{sql}");
        assert_eq!(binds.len(), 1);
        // legacy has created + last_modified columns
        assert_eq!(layout.created_idx, Some(3));
        assert_eq!(layout.last_modified_idx, Some(4));
    }

    #[test]
    fn select_by_keys_in_list() {
        let op = SodaOperation {
            keys: Some(vec!["a".into(), "b".into(), "c".into()]),
            ..Default::default()
        };
        let (sql, binds, _) = op.build_select_sql(&legacy_meta()).expect("build select");
        assert!(sql.contains("\"ID\" IN (:1, :2, :3)"), "{sql}");
        assert_eq!(binds.len(), 3);
    }

    #[test]
    fn select_with_filter() {
        let op = SodaOperation {
            filter: Some(r#"{"age": {"$gt": 18}}"#.into()),
            ..Default::default()
        };
        let (sql, binds, _) = op.build_select_sql(&legacy_meta()).expect("build select");
        assert!(
            sql.contains("JSON_EXISTS(\"JSON_DOCUMENT\", '$.age?(@ > 18)')"),
            "{sql}"
        );
        assert!(binds.is_empty());
    }

    #[test]
    fn count_rejects_skip_limit() {
        let op = SodaOperation {
            limit: Some(5),
            ..Default::default()
        };
        let err = op
            .build_count_sql(&legacy_meta())
            .expect_err("count with limit should fail");
        // Surfaces as an ORA-40748 server-style error.
        assert!(err.to_string().contains("ORA-40748"), "{err}");
    }

    #[test]
    fn skip_limit_pagination() {
        let op = SodaOperation {
            filter: Some(r#"{"$orderby": [{"path": "name", "order": "desc"}]}"#.into()),
            skip: Some(2),
            limit: Some(1),
            ..Default::default()
        };
        let (sql, _, _) = op.build_select_sql(&legacy_meta()).expect("build select");
        assert!(sql.contains("ORDER BY"), "{sql}");
        assert!(sql.contains("OFFSET 2 ROWS"), "{sql}");
        assert!(sql.contains("FETCH NEXT 1 ROWS ONLY"), "{sql}");
    }

    #[test]
    fn delete_by_filter() {
        let op = SodaOperation {
            filter: Some(r#"{"name": {"$like": "John%"}}"#.into()),
            ..Default::default()
        };
        let (sql, _, _) = op.build_select_sql(&legacy_meta()).expect("build select");
        assert!(sql.contains("@ like \"John%\""), "{sql}");
        let (dsql, _) = op.build_delete_sql(&legacy_meta()).expect("build delete");
        assert!(dsql.starts_with("DELETE FROM \"MYLEGACY\" WHERE"), "{dsql}");
    }

    #[test]
    fn lock_adds_for_update() {
        let op = SodaOperation {
            lock: true,
            ..Default::default()
        };
        let (sql, _, _) = op.build_select_sql(&legacy_meta()).expect("build select");
        assert!(sql.ends_with("FOR UPDATE"), "{sql}");
    }

    #[test]
    fn version_predicate_on_key() {
        let op = SodaOperation {
            key: Some("uuid-key".into()),
            version: Some("v1".into()),
            ..Default::default()
        };
        let (sql, binds, _) = op.build_select_sql(&legacy_meta()).expect("build select");
        assert!(sql.contains("\"VERSION\" = :2"), "{sql}");
        assert_eq!(binds.len(), 2);
    }

    #[test]
    fn empty_keys_matches_nothing() {
        let op = SodaOperation {
            keys: Some(vec![]),
            ..Default::default()
        };
        let (sql, _) = op.build_count_sql(&legacy_meta()).expect("build count");
        assert!(sql.contains("1=0"), "{sql}");
    }

    #[test]
    fn mixed_case_descriptor_columns_are_quoted_in_operation_sql() {
        let meta = mixed_case_meta();
        let op = SodaOperation {
            key: Some("doc-key".into()),
            version: Some("doc-version".into()),
            filter: Some(r#"{"name": {"$eq": "Ada"}}"#.into()),
            ..Default::default()
        };

        let (sql, binds, _) = op.build_select_sql(&meta).expect("build select");
        assert!(sql.contains("\"CamelKey\""), "{sql}");
        assert!(sql.contains("\"JsonDoc\""), "{sql}");
        assert!(sql.contains("\"DocVersion\""), "{sql}");
        assert!(sql.contains("TO_CHAR(\"CreatedAt\""), "{sql}");
        assert!(sql.contains("TO_CHAR(\"LastModifiedAt\""), "{sql}");
        assert!(sql.contains("\"MimeType\""), "{sql}");
        assert!(sql.contains("WHERE \"CamelKey\" = :1"), "{sql}");
        assert!(sql.contains("\"DocVersion\" = :2"), "{sql}");
        assert_eq!(binds.len(), 2);

        let filter_op = SodaOperation {
            filter: Some(r#"{"name": {"$eq": "Ada"}}"#.into()),
            ..Default::default()
        };
        let (filter_sql, _, _) = filter_op
            .build_select_sql(&meta)
            .expect("build filter select");
        assert!(
            filter_sql.contains("JSON_EXISTS(\"JsonDoc\", '$.name?(@ == \"Ada\")')"),
            "{filter_sql}"
        );
        let (count_sql, _) = filter_op.build_count_sql(&meta).expect("build count");
        assert!(
            count_sql.contains("JSON_EXISTS(\"JsonDoc\", '$.name?(@ == \"Ada\")')"),
            "{count_sql}"
        );
    }
}
