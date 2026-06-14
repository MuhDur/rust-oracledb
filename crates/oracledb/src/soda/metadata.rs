//! SODA collection metadata: the parsed form of the JSON descriptor that
//! `USER_SODA_COLLECTIONS` stores for each collection. This drives SQL
//! generation (which columns to read/write, how keys and versions behave).

use serde_json::Value;

use super::error::{Result, SodaError};

/// How the key column value is assigned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyAssignment {
    /// Server generates a UUID (legacy collections).
    Uuid,
    /// Server sequence.
    Sequence,
    /// Client supplies the key.
    Client,
    /// Native 23ai collection: the key is an embedded OID stored in the JSON
    /// document under the `path` (usually `_id`) and surfaced as the RESID raw
    /// column. The string key is the hex of the RESID raw.
    EmbeddedOid,
    /// GUID assignment (RAW(16) SYS_GUID).
    Guid,
}

/// SQL type of the content column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentSqlType {
    Blob,
    Clob,
    Varchar2,
    /// Native Oracle JSON type (23ai native collections, OSON on the wire).
    Json,
    /// Raw bytes.
    Raw,
}

/// How the version column value is computed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionMethod {
    Uuid,
    Timestamp,
    /// SHA hash of the content (`SHA256` etc.).
    Hash(String),
    Sequential,
    None,
}

/// Parsed metadata for a SODA collection.
#[derive(Debug, Clone)]
pub struct SodaCollectionMetadata {
    /// The backing table name (unquoted, as stored).
    pub table_name: String,
    /// Schema owning the table, if recorded.
    pub schema_name: Option<String>,
    /// Key column name (e.g. `ID` or `RESID`).
    pub key_column: String,
    /// Key column SQL type (`VARCHAR2`, `RAW`, `NUMBER`).
    pub key_sql_type: String,
    /// How keys are assigned.
    pub key_assignment: KeyAssignment,
    /// For embedded-OID keys, the JSON path the key lives under (e.g. `_id`).
    pub key_path: Option<String>,
    /// Content column name (e.g. `DATA` or `JSON_DOCUMENT`).
    pub content_column: String,
    /// Content column SQL type.
    pub content_sql_type: ContentSqlType,
    /// Version column name, if present.
    pub version_column: Option<String>,
    /// Version method.
    pub version_method: VersionMethod,
    /// Last-modified timestamp column name, if present.
    pub last_modified_column: Option<String>,
    /// Creation-time timestamp column name, if present.
    pub creation_time_column: Option<String>,
    /// Media-type column name, if present (mixed-media collections).
    pub media_type_column: Option<String>,
    /// Whether the collection is read-only.
    pub read_only: bool,
    /// Whether this is a 23ai native JSON collection (embedded-OID key, native
    /// JSON content).
    pub native: bool,
}

impl SodaCollectionMetadata {
    /// Is the content column stored as native Oracle JSON (vs BLOB/CLOB bytes)?
    pub fn content_is_native_json(&self) -> bool {
        matches!(self.content_sql_type, ContentSqlType::Json)
    }

    /// Is the key an embedded OID (native collection)?
    pub fn key_is_embedded_oid(&self) -> bool {
        matches!(self.key_assignment, KeyAssignment::EmbeddedOid)
    }
}

/// Parse a collection descriptor JSON (as stored in `USER_SODA_COLLECTIONS`)
/// into [`SodaCollectionMetadata`].
pub fn parse_metadata(json: &Value) -> Result<SodaCollectionMetadata> {
    let obj = json
        .as_object()
        .ok_or_else(|| SodaError::InvalidMetadata("descriptor is not an object".to_string()))?;

    let table_name = obj
        .get("tableName")
        .and_then(Value::as_str)
        .ok_or_else(|| SodaError::InvalidMetadata("missing tableName".to_string()))?
        .to_string();
    let schema_name = obj
        .get("schemaName")
        .and_then(Value::as_str)
        .map(str::to_string);

    let key = obj
        .get("keyColumn")
        .and_then(Value::as_object)
        .ok_or_else(|| SodaError::InvalidMetadata("missing keyColumn".to_string()))?;
    let key_column = key
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| SodaError::InvalidMetadata("missing keyColumn.name".to_string()))?
        .to_string();
    let key_sql_type = key
        .get("sqlType")
        .and_then(Value::as_str)
        .unwrap_or("VARCHAR2")
        .to_string();
    let key_assignment = match key
        .get("assignmentMethod")
        .and_then(Value::as_str)
        .unwrap_or("UUID")
        .to_ascii_uppercase()
        .as_str()
    {
        "UUID" => KeyAssignment::Uuid,
        "SEQUENCE" => KeyAssignment::Sequence,
        "CLIENT" => KeyAssignment::Client,
        "EMBEDDED_OID" => KeyAssignment::EmbeddedOid,
        "GUID" => KeyAssignment::Guid,
        other => {
            return Err(SodaError::NotSupported(format!(
                "key assignment method {other} is not supported"
            )));
        }
    };
    let key_path = key.get("path").and_then(Value::as_str).map(str::to_string);

    let content = obj
        .get("contentColumn")
        .and_then(Value::as_object)
        .ok_or_else(|| SodaError::InvalidMetadata("missing contentColumn".to_string()))?;
    let content_column = content
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| SodaError::InvalidMetadata("missing contentColumn.name".to_string()))?
        .to_string();
    let content_sql_type = match content
        .get("sqlType")
        .and_then(Value::as_str)
        .unwrap_or("BLOB")
        .to_ascii_uppercase()
        .as_str()
    {
        "BLOB" => ContentSqlType::Blob,
        "CLOB" => ContentSqlType::Clob,
        "VARCHAR2" => ContentSqlType::Varchar2,
        "JSON" => ContentSqlType::Json,
        "RAW" => ContentSqlType::Raw,
        other => {
            return Err(SodaError::NotSupported(format!(
                "content SQL type {other} is not supported"
            )));
        }
    };

    let (version_column, version_method) = match obj.get("versionColumn").and_then(Value::as_object)
    {
        Some(v) => {
            let name = v.get("name").and_then(Value::as_str).map(str::to_string);
            let method = match v
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or("NONE")
                .to_ascii_uppercase()
                .as_str()
            {
                "UUID" => VersionMethod::Uuid,
                "TIMESTAMP" => VersionMethod::Timestamp,
                "SEQUENTIAL" => VersionMethod::Sequential,
                "NONE" => VersionMethod::None,
                hash @ ("MD5" | "SHA1" | "SHA256") => VersionMethod::Hash(hash.to_string()),
                other => {
                    return Err(SodaError::NotSupported(format!(
                        "version method {other} is not supported"
                    )));
                }
            };
            (name, method)
        }
        None => (None, VersionMethod::None),
    };

    let last_modified_column = obj
        .get("lastModifiedColumn")
        .and_then(Value::as_object)
        .and_then(|c| c.get("name"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let creation_time_column = obj
        .get("creationTimeColumn")
        .and_then(Value::as_object)
        .and_then(|c| c.get("name"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let media_type_column = obj
        .get("mediaTypeColumn")
        .and_then(Value::as_object)
        .and_then(|c| c.get("name"))
        .and_then(Value::as_str)
        .map(str::to_string);

    let read_only = obj
        .get("readOnly")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let native = obj.get("native").and_then(Value::as_bool).unwrap_or(false)
        || key_assignment == KeyAssignment::EmbeddedOid;

    Ok(SodaCollectionMetadata {
        table_name,
        schema_name,
        key_column,
        key_sql_type,
        key_assignment,
        key_path,
        content_column,
        content_sql_type,
        version_column,
        version_method,
        last_modified_column,
        creation_time_column,
        media_type_column,
        read_only,
        native,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_native_default_descriptor() {
        // The real descriptor a 23ai default collection produces (probed live).
        let desc = json!({
            "schemaName": "PYTHONTEST",
            "tableName": "MYCOLL",
            "keyColumn": {
                "name": "RESID",
                "sqlType": "RAW",
                "assignmentMethod": "EMBEDDED_OID",
                "path": "_id"
            },
            "contentColumn": {"name": "DATA", "sqlType": "JSON"},
            "versionColumn": {"name": "ETAG", "method": "NONE"},
            "readOnly": false,
            "native": true
        });
        let m = parse_metadata(&desc).unwrap();
        assert_eq!(m.table_name, "MYCOLL");
        assert_eq!(m.key_column, "RESID");
        assert_eq!(m.key_assignment, KeyAssignment::EmbeddedOid);
        assert_eq!(m.key_path.as_deref(), Some("_id"));
        assert_eq!(m.content_column, "DATA");
        assert!(m.content_is_native_json());
        assert!(m.native);
        assert_eq!(m.version_column.as_deref(), Some("ETAG"));
        assert_eq!(m.version_method, VersionMethod::None);
        assert!(!m.read_only);
    }

    #[test]
    fn parse_legacy_blob_descriptor() {
        // The real descriptor a legacy BLOB collection produces (probed live).
        let desc = json!({
            "schemaName": "PYTHONTEST",
            "tableName": "MYLEGACY",
            "keyColumn": {
                "name": "ID",
                "sqlType": "VARCHAR2",
                "maxLength": 255,
                "assignmentMethod": "UUID"
            },
            "contentColumn": {
                "name": "JSON_DOCUMENT",
                "sqlType": "BLOB",
                "validation": "STANDARD"
            },
            "lastModifiedColumn": {"name": "LAST_MODIFIED"},
            "versionColumn": {"name": "VERSION", "method": "UUID"},
            "creationTimeColumn": {"name": "CREATED_ON"},
            "readOnly": false
        });
        let m = parse_metadata(&desc).unwrap();
        assert_eq!(m.table_name, "MYLEGACY");
        assert_eq!(m.key_column, "ID");
        assert_eq!(m.key_assignment, KeyAssignment::Uuid);
        assert_eq!(m.content_column, "JSON_DOCUMENT");
        assert_eq!(m.content_sql_type, ContentSqlType::Blob);
        assert!(!m.content_is_native_json());
        assert!(!m.native);
        assert_eq!(m.version_column.as_deref(), Some("VERSION"));
        assert_eq!(m.version_method, VersionMethod::Uuid);
        assert_eq!(m.last_modified_column.as_deref(), Some("LAST_MODIFIED"));
        assert_eq!(m.creation_time_column.as_deref(), Some("CREATED_ON"));
    }

    #[test]
    fn missing_table_name_errors() {
        let desc = json!({"keyColumn": {"name": "ID"}, "contentColumn": {"name": "DATA"}});
        assert!(matches!(
            parse_metadata(&desc),
            Err(SodaError::InvalidMetadata(_))
        ));
    }

    #[test]
    fn media_type_column_parsed() {
        let desc = json!({
            "tableName": "MIXED",
            "keyColumn": {"name": "ID", "sqlType": "VARCHAR2", "assignmentMethod": "UUID"},
            "contentColumn": {"name": "DATA", "sqlType": "BLOB"},
            "mediaTypeColumn": {"name": "MEDIA_TYPE"}
        });
        let m = parse_metadata(&desc).unwrap();
        assert_eq!(m.media_type_column.as_deref(), Some("MEDIA_TYPE"));
    }
}
