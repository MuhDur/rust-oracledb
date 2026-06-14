//! SODA document representation.

use oracledb_protocol::oson::OsonValue;

/// A SODA document: content plus the SODA-managed metadata (key, version,
/// timestamps, media type).
///
/// Content can be carried two ways depending on the collection's content
/// column type:
/// - native JSON collections return content already decoded as an [`OsonValue`]
///   (the `content_oson` field);
/// - BLOB/CLOB/VARCHAR2 collections carry raw bytes (`content_bytes`).
///
/// For write operations the caller sets exactly one of the two.
#[derive(Debug, Clone, Default)]
pub struct SodaDocument {
    /// Unique key. `None` for documents built locally before insert into a
    /// server-key collection.
    pub key: Option<String>,
    /// Raw content bytes (for BLOB/CLOB/VARCHAR2 collections, or documents
    /// created from bytes/strings).
    pub content_bytes: Option<Vec<u8>>,
    /// Decoded native JSON content (for native JSON collections).
    pub content_oson: Option<OsonValue>,
    /// Media type; defaults to `application/json`.
    pub media_type: String,
    /// Document version / ETag.
    pub version: Option<String>,
    /// Creation timestamp (ISO 8601).
    pub created_on: Option<String>,
    /// Last-modified timestamp (ISO 8601).
    pub last_modified: Option<String>,
}

impl SodaDocument {
    /// Construct a document from raw content bytes (the `createDocument` path).
    pub fn from_bytes(content: Vec<u8>, key: Option<String>, media_type: Option<String>) -> Self {
        SodaDocument {
            key,
            content_bytes: Some(content),
            content_oson: None,
            media_type: media_type.unwrap_or_else(|| "application/json".to_string()),
            version: None,
            created_on: None,
            last_modified: None,
        }
    }

    /// Construct a document from a decoded JSON value (the native
    /// `createDocument`/`create_json_document` path).
    pub fn from_oson(content: OsonValue, key: Option<String>) -> Self {
        SodaDocument {
            key,
            content_bytes: None,
            content_oson: Some(content),
            media_type: "application/json".to_string(),
            version: None,
            created_on: None,
            last_modified: None,
        }
    }

    /// Borrow the raw content bytes, if this document carries them.
    pub fn content_as_bytes(&self) -> Option<&[u8]> {
        self.content_bytes.as_deref()
    }

    /// Borrow the decoded OSON content, if this document carries it.
    pub fn content_as_oson(&self) -> Option<&OsonValue> {
        self.content_oson.as_ref()
    }

    /// Whether the document has any content.
    pub fn has_content(&self) -> bool {
        self.content_bytes.is_some() || self.content_oson.is_some()
    }
}
