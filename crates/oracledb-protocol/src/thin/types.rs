#![forbid(unsafe_code)]

use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptInfo {
    pub protocol_version: u16,
    pub protocol_options: u16,
    pub sdu: u32,
    pub supports_fast_auth: bool,
    pub supports_oob_check: bool,
    /// Whether the server advertised out-of-band (urgent-TCP) break support in
    /// the accept's `protocol_options` (`& TNS_GSO_CAN_RECV_ATTENTION`), the
    /// reference `Capabilities.supports_oob` (capabilities.pyx:121).
    pub supports_oob: bool,
    pub supports_end_of_response: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AuthResponse {
    pub session_data: BTreeMap<String, String>,
    pub verifier_type: Option<u32>,
    pub capabilities: Option<ClientCapabilities>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientCapabilities {
    pub ttc_field_version: u8,
    pub max_string_size: u32,
    /// Database character set id from the protocol-info response. Charset
    /// ids >= 800 are multi-byte (drives direct path CLOB form selection).
    pub charset_id: u16,
}

impl Default for ClientCapabilities {
    fn default() -> Self {
        Self {
            ttc_field_version: 24,
            max_string_size: 32_767,
            // AL32UTF8: the charset the thin protocol always negotiates
            charset_id: 873,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ColumnMetadata {
    pub(crate) name: String,
    pub(crate) ora_type_num: u8,
    pub(crate) csfrm: u8,
    pub(crate) precision: i8,
    pub(crate) scale: i8,
    pub(crate) buffer_size: u32,
    pub(crate) max_size: u32,
    pub(crate) nulls_allowed: bool,
    pub(crate) is_json: bool,
    pub(crate) is_oson: bool,
    pub(crate) object_schema: Option<String>,
    pub(crate) object_type_name: Option<String>,
    pub(crate) is_array: bool,
    /// VECTOR columns only: the fixed dimension count, or `None` for a
    /// flexible-dimension column (server sends 0).
    pub(crate) vector_dimensions: Option<u32>,
    /// VECTOR columns only: the storage format byte (`VECTOR_FORMAT_*`); 0
    /// for a flexible-format column.
    pub(crate) vector_format: u8,
    /// VECTOR columns only: the metadata flags byte (sparse / flexible).
    pub(crate) vector_flags: u8,
    /// SQL data-use-case domain schema (23ai+), or `None` if the column has no
    /// domain.
    pub(crate) domain_schema: Option<String>,
    /// SQL data-use-case domain name (23ai+), or `None` if the column has no
    /// domain.
    pub(crate) domain_name: Option<String>,
    /// Ordered column annotations (23ai+), as (key, value) pairs preserving
    /// server order; `None` if the column has no annotations. A null annotation
    /// value is normalized to an empty string, matching python-oracledb.
    pub(crate) annotations: Option<Vec<(String, String)>>,
}

impl ColumnMetadata {
    pub fn new(name: impl Into<String>, ora_type_num: u8) -> Self {
        Self {
            name: name.into(),
            ora_type_num,
            ..Self::default()
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    pub fn ora_type_num(&self) -> u8 {
        self.ora_type_num
    }

    #[must_use]
    pub fn with_ora_type_num(mut self, ora_type_num: u8) -> Self {
        self.ora_type_num = ora_type_num;
        self
    }

    pub fn csfrm(&self) -> u8 {
        self.csfrm
    }

    #[must_use]
    pub fn with_csfrm(mut self, csfrm: u8) -> Self {
        self.csfrm = csfrm;
        self
    }

    pub fn precision(&self) -> i8 {
        self.precision
    }

    #[must_use]
    pub fn with_precision(mut self, precision: i8) -> Self {
        self.precision = precision;
        self
    }

    pub fn scale(&self) -> i8 {
        self.scale
    }

    #[must_use]
    pub fn with_scale(mut self, scale: i8) -> Self {
        self.scale = scale;
        self
    }

    pub fn buffer_size(&self) -> u32 {
        self.buffer_size
    }

    #[must_use]
    pub fn with_buffer_size(mut self, buffer_size: u32) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    pub fn max_size(&self) -> u32 {
        self.max_size
    }

    #[must_use]
    pub fn with_max_size(mut self, max_size: u32) -> Self {
        self.max_size = max_size;
        self
    }

    pub fn nulls_allowed(&self) -> bool {
        self.nulls_allowed
    }

    #[must_use]
    pub fn with_nulls_allowed(mut self, nulls_allowed: bool) -> Self {
        self.nulls_allowed = nulls_allowed;
        self
    }

    pub fn is_json(&self) -> bool {
        self.is_json
    }

    #[must_use]
    pub fn with_is_json(mut self, is_json: bool) -> Self {
        self.is_json = is_json;
        self
    }

    pub fn is_oson(&self) -> bool {
        self.is_oson
    }

    #[must_use]
    pub fn with_is_oson(mut self, is_oson: bool) -> Self {
        self.is_oson = is_oson;
        self
    }

    pub fn object_schema(&self) -> Option<&str> {
        self.object_schema.as_deref()
    }

    #[must_use]
    pub fn with_object_schema(mut self, object_schema: Option<String>) -> Self {
        self.object_schema = object_schema;
        self
    }

    pub fn object_type_name(&self) -> Option<&str> {
        self.object_type_name.as_deref()
    }

    #[must_use]
    pub fn with_object_type_name(mut self, object_type_name: Option<String>) -> Self {
        self.object_type_name = object_type_name;
        self
    }

    pub fn is_array(&self) -> bool {
        self.is_array
    }

    #[must_use]
    pub fn with_is_array(mut self, is_array: bool) -> Self {
        self.is_array = is_array;
        self
    }

    pub fn vector_dimensions(&self) -> Option<u32> {
        self.vector_dimensions
    }

    #[must_use]
    pub fn with_vector_dimensions(mut self, vector_dimensions: Option<u32>) -> Self {
        self.vector_dimensions = vector_dimensions;
        self
    }

    pub fn vector_format(&self) -> u8 {
        self.vector_format
    }

    #[must_use]
    pub fn with_vector_format(mut self, vector_format: u8) -> Self {
        self.vector_format = vector_format;
        self
    }

    pub fn vector_flags(&self) -> u8 {
        self.vector_flags
    }

    #[must_use]
    pub fn with_vector_flags(mut self, vector_flags: u8) -> Self {
        self.vector_flags = vector_flags;
        self
    }

    pub fn domain_schema(&self) -> Option<&str> {
        self.domain_schema.as_deref()
    }

    #[must_use]
    pub fn with_domain_schema(mut self, domain_schema: Option<String>) -> Self {
        self.domain_schema = domain_schema;
        self
    }

    pub fn domain_name(&self) -> Option<&str> {
        self.domain_name.as_deref()
    }

    #[must_use]
    pub fn with_domain_name(mut self, domain_name: Option<String>) -> Self {
        self.domain_name = domain_name;
        self
    }

    pub fn annotations(&self) -> Option<&[(String, String)]> {
        self.annotations.as_deref()
    }

    #[must_use]
    pub fn with_annotations(mut self, annotations: Option<Vec<(String, String)>>) -> Self {
        self.annotations = annotations;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BindTypeInfo {
    pub ora_type_num: u8,
    pub csfrm: u8,
    pub buffer_size: u32,
}

/// Heap payload of [`QueryValue::Cursor`]. Boxed out of the enum because a
/// REF CURSOR carries a full column-metadata vector — see [`QueryValue`].
#[derive(Clone, Debug, PartialEq)]
pub struct CursorValue {
    pub columns: Vec<ColumnMetadata>,
    pub cursor_id: u32,
}

/// Heap payload of [`QueryValue::Object`] (ADT / collection image). Boxed out
/// of the enum — see [`QueryValue`].
#[derive(Clone, Debug, PartialEq)]
pub struct ObjectValue {
    pub schema: Option<String>,
    pub type_name: Option<String>,
    pub packed_data: Vec<u8>,
}

/// Heap payload of [`QueryValue::Lob`] (LOB / BFILE locator). Boxed out of the
/// enum — see [`QueryValue`].
#[derive(Clone, Debug, PartialEq)]
pub struct LobValue {
    pub ora_type_num: u8,
    pub csfrm: u8,
    pub locator: Vec<u8>,
    pub size: u64,
    pub chunk_size: u32,
}

// `Eq` is intentionally omitted: VECTOR values carry floating-point elements,
// which only implement `PartialEq`.
//
// COLD variants are boxed: `Cursor`, `Object`, `Lob`, `Vector` and `Json`
// each carried a large (32-72 byte) inline payload that dominated the enum and
// bloated the hot per-row fetch `Vec<QueryValue>`, hurting cache locality.
// Boxing them moves the payload to the heap so the enum shrinks to the common
// scalar footprint (the largest *hot* scalar variant is `Number`/`TextRaw` at
// 32 bytes). The boxing is pure indirection: no semantics change, only the
// cold/rare values now live behind a pointer. See the `const _` size guard
// below for the enforced upper bound. (Perf pre-req for borrowed-fetch and
// decode-offload work.)
#[derive(Clone, Debug, PartialEq)]
pub enum QueryValue {
    Text(String),
    /// Character data that could not be decoded as valid text; the raw bytes
    /// are preserved so the caller can apply an `encoding_errors` policy.
    TextRaw {
        bytes: Vec<u8>,
        csfrm: u8,
    },
    Raw(Vec<u8>),
    Rowid(String),
    BinaryDouble(String),
    IntervalDS {
        days: i32,
        hours: i32,
        minutes: i32,
        seconds: i32,
        fseconds: i32,
    },
    IntervalYM {
        years: i32,
        months: i32,
    },
    /// Oracle `NUMBER` / `BINARY_INTEGER`. The lossless decimal value is carried
    /// inline as a `{ coefficient: i128, scale: i16 }` form (no per-cell heap
    /// allocation for the common case), with a boxed text fallback for the rare
    /// value that does not fit inline. See [`OracleNumber`].
    Number(OracleNumber),
    /// Native Oracle `DB_TYPE_BOOLEAN` (`ora_type_num` 252, 23ai+): surfaced as
    /// a Python `bool` rather than an integer.
    Boolean(bool),
    /// REF CURSOR (cold): payload boxed, see [`CursorValue`].
    Cursor(Box<CursorValue>),
    DateTime {
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        nanosecond: u32,
    },
    /// ADT / collection image (cold): payload boxed, see [`ObjectValue`].
    Object(Box<ObjectValue>),
    /// LOB / BFILE locator (cold): payload boxed, see [`LobValue`].
    Lob(Box<LobValue>),
    /// VECTOR (cold): the per-element data is boxed out of the hot enum.
    Vector(Box<crate::vector::Vector>),
    /// Native Oracle JSON (`DB_TYPE_JSON`, `ora_type_num` 119): the OSON image
    /// is decoded eagerly into the lossless [`crate::oson::OsonValue`] tree
    /// (cold): the tree is boxed out of the hot enum.
    Json(Box<crate::oson::OsonValue>),
    Array(Vec<Option<QueryValue>>),
}

// Compile-time guard for the hot per-row fetch path. `Vec<QueryValue>` is
// allocated once per fetched row, so the enum's stack footprint directly drives
// cache locality. Boxing the cold variants (Cursor/Object/Lob/Vector/Json)
// brought this from 72 bytes down to 32. The largest hot scalar variant is now
// `Number(OracleNumber)`: the inline `OracleNumber` form is `{ i128 coefficient
// (16) + i16 scale (2) + bool + tag }`, and its boxed-text fallback variant is a
// `Box<str>` (16) + bool — both well within 24 bytes, so `QueryValue` stays at
// the 32-byte budget (the `Text(String)` / `Array(Vec)` variants remain the
// 24-byte width drivers; the discriminant tucks into their spare bytes). Adding
// a new large *inline* variant must either stay under the bound or be boxed; do
// not bump N without re-confirming the hot fetch path.
const _: () = assert!(core::mem::size_of::<QueryValue>() <= 32);
// Explicit guard for the inline NUMBER carrier: it must not by itself push
// `QueryValue` past budget (it is the perf-critical inline payload).
const _: () = assert!(core::mem::size_of::<OracleNumber>() <= 24);

impl QueryValue {
    /// Construct a `NUMBER` value from already-canonical decimal `text` and an
    /// explicit `is_integer` flag. Convenience for binds / tests that hold the
    /// canonical text; folds into the inline form when it fits. The text MUST be
    /// canonical Oracle `NUMBER` text (the form the decoder emits).
    pub fn number_from_text(text: &str, is_integer: bool) -> Self {
        QueryValue::Number(OracleNumber::from_canonical_text_with_flag(
            text, is_integer,
        ))
    }

    /// Borrow this value as decoded text when it is a `VARCHAR2` / `CHAR` /
    /// `NVARCHAR2` / `CLOB`-inlined string, otherwise `None`. Convenience
    /// accessor for callers that want to avoid matching the full enum.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            QueryValue::Text(value) => Some(value.as_str()),
            _ => None,
        }
    }

    /// Interpret this value as a 64-bit integer. Works for `NUMBER` values
    /// whose canonical text parses as an `i64`, and for the native
    /// `DB_TYPE_BOOLEAN` (`true` -> 1, `false` -> 0). Returns `None` for any
    /// other variant, or a `NUMBER` that does not fit / is not integral.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            QueryValue::Number(num) => num
                .to_i64()
                .or_else(|| num.to_canonical_string().parse::<i64>().ok()),
            QueryValue::Boolean(value) => Some(i64::from(*value)),
            _ => None,
        }
    }

    /// Interpret this value as an `f64`. Works for `NUMBER`, `BINARY_DOUBLE`
    /// and `BINARY_FLOAT` (all carried as canonical text), otherwise `None`.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            QueryValue::Number(num) => num.to_canonical_string().parse::<f64>().ok(),
            QueryValue::BinaryDouble(text) => text.parse::<f64>().ok(),
            _ => None,
        }
    }

    /// The canonical decimal text of a `NUMBER` value (lossless, arbitrary
    /// precision), otherwise `None`. The owned inline form synthesizes the text
    /// on demand via the shared formatter, so this returns an owned `Cow`.
    pub fn as_number_text(&self) -> Option<std::borrow::Cow<'_, str>> {
        match self {
            QueryValue::Number(num) => Some(num.to_canonical_cow()),
            _ => None,
        }
    }

    /// Borrow the inline `NUMBER` representation, otherwise `None`.
    pub fn as_number(&self) -> Option<&OracleNumber> {
        match self {
            QueryValue::Number(num) => Some(num),
            _ => None,
        }
    }

    /// Return the boolean of a native `DB_TYPE_BOOLEAN` value, otherwise
    /// `None`.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            QueryValue::Boolean(value) => Some(*value),
            _ => None,
        }
    }

    /// Borrow the bytes of a `RAW` value, otherwise `None`.
    pub fn as_raw(&self) -> Option<&[u8]> {
        match self {
            QueryValue::Raw(bytes) => Some(bytes.as_slice()),
            _ => None,
        }
    }

    /// Borrow the encoded text of a `ROWID` / `UROWID` value, otherwise
    /// `None`.
    pub fn as_rowid(&self) -> Option<&str> {
        match self {
            QueryValue::Rowid(value) => Some(value.as_str()),
            _ => None,
        }
    }
}

/// A borrowed, zero-copy mirror of the hot scalar [`QueryValue`] variants whose
/// payload lives **inside the fetch decode buffer** (the network response
/// `Vec<u8>`). A consumer iterating a fetched row batch via the borrowed path
/// receives `QueryValueRef<'buf>` values that point straight at the wire bytes —
/// the common scalar case pays *zero* per-cell allocations, in contrast to the
/// owned [`QueryValue`] path which materializes a `String`/`Vec<u8>` for every
/// column of every row.
///
/// ## What is and is not borrowed
///
/// - `Text` / `Raw` borrow a contiguous slice of the wire buffer directly: the
///   common single-chunk `VARCHAR2`/`CHAR`/`RAW` value costs nothing.
/// - `Number` borrows the **canonically reformatted** decimal text. Oracle's
///   `NUMBER` is not stored as ASCII on the wire, so the borrowed path decodes
///   it into a scratch arena the batch owns (see the borrowed fetch API) and
///   borrows from there — still zero per-cell heap allocation, the arena grows
///   amortized across the batch.
/// - `Boolean` / `IntervalDS` / `IntervalYM` / `DateTime` are tiny `Copy`
///   values decoded from the wire bytes; they never touched the heap on the
///   owned path either.
/// - The cold variants (`Cursor`, `Object`, `Lob`, `Vector`, `Json`) and the
///   rare UTF-16 (`NCHAR`) text / synthesized `ROWID` / `BinaryDouble` cases
///   cannot be borrowed losslessly from the wire, so they fall back to an owned
///   boxed [`QueryValue`] (the [`QueryValueRef::Owned`] variant). These are the
///   uncommon path; the hot scalar grid stays borrowed.
///
/// `Copy` + small: the enum holds borrowed scalars and small `Copy` payloads
/// only (cold values live behind the boxed `Owned` pointer), so a row of
/// `QueryValueRef` is a flat, cache-friendly slice. Convert to the owned form
/// with [`QueryValueRef::to_owned_value`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum QueryValueRef<'buf> {
    /// Decoded text borrowing the wire buffer (single-chunk `VARCHAR2` / `CHAR`
    /// / `LONG` that decoded as valid UTF-8).
    Text(&'buf str),
    /// `RAW` / `LONG_RAW` bytes borrowing the wire buffer (single chunk).
    Raw(&'buf [u8]),
    /// Canonical decimal text of a `NUMBER`, borrowed from the batch's number
    /// scratch arena (the wire form is binary, so it is reformatted once).
    Number {
        text: &'buf str,
        is_integer: bool,
    },
    /// Native `DB_TYPE_BOOLEAN`.
    Boolean(bool),
    IntervalDS {
        days: i32,
        hours: i32,
        minutes: i32,
        seconds: i32,
        fseconds: i32,
    },
    IntervalYM {
        years: i32,
        months: i32,
    },
    DateTime {
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        nanosecond: u32,
    },
    /// Fallback for the cold / non-borrowable variants (Cursor / Object / Lob /
    /// Vector / Json, UTF-16 `NCHAR` text, synthesized `ROWID`, `BinaryDouble`).
    /// Boxed so the borrowed enum stays small. This is the rare path.
    Owned(&'buf QueryValue),
}

impl QueryValueRef<'_> {
    /// Materialize an owned [`QueryValue`] from this borrowed reference,
    /// allocating exactly as the owned decode path would. Use this when a
    /// borrowed row must outlive the batch buffer (e.g. crossing the Python
    /// boundary), or to compare borrowed and owned paths in tests.
    pub fn to_owned_value(&self) -> QueryValue {
        match *self {
            QueryValueRef::Text(text) => QueryValue::Text(text.to_string()),
            QueryValueRef::Raw(bytes) => QueryValue::Raw(bytes.to_vec()),
            QueryValueRef::Number { text, is_integer } => QueryValue::Number(
                OracleNumber::from_canonical_text_with_flag(text, is_integer),
            ),
            QueryValueRef::Boolean(value) => QueryValue::Boolean(value),
            QueryValueRef::IntervalDS {
                days,
                hours,
                minutes,
                seconds,
                fseconds,
            } => QueryValue::IntervalDS {
                days,
                hours,
                minutes,
                seconds,
                fseconds,
            },
            QueryValueRef::IntervalYM { years, months } => QueryValue::IntervalYM { years, months },
            QueryValueRef::DateTime {
                year,
                month,
                day,
                hour,
                minute,
                second,
                nanosecond,
            } => QueryValue::DateTime {
                year,
                month,
                day,
                hour,
                minute,
                second,
                nanosecond,
            },
            QueryValueRef::Owned(value) => value.clone(),
        }
    }

    /// Borrow this value as decoded text when it is a borrowed `Text`, otherwise
    /// `None`. Mirror of [`QueryValue::as_text`] for the borrowed path.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            QueryValueRef::Text(value) => Some(value),
            QueryValueRef::Owned(QueryValue::Text(value)) => Some(value.as_str()),
            _ => None,
        }
    }

    /// Borrow the canonical decimal text of a `NUMBER` value, otherwise `None`.
    /// Mirror of [`QueryValue::as_number_text`] for the borrowed path. The hot
    /// case borrows the per-row number arena (zero copy). The `Owned` fallback
    /// only yields a borrow when the inline form spilled to boxed text; the
    /// inline numeric form has no stored `&str` (it never reaches `Owned` from
    /// the fetch path — NUMBERs are arena-resident).
    pub fn as_number_text(&self) -> Option<&str> {
        match self {
            QueryValueRef::Number { text, .. } => Some(text),
            QueryValueRef::Owned(QueryValue::Number(num)) => num.as_borrowed_text(),
            _ => None,
        }
    }

    /// Borrow the bytes of a `RAW` value, otherwise `None`. Mirror of
    /// [`QueryValue::as_raw`] for the borrowed path.
    pub fn as_raw(&self) -> Option<&[u8]> {
        match self {
            QueryValueRef::Raw(bytes) => Some(bytes),
            QueryValueRef::Owned(QueryValue::Raw(bytes)) => Some(bytes.as_slice()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum BindValue {
    Null,
    TypedNull {
        ora_type_num: u8,
        csfrm: u8,
        buffer_size: u32,
    },
    Output {
        ora_type_num: u8,
        csfrm: u8,
        buffer_size: u32,
    },
    ReturnOutput {
        ora_type_num: u8,
        csfrm: u8,
        buffer_size: u32,
    },
    ObjectOutput {
        schema: String,
        type_name: String,
        oid: Vec<u8>,
        version: u32,
        buffer_size: u32,
        is_return: bool,
    },
    /// A DbObject bound as IN (or IN/OUT). The fully packed pickle `image` is
    /// built by the pyshim (it owns the recursive Python attribute values); the
    /// protocol only frames it (toid/oid/snapshot/version/len/flags + image).
    ObjectInput {
        schema: String,
        type_name: String,
        oid: Vec<u8>,
        version: u32,
        image: Vec<u8>,
        buffer_size: u32,
    },
    Text(String),
    Raw(Vec<u8>),
    Lob {
        ora_type_num: u8,
        csfrm: u8,
        locator: Vec<u8>,
    },
    Number(String),
    BinaryInteger(String),
    BinaryDouble(f64),
    BinaryFloat(f64),
    Boolean(bool),
    IntervalDS {
        days: i32,
        seconds: i32,
        microseconds: i32,
    },
    IntervalYM {
        years: i32,
        months: i32,
    },
    DateTime {
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
    },
    Timestamp {
        ora_type_num: u8,
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        nanosecond: u32,
    },
    Array {
        ora_type_num: u8,
        csfrm: u8,
        buffer_size: u32,
        max_elements: u32,
        values: Vec<Option<BindValue>>,
    },
    Vector(crate::vector::Vector),
    /// Native Oracle JSON bind (`DB_TYPE_JSON`): the already-encoded OSON image.
    /// The Python-facing layer encodes the value to OSON before binding so the
    /// connection's long-field-name capability can be applied.
    Json(Vec<u8>),
    Cursor {
        cursor_id: u32,
    },
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<ColumnMetadata>,
    pub rows: Vec<Vec<Option<QueryValue>>>,
    pub out_values: Vec<(usize, Option<QueryValue>)>,
    pub return_values: Vec<(usize, Vec<Option<QueryValue>>)>,
    pub cursor_id: u32,
    pub row_count: u64,
    pub more_rows: bool,
    pub compilation_error_warning: bool,
    /// Encoded rowid of the last affected row (reference cursor `lastrowid`).
    pub last_rowid: Option<String>,
    /// Batch errors collected with `executemany(batcherrors=True)`.
    pub batch_errors: Vec<BatchServerError>,
    /// Per-iteration row counts from `executemany(arraydmlrowcounts=True)`.
    pub array_dml_row_counts: Option<Vec<u64>>,
    /// Child cursors returned via `dbms_sql.return_result`
    /// (`QueryValue::Cursor` entries); `Some` only when the response carried
    /// a TNS_MSG_TYPE_IMPLICIT_RESULTSET message.
    pub implicit_resultsets: Option<Vec<QueryValue>>,
    /// Pipeline token echoed by the server (TNS message 33) at the start of
    /// each pipelined response; `None` outside pipelines.
    pub token_num: Option<u64>,
    /// Sessionless transaction state update carried by the response's SYNC
    /// server-side piggyback (reference `_update_sessionless_txn_state`);
    /// `None` when the execute did not change the sessionless state.
    pub sessionless_txn_state: Option<SessionlessTxnState>,
    /// CQN registered-query id read from the registration-info block of the
    /// execute return parameters (reference `cursor_impl._query_id`,
    /// base.pyx:1300-1309). `Some(0)` when the server returned no query id
    /// (qos without SUBSCR_QOS_QUERY); `None` when the block was absent.
    pub query_id: Option<u64>,
    /// Whether a server-side transaction is in progress, sampled from the final
    /// end-of-call status bit `TNS_EOCS_FLAGS_TXN_IN_PROGRESS` (reference
    /// protocol.pyx `_process_call_status`). `None` when the response carried no
    /// STATUS message (the caller then leaves the flag unchanged).
    pub txn_in_progress: Option<bool>,
}

impl QueryResult {
    /// Borrow the value at `(row, col)` of the fetched result, or `None` when
    /// either index is out of range or the cell is SQL `NULL`. Convenience
    /// accessor so callers do not have to index `rows[row][col]` and unwrap
    /// the `Option` by hand.
    pub fn cell(&self, row: usize, col: usize) -> Option<&QueryValue> {
        self.rows.get(row)?.get(col)?.as_ref()
    }

    /// Zero-based column index of the column whose name matches `name`
    /// case-insensitively (Oracle folds unquoted identifiers to upper case),
    /// or `None` when there is no such column.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|col| col.name.eq_ignore_ascii_case(name))
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LobReadResult {
    pub data: Option<Vec<u8>>,
    pub locator: Vec<u8>,
    pub amount: u64,
}

/// Outcome of a sessionless transaction switch / suspend round trip, as
/// signalled by the server through the transaction-id key/value pair
/// (reference messages/base.pyx `_update_sessionless_txn_state`). `None`
/// means the response carried no transaction-id update.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionlessTxnState {
    /// A sessionless transaction was started or resumed (`TXNID_SYNC_SET`).
    Set { started_on_server: bool },
    /// The active sessionless transaction was suspended or ended
    /// (`TXNID_SYNC_UNSET`).
    Unset,
}

/// Outcome of a TPC transaction-switch (func 103) round trip used by
/// `tpc_begin` (START) and `tpc_end` (DETACH). Reference tpc_switch.pyx
/// `_process_return_parameters` captures the application value and the returned
/// transaction context; the txn-in-progress bit is sampled from the final call
/// status (reference protocol.pyx `_process_call_status`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TpcSwitchResponse {
    /// The transaction context returned by the server on begin; must be stored
    /// verbatim and echoed on end/prepare/commit/rollback.
    pub context: Vec<u8>,
    /// `call_status & TNS_EOCS_FLAGS_TXN_IN_PROGRESS` from the last status.
    pub txn_in_progress: bool,
    /// Any sessionless-state update carried by a transaction-id key/value pair
    /// (only relevant on the sessionless path, retained for shared parsing).
    pub sessionless_state: Option<SessionlessTxnState>,
}

/// Outcome of a TPC transaction change-state (func 104) round trip used by
/// `tpc_prepare` / `tpc_commit` / `tpc_rollback`. Reference tpc_change_state.pyx
/// `_process_return_parameters` reads the out state (ub4) from the PARAMETER
/// message; the txn-in-progress bit is sampled from the final call status.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TpcChangeStateResponse {
    /// The out state returned by the server (one of the `TNS_TPC_TXN_STATE_*`).
    pub state: u32,
    /// `call_status & TNS_EOCS_FLAGS_TXN_IN_PROGRESS` from the last status.
    pub txn_in_progress: bool,
}

/// Optional execute modes (reference ExecuteMessage attributes).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecuteOptions {
    pub(crate) batcherrors: bool,
    pub(crate) arraydmlrowcounts: bool,
    /// Parse/describe without executing (reference `parse_only`).
    pub(crate) parse_only: bool,
    /// Pipeline token; pipelined operations carry tokens 1..N
    /// (impl/thin/connection.pyx `_create_messages_for_pipeline`),
    /// everything else carries 0.
    pub(crate) token_num: u64,
    /// Server cursor id of an already-parsed statement; non-zero skips the
    /// PARSE option and SQL text (reference Statement._cursor_id).
    pub(crate) cursor_id: u32,
    /// Whether the statement may be kept in the connection statement cache
    /// (reference `cursor.prepare(cache_statement=...)`).
    pub(crate) cache_statement: bool,
    /// Whether the cursor was opened scrollable; sets the scrollable execute
    /// flags and primes the fetch orientation (reference `cursor_impl.scrollable`).
    pub(crate) scrollable: bool,
    /// Fetch orientation for the next fetch (reference `fetch_orientation`,
    /// al8i4[10]); one of the `TNS_FETCH_ORIENTATION_*` constants. Zero leaves
    /// the server default.
    pub(crate) fetch_orientation: u32,
    /// Desired row position paired with `fetch_orientation` (reference
    /// `fetch_pos`, al8i4[11]).
    pub(crate) fetch_pos: u32,
    /// True when this execute is a scroll request: the EXECUTE/BIND options are
    /// suppressed so the server only repositions the open cursor and fetches
    /// (reference `scroll_operation`).
    pub(crate) scroll_operation: bool,
    /// Suspend the active sessionless transaction once this execute succeeds
    /// (reference `cursor_impl.suspend_on_success`); the driver folds a
    /// post-detach into the sessionless piggyback. Does not affect the execute
    /// wire body itself.
    pub(crate) suspend_on_success: bool,
    /// Suppress the FETCH execute option so the server does not prefetch any
    /// rows during the execute round trip (reference `stmt._no_prefetch`,
    /// execute.pyx:99). Set when re-executing an open cursor whose columns
    /// require a client-side define (VECTOR): a prefetched row would otherwise
    /// exhaust the cursor before the define-fetch runs, yielding ORA-01002 on
    /// the subsequent fetch.
    pub(crate) no_prefetch: bool,
    /// CQN registration id threaded into the execute body (split into lsb/msb
    /// at the al8i4 slots) when registering a query against a subscription
    /// (reference `cursor_impl._registration_id`, execute.pyx:116-163). Zero
    /// for ordinary executes.
    pub(crate) registration_id: u64,
}

impl Default for ExecuteOptions {
    fn default() -> Self {
        Self {
            batcherrors: false,
            arraydmlrowcounts: false,
            parse_only: false,
            token_num: 0,
            cursor_id: 0,
            cache_statement: true,
            scrollable: false,
            fetch_orientation: 0,
            fetch_pos: 0,
            scroll_operation: false,
            suspend_on_success: false,
            no_prefetch: false,
            registration_id: 0,
        }
    }
}

impl ExecuteOptions {
    pub fn batcherrors(&self) -> bool {
        self.batcherrors
    }

    #[must_use]
    pub fn with_batcherrors(mut self, enabled: bool) -> Self {
        self.batcherrors = enabled;
        self
    }

    pub fn arraydmlrowcounts(&self) -> bool {
        self.arraydmlrowcounts
    }

    #[must_use]
    pub fn with_arraydmlrowcounts(mut self, enabled: bool) -> Self {
        self.arraydmlrowcounts = enabled;
        self
    }

    pub fn parse_only(&self) -> bool {
        self.parse_only
    }

    #[must_use]
    pub fn with_parse_only(mut self, enabled: bool) -> Self {
        self.parse_only = enabled;
        self
    }

    pub fn token_num(&self) -> u64 {
        self.token_num
    }

    #[must_use]
    pub fn with_token_num(mut self, token_num: u64) -> Self {
        self.token_num = token_num;
        self
    }

    pub fn cursor_id(&self) -> u32 {
        self.cursor_id
    }

    #[must_use]
    pub fn with_cursor_id(mut self, cursor_id: u32) -> Self {
        self.cursor_id = cursor_id;
        self
    }

    pub fn cache_statement(&self) -> bool {
        self.cache_statement
    }

    #[must_use]
    pub fn with_cache_statement(mut self, enabled: bool) -> Self {
        self.cache_statement = enabled;
        self
    }

    pub fn scrollable(&self) -> bool {
        self.scrollable
    }

    #[must_use]
    pub fn with_scrollable(mut self, enabled: bool) -> Self {
        self.scrollable = enabled;
        self
    }

    pub fn fetch_orientation(&self) -> u32 {
        self.fetch_orientation
    }

    #[must_use]
    pub fn with_fetch_orientation(mut self, fetch_orientation: u32) -> Self {
        self.fetch_orientation = fetch_orientation;
        self
    }

    pub fn fetch_pos(&self) -> u32 {
        self.fetch_pos
    }

    #[must_use]
    pub fn with_fetch_pos(mut self, fetch_pos: u32) -> Self {
        self.fetch_pos = fetch_pos;
        self
    }

    pub fn scroll_operation(&self) -> bool {
        self.scroll_operation
    }

    #[must_use]
    pub fn with_scroll_operation(mut self, enabled: bool) -> Self {
        self.scroll_operation = enabled;
        self
    }

    pub fn suspend_on_success(&self) -> bool {
        self.suspend_on_success
    }

    #[must_use]
    pub fn with_suspend_on_success(mut self, enabled: bool) -> Self {
        self.suspend_on_success = enabled;
        self
    }

    pub fn no_prefetch(&self) -> bool {
        self.no_prefetch
    }

    #[must_use]
    pub fn with_no_prefetch(mut self, enabled: bool) -> Self {
        self.no_prefetch = enabled;
        self
    }

    pub fn registration_id(&self) -> u64 {
        self.registration_id
    }

    #[must_use]
    pub fn with_registration_id(mut self, registration_id: u64) -> Self {
        self.registration_id = registration_id;
        self
    }
}

/// One batch error entry from `executemany(batcherrors=True)` (reference
/// impl/thin/messages/base.pyx batch error codes/offsets/messages arrays).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BatchServerError {
    pub(crate) code: u32,
    pub(crate) offset: u32,
    pub(crate) message: String,
}

impl BatchServerError {
    pub fn new(code: u32, offset: u32, message: impl Into<String>) -> Self {
        Self {
            code,
            offset,
            message: message.into(),
        }
    }

    pub fn code(&self) -> u32 {
        self.code
    }

    pub fn offset(&self) -> u32 {
        self.offset
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn into_parts(self) -> (u32, u32, String) {
        (self.code, self.offset, self.message)
    }
}

#[cfg(test)]
mod accessor_tests {
    use super::*;

    #[test]
    fn query_value_typed_accessors() {
        let text = QueryValue::Text("hello".to_string());
        assert_eq!(text.as_text(), Some("hello"));
        assert_eq!(text.as_i64(), None);

        let int = QueryValue::number_from_text("42", true);
        assert_eq!(int.as_i64(), Some(42));
        assert_eq!(int.as_f64(), Some(42.0));
        assert_eq!(int.as_number_text().as_deref(), Some("42"));

        let dbl = QueryValue::BinaryDouble("2.5".to_string());
        assert_eq!(dbl.as_f64(), Some(2.5));

        let boolean = QueryValue::Boolean(true);
        assert_eq!(boolean.as_bool(), Some(true));
        assert_eq!(boolean.as_i64(), Some(1));

        let raw = QueryValue::Raw(vec![0xDE, 0xAD]);
        assert_eq!(raw.as_raw(), Some([0xDE, 0xAD].as_slice()));

        let rowid = QueryValue::Rowid("AAAR".to_string());
        assert_eq!(rowid.as_rowid(), Some("AAAR"));
    }

    #[test]
    fn query_result_cell_and_column_index() {
        let result = QueryResult {
            columns: vec![
                ColumnMetadata {
                    name: "ID".to_string(),
                    ..ColumnMetadata::default()
                },
                ColumnMetadata {
                    name: "NAME".to_string(),
                    ..ColumnMetadata::default()
                },
            ],
            rows: vec![vec![Some(QueryValue::number_from_text("7", true)), None]],
            ..QueryResult::default()
        };
        assert_eq!(result.cell(0, 0).and_then(QueryValue::as_i64), Some(7));
        assert!(result.cell(0, 1).is_none(), "NULL cell is None");
        assert!(result.cell(1, 0).is_none(), "out-of-range row is None");
        assert_eq!(result.column_index("name"), Some(1));
        assert_eq!(result.column_index("missing"), None);
    }
}

#[cfg(test)]
mod query_value_ref_tests {
    use super::*;

    // A `QueryValueRef` borrowing scalar bytes out of a `buf` round-trips to the
    // exact owned `QueryValue` the owned decode path would have produced.
    #[test]
    fn borrowed_scalars_to_owned_equal_owned_values() {
        let buf = String::from("héllo");
        let text = QueryValueRef::Text(buf.as_str());
        assert_eq!(text.to_owned_value(), QueryValue::Text("héllo".to_string()));

        let raw_buf = [0xDEu8, 0xAD, 0xBE, 0xEF];
        let raw = QueryValueRef::Raw(&raw_buf);
        assert_eq!(
            raw.to_owned_value(),
            QueryValue::Raw(vec![0xDE, 0xAD, 0xBE, 0xEF])
        );

        let num_buf = String::from("-12.5");
        let number = QueryValueRef::Number {
            text: num_buf.as_str(),
            is_integer: false,
        };
        assert_eq!(
            number.to_owned_value(),
            QueryValue::number_from_text("-12.5", false)
        );

        let boolean = QueryValueRef::Boolean(true);
        assert_eq!(boolean.to_owned_value(), QueryValue::Boolean(true));

        let ds = QueryValueRef::IntervalDS {
            days: 1,
            hours: 2,
            minutes: 3,
            seconds: 4,
            fseconds: 5,
        };
        assert_eq!(
            ds.to_owned_value(),
            QueryValue::IntervalDS {
                days: 1,
                hours: 2,
                minutes: 3,
                seconds: 4,
                fseconds: 5,
            }
        );
    }

    // `QueryValueRef` is a small `Copy` value: it carries borrowed scalars only,
    // never owned heap payloads. Hold the line at 32 bytes (the owned enum's
    // footprint) so the borrowed row `Vec` stays cache-friendly. Cold variants
    // are boxed-owned behind a pointer.
    #[test]
    fn query_value_ref_is_small_and_copy() {
        const fn is_copy<T: Copy>() {}
        is_copy::<QueryValueRef<'static>>();
        assert!(core::mem::size_of::<QueryValueRef<'static>>() <= 32);
    }
}
