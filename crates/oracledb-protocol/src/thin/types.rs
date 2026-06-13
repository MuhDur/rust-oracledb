#![forbid(unsafe_code)]

use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptInfo {
    pub protocol_version: u16,
    pub protocol_options: u16,
    pub sdu: u32,
    pub supports_fast_auth: bool,
    pub supports_oob_check: bool,
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
    pub name: String,
    pub ora_type_num: u8,
    pub csfrm: u8,
    pub precision: i8,
    pub scale: i8,
    pub buffer_size: u32,
    pub max_size: u32,
    pub nulls_allowed: bool,
    pub is_json: bool,
    pub is_oson: bool,
    pub object_schema: Option<String>,
    pub object_type_name: Option<String>,
    pub is_array: bool,
    /// VECTOR columns only: the fixed dimension count, or `None` for a
    /// flexible-dimension column (server sends 0).
    pub vector_dimensions: Option<u32>,
    /// VECTOR columns only: the storage format byte (`VECTOR_FORMAT_*`); 0
    /// for a flexible-format column.
    pub vector_format: u8,
    /// VECTOR columns only: the metadata flags byte (sparse / flexible).
    pub vector_flags: u8,
    /// SQL data-use-case domain schema (23ai+), or `None` if the column has no
    /// domain.
    pub domain_schema: Option<String>,
    /// SQL data-use-case domain name (23ai+), or `None` if the column has no
    /// domain.
    pub domain_name: Option<String>,
    /// Ordered column annotations (23ai+), as (key, value) pairs preserving
    /// server order; `None` if the column has no annotations. A null annotation
    /// value is normalized to an empty string, matching python-oracledb.
    pub annotations: Option<Vec<(String, String)>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BindTypeInfo {
    pub ora_type_num: u8,
    pub csfrm: u8,
    pub buffer_size: u32,
}

// `Eq` is intentionally omitted: VECTOR values carry floating-point elements,
// which only implement `PartialEq`.
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
    Number {
        text: String,
        is_integer: bool,
    },
    /// Native Oracle `DB_TYPE_BOOLEAN` (`ora_type_num` 252, 23ai+): surfaced as
    /// a Python `bool` rather than an integer.
    Boolean(bool),
    Cursor {
        columns: Vec<ColumnMetadata>,
        cursor_id: u32,
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
    Object {
        schema: Option<String>,
        type_name: Option<String>,
        packed_data: Vec<u8>,
    },
    Lob {
        ora_type_num: u8,
        csfrm: u8,
        locator: Vec<u8>,
        size: u64,
        chunk_size: u32,
    },
    Vector(crate::vector::Vector),
    /// Native Oracle JSON (`DB_TYPE_JSON`, `ora_type_num` 119): the OSON image
    /// is decoded eagerly into the lossless [`crate::oson::OsonValue`] tree.
    Json(crate::oson::OsonValue),
    Array(Vec<Option<QueryValue>>),
}

impl QueryValue {
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
            QueryValue::Number { text, .. } => text.parse::<i64>().ok(),
            QueryValue::Boolean(value) => Some(i64::from(*value)),
            _ => None,
        }
    }

    /// Interpret this value as an `f64`. Works for `NUMBER`, `BINARY_DOUBLE`
    /// and `BINARY_FLOAT` (all carried as canonical text), otherwise `None`.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            QueryValue::Number { text, .. } => text.parse::<f64>().ok(),
            QueryValue::BinaryDouble(text) => text.parse::<f64>().ok(),
            _ => None,
        }
    }

    /// Borrow the canonical decimal text of a `NUMBER` value (lossless,
    /// arbitrary precision), otherwise `None`.
    pub fn as_number_text(&self) -> Option<&str> {
        match self {
            QueryValue::Number { text, .. } => Some(text.as_str()),
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
    pub batcherrors: bool,
    pub arraydmlrowcounts: bool,
    /// Parse/describe without executing (reference `parse_only`).
    pub parse_only: bool,
    /// Pipeline token; pipelined operations carry tokens 1..N
    /// (impl/thin/connection.pyx `_create_messages_for_pipeline`),
    /// everything else carries 0.
    pub token_num: u64,
    /// Server cursor id of an already-parsed statement; non-zero skips the
    /// PARSE option and SQL text (reference Statement._cursor_id).
    pub cursor_id: u32,
    /// Whether the statement may be kept in the connection statement cache
    /// (reference `cursor.prepare(cache_statement=...)`).
    pub cache_statement: bool,
    /// Whether the cursor was opened scrollable; sets the scrollable execute
    /// flags and primes the fetch orientation (reference `cursor_impl.scrollable`).
    pub scrollable: bool,
    /// Fetch orientation for the next fetch (reference `fetch_orientation`,
    /// al8i4[10]); one of the `TNS_FETCH_ORIENTATION_*` constants. Zero leaves
    /// the server default.
    pub fetch_orientation: u32,
    /// Desired row position paired with `fetch_orientation` (reference
    /// `fetch_pos`, al8i4[11]).
    pub fetch_pos: u32,
    /// True when this execute is a scroll request: the EXECUTE/BIND options are
    /// suppressed so the server only repositions the open cursor and fetches
    /// (reference `scroll_operation`).
    pub scroll_operation: bool,
    /// Suspend the active sessionless transaction once this execute succeeds
    /// (reference `cursor_impl.suspend_on_success`); the driver folds a
    /// post-detach into the sessionless piggyback. Does not affect the execute
    /// wire body itself.
    pub suspend_on_success: bool,
    /// Suppress the FETCH execute option so the server does not prefetch any
    /// rows during the execute round trip (reference `stmt._no_prefetch`,
    /// execute.pyx:99). Set when re-executing an open cursor whose columns
    /// require a client-side define (VECTOR): a prefetched row would otherwise
    /// exhaust the cursor before the define-fetch runs, yielding ORA-01002 on
    /// the subsequent fetch.
    pub no_prefetch: bool,
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
        }
    }
}

/// One batch error entry from `executemany(batcherrors=True)` (reference
/// impl/thin/messages/base.pyx batch error codes/offsets/messages arrays).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BatchServerError {
    pub code: u32,
    pub offset: u32,
    pub message: String,
}

#[cfg(test)]
mod accessor_tests {
    use super::*;

    #[test]
    fn query_value_typed_accessors() {
        let text = QueryValue::Text("hello".to_string());
        assert_eq!(text.as_text(), Some("hello"));
        assert_eq!(text.as_i64(), None);

        let int = QueryValue::Number {
            text: "42".to_string(),
            is_integer: true,
        };
        assert_eq!(int.as_i64(), Some(42));
        assert_eq!(int.as_f64(), Some(42.0));
        assert_eq!(int.as_number_text(), Some("42"));

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
            rows: vec![vec![
                Some(QueryValue::Number {
                    text: "7".to_string(),
                    is_integer: true,
                }),
                None,
            ]],
            ..QueryResult::default()
        };
        assert_eq!(result.cell(0, 0).and_then(QueryValue::as_i64), Some(7));
        assert!(result.cell(0, 1).is_none(), "NULL cell is None");
        assert!(result.cell(1, 0).is_none(), "out-of-range row is None");
        assert_eq!(result.column_index("name"), Some(1));
        assert_eq!(result.column_index("missing"), None);
    }
}
