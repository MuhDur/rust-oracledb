//! A pure-Rust, thin-mode driver for Oracle Database.
//!
//! `oracledb` speaks the Oracle TNS/TTC wire protocol directly over TCP. It
//! needs no Oracle Instant Client, no OCI libraries, and no C toolchain: add
//! the crate, point it at a listener, and connect. The driver is a faithful
//! port of the python-oracledb thin client, so its behavior tracks that
//! reference implementation.
//!
//! # Why thin mode
//!
//! A traditional OCI-based client links a large native library and inherits
//! whatever identity the OS hands it. Because this driver builds every packet
//! itself, the application controls the full connection envelope, including the
//! session identity the database records.
//!
//! # Caller-set identity (the differentiator)
//!
//! Every connection carries a [`ClientIdentity`](protocol::ClientIdentity) the
//! caller supplies: `program`, `machine`, `osuser`, and `terminal`. The
//! database stores those exact values in `v$session`. An OCI client reports the
//! host process and OS user it happens to run as; here the application decides.
//! This is invaluable for multi-tenant services and connection multiplexers
//! that need each logical user attributed correctly in the DBA's session views,
//! audit trail, and resource-manager rules.
//!
//! ```no_run
//! use oracledb::{BlockingConnection, ConnectOptions};
//! use oracledb::protocol::ClientIdentity;
//! use oracledb::protocol::thin::{BindValue, QueryValue};
//!
//! # fn main() -> Result<(), oracledb::Error> {
//! // The identity the database will record for this session.
//! let identity = ClientIdentity::new(
//!     "billing-worker", // program
//!     "edge-pod-7",     // machine
//!     "tenant-42",      // osuser
//!     "shard-a",        // terminal
//!     "rust-oracledb",  // driver name
//! )?;
//!
//! let options = ConnectOptions::new(
//!     "dbhost:1521/FREEPDB1", // EasyConnect string
//!     "app_user",
//!     "app_password",
//!     identity,
//! );
//!
//! let mut conn = BlockingConnection::connect(options)?;
//!
//! // Bind parameters positionally (:1, :2, ...).
//! let result = BlockingConnection::execute_query_with_binds(
//!     &mut conn,
//!     "select :1 + :2 from dual",
//!     1,
//!     &[
//!         BindValue::Number("40".to_string()),
//!         BindValue::Number("2".to_string()),
//!     ],
//! )?;
//!
//! // Typed accessors avoid matching the full value enum.
//! assert_eq!(result.cell(0, 0).and_then(QueryValue::as_i64), Some(42));
//!
//! BlockingConnection::close(conn)?;
//! # Ok(())
//! # }
//! ```
//!
//! # Choosing an API surface
//!
//! Two equivalent surfaces are exposed:
//!
//! - [`BlockingConnection`] runs each operation on a private single-threaded
//!   runtime and blocks the calling thread. Use it from synchronous code; it is
//!   the simplest way to use the driver as a library.
//! - [`Connection`] is the native asynchronous API. Every method takes an
//!   `&Cx` (the Asupersync request context) so the work participates in
//!   structured concurrency and cancellation. Use it inside an Asupersync
//!   runtime.
//!
//! `BlockingConnection` is a thin shim over `Connection`: it owns a
//! [`Connection`] and drives the async methods to completion.
//!
//! # Working with values
//!
//! Fetched cells are [`QueryValue`](protocol::thin::QueryValue), a sum type
//! over every Oracle scalar (NUMBER carried as lossless text, VARCHAR2, DATE /
//! TIMESTAMP, RAW, ROWID, BOOLEAN, BINARY_DOUBLE, VECTOR, JSON, LOB locators,
//! object images, ...). Convenience accessors
//! ([`as_i64`](protocol::thin::QueryValue::as_i64),
//! [`as_text`](protocol::thin::QueryValue::as_text),
//! [`as_f64`](protocol::thin::QueryValue::as_f64), and friends) and
//! [`QueryResult::cell`](protocol::thin::QueryResult::cell) cover the common
//! cases without an explicit `match`.
//!
//! Columns that stream their value through a client-side define (`CLOB`,
//! `BLOB`, `VECTOR`, native `JSON`) come back from a plain
//! [`Connection::execute_query`] as describe-only metadata with a `None` cell,
//! matching the wire protocol. Use
//! [`execute_query_collect`](Connection::execute_query_collect) to fetch the
//! first batch with those cells fully materialized in a single call.
//!
//! # Optional features
//!
//! - `arrow`: fetch result sets directly into Apache Arrow `RecordBatch`es via
//!   [`Connection::fetch_all_record_batch`] and
//!   [`Connection::fetch_record_batches`].
//!
//! # Connection pooling
//!
//! The [`pool`] module provides a connection-pool engine (`PoolEngine`) that
//! mirrors python-oracledb's thin pool: free/busy lists, growth planning,
//! getmode semantics, ping policy, idle timeout, and max lifetime. The engine
//! is generic over a [`PoolBackend`](pool::PoolBackend) so the embedder
//! supplies how a pooled connection is created, pinged, and closed.
#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::process;
use std::sync::Arc;
use std::time::Duration;

use asupersync::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use asupersync::net::{OwnedReadHalf, OwnedWriteHalf, TcpStream};
use asupersync::runtime::{reactor, Runtime, RuntimeBuilder};
use asupersync::sync::Mutex as AsyncMutex;
use asupersync::{time, Cx};
use oracledb_protocol::thin::aq::{
    build_aq_array_deq_payload, build_aq_array_enq_payload, build_aq_deq_payload,
    build_aq_enq_payload, parse_aq_array_response, parse_aq_deq_response, parse_aq_enq_response,
    AqArrayResult, AqDeqOptions, AqDeqResult, AqEnqOptions, AqMsgProps, AqQueueDesc,
};
use oracledb_protocol::thin::{
    adjust_refetch_metadata, build_auth_phase_two_payload_with_proxy_with_seq,
    build_begin_pipeline_piggyback, build_change_password_payload_with_seq,
    build_connect_packet_payload, build_define_fetch_payload_with_seq,
    build_end_pipeline_payload_with_seq, build_execute_payload_with_bind_rows_and_options_with_seq,
    build_execute_payload_with_bind_rows_with_seq_and_token, build_execute_payload_with_seq,
    build_fast_auth_phase_one_payload, build_fetch_payload_with_seq,
    build_function_payload_with_seq, build_function_payload_with_seq_and_token,
    build_lob_create_temp_payload_with_seq, build_lob_free_temp_payload_with_seq,
    build_lob_read_payload_with_seq, build_lob_trim_payload_with_seq,
    build_lob_write_payload_with_seq, parse_accept_payload, parse_auth_response,
    parse_fetch_response_with_context, parse_lob_create_temp_response,
    parse_lob_free_temp_response, parse_lob_read_response, parse_lob_trim_response,
    parse_lob_write_response, parse_plain_function_response, parse_query_response,
    parse_query_response_with_binds_options_and_columns, parse_tpc_txn_switch_response, BindValue,
    ClientCapabilities, ColumnMetadata, ExecuteOptions, LobReadResult, QueryResult,
    SessionlessTxnState, TNS_DATA_FLAGS_BEGIN_PIPELINE, TNS_DATA_FLAGS_END_OF_REQUEST,
    TNS_FUNC_COMMIT, TNS_FUNC_LOGOFF, TNS_FUNC_PING, TNS_FUNC_ROLLBACK,
    TNS_MSG_TYPE_END_OF_RESPONSE, TNS_MSG_TYPE_FLUSH_OUT_BINDS, TNS_PACKET_TYPE_ACCEPT,
    TNS_PACKET_TYPE_CONNECT, TNS_PACKET_TYPE_DATA, TNS_PACKET_TYPE_REDIRECT,
    TNS_PACKET_TYPE_REFUSE, TNS_PIPELINE_MODE_ABORT_ON_ERROR, TNS_PIPELINE_MODE_CONTINUE_ON_ERROR,
    TNS_TPC_TXN_DETACH, TNS_TPC_TXN_POST_DETACH, TNS_TPC_TXN_START, TPC_TXN_FLAGS_NEW,
    TPC_TXN_FLAGS_RESUME, TPC_TXN_FLAGS_SESSIONLESS,
};
use oracledb_protocol::thin::{build_sessionless_piggyback, build_tpc_txn_switch_payload_with_seq};
use oracledb_protocol::thin::{TNS_AQ_ARRAY_DEQ, TNS_AQ_ARRAY_ENQ};
use oracledb_protocol::wire::{encode_packet, PacketLengthWidth};
use oracledb_protocol::{net::EasyConnect, ClientIdentity};

const PYTHON_ORACLEDB_COMPAT_VERSION_NUM: u32 = 0x0400_1000;
const DEFAULT_SDU: usize = 8192;
const TNS_DATA_PACKET_OVERHEAD: usize = 10;

pub use oracledb_protocol as protocol;

#[cfg(feature = "arrow")]
pub mod arrow;
pub mod pool;

type SharedWriteHalf = Arc<AsyncMutex<OwnedWriteHalf>>;

/// Oracle error codes that python-oracledb maps to DPY-4011 (connection
/// closed); seeing one of these marks the connection as dead so pools can
/// discard it on release (reference `errors.ERR_ORACLE_ERROR_XREF`).
const SESSION_DEAD_ORA_CODES: &[u32] = &[
    22, 28, 31, 45, 378, 600, 602, 603, 609, 1012, 1041, 1043, 1089, 1092, 2396, 3113, 3114, 3122,
    3135, 12153, 12537, 12547, 12570, 12583, 27146, 28511, 56600,
];

/// TTC field-version threshold where the database version number encoding
/// changed (reference thin/constants.pxi `TNS_CCAP_FIELD_VERSION_18_1_EXT_1`).
const TNS_CCAP_FIELD_VERSION_18_1_EXT_1: u8 = 11;

/// Decode the packed `AUTH_VERSION_NO` value into the database version
/// 5-tuple. The bit layout changed with Oracle Database 18
/// (reference messages/auth.pyx `_get_version_tuple`).
fn decode_server_version_number(full: u32, new_format: bool) -> (u8, u8, u8, u8, u8) {
    if new_format {
        (
            ((full >> 24) & 0xFF) as u8,
            ((full >> 16) & 0xFF) as u8,
            ((full >> 12) & 0x0F) as u8,
            ((full >> 4) & 0xFF) as u8,
            (full & 0x0F) as u8,
        )
    } else {
        (
            ((full >> 24) & 0xFF) as u8,
            ((full >> 20) & 0x0F) as u8,
            ((full >> 12) & 0x0F) as u8,
            ((full >> 8) & 0x0F) as u8,
            (full & 0x0F) as u8,
        )
    }
}

fn protocol_error_is_session_dead(err: &oracledb_protocol::ProtocolError) -> bool {
    let message = match err {
        oracledb_protocol::ProtocolError::ServerError(message) => message,
        oracledb_protocol::ProtocolError::ServerErrorWithRowCount { message, .. } => message,
        _ => return false,
    };
    let Some(start) = message.find("ORA-") else {
        return false;
    };
    let digits = message[start + 4..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    digits
        .parse::<u32>()
        .is_ok_and(|code| SESSION_DEAD_ORA_CODES.contains(&code))
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Protocol(#[from] oracledb_protocol::ProtocolError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("asupersync runtime error: {0}")]
    Runtime(String),
    #[error("listener redirected this connection; redirect handling is not implemented yet")]
    RedirectUnsupported,
    #[error("listener refused connection: {0}")]
    ListenerRefused(String),
    #[error("server did not advertise fast authentication")]
    FastAuthRequired,
    #[error("server response did not contain {0}")]
    MissingSessionField(&'static str),
    #[error("call timeout of {0} ms exceeded")]
    CallTimeout(u32),
    /// A sessionless transaction client-API misuse (reference
    /// ERR_SESSIONLESS_* / DPY-3034/3035/3036). The payload is the DPY full
    /// code so the shim can raise the matching DatabaseError.
    #[error("{0}")]
    SessionlessTransaction(SessionlessError),
    #[cfg(feature = "arrow")]
    #[error(transparent)]
    ArrowConversion(#[from] arrow::ArrowConversionError),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Client-API misuse of the sessionless transaction API, mirroring the
/// reference `ERR_SESSIONLESS_*` errors (impl/oracledb/errors.py:338-340).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionlessError {
    /// DPY-3034: suspend/resume was attempted on a transaction started with
    /// DBMS_TRANSACTION (or vice versa).
    DifferingMethods,
    /// DPY-3035: a sessionless transaction is already active on the connection.
    AlreadyActive,
    /// DPY-3036: no sessionless transaction is active on the connection.
    Inactive,
}

impl SessionlessError {
    /// The DPY full code (reference errors.py full codes).
    pub fn full_code(self) -> &'static str {
        match self {
            Self::DifferingMethods => "DPY-3034",
            Self::AlreadyActive => "DPY-3035",
            Self::Inactive => "DPY-3036",
        }
    }

    /// The reference error message text (errors.py:945-953).
    pub fn message(self) -> &'static str {
        match self {
            Self::DifferingMethods => {
                "suspending or resuming a Sessionless Transaction can be done with \
                 DBMS_TRANSACTION or with python-oracledb, but not both"
            }
            Self::AlreadyActive => {
                "suspend, commit, or rollback the current active sessionless \
                 transaction before beginning or resuming another one"
            }
            Self::Inactive => "no Sessionless Transaction is active",
        }
    }
}

impl std::fmt::Display for SessionlessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.full_code(), self.message())
    }
}

/// Whether an execute error is the server signalling that the cached
/// statement's types no longer match (ORA-00932 inconsistent datatypes /
/// ORA-01007 variable not in select list), the two errors the reference
/// retries with a full parse (impl/thin/constants.pxi:166-167,
/// messages/base.pyx:1199-1213).
fn refetch_retry_applies(err: &Error) -> bool {
    let message = match err {
        Error::Protocol(oracledb_protocol::ProtocolError::ServerError(message)) => message,
        Error::Protocol(oracledb_protocol::ProtocolError::ServerErrorWithRowCount {
            message,
            ..
        }) => message,
        Error::Protocol(oracledb_protocol::ProtocolError::ServerErrorInfo(details)) => {
            // structured error path: match by ORA code directly (ORA-00932
            // inconsistent data types / ORA-01007 variable not in select list)
            return details.code == 932 || details.code == 1007;
        }
        _ => return false,
    };
    message.starts_with("ORA-00932") || message.starts_with("ORA-01007")
}

/// Everything needed to open a connection: where to connect, who to
/// authenticate as, and the [`ClientIdentity`] the database will record.
///
/// Build the required fields with [`ConnectOptions::new`], then layer optional
/// settings with the `with_*` methods.
#[derive(Clone, Debug)]
pub struct ConnectOptions {
    /// EasyConnect descriptor, `host:port/service_name` (the port and service
    /// may be omitted to take the listener defaults).
    pub connect_string: String,
    /// Database user to authenticate as.
    pub user: String,
    /// Password for `user`.
    pub password: String,
    /// Session identity reported to the database (`v$session`).
    pub identity: ClientIdentity,
    /// Application-context triples `(namespace, key, value)` set on the
    /// session at logon (reference `connection.appcontext`).
    pub app_context: Vec<(String, String, String)>,
    /// Session Data Unit (negotiated packet size) in bytes.
    pub sdu: u16,
    /// Proxy user for `[proxy_user]` style connections, if any.
    pub proxy_user: Option<String>,
}

impl ConnectOptions {
    /// Create connect options with the required fields. `connect_string` is an
    /// EasyConnect descriptor (`host:port/service_name`); `identity` is the
    /// session identity the database will record. Optional settings default to
    /// an 8 KiB SDU, no application context, and no proxy user.
    pub fn new(
        connect_string: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
        identity: ClientIdentity,
    ) -> Self {
        Self {
            connect_string: connect_string.into(),
            user: user.into(),
            password: password.into(),
            identity,
            app_context: Vec::new(),
            sdu: 8192,
            proxy_user: None,
        }
    }

    /// Set the application-context triples applied at logon.
    pub fn with_app_context(mut self, app_context: Vec<(String, String, String)>) -> Self {
        self.app_context = app_context;
        self
    }

    /// Set the proxy user for `[proxy_user]` style authentication.
    pub fn with_proxy_user(mut self, proxy_user: Option<String>) -> Self {
        self.proxy_user = proxy_user;
        self
    }

    /// Request a Session Data Unit size, clamped to the protocol-legal range
    /// `512..=65535` bytes. The value is a hint; the server negotiates the
    /// effective SDU at connect time.
    pub fn with_sdu(mut self, sdu: u32) -> Self {
        let clamped = sdu.clamp(512, u32::from(u16::MAX));
        self.sdu = u16::try_from(clamped).unwrap_or(u16::MAX);
        self
    }
}

/// A live asynchronous connection to an Oracle Database session.
///
/// Every method takes an `&Cx` and runs on an Asupersync runtime. For
/// synchronous code use [`BlockingConnection`], which owns a `Connection` and
/// drives these methods to completion on a private runtime.
///
/// A connection is not `Clone` and is not safe to use from two tasks at once;
/// drive one operation to completion before starting the next. To pool
/// connections, use the [`pool`] engine.
#[derive(Debug)]
pub struct Connection {
    descriptor: EasyConnect,
    identity: ClientIdentity,
    read: OwnedReadHalf,
    write: SharedWriteHalf,
    session_id: u32,
    serial_num: u16,
    server_version: Option<String>,
    server_version_tuple: Option<(u8, u8, u8, u8, u8)>,
    capabilities: ClientCapabilities,
    ttc_seq_num: u8,
    sdu: usize,
    supports_end_of_response: bool,
    cursor_columns: BTreeMap<u32, Vec<ColumnMetadata>>,
    /// Fetch metadata of the most recent execution keyed by SQL text,
    /// mirroring the reference statement cache's per-statement
    /// `_fetch_var_impls` retention (impl/thin/statement.pyx:300-310) that
    /// drives the re-execute type-change adjustment.
    fetch_metadata_by_sql: HashMap<String, Vec<ColumnMetadata>>,
    /// Insertion order for [`Self::fetch_metadata_by_sql`] eviction.
    fetch_metadata_order: VecDeque<String>,
    dead: bool,
    /// Logon user, retained for the change-password call.
    user: String,
    /// Session combo key from verifier generation, retained for the
    /// change-password call (reference keeps `conn_impl._combo_key`).
    combo_key: Vec<u8>,
    /// LRU statement cache: SQL text -> open server cursor id (reference
    /// thin/statement_cache.pyx, default size 20).
    statement_cache: Vec<(String, u32)>,
    /// Server cursor ids currently held by a live cursor (reference
    /// `Statement._in_use`). A cached cursor whose id is in this set must NOT
    /// be reused by a second cursor: `get_statement` returns a fresh
    /// (re-parsed) cursor instead, so interleaved fetches on different cursors
    /// of the same connection cannot reset each other's server-side fetch
    /// position (ORA-01002 fetch out of sequence). Cleared when the owning
    /// cursor releases the id (close / re-prepare to a different statement).
    in_use_cursors: HashSet<u32>,
    /// Server cursor ids that were parsed as a fresh copy because the cached
    /// statement was in use (reference statement with `_return_to_cache =
    /// False`). These are never returned to the statement cache; when the
    /// owning cursor releases the id it is queued for close instead of being
    /// kept open (reference `return_statement` -> `_add_cursor_to_close`).
    copied_cursors: HashSet<u32>,
    /// Server cursor ids queued for the close-cursors piggyback (reference
    /// `_cursors_to_close`).
    cursors_to_close: Vec<u32>,
    /// State of the active sessionless transaction (reference
    /// `BaseThinConnImpl._sessionless_data`); `None` when no sessionless
    /// transaction is active on this connection.
    sessionless_data: Option<SessionlessData>,
}

/// Mirrors the reference `_SessionlessData` (impl/thin/connection.pyx): the
/// pending or active sessionless transaction tracked on the connection.
#[derive(Clone, Debug)]
struct SessionlessData {
    transaction_id: Vec<u8>,
    timeout: u32,
    /// One of `TNS_TPC_TXN_START` / `TNS_TPC_TXN_DETACH`, optionally OR'd with
    /// `TNS_TPC_TXN_POST_DETACH` once a suspend-on-success is folded in.
    operation: u32,
    /// `TPC_TXN_FLAGS_NEW` or `TPC_TXN_FLAGS_RESUME` (SESSIONLESS is added when
    /// the message is built).
    flags: u32,
    /// A begin/resume that must ride as a piggyback on the next execute
    /// (`defer_round_trip=True`, or a folded-in suspend-on-success).
    piggyback_pending: bool,
    /// The transaction was started via DBMS_TRANSACTION on the server; the
    /// client API may not suspend/resume it (reference `started_on_server`).
    started_on_server: bool,
}

const STATEMENT_CACHE_SIZE: usize = 20;

/// One operation in a pipelined batch (`Connection::run_pipeline`).
#[derive(Clone, Debug)]
pub enum PipelineRequest {
    Execute {
        sql: String,
        bind_rows: Vec<Vec<BindValue>>,
        prefetch_rows: u32,
    },
    Commit,
}

#[derive(Debug)]
pub struct CancelHandle {
    write: SharedWriteHalf,
}

impl Connection {
    /// Open a connection: resolve the EasyConnect descriptor, complete the TNS
    /// handshake and TTC capability negotiation, and authenticate `user` with
    /// the supplied [`ClientIdentity`]. On success the database has recorded a
    /// session whose `program` / `machine` / `osuser` / `terminal` are exactly
    /// the identity fields.
    pub async fn connect(cx: &Cx, options: ConnectOptions) -> Result<Self> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let descriptor = EasyConnect::parse(&options.connect_string)?;
        let identity = options.identity;
        trace_connect_step("tcp connect");
        let stream = TcpStream::connect_timeout(
            (descriptor.host.clone(), descriptor.port),
            Duration::from_secs(20),
        )
        .await?;
        stream.set_nodelay(true)?;
        let (mut read, write) = stream.into_split();
        let write = Arc::new(AsyncMutex::with_name("oracle_tcp_write", write));
        trace_connect_step("tcp connected");

        let connect_descriptor = listener_connect_descriptor(&descriptor, &identity);
        trace_connect_value("CONNECT descriptor", &connect_descriptor);
        let connect_payload = build_connect_packet_payload(&connect_descriptor, options.sdu)?;
        let packet = encode_packet(
            TNS_PACKET_TYPE_CONNECT,
            0,
            None,
            &connect_payload,
            PacketLengthWidth::Legacy16,
        )?;
        trace_connect_bytes("CONNECT packet", &packet);
        trace_connect_step("send CONNECT");
        write_all_shared(cx, &write, &packet).await?;

        trace_connect_step("read ACCEPT");
        let accept = read_packet(&mut read, PacketLengthWidth::Legacy16).await?;
        match accept.packet_type {
            TNS_PACKET_TYPE_ACCEPT => {}
            TNS_PACKET_TYPE_REDIRECT => return Err(Error::RedirectUnsupported),
            TNS_PACKET_TYPE_REFUSE => {
                return Err(Error::ListenerRefused(
                    String::from_utf8_lossy(&accept.payload).to_string(),
                ))
            }
            other => {
                return Err(oracledb_protocol::ProtocolError::UnknownMessageType {
                    message_type: other,
                    position: 4,
                }
                .into())
            }
        }
        let accept_info = parse_accept_payload(&accept.payload)?;
        if !accept_info.supports_fast_auth {
            return Err(Error::FastAuthRequired);
        }
        let sdu = usize::try_from(accept_info.sdu)
            .unwrap_or(DEFAULT_SDU)
            .max(TNS_DATA_PACKET_OVERHEAD + 1);

        let client_pid = process::id();
        let auth_one = build_fast_auth_phase_one_payload(
            &options.user,
            &identity.program,
            &identity.machine,
            &identity.osuser,
            &identity.terminal,
            client_pid,
        )?;
        trace_connect_bytes("AUTH phase one payload", &auth_one);
        trace_connect_step("send AUTH phase one");
        send_data_packet_shared(cx, &write, &auth_one, sdu).await?;
        trace_connect_step("read AUTH phase one");
        let auth_one_response = read_data_response(&mut read, cx, &write).await?;
        trace_connect_bytes("AUTH phase one response", &auth_one_response);
        let auth_one = parse_auth_response(&auth_one_response)?;
        let capabilities = auth_one.capabilities.unwrap_or_default();
        let mut ttc_seq_num = 1;
        let verifier_type = auth_one
            .verifier_type
            .ok_or(Error::MissingSessionField("AUTH_VFR_DATA verifier type"))?;
        let encrypted = oracledb_protocol::crypto::generate_verifier(
            options.password.as_bytes(),
            &auth_one.session_data,
            verifier_type,
        )?;
        let auth_connect_string = auth_connect_descriptor(&descriptor);
        let auth_two = build_auth_phase_two_payload_with_proxy_with_seq(
            &options.user,
            &encrypted,
            &identity.driver_name,
            PYTHON_ORACLEDB_COMPAT_VERSION_NUM,
            &auth_connect_string,
            next_ttc_sequence(&mut ttc_seq_num),
            &options.app_context,
            options.proxy_user.as_deref(),
        )?;
        trace_connect_bytes("AUTH phase two payload", &auth_two);
        trace_connect_step("send AUTH phase two");
        send_data_packet_shared(cx, &write, &auth_two, sdu).await?;
        trace_connect_step("read AUTH phase two");
        let auth_two_response = read_data_response(&mut read, cx, &write).await?;
        trace_connect_bytes("AUTH phase two response", &auth_two_response);
        let auth_two = parse_auth_response(&auth_two_response)?;
        oracledb_protocol::crypto::verify_server_response(
            &encrypted.combo_key,
            &auth_two.session_data,
        )?;

        let session_id = parse_session_u32(&auth_two.session_data, "AUTH_SESSION_ID")?;
        let serial_num = parse_session_u16(&auth_two.session_data, "AUTH_SERIAL_NUM")?;
        let server_version = auth_two.session_data.get("AUTH_VERSION_STRING").cloned();
        let server_version_tuple = auth_two
            .session_data
            .get("AUTH_VERSION_NO")
            .and_then(|value| value.trim().parse::<u32>().ok())
            .map(|num| {
                decode_server_version_number(
                    num,
                    capabilities.ttc_field_version >= TNS_CCAP_FIELD_VERSION_18_1_EXT_1,
                )
            });

        Ok(Self {
            descriptor,
            identity,
            read,
            write,
            session_id,
            serial_num,
            server_version,
            server_version_tuple,
            capabilities,
            ttc_seq_num,
            sdu,
            supports_end_of_response: accept_info.supports_end_of_response,
            cursor_columns: BTreeMap::new(),
            fetch_metadata_by_sql: HashMap::new(),
            fetch_metadata_order: VecDeque::new(),
            dead: false,
            user: options.user,
            combo_key: encrypted.combo_key,
            statement_cache: Vec::new(),
            in_use_cursors: HashSet::new(),
            copied_cursors: HashSet::new(),
            cursors_to_close: Vec::new(),
            sessionless_data: None,
        })
    }

    pub fn descriptor(&self) -> &EasyConnect {
        &self.descriptor
    }

    /// The [`ClientIdentity`] this session was opened with (the values the
    /// database recorded in `v$session`).
    pub fn identity(&self) -> &ClientIdentity {
        &self.identity
    }

    /// Server-assigned session id (`v$session.sid`).
    pub fn session_id(&self) -> u32 {
        self.session_id
    }

    /// Server-assigned session serial number (`v$session.serial#`).
    pub fn serial_num(&self) -> u16 {
        self.serial_num
    }

    /// Server version banner, if the server reported one.
    pub fn server_version(&self) -> Option<&str> {
        self.server_version.as_deref()
    }

    /// Database version 5-tuple decoded from `AUTH_VERSION_NO`
    /// (reference messages/auth.pyx `_get_version_tuple`).
    pub fn server_version_tuple(&self) -> Option<(u8, u8, u8, u8, u8)> {
        self.server_version_tuple
    }

    /// Whether the server supports OSON long field names (server major version
    /// >= 23). Mirrors the reference `conn_impl.supports_oson_long_field_names`.
    fn supports_oson_long_fnames(&self) -> bool {
        self.server_version_tuple
            .map(|(major, ..)| major >= 23)
            .unwrap_or(false)
    }

    pub fn sdu(&self) -> usize {
        self.sdu
    }

    /// Whether the server negotiated END_OF_RESPONSE framing at accept time
    /// (protocol version >= 319 with TNS_ACCEPT_FLAG_HAS_END_OF_RESPONSE) --
    /// the prerequisite for pipelining (impl/thin/capabilities.pyx:126-130).
    pub fn supports_pipelining(&self) -> bool {
        self.supports_end_of_response
    }

    pub fn cancel_handle(&self) -> Result<CancelHandle> {
        Ok(CancelHandle {
            write: Arc::clone(&self.write),
        })
    }

    /// Whether a session-dead Oracle error (mapped to DPY-4011 by the Python
    /// layer) has been observed on this connection.
    pub fn is_dead(&self) -> bool {
        self.dead
    }

    /// Wrap a protocol parse result, recording session-dead errors.
    fn note_parse<T>(
        &mut self,
        result: std::result::Result<T, oracledb_protocol::ProtocolError>,
    ) -> Result<T> {
        match result {
            Ok(value) => Ok(value),
            Err(err) => {
                if protocol_error_is_session_dead(&err) {
                    self.dead = true;
                }
                Err(Error::Protocol(err))
            }
        }
    }

    /// Round-trip a lightweight PING to verify the session is alive.
    pub async fn ping(&mut self, cx: &Cx) -> Result<()> {
        self.send_function(cx, TNS_FUNC_PING).await
    }

    /// Change the session password via the dedicated auth round trip
    /// (reference `ThinConnImpl.change_password`): an AUTH_PHASE_TWO message
    /// carrying the combo-key-encrypted old/new passwords. Server errors
    /// (ORA-28218, ORA-01017, ...) surface unchanged.
    pub async fn change_password(
        &mut self,
        cx: &Cx,
        old_password: &str,
        new_password: &str,
    ) -> Result<()> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let (encoded_password, encoded_newpassword) =
            oracledb_protocol::crypto::encrypt_change_password_pair(
                &self.combo_key,
                old_password.as_bytes(),
                new_password.as_bytes(),
            )?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_change_password_payload_with_seq(
            &self.user,
            &encoded_password,
            &encoded_newpassword,
            seq_num,
        )?;
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        self.note_parse(parse_auth_response(&response).map(|_| ()))?;
        Ok(())
    }

    /// Ping with an upper bound on the round trip, used by pool health
    /// checks (reference pings under `ping_timeout`).
    pub async fn ping_with_timeout(&mut self, cx: &Cx, timeout_ms: u32) -> Result<()> {
        if timeout_ms == 0 {
            return self.ping(cx).await;
        }
        match time::timeout(
            time::wall_now(),
            Duration::from_millis(u64::from(timeout_ms)),
            self.ping(cx),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(Error::CallTimeout(timeout_ms)),
        }
    }

    /// Commit the current transaction. DML on a connection is not durable
    /// until committed.
    pub async fn commit(&mut self, cx: &Cx) -> Result<()> {
        self.send_function(cx, TNS_FUNC_COMMIT).await?;
        // a commit ends any active sessionless transaction on the server
        // (reference clears `_sessionless_data` via the SYNC piggyback)
        self.sessionless_data = None;
        Ok(())
    }

    /// Roll back the current transaction, discarding uncommitted DML.
    pub async fn rollback(&mut self, cx: &Cx) -> Result<()> {
        self.send_function(cx, TNS_FUNC_ROLLBACK).await?;
        self.sessionless_data = None;
        Ok(())
    }

    /// Begins (`flags = TPC_TXN_FLAGS_NEW`) or resumes
    /// (`flags = TPC_TXN_FLAGS_RESUME`) a sessionless transaction. With
    /// `defer_round_trip = false` the request is sent immediately; with `true`
    /// it is queued as a piggyback on the next execute (reference
    /// impl/thin/connection.pyx `_start_sessionless_transaction`).
    async fn start_sessionless_transaction(
        &mut self,
        cx: &Cx,
        transaction_id: &[u8],
        timeout: u32,
        flags: u32,
        defer_round_trip: bool,
    ) -> Result<()> {
        if self.sessionless_data.is_some() {
            return Err(Error::SessionlessTransaction(
                SessionlessError::AlreadyActive,
            ));
        }
        let data = SessionlessData {
            transaction_id: transaction_id.to_vec(),
            timeout,
            operation: TNS_TPC_TXN_START,
            flags,
            piggyback_pending: defer_round_trip,
            started_on_server: false,
        };
        if defer_round_trip {
            // queue the begin/resume to ride on the next execute
            self.sessionless_data = Some(data);
            return Ok(());
        }
        // send the begin/resume immediately
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_tpc_txn_switch_payload_with_seq(
            seq_num,
            0,
            data.operation,
            data.flags | TPC_TXN_FLAGS_SESSIONLESS,
            data.timeout,
            Some(transaction_id),
        );
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        let state = self.note_parse(parse_tpc_txn_switch_response(&response, self.capabilities))?;
        self.sessionless_data = Some(data);
        self.apply_sessionless_state(state);
        Ok(())
    }

    /// Begins a new sessionless transaction (reference
    /// `begin_sessionless_transaction`).
    pub async fn begin_sessionless_transaction(
        &mut self,
        cx: &Cx,
        transaction_id: &[u8],
        timeout: u32,
        defer_round_trip: bool,
    ) -> Result<()> {
        self.start_sessionless_transaction(
            cx,
            transaction_id,
            timeout,
            TPC_TXN_FLAGS_NEW,
            defer_round_trip,
        )
        .await
    }

    /// Resumes an existing sessionless transaction (reference
    /// `resume_sessionless_transaction`).
    pub async fn resume_sessionless_transaction(
        &mut self,
        cx: &Cx,
        transaction_id: &[u8],
        timeout: u32,
        defer_round_trip: bool,
    ) -> Result<()> {
        self.start_sessionless_transaction(
            cx,
            transaction_id,
            timeout,
            TPC_TXN_FLAGS_RESUME,
            defer_round_trip,
        )
        .await
    }

    /// Suspends the active sessionless transaction immediately (reference
    /// `suspend_sessionless_transaction`).
    pub async fn suspend_sessionless_transaction(&mut self, cx: &Cx) -> Result<()> {
        match &self.sessionless_data {
            None => return Err(Error::SessionlessTransaction(SessionlessError::Inactive)),
            Some(data) if data.started_on_server => {
                return Err(Error::SessionlessTransaction(
                    SessionlessError::DifferingMethods,
                ));
            }
            Some(_) => {}
        }
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_tpc_txn_switch_payload_with_seq(
            seq_num,
            0,
            TNS_TPC_TXN_DETACH,
            TPC_TXN_FLAGS_SESSIONLESS,
            0,
            None,
        );
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        let state = self.note_parse(parse_tpc_txn_switch_response(&response, self.capabilities))?;
        // a suspend always clears the active transaction locally; the server's
        // SYNC piggyback confirms it (reference clears `_sessionless_data`)
        self.sessionless_data = None;
        self.apply_sessionless_state(state);
        Ok(())
    }

    /// Validate that a `suspend_on_success` request is legal and fold the
    /// post-detach into the pending sessionless piggyback (reference
    /// execute.pyx `_handle_sessionless_suspend`). Called by the cursor execute
    /// path before building the execute message.
    pub fn prepare_sessionless_suspend_on_success(&mut self) -> Result<()> {
        match &mut self.sessionless_data {
            None => Err(Error::SessionlessTransaction(SessionlessError::Inactive)),
            Some(data) if data.started_on_server => Err(Error::SessionlessTransaction(
                SessionlessError::DifferingMethods,
            )),
            Some(data) => {
                if data.piggyback_pending {
                    data.operation |= TNS_TPC_TXN_POST_DETACH;
                } else {
                    data.operation = TNS_TPC_TXN_POST_DETACH;
                    data.flags = TPC_TXN_FLAGS_SESSIONLESS;
                    data.piggyback_pending = true;
                }
                Ok(())
            }
        }
    }

    /// Take the pending sessionless piggyback bytes (if any) to prepend to the
    /// next execute payload, mirroring the close-cursors piggyback flow. The
    /// piggyback's sequence number is consumed from the connection counter so
    /// it precedes the execute's own sequence number.
    fn take_sessionless_piggyback(&mut self) -> Option<Vec<u8>> {
        let data = self.sessionless_data.as_mut()?;
        if !data.piggyback_pending {
            return None;
        }
        data.piggyback_pending = false;
        let xid = if data.operation & TNS_TPC_TXN_START != 0 {
            Some(data.transaction_id.clone())
        } else {
            None
        };
        let flags = data.flags | TPC_TXN_FLAGS_SESSIONLESS;
        let operation = data.operation;
        let timeout = data.timeout;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        Some(build_sessionless_piggyback(
            seq_num,
            0,
            operation,
            flags,
            timeout,
            xid.as_deref(),
        ))
    }

    /// Apply a sessionless state update reported by the server (via the SYNC
    /// piggyback) to the connection's tracked state (reference
    /// `_update_sessionless_txn_state`).
    fn apply_sessionless_state(&mut self, state: Option<SessionlessTxnState>) {
        match state {
            // transaction ended/suspended on the server (reference clears
            // `_sessionless_data`)
            Some(SessionlessTxnState::Unset) => self.sessionless_data = None,
            // transaction started/resumed (reference replaces `_sessionless_data`
            // with a fresh `_SessionlessData`). This also covers a transaction
            // started via DBMS_TRANSACTION on the server, where no client-side
            // data existed yet: the server SET carries `started_on_server` so a
            // later client suspend/resume correctly raises DPY-3034.
            Some(SessionlessTxnState::Set { started_on_server }) => {
                match self.sessionless_data.as_mut() {
                    Some(data) => {
                        data.started_on_server = started_on_server;
                        data.piggyback_pending = false;
                    }
                    None => {
                        self.sessionless_data = Some(SessionlessData {
                            transaction_id: Vec::new(),
                            timeout: 0,
                            operation: TNS_TPC_TXN_START,
                            flags: 0,
                            piggyback_pending: false,
                            started_on_server,
                        });
                    }
                }
            }
            None => {}
        }
    }

    /// Execute `sql` with no binds and return the first fetch batch.
    ///
    /// For a query, up to `prefetch_rows` rows are returned in the
    /// [`QueryResult`]; if [`QueryResult::more_rows`] is set, fetch the rest
    /// with [`Self::fetch_rows`] on the result's `cursor_id`. For DML/DDL the
    /// row count is in [`QueryResult::row_count`].
    ///
    /// Columns that need a client-side define (`CLOB` / `BLOB` / `VECTOR` /
    /// native `JSON`) return describe-only metadata with `None` cells here;
    /// use [`Self::execute_query_collect`] to materialize them in one call.
    pub async fn execute_query(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
    ) -> Result<QueryResult> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        // Flush any cursors queued for close (via `close_cursor`) ahead of this
        // execute: the close-cursors piggyback carries its own sequence number
        // and is prepended to the execute payload, mirroring the bind-rows
        // execute path. With no queued closes this is a no-op.
        let close_piggyback = self.take_close_cursors_piggyback();
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let mut payload =
            build_execute_payload_with_seq(sql, prefetch_rows, seq_num, statement_is_query(sql))?;
        if let Some(mut piggyback_bytes) = close_piggyback {
            piggyback_bytes.extend_from_slice(&payload);
            payload = piggyback_bytes;
        }
        trace_query_bytes("EXECUTE query payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("EXECUTE query response", &response);
        let parsed = parse_query_response(&response, self.capabilities);
        let result = self.note_parse(parsed)?;
        self.remember_cursor_columns(&result);
        Ok(result)
    }

    /// Execute `sql` and return the first fetch batch with every cell fully
    /// materialized, including columns that need a client-side define to
    /// stream their value (`CLOB` / `BLOB` / `VECTOR` / native `JSON`).
    ///
    /// Plain [`Self::execute_query`] mirrors the wire protocol exactly: for a
    /// define-requiring column it returns the describe metadata but a `None`
    /// cell, because the value only arrives after a follow-up define-fetch
    /// round trip. This convenience wrapper performs that round trip for the
    /// first batch automatically, so a standalone caller selecting such a
    /// column gets the actual value without hand-driving the cursor. For
    /// scalar-only result sets it is identical to `execute_query`.
    ///
    /// `prefetch_rows` is the requested batch size. Rows beyond the first
    /// batch (when `more_rows` is set) are fetched with the cursor's
    /// `fetch_rows` / `define_and_fetch_rows_with_columns` methods as usual.
    pub async fn execute_query_collect(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
    ) -> Result<QueryResult> {
        let mut result = self.execute_query(cx, sql, prefetch_rows).await?;
        if !columns_require_define(&result.columns) || result.cursor_id == 0 {
            return Ok(result);
        }
        // When the open server cursor already streamed rows inline (an active
        // define on a re-execute), those rows are authoritative; keep them.
        if !result.rows.is_empty() {
            return Ok(result);
        }
        let cursor_id = result.cursor_id;
        let columns = result.columns.clone();
        let fetched = self
            .define_and_fetch_rows_with_columns(cx, cursor_id, prefetch_rows.max(1), &columns, None)
            .await?;
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

    pub async fn execute_query_with_timeout(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        self.execute_query_call_timeout(cx, sql, prefetch_rows, timeout_ms)
            .await
    }

    /// Execute `sql` with one row of bind values and return the first batch.
    ///
    /// Binds are positional: `binds[0]` fills `:1` (or the first named
    /// placeholder in declaration order), `binds[1]` fills `:2`, and so on. Use
    /// [`Self::execute_query_with_bind_rows`] to run the same statement over
    /// many bind rows in a single array-DML round trip.
    pub async fn execute_query_with_binds(
        &mut self,
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
        self.execute_query_with_bind_rows_and_options(
            cx,
            sql,
            prefetch_rows,
            &bind_rows,
            ExecuteOptions::default(),
        )
        .await
    }

    pub async fn execute_query_with_binds_and_timeout(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        binds: &[BindValue],
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        self.execute_query_with_binds_call_timeout(cx, sql, prefetch_rows, binds, timeout_ms)
            .await
    }

    /// Execute `sql` once per bind row (array DML / `executemany`). Each inner
    /// `Vec<BindValue>` is one positional bind row; the server applies the
    /// statement to every row in a single round trip and reports the total in
    /// [`QueryResult::row_count`]. For per-iteration row counts or collected
    /// batch errors, use
    /// [`Self::execute_query_with_bind_rows_and_options`] with the matching
    /// [`ExecuteOptions`] flags.
    pub async fn execute_query_with_bind_rows(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        bind_rows: &[Vec<BindValue>],
    ) -> Result<QueryResult> {
        self.execute_query_with_bind_rows_and_options(
            cx,
            sql,
            prefetch_rows,
            bind_rows,
            ExecuteOptions::default(),
        )
        .await
    }

    pub async fn execute_query_with_bind_rows_and_options(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        bind_rows: &[Vec<BindValue>],
        exec_options: ExecuteOptions,
    ) -> Result<QueryResult> {
        match self
            .execute_query_with_bind_rows_options_adjusted(
                cx,
                sql,
                prefetch_rows,
                bind_rows,
                exec_options,
            )
            .await
        {
            // a query re-executed against an open server cursor whose select
            // list changed since it was parsed reports ORA-00932 (inconsistent
            // data types) or ORA-01007 (variable not in select list); the
            // reference clears the cursor and retries once with a full parse
            // (impl/thin/messages/base.pyx:1199-1213). The failing adjusted
            // call has already evicted the stale cursor from the statement
            // cache, so the retry re-parses from scratch.
            Err(err) if refetch_retry_applies(&err) && statement_is_query(sql) => {
                // also drop any retained by-SQL fetch metadata used by the
                // older refetch path so the retry rebuilds it
                self.forget_fetch_metadata(sql);
                self.execute_query_with_bind_rows_options_adjusted(
                    cx,
                    sql,
                    prefetch_rows,
                    bind_rows,
                    exec_options,
                )
                .await
            }
            other => other,
        }
    }

    async fn execute_query_with_bind_rows_options_adjusted(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        bind_rows: &[Vec<BindValue>],
        exec_options: ExecuteOptions,
    ) -> Result<QueryResult> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let mut exec_options = exec_options;
        // a `suspend_on_success` execute folds a post-detach into the pending
        // sessionless piggyback; validate (DPY-3034/3036) before any wire work
        // (reference execute.pyx `_handle_sessionless_suspend`)
        if exec_options.suspend_on_success {
            self.prepare_sessionless_suspend_on_success()?;
        }
        let use_cache = exec_options.cache_statement && !exec_options.parse_only;
        // Whether the cursor produced by this execute may be returned to the
        // statement cache (reference `Statement._return_to_cache`). A statement
        // that had to be copied because the cached cursor was in use is NOT
        // returnable: returning it would evict the still-live original from the
        // cache and reset its fetch position (ORA-01002).
        let mut is_copy = false;
        if exec_options.cursor_id == 0 && !exec_options.parse_only {
            if use_cache {
                if self.statement_is_in_use(sql) {
                    // cached cursor busy: this execute parses a fresh (copy)
                    // cursor that must not be returned to the cache
                    is_copy = true;
                } else if let Some(cursor_id) = self.statement_cache_get(sql) {
                    exec_options.cursor_id = cursor_id;
                }
            } else if let Some(cursor_id) = self.statement_cache_take(sql) {
                // reference pops the statement from the cache even when
                // cache_statement=False, reusing its open cursor once
                exec_options.cursor_id = cursor_id;
            }
        }
        // Re-executing an open cursor whose columns require a client-side define
        // (VECTOR) must suppress server-side prefetch (reference
        // `stmt._no_prefetch`, set once during describe in messages/base.pyx
        // 1159-1164 and persisted on the cached statement). Otherwise the
        // re-execute prefetches the row inline and exhausts the cursor before
        // the define-fetch runs, raising ORA-01002 on the next fetch.
        if exec_options.cursor_id != 0 && statement_is_query(sql) {
            if let Some(columns) = self.cursor_columns.get(&exec_options.cursor_id) {
                if columns.iter().any(|column| {
                    column.ora_type_num == oracledb_protocol::thin::ORA_TYPE_NUM_VECTOR
                }) {
                    exec_options.no_prefetch = true;
                }
            }
        }
        let piggyback = self.take_close_cursors_piggyback();
        if piggyback.is_none() {
            let has_ref_cursor_output = bind_rows.iter().any(|row| {
                row.iter().any(|value| {
                    matches!(
                        value,
                        BindValue::Output {
                            ora_type_num: oracledb_protocol::thin::ORA_TYPE_NUM_CURSOR,
                            ..
                        }
                    )
                })
            });
            if has_ref_cursor_output {
                // python-oracledb reserves this sequence slot for a
                // close-cursor piggyback.
                let _ = next_ttc_sequence(&mut self.ttc_seq_num);
            }
        }
        // a deferred begin/resume or a folded-in suspend-on-success rides as a
        // sessionless piggyback prepended to this execute (reference
        // messages/base.pyx `_write_sessionless_piggyback`); its sequence number
        // is consumed before the execute's, after the close-cursors piggyback's.
        let sessionless_piggyback = self.take_sessionless_piggyback();
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let mut payload = build_execute_payload_with_bind_rows_and_options_with_seq(
            sql,
            prefetch_rows,
            seq_num,
            statement_is_query(sql),
            bind_rows,
            exec_options,
        )?;
        if let Some(piggyback_bytes) = sessionless_piggyback {
            let mut combined = piggyback_bytes;
            combined.extend_from_slice(&payload);
            payload = combined;
        }
        if let Some(mut piggyback_bytes) = piggyback {
            piggyback_bytes.extend_from_slice(&payload);
            payload = piggyback_bytes;
        }
        trace_query_bytes("EXECUTE query payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response =
            read_data_response_flushing_out_binds(&mut self.read, cx, &self.write, self.sdu)
                .await?;
        trace_query_bytes("EXECUTE query response", &response);
        let known_columns = if exec_options.cursor_id != 0 {
            self.cursor_columns
                .get(&exec_options.cursor_id)
                .cloned()
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        let parsed = parse_query_response_with_binds_options_and_columns(
            &response,
            self.capabilities,
            bind_rows.first().map(Vec::as_slice).unwrap_or(&[]),
            exec_options,
            &known_columns,
        );
        match self.note_parse(parsed) {
            Ok(result) => {
                // a deferred begin/resume or a suspend-on-success reports its
                // outcome through the response's SYNC piggyback
                self.apply_sessionless_state(result.sessionless_txn_state);
                if is_copy {
                    // a copied cursor is never returned to the statement cache;
                    // it is closed when its owning cursor releases it (reference
                    // `_return_to_cache = False` -> `_add_cursor_to_close`).
                    if result.cursor_id != 0 {
                        self.copied_cursors.insert(result.cursor_id);
                    }
                } else if use_cache {
                    self.statement_cache_put(sql, result.cursor_id);
                }
                // Mark the open query cursor as in use so a concurrent execute
                // of the same SQL on another cursor of this connection does not
                // reuse it (and reset its server-side fetch position). Released
                // by `release_cursor` when the owning cursor closes or
                // re-prepares (reference `Statement._in_use`). Only query
                // cursors hold a fetch position vulnerable to ORA-01002.
                if result.cursor_id != 0 && statement_is_query(sql) && !exec_options.parse_only {
                    self.in_use_cursors.insert(result.cursor_id);
                }
                // A cursor passed as an IN REF CURSOR bind may be closed
                // server-side by the called PL/SQL (e.g. `close a_cursor`); its
                // cached cursor_id is then invalid. Drop any statement-cache
                // entry pointing at a bound cursor_id so the next execute on
                // that cursor re-parses with a fresh one instead of reusing the
                // closed one (ORA-01001). Test 1315 / 5815.
                self.invalidate_bound_ref_cursors(bind_rows);
                self.remember_cursor_columns(&result);
                if exec_options.parse_only {
                    return Ok(result);
                }
                self.apply_refetch_metadata(cx, sql, result, prefetch_rows.max(2))
                    .await
            }
            Err(err) => {
                // drop the cached cursor so the next execute re-parses
                // (reference base.pyx:1186-1189 clear_cursor on errors)
                if use_cache {
                    self.statement_cache_invalidate(sql, exec_options.cursor_id);
                }
                Err(err)
            }
        }
    }

    pub async fn execute_query_with_bind_rows_and_timeout(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        bind_rows: &[Vec<BindValue>],
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        self.execute_query_with_bind_rows_call_timeout(
            cx,
            sql,
            prefetch_rows,
            bind_rows,
            timeout_ms,
        )
        .await
    }

    pub async fn execute_query_with_bind_rows_options_and_timeout(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        bind_rows: &[Vec<BindValue>],
        exec_options: ExecuteOptions,
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        let Some(timeout_ms) = timeout_ms.filter(|value| *value > 0) else {
            return self
                .execute_query_with_bind_rows_and_options(
                    cx,
                    sql,
                    prefetch_rows,
                    bind_rows,
                    exec_options,
                )
                .await;
        };
        match time::timeout(
            time::wall_now(),
            Duration::from_millis(u64::from(timeout_ms)),
            self.execute_query_with_bind_rows_and_options(
                cx,
                sql,
                prefetch_rows,
                bind_rows,
                exec_options,
            ),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                let _ = send_marker_shared(cx, &self.write, TNS_MARKER_TYPE_BREAK).await;
                Err(Error::CallTimeout(timeout_ms))
            }
        }
    }

    pub async fn fetch_rows(
        &mut self,
        cx: &Cx,
        cursor_id: u32,
        arraysize: u32,
        previous_row: Option<&[Option<oracledb_protocol::thin::QueryValue>]>,
    ) -> Result<QueryResult> {
        self.fetch_rows_with_columns(cx, cursor_id, arraysize, &[], previous_row)
            .await
    }

    pub async fn fetch_rows_with_columns(
        &mut self,
        cx: &Cx,
        cursor_id: u32,
        arraysize: u32,
        known_columns: &[ColumnMetadata],
        previous_row: Option<&[Option<oracledb_protocol::thin::QueryValue>]>,
    ) -> Result<QueryResult> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_fetch_payload_with_seq(cursor_id, arraysize, seq_num);
        trace_query_bytes("FETCH payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("FETCH response", &response);
        let columns = self
            .cursor_columns
            .get(&cursor_id)
            .cloned()
            .unwrap_or_else(|| known_columns.to_vec());
        let parsed =
            parse_fetch_response_with_context(&response, self.capabilities, &columns, previous_row);
        let result = self.note_parse(parsed)?;
        self.remember_cursor_columns(&result);
        Ok(result)
    }

    pub async fn define_and_fetch_rows_with_columns(
        &mut self,
        cx: &Cx,
        cursor_id: u32,
        arraysize: u32,
        define_columns: &[ColumnMetadata],
        previous_row: Option<&[Option<oracledb_protocol::thin::QueryValue>]>,
    ) -> Result<QueryResult> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload =
            build_define_fetch_payload_with_seq(cursor_id, arraysize, seq_num, define_columns)?;
        trace_query_bytes("DEFINE FETCH payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("DEFINE FETCH response", &response);
        let result = parse_fetch_response_with_context(
            &response,
            self.capabilities,
            define_columns,
            previous_row,
        )
        .map_err(Error::from)?;
        self.cursor_columns
            .insert(cursor_id, define_columns.to_vec());
        self.remember_cursor_columns(&result);
        Ok(result)
    }

    /// Sends a scroll request on an open scrollable cursor and returns the
    /// repositioned buffer (reference `_create_scroll_message` +
    /// `_post_process_scroll`). The caller computes the orientation/position;
    /// `arraysize` is the prefetch/iteration count used for the fetch.
    pub async fn scroll_cursor(
        &mut self,
        cx: &Cx,
        sql: &str,
        cursor_id: u32,
        arraysize: u32,
        fetch_orientation: u32,
        fetch_pos: u32,
    ) -> Result<QueryResult> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let exec_options = ExecuteOptions {
            cursor_id,
            scrollable: true,
            scroll_operation: true,
            fetch_orientation,
            fetch_pos,
            cache_statement: false,
            ..ExecuteOptions::default()
        };
        let piggyback = self.take_close_cursors_piggyback();
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let mut payload = build_execute_payload_with_bind_rows_and_options_with_seq(
            sql,
            arraysize,
            seq_num,
            true,
            &[],
            exec_options,
        )?;
        if let Some(mut piggyback_bytes) = piggyback {
            piggyback_bytes.extend_from_slice(&payload);
            payload = piggyback_bytes;
        }
        trace_query_bytes("SCROLL payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response =
            read_data_response_flushing_out_binds(&mut self.read, cx, &self.write, self.sdu)
                .await?;
        trace_query_bytes("SCROLL response", &response);
        let known_columns = self
            .cursor_columns
            .get(&cursor_id)
            .cloned()
            .unwrap_or_default();
        let parsed = parse_query_response_with_binds_options_and_columns(
            &response,
            self.capabilities,
            &[],
            exec_options,
            &known_columns,
        );
        let result = self.note_parse(parsed)?;
        self.remember_cursor_columns(&result);
        Ok(result)
    }

    /// Read up to `amount` units from the LOB identified by `locator`,
    /// starting at 1-based `offset`. The `locator` comes from a
    /// [`QueryValue::Lob`](protocol::thin::QueryValue::Lob) cell. The returned
    /// bytes are the raw LOB content in the column's character-set form; decode
    /// CLOB/NCLOB text with
    /// [`decode_lob_text`](protocol::thin::decode_lob_text).
    pub async fn read_lob(
        &mut self,
        cx: &Cx,
        locator: &[u8],
        offset: u64,
        amount: u64,
    ) -> Result<LobReadResult> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_lob_read_payload_with_seq(
            locator,
            offset,
            amount,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("LOB READ payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("LOB READ response", &response);
        self.note_parse(parse_lob_read_response(
            &response,
            self.capabilities,
            locator,
        ))
    }

    pub async fn read_lob_with_timeout(
        &mut self,
        cx: &Cx,
        locator: &[u8],
        offset: u64,
        amount: u64,
        timeout_ms: Option<u32>,
    ) -> Result<LobReadResult> {
        self.read_lob_call_timeout(cx, locator, offset, amount, timeout_ms)
            .await
    }

    /// Enqueues a single AQ message (FUNC 121), returning the assigned 16-byte
    /// message id. The TTC round-trip mirrors `read_lob`.
    pub async fn aq_enq_one(
        &mut self,
        cx: &Cx,
        queue: &AqQueueDesc,
        props: &AqMsgProps,
        enq_options: &AqEnqOptions,
    ) -> Result<Option<Vec<u8>>> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_aq_enq_payload(
            queue,
            props,
            enq_options,
            seq_num,
            self.capabilities.ttc_field_version,
            self.supports_oson_long_fnames(),
        )?;
        trace_query_bytes("AQ ENQ payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("AQ ENQ response", &response);
        self.note_parse(parse_aq_enq_response(&response, self.capabilities))
    }

    /// Dequeues a single AQ message (FUNC 122). Returns `None` when the queue is
    /// empty (ORA-25228 cleared server-side).
    pub async fn aq_deq_one(
        &mut self,
        cx: &Cx,
        queue: &AqQueueDesc,
        deq_options: &AqDeqOptions,
    ) -> Result<AqDeqResult> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_aq_deq_payload(
            queue,
            deq_options,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("AQ DEQ payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("AQ DEQ response", &response);
        self.note_parse(parse_aq_deq_response(
            &response,
            self.capabilities,
            &queue.kind,
        ))
    }

    /// Enqueues many AQ messages in one array round-trip (FUNC 145, op=ENQ),
    /// returning the assigned msgid per input message in order.
    pub async fn aq_enq_many(
        &mut self,
        cx: &Cx,
        queue: &AqQueueDesc,
        props_list: &[AqMsgProps],
        enq_options: &AqEnqOptions,
    ) -> Result<Vec<Vec<u8>>> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_aq_array_enq_payload(
            queue,
            props_list,
            enq_options,
            seq_num,
            self.capabilities.ttc_field_version,
            self.supports_oson_long_fnames(),
        )?;
        trace_query_bytes("AQ ARRAY ENQ payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("AQ ARRAY ENQ response", &response);
        let result: AqArrayResult = self.note_parse(parse_aq_array_response(
            &response,
            self.capabilities,
            TNS_AQ_ARRAY_ENQ,
            props_list.len() as u32,
            &queue.kind,
        ))?;
        Ok(result.enq_msgids)
    }

    /// Dequeues up to `max_num_messages` AQ messages in one array round-trip
    /// (FUNC 145, op=DEQ). Returns the dequeued messages (empty when none).
    pub async fn aq_deq_many(
        &mut self,
        cx: &Cx,
        queue: &AqQueueDesc,
        deq_options: &AqDeqOptions,
        max_num_messages: u32,
    ) -> Result<Vec<oracledb_protocol::thin::aq::AqDeqMessage>> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_aq_array_deq_payload(
            queue,
            deq_options,
            max_num_messages,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("AQ ARRAY DEQ payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("AQ ARRAY DEQ response", &response);
        let result: AqArrayResult = self.note_parse(parse_aq_array_response(
            &response,
            self.capabilities,
            TNS_AQ_ARRAY_DEQ,
            max_num_messages,
            &queue.kind,
        ))?;
        Ok(result.deq_messages)
    }

    pub async fn create_temp_lob(
        &mut self,
        cx: &Cx,
        ora_type_num: u8,
        csfrm: u8,
    ) -> Result<LobReadResult> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_lob_create_temp_payload_with_seq(
            ora_type_num,
            csfrm,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("LOB CREATE TEMP payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("LOB CREATE TEMP response", &response);
        self.note_parse(parse_lob_create_temp_response(&response, self.capabilities))
    }

    pub async fn write_lob(
        &mut self,
        cx: &Cx,
        locator: &[u8],
        offset: u64,
        data: &[u8],
    ) -> Result<LobReadResult> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_lob_write_payload_with_seq(
            locator,
            offset,
            data,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("LOB WRITE payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("LOB WRITE response", &response);
        self.note_parse(parse_lob_write_response(
            &response,
            self.capabilities,
            locator,
        ))
    }

    pub async fn write_lob_with_timeout(
        &mut self,
        cx: &Cx,
        locator: &[u8],
        offset: u64,
        data: &[u8],
        timeout_ms: Option<u32>,
    ) -> Result<LobReadResult> {
        self.write_lob_call_timeout(cx, locator, offset, data, timeout_ms)
            .await
    }

    pub async fn trim_lob(
        &mut self,
        cx: &Cx,
        locator: &[u8],
        new_size: u64,
    ) -> Result<LobReadResult> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_lob_trim_payload_with_seq(
            locator,
            new_size,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("LOB TRIM payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("LOB TRIM response", &response);
        self.note_parse(parse_lob_trim_response(
            &response,
            self.capabilities,
            locator,
        ))
    }

    pub async fn trim_lob_with_timeout(
        &mut self,
        cx: &Cx,
        locator: &[u8],
        new_size: u64,
        timeout_ms: Option<u32>,
    ) -> Result<LobReadResult> {
        self.trim_lob_call_timeout(cx, locator, new_size, timeout_ms)
            .await
    }

    pub async fn free_temp_lobs(&mut self, cx: &Cx, locators: &[Vec<u8>]) -> Result<()> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        if locators.is_empty() {
            return Ok(());
        }
        let returned_parameter_len = locators.iter().map(Vec::len).sum();
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_lob_free_temp_payload_with_seq(
            locators,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("LOB FREE TEMP payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("LOB FREE TEMP response", &response);
        self.note_parse(parse_lob_free_temp_response(
            &response,
            self.capabilities,
            returned_parameter_len,
        ))
    }

    pub async fn free_temp_lobs_with_timeout(
        &mut self,
        cx: &Cx,
        locators: &[Vec<u8>],
        timeout_ms: Option<u32>,
    ) -> Result<()> {
        self.free_temp_lobs_call_timeout(cx, locators, timeout_ms)
            .await
    }

    async fn execute_query_call_timeout(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        let Some(timeout_ms) = timeout_ms.filter(|value| *value > 0) else {
            return self.execute_query(cx, sql, prefetch_rows).await;
        };
        match time::timeout(
            time::wall_now(),
            Duration::from_millis(u64::from(timeout_ms)),
            self.execute_query(cx, sql, prefetch_rows),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                let _ = send_marker_shared(cx, &self.write, TNS_MARKER_TYPE_BREAK).await;
                Err(Error::CallTimeout(timeout_ms))
            }
        }
    }

    async fn execute_query_with_binds_call_timeout(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        binds: &[BindValue],
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        let Some(timeout_ms) = timeout_ms.filter(|value| *value > 0) else {
            return self
                .execute_query_with_binds(cx, sql, prefetch_rows, binds)
                .await;
        };
        match time::timeout(
            time::wall_now(),
            Duration::from_millis(u64::from(timeout_ms)),
            self.execute_query_with_binds(cx, sql, prefetch_rows, binds),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                let _ = send_marker_shared(cx, &self.write, TNS_MARKER_TYPE_BREAK).await;
                Err(Error::CallTimeout(timeout_ms))
            }
        }
    }

    async fn execute_query_with_bind_rows_call_timeout(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        bind_rows: &[Vec<BindValue>],
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        let Some(timeout_ms) = timeout_ms.filter(|value| *value > 0) else {
            return self
                .execute_query_with_bind_rows(cx, sql, prefetch_rows, bind_rows)
                .await;
        };
        match time::timeout(
            time::wall_now(),
            Duration::from_millis(u64::from(timeout_ms)),
            self.execute_query_with_bind_rows(cx, sql, prefetch_rows, bind_rows),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                let _ = send_marker_shared(cx, &self.write, TNS_MARKER_TYPE_BREAK).await;
                Err(Error::CallTimeout(timeout_ms))
            }
        }
    }

    async fn read_lob_call_timeout(
        &mut self,
        cx: &Cx,
        locator: &[u8],
        offset: u64,
        amount: u64,
        timeout_ms: Option<u32>,
    ) -> Result<LobReadResult> {
        let Some(timeout_ms) = timeout_ms.filter(|value| *value > 0) else {
            return self.read_lob(cx, locator, offset, amount).await;
        };
        match time::timeout(
            time::wall_now(),
            Duration::from_millis(u64::from(timeout_ms)),
            self.read_lob(cx, locator, offset, amount),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                let _ = send_marker_shared(cx, &self.write, TNS_MARKER_TYPE_BREAK).await;
                Err(Error::CallTimeout(timeout_ms))
            }
        }
    }

    async fn write_lob_call_timeout(
        &mut self,
        cx: &Cx,
        locator: &[u8],
        offset: u64,
        data: &[u8],
        timeout_ms: Option<u32>,
    ) -> Result<LobReadResult> {
        let Some(timeout_ms) = timeout_ms.filter(|value| *value > 0) else {
            return self.write_lob(cx, locator, offset, data).await;
        };
        match time::timeout(
            time::wall_now(),
            Duration::from_millis(u64::from(timeout_ms)),
            self.write_lob(cx, locator, offset, data),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                let _ = send_marker_shared(cx, &self.write, TNS_MARKER_TYPE_BREAK).await;
                Err(Error::CallTimeout(timeout_ms))
            }
        }
    }

    async fn trim_lob_call_timeout(
        &mut self,
        cx: &Cx,
        locator: &[u8],
        new_size: u64,
        timeout_ms: Option<u32>,
    ) -> Result<LobReadResult> {
        let Some(timeout_ms) = timeout_ms.filter(|value| *value > 0) else {
            return self.trim_lob(cx, locator, new_size).await;
        };
        match time::timeout(
            time::wall_now(),
            Duration::from_millis(u64::from(timeout_ms)),
            self.trim_lob(cx, locator, new_size),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                let _ = send_marker_shared(cx, &self.write, TNS_MARKER_TYPE_BREAK).await;
                Err(Error::CallTimeout(timeout_ms))
            }
        }
    }

    async fn free_temp_lobs_call_timeout(
        &mut self,
        cx: &Cx,
        locators: &[Vec<u8>],
        timeout_ms: Option<u32>,
    ) -> Result<()> {
        let Some(timeout_ms) = timeout_ms.filter(|value| *value > 0) else {
            return self.free_temp_lobs(cx, locators).await;
        };
        match time::timeout(
            time::wall_now(),
            Duration::from_millis(u64::from(timeout_ms)),
            self.free_temp_lobs(cx, locators),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                let _ = send_marker_shared(cx, &self.write, TNS_MARKER_TYPE_BREAK).await;
                Err(Error::CallTimeout(timeout_ms))
            }
        }
    }

    /// Sends a direct path prepare (TTC function 128) for the given table and
    /// returns the server column metadata plus the direct path cursor id.
    pub async fn direct_path_prepare(
        &mut self,
        cx: &Cx,
        schema_name: &str,
        table_name: &str,
        column_names: &[String],
    ) -> Result<oracledb_protocol::dpl::DirectPathPrepareResult> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = oracledb_protocol::dpl::build_direct_path_prepare_payload(
            schema_name,
            table_name,
            column_names,
            seq_num,
        )?;
        trace_query_bytes("DIRECT PATH PREPARE payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("DIRECT PATH PREPARE response", &response);
        oracledb_protocol::dpl::parse_direct_path_prepare_response(&response, self.capabilities)
            .map_err(Error::from)
    }

    /// Sends one direct path load stream message (TTC function 129).
    pub async fn direct_path_load_stream(
        &mut self,
        cx: &Cx,
        cursor_id: u16,
        stream: &oracledb_protocol::dpl::DirectPathStream,
    ) -> Result<()> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = oracledb_protocol::dpl::build_direct_path_load_stream_payload(
            cursor_id, stream, seq_num,
        )?;
        trace_query_bytes("DIRECT PATH LOAD STREAM payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("DIRECT PATH LOAD STREAM response", &response);
        oracledb_protocol::dpl::parse_direct_path_simple_response(&response, self.capabilities)
            .map_err(Error::from)
    }

    /// Sends a direct path op message (TTC function 130).
    /// [`oracledb_protocol::dpl::TNS_DP_OP_FINISH`] commits the load
    /// server-side; [`oracledb_protocol::dpl::TNS_DP_OP_ABORT`] discards it.
    pub async fn direct_path_op(&mut self, cx: &Cx, cursor_id: u16, op_code: u32) -> Result<()> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload =
            oracledb_protocol::dpl::build_direct_path_op_payload(cursor_id, op_code, seq_num);
        trace_query_bytes("DIRECT PATH OP payload", &payload);
        send_data_packet_shared(cx, &self.write, &payload, self.sdu).await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        trace_query_bytes("DIRECT PATH OP response", &response);
        oracledb_protocol::dpl::parse_direct_path_simple_response(&response, self.capabilities)
            .map_err(Error::from)
    }

    /// Loads `rows` into `schema_name.table_name` via the direct path load
    /// interface, mirroring the reference driver loop
    /// (impl/thin/connection.pyx `direct_path_load`): prepare, stream batches
    /// of `batch_size` rows, then FINISH (which commits) or ABORT on error.
    /// The op message is always sent, even when streaming fails, so the
    /// session is never left wedged.
    pub async fn direct_path_load(
        &mut self,
        cx: &Cx,
        schema_name: &str,
        table_name: &str,
        column_names: &[String],
        rows: &[Vec<oracledb_protocol::dpl::DirectPathColumnValue>],
        batch_size: u32,
    ) -> Result<()> {
        let prepare = self
            .direct_path_prepare(cx, schema_name, table_name, column_names)
            .await?;
        let load_result = self
            .direct_path_load_batches(cx, &prepare, rows, batch_size)
            .await;
        let op_code = if load_result.is_ok() {
            oracledb_protocol::dpl::TNS_DP_OP_FINISH
        } else {
            oracledb_protocol::dpl::TNS_DP_OP_ABORT
        };
        let op_result = self.direct_path_op(cx, prepare.cursor_id, op_code).await;
        load_result?;
        op_result
    }

    /// Loads pre-converted rows against an already-prepared direct path cursor,
    /// then sends the FINISH (or ABORT on failure) op. Lets a caller convert its
    /// data using the prepared `column_metadata` without a second PREPARE round
    /// trip (so the reference round-trip count holds).
    pub async fn direct_path_load_prepared(
        &mut self,
        cx: &Cx,
        prepare: &oracledb_protocol::dpl::DirectPathPrepareResult,
        rows: &[Vec<oracledb_protocol::dpl::DirectPathColumnValue>],
        batch_size: u32,
    ) -> Result<()> {
        let load_result = self
            .direct_path_load_batches(cx, prepare, rows, batch_size)
            .await;
        let op_code = if load_result.is_ok() {
            oracledb_protocol::dpl::TNS_DP_OP_FINISH
        } else {
            oracledb_protocol::dpl::TNS_DP_OP_ABORT
        };
        let op_result = self.direct_path_op(cx, prepare.cursor_id, op_code).await;
        load_result?;
        op_result
    }

    async fn direct_path_load_batches(
        &mut self,
        cx: &Cx,
        prepare: &oracledb_protocol::dpl::DirectPathPrepareResult,
        rows: &[Vec<oracledb_protocol::dpl::DirectPathColumnValue>],
        batch_size: u32,
    ) -> Result<()> {
        // verify all row widths before sending anything (reference
        // _verify_metadata raises DPY-4009 before the first stream message)
        for row in rows {
            if row.len() != prepare.column_metadata.len() {
                return Err(oracledb_protocol::ProtocolError::TtcDecode(
                    "direct path row width does not match column metadata",
                )
                .into());
            }
        }
        let mut state =
            oracledb_protocol::dpl::BatchLoadState::for_rows(rows.len() as u64, batch_size)?;
        // 1-based running row counter across batches for error messages
        let mut row_num: u64 = 1;
        while !state.is_done() {
            let start = usize::try_from(state.offset()).map_err(|_| {
                oracledb_protocol::ProtocolError::TtcDecode("direct path offset overflow")
            })?;
            let end = start + state.num_rows() as usize;
            let stream = oracledb_protocol::dpl::encode_direct_path_rows(
                &prepare.column_metadata,
                &rows[start..end],
                row_num,
            )?;
            row_num += (end - start) as u64;
            self.direct_path_load_stream(cx, prepare.cursor_id, &stream)
                .await?;
            state.next_batch();
        }
        Ok(())
    }

    async fn drain_cancel_response(&mut self, cx: &Cx) -> Result<()> {
        match time::timeout(
            time::wall_now(),
            Duration::from_secs(5),
            read_data_response(&mut self.read, cx, &self.write),
        )
        .await
        {
            Ok(response) => {
                let response = response?;
                trace_query_bytes("CANCEL drain response", &response);
                Ok(())
            }
            Err(_) => Ok(()),
        }
    }

    fn remember_cursor_columns(&mut self, result: &QueryResult) {
        if result.cursor_id != 0 && !result.columns.is_empty() {
            self.cursor_columns
                .insert(result.cursor_id, result.columns.clone());
        }
    }

    /// Retains the fetch metadata of the most recent execution of `sql`,
    /// evicting the oldest entry beyond the cap (reference retains this on
    /// the cached Statement object, impl/thin/statement.pyx:300-310).
    fn remember_fetch_metadata(&mut self, sql: &str, columns: &[ColumnMetadata]) {
        const FETCH_METADATA_RETENTION_CAP: usize = 100;
        if !self.fetch_metadata_by_sql.contains_key(sql) {
            if self.fetch_metadata_order.len() >= FETCH_METADATA_RETENTION_CAP {
                if let Some(oldest) = self.fetch_metadata_order.pop_front() {
                    self.fetch_metadata_by_sql.remove(&oldest);
                }
            }
            self.fetch_metadata_order.push_back(sql.to_string());
        }
        self.fetch_metadata_by_sql
            .insert(sql.to_string(), columns.to_vec());
    }

    /// Drops the retained fetch metadata for `sql` (the reference clears the
    /// cached statement's cursor before a type-change retry,
    /// impl/thin/messages/base.pyx:1206-1213). Returns whether an entry
    /// existed.
    fn forget_fetch_metadata(&mut self, sql: &str) -> bool {
        if self.fetch_metadata_by_sql.remove(sql).is_some() {
            self.fetch_metadata_order.retain(|entry| entry != sql);
            return true;
        }
        false
    }

    /// Applies the re-execute type-change rule: when the retained fetch
    /// metadata for this SQL says a column previously fetched as char/raw is
    /// now described as CLOB/BLOB, re-define the cursor so the data streams
    /// as LONG/LONG RAW (reference _adjust_metadata + _requires_define,
    /// impl/thin/messages/base.pyx:820-845, 1148-1158).
    async fn apply_refetch_metadata(
        &mut self,
        cx: &Cx,
        sql: &str,
        mut result: QueryResult,
        arraysize: u32,
    ) -> Result<QueryResult> {
        if result.columns.is_empty() {
            return Ok(result);
        }
        if let Some(previous_columns) = self.fetch_metadata_by_sql.get(sql) {
            let mut adjusted = result.columns.clone();
            let mut any_adjusted = false;
            for (index, column) in adjusted.iter_mut().enumerate() {
                if let Some(previous) = previous_columns.get(index) {
                    any_adjusted |= adjust_refetch_metadata(previous, column);
                }
            }
            if any_adjusted && result.cursor_id != 0 {
                let cursor_id = result.cursor_id;
                let mut redefined = self
                    .define_and_fetch_rows_with_columns(
                        cx,
                        cursor_id,
                        arraysize.max(1),
                        &adjusted,
                        None,
                    )
                    .await?;
                if redefined.columns.is_empty() {
                    redefined.columns = adjusted;
                }
                if redefined.cursor_id == 0 {
                    redefined.cursor_id = cursor_id;
                }
                result = redefined;
            }
        }
        self.remember_fetch_metadata(sql, &result.columns);
        Ok(result)
    }

    /// Looks up an open server cursor for the SQL text, refreshing its LRU
    /// position (reference `_statement_cache.get_statement`). A cached cursor
    /// that is currently `_in_use` by another live cursor is NOT handed out:
    /// the reference makes a `stmt.copy()` (fresh cursor id) in that case, so
    /// concurrent cursors over identical SQL each drive their own server
    /// cursor and cannot reset each other's fetch position (ORA-01002). We
    /// model the copy by returning `None`, which forces a fresh PARSE.
    fn statement_cache_get(&mut self, sql: &str) -> Option<u32> {
        let index = self
            .statement_cache
            .iter()
            .position(|(cached_sql, _)| cached_sql == sql)?;
        let cursor_id = self.statement_cache[index].1;
        if cursor_id != 0 && self.in_use_cursors.contains(&cursor_id) {
            return None;
        }
        let entry = self.statement_cache.remove(index);
        self.statement_cache.push(entry);
        Some(cursor_id)
    }

    /// Removes and returns the open cursor for the SQL text; used when the
    /// caller requested `cache_statement=False` but the statement is still
    /// present from an earlier cached execution (reference `_get_statement`
    /// pops from the cache unconditionally).
    fn statement_cache_take(&mut self, sql: &str) -> Option<u32> {
        let index = self
            .statement_cache
            .iter()
            .position(|(cached_sql, _)| cached_sql == sql)?;
        Some(self.statement_cache.remove(index).1)
    }

    /// Stores/updates the open cursor for the SQL text, evicting the least
    /// recently used entry into the close-cursors piggyback queue (reference
    /// `_statement_cache.return_statement`).
    fn statement_cache_put(&mut self, sql: &str, cursor_id: u32) {
        if cursor_id == 0 {
            return;
        }
        if let Some(index) = self
            .statement_cache
            .iter()
            .position(|(cached_sql, _)| cached_sql == sql)
        {
            let (_, cached_id) = self.statement_cache.remove(index);
            if cached_id != 0 && cached_id != cursor_id {
                self.cursors_to_close.push(cached_id);
            }
        }
        self.statement_cache.push((sql.to_string(), cursor_id));
        while self.statement_cache.len() > STATEMENT_CACHE_SIZE {
            let (_, evicted_id) = self.statement_cache.remove(0);
            if evicted_id != 0 {
                self.cursors_to_close.push(evicted_id);
            }
        }
    }

    /// Drops any statement-cache entry whose open cursor was passed as an IN
    /// REF CURSOR bind in `bind_rows`. The called PL/SQL may have closed the
    /// cursor server-side, leaving the cached cursor_id invalid; clearing the
    /// entry forces a re-parse on the next execute of that SQL rather than
    /// reusing the closed cursor (ORA-01001). Test 1315 / 5815.
    fn invalidate_bound_ref_cursors(&mut self, bind_rows: &[Vec<BindValue>]) {
        for row in bind_rows {
            for value in row {
                if let BindValue::Cursor { cursor_id } = value {
                    if *cursor_id == 0 {
                        continue;
                    }
                    self.statement_cache
                        .retain(|(_, cached_id)| cached_id != cursor_id);
                    self.cursor_columns.remove(cursor_id);
                }
            }
        }
    }

    /// Releases a server cursor id previously marked in use by an executing
    /// query cursor (reference `_return_statement` clearing `Statement._in_use`).
    /// Called when the owning cursor closes or re-prepares; once released the
    /// cached cursor may be reused by the next execute of the same SQL. The
    /// cursor id stays in the statement cache (the open server cursor is kept
    /// for reuse, mirroring `_return_to_cache`).
    pub fn release_cursor(&mut self, cursor_id: u32) {
        if cursor_id == 0 {
            return;
        }
        self.in_use_cursors.remove(&cursor_id);
        // A copied cursor (parsed because the cached statement was busy) is not
        // kept open: queue it for the close-cursors piggyback now that its
        // owning cursor is done with it (reference `_add_cursor_to_close`).
        if self.copied_cursors.remove(&cursor_id) {
            self.cursors_to_close.push(cursor_id);
            self.cursor_columns.remove(&cursor_id);
        }
    }

    /// Queue an open server cursor to be closed on the next round trip
    /// (reference `_add_cursor_to_close`). Unlike [`Self::release_cursor`],
    /// which returns a cached cursor to the statement cache for reuse, this
    /// drops the cursor entirely: its id is sent in the close-cursors piggyback
    /// that rides the next execute, and its retained describe metadata is
    /// forgotten. Use this for a non-cached cursor (for example one opened by
    /// [`Self::execute_query_collect`]) once its result is fully consumed, to
    /// keep a long-lived connection from accumulating open cursors. A cursor id
    /// of `0` is ignored.
    pub fn close_cursor(&mut self, cursor_id: u32) {
        if cursor_id == 0 {
            return;
        }
        self.in_use_cursors.remove(&cursor_id);
        self.copied_cursors.remove(&cursor_id);
        self.cursor_columns.remove(&cursor_id);
        if !self.cursors_to_close.contains(&cursor_id) {
            self.cursors_to_close.push(cursor_id);
        }
    }

    /// Returns true when the SQL text has a cached open cursor that is
    /// currently in use by another live cursor (reference `Statement._in_use`
    /// checked in `get_statement`).
    fn statement_is_in_use(&self, sql: &str) -> bool {
        self.statement_cache
            .iter()
            .find(|(cached_sql, _)| cached_sql == sql)
            .is_some_and(|(_, cursor_id)| {
                *cursor_id != 0 && self.in_use_cursors.contains(cursor_id)
            })
    }

    /// Drops the cached cursor for the SQL text after a server error so the
    /// next execute re-parses (reference `_statement_cache.clear_cursor`).
    fn statement_cache_invalidate(&mut self, sql: &str, cursor_id: u32) {
        if let Some(index) = self
            .statement_cache
            .iter()
            .position(|(cached_sql, _)| cached_sql == sql)
        {
            self.statement_cache.remove(index);
        }
        if cursor_id != 0 {
            self.cursors_to_close.push(cursor_id);
            self.cursor_columns.remove(&cursor_id);
            self.in_use_cursors.remove(&cursor_id);
            self.copied_cursors.remove(&cursor_id);
        }
    }

    /// Builds the close-cursors piggyback bytes for any queued cursor ids;
    /// the piggyback consumes its own TTC sequence number.
    fn take_close_cursors_piggyback(&mut self) -> Option<Vec<u8>> {
        if self.cursors_to_close.is_empty() {
            return None;
        }
        let cursor_ids = std::mem::take(&mut self.cursors_to_close);
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        Some(oracledb_protocol::thin::build_close_cursors_piggyback(
            &cursor_ids,
            seq_num,
        ))
    }

    /// Log off and close the connection, consuming it. Any uncommitted
    /// transaction is rolled back by the server.
    pub async fn close(mut self, cx: &Cx) -> Result<()> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        match time::timeout(time::wall_now(), Duration::from_secs(5), self.rollback(cx)).await {
            Ok(result) => result?,
            Err(_) => {
                let eof = encode_packet(
                    TNS_PACKET_TYPE_DATA,
                    0,
                    Some(oracledb_protocol::thin::TNS_DATA_FLAGS_EOF),
                    &[],
                    PacketLengthWidth::Large32,
                )?;
                let _ = write_all_shared(cx, &self.write, &eof).await;
                let _ = shutdown_write_shared(cx, &self.write).await;
                return Ok(());
            }
        }
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        send_data_packet_shared(
            cx,
            &self.write,
            &build_function_payload_with_seq(TNS_FUNC_LOGOFF, seq_num),
            self.sdu,
        )
        .await?;
        if let Ok(response) = time::timeout(
            time::wall_now(),
            Duration::from_secs(5),
            read_data_response(&mut self.read, cx, &self.write),
        )
        .await
        {
            let _ = response?;
        }
        let eof = encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(oracledb_protocol::thin::TNS_DATA_FLAGS_EOF),
            &[],
            PacketLengthWidth::Large32,
        )?;
        write_all_shared(cx, &self.write, &eof).await?;
        let _ = shutdown_write_shared(cx, &self.write).await;
        Ok(())
    }

    /// Runs a batch of operations as a true wire pipeline (single round trip):
    /// every request is written before anything is read, then the N+1
    /// boundary-delimited responses (one per operation plus the end-pipeline
    /// response) are returned as raw TTC payloads in token order. Mirrors the
    /// reference flow (impl/thin/connection.pyx `run_pipeline_with_pipelining`
    /// and protocol.pyx `end_pipeline`):
    ///
    /// * the first message is prefixed with the begin-pipeline piggyback and
    ///   its first packet carries TNS_DATA_FLAGS_BEGIN_PIPELINE,
    /// * each operation message carries token 1..N and its final packet
    ///   carries TNS_DATA_FLAGS_END_OF_REQUEST,
    /// * the end-pipeline message (function 200) closes the batch,
    /// * marker packets received while reading pipeline responses are dropped
    ///   without sending a reset (packet.pyx:346-370),
    /// * responses are read for every operation even after a server error --
    ///   the server answers each message in both pipeline modes, so callers
    ///   parse per-operation payloads and decide error semantics.
    pub async fn run_pipeline(
        &mut self,
        cx: &Cx,
        requests: &[PipelineRequest],
        continue_on_error: bool,
    ) -> Result<Vec<Vec<u8>>> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let pipeline_mode = if continue_on_error {
            TNS_PIPELINE_MODE_CONTINUE_ON_ERROR
        } else {
            TNS_PIPELINE_MODE_ABORT_ON_ERROR
        };
        for (index, request) in requests.iter().enumerate() {
            let token_num = index as u64 + 1;
            let mut payload = Vec::new();
            let mut first_packet_flags = 0u16;
            if index == 0 {
                let piggyback_seq = next_ttc_sequence(&mut self.ttc_seq_num);
                payload.extend_from_slice(&build_begin_pipeline_piggyback(
                    piggyback_seq,
                    token_num,
                    pipeline_mode,
                ));
                first_packet_flags |= TNS_DATA_FLAGS_BEGIN_PIPELINE;
            }
            let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
            match request {
                PipelineRequest::Execute {
                    sql,
                    bind_rows,
                    prefetch_rows,
                } => payload.extend_from_slice(
                    &build_execute_payload_with_bind_rows_with_seq_and_token(
                        sql,
                        *prefetch_rows,
                        seq_num,
                        statement_is_query(sql),
                        bind_rows,
                        token_num,
                    )?,
                ),
                PipelineRequest::Commit => {
                    payload.extend_from_slice(&build_function_payload_with_seq_and_token(
                        TNS_FUNC_COMMIT,
                        seq_num,
                        token_num,
                    ));
                }
            }
            trace_query_bytes("PIPELINE op payload", &payload);
            send_data_packet_shared_with_flags(
                cx,
                &self.write,
                &payload,
                self.sdu,
                first_packet_flags,
                TNS_DATA_FLAGS_END_OF_REQUEST,
            )
            .await?;
        }
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let end_payload = build_end_pipeline_payload_with_seq(seq_num);
        trace_query_bytes("PIPELINE end payload", &end_payload);
        send_data_packet_shared(cx, &self.write, &end_payload, self.sdu).await?;
        let mut responses = Vec::with_capacity(requests.len() + 1);
        for _ in 0..=requests.len() {
            let response =
                read_data_response_boundary(&mut self.read, cx, &self.write, true).await?;
            trace_query_bytes("PIPELINE response", &response.payload);
            responses.push(response.payload);
        }
        Ok(responses)
    }

    async fn send_function(&mut self, cx: &Cx, function_code: u8) -> Result<()> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        send_data_packet_shared(
            cx,
            &self.write,
            &build_function_payload_with_seq(function_code, seq_num),
            self.sdu,
        )
        .await?;
        let response = read_data_response(&mut self.read, cx, &self.write).await?;
        // Surface server errors (e.g. ORA-01012 after a killed session) that
        // arrive on plain function round trips; pool ping health checks and
        // commit/rollback depend on these not being silently swallowed.
        self.note_parse(parse_plain_function_response(&response, self.capabilities))?;
        Ok(())
    }
}

impl CancelHandle {
    pub fn cancel(&mut self) -> Result<()> {
        let runtime = build_io_runtime()?;
        let write = Arc::clone(&self.write);
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            send_marker_shared(&cx, &write, TNS_MARKER_TYPE_BREAK).await
        })
    }
}

/// Synchronous facade over [`Connection`].
///
/// Each associated function spins up a private single-threaded Asupersync
/// runtime, drives the corresponding async [`Connection`] method to
/// completion, and blocks the calling thread until it returns. The functions
/// take a `&mut Connection` (returned by [`BlockingConnection::connect`]) so a
/// connection can be reused across calls. This is the simplest way to use the
/// driver from ordinary synchronous Rust.
///
/// ```no_run
/// use oracledb::{BlockingConnection, ConnectOptions};
/// use oracledb::protocol::ClientIdentity;
/// use oracledb::protocol::thin::QueryValue;
///
/// # fn main() -> Result<(), oracledb::Error> {
/// let identity = ClientIdentity::new("svc", "host", "user", "term", "rust-oracledb")?;
/// let mut conn = BlockingConnection::connect(
///     ConnectOptions::new("dbhost:1521/FREEPDB1", "app", "pw", identity),
/// )?;
/// let result = BlockingConnection::execute_query(&mut conn, "select 1 from dual", 1)?;
/// assert_eq!(result.cell(0, 0).and_then(QueryValue::as_i64), Some(1));
/// BlockingConnection::close(conn)?;
/// # Ok(())
/// # }
/// ```
pub struct BlockingConnection;

impl BlockingConnection {
    /// Open a connection synchronously. See [`Connection::connect`].
    pub fn connect(options: ConnectOptions) -> Result<Connection> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            Connection::connect(&cx, options).await
        })
    }

    pub fn ping(connection: &mut Connection) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.ping(&cx).await
        })
    }

    pub fn ping_with_timeout(connection: &mut Connection, timeout_ms: u32) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.ping_with_timeout(&cx, timeout_ms).await
        })
    }

    pub fn change_password(
        connection: &mut Connection,
        old_password: &str,
        new_password: &str,
    ) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .change_password(&cx, old_password, new_password)
                .await
        })
    }

    pub fn commit(connection: &mut Connection) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.commit(&cx).await
        })
    }

    pub fn rollback(connection: &mut Connection) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.rollback(&cx).await
        })
    }

    pub fn begin_sessionless_transaction(
        connection: &mut Connection,
        transaction_id: &[u8],
        timeout: u32,
        defer_round_trip: bool,
    ) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .begin_sessionless_transaction(&cx, transaction_id, timeout, defer_round_trip)
                .await
        })
    }

    pub fn resume_sessionless_transaction(
        connection: &mut Connection,
        transaction_id: &[u8],
        timeout: u32,
        defer_round_trip: bool,
    ) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .resume_sessionless_transaction(&cx, transaction_id, timeout, defer_round_trip)
                .await
        })
    }

    pub fn suspend_sessionless_transaction(connection: &mut Connection) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.suspend_sessionless_transaction(&cx).await
        })
    }

    pub fn execute_query(
        connection: &mut Connection,
        sql: &str,
        prefetch_rows: u32,
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.execute_query(&cx, sql, prefetch_rows).await
        })
    }

    /// Blocking wrapper for [`Connection::execute_query_collect`]: execute and
    /// return the first batch with `CLOB` / `BLOB` / `VECTOR` / native `JSON`
    /// cells fully materialized via an automatic define-fetch round trip.
    pub fn execute_query_collect(
        connection: &mut Connection,
        sql: &str,
        prefetch_rows: u32,
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .execute_query_collect(&cx, sql, prefetch_rows)
                .await
        })
    }

    pub fn execute_query_with_timeout(
        connection: &mut Connection,
        sql: &str,
        prefetch_rows: u32,
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .execute_query_call_timeout(&cx, sql, prefetch_rows, timeout_ms)
                .await
        })
    }

    pub fn execute_query_with_binds(
        connection: &mut Connection,
        sql: &str,
        prefetch_rows: u32,
        binds: &[BindValue],
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .execute_query_with_binds(&cx, sql, prefetch_rows, binds)
                .await
        })
    }

    pub fn execute_query_with_binds_and_timeout(
        connection: &mut Connection,
        sql: &str,
        prefetch_rows: u32,
        binds: &[BindValue],
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .execute_query_with_binds_call_timeout(&cx, sql, prefetch_rows, binds, timeout_ms)
                .await
        })
    }

    pub fn execute_query_with_bind_rows(
        connection: &mut Connection,
        sql: &str,
        prefetch_rows: u32,
        bind_rows: &[Vec<BindValue>],
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .execute_query_with_bind_rows(&cx, sql, prefetch_rows, bind_rows)
                .await
        })
    }

    pub fn execute_query_with_bind_rows_and_timeout(
        connection: &mut Connection,
        sql: &str,
        prefetch_rows: u32,
        bind_rows: &[Vec<BindValue>],
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .execute_query_with_bind_rows_call_timeout(
                    &cx,
                    sql,
                    prefetch_rows,
                    bind_rows,
                    timeout_ms,
                )
                .await
        })
    }

    pub fn execute_query_with_bind_rows_options_and_timeout(
        connection: &mut Connection,
        sql: &str,
        prefetch_rows: u32,
        bind_rows: &[Vec<BindValue>],
        exec_options: ExecuteOptions,
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .execute_query_with_bind_rows_options_and_timeout(
                    &cx,
                    sql,
                    prefetch_rows,
                    bind_rows,
                    exec_options,
                    timeout_ms,
                )
                .await
        })
    }

    pub fn fetch_rows(
        connection: &mut Connection,
        cursor_id: u32,
        arraysize: u32,
        previous_row: Option<&[Option<oracledb_protocol::thin::QueryValue>]>,
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .fetch_rows(&cx, cursor_id, arraysize, previous_row)
                .await
        })
    }

    pub fn fetch_rows_with_columns(
        connection: &mut Connection,
        cursor_id: u32,
        arraysize: u32,
        known_columns: &[ColumnMetadata],
        previous_row: Option<&[Option<oracledb_protocol::thin::QueryValue>]>,
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .fetch_rows_with_columns(&cx, cursor_id, arraysize, known_columns, previous_row)
                .await
        })
    }

    pub fn define_and_fetch_rows_with_columns(
        connection: &mut Connection,
        cursor_id: u32,
        arraysize: u32,
        define_columns: &[ColumnMetadata],
        previous_row: Option<&[Option<oracledb_protocol::thin::QueryValue>]>,
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .define_and_fetch_rows_with_columns(
                    &cx,
                    cursor_id,
                    arraysize,
                    define_columns,
                    previous_row,
                )
                .await
        })
    }

    pub fn scroll_cursor(
        connection: &mut Connection,
        sql: &str,
        cursor_id: u32,
        arraysize: u32,
        fetch_orientation: u32,
        fetch_pos: u32,
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .scroll_cursor(&cx, sql, cursor_id, arraysize, fetch_orientation, fetch_pos)
                .await
        })
    }

    pub fn read_lob(
        connection: &mut Connection,
        locator: &[u8],
        offset: u64,
        amount: u64,
    ) -> Result<LobReadResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.read_lob(&cx, locator, offset, amount).await
        })
    }

    pub fn read_lob_with_timeout(
        connection: &mut Connection,
        locator: &[u8],
        offset: u64,
        amount: u64,
        timeout_ms: Option<u32>,
    ) -> Result<LobReadResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .read_lob_call_timeout(&cx, locator, offset, amount, timeout_ms)
                .await
        })
    }

    pub fn aq_enq_one(
        connection: &mut Connection,
        queue: &AqQueueDesc,
        props: &AqMsgProps,
        enq_options: &AqEnqOptions,
    ) -> Result<Option<Vec<u8>>> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.aq_enq_one(&cx, queue, props, enq_options).await
        })
    }

    pub fn aq_deq_one(
        connection: &mut Connection,
        queue: &AqQueueDesc,
        deq_options: &AqDeqOptions,
    ) -> Result<AqDeqResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.aq_deq_one(&cx, queue, deq_options).await
        })
    }

    pub fn aq_enq_many(
        connection: &mut Connection,
        queue: &AqQueueDesc,
        props_list: &[AqMsgProps],
        enq_options: &AqEnqOptions,
    ) -> Result<Vec<Vec<u8>>> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .aq_enq_many(&cx, queue, props_list, enq_options)
                .await
        })
    }

    pub fn aq_deq_many(
        connection: &mut Connection,
        queue: &AqQueueDesc,
        deq_options: &AqDeqOptions,
        max_num_messages: u32,
    ) -> Result<Vec<oracledb_protocol::thin::aq::AqDeqMessage>> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .aq_deq_many(&cx, queue, deq_options, max_num_messages)
                .await
        })
    }

    pub fn create_temp_lob(
        connection: &mut Connection,
        ora_type_num: u8,
        csfrm: u8,
    ) -> Result<LobReadResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.create_temp_lob(&cx, ora_type_num, csfrm).await
        })
    }

    pub fn write_lob(
        connection: &mut Connection,
        locator: &[u8],
        offset: u64,
        data: &[u8],
    ) -> Result<LobReadResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.write_lob(&cx, locator, offset, data).await
        })
    }

    pub fn write_lob_with_timeout(
        connection: &mut Connection,
        locator: &[u8],
        offset: u64,
        data: &[u8],
        timeout_ms: Option<u32>,
    ) -> Result<LobReadResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .write_lob_call_timeout(&cx, locator, offset, data, timeout_ms)
                .await
        })
    }

    pub fn trim_lob_with_timeout(
        connection: &mut Connection,
        locator: &[u8],
        new_size: u64,
        timeout_ms: Option<u32>,
    ) -> Result<LobReadResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .trim_lob_call_timeout(&cx, locator, new_size, timeout_ms)
                .await
        })
    }

    pub fn free_temp_lobs_with_timeout(
        connection: &mut Connection,
        locators: &[Vec<u8>],
        timeout_ms: Option<u32>,
    ) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .free_temp_lobs_call_timeout(&cx, locators, timeout_ms)
                .await
        })
    }

    pub fn direct_path_load(
        connection: &mut Connection,
        schema_name: &str,
        table_name: &str,
        column_names: &[String],
        rows: &[Vec<oracledb_protocol::dpl::DirectPathColumnValue>],
        batch_size: u32,
    ) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .direct_path_load(&cx, schema_name, table_name, column_names, rows, batch_size)
                .await
        })
    }

    pub fn run_pipeline(
        connection: &mut Connection,
        requests: &[PipelineRequest],
        continue_on_error: bool,
    ) -> Result<Vec<Vec<u8>>> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .run_pipeline(&cx, requests, continue_on_error)
                .await
        })
    }

    pub fn direct_path_prepare(
        connection: &mut Connection,
        schema_name: &str,
        table_name: &str,
        column_names: &[String],
    ) -> Result<oracledb_protocol::dpl::DirectPathPrepareResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .direct_path_prepare(&cx, schema_name, table_name, column_names)
                .await
        })
    }

    pub fn direct_path_load_prepared(
        connection: &mut Connection,
        prepare: &oracledb_protocol::dpl::DirectPathPrepareResult,
        rows: &[Vec<oracledb_protocol::dpl::DirectPathColumnValue>],
        batch_size: u32,
    ) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .direct_path_load_prepared(&cx, prepare, rows, batch_size)
                .await
        })
    }

    pub fn drain_cancel_response(connection: &mut Connection) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.drain_cancel_response(&cx).await
        })
    }

    pub fn close(connection: Connection) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.close(&cx).await
        })
    }
}

/// Construct a fresh single-threaded Asupersync runtime with a native reactor.
///
/// This is the heavy path: it creates an epoll reactor and spawns a worker OS
/// thread. It is only called once per thread by [`io_runtime`], which caches
/// the result; callers should use [`build_io_runtime`] (the cached accessor)
/// rather than this directly.
fn new_io_runtime() -> Result<Runtime> {
    let reactor = reactor::create_reactor()?;
    RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .map_err(|err| Error::Runtime(err.to_string()))
}

thread_local! {
    /// One blocking-facade runtime per calling thread, built lazily on first
    /// use and reused for every subsequent `BlockingConnection` /
    /// `CancelHandle` call on that thread.
    ///
    /// The previous behaviour built a brand-new runtime — a fresh epoll reactor
    /// plus a worker OS thread that is spawned and immediately joined — on every
    /// single call. For the synchronous facade, which the PyO3 shim drives for
    /// every suite operation, that fixed per-call cost dominated cheap
    /// operations like `select 1 from dual`. Caching the runtime per thread
    /// removes that overhead from every call after the first.
    ///
    /// Correctness is preserved: each `Runtime::block_on` still installs a fresh
    /// request-scoped `Cx` (with `Budget::INFINITE`) and runtime/Cx guards for
    /// the duration of the polled future, so cancellation and context semantics
    /// are unchanged. The connection's socket re-registers (`rearm`) with the
    /// persistent reactor on each call exactly as Asupersync's owned TCP halves
    /// are designed to; this is strictly less work than dropping and rebuilding
    /// a reactor every call. The runtime is current-thread, so it never crosses
    /// threads, and it lives for the thread's lifetime.
    static IO_RUNTIME: std::cell::RefCell<Option<Runtime>> =
        const { std::cell::RefCell::new(None) };
}

/// Return this thread's cached blocking-facade runtime, building it on first
/// use. The returned `Runtime` is a cheap `Arc`-backed clone of the cached
/// instance; cloning does not spawn threads or create reactors. Behaviourally
/// equivalent to constructing a runtime per call, minus the per-call build cost.
fn build_io_runtime() -> Result<Runtime> {
    IO_RUNTIME.with(|slot| {
        if let Some(runtime) = slot.borrow().as_ref() {
            return Ok(runtime.clone());
        }
        let runtime = new_io_runtime()?;
        *slot.borrow_mut() = Some(runtime.clone());
        Ok(runtime)
    })
}

/// Runs a connection future to completion on a fresh blocking runtime,
/// passing it the ambient [`Cx`] (shared shape of the `BlockingConnection`
/// wrappers). Currently only used by the arrow-feature wrappers.
#[cfg(feature = "arrow")]
pub(crate) fn block_on_connection<F, Fut, T>(operation: F) -> Result<T>
where
    F: FnOnce(Cx) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let runtime = build_io_runtime()?;
    runtime.block_on(async {
        let cx = Cx::current()
            .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
        operation(cx).await
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct IncomingPacket {
    packet_type: u8,
    payload: Vec<u8>,
}

async fn lock_write<'a>(
    cx: &Cx,
    write: &'a SharedWriteHalf,
) -> Result<asupersync::sync::MutexGuard<'a, OwnedWriteHalf>> {
    write
        .lock(cx)
        .await
        .map_err(|err| Error::Runtime(err.to_string()))
}

async fn write_all_shared(cx: &Cx, write: &SharedWriteHalf, packet: &[u8]) -> Result<()> {
    let mut guard = lock_write(cx, write).await?;
    guard.write_all(packet).await?;
    guard.flush().await?;
    Ok(())
}

async fn shutdown_write_shared(cx: &Cx, write: &SharedWriteHalf) -> Result<()> {
    let mut guard = lock_write(cx, write).await?;
    guard.shutdown().await?;
    Ok(())
}

async fn send_data_packet_shared(
    cx: &Cx,
    write: &SharedWriteHalf,
    payload: &[u8],
    sdu: usize,
) -> Result<()> {
    let mut guard = lock_write(cx, write).await?;
    send_data_packet(&mut *guard, payload, sdu).await
}

async fn send_data_packet_shared_with_flags(
    cx: &Cx,
    write: &SharedWriteHalf,
    payload: &[u8],
    sdu: usize,
    first_packet_flags: u16,
    last_packet_flags: u16,
) -> Result<()> {
    let mut guard = lock_write(cx, write).await?;
    send_data_packet_with_flags(
        &mut *guard,
        payload,
        sdu,
        first_packet_flags,
        last_packet_flags,
    )
    .await
}

async fn send_marker_shared(cx: &Cx, write: &SharedWriteHalf, marker_type: u8) -> Result<()> {
    let mut guard = lock_write(cx, write).await?;
    send_marker(&mut *guard, marker_type).await
}

async fn send_data_packet<W>(stream: &mut W, payload: &[u8], sdu: usize) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    send_data_packet_with_flags(stream, payload, sdu, 0, 0).await
}

/// Sends a TTC payload as one or more data packets, applying
/// `first_packet_flags` to the first packet and `last_packet_flags` to the
/// last (combined when the payload fits a single packet) -- the WriteBuffer
/// `_data_flags` semantics the pipeline framing relies on (BEGIN_PIPELINE on
/// the packet carrying the begin piggyback, END_OF_REQUEST on a message's
/// final packet).
async fn send_data_packet_with_flags<W>(
    stream: &mut W,
    payload: &[u8],
    sdu: usize,
    first_packet_flags: u16,
    last_packet_flags: u16,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let max_payload = sdu.saturating_sub(TNS_DATA_PACKET_OVERHEAD).max(1);
    let chunk_count = payload.chunks(max_payload).len();
    for (index, chunk) in payload.chunks(max_payload).enumerate() {
        let mut flags = 0u16;
        if index == 0 {
            flags |= first_packet_flags;
        }
        if index + 1 == chunk_count {
            flags |= last_packet_flags;
        }
        let packet = encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(flags),
            chunk,
            PacketLengthWidth::Large32,
        )?;
        stream.write_all(&packet).await?;
    }
    stream.flush().await?;
    Ok(())
}

struct DataResponse {
    payload: Vec<u8>,
    flush_out_binds: bool,
}

async fn read_data_response(
    read: &mut OwnedReadHalf,
    cx: &Cx,
    write: &SharedWriteHalf,
) -> Result<Vec<u8>> {
    Ok(read_data_response_boundary(read, cx, write, false)
        .await?
        .payload)
}

async fn read_data_response_flushing_out_binds(
    read: &mut OwnedReadHalf,
    cx: &Cx,
    write: &SharedWriteHalf,
    sdu: usize,
) -> Result<Vec<u8>> {
    let mut response = read_data_response_boundary(read, cx, write, false).await?;
    let mut payload = response.payload;
    while response.flush_out_binds {
        if matches!(payload.last(), Some(&TNS_MSG_TYPE_FLUSH_OUT_BINDS)) {
            payload.pop();
        }
        send_data_packet_shared(cx, write, &[TNS_MSG_TYPE_FLUSH_OUT_BINDS], sdu).await?;
        response = read_data_response_boundary(read, cx, write, false).await?;
        payload.extend_from_slice(&response.payload);
    }
    Ok(payload)
}

/// Reads one boundary-delimited TTC response. While `in_pipeline` is set,
/// marker packets are silently dropped instead of triggering the
/// send-reset/await-reset dance -- the reference does the same while reading
/// pipelined responses (packet.pyx:346-370, protocol.pyx:889-906), since the
/// server emits a marker alongside an in-pipeline error without expecting a
/// reset exchange.
async fn read_data_response_boundary(
    read: &mut OwnedReadHalf,
    cx: &Cx,
    write: &SharedWriteHalf,
    in_pipeline: bool,
) -> Result<DataResponse> {
    let mut response = Vec::new();
    let mut flush_out_binds = false;
    let mut pending_packet = None;
    loop {
        let packet = match pending_packet.take() {
            Some(packet) => packet,
            None => read_packet(read, PacketLengthWidth::Large32).await?,
        };
        if packet.packet_type == TNS_PACKET_TYPE_MARKER {
            if in_pipeline {
                trace_connect_bytes("MARKER packet skipped in pipeline", &packet.payload);
                continue;
            }
            pending_packet = reset_after_marker(read, cx, write, &packet).await?;
            continue;
        }
        if packet.packet_type != TNS_PACKET_TYPE_DATA {
            return Err(oracledb_protocol::ProtocolError::UnknownMessageType {
                message_type: packet.packet_type,
                position: 4,
            }
            .into());
        }
        let (data_flags, payload) = packet.payload.split_at_checked(2).ok_or(
            oracledb_protocol::ProtocolError::TtcDecode("missing data packet flags"),
        )?;
        let flags = u16::from_be_bytes(
            data_flags
                .try_into()
                .map_err(|_| oracledb_protocol::ProtocolError::TtcDecode("invalid flags"))?,
        );
        response.extend_from_slice(payload);
        if matches!(payload.last(), Some(&TNS_MSG_TYPE_FLUSH_OUT_BINDS)) {
            flush_out_binds = true;
            break;
        }
        if flags & oracledb_protocol::thin::TNS_DATA_FLAGS_END_OF_RESPONSE != 0 {
            break;
        }
        if matches!(payload.last(), Some(&TNS_MSG_TYPE_END_OF_RESPONSE)) {
            break;
        }
    }
    Ok(DataResponse {
        payload: response,
        flush_out_binds,
    })
}

const TNS_PACKET_TYPE_MARKER: u8 = 12;
const TNS_MARKER_TYPE_BREAK: u8 = 1;
const TNS_MARKER_TYPE_RESET: u8 = 2;

async fn reset_after_marker(
    read: &mut OwnedReadHalf,
    cx: &Cx,
    write: &SharedWriteHalf,
    initial_marker: &IncomingPacket,
) -> Result<Option<IncomingPacket>> {
    trace_connect_bytes("MARKER packet", &initial_marker.payload);
    send_marker_shared(cx, write, TNS_MARKER_TYPE_RESET).await?;
    loop {
        let packet = read_packet(read, PacketLengthWidth::Large32).await?;
        if packet.packet_type != TNS_PACKET_TYPE_MARKER {
            return Ok(Some(packet));
        }
        trace_connect_bytes("MARKER reset response", &packet.payload);
        if matches!(packet.payload.get(2), Some(&TNS_MARKER_TYPE_RESET)) {
            return Ok(None);
        }
    }
}

async fn send_marker<W>(stream: &mut W, marker_type: u8) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let packet = encode_packet(
        TNS_PACKET_TYPE_MARKER,
        0,
        None,
        &[1, 0, marker_type],
        PacketLengthWidth::Large32,
    )?;
    trace_connect_bytes("send MARKER", &packet);
    stream.write_all(&packet).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_packet<R>(stream: &mut R, width: PacketLengthWidth) -> Result<IncomingPacket>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; 8];
    stream.read_exact(&mut header).await?;
    let [len0, len1, len2, len3, packet_type, _, _, _] = header;
    let declared = match width {
        PacketLengthWidth::Legacy16 => usize::from(u16::from_be_bytes([len0, len1])),
        PacketLengthWidth::Large32 => {
            usize::try_from(u32::from_be_bytes([len0, len1, len2, len3])).unwrap_or(usize::MAX)
        }
    };
    if declared < header.len() {
        return Err(oracledb_protocol::ProtocolError::InvalidPacketLength {
            length: declared,
            minimum: header.len(),
        }
        .into());
    }
    let mut payload = vec![0u8; declared - header.len()];
    stream.read_exact(&mut payload).await?;
    Ok(IncomingPacket {
        packet_type,
        payload,
    })
}

fn listener_connect_descriptor(descriptor: &EasyConnect, identity: &ClientIdentity) -> String {
    format!(
        "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST={})(PORT={}))(CONNECT_DATA=(SERVICE_NAME={})(CID=(PROGRAM={})(HOST={})(USER={}))))",
        descriptor.host,
        descriptor.port,
        descriptor.service_name,
        identity.program,
        identity.machine,
        identity.osuser,
    )
}

fn auth_connect_descriptor(descriptor: &EasyConnect) -> String {
    format!(
        "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST={})(PORT={}))(CONNECT_DATA=(SERVICE_NAME={})))",
        descriptor.host, descriptor.port, descriptor.service_name
    )
}

fn parse_session_u32(
    data: &std::collections::BTreeMap<String, String>,
    key: &'static str,
) -> Result<u32> {
    data.get(key)
        .ok_or(Error::MissingSessionField(key))?
        .parse::<u32>()
        .map_err(|_| Error::MissingSessionField(key))
}

fn parse_session_u16(
    data: &std::collections::BTreeMap<String, String>,
    key: &'static str,
) -> Result<u16> {
    data.get(key)
        .ok_or(Error::MissingSessionField(key))?
        .parse::<u16>()
        .map_err(|_| Error::MissingSessionField(key))
}

fn next_ttc_sequence(seq_num: &mut u8) -> u8 {
    *seq_num = seq_num.wrapping_add(1);
    if *seq_num == 0 {
        *seq_num = 1;
    }
    *seq_num
}

fn statement_is_query(sql: &str) -> bool {
    sql.trim_start()
        .split(|ch: char| !ch.is_ascii_alphabetic())
        .next()
        .is_some_and(|keyword| keyword.eq_ignore_ascii_case("select"))
}

/// True when any column needs a client-side define to stream its value:
/// `CLOB` / `BLOB` / `VECTOR` / native `JSON`. Such columns come back from the
/// initial execute as describe-only metadata; the value is delivered on a
/// follow-up define-fetch round trip (reference `statement._requires_define`).
fn columns_require_define(columns: &[ColumnMetadata]) -> bool {
    use oracledb_protocol::thin::{
        ORA_TYPE_NUM_BLOB, ORA_TYPE_NUM_CLOB, ORA_TYPE_NUM_JSON, ORA_TYPE_NUM_VECTOR,
    };
    columns.iter().any(|column| {
        matches!(
            column.ora_type_num,
            ORA_TYPE_NUM_CLOB | ORA_TYPE_NUM_BLOB | ORA_TYPE_NUM_VECTOR | ORA_TYPE_NUM_JSON
        )
    })
}

fn trace_connect_step(step: &'static str) {
    if std::env::var_os("ORACLEDB_TRACE_CONNECT").is_some() {
        eprintln!("oracledb::connect: {step}");
    }
}

fn trace_connect_value(label: &'static str, value: &str) {
    if std::env::var_os("ORACLEDB_TRACE_CONNECT").is_some() {
        eprintln!("oracledb::connect: {label}: {value}");
    }
}

fn trace_connect_bytes(label: &'static str, bytes: &[u8]) {
    if std::env::var_os("ORACLEDB_TRACE_CONNECT").is_some() {
        let mut hex = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            use std::fmt::Write as _;
            let _ = write!(&mut hex, "{byte:02x}");
        }
        eprintln!("oracledb::connect: {label} len={} hex={hex}", bytes.len());
    }
}

fn trace_query_bytes(label: &'static str, bytes: &[u8]) {
    if std::env::var_os("ORACLEDB_TRACE_QUERY").is_some() {
        let mut hex = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            use std::fmt::Write as _;
            let _ = write!(&mut hex, "{byte:02x}");
        }
        eprintln!("oracledb::query: {label} len={} hex={hex}", bytes.len());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    fn identity() -> ClientIdentity {
        ClientIdentity::new("program", "machine", "osuser", "terminal", "driver")
            .expect("test identity should be valid")
    }

    #[test]
    fn descriptor_builder_uses_identity_in_listener_cid() {
        let options = ConnectOptions::new("localhost/FREEPDB1", "user", "password", identity());
        let descriptor =
            EasyConnect::parse(&options.connect_string).expect("test connect string should parse");
        let built = listener_connect_descriptor(&descriptor, &options.identity);
        assert!(built.contains("(PROGRAM=program)"));
        assert!(built.contains("(HOST=machine)"));
        assert!(built.contains("(USER=osuser)"));
    }

    #[test]
    fn cancel_handle_sends_tns_break_marker() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            let mut packet = [0u8; 11];
            socket.read_exact(&mut packet).expect("read marker packet");
            packet
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let mut handle = runtime.block_on(async {
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (_read, write) = stream.into_split();
            CancelHandle {
                write: Arc::new(AsyncMutex::with_name("oracle_tcp_write_test", write)),
            }
        });

        handle.cancel().expect("cancel marker write");

        let packet = server.join().expect("server thread joins");
        assert_eq!(
            packet,
            [
                0,
                0,
                0,
                11,
                TNS_PACKET_TYPE_MARKER,
                0,
                0,
                0,
                1,
                0,
                TNS_MARKER_TYPE_BREAK
            ]
        );
    }
}
