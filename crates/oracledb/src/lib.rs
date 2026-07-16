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
//! Every connection carries a [`ClientIdentity`] the
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
//! // Bind parameters positionally (:1, :2, ...) as a tuple.
//! let row = BlockingConnection::query_one(
//!     &mut conn,
//!     "select :1 + :2 from dual",
//!     (40, 2),
//! )?;
//!
//! // Typed column access converts the Oracle NUMBER straight into an integer.
//! let sum: i64 = row.get(0)?;
//! assert_eq!(sum, 42);
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
//! [`Connection::execute_raw`] as describe-only metadata with a `None` cell,
//! matching the wire protocol. Use
//! [`Connection::define_and_fetch_rows_with_columns`] after opening the cursor
//! when you need the first batch materialized explicitly.
//!
//! # Optional features
//!
//! - `arrow`: fetch result sets directly into Apache Arrow `RecordBatch`es via
//!   `Connection::fetch_all_record_batch` and
//!   `Connection::fetch_record_batches`.
//!
//! # Connection pooling
//!
//! The [`pool`] module provides async [`Pool`](pool::Pool) and
//! [`BlockingPool`](pool::BlockingPool) facades that mirror python-oracledb's
//! thin pool: free/busy lists, growth planning, getmode semantics, ping policy,
//! idle timeout, and max lifetime. The pool is generic over a
//! [`PoolBackend`](pool::PoolBackend) so the embedder supplies how a pooled
//! connection is created, pinged, and closed.
#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::num::NonZeroU32;
use std::process;
use std::sync::Arc;
use std::time::Duration;

use asupersync::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use asupersync::net::TcpStream;
use asupersync::runtime::{reactor, Runtime, RuntimeBuilder};
use asupersync::sync::Mutex as AsyncMutex;
use asupersync::{time, Cx};
use oracledb_protocol::thin::aq::{
    build_aq_array_deq_payload, build_aq_array_enq_payload, build_aq_deq_payload,
    build_aq_enq_payload, parse_aq_array_response_with_limits, parse_aq_deq_response_with_limits,
    parse_aq_enq_response_with_limits, AqArrayResult, AqDeqOptions, AqDeqResult, AqEnqOptions,
    AqMsgProps, AqQueueDesc,
};
use oracledb_protocol::thin::{
    adjust_refetch_metadata, build_auth_phase_one_payload,
    build_auth_phase_two_payload_with_proxy_with_seq, build_begin_pipeline_piggyback,
    build_change_password_payload_with_seq, build_connect_packet_payload, build_data_types_payload,
    build_define_fetch_payload_with_seq, build_end_pipeline_payload_with_seq,
    build_execute_payload_with_bind_rows_and_options_with_seq, build_fast_auth_phase_one_payload,
    build_fast_auth_token_payload, build_fetch_payload_with_seq, build_function_payload_with_seq,
    build_function_payload_with_seq_and_token, build_lob_create_temp_payload_with_seq,
    build_lob_free_temp_payload_with_seq, build_lob_read_payload_with_seq,
    build_lob_trim_payload_with_seq, build_lob_write_payload_with_seq,
    build_protocol_negotiation_payload, classic_connect_response_is_complete,
    connect_data_fits_inline, parse_accept_payload, parse_auth_response_with_limits,
    parse_define_fetch_response_borrowed_with_limits,
    parse_define_fetch_response_with_context_and_limits,
    parse_fetch_response_with_context_and_limits, parse_lob_create_temp_response_with_limits,
    parse_lob_free_temp_response_with_limits, parse_lob_read_response_with_limits,
    parse_lob_trim_response_with_limits, parse_lob_write_response_with_limits,
    parse_plain_function_response_with_limits, parse_query_response_borrowed_with_limits,
    parse_query_response_with_binds_options_columns_and_limits,
    parse_tpc_txn_switch_response_with_limits, BindValue, BorrowedFetchResult, ClientCapabilities,
    ColumnMetadata, CursorValue, ExecuteOptions, LobReadResult, QueryResult, QueryValueRef,
    SessionlessTxnState, TpcChangeStateResponse, TpcSwitchResponse, TpcXid,
    TNS_DATA_FLAGS_BEGIN_PIPELINE, TNS_DATA_FLAGS_END_OF_REQUEST, TNS_FUNC_COMMIT, TNS_FUNC_LOGOFF,
    TNS_FUNC_PING, TNS_FUNC_ROLLBACK, TNS_MSG_TYPE_END_OF_RESPONSE, TNS_MSG_TYPE_FLUSH_OUT_BINDS,
    TNS_PACKET_FLAG_REDIRECT, TNS_PACKET_TYPE_ACCEPT, TNS_PACKET_TYPE_CONNECT,
    TNS_PACKET_TYPE_DATA, TNS_PACKET_TYPE_REDIRECT, TNS_PACKET_TYPE_REFUSE, TNS_PACKET_TYPE_RESEND,
    TNS_PIPELINE_MODE_ABORT_ON_ERROR, TNS_PIPELINE_MODE_CONTINUE_ON_ERROR, TNS_TPC_TXN_ABORT,
    TNS_TPC_TXN_COMMIT, TNS_TPC_TXN_DETACH, TNS_TPC_TXN_POST_DETACH, TNS_TPC_TXN_PREPARE,
    TNS_TPC_TXN_START, TNS_TPC_TXN_STATE_ABORTED, TNS_TPC_TXN_STATE_COMMITTED,
    TNS_TPC_TXN_STATE_FORGOTTEN, TNS_TPC_TXN_STATE_PREPARE, TNS_TPC_TXN_STATE_READ_ONLY,
    TNS_TPC_TXN_STATE_REQUIRES_COMMIT, TPC_TXN_FLAGS_NEW, TPC_TXN_FLAGS_RESUME,
    TPC_TXN_FLAGS_SESSIONLESS,
};
use oracledb_protocol::thin::{
    build_notify_payload_with_seq, build_subscribe_payload_with_seq,
    check_notification_header_with_limits, parse_subscribe_response_with_limits,
    try_parse_oac_record_with_limits, NotificationRecord, SubscribeResult, TNS_SUBSCR_OP_REGISTER,
    TNS_SUBSCR_OP_UNREGISTER,
};
use oracledb_protocol::thin::{
    build_sessionless_piggyback, build_tpc_change_state_payload_with_seq_and_version,
    build_tpc_switch_payload_with_seq_and_version, build_tpc_txn_switch_payload_with_seq,
    parse_tpc_change_state_response_with_limits, parse_tpc_switch_response_with_limits,
};
use oracledb_protocol::thin::{TNS_AQ_ARRAY_DEQ, TNS_AQ_ARRAY_ENQ};
use oracledb_protocol::wire::{encode_packet, PacketLengthWidth, ProtocolLimits};
use oracledb_protocol::{
    net::{
        connectstring::{
            Address, AddressList, Description, Descriptor, DEFAULT_SDU as DSN_DEFAULT_SDU,
        },
        EasyConnect, Protocol as NetProtocol,
    },
    ClientIdentity,
};

const PYTHON_ORACLEDB_COMPAT_VERSION_NUM: u32 = 0x0400_1000;
const DEFAULT_SDU: usize = 8192;

/// Upper bound on server-requested CONNECT resends before giving up. A real
/// server asks once (pre-23ai, long connect data); the bound only guards
/// against a peer that never stops answering RESEND.
const MAX_CONNECT_RESEND_ROUNDS: u8 = 8;

/// Upper bound on listener REDIRECT hops before giving up. A real redirect
/// chain is one hop (shared server / RAC dispatcher); the reference does not
/// bound the loop, but a pair of misconfigured listeners redirecting to each
/// other must terminate here instead of spinning forever.
const MAX_CONNECT_REDIRECT_ROUNDS: u8 = 8;

/// Human name for a TNS network packet type, for connect-phase diagnostics.
/// These are packet-layer types (header offset 4), not TTC message types.
fn tns_packet_type_name(packet_type: u8) -> &'static str {
    match packet_type {
        1 => "CONNECT",
        2 => "ACCEPT",
        3 => "ACK",
        4 => "REFUSE",
        5 => "REDIRECT",
        6 => "DATA",
        7 => "NULL",
        9 => "ABORT",
        11 => "RESEND",
        12 => "MARKER",
        13 => "ATTENTION",
        14 => "CONTROL",
        _ => "unknown",
    }
}
const TNS_DATA_PACKET_OVERHEAD: usize = 10;

pub use oracledb_protocol as protocol;

/// The version of this driver crate, e.g. `"0.7.3"`.
///
/// Consumers that wrap the driver (for example `oraclemcp-db`'s `doctor`) must
/// report the *driver's* real version, not their own: `env!("CARGO_PKG_VERSION")`
/// evaluated inside a wrapping crate resolves to that wrapper's version. This
/// const is evaluated in the driver crate, so it always reflects the actual
/// `oracledb` version the wrapper is linked against.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Profiling-only read/decode attribution counters for the fetch paging loop.
///
/// **This is measurement-only instrumentation, not part of the optimization.**
/// When the `ORACLEDB_PROFILE_FETCH` environment variable is set (checked once,
/// lazily), [`fetch_rows_with_columns`](Connection::fetch_rows_with_columns)
/// accumulates the wall time spent in the socket read (`read_response`) vs the
/// CPU decode (`parse_fetch_response`) into these atomics. A bench / example can
/// read the split with [`fetch_profile_read_decode_ns`] to attribute how much of
/// a paged fetch is socket-bound (overlap candidate) vs CPU-bound. The flag is
/// off by default, so the production path pays one relaxed atomic load per page
/// and nothing else.
mod fetch_profile {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::OnceLock;

    static READ_NS: AtomicU64 = AtomicU64::new(0);
    static DECODE_NS: AtomicU64 = AtomicU64::new(0);
    static ENABLED: OnceLock<bool> = OnceLock::new();
    /// Lets a bench arm/disarm attribution without an env var (so a single
    /// process can measure a clean window).
    static FORCE: AtomicBool = AtomicBool::new(false);

    #[inline]
    pub(crate) fn enabled() -> bool {
        FORCE.load(Ordering::Relaxed)
            || *ENABLED.get_or_init(|| std::env::var_os("ORACLEDB_PROFILE_FETCH").is_some())
    }

    #[inline]
    pub(crate) fn add_read(ns: u64) {
        READ_NS.fetch_add(ns, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn add_decode(ns: u64) {
        DECODE_NS.fetch_add(ns, Ordering::Relaxed);
    }

    pub(crate) fn snapshot() -> (u64, u64) {
        (
            READ_NS.load(Ordering::Relaxed),
            DECODE_NS.load(Ordering::Relaxed),
        )
    }

    pub(crate) fn reset() {
        READ_NS.store(0, Ordering::Relaxed);
        DECODE_NS.store(0, Ordering::Relaxed);
    }

    pub(crate) fn set_force(on: bool) {
        FORCE.store(on, Ordering::Relaxed);
    }
}

/// Profiling-only: snapshot the cumulative `(read_ns, decode_ns)` split that the
/// fetch paging loop has accumulated since the last [`fetch_profile_reset`].
///
/// Only populated when fetch profiling is armed (the `ORACLEDB_PROFILE_FETCH`
/// env var is set, or [`fetch_profile_arm(true)`](fetch_profile_arm) was called).
/// This is benchmark/diagnostic instrumentation; it is not part of the normal
/// data path.
pub fn fetch_profile_read_decode_ns() -> (u64, u64) {
    fetch_profile::snapshot()
}

/// Profiling-only: zero the fetch read/decode attribution counters.
pub fn fetch_profile_reset() {
    fetch_profile::reset();
}

/// Profiling-only: arm or disarm fetch read/decode attribution for this process
/// without setting an environment variable.
pub fn fetch_profile_arm(on: bool) {
    fetch_profile::set_force(on);
}

#[cfg(feature = "arrow")]
#[path = "arrow/mod.rs"]
pub mod arrow;
/// Executemany batch-chunk bookkeeping. Private module: the user-facing surface
/// is the three items re-exported at the crate root below, so there is exactly
/// one public path per item (no `oracledb::cursor_logic::…` second path).
mod cursor_logic;
/// Feature-gated observability seam (bead rust-oracledb-lv6). Always compiled so
/// the `obs_span!` / `obs_record!` macros resolve, but its `tracing`-touching
/// items are themselves `#[cfg(feature = "tracing")]`, so the off-build pulls in
/// no `tracing` dependency. See `docs/OBSERVABILITY.md`.
#[macro_use]
mod obs;
/// Lazy, on-demand streaming over LOB locators (bead a4-bbx). The user-facing
/// types are re-exported at the crate root (the single canonical public path).
pub(crate) mod lob_stream;
pub mod pool;
mod recovery;
mod request;
/// Idempotency-gated retry executor over the ORA error taxonomy (bead a4-r9a).
pub mod retry;
mod routine;
/// Owning row-by-row query stream (K10). The user-facing [`OwnedRowStream`] is
/// re-exported at the crate root (the single canonical public path).
mod row_stream;
mod rows;
/// Cross-connection statement-shape cache with DDL-invalidation self-heal
/// (bead a4-8pp). The user-facing types are re-exported at the crate root (the
/// single canonical public path).
pub(crate) mod shape_cache;
#[cfg(feature = "soda")]
pub mod soda;
mod sql_convert;
pub(crate) mod tls;
// The `tls` module itself is crate-private, but the wallet-precedence
// resolution accessor and its outcome types are part of the public API (a
// server doctor calls them to report which wallet file wins without re-deriving
// the driver's precedence). Re-exported flat at the crate root — the single
// public path for each.
pub use tls::{resolve_wallet, WalletFile, WalletResolution};
pub mod transport;

/// L2 version cassettes: live capture + offline replay of the per-version
/// connect-negotiation wire exchange (bead rust-oracledb-xver-parity-so3w.3).
/// Test-only, and only with the `cassette` feature (it uses the record/replay
/// transport seam). Not part of the public API and compiled out of every
/// shipping build.
#[cfg(all(test, feature = "cassette"))]
mod version_cassettes;

/// Re-export of the `tracing` crate for the `obs_span!` / `obs_record!` macros
/// (`$crate::__tracing::…`). Hidden and feature-gated; not part of the public
/// API. Only exists when the `tracing` feature is on.
#[cfg(feature = "tracing")]
#[doc(hidden)]
pub use tracing as __tracing;

/// Off-build no-op span guard the `obs_span!` macro yields when the `tracing`
/// feature is off (hoisted to crate root for `$crate::ObsSpanGuard`).
#[cfg(not(feature = "tracing"))]
#[doc(hidden)]
pub use obs::ObsSpanGuard;

pub use cursor_logic::{
    bind_rows_need_iterative_plsql, ExecutemanyManager, ExecutemanyManagerError,
};

pub use request::{Batch, BatchRows, Execute, Query, Registration, Scroll};
use request::{DeadlineExpiry, QueryDeadline};
pub use row_stream::OwnedRowStream;
pub use rows::{
    BatchError, BatchOutcome, BlockingRows, ExecuteOutcome, OutBinds, RegistrationOutcome,
    ReturningRows, Row, Rows,
};

pub use routine::{OutType, RoutineCall, RoutineOutcome};

pub use shape_cache::{ColumnShape, ShapeObservation, StatementShapeCache};

pub use lob_stream::{ClobReader, LobReader, LobWriter};

pub use sql_convert::{
    check_bind_rows, check_positional_binds, declared_bind_count, BindError, ConversionError,
    FromRow, FromSql, IntoBinds, Params, QueryResultExt, ToSql, TypedRow,
};

/// Derive a [`FromRow`] implementation that maps a query row into a struct with
/// compile-time-checked field types.
///
/// Available with the default-on `derive` feature. The derive and the
/// [`FromRow`] trait share a name, so a single `use oracledb::FromRow;` brings
/// both into scope.
///
/// ```no_run
/// use oracledb::FromRow;
///
/// #[derive(FromRow)]
/// struct Emp {
///     id: i64,
///     name: String,
///     manager_id: Option<i64>,
/// }
/// ```
///
/// See the [`FromRow`] trait docs for the supported shapes and `#[oracledb(...)]`
/// attributes.
#[cfg(feature = "derive")]
pub use oracledb_derive::FromRow;

/// The everyday types and traits, for a single glob import.
///
/// A typical program needs a connection type, [`ConnectOptions`], the
/// [`ClientIdentity`] the database records, the value
/// types it binds and reads back ([`BindValue`] /
/// [`QueryValue`](protocol::thin::QueryValue)), and the typed-row helpers
/// ([`FromRow`], [`QueryResultExt`], [`params!`]). Those live across two
/// namespaces (the driver root and the [`protocol`] re-export), so the prelude
/// gathers them so callers can write one line:
///
/// ```no_run
/// use oracledb::prelude::*;
///
/// # fn main() -> oracledb::Result<()> {
/// let identity = ClientIdentity::new("app", "host", "user", "term", "rust-oracledb")?;
/// let options = ConnectOptions::new("dbhost:1521/FREEPDB1", "app_user", "app_pw", identity);
/// let mut conn = BlockingConnection::connect(options)?;
/// let binds = params! { "id" => 42_i64 };
/// # let _ = (&mut conn, binds);
/// # BlockingConnection::close(conn)?;
/// # Ok(())
/// # }
/// ```
///
/// The prelude is a curated convenience namespace, not a second canonical home:
/// each item's one obvious path is still its non-prelude path
/// (`oracledb::Connection`, `oracledb::protocol::thin::QueryValue`, ...). Reach
/// for an explicit `use` when you want exactly one name or a less common type.
///
/// It deliberately does **not** re-export [`Result`] or [`Error`]: a 1-argument
/// `Result` alias and an `Error` type in a glob import shadow
/// `std::result::Result` / `std::error::Error`, which surprises callers. Name
/// those explicitly as `oracledb::Result` / `oracledb::Error`.
pub mod prelude {
    pub use crate::protocol::thin::{BindValue, QueryValue};
    pub use crate::protocol::ClientIdentity;
    pub use crate::{
        params, BlockingConnection, ConnectOptions, Connection, FromRow, Params, QueryResultExt,
    };
}

#[cfg(test)]
use recovery::cancel_disposition;
use recovery::{
    classify_recovery_result, decode_server_version_number,
    observe_cancellation_between_round_trips, post_sync_protocol_error_disposition,
    protocol_error_is_session_dead, protocol_error_kind, protocol_error_offset,
    protocol_error_ora_code, run_recovery_without_current_cx,
    server_version_number_uses_extended_layout, CancelDisposition, PostSyncProtocolDisposition,
    RecoveryWireAction, SessionRecovery, SessionRecoveryPhase, CONNECTION_LOST_ORA_CODES,
    SESSION_DEAD_ORA_CODES, TRANSIENT_ORA_CODES,
};

use transport::{Connector, WireTransport};

type DriverConnector = transport::OracleConnector;
type DriverTransport = <DriverConnector as Connector>::Transport;
type SharedWriteHalf<T = DriverTransport> = Arc<AsyncMutex<<T as WireTransport>::Write>>;
type DriverCore = ConnectionCore<DriverTransport>;

#[derive(Debug)]
struct ConnectionCore<T: WireTransport> {
    read: Option<T::Read>,
    write: SharedWriteHalf<T>,
    recovery: Arc<SessionRecovery>,
    protocol_limits: ProtocolLimits,
    /// Optional per-read inactivity deadline (GH#14). `None` = unbounded reads
    /// (prior behaviour); `Some(d)` fails a stalled post-auth read with
    /// [`Error::CallTimeout`] after `d`. Set once at connect from
    /// [`ConnectOptions::inactivity_timeout`].
    inactivity_timeout: Option<Duration>,
    /// Whether this session negotiated the *classic* (pre-END_OF_RESPONSE)
    /// framing, i.e. `!supports_end_of_response`. Set once at connect time from
    /// the ACCEPT capabilities. The recovery drain reads this to decide the
    /// trailing-error boundary the way the connected server frames it (bead
    /// rust-oracledb-99xu); `false` (23ai framing) until the session negotiates.
    classic_framing: bool,
}

impl<T: WireTransport> ConnectionCore<T> {
    fn from_halves(read: T::Read, write: T::Write, write_name: &'static str) -> Self {
        Self {
            read: Some(read),
            write: Arc::new(AsyncMutex::with_name(write_name, write)),
            recovery: Arc::new(SessionRecovery::new()),
            protocol_limits: ProtocolLimits::DEFAULT,
            inactivity_timeout: None,
            classic_framing: false,
        }
    }

    /// Records whether this session uses classic (pre-END_OF_RESPONSE) framing,
    /// so the recovery drain can decide the trailing-error boundary correctly
    /// on pre-23ai servers (bead rust-oracledb-99xu).
    fn set_classic_framing(&mut self, classic: bool) {
        self.classic_framing = classic;
    }

    fn set_protocol_limits(&mut self, limits: ProtocolLimits) -> Result<()> {
        self.protocol_limits = limits.validate()?;
        Ok(())
    }

    /// Sets the per-read inactivity deadline for this session (GH#14). `None`
    /// leaves reads unbounded (prior behaviour).
    fn set_inactivity_timeout(&mut self, timeout: Option<Duration>) {
        self.inactivity_timeout = timeout;
    }

    fn read_mut(&mut self) -> Result<&mut T::Read> {
        self.read.as_mut().ok_or_else(|| {
            Error::ConnectionClosed("connection read half unavailable during recovery".into())
        })
    }

    fn take_read(&mut self) -> Result<T::Read> {
        self.read.take().ok_or_else(|| {
            Error::ConnectionClosed("connection read half unavailable during recovery".into())
        })
    }

    fn write_handle(&self) -> SharedWriteHalf<T> {
        Arc::clone(&self.write)
    }

    async fn write_all(&self, cx: &Cx, packet: &[u8]) -> Result<()> {
        write_all_shared(cx, &self.write, packet).await
    }

    async fn shutdown_write(&self, cx: &Cx) -> Result<()> {
        shutdown_write_shared(cx, &self.write).await
    }

    async fn send_data_packet(&self, cx: &Cx, payload: &[u8], sdu: usize) -> Result<()> {
        send_data_packet_shared(cx, &self.write, payload, sdu).await
    }

    async fn send_data_packet_with_flags(
        &self,
        cx: &Cx,
        payload: &[u8],
        sdu: usize,
        first_packet_flags: u16,
        last_packet_flags: u16,
    ) -> Result<()> {
        send_data_packet_shared_with_flags(
            cx,
            &self.write,
            payload,
            sdu,
            first_packet_flags,
            last_packet_flags,
        )
        .await
    }

    async fn read_packet(&mut self, width: PacketLengthWidth) -> Result<IncomingPacket> {
        let limits = self.protocol_limits;
        let inactivity = self.inactivity_timeout;
        let result = apply_inactivity_timeout(
            inactivity,
            read_packet_with_limits(self.read_mut()?, width, limits),
        )
        .await;
        self.note_post_sync_result(result)
    }

    async fn read_data_response(&mut self, cx: &Cx) -> Result<Vec<u8>> {
        let write = Arc::clone(&self.write);
        let limits = self.protocol_limits;
        let inactivity = self.inactivity_timeout;
        let result = apply_inactivity_timeout(
            inactivity,
            read_data_response_with_limits(self.read_mut()?, cx, &write, limits),
        )
        .await;
        self.note_post_sync_result(result)
    }

    /// Reads one classic (pre-END_OF_RESPONSE) connect-phase response. Servers
    /// that did not negotiate END_OF_RESPONSE framing never set the
    /// end-of-response DATA flag, so the flag-driven boundary reader would wait
    /// forever; completion is decided by the payload's terminal message instead
    /// (`classic_connect_response_is_complete`).
    async fn read_classic_data_response(&mut self, cx: &Cx) -> Result<Vec<u8>> {
        let write = Arc::clone(&self.write);
        let limits = self.protocol_limits;
        let inactivity = self.inactivity_timeout;
        let result = apply_inactivity_timeout(
            inactivity,
            read_classic_data_response_with_limits(self.read_mut()?, cx, &write, limits),
        )
        .await;
        self.note_post_sync_result(result)
    }

    /// Reads one TTC response, deciding completion the way the connected
    /// server framing requires. With `classic == false` this is exactly
    /// [`read_data_response`](Self::read_data_response) (END_OF_RESPONSE/EOF
    /// data-flag framing, 23ai+). With `classic == true` (ACCEPT protocol
    /// version < 319 — the server never sends those flags) DATA packets are
    /// accumulated and `probe(&accumulated)` decides completion after each
    /// packet: the caller probes with the same parser it will run on the
    /// returned buffer, so the response is complete precisely when that parser
    /// can consume it without running out of bytes (reference
    /// messages/base.pyx:249-252, 294-298 — a classic response ends at its
    /// terminal ERROR/STATUS/FLUSH_OUT_BINDS message).
    async fn read_data_response_probed(
        &mut self,
        cx: &Cx,
        classic: bool,
        probe: impl Fn(&[u8]) -> bool,
    ) -> Result<Vec<u8>> {
        if !classic {
            return self.read_data_response(cx).await;
        }
        let write = Arc::clone(&self.write);
        let limits = self.protocol_limits;
        let inactivity = self.inactivity_timeout;
        let result = apply_inactivity_timeout(
            inactivity,
            read_classic_data_response_probed_with_limits(
                self.read_mut()?,
                cx,
                &write,
                &probe,
                limits,
            ),
        )
        .await;
        self.note_post_sync_result(result)
    }

    /// [`read_data_response_probed`](Self::read_data_response_probed) for the
    /// bind/execute path, which must answer FLUSH_OUT_BINDS requests. With
    /// `classic == false` this is exactly
    /// [`read_data_response_flushing_out_binds`](Self::read_data_response_flushing_out_binds).
    async fn read_data_response_flushing_out_binds_probed(
        &mut self,
        cx: &Cx,
        sdu: usize,
        classic: bool,
        probe: impl Fn(&[u8]) -> bool,
    ) -> Result<Vec<u8>> {
        if !classic {
            return self.read_data_response_flushing_out_binds(cx, sdu).await;
        }
        let write = Arc::clone(&self.write);
        let limits = self.protocol_limits;
        let inactivity = self.inactivity_timeout;
        let result = apply_inactivity_timeout(
            inactivity,
            read_classic_data_response_flushing_out_binds_probed_with_limits(
                self.read_mut()?,
                cx,
                &write,
                sdu,
                &probe,
                limits,
            ),
        )
        .await;
        self.note_post_sync_result(result)
    }

    async fn read_data_response_boundary(
        &mut self,
        cx: &Cx,
        in_pipeline: bool,
    ) -> Result<DataResponse> {
        let write = Arc::clone(&self.write);
        let limits = self.protocol_limits;
        let inactivity = self.inactivity_timeout;
        let result = apply_inactivity_timeout(
            inactivity,
            read_data_response_boundary_with_limits(
                self.read_mut()?,
                cx,
                &write,
                in_pipeline,
                limits,
            ),
        )
        .await;
        self.note_post_sync_result(result)
    }

    async fn read_data_response_flushing_out_binds(
        &mut self,
        cx: &Cx,
        sdu: usize,
    ) -> Result<Vec<u8>> {
        let write = Arc::clone(&self.write);
        let limits = self.protocol_limits;
        let inactivity = self.inactivity_timeout;
        let result = apply_inactivity_timeout(
            inactivity,
            read_data_response_flushing_out_binds_with_limits(
                self.read_mut()?,
                cx,
                &write,
                sdu,
                limits,
            ),
        )
        .await;
        self.note_post_sync_result(result)
    }

    fn note_post_sync_result<U>(&self, result: Result<U>) -> Result<U> {
        if let Err(Error::Protocol(err)) = &result {
            if post_sync_protocol_error_disposition(err) == PostSyncProtocolDisposition::Dead {
                self.recovery.mark_dead();
            }
        }
        result
    }

    fn break_and_drain_wire(&mut self, recovery_timeout: Duration) -> Result<()> {
        self.run_recovery_drain(RecoveryWireAction::BreakAndDrain, recovery_timeout)
    }

    fn cancel_and_drain_wire(&mut self, recovery_timeout: Duration) -> Result<()> {
        self.run_recovery_drain(RecoveryWireAction::BreakAndDrain, recovery_timeout)
    }

    fn drain_cancel_wire(&mut self, recovery_timeout: Duration) -> Result<()> {
        self.run_recovery_drain(RecoveryWireAction::DrainCancel, recovery_timeout)
    }

    fn run_recovery_drain(
        &mut self,
        action: RecoveryWireAction,
        recovery_timeout: Duration,
    ) -> Result<()> {
        let read = self.take_read()?;
        let write = Arc::clone(&self.write);
        let limits = self.protocol_limits;
        let classic = self.classic_framing;
        let thread = std::thread::Builder::new()
            .name("oracledb-recovery-drain".to_string())
            .spawn(move || {
                let mut read = read;
                let result = run_recovery_without_current_cx(
                    &mut read,
                    &write,
                    action,
                    recovery_timeout,
                    limits,
                    classic,
                );
                (read, result)
            })
            .map_err(|err| {
                Error::ConnectionClosed(format!("failed to start recovery drain thread: {err}"))
            })?;

        match thread.join() {
            Ok((read, result)) => {
                self.read = Some(read);
                result
            }
            Err(_) => Err(Error::ConnectionClosed(
                "recovery drain thread panicked".to_string(),
            )),
        }
    }
}

/// Render a compiler-style caret diagnostic: the line of `sql` containing the
/// 1-based character `offset` (the position Oracle reports for a parse error),
/// with a `^` under that character, beneath `headline`.
///
/// Pure and panic-free: `offset` is clamped into range (a 0 or out-of-range
/// offset points at the start / just past the end). Counts in Unicode scalar
/// values, so multibyte SQL stays aligned; tabs count as one column.
///
/// ```
/// let d = oracledb::render_caret(
///     "select * from no_such_table",
///     15,
///     "ORA-00942: table or view does not exist",
/// );
/// assert!(d.contains("no_such_table"));
/// assert!(d.lines().last().unwrap().trim_end().ends_with('^'));
/// ```
pub fn render_caret(sql: &str, offset: usize, headline: &str) -> String {
    let chars: Vec<char> = sql.chars().collect();
    let total = chars.len();
    // 1-based -> 0-based char index, clamped into [0, total].
    let target = offset.saturating_sub(1).min(total);

    let mut line_start = 0usize;
    let mut line_no = 1usize;
    let mut col = 0usize;
    for (i, &c) in chars.iter().enumerate() {
        if i == target {
            break;
        }
        if c == '\n' {
            line_no += 1;
            line_start = i + 1;
            col = 0;
        } else {
            col += 1;
        }
    }

    let mut line_end = line_start;
    while line_end < total && chars[line_end] != '\n' {
        line_end += 1;
    }
    let line: String = chars[line_start..line_end].iter().collect();
    let gutter = line_no.to_string();
    let pad = " ".repeat(gutter.len());
    let caret_indent = " ".repeat(col);
    format!("{headline}\n{pad} |\n{gutter} | {line}\n{pad} | {caret_indent}^")
}

/// Captured `DBMS_OUTPUT`, bounded by the caller's line/char limits. Returned by
/// [`Connection::read_dbms_output`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DbmsOutput {
    /// The captured lines, in the order `DBMS_OUTPUT.GET_LINE` returned them.
    lines: Vec<String>,
    /// Number of lines captured (`== lines.len()`).
    line_count: usize,
    /// Total characters (Unicode scalar values) across all captured lines.
    char_count: usize,
    /// `true` if capture stopped at a `max_lines`/`max_chars` bound while more
    /// output was still buffered server-side; `false` if it drained to the end.
    truncated: bool,
}

impl DbmsOutput {
    pub fn new(lines: Vec<String>, truncated: bool) -> Self {
        let line_count = lines.len();
        let char_count = lines.iter().map(|line| line.chars().count()).sum();
        Self {
            lines,
            line_count,
            char_count,
            truncated,
        }
    }

    pub fn lines(&self) -> &[String] {
        &self.lines
    }

    pub fn line_count(&self) -> usize {
        self.line_count
    }

    pub fn char_count(&self) -> usize {
        self.char_count
    }

    pub fn truncated(&self) -> bool {
        self.truncated
    }

    pub fn into_lines(self) -> Vec<String> {
        self.lines
    }
}

/// A scalar attribute of an Oracle ADT/object type (from the data dictionary).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectAttribute {
    /// Attribute name, uppercased as Oracle stores it.
    pub name: String,
    /// Oracle attribute type name (`ALL_TYPE_ATTRS.ATTR_TYPE_NAME`), e.g.
    /// `"VARCHAR2"`, `"NUMBER"`, `"DATE"`.
    pub type_name: String,
    /// `Some(owner)` when the attribute is itself an object/collection type (a
    /// nested ADT); `None` for built-in scalar types.
    pub type_owner: Option<String>,
}

/// Element type metadata for an Oracle collection type (VARRAY / nested table),
/// from `ALL_COLL_TYPES`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionElement {
    /// Oracle element type name (`ALL_COLL_TYPES.ELEM_TYPE_NAME`), e.g.
    /// `"NUMBER"`, `"VARCHAR2"`.
    pub type_name: String,
    /// `Some(owner)` when the element is itself an object/collection type; `None`
    /// for built-in scalar element types.
    pub type_owner: Option<String>,
}

/// Metadata for an Oracle ADT/object type, fetched from the data dictionary by
/// [`Connection::describe_object_type`]. A type is either an *object* (carrying
/// `attributes`) or a *collection* (carrying `collection_element`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectType {
    /// Owning schema (uppercased).
    pub schema: String,
    /// Type name (uppercased).
    pub name: String,
    /// Attributes in declaration order (empty for a collection type).
    pub attributes: Vec<ObjectAttribute>,
    /// `Some(..)` when this type is a collection (VARRAY / nested table); carries
    /// its element metadata. `None` for object types (see `attributes`).
    pub collection_element: Option<CollectionElement>,
}

/// A decoded Oracle ADT value. For an *object* type the scalar attributes are in
/// `attributes`; for a *collection* type the elements are in `elements`. Each
/// value is decoded with the same rules as a normal column. Returned by
/// [`decode_object`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DecodedObject {
    /// The object type's schema.
    type_schema: String,
    /// The object type's name.
    type_name: String,
    /// `(attribute name, value)` in declaration order; `None` is SQL `NULL`.
    /// Empty when the decoded value is a collection (see `elements`).
    attributes: Vec<(String, Option<oracledb_protocol::thin::QueryValue>)>,
    /// `Some(elements)` when the decoded value is a collection; each entry is one
    /// element in order (`None` is a NULL element). `None` for object values.
    elements: Option<Vec<Option<oracledb_protocol::thin::QueryValue>>>,
}

impl DecodedObject {
    pub fn object(
        type_schema: impl Into<String>,
        type_name: impl Into<String>,
        attributes: Vec<(String, Option<oracledb_protocol::thin::QueryValue>)>,
    ) -> Self {
        Self {
            type_schema: type_schema.into(),
            type_name: type_name.into(),
            attributes,
            elements: None,
        }
    }

    pub fn collection(
        type_schema: impl Into<String>,
        type_name: impl Into<String>,
        elements: Vec<Option<oracledb_protocol::thin::QueryValue>>,
    ) -> Self {
        Self {
            type_schema: type_schema.into(),
            type_name: type_name.into(),
            attributes: Vec::new(),
            elements: Some(elements),
        }
    }

    pub fn type_schema(&self) -> &str {
        &self.type_schema
    }

    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    pub fn attributes(&self) -> &[(String, Option<oracledb_protocol::thin::QueryValue>)] {
        &self.attributes
    }

    pub fn elements(&self) -> Option<&[Option<oracledb_protocol::thin::QueryValue>]> {
        self.elements.as_deref()
    }

    pub fn is_collection(&self) -> bool {
        self.elements.is_some()
    }

    pub fn into_attributes(self) -> Vec<(String, Option<oracledb_protocol::thin::QueryValue>)> {
        self.attributes
    }

    pub fn into_elements(self) -> Option<Vec<Option<oracledb_protocol::thin::QueryValue>>> {
        self.elements
    }
}

/// Decode a returned Oracle ADT object value (the payload of a
/// `QueryValue::Object`) into its scalar attributes, using the type metadata
/// from [`Connection::describe_object_type`]. Bounded by the object image
/// length, so a malformed/huge image cannot cause unbounded work.
///
/// Scoped to objects with scalar attributes: a nested object/collection
/// attribute yields a typed `Error::Protocol(UnsupportedFeature(..))`, so a
/// caller can classify "this shape isn't decodable yet" distinctly from a real
/// failure. python-oracledb only decodes objects through its thick/DbObject
/// machinery; this is the native structured surface.
pub fn decode_object(
    value: &oracledb_protocol::thin::ObjectValue,
    ty: &ObjectType,
) -> Result<DecodedObject> {
    use oracledb_protocol::thin::DbObjectPackedReader;
    use oracledb_protocol::wire::{BoundedReader, ProtocolLimits};
    let mut reader = DbObjectPackedReader::new(&value.packed_data);
    reader
        .limits()
        .check_response_bytes(value.packed_data.len())?;
    reader.read_header()?;

    // Collection type: 1 collection-flags byte, then an element count, then each
    // element value (reference impl/thin/dbobject.pyx `_unpack_data_from_buf`).
    if let Some(elem) = &ty.collection_element {
        if elem.type_owner.is_some() {
            return Err(oracledb_protocol::ProtocolError::UnsupportedFeature(
                "collection of nested object/collection elements is not decodable yet",
            )
            .into());
        }
        let _collection_flags = reader.read_u8()?;
        let num_elements = reader.read_length()?;
        reader.limits().check_object_elements(num_elements)?;
        // Every element consumes at least one byte, so the real count cannot
        // exceed the bytes still in the image; cap the pre-allocation against
        // `remaining()` so a malformed huge count can't force an unbounded Vec.
        // The loop itself is bounded too: `read_value_bytes` errors (truncated)
        // once the image is exhausted.
        let mut elements: Vec<Option<oracledb_protocol::thin::QueryValue>> =
            reader.with_capacity_limited(num_elements, 1, ProtocolLimits::check_object_elements)?;
        for _ in 0..num_elements {
            let decoded = match reader.read_value_bytes()? {
                None => None,
                Some(bytes) => Some(decode_object_scalar(&elem.type_name, bytes)?),
            };
            elements.push(decoded);
        }
        return Ok(DecodedObject {
            type_schema: ty.schema.clone(),
            type_name: ty.name.clone(),
            attributes: Vec::new(),
            elements: Some(elements),
        });
    }

    let mut attributes = Vec::with_capacity(ty.attributes.len());
    for attr in &ty.attributes {
        if attr.type_owner.is_some() {
            return Err(oracledb_protocol::ProtocolError::UnsupportedFeature(
                "nested object/collection attribute is not decodable yet",
            )
            .into());
        }
        let decoded = match reader.read_value_bytes()? {
            None => None,
            Some(bytes) => Some(decode_object_scalar(&attr.type_name, bytes)?),
        };
        attributes.push((attr.name.clone(), decoded));
    }
    Ok(DecodedObject {
        type_schema: ty.schema.clone(),
        type_name: ty.name.clone(),
        attributes,
        elements: None,
    })
}

/// Decode one scalar attribute/element value of an Oracle object/collection,
/// using the same codecs as normal column values. Returns a typed
/// `UnsupportedFeature` for a type we do not decode yet (so a caller can classify
/// the shape distinctly from a real failure).
fn decode_object_scalar(
    type_name: &str,
    bytes: Vec<u8>,
) -> Result<oracledb_protocol::thin::QueryValue> {
    use oracledb_protocol::thin::{
        decode_datetime_value, decode_dbobject_text, decode_number_value, QueryValue,
    };
    let v = if type_name.starts_with("TIMESTAMP") {
        decode_datetime_value(&bytes)?
    } else {
        match type_name {
            "VARCHAR2" | "VARCHAR" | "CHAR" => {
                QueryValue::Text(decode_dbobject_text(&bytes, "DB_TYPE_VARCHAR")?)
            }
            "NVARCHAR2" | "NCHAR" => {
                QueryValue::Text(decode_dbobject_text(&bytes, "DB_TYPE_NCHAR")?)
            }
            "NUMBER" | "FLOAT" | "INTEGER" => decode_number_value(&bytes)?,
            "DATE" => decode_datetime_value(&bytes)?,
            "RAW" => QueryValue::Raw(bytes),
            _ => {
                return Err(oracledb_protocol::ProtocolError::UnsupportedFeature(
                    "object attribute/element type is not decodable yet",
                )
                .into())
            }
        }
    };
    Ok(v)
}

/// Stable top-level error bucket for [`Error::kind`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ErrorKind {
    Network,
    Timeout,
    Cancel,
    Protocol,
    Database,
    Conversion,
    Pool,
    ResourceLimit,
    Authentication,
}

/// Whether the connection that produced an error can be reused.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectionDisposition {
    Reusable,
    Dead,
}

/// Conservative retry guidance for caller-proven idempotent operations.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum RetryHint {
    Never,
    RetrySameConnectionIfIdempotent,
    ReconnectThenRetryIfIdempotent,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Protocol(#[from] oracledb_protocol::ProtocolError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("asupersync runtime error: {0}")]
    Runtime(String),
    /// The listener redirected the connection to a target whose shape this
    /// driver refuses to follow: the redirect address demands a transport
    /// protocol CHANGE. Continuing a `tcps` connect over plain `tcp` would be
    /// a silent TLS downgrade, and a mid-connect `tcp` -> `tcps` upgrade is
    /// not supported; a redirect that keeps the original transport protocol
    /// is followed transparently.
    #[error(
        "listener redirect demands a transport protocol change (e.g. a tcps -> tcp \
         downgrade); refusing to follow it"
    )]
    RedirectUnsupported,
    /// The listener answered CONNECT with a REDIRECT packet whose redirect
    /// data could not be understood (truncated length prefix, missing the
    /// NUL separator between the target address and its connect data, or an
    /// unparseable target address). The payload describes the defect; the
    /// raw redirect bytes are not echoed verbatim.
    #[error("listener redirect data is malformed: {0}")]
    InvalidRedirectData(String),
    /// The listener kept answering every CONNECT with another REDIRECT. A
    /// real redirect chain is one hop (shared server / RAC); the bound only
    /// guards against a redirect loop between misconfigured listeners.
    #[error("listener kept redirecting the connection ({0} redirects); giving up")]
    ConnectRedirectLoop(u8),
    #[error("listener refused connection: {0}")]
    ListenerRefused(String),
    /// Every address in a multi-address connect descriptor (`ADDRESS_LIST` or
    /// several `ADDRESS` entries) failed to establish a transport. The payload
    /// aggregates the per-address failure reasons in the order they were tried.
    /// The connection never reached a listener, so there is nothing to reuse.
    #[error("all connect addresses failed: {0}")]
    AllAddressesFailed(String),
    /// `use_sni=true` was explicitly requested but the Oracle TCPS SNI string
    /// (`S{len}.{service}.V3.{version}`) is not a valid rustls DNS name (its
    /// trailing all-numeric label is rejected by RFC-strict rustls), so it
    /// cannot be transmitted. The driver fails closed here instead of silently
    /// connecting without SNI. Reconnect with `use_sni=false` (the default) to
    /// rely on the post-handshake Oracle DN match, which secures the connection
    /// without SNI.
    #[error(
        "use_sni=true cannot be honored: the Oracle SNI \"{0}\" is not a valid \
         rustls DNS name; reconnect with use_sni=false to secure the connection \
         with the post-handshake DN match instead"
    )]
    UnsupportedSni(String),
    #[error(
        "unexpected TNS packet type {ty} ({name})",
        ty = .0,
        name = tns_packet_type_name(*.0)
    )]
    UnexpectedPacket(u8),
    #[error("server kept requesting CONNECT resend ({0} rounds); giving up")]
    ConnectResendLoop(u8),
    #[error("server did not advertise fast authentication")]
    FastAuthRequired,
    #[error("server response did not contain {0}")]
    MissingSessionField(&'static str),
    #[error("call timeout of {0} ms exceeded")]
    CallTimeout(u32),
    #[error("query returned no rows")]
    NoRows,
    #[error("query returned more than one row")]
    TooManyRows,
    /// The in-flight operation was explicitly cancelled by the user (via
    /// [`Connection::cancel`] or by dropping a cancellable fetch future), the
    /// driver's analog of the server-side `ORA-01013` "user requested cancel of
    /// current operation". Like a [`Self::CallTimeout`] (`DPY-4024`), a cancel
    /// drains the wire and leaves the session ALIVE and the connection clean and
    /// reusable: it is therefore **not** [`Self::is_connection_lost`] and **is**
    /// [`Self::is_transient`] (re-run the idempotent call on the same
    /// connection). It is distinguished from `CallTimeout` only so callers can
    /// tell a deliberate cancel apart from a deadline overrun.
    #[error("ORA-01013: user requested cancel of current operation")]
    Cancelled,
    /// The connection was closed because recovery from a prior failure could
    /// not complete: most commonly a **second** timeout while draining the
    /// server's response after a [`Self::CallTimeout`] break (mirroring the
    /// reference `ERR_CONNECTION_CLOSED` raised when the post-break
    /// `_receive_packet` itself times out, protocol.pyx:454-458). Unlike
    /// [`Self::CallTimeout`], the wire stream could not be left clean, so the
    /// connection is dead and must be discarded — [`Self::is_connection_lost`]
    /// is `true` for this variant. The payload is the human-readable reason.
    #[error("DPY-4011: the database or network closed the connection: {0}")]
    ConnectionClosed(String),
    /// A TCPS/TLS transport error (wallet load, handshake, or server-cert /
    /// DN-match failure).
    #[error("TLS/TCPS error: {0}")]
    Tls(String),
    /// A wallet-location or wallet-format error. Kept distinct from generic
    /// TLS failures so callers can classify unsupported wallet formats and
    /// operator setup issues without parsing strings.
    #[error("wallet error: {0}")]
    Wallet(#[from] oracledb_protocol::tls::wallet::WalletError),
    /// Access-token authentication was requested over a non-TLS transport.
    /// A database access token must only travel over TCPS so it is not exposed
    /// in clear text (reference protocol.pyx `ERR_ACCESS_TOKEN_REQUIRES_TCPS` /
    /// DPY-3001). Reconnect with a `tcps://` connect string. The token itself is
    /// never included in this error.
    #[error("DPY-3001: access token authentication requires a TLS (TCPS) connection")]
    AccessTokenRequiresTcps,
    /// A pluggable [`TokenSource`] failed to produce a database access token.
    /// The failure is a redacted [`TokenSourceError`] class; the token and any
    /// provider detail never appear in this error.
    #[error("{0}")]
    TokenSource(TokenSourceError),
    /// The caller selected a known authentication mode that this thin build
    /// does not implement. The mode is structured so diagnostic tools can
    /// distinguish capability gaps from bad credentials, listener failures, or
    /// TLS setup errors without parsing the display string.
    #[error("{0}")]
    UnsupportedAuthMode(UnsupportedAuthMode),
    /// A sessionless transaction client-API misuse (reference
    /// ERR_SESSIONLESS_* / DPY-3034/3035/3036). The payload is the DPY full
    /// code so the shim can raise the matching DatabaseError.
    #[error("{0}")]
    SessionlessTransaction(SessionlessError),
    /// A TPC (two-phase commit) state machine returned an unexpected out state
    /// (reference `ERR_UNKNOWN_TRANSACTION_STATE` / DPY-5010). The payload is
    /// the unexpected state value.
    #[error("DPY-5010: internal error: unknown transaction state {0}")]
    UnknownTransactionState(u32),
    /// A client-side bind payload was provably incompatible with the SQL text.
    #[error("bind validation failed: {0}")]
    Bind(#[from] BindError),
    /// A typed [`FromSql`] conversion failed: the fetched value did not match
    /// the requested Rust type, was out of range, or could not be parsed. The
    /// payload describes the mismatch.
    #[error("type conversion failed: {0}")]
    Conversion(ConversionError),
    #[cfg(feature = "arrow")]
    #[error(transparent)]
    ArrowConversion(#[from] arrow::ArrowConversionError),
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn duration_to_millis_saturating(duration: Duration) -> u32 {
    duration.as_millis().min(u128::from(u32::MAX)) as u32
}

/// A REF CURSOR handle returned in a row or implicit result set.
pub type Cursor = CursorValue;

/// Resolve an owned [`Row`] column by index or by case-insensitive column name.
///
/// This trait is sealed; supported indexes are `usize` and `&str`.
pub trait ColumnIndex: column_index_private::Sealed {
    #[doc(hidden)]
    fn resolve(self, columns: &[ColumnMetadata]) -> std::result::Result<usize, ConversionError>;
}

mod column_index_private {
    pub trait Sealed {}
}

impl column_index_private::Sealed for usize {}

impl ColumnIndex for usize {
    fn resolve(self, columns: &[ColumnMetadata]) -> std::result::Result<usize, ConversionError> {
        if self < columns.len() {
            Ok(self)
        } else {
            Err(ConversionError::OutOfRange {
                expected: "column index",
                detail: format!("no column at index {self}"),
            })
        }
    }
}

impl column_index_private::Sealed for &str {}

impl ColumnIndex for &str {
    fn resolve(self, columns: &[ColumnMetadata]) -> std::result::Result<usize, ConversionError> {
        columns
            .iter()
            .position(|col| col.name().eq_ignore_ascii_case(self))
            .ok_or_else(|| ConversionError::OutOfRange {
                expected: "column name",
                detail: format!("no column named {self:?}"),
            })
    }
}

/// Structured classification of an [`Error`].
///
/// These accessors promote the driver's *internal* retry knowledge (which ORA
/// codes mean "the session died", "the resource was busy", "the cached plan is
/// stale") into a public, matchable taxonomy. python-oracledb gives you a bare
/// `.code` integer and leaves the classification to you; here the curated lists
/// ship with the driver so production retry / circuit-breaker code is trivial:
///
/// ```no_run
/// # use oracledb::Error;
/// # fn classify(err: &Error) {
/// if err.is_connection_lost() {
///     // reconnect, then retry
/// } else if err.is_retryable() {
///     // back off and retry on the same connection
/// }
/// # }
/// ```
///
/// The classification sources, in priority order:
///
/// 1. The structured server error code (`ServerErrorInfo.code`) when present.
/// 2. Otherwise the `ORA-NNNNN` prefix parsed from the error message.
///
/// The curated transient and connection-lost code sets are maintained in this
/// module and exposed through the stable methods below.
impl Error {
    /// Stable top-level error category.
    pub fn kind(&self) -> ErrorKind {
        match self {
            Error::Protocol(err) => protocol_error_kind(err),
            Error::Io(_)
            | Error::ListenerRefused(_)
            | Error::AllAddressesFailed(_)
            | Error::ConnectionClosed(_)
            | Error::Tls(_)
            | Error::UnsupportedSni(_)
            | Error::Wallet(_) => ErrorKind::Network,
            Error::CallTimeout(_) => ErrorKind::Timeout,
            Error::Cancelled => ErrorKind::Cancel,
            Error::Conversion(_) | Error::Bind(_) => ErrorKind::Conversion,
            #[cfg(feature = "arrow")]
            Error::ArrowConversion(_) => ErrorKind::Conversion,
            Error::AccessTokenRequiresTcps
            | Error::UnsupportedAuthMode(_)
            | Error::TokenSource(_) => ErrorKind::Authentication,
            Error::RedirectUnsupported
            | Error::InvalidRedirectData(_)
            | Error::ConnectRedirectLoop(_)
            | Error::Runtime(_)
            | Error::FastAuthRequired
            | Error::UnexpectedPacket(_)
            | Error::ConnectResendLoop(_)
            | Error::MissingSessionField(_)
            | Error::SessionlessTransaction(_)
            | Error::UnknownTransactionState(_) => ErrorKind::Protocol,
            Error::NoRows | Error::TooManyRows => ErrorKind::Database,
        }
    }

    /// Structured resource-limit details when this error came from
    /// [`oracledb_protocol::wire::ProtocolLimits`].
    pub fn resource_limit(&self) -> Option<oracledb_protocol::ResourceLimit> {
        match self {
            Error::Protocol(err) => err.resource_limit(),
            _ => None,
        }
    }

    /// The Oracle error number (`ORA-NNNNN`) this error carries, if any.
    ///
    /// Returns the structured `ServerErrorInfo.code` when the error came back
    /// on the structured path; otherwise it parses the `ORA-` prefix out of the
    /// server message. `None` for non-server errors (I/O, timeouts, protocol
    /// decode failures) and server messages with no `ORA-` code.
    ///
    /// The value is an `i32` (rather than `u32`) so it composes directly with
    /// the `i32`-typed codes most retry tables and logging layers already use;
    /// every real Oracle code fits comfortably.
    pub fn ora_code(&self) -> Option<i32> {
        match self {
            Error::Protocol(err) => protocol_error_ora_code(err).map(|code| code as i32),
            // A user cancel is the client-side shape of the server's ORA-01013
            // "user requested cancel of current operation".
            Error::Cancelled => Some(1013),
            _ => None,
        }
    }

    /// Alias for [`Self::ora_code`] using the API-design terminology.
    pub fn oracle_code(&self) -> Option<i32> {
        self.ora_code()
    }

    /// The server-reported parse offset / error position for this error, if the
    /// server provided one (1-based character offset into the SQL text for a
    /// parse error). Only the structured server-error path retains the offset;
    /// `None` everywhere else, and `None` when the server reported offset 0.
    pub fn offset(&self) -> Option<i32> {
        match self {
            Error::Protocol(err) => protocol_error_offset(err),
            _ => None,
        }
    }

    /// Render a compiler-style caret diagnostic for a parse error, pointing at
    /// the exact character in `sql` the server flagged ([`Error::offset`]):
    ///
    /// ```text
    /// ORA-00942: table or view does not exist
    ///   |
    /// 1 | select * from no_such_table
    ///   |               ^
    /// ```
    ///
    /// Returns `None` when the error carries no parse offset (only structured
    /// server parse errors do). The headline is this error's first `Display`
    /// line (the `ORA-` code + message). python-oracledb hands you a bare offset
    /// integer and leaves the rendering to you; this does it.
    pub fn caret(&self, sql: &str) -> Option<String> {
        let offset = usize::try_from(self.offset()?).ok()?;
        let full = self.to_string();
        let headline = full.lines().next().unwrap_or(full.as_str());
        Some(render_caret(sql, offset, headline))
    }

    /// Whether the connection that surfaced this error is still reusable. A
    /// dead disposition means the session was killed, the socket was reset, or
    /// the server reported one of the session-dead Oracle codes; callers should
    /// discard the connection before continuing.
    ///
    /// Raw I/O errors ([`Error::Io`]) and the recovery-failure
    /// [`Error::ConnectionClosed`] also count as connection-lost: the transport
    /// is no longer usable.
    ///
    /// A plain [`Error::CallTimeout`] is deliberately **not** connection-lost.
    /// On a call timeout the driver sends a BREAK, then drains the server's
    /// in-flight response and the RESET handshake, leaving the wire stream
    /// clean and the connection reusable — exactly as python-oracledb does for
    /// `DPY-4024` (`ERR_CALL_TIMEOUT_EXCEEDED`), which, unlike `DPY-4011`
    /// (`ERR_CONNECTION_CLOSED`), does **not** set `is_session_dead`
    /// (errors.py:124-125). The connection survives; retry on the same one. Only
    /// when that drain itself fails (a *second* timeout) does the driver give up
    /// and surface [`Error::ConnectionClosed`], which *is* connection-lost.
    pub fn connection_disposition(&self) -> ConnectionDisposition {
        match self {
            Error::Io(_) | Error::ConnectionClosed(_) => ConnectionDisposition::Dead,
            _ if self
                .ora_code()
                .is_some_and(|code| SESSION_DEAD_ORA_CODES.contains(&(code as u32))) =>
            {
                ConnectionDisposition::Dead
            }
            _ => ConnectionDisposition::Reusable,
        }
    }

    pub fn is_connection_lost(&self) -> bool {
        match self {
            Error::Io(_) | Error::ConnectionClosed(_) => true,
            _ => self
                .ora_code()
                .is_some_and(|code| CONNECTION_LOST_ORA_CODES.contains(&(code as u32))),
        }
    }

    /// Whether this error is *transient*: the operation failed for a reason
    /// expected to clear on its own (lock contention, deadlock victim, listener
    /// hand-off congestion, resource-manager throttle, or a call timeout), so
    /// the same call may be retried on the **same** connection after a short
    /// back-off. Does **not** include connection-lost codes (those need a
    /// reconnect first — use [`Self::is_connection_lost`]).
    ///
    /// [`Error::CallTimeout`] is transient: after the driver drains the wire the
    /// connection is clean and reusable, so re-running the (idempotent) call on
    /// the same connection — e.g. with a longer timeout — is the natural retry.
    /// [`Error::Cancelled`] is transient for the same reason: an explicit cancel
    /// also drains the wire and leaves the session alive.
    pub fn is_transient(&self) -> bool {
        matches!(self, Error::CallTimeout(_) | Error::Cancelled)
            || self
                .ora_code()
                .is_some_and(|code| TRANSIENT_ORA_CODES.contains(&(code as u32)))
    }

    /// Conservative retry guidance. Any returned retry action still requires
    /// the caller to know the operation is idempotent; non-idempotent operations
    /// must not be replayed automatically.
    pub fn retry_hint(&self) -> RetryHint {
        if self.is_transient() {
            RetryHint::RetrySameConnectionIfIdempotent
        } else if self.is_connection_lost() {
            RetryHint::ReconnectThenRetryIfIdempotent
        } else {
            RetryHint::Never
        }
    }

    pub fn is_retryable(&self) -> bool {
        !matches!(self.retry_hint(), RetryHint::Never)
    }
}

/// Client-API misuse of the sessionless transaction API, mirroring the
/// reference `ERR_SESSIONLESS_*` errors (impl/oracledb/errors.py:338-340).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
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

/// A database access token used in place of a password — an OCI IAM database
/// token or an OAuth2 token. Its [`Debug`] output is redacted so the secret
/// never leaks into logs, error messages, or panic output. Set it with
/// [`ConnectOptions::with_access_token`].
#[derive(Clone)]
pub struct AccessToken(String);

const REDACTED_SECRET: &str = "***redacted***";

impl AccessToken {
    /// Wrap a token string. The value is never printed; see the type docs.
    pub fn new(token: impl Into<String>) -> Self {
        Self(token.into())
    }

    /// The raw token, for sending on the wire. Crate-internal so callers cannot
    /// accidentally route the secret through `Display`/formatting.
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for AccessToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never render the token, regardless of formatter flags.
        f.write_str("AccessToken(")?;
        f.write_str(REDACTED_SECRET)?;
        f.write_str(")")
    }
}

/// A boxed, `Send` future — the return type of [`TokenSource::get_token`].
///
/// Defined locally because the driver takes no dependency on the `futures`
/// crate; it is the conventional `Pin<Box<dyn Future + Send>>` shape.
pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// Failure classes a [`TokenSource`] may report.
///
/// Every variant is **fully redacted**: neither [`Debug`] nor [`std::fmt::Display`]
/// reveals any inner detail. The variants carry no payload by construction, so a
/// token, a signed assertion, or a raw provider response can never leak through
/// a token-source failure into logs, error chains, or panic output. A provider
/// maps its underlying error into one of these classes and drops the detail —
/// fail-closed.
#[derive(Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
pub enum TokenSourceError {
    /// The provider (command / process / HTTP call) failed to execute or exited
    /// unsuccessfully.
    Exec,
    /// The provider ran but returned something that is not a usable token.
    Invalid,
    /// The provider did not produce a token within its own deadline.
    Timeout,
    /// Any other provider failure.
    Other,
}

impl TokenSourceError {
    /// A stable, non-secret label for this failure class.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Exec => "exec",
            Self::Invalid => "invalid",
            Self::Timeout => "timeout",
            Self::Other => "other",
        }
    }
}

impl std::fmt::Display for TokenSourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = match self {
            Self::Exec => "token source failed to execute",
            Self::Invalid => "token source returned an invalid token",
            Self::Timeout => "token source timed out",
            Self::Other => "token source failed",
        };
        f.write_str(msg)
    }
}

impl std::fmt::Debug for TokenSourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Only the class name — there is no payload and nothing to leak.
        write!(f, "TokenSourceError::{}", {
            match self {
                Self::Exec => "Exec",
                Self::Invalid => "Invalid",
                Self::Timeout => "Timeout",
                Self::Other => "Other",
            }
        })
    }
}

impl std::error::Error for TokenSourceError {}

/// A pluggable source of OCI IAM / OAuth2 database access tokens.
///
/// Implement this to obtain a fresh token at connect time (for example by
/// shelling out to the OCI CLI, calling an instance-principal endpoint, or
/// reading a short-lived token file). The driver calls [`get_token`] **once at
/// connect**, and again **only if** the initial token is rejected during
/// authentication (token refresh on expiry). The returned token is placed in
/// `AUTH_TOKEN` and therefore requires a TCPS transport; a token source on a
/// plaintext descriptor is refused with [`Error::AccessTokenRequiresTcps`]
/// *before* the source is ever consulted, so a token is never fetched for a
/// connection that could not carry it securely.
///
/// The token itself must never be logged; report failures as the redacted
/// [`TokenSourceError`].
///
/// [`get_token`]: TokenSource::get_token
pub trait TokenSource: Send + Sync {
    /// Fetch a fresh database access token, or a redacted failure class.
    fn get_token(&self) -> BoxFuture<'_, std::result::Result<String, TokenSourceError>>;
}

/// Stable classifier for the thin driver's authentication modes.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum AuthModeKind {
    Password,
    Proxy,
    External,
    IamToken,
    Kerberos,
    Radius,
}

impl std::fmt::Display for AuthModeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Password => "password",
            Self::Proxy => "proxy",
            Self::External => "external",
            Self::IamToken => "iam-token",
            Self::Kerberos => "kerberos",
            Self::Radius => "radius",
        };
        f.write_str(name)
    }
}

/// Whether a known authentication mode is implemented by this thin driver.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum AuthModeSupport {
    Supported,
    UnsupportedInThin,
}

/// Queryable authentication capability metadata for this build.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct AuthCapabilities {
    pub password: AuthModeSupport,
    pub proxy: AuthModeSupport,
    pub external: AuthModeSupport,
    pub iam_token: AuthModeSupport,
    pub kerberos: AuthModeSupport,
    pub radius: AuthModeSupport,
}

impl AuthCapabilities {
    /// Capabilities of the current pure-thin implementation.
    pub const THIN: Self = Self {
        password: AuthModeSupport::Supported,
        proxy: AuthModeSupport::Supported,
        external: AuthModeSupport::UnsupportedInThin,
        iam_token: AuthModeSupport::Supported,
        kerberos: AuthModeSupport::UnsupportedInThin,
        radius: AuthModeSupport::UnsupportedInThin,
    };

    #[must_use]
    pub fn support(self, mode: AuthModeKind) -> AuthModeSupport {
        match mode {
            AuthModeKind::Password => self.password,
            AuthModeKind::Proxy => self.proxy,
            AuthModeKind::External => self.external,
            AuthModeKind::IamToken => self.iam_token,
            AuthModeKind::Kerberos => self.kerberos,
            AuthModeKind::Radius => self.radius,
        }
    }
}

/// A known authentication mode selected by the caller.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum AuthMode {
    Password,
    Proxy,
    External,
    IamToken,
    Kerberos {
        principal: Option<String>,
        keytab: Option<String>,
    },
    Radius {
        challenge: Option<String>,
    },
}

impl AuthMode {
    #[must_use]
    pub fn kind(&self) -> AuthModeKind {
        match self {
            Self::Password => AuthModeKind::Password,
            Self::Proxy => AuthModeKind::Proxy,
            Self::External => AuthModeKind::External,
            Self::IamToken => AuthModeKind::IamToken,
            Self::Kerberos { .. } => AuthModeKind::Kerberos,
            Self::Radius { .. } => AuthModeKind::Radius,
        }
    }

    fn unsupported_in_thin(&self) -> Option<UnsupportedAuthMode> {
        let mode = self.kind();
        (AuthCapabilities::THIN.support(mode) == AuthModeSupport::UnsupportedInThin)
            .then_some(UnsupportedAuthMode { mode })
    }
}

impl std::fmt::Debug for AuthMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn redacted(value: &Option<String>) -> Option<&'static str> {
            value.as_ref().map(|_| REDACTED_SECRET)
        }

        match self {
            Self::Password => f.write_str("Password"),
            Self::Proxy => f.write_str("Proxy"),
            Self::External => f.write_str("External"),
            Self::IamToken => f.write_str("IamToken"),
            Self::Kerberos { principal, keytab } => f
                .debug_struct("Kerberos")
                .field("principal", &redacted(principal))
                .field("keytab", &redacted(keytab))
                .finish(),
            Self::Radius { challenge } => f
                .debug_struct("Radius")
                .field("challenge", &redacted(challenge))
                .finish(),
        }
    }
}

/// Structured unsupported-authentication diagnostic.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("authentication mode {mode} is not supported by this thin build")]
pub struct UnsupportedAuthMode {
    mode: AuthModeKind,
}

impl UnsupportedAuthMode {
    #[must_use]
    pub fn mode(&self) -> AuthModeKind {
        self.mode
    }
}

/// Everything needed to open a connection: where to connect, who to
/// authenticate as, and the [`ClientIdentity`] the database will record.
///
/// Build the required fields with [`ConnectOptions::new`], then layer optional
/// settings with the `with_*` methods.
#[derive(Clone)]
pub struct ConnectOptions {
    /// EasyConnect descriptor, `host:port/service_name` (the port and service
    /// may be omitted to take the listener defaults).
    connect_string: String,
    /// Database user to authenticate as.
    user: String,
    /// Password for `user`.
    password: String,
    /// Session identity reported to the database (`v$session`).
    identity: ClientIdentity,
    /// Application-context triples `(namespace, key, value)` set on the
    /// session at logon (reference `connection.appcontext`).
    app_context: Vec<(String, String, String)>,
    /// Session Data Unit (negotiated packet size) in bytes.
    sdu: u16,
    /// Proxy user for `[proxy_user]` style connections, if any.
    proxy_user: Option<String>,
    /// Authentication mode selected by the caller. Unsupported thin modes fail
    /// before network I/O with [`Error::UnsupportedAuthMode`].
    auth_mode: AuthMode,
    /// When set, `(SERVER=emon)` is injected into the connect descriptor's
    /// `CONNECT_DATA`. This routes the connection to the database EMON process
    /// used to push CQN notifications (reference `subscr.pyx` rewrites
    /// `description.server_type = "emon"` for the background connection).
    server_type_emon: bool,
    /// TCPS wallet directory (`MY_WALLET_DIRECTORY` / `wallet_location`). The
    /// directory should contain `ewallet.pem`, `ewallet.p12` (requires
    /// `wallet_password`), or `cwallet.sso`. When `None`, `TNS_ADMIN` is
    /// consulted; the special value `SYSTEM` (case-insensitive) forces the
    /// system trust store. Only consulted for TCPS connections.
    wallet_location: Option<String>,
    /// Password for an encrypted wallet (`ewallet.p12`, or an `ewallet.pem`
    /// with an encrypted private key). `None` for auto-login or verify-only
    /// wallets.
    wallet_password: Option<String>,
    /// Oracle edition for Edition-Based Redefinition (`AUTH_ORA_EDITION`),
    /// applied during authentication before any user SQL. `None` uses the
    /// database default edition.
    edition: Option<String>,
    /// Run the Oracle server-DN match after the TLS handshake
    /// (`ssl_server_dn_match`, reference default `true`).
    ssl_server_dn_match: bool,
    /// Explicit expected server-certificate distinguished name
    /// (`ssl_server_cert_dn`). When set, the server's subject DN must equal
    /// this exactly; when `None`, the host name is matched against the
    /// certificate's SAN DNS names and common names.
    ssl_server_cert_dn: Option<String>,
    /// Send the Oracle TCPS SNI string (`use_sni`, reference default `false`).
    /// See [`tls::TlsParams::use_sni`] for the rustls-name-validity caveat.
    use_sni: bool,
    /// Authenticate with a database access token (OCI IAM / OAuth2) instead of
    /// `password`. When set, the token is sent as `AUTH_TOKEN` and no password
    /// verifier is exchanged. Token auth requires a TLS/TCPS transport; see
    /// [`ConnectOptions::with_access_token`]. The token is redacted from `Debug`.
    access_token: Option<AccessToken>,
    /// Pluggable source of database access tokens (OCI IAM / OAuth2). When set
    /// and no static [`access_token`](Self::access_token) is present, the driver
    /// calls it once at connect to obtain the token (and again only on an auth
    /// rejection). Like a static token it requires TCPS; a token source on a
    /// plaintext descriptor is refused before it is ever consulted. Set with
    /// [`ConnectOptions::with_token_source`].
    token_source: Option<std::sync::Arc<dyn TokenSource>>,
    /// Maximum number of open statements kept in this connection's statement
    /// cache. Defaults to 20 (the reference default). `0` disables caching
    /// entirely (every statement's cursor is closed after use, never retained),
    /// matching python-oracledb's `stmtcachesize=0`. The cache holds at most this
    /// many entries, each a small `(sql, cursor_id)` pair, so it is bounded by
    /// construction. Set with [`ConnectOptions::with_statement_cache_size`].
    statement_cache_size: usize,
    /// Resource policy for thin-protocol decoding and packet reassembly.
    protocol_limits: ProtocolLimits,
    /// Optional read-inactivity deadline applied to every post-auth wire read
    /// (GH#14). When `Some(d)`, a single read operation that makes no progress
    /// within `d` fails with [`Error::CallTimeout`] rather than hanging forever
    /// on a silent or half-open server. `None` (the default) preserves the prior
    /// unbounded-read behaviour; operators opt in per connection with
    /// [`ConnectOptions::with_inactivity_timeout`]. The CONNECT/ACCEPT phase is
    /// bounded separately by the DSN transport-connect timeout, and TCP keepalive
    /// is derived from a DSN `EXPIRE_TIME`.
    inactivity_timeout: Option<Duration>,
    /// Optional cross-connection statement-shape cache (bead a4-8pp). When set,
    /// every connection built from these options shares this cache, so a query's
    /// described result-column shape is tracked across connections and a
    /// concurrent DDL that changes the shape triggers a self-heal (re-describe)
    /// on the next execute instead of a stale decode. `None` (default) gives
    /// each connection a private cache, preserving the prior per-connection
    /// behaviour. Set with
    /// [`ConnectOptions::with_shared_statement_shape_cache`].
    statement_shape_cache: Option<Arc<StatementShapeCache>>,
}

impl std::fmt::Debug for ConnectOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let wallet_location = self.wallet_location.as_ref().map(|_| REDACTED_SECRET);
        let wallet_password = self.wallet_password.as_ref().map(|_| REDACTED_SECRET);
        let server_cert_dn = self.ssl_server_cert_dn.as_ref().map(|_| REDACTED_SECRET);
        f.debug_struct("ConnectOptions")
            .field("connect_string", &self.connect_string)
            .field("user", &self.user)
            .field("password", &REDACTED_SECRET)
            .field("identity", &self.identity)
            .field("app_context", &self.app_context)
            .field("sdu", &self.sdu)
            .field("proxy_user", &self.proxy_user)
            .field("auth_mode", &self.auth_mode)
            .field("server_type_emon", &self.server_type_emon)
            .field("wallet_location", &wallet_location)
            .field("wallet_password", &wallet_password)
            .field("edition", &self.edition)
            .field("ssl_server_dn_match", &self.ssl_server_dn_match)
            .field("ssl_server_cert_dn", &server_cert_dn)
            .field("use_sni", &self.use_sni)
            .field("access_token", &self.access_token)
            .field(
                "token_source",
                &self.token_source.as_ref().map(|_| "<token source>"),
            )
            .field("statement_cache_size", &self.statement_cache_size)
            .field("protocol_limits", &self.protocol_limits)
            .field("inactivity_timeout", &self.inactivity_timeout)
            .field(
                "statement_shape_cache",
                &self
                    .statement_shape_cache
                    .as_ref()
                    .map(|_| "<shared statement-shape cache>"),
            )
            .finish()
    }
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
            auth_mode: AuthMode::Password,
            server_type_emon: false,
            wallet_location: None,
            wallet_password: None,
            ssl_server_dn_match: true,
            ssl_server_cert_dn: None,
            use_sni: false,
            edition: None,
            access_token: None,
            token_source: None,
            statement_cache_size: STATEMENT_CACHE_SIZE,
            protocol_limits: ProtocolLimits::DEFAULT,
            inactivity_timeout: None,
            statement_shape_cache: None,
        }
    }

    /// Create connect options that express passwordless external authentication
    /// intent without caller-supplied dummy credentials. This thin build
    /// currently reports the mode as [`Error::UnsupportedAuthMode`] before any
    /// network I/O; use [`Self::auth_capabilities`] to inspect support.
    pub fn external_auth(connect_string: impl Into<String>, identity: ClientIdentity) -> Self {
        let mut options = Self::new(connect_string, "", "", identity);
        options.auth_mode = AuthMode::External;
        options
    }

    /// Create connect options for Kerberos intent. Real Kerberos/GSSAPI
    /// exchange is not implemented in this thin build; principal and keytab are
    /// carried only for structured diagnostics and are redacted from `Debug`.
    pub fn kerberos_auth(
        connect_string: impl Into<String>,
        principal: impl Into<String>,
        keytab: impl Into<String>,
        identity: ClientIdentity,
    ) -> Self {
        let mut options = Self::new(connect_string, "", "", identity);
        options.auth_mode = AuthMode::Kerberos {
            principal: Some(principal.into()),
            keytab: Some(keytab.into()),
        };
        options
    }

    /// Create connect options for RADIUS/native-MFA intent. Real challenge
    /// exchange is not implemented in this thin build; the challenge hint is
    /// carried only for structured diagnostics and is redacted from `Debug`.
    pub fn radius_auth(
        connect_string: impl Into<String>,
        challenge: impl Into<String>,
        identity: ClientIdentity,
    ) -> Self {
        let mut options = Self::new(connect_string, "", "", identity);
        options.auth_mode = AuthMode::Radius {
            challenge: Some(challenge.into()),
        };
        options
    }

    /// Set the thin-protocol resource limits. Invalid policies are rejected at
    /// connect time before any network I/O.
    #[must_use]
    pub fn with_protocol_limits(mut self, limits: ProtocolLimits) -> Self {
        self.protocol_limits = limits;
        self
    }

    /// Set a read-inactivity deadline for every post-auth wire read on this
    /// connection (GH#14). A read operation that makes no progress within
    /// `timeout` fails with [`Error::CallTimeout`] rather than hanging forever on
    /// a silent or half-open peer (a half-open connection whose FIN/RST was lost
    /// otherwise wedges the read indefinitely). Unset by default, which keeps the
    /// prior unbounded-read behaviour. The CONNECT/ACCEPT phase is bounded by the
    /// DSN transport-connect timeout; this governs only established-session reads.
    #[must_use]
    pub fn with_inactivity_timeout(mut self, timeout: Duration) -> Self {
        self.inactivity_timeout = Some(timeout);
        self
    }

    /// Set the statement-cache capacity for this connection (number of open
    /// statements retained for reuse). `0` disables caching — every statement's
    /// cursor is closed after use rather than cached (python-oracledb
    /// `stmtcachesize=0` semantics). The default is 20. The cache is bounded to
    /// this many small entries, so a large value cannot cause unbounded growth
    /// beyond the count of distinct prepared statements.
    #[must_use]
    pub fn with_statement_cache_size(mut self, size: usize) -> Self {
        self.statement_cache_size = size;
        self
    }

    /// Authenticate with a database access token instead of a password — an OCI
    /// IAM database token or an OAuth2 token (python-oracledb's `access_token`).
    /// The token is sent as `AUTH_TOKEN` with no password-verifier exchange.
    ///
    /// Token authentication **requires** a TLS/TCPS connection (the token would
    /// otherwise travel in clear text); connecting with a token over plain TCP
    /// fails with the typed [`Error::AccessTokenRequiresTcps`]. The token is
    /// wrapped in [`AccessToken`], whose `Debug` is redacted, so it never appears
    /// in logs or error output.
    #[must_use]
    pub fn with_access_token(mut self, token: impl Into<String>) -> Self {
        self.access_token = Some(AccessToken::new(token));
        self.auth_mode = AuthMode::IamToken;
        self
    }

    /// Authenticate with a database access token obtained from a pluggable
    /// [`TokenSource`] (OCI IAM / OAuth2). The driver calls the source once at
    /// connect to fetch the token — and again only if the token is rejected at
    /// authentication (refresh on expiry). Like [`Self::with_access_token`] this
    /// selects token auth and therefore **requires** a TLS/TCPS connection: a
    /// token source on a plaintext descriptor fails with the typed
    /// [`Error::AccessTokenRequiresTcps`] *before* the source is consulted, so a
    /// token is never fetched for a transport that could not carry it securely.
    ///
    /// A static [`Self::with_access_token`] takes precedence if both are set.
    #[must_use]
    pub fn with_token_source(mut self, source: std::sync::Arc<dyn TokenSource>) -> Self {
        self.token_source = Some(source);
        self.auth_mode = AuthMode::IamToken;
        self
    }

    /// The configured [`TokenSource`], if any.
    pub fn token_source(&self) -> Option<&std::sync::Arc<dyn TokenSource>> {
        self.token_source.as_ref()
    }

    /// Share a [`StatementShapeCache`] across every connection built from these
    /// options (bead a4-8pp). With a shared cache, a query's described
    /// result-column shape is tracked cross-connection: if a concurrent DDL on
    /// one connection changes the shape, the next execute of the same statement
    /// on any sharing connection self-heals (re-describes) rather than decoding
    /// against the stale shape. Without this, each connection keeps a private
    /// cache and behaves exactly as before.
    #[must_use]
    pub fn with_shared_statement_shape_cache(mut self, cache: Arc<StatementShapeCache>) -> Self {
        self.statement_shape_cache = Some(cache);
        self
    }

    /// The shared [`StatementShapeCache`], if one was configured.
    pub fn statement_shape_cache(&self) -> Option<&Arc<StatementShapeCache>> {
        self.statement_shape_cache.as_ref()
    }

    /// Select passwordless external authentication intent on an existing
    /// options value. This is useful for code that starts from a shared
    /// `ConnectOptions::new` builder path; [`Self::external_auth`] avoids
    /// requiring credentials in the first place.
    #[must_use]
    pub fn with_external_auth(mut self) -> Self {
        self.auth_mode = AuthMode::External;
        self.user.clear();
        self.password.clear();
        self
    }

    /// Select Kerberos authentication intent. Principal and keytab values are
    /// redacted from every `Debug` representation and are not sent on the wire
    /// because this thin build returns [`Error::UnsupportedAuthMode`] for the
    /// mode before network I/O.
    #[must_use]
    pub fn with_kerberos_auth(
        mut self,
        principal: impl Into<String>,
        keytab: impl Into<String>,
    ) -> Self {
        self.auth_mode = AuthMode::Kerberos {
            principal: Some(principal.into()),
            keytab: Some(keytab.into()),
        };
        self.user.clear();
        self.password.clear();
        self
    }

    /// Select RADIUS/native-MFA authentication intent. The challenge hint is
    /// redacted from `Debug` and is not sent on the wire because this thin
    /// build returns [`Error::UnsupportedAuthMode`] for the mode before network
    /// I/O.
    #[must_use]
    pub fn with_radius_auth(mut self, challenge: impl Into<String>) -> Self {
        self.auth_mode = AuthMode::Radius {
            challenge: Some(challenge.into()),
        };
        self.user.clear();
        self.password.clear();
        self
    }

    /// Select the Oracle edition for this session (Edition-Based Redefinition).
    /// Applied via `AUTH_ORA_EDITION` during authentication, *before* any user
    /// SQL — deterministic even for pooled/reused sessions. Verify with
    /// `SYS_CONTEXT('USERENV','CURRENT_EDITION_NAME')`. An invalid/unauthorized
    /// edition surfaces as a typed server error at connect time.
    #[must_use]
    pub fn with_edition(mut self, edition: impl Into<String>) -> Self {
        self.edition = Some(edition.into());
        self
    }

    /// Enable sending the Oracle TCPS SNI string (`use_sni`, default off).
    #[must_use]
    pub fn with_use_sni(mut self, use_sni: bool) -> Self {
        self.use_sni = use_sni;
        self
    }

    /// Set the TCPS wallet directory (`wallet_location` /
    /// `MY_WALLET_DIRECTORY`). Only used for TCPS connections.
    #[must_use]
    pub fn with_wallet_location(mut self, location: impl Into<String>) -> Self {
        self.wallet_location = Some(location.into());
        self
    }

    /// Set the wallet password — required for `ewallet.p12` wallets and for an
    /// `ewallet.pem` whose private key is encrypted (PKCS#8 `ENCRYPTED PRIVATE
    /// KEY`). Not needed for auto-login (`cwallet.sso`) or verify-only wallets.
    #[must_use]
    pub fn with_wallet_password(mut self, password: impl Into<String>) -> Self {
        self.wallet_password = Some(password.into());
        self
    }

    /// Enable or disable the Oracle server-DN match (`ssl_server_dn_match`,
    /// default enabled).
    #[must_use]
    pub fn with_ssl_server_dn_match(mut self, enabled: bool) -> Self {
        self.ssl_server_dn_match = enabled;
        self
    }

    /// Set the explicit expected server-certificate DN
    /// (`ssl_server_cert_dn`).
    #[must_use]
    pub fn with_ssl_server_cert_dn(mut self, dn: impl Into<String>) -> Self {
        self.ssl_server_cert_dn = Some(dn.into());
        self
    }

    /// Route this connection to the database EMON process by injecting
    /// `(SERVER=emon)` into the connect descriptor (used by the CQN background
    /// notification connection).
    pub fn with_server_type_emon(mut self, emon: bool) -> Self {
        self.server_type_emon = emon;
        self
    }

    /// Set the application-context triples applied at logon.
    pub fn with_app_context(mut self, app_context: Vec<(String, String, String)>) -> Self {
        self.app_context = app_context;
        self
    }

    /// Set the proxy user for `[proxy_user]` style authentication.
    pub fn with_proxy_user(mut self, proxy_user: Option<String>) -> Self {
        if proxy_user.is_some() {
            self.auth_mode = AuthMode::Proxy;
        } else if matches!(self.auth_mode, AuthMode::Proxy) {
            self.auth_mode = AuthMode::Password;
        }
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

    pub fn connect_string(&self) -> &str {
        &self.connect_string
    }

    pub fn user(&self) -> &str {
        &self.user
    }

    pub fn password(&self) -> &str {
        &self.password
    }

    pub fn identity(&self) -> &ClientIdentity {
        &self.identity
    }

    pub fn app_context(&self) -> &[(String, String, String)] {
        &self.app_context
    }

    pub fn sdu(&self) -> u16 {
        self.sdu
    }

    pub fn proxy_user(&self) -> Option<&str> {
        self.proxy_user.as_deref()
    }

    pub fn auth_mode(&self) -> &AuthMode {
        &self.auth_mode
    }

    pub fn auth_capabilities(&self) -> AuthCapabilities {
        AuthCapabilities::THIN
    }

    pub fn server_type_emon(&self) -> bool {
        self.server_type_emon
    }

    pub fn wallet_location(&self) -> Option<&str> {
        self.wallet_location.as_deref()
    }

    pub fn wallet_password(&self) -> Option<&str> {
        self.wallet_password.as_deref()
    }

    pub fn edition(&self) -> Option<&str> {
        self.edition.as_deref()
    }

    pub fn ssl_server_dn_match(&self) -> bool {
        self.ssl_server_dn_match
    }

    pub fn ssl_server_cert_dn(&self) -> Option<&str> {
        self.ssl_server_cert_dn.as_deref()
    }

    pub fn use_sni(&self) -> bool {
        self.use_sni
    }

    pub fn access_token(&self) -> Option<&AccessToken> {
        self.access_token.as_ref()
    }

    pub fn statement_cache_size(&self) -> usize {
        self.statement_cache_size
    }

    pub fn protocol_limits(&self) -> ProtocolLimits {
        self.protocol_limits
    }

    pub fn inactivity_timeout(&self) -> Option<Duration> {
        self.inactivity_timeout
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
    /// The private transport core owns packet I/O and cancellation-drain state.
    /// Cancellable reads arm its drain flag so the next operation can break and
    /// drain before issuing a new request.
    core: DriverCore,
    protocol_limits: ProtocolLimits,
    session_id: u32,
    serial_num: u16,
    server_version: Option<String>,
    server_version_tuple: Option<(u8, u8, u8, u8, u8)>,
    /// `SYS_CONTEXT('USERENV','DB_UNIQUE_NAME')` from the AUTH phase-two session
    /// data key `AUTH_SC_REAL_DBUNIQUE_NAME` (reference `db_unique_name`,
    /// upstream 16a57f1cbd58). `None` when the server did not send the key.
    db_unique_name: Option<String>,
    capabilities: ClientCapabilities,
    ttc_seq_num: u8,
    sdu: usize,
    /// Negotiated TTC protocol version from the server's ACCEPT
    /// (`AcceptInfo.protocol_version`). Surfaced via
    /// [`Connection::protocol_version`].
    protocol_version: u16,
    /// Whether authentication used the combined fast-auth bundle: the server
    /// advertised `TNS_ACCEPT_FLAG_FAST_AUTH` (`AcceptInfo.supports_fast_auth`)
    /// and the driver took the single-round-trip auth path. Surfaced via
    /// [`Connection::supports_fast_auth`].
    supports_fast_auth: bool,
    supports_end_of_response: bool,
    /// Whether the server negotiated out-of-band (urgent-TCP) break support
    /// (`protocol_options & TNS_GSO_CAN_RECV_ATTENTION`, reference
    /// `Capabilities.supports_oob`). Surfaced via [`Connection::supports_oob`];
    /// [`Connection::cancel`] always uses the in-band BREAK marker regardless,
    /// because the transport does not expose `MSG_OOB`.
    supports_oob: bool,
    cursor_columns: BTreeMap<u32, Vec<ColumnMetadata>>,
    /// Fetch metadata of the most recent execution keyed by SQL text,
    /// mirroring the reference statement cache's per-statement
    /// `_fetch_var_impls` retention (impl/thin/statement.pyx:300-310) that
    /// drives the re-execute type-change adjustment.
    fetch_metadata_by_sql: HashMap<String, Vec<ColumnMetadata>>,
    /// Insertion order for [`Self::fetch_metadata_by_sql`] eviction.
    fetch_metadata_order: VecDeque<String>,
    /// Cross-connection statement-shape cache (bead a4-8pp). Private per
    /// connection unless a shared one was supplied via
    /// [`ConnectOptions::with_shared_statement_shape_cache`]. Each query execute
    /// observes its freshly-described shape here; a cross-connection shape change
    /// (concurrent DDL) self-heals by dropping this connection's retained per-SQL
    /// fetch metadata so it re-describes instead of serving a stale decode.
    shape_cache: Arc<StatementShapeCache>,
    dead: bool,
    /// Logon user, retained for the change-password call.
    user: String,
    /// Session combo key from verifier generation, retained for the
    /// change-password call (reference keeps `conn_impl._combo_key`).
    combo_key: Vec<u8>,
    /// LRU statement cache: SQL text -> open server cursor id plus the bind
    /// TYPE shape the cursor was last bound with (reference
    /// thin/statement_cache.pyx, default size 20). The shape guards against
    /// reusing a cursor whose server-side bind metadata no longer matches the
    /// new binds (bead rust-oracledb-ilel: ORA-01722 when a text bind rides a
    /// cursor parsed with a NUMBER bind).
    statement_cache: Vec<CachedStatement>,
    /// Capacity of [`Self::statement_cache`] (from
    /// [`ConnectOptions::statement_cache_size`]); `0` disables caching.
    statement_cache_size: usize,
    /// Server cursor ids currently held by a live cursor (reference
    /// `Statement._in_use`). A cached cursor whose id is in this set must NOT
    /// be reused by a second cursor: `get_statement` returns a fresh
    /// (re-parsed) cursor instead, so interleaved fetches on different cursors
    /// of the same connection cannot reset each other's server-side fetch
    /// position (ORA-01002 fetch out of sequence). Cleared when the owning
    /// cursor releases the id (close / re-prepare to a different statement).
    in_use_cursors: HashSet<u32>,
    /// Cursor ids whose active server define returns LOB locator rows with the
    /// extra size/chunk fields. Plain `stream_lobs()` cursors are intentionally
    /// absent even when their describe metadata contains CLOB/BLOB columns.
    lob_prefetch_cursors: BTreeSet<u32>,
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
    /// Leftover (partially-decoded) bytes from the EMON notification stream.
    /// The reference `ReadBuffer` chains pushed packets so a single logical
    /// `process()` call decodes records that span packet boundaries; this
    /// buffer plays the same role for [`Connection::recv_notification`].
    notification_buffer: Vec<u8>,
    /// Whether the leading `TNS_MSG_TYPE_OAC` byte of the notification stream
    /// has been consumed (the reference reads it once via the outer
    /// `process()` loop before delivering any record).
    notification_header_consumed: bool,
    /// The TPC (two-phase commit) transaction context returned by `tpc_begin`,
    /// echoed back on `tpc_end`/`tpc_prepare`/`tpc_commit`/`tpc_rollback`
    /// (reference `BaseThinConnImpl._transaction_context`). `None` when no XA
    /// transaction context has been captured.
    transaction_context: Option<Vec<u8>>,
    /// Whether a server-side transaction is in progress, derived from the wire
    /// end-of-call status bit `TNS_EOCS_FLAGS_TXN_IN_PROGRESS` on every round
    /// trip (reference protocol.pyx `_process_call_status` /
    /// `_txn_in_progress`).
    txn_in_progress: bool,
    /// Secret-free support-capture guard (bead K6). Armed only when
    /// `ORACLEDB_CAPTURE` was set at connect time and the `cassette` feature is
    /// compiled in; on drop/close it writes a scrubbed, secret-free
    /// `.tns-cassette` of the whole session to that path (fail-closed: a
    /// surviving secret refuses the write and leaves no file). `None` otherwise,
    /// in which case the transport path is byte-identical to a non-capturing
    /// session.
    ///
    /// Held only for its `Drop` side effect (persist-on-session-end), never read.
    #[cfg(feature = "cassette")]
    #[allow(dead_code)]
    capture_guard: Option<transport::CaptureGuard>,
}

/// Owns the lifecycle of the open query cursor used by
/// [`Connection::for_each_row_ref`]. The borrowed-row path deliberately keeps a
/// speculative response in flight while it runs the user's callback, so every
/// post-execute early return -- including cancellation by dropping the method
/// future -- must retire the cursor locally. Wire recovery remains the
/// transport's job: the next request drains any stranded response before it
/// sends this guard's queued close-cursor piggyback.
struct BorrowedStreamCursorGuard<'conn> {
    connection: &'conn mut Connection,
    cursor_id: u32,
}

impl<'conn> BorrowedStreamCursorGuard<'conn> {
    fn new(connection: &'conn mut Connection, cursor_id: u32) -> Self {
        Self {
            connection,
            cursor_id,
        }
    }

    fn connection(&mut self) -> &mut Connection {
        self.connection
    }

    /// The cursor reached normal end-of-data, so it remains valid for statement
    /// cache reuse. Disarm the fail-closed drop path after normal release.
    fn release(mut self) {
        self.connection.release_cursor(self.cursor_id);
        self.cursor_id = 0;
    }
}

impl Drop for BorrowedStreamCursorGuard<'_> {
    fn drop(&mut self) {
        if self.cursor_id != 0 {
            self.connection.close_cursor(self.cursor_id);
        }
    }
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

/// Result of one bounded notification packet read.
enum PacketRead {
    /// A DATA packet's payload was appended to the notification buffer.
    Appended,
    /// The read timed out (no data within the window); the caller should poll
    /// its shutdown flag and may retry.
    TimedOut,
    /// The emon socket was closed or returned a non-DATA packet; the stream is
    /// finished.
    Closed,
}

/// Outcome of [`Connection::recv_notification`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum NotificationOutcome {
    /// A decoded notification record to deliver to the callback.
    Record(NotificationRecord),
    /// No record arrived within the read window; poll the shutdown flag and
    /// call again to keep waiting.
    TimedOut,
    /// The emon socket closed (teardown / STOP_NOTIF); stop the receive loop.
    Closed,
}

const STATEMENT_CACHE_SIZE: usize = 20;

/// One operation in a pipelined batch (`Connection::run_pipeline`).
#[derive(Clone, Debug)]
pub enum PipelineRequest {
    #[non_exhaustive]
    Execute {
        sql: String,
        bind_rows: Vec<Vec<BindValue>>,
        prefetch_rows: u32,
    },
    Commit,
}

impl PipelineRequest {
    pub fn execute(
        sql: impl Into<String>,
        bind_rows: Vec<Vec<BindValue>>,
        prefetch_rows: u32,
    ) -> Self {
        Self::Execute {
            sql: sql.into(),
            bind_rows,
            prefetch_rows,
        }
    }

    pub fn commit() -> Self {
        Self::Commit
    }

    pub fn sql(&self) -> Option<&str> {
        match self {
            Self::Execute { sql, .. } => Some(sql),
            Self::Commit => None,
        }
    }

    pub fn bind_rows(&self) -> Option<&[Vec<BindValue>]> {
        match self {
            Self::Execute { bind_rows, .. } => Some(bind_rows),
            Self::Commit => None,
        }
    }

    pub fn prefetch_rows(&self) -> Option<u32> {
        match self {
            Self::Execute { prefetch_rows, .. } => Some(*prefetch_rows),
            Self::Commit => None,
        }
    }

    pub fn is_commit(&self) -> bool {
        matches!(self, Self::Commit)
    }
}

#[derive(Debug)]
pub struct CancelHandle {
    write: SharedWriteHalf,
    recovery: Arc<SessionRecovery>,
}

impl Connection {
    /// Open a connection: resolve the EasyConnect descriptor, complete the TNS
    /// handshake and TTC capability negotiation, and authenticate `user` with
    /// the supplied [`ClientIdentity`]. On success the database has recorded a
    /// session whose `program` / `machine` / `osuser` / `terminal` are exactly
    /// the identity fields.
    pub async fn connect(cx: &Cx, mut options: ConnectOptions) -> Result<Self> {
        observe_cancellation_between_round_trips(cx)?;
        let protocol_limits = options.protocol_limits.validate()?;
        if let Some(unsupported) = options.auth_mode.unsupported_in_thin() {
            return Err(Error::UnsupportedAuthMode(unsupported));
        }
        let descriptor = EasyConnect::parse(&options.connect_string)?;
        // Fail closed BEFORE any network I/O: a database access token (OCI IAM /
        // OAuth2) must never be put on the wire in clear text. The reference
        // enforces this during the auth exchange (protocol.pyx
        // `ERR_ACCESS_TOKEN_REQUIRES_TCPS`); we additionally refuse up front, so
        // the token never leaves the process when the descriptor is plaintext
        // TCP. The in-auth guard below stays as defense in depth. Uniform typed
        // error; the token is never rendered. A pluggable token *source* is held
        // to the same rule and — crucially — is refused here BEFORE it is ever
        // consulted, so no token is fetched for a transport that could not carry
        // it securely.
        if (options.access_token.is_some() || options.token_source.is_some())
            && !descriptor.protocol.is_tls()
        {
            return Err(Error::AccessTokenRequiresTcps);
        }
        // Resolve a pluggable token source into a concrete access token once, at
        // connect (the transport is now known to be TCPS). A static access token
        // takes precedence. The provider's failure is surfaced as the redacted
        // `Error::TokenSource`; its detail (and the token) never leak.
        if options.access_token.is_none() {
            if let Some(source) = options.token_source.clone() {
                let token = source.get_token().await.map_err(Error::TokenSource)?;
                options.access_token = Some(AccessToken::new(token));
            }
        }
        let full_descriptor = EasyConnect::parse_descriptor(&options.connect_string)?;
        let primary_description = full_descriptor.first_description().clone();
        let connect_timeout =
            transport_connect_timeout_duration(primary_description.tcp_connect_timeout);
        let connect_timeout_ms = duration_to_millis_saturating(connect_timeout);
        // GH#14: the opt-in per-read inactivity deadline, and the TCP keepalive
        // idle interval derived from a DSN `EXPIRE_TIME` (minutes). Both are Copy
        // values captured into the connect block: keepalive is set on the socket
        // right after dial (before any TLS wrap), and the inactivity deadline is
        // installed on the session core once the transport is established.
        let inactivity_timeout = options.inactivity_timeout;
        let keepalive_idle = keepalive_idle_from_expire_time(primary_description.expire_time);
        let connect_result = time::timeout(time::wall_now(), connect_timeout, async {
            // Connect span (feature-gated, zero-cost when off). Carries only the
            // server address / port / service — never the password.
            let _span = obs_span!(
                "oracledb.connect",
                db.system = "oracle",
                server.address = %descriptor.host,
                server.port = descriptor.port as u64,
                db.name = %descriptor.service_name,
            );
            let token_auth = options.access_token.is_some();
            let descriptor_ssl_server_dn_match =
                primary_description.security.ssl_server_dn_match && options.ssl_server_dn_match;
            let descriptor_ssl_server_cert_dn = options
                .ssl_server_cert_dn
                .as_deref()
                .or(primary_description.security.ssl_server_cert_dn.as_deref());
            let identity = options.identity;
            // F1 (bead rust-oracledb-clvm): apply the DSN-parsed transport
            // parameters that were previously parsed and dropped. The SDU
            // advertised in the CONNECT packet honours a DSN `(SDU=...)` (see
            // `resolve_effective_sdu`); the wallet directory and the SNI toggle
            // fall back to the DSN `SECURITY`/`USE_SNI` when the structured
            // builder did not set them.
            let advertised_sdu =
                u16::try_from(resolve_effective_sdu(options.sdu, &primary_description))
                    .unwrap_or(u16::MAX);
            let effective_use_sni = options.use_sni || primary_description.use_sni;
            let effective_wallet_location = options
                .wallet_location
                .as_deref()
                .or(primary_description.security.wallet_location.as_deref());
            // TCPS: TLS parameters (wallet, DN-match, SNI policy) are resolved
            // once and reused when a listener REDIRECT re-establishes the
            // transport to a new address (the redirected connection keeps the
            // original transport protocol, reference `_connect_phase_one`).
            let server_type = if options.server_type_emon {
                Some("emon")
            } else {
                None
            };
            let tls_params = if descriptor.protocol.is_tls() {
                Some(tls::resolve_tls_params(
                    &descriptor,
                    effective_wallet_location,
                    options.wallet_password.as_deref(),
                    options.ssl_server_dn_match,
                    options.ssl_server_cert_dn.as_deref(),
                    effective_use_sni,
                )?)
            } else {
                None
            };
            // F3 (bead rust-oracledb-clvm): fail closed *before* any transport
            // is dialled if `use_sni=true` was requested but the Oracle SNI
            // cannot be encoded as a rustls DNS name — never a silent no-SNI.
            if let Some(tls_params) = tls_params.as_ref() {
                tls::decide_sni(tls_params.use_sni, &descriptor.service_name, server_type)?;
            }
            let connector = DriverConnector::default();
            // Secret-free support capture (bead K6): when `ORACLEDB_CAPTURE` is
            // set, a recorder is created here and installed around each dial's
            // transport split so the WHOLE session (connect + auth + queries) is
            // teed into it. The recorder handle rides on the returned
            // `Connection`; on drop/close the auth phase is scrubbed and a
            // fail-closed refuse gate persists the cassette to the path (or
            // refuses if any secret survives). Off / feature-absent = no
            // behaviour change.
            #[cfg(feature = "cassette")]
            let capture_path = transport::capture_path_from_env();
            #[cfg(feature = "cassette")]
            let capture_recorder = capture_path
                .as_ref()
                .map(|_| transport::CassetteRecorder::new());
            // Dials one listener endpoint: TCP connect, then — for a TCPS
            // original — the TLS handshake on the whole socket before
            // splitting and before any TNS bytes are sent (implicit TLS,
            // matching python-oracledb thin's _connect_tcp ordering). Used
            // for the initial address and for every REDIRECT target.
            let dial = |host: String, port: u16| {
                let descriptor = &descriptor;
                let connector = &connector;
                let tls_params = tls_params.as_ref();
                #[cfg(feature = "cassette")]
                let capture_recorder = capture_recorder.as_ref();
                async move {
                    trace_connect_step("tcp connect");
                    let stream = TcpStream::connect_timeout((host, port), connect_timeout).await?;
                    stream.set_nodelay(true)?;
                    // GH#14: enable TCP keepalive so a half-open/dead peer is
                    // detected instead of wedging a later read forever. The idle
                    // interval comes from the DSN `EXPIRE_TIME` (minutes); on a
                    // TCPS descriptor it is set on the underlying socket here,
                    // before the TLS handshake consumes the stream.
                    if let Some(idle) = keepalive_idle {
                        stream.set_keepalive(Some(idle))?;
                    }
                    trace_connect_step("tcp connected");
                    let halves = if let Some(tls_params) = tls_params {
                        trace_connect_step("tls handshake");
                        let tls_stream =
                            tls::tls_handshake(descriptor, server_type, tls_params, stream).await?;
                        trace_connect_step("tls established");
                        // Install the capture recorder around the SYNCHRONOUS
                        // split only (no await while held) so the thread-local
                        // is observed on the thread performing the split.
                        #[cfg(feature = "cassette")]
                        let _capture =
                            capture_recorder.map(|r| transport::install_recorder_scope(r.clone()));
                        connector.tls_split(tls_stream)
                    } else {
                        #[cfg(feature = "cassette")]
                        let _capture =
                            capture_recorder.map(|r| transport::install_recorder_scope(r.clone()));
                        connector.plain_split(stream)
                    };
                    Ok::<_, Error>(halves)
                }
            };
            // F2 (bead rust-oracledb-clvm): sequential multi-address failover.
            // A DESCRIPTION with an ADDRESS_LIST (or several ADDRESS entries)
            // is tried in order — honouring LOAD_BALANCE (shuffle) and
            // RETRY_COUNT/RETRY_DELAY — until one address establishes a
            // transport; if every address fails, the per-address reasons are
            // aggregated into `Error::AllAddressesFailed`. Only
            // transport-establishment errors (TCP dial / TLS handshake) fail
            // over to the next address; a configuration error aborts the whole
            // connect. The overall connect deadline (the DSN transport connect
            // timeout) still bounds the total, shared across attempts.
            let candidates = resolve_connect_addresses(&full_descriptor, descriptor.protocol);
            let retry_count = primary_description.retry_count;
            let retry_delay = Duration::from_secs(u64::from(primary_description.retry_delay));
            let mut attempt_errors: Vec<String> = Vec::new();
            let mut connected = None;
            'failover: for round in 0..=retry_count {
                if round > 0 && !retry_delay.is_zero() {
                    trace_connect_step("failover retry delay");
                    // Sleep `retry_delay` by timing out a never-ready future
                    // (asupersync exposes the deadline primitive, not a bare
                    // sleep); the elapsed Err is expected and ignored.
                    let _ =
                        time::timeout(time::wall_now(), retry_delay, std::future::pending::<()>())
                            .await;
                }
                for candidate in &candidates {
                    match dial(candidate.host.clone(), candidate.port).await {
                        Ok(halves) => {
                            connected = Some((halves, candidate.clone()));
                            break 'failover;
                        }
                        Err(err) if is_failover_eligible(&err) => {
                            attempt_errors
                                .push(format!("{}:{} ({err})", candidate.host, candidate.port));
                        }
                        Err(err) => return Err(err),
                    }
                }
            }
            let ((read, write), active_address) = match connected {
                Some(pair) => pair,
                None => {
                    return Err(Error::AllAddressesFailed(format!(
                        "tried {} address(es): {}",
                        attempt_errors.len(),
                        attempt_errors.join("; ")
                    )));
                }
            };
            // Rebind the working descriptor to the address that actually
            // connected so the CONNECT descriptor's ADDRESS clause reflects the
            // live endpoint (the TLS params/DN match keep the configured host).
            let descriptor = EasyConnect {
                host: active_address.host,
                port: active_address.port,
                service_name: descriptor.service_name.clone(),
                protocol: descriptor.protocol,
            };
            let mut core = ConnectionCore::from_halves(read, write, "oracle_tcp_write");
            core.set_protocol_limits(protocol_limits)?;
            core.set_inactivity_timeout(inactivity_timeout);

            let connect_descriptor = listener_connect_descriptor_with_server(
                &descriptor,
                &primary_description,
                &identity,
                options.server_type_emon,
                token_auth,
                descriptor_ssl_server_dn_match,
                descriptor_ssl_server_cert_dn,
            );
            trace_connect_value("CONNECT descriptor", &connect_descriptor);
            // A descriptor longer than TNS_MAX_CONNECT_DATA travels in a DATA
            // packet right behind the CONNECT packet, and the server may answer
            // with a RESEND packet asking for the whole exchange again before it
            // ACCEPTs (reference protocol.pyx `_connect_phase_one`: "this may
            // request the message to be resent multiple times"). Pre-23ai
            // servers RESEND routinely; a bounded loop guards against a
            // misbehaving peer that never stops asking. A REDIRECT answer
            // reconnects the transport to the redirected address and resends
            // the CONNECT there with the REDIRECT packet flag (and the
            // redirect-supplied connect data); RESEND and REDIRECT may
            // interleave — a RESEND after a redirect resends the redirected
            // CONNECT, flag included.
            let mut connect_data = connect_descriptor;
            let mut packet_flags = 0u8;
            let mut resend_rounds = 0u8;
            let mut redirect_rounds = 0u8;
            let accept = loop {
                let connect_payload = build_connect_packet_payload(&connect_data, advertised_sdu)?;
                let packet = encode_packet(
                    TNS_PACKET_TYPE_CONNECT,
                    packet_flags,
                    None,
                    &connect_payload,
                    PacketLengthWidth::Legacy16,
                )?;
                trace_connect_bytes("CONNECT packet", &packet);
                let split_connect_data = !connect_data_fits_inline(&connect_data);
                trace_connect_step("send CONNECT");
                core.write_all(cx, &packet).await?;
                if split_connect_data {
                    trace_connect_step("send CONNECT descriptor (data packet)");
                    core.send_data_packet(cx, connect_data.as_bytes(), usize::from(advertised_sdu))
                        .await?;
                }

                trace_connect_step("read ACCEPT");
                let reply = core.read_packet(PacketLengthWidth::Legacy16).await?;
                match reply.packet_type {
                    TNS_PACKET_TYPE_ACCEPT => break reply,
                    TNS_PACKET_TYPE_RESEND => {
                        resend_rounds += 1;
                        if resend_rounds > MAX_CONNECT_RESEND_ROUNDS {
                            return Err(Error::ConnectResendLoop(resend_rounds));
                        }
                        trace_connect_step("RESEND requested; resending CONNECT");
                        continue;
                    }
                    TNS_PACKET_TYPE_REDIRECT => {
                        redirect_rounds += 1;
                        if redirect_rounds > MAX_CONNECT_REDIRECT_ROUNDS {
                            return Err(Error::ConnectRedirectLoop(redirect_rounds));
                        }
                        let redirect_data = read_redirect_data(&mut core, &reply.payload).await?;
                        let target = parse_redirect_target(&redirect_data, descriptor.protocol)?;
                        trace_connect_value(
                            "REDIRECT target",
                            &format!("{}:{}", target.host, target.port),
                        );
                        // Reconnect the transport to the redirected listener
                        // (dropping the old connection closes it) and resend
                        // the CONNECT there, flagged as a redirect follow-up
                        // and carrying the redirect-supplied connect data
                        // (reference `_connect_phase_one`).
                        let (read, write) = dial(target.host, target.port).await?;
                        core = ConnectionCore::from_halves(read, write, "oracle_tcp_write");
                        core.set_protocol_limits(protocol_limits)?;
                        core.set_inactivity_timeout(inactivity_timeout);
                        connect_data = target.connect_data;
                        packet_flags = TNS_PACKET_FLAG_REDIRECT;
                        // The redirected listener negotiates from scratch and
                        // may itself ask for resends.
                        resend_rounds = 0;
                        trace_connect_step("REDIRECT: reconnected; resending CONNECT");
                        continue;
                    }
                    TNS_PACKET_TYPE_REFUSE => {
                        trace_connect_step("REFUSE received");
                        return Err(Error::ListenerRefused(
                            String::from_utf8_lossy(&reply.payload).to_string(),
                        ));
                    }
                    other => return Err(Error::UnexpectedPacket(other)),
                }
            };
            let accept_info = parse_accept_payload(&accept.payload)?;
            // Surface the negotiated ACCEPT capabilities so a captured trace
            // shows *why* the auth path forked: `fast_auth=true` takes the
            // combined fast-auth bundle, `fast_auth=false` falls back to the
            // classic protocol-negotiation + data-types round trips. This is the
            // single most useful line for diagnosing a "missing/failed fast-auth"
            // exchange. None of these values are secret (they mirror v$session /
            // negotiated SDU).
            trace_connect_value(
                "ACCEPT",
                &format!(
                    "sdu={} fast_auth={} end_of_response={} oob={}",
                    accept_info.sdu,
                    accept_info.supports_fast_auth,
                    accept_info.supports_end_of_response,
                    accept_info.supports_oob,
                ),
            );
            // Record the framing mode so the recovery drain (which runs on a raw
            // read half, without the Connection) decides the trailing-error
            // boundary the way this server frames it: pre-23ai (no
            // END_OF_RESPONSE) needs message-driven completion (bead
            // rust-oracledb-99xu).
            core.set_classic_framing(!accept_info.supports_end_of_response);
            let sdu = usize::try_from(accept_info.sdu)
                .unwrap_or(DEFAULT_SDU)
                .max(TNS_DATA_PACKET_OVERHEAD + 1);

            let mut ttc_seq_num = 1;
            let auth_connect_string = auth_connect_descriptor(
                &descriptor,
                &primary_description,
                token_auth,
                descriptor_ssl_server_dn_match,
                descriptor_ssl_server_cert_dn,
            );

            // Authentication has two shapes. Token auth (OCI IAM / OAuth2) carries
            // the credential in `AUTH_TOKEN` and skips the password-verifier round
            // trip entirely; password auth does the classic phase-one challenge /
            // phase-two verifier exchange. Both converge on a parsed auth response
            // plus the negotiated capabilities; only password auth yields a combo key
            // (used later to verify the server's response and for change-password).
            let (auth_two, capabilities, combo_key) = if let Some(token) = &options.access_token {
                // A database access token must never travel in clear text: require
                // TLS/TCPS, exactly as the reference does
                // (protocol.pyx `ERR_ACCESS_TOKEN_REQUIRES_TCPS`).
                if !descriptor.protocol.is_tls() {
                    return Err(Error::AccessTokenRequiresTcps);
                }
                // Token auth is only wired through the combined fast-auth
                // bundle; the servers that accept database tokens (23ai-era)
                // all advertise fast auth.
                if !accept_info.supports_fast_auth {
                    return Err(Error::FastAuthRequired);
                }
                // One combined fast-auth bundle carrying a phase-two `AUTH_TOKEN`
                // message; no resend. The payload (which embeds the token) is never
                // passed to `trace_connect_bytes`, so the secret stays out of logs.
                let auth_payload = build_fast_auth_token_payload(
                    &options.user,
                    token.expose(),
                    &identity.driver_name,
                    PYTHON_ORACLEDB_COMPAT_VERSION_NUM,
                    &auth_connect_string,
                    options.edition.as_deref(),
                )?;
                trace_connect_step("send AUTH token (fast-auth phase two)");
                core.send_data_packet(cx, &auth_payload, sdu).await?;
                trace_connect_step("read AUTH token response");
                let response = core.read_data_response(cx).await?;
                trace_connect_bytes("AUTH token response", &response);
                let auth = parse_auth_response_with_limits(&response, protocol_limits)?;
                let capabilities = auth.capabilities.unwrap_or_default();
                // Token auth derives no shared password key: there is no combo key,
                // hence no server-response MAC to verify and no change-password.
                (auth, capabilities, Vec::new())
            } else {
                let client_pid = process::id();
                // Pre-23ai servers do not understand the combined fast-auth
                // bundle: run the classic handshake instead — protocol
                // negotiation and data types as their own round trips
                // (reference protocol.pyx `_connect_phase_two`), then the same
                // two-phase password auth. The standalone payloads are exact
                // slices of the fast-auth bundle, so both paths negotiate
                // byte-identically.
                let negotiated_capabilities = if accept_info.supports_fast_auth {
                    None
                } else {
                    let protocol_payload = build_protocol_negotiation_payload()?;
                    trace_connect_step("send protocol negotiation (classic)");
                    core.send_data_packet(cx, &protocol_payload, sdu).await?;
                    trace_connect_step("read protocol negotiation");
                    let response = core.read_classic_data_response(cx).await?;
                    trace_connect_bytes("protocol negotiation response", &response);
                    let negotiated = parse_auth_response_with_limits(&response, protocol_limits)?;

                    let data_types_payload = build_data_types_payload()?;
                    trace_connect_step("send data types (classic)");
                    core.send_data_packet(cx, &data_types_payload, sdu).await?;
                    trace_connect_step("read data types");
                    let response = core.read_classic_data_response(cx).await?;
                    trace_connect_bytes("data types response", &response);
                    parse_auth_response_with_limits(&response, protocol_limits)?;

                    Some(negotiated.capabilities.unwrap_or_default())
                };
                let auth_one = if accept_info.supports_fast_auth {
                    build_fast_auth_phase_one_payload(
                        &options.user,
                        &identity.program,
                        &identity.machine,
                        &identity.osuser,
                        &identity.terminal,
                        client_pid,
                    )?
                } else {
                    build_auth_phase_one_payload(
                        &options.user,
                        &identity.program,
                        &identity.machine,
                        &identity.osuser,
                        &identity.terminal,
                        client_pid,
                    )?
                };
                trace_connect_bytes("AUTH phase one payload", &auth_one);
                trace_connect_step("send AUTH phase one");
                core.send_data_packet(cx, &auth_one, sdu).await?;
                trace_connect_step("read AUTH phase one");
                let auth_one_response = if accept_info.supports_fast_auth {
                    core.read_data_response(cx).await?
                } else {
                    core.read_classic_data_response(cx).await?
                };
                trace_connect_bytes("AUTH phase one response", &auth_one_response);
                let auth_one =
                    parse_auth_response_with_limits(&auth_one_response, protocol_limits)?;
                let capabilities = negotiated_capabilities
                    .or(auth_one.capabilities)
                    .unwrap_or_default();
                let verifier_type = auth_one
                    .verifier_type
                    .ok_or(Error::MissingSessionField("AUTH_VFR_DATA verifier type"))?;
                let encrypted = oracledb_protocol::crypto::generate_verifier(
                    options.password.as_bytes(),
                    &auth_one.session_data,
                    verifier_type,
                )?;
                let auth_two_payload = build_auth_phase_two_payload_with_proxy_with_seq(
                    &options.user,
                    &encrypted,
                    &identity.driver_name,
                    PYTHON_ORACLEDB_COMPAT_VERSION_NUM,
                    &auth_connect_string,
                    next_ttc_sequence(&mut ttc_seq_num),
                    &options.app_context,
                    options.proxy_user.as_deref(),
                    options.edition.as_deref(),
                    capabilities.ttc_field_version,
                )?;
                trace_connect_bytes("AUTH phase two payload", &auth_two_payload);
                trace_connect_step("send AUTH phase two");
                core.send_data_packet(cx, &auth_two_payload, sdu).await?;
                trace_connect_step("read AUTH phase two");
                let auth_two_response = if accept_info.supports_fast_auth {
                    core.read_data_response(cx).await?
                } else {
                    core.read_classic_data_response(cx).await?
                };
                trace_connect_bytes("AUTH phase two response", &auth_two_response);
                let auth_two =
                    parse_auth_response_with_limits(&auth_two_response, protocol_limits)?;
                oracledb_protocol::crypto::verify_server_response(
                    &encrypted.combo_key,
                    &auth_two.session_data,
                )?;
                (auth_two, capabilities, encrypted.combo_key)
            };

            let session_id = parse_session_u32(&auth_two.session_data, "AUTH_SESSION_ID")?;
            let serial_num = parse_session_u16(&auth_two.session_data, "AUTH_SERIAL_NUM")?;
            // Final handshake milestone: authentication succeeded and the server
            // handed back a session. `sid`/`serial` are the v$session identifiers
            // (not secret) and let an operator correlate a captured trace with a
            // server-side session.
            trace_connect_value(
                "session established",
                &format!("sid={session_id} serial={serial_num}"),
            );
            let server_version = auth_two.session_data.get("AUTH_VERSION_STRING").cloned();
            let db_unique_name = parse_db_unique_name(&auth_two.session_data);
            let server_version_tuple = auth_two
                .session_data
                .get("AUTH_VERSION_NO")
                .and_then(|value| value.trim().parse::<u32>().ok())
                .map(|num| {
                    decode_server_version_number(
                        num,
                        server_version_number_uses_extended_layout(capabilities.ttc_field_version),
                    )
                });

            // Arm the support-capture guard from the recorder installed at dial
            // time (both `Some` iff `ORACLEDB_CAPTURE` was set). On drop/close it
            // scrubs + gate-checks + persists the cassette.
            #[cfg(feature = "cassette")]
            let capture_guard = match (capture_recorder, capture_path) {
                (Some(recorder), Some(path)) => Some(transport::CaptureGuard::new(recorder, path)),
                _ => None,
            };
            Ok(Self {
                descriptor,
                identity,
                core,
                protocol_limits,
                session_id,
                serial_num,
                server_version,
                server_version_tuple,
                db_unique_name,
                capabilities,
                ttc_seq_num,
                sdu,
                protocol_version: accept_info.protocol_version,
                supports_fast_auth: accept_info.supports_fast_auth,
                supports_end_of_response: accept_info.supports_end_of_response,
                supports_oob: accept_info.supports_oob,
                cursor_columns: BTreeMap::new(),
                fetch_metadata_by_sql: HashMap::new(),
                fetch_metadata_order: VecDeque::new(),
                shape_cache: options.statement_shape_cache.clone().unwrap_or_default(),
                dead: false,
                user: options.user,
                combo_key,
                statement_cache: Vec::new(),
                statement_cache_size: options.statement_cache_size,
                in_use_cursors: HashSet::new(),
                lob_prefetch_cursors: BTreeSet::new(),
                copied_cursors: HashSet::new(),
                cursors_to_close: Vec::new(),
                sessionless_data: None,
                notification_buffer: Vec::new(),
                notification_header_consumed: false,
                transaction_context: None,
                txn_in_progress: false,
                #[cfg(feature = "cassette")]
                capture_guard,
            })
        })
        .await;
        match connect_result {
            Ok(result) => result,
            Err(_) => Err(Error::CallTimeout(connect_timeout_ms)),
        }
    }

    pub fn descriptor(&self) -> &EasyConnect {
        &self.descriptor
    }

    /// Host of the connected endpoint (reference thin-mode `connection.host`,
    /// upstream da4ec2d2526a). This is the address the session actually
    /// connected to, as captured in the resolved descriptor.
    pub fn host(&self) -> &str {
        &self.descriptor.host
    }

    /// Port of the connected endpoint (reference `connection.port`).
    pub fn port(&self) -> u16 {
        self.descriptor.port
    }

    /// Transport protocol (TCP / TCPS) of the connected endpoint (reference
    /// `connection.protocol`).
    pub fn protocol(&self) -> NetProtocol {
        self.descriptor.protocol
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

    /// The database unique name (`SYS_CONTEXT('USERENV','DB_UNIQUE_NAME')`),
    /// parsed from the AUTH phase-two `AUTH_SC_REAL_DBUNIQUE_NAME` field
    /// (reference thin-mode `connection.db_unique_name`, upstream 16a57f1cbd58).
    /// Empty when the server did not send it.
    pub fn db_unique_name(&self) -> &str {
        self.db_unique_name.as_deref().unwrap_or("")
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

    /// The negotiated TTC protocol version from the server's ACCEPT packet
    /// (`AcceptInfo.protocol_version`, reference connect.pyx). This is the
    /// TNS_VERSION_* level the client and server agreed on (e.g. 319 for a
    /// 19c+/23ai-era server that supports END_OF_RESPONSE framing).
    pub fn protocol_version(&self) -> u16 {
        self.protocol_version
    }

    /// Whether this session authenticated over the combined fast-auth bundle:
    /// the server advertised `TNS_ACCEPT_FLAG_FAST_AUTH` at accept time and the
    /// driver used the single-round-trip fast-auth path (23ai-era servers). A
    /// `false` value means the classic protocol-negotiation + data-types
    /// handshake was used instead.
    pub fn supports_fast_auth(&self) -> bool {
        self.supports_fast_auth
    }

    pub fn cancel_handle(&self) -> Result<CancelHandle> {
        Ok(CancelHandle {
            write: self.core.write_handle(),
            recovery: Arc::clone(&self.core.recovery),
        })
    }

    /// Whether a session-dead Oracle error (mapped to DPY-4011 by the Python
    /// layer) has been observed on this connection.
    pub fn is_dead(&self) -> bool {
        self.dead || self.core.recovery.is_dead()
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
                    self.core.recovery.mark_dead();
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
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
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
            self.capabilities.ttc_field_version,
        )?;
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        // change_password is an auth-shaped round trip, so the classic probe is
        // the same terminal-message rule the connect-phase reads use.
        let limits = self.protocol_limits;
        let response = self
            .read_response_cancellable(cx, !self.supports_end_of_response, |bytes| {
                classic_connect_response_is_complete(bytes, limits).unwrap_or(true)
            })
            .await?;
        self.note_parse(
            parse_auth_response_with_limits(&response, self.protocol_limits).map(|_| ()),
        )?;
        Ok(())
    }

    /// Register a CQN subscription (FUNC 125, opcode 1) on this connection.
    /// Returns the registration id (`Subscription.id`) and the EMON client id
    /// echoed in the subsequent NOTIFY. `public_qos`/`operations` are the public
    /// `SUBSCR_QOS_*` / `OPCODE_*` values; the wire derivation lives in the
    /// protocol builder. Reference `ThinSubscrImpl.subscribe`.
    #[allow(clippy::too_many_arguments)]
    pub async fn subscribe_register(
        &mut self,
        cx: &Cx,
        namespace: u32,
        name: Option<&str>,
        public_qos: u32,
        operations: u32,
        timeout: u32,
        grouping_class: u8,
        grouping_value: u32,
        grouping_type: u8,
    ) -> Result<SubscribeResult> {
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_subscribe_payload_with_seq(
            seq_num,
            TNS_SUBSCR_OP_REGISTER,
            Some(&self.user),
            None,
            namespace,
            name,
            public_qos,
            operations,
            timeout,
            grouping_class,
            grouping_value,
            grouping_type,
            0,
            self.capabilities.ttc_field_version,
        )?;
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        // Classic-aware read: pre-23ai servers never negotiate END_OF_RESPONSE
        // framing, so the flag-driven reader would block forever. The probe
        // decides completion by parsing the accumulated payload (bead
        // rust-oracledb-eyp7).
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let response = self
            .read_response_cancellable(cx, !self.supports_end_of_response, |bytes| {
                response_complete(&parse_subscribe_response_with_limits(
                    bytes,
                    capabilities,
                    limits,
                ))
            })
            .await?;
        self.note_parse(parse_subscribe_response_with_limits(
            &response,
            self.capabilities,
            self.protocol_limits,
        ))
    }

    /// Unregister a CQN subscription (FUNC 125, opcode 2). The `client_id` is
    /// the value returned by [`Self::subscribe_register`] (now non-None so its
    /// pointer/bytes are emitted) and `registration_id` rides on the tail.
    /// Reference `ThinSubscrImpl.unsubscribe`.
    #[allow(clippy::too_many_arguments)]
    pub async fn subscribe_unregister(
        &mut self,
        cx: &Cx,
        registration_id: u64,
        client_id: &[u8],
        namespace: u32,
        name: Option<&str>,
        public_qos: u32,
        operations: u32,
        timeout: u32,
        grouping_class: u8,
        grouping_value: u32,
        grouping_type: u8,
    ) -> Result<()> {
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        // unregister reuses the same `_write_message` path: the name/qos/
        // operations/grouping fields mirror the original registration so the
        // server can match the subscription (reference passes the same
        // `subscr_impl`).
        let payload = build_subscribe_payload_with_seq(
            seq_num,
            TNS_SUBSCR_OP_UNREGISTER,
            Some(&self.user),
            Some(client_id),
            namespace,
            name,
            public_qos,
            operations,
            timeout,
            grouping_class,
            grouping_value,
            grouping_type,
            registration_id,
            self.capabilities.ttc_field_version,
        )?;
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        // Classic-aware read (pre-23ai has no END_OF_RESPONSE framing); bead
        // rust-oracledb-eyp7.
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let response = self
            .read_response_cancellable(cx, !self.supports_end_of_response, |bytes| {
                response_complete(&parse_subscribe_response_with_limits(
                    bytes,
                    capabilities,
                    limits,
                ))
            })
            .await?;
        self.note_parse(parse_subscribe_response_with_limits(
            &response,
            self.capabilities,
            self.protocol_limits,
        ))?;
        Ok(())
    }

    /// Send the single NOTIFY message (FUNC 187) that arms the EMON push stream
    /// on this (emon) connection. No response is read here; pushed notification
    /// packets are consumed by [`Self::recv_notification`]. Reference
    /// `ThinSubscrImpl._bg_task_func` (sends NOTIFY then blocks reading).
    pub async fn notify_register(&mut self, cx: &Cx, client_id: &[u8]) -> Result<()> {
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload =
            build_notify_payload_with_seq(seq_num, client_id, self.capabilities.ttc_field_version)?;
        // NOTIFY sets the END_OF_REQUEST data flag on its (single) packet.
        self.core
            .send_data_packet_with_flags(cx, &payload, self.sdu, 0, TNS_DATA_FLAGS_END_OF_REQUEST)
            .await?;
        Ok(())
    }

    /// Wait for the next CQN notification record pushed by the EMON process and
    /// decode it. `read_timeout` bounds each underlying socket read so the
    /// background receive loop can poll a shutdown flag between reads (the DB
    /// never sends an end-of-stream marker; teardown unblocks the loop). The
    /// reference blocks forever and is unblocked by a forced socket close; the
    /// bounded read achieves the same clean teardown without a cross-thread
    /// socket close, and never hangs.
    ///
    /// Records may span several pushed packets, so this chains packets through
    /// `notification_buffer` exactly like the reference `ReadBuffer`.
    pub async fn recv_notification(
        &mut self,
        cx: &Cx,
        namespace: u32,
        public_qos: u32,
        read_timeout: Duration,
    ) -> Result<NotificationOutcome> {
        observe_cancellation_between_round_trips(cx)?;
        let db_name = self.descriptor.service_name.clone();
        loop {
            observe_cancellation_between_round_trips(cx)?;
            // consume the leading OAC message-type byte once
            if !self.notification_header_consumed {
                if self.notification_buffer.is_empty() {
                    match self.read_one_notification_packet(read_timeout).await? {
                        PacketRead::Appended => continue,
                        PacketRead::TimedOut => return Ok(NotificationOutcome::TimedOut),
                        PacketRead::Closed => return Ok(NotificationOutcome::Closed),
                    }
                }
                let consumed = check_notification_header_with_limits(
                    &self.notification_buffer,
                    self.protocol_limits,
                )?;
                self.notification_buffer.drain(..consumed);
                self.notification_header_consumed = true;
            }
            // try to decode one full record from the buffered bytes
            if !self.notification_buffer.is_empty() {
                if let Some((record, consumed)) = try_parse_oac_record_with_limits(
                    &self.notification_buffer,
                    namespace,
                    public_qos,
                    Some(&db_name),
                    self.protocol_limits,
                )? {
                    self.notification_buffer.drain(..consumed);
                    return Ok(NotificationOutcome::Record(record));
                }
            }
            // need more bytes: read another pushed packet (bounded)
            match self.read_one_notification_packet(read_timeout).await? {
                PacketRead::Appended => {}
                // a timeout mid-record is unusual; surface it so the caller can
                // re-check the shutdown flag and resume reading
                PacketRead::TimedOut => return Ok(NotificationOutcome::TimedOut),
                PacketRead::Closed => return Ok(NotificationOutcome::Closed),
            }
        }
    }

    /// Reads one DATA packet from the emon socket (bounded by `read_timeout`)
    /// and appends its TTC payload (after the 2-byte data flags) to
    /// `notification_buffer`. Reports a timeout (so the caller can poll its
    /// shutdown flag) or a closed/errored socket distinctly. Non-DATA packets
    /// (markers, disconnect) end the stream.
    async fn read_one_notification_packet(&mut self, read_timeout: Duration) -> Result<PacketRead> {
        let read = self.core.read_packet(PacketLengthWidth::Large32);
        let packet = match time::timeout(time::wall_now(), read_timeout, read).await {
            Ok(Ok(packet)) => packet,
            // socket closed / errored (incl. force-close on teardown): end the
            // stream cleanly, mirroring the reference's swallowed read error
            Ok(Err(_)) => return Ok(PacketRead::Closed),
            Err(_) => return Ok(PacketRead::TimedOut),
        };
        if packet.packet_type != TNS_PACKET_TYPE_DATA {
            return Ok(PacketRead::Closed);
        }
        let Some((_data_flags, payload)) = packet.payload.split_at_checked(2) else {
            return Ok(PacketRead::Closed);
        };
        self.notification_buffer.extend_from_slice(payload);
        Ok(PacketRead::Appended)
    }

    /// Ping with an upper bound on the round trip, used by pool health
    /// checks (reference pings under `ping_timeout`).
    pub async fn ping_with_timeout(&mut self, cx: &Cx, timeout_ms: u32) -> Result<()> {
        if timeout_ms == 0 {
            return self.ping(cx).await;
        }
        let deadline = QueryDeadline::from_timeout(Duration::from_millis(u64::from(timeout_ms)));
        match deadline.run(self.ping(cx)).await {
            Ok(result) => result,
            Err(DeadlineExpiry::BeforeStart) => self.reject_before_operation_start(cx, timeout_ms),
            // Previously this returned bare CallTimeout without even sending a
            // BREAK, leaving the half-sent ping round trip on the wire to poison
            // the next reuse. Break + drain like every other timeout path.
            Err(DeadlineExpiry::InFlight) => self.recover_from_call_timeout(cx, timeout_ms).await,
        }
    }

    /// Commit the current transaction. DML on a connection is not durable
    /// until committed.
    pub async fn commit(&mut self, cx: &Cx) -> Result<()> {
        let _span = obs_span!("oracledb.commit");
        self.send_function(cx, TNS_FUNC_COMMIT).await?;
        // a commit ends any active sessionless transaction on the server
        // (reference clears `_sessionless_data` via the SYNC piggyback)
        self.sessionless_data = None;
        Ok(())
    }

    /// Roll back the current transaction, discarding uncommitted DML.
    pub async fn rollback(&mut self, cx: &Cx) -> Result<()> {
        let _span = obs_span!("oracledb.rollback");
        self.send_function(cx, TNS_FUNC_ROLLBACK).await?;
        self.sessionless_data = None;
        Ok(())
    }

    /// Enable server-side `DBMS_OUTPUT` buffering for this session. `buffer_bytes`
    /// caps the server buffer; `None` requests an unbounded buffer
    /// (`DBMS_OUTPUT.ENABLE(NULL)`). Call once before running the PL/SQL whose
    /// output you want, then [`read_dbms_output`](Self::read_dbms_output).
    pub async fn enable_dbms_output(&mut self, cx: &Cx, buffer_bytes: Option<u32>) -> Result<()> {
        // `buffer_bytes` is a u32 we own (never untrusted text), so inlining it
        // as a numeric literal is injection-safe and avoids an extra IN bind.
        let arg = buffer_bytes.map_or_else(|| "null".to_string(), |n| n.to_string());
        self.execute_query_with_bind_rows_and_options_core(
            cx,
            &format!("begin dbms_output.enable({arg}); end;"),
            0,
            &[],
            ExecuteOptions::default(),
        )
        .await?;
        Ok(())
    }

    /// Read buffered `DBMS_OUTPUT` from this session via the canonical
    /// `GET_LINE(:line, :status)` loop, bounded by `max_lines` and `max_chars`.
    /// Stops cleanly when the server reports no more lines (`status != 0`) or
    /// when a bound is reached (setting [`DbmsOutput::truncated`]). Output is
    /// captured from this exact connection/session. python-oracledb leaves this
    /// loop to the caller; this centralizes it.
    pub async fn read_dbms_output(
        &mut self,
        cx: &Cx,
        max_lines: usize,
        max_chars: usize,
    ) -> Result<DbmsOutput> {
        // ORA_TYPE_SIZE_NUMBER is crate-private upstream; 22 is the NUMBER size.
        const NUMBER_SIZE: u32 = 22;
        let mut out = DbmsOutput::default();
        loop {
            observe_cancellation_between_round_trips(cx)?;
            let binds = [vec![
                BindValue::Output {
                    ora_type_num: oracledb_protocol::thin::ORA_TYPE_NUM_VARCHAR,
                    csfrm: oracledb_protocol::thin::CS_FORM_IMPLICIT,
                    buffer_size: 32767,
                },
                BindValue::Output {
                    ora_type_num: oracledb_protocol::thin::ORA_TYPE_NUM_NUMBER,
                    csfrm: 0,
                    buffer_size: NUMBER_SIZE,
                },
            ]];
            let res = self
                .execute_query_with_bind_rows_and_options_core(
                    cx,
                    "begin dbms_output.get_line(:1, :2); end;",
                    0,
                    &binds,
                    ExecuteOptions::default(),
                )
                .await?;
            // OUT values come back in bind order: [0] = line, [1] = status.
            let status = res
                .out_values
                .get(1)
                .and_then(|(_, v)| v.as_ref())
                .and_then(oracledb_protocol::thin::QueryValue::as_i64)
                .unwrap_or(1);
            if status != 0 {
                break; // no more lines buffered
            }
            let line = res
                .out_values
                .first()
                .and_then(|(_, v)| v.as_ref())
                .and_then(oracledb_protocol::thin::QueryValue::as_text)
                .unwrap_or("")
                .to_string();
            let chars = line.chars().count();
            // A real line is in hand. If it would cross a bound, stop and report
            // truncation: this line is past the bound and is discarded (it is part
            // of the acknowledged-truncated remainder). Checking after the read —
            // rather than before — keeps `truncated` exact: output that ends right
            // at `max_lines` reports `truncated == false`, not a false positive.
            if out.lines.len() >= max_lines || out.char_count + chars > max_chars {
                out.truncated = true;
                break;
            }
            out.char_count += chars;
            out.lines.push(line);
            out.line_count += 1;
        }
        Ok(out)
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
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_tpc_txn_switch_payload_with_seq(
            seq_num,
            0,
            data.operation,
            data.flags | TPC_TXN_FLAGS_SESSIONLESS,
            data.timeout,
            Some(transaction_id),
        );
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        // Classic-aware read (pre-23ai has no END_OF_RESPONSE framing); bead
        // rust-oracledb-eyp7.
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let response = self
            .read_response_cancellable(cx, !self.supports_end_of_response, |bytes| {
                response_complete(&parse_tpc_txn_switch_response_with_limits(
                    bytes,
                    capabilities,
                    limits,
                ))
            })
            .await?;
        let state = self.note_parse(parse_tpc_txn_switch_response_with_limits(
            &response,
            self.capabilities,
            self.protocol_limits,
        ))?;
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
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_tpc_txn_switch_payload_with_seq(
            seq_num,
            0,
            TNS_TPC_TXN_DETACH,
            TPC_TXN_FLAGS_SESSIONLESS,
            0,
            None,
        );
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        // Classic-aware read (pre-23ai has no END_OF_RESPONSE framing); bead
        // rust-oracledb-eyp7.
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let response = self
            .read_response_cancellable(cx, !self.supports_end_of_response, |bytes| {
                response_complete(&parse_tpc_txn_switch_response_with_limits(
                    bytes,
                    capabilities,
                    limits,
                ))
            })
            .await?;
        let state = self.note_parse(parse_tpc_txn_switch_response_with_limits(
            &response,
            self.capabilities,
            self.protocol_limits,
        ))?;
        // a suspend always clears the active transaction locally; the server's
        // SYNC piggyback confirms it (reference clears `_sessionless_data`)
        self.sessionless_data = None;
        self.apply_sessionless_state(state);
        Ok(())
    }

    /// Run a TPC transaction-switch (func 103) round trip and capture its
    /// response. Shared by `tpc_begin` (START) and `tpc_end` (DETACH).
    async fn tpc_switch_round_trip(
        &mut self,
        cx: &Cx,
        operation: u32,
        flags: u32,
        timeout: u32,
        xid: Option<&TpcXid<'_>>,
        context: Option<&[u8]>,
    ) -> Result<TpcSwitchResponse> {
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_tpc_switch_payload_with_seq_and_version(
            seq_num,
            operation,
            flags,
            timeout,
            xid,
            context,
            self.capabilities.ttc_field_version,
        );
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        // Classic-aware read (pre-23ai has no END_OF_RESPONSE framing); bead
        // rust-oracledb-eyp7.
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let response = self
            .read_response_cancellable(cx, !self.supports_end_of_response, |bytes| {
                response_complete(&parse_tpc_switch_response_with_limits(
                    bytes,
                    capabilities,
                    limits,
                ))
            })
            .await?;
        self.note_parse(parse_tpc_switch_response_with_limits(
            &response,
            self.capabilities,
            self.protocol_limits,
        ))
    }

    /// Run a TPC change-state (func 104) round trip and capture its response.
    /// Shared by `tpc_prepare` (PREPARE), `tpc_commit` (COMMIT) and
    /// `tpc_rollback` (ABORT).
    async fn tpc_change_state_round_trip(
        &mut self,
        cx: &Cx,
        operation: u32,
        requested_state: u32,
        xid: Option<&TpcXid<'_>>,
        context: Option<&[u8]>,
    ) -> Result<TpcChangeStateResponse> {
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_tpc_change_state_payload_with_seq_and_version(
            seq_num,
            operation,
            requested_state,
            0,
            xid,
            context,
            self.capabilities.ttc_field_version,
        );
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        // Classic-aware read (pre-23ai has no END_OF_RESPONSE framing); bead
        // rust-oracledb-eyp7.
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let response = self
            .read_response_cancellable(cx, !self.supports_end_of_response, |bytes| {
                response_complete(&parse_tpc_change_state_response_with_limits(
                    bytes,
                    capabilities,
                    limits,
                ))
            })
            .await?;
        self.note_parse(parse_tpc_change_state_response_with_limits(
            &response,
            self.capabilities,
            self.protocol_limits,
        ))
    }

    /// Begin (or resume/promote, via `flags`) an XA global transaction
    /// (reference impl/thin/connection.pyx `tpc_begin`). The server returns a
    /// transaction context which is captured for the subsequent
    /// end/prepare/commit/rollback round trips.
    pub async fn tpc_begin(
        &mut self,
        cx: &Cx,
        format_id: u32,
        global_transaction_id: &[u8],
        branch_qualifier: &[u8],
        flags: u32,
        timeout: u32,
    ) -> Result<()> {
        let xid = TpcXid {
            format_id,
            global_transaction_id,
            branch_qualifier,
        };
        let response = self
            .tpc_switch_round_trip(cx, TNS_TPC_TXN_START, flags, timeout, Some(&xid), None)
            .await?;
        self.transaction_context = Some(response.context);
        self.txn_in_progress = response.txn_in_progress;
        Ok(())
    }

    /// End (detach) an XA global transaction branch (reference `tpc_end`). The
    /// retained transaction context is echoed back; `xid` is `None` to detach
    /// the implicit current transaction. The context is cleared afterwards.
    pub async fn tpc_end(
        &mut self,
        cx: &Cx,
        xid: Option<(u32, &[u8], &[u8])>,
        flags: u32,
    ) -> Result<()> {
        let xid = xid.map(|(format_id, gtid, bqual)| TpcXid {
            format_id,
            global_transaction_id: gtid,
            branch_qualifier: bqual,
        });
        let context = self.transaction_context.clone();
        let response = self
            .tpc_switch_round_trip(
                cx,
                TNS_TPC_TXN_DETACH,
                flags,
                0,
                xid.as_ref(),
                context.as_deref(),
            )
            .await?;
        self.txn_in_progress = response.txn_in_progress;
        self.transaction_context = None;
        Ok(())
    }

    /// Prepare an XA global transaction for commit (reference `tpc_prepare`).
    /// Returns `true` when the transaction requires a commit, `false` when it
    /// is read-only; an unexpected out state raises DPY-5010.
    pub async fn tpc_prepare(&mut self, cx: &Cx, xid: Option<(u32, &[u8], &[u8])>) -> Result<bool> {
        let xid = xid.map(|(format_id, gtid, bqual)| TpcXid {
            format_id,
            global_transaction_id: gtid,
            branch_qualifier: bqual,
        });
        let context = self.transaction_context.clone();
        let response = self
            .tpc_change_state_round_trip(
                cx,
                TNS_TPC_TXN_PREPARE,
                TNS_TPC_TXN_STATE_PREPARE,
                xid.as_ref(),
                context.as_deref(),
            )
            .await?;
        self.txn_in_progress = response.txn_in_progress;
        match response.state {
            TNS_TPC_TXN_STATE_REQUIRES_COMMIT => Ok(true),
            TNS_TPC_TXN_STATE_READ_ONLY => Ok(false),
            other => Err(Error::UnknownTransactionState(other)),
        }
    }

    /// Commit an XA global transaction (reference `tpc_commit`). `one_phase`
    /// requests a single-phase (read-only) commit; two-phase requests a
    /// committed state and expects the server to return FORGOTTEN. The retained
    /// context is sent and cleared. An unexpected out state raises DPY-5010.
    pub async fn tpc_commit(
        &mut self,
        cx: &Cx,
        xid: Option<(u32, &[u8], &[u8])>,
        one_phase: bool,
    ) -> Result<()> {
        let xid = xid.map(|(format_id, gtid, bqual)| TpcXid {
            format_id,
            global_transaction_id: gtid,
            branch_qualifier: bqual,
        });
        let requested_state = if one_phase {
            TNS_TPC_TXN_STATE_READ_ONLY
        } else {
            TNS_TPC_TXN_STATE_COMMITTED
        };
        let context = self.transaction_context.clone();
        let response = self
            .tpc_change_state_round_trip(
                cx,
                TNS_TPC_TXN_COMMIT,
                requested_state,
                xid.as_ref(),
                context.as_deref(),
            )
            .await?;
        self.txn_in_progress = response.txn_in_progress;
        // reference `_check_tpc_commit_state`: one-phase must be READ_ONLY or
        // COMMITTED; two-phase must be FORGOTTEN.
        let state = response.state;
        let ok = if one_phase {
            state == TNS_TPC_TXN_STATE_READ_ONLY || state == TNS_TPC_TXN_STATE_COMMITTED
        } else {
            state == TNS_TPC_TXN_STATE_FORGOTTEN
        };
        if !ok {
            return Err(Error::UnknownTransactionState(state));
        }
        self.transaction_context = None;
        Ok(())
    }

    /// Roll back an XA global transaction (reference `tpc_rollback`). The
    /// retained context is sent; the server is expected to return ABORTED. An
    /// unexpected out state raises DPY-5010.
    pub async fn tpc_rollback(&mut self, cx: &Cx, xid: Option<(u32, &[u8], &[u8])>) -> Result<()> {
        let xid = xid.map(|(format_id, gtid, bqual)| TpcXid {
            format_id,
            global_transaction_id: gtid,
            branch_qualifier: bqual,
        });
        let context = self.transaction_context.clone();
        let response = self
            .tpc_change_state_round_trip(
                cx,
                TNS_TPC_TXN_ABORT,
                TNS_TPC_TXN_STATE_ABORTED,
                xid.as_ref(),
                context.as_deref(),
            )
            .await?;
        self.txn_in_progress = response.txn_in_progress;
        if response.state != TNS_TPC_TXN_STATE_ABORTED {
            return Err(Error::UnknownTransactionState(response.state));
        }
        Ok(())
    }

    /// Whether a server-side transaction is in progress (reference
    /// `get_transaction_in_progress` -> `_txn_in_progress`).
    pub fn transaction_in_progress(&self) -> bool {
        self.txn_in_progress
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
            // `_sessionless_data` and sets `_txn_in_progress = False`,
            // base.pyx:152/161)
            Some(SessionlessTxnState::Unset) => {
                self.sessionless_data = None;
                self.txn_in_progress = false;
            }
            // transaction started/resumed (reference replaces `_sessionless_data`
            // with a fresh `_SessionlessData`). This also covers a transaction
            // started via DBMS_TRANSACTION on the server, where no client-side
            // data existed yet: the server SET carries `started_on_server` so a
            // later client suspend/resume correctly raises DPY-3034.
            Some(SessionlessTxnState::Set { started_on_server }) => {
                self.txn_in_progress = true;
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

    #[allow(dead_code)]
    async fn execute_query_collect_core(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
    ) -> Result<QueryResult> {
        let mut result = self
            .execute_query_with_bind_rows_and_options_core(
                cx,
                sql,
                prefetch_rows,
                &[],
                ExecuteOptions::default(),
            )
            .await?;
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
        let fetched_result = self
            .define_and_fetch_rows_with_columns(cx, cursor_id, prefetch_rows.max(1), &columns, None)
            .await;
        let fetched = self.close_cursor_on_error(cursor_id, fetched_result)?;
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

    /// True when `sql` already has an open server cursor in the statement cache.
    /// The Arrow fetch entry points use this to tell a COLD query (freshly
    /// parsed cursor, no server-side define yet) from a WARM one (a cached cursor
    /// whose client-side define was established by an earlier fetch and persists
    /// server-side) — see [`Self::establish_cold_define`].
    #[cfg(feature = "arrow")]
    pub(crate) fn statement_has_cached_cursor(&self, sql: &str) -> bool {
        self.statement_cache
            .iter()
            .any(|entry| entry.sql == sql && entry.cursor_id != 0)
    }

    /// Establishes the client-side define for a COLD define-requiring query
    /// (`VECTOR` / native `JSON` / `CLOB` / `BLOB`) so the first fetch on the
    /// freshly parsed cursor carries the define. Such columns come back from the
    /// execute as describe-only metadata (an empty first page); the server only
    /// streams their values once a define-fetch has told it the client's buffer
    /// shape. The row query paths ([`Self::query_with`],
    /// [`Self::execute_query_collect_core`]) already do this; the Arrow fetch
    /// paths ran a plain fetch instead and desynced ("invalid ub8 length") on a
    /// cold `VECTOR` (bead a4-0mk).
    ///
    /// `warm` (from [`Self::statement_has_cached_cursor`], captured BEFORE the
    /// execute) skips this: a warm cursor already carries the server-side define
    /// from an earlier fetch, so a plain fetch is correct — this keeps the
    /// already-working warm Arrow path byte-identical. Non-define-requiring
    /// queries (the scalar columnar fast path) and executes that streamed rows
    /// inline are also no-ops.
    #[cfg(feature = "arrow")]
    pub(crate) async fn establish_cold_define(
        &mut self,
        cx: &Cx,
        warm: bool,
        result: &mut QueryResult,
        prefetch_rows: u32,
    ) -> Result<()> {
        if warm
            || result.cursor_id == 0
            || !result.rows.is_empty()
            || !columns_require_define(&result.columns)
        {
            return Ok(());
        }
        let cursor_id = result.cursor_id;
        let columns = result.columns.clone();
        let fetched_result = self
            .define_and_fetch_rows_with_columns(cx, cursor_id, prefetch_rows.max(1), &columns, None)
            .await;
        let fetched = self.close_cursor_on_error(cursor_id, fetched_result)?;
        result.rows = fetched.rows;
        result.more_rows = fetched.more_rows;
        if !fetched.columns.is_empty() {
            result.columns = fetched.columns;
        }
        if result.cursor_id == 0 {
            result.cursor_id = cursor_id;
        }
        Ok(())
    }

    /// Ergonomic query: bind typed Rust values positionally and return a lazy
    /// [`Rows`] facade. `params` is anything that converts into [`Params`]: a tuple
    /// `(40, "alice")`, a homogeneous array `[1, 2, 3]`, a raw
    /// `Vec<BindValue>`, or the named [`params!`](crate::params) form:
    ///
    /// ```no_run
    /// # use oracledb::Connection;
    /// # use asupersync::Cx;
    /// # async fn demo(conn: &mut Connection, cx: &Cx) -> Result<(), oracledb::Error> {
    /// let positional = conn.query(cx, "select :1 + :2 from dual", (40, 2)).await?
    ///     .collect(cx)
    ///     .await?;
    /// let named = conn
    ///     .query(cx, "select :a + :b from dual", oracledb::params!{ ":a" => 40, ":b" => 2 })
    ///     .await?
    ///     .collect(cx)
    ///     .await?;
    /// # let _ = (positional, named); Ok(()) }
    /// ```
    ///
    /// This is sugar over [`Self::query_with`]; the arraysize defaults to 100
    /// rows and define-requiring cells are materialized by default.
    pub async fn query<'p>(
        &mut self,
        cx: &Cx,
        sql: &str,
        params: impl Into<crate::Params<'p>>,
    ) -> Result<Rows<'_>> {
        self.query_with(cx, Query::owned_sql(sql.to_string()).bind(params))
            .await
    }

    /// Run a query that must return exactly one row.
    pub async fn query_one<'p>(
        &mut self,
        cx: &Cx,
        sql: &str,
        params: impl Into<crate::Params<'p>>,
    ) -> Result<Row> {
        let mut rows = self
            .query_with(
                cx,
                Query::owned_sql(sql.to_string())
                    .bind(params)
                    .arraysize(NonZeroU32::new(2).expect("two is non-zero"))
                    .prefetch(2),
            )
            .await?;
        rows.materialize_for_cardinality(cx).await?;
        rows.one()
    }

    /// Run a query that may return zero or one row.
    pub async fn query_opt<'p>(
        &mut self,
        cx: &Cx,
        sql: &str,
        params: impl Into<crate::Params<'p>>,
    ) -> Result<Option<Row>> {
        let mut rows = self
            .query_with(
                cx,
                Query::owned_sql(sql.to_string())
                    .bind(params)
                    .arraysize(NonZeroU32::new(2).expect("two is non-zero"))
                    .prefetch(2),
            )
            .await?;
        rows.materialize_for_cardinality(cx).await?;
        rows.opt()
    }

    /// Run a query and eagerly drain every fetch batch.
    pub async fn query_all<'p>(
        &mut self,
        cx: &Cx,
        sql: &str,
        params: impl Into<crate::Params<'p>>,
    ) -> Result<Vec<Row>> {
        self.query(cx, sql, params).await?.collect(cx).await
    }

    /// Run a query described by a [`Query`] builder and return a lazy row
    /// facade for the first batch plus continuation fetches.
    pub async fn query_with<'conn, 'q>(
        &'conn mut self,
        cx: &Cx,
        query: Query<'q>,
    ) -> Result<Rows<'conn>> {
        let Query {
            sql,
            params,
            arraysize,
            prefetch,
            prefetch_set: _,
            materialize_lobs,
            scrollable,
            timeout,
        } = query;
        let sql_owned = sql.into_owned();
        let binds = crate::sql_convert::resolve_params(&sql_owned, params)?;
        let bind_rows = if binds.is_empty() {
            Vec::new()
        } else {
            vec![binds]
        };
        let exec_options = ExecuteOptions::default().with_scrollable(scrollable);
        let deadline = QueryDeadline::new(cx, timeout);
        let mut result = match deadline
            .run(self.execute_query_with_bind_rows_and_options_core(
                cx,
                &sql_owned,
                prefetch,
                &bind_rows,
                exec_options,
            ))
            .await
        {
            Ok(result) => result?,
            Err(DeadlineExpiry::BeforeStart) => {
                return self.reject_before_operation_start(cx, deadline.timeout_ms());
            }
            Err(DeadlineExpiry::InFlight) => {
                return self
                    .recover_from_call_timeout(cx, deadline.timeout_ms())
                    .await
            }
        };
        if materialize_lobs
            && columns_require_define(&result.columns)
            && result.cursor_id != 0
            && result.rows.is_empty()
        {
            let cursor_id = result.cursor_id;
            let columns = result.columns.clone();
            let fetched = match deadline
                .run(self.define_and_fetch_rows_with_columns(
                    cx,
                    cursor_id,
                    prefetch.max(1),
                    &columns,
                    None,
                ))
                .await
            {
                Ok(result) => self.close_cursor_on_error(cursor_id, result)?,
                Err(DeadlineExpiry::BeforeStart) => {
                    self.release_cursor(cursor_id);
                    return self.reject_before_operation_start(cx, deadline.timeout_ms());
                }
                Err(DeadlineExpiry::InFlight) => {
                    let recovered = self
                        .recover_from_call_timeout(cx, deadline.timeout_ms())
                        .await;
                    self.close_cursor(cursor_id);
                    return recovered;
                }
            };
            result.rows = fetched.rows;
            result.more_rows = fetched.more_rows;
            if !fetched.columns.is_empty() {
                result.columns = fetched.columns;
            }
            if result.cursor_id == 0 {
                result.cursor_id = cursor_id;
            }
        }
        Ok(Rows::from_result(
            self, sql_owned, arraysize, deadline, scrollable, result,
        ))
    }

    /// Execute DML, DDL, or PL/SQL with a single bind row.
    pub async fn execute<'p>(
        &mut self,
        cx: &Cx,
        sql: &str,
        params: impl Into<crate::Params<'p>>,
    ) -> Result<ExecuteOutcome> {
        self.execute_with(cx, Execute::owned_sql(sql.to_string()).bind(params))
            .await
    }

    /// Execute DML, DDL, or PL/SQL described by an [`Execute`] builder.
    pub async fn execute_with<'e>(
        &mut self,
        cx: &Cx,
        execute: Execute<'e>,
    ) -> Result<ExecuteOutcome> {
        let deadline = QueryDeadline::new(cx, execute.timeout_duration());
        self.execute_with_deadline(cx, execute, deadline).await
    }

    async fn execute_with_deadline<'e>(
        &mut self,
        cx: &Cx,
        execute: Execute<'e>,
        deadline: QueryDeadline,
    ) -> Result<ExecuteOutcome> {
        let Execute {
            sql,
            params,
            timeout: _,
            options,
        } = execute;
        let sql_owned = sql.into_owned();
        let binds = crate::sql_convert::resolve_params(&sql_owned, params)?;
        let bind_rows = if binds.is_empty() {
            Vec::new()
        } else {
            vec![binds]
        };
        let result = match deadline
            .run(self.execute_query_with_bind_rows_and_options_core(
                cx, &sql_owned, 0, &bind_rows, options,
            ))
            .await
        {
            Ok(result) => result?,
            Err(DeadlineExpiry::BeforeStart) => {
                return self.reject_before_operation_start(cx, deadline.timeout_ms());
            }
            Err(DeadlineExpiry::InFlight) => {
                return self
                    .recover_from_call_timeout(cx, deadline.timeout_ms())
                    .await
            }
        };
        Ok(ExecuteOutcome::from_query_result(result))
    }

    /// Execute `sql` once for each bind row in a single array-DML operation.
    pub async fn execute_many<'b>(
        &mut self,
        cx: &Cx,
        sql: &str,
        rows: impl Into<crate::BatchRows<'b>>,
    ) -> Result<BatchOutcome> {
        self.execute_many_with(cx, Batch::owned_sql(sql.to_string(), rows))
            .await
    }

    /// Execute array DML described by a [`Batch`] builder.
    pub async fn execute_many_with<'b>(
        &mut self,
        cx: &Cx,
        batch: Batch<'b>,
    ) -> Result<BatchOutcome> {
        let Batch {
            sql,
            rows,
            timeout,
            options,
        } = batch;
        rows.validate_rectangular()?;
        if rows.is_empty() {
            return Ok(BatchOutcome::empty(options.arraydmlrowcounts()));
        }
        let sql_owned = sql.into_owned();
        let deadline = QueryDeadline::new(cx, timeout);
        let result = match deadline
            .run(self.execute_query_with_bind_rows_and_options_core(
                cx,
                &sql_owned,
                0,
                rows.as_slice(),
                options,
            ))
            .await
        {
            Ok(result) => result?,
            Err(DeadlineExpiry::BeforeStart) => {
                return self.reject_before_operation_start(cx, deadline.timeout_ms());
            }
            Err(DeadlineExpiry::InFlight) => {
                return self
                    .recover_from_call_timeout(cx, deadline.timeout_ms())
                    .await
            }
        };
        Ok(BatchOutcome::from_query_result(result))
    }

    /// Register a query against an existing CQN subscription.
    pub async fn register_query<'r>(
        &mut self,
        cx: &Cx,
        registration: Registration<'r>,
    ) -> Result<RegistrationOutcome> {
        let Registration {
            sql,
            params,
            registration_id,
            timeout,
        } = registration;
        let sql_owned = sql.into_owned();
        let binds = crate::sql_convert::resolve_params(&sql_owned, params)?;
        let bind_rows = if binds.is_empty() {
            Vec::new()
        } else {
            vec![binds]
        };
        let exec_options = ExecuteOptions::default().with_registration_id(registration_id);
        let deadline = QueryDeadline::new(cx, timeout);
        let result = match deadline
            .run(self.execute_query_with_bind_rows_and_options_core(
                cx,
                &sql_owned,
                0,
                &bind_rows,
                exec_options,
            ))
            .await
        {
            Ok(result) => result?,
            Err(DeadlineExpiry::BeforeStart) => {
                return self.reject_before_operation_start(cx, deadline.timeout_ms());
            }
            Err(DeadlineExpiry::InFlight) => {
                return self
                    .recover_from_call_timeout(cx, deadline.timeout_ms())
                    .await
            }
        };
        Ok(RegistrationOutcome::from_query_result(result))
    }

    pub(crate) async fn execute_query_with_binds_core(
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
        self.execute_query_with_bind_rows_and_options_core(
            cx,
            sql,
            prefetch_rows,
            &bind_rows,
            ExecuteOptions::default(),
        )
        .await
    }

    pub(crate) async fn execute_query_with_bind_rows_and_options_core(
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
                observe_cancellation_between_round_trips(cx)?;
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
        // Bind/execute round-trip span (feature-gated, zero-cost when off).
        // Carries the SQL digest, the bind count (binds per row — NEVER any bind
        // value), and the executemany row count; `db.rows_fetched` is filled
        // after the response.
        let _span = obs_span!(
            "oracledb.execute",
            db.statement = %crate::obs::sql_digest(sql),
            db.bind_count = bind_rows.first().map_or(0, Vec::len) as u64,
            db.bind_rows = bind_rows.len() as u64,
            db.rows_fetched = tracing::field::Empty,
        );
        observe_cancellation_between_round_trips(cx)?;
        // A parse-only describe carries no binds (the wire bind count is zero),
        // and a scroll continuation re-uses an already-bound cursor; neither
        // should be shape-validated. The raw validator only rejects ragged
        // array-DML rows — see `validate_bind_rows_shape`.
        if !exec_options.scroll_operation() && !exec_options.parse_only() {
            crate::sql_convert::validate_bind_rows_shape(sql, bind_rows)?;
            // First-execute type validation: array DML binds one type per column,
            // so a batch row whose value type disagrees with the type the first
            // typed row established would be silently coerced by the server into a
            // cryptic ORA error. Reject it up front with a precise typed error
            // (reference DPY-2006). A single-row execute always passes.
            crate::sql_convert::validate_bind_rows_types(bind_rows)?;
        }
        // If a prior cancellable round trip was dropped mid-read, break + drain
        // the stranded call before issuing this execute (Scope cancel-on-drop).
        self.ensure_clean_before_request().await?;
        let mut exec_options = exec_options.with_max_string_size(self.capabilities.max_string_size);
        // a `suspend_on_success` execute folds a post-detach into the pending
        // sessionless piggyback; validate (DPY-3034/3036) before any wire work
        // (reference execute.pyx `_handle_sessionless_suspend`)
        if exec_options.suspend_on_success() {
            self.prepare_sessionless_suspend_on_success()?;
        }
        let use_cache = exec_options.cache_statement() && !exec_options.parse_only();
        // Whether the cursor produced by this execute may be returned to the
        // statement cache (reference `Statement._return_to_cache`). A statement
        // that had to be copied because the cached cursor was in use is NOT
        // returnable: returning it would evict the still-live original from the
        // cache and reset its fetch position (ORA-01002).
        let mut is_copy = false;
        // Bind TYPE shape of this execute: a cached cursor is only reused when
        // the shape it was parsed/bound with is still compatible, otherwise it
        // is dropped and this execute re-parses with the new bind metadata
        // (bead rust-oracledb-ilel: ORA-01722 on a NUMBER->TEXT rebind).
        let bind_shape = bind_type_shape(bind_rows);
        if exec_options.cursor_id() == 0 && !exec_options.parse_only() {
            if use_cache {
                if self.statement_is_in_use(sql) {
                    // cached cursor busy: this execute parses a fresh (copy)
                    // cursor that must not be returned to the cache
                    is_copy = true;
                } else if let Some(cursor_id) = self.statement_cache_get(sql, &bind_shape) {
                    exec_options = exec_options.with_cursor_id(cursor_id);
                }
            } else if let Some(cursor_id) = self.statement_cache_take(sql, &bind_shape) {
                // reference pops the statement from the cache even when
                // cache_statement=False, reusing its open cursor once
                exec_options = exec_options.with_cursor_id(cursor_id);
            }
        }
        // Re-executing an open cursor whose columns require a client-side define
        // (VECTOR) must suppress server-side prefetch (reference
        // `stmt._no_prefetch`, set once during describe in messages/base.pyx
        // 1159-1164 and persisted on the cached statement). Otherwise the
        // re-execute prefetches the row inline and exhausts the cursor before
        // the define-fetch runs, raising ORA-01002 on the next fetch.
        if exec_options.cursor_id() != 0 && statement_is_query(sql) {
            if let Some(columns) = self.cursor_columns.get(&exec_options.cursor_id()) {
                if columns.iter().any(|column| {
                    column.ora_type_num() == oracledb_protocol::thin::ORA_TYPE_NUM_VECTOR
                }) {
                    exec_options = exec_options.with_no_prefetch(true);
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
        self.protocol_limits.check_batch_rows(bind_rows.len())?;
        if let Some(first_row) = bind_rows.first() {
            self.protocol_limits.check_binds(first_row.len())?;
        }
        let sessionless_piggyback = self.take_sessionless_piggyback();
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let mut payload = build_execute_payload_with_bind_rows_and_options_with_seq(
            sql,
            prefetch_rows,
            seq_num,
            statement_is_query(sql),
            bind_rows,
            exec_options,
            self.capabilities.ttc_field_version,
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
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        let known_columns = if exec_options.cursor_id() != 0 {
            self.cursor_columns
                .get(&exec_options.cursor_id())
                .cloned()
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        // Read under a cancel-on-drop guard: a dropped execute future arms the
        // next operation's break + drain. The classic probe is the same parse
        // this site runs below, with its result discarded.
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let first_bind_row = bind_rows.first().map(Vec::as_slice).unwrap_or(&[]);
        let classic = !self.supports_end_of_response;
        let response = self
            .read_flushing_out_binds_cancellable(cx, classic, |bytes| {
                response_complete(&parse_query_response_with_binds_options_columns_and_limits(
                    bytes,
                    capabilities,
                    first_bind_row,
                    exec_options,
                    &known_columns,
                    limits,
                ))
            })
            .await?;
        trace_query_bytes("EXECUTE query response", &response);
        let parsed = parse_query_response_with_binds_options_columns_and_limits(
            &response,
            self.capabilities,
            bind_rows.first().map(Vec::as_slice).unwrap_or(&[]),
            exec_options,
            &known_columns,
            self.protocol_limits,
        );
        match self.note_parse(parsed) {
            Ok(result) => {
                if result.cursor_id != 0
                    && !result.rows.is_empty()
                    && columns_have_lob_prefetch_fields(&result.columns)
                {
                    self.lob_prefetch_cursors.insert(result.cursor_id);
                }
                // a deferred begin/resume or a suspend-on-success reports its
                // outcome through the response's SYNC piggyback
                self.apply_sessionless_state(result.sessionless_txn_state);
                // refresh the transaction-in-progress flag from the wire
                // end-of-call status (reference protocol.pyx
                // `_process_call_status`); leave unchanged if the response
                // carried no STATUS message.
                if let Some(txn_in_progress) = result.txn_in_progress {
                    self.txn_in_progress = txn_in_progress;
                }
                if is_copy {
                    // a copied cursor is never returned to the statement cache;
                    // it is closed when its owning cursor releases it (reference
                    // `_return_to_cache = False` -> `_add_cursor_to_close`).
                    if result.cursor_id != 0 {
                        self.copied_cursors.insert(result.cursor_id);
                    }
                } else if use_cache {
                    self.statement_cache_put(sql, result.cursor_id, bind_shape);
                }
                // Mark the open query cursor as in use so a concurrent execute
                // of the same SQL on another cursor of this connection does not
                // reuse it (and reset its server-side fetch position). Released
                // by `release_cursor` when the owning cursor closes or
                // re-prepares (reference `Statement._in_use`). Only query
                // cursors hold a fetch position vulnerable to ORA-01002.
                if result.cursor_id != 0 && statement_is_query(sql) && !exec_options.parse_only() {
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
                obs_record!(_span, db.rows_fetched = result.rows.len() as u64);
                if exec_options.parse_only() {
                    return Ok(result);
                }
                self.apply_refetch_metadata(cx, sql, result, prefetch_rows.max(2))
                    .await
            }
            Err(err) => {
                // drop the cached cursor so the next execute re-parses
                // (reference base.pyx:1186-1189 clear_cursor on errors)
                if use_cache {
                    self.statement_cache_invalidate(sql, exec_options.cursor_id());
                }
                Err(err)
            }
        }
    }

    /// Low-level raw execute: run `sql` once per bind row with explicit
    /// [`ExecuteOptions`] and an optional per-call timeout, returning the first
    /// fetch batch as a raw [`QueryResult`] (columns, `cursor_id`, `more_rows`,
    /// `rows`, OUT/RETURNING binds, batch errors, array DML row counts).
    ///
    /// This is the execute-side counterpart to the retained low-level fetch
    /// primitives ([`Self::fetch_rows`],
    /// [`Self::define_and_fetch_rows_with_columns`], [`Self::scroll_cursor`],
    /// [`Self::fetch_cursor`]): the four operation families
    /// ([`Self::query`]/[`Self::execute`]/[`Self::execute_many`]) are the
    /// ergonomic surface built *over* this, and project the `QueryResult` into
    /// the curated [`Rows`]/[`ExecuteOutcome`]/[`BatchOutcome`] outcomes. Use
    /// `execute_raw` only when you need the unprojected wire result — for
    /// example a statement-type-agnostic caller that decides query-vs-DML from
    /// `result.columns`, a parse-only describe (`exec_options.with_parse_only`),
    /// or per-bind-row OUT/RETURNING aggregation. Prefer the families for
    /// ordinary application code.
    ///
    /// `bind_rows` is positional array DML: each inner `Vec<BindValue>` is one
    /// bind row applied in a single round trip (an empty slice runs `sql` once
    /// with no binds). `prefetch_rows` is the requested first-batch size; rows
    /// beyond the first batch (when [`QueryResult::more_rows`] is set) are
    /// fetched with the cursor primitives above. `timeout_ms` of `Some(n)` with
    /// `n > 0` bounds the round trip with a BREAK→drain→[`Error::CallTimeout`]
    /// recovery that leaves the session usable; `None`/`Some(0)` runs untimed.
    pub async fn execute_raw(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        bind_rows: &[Vec<BindValue>],
        exec_options: ExecuteOptions,
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        self.execute_query_with_bind_rows_options_call_timeout(
            cx,
            sql,
            prefetch_rows,
            bind_rows,
            exec_options,
            timeout_ms,
        )
        .await
    }

    pub(crate) async fn execute_query_with_bind_rows_options_call_timeout(
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
                .execute_query_with_bind_rows_and_options_core(
                    cx,
                    sql,
                    prefetch_rows,
                    bind_rows,
                    exec_options,
                )
                .await;
        };
        let deadline = QueryDeadline::from_timeout(Duration::from_millis(u64::from(timeout_ms)));
        match deadline
            .run(self.execute_query_with_bind_rows_and_options_core(
                cx,
                sql,
                prefetch_rows,
                bind_rows,
                exec_options,
            ))
            .await
        {
            Ok(result) => result,
            Err(DeadlineExpiry::BeforeStart) => self.reject_before_operation_start(cx, timeout_ms),
            Err(DeadlineExpiry::InFlight) => self.recover_from_call_timeout(cx, timeout_ms).await,
        }
    }

    /// If a previous cancellable fetch future was dropped mid-read (its
    /// `CancelDrainGuard` moved the recovery phase to `BreakSent`), break +
    /// drain the stranded server call now — before this round trip sends its own
    /// request — so the leftover bytes / still-running call cannot poison this
    /// response. A failed drain marks the connection dead and surfaces
    /// [`Error::ConnectionClosed`].
    ///
    /// A bare [`fetch_rows_request`](Self::fetch_rows_request) whose paired
    /// `fetch_rows_ref_response` is never consumed (the caller abandons the
    /// speculative page without dropping a response future to fire the guard)
    /// leaves the phase `InFlight` with a stranded response on the wire. Treat
    /// that the same as a dropped fetch: move it to `BreakSent` so the drain
    /// below reclaims the wire instead of wedging the connection on every
    /// subsequent operation. This is only ever reached from the *start* of the
    /// next operation — `fetch_rows_ref_response`, the one path that legitimately
    /// reads its own `InFlight` response, never calls this.
    async fn ensure_clean_before_request(&mut self) -> Result<()> {
        if self.core.recovery.phase() == SessionRecoveryPhase::InFlight {
            self.core.recovery.mark_break_required();
        }
        if !self.core.recovery.begin_pending_drain()? {
            return Ok(());
        }
        match self
            .core
            .cancel_and_drain_wire(BREAK_DRAIN_RECOVERY_TIMEOUT)
        {
            Ok(()) => {
                self.core.recovery.finish_drain_ready();
                Ok(())
            }
            Err(err) => {
                self.core.recovery.mark_dead();
                self.dead = true;
                Err(err)
            }
        }
    }

    /// Read one TTC response under a `CancelDrainGuard`: if THIS read future is
    /// dropped mid-flight (the fetch was cancelled / raced), the guard moves the
    /// recovery phase to `BreakSent` so the next operation breaks + drains the
    /// stranded call. A normal completion disarms the guard, so the uncancelled
    /// path costs nothing beyond an `Arc::clone`.
    async fn read_response_cancellable(
        &mut self,
        cx: &Cx,
        classic: bool,
        probe: impl Fn(&[u8]) -> bool,
    ) -> Result<Vec<u8>> {
        // Clone the Arc so the guard owns a handle independent of the `&mut self`
        // read borrow (the two touch disjoint state but the borrow checker can't
        // prove it across the guard's lifetime).
        let recovery = Arc::clone(&self.core.recovery);
        let mut guard = CancelDrainGuard::arm(recovery)?;
        let response = self
            .core
            .read_data_response_probed(cx, classic, probe)
            .await?;
        guard.disarm();
        Ok(response)
    }

    /// [`Self::read_response_cancellable`] for the bind/execute path, which reads
    /// via [`read_data_response_flushing_out_binds`] (it answers FLUSH_OUT_BINDS
    /// requests). Same cancel-on-drop semantics: a dropped execute future arms
    /// the next operation's break + drain.
    async fn read_flushing_out_binds_cancellable(
        &mut self,
        cx: &Cx,
        classic: bool,
        probe: impl Fn(&[u8]) -> bool,
    ) -> Result<Vec<u8>> {
        let recovery = Arc::clone(&self.core.recovery);
        let mut guard = CancelDrainGuard::arm(recovery)?;
        let response = self
            .core
            .read_data_response_flushing_out_binds_probed(cx, self.sdu, classic, probe)
            .await?;
        guard.disarm();
        Ok(response)
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
        // Fetch round-trip span (feature-gated, zero-cost when off). Carries the
        // cursor id and the requested arraysize; `db.rows_fetched` is filled
        // after the response.
        let _span = obs_span!(
            "oracledb.fetch",
            db.cursor_id = cursor_id as u64,
            db.arraysize = arraysize as u64,
            db.rows_fetched = tracing::field::Empty,
        );
        observe_cancellation_between_round_trips(cx)?;
        // If a prior fetch future was cancelled mid-read, break + drain the
        // stranded call before issuing this fetch (Scope-based cancel-on-drop).
        self.ensure_clean_before_request().await?;
        let columns = self
            .cursor_columns
            .get(&cursor_id)
            .cloned()
            .unwrap_or_else(|| known_columns.to_vec());
        let lob_prefetch = self.lob_prefetch_cursors.contains(&cursor_id);
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = if lob_prefetch {
            build_define_fetch_payload_with_seq(
                cursor_id,
                arraysize,
                seq_num,
                &columns,
                self.capabilities.ttc_field_version,
            )?
        } else {
            build_fetch_payload_with_seq(
                cursor_id,
                arraysize,
                seq_num,
                self.capabilities.ttc_field_version,
            )
        };
        trace_query_bytes("FETCH payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        // Read under a cancel-on-drop guard: if THIS fetch future is dropped
        // mid-read, the next operation will break + drain the stranded call.
        // The classic probe is the same parse this site runs below, with its
        // result discarded.
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let classic = !self.supports_end_of_response;
        let profile = fetch_profile::enabled();
        let read_start = profile.then(time::wall_now);
        let response = self
            .read_response_cancellable(cx, classic, |bytes| {
                response_complete(&if lob_prefetch {
                    parse_define_fetch_response_with_context_and_limits(
                        bytes,
                        capabilities,
                        &columns,
                        previous_row,
                        limits,
                    )
                } else {
                    parse_fetch_response_with_context_and_limits(
                        bytes,
                        capabilities,
                        &columns,
                        previous_row,
                        limits,
                    )
                })
            })
            .await?;
        if let Some(start) = read_start {
            fetch_profile::add_read(time::wall_now().duration_since(start));
        }
        trace_query_bytes("FETCH response", &response);
        let decode_start = profile.then(time::wall_now);
        let parsed = if lob_prefetch {
            parse_define_fetch_response_with_context_and_limits(
                &response,
                self.capabilities,
                &columns,
                previous_row,
                self.protocol_limits,
            )
        } else {
            parse_fetch_response_with_context_and_limits(
                &response,
                self.capabilities,
                &columns,
                previous_row,
                self.protocol_limits,
            )
        };
        if let Some(start) = decode_start {
            fetch_profile::add_decode(time::wall_now().duration_since(start));
        }
        let result = self.note_parse(parsed)?;
        obs_record!(_span, db.rows_fetched = result.rows.len() as u64);
        self.remember_cursor_columns(&result);
        Ok(result)
    }

    /// Zero-copy companion to [`fetch_rows`](Self::fetch_rows): fetch one batch
    /// of rows from an open server cursor and return a
    /// [`BorrowedFetchResult`]
    /// whose rows borrow the response buffer (no per-cell allocation for the
    /// common scalar case). Iterate the rows with
    /// [`BorrowedRowBatch::for_each_row_ref`](oracledb_protocol::thin::BorrowedRowBatch::for_each_row_ref).
    ///
    /// This is additive: the owned [`fetch_rows`](Self::fetch_rows) path is
    /// unchanged. Prefer [`for_each_row_ref`](Self::for_each_row_ref) for the
    /// common "execute and drain" case; this lower-level method exists for
    /// callers that page manually.
    pub async fn fetch_rows_ref(
        &mut self,
        cx: &Cx,
        cursor_id: u32,
        arraysize: u32,
        previous_row: Option<&[Option<oracledb_protocol::thin::QueryValue>]>,
    ) -> Result<BorrowedFetchResult> {
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        let columns = self
            .cursor_columns
            .get(&cursor_id)
            .cloned()
            .unwrap_or_default();
        let lob_prefetch = self.lob_prefetch_cursors.contains(&cursor_id);
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = if lob_prefetch {
            build_define_fetch_payload_with_seq(
                cursor_id,
                arraysize,
                seq_num,
                &columns,
                self.capabilities.ttc_field_version,
            )?
        } else {
            build_fetch_payload_with_seq(
                cursor_id,
                arraysize,
                seq_num,
                self.capabilities.ttc_field_version,
            )
        };
        trace_query_bytes("FETCH payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        // The classic probe runs the same borrowed parsers this site uses
        // below; the borrowed result is dropped inside the closure, so no
        // borrow of the probe bytes escapes.
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let classic = !self.supports_end_of_response;
        let profile = fetch_profile::enabled();
        let read_start = profile.then(time::wall_now);
        let response = self
            .read_response_cancellable(cx, classic, |bytes| {
                if lob_prefetch {
                    response_complete(&parse_define_fetch_response_borrowed_with_limits(
                        bytes,
                        capabilities,
                        &columns,
                        previous_row,
                        limits,
                    ))
                } else {
                    response_complete(&parse_query_response_borrowed_with_limits(
                        bytes,
                        capabilities,
                        &columns,
                        previous_row,
                        limits,
                    ))
                }
            })
            .await?;
        if let Some(start) = read_start {
            fetch_profile::add_read(time::wall_now().duration_since(start));
        }
        trace_query_bytes("FETCH response", &response);
        let decode_start = profile.then(time::wall_now);
        let parsed = if lob_prefetch {
            parse_define_fetch_response_borrowed_with_limits(
                &response,
                self.capabilities,
                &columns,
                previous_row,
                self.protocol_limits,
            )
        } else {
            parse_query_response_borrowed_with_limits(
                &response,
                self.capabilities,
                &columns,
                previous_row,
                self.protocol_limits,
            )
        };
        if let Some(start) = decode_start {
            fetch_profile::add_decode(time::wall_now().duration_since(start));
        }
        let result = self.note_parse(parsed)?;
        // Mirror the owned `fetch_rows` path: if the server re-described the
        // cursor mid-paging (the type-change refetch path emits DESCRIBE_INFO),
        // persist the adjusted column list under this cursor id so subsequent
        // pages decode with the new schema. Keyed on the known `cursor_id`
        // (the response's own cursor_id is 0 on an ordinary fetch).
        if cursor_id != 0 && !result.batch.columns().is_empty() {
            self.cursor_columns
                .insert(cursor_id, result.batch.columns().to_vec());
        }
        Ok(result)
    }

    /// Send a FETCH request for the next page on an open cursor **without**
    /// reading its response — the *request* half of the speculative next-page
    /// prefetch that overlaps page K+1's wire round trip with page K's decode
    /// (bead rust-oracledb-xad / 3oi). Pair it with
    /// [`fetch_rows_ref_response`](Self::fetch_rows_ref_response), which reads +
    /// decodes the outstanding page.
    ///
    /// ## Cancellation safety
    ///
    /// Issuing this request leaves a server response in flight on the wire. So
    /// it moves the recovery phase to `InFlight` for the entire window until
    /// that response is consumed: if the owning future is dropped while the
    /// response is in flight — whether during the prior page's decode (no read
    /// yet) or during the response read itself — the next operation breaks +
    /// drains the stranded page before issuing its own request (the same
    /// machinery that protects a dropped fetch, see `CancelDrainGuard`). A
    /// request can only be issued from a clean wire boundary, so it first drains
    /// any *previously* stranded call.
    pub async fn fetch_rows_request(
        &mut self,
        cx: &Cx,
        cursor_id: u32,
        arraysize: u32,
    ) -> Result<()> {
        observe_cancellation_between_round_trips(cx)?;
        // A request must start from a clean boundary: if a prior cancelled op
        // left a drain pending, break + drain it first.
        self.ensure_clean_before_request().await?;
        let columns = self
            .cursor_columns
            .get(&cursor_id)
            .cloned()
            .unwrap_or_default();
        let lob_prefetch = self.lob_prefetch_cursors.contains(&cursor_id);
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = if lob_prefetch {
            build_define_fetch_payload_with_seq(
                cursor_id,
                arraysize,
                seq_num,
                &columns,
                self.capabilities.ttc_field_version,
            )?
        } else {
            build_fetch_payload_with_seq(
                cursor_id,
                arraysize,
                seq_num,
                self.capabilities.ttc_field_version,
            )
        };
        trace_query_bytes("FETCH payload (prefetch)", &payload);
        self.core.recovery.begin_operation()?;
        match self.core.send_data_packet(cx, &payload, self.sdu).await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.core.recovery.mark_dead();
                self.dead = true;
                Err(err)
            }
        }?;
        // A speculative response is now outstanding: the recovery phase remains
        // InFlight for the WHOLE window until it is consumed by
        // `fetch_rows_ref_response`, so a drop anywhere in {decode prior page,
        // read this response} cleans it up before the next op runs.
        Ok(())
    }

    /// Read + decode the response to a FETCH request previously issued by
    /// [`fetch_rows_request`](Self::fetch_rows_request) — the *response* half of
    /// the speculative next-page prefetch. Returns the same
    /// [`BorrowedFetchResult`] as [`fetch_rows_ref`](Self::fetch_rows_ref).
    ///
    /// Must be called exactly once per `fetch_rows_request`, with the request's
    /// `cursor_id` and the `previous_row` seed for duplicate-column decoding
    /// (the last row of the prior page — known by now because the prior page is
    /// fully decoded). The read runs under a `CancelDrainGuard` (a mid-read
    /// drop re-arms the pending drain); a clean read disarms the
    /// `fetch_rows_request` arming, leaving no stranded page.
    pub async fn fetch_rows_ref_response(
        &mut self,
        cx: &Cx,
        cursor_id: u32,
        previous_row: Option<&[Option<oracledb_protocol::thin::QueryValue>]>,
    ) -> Result<BorrowedFetchResult> {
        observe_cancellation_between_round_trips(cx)?;
        // Read under the cancel-on-drop guard. NOTE: do NOT
        // `ensure_clean_before_request` here — the InFlight phase is set by our
        // own `fetch_rows_request` and marks the response we are about to read
        // legitimately, not a stranded call to discard.
        let columns = self
            .cursor_columns
            .get(&cursor_id)
            .cloned()
            .unwrap_or_default();
        let lob_prefetch = self.lob_prefetch_cursors.contains(&cursor_id);
        // The classic probe runs the same borrowed parsers this site uses
        // below; the borrowed result is dropped inside the closure.
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let classic = !self.supports_end_of_response;
        let profile = fetch_profile::enabled();
        let read_start = profile.then(time::wall_now);
        let response = self
            .read_response_cancellable(cx, classic, |bytes| {
                if lob_prefetch {
                    response_complete(&parse_define_fetch_response_borrowed_with_limits(
                        bytes,
                        capabilities,
                        &columns,
                        previous_row,
                        limits,
                    ))
                } else {
                    response_complete(&parse_query_response_borrowed_with_limits(
                        bytes,
                        capabilities,
                        &columns,
                        previous_row,
                        limits,
                    ))
                }
            })
            .await?;
        if let Some(start) = read_start {
            fetch_profile::add_read(time::wall_now().duration_since(start));
        }
        trace_query_bytes("FETCH response (prefetch)", &response);
        let decode_start = profile.then(time::wall_now);
        let parsed = if lob_prefetch {
            parse_define_fetch_response_borrowed_with_limits(
                &response,
                self.capabilities,
                &columns,
                previous_row,
                self.protocol_limits,
            )
        } else {
            parse_query_response_borrowed_with_limits(
                &response,
                self.capabilities,
                &columns,
                previous_row,
                self.protocol_limits,
            )
        };
        if let Some(start) = decode_start {
            fetch_profile::add_decode(time::wall_now().duration_since(start));
        }
        let result = self.note_parse(parsed)?;
        if cursor_id != 0 && !result.batch.columns().is_empty() {
            self.cursor_columns
                .insert(cursor_id, result.batch.columns().to_vec());
        }
        Ok(result)
    }

    /// Execute `sql` and drive every fetched row through `callback` as a slice
    /// of borrowed [`QueryValueRef`] —
    /// the zero-copy fetch fast path. Scalar cells (Text / Number / Raw /
    /// Boolean / Interval / DateTime) borrow the fetch buffer directly, so a
    /// Rust consumer iterating a wide many-row result pays ~0 allocations per
    /// cell, in contrast to the owned [`execute_raw`](Self::execute_raw) +
    /// [`fetch_rows`](Self::fetch_rows) path which materializes a `String` /
    /// `Vec<u8>` per scalar cell of every row.
    ///
    /// The `&[Option<QueryValueRef>]` row slice is valid only for the duration
    /// of each `callback` call — it borrows the batch buffer and cannot escape.
    /// Use [`QueryValueRef::to_owned_value`](oracledb_protocol::thin::QueryValueRef::to_owned_value)
    /// to keep a value past the call. Cold cells (LOB / Cursor / Object / Vector
    /// / JSON / non-UTF-8 / ROWID) surface as `QueryValueRef::Owned`.
    ///
    /// Pages through the cursor with the given `arraysize` until the server
    /// reports no more rows, releasing the server cursor back to the statement
    /// cache when done. The owned fetch path is untouched.
    pub async fn for_each_row_ref<F>(
        &mut self,
        cx: &Cx,
        sql: &str,
        arraysize: u32,
        mut callback: F,
    ) -> Result<()>
    where
        F: FnMut(&[Option<QueryValueRef<'_>>]) -> Result<()>,
    {
        // Streaming-query span (feature-gated, zero-cost when off). Carries the
        // SQL digest and the arraysize (the prefetch fill target); filled after
        // the loop with the total rows streamed, the number of paged fetch round
        // trips, and the max prefetch look-ahead depth. This path keeps a single
        // page in flight, so the look-ahead (queue) depth is 0 or 1 — the
        // backpressure signal; rows/sec is the operator's db.rows_streamed over
        // the span duration. NEVER row data.
        let _stream_span = obs_span!(
            "oracledb.stream",
            db.statement = %crate::obs::sql_digest(sql),
            db.arraysize = arraysize as u64,
            db.rows_streamed = tracing::field::Empty,
            db.pages_fetched = tracing::field::Empty,
            db.prefetch_inflight_max = tracing::field::Empty,
        );
        #[cfg(feature = "tracing")]
        let mut rows_streamed: u64 = 0;
        #[cfg(feature = "tracing")]
        let mut pages_fetched: u64 = 0;
        #[cfg(feature = "tracing")]
        let mut prefetch_inflight_max: u64 = 0;

        // First round trip: EXECUTE + first fetch batch (owned), to obtain the
        // open cursor id and column metadata. The first batch's rows are decoded
        // borrowed by re-parsing nothing — instead we capture them from the
        // owned result below. To keep the borrowed guarantee for the first batch
        // too, we re-fetch borrowed pages from the cursor.
        let first = self
            .execute_query_with_bind_rows_and_options_core(
                cx,
                sql,
                arraysize,
                &[],
                ExecuteOptions::default(),
            )
            .await?;
        let cursor_id = first.cursor_id;
        let mut cursor = BorrowedStreamCursorGuard::new(self, cursor_id);
        if cursor_id != 0 && columns_have_lob_prefetch_fields(&first.columns) {
            cursor.connection().lob_prefetch_cursors.insert(cursor_id);
        }

        // Emit the first (owned) batch's rows as borrowed refs over owned values.
        // The first execute round trip already materialized them; surfacing them
        // through QueryValueRef::Owned keeps the callback's type uniform without
        // a second round trip for batch one.
        for row in &first.rows {
            let refs: Vec<Option<QueryValueRef<'_>>> = row
                .iter()
                .map(|cell| cell.as_ref().map(QueryValueRef::Owned))
                .collect();
            callback(&refs)?;
        }
        #[cfg(feature = "tracing")]
        {
            rows_streamed += first.rows.len() as u64;
        }

        let mut more_rows = first.more_rows;
        let mut previous_row: Option<Vec<Option<oracledb_protocol::thin::QueryValue>>> =
            first.rows.last().cloned();

        // Speculative next-page prefetch (bead xad / 3oi): overlap the wire round
        // trip of page K+1 with the CPU decode of page K. The trick is that a
        // FETCH *request* needs only the cursor id + arraysize (not page K's
        // rows), while `previous_row` — page K's last row, the duplicate-column
        // seed — is needed only when *decoding* K+1, by which point K is done.
        //
        // So the loop keeps one page of look-ahead outstanding on the wire:
        //   1. request page K+1   (just the send; arms the prefetch drain guard)
        //   2. read+decode page K (callback) — overlaps with K+1 in flight
        //   3. read+decode page K+1 next iteration, immediately request K+2, ...
        //
        // Cancellation: `fetch_rows_request` leaves the recovery phase InFlight
        // until the response is consumed, so a drop anywhere in this loop
        // (including inside the callback, with a page in flight) leaves the
        // stranded page to be broken + drained by the next op on this
        // connection. Decode stays on this task, so the borrowed batch buffer
        // never crosses a thread/await boundary that outlives it — the
        // borrowed-fetch guarantee is preserved and `#![forbid(unsafe_code)]`
        // holds.

        // Prime the pipeline: request the first paged batch ahead of decoding.
        if more_rows && cursor_id != 0 {
            cursor
                .connection()
                .fetch_rows_request(cx, cursor_id, arraysize)
                .await?;
            #[cfg(feature = "tracing")]
            {
                // One page is now outstanding on the wire (look-ahead depth 1).
                prefetch_inflight_max = 1;
            }
        }

        while more_rows && cursor_id != 0 {
            // Read + decode the page whose request is already in flight.
            let result = cursor
                .connection()
                .fetch_rows_ref_response(cx, cursor_id, previous_row.as_deref())
                .await?;
            let next_more = result.more_rows;

            // Speculatively request the NEXT page BEFORE running the callback, so
            // its round trip overlaps this page's decode + the callback's work.
            // The request needs no data from `result`, so `result`'s buffer stays
            // alive and untouched across this send.
            if next_more {
                cursor
                    .connection()
                    .fetch_rows_request(cx, cursor_id, arraysize)
                    .await?;
            }

            // Snapshot ONLY the last row of the page as the next page's
            // duplicate-column seed; materializing every row to owned (the old
            // behaviour) would defeat the zero-copy fast path. `row_count()` is
            // the `for_each_row_ref` iteration count (both are `row_starts.len()`),
            // so this captures exactly the row the old "overwrite every iteration"
            // logic left in `last_owned`. Seed correctness is covered by the
            // duplicate-CLOB compression regression in `live_borrowed_fetch`.
            let row_count = result.batch.row_count();
            #[cfg(feature = "tracing")]
            {
                pages_fetched += 1;
                rows_streamed += row_count as u64;
                if next_more {
                    prefetch_inflight_max = prefetch_inflight_max.max(1);
                }
            }
            let mut last_owned: Option<Vec<Option<oracledb_protocol::thin::QueryValue>>> = None;
            let mut row_idx = 0usize;
            result.batch.for_each_row_ref(|row| {
                if row_idx + 1 == row_count {
                    last_owned = Some(
                        row.iter()
                            .map(|cell| cell.map(|v| v.to_owned_value()))
                            .collect(),
                    );
                }
                row_idx += 1;
                callback(row)
            })?;
            if let Some(last) = last_owned {
                previous_row = Some(last);
            }
            more_rows = next_more;
        }

        obs_record!(_stream_span, db.rows_streamed = rows_streamed);
        obs_record!(_stream_span, db.pages_fetched = pages_fetched);
        obs_record!(
            _stream_span,
            db.prefetch_inflight_max = prefetch_inflight_max
        );
        cursor.release();
        Ok(())
    }

    pub async fn define_and_fetch_rows_with_columns(
        &mut self,
        cx: &Cx,
        cursor_id: u32,
        arraysize: u32,
        define_columns: &[ColumnMetadata],
        previous_row: Option<&[Option<oracledb_protocol::thin::QueryValue>]>,
    ) -> Result<QueryResult> {
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_define_fetch_payload_with_seq(
            cursor_id,
            arraysize,
            seq_num,
            define_columns,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("DEFINE FETCH payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let response = self
            .read_response_cancellable(cx, !self.supports_end_of_response, |bytes| {
                response_complete(&parse_define_fetch_response_with_context_and_limits(
                    bytes,
                    capabilities,
                    define_columns,
                    previous_row,
                    limits,
                ))
            })
            .await?;
        trace_query_bytes("DEFINE FETCH response", &response);
        let result = parse_define_fetch_response_with_context_and_limits(
            &response,
            self.capabilities,
            define_columns,
            previous_row,
            self.protocol_limits,
        )
        .map_err(Error::from)?;
        if columns_have_lob_prefetch_fields(define_columns) {
            self.lob_prefetch_cursors.insert(cursor_id);
            if result.cursor_id != 0 {
                self.lob_prefetch_cursors.insert(result.cursor_id);
            }
        } else {
            self.lob_prefetch_cursors.remove(&cursor_id);
            if result.cursor_id != 0 {
                self.lob_prefetch_cursors.remove(&result.cursor_id);
            }
        }
        self.cursor_columns
            .insert(cursor_id, define_columns.to_vec());
        self.remember_cursor_columns(&result);
        Ok(result)
    }

    /// Fetch up to `max_rows` rows from a returned REF CURSOR — a
    /// `QueryValue::Cursor` OUT value, or an entry of
    /// [`QueryResult::implicit_resultsets`]. A returned cursor is
    /// self-describing (its column metadata travels with it as
    /// `CursorValue::columns`), so no manual cursor-id handling is required.
    /// The child result set's column metadata is on the returned
    /// [`QueryResult::columns`]; fetching is bounded by `max_rows`. Like the
    /// reference, the nested cursor's id is not independently closed — the
    /// server owns its lifecycle as part of the parent statement (reference
    /// `_add_cursor_to_close` skips nested cursors). python-oracledb makes you
    /// drive the cursor lifecycle by hand; this does it.
    pub async fn fetch_cursor(
        &mut self,
        cx: &Cx,
        cursor: &oracledb_protocol::thin::CursorValue,
        max_rows: usize,
    ) -> Result<QueryResult> {
        // Cap the per-fetch array size, but never ask the server for more rows
        // than the caller wants: a small `max_rows` must not drag a full batch
        // off the wire. (`.max(1)` keeps the degenerate `max_rows == 0` define
        // fetch valid; the result is still truncated to 0 below.)
        const ARRAYSIZE: usize = 100;
        let fetch_size =
            |fetched: usize| -> u32 { max_rows.saturating_sub(fetched).clamp(1, ARRAYSIZE) as u32 };
        let mut rows: Vec<Vec<Option<oracledb_protocol::thin::QueryValue>>> = Vec::new();
        // First fetch is a DEFINE-FETCH: it establishes the column buffers for
        // the cursor and returns the first batch.
        let mut batch = self
            .define_and_fetch_rows_with_columns(
                cx,
                cursor.cursor_id,
                fetch_size(0),
                &cursor.columns,
                None,
            )
            .await?;
        let mut more = batch.more_rows;
        let mut cid = if batch.cursor_id != 0 {
            batch.cursor_id
        } else {
            cursor.cursor_id
        };
        rows.append(&mut batch.rows);
        // Continuation fetches until the cursor drains or the bound is reached.
        while more && cid != 0 && rows.len() < max_rows {
            observe_cancellation_between_round_trips(cx)?;
            let previous_row = rows.last().cloned();
            let mut next = self
                .fetch_rows_with_columns(
                    cx,
                    cid,
                    fetch_size(rows.len()),
                    &cursor.columns,
                    previous_row.as_deref(),
                )
                .await?;
            more = next.more_rows;
            if next.cursor_id != 0 {
                cid = next.cursor_id;
            }
            rows.append(&mut next.rows);
        }
        rows.truncate(max_rows);
        self.release_cursor(cid);
        Ok(QueryResult {
            columns: cursor.columns.clone(),
            rows,
            ..Default::default()
        })
    }

    /// Fetch the metadata for an Oracle ADT type from the data dictionary. For an
    /// *object* type, returns its attributes in declaration order (`ALL_TYPE_ATTRS`),
    /// each with its Oracle type name and whether it is itself a nested object
    /// type. For a *collection* type (VARRAY / nested table), returns its element
    /// metadata in `collection_element` (`ALL_COLL_TYPES`). Pair with
    /// [`decode_object`] to turn a returned `QueryValue::Object` into structured
    /// values. `schema`/`type_name` are matched case-insensitively.
    pub async fn describe_object_type(
        &mut self,
        cx: &Cx,
        schema: &str,
        type_name: &str,
    ) -> Result<ObjectType> {
        let schema = schema.to_ascii_uppercase();
        let name = type_name.to_ascii_uppercase();
        let binds = || {
            vec![
                oracledb_protocol::thin::BindValue::Text(schema.clone()),
                oracledb_protocol::thin::BindValue::Text(name.clone()),
            ]
        };
        let row_text =
            |row: &[Option<oracledb_protocol::thin::QueryValue>], i: usize| -> Option<String> {
                match row.get(i) {
                    Some(Some(v)) => {
                        oracledb_protocol::thin::QueryValue::as_text(v).map(str::to_string)
                    }
                    _ => None,
                }
            };

        // A collection type (VARRAY / nested table) is described by its element
        // type, not by attributes — check ALL_COLL_TYPES first.
        let coll = self
            .execute_query_with_binds_core(
                cx,
                "select elem_type_name, elem_type_owner from all_coll_types \
                 where owner = :1 and type_name = :2",
                10,
                &binds(),
            )
            .await?;
        if let Some(row) = coll.rows.first() {
            return Ok(ObjectType {
                schema,
                name,
                attributes: Vec::new(),
                collection_element: Some(CollectionElement {
                    type_name: row_text(row, 0).unwrap_or_default(),
                    type_owner: row_text(row, 1),
                }),
            });
        }

        // High prefetch so every attribute row arrives in one batch. Oracle caps
        // a type at 1000 attributes, so this never truncates the metadata.
        let res = self
            .execute_query_with_binds_core(
                cx,
                "select attr_name, attr_type_name, attr_type_owner from all_type_attrs \
                 where owner = :1 and type_name = :2 order by attr_no",
                1000,
                &binds(),
            )
            .await?;
        let attributes: Vec<ObjectAttribute> = res
            .rows
            .iter()
            .map(|row| ObjectAttribute {
                name: row_text(row, 0).unwrap_or_default(),
                type_name: row_text(row, 1).unwrap_or_default(),
                type_owner: row_text(row, 2),
            })
            .collect();
        if attributes.is_empty() {
            return Err(Error::Protocol(
                oracledb_protocol::ProtocolError::UnsupportedFeature(
                    "object type not found or has no attributes",
                ),
            ));
        }
        Ok(ObjectType {
            schema,
            name,
            attributes,
            collection_element: None,
        })
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
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        let exec_options = ExecuteOptions::default()
            .with_max_string_size(self.capabilities.max_string_size)
            .with_cursor_id(cursor_id)
            .with_scrollable(true)
            .with_scroll_operation(true)
            .with_fetch_orientation(fetch_orientation)
            .with_fetch_pos(fetch_pos)
            .with_cache_statement(false);
        let piggyback = self.take_close_cursors_piggyback();
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let mut payload = build_execute_payload_with_bind_rows_and_options_with_seq(
            sql,
            arraysize,
            seq_num,
            true,
            &[],
            exec_options,
            self.capabilities.ttc_field_version,
        )?;
        if let Some(mut piggyback_bytes) = piggyback {
            piggyback_bytes.extend_from_slice(&payload);
            payload = piggyback_bytes;
        }
        trace_query_bytes("SCROLL payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        let known_columns = self
            .cursor_columns
            .get(&cursor_id)
            .cloned()
            .unwrap_or_default();
        // The classic probe is the same parse this site runs below, with its
        // result discarded (a scroll is an execute-shaped round trip).
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let response = self
            .core
            .read_data_response_flushing_out_binds_probed(
                cx,
                self.sdu,
                !self.supports_end_of_response,
                |bytes| {
                    response_complete(&parse_query_response_with_binds_options_columns_and_limits(
                        bytes,
                        capabilities,
                        &[],
                        exec_options,
                        &known_columns,
                        limits,
                    ))
                },
            )
            .await?;
        trace_query_bytes("SCROLL response", &response);
        let parsed = parse_query_response_with_binds_options_columns_and_limits(
            &response,
            self.capabilities,
            &[],
            exec_options,
            &known_columns,
            self.protocol_limits,
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
        // LOB read span (feature-gated, zero-cost when off). Carries the offset
        // and requested amount — never the locator bytes or the LOB data.
        let _span = obs_span!(
            "oracledb.lob",
            db.operation = "read",
            db.lob_offset = offset,
            db.lob_amount = amount,
        );
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_lob_read_payload_with_seq(
            locator,
            offset,
            amount,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("LOB READ payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let response = self
            .read_response_cancellable(cx, !self.supports_end_of_response, |bytes| {
                response_complete(&parse_lob_read_response_with_limits(
                    bytes,
                    capabilities,
                    locator,
                    limits,
                ))
            })
            .await?;
        trace_query_bytes("LOB READ response", &response);
        self.note_parse(parse_lob_read_response_with_limits(
            &response,
            self.capabilities,
            locator,
            self.protocol_limits,
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
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        self.protocol_limits.check_frame_bytes(queue.name.len())?;
        self.protocol_limits
            .check_frame_bytes(queue.payload_toid.len())?;
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
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        // Classic-aware read (pre-23ai has no END_OF_RESPONSE framing); bead
        // rust-oracledb-eyp7.
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let response = self
            .read_response_cancellable(cx, !self.supports_end_of_response, |bytes| {
                response_complete(&parse_aq_enq_response_with_limits(
                    bytes,
                    capabilities,
                    limits,
                ))
            })
            .await?;
        trace_query_bytes("AQ ENQ response", &response);
        self.note_parse(parse_aq_enq_response_with_limits(
            &response,
            self.capabilities,
            self.protocol_limits,
        ))
    }

    /// Dequeues a single AQ message (FUNC 122). Returns `None` when the queue is
    /// empty (ORA-25228 cleared server-side).
    pub async fn aq_deq_one(
        &mut self,
        cx: &Cx,
        queue: &AqQueueDesc,
        deq_options: &AqDeqOptions,
    ) -> Result<AqDeqResult> {
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        self.protocol_limits.check_frame_bytes(queue.name.len())?;
        self.protocol_limits
            .check_frame_bytes(queue.payload_toid.len())?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_aq_deq_payload(
            queue,
            deq_options,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("AQ DEQ payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        // Classic-aware read (pre-23ai has no END_OF_RESPONSE framing); bead
        // rust-oracledb-eyp7.
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let response = self
            .read_response_cancellable(cx, !self.supports_end_of_response, |bytes| {
                response_complete(&parse_aq_deq_response_with_limits(
                    bytes,
                    capabilities,
                    &queue.kind,
                    limits,
                ))
            })
            .await?;
        trace_query_bytes("AQ DEQ response", &response);
        self.note_parse(parse_aq_deq_response_with_limits(
            &response,
            self.capabilities,
            &queue.kind,
            self.protocol_limits,
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
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        self.protocol_limits.check_batch_rows(props_list.len())?;
        self.protocol_limits.check_frame_bytes(queue.name.len())?;
        self.protocol_limits
            .check_frame_bytes(queue.payload_toid.len())?;
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
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        // Classic-aware read (pre-23ai has no END_OF_RESPONSE framing); bead
        // rust-oracledb-eyp7.
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let response = self
            .read_response_cancellable(cx, !self.supports_end_of_response, |bytes| {
                response_complete(&parse_aq_array_response_with_limits(
                    bytes,
                    capabilities,
                    TNS_AQ_ARRAY_ENQ,
                    props_list.len() as u32,
                    &queue.kind,
                    limits,
                ))
            })
            .await?;
        trace_query_bytes("AQ ARRAY ENQ response", &response);
        let result: AqArrayResult = self.note_parse(parse_aq_array_response_with_limits(
            &response,
            self.capabilities,
            TNS_AQ_ARRAY_ENQ,
            props_list.len() as u32,
            &queue.kind,
            self.protocol_limits,
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
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        self.protocol_limits
            .check_batch_rows(max_num_messages as usize)?;
        self.protocol_limits.check_frame_bytes(queue.name.len())?;
        self.protocol_limits
            .check_frame_bytes(queue.payload_toid.len())?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_aq_array_deq_payload(
            queue,
            deq_options,
            max_num_messages,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("AQ ARRAY DEQ payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        // Classic-aware read (pre-23ai has no END_OF_RESPONSE framing); bead
        // rust-oracledb-eyp7.
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let response = self
            .read_response_cancellable(cx, !self.supports_end_of_response, |bytes| {
                response_complete(&parse_aq_array_response_with_limits(
                    bytes,
                    capabilities,
                    TNS_AQ_ARRAY_DEQ,
                    max_num_messages,
                    &queue.kind,
                    limits,
                ))
            })
            .await?;
        trace_query_bytes("AQ ARRAY DEQ response", &response);
        let result: AqArrayResult = self.note_parse(parse_aq_array_response_with_limits(
            &response,
            self.capabilities,
            TNS_AQ_ARRAY_DEQ,
            max_num_messages,
            &queue.kind,
            self.protocol_limits,
        ))?;
        Ok(result.deq_messages)
    }

    pub async fn create_temp_lob(
        &mut self,
        cx: &Cx,
        ora_type_num: u8,
        csfrm: u8,
    ) -> Result<LobReadResult> {
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_lob_create_temp_payload_with_seq(
            ora_type_num,
            csfrm,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("LOB CREATE TEMP payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let response = self
            .read_response_cancellable(cx, !self.supports_end_of_response, |bytes| {
                response_complete(&parse_lob_create_temp_response_with_limits(
                    bytes,
                    capabilities,
                    limits,
                ))
            })
            .await?;
        trace_query_bytes("LOB CREATE TEMP response", &response);
        self.note_parse(parse_lob_create_temp_response_with_limits(
            &response,
            self.capabilities,
            self.protocol_limits,
        ))
    }

    pub async fn write_lob(
        &mut self,
        cx: &Cx,
        locator: &[u8],
        offset: u64,
        data: &[u8],
    ) -> Result<LobReadResult> {
        // LOB write span (feature-gated, zero-cost when off). Carries the offset
        // and the byte count written — never the locator bytes or the LOB data.
        let _span = obs_span!(
            "oracledb.lob",
            db.operation = "write",
            db.lob_offset = offset,
            db.lob_bytes = data.len() as u64,
        );
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        self.protocol_limits.check_frame_bytes(locator.len())?;
        self.protocol_limits.check_frame_bytes(data.len())?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_lob_write_payload_with_seq(
            locator,
            offset,
            data,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("LOB WRITE payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let response = self
            .read_response_cancellable(cx, !self.supports_end_of_response, |bytes| {
                response_complete(&parse_lob_write_response_with_limits(
                    bytes,
                    capabilities,
                    locator,
                    limits,
                ))
            })
            .await?;
        trace_query_bytes("LOB WRITE response", &response);
        self.note_parse(parse_lob_write_response_with_limits(
            &response,
            self.capabilities,
            locator,
            self.protocol_limits,
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
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        self.protocol_limits.check_frame_bytes(locator.len())?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_lob_trim_payload_with_seq(
            locator,
            new_size,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("LOB TRIM payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let response = self
            .read_response_cancellable(cx, !self.supports_end_of_response, |bytes| {
                response_complete(&parse_lob_trim_response_with_limits(
                    bytes,
                    capabilities,
                    locator,
                    limits,
                ))
            })
            .await?;
        trace_query_bytes("LOB TRIM response", &response);
        self.note_parse(parse_lob_trim_response_with_limits(
            &response,
            self.capabilities,
            locator,
            self.protocol_limits,
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
        observe_cancellation_between_round_trips(cx)?;
        if locators.is_empty() {
            return Ok(());
        }
        self.ensure_clean_before_request().await?;
        self.protocol_limits.check_lob_chunks(locators.len())?;
        let returned_parameter_len = locators.iter().try_fold(0usize, |total, locator| {
            self.protocol_limits.check_frame_bytes(locator.len())?;
            total.checked_add(locator.len()).ok_or(
                oracledb_protocol::ProtocolError::ResourceLimit {
                    limit: "frame_bytes",
                    observed: usize::MAX,
                    maximum: self.protocol_limits.max_frame_bytes,
                },
            )
        })?;
        self.protocol_limits
            .check_frame_bytes(returned_parameter_len)?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_lob_free_temp_payload_with_seq(
            locators,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("LOB FREE TEMP payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let response = self
            .read_response_cancellable(cx, !self.supports_end_of_response, |bytes| {
                response_complete(&parse_lob_free_temp_response_with_limits(
                    bytes,
                    capabilities,
                    returned_parameter_len,
                    limits,
                ))
            })
            .await?;
        trace_query_bytes("LOB FREE TEMP response", &response);
        self.note_parse(parse_lob_free_temp_response_with_limits(
            &response,
            self.capabilities,
            returned_parameter_len,
            self.protocol_limits,
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

    #[allow(dead_code)]
    async fn execute_query_call_timeout(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        let Some(timeout_ms) = timeout_ms.filter(|value| *value > 0) else {
            return self
                .execute_query_with_bind_rows_and_options_core(
                    cx,
                    sql,
                    prefetch_rows,
                    &[],
                    ExecuteOptions::default(),
                )
                .await;
        };
        let deadline = QueryDeadline::from_timeout(Duration::from_millis(u64::from(timeout_ms)));
        match deadline
            .run(self.execute_query_with_bind_rows_and_options_core(
                cx,
                sql,
                prefetch_rows,
                &[],
                ExecuteOptions::default(),
            ))
            .await
        {
            Ok(result) => result,
            Err(DeadlineExpiry::BeforeStart) => self.reject_before_operation_start(cx, timeout_ms),
            Err(DeadlineExpiry::InFlight) => self.recover_from_call_timeout(cx, timeout_ms).await,
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
                .execute_query_with_binds_core(cx, sql, prefetch_rows, binds)
                .await;
        };
        let deadline = QueryDeadline::from_timeout(Duration::from_millis(u64::from(timeout_ms)));
        match deadline
            .run(self.execute_query_with_binds_core(cx, sql, prefetch_rows, binds))
            .await
        {
            Ok(result) => result,
            Err(DeadlineExpiry::BeforeStart) => self.reject_before_operation_start(cx, timeout_ms),
            Err(DeadlineExpiry::InFlight) => self.recover_from_call_timeout(cx, timeout_ms).await,
        }
    }

    #[allow(dead_code)]
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
                .execute_query_with_bind_rows_and_options_core(
                    cx,
                    sql,
                    prefetch_rows,
                    bind_rows,
                    ExecuteOptions::default(),
                )
                .await;
        };
        let deadline = QueryDeadline::from_timeout(Duration::from_millis(u64::from(timeout_ms)));
        match deadline
            .run(self.execute_query_with_bind_rows_and_options_core(
                cx,
                sql,
                prefetch_rows,
                bind_rows,
                ExecuteOptions::default(),
            ))
            .await
        {
            Ok(result) => result,
            Err(DeadlineExpiry::BeforeStart) => self.reject_before_operation_start(cx, timeout_ms),
            Err(DeadlineExpiry::InFlight) => self.recover_from_call_timeout(cx, timeout_ms).await,
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
        let deadline = QueryDeadline::from_timeout(Duration::from_millis(u64::from(timeout_ms)));
        match deadline
            .run(self.read_lob(cx, locator, offset, amount))
            .await
        {
            Ok(result) => result,
            Err(DeadlineExpiry::BeforeStart) => self.reject_before_operation_start(cx, timeout_ms),
            Err(DeadlineExpiry::InFlight) => self.recover_from_call_timeout(cx, timeout_ms).await,
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
        let deadline = QueryDeadline::from_timeout(Duration::from_millis(u64::from(timeout_ms)));
        match deadline
            .run(self.write_lob(cx, locator, offset, data))
            .await
        {
            Ok(result) => result,
            Err(DeadlineExpiry::BeforeStart) => self.reject_before_operation_start(cx, timeout_ms),
            Err(DeadlineExpiry::InFlight) => self.recover_from_call_timeout(cx, timeout_ms).await,
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
        let deadline = QueryDeadline::from_timeout(Duration::from_millis(u64::from(timeout_ms)));
        match deadline.run(self.trim_lob(cx, locator, new_size)).await {
            Ok(result) => result,
            Err(DeadlineExpiry::BeforeStart) => self.reject_before_operation_start(cx, timeout_ms),
            Err(DeadlineExpiry::InFlight) => self.recover_from_call_timeout(cx, timeout_ms).await,
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
        let deadline = QueryDeadline::from_timeout(Duration::from_millis(u64::from(timeout_ms)));
        match deadline.run(self.free_temp_lobs(cx, locators)).await {
            Ok(result) => result,
            Err(DeadlineExpiry::BeforeStart) => self.reject_before_operation_start(cx, timeout_ms),
            Err(DeadlineExpiry::InFlight) => self.recover_from_call_timeout(cx, timeout_ms).await,
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
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        self.protocol_limits.check_columns(column_names.len())?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = oracledb_protocol::dpl::build_direct_path_prepare_payload_with_version(
            schema_name,
            table_name,
            column_names,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("DIRECT PATH PREPARE payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        // Classic-aware read (pre-23ai has no END_OF_RESPONSE framing); bead
        // rust-oracledb-eyp7.
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let response = self
            .read_response_cancellable(cx, !self.supports_end_of_response, |bytes| {
                response_complete(
                    &oracledb_protocol::dpl::parse_direct_path_prepare_response_with_limits(
                        bytes,
                        capabilities,
                        limits,
                    ),
                )
            })
            .await?;
        trace_query_bytes("DIRECT PATH PREPARE response", &response);
        oracledb_protocol::dpl::parse_direct_path_prepare_response_with_limits(
            &response,
            self.capabilities,
            self.protocol_limits,
        )
        .map_err(Error::from)
    }

    /// Sends one direct path load stream message (TTC function 129).
    pub async fn direct_path_load_stream(
        &mut self,
        cx: &Cx,
        cursor_id: u16,
        stream: &oracledb_protocol::dpl::DirectPathStream,
    ) -> Result<()> {
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = oracledb_protocol::dpl::build_direct_path_load_stream_payload_with_version(
            cursor_id,
            stream,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("DIRECT PATH LOAD STREAM payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        // Classic-aware read (pre-23ai has no END_OF_RESPONSE framing); bead
        // rust-oracledb-eyp7.
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let response = self
            .read_response_cancellable(cx, !self.supports_end_of_response, |bytes| {
                response_complete(
                    &oracledb_protocol::dpl::parse_direct_path_simple_response_with_limits(
                        bytes,
                        capabilities,
                        limits,
                    ),
                )
            })
            .await?;
        trace_query_bytes("DIRECT PATH LOAD STREAM response", &response);
        oracledb_protocol::dpl::parse_direct_path_simple_response_with_limits(
            &response,
            self.capabilities,
            self.protocol_limits,
        )
        .map_err(Error::from)
    }

    /// Sends a direct path op message (TTC function 130).
    /// [`oracledb_protocol::dpl::TNS_DP_OP_FINISH`] commits the load
    /// server-side; [`oracledb_protocol::dpl::TNS_DP_OP_ABORT`] discards it.
    pub async fn direct_path_op(&mut self, cx: &Cx, cursor_id: u16, op_code: u32) -> Result<()> {
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = oracledb_protocol::dpl::build_direct_path_op_payload_with_version(
            cursor_id,
            op_code,
            seq_num,
            self.capabilities.ttc_field_version,
        );
        trace_query_bytes("DIRECT PATH OP payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        // Classic-aware read (pre-23ai has no END_OF_RESPONSE framing); bead
        // rust-oracledb-eyp7.
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let response = self
            .read_response_cancellable(cx, !self.supports_end_of_response, |bytes| {
                response_complete(
                    &oracledb_protocol::dpl::parse_direct_path_simple_response_with_limits(
                        bytes,
                        capabilities,
                        limits,
                    ),
                )
            })
            .await?;
        trace_query_bytes("DIRECT PATH OP response", &response);
        oracledb_protocol::dpl::parse_direct_path_simple_response_with_limits(
            &response,
            self.capabilities,
            self.protocol_limits,
        )
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
        // Verify all row widths match the column metadata before sending
        // anything (reference `_verify_metadata` rejects a width mismatch before
        // the first stream message). `batch_size` is a chunking *upper bound*,
        // not a row count: `BatchLoadState` clamps it to the data length per
        // batch (`calculate_num_rows_in_batch`), and the reference imposes no
        // cap on `batch_size` (its default is the `2**32 - 1` "all rows"
        // sentinel; oversized data is piece-streamed). So it must NOT be
        // limit-checked here — doing so spuriously raised "protocol resource
        // limit exceeded" for the default sentinel. The genuine per-execute row
        // caps remain on the array-DML / object paths, which pass real counts.
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
            observe_cancellation_between_round_trips(cx)?;
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

    /// On a call timeout, send a BREAK and drain the server's in-flight
    /// response so the wire stream is left clean and the connection stays
    /// reusable — the parity-faithful recovery python-oracledb performs before
    /// raising `DPY-4024` (`_break_external` + `_receive_packet`/`_reset`,
    /// protocol.pyx:449-451). Delegates to [`break_and_drain_wire`].
    ///
    /// `Ok(())` means the drain succeeded and the connection is usable; the
    /// caller then returns [`Error::CallTimeout`]. If the drain fails (a second
    /// timeout or a wire error), the connection is marked [`Self::dead`] and the
    /// returned [`Error::ConnectionClosed`] is propagated instead — mirroring
    /// the reference's disconnect-on-second-timeout (protocol.pyx:454-458).
    async fn break_and_drain(&mut self) -> Result<()> {
        self.core.recovery.begin_drain_after_break()?;
        match self.core.break_and_drain_wire(BREAK_DRAIN_RECOVERY_TIMEOUT) {
            Ok(()) => {
                self.core.recovery.finish_drain_ready();
                Ok(())
            }
            Err(err) => {
                // Recovery failed: the stream is poisoned, the connection is
                // dead. Pools must discard it (see `is_dead` / `is_connection_lost`).
                self.core.recovery.mark_dead();
                self.dead = true;
                Err(err)
            }
        }
    }

    /// Reject an operation whose deadline elapsed before its future was ever
    /// polled. No request bytes can exist, so sending BREAK here would corrupt
    /// an idle session; structured context cancellation still determines the
    /// public error and whether the connection remains reusable.
    pub(crate) fn reject_before_operation_start<T>(
        &mut self,
        cx: &Cx,
        timeout_ms: u32,
    ) -> Result<T> {
        let disposition = cx
            .cancel_reason()
            .map(|reason| CancelDisposition::from_kind(reason.kind))
            .unwrap_or(CancelDisposition::Timeout);
        if disposition == CancelDisposition::Close {
            self.core.recovery.mark_dead();
            self.dead = true;
        }
        Err(disposition.into_error(timeout_ms))
    }

    /// Common tail for every `*_call_timeout` arm: the in-flight operation hit
    /// its deadline (the user's `call_timeout`) or the caller's `Cx` was
    /// cancelled, so break + drain the wire and then surface the right error.
    ///
    /// The drain is unconditional — whatever the reason, the cancelled call's
    /// in-flight bytes must be cleared off the socket before the connection can
    /// be reused or discarded cleanly. After a clean drain we branch on the
    /// asupersync [`CancelKind`] (via [`CancelDisposition`]) to flatten to the
    /// right public error:
    ///
    /// * **No `Cx` cancel recorded** (a pure `call_timeout` deadline): the
    ///   classic [`Error::CallTimeout`] (`DPY-4024`) — the session survives and
    ///   the error is connection-reusable + retryable.
    /// * **Timeout/deadline/quota cancel**: same — [`Error::CallTimeout`].
    /// * **Shutdown / resource-loss cancel** ([`CancelDisposition::Close`]):
    ///   even though the drain succeeded, the runtime is going away, so the
    ///   connection is marked **dead** and [`Error::ConnectionClosed`] is
    ///   surfaced — the caller must discard it.
    /// * **Explicit / topological cancel** ([`CancelDisposition::Cancel`]): the
    ///   distinct [`Error::Cancelled`] (`ORA-01013`), connection-reusable.
    ///
    /// On a **failed** drain the connection is already dead and
    /// [`Error::ConnectionClosed`] (`DPY-4011`) is propagated regardless of the
    /// cancel kind. Always returns `Err`, so it composes as the `Err(_)` branch
    /// of the timeout `match`.
    pub(crate) async fn recover_from_call_timeout<T>(
        &mut self,
        cx: &Cx,
        timeout_ms: u32,
    ) -> Result<T> {
        match self.break_and_drain().await {
            Ok(()) => {
                // This arm is reached because a deadline elapsed. If the `Cx`
                // also carries a structured cancel, its kind drives the
                // disposition; otherwise (no recorded cancel) it is a pure
                // `call_timeout` deadline, which is the `Timeout` disposition —
                // NOT the generic `Cancel` fallback used at the between-round-
                // trip checkpoint boundary.
                let disposition = cx
                    .cancel_reason()
                    .map(|reason| CancelDisposition::from_kind(reason.kind))
                    .unwrap_or(CancelDisposition::Timeout);
                if disposition == CancelDisposition::Close {
                    // Drain left the wire clean, but a runtime shutdown means the
                    // connection must not be handed back to the pool.
                    self.core.recovery.mark_dead();
                    self.dead = true;
                }
                Err(disposition.into_error(timeout_ms))
            }
            Err(closed) => Err(closed),
        }
    }

    /// Cleans the wire after a two-thread cancel: a [`CancelHandle`] on another
    /// thread already sent the BREAK while this thread was blocked in a query, so
    /// the socket now holds the full multi-stage cancel response. Drains it with
    /// the SAME machinery the call-timeout path uses ([`drain_cancel_wire`] ->
    /// [`drain_break_response_recovery`]) — the cancelled call's in-flight DATA
    /// response, the break-ack MARKER, the RESET handshake, and the trailing
    /// ORA-01013 — leaving the connection clean and reusable.
    ///
    /// Before this used the proper drain it ran a single `read_data_response`
    /// that stopped at the in-flight response's end-of-response boundary, leaking
    /// the MARKER + ORA-01013 into the socket where the NEXT operation misread
    /// them (bead rust-oracledb-wnz). A failed drain marks the connection dead
    /// and surfaces [`Error::ConnectionClosed`].
    async fn drain_cancel_response(&mut self) -> Result<()> {
        self.core.recovery.begin_drain_after_break()?;
        match self.core.drain_cancel_wire(BREAK_DRAIN_RECOVERY_TIMEOUT) {
            Ok(()) => {
                self.core.recovery.finish_drain_ready();
                Ok(())
            }
            Err(err) => {
                self.core.recovery.mark_dead();
                self.dead = true;
                Err(err)
            }
        }
    }

    /// Explicitly cancel the in-flight operation on this connection and leave the
    /// connection in a clean, reusable state.
    ///
    /// This sends a BREAK to the server and then **drains** the entire cancel
    /// response (any in-flight DATA response of the cancelled call, the break-ack
    /// MARKER, the RESET handshake, and the trailing `ORA-01013`) so the wire is
    /// left at a clean message boundary — exactly the recovery python-oracledb
    /// performs in `Connection.cancel()` (`_break_external()` + `_reset()`,
    /// protocol.pyx:533-557). The reference would send an out-of-band urgent-TCP
    /// break when `supports_oob` is negotiated and fall back to this in-band
    /// BREAK marker otherwise (protocol.pyx:56-69); asupersync's transport does
    /// not expose `MSG_OOB`, so the portable in-band path is always taken (the
    /// server handles it identically — see [`Self::supports_oob`]).
    ///
    /// On success the connection is **usable for the next operation** (the cancel
    /// mirrors `DPY-4024` semantics: the session is alive, the wire is clean).
    /// `Ok(())` means the cancel completed and the connection survives. If the
    /// drain fails (a second timeout or a wire error) the connection is marked
    /// dead and [`Error::ConnectionClosed`] is returned instead.
    ///
    /// Unlike [`Self::cancel_handle`] (which only fires a bare BREAK from another
    /// thread, leaving the owner-side drain to private recovery machinery), this
    /// is the single-call, self-contained cancel: break **and** drain in one
    /// place.
    pub async fn cancel(&mut self, _cx: &Cx) -> Result<()> {
        self.core.recovery.begin_drain_after_break()?;
        match self
            .core
            .cancel_and_drain_wire(BREAK_DRAIN_RECOVERY_TIMEOUT)
        {
            Ok(()) => {
                self.core.recovery.finish_drain_ready();
                Ok(())
            }
            Err(err) => {
                self.core.recovery.mark_dead();
                self.dead = true;
                Err(err)
            }
        }
    }

    /// Whether the server negotiated out-of-band (urgent-TCP) break support at
    /// accept time (`protocol_options & TNS_GSO_CAN_RECV_ATTENTION`, the
    /// reference `Capabilities.supports_oob`, capabilities.pyx:120). This driver
    /// always uses the in-band BREAK marker for [`Self::cancel`] regardless,
    /// because asupersync's `TcpStream` does not expose `send(MSG_OOB)`; the bit
    /// is surfaced for diagnostics and parity with the reference capability
    /// negotiation. The server accepts the in-band break on every connection.
    pub fn supports_oob(&self) -> bool {
        self.supports_oob
    }

    fn remember_cursor_columns(&mut self, result: &QueryResult) {
        if result.cursor_id != 0 && !result.columns.is_empty() {
            // On a statement-cache hit the same cursor re-executes with identical
            // columns, so the map already holds an equal value. Cloning the
            // `Vec<ColumnMetadata>` (and each column's `name`/object/domain
            // `String`) every call would be pure waste on that hot path. Skip the
            // clone when the cached value already matches; the map ends with the
            // same content either way (behavior-preserving). The equality check is
            // a cheap field compare versus the String allocations a clone makes.
            if self.cursor_columns.get(&result.cursor_id) == Some(&result.columns) {
                return;
            }
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
        // Cross-connection statement-shape observation (bead a4-8pp): record the
        // freshly-described shape in the shared cache. If another connection
        // changed the shape since it was last seen (a concurrent DDL), self-heal
        // by dropping THIS connection's retained per-SQL fetch metadata so the
        // adjust below cannot re-define the fresh columns toward the now-stale
        // shape. The decode itself always uses the live `result.columns`, so it
        // is never stale; the cache only forces a re-describe when the shape
        // drifted. Self-heal only ever invalidates (heals down), never loosens.
        if self.shape_cache.observe(sql, &result.columns).self_healed {
            self.forget_fetch_metadata(sql);
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
                observe_cancellation_between_round_trips(cx)?;
                let cursor_id = result.cursor_id;
                let redefined_result = self
                    .define_and_fetch_rows_with_columns(
                        cx,
                        cursor_id,
                        arraysize.max(1),
                        &adjusted,
                        None,
                    )
                    .await;
                let mut redefined = self.close_cursor_on_error(cursor_id, redefined_result)?;
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
    ///
    /// A cached cursor whose recorded bind TYPE shape is incompatible with
    /// `bind_shape` is dropped (queued for the close-cursors piggyback) and
    /// `None` is returned, forcing a fresh PARSE with the new bind metadata:
    /// re-executing it would make the server coerce the new values through
    /// the stale parsed types (bead rust-oracledb-ilel, ORA-01722).
    fn statement_cache_get(&mut self, sql: &str, bind_shape: &[BindShapeSlot]) -> Option<u32> {
        let index = self
            .statement_cache
            .iter()
            .position(|entry| entry.sql == sql)?;
        let cursor_id = self.statement_cache[index].cursor_id;
        if cursor_id != 0 && self.in_use_cursors.contains(&cursor_id) {
            return None;
        }
        if !bind_shape_is_compatible(&self.statement_cache[index].bind_shape, bind_shape) {
            self.statement_cache_invalidate(sql, cursor_id);
            return None;
        }
        let entry = self.statement_cache.remove(index);
        self.statement_cache.push(entry);
        Some(cursor_id)
    }

    /// Removes and returns the open cursor for the SQL text; used when the
    /// caller requested `cache_statement=False` but the statement is still
    /// present from an earlier cached execution (reference `_get_statement`
    /// pops from the cache unconditionally). A bind-shape mismatch drops the
    /// cursor instead of handing it out (same rule as
    /// [`Self::statement_cache_get`]).
    fn statement_cache_take(&mut self, sql: &str, bind_shape: &[BindShapeSlot]) -> Option<u32> {
        let index = self
            .statement_cache
            .iter()
            .position(|entry| entry.sql == sql)?;
        if !bind_shape_is_compatible(&self.statement_cache[index].bind_shape, bind_shape) {
            let cursor_id = self.statement_cache[index].cursor_id;
            self.statement_cache_invalidate(sql, cursor_id);
            return None;
        }
        Some(self.statement_cache.remove(index).cursor_id)
    }

    /// Stores/updates the open cursor for the SQL text along with the bind
    /// TYPE shape it was bound with, evicting the least recently used entry
    /// into the close-cursors piggyback queue (reference
    /// `_statement_cache.return_statement`).
    fn statement_cache_put(&mut self, sql: &str, cursor_id: u32, bind_shape: Vec<BindShapeSlot>) {
        let to_close = statement_cache_insert(
            &mut self.statement_cache,
            self.statement_cache_size,
            sql,
            cursor_id,
            bind_shape,
        );
        for cursor_id in &to_close {
            self.lob_prefetch_cursors.remove(cursor_id);
            self.cursor_columns.remove(cursor_id);
        }
        self.cursors_to_close.extend(to_close);
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
                        .retain(|entry| entry.cursor_id != *cursor_id);
                    self.cursor_columns.remove(cursor_id);
                    self.lob_prefetch_cursors.remove(cursor_id);
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
            self.lob_prefetch_cursors.remove(&cursor_id);
        }
    }

    /// Apply the fail-closed lifecycle rule for an operation on an already-open
    /// cursor: a successful result keeps ownership unchanged, while an error
    /// retires the cursor because its server-side validity is no longer proven.
    /// Callers that prove an operation never started must use `release_cursor`
    /// directly instead, preserving a valid cached cursor for reuse.
    pub(crate) fn close_cursor_on_error<T>(
        &mut self,
        cursor_id: u32,
        result: Result<T>,
    ) -> Result<T> {
        if result.is_err() {
            self.close_cursor(cursor_id);
        }
        result
    }

    /// Queue an open server cursor to be closed on the next round trip
    /// (reference `_add_cursor_to_close`). Unlike [`Self::release_cursor`],
    /// which returns a cached cursor to the statement cache for reuse, this
    /// drops the cursor entirely: its id is sent in the close-cursors piggyback
    /// that rides the next execute, any statement-cache entry pointing at the
    /// id is evicted, and its retained describe metadata is forgotten. Use this
    /// for a non-cached cursor (for example one opened by [`Self::execute_raw`])
    /// once its result is fully consumed, or for any cursor whose validity was
    /// lost after a failed operation. A cursor id of `0` is ignored.
    pub fn close_cursor(&mut self, cursor_id: u32) {
        if cursor_id == 0 {
            return;
        }
        self.statement_cache
            .retain(|entry| entry.cursor_id != cursor_id);
        self.in_use_cursors.remove(&cursor_id);
        self.copied_cursors.remove(&cursor_id);
        self.cursor_columns.remove(&cursor_id);
        self.lob_prefetch_cursors.remove(&cursor_id);
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
            .find(|entry| entry.sql == sql)
            .is_some_and(|entry| {
                entry.cursor_id != 0 && self.in_use_cursors.contains(&entry.cursor_id)
            })
    }

    /// Drops the cached cursor for the SQL text after a server error so the
    /// next execute re-parses (reference `_statement_cache.clear_cursor`).
    fn statement_cache_invalidate(&mut self, sql: &str, cursor_id: u32) {
        if let Some(index) = self
            .statement_cache
            .iter()
            .position(|entry| entry.sql == sql)
        {
            self.statement_cache.remove(index);
        }
        if cursor_id != 0 {
            self.cursors_to_close.push(cursor_id);
            self.cursor_columns.remove(&cursor_id);
            self.lob_prefetch_cursors.remove(&cursor_id);
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
            self.capabilities.ttc_field_version,
        ))
    }

    /// Log off and close the connection, consuming it. Any uncommitted
    /// transaction is rolled back by the server.
    pub async fn close(mut self, cx: &Cx) -> Result<()> {
        observe_cancellation_between_round_trips(cx)?;
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
                let _ = self.core.write_all(cx, &eof).await;
                let _ = self.core.shutdown_write(cx).await;
                return Ok(());
            }
        }
        self.ensure_clean_before_request().await?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        self.core
            .send_data_packet(
                cx,
                &build_function_payload_with_seq(
                    TNS_FUNC_LOGOFF,
                    seq_num,
                    self.capabilities.ttc_field_version,
                ),
                self.sdu,
            )
            .await?;
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        if let Ok(response) = time::timeout(
            time::wall_now(),
            Duration::from_secs(5),
            self.core
                .read_data_response_probed(cx, !self.supports_end_of_response, |bytes| {
                    response_complete(&parse_plain_function_response_with_limits(
                        bytes,
                        capabilities,
                        limits,
                    ))
                }),
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
        self.core.write_all(cx, &eof).await?;
        let _ = self.core.shutdown_write(cx).await;
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
        // Pipelining is defined in terms of END_OF_RESPONSE boundary framing
        // (impl/thin/capabilities.pyx:126-130); a pre-23ai server that did not
        // negotiate it cannot delimit the N+1 pipelined responses, so fail
        // closed instead of hanging on the first boundary read.
        if !self.supports_end_of_response {
            return Err(Error::Protocol(
                oracledb_protocol::ProtocolError::UnsupportedFeature(
                    "pipelining requires END_OF_RESPONSE framing, which this server \
                     did not negotiate (requires Oracle Database 23ai or later)",
                ),
            ));
        }
        observe_cancellation_between_round_trips(cx)?;
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_clean_before_request().await?;
        self.protocol_limits
            .check_length_prefixed_elements(requests.len())?;
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
                // Flush any pending close-cursors piggyback on the first op so
                // server cursors retired since the last round trip (evicted from
                // the statement cache or released by a closed cursor) are
                // actually closed — otherwise a sequence of pipelines that open
                // query cursors leaks them server-side (ORA-01000). The
                // reference likewise rides queued closes as a piggyback on the
                // next message; it is consumed before the begin-pipeline
                // piggyback's sequence number, matching the ordinary execute
                // path where the close-cursors piggyback is prepended first.
                if let Some(close_piggyback) = self.take_close_cursors_piggyback() {
                    payload.extend_from_slice(&close_piggyback);
                }
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
                } => {
                    self.protocol_limits.check_batch_rows(bind_rows.len())?;
                    if let Some(first_row) = bind_rows.first() {
                        self.protocol_limits.check_binds(first_row.len())?;
                    }
                    payload.extend_from_slice(
                        &build_execute_payload_with_bind_rows_and_options_with_seq(
                            sql,
                            *prefetch_rows,
                            seq_num,
                            statement_is_query(sql),
                            bind_rows,
                            ExecuteOptions::default()
                                .with_token_num(token_num)
                                .with_max_string_size(self.capabilities.max_string_size),
                            self.capabilities.ttc_field_version,
                        )?,
                    );
                }
                PipelineRequest::Commit => {
                    payload.extend_from_slice(&build_function_payload_with_seq_and_token(
                        TNS_FUNC_COMMIT,
                        seq_num,
                        token_num,
                        self.capabilities.ttc_field_version,
                    ));
                }
            }
            trace_query_bytes("PIPELINE op payload", &payload);
            self.core
                .send_data_packet_with_flags(
                    cx,
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
        self.core
            .send_data_packet(cx, &end_payload, self.sdu)
            .await?;
        let mut responses = Vec::with_capacity(requests.len() + 1);
        for _ in 0..=requests.len() {
            let response = self.core.read_data_response_boundary(cx, true).await?;
            trace_query_bytes("PIPELINE response", &response.payload);
            responses.push(response.payload);
        }
        Ok(responses)
    }

    /// Runs a batch as a true single-round-trip pipeline (like [`Self::run_pipeline`])
    /// and decodes each per-operation response into a [`QueryResult`], reusing
    /// the same `parse_query_response_*` decoders the ordinary execute path uses
    /// — no result-layer reimplementation. The end-pipeline response (the N+1th
    /// raw payload) is consumed for framing but not returned.
    ///
    /// Each operation is decoded with its own bind row and prefetch (carried by
    /// [`PipelineRequest::Execute`]); the returned vector has one entry per
    /// request, in token order. A per-operation server error is captured as
    /// `Err` for that slot rather than aborting the batch, so the caller can
    /// implement both abort-on-error and continue-on-error semantics over the
    /// decoded results (the wire batch already ran to completion — the server
    /// answers every message in both pipeline modes).
    ///
    /// The connection's `txn_in_progress` flag is refreshed from the last
    /// successfully decoded operation that carried an end-of-call STATUS, so a
    /// pipeline ending in commit/DML leaves the flag consistent with the
    /// sequential path (test_7614). No extra round trips are issued here: a
    /// query whose rows did not all fit in the prefetch returns its open
    /// `cursor_id` + `columns` + `more_rows` in the [`QueryResult`] so the
    /// caller can finish the fetch over the ordinary public cursor API, exactly
    /// as the reference `_complete_pipeline_op` does.
    pub async fn run_pipeline_decoded(
        &mut self,
        cx: &Cx,
        requests: &[PipelineRequest],
        continue_on_error: bool,
    ) -> Result<Vec<Result<QueryResult>>> {
        let raw = self.run_pipeline(cx, requests, continue_on_error).await?;
        // raw has requests.len() + 1 entries (the last is the end-pipeline
        // response, consumed for framing only).
        let mut decoded = Vec::with_capacity(requests.len());
        for (index, request) in requests.iter().enumerate() {
            let payload = &raw[index];
            let outcome = match request {
                PipelineRequest::Commit => {
                    // A commit op answers with a plain function response; decode
                    // it the same way the standalone commit path does so the
                    // txn-in-progress bit is sampled identically.
                    match parse_plain_function_response_with_limits(
                        payload,
                        self.capabilities,
                        self.protocol_limits,
                    ) {
                        Ok(txn_in_progress) => Ok(QueryResult {
                            txn_in_progress: Some(txn_in_progress),
                            ..QueryResult::default()
                        }),
                        Err(err) => Err(Error::Protocol(err)),
                    }
                }
                PipelineRequest::Execute { sql, bind_rows, .. } => {
                    parse_query_response_with_binds_options_columns_and_limits(
                        payload,
                        self.capabilities,
                        bind_rows.first().map(Vec::as_slice).unwrap_or(&[]),
                        ExecuteOptions::default(),
                        &[],
                        self.protocol_limits,
                    )
                    .map_err(Error::Protocol)
                    .inspect(|result| {
                        // Track open query cursors so a later op or a follow-up
                        // fetch on this connection does not collide with them.
                        self.remember_cursor_columns(result);
                        if result.cursor_id != 0 && statement_is_query(sql) {
                            self.in_use_cursors.insert(result.cursor_id);
                        }
                    })
                }
            };
            // Refresh txn-in-progress from any op that carried a STATUS message,
            // mirroring the sequential per-op execute bookkeeping.
            if let Ok(result) = &outcome {
                if let Some(txn_in_progress) = result.txn_in_progress {
                    self.txn_in_progress = txn_in_progress;
                }
            }
            decoded.push(outcome);
        }
        Ok(decoded)
    }

    async fn send_function(&mut self, cx: &Cx, function_code: u8) -> Result<()> {
        observe_cancellation_between_round_trips(cx)?;
        self.ensure_clean_before_request().await?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        self.core
            .send_data_packet(
                cx,
                &build_function_payload_with_seq(
                    function_code,
                    seq_num,
                    self.capabilities.ttc_field_version,
                ),
                self.sdu,
            )
            .await?;
        let capabilities = self.capabilities;
        let limits = self.protocol_limits;
        let response = self
            .read_response_cancellable(cx, !self.supports_end_of_response, |bytes| {
                response_complete(&parse_plain_function_response_with_limits(
                    bytes,
                    capabilities,
                    limits,
                ))
            })
            .await?;
        // Surface server errors (e.g. ORA-01012 after a killed session) that
        // arrive on plain function round trips; pool ping health checks and
        // commit/rollback depend on these not being silently swallowed. The
        // returned bit refreshes `txn_in_progress` from the wire end-of-call
        // status (reference protocol.pyx `_process_call_status`).
        let txn_in_progress = self.note_parse(parse_plain_function_response_with_limits(
            &response,
            self.capabilities,
            self.protocol_limits,
        ))?;
        self.txn_in_progress = txn_in_progress;
        Ok(())
    }

    /// Mark CLOB/BLOB result columns that actually hold JSON.
    ///
    /// A column described over the wire as CLOB or BLOB can be a JSON column
    /// (`IS JSON` storage); the fetch metadata does not say so directly. For each
    /// such candidate this runs a catalog probe against `ALL_JSON_COLUMNS` in the
    /// current schema and flips `is_json` when the column is registered as JSON.
    /// Columns already flagged JSON, non-LOB columns, and unnamed (expression)
    /// columns are skipped. `timeout_ms` bounds each probe (the call timeout).
    ///
    /// The reference fires the same `ALL_JSON_COLUMNS` lookup after describing a
    /// LOB result column so JSON-in-LOB values decode correctly.
    pub async fn supplement_json_column_metadata(
        &mut self,
        cx: &Cx,
        columns: &mut [ColumnMetadata],
        timeout_ms: Option<u32>,
    ) -> Result<()> {
        let candidates = json_lob_probe_candidates(columns);
        if candidates.is_empty() {
            return Ok(());
        }
        for (index, column_name) in candidates {
            let result = self
                .execute_query_with_binds_call_timeout(
                    cx,
                    "select 1 \
                     from all_json_columns \
                     where owner = sys_context('USERENV', 'CURRENT_SCHEMA') \
                       and column_name = :1",
                    1,
                    &[BindValue::Text(column_name)],
                    timeout_ms,
                )
                .await?;
            if !result.rows.is_empty() {
                columns[index] = columns[index].clone().with_is_json(true);
            }
        }
        Ok(())
    }
}

impl CancelHandle {
    /// Request cancellation of the connection operation currently in flight.
    ///
    /// The blocking facade for synchronous callers is [`Self::cancel_blocking`];
    /// Rust cannot overload that zero-argument wrapper with this `&Cx` form.
    ///
    /// This is request-only: it sends the BREAK marker and records that recovery
    /// is pending, but the connection owner remains responsible for draining the
    /// cancel response and reconciling the session back to Ready or Dead.
    pub async fn cancel(&mut self, cx: &Cx) -> Result<()> {
        observe_cancellation_between_round_trips(cx)?;
        if !self.should_send_break_request()? {
            return Ok(());
        }
        let mut write = lock_write(cx, &self.write).await?;
        if !self.should_send_break_request()? {
            return Ok(());
        }
        match send_marker(&mut *write, TNS_MARKER_TYPE_BREAK).await {
            Ok(()) => self.recovery.mark_break_sent(),
            Err(err) => {
                self.recovery.mark_dead();
                Err(err)
            }
        }
    }

    fn should_send_break_request(&self) -> Result<bool> {
        match self.recovery.phase() {
            SessionRecoveryPhase::Dead => {
                Err(Error::ConnectionClosed("connection is closed".into()))
            }
            SessionRecoveryPhase::BreakSent | SessionRecoveryPhase::Draining => Ok(false),
            SessionRecoveryPhase::Ready | SessionRecoveryPhase::InFlight => Ok(true),
        }
    }

    /// Blocking facade for synchronous callers.
    pub fn cancel_blocking(&mut self) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            self.cancel(&cx).await
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
///
/// # fn main() -> Result<(), oracledb::Error> {
/// let identity = ClientIdentity::new("svc", "host", "user", "term", "rust-oracledb")?;
/// let mut conn = BlockingConnection::connect(
///     ConnectOptions::new("dbhost:1521/FREEPDB1", "app", "pw", identity),
/// )?;
/// let row = BlockingConnection::query_one(&mut conn, "select 1 from dual", ())?;
/// let value: i64 = row.get(0)?;
/// assert_eq!(value, 1);
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

    /// Blocking wrapper for [`Connection::cancel`].
    pub fn cancel(connection: &mut Connection) -> Result<()> {
        block_on_io(|cx| async move { connection.cancel(&cx).await })
    }

    pub fn commit(connection: &mut Connection) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.commit(&cx).await
        })
    }

    /// Register a CQN subscription (FUNC 125, opcode 1). See
    /// [`Connection::subscribe_register`].
    #[allow(clippy::too_many_arguments)]
    pub fn subscribe_register(
        connection: &mut Connection,
        namespace: u32,
        name: Option<&str>,
        public_qos: u32,
        operations: u32,
        timeout: u32,
        grouping_class: u8,
        grouping_value: u32,
        grouping_type: u8,
    ) -> Result<SubscribeResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .subscribe_register(
                    &cx,
                    namespace,
                    name,
                    public_qos,
                    operations,
                    timeout,
                    grouping_class,
                    grouping_value,
                    grouping_type,
                )
                .await
        })
    }

    /// Unregister a CQN subscription (FUNC 125, opcode 2). See
    /// [`Connection::subscribe_unregister`].
    #[allow(clippy::too_many_arguments)]
    pub fn subscribe_unregister(
        connection: &mut Connection,
        registration_id: u64,
        client_id: &[u8],
        namespace: u32,
        name: Option<&str>,
        public_qos: u32,
        operations: u32,
        timeout: u32,
        grouping_class: u8,
        grouping_value: u32,
        grouping_type: u8,
    ) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .subscribe_unregister(
                    &cx,
                    registration_id,
                    client_id,
                    namespace,
                    name,
                    public_qos,
                    operations,
                    timeout,
                    grouping_class,
                    grouping_value,
                    grouping_type,
                )
                .await
        })
    }

    /// Send the blocking CQN NOTIFY registration message. See
    /// [`Connection::notify_register`].
    pub fn notify_register(connection: &mut Connection, client_id: &[u8]) -> Result<()> {
        block_on_io(|cx| async move { connection.notify_register(&cx, client_id).await })
    }

    /// Blocking wrapper for [`Connection::recv_notification`].
    pub fn recv_notification(
        connection: &mut Connection,
        namespace: u32,
        public_qos: u32,
        read_timeout: Duration,
    ) -> Result<NotificationOutcome> {
        block_on_io(|cx| async move {
            connection
                .recv_notification(&cx, namespace, public_qos, read_timeout)
                .await
        })
    }

    /// Blocking wrapper for [`Connection::register_query`].
    pub fn register_query<'r>(
        connection: &mut Connection,
        registration: Registration<'r>,
    ) -> Result<RegistrationOutcome> {
        block_on_io(|cx| async move { connection.register_query(&cx, registration).await })
    }

    pub fn rollback(connection: &mut Connection) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.rollback(&cx).await
        })
    }

    /// Blocking wrapper for [`Connection::enable_dbms_output`].
    pub fn enable_dbms_output(
        connection: &mut Connection,
        buffer_bytes: Option<u32>,
    ) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.enable_dbms_output(&cx, buffer_bytes).await
        })
    }

    /// Blocking wrapper for [`Connection::read_dbms_output`].
    pub fn read_dbms_output(
        connection: &mut Connection,
        max_lines: usize,
        max_chars: usize,
    ) -> Result<DbmsOutput> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.read_dbms_output(&cx, max_lines, max_chars).await
        })
    }

    /// Blocking wrapper for [`Connection::fetch_cursor`].
    pub fn fetch_cursor(
        connection: &mut Connection,
        cursor: &oracledb_protocol::thin::CursorValue,
        max_rows: usize,
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.fetch_cursor(&cx, cursor, max_rows).await
        })
    }

    /// Blocking wrapper for [`Connection::describe_object_type`].
    pub fn describe_object_type(
        connection: &mut Connection,
        schema: &str,
        type_name: &str,
    ) -> Result<ObjectType> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .describe_object_type(&cx, schema, type_name)
                .await
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

    #[allow(clippy::too_many_arguments)]
    pub fn tpc_begin(
        connection: &mut Connection,
        format_id: u32,
        global_transaction_id: &[u8],
        branch_qualifier: &[u8],
        flags: u32,
        timeout: u32,
    ) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .tpc_begin(
                    &cx,
                    format_id,
                    global_transaction_id,
                    branch_qualifier,
                    flags,
                    timeout,
                )
                .await
        })
    }

    pub fn tpc_end(
        connection: &mut Connection,
        xid: Option<(u32, &[u8], &[u8])>,
        flags: u32,
    ) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.tpc_end(&cx, xid, flags).await
        })
    }

    pub fn tpc_prepare(
        connection: &mut Connection,
        xid: Option<(u32, &[u8], &[u8])>,
    ) -> Result<bool> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.tpc_prepare(&cx, xid).await
        })
    }

    pub fn tpc_commit(
        connection: &mut Connection,
        xid: Option<(u32, &[u8], &[u8])>,
        one_phase: bool,
    ) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.tpc_commit(&cx, xid, one_phase).await
        })
    }

    pub fn tpc_rollback(
        connection: &mut Connection,
        xid: Option<(u32, &[u8], &[u8])>,
    ) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.tpc_rollback(&cx, xid).await
        })
    }

    /// Blocking wrapper for [`Connection::query`]: bind typed Rust values
    /// positionally or by name, then return a blocking lazy row facade.
    pub fn query<'conn, 'p>(
        connection: &'conn mut Connection,
        sql: &str,
        params: impl Into<crate::Params<'p>>,
    ) -> Result<BlockingRows<'conn>> {
        block_on_io(|cx| async move {
            connection
                .query(&cx, sql, params)
                .await
                .map(BlockingRows::new)
        })
    }

    /// Blocking wrapper for [`Connection::query_one`].
    pub fn query_one<'p>(
        connection: &mut Connection,
        sql: &str,
        params: impl Into<crate::Params<'p>>,
    ) -> Result<Row> {
        block_on_io(|cx| async move { connection.query_one(&cx, sql, params).await })
    }

    /// Blocking wrapper for [`Connection::query_opt`].
    pub fn query_opt<'p>(
        connection: &mut Connection,
        sql: &str,
        params: impl Into<crate::Params<'p>>,
    ) -> Result<Option<Row>> {
        block_on_io(|cx| async move { connection.query_opt(&cx, sql, params).await })
    }

    /// Blocking wrapper for [`Connection::query_all`].
    pub fn query_all<'p>(
        connection: &mut Connection,
        sql: &str,
        params: impl Into<crate::Params<'p>>,
    ) -> Result<Vec<Row>> {
        block_on_io(|cx| async move { connection.query_all(&cx, sql, params).await })
    }

    /// Blocking wrapper for [`Connection::query_with`].
    pub fn query_with<'conn, 'q>(
        connection: &'conn mut Connection,
        query: Query<'q>,
    ) -> Result<BlockingRows<'conn>> {
        block_on_io(|cx| async move {
            connection
                .query_with(&cx, query)
                .await
                .map(BlockingRows::new)
        })
    }

    /// Blocking wrapper for [`Connection::execute`].
    pub fn execute<'p>(
        connection: &mut Connection,
        sql: &str,
        params: impl Into<crate::Params<'p>>,
    ) -> Result<ExecuteOutcome> {
        block_on_io(|cx| async move { connection.execute(&cx, sql, params).await })
    }

    /// Blocking wrapper for [`Connection::execute_with`].
    pub fn execute_with<'e>(
        connection: &mut Connection,
        execute: Execute<'e>,
    ) -> Result<ExecuteOutcome> {
        block_on_io(|cx| async move { connection.execute_with(&cx, execute).await })
    }

    /// Blocking wrapper for [`Connection::execute_many`].
    pub fn execute_many<'b>(
        connection: &mut Connection,
        sql: &str,
        rows: impl Into<crate::BatchRows<'b>>,
    ) -> Result<BatchOutcome> {
        block_on_io(|cx| async move { connection.execute_many(&cx, sql, rows).await })
    }

    /// Blocking wrapper for [`Connection::execute_many_with`].
    pub fn execute_many_with<'b>(
        connection: &mut Connection,
        batch: Batch<'b>,
    ) -> Result<BatchOutcome> {
        block_on_io(|cx| async move { connection.execute_many_with(&cx, batch).await })
    }

    /// Blocking wrapper for [`Connection::execute_raw`]: the low-level raw
    /// execute primitive returning the unprojected [`QueryResult`], the
    /// execute-side counterpart to the retained blocking fetch primitives
    /// ([`Self::fetch_rows`], [`Self::define_and_fetch_rows_with_columns`],
    /// [`Self::scroll_cursor`], [`Self::fetch_cursor`]). Prefer the blocking
    /// operation families ([`Self::query`]/[`Self::execute`]/[`Self::execute_many`])
    /// for ordinary code; reach for `execute_raw` only when you need the raw
    /// wire result (statement-type-agnostic dispatch, parse-only describe, or
    /// per-bind-row OUT/RETURNING aggregation).
    pub fn execute_raw(
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
                .execute_raw(&cx, sql, prefetch_rows, bind_rows, exec_options, timeout_ms)
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

    pub fn trim_lob(
        connection: &mut Connection,
        locator: &[u8],
        new_size: u64,
    ) -> Result<LobReadResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.trim_lob(&cx, locator, new_size).await
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

    pub fn free_temp_lobs(connection: &mut Connection, locators: &[Vec<u8>]) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection.free_temp_lobs(&cx, locators).await
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

    pub fn run_pipeline_decoded(
        connection: &mut Connection,
        requests: &[PipelineRequest],
        continue_on_error: bool,
    ) -> Result<Vec<Result<QueryResult>>> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .run_pipeline_decoded(&cx, requests, continue_on_error)
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

    #[doc(hidden)]
    pub fn __pyshim_drain_cancel_response(connection: &mut Connection) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async { connection.drain_cancel_response().await })
    }

    /// Blocking wrapper for [`Connection::supplement_json_column_metadata`].
    pub fn supplement_json_column_metadata(
        connection: &mut Connection,
        columns: &mut [ColumnMetadata],
        timeout_ms: Option<u32>,
    ) -> Result<()> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .supplement_json_column_metadata(&cx, columns, timeout_ms)
                .await
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

/// Build a fresh single-thread asupersync runtime owned by a connection pool.
///
/// Unlike [`build_io_runtime`], which returns a thread-local runtime reused for
/// every blocking-facade call on a thread, the pool needs a *persistent, owned*
/// runtime whose worker thread hosts the region-owned reaper task for the pool's
/// whole lifetime. Each call returns a brand-new runtime; the pool stores it and
/// drops it (shutting the reaper down) when the last pool handle is dropped.
///
/// The worker thread carries the `oracledb-pool-bg` name prefix (the same name
/// the old detached worker used) so it is identifiable in stack dumps and so the
/// pool's threads are distinguishable from the shared blocking-facade runtime's.
pub(crate) fn new_pool_runtime() -> Result<Runtime> {
    let reactor = reactor::create_reactor()?;
    RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .thread_name_prefix("oracledb-pool-bg")
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

/// Run a blocking-facade operation on this thread's cached I/O runtime,
/// passing it the ambient [`Cx`] installed by [`Runtime::block_on`].
pub(crate) fn block_on_io<F, Fut, T>(operation: F) -> Result<T>
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

/// Runs a connection future to completion on a blocking runtime, passing it the
/// ambient [`Cx`] (shared shape of the `BlockingConnection` wrappers).
#[cfg(feature = "arrow")]
pub(crate) fn block_on_connection<F, Fut, T>(operation: F) -> Result<T>
where
    F: FnOnce(Cx) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    block_on_io(operation)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct IncomingPacket {
    packet_type: u8,
    payload: Vec<u8>,
}

async fn lock_write<W>(
    cx: &Cx,
    write: &Arc<AsyncMutex<W>>,
) -> Result<asupersync::sync::OwnedMutexGuard<W>>
where
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    // asupersync 0.3.9 makes borrowed mutex guards !Send because their
    // thread-local lock-order state must be released on the acquiring thread.
    // A write may await I/O, so retain the connection's Send future contract
    // with an Arc-backed owned guard while preserving write serialization.
    asupersync::sync::OwnedMutexGuard::lock(Arc::clone(write), cx)
        .await
        .map_err(|err| Error::Runtime(err.to_string()))
}

async fn write_all_shared<W>(cx: &Cx, write: &Arc<AsyncMutex<W>>, packet: &[u8]) -> Result<()>
where
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    let mut guard = lock_write(cx, write).await?;
    guard.write_all(packet).await?;
    guard.flush().await?;
    Ok(())
}

async fn shutdown_write_shared<W>(cx: &Cx, write: &Arc<AsyncMutex<W>>) -> Result<()>
where
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    let mut guard = lock_write(cx, write).await?;
    guard.shutdown().await?;
    Ok(())
}

async fn send_data_packet_shared<W>(
    cx: &Cx,
    write: &Arc<AsyncMutex<W>>,
    payload: &[u8],
    sdu: usize,
) -> Result<()>
where
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    let mut guard = lock_write(cx, write).await?;
    send_data_packet(&mut *guard, payload, sdu).await
}

async fn send_data_packet_shared_with_flags<W>(
    cx: &Cx,
    write: &Arc<AsyncMutex<W>>,
    payload: &[u8],
    sdu: usize,
    first_packet_flags: u16,
    last_packet_flags: u16,
) -> Result<()>
where
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
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

async fn send_marker_shared<W>(cx: &Cx, write: &Arc<AsyncMutex<W>>, marker_type: u8) -> Result<()>
where
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    let mut guard = lock_write(cx, write).await?;
    send_marker(&mut *guard, marker_type).await
}

fn lock_write_for_recovery<W>(
    write: &Arc<AsyncMutex<W>>,
) -> Result<asupersync::sync::OwnedMutexGuard<W>>
where
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    asupersync::sync::OwnedMutexGuard::try_lock(Arc::clone(write)).map_err(|err| match err {
        asupersync::sync::TryLockError::Locked => Error::ConnectionClosed(
            "write lock unavailable while recovering from cancellation".into(),
        ),
        asupersync::sync::TryLockError::Poisoned => {
            Error::ConnectionClosed("write lock poisoned while recovering from cancellation".into())
        }
    })
}

async fn send_marker_recovery<W>(write: &Arc<AsyncMutex<W>>, marker_type: u8) -> Result<()>
where
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    let mut guard = lock_write_for_recovery(write)?;
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

#[cfg(test)]
async fn read_data_response<R, W>(
    read: &mut R,
    cx: &Cx,
    write: &Arc<AsyncMutex<W>>,
) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    read_data_response_with_limits(read, cx, write, ProtocolLimits::DEFAULT).await
}

async fn read_data_response_with_limits<R, W>(
    read: &mut R,
    cx: &Cx,
    write: &Arc<AsyncMutex<W>>,
    limits: ProtocolLimits,
) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    Ok(
        read_data_response_boundary_with_limits(read, cx, write, false, limits)
            .await?
            .payload,
    )
}

/// Accumulates DATA packets for one classic (pre-END_OF_RESPONSE)
/// connect-phase response until the payload's terminal message is complete.
/// Used only for the pre-23ai protocol-negotiation / data-types / auth round
/// trips, where END_OF_RESPONSE framing is not negotiated and completion is
/// message-driven (reference messages/base.pyx `Message.process`).
///
/// MARKER packets run the same reset dance as the post-connect readers: a
/// pre-23ai server answers a failed classic login (e.g. wrong password) with
/// a break MARKER *before* the ERROR response, so without the reset exchange
/// the caller would surface a misleading `UnexpectedPacket(MARKER)` instead
/// of the real ORA-01017 (reference packet.pyx: markers are handled uniformly
/// on every read, including the connect-phase auth round trips).
async fn read_classic_data_response_with_limits<R, W>(
    read: &mut R,
    cx: &Cx,
    write: &Arc<AsyncMutex<W>>,
    limits: ProtocolLimits,
) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    let mut response = Vec::new();
    let mut pending_packet: Option<IncomingPacket> = None;
    let mut after_reset = false;
    loop {
        let packet = match pending_packet.take() {
            Some(packet) => packet,
            None => read_packet_with_limits(read, PacketLengthWidth::Large32, limits).await?,
        };
        if packet.packet_type == TNS_PACKET_TYPE_MARKER {
            pending_packet =
                reset_after_marker_with_limits(read, cx, write, &packet, limits).await?;
            after_reset = true;
            continue;
        }
        if packet.packet_type != TNS_PACKET_TYPE_DATA {
            return Err(Error::UnexpectedPacket(packet.packet_type));
        }
        let payload =
            packet
                .payload
                .get(2..)
                .ok_or(oracledb_protocol::ProtocolError::TtcDecode(
                    "missing data packet flags",
                ))?;
        let combined = response.len().checked_add(payload.len()).ok_or(
            oracledb_protocol::ProtocolError::ResourceLimit {
                limit: "response_bytes",
                observed: usize::MAX,
                maximum: limits.max_response_bytes,
            },
        )?;
        limits.check_response_bytes(combined)?;
        response.extend_from_slice(payload);
        if (after_reset && post_reset_packet_ends_response(payload))
            || classic_connect_response_is_complete(&response, limits)?
        {
            return Ok(response);
        }
    }
}

/// Probe predicate for classic (pre-END_OF_RESPONSE) response reassembly:
/// whether a parse attempt over the accumulated payload says the response is
/// complete. `TtcDecode` is the decoder's "ran out of bytes / short read"
/// error — the response needs more packets. ANY other outcome means the
/// parser consumed a full response: `Ok` is the happy path, a
/// `ServerError`/`ServerErrorInfo` means the terminal ERROR message was
/// reached (the caller's real parse will surface it), and structural errors
/// (`UnknownMessageType`, `ResourceLimit`, ...) are returned to the caller by
/// its real parse instead of hanging the read loop forever.
fn response_complete<T>(result: &oracledb_protocol::Result<T>) -> bool {
    !matches!(result, Err(oracledb_protocol::ProtocolError::TtcDecode(_)))
}

/// Accumulates DATA packets for one classic (pre-END_OF_RESPONSE) post-connect
/// response, deciding completion with the caller's `probe` over the
/// accumulated payload (see
/// [`ConnectionCore::read_data_response_probed`]). MARKER packets run the
/// same reset dance as [`read_data_response_boundary_seeded`]; after a reset
/// the terminal-message-byte relaxation ([`post_reset_packet_ends_response`])
/// also ends the response, exactly like the flag-framed reader.
async fn read_classic_data_response_probed_with_limits<R, W, P>(
    read: &mut R,
    cx: &Cx,
    write: &Arc<AsyncMutex<W>>,
    probe: &P,
    limits: ProtocolLimits,
) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
    P: Fn(&[u8]) -> bool,
{
    let mut response = Vec::new();
    read_classic_data_response_probed_into(read, cx, write, probe, limits, &mut response).await?;
    Ok(response)
}

/// The reassembly loop of [`read_classic_data_response_probed_with_limits`],
/// appending onto an existing buffer so the flush-out-binds continuation can
/// probe the COMBINED payload (the parser always parses from the response
/// start).
async fn read_classic_data_response_probed_into<R, W, P>(
    read: &mut R,
    cx: &Cx,
    write: &Arc<AsyncMutex<W>>,
    probe: &P,
    limits: ProtocolLimits,
    response: &mut Vec<u8>,
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
    P: Fn(&[u8]) -> bool,
{
    let mut pending_packet: Option<IncomingPacket> = None;
    let mut after_reset = false;
    loop {
        let packet = match pending_packet.take() {
            Some(packet) => packet,
            None => read_packet_with_limits(read, PacketLengthWidth::Large32, limits).await?,
        };
        if packet.packet_type == TNS_PACKET_TYPE_MARKER {
            pending_packet =
                reset_after_marker_with_limits(read, cx, write, &packet, limits).await?;
            after_reset = true;
            continue;
        }
        if packet.packet_type != TNS_PACKET_TYPE_DATA {
            return Err(Error::UnexpectedPacket(packet.packet_type));
        }
        let payload =
            packet
                .payload
                .get(2..)
                .ok_or(oracledb_protocol::ProtocolError::TtcDecode(
                    "missing data packet flags",
                ))?;
        let combined = response.len().checked_add(payload.len()).ok_or(
            oracledb_protocol::ProtocolError::ResourceLimit {
                limit: "response_bytes",
                observed: usize::MAX,
                maximum: limits.max_response_bytes,
            },
        )?;
        limits.check_response_bytes(combined)?;
        response.extend_from_slice(payload);
        if (after_reset && post_reset_packet_ends_response(payload)) || probe(response) {
            return Ok(());
        }
    }
}

/// Classic (pre-END_OF_RESPONSE) counterpart of
/// [`read_data_response_flushing_out_binds_with_limits`]: reads one probed
/// response and, while it ends at a FLUSH_OUT_BINDS request (terminal message
/// byte, same detection as the flag-framed path), answers it and keeps
/// accumulating — probing the combined payload — until the real response is
/// complete.
async fn read_classic_data_response_flushing_out_binds_probed_with_limits<R, W, P>(
    read: &mut R,
    cx: &Cx,
    write: &Arc<AsyncMutex<W>>,
    sdu: usize,
    probe: &P,
    limits: ProtocolLimits,
) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
    P: Fn(&[u8]) -> bool,
{
    let mut payload = Vec::new();
    read_classic_data_response_probed_into(read, cx, write, probe, limits, &mut payload).await?;
    while matches!(payload.last(), Some(&TNS_MSG_TYPE_FLUSH_OUT_BINDS)) {
        observe_cancellation_between_round_trips(cx)?;
        payload.pop();
        send_data_packet_shared(cx, write, &[TNS_MSG_TYPE_FLUSH_OUT_BINDS], sdu).await?;
        read_classic_data_response_probed_into(read, cx, write, probe, limits, &mut payload)
            .await?;
    }
    Ok(payload)
}

/// Upper bound on how long the post-break recovery drain may take before the
/// driver gives up and declares the connection dead. Mirrors the reference's
/// "second timeout while recovering" disconnect (protocol.pyx:454-458): the
/// first timeout was the user's `call_timeout`; this guards the *recovery*
/// read so a server that never answers the BREAK cannot hang the caller
/// forever. Reuses the same 5 s ceiling as [`Connection::drain_cancel_response`].
const BREAK_DRAIN_RECOVERY_TIMEOUT: Duration = Duration::from_secs(5);

/// Sends a BREAK marker and then **drains** the server's entire break response
/// so the wire stream is left at a clean message boundary, exactly as
/// python-oracledb does on a call timeout (`_break_external()` then
/// `_receive_packet()` / `_reset()`, protocol.pyx:449-451, 507-557).
///
/// The break response is multi-stage and racy on the wire (confirmed by live
/// trace against Oracle 23/26ai): when the server is mid-call it may flush the
/// **in-flight response** of the timed-out call *first* — a complete DATA
/// response carrying its own end-of-response flag — and only *then* send the
/// break-acknowledge **MARKER**, the **RESET** handshake, and the **trailing
/// error packet** (ORA-01013 "user requested cancel"). A naive
/// `read_data_response` stops at the in-flight response's end-of-response and
/// leaves the MARKER + ORA-01013 in the socket, where the *next* operation
/// misreads them (it surfaces ORA-01013 / desyncs). The reference avoids this
/// because its `_reset()` is what clears `_break_in_progress`, and it always
/// runs `_reset()` (consuming the MARKER, the RESET marker, and the trailing
/// error packet) before the connection is considered recovered.
///
/// So this drain does NOT stop at the first end-of-response: it discards any
/// in-flight DATA responses until it meets the break-acknowledge MARKER, runs
/// the RESET dance via [`reset_after_marker`] (send RESET, discard packets until
/// the server RESET marker), and then consumes the trailing error response to
/// its end-of-response boundary. Everything read here is discarded — it is the
/// dead remains of the cancelled call, not a result for any caller.
///
/// On success (`Ok(())`) the stream is clean and the connection is reusable. If
/// the drain errors or its bounded *secondary* timeout fires, the wire could not
/// be left clean, so the connection must be discarded — the error is surfaced as
/// [`Error::ConnectionClosed`], which is [`Error::is_connection_lost`].
#[allow(dead_code)]
async fn break_and_drain_wire<R, W>(
    read: &mut R,
    write: &Arc<AsyncMutex<W>>,
    recovery_timeout: Duration,
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    let result = time::timeout(
        time::wall_now(),
        recovery_timeout,
        break_and_drain_wire_unbounded(read, write),
    )
    .await
    .ok();
    classify_recovery_result(RecoveryWireAction::BreakAndDrain, result)
}

async fn break_and_drain_wire_unbounded<R, W>(
    read: &mut R,
    write: &Arc<AsyncMutex<W>>,
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    break_and_drain_wire_unbounded_with_limits(read, write, ProtocolLimits::DEFAULT, false).await
}

async fn break_and_drain_wire_unbounded_with_limits<R, W>(
    read: &mut R,
    write: &Arc<AsyncMutex<W>>,
    limits: ProtocolLimits,
    classic: bool,
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    // 1) Send the BREAK marker (reference `_break_external`).
    send_marker_recovery(write, TNS_MARKER_TYPE_BREAK)
        .await
        .map_err(|err| {
            Error::ConnectionClosed(format!(
                "failed to send break marker on call timeout: {err}"
            ))
        })?;
    // 2) Drain the whole break response.
    drain_break_response_recovery_with_limits(read, write, limits, classic).await
}

/// Sends a BREAK and drains the server's cancel response so the wire is left at
/// a clean boundary, for an **explicit** user cancel (rather than a call
/// timeout). This is the wire half of [`Connection::cancel`].
///
/// The wire sequence a user cancel triggers is byte-for-byte the same as a call
/// timeout — python-oracledb routes both through `_break_external()` +
/// `_reset()` (`cancel()` is `connection.pyx:291` -> `_break_external`;
/// `protocol.pyx:533-557`). So this delegates straight to
/// [`break_and_drain_wire`] (no duplicated drain loop): the BREAK marker, the
/// discard of any in-flight DATA responses, the RESET handshake, and the
/// consumption of the trailing ORA-01013 error all happen there.
///
/// The reference would send an out-of-band (urgent-TCP) break first when
/// `supports_oob` is negotiated; when OOB is unavailable it falls back to this
/// in-band INTERRUPT/BREAK marker (`protocol.pyx:56-69`). Asupersync's
/// `TcpStream` does not expose `send(MSG_OOB)`, so we always take the in-band
/// path — which the reference itself uses on every platform where OOB is off
/// (e.g. Windows, or `disable_oob`), and which the server handles identically.
///
/// `Ok(())` means the wire is clean and the connection is reusable; the caller
/// surfaces [`Error::Cancelled`]. On a failed drain the error is
/// [`Error::ConnectionClosed`] and the connection must be discarded.
#[allow(dead_code)]
async fn cancel_and_drain_wire<R, W>(
    read: &mut R,
    write: &Arc<AsyncMutex<W>>,
    recovery_timeout: Duration,
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    break_and_drain_wire(read, write, recovery_timeout).await
}

/// Drains the server's cancel response **without** sending a BREAK first.
///
/// Used by the two-thread cancel path: a [`CancelHandle`] on a *separate* thread
/// has already sent the BREAK marker while the main thread was blocked inside a
/// query round trip. The wire now carries the same multi-stage cancel response a
/// timeout break would (the cancelled call's in-flight DATA response, the
/// break-ack MARKER, the RESET handshake, and the trailing ORA-01013), so this
/// reuses [`drain_break_response_recovery`] — the SAME drain
/// `break_and_drain_wire` runs — under the same bounded recovery timeout. It
/// just omits the `send_marker` BREAK that the handle thread already issued;
/// sending a second BREAK here would inject an extra marker the server answers
/// with an extra reset, desyncing the reused connection.
///
/// `Ok(())` leaves the wire clean and the connection reusable. A drain error or
/// a secondary timeout yields [`Error::ConnectionClosed`] (the connection must
/// be discarded), matching the break+drain failure semantics.
#[allow(dead_code)]
async fn drain_cancel_wire<R, W>(
    read: &mut R,
    write: &Arc<AsyncMutex<W>>,
    recovery_timeout: Duration,
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    let result = time::timeout(
        time::wall_now(),
        recovery_timeout,
        drain_cancel_wire_unbounded(read, write),
    )
    .await
    .ok();
    classify_recovery_result(RecoveryWireAction::DrainCancel, result)
}

async fn drain_cancel_wire_unbounded<R, W>(read: &mut R, write: &Arc<AsyncMutex<W>>) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    drain_cancel_wire_unbounded_with_limits(read, write, ProtocolLimits::DEFAULT, false).await
}

async fn drain_cancel_wire_unbounded_with_limits<R, W>(
    read: &mut R,
    write: &Arc<AsyncMutex<W>>,
    limits: ProtocolLimits,
    classic: bool,
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    drain_break_response_recovery_with_limits(read, write, limits, classic).await
}

/// Drop-guard that marks a connection's recovery phase `BreakSent` if a
/// cancellable round-trip read future is **dropped while still in flight**.
///
/// This is the Scope-based cancel-on-drop half of the cancellation story: when a
/// fetch/execute future is raced by a `select!` or a `time::timeout` and the
/// losing branch is dropped, the request has already gone out but its response
/// is still arriving (or the server is still mid-call). Dropping the future ends
/// the `&mut Connection` borrow but leaves those bytes / that running call on the
/// wire. The guard's `Drop` records in the single recovery owner that the next
/// operation must first send a BREAK and drain (via [`cancel_and_drain_wire`])
/// before issuing its own request — so a cancelled fetch never poisons the
/// stream for the next one.
///
/// A read that completes normally calls `CancelDrainGuard::disarm` first, so
/// the common (uncancelled) path never arms the flag and pays nothing.
struct CancelDrainGuard {
    recovery: Arc<SessionRecovery>,
    armed: bool,
}

impl CancelDrainGuard {
    /// Mark the response as in flight for the duration of a cancellable read.
    fn arm(recovery: Arc<SessionRecovery>) -> Result<Self> {
        recovery.begin_or_adopt_operation()?;
        Ok(Self {
            recovery,
            armed: true,
        })
    }

    /// Disarm the guard after the read completed normally, so its `Drop` is a
    /// no-op and the next operation does not needlessly break + drain.
    fn disarm(&mut self) {
        self.recovery.complete_operation();
        self.armed = false;
    }
}

impl Drop for CancelDrainGuard {
    fn drop(&mut self) {
        if self.armed {
            // The future was dropped mid-read (cancelled): tell the next
            // operation to break + drain the stranded server call first.
            self.recovery.mark_break_required();
        }
    }
}

/// Reads and discards the full server response to a BREAK: any in-flight DATA
/// response(s) of the cancelled call, then the break-acknowledge MARKER, the
/// RESET handshake, and the trailing error packet — leaving the reader at a
/// clean boundary. See [`break_and_drain_wire`] for why stopping at the first
/// end-of-response is insufficient.
#[cfg(test)]
#[allow(dead_code)]
async fn drain_break_response_recovery<R, W>(read: &mut R, write: &Arc<AsyncMutex<W>>) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    drain_break_response_recovery_with_limits(read, write, ProtocolLimits::DEFAULT, false).await
}

async fn drain_break_response_recovery_with_limits<R, W>(
    read: &mut R,
    write: &Arc<AsyncMutex<W>>,
    limits: ProtocolLimits,
    classic: bool,
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    // Phase A: discard whole DATA responses until the break-acknowledge MARKER.
    // The server flushes the cancelled call's in-flight response first; each is
    // a complete DATA response (its own end-of-response) that we drop on the
    // floor. The MARKER is what drives the RESET handshake.
    let initial_marker = loop {
        let packet = read_packet_with_limits(read, PacketLengthWidth::Large32, limits).await?;
        match packet.packet_type {
            TNS_PACKET_TYPE_MARKER => break packet,
            TNS_PACKET_TYPE_DATA => {
                trace_connect_bytes("BREAK drain: discarded in-flight packet", &packet.payload);
                continue;
            }
            other => {
                // Packet-layer byte (header offset 4), NOT a TTC message
                // type: name the TNS packet type so triage is not steered
                // toward the application layer (bead
                // rust-oracledb-pre23ai-connect-z47u.3).
                return Err(Error::UnexpectedPacket(other));
            }
        }
    };

    // Phase B: run the RESET dance (send RESET, discard packets until the server
    // RESET marker). `reset_after_marker` returns the first non-marker packet
    // after the RESET confirmation, if any — that is the head of the trailing
    // error response (ORA-01013).
    let pending =
        reset_after_marker_recovery_with_limits(read, write, &initial_marker, limits).await?;

    // Phase C: consume the trailing error response to its end-of-response
    // boundary and discard it. Reuses the same boundary loop the normal read
    // path uses, seeded with the packet `reset_after_marker` already pulled.
    let trailing = read_data_response_boundary_from_recovery_with_limits(
        read, write, pending, limits, classic,
    )
    .await?;
    trace_connect_bytes("BREAK drain: trailing error response", &trailing.payload);
    Ok(())
}

#[cfg(test)]
async fn read_data_response_flushing_out_binds<R, W>(
    read: &mut R,
    cx: &Cx,
    write: &Arc<AsyncMutex<W>>,
    sdu: usize,
) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    read_data_response_flushing_out_binds_with_limits(read, cx, write, sdu, ProtocolLimits::DEFAULT)
        .await
}

async fn read_data_response_flushing_out_binds_with_limits<R, W>(
    read: &mut R,
    cx: &Cx,
    write: &Arc<AsyncMutex<W>>,
    sdu: usize,
    limits: ProtocolLimits,
) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    let mut response =
        read_data_response_boundary_with_limits(read, cx, write, false, limits).await?;
    let mut payload = response.payload;
    while response.flush_out_binds {
        observe_cancellation_between_round_trips(cx)?;
        if matches!(payload.last(), Some(&TNS_MSG_TYPE_FLUSH_OUT_BINDS)) {
            payload.pop();
        }
        send_data_packet_shared(cx, write, &[TNS_MSG_TYPE_FLUSH_OUT_BINDS], sdu).await?;
        response = read_data_response_boundary_with_limits(read, cx, write, false, limits).await?;
        let combined = payload.len().checked_add(response.payload.len()).ok_or(
            oracledb_protocol::ProtocolError::ResourceLimit {
                limit: "response_bytes",
                observed: usize::MAX,
                maximum: limits.max_response_bytes,
            },
        )?;
        limits.check_response_bytes(combined)?;
        payload.extend_from_slice(&response.payload);
    }
    Ok(payload)
}

/// Returns whether this DATA packet carries the end of the TTC response, given
/// the packet's 2-byte data flags and its post-flags payload.
///
/// This mirrors the reference `Packet.has_end_of_response`
/// (impl/thin/packet.pyx:58-73). The end of a response is signalled either by
/// the `END_OF_RESPONSE` / `EOF` data flag, or by a trailing
/// `TNS_MSG_TYPE_END_OF_RESPONSE` (29 / 0x1d) byte that arrives **as its own
/// minimal packet** -- a packet whose entire post-flags payload is exactly that
/// one byte (reference condition `packet_size == PACKET_HEADER_SIZE + 3`).
///
/// The size guard is load-bearing for multi-packet wide-row results. Without it,
/// any DATA packet whose payload merely *happens to end* in byte 0x1d -- an
/// utterly ordinary value inside a NUMBER mantissa, a length prefix, or a text
/// byte -- would be misread as the end of the response. A wide (e.g. 20-column
/// NUMBER/VARCHAR2) single fetch of ~1500+ rows spans several network packets,
/// and a mid-stream packet boundary lands on a 0x1d byte often enough that the
/// reassembly loop terminated early, truncating the buffer. The TTC decoder then
/// mis-framed the continuation, surfacing as "encoded NUMBER too long" /
/// "truncated TTC payload" (bead rust-oracledb-n2s).
fn data_packet_ends_response(flags: u16, payload: &[u8]) -> bool {
    if flags
        & (oracledb_protocol::thin::TNS_DATA_FLAGS_END_OF_RESPONSE
            | oracledb_protocol::thin::TNS_DATA_FLAGS_EOF)
        != 0
    {
        return true;
    }
    // Fallback for servers that did not negotiate END_OF_RESPONSE framing: a
    // response that is a single minimal packet whose entire post-flags payload
    // is just the END_OF_RESPONSE marker, or just the FLUSH_OUT_BINDS marker
    // (which the reference also treats as end_of_response,
    // messages/base.pyx:1267-1269). The exact-length match is the load-bearing
    // guard: a multi-packet body packet that merely *ends* in one of these bytes
    // must NOT terminate the response.
    payload == [TNS_MSG_TYPE_END_OF_RESPONSE] || payload == [TNS_MSG_TYPE_FLUSH_OUT_BINDS]
}

/// Whether a DATA packet read **after a RESET dance** ends the response, based
/// on its terminal TTC message byte alone.
///
/// After a BREAK/RESET the server stops using request-boundary framing: the
/// trailing packets of the break-recovery response do NOT carry the
/// `END_OF_RESPONSE` data flag (confirmed by live wire trace against Oracle
/// 23ai on the DML-RETURNING error path, test_1600 test_1612 / ORA-12899). The
/// server instead sends message-byte-framed packets — e.g. a `FLUSH_OUT_BINDS`
/// *request* (a DATA packet ending in byte 0x13) that expects a
/// `FLUSH_OUT_BINDS` reply, then the real error packet. The reference detects
/// the boundary while *processing* the message (`TNS_MSG_TYPE_FLUSH_OUT_BINDS`
/// and `TNS_MSG_TYPE_END_OF_RESPONSE` both set `end_of_response`,
/// messages/base.pyx:1267-1269), because its `_check_request_boundary` is off
/// for post-reset packets (protocol.pyx:896-906).
///
/// Unlike [`data_packet_ends_response`], this does NOT require the marker byte
/// to be the packet's sole byte — the FLUSH_OUT_BINDS request arrives as
/// `07 00 00 13`, the marker as the *last* byte. That is safe here precisely
/// because it is gated on the post-reset context, which carries no multi-packet
/// wide-row body (the bead rust-oracledb-n2s false-positive only arises on the
/// normal request-boundary-framed read path, which never sets `after_reset`).
fn post_reset_packet_ends_response(payload: &[u8]) -> bool {
    matches!(
        payload.last(),
        Some(&TNS_MSG_TYPE_FLUSH_OUT_BINDS) | Some(&TNS_MSG_TYPE_END_OF_RESPONSE)
    )
}

/// Reads one boundary-delimited TTC response. While `in_pipeline` is set,
/// marker packets are silently dropped instead of triggering the
/// send-reset/await-reset dance -- the reference does the same while reading
/// pipelined responses (packet.pyx:346-370, protocol.pyx:889-906), since the
/// server emits a marker alongside an in-pipeline error without expecting a
/// reset exchange.
#[cfg(test)]
#[allow(dead_code)]
async fn read_data_response_boundary<R, W>(
    read: &mut R,
    cx: &Cx,
    write: &Arc<AsyncMutex<W>>,
    in_pipeline: bool,
) -> Result<DataResponse>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    read_data_response_boundary_with_limits(read, cx, write, in_pipeline, ProtocolLimits::DEFAULT)
        .await
}

async fn read_data_response_boundary_with_limits<R, W>(
    read: &mut R,
    cx: &Cx,
    write: &Arc<AsyncMutex<W>>,
    in_pipeline: bool,
    limits: ProtocolLimits,
) -> Result<DataResponse>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    // Normal (23ai / END_OF_RESPONSE-framed) read path: never classic.
    read_data_response_boundary_seeded(read, Some(cx), write, in_pipeline, None, limits, false)
        .await
}

/// Like [`read_data_response_boundary`] but seeds the reassembly loop with an
/// already-read `seed` packet (e.g. the trailing packet `reset_after_marker`
/// pulled past a RESET marker) before reading more from the wire. Used by the
/// break-drain path to consume the trailing error response. Always runs the
/// non-pipeline (reset-handling) variant.
#[cfg(test)]
#[allow(dead_code)]
async fn read_data_response_boundary_from_recovery<R, W>(
    read: &mut R,
    write: &Arc<AsyncMutex<W>>,
    seed: Option<IncomingPacket>,
) -> Result<DataResponse>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    read_data_response_boundary_from_recovery_with_limits(
        read,
        write,
        seed,
        ProtocolLimits::DEFAULT,
        false,
    )
    .await
}

async fn read_data_response_boundary_from_recovery_with_limits<R, W>(
    read: &mut R,
    write: &Arc<AsyncMutex<W>>,
    seed: Option<IncomingPacket>,
    limits: ProtocolLimits,
    classic: bool,
) -> Result<DataResponse>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    read_data_response_boundary_seeded(read, None, write, false, seed, limits, classic).await
}

async fn read_data_response_boundary_seeded<R, W>(
    read: &mut R,
    cx: Option<&Cx>,
    write: &Arc<AsyncMutex<W>>,
    in_pipeline: bool,
    seed: Option<IncomingPacket>,
    limits: ProtocolLimits,
    classic: bool,
) -> Result<DataResponse>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    let mut response = Vec::new();
    let mut pending_packet = seed;
    // Set once this loop has run a RESET dance (reference `_reset`). After a
    // RESET the server stops using request-boundary (END_OF_RESPONSE data flag)
    // framing for the trailing error response: it sends message-byte-framed
    // packets, exactly like the reference's `message.process()` after
    // `_reset()`, where `_check_request_boundary` is off (protocol.pyx:819-821,
    // 896-906). So a post-reset packet whose payload ends in a terminal message
    // byte (FLUSH_OUT_BINDS / END_OF_RESPONSE) ends the response even without
    // the data flag. This relaxation is gated on `after_reset` so the wide-row
    // false-positive guard (bead rust-oracledb-n2s) on the normal framing path
    // is left entirely intact.
    let mut after_reset = false;
    loop {
        let packet = match pending_packet.take() {
            Some(packet) => packet,
            None => read_packet_with_limits(read, PacketLengthWidth::Large32, limits).await?,
        };
        if packet.packet_type == TNS_PACKET_TYPE_MARKER {
            if in_pipeline {
                trace_connect_bytes("MARKER packet skipped in pipeline", &packet.payload);
                continue;
            }
            pending_packet = match cx {
                Some(cx) => {
                    reset_after_marker_with_limits(read, cx, write, &packet, limits).await?
                }
                None => {
                    reset_after_marker_recovery_with_limits(read, write, &packet, limits).await?
                }
            };
            after_reset = true;
            continue;
        }
        if packet.packet_type != TNS_PACKET_TYPE_DATA {
            // Packet-layer byte (header offset 4), NOT a TTC message type:
            // name the TNS packet type so triage is not steered toward the
            // application layer (bead rust-oracledb-pre23ai-connect-z47u.3).
            return Err(Error::UnexpectedPacket(packet.packet_type));
        }
        let (data_flags, payload) = packet.payload.split_at_checked(2).ok_or(
            oracledb_protocol::ProtocolError::TtcDecode("missing data packet flags"),
        )?;
        let flags = u16::from_be_bytes(
            data_flags
                .try_into()
                .map_err(|_| oracledb_protocol::ProtocolError::TtcDecode("invalid flags"))?,
        );
        let ends = data_packet_ends_response(flags, payload)
            || (after_reset && post_reset_packet_ends_response(payload));
        // Single-packet passthrough (bead rust-oracledb-0n0): when the whole
        // response is ONE DATA packet (nothing accumulated yet AND this packet
        // ends the response), move the packet's owned buffer into `response` and
        // strip the 2 flag bytes in place, instead of allocating a fresh Vec and
        // copying the entire payload into it. The flag-strip is preserved (drain
        // removes the same 2 leading bytes), the end-of-response decision is the
        // exact same `ends` computed above, and the FLUSH_OUT_BINDS terminal-byte
        // detection below sees an identical byte stream. The multi-packet path is
        // unchanged (it must reassemble, so it extends).
        if ends && response.is_empty() {
            limits.check_response_bytes(payload.len())?;
            response = packet.payload;
            response.drain(..2);
            break;
        }
        let combined = response.len().checked_add(payload.len()).ok_or(
            oracledb_protocol::ProtocolError::ResourceLimit {
                limit: "response_bytes",
                observed: usize::MAX,
                maximum: limits.max_response_bytes,
            },
        )?;
        limits.check_response_bytes(combined)?;
        response.extend_from_slice(payload);
        if ends {
            break;
        }
        // Classic (pre-END_OF_RESPONSE) recovery framing: on a server that never
        // negotiated END_OF_RESPONSE (protocol < 319, i.e. everything before
        // 23ai), the trailing error response after a BREAK/RESET carries neither
        // the END_OF_RESPONSE data flag nor a terminal marker byte -- it ends at
        // its terminal TTC message (ERROR/STATUS). Decide completion by parsing
        // the accumulated stream, exactly like the classic connect-phase reader,
        // so the recovery drain does not block until its secondary timeout and
        // surface a spurious ConnectionClosed instead of the real CallTimeout /
        // Cancelled (bead rust-oracledb-99xu). Gated on `classic` so the 23ai
        // flag-framed path (and its wide-row false-positive guard) is untouched.
        if classic && classic_connect_response_is_complete(&response, limits).unwrap_or(false) {
            break;
        }
    }
    // A flush-out-binds response ends with the FLUSH_OUT_BINDS message byte
    // (reference messages/base.pyx:1267-1269, which also sets end_of_response).
    // Detect it from the terminal message byte of the fully reassembled stream
    // rather than mid-stream, so an ordinary data byte 0x13 at a packet boundary
    // is never mistaken for it.
    let flush_out_binds = matches!(response.last(), Some(&TNS_MSG_TYPE_FLUSH_OUT_BINDS));
    Ok(DataResponse {
        payload: response,
        flush_out_binds,
    })
}

const TNS_PACKET_TYPE_MARKER: u8 = 12;
const TNS_MARKER_TYPE_BREAK: u8 = 1;
const TNS_MARKER_TYPE_RESET: u8 = 2;

#[cfg(test)]
#[allow(dead_code)]
async fn reset_after_marker_recovery<R, W>(
    read: &mut R,
    write: &Arc<AsyncMutex<W>>,
    initial_marker: &IncomingPacket,
) -> Result<Option<IncomingPacket>>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    reset_after_marker_recovery_with_limits(read, write, initial_marker, ProtocolLimits::DEFAULT)
        .await
}

async fn reset_after_marker_recovery_with_limits<R, W>(
    read: &mut R,
    write: &Arc<AsyncMutex<W>>,
    initial_marker: &IncomingPacket,
    limits: ProtocolLimits,
) -> Result<Option<IncomingPacket>>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    trace_connect_bytes("MARKER packet", &initial_marker.payload);
    send_marker_recovery(write, TNS_MARKER_TYPE_RESET).await?;
    drain_reset_markers_with_limits(read, limits).await
}

#[cfg(test)]
#[allow(dead_code)]
async fn reset_after_marker<R, W>(
    read: &mut R,
    cx: &Cx,
    write: &Arc<AsyncMutex<W>>,
    initial_marker: &IncomingPacket,
) -> Result<Option<IncomingPacket>>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    reset_after_marker_with_limits(read, cx, write, initial_marker, ProtocolLimits::DEFAULT).await
}

async fn reset_after_marker_with_limits<R, W>(
    read: &mut R,
    cx: &Cx,
    write: &Arc<AsyncMutex<W>>,
    initial_marker: &IncomingPacket,
    limits: ProtocolLimits,
) -> Result<Option<IncomingPacket>>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    trace_connect_bytes("MARKER packet", &initial_marker.payload);
    send_marker_shared(cx, write, TNS_MARKER_TYPE_RESET).await?;
    drain_reset_markers_with_limits(read, limits).await
}

#[cfg(test)]
#[allow(dead_code)]
async fn drain_reset_markers<R>(read: &mut R) -> Result<Option<IncomingPacket>>
where
    R: AsyncRead + Unpin,
{
    drain_reset_markers_with_limits(read, ProtocolLimits::DEFAULT).await
}

async fn drain_reset_markers_with_limits<R>(
    read: &mut R,
    limits: ProtocolLimits,
) -> Result<Option<IncomingPacket>>
where
    R: AsyncRead + Unpin,
{
    // Drain the RESET handshake: consume EVERY trailing marker packet — the
    // RESET acknowledgement AND any additional markers the server sends after
    // it (a documented variant: reference _reset's second loop,
    // protocol.pyx:554-556, "some servers send multiple reset markers"). Return
    // the first NON-marker packet (the trailing error/data packet) so the caller
    // is seeded with it. Returning early on the first RESET marker would leave a
    // following marker in the stream, which the caller mis-reads as a fresh
    // break and answers with a DUPLICATE RESET, poisoning a reused connection
    // (bead rust-oracledb-yhz). Exactly one RESET is ever sent, here.
    loop {
        let packet = read_packet_with_limits(read, PacketLengthWidth::Large32, limits).await?;
        if packet.packet_type != TNS_PACKET_TYPE_MARKER {
            return Ok(Some(packet));
        }
        trace_connect_bytes("MARKER reset response", &packet.payload);
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

#[cfg(test)]
#[allow(dead_code)]
async fn read_packet<R>(stream: &mut R, width: PacketLengthWidth) -> Result<IncomingPacket>
where
    R: AsyncRead + Unpin,
{
    read_packet_with_limits(stream, width, ProtocolLimits::DEFAULT).await
}

/// Derives the TCP keepalive idle interval from a DSN `EXPIRE_TIME` (minutes,
/// reference net.pyx). `0` (the default) disables keepalive; any positive value
/// enables it with that many minutes of socket idle time before the first probe,
/// so a half-open/dead peer is detected instead of wedging a later read (GH#14).
fn keepalive_idle_from_expire_time(expire_time_minutes: u32) -> Option<Duration> {
    (expire_time_minutes > 0).then(|| Duration::from_secs(u64::from(expire_time_minutes) * 60))
}

/// Applies the connection's optional read-inactivity deadline (GH#14) to a wire
/// read future. `None` awaits unbounded (the prior behaviour); `Some(d)` fails
/// the read with [`Error::CallTimeout`] if it does not complete within `d`, so a
/// silent or half-open server cannot wedge a post-auth read forever. The
/// deadline bounds a whole read operation (which may span several framing-layer
/// `read_exact` calls), so every one of those reads is transitively bounded.
async fn apply_inactivity_timeout<F, U>(timeout: Option<Duration>, fut: F) -> Result<U>
where
    F: std::future::Future<Output = Result<U>>,
{
    match timeout {
        None => fut.await,
        Some(deadline) => match time::timeout(time::wall_now(), deadline, fut).await {
            Ok(result) => result,
            Err(_) => Err(Error::CallTimeout(duration_to_millis_saturating(deadline))),
        },
    }
}

/// Deterministic, container-free coverage for the GH#14 connect/idle/keepalive
/// timeouts (bead a4/A1.1). The marquee change — a per-read inactivity deadline
/// and a keepalive interval derived from `EXPIRE_TIME` — is wired into every
/// `ConnectionCore` read via [`apply_inactivity_timeout`] (each `read_*` method
/// wraps its read future, so every framing-layer `read_exact` is transitively
/// bounded, AC4) and into the CONNECT/ACCEPT phase via the `time::timeout`
/// around the connect block. These tests pin the two behaviours the DoD calls
/// out — the deadline FIRES on a silent peer instead of hanging (AC1), and a
/// successful read RESETS the window (AC2) — plus the `EXPIRE_TIME` derivation,
/// without a live server: a never-completing future stands in for a silent
/// socket and a bounded sleep stands in for a slow-but-alive one.
#[cfg(test)]
mod inactivity_timeout_tests {
    use super::*;
    use std::time::Instant;

    /// `EXPIRE_TIME=0` disables keepalive; a positive value derives an idle
    /// interval of that many MINUTES before the first probe (GH#14, net.pyx).
    #[test]
    fn keepalive_idle_is_derived_from_expire_time_minutes() {
        assert_eq!(keepalive_idle_from_expire_time(0), None);
        assert_eq!(
            keepalive_idle_from_expire_time(1),
            Some(Duration::from_secs(60))
        );
        assert_eq!(
            keepalive_idle_from_expire_time(30),
            Some(Duration::from_secs(30 * 60))
        );
    }

    /// AC1: a silent peer must not wedge a read forever. With a deadline set, a
    /// never-completing read fails with `CallTimeout` reporting the deadline (a
    /// tiny stand-in for the 5 s the bead specifies) — the test terminating at
    /// all is the proof it did not hang past the deadline.
    #[test]
    fn silent_read_trips_the_inactivity_deadline() {
        let runtime = build_io_runtime().expect("io runtime");
        runtime.block_on(async {
            let deadline = Duration::from_millis(120);
            let start = Instant::now();
            let result: Result<()> =
                apply_inactivity_timeout(Some(deadline), std::future::pending::<Result<()>>())
                    .await;
            let elapsed = start.elapsed();
            match result {
                Err(Error::CallTimeout(ms)) => {
                    assert_eq!(ms, duration_to_millis_saturating(deadline));
                }
                other => panic!("expected CallTimeout, got {other:?}"),
            }
            // Fired at ~the deadline, not far beyond it (generous ceiling so a
            // loaded CI box stays green while still catching a real hang).
            assert!(
                elapsed < Duration::from_secs(5),
                "inactivity deadline fired far too late: {elapsed:?}"
            );
        });
    }

    /// A read that completes within the deadline returns its value untouched;
    /// `None` awaits unbounded (the pre-GH#14 behaviour is preserved).
    #[test]
    fn completing_read_is_not_disturbed() {
        let runtime = build_io_runtime().expect("io runtime");
        runtime.block_on(async {
            let bounded: Result<u32> =
                apply_inactivity_timeout(Some(Duration::from_secs(30)), async { Ok(42u32) }).await;
            assert_eq!(bounded.expect("bounded read ok"), 42);

            let unbounded: Result<u32> = apply_inactivity_timeout(None, async { Ok(7u32) }).await;
            assert_eq!(unbounded.expect("unbounded read ok"), 7);
        });
    }

    /// AC2: each read OPERATION gets a fresh deadline window, so a successful
    /// read resets the inactivity clock. Two sequential reads that each finish
    /// inside the deadline both succeed even though their combined time exceeds
    /// a single deadline — a live peer answering keepalives is never wrongly
    /// timed out.
    #[test]
    fn deadline_resets_per_read_operation() {
        let runtime = build_io_runtime().expect("io runtime");
        runtime.block_on(async {
            let deadline = Duration::from_millis(200);
            let op_delay = Duration::from_millis(130);
            // Two ops of 130 ms = 260 ms total, past the 200 ms single-op window.
            for _ in 0..2 {
                let res: Result<()> = apply_inactivity_timeout(Some(deadline), async {
                    time::sleep(time::wall_now(), op_delay).await;
                    Ok(())
                })
                .await;
                res.expect("each within-deadline read succeeds — the deadline resets per op");
            }
        });
    }
}

async fn read_packet_with_limits<R>(
    stream: &mut R,
    width: PacketLengthWidth,
    limits: ProtocolLimits,
) -> Result<IncomingPacket>
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
    limits.check_packet_bytes(declared)?;
    let mut payload = vec![0u8; declared - header.len()];
    stream.read_exact(&mut payload).await?;
    Ok(IncomingPacket {
        packet_type,
        payload,
    })
}

/// The endpoint a listener REDIRECT points at, split out of the redirect
/// data (reference protocol.pyx `_connect_phase_one` redirect branch).
#[derive(Clone, Debug, Eq, PartialEq)]
struct RedirectTarget {
    /// Host to reconnect the transport to.
    host: String,
    /// Port to reconnect the transport to.
    port: u16,
    /// The connect data to send in the CONNECT packet on the redirected
    /// connection (the part of the redirect data after the NUL separator).
    connect_data: String,
}

/// Splits a REDIRECT packet payload into the declared redirect-data length
/// (u16be prefix) and the data bytes carried inline in this packet. The
/// inline bytes may be shorter than the declared length (the remainder then
/// arrives in follow-up packets, reference ConnectMessage.process
/// `wait_for_packets_sync`) or longer (trailing bytes beyond the declared
/// length are ignored).
fn redirect_payload_prefix(payload: &[u8]) -> Result<(usize, &[u8])> {
    let Some((length_bytes, inline)) = payload.split_first_chunk::<2>() else {
        return Err(Error::InvalidRedirectData(format!(
            "REDIRECT packet payload of {} byte(s) is too short for the u16 redirect-data length",
            payload.len()
        )));
    };
    let declared = usize::from(u16::from_be_bytes(*length_bytes));
    let take = inline.len().min(declared);
    Ok((declared, &inline[..take]))
}

/// Assembles the full redirect data starting from the first REDIRECT packet's
/// payload, reading follow-up packets from the same connection while the
/// declared length is not yet satisfied (the listener may send the length in
/// one packet and the data in the next). Follow-up packets must be REDIRECT
/// or DATA packets; anything else fails closed as an unexpected packet.
async fn read_redirect_data(core: &mut DriverCore, first_payload: &[u8]) -> Result<String> {
    let (declared, inline) = redirect_payload_prefix(first_payload)?;
    let mut data = inline.to_vec();
    while data.len() < declared {
        let packet = core.read_packet(PacketLengthWidth::Legacy16).await?;
        if packet.packet_type != TNS_PACKET_TYPE_REDIRECT
            && packet.packet_type != TNS_PACKET_TYPE_DATA
        {
            return Err(Error::UnexpectedPacket(packet.packet_type));
        }
        if packet.payload.is_empty() {
            return Err(Error::InvalidRedirectData(format!(
                "listener stopped short of the declared redirect data \
                 ({} of {declared} byte(s) received)",
                data.len()
            )));
        }
        data.extend_from_slice(&packet.payload);
    }
    data.truncate(declared);
    String::from_utf8(data)
        .map_err(|_| Error::InvalidRedirectData("redirect data is not valid UTF-8".to_string()))
}

/// Parses assembled redirect data (`"<address>\0<connect data>"`) into the
/// target endpoint, enforcing that the redirect keeps the original transport
/// protocol: a `tcps` connect is never silently downgraded to plain `tcp`
/// (and a mid-connect `tcp` -> `tcps` upgrade is not supported). The address
/// part is a TNS address/descriptor fragment, e.g.
/// `(ADDRESS=(PROTOCOL=tcp)(HOST=dispatcher)(PORT=1621))` (reference parses
/// it with `ConnectParamsImpl._parse_connect_string` and takes the first
/// address). NOTE: when the original connect is `tcps`, the redirect address
/// must say `PROTOCOL=tcps` explicitly — an omitted protocol parses as plain
/// `tcp` and is refused as a downgrade (fail closed; the reference instead
/// ignores the redirect protocol entirely and keeps its original transport).
fn parse_redirect_target(
    redirect_data: &str,
    original_protocol: NetProtocol,
) -> Result<RedirectTarget> {
    let Some((address_part, connect_data)) = redirect_data.split_once('\0') else {
        return Err(Error::InvalidRedirectData(
            "missing NUL separator between the redirect address and its connect data".to_string(),
        ));
    };
    let descriptor = connectstring_parse(address_part).map_err(|err| {
        Error::InvalidRedirectData(format!("unparseable redirect address: {err}"))
    })?;
    let address = descriptor
        .as_ref()
        .and_then(|descriptor| descriptor.first_address())
        .ok_or_else(|| {
            Error::InvalidRedirectData("redirect address defines no usable endpoint".to_string())
        })?;
    let host = address
        .host
        .clone()
        .ok_or_else(|| Error::InvalidRedirectData("redirect address has no HOST".to_string()))?;
    let target_protocol: NetProtocol = address.protocol.into();
    if target_protocol.is_tls() != original_protocol.is_tls() {
        return Err(Error::RedirectUnsupported);
    }
    Ok(RedirectTarget {
        host,
        port: address.port,
        connect_data: connect_data.to_string(),
    })
}

/// [`oracledb_protocol::net::connectstring::parse`] under a driver-error
/// signature (used by the redirect-target parser above).
fn connectstring_parse(
    input: &str,
) -> std::result::Result<
    Option<oracledb_protocol::net::connectstring::Descriptor>,
    oracledb_protocol::ProtocolError,
> {
    oracledb_protocol::net::connectstring::parse(input)
}

fn transport_connect_timeout_duration(seconds: f64) -> Duration {
    let seconds = if seconds.is_finite() && seconds > 0.0 {
        seconds
    } else {
        20.0
    };
    Duration::from_secs_f64(seconds.max(0.001))
}

/// The SDU (session data unit, in bytes) advertised in the CONNECT packet
/// (F1, bead `rust-oracledb-clvm`). Precedence: an explicit
/// [`ConnectOptions::with_sdu`] value wins; otherwise a DSN-parsed `(SDU=...)`
/// (`Description::sdu`, already clamped by the connect-string parser to
/// 512..=2_097_152) is honoured; otherwise the shared 8192 default. The caller
/// clamps the result to the 16-bit wire field (the classic CONNECT-packet SDU
/// ceiling of 65535); the server negotiates the effective SDU down from the
/// advertised value in its ACCEPT.
fn resolve_effective_sdu(options_sdu: u16, description: &Description) -> u32 {
    const BUILDER_DEFAULT_SDU: u16 = 8192;
    if options_sdu != BUILDER_DEFAULT_SDU {
        u32::from(options_sdu)
    } else if description.sdu != DSN_DEFAULT_SDU {
        description.sdu
    } else {
        u32::from(BUILDER_DEFAULT_SDU)
    }
}

/// One transport endpoint to try during multi-address failover (F2).
#[derive(Clone, Debug, PartialEq, Eq)]
struct ConnectAddress {
    host: String,
    port: u16,
}

/// Build the ordered list of transport endpoints to try for a (possibly
/// multi-address) connect descriptor (F2, bead `rust-oracledb-clvm`),
/// restricted to the primary transport `protocol`. Honours `LOAD_BALANCE`
/// (shuffle within the balanced scope) and `FAILOVER=OFF` (only the first
/// address of that list). Addresses without a host, or whose protocol differs
/// from the primary, are skipped. The first entry equals the endpoint
/// [`EasyConnect::parse`] selected as primary, so a single-address descriptor
/// behaves exactly as before.
fn resolve_connect_addresses(
    descriptor: &Descriptor,
    protocol: NetProtocol,
) -> Vec<ConnectAddress> {
    let mut out: Vec<ConnectAddress> = Vec::new();
    let mut descriptions: Vec<&Description> = descriptor.descriptions.iter().collect();
    if descriptor.load_balance {
        shuffle_in_place(&mut descriptions);
    }
    for description in descriptions {
        let mut lists: Vec<&AddressList> = description.address_lists.iter().collect();
        if description.load_balance {
            shuffle_in_place(&mut lists);
        }
        for list in lists {
            let mut addresses: Vec<&Address> = list
                .addresses
                .iter()
                .filter(|address| {
                    address.host.is_some() && NetProtocol::from(address.protocol) == protocol
                })
                .collect();
            if list.load_balance || description.load_balance {
                shuffle_in_place(&mut addresses);
            }
            let limit = if list.failover {
                addresses.len()
            } else {
                addresses.len().min(1)
            };
            for address in addresses.into_iter().take(limit) {
                if let Some(host) = &address.host {
                    out.push(ConnectAddress {
                        host: host.clone(),
                        port: address.port,
                    });
                }
            }
        }
    }
    out
}

/// Whether an error justifies trying the next address in a multi-address
/// descriptor (F2). Only transport-establishment failures (the TCP dial or the
/// TLS handshake) fail over; configuration errors (wallet, unsupported SNI,
/// auth) are fatal and abort the whole connect so the operator sees the real
/// cause instead of an aggregated all-addresses-failed message.
fn is_failover_eligible(err: &Error) -> bool {
    matches!(err, Error::Io(_) | Error::Tls(_))
}

/// In-place Fisher–Yates shuffle seeded from the wall clock. `LOAD_BALANCE`
/// only needs unpredictability across connects, not cryptographic randomness,
/// so this stays dependency-free (no `rand` in the driver crate).
fn shuffle_in_place<T>(items: &mut [T]) {
    if items.len() < 2 {
        return;
    }
    let mut state = shuffle_seed();
    for i in (1..items.len()).rev() {
        // xorshift64*
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let j = (state % (i as u64 + 1)) as usize;
        items.swap(i, j);
    }
}

fn shuffle_seed() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9E37_79B9_7F4A_7C15);
    // Mix and force a non-zero (odd) seed so xorshift never degenerates.
    (nanos ^ nanos.rotate_left(32).wrapping_mul(0x2545_F491_4F6C_DD1D)) | 1
}

/// Builds the listener connect descriptor, optionally injecting `(SERVER=emon)`
/// into `CONNECT_DATA` (between `SERVICE_NAME` and `CID`, matching the golden
/// emon connect packet). The reference sets `description.server_type = "emon"`
/// for the background CQN connection (subscr.pyx:70-73).
fn listener_connect_descriptor_with_server(
    descriptor: &EasyConnect,
    description: &Description,
    identity: &ClientIdentity,
    server_type_emon: bool,
    token_auth: bool,
    ssl_server_dn_match: bool,
    ssl_server_cert_dn: Option<&str>,
) -> String {
    let address = descriptor_address_clause(descriptor);
    let connect_data = listener_connect_data_clause(description, identity, server_type_emon);
    let security = descriptor_security_clause(
        descriptor.protocol,
        description,
        token_auth,
        ssl_server_dn_match,
        ssl_server_cert_dn,
    );
    format!("(DESCRIPTION={}{}{})", address, connect_data, security)
}

fn auth_connect_descriptor(
    descriptor: &EasyConnect,
    description: &Description,
    token_auth: bool,
    ssl_server_dn_match: bool,
    ssl_server_cert_dn: Option<&str>,
) -> String {
    let address = descriptor_address_clause(descriptor);
    let connect_data = auth_connect_data_clause(description, descriptor);
    let security = descriptor_security_clause(
        descriptor.protocol,
        description,
        token_auth,
        ssl_server_dn_match,
        ssl_server_cert_dn,
    );
    format!("(DESCRIPTION={}{}{})", address, connect_data, security)
}

fn descriptor_address_clause(descriptor: &EasyConnect) -> String {
    let protocol = match descriptor.protocol {
        NetProtocol::Tcp => "tcp",
        NetProtocol::Tcps => "tcps",
    };
    format!(
        "(ADDRESS=(PROTOCOL={})(HOST={})(PORT={}))",
        protocol, descriptor.host, descriptor.port
    )
}

fn listener_connect_data_clause(
    description: &Description,
    identity: &ClientIdentity,
    server_type_emon: bool,
) -> String {
    let mut out = auth_connect_data_clause_from_service(description, None);
    if server_type_emon {
        out.push_str("(SERVER=emon)");
    } else if let Some(server_type) = description.connect_data.server_type {
        out.push_str("(SERVER=");
        out.push_str(server_type.as_str());
        out.push(')');
    }
    for (key, value) in &description.connect_data.extra {
        out.push('(');
        out.push_str(key);
        out.push('=');
        out.push_str(value);
        out.push(')');
    }
    out.push_str("(CID=(PROGRAM=");
    out.push_str(&identity.program);
    out.push_str(")(HOST=");
    out.push_str(&identity.machine);
    out.push_str(")(USER=");
    out.push_str(&identity.osuser);
    out.push_str(")))");
    out
}

fn auth_connect_data_clause(description: &Description, descriptor: &EasyConnect) -> String {
    let mut out =
        auth_connect_data_clause_from_service(description, Some(&descriptor.service_name));
    if let Some(server_type) = description.connect_data.server_type {
        out.push_str("(SERVER=");
        out.push_str(server_type.as_str());
        out.push(')');
    }
    for (key, value) in &description.connect_data.extra {
        out.push('(');
        out.push_str(key);
        out.push('=');
        out.push_str(value);
        out.push(')');
    }
    out.push(')');
    out
}

fn auth_connect_data_clause_from_service(
    description: &Description,
    fallback_service_name: Option<&str>,
) -> String {
    let service_name = description
        .connect_data
        .service_name
        .as_deref()
        .or(fallback_service_name)
        .unwrap_or("");
    format!("(CONNECT_DATA=(SERVICE_NAME={service_name})")
}

fn descriptor_security_clause(
    protocol: NetProtocol,
    description: &Description,
    token_auth: bool,
    ssl_server_dn_match: bool,
    ssl_server_cert_dn: Option<&str>,
) -> String {
    let security = &description.security;
    if !protocol.is_tls()
        && !token_auth
        && ssl_server_dn_match
        && ssl_server_cert_dn.is_none()
        && security.extra.is_empty()
    {
        return String::new();
    }

    let mut out = String::from("(SECURITY=");
    if protocol.is_tls() || !ssl_server_dn_match {
        out.push_str("(SSL_SERVER_DN_MATCH=");
        out.push_str(if ssl_server_dn_match { "ON" } else { "OFF" });
        out.push(')');
    }
    if let Some(cert_dn) = ssl_server_cert_dn {
        out.push_str("(SSL_SERVER_CERT_DN=");
        out.push_str(cert_dn);
        out.push(')');
    }
    for (key, value) in &security.extra {
        if key.eq_ignore_ascii_case("TOKEN_AUTH") {
            continue;
        }
        out.push('(');
        out.push_str(key);
        out.push('=');
        out.push_str(value);
        out.push(')');
    }
    if token_auth {
        out.push_str("(TOKEN_AUTH=OCI_TOKEN)");
    }
    out.push(')');
    out
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
        .parse::<u64>()
        .map(|value| value as u16)
        .map_err(|_| Error::MissingSessionField(key))
}

/// Extract `db_unique_name` from the AUTH phase-two session data: the value of
/// `AUTH_SC_REAL_DBUNIQUE_NAME` ONLY (reference 16a57f1cbd58). This key is
/// DISTINCT from `AUTH_SC_DBUNIQUE_NAME` (which upstream maps to `db_name`), so
/// there is deliberately no fallback. `None` when the key is absent.
fn parse_db_unique_name(data: &std::collections::BTreeMap<String, String>) -> Option<String> {
    data.get("AUTH_SC_REAL_DBUNIQUE_NAME").cloned()
}

fn next_ttc_sequence(seq_num: &mut u8) -> u8 {
    *seq_num = seq_num.wrapping_add(1);
    if *seq_num == 0 {
        *seq_num = 1;
    }
    *seq_num
}

/// LRU statement-cache insert, bounded to `capacity`. Moves an existing entry
/// for `sql` to most-recently-used (replacing its cursor) and evicts the oldest
/// entries past `capacity`. Returns the cursor ids that should be closed (the
/// replaced cursor and any evicted ones). `capacity == 0` disables caching: the
/// freshly inserted cursor is itself evicted and returned for closing, so the
/// cache stays empty (python-oracledb `stmtcachesize=0`). A `cursor_id` of 0
/// (no server cursor) is never cached. Pure so it is unit-testable without a
/// live connection.
fn statement_cache_insert(
    cache: &mut Vec<CachedStatement>,
    capacity: usize,
    sql: &str,
    cursor_id: u32,
    mut bind_shape: Vec<BindShapeSlot>,
) -> Vec<u32> {
    let mut to_close = Vec::new();
    if cursor_id == 0 {
        return to_close;
    }
    if let Some(index) = cache.iter().position(|entry| entry.sql == sql) {
        let old = cache.remove(index);
        if old.cursor_id != 0 && old.cursor_id != cursor_id {
            to_close.push(old.cursor_id);
        } else if old.cursor_id == cursor_id && old.bind_shape.len() == bind_shape.len() {
            // Re-execution of the SAME open cursor: an untyped-NULL bind is
            // written with placeholder VARCHAR metadata and does not disturb
            // the type the cursor was parsed with, so the previous concrete
            // slot is inherited instead of downgrading it to the placeholder.
            for (slot, old_slot) in bind_shape.iter_mut().zip(old.bind_shape) {
                if slot.untyped_null {
                    *slot = old_slot;
                }
            }
        }
    }
    cache.push(CachedStatement {
        sql: sql.to_string(),
        cursor_id,
        bind_shape,
    });
    while cache.len() > capacity {
        let evicted = cache.remove(0);
        if evicted.cursor_id != 0 {
            to_close.push(evicted.cursor_id);
        }
    }
    to_close
}

/// One statement-cache entry: the open server cursor for a SQL text and the
/// bind TYPE shape it was last bound/parsed with (see
/// [`Connection::statement_cache`] and bead rust-oracledb-ilel).
#[derive(Clone, Debug, PartialEq, Eq)]
struct CachedStatement {
    sql: String,
    cursor_id: u32,
    bind_shape: Vec<BindShapeSlot>,
}

/// Per-position bind TYPE shape used to decide whether an open cached cursor
/// may be re-executed with the current binds. The server resolves bind
/// conversions against the metadata the cursor was PARSED with, not the
/// metadata sent on a re-execute: rebinding a different type through a cached
/// cursor makes the server coerce through the stale type (ORA-01722 when text
/// rides a NUMBER-parsed cursor). python-oracledb tracks the same change via
/// `Statement._binds_changed` (thin/statement.pyx `_set_var`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BindShapeSlot {
    /// Bind type folded to its interchangeable family: CHAR/VARCHAR/LONG all
    /// map to VARCHAR and RAW/LONG_RAW to RAW (mirroring the wire writer's
    /// `bind_metadata_types_are_compatible`), so a pure SIZE change never
    /// invalidates the cursor — only a TYPE change does.
    family: u8,
    /// Character-set form: distinguishes NCHAR text from implicit-charset
    /// text (python-oracledb `_set_var` also compares `csfrm`).
    csfrm: u8,
    /// True when every row's value at this position is an untyped NULL. Such
    /// a bind is written with placeholder VARCHAR metadata and converts to
    /// any parsed type server-side, so it is compatible with any cached slot.
    untyped_null: bool,
}

/// Folds a wire bind type into its statement-cache compatibility family (the
/// same classes the metadata writer merges across bind rows).
fn bind_type_family(ora_type_num: u8) -> u8 {
    use oracledb_protocol::thin::{
        ORA_TYPE_NUM_CHAR, ORA_TYPE_NUM_LONG, ORA_TYPE_NUM_LONG_RAW, ORA_TYPE_NUM_RAW,
        ORA_TYPE_NUM_VARCHAR,
    };
    match ora_type_num {
        ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_LONG => ORA_TYPE_NUM_VARCHAR,
        ORA_TYPE_NUM_LONG_RAW => ORA_TYPE_NUM_RAW,
        other => other,
    }
}

/// Computes the bind TYPE shape of an execute's bind rows. Mirrors the wire
/// metadata writer's inference (`write_bind_metadata_for_rows`): the first
/// non-NULL value in a column determines its type; a column that is untyped
/// NULL in every row is a wildcard slot.
fn bind_type_shape(bind_rows: &[Vec<BindValue>]) -> Vec<BindShapeSlot> {
    use oracledb_protocol::thin::{bind_value_type_info, CS_FORM_IMPLICIT, ORA_TYPE_NUM_VARCHAR};
    let Some(first_row) = bind_rows.first() else {
        return Vec::new();
    };
    (0..first_row.len())
        .map(|index| {
            let info = bind_rows
                .iter()
                .find_map(|row| row.get(index).and_then(bind_value_type_info));
            match info {
                Some(info) => BindShapeSlot {
                    family: bind_type_family(info.ora_type_num),
                    csfrm: info.csfrm,
                    untyped_null: false,
                },
                None => BindShapeSlot {
                    family: ORA_TYPE_NUM_VARCHAR,
                    csfrm: CS_FORM_IMPLICIT,
                    untyped_null: true,
                },
            }
        })
        .collect()
}

/// True when a cached cursor bound with `cached` may be re-executed with
/// binds of shape `new`: every position must keep its type family + charset
/// form, except that an untyped NULL in the NEW binds matches anything (it is
/// written as a placeholder and null-converts to any parsed type).
fn bind_shape_is_compatible(cached: &[BindShapeSlot], new: &[BindShapeSlot]) -> bool {
    cached.len() == new.len()
        && cached.iter().zip(new).all(|(cached, new)| {
            new.untyped_null || (cached.family == new.family && cached.csfrm == new.csfrm)
        })
}

/// Returns whether Oracle will execute `sql` on its row-producing path.
///
/// A leading `WITH` does not determine that by itself: Oracle permits a CTE
/// list before `SELECT` and before DML. Walk each complete CTE definition, then
/// inspect the first statement keyword after the list. Syntax we cannot
/// confidently walk stays on the non-query path.
fn statement_is_query(sql: &str) -> bool {
    let mut cursor = SqlStatementCursor::new(sql);
    match cursor.next_keyword() {
        Some(keyword) if keyword.eq_ignore_ascii_case("select") => true,
        Some(keyword) if keyword.eq_ignore_ascii_case("with") => cursor
            .cte_statement_keyword()
            .is_some_and(|keyword| keyword.eq_ignore_ascii_case("select")),
        _ => false,
    }
}

/// A deliberately narrow SQL cursor for query-path selection. It understands
/// comments, quoted strings/identifiers, balanced parentheses, and the CTE
/// grammar prefix; it is not a general SQL parser.
struct SqlStatementCursor<'a> {
    bytes: &'a [u8],
    index: usize,
}

impl<'a> SqlStatementCursor<'a> {
    fn new(sql: &'a str) -> Self {
        Self {
            bytes: sql.as_bytes(),
            index: 0,
        }
    }

    fn next_keyword(&mut self) -> Option<&'a str> {
        self.skip_trivia();
        let start = self.index;
        while self
            .bytes
            .get(self.index)
            .is_some_and(u8::is_ascii_alphabetic)
        {
            self.index += 1;
        }
        (start != self.index)
            .then(|| std::str::from_utf8(&self.bytes[start..self.index]).ok())
            .flatten()
    }

    /// Return the statement keyword after one or more conventional CTEs.
    ///
    /// This accepts only `name [(columns)] AS (subquery)` definitions. A
    /// `WITH FUNCTION` or malformed `WITH` is intentionally not interpreted
    /// as a query, because treating it as one could route a non-query through
    /// the fetch path.
    fn cte_statement_keyword(&mut self) -> Option<&'a str> {
        loop {
            if !self.consume_identifier() {
                return None;
            }
            self.skip_trivia();
            if self.peek() == Some(b'(') && !self.consume_parentheses() {
                return None;
            }
            if !self.consume_keyword("as") || !self.consume_parentheses() {
                return None;
            }
            self.skip_trivia();
            if self.peek() == Some(b',') {
                self.index += 1;
                continue;
            }
            return self.next_keyword();
        }
    }

    fn consume_keyword(&mut self, expected: &str) -> bool {
        self.next_keyword()
            .is_some_and(|keyword| keyword.eq_ignore_ascii_case(expected))
    }

    fn consume_identifier(&mut self) -> bool {
        self.skip_trivia();
        if self.peek() == Some(b'\"') {
            return self.consume_quoted_identifier();
        }
        let start = self.index;
        if !self.peek().is_some_and(|byte| byte.is_ascii_alphabetic()) {
            return false;
        }
        while self
            .bytes
            .get(self.index)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$' | b'#'))
        {
            self.index += 1;
        }
        start != self.index
    }

    fn consume_parentheses(&mut self) -> bool {
        self.skip_trivia();
        if self.peek() != Some(b'(') {
            return false;
        }
        let mut depth = 0_u32;
        while let Some(byte) = self.peek() {
            match byte {
                b'\'' => {
                    if !self.consume_single_quoted() {
                        return false;
                    }
                }
                b'\"' => {
                    if !self.consume_quoted_identifier() {
                        return false;
                    }
                }
                b'q' | b'Q' if self.bytes.get(self.index + 1) == Some(&b'\'') => {
                    if !self.consume_q_quoted() {
                        return false;
                    }
                }
                b'-' if self.bytes.get(self.index + 1) == Some(&b'-') => {
                    self.skip_line_comment();
                }
                b'/' if self.bytes.get(self.index + 1) == Some(&b'*') => {
                    if !self.skip_block_comment() {
                        return false;
                    }
                }
                b'(' => {
                    depth += 1;
                    self.index += 1;
                }
                b')' => {
                    if depth == 0 {
                        return false;
                    }
                    depth -= 1;
                    self.index += 1;
                    if depth == 0 {
                        return true;
                    }
                }
                _ => self.index += 1,
            }
        }
        false
    }

    fn skip_trivia(&mut self) {
        loop {
            while self.peek().is_some_and(|byte| byte.is_ascii_whitespace()) {
                self.index += 1;
            }
            match (self.peek(), self.bytes.get(self.index + 1)) {
                (Some(b'-'), Some(b'-')) => self.skip_line_comment(),
                (Some(b'/'), Some(b'*')) => {
                    if !self.skip_block_comment() {
                        return;
                    }
                }
                _ => return,
            }
        }
    }

    fn consume_single_quoted(&mut self) -> bool {
        debug_assert_eq!(self.peek(), Some(b'\''));
        self.index += 1;
        while let Some(byte) = self.peek() {
            self.index += 1;
            if byte == b'\'' {
                if self.peek() == Some(b'\'') {
                    self.index += 1;
                } else {
                    return true;
                }
            }
        }
        false
    }

    fn consume_quoted_identifier(&mut self) -> bool {
        debug_assert_eq!(self.peek(), Some(b'\"'));
        self.index += 1;
        while let Some(byte) = self.peek() {
            self.index += 1;
            if byte == b'\"' {
                if self.peek() == Some(b'\"') {
                    self.index += 1;
                } else {
                    return true;
                }
            }
        }
        false
    }

    fn consume_q_quoted(&mut self) -> bool {
        debug_assert!(matches!(self.peek(), Some(b'q' | b'Q')));
        let Some(&opening) = self.bytes.get(self.index + 2) else {
            return false;
        };
        let closing = match opening {
            b'[' => b']',
            b'{' => b'}',
            b'(' => b')',
            b'<' => b'>',
            other => other,
        };
        self.index += 3;
        while self.index + 1 < self.bytes.len() {
            if self.bytes[self.index] == closing && self.bytes[self.index + 1] == b'\'' {
                self.index += 2;
                return true;
            }
            self.index += 1;
        }
        false
    }

    fn skip_line_comment(&mut self) {
        debug_assert_eq!(
            self.bytes.get(self.index..self.index + 2),
            Some(b"--".as_slice())
        );
        self.index += 2;
        while self
            .peek()
            .is_some_and(|byte| !matches!(byte, b'\n' | b'\r'))
        {
            self.index += 1;
        }
    }

    fn skip_block_comment(&mut self) -> bool {
        debug_assert_eq!(
            self.bytes.get(self.index..self.index + 2),
            Some(b"/*".as_slice())
        );
        self.index += 2;
        while self.index + 1 < self.bytes.len() {
            if self.bytes[self.index] == b'*' && self.bytes[self.index + 1] == b'/' {
                self.index += 2;
                return true;
            }
            self.index += 1;
        }
        false
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.index).copied()
    }
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
            column.ora_type_num(),
            ORA_TYPE_NUM_CLOB | ORA_TYPE_NUM_BLOB | ORA_TYPE_NUM_VECTOR | ORA_TYPE_NUM_JSON
        )
    })
}

fn columns_have_lob_prefetch_fields(columns: &[ColumnMetadata]) -> bool {
    use oracledb_protocol::thin::{ORA_TYPE_NUM_BLOB, ORA_TYPE_NUM_CLOB};
    columns
        .iter()
        .any(|column| matches!(column.ora_type_num(), ORA_TYPE_NUM_CLOB | ORA_TYPE_NUM_BLOB))
}

/// Columns that warrant an `ALL_JSON_COLUMNS` probe to learn whether a CLOB/BLOB
/// actually stores JSON: not already flagged JSON, of LOB type, and named (an
/// unnamed expression column cannot be looked up by name). Returns each
/// candidate's index paired with its upper-cased name (the catalog stores names
/// upper-cased) so the caller can flip `is_json` in place after the probe.
fn json_lob_probe_candidates(columns: &[ColumnMetadata]) -> Vec<(usize, String)> {
    use oracledb_protocol::thin::{ORA_TYPE_NUM_BLOB, ORA_TYPE_NUM_CLOB};
    columns
        .iter()
        .enumerate()
        .filter(|(_, metadata)| {
            !metadata.is_json()
                && matches!(
                    metadata.ora_type_num(),
                    ORA_TYPE_NUM_CLOB | ORA_TYPE_NUM_BLOB
                )
                && !metadata.name().is_empty()
        })
        .map(|(index, metadata)| (index, metadata.name().to_ascii_uppercase()))
        .collect()
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
    use asupersync::lab::{DporExplorer, ExplorerConfig, LabRuntime};
    use asupersync::types::{Budget, CancelKind, Time};
    use oracledb_protocol::thin::QueryValue;
    use std::future::{poll_fn, Future};
    use std::io::Read;
    use std::net::TcpListener;
    use std::pin::pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Poll, Waker};
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn statement_is_query_recognizes_select_after_cte_list() {
        for sql in [
            "WITH x AS (SELECT 1 AS id FROM dual) SELECT id FROM x",
            concat!(
                "WITH first_cte AS (SELECT 1 AS id FROM dual), ",
                "second_cte AS (SELECT id + 1 AS id FROM first_cte) ",
                "SELECT id FROM second_cte"
            ),
            concat!(
                "WITH outer_cte AS (",
                "WITH inner_cte AS (SELECT q'[)]' AS marker FROM dual) ",
                "SELECT marker FROM inner_cte",
                ") SELECT marker FROM outer_cte"
            ),
        ] {
            assert!(
                statement_is_query(sql),
                "CTE SELECT must use query path: {sql}"
            );
        }
    }

    #[test]
    fn statement_is_query_rejects_cte_prefixed_dml_and_plsql() {
        for sql in [
            "WITH x AS (SELECT 1 AS id FROM dual) INSERT INTO cte_target (id) SELECT id FROM x",
            "WITH x AS (SELECT 1 AS id FROM dual) UPDATE cte_target SET id = id + 1",
            "WITH x AS (SELECT 1 AS id FROM dual) DELETE FROM cte_target WHERE id IN (SELECT id FROM x)",
            concat!(
                "WITH x AS (SELECT 1 AS id FROM dual) ",
                "MERGE INTO cte_target target USING x ON (target.id = x.id) ",
                "WHEN MATCHED THEN UPDATE SET target.id = x.id"
            ),
            concat!(
                "WITH FUNCTION answer RETURN NUMBER IS BEGIN RETURN 42; END; ",
                "SELECT answer FROM dual"
            ),
        ] {
            assert!(
                !statement_is_query(sql),
                "only SELECT after a conventional CTE list may use query path: {sql}"
            );
        }
    }

    #[test]
    fn statement_is_query_skips_leading_whitespace_and_comments() {
        for sql in [
            " \n\t-- leading line comment\nWITH x AS (SELECT 1 FROM dual) SELECT * FROM x",
            "/* leading block comment */ WITH x AS (SELECT 1 FROM dual) SELECT * FROM x",
            "/* leading block comment */\nSELECT 1 FROM dual",
        ] {
            assert!(
                statement_is_query(sql),
                "comments must not hide a query: {sql}"
            );
        }
    }

    // ---- bead rust-oracledb-clvm: DSN transport params (F1) + failover (F2) ----

    /// Read the SDU advertised in a CONNECT-packet payload (the 4th `u16be`
    /// field; see `build_connect_packet_payload`).
    fn connect_packet_sdu(payload: &[u8]) -> u16 {
        u16::from_be_bytes([payload[6], payload[7]])
    }

    #[test]
    fn f1_dsn_sdu_reaches_the_connect_config() {
        // A DSN-set SDU must reach the connect config when the structured
        // builder left SDU at its default (the common DSN-only case).
        let desc = EasyConnect::parse_descriptor(
            "(DESCRIPTION=(SDU=32768)(ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1521))\
             (CONNECT_DATA=(SERVICE_NAME=svc)))",
        )
        .expect("descriptor parses");
        let primary = desc.first_description();
        assert_eq!(primary.sdu, 32768, "parser must capture DSN SDU");
        // builder default (8192) => DSN SDU wins.
        let advertised =
            u16::try_from(resolve_effective_sdu(8192, primary)).expect("32768 fits u16");
        assert_eq!(advertised, 32768);
        // and it reaches the actual CONNECT packet bytes.
        let payload = build_connect_packet_payload(
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1521))\
             (CONNECT_DATA=(SERVICE_NAME=svc)))",
            advertised,
        )
        .expect("payload builds");
        assert_eq!(connect_packet_sdu(&payload), 32768);
    }

    #[test]
    fn f1_effective_sdu_precedence() {
        let dsn = EasyConnect::parse_descriptor(
            "(DESCRIPTION=(SDU=16384)(ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1))\
             (CONNECT_DATA=(SERVICE_NAME=s)))",
        )
        .expect("parse");
        let desc = dsn.first_description();
        // explicit builder SDU wins over the DSN.
        assert_eq!(resolve_effective_sdu(4096, desc), 4096);
        // builder at default => DSN SDU used.
        assert_eq!(resolve_effective_sdu(8192, desc), 16384);
        // neither set => shared default.
        let plain = EasyConnect::parse_descriptor(
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1))(CONNECT_DATA=(SERVICE_NAME=s)))",
        )
        .expect("parse");
        assert_eq!(resolve_effective_sdu(8192, plain.first_description()), 8192);
    }

    #[test]
    fn f1_dsn_transport_connect_timeout_reaches_the_deadline() {
        // A DSN-set transport_connect_timeout must drive the connect deadline,
        // not the hard-coded 20s.
        let desc = EasyConnect::parse_descriptor("h:1521/svc?transport_connect_timeout=3.5")
            .expect("EZConnect-Plus parses");
        let secs = desc.first_description().tcp_connect_timeout;
        assert!((secs - 3.5).abs() < 1e-9, "parser captured {secs}");
        let deadline = transport_connect_timeout_duration(secs);
        assert_eq!(deadline, Duration::from_secs_f64(3.5));
        // sanity: a descriptor without the param keeps the 20s default.
        let default_desc = EasyConnect::parse_descriptor("h:1521/svc").expect("parse");
        assert_eq!(
            transport_connect_timeout_duration(
                default_desc.first_description().tcp_connect_timeout
            ),
            Duration::from_secs(20)
        );
    }

    /// GH#14: the TCP keepalive idle interval is derived from a DSN `EXPIRE_TIME`
    /// (minutes). `0`/absent disables it; a positive value maps to that many
    /// minutes of socket idle time.
    #[test]
    fn a11_keepalive_idle_derives_from_expire_time_minutes() {
        assert_eq!(keepalive_idle_from_expire_time(0), None);
        assert_eq!(
            keepalive_idle_from_expire_time(1),
            Some(Duration::from_secs(60))
        );
        assert_eq!(
            keepalive_idle_from_expire_time(5),
            Some(Duration::from_secs(300))
        );
        // Tie the derivation to the DSN parser: a descriptor `EXPIRE_TIME=2`
        // yields a 120s keepalive idle; the default descriptor disables it.
        let with_expire =
            EasyConnect::parse_descriptor("h:1521/svc?expire_time=2").expect("parse expire_time");
        assert_eq!(
            keepalive_idle_from_expire_time(with_expire.first_description().expire_time),
            Some(Duration::from_secs(120))
        );
        let default_desc = EasyConnect::parse_descriptor("h:1521/svc").expect("parse default");
        assert_eq!(
            keepalive_idle_from_expire_time(default_desc.first_description().expire_time),
            None
        );
    }

    /// An `AsyncRead` that never yields a byte and never wakes — models a silent
    /// or half-open peer whose data (or FIN/RST) never arrives, so a naive read
    /// would hang forever.
    #[derive(Debug)]
    struct SilentRead;

    impl asupersync::io::AsyncRead for SilentRead {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut asupersync::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Pending
        }
    }

    /// GH#14 / AC1+AC4+AC5: with an inactivity deadline set, a post-auth framing
    /// read against a silent server fails at ~the deadline with `CallTimeout`
    /// instead of hanging. Deterministic via the `SilentRead` mock transport.
    #[test]
    fn a11_inactivity_deadline_fires_on_a_silent_server() {
        let runtime = build_io_runtime().expect("io runtime");
        let elapsed = runtime.block_on(async {
            let mut reader = SilentRead;
            let started = std::time::Instant::now();
            let result = apply_inactivity_timeout(
                Some(Duration::from_millis(200)),
                read_packet_with_limits(
                    &mut reader,
                    PacketLengthWidth::Large32,
                    ProtocolLimits::DEFAULT,
                ),
            )
            .await;
            let elapsed = started.elapsed();
            assert!(
                matches!(result, Err(Error::CallTimeout(_))),
                "expected CallTimeout, got {result:?}"
            );
            elapsed
        });
        // Fired promptly at ~200ms — nowhere near an unbounded hang. The bounds
        // are generous to stay robust on a loaded CI host.
        assert!(
            elapsed >= Duration::from_millis(150),
            "deadline fired too early ({elapsed:?}); it must honour the configured 200ms"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "deadline should fire promptly, took {elapsed:?}"
        );
    }

    /// GH#14: `None` (the default) imposes no deadline — a read that completes
    /// passes through unchanged, and a generous deadline does not disturb a fast
    /// read. Guards against the wrapper regressing existing unbounded behaviour.
    #[test]
    fn a11_inactivity_none_and_slack_pass_through() {
        let runtime = build_io_runtime().expect("io runtime");
        runtime.block_on(async {
            let none: Result<u32> =
                apply_inactivity_timeout(None, async { Ok::<u32, Error>(42) }).await;
            assert_eq!(none.expect("None passes value through"), 42);
            let slack: Result<u32> =
                apply_inactivity_timeout(Some(Duration::from_secs(30)), async {
                    Ok::<u32, Error>(7)
                })
                .await;
            assert_eq!(slack.expect("fast op beats the deadline"), 7);
        });
    }

    #[test]
    fn f2_address_list_yields_all_addresses_in_order() {
        let desc = EasyConnect::parse_descriptor(
            "(DESCRIPTION=(ADDRESS_LIST=\
             (ADDRESS=(PROTOCOL=tcp)(HOST=primary)(PORT=1521))\
             (ADDRESS=(PROTOCOL=tcp)(HOST=standby)(PORT=1522)))\
             (CONNECT_DATA=(SERVICE_NAME=svc)))",
        )
        .expect("parse");
        let addrs = resolve_connect_addresses(&desc, NetProtocol::Tcp);
        assert_eq!(
            addrs,
            vec![
                ConnectAddress {
                    host: "primary".into(),
                    port: 1521
                },
                ConnectAddress {
                    host: "standby".into(),
                    port: 1522
                },
            ]
        );
    }

    #[test]
    fn f2_failover_off_keeps_only_first_address() {
        let desc = EasyConnect::parse_descriptor(
            "(DESCRIPTION=(ADDRESS_LIST=(FAILOVER=OFF)\
             (ADDRESS=(PROTOCOL=tcp)(HOST=only)(PORT=1521))\
             (ADDRESS=(PROTOCOL=tcp)(HOST=nope)(PORT=1522)))\
             (CONNECT_DATA=(SERVICE_NAME=svc)))",
        )
        .expect("parse");
        let addrs = resolve_connect_addresses(&desc, NetProtocol::Tcp);
        assert_eq!(
            addrs,
            vec![ConnectAddress {
                host: "only".into(),
                port: 1521
            }]
        );
    }

    #[test]
    fn f2_load_balance_preserves_address_set() {
        // LOAD_BALANCE may reorder, but every address must still be present.
        let desc = EasyConnect::parse_descriptor(
            "(DESCRIPTION=(ADDRESS_LIST=(LOAD_BALANCE=ON)\
             (ADDRESS=(PROTOCOL=tcp)(HOST=a)(PORT=1))\
             (ADDRESS=(PROTOCOL=tcp)(HOST=b)(PORT=2))\
             (ADDRESS=(PROTOCOL=tcp)(HOST=c)(PORT=3)))\
             (CONNECT_DATA=(SERVICE_NAME=svc)))",
        )
        .expect("parse");
        let mut addrs = resolve_connect_addresses(&desc, NetProtocol::Tcp);
        addrs.sort_by(|l, r| l.port.cmp(&r.port));
        assert_eq!(
            addrs,
            vec![
                ConnectAddress {
                    host: "a".into(),
                    port: 1
                },
                ConnectAddress {
                    host: "b".into(),
                    port: 2
                },
                ConnectAddress {
                    host: "c".into(),
                    port: 3
                },
            ]
        );
    }

    #[test]
    fn f2_only_primary_protocol_addresses_are_candidates() {
        // A mixed-protocol descriptor only yields the primary-protocol
        // endpoints (the transport/TLS setup is resolved for one protocol).
        let desc = EasyConnect::parse_descriptor(
            "(DESCRIPTION=(ADDRESS_LIST=\
             (ADDRESS=(PROTOCOL=tcp)(HOST=plain)(PORT=1521))\
             (ADDRESS=(PROTOCOL=tcps)(HOST=secure)(PORT=2484)))\
             (CONNECT_DATA=(SERVICE_NAME=svc)))",
        )
        .expect("parse");
        assert_eq!(
            resolve_connect_addresses(&desc, NetProtocol::Tcp),
            vec![ConnectAddress {
                host: "plain".into(),
                port: 1521
            }]
        );
        assert_eq!(
            resolve_connect_addresses(&desc, NetProtocol::Tcps),
            vec![ConnectAddress {
                host: "secure".into(),
                port: 2484
            }]
        );
    }

    #[test]
    fn f2_failover_only_covers_transport_errors() {
        // TCP dial / TLS handshake failures fail over; config/auth errors abort.
        assert!(is_failover_eligible(&Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused"
        ))));
        assert!(is_failover_eligible(&Error::Tls("handshake".into())));
        assert!(!is_failover_eligible(&Error::UnsupportedSni(
            "S1.X.V3.319".into()
        )));
        assert!(!is_failover_eligible(&Error::AccessTokenRequiresTcps));
    }

    #[test]
    fn api_design_nothing_lost_map_covers_current_surface() {
        // `BindValue` / `QueryValue` are `#[non_exhaustive]`: the exhaustive
        // variant-name `match` that flags a newly-added variant now lives on the
        // types themselves (in oracledb-protocol), so this cross-crate test reads
        // through the stable `variant_name()` accessor instead of a fragile
        // external match. The compile-time tripwire is preserved where the enums
        // are defined.
        assert_eq!(BindValue::Null.variant_name(), "Null");
        assert_eq!(QueryValue::Text(String::new()).variant_name(), "Text");

        let design = include_str!("../../../docs/API_DESIGN.md");
        for method in [
            "execute_query_for_registration",
            "execute_query",
            "execute_query_collect",
            "execute_query_with_timeout",
            "execute_query_with_binds",
            "execute_query_with_binds_and_timeout",
            "query",
            "query_named",
            "query_named_with_timeout",
            "execute_query_with_bind_rows",
            "execute_query_with_bind_rows_and_options",
            "execute_query_with_bind_rows_and_timeout",
            "execute_query_with_bind_rows_options_and_timeout",
        ] {
            assert!(design.contains(method), "API_DESIGN.md missing {method}");
        }

        for group in 1..=24 {
            let marker = format!("C{group:02}");
            assert!(
                design.contains(&marker),
                "API_DESIGN.md missing capability group {marker}"
            );
        }

        for field in [
            "batcherrors",
            "arraydmlrowcounts",
            "parse_only",
            "token_num",
            "cursor_id",
            "cache_statement",
            "scrollable",
            "fetch_orientation",
            "fetch_pos",
            "scroll_operation",
            "suspend_on_success",
            "no_prefetch",
            "registration_id",
            "max_string_size",
        ] {
            assert!(
                design.contains(field),
                "API_DESIGN.md missing ExecuteOptions field {field}"
            );
        }

        let bind_variants = [
            "Null",
            "TypedNull",
            "Output",
            "ReturnOutput",
            "ObjectOutput",
            "ObjectInput",
            "Text",
            "Raw",
            "Lob",
            "Number",
            "BinaryInteger",
            "BinaryDouble",
            "BinaryFloat",
            "Boolean",
            "IntervalDS",
            "IntervalYM",
            "DateTime",
            "Timestamp",
            "TimestampTz",
            "Array",
            "Vector",
            "Json",
            "Cursor",
        ];
        assert_eq!(bind_variants.len(), 23);
        for variant in bind_variants {
            assert!(
                design.contains(&format!("`{variant}`")),
                "API_DESIGN.md missing BindValue::{variant}"
            );
        }

        let query_variants = [
            "Text",
            "TextRaw",
            "Raw",
            "Rowid",
            "BinaryDouble",
            "IntervalDS",
            "IntervalYM",
            "Number",
            "Boolean",
            "Cursor",
            "DateTime",
            "TimestampTz",
            "Object",
            "Lob",
            "Vector",
            "Json",
            "Array",
        ];
        assert_eq!(query_variants.len(), 17);
        for variant in query_variants {
            assert!(
                design.contains(&format!("`{variant}`")),
                "API_DESIGN.md missing QueryValue::{variant}"
            );
        }
    }

    #[test]
    fn migration_guide_covers_every_deprecated_method() {
        // No orphan removal: every old execute/query name that carried a
        // `#[deprecated(since = "0.3.0")]` shim and is now removed in
        // 0.5.0 must still appear in the user-facing 0.3.0 migration
        // guide, so an external consumer can always find the replacement.
        // These are exactly the names listed in API_DESIGN.md §8.
        let guide = include_str!("../../../docs/MIGRATING-0.3.md");
        for method in [
            "execute_query",
            "execute_query_collect",
            "execute_query_with_timeout",
            "execute_query_with_binds",
            "execute_query_with_binds_and_timeout",
            "query_named",
            "query_named_with_timeout",
            "execute_query_with_bind_rows",
            "execute_query_with_bind_rows_and_options",
            "execute_query_with_bind_rows_and_timeout",
            "execute_query_with_bind_rows_options_and_timeout",
            "execute_query_for_registration",
        ] {
            assert!(
                guide.contains(method),
                "MIGRATING-0.3.md missing deprecated method {method}"
            );
        }
        // The replacement families and builders must be documented too.
        for replacement in [
            "query_with",
            "execute_with",
            "execute_many",
            "execute_many_with",
            "register_query",
            "query_one",
            "query_opt",
            "query_all",
            "Query::timeout",
            "Execute::raw_options",
            "Batch::raw_options",
            "Registration::new",
            "execute_raw",
        ] {
            assert!(
                guide.contains(replacement),
                "MIGRATING-0.3.md missing replacement {replacement}"
            );
        }
        // The one-release removal window must be stated so external users know
        // the shims disappear in 0.5.0.
        assert!(
            guide.contains("0.5.0"),
            "MIGRATING-0.3.md must state the shims are removed in 0.5.0"
        );
    }

    fn cache_entry(sql: &str, cursor_id: u32) -> CachedStatement {
        CachedStatement {
            sql: sql.into(),
            cursor_id,
            bind_shape: Vec::new(),
        }
    }

    #[test]
    fn statement_cache_evicts_lru_past_capacity() {
        let mut cache = Vec::new();
        // capacity 2: a third distinct statement evicts the oldest (cursor 10).
        assert!(statement_cache_insert(&mut cache, 2, "a", 10, Vec::new()).is_empty());
        assert!(statement_cache_insert(&mut cache, 2, "b", 11, Vec::new()).is_empty());
        assert_eq!(
            statement_cache_insert(&mut cache, 2, "c", 12, Vec::new()),
            vec![10]
        );
        assert_eq!(
            cache,
            vec![cache_entry("b", 11), cache_entry("c", 12)],
            "LRU order retained"
        );
        // Re-inserting an existing SQL with a new cursor closes the old cursor
        // and moves it to most-recently-used; nothing is evicted.
        assert_eq!(
            statement_cache_insert(&mut cache, 2, "b", 99, Vec::new()),
            vec![11]
        );
        assert_eq!(cache, vec![cache_entry("c", 12), cache_entry("b", 99)]);
    }

    #[test]
    fn statement_cache_size_zero_disables_caching() {
        let mut cache = Vec::new();
        // capacity 0: the freshly inserted cursor is itself evicted (queued for
        // close) and the cache stays empty — caching disabled.
        assert_eq!(
            statement_cache_insert(&mut cache, 0, "a", 10, Vec::new()),
            vec![10]
        );
        assert!(cache.is_empty(), "size 0 must never retain a statement");
        // A no-cursor (0) insert is never cached and closes nothing.
        assert!(statement_cache_insert(&mut cache, 5, "a", 0, Vec::new()).is_empty());
        assert!(cache.is_empty());
    }

    fn shape_of(values: &[BindValue]) -> Vec<BindShapeSlot> {
        bind_type_shape(&[values.to_vec()])
    }

    #[test]
    fn bind_type_shape_change_is_incompatible_but_size_change_is_not() {
        use oracledb_protocol::thin::ORA_TYPE_NUM_LONG;
        let number = shape_of(&[BindValue::Number("42".into())]);
        let short_text = shape_of(&[BindValue::Text("a".into())]);
        let long_text = shape_of(&[BindValue::Text("x".repeat(40_000))]);
        let raw = shape_of(&[BindValue::Raw(vec![1, 2, 3])]);
        // The repro from bead rust-oracledb-ilel: NUMBER -> TEXT must NOT
        // reuse the cached cursor (server-side ORA-01722), nor TEXT -> RAW.
        assert!(!bind_shape_is_compatible(&number, &short_text));
        assert!(!bind_shape_is_compatible(&short_text, &number));
        assert!(!bind_shape_is_compatible(&short_text, &raw));
        assert!(!bind_shape_is_compatible(&raw, &number));
        // Same type family: identical, and a pure size change stays
        // compatible (no re-parse on every string-length change).
        assert!(bind_shape_is_compatible(&number, &number));
        assert!(bind_shape_is_compatible(&short_text, &long_text));
        assert!(bind_shape_is_compatible(&long_text, &short_text));
        // CHAR/VARCHAR/LONG fold into one family (the wire writer merges them
        // via bind_metadata_types_are_compatible).
        let long_typed = shape_of(&[BindValue::TypedNull {
            ora_type_num: ORA_TYPE_NUM_LONG,
            csfrm: 1,
            buffer_size: 10,
        }]);
        assert!(bind_shape_is_compatible(&short_text, &long_typed));
        // Bind-count mismatch is never compatible.
        assert!(!bind_shape_is_compatible(
            &number,
            &shape_of(&[BindValue::Number("1".into()), BindValue::Number("2".into())])
        ));
    }

    #[test]
    fn bind_type_shape_untyped_null_matches_any_cached_slot() {
        let number = shape_of(&[BindValue::Number("42".into())]);
        let null = shape_of(&[BindValue::Null]);
        assert!(null[0].untyped_null);
        // A NULL bind rides any cached cursor (written as a placeholder, the
        // value null-converts server-side)...
        assert!(bind_shape_is_compatible(&number, &null));
        // ...but a concrete NUMBER does not ride a cursor parsed with the
        // VARCHAR placeholder: re-parse for correct select-list typing.
        assert!(!bind_shape_is_compatible(&null, &number));
        // Text matches the placeholder family.
        assert!(bind_shape_is_compatible(
            &null,
            &shape_of(&[BindValue::Text("x".into())])
        ));
    }

    #[test]
    fn bind_type_shape_infers_column_type_from_first_non_null_row() {
        // executemany with a leading NULL: the column type comes from the
        // first non-NULL row (same inference as the wire metadata writer).
        let rows = vec![
            vec![BindValue::Null],
            vec![BindValue::Number("7".into())],
            vec![BindValue::Null],
        ];
        let shape = bind_type_shape(&rows);
        assert!(!shape[0].untyped_null);
        assert_eq!(
            shape,
            shape_of(&[BindValue::Number("7".into())]),
            "inferred NUMBER column"
        );
        // All-NULL column stays a wildcard.
        let all_null = bind_type_shape(&[vec![BindValue::Null], vec![BindValue::Null]]);
        assert!(all_null[0].untyped_null);
    }

    #[test]
    fn statement_cache_insert_inherits_concrete_slot_over_untyped_null() {
        // First execute binds NUMBER; a later re-execute of the SAME cursor
        // binds NULL. The stored shape must keep NUMBER (the cursor is still
        // parsed for NUMBER), so a following NUMBER execute reuses it.
        let number = shape_of(&[BindValue::Number("42".into())]);
        let null = shape_of(&[BindValue::Null]);
        let mut cache = Vec::new();
        assert!(statement_cache_insert(&mut cache, 5, "q", 10, number.clone()).is_empty());
        assert!(statement_cache_insert(&mut cache, 5, "q", 10, null).is_empty());
        assert_eq!(cache[0].bind_shape, number, "concrete slot inherited");
        // A REPLACED cursor (fresh parse) records the new shape verbatim.
        let text = shape_of(&[BindValue::Text("x".into())]);
        assert_eq!(
            statement_cache_insert(&mut cache, 5, "q", 11, text.clone()),
            vec![10]
        );
        assert_eq!(cache[0].bind_shape, text);
    }

    #[test]
    fn auth_serial_num_truncates_to_low_u16_instead_of_rejecting() {
        let mut data = BTreeMap::new();
        data.insert("AUTH_SERIAL_NUM".to_string(), "70000".to_string());

        assert_eq!(
            parse_session_u16(&data, "AUTH_SERIAL_NUM")
                .expect("large AUTH_SERIAL_NUM should parse"),
            70000_u64 as u16
        );
    }

    #[test]
    fn db_unique_name_reads_real_key_not_confused_with_dbunique_name() {
        // etib.8: db_unique_name comes from AUTH_SC_REAL_DBUNIQUE_NAME ONLY. It
        // must NOT be confused with AUTH_SC_DBUNIQUE_NAME (which upstream maps to
        // db_name), and there is no fallback to it.
        let mut data = BTreeMap::new();
        data.insert(
            "AUTH_SC_REAL_DBUNIQUE_NAME".to_string(),
            "MYCDB_UNIQUE".to_string(),
        );
        data.insert(
            "AUTH_SC_DBUNIQUE_NAME".to_string(),
            "MYCDB_SHOULD_NOT_WIN".to_string(),
        );
        assert_eq!(parse_db_unique_name(&data).as_deref(), Some("MYCDB_UNIQUE"));

        // Only the non-REAL key present -> None (no fallback).
        let mut only_non_real = BTreeMap::new();
        only_non_real.insert("AUTH_SC_DBUNIQUE_NAME".to_string(), "MYCDB".to_string());
        assert_eq!(parse_db_unique_name(&only_non_real), None);

        // Missing entirely -> None.
        assert_eq!(parse_db_unique_name(&BTreeMap::new()), None);
    }

    // Character column of the first `needle` in `line` (chars, not bytes — the
    // caret aligns by display column, so multibyte chars must be counted as 1).
    fn char_col(line: &str, needle: char) -> usize {
        line.chars()
            .position(|c| c == needle)
            .expect("char present")
    }

    #[test]
    fn caret_points_at_the_flagged_char() {
        // offset 8 (1-based) is the `x`.
        let out = render_caret("select x from t", 8, "ORA-00904: invalid identifier");
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "ORA-00904: invalid identifier");
        assert!(lines[2].ends_with("select x from t"), "{:?}", lines[2]);
        assert_eq!(
            char_col(lines[3], '^'),
            char_col(lines[2], 'x'),
            "caret column must align under the flagged char"
        );
    }

    #[test]
    fn caret_handles_multiline_sql() {
        // "select *\n" is 9 chars (\n at index 8); the `n` of `no_such` is char
        // index 14 -> 1-based offset 15, on line 2.
        let out = render_caret("select *\nfrom no_such", 15, "ORA-00942");
        let lines: Vec<&str> = out.lines().collect();
        assert!(lines[2].starts_with("2 | from no_such"), "{:?}", lines[2]);
        assert_eq!(char_col(lines[3], '^'), char_col(lines[2], 'n'));
    }

    #[test]
    fn caret_counts_unicode_scalar_values() {
        // The multibyte `é` before the offset must not push the caret off — we
        // count chars, not bytes.
        let sql = "select 'café' x from t";
        let x_idx = sql.chars().position(|c| c == 'x').unwrap();
        let out = render_caret(sql, x_idx + 1, "h");
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(char_col(lines[3], '^'), char_col(lines[2], 'x'));
    }

    #[test]
    fn caret_clamps_and_never_panics() {
        // out-of-range, zero, and empty inputs must render without panicking.
        let _ = render_caret("select 1", 999, "h");
        let _ = render_caret("select 1", 0, "h");
        let _ = render_caret("", 5, "h");
        assert!(render_caret("abc", 4, "h").ends_with('^'));
    }

    fn identity() -> ClientIdentity {
        ClientIdentity::new("program", "machine", "osuser", "terminal", "driver")
            .expect("test identity should be valid")
    }

    pub(crate) fn loopback_connection(
        read: transport::OracleReadHalf,
        write: transport::OracleWriteHalf,
    ) -> Connection {
        loopback_connection_from_core(ConnectionCore::<DriverTransport>::from_halves(
            read,
            write,
            "loopback_test_write",
        ))
    }

    fn loopback_connection_from_core(core: DriverCore) -> Connection {
        Connection {
            descriptor: EasyConnect::parse("127.0.0.1:1521/FREEPDB1")
                .expect("test connect string should parse"),
            identity: identity(),
            core,
            session_id: 0,
            serial_num: 0,
            server_version: None,
            server_version_tuple: None,
            db_unique_name: None,
            capabilities: ClientCapabilities::default(),
            protocol_limits: ProtocolLimits::DEFAULT,
            ttc_seq_num: 0,
            sdu: 8192,
            protocol_version: 0,
            supports_fast_auth: false,
            supports_end_of_response: true,
            supports_oob: false,
            cursor_columns: BTreeMap::new(),
            fetch_metadata_by_sql: HashMap::new(),
            fetch_metadata_order: VecDeque::new(),
            shape_cache: Arc::new(StatementShapeCache::new()),
            dead: false,
            user: "test_user".into(),
            combo_key: Vec::new(),
            statement_cache: Vec::new(),
            statement_cache_size: STATEMENT_CACHE_SIZE,
            in_use_cursors: HashSet::new(),
            lob_prefetch_cursors: BTreeSet::new(),
            copied_cursors: HashSet::new(),
            cursors_to_close: Vec::new(),
            sessionless_data: None,
            notification_buffer: Vec::new(),
            notification_header_consumed: false,
            transaction_context: None,
            txn_in_progress: false,
            #[cfg(feature = "cassette")]
            capture_guard: None,
        }
    }

    #[cfg(feature = "cassette")]
    fn synthetic_number_columns() -> Vec<ColumnMetadata> {
        vec![
            ColumnMetadata::new("INTCOL", oracledb_protocol::thin::ORA_TYPE_NUM_NUMBER)
                .with_csfrm(oracledb_protocol::thin::CS_FORM_IMPLICIT)
                .with_buffer_size(22)
                .with_max_size(22)
                .with_nulls_allowed(true),
            ColumnMetadata::new("NUMBERCOL", oracledb_protocol::thin::ORA_TYPE_NUM_NUMBER)
                .with_csfrm(oracledb_protocol::thin::CS_FORM_IMPLICIT)
                .with_buffer_size(22)
                .with_max_size(22)
                .with_nulls_allowed(true),
        ]
    }

    #[cfg(feature = "cassette")]
    fn synthetic_connect_packet() -> Result<Vec<u8>> {
        let payload = build_connect_packet_payload(
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=fixture-host)(PORT=0))\
             (CONNECT_DATA=(SERVICE_NAME=SYNTHETIC)(CID=(PROGRAM=rust-oracledb)\
             (HOST=fixture-host)(USER=fixture-user))))",
            8192,
        )?;
        Ok(encode_packet(
            TNS_PACKET_TYPE_CONNECT,
            0,
            None,
            &payload,
            PacketLengthWidth::Legacy16,
        )?)
    }

    #[cfg(feature = "cassette")]
    fn synthetic_accept_packet() -> Result<Vec<u8>> {
        Ok(encode_packet(
            TNS_PACKET_TYPE_ACCEPT,
            0,
            None,
            b"SYNTHETIC-ACCEPT",
            PacketLengthWidth::Legacy16,
        )?)
    }

    #[cfg(feature = "cassette")]
    fn synthetic_execute_packet() -> Result<Vec<u8>> {
        let payload = build_execute_payload_with_bind_rows_and_options_with_seq(
            "select value from synthetic_fixture",
            2,
            1,
            true,
            &[],
            ExecuteOptions::default(),
            ClientCapabilities::default().ttc_field_version,
        )?;
        Ok(encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(0),
            &payload,
            PacketLengthWidth::Large32,
        )?)
    }

    #[cfg(feature = "cassette")]
    fn synthetic_fetch_packet() -> Result<Vec<u8>> {
        let payload =
            build_fetch_payload_with_seq(42, 2, 2, ClientCapabilities::default().ttc_field_version);
        Ok(encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(0),
            &payload,
            PacketLengthWidth::Large32,
        )?)
    }

    #[cfg(feature = "cassette")]
    fn synthetic_function_packet(function_code: u8, seq_num: u8) -> Result<Vec<u8>> {
        let payload = build_function_payload_with_seq(
            function_code,
            seq_num,
            ClientCapabilities::default().ttc_field_version,
        );
        Ok(encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(0),
            &payload,
            PacketLengthWidth::Large32,
        )?)
    }

    #[cfg(feature = "cassette")]
    fn hex_value(byte: u8) -> Result<u8> {
        match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            b'A'..=b'F' => Ok(byte - b'A' + 10),
            _ => Err(Error::Runtime(format!(
                "invalid synthetic fixture hex byte {byte:#04x}"
            ))),
        }
    }

    #[cfg(feature = "cassette")]
    fn decode_hex_fixture(hex: &str) -> Result<Vec<u8>> {
        let clean = hex
            .bytes()
            .filter(|byte| !byte.is_ascii_whitespace())
            .collect::<Vec<_>>();
        if clean.len() % 2 != 0 {
            return Err(Error::Runtime(
                "synthetic fixture hex must contain an even number of digits".into(),
            ));
        }
        let mut out = Vec::with_capacity(clean.len() / 2);
        for pair in clean.chunks_exact(2) {
            out.push((hex_value(pair[0])? << 4) | hex_value(pair[1])?);
        }
        Ok(out)
    }

    #[cfg(feature = "cassette")]
    fn synthetic_execute_response_payload() -> Result<Vec<u8>> {
        decode_hex_fixture(concat!(
            "101710740fb986350b6010fbcb6e06a74ed0787e060a110328014001018201800000",
            "014000000000020369010140023ffe010501050556414c554500000000000000000000",
            "010707787e060a110b1000021fe8010a010a00062201010001020000000708414c33",
            "32555446380801060323a4d500010100000000000004010102013b010102057b0000",
            "01010003000000000000000000000000030001010000000002057b0101010300194f",
            "52412d30313430333a206e6f206461746120666f756e640a1d",
        ))
    }

    #[cfg(feature = "cassette")]
    fn synthetic_fetch_response_payload() -> Result<Vec<u8>> {
        decode_hex_fixture("06020101000205dc0001010101000702c1041d")
    }

    #[cfg(feature = "cassette")]
    fn synthetic_plain_function_response_payload() -> [u8; 1] {
        [TNS_MSG_TYPE_END_OF_RESPONSE]
    }

    #[cfg(feature = "cassette")]
    #[test]
    fn synthetic_cassette_replays_connect_execute_fetch_close_offline() -> Result<()> {
        let cassette = include_bytes!("../tests/fixtures/cassettes/select_7_plus_5.tns-cassette");
        let (read, write, audit) =
            transport::replay_split_with_audit(cassette, transport::ReplayWriteMode::Check)
                .map_err(|err| Error::Runtime(format!("invalid replay cassette: {err}")))?;
        let mut core = ConnectionCore::<DriverTransport>::from_halves(
            read,
            write,
            "synthetic_fixture_replay_write",
        );
        let runtime = build_io_runtime()?;

        runtime.block_on(async {
            let cx = test_cx()?;
            let connect_packet = synthetic_connect_packet()?;
            core.write_all(&cx, &connect_packet).await?;
            let accept = core.read_packet(PacketLengthWidth::Legacy16).await?;
            assert_eq!(accept.packet_type, TNS_PACKET_TYPE_ACCEPT);
            assert_eq!(accept.payload, b"SYNTHETIC-ACCEPT");

            let mut conn = loopback_connection_from_core(core);
            let execute = conn
                .execute_raw(
                    &cx,
                    "select value from synthetic_fixture",
                    2,
                    &[],
                    ExecuteOptions::default(),
                    None,
                )
                .await?;
            assert_eq!(execute.columns.len(), 1);
            assert_eq!(execute.rows.len(), 1);
            assert_eq!(
                execute.cell(0, 0).and_then(QueryValue::as_text),
                Some("AL32UTF8")
            );

            let previous_row = vec![
                Some(QueryValue::number_from_text("2", true)),
                Some(QueryValue::number_from_text("0.5", false)),
            ];
            let columns = synthetic_number_columns();
            let fetched = conn
                .fetch_rows_with_columns(&cx, 42, 2, &columns, Some(&previous_row))
                .await?;
            assert_eq!(fetched.rows.len(), 1);
            assert_eq!(
                fetched
                    .cell(0, 0)
                    .and_then(QueryValue::as_number_text)
                    .as_deref(),
                Some("3")
            );
            assert_eq!(
                fetched
                    .cell(0, 1)
                    .and_then(QueryValue::as_number_text)
                    .as_deref(),
                Some("0.5")
            );

            conn.close(&cx).await?;
            Ok::<_, Error>(())
        })?;

        audit
            .assert_finished()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let _ = (
            synthetic_accept_packet()?,
            synthetic_execute_packet()?,
            synthetic_fetch_packet()?,
            synthetic_function_packet(TNS_FUNC_ROLLBACK, 3)?,
            synthetic_function_packet(TNS_FUNC_LOGOFF, 4)?,
            synthetic_execute_response_payload()?,
            synthetic_fetch_response_payload()?,
            synthetic_plain_function_response_payload(),
        );
        Ok(())
    }

    fn server_error(message: &str) -> Error {
        Error::Protocol(oracledb_protocol::ProtocolError::ServerError(
            message.to_string(),
        ))
    }

    fn structured_error(code: u32, pos: i32) -> Error {
        Error::Protocol(oracledb_protocol::ProtocolError::ServerErrorInfo(Box::new(
            oracledb_protocol::ServerErrorDetails {
                message: format!("ORA-{code:05}: synthetic"),
                code,
                pos,
                ..Default::default()
            },
        )))
    }

    fn column(name: &str, ora_type_num: u8, is_json: bool) -> ColumnMetadata {
        ColumnMetadata::new(name, ora_type_num).with_is_json(is_json)
    }

    #[test]
    fn json_lob_probe_candidates_selects_named_non_json_lobs() {
        use oracledb_protocol::thin::{ORA_TYPE_NUM_BLOB, ORA_TYPE_NUM_CLOB, ORA_TYPE_NUM_VARCHAR};
        let columns = vec![
            column("doc", ORA_TYPE_NUM_CLOB, false),         // candidate
            column("blob_doc", ORA_TYPE_NUM_BLOB, false),    // candidate
            column("already_json", ORA_TYPE_NUM_CLOB, true), // skipped: is_json
            column("name", ORA_TYPE_NUM_VARCHAR, false),     // skipped: not a LOB
            column("", ORA_TYPE_NUM_CLOB, false),            // skipped: unnamed expression
        ];
        // Names come back upper-cased (the catalog stores them upper-cased) and
        // only the two named, non-JSON LOB columns are probed, by original index.
        assert_eq!(
            json_lob_probe_candidates(&columns),
            vec![(0, "DOC".to_string()), (1, "BLOB_DOC".to_string())]
        );
    }

    #[test]
    fn json_lob_probe_candidates_empty_when_no_lobs() {
        use oracledb_protocol::thin::ORA_TYPE_NUM_VARCHAR;
        let columns = vec![column("name", ORA_TYPE_NUM_VARCHAR, false)];
        assert!(json_lob_probe_candidates(&columns).is_empty());
    }

    #[test]
    fn ora_code_parses_from_message_and_struct() {
        // string path: parsed from the ORA- prefix
        assert_eq!(
            server_error("ORA-00060: deadlock detected").ora_code(),
            Some(60)
        );
        // structured path: read straight from .code
        assert_eq!(structured_error(942, 0).ora_code(), Some(942));
        // no ORA- code present
        assert_eq!(server_error("listener problem").ora_code(), None);
        // non-server errors have no code
        assert_eq!(Error::CallTimeout(500).ora_code(), None);
    }

    #[test]
    fn stable_error_methods_classify_without_display_parsing() {
        let transient = server_error("ORA-00060: deadlock detected");
        assert_eq!(transient.kind(), ErrorKind::Database);
        assert_eq!(transient.oracle_code(), Some(60));
        assert_eq!(
            transient.connection_disposition(),
            ConnectionDisposition::Reusable
        );
        assert_eq!(
            transient.retry_hint(),
            RetryHint::RetrySameConnectionIfIdempotent
        );

        let lost = server_error("ORA-03113: end-of-file on communication channel");
        assert_eq!(lost.kind(), ErrorKind::Database);
        assert_eq!(lost.connection_disposition(), ConnectionDisposition::Dead);
        assert_eq!(lost.retry_hint(), RetryHint::ReconnectThenRetryIfIdempotent);

        let timeout = Error::CallTimeout(500);
        assert_eq!(timeout.kind(), ErrorKind::Timeout);
        assert_eq!(
            timeout.connection_disposition(),
            ConnectionDisposition::Reusable
        );
        assert_eq!(
            timeout.retry_hint(),
            RetryHint::RetrySameConnectionIfIdempotent
        );

        let cancelled = Error::Cancelled;
        assert_eq!(cancelled.kind(), ErrorKind::Cancel);
        assert_eq!(cancelled.oracle_code(), Some(1013));
        assert_eq!(
            cancelled.retry_hint(),
            RetryHint::RetrySameConnectionIfIdempotent
        );

        let bind = Error::Bind(BindError::PositionalCountMismatch {
            expected: 2,
            actual: 1,
        });
        assert_eq!(bind.kind(), ErrorKind::Conversion);
        assert_eq!(bind.retry_hint(), RetryHint::Never);

        let resource = Error::Protocol(oracledb_protocol::ProtocolError::ResourceLimit {
            limit: "binds",
            observed: 4,
            maximum: 3,
        });
        assert_eq!(resource.kind(), ErrorKind::ResourceLimit);
        assert_eq!(
            resource.connection_disposition(),
            ConnectionDisposition::Reusable
        );
        assert_eq!(resource.retry_hint(), RetryHint::Never);

        let session_dead = server_error("ORA-00600: internal error code, arguments: []");
        assert_eq!(
            session_dead.connection_disposition(),
            ConnectionDisposition::Dead
        );
        assert!(
            !session_dead.is_connection_lost(),
            "ORA-00600 kills the session but is not a connection-lost retry code"
        );
        assert_eq!(session_dead.retry_hint(), RetryHint::Never);
    }

    // ---- W1-T6.2: internal Outcome/CancelKind discipline ----------------------
    //
    // Cancellation is not "just another error". Each asupersync `CancelKind`
    // drives a specific connection disposition BEFORE we flatten to the public
    // `Error` at the boundary, and the mapping is METHOD-based (it reads
    // `CancelReason::kind`), never a display-string parse.

    #[test]
    fn cancel_kind_maps_to_disposition() {
        // The timeout family (deadline / quota exhaustion) drains and stays
        // reusable — it composes like a `call_timeout`.
        for kind in [
            CancelKind::Timeout,
            CancelKind::Deadline,
            CancelKind::PollQuota,
            CancelKind::CostBudget,
        ] {
            assert_eq!(
                CancelDisposition::from_kind(kind),
                CancelDisposition::Timeout,
                "{kind:?} must drain + stay reusable (timeout disposition)"
            );
        }

        // Runtime shutdown / resource loss / linked-exit closes the connection.
        for kind in [
            CancelKind::Shutdown,
            CancelKind::ResourceUnavailable,
            CancelKind::LinkedExit,
        ] {
            assert_eq!(
                CancelDisposition::from_kind(kind),
                CancelDisposition::Close,
                "{kind:?} must close the connection"
            );
        }

        // Explicit / topological cancels drain quietly and stay reusable.
        for kind in [
            CancelKind::User,
            CancelKind::RaceLost,
            CancelKind::FailFast,
            CancelKind::ParentCancelled,
        ] {
            assert_eq!(
                CancelDisposition::from_kind(kind),
                CancelDisposition::Cancel,
                "{kind:?} must drain quietly + stay reusable (cancel disposition)"
            );
        }
    }

    #[test]
    fn cancel_disposition_flattens_to_distinct_error_variants() {
        // The boundary flatten: each disposition produces a DISTINCT public
        // error variant — a cancel is NEVER a generic `Runtime`/`Io` error — and
        // the resulting error classifies correctly via the W1-T6.1 methods.

        // Timeout -> CallTimeout: reusable + retryable on the same connection.
        let timeout = CancelDisposition::Timeout.into_error(750);
        assert!(matches!(timeout, Error::CallTimeout(750)));
        assert_eq!(timeout.kind(), ErrorKind::Timeout);
        assert_eq!(
            timeout.connection_disposition(),
            ConnectionDisposition::Reusable
        );
        assert_eq!(
            timeout.retry_hint(),
            RetryHint::RetrySameConnectionIfIdempotent,
            "a drained timeout may be retried on the same connection"
        );

        // Cancel -> Cancelled (ORA-01013): distinct cancel variant, reusable +
        // retryable — explicitly NOT Error::Runtime / Error::Io.
        let cancelled = CancelDisposition::Cancel.into_error(750);
        assert!(matches!(cancelled, Error::Cancelled));
        assert!(
            !matches!(cancelled, Error::Runtime(_) | Error::Io(_)),
            "a cancel must be a distinct variant, never a generic runtime/io error"
        );
        assert_eq!(cancelled.kind(), ErrorKind::Cancel);
        assert_eq!(cancelled.oracle_code(), Some(1013));
        assert_eq!(
            cancelled.connection_disposition(),
            ConnectionDisposition::Reusable
        );
        assert_eq!(
            cancelled.retry_hint(),
            RetryHint::RetrySameConnectionIfIdempotent
        );

        // Close -> ConnectionClosed: connection is dead; reconnect before retry.
        let closed = CancelDisposition::Close.into_error(750);
        assert!(matches!(closed, Error::ConnectionClosed(_)));
        assert_eq!(closed.kind(), ErrorKind::Network);
        assert_eq!(closed.connection_disposition(), ConnectionDisposition::Dead);
        assert_eq!(
            closed.retry_hint(),
            RetryHint::ReconnectThenRetryIfIdempotent
        );
    }

    #[test]
    fn missing_cancel_reason_is_a_plain_cancel_at_checkpoint() {
        // A cancel with no recorded `CancelReason` is still a cancel — never a
        // runtime error. (The in-operation timeout path defaults the OTHER way,
        // to Timeout, because it is entered by a deadline elapse; see
        // `recover_from_call_timeout`.)
        assert_eq!(cancel_disposition(None), CancelDisposition::Cancel);
    }

    #[test]
    fn cancelled_cx_resolves_to_the_kind_mapped_distinct_error() {
        // End-to-end against a real asupersync context: inject a cancel of a
        // given kind, confirm `checkpoint()` actually fails and `cancel_reason()`
        // carries the kind, then assert the SAME mapping the between-round-trip
        // helper applies surfaces the kind-mapped DISTINCT error (never the old
        // Error::Runtime(display_string)). A `detached_cancel_context` carries
        // cancellation + budget state with no effect caps — exactly what a unit
        // test of the cancel mapping needs, with no live runtime required.
        fn err_for(kind: CancelKind) -> Error {
            let cx = Cx::detached_cancel_context();
            cx.cancel_with(kind, Some("test cancel"));
            // The cancel must actually be observable through the methods the
            // driver relies on — not a display string.
            assert!(
                cx.checkpoint().is_err(),
                "{kind:?}: a cancelled context must fail its checkpoint"
            );
            let reason = cx.cancel_reason().unwrap_or_else(|| {
                panic!("{kind:?}: cancel_reason must carry the structured kind")
            });
            assert_eq!(reason.kind, kind, "cancel_reason must round-trip the kind");
            cancel_disposition(Some(reason)).into_error(750)
        }

        // Timeout family -> Error::CallTimeout (Timeout kind, reusable).
        for kind in [CancelKind::Timeout, CancelKind::Deadline] {
            let err = err_for(kind);
            assert!(
                matches!(err, Error::CallTimeout(_)),
                "{kind:?} should surface CallTimeout, got {err:?}"
            );
            assert_eq!(err.kind(), ErrorKind::Timeout);
        }

        // Shutdown -> Error::ConnectionClosed (dead).
        let shutdown = err_for(CancelKind::Shutdown);
        assert!(
            matches!(shutdown, Error::ConnectionClosed(_)),
            "Shutdown should surface ConnectionClosed, got {shutdown:?}"
        );
        assert_eq!(
            shutdown.connection_disposition(),
            ConnectionDisposition::Dead
        );

        // User / RaceLost -> Error::Cancelled (distinct, reusable) — and crucially
        // NOT the old Error::Runtime(display_string).
        for kind in [CancelKind::User, CancelKind::RaceLost] {
            let err = err_for(kind);
            assert!(
                matches!(err, Error::Cancelled),
                "{kind:?} should surface Cancelled, got {err:?}"
            );
            assert!(
                !matches!(err, Error::Runtime(_)),
                "{kind:?} must NOT be flattened to a generic Error::Runtime"
            );
            assert_eq!(err.kind(), ErrorKind::Cancel);
        }
    }

    #[test]
    fn uncancelled_checkpoint_is_ok() {
        // The fast path: a healthy context passes the checkpoint untouched, so
        // the cancel-mapping branch is never taken.
        let cx = Cx::detached_cancel_context();
        assert!(cx.checkpoint().is_ok());
        assert!(cx.cancel_reason().is_none());
    }

    #[test]
    fn offset_only_from_structured_nonzero() {
        assert_eq!(structured_error(942, 14).offset(), Some(14));
        assert_eq!(structured_error(942, 0).offset(), None);
        assert_eq!(
            server_error("ORA-00942: table or view does not exist").offset(),
            None
        );
    }

    #[test]
    fn transient_classification() {
        for &code in TRANSIENT_ORA_CODES {
            let err = server_error(&format!("ORA-{code:05}: transient"));
            assert!(err.is_transient(), "ORA-{code:05} should be transient");
            assert!(err.is_retryable(), "transient implies retryable");
            assert!(
                !err.is_connection_lost(),
                "ORA-{code:05} is not connection-lost"
            );
        }
        // a permanent error: table or view does not exist
        let perm = server_error("ORA-00942: table or view does not exist");
        assert!(!perm.is_transient());
        assert!(!perm.is_connection_lost());
        assert!(!perm.is_retryable());
    }

    #[test]
    fn connection_lost_classification() {
        for &code in CONNECTION_LOST_ORA_CODES {
            let err = server_error(&format!("ORA-{code:05}: lost"));
            assert!(
                err.is_connection_lost(),
                "ORA-{code:05} should be connection-lost"
            );
            assert!(err.is_retryable(), "connection-lost implies retryable");
            assert!(
                !err.is_transient(),
                "ORA-{code:05} is not a transient (retry-in-place) code"
            );
        }
        // raw I/O counts as the transport being gone
        let io = Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ));
        assert!(io.is_connection_lost());
        assert!(io.is_retryable());

        // A plain call timeout is NOT connection-lost: on a timeout the driver
        // breaks + drains the wire and the connection stays reusable, mirroring
        // python-oracledb's DPY-4024 (ERR_CALL_TIMEOUT_EXCEEDED) which — unlike
        // DPY-4011 — does not set is_session_dead (errors.py:124-125). It is
        // transient (retry in place) and therefore retryable.
        let timeout = Error::CallTimeout(1000);
        assert!(
            !timeout.is_connection_lost(),
            "a call timeout leaves the connection usable after the drain"
        );
        assert!(
            timeout.is_transient(),
            "a call timeout is a retry-in-place (transient) condition"
        );
        assert!(
            timeout.is_retryable(),
            "transient implies retryable on the same connection"
        );

        // ConnectionClosed (raised only when the post-timeout drain itself fails
        // — a SECOND timeout, the reference's disconnect path) IS connection-lost:
        // the wire could not be left clean, so the connection must be discarded.
        let recovery_failed =
            Error::ConnectionClosed("socket timed out while recovering".to_string());
        assert!(
            recovery_failed.is_connection_lost(),
            "a failed timeout-recovery drain marks the connection lost"
        );
        assert!(recovery_failed.is_retryable(), "reconnect, then retry");
        assert!(
            !recovery_failed.is_transient(),
            "ConnectionClosed needs a reconnect first, so it is not retry-in-place"
        );
    }

    #[test]
    fn resource_limit_error_defines_pre_sync_and_post_sync_disposition() {
        let err = oracledb_protocol::ProtocolError::ResourceLimit {
            limit: "columns",
            observed: 3,
            maximum: 2,
        };
        assert_eq!(
            post_sync_protocol_error_disposition(&err),
            PostSyncProtocolDisposition::Dead,
            "a post-sync resource-limit decode failure leaves unread response bytes"
        );

        let pre_sync = Error::Protocol(oracledb_protocol::ProtocolError::ResourceLimit {
            limit: "columns",
            observed: 3,
            maximum: 2,
        });
        assert_eq!(
            pre_sync.resource_limit(),
            Some(oracledb_protocol::ResourceLimit {
                limit: "columns",
                observed: 3,
                maximum: 2,
            })
        );
        assert!(
            !pre_sync.is_connection_lost(),
            "client-side/pre-sync resource-limit validation does not imply a lost connection"
        );
        assert!(
            !pre_sync.is_transient(),
            "raising the configured limit, not retrying in-place, is the remedy"
        );
        assert!(
            !pre_sync.is_retryable(),
            "resource limits are deterministic for the same input and policy"
        );
        assert!(matches!(
            pre_sync,
            Error::Protocol(oracledb_protocol::ProtocolError::ResourceLimit {
                limit: "columns",
                observed: 3,
                maximum: 2,
            })
        ));
    }

    #[test]
    fn post_sync_resource_limit_marks_live_connection_dead() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (_socket, _) = listener.accept().expect("accept test client");
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let mut conn = runtime.block_on(async {
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (read, write) = transport::plain_split(stream);
            loopback_connection(read, write)
        });

        let err = conn
            .note_parse::<()>(Err(oracledb_protocol::ProtocolError::ResourceLimit {
                limit: "response_bytes",
                observed: 33,
                maximum: 32,
            }))
            .expect_err("post-sync resource-limit violation must be surfaced");

        assert!(matches!(
            err,
            Error::Protocol(oracledb_protocol::ProtocolError::ResourceLimit {
                limit: "response_bytes",
                observed: 33,
                maximum: 32,
            })
        ));
        assert!(
            conn.is_dead(),
            "post-sync resource-limit violation stops consuming a response and kills the session"
        );

        drop(conn);
        server.join().expect("server thread joins");
        Ok(())
    }

    #[test]
    fn protocol_version_and_fast_auth_accessors_report_negotiated_values() {
        // K2 ServerFeatures accessors: the two getters read the fields the
        // ACCEPT negotiation populates. Build a loopback connection, set the
        // two accept-derived fields to distinct non-default values, and assert
        // each getter returns its own field (distinct types u16/bool guarantee
        // they cannot be transposed).
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (_socket, _) = listener.accept().expect("accept test client");
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let mut conn = runtime.block_on(async {
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (read, write) = transport::plain_split(stream);
            loopback_connection(read, write)
        });

        // Defaults from the loopback constructor.
        assert_eq!(conn.protocol_version(), 0);
        assert!(!conn.supports_fast_auth());

        // Simulate a negotiated ACCEPT: TNS version 319 over the fast-auth path.
        conn.protocol_version = 319;
        conn.supports_fast_auth = true;
        assert_eq!(conn.protocol_version(), 319);
        assert!(conn.supports_fast_auth());

        drop(conn);
        server.join().expect("server thread joins");
    }

    #[test]
    fn host_port_protocol_accessors_report_connected_descriptor() {
        // etib.7: host()/port()/protocol() expose the connected endpoint from the
        // resolved descriptor. Build a loopback connection, plant a known
        // descriptor, and assert each getter returns its own field (the distinct
        // types &str/u16/Protocol guarantee they cannot be transposed).
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (_socket, _) = listener.accept().expect("accept test client");
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let mut conn = runtime.block_on(async {
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (read, write) = transport::plain_split(stream);
            loopback_connection(read, write)
        });

        conn.descriptor = EasyConnect {
            host: "db.example.com".to_string(),
            port: 2484,
            service_name: "FREEPDB1".to_string(),
            protocol: NetProtocol::Tcps,
        };

        assert_eq!(conn.host(), "db.example.com");
        assert_eq!(conn.port(), 2484);
        assert_eq!(conn.protocol(), NetProtocol::Tcps);
        // The getters agree with the descriptor they delegate to.
        assert_eq!(conn.host(), conn.descriptor().host);
        assert_eq!(conn.port(), conn.descriptor().port);
        assert_eq!(conn.protocol(), conn.descriptor().protocol);

        drop(conn);
        server.join().expect("server thread joins");
    }

    // ---- A8 (bead oraclemcp-release-073-iec3.1.11): native single-round-trip
    // pipelining, proven offline over a loopback socket -----------------------
    //
    // `run_pipeline`/`run_pipeline_decoded` write every operation *before*
    // reading any response (one write burst, then N+1 boundary reads), so an
    // N-statement batch is a single wire round trip. python-oracledb's thin
    // mode issues the same batch as N sequential execute round trips. The
    // loopback "server" below refuses to answer until it has read the whole
    // batch, which terminates only if the client truly pipelined — a
    // deterministic, offline proof of the collapse (no live server, cassette
    // decode reused for the per-op result-materialization layer).

    /// The synthetic execute response payload reused from the committed
    /// `select_7_plus_5` cassette fixture — no live capture, pure TNS framing +
    /// TTC decoder exercise. Decodes with the *same* per-op decoder the
    /// sequential execute path uses.
    fn synthetic_pipeline_execute_response_payload() -> Vec<u8> {
        const HEX: &str = concat!(
            "101710740fb986350b6010fbcb6e06a74ed0787e060a110328014001018201800000",
            "014000000000020369010140023ffe010501050556414c554500000000000000000000",
            "010707787e060a110b1000021fe8010a010a00062201010001020000000708414c33",
            "32555446380801060323a4d500010100000000000004010102013b010102057b0000",
            "01010003000000000000000000000000030001010000000002057b0101010300194f",
            "52412d30313430333a206e6f206461746120666f756e640a1d",
        );
        let raw = HEX.as_bytes();
        let mut bytes = Vec::with_capacity(raw.len() / 2);
        let mut index = 0;
        while index < raw.len() {
            let hi = (raw[index] as char).to_digit(16).expect("hex digit");
            let lo = (raw[index + 1] as char).to_digit(16).expect("hex digit");
            bytes.push(((hi << 4) | lo) as u8);
            index += 2;
        }
        bytes
    }

    fn synthetic_lob_read_response_payload(locator: &[u8], data: &[u8], amount: u64) -> Vec<u8> {
        let mut payload = oracledb_protocol::wire::TtcWriter::new();
        payload.write_u8(oracledb_protocol::thin::TNS_MSG_TYPE_LOB_DATA);
        payload
            .write_bytes_with_length(data)
            .expect("synthetic LOB payload is encodable");
        payload.write_u8(oracledb_protocol::thin::TNS_MSG_TYPE_PARAMETER);
        payload.write_raw(locator);
        payload.write_ub8(amount);
        payload.write_u8(TNS_MSG_TYPE_END_OF_RESPONSE);
        payload.into_bytes()
    }

    fn synthetic_aq_enqueue_response_payload(msgid: &[u8; 16]) -> Vec<u8> {
        let mut payload = oracledb_protocol::wire::TtcWriter::new();
        payload.write_u8(oracledb_protocol::thin::TNS_MSG_TYPE_PARAMETER);
        payload.write_raw(msgid);
        payload.write_ub2(0);
        payload.write_u8(TNS_MSG_TYPE_END_OF_RESPONSE);
        payload.into_bytes()
    }

    fn synthetic_direct_path_prepare_response_payload(cursor_id: u16) -> Vec<u8> {
        let mut payload = oracledb_protocol::wire::TtcWriter::new();
        payload.write_u8(oracledb_protocol::thin::TNS_MSG_TYPE_PARAMETER);
        payload.write_ub4(0); // column metadata count
        payload.write_ub2(0); // parameter count
        payload.write_ub2(4); // output value count; cursor id is index 3
        payload.write_ub4(0);
        payload.write_ub4(0);
        payload.write_ub4(0);
        payload.write_ub4(u32::from(cursor_id));
        payload.write_u8(TNS_MSG_TYPE_END_OF_RESPONSE);
        let payload = payload.into_bytes();

        let decoded = oracledb_protocol::dpl::parse_direct_path_prepare_response(
            &payload,
            ClientCapabilities::default(),
        )
        .expect("synthetic direct path prepare response decodes");
        assert_eq!(decoded.cursor_id, cursor_id);
        assert!(decoded.column_metadata.is_empty());
        payload
    }

    fn synthetic_direct_path_simple_response_payload() -> Vec<u8> {
        let mut payload = oracledb_protocol::wire::TtcWriter::new();
        payload.write_u8(oracledb_protocol::thin::TNS_MSG_TYPE_PARAMETER);
        payload.write_ub2(0); // output value count
        payload.write_u8(TNS_MSG_TYPE_END_OF_RESPONSE);
        let payload = payload.into_bytes();
        oracledb_protocol::dpl::parse_direct_path_simple_response(
            &payload,
            ClientCapabilities::default(),
        )
        .expect("synthetic direct path simple response decodes");
        payload
    }

    fn synthetic_subscribe_register_response_payload() -> Vec<u8> {
        // Real thin CQN register response captured by the protocol golden at
        // `thin::subscr::tests::subscribe_response_decodes_registration_and_client_id`.
        let payload = vec![
            0x08, 0x01, 0x01, 0x00, 0x02, 0x01, 0x2E, 0x01, 0x01, 0x02, 0x01, 0x2E, 0x00, 0x00,
            0x01, 0x01, 0x01, 0x36, 0x36, 0x28, 0x41, 0x44, 0x44, 0x52, 0x45, 0x53, 0x53, 0x3D,
            0x28, 0x50, 0x52, 0x4F, 0x54, 0x4F, 0x43, 0x4F, 0x4C, 0x3D, 0x54, 0x43, 0x50, 0x29,
            0x28, 0x48, 0x4F, 0x53, 0x54, 0x3D, 0x32, 0x39, 0x30, 0x61, 0x63, 0x30, 0x33, 0x30,
            0x30, 0x33, 0x38, 0x37, 0x29, 0x28, 0x50, 0x4F, 0x52, 0x54, 0x3D, 0x31, 0x35, 0x32,
            0x31, 0x29, 0x29, 0x01, 0x0A, 0x0A, 0x4F, 0x43, 0x49, 0x3A, 0x45, 0x50, 0x3A, 0x33,
            0x30, 0x31, 0x09, 0x01, 0x01, 0x02, 0xDD, 0x48, 0x1D,
        ];
        let decoded = parse_subscribe_response_with_limits(
            &payload,
            ClientCapabilities::default(),
            ProtocolLimits::DEFAULT,
        )
        .expect("captured CQN register response decodes");
        assert_eq!(decoded.registration_id, 302);
        assert_eq!(decoded.client_id.as_deref(), Some(&b"OCI:EP:301"[..]));
        payload
    }

    fn serve_dropped_response_recovery(
        listener: TcpListener,
        expected_request: Vec<u8>,
        stranded_response: Vec<u8>,
        request_seen: std::sync::mpsc::Sender<()>,
    ) -> std::io::Result<bool> {
        const TRAILING_CANCEL_ERROR: &[u8] = &[0x04, 0x01, 0x0d];

        use std::io::Write as _;
        let (mut socket, _) = listener.accept()?;
        socket.set_read_timeout(Some(Duration::from_secs(5)))?;

        assert_eq!(
            read_one_wire_data_payload(&mut socket),
            expected_request,
            "request must preserve its exact payload"
        );
        request_seen
            .send(())
            .expect("client waits for request proof");

        let (next_packet_type, next_body) = read_one_wire_packet_bytes(&mut socket);
        if next_packet_type == TNS_PACKET_TYPE_MARKER {
            assert_eq!(
                next_body,
                vec![1, 0, TNS_MARKER_TYPE_BREAK],
                "reuse must BREAK the stranded response before its request"
            );
            socket.write_all(&data_packet(&stranded_response, true))?;
            socket.write_all(&marker_packet(TNS_MARKER_TYPE_BREAK))?;
            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_RESET,
                "response drain must complete the RESET handshake"
            );
            socket.write_all(&marker_packet(TNS_MARKER_TYPE_RESET))?;
            socket.write_all(&data_packet(TRAILING_CANCEL_ERROR, true))?;

            assert_eq!(
                read_one_wire_packet(&mut socket),
                TNS_PACKET_TYPE_DATA,
                "fresh execute follows the completed response drain"
            );
            socket.write_all(&data_packet(
                &synthetic_pipeline_execute_response_payload(),
                true,
            ))?;
            socket.flush()?;
            Ok(true)
        } else {
            assert_eq!(
                next_packet_type, TNS_PACKET_TYPE_DATA,
                "without recovery the next operation is sent directly"
            );
            socket.write_all(&data_packet(&stranded_response, true))?;
            socket.write_all(&data_packet(
                &synthetic_pipeline_execute_response_payload(),
                true,
            ))?;
            socket.flush()?;
            Ok(false)
        }
    }

    fn synthetic_aq_enqueue_request() -> (AqQueueDesc, AqMsgProps, AqEnqOptions) {
        let queue = AqQueueDesc::new(
            "AQ_QUEUE".to_owned(),
            oracledb_protocol::thin::aq::AqPayloadKind::Raw,
            None,
        );
        let props = AqMsgProps {
            payload: Some(oracledb_protocol::thin::aq::AqPayloadValue::Raw(
                b"payload".to_vec(),
            )),
            ..AqMsgProps::default()
        };
        (queue, props, AqEnqOptions::default())
    }

    /// The committed single-row execute fixture ends with ORA-01403, which
    /// marks the cursor exhausted. Rewrite only that terminal error number and
    /// omit its message to model the same decoded row/metadata with an open
    /// cursor whose continuation must be fetched.
    fn synthetic_open_cursor_execute_response_payload() -> Vec<u8> {
        const TERMINAL_NO_DATA_PREFIX: &[u8] = &[
            0x02, 0x05, 0x7b, // error number: ub4(1403)
            0x01, 0x01, // row count: ub8(1)
            0x01, 0x03, // SQL type: ub4(3)
            0x00, // server checksum: ub4(0)
            0x19, // ORA-01403 message length
        ];

        let mut response = synthetic_pipeline_execute_response_payload();
        let terminal = response
            .windows(TERMINAL_NO_DATA_PREFIX.len())
            .rposition(|window| window == TERMINAL_NO_DATA_PREFIX)
            .expect("synthetic response must contain its terminal ORA-01403");
        response[terminal + 1] = 0;
        response[terminal + 2] = 0;
        response.truncate(terminal + 8);
        response.push(TNS_MSG_TYPE_END_OF_RESPONSE);

        let decoded = sequential_op_decode(&response);
        assert_ne!(decoded.cursor_id, 0, "fixture must retain its cursor id");
        assert!(decoded.more_rows, "fixture must leave the cursor open");
        assert_eq!(decoded.rows.len(), 1, "fixture must retain its first row");
        response
    }

    /// One borrowed NUMBER continuation row with no terminal ORA-01403. The
    /// parser therefore reports `more_rows = true`, allowing tests to prove the
    /// speculative request-before-callback ordering without a live database.
    fn synthetic_open_borrowed_fetch_response(value: &str) -> Vec<u8> {
        use oracledb_protocol::thin::{
            encode_number_text, TNS_MSG_TYPE_ROW_DATA, TNS_MSG_TYPE_ROW_HEADER,
        };

        let number = encode_number_text(value).expect("synthetic NUMBER encodes");
        let number_len = u8::try_from(number.len()).expect("NUMBER uses short TTC bytes");
        let mut response = vec![
            TNS_MSG_TYPE_ROW_HEADER,
            0, // row-header flags
            1,
            1, // ub2(num requests = 1)
            1,
            1, // ub4(iteration = 1)
            1,
            1, // ub4(num iterations = 1)
            0, // ub2(buffer length = 0)
            0, // ub4(bit-vector bytes = 0)
            0, // ub4(rxhrid length = 0)
            TNS_MSG_TYPE_ROW_DATA,
            number_len,
        ];
        response.extend_from_slice(&number);
        response.push(TNS_MSG_TYPE_END_OF_RESPONSE);
        response
    }

    /// Reference decode of one op's response through the *same* public decoder
    /// the sequential execute path invokes (default caps/limits, matching the
    /// loopback connection) — the "sequential" side of the byte-identity check.
    fn sequential_op_decode(payload: &[u8]) -> QueryResult {
        parse_query_response_with_binds_options_columns_and_limits(
            payload,
            ClientCapabilities::default(),
            &[],
            ExecuteOptions::default(),
            &[],
            ProtocolLimits::DEFAULT,
        )
        .expect("synthetic execute response decodes")
    }

    /// Read one whole Large32-framed TNS packet (header + body) from a blocking
    /// std socket, returning the packet-type byte. Post-connect every packet on
    /// the wire is a Large32 DATA packet.
    fn read_one_wire_packet_bytes(socket: &mut std::net::TcpStream) -> (u8, Vec<u8>) {
        let mut header = [0u8; 8];
        socket.read_exact(&mut header).expect("read packet header");
        let declared = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let mut body = vec![0u8; declared - header.len()];
        socket.read_exact(&mut body).expect("read packet body");
        (header[4], body)
    }

    fn read_one_wire_packet(socket: &mut std::net::TcpStream) -> u8 {
        read_one_wire_packet_bytes(socket).0
    }

    fn read_one_wire_data_payload(socket: &mut std::net::TcpStream) -> Vec<u8> {
        let (packet_type, body) = read_one_wire_packet_bytes(socket);
        assert_eq!(
            packet_type, TNS_PACKET_TYPE_DATA,
            "expected a DATA request after recovery"
        );
        assert!(body.len() >= 2, "DATA packet must contain its flags");
        body[2..].to_vec()
    }

    /// Drive an `n`-statement pipeline over a loopback socket whose server side
    /// reads the ENTIRE batch (n ops + end-pipeline) before writing any byte
    /// back. Returns the decoded per-op results and the number of request
    /// packets the server read before it answered (== `n + 1` iff the client
    /// pipelined into a single round trip; a sequential client would deadlock
    /// this server after op-1).
    fn pipeline_batch_over_loopback(n: usize) -> (Vec<QueryResult>, usize) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            use std::io::Write as _;
            let (mut socket, _) = listener.accept().expect("accept test client");
            let mut requests_before_first_response = 0usize;
            for _ in 0..=n {
                let _packet_type = read_one_wire_packet(&mut socket);
                requests_before_first_response += 1;
            }
            let execute_response =
                data_packet(&synthetic_pipeline_execute_response_payload(), true);
            for _ in 0..n {
                socket
                    .write_all(&execute_response)
                    .expect("write op response");
            }
            // The (n+1)-th (end-pipeline) response: a bare END_OF_RESPONSE frame,
            // consumed by the runner for framing only.
            socket
                .write_all(&data_packet(
                    &[oracledb_protocol::thin::TNS_MSG_TYPE_END_OF_RESPONSE],
                    true,
                ))
                .expect("write end-pipeline response");
            socket.flush().expect("flush responses");
            requests_before_first_response
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let results = runtime.block_on(async {
            let cx = Cx::current().expect("ambient Cx");
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (read, write) = transport::plain_split(stream);
            let mut conn = loopback_connection(read, write);
            let requests: Vec<PipelineRequest> = (0..n)
                .map(|_| {
                    PipelineRequest::execute("select value from synthetic_fixture", Vec::new(), 2)
                })
                .collect();
            conn.run_pipeline_decoded(&cx, &requests, false)
                .await
                .expect("pipelined batch runs as one round trip")
                .into_iter()
                .enumerate()
                .map(|(index, op)| op.unwrap_or_else(|err| panic!("op {index} decoded: {err:?}")))
                .collect::<Vec<_>>()
        });

        let requests_before_first_response = server.join().expect("server thread joins");
        (results, requests_before_first_response)
    }

    #[test]
    fn pipeline_batch_offline_collapses_to_one_round_trip() {
        const N: usize = 10;
        let (results, requests_before_first_response) = pipeline_batch_over_loopback(N);

        assert_eq!(
            requests_before_first_response,
            N + 1,
            "all {N} ops + end-pipeline are written before any response is read == 1 round trip"
        );
        assert_eq!(results.len(), N, "one decoded result per pipelined op");

        // Byte-identical to sequential: each pipelined op decodes through the
        // same per-op decoder the ordinary execute path uses, over the same wire
        // bytes, so the materialized QueryResult is identical.
        let reference = sequential_op_decode(&synthetic_pipeline_execute_response_payload());
        for (index, decoded) in results.into_iter().enumerate() {
            assert_eq!(
                decoded, reference,
                "pipelined op {index} decodes byte-identically to the sequential path"
            );
        }
    }

    #[test]
    fn concurrent_pipeline_batches_do_not_serialize() {
        // Two independent connections each run a pipeline concurrently on their
        // own OS thread + runtime. The driver holds no cross-connection lock
        // (each Connection owns its transport), so — unlike python-oracledb's
        // GIL-bound thin mode — the batches proceed in parallel. Both must
        // complete with results identical to the sequential decode.
        const N: usize = 10;
        let reference = sequential_op_decode(&synthetic_pipeline_execute_response_payload());

        let worker_a = thread::spawn(|| pipeline_batch_over_loopback(N));
        let worker_b = thread::spawn(|| pipeline_batch_over_loopback(N));
        let (results_a, count_a) = worker_a.join().expect("worker a joins");
        let (results_b, count_b) = worker_b.join().expect("worker b joins");

        assert_eq!(count_a, N + 1, "batch A collapsed to one round trip");
        assert_eq!(count_b, N + 1, "batch B collapsed to one round trip");
        assert_eq!(results_a.len(), N);
        assert_eq!(results_b.len(), N);
        for (index, decoded) in results_a.iter().chain(results_b.iter()).enumerate() {
            assert_eq!(
                *decoded, reference,
                "concurrent pipelined op {index} matches the sequential decode"
            );
        }
    }

    #[test]
    fn in_flight_packet_resource_limit_marks_connection_dead() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            use std::io::Write as _;
            socket
                .write_all(&data_packet(&[0x01, 0x02, 0x03], true))
                .expect("write oversized packet");
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let conn = runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (read, write) = transport::plain_split(stream);
            let mut conn = loopback_connection(read, write);
            let limits = ProtocolLimits {
                max_packet_bytes: 12,
                max_frame_bytes: 16,
                max_response_bytes: 32,
                ..ProtocolLimits::DEFAULT
            }
            .validate()?;
            conn.protocol_limits = limits;
            conn.core.set_protocol_limits(limits)?;

            let err = conn
                .core
                .read_data_response(&cx)
                .await
                .expect_err("oversized in-flight packet must fail closed");
            assert!(matches!(
                err,
                Error::Protocol(oracledb_protocol::ProtocolError::ResourceLimit {
                    limit: "packet_bytes",
                    observed: 13,
                    maximum: 12,
                })
            ));
            Ok::<Connection, Error>(conn)
        })?;

        assert!(
            conn.is_dead(),
            "in-flight resource-limit violations leave unread wire bytes"
        );
        assert_eq!(conn.core.recovery.phase(), SessionRecoveryPhase::Dead);

        drop(conn);
        server.join().expect("server thread joins");
        Ok(())
    }

    // ---- classic (pre-END_OF_RESPONSE) probed response reads ----------------
    //
    // Pre-23ai servers (ACCEPT protocol version < 319) never send the
    // END_OF_RESPONSE/EOF data flags, so `read_data_response_probed` in classic
    // mode must decide completion with the caller's probe over the accumulated
    // payload — the flag-driven reader would wait until the call timeout.

    /// A classic two-packet response: the probe sees the accumulated payload
    /// after each DATA packet, reports "incomplete" after packet 1 and
    /// "complete" after packet 2, and the returned buffer is the flag-stripped
    /// concatenation of both packets.
    #[test]
    fn classic_probed_read_reassembles_until_probe_reports_complete() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            use std::io::Write as _;
            // Both packets are flagless: a classic server never sets
            // END_OF_RESPONSE/EOF, so only the probe can end the read.
            socket
                .write_all(&data_packet(&[0x01, 0x02], false))
                .expect("write first classic packet");
            socket
                .write_all(&data_packet(&[0x03, 0x04], false))
                .expect("write second classic packet");
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let probed = Arc::new(std::sync::Mutex::new(Vec::<Vec<u8>>::new()));
        let probed_in = Arc::clone(&probed);
        let response = runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (read, write) = transport::plain_split(stream);
            let mut conn = loopback_connection(read, write);
            conn.core
                .read_data_response_probed(&cx, true, move |bytes| {
                    probed_in
                        .lock()
                        .expect("probe snapshot lock")
                        .push(bytes.to_vec());
                    // "Parser consumed the whole response": needs all 4 bytes.
                    bytes.len() >= 4
                })
                .await
        })?;

        assert_eq!(
            response,
            [0x01, 0x02, 0x03, 0x04],
            "classic read must reassemble both flag-stripped payloads"
        );
        let probed = probed.lock().expect("probe snapshot lock");
        assert_eq!(
            probed.as_slice(),
            &[vec![0x01, 0x02], vec![0x01, 0x02, 0x03, 0x04]],
            "the probe must run over the accumulated payload after every packet"
        );
        server.join().expect("server thread joins");
        Ok(())
    }

    /// MARKER-free happy path: a single flagless classic packet whose probe
    /// reports complete immediately returns that packet's payload.
    #[test]
    fn classic_probed_read_single_packet_happy_path() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            use std::io::Write as _;
            socket
                .write_all(&data_packet(
                    &[0x09, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06],
                    false,
                ))
                .expect("write classic packet");
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let response = runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (read, write) = transport::plain_split(stream);
            let mut conn = loopback_connection(read, write);
            conn.core
                .read_data_response_probed(&cx, true, |bytes| !bytes.is_empty())
                .await
        })?;

        assert_eq!(response, [0x09, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
        server.join().expect("server thread joins");
        Ok(())
    }

    /// With `classic == false` the probed read delegates to the flag-framed
    /// reader: an END_OF_RESPONSE-flagged packet completes the response even
    /// though the probe never says so — the 23ai path is byte-identical and
    /// the probe is never consulted.
    #[test]
    fn probed_read_ignores_probe_when_not_classic() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            use std::io::Write as _;
            socket
                .write_all(&data_packet(&[0x01, 0x02, 0x03], true))
                .expect("write flag-framed packet");
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let response = runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (read, write) = transport::plain_split(stream);
            let mut conn = loopback_connection(read, write);
            conn.core
                .read_data_response_probed(&cx, false, |_| false)
                .await
        })?;

        assert_eq!(response, [0x01, 0x02, 0x03]);
        server.join().expect("server thread joins");
        Ok(())
    }

    /// A non-DATA/non-MARKER packet mid-response is a protocol violation: the
    /// classic probed read fails closed with `UnexpectedPacket` instead of
    /// accumulating garbage.
    #[test]
    fn classic_probed_read_rejects_non_data_packet() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            use std::io::Write as _;
            let packet = encode_packet(
                TNS_PACKET_TYPE_ACCEPT,
                0,
                None,
                &[0x00],
                PacketLengthWidth::Large32,
            )
            .expect("encode unexpected packet");
            socket.write_all(&packet).expect("write unexpected packet");
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let err = runtime.block_on(async {
            let cx = Cx::current().expect("ambient Cx");
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (read, write) = transport::plain_split(stream);
            let mut conn = loopback_connection(read, write);
            conn.core
                .read_data_response_probed(&cx, true, |_| true)
                .await
                .expect_err("non-DATA packet must fail closed")
        });

        assert!(matches!(
            err,
            Error::UnexpectedPacket(TNS_PACKET_TYPE_ACCEPT)
        ));
        assert_eq!(err.kind(), ErrorKind::Protocol);
        server.join().expect("server thread joins");
        Ok(())
    }

    // Regression (bead rust-oracledb-n2s): the multi-packet wide-row response
    // reassembler must NOT treat a non-final DATA packet that merely ends in the
    // byte 0x1d (TNS_MSG_TYPE_END_OF_RESPONSE, 29) as the end of the response.
    // Only a packet carrying the END_OF_RESPONSE/EOF data flag, or a minimal
    // packet whose entire post-flags payload is exactly the single byte 0x1d,
    // ends the response. Before the fix the `0x1d` check was unguarded and a
    // mid-stream wide-row packet boundary that happened to land on 0x1d
    // truncated the buffer ("encoded NUMBER too long" / "truncated TTC payload").
    #[test]
    fn data_packet_ends_response_requires_flag_or_lone_marker_byte() {
        const EOR: u8 = TNS_MSG_TYPE_END_OF_RESPONSE; // 29 / 0x1d
        const FOB: u8 = TNS_MSG_TYPE_FLUSH_OUT_BINDS; // 19 / 0x13
        let eor_flag = oracledb_protocol::thin::TNS_DATA_FLAGS_END_OF_RESPONSE;
        let eof_flag = oracledb_protocol::thin::TNS_DATA_FLAGS_EOF;

        // The END_OF_RESPONSE data flag ends the response regardless of payload.
        assert!(data_packet_ends_response(eor_flag, &[0x01, 0x02, EOR]));
        assert!(data_packet_ends_response(eor_flag, &[]));
        // The EOF data flag (final packet of legacy framing) likewise ends it.
        assert!(data_packet_ends_response(eof_flag, &[0x01, 0x02, 0x03]));

        // A lone marker byte arriving as its own minimal packet is a real
        // end-of-response (END_OF_RESPONSE or FLUSH_OUT_BINDS) -- the no-EOR
        // framing fallback.
        assert!(data_packet_ends_response(0x0000, &[EOR]));
        assert!(data_packet_ends_response(0x0000, &[FOB]));

        // THE BUG: a flagless (mid-stream) wide-row packet whose payload merely
        // ENDS in a marker byte (0x1d END_OF_RESPONSE, or 0x13 FLUSH_OUT_BINDS) is
        // NOT the end of the response. These must all be false so reassembly keeps
        // reading the following packets.
        assert!(!data_packet_ends_response(0x0000, &[0xc1, 0x02, EOR]));
        assert!(!data_packet_ends_response(0x0000, &[0x00, EOR]));
        assert!(!data_packet_ends_response(0x0000, &[EOR, 0x05, 0x06, EOR]));
        assert!(!data_packet_ends_response(0x0000, &[0xc1, 0x02, FOB]));
        assert!(!data_packet_ends_response(0x0000, &[0x00, FOB]));
        // A flagless packet that does not end in a marker byte also keeps reading.
        assert!(!data_packet_ends_response(0x0000, &[0x01, 0x02, 0x03]));
        assert!(!data_packet_ends_response(0x0000, &[]));
    }

    /// Pure replay of the `read_data_response_boundary` decision logic over a
    /// hand-built packet sequence, returning the reassembled bytes, the index it
    /// stopped at, and whether flush-out-binds was detected. Mirrors the async
    /// loop's break conditions exactly (minus I/O), INCLUDING the single-packet
    /// passthrough (bead rust-oracledb-0n0): when the response is one terminal
    /// packet, the owned buffer is moved instead of copied. The `payload` here is
    /// already flag-stripped, so the passthrough is a plain move of the same bytes
    /// — proving the optimization is byte-identical to the extend path.
    fn replay_boundary(packets: &[(u16, Vec<u8>)]) -> (Vec<u8>, Option<usize>, bool) {
        let mut reassembled = Vec::new();
        let mut stopped_at = None;
        for (index, (flags, payload)) in packets.iter().enumerate() {
            let ends = data_packet_ends_response(*flags, payload);
            if ends && reassembled.is_empty() {
                // Passthrough: move the (already flag-stripped) owned buffer.
                reassembled = payload.clone();
                stopped_at = Some(index);
                break;
            }
            reassembled.extend_from_slice(payload);
            if ends {
                stopped_at = Some(index);
                break;
            }
        }
        let flush_out_binds = matches!(reassembled.last(), Some(&TNS_MSG_TYPE_FLUSH_OUT_BINDS));
        (reassembled, stopped_at, flush_out_binds)
    }

    // End-to-end of the boundary loop over a hand-built multi-packet sequence:
    // body packets that END in the marker bytes 0x1d (END_OF_RESPONSE) and 0x13
    // (FLUSH_OUT_BINDS) -- the old false-positive triggers -- followed by the
    // real END_OF_RESPONSE-flagged tail. The reassembled payload must concatenate
    // every body packet's bytes (after its 2-byte data flags) in order, proving
    // no early break and no byte loss.
    #[test]
    fn boundary_loop_reassembles_packets_ending_in_marker_byte() {
        const EOR: u8 = TNS_MSG_TYPE_END_OF_RESPONSE;
        const FOB: u8 = TNS_MSG_TYPE_FLUSH_OUT_BINDS;
        let packets: [(u16, Vec<u8>); 5] = [
            (0x0000, vec![0x10, 0x11, EOR]), // ends in 0x1d -> must NOT stop
            (0x0000, vec![0x20, 0x21, 0x22, FOB]), // ends in 0x13 -> must NOT stop
            (0x0000, vec![0x30, 0x31, 0x32, EOR]), // ends in 0x1d -> must NOT stop
            (0x0000, vec![0x33, 0x34, 0x35]), // ordinary body packet
            (
                oracledb_protocol::thin::TNS_DATA_FLAGS_END_OF_RESPONSE,
                vec![0x40, 0x41, EOR], // real end: carries the EOR flag
            ),
        ];

        let (reassembled, stopped_at, flush_out_binds) = replay_boundary(&packets);

        assert_eq!(
            stopped_at,
            Some(4),
            "reassembly must stop only on the flagged final packet, not on a body packet ending in a marker byte"
        );
        assert!(
            !flush_out_binds,
            "the response ended in END_OF_RESPONSE, not FLUSH_OUT_BINDS"
        );
        assert_eq!(
            reassembled,
            vec![
                0x10, 0x11, EOR, // packet 0
                0x20, 0x21, 0x22, FOB, // packet 1
                0x30, 0x31, 0x32, EOR, // packet 2
                0x33, 0x34, 0x35, // packet 3
                0x40, 0x41, EOR, // packet 4 (final)
            ],
            "every body packet's bytes must be concatenated in order with none dropped"
        );
    }

    // A genuine flush-out-binds response (the EOR-flagged final packet ends in
    // the FLUSH_OUT_BINDS byte) must set the flag -- AND a mid-stream body packet
    // ending in that same byte must not have ended the response prematurely.
    #[test]
    fn boundary_loop_detects_flush_out_binds_only_at_true_boundary() {
        const FOB: u8 = TNS_MSG_TYPE_FLUSH_OUT_BINDS;
        let packets: [(u16, Vec<u8>); 2] = [
            (0x0000, vec![0x01, 0x02, FOB]), // body packet ending in 0x13 -> keep reading
            (
                oracledb_protocol::thin::TNS_DATA_FLAGS_END_OF_RESPONSE,
                vec![0x03, FOB], // EOR-flagged tail whose last message is FLUSH_OUT_BINDS
            ),
        ];

        let (reassembled, stopped_at, flush_out_binds) = replay_boundary(&packets);

        assert_eq!(stopped_at, Some(1), "stop on the EOR-flagged tail");
        assert!(
            flush_out_binds,
            "flush-out-binds must be detected from the terminal FLUSH_OUT_BINDS message byte"
        );
        assert_eq!(reassembled, vec![0x01, 0x02, FOB, 0x03, FOB]);
    }

    // Single-packet passthrough (bead rust-oracledb-0n0): a response that is ONE
    // terminal DATA packet must produce byte-identical reassembled output whether
    // it takes the passthrough (move the owned buffer) or the legacy extend path,
    // and FLUSH_OUT_BINDS detection on its terminal byte must be unchanged.
    #[test]
    fn single_packet_passthrough_is_byte_identical_to_extend() {
        const EOR: u8 = TNS_MSG_TYPE_END_OF_RESPONSE;
        const FOB: u8 = TNS_MSG_TYPE_FLUSH_OUT_BINDS;
        let eor_flag = oracledb_protocol::thin::TNS_DATA_FLAGS_END_OF_RESPONSE;

        // Helper: the legacy extend-only reassembly, for the equivalence oracle.
        fn replay_extend_only(packets: &[(u16, Vec<u8>)]) -> (Vec<u8>, Option<usize>, bool) {
            let mut reassembled = Vec::new();
            let mut stopped_at = None;
            for (index, (flags, payload)) in packets.iter().enumerate() {
                reassembled.extend_from_slice(payload);
                if data_packet_ends_response(*flags, payload) {
                    stopped_at = Some(index);
                    break;
                }
            }
            let flush = matches!(reassembled.last(), Some(&TNS_MSG_TYPE_FLUSH_OUT_BINDS));
            (reassembled, stopped_at, flush)
        }

        // Several single-terminal-packet shapes, including one ending in the
        // FLUSH_OUT_BINDS byte (so the flag-detection path is exercised).
        let cases: &[(u16, Vec<u8>)] = &[
            (eor_flag, vec![0x40, 0x41, EOR]),
            (eor_flag, vec![0x03, FOB]), // terminal FLUSH_OUT_BINDS
            (eor_flag, vec![0xde, 0xad, 0xbe, 0xef]),
            (eor_flag, vec![0x00]),
        ];
        for (flags, payload) in cases {
            let one = [(*flags, payload.clone())];
            let passthrough = replay_boundary(&one);
            let extend = replay_extend_only(&one);
            assert_eq!(
                passthrough, extend,
                "passthrough must equal extend for single packet {payload:02x?}"
            );
            // And the bytes are exactly the (already flag-stripped) payload.
            assert_eq!(&passthrough.0, payload);
        }
    }

    #[test]
    fn descriptor_builder_uses_identity_in_listener_cid() {
        let options = ConnectOptions::new("localhost/FREEPDB1", "user", "password", identity());
        let descriptor =
            EasyConnect::parse(&options.connect_string).expect("test connect string should parse");
        let full_descriptor = EasyConnect::parse_descriptor(&options.connect_string)
            .expect("test connect string should parse as full descriptor");
        let description = full_descriptor.first_description();
        let built = listener_connect_descriptor_with_server(
            &descriptor,
            description,
            &options.identity,
            false,
            false,
            true,
            None,
        );
        assert!(built.contains("(PROGRAM=program)"));
        assert!(built.contains("(HOST=machine)"));
        assert!(built.contains("(USER=osuser)"));
        assert!(!built.contains("(SERVER=emon)"));
        // emon variant injects the SERVER directive ahead of the CID block
        let emon = listener_connect_descriptor_with_server(
            &descriptor,
            description,
            &options.identity,
            true,
            false,
            true,
            None,
        );
        assert!(emon.contains("(SERVICE_NAME=FREEPDB1)(SERVER=emon)(CID="));
    }

    #[test]
    fn token_auth_descriptor_uses_tcps_security_and_passthrough() {
        let connect_string = concat!(
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(HOST=adb.example.test)(PORT=2484))",
            "(CONNECT_DATA=(SERVICE_NAME=adbsvc))",
            "(SECURITY=(SSL_SERVER_DN_MATCH=off)",
            "(SSL_SERVER_CERT_DN=CN=adb.example.test)",
            "(OCI_IAM_HOST=private-endpoint)))"
        );
        let descriptor = EasyConnect::parse(connect_string).expect("parse tcps descriptor");
        let full_descriptor =
            EasyConnect::parse_descriptor(connect_string).expect("parse full descriptor");
        let description = full_descriptor.first_description();
        let built = auth_connect_descriptor(
            &descriptor,
            description,
            true,
            false,
            description.security.ssl_server_cert_dn.as_deref(),
        );

        assert!(built.contains("(PROTOCOL=tcps)"));
        assert!(built.contains("(SECURITY="));
        assert!(built.contains("(SSL_SERVER_DN_MATCH=OFF)"));
        assert!(built.contains("(SSL_SERVER_CERT_DN=CN=adb.example.test)"));
        assert!(built.contains("(OCI_IAM_HOST=private-endpoint)"));
        assert!(built.contains("(TOKEN_AUTH=OCI_TOKEN)"));
        assert!(!built.contains("WALLET"));
    }

    #[test]
    fn transport_connect_timeout_bounds_post_dial_accept_read() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || -> std::io::Result<()> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(5)))?;
            let mut header = [0u8; 8];
            socket.read_exact(&mut header)?;
            let declared = usize::from(u16::from_be_bytes([header[0], header[1]]));
            let mut payload = vec![0u8; declared.saturating_sub(header.len())];
            socket.read_exact(&mut payload)?;
            thread::sleep(Duration::from_millis(300));
            Ok(())
        });

        let options = ConnectOptions::new(
            format!(
                "127.0.0.1:{}/FREEPDB1?transport_connect_timeout=100ms",
                addr.port()
            ),
            "user",
            "password",
            identity(),
        );
        let runtime = build_io_runtime().expect("asupersync runtime");
        let started = Instant::now();
        let err = runtime
            .block_on(async {
                let cx = Cx::current().expect("ambient Cx");
                Connection::connect(&cx, options).await
            })
            .expect_err("stalling listener should hit transport connect timeout");
        assert!(
            matches!(err, Error::CallTimeout(ms) if ms == 100),
            "expected 100ms CallTimeout, got {err:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "post-dial ACCEPT read should be bounded"
        );
        server.join().expect("server thread joins")?;
        Ok(())
    }

    #[test]
    fn async_cancel_handle_requests_break_and_reconciles_ready() -> Result<()> {
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
        let recovery = Arc::new(SessionRecovery::new());
        let mut handle = runtime.block_on(async {
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (_read, write) = transport::plain_split(stream);
            CancelHandle {
                write: Arc::new(AsyncMutex::with_name("oracle_tcp_write_test", write)),
                recovery: Arc::clone(&recovery),
            }
        });

        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            handle.cancel(&cx).await
        })?;

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
        assert_eq!(
            recovery.phase(),
            SessionRecoveryPhase::BreakSent,
            "CancelHandle::cancel(&Cx) requests cancellation without draining"
        );
        assert!(
            recovery.begin_pending_drain()?,
            "the connection owner must be able to adopt the pending cancel response"
        );
        recovery.finish_drain_ready();
        assert_eq!(
            recovery.phase(),
            SessionRecoveryPhase::Ready,
            "successful cancel-response drain reconciles the session to Ready"
        );
        Ok(())
    }

    #[test]
    fn async_cancel_handle_owner_drain_reconciles_ready() -> Result<()> {
        const INFLIGHT_BODY: &[u8] = &[0xCA, 0xFE];
        const ERROR_BODY: &[u8] = &[0x04, 0x01, 0x0d];
        const FRESH_BODY: &[u8] = &[0x07, 0x05, 0x0c];

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            use std::io::Write as _;

            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_BREAK,
                "CancelHandle must send exactly one BREAK request"
            );
            socket
                .write_all(&data_packet(INFLIGHT_BODY, true))
                .expect("write in-flight response");
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_BREAK))
                .expect("write break-ack marker");
            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_RESET,
                "owner drain must answer the break marker with RESET"
            );
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_RESET))
                .expect("write reset-confirm marker");
            socket
                .write_all(&data_packet(ERROR_BODY, true))
                .expect("write trailing cancel error packet");
            socket
                .write_all(&data_packet(FRESH_BODY, true))
                .expect("write fresh response");
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let next = runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (read, write) = transport::plain_split(stream);
            let mut conn = loopback_connection(read, write);
            let mut handle = conn.cancel_handle()?;

            handle.cancel(&cx).await?;
            assert_eq!(
                conn.core.recovery.phase(),
                SessionRecoveryPhase::BreakSent,
                "handle cancel only requests recovery"
            );
            conn.drain_cancel_response().await?;
            assert_eq!(
                conn.core.recovery.phase(),
                SessionRecoveryPhase::Ready,
                "owner drain reconciles a successful cancel response to Ready"
            );
            conn.core.read_data_response(&cx).await
        })?;

        assert_eq!(
            next, FRESH_BODY,
            "after owner drain the next read must consume the fresh response"
        );
        server.join().expect("server thread joins");
        Ok(())
    }

    #[test]
    fn async_cancel_handle_owner_drain_failure_marks_dead() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_BREAK,
                "CancelHandle must send the BREAK before the failed drain"
            );
            // Drop the socket without sending a complete cancel response.
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (read, write) = transport::plain_split(stream);
            let mut conn = loopback_connection(read, write);
            let mut handle = conn.cancel_handle()?;

            handle.cancel(&cx).await?;
            assert_eq!(conn.core.recovery.phase(), SessionRecoveryPhase::BreakSent);
            assert!(
                conn.drain_cancel_response().await.is_err(),
                "incomplete cancel response must fail owner drain"
            );
            assert!(
                conn.is_dead(),
                "failed owner drain marks the connection dead"
            );
            assert_eq!(conn.core.recovery.phase(), SessionRecoveryPhase::Dead);
            Ok::<(), Error>(())
        })?;

        server.join().expect("server thread joins");
        Ok(())
    }

    #[test]
    fn async_cancel_handle_does_not_send_duplicate_break_when_recovery_pending() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            let mut packet = [0u8; 11];
            socket.read_exact(&mut packet).expect("read first break");
            socket
                .set_read_timeout(Some(Duration::from_millis(200)))
                .expect("set short read timeout");
            let mut extra = [0u8; 1];
            let extra_read = socket.read_exact(&mut extra);
            assert!(
                matches!(
                    extra_read.as_ref().map_err(std::io::Error::kind),
                    Err(std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)
                ),
                "duplicate BREAK check expected a read timeout, got {extra_read:?} with byte {:02x}",
                extra[0]
            );
            packet
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let recovery = Arc::new(SessionRecovery::new());
        let mut handle = runtime.block_on(async {
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (_read, write) = transport::plain_split(stream);
            CancelHandle {
                write: Arc::new(AsyncMutex::with_name("oracle_tcp_write_test", write)),
                recovery: Arc::clone(&recovery),
            }
        });

        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            handle.cancel(&cx).await?;
            assert_eq!(recovery.phase(), SessionRecoveryPhase::BreakSent);
            handle.cancel(&cx).await?;
            assert!(
                recovery.begin_pending_drain()?,
                "test moves the pending cancel into Draining"
            );
            handle.cancel(&cx).await
        })?;

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
        assert_eq!(recovery.phase(), SessionRecoveryPhase::Draining);
        Ok(())
    }

    #[test]
    fn blocking_cancel_handle_sends_tns_break_marker() {
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
        let recovery = Arc::new(SessionRecovery::new());
        let mut handle = runtime.block_on(async {
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (_read, write) = transport::plain_split(stream);
            CancelHandle {
                write: Arc::new(AsyncMutex::with_name("oracle_tcp_write_test", write)),
                recovery: Arc::clone(&recovery),
            }
        });

        handle.cancel_blocking().expect("cancel marker write");

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
        assert_eq!(recovery.phase(), SessionRecoveryPhase::BreakSent);
    }

    // ---- break_and_drain regression (bead rust-oracledb-2vx) -------------------
    //
    // On a call timeout the driver must send a BREAK and then DRAIN the server's
    // in-flight response + RESET handshake + trailing error packet, leaving the
    // wire at a clean boundary so the NEXT operation on the reused connection
    // reads its own response — not the stale bytes left behind by the timed-out
    // call. The reference does this via `_break_external()` + `_receive_packet()`
    // (-> `_reset()` on the MARKER), protocol.pyx:449-451, 507-557.

    const EOR_FLAG: u16 = oracledb_protocol::thin::TNS_DATA_FLAGS_END_OF_RESPONSE;

    /// A DATA packet carrying `message` after its 2-byte data flags. When
    /// `end_of_response` is set it carries the END_OF_RESPONSE data flag, so the
    /// reassembler treats it as the final packet of a response.
    fn data_packet(message: &[u8], end_of_response: bool) -> Vec<u8> {
        let flags = if end_of_response { EOR_FLAG } else { 0 };
        encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(flags),
            message,
            PacketLengthWidth::Large32,
        )
        .expect("encode data packet")
    }

    /// A MARKER packet of the given marker type (`[1, 0, marker_type]` payload,
    /// matching `send_marker`).
    fn marker_packet(marker_type: u8) -> Vec<u8> {
        encode_packet(
            TNS_PACKET_TYPE_MARKER,
            0,
            None,
            &[1, 0, marker_type],
            PacketLengthWidth::Large32,
        )
        .expect("encode marker packet")
    }

    #[derive(Debug)]
    struct ScriptedTransport;

    impl WireTransport for ScriptedTransport {
        type Read = ScriptedRead;
        type Write = ScriptedWrite;
    }

    #[derive(Debug)]
    struct ScriptedRead {
        state: Arc<std::sync::Mutex<ScriptedIoState>>,
    }

    impl ScriptedRead {
        fn from_state(state: Arc<std::sync::Mutex<ScriptedIoState>>) -> Self {
            Self { state }
        }
    }

    impl asupersync::io::AsyncRead for ScriptedRead {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut asupersync::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let Ok(mut state) = self.state.lock() else {
                return std::task::Poll::Ready(Err(std::io::Error::other(
                    "scripted transport: poisoned read state",
                )));
            };

            loop {
                match state.read.front_mut() {
                    Some(ReadAction::Bytes {
                        bytes,
                        offset,
                        max_chunk,
                        cancel_current_on_completion,
                    }) => {
                        if *offset >= bytes.len() {
                            state.read.pop_front();
                            continue;
                        }
                        let cap = max_chunk.unwrap_or(usize::MAX);
                        let available = bytes.len() - *offset;
                        let take = available.min(buf.remaining()).min(cap);
                        if take == 0 {
                            return std::task::Poll::Ready(Ok(()));
                        }
                        buf.put_slice(&bytes[*offset..*offset + take]);
                        *offset += take;
                        if *offset >= bytes.len() {
                            let cancel_current_on_completion = *cancel_current_on_completion;
                            state.read.pop_front();
                            if cancel_current_on_completion {
                                if let Some(cx) = Cx::current() {
                                    cx.cancel_fast(asupersync::CancelKind::User);
                                }
                            }
                        }
                        return std::task::Poll::Ready(Ok(()));
                    }
                    Some(ReadAction::Pending) => {
                        state.read.pop_front();
                        cx.waker().wake_by_ref();
                        return std::task::Poll::Pending;
                    }
                    Some(ReadAction::PendingUntil(gate)) => {
                        if gate.is_open() {
                            state.read.pop_front();
                            continue;
                        }
                        gate.register(cx.waker());
                        return std::task::Poll::Pending;
                    }
                    Some(ReadAction::Eof) | None => return std::task::Poll::Ready(Ok(())),
                    Some(ReadAction::Error(message)) => {
                        let message = *message;
                        state.read.pop_front();
                        return std::task::Poll::Ready(Err(std::io::Error::other(message)));
                    }
                    Some(ReadAction::AdvanceTime(duration)) => {
                        let duration = *duration;
                        state.read.pop_front();
                        state.clock.advance(duration);
                    }
                }
            }
        }
    }

    #[derive(Debug)]
    struct ScriptedWrite {
        state: Arc<std::sync::Mutex<ScriptedIoState>>,
    }

    impl ScriptedWrite {
        fn from_state(state: Arc<std::sync::Mutex<ScriptedIoState>>) -> Self {
            Self { state }
        }
    }

    impl asupersync::io::AsyncWrite for ScriptedWrite {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            let Ok(mut state) = self.state.lock() else {
                return std::task::Poll::Ready(Err(std::io::Error::other(
                    "scripted transport: poisoned write state",
                )));
            };

            loop {
                match state.write.front_mut() {
                    Some(WriteAction::Expect { .. }) => {
                        let mut completed = None;
                        let take = {
                            let Some(WriteAction::Expect {
                                bytes,
                                offset,
                                max_chunk,
                            }) = state.write.front_mut()
                            else {
                                unreachable!("front action already matched Expect");
                            };
                            if *offset >= bytes.len() {
                                state.write.pop_front();
                                continue;
                            }
                            let cap = max_chunk.unwrap_or(usize::MAX);
                            let available = bytes.len() - *offset;
                            let take = available.min(buf.len()).min(cap);
                            if take == 0 {
                                return std::task::Poll::Ready(Ok(0));
                            }
                            if bytes[*offset..*offset + take] != buf[..take] {
                                return std::task::Poll::Ready(Err(std::io::Error::other(
                                    "scripted transport: write mismatch",
                                )));
                            }
                            *offset += take;
                            if *offset >= bytes.len() {
                                completed = Some(bytes.clone());
                            }
                            take
                        };
                        if let Some(bytes) = completed {
                            state.note_write(&bytes);
                            state.write.pop_front();
                        }
                        return std::task::Poll::Ready(Ok(take));
                    }
                    Some(WriteAction::Pending) => {
                        state.write.pop_front();
                        cx.waker().wake_by_ref();
                        return std::task::Poll::Pending;
                    }
                    Some(WriteAction::Eof) => {
                        state.write.pop_front();
                        return std::task::Poll::Ready(Ok(0));
                    }
                    Some(WriteAction::Error(message)) => {
                        let message = *message;
                        state.write.pop_front();
                        return std::task::Poll::Ready(Err(std::io::Error::other(message)));
                    }
                    Some(WriteAction::AdvanceTime(duration)) => {
                        let duration = *duration;
                        state.write.pop_front();
                        state.clock.advance(duration);
                    }
                    None => {
                        return std::task::Poll::Ready(Err(std::io::Error::other(
                            "scripted transport: unexpected write",
                        )));
                    }
                }
            }
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[derive(Clone, Debug, Default)]
    struct ScriptedClock {
        nanos: Arc<std::sync::atomic::AtomicU64>,
    }

    impl ScriptedClock {
        fn advance(&self, duration: Duration) {
            let nanos = match u64::try_from(duration.as_nanos()) {
                Ok(nanos) => nanos,
                Err(_) => u64::MAX,
            };
            self.nanos.fetch_add(nanos, Ordering::Relaxed);
        }

        fn elapsed(&self) -> Duration {
            Duration::from_nanos(self.nanos.load(Ordering::Relaxed))
        }
    }

    #[derive(Clone, Default)]
    struct ScriptedGate {
        ready: Arc<AtomicBool>,
        waker: Arc<std::sync::Mutex<Option<Waker>>>,
    }

    impl std::fmt::Debug for ScriptedGate {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ScriptedGate")
                .field("ready", &self.ready.load(Ordering::Relaxed))
                .finish_non_exhaustive()
        }
    }

    impl ScriptedGate {
        fn open(&self) {
            self.ready.store(true, Ordering::Release);
            if let Ok(mut waker) = self.waker.lock() {
                if let Some(waker) = waker.take() {
                    waker.wake();
                }
            }
        }

        fn is_open(&self) -> bool {
            self.ready.load(Ordering::Acquire)
        }

        fn register(&self, waker: &Waker) {
            if let Ok(mut current) = self.waker.lock() {
                let replace = current
                    .as_ref()
                    .is_none_or(|registered| !registered.will_wake(waker));
                if replace {
                    *current = Some(waker.clone());
                }
            }
        }
    }

    #[derive(Debug)]
    struct ScriptedIoState {
        read: VecDeque<ReadAction>,
        write: VecDeque<WriteAction>,
        clock: ScriptedClock,
        break_writes: usize,
        reset_writes: usize,
    }

    impl ScriptedIoState {
        fn new(read: Vec<ReadAction>, write: Vec<WriteAction>, clock: ScriptedClock) -> Self {
            Self {
                read: read.into(),
                write: write.into(),
                clock,
                break_writes: 0,
                reset_writes: 0,
            }
        }

        fn is_consumed(&self) -> bool {
            self.read.is_empty() && self.write.is_empty()
        }

        fn note_write(&mut self, bytes: &[u8]) {
            let break_marker = marker_packet(TNS_MARKER_TYPE_BREAK);
            let reset_marker = marker_packet(TNS_MARKER_TYPE_RESET);
            if bytes == break_marker.as_slice() {
                self.break_writes += 1;
            } else if bytes == reset_marker.as_slice() {
                self.reset_writes += 1;
            }
        }
    }

    #[derive(Debug)]
    enum ReadAction {
        Bytes {
            bytes: Vec<u8>,
            offset: usize,
            max_chunk: Option<usize>,
            cancel_current_on_completion: bool,
        },
        Pending,
        PendingUntil(ScriptedGate),
        Eof,
        Error(&'static str),
        AdvanceTime(Duration),
    }

    impl ReadAction {
        fn bytes(bytes: Vec<u8>, max_chunk: Option<usize>) -> Self {
            Self::Bytes {
                bytes,
                offset: 0,
                max_chunk,
                cancel_current_on_completion: false,
            }
        }

        fn bytes_then_cancel_current(bytes: Vec<u8>, max_chunk: Option<usize>) -> Self {
            Self::Bytes {
                bytes,
                offset: 0,
                max_chunk,
                cancel_current_on_completion: true,
            }
        }
    }

    #[derive(Debug)]
    enum WriteAction {
        Expect {
            bytes: Vec<u8>,
            offset: usize,
            max_chunk: Option<usize>,
        },
        Pending,
        Eof,
        Error(&'static str),
        AdvanceTime(Duration),
    }

    impl WriteAction {
        fn expect_bytes(bytes: Vec<u8>, max_chunk: Option<usize>) -> Self {
            Self::Expect {
                bytes,
                offset: 0,
                max_chunk,
            }
        }
    }

    fn test_cx() -> Result<Cx> {
        Cx::current().ok_or_else(|| Error::Runtime("missing ambient Cx in test runtime".into()))
    }

    #[test]
    fn connection_core_routes_connect_execute_fetch_over_scripted_transport() -> Result<()> {
        const EXECUTE_BODY: &[u8] = b"scripted execute payload";
        const FETCH_BODY: &[u8] = b"scripted fetch payload";
        const EXECUTE_RESPONSE: &[u8] = b"scripted execute response";
        const FETCH_RESPONSE: &[u8] = b"scripted fetch response";

        let connect_packet = encode_packet(
            TNS_PACKET_TYPE_CONNECT,
            0,
            None,
            b"SCRIPTED-CONNECT",
            PacketLengthWidth::Legacy16,
        )?;
        let accept_packet = encode_packet(
            TNS_PACKET_TYPE_ACCEPT,
            0,
            None,
            b"SCRIPTED-ACCEPT",
            PacketLengthWidth::Legacy16,
        )?;
        let execute_packet = encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(0),
            EXECUTE_BODY,
            PacketLengthWidth::Large32,
        )?;
        let fetch_packet = encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(0),
            FETCH_BODY,
            PacketLengthWidth::Large32,
        )?;

        let script = Arc::new(std::sync::Mutex::new(ScriptedIoState::new(
            vec![
                ReadAction::bytes(accept_packet, None),
                ReadAction::bytes(data_packet(EXECUTE_RESPONSE, true), None),
                ReadAction::bytes(data_packet(FETCH_RESPONSE, true), None),
            ],
            vec![
                WriteAction::expect_bytes(connect_packet.clone(), None),
                WriteAction::expect_bytes(execute_packet, None),
                WriteAction::expect_bytes(fetch_packet, None),
            ],
            ScriptedClock::default(),
        )));
        let mut core = ConnectionCore::<ScriptedTransport>::from_halves(
            ScriptedRead::from_state(Arc::clone(&script)),
            ScriptedWrite::from_state(Arc::clone(&script)),
            "scripted_core_write",
        );

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            core.write_all(&cx, &connect_packet).await?;
            let accept = core.read_packet(PacketLengthWidth::Legacy16).await?;
            assert_eq!(accept.packet_type, TNS_PACKET_TYPE_ACCEPT);
            assert_eq!(accept.payload, b"SCRIPTED-ACCEPT");

            core.send_data_packet(&cx, EXECUTE_BODY, 8192).await?;
            let execute_response = core.read_data_response(&cx).await?;
            assert_eq!(execute_response, EXECUTE_RESPONSE);

            core.send_data_packet(&cx, FETCH_BODY, 8192).await?;
            let fetch_response = core.read_data_response(&cx).await?;
            assert_eq!(fetch_response, FETCH_RESPONSE);
            Ok::<_, Error>(())
        })?;

        let state = script
            .lock()
            .map_err(|_| Error::Runtime("scripted I/O state lock poisoned".into()))?;
        assert!(
            state.is_consumed(),
            "scripted core must perform exactly the expected connect/execute/fetch I/O"
        );
        Ok(())
    }

    #[test]
    fn scripted_transport_replays_short_pending_and_virtual_time() -> Result<()> {
        const EXECUTE_BODY: &[u8] = b"fault-matrix execute payload";
        const EXECUTE_RESPONSE: &[u8] = b"fault-matrix execute response";

        let connect_packet = encode_packet(
            TNS_PACKET_TYPE_CONNECT,
            0,
            None,
            b"FAULT-MATRIX-CONNECT",
            PacketLengthWidth::Legacy16,
        )?;
        let accept_packet = encode_packet(
            TNS_PACKET_TYPE_ACCEPT,
            0,
            None,
            b"FAULT-MATRIX-ACCEPT",
            PacketLengthWidth::Legacy16,
        )?;
        let execute_packet = encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(0),
            EXECUTE_BODY,
            PacketLengthWidth::Large32,
        )?;
        let execute_response_packet = data_packet(EXECUTE_RESPONSE, true);

        let clock = ScriptedClock::default();
        let script = Arc::new(std::sync::Mutex::new(ScriptedIoState::new(
            vec![
                ReadAction::Pending,
                ReadAction::AdvanceTime(Duration::from_millis(7)),
                ReadAction::bytes(accept_packet, Some(3)),
                ReadAction::Pending,
                ReadAction::AdvanceTime(Duration::from_millis(11)),
                ReadAction::bytes(execute_response_packet, Some(2)),
            ],
            vec![
                WriteAction::Pending,
                WriteAction::AdvanceTime(Duration::from_millis(5)),
                WriteAction::expect_bytes(connect_packet.clone(), Some(2)),
                WriteAction::Pending,
                WriteAction::AdvanceTime(Duration::from_millis(13)),
                WriteAction::expect_bytes(execute_packet, Some(3)),
            ],
            clock.clone(),
        )));
        let mut core = ConnectionCore::<ScriptedTransport>::from_halves(
            ScriptedRead::from_state(Arc::clone(&script)),
            ScriptedWrite::from_state(Arc::clone(&script)),
            "scripted_fault_matrix_write",
        );

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            core.write_all(&cx, &connect_packet).await?;
            let accept = core.read_packet(PacketLengthWidth::Legacy16).await?;
            assert_eq!(accept.packet_type, TNS_PACKET_TYPE_ACCEPT);
            assert_eq!(accept.payload, b"FAULT-MATRIX-ACCEPT");

            core.send_data_packet(&cx, EXECUTE_BODY, 8192).await?;
            let execute_response = core.read_data_response(&cx).await?;
            assert_eq!(execute_response, EXECUTE_RESPONSE);
            Ok::<_, Error>(())
        })?;

        assert_eq!(
            clock.elapsed(),
            Duration::from_millis(36),
            "virtual time advances are deterministic and do not require wall-clock sleeps"
        );
        let state = script
            .lock()
            .map_err(|_| Error::Runtime("scripted I/O state lock poisoned".into()))?;
        assert!(
            state.is_consumed(),
            "scripted fault matrix must consume every read/write step"
        );
        Ok(())
    }

    const DPOR_SATURATION_WINDOW: usize = 1;
    const DPOR_WIRE_SEED: u64 = 0xE3_E4_D0_00;
    const DPOR_WIRE_MAX_ITERS: usize = 96;
    const DPOR_WIRE_TIMEOUT_MS: u32 = 1;
    fn dpor_wire_recovery_timeout() -> Duration {
        Duration::from_secs(1)
    }

    #[derive(Clone, Copy, Debug)]
    enum DporWireMode {
        UserCancel,
        Timeout,
    }

    fn dpor_wire_seed(mode: DporWireMode) -> u64 {
        DPOR_WIRE_SEED
            + match mode {
                DporWireMode::UserCancel => 0,
                DporWireMode::Timeout => 1,
            }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum DporWireResultKind {
        Cancelled,
        CallTimeout,
    }

    #[derive(Debug)]
    struct DporWireObservation {
        result: DporWireResultKind,
        phase: SessionRecoveryPhase,
        break_writes: usize,
        reset_writes: usize,
        script_consumed: bool,
    }

    async fn dpor_read_until_cancel_gate(
        core: &mut ConnectionCore<ScriptedTransport>,
        cx: &Cx,
        cancel_gate: &ScriptedGate,
    ) -> Result<Vec<u8>> {
        let recovery = Arc::clone(&core.recovery);
        let read = async {
            let mut guard = CancelDrainGuard::arm(recovery)?;
            let response = core.read_data_response(cx).await;
            if response.is_ok() {
                guard.disarm();
            }
            response
        };
        let cancel = poll_fn(|task_cx| {
            if cancel_gate.is_open() {
                Poll::Ready(())
            } else {
                cancel_gate.register(task_cx.waker());
                Poll::Pending
            }
        });
        let mut read = pin!(read);
        let mut cancel = pin!(cancel);

        poll_fn(|task_cx| {
            if cancel_gate.is_open() {
                return Poll::Ready(Err(Error::Cancelled));
            }
            if let Poll::Ready(result) = read.as_mut().poll(task_cx) {
                return Poll::Ready(result);
            }
            if let Poll::Ready(()) = cancel.as_mut().poll(task_cx) {
                return Poll::Ready(Err(Error::Cancelled));
            }
            Poll::Pending
        })
        .await
    }

    async fn dpor_read_until_timeout_gate(
        core: &mut ConnectionCore<ScriptedTransport>,
        cx: &Cx,
        timeout_gate: &ScriptedGate,
    ) -> Result<Vec<u8>> {
        let recovery = Arc::clone(&core.recovery);
        let read = async {
            let mut guard = CancelDrainGuard::arm(recovery)?;
            let response = core.read_data_response(cx).await;
            if response.is_ok() {
                guard.disarm();
            }
            response
        };
        let timeout = poll_fn(|task_cx| {
            if timeout_gate.is_open() {
                Poll::Ready(())
            } else {
                timeout_gate.register(task_cx.waker());
                Poll::Pending
            }
        });
        let mut read = pin!(read);
        let mut timeout = pin!(timeout);

        poll_fn(|task_cx| {
            if timeout_gate.is_open() {
                return Poll::Ready(Err(Error::CallTimeout(DPOR_WIRE_TIMEOUT_MS)));
            }
            if let Poll::Ready(result) = read.as_mut().poll(task_cx) {
                return Poll::Ready(result);
            }
            if let Poll::Ready(()) = timeout.as_mut().poll(task_cx) {
                return Poll::Ready(Err(Error::CallTimeout(DPOR_WIRE_TIMEOUT_MS)));
            }
            Poll::Pending
        })
        .await
    }

    fn dpor_wire_script(
        gate: ScriptedGate,
        execute_packet: Vec<u8>,
    ) -> Arc<std::sync::Mutex<ScriptedIoState>> {
        const INFLIGHT_BODY: &[u8] = b"dpor in-flight response";
        const ERROR_BODY: &[u8] = &[0x04, 0x01, 0x0d];

        Arc::new(std::sync::Mutex::new(ScriptedIoState::new(
            vec![
                ReadAction::PendingUntil(gate),
                ReadAction::bytes(data_packet(INFLIGHT_BODY, true), Some(3)),
                ReadAction::Pending,
                ReadAction::bytes(marker_packet(TNS_MARKER_TYPE_BREAK), Some(2)),
                ReadAction::Pending,
                ReadAction::bytes(marker_packet(TNS_MARKER_TYPE_RESET), Some(2)),
                ReadAction::Pending,
                ReadAction::bytes(data_packet(ERROR_BODY, true), Some(2)),
            ],
            vec![
                WriteAction::Pending,
                WriteAction::expect_bytes(execute_packet, Some(3)),
                WriteAction::Pending,
                WriteAction::expect_bytes(marker_packet(TNS_MARKER_TYPE_BREAK), Some(2)),
                WriteAction::Pending,
                WriteAction::expect_bytes(marker_packet(TNS_MARKER_TYPE_RESET), Some(2)),
            ],
            ScriptedClock::default(),
        )))
    }

    async fn run_dpor_wire_operation(
        mode: DporWireMode,
        gate: ScriptedGate,
        script: Arc<std::sync::Mutex<ScriptedIoState>>,
        execute_body: &'static [u8],
    ) -> Result<DporWireObservation> {
        let cx = Cx::current().expect("LabRuntime task should install an ambient Cx");
        let mut core = ConnectionCore::<ScriptedTransport>::from_halves(
            ScriptedRead::from_state(Arc::clone(&script)),
            ScriptedWrite::from_state(Arc::clone(&script)),
            "dpor_wire_core_write",
        );
        core.send_data_packet(&cx, execute_body, 8192).await?;

        let result = match mode {
            DporWireMode::UserCancel => dpor_read_until_cancel_gate(&mut core, &cx, &gate).await,
            DporWireMode::Timeout => dpor_read_until_timeout_gate(&mut core, &cx, &gate).await,
        };
        let result = match result {
            Ok(payload) => {
                return Err(Error::Runtime(format!(
                    "DPOR wire race unexpectedly completed normally with payload {payload:?}"
                )));
            }
            Err(Error::Cancelled) => {
                core.recovery.begin_drain_after_break()?;
                core.cancel_and_drain_wire(dpor_wire_recovery_timeout())?;
                core.recovery.finish_drain_ready();
                DporWireResultKind::Cancelled
            }
            Err(Error::CallTimeout(_)) => {
                core.recovery.begin_drain_after_break()?;
                core.break_and_drain_wire(dpor_wire_recovery_timeout())?;
                core.recovery.finish_drain_ready();
                DporWireResultKind::CallTimeout
            }
            Err(err) => return Err(err),
        };

        let state = script
            .lock()
            .map_err(|_| Error::Runtime("scripted DPOR wire state lock poisoned".into()))?;
        Ok(DporWireObservation {
            result,
            phase: core.recovery.phase(),
            break_writes: state.break_writes,
            reset_writes: state.reset_writes,
            script_consumed: state.is_consumed(),
        })
    }

    fn explore_dpor_wire_mode(mode: DporWireMode) -> asupersync::lab::ExplorationReport {
        const EXECUTE_BODY: &[u8] = b"dpor execute payload";
        let execute_packet = encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(0),
            EXECUTE_BODY,
            PacketLengthWidth::Large32,
        )
        .expect("encode DPOR execute packet");

        let mut explorer = DporExplorer::new(
            ExplorerConfig::new(dpor_wire_seed(mode), DPOR_WIRE_MAX_ITERS).max_steps(100_000),
        );
        explorer.explore(|runtime: &mut LabRuntime| {
            // Full ConnectionCore execution inside LabRuntime does not quiesce
            // because the recovery path intentionally leaves the lab executor
            // and drains on a blocking recovery thread. Keep DPOR on the finite
            // operation-vs-interrupt ordering, then run the actual scripted
            // ConnectionCore recovery path below for every explored schedule.
            let order = Arc::new(std::sync::Mutex::new(Vec::new()));
            let root = runtime.state.create_root_region(Budget::INFINITE);

            let operation_order = Arc::clone(&order);
            let (operation, _operation_handle) = runtime
                .state
                .create_task(root, Budget::INFINITE, async move {
                    operation_order
                        .lock()
                        .expect("record DPOR wire operation ordering")
                        .push("operation");
                })
                .expect("create DPOR wire operation task");
            runtime.scheduler.lock().schedule(operation, 0);

            let interrupt_order = Arc::clone(&order);
            let (interrupt, _interrupt_handle) = runtime
                .state
                .create_task(root, Budget::INFINITE, async move {
                    interrupt_order
                        .lock()
                        .expect("record DPOR wire interrupt ordering")
                        .push("interrupt");
                })
                .expect("create DPOR wire interrupt task");
            runtime.scheduler.lock().schedule(interrupt, 0);
            runtime.run_until_quiescent();
            assert!(
                runtime.is_quiescent(),
                "DPOR wire ordering model did not quiesce"
            );

            let observed_order = order.lock().expect("read DPOR wire ordering").clone();
            assert_eq!(
                observed_order.len(),
                2,
                "DPOR wire ordering should include operation and interrupt"
            );

            let replay_gate = ScriptedGate::default();
            replay_gate.open();
            let script = dpor_wire_script(replay_gate.clone(), execute_packet.clone());
            let io_runtime = build_io_runtime().expect("asupersync runtime for DPOR wire replay");
            let observed = io_runtime
                .block_on(run_dpor_wire_operation(
                    mode,
                    replay_gate,
                    script,
                    EXECUTE_BODY,
                ))
                .expect("DPOR wire operation should not fail");
            let expected = match mode {
                DporWireMode::UserCancel => DporWireResultKind::Cancelled,
                DporWireMode::Timeout => DporWireResultKind::CallTimeout,
            };
            assert_eq!(
                observed.result, expected,
                "delivered cancel/timeout mapped to the wrong public error"
            );
            assert_eq!(
                observed.phase,
                SessionRecoveryPhase::Ready,
                "wire recovery must finish at a clean Ready boundary"
            );
            assert_eq!(observed.break_writes, 1, "exactly one BREAK is required");
            assert_eq!(observed.reset_writes, 1, "exactly one RESET is required");
            assert!(
                observed.script_consumed,
                "wire recovery must consume the whole scripted break response"
            );
        })
    }

    #[test]
    fn dpor_wire_cancel_and_timeout_recovery_saturates() {
        for mode in [DporWireMode::UserCancel, DporWireMode::Timeout] {
            let report = explore_dpor_wire_mode(mode);
            eprintln!(
                "[dpor-wire] mode={mode:?} seed={} max_iters={} runs={} classes={} saturated={}",
                dpor_wire_seed(mode),
                DPOR_WIRE_MAX_ITERS,
                report.total_runs,
                report.unique_classes,
                report.coverage.is_saturated(DPOR_SATURATION_WINDOW)
            );
            assert!(
                !report.has_violations(),
                "DPOR wire {mode:?} found violations at seeds {:?}",
                report.violation_seeds()
            );
            assert!(
                report.total_runs == DPOR_WIRE_MAX_ITERS,
                "DPOR wire fallback seed space did not complete for {mode:?}: runs={}, classes={}, new={}",
                report.total_runs,
                report.unique_classes,
                report.coverage.new_class_discoveries
            );
        }
    }

    #[test]
    fn flush_out_binds_observes_cancel_before_follow_up_round_trip() -> Result<()> {
        let first_response = data_packet(&[TNS_MSG_TYPE_FLUSH_OUT_BINDS], true);
        let unread_response = data_packet(b"must-not-read-after-cancel", true);
        let script = Arc::new(std::sync::Mutex::new(ScriptedIoState::new(
            vec![
                ReadAction::bytes_then_cancel_current(first_response, None),
                ReadAction::bytes(unread_response, None),
            ],
            Vec::new(),
            ScriptedClock::default(),
        )));

        let runtime = build_io_runtime()?;
        let err = runtime.block_on(async {
            let cx = test_cx()?;
            let mut read = ScriptedRead::from_state(Arc::clone(&script));
            let write = Arc::new(AsyncMutex::with_name(
                "flush_cancel_checkpoint_test_write",
                ScriptedWrite::from_state(Arc::clone(&script)),
            ));
            let err = read_data_response_flushing_out_binds(&mut read, &cx, &write, 8192)
                .await
                .expect_err("cancel checkpoint must stop before FLUSH_OUT_BINDS follow-up");
            Ok::<_, Error>(err)
        })?;

        // W1-T6.2: a cancel checkpoint surfaces the DISTINCT Error::Cancelled
        // (ORA-01013), never a generic Error::Runtime(display_string).
        assert!(
            matches!(&err, Error::Cancelled),
            "flush continuation should stop on the cancellation checkpoint with a distinct cancel error, got {err:?}"
        );
        assert_eq!(err.kind(), ErrorKind::Cancel);
        let state = script
            .lock()
            .map_err(|_| Error::Runtime("scripted I/O state lock poisoned".into()))?;
        assert_eq!(
            state.read.len(),
            1,
            "the next response packet must not be read after cancellation"
        );
        assert!(
            state.write.is_empty(),
            "the FLUSH_OUT_BINDS follow-up must not be sent after cancellation"
        );
        Ok(())
    }

    #[test]
    fn pending_cx_cancel_is_observed_before_fetch_continuation_write() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<Option<u8>> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_millis(300)))?;
            let mut first_byte = [0u8; 1];
            match socket.read(&mut first_byte) {
                Ok(0) => Ok(None),
                Ok(_) => Ok(Some(first_byte[0])),
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

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            let before_seq = connection.ttc_seq_num;

            cx.cancel_fast(asupersync::CancelKind::User);
            let err = connection
                .fetch_rows_request(&cx, 42, 10)
                .await
                .expect_err("fetch continuation must checkpoint before writing");
            // W1-T6.2: an explicit user cancel surfaces the distinct
            // Error::Cancelled, not a generic Error::Runtime(display_string).
            assert!(
                matches!(&err, Error::Cancelled),
                "fetch continuation should stop on the cancellation checkpoint with a distinct cancel error, got {err:?}"
            );
            assert_eq!(err.kind(), ErrorKind::Cancel);
            assert_eq!(
                connection.ttc_seq_num, before_seq,
                "checkpoint must run before allocating the next TTC sequence number"
            );
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready,
                "checkpoint failure must not arm the recovery state machine"
            );
            Ok::<_, Error>(())
        })?;

        let received = server.join().expect("server thread joins")?;
        assert_eq!(
            received, None,
            "cancelled fetch continuation must not write a FETCH packet"
        );
        Ok(())
    }

    #[test]
    fn fetch_rows_ref_drop_mid_read_arms_break_recovery() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let (packet_tx, packet_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || -> std::io::Result<()> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;
            let mut buf = [0u8; 1024];
            let read = socket.read(&mut buf)?;
            packet_tx
                .send(read)
                .expect("test packet notification receiver is alive");
            let _ = release_rx.recv_timeout(Duration::from_secs(2));
            Ok(())
        });

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);

            {
                let mut fetch = pin!(connection.fetch_rows_ref(&cx, 42, 10, None));
                let first_poll = poll_fn(|task_cx| Poll::Ready(fetch.as_mut().poll(task_cx))).await;
                assert!(
                    matches!(first_poll, Poll::Pending),
                    "fetch_rows_ref must wait for the server response"
                );
                let packet_len = packet_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("fetch request should be sent before the response read waits");
                assert!(packet_len > 0, "fetch request packet must not be empty");

                let second_poll =
                    poll_fn(|task_cx| Poll::Ready(fetch.as_mut().poll(task_cx))).await;
                assert!(
                    matches!(second_poll, Poll::Pending),
                    "fetch_rows_ref response read should still be pending"
                );
            }

            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::BreakSent,
                "dropping fetch_rows_ref mid-read must arm break/drain recovery"
            );
            release_tx
                .send(())
                .expect("test server release receiver is alive");
            Ok::<_, Error>(())
        })?;

        server
            .join()
            .expect("server thread joins")
            .map_err(Error::Io)?;
        Ok(())
    }

    #[test]
    fn dropped_cancellable_read_is_drained_before_commit_request() -> Result<()> {
        const STRANDED_BODY: &[u8] = b"stranded response";
        const TRAILING_CANCEL_ERROR: &[u8] = &[0x04, 0x01, 0x0d];

        let commit_packet = encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(0),
            &build_function_payload_with_seq(
                TNS_FUNC_COMMIT,
                1,
                ClientCapabilities::default().ttc_field_version,
            ),
            PacketLengthWidth::Large32,
        )?;
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            use std::io::Write as _;

            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_BREAK,
                "commit must send BREAK before its own request when recovery is pending"
            );
            socket
                .write_all(&data_packet(STRANDED_BODY, true))
                .expect("write stranded response");
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_BREAK))
                .expect("write break-ack marker");
            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_RESET,
                "commit recovery drain must answer the server break marker with RESET"
            );
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_RESET))
                .expect("write reset-confirm marker");
            socket
                .write_all(&data_packet(TRAILING_CANCEL_ERROR, true))
                .expect("write trailing cancel error packet");

            let mut header = [0u8; 8];
            socket
                .read_exact(&mut header)
                .expect("read fresh commit request header");
            let len = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
            let mut commit_request = header.to_vec();
            let mut body = vec![0u8; len - header.len()];
            socket
                .read_exact(&mut body)
                .expect("read fresh commit request body");
            commit_request.extend_from_slice(&body);
            assert_eq!(
                commit_request, commit_packet,
                "fresh COMMIT request must be written after BREAK/RESET drain"
            );
            socket
                .write_all(&data_packet(&[TNS_MSG_TYPE_END_OF_RESPONSE], true))
                .expect("write commit response");
        });

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            {
                let _guard = CancelDrainGuard::arm(Arc::clone(&connection.core.recovery))?;
            }
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::BreakSent
            );
            connection.commit(&cx).await?;
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready
            );
            Ok::<_, Error>(())
        })?;

        server.join().expect("server thread joins");
        Ok(())
    }

    fn assert_borrowed_stream_cursor_retired(
        connection: &Connection,
        cursor_id: u32,
        expected_phase: SessionRecoveryPhase,
    ) {
        assert_eq!(connection.core.recovery.phase(), expected_phase);
        assert!(!connection.in_use_cursors.contains(&cursor_id));
        assert!(!connection.copied_cursors.contains(&cursor_id));
        assert!(
            connection
                .statement_cache
                .iter()
                .all(|entry| entry.cursor_id != cursor_id),
            "failed borrowed stream must evict its cached cursor"
        );
        assert!(!connection.cursor_columns.contains_key(&cursor_id));
        assert!(!connection.lob_prefetch_cursors.contains(&cursor_id));
        assert_eq!(
            connection
                .cursors_to_close
                .iter()
                .filter(|queued| **queued == cursor_id)
                .count(),
            1,
            "failed borrowed stream must queue exactly one cursor close"
        );
    }

    fn expected_next_close_piggyback(connection: &Connection, cursor_id: u32) -> Vec<u8> {
        let mut seq_num = connection.ttc_seq_num;
        let close_seq = next_ttc_sequence(&mut seq_num);
        oracledb_protocol::thin::build_close_cursors_piggyback(
            &[cursor_id],
            close_seq,
            connection.capabilities.ttc_field_version,
        )
    }

    fn serve_borrowed_stream_break_drain(
        socket: &mut std::net::TcpStream,
        stranded_response: &[u8],
    ) {
        use std::io::Write as _;
        const TRAILING_CANCEL_ERROR: &[u8] = &[0x04, 0x01, 0x0d];

        assert_eq!(
            read_marker_type(socket),
            TNS_MARKER_TYPE_BREAK,
            "reuse must BREAK before sending a request or cursor-close piggyback"
        );
        socket
            .write_all(&data_packet(stranded_response, true))
            .expect("write stranded borrowed response");
        socket
            .write_all(&marker_packet(TNS_MARKER_TYPE_BREAK))
            .expect("write break acknowledgement");
        assert_eq!(
            read_marker_type(socket),
            TNS_MARKER_TYPE_RESET,
            "drain must RESET before the reuse request"
        );
        socket
            .write_all(&marker_packet(TNS_MARKER_TYPE_RESET))
            .expect("write reset confirmation");
        socket
            .write_all(&data_packet(TRAILING_CANCEL_ERROR, true))
            .expect("write trailing cancel response");
    }

    #[test]
    fn borrowed_stream_first_page_callback_error_retires_cursor() -> Result<()> {
        const SQL: &str = "select value from borrowed_callback_failure";
        let response = synthetic_pipeline_execute_response_payload();
        let expected = sequential_op_decode(&response);
        let cursor_id = expected.cursor_id;
        assert_ne!(cursor_id, 0, "fixture must open a query cursor");
        assert!(
            !expected.rows.is_empty(),
            "fixture must invoke the first-page callback"
        );

        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<()> {
            use std::io::Write as _;
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;
            let _execute = read_one_wire_packet(&mut socket);
            let response_packet = data_packet(&response, true);
            socket.write_all(&response_packet)?;
            socket.flush()?;

            // The retry must parse a fresh statement after the failed stream
            // evicted its cached cursor. Its request also carries the deferred
            // close piggyback for the abandoned cursor.
            let _retry = read_one_wire_packet(&mut socket);
            socket.write_all(&response_packet)?;
            socket.flush()?;
            Ok(())
        });

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);

            let err = connection
                .for_each_row_ref(&cx, SQL, 2, |_row| {
                    Err(Error::Runtime("stop borrowed callback".to_string()))
                })
                .await
                .expect_err("callback failure must be surfaced");
            assert!(matches!(err, Error::Runtime(message) if message == "stop borrowed callback"));
            assert!(!connection.in_use_cursors.contains(&cursor_id));
            assert!(
                connection
                    .statement_cache
                    .iter()
                    .all(|entry| entry.cursor_id != cursor_id),
                "failed borrowed stream must not leave its cursor reusable"
            );
            assert!(!connection.cursor_columns.contains_key(&cursor_id));
            assert_eq!(
                connection
                    .cursors_to_close
                    .iter()
                    .filter(|queued| **queued == cursor_id)
                    .count(),
                1,
                "failed borrowed stream must queue one cursor close"
            );
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready,
                "a first-page callback error has no stranded response"
            );

            let mut retried_rows = 0usize;
            connection
                .for_each_row_ref(&cx, SQL, 2, |_row| {
                    retried_rows += 1;
                    Ok(())
                })
                .await?;
            assert_eq!(retried_rows, expected.rows.len());
            assert!(
                connection
                    .statement_cache
                    .iter()
                    .any(|entry| entry.cursor_id == cursor_id),
                "a successful retry must return its fresh cursor to the cache"
            );
            assert!(!connection.in_use_cursors.contains(&cursor_id));
            assert!(!connection.copied_cursors.contains(&cursor_id));
            assert!(
                !connection.cursors_to_close.contains(&cursor_id),
                "the retry request must consume the abandoned cursor's close piggyback"
            );
            Ok::<_, Error>(())
        })?;

        server.join().expect("server thread joins")?;
        Ok(())
    }

    #[test]
    fn borrowed_stream_paged_callback_error_drains_before_close_and_reuse() -> Result<()> {
        const SQL: &str = "select value from paged_borrowed_callback_failure";
        const FRESH_SQL: &str = "select 7 + 5 as value from dual";

        let execute_response = synthetic_open_cursor_execute_response_payload();
        let first = sequential_op_decode(&execute_response);
        let cursor_id = first.cursor_id;
        let continuation = synthetic_open_borrowed_fetch_response("13");
        let parsed_continuation = parse_query_response_borrowed_with_limits(
            &continuation,
            ClientCapabilities::default(),
            &first.columns,
            first.rows.last().map(Vec::as_slice),
            ProtocolLimits::DEFAULT,
        )
        .expect("synthetic continuation must decode through the borrowed parser");
        assert_eq!(parsed_continuation.batch.row_count(), 1);
        assert!(parsed_continuation.more_rows);
        drop(parsed_continuation);

        let (close_tx, close_rx) = std::sync::mpsc::channel::<Vec<u8>>();
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let stranded = continuation.clone();
        let server = thread::spawn(move || {
            use std::io::Write as _;
            let (mut socket, _) = listener.accept().expect("accept test client");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");

            let _execute = read_one_wire_data_payload(&mut socket);
            socket
                .write_all(&data_packet(&execute_response, true))
                .expect("write open-cursor execute response");
            let _first_fetch = read_one_wire_data_payload(&mut socket);
            socket
                .write_all(&data_packet(&continuation, true))
                .expect("write first continuation page");

            // `for_each_row_ref` must send this speculative request before it
            // invokes the page callback that fails.
            let _speculative_fetch = read_one_wire_data_payload(&mut socket);
            serve_borrowed_stream_break_drain(&mut socket, &stranded);

            let reuse = read_one_wire_data_payload(&mut socket);
            let expected_close = close_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("client provides expected close piggyback");
            assert!(
                reuse.starts_with(&expected_close),
                "cursor close must be deduplicated and prepended only after BREAK/RESET drain"
            );
            socket
                .write_all(&data_packet(
                    &synthetic_pipeline_execute_response_payload(),
                    true,
                ))
                .expect("write fresh reuse response");
        });

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            let mut rows_seen = 0usize;

            let err = connection
                .for_each_row_ref(&cx, SQL, 2, |_row| {
                    rows_seen += 1;
                    if rows_seen == 2 {
                        Err(Error::Runtime("stop paged callback".into()))
                    } else {
                        Ok(())
                    }
                })
                .await
                .expect_err("paged callback failure must be surfaced");
            assert!(matches!(err, Error::Runtime(message) if message == "stop paged callback"));
            assert_eq!(
                rows_seen, 2,
                "failure must occur in the fetched-page callback, after speculative send"
            );
            assert_borrowed_stream_cursor_retired(
                &connection,
                cursor_id,
                SessionRecoveryPhase::InFlight,
            );
            connection.close_cursor(cursor_id);
            assert_borrowed_stream_cursor_retired(
                &connection,
                cursor_id,
                SessionRecoveryPhase::InFlight,
            );

            close_tx
                .send(expected_next_close_piggyback(&connection, cursor_id))
                .expect("server is waiting for close proof");
            let fresh = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await?;
            assert_eq!(
                fresh,
                sequential_op_decode(&synthetic_pipeline_execute_response_payload()),
                "same connection must decode its own response after drain"
            );
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready
            );
            assert!(!connection.cursors_to_close.contains(&cursor_id));
            connection.release_cursor(fresh.cursor_id);
            Ok::<_, Error>(())
        })?;

        server.join().expect("server thread joins");
        Ok(())
    }

    fn exercise_borrowed_stream_fetch_decode_failure(malformed: Vec<u8>) -> Result<()> {
        const SQL: &str = "select value from malformed_borrowed_fetch";
        const FRESH_SQL: &str = "select 7 + 5 as value from dual";

        let execute_response = synthetic_open_cursor_execute_response_payload();
        let cursor_id = sequential_op_decode(&execute_response).cursor_id;
        let (close_tx, close_rx) = std::sync::mpsc::channel::<Vec<u8>>();
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || {
            use std::io::Write as _;
            let (mut socket, _) = listener.accept().expect("accept test client");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");

            let _execute = read_one_wire_data_payload(&mut socket);
            socket
                .write_all(&data_packet(&execute_response, true))
                .expect("write open-cursor execute response");
            let _fetch = read_one_wire_data_payload(&mut socket);
            socket
                .write_all(&data_packet(&malformed, true))
                .expect("write malformed fetch response");

            // The malformed response was fully framed and consumed, so there
            // must be no BREAK. The very next packet is the close+reuse DATA.
            let reuse = read_one_wire_data_payload(&mut socket);
            let expected_close = close_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("client provides expected close piggyback");
            assert!(reuse.starts_with(&expected_close));
            socket
                .write_all(&data_packet(
                    &synthetic_pipeline_execute_response_payload(),
                    true,
                ))
                .expect("write fresh reuse response");
        });

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            let mut rows_seen = 0usize;

            let err = connection
                .for_each_row_ref(&cx, SQL, 2, |_row| {
                    rows_seen += 1;
                    Ok(())
                })
                .await
                .expect_err("malformed continuation must fail decoding");
            assert!(matches!(err, Error::Protocol(_)));
            assert_eq!(rows_seen, 1, "only the execute page may reach the callback");
            assert_borrowed_stream_cursor_retired(
                &connection,
                cursor_id,
                SessionRecoveryPhase::Ready,
            );
            connection.close_cursor(cursor_id);
            assert_borrowed_stream_cursor_retired(
                &connection,
                cursor_id,
                SessionRecoveryPhase::Ready,
            );

            close_tx
                .send(expected_next_close_piggyback(&connection, cursor_id))
                .expect("server is waiting for close proof");
            let fresh = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await?;
            assert_eq!(
                fresh,
                sequential_op_decode(&synthetic_pipeline_execute_response_payload())
            );
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready
            );
            assert!(!connection.cursors_to_close.contains(&cursor_id));
            connection.release_cursor(fresh.cursor_id);
            Ok::<_, Error>(())
        })?;

        server.join().expect("server thread joins");
        Ok(())
    }

    #[test]
    fn borrowed_stream_malformed_and_truncated_fetches_retire_before_reuse() -> Result<()> {
        exercise_borrowed_stream_fetch_decode_failure(vec![0xff])?;
        exercise_borrowed_stream_fetch_decode_failure(vec![
            oracledb_protocol::thin::TNS_MSG_TYPE_ROW_HEADER,
        ])
    }

    #[test]
    fn borrowed_stream_fetch_eof_retires_cursor() -> Result<()> {
        const SQL: &str = "select value from eof_borrowed_fetch";
        let execute_response = synthetic_open_cursor_execute_response_payload();
        let cursor_id = sequential_op_decode(&execute_response).cursor_id;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || {
            use std::io::Write as _;
            let (mut socket, _) = listener.accept().expect("accept test client");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            let _execute = read_one_wire_data_payload(&mut socket);
            socket
                .write_all(&data_packet(&execute_response, true))
                .expect("write open-cursor execute response");
            let _fetch = read_one_wire_data_payload(&mut socket);
            socket
                .shutdown(std::net::Shutdown::Both)
                .expect("close during fetch response");
        });

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            let err = connection
                .for_each_row_ref(&cx, SQL, 2, |_row| Ok(()))
                .await
                .expect_err("EOF during continuation fetch must fail");
            assert!(matches!(err, Error::Io(_) | Error::ConnectionClosed(_)));
            assert_borrowed_stream_cursor_retired(
                &connection,
                cursor_id,
                SessionRecoveryPhase::BreakSent,
            );
            connection.close_cursor(cursor_id);
            assert_borrowed_stream_cursor_retired(
                &connection,
                cursor_id,
                SessionRecoveryPhase::BreakSent,
            );
            Ok::<_, Error>(())
        })?;

        server.join().expect("server thread joins");
        Ok(())
    }

    #[test]
    fn borrowed_stream_cancel_before_fetch_request_retires_without_wire_write() -> Result<()> {
        const SQL: &str = "select value from pre_request_borrowed_cancel";
        let execute_response = synthetic_open_cursor_execute_response_payload();
        let cursor_id = sequential_op_decode(&execute_response).cursor_id;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<Option<u8>> {
            use std::io::Write as _;
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_millis(500)))?;
            let _execute = read_one_wire_data_payload(&mut socket);
            socket.write_all(&data_packet(&execute_response, true))?;
            socket.flush()?;

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

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            let err = connection
                .for_each_row_ref(&cx, SQL, 2, |_row| {
                    cx.cancel_fast(asupersync::CancelKind::User);
                    Ok(())
                })
                .await
                .expect_err("cancel checkpoint must stop before FETCH write");
            assert!(matches!(err, Error::Cancelled));
            assert_eq!(
                connection.ttc_seq_num, 1,
                "cancel-before-request must not allocate a FETCH sequence"
            );
            assert_borrowed_stream_cursor_retired(
                &connection,
                cursor_id,
                SessionRecoveryPhase::Ready,
            );
            connection.close_cursor(cursor_id);
            assert_borrowed_stream_cursor_retired(
                &connection,
                cursor_id,
                SessionRecoveryPhase::Ready,
            );
            Ok::<_, Error>(())
        })?;

        let received = server.join().expect("server thread joins")?;
        assert_eq!(received, None, "cancel-before-request must write no FETCH");
        Ok(())
    }

    #[test]
    fn borrowed_stream_drop_after_fetch_request_drains_before_close_and_reuse() -> Result<()> {
        const SQL: &str = "select value from post_request_borrowed_cancel";
        const FRESH_SQL: &str = "select 7 + 5 as value from dual";
        let execute_response = synthetic_open_cursor_execute_response_payload();
        let cursor_id = sequential_op_decode(&execute_response).cursor_id;
        let stranded = synthetic_open_borrowed_fetch_response("14");
        let (execute_ready_tx, execute_ready_rx) = std::sync::mpsc::channel();
        let (fetch_seen_tx, fetch_seen_rx) = std::sync::mpsc::channel();
        let (close_tx, close_rx) = std::sync::mpsc::channel::<Vec<u8>>();
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || {
            use std::io::Write as _;
            let (mut socket, _) = listener.accept().expect("accept test client");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            let _execute = read_one_wire_data_payload(&mut socket);
            socket
                .write_all(&data_packet(&execute_response, true))
                .expect("write open-cursor execute response");
            socket.flush().expect("flush execute response");
            execute_ready_tx
                .send(())
                .expect("client waits for execute response");
            let _fetch = read_one_wire_data_payload(&mut socket);
            fetch_seen_tx
                .send(())
                .expect("client waits for FETCH write");

            serve_borrowed_stream_break_drain(&mut socket, &stranded);
            let reuse = read_one_wire_data_payload(&mut socket);
            let expected_close = close_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("client provides expected close piggyback");
            assert!(reuse.starts_with(&expected_close));
            socket
                .write_all(&data_packet(
                    &synthetic_pipeline_execute_response_payload(),
                    true,
                ))
                .expect("write fresh reuse response");
        });

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            let mut rows_seen = 0usize;

            {
                let mut fetch = pin!(connection.for_each_row_ref(&cx, SQL, 2, |_row| {
                    rows_seen += 1;
                    Ok(())
                }));
                let first_poll = poll_fn(|task_cx| Poll::Ready(fetch.as_mut().poll(task_cx))).await;
                assert!(matches!(first_poll, Poll::Pending));
                execute_ready_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("server wrote execute response");

                let deadline = Instant::now() + Duration::from_secs(2);
                loop {
                    match fetch_seen_rx.try_recv() {
                        Ok(()) => break,
                        Err(std::sync::mpsc::TryRecvError::Empty) => {}
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            panic!("server exited before seeing FETCH")
                        }
                    }
                    assert!(Instant::now() < deadline, "FETCH request was never sent");
                    let polled = poll_fn(|task_cx| Poll::Ready(fetch.as_mut().poll(task_cx))).await;
                    assert!(matches!(polled, Poll::Pending));
                    thread::yield_now();
                }
            }

            assert_eq!(
                rows_seen, 1,
                "execute page callback must run before FETCH wait"
            );
            assert_borrowed_stream_cursor_retired(
                &connection,
                cursor_id,
                SessionRecoveryPhase::BreakSent,
            );
            connection.close_cursor(cursor_id);
            assert_borrowed_stream_cursor_retired(
                &connection,
                cursor_id,
                SessionRecoveryPhase::BreakSent,
            );

            close_tx
                .send(expected_next_close_piggyback(&connection, cursor_id))
                .expect("server is waiting for close proof");
            let fresh = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await?;
            assert_eq!(
                fresh,
                sequential_op_decode(&synthetic_pipeline_execute_response_payload())
            );
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready
            );
            assert!(!connection.cursors_to_close.contains(&cursor_id));
            connection.release_cursor(fresh.cursor_id);
            Ok::<_, Error>(())
        })?;

        server.join().expect("server thread joins");
        Ok(())
    }

    #[test]
    fn streaming_cancel_mid_stream_leaves_connection_reusable() -> Result<()> {
        // a4-x3s (rust-oracledb iec3.1.12) offline negative control: cancelling
        // the async row stream mid-flight must leave the connection at a CLEAN
        // wire boundary and fully reusable — no protocol desync, no leaked BREAK,
        // no stranded response bytes bleeding into the next query.
        //
        // The stream is `fetch_rows_ref` (the constant-memory borrowed-batch
        // lending iterator). Dropping its in-flight response read arms the
        // `CancelDrainGuard` (phase -> BreakSent). The NEXT operation runs the
        // shared `ensure_clean_before_request` drain: BREAK, then drain the
        // server's stranded response + break-ack MARKER + RESET handshake +
        // trailing ORA-01013 cancel error, then issue its own request against a
        // clean wire (mirrors python-oracledb `_break_external`/`_reset`,
        // protocol.pyx). Reuse is proven by running a REAL query afterwards and
        // decoding it byte-identically to the reference — not merely a phase.
        const STRANDED_BODY: &[u8] = b"stranded stream response";
        const TRAILING_CANCEL_ERROR: &[u8] = &[0x04, 0x01, 0x0d]; // ORA-01013 user cancel

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            use std::io::Write as _;
            let (mut socket, _) = listener.accept().expect("accept test client");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");

            // 1) The stream's fetch request goes out first.
            let _fetch_request = read_one_wire_packet(&mut socket);

            // 2) The reuse op's cancel drain breaks and drains the stranded call.
            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_BREAK,
                "the reuse op must BREAK the stranded stream before its own request"
            );
            socket
                .write_all(&data_packet(STRANDED_BODY, true))
                .expect("write stranded stream response");
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_BREAK))
                .expect("write break-ack marker");
            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_RESET,
                "the drain must answer the server break marker with RESET"
            );
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_RESET))
                .expect("write reset-confirm marker");
            socket
                .write_all(&data_packet(TRAILING_CANCEL_ERROR, true))
                .expect("write trailing cancel error packet");

            // 3) The reuse query's fresh request lands on a clean wire; answer it.
            let _reuse_request = read_one_wire_packet(&mut socket);
            socket
                .write_all(&data_packet(
                    &synthetic_pipeline_execute_response_payload(),
                    true,
                ))
                .expect("write reuse query response");
            socket.flush().expect("flush responses");
        });

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);

            // Start streaming, drive the request out, then cancel mid-stream by
            // dropping the borrowed-batch fetch future while its response read is
            // still pending.
            {
                let mut fetch = pin!(connection.fetch_rows_ref(&cx, 42, 10, None));
                let first = poll_fn(|task_cx| Poll::Ready(fetch.as_mut().poll(task_cx))).await;
                assert!(
                    matches!(first, Poll::Pending),
                    "the stream must be waiting on the server response when cancelled"
                );
            }
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::BreakSent,
                "cancelling the stream mid-read arms break/drain recovery"
            );

            // Reuse the SAME connection: a real query must decode correctly,
            // proving the wire was drained to a clean boundary.
            let reused = connection
                .execute_raw(
                    &cx,
                    "select value from synthetic_fixture",
                    2,
                    &[],
                    ExecuteOptions::default(),
                    None,
                )
                .await?;
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready,
                "after the drain the connection is back to Ready"
            );
            assert_eq!(
                reused,
                sequential_op_decode(&synthetic_pipeline_execute_response_payload()),
                "the reused connection decodes the follow-up query byte-identically"
            );
            Ok::<_, Error>(())
        })?;

        server.join().expect("server thread joins");
        Ok(())
    }

    #[test]
    fn dropped_define_fetch_mid_read_drains_before_connection_reuse() -> Result<()> {
        const CURSOR_ID: u32 = 42;
        const STRANDED_BODY: &[u8] = b"stranded define-fetch response";
        const TRAILING_CANCEL_ERROR: &[u8] = &[0x04, 0x01, 0x0d];
        const FRESH_SQL: &str = "select value from define_fetch_reuse_fixture";

        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let (define_seen_tx, define_seen_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || -> std::io::Result<bool> {
            use std::io::Write as _;
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(5)))?;

            assert_eq!(
                read_one_wire_packet(&mut socket),
                TNS_PACKET_TYPE_DATA,
                "define-fetch must send a DATA request"
            );
            define_seen_tx
                .send(())
                .expect("client waits for define-fetch request proof");

            let (next_packet_type, next_body) = read_one_wire_packet_bytes(&mut socket);
            if next_packet_type == TNS_PACKET_TYPE_MARKER {
                assert_eq!(
                    next_body,
                    vec![1, 0, TNS_MARKER_TYPE_BREAK],
                    "reuse must BREAK the stranded define-fetch before its request"
                );
                socket.write_all(&data_packet(STRANDED_BODY, true))?;
                socket.write_all(&marker_packet(TNS_MARKER_TYPE_BREAK))?;
                assert_eq!(
                    read_marker_type(&mut socket),
                    TNS_MARKER_TYPE_RESET,
                    "define-fetch drain must complete the RESET handshake"
                );
                socket.write_all(&marker_packet(TNS_MARKER_TYPE_RESET))?;
                socket.write_all(&data_packet(TRAILING_CANCEL_ERROR, true))?;

                assert_eq!(
                    read_one_wire_packet(&mut socket),
                    TNS_PACKET_TYPE_DATA,
                    "fresh execute follows the completed drain"
                );
                socket.write_all(&data_packet(
                    &synthetic_pipeline_execute_response_payload(),
                    true,
                ))?;
                socket.flush()?;
                Ok(true)
            } else {
                assert_eq!(
                    next_packet_type, TNS_PACKET_TYPE_DATA,
                    "without recovery the next operation is sent directly"
                );
                // Reproduce stale-wire poisoning: the abandoned define-fetch
                // response arrives before the fresh execute response.
                socket.write_all(&data_packet(STRANDED_BODY, true))?;
                socket.write_all(&data_packet(
                    &synthetic_pipeline_execute_response_payload(),
                    true,
                ))?;
                socket.flush()?;
                Ok(false)
            }
        });

        let runtime = build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            let define_columns = vec![ColumnMetadata::new(
                "DOC",
                oracledb_protocol::thin::ORA_TYPE_NUM_JSON,
            )];

            {
                let mut define = pin!(connection.define_and_fetch_rows_with_columns(
                    &cx,
                    CURSOR_ID,
                    10,
                    &define_columns,
                    None,
                ));
                let first = poll_fn(|task_cx| Poll::Ready(define.as_mut().poll(task_cx))).await;
                assert!(
                    matches!(first, Poll::Pending),
                    "define-fetch must be waiting for its response before drop"
                );
                define_seen_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("server observed define-fetch request");
            }

            let reused = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await;
            Ok::<_, Error>((reused, connection.core.recovery.phase()))
        });

        let recovered = server.join().expect("define-fetch server joins")?;
        let (reused, phase) = outcome?;
        let reused = reused.expect("fresh execute must not decode the stranded define-fetch");
        assert_eq!(
            reused,
            sequential_op_decode(&synthetic_pipeline_execute_response_payload()),
            "reuse must decode its own response byte-identically"
        );
        assert!(recovered, "reuse must take the BREAK/drain branch");
        assert_eq!(phase, SessionRecoveryPhase::Ready);
        Ok(())
    }

    #[test]
    fn dropped_commit_mid_read_drains_before_connection_reuse() -> Result<()> {
        const STRANDED_BODY: &[u8] = &[TNS_MSG_TYPE_END_OF_RESPONSE];
        const TRAILING_CANCEL_ERROR: &[u8] = &[0x04, 0x01, 0x0d];
        const FRESH_SQL: &str = "select value from commit_reuse_fixture";

        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let (commit_seen_tx, commit_seen_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || -> std::io::Result<bool> {
            use std::io::Write as _;
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(5)))?;

            assert_eq!(
                read_one_wire_packet(&mut socket),
                TNS_PACKET_TYPE_DATA,
                "commit must send a DATA request"
            );
            commit_seen_tx
                .send(())
                .expect("client waits for commit request proof");

            let (next_packet_type, next_body) = read_one_wire_packet_bytes(&mut socket);
            if next_packet_type == TNS_PACKET_TYPE_MARKER {
                assert_eq!(
                    next_body,
                    vec![1, 0, TNS_MARKER_TYPE_BREAK],
                    "reuse must BREAK the stranded commit before its request"
                );
                socket.write_all(&data_packet(STRANDED_BODY, true))?;
                socket.write_all(&marker_packet(TNS_MARKER_TYPE_BREAK))?;
                assert_eq!(
                    read_marker_type(&mut socket),
                    TNS_MARKER_TYPE_RESET,
                    "commit drain must complete the RESET handshake"
                );
                socket.write_all(&marker_packet(TNS_MARKER_TYPE_RESET))?;
                socket.write_all(&data_packet(TRAILING_CANCEL_ERROR, true))?;

                assert_eq!(
                    read_one_wire_packet(&mut socket),
                    TNS_PACKET_TYPE_DATA,
                    "fresh execute follows the completed drain"
                );
                socket.write_all(&data_packet(
                    &synthetic_pipeline_execute_response_payload(),
                    true,
                ))?;
                socket.flush()?;
                Ok(true)
            } else {
                assert_eq!(
                    next_packet_type, TNS_PACKET_TYPE_DATA,
                    "without recovery the next operation is sent directly"
                );
                // Negative control for the unfixed path: the abandoned commit
                // response arrives before the fresh execute response.
                socket.write_all(&data_packet(STRANDED_BODY, true))?;
                socket.write_all(&data_packet(
                    &synthetic_pipeline_execute_response_payload(),
                    true,
                ))?;
                socket.flush()?;
                Ok(false)
            }
        });

        let runtime = build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);

            {
                let mut commit = pin!(connection.commit(&cx));
                let first = poll_fn(|task_cx| Poll::Ready(commit.as_mut().poll(task_cx))).await;
                assert!(
                    matches!(first, Poll::Pending),
                    "commit must be waiting for its response before drop"
                );
                commit_seen_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("server observed commit request");
            }

            let phase_after_drop = connection.core.recovery.phase();
            let reused = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await;
            Ok::<_, Error>((phase_after_drop, reused, connection.core.recovery.phase()))
        });

        let recovered = server.join().expect("commit server joins")?;
        let (phase_after_drop, reused, final_phase) = outcome?;
        let reused = reused.expect("fresh execute must not decode the stranded commit");
        assert_eq!(
            reused,
            sequential_op_decode(&synthetic_pipeline_execute_response_payload()),
            "reuse must decode its own response byte-identically"
        );
        assert!(recovered, "reuse must take the BREAK/drain branch");
        assert_eq!(phase_after_drop, SessionRecoveryPhase::BreakSent);
        assert_eq!(final_phase, SessionRecoveryPhase::Ready);
        Ok(())
    }

    #[test]
    fn successful_commit_disarms_recovery_before_reuse() -> Result<()> {
        const FRESH_SQL: &str = "select value from commit_success_fixture";
        let execute_response = synthetic_pipeline_execute_response_payload();
        let server_execute_response = execute_response.clone();
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<()> {
            use std::io::Write as _;
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;

            assert_eq!(
                read_one_wire_packet(&mut socket),
                TNS_PACKET_TYPE_DATA,
                "commit must send a DATA request"
            );
            socket.write_all(&data_packet(&[TNS_MSG_TYPE_END_OF_RESPONSE], true))?;
            socket.flush()?;

            assert_eq!(
                read_one_wire_packet(&mut socket),
                TNS_PACKET_TYPE_DATA,
                "a successful commit must not cause a spurious BREAK on reuse"
            );
            socket.write_all(&data_packet(&server_execute_response, true))?;
            socket.flush()
        });

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);

            connection.commit(&cx).await?;
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready,
                "a completed response must disarm the plain-function guard"
            );

            let reused = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await?;
            assert_eq!(reused, sequential_op_decode(&execute_response));
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready
            );
            Ok::<_, Error>(())
        })?;

        server.join().expect("successful commit server joins")?;
        Ok(())
    }

    #[test]
    fn precancelled_commit_writes_nothing_and_keeps_wire_ready() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<bool> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_millis(300)))?;
            let mut byte = [0u8; 1];
            match socket.read(&mut byte) {
                Ok(0) => Ok(false),
                Ok(_) => Ok(true),
                Err(err)
                    if matches!(
                        err.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    Ok(false)
                }
                Err(err) => Err(err),
            }
        });

        let runtime = build_io_runtime()?;
        let (phase, sequence_before, sequence_after) = runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            let sequence_before = connection.ttc_seq_num;
            cx.cancel_fast(CancelKind::User);

            let err = connection
                .commit(&cx)
                .await
                .expect_err("pending cancellation stops before COMMIT");
            assert!(matches!(err, Error::Cancelled), "{err:?}");
            Ok::<_, Error>((
                connection.core.recovery.phase(),
                sequence_before,
                connection.ttc_seq_num,
            ))
        })?;

        assert!(
            !server.join().expect("pre-cancel commit server joins")?,
            "pre-cancelled COMMIT must not write any wire bytes"
        );
        assert_eq!(phase, SessionRecoveryPhase::Ready);
        assert_eq!(sequence_after, sequence_before);
        Ok(())
    }

    #[test]
    fn dropped_change_password_mid_response_drains_before_connection_reuse() -> Result<()> {
        const STRANDED_BODY: &[u8] = &[TNS_MSG_TYPE_END_OF_RESPONSE];
        const TRAILING_CANCEL_ERROR: &[u8] = &[0x04, 0x01, 0x0d];
        const FRESH_SQL: &str = "select value from change_password_reuse_fixture";
        const OLD_PASSWORD: &str = "old-change-password-proof";
        const NEW_PASSWORD: &str = "new-change-password-proof";

        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let (change_seen_tx, change_seen_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || -> std::io::Result<bool> {
            use std::io::Write as _;
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(5)))?;

            let (packet_type, body) = read_one_wire_packet_bytes(&mut socket);
            assert_eq!(
                packet_type, TNS_PACKET_TYPE_DATA,
                "password change must send a DATA request"
            );
            assert!(
                !body
                    .windows(OLD_PASSWORD.len())
                    .any(|window| window == OLD_PASSWORD.as_bytes()),
                "password-change request must not expose the old password"
            );
            assert!(
                !body
                    .windows(NEW_PASSWORD.len())
                    .any(|window| window == NEW_PASSWORD.as_bytes()),
                "password-change request must not expose the new password"
            );
            change_seen_tx
                .send(())
                .expect("client waits for password-change request proof");

            let (next_packet_type, next_body) = read_one_wire_packet_bytes(&mut socket);
            if next_packet_type == TNS_PACKET_TYPE_MARKER {
                assert_eq!(
                    next_body,
                    vec![1, 0, TNS_MARKER_TYPE_BREAK],
                    "reuse must BREAK the stranded password change before its request"
                );
                socket.write_all(&data_packet(STRANDED_BODY, true))?;
                socket.write_all(&marker_packet(TNS_MARKER_TYPE_BREAK))?;
                assert_eq!(
                    read_marker_type(&mut socket),
                    TNS_MARKER_TYPE_RESET,
                    "password-change drain must complete the RESET handshake"
                );
                socket.write_all(&marker_packet(TNS_MARKER_TYPE_RESET))?;
                socket.write_all(&data_packet(TRAILING_CANCEL_ERROR, true))?;

                assert_eq!(
                    read_one_wire_packet(&mut socket),
                    TNS_PACKET_TYPE_DATA,
                    "fresh execute follows the completed drain"
                );
                socket.write_all(&data_packet(
                    &synthetic_pipeline_execute_response_payload(),
                    true,
                ))?;
                socket.flush()?;
                Ok(true)
            } else {
                assert_eq!(
                    next_packet_type, TNS_PACKET_TYPE_DATA,
                    "without recovery the next operation is sent directly"
                );
                socket.write_all(&data_packet(STRANDED_BODY, true))?;
                socket.write_all(&data_packet(
                    &synthetic_pipeline_execute_response_payload(),
                    true,
                ))?;
                socket.flush()?;
                Ok(false)
            }
        });

        let runtime = build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            connection.combo_key = vec![0x11; 32];

            {
                let mut change_password =
                    pin!(connection.change_password(&cx, OLD_PASSWORD, NEW_PASSWORD,));
                let first =
                    poll_fn(|task_cx| Poll::Ready(change_password.as_mut().poll(task_cx))).await;
                assert!(
                    matches!(first, Poll::Pending),
                    "password change must be waiting for its response before drop"
                );
                change_seen_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("server observed password-change request");
            }

            let phase_after_drop = connection.core.recovery.phase();
            let reused = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await;
            Ok::<_, Error>((phase_after_drop, reused, connection.core.recovery.phase()))
        });

        let recovered = server.join().expect("password-change server joins")?;
        let (phase_after_drop, reused, final_phase) = outcome?;
        let reused = reused.expect("fresh execute must not decode the stranded password change");
        assert_eq!(
            reused,
            sequential_op_decode(&synthetic_pipeline_execute_response_payload()),
            "reuse must decode its own response byte-identically"
        );
        assert!(recovered, "reuse must take the BREAK/drain branch");
        assert_eq!(phase_after_drop, SessionRecoveryPhase::BreakSent);
        assert_eq!(final_phase, SessionRecoveryPhase::Ready);
        Ok(())
    }

    #[test]
    fn successful_change_password_disarms_recovery_before_reuse() -> Result<()> {
        const FRESH_SQL: &str = "select value from change_password_success_fixture";
        let execute_response = synthetic_pipeline_execute_response_payload();
        let server_execute_response = execute_response.clone();
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<()> {
            use std::io::Write as _;
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;

            assert_eq!(
                read_one_wire_packet(&mut socket),
                TNS_PACKET_TYPE_DATA,
                "password change must send a DATA request"
            );
            socket.write_all(&data_packet(&[TNS_MSG_TYPE_END_OF_RESPONSE], true))?;
            socket.flush()?;

            assert_eq!(
                read_one_wire_packet(&mut socket),
                TNS_PACKET_TYPE_DATA,
                "a successful password change must not cause a spurious BREAK on reuse"
            );
            socket.write_all(&data_packet(&server_execute_response, true))?;
            socket.flush()
        });

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            connection.combo_key = vec![0x11; 32];

            connection
                .change_password(
                    &cx,
                    "old-change-password-proof",
                    "new-change-password-proof",
                )
                .await?;
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready,
                "a completed password-change response must disarm its guard"
            );

            let reused = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await?;
            assert_eq!(reused, sequential_op_decode(&execute_response));
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready
            );
            Ok::<_, Error>(())
        })?;

        server
            .join()
            .expect("successful password-change server joins")?;
        Ok(())
    }

    #[test]
    fn precancelled_change_password_writes_nothing_and_keeps_wire_ready() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<bool> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_millis(300)))?;
            let mut byte = [0u8; 1];
            match socket.read(&mut byte) {
                Ok(0) => Ok(false),
                Ok(_) => Ok(true),
                Err(err)
                    if matches!(
                        err.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    Ok(false)
                }
                Err(err) => Err(err),
            }
        });

        let runtime = build_io_runtime()?;
        let (phase, sequence_before, sequence_after) = runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            let sequence_before = connection.ttc_seq_num;
            cx.cancel_fast(CancelKind::User);

            let err = connection
                .change_password(
                    &cx,
                    "old-change-password-proof",
                    "new-change-password-proof",
                )
                .await
                .expect_err("pending cancellation stops before PASSWORD CHANGE");
            assert!(matches!(err, Error::Cancelled), "{err:?}");
            Ok::<_, Error>((
                connection.core.recovery.phase(),
                sequence_before,
                connection.ttc_seq_num,
            ))
        })?;

        assert!(
            !server
                .join()
                .expect("pre-cancel password-change server joins")?,
            "pre-cancelled PASSWORD CHANGE must not write any wire bytes"
        );
        assert_eq!(phase, SessionRecoveryPhase::Ready);
        assert_eq!(sequence_after, sequence_before);
        Ok(())
    }

    #[test]
    fn dropped_subscribe_register_mid_response_drains_before_connection_reuse() -> Result<()> {
        const FRESH_SQL: &str = "select value from subscribe_register_reuse_fixture";

        let expected_request = build_subscribe_payload_with_seq(
            1,
            TNS_SUBSCR_OP_REGISTER,
            Some("test_user"),
            None,
            oracledb_protocol::thin::TNS_SUBSCR_NAMESPACE_DBCHANGE,
            None,
            oracledb_protocol::thin::SUBSCR_QOS_ROWIDS,
            0,
            10,
            0,
            0,
            0,
            0,
            ClientCapabilities::default().ttc_field_version,
        )?;
        let stranded_response = synthetic_subscribe_register_response_payload();
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let (request_seen_tx, request_seen_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            serve_dropped_response_recovery(
                listener,
                expected_request,
                stranded_response,
                request_seen_tx,
            )
        });

        let runtime = build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);

            {
                let mut subscribe = pin!(connection.subscribe_register(
                    &cx,
                    oracledb_protocol::thin::TNS_SUBSCR_NAMESPACE_DBCHANGE,
                    None,
                    oracledb_protocol::thin::SUBSCR_QOS_ROWIDS,
                    0,
                    10,
                    0,
                    0,
                    0,
                ));
                let first = poll_fn(|task_cx| Poll::Ready(subscribe.as_mut().poll(task_cx))).await;
                assert!(
                    matches!(first, Poll::Pending),
                    "CQN register must be waiting for its response before drop"
                );
                request_seen_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("server observed CQN register request");
            }

            let phase_after_drop = connection.core.recovery.phase();
            let reused = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await;
            Ok::<_, Error>((phase_after_drop, reused, connection.core.recovery.phase()))
        });

        let recovered = server.join().expect("CQN register server joins")?;
        let (phase_after_drop, reused, final_phase) = outcome?;
        let reused = reused.expect("fresh execute must not decode the stranded CQN register");
        assert_eq!(
            reused,
            sequential_op_decode(&synthetic_pipeline_execute_response_payload()),
            "reuse must decode its own response byte-identically"
        );
        assert!(recovered, "reuse must take the BREAK/drain branch");
        assert_eq!(phase_after_drop, SessionRecoveryPhase::BreakSent);
        assert_eq!(final_phase, SessionRecoveryPhase::Ready);
        Ok(())
    }

    #[test]
    fn dropped_subscribe_unregister_mid_response_drains_before_connection_reuse() -> Result<()> {
        const FRESH_SQL: &str = "select value from subscribe_unregister_reuse_fixture";
        const CLIENT_ID: &[u8] = b"OCI:EP:301";

        let expected_request = build_subscribe_payload_with_seq(
            1,
            TNS_SUBSCR_OP_UNREGISTER,
            Some("test_user"),
            Some(CLIENT_ID),
            oracledb_protocol::thin::TNS_SUBSCR_NAMESPACE_DBCHANGE,
            None,
            oracledb_protocol::thin::SUBSCR_QOS_ROWIDS,
            0,
            10,
            0,
            0,
            0,
            302,
            ClientCapabilities::default().ttc_field_version,
        )?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let (request_seen_tx, request_seen_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            serve_dropped_response_recovery(
                listener,
                expected_request,
                vec![TNS_MSG_TYPE_END_OF_RESPONSE],
                request_seen_tx,
            )
        });

        let runtime = build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);

            {
                let mut unsubscribe = pin!(connection.subscribe_unregister(
                    &cx,
                    302,
                    CLIENT_ID,
                    oracledb_protocol::thin::TNS_SUBSCR_NAMESPACE_DBCHANGE,
                    None,
                    oracledb_protocol::thin::SUBSCR_QOS_ROWIDS,
                    0,
                    10,
                    0,
                    0,
                    0,
                ));
                let first =
                    poll_fn(|task_cx| Poll::Ready(unsubscribe.as_mut().poll(task_cx))).await;
                assert!(
                    matches!(first, Poll::Pending),
                    "CQN unregister must be waiting for its response before drop"
                );
                request_seen_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("server observed CQN unregister request");
            }

            let phase_after_drop = connection.core.recovery.phase();
            let reused = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await;
            Ok::<_, Error>((phase_after_drop, reused, connection.core.recovery.phase()))
        });

        let recovered = server.join().expect("CQN unregister server joins")?;
        let (phase_after_drop, reused, final_phase) = outcome?;
        let reused = reused.expect("fresh execute must not decode the stranded CQN unregister");
        assert_eq!(
            reused,
            sequential_op_decode(&synthetic_pipeline_execute_response_payload()),
            "reuse must decode its own response byte-identically"
        );
        assert!(recovered, "reuse must take the BREAK/drain branch");
        assert_eq!(phase_after_drop, SessionRecoveryPhase::BreakSent);
        assert_eq!(final_phase, SessionRecoveryPhase::Ready);
        Ok(())
    }

    #[test]
    fn successful_subscribe_round_trips_disarm_recovery_before_reuse() -> Result<()> {
        const CLIENT_ID: &[u8] = b"OCI:EP:301";
        const FRESH_SQL: &str = "select value from subscribe_success_fixture";

        let register_request = build_subscribe_payload_with_seq(
            1,
            TNS_SUBSCR_OP_REGISTER,
            Some("test_user"),
            None,
            oracledb_protocol::thin::TNS_SUBSCR_NAMESPACE_DBCHANGE,
            None,
            oracledb_protocol::thin::SUBSCR_QOS_ROWIDS,
            0,
            10,
            0,
            0,
            0,
            0,
            ClientCapabilities::default().ttc_field_version,
        )?;
        let unregister_request = build_subscribe_payload_with_seq(
            2,
            TNS_SUBSCR_OP_UNREGISTER,
            Some("test_user"),
            Some(CLIENT_ID),
            oracledb_protocol::thin::TNS_SUBSCR_NAMESPACE_DBCHANGE,
            None,
            oracledb_protocol::thin::SUBSCR_QOS_ROWIDS,
            0,
            10,
            0,
            0,
            0,
            302,
            ClientCapabilities::default().ttc_field_version,
        )?;
        let register_response = synthetic_subscribe_register_response_payload();
        let execute_response = synthetic_pipeline_execute_response_payload();
        let server_execute_response = execute_response.clone();
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<()> {
            use std::io::Write as _;
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;

            assert_eq!(
                read_one_wire_data_payload(&mut socket),
                register_request,
                "successful CQN register request remains byte-identical"
            );
            socket.write_all(&data_packet(&register_response, true))?;
            socket.flush()?;

            assert_eq!(
                read_one_wire_data_payload(&mut socket),
                unregister_request,
                "successful CQN unregister must follow directly without a spurious BREAK"
            );
            socket.write_all(&data_packet(&[TNS_MSG_TYPE_END_OF_RESPONSE], true))?;
            socket.flush()?;

            assert_eq!(
                read_one_wire_packet(&mut socket),
                TNS_PACKET_TYPE_DATA,
                "fresh execute must follow both completed CQN responses directly"
            );
            socket.write_all(&data_packet(&server_execute_response, true))?;
            socket.flush()
        });

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            connection.supports_end_of_response = false;

            let registered = connection
                .subscribe_register(
                    &cx,
                    oracledb_protocol::thin::TNS_SUBSCR_NAMESPACE_DBCHANGE,
                    None,
                    oracledb_protocol::thin::SUBSCR_QOS_ROWIDS,
                    0,
                    10,
                    0,
                    0,
                    0,
                )
                .await?;
            assert_eq!(registered.registration_id, 302);
            assert_eq!(registered.client_id.as_deref(), Some(CLIENT_ID));
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready,
                "completed classic CQN register response must disarm its guard"
            );

            connection
                .subscribe_unregister(
                    &cx,
                    registered.registration_id,
                    CLIENT_ID,
                    oracledb_protocol::thin::TNS_SUBSCR_NAMESPACE_DBCHANGE,
                    None,
                    oracledb_protocol::thin::SUBSCR_QOS_ROWIDS,
                    0,
                    10,
                    0,
                    0,
                    0,
                )
                .await?;
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready,
                "completed classic CQN unregister response must disarm its guard"
            );

            let reused = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await?;
            assert_eq!(reused, sequential_op_decode(&execute_response));
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready
            );
            Ok::<_, Error>(())
        })?;

        server.join().expect("successful CQN server joins")?;
        Ok(())
    }

    #[test]
    fn dropped_sessionless_begin_mid_response_drains_before_connection_reuse() -> Result<()> {
        const FRESH_SQL: &str = "select value from sessionless_begin_reuse_fixture";
        const TRANSACTION_ID: &[u8] = b"qa-sessionless-begin";

        let expected_request = build_tpc_txn_switch_payload_with_seq(
            1,
            0,
            TNS_TPC_TXN_START,
            TPC_TXN_FLAGS_NEW | TPC_TXN_FLAGS_SESSIONLESS,
            30,
            Some(TRANSACTION_ID),
        );
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let (request_seen_tx, request_seen_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            serve_dropped_response_recovery(
                listener,
                expected_request,
                vec![TNS_MSG_TYPE_END_OF_RESPONSE],
                request_seen_tx,
            )
        });

        let runtime = build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);

            {
                let mut begin =
                    pin!(connection.begin_sessionless_transaction(&cx, TRANSACTION_ID, 30, false,));
                let first = poll_fn(|task_cx| Poll::Ready(begin.as_mut().poll(task_cx))).await;
                assert!(
                    matches!(first, Poll::Pending),
                    "sessionless begin must be waiting for its response before drop"
                );
                request_seen_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("server observed sessionless begin request");
            }

            let phase_after_drop = connection.core.recovery.phase();
            let reused = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await;
            Ok::<_, Error>((phase_after_drop, reused, connection.core.recovery.phase()))
        });

        let recovered = server.join().expect("sessionless begin server joins")?;
        let (phase_after_drop, reused, final_phase) = outcome?;
        let reused =
            reused.expect("fresh execute must not decode the stranded sessionless response");
        assert_eq!(
            reused,
            // ubs:ignore — decodes an Oracle TTC test fixture, not a JWT.
            sequential_op_decode(&synthetic_pipeline_execute_response_payload()),
            "reuse must decode its own response byte-identically"
        );
        assert!(recovered, "reuse must take the BREAK/drain branch");
        assert_eq!(phase_after_drop, SessionRecoveryPhase::BreakSent);
        assert_eq!(final_phase, SessionRecoveryPhase::Ready);
        Ok(())
    }

    #[test]
    fn dropped_sessionless_suspend_mid_response_drains_before_connection_reuse() -> Result<()> {
        const FRESH_SQL: &str = "select value from sessionless_suspend_reuse_fixture";
        const TRANSACTION_ID: &[u8] = b"qa-sessionless-suspend";

        let expected_request = build_tpc_txn_switch_payload_with_seq(
            1,
            0,
            TNS_TPC_TXN_DETACH,
            TPC_TXN_FLAGS_SESSIONLESS,
            0,
            None,
        );
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let (request_seen_tx, request_seen_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            serve_dropped_response_recovery(
                listener,
                expected_request,
                vec![TNS_MSG_TYPE_END_OF_RESPONSE],
                request_seen_tx,
            )
        });

        let runtime = build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            connection.sessionless_data = Some(SessionlessData {
                transaction_id: TRANSACTION_ID.to_vec(),
                timeout: 30,
                operation: TNS_TPC_TXN_START,
                flags: TPC_TXN_FLAGS_NEW,
                piggyback_pending: false,
                started_on_server: false,
            });

            {
                let mut suspend = pin!(connection.suspend_sessionless_transaction(&cx));
                let first = poll_fn(|task_cx| Poll::Ready(suspend.as_mut().poll(task_cx))).await;
                assert!(
                    matches!(first, Poll::Pending),
                    "sessionless suspend must be waiting for its response before drop"
                );
                request_seen_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("server observed sessionless suspend request");
            }

            let phase_after_drop = connection.core.recovery.phase();
            let reused = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await;
            Ok::<_, Error>((phase_after_drop, reused, connection.core.recovery.phase()))
        });

        let recovered = server.join().expect("sessionless suspend server joins")?;
        let (phase_after_drop, reused, final_phase) = outcome?;
        let reused =
            reused.expect("fresh execute must not decode the stranded sessionless response");
        assert_eq!(
            reused,
            // ubs:ignore — decodes an Oracle TTC test fixture, not a JWT.
            sequential_op_decode(&synthetic_pipeline_execute_response_payload()),
            "reuse must decode its own response byte-identically"
        );
        assert!(recovered, "reuse must take the BREAK/drain branch");
        assert_eq!(phase_after_drop, SessionRecoveryPhase::BreakSent);
        assert_eq!(final_phase, SessionRecoveryPhase::Ready);
        Ok(())
    }

    #[test]
    fn dropped_tpc_begin_mid_response_drains_before_connection_reuse() -> Result<()> {
        const FRESH_SQL: &str = "select value from tpc_begin_reuse_fixture";
        const GTID: &[u8] = b"qa-tpc-begin";
        const BQUAL: &[u8] = b"branch";

        let xid = TpcXid {
            format_id: 4400,
            global_transaction_id: GTID,
            branch_qualifier: BQUAL,
        };
        let expected_request = build_tpc_switch_payload_with_seq_and_version(
            1,
            TNS_TPC_TXN_START,
            TPC_TXN_FLAGS_NEW,
            0,
            Some(&xid),
            None,
            ClientCapabilities::default().ttc_field_version,
        );
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let (request_seen_tx, request_seen_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            serve_dropped_response_recovery(
                listener,
                expected_request,
                vec![TNS_MSG_TYPE_END_OF_RESPONSE],
                request_seen_tx,
            )
        });

        let runtime = build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);

            {
                let mut begin =
                    pin!(connection.tpc_begin(&cx, 4400, GTID, BQUAL, TPC_TXN_FLAGS_NEW, 0,));
                let first = poll_fn(|task_cx| Poll::Ready(begin.as_mut().poll(task_cx))).await;
                assert!(
                    matches!(first, Poll::Pending),
                    "TPC begin must be waiting for its response before drop"
                );
                request_seen_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("server observed TPC begin request");
            }

            let phase_after_drop = connection.core.recovery.phase();
            let reused = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await;
            Ok::<_, Error>((phase_after_drop, reused, connection.core.recovery.phase()))
        });

        let recovered = server.join().expect("TPC begin server joins")?;
        let (phase_after_drop, reused, final_phase) = outcome?;
        let reused =
            reused.expect("fresh execute must not decode the stranded TPC switch response");
        assert_eq!(
            reused,
            // ubs:ignore — decodes an Oracle TTC test fixture, not a JWT.
            sequential_op_decode(&synthetic_pipeline_execute_response_payload()),
            "reuse must decode its own response byte-identically"
        );
        assert!(recovered, "reuse must take the BREAK/drain branch");
        assert_eq!(phase_after_drop, SessionRecoveryPhase::BreakSent);
        assert_eq!(final_phase, SessionRecoveryPhase::Ready);
        Ok(())
    }

    #[test]
    fn dropped_tpc_commit_mid_response_drains_before_connection_reuse() -> Result<()> {
        const FRESH_SQL: &str = "select value from tpc_commit_reuse_fixture";
        const GTID: &[u8] = b"qa-tpc-commit";
        const BQUAL: &[u8] = b"branch";

        let xid = TpcXid {
            format_id: 4400,
            global_transaction_id: GTID,
            branch_qualifier: BQUAL,
        };
        let expected_request = build_tpc_change_state_payload_with_seq_and_version(
            1,
            TNS_TPC_TXN_COMMIT,
            TNS_TPC_TXN_STATE_READ_ONLY,
            0,
            Some(&xid),
            None,
            ClientCapabilities::default().ttc_field_version,
        );
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let (request_seen_tx, request_seen_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            serve_dropped_response_recovery(
                listener,
                expected_request,
                vec![TNS_MSG_TYPE_END_OF_RESPONSE],
                request_seen_tx,
            )
        });

        let runtime = build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);

            {
                let mut commit = pin!(connection.tpc_commit(&cx, Some((4400, GTID, BQUAL)), true,));
                let first = poll_fn(|task_cx| Poll::Ready(commit.as_mut().poll(task_cx))).await;
                assert!(
                    matches!(first, Poll::Pending),
                    "TPC commit must be waiting for its response before drop"
                );
                request_seen_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("server observed TPC commit request");
            }

            let phase_after_drop = connection.core.recovery.phase();
            let reused = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await;
            Ok::<_, Error>((phase_after_drop, reused, connection.core.recovery.phase()))
        });

        let recovered = server.join().expect("TPC commit server joins")?;
        let (phase_after_drop, reused, final_phase) = outcome?;
        let reused = reused.expect("fresh execute must not decode the stranded TPC response");
        assert_eq!(
            reused,
            // ubs:ignore — decodes an Oracle TTC test fixture, not a JWT.
            sequential_op_decode(&synthetic_pipeline_execute_response_payload()),
            "reuse must decode its own response byte-identically"
        );
        assert!(recovered, "reuse must take the BREAK/drain branch");
        assert_eq!(phase_after_drop, SessionRecoveryPhase::BreakSent);
        assert_eq!(final_phase, SessionRecoveryPhase::Ready);
        Ok(())
    }

    #[test]
    fn successful_tpc_round_trips_disarm_recovery() -> Result<()> {
        const GTID: &[u8] = b"qa-tpc-success";
        const BQUAL: &[u8] = b"branch";

        let xid = TpcXid {
            format_id: 4400,
            global_transaction_id: GTID,
            branch_qualifier: BQUAL,
        };
        let expected_begin = build_tpc_switch_payload_with_seq_and_version(
            1,
            TNS_TPC_TXN_START,
            TPC_TXN_FLAGS_NEW,
            0,
            Some(&xid),
            None,
            ClientCapabilities::default().ttc_field_version,
        );
        let expected_commit = build_tpc_change_state_payload_with_seq_and_version(
            2,
            TNS_TPC_TXN_COMMIT,
            TNS_TPC_TXN_STATE_READ_ONLY,
            0,
            Some(&xid),
            Some(&[]),
            ClientCapabilities::default().ttc_field_version,
        );
        let mut commit_response = oracledb_protocol::wire::TtcWriter::new();
        commit_response.write_u8(oracledb_protocol::thin::TNS_MSG_TYPE_PARAMETER);
        commit_response.write_ub4(TNS_TPC_TXN_STATE_READ_ONLY);
        commit_response.write_u8(TNS_MSG_TYPE_END_OF_RESPONSE);
        let commit_response = commit_response.into_bytes();

        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<()> {
            use std::io::Write as _;
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;

            assert_eq!(read_one_wire_data_payload(&mut socket), expected_begin);
            socket.write_all(&data_packet(&[TNS_MSG_TYPE_END_OF_RESPONSE], true))?;
            socket.flush()?;

            assert_eq!(read_one_wire_data_payload(&mut socket), expected_commit);
            socket.write_all(&data_packet(&commit_response, true))?;
            socket.flush()
        });

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);

            connection
                .tpc_begin(&cx, 4400, GTID, BQUAL, TPC_TXN_FLAGS_NEW, 0)
                .await?;
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready
            );
            connection
                .tpc_commit(&cx, Some((4400, GTID, BQUAL)), true)
                .await?;
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready
            );
            Ok::<_, Error>(())
        })?;

        server.join().expect("successful TPC server joins")?;
        Ok(())
    }

    #[test]
    fn dropped_direct_path_prepare_mid_response_drains_before_connection_reuse() -> Result<()> {
        const FRESH_SQL: &str = "select value from dpl_prepare_reuse_fixture";
        const CURSOR_ID: u16 = 73;

        let column_names = Vec::<String>::new();
        let expected_request =
            oracledb_protocol::dpl::build_direct_path_prepare_payload_with_version(
                "QA",
                "DPL_DROP",
                &column_names,
                1,
                ClientCapabilities::default().ttc_field_version,
            )?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let (request_seen_tx, request_seen_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            serve_dropped_response_recovery(
                listener,
                expected_request,
                synthetic_direct_path_prepare_response_payload(CURSOR_ID),
                request_seen_tx,
            )
        });

        let runtime = build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);

            {
                let mut prepare =
                    pin!(connection.direct_path_prepare(&cx, "QA", "DPL_DROP", &column_names,));
                let first = poll_fn(|task_cx| Poll::Ready(prepare.as_mut().poll(task_cx))).await;
                assert!(
                    matches!(first, Poll::Pending),
                    "direct path prepare must await its response before drop"
                );
                request_seen_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("server observed direct path prepare request");
            }

            let phase_after_drop = connection.core.recovery.phase();
            let reused = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await;
            Ok::<_, Error>((phase_after_drop, reused, connection.core.recovery.phase()))
        });

        let recovered = server.join().expect("direct path prepare server joins")?;
        let (phase_after_drop, reused, final_phase) = outcome?;
        let reused = reused.expect("fresh execute must not decode the stranded DPL response");
        assert_eq!(
            reused,
            // ubs:ignore — decodes an Oracle TTC test fixture, not a JWT.
            sequential_op_decode(&synthetic_pipeline_execute_response_payload()),
            "reuse must decode its own response byte-identically"
        );
        assert!(recovered, "reuse must take the BREAK/drain branch");
        assert_eq!(phase_after_drop, SessionRecoveryPhase::BreakSent);
        assert_eq!(final_phase, SessionRecoveryPhase::Ready);
        Ok(())
    }

    #[test]
    fn dropped_direct_path_load_stream_mid_response_drains_before_connection_reuse() -> Result<()> {
        const FRESH_SQL: &str = "select value from dpl_stream_reuse_fixture";
        const CURSOR_ID: u16 = 73;

        let stream = oracledb_protocol::dpl::encode_direct_path_rows(&[], &[], 1)?;
        let expected_request =
            oracledb_protocol::dpl::build_direct_path_load_stream_payload_with_version(
                CURSOR_ID,
                &stream,
                1,
                ClientCapabilities::default().ttc_field_version,
            )?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let (request_seen_tx, request_seen_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            serve_dropped_response_recovery(
                listener,
                expected_request,
                synthetic_direct_path_simple_response_payload(),
                request_seen_tx,
            )
        });

        let runtime = build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = test_cx()?;
            let stream_socket = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream_socket);
            let mut connection = loopback_connection(read, write);

            {
                let mut load = pin!(connection.direct_path_load_stream(&cx, CURSOR_ID, &stream));
                let first = poll_fn(|task_cx| Poll::Ready(load.as_mut().poll(task_cx))).await;
                assert!(
                    matches!(first, Poll::Pending),
                    "direct path load stream must await its response before drop"
                );
                request_seen_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("server observed direct path load stream request");
            }

            let phase_after_drop = connection.core.recovery.phase();
            let reused = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await;
            Ok::<_, Error>((phase_after_drop, reused, connection.core.recovery.phase()))
        });

        let recovered = server.join().expect("direct path load server joins")?;
        let (phase_after_drop, reused, final_phase) = outcome?;
        let reused = reused.expect("fresh execute must not decode the stranded DPL response");
        assert_eq!(
            reused,
            // ubs:ignore — decodes an Oracle TTC test fixture, not a JWT.
            sequential_op_decode(&synthetic_pipeline_execute_response_payload()),
            "reuse must decode its own response byte-identically"
        );
        assert!(recovered, "reuse must take the BREAK/drain branch");
        assert_eq!(phase_after_drop, SessionRecoveryPhase::BreakSent);
        assert_eq!(final_phase, SessionRecoveryPhase::Ready);
        Ok(())
    }

    #[test]
    fn dropped_direct_path_op_mid_response_drains_before_connection_reuse() -> Result<()> {
        const FRESH_SQL: &str = "select value from dpl_op_reuse_fixture";
        const CURSOR_ID: u16 = 73;

        let expected_request = oracledb_protocol::dpl::build_direct_path_op_payload_with_version(
            CURSOR_ID,
            oracledb_protocol::dpl::TNS_DP_OP_FINISH,
            1,
            ClientCapabilities::default().ttc_field_version,
        );
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let (request_seen_tx, request_seen_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            serve_dropped_response_recovery(
                listener,
                expected_request,
                synthetic_direct_path_simple_response_payload(),
                request_seen_tx,
            )
        });

        let runtime = build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);

            {
                let mut op = pin!(connection.direct_path_op(
                    &cx,
                    CURSOR_ID,
                    oracledb_protocol::dpl::TNS_DP_OP_FINISH,
                ));
                let first = poll_fn(|task_cx| Poll::Ready(op.as_mut().poll(task_cx))).await;
                assert!(
                    matches!(first, Poll::Pending),
                    "direct path op must await its response before drop"
                );
                request_seen_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("server observed direct path op request");
            }

            let phase_after_drop = connection.core.recovery.phase();
            let reused = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await;
            Ok::<_, Error>((phase_after_drop, reused, connection.core.recovery.phase()))
        });

        let recovered = server.join().expect("direct path op server joins")?;
        let (phase_after_drop, reused, final_phase) = outcome?;
        let reused = reused.expect("fresh execute must not decode the stranded DPL response");
        assert_eq!(
            reused,
            // ubs:ignore — decodes an Oracle TTC test fixture, not a JWT.
            sequential_op_decode(&synthetic_pipeline_execute_response_payload()),
            "reuse must decode its own response byte-identically"
        );
        assert!(recovered, "reuse must take the BREAK/drain branch");
        assert_eq!(phase_after_drop, SessionRecoveryPhase::BreakSent);
        assert_eq!(final_phase, SessionRecoveryPhase::Ready);
        Ok(())
    }

    #[test]
    fn successful_direct_path_round_trips_disarm_recovery_before_reuse() -> Result<()> {
        const FRESH_SQL: &str = "select value from dpl_success_fixture";
        const CURSOR_ID: u16 = 73;

        let column_names = Vec::<String>::new();
        let stream = oracledb_protocol::dpl::encode_direct_path_rows(&[], &[], 1)?;
        let field_version = ClientCapabilities::default().ttc_field_version;
        let expected_prepare =
            oracledb_protocol::dpl::build_direct_path_prepare_payload_with_version(
                "QA",
                "DPL_SUCCESS",
                &column_names,
                1,
                field_version,
            )?;
        let expected_load =
            oracledb_protocol::dpl::build_direct_path_load_stream_payload_with_version(
                CURSOR_ID,
                &stream,
                2,
                field_version,
            )?;
        let expected_op = oracledb_protocol::dpl::build_direct_path_op_payload_with_version(
            CURSOR_ID,
            oracledb_protocol::dpl::TNS_DP_OP_FINISH,
            3,
            field_version,
        );
        let execute_response = synthetic_pipeline_execute_response_payload();
        let server_execute_response = execute_response.clone();
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<()> {
            use std::io::Write as _;
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;

            assert_eq!(read_one_wire_data_payload(&mut socket), expected_prepare);
            socket.write_all(&data_packet(
                &synthetic_direct_path_prepare_response_payload(CURSOR_ID),
                true,
            ))?;
            socket.flush()?;

            assert_eq!(read_one_wire_data_payload(&mut socket), expected_load);
            socket.write_all(&data_packet(
                &synthetic_direct_path_simple_response_payload(),
                true,
            ))?;
            socket.flush()?;

            assert_eq!(read_one_wire_data_payload(&mut socket), expected_op);
            socket.write_all(&data_packet(
                &synthetic_direct_path_simple_response_payload(),
                true,
            ))?;
            socket.flush()?;

            assert_eq!(
                read_one_wire_packet(&mut socket),
                TNS_PACKET_TYPE_DATA,
                "reuse after DPL op must not send a spurious BREAK"
            );
            socket.write_all(&data_packet(&server_execute_response, true))?;
            socket.flush()
        });

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            let socket = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(socket);
            let mut connection = loopback_connection(read, write);

            let prepared = connection
                .direct_path_prepare(&cx, "QA", "DPL_SUCCESS", &column_names)
                .await?;
            assert_eq!(prepared.cursor_id, CURSOR_ID);
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready
            );

            connection
                .direct_path_load_stream(&cx, CURSOR_ID, &stream)
                .await?;
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready
            );

            connection
                .direct_path_op(&cx, CURSOR_ID, oracledb_protocol::dpl::TNS_DP_OP_FINISH)
                .await?;
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready
            );

            let reused = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await?;
            assert_eq!(reused, sequential_op_decode(&execute_response));
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready
            );
            Ok::<_, Error>(())
        })?;

        server
            .join()
            .expect("successful direct path server joins")?;
        Ok(())
    }

    #[test]
    fn precancelled_direct_path_prepare_writes_nothing_and_keeps_wire_ready() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<bool> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_millis(300)))?;
            let mut byte = [0u8; 1];
            match socket.read(&mut byte) {
                Ok(0) => Ok(false),
                Ok(_) => Ok(true),
                Err(err)
                    if matches!(
                        err.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    Ok(false)
                }
                Err(err) => Err(err),
            }
        });

        let runtime = build_io_runtime()?;
        let (phase, sequence_before, sequence_after) = runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            let sequence_before = connection.ttc_seq_num;
            cx.cancel_fast(CancelKind::User);

            let err = connection
                .direct_path_prepare(&cx, "QA", "DPL_PRE_CANCEL", &[])
                .await
                .expect_err("pending cancellation stops before DPL PREPARE");
            assert!(matches!(err, Error::Cancelled), "{err:?}");
            Ok::<_, Error>((
                connection.core.recovery.phase(),
                sequence_before,
                connection.ttc_seq_num,
            ))
        })?;

        assert!(
            !server
                .join()
                .expect("pre-cancel direct path server joins")?,
            "pre-cancelled DPL PREPARE must not write any wire bytes"
        );
        assert_eq!(phase, SessionRecoveryPhase::Ready);
        assert_eq!(sequence_after, sequence_before);
        Ok(())
    }

    #[test]
    fn precancelled_subscribe_register_writes_nothing_and_keeps_wire_ready() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<bool> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_millis(300)))?;
            let mut byte = [0u8; 1];
            match socket.read(&mut byte) {
                Ok(0) => Ok(false),
                Ok(_) => Ok(true),
                Err(err)
                    if matches!(
                        err.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    Ok(false)
                }
                Err(err) => Err(err),
            }
        });

        let runtime = build_io_runtime()?;
        let (phase, sequence_before, sequence_after) = runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            let sequence_before = connection.ttc_seq_num;
            cx.cancel_fast(CancelKind::User);

            let err = connection
                .subscribe_register(
                    &cx,
                    oracledb_protocol::thin::TNS_SUBSCR_NAMESPACE_DBCHANGE,
                    None,
                    oracledb_protocol::thin::SUBSCR_QOS_ROWIDS,
                    0,
                    10,
                    0,
                    0,
                    0,
                )
                .await
                .expect_err("pending cancellation stops before CQN REGISTER");
            assert!(matches!(err, Error::Cancelled), "{err:?}");
            Ok::<_, Error>((
                connection.core.recovery.phase(),
                sequence_before,
                connection.ttc_seq_num,
            ))
        })?;

        assert!(
            !server
                .join()
                .expect("pre-cancel CQN register server joins")?,
            "pre-cancelled CQN REGISTER must not write any wire bytes"
        );
        assert_eq!(phase, SessionRecoveryPhase::Ready);
        assert_eq!(sequence_after, sequence_before);
        Ok(())
    }

    #[test]
    fn dropped_lob_read_mid_response_drains_before_connection_reuse() -> Result<()> {
        const TRAILING_CANCEL_ERROR: &[u8] = &[0x04, 0x01, 0x0d];
        const FRESH_SQL: &str = "select value from lob_read_reuse_fixture";

        let locator = vec![0x11; 4];
        let stranded_response = synthetic_lob_read_response_payload(&locator, b"x", 1);
        let server_stranded_response = stranded_response.clone();
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let (lob_seen_tx, lob_seen_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || -> std::io::Result<bool> {
            use std::io::Write as _;
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(5)))?;

            assert_eq!(
                read_one_wire_packet(&mut socket),
                TNS_PACKET_TYPE_DATA,
                "LOB read must send a DATA request"
            );
            lob_seen_tx
                .send(())
                .expect("client waits for LOB read request proof");

            let (next_packet_type, next_body) = read_one_wire_packet_bytes(&mut socket);
            if next_packet_type == TNS_PACKET_TYPE_MARKER {
                assert_eq!(
                    next_body,
                    vec![1, 0, TNS_MARKER_TYPE_BREAK],
                    "reuse must BREAK the stranded LOB read before its request"
                );
                socket.write_all(&data_packet(&server_stranded_response, true))?;
                socket.write_all(&marker_packet(TNS_MARKER_TYPE_BREAK))?;
                assert_eq!(
                    read_marker_type(&mut socket),
                    TNS_MARKER_TYPE_RESET,
                    "LOB read drain must complete the RESET handshake"
                );
                socket.write_all(&marker_packet(TNS_MARKER_TYPE_RESET))?;
                socket.write_all(&data_packet(TRAILING_CANCEL_ERROR, true))?;

                assert_eq!(
                    read_one_wire_packet(&mut socket),
                    TNS_PACKET_TYPE_DATA,
                    "fresh execute follows the completed drain"
                );
                socket.write_all(&data_packet(
                    &synthetic_pipeline_execute_response_payload(),
                    true,
                ))?;
                socket.flush()?;
                Ok(true)
            } else {
                assert_eq!(
                    next_packet_type, TNS_PACKET_TYPE_DATA,
                    "without recovery the next operation is sent directly"
                );
                socket.write_all(&data_packet(&server_stranded_response, true))?;
                socket.write_all(&data_packet(
                    &synthetic_pipeline_execute_response_payload(),
                    true,
                ))?;
                socket.flush()?;
                Ok(false)
            }
        });

        let runtime = build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);

            {
                let mut read_lob = pin!(connection.read_lob(&cx, &locator, 1, 1));
                let first = poll_fn(|task_cx| Poll::Ready(read_lob.as_mut().poll(task_cx))).await;
                assert!(
                    matches!(first, Poll::Pending),
                    "LOB read must be waiting for its response before drop"
                );
                lob_seen_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("server observed LOB read request");
            }

            let phase_after_drop = connection.core.recovery.phase();
            let reused = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await;
            Ok::<_, Error>((phase_after_drop, reused, connection.core.recovery.phase()))
        });

        let recovered = server.join().expect("LOB read server joins")?;
        let (phase_after_drop, reused, final_phase) = outcome?;
        let reused = reused.expect("fresh execute must not decode the stranded LOB response");
        assert_eq!(
            reused,
            sequential_op_decode(&synthetic_pipeline_execute_response_payload()),
            "reuse must decode its own response byte-identically"
        );
        assert!(recovered, "reuse must take the BREAK/drain branch");
        assert_eq!(phase_after_drop, SessionRecoveryPhase::BreakSent);
        assert_eq!(final_phase, SessionRecoveryPhase::Ready);
        Ok(())
    }

    #[test]
    fn dropped_aq_enqueue_mid_response_drains_before_connection_reuse() -> Result<()> {
        const TRAILING_CANCEL_ERROR: &[u8] = &[0x04, 0x01, 0x0d];
        const FRESH_SQL: &str = "select value from aq_enqueue_reuse_fixture";

        let msgid = [0x2a; 16];
        let stranded_response = synthetic_aq_enqueue_response_payload(&msgid);
        let server_stranded_response = stranded_response.clone();
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let (aq_seen_tx, aq_seen_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || -> std::io::Result<bool> {
            use std::io::Write as _;
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(5)))?;

            assert_eq!(
                read_one_wire_packet(&mut socket),
                TNS_PACKET_TYPE_DATA,
                "AQ enqueue must send a DATA request"
            );
            aq_seen_tx
                .send(())
                .expect("client waits for AQ enqueue request proof");

            let (next_packet_type, next_body) = read_one_wire_packet_bytes(&mut socket);
            if next_packet_type == TNS_PACKET_TYPE_MARKER {
                assert_eq!(
                    next_body,
                    vec![1, 0, TNS_MARKER_TYPE_BREAK],
                    "reuse must BREAK the stranded AQ enqueue before its request"
                );
                socket.write_all(&data_packet(&server_stranded_response, true))?;
                socket.write_all(&marker_packet(TNS_MARKER_TYPE_BREAK))?;
                assert_eq!(
                    read_marker_type(&mut socket),
                    TNS_MARKER_TYPE_RESET,
                    "AQ enqueue drain must complete the RESET handshake"
                );
                socket.write_all(&marker_packet(TNS_MARKER_TYPE_RESET))?;
                socket.write_all(&data_packet(TRAILING_CANCEL_ERROR, true))?;

                assert_eq!(
                    read_one_wire_packet(&mut socket),
                    TNS_PACKET_TYPE_DATA,
                    "fresh execute follows the completed drain"
                );
                socket.write_all(&data_packet(
                    &synthetic_pipeline_execute_response_payload(),
                    true,
                ))?;
                socket.flush()?;
                Ok(true)
            } else {
                assert_eq!(
                    next_packet_type, TNS_PACKET_TYPE_DATA,
                    "without recovery the next operation is sent directly"
                );
                socket.write_all(&data_packet(&server_stranded_response, true))?;
                socket.write_all(&data_packet(
                    &synthetic_pipeline_execute_response_payload(),
                    true,
                ))?;
                socket.flush()?;
                Ok(false)
            }
        });

        let runtime = build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            let (queue, props, options) = synthetic_aq_enqueue_request();

            {
                let mut enqueue = pin!(connection.aq_enq_one(&cx, &queue, &props, &options));
                let first = poll_fn(|task_cx| Poll::Ready(enqueue.as_mut().poll(task_cx))).await;
                assert!(
                    matches!(first, Poll::Pending),
                    "AQ enqueue must be waiting for its response before drop"
                );
                aq_seen_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("server observed AQ enqueue request");
            }

            let phase_after_drop = connection.core.recovery.phase();
            let reused = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await;
            Ok::<_, Error>((phase_after_drop, reused, connection.core.recovery.phase()))
        });

        let recovered = server.join().expect("AQ enqueue server joins")?;
        let (phase_after_drop, reused, final_phase) = outcome?;
        let reused = reused.expect("fresh execute must not decode the stranded AQ response");
        assert_eq!(
            reused,
            sequential_op_decode(&synthetic_pipeline_execute_response_payload()),
            "reuse must decode its own response byte-identically"
        );
        assert!(recovered, "reuse must take the BREAK/drain branch");
        assert_eq!(phase_after_drop, SessionRecoveryPhase::BreakSent);
        assert_eq!(final_phase, SessionRecoveryPhase::Ready);
        Ok(())
    }

    #[test]
    fn successful_aq_enqueue_disarms_recovery_before_reuse() -> Result<()> {
        const FRESH_SQL: &str = "select value from aq_enqueue_success_fixture";
        let msgid = [0x2a; 16];
        let aq_response = synthetic_aq_enqueue_response_payload(&msgid);
        let execute_response = synthetic_pipeline_execute_response_payload();
        let server_execute_response = execute_response.clone();
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<()> {
            use std::io::Write as _;
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;

            assert_eq!(
                read_one_wire_packet(&mut socket),
                TNS_PACKET_TYPE_DATA,
                "AQ enqueue must send a DATA request"
            );
            socket.write_all(&data_packet(&aq_response, true))?;
            socket.flush()?;

            assert_eq!(
                read_one_wire_packet(&mut socket),
                TNS_PACKET_TYPE_DATA,
                "a successful AQ enqueue must not cause a spurious BREAK on reuse"
            );
            socket.write_all(&data_packet(&server_execute_response, true))?;
            socket.flush()
        });

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            let (queue, props, options) = synthetic_aq_enqueue_request();

            let assigned = connection.aq_enq_one(&cx, &queue, &props, &options).await?;
            assert_eq!(assigned.as_deref(), Some(&msgid[..]));
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready,
                "a completed AQ response must disarm its guard"
            );

            let reused = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await?;
            assert_eq!(reused, sequential_op_decode(&execute_response));
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready
            );
            Ok::<_, Error>(())
        })?;

        server.join().expect("successful AQ enqueue server joins")?;
        Ok(())
    }

    #[test]
    fn precancelled_aq_enqueue_writes_nothing_and_keeps_wire_ready() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<bool> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_millis(300)))?;
            let mut byte = [0u8; 1];
            match socket.read(&mut byte) {
                Ok(0) => Ok(false),
                Ok(_) => Ok(true),
                Err(err)
                    if matches!(
                        err.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    Ok(false)
                }
                Err(err) => Err(err),
            }
        });

        let runtime = build_io_runtime()?;
        let (phase, sequence_before, sequence_after) = runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            let sequence_before = connection.ttc_seq_num;
            let (queue, props, options) = synthetic_aq_enqueue_request();
            cx.cancel_fast(CancelKind::User);

            let err = connection
                .aq_enq_one(&cx, &queue, &props, &options)
                .await
                .expect_err("pending cancellation stops before AQ ENQUEUE");
            assert!(matches!(err, Error::Cancelled), "{err:?}");
            Ok::<_, Error>((
                connection.core.recovery.phase(),
                sequence_before,
                connection.ttc_seq_num,
            ))
        })?;

        assert!(
            !server.join().expect("pre-cancel AQ enqueue server joins")?,
            "pre-cancelled AQ ENQUEUE must not write any wire bytes"
        );
        assert_eq!(phase, SessionRecoveryPhase::Ready);
        assert_eq!(sequence_after, sequence_before);
        Ok(())
    }

    #[test]
    fn successful_lob_read_disarms_recovery_before_reuse() -> Result<()> {
        const FRESH_SQL: &str = "select value from lob_read_success_fixture";
        let locator = vec![0x11; 4];
        let replacement_locator = vec![0x22; 4];
        let lob_response = synthetic_lob_read_response_payload(&replacement_locator, b"abc", 3);
        let execute_response = synthetic_pipeline_execute_response_payload();
        let server_execute_response = execute_response.clone();
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<()> {
            use std::io::Write as _;
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;

            assert_eq!(
                read_one_wire_packet(&mut socket),
                TNS_PACKET_TYPE_DATA,
                "LOB read must send a DATA request"
            );
            socket.write_all(&data_packet(&lob_response, true))?;
            socket.flush()?;

            assert_eq!(
                read_one_wire_packet(&mut socket),
                TNS_PACKET_TYPE_DATA,
                "a successful LOB read must not cause a spurious BREAK on reuse"
            );
            socket.write_all(&data_packet(&server_execute_response, true))?;
            socket.flush()
        });

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);

            let result = connection.read_lob(&cx, &locator, 1, 3).await?;
            assert_eq!(result.data.as_deref(), Some(&b"abc"[..]));
            assert_eq!(result.locator, replacement_locator);
            assert_eq!(result.amount, 3);
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready,
                "a completed LOB response must disarm its guard"
            );

            let reused = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await?;
            assert_eq!(reused, sequential_op_decode(&execute_response));
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready
            );
            Ok::<_, Error>(())
        })?;

        server.join().expect("successful LOB read server joins")?;
        Ok(())
    }

    #[test]
    fn precancelled_lob_read_writes_nothing_and_keeps_wire_ready() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<bool> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_millis(300)))?;
            let mut byte = [0u8; 1];
            match socket.read(&mut byte) {
                Ok(0) => Ok(false),
                Ok(_) => Ok(true),
                Err(err)
                    if matches!(
                        err.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    Ok(false)
                }
                Err(err) => Err(err),
            }
        });

        let runtime = build_io_runtime()?;
        let (phase, sequence_before, sequence_after) = runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            let sequence_before = connection.ttc_seq_num;
            cx.cancel_fast(CancelKind::User);

            let err = connection
                .read_lob(&cx, &[0x11; 4], 1, 1)
                .await
                .expect_err("pending cancellation stops before LOB READ");
            assert!(matches!(err, Error::Cancelled), "{err:?}");
            Ok::<_, Error>((
                connection.core.recovery.phase(),
                sequence_before,
                connection.ttc_seq_num,
            ))
        })?;

        assert!(
            !server.join().expect("pre-cancel LOB read server joins")?,
            "pre-cancelled LOB READ must not write any wire bytes"
        );
        assert_eq!(phase, SessionRecoveryPhase::Ready);
        assert_eq!(sequence_after, sequence_before);
        Ok(())
    }

    #[test]
    fn precancelled_define_fetch_writes_nothing_and_keeps_wire_ready() -> Result<()> {
        const CURSOR_ID: u32 = 42;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<bool> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_millis(300)))?;
            let mut byte = [0u8; 1];
            match socket.read(&mut byte) {
                Ok(0) => Ok(false),
                Ok(_) => Ok(true),
                Err(err)
                    if matches!(
                        err.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    Ok(false)
                }
                Err(err) => Err(err),
            }
        });

        let runtime = build_io_runtime()?;
        let (phase, sequence_before, sequence_after) = runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            let sequence_before = connection.ttc_seq_num;
            cx.cancel_fast(CancelKind::User);

            let err = connection
                .define_and_fetch_rows_with_columns(
                    &cx,
                    CURSOR_ID,
                    10,
                    &[ColumnMetadata::new(
                        "DOC",
                        oracledb_protocol::thin::ORA_TYPE_NUM_JSON,
                    )],
                    None,
                )
                .await
                .expect_err("pending cancellation stops before DEFINE-FETCH");
            assert!(matches!(err, Error::Cancelled), "{err:?}");
            Ok::<_, Error>((
                connection.core.recovery.phase(),
                sequence_before,
                connection.ttc_seq_num,
            ))
        })?;

        assert!(
            !server.join().expect("pre-cancel server joins")?,
            "pre-cancelled DEFINE-FETCH must not write any wire bytes"
        );
        assert_eq!(phase, SessionRecoveryPhase::Ready);
        assert_eq!(sequence_after, sequence_before);
        Ok(())
    }

    #[test]
    fn successful_define_fetch_disarms_recovery_before_reuse() -> Result<()> {
        const CURSOR_ID: u32 = 42;
        const FRESH_SQL: &str = "select value from define_fetch_success_fixture";
        let response = synthetic_pipeline_execute_response_payload();
        let server_response = response.clone();
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<()> {
            use std::io::Write as _;
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;
            assert_eq!(read_one_wire_packet(&mut socket), TNS_PACKET_TYPE_DATA);
            socket.write_all(&data_packet(&server_response, true))?;
            socket.flush()?;

            assert_eq!(
                read_one_wire_packet(&mut socket),
                TNS_PACKET_TYPE_DATA,
                "a successful DEFINE-FETCH must not cause a spurious BREAK on reuse"
            );
            socket.write_all(&data_packet(&server_response, true))?;
            socket.flush()
        });

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut connection = loopback_connection(read, write);
            let columns = vec![ColumnMetadata::new(
                "VALUE",
                oracledb_protocol::thin::ORA_TYPE_NUM_NUMBER,
            )];

            let defined = connection
                .define_and_fetch_rows_with_columns(&cx, CURSOR_ID, 10, &columns, None)
                .await?;
            assert_eq!(defined.rows.len(), 1);
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready
            );

            let reused = connection
                .execute_raw(&cx, FRESH_SQL, 2, &[], ExecuteOptions::default(), None)
                .await?;
            assert_eq!(reused, sequential_op_decode(&response));
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready
            );
            Ok::<_, Error>(())
        })?;

        server
            .join()
            .expect("successful define-fetch server joins")?;
        Ok(())
    }

    #[test]
    fn oob_cancel_on_one_lane_leaves_other_lane_undisturbed() -> Result<()> {
        // a4-cn4 (rust-oracledb iec3.1.16) cross-lane isolation: an out-of-band
        // cancel — a bare BREAK fired from a `CancelHandle` (python-oracledb
        // `_break_external`) — on lane A must break + recover ONLY lane A. Lane B,
        // a fully independent `Connection` with its own socket, write mutex, and
        // recovery state, must complete its query untouched. The driver holds NO
        // cross-connection lock, so the isolation is structural; this pins it and
        // doubles as the no-cancel negative control (lane B never cancels).
        const A_INFLIGHT: &[u8] = b"lane-A in-flight response";
        const A_CANCEL_ERROR: &[u8] = &[0x04, 0x01, 0x0d]; // ORA-01013 user cancel

        // --- lane A: scripts the out-of-band cancel choreography ---
        let listener_a = TcpListener::bind("127.0.0.1:0").expect("bind lane A");
        let addr_a = listener_a.local_addr().expect("lane A address");
        let server_a = thread::spawn(move || {
            use std::io::Write as _;
            let (mut socket, _) = listener_a.accept().expect("accept lane A");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_BREAK,
                "lane A must receive the out-of-band BREAK"
            );
            socket
                .write_all(&data_packet(A_INFLIGHT, true))
                .expect("write lane A in-flight response");
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_BREAK))
                .expect("write lane A break-ack marker");
            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_RESET,
                "lane A owner drain answers the break marker with RESET"
            );
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_RESET))
                .expect("write lane A reset-confirm marker");
            socket
                .write_all(&data_packet(A_CANCEL_ERROR, true))
                .expect("write lane A trailing cancel error");
        });

        // --- lane B: answers an ordinary query, never cancelled ---
        let listener_b = TcpListener::bind("127.0.0.1:0").expect("bind lane B");
        let addr_b = listener_b.local_addr().expect("lane B address");
        let server_b = thread::spawn(move || {
            use std::io::Write as _;
            let (mut socket, _) = listener_b.accept().expect("accept lane B");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            let _query_request = read_one_wire_packet(&mut socket);
            socket
                .write_all(&data_packet(
                    &synthetic_pipeline_execute_response_payload(),
                    true,
                ))
                .expect("write lane B query response");
            socket.flush().expect("flush lane B");
        });

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            let (read_a, write_a) = transport::plain_split(TcpStream::connect(addr_a).await?);
            let mut conn_a = loopback_connection(read_a, write_a);
            let (read_b, write_b) = transport::plain_split(TcpStream::connect(addr_b).await?);
            let mut conn_b = loopback_connection(read_b, write_b);

            // Out-of-band cancel lane A: fire the bare BREAK from the handle, then
            // the owner drains the multi-stage cancel response back to Ready.
            let mut handle = conn_a.cancel_handle()?;
            handle.cancel(&cx).await?;
            assert_eq!(
                conn_a.core.recovery.phase(),
                SessionRecoveryPhase::BreakSent,
                "the out-of-band handle only requests the break"
            );
            conn_a.drain_cancel_response().await?;
            assert_eq!(
                conn_a.core.recovery.phase(),
                SessionRecoveryPhase::Ready,
                "lane A recovers cleanly from the out-of-band cancel"
            );
            assert!(
                !conn_a.is_dead(),
                "an out-of-band cancel keeps lane A's session alive"
            );

            // Lane B — untouched by lane A's cancel — completes its query
            // correctly (the no-cancel negative control).
            let result_b = conn_b
                .execute_raw(
                    &cx,
                    "select value from synthetic_fixture",
                    2,
                    &[],
                    ExecuteOptions::default(),
                    None,
                )
                .await?;
            assert_eq!(
                result_b,
                sequential_op_decode(&synthetic_pipeline_execute_response_payload()),
                "lane B's query is undisturbed by lane A's out-of-band cancel"
            );
            assert_eq!(
                conn_b.core.recovery.phase(),
                SessionRecoveryPhase::Ready,
                "lane B never entered recovery"
            );
            Ok::<_, Error>(())
        })?;

        server_a.join().expect("lane A server joins");
        server_b.join().expect("lane B server joins");
        Ok(())
    }

    #[test]
    fn scripted_transport_injects_errors_and_eof() -> Result<()> {
        let payload = encode_packet(
            TNS_PACKET_TYPE_CONNECT,
            0,
            None,
            b"FAULT",
            PacketLengthWidth::Legacy16,
        )?;

        let runtime = build_io_runtime()?;

        let read_error = Arc::new(std::sync::Mutex::new(ScriptedIoState::new(
            vec![ReadAction::Error("scripted read fault")],
            Vec::new(),
            ScriptedClock::default(),
        )));
        let mut core = ConnectionCore::<ScriptedTransport>::from_halves(
            ScriptedRead::from_state(read_error),
            ScriptedWrite::from_state(Arc::new(std::sync::Mutex::new(ScriptedIoState::new(
                Vec::new(),
                Vec::new(),
                ScriptedClock::default(),
            )))),
            "scripted_read_error_write",
        );
        let read_err = runtime.block_on(async {
            match core.read_packet(PacketLengthWidth::Legacy16).await {
                Ok(_) => Err(Error::Runtime(
                    "scripted read fault unexpectedly succeeded".into(),
                )),
                Err(err) => Ok(err),
            }
        })?;
        assert!(
            read_err.to_string().contains("scripted read fault"),
            "scripted read error should keep its diagnostic"
        );

        let read_eof = Arc::new(std::sync::Mutex::new(ScriptedIoState::new(
            vec![ReadAction::Eof],
            Vec::new(),
            ScriptedClock::default(),
        )));
        let mut core = ConnectionCore::<ScriptedTransport>::from_halves(
            ScriptedRead::from_state(read_eof),
            ScriptedWrite::from_state(Arc::new(std::sync::Mutex::new(ScriptedIoState::new(
                Vec::new(),
                Vec::new(),
                ScriptedClock::default(),
            )))),
            "scripted_read_eof_write",
        );
        let eof_err = runtime.block_on(async {
            match core.read_packet(PacketLengthWidth::Legacy16).await {
                Ok(_) => Err(Error::Runtime(
                    "scripted read EOF unexpectedly succeeded".into(),
                )),
                Err(err) => Ok(err),
            }
        })?;
        let eof_message = eof_err.to_string().to_ascii_lowercase();
        assert!(
            matches!(&eof_err, Error::Io(_))
                && (eof_message.contains("failed to fill whole buffer")
                    || eof_message.contains("early eof")
                    || eof_message.contains("unexpected eof")
                    || eof_message.contains("end of file")),
            "scripted EOF should surface as an incomplete read, got {eof_err:?}"
        );

        let write_error = Arc::new(std::sync::Mutex::new(ScriptedIoState::new(
            Vec::new(),
            vec![WriteAction::Error("scripted write fault")],
            ScriptedClock::default(),
        )));
        let core = ConnectionCore::<ScriptedTransport>::from_halves(
            ScriptedRead::from_state(Arc::new(std::sync::Mutex::new(ScriptedIoState::new(
                Vec::new(),
                Vec::new(),
                ScriptedClock::default(),
            )))),
            ScriptedWrite::from_state(write_error),
            "scripted_write_error_write",
        );
        let write_err = runtime.block_on(async {
            let cx = test_cx()?;
            match core.write_all(&cx, &payload).await {
                Ok(()) => Err(Error::Runtime(
                    "scripted write error unexpectedly succeeded".into(),
                )),
                Err(err) => Ok(err),
            }
        })?;
        assert!(
            write_err.to_string().contains("scripted write fault"),
            "scripted write error should keep its diagnostic"
        );

        let write_eof = Arc::new(std::sync::Mutex::new(ScriptedIoState::new(
            Vec::new(),
            vec![WriteAction::Eof],
            ScriptedClock::default(),
        )));
        let core = ConnectionCore::<ScriptedTransport>::from_halves(
            ScriptedRead::from_state(Arc::new(std::sync::Mutex::new(ScriptedIoState::new(
                Vec::new(),
                Vec::new(),
                ScriptedClock::default(),
            )))),
            ScriptedWrite::from_state(write_eof),
            "scripted_write_eof_write",
        );
        let write_eof_err = runtime.block_on(async {
            let cx = test_cx()?;
            match core.write_all(&cx, &payload).await {
                Ok(()) => Err(Error::Runtime(
                    "scripted write EOF unexpectedly succeeded".into(),
                )),
                Err(err) => Ok(err),
            }
        })?;
        assert!(
            write_eof_err
                .to_string()
                .contains("failed to write whole buffer")
                || write_eof_err.to_string().contains("write zero"),
            "scripted write EOF should surface as an incomplete write"
        );

        Ok(())
    }

    #[test]
    fn scripted_transport_rejects_mismatched_and_extra_writes() -> Result<()> {
        let expected = encode_packet(
            TNS_PACKET_TYPE_CONNECT,
            0,
            None,
            b"EXPECTED",
            PacketLengthWidth::Legacy16,
        )?;
        let actual = encode_packet(
            TNS_PACKET_TYPE_CONNECT,
            0,
            None,
            b"ACTUAL",
            PacketLengthWidth::Legacy16,
        )?;

        let runtime = build_io_runtime()?;
        let mismatch = Arc::new(std::sync::Mutex::new(ScriptedIoState::new(
            Vec::new(),
            vec![WriteAction::expect_bytes(expected, None)],
            ScriptedClock::default(),
        )));
        let core = ConnectionCore::<ScriptedTransport>::from_halves(
            ScriptedRead::from_state(Arc::new(std::sync::Mutex::new(ScriptedIoState::new(
                Vec::new(),
                Vec::new(),
                ScriptedClock::default(),
            )))),
            ScriptedWrite::from_state(mismatch),
            "scripted_mismatch_write",
        );
        let mismatch_err = runtime.block_on(async {
            let cx = test_cx()?;
            match core.write_all(&cx, &actual).await {
                Ok(()) => Err(Error::Runtime(
                    "scripted mismatched write unexpectedly succeeded".into(),
                )),
                Err(err) => Ok(err),
            }
        })?;
        assert!(
            mismatch_err.to_string().contains("write mismatch"),
            "scripted write mismatch should be explicit"
        );

        let extra = Arc::new(std::sync::Mutex::new(ScriptedIoState::new(
            Vec::new(),
            Vec::new(),
            ScriptedClock::default(),
        )));
        let core = ConnectionCore::<ScriptedTransport>::from_halves(
            ScriptedRead::from_state(Arc::new(std::sync::Mutex::new(ScriptedIoState::new(
                Vec::new(),
                Vec::new(),
                ScriptedClock::default(),
            )))),
            ScriptedWrite::from_state(extra),
            "scripted_extra_write",
        );
        let extra_err = runtime.block_on(async {
            let cx = test_cx()?;
            match core.write_all(&cx, &actual).await {
                Ok(()) => Err(Error::Runtime(
                    "scripted extra write unexpectedly succeeded".into(),
                )),
                Err(err) => Ok(err),
            }
        })?;
        assert!(
            extra_err.to_string().contains("unexpected write"),
            "scripted extra write should be rejected"
        );

        Ok(())
    }

    #[cfg(feature = "cassette")]
    #[test]
    fn connection_core_routes_connect_execute_fetch_over_replay_transport() -> Result<()> {
        use oracledb_protocol::net::cassette::{self, Direction};

        const EXECUTE_BODY: &[u8] = b"replay execute payload";
        const FETCH_BODY: &[u8] = b"replay fetch payload";
        const EXECUTE_RESPONSE: &[u8] = b"replay execute response";
        const FETCH_RESPONSE: &[u8] = b"replay fetch response";

        let connect_packet = encode_packet(
            TNS_PACKET_TYPE_CONNECT,
            0,
            None,
            b"REPLAY-CONNECT",
            PacketLengthWidth::Legacy16,
        )?;
        let accept_packet = encode_packet(
            TNS_PACKET_TYPE_ACCEPT,
            0,
            None,
            b"REPLAY-ACCEPT",
            PacketLengthWidth::Legacy16,
        )?;
        let execute_packet = encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(0),
            EXECUTE_BODY,
            PacketLengthWidth::Large32,
        )?;
        let fetch_packet = encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(0),
            FETCH_BODY,
            PacketLengthWidth::Large32,
        )?;

        let mut cassette_bytes = Vec::new();
        cassette::write_header(&mut cassette_bytes);
        cassette::write_frame(
            &mut cassette_bytes,
            Direction::ClientToServer,
            0,
            &connect_packet,
        );
        cassette::write_frame(
            &mut cassette_bytes,
            Direction::ServerToClient,
            1,
            &accept_packet,
        );
        cassette::write_frame(
            &mut cassette_bytes,
            Direction::ClientToServer,
            2,
            &execute_packet,
        );
        cassette::write_frame(
            &mut cassette_bytes,
            Direction::ServerToClient,
            3,
            &data_packet(EXECUTE_RESPONSE, true),
        );
        cassette::write_frame(
            &mut cassette_bytes,
            Direction::ClientToServer,
            4,
            &fetch_packet,
        );
        cassette::write_frame(
            &mut cassette_bytes,
            Direction::ServerToClient,
            5,
            &data_packet(FETCH_RESPONSE, true),
        );

        let (read, write) =
            transport::replay_split(&cassette_bytes, transport::ReplayWriteMode::Check)
                .map_err(|err| Error::Runtime(format!("invalid replay cassette: {err}")))?;
        let mut core =
            ConnectionCore::<DriverTransport>::from_halves(read, write, "replay_core_write");

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = test_cx()?;
            core.write_all(&cx, &connect_packet).await?;
            let accept = core.read_packet(PacketLengthWidth::Legacy16).await?;
            assert_eq!(accept.packet_type, TNS_PACKET_TYPE_ACCEPT);
            assert_eq!(accept.payload, b"REPLAY-ACCEPT");

            core.send_data_packet(&cx, EXECUTE_BODY, 8192).await?;
            let execute_response = core.read_data_response(&cx).await?;
            assert_eq!(execute_response, EXECUTE_RESPONSE);

            core.send_data_packet(&cx, FETCH_BODY, 8192).await?;
            let fetch_response = core.read_data_response(&cx).await?;
            assert_eq!(fetch_response, FETCH_RESPONSE);
            Ok::<_, Error>(())
        })?;
        Ok(())
    }

    /// K6 DoD: a captured query-FAILURE session is scrubbed of auth secrets,
    /// the artifact passes the secret-scan (C4), and offline replay of the
    /// artifact reproduces the failure with no database.
    #[cfg(feature = "cassette")]
    #[test]
    fn support_capture_scrubbed_cassette_replays_query_failure() -> Result<()> {
        use oracledb_protocol::net::cassette::{self, Direction};

        const QUERY_BODY: &[u8] = b"SELECT * FROM missing_table";
        // The failure that offline replay must reproduce (secret-free ORA text).
        const ERROR_RESPONSE: &[u8] = b"ORA-00942: table or view does not exist";

        let connect_packet = encode_packet(
            TNS_PACKET_TYPE_CONNECT,
            0,
            None,
            b"K6-CONNECT",
            PacketLengthWidth::Legacy16,
        )?;
        let accept_packet = encode_packet(
            TNS_PACKET_TYPE_ACCEPT,
            0,
            None,
            b"K6-ACCEPT",
            PacketLengthWidth::Legacy16,
        )?;
        // Auth-phase frames carrying secret material (password verifier +
        // session key) — exactly what a naive capture would leak to disk.
        let mut auth_body = b"AUTH_PASSWORD=hunter2 AUTH_SESSKEY=".to_vec();
        auth_body.extend_from_slice(&[0x5A_u8; 48]);
        let auth_request = encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(0),
            &auth_body,
            PacketLengthWidth::Large32,
        )?;
        let auth_response_body = b"AUTH_SVR SESSION_KEY=deadbeefcafef00ddeadbeefcafef00d".to_vec();
        let query_packet = encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(0),
            QUERY_BODY,
            PacketLengthWidth::Large32,
        )?;

        // The raw captured session, as the recording transport would produce it.
        let mut raw = Vec::new();
        cassette::write_header(&mut raw);
        cassette::write_frame(&mut raw, Direction::ClientToServer, 0, &connect_packet);
        cassette::write_frame(&mut raw, Direction::ServerToClient, 1, &accept_packet);
        cassette::write_frame(&mut raw, Direction::ClientToServer, 2, &auth_request);
        cassette::write_frame(
            &mut raw,
            Direction::ServerToClient,
            3,
            &data_packet(&auth_response_body, true),
        );
        cassette::write_frame(&mut raw, Direction::ClientToServer, 4, &query_packet);
        cassette::write_frame(
            &mut raw,
            Direction::ServerToClient,
            5,
            &data_packet(ERROR_RESPONSE, true),
        );

        // The RAW capture carries secrets — this is why the scrub gate exists.
        assert!(
            !transport::scan_for_secret_fields(&raw).is_empty(),
            "raw capture is expected to contain auth secrets"
        );

        // Scrub the auth phase and run the fail-closed refuse gate.
        let (scrubbed, report) = transport::scrub_and_gate(&raw)
            .map_err(|e| Error::Runtime(format!("scrub gate refused unexpectedly: {e}")))?;
        assert!(report.redacted_frames >= 1, "auth frames must be redacted");
        // C4 secret-scan passes on the persisted artifact.
        assert!(
            transport::scan_for_secret_fields(&scrubbed).is_empty(),
            "scrubbed artifact must pass the secret scan"
        );

        // Persist as a shareable file, reload it, and confirm it still scans clean.
        let path = std::env::temp_dir().join(format!(
            "oracledb-k6-replay-{}.tns-cassette",
            std::process::id()
        ));
        std::fs::write(&path, &scrubbed).map_err(|e| Error::Runtime(e.to_string()))?;
        let from_disk = std::fs::read(&path).map_err(|e| Error::Runtime(e.to_string()))?;
        assert!(transport::scan_for_secret_fields(&from_disk).is_empty());

        // Offline replay reproduces the query FAILURE with no database.
        let (read, write) = transport::replay_split(&from_disk, transport::ReplayWriteMode::Ignore)
            .map_err(|err| Error::Runtime(format!("invalid replay cassette: {err}")))?;
        let mut core =
            ConnectionCore::<DriverTransport>::from_halves(read, write, "k6_replay_write");
        let runtime = build_io_runtime()?;
        let replay = runtime.block_on(async {
            let cx = test_cx()?;
            core.write_all(&cx, &connect_packet).await?;
            let accept = core.read_packet(PacketLengthWidth::Legacy16).await?;
            assert_eq!(accept.packet_type, TNS_PACKET_TYPE_ACCEPT);

            // Auth round-trip: the recorded server frame is redacted but its TNS
            // framing is intact, so the decoder walks past it structurally.
            core.send_data_packet(&cx, &auth_body, 8192).await?;
            let _auth_response = core.read_data_response(&cx).await?;

            core.send_data_packet(&cx, QUERY_BODY, 8192).await?;
            core.read_data_response(&cx).await
        });
        let _ = std::fs::remove_file(&path);
        let failure = replay?;
        assert_eq!(
            failure, ERROR_RESPONSE,
            "offline replay must reproduce the recorded query failure"
        );
        Ok(())
    }

    #[cfg(feature = "cassette")]
    #[test]
    fn connection_core_routes_connect_execute_fetch_over_recording_transport() -> Result<()> {
        use oracledb_protocol::net::cassette::{self, Direction};
        use std::io::Write as _;

        const EXECUTE_BODY: &[u8] = b"recording execute payload";
        const FETCH_BODY: &[u8] = b"recording fetch payload";
        const EXECUTE_RESPONSE: &[u8] = b"recording execute response";
        const FETCH_RESPONSE: &[u8] = b"recording fetch response";

        let connect_packet = encode_packet(
            TNS_PACKET_TYPE_CONNECT,
            0,
            None,
            b"RECORDING-CONNECT",
            PacketLengthWidth::Legacy16,
        )?;
        let accept_packet = encode_packet(
            TNS_PACKET_TYPE_ACCEPT,
            0,
            None,
            b"RECORDING-ACCEPT",
            PacketLengthWidth::Legacy16,
        )?;
        let execute_packet = encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(0),
            EXECUTE_BODY,
            PacketLengthWidth::Large32,
        )?;
        let fetch_packet = encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(0),
            FETCH_BODY,
            PacketLengthWidth::Large32,
        )?;
        let execute_response_packet = data_packet(EXECUTE_RESPONSE, true);
        let fetch_response_packet = data_packet(FETCH_RESPONSE, true);

        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server_connect = connect_packet.clone();
        let server_accept = accept_packet.clone();
        let server_execute = execute_packet.clone();
        let server_fetch = fetch_packet.clone();
        let server_execute_response = execute_response_packet.clone();
        let server_fetch_response = fetch_response_packet.clone();
        let server = thread::spawn(move || -> std::io::Result<()> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(5)))?;

            let mut got = vec![0u8; server_connect.len()];
            socket.read_exact(&mut got)?;
            assert_eq!(got, server_connect);
            socket.write_all(&server_accept)?;

            let mut got = vec![0u8; server_execute.len()];
            socket.read_exact(&mut got)?;
            assert_eq!(got, server_execute);
            socket.write_all(&server_execute_response)?;

            let mut got = vec![0u8; server_fetch.len()];
            socket.read_exact(&mut got)?;
            assert_eq!(got, server_fetch);
            socket.write_all(&server_fetch_response)?;
            Ok(())
        });

        let runtime = build_io_runtime()?;
        let cassette_bytes = runtime.block_on(async {
            let cx = test_cx()?;
            let scope = transport::capture_scope();
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(stream);
            let mut core =
                ConnectionCore::<DriverTransport>::from_halves(read, write, "recording_core_write");

            core.write_all(&cx, &connect_packet).await?;
            let accept = core.read_packet(PacketLengthWidth::Legacy16).await?;
            assert_eq!(accept.packet_type, TNS_PACKET_TYPE_ACCEPT);
            assert_eq!(accept.payload, b"RECORDING-ACCEPT");

            core.send_data_packet(&cx, EXECUTE_BODY, 8192).await?;
            let execute_response = core.read_data_response(&cx).await?;
            assert_eq!(execute_response, EXECUTE_RESPONSE);

            core.send_data_packet(&cx, FETCH_BODY, 8192).await?;
            let fetch_response = core.read_data_response(&cx).await?;
            assert_eq!(fetch_response, FETCH_RESPONSE);

            Ok::<_, Error>(scope.to_cassette_bytes())
        })?;

        server
            .join()
            .map_err(|_| Error::Runtime("recording test server thread panicked".into()))??;
        let frames = cassette::decode_all(&cassette_bytes)
            .map_err(|err| Error::Runtime(format!("invalid recorded cassette: {err}")))?;
        let (accept_header, accept_payload) = accept_packet.split_at(8);
        let (execute_response_header, execute_response_payload) =
            execute_response_packet.split_at(8);
        let (fetch_response_header, fetch_response_payload) = fetch_response_packet.split_at(8);

        assert_eq!(frames.len(), 9);
        assert_eq!(frames[0].direction, Direction::ClientToServer);
        assert_eq!(frames[0].bytes, connect_packet);
        assert_eq!(frames[1].direction, Direction::ServerToClient);
        assert_eq!(frames[1].bytes, accept_header);
        assert_eq!(frames[2].direction, Direction::ServerToClient);
        assert_eq!(frames[2].bytes, accept_payload);
        assert_eq!(frames[3].direction, Direction::ClientToServer);
        assert_eq!(frames[3].bytes, execute_packet);
        assert_eq!(frames[4].direction, Direction::ServerToClient);
        assert_eq!(frames[4].bytes, execute_response_header);
        assert_eq!(frames[5].direction, Direction::ServerToClient);
        assert_eq!(frames[5].bytes, execute_response_payload);
        assert_eq!(frames[6].direction, Direction::ClientToServer);
        assert_eq!(frames[6].bytes, fetch_packet);
        assert_eq!(frames[7].direction, Direction::ServerToClient);
        assert_eq!(frames[7].bytes, fetch_response_header);
        assert_eq!(frames[8].direction, Direction::ServerToClient);
        assert_eq!(frames[8].bytes, fetch_response_payload);
        Ok(())
    }

    /// Reads exactly one 11-byte TNS marker packet from `socket` and returns its
    /// marker type byte (payload byte 2). Used by the server side of the seam to
    /// observe the BREAK and RESET markers the client emits.
    fn read_marker_type(socket: &mut std::net::TcpStream) -> u8 {
        let mut packet = [0u8; 11];
        socket.read_exact(&mut packet).expect("read marker packet");
        assert_eq!(
            packet[4], TNS_PACKET_TYPE_MARKER,
            "expected a MARKER packet"
        );
        packet[10]
    }

    // THE FIX: break_and_drain_wire sends BREAK, then consumes the ENTIRE
    // post-timeout sequence so the stream is left at a clean boundary. The
    // sequence here matches the live wire trace against Oracle 23/26ai: the
    // server flushes the cancelled call's IN-FLIGHT RESPONSE *first* (a complete
    // DATA response carrying its own end-of-response flag) and only THEN sends
    // the break-ack MARKER, the RESET handshake, and the trailing ORA-01013
    // error packet. A drain that stopped at the in-flight response's
    // end-of-response would leave the MARKER + error in the socket; the fix
    // discards the in-flight response(s), runs the RESET dance, and consumes the
    // trailing error too. A FOLLOWING read_data_response then decodes the NEXT
    // response correctly rather than the stale leftovers.
    #[test]
    fn break_and_drain_consumes_inflight_response_and_reset_then_next_read_is_fresh() {
        // The cancelled call's in-flight response (carries end-of-response): the
        // stale bytes that must be discarded, NOT mistaken for the next result.
        const INFLIGHT_BODY: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];
        // The trailing error packet (ORA-01013-shaped; arbitrary payload here).
        const ERROR_BODY: &[u8] = &[0x04, 0x01, 0x02];
        // The genuine response to the NEXT operation on the reused connection.
        const FRESH_BODY: &[u8] = &[0x11, 0x22, 0x33, 0x44, 0x55];

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            use std::io::Write as _;

            // 1) Client sends BREAK.
            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_BREAK,
                "client must send a BREAK marker first"
            );
            // 2) Server flushes the cancelled call's in-flight response FIRST,
            //    with its OWN end-of-response flag (the exact race that made a
            //    stop-at-first-boundary drain leak the MARKER + error).
            socket
                .write_all(&data_packet(INFLIGHT_BODY, true))
                .expect("write in-flight response");
            // 3) Server's break-ack MARKER -> drives the client's RESET dance.
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_BREAK))
                .expect("write break-ack marker");
            // 4) Client replies with RESET; server confirms with a RESET marker.
            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_RESET,
                "client must answer the marker with a RESET"
            );
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_RESET))
                .expect("write reset-confirm marker");
            // 5) Trailing error packet (ORA-01013) that ends the break response.
            socket
                .write_all(&data_packet(ERROR_BODY, true))
                .expect("write trailing error packet");
            // 6) The FRESH response to the next operation on the reused conn.
            socket
                .write_all(&data_packet(FRESH_BODY, true))
                .expect("write fresh response");
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let next = runtime.block_on(async {
            let cx = Cx::current().expect("ambient Cx");
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (mut read, write) = transport::plain_split(stream);
            let write: SharedWriteHalf = Arc::new(AsyncMutex::with_name("drain_test_write", write));

            // The fix: break + drain leaves the stream clean.
            break_and_drain_wire(&mut read, &write, Duration::from_secs(5))
                .await
                .expect("drain must succeed and leave the stream clean");

            // The next operation reads its OWN response, not the stale leftovers.
            read_data_response(&mut read, &cx, &write)
                .await
                .expect("next read after drain must decode cleanly")
        });

        assert_eq!(
            next, FRESH_BODY,
            "after break_and_drain the reused connection must read the FRESH response, \
             not the stale in-flight response ({INFLIGHT_BODY:?}) or error body ({ERROR_BODY:?})"
        );
        server.join().expect("server thread joins");
    }

    // bead rust-oracledb-99xu: on a pre-23ai server (no END_OF_RESPONSE framing)
    // the trailing error response after a BREAK/RESET carries NEITHER the
    // END_OF_RESPONSE data flag NOR a terminal marker byte -- it ends at its
    // terminal TTC message (here a STATUS). The flag-only recovery reader could
    // not detect that boundary and blocked until its secondary timeout, so the
    // call-timeout / cancel surfaced as ConnectionClosed instead of the real
    // CallTimeout / Cancelled (observed live against Oracle 18c/21c). The
    // classic-framing drain must complete on the terminal message and leave the
    // stream clean for the reused connection.
    #[test]
    fn classic_pre23ai_break_drain_completes_on_terminal_message_not_flag() {
        // A minimal, valid classic terminal: a STATUS message (msg type 9) whose
        // ub4 call-status and ub2 sequence are both zero-length. It carries NO
        // end-of-response flag and does NOT end in a marker byte.
        // TNS_MSG_TYPE_STATUS == 9 (oracledb_protocol::thin::constants).
        const CLASSIC_STATUS_TERMINAL: &[u8] = &[9, 0, 0];
        const FRESH_BODY: &[u8] = &[0x11, 0x22, 0x33, 0x44, 0x55];

        // Guard: the flag-only detectors must NOT recognise this classic
        // terminal, which is precisely why the `classic` completion rule is
        // load-bearing (if either fired, the bug could never have occurred).
        assert!(
            !data_packet_ends_response(0, CLASSIC_STATUS_TERMINAL),
            "flag-framed detector must not terminate a flagless classic response"
        );
        assert!(
            !post_reset_packet_ends_response(CLASSIC_STATUS_TERMINAL),
            "post-reset marker-byte detector must not terminate a STATUS terminal"
        );
        assert!(
            classic_connect_response_is_complete(CLASSIC_STATUS_TERMINAL, ProtocolLimits::DEFAULT)
                .unwrap(),
            "the classic reader must recognise the STATUS terminal as complete"
        );

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            use std::io::Write as _;

            // The two-thread cancel path: the BREAK was already sent by the
            // handle thread, so the drain begins at the server's break-ack
            // MARKER. Reference cancel wire sequence, pre-23ai framing.
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_BREAK))
                .expect("write break-ack marker");
            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_RESET,
                "client must answer the marker with a RESET"
            );
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_RESET))
                .expect("write reset-confirm marker");
            // Trailing error/STATUS response in CLASSIC framing: no EOR flag.
            socket
                .write_all(&data_packet(CLASSIC_STATUS_TERMINAL, false))
                .expect("write flagless classic terminal");
            // The FRESH response the reused connection must read next.
            socket
                .write_all(&data_packet(FRESH_BODY, true))
                .expect("write fresh response");
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let next = runtime.block_on(async {
            let cx = Cx::current().expect("ambient Cx");
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (mut read, write) = transport::plain_split(stream);
            let write: SharedWriteHalf =
                Arc::new(AsyncMutex::with_name("classic_drain_test_write", write));

            // classic = true: the drain completes on the terminal message.
            drain_break_response_recovery_with_limits(
                &mut read,
                &write,
                ProtocolLimits::DEFAULT,
                true,
            )
            .await
            .expect("classic drain must complete on the terminal message");

            read_data_response(&mut read, &cx, &write)
                .await
                .expect("next read after classic drain must decode cleanly")
        });

        assert_eq!(
            next, FRESH_BODY,
            "after the classic drain the reused connection must read the FRESH response"
        );
        server.join().expect("server thread joins");
    }

    // W1-T2.2: core recovery must not inherit the expired operation context.
    // The core moves the read half to a short-lived no-ambient recovery thread;
    // this test timeout-cancels the caller context before recovery starts and
    // proves the bounded drain still completes.
    #[test]
    fn core_break_and_drain_runs_after_caller_context_timeout() {
        const INFLIGHT_BODY: &[u8] = &[0xD1, 0xA1, 0xB1];
        const ERROR_BODY: &[u8] = &[0x04, 0x01, 0x0d];

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            use std::io::Write as _;

            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_BREAK,
                "recovery must send BREAK even after caller context timeout"
            );
            socket
                .write_all(&data_packet(INFLIGHT_BODY, true))
                .expect("write in-flight response");
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_BREAK))
                .expect("write break-ack marker");
            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_RESET,
                "recovery must answer break marker with RESET"
            );
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_RESET))
                .expect("write reset-confirm marker");
            socket
                .write_all(&data_packet(ERROR_BODY, true))
                .expect("write trailing error packet");
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        runtime.block_on(async {
            let cx = Cx::current().expect("ambient Cx");
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (read, write) = transport::plain_split(stream);
            let mut core = ConnectionCore::<DriverTransport>::from_halves(
                read,
                write,
                "timeout_drain_test_write",
            );

            cx.cancel_fast(asupersync::CancelKind::Timeout);
            assert!(
                cx.checkpoint().is_err(),
                "test must start from an expired caller context"
            );

            core.break_and_drain_wire(Duration::from_secs(5))
                .expect("recovery drain must ignore the expired caller context");
        });

        server.join().expect("server thread joins");
    }

    #[test]
    fn preexpired_query_deadline_does_not_break_idle_connection() -> Result<()> {
        const INFLIGHT_BODY: &[u8] = &[0xd1, 0xa1, 0xb1];
        const CANCEL_ERROR: &[u8] = &[0x04, 0x01, 0x0d];

        let commit_packet = encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(0),
            &build_function_payload_with_seq(
                TNS_FUNC_COMMIT,
                1,
                ClientCapabilities::default().ttc_field_version,
            ),
            PacketLengthWidth::Large32,
        )?;

        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<usize> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(5)))?;
            use std::io::Write as _;

            let mut break_count = 0usize;
            loop {
                let mut header = [0u8; 8];
                socket.read_exact(&mut header)?;
                let declared =
                    u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
                let mut packet = header.to_vec();
                let mut body = vec![0u8; declared - header.len()];
                socket.read_exact(&mut body)?;
                packet.extend_from_slice(&body);
                if packet[4] == TNS_PACKET_TYPE_MARKER {
                    assert_eq!(packet[10], TNS_MARKER_TYPE_BREAK);
                    break_count += 1;
                    socket.write_all(&data_packet(INFLIGHT_BODY, true))?;
                    socket.write_all(&marker_packet(TNS_MARKER_TYPE_BREAK))?;
                    assert_eq!(read_marker_type(&mut socket), TNS_MARKER_TYPE_RESET);
                    socket.write_all(&marker_packet(TNS_MARKER_TYPE_RESET))?;
                    socket.write_all(&data_packet(CANCEL_ERROR, true))?;
                    socket.flush()?;
                    continue;
                }

                assert_eq!(
                    packet, commit_packet,
                    "the first ordinary request after both local expiries must be COMMIT"
                );
                socket.write_all(&data_packet(&[TNS_MSG_TYPE_END_OF_RESPONSE], true))?;
                socket.flush()?;
                return Ok(break_count);
            }
        });

        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current().expect("runtime installs an ambient Cx");
            let socket = TcpStream::connect(addr).await?;
            let (read, write) = transport::plain_split(socket);
            let mut connection = loopback_connection(read, write);

            fn assert_send<T: Send>(_: &T) {}
            let zero_timeout = connection.execute_with(
                &cx,
                Execute::new("begin null; end;").timeout(Duration::ZERO),
            );
            assert_send(&zero_timeout);
            let err = zero_timeout
                .await
                .expect_err("an already-expired query deadline must fail");
            assert!(
                matches!(err, Error::CallTimeout(0)),
                "pre-expired deadline must surface CallTimeout(0), got {err:?}"
            );

            let ambient_deadline = QueryDeadline::from_budget(
                asupersync::time::wall_now(),
                Budget::new().with_deadline(Time::ZERO),
                None,
            );
            let ambient_err = connection
                .execute_with_deadline(&cx, Execute::new("begin null; end;"), ambient_deadline)
                .await
                .expect_err("an expired ambient deadline must fail before polling");
            assert!(
                matches!(ambient_err, Error::CallTimeout(0)),
                "expired ambient deadline must surface CallTimeout(0), got {ambient_err:?}"
            );
            assert_eq!(
                connection.core.recovery.phase(),
                SessionRecoveryPhase::Ready,
                "a future that was never polled must not arm wire recovery"
            );
            assert!(
                !connection.dead,
                "local before-start expiry must not mark a healthy connection dead"
            );

            connection.commit(&cx).await?;
            Ok::<_, Error>(())
        })?;

        let break_count = server.join().expect("server thread joins")?;
        assert_eq!(
            break_count, 0,
            "neither a zero request timeout nor an expired ambient deadline may send BREAK"
        );
        Ok(())
    }

    #[test]
    fn prestart_expiry_preserves_structured_cancel_without_wire_io() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<Vec<usize>> {
            let mut received = Vec::with_capacity(2);
            for _ in 0..2 {
                let (mut socket, _) = listener.accept()?;
                socket.set_read_timeout(Some(Duration::from_secs(5)))?;
                let mut bytes = Vec::new();
                socket.read_to_end(&mut bytes)?;
                received.push(bytes.len());
            }
            Ok(received)
        });

        for kind in [CancelKind::User, CancelKind::Shutdown] {
            let runtime = build_io_runtime()?;
            runtime.block_on(async {
                let socket = TcpStream::connect(addr).await?;
                let (read, write) = transport::plain_split(socket);
                let mut connection = loopback_connection(read, write);
                let cx = Cx::current().expect("runtime installs an ambient Cx");
                cx.cancel_with(kind, Some("pre-start cancellation test"));

                let err = connection
                    .execute_with(
                        &cx,
                        Execute::new("begin null; end;").timeout(Duration::ZERO),
                    )
                    .await
                    .expect_err("an already-expired operation must not start");

                match kind {
                    CancelKind::User => {
                        assert!(
                            matches!(err, Error::Cancelled),
                            "user cancellation must remain distinct, got {err:?}"
                        );
                        assert!(
                            !connection.is_dead(),
                            "user cancellation before start leaves the connection reusable"
                        );
                        assert_eq!(
                            connection.core.recovery.phase(),
                            SessionRecoveryPhase::Ready
                        );
                    }
                    CancelKind::Shutdown => {
                        assert!(
                            matches!(err, Error::ConnectionClosed(_)),
                            "shutdown cancellation must close the connection, got {err:?}"
                        );
                        assert!(
                            connection.is_dead(),
                            "shutdown cancellation before start must mark the connection dead"
                        );
                        assert_eq!(connection.core.recovery.phase(), SessionRecoveryPhase::Dead);
                    }
                    _ => unreachable!("the test covers exactly User and Shutdown"),
                }

                drop(connection);
                Ok::<_, Error>(())
            })?;
        }

        let received = server.join().expect("server thread joins")?;
        assert_eq!(
            received,
            vec![0, 0],
            "before-start cancellation must emit neither a request nor BREAK"
        );
        Ok(())
    }

    // bead rust-oracledb-yhz: a compliant-but-non-minimal server may send
    // MULTIPLE RESET markers after the client's RESET (reference _reset second
    // loop, protocol.pyx:554-556). The drain must consume ALL of them and send
    // exactly ONE RESET. The pre-fix reset_after_marker returned on the first
    // RESET marker, so the caller read the second one, mistook it for a fresh
    // break, and sent a DUPLICATE RESET — poisoning the reused connection.
    #[test]
    fn reset_after_marker_drains_multiple_trailing_markers_no_duplicate_reset() {
        const INFLIGHT_BODY: &[u8] = &[0xDE, 0xAD];
        const ERROR_BODY: &[u8] = &[0x04, 0x01, 0x02];
        const FRESH_BODY: &[u8] = &[0x11, 0x22, 0x33];

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            use std::io::Write as _;

            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_BREAK,
                "client must send a BREAK marker first"
            );
            socket
                .write_all(&data_packet(INFLIGHT_BODY, true))
                .expect("write in-flight response");
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_BREAK))
                .expect("write break-ack marker");
            // The client answers the marker with exactly ONE RESET.
            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_RESET,
                "client must answer with a RESET"
            );
            // Server now sends TWO RESET markers (the yhz trigger) before the
            // trailing error + the fresh response.
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_RESET))
                .expect("write reset marker #1");
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_RESET))
                .expect("write reset marker #2");
            socket
                .write_all(&data_packet(ERROR_BODY, true))
                .expect("write trailing error packet");
            socket
                .write_all(&data_packet(FRESH_BODY, true))
                .expect("write fresh response");
            // No DUPLICATE RESET may arrive. With the bug the client answers the
            // second RESET marker with a second RESET, which (sent during the
            // drain) is already in our buffer by now.
            socket
                .set_read_timeout(Some(Duration::from_millis(750)))
                .expect("set short read timeout");
            let mut extra = [0u8; 11];
            if socket.read_exact(&mut extra).is_ok() {
                panic!(
                    "client sent a DUPLICATE marker (type {}): the drain did not \
                     consume all trailing RESET markers (bead rust-oracledb-yhz)",
                    extra[10]
                );
            }
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let next = runtime.block_on(async {
            let cx = Cx::current().expect("ambient Cx");
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (mut read, write) = transport::plain_split(stream);
            let write: SharedWriteHalf = Arc::new(AsyncMutex::with_name("yhz_test_write", write));
            break_and_drain_wire(&mut read, &write, Duration::from_secs(5))
                .await
                .expect("drain must succeed even with multiple RESET markers");
            read_data_response(&mut read, &cx, &write)
                .await
                .expect("next read after drain must decode cleanly")
        });

        assert_eq!(
            next, FRESH_BODY,
            "after draining multiple RESET markers the reused connection must read \
             the FRESH response"
        );
        server.join().expect("server thread joins");
    }

    // THE BUG (pre-fix contrast): if the timeout path sends ONLY a BREAK and
    // does NOT drain, the in-flight response tail is still sitting in the socket.
    // The next read_data_response then reassembles those STALE bytes as if they
    // were the next operation's response — the wire is desynced. This test pins
    // that broken behavior to prove the regression test above is meaningful: it
    // asserts that without the drain the next read returns the stale tail.
    #[test]
    fn break_without_drain_leaves_stale_bytes_for_next_read() {
        const STALE_BODY: &[u8] = &[0x53, 0x54, 0x41, 0x4c, 0x45]; // "STALE"
        const FRESH_BODY: &[u8] = &[0x11, 0x22, 0x33];

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            use std::io::Write as _;
            // Client sends BREAK (the only thing the OLD code did).
            assert_eq!(read_marker_type(&mut socket), TNS_MARKER_TYPE_BREAK);
            // The server's in-flight response (end-of-response) was already on
            // its way when the break fired: it lands in the socket unconsumed.
            socket
                .write_all(&data_packet(STALE_BODY, true))
                .expect("write stale in-flight response");
            // ... and then the fresh response the caller actually wanted.
            socket
                .write_all(&data_packet(FRESH_BODY, true))
                .expect("write fresh response");
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let first_read = runtime.block_on(async {
            let cx = Cx::current().expect("ambient Cx");
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (mut read, write) = transport::plain_split(stream);
            let write: SharedWriteHalf =
                Arc::new(AsyncMutex::with_name("nodrain_test_write", write));

            // Reproduce the OLD timeout path: send BREAK, do NOT drain.
            send_marker_shared(&cx, &write, TNS_MARKER_TYPE_BREAK)
                .await
                .expect("send break");

            // The very next read picks up the STALE in-flight response.
            read_data_response(&mut read, &cx, &write)
                .await
                .expect("read after bare break")
        });

        assert_eq!(
            first_read, STALE_BODY,
            "without the drain, the next read misframes onto the stale in-flight bytes — \
             this is the bug break_and_drain fixes"
        );
        server.join().expect("server thread joins");
    }

    // bead rust-oracledb-zhm: the DML-RETURNING error path (test_1600 test_1612,
    // ORA-12899) deadlocked. Confirmed by live wire trace against Oracle 23ai:
    // a RETURNING statement that errors does NOT come back as a plain DATA
    // response. The server signals it out-of-band, exactly as on a call-timeout
    // BREAK: it sends a BREAK marker, the client runs the RESET dance, and the
    // server then sends a FLUSH_OUT_BINDS *request* — a DATA packet whose data
    // flags are 0x0000 (NO end-of-response flag, because the break-recovery path
    // does not use request-boundary framing) and whose payload ends in the
    // FLUSH_OUT_BINDS message byte (0x13). The reference recognises this as
    // end-of-response while *processing* the message (messages/base.pyx:1267-1269
    // sets end_of_response on TNS_MSG_TYPE_FLUSH_OUT_BINDS) and replies with a
    // FLUSH_OUT_BINDS message; the server then sends another BREAK/RESET pair and
    // finally the real ORA-12899 error packet.
    //
    // THE BUG: our `read_data_response_boundary` fed the post-RESET trailing
    // packet back through `data_packet_ends_response`, which (correctly, for the
    // wide-row false-positive guard, bead n2s) returns false for a flagless
    // packet that merely *ends* in 0x13. So the boundary loop tried to read
    // another packet that the server never sends (it is waiting for our
    // FLUSH_OUT_BINDS reply) and we blocked forever in recvfrom/epoll.
    //
    // THE FIX: a packet that arrives *after a RESET* inside the boundary loop is
    // message-byte framed, not request-boundary framed (mirroring the reference,
    // whose `_check_request_boundary` is off for post-reset packets). So once the
    // loop has run a RESET, a trailing FLUSH_OUT_BINDS / END_OF_RESPONSE message
    // byte terminates the response. The wide-row guard is untouched because it
    // applies only to the normal (no-reset) framing.
    //
    // This hermetic test replays that exact sequence and drives the real
    // execute-path reader (`read_data_response_flushing_out_binds`). Pre-fix it
    // hangs at step (4) below; the bounded timeout converts the hang into a test
    // failure instead of stalling the whole suite.
    #[test]
    fn dml_returning_error_flush_out_binds_after_reset_completes_without_hang() {
        // The real ORA-12899 error payload tail (end-of-response flagged).
        const ERROR_BODY: &[u8] = &[0x04, 0x01, 0x02, 0x37];
        // The FLUSH_OUT_BINDS *request* the server sends after the first reset:
        // flagless DATA packet whose payload ends in the FLUSH_OUT_BINDS byte.
        // Matches the live trace body `07 00 00 13`.
        const FLUSH_REQUEST_BODY: &[u8] = &[0x07, 0x00, 0x00, TNS_MSG_TYPE_FLUSH_OUT_BINDS];

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            use std::io::Write as _;

            // 1) Server signals the RETURNING error out-of-band with a BREAK.
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_BREAK))
                .expect("write break marker");
            // 2) Client answers with RESET; server confirms with a RESET marker.
            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_RESET,
                "client must answer the BREAK with a RESET"
            );
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_RESET))
                .expect("write reset-confirm marker");
            // 3) Server sends the FLUSH_OUT_BINDS request: flagless DATA packet
            //    ending in 0x13. THIS is the packet the pre-fix loop refused to
            //    treat as a boundary, then blocked reading the next packet.
            socket
                .write_all(&data_packet(FLUSH_REQUEST_BODY, false))
                .expect("write flush-out-binds request");
            // 4) Client must reply with a FLUSH_OUT_BINDS message of its own
            //    (a DATA packet whose single-byte payload is 0x13).
            let mut header = [0u8; 8];
            socket
                .read_exact(&mut header)
                .expect("read flush-out-binds reply header");
            assert_eq!(
                header[4], TNS_PACKET_TYPE_DATA,
                "client's flush-out-binds reply must be a DATA packet"
            );
            let len = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
            let mut body = vec![0u8; len - 8];
            socket
                .read_exact(&mut body)
                .expect("read flush-out-binds reply body");
            assert_eq!(
                body.last().copied(),
                Some(TNS_MSG_TYPE_FLUSH_OUT_BINDS),
                "client must reply with a FLUSH_OUT_BINDS message"
            );
            // 5) Server sends another BREAK/RESET pair before the real error.
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_BREAK))
                .expect("write second break marker");
            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_RESET,
                "client must answer the second BREAK with a RESET"
            );
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_RESET))
                .expect("write second reset-confirm marker");
            // 6) Finally, the genuine ORA-12899 error packet (end-of-response).
            socket
                .write_all(&data_packet(ERROR_BODY, true))
                .expect("write trailing ORA-12899 error packet");
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let payload = runtime.block_on(async {
            let cx = Cx::current().expect("ambient Cx");
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (mut read, write) = transport::plain_split(stream);
            let write: SharedWriteHalf =
                Arc::new(AsyncMutex::with_name("returning_err_test_write", write));

            // Drive the real execute-path reader. Bound it so the pre-fix hang
            // surfaces as a timeout error rather than stalling the whole suite.
            time::timeout(
                time::wall_now(),
                Duration::from_secs(10),
                read_data_response_flushing_out_binds(&mut read, &cx, &write, 8192),
            )
            .await
            .expect("must NOT hang on the DML-RETURNING error path (flush-out-binds after reset)")
            .expect("read must complete and yield the trailing error payload")
        });

        // The fully reassembled response ends with the real error packet's bytes
        // (the FLUSH_OUT_BINDS request body is consumed/popped, not surfaced).
        assert!(
            payload.ends_with(ERROR_BODY),
            "the reassembled response must end with the ORA-12899 error payload, got {payload:?}"
        );
        server.join().expect("server thread joins");
    }

    // ---- explicit cancel (bead rust-oracledb-wnz) -----------------------------
    //
    // Connection::cancel() must do for an EXPLICIT user cancel exactly what the
    // call-timeout path does for a timeout: send the BREAK, drain the server's
    // in-flight response + the break-ack MARKER + the RESET handshake + the
    // trailing ORA-01013 error, then leave the wire at a clean boundary so the
    // SAME connection is reusable for the next operation. It reuses the proven
    // `break_and_drain_wire` machinery (no duplicate drain loop). The only thing
    // that differs from the timeout path is the surfaced error semantics: a
    // successful cancel is `Ok(())` (the connection is clean), and the in-flight
    // operation observes `Error::Cancelled` (ORA-01013 user-requested-cancel),
    // which — like DPY-4024 — is NOT connection-lost (the session survives).

    #[test]
    fn cancel_and_drain_wire_leaves_connection_reusable() {
        // The cancelled call's in-flight response (its own end-of-response): the
        // stale bytes that must be discarded, never mistaken for the next result.
        const INFLIGHT_BODY: &[u8] = &[0xCA, 0xFE];
        // The trailing ORA-01013-shaped error packet ending the cancel response.
        const ERROR_BODY: &[u8] = &[0x04, 0x01, 0x0d];
        // The genuine response to `select 7+5` on the reused connection.
        const FRESH_BODY: &[u8] = &[0x07, 0x05, 0x0c];

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            use std::io::Write as _;

            // 1) Client sends BREAK to cancel the in-flight slow query.
            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_BREAK,
                "cancel must send a BREAK marker first"
            );
            // 2) Server flushes the cancelled call's in-flight response FIRST.
            socket
                .write_all(&data_packet(INFLIGHT_BODY, true))
                .expect("write in-flight response");
            // 3) Server's break-ack MARKER drives the client's RESET dance.
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_BREAK))
                .expect("write break-ack marker");
            // 4) Client answers with RESET; server confirms.
            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_RESET,
                "cancel must answer the marker with a RESET"
            );
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_RESET))
                .expect("write reset-confirm marker");
            // 5) Trailing ORA-01013 error ending the cancel response.
            socket
                .write_all(&data_packet(ERROR_BODY, true))
                .expect("write trailing error packet");
            // 6) The FRESH response to the next operation (select 7+5 -> 12).
            socket
                .write_all(&data_packet(FRESH_BODY, true))
                .expect("write fresh response");
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let next = runtime.block_on(async {
            let cx = Cx::current().expect("ambient Cx");
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (mut read, write) = transport::plain_split(stream);
            let write: SharedWriteHalf =
                Arc::new(AsyncMutex::with_name("cancel_test_write", write));

            // The cancel: break + drain leaves the stream clean and reusable.
            cancel_and_drain_wire(&mut read, &write, Duration::from_secs(5))
                .await
                .expect("cancel drain must succeed and leave the stream clean");

            // The next operation reads its OWN response on the SAME connection.
            read_data_response(&mut read, &cx, &write)
                .await
                .expect("next read after cancel must decode cleanly")
        });

        assert_eq!(
            next, FRESH_BODY,
            "after cancel the reused connection must read the FRESH response, not the \
             stale in-flight response ({INFLIGHT_BODY:?}) or error body ({ERROR_BODY:?})"
        );
        server.join().expect("server thread joins");
    }

    // A bare `fetch_rows_request` whose paired `fetch_rows_ref_response` is never
    // consumed leaves the recovery phase `InFlight` with a stranded page on the
    // wire (no response future was dropped to fire a `CancelDrainGuard`).
    // `ensure_clean_before_request` must treat that exactly like a dropped fetch:
    // break + drain the stranded page and reconcile to `Ready`, so the next
    // operation reads its OWN response instead of the connection wedging forever
    // on "operation attempted while a response is still in flight". Offline
    // regression for the live `stranded_prefetch_request_is_drained_before_reuse`
    // proof (bead rust-oracledb-004o).
    #[test]
    fn ensure_clean_drains_inflight_bare_request_before_reuse() {
        // The stranded speculative page (its own end-of-response): stale bytes
        // that must be discarded, never mistaken for the next result.
        const INFLIGHT_BODY: &[u8] = &[0xCA, 0xFE];
        // The trailing ORA-01013-shaped error packet ending the drain response.
        const ERROR_BODY: &[u8] = &[0x04, 0x01, 0x0d];
        // The genuine response to the reuse operation.
        const FRESH_BODY: &[u8] = &[0x07, 0x05, 0x0c];

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            use std::io::Write as _;

            // The reuse op must break + drain the stranded page first. Without the
            // InFlight handling this BREAK never arrives (the op errors out) and
            // this read times out — the regression's failure mode.
            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_BREAK,
                "reuse after a bare request must send a BREAK to drain the stranded page"
            );
            socket
                .write_all(&data_packet(INFLIGHT_BODY, true))
                .expect("write stranded page");
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_BREAK))
                .expect("write break-ack marker");
            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_RESET,
                "drain must answer the break-ack marker with a RESET"
            );
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_RESET))
                .expect("write reset-confirm marker");
            socket
                .write_all(&data_packet(ERROR_BODY, true))
                .expect("write trailing error packet");
            socket
                .write_all(&data_packet(FRESH_BODY, true))
                .expect("write fresh response");
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let next = runtime
            .block_on(async {
                let cx = Cx::current().expect("ambient Cx");
                let stream = TcpStream::connect(addr).await.expect("connect to listener");
                let (read, write) = transport::plain_split(stream);
                let mut conn = loopback_connection(read, write);

                // Simulate a bare `fetch_rows_request`: a speculative response is
                // outstanding, so the phase is `InFlight` with no guard armed.
                conn.core
                    .recovery
                    .begin_operation()
                    .expect("enter InFlight");
                assert_eq!(conn.core.recovery.phase(), SessionRecoveryPhase::InFlight);

                // The next operation's pre-flight cleanup must reclaim the wire.
                conn.ensure_clean_before_request()
                    .await
                    .expect("a stranded bare request must be drained, not wedge the connection");
                assert_eq!(
                    conn.core.recovery.phase(),
                    SessionRecoveryPhase::Ready,
                    "after draining the stranded page the session is Ready for reuse"
                );

                conn.core.read_data_response(&cx).await
            })
            .expect("the reused connection must read its fresh response");

        assert_eq!(
            next, FRESH_BODY,
            "after draining the stranded bare request the reused connection must read \
             the FRESH response, not the stranded page ({INFLIGHT_BODY:?})"
        );
        server.join().expect("server thread joins");
    }

    // The two-thread cancel path (a `CancelHandle` on another thread already
    // sent the BREAK while the main thread is blocked in the query) needs a
    // DRAIN-ONLY clean-up: it must NOT send a second BREAK, but it must still
    // consume the in-flight response + break-ack MARKER + RESET handshake +
    // trailing ORA-01013, exactly like the full break+drain. `drain_cancel_wire`
    // is that drain-only half, sharing `drain_break_response_recovery` with the
    // break+drain path. The server here sends NO marker before its in-flight
    // response is flushed and does NOT expect a BREAK from us (the handle thread
    // already sent it) — only the RESET in answer to the break-ack marker.
    #[test]
    fn drain_cancel_wire_drains_without_sending_a_break() {
        const INFLIGHT_BODY: &[u8] = &[0xCA, 0xFE];
        const ERROR_BODY: &[u8] = &[0x04, 0x01, 0x0d];
        const FRESH_BODY: &[u8] = &[0x07, 0x05, 0x0c];

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            use std::io::Write as _;

            // The drain-only path sends NO BREAK first (the handle thread did).
            // Server flushes the in-flight response then the break-ack MARKER.
            socket
                .write_all(&data_packet(INFLIGHT_BODY, true))
                .expect("write in-flight response");
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_BREAK))
                .expect("write break-ack marker");
            // The drain answers the marker with a RESET.
            assert_eq!(
                read_marker_type(&mut socket),
                TNS_MARKER_TYPE_RESET,
                "drain must answer the break-ack marker with a RESET"
            );
            socket
                .write_all(&marker_packet(TNS_MARKER_TYPE_RESET))
                .expect("write reset-confirm marker");
            socket
                .write_all(&data_packet(ERROR_BODY, true))
                .expect("write trailing error packet");
            socket
                .write_all(&data_packet(FRESH_BODY, true))
                .expect("write fresh response");
            // No BREAK marker may ever arrive on this path.
            socket
                .set_read_timeout(Some(Duration::from_millis(500)))
                .expect("set short read timeout");
            let mut extra = [0u8; 11];
            if let Ok(()) = socket.read_exact(&mut extra) {
                assert_ne!(
                    extra[10], TNS_MARKER_TYPE_BREAK,
                    "drain-only cancel must NOT send a BREAK marker"
                );
            }
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let next = runtime.block_on(async {
            let cx = Cx::current().expect("ambient Cx");
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (mut read, write) = transport::plain_split(stream);
            let write: SharedWriteHalf =
                Arc::new(AsyncMutex::with_name("drain_cancel_test_write", write));

            drain_cancel_wire(&mut read, &write, Duration::from_secs(5))
                .await
                .expect("drain-only cancel must succeed and leave the stream clean");

            read_data_response(&mut read, &cx, &write)
                .await
                .expect("next read after drain must decode cleanly")
        });

        assert_eq!(
            next, FRESH_BODY,
            "after drain-only cancel the reused connection must read the FRESH response"
        );
        server.join().expect("server thread joins");
    }

    // The Scope-based cancel-on-drop guard. While a cancellable round trip's
    // read future is in flight the guard owns the InFlight recovery phase; if the
    // future is dropped (cancelled — e.g. by a `select!`/`timeout` racing it)
    // before the read completes, the guard's Drop moves the phase to BreakSent,
    // so the NEXT operation on the connection breaks + drains the stranded
    // server call rather than reassembling its leftover bytes as its own
    // response. A CLEAN completion calls `disarm()` first, so a normal read
    // returns the phase to Ready.
    #[test]
    fn cancel_drain_guard_transitions_recovery_phase_only_when_dropped_in_flight() -> Result<()> {
        let recovery = Arc::new(SessionRecovery::new());

        // A guard dropped WITHOUT disarming (the future was cancelled mid-read):
        // the recovery phase records that a break/drain is required.
        {
            let _guard = CancelDrainGuard::arm(Arc::clone(&recovery))?;
            assert_eq!(recovery.phase(), SessionRecoveryPhase::InFlight);
        }
        assert_eq!(
            recovery.phase(),
            SessionRecoveryPhase::BreakSent,
            "dropping an armed guard (cancelled in flight) must require recovery"
        );
        assert!(recovery.begin_pending_drain()?);
        assert_eq!(recovery.phase(), SessionRecoveryPhase::Draining);
        assert!(
            recovery.begin_pending_drain().is_err(),
            "a drain that is already running must not start a second drain"
        );
        recovery.finish_drain_ready();
        assert_eq!(recovery.phase(), SessionRecoveryPhase::Ready);

        // Strict operation starts require Ready; a second operation cannot start
        // while an earlier response is still in flight.
        recovery.begin_operation()?;
        match recovery.begin_operation() {
            Err(Error::ConnectionClosed(message)) => {
                assert!(message.contains("still in flight"));
            }
            other => {
                return Err(Error::Runtime(format!(
                    "second operation start should fail while InFlight, got {other:?}"
                )));
            }
        }
        recovery.complete_operation();
        assert_eq!(recovery.phase(), SessionRecoveryPhase::Ready);

        // A response reader may adopt an already-sent prefetch request's
        // InFlight phase and complete it back to Ready.
        recovery.begin_operation()?;
        {
            let mut guard = CancelDrainGuard::arm(Arc::clone(&recovery))?;
            assert_eq!(recovery.phase(), SessionRecoveryPhase::InFlight);
            guard.disarm();
        }
        assert_eq!(recovery.phase(), SessionRecoveryPhase::Ready);

        // A guard that DISARMS before drop (the read completed normally): the
        // phase returns to Ready and no recovery is needed.
        {
            let mut guard = CancelDrainGuard::arm(Arc::clone(&recovery))?;
            assert_eq!(recovery.phase(), SessionRecoveryPhase::InFlight);
            guard.disarm();
        }
        assert_eq!(
            recovery.phase(),
            SessionRecoveryPhase::Ready,
            "a disarmed guard (clean completion) must NOT require recovery"
        );
        Ok(())
    }

    #[test]
    fn cancelled_error_is_not_connection_lost_but_is_transient() {
        let cancelled = Error::Cancelled;
        assert!(
            !cancelled.is_connection_lost(),
            "a user cancel leaves the session alive (ORA-01013 / DPY-4024 semantics)"
        );
        assert!(
            cancelled.is_transient(),
            "a cancelled operation may be retried on the same clean connection"
        );
        // ORA-01013 is the server-side code for user-requested cancel.
        assert_eq!(cancelled.ora_code(), Some(1013));
    }

    // ------------------------------------------------------------------
    // Listener REDIRECT handling (bead rust-oracledb-pre23ai-connect-z47u.6)
    // ------------------------------------------------------------------

    /// Reads one TNS packet from a test-listener socket, returning
    /// `(packet_type, packet_flags, payload)`.
    fn read_tns_packet_sync(
        socket: &mut std::net::TcpStream,
    ) -> std::io::Result<(u8, u8, Vec<u8>)> {
        let mut header = [0u8; 8];
        socket.read_exact(&mut header)?;
        let declared = usize::from(u16::from_be_bytes([header[0], header[1]]));
        let mut payload = vec![0u8; declared.saturating_sub(header.len())];
        socket.read_exact(&mut payload)?;
        Ok((header[4], header[5], payload))
    }

    fn send_tns_packet_sync(
        socket: &mut std::net::TcpStream,
        packet_type: u8,
        payload: &[u8],
    ) -> std::io::Result<()> {
        use std::io::Write as _;
        let packet = encode_packet(packet_type, 0, None, payload, PacketLengthWidth::Legacy16)
            .expect("encode test packet");
        socket.write_all(&packet)
    }

    /// The REDIRECT packet payload for `redirect_data`: u16be length prefix
    /// followed by the data bytes.
    fn redirect_packet_payload(redirect_data: &str) -> Vec<u8> {
        let bytes = redirect_data.as_bytes();
        let mut payload = Vec::with_capacity(2 + bytes.len());
        payload.extend_from_slice(
            &u16::try_from(bytes.len())
                .expect("test data fits")
                .to_be_bytes(),
        );
        payload.extend_from_slice(bytes);
        payload
    }

    #[test]
    fn redirect_payload_prefix_accepts_inline_partial_and_rejects_truncated() {
        // Well-formed: length prefix + full inline data.
        let payload = redirect_packet_payload("abc");
        assert_eq!(redirect_payload_prefix(&payload).unwrap(), (3, &b"abc"[..]));
        // Declared length exceeding the inline bytes: the remainder arrives in
        // follow-up packets; the prefix reports what is available.
        assert_eq!(
            redirect_payload_prefix(&[0x00, 0x10, b'x']).unwrap(),
            (16, &b"x"[..])
        );
        // Length-only payload (data entirely in follow-up packets).
        assert_eq!(
            redirect_payload_prefix(&[0x00, 0x05]).unwrap(),
            (5, &[][..])
        );
        // Trailing bytes beyond the declared length are ignored.
        assert_eq!(
            redirect_payload_prefix(&[0x00, 0x01, b'a', b'b']).unwrap(),
            (1, &b"a"[..])
        );
        // Truncated: too short to even carry the u16 length.
        for bad in [&[][..], &[0x00][..]] {
            assert!(
                matches!(
                    redirect_payload_prefix(bad),
                    Err(Error::InvalidRedirectData(_))
                ),
                "payload {bad:?} must be rejected as malformed"
            );
        }
    }

    #[test]
    fn parse_redirect_target_well_formed_and_malformed() {
        // Well-formed: "<address>\0<connect data>".
        let data = "(ADDRESS=(PROTOCOL=tcp)(HOST=dispatcher.example)(PORT=1621))\0\
                    (DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=dispatcher.example)(PORT=1621))\
                    (CONNECT_DATA=(SERVICE_NAME=svc)))";
        let target = parse_redirect_target(data, NetProtocol::Tcp).expect("well-formed redirect");
        assert_eq!(target.host, "dispatcher.example");
        assert_eq!(target.port, 1621);
        assert!(target.connect_data.starts_with("(DESCRIPTION="));
        // Missing the NUL separator between address and connect data.
        assert!(matches!(
            parse_redirect_target("(ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1))", NetProtocol::Tcp),
            Err(Error::InvalidRedirectData(_))
        ));
        // Address part with no usable HOST.
        assert!(matches!(
            parse_redirect_target("(ADDRESS=(PROTOCOL=tcp)(PORT=1621))\0x", NetProtocol::Tcp),
            Err(Error::InvalidRedirectData(_))
        ));
        // Unparseable address part.
        assert!(matches!(
            parse_redirect_target("(((\0x", NetProtocol::Tcp),
            Err(Error::InvalidRedirectData(_))
        ));
    }

    /// `Error::RedirectUnsupported` is kept ONLY for a redirect that demands a
    /// transport protocol change: a `tcps` connect is never downgraded to
    /// plain `tcp` (silent TLS strip), and a mid-connect `tcp` -> `tcps`
    /// upgrade is not supported. Same-protocol redirects are followed.
    #[test]
    fn parse_redirect_target_refuses_transport_protocol_change() {
        let tcp_addr = "(ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1521))\0cd";
        let tcps_addr = "(ADDRESS=(PROTOCOL=tcps)(HOST=h)(PORT=2484))\0cd";
        let no_protocol = "(ADDRESS=(HOST=h)(PORT=1521))\0cd";
        // tcps -> tcp downgrade refused, whether explicit or (fail closed)
        // because the redirect omitted the protocol.
        assert!(matches!(
            parse_redirect_target(tcp_addr, NetProtocol::Tcps),
            Err(Error::RedirectUnsupported)
        ));
        assert!(matches!(
            parse_redirect_target(no_protocol, NetProtocol::Tcps),
            Err(Error::RedirectUnsupported)
        ));
        // tcp -> tcps upgrade is not supported mid-connect.
        assert!(matches!(
            parse_redirect_target(tcps_addr, NetProtocol::Tcp),
            Err(Error::RedirectUnsupported)
        ));
        // Protocol preserved: followed.
        assert!(parse_redirect_target(tcps_addr, NetProtocol::Tcps).is_ok());
        assert!(parse_redirect_target(no_protocol, NetProtocol::Tcp).is_ok());
        assert!(parse_redirect_target(tcp_addr, NetProtocol::Tcp).is_ok());
    }

    /// End-to-end redirect through real sockets: the first listener answers
    /// CONNECT with REDIRECT; the driver must reconnect to the redirected
    /// address and resend CONNECT there carrying TNS_PACKET_FLAG_REDIRECT and
    /// the redirect-supplied connect data. The target listener REFUSEs so the
    /// test ends before authentication; the surfaced ListenerRefused proves
    /// the whole redirect hop executed.
    #[test]
    fn connect_follows_listener_redirect_and_flags_the_new_connect() -> Result<()> {
        let redirect_listener = TcpListener::bind("127.0.0.1:0").expect("bind redirect listener");
        let target_listener = TcpListener::bind("127.0.0.1:0").expect("bind target listener");
        let redirect_addr = redirect_listener.local_addr().expect("redirect addr");
        let target_addr = target_listener.local_addr().expect("target addr");
        let redirect_connect_data = format!(
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT={}))\
             (CONNECT_DATA=(SERVICE_NAME=redirsvc)))",
            target_addr.port()
        );
        let redirect_data = format!(
            "(ADDRESS=(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT={}))\0{}",
            target_addr.port(),
            redirect_connect_data
        );

        let first = thread::spawn(move || -> std::io::Result<(u8, u8)> {
            let (mut socket, _) = redirect_listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(5)))?;
            let (packet_type, packet_flags, _payload) = read_tns_packet_sync(&mut socket)?;
            send_tns_packet_sync(
                &mut socket,
                TNS_PACKET_TYPE_REDIRECT,
                &redirect_packet_payload(&redirect_data),
            )?;
            Ok((packet_type, packet_flags))
        });
        let expected_connect_data = redirect_connect_data;
        let second = thread::spawn(move || -> std::io::Result<(u8, u8, bool)> {
            let (mut socket, _) = target_listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(5)))?;
            let (packet_type, packet_flags, payload) = read_tns_packet_sync(&mut socket)?;
            let carries_connect_data = payload
                .windows(expected_connect_data.len())
                .any(|window| window == expected_connect_data.as_bytes());
            send_tns_packet_sync(&mut socket, TNS_PACKET_TYPE_REFUSE, b"(ERR=12514)")?;
            Ok((packet_type, packet_flags, carries_connect_data))
        });

        let options = ConnectOptions::new(
            format!("127.0.0.1:{}/redirsvc", redirect_addr.port()),
            "user",
            "password",
            identity(),
        );
        let runtime = build_io_runtime().expect("asupersync runtime");
        let err = runtime
            .block_on(async {
                let cx = Cx::current().expect("ambient Cx");
                Connection::connect(&cx, options).await
            })
            .expect_err("target listener refuses; the refusal must surface");
        assert!(
            matches!(&err, Error::ListenerRefused(msg) if msg.contains("ERR=12514")),
            "expected the TARGET listener's refusal, got {err:?}"
        );
        let (first_type, first_flags) = first.join().expect("redirect listener thread")?;
        assert_eq!(first_type, TNS_PACKET_TYPE_CONNECT);
        assert_eq!(first_flags, 0, "initial CONNECT carries no redirect flag");
        let (second_type, second_flags, carries_connect_data) =
            second.join().expect("target listener thread")?;
        assert_eq!(second_type, TNS_PACKET_TYPE_CONNECT);
        assert_eq!(
            second_flags, TNS_PACKET_FLAG_REDIRECT,
            "the CONNECT resent to the redirect target must carry the REDIRECT packet flag"
        );
        assert!(
            carries_connect_data,
            "the redirected CONNECT must carry the redirect-supplied connect data"
        );
        Ok(())
    }

    /// Redirect/RESEND interplay plus chunked redirect data: the first
    /// listener asks for a RESEND before redirecting, and its REDIRECT packet
    /// carries only the u16 length (the data follows in a second REDIRECT
    /// packet). The redirected listener then ALSO asks for a RESEND — the
    /// resent CONNECT on the redirected connection must still carry the
    /// REDIRECT packet flag (reference keeps the flag on the recreated
    /// ConnectMessage across resends).
    #[test]
    fn connect_redirect_interleaves_with_resend_and_chunked_redirect_data() -> Result<()> {
        let redirect_listener = TcpListener::bind("127.0.0.1:0").expect("bind redirect listener");
        let target_listener = TcpListener::bind("127.0.0.1:0").expect("bind target listener");
        let redirect_addr = redirect_listener.local_addr().expect("redirect addr");
        let target_addr = target_listener.local_addr().expect("target addr");
        let redirect_data = format!(
            "(ADDRESS=(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT={port}))\0\
             (DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT={port}))\
             (CONNECT_DATA=(SERVICE_NAME=redirsvc)))",
            port = target_addr.port()
        );

        let first = thread::spawn(move || -> std::io::Result<()> {
            let (mut socket, _) = redirect_listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(5)))?;
            let _ = read_tns_packet_sync(&mut socket)?;
            // Ask for a resend BEFORE redirecting (pre-23ai listeners resend
            // routinely).
            send_tns_packet_sync(&mut socket, TNS_PACKET_TYPE_RESEND, &[])?;
            let _ = read_tns_packet_sync(&mut socket)?;
            // Chunked redirect: length-only REDIRECT packet, data in a
            // follow-up REDIRECT packet.
            let length = u16::try_from(redirect_data.len()).expect("test redirect data fits u16");
            send_tns_packet_sync(&mut socket, TNS_PACKET_TYPE_REDIRECT, &length.to_be_bytes())?;
            send_tns_packet_sync(
                &mut socket,
                TNS_PACKET_TYPE_REDIRECT,
                redirect_data.as_bytes(),
            )?;
            Ok(())
        });
        let second = thread::spawn(move || -> std::io::Result<(u8, u8)> {
            let (mut socket, _) = target_listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(5)))?;
            let (_, first_flags, _) = read_tns_packet_sync(&mut socket)?;
            // The redirected listener itself asks for a resend; the resent
            // CONNECT must still be redirect-flagged.
            send_tns_packet_sync(&mut socket, TNS_PACKET_TYPE_RESEND, &[])?;
            let (_, resent_flags, _) = read_tns_packet_sync(&mut socket)?;
            send_tns_packet_sync(&mut socket, TNS_PACKET_TYPE_REFUSE, b"(ERR=12514)")?;
            Ok((first_flags, resent_flags))
        });

        let options = ConnectOptions::new(
            format!("127.0.0.1:{}/redirsvc", redirect_addr.port()),
            "user",
            "password",
            identity(),
        );
        let runtime = build_io_runtime().expect("asupersync runtime");
        let err = runtime
            .block_on(async {
                let cx = Cx::current().expect("ambient Cx");
                Connection::connect(&cx, options).await
            })
            .expect_err("target listener refuses; the refusal must surface");
        assert!(
            matches!(&err, Error::ListenerRefused(msg) if msg.contains("ERR=12514")),
            "expected the TARGET listener's refusal, got {err:?}"
        );
        first.join().expect("redirect listener thread")?;
        let (first_flags, resent_flags) = second.join().expect("target listener thread")?;
        assert_eq!(
            first_flags, TNS_PACKET_FLAG_REDIRECT,
            "redirected CONNECT must be flagged"
        );
        assert_eq!(
            resent_flags, TNS_PACKET_FLAG_REDIRECT,
            "a RESEND on the redirected connection must keep the redirect flag"
        );
        Ok(())
    }

    /// A listener that answers every CONNECT with another REDIRECT (here: to
    /// itself) must terminate with a structured error instead of spinning
    /// forever — never a hang, never an opaque I/O error.
    #[test]
    fn connect_redirect_loop_is_bounded() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let redirect_data = format!(
            "(ADDRESS=(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT={port}))\0\
             (DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT={port}))\
             (CONNECT_DATA=(SERVICE_NAME=loopsvc)))",
            port = addr.port()
        );
        let hops = u32::from(MAX_CONNECT_REDIRECT_ROUNDS) + 1;
        let server = thread::spawn(move || -> std::io::Result<u32> {
            let mut served = 0;
            // Initial connection plus MAX redirected reconnects, each answered
            // with a self-redirect.
            for _ in 0..hops {
                let (mut socket, _) = listener.accept()?;
                socket.set_read_timeout(Some(Duration::from_secs(5)))?;
                let _ = read_tns_packet_sync(&mut socket)?;
                send_tns_packet_sync(
                    &mut socket,
                    TNS_PACKET_TYPE_REDIRECT,
                    &redirect_packet_payload(&redirect_data),
                )?;
                served += 1;
            }
            Ok(served)
        });

        let options = ConnectOptions::new(
            format!("127.0.0.1:{}/loopsvc", addr.port()),
            "user",
            "password",
            identity(),
        );
        let runtime = build_io_runtime().expect("asupersync runtime");
        let err = runtime
            .block_on(async {
                let cx = Cx::current().expect("ambient Cx");
                Connection::connect(&cx, options).await
            })
            .expect_err("a redirect loop must terminate with a structured error");
        assert!(
            matches!(err, Error::ConnectRedirectLoop(rounds)
                if rounds == MAX_CONNECT_REDIRECT_ROUNDS + 1),
            "expected ConnectRedirectLoop, got {err:?}"
        );
        assert_eq!(err.kind(), ErrorKind::Protocol);
        let served = server.join().expect("listener thread")?;
        assert_eq!(served, hops, "every hop reached the listener");
        Ok(())
    }

    // ------------------------------------------------------------------
    // Packet-layer vs TTC-layer error labelling
    // (bead rust-oracledb-pre23ai-connect-z47u.3)
    // ------------------------------------------------------------------

    /// The packet-layer error text is self-triaging: it names known TNS
    /// packet types so a stray RESEND is never mistaken for TTC message 11
    /// (IO_VECTOR) — the mislabel that once steered a triage session toward
    /// SDU-reassembly hypotheses.
    #[test]
    fn unexpected_packet_error_names_tns_packet_types() {
        assert_eq!(
            Error::UnexpectedPacket(TNS_PACKET_TYPE_RESEND).to_string(),
            "unexpected TNS packet type 11 (RESEND)"
        );
        assert_eq!(
            Error::UnexpectedPacket(99).to_string(),
            "unexpected TNS packet type 99 (unknown)"
        );
    }

    /// The flag-framed boundary reader's non-DATA arm reports the NETWORK
    /// packet type byte (header offset 4) as `Error::UnexpectedPacket`, not
    /// as a TTC `UnknownMessageType` (which names application-layer message
    /// types and previously mislabelled this byte with `position: 4`).
    #[test]
    fn flag_framed_reader_labels_non_data_packet_as_packet_layer_error() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            use std::io::Write as _;
            let packet = encode_packet(
                TNS_PACKET_TYPE_RESEND,
                0,
                None,
                &[],
                PacketLengthWidth::Large32,
            )
            .expect("encode unexpected packet");
            socket.write_all(&packet).expect("write unexpected packet");
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let err = runtime.block_on(async {
            let cx = Cx::current().expect("ambient Cx");
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (read, write) = transport::plain_split(stream);
            let mut conn = loopback_connection(read, write);
            conn.core
                .read_data_response(&cx)
                .await
                .expect_err("non-DATA packet must fail closed")
        });

        assert!(
            matches!(err, Error::UnexpectedPacket(TNS_PACKET_TYPE_RESEND)),
            "expected the packet-layer error, got {err:?}"
        );
        assert!(
            err.to_string().contains("TNS packet type 11 (RESEND)"),
            "error text must name the TNS packet type: {err}"
        );
        server.join().expect("server thread joins");
        Ok(())
    }

    /// The break-drain reader (phase A: discarding in-flight responses until
    /// the break-acknowledge MARKER) likewise reports a stray non-DATA /
    /// non-MARKER packet as `Error::UnexpectedPacket` — a packet-layer byte,
    /// not a TTC message type.
    #[test]
    fn break_drain_labels_non_data_packet_as_packet_layer_error() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept test client");
            use std::io::Write as _;
            let packet = encode_packet(
                TNS_PACKET_TYPE_ACCEPT,
                0,
                None,
                &[0x00],
                PacketLengthWidth::Large32,
            )
            .expect("encode unexpected packet");
            socket.write_all(&packet).expect("write unexpected packet");
        });

        let runtime = build_io_runtime().expect("asupersync runtime");
        let err = runtime.block_on(async {
            let stream = TcpStream::connect(addr).await.expect("connect to listener");
            let (mut read, write) = transport::plain_split(stream);
            let write = Arc::new(AsyncMutex::with_name("break_drain_test_write", write));
            drain_break_response_recovery(&mut read, &write)
                .await
                .expect_err("non-DATA/non-MARKER packet must fail closed")
        });

        assert!(
            matches!(err, Error::UnexpectedPacket(TNS_PACKET_TYPE_ACCEPT)),
            "expected the packet-layer error, got {err:?}"
        );
        assert!(
            err.to_string().contains("TNS packet type 2 (ACCEPT)"),
            "error text must name the TNS packet type: {err}"
        );
        server.join().expect("server thread joins");
        Ok(())
    }
}
