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
//! The [`pool`] module provides async [`Pool`](pool::Pool) and
//! [`BlockingPool`](pool::BlockingPool) facades that mirror python-oracledb's
//! thin pool: free/busy lists, growth planning, getmode semantics, ping policy,
//! idle timeout, and max lifetime. The pool is generic over a
//! [`PoolBackend`](pool::PoolBackend) so the embedder supplies how a pooled
//! connection is created, pinged, and closed.
#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::num::NonZeroU32;
use std::pin::pin;
use std::process;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake};
use std::time::{Duration, Instant};

use asupersync::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use asupersync::net::TcpStream;
use asupersync::runtime::{reactor, Runtime, RuntimeBuilder};
use asupersync::sync::Mutex as AsyncMutex;
use asupersync::types::{CancelKind, CancelReason};
use asupersync::{time, Cx};
use oracledb_protocol::thin::aq::{
    build_aq_array_deq_payload, build_aq_array_enq_payload, build_aq_deq_payload,
    build_aq_enq_payload, parse_aq_array_response_with_limits, parse_aq_deq_response_with_limits,
    parse_aq_enq_response_with_limits, AqArrayResult, AqDeqOptions, AqDeqResult, AqEnqOptions,
    AqMsgProps, AqQueueDesc,
};
use oracledb_protocol::thin::{
    adjust_refetch_metadata, build_auth_phase_two_payload_with_proxy_with_seq,
    build_begin_pipeline_piggyback, build_change_password_payload_with_seq,
    build_connect_packet_payload, build_define_fetch_payload_with_seq,
    build_end_pipeline_payload_with_seq, build_execute_payload_with_bind_rows_and_options_with_seq,
    build_execute_payload_with_bind_rows_with_seq_and_token, build_fast_auth_phase_one_payload,
    build_fast_auth_token_payload, build_fetch_payload_with_seq, build_function_payload_with_seq,
    build_function_payload_with_seq_and_token, build_lob_create_temp_payload_with_seq,
    build_lob_free_temp_payload_with_seq, build_lob_read_payload_with_seq,
    build_lob_trim_payload_with_seq, build_lob_write_payload_with_seq, parse_accept_payload,
    parse_auth_response_with_limits, parse_define_fetch_response_with_context_and_limits,
    parse_fetch_response_with_context_and_limits, parse_lob_create_temp_response_with_limits,
    parse_lob_free_temp_response_with_limits, parse_lob_read_response_with_limits,
    parse_lob_trim_response_with_limits, parse_lob_write_response_with_limits,
    parse_plain_function_response_with_limits, parse_query_response_borrowed_with_limits,
    parse_query_response_with_binds_options_columns_and_limits,
    parse_tpc_txn_switch_response_with_limits, BatchServerError, BindValue, BorrowedFetchResult,
    ClientCapabilities, ColumnMetadata, CursorValue, ExecuteOptions, LobReadResult, QueryResult,
    QueryValue, QueryValueRef, SessionlessTxnState, TpcChangeStateResponse, TpcSwitchResponse,
    TpcXid, TNS_DATA_FLAGS_BEGIN_PIPELINE, TNS_DATA_FLAGS_END_OF_REQUEST,
    TNS_FETCH_ORIENTATION_ABSOLUTE, TNS_FETCH_ORIENTATION_CURRENT, TNS_FETCH_ORIENTATION_FIRST,
    TNS_FETCH_ORIENTATION_LAST, TNS_FETCH_ORIENTATION_NEXT, TNS_FETCH_ORIENTATION_PRIOR,
    TNS_FETCH_ORIENTATION_RELATIVE, TNS_FUNC_COMMIT, TNS_FUNC_LOGOFF, TNS_FUNC_PING,
    TNS_FUNC_ROLLBACK, TNS_MSG_TYPE_END_OF_RESPONSE, TNS_MSG_TYPE_FLUSH_OUT_BINDS,
    TNS_PACKET_TYPE_ACCEPT, TNS_PACKET_TYPE_CONNECT, TNS_PACKET_TYPE_DATA,
    TNS_PACKET_TYPE_REDIRECT, TNS_PACKET_TYPE_REFUSE, TNS_PIPELINE_MODE_ABORT_ON_ERROR,
    TNS_PIPELINE_MODE_CONTINUE_ON_ERROR, TNS_TPC_TXN_ABORT, TNS_TPC_TXN_COMMIT, TNS_TPC_TXN_DETACH,
    TNS_TPC_TXN_POST_DETACH, TNS_TPC_TXN_PREPARE, TNS_TPC_TXN_START, TNS_TPC_TXN_STATE_ABORTED,
    TNS_TPC_TXN_STATE_COMMITTED, TNS_TPC_TXN_STATE_FORGOTTEN, TNS_TPC_TXN_STATE_PREPARE,
    TNS_TPC_TXN_STATE_READ_ONLY, TNS_TPC_TXN_STATE_REQUIRES_COMMIT, TPC_TXN_FLAGS_NEW,
    TPC_TXN_FLAGS_RESUME, TPC_TXN_FLAGS_SESSIONLESS,
};
use oracledb_protocol::thin::{
    build_notify_payload_with_seq, build_subscribe_payload_with_seq,
    check_notification_header_with_limits, parse_subscribe_response_with_limits,
    try_parse_oac_record_with_limits, NotificationRecord, SubscribeResult, TNS_SUBSCR_OP_REGISTER,
    TNS_SUBSCR_OP_UNREGISTER,
};
use oracledb_protocol::thin::{
    build_sessionless_piggyback, build_tpc_change_state_payload_with_seq,
    build_tpc_switch_payload_with_seq, build_tpc_txn_switch_payload_with_seq,
    parse_tpc_change_state_response_with_limits, parse_tpc_switch_response_with_limits,
};
use oracledb_protocol::thin::{TNS_AQ_ARRAY_DEQ, TNS_AQ_ARRAY_ENQ};
use oracledb_protocol::wire::{encode_packet, PacketLengthWidth, ProtocolLimits};
use oracledb_protocol::{net::EasyConnect, ClientIdentity};

const PYTHON_ORACLEDB_COMPAT_VERSION_NUM: u32 = 0x0400_1000;
const DEFAULT_SDU: usize = 8192;
const TNS_DATA_PACKET_OVERHEAD: usize = 10;

pub use oracledb_protocol as protocol;

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
pub mod pool;
#[cfg(feature = "soda")]
pub mod soda;
mod sql_convert;
pub mod tls;
pub mod transport;

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

pub use sql_convert::{
    BindError, ConversionError, FromRow, FromSql, IntoBinds, Params, QueryResultExt, ToSql,
    TypedRow,
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
/// [`ClientIdentity`](protocol::ClientIdentity) the database records, the value
/// types it binds and reads back ([`BindValue`](protocol::thin::BindValue) /
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
}

impl<T: WireTransport> ConnectionCore<T> {
    fn from_halves(read: T::Read, write: T::Write, write_name: &'static str) -> Self {
        Self {
            read: Some(read),
            write: Arc::new(AsyncMutex::with_name(write_name, write)),
            recovery: Arc::new(SessionRecovery::new()),
            protocol_limits: ProtocolLimits::DEFAULT,
        }
    }

    fn set_protocol_limits(&mut self, limits: ProtocolLimits) -> Result<()> {
        self.protocol_limits = limits.validate()?;
        Ok(())
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
        let result = read_packet_with_limits(self.read_mut()?, width, limits).await;
        self.note_post_sync_result(result)
    }

    async fn read_data_response(&mut self, cx: &Cx) -> Result<Vec<u8>> {
        let write = Arc::clone(&self.write);
        let limits = self.protocol_limits;
        let result = read_data_response_with_limits(self.read_mut()?, cx, &write, limits).await;
        self.note_post_sync_result(result)
    }

    async fn read_data_response_boundary(
        &mut self,
        cx: &Cx,
        in_pipeline: bool,
    ) -> Result<DataResponse> {
        let write = Arc::clone(&self.write);
        let limits = self.protocol_limits;
        let result = read_data_response_boundary_with_limits(
            self.read_mut()?,
            cx,
            &write,
            in_pipeline,
            limits,
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
        let result = read_data_response_flushing_out_binds_with_limits(
            self.read_mut()?,
            cx,
            &write,
            sdu,
            limits,
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

#[derive(Clone, Copy, Debug)]
enum RecoveryWireAction {
    BreakAndDrain,
    DrainCancel,
}

impl RecoveryWireAction {
    fn timeout_message(self) -> &'static str {
        match self {
            Self::BreakAndDrain => "socket timed out while recovering from previous call timeout",
            Self::DrainCancel => "socket timed out while draining cancel response",
        }
    }

    fn wire_error_prefix(self) -> &'static str {
        match self {
            Self::BreakAndDrain => "wire error while recovering from call timeout",
            Self::DrainCancel => "wire error while draining cancel response",
        }
    }
}

struct RecoveryThreadWaker {
    thread: std::thread::Thread,
}

impl Wake for RecoveryThreadWaker {
    fn wake(self: Arc<Self>) {
        self.thread.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.thread.unpark();
    }
}

fn block_on_recovery_deadline<F>(future: F, recovery_timeout: Duration) -> Option<F::Output>
where
    F: Future,
{
    let start = Instant::now();
    let deadline = start.checked_add(recovery_timeout).unwrap_or(start);
    let waker = std::task::Waker::from(Arc::new(RecoveryThreadWaker {
        thread: std::thread::current(),
    }));
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);

    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return Some(output),
            Poll::Pending => {
                let now = Instant::now();
                if now >= deadline {
                    return None;
                }
                std::thread::park_timeout((deadline - now).min(Duration::from_millis(10)));
            }
        }
    }
}

fn classify_recovery_result(action: RecoveryWireAction, result: Option<Result<()>>) -> Result<()> {
    match result {
        Some(Ok(())) => Ok(()),
        Some(Err(Error::ConnectionClosed(message))) => Err(Error::ConnectionClosed(message)),
        Some(Err(err)) => Err(Error::ConnectionClosed(format!(
            "{}: {err}",
            action.wire_error_prefix()
        ))),
        None => Err(Error::ConnectionClosed(
            action.timeout_message().to_string(),
        )),
    }
}

fn run_recovery_without_current_cx<R, W>(
    read: &mut R,
    write: &Arc<AsyncMutex<W>>,
    action: RecoveryWireAction,
    recovery_timeout: Duration,
    limits: ProtocolLimits,
) -> Result<()>
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + std::fmt::Debug + Send + Unpin + 'static,
{
    let result = block_on_recovery_deadline(
        async {
            match action {
                RecoveryWireAction::BreakAndDrain => {
                    break_and_drain_wire_unbounded_with_limits(read, write, limits).await
                }
                RecoveryWireAction::DrainCancel => {
                    drain_cancel_wire_unbounded_with_limits(read, write, limits).await
                }
            }
        },
        recovery_timeout,
    );
    classify_recovery_result(action, result)
}

/// How a driver operation should dispose of the connection when asupersync
/// cancels it, derived from the structured [`CancelKind`] — never from a display
/// string. This is the internal half of the W1-T6 *Outcome/CancelKind
/// discipline*: cancellation is not "just another error", so each kind drives a
/// specific recovery posture before we flatten to the public [`Error`] at the
/// boundary.
///
/// The mapping mirrors the asupersync severity model (`Timeout` ≈ retry/degrade,
/// `Shutdown` ≈ stop and close, `RaceLost` ≈ loser drains quietly):
///
/// | [`CancelKind`]                                   | [`CancelDisposition`] | Public [`Error`]              | Connection |
/// |--------------------------------------------------|-----------------------|-------------------------------|------------|
/// | `Timeout`/`Deadline`/`PollQuota`/`CostBudget`    | `Timeout`             | [`Error::CallTimeout`]        | reusable, retryable |
/// | `Shutdown`/`ResourceUnavailable`/`LinkedExit`    | `Close`               | [`Error::ConnectionClosed`]   | dead       |
/// | `User`/`RaceLost`/`FailFast`/`ParentCancelled`   | `Cancel`              | [`Error::Cancelled`]          | reusable, retryable |
///
/// `Timeout` and `Cancel` both leave the session alive (the wire is drained to a
/// clean boundary by the call-timeout / cancel recovery path), so the surfaced
/// error is connection-*reusable* and carries a conservative `retry_hint()`.
/// `Close` is the only disposition that marks the connection dead.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CancelDisposition {
    /// Budget/deadline exhaustion: drain the wire and surface a retryable
    /// timeout on a still-reusable connection.
    Timeout,
    /// Runtime shutdown / unrecoverable resource loss: the connection must be
    /// discarded.
    Close,
    /// An explicit or topological cancel (user cancel, race loser, fail-fast
    /// sibling, parent region closing): drain quietly and surface a distinct,
    /// retryable cancel — never a generic I/O or runtime error.
    Cancel,
}

impl CancelDisposition {
    /// Classify a [`CancelKind`] into the driver's recovery posture. Pure and
    /// total over the asupersync enum. The match is exhaustive (no `_` arm) on
    /// purpose: if a future asupersync release adds a `CancelKind` variant, this
    /// fails to compile and forces a deliberate disposition choice rather than
    /// silently defaulting a new kind to a connection close.
    fn from_kind(kind: CancelKind) -> Self {
        match kind {
            // Deadline / quota exhaustion is the timeout family: the operation
            // ran out of its budget. The session survives once the wire drains,
            // so it composes exactly like a `call_timeout` (DPY-4024).
            CancelKind::Timeout
            | CancelKind::Deadline
            | CancelKind::PollQuota
            | CancelKind::CostBudget => CancelDisposition::Timeout,
            // Runtime is shutting down, or a resource the connection depends on
            // is gone, or a linked task died abnormally — stop acquiring work
            // and discard this connection rather than reuse it.
            CancelKind::Shutdown | CancelKind::ResourceUnavailable | CancelKind::LinkedExit => {
                CancelDisposition::Close
            }
            // Explicit user cancel, or topological cancellation (race loser,
            // fail-fast sibling, parent region closing). The session is alive;
            // the loser/owner just drains quietly.
            CancelKind::User
            | CancelKind::RaceLost
            | CancelKind::FailFast
            | CancelKind::ParentCancelled => CancelDisposition::Cancel,
        }
    }

    /// The public-boundary [`Error`] this disposition flattens to. This is the
    /// ONLY place a cancellation crosses from the internal Outcome/CancelKind
    /// world into a `Result`; `Cancelled` is always a distinct variant, never a
    /// generic [`Error::Runtime`] or [`Error::Io`].
    fn into_error(self, timeout_ms: u32) -> Error {
        match self {
            CancelDisposition::Timeout => Error::CallTimeout(timeout_ms),
            CancelDisposition::Close => {
                Error::ConnectionClosed("operation cancelled by runtime shutdown".into())
            }
            CancelDisposition::Cancel => Error::Cancelled,
        }
    }
}

/// Read the structured cancel disposition for a context known to be cancelled.
/// Falls back to [`CancelDisposition::Cancel`] when no [`CancelReason`] is
/// attached (a cancel with no recorded kind is still a cancel, never a runtime
/// error). Pure inspection of [`CancelReason::kind`] — no display parsing.
fn cancel_disposition(reason: Option<CancelReason>) -> CancelDisposition {
    reason
        .map(|reason| CancelDisposition::from_kind(reason.kind))
        .unwrap_or(CancelDisposition::Cancel)
}

/// Contract checkpoint for multi-round-trip loops: call after the previous
/// round trip has reached a clean boundary and before issuing the next one.
///
/// On a clean checkpoint this is `Ok(())`. When asupersync has cancelled the
/// context, the checkpoint fails and we branch on the structured [`CancelKind`]
/// to flatten it to the right *distinct* public error — a timeout
/// ([`Error::CallTimeout`]), a shutdown close ([`Error::ConnectionClosed`]), or
/// an explicit cancel ([`Error::Cancelled`]) — instead of the old
/// `Error::Runtime(display_string)`. Because this runs at a clean between-round-
/// trip boundary (no bytes in flight), there is nothing to drain here; the
/// recovery drain happens in the in-operation timeout/cancel path
/// ([`Connection::recover_from_call_timeout`]).
///
/// Recovery drains are the exception: they run without the expired caller `Cx`
/// and are bounded by their fresh recovery deadline instead.
/// Single wire round trips that internally write/read multiple frames (pipeline,
/// packet reassembly) are not clean boundaries until the whole response drains.
fn observe_cancellation_between_round_trips(cx: &Cx) -> Result<()> {
    if cx.checkpoint().is_ok() {
        return Ok(());
    }
    // Cancelled: map the structured kind to a distinct public error. The
    // between-round-trip boundary has no in-flight wire, so the `timeout_ms`
    // here is the context's remaining budget (best-effort) for the timeout
    // family; the cancel/close arms ignore it.
    let timeout_ms = cx
        .budget()
        .remaining_time(time::wall_now())
        .map(duration_to_millis_saturating)
        .unwrap_or(0);
    Err(cancel_disposition(cx.cancel_reason()).into_error(timeout_ms))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum SessionRecoveryPhase {
    Ready = 0,
    InFlight = 1,
    BreakSent = 2,
    Draining = 3,
    Dead = 4,
}

impl SessionRecoveryPhase {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Ready,
            1 => Self::InFlight,
            2 => Self::BreakSent,
            3 => Self::Draining,
            _ => Self::Dead,
        }
    }
}

#[derive(Debug)]
struct SessionRecovery {
    phase: AtomicU8,
}

impl SessionRecovery {
    fn new() -> Self {
        Self {
            phase: AtomicU8::new(SessionRecoveryPhase::Ready as u8),
        }
    }

    fn phase(&self) -> SessionRecoveryPhase {
        SessionRecoveryPhase::from_u8(self.phase.load(Ordering::SeqCst))
    }

    fn is_dead(&self) -> bool {
        self.phase() == SessionRecoveryPhase::Dead
    }

    fn begin_operation(&self) -> Result<()> {
        match self.phase.compare_exchange(
            SessionRecoveryPhase::Ready as u8,
            SessionRecoveryPhase::InFlight as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => Ok(()),
            Err(current) => match SessionRecoveryPhase::from_u8(current) {
                SessionRecoveryPhase::InFlight => Err(Error::ConnectionClosed(
                    "operation attempted while a response is still in flight".into(),
                )),
                SessionRecoveryPhase::BreakSent | SessionRecoveryPhase::Draining => {
                    Err(Error::ConnectionClosed(
                        "operation attempted while session recovery is pending".into(),
                    ))
                }
                SessionRecoveryPhase::Dead => {
                    Err(Error::ConnectionClosed("connection is closed".into()))
                }
                SessionRecoveryPhase::Ready => Ok(()),
            },
        }
    }

    fn begin_or_adopt_operation(&self) -> Result<()> {
        match self.phase.compare_exchange(
            SessionRecoveryPhase::Ready as u8,
            SessionRecoveryPhase::InFlight as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => Ok(()),
            Err(current) => match SessionRecoveryPhase::from_u8(current) {
                SessionRecoveryPhase::InFlight => Ok(()),
                SessionRecoveryPhase::BreakSent | SessionRecoveryPhase::Draining => {
                    Err(Error::ConnectionClosed(
                        "operation attempted while session recovery is pending".into(),
                    ))
                }
                SessionRecoveryPhase::Dead => {
                    Err(Error::ConnectionClosed("connection is closed".into()))
                }
                SessionRecoveryPhase::Ready => Ok(()),
            },
        }
    }

    fn complete_operation(&self) {
        let _ = self.phase.compare_exchange(
            SessionRecoveryPhase::InFlight as u8,
            SessionRecoveryPhase::Ready as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    fn mark_break_required(&self) {
        let _ = self.phase.compare_exchange(
            SessionRecoveryPhase::InFlight as u8,
            SessionRecoveryPhase::BreakSent as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    fn mark_break_sent(&self) -> Result<()> {
        loop {
            let current = self.phase.load(Ordering::SeqCst);
            match SessionRecoveryPhase::from_u8(current) {
                SessionRecoveryPhase::Dead => {
                    return Err(Error::ConnectionClosed("connection is closed".into()));
                }
                SessionRecoveryPhase::BreakSent | SessionRecoveryPhase::Draining => return Ok(()),
                SessionRecoveryPhase::Ready | SessionRecoveryPhase::InFlight => {
                    if self
                        .phase
                        .compare_exchange(
                            current,
                            SessionRecoveryPhase::BreakSent as u8,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                    {
                        return Ok(());
                    }
                }
            }
        }
    }

    fn begin_pending_drain(&self) -> Result<bool> {
        match self.phase.compare_exchange(
            SessionRecoveryPhase::BreakSent as u8,
            SessionRecoveryPhase::Draining as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => Ok(true),
            Err(current) => match SessionRecoveryPhase::from_u8(current) {
                SessionRecoveryPhase::Ready => Ok(false),
                SessionRecoveryPhase::InFlight => Err(Error::ConnectionClosed(
                    "operation attempted while a response is still in flight".into(),
                )),
                SessionRecoveryPhase::Draining => Err(Error::ConnectionClosed(
                    "session recovery is already draining".into(),
                )),
                SessionRecoveryPhase::BreakSent => Ok(false),
                SessionRecoveryPhase::Dead => {
                    Err(Error::ConnectionClosed("connection is closed".into()))
                }
            },
        }
    }

    fn begin_drain_after_break(&self) -> Result<()> {
        self.mark_break_sent()?;
        match self.begin_pending_drain()? {
            true => Ok(()),
            false => Err(Error::ConnectionClosed(
                "session recovery did not enter draining state".into(),
            )),
        }
    }

    fn finish_drain_ready(&self) {
        self.phase
            .store(SessionRecoveryPhase::Ready as u8, Ordering::SeqCst);
    }

    fn mark_dead(&self) {
        self.phase
            .store(SessionRecoveryPhase::Dead as u8, Ordering::SeqCst);
    }
}

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

/// Curated set of Oracle error codes that are *transient*: the request failed
/// for a reason that is expected to clear on its own, so a caller may safely
/// retry the same operation after a short back-off without changing anything.
///
/// This is the list every production shop hand-rolls on top of python-oracledb's
/// bare `.code` int; we ship it curated and documented. Codes covered:
///
/// - `ORA-00054` resource busy / `NOWAIT` lock contention
/// - `ORA-00060` deadlock detected while waiting for a resource
/// - `ORA-00104`/`ORA-00257` instance/archiver hung — resource starvation
/// - `ORA-12516`/`ORA-12520`/`ORA-12526`/`ORA-12528` listener could not hand
///   off a handler / all appropriate handlers busy or restricted (TAF retry)
/// - `ORA-30006` resource busy waiting for a `WAIT POLICY`
/// - `ORA-51535` concurrency limit exceeded (database resource manager)
///
/// Connection-lost codes ([`CONNECTION_LOST_ORA_CODES`]) are *also* retryable
/// (after re-establishing the connection); [`Error::is_retryable`] reports the
/// union of both sets.
const TRANSIENT_ORA_CODES: &[u32] = &[54, 60, 104, 257, 12516, 12520, 12526, 12528, 30006, 51535];

/// Curated set of Oracle error codes that mean the *connection itself was
/// lost* (the session is gone, the socket was reset, or the listener/server
/// dropped the link). These are a subset of [`SESSION_DEAD_ORA_CODES`] —
/// the codes that specifically signal a severed network/session link rather
/// than an internal server fault. Reconnect, then retry.
///
/// Codes covered:
///
/// - `ORA-00028` your session has been killed
/// - `ORA-01012` not logged on (session terminated server-side)
/// - `ORA-01041`/`ORA-01089` internal error / immediate shutdown in progress
/// - `ORA-02396` exceeded maximum idle time, session reconnected
/// - `ORA-03113` end-of-file on communication channel
/// - `ORA-03114` not connected to Oracle
/// - `ORA-03135` connection lost contact
/// - `ORA-12537` TNS: connection closed
/// - `ORA-12547` TNS: lost contact
/// - `ORA-12570` TNS: packet reader failure
/// - `ORA-28511` lost RPC connection to heterogeneous remote agent
const CONNECTION_LOST_ORA_CODES: &[u32] = &[
    28, 1012, 1041, 1089, 2396, 3113, 3114, 3135, 12537, 12547, 12570, 28511,
];

/// Extract the leading `ORA-NNNNN` numeric code from an Oracle error message,
/// if the message carries one. Used as the fallback when a structured
/// [`ServerErrorDetails`] code is not available (string-only error variants).
fn parse_ora_code_from_message(message: &str) -> Option<u32> {
    let start = message.find("ORA-")?;
    let digits: String = message[start + 4..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect();
    digits.parse::<u32>().ok()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PostSyncProtocolDisposition {
    Ready,
    Dead,
}

/// Classify protocol errors after bytes for an operation have crossed the wire.
///
/// Pre-sync/client-side validation errors return directly and keep any existing
/// connection usable. Once a server response is being decoded, a resource-limit
/// violation means the client intentionally stopped consuming an in-flight
/// response, so the wire can no longer be assumed aligned.
fn post_sync_protocol_error_disposition(
    err: &oracledb_protocol::ProtocolError,
) -> PostSyncProtocolDisposition {
    match err {
        oracledb_protocol::ProtocolError::ResourceLimit { .. } => PostSyncProtocolDisposition::Dead,
        _ if protocol_error_ora_code(err)
            .is_some_and(|code| SESSION_DEAD_ORA_CODES.contains(&code)) =>
        {
            PostSyncProtocolDisposition::Dead
        }
        _ => PostSyncProtocolDisposition::Ready,
    }
}

fn protocol_error_is_session_dead(err: &oracledb_protocol::ProtocolError) -> bool {
    post_sync_protocol_error_disposition(err) == PostSyncProtocolDisposition::Dead
}

fn protocol_error_kind(err: &oracledb_protocol::ProtocolError) -> ErrorKind {
    match err {
        oracledb_protocol::ProtocolError::ResourceLimit { .. } => ErrorKind::ResourceLimit,
        oracledb_protocol::ProtocolError::ServerError(_)
        | oracledb_protocol::ProtocolError::ServerErrorWithRowCount { .. }
        | oracledb_protocol::ProtocolError::ServerErrorInfo(_) => ErrorKind::Database,
        _ => ErrorKind::Protocol,
    }
}

/// The Oracle error number carried by a [`ProtocolError`], whether it is the
/// structured [`ServerErrorInfo`] variant (read directly from `.code`) or a
/// string variant (parsed from the `ORA-NNNNN` prefix). `None` for protocol
/// errors that are not server errors (truncated packets, decode failures, ...).
fn protocol_error_ora_code(err: &oracledb_protocol::ProtocolError) -> Option<u32> {
    match err {
        oracledb_protocol::ProtocolError::ServerError(message) => {
            parse_ora_code_from_message(message)
        }
        oracledb_protocol::ProtocolError::ServerErrorWithRowCount { message, .. } => {
            parse_ora_code_from_message(message)
        }
        oracledb_protocol::ProtocolError::ServerErrorInfo(details) => Some(details.code),
        _ => None,
    }
}

/// The server-reported error position / parse offset carried by a
/// [`ProtocolError`], if any. Only the structured [`ServerErrorInfo`] variant
/// retains the offset; the string variants drop it on the wire path.
fn protocol_error_offset(err: &oracledb_protocol::ProtocolError) -> Option<i32> {
    match err {
        oracledb_protocol::ProtocolError::ServerErrorInfo(details) if details.pos != 0 => {
            Some(details.pos)
        }
        _ => None,
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
    /// Access-token authentication was requested over a non-TLS transport.
    /// A database access token must only travel over TCPS so it is not exposed
    /// in clear text (reference protocol.pyx `ERR_ACCESS_TOKEN_REQUIRES_TCPS` /
    /// DPY-3001). Reconnect with a `tcps://` connect string. The token itself is
    /// never included in this error.
    #[error("DPY-3001: access token authentication requires a TLS (TCPS) connection")]
    AccessTokenRequiresTcps,
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

const DEFAULT_QUERY_ARRAYSIZE: u32 = 100;

fn default_query_arraysize() -> NonZeroU32 {
    NonZeroU32::new(DEFAULT_QUERY_ARRAYSIZE).expect("default query arraysize is non-zero")
}

fn duration_to_nanos_saturating(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn duration_to_millis_saturating(duration: Duration) -> u32 {
    duration.as_millis().min(u128::from(u32::MAX)) as u32
}

/// A REF CURSOR handle returned in a row or implicit result set.
pub type Cursor = CursorValue;

/// Scroll target for [`Rows::scroll`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Scroll {
    Current,
    Next,
    Prior,
    First,
    Last,
    Absolute(u32),
    Relative(u32),
}

impl Scroll {
    fn into_wire_parts(self) -> (u32, u32) {
        match self {
            Scroll::Current => (TNS_FETCH_ORIENTATION_CURRENT, 0),
            Scroll::Next => (TNS_FETCH_ORIENTATION_NEXT, 0),
            Scroll::Prior => (TNS_FETCH_ORIENTATION_PRIOR, 0),
            Scroll::First => (TNS_FETCH_ORIENTATION_FIRST, 0),
            Scroll::Last => (TNS_FETCH_ORIENTATION_LAST, 0),
            Scroll::Absolute(pos) => (TNS_FETCH_ORIENTATION_ABSOLUTE, pos),
            Scroll::Relative(pos) => (TNS_FETCH_ORIENTATION_RELATIVE, pos),
        }
    }
}

/// Query builder for the high-level row API.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Query<'a> {
    sql: std::borrow::Cow<'a, str>,
    params: Params<'a>,
    arraysize: NonZeroU32,
    prefetch: u32,
    prefetch_set: bool,
    materialize_lobs: bool,
    scrollable: bool,
    timeout: Option<Duration>,
}

impl<'a> Query<'a> {
    pub fn new(sql: &'a str) -> Self {
        let arraysize = default_query_arraysize();
        Self {
            sql: std::borrow::Cow::Borrowed(sql),
            params: Params::None,
            arraysize,
            prefetch: arraysize.get(),
            prefetch_set: false,
            materialize_lobs: true,
            scrollable: false,
            timeout: None,
        }
    }

    fn owned_sql(sql: String) -> Self {
        let arraysize = default_query_arraysize();
        Self {
            sql: std::borrow::Cow::Owned(sql),
            params: Params::None,
            arraysize,
            prefetch: arraysize.get(),
            prefetch_set: false,
            materialize_lobs: true,
            scrollable: false,
            timeout: None,
        }
    }

    pub fn bind(mut self, params: impl Into<Params<'a>>) -> Self {
        self.params = params.into();
        self
    }

    pub fn arraysize(mut self, n: NonZeroU32) -> Self {
        self.arraysize = n;
        if !self.prefetch_set {
            self.prefetch = n.get();
        }
        self
    }

    pub fn prefetch(mut self, n: u32) -> Self {
        self.prefetch = n;
        self.prefetch_set = true;
        self
    }

    pub fn stream_lobs(mut self) -> Self {
        self.materialize_lobs = false;
        self
    }

    pub fn scrollable(mut self) -> Self {
        self.scrollable = true;
        self
    }

    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    pub fn sql(&self) -> &str {
        self.sql.as_ref()
    }

    pub fn params(&self) -> &Params<'a> {
        &self.params
    }

    pub fn arraysize_value(&self) -> NonZeroU32 {
        self.arraysize
    }

    pub fn prefetch_rows(&self) -> u32 {
        self.prefetch
    }

    pub fn materialize_lobs(&self) -> bool {
        self.materialize_lobs
    }

    pub fn is_scrollable(&self) -> bool {
        self.scrollable
    }

    pub fn timeout_duration(&self) -> Option<Duration> {
        self.timeout
    }
}

/// Execute builder for DML, DDL, and PL/SQL operations that use at most one
/// bind row.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Execute<'a> {
    sql: std::borrow::Cow<'a, str>,
    params: Params<'a>,
    timeout: Option<Duration>,
    options: ExecuteOptions,
}

impl<'a> Execute<'a> {
    pub fn new(sql: &'a str) -> Self {
        Self {
            sql: std::borrow::Cow::Borrowed(sql),
            params: Params::None,
            timeout: None,
            options: ExecuteOptions::default(),
        }
    }

    fn owned_sql(sql: String) -> Self {
        Self {
            sql: std::borrow::Cow::Owned(sql),
            params: Params::None,
            timeout: None,
            options: ExecuteOptions::default(),
        }
    }

    pub fn bind(mut self, params: impl Into<Params<'a>>) -> Self {
        self.params = params.into();
        self
    }

    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    pub fn parse_only(mut self) -> Self {
        self.options = self.options.with_parse_only(true);
        self
    }

    pub fn raw_options(mut self, options: ExecuteOptions) -> Self {
        self.options = options;
        self
    }

    pub fn sql(&self) -> &str {
        self.sql.as_ref()
    }

    pub fn params(&self) -> &Params<'a> {
        &self.params
    }

    pub fn timeout_duration(&self) -> Option<Duration> {
        self.timeout
    }

    pub fn options(&self) -> ExecuteOptions {
        self.options
    }
}

/// Bind rows for [`Connection::execute_many`]. Each inner `Vec<BindValue>` is
/// one execution of the statement.
#[derive(Clone, Debug, PartialEq)]
pub enum BatchRows<'a> {
    Borrowed(&'a [Vec<BindValue>]),
    Owned(Vec<Vec<BindValue>>),
}

impl<'a> BatchRows<'a> {
    fn as_slice(&self) -> &[Vec<BindValue>] {
        match self {
            Self::Borrowed(rows) => rows,
            Self::Owned(rows) => rows.as_slice(),
        }
    }

    fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    fn bind_width(&self) -> Option<usize> {
        self.as_slice().first().map(Vec::len)
    }

    fn validate_rectangular(&self) -> Result<()> {
        let Some(expected) = self.bind_width() else {
            return Ok(());
        };
        for (row_index, row) in self.as_slice().iter().enumerate().skip(1) {
            if row.len() != expected {
                return Err(Error::Bind(BindError::BatchRowWidthMismatch {
                    row_index,
                    expected,
                    actual: row.len(),
                }));
            }
        }
        Ok(())
    }
}

impl<'a> From<&'a [Vec<BindValue>]> for BatchRows<'a> {
    fn from(rows: &'a [Vec<BindValue>]) -> Self {
        Self::Borrowed(rows)
    }
}

impl<'a> From<&'a Vec<Vec<BindValue>>> for BatchRows<'a> {
    fn from(rows: &'a Vec<Vec<BindValue>>) -> Self {
        Self::Borrowed(rows.as_slice())
    }
}

impl<'a, const N: usize> From<&'a [Vec<BindValue>; N]> for BatchRows<'a> {
    fn from(rows: &'a [Vec<BindValue>; N]) -> Self {
        Self::Borrowed(rows.as_slice())
    }
}

impl<'a> From<Vec<Vec<BindValue>>> for BatchRows<'a> {
    fn from(rows: Vec<Vec<BindValue>>) -> Self {
        Self::Owned(rows)
    }
}

/// Execute-many builder for array DML.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct Batch<'a> {
    sql: std::borrow::Cow<'a, str>,
    rows: BatchRows<'a>,
    timeout: Option<Duration>,
    options: ExecuteOptions,
}

impl<'a> Batch<'a> {
    pub fn new(sql: &'a str, rows: impl Into<BatchRows<'a>>) -> Self {
        Self {
            sql: std::borrow::Cow::Borrowed(sql),
            rows: rows.into(),
            timeout: None,
            options: ExecuteOptions::default(),
        }
    }

    fn owned_sql(sql: String, rows: impl Into<BatchRows<'a>>) -> Self {
        Self {
            sql: std::borrow::Cow::Owned(sql),
            rows: rows.into(),
            timeout: None,
            options: ExecuteOptions::default(),
        }
    }

    pub fn collect_errors(mut self) -> Self {
        self.options = self.options.with_batcherrors(true);
        self
    }

    pub fn row_counts(mut self) -> Self {
        self.options = self.options.with_arraydmlrowcounts(true);
        self
    }

    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    pub fn raw_options(mut self, options: ExecuteOptions) -> Self {
        self.options = options;
        self
    }

    pub fn sql(&self) -> &str {
        self.sql.as_ref()
    }

    pub fn rows(&self) -> &BatchRows<'a> {
        &self.rows
    }

    pub fn timeout_duration(&self) -> Option<Duration> {
        self.timeout
    }

    pub fn options(&self) -> ExecuteOptions {
        self.options
    }
}

/// OUT and IN/OUT bind values returned by [`Connection::execute`].
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct OutBinds {
    values: Vec<(usize, Option<QueryValue>)>,
}

impl OutBinds {
    fn new(values: Vec<(usize, Option<QueryValue>)>) -> Self {
        Self { values }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn values(&self) -> &[(usize, Option<QueryValue>)] {
        &self.values
    }

    pub fn get(&self, bind_index: usize) -> Option<&Option<QueryValue>> {
        self.values
            .iter()
            .find_map(|(index, value)| (*index == bind_index).then_some(value))
    }

    pub fn into_values(self) -> Vec<(usize, Option<QueryValue>)> {
        self.values
    }
}

/// Per-bind rows returned by DML `RETURNING INTO`.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct ReturningRows {
    values: Vec<(usize, Vec<Option<QueryValue>>)>,
}

impl ReturningRows {
    fn new(values: Vec<(usize, Vec<Option<QueryValue>>)>) -> Self {
        Self { values }
    }

    /// Build from raw per-call return-value groups, coalescing groups that share
    /// a bind index. Array DML (`execute_many`) decodes `RETURNING` once per
    /// iteration, emitting one `(bind_index, rows)` group per iteration; without
    /// coalescing `rows_for(bind_index)` — which returns the first matching
    /// group — would expose only the first iteration's value. Coalescing merges
    /// them in input order so `rows_for(bind_index)` returns every affected
    /// row's value, consistent with single-statement `RETURNING`, which already
    /// arrives as one group per bind. (The raw per-iteration grouping is
    /// preserved at the protocol layer for consumers that need it.)
    fn coalesced(raw: Vec<(usize, Vec<Option<QueryValue>>)>) -> Self {
        let mut values: Vec<(usize, Vec<Option<QueryValue>>)> = Vec::new();
        for (index, rows) in raw {
            if let Some((_, existing)) = values.iter_mut().find(|(i, _)| *i == index) {
                existing.extend(rows);
            } else {
                values.push((index, rows));
            }
        }
        Self { values }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn values(&self) -> &[(usize, Vec<Option<QueryValue>>)] {
        &self.values
    }

    pub fn rows_for(&self, bind_index: usize) -> Option<&[Option<QueryValue>]> {
        self.values
            .iter()
            .find_map(|(index, rows)| (*index == bind_index).then_some(rows.as_slice()))
    }

    pub fn into_values(self) -> Vec<(usize, Vec<Option<QueryValue>>)> {
        self.values
    }
}

/// Result of an [`Execute`] operation.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct ExecuteOutcome {
    rows_affected: u64,
    last_rowid: Option<String>,
    out_binds: OutBinds,
    returning: ReturningRows,
    implicit_results: Vec<Cursor>,
    compilation_warning: bool,
}

impl ExecuteOutcome {
    const COMPILATION_WARNING: &'static str = "PL/SQL compiled with warnings";

    fn from_query_result(result: QueryResult) -> Self {
        let implicit_results = result
            .implicit_resultsets
            .unwrap_or_default()
            .into_iter()
            .filter_map(|value| match value {
                QueryValue::Cursor(cursor) => Some(*cursor),
                _ => None,
            })
            .collect();
        Self {
            rows_affected: result.row_count,
            last_rowid: result.last_rowid,
            out_binds: OutBinds::new(result.out_values),
            returning: ReturningRows::new(result.return_values),
            implicit_results,
            compilation_warning: result.compilation_error_warning,
        }
    }

    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }

    pub fn last_rowid(&self) -> Option<&str> {
        self.last_rowid.as_deref()
    }

    pub fn out_binds(&self) -> &OutBinds {
        &self.out_binds
    }

    pub fn returning(&self) -> &ReturningRows {
        &self.returning
    }

    pub fn implicit_results(&self) -> &[Cursor] {
        &self.implicit_results
    }

    pub fn compilation_warning(&self) -> Option<&str> {
        self.compilation_warning
            .then_some(Self::COMPILATION_WARNING)
    }
}

/// One row-level error collected by [`Batch::collect_errors`].
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct BatchError {
    row_index: u32,
    code: u32,
    message: String,
}

impl BatchError {
    fn from_server(error: BatchServerError) -> Self {
        let (code, row_index, message) = error.into_parts();
        Self {
            row_index,
            code,
            message,
        }
    }

    pub fn row_index(&self) -> u32 {
        self.row_index
    }

    pub fn code(&self) -> u32 {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for BatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.message.is_empty() {
            write!(f, "ORA-{:05} at batch row {}", self.code, self.row_index)
        } else {
            write!(f, "{} at batch row {}", self.message, self.row_index)
        }
    }
}

/// Result of an [`execute_many`](Connection::execute_many) operation.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct BatchOutcome {
    rows_affected: u64,
    per_row_counts: Option<Vec<u64>>,
    errors: Vec<BatchError>,
    returning: ReturningRows,
}

impl BatchOutcome {
    fn empty(array_dml_row_counts: bool) -> Self {
        Self {
            rows_affected: 0,
            per_row_counts: array_dml_row_counts.then(Vec::new),
            errors: Vec::new(),
            returning: ReturningRows::default(),
        }
    }

    fn from_query_result(result: QueryResult) -> Self {
        Self {
            rows_affected: result.row_count,
            per_row_counts: result.array_dml_row_counts,
            errors: result
                .batch_errors
                .into_iter()
                .map(BatchError::from_server)
                .collect(),
            returning: ReturningRows::coalesced(result.return_values),
        }
    }

    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }

    pub fn per_row_counts(&self) -> Option<&[u64]> {
        self.per_row_counts.as_deref()
    }

    pub fn errors(&self) -> &[BatchError] {
        &self.errors
    }

    pub fn returning(&self) -> &ReturningRows {
        &self.returning
    }
}

/// Registered-query builder for Continuous Query Notification (CQN).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Registration<'a> {
    sql: std::borrow::Cow<'a, str>,
    params: Params<'a>,
    registration_id: u64,
    timeout: Option<Duration>,
}

impl<'a> Registration<'a> {
    pub fn new(sql: &'a str, registration_id: u64) -> Self {
        Self {
            sql: std::borrow::Cow::Borrowed(sql),
            params: Params::None,
            registration_id,
            timeout: None,
        }
    }

    fn owned_sql(sql: String, registration_id: u64) -> Self {
        Self {
            sql: std::borrow::Cow::Owned(sql),
            params: Params::None,
            registration_id,
            timeout: None,
        }
    }

    pub fn bind(mut self, params: impl Into<Params<'a>>) -> Self {
        self.params = params.into();
        self
    }

    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    pub fn sql(&self) -> &str {
        self.sql.as_ref()
    }

    pub fn params(&self) -> &Params<'a> {
        &self.params
    }

    pub fn registration_id(&self) -> u64 {
        self.registration_id
    }

    pub fn timeout_duration(&self) -> Option<Duration> {
        self.timeout
    }
}

/// Result of a [`register_query`](Connection::register_query) operation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct RegistrationOutcome {
    query_id: Option<u64>,
}

impl RegistrationOutcome {
    fn from_query_result(result: QueryResult) -> Self {
        Self {
            query_id: result.query_id.filter(|id| *id != 0),
        }
    }

    pub fn query_id(&self) -> Option<u64> {
        self.query_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct QueryDeadline {
    deadline: Option<asupersync::types::Time>,
    timeout_ms: u32,
}

impl QueryDeadline {
    fn new(cx: &Cx, timeout: Option<Duration>) -> Self {
        let now = time::wall_now();
        let query_deadline = timeout
            .map(|duration| now.saturating_add_nanos(duration_to_nanos_saturating(duration)));
        let cx_deadline = cx.budget().deadline;
        let deadline = match (query_deadline, cx_deadline) {
            (Some(query), Some(cx)) => Some(query.min(cx)),
            (Some(query), None) => Some(query),
            (None, Some(cx)) => Some(cx),
            (None, None) => None,
        };
        let timeout_ms = timeout
            .map(duration_to_millis_saturating)
            .or_else(|| {
                cx.budget()
                    .remaining_time(now)
                    .map(duration_to_millis_saturating)
            })
            .unwrap_or(0);
        Self {
            deadline,
            timeout_ms,
        }
    }

    fn timeout_ms(self) -> u32 {
        self.timeout_ms
    }

    async fn run<T, F>(self, future: F) -> std::result::Result<Result<T>, ()>
    where
        F: Future<Output = Result<T>>,
    {
        let Some(deadline) = self.deadline else {
            return Ok(future.await);
        };
        let now = time::wall_now();
        if now >= deadline {
            return Err(());
        }
        let remaining = Duration::from_nanos(deadline.as_nanos().saturating_sub(now.as_nanos()));
        match time::timeout(now, remaining, future).await {
            Ok(result) => Ok(result),
            Err(_) => Err(()),
        }
    }
}

/// One owned query row.
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    columns: Arc<[ColumnMetadata]>,
    values: Vec<Option<QueryValue>>,
}

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

impl Row {
    fn new(columns: Arc<[ColumnMetadata]>, values: Vec<Option<QueryValue>>) -> Self {
        Self { columns, values }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn columns(&self) -> &[ColumnMetadata] {
        &self.columns
    }

    pub fn values(&self) -> &[Option<QueryValue>] {
        &self.values
    }

    pub fn value(&self, col: impl ColumnIndex) -> Option<&QueryValue> {
        let col = col.resolve(&self.columns).ok()?;
        self.values.get(col).and_then(Option::as_ref)
    }

    pub fn typed_row(&self) -> TypedRow<'_> {
        TypedRow::new(&self.columns, &self.values, 0)
    }

    pub fn get<T: FromSql>(&self, col: impl ColumnIndex) -> Result<T> {
        let col = col.resolve(&self.columns).map_err(Error::Conversion)?;
        self.typed_row().get(col)
    }

    pub fn try_get<T: FromSql>(&self, col: impl ColumnIndex) -> Result<Option<T>> {
        let col = col.resolve(&self.columns).map_err(Error::Conversion)?;
        self.typed_row().try_get_opt(col).map_err(Error::Conversion)
    }

    pub fn get_by_name<T: FromSql>(&self, name: &str) -> Result<T> {
        self.get(name)
    }

    pub fn into_values(self) -> Vec<Option<QueryValue>> {
        self.values
    }
}

/// Lazy result-set facade returned by [`Connection::query`] and
/// [`Connection::query_with`].
#[derive(Debug)]
#[non_exhaustive]
pub struct Rows<'conn> {
    connection: &'conn mut Connection,
    sql: String,
    columns: Arc<[ColumnMetadata]>,
    batch: Vec<Row>,
    cursor_id: u32,
    more_rows: bool,
    arraysize: NonZeroU32,
    deadline: QueryDeadline,
    scrollable: bool,
    cursor: Option<Cursor>,
}

impl Rows<'_> {
    fn from_result<'conn>(
        connection: &'conn mut Connection,
        sql: String,
        arraysize: NonZeroU32,
        deadline: QueryDeadline,
        scrollable: bool,
        result: QueryResult,
    ) -> Rows<'conn> {
        let cursor_id = result.cursor_id;
        let more_rows = result.more_rows;
        let cursor = first_cursor_from_result(&result);
        let columns: Arc<[ColumnMetadata]> = Arc::from(result.columns.into_boxed_slice());
        let batch = result
            .rows
            .into_iter()
            .map(|values| Row::new(Arc::clone(&columns), values))
            .collect();
        Rows {
            connection,
            sql,
            columns,
            batch,
            cursor_id,
            more_rows,
            arraysize,
            deadline,
            scrollable,
            cursor,
        }
    }

    pub fn columns(&self) -> &[ColumnMetadata] {
        &self.columns
    }

    pub fn batch(&self) -> &[Row] {
        &self.batch
    }

    pub async fn next_batch(&mut self, cx: &Cx) -> Result<bool> {
        if !self.more_rows || self.cursor_id == 0 {
            self.release_cursor();
            return Ok(false);
        }
        observe_cancellation_between_round_trips(cx)?;
        let previous_row = self.batch.last().map(|row| row.values.clone());
        let cursor_id = self.cursor_id;
        let arraysize = self.arraysize.get();
        let columns = self.columns.to_vec();
        let result = match self
            .deadline
            .run(self.connection.fetch_rows_with_columns(
                cx,
                cursor_id,
                arraysize,
                &columns,
                previous_row.as_deref(),
            ))
            .await
        {
            Ok(result) => result?,
            Err(()) => {
                self.release_cursor();
                return self
                    .connection
                    .recover_from_call_timeout(cx, self.deadline.timeout_ms())
                    .await;
            }
        };
        self.apply_result(result);
        let batch_available = !self.batch.is_empty() || self.more_rows;
        if !self.more_rows {
            self.release_cursor();
        }
        Ok(batch_available)
    }

    pub async fn collect(mut self, cx: &Cx) -> Result<Vec<Row>> {
        let mut rows = Vec::new();
        rows.append(&mut self.batch);
        while self.more_rows {
            if let Err(err) = self.next_batch(cx).await {
                self.release_cursor();
                return Err(err);
            }
            rows.append(&mut self.batch);
        }
        self.release_cursor();
        Ok(rows)
    }

    /// Fetch ahead until the batch holds at least two rows or the server has
    /// confirmed end-of-data, so the cardinality check in [`one`](Self::one) /
    /// [`opt`](Self::opt) cannot mistake a still-pending `more_rows` flag for a
    /// second row.
    ///
    /// `more_rows` means only "the server has not yet signalled end-of-data",
    /// not ">1 row". A LONG / LONG RAW column forces a per-row define-fetch that
    /// ignores the requested arraysize, so a genuine single-row result comes
    /// back with one row and `more_rows` still set; without this confirmation
    /// `one()` would wrongly raise [`Error::TooManyRows`]. Bounded: at most one
    /// extra round trip for a single-row result, and it stops the moment a
    /// second row is in hand.
    async fn materialize_for_cardinality(&mut self, cx: &Cx) -> Result<()> {
        let mut held: Vec<Row> = Vec::new();
        while held.len() + self.batch.len() < 2 && self.more_rows && self.cursor_id != 0 {
            // `next_batch` keys the LONG/LOB define-fetch continuation off
            // `self.batch.last()` and then REPLACES `self.batch`. Clone the row
            // we already hold into `held` (leaving the original in place as the
            // continuation key) so it survives the fetch.
            if let Some(last) = self.batch.last() {
                held.push(last.clone());
            }
            self.next_batch(cx).await?;
        }
        if !held.is_empty() {
            held.append(&mut self.batch);
            self.batch = held;
        }
        Ok(())
    }

    pub fn one(mut self) -> Result<Row> {
        let too_many = self.more_rows || self.batch.len() > 1;
        self.release_cursor();
        if too_many {
            return Err(Error::TooManyRows);
        }
        self.batch.pop().ok_or(Error::NoRows)
    }

    pub fn opt(mut self) -> Result<Option<Row>> {
        let too_many = self.more_rows || self.batch.len() > 1;
        self.release_cursor();
        if too_many {
            return Err(Error::TooManyRows);
        }
        Ok(self.batch.pop())
    }

    pub fn into_typed<T: FromRow>(mut self) -> Result<Vec<T>> {
        self.release_cursor();
        self.batch
            .iter()
            .map(|row| T::from_row(&row.typed_row()).map_err(Error::Conversion))
            .collect()
    }

    pub fn cursor(&self) -> Option<&Cursor> {
        self.cursor.as_ref()
    }

    pub async fn scroll(&mut self, cx: &Cx, to: Scroll) -> Result<()> {
        if !self.scrollable {
            return Err(Error::Runtime(
                "Rows::scroll requires Query::scrollable".to_string(),
            ));
        }
        if self.cursor_id == 0 {
            return Err(Error::Runtime(
                "Rows::scroll requires an open cursor".to_string(),
            ));
        }
        observe_cancellation_between_round_trips(cx)?;
        let (orientation, position) = to.into_wire_parts();
        let result = match self
            .deadline
            .run(self.connection.scroll_cursor(
                cx,
                &self.sql,
                self.cursor_id,
                self.arraysize.get(),
                orientation,
                position,
            ))
            .await
        {
            Ok(result) => result?,
            Err(()) => {
                self.release_cursor();
                return self
                    .connection
                    .recover_from_call_timeout(cx, self.deadline.timeout_ms())
                    .await;
            }
        };
        self.apply_result(result);
        Ok(())
    }

    fn apply_result(&mut self, result: QueryResult) {
        let cursor = first_cursor_from_result(&result);
        if result.cursor_id != 0 {
            self.cursor_id = result.cursor_id;
        }
        if !result.columns.is_empty() {
            self.columns = Arc::from(result.columns.into_boxed_slice());
        }
        self.more_rows = result.more_rows;
        if self.cursor.is_none() {
            self.cursor = cursor;
        }
        self.batch = result
            .rows
            .into_iter()
            .map(|values| Row::new(Arc::clone(&self.columns), values))
            .collect();
    }

    fn release_cursor(&mut self) {
        if self.cursor_id == 0 {
            return;
        }
        self.connection.release_cursor(self.cursor_id);
        self.cursor_id = 0;
        self.more_rows = false;
    }
}

impl Drop for Rows<'_> {
    fn drop(&mut self) {
        self.release_cursor();
    }
}

/// Blocking lazy result-set facade returned by [`BlockingConnection::query`]
/// and [`BlockingConnection::query_with`].
///
/// `BlockingRows` owns the same server cursor state as [`Rows`], but its
/// continuation methods drive the async cursor operations on the blocking
/// facade runtime so synchronous callers never need to pass a [`Cx`].
#[derive(Debug)]
#[non_exhaustive]
pub struct BlockingRows<'conn> {
    inner: Rows<'conn>,
}

impl<'conn> BlockingRows<'conn> {
    fn new(inner: Rows<'conn>) -> Self {
        Self { inner }
    }

    pub fn columns(&self) -> &[ColumnMetadata] {
        self.inner.columns()
    }

    pub fn batch(&self) -> &[Row] {
        self.inner.batch()
    }

    pub fn next_batch(&mut self) -> Result<bool> {
        block_on_io(|cx| async move { self.inner.next_batch(&cx).await })
    }

    pub fn collect(self) -> Result<Vec<Row>> {
        block_on_io(|cx| async move { self.inner.collect(&cx).await })
    }

    pub fn one(self) -> Result<Row> {
        self.inner.one()
    }

    pub fn opt(self) -> Result<Option<Row>> {
        self.inner.opt()
    }

    pub fn into_typed<T: FromRow>(self) -> Result<Vec<T>> {
        self.collect()?
            .iter()
            .map(|row| T::from_row(&row.typed_row()).map_err(Error::Conversion))
            .collect()
    }

    pub fn cursor(&self) -> Option<&Cursor> {
        self.inner.cursor()
    }

    pub fn scroll(&mut self, to: Scroll) -> Result<()> {
        block_on_io(|cx| async move { self.inner.scroll(&cx, to).await })
    }
}

fn first_cursor_from_result(result: &QueryResult) -> Option<Cursor> {
    result
        .implicit_resultsets
        .as_ref()
        .and_then(|values| values.iter().find_map(cursor_from_value))
        .or_else(|| {
            result
                .rows
                .iter()
                .flat_map(|row| row.iter())
                .find_map(|cell| cell.as_ref().and_then(cursor_from_value))
        })
}

fn cursor_from_value(value: &QueryValue) -> Option<Cursor> {
    match value {
        QueryValue::Cursor(cursor) => Some((**cursor).clone()),
        _ => None,
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
            | Error::ConnectionClosed(_)
            | Error::Tls(_) => ErrorKind::Network,
            Error::CallTimeout(_) => ErrorKind::Timeout,
            Error::Cancelled => ErrorKind::Cancel,
            Error::Conversion(_) | Error::Bind(_) => ErrorKind::Conversion,
            #[cfg(feature = "arrow")]
            Error::ArrowConversion(_) => ErrorKind::Conversion,
            Error::RedirectUnsupported
            | Error::Runtime(_)
            | Error::FastAuthRequired
            | Error::MissingSessionField(_)
            | Error::AccessTokenRequiresTcps
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
        f.write_str("AccessToken(***redacted***)")
    }
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
    /// When set, `(SERVER=emon)` is injected into the connect descriptor's
    /// `CONNECT_DATA`. This routes the connection to the database EMON process
    /// used to push CQN notifications (reference `subscr.pyx` rewrites
    /// `description.server_type = "emon"` for the background connection).
    server_type_emon: bool,
    /// TCPS wallet directory (`MY_WALLET_DIRECTORY` / `wallet_location`). The
    /// directory should contain `ewallet.pem` (or, with the `experimental`
    /// feature, `cwallet.sso`). When `None`, `TNS_ADMIN` is consulted; the
    /// special value `SYSTEM` (case-insensitive) forces the system trust store.
    /// Only consulted for TCPS connections.
    wallet_location: Option<String>,
    /// Password for an encrypted wallet (mTLS key). `None` for auto-login or
    /// verify-only wallets.
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
    /// Maximum number of open statements kept in this connection's statement
    /// cache. Defaults to 20 (the reference default). `0` disables caching
    /// entirely (every statement's cursor is closed after use, never retained),
    /// matching python-oracledb's `stmtcachesize=0`. The cache holds at most this
    /// many entries, each a small `(sql, cursor_id)` pair, so it is bounded by
    /// construction. Set with [`ConnectOptions::with_statement_cache_size`].
    statement_cache_size: usize,
    /// Resource policy for thin-protocol decoding and packet reassembly.
    protocol_limits: ProtocolLimits,
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
            server_type_emon: false,
            wallet_location: None,
            wallet_password: None,
            ssl_server_dn_match: true,
            ssl_server_cert_dn: None,
            use_sni: false,
            edition: None,
            access_token: None,
            statement_cache_size: STATEMENT_CACHE_SIZE,
            protocol_limits: ProtocolLimits::DEFAULT,
        }
    }

    /// Set the thin-protocol resource limits. Invalid policies are rejected at
    /// connect time before any network I/O.
    #[must_use]
    pub fn with_protocol_limits(mut self, limits: ProtocolLimits) -> Self {
        self.protocol_limits = limits;
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

    /// Set the wallet password (for an encrypted mTLS key).
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
    capabilities: ClientCapabilities,
    ttc_seq_num: u8,
    sdu: usize,
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
    dead: bool,
    /// Logon user, retained for the change-password call.
    user: String,
    /// Session combo key from verifier generation, retained for the
    /// change-password call (reference keeps `conn_impl._combo_key`).
    combo_key: Vec<u8>,
    /// LRU statement cache: SQL text -> open server cursor id (reference
    /// thin/statement_cache.pyx, default size 20).
    statement_cache: Vec<(String, u32)>,
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
    pub async fn connect(cx: &Cx, options: ConnectOptions) -> Result<Self> {
        observe_cancellation_between_round_trips(cx)?;
        let protocol_limits = options.protocol_limits.validate()?;
        let descriptor = EasyConnect::parse(&options.connect_string)?;
        // Connect span (feature-gated, zero-cost when off). Carries only the
        // server address / port / service — never the password.
        let _span = obs_span!(
            "oracledb.connect",
            db.system = "oracle",
            server.address = %descriptor.host,
            server.port = descriptor.port as u64,
            db.name = %descriptor.service_name,
        );
        let identity = options.identity;
        trace_connect_step("tcp connect");
        let stream = TcpStream::connect_timeout(
            (descriptor.host.clone(), descriptor.port),
            Duration::from_secs(20),
        )
        .await?;
        stream.set_nodelay(true)?;
        trace_connect_step("tcp connected");

        // TCPS: complete the TLS handshake on the whole socket before splitting
        // and before any TNS bytes are sent (implicit TLS, matching
        // python-oracledb thin's _connect_tcp ordering).
        let connector = DriverConnector::default();
        let (read, write) = if descriptor.protocol.is_tls() {
            trace_connect_step("tls handshake");
            let server_type = if options.server_type_emon {
                Some("emon")
            } else {
                None
            };
            let tls_params = tls::resolve_tls_params(
                &descriptor,
                options.wallet_location.as_deref(),
                options.wallet_password.as_deref(),
                options.ssl_server_dn_match,
                options.ssl_server_cert_dn.as_deref(),
                options.use_sni,
            )?;
            let tls_stream =
                tls::tls_handshake(&descriptor, server_type, &tls_params, stream).await?;
            trace_connect_step("tls established");
            connector.tls_split(tls_stream)
        } else {
            connector.plain_split(stream)
        };
        let mut core = ConnectionCore::from_halves(read, write, "oracle_tcp_write");
        core.set_protocol_limits(protocol_limits)?;

        let connect_descriptor = listener_connect_descriptor_with_server(
            &descriptor,
            &identity,
            options.server_type_emon,
        );
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
        core.write_all(cx, &packet).await?;

        trace_connect_step("read ACCEPT");
        let accept = core.read_packet(PacketLengthWidth::Legacy16).await?;
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

        let mut ttc_seq_num = 1;
        let auth_connect_string = auth_connect_descriptor(&descriptor);

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
            core.send_data_packet(cx, &auth_one, sdu).await?;
            trace_connect_step("read AUTH phase one");
            let auth_one_response = core.read_data_response(cx).await?;
            trace_connect_bytes("AUTH phase one response", &auth_one_response);
            let auth_one = parse_auth_response_with_limits(&auth_one_response, protocol_limits)?;
            let capabilities = auth_one.capabilities.unwrap_or_default();
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
            )?;
            trace_connect_bytes("AUTH phase two payload", &auth_two_payload);
            trace_connect_step("send AUTH phase two");
            core.send_data_packet(cx, &auth_two_payload, sdu).await?;
            trace_connect_step("read AUTH phase two");
            let auth_two_response = core.read_data_response(cx).await?;
            trace_connect_bytes("AUTH phase two response", &auth_two_response);
            let auth_two = parse_auth_response_with_limits(&auth_two_response, protocol_limits)?;
            oracledb_protocol::crypto::verify_server_response(
                &encrypted.combo_key,
                &auth_two.session_data,
            )?;
            (auth_two, capabilities, encrypted.combo_key)
        };

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
            core,
            protocol_limits,
            session_id,
            serial_num,
            server_version,
            server_version_tuple,
            capabilities,
            ttc_seq_num,
            sdu,
            supports_end_of_response: accept_info.supports_end_of_response,
            supports_oob: accept_info.supports_oob,
            cursor_columns: BTreeMap::new(),
            fetch_metadata_by_sql: HashMap::new(),
            fetch_metadata_order: VecDeque::new(),
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
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        let response = self.core.read_data_response(cx).await?;
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
        let response = self.core.read_data_response(cx).await?;
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
        let response = self.core.read_data_response(cx).await?;
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

    /// Execute a registerquery: run `sql` with the CQN `registration_id`
    /// threaded into the execute body and return the query id read back from the
    /// registration-info block (reference `ThinSubscrImpl.register_query` ->
    /// `cursor_impl._query_id`). Returns `Some(0)`/`None` when the server sent no
    /// query id (qos without SUBSCR_QOS_QUERY). Server errors (ORA-00942,
    /// ORA-29975) surface unchanged.
    #[deprecated(
        since = "0.3.0",
        note = "use Connection::register_query; see docs/MIGRATING-0.3.md"
    )]
    pub async fn execute_query_for_registration(
        &mut self,
        cx: &Cx,
        sql: &str,
        registration_id: u64,
    ) -> Result<Option<u64>> {
        self.register_query(
            cx,
            Registration::owned_sql(sql.to_string(), registration_id),
        )
        .await
        .map(|outcome| outcome.query_id())
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
        match time::timeout(
            time::wall_now(),
            Duration::from_millis(u64::from(timeout_ms)),
            self.ping(cx),
        )
        .await
        {
            Ok(result) => result,
            // Previously this returned bare CallTimeout without even sending a
            // BREAK, leaving the half-sent ping round trip on the wire to poison
            // the next reuse. Break + drain like every other timeout path.
            Err(_) => self.recover_from_call_timeout(cx, timeout_ms).await,
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
        let response = self.core.read_data_response(cx).await?;
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
        let response = self.core.read_data_response(cx).await?;
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
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload =
            build_tpc_switch_payload_with_seq(seq_num, operation, flags, timeout, xid, context);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        let response = self.core.read_data_response(cx).await?;
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
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_tpc_change_state_payload_with_seq(
            seq_num,
            operation,
            requested_state,
            0,
            xid,
            context,
        );
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        let response = self.core.read_data_response(cx).await?;
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
    #[deprecated(
        since = "0.3.0",
        note = "use Connection::query/query_with for rows or Connection::execute/execute_with for DML/PLSQL; see docs/MIGRATING-0.3.md"
    )]
    pub async fn execute_query(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
    ) -> Result<QueryResult> {
        self.execute_query_with_bind_rows_and_options_core(
            cx,
            sql,
            prefetch_rows,
            &[],
            ExecuteOptions::default(),
        )
        .await
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
    #[deprecated(
        since = "0.3.0",
        note = "use Connection::query/query_with; Query materializes LOB/JSON/vector cells by default; see docs/MIGRATING-0.3.md"
    )]
    pub async fn execute_query_collect(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
    ) -> Result<QueryResult> {
        self.execute_query_collect_core(cx, sql, prefetch_rows)
            .await
    }

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

    #[deprecated(
        since = "0.3.0",
        note = "use Query::timeout with Connection::query/query_with or Execute::timeout with Connection::execute/execute_with; see docs/MIGRATING-0.3.md"
    )]
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
    #[deprecated(
        since = "0.3.0",
        note = "use Connection::query/query_with for rows or Connection::execute/execute_with for DML/PLSQL; see docs/MIGRATING-0.3.md"
    )]
    pub async fn execute_query_with_binds(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        binds: &[BindValue],
    ) -> Result<QueryResult> {
        self.execute_query_with_binds_core(cx, sql, prefetch_rows, binds)
            .await
    }

    #[deprecated(
        since = "0.3.0",
        note = "use Query::timeout with query/query_with or Execute::timeout with execute/execute_with; see docs/MIGRATING-0.3.md"
    )]
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
            Err(()) => {
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
                Ok(result) => result?,
                Err(()) => {
                    return self
                        .recover_from_call_timeout(cx, deadline.timeout_ms())
                        .await
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
        let Execute {
            sql,
            params,
            timeout,
            options,
        } = execute;
        let sql_owned = sql.into_owned();
        let binds = crate::sql_convert::resolve_params(&sql_owned, params)?;
        let bind_rows = if binds.is_empty() {
            Vec::new()
        } else {
            vec![binds]
        };
        let deadline = QueryDeadline::new(cx, timeout);
        let result = match deadline
            .run(self.execute_query_with_bind_rows_and_options_core(
                cx, &sql_owned, 0, &bind_rows, options,
            ))
            .await
        {
            Ok(result) => result?,
            Err(()) => {
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
            Err(()) => {
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
            Err(()) => {
                return self
                    .recover_from_call_timeout(cx, deadline.timeout_ms())
                    .await
            }
        };
        Ok(RegistrationOutcome::from_query_result(result))
    }

    /// Ergonomic execute with *named* binds. Pass the
    /// [`params!`](crate::params) named form
    /// (`params!{ ":id" => 40, ":name" => "alice" }`), which yields a
    /// `Vec<(String, BindValue)>`. The names are reordered to match the
    /// first-appearance order of the placeholders in `sql`, so the caller never
    /// has to track bind positions:
    ///
    /// ```no_run
    /// # use oracledb::{Connection, params};
    /// # use asupersync::Cx;
    /// # async fn demo(conn: &mut Connection, cx: &Cx) -> Result<(), oracledb::Error> {
    /// let rows = conn
    ///     .query_named(
    ///         cx,
    ///         "select * from emp where id = :id and name = :name",
    ///         params!{ ":id" => 40, ":name" => "alice" },
    ///     )
    ///     .await?;
    /// # let _ = rows; Ok(()) }
    /// ```
    #[deprecated(
        since = "0.3.0",
        note = "use Connection::query with params!{} named parameters; see docs/MIGRATING-0.3.md"
    )]
    pub async fn query_named(
        &mut self,
        cx: &Cx,
        sql: &str,
        named_params: Vec<(String, BindValue)>,
    ) -> Result<QueryResult> {
        let binds = crate::sql_convert::resolve_params(sql, crate::Params::from(named_params))?;
        self.execute_query_with_binds_core(cx, sql, 1, &binds).await
    }

    /// [`Self::query_named`] with a per-call timeout, for parity with the
    /// positional [`Self::execute_query_with_binds_and_timeout`]. `timeout_ms`
    /// bounds the round trip: on expiry the driver sends a BREAK and the call
    /// fails with [`Error::CallTimeout`] (the connection stays usable). `None`
    /// means no timeout. Like [`Self::query_named`], the named binds are
    /// reordered to the placeholders' first-appearance order in `sql`.
    #[deprecated(
        since = "0.3.0",
        note = "use Query::timeout with Connection::query and params!{} named parameters; see docs/MIGRATING-0.3.md"
    )]
    pub async fn query_named_with_timeout(
        &mut self,
        cx: &Cx,
        sql: &str,
        named_params: Vec<(String, BindValue)>,
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        let binds = crate::sql_convert::resolve_params(sql, crate::Params::from(named_params))?;
        self.execute_query_with_binds_call_timeout(cx, sql, 1, &binds, timeout_ms)
            .await
    }

    /// Execute `sql` once per bind row (array DML / `executemany`). Each inner
    /// `Vec<BindValue>` is one positional bind row; the server applies the
    /// statement to every row in a single round trip and reports the total in
    /// [`QueryResult::row_count`]. For per-iteration row counts or collected
    /// batch errors, use
    /// [`Self::execute_query_with_bind_rows_and_options`] with the matching
    /// [`ExecuteOptions`] flags.
    #[deprecated(
        since = "0.3.0",
        note = "use Connection::execute_many/execute_many_with for array DML or Connection::query/query_with for rows; see docs/MIGRATING-0.3.md"
    )]
    pub async fn execute_query_with_bind_rows(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        bind_rows: &[Vec<BindValue>],
    ) -> Result<QueryResult> {
        self.execute_query_with_bind_rows_and_options_core(
            cx,
            sql,
            prefetch_rows,
            bind_rows,
            ExecuteOptions::default(),
        )
        .await
    }

    #[deprecated(
        since = "0.3.0",
        note = "use execute_raw for the byte-identical raw QueryResult, or the curated families (Batch::raw_options with execute_many_with, Execute::raw_options with execute_with, Query builders); see docs/MIGRATING-0.3.md"
    )]
    pub async fn execute_query_with_bind_rows_and_options(
        &mut self,
        cx: &Cx,
        sql: &str,
        prefetch_rows: u32,
        bind_rows: &[Vec<BindValue>],
        exec_options: ExecuteOptions,
    ) -> Result<QueryResult> {
        self.execute_query_with_bind_rows_and_options_core(
            cx,
            sql,
            prefetch_rows,
            bind_rows,
            exec_options,
        )
        .await
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
        }
        // If a prior cancellable round trip was dropped mid-read, break + drain
        // the stranded call before issuing this execute (Scope cancel-on-drop).
        self.ensure_clean_before_request().await?;
        let mut exec_options = exec_options;
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
        if exec_options.cursor_id() == 0 && !exec_options.parse_only() {
            if use_cache {
                if self.statement_is_in_use(sql) {
                    // cached cursor busy: this execute parses a fresh (copy)
                    // cursor that must not be returned to the cache
                    is_copy = true;
                } else if let Some(cursor_id) = self.statement_cache_get(sql) {
                    exec_options = exec_options.with_cursor_id(cursor_id);
                }
            } else if let Some(cursor_id) = self.statement_cache_take(sql) {
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
        // Read under a cancel-on-drop guard: a dropped execute future arms the
        // next operation's break + drain.
        let response = self.read_flushing_out_binds_cancellable(cx).await?;
        trace_query_bytes("EXECUTE query response", &response);
        let known_columns = if exec_options.cursor_id() != 0 {
            self.cursor_columns
                .get(&exec_options.cursor_id())
                .cloned()
                .unwrap_or_default()
        } else {
            Vec::new()
        };
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
                    self.statement_cache_put(sql, result.cursor_id);
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

    #[deprecated(
        since = "0.3.0",
        note = "use Batch::timeout with Connection::execute_many_with or Query::timeout with Connection::query_with; see docs/MIGRATING-0.3.md"
    )]
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

    #[deprecated(
        since = "0.3.0",
        note = "use execute_raw (pass timeout_ms) for the byte-identical raw QueryResult, or the curated families (Batch::raw_options(...).timeout(...) with execute_many_with, Execute::raw_options(...).timeout(...) with execute_with, Query builders); see docs/MIGRATING-0.3.md"
    )]
    pub async fn execute_query_with_bind_rows_options_and_timeout(
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
        match time::timeout(
            time::wall_now(),
            Duration::from_millis(u64::from(timeout_ms)),
            self.execute_query_with_bind_rows_and_options_core(
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
            Err(_) => self.recover_from_call_timeout(cx, timeout_ms).await,
        }
    }

    /// If a previous cancellable fetch future was dropped mid-read (its
    /// [`CancelDrainGuard`] moved the recovery phase to `BreakSent`), break +
    /// drain the stranded server call now — before this round trip sends its own
    /// request — so the leftover bytes / still-running call cannot poison this
    /// response. A failed drain marks the connection dead and surfaces
    /// [`Error::ConnectionClosed`].
    async fn ensure_clean_before_request(&mut self) -> Result<()> {
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

    /// Read one TTC response under a [`CancelDrainGuard`]: if THIS read future is
    /// dropped mid-flight (the fetch was cancelled / raced), the guard moves the
    /// recovery phase to `BreakSent` so the next operation breaks + drains the
    /// stranded call. A normal completion disarms the guard, so the uncancelled
    /// path costs nothing beyond an `Arc::clone`.
    async fn read_response_cancellable(&mut self, cx: &Cx) -> Result<Vec<u8>> {
        // Clone the Arc so the guard owns a handle independent of the `&mut self`
        // read borrow (the two touch disjoint state but the borrow checker can't
        // prove it across the guard's lifetime).
        let recovery = Arc::clone(&self.core.recovery);
        let mut guard = CancelDrainGuard::arm(recovery)?;
        let response = self.core.read_data_response(cx).await?;
        guard.disarm();
        Ok(response)
    }

    /// [`Self::read_response_cancellable`] for the bind/execute path, which reads
    /// via [`read_data_response_flushing_out_binds`] (it answers FLUSH_OUT_BINDS
    /// requests). Same cancel-on-drop semantics: a dropped execute future arms
    /// the next operation's break + drain.
    async fn read_flushing_out_binds_cancellable(&mut self, cx: &Cx) -> Result<Vec<u8>> {
        let recovery = Arc::clone(&self.core.recovery);
        let mut guard = CancelDrainGuard::arm(recovery)?;
        let response = self
            .core
            .read_data_response_flushing_out_binds(cx, self.sdu)
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
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_fetch_payload_with_seq(cursor_id, arraysize, seq_num);
        trace_query_bytes("FETCH payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        // Read under a cancel-on-drop guard: if THIS fetch future is dropped
        // mid-read, the next operation will break + drain the stranded call.
        let profile = fetch_profile::enabled();
        let read_start = profile.then(time::wall_now);
        let response = self.read_response_cancellable(cx).await?;
        if let Some(start) = read_start {
            fetch_profile::add_read(time::wall_now().duration_since(start));
        }
        trace_query_bytes("FETCH response", &response);
        let columns = self
            .cursor_columns
            .get(&cursor_id)
            .cloned()
            .unwrap_or_else(|| known_columns.to_vec());
        let decode_start = profile.then(time::wall_now);
        let lob_prefetch = self.lob_prefetch_cursors.contains(&cursor_id);
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
    /// [`BorrowedFetchResult`](oracledb_protocol::thin::BorrowedFetchResult)
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
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_fetch_payload_with_seq(cursor_id, arraysize, seq_num);
        trace_query_bytes("FETCH payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        let profile = fetch_profile::enabled();
        let read_start = profile.then(time::wall_now);
        let response = self.read_response_cancellable(cx).await?;
        if let Some(start) = read_start {
            fetch_profile::add_read(time::wall_now().duration_since(start));
        }
        trace_query_bytes("FETCH response", &response);
        let columns = self
            .cursor_columns
            .get(&cursor_id)
            .cloned()
            .unwrap_or_default();
        let decode_start = profile.then(time::wall_now);
        let parsed = parse_query_response_borrowed_with_limits(
            &response,
            self.capabilities,
            &columns,
            previous_row,
            self.protocol_limits,
        );
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
    /// machinery that protects a dropped fetch, see [`CancelDrainGuard`]). A
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
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_fetch_payload_with_seq(cursor_id, arraysize, seq_num);
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
    /// fully decoded). The read runs under a [`CancelDrainGuard`] (a mid-read
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
        let profile = fetch_profile::enabled();
        let read_start = profile.then(time::wall_now);
        let response = self.read_response_cancellable(cx).await?;
        if let Some(start) = read_start {
            fetch_profile::add_read(time::wall_now().duration_since(start));
        }
        trace_query_bytes("FETCH response (prefetch)", &response);
        let columns = self
            .cursor_columns
            .get(&cursor_id)
            .cloned()
            .unwrap_or_default();
        let decode_start = profile.then(time::wall_now);
        let parsed = parse_query_response_borrowed_with_limits(
            &response,
            self.capabilities,
            &columns,
            previous_row,
            self.protocol_limits,
        );
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
    /// of borrowed [`QueryValueRef`](oracledb_protocol::thin::QueryValueRef) —
    /// the zero-copy fetch fast path. Scalar cells (Text / Number / Raw /
    /// Boolean / Interval / DateTime) borrow the fetch buffer directly, so a
    /// Rust consumer iterating a wide many-row result pays ~0 allocations per
    /// cell, in contrast to the owned [`execute_query`](Self::execute_query) +
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
            self.fetch_rows_request(cx, cursor_id, arraysize).await?;
        }

        while more_rows && cursor_id != 0 {
            // Read + decode the page whose request is already in flight.
            let result = self
                .fetch_rows_ref_response(cx, cursor_id, previous_row.as_deref())
                .await?;
            let next_more = result.more_rows;

            // Speculatively request the NEXT page BEFORE running the callback, so
            // its round trip overlaps this page's decode + the callback's work.
            // The request needs no data from `result`, so `result`'s buffer stays
            // alive and untouched across this send.
            if next_more {
                self.fetch_rows_request(cx, cursor_id, arraysize).await?;
            }

            // Snapshot the last row for the next page's duplicate-column seed
            // before consuming the batch in the callback.
            let mut last_owned: Option<Vec<Option<oracledb_protocol::thin::QueryValue>>> = None;
            result.batch.for_each_row_ref(|row| {
                last_owned = Some(
                    row.iter()
                        .map(|cell| cell.map(|v| v.to_owned_value()))
                        .collect(),
                );
                callback(row)
            })?;
            if let Some(last) = last_owned {
                previous_row = Some(last);
            }
            more_rows = next_more;
        }

        self.release_cursor(cursor_id);
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
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload =
            build_define_fetch_payload_with_seq(cursor_id, arraysize, seq_num, define_columns)?;
        trace_query_bytes("DEFINE FETCH payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        let response = self.core.read_data_response(cx).await?;
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
        let exec_options = ExecuteOptions::default()
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
        )?;
        if let Some(mut piggyback_bytes) = piggyback {
            piggyback_bytes.extend_from_slice(&payload);
            payload = piggyback_bytes;
        }
        trace_query_bytes("SCROLL payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        let response = self
            .core
            .read_data_response_flushing_out_binds(cx, self.sdu)
            .await?;
        trace_query_bytes("SCROLL response", &response);
        let known_columns = self
            .cursor_columns
            .get(&cursor_id)
            .cloned()
            .unwrap_or_default();
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
        let response = self.core.read_data_response(cx).await?;
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
        let response = self.core.read_data_response(cx).await?;
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
        let response = self.core.read_data_response(cx).await?;
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
        let response = self.core.read_data_response(cx).await?;
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
        let response = self.core.read_data_response(cx).await?;
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
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = build_lob_create_temp_payload_with_seq(
            ora_type_num,
            csfrm,
            seq_num,
            self.capabilities.ttc_field_version,
        )?;
        trace_query_bytes("LOB CREATE TEMP payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        let response = self.core.read_data_response(cx).await?;
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
        let response = self.core.read_data_response(cx).await?;
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
        let response = self.core.read_data_response(cx).await?;
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
        let response = self.core.read_data_response(cx).await?;
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
        match time::timeout(
            time::wall_now(),
            Duration::from_millis(u64::from(timeout_ms)),
            self.execute_query_with_bind_rows_and_options_core(
                cx,
                sql,
                prefetch_rows,
                &[],
                ExecuteOptions::default(),
            ),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => self.recover_from_call_timeout(cx, timeout_ms).await,
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
        match time::timeout(
            time::wall_now(),
            Duration::from_millis(u64::from(timeout_ms)),
            self.execute_query_with_binds_core(cx, sql, prefetch_rows, binds),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => self.recover_from_call_timeout(cx, timeout_ms).await,
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
                .execute_query_with_bind_rows_and_options_core(
                    cx,
                    sql,
                    prefetch_rows,
                    bind_rows,
                    ExecuteOptions::default(),
                )
                .await;
        };
        match time::timeout(
            time::wall_now(),
            Duration::from_millis(u64::from(timeout_ms)),
            self.execute_query_with_bind_rows_and_options_core(
                cx,
                sql,
                prefetch_rows,
                bind_rows,
                ExecuteOptions::default(),
            ),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => self.recover_from_call_timeout(cx, timeout_ms).await,
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
            Err(_) => self.recover_from_call_timeout(cx, timeout_ms).await,
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
            Err(_) => self.recover_from_call_timeout(cx, timeout_ms).await,
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
            Err(_) => self.recover_from_call_timeout(cx, timeout_ms).await,
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
            Err(_) => self.recover_from_call_timeout(cx, timeout_ms).await,
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
        self.protocol_limits.check_columns(column_names.len())?;
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = oracledb_protocol::dpl::build_direct_path_prepare_payload(
            schema_name,
            table_name,
            column_names,
            seq_num,
        )?;
        trace_query_bytes("DIRECT PATH PREPARE payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        let response = self.core.read_data_response(cx).await?;
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
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload = oracledb_protocol::dpl::build_direct_path_load_stream_payload(
            cursor_id, stream, seq_num,
        )?;
        trace_query_bytes("DIRECT PATH LOAD STREAM payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        let response = self.core.read_data_response(cx).await?;
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
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        let payload =
            oracledb_protocol::dpl::build_direct_path_op_payload(cursor_id, op_code, seq_num);
        trace_query_bytes("DIRECT PATH OP payload", &payload);
        self.core.send_data_packet(cx, &payload, self.sdu).await?;
        let response = self.core.read_data_response(cx).await?;
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
    async fn recover_from_call_timeout<T>(&mut self, cx: &Cx, timeout_ms: u32) -> Result<T> {
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
        let to_close = statement_cache_insert(
            &mut self.statement_cache,
            self.statement_cache_size,
            sql,
            cursor_id,
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
                        .retain(|(_, cached_id)| cached_id != cursor_id);
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
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        self.core
            .send_data_packet(
                cx,
                &build_function_payload_with_seq(TNS_FUNC_LOGOFF, seq_num),
                self.sdu,
            )
            .await?;
        if let Ok(response) = time::timeout(
            time::wall_now(),
            Duration::from_secs(5),
            self.core.read_data_response(cx),
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
        observe_cancellation_between_round_trips(cx)?;
        if requests.is_empty() {
            return Ok(Vec::new());
        }
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
                        &build_execute_payload_with_bind_rows_with_seq_and_token(
                            sql,
                            *prefetch_rows,
                            seq_num,
                            statement_is_query(sql),
                            bind_rows,
                            token_num,
                        )?,
                    );
                }
                PipelineRequest::Commit => {
                    payload.extend_from_slice(&build_function_payload_with_seq_and_token(
                        TNS_FUNC_COMMIT,
                        seq_num,
                        token_num,
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

    /// Runs a batch as a true single-round-trip pipeline (like [`run_pipeline`])
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
        let seq_num = next_ttc_sequence(&mut self.ttc_seq_num);
        self.core
            .send_data_packet(
                cx,
                &build_function_payload_with_seq(function_code, seq_num),
                self.sdu,
            )
            .await?;
        let response = self.core.read_data_response(cx).await?;
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

    /// Execute a registerquery (registration id into the execute, query id out).
    /// See [`Connection::execute_query_for_registration`].
    #[deprecated(
        since = "0.3.0",
        note = "use BlockingConnection::register_query; see docs/MIGRATING-0.3.md"
    )]
    pub fn execute_query_for_registration(
        connection: &mut Connection,
        sql: &str,
        registration_id: u64,
    ) -> Result<Option<u64>> {
        Self::register_query(
            connection,
            Registration::owned_sql(sql.to_string(), registration_id),
        )
        .map(|outcome| outcome.query_id())
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

    #[deprecated(
        since = "0.3.0",
        note = "use BlockingConnection::query/query_with for rows or BlockingConnection::execute/execute_with for DML/PLSQL; see docs/MIGRATING-0.3.md"
    )]
    pub fn execute_query(
        connection: &mut Connection,
        sql: &str,
        prefetch_rows: u32,
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .execute_query_with_bind_rows_and_options_core(
                    &cx,
                    sql,
                    prefetch_rows,
                    &[],
                    ExecuteOptions::default(),
                )
                .await
        })
    }

    /// Blocking wrapper for [`Connection::execute_query_collect`]: execute and
    /// return the first batch with `CLOB` / `BLOB` / `VECTOR` / native `JSON`
    /// cells fully materialized via an automatic define-fetch round trip.
    #[deprecated(
        since = "0.3.0",
        note = "use BlockingConnection::query/query_with; Query materializes LOB/JSON/vector cells by default; see docs/MIGRATING-0.3.md"
    )]
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
                .execute_query_collect_core(&cx, sql, prefetch_rows)
                .await
        })
    }

    #[deprecated(
        since = "0.3.0",
        note = "use Query::timeout with BlockingConnection::query/query_with or Execute::timeout with BlockingConnection::execute/execute_with; see docs/MIGRATING-0.3.md"
    )]
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

    #[deprecated(
        since = "0.3.0",
        note = "use BlockingConnection::query/query_with for rows or BlockingConnection::execute/execute_with for DML/PLSQL; see docs/MIGRATING-0.3.md"
    )]
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
                .execute_query_with_binds_core(&cx, sql, prefetch_rows, binds)
                .await
        })
    }

    #[deprecated(
        since = "0.3.0",
        note = "use Query::timeout with BlockingConnection::query/query_with or Execute::timeout with BlockingConnection::execute/execute_with; see docs/MIGRATING-0.3.md"
    )]
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

    /// Blocking wrapper for [`Connection::query_named`]: bind the
    /// [`params!`](crate::params) named form
    /// (`params!{ ":id" => 40 }`); names are reordered to the placeholder
    /// first-appearance order in `sql`.
    #[deprecated(
        since = "0.3.0",
        note = "use BlockingConnection::query with params!{} named parameters; see docs/MIGRATING-0.3.md"
    )]
    pub fn query_named(
        connection: &mut Connection,
        sql: &str,
        named_params: Vec<(String, BindValue)>,
    ) -> Result<QueryResult> {
        let binds = crate::sql_convert::resolve_params(sql, crate::Params::from(named_params))?;
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .execute_query_with_binds_core(&cx, sql, 1, &binds)
                .await
        })
    }

    /// Blocking wrapper for [`Connection::query_named_with_timeout`]: named binds
    /// plus a per-call timeout (`Error::CallTimeout` on expiry; `None` = no
    /// timeout).
    #[deprecated(
        since = "0.3.0",
        note = "use Query::timeout with BlockingConnection::query_with and params!{} named parameters; see docs/MIGRATING-0.3.md"
    )]
    pub fn query_named_with_timeout(
        connection: &mut Connection,
        sql: &str,
        named_params: Vec<(String, BindValue)>,
        timeout_ms: Option<u32>,
    ) -> Result<QueryResult> {
        let binds = crate::sql_convert::resolve_params(sql, crate::Params::from(named_params))?;
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .execute_query_with_binds_call_timeout(&cx, sql, 1, &binds, timeout_ms)
                .await
        })
    }

    #[deprecated(
        since = "0.3.0",
        note = "use BlockingConnection::execute_many/execute_many_with for array DML or BlockingConnection::query/query_with for rows; see docs/MIGRATING-0.3.md"
    )]
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
                .execute_query_with_bind_rows_and_options_core(
                    &cx,
                    sql,
                    prefetch_rows,
                    bind_rows,
                    ExecuteOptions::default(),
                )
                .await
        })
    }

    #[deprecated(
        since = "0.3.0",
        note = "use execute_raw for the byte-identical raw QueryResult, or the curated families (Batch::raw_options with execute_many_with, Execute::raw_options with execute_with, Query builders); see docs/MIGRATING-0.3.md"
    )]
    pub fn execute_query_with_bind_rows_and_options(
        connection: &mut Connection,
        sql: &str,
        prefetch_rows: u32,
        bind_rows: &[Vec<BindValue>],
        exec_options: ExecuteOptions,
    ) -> Result<QueryResult> {
        let runtime = build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            connection
                .execute_query_with_bind_rows_and_options_core(
                    &cx,
                    sql,
                    prefetch_rows,
                    bind_rows,
                    exec_options,
                )
                .await
        })
    }

    #[deprecated(
        since = "0.3.0",
        note = "use Batch::timeout with BlockingConnection::execute_many_with or Query::timeout with BlockingConnection::query_with; see docs/MIGRATING-0.3.md"
    )]
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

    #[deprecated(
        since = "0.3.0",
        note = "use execute_raw (pass timeout_ms) for the byte-identical raw QueryResult, or the curated families (Batch::raw_options(...).timeout(...) with execute_many_with, Execute::raw_options(...).timeout(...) with execute_with, Query builders); see docs/MIGRATING-0.3.md"
    )]
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
                .execute_query_with_bind_rows_options_call_timeout(
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
fn block_on_io<F, Fut, T>(operation: F) -> Result<T>
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

async fn lock_write<'a, W>(
    cx: &Cx,
    write: &'a Arc<AsyncMutex<W>>,
) -> Result<asupersync::sync::MutexGuard<'a, W>>
where
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    write
        .lock(cx)
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
) -> Result<asupersync::sync::MutexGuard<'_, W>>
where
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    write.try_lock().map_err(|err| match err {
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
    break_and_drain_wire_unbounded_with_limits(read, write, ProtocolLimits::DEFAULT).await
}

async fn break_and_drain_wire_unbounded_with_limits<R, W>(
    read: &mut R,
    write: &Arc<AsyncMutex<W>>,
    limits: ProtocolLimits,
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
    drain_break_response_recovery_with_limits(read, write, limits).await
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
    drain_cancel_wire_unbounded_with_limits(read, write, ProtocolLimits::DEFAULT).await
}

async fn drain_cancel_wire_unbounded_with_limits<R, W>(
    read: &mut R,
    write: &Arc<AsyncMutex<W>>,
    limits: ProtocolLimits,
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    drain_break_response_recovery_with_limits(read, write, limits).await
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
/// A read that completes normally calls [`CancelDrainGuard::disarm`] first, so
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
    drain_break_response_recovery_with_limits(read, write, ProtocolLimits::DEFAULT).await
}

async fn drain_break_response_recovery_with_limits<R, W>(
    read: &mut R,
    write: &Arc<AsyncMutex<W>>,
    limits: ProtocolLimits,
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
                return Err(oracledb_protocol::ProtocolError::UnknownMessageType {
                    message_type: other,
                    position: 4,
                }
                .into())
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
    let trailing =
        read_data_response_boundary_from_recovery_with_limits(read, write, pending, limits).await?;
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
    read_data_response_boundary_seeded(read, Some(cx), write, in_pipeline, None, limits).await
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
    )
    .await
}

async fn read_data_response_boundary_from_recovery_with_limits<R, W>(
    read: &mut R,
    write: &Arc<AsyncMutex<W>>,
    seed: Option<IncomingPacket>,
    limits: ProtocolLimits,
) -> Result<DataResponse>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + std::fmt::Debug + Unpin,
{
    read_data_response_boundary_seeded(read, None, write, false, seed, limits).await
}

async fn read_data_response_boundary_seeded<R, W>(
    read: &mut R,
    cx: Option<&Cx>,
    write: &Arc<AsyncMutex<W>>,
    in_pipeline: bool,
    seed: Option<IncomingPacket>,
    limits: ProtocolLimits,
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

/// Builds the listener connect descriptor, optionally injecting `(SERVER=emon)`
/// into `CONNECT_DATA` (between `SERVICE_NAME` and `CID`, matching the golden
/// emon connect packet). The reference sets `description.server_type = "emon"`
/// for the background CQN connection (subscr.pyx:70-73).
fn listener_connect_descriptor_with_server(
    descriptor: &EasyConnect,
    identity: &ClientIdentity,
    server_type_emon: bool,
) -> String {
    let server = if server_type_emon {
        "(SERVER=emon)"
    } else {
        ""
    };
    format!(
        "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST={})(PORT={}))(CONNECT_DATA=(SERVICE_NAME={}){}(CID=(PROGRAM={})(HOST={})(USER={}))))",
        descriptor.host,
        descriptor.port,
        descriptor.service_name,
        server,
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

/// LRU statement-cache insert, bounded to `capacity`. Moves an existing entry
/// for `sql` to most-recently-used (replacing its cursor) and evicts the oldest
/// entries past `capacity`. Returns the cursor ids that should be closed (the
/// replaced cursor and any evicted ones). `capacity == 0` disables caching: the
/// freshly inserted cursor is itself evicted and returned for closing, so the
/// cache stays empty (python-oracledb `stmtcachesize=0`). A `cursor_id` of 0
/// (no server cursor) is never cached. Pure so it is unit-testable without a
/// live connection.
fn statement_cache_insert(
    cache: &mut Vec<(String, u32)>,
    capacity: usize,
    sql: &str,
    cursor_id: u32,
) -> Vec<u32> {
    let mut to_close = Vec::new();
    if cursor_id == 0 {
        return to_close;
    }
    if let Some(index) = cache.iter().position(|(cached_sql, _)| cached_sql == sql) {
        let (_, cached_id) = cache.remove(index);
        if cached_id != 0 && cached_id != cursor_id {
            to_close.push(cached_id);
        }
    }
    cache.push((sql.to_string(), cursor_id));
    while cache.len() > capacity {
        let (_, evicted_id) = cache.remove(0);
        if evicted_id != 0 {
            to_close.push(evicted_id);
        }
    }
    to_close
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
    use asupersync::types::Budget;
    use std::future::{poll_fn, Future};
    use std::io::Read;
    use std::net::TcpListener;
    use std::pin::pin;
    use std::sync::atomic::AtomicBool;
    use std::task::{Poll, Waker};
    use std::thread;
    use std::time::Duration;

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
            "Array",
            "Vector",
            "Json",
            "Cursor",
        ];
        assert_eq!(bind_variants.len(), 22);
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
            "Object",
            "Lob",
            "Vector",
            "Json",
            "Array",
        ];
        assert_eq!(query_variants.len(), 16);
        for variant in query_variants {
            assert!(
                design.contains(&format!("`{variant}`")),
                "API_DESIGN.md missing QueryValue::{variant}"
            );
        }
    }

    #[test]
    fn migration_guide_covers_every_deprecated_method() {
        // No orphan deprecation: every old execute/query name that carries a
        // `#[deprecated(since = "0.3.0")]` shim must appear in the user-facing
        // 0.3.0 migration guide, so an external consumer can always find the
        // replacement. These are exactly the names listed in API_DESIGN.md §8.
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
        // the shims disappear before 1.0.0-rc.1.
        assert!(
            guide.contains("1.0.0-rc.1"),
            "MIGRATING-0.3.md must state the shims are removed before 1.0.0-rc.1"
        );
    }

    #[test]
    fn statement_cache_evicts_lru_past_capacity() {
        let mut cache = Vec::new();
        // capacity 2: a third distinct statement evicts the oldest (cursor 10).
        assert!(statement_cache_insert(&mut cache, 2, "a", 10).is_empty());
        assert!(statement_cache_insert(&mut cache, 2, "b", 11).is_empty());
        assert_eq!(statement_cache_insert(&mut cache, 2, "c", 12), vec![10]);
        assert_eq!(
            cache,
            vec![("b".into(), 11), ("c".into(), 12)],
            "LRU order retained"
        );
        // Re-inserting an existing SQL with a new cursor closes the old cursor
        // and moves it to most-recently-used; nothing is evicted.
        assert_eq!(statement_cache_insert(&mut cache, 2, "b", 99), vec![11]);
        assert_eq!(cache, vec![("c".into(), 12), ("b".into(), 99)]);
    }

    #[test]
    fn statement_cache_size_zero_disables_caching() {
        let mut cache = Vec::new();
        // capacity 0: the freshly inserted cursor is itself evicted (queued for
        // close) and the cache stays empty — caching disabled.
        assert_eq!(statement_cache_insert(&mut cache, 0, "a", 10), vec![10]);
        assert!(cache.is_empty(), "size 0 must never retain a statement");
        // A no-cursor (0) insert is never cached and closes nothing.
        assert!(statement_cache_insert(&mut cache, 5, "a", 0).is_empty());
        assert!(cache.is_empty());
    }

    #[test]
    fn query_arraysize_updates_default_prefetch_until_overridden() {
        let seven = NonZeroU32::new(7).expect("non-zero");
        let eleven = NonZeroU32::new(11).expect("non-zero");

        let query = Query::new("select * from dual").arraysize(seven);
        assert_eq!(query.arraysize, seven);
        assert_eq!(
            query.prefetch, 7,
            "default prefetch follows arraysize when not explicitly set"
        );

        let query = Query::new("select * from dual")
            .prefetch(3)
            .arraysize(eleven);
        assert_eq!(query.arraysize, eleven);
        assert_eq!(query.prefetch, 3, "explicit prefetch must be stable");
    }

    #[test]
    fn query_deadline_captures_one_absolute_query_timeout() {
        let runtime = build_io_runtime().expect("runtime");
        runtime.block_on(async {
            let cx = Cx::current().expect("runtime installs Cx");

            let deadline = QueryDeadline::new(&cx, Some(Duration::from_secs(5)));
            let captured = deadline.deadline.expect("query timeout sets deadline");

            assert_eq!(deadline.timeout_ms(), 5_000);
            assert_eq!(
                deadline.deadline,
                Some(captured),
                "the deadline is captured once and then carried by value"
            );
        });
    }

    #[test]
    fn row_reuses_typed_row_conversion_path() {
        #[derive(Debug, Eq, PartialEq)]
        struct Named {
            id: i64,
            name: String,
        }

        impl FromRow for Named {
            fn from_row(row: &TypedRow<'_>) -> std::result::Result<Self, ConversionError> {
                Ok(Self {
                    id: row.try_get_by_name("id")?,
                    name: row.try_get_by_name("name")?,
                })
            }
        }

        let columns: Arc<[ColumnMetadata]> = Arc::from(
            vec![
                ColumnMetadata::new("ID", 0),
                ColumnMetadata::new("NAME", 0),
                ColumnMetadata::new("NICK", 0),
            ]
            .into_boxed_slice(),
        );
        let row = Row::new(
            columns,
            vec![
                Some(QueryValue::number_from_text("42", true)),
                Some(QueryValue::Text("alice".to_string())),
                None,
            ],
        );

        assert_eq!(row.get::<i64>(0).unwrap(), 42);
        assert_eq!(row.get::<i64>("id").unwrap(), 42);
        assert_eq!(row.get_by_name::<i64>("id").unwrap(), 42);
        assert_eq!(row.get::<String>(1).unwrap(), "alice");
        assert_eq!(row.get::<String>("NAME").unwrap(), "alice");
        assert_eq!(
            row.value("name").and_then(QueryValue::as_text),
            Some("alice")
        );
        assert_eq!(row.value(1).and_then(QueryValue::as_text), Some("alice"));
        assert_eq!(row.try_get::<String>(2).unwrap(), None);
        assert_eq!(row.try_get::<String>("nick").unwrap(), None);
        assert!(row.try_get::<String>(99).is_err());
        assert!(row.try_get::<String>("missing").is_err());
        assert_eq!(
            Named::from_row(&row.typed_row()).unwrap(),
            Named {
                id: 42,
                name: "alice".to_string()
            }
        );
    }

    #[test]
    fn execute_raw_options_preserves_full_escape_hatch() {
        let options = ExecuteOptions::default()
            .with_batcherrors(true)
            .with_arraydmlrowcounts(true)
            .with_parse_only(true)
            .with_token_num(7)
            .with_cursor_id(11)
            .with_cache_statement(false)
            .with_scrollable(true)
            .with_fetch_orientation(TNS_FETCH_ORIENTATION_ABSOLUTE)
            .with_fetch_pos(3)
            .with_scroll_operation(true)
            .with_suspend_on_success(true)
            .with_no_prefetch(true)
            .with_registration_id(13);

        let execute = Execute::new("begin null; end;").raw_options(options);

        assert_eq!(execute.options, options);
    }

    #[test]
    fn execute_outcome_projects_query_result_fields() {
        let result = QueryResult {
            row_count: 7,
            last_rowid: Some("AAABBB".to_string()),
            out_values: vec![(0, Some(QueryValue::Text("out".to_string())))],
            return_values: vec![(1, vec![Some(QueryValue::number_from_text("42", true))])],
            implicit_resultsets: Some(vec![QueryValue::Cursor(Box::new(CursorValue {
                columns: Vec::new(),
                cursor_id: 99,
            }))]),
            compilation_error_warning: true,
            ..QueryResult::default()
        };

        let outcome = ExecuteOutcome::from_query_result(result);

        assert_eq!(outcome.rows_affected(), 7);
        assert_eq!(outcome.last_rowid(), Some("AAABBB"));
        assert_eq!(
            outcome.out_binds().get(0),
            Some(&Some(QueryValue::Text("out".to_string())))
        );
        assert_eq!(
            outcome
                .returning()
                .rows_for(1)
                .and_then(|rows| rows.first())
                .and_then(Option::as_ref)
                .and_then(QueryValue::as_i64),
            Some(42)
        );
        assert_eq!(outcome.implicit_results()[0].cursor_id, 99);
        assert_eq!(
            outcome.compilation_warning(),
            Some(ExecuteOutcome::COMPILATION_WARNING)
        );
    }

    #[test]
    fn batch_builder_sets_batch_execution_flags() {
        let rows = vec![
            vec![BindValue::Number("1".to_string())],
            vec![BindValue::Number("2".to_string())],
        ];

        let batch = Batch::new("delete from t where id = :1", &rows)
            .collect_errors()
            .row_counts()
            .timeout(Duration::from_secs(3));

        assert!(matches!(batch.rows, BatchRows::Borrowed(_)));
        assert!(batch.options.batcherrors());
        assert!(batch.options.arraydmlrowcounts());
        assert_eq!(batch.timeout, Some(Duration::from_secs(3)));
    }

    #[test]
    fn batch_raw_options_preserves_escape_hatch() {
        let rows = vec![vec![BindValue::Number("1".to_string())]];
        let options = ExecuteOptions::default()
            .with_batcherrors(true)
            .with_arraydmlrowcounts(true)
            .with_parse_only(true)
            .with_token_num(9)
            .with_cursor_id(17)
            .with_cache_statement(false)
            .with_scrollable(true)
            .with_fetch_orientation(TNS_FETCH_ORIENTATION_ABSOLUTE)
            .with_fetch_pos(4)
            .with_scroll_operation(true)
            .with_suspend_on_success(true)
            .with_no_prefetch(true)
            .with_registration_id(21);

        let batch = Batch::new("begin null; end;", rows).raw_options(options);

        assert_eq!(batch.options, options);
    }

    #[test]
    fn batch_outcome_projects_query_result_fields() {
        let result = QueryResult {
            row_count: 3,
            batch_errors: vec![BatchServerError::new(1, 2, "bad row")],
            array_dml_row_counts: Some(vec![1, 0, 1]),
            return_values: vec![(0, vec![Some(QueryValue::Text("AAABBB".to_string()))])],
            ..QueryResult::default()
        };

        let outcome = BatchOutcome::from_query_result(result);

        assert_eq!(outcome.rows_affected(), 3);
        assert_eq!(outcome.per_row_counts(), Some([1, 0, 1].as_slice()));
        assert_eq!(outcome.errors()[0].row_index(), 2);
        assert_eq!(outcome.errors()[0].code(), 1);
        assert_eq!(outcome.errors()[0].message(), "bad row");
        assert_eq!(
            outcome
                .returning()
                .rows_for(0)
                .and_then(|rows| rows.first())
                .and_then(Option::as_ref)
                .and_then(QueryValue::as_text),
            Some("AAABBB")
        );
    }

    #[test]
    fn batch_outcome_coalesces_array_dml_returning_per_bind() {
        // Regression: array DML decodes RETURNING once per iteration, so a
        // single RETURNING bind (index 2) arrives as one group per affected
        // input row. BatchOutcome must coalesce groups that share a bind index
        // so rows_for(2) exposes every affected row's value, not just the first
        // iteration's. (Found by the W3-E7.4 live e2e suite.)
        let result = QueryResult {
            row_count: 2,
            array_dml_row_counts: Some(vec![1, 1]),
            return_values: vec![
                (2, vec![Some(QueryValue::Text("first".to_string()))]),
                (2, vec![Some(QueryValue::Text("second".to_string()))]),
            ],
            ..QueryResult::default()
        };

        let outcome = BatchOutcome::from_query_result(result);

        // One coalesced group for bind index 2 (not one group per iteration).
        assert_eq!(outcome.returning().len(), 1);
        let rows = outcome
            .returning()
            .rows_for(2)
            .expect("returning group for bind index 2");
        assert_eq!(
            rows.len(),
            2,
            "both affected rows' RETURNING values must be present"
        );
        assert_eq!(
            rows[0].as_ref().and_then(QueryValue::as_text),
            Some("first")
        );
        assert_eq!(
            rows[1].as_ref().and_then(QueryValue::as_text),
            Some("second")
        );
    }

    #[test]
    fn empty_batch_outcome_preserves_requested_row_counts_shape() {
        let without_counts = BatchOutcome::empty(false);
        let with_counts = BatchOutcome::empty(true);

        assert_eq!(without_counts.rows_affected(), 0);
        assert_eq!(without_counts.per_row_counts(), None);
        assert_eq!(with_counts.rows_affected(), 0);
        assert_eq!(with_counts.per_row_counts(), Some([].as_slice()));
    }

    #[test]
    fn batch_rows_reject_ragged_bind_shapes() {
        let rows = vec![
            vec![
                BindValue::Number("1".to_string()),
                BindValue::Text("a".into()),
            ],
            vec![BindValue::Number("2".to_string())],
        ];
        let batch = Batch::new("insert into t values (:1, :2)", &rows);

        let err = batch.rows.validate_rectangular().unwrap_err();

        assert!(matches!(
            err,
            Error::Bind(BindError::BatchRowWidthMismatch {
                row_index: 1,
                expected: 2,
                actual: 1
            })
        ));
    }

    #[test]
    fn registration_builder_carries_params_subscription_and_timeout() {
        let registration = Registration::new("select * from rust_cqn_t where id = :id", 42)
            .bind(vec![(
                ":id".to_string(),
                BindValue::Number("7".to_string()),
            )])
            .timeout(Duration::from_secs(9));

        assert_eq!(registration.registration_id, 42);
        assert_eq!(registration.timeout, Some(Duration::from_secs(9)));
        match registration.params {
            Params::Named(values) => {
                assert_eq!(values[0].0, ":id");
                assert_eq!(values[0].1, BindValue::Number("7".to_string()));
            }
            other => panic!("expected named params, got {other:?}"),
        }
    }

    #[test]
    fn registration_outcome_projects_query_id() {
        let with_id = RegistrationOutcome::from_query_result(QueryResult {
            query_id: Some(123),
            ..QueryResult::default()
        });
        let zero_id = RegistrationOutcome::from_query_result(QueryResult {
            query_id: Some(0),
            ..QueryResult::default()
        });
        let without_id = RegistrationOutcome::from_query_result(QueryResult::default());

        assert_eq!(with_id.query_id(), Some(123));
        assert_eq!(zero_id.query_id(), None);
        assert_eq!(without_id.query_id(), None);
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

    fn loopback_connection(
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
            capabilities: ClientCapabilities::default(),
            protocol_limits: ProtocolLimits::DEFAULT,
            ttc_seq_num: 0,
            sdu: 8192,
            supports_end_of_response: true,
            supports_oob: false,
            cursor_columns: BTreeMap::new(),
            fetch_metadata_by_sql: HashMap::new(),
            fetch_metadata_order: VecDeque::new(),
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
        let payload = build_fetch_payload_with_seq(42, 2, 2);
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
        let payload = build_function_payload_with_seq(function_code, seq_num);
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
    #[allow(deprecated)]
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
                .execute_query(&cx, "select value from synthetic_fixture", 2)
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
        let built = listener_connect_descriptor_with_server(&descriptor, &options.identity, false);
        assert!(built.contains("(PROGRAM=program)"));
        assert!(built.contains("(HOST=machine)"));
        assert!(built.contains("(USER=osuser)"));
        assert!(!built.contains("(SERVER=emon)"));
        // emon variant injects the SERVER directive ahead of the CID block
        let emon = listener_connect_descriptor_with_server(&descriptor, &options.identity, true);
        assert!(emon.contains("(SERVICE_NAME=FREEPDB1)(SERVER=emon)(CID="));
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
}
