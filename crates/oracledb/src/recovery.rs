use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake};
use std::time::{Duration, Instant};

use asupersync::io::{AsyncRead, AsyncWrite};
use asupersync::sync::Mutex as AsyncMutex;
use asupersync::types::{CancelKind, CancelReason};
use asupersync::{time, Cx};
use oracledb_protocol::wire::ProtocolLimits;

use crate::{
    break_and_drain_wire_unbounded_with_limits, drain_cancel_wire_unbounded_with_limits,
    duration_to_millis_saturating, Error, ErrorKind, Result,
};

#[derive(Clone, Copy, Debug)]
pub(crate) enum RecoveryWireAction {
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

pub(crate) fn classify_recovery_result(
    action: RecoveryWireAction,
    result: Option<Result<()>>,
) -> Result<()> {
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

pub(crate) fn run_recovery_without_current_cx<R, W>(
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
pub(crate) enum CancelDisposition {
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
    pub(crate) fn from_kind(kind: CancelKind) -> Self {
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
    pub(crate) fn into_error(self, timeout_ms: u32) -> Error {
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
pub(crate) fn cancel_disposition(reason: Option<CancelReason>) -> CancelDisposition {
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
pub(crate) fn observe_cancellation_between_round_trips(cx: &Cx) -> Result<()> {
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
pub(crate) enum SessionRecoveryPhase {
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
pub(crate) struct SessionRecovery {
    phase: AtomicU8,
}

impl SessionRecovery {
    pub(crate) fn new() -> Self {
        Self {
            phase: AtomicU8::new(SessionRecoveryPhase::Ready as u8),
        }
    }

    pub(crate) fn phase(&self) -> SessionRecoveryPhase {
        SessionRecoveryPhase::from_u8(self.phase.load(Ordering::SeqCst))
    }

    pub(crate) fn is_dead(&self) -> bool {
        self.phase() == SessionRecoveryPhase::Dead
    }

    pub(crate) fn begin_operation(&self) -> Result<()> {
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

    pub(crate) fn begin_or_adopt_operation(&self) -> Result<()> {
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

    pub(crate) fn complete_operation(&self) {
        let _ = self.phase.compare_exchange(
            SessionRecoveryPhase::InFlight as u8,
            SessionRecoveryPhase::Ready as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    pub(crate) fn mark_break_required(&self) {
        let _ = self.phase.compare_exchange(
            SessionRecoveryPhase::InFlight as u8,
            SessionRecoveryPhase::BreakSent as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    pub(crate) fn mark_break_sent(&self) -> Result<()> {
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

    pub(crate) fn begin_pending_drain(&self) -> Result<bool> {
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

    pub(crate) fn begin_drain_after_break(&self) -> Result<()> {
        self.mark_break_sent()?;
        match self.begin_pending_drain()? {
            true => Ok(()),
            false => Err(Error::ConnectionClosed(
                "session recovery did not enter draining state".into(),
            )),
        }
    }

    pub(crate) fn finish_drain_ready(&self) {
        self.phase
            .store(SessionRecoveryPhase::Ready as u8, Ordering::SeqCst);
    }

    pub(crate) fn mark_dead(&self) {
        self.phase
            .store(SessionRecoveryPhase::Dead as u8, Ordering::SeqCst);
    }
}

/// Oracle error codes that python-oracledb maps to DPY-4011 (connection
/// closed); seeing one of these marks the connection as dead so pools can
/// discard it on release (reference `errors.ERR_ORACLE_ERROR_XREF`).
pub(crate) const SESSION_DEAD_ORA_CODES: &[u32] = &[
    22, 28, 31, 45, 378, 600, 602, 603, 609, 1012, 1041, 1043, 1089, 1092, 2396, 3113, 3114, 3122,
    3135, 12153, 12537, 12547, 12570, 12583, 27146, 28511, 56600,
];

/// TTC field-version threshold where the database version number encoding
/// changed (reference thin/constants.pxi `TNS_CCAP_FIELD_VERSION_18_1_EXT_1`).
pub(crate) const TNS_CCAP_FIELD_VERSION_18_1_EXT_1: u8 = 11;

/// Decode the packed `AUTH_VERSION_NO` value into the database version
/// 5-tuple. The bit layout changed with Oracle Database 18
/// (reference messages/auth.pyx `_get_version_tuple`).
pub(crate) fn decode_server_version_number(full: u32, new_format: bool) -> (u8, u8, u8, u8, u8) {
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
pub(crate) const TRANSIENT_ORA_CODES: &[u32] =
    &[54, 60, 104, 257, 12516, 12520, 12526, 12528, 30006, 51535];

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
pub(crate) const CONNECTION_LOST_ORA_CODES: &[u32] = &[
    28, 1012, 1041, 1089, 2396, 3113, 3114, 3135, 12537, 12547, 12570, 28511,
];

/// Extract the leading `ORA-NNNNN` numeric code from an Oracle error message,
/// if the message carries one. Used as the fallback when a structured
/// [`ServerErrorDetails`] code is not available (string-only error variants).
pub(crate) fn parse_ora_code_from_message(message: &str) -> Option<u32> {
    let start = message.find("ORA-")?;
    let digits: String = message[start + 4..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect();
    digits.parse::<u32>().ok()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PostSyncProtocolDisposition {
    Ready,
    Dead,
}

/// Classify protocol errors after bytes for an operation have crossed the wire.
///
/// Pre-sync/client-side validation errors return directly and keep any existing
/// connection usable. Once a server response is being decoded, a resource-limit
/// violation means the client intentionally stopped consuming an in-flight
/// response, so the wire can no longer be assumed aligned.
pub(crate) fn post_sync_protocol_error_disposition(
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

pub(crate) fn protocol_error_is_session_dead(err: &oracledb_protocol::ProtocolError) -> bool {
    post_sync_protocol_error_disposition(err) == PostSyncProtocolDisposition::Dead
}

pub(crate) fn protocol_error_kind(err: &oracledb_protocol::ProtocolError) -> ErrorKind {
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
pub(crate) fn protocol_error_ora_code(err: &oracledb_protocol::ProtocolError) -> Option<u32> {
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
pub(crate) fn protocol_error_offset(err: &oracledb_protocol::ProtocolError) -> Option<i32> {
    match err {
        oracledb_protocol::ProtocolError::ServerErrorInfo(details) if details.pos != 0 => {
            Some(details.pos)
        }
        _ => None,
    }
}
