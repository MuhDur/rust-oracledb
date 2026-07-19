//! Retry executor over the ORA error taxonomy, gated by operation idempotency.
//!
//! The driver already classifies every failure ([`Error::retry_hint`]) into one
//! of three postures: never retry, retry on the same connection, or reconnect
//! then retry. What it did *not* have is a component that actually drives a
//! retry loop — and, crucially, one that refuses to replay an operation that
//! might double-apply.
//!
//! This module is that component. It is a thin layer over the existing taxonomy
//! (it consults [`Error::retry_hint`], it does not re-derive its own ORA-code
//! table), plus one hard rule:
//!
//! > **A non-idempotent operation is NEVER retried, whatever the error says.**
//!
//! A transient failure (a lock contention, a lost connection) can surface
//! *after* the server has already applied the statement — the acknowledgement is
//! simply what got lost. Re-running a plain `INSERT`/`UPDATE`/PL/SQL block in
//! that situation would double-apply. So the gate is fail-safe: only an
//! operation the caller has *proven* idempotent (a `SELECT`, or a DML the caller
//! vouches for) is ever replayed. When in doubt, we surface, we do not retry.

use crate::recovery::observe_cancellation_between_round_trips;
use crate::{Error, Result, RetryHint};
use asupersync::{time, Cx};
use std::future::{poll_fn, Future};
use std::pin::pin;
use std::task::Poll;
use std::time::Duration;

/// Whether an operation may be safely re-executed after a transient failure.
///
/// This is a *caller* assertion, not something the driver can infer for
/// arbitrary SQL: only the caller knows whether replaying a statement is safe
/// (a keyed `MERGE` may be idempotent; a bare `INSERT` is not). Use
/// [`Idempotency::classify_sql`] for a conservative default when you only have
/// the SQL text.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Idempotency {
    /// Safe to re-run: a read-only `SELECT`, or a statement the caller has
    /// proven has no double-apply hazard.
    Idempotent,
    /// Not safe to re-run automatically. The executor surfaces the failure
    /// instead of replaying it, even when the error is otherwise retriable.
    NonIdempotent,
}

impl Idempotency {
    /// A conservative, fail-safe classification from SQL text alone.
    ///
    /// Only a leading `SELECT` keyword is treated as [`Idempotent`]. Everything
    /// else — DML, DDL, PL/SQL blocks (`BEGIN`/`DECLARE`), `CALL`, `MERGE`,
    /// `WITH ... SELECT` (which can carry a `WITH FUNCTION` side effect), and any
    /// text we do not recognize — is [`NonIdempotent`]. A blind retry of those
    /// could double-apply, so the safe default is "do not retry".
    ///
    /// A caller who *knows* a statement is safe to replay should pass
    /// [`Idempotency::Idempotent`] explicitly rather than rely on this.
    ///
    /// [`Idempotent`]: Idempotency::Idempotent
    /// [`NonIdempotent`]: Idempotency::NonIdempotent
    pub fn classify_sql(sql: &str) -> Idempotency {
        // Skip leading whitespace and a single leading line/block comment run so
        // a hinted `SELECT /*+ ... */` or a commented statement still classifies.
        let head = sql.trim_start();
        let keyword = head
            .split(|ch: char| !ch.is_ascii_alphabetic())
            .next()
            .unwrap_or("");
        if keyword.eq_ignore_ascii_case("select") {
            Idempotency::Idempotent
        } else {
            Idempotency::NonIdempotent
        }
    }
}

/// The action the executor takes after an attempt has failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryAction {
    /// Give up: surface the error to the caller unchanged.
    Surface,
    /// Re-run on the **same** connection after `backoff`.
    RetrySameConnection { backoff: Duration },
    /// Reconnect first, then re-run after `backoff`. The same-connection entry
    /// point ([`run_with_retry`]) treats this as [`Surface`] because it has no
    /// reconnect hook; [`run_with_retry_reconnecting`] honors it.
    ///
    /// [`Surface`]: RetryAction::Surface
    ReconnectThenRetry { backoff: Duration },
}

/// Retry budget and backoff schedule.
///
/// `max_retries` counts retries *after* the first attempt: `max_retries = 3`
/// permits up to four executions total. Backoff is exponential from
/// `base_backoff`, doubling each attempt, capped at `max_backoff`. A zero
/// `base_backoff` disables sleeping entirely (used in deterministic tests).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    /// Maximum number of retries after the initial attempt.
    pub max_retries: u32,
    /// Backoff before the first retry; doubles each subsequent retry.
    pub base_backoff: Duration,
    /// Upper bound on any single backoff.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    /// A conservative default: three retries, 50 ms base backoff, 2 s cap.
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(2),
        }
    }
}

impl RetryPolicy {
    /// A policy that never retries (single attempt). Useful as an explicit
    /// opt-out and for asserting the fail-safe path.
    pub const fn none() -> Self {
        Self {
            max_retries: 0,
            base_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        }
    }

    /// The exponential backoff for the retry that follows the `attempts_made`-th
    /// failure (`attempts_made >= 1`): `base * 2^(attempts_made - 1)`, capped.
    fn backoff_for(&self, attempts_made: u32) -> Duration {
        if self.base_backoff.is_zero() {
            return Duration::ZERO;
        }
        let shift = attempts_made.saturating_sub(1).min(31);
        let scaled = self
            .base_backoff
            .checked_mul(1u32 << shift)
            .unwrap_or(self.max_backoff);
        scaled.min(self.max_backoff)
    }

    /// Decide what to do after an attempt failed with `err`, given the operation
    /// idempotency and the number of attempts already made (`>= 1`).
    ///
    /// This is the whole policy, pure and total: no I/O, no clock. The gate is
    /// applied *before* the error's own hint is even consulted — a
    /// non-idempotent operation surfaces regardless of how retriable the failure
    /// looks.
    pub fn decide(&self, err: &Error, idempotency: Idempotency, attempts_made: u32) -> RetryAction {
        // Hard gate, first: never replay something that might double-apply.
        if idempotency == Idempotency::NonIdempotent {
            return RetryAction::Surface;
        }
        // Budget exhausted: this failure was the last permitted attempt.
        if attempts_made > self.max_retries {
            return RetryAction::Surface;
        }
        let backoff = self.backoff_for(attempts_made);
        match err.retry_hint() {
            RetryHint::Never => RetryAction::Surface,
            RetryHint::RetrySameConnectionIfIdempotent => {
                RetryAction::RetrySameConnection { backoff }
            }
            RetryHint::ReconnectThenRetryIfIdempotent => {
                RetryAction::ReconnectThenRetry { backoff }
            }
        }
    }
}

/// Sleep for `backoff` while continuing to observe caller cancellation.
///
/// A retry is a new operation boundary: cancellation must win before another
/// attempt or reconnect starts. Polling the checkpoint beside the timer also
/// lets a runtime cancellation wake the task instead of waiting out the whole
/// backoff first.
async fn backoff_sleep(cx: &Cx, backoff: Duration) -> Result<()> {
    observe_cancellation_between_round_trips(cx)?;
    if backoff.is_zero() {
        return Ok(());
    }

    let mut sleep = pin!(time::sleep(time::wall_now(), backoff));
    poll_fn(|task_cx| {
        if let Err(err) = observe_cancellation_between_round_trips(cx) {
            return Poll::Ready(Err(err));
        }
        sleep.as_mut().poll(task_cx).map(Ok)
    })
    .await?;
    observe_cancellation_between_round_trips(cx)
}

/// Run an idempotency-gated retry loop on a **single** connection.
///
/// `op` is an operation *factory*: it is called once per attempt and must
/// produce a fresh future each time (so a retry re-issues the call). On a
/// transient failure of an [`Idempotency::Idempotent`] operation, the loop
/// backs off and re-runs `op`. A [`Idempotency::NonIdempotent`] operation is
/// never re-run — the first failure is surfaced.
///
/// A failure whose posture is [`RetryHint::ReconnectThenRetryIfIdempotent`]
/// (the connection was lost) is surfaced here, because this entry point cannot
/// reconnect. Use [`run_with_retry_reconnecting`] when a reconnect hook is
/// available.
pub async fn run_with_retry<T, MkFut, Fut>(
    cx: &Cx,
    policy: &RetryPolicy,
    idempotency: Idempotency,
    op: MkFut,
) -> Result<T>
where
    MkFut: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    run_inner(cx, policy, idempotency, op, NoReconnect).await
}

/// Run an idempotency-gated retry loop that can reconnect between attempts.
///
/// Identical to [`run_with_retry`], except a connection-lost failure of an
/// idempotent operation triggers `reconnect` before the next attempt. If
/// `reconnect` itself fails, that error is surfaced (the session is unusable and
/// the original error is moot). `reconnect` is only ever invoked for an
/// idempotent operation whose failure was classified connection-lost.
pub async fn run_with_retry_reconnecting<T, MkFut, Fut, Recon, ReconFut>(
    cx: &Cx,
    policy: &RetryPolicy,
    idempotency: Idempotency,
    op: MkFut,
    reconnect: Recon,
) -> Result<T>
where
    MkFut: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
    Recon: FnMut() -> ReconFut,
    ReconFut: Future<Output = Result<()>>,
{
    run_inner(cx, policy, idempotency, op, Reconnect(reconnect)).await
}

/// A reconnect strategy: either a real hook, or a marker that refuses to
/// reconnect (so a connection-lost verdict surfaces on the same-connection path).
trait ReconnectHook {
    /// `Ok(true)` = reconnected, retry may proceed; `Ok(false)` = no reconnect
    /// capability, surface the original error; `Err` = reconnect failed.
    fn reconnect(&mut self) -> impl Future<Output = Result<bool>>;
}

struct NoReconnect;
impl ReconnectHook for NoReconnect {
    async fn reconnect(&mut self) -> Result<bool> {
        Ok(false)
    }
}

struct Reconnect<R>(R);
impl<R, Fut> ReconnectHook for Reconnect<R>
where
    R: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    async fn reconnect(&mut self) -> Result<bool> {
        (self.0)().await.map(|()| true)
    }
}

async fn run_inner<T, MkFut, Fut, H>(
    cx: &Cx,
    policy: &RetryPolicy,
    idempotency: Idempotency,
    mut op: MkFut,
    mut hook: H,
) -> Result<T>
where
    MkFut: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
    H: ReconnectHook,
{
    let mut attempts_made: u32 = 0;
    loop {
        // Do not start even the first attempt when the caller is already
        // cancelled. Every later retry returns through this same checkpoint.
        observe_cancellation_between_round_trips(cx)?;
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                attempts_made += 1;
                match policy.decide(&err, idempotency, attempts_made) {
                    RetryAction::Surface => return Err(err),
                    RetryAction::RetrySameConnection { backoff } => {
                        backoff_sleep(cx, backoff).await?;
                    }
                    RetryAction::ReconnectThenRetry { backoff } => {
                        backoff_sleep(cx, backoff).await?;
                        match hook.reconnect().await {
                            // Reconnected: retry on the fresh connection.
                            Ok(true) => {}
                            // No reconnect capability on this path: surface the
                            // connection-lost error as-is.
                            Ok(false) => return Err(err),
                            // Reconnect failed: the session is gone; surface that.
                            Err(reconnect_err) => return Err(reconnect_err),
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asupersync::runtime::{reactor, RuntimeBuilder};
    use asupersync::types::CancelKind;
    use std::cell::Cell;

    // --- error synthesis (mirrors the crate's other test helpers) ------------

    fn structured_error(code: u32) -> Error {
        Error::Protocol(oracledb_protocol::ProtocolError::ServerErrorInfo(Box::new(
            oracledb_protocol::ServerErrorDetails {
                message: format!("ORA-{code:05}: synthetic"),
                code,
                ..Default::default()
            },
        )))
    }

    // Transient (retry same connection): ORA-00054 resource busy.
    fn transient() -> Error {
        structured_error(54)
    }
    // Connection-lost (reconnect then retry): ORA-03113 end-of-file on channel.
    fn connection_lost() -> Error {
        structured_error(3113)
    }
    // Permanent (never retry): ORA-00942 table or view does not exist.
    fn permanent() -> Error {
        structured_error(942)
    }

    fn instant_policy(max_retries: u32) -> RetryPolicy {
        RetryPolicy {
            max_retries,
            base_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        }
    }

    // --- pure decision logic (no runtime needed) -----------------------------

    #[test]
    fn transient_idempotent_retries_within_budget() {
        let p = instant_policy(3);
        assert_eq!(
            p.decide(&transient(), Idempotency::Idempotent, 1),
            RetryAction::RetrySameConnection {
                backoff: Duration::ZERO
            }
        );
    }

    #[test]
    fn transient_non_idempotent_always_surfaces() {
        let p = instant_policy(3);
        // Even though the error itself is transient, a non-idempotent op is
        // never replayed — the double-apply hazard dominates.
        assert_eq!(
            p.decide(&transient(), Idempotency::NonIdempotent, 1),
            RetryAction::Surface
        );
    }

    #[test]
    fn connection_lost_idempotent_asks_for_reconnect() {
        let p = instant_policy(3);
        assert_eq!(
            p.decide(&connection_lost(), Idempotency::Idempotent, 1),
            RetryAction::ReconnectThenRetry {
                backoff: Duration::ZERO
            }
        );
    }

    #[test]
    fn connection_lost_non_idempotent_surfaces() {
        let p = instant_policy(3);
        assert_eq!(
            p.decide(&connection_lost(), Idempotency::NonIdempotent, 1),
            RetryAction::Surface
        );
    }

    #[test]
    fn permanent_never_retries_even_when_idempotent() {
        let p = instant_policy(3);
        assert_eq!(
            p.decide(&permanent(), Idempotency::Idempotent, 1),
            RetryAction::Surface
        );
    }

    #[test]
    fn budget_exhaustion_surfaces() {
        let p = instant_policy(2);
        // attempts_made 1 and 2 may retry; the 3rd failure has spent the budget.
        assert!(matches!(
            p.decide(&transient(), Idempotency::Idempotent, 2),
            RetryAction::RetrySameConnection { .. }
        ));
        assert_eq!(
            p.decide(&transient(), Idempotency::Idempotent, 3),
            RetryAction::Surface
        );
    }

    #[test]
    fn zero_retry_policy_surfaces_first_failure() {
        let p = instant_policy(0);
        assert_eq!(
            p.decide(&transient(), Idempotency::Idempotent, 1),
            RetryAction::Surface
        );
    }

    #[test]
    fn backoff_is_exponential_and_capped() {
        let p = RetryPolicy {
            max_retries: 10,
            base_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(50),
        };
        assert_eq!(p.backoff_for(1), Duration::from_millis(10));
        assert_eq!(p.backoff_for(2), Duration::from_millis(20));
        assert_eq!(p.backoff_for(3), Duration::from_millis(40));
        // Doubling would give 80ms; capped at 50ms.
        assert_eq!(p.backoff_for(4), Duration::from_millis(50));
        // Large shift must not overflow.
        assert_eq!(p.backoff_for(1000), Duration::from_millis(50));
    }

    #[test]
    fn classify_sql_only_select_is_idempotent() {
        assert_eq!(
            Idempotency::classify_sql("SELECT * FROM dual"),
            Idempotency::Idempotent
        );
        assert_eq!(
            Idempotency::classify_sql("  select 1 from dual"),
            Idempotency::Idempotent
        );
        assert_eq!(
            Idempotency::classify_sql("INSERT INTO t VALUES (1)"),
            Idempotency::NonIdempotent
        );
        assert_eq!(
            Idempotency::classify_sql("UPDATE t SET x = 1"),
            Idempotency::NonIdempotent
        );
        assert_eq!(
            Idempotency::classify_sql("BEGIN proc(); END;"),
            Idempotency::NonIdempotent
        );
        assert_eq!(
            Idempotency::classify_sql("MERGE INTO t USING s ON (t.k=s.k)"),
            Idempotency::NonIdempotent
        );
        // WITH is conservatively non-idempotent (WITH FUNCTION can side-effect).
        assert_eq!(
            Idempotency::classify_sql("WITH q AS (SELECT 1) SELECT * FROM q"),
            Idempotency::NonIdempotent
        );
        assert_eq!(Idempotency::classify_sql(""), Idempotency::NonIdempotent);
    }

    // --- async executor over a scripted transport ----------------------------
    //
    // The "scripted transport" is a queue of pre-programmed outcomes with an
    // invocation counter: it lets us prove, offline and with zero container,
    // exactly how many times the operation was executed.

    struct ScriptedOp {
        script: Vec<std::result::Result<u64, Error>>,
        next: Cell<usize>,
        calls: Cell<usize>,
    }

    impl ScriptedOp {
        fn new(script: Vec<std::result::Result<u64, Error>>) -> Self {
            Self {
                script,
                next: Cell::new(0),
                calls: Cell::new(0),
            }
        }

        fn run_once(&self) -> Result<u64> {
            let i = self.next.get();
            self.calls.set(self.calls.get() + 1);
            self.next.set(i + 1);
            match self.script.get(i) {
                Some(Ok(v)) => Ok(*v),
                Some(Err(e)) => Err(clone_error(e)),
                None => panic!("scripted op called more times than scripted"),
            }
        }

        fn calls(&self) -> usize {
            self.calls.get()
        }
    }

    // Errors are not Clone; rebuild the synthetic ones the script uses.
    fn clone_error(e: &Error) -> Error {
        match e.ora_code() {
            Some(code) => structured_error(code as u32),
            None => Error::Runtime("scripted".into()),
        }
    }

    fn block_on<F: Future<Output = ()>>(fut: F) {
        let reactor = reactor::create_reactor().expect("reactor");
        let runtime = RuntimeBuilder::current_thread()
            .with_reactor(reactor)
            .build()
            .expect("runtime");
        runtime.block_on(fut);
    }

    #[test]
    fn idempotent_transient_then_success_retries() {
        block_on(async {
            let cx = Cx::current().expect("cx");
            let op = ScriptedOp::new(vec![Err(transient()), Ok(42)]);
            let out = run_with_retry(&cx, &instant_policy(3), Idempotency::Idempotent, || async {
                op.run_once()
            })
            .await;
            assert_eq!(out.unwrap(), 42);
            assert_eq!(op.calls(), 2, "one retry after the transient failure");
        });
    }

    #[test]
    fn pre_cancelled_context_does_not_start_operation() {
        block_on(async {
            let cx = Cx::current().expect("cx");
            cx.cancel_fast(CancelKind::User);
            let calls = Cell::new(0usize);

            let out = run_with_retry(&cx, &instant_policy(3), Idempotency::Idempotent, || async {
                calls.set(calls.get() + 1);
                Ok::<_, Error>(42)
            })
            .await;

            assert!(matches!(out, Err(Error::Cancelled)), "got {out:?}");
            assert_eq!(calls.get(), 0, "cancelled work must not start");
        });
    }

    #[test]
    fn cancellation_after_failure_stops_before_retry() {
        block_on(async {
            let cx = Cx::current().expect("cx");
            let calls = Cell::new(0usize);

            let out = run_with_retry(&cx, &instant_policy(3), Idempotency::Idempotent, || async {
                calls.set(calls.get() + 1);
                if calls.get() == 1 {
                    cx.cancel_fast(CancelKind::User);
                    Err(transient())
                } else {
                    Ok(42)
                }
            })
            .await;

            assert!(matches!(out, Err(Error::Cancelled)), "got {out:?}");
            assert_eq!(calls.get(), 1, "cancellation must suppress the retry");
        });
    }

    #[test]
    fn shutdown_cancellation_stops_before_reconnect() {
        block_on(async {
            let cx = Cx::current().expect("cx");
            let calls = Cell::new(0usize);
            let reconnects = Cell::new(0usize);

            let out = run_with_retry_reconnecting(
                &cx,
                &instant_policy(3),
                Idempotency::Idempotent,
                || async {
                    calls.set(calls.get() + 1);
                    cx.cancel_fast(CancelKind::Shutdown);
                    Err::<u64, _>(connection_lost())
                },
                || async {
                    reconnects.set(reconnects.get() + 1);
                    Ok(())
                },
            )
            .await;

            assert!(
                matches!(out, Err(Error::ConnectionClosed(_))),
                "got {out:?}"
            );
            assert_eq!(calls.get(), 1, "cancelled operation must not retry");
            assert_eq!(reconnects.get(), 0, "shutdown must not reconnect");
        });
    }

    #[test]
    fn non_idempotent_transient_is_surfaced_not_rerun() {
        block_on(async {
            let cx = Cx::current().expect("cx");
            // If a retry were (wrongly) attempted, run_once would return Ok(99);
            // we prove it never gets there.
            let op = ScriptedOp::new(vec![Err(transient()), Ok(99)]);
            let out = run_with_retry(
                &cx,
                &instant_policy(3),
                Idempotency::NonIdempotent,
                || async { op.run_once() },
            )
            .await;
            assert!(out.is_err(), "non-idempotent op surfaces the failure");
            assert_eq!(out.unwrap_err().ora_code(), Some(54));
            assert_eq!(op.calls(), 1, "non-idempotent op must NOT be re-run");
        });
    }

    #[test]
    fn idempotent_exhausts_budget_then_surfaces() {
        block_on(async {
            let cx = Cx::current().expect("cx");
            let op = ScriptedOp::new(vec![
                Err(transient()),
                Err(transient()),
                Err(transient()),
                Err(transient()),
            ]);
            let out = run_with_retry(&cx, &instant_policy(2), Idempotency::Idempotent, || async {
                op.run_once()
            })
            .await;
            assert!(out.is_err());
            // 1 initial + 2 retries = 3 executions, then the budget is spent.
            assert_eq!(op.calls(), 3);
        });
    }

    #[test]
    fn connection_lost_without_hook_surfaces_on_same_conn_path() {
        block_on(async {
            let cx = Cx::current().expect("cx");
            let op = ScriptedOp::new(vec![Err(connection_lost()), Ok(7)]);
            let out = run_with_retry(&cx, &instant_policy(3), Idempotency::Idempotent, || async {
                op.run_once()
            })
            .await;
            assert!(out.is_err(), "same-conn path cannot reconnect");
            assert_eq!(op.calls(), 1);
        });
    }

    #[test]
    fn connection_lost_reconnects_then_retries() {
        block_on(async {
            let cx = Cx::current().expect("cx");
            let op = ScriptedOp::new(vec![Err(connection_lost()), Ok(7)]);
            let reconnects = Cell::new(0usize);
            let out = run_with_retry_reconnecting(
                &cx,
                &instant_policy(3),
                Idempotency::Idempotent,
                || async { op.run_once() },
                || async {
                    reconnects.set(reconnects.get() + 1);
                    Ok(())
                },
            )
            .await;
            assert_eq!(out.unwrap(), 7);
            assert_eq!(op.calls(), 2);
            assert_eq!(reconnects.get(), 1, "reconnected once before the retry");
        });
    }

    #[test]
    fn reconnect_failure_is_surfaced() {
        block_on(async {
            let cx = Cx::current().expect("cx");
            let op = ScriptedOp::new(vec![Err(connection_lost()), Ok(7)]);
            let out = run_with_retry_reconnecting(
                &cx,
                &instant_policy(3),
                Idempotency::Idempotent,
                || async { op.run_once() },
                || async { Err(Error::Runtime("reconnect refused".into())) },
            )
            .await;
            let err = out.unwrap_err();
            assert!(matches!(err, Error::Runtime(msg) if msg.contains("reconnect refused")));
            // The op ran once; the retry was blocked by the failed reconnect.
            assert_eq!(op.calls(), 1);
        });
    }
}
