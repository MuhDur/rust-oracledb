//! Connection pool engine mirroring python-oracledb's thin pool algebra
//! (`impl/thin/pool.pyx`). The engine owns the pool state machine (free
//! lists, busy list, growth planning, getmode semantics, ping policy, idle
//! timeout, max lifetime) and a background worker thread that creates, pings
//! and closes connections through a [`PoolBackend`].
//!
//! The engine is deliberately free of any Python types; the pyshim provides
//! a backend whose `Conn` payload carries shared handles to the underlying
//! transport. [`Pool::acquire`] waits on an async notification and
//! checkpoints the caller's [`asupersync::Cx`] between waits; the blocking
//! facade is a thin `block_on` wrapper over that async path.

use asupersync::runtime::{JoinHandle as TaskJoinHandle, Runtime};
use asupersync::sync::Notify;
use asupersync::Cx;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

mod acquire;
mod engine;

use acquire::{acquire_wait_future, enqueue_request, AsyncAcquireRequest};
use engine::{drop_conn, reaper_main, return_connection_helper};

#[cfg(test)]
use acquire::poll_request_completion;
#[cfg(test)]
use std::future::poll_fn;
#[cfg(test)]
use std::pin::Pin;
#[cfg(test)]
use std::task::Poll;

pub const POOL_GETMODE_WAIT: u32 = 0;
pub const POOL_GETMODE_NOWAIT: u32 = 1;
pub const POOL_GETMODE_FORCEGET: u32 = 2;
pub const POOL_GETMODE_TIMEDWAIT: u32 = 3;

pub const PURITY_DEFAULT: u32 = 0;
pub const PURITY_NEW: u32 = 1;
pub const PURITY_SELF: u32 = 2;

/// Error surface of the pool engine. The embedder maps these onto the
/// corresponding python-oracledb driver errors.
#[derive(Debug)]
#[non_exhaustive]
pub enum PoolError {
    /// Pool is closed (DPY-1002 / ERR_POOL_NOT_OPEN).
    Closed,
    /// No connection available within constraints (DPY-4005).
    NoConnectionAvailable,
    /// Pool has busy connections and close was not forced (DPY-1005).
    HasBusyConnections,
    /// A connection was returned/released to the pool but is not currently
    /// checked out (a double-release, or a connection that was already dropped).
    /// The reference raises DPY-1001 / `ERR_NOT_CONNECTED` here via its
    /// verify-connected guard; we surface a typed error instead of the former
    /// silent `Ok(())` no-op so caller programming errors are not hidden.
    ConnectionNotAcquired,
    /// A backend operation (typically connection creation) failed. The
    /// message is the backend's error display, re-raised on the acquiring
    /// thread just like the reference re-raises background exceptions.
    Backend(String),
    /// The caller's async context was cancelled while waiting for a pool slot.
    Cancelled(String),
    /// Internal invariant violation (poisoned lock, unknown id, ...).
    Internal(String),
}

impl std::fmt::Display for PoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PoolError::Closed => write!(f, "connection pool is not open"),
            PoolError::NoConnectionAvailable => {
                write!(f, "no connection available in pool")
            }
            PoolError::HasBusyConnections => {
                write!(f, "connection pool has busy connections")
            }
            PoolError::ConnectionNotAcquired => {
                write!(f, "connection is not currently acquired from this pool")
            }
            PoolError::Backend(message) => write!(f, "{message}"),
            PoolError::Cancelled(message) => write!(f, "pool acquire cancelled: {message}"),
            PoolError::Internal(message) => write!(f, "pool internal error: {message}"),
        }
    }
}

/// Static pool configuration captured at pool creation. Mutable attributes
/// (getmode, timeouts, ping interval) have engine setters.
#[derive(Clone, Debug)]
pub struct PoolConfig {
    min: u32,
    max: u32,
    increment: u32,
    getmode: u32,
    wait_timeout_ms: u32,
    timeout_secs: u32,
    max_lifetime_session_secs: u32,
    ping_interval_secs: i64,
    ping_timeout_ms: u32,
    creation_cclass: Option<String>,
}

impl PoolConfig {
    pub fn new(min: u32, max: u32, increment: u32) -> Self {
        Self {
            min,
            max,
            increment,
            getmode: POOL_GETMODE_WAIT,
            wait_timeout_ms: 0,
            timeout_secs: 0,
            max_lifetime_session_secs: 0,
            ping_interval_secs: 60,
            ping_timeout_ms: 5_000,
            creation_cclass: None,
        }
    }

    pub fn min(&self) -> u32 {
        self.min
    }

    pub fn max(&self) -> u32 {
        self.max
    }

    pub fn increment(&self) -> u32 {
        self.increment
    }

    pub fn getmode(&self) -> u32 {
        self.getmode
    }

    #[must_use]
    pub fn with_getmode(mut self, getmode: u32) -> Self {
        self.getmode = getmode;
        self
    }

    pub fn wait_timeout_ms(&self) -> u32 {
        self.wait_timeout_ms
    }

    #[must_use]
    pub fn with_wait_timeout_ms(mut self, wait_timeout_ms: u32) -> Self {
        self.wait_timeout_ms = wait_timeout_ms;
        self
    }

    pub fn timeout_secs(&self) -> u32 {
        self.timeout_secs
    }

    #[must_use]
    pub fn with_timeout_secs(mut self, timeout_secs: u32) -> Self {
        self.timeout_secs = timeout_secs;
        self
    }

    pub fn max_lifetime_session_secs(&self) -> u32 {
        self.max_lifetime_session_secs
    }

    #[must_use]
    pub fn with_max_lifetime_session_secs(mut self, max_lifetime_session_secs: u32) -> Self {
        self.max_lifetime_session_secs = max_lifetime_session_secs;
        self
    }

    pub fn ping_interval_secs(&self) -> i64 {
        self.ping_interval_secs
    }

    #[must_use]
    pub fn with_ping_interval_secs(mut self, ping_interval_secs: i64) -> Self {
        self.ping_interval_secs = ping_interval_secs;
        self
    }

    pub fn ping_timeout_ms(&self) -> u32 {
        self.ping_timeout_ms
    }

    #[must_use]
    pub fn with_ping_timeout_ms(mut self, ping_timeout_ms: u32) -> Self {
        self.ping_timeout_ms = ping_timeout_ms;
        self
    }

    pub fn creation_cclass(&self) -> Option<&str> {
        self.creation_cclass.as_deref()
    }

    #[must_use]
    pub fn with_creation_cclass(mut self, creation_cclass: impl Into<String>) -> Self {
        self.creation_cclass = Some(creation_cclass.into());
        self
    }
}

/// Per-acquire options derived from the acquire-time connect params.
#[derive(Clone, Debug, Default)]
pub struct AcquireOptions {
    /// PURITY_NEW was requested: never reuse a previously used connection.
    wants_new: bool,
    /// Connection class requested at acquire time.
    cclass: Option<String>,
}

impl AcquireOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn wants_new(&self) -> bool {
        self.wants_new
    }

    #[must_use]
    pub fn with_wants_new(mut self, wants_new: bool) -> Self {
        self.wants_new = wants_new;
        self
    }

    pub fn cclass(&self) -> Option<&str> {
        self.cclass.as_deref()
    }

    #[must_use]
    pub fn with_cclass(mut self, cclass: impl Into<String>) -> Self {
        self.cclass = Some(cclass.into());
        self
    }

    #[must_use]
    pub fn with_optional_cclass(mut self, cclass: Option<String>) -> Self {
        self.cclass = cclass;
        self
    }
}

/// Snapshot of derived pool lifecycle counts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PoolStats {
    open: u32,
    busy: u32,
    idle: u32,
    opening: u32,
    validating: u32,
    retiring: u32,
    waiters: u32,
}

impl PoolStats {
    pub fn open_count(&self) -> u32 {
        self.open
    }

    pub fn busy_count(&self) -> u32 {
        self.busy
    }

    pub fn idle_count(&self) -> u32 {
        self.idle
    }

    pub fn opening_count(&self) -> u32 {
        self.opening
    }

    pub fn validating_count(&self) -> u32 {
        self.validating
    }

    pub fn retiring_count(&self) -> u32 {
        self.retiring
    }

    pub fn waiter_count(&self) -> u32 {
        self.waiters
    }
}

/// Backend operations performed by the pool engine. All methods are invoked
/// without any engine lock held (except [`PoolBackend::connection_is_open`],
/// which must therefore be non-blocking and lock-free with respect to the
/// embedder's own slow paths).
pub trait PoolBackend: Send + Sync + 'static {
    type Conn: Send + 'static;

    /// Create (and fully connect) a new pooled connection. `id` is the
    /// engine-assigned identity of the connection; `cclass` is the request
    /// cclass when the connection is created for a specific request, or the
    /// pool creation cclass otherwise.
    fn create_connection(&self, id: u64, cclass: Option<&str>) -> Result<Self::Conn, String>;

    /// Ping the connection, honouring `ping_timeout_ms`. Returns true when
    /// the connection is healthy.
    fn ping_connection(&self, conn: &Self::Conn, ping_timeout_ms: u32) -> bool;

    /// Close the connection's transport and release any embedder-side
    /// bookkeeping for `id`. Must be idempotent for already-dead transports.
    fn close_connection(&self, id: u64, conn: Self::Conn);

    /// Cheap, non-blocking local liveness check (no round trip).
    fn connection_is_open(&self, conn: &Self::Conn) -> bool;
}

// W1-T10 lands this pure model before W1-T7 wires it into the async pool.
#[allow(dead_code)]
pub(crate) mod lifecycle {
    use std::collections::VecDeque;

    pub(crate) type SlotId = u64;
    pub(crate) type WaiterId = u64;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) enum PoolCloseReason {
        Reap,
        Graceful,
        Force,
        Unhealthy,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) enum PoolEffect {
        Open {
            slot: SlotId,
            waiter: Option<WaiterId>,
        },
        Ping {
            slot: SlotId,
            waiter: WaiterId,
        },
        Close {
            slot: SlotId,
            reason: PoolCloseReason,
        },
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) enum PoolSlotState {
        Opening { waiter: Option<WaiterId> },
        Idle,
        CheckedOut { waiter: WaiterId },
        Validating { waiter: Option<WaiterId> },
        Retiring,
        Closing,
        Closed,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) enum PoolLifecycleError {
        Busy,
        Closed,
        UnknownSlot(SlotId),
        InvalidState {
            slot: SlotId,
            state: PoolSlotState,
            action: &'static str,
        },
    }

    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub(crate) struct PoolCounts {
        pub opening: usize,
        pub idle: usize,
        pub checked_out: usize,
        pub validating: usize,
        pub retiring: usize,
        pub closing: usize,
        pub closed: usize,
        pub waiters: usize,
    }

    #[derive(Clone, Debug)]
    struct PoolSlot {
        id: SlotId,
        state: PoolSlotState,
    }

    #[derive(Clone, Debug)]
    pub(crate) struct PurePoolState {
        min: usize,
        max: usize,
        slots: Vec<PoolSlot>,
        waiters: VecDeque<WaiterId>,
        effects: VecDeque<PoolEffect>,
        history: Vec<String>,
        closing: bool,
        next_slot: SlotId,
        next_waiter: WaiterId,
    }

    impl PurePoolState {
        pub(crate) fn new(min: usize, max: usize) -> Self {
            assert!(min <= max, "pool min must be <= max");
            let mut state = Self {
                min,
                max,
                slots: Vec::new(),
                waiters: VecDeque::new(),
                effects: VecDeque::new(),
                history: Vec::new(),
                closing: false,
                next_slot: 1,
                next_waiter: 1,
            };
            state.ensure_min_opening();
            state
        }

        pub(crate) fn request_acquire(&mut self) -> Result<WaiterId, PoolLifecycleError> {
            if self.closing {
                return Err(PoolLifecycleError::Closed);
            }
            let waiter = self.next_waiter;
            self.next_waiter += 1;
            self.history.push(format!("acquire waiter={waiter}"));
            self.waiters.push_back(waiter);
            self.drive_waiters();
            Ok(waiter)
        }

        pub(crate) fn cancel_acquire(
            &mut self,
            waiter: WaiterId,
        ) -> Result<(), PoolLifecycleError> {
            self.history.push(format!("cancel waiter={waiter}"));
            self.waiters.retain(|queued| *queued != waiter);
            for ix in 0..self.slots.len() {
                match self.slots[ix].state {
                    PoolSlotState::Opening {
                        waiter: Some(current),
                    } if current == waiter => {
                        self.slots[ix].state = PoolSlotState::Opening { waiter: None };
                        return Ok(());
                    }
                    PoolSlotState::Validating {
                        waiter: Some(current),
                    } if current == waiter => {
                        self.slots[ix].state = PoolSlotState::Validating { waiter: None };
                        return Ok(());
                    }
                    PoolSlotState::CheckedOut { waiter: current } if current == waiter => {
                        self.release_slot_at(ix, PoolCloseReason::Graceful);
                        self.drive_waiters();
                        return Ok(());
                    }
                    _ => {}
                }
            }
            Ok(())
        }

        pub(crate) fn complete_open(
            &mut self,
            slot: SlotId,
            healthy: bool,
        ) -> Result<(), PoolLifecycleError> {
            let ix = self.slot_index(slot)?;
            let PoolSlotState::Opening { waiter } = self.slots[ix].state else {
                return Err(self.invalid_state(slot, "complete_open"));
            };
            if healthy {
                if self.closing {
                    self.slots[ix].state = PoolSlotState::Closing;
                    self.effects.push_back(PoolEffect::Close {
                        slot,
                        reason: PoolCloseReason::Force,
                    });
                    self.history.push(format!("open slot={slot} close"));
                } else if let Some(waiter) = waiter {
                    self.slots[ix].state = PoolSlotState::CheckedOut { waiter };
                    self.history
                        .push(format!("open slot={slot} grant waiter={waiter}"));
                } else {
                    self.slots[ix].state = PoolSlotState::Idle;
                    self.history.push(format!("open slot={slot} idle"));
                    self.drive_waiters();
                }
            } else {
                self.slots[ix].state = PoolSlotState::Closed;
                self.history.push(format!("open slot={slot} failed"));
                if !self.closing {
                    if let Some(waiter) = waiter {
                        self.waiters.push_front(waiter);
                    }
                }
                self.drive_waiters();
                self.ensure_min_opening();
            }
            Ok(())
        }

        pub(crate) fn complete_ping(
            &mut self,
            slot: SlotId,
            healthy: bool,
        ) -> Result<(), PoolLifecycleError> {
            let ix = self.slot_index(slot)?;
            let PoolSlotState::Validating { waiter } = self.slots[ix].state else {
                return Err(self.invalid_state(slot, "complete_ping"));
            };
            if healthy {
                if self.closing {
                    self.slots[ix].state = PoolSlotState::Closing;
                    self.effects.push_back(PoolEffect::Close {
                        slot,
                        reason: PoolCloseReason::Force,
                    });
                } else if let Some(waiter) = waiter {
                    self.slots[ix].state = PoolSlotState::CheckedOut { waiter };
                    self.history
                        .push(format!("ping slot={slot} grant waiter={waiter}"));
                } else {
                    self.slots[ix].state = PoolSlotState::Idle;
                    self.history.push(format!("ping slot={slot} idle"));
                    self.drive_waiters();
                }
            } else {
                self.slots[ix].state = PoolSlotState::Retiring;
                self.effects.push_back(PoolEffect::Close {
                    slot,
                    reason: PoolCloseReason::Unhealthy,
                });
                if !self.closing {
                    if let Some(waiter) = waiter {
                        self.waiters.push_front(waiter);
                    }
                }
                self.history.push(format!("ping slot={slot} unhealthy"));
            }
            Ok(())
        }

        pub(crate) fn release(&mut self, slot: SlotId) -> Result<(), PoolLifecycleError> {
            let ix = self.slot_index(slot)?;
            let PoolSlotState::CheckedOut { .. } = self.slots[ix].state else {
                return Err(self.invalid_state(slot, "release"));
            };
            self.release_slot_at(ix, PoolCloseReason::Graceful);
            self.drive_waiters();
            Ok(())
        }

        pub(crate) fn reap_idle(&mut self, slot: SlotId) -> Result<(), PoolLifecycleError> {
            let ix = self.slot_index(slot)?;
            let PoolSlotState::Idle = self.slots[ix].state else {
                return Err(self.invalid_state(slot, "reap_idle"));
            };
            self.slots[ix].state = PoolSlotState::Retiring;
            self.effects.push_back(PoolEffect::Close {
                slot,
                reason: PoolCloseReason::Reap,
            });
            self.history.push(format!("reap slot={slot}"));
            Ok(())
        }

        pub(crate) fn complete_close(&mut self, slot: SlotId) -> Result<(), PoolLifecycleError> {
            let ix = self.slot_index(slot)?;
            match self.slots[ix].state {
                PoolSlotState::Retiring | PoolSlotState::Closing => {
                    self.slots[ix].state = PoolSlotState::Closed;
                    self.history.push(format!("close slot={slot} done"));
                    self.drive_waiters();
                    self.ensure_min_opening();
                    Ok(())
                }
                _ => Err(self.invalid_state(slot, "complete_close")),
            }
        }

        pub(crate) fn begin_close(&mut self, force: bool) -> Result<(), PoolLifecycleError> {
            self.history.push(format!("begin_close force={force}"));
            if !force
                && (self.counts().checked_out > 0
                    || self.counts().validating > 0
                    || self.counts().opening > 0
                    || !self.waiters.is_empty())
            {
                return Err(PoolLifecycleError::Busy);
            }
            self.closing = true;
            if force {
                self.waiters.clear();
            }
            for slot in &mut self.slots {
                match slot.state {
                    PoolSlotState::Idle | PoolSlotState::CheckedOut { .. } => {
                        slot.state = PoolSlotState::Closing;
                        self.effects.push_back(PoolEffect::Close {
                            slot: slot.id,
                            reason: if force {
                                PoolCloseReason::Force
                            } else {
                                PoolCloseReason::Graceful
                            },
                        });
                    }
                    PoolSlotState::Opening { .. } | PoolSlotState::Validating { .. } => {
                        // The open/ping effect is already in flight. Its completion
                        // event will observe `closing` and schedule the close effect.
                    }
                    PoolSlotState::Retiring | PoolSlotState::Closing | PoolSlotState::Closed => {}
                }
            }
            Ok(())
        }

        pub(crate) fn counts(&self) -> PoolCounts {
            let mut counts = PoolCounts {
                waiters: self.waiters.len(),
                ..PoolCounts::default()
            };
            for slot in &self.slots {
                match slot.state {
                    PoolSlotState::Opening { .. } => counts.opening += 1,
                    PoolSlotState::Idle => counts.idle += 1,
                    PoolSlotState::CheckedOut { .. } => counts.checked_out += 1,
                    PoolSlotState::Validating { .. } => counts.validating += 1,
                    PoolSlotState::Retiring => counts.retiring += 1,
                    PoolSlotState::Closing => counts.closing += 1,
                    PoolSlotState::Closed => counts.closed += 1,
                }
            }
            counts
        }

        pub(crate) fn state_of(&self, slot: SlotId) -> Option<PoolSlotState> {
            self.slots
                .iter()
                .find(|candidate| candidate.id == slot)
                .map(|candidate| candidate.state)
        }

        pub(crate) fn take_effects(&mut self) -> Vec<PoolEffect> {
            self.effects.drain(..).collect()
        }

        pub(crate) fn history(&self) -> &[String] {
            &self.history
        }

        fn drive_waiters(&mut self) {
            while !self.closing {
                let Some(waiter) = self.waiters.pop_front() else {
                    return;
                };
                if let Some(ix) = self
                    .slots
                    .iter()
                    .position(|slot| matches!(slot.state, PoolSlotState::Idle))
                {
                    let slot = self.slots[ix].id;
                    self.slots[ix].state = PoolSlotState::Validating {
                        waiter: Some(waiter),
                    };
                    self.effects.push_back(PoolEffect::Ping { slot, waiter });
                    self.history
                        .push(format!("validate slot={slot} waiter={waiter}"));
                    continue;
                }
                if self.live_slot_count() < self.max {
                    self.open_slot(Some(waiter));
                    continue;
                }
                self.waiters.push_front(waiter);
                return;
            }
        }

        fn ensure_min_opening(&mut self) {
            while !self.closing && self.live_slot_count() < self.min {
                self.open_slot(None);
            }
        }

        fn open_slot(&mut self, waiter: Option<WaiterId>) {
            let slot = self.next_slot;
            self.next_slot += 1;
            self.slots.push(PoolSlot {
                id: slot,
                state: PoolSlotState::Opening { waiter },
            });
            self.effects.push_back(PoolEffect::Open { slot, waiter });
            self.history
                .push(format!("open slot={slot} waiter={waiter:?}"));
        }

        fn release_slot_at(&mut self, ix: usize, reason: PoolCloseReason) {
            let slot = self.slots[ix].id;
            if self.closing {
                self.slots[ix].state = PoolSlotState::Closing;
                self.effects.push_back(PoolEffect::Close { slot, reason });
                self.history.push(format!("release slot={slot} close"));
            } else {
                self.slots[ix].state = PoolSlotState::Idle;
                self.history.push(format!("release slot={slot} idle"));
            }
        }

        fn live_slot_count(&self) -> usize {
            self.slots
                .iter()
                .filter(|slot| !matches!(slot.state, PoolSlotState::Closed))
                .count()
        }

        fn slot_index(&self, slot: SlotId) -> Result<usize, PoolLifecycleError> {
            self.slots
                .iter()
                .position(|candidate| candidate.id == slot)
                .ok_or(PoolLifecycleError::UnknownSlot(slot))
        }

        fn invalid_state(&self, slot: SlotId, action: &'static str) -> PoolLifecycleError {
            let state = self
                .state_of(slot)
                .expect("slot existence checked before invalid_state");
            PoolLifecycleError::InvalidState {
                slot,
                state,
                action,
            }
        }
    }
}

struct PooledConn<C> {
    id: u64,
    conn: C,
    cclass: Option<String>,
    time_created: Instant,
    time_returned: Instant,
    is_pool_extra: bool,
    ever_acquired: bool,
}

struct Request<C> {
    id: u64,
    cclass: Option<String>,
    cclass_matches: bool,
    wants_new: bool,
    requires_ping: bool,
    bg_processing: bool,
    is_extra: bool,
    is_replacing: bool,
    in_progress: bool,
    completed: bool,
    waiting: bool,
    conn: Option<PooledConn<C>>,
    error: Option<String>,
}

struct PoolState<C> {
    open: bool,
    config: PoolConfig,
    force_get: bool,
    /// `None` when getmode is not TIMEDWAIT (reference stores None vs value).
    wait_timeout_ms: Option<u32>,
    free_new: Vec<PooledConn<C>>,
    free_used: Vec<PooledConn<C>>,
    busy: Vec<PooledConn<C>>,
    to_drop: VecDeque<PooledConn<C>>,
    requests: Vec<Request<C>>,
    open_effects: VecDeque<lifecycle::PoolEffect>,
    in_flight_open_effects: VecDeque<lifecycle::PoolEffect>,
    next_conn_id: u64,
    next_request_id: u64,
}

struct EngineInner<B: PoolBackend> {
    backend: B,
    state: Mutex<PoolState<B::Conn>>,
    drop_returns_tx: mpsc::Sender<u64>,
    drop_returns_rx: Mutex<mpsc::Receiver<u64>>,
    /// Woken whenever an async waiter's `fulfill` predicate may have changed.
    async_waiters: Notify,
    /// Woken whenever the region-owned reaper task has work to do (a request to
    /// process, a connection to create or close, or a shutdown request). This
    /// replaces the previous `Condvar` that parked a detached OS thread.
    ///
    /// Held behind an `Arc` so the reaper can park on it (`bg.notified().await`)
    /// without keeping `EngineInner` itself alive across the park — that lets the
    /// last external handle drop `EngineInner` (and its runtime) even while the
    /// reaper is asleep.
    bg: Arc<Notify>,
    /// Cooperative stop flag for the reaper task. Set under the state lock when
    /// the pool is closed; the reaper observes it at the top of its loop, drains
    /// the close queue, and returns. The async path joins the task afterwards.
    reaper_stop: AtomicBool,
    /// The asupersync task handle for the region-owned reaper, joined (awaited,
    /// never blocked) by [`PoolEngine::close_async`].
    reaper_handle: Mutex<Option<TaskJoinHandle<()>>>,
}

pub struct Pool<B: PoolBackend> {
    engine: PoolEngine<B>,
}

impl<B: PoolBackend> Clone for Pool<B> {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
        }
    }
}

impl<B: PoolBackend> Pool<B> {
    /// Create an async-native pool and start its background worker.
    pub fn start(backend: B, config: PoolConfig) -> Result<Self, PoolError> {
        Ok(Self {
            engine: PoolEngine::start(backend, config)?,
        })
    }

    /// Return the blocking facade over this async pool.
    pub fn blocking(&self) -> BlockingPool<B> {
        BlockingPool { pool: self.clone() }
    }

    /// Acquire a guarded pooled connection without parking the current OS thread.
    pub async fn acquire(
        &self,
        cx: &Cx,
        opts: AcquireOptions,
    ) -> Result<PooledConnection<B>, PoolError> {
        let conn_id = self.engine.acquire_async(cx, opts).await?;
        Ok(PooledConnection::new(self.clone(), conn_id))
    }

    /// Close the pool through the async facade.
    pub async fn close(&self, cx: &Cx, force: bool) -> Result<(), PoolError> {
        self.engine.close_async(cx, force).await
    }

    /// Drain queued guard-drop return events through the async facade.
    pub async fn drain(&self, cx: &Cx) -> Result<(), PoolError> {
        self.engine.drain_async(cx).await
    }

    /// Return a derived pool lifecycle snapshot through the async facade.
    pub async fn stats(&self, cx: &Cx) -> Result<PoolStats, PoolError> {
        self.engine.stats_async(cx).await
    }

    pub async fn busy_count(&self, cx: &Cx) -> Result<u32, PoolError> {
        self.engine.busy_count_async(cx).await
    }

    pub async fn open_count(&self, cx: &Cx) -> Result<u32, PoolError> {
        self.engine.open_count_async(cx).await
    }

    pub fn getmode(&self) -> Result<u32, PoolError> {
        self.engine.getmode()
    }

    pub fn set_getmode(&self, value: u32) -> Result<(), PoolError> {
        self.engine.set_getmode(value)
    }

    pub fn wait_timeout_ms(&self) -> Result<Option<u32>, PoolError> {
        self.engine.wait_timeout_ms()
    }

    pub fn set_wait_timeout_ms(&self, value: u32) -> Result<(), PoolError> {
        self.engine.set_wait_timeout_ms(value)
    }

    pub fn timeout_secs(&self) -> Result<u32, PoolError> {
        self.engine.timeout_secs()
    }

    pub fn set_timeout_secs(&self, value: u32) -> Result<(), PoolError> {
        self.engine.set_timeout_secs(value)
    }

    pub fn max_lifetime_session_secs(&self) -> Result<u32, PoolError> {
        self.engine.max_lifetime_session_secs()
    }

    pub fn set_max_lifetime_session_secs(&self, value: u32) -> Result<(), PoolError> {
        self.engine.set_max_lifetime_session_secs(value)
    }

    pub fn ping_interval_secs(&self) -> Result<i64, PoolError> {
        self.engine.ping_interval_secs()
    }

    pub fn set_ping_interval_secs(&self, value: i64) -> Result<(), PoolError> {
        self.engine.set_ping_interval_secs(value)
    }
}

pub struct BlockingPool<B: PoolBackend> {
    pool: Pool<B>,
}

impl<B: PoolBackend> Clone for BlockingPool<B> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
        }
    }
}

impl<B: PoolBackend> BlockingPool<B> {
    /// Blocking facade for [`Pool::acquire`].
    pub fn acquire(&self, opts: AcquireOptions) -> Result<BlockingPooledConnection<B>, PoolError> {
        let conn_id = self.pool.engine.acquire(opts)?;
        Ok(BlockingPooledConnection::new(self.pool.clone(), conn_id))
    }

    pub fn close(&self, force: bool) -> Result<(), PoolError> {
        self.pool.engine.close(force)
    }

    pub fn drain(&self) -> Result<(), PoolError> {
        self.pool.engine.drain()
    }

    pub fn stats(&self) -> Result<PoolStats, PoolError> {
        self.pool.engine.stats()
    }

    pub fn busy_count(&self) -> Result<u32, PoolError> {
        self.pool.engine.busy_count()
    }

    pub fn open_count(&self) -> Result<u32, PoolError> {
        self.pool.engine.open_count()
    }

    pub fn getmode(&self) -> Result<u32, PoolError> {
        self.pool.getmode()
    }

    pub fn set_getmode(&self, value: u32) -> Result<(), PoolError> {
        self.pool.set_getmode(value)
    }

    pub fn wait_timeout_ms(&self) -> Result<Option<u32>, PoolError> {
        self.pool.wait_timeout_ms()
    }

    pub fn set_wait_timeout_ms(&self, value: u32) -> Result<(), PoolError> {
        self.pool.set_wait_timeout_ms(value)
    }

    pub fn timeout_secs(&self) -> Result<u32, PoolError> {
        self.pool.timeout_secs()
    }

    pub fn set_timeout_secs(&self, value: u32) -> Result<(), PoolError> {
        self.pool.set_timeout_secs(value)
    }

    pub fn max_lifetime_session_secs(&self) -> Result<u32, PoolError> {
        self.pool.max_lifetime_session_secs()
    }

    pub fn set_max_lifetime_session_secs(&self, value: u32) -> Result<(), PoolError> {
        self.pool.set_max_lifetime_session_secs(value)
    }

    pub fn ping_interval_secs(&self) -> Result<i64, PoolError> {
        self.pool.ping_interval_secs()
    }

    pub fn set_ping_interval_secs(&self, value: i64) -> Result<(), PoolError> {
        self.pool.set_ping_interval_secs(value)
    }
}

pub struct BlockingPooledConnection<B: PoolBackend> {
    inner: PooledConnection<B>,
}

impl<B: PoolBackend> BlockingPooledConnection<B> {
    fn new(pool: Pool<B>, conn_id: u64) -> Self {
        Self {
            inner: PooledConnection::new(pool, conn_id),
        }
    }

    /// Engine-assigned identity for embedders that keep sidecar objects.
    pub fn id(&self) -> u64 {
        self.inner.id()
    }

    /// Eagerly return the connection to the pool through the blocking facade.
    pub fn release(self) -> Result<(), PoolError> {
        self.inner.release_blocking_impl()
    }

    /// Drop the physical connection from the pool through the blocking facade.
    pub fn drop_from_pool(self) -> Result<(), PoolError> {
        self.inner.drop_from_pool_blocking_impl()
    }
}

pub struct PooledConnection<B: PoolBackend> {
    pool: Pool<B>,
    conn_id: u64,
    armed: bool,
}

impl<B: PoolBackend> PooledConnection<B> {
    fn new(pool: Pool<B>, conn_id: u64) -> Self {
        Self {
            pool,
            conn_id,
            armed: true,
        }
    }

    /// Engine-assigned identity for embedders that keep sidecar objects.
    pub fn id(&self) -> u64 {
        self.conn_id
    }

    /// Eagerly return the connection to the pool through the async facade.
    pub async fn release(mut self, cx: &Cx) -> Result<(), PoolError> {
        checkpoint_pool(cx)?;
        let conn_id = self.disarm();
        match self.pool.engine.return_connection_impl(conn_id) {
            Ok(()) => Ok(()),
            Err(err) => {
                self.rearm();
                Err(err)
            }
        }
    }

    fn release_blocking_impl(mut self) -> Result<(), PoolError> {
        let conn_id = self.disarm();
        match self.pool.engine.return_connection(conn_id) {
            Ok(()) => Ok(()),
            Err(err) => {
                self.rearm();
                Err(err)
            }
        }
    }

    /// Drop the physical connection from the pool instead of returning it.
    pub async fn drop_from_pool(mut self, cx: &Cx) -> Result<(), PoolError> {
        checkpoint_pool(cx)?;
        let conn_id = self.disarm();
        match self.pool.engine.drop_connection_impl(conn_id) {
            Ok(()) => Ok(()),
            Err(err) => {
                self.rearm();
                Err(err)
            }
        }
    }

    fn drop_from_pool_blocking_impl(mut self) -> Result<(), PoolError> {
        let conn_id = self.disarm();
        match self.pool.engine.drop_connection(conn_id) {
            Ok(()) => Ok(()),
            Err(err) => {
                self.rearm();
                Err(err)
            }
        }
    }

    fn disarm(&mut self) -> u64 {
        self.armed = false;
        self.conn_id
    }

    fn rearm(&mut self) {
        self.armed = true;
    }
}

impl<B: PoolBackend> Drop for PooledConnection<B> {
    fn drop(&mut self) {
        if self.armed {
            self.pool.engine.enqueue_drop_return(self.conn_id);
        }
    }
}

pub(crate) struct PoolEngine<B: PoolBackend> {
    inner: Arc<EngineInner<B>>,
    /// The dedicated single-thread asupersync runtime that hosts the reaper task.
    ///
    /// CRITICAL: the runtime is owned **here**, by the external pool handles —
    /// NOT inside `EngineInner`. The reaper runs on this runtime's worker thread
    /// and can transiently hold the last `Arc<EngineInner>`; if the runtime lived
    /// in `EngineInner`, that final drop (on the worker thread) would drop the
    /// runtime and try to join the worker from within itself (EDEADLK). Owning
    /// the runtime on the external handles lets the last pool handle hand the
    /// runtime join to a helper thread, never the worker thread itself.
    runtime: Option<Arc<Runtime>>,
    /// Live external `PoolEngine` handles. Clone increments; Drop decrements
    /// with `fetch_sub`. Exactly one drop observes the `1 → 0` transition and
    /// runs last-handle teardown. This deliberately does **not** use
    /// `Arc::strong_count` on `runtime` — strong-count is not a synchronization
    /// API and would be perturbed if anything else ever held the `Runtime` Arc.
    handles: Arc<AtomicUsize>,
}

impl<B: PoolBackend> Clone for PoolEngine<B> {
    fn clone(&self) -> Self {
        self.handles.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
            runtime: Some(Arc::clone(
                self.runtime
                    .as_ref()
                    .expect("pool runtime present before drop"),
            )),
            handles: Arc::clone(&self.handles),
        }
    }
}

impl<B: PoolBackend> Drop for PoolEngine<B> {
    fn drop(&mut self) {
        // Already torn down (runtime taken by a prior last-handle path) — nothing
        // to do. Normal Clone/Drop pairs always leave `runtime` populated until
        // the sole last-handle winner takes it.
        if self.runtime.is_none() {
            return;
        }
        // Atomic handle accounting: exactly one drop observes prev == 1.
        let prev = self.handles.fetch_sub(1, Ordering::AcqRel);
        if prev != 1 {
            return;
        }
        // Last handle. Close every connection RIGHT HERE, on the dropping thread,
        // WITHOUT awaiting the reaper. Doing the close synchronously here (rather
        // than delegating to the reaper or to `EngineInner::drop`, which could
        // run on the worker thread that the `runtime` field is about to tear
        // down) makes transport release deterministic and race-free: it cannot
        // lose a connection to the worker being force-stopped mid-drain.
        close_all_connections(&self.inner);
        let runtime = self
            .runtime
            .take()
            .expect("pool runtime present for last handle");
        // Some embedders run finalizers while holding a VM lock (for example the
        // Python GIL). The pool worker may be inside `backend.create_connection`
        // and need that same lock to finish. Dropping the runtime here would join
        // the worker while still holding the embedder's lock, so hand that final
        // join to a detached helper thread. The handoff guarantees the runtime is
        // NEVER dropped on this (possibly lock-holding) thread: if the helper
        // cannot be spawned (or vanishes before receiving), we `mem::forget` the
        // runtime — leaking it at pool/process teardown is strictly preferable to
        // re-introducing the very GIL-vs-worker-join deadlock this avoids.
        //
        // This single helper is unrelated to TIMEDWAIT: it runs once per pool
        // lifetime at last-handle teardown, never once per waiter.
        let (tx, rx) = std::sync::mpsc::sync_channel::<Arc<Runtime>>(1);
        match std::thread::Builder::new()
            .name("oracledb-pool-runtime-drop".to_string())
            .spawn(move || {
                if let Ok(runtime) = rx.recv() {
                    drop(runtime);
                }
            }) {
            Ok(_) => {
                if let Err(std::sync::mpsc::SendError(runtime)) = tx.send(runtime) {
                    std::mem::forget(runtime);
                }
            }
            Err(_) => std::mem::forget(runtime),
        }
    }
}

impl<B: PoolBackend> Drop for EngineInner<B> {
    fn drop(&mut self) {
        // Backstop: if `EngineInner` is dropped through any path other than the
        // last `PoolEngine` handle, still release every remaining transport.
        // `close_all_connections` is idempotent — connections are removed from
        // state as they are closed — so running it here too never double-closes.
        self.bg.notify_one();
        close_all_connections(self);
    }
}

/// Mark the pool closed and synchronously close every connection it still owns
/// (idle, busy, queued-to-drop, or attached to an in-flight request). Removing
/// each connection from `state` as it is closed makes repeated calls idempotent.
fn close_all_connections<B: PoolBackend>(inner: &EngineInner<B>) {
    if let Ok(mut state) = inner.state.lock() {
        state.open = false;
        inner.reaper_stop.store(true, Ordering::SeqCst);
        let mut leftovers: Vec<PooledConn<B::Conn>> = std::mem::take(&mut state.free_new);
        leftovers.append(&mut state.free_used);
        leftovers.append(&mut state.busy);
        leftovers.extend(state.to_drop.drain(..));
        for request in &mut state.requests {
            if let Some(conn) = request.conn.take() {
                leftovers.push(conn);
            }
        }
        for conn in leftovers {
            inner.backend.close_connection(conn.id, conn.conn);
        }
    }
}

type Locked<'a, C> = std::sync::MutexGuard<'a, PoolState<C>>;

fn lock_state<B: PoolBackend>(inner: &EngineInner<B>) -> Result<Locked<'_, B::Conn>, PoolError> {
    inner
        .state
        .lock()
        .map_err(|err| PoolError::Internal(err.to_string()))
}

fn checkpoint_pool(cx: &Cx) -> Result<(), PoolError> {
    cx.checkpoint()
        .map_err(|err| PoolError::Cancelled(err.to_string()))
}

fn block_on_pool<F, Fut, T>(operation: F) -> Result<T, PoolError>
where
    F: FnOnce(Cx) -> Fut,
    Fut: Future<Output = Result<T, PoolError>>,
{
    let runtime = crate::build_io_runtime().map_err(|err| PoolError::Internal(err.to_string()))?;
    runtime.block_on(async {
        let cx = Cx::current().ok_or_else(|| {
            PoolError::Internal("asupersync did not install an ambient Cx".to_string())
        })?;
        operation(cx).await
    })
}

fn wake_waiters<B: PoolBackend>(inner: &EngineInner<B>) {
    inner.async_waiters.notify_waiters();
}

fn saturating_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn request_is_opening<C>(request: &Request<C>) -> bool {
    request.bg_processing
        && !request.requires_ping
        && !request.completed
        && request.error.is_none()
        && (request.in_progress || request.is_extra || request.is_replacing)
}

fn pending_open_count<C>(state: &PoolState<C>) -> u32 {
    let effect_count = state
        .open_effects
        .iter()
        .chain(state.in_flight_open_effects.iter())
        .filter(|effect| matches!(effect, lifecycle::PoolEffect::Open { .. }))
        .count();
    let request_count = state
        .requests
        .iter()
        .filter(|request| request_is_opening(request))
        .count();
    saturating_u32(effect_count.saturating_add(request_count))
}

fn compatible_idle_count<C>(
    state: &PoolState<C>,
    wants_new: bool,
    request_cclass: Option<&str>,
    cclass_matches: bool,
) -> u32 {
    if wants_new {
        return 0;
    }
    let used = state
        .free_used
        .iter()
        .filter(|conn| request_cclass.is_none() || conn.cclass.as_deref() == request_cclass)
        .count();
    let new = if cclass_matches {
        state.free_new.len()
    } else {
        0
    };
    saturating_u32(used.saturating_add(new))
}

fn compatible_waiting_demand<C>(state: &PoolState<C>) -> u32 {
    saturating_u32(
        state
            .requests
            .iter()
            .filter(|request| {
                request.waiting
                    && request.cclass_matches
                    && !request.completed
                    && !request.requires_ping
                    && !request.is_extra
                    && !request.is_replacing
                    && request.conn.is_none()
                    && request.error.is_none()
            })
            .count(),
    )
}

fn active_open_count<C>(state: &PoolState<C>) -> u32 {
    let request_connections = state
        .requests
        .iter()
        .filter(|request| request.conn.is_some())
        .count();
    saturating_u32(
        state
            .free_new
            .len()
            .saturating_add(state.free_used.len())
            .saturating_add(state.busy.len())
            .saturating_add(request_connections),
    )
}

fn reserved_open_count<C>(state: &PoolState<C>) -> u32 {
    active_open_count(state).saturating_add(pending_open_count(state))
}

fn derived_lifecycle_counts<C>(state: &PoolState<C>) -> lifecycle::PoolCounts {
    lifecycle::PoolCounts {
        opening: pending_open_count(state) as usize,
        idle: state.free_new.len().saturating_add(state.free_used.len()),
        checked_out: state.busy.len(),
        validating: state
            .requests
            .iter()
            .filter(|request| {
                request.requires_ping && (request.bg_processing || request.in_progress)
            })
            .count(),
        retiring: state.to_drop.len(),
        waiters: state
            .requests
            .iter()
            .filter(|request| request.waiting && !request.completed && request.error.is_none())
            .count(),
        ..lifecycle::PoolCounts::default()
    }
}

fn pool_stats<C>(state: &PoolState<C>) -> PoolStats {
    let counts = derived_lifecycle_counts(state);
    PoolStats {
        open: active_open_count(state),
        busy: saturating_u32(counts.checked_out),
        idle: saturating_u32(counts.idle),
        opening: saturating_u32(counts.opening),
        validating: saturating_u32(counts.validating),
        retiring: saturating_u32(counts.retiring),
        waiters: saturating_u32(counts.waiters),
    }
}

fn schedule_open_effects<C>(state: &mut PoolState<C>, count: u32) {
    for _ in 0..count {
        let slot = state.next_conn_id;
        state.next_conn_id += 1;
        state
            .open_effects
            .push_back(lifecycle::PoolEffect::Open { slot, waiter: None });
    }
}

fn complete_open_effect<C>(state: &mut PoolState<C>, conn_id: u64) {
    if let Some(position) = state.in_flight_open_effects.iter().position(|effect| {
        matches!(
            effect,
            lifecycle::PoolEffect::Open { slot, .. } if *slot == conn_id
        )
    }) {
        state.in_flight_open_effects.remove(position);
    }
}

fn drain_drop_returns<B: PoolBackend>(
    state: &mut PoolState<B::Conn>,
    inner: &EngineInner<B>,
) -> Result<bool, PoolError> {
    let receiver = inner
        .drop_returns_rx
        .lock()
        .map_err(|err| PoolError::Internal(err.to_string()))?;
    let mut drained = false;
    while let Ok(conn_id) = receiver.try_recv() {
        drained = true;
        if !state.open {
            continue;
        }
        let Some(position) = state.busy.iter().position(|conn| conn.id == conn_id) else {
            continue;
        };
        let conn = state.busy.remove(position);
        let is_open = inner.backend.connection_is_open(&conn.conn);
        return_connection_helper(state, inner, conn, is_open);
    }
    Ok(drained)
}

fn request_worker_shutdown_locked<B: PoolBackend>(
    state: &mut PoolState<B::Conn>,
    inner: &EngineInner<B>,
) {
    state.open = false;
    // Flip the cooperative stop flag the reaper observes at the top of its loop.
    inner.reaper_stop.store(true, Ordering::SeqCst);
    state.open_effects.clear();
    let free_new = std::mem::take(&mut state.free_new);
    let free_used = std::mem::take(&mut state.free_used);
    let busy = std::mem::take(&mut state.busy);
    state
        .to_drop
        .extend(free_new.into_iter().chain(free_used).chain(busy));
    let mut in_flight = Vec::new();
    for mut request in std::mem::take(&mut state.requests) {
        request.waiting = false;
        if let Some(conn) = request.conn.take() {
            state.to_drop.push_back(conn);
        }
        if request.in_progress {
            in_flight.push(request);
        }
    }
    state.requests = in_flight;
    inner.bg.notify_one();
    wake_waiters(inner);
}

fn request_worker_shutdown<B: PoolBackend>(
    inner: &EngineInner<B>,
    force: bool,
) -> Result<(), PoolError> {
    let mut state = lock_state(inner)?;
    if drain_drop_returns(&mut state, inner)? {
        wake_waiters(inner);
    }
    if !state.open {
        inner.bg.notify_one();
        wake_waiters(inner);
        return Ok(());
    }
    if !force {
        let has_waiters = state.requests.iter().any(|request| request.waiting);
        if !state.busy.is_empty() || has_waiters {
            return Err(PoolError::HasBusyConnections);
        }
    }
    request_worker_shutdown_locked(&mut state, inner);
    Ok(())
}

impl<B: PoolBackend> PoolEngine<B> {
    /// Create the engine and start its background worker, which eagerly
    /// grows the pool to `min` connections.
    pub fn start(backend: B, config: PoolConfig) -> Result<Self, PoolError> {
        let force_get = config.getmode == POOL_GETMODE_FORCEGET;
        let wait_timeout_ms = if config.getmode == POOL_GETMODE_TIMEDWAIT {
            Some(config.wait_timeout_ms)
        } else {
            None
        };
        let mut state = PoolState {
            open: true,
            config,
            force_get,
            wait_timeout_ms,
            free_new: Vec::new(),
            free_used: Vec::new(),
            busy: Vec::new(),
            to_drop: VecDeque::new(),
            requests: Vec::new(),
            open_effects: VecDeque::new(),
            in_flight_open_effects: VecDeque::new(),
            next_conn_id: 1,
            next_request_id: 1,
        };
        let min = state.config.min;
        schedule_open_effects(&mut state, min);
        let (drop_returns_tx, drop_returns_rx) = mpsc::channel();
        // The pool owns a dedicated single-thread asupersync runtime. Its one
        // worker thread runs the scheduler loop continuously, so the spawned
        // reaper task makes progress independently of any `block_on`. The runtime
        // is held by the external `PoolEngine` handles (see the field docs), NOT
        // by `EngineInner`, so the worker thread never drops/joins itself.
        let runtime = Arc::new(
            crate::new_pool_runtime().map_err(|err| PoolError::Internal(err.to_string()))?,
        );
        let inner = Arc::new(EngineInner {
            backend,
            state: Mutex::new(state),
            drop_returns_tx,
            drop_returns_rx: Mutex::new(drop_returns_rx),
            async_waiters: Notify::new(),
            bg: Arc::new(Notify::new()),
            reaper_stop: AtomicBool::new(false),
            reaper_handle: Mutex::new(None),
        });
        // The reaper holds a `Weak` so it never keeps `EngineInner` alive across a
        // park; it also holds a strong `Arc` to the `bg` notifier so it can park
        // without upgrading the `Weak`. When the last pool handle is dropped, the
        // runtime (owned by that handle) is shut down, which stops the reaper.
        let reaper_inner = Arc::downgrade(&inner);
        let reaper_bg = Arc::clone(&inner.bg);
        let handle = runtime.handle().spawn(reaper_main(reaper_inner, reaper_bg));
        *inner
            .reaper_handle
            .lock()
            .map_err(|err| PoolError::Internal(err.to_string()))? = Some(handle);
        Ok(Self {
            inner,
            runtime: Some(runtime),
            handles: Arc::new(AtomicUsize::new(1)),
        })
    }

    fn enqueue_drop_return(&self, conn_id: u64) {
        if self.inner.drop_returns_tx.send(conn_id).is_ok() {
            self.inner.bg.notify_one();
            wake_waiters(&self.inner);
        }
    }

    /// Blocking facade for [`Self::acquire_async`].
    ///
    /// Returns the engine id of the connection now recorded as busy. Callers
    /// must not hold the GIL or any embedder lock.
    pub fn acquire(&self, opts: AcquireOptions) -> Result<u64, PoolError> {
        block_on_pool(|cx| async move { self.acquire_async(&cx, opts).await })
    }

    /// Acquire a connection without parking the current OS thread.
    ///
    /// This uses the same request queue and fulfillment algebra as
    /// [`Self::acquire`], but waits on an async notification and checkpoints
    /// the caller's [`Cx`] before and after each wait. The existing blocking
    /// method remains the pyshim/sync facade.
    pub async fn acquire_async(&self, cx: &Cx, opts: AcquireOptions) -> Result<u64, PoolError> {
        checkpoint_pool(cx)?;
        let inner = &*self.inner;
        let (request_id, wait_timeout) = {
            let mut state = lock_state(inner)?;
            if drain_drop_returns(&mut state, inner)? {
                wake_waiters(inner);
            }
            let request_id = enqueue_request(&mut state, opts)?;
            let wait_timeout = state
                .wait_timeout_ms
                .map(|ms| Duration::from_millis(u64::from(ms)));
            (request_id, wait_timeout)
        };
        let mut request = AsyncAcquireRequest::new(inner, request_id);
        let result = acquire_wait_future(cx, inner, request_id, wait_timeout).await;

        match result {
            Ok(conn_id) => {
                request.complete();
                Ok(conn_id)
            }
            Err(err) => {
                request.abandon();
                Err(err)
            }
        }
    }

    /// Return a busy connection to the pool. The embedder performs the
    /// end-of-request work (rollback) before calling this. No-op when the
    /// pool is already closed (mirrors the reference).
    pub fn return_connection(&self, conn_id: u64) -> Result<(), PoolError> {
        block_on_pool(|cx| async move { self.return_connection_async(&cx, conn_id).await })
    }

    /// Return a busy connection through the async pool facade.
    pub(crate) async fn return_connection_async(
        &self,
        cx: &Cx,
        conn_id: u64,
    ) -> Result<(), PoolError> {
        checkpoint_pool(cx)?;
        self.return_connection_impl(conn_id)
    }

    fn return_connection_impl(&self, conn_id: u64) -> Result<(), PoolError> {
        let inner = &*self.inner;
        let mut state = lock_state(inner)?;
        drain_drop_returns(&mut state, inner)?;
        if !state.open {
            return Ok(());
        }
        let Some(position) = state.busy.iter().position(|conn| conn.id == conn_id) else {
            // The connection is not currently checked out: a double-release, or a
            // connection already dropped/returned. The reference raises DPY-1001
            // here; return a typed error instead of the former silent no-op so a
            // caller programming error is surfaced, not hidden. (The best-effort
            // Drop path in `drain_drop_returns` keeps its silent skip — a queued
            // return for an already-returned conn is expected cleanup, not a bug.)
            return Err(PoolError::ConnectionNotAcquired);
        };
        let conn = state.busy.remove(position);
        let is_open = inner.backend.connection_is_open(&conn.conn);
        return_connection_helper(&mut state, inner, conn, is_open);
        wake_waiters(inner);
        Ok(())
    }

    /// Drop a busy connection from the pool (`ConnectionPool.drop`).
    pub fn drop_connection(&self, conn_id: u64) -> Result<(), PoolError> {
        block_on_pool(|cx| async move { self.drop_connection_async(&cx, conn_id).await })
    }

    /// Drop a busy connection through the async pool facade.
    pub(crate) async fn drop_connection_async(
        &self,
        cx: &Cx,
        conn_id: u64,
    ) -> Result<(), PoolError> {
        checkpoint_pool(cx)?;
        self.drop_connection_impl(conn_id)
    }

    fn drop_connection_impl(&self, conn_id: u64) -> Result<(), PoolError> {
        let inner = &*self.inner;
        let mut state = lock_state(inner)?;
        drain_drop_returns(&mut state, inner)?;
        if !state.open {
            return Ok(());
        }
        let Some(position) = state.busy.iter().position(|conn| conn.id == conn_id) else {
            return Ok(());
        };
        let conn = state.busy.remove(position);
        drop_conn(&mut state, inner, conn);
        wake_waiters(inner);
        Ok(())
    }

    /// Close the pool. With `force == false`, fails when busy connections or
    /// live waiters exist (DPY-1005). Cooperatively joins the region-owned
    /// reaper task, so all transports are closed by the time this returns.
    ///
    /// Blocking: callers must not hold the GIL or any embedder lock.
    pub fn close(&self, force: bool) -> Result<(), PoolError> {
        block_on_pool(|cx| async move { self.close_async(&cx, force).await })
    }

    /// Close the pool through the async facade.
    ///
    /// This requests shutdown and then **awaits** the region-owned reaper task to
    /// completion — it never parks the executor on a synchronous OS-thread join.
    /// The reaper, observing the cooperative stop flag, drains the close queue
    /// (closing every transport) and returns, which completes the task handle we
    /// await here.
    pub(crate) async fn close_async(&self, cx: &Cx, force: bool) -> Result<(), PoolError> {
        checkpoint_pool(cx)?;
        let inner = &*self.inner;
        request_worker_shutdown(inner, force)?;
        // Wake the reaper so it observes the stop flag promptly, then take its
        // join handle. Taking under the lock makes concurrent/duplicate closes
        // race-free: only the first close awaits; later ones find `None`.
        inner.bg.notify_one();
        let handle = inner
            .reaper_handle
            .lock()
            .map_err(|err| PoolError::Internal(err.to_string()))?
            .take();
        if let Some(handle) = handle {
            checkpoint_pool(cx)?;
            // Cooperative async join: the reaper runs on the pool's own runtime;
            // this await is woken when it finishes. No std-thread join, so the
            // executor worker polling this future is never blocked.
            handle.await;
        }
        Ok(())
    }

    pub fn drain(&self) -> Result<(), PoolError> {
        block_on_pool(|cx| async move { self.drain_async(&cx).await })
    }

    pub(crate) async fn drain_async(&self, cx: &Cx) -> Result<(), PoolError> {
        checkpoint_pool(cx)?;
        let mut state = lock_state(&self.inner)?;
        if drain_drop_returns(&mut state, &self.inner)? {
            wake_waiters(&self.inner);
        }
        Ok(())
    }

    pub fn stats(&self) -> Result<PoolStats, PoolError> {
        block_on_pool(|cx| async move { self.stats_async(&cx).await })
    }

    pub(crate) async fn stats_async(&self, cx: &Cx) -> Result<PoolStats, PoolError> {
        checkpoint_pool(cx)?;
        let mut state = lock_state(&self.inner)?;
        if drain_drop_returns(&mut state, &self.inner)? {
            wake_waiters(&self.inner);
        }
        Ok(pool_stats(&state))
    }

    pub fn busy_count(&self) -> Result<u32, PoolError> {
        block_on_pool(|cx| async move { self.busy_count_async(&cx).await })
    }

    pub(crate) async fn busy_count_async(&self, cx: &Cx) -> Result<u32, PoolError> {
        checkpoint_pool(cx)?;
        let mut state = lock_state(&self.inner)?;
        if drain_drop_returns(&mut state, &self.inner)? {
            wake_waiters(&self.inner);
        }
        Ok(saturating_u32(derived_lifecycle_counts(&state).checked_out))
    }

    pub fn open_count(&self) -> Result<u32, PoolError> {
        block_on_pool(|cx| async move { self.open_count_async(&cx).await })
    }

    pub(crate) async fn open_count_async(&self, cx: &Cx) -> Result<u32, PoolError> {
        checkpoint_pool(cx)?;
        let mut state = lock_state(&self.inner)?;
        if drain_drop_returns(&mut state, &self.inner)? {
            wake_waiters(&self.inner);
        }
        Ok(active_open_count(&state))
    }

    pub fn getmode(&self) -> Result<u32, PoolError> {
        let state = lock_state(&self.inner)?;
        Ok(state.config.getmode)
    }

    /// Mirrors reference `set_getmode`: switching to TIMEDWAIT resets the
    /// wait timeout to 0; any other mode clears it entirely.
    pub fn set_getmode(&self, value: u32) -> Result<(), PoolError> {
        let mut state = lock_state(&self.inner)?;
        if state.config.getmode != value {
            state.config.getmode = value;
            state.force_get = value == POOL_GETMODE_FORCEGET;
            state.wait_timeout_ms = if value == POOL_GETMODE_TIMEDWAIT {
                Some(0)
            } else {
                None
            };
        }
        Ok(())
    }

    /// Mirrors reference `get_wait_timeout`: the stored value when getmode is
    /// TIMEDWAIT, otherwise 0. The stored value is in milliseconds; the
    /// embedder reproduces the reference's seconds-float quirk.
    pub fn wait_timeout_ms(&self) -> Result<Option<u32>, PoolError> {
        let state = lock_state(&self.inner)?;
        Ok(state.wait_timeout_ms)
    }

    pub fn set_wait_timeout_ms(&self, value: u32) -> Result<(), PoolError> {
        let mut state = lock_state(&self.inner)?;
        state.wait_timeout_ms = if state.config.getmode == POOL_GETMODE_TIMEDWAIT {
            Some(value)
        } else {
            None
        };
        Ok(())
    }

    pub fn timeout_secs(&self) -> Result<u32, PoolError> {
        let state = lock_state(&self.inner)?;
        Ok(state.config.timeout_secs)
    }

    pub fn set_timeout_secs(&self, value: u32) -> Result<(), PoolError> {
        let mut state = lock_state(&self.inner)?;
        state.config.timeout_secs = value;
        self.inner.bg.notify_one();
        Ok(())
    }

    pub fn max_lifetime_session_secs(&self) -> Result<u32, PoolError> {
        let state = lock_state(&self.inner)?;
        Ok(state.config.max_lifetime_session_secs)
    }

    pub fn set_max_lifetime_session_secs(&self, value: u32) -> Result<(), PoolError> {
        let mut state = lock_state(&self.inner)?;
        state.config.max_lifetime_session_secs = value;
        Ok(())
    }

    pub fn ping_interval_secs(&self) -> Result<i64, PoolError> {
        let state = lock_state(&self.inner)?;
        Ok(state.config.ping_interval_secs)
    }

    pub fn set_ping_interval_secs(&self, value: i64) -> Result<(), PoolError> {
        let mut state = lock_state(&self.inner)?;
        state.config.ping_interval_secs = value;
        Ok(())
    }
}

fn request_position<C>(state: &PoolState<C>, request_id: u64) -> Option<usize> {
    state
        .requests
        .iter()
        .position(|request| request.id == request_id)
}

/// Remove a request that will never be completed (timeout / error). When the
/// background worker still owns it, leave it queued: post-processing will see
/// `waiting == false` and reject any connection back into the pool.
fn abandon_request<B: PoolBackend>(
    state: &mut PoolState<B::Conn>,
    inner: &EngineInner<B>,
    request_id: u64,
) {
    let Some(position) = request_position(state, request_id) else {
        return;
    };
    state.requests[position].waiting = false;
    if !state.requests[position].bg_processing {
        let mut request = state.requests.remove(position);
        reject(state, inner, &mut request);
    }
}

/// Reference `PooledConnRequest.reject`: hand any attached connection back to
/// the appropriate free list (or the drop queue for extra connections).
fn reject<B: PoolBackend>(
    state: &mut PoolState<B::Conn>,
    inner: &EngineInner<B>,
    request: &mut Request<B::Conn>,
) {
    if let Some(mut conn) = request.conn.take() {
        if !state.open {
            conn.is_pool_extra = false;
            state.to_drop.push_back(conn);
            inner.bg.notify_one();
            wake_waiters(inner);
            return;
        }
        if conn.is_pool_extra {
            conn.is_pool_extra = false;
            state.to_drop.push_back(conn);
            inner.bg.notify_one();
        } else if !conn.ever_acquired {
            state.free_new.push(conn);
        } else {
            state.free_used.push(conn);
        }
        wake_waiters(inner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asupersync::lab::{DporExplorer, ExplorerConfig, LabRuntime};
    use asupersync::types::Budget;
    use lifecycle::{
        PoolCloseReason, PoolCounts, PoolEffect, PoolSlotState, PurePoolState, SlotId, WaiterId,
    };
    use std::collections::{BTreeMap, BTreeSet, VecDeque};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    // Test-only: `BlockingCreateBackend` is a fake backend that blocks inside
    // `create_connection` to exercise the in-flight-create close path. Its
    // `Condvar` models a slow remote server; it is unrelated to the (now async)
    // pool reaper, which no longer uses any `Condvar`.
    use std::sync::{Barrier, Condvar};

    #[test]
    fn pure_pool_state_opens_min_and_derives_counts() {
        let mut state = PurePoolState::new(2, 4);
        assert_eq!(
            state.take_effects(),
            vec![
                PoolEffect::Open {
                    slot: 1,
                    waiter: None,
                },
                PoolEffect::Open {
                    slot: 2,
                    waiter: None,
                },
            ]
        );
        assert_eq!(
            state.counts(),
            PoolCounts {
                opening: 2,
                ..PoolCounts::default()
            }
        );

        state.complete_open(1, true).unwrap();
        state.complete_open(2, true).unwrap();
        assert_eq!(
            state.counts(),
            PoolCounts {
                idle: 2,
                ..PoolCounts::default()
            }
        );
        assert_eq!(
            state.history(),
            &[
                "open slot=1 waiter=None".to_string(),
                "open slot=2 waiter=None".to_string(),
                "open slot=1 idle".to_string(),
                "open slot=2 idle".to_string(),
            ]
        );
    }

    #[test]
    fn pure_pool_state_cancelled_opening_hands_slot_to_next_waiter() {
        let mut state = PurePoolState::new(0, 1);
        let first = state.request_acquire().unwrap();
        assert_eq!(
            state.take_effects(),
            vec![PoolEffect::Open {
                slot: 1,
                waiter: Some(first),
            }]
        );
        let second = state.request_acquire().unwrap();
        assert!(state.take_effects().is_empty());

        state.cancel_acquire(first).unwrap();
        state.complete_open(1, true).unwrap();
        assert_eq!(
            state.take_effects(),
            vec![PoolEffect::Ping {
                slot: 1,
                waiter: second,
            }]
        );
        state.complete_ping(1, true).unwrap();

        assert_eq!(
            state.state_of(1),
            Some(PoolSlotState::CheckedOut { waiter: second })
        );
        assert_eq!(
            state.counts(),
            PoolCounts {
                checked_out: 1,
                ..PoolCounts::default()
            }
        );
    }

    #[test]
    fn pure_pool_state_cancelled_checked_out_hands_slot_to_next_waiter() {
        let mut state = PurePoolState::new(0, 1);
        let first = state.request_acquire().unwrap();
        assert_eq!(
            state.take_effects(),
            vec![PoolEffect::Open {
                slot: 1,
                waiter: Some(first),
            }]
        );
        state.complete_open(1, true).unwrap();
        assert_eq!(
            state.state_of(1),
            Some(PoolSlotState::CheckedOut { waiter: first })
        );

        let second = state.request_acquire().unwrap();
        assert!(state.take_effects().is_empty());
        state.cancel_acquire(first).unwrap();
        assert_eq!(
            state.take_effects(),
            vec![PoolEffect::Ping {
                slot: 1,
                waiter: second,
            }]
        );
        state.complete_ping(1, true).unwrap();
        assert_eq!(
            state.state_of(1),
            Some(PoolSlotState::CheckedOut { waiter: second })
        );
        assert_eq!(
            state.counts(),
            PoolCounts {
                checked_out: 1,
                ..PoolCounts::default()
            }
        );
    }

    #[test]
    fn pure_pool_state_release_respects_fifo_waiter_order() {
        let mut state = PurePoolState::new(1, 1);
        assert_eq!(state.take_effects().len(), 1);
        state.complete_open(1, true).unwrap();
        let first = state.request_acquire().unwrap();
        assert_eq!(
            state.take_effects(),
            vec![PoolEffect::Ping {
                slot: 1,
                waiter: first,
            }]
        );
        state.complete_ping(1, true).unwrap();
        let second = state.request_acquire().unwrap();
        assert!(state.take_effects().is_empty());

        state.release(1).unwrap();
        assert_eq!(
            state.take_effects(),
            vec![PoolEffect::Ping {
                slot: 1,
                waiter: second,
            }]
        );
        state.complete_ping(1, true).unwrap();
        assert_eq!(
            state.state_of(1),
            Some(PoolSlotState::CheckedOut { waiter: second })
        );
    }

    #[test]
    fn pure_pool_state_unhealthy_ping_requeues_waiter_and_reopens() {
        let mut state = PurePoolState::new(1, 1);
        assert_eq!(state.take_effects().len(), 1);
        state.complete_open(1, true).unwrap();
        let waiter = state.request_acquire().unwrap();
        assert_eq!(
            state.take_effects(),
            vec![PoolEffect::Ping { slot: 1, waiter }]
        );

        state.complete_ping(1, false).unwrap();
        assert_eq!(
            state.take_effects(),
            vec![PoolEffect::Close {
                slot: 1,
                reason: PoolCloseReason::Unhealthy,
            }]
        );
        assert_eq!(
            state.counts(),
            PoolCounts {
                retiring: 1,
                waiters: 1,
                ..PoolCounts::default()
            }
        );

        state.complete_close(1).unwrap();
        assert_eq!(
            state.take_effects(),
            vec![PoolEffect::Open {
                slot: 2,
                waiter: Some(waiter),
            }]
        );
        state.complete_open(2, true).unwrap();
        assert_eq!(
            state.state_of(2),
            Some(PoolSlotState::CheckedOut { waiter })
        );
    }

    #[test]
    fn pure_pool_state_reap_and_close_modes_are_event_driven() {
        let mut state = PurePoolState::new(1, 2);
        assert_eq!(state.take_effects().len(), 1);
        state.complete_open(1, true).unwrap();

        state.reap_idle(1).unwrap();
        assert_eq!(
            state.take_effects(),
            vec![PoolEffect::Close {
                slot: 1,
                reason: PoolCloseReason::Reap,
            }]
        );
        state.complete_close(1).unwrap();
        assert_eq!(
            state.take_effects(),
            vec![PoolEffect::Open {
                slot: 2,
                waiter: None,
            }]
        );
        state.complete_open(2, true).unwrap();

        let waiter = state.request_acquire().unwrap();
        assert_eq!(
            state.take_effects(),
            vec![PoolEffect::Ping { slot: 2, waiter }]
        );
        state.complete_ping(2, true).unwrap();
        assert_eq!(
            state.begin_close(false),
            Err(lifecycle::PoolLifecycleError::Busy)
        );

        let waiting = state.request_acquire().unwrap();
        assert_eq!(
            state.take_effects(),
            vec![PoolEffect::Open {
                slot: 3,
                waiter: Some(waiting),
            }]
        );
        state.begin_close(true).unwrap();
        assert_eq!(
            state.take_effects(),
            vec![PoolEffect::Close {
                slot: 2,
                reason: PoolCloseReason::Force,
            }]
        );
        assert_eq!(
            state.counts(),
            PoolCounts {
                opening: 1,
                closing: 1,
                closed: 1,
                ..PoolCounts::default()
            }
        );
        state.complete_open(3, true).unwrap();
        assert_eq!(
            state.take_effects(),
            vec![PoolEffect::Close {
                slot: 3,
                reason: PoolCloseReason::Force,
            }]
        );
    }

    #[test]
    fn pure_pool_state_force_close_waits_for_in_flight_open() {
        let mut state = PurePoolState::new(0, 1);
        let waiter = state.request_acquire().unwrap();
        assert_eq!(
            state.take_effects(),
            vec![PoolEffect::Open {
                slot: 1,
                waiter: Some(waiter),
            }]
        );

        state.begin_close(true).unwrap();
        assert!(
            state.take_effects().is_empty(),
            "open is in flight; close is emitted after open completion"
        );
        assert_eq!(
            state.counts(),
            PoolCounts {
                opening: 1,
                ..PoolCounts::default()
            }
        );

        state.complete_open(1, true).unwrap();
        assert_eq!(
            state.take_effects(),
            vec![PoolEffect::Close {
                slot: 1,
                reason: PoolCloseReason::Force,
            }]
        );
        assert_eq!(
            state.counts(),
            PoolCounts {
                closing: 1,
                ..PoolCounts::default()
            }
        );
    }

    const DPOR_POOL_SATURATION_WINDOW: usize = 1;
    const DPOR_POOL_SEED: u64 = 0xE4_D0_00;
    const DPOR_POOL_MAX_ITERS: usize = 128;
    const DPOR_POOL_LIFECYCLE_DEPTH: usize = 7;

    #[derive(Clone, Debug)]
    struct LifecycleDporModel {
        state: PurePoolState,
        pending_open: VecDeque<SlotId>,
        pending_ping: VecDeque<SlotId>,
        pending_close: VecDeque<SlotId>,
        fifo_waiters: VecDeque<WaiterId>,
        checked_out: BTreeMap<WaiterId, SlotId>,
        closing: bool,
    }

    impl LifecycleDporModel {
        fn new() -> Self {
            let mut model = Self {
                state: PurePoolState::new(0, 2),
                pending_open: VecDeque::new(),
                pending_ping: VecDeque::new(),
                pending_close: VecDeque::new(),
                fifo_waiters: VecDeque::new(),
                checked_out: BTreeMap::new(),
                closing: false,
            };
            model.drain_effects();
            model
        }

        fn apply(&mut self, op: LifecycleDporOp) -> bool {
            match op {
                LifecycleDporOp::Acquire => {
                    if self.closing
                        || self
                            .fifo_waiters
                            .len()
                            .saturating_add(self.checked_out.len())
                            >= 3
                    {
                        return false;
                    }
                    let waiter = self
                        .state
                        .request_acquire()
                        .expect("lifecycle acquire before close");
                    self.fifo_waiters.push_back(waiter);
                }
                LifecycleDporOp::CancelOldest => {
                    let Some(waiter) = self.fifo_waiters.pop_front() else {
                        return false;
                    };
                    self.state
                        .cancel_acquire(waiter)
                        .expect("cancel tracked lifecycle waiter");
                }
                LifecycleDporOp::OpenHealthy => {
                    let Some(slot) = self.pending_open.pop_front() else {
                        return false;
                    };
                    let waiter = match self.state.state_of(slot) {
                        Some(PoolSlotState::Opening { waiter }) => waiter,
                        state => {
                            panic!("slot {slot} was not opening before complete_open: {state:?}")
                        }
                    };
                    self.state
                        .complete_open(slot, true)
                        .expect("complete healthy open");
                    if !self.closing {
                        self.grant_if_waiting(slot, waiter);
                    }
                }
                LifecycleDporOp::OpenUnhealthy => {
                    let Some(slot) = self.pending_open.pop_front() else {
                        return false;
                    };
                    let waiter = match self.state.state_of(slot) {
                        Some(PoolSlotState::Opening { waiter }) => waiter,
                        state => {
                            panic!("slot {slot} was not opening before failed open: {state:?}")
                        }
                    };
                    self.state
                        .complete_open(slot, false)
                        .expect("complete failed open");
                    if !self.closing {
                        self.promote_front_waiter_if_present(waiter);
                    }
                }
                LifecycleDporOp::PingHealthy => {
                    let Some(slot) = self.pending_ping.pop_front() else {
                        return false;
                    };
                    let waiter = match self.state.state_of(slot) {
                        Some(PoolSlotState::Validating { waiter }) => waiter,
                        state => {
                            panic!("slot {slot} was not validating before complete_ping: {state:?}")
                        }
                    };
                    self.state
                        .complete_ping(slot, true)
                        .expect("complete healthy ping");
                    if !self.closing {
                        self.grant_if_waiting(slot, waiter);
                    }
                }
                LifecycleDporOp::PingUnhealthy => {
                    let Some(slot) = self.pending_ping.pop_front() else {
                        return false;
                    };
                    let waiter = match self.state.state_of(slot) {
                        Some(PoolSlotState::Validating { waiter }) => waiter,
                        state => {
                            panic!("slot {slot} was not validating before failed ping: {state:?}")
                        }
                    };
                    self.state
                        .complete_ping(slot, false)
                        .expect("complete failed ping");
                    if !self.closing {
                        self.promote_front_waiter_if_present(waiter);
                    }
                }
                LifecycleDporOp::ReleaseOldest => {
                    let Some((waiter, slot)) = self
                        .checked_out
                        .iter()
                        .next()
                        .map(|(waiter, slot)| (*waiter, *slot))
                    else {
                        return false;
                    };
                    self.checked_out.remove(&waiter);
                    self.state.release(slot).expect("release checked-out slot");
                }
                LifecycleDporOp::ForceClose => {
                    if self.closing {
                        return false;
                    }
                    self.state.begin_close(true).expect("force-close lifecycle");
                    self.fifo_waiters.clear();
                    self.checked_out.clear();
                    self.closing = true;
                }
                LifecycleDporOp::CloseDone => {
                    let Some(slot) = self.pending_close.pop_front() else {
                        return false;
                    };
                    self.state
                        .complete_close(slot)
                        .expect("complete pending close");
                }
            }
            self.drain_effects();
            self.assert_invariants();
            true
        }

        fn grant_if_waiting(&mut self, slot: SlotId, waiter: Option<WaiterId>) {
            if let Some(waiter) = waiter {
                let prior_len = self.fifo_waiters.len();
                self.fifo_waiters.retain(|queued| *queued != waiter);
                assert!(
                    self.fifo_waiters.len() < prior_len || self.closing,
                    "slot {slot} granted waiter {waiter} outside the tracked waiter set"
                );
                assert!(
                    self.checked_out.insert(waiter, slot).is_none(),
                    "waiter {waiter} was handed out twice"
                );
            }
        }

        fn promote_front_waiter_if_present(&mut self, waiter: Option<WaiterId>) {
            if let Some(waiter) = waiter {
                self.fifo_waiters.retain(|queued| *queued != waiter);
                self.fifo_waiters.push_front(waiter);
                assert_eq!(
                    self.fifo_waiters.front(),
                    Some(&waiter),
                    "failed open/ping must requeue the waiter at the front"
                );
            }
        }

        fn drain_effects(&mut self) {
            for effect in self.state.take_effects() {
                match effect {
                    PoolEffect::Open { slot, waiter } => {
                        if !self.closing {
                            self.promote_front_waiter_if_present(waiter);
                        }
                        self.pending_open.push_back(slot);
                    }
                    PoolEffect::Ping { slot, waiter } => {
                        if !self.closing {
                            self.promote_front_waiter_if_present(Some(waiter));
                        }
                        self.pending_ping.push_back(slot);
                    }
                    PoolEffect::Close { slot, .. } => self.pending_close.push_back(slot),
                }
            }
        }

        fn assert_invariants(&self) {
            let counts = self.state.counts();
            let live_slots = counts
                .opening
                .saturating_add(counts.idle)
                .saturating_add(counts.checked_out)
                .saturating_add(counts.validating)
                .saturating_add(counts.retiring)
                .saturating_add(counts.closing);
            assert!(live_slots <= 2, "pure lifecycle exceeded max live slots");
            assert_eq!(
                counts.checked_out,
                self.checked_out.len(),
                "tracked checked-out grants diverged from lifecycle state"
            );
            if !self.closing {
                assert!(
                    counts.idle == 0 || counts.waiters == 0,
                    "idle slot coexisted with a queued waiter: missed wakeup"
                );
            } else {
                assert_eq!(counts.waiters, 0, "force close must drain waiters");
                assert!(
                    self.fifo_waiters.is_empty(),
                    "force close left FIFO waiters"
                );
            }

            let mut effect_slots = BTreeSet::new();
            for slot in self
                .pending_open
                .iter()
                .chain(self.pending_ping.iter())
                .chain(self.pending_close.iter())
            {
                assert!(
                    effect_slots.insert(*slot),
                    "slot {slot} has duplicate pending lifecycle effects"
                );
            }
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum LifecycleDporOp {
        Acquire,
        CancelOldest,
        OpenHealthy,
        OpenUnhealthy,
        PingHealthy,
        PingUnhealthy,
        ReleaseOldest,
        ForceClose,
        CloseDone,
    }

    const LIFECYCLE_DPOR_OPS: &[LifecycleDporOp] = &[
        LifecycleDporOp::Acquire,
        LifecycleDporOp::CancelOldest,
        LifecycleDporOp::OpenHealthy,
        LifecycleDporOp::OpenUnhealthy,
        LifecycleDporOp::PingHealthy,
        LifecycleDporOp::PingUnhealthy,
        LifecycleDporOp::ReleaseOldest,
        LifecycleDporOp::ForceClose,
        LifecycleDporOp::CloseDone,
    ];

    fn enumerate_lifecycle_dpor(model: LifecycleDporModel, depth: usize, leaves: &mut usize) {
        if depth == 0 {
            *leaves = leaves.saturating_add(1);
            return;
        }

        let mut progressed = false;
        for op in LIFECYCLE_DPOR_OPS {
            let mut next = model.clone();
            if next.apply(*op) {
                progressed = true;
                enumerate_lifecycle_dpor(next, depth - 1, leaves);
            }
        }

        if !progressed {
            *leaves = leaves.saturating_add(1);
        }
    }

    #[test]
    fn dpor_pool_lifecycle_sequences_exhaust_structural_invariants() {
        let model = LifecycleDporModel::new();
        model.assert_invariants();
        let mut leaves = 0;
        enumerate_lifecycle_dpor(model, DPOR_POOL_LIFECYCLE_DEPTH, &mut leaves);
        eprintln!(
            "[dpor-pool-lifecycle] exhaustive_depth={} terminal_sequences={}",
            DPOR_POOL_LIFECYCLE_DEPTH, leaves
        );
        assert!(
            leaves > 1_000,
            "lifecycle DPOR bound was not saturated enough"
        );
    }

    #[derive(Clone, Copy, Debug)]
    enum PoolDporMode {
        ReleaseClose,
        ImmediateTimedExpiry,
    }

    fn dpor_pool_seed(mode: PoolDporMode) -> u64 {
        DPOR_POOL_SEED
            + match mode {
                PoolDporMode::ReleaseClose => 0,
                PoolDporMode::ImmediateTimedExpiry => 1,
            }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum PoolDporEvent {
        Acquired(u64),
        AcquireQueued,
        TimedOut,
        AcquireClosed,
        ReleaseReturned,
        ReleaseIgnored,
        CloseForced,
    }

    fn make_dpor_pool_inner() -> (Arc<EngineInner<Arc<FakeBackend>>>, Arc<FakeBackend>, u64) {
        let backend = Arc::new(FakeBackend::new());
        let held = 1;
        let (drop_returns_tx, drop_returns_rx) = mpsc::channel();
        let now = Instant::now();
        let state = PoolState {
            open: true,
            config: test_config(0, 1, 1, POOL_GETMODE_WAIT),
            force_get: false,
            wait_timeout_ms: None,
            free_new: Vec::new(),
            free_used: Vec::new(),
            busy: vec![PooledConn {
                id: held,
                conn: FakeConn {
                    alive: Arc::new(AtomicBool::new(true)),
                },
                cclass: None,
                time_created: now,
                time_returned: now,
                is_pool_extra: false,
                ever_acquired: true,
            }],
            to_drop: VecDeque::new(),
            requests: Vec::new(),
            open_effects: VecDeque::new(),
            in_flight_open_effects: VecDeque::new(),
            next_conn_id: 2,
            next_request_id: 1,
        };
        let inner = Arc::new(EngineInner {
            backend: Arc::clone(&backend),
            state: Mutex::new(state),
            drop_returns_tx,
            drop_returns_rx: Mutex::new(drop_returns_rx),
            async_waiters: Notify::new(),
            bg: Arc::new(Notify::new()),
            reaper_stop: AtomicBool::new(false),
            reaper_handle: Mutex::new(None),
        });
        (inner, backend, held)
    }

    async fn dpor_pool_acquire_waiter(
        inner: Arc<EngineInner<Arc<FakeBackend>>>,
        wait_timeout_ms: Option<u32>,
        events: Arc<Mutex<Vec<PoolDporEvent>>>,
    ) {
        if wait_timeout_ms == Some(0) {
            events
                .lock()
                .expect("record timed-out pool acquire")
                .push(PoolDporEvent::TimedOut);
            return;
        }

        let request_id = {
            let mut state = lock_state(&inner).expect("lock DPOR pool state for acquire");
            match enqueue_request(&mut state, AcquireOptions::default()) {
                Ok(request_id) => request_id,
                Err(PoolError::Closed) => {
                    events
                        .lock()
                        .expect("record closed acquire")
                        .push(PoolDporEvent::AcquireClosed);
                    return;
                }
                Err(err) => panic!("unexpected DPOR pool enqueue error: {err:?}"),
            }
        };
        let result = {
            let mut state = lock_state(&inner).expect("poll DPOR pool acquire once");
            poll_request_completion(&mut state, &inner, request_id)
        };
        match result {
            Ok(Some(conn_id)) => {
                events
                    .lock()
                    .expect("record acquired pool connection")
                    .push(PoolDporEvent::Acquired(conn_id));
            }
            Ok(None) => {
                events
                    .lock()
                    .expect("record queued pool acquire")
                    .push(PoolDporEvent::AcquireQueued);
            }
            Err(PoolError::Closed) => {
                events
                    .lock()
                    .expect("record closed pool acquire")
                    .push(PoolDporEvent::AcquireClosed);
            }
            Err(err) => panic!("unexpected DPOR pool acquire error: {err:?}"),
        }
    }

    async fn dpor_pool_release_held(
        inner: Arc<EngineInner<Arc<FakeBackend>>>,
        held: u64,
        events: Arc<Mutex<Vec<PoolDporEvent>>>,
    ) {
        let cx = Cx::current().expect("LabRuntime task should install an ambient Cx");
        checkpoint_pool(&cx).expect("release checkpoint");
        let released = {
            let mut state = lock_state(&inner).expect("lock DPOR pool state for release");
            if !state.open {
                false
            } else if let Some(position) = state.busy.iter().position(|conn| conn.id == held) {
                let conn = state.busy.remove(position);
                let is_open = inner.backend.connection_is_open(&conn.conn);
                return_connection_helper(&mut state, &inner, conn, is_open);
                true
            } else {
                false
            }
        };
        wake_waiters(&inner);
        events
            .lock()
            .expect("record DPOR pool release")
            .push(if released {
                PoolDporEvent::ReleaseReturned
            } else {
                PoolDporEvent::ReleaseIgnored
            });
    }

    async fn dpor_pool_force_close(
        inner: Arc<EngineInner<Arc<FakeBackend>>>,
        events: Arc<Mutex<Vec<PoolDporEvent>>>,
    ) {
        let cx = Cx::current().expect("LabRuntime task should install an ambient Cx");
        checkpoint_pool(&cx).expect("close checkpoint");
        let (leftovers, drained_waiters) = {
            let mut state = lock_state(&inner).expect("lock DPOR pool state for close");
            let drained_waiters = state.requests.iter().any(|request| request.waiting);
            request_worker_shutdown_locked(&mut state, &inner);
            (state.to_drop.drain(..).collect::<Vec<_>>(), drained_waiters)
        };
        for conn in leftovers {
            inner.backend.close_connection(conn.id, conn.conn);
        }
        wake_waiters(&inner);
        let mut events = events.lock().expect("record DPOR pool close");
        if drained_waiters {
            events.push(PoolDporEvent::AcquireClosed);
        }
        events.push(PoolDporEvent::CloseForced);
    }

    fn assert_dpor_pool_async_invariants(
        mode: PoolDporMode,
        inner: &EngineInner<Arc<FakeBackend>>,
        backend: &FakeBackend,
        events: &[PoolDporEvent],
    ) {
        let acquire_terminals = events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    PoolDporEvent::Acquired(_)
                        | PoolDporEvent::TimedOut
                        | PoolDporEvent::AcquireClosed
                )
            })
            .count();
        assert_eq!(
            acquire_terminals, 1,
            "each DPOR pool schedule must produce one acquire terminal: {events:?}"
        );

        let acquired = events
            .iter()
            .filter_map(|event| match event {
                PoolDporEvent::Acquired(conn_id) => Some(*conn_id),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            acquired.iter().all(|conn_id| *conn_id == 1),
            "pool handed out an unexpected connection id: {events:?}"
        );
        assert!(
            acquired.len() <= 1,
            "pool handed out the same slot more than once: {events:?}"
        );

        let state = inner.state.lock().expect("inspect DPOR pool state");
        let mut live_ids = BTreeSet::new();
        for conn in state
            .free_new
            .iter()
            .chain(state.free_used.iter())
            .chain(state.busy.iter())
            .chain(state.to_drop.iter())
        {
            assert!(
                live_ids.insert(conn.id),
                "connection id {} appears in more than one pool list",
                conn.id
            );
        }
        assert!(state.busy.len() <= 1, "pool has duplicate busy grants");

        match mode {
            PoolDporMode::ReleaseClose => {
                assert!(
                    events.contains(&PoolDporEvent::CloseForced),
                    "release/close DPOR scenario did not execute close: {events:?}"
                );
                assert!(!state.open, "force-close schedule left pool open");
                assert!(
                    state.requests.iter().all(|request| !request.waiting),
                    "force-close schedule left a waiting acquire request"
                );
                assert!(
                    state.free_new.is_empty()
                        && state.free_used.is_empty()
                        && state.busy.is_empty()
                        && state.to_drop.is_empty(),
                    "force-close schedule left live pool lists populated"
                );
                assert_eq!(
                    backend.closed.load(Ordering::SeqCst),
                    1,
                    "force-close must close the single physical connection exactly once"
                );
                assert!(
                    !events.contains(&PoolDporEvent::TimedOut),
                    "release/close scenario must not report timeout: {events:?}"
                );
            }
            PoolDporMode::ImmediateTimedExpiry => {
                assert_eq!(
                    events,
                    &[PoolDporEvent::TimedOut],
                    "immediate timed-wait expiry must map to DPY-4005"
                );
                assert!(state.open, "timed-wait expiry must leave the pool open");
                assert_eq!(
                    state.requests.len(),
                    0,
                    "timed-out acquire must abandon its waiter"
                );
                assert_eq!(
                    state.busy.len(),
                    1,
                    "timed-out acquire must not steal the caller's held slot"
                );
            }
        }
    }

    fn explore_dpor_pool_mode(mode: PoolDporMode) -> asupersync::lab::ExplorationReport {
        // Full acquire_wait_future/Notify polling inside LabRuntime did not
        // quiesce: the production async waiter can remain parked waiting for a
        // wake managed outside the finite DPOR task body. Keep DPOR on finite
        // enqueue/release/close ordering, and keep the real timed-wait future
        // covered by async_acquire_timedwait_honors_deadline.
        let mut explorer = DporExplorer::new(
            ExplorerConfig::new(dpor_pool_seed(mode), DPOR_POOL_MAX_ITERS).max_steps(100_000),
        );
        explorer.explore(|runtime: &mut LabRuntime| {
            let (inner, backend, held) = make_dpor_pool_inner();
            let events = Arc::new(Mutex::new(Vec::new()));
            let root = runtime.state.create_root_region(Budget::INFINITE);

            let acquire_inner = Arc::clone(&inner);
            let acquire_events = Arc::clone(&events);
            let wait_timeout_ms = match mode {
                PoolDporMode::ReleaseClose => None,
                PoolDporMode::ImmediateTimedExpiry => Some(0),
            };
            let (acquire, _acquire_handle) = runtime
                .state
                .create_task(root, Budget::INFINITE, async move {
                    dpor_pool_acquire_waiter(acquire_inner, wait_timeout_ms, acquire_events).await;
                })
                .expect("create DPOR pool acquire task");
            runtime.scheduler.lock().schedule(acquire, 0);

            if matches!(mode, PoolDporMode::ReleaseClose) {
                let release_inner = Arc::clone(&inner);
                let release_events = Arc::clone(&events);
                let (release, _release_handle) = runtime
                    .state
                    .create_task(root, Budget::INFINITE, async move {
                        dpor_pool_release_held(release_inner, held, release_events).await;
                    })
                    .expect("create DPOR pool release task");
                let close_inner = Arc::clone(&inner);
                let close_events = Arc::clone(&events);
                let (close, _close_handle) = runtime
                    .state
                    .create_task(root, Budget::INFINITE, async move {
                        dpor_pool_force_close(close_inner, close_events).await;
                    })
                    .expect("create DPOR pool close task");
                let mut scheduler = runtime.scheduler.lock();
                scheduler.schedule(release, 0);
                scheduler.schedule(close, 0);
            }

            runtime.run_until_quiescent();
            assert!(
                runtime.is_quiescent(),
                "DPOR pool ordering model did not quiesce"
            );
            let events = events.lock().expect("read DPOR pool events").clone();
            assert_dpor_pool_async_invariants(mode, &inner, &backend, &events);
        })
    }

    #[test]
    fn dpor_pool_async_waiter_release_close_and_timeout_saturate() {
        for mode in [
            PoolDporMode::ReleaseClose,
            PoolDporMode::ImmediateTimedExpiry,
        ] {
            let report = explore_dpor_pool_mode(mode);
            eprintln!(
                "[dpor-pool-async] mode={mode:?} seed={} max_iters={} runs={} classes={} saturated={}",
                dpor_pool_seed(mode),
                DPOR_POOL_MAX_ITERS,
                report.total_runs,
                report.unique_classes,
                report.coverage.is_saturated(DPOR_POOL_SATURATION_WINDOW)
            );
            assert!(
                !report.has_violations(),
                "DPOR pool {mode:?} found violations at seeds {:?}",
                report.violation_seeds()
            );
            assert!(
                report.total_runs == DPOR_POOL_MAX_ITERS,
                "DPOR pool fallback seed space did not complete for {mode:?}: runs={}, classes={}, new={}",
                report.total_runs,
                report.unique_classes,
                report.coverage.new_class_discoveries
            );
        }
    }

    struct FakeConn {
        alive: Arc<AtomicBool>,
    }

    struct FakeBackend {
        created: AtomicU64,
        closed: AtomicU64,
        fail_creation: AtomicBool,
    }

    impl FakeBackend {
        fn new() -> Self {
            Self {
                created: AtomicU64::new(0),
                closed: AtomicU64::new(0),
                fail_creation: AtomicBool::new(false),
            }
        }
    }

    struct BlockingCreateBackend {
        created: AtomicU64,
        closed: AtomicU64,
        entered_create: AtomicBool,
        allow_create: Mutex<bool>,
        create_ready: Condvar,
    }

    impl BlockingCreateBackend {
        fn new() -> Self {
            Self {
                created: AtomicU64::new(0),
                closed: AtomicU64::new(0),
                entered_create: AtomicBool::new(false),
                allow_create: Mutex::new(false),
                create_ready: Condvar::new(),
            }
        }

        fn wait_for_create_started(&self) {
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                if self.entered_create.load(Ordering::SeqCst) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            assert!(
                self.entered_create.load(Ordering::SeqCst),
                "worker never started the in-flight create"
            );
        }

        fn release_create(&self) {
            *self.allow_create.lock().unwrap() = true;
            self.create_ready.notify_all();
        }
    }

    impl PoolBackend for Arc<FakeBackend> {
        type Conn = FakeConn;

        fn create_connection(&self, _id: u64, _cclass: Option<&str>) -> Result<FakeConn, String> {
            if self.fail_creation.load(Ordering::SeqCst) {
                return Err("server returned Oracle error: ORA-01017: bad password".to_string());
            }
            self.created.fetch_add(1, Ordering::SeqCst);
            Ok(FakeConn {
                alive: Arc::new(AtomicBool::new(true)),
            })
        }

        fn ping_connection(&self, conn: &FakeConn, _ping_timeout_ms: u32) -> bool {
            conn.alive.load(Ordering::SeqCst)
        }

        fn close_connection(&self, _id: u64, conn: FakeConn) {
            conn.alive.store(false, Ordering::SeqCst);
            self.closed.fetch_add(1, Ordering::SeqCst);
        }

        fn connection_is_open(&self, conn: &FakeConn) -> bool {
            conn.alive.load(Ordering::SeqCst)
        }
    }

    impl PoolBackend for Arc<BlockingCreateBackend> {
        type Conn = FakeConn;

        fn create_connection(&self, _id: u64, _cclass: Option<&str>) -> Result<FakeConn, String> {
            self.entered_create.store(true, Ordering::SeqCst);
            let mut allow_create = self.allow_create.lock().unwrap();
            while !*allow_create {
                allow_create = self.create_ready.wait(allow_create).unwrap();
            }
            self.created.fetch_add(1, Ordering::SeqCst);
            Ok(FakeConn {
                alive: Arc::new(AtomicBool::new(true)),
            })
        }

        fn ping_connection(&self, conn: &FakeConn, _ping_timeout_ms: u32) -> bool {
            conn.alive.load(Ordering::SeqCst)
        }

        fn close_connection(&self, _id: u64, conn: FakeConn) {
            conn.alive.store(false, Ordering::SeqCst);
            self.closed.fetch_add(1, Ordering::SeqCst);
        }

        fn connection_is_open(&self, conn: &FakeConn) -> bool {
            conn.alive.load(Ordering::SeqCst)
        }
    }

    fn test_config(min: u32, max: u32, increment: u32, getmode: u32) -> PoolConfig {
        PoolConfig::new(min, max, increment)
            .with_getmode(getmode)
            .with_wait_timeout_ms(1_000)
            .with_ping_interval_secs(-1)
            .with_ping_timeout_ms(5_000)
    }

    fn wait_for_open_count<B: PoolBackend>(engine: &PoolEngine<B>, expected: u32) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if engine.open_count().unwrap() == expected {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            engine.open_count().unwrap(),
            expected,
            "open count never reached {expected}"
        );
    }

    fn wait_for_closed_count(backend: &FakeBackend, expected: u64) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if backend.closed.load(Ordering::SeqCst) == expected {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            backend.closed.load(Ordering::SeqCst),
            expected,
            "closed count never reached {expected}"
        );
    }

    fn wait_for_worker_exit<B: PoolBackend>(weak: &std::sync::Weak<EngineInner<B>>) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if weak.upgrade().is_none() {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            weak.upgrade().is_none(),
            "pool worker kept EngineInner alive after the last external handle was dropped"
        );
    }

    fn wait_for_pool_closed_flag<B: PoolBackend>(engine: &PoolEngine<B>) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if let Ok(state) = engine.inner.state.try_lock() {
                if !state.open {
                    return;
                }
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let state = engine.inner.state.lock().unwrap();
        assert!(
            !state.open,
            "close could not mark the pool closed while backend create was in flight"
        );
    }

    fn pool_counts<B: PoolBackend>(engine: &PoolEngine<B>) -> PoolCounts {
        let state = engine.inner.state.lock().unwrap();
        derived_lifecycle_counts(&state)
    }

    #[test]
    fn grows_to_min_and_reuses_lifo() {
        let backend = Arc::new(FakeBackend::new());
        let engine = PoolEngine::start(
            Arc::clone(&backend),
            test_config(2, 4, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();
        wait_for_open_count(&engine, 2);
        assert_eq!(
            pool_counts(&engine),
            PoolCounts {
                idle: 2,
                ..PoolCounts::default()
            },
            "production counts must be derived from idle payload state after min growth"
        );
        let first = engine.acquire(AcquireOptions::default()).unwrap();
        assert_eq!(engine.busy_count().unwrap(), 1);
        assert_eq!(
            pool_counts(&engine),
            PoolCounts {
                idle: 1,
                checked_out: 1,
                ..PoolCounts::default()
            },
            "public busy count must mirror derived checked-out lifecycle state"
        );
        engine.return_connection(first).unwrap();
        assert_eq!(engine.busy_count().unwrap(), 0);
        assert_eq!(
            pool_counts(&engine),
            PoolCounts {
                idle: 2,
                ..PoolCounts::default()
            },
            "returning a connection must restore derived idle state without a stored counter"
        );
        let second = engine.acquire(AcquireOptions::default()).unwrap();
        assert_eq!(second, first, "expected LIFO reuse of returned connection");
        engine.return_connection(second).unwrap();
        engine.close(false).unwrap();
        assert_eq!(backend.closed.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn double_release_returns_typed_error_and_leaves_pool_intact() {
        // Upstream #596-adjacent (#4b5aeb23d602): a double-release must surface a
        // typed error (reference DPY-1001 / ERR_NOT_CONNECTED), NOT a silent Ok,
        // and must not corrupt pool state.
        let backend = Arc::new(FakeBackend::new());
        let engine = PoolEngine::start(
            Arc::clone(&backend),
            test_config(1, 2, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();
        wait_for_open_count(&engine, 1);

        let conn = engine.acquire(AcquireOptions::default()).unwrap();
        assert_eq!(engine.busy_count().unwrap(), 1);

        // First release succeeds.
        engine.return_connection(conn).unwrap();
        assert_eq!(engine.busy_count().unwrap(), 0);

        // Second release of the SAME id is a typed error, not Ok.
        let err = engine
            .return_connection(conn)
            .expect_err("double release must be a typed error");
        assert!(
            matches!(err, PoolError::ConnectionNotAcquired),
            "expected ConnectionNotAcquired, got {err:?}"
        );

        // Releasing a never-acquired id is likewise a typed error.
        let never = conn.wrapping_add(9999);
        assert!(matches!(
            engine.return_connection(never),
            Err(PoolError::ConnectionNotAcquired)
        ));

        // State is intact: the connection is back in the idle set and re-acquire
        // returns it (LIFO), then a normal release + close still balances.
        assert_eq!(engine.busy_count().unwrap(), 0);
        let reacquired = engine.acquire(AcquireOptions::default()).unwrap();
        assert_eq!(reacquired, conn, "the returned conn is still reusable");
        engine.return_connection(reacquired).unwrap();
        engine.close(false).unwrap();
    }

    #[test]
    fn returning_dead_connection_invokes_backend_close() {
        let backend = Arc::new(FakeBackend::new());
        let engine = PoolEngine::start(
            Arc::clone(&backend),
            test_config(1, 1, 1, POOL_GETMODE_WAIT),
        )
        .expect("pool starts");
        wait_for_open_count(&engine, 1);
        let held = engine
            .acquire(AcquireOptions::default())
            .expect("acquire pooled connection");
        {
            let state = engine.inner.state.lock().expect("lock pool state");
            let conn = state
                .busy
                .iter()
                .find(|conn| conn.id == held)
                .expect("held connection remains busy");
            conn.conn.alive.store(false, Ordering::SeqCst);
        }

        engine
            .return_connection(held)
            .expect("return dead connection");

        wait_for_closed_count(&backend, 1);
        assert_eq!(engine.busy_count().expect("busy count"), 0);
        wait_for_open_count(&engine, 1);
        engine.close(false).expect("close pool");
    }

    #[test]
    fn blocking_pool_guard_release_disarms_drop() {
        let backend = Arc::new(FakeBackend::new());
        let pool = Pool::start(
            Arc::clone(&backend),
            test_config(1, 1, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();
        wait_for_open_count(&pool.engine, 1);
        let blocking = pool.blocking();

        let first = blocking.acquire(AcquireOptions::default()).unwrap();
        let first_id = first.id();
        assert_eq!(blocking.busy_count().unwrap(), 1);
        let stats = blocking.stats().unwrap();
        assert_eq!(stats.open_count(), 1);
        assert_eq!(stats.busy_count(), 1);
        assert_eq!(stats.idle_count(), 0);
        first.release().unwrap();
        assert_eq!(
            blocking.busy_count().unwrap(),
            0,
            "explicit release must consume the guard without leaving a drop-return event"
        );

        let second = blocking.acquire(AcquireOptions::default()).unwrap();
        assert_eq!(second.id(), first_id);
        second.release().unwrap();
        blocking.close(false).unwrap();
        assert_eq!(backend.closed.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn dropping_pool_guard_returns_connection_for_next_acquire() {
        let backend = Arc::new(FakeBackend::new());
        let pool = Pool::start(
            Arc::clone(&backend),
            test_config(1, 1, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();
        wait_for_open_count(&pool.engine, 1);
        let blocking = pool.blocking();

        let first = blocking.acquire(AcquireOptions::default()).unwrap();
        let first_id = first.id();
        assert_eq!(blocking.busy_count().unwrap(), 1);
        drop(first);
        blocking.drain().unwrap();
        assert_eq!(
            blocking.busy_count().unwrap(),
            0,
            "explicit drain must reconcile queued guard return events"
        );

        let second = blocking.acquire(AcquireOptions::default()).unwrap();
        assert_eq!(
            second.id(),
            first_id,
            "acquire must reconcile the guard drop-return event before waiting"
        );
        assert_eq!(
            blocking.busy_count().unwrap(),
            1,
            "reacquired guard must be the only busy connection"
        );
        second.release().unwrap();
        blocking.close(false).unwrap();
        assert_eq!(backend.closed.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn cancelled_async_guard_release_falls_back_to_drop_return() {
        let backend = Arc::new(FakeBackend::new());
        let pool = Pool::start(
            Arc::clone(&backend),
            test_config(1, 1, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();
        wait_for_open_count(&pool.engine, 1);
        let blocking = pool.blocking();
        let async_pool = pool.clone();

        let runtime = crate::build_io_runtime().expect("asupersync runtime");
        let (conn_id, err) = runtime.block_on(async {
            let cx = Cx::current().expect("asupersync installs an ambient Cx");
            let conn = async_pool
                .acquire(&cx, AcquireOptions::default())
                .await
                .unwrap();
            let conn_id = conn.id();
            cx.cancel_fast(asupersync::CancelKind::Shutdown);
            (conn_id, conn.release(&cx).await.unwrap_err())
        });

        assert!(matches!(err, PoolError::Cancelled(_)));
        assert_eq!(
            blocking.busy_count().unwrap(),
            0,
            "cancelled release must leave the guard armed so Drop can enqueue a return"
        );
        let reacquired = blocking.acquire(AcquireOptions::default()).unwrap();
        assert_eq!(reacquired.id(), conn_id);
        reacquired.release().unwrap();
        blocking.close(false).unwrap();
    }

    #[test]
    fn close_reconciles_dropped_guard_before_busy_check() {
        let backend = Arc::new(FakeBackend::new());
        let pool = Pool::start(
            Arc::clone(&backend),
            test_config(1, 1, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();
        wait_for_open_count(&pool.engine, 1);
        let blocking = pool.blocking();

        let conn = blocking.acquire(AcquireOptions::default()).unwrap();
        drop(conn);

        blocking.close(false).unwrap();
        assert_eq!(
            backend.closed.load(Ordering::SeqCst),
            1,
            "non-forced close must observe the queued guard return before rejecting busy state"
        );
    }

    #[test]
    fn sync_acquire_block_on_waits_for_async_returned_connection() {
        let backend = Arc::new(FakeBackend::new());
        let engine = PoolEngine::start(
            Arc::clone(&backend),
            test_config(1, 1, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();
        wait_for_open_count(&engine, 1);
        let held = engine.acquire(AcquireOptions::default()).unwrap();

        let returner_engine = engine.clone();
        let returner = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(25));
            returner_engine.return_connection(held).unwrap();
        });

        let acquired = engine.acquire(AcquireOptions::default()).unwrap();
        returner.join().expect("returner thread");
        assert_eq!(
            acquired, held,
            "sync acquire must block_on the async waiter path and receive the returned connection"
        );
        engine.return_connection(acquired).unwrap();
        engine.close(false).unwrap();
    }

    #[test]
    fn async_close_joins_worker_and_closes_idle_connections() {
        let backend = Arc::new(FakeBackend::new());
        let engine = PoolEngine::start(
            Arc::clone(&backend),
            test_config(1, 1, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();
        wait_for_open_count(&engine, 1);

        let runtime = crate::build_io_runtime().expect("asupersync runtime");
        runtime
            .block_on(async {
                let cx = Cx::current().expect("asupersync installs an ambient Cx");
                engine.close_async(&cx, false).await
            })
            .unwrap();

        assert!(
            engine.inner.reaper_handle.lock().unwrap().is_none(),
            "async close must consume the reaper task handle after joining it"
        );
        assert_eq!(
            backend.closed.load(Ordering::SeqCst),
            1,
            "close must let the reaper close idle connections before returning"
        );
    }

    #[test]
    fn force_close_waits_for_in_flight_create_and_closes_result() {
        let backend = Arc::new(BlockingCreateBackend::new());
        let engine = PoolEngine::start(
            Arc::clone(&backend),
            test_config(0, 1, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();

        let acquire_engine = engine.clone();
        let acquire = std::thread::spawn(move || acquire_engine.acquire(AcquireOptions::default()));
        backend.wait_for_create_started();

        let close_engine = engine.clone();
        let closer = std::thread::spawn(move || close_engine.close(true));
        wait_for_pool_closed_flag(&engine);
        assert_eq!(
            pool_counts(&engine),
            PoolCounts {
                opening: 1,
                ..PoolCounts::default()
            },
            "force close must retain the in-flight open as a derived opening effect"
        );
        assert_eq!(
            backend.closed.load(Ordering::SeqCst),
            0,
            "close must wait for the in-flight create result before returning"
        );

        backend.release_create();
        closer.join().expect("close thread").unwrap();
        assert!(matches!(
            acquire.join().expect("acquire thread"),
            Err(PoolError::Closed)
        ));
        assert_eq!(
            backend.closed.load(Ordering::SeqCst),
            1,
            "created connection must be rejected into the close queue"
        );
        assert_eq!(
            pool_counts(&engine),
            PoolCounts::default(),
            "joined close must drain request lifecycle metadata and close effects"
        );
    }

    #[test]
    fn dropping_last_pool_handle_stops_worker_without_joining() {
        let backend = Arc::new(FakeBackend::new());
        let engine = PoolEngine::start(
            Arc::clone(&backend),
            test_config(1, 1, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();
        wait_for_open_count(&engine, 1);
        let weak = Arc::downgrade(&engine.inner);

        drop(engine);

        wait_for_worker_exit(&weak);
        assert_eq!(
            backend.closed.load(Ordering::SeqCst),
            1,
            "dropping the last pool handle must request forced worker shutdown"
        );
    }

    /// DK1: concurrent last-handle drops must close transports exactly once.
    /// Handle accounting is via AtomicUsize, not Arc::strong_count(runtime).
    #[test]
    fn racing_pool_handle_drops_close_exactly_once() {
        let backend = Arc::new(FakeBackend::new());
        let engine = PoolEngine::start(
            Arc::clone(&backend),
            test_config(1, 1, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();
        wait_for_open_count(&engine, 1);
        let weak = Arc::downgrade(&engine.inner);

        const N: usize = 16;
        let barrier = Arc::new(std::sync::Barrier::new(N));
        let mut join_handles = Vec::with_capacity(N);
        for _ in 0..N {
            let clone = engine.clone();
            let barrier = Arc::clone(&barrier);
            join_handles.push(std::thread::spawn(move || {
                barrier.wait();
                drop(clone);
            }));
        }
        // Drop the original after clones exist so only the racing threads can win.
        drop(engine);
        for handle in join_handles {
            handle.join().expect("racing drop thread");
        }

        wait_for_worker_exit(&weak);
        assert_eq!(
            backend.closed.load(Ordering::SeqCst),
            1,
            "racing PoolEngine drops must close each pooled connection exactly once"
        );
    }

    fn process_thread_count() -> usize {
        std::fs::read_dir(format!("/proc/{}/task", std::process::id()))
            .map(|entries| entries.count())
            .unwrap_or(0)
    }

    /// DK2: N concurrent TIMEDWAIT acquires must not spawn N OS timer threads.
    /// Timeouts ride asupersync's ambient timer driver / shared fallback pump.
    #[test]
    fn timedwait_acquires_do_not_spawn_one_os_thread_each() {
        let backend = Arc::new(FakeBackend::new());
        let config = test_config(1, 1, 1, POOL_GETMODE_TIMEDWAIT).with_wait_timeout_ms(200);
        let engine = PoolEngine::start(Arc::clone(&backend), config).unwrap();
        wait_for_open_count(&engine, 1);
        let held = engine.acquire(AcquireOptions::default()).unwrap();

        // Warm the thread-local I/O runtime so baseline includes its worker.
        let _ = engine.stats();
        let runtime = crate::build_io_runtime().expect("asupersync runtime");

        const WAITERS: usize = 8;
        let baseline = process_thread_count();
        let peak = Arc::new(AtomicUsize::new(baseline));
        let peak_for_sampler = Arc::clone(&peak);
        let sampling = Arc::new(AtomicBool::new(true));
        let sampling_flag = Arc::clone(&sampling);
        let sampler = std::thread::spawn(move || {
            while sampling_flag.load(Ordering::Acquire) {
                let now = process_thread_count();
                peak_for_sampler.fetch_max(now, Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(5));
            }
        });

        // Drive N concurrent timed acquires on ONE async runtime so the test
        // harness itself does not contribute N OS threads. Old code spawned one
        // park_timeout thread per waiter on top of that.
        let results = runtime.block_on(async {
            let cx = Cx::current().expect("asupersync installs an ambient Cx");
            let e0 = engine.clone();
            let e1 = engine.clone();
            let e2 = engine.clone();
            let e3 = engine.clone();
            let e4 = engine.clone();
            let e5 = engine.clone();
            let e6 = engine.clone();
            let e7 = engine.clone();
            let c0 = cx.clone();
            let c1 = cx.clone();
            let c2 = cx.clone();
            let c3 = cx.clone();
            let c4 = cx.clone();
            let c5 = cx.clone();
            let c6 = cx.clone();
            let c7 = cx.clone();
            asupersync::join!(
                e0.acquire_async(&c0, AcquireOptions::default()),
                e1.acquire_async(&c1, AcquireOptions::default()),
                e2.acquire_async(&c2, AcquireOptions::default()),
                e3.acquire_async(&c3, AcquireOptions::default()),
                e4.acquire_async(&c4, AcquireOptions::default()),
                e5.acquire_async(&c5, AcquireOptions::default()),
                e6.acquire_async(&c6, AcquireOptions::default()),
                e7.acquire_async(&c7, AcquireOptions::default()),
            )
        });

        sampling.store(false, Ordering::Release);
        sampler.join().expect("sampler thread");

        let timed_out = [
            results.0, results.1, results.2, results.3, results.4, results.5, results.6, results.7,
        ]
        .into_iter()
        .filter(|r| matches!(r, Err(PoolError::NoConnectionAvailable)))
        .count();
        assert_eq!(
            timed_out, WAITERS,
            "every concurrent TIMEDWAIT acquire must time out while the slot is held"
        );

        let observed_peak = peak.load(Ordering::Relaxed);
        let growth = observed_peak.saturating_sub(baseline);
        // Sampler (+1) and at most the process-shared asupersync fallback pump
        // (+1) are allowed. Reject the old N-threads-per-N-waiters shape.
        assert!(
            growth < WAITERS,
            "TIMEDWAIT must not spawn one OS thread per waiter: baseline={baseline} peak={observed_peak} growth={growth} waiters={WAITERS}"
        );

        engine.return_connection(held).unwrap();
        engine.close(false).unwrap();
    }

    #[test]
    fn nowait_raises_when_full_and_forceget_exceeds_max() {
        let backend = Arc::new(FakeBackend::new());
        let engine = PoolEngine::start(
            Arc::clone(&backend),
            test_config(1, 2, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();
        let a = engine.acquire(AcquireOptions::default()).unwrap();
        let b = engine.acquire(AcquireOptions::default()).unwrap();
        assert_eq!(engine.open_count().unwrap(), 2);
        engine.set_getmode(POOL_GETMODE_NOWAIT).unwrap();
        assert!(matches!(
            engine.acquire(AcquireOptions::default()),
            Err(PoolError::NoConnectionAvailable)
        ));
        engine.set_getmode(POOL_GETMODE_FORCEGET).unwrap();
        let c = engine.acquire(AcquireOptions::default()).unwrap();
        assert_eq!(engine.open_count().unwrap(), 3);
        assert_eq!(engine.busy_count().unwrap(), 3);
        engine.return_connection(c).unwrap();
        // Extra connection beyond max is discarded on return.
        wait_for_open_count(&engine, 2);
        engine.return_connection(a).unwrap();
        engine.return_connection(b).unwrap();
        engine.close(false).unwrap();
    }

    #[test]
    fn cclass_mismatch_uses_dedicated_opening_without_pool_growth_leak() {
        let backend = Arc::new(FakeBackend::new());
        let config = test_config(0, 2, 1, POOL_GETMODE_WAIT).with_creation_cclass("pool");
        let engine = PoolEngine::start(Arc::clone(&backend), config).unwrap();

        let custom = engine
            .acquire(AcquireOptions::new().with_cclass("custom"))
            .unwrap();

        assert_eq!(engine.open_count().unwrap(), 1);
        assert_eq!(
            pool_counts(&engine),
            PoolCounts {
                checked_out: 1,
                ..PoolCounts::default()
            },
            "custom cclass request creates must be represented as checked-out payload state"
        );
        engine.return_connection(custom).unwrap();
        assert_eq!(
            pool_counts(&engine),
            PoolCounts {
                idle: 1,
                ..PoolCounts::default()
            },
            "dedicated cclass create must not leave queued pool-growth effects behind"
        );

        let reused = engine
            .acquire(AcquireOptions::new().with_cclass("custom"))
            .unwrap();
        assert_eq!(reused, custom);
        engine.return_connection(reused).unwrap();
        engine.close(false).unwrap();
    }

    #[test]
    fn close_with_busy_requires_force() {
        let backend = Arc::new(FakeBackend::new());
        let engine = PoolEngine::start(
            Arc::clone(&backend),
            test_config(1, 2, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();
        let _conn = engine.acquire(AcquireOptions::default()).unwrap();
        assert!(matches!(
            engine.close(false),
            Err(PoolError::HasBusyConnections)
        ));
        engine.close(true).unwrap();
        assert!(matches!(
            engine.acquire(AcquireOptions::default()),
            Err(PoolError::Closed)
        ));
    }

    #[test]
    fn creation_errors_surface_on_acquire() {
        let backend = Arc::new(FakeBackend::new());
        backend.fail_creation.store(true, Ordering::SeqCst);
        let engine = PoolEngine::start(
            Arc::clone(&backend),
            test_config(1, 2, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();
        let error = engine.acquire(AcquireOptions::default()).err();
        assert!(matches!(
            error,
            Some(PoolError::Backend(message)) if message.contains("ORA-01017")
        ));
    }

    #[test]
    fn purity_new_replaces_used_connection_when_full() {
        let backend = Arc::new(FakeBackend::new());
        let engine = PoolEngine::start(
            Arc::clone(&backend),
            test_config(1, 2, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();
        let a = engine.acquire(AcquireOptions::default()).unwrap();
        let b = engine.acquire(AcquireOptions::default()).unwrap();
        assert_eq!(engine.open_count().unwrap(), 2);
        engine.return_connection(a).unwrap();
        engine.return_connection(b).unwrap();
        let c = engine
            .acquire(AcquireOptions::new().with_wants_new(true))
            .unwrap();
        assert_ne!(c, a);
        assert_ne!(c, b);
        assert_eq!(engine.open_count().unwrap(), 2, "replacement keeps count");
        engine.return_connection(c).unwrap();
        engine.close(false).unwrap();
    }

    #[test]
    fn async_acquire_waits_on_notification_and_reuses_returned_connection() {
        let backend = Arc::new(FakeBackend::new());
        let engine = PoolEngine::start(
            Arc::clone(&backend),
            test_config(1, 1, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();
        wait_for_open_count(&engine, 1);
        let held = engine.acquire(AcquireOptions::default()).unwrap();

        let returner_engine = engine.clone();
        let returner = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(25));
            returner_engine.return_connection(held).unwrap();
        });

        let runtime = crate::build_io_runtime().expect("asupersync runtime");
        let acquired = runtime
            .block_on(async {
                let cx = Cx::current().expect("asupersync installs an ambient Cx");
                engine.acquire_async(&cx, AcquireOptions::default()).await
            })
            .unwrap();
        returner.join().expect("returner thread");
        assert_eq!(acquired, held);
        engine.return_connection(acquired).unwrap();
        engine.close(false).unwrap();
    }

    #[test]
    fn wait_acquires_beyond_increment_grow_pool_for_each_waiter() {
        let backend = Arc::new(FakeBackend::new());
        let engine = PoolEngine::start(
            Arc::clone(&backend),
            test_config(0, 4, 1, POOL_GETMODE_WAIT),
        )
        .expect("pool engine should start");
        assert_eq!(engine.open_count().expect("open count"), 0);

        let barrier = Arc::new(Barrier::new(4));
        let handles = (0..3)
            .map(|_| {
                let engine = engine.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    engine.acquire(AcquireOptions::default())
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();

        let mut acquired = Vec::new();
        for handle in handles {
            acquired.push(
                handle
                    .join()
                    .expect("acquire thread should join")
                    .expect("WAIT acquire should be served"),
            );
        }
        acquired.sort_unstable();
        acquired.dedup();

        assert_eq!(
            acquired.len(),
            3,
            "each waiter receives a distinct connection"
        );
        assert_eq!(engine.busy_count().expect("busy count"), 3);
        assert_eq!(engine.open_count().expect("open count"), 3);
        assert_eq!(
            backend.created.load(Ordering::SeqCst),
            3,
            "pool grows beyond increment=1 to cover concurrent waiters"
        );

        for conn_id in acquired {
            engine
                .return_connection(conn_id)
                .expect("return acquired connection");
        }
        engine.close(false).expect("close pool");
    }

    #[test]
    fn async_acquire_timedwait_honors_deadline() {
        let backend = Arc::new(FakeBackend::new());
        let config = test_config(1, 1, 1, POOL_GETMODE_TIMEDWAIT).with_wait_timeout_ms(20);
        let engine = PoolEngine::start(Arc::clone(&backend), config).unwrap();
        wait_for_open_count(&engine, 1);
        let held = engine.acquire(AcquireOptions::default()).unwrap();

        let runtime = crate::build_io_runtime().expect("asupersync runtime");
        let started = Instant::now();
        let err = runtime
            .block_on(async {
                let cx = Cx::current().expect("asupersync installs an ambient Cx");
                engine.acquire_async(&cx, AcquireOptions::default()).await
            })
            .unwrap_err();

        assert!(matches!(err, PoolError::NoConnectionAvailable));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "timed async acquire should not park indefinitely"
        );
        engine.return_connection(held).unwrap();
        engine.close(false).unwrap();
    }

    #[test]
    fn async_acquire_checkpoint_cancellation_does_not_enqueue_request() {
        let backend = Arc::new(FakeBackend::new());
        let engine = PoolEngine::start(
            Arc::clone(&backend),
            test_config(1, 1, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();
        wait_for_open_count(&engine, 1);
        let held = engine.acquire(AcquireOptions::default()).unwrap();

        let runtime = crate::build_io_runtime().expect("asupersync runtime");
        let err = runtime
            .block_on(async {
                let cx = Cx::current().expect("asupersync installs an ambient Cx");
                cx.cancel_fast(asupersync::CancelKind::Shutdown);
                engine.acquire_async(&cx, AcquireOptions::default()).await
            })
            .unwrap_err();

        assert!(matches!(err, PoolError::Cancelled(_)));
        assert_eq!(engine.busy_count().unwrap(), 1);
        engine.return_connection(held).unwrap();
        let reacquired = engine.acquire(AcquireOptions::default()).unwrap();
        assert_eq!(
            reacquired, held,
            "cancelled async acquire must not leave a stale waiter ahead of later sync acquires"
        );
        engine.return_connection(reacquired).unwrap();
        engine.close(false).unwrap();
    }

    #[test]
    fn async_acquire_checkpoint_cancellation_while_waiting_abandons_request() {
        let backend = Arc::new(FakeBackend::new());
        let engine = PoolEngine::start(
            Arc::clone(&backend),
            test_config(1, 1, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();
        wait_for_open_count(&engine, 1);
        let held = engine.acquire(AcquireOptions::default()).unwrap();

        let runtime = crate::build_io_runtime().expect("asupersync runtime");
        let returner_engine = engine.clone();
        let err = runtime.block_on(async {
            let cx = Cx::current().expect("asupersync installs an ambient Cx");
            let cancel_cx = cx.clone();
            let returner = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(25));
                cancel_cx.cancel_fast(asupersync::CancelKind::Shutdown);
                returner_engine.return_connection(held).unwrap();
            });
            let err = engine
                .acquire_async(&cx, AcquireOptions::default())
                .await
                .unwrap_err();
            returner.join().expect("returner thread");
            err
        });

        assert!(matches!(err, PoolError::Cancelled(_)));
        assert_eq!(engine.busy_count().unwrap(), 0);
        assert_eq!(
            pool_counts(&engine),
            PoolCounts {
                idle: 1,
                ..PoolCounts::default()
            },
            "cancelled waiter must leave no queued lifecycle metadata"
        );
        let reacquired = engine.acquire(AcquireOptions::default()).unwrap();
        assert_eq!(
            reacquired, held,
            "cancelled waiter must not retain the returned connection or block later acquires"
        );
        engine.return_connection(reacquired).unwrap();
        engine.close(false).unwrap();
    }

    #[test]
    fn async_acquire_dropped_future_abandons_registered_waiter() {
        let backend = Arc::new(FakeBackend::new());
        let engine = PoolEngine::start(
            Arc::clone(&backend),
            test_config(1, 1, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();
        wait_for_open_count(&engine, 1);
        let held = engine.acquire(AcquireOptions::default()).unwrap();

        let runtime = crate::build_io_runtime().expect("asupersync runtime");
        runtime.block_on(async {
            let cx = Cx::current().expect("asupersync installs an ambient Cx");
            let acquire_engine = engine.clone();
            let mut pending =
                Box::pin(acquire_engine.acquire_async(&cx, AcquireOptions::default()));
            poll_fn(|task_cx| {
                let first_poll = pending.as_mut().poll(task_cx);
                assert!(
                    first_poll.is_pending(),
                    "contended async acquire completed before it could be dropped: {first_poll:?}"
                );
                Poll::Ready(())
            })
            .await;
            drop(pending);
        });

        assert_eq!(
            pool_counts(&engine),
            PoolCounts {
                checked_out: 1,
                ..PoolCounts::default()
            },
            "dropping a pending acquire must remove the waiter from derived lifecycle counts"
        );
        assert_eq!(
            engine.busy_count().unwrap(),
            1,
            "dropping a registered acquire must not release the caller's held connection"
        );
        engine.return_connection(held).unwrap();
        let reacquired = engine.acquire(AcquireOptions::default()).unwrap();
        assert_eq!(
            reacquired, held,
            "dropped async waiter must de-register and leave the returned connection reusable"
        );
        engine.return_connection(reacquired).unwrap();
        engine.close(false).unwrap();
    }

    #[test]
    fn async_acquire_dropped_after_handoff_returns_granted_connection() {
        let backend = Arc::new(FakeBackend::new());
        let engine = PoolEngine::start(
            Arc::clone(&backend),
            test_config(1, 1, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();
        wait_for_open_count(&engine, 1);
        let held = engine.acquire(AcquireOptions::default()).unwrap();

        let runtime = crate::build_io_runtime().expect("asupersync runtime");
        runtime.block_on(async {
            let cx = Cx::current().expect("asupersync installs an ambient Cx");
            let acquire_engine = engine.clone();
            let mut pending =
                Box::pin(acquire_engine.acquire_async(&cx, AcquireOptions::default()));
            poll_fn(|task_cx| {
                let first_poll = pending.as_mut().poll(task_cx);
                assert!(
                    first_poll.is_pending(),
                    "contended async acquire completed before it could be handed off: {first_poll:?}"
                );
                Poll::Ready(())
            })
            .await;

            engine.return_connection_async(&cx, held).await.unwrap();
            assert_eq!(
                engine.busy_count_async(&cx).await.unwrap(),
                0,
                "returned connection is granted to the pending request before future drop"
            );
            drop(pending);
        });

        let reacquired = engine.acquire(AcquireOptions::default()).unwrap();
        assert_eq!(
            reacquired, held,
            "dropping after handoff must return the granted connection to the pool"
        );
        assert_eq!(
            engine.busy_count().unwrap(),
            1,
            "the reacquired connection must be the only busy connection"
        );
        engine.return_connection(reacquired).unwrap();
        engine.close(false).unwrap();
    }

    /// W1-T7.4 regression: the idle/expiry reaper is an asupersync task owned by
    /// the pool's dedicated runtime, and `close(&Cx, ...).await` cooperatively
    /// *awaits* (never synchronously OS-thread-joins) it.
    ///
    /// Proof obligations exercised here:
    ///   (a) The reaper makes progress purely as an async task: with no thread of
    ///       our own poking the engine, the pool still grows to `min` and pings
    ///       returned connections — work that only the spawned reaper performs.
    ///   (b) `close_async` completes by joining the reaper cooperatively: while it
    ///       is awaiting the reaper (which is wedged inside a slow create on the
    ///       pool's *own* runtime), a second future on the *caller's* single
    ///       worker thread still interleaves and makes progress. A synchronous
    ///       `JoinHandle::join()` (the old behaviour) would have parked that one
    ///       worker thread and starved the marker future.
    ///   (c) The reaper task handle is consumed by the join, and a second close is
    ///       an idempotent no-op.
    #[test]
    fn reaper_is_async_task_cooperatively_joined_on_close() {
        use std::sync::atomic::AtomicU64 as Counter;

        // (a) Async-task progress with nobody driving the engine by hand.
        let backend = Arc::new(FakeBackend::new());
        let engine = PoolEngine::start(
            Arc::clone(&backend),
            test_config(2, 4, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();
        // Only the spawned async reaper can satisfy this — there is no detached
        // OS-thread worker any more.
        wait_for_open_count(&engine, 2);
        eprintln!("[reaper-test] reaper grew pool to min=2 as an async task");
        assert!(
            engine.inner.reaper_handle.lock().unwrap().is_some(),
            "the reaper must be a live spawned task before close"
        );

        // (b) Cooperative async join. Use a backend that wedges inside create so
        // the reaper is busy on the pool's own runtime while close awaits it.
        let slow = Arc::new(BlockingCreateBackend::new());
        let slow_engine =
            PoolEngine::start(Arc::clone(&slow), test_config(1, 1, 1, POOL_GETMODE_WAIT)).unwrap();
        slow.wait_for_create_started();
        eprintln!("[reaper-test] slow-create reaper is wedged inside create_connection");

        // Release the wedged create slightly later, from an ordinary thread, so
        // the reaper can finish draining and the awaited close can resolve.
        let releaser_slow = Arc::clone(&slow);
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            releaser_slow.release_create();
        });

        let marker = Arc::new(Counter::new(0));
        let marker_for_task = Arc::clone(&marker);
        let close_engine = slow_engine.clone();
        let runtime = crate::build_io_runtime().expect("asupersync runtime");
        let close_result = runtime.block_on(async move {
            let cx = Cx::current().expect("asupersync installs an ambient Cx");
            // A marker future that yields repeatedly. If close synchronously
            // joined an OS thread, this could not advance while close was
            // outstanding because the close future would never return `Pending`.
            let mut ticker = Box::pin(async {
                for _ in 0..200u32 {
                    marker_for_task.fetch_add(1, Ordering::SeqCst);
                    asupersync::time::sleep(asupersync::time::wall_now(), Duration::from_millis(1))
                        .await;
                }
            });
            // Force-close (busy in-flight create) interleaved with the ticker on
            // the SAME task. We poll the close future to completion, polling the
            // ticker on every turn; both must make progress.
            let mut closing = Box::pin(close_engine.close_async(&cx, true));
            poll_fn(move |task_cx| {
                // Always give the ticker a turn; it is cooperative.
                let _ = Future::poll(Pin::as_mut(&mut ticker), task_cx);
                Future::poll(Pin::as_mut(&mut closing), task_cx)
            })
            .await
        });
        close_result.expect("async close must complete");
        releaser.join().expect("releaser thread");
        assert!(
            marker.load(Ordering::SeqCst) > 0,
            "the caller's worker thread kept interleaving while close awaited the reaper \
             — close did not synchronously block on an OS-thread join"
        );
        eprintln!(
            "[reaper-test] caller thread advanced marker={} while close awaited the reaper",
            marker.load(Ordering::SeqCst)
        );
        assert_eq!(
            slow.closed.load(Ordering::SeqCst),
            1,
            "the cooperatively-joined reaper must have closed the wedged connection"
        );

        // (c) Handle consumed; second close is an idempotent no-op.
        assert!(
            slow_engine.inner.reaper_handle.lock().unwrap().is_none(),
            "the reaper task handle must be consumed once close has joined it"
        );
        slow_engine.close(true).expect("second close is idempotent");

        // Tidy the async-progress pool from part (a).
        engine.close(false).unwrap();
    }
}
