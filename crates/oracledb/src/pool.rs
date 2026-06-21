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
use asupersync::{time, Cx};
use std::collections::VecDeque;
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex, Weak};
use std::task::Poll;
use std::time::{Duration, Instant};

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
pub enum PoolError {
    /// Pool is closed (DPY-1002 / ERR_POOL_NOT_OPEN).
    Closed,
    /// No connection available within constraints (DPY-4005).
    NoConnectionAvailable,
    /// Pool has busy connections and close was not forced (DPY-1005).
    HasBusyConnections,
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
                if let Some(waiter) = waiter {
                    self.waiters.push_front(waiter);
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
                if let Some(waiter) = waiter {
                    self.waiters.push_front(waiter);
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
    /// the runtime on the external handles means it is always dropped by the
    /// thread that drops the last `PoolEngine`, never by the worker thread.
    runtime: Arc<Runtime>,
}

impl<B: PoolBackend> Clone for PoolEngine<B> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            runtime: Arc::clone(&self.runtime),
        }
    }
}

impl<B: PoolBackend> Drop for PoolEngine<B> {
    fn drop(&mut self) {
        // Detect the last pool handle via the `runtime` Arc, which is held ONLY
        // by `PoolEngine` clones — the reaper captures a `Weak<EngineInner>` and
        // a `RuntimeHandle`, never the `Arc<Runtime>`, so this count is exactly
        // the number of live pool handles (unperturbed by the reaper transiently
        // upgrading its `Weak` during a work phase). More than one means another
        // handle is still live; do nothing.
        if Arc::strong_count(&self.runtime) > 1 {
            return;
        }
        // Last handle. Close every connection RIGHT HERE, on the dropping thread,
        // WITHOUT awaiting or waking the reaper (R11 — `Drop` never blocks or
        // spawns). Doing the close synchronously here (rather than delegating to
        // the reaper or to `EngineInner::drop`, which could run on the worker
        // thread that the `runtime` field is about to tear down) makes transport
        // release deterministic and race-free: it cannot lose a connection to the
        // worker being force-stopped mid-drain. Then, after this method returns,
        // the `inner` and `runtime` fields drop on THIS thread — the worker is
        // joined from outside itself, never self-joined.
        close_all_connections(&self.inner);
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

struct AsyncAcquireRequest<'a, B: PoolBackend> {
    inner: &'a EngineInner<B>,
    request_id: u64,
    active: bool,
}

impl<'a, B: PoolBackend> AsyncAcquireRequest<'a, B> {
    fn new(inner: &'a EngineInner<B>, request_id: u64) -> Self {
        Self {
            inner,
            request_id,
            active: true,
        }
    }

    fn complete(&mut self) {
        self.active = false;
    }

    fn abandon(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut state) = self.inner.state.lock() {
            abandon_request(&mut state, self.inner, self.request_id);
            wake_waiters(self.inner);
        }
        self.active = false;
    }
}

impl<B: PoolBackend> Drop for AsyncAcquireRequest<'_, B> {
    fn drop(&mut self) {
        self.abandon();
    }
}

fn enqueue_request<C>(state: &mut PoolState<C>, opts: AcquireOptions) -> Result<u64, PoolError> {
    if !state.open {
        return Err(PoolError::Closed);
    }
    let request_id = state.next_request_id;
    state.next_request_id += 1;
    let pool_cclass = state.config.creation_cclass.clone();
    let cclass_matches = opts.cclass.is_none() || opts.cclass.as_deref() == pool_cclass.as_deref();
    state.requests.push(Request {
        id: request_id,
        cclass: opts.cclass,
        cclass_matches,
        wants_new: opts.wants_new,
        requires_ping: false,
        bg_processing: false,
        is_extra: false,
        is_replacing: false,
        in_progress: false,
        completed: false,
        waiting: true,
        conn: None,
        error: None,
    });
    Ok(request_id)
}

fn finish_completed_request<C>(
    state: &mut PoolState<C>,
    request_id: u64,
) -> Result<u64, PoolError> {
    let position = request_position(state, request_id)
        .ok_or_else(|| PoolError::Internal("request lost".to_string()))?;
    let mut request = state.requests.remove(position);
    let Some(mut conn) = request.conn.take() else {
        return Err(PoolError::Internal(
            "completed request without connection".to_string(),
        ));
    };
    conn.ever_acquired = true;
    let conn_id = conn.id;
    state.busy.push(conn);
    Ok(conn_id)
}

fn poll_request_completion<B: PoolBackend>(
    state: &mut PoolState<B::Conn>,
    inner: &EngineInner<B>,
    request_id: u64,
) -> Result<Option<u64>, PoolError> {
    match fulfill(state, inner, request_id) {
        Ok(true) => finish_completed_request(state, request_id).map(Some),
        Ok(false) => Ok(None),
        Err(err) => {
            abandon_request(state, inner, request_id);
            Err(err)
        }
    }
}

fn acquire_wait_future<'a, B: PoolBackend>(
    cx: &'a Cx,
    inner: &'a EngineInner<B>,
    request_id: u64,
) -> impl Future<Output = Result<u64, PoolError>> + 'a {
    let mut notified = None;
    poll_fn(move |task_cx| loop {
        if let Err(err) = checkpoint_pool(cx) {
            return Poll::Ready(Err(err));
        }

        let mut state = match lock_state(inner) {
            Ok(state) => state,
            Err(err) => return Poll::Ready(Err(err)),
        };
        match poll_request_completion(&mut state, inner, request_id) {
            Ok(Some(conn_id)) => return Poll::Ready(Ok(conn_id)),
            Ok(None) => {}
            Err(err) => return Poll::Ready(Err(err)),
        }

        let waiter = notified.get_or_insert_with(|| Box::pin(inner.async_waiters.notified()));
        match Future::poll(Pin::as_mut(waiter), task_cx) {
            Poll::Ready(()) => {
                notified = None;
            }
            Poll::Pending => return Poll::Pending,
        }
    })
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
        Ok(Self { inner, runtime })
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
        let acquire = acquire_wait_future(cx, inner, request_id);
        let result = if let Some(wait_timeout) = wait_timeout {
            match time::timeout(time::wall_now(), wait_timeout, Box::pin(acquire)).await {
                Ok(result) => result,
                Err(_) => Err(PoolError::NoConnectionAvailable),
            }
        } else {
            acquire.await
        };

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
            return Ok(());
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

/// Reference `_drop_conn_impl` + `_ensure_min_connections`.
fn drop_conn<B: PoolBackend>(
    state: &mut PoolState<B::Conn>,
    inner: &EngineInner<B>,
    conn: PooledConn<B::Conn>,
) {
    state.to_drop.push_back(conn);
    ensure_min_connections(state, inner);
    inner.bg.notify_one();
}

fn ensure_min_connections<B: PoolBackend>(state: &mut PoolState<B::Conn>, inner: &EngineInner<B>) {
    if state.open {
        let reserved = reserved_open_count(state);
        if reserved < state.config.min {
            schedule_open_effects(state, state.config.min - reserved);
        }
        inner.bg.notify_one();
    }
}

/// Reference `_check_connection`: validate a candidate pulled from a free
/// list. Returns the connection to the request, schedules a ping, or drops a
/// dead/expired connection (in which case scanning continues).
fn check_connection<B: PoolBackend>(
    state: &mut PoolState<B::Conn>,
    inner: &EngineInner<B>,
    request_id: u64,
    conn: PooledConn<B::Conn>,
) {
    if !inner.backend.connection_is_open(&conn.conn) {
        drop_conn(state, inner, conn);
        return;
    }
    let max_lifetime = state.config.max_lifetime_session_secs;
    if max_lifetime > 0
        && conn.time_created.elapsed() > Duration::from_secs(u64::from(max_lifetime))
    {
        drop_conn(state, inner, conn);
        return;
    }
    let ping_interval = state.config.ping_interval_secs;
    let requires_ping = if ping_interval == 0 {
        true
    } else if ping_interval > 0 {
        conn.time_returned.elapsed() > Duration::from_secs(ping_interval.unsigned_abs())
    } else {
        false
    };
    let Some(position) = request_position(state, request_id) else {
        // The acquirer vanished; treat as a reject.
        let mut orphan = Request {
            id: 0,
            cclass: None,
            cclass_matches: true,
            wants_new: false,
            requires_ping: false,
            bg_processing: false,
            is_extra: false,
            is_replacing: false,
            in_progress: false,
            completed: false,
            waiting: false,
            conn: Some(conn),
            error: None,
        };
        reject(state, inner, &mut orphan);
        return;
    };
    let request = &mut state.requests[position];
    request.conn = Some(conn);
    if requires_ping {
        request.requires_ping = true;
        add_request_for_bg(state, inner, request_id);
    } else {
        request.completed = true;
    }
}

/// Reference `_add_request`: mark the request as queued for the worker.
fn add_request_for_bg<B: PoolBackend>(
    state: &mut PoolState<B::Conn>,
    inner: &EngineInner<B>,
    request_id: u64,
) {
    if let Some(position) = request_position(state, request_id) {
        let request = &mut state.requests[position];
        request.bg_processing = true;
        request.completed = false;
        inner.bg.notify_one();
    }
}

/// Reference `PooledConnRequest.fulfill`, evaluated by the acquiring thread
/// under the state lock. `Ok(true)` means the request is completed.
fn fulfill<B: PoolBackend>(
    state: &mut PoolState<B::Conn>,
    inner: &EngineInner<B>,
    request_id: u64,
) -> Result<bool, PoolError> {
    if !state.open {
        return Err(PoolError::Closed);
    }
    let Some(position) = request_position(state, request_id) else {
        return Err(PoolError::Internal("request lost".to_string()));
    };
    {
        let request = &mut state.requests[position];
        if let Some(error) = request.error.take() {
            return Err(PoolError::Backend(error));
        }
        if request.completed {
            return Ok(true);
        }
        if request.bg_processing {
            return Ok(false);
        }
    }
    let wants_new = state.requests[position].wants_new;
    let request_cclass = state.requests[position].cclass.clone();
    let cclass_matches = state.requests[position].cclass_matches;

    // Check used connections, scanning from the tail (LIFO).
    if !wants_new {
        let mut ix = state.free_used.len();
        while ix > 0 {
            ix -= 1;
            let matches = request_cclass.is_none()
                || state.free_used[ix].cclass.as_deref() == request_cclass.as_deref();
            if !matches {
                continue;
            }
            let conn = state.free_used.remove(ix);
            check_connection(state, inner, request_id, conn);
            let Some(position) = request_position(state, request_id) else {
                return Err(PoolError::Internal("request lost".to_string()));
            };
            let request = &state.requests[position];
            if request.completed || request.requires_ping {
                return Ok(request.completed);
            }
            // Connection was dropped; resume scanning what remains.
            ix = ix.min(state.free_used.len());
        }
    }

    // Check new (never used) connections; only when the cclass matches.
    if cclass_matches {
        while let Some(conn) = state.free_new.pop() {
            check_connection(state, inner, request_id, conn);
            let Some(position) = request_position(state, request_id) else {
                return Err(PoolError::Internal("request lost".to_string()));
            };
            let request = &state.requests[position];
            if request.completed || request.requires_ping {
                return Ok(request.completed);
            }
        }
    }

    // No usable free connection. Reset requires_ping (mirrors reference).
    if let Some(position) = request_position(state, request_id) {
        state.requests[position].requires_ping = false;
    }
    if reserved_open_count(state) >= state.config.max {
        if let Some(victim) = state.free_new.pop() {
            if let Some(position) = request_position(state, request_id) {
                state.requests[position].is_replacing = true;
            }
            state.to_drop.push_back(victim);
            add_request_for_bg(state, inner, request_id);
            return Ok(false);
        } else if let Some(victim) = state.free_used.pop() {
            if let Some(position) = request_position(state, request_id) {
                state.requests[position].is_replacing = true;
            }
            state.to_drop.push_back(victim);
            add_request_for_bg(state, inner, request_id);
            return Ok(false);
        } else if state.force_get {
            if let Some(position) = request_position(state, request_id) {
                state.requests[position].is_extra = true;
            }
            add_request_for_bg(state, inner, request_id);
            return Ok(false);
        } else if state.config.getmode == POOL_GETMODE_NOWAIT {
            return Err(PoolError::NoConnectionAvailable);
        }
    } else if cclass_matches && pending_open_count(state) == 0 {
        let remaining_capacity = state.config.max.saturating_sub(reserved_open_count(state));
        schedule_open_effects(state, state.config.increment.min(remaining_capacity));
    }
    add_request_for_bg(state, inner, request_id);
    Ok(false)
}

/// Reference `_get_next_request`: pick the first live request the worker can
/// make progress on. Completed/errored requests awaiting their acquirer are
/// transparent to this scan (the reference removes them from its queue
/// eagerly; this engine keeps them as the acquirer's storage).
fn get_next_request<C>(state: &mut PoolState<C>) -> Option<u64> {
    let id = peek_next_request(state)?;
    if let Some(position) = request_position(state, id) {
        let request = &mut state.requests[position];
        request.in_progress = request.waiting;
    }
    Some(id)
}

/// Non-mutating variant of [`get_next_request`] used to decide whether the
/// worker can park.
fn peek_next_request<C>(state: &PoolState<C>) -> Option<u64> {
    for request in &state.requests {
        if request.completed || request.error.is_some() || request.in_progress {
            continue;
        }
        if !request.bg_processing {
            continue;
        }
        if !request.waiting
            || request.requires_ping
            || request.is_replacing
            || request.is_extra
            || (!request.cclass_matches && reserved_open_count(state) < state.config.max)
        {
            return Some(request.id);
        }
        break;
    }
    None
}

/// Reference `_post_process_request`.
fn post_process_request<B: PoolBackend>(
    state: &mut PoolState<B::Conn>,
    inner: &EngineInner<B>,
    request_id: u64,
) {
    let Some(position) = request_position(state, request_id) else {
        return;
    };
    let request = &mut state.requests[position];
    request.in_progress = false;
    request.bg_processing = false;
    if request.conn.is_some() {
        request.completed = true;
        let request = &mut state.requests[position];
        if !request.waiting {
            let mut request = state.requests.remove(position);
            reject(state, inner, &mut request);
        }
    } else {
        if request.requires_ping {
            ensure_min_connections(state, inner);
        }
        let request = &mut state.requests[position];
        if !request.waiting {
            state.requests.remove(position);
        }
    }
    wake_waiters(inner);
}

/// Reference `_post_create_conn_impl`: integrate a connection created for
/// pool growth (not for a specific request).
fn post_create_conn<B: PoolBackend>(
    state: &mut PoolState<B::Conn>,
    inner: &EngineInner<B>,
    conn_id: u64,
    created: Result<PooledConn<B::Conn>, String>,
) {
    complete_open_effect(state, conn_id);
    let conn = match created {
        Ok(conn) => conn,
        Err(error) => {
            if state.open {
                if let Some(request) = state.requests.iter_mut().find(|request| {
                    request.bg_processing
                        && request.waiting
                        && !request.requires_ping
                        && !request.is_extra
                        && !request.is_replacing
                        && request.cclass_matches
                        && request.conn.is_none()
                }) {
                    request.bg_processing = false;
                    request.error = Some(error);
                    wake_waiters(inner);
                }
            }
            return;
        }
    };
    debug_assert_eq!(conn.id, conn_id);
    if !state.open {
        state.to_drop.push_back(conn);
        inner.bg.notify_one();
        return;
    }
    let mut conn = Some(conn);
    let max = state.config.max;
    let open_count = active_open_count(state).saturating_add(1);
    for request in &mut state.requests {
        if request.in_progress
            || request.conn.is_some()
            || !request.waiting
            || request.completed
            || request.error.is_some()
        {
            continue;
        }
        let candidate = conn.as_ref().expect("connection still available");
        if request.cclass.is_none() || request.cclass.as_deref() == candidate.cclass.as_deref() {
            request.conn = conn.take();
            request.completed = true;
            request.bg_processing = false;
            wake_waiters(inner);
            break;
        } else if !request.cclass_matches && open_count >= max {
            request.conn = conn.take();
            request.is_replacing = true;
            break;
        }
    }
    if let Some(conn) = conn {
        state.free_new.push(conn);
        wake_waiters(inner);
    }
}

/// Reference `_return_connection_helper`.
fn return_connection_helper<B: PoolBackend>(
    state: &mut PoolState<B::Conn>,
    inner: &EngineInner<B>,
    mut conn: PooledConn<B::Conn>,
    mut is_open: bool,
) {
    if !is_open {
        ensure_min_connections(state, inner);
    }
    if conn.is_pool_extra {
        conn.is_pool_extra = false;
        let count_with_returned = active_open_count(state).saturating_add(u32::from(is_open));
        if is_open && count_with_returned > state.config.max {
            drop_conn(state, inner, conn);
            return;
        }
    }
    let mut returned = Some(conn);
    if is_open {
        let conn = returned.as_mut().expect("connection still available");
        conn.time_returned = Instant::now();
        let max_lifetime = state.config.max_lifetime_session_secs;
        if max_lifetime != 0
            && conn.time_created.elapsed() > Duration::from_secs(u64::from(max_lifetime))
        {
            let conn = returned.take().expect("connection still available");
            drop_conn(state, inner, conn);
            is_open = false;
        }
    }
    if is_open {
        let mut conn = returned;
        for request in &mut state.requests {
            if request.in_progress
                || request.wants_new
                || request.conn.is_some()
                || !request.waiting
                || request.completed
                || request.error.is_some()
            {
                continue;
            }
            let candidate = conn.as_ref().expect("connection still available");
            let matches = request.cclass.is_none()
                || request.cclass.as_deref() == candidate.cclass.as_deref();
            if matches {
                request.conn = conn.take();
                request.completed = true;
                request.bg_processing = false;
                wake_waiters(inner);
                return;
            }
        }
        if let Some(conn) = conn {
            state.free_used.push(conn);
        }
    }
}

/// Drop free connections that have been idle longer than `timeout_secs`,
/// oldest first, while keeping at least `min` connections open. Reference
/// `_timeout_helper` driven by a timer; here the worker sweeps periodically.
fn sweep_idle_timeout<B: PoolBackend>(state: &mut PoolState<B::Conn>, inner: &EngineInner<B>) {
    let timeout_secs = state.config.timeout_secs;
    if timeout_secs == 0 {
        return;
    }
    let limit = Duration::from_secs(u64::from(timeout_secs));
    for list in ["new", "used"] {
        loop {
            if active_open_count(state) <= state.config.min {
                return;
            }
            let conns = if list == "new" {
                &mut state.free_new
            } else {
                &mut state.free_used
            };
            let Some(first) = conns.first() else {
                break;
            };
            if first.time_returned.elapsed() < limit {
                break;
            }
            let conn = conns.remove(0);
            drop_conn(state, inner, conn);
        }
    }
}

/// Region-owned reaper: process queued requests (pings and dedicated creates),
/// grow the pool, close queued connections and sweep idle timeouts. Mirrors the
/// reference `_bg_task_func` structure.
///
/// This is the async successor to the former detached-OS-thread `bg_main`. It
/// runs as an asupersync task owned by the pool's dedicated runtime: connection
/// create/ping/close still happen off the state lock (the original perf
/// rationale), the former `Condvar` parking becomes an awaited
/// [`Notify::notified`], and a `cx.checkpoint()` rides each loop iteration so a
/// runtime/region cancel is observed promptly.
///
/// The task holds a [`Weak`] to `EngineInner`: it never keeps the pool (and the
/// runtime it lives on) alive. When the last pool handle is dropped, the upgrade
/// fails and the reaper exits; a [`PoolEngine::close_async`] instead flips the
/// cooperative `reaper_stop` flag, drains the close queue, and the close path
/// awaits this task to completion.
async fn reaper_main<B: PoolBackend>(weak: Weak<EngineInner<B>>, bg: Arc<Notify>) {
    let cx = Cx::current();
    let mut current_request: Option<u64> = None;
    loop {
        let Some(inner) = weak.upgrade() else {
            // Last pool handle dropped: nothing left to reconcile.
            return;
        };
        // Observe runtime/region cancellation cooperatively. A cancelled Cx
        // means the owning runtime is shutting down, so stop reconciling.
        if let Some(cx) = cx.as_ref() {
            if cx.checkpoint().is_err() {
                return;
            }
        }
        let stopping = inner.reaper_stop.load(Ordering::SeqCst);

        // Pick up a request to process if none is pending.
        let mut open;
        {
            let Ok(mut state) = inner.state.lock() else {
                return;
            };
            if drain_drop_returns(&mut state, &inner).is_err() {
                return;
            }
            open = state.open;
            if current_request.is_none() && open {
                current_request = get_next_request(&mut state);
            }
        }
        if let Some(request_id) = current_request.take() {
            if open {
                process_request(&inner, request_id);
                let Ok(mut state) = inner.state.lock() else {
                    return;
                };
                post_process_request(&mut state, &inner, request_id);
                current_request = get_next_request(&mut state);
                continue;
            }
        }

        // Create a connection for an explicit pool growth effect if requested.
        let (open_effect, cclass) = {
            let Ok(mut state) = inner.state.lock() else {
                return;
            };
            open = state.open;
            let effect = if open {
                if let Some(effect) = state.open_effects.pop_front() {
                    state.in_flight_open_effects.push_back(effect);
                }
                state.in_flight_open_effects.back().copied()
            } else {
                None
            };
            (effect, state.config.creation_cclass.clone())
        };
        if let Some(lifecycle::PoolEffect::Open { slot: conn_id, .. }) = open_effect {
            let created = inner
                .backend
                .create_connection(conn_id, cclass.as_deref())
                .map(|conn| PooledConn {
                    id: conn_id,
                    conn,
                    cclass,
                    time_created: Instant::now(),
                    time_returned: Instant::now(),
                    is_pool_extra: false,
                    ever_acquired: false,
                });
            let Ok(mut state) = inner.state.lock() else {
                return;
            };
            post_create_conn(&mut state, &inner, conn_id, created);
            continue;
        }

        // Close a queued connection.
        let next_drop = {
            let Ok(mut state) = inner.state.lock() else {
                return;
            };
            state.to_drop.pop_back()
        };
        if let Some(conn) = next_drop {
            inner.backend.close_connection(conn.id, conn.conn);
            continue;
        }

        // Idle-timeout sweep, then decide whether to park or exit.
        let timeout_armed;
        {
            let Ok(mut state) = inner.state.lock() else {
                return;
            };
            sweep_idle_timeout(&mut state, &inner);
            if (!state.open || stopping) && state.to_drop.is_empty() {
                return;
            }
            let has_work = !state.open_effects.is_empty()
                || !state.to_drop.is_empty()
                || peek_next_request(&state).is_some();
            if has_work {
                continue;
            }
            timeout_armed = state.config.timeout_secs > 0
                && active_open_count(&state) > state.config.min
                && (!state.free_new.is_empty() || !state.free_used.is_empty());
        }
        // Release the strong `EngineInner` ref BEFORE parking, so that a
        // concurrent drop of the last pool handle can actually drop `EngineInner`
        // (and the runtime) while the reaper is asleep. We park on the standalone
        // `bg` notifier (an `Arc<Notify>` we own), not on `inner`. The guard is
        // never held across an `.await`. A wakeup (`bg.notify_one`) or the 1s
        // idle-sweep tick resumes us; `EngineInner::drop` also wakes us so the
        // upgrade at the top of the next loop fails fast.
        drop(inner);
        let notified = bg.notified();
        if timeout_armed {
            let _ = time::timeout(time::wall_now(), Duration::from_secs(1), notified).await;
        } else {
            notified.await;
        }
    }
}

/// Reference `_process_request`: ping validation or dedicated creation,
/// performed without the state lock held.
fn process_request<B: PoolBackend>(inner: &Arc<EngineInner<B>>, request_id: u64) {
    enum Work<C> {
        Ping {
            conn: PooledConn<C>,
            ping_timeout_ms: u32,
        },
        Create {
            conn_id: u64,
            cclass: Option<String>,
            is_extra: bool,
            replaced: Option<PooledConn<C>>,
        },
        Nothing,
    }
    let work = {
        let Ok(mut state) = inner.state.lock() else {
            return;
        };
        let Some(position) = request_position(&state, request_id) else {
            return;
        };
        let ping_timeout_ms = state.config.ping_timeout_ms;
        let request = &mut state.requests[position];
        if request.requires_ping {
            match request.conn.take() {
                Some(conn) => Work::Ping {
                    conn,
                    ping_timeout_ms,
                },
                None => Work::Nothing,
            }
        } else if request.is_replacing || request.is_extra || !request.cclass_matches {
            let cclass = request.cclass.clone();
            let is_extra = request.is_extra;
            let replaced = request.conn.take();
            let conn_id = state.next_conn_id;
            state.next_conn_id += 1;
            Work::Create {
                conn_id,
                cclass,
                is_extra,
                replaced,
            }
        } else {
            Work::Nothing
        }
    };
    match work {
        Work::Ping {
            conn,
            ping_timeout_ms,
        } => {
            let healthy = inner.backend.ping_connection(&conn.conn, ping_timeout_ms);
            if healthy {
                let Ok(mut state) = inner.state.lock() else {
                    return;
                };
                if let Some(position) = request_position(&state, request_id) {
                    state.requests[position].conn = Some(conn);
                }
            } else {
                inner.backend.close_connection(conn.id, conn.conn);
            }
        }
        Work::Create {
            conn_id,
            cclass,
            is_extra,
            replaced,
        } => {
            let result = inner.backend.create_connection(conn_id, cclass.as_deref());
            let Ok(mut state) = inner.state.lock() else {
                return;
            };
            if let Some(old) = replaced {
                drop_conn(&mut state, inner, old);
            }
            let Some(position) = request_position(&state, request_id) else {
                // Acquirer vanished entirely; close the new connection.
                if let Ok(conn) = result {
                    state.to_drop.push_back(PooledConn {
                        id: conn_id,
                        conn,
                        cclass,
                        time_created: Instant::now(),
                        time_returned: Instant::now(),
                        is_pool_extra: false,
                        ever_acquired: false,
                    });
                    inner.bg.notify_one();
                }
                return;
            };
            match result {
                Ok(conn) => {
                    state.requests[position].conn = Some(PooledConn {
                        id: conn_id,
                        conn,
                        cclass,
                        time_created: Instant::now(),
                        time_returned: Instant::now(),
                        is_pool_extra: is_extra,
                        ever_acquired: false,
                    });
                }
                Err(error) => {
                    state.requests[position].error = Some(error);
                }
            }
        }
        Work::Nothing => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lifecycle::{PoolCloseReason, PoolCounts, PoolEffect, PoolSlotState, PurePoolState};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    // Test-only: `BlockingCreateBackend` is a fake backend that blocks inside
    // `create_connection` to exercise the in-flight-create close path. Its
    // `Condvar` models a slow remote server; it is unrelated to the (now async)
    // pool reaper, which no longer uses any `Condvar`.
    use std::sync::Condvar;

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
