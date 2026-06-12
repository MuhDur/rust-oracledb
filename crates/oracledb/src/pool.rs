//! Connection pool engine mirroring python-oracledb's thin pool algebra
//! (`impl/thin/pool.pyx`). The engine owns the pool state machine (free
//! lists, busy list, growth planning, getmode semantics, ping policy, idle
//! timeout, max lifetime) and a background worker thread that creates,
//! pings and closes connections through a [`PoolBackend`].
//!
//! The engine is deliberately free of any Python types; the pyshim provides
//! a backend whose `Conn` payload carries shared handles to the underlying
//! transport. Acquire-side waiting happens on a [`Condvar`] with no foreign
//! locks held, so embedders must release the GIL (or equivalent) before
//! calling into blocking engine entry points.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
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
            PoolError::Internal(message) => write!(f, "pool internal error: {message}"),
        }
    }
}

/// Static pool configuration captured at pool creation. Mutable attributes
/// (getmode, timeouts, ping interval) have engine setters.
#[derive(Clone, Debug)]
pub struct PoolConfig {
    pub min: u32,
    pub max: u32,
    pub increment: u32,
    pub getmode: u32,
    pub wait_timeout_ms: u32,
    pub timeout_secs: u32,
    pub max_lifetime_session_secs: u32,
    pub ping_interval_secs: i64,
    pub ping_timeout_ms: u32,
    pub creation_cclass: Option<String>,
}

/// Per-acquire options derived from the acquire-time connect params.
#[derive(Clone, Debug, Default)]
pub struct AcquireOptions {
    /// PURITY_NEW was requested: never reuse a previously used connection.
    pub wants_new: bool,
    /// Connection class requested at acquire time.
    pub cclass: Option<String>,
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
    open_count: u32,
    num_to_create: u32,
    next_conn_id: u64,
    next_request_id: u64,
}

struct EngineInner<B: PoolBackend> {
    backend: B,
    state: Mutex<PoolState<B::Conn>>,
    /// Woken whenever a waiter's `fulfill` predicate may have changed.
    waiters: Condvar,
    /// Woken whenever the background worker has work to do.
    bg: Condvar,
    bg_handle: Mutex<Option<JoinHandle<()>>>,
}

pub struct PoolEngine<B: PoolBackend> {
    inner: Arc<EngineInner<B>>,
}

impl<B: PoolBackend> Clone for PoolEngine<B> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
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
        let num_to_create = config.min;
        let state = PoolState {
            open: true,
            config,
            force_get,
            wait_timeout_ms,
            free_new: Vec::new(),
            free_used: Vec::new(),
            busy: Vec::new(),
            to_drop: VecDeque::new(),
            requests: Vec::new(),
            open_count: 0,
            num_to_create,
            next_conn_id: 1,
            next_request_id: 1,
        };
        let inner = Arc::new(EngineInner {
            backend,
            state: Mutex::new(state),
            waiters: Condvar::new(),
            bg: Condvar::new(),
            bg_handle: Mutex::new(None),
        });
        let bg_inner = Arc::clone(&inner);
        let handle = std::thread::Builder::new()
            .name("oracledb-pool-bg".to_string())
            .spawn(move || bg_main(bg_inner))
            .map_err(|err| PoolError::Internal(err.to_string()))?;
        *inner
            .bg_handle
            .lock()
            .map_err(|err| PoolError::Internal(err.to_string()))? = Some(handle);
        Ok(Self { inner })
    }

    /// Acquire a connection following the reference `fulfill` algebra.
    /// Returns the engine id of the connection now recorded as busy.
    ///
    /// Blocking: callers must not hold the GIL or any embedder lock.
    pub fn acquire(&self, opts: AcquireOptions) -> Result<u64, PoolError> {
        let inner = &*self.inner;
        let mut state = lock_state(inner)?;
        if !state.open {
            return Err(PoolError::Closed);
        }
        let request_id = state.next_request_id;
        state.next_request_id += 1;
        let pool_cclass = state.config.creation_cclass.clone();
        let cclass_matches =
            opts.cclass.is_none() || opts.cclass.as_deref() == pool_cclass.as_deref();
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
        let deadline = state
            .wait_timeout_ms
            .map(|ms| Instant::now() + Duration::from_millis(u64::from(ms)));
        loop {
            match fulfill(&mut state, inner, request_id) {
                Ok(true) => {
                    let position = request_position(&state, request_id)
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
                    return Ok(conn_id);
                }
                Ok(false) => {}
                Err(err) => {
                    abandon_request(&mut state, inner, request_id);
                    return Err(err);
                }
            }
            if let Some(deadline) = deadline {
                let now = Instant::now();
                if now >= deadline {
                    // Re-check completion before failing: the worker may have
                    // satisfied the request between the wait and this point.
                    if request_position(&state, request_id)
                        .map(|ix| state.requests[ix].completed)
                        .unwrap_or(false)
                    {
                        continue;
                    }
                    abandon_request(&mut state, inner, request_id);
                    return Err(PoolError::NoConnectionAvailable);
                }
                let (next, _) = inner
                    .waiters
                    .wait_timeout(state, deadline - now)
                    .map_err(|err| PoolError::Internal(err.to_string()))?;
                state = next;
            } else {
                state = inner
                    .waiters
                    .wait(state)
                    .map_err(|err| PoolError::Internal(err.to_string()))?;
            }
        }
    }

    /// Return a busy connection to the pool. The embedder performs the
    /// end-of-request work (rollback) before calling this. No-op when the
    /// pool is already closed (mirrors the reference).
    pub fn return_connection(&self, conn_id: u64) -> Result<(), PoolError> {
        let inner = &*self.inner;
        let mut state = lock_state(inner)?;
        if !state.open {
            return Ok(());
        }
        let Some(position) = state.busy.iter().position(|conn| conn.id == conn_id) else {
            return Ok(());
        };
        let conn = state.busy.remove(position);
        let is_open = inner.backend.connection_is_open(&conn.conn);
        return_connection_helper(&mut state, inner, conn, is_open);
        inner.waiters.notify_all();
        Ok(())
    }

    /// Drop a busy connection from the pool (`ConnectionPool.drop`).
    pub fn drop_connection(&self, conn_id: u64) -> Result<(), PoolError> {
        let inner = &*self.inner;
        let mut state = lock_state(inner)?;
        if !state.open {
            return Ok(());
        }
        let Some(position) = state.busy.iter().position(|conn| conn.id == conn_id) else {
            return Ok(());
        };
        state.open_count = state.open_count.saturating_sub(1);
        let conn = state.busy.remove(position);
        drop_conn(&mut state, inner, conn);
        inner.waiters.notify_all();
        Ok(())
    }

    /// Close the pool. With `force == false`, fails when busy connections or
    /// live waiters exist (DPY-1005). Joins the background worker, so all
    /// transports are closed by the time this returns.
    ///
    /// Blocking: callers must not hold the GIL or any embedder lock.
    pub fn close(&self, force: bool) -> Result<(), PoolError> {
        let inner = &*self.inner;
        {
            let mut state = lock_state(inner)?;
            if !state.open {
                return Ok(());
            }
            if !force {
                let has_waiters = state.requests.iter().any(|request| request.waiting);
                if !state.busy.is_empty() || has_waiters {
                    return Err(PoolError::HasBusyConnections);
                }
            }
            state.open = false;
            let free_new = std::mem::take(&mut state.free_new);
            let free_used = std::mem::take(&mut state.free_used);
            let busy = std::mem::take(&mut state.busy);
            state
                .to_drop
                .extend(free_new.into_iter().chain(free_used).chain(busy));
            inner.bg.notify_all();
            inner.waiters.notify_all();
        }
        let handle = self
            .inner
            .bg_handle
            .lock()
            .map_err(|err| PoolError::Internal(err.to_string()))?
            .take();
        if let Some(handle) = handle {
            handle
                .join()
                .map_err(|_| PoolError::Internal("pool worker panicked".to_string()))?;
        }
        Ok(())
    }

    pub fn busy_count(&self) -> Result<u32, PoolError> {
        let state = lock_state(&self.inner)?;
        Ok(u32::try_from(state.busy.len()).unwrap_or(u32::MAX))
    }

    pub fn open_count(&self) -> Result<u32, PoolError> {
        let state = lock_state(&self.inner)?;
        Ok(state.open_count)
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
        self.inner.bg.notify_all();
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
        if conn.is_pool_extra {
            conn.is_pool_extra = false;
            state.to_drop.push_back(conn);
            inner.bg.notify_all();
        } else if !conn.ever_acquired {
            state.free_new.push(conn);
        } else {
            state.free_used.push(conn);
        }
        inner.waiters.notify_all();
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
    inner.bg.notify_all();
}

fn ensure_min_connections<B: PoolBackend>(state: &mut PoolState<B::Conn>, inner: &EngineInner<B>) {
    if state.open_count < state.config.min {
        state.num_to_create = state.num_to_create.max(state.config.min - state.open_count);
        inner.bg.notify_all();
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
        state.open_count = state.open_count.saturating_sub(1);
        drop_conn(state, inner, conn);
        return;
    }
    let max_lifetime = state.config.max_lifetime_session_secs;
    if max_lifetime > 0
        && conn.time_created.elapsed() > Duration::from_secs(u64::from(max_lifetime))
    {
        state.open_count = state.open_count.saturating_sub(1);
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
        inner.bg.notify_all();
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
    if state.open_count + state.num_to_create >= state.config.max {
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
    } else if cclass_matches && state.num_to_create == 0 {
        state.num_to_create = state
            .config
            .increment
            .min(state.config.max - state.open_count);
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
            || state.open_count < state.config.max
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
        if !request.is_replacing && !request.requires_ping {
            state.open_count += 1;
            state.num_to_create = state.num_to_create.saturating_sub(1);
        }
        let request = &mut state.requests[position];
        if !request.waiting {
            let mut request = state.requests.remove(position);
            reject(state, inner, &mut request);
        }
    } else {
        if request.requires_ping {
            state.open_count = state.open_count.saturating_sub(1);
            if state.num_to_create == 0 && state.open_count < state.config.min {
                state.num_to_create = state.config.min - state.open_count;
            }
        }
        let request = &mut state.requests[position];
        if !request.waiting {
            state.requests.remove(position);
        }
    }
    inner.waiters.notify_all();
}

/// Reference `_post_create_conn_impl`: integrate a connection created for
/// pool growth (not for a specific request).
fn post_create_conn<B: PoolBackend>(
    state: &mut PoolState<B::Conn>,
    inner: &EngineInner<B>,
    created: Option<PooledConn<B::Conn>>,
) {
    let Some(conn) = created else {
        state.num_to_create = 0;
        return;
    };
    if !state.open {
        state.to_drop.push_back(conn);
        inner.bg.notify_all();
        return;
    }
    state.open_count += 1;
    state.num_to_create = state.num_to_create.saturating_sub(1);
    let mut conn = Some(conn);
    let max = state.config.max;
    let open_count = state.open_count;
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
            inner.waiters.notify_all();
            break;
        } else if !request.cclass_matches && open_count >= max {
            request.conn = conn.take();
            request.is_replacing = true;
            break;
        }
    }
    if let Some(conn) = conn {
        state.free_new.push(conn);
        inner.waiters.notify_all();
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
        state.open_count = state.open_count.saturating_sub(1);
        ensure_min_connections(state, inner);
    }
    if conn.is_pool_extra {
        conn.is_pool_extra = false;
        if is_open && state.open_count >= state.config.max {
            if !state.free_new.is_empty() && state.open_count == state.config.max {
                let victim = state.free_new.remove(0);
                drop_conn(state, inner, victim);
            } else {
                state.open_count = state.open_count.saturating_sub(1);
                drop_conn(state, inner, conn);
                return;
            }
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
            state.open_count = state.open_count.saturating_sub(1);
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
                inner.waiters.notify_all();
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
            if state.open_count <= state.config.min {
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
            state.open_count = state.open_count.saturating_sub(1);
            drop_conn(state, inner, conn);
        }
    }
}

/// Background worker: process queued requests (pings and dedicated creates),
/// grow the pool, close queued connections and sweep idle timeouts. Mirrors
/// the reference `_bg_task_func` structure.
fn bg_main<B: PoolBackend>(inner: Arc<EngineInner<B>>) {
    let mut current_request: Option<u64> = None;
    loop {
        // Pick up a request to process if none is pending.
        let mut open;
        {
            let Ok(mut state) = inner.state.lock() else {
                return;
            };
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

        // Create a connection for pool growth if requested.
        let (num_to_create, conn_id, cclass) = {
            let Ok(mut state) = inner.state.lock() else {
                return;
            };
            open = state.open;
            let id = state.next_conn_id;
            if state.num_to_create > 0 && open {
                state.next_conn_id += 1;
            }
            (
                state.num_to_create,
                id,
                state.config.creation_cclass.clone(),
            )
        };
        if num_to_create > 0 && open {
            let created = inner
                .backend
                .create_connection(conn_id, cclass.as_deref())
                .ok()
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
            post_create_conn(&mut state, &inner, created);
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

        // Idle-timeout sweep and worker parking.
        {
            let Ok(mut state) = inner.state.lock() else {
                return;
            };
            sweep_idle_timeout(&mut state, &inner);
            if !state.open && state.to_drop.is_empty() {
                return;
            }
            let has_work = state.num_to_create > 0
                || !state.to_drop.is_empty()
                || peek_next_request(&state).is_some();
            if has_work {
                continue;
            }
            let timeout_armed = state.config.timeout_secs > 0
                && state.open_count > state.config.min
                && (!state.free_new.is_empty() || !state.free_used.is_empty());
            if timeout_armed {
                match inner.bg.wait_timeout(state, Duration::from_secs(1)) {
                    Ok(_) | Err(_) => {}
                }
            } else {
                match inner.bg.wait(state) {
                    Ok(_) | Err(_) => {}
                }
            }
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
        } else if request.requires_ping || request.is_replacing || request.waiting {
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
                    inner.bg.notify_all();
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
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

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

    fn test_config(min: u32, max: u32, increment: u32, getmode: u32) -> PoolConfig {
        PoolConfig {
            min,
            max,
            increment,
            getmode,
            wait_timeout_ms: 1_000,
            timeout_secs: 0,
            max_lifetime_session_secs: 0,
            ping_interval_secs: -1,
            ping_timeout_ms: 5_000,
            creation_cclass: None,
        }
    }

    fn wait_for_open_count<B: PoolBackend>(engine: &PoolEngine<B>, expected: u32) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if engine.open_count().unwrap() == expected {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!(
            "open count never reached {expected}; current {}",
            engine.open_count().unwrap()
        );
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
        let first = engine.acquire(AcquireOptions::default()).unwrap();
        assert_eq!(engine.busy_count().unwrap(), 1);
        engine.return_connection(first).unwrap();
        assert_eq!(engine.busy_count().unwrap(), 0);
        let second = engine.acquire(AcquireOptions::default()).unwrap();
        assert_eq!(second, first, "expected LIFO reuse of returned connection");
        engine.return_connection(second).unwrap();
        engine.close(false).unwrap();
        assert_eq!(backend.closed.load(Ordering::SeqCst), 2);
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
        match engine.acquire(AcquireOptions::default()) {
            Err(PoolError::NoConnectionAvailable) => {}
            other => panic!("expected NoConnectionAvailable, got {other:?}"),
        }
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
    fn close_with_busy_requires_force() {
        let backend = Arc::new(FakeBackend::new());
        let engine = PoolEngine::start(
            Arc::clone(&backend),
            test_config(1, 2, 1, POOL_GETMODE_WAIT),
        )
        .unwrap();
        let _conn = engine.acquire(AcquireOptions::default()).unwrap();
        match engine.close(false) {
            Err(PoolError::HasBusyConnections) => {}
            other => panic!("expected HasBusyConnections, got {other:?}"),
        }
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
        match engine.acquire(AcquireOptions::default()) {
            Err(PoolError::Backend(message)) => {
                assert!(message.contains("ORA-01017"), "message: {message}");
            }
            other => panic!("expected Backend error, got {other:?}"),
        }
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
            .acquire(AcquireOptions {
                wants_new: true,
                cclass: None,
            })
            .unwrap();
        assert_ne!(c, a);
        assert_ne!(c, b);
        assert_eq!(engine.open_count().unwrap(), 2, "replacement keeps count");
        engine.return_connection(c).unwrap();
        engine.close(false).unwrap();
    }
}
