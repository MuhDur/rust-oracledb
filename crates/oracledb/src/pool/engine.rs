use super::lifecycle;
use super::{
    active_open_count, compatible_idle_count, compatible_waiting_demand, complete_open_effect,
    drain_drop_returns, pending_open_count, reject, request_position, reserved_open_count,
    schedule_open_effects, wake_waiters, EngineInner, PoolBackend, PoolError, PoolState,
    PooledConn, Request,
};
use asupersync::sync::Notify;
use asupersync::{time, Cx};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

/// Reference `_drop_conn_impl` + `_ensure_min_connections`.
pub(super) fn drop_conn<B: PoolBackend>(
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
pub(super) fn fulfill<B: PoolBackend>(
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
        } else if state.config.getmode == super::POOL_GETMODE_NOWAIT {
            return Err(PoolError::NoConnectionAvailable);
        }
    } else if cclass_matches {
        let remaining_capacity = state.config.max.saturating_sub(reserved_open_count(state));
        let pending = pending_open_count(state);
        let idle =
            compatible_idle_count(state, wants_new, request_cclass.as_deref(), cclass_matches);
        let demand = compatible_waiting_demand(state);
        let supply = pending.saturating_add(idle);
        let shortfall = demand.saturating_sub(supply);
        let desired = if pending == 0 {
            state.config.increment.max(shortfall)
        } else {
            shortfall
        };
        if desired > 0 {
            schedule_open_effects(state, desired.min(remaining_capacity));
        }
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
pub(super) fn return_connection_helper<B: PoolBackend>(
    state: &mut PoolState<B::Conn>,
    inner: &EngineInner<B>,
    mut conn: PooledConn<B::Conn>,
    mut is_open: bool,
) {
    if !is_open {
        drop_conn(state, inner, conn);
        return;
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
/// fails and the reaper exits; a [`super::PoolEngine::close_async`] instead
/// flips the cooperative `reaper_stop` flag, drains the close queue, and the
/// close path awaits this task to completion.
pub(super) async fn reaper_main<B: PoolBackend>(weak: Weak<EngineInner<B>>, bg: Arc<Notify>) {
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
