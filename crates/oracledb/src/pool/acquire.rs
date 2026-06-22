use super::engine::fulfill;
use super::{
    abandon_request, checkpoint_pool, lock_state, wake_waiters, AcquireOptions, EngineInner,
    PoolBackend, PoolError, PoolState, Request,
};
use asupersync::Cx;
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use std::time::{Duration, Instant};

pub(super) struct AsyncAcquireRequest<'a, B: PoolBackend> {
    inner: &'a EngineInner<B>,
    request_id: u64,
    active: bool,
}

impl<'a, B: PoolBackend> AsyncAcquireRequest<'a, B> {
    pub(super) fn new(inner: &'a EngineInner<B>, request_id: u64) -> Self {
        Self {
            inner,
            request_id,
            active: true,
        }
    }

    pub(super) fn complete(&mut self) {
        self.active = false;
    }

    pub(super) fn abandon(&mut self) {
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

struct TimedAcquireWakeState {
    stop: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

pub(super) struct TimedAcquireDeadline {
    deadline: Instant,
    wake_state: Arc<TimedAcquireWakeState>,
    wake_thread: std::thread::Thread,
}

impl TimedAcquireDeadline {
    pub(super) fn new(wait_timeout: Duration) -> Self {
        let start = Instant::now();
        let deadline = start
            .checked_add(wait_timeout)
            .expect("u32 millisecond pool wait timeout must fit in Instant");
        let wake_state = Arc::new(TimedAcquireWakeState {
            stop: AtomicBool::new(false),
            waker: Mutex::new(None),
        });
        let wake_state_for_thread = Arc::clone(&wake_state);
        let join = std::thread::spawn(move || {
            while !wake_state_for_thread.stop.load(Ordering::Acquire) {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                std::thread::park_timeout(deadline.saturating_duration_since(now));
            }

            if wake_state_for_thread.stop.load(Ordering::Acquire) {
                return;
            }
            if let Ok(mut waker) = wake_state_for_thread.waker.lock() {
                if let Some(waker) = waker.take() {
                    waker.wake();
                }
            }
        });
        let wake_thread = join.thread().clone();
        drop(join);
        Self {
            deadline,
            wake_state,
            wake_thread,
        }
    }

    fn register_waker(&self, waker: &Waker) {
        if let Ok(mut registered) = self.wake_state.waker.lock() {
            let replace = registered
                .as_ref()
                .is_none_or(|current| !current.will_wake(waker));
            if replace {
                *registered = Some(waker.clone());
            }
        }
    }

    fn is_elapsed(&self) -> bool {
        Instant::now() >= self.deadline
    }
}

impl Drop for TimedAcquireDeadline {
    fn drop(&mut self) {
        self.wake_state.stop.store(true, Ordering::Release);
        self.wake_thread.unpark();
    }
}

pub(super) fn enqueue_request<C>(
    state: &mut PoolState<C>,
    opts: AcquireOptions,
) -> Result<u64, PoolError> {
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
    let position = super::request_position(state, request_id)
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

pub(super) fn poll_request_completion<B: PoolBackend>(
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

pub(super) fn acquire_wait_future<'a, B: PoolBackend>(
    cx: &'a Cx,
    inner: &'a EngineInner<B>,
    request_id: u64,
    deadline: Option<&'a TimedAcquireDeadline>,
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

        if let Some(deadline) = deadline {
            deadline.register_waker(task_cx.waker());
            if deadline.is_elapsed() {
                return Poll::Ready(Err(PoolError::NoConnectionAvailable));
            }
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
