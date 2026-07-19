use super::engine::fulfill;
use super::{
    abandon_request, checkpoint_pool, lock_state, wake_waiters, AcquireOptions, EngineInner,
    PoolBackend, PoolError, PoolState, Request,
};
use asupersync::{time, Cx};
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

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
    wait_timeout: Option<Duration>,
) -> impl Future<Output = Result<u64, PoolError>> + 'a {
    let mut notified = None;
    // `asupersync::time::Sleep` registers with the runtime's shared timer
    // driver. A TIMEDWAIT acquire therefore consumes one small future, not one
    // detached operating-system thread. Dropping the acquire future drops both
    // this timer registration and `AsyncAcquireRequest`, so cancellation also
    // removes the queued request without a stray wake thread.
    let mut deadline = wait_timeout.map(|timeout| Box::pin(time::sleep(time::wall_now(), timeout)));
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

        if let Some(deadline) = deadline.as_mut() {
            if Future::poll(Pin::as_mut(deadline), task_cx).is_ready() {
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
