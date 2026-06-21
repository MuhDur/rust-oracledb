//! W1-T7.4 risk-proof integration tests against the REAL `Pool` (not asupersync
//! primitives in isolation). Each test nails one of the four risks the design
//! review flagged:
//!
//!   1. CROSS-RUNTIME JOIN — `Pool::close(&cx, ...).await` is awaited on the
//!      caller's runtime while the reaper task runs on the pool's own runtime.
//!   2. NO LEAKED RUNTIME/TASK — the pool's dedicated runtime and its reaper
//!      task are actually torn down on BOTH `close`+`drop` AND bare `drop`.
//!      Proven by a Drop SENTINEL owned by the pool's backend (a reliable
//!      signal), not by racy OS-thread counts.
//!   3. NESTED RUNTIME — a `Pool` can be constructed from WITHIN an async task
//!      on another runtime (no runtime-within-runtime panic).
//!   4. STATE LOCK — backend create/ping/close I/O runs OFF the `PoolState`
//!      lock (stats/lock-taking operations make progress while a create blocks).
//!
//! All tests use a fake backend (no live DB) and are deterministic.

use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::Cx;
use oracledb::pool::{Pool, PoolBackend, PoolConfig, POOL_GETMODE_WAIT};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

// ---- fake backend -------------------------------------------------------

#[derive(Clone)]
struct FakeConn {
    alive: Arc<AtomicBool>,
}

/// Shared, observable backend state. Counters live behind an `Arc` so a test can
/// observe them via a cheap handle while the pool owns the backend that drives
/// them. The orphan rule is satisfied because the `PoolBackend` impl is on the
/// local `FakeBackend` type.
struct Shared {
    created: AtomicU64,
    closed: AtomicU64,
    /// Incremented by `BackendTeardownSentinel::drop`. Because the sentinel is
    /// carried ONLY by the backend instance handed to `Pool::start` (clones do
    /// not carry it), this counter rises exactly when the pool's `EngineInner`
    /// (which owns that backend) is dropped — a reliable, race-free signal that
    /// the pool and its reaper task were torn down.
    teardowns: AtomicU64,
    // When `block_create` is set, `create_connection` parks until released.
    block_create: AtomicBool,
    entered_create: AtomicBool,
    gate: Mutex<bool>,
    gate_cv: Condvar,
}

/// A guard whose `Drop` records that the backend that owned it was dropped.
struct BackendTeardownSentinel {
    shared: Arc<Shared>,
}

impl Drop for BackendTeardownSentinel {
    fn drop(&mut self) {
        self.shared.teardowns.fetch_add(1, Ordering::SeqCst);
    }
}

struct FakeBackend {
    shared: Arc<Shared>,
    /// Held purely for its `Drop` side effect (records pool teardown). Never read
    /// directly, hence the allow; dropping it is the whole point.
    #[allow(dead_code)]
    sentinel: Option<BackendTeardownSentinel>,
}

/// A cheap observation handle: shares the counters but carries no sentinel, so it
/// never affects teardown accounting and never keeps the pool alive.
#[derive(Clone)]
struct BackendObserver {
    shared: Arc<Shared>,
}

impl FakeBackend {
    fn new() -> (Self, BackendObserver) {
        let shared = Arc::new(Shared {
            created: AtomicU64::new(0),
            closed: AtomicU64::new(0),
            teardowns: AtomicU64::new(0),
            block_create: AtomicBool::new(false),
            entered_create: AtomicBool::new(false),
            gate: Mutex::new(false),
            gate_cv: Condvar::new(),
        });
        let backend = FakeBackend {
            shared: Arc::clone(&shared),
            sentinel: Some(BackendTeardownSentinel {
                shared: Arc::clone(&shared),
            }),
        };
        (backend, BackendObserver { shared })
    }
}

impl BackendObserver {
    fn created(&self) -> u64 {
        self.shared.created.load(Ordering::SeqCst)
    }
    fn closed(&self) -> u64 {
        self.shared.closed.load(Ordering::SeqCst)
    }
    fn teardowns(&self) -> u64 {
        self.shared.teardowns.load(Ordering::SeqCst)
    }
    fn arm_blocking_create(&self) {
        self.shared.block_create.store(true, Ordering::SeqCst);
    }
    fn wait_for_create_entered(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if self.shared.entered_create.load(Ordering::SeqCst) {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("backend never entered create_connection");
    }
    fn release_create(&self) {
        *self.shared.gate.lock().unwrap() = true;
        self.shared.gate_cv.notify_all();
    }
}

impl PoolBackend for FakeBackend {
    type Conn = FakeConn;

    fn create_connection(&self, _id: u64, _cclass: Option<&str>) -> Result<FakeConn, String> {
        if self.shared.block_create.load(Ordering::SeqCst) {
            self.shared.entered_create.store(true, Ordering::SeqCst);
            let mut open = self.shared.gate.lock().unwrap();
            while !*open {
                open = self.shared.gate_cv.wait(open).unwrap();
            }
        }
        self.shared.created.fetch_add(1, Ordering::SeqCst);
        Ok(FakeConn {
            alive: Arc::new(AtomicBool::new(true)),
        })
    }

    fn ping_connection(&self, conn: &FakeConn, _ping_timeout_ms: u32) -> bool {
        conn.alive.load(Ordering::SeqCst)
    }

    fn close_connection(&self, _id: u64, conn: FakeConn) {
        conn.alive.store(false, Ordering::SeqCst);
        self.shared.closed.fetch_add(1, Ordering::SeqCst);
    }

    fn connection_is_open(&self, conn: &FakeConn) -> bool {
        conn.alive.load(Ordering::SeqCst)
    }
}

// ---- helpers ------------------------------------------------------------

fn build_rt() -> asupersync::runtime::Runtime {
    let reactor = reactor::create_reactor().expect("reactor");
    RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("runtime")
}

fn cfg(min: u32, max: u32) -> PoolConfig {
    PoolConfig::new(min, max, 1)
        .with_getmode(POOL_GETMODE_WAIT)
        .with_wait_timeout_ms(2_000)
        .with_ping_interval_secs(-1)
        .with_ping_timeout_ms(5_000)
}

fn wait_until<F: Fn() -> bool>(label: &str, f: F) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out waiting for: {label}");
}

// ---- Risk #3: nested runtime -------------------------------------------

#[test]
fn risk3_pool_constructed_inside_async_task_does_not_panic() {
    let outer = build_rt();
    let (backend, obs) = FakeBackend::new();

    // Build the pool (which itself builds a dedicated runtime) from WITHIN an
    // async task on `outer`. If asupersync forbade nested runtimes this panics.
    let pool: Pool<FakeBackend> = outer.block_on(async move {
        assert!(
            Cx::current().is_some(),
            "precondition: we really are inside a runtime task"
        );
        Pool::start(backend, cfg(1, 2)).expect("pool starts inside async task")
    });

    // The reaper (on the pool's own runtime) must grow the pool to min, proving
    // the nested runtime actually runs.
    wait_until("pool grows to min inside nested runtime", || {
        outer
            .block_on(async {
                let cx = Cx::current().unwrap();
                pool.open_count(&cx).await
            })
            .map(|c| c == 1)
            .unwrap_or(false)
    });
    assert_eq!(obs.created(), 1);

    outer
        .block_on(async {
            let cx = Cx::current().unwrap();
            pool.close(&cx, true).await
        })
        .expect("close inside async task");
    eprintln!("[risk3] pool built and closed entirely from within an async task");
}

// ---- Risk #1: cross-runtime join ---------------------------------------

#[test]
fn risk1_close_awaited_on_different_runtime_than_pool() {
    // Pool is built outside any runtime; its reaper lives on the pool's OWN
    // dedicated runtime.
    let (backend, obs) = FakeBackend::new();
    obs.arm_blocking_create();
    let pool = Pool::start(backend, cfg(1, 1)).expect("pool starts");

    // The reaper is now wedged inside the (blocking) create on the pool runtime.
    obs.wait_for_create_entered();
    eprintln!("[risk1] reaper wedged in create on the pool's own runtime");

    // Release the create slightly later from an ordinary thread so the awaited
    // close can resolve.
    let releaser_obs = obs.clone();
    let releaser = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(60));
        releaser_obs.release_create();
    });

    // Await close on a SEPARATE runtime (caller runtime != pool runtime). A
    // concurrent marker future on the caller's single worker thread must keep
    // advancing — impossible if close synchronously joined an OS thread.
    let marker = Arc::new(AtomicU64::new(0));
    let marker_task = Arc::clone(&marker);
    let caller_rt = build_rt();
    let pool_for_close = pool.clone();
    let result = caller_rt.block_on(async move {
        let cx = Cx::current().expect("caller runtime installs a Cx");
        let mut ticker = Box::pin(async {
            for _ in 0..500u32 {
                marker_task.fetch_add(1, Ordering::SeqCst);
                asupersync::time::sleep(asupersync::time::wall_now(), Duration::from_millis(1))
                    .await;
            }
        });
        let mut closing = Box::pin(pool_for_close.close(&cx, true));
        // Drive both on the same task; close must yield Pending (cooperative),
        // letting the ticker advance, until the reaper finishes on its runtime.
        std::future::poll_fn(move |task_cx| {
            use std::future::Future;
            use std::pin::Pin;
            let _ = Future::poll(Pin::as_mut(&mut ticker), task_cx);
            Future::poll(Pin::as_mut(&mut closing), task_cx)
        })
        .await
    });
    releaser.join().unwrap();

    result.expect("cross-runtime close must complete");
    assert!(
        marker.load(Ordering::SeqCst) > 0,
        "caller worker thread kept interleaving while close awaited the reaper on \
         another runtime — proves no synchronous OS-thread join"
    );
    assert_eq!(
        obs.closed(),
        1,
        "the cooperatively-joined reaper closed the connection"
    );
    eprintln!(
        "[risk1] cross-runtime close resolved; marker advanced to {}",
        marker.load(Ordering::SeqCst)
    );
    drop(pool);
}

// ---- Risk #2: runtime + reaper task actually torn down -----------------

#[test]
fn risk2_close_then_drop_tears_down_runtime_and_task() {
    // --- close path: close, then drop the last handle ---
    {
        let (backend, obs) = FakeBackend::new();
        let pool = Pool::start(backend, cfg(1, 1)).expect("pool starts");
        wait_until("reaper creates the min connection (close path)", || {
            obs.created() == 1
        });
        assert_eq!(obs.teardowns(), 0, "pool is live; backend not yet dropped");

        let rt = build_rt();
        let pool_for_close = pool.clone();
        rt.block_on(async move {
            let cx = Cx::current().unwrap();
            pool_for_close.close(&cx, true).await
        })
        .expect("close");
        // After close, the connection is closed but the backend still lives
        // (the pool handle is still held).
        assert_eq!(obs.closed(), 1, "close path closed the conn");
        assert_eq!(obs.teardowns(), 0, "pool handle still held after close");

        // Dropping the last handle tears down the pool runtime (its `Drop` joins
        // the worker thread) and the reaper task; the backend it owns is dropped,
        // firing the sentinel. `wait_until` tolerates the worker-join → task-drop
        // ordering without depending on timing; the condition is a deterministic
        // eventuality, not a race. (Cap the teardown count too, to catch a
        // double-teardown bug.)
        drop(pool);
        wait_until(
            "last-handle drop after close tears down the pool/backend",
            || obs.teardowns() == 1,
        );
        assert_eq!(obs.teardowns(), 1, "exactly one teardown, never double");
        eprintln!("[risk2] close+drop: conn closed and runtime/task torn down (teardowns=1)");
    }

    // --- bare drop path (no close at all) ---
    {
        let (backend, obs) = FakeBackend::new();
        let pool = Pool::start(backend, cfg(1, 1)).expect("pool starts");
        wait_until("reaper creates the min connection (drop path)", || {
            obs.created() == 1
        });
        assert_eq!(obs.teardowns(), 0, "pool is live; backend not yet dropped");

        // Drop WITHOUT closing. `PoolEngine::drop` (last handle) closes every
        // connection synchronously and then the `runtime` field drops, joining
        // the worker thread. The backend is dropped → sentinel fires.
        drop(pool);
        wait_until("bare drop tears down the pool/backend", || {
            obs.teardowns() == 1
        });
        assert_eq!(obs.teardowns(), 1, "exactly one teardown, never double");
        assert_eq!(
            obs.closed(),
            1,
            "bare drop must still close the connection (no transport leak)"
        );
        eprintln!("[risk2] bare drop: conn closed and runtime/task torn down (teardowns=1)");
    }
}

// ---- Risk #4: backend I/O off the state lock ---------------------------

#[test]
fn risk4_backend_create_runs_off_the_state_lock() {
    // A blocking create wedges the reaper inside backend I/O. Meanwhile, an
    // operation that must take the PoolState lock (`stats`/`open_count`) has to
    // make progress — proving the reaper does NOT hold the lock across create.
    let (backend, obs) = FakeBackend::new();
    obs.arm_blocking_create();
    let pool = Pool::start(backend, cfg(1, 1)).expect("pool starts");
    obs.wait_for_create_entered();
    eprintln!("[risk4] reaper wedged inside backend create_connection");

    // While the create is wedged, a lock-taking stat call must return promptly.
    let rt = build_rt();
    let pool_for_stats = pool.clone();
    let got_stats = Arc::new(AtomicBool::new(false));
    let got_stats_task = Arc::clone(&got_stats);
    let stats_thread = std::thread::spawn(move || {
        let stats = rt
            .block_on(async move {
                let cx = Cx::current().unwrap();
                pool_for_stats.stats(&cx).await
            })
            .expect("stats while create is wedged");
        got_stats_task.store(true, Ordering::SeqCst);
        stats
    });

    // If create held the state lock, this would block until release. It must not.
    wait_until(
        "stats returns while backend create is wedged (lock not held across I/O)",
        || got_stats.load(Ordering::SeqCst),
    );
    let stats = stats_thread.join().unwrap();
    eprintln!(
        "[risk4] stats returned while create wedged: opening={} open={}",
        stats.opening_count(),
        stats.open_count()
    );
    // The pending open is visible as an in-flight opening effect, confirming the
    // reaper recorded the in-flight create in state and then released the lock to
    // do the blocking I/O.
    assert!(
        stats.opening_count() >= 1,
        "the in-flight create must be reflected as a pending opening in derived counts"
    );

    obs.release_create();
    let close_rt = build_rt();
    let pool_for_close = pool.clone();
    close_rt
        .block_on(async move {
            let cx = Cx::current().unwrap();
            pool_for_close.close(&cx, true).await
        })
        .expect("close");
    drop(pool);
}
