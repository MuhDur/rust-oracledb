# Upstream bug report — asupersync 0.3.4: short timers fire only once per process

**Status:** drafted + reproduced locally, ready to file upstream (needs the
asupersync issue-tracker URL / submission access).
**Affects:** `asupersync = "=0.3.4"` (also observed 0.3.2). **Severity:** high for
any code that awaits more than one short timer in a process.
**Driver impact:** worked around — see "Our workaround" below. This document is
the reproduction + root-cause writeup to hand to the asupersync maintainers.

## Summary

Under a `current_thread` runtime, a short `time::timeout` / `time::sleep`
delivers its wakeup **only for the first timed use in the process**. The second
and subsequent short timers never fire, so the awaiting task hangs forever. The
failure is deterministic (1st-pass / 2nd-hang), not a race.

## Minimal reproduction

```toml
# Cargo.toml
[dependencies]
asupersync = "=0.3.4"
```

```rust
// src/main.rs — nightly toolchain (asupersync requires nightly)
use std::time::{Duration, Instant};
use asupersync::runtime::RuntimeBuilder;
use asupersync::time::{self, wall_now};

fn main() {
    let worker = std::thread::spawn(|| {
        let rt = RuntimeBuilder::current_thread().build().expect("build runtime");
        rt.block_on(async {
            for i in 0..3u32 {
                let start = Instant::now();
                // A pending future that can only resolve when the timeout fires.
                let _ = time::timeout(
                    wall_now(),
                    Duration::from_millis(50),
                    std::future::pending::<()>(),
                )
                .await;
                eprintln!("timer {i} fired after {:?}", start.elapsed());
            }
        });
    });

    // Independent OS-thread watchdog: 3 × 50 ms must finish well under 3 s.
    let start = Instant::now();
    loop {
        if worker.is_finished() {
            println!("RESULT: all timers fired (no bug)");
            return;
        }
        if start.elapsed() > Duration::from_secs(3) {
            println!("RESULT: BUG — a short timer after the first never fired");
            std::process::exit(1);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}
```

**Observed:** `RESULT: BUG — a short timer after the first never fired`
(`block_on` hangs; the loop never gets past `timer 1`).
**Expected:** three lines `timer 0/1/2 fired after ~50ms`, then
`RESULT: all timers fired`.

## Root cause (source-level, asupersync-0.3.4)

`Sleep::poll` (`src/time/sleep.rs`) prefers a bound or ambient
`TimerDriverHandle` over the background OS-thread fallback:

- On `Poll::Pending`, when a `timer_driver` is present it registers the deadline
  with the driver and **stops/discards any existing fallback thread**
  (`sleep.rs` ~535: `if let Some(fallback) = state.fallback.take() { request_stop_fallback(..) }`).
- The OS-thread fallback (`sleep.rs` ~624, `if state.fallback.is_none() { spawn … }`)
  is therefore only used when **no** driver exists.
- `Sleep::with_time_getter` does **not** force the fallback when an ambient
  driver is present (`sleep.rs:483/535/624`, `timeout_future.rs:231`).

So `Sleep` inherits the ambient runtime timer driver's wheel behaviour, and that
wheel appears to arm/deliver only the **first** short timer registered in the
process; subsequent short registrations are accepted but never fire. Because
`Sleep` has handed ownership to the (buggy) driver and torn down the fallback
thread, there is no independent path to wake the task.

Two independently-actionable fixes for the maintainers to consider:

1. Fix the timer-driver wheel so repeated short deadlines re-arm and fire
   (the actual defect); and/or
2. Make `Sleep`/`timeout` fall back to the OS-thread timer when the driver does
   not confirm the registration will fire within the requested short window —
   i.e. don't tear down the fallback until the driver has demonstrably armed the
   timer (defence in depth so a driver wheel bug cannot silently hang tasks).

## Our workaround (rust-oracledb)

For the pool `TIMEDWAIT` acquire — the one place we depend on a **short** timer
firing repeatedly — we do not rely on the asupersync wheel. `TimedAcquireDeadline`
(`crates/oracledb/src/pool/acquire.rs`) spawns a dedicated `std::thread` that
`park_timeout`s until the deadline and wakes the waiter directly, i.e. it forces
the OS-thread timer path that `Sleep` would otherwise have used only for the
first timer. The driver's pool and live timeout suites are green with this in
place. Long timers (connect/read timeouts, seconds-scale) are unaffected by the
bug and continue to use `time::timeout` directly.
