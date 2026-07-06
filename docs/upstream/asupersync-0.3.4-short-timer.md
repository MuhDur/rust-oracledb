# asupersync 0.3.4: after the first timer, sub-250ms timeouts are floored to ~250ms

**Affects:** `asupersync = "=0.3.4"` (also observed on 0.3.2). **Impact:** any code
that awaits more than one short (`< ~250ms`) `time::timeout` / `time::sleep` in a
runtime gets the wrong (much longer) delay on every timer after the first.
**Not** a hang and **not** a lost wakeup — the timers fire, but late.

> Note: an earlier internal draft of this report described the symptom as
> "subsequent short timers never fire / `block_on` hangs". That was produced with
> a runtime built **without** a reactor (`RuntimeBuilder::current_thread().build()`)
> and is **incorrect**. With a properly configured runtime (a reactor installed,
> as in real use) the timers fire — they are just floored to ~250ms. The accurate
> characterization is below.

## Summary

Under a `current_thread` runtime with a reactor, the **first** timer fires
accurately at its requested duration. **Every subsequent** timer in that runtime
has a **~250ms floor**: a request shorter than ~250ms fires at ~250ms instead of
its requested time; requests ≥ ~250ms are accurate. The first-timer exemption
and the ~250ms floor are deterministic and reproducible.

## Reproduction

```toml
# Cargo.toml
[dependencies]
asupersync = "=0.3.4"
```

```rust
// src/main.rs — nightly toolchain
use std::time::{Duration, Instant};
use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::time::{self, wall_now};

fn main() {
    let reactor = reactor::create_reactor().expect("reactor");
    let rt = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("runtime");
    rt.block_on(async {
        for (label, ms) in [("first", 50u64), ("second", 50), ("third", 50)] {
            let start = Instant::now();
            let _ = time::timeout(
                wall_now(),
                Duration::from_millis(ms),
                std::future::pending::<()>(),
            )
            .await;
            println!("{label}: requested {ms}ms, fired at {:?}", start.elapsed());
        }
    });
}
```

**Observed:**

```
first:  requested 50ms, fired at ~50ms      <- accurate
second: requested 50ms, fired at ~250ms     <- floored
third:  requested 50ms, fired at ~250ms     <- floored
```

**Expected:** all three fire at ~50ms.

### The floor, measured across durations

Warming with one timer, then measuring the **second** timer at various requested
durations (each in a fresh runtime):

| requested | 2nd timer fires at | verdict |
|-----------|--------------------|---------|
| 1 ms   | ~250 ms | floored |
| 10 ms  | ~250 ms | floored |
| 50 ms  | ~250 ms | floored |
| 200 ms | ~250 ms | ~floored |
| 300 ms | ~300 ms | accurate |
| 500 ms | ~500 ms | accurate |
| 1000 ms| ~1000 ms| accurate |

So the effect is a **~250ms lower bound on every timer after the first**, not a
fixed added delay and not a clamp of long timers (long timeouts are *not* fired
early — good). The first timer of each fresh runtime is exempt (accurate at any
duration, including 1ms).

## Hypothesis (unverified — observable behavior above is the ground truth)

It looks like the first timer registration arms the precise timer path, but
subsequent short-timer registrations do not re-arm the wheel to the nearer
deadline and instead wait for an existing ~250ms tick / coarse background poll.
Relevant area: `src/time/sleep.rs` `Sleep::poll` (timer-driver vs OS-thread
fallback selection and the wheel re-arm on `Poll::Pending`). This is a
hypothesis; please treat the repro + measurements as the authoritative report.

## Workaround in the consuming project (rust-oracledb)

Only short, repeated timers are affected, so for the one place we depend on a
short timer firing on time (the connection-pool `TIMEDWAIT` acquire) we bypass
the async timer entirely: `TimedAcquireDeadline`
(`crates/oracledb/src/pool/acquire.rs`) spawns a dedicated `std::thread` that
`park_timeout`s to the deadline and wakes the waiter. Seconds-scale connect/read
timeouts use `time::timeout` directly and are unaffected (≥ 250ms is accurate).
