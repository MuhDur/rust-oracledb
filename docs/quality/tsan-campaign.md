# ThreadSanitizer Cancel-Safety Campaign

Date: 2026-07-08

## Scope

This campaign runs the driver's async **cancel and concurrency** paths under
ThreadSanitizer (TSan) to surface data races in the cross-thread machinery:

- **Query cancellation** — the explicit `Connection::cancel` break+drain and the
  drop-cancel auto-drain (`CancelDrainGuard`), which interact with a background
  recovery-drain thread.
- **Call-timeout teardown** — `recover_from_call_timeout` break+drain after a
  deadline, and connection reuse afterward.
- **Stream / LOB cancel cleanup** — mid-stream cancel of the borrowed-fetch path
  and LOB locator streaming.
- **Connection teardown races** — the async pool's acquire / return / close /
  reap paths, which use real `std::thread::spawn` and shared pool state.
- **The K10 `OwnedRowStream` drop path** — dropping the owning stream mid-page
  (idle and after early stop), and its move-out/move-back connection handoff.

The driver's concurrency model is asupersync `current_thread` (≈2 OS threads per
lane) plus a background recovery-drain thread and a separate BREAK-sending
thread for two-thread cancel. Those real threads sharing connection / pool state
are exactly what TSan instruments here.

## Build recipe

TSan needs an instrumented std, so the campaign uses `-Zbuild-std` on the pinned
nightly with the `rust-src` component:

```text
RUSTFLAGS="-Zsanitizer=thread"
TSAN_OPTIONS="halt_on_error=0 exitcode=66 history_size=4"
cargo +nightly-2026-05-11 test -p oracledb --lib \
  -Zbuild-std --target x86_64-unknown-linux-gnu -- --test-threads=4
```

- `--target x86_64-unknown-linux-gnu` keeps proc-macros / build scripts on the
  host build (uninstrumented) — a proc-macro cannot link the TSan runtime, so
  the explicit target is what makes the instrumented build succeed.
- `exitcode=66` makes any reported race fail the `cargo test` process, so the
  campaign is a real gate, not a log-only run.
- All heavy runs were memory-capped:
  `systemd-run --user --scope -q -p MemoryMax=16G -p MemorySwapMax=0`, with
  `CARGO_TARGET_DIR=/home/durakovic/.cache/cargo-target-driver` and
  `CARGO_BUILD_JOBS=16`.

The one-command entry point is [`scripts/tsan_campaign.sh`](../../scripts/tsan_campaign.sh):
it runs the offline instrumented suite always, and the live suite when
`PYO_TEST_*` is exported.

## What ran

Instrumented build: clean, ~2m11s for the full tree (asupersync, rustls, ring,
protocol, driver) plus std.

### Offline (no database) — always

The entire `oracledb` lib unit suite ran instrumented. It includes the
concurrency-critical tests:

- `tests::dpor_wire_cancel_and_timeout_recovery_saturates` — DPOR over the wire
  cancel + timeout-recovery interleavings.
- `pool::tests::dpor_pool_async_waiter_release_close_and_timeout_saturate` — DPOR
  over the async pool waiter/release/close/timeout interleavings.
- `pool::tests::*` multi-threaded tests that spawn real returner / acquire /
  closer / releaser threads against shared pool state.
- `tests::streaming_cancel_mid_stream_leaves_connection_reusable`.
- `tests::a11_inactivity_deadline_fires_on_a_silent_server` and the
  transport/recovery timeout tests.
- `row_stream::tests::*` — the OwnedRowStream buffer/seed/drop/poison paths.

Result: **239 passed, 0 failed, 0 races** (`--test-threads=4`).

### Live (gvenzl / container) — when `PYO_TEST_*` is set

Run against free23 (`localhost:1522/FREEPDB1`, `pythontest`), instrumented, with
`--include-ignored --test-threads=1`:

| Test binary | Tests | Surface |
| --- | ---: | --- |
| `cancel_then_reuse` | 1 | explicit cancel + drop-cancel auto-drain, real BREAK + recovery-drain thread |
| `reuse_after_call_timeout` | 1 | call-timeout break+drain teardown, reuse |
| `live_lob_stream` | 2 | CLOB/BLOB locator streaming cancel cleanup |
| `live_owned_row_stream` | 6 | OwnedRowStream drain / multipage / mid-stream error / early-stop recovery / drop |

Result: **10 passed, 0 failed, 0 races.**

## Results

**No data race was reported by ThreadSanitizer** across the offline (239) or live
(10) instrumented runs. `exitcode=66` was armed throughout, so a clean pass is a
positive result, not a suppressed one.

Because no race was found, there is **no new regression test** to convert from a
reproduction — the existing DPOR cancel/timeout saturation tests, the
multi-threaded pool tests, and the new `live_owned_row_stream` drop/early-stop
tests already pin the cancel-safety behavior and now double as the standing TSan
corpus.

## Honest caveats

- **`ring` assembly is not instrumented.** TSan cannot see hand-written assembly,
  so a race living entirely inside a crypto primitive would be invisible here.
  The cancel / pool / stream machinery this campaign targets is plain Rust
  `std::sync` + thread code, which `-Zbuild-std` instruments fully.
- **CI runs the offline half only.** The `.github/workflows/tsan.yml` nightly /
  manual lane runs the deterministic offline instrumented suite (no database
  dependency, no flakiness). The live half is run from the same script locally or
  on the live lane by exporting `PYO_TEST_*`; it was run green for this report on
  free23 and xe21 (`localhost:1520/XEPDB1`, `testuser`).
- **Interleaving coverage is best-effort, not exhaustive.** TSan observes the
  interleavings that actually occur in a run; it is a dynamic race detector, not
  a model checker. The DPOR tests (which *do* enumerate interleavings) complement
  it and run on every PR in the ordinary `cargo test` job.

## Reproduce

```bash
# offline instrumented suite
scripts/tsan_campaign.sh

# + live suite against a container
PYO_TEST_CONNECT_STRING=localhost:1522/FREEPDB1 \
PYO_TEST_MAIN_USER=pythontest \
PYO_TEST_MAIN_PASSWORD=pythontest \
  scripts/tsan_campaign.sh
```
