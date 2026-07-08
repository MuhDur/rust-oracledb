#!/usr/bin/env bash
# ThreadSanitizer cancel-safety campaign for the async driver (K10 / cancel paths).
#
# Builds the driver's concurrency-relevant tests with `-Zsanitizer=thread` over a
# `-Zbuild-std`-instrumented std and runs them, so a data race in the cross-thread
# cancel/recovery machinery aborts the run. Covered surfaces:
#
#   * Offline (always): the whole `oracledb` lib unit suite, which includes the
#     DPOR cancel/timeout-recovery saturation tests, the multi-threaded async
#     pool acquire/return/close/reap tests (real `std::thread::spawn`), the
#     streaming-cancel-mid-stream reuse test, and the OwnedRowStream drop path.
#   * Live (only when PYO_TEST_* is set): the real cross-thread BREAK + recovery
#     drain thread (`cancel_then_reuse`), call-timeout teardown recovery
#     (`reuse_after_call_timeout`), LOB stream cancel cleanup (`live_lob_stream`),
#     and the OwnedRowStream lifecycle (`live_owned_row_stream`).
#
# TSan finds nothing to instrument in `ring`'s hand-written assembly, so a race
# that lives purely inside a crypto primitive would be invisible here; the cancel
# and pool machinery this campaign targets is plain Rust `std::sync`/thread code,
# which build-std instruments fully.
#
# Requirements: nightly toolchain (rust-toolchain.toml) + the `rust-src`
# component (`rustup component add rust-src`). Honours CARGO_BUILD_JOBS.
#
# Usage:
#   scripts/tsan_campaign.sh                 # offline instrumented suite
#   PYO_TEST_CONNECT_STRING=... PYO_TEST_MAIN_USER=... PYO_TEST_MAIN_PASSWORD=... \
#     scripts/tsan_campaign.sh               # + live cancel/stream/LOB suite
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TOOLCHAIN="${TSAN_TOOLCHAIN:-nightly-2026-05-11}"
TARGET="${TSAN_TARGET:-x86_64-unknown-linux-gnu}"
TEST_THREADS="${TSAN_TEST_THREADS:-4}"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-16}"

# `-Zsanitizer=thread` instruments only the target build; with an explicit
# `--target`, cargo builds proc-macros / build scripts for the host WITHOUT these
# flags, so the sanitizer never leaks into a proc-macro (which cannot link it).
export RUSTFLAGS="${RUSTFLAGS:-} -Zsanitizer=thread"
# halt_on_error=0: report every race, don't stop at the first. exitcode=66: a
# race makes the test process exit non-zero so `cargo test` fails the gate.
export TSAN_OPTIONS="${TSAN_OPTIONS:-halt_on_error=0 exitcode=66 history_size=4}"

if ! rustup component list --toolchain "$TOOLCHAIN" 2>/dev/null | grep -q 'rust-src (installed)'; then
  echo "tsan-campaign: rust-src not installed for $TOOLCHAIN" >&2
  echo "  run: rustup component add rust-src --toolchain $TOOLCHAIN" >&2
  exit 1
fi

echo "tsan-campaign: toolchain=$TOOLCHAIN target=$TARGET jobs=$CARGO_BUILD_JOBS threads=$TEST_THREADS"

echo "tsan-campaign: [1/2] offline instrumented lib suite"
cargo "+$TOOLCHAIN" test -p oracledb --lib \
  -Zbuild-std --target "$TARGET" \
  -- --test-threads="$TEST_THREADS"

if [ -n "${PYO_TEST_CONNECT_STRING:-}" ] \
   && [ -n "${PYO_TEST_MAIN_USER:-}" ] \
   && [ -n "${PYO_TEST_MAIN_PASSWORD:-}" ]; then
  echo "tsan-campaign: [2/2] live cancel/teardown/LOB/stream suite ($PYO_TEST_CONNECT_STRING)"
  cargo "+$TOOLCHAIN" test -p oracledb \
    --test cancel_then_reuse \
    --test reuse_after_call_timeout \
    --test live_lob_stream \
    --test live_owned_row_stream \
    -Zbuild-std --target "$TARGET" \
    -- --include-ignored --test-threads=1
else
  echo "tsan-campaign: [2/2] live suite SKIPPED (PYO_TEST_* not set)"
fi

echo "tsan-campaign: OK — no data race reported under ThreadSanitizer"
