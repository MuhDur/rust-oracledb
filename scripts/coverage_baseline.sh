#!/usr/bin/env bash
# Empirical cargo-llvm-cov coverage baseline for the oracledb driver workspace
# (bead oraclemcp-eng-program-bp8ia.5.1, driver half of D1).
#
# THIS IS THE FOUNDATION ONLY: a reproducible line/region/function coverage
# measurement plus a committed baseline. There is NO ratchet, NO changed-line
# diff gate, and NO per-crate mutation floor here -- that ranking/gating logic
# is a separate follow-up. This driver already has a mutation gate
# (scripts/mutation_gate.py) and an async-blocking coverage gate; this script
# is orthogonal to both. It only answers "what does line/region/function
# coverage measure RIGHT NOW", empirically, per crate -- see
# tests/coverage/BASELINE.md for the numbers.
#
# What is measured (read this before trusting a number in the baseline):
#   - LINE, REGION, and FUNCTION coverage from cargo-llvm-cov's JSON export
#     (`--json --summary-only`), aggregated per crate by source path
#     (crates/<crate>/src/...) plus a workspace TOTAL row.
#   - Default Cargo features only, matching the driver's DEFAULT CI test lane
#     `cargo test --workspace --exclude oracledb-pyshim` (.github/workflows/ci.yml):
#       * oracledb-pyshim is EXCLUDED. It is a PyO3 `extension-module`
#         (abi3-py310) cdylib: its test binary cannot link libpython in a plain
#         `cargo test`, which is exactly why CI excludes it from the default
#         lane. Including it would fail the whole instrumented run, so we mirror
#         CI and leave it out. A pyshim coverage baseline is a documented
#         follow-up, not silently folded in here.
#       * The `cassette` feature (a separate `cargo test -p oracledb --features
#         cassette` CI lane) and the live-DB / version-matrix suites are OUT of
#         scope. This run sets NO `ORACLEDB_*` / `PYO_TEST_*` env, so the
#         live-gated tests self-skip or stay `#[ignore]`d: the run is hermetic
#         (no database), matching the default CI lane.
#   - cargo-llvm-cov already scopes its report to each workspace member's own
#     `src/`: dependency source, integration-test files (crates/*/tests/*.rs),
#     fuzz targets, and examples never appear in the export, so this is
#     source-line coverage, not "how much of the test suite ran". The
#     per-crate-sum-reconciles-to-total sanity check in coverage_baseline.py
#     verifies this empirically on every run.
#   - Doctests are excluded: `--doctests` is unstable in the pinned
#     cargo-llvm-cov and slow; `cargo test --workspace --doc` is a separate,
#     existing CI lane and not part of this measurement.
#
# Scope note: this covers the rust-oracledb (oracledb) driver workspace only.
# The server (oraclemcp, a separate repo) has its own baseline.
#
# Modes:
#   scripts/coverage_baseline.sh            Run the full instrumented build +
#                                            test pass and overwrite the
#                                            committed baseline
#                                            (tests/coverage/BASELINE.json,
#                                            tests/coverage/BASELINE.md).
#   scripts/coverage_baseline.sh --check    Structural validation ONLY: the
#                                            committed baseline exists, is
#                                            well-formed, and matches its own
#                                            recorded schema. This does NOT
#                                            re-run coverage and does NOT detect
#                                            that the numbers have drifted from
#                                            HEAD -- that drift check is a
#                                            separate ratchet, to be built on
#                                            top of this.
#
# Prerequisites: the `cargo-llvm-cov` cargo subcommand plus the `llvm-tools`
# rustup component for the pinned toolchain (rust-toolchain.toml). This script
# fails closed with the exact install command when either is missing, rather
# than fabricating numbers:
#   cargo install cargo-llvm-cov
#   rustup component add llvm-tools --toolchain nightly-2026-05-11
#
# This is a heavy, slow, INSTRUMENTED build -- run it deliberately, never
# per-PR. Cap the build on a shared host: this driver has an OOM history, so
# export CARGO_BUILD_JOBS (e.g. 4) and point CARGO_TARGET_DIR at a dedicated
# directory (e.g. target-cov) so you never clobber the shared target/ cache and
# never contend with another concurrent build:
#   CARGO_BUILD_JOBS=4 CARGO_TARGET_DIR=target-cov scripts/coverage_baseline.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="$ROOT/tests/coverage"

MODE="write"
case "${1:-}" in
  ""|--write) MODE="write" ;;
  --check) MODE="check" ;;
  -h|--help)
    grep '^#' "$0" | sed 's/^# \{0,1\}//'
    exit 0 ;;
  *) echo "coverage_baseline: unknown argument: $1" >&2; exit 2 ;;
esac

if [ "$MODE" = "check" ]; then
  exec python3 "$ROOT/scripts/coverage_baseline.py" check --out-dir "$OUT_DIR"
fi

if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
  cat >&2 <<'EOF'
coverage_baseline: cargo-llvm-cov is not installed.

Install it (and the llvm-tools component for the pinned toolchain) with:
  cargo install cargo-llvm-cov
  rustup component add llvm-tools --toolchain nightly-2026-05-11

Then re-run: scripts/coverage_baseline.sh
EOF
  exit 2
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
raw_json="$tmpdir/raw-llvm-cov.json"

CMD=(cargo llvm-cov --workspace --exclude oracledb-pyshim --locked --summary-only --json --output-path "$raw_json")
echo "coverage_baseline: running: ${CMD[*]}" >&2
echo "coverage_baseline: this is a full instrumented workspace build + test pass; it is slow by design, be patient." >&2
"${CMD[@]}"

mkdir -p "$OUT_DIR"
python3 "$ROOT/scripts/coverage_baseline.py" generate \
  --raw "$raw_json" \
  --out-dir "$OUT_DIR" \
  --command "${CMD[*]}"

echo "coverage_baseline: wrote $OUT_DIR/BASELINE.json and $OUT_DIR/BASELINE.md" >&2
