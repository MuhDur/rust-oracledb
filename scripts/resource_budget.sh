#!/usr/bin/env bash
# Run a heavy command under a declared, ENFORCED resource budget (bead f1cl.9).
#
#   scripts/resource_budget.sh --profile build -- cargo test --workspace
#   scripts/resource_budget.sh --profile mutants --emit-budget   # JSON only, runs nothing
#
# Why this exists
# ---------------
# Three incidents, one shape: a heavy run helps itself to the whole box.
#
#   ~40GB RSS global OOM.
#   cargo-mutants fanned out to ~9,700 threads and exhausted the cgroup's 512-task
#   limit, so even fork() failed; a "safe" 4-shard rerun re-created the lockout.
#   2026-07-16: eight concurrent --workspace builds filled the 124G tmpfs behind
#   CARGO_TARGET_DIR and wedged the machine for every agent.
#
# The retrospective's finding, which this encodes: **a memory cap alone was not
# enough** — the PID/task budget was the scarce resource. So every budget here
# declares memory AND tasks AND an isolated target dir, and the kernel enforces
# all three.
#
# Mechanism: systemd-run --user --scope, i.e. a real cgroup v2 scope per run.
#
# NOT ulimit -u. RLIMIT_NPROC is per-UID, not per-process-tree. Every agent in
# this swarm runs as the same user, so `ulimit -u 256` would cap against the
# other agents' processes too: your build would fail to fork because someone
# else's test suite was busy. It is also one-way (a lowered limit cannot be
# raised back). A cgroup scope binds this run's subtree and nothing else.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Base for isolated per-run target dirs. MUST be disk-backed; see the tmpfs guard.
BUDGET_BASE="${ORACLEDB_BUDGET_BASE:-$HOME/.cache/oracledb-budget-runs}"

usage() {
  cat >&2 <<'EOF'
usage: resource_budget.sh --profile <name> [--run-id ID] [--emit-budget] [-- CMD...]

  --profile NAME   one of: build, test, mutants, live
  --run-id ID      name the run (default: profile-$$); fixes the target dir
  --emit-budget    print the resource_budget JSON and exit without running
  -- CMD...        the command to run under the budget

profiles (memory_max_bytes / pid_task_max):
  build     16G / 512    cargo build|check|clippy
  test      16G / 512    cargo test
  mutants    8G / 256    cargo-mutants: the incident profile. Deliberately the
                         tightest task budget -- ~9,700 threads is what broke the
                         box, and mutants is the tool that did it.
  live       8G / 256    live/container-backed suites
EOF
  exit 64
}

PROFILE=""
RUN_ID=""
EMIT_ONLY=false
CMD=()

while [ $# -gt 0 ]; do
  case "$1" in
    --profile) PROFILE="${2:-}"; shift 2 ;;
    --run-id)  RUN_ID="${2:-}"; shift 2 ;;
    --emit-budget) EMIT_ONLY=true; shift ;;
    --help|-h) usage ;;
    --) shift; CMD=("$@"); break ;;
    *) echo "resource_budget: unknown argument: $1" >&2; usage ;;
  esac
done

[ -n "$PROFILE" ] || usage

case "$PROFILE" in
  build)   MEM_BYTES=$((16 * 1024 * 1024 * 1024)); TASKS=512 ;;
  test)    MEM_BYTES=$((16 * 1024 * 1024 * 1024)); TASKS=512 ;;
  mutants) MEM_BYTES=$((8 * 1024 * 1024 * 1024));  TASKS=256 ;;
  live)    MEM_BYTES=$((8 * 1024 * 1024 * 1024));  TASKS=256 ;;
  *) echo "resource_budget: unknown profile: $PROFILE" >&2; usage ;;
esac

RUN_ID="${RUN_ID:-${PROFILE}-$$}"
TARGET_DIR="$BUDGET_BASE/$RUN_ID/target"

mkdir -p "$TARGET_DIR"

# ---------------------------------------------------------------------------
# Fail-closed: the target dir must not be on tmpfs.
#
# This is 2026-07-16 encoded as a check rather than a docs paragraph. tmpfs is
# RAM: a 73G build cache there is 73G of memory the box cannot use, and when it
# fills, the failure is EDQUOT and a wedged machine, not a clean "disk full".
# Refuse rather than "warn": a warning on a heavy run is a warning nobody reads.
# ---------------------------------------------------------------------------
fstype="$(stat -f -c %T "$TARGET_DIR")"
if [ "$fstype" = "tmpfs" ] || [ "$fstype" = "ramfs" ]; then
  cat >&2 <<EOF
resource_budget: REFUSING to run.

  target dir : $TARGET_DIR
  filesystem : $fstype  (RAM-backed)

A build cache on tmpfs is build artifacts stored in RAM. On 2026-07-16 that
filled a 124G tmpfs and wedged the box for every agent: writes returned EDQUOT,
the linker died with SIGBUS, and commands that produced output failed with no
output at all.

Point ORACLEDB_BUDGET_BASE at a disk-backed path.
EOF
  exit 78
fi

if $EMIT_ONLY; then
  # The exact resource_budget block required by required-proof/v1 and
  # mutation-result/v1, so a proof embeds the budget it actually ran under
  # rather than a number someone typed.
  printf '{\n  "isolated_target_dir": "%s",\n  "memory_max_bytes": %s,\n  "pid_task_max": %s\n}\n' \
    "$TARGET_DIR" "$MEM_BYTES" "$TASKS"
  exit 0
fi

[ "${#CMD[@]}" -gt 0 ] || { echo "resource_budget: no command given" >&2; usage; }

if ! command -v systemd-run >/dev/null 2>&1; then
  echo "resource_budget: systemd-run is required to enforce the budget; refusing to run unbounded" >&2
  exit 78
fi

echo "resource_budget: profile=$PROFILE tasks=$TASKS memory=$((MEM_BYTES / 1024 / 1024 / 1024))G" >&2
echo "resource_budget: isolated target dir=$TARGET_DIR ($fstype)" >&2

# --scope runs the command in a transient cgroup scope in this session.
# --collect reaps the unit even when the command fails, so a failed run does not
# leave a unit behind and break the next one.
# CARGO_TARGET_DIR is exported into the scope: an isolated target dir is part of
# the budget, not a suggestion, and it is what makes a run's inputs attributable.
exec systemd-run --user --scope --quiet --collect \
  -p "TasksMax=$TASKS" \
  -p "MemoryMax=$MEM_BYTES" \
  --setenv=CARGO_TARGET_DIR="$TARGET_DIR" \
  -- "${CMD[@]}"
