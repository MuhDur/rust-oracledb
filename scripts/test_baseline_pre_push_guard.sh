#!/usr/bin/env bash
# Required contract test for the versioned baseline pre-push guard.
#
# It proves both directions against a real disposable bare remote:
#   1. 5ed87b0's source-only session-serial change is rejected.
#   2. Regenerating docs/baseline makes the identical push succeed.
#   3. An unrelated 34-line layout shift succeeds without a baseline update.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SESSION_SERIAL_COMMIT="5ed87b09ef39bb70ab7dd7bd838871b936285c52"

fail() {
  echo "baseline-pre-push-guard: FAIL: $*" >&2
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required command: $1"
}

for command in git rg; do
  need "$command"
done

git -C "$ROOT" rev-parse --verify "$SESSION_SERIAL_COMMIT^{commit}" >/dev/null || \
  fail "required regression commit $SESSION_SERIAL_COMMIT is unavailable"
[[ -x "$ROOT/.githooks/pre-push" ]] || fail "missing executable .githooks/pre-push"
[[ -x "$ROOT/scripts/install_git_hooks.sh" ]] || fail "missing executable scripts/install_git_hooks.sh"

scratch="$(mktemp -d "${TMPDIR:-/tmp}/oracledb-baseline-pre-push-guard.XXXXXX")"
if [[ "${ORACLEDB_GUARD_TEST_KEEP_DIR:-0}" == "1" ]]; then
  echo "baseline-pre-push-guard: preserving fixture at $scratch"
else
  trap 'rm -rf -- "$scratch"' EXIT
fi

fixture="$scratch/fixture"
remote="$scratch/remote.git"
git clone --no-hardlinks "$ROOT" "$fixture" >/dev/null
git -C "$fixture" config user.name "baseline-pre-push guard test"
git -C "$fixture" config user.email "baseline-pre-push-guard@example.invalid"

# Start from a source state that predates the session-serial correction while
# retaining the current hook implementation. Reverting then cherry-picking the
# exact historical commit proves the same missing-baseline failure CI saw.
git -C "$fixture" revert --no-commit --no-edit "$SESSION_SERIAL_COMMIT"
(cd "$fixture" && scripts/gen_baseline.sh)
git -C "$fixture" add -- crates/oracledb/src/lib.rs docs/baseline
git -C "$fixture" commit --no-verify -m "test: seed pre-session-serial baseline" >/dev/null

git init --bare "$remote" >/dev/null
git -C "$fixture" remote add guard "$remote"
git -C "$fixture" push guard HEAD:refs/heads/main >/dev/null
seed_sha="$(git -C "$fixture" rev-parse HEAD)"

# A clone with no local hooks configuration must report NOT INSTALLED, rather
# than allowing a missing hook to look like a passing check.
if "$fixture/scripts/install_git_hooks.sh" --check >"$scratch/uninstalled.log" 2>&1; then
  fail "uninstalled hook incorrectly reported success"
fi
rg -q '^git-hooks: NOT INSTALLED:' "$scratch/uninstalled.log" || \
  fail "uninstalled hook diagnostic did not say NOT INSTALLED"
"$fixture/scripts/install_git_hooks.sh" >/dev/null
"$fixture/scripts/install_git_hooks.sh" --check >/dev/null

# Leg 1: replay 5ed87b0 without a regenerated baseline. The hook must reject
# the actual push and leave the bare remote at the seed commit.
git -C "$fixture" cherry-pick --no-commit "$SESSION_SERIAL_COMMIT"
git -C "$fixture" add -- crates/oracledb/src/lib.rs
git -C "$fixture" commit --no-verify -m "test: replay 5ed87b0 without baseline" >/dev/null
rejected_sha="$(git -C "$fixture" rev-parse HEAD)"
if ORACLEDB_HOOK_KEEP_SCRATCH="${ORACLEDB_GUARD_TEST_KEEP_DIR:-0}" \
  git -C "$fixture" push guard HEAD:refs/heads/main >"$scratch/rejected.log" 2>&1; then
  fail "source-only replay $rejected_sha unexpectedly pushed"
fi
[[ "$(git --git-dir="$remote" rev-parse refs/heads/main)" == "$seed_sha" ]] || \
  fail "rejected push advanced the disposable remote"
rg -q '^baseline pre-push: REJECTED stale derived baseline$' "$scratch/rejected.log" || \
  fail "rejected push did not emit the stale-baseline diagnosis"
echo "baseline-pre-push-guard: leg 1 OK — source-only 5ed87b0 replay rejected; remote unchanged"

# Leg 2: regenerate and commit every derived artifact, then make the same push.
(cd "$fixture" && scripts/gen_baseline.sh)
if git -C "$fixture" diff --quiet -- docs/baseline; then
  fail "regenerating after 5ed87b0 replay changed no baseline artifact"
fi
git -C "$fixture" add -- docs/baseline
git -C "$fixture" commit --no-verify -m "test: regenerate baseline after 5ed87b0 replay" >/dev/null
git -C "$fixture" push guard HEAD:refs/heads/main >"$scratch/regenerated.log" 2>&1
regenerated_sha="$(git -C "$fixture" rev-parse HEAD)"
[[ "$(git --git-dir="$remote" rev-parse refs/heads/main)" == "$regenerated_sha" ]] || \
  fail "regenerated baseline push did not advance the disposable remote"
rg -q '^baseline pre-push: OK$' "$scratch/regenerated.log" || \
  fail "regenerated baseline push did not run and pass the hook"
echo "baseline-pre-push-guard: leg 2 OK — regenerated baseline push accepted"

# Leg 3: 34 comment-only lines above every declaration change source locations
# but not semantic public declarations. The baseline must remain unchanged and
# the real push must pass.
git -C "$fixture" apply --whitespace=nowarn - <<'PATCH'
diff --git a/crates/oracledb/src/lib.rs b/crates/oracledb/src/lib.rs
--- a/crates/oracledb/src/lib.rs
+++ b/crates/oracledb/src/lib.rs
@@ -1,2 +1,36 @@
+// baseline pre-push layout fixture 01
+// baseline pre-push layout fixture 02
+// baseline pre-push layout fixture 03
+// baseline pre-push layout fixture 04
+// baseline pre-push layout fixture 05
+// baseline pre-push layout fixture 06
+// baseline pre-push layout fixture 07
+// baseline pre-push layout fixture 08
+// baseline pre-push layout fixture 09
+// baseline pre-push layout fixture 10
+// baseline pre-push layout fixture 11
+// baseline pre-push layout fixture 12
+// baseline pre-push layout fixture 13
+// baseline pre-push layout fixture 14
+// baseline pre-push layout fixture 15
+// baseline pre-push layout fixture 16
+// baseline pre-push layout fixture 17
+// baseline pre-push layout fixture 18
+// baseline pre-push layout fixture 19
+// baseline pre-push layout fixture 20
+// baseline pre-push layout fixture 21
+// baseline pre-push layout fixture 22
+// baseline pre-push layout fixture 23
+// baseline pre-push layout fixture 24
+// baseline pre-push layout fixture 25
+// baseline pre-push layout fixture 26
+// baseline pre-push layout fixture 27
+// baseline pre-push layout fixture 28
+// baseline pre-push layout fixture 29
+// baseline pre-push layout fixture 30
+// baseline pre-push layout fixture 31
+// baseline pre-push layout fixture 32
+// baseline pre-push layout fixture 33
+// baseline pre-push layout fixture 34
 // Unit-test assertions intentionally panic on invariant violations.
 #![cfg_attr(test, allow(clippy::unwrap_used))]
PATCH
git -C "$fixture" add -- crates/oracledb/src/lib.rs
git -C "$fixture" commit --no-verify -m "test: shift layout without changing declarations" >/dev/null
if ! git -C "$fixture" diff --quiet HEAD^ -- docs/baseline; then
  fail "layout-only change unexpectedly modified docs/baseline"
fi
git -C "$fixture" push guard HEAD:refs/heads/main >"$scratch/layout.log" 2>&1
layout_sha="$(git -C "$fixture" rev-parse HEAD)"
[[ "$(git --git-dir="$remote" rev-parse refs/heads/main)" == "$layout_sha" ]] || \
  fail "layout-only push did not advance the disposable remote"
rg -q '^baseline pre-push: OK$' "$scratch/layout.log" || \
  fail "layout-only push did not run and pass the hook"
echo "baseline-pre-push-guard: leg 3 OK — 34-line layout-only push accepted without baseline update"
echo "baseline-pre-push-guard: OK"
