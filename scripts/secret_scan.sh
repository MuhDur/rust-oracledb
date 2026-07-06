#!/usr/bin/env bash
# C4 release blocker: scan the tracked tree for confidential deployment identifiers.
#
# - Structural patterns (safe to publish) always run in CI.
# - Operator-specific literals live in a gitignored denylist (never committed).
# - Generic heuristics catch common secret shapes in tracked files.
#
# Usage:
#   bash scripts/secret_scan.sh           # full scan (exit 1 on any hit)
#   bash scripts/secret_scan.sh --self-test
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SELFTEST=false
if [[ "${1:-}" == --self-test ]]; then
  SELFTEST=true
fi

DEFAULT_DENYLIST="$ROOT/.secret_scan_denylist"
DENYLIST_FILE="${SECRET_SCAN_DENYLIST_FILE:-$DEFAULT_DENYLIST}"

STRUCTURAL_PATTERNS=(
  'CN=[^[:space:]]*\.oraclecloud\.com'
  'ocid1\.[a-z0-9]+\.[a-z0-9-]+\.[a-z0-9]+\.'
)

GENERIC_PATTERNS=(
  'BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY'
  'AKIA[0-9A-Z]{16}'
  'xox[baprs]-[0-9A-Za-z-]{10,}'
  'ghp_[0-9A-Za-z]{36,}'
  'gho_[0-9A-Za-z]{36,}'
  'github_pat_[0-9A-Za-z_]{20,}'
  '-----BEGIN CERTIFICATE-----'
  'oraclecloud\.com'
  'oraclevcn\.com'
  'adb\.[a-z0-9-]+\.oraclecloud\.com'
)

scan_paths() {
  if [[ -n "${SECRET_SCAN_SELFTEST_PATH:-}" ]]; then
    printf '%s\0' "$SECRET_SCAN_SELFTEST_PATH"
    return
  fi
  # The scanner's own ruleset files necessarily embed the very patterns (and a
  # self-test marker) they hunt for; scanning them would flag the scanner on
  # itself. Exclude them here so every phase (structural/denylist/generic) skips
  # them uniformly.
  if [[ -d .git ]]; then
    git ls-files -z
  else
    find . -type f \
      ! -path './.git/*' \
      ! -path './target/*' \
      ! -path './reference/*' \
      -print0
  fi | grep -zvE -e 'scripts/secret_scan\.sh$' -e '\.secret_scan_denylist\.example$'
}

run_structural_and_denylist() {
  local hits=0
  local pattern path

  for pattern in "${STRUCTURAL_PATTERNS[@]}"; do
    while IFS= read -r -d '' path; do
      [[ -f "$path" ]] || continue
      if grep -nE -- "$pattern" "$path" >/dev/null 2>&1; then
        echo "secret_scan: structural match ($pattern) in $path" >&2
        grep -nE -- "$pattern" "$path" | head -5 >&2 || true
        hits=$((hits + 1))
      fi
    done < <(scan_paths)
  done

  if [[ -f "$DENYLIST_FILE" ]]; then
    while IFS= read -r pattern || [[ -n "$pattern" ]]; do
      pattern="${pattern%%#*}"
      pattern="${pattern#"${pattern%%[![:space:]]*}"}"
      pattern="${pattern%"${pattern##*[![:space:]]}"}"
      [[ -z "$pattern" ]] && continue
      while IFS= read -r -d '' path; do
        [[ -f "$path" ]] || continue
        if grep -nE -- "$pattern" "$path" >/dev/null 2>&1; then
          echo "secret_scan: denylist match in $path (pattern from $DENYLIST_FILE)" >&2
          grep -nE -- "$pattern" "$path" | head -5 >&2 || true
          hits=$((hits + 1))
        fi
      done < <(scan_paths)
    done < "$DENYLIST_FILE"
  fi

  return "$hits"
}

should_skip_generic() {
  local path="$1"
  case "$path" in
    crates/oracledb/tests/fixtures/tls/*) return 0 ;;
    crates/oracledb-protocol/tests/tls_wallet.rs) return 0 ;;
  esac
  return 1
}

run_generic_heuristics() {
  local hits=0
  local pattern path

  for pattern in "${GENERIC_PATTERNS[@]}"; do
    while IFS= read -r -d '' path; do
      [[ -f "$path" ]] || continue
      if should_skip_generic "$path"; then
        continue
      fi
      if grep -nE -- "$pattern" "$path" >/dev/null 2>&1; then
        echo "secret_scan: generic match ($pattern) in $path" >&2
        grep -nE -- "$pattern" "$path" | head -5 >&2 || true
        hits=$((hits + 1))
      fi
    done < <(scan_paths)
  done

  return "$hits"
}

run_selftest() {
  local scratch
  scratch="$(mktemp)"
  trap 'rm -f "$scratch"' RETURN
  printf '%s\n' 'CN=scan-selftest.example.oraclecloud.com' >"$scratch"

  SECRET_SCAN_SELFTEST_PATH="$scratch"
  if run_structural_and_denylist; then
    echo "secret_scan: self-test FAILED (scanner did not fail on planted marker)" >&2
    unset SECRET_SCAN_SELFTEST_PATH
    return 1
  fi
  unset SECRET_SCAN_SELFTEST_PATH
  echo "secret_scan: self-test OK (planted marker trips structural scan)" >&2
  return 0
}

if $SELFTEST; then
  run_selftest
  exit $?
fi

hits=0
run_structural_and_denylist
r=$?
[[ $r -ne 0 ]] && hits=$((hits + r))

run_generic_heuristics
r=$?
[[ $r -ne 0 ]] && hits=$((hits + r))

if [[ "$hits" -gt 0 ]]; then
  echo "secret_scan: FAIL ($hits issue class(es))" >&2
  echo "Add operator literals only to $DEFAULT_DENYLIST (gitignored), never to the repo." >&2
  exit 1
fi

echo "secret_scan: OK (tracked tree)"