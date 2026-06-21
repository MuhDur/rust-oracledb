#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REFERENCE_DIR="${ORACLEDB_REFERENCE_DIR:-$ROOT/reference/python-oracledb}"
FILTER_FILE="${ORACLEDB_FILTER_FILE:-$ROOT/harness/filter.txt}"
BASELINE_DIR="${ORACLEDB_BASELINE_DIR:-$ROOT/harness/.baseline}"
RESULTS_DIR="${ORACLEDB_RESULTS_DIR:-$ROOT/harness/.results}"
if [ -n "${ORACLEDB_VENV_DIR:-}" ] && [ -x "$ORACLEDB_VENV_DIR/bin/python" ]; then
  PYTHON_BIN="$ORACLEDB_VENV_DIR/bin/python"
elif [ -n "${PYTHON:-}" ]; then
  PYTHON_BIN="$PYTHON"
elif [ -x "$ROOT/.venv-py313/bin/python" ]; then
  PYTHON_BIN="$ROOT/.venv-py313/bin/python"
elif [ -x "$ROOT/.venv/bin/python" ]; then
  PYTHON_BIN="$ROOT/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

usage() {
  printf 'usage: %s baseline|rust|diff|list\n' "$0" >&2
}

require_reference() {
  if [ ! -d "$REFERENCE_DIR/tests" ]; then
    printf 'reference checkout not found at %s\n' "$REFERENCE_DIR" >&2
    printf 'run scripts/pin-reference.sh first\n' >&2
    exit 2
  fi
}

selected_tests() {
  "$PYTHON_BIN" "$ROOT/scripts/select_tests.py" \
    --reference "$REFERENCE_DIR" \
    --filter "$FILTER_FILE"
}

run_pytest() {
  local manifest_path="$1"
  shift
  mapfile -t tests < <(selected_tests)
  mkdir -p "$(dirname "$manifest_path")"
  "$PYTHON_BIN" -m pytest \
    "${tests[@]}" \
    --tb=short \
    --json-report \
    --json-report-file "$manifest_path" \
    "$@"
}

run_pytest_segmented() {
  local manifest_path="$1"
  shift
  local manifest_dir
  local manifest_name
  local parts_dir
  local exit_code=0
  local counter=0
  local part_path
  local test_path
  local -a reports=()
  local -a tests=()

  mapfile -t tests < <(selected_tests)
  manifest_dir="$(dirname "$manifest_path")"
  manifest_name="$(basename "$manifest_path" .json)"
  parts_dir="$manifest_dir/parts-$manifest_name-$(date -u +%Y%m%dT%H%M%SZ)-$$"
  mkdir -p "$parts_dir"

  for test_path in "${tests[@]}"; do
    counter=$((counter + 1))
    part_path="$(printf '%s/%03d-%s.json' "$parts_dir" "$counter" "$(basename "$test_path" .py)")"
    reports+=("$part_path")
    if "$PYTHON_BIN" -m pytest \
      "$test_path" \
      --tb=short \
      --json-report \
      --json-report-file "$part_path" \
      "$@"; then
      :
    else
      status=$?
      # Pytest uses exit code 5 when a selected diagnostic module collects no tests.
      if [ "$status" -ne 5 ]; then
        exit_code=1
      fi
    fi
  done

  "$PYTHON_BIN" "$ROOT/scripts/merge_pytest_json.py" \
    --output "$manifest_path" \
    "${reports[@]}"
  return "$exit_code"
}

run_selected_pytest() {
  local manifest_path="$1"
  shift
  if [ "${ORACLEDB_HARNESS_MODE:-segmented}" = "single" ]; then
    run_pytest "$manifest_path" "$@"
  else
    run_pytest_segmented "$manifest_path" "$@"
  fi
}

python_prefix() {
  "$PYTHON_BIN" -c 'import sys; print(sys.prefix)'
}

develop_pyshim() {
  local prefix
  prefix="$(python_prefix)"
  VIRTUAL_ENV="$prefix" \
    PATH="$prefix/bin:$PATH" \
    "$PYTHON_BIN" -m maturin develop -m "$ROOT/crates/oracledb-pyshim/Cargo.toml"
}

preflight_pyshim() {
  PYTHONPATH="$ROOT/harness:${PYTHONPATH:-}" "$PYTHON_BIN" - <<'PY'
import importlib

importlib.import_module("oracledb_pyshim")
PY
}

case "${1:-}" in
  baseline)
    require_reference
    run_selected_pytest "$BASELINE_DIR/baseline.json"
    ;;
  rust)
    require_reference
    develop_pyshim
    preflight_pyshim
    PYTHONPATH="$ROOT/harness:${PYTHONPATH:-}" \
      run_selected_pytest "$RESULTS_DIR/rust.json" -p shim_inject
    ;;
  diff)
    "$PYTHON_BIN" "$ROOT/scripts/compare_pytest_json.py" \
      "$BASELINE_DIR/baseline.json" \
      "$RESULTS_DIR/rust.json"
    ;;
  list)
    require_reference
    selected_tests
    ;;
  *)
    usage
    exit 2
    ;;
esac
