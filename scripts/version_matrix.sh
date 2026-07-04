#!/usr/bin/env bash
# Multi-version Oracle server matrix for live protocol coverage
# (bead rust-oracledb-pre23ai-connect-z47u.5).
#
# The 0.5.x line was only ever live-tested against 23ai FREE, which is how a
# connect path that could not reach ANY pre-23ai server shipped. This helper
# manages one container per server generation and runs the live suites
# against each:
#
#   xe11    gvenzl/oracle-xe:11-slim    (BELOW the protocol floor: refusal lane)
#   xe18    gvenzl/oracle-xe:18-slim    (closest free proxy to a 19c fleet)
#   xe21    gvenzl/oracle-xe:21-slim
#   free23  gvenzl/oracle-free:23-slim  (fast-auth / END_OF_RESPONSE era)
#
# Protocol behaviors that only pre-23ai lanes exercise: RESEND on connect,
# classic (non-fast-auth) session establishment, no END_OF_RESPONSE framing,
# low negotiated ttc field versions (no ub8 function-header tokens), break
# MARKER before the classic auth ERROR response.
#
# The xe11 lane is deliberately BELOW the accepted protocol floor
# (TNS_VERSION_MIN_ACCEPTED = 315; Oracle 11g negotiates 314). Its assertion
# is inverted: smoke/full PASS when the driver refuses the server with the
# structured UnsupportedVersion error naming the floor (reference parity:
# python-oracledb DPY-3010) — never a hang, never a decode error.
#
# Subcommands:
#   up      create/start the lane container(s)
#   health  check "DATABASE IS READY TO USE" in the container log
#   smoke   quick connect + two queries (examples/smoke.rs);
#           xe11: structured-refusal assertion
#   full    deep suite with VALUE assertions (examples/matrix_full.rs):
#           identity, multi-packet fetch, wide rows, bind DML +
#           rollback/commit, CLOB/BLOB write+readback, describe, NULLs,
#           scalar round-trips, error paths; xe11: structured-refusal
#           assertion. This is the standing release gate (see
#           scripts/release_matrix_gate.sh).
#   truth   statement-suite ground-truth differential (bead
#           rust-oracledb-rwoh): runs the IDENTICAL statement corpus through
#           the Rust driver (examples/statement_ground_truth.rs) AND
#           python-oracledb (scripts/statement_ground_truth.py, needs a
#           python with the oracledb module — auto-detected from
#           .venv-py313, override with ORACLEDB_GT_PYTHON), then diffs the
#           two JSON documents field-by-field. Any mismatch fails the lane.
#           Skipped for xe11 (below the protocol floor; nothing to compare).
#   env     print PYO_TEST_* exports for the lane
#   stop    stop the lane container(s)
#
# usage: scripts/version_matrix.sh up|health|smoke|full|truth|env|stop [lane]
#   lane: xe11 | xe18 | xe21 | free23 | all (default all)

# The lane_* functions are dispatched dynamically via "lane_$cmd".
# shellcheck disable=SC2329
set -euo pipefail

ORACLE_PASSWORD="${ORACLE_PASSWORD:-OracledbTest#2026}"
APP_USER="${ORACLEDB_MATRIX_APP_USER:-testuser}"
APP_USER_PASSWORD="${ORACLEDB_MATRIX_APP_PASSWORD:-testpw}"

# lane -> name / image / host port / service / suite user / suite password
lane_fields() {
  case "$1" in
    xe11)   printf '%s\n' "${ORACLEDB_XE11_CONTAINER:-oracle-xe11-1511}" \
                          "gvenzl/oracle-xe:11-slim" \
                          "${ORACLEDB_XE11_PORT:-1511}" "XE" \
                          "$APP_USER" "$APP_USER_PASSWORD" ;;
    xe18)   printf '%s\n' "${ORACLEDB_XE18_CONTAINER:-oracle-xe18-1518}" \
                          "gvenzl/oracle-xe:18-slim" \
                          "${ORACLEDB_XE18_PORT:-1518}" "XEPDB1" \
                          "$APP_USER" "$APP_USER_PASSWORD" ;;
    xe21)   printf '%s\n' "${ORACLEDB_XE21_CONTAINER:-oracle-xe21-1520}" \
                          "gvenzl/oracle-xe:21-slim" \
                          "${ORACLEDB_XE21_PORT:-1520}" "XEPDB1" \
                          "$APP_USER" "$APP_USER_PASSWORD" ;;
    free23) printf '%s\n' "${ORACLEDB_CONTAINER_NAME:-rust-oracledb-free}" \
                          "gvenzl/oracle-free:23-slim" \
                          "${ORACLEDB_HOST_PORT:-1522}" "FREEPDB1" \
                          "${PYO_TEST_MAIN_USER:-pythontest}" \
                          "${PYO_TEST_MAIN_PASSWORD:-pythontest}" ;;
    *) printf 'unknown lane: %s (xe11|xe18|xe21|free23)\n' "$1" >&2; return 2 ;;
  esac
}

# Whether a lane asserts the below-floor structured refusal instead of a
# working connection.
lane_expects_refusal() {
  [ "$1" = "xe11" ]
}

lanes_for() {
  case "${1:-all}" in
    all) printf 'xe11\nxe18\nxe21\nfree23\n' ;;
    *)   printf '%s\n' "$1" ;;
  esac
}

lane_up() {
  local lane="$1" name image port
  { read -r name; read -r image; read -r port; } < <(lane_fields "$lane")
  if docker ps --format '{{.Names}}' | grep -qx "$name"; then
    printf '%-7s already running: %s\n' "$lane" "$name"
  elif docker ps -a --format '{{.Names}}' | grep -qx "$name"; then
    docker start "$name" >/dev/null
    printf '%-7s started: %s\n' "$lane" "$name"
  else
    docker run -d --name "$name" -p "$port:1521" \
      -e ORACLE_PASSWORD="$ORACLE_PASSWORD" \
      -e APP_USER="$APP_USER" \
      -e APP_USER_PASSWORD="$APP_USER_PASSWORD" \
      "$image" >/dev/null
    printf '%-7s created: %s (first boot takes minutes)\n' "$lane" "$name"
  fi
}

lane_health() {
  local lane="$1" name
  { read -r name; } < <(lane_fields "$lane")
  # No `grep -q`: early exit would SIGPIPE `docker logs` and, under pipefail,
  # report a ready database as not ready.
  if docker logs "$name" 2>&1 | grep -F 'DATABASE IS READY TO USE' >/dev/null; then
    printf '%-7s READY\n' "$lane"
  else
    printf '%-7s NOT READY\n' "$lane"
    return 1
  fi
}

lane_env() {
  local lane="$1" name image port service user password
  { read -r name; read -r image; read -r port; read -r service; \
    read -r user; read -r password; } < <(lane_fields "$lane")
  printf '# %s (%s)\n' "$lane" "$image"
  printf 'export PYO_TEST_CONNECT_STRING=localhost:%s/%s\n' "$port" "$service"
  printf 'export PYO_TEST_MAIN_USER=%q\n' "$user"
  printf 'export PYO_TEST_MAIN_PASSWORD=%q\n' "$password"
}

lane_smoke() {
  local lane="$1" name image port service user password
  { read -r name; read -r image; read -r port; read -r service; \
    read -r user; read -r password; } < <(lane_fields "$lane")
  printf '=== %s (%s) localhost:%s/%s ===\n' "$lane" "$image" "$port" "$service"
  if lane_expects_refusal "$lane"; then
    # Below-floor lane: PASS means the driver cleanly refused the server.
    if cargo run -q --example matrix_full -- --expect-version-refusal \
        "localhost:$port/$service" "$user" "$password"; then
      printf '%-7s SMOKE GREEN (structured refusal verified)\n' "$lane"
    else
      printf '%-7s SMOKE FAILED (refusal missing or malformed)\n' "$lane"
      return 1
    fi
  elif cargo run -q --example smoke -- \
      "localhost:$port/$service" "$user" "$password"; then
    printf '%-7s SMOKE GREEN\n' "$lane"
  else
    printf '%-7s SMOKE FAILED\n' "$lane"
    return 1
  fi
}

lane_full() {
  local lane="$1" name image port service user password
  { read -r name; read -r image; read -r port; read -r service; \
    read -r user; read -r password; } < <(lane_fields "$lane")
  printf '=== %s FULL (%s) localhost:%s/%s ===\n' "$lane" "$image" "$port" "$service"
  local -a refusal_flag=()
  if lane_expects_refusal "$lane"; then
    refusal_flag=(--expect-version-refusal)
  fi
  if cargo run -q --example matrix_full -- "${refusal_flag[@]}" \
      "localhost:$port/$service" "$user" "$password"; then
    printf '%-7s FULL GREEN\n' "$lane"
  else
    printf '%-7s FULL FAILED\n' "$lane"
    return 1
  fi
}

# Ground-truth differential (rust vs python-oracledb) for one lane (bead
# rust-oracledb-rwoh). Any field-by-field mismatch fails the lane.
lane_truth() {
  local lane="$1" name image port service user password
  { read -r name; read -r image; read -r port; read -r service; \
    read -r user; read -r password; } < <(lane_fields "$lane")
  if lane_expects_refusal "$lane"; then
    printf '%-7s TRUTH SKIPPED (below-floor refusal lane)\n' "$lane"
    return 0
  fi
  local python="${ORACLEDB_GT_PYTHON:-.venv-py313/bin/python}"
  if ! "$python" -c 'import oracledb' >/dev/null 2>&1; then
    printf '%-7s TRUTH FAILED (no python with the oracledb module; set ORACLEDB_GT_PYTHON)\n' "$lane"
    return 1
  fi
  printf '=== %s TRUTH (%s) localhost:%s/%s ===\n' "$lane" "$image" "$port" "$service"
  local out_dir="${TMPDIR:-/tmp}/oracledb-gt-$lane-$$"
  mkdir -p "$out_dir"
  if ! cargo run -q -p oracledb --example statement_ground_truth -- \
      "localhost:$port/$service" "$user" "$password" > "$out_dir/rust.json"; then
    printf '%-7s TRUTH FAILED (rust emitter)\n' "$lane"
    return 1
  fi
  if ! "$python" scripts/statement_ground_truth.py \
      "localhost:$port/$service" "$user" "$password" > "$out_dir/python.json"; then
    printf '%-7s TRUTH FAILED (python twin)\n' "$lane"
    return 1
  fi
  if "$python" scripts/statement_ground_truth.py --diff \
      "$out_dir/rust.json" "$out_dir/python.json"; then
    printf '%-7s TRUTH GREEN (field-by-field identical)\n' "$lane"
  else
    printf '%-7s TRUTH FAILED (ground-truth mismatch; artifacts in %s)\n' "$lane" "$out_dir"
    return 1
  fi
}

cmd="${1:-}"
lane_arg="${2:-all}"
rc=0
case "$cmd" in
  up|health|env|smoke|full|truth)
    while read -r lane; do
      "lane_$cmd" "$lane" || rc=1
    done < <(lanes_for "$lane_arg")
    ;;
  stop)
    while read -r lane; do
      { read -r name; } < <(lane_fields "$lane")
      docker stop "$name" >/dev/null && printf '%-7s stopped: %s\n' "$lane" "$name"
    done < <(lanes_for "$lane_arg")
    ;;
  *)
    printf 'usage: %s up|health|smoke|full|truth|env|stop [xe11|xe18|xe21|free23|all]\n' "$0" >&2
    exit 2
    ;;
esac
exit "$rc"
