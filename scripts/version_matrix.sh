#!/usr/bin/env bash
# Multi-version Oracle server matrix for live protocol coverage
# (bead rust-oracledb-pre23ai-connect-z47u.5).
#
# The 0.5.x line was only ever live-tested against 23ai FREE, which is how a
# connect path that could not reach ANY pre-23ai server shipped. This helper
# manages one container per server generation and runs the connect smoke
# against each:
#
#   xe18    gvenzl/oracle-xe:18-slim    (closest free proxy to a 19c fleet)
#   xe21    gvenzl/oracle-xe:21-slim
#   free23  gvenzl/oracle-free:23-slim  (fast-auth / END_OF_RESPONSE era)
#
# Protocol behaviors that only pre-23ai lanes exercise: RESEND on connect,
# classic (non-fast-auth) session establishment, no END_OF_RESPONSE framing,
# low negotiated ttc field versions (no ub8 function-header tokens).
#
# usage: scripts/version_matrix.sh up|health|smoke|env|stop [lane]
#   lane: xe18 | xe21 | free23 | all (default all)
set -euo pipefail

ORACLE_PASSWORD="${ORACLE_PASSWORD:-OracledbTest#2026}"
APP_USER="${ORACLEDB_MATRIX_APP_USER:-testuser}"
APP_USER_PASSWORD="${ORACLEDB_MATRIX_APP_PASSWORD:-testpw}"

# lane -> name / image / host port / service / smoke user / smoke password
lane_fields() {
  case "$1" in
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
    *) printf 'unknown lane: %s (xe18|xe21|free23)\n' "$1" >&2; return 2 ;;
  esac
}

lanes_for() {
  case "${1:-all}" in
    all) printf 'xe18\nxe21\nfree23\n' ;;
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
  if cargo run -q --example smoke -- \
      "localhost:$port/$service" "$user" "$password"; then
    printf '%-7s SMOKE GREEN\n' "$lane"
  else
    printf '%-7s SMOKE FAILED\n' "$lane"
    return 1
  fi
}

cmd="${1:-}"
lane_arg="${2:-all}"
rc=0
case "$cmd" in
  up|health|env|smoke)
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
    printf 'usage: %s up|health|smoke|env|stop [xe18|xe21|free23|all]\n' "$0" >&2
    exit 2
    ;;
esac
exit "$rc"
