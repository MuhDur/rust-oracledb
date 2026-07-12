#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="${ORACLEDB_CONTAINER_NAME:-rust-oracledb-free}"
IMAGE="${ORACLEDB_IMAGE:-gvenzl/oracle-free:23-slim}"
ORACLE_PASSWORD="${ORACLE_PASSWORD:-OracledbTest#2026}"
HOST_PORT="${ORACLEDB_HOST_PORT:-1522}"
MAIN_USER="${PYO_TEST_MAIN_USER:-pythontest}"
MAIN_PASSWORD="${PYO_TEST_MAIN_PASSWORD:-testpw}"
PROXY_USER="${PYO_TEST_PROXY_USER:-pythontestproxy}"
PROXY_PASSWORD="${PYO_TEST_PROXY_PASSWORD:-proxypw}"

usage() {
  printf 'usage: %s up|health|env|stop\n' "$0" >&2
}

case "${1:-}" in
  up)
    if docker ps --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
      printf 'container already running: %s\n' "$CONTAINER_NAME"
    elif docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
      docker start "$CONTAINER_NAME" >/dev/null
      printf 'container started: %s\n' "$CONTAINER_NAME"
    else
      docker run -d \
        --name "$CONTAINER_NAME" \
        -p "$HOST_PORT:1521" \
        -e ORACLE_PASSWORD="$ORACLE_PASSWORD" \
        "$IMAGE" >/dev/null
      printf 'container created: %s\n' "$CONTAINER_NAME"
    fi
    ;;
  health)
    docker logs "$CONTAINER_NAME" 2>&1 | grep -F 'DATABASE IS READY TO USE'
    ;;
  env)
    if [ "$MAIN_USER" = "$MAIN_PASSWORD" ]; then
      echo "container env: PYO_TEST_MAIN_PASSWORD must differ from PYO_TEST_MAIN_USER so connect-trace secret checks are meaningful" >&2
      exit 2
    fi
    printf 'export PYO_TEST_DRIVER_MODE=thin\n'
    printf 'export PYO_TEST_CONNECT_STRING=localhost:%s/FREEPDB1\n' "$HOST_PORT"
    printf 'export PYO_TEST_ADMIN_USER=system\n'
    printf 'export PYO_TEST_ADMIN_PASSWORD=%q\n' "$ORACLE_PASSWORD"
    printf 'export PYO_TEST_SYSTEM_USER=system\n'
    printf 'export PYO_TEST_SYSTEM_PASSWORD=%q\n' "$ORACLE_PASSWORD"
    printf 'export PYO_TEST_MAIN_USER=%q\n' "$MAIN_USER"
    printf 'export PYO_TEST_MAIN_PASSWORD=%q\n' "$MAIN_PASSWORD"
    printf 'export PYO_TEST_PROXY_USER=%q\n' "$PROXY_USER"
    printf 'export PYO_TEST_PROXY_PASSWORD=%q\n' "$PROXY_PASSWORD"
    ;;
  stop)
    docker stop "$CONTAINER_NAME" >/dev/null
    printf 'container stopped without removal: %s\n' "$CONTAINER_NAME"
    ;;
  *)
    usage
    exit 2
    ;;
esac
