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
#           scalar round-trips, error paths; PLUS per-capability 0.7.3
#           differentiator value-asserts (A5.1) — pipelining (23ai gate),
#           batch-error continuation, call timeout, statement-shape self-heal,
#           VECTOR->FixedSizeList (23ai gate), LOB streaming, the idempotency-
#           gated retry executor, and thin SODA (21c gate). Built with
#           --features "$MATRIX_FULL_FEATURES" (soda,arrow). xe11:
#           structured-refusal assertion. This is the standing release gate
#           (see scripts/release_matrix_gate.sh).
#   truth   statement-suite ground-truth differential (bead
#           rust-oracledb-rwoh): runs the IDENTICAL statement corpus through
#           the Rust driver (examples/statement_ground_truth.rs) AND
#           python-oracledb (scripts/statement_ground_truth.py, needs a
#           python with the oracledb module — auto-detected from
#           .venv-py313, override with ORACLEDB_GT_PYTHON), then diffs the
#           two JSON documents field-by-field. Any mismatch fails the lane.
#           Skipped for xe11 (below the protocol floor; nothing to compare).
#   tcps    OCI TCPS lane (A5.2): local rustls TCPS + wallet suites over the C1
#           synthetic wallet fixtures — wallet decrypt, TCPS handshake, DN/name
#           match (+negatives), mutual TLS, and the OCI IAM token frame +
#           non-TCPS refusal. No container (gvenzl cannot speak TCPS); offline
#           and deterministic. Equivalent to `full octcps`.
#   env     print PYO_TEST_* exports for the lane
#   stop    stop the lane container(s)
#
# usage: scripts/version_matrix.sh up|health|smoke|full|truth|tcps|env|stop [lane]
#   lane: xe11 | xe18 | xe21 | free23 | octcps | all (default all)
#   (octcps is the local TCPS lane; it supports `full`/`tcps` only, no container)

# The lane_* functions are dispatched dynamically via "lane_$cmd".
# shellcheck disable=SC2329
set -euo pipefail

# Local matrix management relies on Docker, while hosted CI provisions the
# Oracle service directly. Check the local capability explicitly instead of
# allowing a missing PATH entry to look like a skipped lane.
require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'version-matrix: required command unavailable: %s\n' "$1" >&2
    return 127
  fi
}

ORACLE_PASSWORD="${ORACLE_PASSWORD:-OracledbTest#2026}"
APP_USER="${ORACLEDB_MATRIX_APP_USER:-testuser}"
APP_USER_PASSWORD="${ORACLEDB_MATRIX_APP_PASSWORD:-testpw}"

# Features the `full`/`smoke` example (examples/matrix_full.rs) is built with so
# the 0.7.3 per-capability differentiator value-asserts (A5.1) actually compile
# in and run: `soda` (thin SODA, 21c+ gate) and `arrow` (VECTOR -> FixedSizeList,
# 23ai gate). Without these the SODA and VECTOR sections are cfg'd out.
MATRIX_FULL_FEATURES="${ORACLEDB_MATRIX_FULL_FEATURES:-soda,arrow}"

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
                          "${PYO_TEST_MAIN_PASSWORD:-testpw}" ;;
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
    if cargo run -q --features "$MATRIX_FULL_FEATURES" --example matrix_full -- --expect-version-refusal \
        "localhost:$port/$service" "$user" "$password"; then
      printf '%-7s SMOKE PASS (structured refusal verified)\n' "$lane"
    else
      printf '%-7s SMOKE FAILED (refusal missing or malformed)\n' "$lane"
      return 1
    fi
  elif cargo run -q --example smoke -- \
      "localhost:$port/$service" "$user" "$password"; then
    printf '%-7s SMOKE PASS\n' "$lane"
  else
    printf '%-7s SMOKE FAILED\n' "$lane"
    return 1
  fi
}

# OCI TCPS lane (A5.2 / bead iec3.1.26): a LOCAL TLS (TCPS) lane driven over the
# C1 synthetic wallet fixtures and the C2 rustls TCPS harness — NOT a gvenzl
# container (the gvenzl images cannot speak TCPS). It runs entirely offline and
# deterministically, and exercises the OCI wallet + transport surface autonomously
# and secret-free:
#
#   * wallet decrypt — the legacy 3DES PKCS12 client identity (a23 mTLS test);
#   * TCPS handshake — CA-trusted + mutual-TLS handshakes against a real rustls
#     server presenting the synthetic leaf (C2);
#   * DN / CN name match — positive and the fail-closed negatives (host mismatch,
#     cert-DN mismatch, wrong trust anchor);
#   * OCI IAM token — the AUTH_TOKEN fast-auth frame over the TCPS lane, and the
#     fail-closed refusal of a token over a plaintext descriptor (C3).
#
# The synthetic fixtures carry only fictional identifiers (CN=oracle-test.invalid),
# so secret_scan stays clean. This is the local-wallet TLS lane the release gate
# runs alongside the four server generations.
lane_tcps() {
  printf '=== octcps FULL (local TCPS over C1 synthetic wallets; no container) ===\n'
  local ok=1
  # C1/C2/C3: driver-side TCPS handshake, DN/name match, mTLS, 3DES wallet
  # decrypt, and the OCI IAM token frame + non-TCPS refusal.
  if ! cargo test -q -p oracledb --test tls_handshake; then ok=0; fi
  # Wallet reader breadth over the same synthetic fixtures (ewallet.pem incl.
  # encrypted keys, ewallet.p12/3DES, cwallet.sso).
  if ! cargo test -q -p oracledb-protocol --test tls_wallet; then ok=0; fi
  if [ "$ok" -eq 1 ]; then
    printf '%-7s FULL PASS\n' octcps
  else
    printf '%-7s FULL FAILED\n' octcps
    return 1
  fi
}

lane_full() {
  local lane="$1" name image port service user password
  # The OCI TCPS lane is not a container: it runs the local rustls TCPS + wallet
  # suites over the C1 synthetic fixtures instead of connecting to a server.
  if [ "$lane" = "octcps" ]; then
    lane_tcps
    return $?
  fi
  { read -r name; read -r image; read -r port; read -r service; \
    read -r user; read -r password; } < <(lane_fields "$lane")
  printf '=== %s FULL (%s) localhost:%s/%s ===\n' "$lane" "$image" "$port" "$service"
  local -a refusal_flag=()
  if lane_expects_refusal "$lane"; then
    refusal_flag=(--expect-version-refusal)
  fi
  if cargo run -q --features "$MATRIX_FULL_FEATURES" --example matrix_full -- "${refusal_flag[@]}" \
      "localhost:$port/$service" "$user" "$password"; then
    printf '%-7s FULL PASS\n' "$lane"
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
    printf '%-7s TRUTH PASS (field-by-field identical)\n' "$lane"
  else
    printf '%-7s TRUTH FAILED (ground-truth mismatch; artifacts in %s)\n' "$lane" "$out_dir"
    return 1
  fi
}

# --- versions: per-lane fixture bootstrap + full live-suite matrix ----------
#
# The `full`/`truth` lanes above run curated example binaries. `versions` is the
# operator's non-negotiable bar (bead rust-oracledb-6mar): for EACH working lane
# it (1) bootstraps that lane's own fixture schema (vx6_* object types, the
# E_TEST edition, proxy user, grants) via scripts/bootstrap_live_schema.sh, then
# (2) RUNS every live integration suite under crates/oracledb/tests against it,
# capturing a per-suite x per-lane PASS / SKIP / FAIL verdict. xe11 stays a
# connect-refusal assertion only (below the protocol floor). A suite may be
# SKIP (non-green but accepted) ONLY through suite_skip_reason() below, after
# its focused capability probe passes. That keeps a server limitation typed,
# documented, and evidenced — never a silent skip.

# Features needed so every cfg(feature = "…")-gated live test COMPILES IN and
# actually runs (arrow/soda suites, rust_decimal/chrono/uuid/serde_json typed
# conversions). Kept in sync with the gates in crates/oracledb/tests/live_*.rs.
LIVE_SUITE_FEATURES="arrow,chrono,rust_decimal,serde_json,soda,uuid"

# Auto-discover the live suites (bead: "and any others under live_*.rs /
# *_live.rs") so a newly added suite is covered without editing this script.
discover_live_suites() {
  local d="crates/oracledb/tests"
  { ls "$d"/live_*.rs "$d"/*_live.rs 2>/dev/null || true; } \
    | sed -E 's#.*/##; s#\.rs$##' | sort -u
}

# DBA password used to bootstrap fixtures for a lane. The xe18/xe21 gvenzl
# containers use ORACLE_PASSWORD=oracle for SYS/SYSTEM; free23 uses the shared
# ORACLE_PASSWORD. Override the xe default with ORACLEDB_XE_SYSTEM_PASSWORD.
lane_system_password() {
  case "$1" in
    xe18|xe21) printf '%s\n' "${ORACLEDB_XE_SYSTEM_PASSWORD:-oracle}" ;;
    # free23 (rust-oracledb-free) SYS/SYSTEM password. Defaults to the
    # container.sh default so `versions all` bootstraps without env juggling;
    # override with ORACLE_PASSWORD when the container uses a different secret.
    *)         printf '%s\n' "${ORACLE_PASSWORD:-OracledbTest#2026}" ;;
  esac
}

# Typed-skip registry. A cell (lane:suite) is accepted as non-green ONLY when
# this returns 0 with a stable reason code AND its focused capability probe
# passes below. No entry => a red cell is an OPEN BUG, not a quiet pass. Kept
# in lockstep with docs/VERSION_MATRIX.md.
suite_skip_reason() {
  local lane="$1" suite="$2"
  case "$lane:$suite" in
    # SODA on Oracle 18c: the driver's write path requires the 21c+
    # JSON_SERIALIZE SQL function. The focused live_soda probe must establish
    # both its absence and create_collection -> ORA-00904 before the suite is
    # typed SKIP. Green on xe21 (21c) and free23 (23ai). Full pre-21c support
    # is tracked in bead rust-oracledb-soda-pre21c.
    xe18:live_soda)
      echo "pre-21c-soda-unsupported"
      return 0
      ;;
    *) return 1 ;;
  esac
}

# Run the non-silent proof required for a typed skip. Keep each proof explicit:
# a generic full-suite invocation would turn an unavailable server capability
# into an opaque failure, while a bare skip would provide no evidence at all.
run_typed_skip_probe() {
  local lane="$1" suite="$2" logf="$3"
  case "$lane:$suite" in
    xe18:live_soda)
      cargo test -q -p oracledb --features "$LIVE_SUITE_FEATURES" \
        --test live_soda soda_gated_on_pre21c_with_proof -- --ignored \
        > "$logf" 2>&1
      ;;
    *)
      printf 'no capability probe registered for typed skip %s:%s\n' \
        "$lane" "$suite" > "$logf"
      return 1
      ;;
  esac
}

# Bootstrap one lane's fixture schema (idempotent drop+recreate of the app user
# with the vx6_* fixtures, E_TEST edition and grants the suites expect).
lane_bootstrap() {
  local lane="$1" name image port service user password syspw proxy
  { read -r name; read -r image; read -r port; read -r service; \
    read -r user; read -r password; } < <(lane_fields "$lane")
  syspw="$(lane_system_password "$lane")"
  proxy="${user}proxy"
  printf '=== %s BOOTSTRAP fixtures (%s %s) ===\n' "$lane" "$name" "$service"
  ORACLEDB_CONTAINER_NAME="$name" \
  ORACLE_PASSWORD="$syspw" \
  ORACLEDB_PDB="$service" \
  PYO_TEST_MAIN_USER="$user" \
  PYO_TEST_MAIN_PASSWORD="$password" \
  PYO_TEST_PROXY_USER="$proxy" \
  PYO_TEST_PROXY_PASSWORD="$proxy" \
    bash scripts/bootstrap_live_schema.sh
}

run_versions() {
  local lane_arg="$1"
  local out_dir="tests/artifacts/version_matrix"
  local log_dir="${TMPDIR:-/tmp}/oracledb-versions-$$"
  mkdir -p "$out_dir" "$log_dir"

  local -a suites=() lanes_run=()
  while read -r s; do suites+=("$s"); done < <(discover_live_suites)
  while read -r l; do lanes_run+=("$l"); done < <(lanes_for "$lane_arg")

  declare -A cell cellnote cellreason
  local overall=PASS lane suite logf summ reason

  for lane in "${lanes_run[@]}"; do
    if lane_expects_refusal "$lane"; then
      local name image port service user password
      { read -r name; read -r image; read -r port; read -r service; \
        read -r user; read -r password; } < <(lane_fields "$lane")
      printf '=== %s REFUSAL assertion (%s) ===\n' "$lane" "$service"
      if cargo run -q --features "$MATRIX_FULL_FEATURES" --example matrix_full -- --expect-version-refusal \
          "localhost:$port/$service" "$user" "$password" \
          > "$log_dir/$lane-REFUSAL.log" 2>&1; then
        cell[$lane:REFUSAL]=PASS
        cellnote[$lane:REFUSAL]="structured UnsupportedVersion refusal verified"
        printf '%-7s %-28s PASS\n' "$lane" REFUSAL
      else
        cell[$lane:REFUSAL]=FAIL
        cellnote[$lane:REFUSAL]="refusal missing or malformed"
        overall=FAIL
        printf '%-7s %-28s FAIL\n' "$lane" REFUSAL
      fi
      continue
    fi

    eval "$(lane_env "$lane")"
    if ! lane_bootstrap "$lane" > "$log_dir/$lane-BOOTSTRAP.log" 2>&1; then
      printf '%-7s BOOTSTRAP FAILED (see %s)\n' "$lane" "$log_dir/$lane-BOOTSTRAP.log" >&2
      overall=FAIL
      for suite in "${suites[@]}"; do
        cell[$lane:$suite]=FAIL
        cellnote[$lane:$suite]="fixture bootstrap failed"
      done
      continue
    fi

    for suite in "${suites[@]}"; do
      logf="$log_dir/$lane-$suite.log"
      if reason="$(suite_skip_reason "$lane" "$suite")"; then
        if run_typed_skip_probe "$lane" "$suite" "$logf"; then
          summ="$(grep -hE '^test result:' "$logf" | tail -1)"
          cell[$lane:$suite]=SKIP
          cellreason[$lane:$suite]="$reason"
          cellnote[$lane:$suite]="${summ:-active capability probe passed}"
          printf '%-7s %-28s SKIP    %s (%s)\n' "$lane" "$suite" \
            "$reason" "${summ:-active capability probe passed}"
        else
          cell[$lane:$suite]=FAIL
          cellnote[$lane:$suite]="typed skip probe failed: $(grep -hE '^test result:|panicked|error\[|error:' "$logf" | tail -3 | tr '\n' ' ')"
          overall=FAIL
          printf '%-7s %-28s FAIL    %s (log: %s)\n' "$lane" "$suite" \
            "${cellnote[$lane:$suite]}" "$logf"
        fi
      elif cargo test -q -p oracledb --features "$LIVE_SUITE_FEATURES" \
          --test "$suite" -- --include-ignored > "$logf" 2>&1; then
        summ="$(grep -hE '^test result:' "$logf" | tail -1)"
        cell[$lane:$suite]=PASS
        cellnote[$lane:$suite]="${summ:-ok}"
        printf '%-7s %-28s PASS    %s\n' "$lane" "$suite" "${summ:-}"
      else
        cell[$lane:$suite]=FAIL
        cellnote[$lane:$suite]="$(grep -hE '^test result:|panicked|error\[|error:' "$logf" | tail -3 | tr '\n' ' ')"
        overall=FAIL
        printf '%-7s %-28s FAIL    %s (log: %s)\n' "$lane" "$suite" \
          "${cellnote[$lane:$suite]}" "$logf"
      fi
    done
  done

  # JSON artifact for the release gate / VERSION_MATRIX doc.
  local sha out
  sha="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
  out="$out_dir/versions-$sha.json"
  {
    printf '{\n  "sha": "%s",\n' "$sha"
    printf '  "recorded_at_utc": "%s",\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '  "suite": "version_matrix.sh versions",\n'
    printf '  "features": "%s",\n' "$LIVE_SUITE_FEATURES"
    printf '  "overall": "%s",\n' "$overall"
    printf '  "cells": [\n'
    local first=1 key
    for lane in "${lanes_run[@]}"; do
      for key in "${!cell[@]}"; do
        case "$key" in "$lane:"*) ;; *) continue ;; esac
        [ "$first" -eq 1 ] || printf ',\n'
        first=0
        if [ -n "${cellreason[$key]:-}" ]; then
          printf '    {"lane": "%s", "suite": "%s", "verdict": "%s", "reason": "%s", "note": "%s"}' \
            "$lane" "${key#*:}" "${cell[$key]}" \
            "$(printf '%s' "${cellreason[$key]}" | sed 's/\\/\\\\/g; s/"/\\"/g')" \
            "$(printf '%s' "${cellnote[$key]}" | sed 's/\\/\\\\/g; s/"/\\"/g')"
        else
          printf '    {"lane": "%s", "suite": "%s", "verdict": "%s", "note": "%s"}' \
            "$lane" "${key#*:}" "${cell[$key]}" \
            "$(printf '%s' "${cellnote[$key]}" | sed 's/\\/\\\\/g; s/"/\\"/g')"
        fi
      done
    done
    printf '\n  ]\n}\n'
  } > "$out"
  printf 'versions: wrote %s (overall=%s)\n' "$out" "$overall"
  printf 'versions: per-suite logs in %s\n' "$log_dir"

  [ "$overall" = PASS ]
}

cmd="${1:-}"
lane_arg="${2:-all}"
rc=0
case "$cmd" in
  versions)
    require_command cargo
    require_command docker
    run_versions "$lane_arg" || rc=1
    ;;
  tcps)
    # OCI TCPS lane (A5.2): local TLS over the C1 synthetic wallets, no container.
    lane_tcps || rc=1
    ;;
  up|health|env|smoke|full|truth)
    case "$cmd" in
      up|health) require_command docker ;;
      smoke|full|truth) require_command cargo ;;
      env) : ;;
    esac
    while read -r lane; do
      "lane_$cmd" "$lane" || rc=1
    done < <(lanes_for "$lane_arg")
    ;;
  stop)
    require_command docker
    while read -r lane; do
      { read -r name; } < <(lane_fields "$lane")
      docker stop "$name" >/dev/null && printf '%-7s stopped: %s\n' "$lane" "$name"
    done < <(lanes_for "$lane_arg")
    ;;
  *)
    printf 'usage: %s up|health|smoke|full|truth|versions|tcps|env|stop [xe11|xe18|xe21|free23|octcps|all]\n' "$0" >&2
    exit 2
    ;;
esac
exit "$rc"
