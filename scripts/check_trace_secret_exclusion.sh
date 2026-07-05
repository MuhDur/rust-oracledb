#!/usr/bin/env bash
# Verify the connect-handshake trace can never dump an authentication secret
# (bead rust-oracledb-connect-trace-mode-vdr0).
#
# The connect path hex-dumps packets to stderr when ORACLEDB_TRACE_CONNECT is
# set (PYO_DEBUG_PACKETS parity). That trace MUST stay shareable: passwords and
# database access tokens must never reach it. The invariant is structural:
#
#   * Password auth encrypts the password (`generate_verifier`) into
#     `auth_two_payload` BEFORE tracing; the encrypted verifier is safe to dump,
#     so `trace_connect_bytes("AUTH phase two payload", &auth_two_payload)` is
#     explicitly ALLOWED.
#   * Token (fast-auth) auth embeds `token.expose()` in `auth_payload`; that
#     payload is sent but NEVER passed to a trace sink — only the *step* and the
#     token-free *response* are traced.
#
# This lint deterministically catches a future edit that would break either rule
# by feeding a secret-bearing value into a `trace_connect_*` sink. It runs
# without a database, so CI enforces it on every push.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FILE="$ROOT/crates/oracledb/src/lib.rs"

if [ ! -f "$FILE" ]; then
  echo "trace-secret: missing $FILE" >&2
  exit 1
fi

# Sanity: the trace sinks must still exist. If they were renamed, this lint
# would silently pass on a stale pattern — fail loudly instead.
if ! grep -qE '\btrace_connect_step\b' "$FILE"; then
  echo "trace-secret: no trace_connect_step found — has the trace been renamed? Update this lint." >&2
  exit 1
fi

# A secret-bearing value fed to a value/bytes trace sink. `trace_connect_step`
# takes a &'static str literal (no runtime data) and cannot leak, so only the
# `value`/`bytes` sinks are inspected. Forbidden argument shapes, each anchored
# inside a single-line `trace_connect_(bytes|value)( ... )` call:
#
#   auth_payload    the fast-auth bundle that embeds the raw access token
#   .expose()       any SecretString reveal (token.expose(), password.expose())
#   .password       the plaintext password field (options.password, &.password)
#
# `[^)]*` keeps the match on the same call and stops at the call's own paren.
FORBIDDEN='trace_connect_(bytes|value)\([^)]*(\bauth_payload\b|\.expose\(|\.password\b)'

violations="$(grep -nE "$FORBIDDEN" "$FILE" || true)"

if [ -n "$violations" ]; then
  echo "trace-secret: FAILED — a secret-bearing value is fed to a connect-trace sink:" >&2
  echo "$violations" >&2
  echo >&2
  echo "The handshake trace must stay shareable. Never pass auth_payload," >&2
  echo "a .expose()'d secret, or the plaintext .password to trace_connect_bytes/" >&2
  echo "trace_connect_value. The ENCRYPTED verifier (auth_two_payload) is the only" >&2
  echo "auth material that may be traced." >&2
  exit 1
fi

echo "trace-secret: OK — no auth secret reachable through the connect trace"
