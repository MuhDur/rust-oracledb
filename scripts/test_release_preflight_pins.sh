#!/usr/bin/env bash
# W4-T3.1 test: prove scripts/release_preflight.sh REJECTS a mismatched
# inter-crate version pin (the 0.2.1/0.2.2 gap). Deliberately rewrites the
# oracledb -> oracledb-protocol requirement to a wrong version, asserts the
# preflight fails, and always restores the manifest (trap on EXIT).
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="$ROOT/crates/oracledb/Cargo.toml"

backup="$(mktemp "${TMPDIR:-/tmp}/oracledb-cargo-toml.XXXXXX")"
cp "$MANIFEST" "$backup"
restore() { cp "$backup" "$MANIFEST"; rm -f "$backup"; }
trap restore EXIT

fail() { echo "test-preflight-pins: FAIL: $*" >&2; exit 1; }

# 1) Baseline: the unmodified manifest must PASS the preflight.
if ! bash "$ROOT/scripts/release_preflight.sh" >/dev/null 2>&1; then
  fail "preflight rejected the unmodified (lockstep) manifest"
fi

# 2) Break the inter-crate pin: oracledb-protocol -> a version that cannot match
#    the workspace version.
sed -i -E \
  's/(^oracledb-protocol[[:space:]]*=[[:space:]]*\{[^}]*version[[:space:]]*=[[:space:]]*")[^"]*(")/\10.0.0-mismatch\2/' \
  "$MANIFEST"

if ! grep -q '0.0.0-mismatch' "$MANIFEST"; then
  fail "could not rewrite the oracledb-protocol pin (manifest format changed?)"
fi

# 3) The preflight MUST now fail.
if bash "$ROOT/scripts/release_preflight.sh" >/dev/null 2>&1; then
  fail "preflight ACCEPTED a mismatched inter-crate pin (0.0.0-mismatch)"
fi

echo "test-preflight-pins: OK — preflight rejects a mismatched inter-crate pin and accepts the lockstep one"
