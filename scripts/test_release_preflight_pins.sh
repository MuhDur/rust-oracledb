#!/usr/bin/env bash
# Regression tests for release_preflight.sh: the inter-crate version-pin guard
# and the pre-tag live-matrix guard. The latter simulates a docs-only candidate
# whose ordinary Required checks are green but whose four path-filtered live
# matrix check-runs are absent.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="$ROOT/crates/oracledb/Cargo.toml"

backup="$(mktemp "${TMPDIR:-/tmp}/oracledb-cargo-toml.XXXXXX")"
cp "$MANIFEST" "$backup"
restore() { cp "$backup" "$MANIFEST"; rm -f "$backup"; }
trap restore EXIT

fail() { echo "test-preflight-pins: FAIL: $*" >&2; exit 1; }

if ! bash "$ROOT/scripts/release_preflight.sh" --self-test; then
  fail "preflight self-test did not reject the docs-only missing-matrix case"
fi

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

echo "test-preflight-pins: OK — preflight rejects docs-only missing-matrix candidates and mismatched inter-crate pins"
