#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REFERENCE_DIR="${ORACLEDB_REFERENCE_DIR:-$ROOT/reference/python-oracledb}"
REFERENCE_URL="${ORACLEDB_REFERENCE_URL:-https://github.com/oracle/python-oracledb.git}"
REFERENCE_TAG="${ORACLEDB_REFERENCE_TAG:-v4.0.1}"
EXPECTED_COMMIT="3daef052904e41668bb862e6fa40f43c22a81beb"

if [ -e "$REFERENCE_DIR" ]; then
  if [ ! -d "$REFERENCE_DIR/.git" ]; then
    printf 'refusing to use non-git reference path: %s\n' "$REFERENCE_DIR" >&2
    exit 2
  fi
  actual="$(git -C "$REFERENCE_DIR" rev-parse HEAD)"
  if [ "$actual" != "$EXPECTED_COMMIT" ]; then
    printf 'reference checkout exists at unexpected commit\n' >&2
    printf 'expected: %s\nactual:   %s\n' "$EXPECTED_COMMIT" "$actual" >&2
    printf 'refusing to move an existing checkout without explicit operator action\n' >&2
    exit 3
  fi
else
  mkdir -p "$(dirname "$REFERENCE_DIR")"
  git clone --branch "$REFERENCE_TAG" "$REFERENCE_URL" "$REFERENCE_DIR"
fi

actual="$(git -C "$REFERENCE_DIR" rev-parse HEAD)"
if [ "$actual" != "$EXPECTED_COMMIT" ]; then
  printf 'reference pin mismatch after clone\n' >&2
  printf 'expected: %s\nactual:   %s\n' "$EXPECTED_COMMIT" "$actual" >&2
  exit 4
fi

git -C "$REFERENCE_DIR" submodule update --init --recursive

printf 'python-oracledb reference pinned: %s %s\n' "$REFERENCE_TAG" "$actual"
