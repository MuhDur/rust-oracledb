#!/usr/bin/env bash
# Validate a prospective release tag at one exact SHA without changing GitHub or Git state.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 "$ROOT/scripts/verify_release_exact_sha.py" "$@"
