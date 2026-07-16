#!/usr/bin/env bash
# CI-taxonomy drift gate (bead f1cl.8).
#
# Fails when docs/ci_taxonomy.json no longer matches .github/workflows/*.yml —
# i.e. when a CI job appeared, disappeared, or changed tier without being
# reclassified. The tier that matters most is `advisory`: a job with
# `continue-on-error: true` never fails its run, so `gh run list` reports the
# run "success" while that job's check-run is red. A required job silently
# gaining continue-on-error would stop gating and nothing would say so.
#
# Offline: derives from the YAML only, no network, no gh, no API token.
# Regenerate with: scripts/ci_taxonomy.py --write
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PYTHON_BIN="${PYTHON:-python3}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "ci-taxonomy: no $PYTHON_BIN on PATH" >&2
  exit 2
fi

if ! "$PYTHON_BIN" -c 'import yaml' >/dev/null 2>&1; then
  echo "ci-taxonomy: PyYAML is required ($PYTHON_BIN -m pip install pyyaml)" >&2
  exit 2
fi

exec "$PYTHON_BIN" "$ROOT/scripts/ci_taxonomy.py" --check
