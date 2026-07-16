#!/usr/bin/env bash
# Offline checks for the exact-SHA release-candidate proof producer.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

python3 scripts/verify_release_exact_sha.py --self-test
bash scripts/check_evidence_contract.sh
