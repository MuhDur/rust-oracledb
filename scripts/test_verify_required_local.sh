#!/usr/bin/env bash
# DB-free contract tests for the required-proof local runner.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNNER="$ROOT/scripts/verify_required_local.sh"

"$RUNNER" --self-test
"$ROOT/scripts/check_evidence_contract.sh"
"$RUNNER" --plan | python3 -c '
import json
import sys

plan = json.load(sys.stdin)["steps"]
commands = {item["name"] for item in plan if item["classification"] == "required-command"}
assert {"Format", "Clippy", "Test workspace", "Test cassette replay", "Supply-chain checks"} <= commands, commands
assert all(item["classification"] != "profile-excluded" or item["enabled_for_required"] is False for item in plan)
assert any(item["name"] == "Package crates" and item["classification"] == "profile-excluded" for item in plan)
assert any(item["name"].startswith("uses: taiki-e/install-action@") and item["classification"] == "setup-action" for item in plan)
print("verify-required-local: plan contains every active Required gate")
'
