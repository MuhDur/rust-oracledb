#!/usr/bin/env bash
# Release gate: run the FULL live version matrix (scripts/version_matrix.sh
# full) against every lane and record the verdict for the current git SHA.
#
# A release CANNOT ship without a green record from this script for the exact
# release SHA: scripts/release_preflight.sh (which runs in the tag-driven
# release workflow) refuses any tag whose HEAD has no committed, all-green
# matrix-results artifact. Workflow:
#
#   1. scripts/release_matrix_gate.sh          # on the commit you intend to tag
#   2. git add tests/artifacts/version_matrix/ # commit the results file
#   3. tag vX.Y.Z on that history               # preflight verifies HEAD's file
#
# The artifact records per-lane pass/fail, the SHA it ran on, and whether the
# worktree was dirty (a dirty run is recorded but REJECTED by preflight — the
# gate must run on exactly the tree being released).
#
# usage: scripts/release_matrix_gate.sh
#   env: ORACLEDB_MATRIX_BOOT_TIMEOUT_SECS (default 600) — container boot wait
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BOOT_TIMEOUT="${ORACLEDB_MATRIX_BOOT_TIMEOUT_SECS:-600}"
LANES=(xe11 xe18 xe21 free23)
OUT_DIR="tests/artifacts/version_matrix"

sha="$(git rev-parse HEAD)"
dirty=false
if ! git diff --quiet || ! git diff --cached --quiet; then
  dirty=true
fi

echo "release-matrix-gate: SHA=$sha dirty=$dirty lanes=${LANES[*]}"

# Bring every lane up and wait for readiness.
bash scripts/version_matrix.sh up all
deadline=$((SECONDS + BOOT_TIMEOUT))
for lane in "${LANES[@]}"; do
  until bash scripts/version_matrix.sh health "$lane" >/dev/null 2>&1; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "release-matrix-gate: lane $lane never became healthy within ${BOOT_TIMEOUT}s" >&2
      exit 1
    fi
    sleep 5
  done
  echo "release-matrix-gate: $lane healthy"
done

# Run the full suite per lane, recording each verdict (keep going on failure so
# the artifact shows the complete picture).
declare -A verdict
overall=pass
for lane in "${LANES[@]}"; do
  if bash scripts/version_matrix.sh full "$lane"; then
    verdict[$lane]=pass
  else
    verdict[$lane]=fail
    overall=fail
  fi
done

mkdir -p "$OUT_DIR"
out="$OUT_DIR/results-$sha.json"
{
  printf '{\n'
  printf '  "sha": "%s",\n' "$sha"
  printf '  "dirty": %s,\n' "$dirty"
  printf '  "recorded_at_utc": "%s",\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '  "suite": "version_matrix.sh full",\n'
  printf '  "lanes": {\n'
  for i in "${!LANES[@]}"; do
    lane="${LANES[$i]}"
    sep=,
    [ "$i" -eq $((${#LANES[@]} - 1)) ] && sep=
    printf '    "%s": "%s"%s\n' "$lane" "${verdict[$lane]}" "$sep"
  done
  printf '  },\n'
  printf '  "overall": "%s"\n' "$overall"
  printf '}\n'
} >"$out"

echo "release-matrix-gate: wrote $out"
for lane in "${LANES[@]}"; do
  printf 'release-matrix-gate: %-7s %s\n' "$lane" "${verdict[$lane]}"
done

if [ "$overall" != "pass" ]; then
  echo "release-matrix-gate: FAILED — a release cannot ship from this SHA" >&2
  exit 1
fi
echo "release-matrix-gate: OK — commit $out so release preflight can verify it"
