#!/usr/bin/env bash
# Release gate: run the FULL live version matrix (scripts/version_matrix.sh
# full) against every lane and record the verdict for the current git SHA.
# "Every lane" = the four server generations (xe11/xe18/xe21/free23) PLUS the
# local OCI TCPS lane (octcps, A5.2) — the latter has no container and runs the
# rustls TCPS + wallet suites over the C1 synthetic fixtures.
#
# A release CANNOT ship without a green record from this script for the exact
# release SHA. It runs in the manual Release Qualification workflow, which
# uploads the result as an immutable GitHub Actions artifact. The tag workflow
# downloads that artifact outside its clean checkout and verifies it with
# scripts/verify_release_exact_sha.py. It is deliberately not committed: adding
# a result file would change the SHA that the result claims to prove. Workflow:
#
#   1. Dispatch Release Qualification for the exact commit you intend to tag.
#   2. Its matrix-evidence job runs this script and uploads the result.
#   3. Push vX.Y.Z on that same commit; release.yml verifies the artifact before
#      any publish job can start.
#
# The artifact records per-lane pass/fail, the required free23 TSTZ descriptor
# probe, the SHA it ran on, and whether the worktree was dirty (a dirty run is
# recorded but REJECTED by preflight — the gate must run on exactly the tree
# being released).
#
# usage: scripts/release_matrix_gate.sh
#   env: ORACLEDB_MATRIX_BOOT_TIMEOUT_SECS (default 600) — container boot wait
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BOOT_TIMEOUT="${ORACLEDB_MATRIX_BOOT_TIMEOUT_SECS:-600}"
# Container lanes (one per server generation) — these get `up`/`health`.
LANES=(xe11 xe18 xe21 free23)
# The full gate set also runs the OCI TCPS lane (A5.2 / bead iec3.1.26): a local
# rustls TCPS + wallet lane over the C1 synthetic fixtures. It has no container,
# so it is NOT brought up/health-checked — it just runs its `full` suite and its
# verdict lands in the same artifact the preflight verifies.
GATE_LANES=("${LANES[@]}" octcps)
OUT_DIR="tests/artifacts/version_matrix"

sha="$(git rev-parse HEAD)"
dirty=false
if ! git diff --quiet || ! git diff --cached --quiet; then
  dirty=true
fi

echo "release-matrix-gate: SHA=$sha dirty=$dirty lanes=${LANES[*]}"

# The free23 lane's suite user defaults to the locally-bootstrapped `pythontest`
# (scripts/version_matrix.sh lane_fields), but this gate provisions EVERY gvenzl
# lane with APP_USER=testuser (version_matrix.sh lane_up boots `-e APP_USER`).
# Connect free23 as that same provisioned user so the gate is self-consistent —
# identical to the green "Version matrix" workflow, which sets exactly this. A
# mismatch here is what surfaced as free23 ORA-01017 (the nonexistent pythontest
# user). Honor an explicit override if the operator points the gate at a
# differently-provisioned listener.
export PYO_TEST_MAIN_USER="${PYO_TEST_MAIN_USER:-${ORACLEDB_MATRIX_APP_USER:-testuser}}"
export PYO_TEST_MAIN_PASSWORD="${PYO_TEST_MAIN_PASSWORD:-${ORACLEDB_MATRIX_APP_PASSWORD:-testpw}}"

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

# Run the full suite per lane (container generations + the OCI TCPS lane),
# recording each verdict (keep going on failure so the artifact shows the
# complete picture).
declare -A verdict probe
overall=PASS
for lane in "${GATE_LANES[@]}"; do
  if bash scripts/version_matrix.sh full "$lane"; then
    verdict[$lane]=PASS
    # `lane_full free23` runs the ignored ALL_TYPE_ATTRS descriptor probe
    # after matrix_full and fails the lane if it fails. Record it separately
    # so preflight can reject an artifact that predates this release gate.
    if [ "$lane" = "free23" ]; then
      probe[free23_tstz_descriptor]=PASS
    fi
  else
    verdict[$lane]=FAIL
    if [ "$lane" = "free23" ]; then
      probe[free23_tstz_descriptor]=FAIL
    fi
    overall=FAIL
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
  for i in "${!GATE_LANES[@]}"; do
    lane="${GATE_LANES[$i]}"
    sep=,
    [ "$i" -eq $((${#GATE_LANES[@]} - 1)) ] && sep=
    printf '    "%s": "%s"%s\n' "$lane" "${verdict[$lane]}" "$sep"
  done
  printf '  },\n'
  printf '  "probes": {"free23_tstz_descriptor": "%s"},\n' \
    "${probe[free23_tstz_descriptor]:-FAIL}"
  printf '  "overall": "%s"\n' "$overall"
  printf '}\n'
} >"$out"

echo "release-matrix-gate: wrote $out"
for lane in "${GATE_LANES[@]}"; do
  printf 'release-matrix-gate: %-7s %s\n' "$lane" "${verdict[$lane]}"
done

if [ "$overall" != "PASS" ]; then
  echo "release-matrix-gate: FAILED — a release cannot ship from this SHA" >&2
  exit 1
fi
echo "release-matrix-gate: OK — upload $out as exact-SHA qualification evidence"
