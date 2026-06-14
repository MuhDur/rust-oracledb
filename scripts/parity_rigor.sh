#!/usr/bin/env bash
# parity_rigor.sh — PARITY-RIGOR reproduction (bead rust-oracledb-0na).
#
# Proves the 116 skipped reference tests are skipped because the ENVIRONMENT /
# thin-mode contract gates them, not because the Rust engine would fail them, and
# demonstrates the capabilities the local 23ai container CAN stand up (DRCP).
#
# ADDITIVE & READ-MOSTLY: this script never edits crate/shim logic. It runs the
# existing harness, queries the container, and (for the DRCP demo) starts the
# DRCP pool at the CDB root. It creates and then drops a throw-away
# IDENTIFIED EXTERNALLY probe user for the external-auth disproof. It never
# deletes files, never rm -rf, never git reset/clean/checkout.
#
# Usage:
#   scripts/parity_rigor.sh                 # full: baseline + rust + taxonomy + DRCP + extauth disproof
#   scripts/parity_rigor.sh --taxonomy      # just enumerate/bucket the skips from an existing baseline
#   scripts/parity_rigor.sh --baseline      # (re)run the reference thin baseline (segmented)
#   scripts/parity_rigor.sh --rust          # (re)run the Rust engine suite (per-module, timeout-guarded)
#   scripts/parity_rigor.sh --drcp          # stand DRCP up + prove a pooled connect through the Rust engine
#   scripts/parity_rigor.sh --externalauth  # disprove external auth: reference thin FAILS all 17 when un-gated
#
# Environment knobs (with lane defaults):
#   ORACLEDB_CONTAINER_NAME (rust-oracledb-lane-1526)
#   ORACLEDB_HOST_PORT      (1526)
#   ORACLE_PASSWORD         (OracledbTest#2026)   # SYSTEM password for the container
#   CARGO_TARGET_DIR / TMPDIR are honoured if exported by the caller.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CONTAINER_NAME="${ORACLEDB_CONTAINER_NAME:-rust-oracledb-lane-1526}"
HOST_PORT="${ORACLEDB_HOST_PORT:-1526}"
ORACLE_PASSWORD="${ORACLE_PASSWORD:-OracledbTest#2026}"
LOGS="$ROOT/.lane-logs"
mkdir -p "$LOGS"

VENV_PY="$ROOT/.venv-py313/bin/python"
if [ ! -x "$VENV_PY" ]; then VENV_PY="$(command -v python3)"; fi

note() { printf '\n=== %s ===\n' "$*"; }

load_env() {
  # Export the PYO_TEST_* env the harness expects, pointed at the lane container.
  eval "$(ORACLEDB_CONTAINER_NAME="$CONTAINER_NAME" ORACLEDB_HOST_PORT="$HOST_PORT" \
    "$ROOT/scripts/container.sh" env)"
}

sqlplus_root() {
  # Run SQL at the CDB root as sysdba inside the container. Reads SQL from stdin.
  docker exec -i "$CONTAINER_NAME" bash -lc 'sqlplus -s / as sysdba' 2>&1
}

sqlplus_pdb() {
  # Run SQL in FREEPDB1 as SYSTEM inside the container. Reads SQL from stdin.
  docker exec -i -e P="$ORACLE_PASSWORD" "$CONTAINER_NAME" \
    bash -lc 'sqlplus -s system/"$P"@localhost:1521/FREEPDB1' 2>&1
}

# ---------------------------------------------------------------------------
# Taxonomy: enumerate + bucket the skips from a pytest-json baseline report.
# ---------------------------------------------------------------------------
taxonomy() {
  local report="${1:-$ROOT/harness/.baseline/baseline.json}"
  if [ ! -f "$report" ]; then
    echo "no baseline report at $report — run: scripts/parity_rigor.sh --baseline" >&2
    return 2
  fi
  note "Skip taxonomy from $report"
  "$VENV_PY" - "$report" <<'PY'
import json, re, sys
from collections import Counter, defaultdict
r = json.load(open(sys.argv[1]))
summ = r.get("summary", {})
print(f"summary: passed={summ.get('passed')} skipped={summ.get('skipped')} total={summ.get('total')}")
rows = []
for t in r["tests"]:
    if t.get("outcome") != "skipped":
        continue
    reason = "??"
    for ph in ("setup", "call", "teardown"):
        lr = t.get(ph, {}).get("longrepr")
        if lr:
            m = re.search(r"Skipped:\s*(.*?)'\)$", lr)
            if m:
                reason = m.group(1)
                break
    rows.append((t["nodeid"].replace("tests/", ""), reason))
rows.sort()
print(f"\nTOTAL SKIPS: {len(rows)}\n")
for reason, n in Counter(r for _, r in rows).most_common():
    print(f"{n:4d}  {reason}")
print("\n--- module x reason ---")
mr = defaultdict(Counter)
for nid, reason in rows:
    mr[nid.split('::')[0]][reason] += 1
for mod in sorted(mr):
    parts = ", ".join(f"{rs}={c}" for rs, c in mr[mod].most_common())
    print(f"{sum(mr[mod].values()):3d}  {mod:42s} {parts}")
PY
}

# ---------------------------------------------------------------------------
# Baseline: reference python-oracledb thin driver, segmented (authoritative).
# ---------------------------------------------------------------------------
run_baseline() {
  note "Reference thin baseline (segmented)"
  load_env
  ORACLEDB_HARNESS_MODE=segmented "$ROOT/harness/run.sh" baseline
}

# ---------------------------------------------------------------------------
# Rust engine: per-module, timeout-guarded (one shutdown hang can't stall all).
# ---------------------------------------------------------------------------
run_rust() {
  note "Rust engine suite (per-module, 200s timeout each)"
  load_env
  # Build/refresh the shim once.
  local prefix; prefix="$("$VENV_PY" -c 'import sys; print(sys.prefix)')"
  VIRTUAL_ENV="$prefix" PATH="$prefix/bin:$PATH" \
    "$VENV_PY" -m maturin develop -m "$ROOT/crates/oracledb-pyshim/Cargo.toml"
  local out="$LOGS/rust-sweep"; mkdir -p "$out"
  local i=0
  local total; total="$("$VENV_PY" "$ROOT/scripts/select_tests.py" \
    --reference "$ROOT/reference/python-oracledb" --filter "$ROOT/harness/filter.txt" | wc -l)"
  "$VENV_PY" "$ROOT/scripts/select_tests.py" \
    --reference "$ROOT/reference/python-oracledb" --filter "$ROOT/harness/filter.txt" \
  | while read -r t; do
      i=$((i + 1))
      local base; base="$(basename "$t" .py)"
      timeout 200 env PYTHONPATH="$ROOT/harness" "$VENV_PY" -m pytest "$t" \
        -p shim_inject -p no:cacheprovider -q --tb=line \
        --json-report --json-report-file "$out/$(printf '%03d' "$i")-$base.json" \
        > "$out/$base.txt" 2>&1
      printf '%3d/%s  rc=%s  %s\n' "$i" "$total" "$?" "$base"
    done
  # Merge into one manifest and show the skip taxonomy for the Rust run too.
  "$VENV_PY" "$ROOT/scripts/merge_pytest_json.py" \
    --output "$LOGS/rust-sweep.json" "$out"/*.json
  taxonomy "$LOGS/rust-sweep.json"
}

# ---------------------------------------------------------------------------
# DRCP: stand the pool up at the CDB root, then prove a POOLED connect works
# through the Rust engine. (Not a skip->pass conversion: the harness default DSN
# is dedicated, so the 27 skip_if_drcp tests already RUN+PASS. Switching to
# :POOLED would make them START skipping. This proves the capability instead.)
# ---------------------------------------------------------------------------
drcp() {
  note "DRCP: start pool at CDB root"
  printf 'set serveroutput on\nwhenever sqlerror continue\nbegin dbms_connection_pool.start_pool(); dbms_output.put_line('"'"'START OK'"'"'); exception when others then dbms_output.put_line('"'"'start: '"'"'||sqlerrm); end;\n/\n' \
    | sqlplus_root | tail -4

  note "DRCP: pooled connect through the Rust engine"
  load_env
  local dsn="localhost:${HOST_PORT}/FREEPDB1:POOLED"
  PYTHONPATH="$ROOT/harness" "$VENV_PY" - "$dsn" <<'PY'
import os, sys, oracledb, oracledb_pyshim  # noqa: F401  (shim must import)
dsn = sys.argv[1]
p = oracledb.ConnectParams(); p.parse_connect_string(dsn)
print("connect-string server_type:", p.server_type, "-> is_drcp:", p.server_type == "pooled")
try:
    conn = oracledb.connect(
        user=os.environ["PYO_TEST_MAIN_USER"],
        password=os.environ["PYO_TEST_MAIN_PASSWORD"],
        dsn=dsn, cclass="RIGOR", purity=oracledb.PURITY_SELF,
    )
    cur = conn.cursor()
    cur.execute("select 1+1 from dual")
    print("DRCP query through Rust engine:", cur.fetchone())
    conn.close()
    print("RESULT: DRCP-through-Rust OK")
except Exception as exc:  # noqa: BLE001
    print("RESULT: DRCP connect FAILED:", type(exc).__name__, str(exc).splitlines()[0])
    sys.exit(1)
PY
}

# ---------------------------------------------------------------------------
# External-auth disproof: un-gate test_5000 by setting PYO_TEST_EXTERNAL_USER to
# a freshly-created IDENTIFIED EXTERNALLY user, run it through the *reference*
# thin driver, and show all 17 FAIL (thin can't do bequeath/OS auth). Proves the
# skip is a correct environment/thin gate, not an engine defect. Cleans up after.
# ---------------------------------------------------------------------------
externalauth() {
  note "External-auth disproof: create probe IDENTIFIED EXTERNALLY user (ops\$oracle)"
  printf "create user ops\$oracle identified externally;\ngrant create session to ops\$oracle;\n" \
    | sqlplus_pdb | tail -3

  note "External-auth: run test_5000 through the REFERENCE thin driver (expect 17 FAILED)"
  load_env
  PYO_TEST_EXTERNAL_USER='ops$oracle' \
    "$VENV_PY" -m pytest "$ROOT/reference/python-oracledb/tests/test_5000_externalauth.py" \
    -p no:cacheprovider --tb=line -q 2>&1 | tail -22

  note "External-auth: drop probe user (keep DB pristine)"
  printf "drop user ops\$oracle cascade;\n" | sqlplus_pdb | tail -2
}

main() {
  case "${1:---all}" in
    --taxonomy)     taxonomy ;;
    --baseline)     run_baseline ;;
    --rust)         run_rust ;;
    --drcp)         drcp ;;
    --externalauth) externalauth ;;
    --all|"")
      run_baseline
      taxonomy
      drcp
      externalauth
      note "DONE — see docs/PARITY_SKIPS.md for the full taxonomy"
      ;;
    *)
      echo "unknown option: $1" >&2
      grep -E '^#   scripts/parity_rigor.sh' "$0" >&2
      exit 2 ;;
  esac
}

main "$@"
