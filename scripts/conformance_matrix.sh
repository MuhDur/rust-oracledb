#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_DIR="${ORACLEDB_RESULTS_DIR:-$ROOT/harness/.results}"
BASELINE_DIR="${ORACLEDB_BASELINE_DIR:-$ROOT/harness/.baseline}"
MATRIX_PATH="${ORACLEDB_MATRIX_PATH:-$RESULTS_DIR/matrix.json}"
DIFF_PATH="${ORACLEDB_MATRIX_DIFF_PATH:-$RESULTS_DIR/matrix-default-diff.json}"
FILTER_FILE="${ORACLEDB_FILTER_FILE:-$ROOT/harness/filter.txt}"
REFERENCE_DIR="${ORACLEDB_REFERENCE_DIR:-$ROOT/reference/python-oracledb}"
RUST_REPORT="$RESULTS_DIR/rust.json"

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/durakovic/.cargo-target-rust-oracledb-e72}"
export TMPDIR="${TMPDIR:-/home/durakovic/.tmp-rust-oracledb}"
mkdir -p "$RESULTS_DIR" "$CARGO_TARGET_DIR" "$TMPDIR"

if [ -n "${ORACLEDB_VENV_DIR:-}" ] && [ -x "$ORACLEDB_VENV_DIR/bin/python" ]; then
  PYTHON_BIN="$ORACLEDB_VENV_DIR/bin/python"
elif [ -n "${PYTHON:-}" ]; then
  PYTHON_BIN="$PYTHON"
elif [ -x "$ROOT/.venv-py313/bin/python" ]; then
  PYTHON_BIN="$ROOT/.venv-py313/bin/python"
elif [ -x "$ROOT/.venv/bin/python" ]; then
  PYTHON_BIN="$ROOT/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

need_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'conformance-matrix: missing required command: %s\n' "$1" >&2
    exit 2
  fi
}

need_command rustc
need_command cargo
need_command git

# Source the local Oracle test environment. Do not print it: it includes secrets.
eval "$("$ROOT/scripts/container.sh" env)"

CELL_ID="local-oracle-free-tcp-password-al32utf8-x86_64-gnu"
printf 'RUN %s: harness/run.sh rust\n' "$CELL_ID"
if "$ROOT/harness/run.sh" rust; then
  RUST_STATUS=0
else
  RUST_STATUS=$?
fi

DIFF_OUTPUT='{}'
DIFF_STATUS=127
if [ -s "$RUST_REPORT" ]; then
  printf 'RUN %s: harness/run.sh diff\n' "$CELL_ID"
  if DIFF_OUTPUT="$("$ROOT/harness/run.sh" diff)"; then
    DIFF_STATUS=0
  else
    DIFF_STATUS=$?
  fi
  printf '%s\n' "$DIFF_OUTPUT" > "$DIFF_PATH"
else
  printf 'FAIL %s: harness/run.sh rust exited %s and did not produce %s\n' \
    "$CELL_ID" "$RUST_STATUS" "$RUST_REPORT" >&2
  exit "$RUST_STATUS"
fi

DB_METADATA_JSON="$(
  REFERENCE_DIR="$REFERENCE_DIR" "$PYTHON_BIN" - <<'PY'
import json
import os
import platform
import subprocess
import sys


def run(cmd):
    try:
        return subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT).strip()
    except Exception as exc:  # pragma: no cover - diagnostic fallback
        return f"unavailable: {exc}"


try:
    import oracledb
except Exception as exc:
    print(json.dumps({"error": f"import oracledb failed: {exc}"}))
    sys.exit(1)


out = {
    "python": {
        "executable": sys.executable,
        "version": platform.python_version(),
        "oracledb_version": getattr(oracledb, "__version__", "unknown"),
    },
    "reference": {
        "git_describe": run(["git", "-C", os.environ["REFERENCE_DIR"], "describe", "--tags", "--always", "--dirty"]),
        "git_revision": run(["git", "-C", os.environ["REFERENCE_DIR"], "rev-parse", "HEAD"]),
    },
    "oracle": {},
}

conn = oracledb.connect(
    user=os.environ["PYO_TEST_MAIN_USER"],
    password=os.environ["PYO_TEST_MAIN_PASSWORD"],
    dsn=os.environ["PYO_TEST_CONNECT_STRING"],
)
try:
    out["oracle"]["connection_version"] = getattr(conn, "version", None)
    with conn.cursor() as cur:
        cur.execute("select banner_full from v$version where rownum = 1")
        out["oracle"]["server_banner"] = cur.fetchone()[0]
        cur.execute("select sys_context('USERENV', 'LANGUAGE') from dual")
        out["oracle"]["session_language"] = cur.fetchone()[0]
        cur.execute(
            """
            select parameter, value
            from nls_database_parameters
            where parameter in ('NLS_CHARACTERSET', 'NLS_NCHAR_CHARACTERSET')
            order by parameter
            """
        )
        out["oracle"]["nls_database_parameters"] = dict(cur.fetchall())
        cur.execute("select nls_charset_id('AL32UTF8') from dual")
        out["oracle"]["al32utf8_charset_id"] = cur.fetchone()[0]
finally:
    conn.close()

print(json.dumps(out, sort_keys=True))
PY
)"

TOOLCHAIN_JSON="$("$PYTHON_BIN" - <<'PY'
import json
import subprocess


def run(cmd):
    try:
        return subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT).strip()
    except Exception as exc:
        return f"unavailable: {exc}"


print(json.dumps({
    "rustc": run(["rustc", "--version"]),
    "rustc_verbose": run(["rustc", "-Vv"]),
    "cargo": run(["cargo", "--version"]),
    "active_toolchain": run(["rustup", "show", "active-toolchain"]),
}, sort_keys=True))
PY
)"

SUMMARY_JSON="$(
  DIFF_JSON="$DIFF_OUTPUT" \
  RUST_STATUS="$RUST_STATUS" \
  DIFF_STATUS="$DIFF_STATUS" \
  "$PYTHON_BIN" - <<'PY'
import json
import os
import sys

rust_status = int(os.environ["RUST_STATUS"])
diff_status = int(os.environ["DIFF_STATUS"])
try:
    diff = json.loads(os.environ["DIFF_JSON"])
except json.JSONDecodeError as exc:
    print(f"invalid diff JSON: {exc}", file=sys.stderr)
    sys.exit(2)

summary = {
    "baseline_count": int(diff.get("baseline_count", 0)),
    "current_count": int(diff.get("current_count", 0)),
    "regression_count": int(diff.get("regression_count", 0)),
    "missing_count": int(diff.get("missing_count", 0)),
    "beat_count": int(diff.get("beat_count", 0)),
    "rust_status": rust_status,
    "diff_status": diff_status,
}
summary["pass"] = (
    summary["baseline_count"] > 0
    and summary["current_count"] > 0
    and summary["regression_count"] == 0
    and summary["missing_count"] == 0
)
print(json.dumps(summary, sort_keys=True))
PY
)"

CELL_RESULT="$(
  SUMMARY_JSON="$SUMMARY_JSON" "$PYTHON_BIN" - <<'PY'
import json
import os

summary = json.loads(os.environ["SUMMARY_JSON"])
print("PASS" if summary["pass"] else "FAIL")
PY
)"

MATRIX_PATH="$MATRIX_PATH" \
DIFF_PATH="$DIFF_PATH" \
FILTER_FILE="$FILTER_FILE" \
BASELINE_PATH="$BASELINE_DIR/baseline.json" \
RUST_PATH="$RESULTS_DIR/rust.json" \
CELL_ID="$CELL_ID" \
SUMMARY_JSON="$SUMMARY_JSON" \
DIFF_JSON="$DIFF_OUTPUT" \
DB_METADATA_JSON="$DB_METADATA_JSON" \
TOOLCHAIN_JSON="$TOOLCHAIN_JSON" \
CARGO_TARGET_DIR="$CARGO_TARGET_DIR" \
TMPDIR="$TMPDIR" \
"$PYTHON_BIN" - <<'PY'
import datetime as dt
import json
import os
from pathlib import Path


def parse_filter(path: Path) -> list[str]:
    excludes = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("::", 1)[0].split()
        if len(parts) == 2 and parts[0] == "exclude":
            excludes.append(parts[1])
    return excludes


summary = json.loads(os.environ["SUMMARY_JSON"])
diff = json.loads(os.environ["DIFF_JSON"])
metadata = json.loads(os.environ["DB_METADATA_JSON"])
toolchain = json.loads(os.environ["TOOLCHAIN_JSON"])
filter_path = Path(os.environ["FILTER_FILE"])
excludes = parse_filter(filter_path)

cell_status = "GREEN" if summary["pass"] else "FAIL"
matrix = {
    "generated_at_utc": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
    "runner": "scripts/conformance_matrix.sh",
    "cell_results": [
        {
            "id": os.environ["CELL_ID"],
            "status": cell_status,
            "locally_runnable": True,
            "transport": "TCP",
            "oracle_server": metadata["oracle"],
            "auth": "password",
            "platform": "x86_64-unknown-linux-gnu",
            "charset": {
                "client_promised": "AL32UTF8",
                "client_charset_id": 873,
                "session_language": metadata["oracle"].get("session_language"),
                "database_nls": metadata["oracle"].get("nls_database_parameters"),
            },
            "evidence": {
                "baseline_path": os.environ["BASELINE_PATH"],
                "rust_path": os.environ["RUST_PATH"],
                "diff_path": os.environ["DIFF_PATH"],
                "baseline_count": summary["baseline_count"],
                "current_count": summary["current_count"],
                "regression_count": summary["regression_count"],
                "missing_count": summary["missing_count"],
                "beat_count": summary["beat_count"],
                "harness_rust_exit_status": summary["rust_status"],
                "harness_diff_exit_status": summary["diff_status"],
            },
        },
        {
            "id": "server-families-12.1-12.2-18c-19c-21c",
            "status": "MANUAL",
            "locally_runnable": False,
            "reason": "The single local container is Oracle Free 23.26.1.0.0; it cannot emulate older Oracle Database server releases.",
            "coverage": "Protocol version gates are covered by Rust tests; live differential coverage requires running this script against each older server family.",
        },
        {
            "id": "tcps-rustls-tls12-tls13",
            "status": "CI/MANUAL",
            "locally_runnable": False,
            "reason": "The gvenzl Oracle Free container exposes a TCP listener only; TCPS needs a TLS-configured Oracle listener and wallet.",
            "coverage": "cargo test --workspace covers TLS wallet parsing and the rustls handshake test; end-to-end Oracle TCPS remains manual with a TCPS listener.",
        },
        {
            "id": "x86_64-unknown-linux-musl",
            "status": "CI",
            "locally_runnable": False,
            "reason": "The python-oracledb differential harness runs on the host glibc Python/pytest environment; musl is covered as a release artifact build rather than a live pytest lane here.",
            "coverage": ".github/workflows/release.yml builds the static musl smoke binary; release-qualification also runs scripts/check_musl_size.sh.",
        },
        {
            "id": "non-password-auth-and-unsupported-auth-modes",
            "status": "CI/MANUAL",
            "locally_runnable": False,
            "reason": "The local differential cell uses password auth only; token auth requires TCPS and Kerberos/RADIUS/external auth are intentionally unsupported for 1.0.",
            "coverage": "Rust tests cover typed fail-closed paths such as AccessTokenRequiresTcps; real token auth requires a TCPS/token-capable database.",
        },
    ],
    "comparison_contract": {
        "command": "harness/run.sh diff",
        "comparator": "scripts/compare_pytest_json.py",
        "compared_fields": ["pytest JSON tests[].nodeid", "pytest JSON tests[].outcome"],
        "normalization": "The comparator reduces each pytest report to nodeid -> outcome. Durations, stdout/stderr, traceback text, ordering, and other pytest metadata are not compared.",
        "pass_condition": "regression_count == 0 and missing_count == 0",
        "allowlist": {
            "filter_file": str(filter_path),
            "exclude_patterns": excludes,
            "note": "No exclude patterns means the v4.0.1 filtered suite has no feature exclusions; thin-mode self-skips remain pytest skipped outcomes in both reports.",
        },
        "raw_diff": diff,
    },
    "versions": {
        "python": metadata["python"],
        "python_oracledb_reference": metadata["reference"],
        "toolchain": toolchain,
    },
    "redaction": {
        "connect_string": "redacted",
        "usernames": "redacted",
        "passwords": "redacted",
    },
    "paths": {
        "cargo_target_dir": os.environ["CARGO_TARGET_DIR"],
        "tmpdir": os.environ["TMPDIR"],
    },
}

matrix_path = Path(os.environ["MATRIX_PATH"])
matrix_path.write_text(json.dumps(matrix, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

SUMMARY_LINE="$(
  CELL_RESULT="$CELL_RESULT" CELL_ID="$CELL_ID" SUMMARY_JSON="$SUMMARY_JSON" "$PYTHON_BIN" - <<'PY'
import json
import os

summary = json.loads(os.environ["SUMMARY_JSON"])
print(
    f"{os.environ['CELL_RESULT']} {os.environ['CELL_ID']}: "
    f"baseline={summary['baseline_count']} "
    f"current={summary['current_count']} "
    f"regressions={summary['regression_count']} "
    f"missing={summary['missing_count']} "
    f"beats={summary['beat_count']} "
    f"rust_exit={summary['rust_status']} "
    f"diff_exit={summary['diff_status']}"
)
PY
)"
printf '%s\n' "$SUMMARY_LINE"
printf 'matrix: %s\n' "$MATRIX_PATH"

if [ "$CELL_RESULT" != "PASS" ]; then
  exit 1
fi
