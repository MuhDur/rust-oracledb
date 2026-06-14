#!/usr/bin/env python3
"""Fake-parity guardrail scan.

The port's central honesty invariant is: the PyO3 shim
(`crates/oracledb-pyshim`) does *marshalling only*. Every server-derived value
the shim hands back to Python must come from a real wire round-trip through the
`oracledb` driver -> `oracledb-protocol` codecs -> the Oracle server. The shim
must never *fabricate* query results, rows, or server-computed values locally.

A naive substring scan (the previous version) flagged any occurrence of
``select``/``tns``/``ttc``/``oson``/``pbkdf2`` anywhere and therefore could
never run clean: the protocol crate legitimately *is* the TNS/TTC/OSON/PBKDF2
codec, and the shim legitimately *passes SQL strings through* to the driver
(e.g. ``select column_name ... from all_tab_columns`` metadata probes, or
``select dbms_sql_monitor.begin_operation(...) from dual``). Those are real
round-trips, not fake parity.

This scanner draws the real distinction:

  * Protocol routing / codec keywords (tns, ttc, oson, pbkdf2, auth_vfr_data)
    are ALLOWED everywhere. They name protocol-layer concerns, not fabrication.

  * SQL strings inside the shim are ALLOWED. The shim has no SQL engine; a SQL
    literal is data handed to ``driver.execute_query*`` and executed on the
    real server. SQL keywords are therefore NOT a fabrication signal.

  * What IS flagged is shim-side *fabrication of server-derived results*: code
    in the pyshim that manufactures rows / cells / server-computed values and
    returns them to Python WITHOUT a driver round-trip. Concretely:

      - Re-introducing a removed client-side protocol SIMULATION
        (``dbms_output`` line buffering, ``v$sql_monitor`` fabrication, a
        client-side query-result cache masquerading as fetched rows).
      - Building a "fetched" result row out of hardcoded literals.
      - Locally computing a value that the server is supposed to compute
        (faking a ``select <expr> from dual`` answer in Rust).

Detectors are deliberately conservative (high precision): each fires on a
shim-side construction signal, not on the mere presence of a SQL/protocol word.

Run modes:
    fake_parity_scan.py <path> [<path> ...]   # scan the given files/dirs
    fake_parity_scan.py --self-test           # prove detectors catch a plant
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

# ---------------------------------------------------------------------------
# Crate roles. The fabrication risk lives only in the shim; the protocol crate
# *is* the codec layer and is exempt from the result-fabrication detectors.
# ---------------------------------------------------------------------------
SHIM_CRATE_MARKER = "oracledb-pyshim"
PROTOCOL_CRATE_MARKER = "oracledb-protocol"

# Protocol routing / codec keywords that are legitimate wherever they appear.
# Kept here purely as documentation of what the scanner intentionally IGNORES
# (the old scanner wrongly flagged every one of these).
ALLOWED_PROTOCOL_KEYWORDS = ("tns", "ttc", "oson", "pbkdf2", "auth_vfr_data")


@dataclass(frozen=True)
class Detector:
    name: str
    # Regex that signals shim-side fabrication of a server-derived result.
    pattern: re.Pattern[str]
    why: str


# Each detector targets *construction of fabricated results in the shim*, never
# the mere mention of a SQL/protocol token. Patterns are matched against source
# with line comments stripped (see _strip_line_comments) so that an explanatory
# comment naming a forbidden concept does not trip the scan; only live code does.
DETECTORS: tuple[Detector, ...] = (
    Detector(
        name="dbms_output-simulation",
        # A client-side dbms_output line buffer / get_line emulation: the shim
        # accumulating output lines in Rust instead of fetching them over TTC.
        pattern=re.compile(
            r"(dbms_output|get_line|getlines)[_\s]*"
            r"(buffer|cache|lines|queue|vec!|vec<|VecDeque|push|simulat)",
            re.IGNORECASE,
        ),
        why="client-side dbms_output buffering must go over the wire, not be simulated in the shim",
    ),
    Detector(
        name="sql-monitor-fabrication",
        # Fabricating v$sql_monitor / sql_monitor report rows in the shim.
        pattern=re.compile(
            r"(v\$sql_monitor|sql_monitor)[_\s]*"
            r"(report|rows?|fabricat|build|format!|synthes)",
            re.IGNORECASE,
        ),
        why="sql_monitor report data must be fetched from the server, not built in the shim",
    ),
    Detector(
        name="fabricated-result-rows",
        # The shim assembling a QueryResult / fetched rows out of literals and
        # returning it as if it came off the wire. We require BOTH a result-row
        # type and a fabrication verb on the same logical statement.
        pattern=re.compile(
            r"(QueryResult|fetched?_rows|result\.rows)\s*=\s*"
            r"(vec!\s*\[|fabricat|synthes|hardcod|fixture|mock)",
            re.IGNORECASE,
        ),
        why="result rows must come from the driver fetch path, not be assembled from literals",
    ),
    Detector(
        name="server-computed-value-fabrication",
        # Locally computing a value the server is supposed to compute, e.g.
        # faking the answer to a `select <expr> from dual` probe in Rust.
        pattern=re.compile(
            r"(from\s+dual|server[_-]?computed|compute_server)\s*"
            r".{0,40}?(return|=)\s*(Ok\()?\s*[\"']?\d",
            re.IGNORECASE | re.DOTALL,
        ),
        why="`select <expr> from dual` answers must be computed by the server, not faked in the shim",
    ),
    Detector(
        name="offline-result-cache",
        # A client-side cache that serves "query results" without a round-trip.
        pattern=re.compile(
            r"(offline|fake|simulated|local)[_-]?(result|row|query)[_-]?(cache|store|table)",
            re.IGNORECASE,
        ),
        why="serving query results from a client-side cache is fake parity",
    ),
)


def _classify_crate(path: Path) -> str | None:
    parts = path.as_posix()
    if SHIM_CRATE_MARKER in parts:
        return "shim"
    if PROTOCOL_CRATE_MARKER in parts:
        return "protocol"
    return None


def _strip_line_comments(text: str) -> str:
    """Drop ``//`` line comments and ``#`` comments so a doc/comment naming a
    forbidden concept does not trip a detector. Block comments and string
    contents are left intact deliberately: a fabricated literal lives in code
    or in a string, and that is exactly what we want to catch."""
    out: list[str] = []
    for line in text.splitlines():
        stripped = line.lstrip()
        # Whole-line Rust/py comment.
        if stripped.startswith("//") or stripped.startswith("#"):
            continue
        # Trailing `// ...` comment on a code line (not inside an obvious URL).
        idx = line.find("//")
        if idx != -1 and "://" not in line[max(0, idx - 1):idx + 2]:
            line = line[:idx]
        out.append(line)
    return "\n".join(out)


def scan_file(path: Path) -> list[str]:
    crate = _classify_crate(path)
    # Result-fabrication detectors only apply to the shim. The protocol crate
    # is the codec layer; SQL/protocol keywords there are by definition real.
    if crate != "shim":
        return []
    raw = path.read_text(encoding="utf-8", errors="ignore")
    code = _strip_line_comments(raw)
    findings: list[str] = []
    for det in DETECTORS:
        m = det.pattern.search(code)
        if m:
            snippet = m.group(0).replace("\n", " ").strip()
            if len(snippet) > 80:
                snippet = snippet[:77] + "..."
            findings.append(f"{path}: [{det.name}] {det.why}\n    matched: {snippet!r}")
    return findings


def iter_source_files(roots: list[Path]):
    for root in roots:
        if root.is_file():
            if root.suffix in {".rs", ".py", ".sh"}:
                yield root
            continue
        for path in sorted(root.rglob("*")):
            if path.is_file() and path.suffix in {".rs", ".py", ".sh"}:
                yield path


# ---------------------------------------------------------------------------
# Self-test: documented test vectors. The NEGATIVE vectors are real shapes that
# appear in the current shim and MUST NOT trip the scan; the POSITIVE vectors
# are planted fabrications the scan MUST catch.
# ---------------------------------------------------------------------------
NEGATIVE_VECTORS = (
    # Legitimate SQL passthrough metadata probe (executed by the driver).
    'let rows = self.query_rows_with_binds("select column_name, data_type '
    'from all_tab_columns where owner = :1", &binds)?;',
    # Legitimate server-side sql_monitor begin via real SQL round-trip.
    'self.query_first_text("select dbms_sql_monitor.begin_operation(:1, null, '
    "'Y') from dual\")?;",
    # Protocol keyword mention in a comment / error type (allowed).
    "// the TTC opcode and OSON image are decoded in the protocol crate",
    # Real from-dual probe sent to the server (string is data for the driver).
    'self.query_first_text("select sys_context(\'USERENV\', \'SERVICE_NAME\') from dual")?;',
)

POSITIVE_VECTORS = (
    # Planted: client-side dbms_output simulation buffer.
    "let mut dbms_output_buffer: Vec<String> = Vec::new();",
    # Planted: fabricated v$sql_monitor report.
    'let sql_monitor_report = format!("SQL Monitoring Report\\n{}", body);',
    # Planted: result rows assembled from literals.
    'let fetched_rows = vec![vec![QueryValue::Text(\"12\".into())]];\n'
    "result.rows = vec![row];",
    # Planted: faking a `select 7+5 from dual` answer in Rust.
    'if sql.contains("from dual") { return Ok(12); }',
    # Planted: offline result cache serving rows without a round-trip.
    "static FAKE_RESULT_CACHE: &[(&str, &str)] = &[];",
)


def run_self_test() -> int:
    import tempfile

    failures: list[str] = []
    with tempfile.TemporaryDirectory() as td:
        shim_dir = Path(td) / "crates" / "oracledb-pyshim" / "src"
        shim_dir.mkdir(parents=True)

        # Negative vectors: must produce ZERO findings.
        for i, vec in enumerate(NEGATIVE_VECTORS):
            f = shim_dir / f"neg_{i}.rs"
            f.write_text(vec + "\n", encoding="utf-8")
            found = scan_file(f)
            if found:
                failures.append(
                    f"FALSE POSITIVE on legitimate code:\n  vector: {vec!r}\n  -> {found}"
                )

        # Positive vectors: each must produce at least one finding.
        for i, vec in enumerate(POSITIVE_VECTORS):
            f = shim_dir / f"pos_{i}.rs"
            f.write_text(vec + "\n", encoding="utf-8")
            found = scan_file(f)
            if not found:
                failures.append(f"MISSED fabrication plant:\n  vector: {vec!r}")

        # Crate-scope check: the same fabrication in the protocol crate is NOT
        # flagged (it is the codec layer, exempt from result-fabrication rules).
        proto_dir = Path(td) / "crates" / "oracledb-protocol" / "src"
        proto_dir.mkdir(parents=True)
        pf = proto_dir / "oson.rs"
        pf.write_text(POSITIVE_VECTORS[0] + "\n", encoding="utf-8")
        if scan_file(pf):
            failures.append("crate-scope leak: protocol-crate code was flagged")

    if failures:
        print("SELF-TEST FAILED:")
        print("\n".join(failures))
        return 1
    print(
        f"self-test passed: {len(NEGATIVE_VECTORS)} legitimate shapes clean, "
        f"{len(POSITIVE_VECTORS)} fabrication plants caught, crate-scope honored"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="run the built-in detector test vectors and exit",
    )
    parser.add_argument("paths", nargs="*", type=Path)
    args = parser.parse_args()

    if args.self_test:
        return run_self_test()

    if not args.paths:
        parser.error("provide one or more paths to scan, or --self-test")

    findings: list[str] = []
    for path in iter_source_files(args.paths):
        findings.extend(scan_file(path))

    if findings:
        print("FAKE-PARITY RISK — shim-side result fabrication detected:")
        print("\n".join(findings))
        return 1
    print("fake-parity scan clean (no shim-side result fabrication)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
