#!/usr/bin/env python3
"""Merge pytest-json-report files produced by segmented harness runs."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path


def load_report(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        print(f"missing pytest JSON report at {path}", file=sys.stderr)
        raise SystemExit(2) from exc
    except json.JSONDecodeError as exc:
        print(f"invalid pytest JSON report at {path}: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("reports", nargs="+", type=Path)
    args = parser.parse_args()

    tests_by_nodeid: dict[str, dict] = {}
    duplicate_nodeids: list[str] = []
    outcome_counts: Counter[str] = Counter()
    collected = 0
    duration = 0.0
    exitcode = 0
    report_summaries = []

    for path in args.reports:
        report = load_report(path)
        summary = report.get("summary", {})
        report_summaries.append(
            {
                "path": str(path),
                "exitcode": report.get("exitcode"),
                "summary": summary,
            }
        )
        collected += int(summary.get("collected", 0))
        duration += float(report.get("duration") or 0.0)
        if report.get("exitcode", 0) != 0:
            exitcode = 1
        for test in report.get("tests", []):
            nodeid = test.get("nodeid")
            if not nodeid:
                continue
            if nodeid in tests_by_nodeid:
                duplicate_nodeids.append(nodeid)
            tests_by_nodeid[nodeid] = test

    if duplicate_nodeids:
        print(
            "duplicate pytest nodeids in segmented reports: "
            + ", ".join(sorted(set(duplicate_nodeids))[:20]),
            file=sys.stderr,
        )
        return 2

    tests = list(tests_by_nodeid.values())
    for test in tests:
        outcome_counts[str(test.get("outcome", "unknown"))] += 1

    summary = dict(sorted(outcome_counts.items()))
    summary["collected"] = collected
    summary["total"] = len(tests)

    merged = {
        "created": "merged-by-rust-oracledb-harness",
        "duration": duration,
        "exitcode": exitcode,
        "root": str(Path.cwd()),
        "summary": summary,
        "tests": tests,
        "segmented_reports": report_summaries,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(merged, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"output": str(args.output), "summary": summary}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
