#!/usr/bin/env python3
"""Compare pytest-json-report manifests under the match-or-beat contract."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PASSING = {"passed"}
NON_BEATABLE = {"failed", "error"}


def load(path: Path) -> dict[str, str]:
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        print(f"invalid pytest JSON report at {path}: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    tests = report.get("tests", [])
    return {case["nodeid"]: case["outcome"] for case in tests}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("baseline", type=Path)
    parser.add_argument("current", type=Path)
    parser.add_argument("--max-details", type=int, default=50)
    args = parser.parse_args()

    baseline = load(args.baseline)
    current = load(args.current)
    regressions: list[str] = []
    beats: list[str] = []
    missing: list[str] = []

    for nodeid, baseline_outcome in baseline.items():
        current_outcome = current.get(nodeid)
        if current_outcome is None:
            missing.append(nodeid)
            continue
        if baseline_outcome in PASSING and current_outcome not in PASSING:
            regressions.append(f"{nodeid}: baseline={baseline_outcome} current={current_outcome}")
        if baseline_outcome in NON_BEATABLE and current_outcome in PASSING:
            beats.append(f"{nodeid}: baseline={baseline_outcome} current={current_outcome}")

    summary = {
        "baseline_count": len(baseline),
        "current_count": len(current),
        "regression_count": len(regressions),
        "beat_count": len(beats),
        "missing_count": len(missing),
        "regressions": regressions[: args.max_details],
        "beats": beats[: args.max_details],
        "missing": missing[: args.max_details],
        "regressions_truncated": max(len(regressions) - args.max_details, 0),
        "beats_truncated": max(len(beats) - args.max_details, 0),
        "missing_truncated": max(len(missing) - args.max_details, 0),
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 1 if regressions or missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
