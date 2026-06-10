#!/usr/bin/env python3
"""Select the filtered python-oracledb module set."""

from __future__ import annotations

import argparse
import fnmatch
from pathlib import Path


def parse_excludes(path: Path) -> list[str]:
    excludes: list[str] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("::", 1)[0].split()
        if len(parts) == 2 and parts[0] == "exclude":
            excludes.append(parts[1])
    return excludes


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference", required=True, type=Path)
    parser.add_argument("--filter", required=True, type=Path)
    args = parser.parse_args()

    tests_dir = args.reference / "tests"
    excludes = parse_excludes(args.filter)
    selected = []
    for path in sorted(tests_dir.glob("test_*.py")):
        if any(fnmatch.fnmatchcase(path.name, pattern) for pattern in excludes):
            continue
        selected.append(path)

    for path in selected:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
