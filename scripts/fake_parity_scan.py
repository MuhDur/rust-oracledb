#!/usr/bin/env python3
"""Small guardrail scan for shim-side protocol logic."""

from __future__ import annotations

import argparse
from pathlib import Path

FORBIDDEN = [
    "select ",
    "insert ",
    "update ",
    "delete ",
    "begin ",
    "tns",
    "ttc",
    "auth_vfr_data",
    "pbkdf2",
    "oson",
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="+", type=Path)
    args = parser.parse_args()

    findings: list[str] = []
    for root in args.paths:
        files = [root] if root.is_file() else sorted(root.rglob("*"))
        for path in files:
            if not path.is_file() or path.suffix not in {".rs", ".py", ".sh"}:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore").lower()
            for needle in FORBIDDEN:
                if needle in text:
                    findings.append(f"{path}: contains {needle!r}")

    if findings:
        print("\n".join(findings))
        return 1
    print("fake-parity scan clean")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
