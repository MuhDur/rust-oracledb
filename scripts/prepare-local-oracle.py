#!/usr/bin/env python3
"""Prepare the disposable local Oracle container for the reference suite."""

from __future__ import annotations

import json
import os
import sys

import oracledb


REQUIRED_COMPAT_ROLES = {"CTXAPP": "create role CTXAPP"}


def env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"missing required environment variable: {name}")
    return value


def ensure_local_connect_string(connect_string: str) -> None:
    allowed_prefixes = ("localhost:", "127.0.0.1:", "[::1]:")
    if not connect_string.startswith(allowed_prefixes):
        raise SystemExit(
            "refusing to prepare a non-local Oracle connect string: "
            f"{connect_string!r}"
        )


def main() -> int:
    connect_string = env("PYO_TEST_CONNECT_STRING")
    ensure_local_connect_string(connect_string)

    conn = oracledb.connect(
        user=env("PYO_TEST_ADMIN_USER"),
        password=env("PYO_TEST_ADMIN_PASSWORD"),
        dsn=connect_string,
    )
    conn.autocommit = True
    cursor = conn.cursor()
    created: list[str] = []
    existing: list[str] = []

    for role, create_sql in REQUIRED_COMPAT_ROLES.items():
        cursor.execute("select count(*) from dba_roles where role = :1", [role])
        if cursor.fetchone()[0]:
            existing.append(role)
            continue
        cursor.execute(create_sql)
        created.append(role)

    print(json.dumps({"created": created, "existing": existing}, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except oracledb.Error as exc:
        print(f"Oracle preparation failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
