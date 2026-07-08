#!/usr/bin/env python3
"""Generate the async-to-blocking public surface coverage table.

The input is a cargo-public-api snapshot. This deliberately uses the reviewed
public API inventory instead of parsing Rust source, so the table tracks the
surface external callers can actually name.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path


METHOD_RE = re.compile(r"^pub (?P<async>async )?fn (?P<full>oracledb::[^(]+)\(")


def normalize_owner(owner: str) -> str:
    return re.sub(r"<[^>]*>", "", owner)


@dataclass(frozen=True)
class Method:
    owner: str
    name: str
    is_async: bool
    line_no: int


@dataclass(frozen=True)
class Surface:
    name: str
    async_owner: str
    blocking_owner: str
    method_map: dict[str, str] = field(default_factory=dict)
    exceptions: dict[str, str] = field(default_factory=dict)
    include_sync: bool = False


SURFACES = [
    Surface(
        name="connection",
        async_owner="oracledb::Connection",
        blocking_owner="oracledb::BlockingConnection",
        exceptions={
            "direct_path_load_stream": "Low-level direct-path streaming primitive stays async-only; blocking direct_path_load/load_prepared cover owned batches.",
            "direct_path_op": "Low-level direct-path operation primitive stays async-only; blocking direct_path_load/load_prepared cover owned batches.",
            "fetch_rows_ref": "Zero-copy borrowed fetch is async-only because borrowed buffers cannot cross the block_on facade.",
            "fetch_rows_ref_response": "Zero-copy borrowed fetch response is async-only because borrowed buffers cannot cross the block_on facade.",
            "fetch_rows_request": "Half-round-trip borrowed-fetch primitive is async-only; blocking callers use fetch_rows/fetch_rows_with_columns.",
            "for_each_row_ref": "Borrowed-row callback is async-only because QueryValueRef lifetimes cannot cross the block_on facade.",
            "into_row_stream": "K10 OwnedRowStream is a futures_core::Stream, inherently async; a blocking caller uses BlockingConnection::query_all for eager owned rows.",
            "into_query_stream": "K10 OwnedRowStream is a futures_core::Stream, inherently async; a blocking caller uses BlockingConnection::query_all for eager owned rows.",
        },
    ),
    Surface(
        name="rows",
        async_owner="oracledb::Rows",
        blocking_owner="oracledb::BlockingRows",
    ),
    Surface(
        name="cancel-handle",
        async_owner="oracledb::CancelHandle",
        blocking_owner="oracledb::CancelHandle",
        method_map={"cancel": "cancel_blocking"},
    ),
    Surface(
        name="pool",
        async_owner="oracledb::pool::Pool",
        blocking_owner="oracledb::pool::BlockingPool",
        exceptions={
            "blocking": "Adapter that returns the blocking facade; not itself mirrored.",
            "clone": "Clone is a trait method, not a pool operation.",
            "start": "Constructor stays on Pool; BlockingPool is obtained with Pool::blocking.",
        },
        include_sync=True,
    ),
    Surface(
        name="pooled-connection",
        async_owner="oracledb::pool::PooledConnection",
        blocking_owner="oracledb::pool::BlockingPooledConnection",
        exceptions={
            "drop": "Drop is the RAII fallback, not a callable facade operation.",
        },
        include_sync=True,
    ),
    Surface(
        name="arrow-record-batch",
        async_owner="oracledb::arrow::RecordBatchFetch",
        blocking_owner="oracledb::BlockingConnection",
        method_map={"next_batch": "next_record_batch"},
    ),
]


def parse_methods(public_api: Path) -> dict[str, list[Method]]:
    methods: dict[str, list[Method]] = {}
    for line_no, line in enumerate(public_api.read_text(encoding="utf-8").splitlines(), 1):
        match = METHOD_RE.match(line)
        if not match:
            continue
        full = match.group("full")
        owner, method = full.rsplit("::", 1)
        method = method.split("<", 1)[0]
        owner = normalize_owner(owner)
        methods.setdefault(owner, []).append(
            Method(
                owner=owner,
                name=method,
                is_async=bool(match.group("async")),
                line_no=line_no,
            )
        )
    return methods


def method_index(methods: dict[str, list[Method]]) -> dict[str, dict[str, Method]]:
    return {
        owner: {method.name: method for method in owner_methods}
        for owner, owner_methods in methods.items()
    }


def generate_rows(public_api: Path) -> tuple[list[list[str]], int]:
    methods = parse_methods(public_api)
    by_owner = method_index(methods)
    rows: list[list[str]] = []
    missing = 0

    for surface in SURFACES:
        async_methods = [
            method
            for method in methods.get(surface.async_owner, [])
            if surface.include_sync or method.is_async
        ]
        async_methods.sort(key=lambda method: (method.line_no, method.name))
        blocking_methods = by_owner.get(surface.blocking_owner, {})

        for method in async_methods:
            blocking_method = surface.method_map.get(method.name, method.name)
            target = blocking_methods.get(blocking_method)
            if target is not None and not target.is_async:
                status = "covered"
                note = "blocking twin present"
            elif method.name in surface.exceptions:
                status = "exception"
                note = surface.exceptions[method.name]
            else:
                status = "missing"
                note = "missing blocking twin and no documented exception"
                missing += 1

            rows.append(
                [
                    surface.name,
                    surface.async_owner,
                    method.name,
                    surface.blocking_owner,
                    blocking_method,
                    status,
                    note,
                ]
            )

    return rows, missing


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("public_api", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()

    if not args.public_api.is_file():
        print(f"async-blocking: missing public API snapshot: {args.public_api}", file=sys.stderr)
        return 2

    rows, missing = generate_rows(args.public_api)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="\n") as fh:
        fh.write(
            "surface\tasync_owner\tasync_method\tblocking_owner\tblocking_method\tstatus\tnote\n"
        )
        for row in rows:
            fh.write("\t".join(row) + "\n")

    if missing:
        print(
            f"async-blocking: {missing} async public method(s) lack a blocking twin or exception",
            file=sys.stderr,
        )
        return 1

    print(f"async-blocking: wrote {args.output} from {args.public_api}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
