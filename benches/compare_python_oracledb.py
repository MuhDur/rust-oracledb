#!/usr/bin/env python3
"""Time the five thin-mode operations from the Rust criterion bench against the
same Oracle container, using python-oracledb in thin mode (no Instant Client).

This is the python-oracledb half of the comparison documented in
``docs/PERFORMANCE.md``. It mirrors ``crates/oracledb/benches/thin_driver.rs``
operation for operation so the two sets of medians are apples to apples: same
SQL, same row counts, same warm-connection reuse, same container.

  1. connect           full handshake: oracledb.connect(...) + connection.close()
  2. select_one_row    cursor.execute("select 1 from dual") + fetchone
  3. fetch_10k_rows    connect by level <= 10000, arraysize 1000, fetchall
  4. executemany_1000  executemany INSERT of 1000 rows, then rollback
  5. read_clob         select a 64 KiB CLOB locator and read its full text

Each operation is timed with ``time.perf_counter`` over a warmup phase followed
by N measured iterations; the reported statistic is the median (plus the median
absolute deviation, to match criterion's MAD reporting).

Usage:

    eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1523 \\
            ORACLEDB_HOST_PORT=1523 scripts/container.sh env)"
    .venv-py313/bin/python benches/compare_python_oracledb.py

Environment (set by ``scripts/container.sh env``):
    PYO_TEST_CONNECT_STRING   host:port/service   (e.g. localhost:1523/FREEPDB1)
    PYO_TEST_MAIN_USER        schema user
    PYO_TEST_MAIN_PASSWORD    schema password

Optional:
    PERF_ITERS_FAST   iterations for cheap ops (default 2000)
    PERF_ITERS_SLOW   iterations for the connect/bulk/executemany ops (default 200)
    PERF_WARMUP       warmup iterations applied before every op (default 50)
"""

from __future__ import annotations

import json
import os
import statistics
import sys
import time
from typing import Callable

try:
    import oracledb
except ImportError:  # pragma: no cover - environment guard
    sys.stderr.write(
        "python-oracledb is not installed in this interpreter; "
        "run with the project venv (.venv-py313).\n"
    )
    sys.exit(2)

SCRATCH_TABLE = "PERFTEST_BENCH"
CLOB_TABLE = "PERFTEST_CLOB"
# 64 chars repeated out to 65536, matching the Rust bench's CLOB body.
CLOB_SEED = "the quick brown fox jumps over the lazy dog 0123456789ABCDEF"


def env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        sys.stderr.write(
            f"skipped: {name} not set (source scripts/container.sh env first)\n"
        )
        sys.exit(0)
    return value


def connect_params() -> dict:
    return {
        "user": env("PYO_TEST_MAIN_USER"),
        "password": env("PYO_TEST_MAIN_PASSWORD"),
        "dsn": env("PYO_TEST_CONNECT_STRING"),
    }


def median_and_mad(samples: list[float]) -> tuple[float, float]:
    """Median and median absolute deviation, both in seconds."""
    med = statistics.median(samples)
    mad = statistics.median([abs(s - med) for s in samples])
    return med, mad


def time_op(
    label: str,
    op: Callable[[], None],
    iters: int,
    warmup: int,
) -> dict:
    """Run ``op`` ``warmup`` times unmeasured, then ``iters`` times measured.

    Each iteration is timed individually so the reported median reflects a
    per-call latency, the same quantity criterion's per-iteration sampling
    estimates on the Rust side.
    """
    for _ in range(warmup):
        op()
    samples: list[float] = []
    for _ in range(iters):
        start = time.perf_counter()
        op()
        samples.append(time.perf_counter() - start)
    med, mad = median_and_mad(samples)
    print(
        f"  {label:<18} median {med * 1e6:10.2f} us   "
        f"MAD {mad * 1e6:8.2f} us   (n={iters})"
    )
    return {
        "operation": label,
        "median_us": med * 1e6,
        "mad_us": mad * 1e6,
        "iterations": iters,
    }


def drop_if_exists(cursor, ddl: str) -> None:
    try:
        cursor.execute(ddl)
    except oracledb.DatabaseError:
        pass


def main() -> int:
    params = connect_params()
    iters_fast = int(os.environ.get("PERF_ITERS_FAST", "2000"))
    iters_slow = int(os.environ.get("PERF_ITERS_SLOW", "200"))
    warmup = int(os.environ.get("PERF_WARMUP", "50"))

    print(f"python-oracledb {oracledb.__version__} (thin mode)")
    print(f"dsn={params['dsn']} user={params['user']}")
    print(f"iters fast/slow={iters_fast}/{iters_slow}  warmup={warmup}\n")

    results: list[dict] = []

    # ----------------------------------------------------------------------
    # (a) connect + auth + close: full handshake per iteration, including TCP.
    # ----------------------------------------------------------------------
    def do_connect() -> None:
        conn = oracledb.connect(**params)
        conn.close()

    print("oracledb_thin (python):")
    results.append(time_op("connect", do_connect, iters_slow, warmup))

    # A single warm connection drives the remaining operation benches, matching
    # the Rust bench which reuses one connection after the connect bench.
    conn = oracledb.connect(**params)
    setup = conn.cursor()

    # ----------------------------------------------------------------------
    # (b) single-row SELECT. One reused cursor; python-oracledb caches the
    #     parsed statement, the same as the Rust cached-cursor path.
    # ----------------------------------------------------------------------
    sel_cursor = conn.cursor()

    def do_select_one() -> None:
        sel_cursor.execute("select 1 from dual")
        sel_cursor.fetchone()

    results.append(time_op("select_one_row", do_select_one, iters_fast, warmup))

    # ----------------------------------------------------------------------
    # (c) bulk fetch: 10000 rows, arraysize 1000 (so the driver pages ~10x).
    # ----------------------------------------------------------------------
    bulk_cursor = conn.cursor()
    bulk_cursor.arraysize = 1000

    def do_fetch_10k() -> None:
        bulk_cursor.execute("select level as n from dual connect by level <= 10000")
        rows = bulk_cursor.fetchall()
        assert len(rows) == 10000

    results.append(time_op("fetch_10k_rows", do_fetch_10k, iters_slow, warmup))

    # ----------------------------------------------------------------------
    # (d) executemany INSERT of 1000 rows, rolled back each iteration so the
    #     table stays empty and the measured cost is the insert path.
    # ----------------------------------------------------------------------
    drop_if_exists(setup, f"drop table {SCRATCH_TABLE} purge")
    setup.execute(
        f"create table {SCRATCH_TABLE} (id number(9), label varchar2(40))"
    )
    insert_sql = f"insert into {SCRATCH_TABLE} (id, label) values (:1, :2)"
    insert_rows = [(i, f"row-{i:05d}") for i in range(1000)]
    insert_cursor = conn.cursor()

    def do_executemany() -> None:
        insert_cursor.executemany(insert_sql, insert_rows)
        assert insert_cursor.rowcount == 1000
        conn.rollback()

    results.append(time_op("executemany_1000", do_executemany, iters_slow, warmup))

    # ----------------------------------------------------------------------
    # (e) CLOB read: select a 64 KiB CLOB locator and read its full text.
    # ----------------------------------------------------------------------
    drop_if_exists(setup, f"drop table {CLOB_TABLE} purge")
    setup.execute(f"create table {CLOB_TABLE} (id number(9), body clob)")
    # Build a real ~64 KiB CLOB by appending 1024-char chunks in PL/SQL: a bare
    # SQL rpad() caps at 4000 chars (VARCHAR2 limit), so it cannot stand in for
    # a large LOB. 64 chunks of 1024 chars give exactly 65536 characters.
    setup.execute(
        f"""
        declare
            l_body clob;
            l_chunk varchar2(1024) := rpad('{CLOB_SEED}', 1024, 'x');
        begin
            dbms_lob.createtemporary(l_body, true);
            for i in 1 .. 64 loop
                dbms_lob.append(l_body, to_clob(l_chunk));
            end loop;
            insert into {CLOB_TABLE} values (1, l_body);
            dbms_lob.freetemporary(l_body);
        end;
        """
    )
    conn.commit()
    clob_cursor = conn.cursor()

    def do_read_clob() -> None:
        clob_cursor.execute(f"select body from {CLOB_TABLE} where id = 1")
        (lob,) = clob_cursor.fetchone()
        text = lob.read()
        assert len(text) == 65536

    results.append(time_op("read_clob", do_read_clob, iters_fast, warmup))

    # Cleanup: only PERFTEST_* objects this script created.
    drop_if_exists(setup, f"drop table {SCRATCH_TABLE} purge")
    drop_if_exists(setup, f"drop table {CLOB_TABLE} purge")
    conn.close()

    if os.environ.get("PERF_JSON"):
        with open(os.environ["PERF_JSON"], "w") as handle:
            json.dump(results, handle, indent=2)
        print(f"\nwrote {os.environ['PERF_JSON']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
