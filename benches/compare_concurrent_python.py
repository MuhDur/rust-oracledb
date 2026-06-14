#!/usr/bin/env python3
"""Concurrent-throughput comparison for python-oracledb thin mode.

This is the python-oracledb half of the concurrency benchmark whose Rust half is
``crates/oracledb/benches/concurrent_throughput.rs``. It measures the same
decode-heavy workload — repeatedly scanning a small wide table that is fully in
the server buffer cache — under two Python concurrency models at the same worker
counts as the Rust bench, so the two scaling curves are apples to apples:

  (a) threads  : N ``threading.Thread`` workers, each with its own connection,
                 each scanning in a loop. python-oracledb thin runs its protocol
                 and codec in pure Python under the CPython GIL, so this
                 CPU-bound decode cannot run two threads' worth of codec at once
                 — the expected result is a plateau, not linear scaling.
  (b) asyncio  : N ``connect_async`` connections driven by N coroutines on one
                 event loop. asyncio overlaps the *I/O* wait of many connections,
                 but the decode still runs on the single event-loop thread under
                 the GIL, so a decode-bound workload again cannot scale with N.

The hypothesis (recorded in ``docs/PERFORMANCE.md``) is that the Rust driver,
having no GIL, scales aggregate decode throughput with worker threads up to the
server's ceiling, while both Python models flatten because the GIL serializes
the codec. This script measures the Python side of that claim; it makes no claim
on its own beyond the numbers it prints.

The workload (table name, shape, scan SQL, arraysize) is kept identical to the
Rust bench:

  * table  ``PERFTEST_CONC`` : WORKLOAD_ROWS rows x WORKLOAD_COLS columns,
            first half NUMBER, second half VARCHAR2(40), buffer-cache warm.
  * scan   ``select * from PERFTEST_CONC`` with arraysize > row count, so the
            whole table returns in one fetch (no paging).

Each worker scans in a tight loop for a fixed wall-clock window; aggregate
throughput is the summed rows/sec across workers, and the scaling factor is
throughput(N) / throughput(1).

Usage:

    eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1526 \\
            ORACLEDB_HOST_PORT=1526 scripts/container.sh env)"
    .venv-py313/bin/python benches/compare_concurrent_python.py

Environment (set by ``scripts/container.sh env``):
    PYO_TEST_CONNECT_STRING   host:port/service
    PYO_TEST_MAIN_USER        schema user
    PYO_TEST_MAIN_PASSWORD    schema password

Optional:
    CONC_MEASURE_SECS   measured window per N, per model (default 6)
    CONC_WARMUP_SECS    per-worker warmup before the window (default 2)
    CONC_WORKERS        comma-separated worker counts (default 1,2,4,8,16)
    CONC_JSON           if set, write the results as JSON to this path
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import threading
import time

try:
    import oracledb
except ImportError:  # pragma: no cover - environment guard
    sys.stderr.write(
        "python-oracledb is not installed in this interpreter; "
        "run with the project venv (.venv-py313).\n"
    )
    sys.exit(2)

# Workload shape — kept in lockstep with concurrent_throughput.rs.
SCAN_TABLE = "PERFTEST_CONC"
WORKLOAD_ROWS = 1_000
WORKLOAD_COLS = 20
NUM_COLS = WORKLOAD_COLS // 2
WORKLOAD_ARRAYSIZE = WORKLOAD_ROWS + 500
SCAN_SQL = f"select * from {SCAN_TABLE}"

WORKER_COUNTS = (1, 2, 4, 8, 16)


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


def create_table_sql() -> str:
    cols = [f"n{i} number" for i in range(NUM_COLS)]
    cols += [f"v{i} varchar2(40)" for i in range(NUM_COLS)]
    return f"create table {SCAN_TABLE} (" + ", ".join(cols) + ")"


def populate_sql() -> str:
    exprs = [f"level * {1_000_003 + i * 17} + {i * 31}" for i in range(NUM_COLS)]
    exprs += [
        f"rpad('row' || to_char(level) || '-c{i}', 40, 'x')" for i in range(NUM_COLS)
    ]
    return (
        f"insert /*+ append */ into {SCAN_TABLE} select "
        + ", ".join(exprs)
        + f" from dual connect by level <= {WORKLOAD_ROWS}"
    )


def setup_table(params: dict) -> None:
    """Create, populate, and warm the scan table once on a dedicated connection.

    Idempotent: drops a stale table first. The same well-known object the Rust
    bench uses, with the same shape, so running either side first is fine.
    """
    conn = oracledb.connect(**params)
    cur = conn.cursor()
    try:
        cur.execute(f"drop table {SCAN_TABLE} purge")
    except oracledb.DatabaseError:
        pass
    cur.execute(create_table_sql())
    cur.execute(populate_sql())
    conn.commit()
    cur.arraysize = WORKLOAD_ARRAYSIZE
    for _ in range(3):  # warm the buffer cache
        cur.execute(SCAN_SQL)
        cur.fetchall()
    conn.close()


def teardown_table(params: dict) -> None:
    try:
        conn = oracledb.connect(**params)
        cur = conn.cursor()
        try:
            cur.execute(f"drop table {SCAN_TABLE} purge")
        except oracledb.DatabaseError:
            pass
        conn.close()
    except oracledb.DatabaseError:
        pass


def touch_rows(rows) -> int:
    """Consume every cell so the decode is not optimized away, mirroring the
    Rust bench's ``touch_all_cells``. python-oracledb has already materialized
    each cell into a Python object by the time ``fetchall`` returns, so simply
    folding over them forces those objects to exist. Returns the row count."""
    sink = 0
    for row in rows:
        for cell in row:
            if isinstance(cell, str):
                sink += len(cell)
            elif cell is not None:
                sink += 1
    if sink < 0:  # never true; keeps the optimizer from proving sink unused
        raise AssertionError
    return len(rows)


# --------------------------------------------------------------------------
# (a) threads model
# --------------------------------------------------------------------------
def thread_worker(params, warmup_s, measure_s, start_barrier, stop_flag, out, idx):
    conn = oracledb.connect(**params)
    cur = conn.cursor()
    cur.arraysize = WORKLOAD_ARRAYSIZE

    warm_deadline = time.perf_counter() + warmup_s
    while time.perf_counter() < warm_deadline:
        cur.execute(SCAN_SQL)
        touch_rows(cur.fetchall())

    start_barrier.wait()
    start = time.perf_counter()
    rows = 0
    while not stop_flag["stop"]:
        cur.execute(SCAN_SQL)
        rows += touch_rows(cur.fetchall())
    elapsed = time.perf_counter() - start
    conn.close()
    out[idx] = (rows, elapsed)


def run_threads(params, n, warmup_s, measure_s):
    start_barrier = threading.Barrier(n + 1)
    stop_flag = {"stop": False}
    out: list = [None] * n
    workers = [
        threading.Thread(
            target=thread_worker,
            args=(params, warmup_s, measure_s, start_barrier, stop_flag, out, i),
        )
        for i in range(n)
    ]
    for w in workers:
        w.start()
    start_barrier.wait()  # all workers have warmed up and are at the line
    time.sleep(measure_s)
    stop_flag["stop"] = True
    for w in workers:
        w.join()
    return out


# --------------------------------------------------------------------------
# (b) asyncio model
# --------------------------------------------------------------------------
async def async_worker(params, warmup_s, stop_at, results, idx):
    conn = await oracledb.connect_async(**params)
    cur = conn.cursor()
    cur.arraysize = WORKLOAD_ARRAYSIZE

    warm_deadline = time.perf_counter() + warmup_s
    while time.perf_counter() < warm_deadline:
        await cur.execute(SCAN_SQL)
        touch_rows(await cur.fetchall())

    # All coroutines pass through here at roughly the same time; the small skew
    # is dwarfed by the multi-second window. Measure to the shared deadline.
    start = time.perf_counter()
    rows = 0
    while time.perf_counter() < stop_at:
        await cur.execute(SCAN_SQL)
        rows += touch_rows(await cur.fetchall())
    elapsed = time.perf_counter() - start
    await conn.close()
    results[idx] = (rows, elapsed)


async def run_asyncio_async(params, n, warmup_s, measure_s):
    results: list = [None] * n
    # Deadline is set after warmup would plausibly finish; each worker computes
    # its own warmup then races to the shared stop time.
    stop_at = time.perf_counter() + warmup_s + measure_s
    await asyncio.gather(
        *(async_worker(params, warmup_s, stop_at, results, i) for i in range(n))
    )
    return results


def run_asyncio(params, n, warmup_s, measure_s):
    return asyncio.run(run_asyncio_async(params, n, warmup_s, measure_s))


# --------------------------------------------------------------------------
# driver
# --------------------------------------------------------------------------
def aggregate(out) -> tuple[float, float]:
    """Aggregate rows/sec and per-worker rows/sec from per-worker (rows,elapsed).

    Uses the longest worker window as the denominator (workers start together
    and stop together within a scan, so the conservative max is honest)."""
    total_rows = sum(r for (r, _e) in out)
    max_elapsed = max(e for (_r, e) in out)
    max_elapsed = max(max_elapsed, 1e-12)
    throughput = total_rows / max_elapsed
    return throughput, throughput / len(out)


def run_model(name, runner, params, warmup_s, measure_s):
    print(f"\npython-oracledb {name}:")
    print(
        f"{'N':>3}  {'rows/sec (aggregate)':>20}  {'rows/sec/worker':>15}  "
        f"{'scaling':>9}  {'efficiency':>11}"
    )
    base = None
    rows_for_json = []
    for n in WORKER_COUNTS:
        out = runner(params, n, warmup_s, measure_s)
        throughput, per_worker = aggregate(out)
        if base is None:
            base = throughput
        scaling = throughput / base
        efficiency = scaling / n
        print(
            f"{n:>3}  {throughput:>20.0f}  {per_worker:>15.0f}  "
            f"{scaling:>8.2f}x  {efficiency * 100:>10.0f}%"
        )
        rows_for_json.append(
            {
                "workers": n,
                "rows_per_sec": throughput,
                "rows_per_sec_per_worker": per_worker,
                "scaling": scaling,
                "efficiency": efficiency,
            }
        )
    return rows_for_json


def main() -> int:
    params = connect_params()
    warmup_s = float(os.environ.get("CONC_WARMUP_SECS", "2"))
    measure_s = float(os.environ.get("CONC_MEASURE_SECS", "6"))
    global WORKER_COUNTS
    if os.environ.get("CONC_WORKERS"):
        WORKER_COUNTS = tuple(
            int(x) for x in os.environ["CONC_WORKERS"].split(",") if x.strip()
        )

    print(f"python-oracledb {oracledb.__version__} (thin mode) — concurrent throughput")
    print(f"dsn={params['dsn']} user={params['user']}")
    print(
        f"workload: scan of {SCAN_TABLE} ({WORKLOAD_ROWS} rows x {WORKLOAD_COLS} cols, "
        f"half NUMBER half VARCHAR2), arraysize {WORKLOAD_ARRAYSIZE}"
    )
    print(
        f"measure window: {measure_s:g}s per N (after {warmup_s:g}s warmup); "
        f"workers={','.join(str(n) for n in WORKER_COUNTS)}"
    )

    setup_table(params)
    try:
        threads_rows = run_model("threads", run_threads, params, warmup_s, measure_s)
        asyncio_rows = run_model("asyncio", run_asyncio, params, warmup_s, measure_s)
    finally:
        teardown_table(params)

    print(
        "\nscaling = throughput(N) / throughput(1); efficiency = scaling / N "
        "(100% = perfect linear). Under the GIL a CPU-bound decode is expected "
        "to plateau well below linear."
    )

    if os.environ.get("CONC_JSON"):
        with open(os.environ["CONC_JSON"], "w") as handle:
            json.dump(
                {"threads": threads_rows, "asyncio": asyncio_rows},
                handle,
                indent=2,
            )
        print(f"\nwrote {os.environ['CONC_JSON']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
