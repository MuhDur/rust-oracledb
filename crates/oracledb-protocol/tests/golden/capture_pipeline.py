"""Golden wire capture: pipelining (BEGIN_PIPELINE piggyback, FUNC 199/200).

Run with the REAL python-oracledb (thin mode) against a disposable local
test container:

    eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1526 \
            ORACLEDB_HOST_PORT=1526 scripts/container.sh env)"
    PYO_DEBUG_PACKETS=1 .venv-py313/bin/python \
        crates/oracledb-protocol/tests/golden/capture_pipeline.py \
        > crates/oracledb-protocol/tests/golden/pipeline_session.txt

The PYO_DEBUG_PACKETS dump goes to stdout; this script writes its own
progress markers to stderr only, so the dump stays parseable.

Scenario (single async connection, deterministic statement order):
  1. drop/create table PIPE_GOLDEN (plain cursor round trips)
  2. pipeline A (abort-on-error mode 2): execute insert x2, commit,
     fetchall select -> BEGIN_PIPELINE piggyback on message 1 (token 1),
     tokens 1..4, END_OF_REQUEST data flags, end-pipeline FUNC 200,
     5 boundary-delimited responses
  3. pipeline B (continue-on-error mode 1): insert, insert into a missing
     table (ORA-00942 mid-pipeline), fetchone -> error response in the
     middle, later ops still answered
  4. pipeline C: single bound execute + fetchall (bind layout under
     pipelining)
"""

import asyncio
import os
import sys

import oracledb

LOG = sys.stderr


async def main() -> None:
    conn = await oracledb.connect_async(
        user=os.environ["PYO_TEST_MAIN_USER"],
        password=os.environ["PYO_TEST_MAIN_PASSWORD"],
        dsn=os.environ["PYO_TEST_CONNECT_STRING"],
    )
    print("connected", conn.version, file=LOG)
    if not conn._impl.supports_pipelining():
        raise SystemExit("server/connection does not support pipelining")
    print("supports_pipelining: True", file=LOG)

    cursor = conn.cursor()
    await cursor.execute("drop table if exists pipe_golden purge")
    await cursor.execute(
        "create table pipe_golden (id number(9), val varchar2(50))"
    )
    print("table ready", file=LOG)

    print("=== capture A: abort-on-error pipeline ===", file=LOG)
    pipeline = oracledb.create_pipeline()
    pipeline.add_execute("insert into pipe_golden values (1, 'one')")
    pipeline.add_execute("insert into pipe_golden values (2, 'two')")
    pipeline.add_commit()
    pipeline.add_fetchall("select id, val from pipe_golden order by id")
    results = await conn.run_pipeline(pipeline)
    print("capture A rows:", results[-1].rows, file=LOG)

    print("=== capture B: continue-on-error pipeline ===", file=LOG)
    pipeline = oracledb.create_pipeline()
    pipeline.add_execute("insert into pipe_golden values (3, 'three')")
    pipeline.add_execute("insert into pipe_golden_missing values (1)")
    pipeline.add_fetchone("select count(*) from pipe_golden")
    results = await conn.run_pipeline(pipeline, continue_on_error=True)
    print("capture B error:", results[1].error.full_code, file=LOG)
    print("capture B rows:", results[2].rows, file=LOG)

    print("=== capture C: bound execute pipeline ===", file=LOG)
    pipeline = oracledb.create_pipeline()
    pipeline.add_execute(
        "insert into pipe_golden values (:1, :2)", [4, "four"]
    )
    pipeline.add_fetchall("select id from pipe_golden order by id")
    results = await conn.run_pipeline(pipeline)
    print("capture C rows:", results[-1].rows, file=LOG)

    await conn.close()
    print("done", file=LOG)


if __name__ == "__main__":
    asyncio.run(main())
