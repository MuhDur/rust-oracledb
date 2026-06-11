"""Golden wire capture: direct path load (TTC functions 128/129/130).

Run with the REAL python-oracledb (thin mode) against a disposable local
test container:

    eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1526 \
            ORACLEDB_HOST_PORT=1526 scripts/container.sh env)"
    PYO_DEBUG_PACKETS=1 .venv-py313/bin/python \
        crates/oracledb-protocol/tests/golden/capture_dpl.py \
        > crates/oracledb-protocol/tests/golden/dpl_session.txt

The PYO_DEBUG_PACKETS dump goes to stdout; this script writes its own
progress markers to stderr only, so the dump stays parseable.

Scenario (single connection, deterministic statement order):
  1. drop/create table DPL_GOLDEN
  2. direct_path_load of 3 rows in one batch (number, varchar2, number(9,2),
     date, timestamp(6), raw, binary_double + NULLs) -> 128, 129, 130 FINISH
  3. direct_path_load of 4 rows with batch_size=2 -> 128, 129 x2, 130 FINISH
  4. direct_path_load with a >250 byte VARCHAR2 value into DPL_GOLDEN_WIDE
     (long-segment 0xfe encoding) -> 128, 129, 130 FINISH
  5. direct_path_load with a NULL into a NOT NULL column -> client-side
     DPY-8001 followed by 130 ABORT
"""

import datetime
import os
import sys

import oracledb

LOG = sys.stderr


def main() -> None:
    conn = oracledb.connect(
        user=os.environ["PYO_TEST_MAIN_USER"],
        password=os.environ["PYO_TEST_MAIN_PASSWORD"],
        dsn=os.environ["PYO_TEST_CONNECT_STRING"],
    )
    print("connected", conn.version, file=LOG)
    cursor = conn.cursor()
    for ddl in (
        "drop table if exists dpl_golden purge",
        "drop table if exists dpl_golden_wide purge",
        """
        create table dpl_golden (
            id          number(9) not null,
            name        varchar2(100) not null,
            salary      number(9, 2),
            hired       date,
            updated     timestamp(6),
            payload     raw(50),
            rating      binary_double
        )
        """,
        "create table dpl_golden_wide (id number(9), wide varchar2(1000))",
    ):
        cursor.execute(ddl)
    print("tables ready", file=LOG)

    rows = [
        (
            1,
            "alpha",
            1234.56,
            datetime.datetime(2024, 1, 2, 3, 4, 5),
            datetime.datetime(2024, 1, 2, 3, 4, 5, 123456),
            b"\x01\x02\x03",
            2.5,
        ),
        (
            2,
            "beta",
            None,
            None,
            None,
            None,
            None,
        ),
        (
            3,
            "gamma",
            -0.01,
            datetime.datetime(1988, 12, 31, 23, 59, 58),
            datetime.datetime(1988, 12, 31, 23, 59, 58, 999999),
            b"\xff" * 16,
            -1.5,
        ),
    ]
    columns = ["id", "name", "salary", "hired", "updated", "payload", "rating"]

    print("=== capture 1: single batch ===", file=LOG)
    conn.direct_path_load("pythontest", "dpl_golden", columns, rows)

    print("=== capture 2: batch_size=2 over 4 rows ===", file=LOG)
    rows4 = [
        (10, "r10", 1.0, None, None, None, None),
        (11, "r11", 2.0, None, None, None, None),
        (12, "r12", 3.0, None, None, None, None),
        (13, "r13", 4.0, None, None, None, None),
    ]
    conn.direct_path_load(
        "pythontest", "dpl_golden", columns, rows4, batch_size=2
    )

    print("=== capture 3: long segment (>0xfa bytes) ===", file=LOG)
    wide_value = "".join(chr(ord("a") + (i % 26)) for i in range(600))
    conn.direct_path_load(
        "pythontest",
        "dpl_golden_wide",
        ["id", "wide"],
        [(100, wide_value)],
    )

    print("=== capture 4: DPY-8001 -> abort op ===", file=LOG)
    try:
        conn.direct_path_load(
            "pythontest",
            "dpl_golden",
            columns,
            [(20, None, None, None, None, None, None)],
        )
    except oracledb.Error as exc:
        print("expected error:", exc, file=LOG)

    cursor.execute("select count(*), min(id), max(id) from dpl_golden")
    print("dpl_golden rows:", cursor.fetchone(), file=LOG)
    cursor.execute("select count(*), max(length(wide)) from dpl_golden_wide")
    print("dpl_golden_wide rows:", cursor.fetchone(), file=LOG)
    conn.close()
    print("done", file=LOG)


if __name__ == "__main__":
    main()
