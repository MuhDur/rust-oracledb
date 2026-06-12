"""Golden wire capture: fetch_df_all (arrow fetch path).

Run with the REAL python-oracledb (thin mode) against a disposable local
test container:

    eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1526 \
            ORACLEDB_HOST_PORT=1526 scripts/container.sh env)"
    PYO_DEBUG_PACKETS=1 .venv-py313/bin/python \
        crates/oracledb-protocol/tests/golden/capture_fetch_df.py \
        > crates/oracledb-protocol/tests/golden/fetch_df_session.txt \
        2> crates/oracledb-protocol/tests/golden/fetch_df_session.meta.txt

stdout carries the PYO_DEBUG_PACKETS dump; stderr carries the resulting
arrow schema + values, recorded as reference evidence for the Rust
fetch->arrow type mapping.
"""

import datetime
import decimal
import os
import sys

import oracledb
import pyarrow

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
        "drop table if exists fdf_golden purge",
        """
        create table fdf_golden (
            id          number(9) not null,
            big         number(19),
            price       number(9, 2),
            anynum      number,
            name        varchar2(40),
            fixed       char(5),
            hired       date,
            updated     timestamp(6),
            payload     raw(30),
            rating      binary_double,
            score       binary_float
        )
        """,
    ):
        cursor.execute(ddl)
    # bind the timestamp column explicitly: python datetime binds as DATE by
    # default, which silently drops fractional seconds on insert
    cursor.setinputsizes(
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        oracledb.DB_TYPE_TIMESTAMP,
        None,
        None,
        None,
    )
    cursor.executemany(
        """
        insert into fdf_golden values
            (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)
        """,
        [
            (
                1,
                12345678901234,
                12.34,
                1.5,
                "alpha",
                "ab",
                datetime.datetime(2024, 1, 2, 3, 4, 5),
                datetime.datetime(2024, 1, 2, 3, 4, 5, 123456),
                b"\x01\x02",
                2.5,
                0.5,
            ),
            (2, None, None, None, None, None, None, None, None, None, None),
            (
                3,
                -42,
                decimal.Decimal("-99.99"),
                -0.25,
                "gamma",
                "xyz",
                datetime.datetime(1988, 12, 31, 23, 59, 58),
                datetime.datetime(1988, 12, 31, 23, 59, 58, 999999),
                b"\xff" * 8,
                -1.5,
                -2.0,
            ),
        ],
    )
    conn.commit()
    print("table ready", file=LOG)

    print("=== capture: fetch_df_all ===", file=LOG)
    odf = conn.fetch_df_all("select * from fdf_golden order by id")
    table = pyarrow.table(odf)
    print("schema:", file=LOG)
    for field in table.schema:
        print(f"  {field.name}: {field.type}", file=LOG)
    print("rows:", file=LOG)
    for row in table.to_pylist():
        print(" ", row, file=LOG)

    print("=== capture: fetch_df_all with fetch_decimals ===", file=LOG)
    odf = conn.fetch_df_all(
        "select id, price, anynum from fdf_golden order by id",
        fetch_decimals=True,
    )
    table = pyarrow.table(odf)
    print("schema (fetch_decimals=True):", file=LOG)
    for field in table.schema:
        print(f"  {field.name}: {field.type}", file=LOG)
    for row in table.to_pylist():
        print(" ", row, file=LOG)

    print("=== capture: null-only column ===", file=LOG)
    odf = conn.fetch_df_all("select null as n from dual")
    table = pyarrow.table(odf)
    for field in table.schema:
        print(f"  {field.name}: {field.type}", file=LOG)
    print(" ", table.to_pylist(), file=LOG)

    conn.close()
    print("done", file=LOG)


if __name__ == "__main__":
    main()
