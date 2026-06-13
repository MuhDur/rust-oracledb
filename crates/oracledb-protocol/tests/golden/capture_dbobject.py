"""Golden wire capture: DbObject (PL/SQL record) IN bind.

Run with the REAL python-oracledb (thin mode) against a disposable local
test container:

    eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1524 \
            ORACLEDB_HOST_PORT=1524 scripts/container.sh env)"
    PYO_DEBUG_PACKETS=1 .venv-py313/bin/python \
        crates/oracledb-protocol/tests/golden/capture_dbobject.py \
        > crates/oracledb-protocol/tests/golden/dbobject_session.txt

The PYO_DEBUG_PACKETS dump goes to stdout; progress markers go to stderr
only, so the dump stays parseable.

Scenario (single connection, deterministic statement order):
  1. gettype PKG_TESTRECORDS.UDT_RECORD
  2. callfunc pkg_TestRecords.GetStringRep with a fully-populated record IN
     bind (the exact values used by tests/test_3200 test_3211).

This is the byte source for `dbobject_golden.rs`, which reconstructs the
packed object image from the same attribute values and asserts byte equality
against the image extracted from the `>>>MARKER GetStringRep<<<` send packet.
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
    cur = conn.cursor()

    type_obj = conn.gettype("PKG_TESTRECORDS.UDT_RECORD")
    obj = type_obj.newobject()
    obj.NUMBERVALUE = 18
    obj.STRINGVALUE = "A string in a record"
    obj.DATEVALUE = datetime.datetime(2016, 2, 15)
    obj.TIMESTAMPVALUE = datetime.datetime(2016, 2, 12, 14, 25, 36)
    obj.BOOLEANVALUE = False
    obj.PLSINTEGERVALUE = 21
    obj.BINARYINTEGERVALUE = 5

    print(">>>MARKER GetStringRep<<<", flush=True)
    result = cur.callfunc("pkg_TestRecords.GetStringRep", str, [obj])
    print(">>>MARKER GetStringRep_done<<<", flush=True)
    print("result:", result, file=LOG)

    conn.close()


if __name__ == "__main__":
    main()
