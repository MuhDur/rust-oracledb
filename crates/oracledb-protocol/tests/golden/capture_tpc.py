"""Golden wire capture: XA / two-phase commit (TPC) transactions
(TTC FUNC 103 TPC_TXN_SWITCH for begin/end, FUNC 104 TPC_TXN_CHANGE_STATE for
prepare/commit/rollback).

Run with the REAL python-oracledb (thin mode) against a disposable local
Oracle 23.6+ test container:

    eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1524 \
            ORACLEDB_HOST_PORT=1524 scripts/container.sh env)"
    PYO_DEBUG_PACKETS=1 .venv-py313/bin/python \
        crates/oracledb-protocol/tests/golden/capture_tpc.py \
        > crates/oracledb-protocol/tests/golden/tpc_session.txt

The PYO_DEBUG_PACKETS dump goes to stdout; progress markers go to stderr only
so the dump stays parseable.

Scenario (single sync connection, fixed XID so the begin/prepare/commit/rollback
XID bytes are deterministic). format_id 0x1130 = 4400, gtid b"txn4400",
bqual b"branchId" — matching the golden trace shapes in the brief:

  A. begin (FUNC 103 START, flags NEW=1, the XID) -> server returns the txn
     context in a PARAMETER message; STATUS call_status has the TXN bit set.
  B. insert (DML execute, keeps the txn in progress).
  C. prepare (FUNC 104 PREPARE=3) -> out state REQUIRES_COMMIT(1).
  D. two-phase commit (FUNC 104 COMMIT=1, requested COMMITTED(2)) -> out state
     FORGOTTEN(5).
  E. a second XID begin/insert/end (FUNC 103 DETACH=2 echoing the context) then
     rollback (FUNC 104 ABORT=2, requested ABORTED(3)) -> out state ABORTED(3).
"""

import os
import sys

import oracledb

LOG = sys.stderr
FORMAT_ID = 4400
GTID = b"txn4400"
BQUAL = b"branchId"
FORMAT_ID_2 = 4406
GTID_2 = b"txn4406"
BQUAL_2 = b"branch4"


def main() -> None:
    conn = oracledb.connect(
        user=os.environ["PYO_TEST_MAIN_USER"],
        password=os.environ["PYO_TEST_MAIN_PASSWORD"],
        dsn=os.environ["PYO_TEST_CONNECT_STRING"],
    )
    print("connected", conn.version, file=LOG)
    cursor = conn.cursor()
    cursor.execute("truncate table TestTempTable")
    print("table ready", file=LOG)

    xid = conn.xid(FORMAT_ID, GTID, BQUAL)

    print("=== capture A: tpc_begin (START) ===", file=LOG)
    conn.tpc_begin(xid)

    print("=== capture B: insert (DML keeps txn in progress) ===", file=LOG)
    cursor.execute(
        "insert into TestTempTable (IntCol, StringCol1) values (:1, :2)",
        (1, "row1"),
    )

    print("=== capture C: tpc_prepare (PREPARE) ===", file=LOG)
    needs_commit = conn.tpc_prepare()
    print("needs_commit", needs_commit, file=LOG)

    print("=== capture D: two-phase tpc_commit (COMMIT) ===", file=LOG)
    conn.tpc_commit()

    print("=== capture E: begin/insert/end + rollback (ABORT) ===", file=LOG)
    cursor.execute("truncate table TestTempTable")
    xid2 = conn.xid(FORMAT_ID_2, GTID_2, BQUAL_2)
    conn.tpc_begin(xid2)
    cursor.execute(
        "insert into TestTempTable (IntCol, StringCol1) values (:1, :2)",
        (2, "row2"),
    )
    conn.tpc_end(xid2)
    conn.tpc_rollback(xid2)

    conn.close()
    print("done", file=LOG)


if __name__ == "__main__":
    main()
