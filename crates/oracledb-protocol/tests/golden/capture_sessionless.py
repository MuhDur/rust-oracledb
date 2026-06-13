"""Golden wire capture: sessionless transactions (TTC FUNC 103
TPC_TXN_SWITCH, sessionless piggyback, SYNC server-side piggyback state).

Run with the REAL python-oracledb (thin mode) against a disposable local
Oracle 23.6+ test container:

    eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1527 \
            ORACLEDB_HOST_PORT=1527 scripts/container.sh env)"
    PYO_DEBUG_PACKETS=1 .venv-py313/bin/python \
        crates/oracledb-protocol/tests/golden/capture_sessionless.py \
        > crates/oracledb-protocol/tests/golden/sessionless_session.txt

The PYO_DEBUG_PACKETS dump goes to stdout; progress markers go to stderr only
so the dump stays parseable.

Scenario (single sync connection, fixed transaction id so the begin/resume XID
bytes are deterministic):
  1. begin_sessionless_transaction(defer_round_trip=False) -> immediate FUNC 103
     TPC_TXN_SWITCH with operation START, flags NEW|SESSIONLESS, the XID
  2. suspend_sessionless_transaction() -> FUNC 103 operation DETACH,
     flags SESSIONLESS, no XID
  3. resume_sessionless_transaction(defer_round_trip=True) followed by an
     insert -> the resume rides as a sessionless PIGGYBACK prepended to the
     execute (operation START, flags RESUME|SESSIONLESS, the XID)
  4. executemany(suspend_on_success=True) -> sessionless PIGGYBACK with
     operation START|POST_DETACH folded in (deferred begin had not yet been
     flushed) OR a standalone POST_DETACH piggyback
"""

import os
import sys

import oracledb

LOG = sys.stderr
TXN_ID = b"golden_8700_txn_id"


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

    print("=== capture A: begin (immediate) ===", file=LOG)
    conn.begin_sessionless_transaction(transaction_id=TXN_ID, timeout=15)
    cursor.execute(
        "insert into TestTempTable (IntCol, StringCol1) values (:1, :2)",
        (1, "row1"),
    )

    print("=== capture B: suspend (immediate) ===", file=LOG)
    conn.suspend_sessionless_transaction()

    print("=== capture C: resume (deferred) + execute piggyback ===", file=LOG)
    conn.resume_sessionless_transaction(
        transaction_id=TXN_ID, timeout=5, defer_round_trip=True
    )
    cursor.execute(
        "insert into TestTempTable (IntCol, StringCol1) values (:1, :2)",
        (2, "row2"),
        suspend_on_success=True,
    )

    print("=== capture D: resume + commit ===", file=LOG)
    conn.resume_sessionless_transaction(transaction_id=TXN_ID)
    conn.commit()

    conn.close()
    print("done", file=LOG)


if __name__ == "__main__":
    main()
