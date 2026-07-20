"""Shim-specific regression tests (not part of the upstream suite).

Run with:
    PYTHONPATH=$PWD/harness .venv-py313/bin/python -m pytest harness/tests -q

Requires the PYO_TEST_* environment variables used by the conformance
harness (see scripts/container.sh env).
"""

import datetime
import importlib
import os
import signal
import sys
import threading
import time

import pytest

# install the Rust shim before the public package is imported (idempotent
# with the shim_inject plugin)
sys.modules.setdefault(
    "oracledb.thin_impl", importlib.import_module("oracledb_pyshim")
)

import oracledb  # noqa: E402


@pytest.fixture
def conn():
    connection = oracledb.connect(
        user=os.environ["PYO_TEST_MAIN_USER"],
        password=os.environ["PYO_TEST_MAIN_PASSWORD"],
        dsn=os.environ["PYO_TEST_CONNECT_STRING"],
    )
    yield connection
    connection.close()


@pytest.fixture
def deadline():
    """Fail (don't hang) if a regression reintroduces a deadlock."""

    def _expired(signum, frame):
        raise AssertionError("deadlock regression: operation did not finish")

    previous = signal.signal(signal.SIGALRM, _expired)
    signal.alarm(30)
    yield
    signal.alarm(0)
    signal.signal(signal.SIGALRM, previous)


def test_var_in_both_inputsizes_and_parameters(conn, deadline):
    """A Var passed through setinputsizes AND as the bind value at the same
    position used to self-deadlock on the ThinVar values mutex (test_4116
    hang): to_bind_value held the lock while converting the stored value,
    which was the variable itself."""
    cursor = conn.cursor()
    out_value = cursor.var(oracledb.DB_TYPE_BOOLEAN)
    cursor.setinputsizes(oracledb.DB_TYPE_VARCHAR, oracledb.NUMBER, out_value)
    cursor.execute("begin proc_Test2(:1,:2,:3); end;", ("hi", 5, out_value))
    assert out_value.getvalue() is True
    cursor.close()


def test_extra_positional_inputsizes_raises_dpy_4009(conn, deadline):
    """setinputsizes with more entries than statement placeholders must raise
    DPY-4009 (reference impl/thin/var.pyx:101-106) and must leave the
    connection usable — the error exit used to leave the cursor wedged."""
    cursor = conn.cursor()
    out_value = cursor.var(oracledb.DB_TYPE_BOOLEAN)
    for _ in range(3):
        cursor.setinputsizes(
            oracledb.DB_TYPE_VARCHAR,
            oracledb.NUMBER,
            out_value,
            oracledb.DB_TYPE_VARCHAR,  # extra argument
        )
        with pytest.raises(oracledb.DatabaseError) as info:
            cursor.callproc("proc_Test2", ("hi", 5, out_value))
        assert info.value.args[0].full_code in ("DPY-4009", "ORA-01036")
    # connection still healthy after the error exits
    cursor.setinputsizes()
    cursor.execute("select 1 from dual")
    assert cursor.fetchone() == (1,)
    cursor.close()


def test_arbitrary_precision_int_bind_is_exact(conn, deadline):
    """PY3: an int wider than i128 must never take the lossy f64 fallback."""
    # 40 decimal digits but only 38 significant digits, so Oracle NUMBER can
    # store it exactly while Rust i128 cannot.
    value = 1234567890123456789012345678901234567800
    with conn.cursor() as cursor:
        cursor.execute("select :value from dual", value=value)
        (actual,) = cursor.fetchone()
    assert type(actual) is int
    assert actual == value


def test_negative_submicrosecond_interval_uses_floor(conn, deadline):
    """PY5: -500 ns becomes -1 us, matching Cython's floor division."""
    with conn.cursor() as cursor:
        cursor.execute(
            "select interval '-0 00:00:00.000000500' "
            "day(1) to second(9) from dual"
        )
        (actual,) = cursor.fetchone()
    assert type(actual) is datetime.timedelta
    assert actual == datetime.timedelta(microseconds=-1)


def test_threaded_cancel_runs_while_blocking_io_is_in_flight(conn, deadline):
    """PY4: fetch I/O releases the GIL for cancel and leaves a clean session."""
    cursor = conn.cursor()
    cursor.prefetchrows = 0
    cursor.arraysize = 1
    # The execute is describe-only. The aggregate is evaluated by the first
    # fetch, giving the cancel thread a deterministic in-flight fetch without
    # creating a database fixture or persistent object.
    cursor.execute(
        "select sum(v) from ("
        "select a.object_id * b.object_id v "
        "from all_objects a cross join all_objects b "
        "where rownum <= 1000000)"
    )
    started = threading.Event()
    cancel_completed_at = []

    def cancel_after_start():
        assert started.wait(timeout=2)
        time.sleep(0.1)
        conn.cancel()
        cancel_completed_at.append(time.monotonic())

    thread = threading.Thread(target=cancel_after_start)
    thread.start()
    started.set()
    started_at = time.monotonic()
    try:
        with pytest.raises(oracledb.OperationalError) as info:
            cursor.fetchone()
        assert info.value.args[0].full_code == "ORA-01013"
    finally:
        thread.join(timeout=5)
    assert not thread.is_alive(), "cancel thread was serialized behind the GIL"
    assert cancel_completed_at, "cancel thread never completed"
    assert (
        cancel_completed_at[0] - started_at < 1.5
    ), "cancel call was serialized behind blocking I/O's GIL hold"

    # Cancellation recovery must leave the session usable.
    cursor.execute("select 1 from dual")
    assert cursor.fetchone() == (1,)
    cursor.close()
