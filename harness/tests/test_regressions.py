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
from decimal import Decimal

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


def test_number_scale_controls_default_python_type(conn, deadline):
    """Default NUMBER conversion is metadata-driven for constrained scale,
    while an unconstrained fractional NUMBER keeps the value fallback."""

    previous = oracledb.defaults.fetch_decimals
    oracledb.defaults.fetch_decimals = False
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "select cast(100 as number(10, 2)), to_number('100.25') from dual"
            )
            constrained_whole, unconstrained_fraction = cursor.fetchone()
        assert type(constrained_whole) is float
        assert constrained_whole == 100.0
        assert type(unconstrained_fraction) is float
        assert unconstrained_fraction == 100.25
    finally:
        oracledb.defaults.fetch_decimals = previous


def test_untyped_decimal_bind_round_trips_exactly(conn, deadline):
    value = Decimal("12345678901234567890.12345678")
    previous = oracledb.defaults.fetch_decimals
    oracledb.defaults.fetch_decimals = True
    try:
        with conn.cursor() as cursor:
            cursor.execute("select :1 from dual", [value])
            (fetched,) = cursor.fetchone()
        assert type(fetched) is Decimal
        assert fetched == value
    finally:
        oracledb.defaults.fetch_decimals = previous


def test_python_int_beyond_i128_binds_without_float_loss(conn, deadline):
    # Forty decimal digits but only 37 significant digits, so Oracle NUMBER can
    # represent it exactly while an f64 conversion cannot.
    value = int("1234567890123456789012345678901234567000")
    previous = oracledb.defaults.fetch_decimals
    oracledb.defaults.fetch_decimals = False
    try:
        with conn.cursor() as cursor:
            cursor.execute("select :1 from dual", [value])
            (fetched,) = cursor.fetchone()
        assert type(fetched) is int
        assert fetched == value
    finally:
        oracledb.defaults.fetch_decimals = previous


def test_negative_submicrosecond_interval_floors_to_timedelta(conn, deadline):
    with conn.cursor() as cursor:
        cursor.execute(
            "select to_dsinterval('-0 00:00:00.000000001'), "
            "to_dsinterval('-0 00:00:00.000000999'), "
            "to_dsinterval('-0 00:00:00.000001000'), "
            "to_dsinterval('-0 00:00:00.000001001'), "
            "to_dsinterval('0 00:00:00.000000999'), "
            "to_dsinterval('0 00:00:00.000001000') from dual"
        )
        values = cursor.fetchone()
    assert all(type(value) is datetime.timedelta for value in values)
    assert values == (
        datetime.timedelta(microseconds=-1),
        datetime.timedelta(microseconds=-1),
        datetime.timedelta(microseconds=-1),
        datetime.timedelta(microseconds=-2),
        datetime.timedelta(0),
        datetime.timedelta(microseconds=1),
    )


def test_fetch_io_releases_gil_while_server_is_stalled(conn, deadline):
    """With prefetch disabled, the sleep function runs in fetch_next_row.

    A second Python thread cannot record progress before the fetch returns if
    that blocking I/O retains the GIL.
    """

    function_name = f"PYSHIM_FETCH_SLEEP_{os.getpid()}"
    with conn.cursor() as setup_cursor:
        setup_cursor.execute(
            f"""
            create or replace function {function_name}(seconds number)
            return number authid definer is
            begin
                dbms_session.sleep(seconds);
                return seconds;
            end;
            """
        )

    progress_at = []

    def record_progress():
        time.sleep(0.2)
        progress_at.append(time.monotonic())

    progress_thread = threading.Thread(target=record_progress)
    try:
        with conn.cursor() as cursor:
            cursor.prefetchrows = 0
            cursor.arraysize = 1
            cursor.execute(f"select {function_name}(1) from dual")
            progress_thread.start()
            started = time.monotonic()
            assert cursor.fetchone() == (1,)
            returned_at = time.monotonic()

        progress_thread.join(timeout=3)
        assert not progress_thread.is_alive()
        assert progress_at and progress_at[0] < returned_at
        assert 0.8 <= returned_at - started < 5
    finally:
        if progress_thread.is_alive():
            progress_thread.join(timeout=3)
        with conn.cursor() as cleanup_cursor:
            cleanup_cursor.execute(f"drop function {function_name}")


def test_blocking_call_can_be_cancelled_from_another_python_thread(conn, deadline):
    errors = []
    cancel_started_at = []

    def cancel_call():
        time.sleep(0.2)
        try:
            cancel_started_at.append(time.monotonic())
            conn.cancel()
        except Exception as exc:  # surfaced below with the thread proof
            errors.append(exc)

    cancel_thread = threading.Thread(target=cancel_call)
    cancel_thread.start()
    started = time.monotonic()
    try:
        with conn.cursor() as cursor:
            with pytest.raises(oracledb.OperationalError) as info:
                cursor.callproc("dbms_session.sleep", [2])
        returned_at = time.monotonic()
    finally:
        cancel_thread.join(timeout=3)

    assert not cancel_thread.is_alive()
    assert not errors
    assert cancel_started_at and cancel_started_at[0] < returned_at
    assert info.value.args[0].full_code == "ORA-01013"
    assert returned_at - started < 5

    with conn.cursor() as cursor:
        cursor.execute("select 1 from dual")
        assert cursor.fetchone() == (1,)


def test_detached_commit_and_rollback_keep_zero_argument_contract(conn, deadline):
    conn.commit()
    conn.rollback()
