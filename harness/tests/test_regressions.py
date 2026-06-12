"""Shim-specific regression tests (not part of the upstream suite).

Run with:
    PYTHONPATH=$PWD/harness .venv-py313/bin/python -m pytest harness/tests -q

Requires the PYO_TEST_* environment variables used by the conformance
harness (see scripts/container.sh env).
"""

import importlib
import os
import signal
import sys

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
