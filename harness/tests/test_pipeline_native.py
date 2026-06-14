"""Native single-round-trip pipelining tests (bead rust-oracledb-wi6).

These are shim-specific regression tests that prove the async pipeline path
runs through the NATIVE single-round-trip transport (``Connection.run_pipeline``)
rather than the sequential per-op fallback, AND that the native path produces
results BYTE-IDENTICAL to the sequential runner for a representative batch.

Run with:
    PYTHONPATH=$PWD/harness .venv-py313/bin/python -m pytest \
        harness/tests/test_pipeline_native.py -q

Requires the PYO_TEST_* environment variables used by the conformance harness
(see scripts/container.sh env).
"""

import asyncio
import importlib
import os
import sys

import pytest

# install the Rust shim before the public package is imported (idempotent
# with the shim_inject plugin)
sys.modules.setdefault(
    "oracledb.thin_impl", importlib.import_module("oracledb_pyshim")
)

import oracledb  # noqa: E402
import oracledb_pyshim  # noqa: E402


async def _aconnect():
    return await oracledb.connect_async(
        user=os.environ["PYO_TEST_MAIN_USER"],
        password=os.environ["PYO_TEST_MAIN_PASSWORD"],
        dsn=os.environ["PYO_TEST_CONNECT_STRING"],
    )


def _representative_pipeline():
    """A mixed batch: execute/DML/query covering the simple op matrix."""
    pipeline = oracledb.create_pipeline()
    pipeline.add_execute("truncate table TestTempTable")
    pipeline.add_execute(
        "insert into TestTempTable (IntCol) values (:1)", [101]
    )
    pipeline.add_executemany(
        "insert into TestTempTable (IntCol) values (:1)", [(102,), (103,)]
    )
    pipeline.add_commit()
    pipeline.add_fetchall(
        "select IntCol from TestTempTable order by IntCol"
    )
    pipeline.add_fetchone(
        "select IntCol from TestTempTable order by IntCol"
    )
    return pipeline


def _result_signature(results):
    """A byte-identical-comparable rendering of the per-op result attrs the
    public PipelineOpResult exposes (rows / return_value / rowcount-ish /
    error full_code / column names)."""
    sig = []
    for r in results:
        cols = None
        if r.columns is not None:
            cols = [c.name for c in r.columns]
        err = None if r.error is None else r.error.full_code
        sig.append(
            (
                r.operation.op_type.name,
                repr(r.rows),
                repr(r.return_value),
                err,
                repr(cols),
            )
        )
    return sig


def test_async_supports_pipelining_is_true():
    """The async connection impl must advertise native pipelining."""

    async def go():
        conn = await _aconnect()
        try:
            assert conn._impl.supports_pipelining() is True
        finally:
            await conn.close()

    asyncio.run(go())


def test_native_pipeline_matches_sequential_byte_identical():
    """A representative mixed batch run through the NATIVE single-round-trip
    path must produce results byte-identical to the sequential runner."""

    async def go():
        conn = await _aconnect()
        try:
            # sequential reference
            oracledb_pyshim.set_force_pipeline_path("sequential")
            seq_results = await conn.run_pipeline(_representative_pipeline())
            seq_sig = _result_signature(seq_results)

            # native single-round-trip
            oracledb_pyshim.set_force_pipeline_path("native")
            nat_results = await conn.run_pipeline(_representative_pipeline())
            nat_sig = _result_signature(nat_results)
        finally:
            oracledb_pyshim.set_force_pipeline_path("auto")
            await conn.close()

        assert nat_sig == seq_sig
        # the last fetchall returns the three inserted rows
        assert nat_results[-2].rows == [(101,), (102,), (103,)]
        assert nat_results[-1].rows == [(101,)]

    asyncio.run(go())


def test_native_pipeline_reports_native_path():
    """The native runner must record that it actually ran natively (not the
    sequential fallback) for an all-simple pipeline."""

    async def go():
        conn = await _aconnect()
        try:
            oracledb_pyshim.set_force_pipeline_path("native")
            oracledb_pyshim.reset_pipeline_path_log()
            await conn.run_pipeline(_representative_pipeline())
            assert oracledb_pyshim.last_pipeline_path() == "native"
        finally:
            oracledb_pyshim.set_force_pipeline_path("auto")
            await conn.close()

    asyncio.run(go())
