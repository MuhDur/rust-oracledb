"""Pytest plugin that replaces python-oracledb thin_impl with the Rust shim."""

import importlib
import sys


def pytest_load_initial_conftests(*_args, **_kwargs):
    shim = importlib.import_module("oracledb_pyshim")
    sys.modules["oracledb.thin_impl"] = shim
