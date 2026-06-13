"""Pytest plugin that replaces python-oracledb thin_impl with the Rust shim."""

import importlib
import sys


def _parse_dsn_secrets(dsn):
    if not isinstance(dsn, str):
        return None, False
    if "@" not in dsn:
        return None, False
    credentials, connect_string = dsn.rsplit("@", 1)
    if not credentials or not connect_string:
        return None, False
    slash_pos = credentials.find("/")
    if slash_pos > 0 and credentials[slash_pos - 1] != ":":
        password = credentials[slash_pos + 1 :] or None
        return password, False
    return None, "/" not in credentials


def _install_connect_capture(shim):
    connection_mod = importlib.import_module("oracledb.connection")

    def capture_connect_args(dsn, kwargs):
        dsn_password, invalid_user_dsn = _parse_dsn_secrets(dsn)
        password = kwargs.get("password")
        if password is None:
            password = dsn_password
        new_password = kwargs.get("newpassword")
        if password is not None or new_password is not None or invalid_user_dsn:
            return shim.record_next_connect_args(
                password=password,
                new_password=new_password,
                invalid_user_dsn=invalid_user_dsn,
            )
        return None

    connection_cls = connection_mod.Connection
    original_init = connection_cls.__init__
    if not getattr(original_init, "_rust_oracledb_capture", False):

        def wrapped_init(self, dsn=None, *, pool=None, params=None, **kwargs):
            if pool is not None:
                return original_init(self, dsn=dsn, pool=pool, params=params, **kwargs)
            capture_id = capture_connect_args(dsn, kwargs)
            try:
                return original_init(self, dsn=dsn, pool=pool, params=params, **kwargs)
            except Exception:
                if capture_id is not None:
                    shim.discard_pending_connect_args(capture_id)
                raise

        wrapped_init._rust_oracledb_capture = True
        connection_cls.__init__ = wrapped_init

    async_connection_cls = connection_mod.AsyncConnection
    original_async_init = async_connection_cls.__init__
    if not getattr(original_async_init, "_rust_oracledb_capture", False):

        def wrapped_async_init(self, dsn, pool, params, kwargs):
            if pool is not None:
                return original_async_init(self, dsn, pool, params, kwargs)
            kwargs = kwargs or {}
            capture_connect_args(dsn, kwargs)
            return original_async_init(self, dsn, pool, params, kwargs)

        wrapped_async_init._rust_oracledb_capture = True
        async_connection_cls.__init__ = wrapped_async_init


def _install_pool_capture(shim):
    """Capture pool creation passwords (unreadable from PoolParamsImpl).

    ``PoolParams.__init__`` stashes any password on the params object;
    ``BaseConnectionPool.__init__`` records the effective password (explicit
    kwarg wins over the params-stashed one) for the shim pool impl to consume.
    """
    pool_params_mod = importlib.import_module("oracledb.pool_params")

    # PoolParams uses __slots__ (no __dict__/__weakref__), so passwords are
    # tracked in a side table keyed by object identity and popped on first
    # use to bound staleness from id() reuse.
    pool_params_passwords = {}

    pool_params_cls = pool_params_mod.PoolParams
    original_params_init = pool_params_cls.__init__
    if not getattr(original_params_init, "_rust_oracledb_capture", False):

        def wrapped_params_init(self, **kwargs):
            password = kwargs.get("password")
            if password is not None:
                pool_params_passwords[id(self)] = password
            return original_params_init(self, **kwargs)

        wrapped_params_init._rust_oracledb_capture = True
        pool_params_cls.__init__ = wrapped_params_init

    pool_mod = importlib.import_module("oracledb.pool")
    pool_cls = pool_mod.BaseConnectionPool
    original_pool_init = pool_cls.__init__
    if not getattr(original_pool_init, "_rust_oracledb_capture", False):

        def wrapped_pool_init(self, dsn=None, *, params=None, **kwargs):
            password = kwargs.get("password")
            if password is None and params is not None:
                password = pool_params_passwords.pop(id(params), None)
            capture_id = None
            if password is not None:
                capture_id = shim.record_next_pool_args(password=password)
            try:
                return original_pool_init(self, dsn=dsn, params=params, **kwargs)
            except Exception:
                if capture_id is not None:
                    shim.discard_pending_pool_args(capture_id)
                raise

        wrapped_pool_init._rust_oracledb_capture = True
        pool_cls.__init__ = wrapped_pool_init


def _install_arrow_impl(shim):
    """Swap the Arrow DataFrame/array/schema impl classes for the Rust shim ones.

    The pure-Python dataframe.py / arrow_array.py / connection.py modules bind
    ``DataFrameImpl`` / ``ArrowArrayImpl`` / ``ArrowSchemaImpl`` via
    ``from .arrow_impl import ...`` at import time. We rebind those names (and the
    ``arrow_impl`` module attributes) to the Rust classes. We deliberately do NOT
    replace ``sys.modules["oracledb.arrow_impl"]``: the compiled ``base_impl`` /
    ``thin_impl`` Cython modules ``cimport`` those classes at the C level and
    perform a struct-size check at init, which a Python-level class would fail.
    """
    arrow_impl = importlib.import_module("oracledb.arrow_impl")
    dataframe_mod = importlib.import_module("oracledb.dataframe")
    arrow_array_mod = importlib.import_module("oracledb.arrow_array")
    connection_mod = importlib.import_module("oracledb.connection")
    for module, name in (
        (dataframe_mod, "DataFrameImpl"),
        (arrow_array_mod, "ArrowArrayImpl"),
        (connection_mod, "ArrowSchemaImpl"),
        (arrow_impl, "DataFrameImpl"),
        (arrow_impl, "ArrowArrayImpl"),
        (arrow_impl, "ArrowSchemaImpl"),
    ):
        setattr(module, name, getattr(shim, name))


def pytest_load_initial_conftests(*_args, **_kwargs):
    shim = importlib.import_module("oracledb_pyshim")
    sys.modules["oracledb.thin_impl"] = shim
    _install_connect_capture(shim)
    _install_pool_capture(shim)
    _install_arrow_impl(shim)
