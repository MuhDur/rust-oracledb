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


def pytest_load_initial_conftests(*_args, **_kwargs):
    shim = importlib.import_module("oracledb_pyshim")
    sys.modules["oracledb.thin_impl"] = shim
    _install_connect_capture(shim)
