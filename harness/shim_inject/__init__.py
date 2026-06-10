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
    connection_cls = connection_mod.Connection
    original_init = connection_cls.__init__
    if getattr(original_init, "_rust_oracledb_capture", False):
        return

    def wrapped_init(self, dsn=None, *, pool=None, params=None, **kwargs):
        if pool is not None:
            return original_init(self, dsn=dsn, pool=pool, params=params, **kwargs)
        dsn_password, invalid_user_dsn = _parse_dsn_secrets(dsn)
        password = kwargs.get("password")
        if password is None:
            password = dsn_password
        new_password = kwargs.get("newpassword")
        capture_id = None
        if password is not None or new_password is not None or invalid_user_dsn:
            capture_id = shim.record_next_connect_args(
                password=password,
                new_password=new_password,
                invalid_user_dsn=invalid_user_dsn,
            )
        try:
            return original_init(self, dsn=dsn, pool=pool, params=params, **kwargs)
        except Exception:
            if capture_id is not None:
                shim.discard_pending_connect_args(capture_id)
            raise

    wrapped_init._rust_oracledb_capture = True
    connection_cls.__init__ = wrapped_init


def pytest_load_initial_conftests(*_args, **_kwargs):
    shim = importlib.import_module("oracledb_pyshim")
    sys.modules["oracledb.thin_impl"] = shim
    _install_connect_capture(shim)
