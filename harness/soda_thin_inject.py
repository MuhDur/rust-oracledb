"""Pytest plugin: enable the SODA reference suites against the Rust thin engine.

python-oracledb's own conftest gates the ``soda_db`` fixture behind
``skip_unless_thick_mode`` because upstream thin mode has no SODA. The Rust
shim *does* implement thin-mode SODA, so this plugin neutralises that one skip
(keeping every server-version / platform guard) so test_3300/test_3400 actually
run against the Rust engine.

Conftest fixtures take precedence over plugin fixtures, so we patch the
``skip_unless_thick_mode`` fixture *function* inside the already-imported
reference conftest module to a no-op. Nothing else about thick/thin detection
changes — ``test_env.use_thick_mode`` stays False, so any test body that itself
checks thick mode still behaves correctly.

Use alongside ``-p shim_inject``:

    pytest tests/test_3300_soda_database.py tests/test_3400_soda_collection.py \
        -p shim_inject -p soda_thin_inject
"""

import sys


def pytest_configure(config):
    # The reference tests package its conftest as the top-level ``conftest``
    # module once collection imports it. Patch lazily in a collection hook so the
    # module is guaranteed to be loaded.
    _patch_skip_unless_thick(config)


def pytest_collectstart(collector):
    _patch_skip_unless_thick(getattr(collector, "config", None))


_patched = False


def _patch_skip_unless_thick(_config):
    global _patched
    if _patched:
        return
    conftest = sys.modules.get("conftest")
    if conftest is None:
        return
    fixture = getattr(conftest, "skip_unless_thick_mode", None)
    if fixture is None:
        return
    # The fixture is a pytest FixtureFunctionMarker wrapping the raw function.
    # Replace its underlying function body with a no-op that performs no skip.
    wrapped = getattr(fixture, "__wrapped__", None) or getattr(
        fixture, "_get_wrapped_function", lambda: None
    )()
    # Re-create the fixture as a no-op while preserving its name/scope.
    import pytest

    @pytest.fixture
    def _no_skip(test_env):  # noqa: ARG001 - signature kept for fixture wiring
        return None

    conftest.skip_unless_thick_mode = _no_skip
    _patched = True
