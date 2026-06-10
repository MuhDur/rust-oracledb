#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [ -n "${PYTHON:-}" ]; then
  PYTHON_BIN="$PYTHON"
elif command -v python3.13 >/dev/null 2>&1; then
  PYTHON_BIN="python3.13"
else
  PYTHON_BIN="python3"
fi
if [ -n "${ORACLEDB_VENV_DIR:-}" ]; then
  VENV_DIR="$ORACLEDB_VENV_DIR"
elif [ "$PYTHON_BIN" = "python3.13" ]; then
  VENV_DIR="$ROOT/.venv-py313"
else
  VENV_DIR="$ROOT/.venv"
fi

if [ ! -x "$VENV_DIR/bin/python" ]; then
  if command -v uv >/dev/null 2>&1; then
    uv venv --python "$PYTHON_BIN" "$VENV_DIR"
  else
    "$PYTHON_BIN" -m venv "$VENV_DIR"
  fi
fi

if command -v uv >/dev/null 2>&1; then
  uv pip install --python "$VENV_DIR/bin/python" \
    maturin \
    pytest \
    pytest-asyncio \
    pytest-json-report \
    "$ROOT/reference/python-oracledb[test]"
else
  "$VENV_DIR/bin/python" -m pip install \
    maturin \
    pytest \
    pytest-asyncio \
    pytest-json-report \
    "$ROOT/reference/python-oracledb[test]"
fi

printf 'Python harness environment ready: %s\n' "$VENV_DIR/bin/python"
