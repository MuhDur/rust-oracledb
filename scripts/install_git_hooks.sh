#!/usr/bin/env bash
# Install and verify the versioned baseline pre-push hook for this clone.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXPECTED_PATH=".githooks"
HOOK="$ROOT/$EXPECTED_PATH/pre-push"

usage() {
  cat <<'USAGE'
Usage: scripts/install_git_hooks.sh [--check]

Without arguments, configure this clone to use the repository's versioned Git
hooks. With --check, report whether the hook is actually installed; a missing
or misconfigured hook is an error, never a passing result.
USAGE
}

check_installed() {
  local configured
  configured="$(git -C "$ROOT" config --local --get core.hooksPath || true)"

  if [[ "$configured" != "$EXPECTED_PATH" ]]; then
    echo "git-hooks: NOT INSTALLED: core.hooksPath is '${configured:-<unset>}'; expected $EXPECTED_PATH" >&2
    echo "git-hooks: install with scripts/install_git_hooks.sh" >&2
    return 1
  fi
  if [[ ! -x "$HOOK" ]]; then
    echo "git-hooks: NOT INSTALLED: missing executable $EXPECTED_PATH/pre-push" >&2
    echo "git-hooks: install with scripts/install_git_hooks.sh" >&2
    return 1
  fi

  echo "git-hooks: OK: baseline pre-push hook is installed"
}

case "${1:-}" in
  "")
    git -C "$ROOT" config --local core.hooksPath "$EXPECTED_PATH"
    check_installed
    ;;
  --check)
    check_installed
    ;;
  -h|--help)
    usage
    ;;
  *)
    echo "git-hooks: unknown argument: $1" >&2
    usage >&2
    exit 2
    ;;
esac
