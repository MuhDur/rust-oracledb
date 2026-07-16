#!/usr/bin/env bash
# Keep the current release truth surfaces aligned with Cargo.toml.
#
# This intentionally checks only current-state claims, not historical release
# notes or design records, which may legitimately name earlier versions.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

fail() {
  printf 'release-surface-versions: %s\n' "$*" >&2
  exit 1
}

expect_text() {
  local file="$1" text="$2"
  grep -Fq -- "$text" "$root/$file" \
    || fail "$file must contain: $text"
}

workspace_version="$(awk '
  /^\[workspace\.package\]$/ { in_workspace_package = 1; next }
  /^\[/ { in_workspace_package = 0 }
  in_workspace_package && $1 == "version" {
    gsub(/"/, "", $3)
    print $3
    exit
  }
' "$root/Cargo.toml")"
[ -n "$workspace_version" ] || fail "could not read workspace version from Cargo.toml"

runtime_version="$(sed -nE 's/^asupersync = "=([^"]+)"$/\1/p' "$root/Cargo.toml")"
[ -n "$runtime_version" ] || fail "could not read exact asupersync pin from Cargo.toml"

expect_text AGENTS.md "workspace version **$workspace_version**"
expect_text docs/PUBLISHING.md "| Latest published release | **$workspace_version** |"
expect_text docs/GROUND_TRUTH.md "**asupersync $runtime_version**"

if grep -Fq -- '| Prepared release |' "$root/docs/PUBLISHING.md"; then
  fail "docs/PUBLISHING.md must not present a stale prepared-release row"
fi

printf 'release-surface-versions: OK (workspace=%s, asupersync=%s)\n' \
  "$workspace_version" "$runtime_version"
