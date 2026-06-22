#!/usr/bin/env bash
# Validate release metadata before a tag can publish crates or build assets.
# (Adapted from oraclemcp's scripts/release_preflight.sh; the OCI/MCP-registry
# checks do not apply to this pure-library workspace and are omitted.)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "release-preflight: missing required command: $1" >&2
    exit 2
  }
}

fail() {
  echo "release-preflight: $*" >&2
  exit 1
}

need cargo
need jq

metadata="$(cargo metadata --no-deps --format-version 1)"

mapfile -t package_lines < <(jq -r '.packages[] | [.name, .version] | @tsv' <<<"$metadata")
[ "${#package_lines[@]}" -gt 0 ] || fail "no workspace packages found"

# Every workspace crate (including the publish=false pyshim harness) inherits
# [workspace.package].version, so they must all agree.
versions="$(
  printf '%s\n' "${package_lines[@]}" |
    awk -F '\t' '{print $2}' |
    sort -u
)"
version_count="$(printf '%s\n' "$versions" | sed '/^$/d' | wc -l | tr -d ' ')"
[ "$version_count" = "1" ] || {
  printf 'release-preflight: workspace packages must share one version:\n%s\n' "$versions" >&2
  exit 1
}
version="$versions"

# The three crates that actually get published, in dependency order.
expected_packages=(
  oracledb-protocol
  oracledb-derive
  oracledb
)

for package in "${expected_packages[@]}"; do
  if ! printf '%s\n' "${package_lines[@]}" | awk -F '\t' '{print $1}' | grep -Fx "$package" >/dev/null; then
    fail "expected workspace package missing: $package"
  fi
done

# Inter-crate version-pin guard (W4-T3.1). The package-version check above proves
# every workspace crate shares one version, but NOT that the published `oracledb`
# crate's path-dependency *requirements* on its siblings equal that version. A
# stale `version = "X"` requirement publishes a crate that resolves a wrong/old
# sibling from crates.io even though the workspace built against the local path —
# the gap that bit 0.2.1/0.2.2. Assert each inter-crate requirement pins the
# current workspace version.
for dep in oracledb-protocol oracledb-derive; do
  req="$(
    jq -r --arg d "$dep" '
      .packages[] | select(.name == "oracledb")
      | .dependencies[] | select(.name == $d) | .req
    ' <<<"$metadata"
  )"
  if [ -z "$req" ] || [ "$req" = "null" ]; then
    fail "the oracledb crate is missing its inter-crate dependency on $dep"
  fi
  # cargo normalizes `version = "X"` to the requirement `^X`; strip a leading
  # comparator (^ ~ = >= <=) and surrounding space to recover the pinned version.
  req_version="$(printf '%s' "$req" | sed -E 's/^[[:space:]]*[\^~=<>]*[[:space:]]*//')"
  [ "$req_version" = "$version" ] || fail \
    "oracledb's '$dep' requirement '$req' (pinned version '$req_version') does not match the workspace version '$version' — bump the inter-crate pin in crates/oracledb/Cargo.toml in lockstep"
done

tag="${RELEASE_TAG:-}"
if [ -z "$tag" ] && [ "${GITHUB_REF_TYPE:-}" = "tag" ]; then
  tag="${GITHUB_REF_NAME:-}"
fi
if [ -z "$tag" ] && [[ "${GITHUB_REF:-}" == refs/tags/* ]]; then
  tag="${GITHUB_REF#refs/tags/}"
fi

if [ -n "$tag" ]; then
  [[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] ||
    fail "tag '$tag' is not a supported semver tag (expected vX.Y.Z or vX.Y.Z-prerelease)"
  [ "$tag" = "v$version" ] ||
    fail "tag '$tag' does not match workspace version '$version' (expected v$version)"
fi

# On a real tag build, require the tagged commit to be contained in origin/main
# so crates.io can never publish from an off-branch commit.
if [ "${RELEASE_REQUIRE_MAIN:-false}" = "true" ]; then
  need git
  git fetch --no-tags origin main >/dev/null 2>&1 || fail "could not fetch origin/main for tag ancestry check"
  git merge-base --is-ancestor HEAD origin/main ||
    fail "release tag commit is not contained in origin/main"
fi

echo "release-preflight: OK version=$version tag=${tag:-none}"
