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

bash "$ROOT/scripts/secret_scan.sh"

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

# Release documentation must advance with the workspace version. Fail closed
# when either source is absent/unreadable, require a real version heading (not a
# narrative substring), and require the K10 record to identify the exact current
# workspace version positively rather than trying to enumerate every stale
# wording that authors might use.
changelog="CHANGELOG.md"
k10_record="docs/design/k10-row-stream.md"
[ -f "$changelog" ] && [ -r "$changelog" ] ||
  fail "$changelog is missing or unreadable"
[ -f "$k10_record" ] && [ -r "$k10_record" ] ||
  fail "$k10_record is missing or unreadable"
version_re="${version//./\\.}"
grep -Eq "^## \\[$version_re\\](\\([^)]*\\))?( - [0-9]{4}-[0-9]{2}-[0-9]{2})?$" "$changelog" ||
  fail "$changelog has no release heading for workspace version $version"
grep -Eq "^Status: implemented in workspace version $version_re;" "$k10_record" ||
  fail "$k10_record does not positively identify workspace version $version as implemented"

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

  # Live version-matrix gate (bead rust-oracledb-pre23ai-connect-z47u.5): a
  # release cannot ship without a committed, all-green FULL matrix run for the
  # exact release SHA. The 0.5.x line shipped a connect path that could not
  # reach any pre-23ai server because only 23ai was ever live-tested; this
  # check makes pre-23ai coverage (xe11 refusal + xe18 + xe21 + free23)
  # structurally unskippable. Produce the artifact with
  # scripts/release_matrix_gate.sh and commit it before tagging.
  need git
  head_sha="$(git rev-parse HEAD)"
  # The verdict file cannot record the SHA of the commit that contains it
  # (writing the file changes the SHA). The gate therefore runs on the
  # release-prep commit and the verdict lands in an immediately following
  # commit that touches ONLY the artifact directory; tagging that artifact
  # commit is equivalent to tagging the tested tree. Accept HEAD's own
  # verdict, or the first parent's verdict when HEAD-vs-parent differs only
  # inside tests/artifacts/version_matrix/.
  gate_sha="$head_sha"
  matrix_results="tests/artifacts/version_matrix/results-$head_sha.json"
  if [ ! -f "$matrix_results" ]; then
    parent_sha="$(git rev-parse HEAD^ 2>/dev/null || true)"
    if [ -n "$parent_sha" ] &&
       [ -f "tests/artifacts/version_matrix/results-$parent_sha.json" ] &&
       [ -z "$(git diff --name-only "HEAD^..HEAD" -- . ':!tests/artifacts/version_matrix')" ]; then
      gate_sha="$parent_sha"
      matrix_results="tests/artifacts/version_matrix/results-$parent_sha.json"
    fi
  fi
  [ -f "$matrix_results" ] ||
    fail "missing live version-matrix results for release SHA $head_sha — run scripts/release_matrix_gate.sh on this commit and commit $matrix_results"
  [ "$(jq -r '.sha' "$matrix_results")" = "$gate_sha" ] ||
    fail "$matrix_results does not record SHA $gate_sha"
  [ "$(jq -r '.dirty' "$matrix_results")" = "false" ] ||
    fail "$matrix_results was recorded on a dirty worktree — rerun scripts/release_matrix_gate.sh on the clean release commit"
  [ "$(jq -r '.overall' "$matrix_results")" = "PASS" ] ||
    fail "$matrix_results is not all-green — a release cannot ship without every matrix lane passing"
  # Four server generations plus the local OCI TCPS lane (A5.2 / bead
  # iec3.1.26): every lane in the committed artifact must be green.
  for lane in xe11 xe18 xe21 free23 octcps; do
    [ "$(jq -r --arg l "$lane" '.lanes[$l]' "$matrix_results")" = "PASS" ] ||
      fail "$matrix_results: lane '$lane' did not pass"
  done
  [ "$(jq -r '.probes.free23_tstz_descriptor' "$matrix_results")" = "PASS" ] ||
    fail "$matrix_results: required free23 TSTZ descriptor probe did not pass"
  echo "release-preflight: version-matrix gate OK ($matrix_results)"
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
