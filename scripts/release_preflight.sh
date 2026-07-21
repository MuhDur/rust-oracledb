#!/usr/bin/env bash
# Validate release metadata before a tag can publish crates or build assets.
# (Adapted from oraclemcp's scripts/release_preflight.sh; the OCI/MCP-registry
# checks do not apply to this pure-library workspace and are omitted.)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

usage() {
  cat <<'USAGE'
Usage: scripts/release_preflight.sh [--pre-tag|--self-test]

Validates release metadata by default. Before creating a release tag, run with
RELEASE_TAG=vX.Y.Z and --pre-tag to require that the exact current origin/main
commit already has every Required check, including the live version matrix.
USAGE
}

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

live_matrix_checks=(
  "free23 full suite"
  "xe11 full suite"
  "xe18 full suite"
  "xe21 full suite"
)

# Validate the machine-readable Required-CI report used by the pre-tag path.
# This stays deliberately strict: an absent, non-terminal, red, or unknown
# check is never evidence that the candidate can be tagged. The four live
# matrix lanes receive a specific diagnostic because their main-only path
# filter is the release trap this preflight closes.
check_pre_tag_ci_status() {
  local candidate_sha="$1"
  local report="$2"
  local expected_matrix_json
  expected_matrix_json="$(printf '%s\n' "${live_matrix_checks[@]}" | jq -R . | jq -s 'sort')"

  if ! jq -e --arg sha "$candidate_sha" '
    .schema == "ci-taxonomy/v1" and
    .sha == $sha and
    (.ci_green | type == "boolean") and
    (.required_not_green | type == "array") and
    (.required_missing_path_filtered | type == "array") and
    (.required_missing_unexpected | type == "array") and
    (.unknown_jobs | type == "array")
  ' >/dev/null <<<"$report"; then
    echo "release-preflight: E_PRETAG_CI_STATUS_INVALID: ci-taxonomy returned an invalid report for $candidate_sha" >&2
    return 2
  fi

  if jq -e --arg sha "$candidate_sha" '
    .sha == $sha and
    .ci_green == true and
    .required_not_green == [] and
    .required_missing_path_filtered == [] and
    .required_missing_unexpected == [] and
    .unknown_jobs == []
  ' >/dev/null <<<"$report"; then
    return 0
  fi

  if jq -e --argjson expected "$expected_matrix_json" '
    .ci_green == false and
    (.required_missing_path_filtered | sort) == $expected and
    .required_not_green == [] and
    .required_missing_unexpected == [] and
    .unknown_jobs == []
  ' >/dev/null <<<"$report"; then
    echo "release-preflight: E_PRETAG_LIVE_MATRIX_MISSING: $candidate_sha has no exact-SHA live version-matrix checks" >&2
    echo "release-preflight: before tagging, dispatch version-matrix.yml on main at this candidate, wait for all four lanes, then rerun: python3 scripts/ci_taxonomy.py --status $candidate_sha" >&2
    return 1
  fi

  echo "release-preflight: E_PRETAG_REQUIRED_CI_NOT_GREEN: Required CI is not fully green for $candidate_sha" >&2
  printf '%s\n' "$report" >&2
  return 1
}

run_self_test() {
  local docs_only_sha="1111111111111111111111111111111111111111"
  local green_sha="2222222222222222222222222222222222222222"
  local docs_only_report
  local green_report
  local output

  docs_only_report="$(jq -n --arg sha "$docs_only_sha" \
    --argjson missing "$(printf '%s\n' "${live_matrix_checks[@]}" | jq -R . | jq -s 'sort')" \
    '{schema: "ci-taxonomy/v1", sha: $sha, ci_green: false, required_not_green: [], required_missing_path_filtered: $missing, required_missing_unexpected: [], unknown_jobs: []}')"
  green_report="$(jq -n --arg sha "$green_sha" \
    '{schema: "ci-taxonomy/v1", sha: $sha, ci_green: true, required_not_green: [], required_missing_path_filtered: [], required_missing_unexpected: [], unknown_jobs: []}')"

  if output="$(check_pre_tag_ci_status "$docs_only_sha" "$docs_only_report" 2>&1)"; then
    fail "self-test accepted a docs-only candidate without the four live matrix checks"
  fi
  grep -Fqx "release-preflight: E_PRETAG_LIVE_MATRIX_MISSING: $docs_only_sha has no exact-SHA live version-matrix checks" <<<"$output" ||
    fail "self-test did not identify the docs-only live-matrix gap"
  check_pre_tag_ci_status "$green_sha" "$green_report" ||
    fail "self-test rejected a fully green Required-CI report"

  echo "release-preflight: self-test OK — docs-only candidate is rejected until all four live matrix checks exist"
}

mode="metadata"
case "${1:-}" in
  "") ;;
  --pre-tag) mode="pre-tag" ;;
  --self-test) mode="self-test" ;;
  -h|--help)
    usage
    exit 0
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

if [ "$mode" = "self-test" ]; then
  need jq
  run_self_test
  exit 0
fi

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
grep -Eq "^Status: implemented in (prepared )?workspace version $version_re;" "$k10_record" ||
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

  # Exact-SHA Required and matrix evidence is produced by the manual
  # release-qualification workflow and consumed in release.yml.  It is kept
  # outside the commit: committing a result changes the SHA it claims to prove.
  # The tag workflow runs verify_release_exact_sha.py after this metadata check;
  # unlike the retired local convention, it never substitutes a parent artifact.
fi

# On a real tag build, require the tagged commit to be contained in origin/main
# so crates.io can never publish from an off-branch commit.
if [ "${RELEASE_REQUIRE_MAIN:-false}" = "true" ]; then
  need git
  git fetch --no-tags origin main >/dev/null 2>&1 || fail "could not fetch origin/main for tag ancestry check"
  git merge-base --is-ancestor HEAD origin/main ||
    fail "release tag commit is not contained in origin/main"
fi

if [ "$mode" = "pre-tag" ]; then
  [ -n "$tag" ] || fail "--pre-tag requires RELEASE_TAG=v$version"
  need git
  need python3
  need gh
  git fetch --no-tags origin main >/dev/null 2>&1 || fail "could not fetch origin/main for pre-tag validation"
  candidate_sha="$(git rev-parse HEAD)"
  main_sha="$(git rev-parse origin/main)"
  [ "$candidate_sha" = "$main_sha" ] ||
    fail "pre-tag candidate $candidate_sha is not current origin/main $main_sha; update to main and qualify that exact SHA"

  ci_status="$(python3 "$ROOT/scripts/ci_taxonomy.py" --status "$candidate_sha" 2>/dev/null)" || {
    [ -n "$ci_status" ] || fail "could not obtain ci-taxonomy status for $candidate_sha"
  }
  check_pre_tag_ci_status "$candidate_sha" "$ci_status" || exit $?
fi

echo "release-preflight: OK version=$version tag=${tag:-none}"
