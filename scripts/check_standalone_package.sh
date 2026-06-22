#!/usr/bin/env bash
# W4-T3.2: prove the published crates build STANDALONE from packaged source, with
# NO workspace path resolution. `cargo package`'s own verify step resolves the
# inter-crate path deps from the workspace, so it does not prove the published
# tarballs (which strip `path =` and resolve siblings by version) build on their
# own. This script:
#   1. packages all three crates,
#   2. inspects `cargo package --list`,
#   3. asserts each packaged Cargo.toml strips inter-crate `path =` and pins the
#      workspace version,
#   4. extracts every .crate OUTSIDE the workspace and builds each one there —
#      oracledb against the extracted sibling tarballs (never the workspace),
#   5. runs `cargo publish --dry-run` for the two leaf crates.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 2

fail() { echo "standalone-package: FAIL: $*" >&2; exit 1; }
note() { echo "standalone-package: $*"; }

command -v cargo >/dev/null 2>&1 || fail "missing cargo"
command -v tar >/dev/null 2>&1 || fail "missing tar"

version="$(
  cargo metadata --no-deps --format-version 1 \
    | jq -r '.packages[] | select(.name == "oracledb") | .version'
)"
[ -n "$version" ] && [ "$version" != "null" ] || fail "could not resolve workspace version"
note "workspace version $version"

PKG_TARGET="${PKG_TARGET_DIR:-${CARGO_TARGET_DIR:-$ROOT/target}}"
PKG_DIR="$PKG_TARGET/package"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/oracledb-standalone.XXXXXX")"
cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT

crates=(oracledb-protocol oracledb-derive oracledb)

# 1) Package (build-verifies within the workspace).
note "packaging ${crates[*]}"
cargo package -p oracledb-protocol -p oracledb-derive -p oracledb --locked --allow-dirty \
  >"$WORK/package.log" 2>&1 || { cat "$WORK/package.log" >&2; fail "cargo package failed"; }

# 2) + 3) Per-crate: list inspection + packaged-manifest assertions.
for crate in "${crates[@]}"; do
  cr="$PKG_DIR/$crate-$version.crate"
  [ -f "$cr" ] || fail "missing packaged artifact: $cr"
  files="$(cargo package --list -p "$crate" --allow-dirty 2>/dev/null | wc -l | tr -d ' ')"
  note "$crate: packaged $files files -> $(basename "$cr")"
  manifest="$(tar xzfO "$cr" "$crate-$version/Cargo.toml" 2>/dev/null)"
  # No inter-crate dependency may carry a `path =` (cargo strips it on publish).
  if printf '%s\n' "$manifest" \
      | awk '/^\[dependencies\.oracledb-(protocol|derive)\]/{f=1} f&&/^\[/&&!/oracledb-(protocol|derive)/{f=0} f' \
      | grep -qE '^path *='; then
    fail "$crate packaged manifest still carries an inter-crate path = dependency"
  fi
  # Any inter-crate version requirement must pin the workspace version.
  while IFS= read -r req; do
    [ "$req" = "$version" ] || fail "$crate packaged manifest pins inter-crate version '$req' != '$version'"
  done < <(printf '%s\n' "$manifest" \
            | awk '/^\[dependencies\.oracledb-(protocol|derive)\]/{f=1;next} f&&/^\[/{f=0} f&&/^version *=/{gsub(/[" ]/,"");sub(/version=/,"");print}')
done

# 4) Extract every .crate outside the workspace and build each there.
for crate in "${crates[@]}"; do
  tar xzf "$PKG_DIR/$crate-$version.crate" -C "$WORK"
done

build_standalone() { # crate  extra-args...
  local crate="$1"; shift
  local dir="$WORK/$crate-$version"
  note "building $crate standalone in $dir"
  ( cd "$dir" && CARGO_TARGET_DIR="$WORK/target-$crate" cargo build "$@" ) \
    >"$WORK/build-$crate.log" 2>&1 || { tail -25 "$WORK/build-$crate.log" >&2; fail "$crate failed to build standalone"; }
}

# protocol + derive have no sibling deps -> build from crates.io directly.
build_standalone oracledb-protocol --all-features
build_standalone oracledb-derive

# oracledb: resolve the siblings from the EXTRACTED tarballs (never the workspace)
# via a crates.io patch pointing at the other extracted packages.
oradir="$WORK/oracledb-$version"
{
  echo ""
  echo "[patch.crates-io]"
  echo "oracledb-protocol = { path = \"../oracledb-protocol-$version\" }"
  echo "oracledb-derive = { path = \"../oracledb-derive-$version\" }"
} >> "$oradir/Cargo.toml"
note "building oracledb standalone against the extracted sibling tarballs (no workspace paths)"
( cd "$oradir" && CARGO_TARGET_DIR="$WORK/target-oracledb" cargo build --all-features ) \
  >"$WORK/build-oracledb.log" 2>&1 || { tail -30 "$WORK/build-oracledb.log" >&2; fail "oracledb failed to build standalone"; }

# 5) Publish dry-runs for the leaf crates (full crates.io resolution; no upload).
for crate in oracledb-protocol oracledb-derive; do
  note "cargo publish --dry-run -p $crate"
  cargo publish --dry-run -p "$crate" --locked --allow-dirty \
    >"$WORK/dryrun-$crate.log" 2>&1 || { tail -20 "$WORK/dryrun-$crate.log" >&2; fail "$crate publish --dry-run failed"; }
done
note "oracledb publish --dry-run is deferred to release time (its siblings must be on crates.io first); its standalone build above is the pre-publish proof"

note "OK — all three crates build standalone from packaged source with no workspace path resolution"
