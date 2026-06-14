#!/usr/bin/env bash
#
# Reproduce the single-static-binary deployment proof for rust-oracledb:
#
#   1. build the `smoke` example fully static for x86_64-unknown-linux-musl
#   2. prove it is static (`file` + `ldd`) and report its size
#   3. build a FROM-scratch Docker image containing ONLY that binary
#   4. run the scratch image against an Oracle listener and confirm it prints 12
#
# No Instant Client, no glibc, no Python interpreter — the whole deployable is
# one executable. See docs/DEPLOYMENT.md for the story and caveats.
#
# Usage:
#   scripts/smoke-static.sh
#
# Connection comes from PYO_TEST_* env (set them yourself, or source the lane
# container env first), defaulting to localhost:1525/FREEPDB1 / pythontest:
#
#   eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1525 \
#           ORACLEDB_HOST_PORT=1525 scripts/container.sh env)"
#   scripts/smoke-static.sh
#
# Knobs (env):
#   CARGO_TARGET_DIR   cargo target dir (must be set for the lane; honored as-is)
#   MUSL_CROSS_DIR     where the x86_64-linux-musl-cross toolchain lives / is
#                      installed (default ~/.cache/x86_64-linux-musl-cross)
#   IMAGE_TAG          scratch image tag (default rust-oracledb-smoke:scratch)
#   SKIP_DOCKER=1      build + prove the static binary, skip the image + run
#   NO_STRIP=1         keep debug symbols in the deployed binary

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="x86_64-unknown-linux-musl"
MUSL_CROSS_DIR="${MUSL_CROSS_DIR:-$HOME/.cache/x86_64-linux-musl-cross}"
MUSL_CROSS_URL="https://musl.cc/x86_64-linux-musl-cross.tgz"
IMAGE_TAG="${IMAGE_TAG:-rust-oracledb-smoke:scratch}"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
export CARGO_TARGET_DIR

PYO_TEST_CONNECT_STRING="${PYO_TEST_CONNECT_STRING:-localhost:1525/FREEPDB1}"
PYO_TEST_MAIN_USER="${PYO_TEST_MAIN_USER:-pythontest}"
PYO_TEST_MAIN_PASSWORD="${PYO_TEST_MAIN_PASSWORD:-pythontest}"

say() { printf '\n=== %s ===\n' "$*"; }

# ---------------------------------------------------------------------------
# 0. musl cross toolchain. ring (rustls' crypto backend) compiles a little C
#    for the musl target, so cc-rs needs x86_64-linux-musl-gcc. We fetch a
#    prebuilt, relocatable toolchain into the user cache (no root required).
# ---------------------------------------------------------------------------
say "musl cross toolchain"
if [ ! -x "$MUSL_CROSS_DIR/bin/x86_64-linux-musl-gcc" ]; then
  if command -v x86_64-linux-musl-gcc >/dev/null 2>&1; then
    MUSL_CROSS_DIR="$(dirname "$(dirname "$(command -v x86_64-linux-musl-gcc)")")"
    echo "using system x86_64-linux-musl-gcc -> $MUSL_CROSS_DIR"
  else
    echo "fetching $MUSL_CROSS_URL ..."
    tmp="$(mktemp -d)"
    curl -sSL --max-time 600 -o "$tmp/musl-cross.tgz" "$MUSL_CROSS_URL"
    mkdir -p "$(dirname "$MUSL_CROSS_DIR")"
    tar xzf "$tmp/musl-cross.tgz" -C "$(dirname "$MUSL_CROSS_DIR")"
    rm -rf "$tmp"
  fi
fi
"$MUSL_CROSS_DIR/bin/x86_64-linux-musl-gcc" --version | head -1
export PATH="$MUSL_CROSS_DIR/bin:$PATH"
export CC_x86_64_unknown_linux_musl="x86_64-linux-musl-gcc"
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER="x86_64-linux-musl-gcc"

# ---------------------------------------------------------------------------
# 1. fully-static musl build of the smoke example.
# ---------------------------------------------------------------------------
say "building static $TARGET binary"
rustup target add "$TARGET" >/dev/null 2>&1 || true
cargo build --release --example smoke -p oracledb --target "$TARGET"

BIN="$CARGO_TARGET_DIR/$TARGET/release/examples/smoke"
[ -x "$BIN" ] || { echo "build did not produce $BIN" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 2. prove it is static and report size.
# ---------------------------------------------------------------------------
say "static linkage proof"
file "$BIN"
ldd "$BIN" 2>&1 || true
case "$(file -b "$BIN")" in
  *statically*|*static-pie*) echo "OK: statically linked" ;;
  *) echo "FAIL: binary is not statically linked" >&2; exit 1 ;;
esac

STAGE="$(mktemp -d)"
cp "$BIN" "$STAGE/smoke"
if [ -z "${NO_STRIP:-}" ]; then
  "$MUSL_CROSS_DIR/bin/x86_64-linux-musl-strip" "$STAGE/smoke" || true
fi
chmod +x "$STAGE/smoke"
printf 'deployed binary size: %s bytes\n' "$(stat -c '%s' "$STAGE/smoke")"

if [ -n "${SKIP_DOCKER:-}" ]; then
  say "SKIP_DOCKER set — built + proved static binary, skipping image + run"
  echo "binary: $STAGE/smoke"
  exit 0
fi

# ---------------------------------------------------------------------------
# 3. FROM-scratch image: ONLY the static binary, nothing else.
# ---------------------------------------------------------------------------
say "building FROM-scratch image $IMAGE_TAG"
cp "$ROOT/docker/Dockerfile.scratch" "$STAGE/Dockerfile.scratch"
docker build -f "$STAGE/Dockerfile.scratch" -t "$IMAGE_TAG" "$STAGE"
printf 'image size: %s\n' "$(docker image inspect "$IMAGE_TAG" --format '{{.Size}}') bytes"
docker images "$IMAGE_TAG" --format '  {{.Repository}}:{{.Tag}}  {{.Size}}'

# ---------------------------------------------------------------------------
# 4. smoke test: run the scratch image against the listener over host network.
#    Plain TCP, so localhost:<port> on the host network reaches the listener.
# ---------------------------------------------------------------------------
say "smoke run against $PYO_TEST_CONNECT_STRING"
docker run --rm --network=host \
  -e PYO_TEST_CONNECT_STRING="$PYO_TEST_CONNECT_STRING" \
  -e PYO_TEST_MAIN_USER="$PYO_TEST_MAIN_USER" \
  -e PYO_TEST_MAIN_PASSWORD="$PYO_TEST_MAIN_PASSWORD" \
  "$IMAGE_TAG"

say "DONE — scratch image connected and printed 12"
rm -rf "$STAGE"
