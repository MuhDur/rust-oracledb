#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="x86_64-unknown-linux-musl"
EXAMPLE="min_connect"
CEILING_BYTES="${MUSL_SIZE_CEILING_BYTES:-600000}"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
MUSL_CROSS_DIR="${MUSL_CROSS_DIR:-$HOME/.cache/x86_64-linux-musl-cross}"
export CARGO_TARGET_DIR

if [ -z "${CC_x86_64_unknown_linux_musl:-}" ]; then
  if [ -x "$MUSL_CROSS_DIR/bin/x86_64-linux-musl-gcc" ]; then
    export PATH="$MUSL_CROSS_DIR/bin:$PATH"
    export CC_x86_64_unknown_linux_musl="x86_64-linux-musl-gcc"
    export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER="${CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER:-x86_64-linux-musl-gcc}"
  elif command -v x86_64-linux-musl-gcc >/dev/null 2>&1; then
    export CC_x86_64_unknown_linux_musl="x86_64-linux-musl-gcc"
    export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER="${CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER:-x86_64-linux-musl-gcc}"
  elif command -v musl-gcc >/dev/null 2>&1; then
    export CC_x86_64_unknown_linux_musl="musl-gcc"
    export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER="${CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER:-musl-gcc}"
  fi
fi

if ! rustup target list --installed | grep -qx "$TARGET"; then
  echo "musl-size: rust target $TARGET is not installed" >&2
  echo "musl-size: install it with: rustup target add $TARGET" >&2
  exit 66
fi

if [ -z "${CC_x86_64_unknown_linux_musl:-}" ]; then
  echo "musl-size: no musl C compiler found for ring" >&2
  echo "musl-size: install musl-tools or set CC_x86_64_unknown_linux_musl and CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER" >&2
  exit 66
fi

echo "musl-size: building $EXAMPLE for $TARGET"
cargo build -p oracledb --example "$EXAMPLE" --release --target "$TARGET"

BIN="$CARGO_TARGET_DIR/$TARGET/release/examples/$EXAMPLE"
if [ ! -x "$BIN" ]; then
  echo "musl-size: build did not produce executable $BIN" >&2
  exit 1
fi

STRIP_TOOL="${STRIP:-}"
if [ -z "$STRIP_TOOL" ]; then
  if [ -x "$MUSL_CROSS_DIR/bin/x86_64-linux-musl-strip" ]; then
    STRIP_TOOL="$MUSL_CROSS_DIR/bin/x86_64-linux-musl-strip"
  elif command -v x86_64-linux-musl-strip >/dev/null 2>&1; then
    STRIP_TOOL="x86_64-linux-musl-strip"
  else
    STRIP_TOOL="strip"
  fi
fi

"$STRIP_TOOL" "$BIN"
SIZE_BYTES="$(stat -c '%s' "$BIN")"
printf 'musl-size: measured=%s bytes ceiling=%s bytes\n' "$SIZE_BYTES" "$CEILING_BYTES"

if [ "$SIZE_BYTES" -gt "$CEILING_BYTES" ]; then
  echo "musl-size: binary exceeds ceiling" >&2
  exit 1
fi

echo "musl-size: OK"
