#!/usr/bin/env bash
# Verify no public type is reachable via two public module paths (W1-T9).
#
# A 1.0 import story has exactly one obvious path per type. A type re-exported at
# a parent module while its defining submodule is also `pub` is reachable via two
# paths (e.g. `tls::WalletContents` AND `tls::wallet::WalletContents`); this
# script fails on that.
#
# Detection (driven by `cargo public-api`, nightly, `--all-features`): a genuine
# re-export of a TYPE renders the FULL definition line (`pub struct|enum|...`) at
# BOTH the canonical path and the re-export path. We flag any leaf type name that
# has two definition paths where one is a strict module-prefix of the other (the
# re-export shape: a parent module re-exporting a child module's item).
#
# Two genuinely DISTINCT types that merely share a leaf name (each defined
# independently, no re-export between them) are not duplicate paths. The only
# such pairs in this workspace are enumerated in DISTINCT_PAIRS below, each
# verified by hand to be two separate `pub enum`/`type` definitions with their
# own bodies (not a re-export). Any NEW collision fails the check.
#
# The `prelude` namespace is a deliberate curated convenience exception.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v cargo-public-api >/dev/null 2>&1; then
  echo "single-path: cargo-public-api not installed (cargo install cargo-public-api)" >&2
  exit 1
fi

# Verified-distinct same-leaf type pairs (NOT re-exports). Format: the two full
# paths, sorted, joined by a space — must match the pair the detector reports.
DISTINCT_PAIRS="$(cat <<'EOF'
oracledb_protocol::net::Protocol oracledb_protocol::net::connectstring::Protocol
oracledb_protocol::Result oracledb_protocol::sql::Result
EOF
)"

is_distinct_pair() {
  # $1 shorter path, $2 longer path
  local probe="$1 $2"
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    if [ "$line" = "$probe" ]; then
      return 0
    fi
  done <<< "$DISTINCT_PAIRS"
  return 1
}

violations=0

for crate in oracledb oracledb-protocol; do
  api="$(cargo public-api -p "$crate" --all-features 2>/dev/null)"
  if [ -z "$api" ]; then
    echo "single-path: empty public-api for $crate" >&2
    exit 1
  fi

  # Top-level TYPE definitions only (struct/enum/trait/union). Strip the kind and
  # generics/bounds; keep the full path. Drop the `prelude` convenience namespace
  # and any associated item (a pre-leaf segment containing an uppercase letter).
  types="$(printf '%s\n' "$api" \
    | grep -E '^pub (struct|enum|trait|union) ' \
    | sed -E 's/^pub (struct|enum|trait|union) //' \
    | sed -E 's/(<| where | = | : ).*$//' \
    | sed -E 's/[[:space:]]+$//' \
    | { grep -vE '(^|::)prelude(::|$)' || true; } \
    | awk -F'::' '{ ok=1; for (i=1;i<NF;i++) if ($i ~ /[A-Z]/) ok=0; if (ok && NF>=2) print }' \
    | sort -u)"

  # Pair up paths that share a leaf name whose MODULE paths are in a strict
  # prefix relation (a parent module re-exporting a child module's item, e.g.
  # module `tls` re-exporting `tls::wallet`'s `WalletContents`). We compare the
  # module path (full path minus the leaf), not the full path: the re-export and
  # the definition share a leaf but live in parent/child modules.
  dup="$(printf '%s\n' "$types" \
    | awk -F'::' '{ leaf=$NF; mod=$0; sub(/::[^:]+$/,"",mod); print leaf "\t" mod "\t" $0 }' \
    | sort -u \
    | awk -F'\t' '
        { leaf=$1; mods[leaf]=mods[leaf] "\n" $2; full[$1 "\t" $2]=$3 }
        END {
          for (l in mods) {
            c = split(mods[l], a, "\n")
            for (i=1;i<=c;i++) for (j=1;j<=c;j++) {
              if (i==j || a[i]=="" || a[j]=="") continue
              # a[i] strict module-prefix of a[j] => re-export shape.
              if (index(a[j], a[i] "::") == 1) {
                print full[l "\t" a[i]] "\t" full[l "\t" a[j]]
              }
            }
          }
        }')"

  while IFS=$'\t' read -r shorter longer; do
    [ -z "$shorter" ] && continue
    if is_distinct_pair "$shorter" "$longer"; then
      continue
    fi
    leaf="${shorter##*::}"
    echo "single-path: $crate: type '$leaf' reachable via two public paths:" >&2
    echo "  $shorter" >&2
    echo "  $longer" >&2
    violations=$((violations + 1))
  done <<< "$dup"
done

if [ "$violations" -ne 0 ]; then
  echo "single-path: FAILED with $violations duplicate-path type(s)" >&2
  exit 1
fi

echo "single-path: OK — no public type reachable via two public paths"
