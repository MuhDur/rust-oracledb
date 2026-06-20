#!/usr/bin/env bash
# Verify that docs/API_LEDGER.md covers every cargo-public-api baseline line.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LEDGER="$ROOT/docs/API_LEDGER.md"
API_DIR="$ROOT/docs/baseline/public_api"

if [ ! -f "$LEDGER" ]; then
  echo "api-ledger: missing $LEDGER" >&2
  exit 1
fi

if [ ! -d "$API_DIR" ]; then
  echo "api-ledger: missing $API_DIR" >&2
  exit 1
fi

declare -a patterns=()
declare -a dispositions=()
declare -a matched=()

while IFS=$'\t' read -r pattern disposition reason; do
  if [ "$pattern" = "pattern" ]; then
    continue
  fi
  case "$disposition" in
    keep|pub\(crate\)|rename|consolidate|deprecate) ;;
    *)
      echo "api-ledger: invalid disposition '$disposition' for pattern '$pattern'" >&2
      exit 1
      ;;
  esac
  if [ -z "${reason:-}" ]; then
    echo "api-ledger: missing reason for pattern '$pattern'" >&2
    exit 1
  fi
  patterns+=("$pattern")
  dispositions+=("$disposition")
  matched+=(0)
done < <(
  awk '
    $0 == "```api-ledger" { in_block = 1; next }
    in_block && $0 == "```" { in_block = 0; next }
    in_block && NF > 0 { print }
  ' "$LEDGER"
)

if [ "${#patterns[@]}" -eq 0 ]; then
  echo "api-ledger: no api-ledger patterns found in $LEDGER" >&2
  exit 1
fi

missing=0
total=0
while IFS= read -r api_line; do
  total=$((total + 1))
  covered=false
  for i in "${!patterns[@]}"; do
    if [[ "$api_line" == ${patterns[$i]} ]]; then
      matched[$i]=$((matched[$i] + 1))
      covered=true
      break
    fi
  done
  if [ "$covered" = false ]; then
    echo "api-ledger: uncovered public API line: $api_line" >&2
    missing=$((missing + 1))
  fi
done < <(
  awk 'FNR > 4 && length($0) > 0 { print }' "$API_DIR"/*.txt | sort -u
)

unused=0
for i in "${!patterns[@]}"; do
  if [ "${matched[$i]}" -eq 0 ]; then
    echo "api-ledger: unused pattern: ${patterns[$i]} (${dispositions[$i]})" >&2
    unused=$((unused + 1))
  fi
done

if [ "$missing" -ne 0 ] || [ "$unused" -ne 0 ]; then
  echo "api-ledger: coverage failed; missing=$missing unused=$unused total=$total" >&2
  exit 1
fi

echo "api-ledger: covered $total public API lines with ${#patterns[@]} ledger patterns"
