#!/usr/bin/env bash
# Verify the generated async/blocking surface table has no untriaged gaps.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COVERAGE="$ROOT/docs/baseline/async_blocking_coverage.tsv"

if [ ! -f "$COVERAGE" ]; then
  echo "async-blocking: missing $COVERAGE" >&2
  exit 1
fi

awk -F '\t' '
  NR == 1 {
    expected = "surface\tasync_owner\tasync_method\tblocking_owner\tblocking_method\tstatus\tnote"
    if ($0 != expected) {
      print "async-blocking: invalid header: " $0 > "/dev/stderr"
      exit 1
    }
    next
  }
  NF != 7 {
    print "async-blocking: invalid row " NR " field-count=" NF > "/dev/stderr"
    failures++
    next
  }
  $6 == "covered" {
    covered++
    next
  }
  $6 == "exception" {
    if ($7 == "") {
      print "async-blocking: exception without note at row " NR > "/dev/stderr"
      failures++
    }
    exceptions++
    next
  }
  {
    print "async-blocking: unresolved " $6 " row " NR ": " $1 " " $2 "::" $3 > "/dev/stderr"
    failures++
  }
  END {
    if (NR == 1) {
      print "async-blocking: empty coverage table" > "/dev/stderr"
      failures++
    }
    if (failures) {
      print "async-blocking: coverage failed; failures=" failures " covered=" covered " exceptions=" exceptions > "/dev/stderr"
      exit 1
    }
    print "async-blocking: covered " covered " async methods with " exceptions " documented exceptions"
  }
' "$COVERAGE"
