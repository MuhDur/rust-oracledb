#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REFERENCE="${PERF_REFERENCE:-$ROOT/docs/baseline/perf_regression_reference.json}"
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
CRITERION_DIR="$TARGET_DIR/criterion"

if [ ! -f "$REFERENCE" ]; then
  echo "perf-regression: missing reference JSON: $REFERENCE" >&2
  exit 66
fi

echo "perf-regression: running deterministic criterion benches"
cargo bench -p oracledb --features cassette --bench single_packet_passthrough -- --noplot

python3 - "$REFERENCE" "$CRITERION_DIR" <<'PY'
import json
import sys
from pathlib import Path

reference_path = Path(sys.argv[1])
criterion_dir = Path(sys.argv[2])
reference = json.loads(reference_path.read_text())
threshold = float(reference["max_regression_ratio"])
benchmarks = reference["benchmarks"]

failures = []
print(f"perf-regression: threshold {threshold:.2f}x over committed reference")
for name, spec in benchmarks.items():
    estimates_path = criterion_dir / spec["criterion_path"] / "new" / "estimates.json"
    if not estimates_path.exists():
        failures.append(f"{name}: missing criterion estimates at {estimates_path}")
        continue
    estimates = json.loads(estimates_path.read_text())
    current = float(estimates["median"]["point_estimate"])
    reference_ns = float(spec["median_ns"])
    ratio = current / reference_ns if reference_ns > 0 else float("inf")
    print(
        f"{name}: current={current:.0f} ns reference={reference_ns:.0f} ns "
        f"ratio={ratio:.2f}x"
    )
    if ratio > threshold:
        failures.append(
            f"{name}: {ratio:.2f}x regression exceeds {threshold:.2f}x "
            f"(current {current:.0f} ns, reference {reference_ns:.0f} ns)"
        )

if failures:
    print("perf-regression: FAILED", file=sys.stderr)
    for failure in failures:
        print(f"  - {failure}", file=sys.stderr)
    sys.exit(1)

print("perf-regression: OK")
PY
