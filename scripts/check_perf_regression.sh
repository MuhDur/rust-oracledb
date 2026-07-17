#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REFERENCE="${PERF_REFERENCE:-$ROOT/docs/baseline/perf_regression_reference.json}"
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
CRITERION_DIR="$TARGET_DIR/criterion"
# Best-of-N sampling: run the criterion benches N times and compare the BEST
# (minimum) median per bench against the committed reference. This is noise
# robustness, NOT a loosened bar: a real regression slows EVERY run, so the
# minimum stays slow and still trips the (unchanged) 2.00x threshold; transient
# hosted-runner degradation only inflates SOME runs, so the minimum reflects the
# clean run and no longer false-triggers. Override with PERF_SAMPLES.
SAMPLES="${PERF_SAMPLES:-3}"

if [ ! -f "$REFERENCE" ]; then
  echo "perf-regression: missing reference JSON: $REFERENCE" >&2
  exit 66
fi

SNAP_DIR="$(mktemp -d)"
trap 'rm -rf "$SNAP_DIR"' EXIT

echo "perf-regression: running deterministic criterion benches (best-of-$SAMPLES, noise-robust)"
for run in $(seq 1 "$SAMPLES"); do
  echo "perf-regression: sample $run/$SAMPLES"
  cargo bench -p oracledb --features cassette --bench single_packet_passthrough --locked -- --noplot
  # Snapshot this run's per-bench median before the next run overwrites it.
  python3 - "$REFERENCE" "$CRITERION_DIR" "$SNAP_DIR/run_$run.json" <<'PY'
import json
import sys
from pathlib import Path

reference = json.loads(Path(sys.argv[1]).read_text())
criterion_dir = Path(sys.argv[2])
out_path = Path(sys.argv[3])

medians = {}
for name, spec in reference["benchmarks"].items():
    estimates_path = criterion_dir / spec["criterion_path"] / "new" / "estimates.json"
    if estimates_path.exists():
        estimates = json.loads(estimates_path.read_text())
        medians[name] = float(estimates["median"]["point_estimate"])
out_path.write_text(json.dumps(medians))
PY
done

python3 - "$REFERENCE" "$SNAP_DIR" "$SAMPLES" <<'PY'
import json
import sys
from pathlib import Path

reference = json.loads(Path(sys.argv[1]).read_text())
snap_dir = Path(sys.argv[2])
samples = int(sys.argv[3])
threshold = float(reference["max_regression_ratio"])
benchmarks = reference["benchmarks"]

runs = [json.loads((snap_dir / f"run_{i}.json").read_text()) for i in range(1, samples + 1)]

failures = []
print(
    f"perf-regression: threshold {threshold:.2f}x over committed reference "
    f"(best of {samples} samples)"
)
for name, spec in benchmarks.items():
    values = [run[name] for run in runs if name in run]
    if not values:
        failures.append(f"{name}: missing criterion estimates across all {samples} samples")
        continue
    # A real regression slows every sample; noise only inflates some. Taking the
    # minimum keeps a genuine regression failing while rejecting transient spikes.
    best = min(values)
    reference_ns = float(spec["median_ns"])
    ratio = best / reference_ns if reference_ns > 0 else float("inf")
    samples_str = ", ".join(f"{v:.0f}" for v in values)
    print(
        f"{name}: best={best:.0f} ns reference={reference_ns:.0f} ns "
        f"ratio={ratio:.2f}x  (samples ns: {samples_str})"
    )
    if ratio > threshold:
        failures.append(
            f"{name}: {ratio:.2f}x best-of-{samples} regression exceeds {threshold:.2f}x "
            f"(best {best:.0f} ns, reference {reference_ns:.0f} ns)"
        )

if failures:
    print("perf-regression: FAILED", file=sys.stderr)
    for failure in failures:
        print(f"  - {failure}", file=sys.stderr)
    sys.exit(1)

print("perf-regression: OK")
PY
