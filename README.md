# rust-oracledb

Pure-Rust async Oracle Database thin-mode driver, planned as a clean-room port
of python-oracledb thin mode with no OCI or Instant Client dependency.

This is an independent community project and is not affiliated with Oracle.

Current status: M0 (harness foundation) and M1 (connect, 12c/11g auth, identity
masquerade gate) are complete. M2 (execute/fetch/binds/core types) is in
progress: on the freshest per-module evidence, 24 of the 72 in-scope suite
modules are fully green and just over half of all executed tests pass, with the
remaining failures mapped to a known set of root causes (pool, intervals, type
handlers, pipelining, dataframes, direct path load). The Oracle protocol
implementation is intentionally not claimed complete yet. See
[docs/GROUND_TRUTH.md](docs/GROUND_TRUTH.md) for the evidence-backed per-module
status table, known debt ledger, and coordination rules.

See [PLAN_TO_PORT_PYTHON_ORACLEDB_THIN_TO_RUST.md](PLAN_TO_PORT_PYTHON_ORACLEDB_THIN_TO_RUST.md)
for the authoritative milestone contract.

## Workspace

- `crates/oracledb-protocol`: sans-I/O TNS/TTC protocol core.
- `crates/oracledb`: Asupersync-based driver crate.
- `crates/oracledb-pyshim`: harness-only PyO3 module injected as
  `oracledb.thin_impl`.
- `harness/`: filtered python-oracledb suite runner and shim injection.

## M0 Commands

```bash
scripts/pin-reference.sh
scripts/setup-python-env.sh
scripts/container.sh up
scripts/container.sh health
eval "$(scripts/container.sh env)"
scripts/prepare-local-oracle.py
harness/run.sh list
harness/run.sh baseline
harness/run.sh rust
harness/run.sh diff
```

The harness defaults to segmented execution so the local Oracle Free container
does not accumulate pressure from the full 72-module run in a single pytest
process. Set `ORACLEDB_HARNESS_MODE=single` to force one pytest invocation.

Recorded local artifact counts:

- `harness/.baseline/baseline.json`: 2,260 collected, 2,236 passed, 24 skipped
  (the match-or-beat target).
- Rust shim runs write per-module JSONs under `harness/.results/parts-rust-*/`;
  the current per-module tallies are recorded in
  [docs/GROUND_TRUTH.md](docs/GROUND_TRUTH.md).

The `.baseline` and `.results` directories are ignored generated artifacts;
rerun the commands above after provisioning the local container.

`harness/run.sh rust` still reports failures in the modules listed red in the
ground truth status table; the remaining red clusters are tracked there and in
the beads issue list.
