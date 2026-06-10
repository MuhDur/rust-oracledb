# rust-oracledb

Pure-Rust async Oracle Database thin-mode driver, planned as a clean-room port
of python-oracledb thin mode with no OCI or Instant Client dependency.

This is an independent community project and is not affiliated with Oracle.

Current status: M0 harness foundation complete; M1 protocol connection work is
next. The workspace and harness exist so the reference baseline, Rust shim run,
and match-or-beat diff can drive the development loop. The Oracle protocol
implementation is intentionally not claimed complete yet.

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

M0 recorded local artifact counts:

- `harness/.baseline/baseline.json`: 2,260 collected, 2,236 passed, 24 skipped.
- `harness/.results/rust.json`: 2,260 collected, 180 passed, 18 skipped,
  166 failed, 1,896 errored at explicit shim placeholders.
- `harness/run.sh diff`: 2,056 expected M0 regressions, 0 missing tests.

The `.baseline` and `.results` directories are ignored generated artifacts;
rerun the commands above after provisioning the local container.

`harness/run.sh rust` is expected to fail until M1 begins implementing the
thin protocol through the Rust crate.
