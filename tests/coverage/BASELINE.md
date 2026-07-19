# Coverage baseline

**Generated, not hand-authored.** Regenerate with `CARGO_BUILD_JOBS=4 CARGO_TARGET_DIR=target-cov bash scripts/coverage_baseline.sh` (heavy, instrumented; run deliberately, not per-PR). Do not hand-edit this file or `BASELINE.json`.

- Generated at: `2026-07-19T22:28:51Z`
- Git SHA: `cb4de98b0d6b6426d845771510a1a9c3e7449cb9`
- Tool: `cargo-llvm-cov 0.8.7`
- Command: `cargo llvm-cov --workspace --exclude oracledb-pyshim --locked --summary-only --json --output-path /tmp/tmp.xzlMKbIlf5/raw-llvm-cov.json`
- Scope: rust-oracledb driver workspace (crates/*, excluding oracledb-pyshim); the server, oraclemcp, is a separate repo with its own baseline, features=default
- Excluded: oracledb-pyshim, cassette-feature, live-db-suites, doctests
- Unit: source lines/regions/functions under crates/*/src (cargo-llvm-cov's own workspace scoping; integration tests, fuzz targets, examples, and dependencies are not instrumented)

This is an EMPIRICAL baseline only. There is no ratchet or gate here -- the driver's separate mutation gate (`scripts/mutation_gate.py`) and async-blocking coverage gate cover that ground; this file is just the current line/region/function measurement.

## Workspace total

| Metric | Covered | Total | Percent |
| --- | ---: | ---: | ---: |
| lines | 33985 | 42438 | 80.08% |
| regions | 53731 | 67066 | 80.12% |
| functions | 3017 | 3955 | 76.28% |

## Per crate

| Crate | Line % | Lines | Region % | Regions | Function % | Functions |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| oracledb | 75.39% | 16972/22512 | 76.14% | 26976/35429 | 69.21% | 1654/2390 |
| oracledb-derive | 71.31% | 169/237 | 71.51% | 251/351 | 80.0% | 12/15 |
| oracledb-protocol | 85.55% | 16844/19689 | 84.72% | 26504/31286 | 87.16% | 1351/1550 |
