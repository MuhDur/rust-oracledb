# 1.0.0-rc.1 Release Qualification Evidence

- Candidate SHA: `b4a0cd3e77e3d7ed9cd875ba8002968860c9954a`
- HEAD at qualification: `b4a0cd3e77e3d7ed9cd875ba8002968860c9954a`
- Qualified on: 2026-06-22T18:28:26Z
- Profile: release-qualification / budget: release (soak-equivalent; fuzz local smoke 30s/target, CI soak 120s)
- Conformance (python-oracledb thin differential): baseline_count=2578, current_count=2578, **regression_count=0, missing_count=0** (rust harness: 2578 collected, 2462 passed, 116 skipped)
- Perf (deterministic criterion vs committed reference, 2.0x threshold): all benches 0.98x–1.04x — `perf-regression: OK`
- Supply chain (cargo-deny): advisories ok, bans ok, licenses ok, sources ok

## Tool versions
```
cargo 1.97.0-nightly (a343accce 2026-05-08)
cargo 1.95.0 (f2d3ce0bd 2026-03-21)
cargo-public-api 0.52.0
cargo-semver-checks 0.48.0
cargo-deny 0.19.7
```

## Checks

| Check | Result | Exit |
| --- | --- | --- |
| fmt | PASS | 0 |
| clippy | PASS | 0 |
| test_workspace | PASS | 0 |
| test_cassette | PASS | 0 |
| test_docs | PASS | 0 |
| build_docs | PASS | 0 |
| stable_protocol | PASS | 0 |
| feature_default | PASS | 0 |
| feature_min | PASS | 0 |
| feature_all | PASS | 0 |
| feature_matrix | PASS | 0 |
| release_preflight | PASS | 0 |
| baseline_drift | PASS | 0 |
| api_ledger | PASS | 0 |
| single_path | PASS | 0 |
| async_blocking | PASS | 0 |
| semver_protocol | PASS | 0 |
| semver_driver | PASS | 0 |
| cargo_deny | PASS | 0 |
| cargo_package | PASS | 0 |
| musl_size | SKIP (musl-gcc not installed; CI-only) | - |
| fuzz_build | PASS | 0 |
| fuzz_smoke_30s_per_target | PASS | 0 |
| perf_regression | PASS | 0 |
| conformance_rust | PASS | 0 |
| conformance_diff | PASS | 0 |

Total FAIL count: 0

## Live driver suite (serial, against FREEPDB1)

The CI gate suite above runs `cargo test` without the live DB env, so the driver's
own self-gating live tests self-skip there; they are covered by the live
differential conformance suite (2578 tests, 0 regressions) and additionally by an
explicit serial run of the whole driver test suite with the live env set:

- `cargo test --workspace --exclude oracledb-pyshim -- --test-threads=1` (live env): **0 failures** across all test binaries.
- The x2ye fix is exercised live: `async_rows_into_typed_drains_all_batches`
  returns all 105 rows across 5 fetch batches — **ok**.

Note (non-blocking): under the *default* parallel `cargo test` against this single
small FREEPDB1 container, one transient `unknown TTC message type 129` framing
error was observed once immediately after the heavy conformance run (residual
container load). It did **not** reproduce: the test passes in isolation, passes
serially (12/12), and passed three consecutive default-parallel reruns (12/12
each). Classified as container-contention flake, not a driver defect; the driver
live suite is run serially here (mirroring the conformance harness's controlled
concurrency).
