# parity-gate.md — perf-push GATE verification

All gates run on branch `perf-push` (master base) against the reused container
`rust-oracledb-lane-1523` (loopback `localhost:1523/FREEPDB1`), shim rebuilt with
`maturin develop` after the perf changes.

## Build / lint gate

| gate | result |
|------|--------|
| `cargo fmt --check` | clean |
| `cargo clippy --workspace --no-deps -- -D warnings` | clean (Finished, no errors/warnings) |
| `cargo clippy -p oracledb --no-deps --features arrow -- -D warnings` | clean |
| `cargo test --workspace` (default features) | green (45 test binaries, 0 failures) |
| `cargo test --workspace --features oracledb/arrow` | green (0 failures) |
| `cargo build -p oracledb --benches --features arrow` | compiles (bench-compile gate) |
| `#![forbid(unsafe_code)]` | intact (oracledb lib.rs:116, protocol lib.rs + wire.rs); arrow.rs has zero `unsafe` |

## New differential / measurement tests

| test | result |
|------|--------|
| `arrow_columnar_diff` (synthetic + LIVE + leak probes) | 7 passed |
| `arrow_columnar_alloc` (counting allocator, asserts >=3x alloc cut) | 1 passed |
| `execute_payload_alloc` (asserts <=2 allocs for execute payload) | 1 passed |
| protocol crate wire-correctness suite | 246 passed |

## Parity sentinels — EXACT

Run with the reference python-oracledb thin suite through the shim
(`pytest <file> -p shim_inject`, `PYTHONPATH=harness`):

| sentinel | expected | measured | status |
|----------|----------|----------|--------|
| `test_1100_connection` | 57p / 5s | **57 passed, 5 skipped** | EXACT |
| `test_2200_number_var` | 39p | **39 passed** | EXACT |
| `test_8000_dataframe` (Arrow dataframes) | 82p | **82 passed** | EXACT |
| `test_6400_vector_var` (VECTOR) | 46p / 2s | **46 passed, 2 skipped** | EXACT |

Parity is GREEN and EXACT. The columnar-Arrow path is a new public crate API that
the shim's `fetch_df_all` does not yet route through (it keeps its proven row path
with LOB inlining / define-fetch / output-type-handler support), so the Arrow
dataframe parity is unchanged. The cursor-release fix and the per-call allocation
micro-opts are byte/behaviour-preserving, so all four sentinels hold exactly.
