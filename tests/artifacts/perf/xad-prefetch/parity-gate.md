# xad/3oi — gate + parity verification (after prefetch landed)

## Build
- maturin develop --release (pyshim rebuilt against the prefetch-bearing oracledb crate)
- The pyshim is UNCHANGED (keeps the owned fetch path); the prefetch lives only in
  the borrowed `for_each_row_ref` loop in the oracledb crate.

## Gate (CARGO_TARGET_DIR=cargo-target-xad)
- cargo fmt --check                                  : clean
- cargo clippy --workspace --no-deps -- -D warnings  : clean
- cargo test --workspace                             : 39 test groups, 0 failed
  (pre-existing `unwrap`/`unwrap_err` lints in oracledb-protocol & some live test
   files only surface under `--all-targets`, which the gate command does not use;
   all NEW files — prefetch_overlap test, profile_fetch_attribution example,
   thin_driver prefetch bench — are clippy-clean under `-D warnings`.)

## Parity sentinels (Rust shim via -p shim_inject, lane container 1523) — EXACT
| sentinel                | expected | got               |
|-------------------------|----------|-------------------|
| test_1100_connection    | 57p / 5s | 57 passed, 5 skip |
| test_2200_number_var    | 39p      | 39 passed         |
| test_4300_cursor_other  | 73p      | 73 passed         |
| test_8000_dataframe     | 82p      | 82 passed (many rows) |

## Live oracledb integration regression (cargo test -p oracledb -- --include-ignored)
- prefetch_overlap (3 tests)   : pass (correctness, split, drop-mid-prefetch reuse)
- live_borrowed_fetch          : pass (now goes through the prefetch loop)
- cancel_then_reuse            : pass (explicit + drop cancel still clean)
- integration_library (12)     : pass
- live_typed (5), wide_row_multipacket (1) : pass
