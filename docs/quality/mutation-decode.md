# Decode Mutation Campaign

Date: 2026-07-08

## Scope

This campaign covered thin-driver decode boundaries in
`oracledb-protocol`, focused on:

- `crates/oracledb-protocol/src/thin/codecs.rs`
- `crates/oracledb-protocol/src/thin/fetch.rs`
- `crates/oracledb-protocol/src/thin/types.rs`

The selected mutant regex was:

```text
decode_|parse_|QueryValueRef|QueryResult::cell|QueryResult::column_index|QueryValue::number_from_text
```

All cargo-heavy runs used:

- `systemd-run --user --scope -q -p MemoryMax=16G -p MemorySwapMax=0`
- `CARGO_TARGET_DIR=/home/durakovic/.cache/cargo-target-driver`
- `CARGO_BUILD_JOBS=16`
- `cargo mutants --jobs 2 --timeout 180 --build-timeout 180 --baseline run --no-times`

## Full Selected Run

Output: `/tmp/rust-oracledb-mutants-d64-20260708123505`

Result:

| Metric | Count |
| --- | ---: |
| Mutants tested | 431 |
| Caught | 281 |
| Missed | 135 |
| Unviable | 15 |
| Kill rate, excluding unviable | 67.5% |

Largest missed buckets:

| Area | Missed |
| --- | ---: |
| `fetch.rs` query response parsing | 35 |
| `fetch.rs` column slot/value/metadata parsing | 34 |
| `codecs.rs` number part decoding | 18 |
| `fetch.rs` IO vector parsing | 9 |
| `types.rs` `QueryValueRef` accessors | 16 |

## Added Coverage

Focused tests were added for the main missed decode surfaces:

- Truncated interval day-to-second wire values are rejected.
- Heap and stack NUMBER decode paths agree on edge-case decimal and integer corpus values.
- `QueryValueRef` accessors cover direct borrowed values and owned fallbacks.
- `ExecuteOptions::parse_only` round-trips through its builder accessor.
- IO vector parsing skips optional fast-fetch and rowid payloads and ignores slots beyond bind count.

The test inventory baseline was regenerated after adding these tests and the
new fuzz target. Public API baselines and the API ledger were unchanged.

## Targeted Rerun

Output: `/tmp/rust-oracledb-mutants-d64-targeted-20260708125808`

Target regex:

```text
decode_interval_ds|decode_number_parts|decode_number_parts_stack|parse_io_vector|QueryValueRef|ExecuteOptions::parse_only|ExecuteOptions::with_parse_only
```

Result:

| Metric | Count |
| --- | ---: |
| Mutants tested | 159 |
| Caught | 139 |
| Missed | 16 |
| Unviable | 4 |
| Kill rate, excluding unviable | 89.7% |

Residual misses after hardening:

- `decode_number_parts`: 6 arithmetic or branch substitutions in digit-walk normalization.
- `decode_number_parts_stack`: 6 matching arithmetic or branch substitutions on the stack path.
- `parse_io_vector`: 4 optional-length comparison substitutions around fast-fetch and rowid skip logic.

These residual misses are documented for the orchestrator-side D6.4 audit
guard. Driver-side changes remain test-only and do not alter the public API
contract.
