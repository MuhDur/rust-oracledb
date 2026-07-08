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

## Round 2 Full Rerun

The round-2 work re-established the full selected baseline on the current tree
before editing:

Output: `/tmp/rust-oracledb-mutants-d64-round2-baseline-20260708145237`

| Metric | Count |
| --- | ---: |
| Mutants tested | 431 |
| Caught | 301 |
| Missed | 112 |
| Unviable | 18 |
| Kill rate, excluding unviable | 72.9% |

Additional tests were then added for:

- Direct heap/stack NUMBER part decoding: emitted digits, decimal-point index,
  integer flag, sign, fused coefficient, and canonical formatted text.
- Owned response wrappers and dispatch: IO vector OUT bind read-back, DML
  RETURNING rows, STATUS/ERROR transaction state, cursor id, row count,
  compilation-warning propagation, rowid, TOKEN, server-side piggyback, flush
  termination, and no-data fetch finalization.
- IO-vector fast-fetch/rowid zero and one-byte boundary skips.
- Describe and column metadata: zero-column describes, annotations, domain
  fields, JSON/OSON flags, and VECTOR dimensions/format/flags.
- Owned column value decode for binary float/double, boolean, intervals,
  timestamps, LOB, VECTOR, JSON, object, cursor, and UROWID boundaries.
- Borrowed row slot decode for borrowed text/raw/number, NCHAR fallback,
  boolean, intervals, and datetime.
- Borrowed response dispatch for DESCRIBE, row header, bit vector, row data,
  parameter, status, server-side piggyback, implicit resultset, token, error,
  duplicate-column carry-forward, and flush termination.
- Query return parameters: registration-info query id and row-count tail.

Final full selected run:

Output: `/tmp/rust-oracledb-mutants-d64-round2-after-tests-20260708152456`

| Metric | Count |
| --- | ---: |
| Mutants tested | 431 |
| Caught | 376 |
| Missed | 38 |
| Unviable | 17 |
| Kill rate, excluding unviable | 90.8% |

Residual misses after the final run:

| Area | Missed | Notes |
| --- | ---: | --- |
| NUMBER digit-walk arithmetic/branch substitutions | 6 | Heap/stack paths remain byte-identical for the tested corpus; the remaining `%` and final-digit branch variants need narrower synthetic wire fixtures. |
| Owned response status/no-data flag condition variants | 4 | Bitwise flag substitutions and no-data branching around response state. |
| Column metadata JSON/OSON flag bitwise variants | 4 | The parsed field values are covered; bitwise alternatives can still survive for some flag combinations. |
| Owned RAW/ROWID arm deletions | 2 | Semantically straightforward follow-up coverage, below the round-2 threshold. |
| Borrowed slot arm deletions | 6 | Most fall back to owned decode and preserve `to_owned_value`; zero-copy shape is covered for hot text, raw, and number paths. |
| Borrowed response bit-vector/error condition variants | 6 | Duplicate-column and no-data behavior is covered; residual condition substitutions remain. |
| Query return parameter block/offset variants | 8 | Query-id and row-count decoding are covered; zero/nonzero block-boundary and byte-offset mutants remain. |
| `QueryValueRef::as_number_text` arm deletions | 2 | Direct and owned fallback accessors are covered by existing tests; inline owned numbers intentionally have no borrowed text. |
