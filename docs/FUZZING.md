# Fuzzing the rust-oracledb wire/protocol decoder

`crates/oracledb-protocol` is a `#![forbid(unsafe_code)]` sans-I/O crate that
decodes everything an Oracle server (or a man-in-the-middle) puts on the wire.
A hostile or buggy server must never be able to crash, hang, or OOM the client.
This document describes the coverage-guided fuzzing harnesses that prove the
decoder fails closed — every malformed input returns a `ProtocolError`, never a
panic / infinite loop / unbounded allocation.

The fuzz crate lives at `crates/oracledb-protocol/fuzz/`. It is a **standalone
cargo workspace** (note the empty `[workspace]` table in its `Cargo.toml`), so
the `libfuzzer-sys` / `arbitrary` dependencies never enter the main workspace
lockfile and the normal `cargo build --workspace` is unaffected.

## Targets

Seven libFuzzer targets, one per untrusted decode boundary. Each takes
`data: &[u8]`, guards the input size, and calls the decoder asserting only that
it returns a `Result` (an `Err` is a perfectly good outcome — a panic / OOM /
hang is the bug).

| # | Target | Entry point | What it stresses |
|---|--------|-------------|------------------|
| 1 | `packet_framing` | `packet::TnsPacket::parse` | 8-byte TNS header framing + body length |
| 2 | `query_response` | `thin::parse_query_response` | TTC message dispatch loop, describe-info, column metadata, row data, bit vectors, every per-column scalar codec, implicit resultsets, annotations |
| 3 | `oson_decoder` | `oson::decode_oson` | OSON (binary JSON) offset-indexed node graph: field-name tables, container nodes, absolute tree-segment seeks |
| 4 | `vector_decoder` | `vector::decode_vector` | VECTOR image header + dense/sparse element arrays (f32/f64/int8/binary) |
| 5 | `scalar_codecs` | `fuzz_api::fuzz_scalar_codecs` | NUMBER / DATE / TIMESTAMP(+TZ) / INTERVAL DS+YM / BINARY_FLOAT+DOUBLE raw-byte codecs |
| 6 | `server_error_info` | `fuzz_api::fuzz_parse_server_error_info` + piggyback skip | TTC error trailer (batch error / offset / message sub-arrays, version-gated 20.1+ tail), server-side piggyback opcodes |
| 7 | `dpl_response` | `dpl::parse_direct_path_{prepare,simple}_response` | Direct Path Load response dispatch + column metadata + return parameters |

Targets 5 and 6 reach `pub(crate)` functions through a tiny, **`#[cfg(fuzzing)]`-only**
shim module `oracledb_protocol::fuzz_api` (see `crates/oracledb-protocol/src/lib.rs`).
The shim is compiled only under `--cfg fuzzing` (which `cargo-fuzz` sets
automatically), so it never widens the crate's normal public API. The
`cfg(fuzzing)` flag is registered in the workspace `[workspace.lints.rust]`
`check-cfg` so the `-D warnings` clippy gate stays quiet for the normal build.

## How to run

Prerequisites: `cargo install cargo-fuzz` and a nightly toolchain
(`rustup toolchain install nightly`). All cargo invocations in this lane use:

```bash
export CARGO_TARGET_DIR=/home/durakovic/.cache/cargo-target-w6fuzz
export TMPDIR=/home/durakovic/.cache/tmp
cd crates/oracledb-protocol
```

Type-check every target without running:

```bash
cargo +nightly fuzz check
```

Run one target for a bounded session (ASan + UBSan are on by default; the fuzz
profile additionally enables `overflow-checks` + `debug-assertions` so
arithmetic-overflow panics are caught):

```bash
cargo +nightly fuzz run <target> -- -max_total_time=120 -rss_limit_mb=2048 -timeout=10
```

List targets / minimize a crash / reproduce a saved artifact:

```bash
cargo +nightly fuzz list
cargo +nightly fuzz tmin <target> fuzz/artifacts/<target>/<crash-file>
cargo +nightly fuzz run  <target> fuzz/artifacts/<target>/<crash-file>   # replay
```

Seed corpora live under `fuzz/corpus/<target>/`. The OSON and VECTOR corpora are
seeded from the DB-validated golden images in `tests/golden/`; the others have
hand-crafted minimal-valid + boundary seeds plus the minimized regression inputs
for every bug below.

## Bugs found and fixed

All four were **denial-of-service** bugs reachable from a single adversarial
server packet: three unbounded allocations (OOM) and one arithmetic-overflow
panic. None require authentication state — the decoder processes them before any
trust is established — so each was a client crash a malicious/buggy server could
trigger at will. Every fix is fail-closed (returns a `ProtocolError`) and is
covered by a regression unit test plus a corpus seed.

### 1. OSON decoder — OOM on oversized child / field-name counts
- **Target:** `oson_decoder`
- **Signature:** `SUMMARY: libFuzzer: out-of-memory` (deep recursive frames)
- **Minimized input (20 bytes):** `ff 4a 5a 01 ff 4a 5a 01 21 02 02 00 00 00 09 00 00 00 00 00`
- **Root cause:** `OsonDecoder::decode_container_node` did
  `array.reserve(num_children as usize)` / `object.reserve(...)`, and the
  non-scalar header did `Vec::with_capacity(num_short_field_names + num_long_field_names)`,
  where all counts are attacker-controlled `u32`s read straight off the wire.
  A node claiming ~hundreds of millions of children reserved multiple gigabytes
  of `OsonValue` before reading a single child. OSON offsets are also absolute
  positions in the tree segment, so a child offset pointing back at an ancestor
  could recurse without bound.
- **Fix** (`src/oson.rs`): cap every speculative reservation by the image byte
  length (a child needs ≥1 offset entry + ≥1 tree byte, so a count larger than
  the image is necessarily a lie) and add an explicit `MAX_OSON_DEPTH` (1000)
  recursion-depth guard in `decode_node`. The per-read bounds checks still fail
  closed on truncation.
- **Regression:** `oson::tests::fuzz_regression_oom_oversized_counts`,
  `oson::tests::fuzz_regression_deep_nesting_is_bounded`;
  seed `fuzz/corpus/oson_decoder/regression_oom_counts`.

### 2. VECTOR decoder — OOM on oversized element count
- **Target:** `vector_decoder`
- **Signature:** `SUMMARY: libFuzzer: out-of-memory`
- **Minimized input (17 bytes):** `db 00 00 12 03 36 00 00 00 00 00 00 00 00 00 00 00`
- **Root cause:** `vector::decode_values` did `Vec::with_capacity(count)` where
  `count` is the header's `u32` element count. A FLOAT64 vector advertising
  ~905M elements (`num_elements = 0x36000000`) reserved ~7 GB before the first
  truncated element read could fail.
- **Fix** (`src/vector.rs`): cap the initial reservation by
  `reader.remaining() / element_size` — a legitimate image always carries
  `count * element_size` value bytes, so this never affects a valid vector while
  making the allocation fail-closed; the per-element `read_raw` still bounds-checks.
- **Regression:** `vector::tests::fuzz_regression_oom_oversized_element_count`;
  seed `fuzz/corpus/vector_decoder/regression_oom_count`.

### 3. Query response — OOM on oversized implicit-resultset / collection counts
- **Target:** `query_response`
- **Signature:** `SUMMARY: libFuzzer: out-of-memory`
- **Minimized input (payload):** `1b 04 25 00 00 00`
  (`TNS_MSG_TYPE_IMPLICIT_RESULTSET` = 27, then a ub4 count of ~620M)
- **Root cause:** several `Vec::with_capacity(num as usize)` sites in
  `src/thin/fetch.rs` reserved from `u32` wire counts before reading any
  element: implicit-resultset count (`_process_implicit_result`), per-column
  annotation count, out-bind array `num_elements`, DML-returning `num_rows`, and
  the `arraydmlrowcounts` `num_rows`.
- **Fix** (`src/thin/fetch.rs`): bound every such reservation by
  `reader.remaining()` (each loop iteration consumes ≥1 byte, so the payload
  size is a hard ceiling on the true element count). The loop bodies already
  fail closed on truncation.
- **Regression:** `thin::fetch::fuzz_regression_tests::fuzz_regression_implicit_resultset_oom`;
  seed `fuzz/corpus/query_response/regression_implicit_rs_oom`.

### 4. TTC reader — `read_sb4` / `read_sb8` negate-overflow panic
- **Target:** `query_response` (surfaced once bug #3 stopped masking it)
- **Signature:** `panicked at src/wire.rs:289: attempt to negate with overflow`
  → `libFuzzer: deadly signal`
- **Root cause:** `TtcReader::read_sb4` / `read_sb8` accumulated the magnitude in
  a *signed* integer and then did `value = -value` for the negative-flagged
  length. A server sending four bytes `80 00 00 00` with the negative-length
  flag yields `i32::MIN`, and `-i32::MIN` overflows (the intermediate
  `value << 8` could also overflow). Harmless in release without overflow-checks,
  but a real panic under `debug-assertions` and undefined-intent on the wire.
- **Fix** (`src/wire.rs`): accumulate in the unsigned width (`u32`/`u64`),
  reinterpret as signed, and negate with `wrapping_neg()` — matching the
  reference C decoder's two's-complement behavior and never panicking.
- **Regression:** `wire::tests::sb4_sb8_negate_overflow_does_not_panic`,
  `wire::tests::sb4_decodes_representative_values`;
  seed `fuzz/corpus/query_response/regression_sb4_negate_overflow`.

## Clean-run evidence

After all four fixes, each target was re-run for a bounded 120 s libFuzzer
session (`-max_total_time=120 -rss_limit_mb=2048 -timeout=10`, ASan + UBSan +
overflow-checks). **Zero surviving crashes** across all seven targets:

| Target | Executions (120 s) | exec/s | Coverage (edges / features) | Crashes |
|--------|-------------------:|-------:|----------------------------|:-------:|
| `packet_framing`    | 86,977,255 | 718,820 | 41 / 48      | 0 |
| `query_response`    |  4,561,592 |  37,699 | 1902 / 9177  | 0 |
| `oson_decoder`      |    723,216 |   5,976 | 704 / 2743   | 0 |
| `vector_decoder`    | 14,450,847 | 119,428 | 210 / 483    | 0 |
| `scalar_codecs`     | 22,992,619 | 190,021 | 220 / 458    | 0 |
| `server_error_info` |  9,598,714 |  79,328 | 629 / 1837   | 0 |
| `dpl_response`      |  5,541,476 |  45,797 | 958 / 4396   | 0 |

Every target runs well above the 1000 exec/s parser floor. Representative
session tail:

```
#86977255    DONE   cov: 41 ft: 48 corp: 5/34b lim: 4096 exec/s: 718820 rss: 583Mb
Done 86977255 runs in 121 second(s)        # packet_framing, 0 crashes
```

Gate status alongside the fuzzing: `cargo fmt --check` clean,
`cargo clippy --workspace --no-deps -- -D warnings` clean,
`cargo test --workspace` green (177 tests passing, including the four new
fuzz-regression tests). The fuzz crate is excluded from the workspace and adds
no dependencies to the `oracledb-protocol` dependency tree.
