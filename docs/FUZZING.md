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

Twenty libFuzzer targets cover the untrusted decode/parse boundaries. Most take
`data: &[u8]`, guard the input size, and call the decoder asserting only that
it returns a `Result` (an `Err` is a perfectly good outcome — a panic / OOM /
hang is the bug). The connect-string target (#10) is **structure-aware**: it
takes an `Arbitrary`-shaped input rather than raw bytes, because a grammar this
deep is unreachable by byte-level mutation alone (see its row below).

| # | Target | Entry point | What it stresses |
|---|--------|-------------|------------------|
| 1 | `packet_framing` | `packet::TnsPacket::parse` | 8-byte TNS header framing + body length |
| 2 | `query_response` | `thin::parse_query_response` | TTC message dispatch loop, describe-info, column metadata, row data, bit vectors, every per-column scalar codec, implicit resultsets, annotations |
| 3 | `oson_decoder` | `oson::decode_oson` | OSON (binary JSON) offset-indexed node graph: field-name tables, container nodes, absolute tree-segment seeks |
| 4 | `vector_decoder` | `vector::decode_vector` | VECTOR image header + dense/sparse element arrays (f32/f64/int8/binary) |
| 5 | `scalar_codecs` | `fuzz_api::fuzz_scalar_codecs` | NUMBER / DATE / TIMESTAMP(+TZ) / INTERVAL DS+YM / BINARY_FLOAT+DOUBLE raw-byte codecs |
| 6 | `server_error_info` | `fuzz_api::fuzz_parse_server_error_info` + piggyback skip | TTC error trailer (batch error / offset / message sub-arrays, version-gated 20.1+ tail), server-side piggyback opcodes |
| 7 | `dpl_response` | `dpl::parse_direct_path_{prepare,simple}_response` | Direct Path Load response dispatch + column metadata + return parameters |
| 8 | `aq_response` | `fuzz_api::fuzz_aq_responses` | Advanced Queuing enqueue / dequeue / array response decoders (RAW / JSON / Object payloads) |
| 9 | `subscr_response` | `fuzz_api::fuzz_subscr_responses` | Subscription (CQN / AQ-notification) subscribe-response + notification-stream decoders (OAC records, grouping notifications) |
| 10 | `connect_string` | `fuzz_api::fuzz_connect_string` | TNS connect-descriptor / EZConnect-Plus parser (`net::connectstring::parse`) + in-memory tnsnames.ora lexer; **structure-aware** (nested-paren `Arbitrary` generator) |
| 11 | `alter_session` | `thin::parse_alter_session_response` | ALTER SESSION response status and trailer handling |
| 12 | `auth_response` | `thin::parse_auth_response` | FAST_AUTH response dictionaries, verifier fields, capabilities, and session data |
| 13 | `accept_payload` | `thin::parse_accept_payload` | Listener ACCEPT payload framing and negotiated flags |
| 14 | `dbobject_image` | `thin::decode_object_value` / DbObject image reader | DbObject/collection image lengths, prefixes, and value payloads |
| 15 | `dbobject_scalars` | DbObject scalar decoders + temporal descriptor normalizer | Object attribute scalar conversion boundaries and bounded UTF-8 `TIMESTAMP`/`TIMESTAMP WITH [LOCAL] TZ` classification |
| 16 | `lob_responses` | LOB response parsers | LOB read/write/trim/free/create response shapes |
| 17 | `sessionless_tpc` | sessionless transaction/TPC parsers | Sessionless transaction piggybacks and TPC state responses |
| 18 | `oac_record` | OAC record parser | Oracle access/control records used in notifications and auth-adjacent payloads |
| 19 | `wallet_parsers` | wallet PEM/SSO parser entry points | Wallet container parsing and typed fail-closed diagnostics |
| 20 | `borrowed_query_response` | borrowed query/define response parsers | Zero-copy row/LOB decode paths and borrowed lifetime-safe parsing |

Several targets reach `pub(crate)` (or `#[cfg(fuzzing)] pub`)
functions through a tiny, **`#[cfg(fuzzing)]`-only** shim module
`oracledb_protocol::fuzz_api` (see `crates/oracledb-protocol/src/lib.rs`).
The shim is compiled only under `--cfg fuzzing` (which `cargo-fuzz` sets
automatically), so it never widens the crate's normal public API. The
`cfg(fuzzing)` flag is registered in the workspace `[workspace.lints.rust]`
`check-cfg` so the `-D warnings` clippy gate stays quiet for the normal build.

### 0.8.4 release: new untrusted-input coverage audit

The 0.8.4 release changed three surfaces that were reviewed against the fuzz
harness. The audit adds no duplicate target: only the temporal normalizer lacked
a direct drive, so it was added to the existing size-guarded `dbobject_scalars`
target under `#[cfg(fuzzing)]`. Its corpus includes a 23ai `TIMESTAMP WITH TZ`
name, a whitespace/precision boundary spelling, and a prefix-collision ADT.

| Release surface | Coverage | Fail-closed boundary |
|---|---|---|
| TSTZ/TSLTZ descriptor normalizer | `dbobject_scalars` drives `public_dbtype_name_from_oracle_type_name`, raw/public precision-scale normalization, and the three temporal seeds. | It receives only bounded UTF-8 text after TTC decoding. It is a classifier, not a decoder: malformed names safely fall through as ADTs, while malformed TTC text is rejected by the existing bounded decoder targets. |
| GH-14 `EXPIRE_TIME` and transport-timeout syntax | Existing structure-aware `connect_string` target already emits `EXPIRE_TIME`, `TRANSPORT_CONNECT_TIMEOUT`, duration atoms, raw strings, and malformed descriptor tails through `net::connectstring::parse`. | `parse_uint` / `parse_duration` return typed parse errors; bounded grammar recursion rejects malformed input rather than hanging or overflowing. |
| asupersync 0.3.9 / BCE DATE mapping | Existing `scalar_codecs` target drives raw DATE/TIMESTAMP decoding. The subsequent `runtime_error` mapping consumes a fixed internal error message and introduces no new byte or string parser. | Invalid DATE bytes remain a protocol decode error; the PyO3 boundary maps the established `invalid DATE year` error to `ValueError` without reparsing attacker input. |

On the pinned `nightly-2026-05-11`, the audit ran ASan for 20 seconds against
`dbobject_scalars` and 12 seconds each against `connect_string` and
`scalar_codecs`, all with the 2 GiB RSS/malloc cap and zero crash artifacts.
Miri also passed `descriptor_normalizer_folds_only_builtin_grammar`. This
toolchain cannot run UBSan: `cargo-fuzz 0.13.1` exposes no `undefined`
sanitizer and `rustc -Zsanitizer=undefined` rejects that value. The fuzz profile
still enables debug assertions and overflow checks. A future sanitizer upgrade
must add a real UBSan run before claiming one; a sanitizer crash would be
minimized with `cargo fuzz tmin` and converted to a regression test before this
release could proceed.

### Target #10: the connect-string parser (structure-aware)

`net::connectstring::parse` and the tnsnames.ora reader consume **untrusted env
/ config / user input** (a `TNS_ADMIN` file, an `ORACLE_CONNECT_STRING`, a DSN
typed by an operator) and, before this lane, had only unit tests. A hostile or
fat-fingered connect string must fail closed (`Err`) — never panic, OOM, or
overflow the stack. The descriptor recursion-depth DoS was fixed in bead `uf8`
(`MAX_DESCRIPTOR_DEPTH = 128`); this target **guards that fix and hunts
siblings** in the EZConnect host/port/quote lexer and the tnsnames comment /
multi-line / paren-balancing tokenizer.

Dumb random bytes almost never reach the interesting states here: the very first
byte must be `(` to enter the descriptor parser, and every deep branch is gated
behind balanced parens and `KEY=` tokens. So the target is **structure-aware**
(`fuzz_targets/connect_string.rs` implements `Arbitrary` for a `ConnectInput`):
a selector byte chooses among (1) a recursive nested-`(KEY=VALUE)` generator
drawing keywords/atoms from the real descriptor grammar (reaching quoted values,
container keywords, and the EZConnect host/port/service forms); (2) a
deliberately over-deep nest (100–400 levels) that drives the
`MAX_DESCRIPTOR_DEPTH` fail-closed path on every run; (3) a valid descriptor
*prefix* + arbitrary garbage tail (the "good so far, then malformed" transition
states); and (4) the raw bytes verbatim (so libFuzzer's byte mutation and the
saved corpus still feed the parser directly). The in-memory tnsnames lexer is
reached through `#[cfg(fuzzing)] pub fn tnsnames::fuzz_parse_file` (the `IFILE`
recursion itself is I/O-bound and is covered by the `ifile_*` unit tests).

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

Run one target for a bounded session (ASan is on by default; the fuzz profile
additionally enables `overflow-checks` + `debug-assertions` so arithmetic-
overflow panics are caught). The pinned toolchain has no UBSan mode; see the
0.8.4 audit above for the exact limitation and Miri complement:

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

## OOM-from-length is now closed by construction

Bugs #1–#3 above were the same bug three times: a length/count field read from
the wire (`u16`/`u32`/`u64`) drove an unbounded `Vec::with_capacity(count)` /
`reserve(count)` *before* a single element was read, so a hostile/buggy server
could force a multi-gigabyte allocation (OOM DoS) with a few bytes. They were
fixed reactively, one decoder at a time. That whole class is now closed
**structurally** rather than case by case.

The invariant: *a length/count field read from the wire can never cause an
allocation larger than the bytes actually remaining in the current message
buffer.* You cannot have `N` elements if fewer than `N * min_bytes_per_elem`
bytes remain.

It is enforced by the **`BoundedReader`** trait (`src/wire.rs`), implemented for
every reader over an untrusted buffer — `TtcReader` (which also serves
`vector.rs`, `fetch.rs`, `dpl.rs`), the OSON `OsonReader`, the CQN `ByteCursor`
(`subscr.rs`), and the `DbObjectPackedReader` (`dbobject.rs`). It anchors two
primitives on `remaining()`:

- `alloc_count_checked(count, min_bytes_per_elem) -> Result<usize>` — fail
  *closed* early with a `ProtocolError` (never a panic, never an OOM) when
  `count * min_bytes_per_elem` exceeds `remaining()` (saturating on overflow).
- `with_capacity_bounded::<T>(count, min_bytes_per_elem) -> Vec<T>` — cap the
  speculative pre-allocation at `remaining() / min_bytes_per_elem` while still
  returning a normal growable `Vec`, so legitimate large payloads (where the
  count really fits) pre-size to the honest count and streamed/chunked fields
  still append correctly as data arrives.

**Every** server-count-driven reservation in the protocol crate now routes
through one of these instead of trusting a raw wire count. The converted sites:

| Decoder family | File | Count field | Primitive |
|----------------|------|-------------|-----------|
| OSON field-name table | `oson.rs` | `num_short/long_field_names`, per-segment `num_fields` | `with_capacity_bounded` |
| OSON container | `oson.rs` | `num_children` (object/array) | `with_capacity_bounded` |
| VECTOR dense | `vector.rs` | `num_elements` (f32/f64/int8) | `with_capacity_bounded` |
| VECTOR sparse | `vector.rs` | `num_sparse` indices | `with_capacity_bounded` |
| Query implicit resultsets | `thin/fetch.rs` | `num_results` | `with_capacity_bounded` |
| Query column annotations | `thin/fetch.rs` | `num_annotations` | `with_capacity_bounded` |
| Out-bind array | `thin/fetch.rs` | array `num_elements` | `with_capacity_bounded` |
| DML RETURNING | `thin/fetch.rs` | `num_rows` | `with_capacity_bounded` |
| arraydmlrowcounts | `thin/fetch.rs` | `num_rows` | `with_capacity_bounded` |
| CQN notification | `thin/subscr.rs` | `num_tables` / `num_rows` / `num_queries` | `with_capacity_bounded` |
| Direct Path prepare | `dpl.rs` | `num_columns`, `out_values_length` | `with_capacity_bounded` |
| DbObject collection | `thin/dbobject.rs` | element count (`read_length`) | `remaining()` exposed for the caller's bound |

The query **column** count (`parse_describe_info`) and the DbObject **attribute**
loop never pre-size — they `push` into a `Vec` that grows as each record is read
— so the loop body's per-element bounds check is the only allocation path and is
already fail-closed; the crafted-input tests below lock that in.

**New decoders MUST use `alloc_count_checked` / `with_capacity_bounded`** for any
`Vec`/collection sized from a wire-supplied count. A raw `Vec::with_capacity(n)`
where `n` comes from the wire is the exact shape this audit greps for; route it
through `BoundedReader` instead.

**Crafted-input tests** (`<huge declared count, few actual bytes> -> clean Err,
not OOM/panic`) cover every count-driven family:
`vector::tests::sparse_oversized_index_count_fails_closed_not_oom`,
`vector::tests::fuzz_regression_oom_oversized_element_count`,
`oson::tests::fuzz_regression_oom_oversized_counts`,
`thin::fetch::fuzz_regression_tests::{describe_info_oversized_column_count_fails_closed_not_oom,
out_bind_array_oversized_element_count_fails_closed_not_oom,
fuzz_regression_implicit_resultset_oom}`,
`dpl::tests::direct_path_oversized_column_count_fails_closed_not_oom`,
`thin::subscr::tests::cqn_oversized_table_count_fails_closed_not_oom`,
`thin::dbobject::bounded_reader_tests::{dbobject_oversized_collection_count_is_bounded_by_remaining,
dbobject_legitimate_collection_count_passes}`,
plus the primitive's own
`wire::tests::{alloc_count_checked_errs_when_count_exceeds_remaining,
with_capacity_bounded_caps_preallocation_but_still_grows}`. Temporarily reverting
the out-bind bound to a raw `Vec::with_capacity(count)` makes the test attempt a
~19.8 GB allocation and abort, confirming the bound is load-bearing.

## Clean-run evidence

After the original four DoS fixes, each then-existing target was re-run for a
bounded 120 s libFuzzer session (`-max_total_time=120 -rss_limit_mb=2048
-timeout=10`, ASan + overflow-checks). This historical run covered the
first seven targets and had **zero surviving crashes**:

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

### `connect_string` (fuzz-harden lane, bead 5dd)

The structure-aware connect-string target was run for a bounded **180 s**
session (`-max_total_time=180 -rss_limit_mb=2048 -timeout=10`, ASan +
overflow-checks):

| Target | Executions | exec/s | Coverage (edges / features) | Crashes |
|--------|-----------:|-------:|----------------------------|:-------:|
| `connect_string` | 2,462,830 | ~13,600 | 2015 / 7964 | 0 |

```
Done 2462830 runs in 181 second(s)         # connect_string, 0 crashes
```

`fuzz/artifacts/connect_string/` is empty (no crash inputs were saved). The
2015 edges / 7964 features confirm the `Arbitrary` generator drives the
descriptor recursion, the EZConnect quote/host/port lexer, and the tnsnames
tokenizer — far past what raw-byte mutation reaches. ~13.6k exec/s is well above
the 1000 exec/s parser floor (the structured generator allocates `String`s, so
it is slower than the raw-`&[u8]` decode targets, by design).

Gate status alongside the fuzzing: `cargo fmt --check` clean,
`cargo clippy --workspace --no-deps -- -D warnings` clean,
`cargo test --workspace` green (460 tests passing). The fuzz crate is excluded
from the workspace and adds no dependencies to the `oracledb-protocol`
dependency tree.

## Differential fuzz oracle: rust decoder vs python-oracledb decoder (bead rcn)

No-panic fuzzing and example tests prove the decoder *fails closed*, but they
structurally miss **silent value divergence** — the decoder returns `Ok`, but
the *wrong value*. That is the exact bug class the project's 8 hand-found bugs
came from (an off-by-one in a date field, a sign/exponent slip in NUMBER, a
fractional-second truncation). A differential oracle is the highest-confidence
guard against it: feed the same input to two independent decoders and assert
they agree.

**What was built** (`harness/differential/diff_oracle.py`): a **container
round-trip differential**. For a proptest-style corpus of extreme / boundary
values (NUMBER and DATE/TIMESTAMP — the two highest-divergence-risk codecs), each
value is INSERTed once; the Oracle server is then the *encoder*, and the SAME
server wire bytes for each column are decoded by **both**:

* the reference python-oracledb thin decoder (`impl/base/decoders.pyx`), and
* rust-oracledb's decoder, surfaced through the `oracledb_pyshim` PyO3 module
  that swaps in for `oracledb.thin_impl` (the same shim the conformance harness
  uses).

The two engines run in **separate subprocesses** (the `oracledb.thin_impl` swap
is process-global, so one interpreter cannot host both decoders). Each fetches
the identical rows and emits canonical JSON; the driver asserts byte/semantic
identity. NUMBER is fetched through an output type handler returning the **exact
decimal string** (not a lossy `float`), so the full decoded digit/sign/exponent
sequence from `decode_number_value` is compared; DATE/TIMESTAMP is compared as
both the `datetime` (microsecond) and the canonical `TO_CHAR(...FF9)`
(nanosecond) rendering, so a fractional-second or civil-field carry bug surfaces.

**Fidelity (stated honestly).** A container round-trip differential is *weaker*
than a pure in-process decoder differential, for two reasons: (1) the server
produces the wire bytes, so only *well-formed* payloads the server actually
emits are exercised — an adversarial / hand-crafted wire image cannot be fed to
both decoders; and (2) both engines share one server, so a server-side quirk is
invisible. A pure decoder differential was investigated and is **impractical**:
python-oracledb's decoders are Cython `cdef` functions taking a C
`OracleDataBuffer*` (e.g. `decode_oracle_number`, `decode_date`), not callable
on raw bytes from Python without re-exporting the Cython layer. What the
round-trip differential *does* prove with high confidence: for every value
Oracle can store and emit, the rust and python-oracledb decoders recover the
**same Python value** — the parity property that actually matters. (The pure
*round-trip* property `decode(encode(x)) == x` for these same codecs is already
covered in-crate by `thin/proptests.rs`; this differential adds the
cross-*engine* dimension the round-trip cannot.)

**Run it:**

```bash
eval "$(ORACLEDB_CONTAINER_NAME=... ORACLEDB_HOST_PORT=... scripts/container.sh env)"
ORACLEDB_VENV_DIR=$PWD/.venv-py313 scripts/setup-python-env.sh   # one-time
# build the rust shim into the venv:
.venv-py313/bin/python -m maturin develop -m crates/oracledb-pyshim/Cargo.toml
.venv-py313/bin/python harness/differential/diff_oracle.py --cases 2000 --seed 0xc0ffee
```

**Result:** run over 3 seeds × 2000 generated cases, **5,944 cases compared**
(3,960 NUMBER + 2,984 DATE/TIMESTAMP after the server rejected ~3% of the most
extreme magnitudes — not decode cases), **0 divergences**. A negative-control
check confirms the comparator is not a tautology: injecting a one-digit NUMBER
change and a one-second timestamp shift is detected, while cosmetic numeric forms
(`1` vs `1.0` vs `1.00`) correctly compare equal.
