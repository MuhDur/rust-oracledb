# micro-opt.md — STEP 3 client-CPU micro-optimization (select-1 hot path)

Target chosen from the STEP 1 map: the #1 client-CPU finding that is NOT the
columnar path — the per-call client work on the `BlockingConnection` execute hot
path (the synchronous facade the PyO3 shim drives), profiled via `select 1 from
dual`. Profile-guided, behavior-preserving, one lever per commit.

## Honest framing (preserved)

`select 1 from dual` is ROUND-TRIP-BOUND: STEP 1 measured ~120-145 us/call, of
which ~all is the one server round trip we cannot beat (loopback; the gap to
python-oracledb's ~80 us is mostly the round trip, not client CPU). So the
optimizable surface is the per-call CLIENT allocations/CPU — every malloc here is
pure client work the server never sees. The claim is "shave the per-call client
overhead", NOT "beat the server's round trip".

## Baseline (counting allocator, warm `BlockingConnection`, `select 1 from dual`)

`crates/oracledb/examples/profile_select1_client_cpu.rs`:
- per-call wall p50: ~120-150 us (host-load sensitive; the round trip dominates)
- per-call allocations: **33 allocs / 2499 bytes** <- the beatable client work

## Lever 1 — preallocate TTC payload writers (execute + fetch)

`TtcWriter::new()` starts at zero capacity, so a payload built from many small
`write_*` pushes grows the backing `Vec` through several doublings, each a heap
allocation. Added `TtcWriter::with_capacity` and sized the execute writer to
`96 + sql_len` and the fixed fetch writer to 32.

- `crates/oracledb-protocol/tests/execute_payload_alloc.rs`: building one 87-byte
  `select 1 from dual` EXECUTE payload: **5 allocs -> 1 alloc** (248 -> 114 bytes).
- End-to-end warm select-1 client work: **33 -> 29 allocations/call**.

Isomorphism proof:
- Ordering preserved: yes — identical `write_*` sequence, only the buffer is
  presized.
- Bytes identical: yes — the produced `Vec<u8>` is byte-for-byte the same. All
  246 protocol wire-correctness tests pass unchanged.
- Floating-point / RNG: N/A.

## Lever 2 — skip redundant cursor-columns clone on a statement-cache hit

`remember_cursor_columns` cloned `result.columns` (the `Vec<ColumnMetadata>` plus
each column's `name`/object/domain `String`s) into the `cursor_columns` map on
EVERY execute. On a cache hit the same cursor re-executes with identical columns,
so the map already holds an equal value and the clone is pure waste. Guarded with
a cheap field-equality check; clone only when the cached value differs.

- End-to-end warm select-1 client work: **29 -> 27 allocations/call**.

Isomorphism proof:
- Map content unchanged: yes — when the cached value equals the new columns we
  skip the re-insert of an identical value; when it differs (re-describe /
  type-change path) we insert as before. The map's final state is identical.
- Behavior preserving: yes — full `oracledb` (25 test binaries) + `arrow` +
  `protocol` (246) suites pass unchanged.
- Floating-point / RNG: N/A.

## Cumulative result

| metric | before | after | delta |
|--------|--------|-------|-------|
| select-1 client allocations/call | 33 | 27 | **-18%** |
| execute payload build allocations | 5 | 1 | -80% |

The per-call WALL is dominated by the unbeatable server round trip, so it stays
within run-to-run noise (~120-150 us depending on host load); the build-
independent, reliable win is the **18% fewer client allocations per call** on the
hot synchronous-facade path the PyO3 shim drives.

## Not pursued (honest scope)

The remaining ~27 allocs/call are structural: the `QueryResult` Vecs (rows,
columns), the decoded row `Vec` + cell, the per-packet read payload, and the
response accumulator. Cutting those needs a pooled/borrowed `QueryResult` or a
reused per-connection scratch buffer threaded through the read path — a larger,
riskier change that risks the shim's parity sentinels for a sub-round-trip gain
on a round-trip-bound op. Per "quality over breadth" and the round-trip-bound
honesty, STEP 3 ships the two clean, proven, byte-identical levers and stops.
