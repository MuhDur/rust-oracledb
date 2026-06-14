# perf-map.md — rust-oracledb perf-push STEP 1 attribution

Profiling-only attribution of the five canonical driver operations into
**server-round-trip (unbeatable)** vs **client-CPU (decode/encode/alloc/runtime
block_on/locking — beatable)** on a stable warm base. This map decides where
STEP 2 (columnar-Arrow) and STEP 3 (micro-opt) spend effort.

HONEST FRAMING (preserved): single small ops are server/network-bound — we
cannot beat the server. The beatable wins live in **decode-heavy fetches** and
the **client-CPU slice** (per-call dispatch, residual allocations). On loopback
the socket read-wait is the *small* RTT; on real network the read term grows and
the decode/alloc share of the *client's own* time is unchanged, so a client-CPU
win is strictly additive to (not masked by) the larger RTT off-loopback.

## Fingerprint

- CPU: AMD EPYC 7713 64-Core (128 threads), governor schedutil (shared host —
  expect run-to-run variance on the cheap ops; medians reported)
- Kernel: 6.17.0-35-generic
- rustc: 1.97.0-nightly (64a965e90 2026-05-11), release (opt-level 3)
- DB: Oracle Free 23ai in container, loopback localhost:1523/FREEPDB1
- git base SHA: f1b2a64 (branch perf-push, master base)
- Harness: `crates/oracledb/examples/perf_attribution_map.rs` (warm connection,
  statement cache hot) + `crates/oracledb/examples/profile_fetch_attribution.rs`
  (read/decode split via the crate's `fetch_profile_*` counters).

## Measured attribution (warm connection, loopback)

| Op | wall | server round-trip (unbeatable) | client-CPU (beatable) | beatable slice |
|----|------|-------------------------------|-----------------------|----------------|
| (1) select_one_row | ~145 us/call | ~all (one round trip) | execute-encode + 1-row decode (sub-us) | tiny — RT-bound floor; can only shave per-call dispatch |
| (2) fetch_10k_rows (1 NUMBER col, ~10 pages) | 5.36 ms | 80.4% read-wait | **19.6% decode-CPU** + per-row Vec alloc | decode + row Vec |
| (3) fetch_wide_analytics (20k rows x 10 typed cols) | 59.5 ms | 72.7% read-wait | **27.3% decode-CPU** + per-row Vec + transpose-on-build | **columnar-Arrow target** |
| (4) executemany_1000 (array DML, 1 RT) | 1.03 ms/call | RT + server array-DML exec | bind-encode of 1000 rows | small — RT/server-bound |

Multi-page wide fetch (50k rows x 1 col, `profile_fetch_attribution`, separate run):
read-wait 75.6% / decode-CPU 24.4%; one-page prefetch (already shipped, bead xad)
hides ~31% of read-wait per page and ~18.6% wall when the consumer does per-row work.

## Hotspot table (the hand-off artifact)

| Rank | Location | Metric | Value | Category | Evidence |
|------|----------|--------|-------|----------|----------|
| 1 | wide-fetch row materialization + Arrow transpose (`fetch_all_record_batch` -> `Vec<Vec<Option<QueryValue>>>` -> `build_record_batch`) | client-CPU share of a 10-col analytics fetch | 27.3% decode-CPU, **2N+ allocs/row** (per-row Vec + String per text + transpose pass) | CPU/alloc | attribution_map.txt (3); borrowed_alloc_count.rs |
| 2 | per-page row decode-CPU (`parse_fetch_response_with_context`) | decode share | 19.6–24.4% of read+decode | CPU | profile_fetch_attribution; attribution_map.txt (2) |
| 3 | `BlockingConnection::*` per-call dispatch (`build_io_runtime` TLS borrow+clone, `runtime.block_on`, `Cx::current()` per call) | per-call overhead on the synchronous facade the PyO3 shim drives | the select-1 client-CPU floor (sub-us of the 145us, but it is ALL the beatable part) | CPU | lib.rs:4236+ (every Blocking fn rebuilds the block_on closure) |
| 4 | socket read-wait | per-page wire round trip | 72–80% of fetch wall on loopback (grows with RTT off-loopback) | I/O | UNBEATABLE (server/network) |

## Interpretation / hypothesis ledger

- **"the columnar-Arrow path is where drastic is real"** : SUPPORTS — the wide
  analytics fetch spends 27.3% in client decode-CPU, and on TOP of that the row
  path allocates a `Vec<Option<QueryValue>>` per row plus a `String` per text
  cell plus a whole transpose pass in `build_record_batch`. The borrowed-fetch
  substrate already proves the per-cell allocations are removable; building Arrow
  column builders straight from the borrowed batch removes BOTH the row Vec AND
  the transpose, and NUMBER -> Decimal128 goes straight from `OracleNumber`'s
  i128 coefficient+scale with no String. This is STEP 2 (bead wf7).

- **"select-1 is where we close the python gap"** : PARTIALLY REJECTS — at
  ~145 us/call it is dominated by the one server round trip; the README's
  123 us vs python 80 us gap is mostly the round trip, not client CPU. The only
  beatable part is per-call dispatch on `BlockingConnection`. Worth a
  behavior-preserving micro-opt (STEP 3) but the honest ceiling is small: we
  cannot beat the server's round trip. Do NOT claim drastic here.

- **"fetch read-wait is the bottleneck so decode doesn't matter"** : REJECTS for
  the client-CPU goal — read-wait is unbeatable (it IS the server), but the
  decode+alloc is the client's OWN cost and is fully removable; off-loopback the
  read term grows (RTT-dominated) so a smaller client-CPU footprint is strictly
  additive, never masked.

- **VECTOR borrowed-&[f32] claim** : REJECTS (honesty gate for bead 0mk) — Oracle
  VECTOR FLOAT32/64 elements are NOT raw IEEE-754 on the wire; each element is
  Oracle's sortable BINARY_DOUBLE/FLOAT encoding (sign-bit transform, then
  big-endian bits). A borrowed `&[f32]` over the wire bytes would be WRONG. The
  columnar VECTOR path must decode element-by-element into a fresh contiguous
  buffer (still one alloc per cell-vector, not per element). Verified in
  `crates/oracledb-protocol/src/vector.rs::decode_binary_float/double`.

## Decision

STEP 2 = columnar-Arrow (bead wf7): decode the borrowed fetch batch DIRECTLY
into per-column Arrow builders, skipping `Vec<Vec<Option<QueryValue>>>` row
materialization and the `build_record_batch` transpose. NUMBER -> Decimal128 from
the inline i128 coefficient+scale; VARCHAR/RAW -> offsets+bytes; NULL ->
NullBuffer; DATE/TIMESTAMP -> Arrow temporal. Gate behind `arrow`. Differential
test: columnar RecordBatch == row-path `build_record_batch` cell-for-cell.
Measure alloc + time win on a wide analytics fetch (counting allocator).

STEP 3 = micro-opt the #1 client-CPU finding that is NOT the columnar path: the
`BlockingConnection` per-call dispatch (the select-1 / synchronous-facade hot
path the PyO3 shim drives). Profile-guided, behavior-preserving. Honest ceiling:
the round trip dominates, so the claim is "shave the per-call client overhead",
not "beat the server".
