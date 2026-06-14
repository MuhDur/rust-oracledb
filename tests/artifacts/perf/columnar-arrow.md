# columnar-arrow.md — STEP 2 measured win (bead rust-oracledb-wf7)

Columnar fetch->Arrow: decode the borrowed fetch batch DIRECTLY into per-column
Arrow builders (transpose-during-parse), skipping the
`Vec<Vec<Option<QueryValue>>>` row materialization AND the `build_record_batch`
transpose pass.

## What shipped

- `oracledb::arrow::build_record_batch_columnar(schema, columns, &BorrowedRowBatch)`
  — one borrowed batch -> one `RecordBatch`, streaming each cell into the column
  builder.
- `Connection::fetch_all_record_batch_columnar(sql, fetch_array_size, options)`
  / `BlockingConnection::fetch_all_record_batch_columnar(...)` — the columnar
  `fetch_df_all`: execute (first owned page) + page the rest borrowed, all
  streamed into one set of accumulating builders, finished once.
- Scalar columnar coverage: NUMBER -> Int64 / Float64 / Decimal128, VARCHAR2 /
  CHAR / LONG -> Utf8 / LargeUtf8, RAW / LONG_RAW -> Binary / LargeBinary /
  FixedSizeBinary, BOOLEAN -> Boolean, DATE / TIMESTAMP(/TZ/LTZ) -> Arrow
  Timestamp(unit) / Date32 / Date64, NULL -> NullBuffer. VECTOR (List/Struct)
  columns transparently fall back to the row path (bead 0mk noted below).
- Gated behind the existing `arrow` feature. `#![forbid(unsafe_code)]` preserved
  (no `unsafe`; the borrowed batch's two-pass arena design carries the lifetime).

## Correctness gate (byte-identical to the row path)

`crates/oracledb/tests/arrow_columnar_diff.rs`:
- SYNTHETIC (offline, no container): owned decode + `build_record_batch` vs
  borrowed decode + `build_record_batch_columnar` over the SAME wire frame of
  NUMBER(int) / NUMBER(2dp) / VARCHAR2 / RAW with scheduled NULLs, asserting
  `RecordBatch ==` cell-for-cell. Covered for default AND `fetch_decimals`
  (Decimal128) options, plus the empty-result case. ALL PASS.
- LIVE (container): the SAME 12,000-row mixed-type query (NUMBER int, NUMBER
  decimal(18,4), VARCHAR2, DATE, NULLs, small int) through BOTH real fetch paths
  (`fetch_all_record_batch` row path vs `fetch_all_record_batch_columnar`),
  asserting the two `RecordBatch`es are equal — the end-to-end wire-decode gate.
  Covered for default AND `fetch_decimals`. ALL PASS (against rust-oracledb-lane-1523).

## Measured allocation + time win

`crates/oracledb/tests/arrow_columnar_alloc.rs` (counting allocator), wide
analytics frame: 5000 rows x 10 typed columns (NUMBER int x4, NUMBER decimal x2,
VARCHAR2 x4), NULLs every 5th row. Both paths START from the SAME wire frame and
END at a `RecordBatch`, so this is the full client-side wire->Arrow cost.

| path | allocs | allocs/row | bytes | decode+build (release) |
|------|--------|-----------|-------|------------------------|
| row (owned rows + `build_record_batch` transpose) | 109,961 | 21.99 | 4,394,939 | 5.85 ms/batch |
| columnar (borrowed + direct-to-builder) | 5,163 | **1.03** | 3,198,472 | 4.29 ms/batch |

- **Allocation reduction: 95.3%** (21.99 -> 1.03 allocs/row). The columnar path's
  remaining allocations are the Arrow value buffers + amortized arena growth; the
  per-row `Vec<Option<QueryValue>>`, the per-text-cell `String`, and the whole
  transpose pass are gone.
- Bytes reduction: 27.2%.
- Time: 5.85 -> 4.29 ms/batch (release, ~27% faster decode+build). Informational
  (host-load sensitive); the headline metric is the allocation count.
- The CI floor asserts the columnar path cuts allocations at least 3x.

### End-to-end live wall (criterion, `benches/thin_driver.rs::oracledb_columnar`)

Full live `fetch_df_all` of a 20,000-row x 6-typed-column analytics result over
loopback, row path vs columnar, each on its own warm connection:

| arm | median wall |
|-----|-------------|
| `fetch_df_row_path` | 45.55 ms |
| `fetch_df_columnar` | 42.79 ms |

~6% end-to-end wall improvement on loopback — bounded, as the STEP 1 map
predicts, by the client decode/build share (the ~73% socket read-wait is
unbeatable). Off-loopback the read term grows with RTT so the wall delta shrinks
as a fraction, while the 95.3% allocation reduction holds regardless. This is the
honest end-to-end number; the drastic, build-independent win is the allocation
count.

### Latent bug fixed in passing

The row path `Connection::fetch_all_record_batch` (and the new columnar method)
now `release_cursor` the fully-drained cursor. Previously a repeated `fetch_df_all`
parsed and never released a copy cursor each call, leaking one server cursor per
call until ORA-01000. Verified by the `leak_probe` tests (250 calls each, no
leak). The shim's `fetch_df_all` uses its own cursor management and is unaffected
(parity unchanged).

## Honest framing

This is the CLIENT-CPU slice. From the STEP 1 map, a wide analytics fetch is
~73% socket read-wait (server/network, UNBEATABLE) and ~27% client decode-CPU.
The columnar win lands squarely in that ~27% decode/alloc slice: it does not (and
cannot) shrink the server round trip. On loopback the read-wait dominates wall
time, so the END-TO-END wall improvement of a full live fetch is bounded by the
decode share; off-loopback the read term grows with RTT and the smaller
client-CPU footprint is strictly additive. The drastic, honest number here is the
**95.3% allocation reduction** and the ~27% faster pure decode+build, NOT a
"drastically faster fetch" claim on a round-trip-bound op.

## Scope note (bead 0mk — VECTOR columnar)

VECTOR FLOAT32/FLOAT64 elements are NOT raw IEEE-754 on the Oracle wire: each is
Oracle's sortable BINARY_DOUBLE/FLOAT encoding (sign-bit transform, then
big-endian bits — see `crates/oracledb-protocol/src/vector.rs::decode_binary_*`).
A borrowed `&[f32]` over the wire bytes would therefore be WRONG; a correct
columnar VECTOR path must decode element-by-element into a fresh contiguous
buffer (one alloc per cell-vector, not per element) before handing it to an Arrow
FixedSizeList. Per "quality over breadth", VECTOR is left on the fully-tested row
path (the columnar entry falls back transparently for List/Struct columns) and
bead 0mk stays OPEN with this verified-endianness finding recorded. The scalar
columnar path (NUMBER/VARCHAR/RAW/BOOLEAN/DATE/TIMESTAMP/NULL) is shipped solidly
and tested — that is the flagship win.

## Shim wiring note

The PyO3 shim's `fetch_df_all` keeps the row path for now: it additionally
handles CLOB/BLOB locator inlining, deferred define-fetch, output type handlers,
and `requested_schema` coercion, which the scalar columnar path does not yet
replicate. Rewiring it safely is a larger task and would risk the
`test_8000_dataframe` parity sentinel (82p), so the columnar path ships as a
tested, measured public crate API a Rust consumer (or a future careful shim
wiring) can adopt. Parity stays EXACT.
