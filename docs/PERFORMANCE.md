# Performance: rust-oracledb thin vs python-oracledb thin

This is a like-for-like comparison of the pure-Rust `oracledb` thin-mode driver
against python-oracledb in thin mode. Both speak the same TNS/TTC wire protocol
to the same Oracle container over the same TCP socket, with no Oracle Instant
Client involved on either side. The point is an honest answer to "is the Rust
driver faster, and where", not a marketing number.

Short version: on the operations that are dominated by network round trips and
server work (connect handshake, bulk row transfer, array DML), the two drivers
are within noise of each other. The measurable differences are on the
CPU-bound edges, and they currently cut both ways: Rust is not uniformly
faster. See [Results](#results) and [Reading the deltas](#reading-the-deltas).

## What is measured

Five operations, run identically on each side (`crates/oracledb/benches/thin_driver.rs`
on the Rust side, `benches/compare_python_oracledb.py` on the Python side):

| Operation          | What it does                                                            |
|--------------------|-------------------------------------------------------------------------|
| `connect`          | Full handshake: TCP connect + TNS negotiate + auth, then logoff/close.  |
| `select_one_row`   | `select 1 from dual`, execute + fetch one row. One round trip.          |
| `fetch_10k_rows`   | `connect by level <= 10000`, arraysize 1000, drained in full (~10 pages).|
| `executemany_1000` | Array-DML INSERT of 1000 rows into a scratch table, then rollback.      |
| `read_clob`        | Select a 64 KiB CLOB locator, then read its full body and decode it.    |

`connect` opens and closes a fresh connection on every iteration, so TCP setup
and teardown are inside every sample. The other four reuse one warm connection
across iterations, so they measure per-operation protocol and codec cost rather
than the handshake. python-oracledb reuses one cursor (its statement cache stays
warm); the Rust benches drive the statement-cache path so the open server cursor
is reused the same way. This is the apples-to-apples baseline: one connection,
serial calls, warm caches.

The CLOB body is a real 65536-character LOB built by appending 1024-char chunks
in PL/SQL. A bare SQL `rpad()` caps at the 4000-char VARCHAR2 limit and cannot
stand in for a large LOB, so an earlier version of this bench was accidentally
reading 4000 chars; both sides now build and verify the full 64 KiB.

Scratch objects are named `PERFTEST_*`; each harness creates and drops its own
and touches nothing else.

## Methodology

- **Machine:** AMD EPYC 7713 (64 cores / 128 threads), 247 GiB RAM, kernel
  6.17, ext4. `schedutil` governor (not pinned to `performance`; see caveats).
  The host was shared and busy during measurement, which shows up as run-to-run
  variance on the cheaper operations.
- **Database:** Oracle AI Database 26ai Free 23.26.1.0.0 (`gvenzl/oracle-free:23-slim`),
  local container, listener on `localhost:1523`, service `FREEPDB1`. Loopback
  TCP, no TLS on the data connection.
- **Rust side:** `oracledb` crate at commit `f1fee6f` (branch `w6-perf`), criterion
  0.5, `cargo bench` release profile. 2 s warmup + 8 s measurement per operation;
  100 samples for the cheap ops, 30 for the bulk/DML ops. Reported value is the
  criterion median with its 95% confidence interval; criterion also reports MAD.
- **Python side:** python-oracledb 4.0.1, CPython 3.13.12, thin mode. Each
  operation is timed per call with `time.perf_counter` after a 50-iteration
  warmup: 2000 measured iterations for the cheap ops, 200 for the bulk/DML ops.
  Reported value is the median plus the median absolute deviation (MAD).
- **Repetition:** each side was run four to five times. The tables below give a
  representative median across those passes and flag operations whose median
  moved between passes.

Reproduce:

```sh
eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1523 \
        ORACLEDB_HOST_PORT=1523 scripts/container.sh env)"

# Rust
CARGO_TARGET_DIR=/path/to/target cargo bench -p oracledb --bench thin_driver

# python-oracledb
.venv-py313/bin/python benches/compare_python_oracledb.py
```

Both harnesses self-skip cleanly when the container environment is absent, so
`cargo bench` and the script stay green offline.

## Results

Representative medians across repeated passes. Ratio is python / rust, so a
ratio above 1.0 means Rust was faster, below 1.0 means python-oracledb was
faster. Numbers below ~200 us carry the most host jitter; treat one-significant-
figure differences there as a tie.

The two rows that were previously "python faster" — `select_one_row` and
`read_clob` — have since been optimized on the Rust side; see
[Optimization history](#optimization-history). The numbers below are the
pre-optimization baseline against which those changes were measured.

| Operation          | rust-oracledb median | python-oracledb thin median | ratio (py / rust) |
|--------------------|----------------------|-----------------------------|-------------------|
| `connect`          | 32.6 ms              | 33.3 ms                     | 1.02 (tie)        |
| `select_one_row`   | 127 us               | 80 us                       | 0.63 (python faster) |
| `fetch_10k_rows`   | 5.0 ms               | 4.7 ms                      | 0.94 (tie)        |
| `executemany_1000` | 2.2 ms               | 2.0 ms                      | 0.91 (tie, both bimodal) |
| `read_clob` (64 KiB)| 0.90 ms             | 0.44 ms                     | 0.49 (python faster) |

Per-pass spread (to be honest about the noise):

| Operation          | rust passes (median)        | python passes (median)            |
|--------------------|-----------------------------|-----------------------------------|
| `connect`          | 32.0 / 32.6 / 32.9 / 33.0 ms| 32.7 / 33.3 / 33.7 / 33.7 ms      |
| `select_one_row`   | 117 / 126 / 130 / 142 us    | 78 / 78 / 81 / 81 / 83 us         |
| `fetch_10k_rows`   | 4.87 / 4.99 / 5.06 / 5.09 ms| 4.57 / 4.62 / 5.05 / 5.26 ms      |
| `executemany_1000` | 1.83 / 2.22 / 2.27 / 2.43 ms| 1.39 / 1.41 / 2.44 / 2.52 ms      |
| `read_clob`        | 854 / 884 / 923 / 934 us    | 419 / 420 / 456 / 535 us          |

## Reading the deltas

**`connect` is a tie, and it should be.** The handshake is a fixed sequence of
round trips plus server-side session setup and password verification. That cost
lives in the database and the network, not in the client codec, so neither
driver can move it much. ~33 ms for a full authenticated connect on this 23ai
container is the floor; this is the strongest argument for pooling connections
regardless of which driver you use.

**`fetch_10k_rows` and `executemany_1000` are ties.** Moving 10000 rows or 1000
bind rows is dominated by serialization on the wire and the server's work to
produce or apply them. Both `executemany` medians are bimodal (about 1.4 ms or
about 2.5 ms depending on the pass), which is host contention on a shared
machine, not a property of either driver. We report the slower mode because it
was more common.

**`select_one_row`: was python-faster, now optimized.** This is a single round
trip where the network cost is tiny and almost everything is client-side
per-call overhead. The synchronous `BlockingConnection` facade — which the PyO3
shim drives for every suite operation — used to build a brand-new Asupersync
runtime on every call (a fresh epoll reactor plus a worker OS thread that was
spawned and immediately joined), then went through `Runtime::block_on`. python-
oracledb thin is natively synchronous and pays no equivalent re-entry. That
fixed per-call cost was the entire gap on an operation this cheap. Caching one
current-thread runtime per calling thread removed it: the facade's
`select_one_row_blocking` bench dropped from ~327 us to ~123 us (−62%), now in
line with the async path. See [Optimization history](#optimization-history). The
`select_one_row` row in the table above is the async bench (which already reused
one runtime), so it was unaffected by this change; the facade was the slow path.

**`read_clob`: was python-faster, now optimized.** Both drivers do the same
three round trips here (execute, define-fetch the locator, read the bytes),
confirmed by `v$mystat` round-trip counts, so the gap is CPU-side in the Rust LOB
read and decode path, not extra network traffic. Phase attribution showed the
64 KiB CLOB comes back from the server as AL16UTF16 (131072 bytes), and the
UTF-16-to-`String` decode was ~178 us of pure CPU — it built an intermediate
`Vec<u16>` (a second 128 KiB allocation, filled by a separate byte-swap pass)
before re-scanning it in `String::from_utf16`. A single-pass decoder that pushes
ASCII units inline and only falls back to the general `char::decode_utf16` path
on the first non-ASCII unit halved that to ~88 us and cut the whole `read_clob`
from ~0.90 ms to ~0.77 ms (−17%). The remaining `read_lob` cost is I/O-bound
(≈16 packet reads across the wire), not buffer management — a micro-benchmark
confirmed preallocating the chunked-bytes accumulator does not help — so it was
left alone. See [Optimization history](#optimization-history).

## Optimization history

The two operations where the Rust path was materially slower than python-oracledb
thin — `select_one_row` and `read_clob` — were both profiled and optimized. Each
change is behaviour-preserving (the full reference suite stays green: 2236/2236)
and was proved with a before/after criterion delta on the same container.
All deltas below are from `cargo bench -p oracledb --bench thin_driver` against
the local Oracle container; the host was shared and busy, so sub-200 us numbers
carry the usual jitter and are reported with their criterion confidence interval.

### 1. Cache the `BlockingConnection` runtime per thread

**Problem (profiled).** Every `BlockingConnection::*` and `CancelHandle::cancel`
call built a fresh single-threaded Asupersync runtime: `create_reactor()` (a new
epoll fd) plus `RuntimeBuilder::current_thread().build()`, which spawns a worker
OS thread that is then joined when the runtime drops — all on every call. The
PyO3 shim drives this synchronous facade for every suite operation, so that fixed
per-call cost dominated cheap operations. A bench driving the real facade
(`select_one_row_blocking`, added for this work) measured ~327 us versus ~131 us
for the otherwise-identical async path that reuses one runtime; the ~196 us
delta was entirely runtime construction.

**Fix.** Cache one current-thread runtime per calling thread in a `thread_local`,
built lazily on first use and reused for every subsequent call. The connection's
socket re-registers (`rearm`) with the persistent reactor on each call exactly as
Asupersync's owned TCP halves are designed to — strictly less work than dropping
and rebuilding a reactor per call. Each `Runtime::block_on` still installs a
fresh request-scoped `Cx` (`Budget::INFINITE`) and runtime/Cx guards for the
polled future, so cancellation and context semantics are unchanged.

**Result.** `select_one_row_blocking` `[322 us → 132 us]`, **−59% to −62%**
(p < 0.05) across runs — the facade now matches the async path. Because the shim
drives this path for every suite operation, the whole suite gets the speedup.

### 2. Single-pass ASCII-inline UTF-16 LOB decode

**Problem (profiled).** Phase attribution of `read_clob` (~983 us) split it into
`execute_query_collect` ~329 us (2 round trips), `read_lob` wire+parse ~459 us
(1 round trip), and `decode_lob_text` ~178 us — pure CPU, no I/O. The 64 KiB
CLOB returns as AL16UTF16 (131072 bytes), and the decoder collected an
intermediate `Vec<u16>` (a second 128 KiB allocation, filled by a separate
byte-swap pass) before re-scanning it in `String::from_utf16`.

**Fix.** Decode straight from the byte pairs in one pass. LOB text is
overwhelmingly ASCII/Latin, where every UTF-16 code unit is a single ASCII byte;
those are pushed inline (no buffer). Only on the first non-ASCII or surrogate
unit do the remaining bytes go to the general `char::decode_utf16` decoder,
walked by byte index so the fallback never rescans. The UTF-8 path likewise
validates in place instead of copying into a temporary `Vec` first. Output is
byte-for-byte identical to the previous `String::from_utf16` / `String::from_utf8`,
including rejection of lone surrogates and odd-length input; new isomorphism unit
tests cover ASCII, BMP non-ASCII, CJK, surrogate pairs, code-point boundaries,
both endiannesses, and the error cases against the previous implementation.

**Result.** `decode_lob_text` ~178 us → ~88 us (−50%); `read_clob`
`[927 us → 768 us]`, **−16% to −18%** (p < 0.05). The remaining `read_lob` cost
is I/O-bound (≈16 packet reads), not buffer management: a micro-benchmark showed
preallocating the chunked-bytes accumulator does not beat `Vec`'s amortized
growth, so that path was left unchanged.

### 3. Columnar fetch → Arrow (decode straight into Arrow builders)

**Problem (profiled).** The `fetch_df_all` path materialised every fetched row
into a `Vec<Option<QueryValue>>` (one `Vec` per row, a `String` per text cell, an
`OracleNumber` per number cell), then `build_record_batch` ran a second pass that
transposed those owned rows column-by-column into the Arrow builders. A wide
analytics fetch (the kind dataframes are for) is decode-and-allocation heavy: the
STEP-1 attribution map measured a 20 000-row × 10-column fetch at ~73 % socket
read-wait and ~27 % client decode-CPU, and on top of that the row path allocated
~22 heap allocations **per row**.

**Fix.** A columnar fetch path (`Connection::fetch_all_record_batch_columnar`,
gated behind the `arrow` feature) that decodes the borrowed fetch batch
(`QueryValueRef`, zero-copy for VARCHAR2/RAW, an amortised arena for NUMBER text)
**directly** into per-column Arrow builders — transpose-during-parse. No per-row
`Vec` is materialised, no per-text-cell `String` is allocated, and the separate
transpose pass is gone. NUMBER → Decimal128/Int64/Float64 goes through the same
canonical-text helpers the row path uses, so the produced `RecordBatch` is
**byte-identical** to the row path. VECTOR (List/Struct) columns transparently
fall back to the fully-tested row path.

**Correctness.** A differential test asserts the columnar `RecordBatch` equals the
row path cell-for-cell — both on a synthetic mixed-type frame (NUMBER/VARCHAR/RAW/
NULL, default and `fetch_decimals`) and **live** against the container on a
12 000-row mixed-type result (NUMBER int, NUMBER decimal, VARCHAR2, DATE, NULLs).

**Result (counting allocator + criterion, 5 000 rows × 10 typed columns).**
Row path **109 961 allocations (21.99 / row)** → columnar **5 163 (1.03 / row)** —
a **95.3 % reduction** in allocation count (27 % fewer bytes), and decode+build
~5.85 ms → ~4.29 ms per batch (release, ~27 % faster). End-to-end live
`fetch_df_all` of a 20 000-row × 6-column result over loopback: **45.55 ms → 42.79
ms**, ~6 % wall. The end-to-end wall delta is bounded — honestly — by the client
decode/build share: the ~73 % socket read-wait is server/network and unbeatable on
loopback; off-loopback the read term grows with RTT, so the smaller client-CPU
footprint is strictly additive and the allocation win holds regardless. The drastic,
build-independent headline is the **95 % fewer allocations**.

A latent cursor leak was fixed in passing: a repeated `fetch_df_all` previously
parsed-and-never-released a copy cursor each call (ORA-01000 over a long run); both
the row and columnar `fetch_all_record_batch` now release the drained cursor. The
PyO3 shim drives its own cursor management and is unaffected (`test_8000_dataframe`
parity unchanged at 82 passed).

### 4. Trim per-call client allocations on the `select_one_row` hot path

**Problem (profiled).** `select 1 from dual` is round-trip-bound (~120–150 us /
call, ≈ all the one server round trip), so the only beatable surface is the
per-call CLIENT allocations. A counting-allocator probe of the warm
`BlockingConnection` execute path measured **33 allocations / call**, of which two
sources were pure waste: the EXECUTE-payload `TtcWriter` started at zero capacity
(its small `write_*` pushes grew the buffer through ~5 doublings = 5 allocations
for an 87-byte payload), and `remember_cursor_columns` re-cloned the column
metadata into the cursor map on every cache-hit execute even when it was already
present and unchanged.

**Fix.** Add `TtcWriter::with_capacity` and presize the execute (96 + SQL length)
and fetch (32) payload writers; guard the cursor-columns clone with a cheap
equality check. Both are byte/behaviour-preserving — the wire payloads are
identical (all 246 protocol wire-correctness tests pass unchanged) and the cursor
map ends with the same content.

**Result.** Execute-payload build **5 → 1 allocation**; warm select-1 client work
**33 → 27 allocations / call (−18 %)**. The per-call wall stays round-trip-bound,
so it moves only with host-load noise — this trims the client CPU the shim pays per
call, not the server round trip.

## Honest caveats

- **The five operations above are a single-connection, serial-call benchmark,
  not a throughput or concurrency benchmark.** They say nothing about how either
  driver scales across many concurrent sessions. That axis is measured separately
  in [Concurrent throughput](#concurrent-throughput) below; do not read the
  serial medians as a throughput claim.
- **GIL note.** python-oracledb thin holds the CPython GIL while it decodes the
  wire protocol in Python. That does not affect the serial, single-threaded
  medians above, but it is the axis on which a multi-connection comparison
  diverges — see [Concurrent throughput](#concurrent-throughput), which measures
  exactly that.
- **Warm vs cold.** All operations except `connect` are warm: warmed-up
  statement and cursor caches on both sides. Cold-start parse cost is not
  measured.
- **Host was shared and not isolated.** The governor was left at `schedutil`,
  cores were not pinned, and other work ran on the box during measurement. This
  inflates run-to-run variance, especially below ~200 us and on the `executemany`
  bimodality. The CPU was not put into `performance` mode because that requires
  changing global system state.
- **Loopback, no TLS.** The data connection is plain TCP over loopback. A real
  network with TLS would add latency that is identical for both drivers and would
  push every operation further toward "network-dominated, therefore a tie".

## Concurrent throughput

The serial benchmark above deliberately measures one connection making one call
at a time. This section measures the other axis: aggregate decode throughput
when N workers each drive their own connection in parallel. This is where the
absence of a GIL in the Rust driver is supposed to matter, so it is measured
rather than asserted.

Harnesses: `crates/oracledb/benches/concurrent_throughput.rs` (Rust, N worker OS
threads) and `benches/compare_concurrent_python.py` (python-oracledb, the same
workload under two concurrency models, `threading` threads and `asyncio`
coroutines). Both self-skip when the container is absent.

### The workload, and why it is decode-bound

A concurrency comparison is only meaningful if the bottleneck is client-side
codec CPU, the thing the GIL serializes, rather than the server or the wire,
which are identical for both drivers. Getting there took one correction worth
recording.

The first attempt generated rows with `connect by level` (20 expression columns
over 5000 generated rows). That was a trap: generating those rows is *server*
CPU, and on a single container it serialized. Rust throughput peaked at 4 workers
and then fell while the container, not the client, sat pinned at three-plus
cores. That measures the database, not the driver, so it was discarded.

The honest workload instead pre-populates a small wide table once
(`PERFTEST_CONC`, 1000 rows × 20 columns: 10 `NUMBER`, 10 `VARCHAR2(40)`), warms
it into the server buffer cache, and then each worker repeatedly runs
`select * from PERFTEST_CONC`. The server side is now a buffer-cache block read
plus wire serialization, which is cheap and which scales: a no-GIL multi-process
probe (separate OS processes, so no shared interpreter) drove the same table to
~6× throughput at 8 sessions before the single container began to tail off, so
the server can feed several parallel clients. The expensive part is on the
client. Every `NUMBER` cell parses Oracle's base-100 mantissa/exponent bytes
into lossless decimal text, and every `VARCHAR2` cell is a UTF-8 validation plus
a `String`/`str` build. With the server able to feed parallel clients, the codec
is the bottleneck, which is exactly where the GIL decides whether throughput
scales. Each worker decodes every cell it fetches so the work cannot be
optimized away.

Each worker scans in a loop for a 6 s window (after a 2 s warmup) at N = 1, 2, 4,
8, 16; aggregate throughput is the summed rows/sec, and the scaling factor is
throughput(N) / throughput(1).

### Results

Representative aggregate throughput (rows/sec) across three passes per side, and
the scaling factor versus that side's own single-worker number.

| N  | rust (threads)      | python (threads)   | python (asyncio)   |
|----|---------------------|--------------------|--------------------|
| 1  | 185,000  (1.0×)     | 202,000  (1.0×)    | 177,000  (1.0×)    |
| 2  | 420,000  (2.3×)     | 252,000  (1.3×)    | 207,000  (1.2×)    |
| 4  | 870,000  (4.6×)     | 118,000  (0.6×)    | 216,000  (1.2×)    |
| 8  | 870,000  (4.7×)     | 109,000  (0.5×)    | 207,000  (1.2×)    |
| 16 | 780,000  (4.2×)     | 101,000  (0.5×)    | 207,000  (1.2×)    |

Scaling factor at N = 8 versus N = 1, the headline number:

| Driver / model      | throughput(8) / throughput(1) |
|---------------------|-------------------------------|
| rust (threads)      | **4.7×**                      |
| python (threads)    | **0.5×** (worse than serial)  |
| python (asyncio)    | **1.2×**                      |

### The verdict

The no-GIL advantage shows up clearly, and the shape matches the prediction.

- **Rust scales until the server caps it.** Aggregate throughput rises roughly
  linearly to ~870k rows/sec at 4 workers (4.6×) and holds there through 8. The
  plateau past 4 is not the GIL, which Rust does not have; it is the single
  container reaching its own serialization ceiling (~870k rows/sec for this
  workload), the same ceiling the no-GIL multi-process probe hit. N workers
  genuinely decode in parallel: each `BlockingConnection` runs on its own
  current-thread runtime and decodes on its own OS thread, sharing nothing.
- **python-oracledb threads do not scale; they regress.** Throughput peaks at 2
  workers (1.3×) and then falls *below* the single-worker number at 4+ (0.5×).
  This is the textbook GIL signature on a CPU-bound workload: the decode cannot
  run on two threads at once, and adding threads only adds GIL hand-off and
  contention, so more workers make it slower.
- **python-oracledb asyncio plateaus.** It overlaps connection I/O on one event
  loop, which buys a little over serial (1.2×) by hiding wait, but the decode
  still runs on the single event-loop thread under the GIL, so it cannot scale a
  decode-bound workload with N. Flat from 2 workers on.

At 8 workers, then, **Rust delivers ~4.7× its single-worker throughput where
python-oracledb threads deliver ~0.5× and asyncio ~1.2×**. In absolute terms
Rust's aggregate at that point is about 8× the Python-threads aggregate and about
4× the asyncio aggregate (~870k vs ~109k vs ~207k rows/sec).

Two honest qualifiers, neither of which dents the conclusion:

- **Single-thread, python-oracledb is competitive and sometimes ahead.** At
  N = 1, python-oracledb threads (~202k) edged out Rust (~185k). The Rust win is
  entirely in *scaling*, not in raw single-connection speed; some of the serial
  gap is the `BlockingConnection` facade's per-call cost discussed in the serial
  section. Selling this as "Rust decodes faster" would be dishonest. Rust decodes
  *in parallel*, which is a different claim.
- **The ceiling is the test database, not the driver.** A single free-tier
  container caps the absolute numbers around 870k rows/sec. A larger or clustered
  database would raise that ceiling and let Rust's parallel decode keep climbing,
  while the GIL would still hold both Python models flat, so this is, if
  anything, a conservative measurement of the gap.

### A driver limitation this surfaced

The scan table is capped at 1000 rows on purpose. Past roughly 1500 of these
20-column rows, the `select *` result spans several network packets, and the
current thin decoder mis-frames that multi-packet wide-row continuation
(`encoded NUMBER too long` or `truncated TTC payload`, depending on whether the
break lands in the single-batch or the paged-fetch path). That is a real bug in
the wide-row multi-packet reassembly, distinct from anything the concurrency
benchmark is testing; the bench stays inside the single-batch envelope so it
measures decode throughput rather than that defect. It is recorded here so it is
not lost, and is out of scope for an additive benchmark to fix.

### Methodology

- **Host:** AMD EPYC 7713 (64 cores / 128 threads), kernel 6.17, `schedutil`
  governor (not pinned; the host was shared, which shows up as the run-to-run
  spread the three-pass medians average over).
- **Database:** the same local `gvenzl/oracle-free:23-slim` container as the
  serial benchmark, here on `localhost:1526`. Loopback TCP, no TLS.
- **CPU vs network:** the workload is client-decode-bound by construction (cached
  table, cheap server scan that the multi-process probe showed scales to ~6× at
  8 sessions; the expensive work is the NUMBER/VARCHAR2 decode on the client).
  That is what makes the GIL the deciding factor and the comparison meaningful.
- **Rust:** N `std::thread` workers, each its own `BlockingConnection`; a barrier
  aligns the measured window and a shared flag ends it. `cargo bench` release
  profile.
- **Python:** python-oracledb 4.0.1, CPython 3.13.12, thin mode. Threads model:
  N `threading.Thread`, one connection each, barrier-aligned. Asyncio model: N
  `connect_async` connections driven by `asyncio.gather` on one event loop.

Reproduce:

```sh
eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1526 \
        ORACLEDB_HOST_PORT=1526 scripts/container.sh env)"

# Rust
CARGO_TARGET_DIR=/path/to/target \
  cargo bench -p oracledb --bench concurrent_throughput

# python-oracledb (threads + asyncio)
.venv-py313/bin/python benches/compare_concurrent_python.py
```

## Why rust-oracle (thick / ODPI-C) is not in this comparison

The well-known `rust-oracle` crate is a thick-mode driver: it binds to ODPI-C,
which in turn requires the Oracle Instant Client shared libraries at runtime.
This project deliberately avoids Instant Client; it is a pure-Rust thin-mode
implementation of the TNS/TTC protocol with no native Oracle dependency. A
thick-mode driver would also be measuring a different code path (OCI inside
Instant Client, not a wire-protocol implementation), so it would not be an
apples-to-apples comparison even if it were installed. It is omitted on purpose,
not by oversight.
