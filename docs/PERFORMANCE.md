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

**`select_one_row`: python-oracledb is faster (about 80 us vs 127 us).** This is
a single round trip where the network cost is tiny and almost everything is
client-side per-call overhead. The Rust blocking benches drive an async runtime
synchronously: every call goes through `Runtime::block_on`, which constructs a
fresh request-scoped `Cx` and installs runtime and context guards before polling
the future (asupersync `builder.rs`). python-oracledb thin is natively
synchronous and pays no equivalent re-entry. On an operation this cheap that
fixed per-call cost is a real fraction of the total. It is an artifact of the
current `BlockingConnection` wrapper, not of the protocol codec, and it would
shrink for a caller driving the async `Connection` API directly inside one
runtime.

**`read_clob`: python-oracledb is faster (about 0.44 ms vs 0.90 ms).** Both
drivers do the same three round trips here (execute, define-fetch the locator,
read the bytes), confirmed by `v$mystat` round-trip counts, so the gap is
CPU-side in the Rust LOB read and decode path, not extra network traffic. This
is the clearest candidate for optimization work: the Rust `read_lob` path
currently reads the full 64 KiB in one request and the difference is in how the
bytes are buffered and decoded. It is called out here precisely because it is
the one place where the Rust path is materially slower and the cause is in our
code.

## Honest caveats

- **This is a single-connection, serial-call benchmark, not a throughput or
  concurrency benchmark.** It says nothing about how either driver scales across
  many concurrent sessions, where the Rust async model and the absence of a GIL
  could matter and where this comparison is silent. Do not read these numbers as
  a throughput claim.
- **GIL note.** python-oracledb thin holds the CPython GIL during its
  pure-Python protocol handling. That does not affect these serial,
  single-threaded medians, but it is the obvious axis on which a multi-connection
  comparison would diverge, and that comparison has not been run here.
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

## Why rust-oracle (thick / ODPI-C) is not in this comparison

The well-known `rust-oracle` crate is a thick-mode driver: it binds to ODPI-C,
which in turn requires the Oracle Instant Client shared libraries at runtime.
This project deliberately avoids Instant Client; it is a pure-Rust thin-mode
implementation of the TNS/TTC protocol with no native Oracle dependency. A
thick-mode driver would also be measuring a different code path (OCI inside
Instant Client, not a wire-protocol implementation), so it would not be an
apples-to-apples comparison even if it were installed. It is omitted on purpose,
not by oversight.
