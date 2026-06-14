# Per-thread decode throughput — measured results

Artifact for the `margin-bench` lane. Isolates the single-threaded **language**
win (compiled, no-per-cell-object decode vs an interpreted decoder that builds a
Python object per cell), with no concurrency and no GIL hand-off on either side.
The companion concurrent results (the *large* margin) are in
[`concurrent-throughput.md`](concurrent-throughput.md).

## Harnesses

- Rust: `crates/oracledb/benches/decode_throughput.rs`
  (`cargo bench -p oracledb --bench decode_throughput`)
- Python: `benches/compare_decode_python.py`
  (`.venv-py313/bin/python benches/compare_decode_python.py`)

Both issue the **byte-identical** SQL — a mixed `NUMBER` + `VARCHAR2` + `DATE`
projection over a `connect by level` generator — to the same container over the
same loopback socket, paged at the same arraysize. The only variable is the
codec.

## Workload

- 300,000 rows × 5 columns: 2 `NUMBER`, 2 `VARCHAR2(32)`, 1 `DATE`.
- Single connection, paged fetch at `arraysize = 1000` (~300 fetch round-trips).
- Narrow row × modest page so no single batch trips the multi-packet wide-row
  reassembly defect documented in `concurrent_throughput.rs`; the decode volume
  is large because there are many *rows*, not because any batch is wide.
- Every cell touched after fetch so the per-cell object/decode is forced to exist
  (`NUMBER` parse, `VARCHAR2` UTF-8 validate + build, `DATE` 7-byte decode).
- Throughput = rows / wall-clock. Median over 5 passes per run; 3 runs per side.

## Fingerprint

- Host: AMD EPYC 7713 (64C / 128T), kernel 6.17, `schedutil` governor, **not
  pinned**, host **shared/busy** (load average ~10 during the run — this is the
  source of the run-to-run spread, see the slow Python run 3 below).
- DB: `gvenzl/oracle-free:23-slim` container, `localhost:1525`, loopback TCP, no
  TLS.
- Rust: `cargo bench` release profile.
- Python: python-oracledb 4.0.1, CPython 3.13.12, thin mode.

## Measured medians (rows/sec decoded, single connection)

Each cell is the median of that run's 5 passes.

| run | rust (rows/sec) | python (rows/sec) | ratio rust/python |
|-----|-----------------|-------------------|-------------------|
| 1   | 334,296         | 276,019           | 1.21×             |
| 2   | 307,767         | 280,386           | 1.10×             |
| 3   | 328,966         | 201,520*          | 1.63×*            |

\* Python run 3 caught a host-contention dip (201k vs its usual ~278k); it is
kept for honesty, not used as the headline.

**Representative figure (median of the per-run medians):**

- Rust:   **~329,000 rows/sec**
- Python: **~276,000 rows/sec**
- **Ratio: ~1.2× (Rust faster), best-case ~1.26×.**

## Honest reading

This is a **real but modest** single-threaded win: ~1.2×. Rust decodes `NUMBER`
into an inline `{ i128, scale }` (no per-cell heap allocation for the common
case), `VARCHAR2` into a `String`/`&str`, and `DATE` into inline fields, where
python-oracledb materializes a Python `int`/`Decimal`/`str`/`datetime` object per
cell. That object-per-cell cost is what Rust avoids, and it shows up even at one
worker — but on loopback, a chunk of the per-pass wall-clock is still the wire
round-trips both drivers pay equally, which caps how large the single-thread ratio
can get.

The **large** margin is not here — it is in concurrency (the no-GIL result), where
Rust's per-thread decode runs on every core at once and Python's cannot. See
`concurrent-throughput.md`: ~4.4× scaling at N=4 for Rust vs python-threads
*regressing* to ~0.6×. This per-thread bench exists to show the language win is
real on its own (so the concurrency win is not just "more cores"), not to claim
the headline.
