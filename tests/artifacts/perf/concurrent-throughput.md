# Concurrent throughput — measured results (the no-GIL margin)

Artifact for the `margin-bench` lane. This is the **large** margin: aggregate
decode throughput when N workers each drive their own connection in parallel.
Rust has no GIL, so N workers decode on N cores; python-oracledb thin runs its
codec under the CPython GIL, so the same CPU-bound decode cannot scale.

## Harnesses

- Rust: `crates/oracledb/benches/concurrent_throughput.rs`
  (`cargo bench -p oracledb --bench concurrent_throughput`)
- Python: `benches/compare_concurrent_python.py` (threads + asyncio models)

Both run the byte-identical decode-bound workload (scan of a warmed
1000-row × 20-column `PERFTEST_CONC` table, 10 `NUMBER` + 10 `VARCHAR2`) at the
same worker counts.

## Fingerprint

- Host: AMD EPYC 7713 (64C / 128T), kernel 6.17, `schedutil` governor, **not
  pinned**, host **shared/busy** (load average ~10 during the run).
- DB: `gvenzl/oracle-free:23-slim` container, `localhost:1525`, loopback TCP, no
  TLS.
- Rust: N `std::thread` workers, each its own `BlockingConnection` (its own
  current-thread runtime: one epoll reactor + one worker OS thread), barrier-
  aligned 6 s window after 2 s warmup.
- Python: python-oracledb 4.0.1, CPython 3.13.12, thin mode. Threads:
  N `threading.Thread`, one connection each. Asyncio: N `connect_async`
  connections via `asyncio.gather` on one event loop.

## Measured medians (rows/sec aggregate, 3 passes per side)

| N  | rust (threads)        | python (threads)      | python (asyncio)      |
|----|-----------------------|-----------------------|-----------------------|
| 1  | 245,000  (1.00×)      | 188,000  (1.00×)      | 176,000  (1.00×)      |
| 2  | 522,000  (2.13×)      | 240,000  (1.28×)      | 207,000  (1.18×)      |
| 4  | 1,065,000 (**4.35×**) | 122,000  (**0.65×**)  | 218,000  (1.23×)      |
| 8  | 895,000  (3.65×)      | 111,000  (0.59×)      | 203,000  (1.15×)      |
| 16 | 702,000  (2.87×)      | 115,000  (0.61×)      | 204,000  (1.16×)      |

Per-side scaling = throughput(N) / throughput(1).

### Raw passes (rows/sec aggregate)

Rust threads:

| N  | pass 1    | pass 2    | pass 3    | median    |
|----|-----------|-----------|-----------|-----------|
| 1  | 254,813   | 245,274   | 229,074   | 245,274   |
| 2  | 522,857   | 521,615   | 522,831   | 522,831   |
| 4  | 1,046,241 | 1,073,061 | 1,064,697 | 1,064,697 |
| 8  | 895,554   | 827,529   | 905,190   | 895,554   |
| 16 | 689,442   | 702,920   | 701,572   | 701,572   |

Python threads:

| N  | pass 1    | pass 2    | pass 3    | median    |
|----|-----------|-----------|-----------|-----------|
| 1  | 187,065   | 188,124   | 188,173   | 188,124   |
| 2  | 239,776   | 244,052   | 239,021   | 239,776   |
| 4  | 121,570   | 142,297   | 121,795   | 121,795   |
| 8  | 111,172   | 128,699   | 107,324   | 111,172   |
| 16 | 104,549   | 122,502   | 115,243   | 115,243   |

Python asyncio:

| N  | pass 1    | pass 2    | pass 3    | median    |
|----|-----------|-----------|-----------|-----------|
| 1  | 175,868   | 189,080   | 174,728   | 175,868   |
| 2  | 209,500   | 207,185   | 206,370   | 207,185   |
| 4  | 218,102   | 220,001   | 214,798   | 218,102   |
| 8  | 208,366   | 203,402   | 195,492   | 203,402   |
| 16 | 206,213   | 202,031   | 203,907   | 203,907   |

## Reading

- **Rust scales until the single container caps it.** Super-linear to N=2
  (2.1×), peaks at N=4 (~1.07M rows/sec, ~4.35×), then the *single free-tier
  container* — not the driver — caps it and aggregate eases off at N=8/16. The
  cap is the server: a no-GIL multi-process probe hit the same ~1M-rows/sec
  ceiling on this container. A larger/clustered DB would raise the ceiling and let
  Rust keep climbing; the GIL would still hold both Python models flat. So this is
  a **conservative** read of the gap, not an inflated one.
- **python-oracledb threads regress.** Textbook GIL signature: peak at N=2
  (1.28×), then *below serial* at N≥4 (~0.6×). Adding threads to a CPU-bound
  decode only adds GIL hand-off, so more workers make it slower.
- **python-oracledb asyncio plateaus.** Overlapping connection I/O on one event
  loop buys ~1.2× by hiding wait, but the decode still runs on the single
  event-loop thread under the GIL, so it cannot scale a decode-bound workload.

**At N=4, Rust's aggregate (~1.07M rows/sec) is ~8.7× python-threads (~122k) and
~4.9× asyncio (~218k).** The headline is the *scaling*, not single-connection
speed (at N=1 python-threads ~188k edges Rust ~245k? — here Rust leads at N=1 too,
but the win that matters and that holds across hosts is the scaling shape).

## Off-loopback (network latency) — reasoned, not measured

We did **not** inject network latency. `tc netem` requires root (no passwordless
sudo on this host) and applying it to `lo` would corrupt every other lane's
container traffic on this shared box — exactly the global state change the
profiling discipline says to never apply un-asked. A userspace TCP latency proxy
was out of scope for an additive bench. So no off-loopback numbers are reported,
and none are fabricated.

The **direction** is nonetheless determined by the structure of the workload:
adding RTT enlarges the per-fetch read-wait that every worker spends blocked on
the socket. On loopback that wait is tiny (~hundreds of µs), so the decode is
already the bottleneck. Add real RTT and the wait grows, which (a) lets more Rust
workers overlap their decode against each other's read-wait before the server
saturates, pushing the parallel-decode advantage *up*, and (b) does nothing for
the GIL-bound Python models, whose single decode thread still cannot run during
any of that wait. So the measured loopback margin is the **floor**; real network
latency widens it. This is the same logic the pipelined-fetch result records, and
it is stated as reasoning, not a number.
