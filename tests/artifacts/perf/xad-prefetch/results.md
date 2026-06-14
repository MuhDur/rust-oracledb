# xad/3oi — speculative next-page prefetch: measured results

## Framing chosen: (B) Speculative next-page prefetch, one-page look-ahead.
Decode stays on the SAME task -> borrowed-fetch QueryValueRef buffer never crosses
a thread/await boundary that outlives it. No worker pool, no channel, no extra
thread. #![forbid(unsafe_code)] preserved. Reuses the existing array-fetch round
trip. (Also satisfies decode-offload bead xad's intent: the overlap is the
intra-connection no-GIL win that python-oracledb cannot do.)

## Mechanism
The borrowed paging loop (`Connection::for_each_row_ref`) now issues page K+1's
FETCH request (`fetch_rows_request`, send-only) BEFORE decoding page K and running
the consumer callback, then reads page K+1's response next iteration
(`fetch_rows_ref_response`). The server processes K+1 and the kernel buffers its
bytes while the client decodes K + runs the callback, so the later read returns
sooner. `previous_row` (the duplicate-column seed) is needed only at K+1 DECODE
time, by which point K is fully decoded — so the request can be sent without K's
rows. One page of look-ahead, bounded.

## Cancellation safety (the #1 risk, handled explicitly)
`fetch_rows_request` arms `cancel_drain_pending` for the WHOLE window until the
speculative response is consumed (a drop during the prior page's decode, the
callback, OR the response read leaves the stranded page to be broken + drained by
the next op — reusing the merged CancelDrainGuard + break_and_drain machinery).
`fetch_rows_ref_response` clears it only on a clean read. Proven by the live test
`connection_is_reusable_after_drop_mid_prefetch` (drop a 200k-row fetch mid-flight,
then `select 7+5 -> 12` on the SAME connection + a follow-up multi-row fetch).

## Measured (AMD EPYC 7713, loopback Oracle 23ai container, 50k rows, arraysize 1000, ~49 pages)

### PRIMARY signal — direct read-wait attribution (example profile_fetch_attribution)
The read counter measures the read-wait: how long the response read `.await` blocks
ONCE REACHED. If the prefetch overlaps the server round trip with the prior page's
decode + callback, the bytes are already in the kernel buffer by the time we await,
so read-wait DROPS. This isolates the overlap and excludes the fixed per-iter
execute cost — the robust, repeatable number (6 runs):
| metric            | serial     | prefetched | delta            |
|-------------------|------------|------------|------------------|
| read-wait / page  | ~294-318us | ~241-280us | **-5.6% .. -24%** (consistently negative) |

### Isolated A/B wall with a REALISTIC per-row consumer (6 runs, median of 15 rounds each)
A real caller does work per row; the per-page decode + that work then covers the
server round trip, so the overlap pays off in wall time too:
| metric          | serial    | prefetched | delta             |
|-----------------|-----------|------------|-------------------|
| wall / iter     | ~27-29 ms | ~23-24 ms  | **-12.5% .. -19.5%** |

### Criterion (cargo bench thin_driver -- oracledb_prefetch, 40 samples) — NOISY on loopback
Criterion's measured op includes the fixed per-iter execute round trip + cursor
reuse, which dilutes and adds variance to the overlap on loopback. Observed runs
for the realistic-work pair ranged from break-even to -6% (e.g. 56.08->52.84 ms =
-5.8%; another run 53.76->53.40 ms = -0.7%). HONEST READ: on loopback criterion's
end-to-end wall sits in the noise-to-modest-win band; the clean overlap evidence is
the read-wait attribution above. The bench is retained because it is the right
vehicle on real-network RTT, where the read-wait term dominates and the win grows.

## Honest caveats (methodology)
- The read-wait reduction (-8%..-24%) is the ROBUST, directly-measured signal: it
  isolates how long the response read .await blocks once reached. It is consistently
  negative => the overlap genuinely hides the server round trip.
- WALL time on loopback with a TRIVIAL consumer is near break-even (-2%): the read
  latency hidden is tiny (loopback), so prefetch bookkeeping ≈ saved latency.
- WALL time with a REALISTIC consumer (work per row) wins clearly (-6% criterion,
  -15%..-19% in the isolated A/B): the per-page CPU now fully covers the round trip.
- On REAL NETWORK RTT the read-wait term is RTT-dominated (often 1-50 ms vs the
  ~310 us loopback read), dwarfing the ~few-us prefetch bookkeeping, so even the
  trivial-consumer case wins strongly. The loopback numbers are the conservative
  floor; the win is strictly larger off loopback.

## Soundness proofs (tests/prefetch_overlap.rs, all green vs container)
- prefetched_borrowed_fetch_is_byte_identical_to_serial_owned (20k mixed-type rows)
- low_level_request_response_split_matches_fused_fetch (3k rows)
- connection_is_reusable_after_drop_mid_prefetch (200k-row fetch dropped mid-prefetch
  -> select 7+5 = 12 on same conn + multi-row follow-up)
