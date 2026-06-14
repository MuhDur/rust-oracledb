# xad/3oi baseline — single-connection multi-page fetch read/decode attribution

## Fingerprint
- CPU: AMD EPYC 7713 64-Core (128 threads), governor schedutil
- Kernel: 6.17.0-35-generic
- rustc: 1.97.0-nightly (64a965e90 2026-05-11)
- Build: release (opt-level 3)
- DB: Oracle Free 23ai in container, loopback localhost:1523/FREEPDB1
- git base SHA: 860dfee (branch xad-decode-offload)
- Scenario: `select level as n from dual connect by level <= 50000`, arraysize 1000
  -> ~49 paged `fetch_rows` calls per iteration, 20 iters, warm connection + statement cache

## Measured (mean of 3 runs)
| metric          | value        |
|-----------------|--------------|
| wall / iter     | ~25.4-27.9 ms |
| read  / page    | ~324-369 us (69-70%) |
| decode / page   | ~140-145 us (30-31%) |

## Hotspot table
| Rank | Location               | Metric      | Value        | Category | Evidence |
|------|------------------------|-------------|--------------|----------|----------|
| 1    | read_response (socket) | cum/page    | ~324 us      | I/O      | profile_fetch_attribution |
| 2    | parse_fetch_response   | cum/page    | ~140 us      | CPU      | profile_fetch_attribution |

## Interpretation / hypothesis ledger
- "decode is overlappable with next read" : SUPPORTS — decode is a steady ~30% (140us)
  fully separable from the socket read (324us). One-page look-ahead prefetch can issue
  page K+1's FETCH while decoding page K, hiding up to min(read,decode)=~140us/page
  (~30% of read+decode) on loopback.
- "loopback read is tiny so overlap is pointless" : REJECTS — read is the LARGER term here
  (324us) and decode (140us) is non-trivial; overlap hides the full decode behind the read.
  On real-network RTT the read term grows (dominated by RTT) so the prefetch saves close to
  a full RTT per page — the win is strictly larger off loopback.

## Decision: Framing (B) Speculative next-page prefetch (bead 3oi), one-page look-ahead.
Reasons over (A) decode-offload-to-worker-pool:
1. Decode stays on the SAME task -> borrowed-fetch (QueryValueRef) buffer never crosses a
   thread/await boundary that outlives it. #![forbid(unsafe_code)] preserved, no lifetime risk.
2. Reuses the existing array-fetch round-trip; no new protocol, no channel, no worker thread.
3. Captures the whole decode-hide win (the entire 30%) with far less complexity/risk.
The only added correctness surface is the in-flight prefetched page on cancel/drop, handled
by extending the existing CancelDrainGuard / cancel_drain_pending machinery (see tests).
