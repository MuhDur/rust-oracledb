# Oracle server-version support matrix

`rust-oracledb` is exercised against every supported Oracle server generation,
not just the newest one. This document records the honest per-suite ×
per-version result of the live test suites, and it is kept in lockstep with the
gate registry in [`scripts/version_matrix.sh`](../scripts/version_matrix.sh)
(`suite_gate_reason`).

## Why this exists

A connect/auth gap that broke every pre-23ai server once shipped because the
live suites only ran against 23ai. The fix is structural: run **all** live
suites across **all** supported generations, and treat any red cell as either a
bug to fix or a genuine server-feature boundary that must be **proven** — never
a silent skip.

## How to run it

```sh
# Bring the lanes up (gvenzl images), then:
export ORACLE_PASSWORD=oracle ORACLEDB_XE_SYSTEM_PASSWORD=oracle
scripts/version_matrix.sh versions all      # all lanes
scripts/version_matrix.sh versions xe18     # one lane
```

Each lane's fixture schema is bootstrapped first
([`scripts/bootstrap_live_schema.sh`](../scripts/bootstrap_live_schema.sh),
connecting **SYS as SYSDBA** — `system` cannot `DROP USER … CASCADE` on 18c,
it raises `ORA-29972`). The run writes a per-SHA verdict artifact under
`tests/artifacts/version_matrix/versions-<sha>.json`.

## Lanes

| Lane | Image | Generation |
|------|-------|-----------|
| `xe11`   | `gvenzl/oracle-xe:11-slim`    | 11g — **below the protocol floor**; asserted as a structured connect refusal (`DPY-3010` parity), never a connection |
| `xe18`   | `gvenzl/oracle-xe:18-slim`    | 18c |
| `xe21`   | `gvenzl/oracle-xe:21-slim`    | 21c |
| `free23` | `gvenzl/oracle-free:23-slim`  | 23ai |

## Result matrix

Legend: **✓** green · **gate** proven server-feature boundary (see reason) ·
**OPEN** unresolved failure (tracked bug, not gated, not claimed green).

| Suite | 18c | 21c | 23ai |
|-------|-----|-----|------|
| live_connect / live_connect_string | ✓ | ✓ | ✓ |
| live_borrowed_fetch | ✓ | ✓ | ✓ |
| live_typed | **OPEN** (CQN) | ✓ | ✓ |
| live_ref_cursor | ✓ | ✓ | ✓ |
| live_object_decode | ✓ | ✓ | ✓ |
| live_dbms_output | ✓ | ✓ | ✓ |
| live_named_bind_timeout | ✓ | ✓ | ✓ |
| live_error_classification | ✓ | ✓ | ✓ |
| live_edition | ✓ | ✓ | ✓ |
| live_statement_cache | ✓ | ✓ | ✓ |
| live_transport_failover | ✓ | ✓ | ✓ |
| pipeline_live | gate¹ | ✓ | ✓ |
| live_dpl_arrow | ✓² | ✓² | ✓ |
| e2e_live | **OPEN** (CQN) | ✓ | ✓ |
| live_soda | gate³ | ✓ | ✓ |

## Notes

1. **pipeline_live on pre-23ai — gated with proof.** Pipelining requires the
   server to negotiate `END_OF_RESPONSE` framing (protocol version ≥ 319),
   which only 23ai+ advertises. `supports_pipelining()` reflects exactly that
   negotiated capability, so on 18c/21c there is legitimately nothing to
   exercise. The test skips with that explicit reason.

2. **live_dpl_arrow — was failing on all pre-23ai, now fixed.** The direct-path
   TTC messages wrote the ub8 pipeline token unconditionally; a pre-23ai server
   misparsed it (`ORA-03147: missing mandatory TTC field`). Fixed by gating the
   token on the negotiated ttc field version (bead `rust-oracledb-dpl23`).

3. **live_soda on 18c — gated with proof (`rust-oracledb-soda-pre21c`).** The
   driver's SODA path uses `JSON_SERIALIZE` (a 21c+ SQL function) and the
   `USER_SODA_COLLECTIONS` catalog view, neither present on 18c. Proof:
   `ORA-00904: "JSON_SERIALIZE": invalid identifier` at collection create, and
   `USER_SODA_COLLECTIONS` absent. Full pre-21c SODA support (the older SODA
   SQL path) is a real, explicitly-bounded feature gap — tracked, not deferred
   silently.

## Open items (NOT green, NOT gated)

- **CQN `register_query` on 18c — `rust-oracledb-cqn18c` (OPEN).** `e2e_live`
  and `live_typed` register-query sub-tests fail on **18c only** with
  `ORA-29970: Specified registration id does not exist`; green on 21c and
  23ai. The subscribe message is already version-gated correctly and returns a
  `registration_id`, but the 18c server rejects that id at `register_query`
  time. This is **not** the ub8-token class (that failed on all pre-23ai). It
  is either an 18c-specific subscribe-response parsing difference or genuine
  18c CQN behavior, and must be root-caused (wire compare 18c vs 23ai) before
  it is fixed or — only with proof — gated. Until then, 18c is **not**
  all-green and is not claimed as such.
