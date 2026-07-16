# Oracle server-version support matrix

`rust-oracledb` is exercised against every supported Oracle server generation,
not just the newest one. This document records the honest per-suite ×
per-version result of the live test suites, and it is kept in lockstep with the
gate registry in [`scripts/version_matrix.sh`](../scripts/version_matrix.sh)
(`suite_skip_reason`).

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

### Running one live suite directly

The live suites are `#[ignore]` and **fully env-driven** — no lane is hardcoded
(the free23 connect string / `pythontest` account are only *default fallbacks*
for a bare `cargo test`). The version-matrix harness above sets these per lane;
to run a single suite by hand, export the lane's coordinates first:

```sh
# xe18 (18c)
export PYO_TEST_CONNECT_STRING=localhost:1518/XEPDB1 \
       PYO_TEST_MAIN_USER=testuser PYO_TEST_MAIN_PASSWORD=testpw
# xe21 (21c): localhost:1520/XEPDB1  testuser / testpw
# free23 (23ai): localhost:1522/FREEPDB1  pythontest / pythontest

cargo test -p oracledb --test live_connect -- --ignored
```

Suites that need a proxy or SODA also read `PYO_TEST_PROXY_USER` /
`PYO_TEST_PROXY_PASSWORD`. Because every suite resolves its connection from
these variables, the same test binary runs unchanged against all three
generations — which is exactly what the matrix exercises, so portability is
proven by the green result matrix below, not assumed.

## Result matrix

Legend: **✓** green · **SKIP** typed, proven server-feature limitation (with a
stable reason code) · **OPEN** unresolved failure (tracked bug, not claimed
green).

A `SKIP` is never rendered as a passing suite result. The matrix accepts one
only after its named live capability probe passes and preserves the distinct
reason code in its JSON artifact.

| Suite | 18c | 21c | 23ai |
|-------|-----|-----|------|
| live_connect / live_connect_string | ✓ | ✓ | ✓ |
| live_borrowed_fetch | ✓ | ✓ | ✓ |
| live_typed | ✓ (CQN gate⁴) | ✓ | ✓ |
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
| e2e_live | ✓ (CQN gate⁴) | ✓ | ✓ |
| live_soda | SKIP³ (`pre-21c-soda-unsupported`) | ✓ | ✓ |

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

3. **live_soda on 18c — typed SKIP with proof
   (`pre-21c-soda-unsupported`; `rust-oracledb-soda-pre21c`).** The driver's
   SODA write path requires `JSON_SERIALIZE`, a 21c+ SQL function. Before
   emitting the `SKIP` cell, the matrix runs
   `soda_gated_on_pre21c_with_proof`, which verifies that the direct probe
   fails and `create_collection` returns `ORA-00904: "JSON_SERIALIZE": invalid
   identifier`. `USER_SODA_COLLECTIONS` is a public synonym that is selectable
   on 18c, so it is deliberately not used as a capability signal. Full pre-21c
   SODA support (the older SODA SQL path) is a real, explicitly-bounded feature
   gap — tracked, not deferred silently.

4. **CQN on pre-21c — gated with proof (`rust-oracledb-cqn18c`).** Change
   notification is a **thin-mode extension beyond python-oracledb thin
   parity** — python-oracledb thin does not implement CQN at all (`DPY-3001:
   bequeath is only supported in thick mode`), so there is no reference to
   match against. The Rust implementation is validated on Oracle 21c+. On 18c
   the subscribe succeeds and returns a `registration_id`, but the server then
   rejects that id at **both** `register_query` and `subscribe_unregister` with
   `ORA-29970: Specified registration id does not exist` — the whole 18c CQN
   registration lifecycle is inoperative, not just one call. The `e2e_live` and
   `live_typed` CQN sub-scenarios therefore skip on servers below 21c
   (`server_version_tuple().0 < 21`) with that explicit reason; the subscribe
   parsing itself is byte-for-byte identical to python-oracledb's
   (`subscribe.pyx`). Full pre-21c thin CQN — if it is even achievable, given
   python thin does not attempt it — is a real, explicitly-bounded extension
   gap, tracked, not deferred silently.

## Open items

None. Every suite is green, has an explicit typed limitation with cited proof,
or has a documented in-test capability gate. The SODA pre-21c limitation and
the CQN pre-21c boundary are tracked as explicit feature-scope beads, not
silent skips.
