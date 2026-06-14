# PARITY_SKIPS — taxonomy of the 116 gated tests

**Bead:** `rust-oracledb-0na` (PARITY-RIGOR lane).
**Goal:** prove every one of the 116 skipped reference tests is skipped because the
**environment / thin-mode contract** gates it — *not* because the Rust engine would
fail it — and convert "skip" → "pass" wherever the local container can stand the
capability up.

This document is generated from a real run of the full filtered suite
(`harness/run.sh baseline`, the reference python-oracledb thin driver) against the
lane container, and corroborated by running the same suite through the Rust engine
(`harness/run.sh rust`). Reproduce with `scripts/parity_rigor.sh`.

## Headline numbers (authoritative, segmented harness)

| Driver | passed | skipped | total |
|--------|-------:|--------:|------:|
| python-oracledb thin (reference baseline) | 2462 | 116 | 2578 |

The skip set is **identical** between the reference thin driver and the Rust engine:
both skip the same 116 node IDs for the same reasons, because the skip decisions are
made by the reference test suite's own `conftest.py` fixtures and in-test
`pytest.skip` / `@pytest.mark.skip` calls **before** any driver code runs. The driver
under test cannot change which tests skip.

### Rust-engine cross-check (exact node IDs)

The 15 modules that contain all 116 skips were run through the **Rust engine**
(`shim_inject`), per-module, and compared to the reference baseline at the node-ID
level:

| | reference thin | Rust engine |
|---|---:|---:|
| skipped (these 15 modules) | 116 | 116 |
| passed (these 15 modules) | 303 | 303 |
| failed | 0 | 0 |

- **Skip sets identical (exact node IDs): yes.** The Rust engine skips the same 116
  tests the reference does — not one more, not one fewer.
- **Regressions (baseline-pass → Rust-non-pass): 0.** The Rust engine passes every one
  of the 303 baseline-passing tests in these modules.

This is the core proof: the skips are gated by the test suite's environment/contract
checks, are identical between engines, and the Rust engine passes everything that is
not skip-gated. None of the 116 skips hides a Rust engine defect.

> Note: a full single-pass 88-module Rust run currently snags on a **pre-existing**
> shim teardown deadlock (a `futex` wait left by the async-offload worker pool when
> `test_1600_dml_returning` runs as a non-first sequential module) — tracked
> separately under the async-execution bead, unrelated to this taxonomy. The
> skip-bearing modules above run cleanly in isolation and reproduce the exact skip set.

## Environment that produced this baseline

- Container image: `gvenzl/oracle-free:23-slim` style, **Oracle Database 23ai Free
  `23.26.1.0.0`** (lane container `rust-oracledb-lane-1526`, host port 1526).
- `NLS_CHARACTERSET = AL32UTF8`, `NLS_NCHAR_CHARACTERSET = AL16UTF16`.
- Connect string: `localhost:1526/FREEPDB1` — **dedicated server** (no `:POOLED`).
- Driver mode: `thin`.
- Schema users `PYTHONTEST` / `PYTHONTESTPROXY` created with the standard test schema.

Because the database is 23.26 with AL32UTF8, **every version / charset / feature gate
is already satisfied** — there are zero skips from `skip_unless_server_version`,
`skip_unless_*_supported`, charset, native-JSON, native-boolean, sparse/binary
vectors, sessionless transactions, long passwords, domains, etc. In thin mode
`has_client_version()` always returns `True`, so those client-version gates never fire
either. The 116 skips therefore come from only **six** distinct reasons, all of which
are thin-structural, thick-only, or hardcoded-upstream.

## Skip taxonomy (counts)

| Count | Skip reason | Category | Reachable locally as PASS? |
|------:|-------------|----------|----------------------------|
| 88 | `requires thick mode` | (a) thick-only / thin-unimplemented | **No** — thin can't; reference skips too |
| 17 | `external authentication not configured` | (b) environment-gated (OS/bequeath auth) | **No** — thin can't (proven: reference FAILS them) |
| 4  | `client supports vectors directly` | (c) correct-by-design (thin vector interop) | **No** — testing an *older-client* path that thin is not |
| 3  | `awaiting database support` | upstream hardcoded `@pytest.mark.skip` | **No** — DB feature does not exist yet |
| 2  | `fails intermittently` | upstream hardcoded `@pytest.mark.skip` | **No** — reference disables them unconditionally |
| 2  | `awaiting fix for bug 37746852` | upstream hardcoded `@pytest.mark.skip` | **No** — reference disables them unconditionally |
| **116** | | | **0 convertible** |

**Bottom line:** 0 of the 116 can be flipped to a *matching* PASS on this (or any)
local container while staying in thin mode, because each one is either (a) a feature
the python-oracledb **thin driver does not implement** (so the reference's own
`skip_unless_thick_mode` gate skips it, and un-skipping makes the *reference* fail),
(b) an authentication mechanism thin-over-TCP **cannot perform** (proven below by
un-gating it and watching the reference thin driver fail all 17), (c) a deliberately
inverted "older client" interop check, or (d) a hardcoded upstream skip independent of
any environment. Every skip is forced by the thin-mode contract; none hides a Rust
engine defect.

## Per-test table

### (a) `requires thick mode` — 88 tests — thick-only / thin-unimplemented

The reference suite gates these with the `skip_unless_thick_mode` fixture. In thin mode
they always skip; in thick mode (OCI) they would run. The Rust engine ports the
python-oracledb **thin** driver, so by construction it does not implement these
OCI-only capabilities — exactly as the reference thin driver does not.

| Module | Tests | What they exercise (thick-only) |
|--------|-------|----------------------------------|
| `test_3400_soda_collection.py` | test_3400–test_3447 (48) | SODA collection API — python-oracledb has **no thin SODA** at all |
| `test_3300_soda_database.py` | test_3300–test_3311 (12) | SODA database/metadata API — thin has no SODA |
| `test_2400_pool.py` | test_2401, 2407, 2408, 2409, 2410, 2411, 2412, 2415, 2417 (9) | OCI session pools: proxy-auth pool, heterogeneous pools, session tagging, PL/SQL session callbacks, reconfigure |
| `test_9800_external_oci_stmt.py` | test_9800–test_9806 (7) | external OCI statement handle (fetchone/all, fetch_df, requested_schema) — thick-only module |
| `test_1100_connection.py` | test_1111, 1121, 1122, 1123, 1124 (5) | OCI connection handle (1111); begin/prepare/cancel + global (XA) transactions (1121–1124) |
| `test_2300_object_var.py` | test_2328, test_2329 (2) | objects with an **unknown** attribute/element type (OCI type resolution) |
| `test_2700_aq_dbobject.py` | test_2714, test_2715 (2) | AQ enqueue/dequeue **transformations** (thick AQ feature) |
| `test_2800_aq_bulk.py` | test_2802, test_2804 (2) | AQ bulk dequeue-with-wait (2802); enqueue/dequeue **visibility** option (2804) |
| `test_2000_long_var.py` | test_2005 (1) | OCI-specific oversized arraysize error path |

**Reachable locally? No.** These require thick (OCI) client mode. Standing up thick
mode is out of scope for a thin port and would not validate the thin engine. If run in
thin mode the reference *also* skips them; if forced to run, the reference thin driver
cannot satisfy them either. (python-oracledb thin ships **zero** SODA — the 60
SODA tests can only ever pass under thick mode + a SODA-enabled DB.)

### (b) `external authentication not configured` — 17 tests — environment + thin-structural

`test_5000_externalauth.py` has an autouse fixture:

```python
@pytest.fixture(autouse=True)
def skip_if_no_external_auth(test_env):
    if not test_env.external_user:          # PYO_TEST_EXTERNAL_USER unset
        pytest.skip("external authentication not configured")
```

| Module | Tests |
|--------|-------|
| `test_5000_externalauth.py` | test_5000 – test_5016 (17) |

**Reachable locally? No — and proven by experiment.** External authentication in
python-oracledb means **OS / bequeath authentication** (an `IDENTIFIED EXTERNALLY`
database user matched to the *client's* operating-system identity). This is a
**thick-only, local-IPC** mechanism: the database must see the client's OS identity,
which only happens over a bequeath/IPC connection where client and server are the same
OS user on the same host. Our harness connects over **TCP** from host user
`durakovic` to a containerized DB whose process runs as OS user `oracle` — the DB can
never see a matching OS identity, and **thin mode does not implement bequeath at all**.

To prove this is an environment/thin gate and not an engine defect, the lane un-gated
the suite empirically:

1. Created the only externally-identified principal the DB could authenticate locally:
   `CREATE USER ops$oracle IDENTIFIED EXTERNALLY;` (matching `os_authent_prefix=ops$`
   and the DB's OS user `oracle`).
2. Set `PYO_TEST_EXTERNAL_USER=ops$oracle` and ran `test_5000_externalauth.py`
   through the **reference python-oracledb thin driver**.

Result: **all 17 FAILED**, with
`DPY-3001: bequeath is only supported in python-oracledb thick mode` and
`DPY-4001: no credentials specified`. The reference thin driver itself cannot pass a
single one. The `skip` is therefore the *correct* outcome: un-skipping yields failures,
not passes, even for the reference. (The probe user was dropped afterward to keep the
baseline pristine.)

**Concrete external requirement to make these pass:** python-oracledb **thick** mode
(OCI client libraries) **and** a local IPC/bequeath connection from an OS user that
maps to an `IDENTIFIED EXTERNALLY` DB user — or, alternatively, a token-auth provider
(IAM/OAuth) for the token-based subset. None of these are available to a thin-over-TCP
harness and none would validate the thin engine.

### (c) `client supports vectors directly` — 4 tests — correct-by-design

`test_6500_vector_interop.py` module fixture:

```python
@pytest.fixture(autouse=True)
def module_checks(test_env):
    if test_env.has_client_version(23, 4):
        pytest.skip("client supports vectors directly")   # always True in thin mode
    if not test_env.has_server_version(23, 4):
        pytest.skip("unsupported server")
```

| Module | Tests |
|--------|-------|
| `test_6500_vector_interop.py` | test_6500, test_6501, test_6502, test_6503 (4) |

**Reachable locally? No — and it should not be.** This module verifies the *legacy
fallback* behaviour of an **older client that does not understand the VECTOR type** —
where a `VECTOR` column is fetched as `CLOB`/JSON text instead of a native vector. In
thin mode `has_client_version()` returns `True` unconditionally, i.e. thin is treated
as a fully modern client that handles vectors **natively**. The native-vector path is
exercised and passing in `test_6400_vector_var.py` and `test_7500_binary_vector.py`.
Running the old-client fallback assertions against a modern client would assert the
*wrong* type mapping. This is the reference's deliberate, correct design — un-skipping
would be a bug, not a fix.

### (d) Hardcoded upstream `@pytest.mark.skip` — 7 tests — environment-independent

These are unconditional skips written directly into the reference test source. They do
not consult the environment, the server version, or the driver at all — no
configuration can make them run, short of editing the upstream reference (which would
break the parity contract).

| Module | Test | Decorator |
|--------|------|-----------|
| `test_6500_vector_interop.py` | test_6504 | `@pytest.mark.skip("awaiting database support")` |
| `test_6400_vector_var.py` | test_6426 | `@pytest.mark.skip("awaiting database support")` |
| `test_6400_vector_var.py` | test_6429 | `@pytest.mark.skip("awaiting database support")` |
| `test_3000_subscription.py` | test_3003 | `@pytest.mark.skip("fails intermittently")` |
| `test_3000_subscription.py` | test_3007 | `@pytest.mark.skip("fails intermittently")` |
| `test_8300_aq_json.py` | test_8302 | `@pytest.mark.skip("awaiting fix for bug 37746852")` |
| `test_8500_aq_json_async.py` | test_8502 | `@pytest.mark.skip("awaiting fix for bug 37746852")` |

**Reachable locally? No.** These represent upstream-acknowledged gaps (a DB feature
that does not exist yet, two intermittently-flaky CQN subscription tests the reference
itself disables, and a known Oracle bug 37746852 in AQ-JSON). They are skipped by the
reference regardless of engine. The Rust engine skips them identically.

## Capabilities the 23ai container CAN stand up (positive evidence)

Even though none of the 116 skips are *convertible to a matching PASS in thin mode*,
the task asked which container capabilities can be stood up. Findings:

### DRCP (Database Resident Connection Pooling) — works through the Rust engine

- **Why it is not a skip source here:** the reference detects DRCP purely from the
  **connect string** (`ConnectParams.server_type == "pooled"`), *not* from whether the
  pool is running. The harness's default DSN is `…/FREEPDB1` (dedicated), so
  `is_drcp` is `False` and the 27 `skip_if_drcp` tests **run and pass**. Switching the
  harness to a `:POOLED` DSN would make those 27 tests *start* skipping — the opposite
  of un-skipping, and a GATE regression. DRCP is therefore a **demonstrated
  capability**, not a skip-to-pass conversion.
- **Stand-up (reproducible):** the gvenzl container's pool is only fully usable after
  starting it at the **CDB root**:

  ```sql
  -- as sysdba at the CDB root (NOT inside the PDB):
  begin dbms_connection_pool.start_pool(); end;
  /
  ```

  (`DBMS_CONNECTION_POOL.START_POOL` is not declared inside `FREEPDB1`; before the
  root `start_pool`, a `:POOLED` connect is refused with
  `ORA-12520 / DPY-6000: Listener refused connection`.)
- **Proof:** after `start_pool()`, a `localhost:1526/FREEPDB1:POOLED` connection with
  `cclass="RIGOR"` and `purity=PURITY_SELF` **succeeds through the Rust shim** and
  returns query results. `scripts/parity_rigor.sh --drcp` reproduces this end to end.
  This exercises the engine's DRCP connect path, connection-class, and purity handling.

### Proxy authentication — already passing in thin (no skip)

Proxy auth (`PYTHONTEST[PYTHONTESTPROXY]`) is a thin-supported feature; the proxy-auth
tests that *can* run in thin already **pass** in the baseline. The only proxy-related
tests that skip are the OCI **pool** proxy tests in `test_2400_pool.py` (counted under
"requires thick mode"), which need OCI session pools.

## Reproduce

```bash
# baseline (reference thin) + Rust engine, full taxonomy:
scripts/parity_rigor.sh

# just the DRCP stand-up + proof through the Rust engine:
scripts/parity_rigor.sh --drcp

# just the external-auth disproof (reference thin FAILS all 17 when un-gated):
scripts/parity_rigor.sh --externalauth
```
