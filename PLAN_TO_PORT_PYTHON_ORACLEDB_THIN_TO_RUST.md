# python-oracledb (thin) → `oracledb` Rust Reformation Plan

**Status:** planning complete, research-backed, ready to convert into the autonomous codex goal.
**Companion brief:** `plan.md` (identity, scope philosophy, guardrails — still authoritative for intent).
**This document:** the detailed, evidence-pinned execution plan. All file:line citations refer to
the pinned reference checkout (see Source Snapshot Lock).

---

## Decision Summary

| # | Decision | Choice | Rationale |
|---|---|---|---|
| D1 | Reference version | **python-oracledb v4.0.1** (tag, 2026-05-19) | Latest stable; v4.0 added thin CQN/AQ-notifications + end-user security context; suite is pytest-based |
| D2 | Conformance harness | **PyO3 shim replacing `oracledb.thin_impl`** under the vendored, unmodified python-oracledb public layer; the reference's own pytest suite then drives the Rust engine | Public layer is ~60% pure delegation to `self._impl` (connection.py:879-896, cursor.py:54); impl boundary is the smallest stable seam; tests assert only on the public API |
| D3 | What stays Python in the harness | Public layer (`src/oracledb/*.py`) + compiled `base_impl` (params objects, DPY error machinery) | Harness-only plumbing. Anything semantically wire-relevant must execute in Rust; see Fake-Parity Guard |
| D4 | Rust architecture | 3-crate workspace: sans-io `oracledb-protocol` core + `oracledb` driver (asupersync I/O) + `oracledb-pyshim` (harness-only cdylib) | Sans-io core = fuzzable, deterministic, runtime-independent; matches asupersync guidance (runtime at boundary only) |
| D5 | Async runtime | **asupersync 0.3.4** (crates.io), `&Cx`-first APIs, rustls driven sans-io; **no tokio anywhere** | Locked by plan.md guideline #2; crate confirmed published |
| D6 | Scope additions vs plan.md | **Defer dataframe/Arrow modules** (test_8000/8100/8900/9000/9100/9200/9300/9400, ~8k LOC) and **direct path load** (test_9600/9700) | Arrow/PyCapsule is a Python-ecosystem integration, not thin protocol; DPL exists to feed it. plan.md grants planning-session this call ("include only if cheap, else defer") — it is not cheap. Revisit post-goal with arrow-rs |
| D7 | Edge features kept IN | **Pipelining** (test_7600) and **sessionless transactions** (test_8700/8800) | Both are protocol-core TTC work on framing/auth the port needs anyway; 23ai Free container supports both |
| D8 | cwallet.sso | Build it as a **value-add beyond the reference** with our own Rust tests | Verified: thin mode reads only `ewallet.pem` (impl/thin/transport.pyx:161-174); cwallet.sso appears only in thick-mode docs. The reference suite cannot cover it — our tests must |
| D9 | Local database | `gvenzl/oracle-free` (23ai Free, FREEPDB1); image `23-slim` already pulled locally | The goal's ONLY database; suite + schema scripts confirmed compatible |
| D10 | Definition of done | Filtered reference suite (62 modules, ~30.5k LOC) **matches-or-beats the recorded python-oracledb baseline manifest** on the same container, plus M1 identity gate + gauntlet certification | "Green" alone is ill-defined (env-dependent skips); differential vs baseline is objective |

## Source-Of-Truth Ladder

1. **Live behavior** of python-oracledb thin v4.0.1 against the local 23ai container (the baseline manifest + differential runs).
2. **Pinned reference source** (commit below) — impl/thin, impl/base.
3. **Reference test-suite assertions** (tests/ + conftest.py).
4. Oracle documentation (data type semantics, error codes).
5. Secondary thin implementations as tiebreakers only: node-oracledb v7 (github.com/oracle/node-oracledb), go-ora v2.9 (github.com/sijms/go-ora).
6. Labeled inference.

Oracle does **not** publish the TNS/TTC wire protocol; the reference source *is* the spec.

## Source Root Inventory

| Root | Pin | Size | Role |
|---|---|---|---|
| `reference/python-oracledb` (gitignored) | tag v4.0.1 = commit `3daef052904e41668bb862e6fa40f43c22a81beb` (2026-05-19) | see below | Clean-room reference + vendored harness public layer + the conformance test suite |
| `src/oracledb/impl/thin/*.pyx` | — | 9,275 LOC | Protocol state machine, transport, connection, pool, cursor, LOB, DbObject |
| `src/oracledb/impl/thin/messages/*.pyx` | — | 5,832 LOC | One class per TTC message (26 files) |
| `src/oracledb/impl/base/*.pyx` | — | 10,054 LOC | Buffers, encoders/decoders, NUMBER/OSON/VECTOR codecs, connect params + tnsnames/EZConnect parsers, metadata |
| `src/oracledb/*.py` (public layer) | — | ~16,900 LOC | Stays Python in the harness (D3); ~60% delegation |
| `tests/test_*.py` + conftest.py + sql/ | — | 87 modules, 45,536 LOC | The conformance oracle |

**Source Snapshot Lock:** re-pin with `git -C reference/python-oracledb checkout v4.0.1`. The
`scripts/pin-reference.sh` task (T0.0) makes this reproducible (clone if absent, fetch tags,
checkout, verify commit hash). Any version bump is an operator decision recorded in this file.

## Business Capability Map (what the port must preserve)

| Capability | Reference evidence | Proof |
|---|---|---|
| Connect/handshake/redirect, proto v300–319 negotiation | impl/thin/protocol.pyx, capabilities.pyx (TNS_VERSION_DESIRED=319, MIN=300) | test_1100, M1 gate |
| Auth: 11g (SHA1) + 12c (PBKDF2-SHA512) verifiers, negotiated from AUTH_VFR_DATA; password change; token/IAM hooks | messages/auth.pyx:82-154, 221-273; crypto.pyx:118-155 | test_1100, test_1000 |
| **Identity masquerade: caller-set program/machine/osuser/terminal/driver_name** | auth.pyx:379-407 (AUTH_PROGRAM_NM, AUTH_MACHINE, AUTH_SID, AUTH_TERMINAL KVPs); defaults in base/defaults.pyx:31-55 | **M1 gate: set values, assert v$session reflects them; test_1100 (existing osuser/program assertions ~lines 750-782)** |
| Proxy auth `user[schema]` | connect_params parse_user; PROXY_CLIENT_NAME KVP | test_1100 proxy tests |
| Execute/fetch/binds (positional+named, IN/OUT, array DML, DML RETURNING, REF CURSOR, implicit results, prefetch, statement cache) | messages/execute.pyx, fetch.pyx; statement.pyx, statement_cache.pyx | test_1300–5200 series |
| Full scalar type set incl. lossless NUMBER, charsets (UTF-8/UTF-16LE), DATE/TS/TZ family, intervals, RAW/LONG/LONG RAW, ROWID/UROWID, FLOAT/DOUBLE, BOOLEAN | base/encoders.pyx, decoders.pyx, buffer.pyx:159-227 (null/length rules) | test_1400–2600, 2900, 3100, 4800, 4900, 7100 |
| LOB/CLOB/NCLOB/BLOB/BFILE incl. locators, chunked ops | impl/thin/lob.pyx, messages/lob_op.pyx | test_1900, 5700 |
| JSON/OSON (incl. 21c+ native), VECTOR (dense/binary/sparse 23.4/23.5/23.7) | base/oson.pyx (875 LOC), base/vector.pyx, packet.pyx:453-616 | test_3500, 6700, 6900, 6400, 6500, 7500, 7700 |
| Object types & collections (DbObject), XMLType | impl/thin/dbobject.pyx, dbobject_cache.pyx (767 LOC type-metadata cache) | test_2300, 5600 |
| Pooling + DRCP (purity, cclass, session release) | impl/thin/pool.pyx (1,097 LOC), messages/session_release.pyx | test_2400, 4700, 5500 |
| Net naming: EZConnect+, full DESCRIPTION/ADDRESS_LIST, **tnsnames.ora incl. IFILE**, sqlnet params, retry/failover | base/connect_params.pyx (1,198 LOC), base/parsers.pyx (1,036 LOC) — confirmed thin-mode | test_4500 (1,756 LOC), test_7200 |
| TCPS/TLS, ewallet.pem, ssl_server_cert_dn matching, SNI encoding | impl/thin/transport.pyx:127-185, crypto.pyx:85-115 | test_4500 + M3 TCPS-on-container gate |
| Async-first API + pipelining (v319 END_OF_RESPONSE framing) | protocol.pyx:580-1043; packet.pyx data flags 0x0800/0x1000/0x2000 | 30 async modules; test_7600 |
| Sessionless transactions (23.6+) | v4 TTC support | test_8700/8800 |
| DPY-####/ORA-#### error surfaces | errors.py XREF tables (stays Python; Rust errors must map into it losslessly) | test_1700, 6800 |

## Target Rust Architecture

```
rust-oracledb/                       (cargo workspace)
├── crates/
│   ├── oracledb-protocol/           # sans-io core. #![forbid(unsafe_code)]. Zero I/O, zero runtime deps.
│   │   ├── packet/                  #   8-byte header framing, chunked reads, data flags, marker/control
│   │   ├── capabilities/            #   compile/runtime caps, version negotiation v300–319
│   │   ├── messages/                #   TTC messages: connect, protocol, dtypes, auth, fast_auth, execute,
│   │   │                            #   fetch, lob_op, commit, rollback, ping, logoff, session_release,
│   │   │                            #   end_pipeline, tpc-free sessionless txn
│   │   ├── auth/                    #   O5LOGON/11g + 12c verifiers, AES-CBC, PBKDF2 (RustCrypto)
│   │   ├── types/                   #   NUMBER (lossless), datetime family, charsets, ROWID/UROWID,
│   │   │                            #   OSON, VECTOR, DbObject pickle format, BOOLEAN, intervals
│   │   └── net/                     #   EZConnect+/DESCRIPTION/tnsnames(IFILE)/sqlnet parsers,
│   │                                #   wallet readers (ewallet.pem + cwallet.sso value-add)
│   ├── oracledb/                    # the driver crate (publishes to crates.io)
│   │   ├── transport (asupersync TCP + rustls ClientConnection driven sans-io; &Cx-first)
│   │   ├── connection / cursor / lob / dbobject / pool (async-native; region-owned tasks,
│   │   │   cx.checkpoint() in fetch/pool loops, cancel-correct two-phase effects)
│   │   └── blocking facade (RuntimeHandle bridge; what the sync shim half uses)
│   └── oracledb-pyshim/             # PyO3 cdylib masquerading as oracledb.thin_impl. NOT published.
└── harness/
    ├── pin-reference.sh, container.sh (gvenzl up/down/health, user setup)
    ├── shim_inject/                 # pytest plugin: sys.modules["oracledb.thin_impl"] = oracledb_pyshim
    │                                # BEFORE `import oracledb` (works because `from . import thin_impl`
    │                                # consults sys.modules first); shim exposes init_thin_impl(pkg) +
    │                                # ThinConnImpl/AsyncThinConnImpl/ThinPoolImpl/AsyncThinPoolImpl + co.
    ├── filter.txt                   # the exact exclusion list (below)
    └── run.sh                       # baseline | rust | diff  (see Harness Mechanics)
```

**Anti-source-shaped-Rust rule:** the reference's *file* layout is evidence, not architecture.
Crate boundaries above follow concepts (framing / capabilities / messages / types / net / driver).
Cython idioms (e.g. the buffer-class method-per-type style, `cdef` inheritance lattice) must not
leak into Rust; use enums + codec traits + typestate where natural.

**Sync/async:** the core is async-only. The reference's dual sync/async stack collapses into one
async implementation plus a thin blocking facade — the suite's 57 sync modules run through that
facade via the shim, which itself proves the bridge.

## Harness Mechanics (the convergence engine — build FIRST)

1. **Vendor & build the reference once** (T0.0–T0.2): pin checkout; `uv venv`; `pip install ./reference/python-oracledb[test]`
   (builds real `base_impl`/`thin_impl`/`thick_impl`); start container; create schema
   (`pytest tests/create_schema.py` with `PYO_TEST_*` env).
2. **Record the baseline manifest** (T0.2): run the filtered suite with *real* python-oracledb thin
   against the container → JSON manifest of per-test pass/fail/skip. This is the objective target:
   *our* run must match-or-beat it (same passes; skips allowed only where baseline skips).
3. **Shim injection** (T0.4): pytest `-p shim_inject` substitutes `oracledb_pyshim` for
   `oracledb.thin_impl` before package import. Required exports (grep `thin_impl\.` in the public
   layer for the authoritative list): `init_thin_impl`, `ThinConnImpl`, `AsyncThinConnImpl`,
   `ThinPoolImpl`, `AsyncThinPoolImpl` (+ whatever that grep reveals). Async impl methods bridge
   asupersync futures into asyncio via threadsafe wakeups; sync methods use the blocking facade.
4. **The loop:** `harness/run.sh rust` → red tests → fix Rust → repeat. Module-by-module
   (M1: test_1100 → M2: type/cursor series → …). Every milestone gate names its module set.
5. **Differential extras** (beyond the suite): identity-masquerade assertion script
   (connect with chosen osuser/program/machine → query v$session), connect-string corpus
   differential (see Fake-Parity Guard), golden wire captures via `PYO_DEBUG_PACKETS=1`
   (transport.pyx:32) replayed into the sans-io core (mask random session keys/nonces).

### Fake-Parity Guard

A suite-green claim is invalid where the green never crossed the shim boundary:

| Risk | Guard |
|---|---|
| test_4500/test_7200 (connect-string/tnsnames parsing) exercise *base_impl's* Python parser, not ours | Corpus differential runner: extract every connect string those tests construct; parse with `oracledb-protocol::net` AND with base_impl; diff the resolved parameter trees. Gate of M3 |
| Pure-Python param/validation tests passing trivially | Per-module annotation in `harness/filter.txt`: `crosses-shim: yes/no/partial`; `no/partial` modules get a named compensating Rust-native or differential test |
| Shim implementing behavior in Python instead of delegating | Shim code-review rule: shim contains marshalling ONLY — no SQL strings, no protocol logic, no type math; `fake_parity_scanner` / mock-code-finder sweep before each milestone claim |
| Hardcoded/short-circuited impl methods to pass a test | Same sweep + adversarial review at M5 (gauntlet) |

### Exclusion filter (exact, evidence-checked)

Excluded **modules** (everything else runs; individually thick-gated tests inside in-scope modules
self-skip via `skip_unless_thick_mode` — verified, e.g. test_2000_long_var.py:126 gates only one test):

| Group | Modules | Why |
|---|---|---|
| AQ | 2700, 2800, 7800, 7900, 8200, 8300, 8400, 8500 | plan.md hard-exclusion |
| SODA | 3300, 3400 | plan.md hard-exclusion (thick-only anyway) |
| XA/TPC | 4400, 7400 | plan.md hard-exclusion |
| CQN/subscription | 3000 | plan.md hard-exclusion (thin support is new in 4.0; still out) |
| External OCI | 9800 | thick-only by nature |
| Dataframe/Arrow | 8000, 8100, 8900, 9000, 9100, 9200, 9300, 9400 | D6 deferral |
| Direct path load | 9600, 9700 | D6 deferral |
| Sharding | (no test modules exist in v4.0.1) | nothing to filter |

**In scope: 62 modules, ~30.5k LOC** (incl. all 1000–7700-series core, pipelining 7600,
sessionless 8700/8800, scrollable-async 8600, LONG 2000, tnsnames 7200, vector 6400/6500/7500/7700).

## Primitive Replacement Map (key rows)

| Legacy primitive | Rust primitive | Evidence |
|---|---|---|
| Cython ReadBuffer/WriteBuffer fused to sockets (packet.pyx) | Cython packet buffers become a sans-io frame codec over owned byte buffers; transport feeds it, never the reverse | packet.pyx:261-735 |
| Python `ssl` module + ewallet.pem | `rustls::ClientConnection` driven sans-io on asupersync TCP; PEM loader; custom DN-match (reference disables hostname check, matches DN itself — transport.pyx:179, crypto.pyx:85-115); SNI string format `S{len}.{service}.V3.{ver}` (transport.pyx:47-59) | transport.pyx:127-185 |
| `cryptography` package (AES-CBC, PBKDF2-HMAC-SHA512, SHA1/512, MD5) | RustCrypto: `aes`+`cbc`, `pbkdf2`, `sha1`, `sha2`, `md-5` | crypto.pyx:118-155 |
| asyncio/sync dual protocol classes | single async core + blocking facade (asupersync RuntimeHandle) | protocol.pyx:580-1043 |
| Oracle NUMBER ↔ Python int/float/Decimal | dedicated `OracleNumber` (20-byte packed codec) with lossless string/`Decimal`-equivalent roundtrip; conversion to f64/i64 only on request | base/encoders.pyx, test_2200 |
| OSON tree codec (oson.pyx, 875 LOC) | OSON encoder/decoder with serde_json::Value-level API + field-name dictionary compression | base/oson.pyx |
| DbObject type cache (dbobject_cache.pyx) | OID-keyed metadata cache behind asupersync sync primitives, region-scoped | dbobject_cache.pyx |
| getpass/socket/sys defaults for identity | std-only equivalents, 30-char sanitize (utils.pyx) — and **first-class overridability** (the differentiator) | base/defaults.pyx:31-55 |
| DPY error machinery (errors.py) | Rust error enum carrying DPY code + ORA code + offsets; shim raises so the vendored errors.py XREF produces byte-identical messages | errors.py:424-587 |
| tnsnames.ora/IFILE/sqlnet parsing | `oracledb-protocol::net` parser with include resolution; differential-tested (Fake-Parity Guard) | base/connect_params.pyx:459, v4.0.1 IFILE fix |

## Verification Strategy Matrix

| Surface | Strategy |
|---|---|
| Whole driver | Filtered reference suite via shim vs **baseline manifest** (the oracle) |
| Identity masquerade | M1 container assertion script + test_1100 existing assertions |
| Wire codecs (packet framing, NUMBER, OSON, VECTOR, DSN parsers) | cargo-fuzz targets (fail-closed parsing) + golden wire captures (PYO_DEBUG_PACKETS) + unit vectors extracted from reference |
| Connect-string/tnsnames | corpus differential vs base_impl (Fake-Parity Guard) |
| Cancellation/timeouts/pool | asupersync LabRuntime deterministic tests (no real DB needed) |
| Sync facade | the 57 sync suite modules themselves |
| asyncio bridge | the 30 async suite modules |
| cwallet.sso (value-add) | own Rust tests with a generated wallet; **experimental flag if format risk materializes** (open question Q1) |
| Performance | honest criterion benches vs python-oracledb thin AND rust-oracle (thick): connect, single-row, bulk fetch, LOB, executemany — published with methodology |
| Metamorphic | same ops via sync facade / async / pipelined batch must produce identical results |

## Port Task Graph (milestones, gated)

**M0 — Harness foundation (build the oracle before the engine):**
- T0.0 `scripts/pin-reference.sh` (clone+pin v4.0.1, verify hash)
- T0.1 `harness/container.sh`: gvenzl/oracle-free up/health (`DATABASE IS READY TO USE`), FREEPDB1, PYO_TEST_* env, schema create/drop
- T0.2 **Baseline manifest**: real python-oracledb thin, filtered suite, JSON results (also proves env)
- T0.3 Cargo workspace scaffold; gates: `cargo fmt --check`, `clippy -D warnings`, nextest, `#![forbid(unsafe_code)]`
- T0.4 Shim skeleton importable as `oracledb.thin_impl`; suite collected & failing honestly (red loop running end-to-end)
- T0.5 asupersync spike: TCP echo + rustls handshake under LabRuntime (de-risk D5)
- **Gate:** `harness/run.sh baseline` and `harness/run.sh rust` both execute end-to-end (latter all-red).

**M1 — Connect + auth + identity (the riskiest piece, proven first):**
packet framing, CONNECT/ACCEPT/REDIRECT, capabilities v319, auth phase1/2 (12c verifier first, 11g second), marker/break, ping/logoff, basic execute of `SELECT 1`.
- **Gate:** identity script green (set osuser/program/machine/terminal → v$session reflects) + `test_1100_connection` matches baseline.

**M2 — Execute/fetch/binds/core types + LOB/LONG:**
statement cache, bind/define, array DML, DML RETURNING, REF CURSOR, implicit results, all scalar codecs, LOB locators+ops, LONG/LONG RAW chunking.
- **Gate:** modules 1000–2600 (minus exclusions), 2900, 3100, 3200, 3500, 3600–5200 match baseline.

**M3 — TLS/wallet/net-naming:**
TCPS against the container (self-signed cert config), ewallet.pem, DN match, tnsnames/IFILE, corpus differential green; cwallet.sso value-add; optional manual ADB check (operator-run, not in goal).
- **Gate:** test_4500/test_7200 match baseline + corpus differential green.

**M4 — Objects/JSON/VECTOR/pool/async breadth:**
DbObject+collections+XMLType, OSON/JSON 23ai, vectors (dense/binary/sparse), pooling+DRCP, pipelining, sessionless txns, all 30 async modules, error parity (1700/6800).
- **Gate:** full in-scope 62-module run matches baseline except a named, shrinking red list.

**M5 — Gauntlet & certification:**
full filtered suite matches-or-beats baseline; fuzz corpora clean; fake-parity sweep; perf benches published; `/running-the-gauntlet-on-your-rust-port` certification; release scorecard vs rust-oracle gap matrix (identity, proxy, cwallet.sso, tnsnames, DN match, objects, LONG/XMLType/BFILE).
- **Gate:** claim contract below satisfiable with evidence artifacts.

## Runtime Proof Readiness

No blocked handoff: the goal's only database is the **local disposable container**
(image already pulled). The autonomous agent gets only that container's connect string —
no production/remote credentials ever. Schema setup needs the container's admin password
(set by `ORACLE_PASSWORD` env at `docker run`; SYSTEM user), which the harness scripts own.
The post-goal production-snapshot acceptance stays operator-run per plan.md §"After the goal".

## Claim Contract

- **Allowed after M5:** "Passes python-oracledb v4.0.1's own thin-mode test suite — 62 of 87
  modules; AQ, SODA, XA/TPC, CQN, sharding, dataframe/Arrow and direct-path-load excluded —
  against Oracle Database 23ai Free, verified differentially against python-oracledb itself."
- **Forbidden ever:** "drop-in", "production-ready", "certified by Oracle", unqualified "full parity".
- README must carry: independent-project disclaimer, NOTICE crediting python-oracledb (UPL/Apache),
  dated known-gaps list (the exclusions + cwallet.sso status).

## Open Questions

| # | Question | Recommendation / fallback | Blocks |
|---|---|---|---|
| Q1 | cwallet.sso (proprietary SSO obfuscation) parseable cleanly? | PEM is the supported path; SSO behind `experimental` flag if format risk materializes; go-ora has prior art | M3 value-add only |
| Q2 | DRCP available on gvenzl Free image for test_2400 DRCP paths? | conftest auto-detects (`skip_if_drcp` etc.); baseline manifest decides — whatever baseline does, we match | nothing (self-resolving) |
| Q3 | asupersync 0.3.4 maturity for socket+TLS workloads | T0.5 spike in M0; escalate to operator if a runtime gap is found (do NOT silently fall back to tokio) | M1 |
| Q4 | asyncio↔asupersync bridge under pytest-anyio | Standard threadsafe-wakeup pattern in T0.4 skeleton; measure early | M1 |
| Q5 | No git remote configured (plan.md names github.com/MuhDur/rust-oracledb) | **Operator action:** create repo + `git remote add origin …`; until then, work commits locally | session-end push only |
| Q6 | XMLType fetch semantics in thin (returns str/CLOB) | Port reference behavior exactly (packet.pyx:618-636) | M4 |

## The Codex Goal (copy-ready, supersedes plan.md draft)

> Build **`oracledb`** — a production-grade pure-Rust **async (asupersync 0.3.4, never tokio)**
> thin-mode Oracle Database driver with zero OCI/Instant Client, as a clean-room port of
> python-oracledb v4.0.1's thin engine (reference pinned at commit
> `3daef052904e41668bb862e6fa40f43c22a81beb` under `reference/`, gitignored; UPL/Apache,
> attributed in NOTICE; dual MIT/Apache; "not affiliated with Oracle").
> Follow `PLAN_TO_PORT_PYTHON_ORACLEDB_THIN_TO_RUST.md` in the repo root exactly: 3-crate
> workspace (sans-io `oracledb-protocol` + asupersync-I/O `oracledb` + harness-only
> `oracledb-pyshim`), `#![forbid(unsafe_code)]`, fail-closed parsing.
> **Build the harness first (M0):** vendor the reference, start the local disposable
> `gvenzl/oracle-free` container (the ONLY database — no remote/production connection ever),
> record the baseline manifest by running the filtered reference pytest suite (62 modules per
> `harness/filter.txt`; AQ/SODA/XA-TPC/CQN/sharding/dataframe/direct-path excluded) with real
> python-oracledb thin, then inject the PyO3 shim as `oracledb.thin_impl` and iterate:
> build → run filtered suite → fix red → repeat, milestone gates M1→M5, until the Rust-backed
> run **matches-or-beats the baseline manifest** — covering caller-set
> program/osuser/machine/terminal identity (M1 gate: assert v$session reflects chosen values),
> proxy auth `user[schema]`, 11g+12c verifiers, TCPS+ewallet.pem (+cwallet.sso value-add with
> own tests), tnsnames/EZConnect/IFILE parsing (with the anti-fake-parity corpus differential),
> and the full in-scope type set (scalars, lossless NUMBER, LOB, LONG, RAW, ROWID, JSON/OSON,
> VECTOR incl. binary+sparse, BOOLEAN, object types/collections, XMLType, BFILE, intervals),
> pooling+DRCP, pipelining, sessionless transactions, sync facade + asyncio bridge.
> Enforce the Fake-Parity Guard for every milestone claim. Then certify with
> `/running-the-gauntlet-on-your-rust-port`: differential parity vs python-oracledb AND
> rust-oracle, fuzz corpora clean, honest published perf. Keep the repo fully generic — never
> hard-code any deployment-specific name, host, schema, or identity. Track all work as beads
> (`br`), commit `.beads/` with code.
