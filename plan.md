# rust-oracledb / crate `oracledb` — Rust thin-mode Oracle driver port plan

**Purpose:** feed this whole file to a planning session, then convert it to a single
autonomous codex goal. It is self-contained and free of any deployment-specific detail —
the implementing agent must keep the repo generic and never hard-code environment specifics.

## Identity (locked)

- **Repo:** `github.com/MuhDur/rust-oracledb` (parallels Oracle's `oracle/python-oracledb`).
- **Crate:** `oracledb` (matches python-oracledb / node-oracledb package names).
- **Description:** "Pure-Rust async Oracle Database driver — a clean-room port of Oracle's
  thin-mode protocol (the engine behind python-oracledb), with no OCI/Instant Client."
- **License:** dual MIT OR Apache-2.0. **NOTICE** credits python-oracledb (UPL/Apache).
- **Disclaimer (README):** "Independent community project; not affiliated with Oracle."
- **Keywords:** `["oracle","database","driver","async","tns"]`; **categories:**
  `["database","asynchronous"]`.

## Why thin mode (the differentiator)

Thick-mode drivers (rust-oracle/ODPI-C) derive the client-reported `v$session` identity
fields — `PROGRAM`, `OSUSER`, `MACHINE`, `TERMINAL` — from the real OS process and cannot
override them. Locked-down enterprise Oracle deployments commonly enforce **logon/DDL
triggers, auditing, or VPD policies keyed on those client-reported fields** (e.g. only
sessions presenting a registered application identity may connect or write). A thick driver
cannot satisfy such policies; a **thin** driver, which builds the TNS login packet itself,
sets those fields freely and passes. **No existing Rust driver offers this** — it is the
decisive capability this port provides.

Setting these identity fields is therefore a **core, first-class, in-goal feature**: it is
built and verified within the autonomous goal on the local container (M1 + the differential
harness — connect with a chosen `osuser`/`program`/`machine` and assert `v$session` reflects
it). The later production-snapshot check (post-goal) does not add the capability — it only
exercises the already-delivered capability against a real login policy.

## The "better idea" (what makes one autonomous goal converge)

Done is a **self-verifying differential conformance harness**, not "port it." The harness
runs identical operations through **python-oracledb thin** (reference) and `oracledb`
against a live Oracle and asserts identical results. The agent loop = "make the harness
green." Build the harness FIRST. Reserve `/running-the-gauntlet-on-your-rust-port` for the
final decisive certification.

## Scope = comprehensive parity, made objective

**Port everything in python-oracledb thin mode EXCEPT the excluded list below.** The
mechanism that guarantees full capture: **the conformance harness is python-oracledb's own
thin `tests/test_*.py` suite (~51k LOC) with the excluded-feature modules removed.** Green
against that filtered suite = feature-complete-minus-exclusions by construction — you
cannot pass it while skipping an in-scope feature. This is also how you surpass oracle-rs
decisively: it has never been run against the reference's full suite.

## Guidelines

| # | Dimension | Precise guideline |
|---|---|---|
| 1 | North star | Pure-Rust **async** thin Oracle driver; zero OCI/ODPI-C/Instant Client. |
| 2 | **Async runtime** | **asupersync** (native Cx/region model), NOT tokio — use the `asupersync-mega-skill`. TLS via rustls (sans-io); TCP on asupersync net. The I/O layer is a from-scratch asupersync design (deepest deviation — lock it first). |
| 3 | Reference | Clean-room port of python-oracledb `src/oracledb/impl/thin/*.pyx` (UPL/Apache; attribute in NOTICE). |
| 4 | **Definition of done** | The **filtered python-oracledb thin test suite** runs 100% green through the differential harness (see Testing). |
| 5 | **The differentiator** | Caller-settable `program`/`osuser`/`machine`/`terminal`/`driver_name` at connect — the identity masquerade thick mode can't do (the whole reason). |
| 6 | Proxy auth | `user[schema]` proxy connect (authenticate as one user, operate in another schema). |
| 7 | Auth | 11g (O5LOGON/SHA-1) + 12c (PBKDF2-HMAC-SHA512) verifiers, negotiated from `AUTH_VFR_DATA`; OCI-IAM token optional. |
| 8 | TLS | TCPS + mutual TLS via rustls (client cert/key from wallet). |
| 9 | Wallet | Read **cwallet.sso** (SSO) + ewallet.pem; `ssl_server_cert_dn` match → unmodified OCI ADB wallet works as-is. |
| 10 | Net naming | Parse `tnsnames.ora`/`sqlnet.ora`/`TNS_ADMIN` + EZConnect-Plus + full DESCRIPTION/ADDRESS_LIST. |
| 11 | TTC protocol | Version negotiate 12.1→23ai+; connect/accept/redirect, auth, data-types, execute, fetch, lob, commit, rollback, ping, logoff. |
| 12 | Bind/fetch | Positional + named binds, IN/OUT/INOUT, array/batch DML, DML RETURNING, REF CURSOR, implicit results, prefetch, statement cache. |
| 13 | Data types | NUMBER (lossless), DATE/TS/TSTZ/LTZ/interval, (N)VARCHAR2/CHAR, CLOB/NCLOB/BLOB/BFILE, RAW/LONG RAW, LONG, ROWID/UROWID, BINARY_FLOAT/DOUBLE, JSON/OSON, VECTOR(23ai), BOOLEAN, **object types + collections (DbObject)**, XMLType. |
| 14 | Pooling | Native async session pool + DRCP (`connection_class`, `purity`). |
| 15 | Safety | `#![forbid(unsafe_code)]`; fail-closed packet/codec parsing; cargo-fuzz the wire decoder. |
| 16 | Integration | Standalone crate/repo; downstream consumers plug it behind their own connection trait via a thin adapter (async→sync bridge first, async-native later). |
| 17 | Parity proof | Differential vs python-oracledb **and** vs rust-oracle thick + metamorphic + golden + fuzz. |
| 18 | **Decisive win** | Pass the reference's own (filtered) suite + beat oracle-rs on the gap matrix (identity, proxy auth, cwallet.sso, tnsnames, DN match, objects, LONG/XMLType/BFILE, maturity) + publish honest perf vs both. |
| 19 | Autonomy loop | build → run filtered suite/harness → fix reds → repeat until green + gauntlet-certified; the suite is the oracle, not human review. |

## Milestones (de-risk the masquerade first; each gated by a slice of the suite)

| M | Gate |
|---|---|
| M1 | Connect + 12c auth + **identity fields settable** + `SELECT 1` → set `osuser`/`program` and assert `v$session` reflects them **on the local container** (riskiest piece, proven early). `test_1100_connection` green. |
| M2 | execute/fetch + binds + core scalar types (number/string/datetime/rowid/boolean) + LOB + LONG. `test_1300..2200`, `2500..3100` green. |
| M3 | TLS + cwallet.sso + tnsnames → live OCI ADB connect (personal instance only). |
| M4 | JSON/OSON/VECTOR + **object types/collections** (`test_2300_object_var`) + DRCP/pooling (`test_2400_pool`) + XMLType/BFILE. |
| M5 | Gauntlet: filtered suite 100% green on the local 23ai container, differential parity vs python-oracledb + rust-oracle, honest perf, release scorecard. (Goal reached here — no external database involved.) |

## Testing strategy

**The codex goal is verified ENTIRELY on a local disposable database.** No production or
remote database is touched during the goal.

1. **THE GOAL'S ONLY DATABASE — local disposable Oracle 23ai Free (Docker)**
   (`gvenzl/oracle-free` or `container-registry.oracle.com/database/free`). The filtered
   python-oracledb suite + the differential harness run **here**, full freedom
   (create/drop tables, recreate the container). The masquerade is proven here too: set
   `osuser`/`program`/`machine`, connect, and assert `v$session` reflects them — no special
   environment needed to prove the differentiator. **The autonomous agent gets ONLY this
   container's connect string — no production credentials, no remote database, ever.**
2. **OPTIONAL — personal OCI Always-Free ADB** for the wallet/TLS path.

## Excluded (defer — the suite filter removes these test modules)

Hard-excluded now (drop the matching `test_*.py` modules from the harness):
- **AQ** — Advanced Queuing (in-DB message queues). `test_2700_aq_*`, `test_2800_aq_*`.
- **SODA** — JSON document API. `test_3300_soda_*`.
- **XA / TPC** — distributed two-phase-commit transactions. tpc tests.
- **CQN / subscriptions** — DB-pushed change notifications. `test_3000_subscription`.
- **Sharding** — cross-DB partitioning by shard key.

Planning-session decides (edge features — include only if cheap, else defer): pipelining,
direct-path load, Application Continuity / TAF, advanced AQ types. Object types,
DRCP/pooling, BFILE, LONG, XMLType are **in scope** (later milestones, not early blockers).

## The codex goal (copy-ready)

> Build **`oracledb`** (repo `MuhDur/rust-oracledb`), a production-grade pure-Rust **async
> (asupersync, not tokio)** thin-mode Oracle driver with no OCI/Instant Client — a
> clean-room reimplementation of Oracle's thin TNS protocol (the engine behind
> python-oracledb; UPL/Apache, attributed; dual MIT/Apache; "not affiliated with Oracle").
> **Done = python-oracledb's own thin `tests/test_*.py` suite, with the AQ/SODA/XA-TPC/
> subscription/sharding modules removed, runs 100% green through a differential harness
> (same ops through python-oracledb thin and `oracledb`) against a local disposable Oracle
> 23ai Free container** — covering caller-set `program`/`osuser`/`machine` identity,
> `user[schema]` proxy auth, 11g+12c verifiers, TCPS+cwallet.sso, tnsnames parsing, and the
> full in-scope type set (scalars, LOB, LONG, RAW, ROWID, JSON/OSON, VECTOR, BOOLEAN, object
> types/collections, XMLType, BFILE) — all against the **local disposable container only**.
> Then certify with `/running-the-gauntlet-on-your-rust-port`: differential parity vs
> python-oracledb **and** rust-oracle, plus honest perf. Operate autonomously: build → run
> the filtered suite → fix red → repeat until green + certified. Keep the repo fully
> generic — never hard-code any deployment-specific name, host, schema, or identity.
> **Excluded features:** AQ, SODA, XA/TPC, CQN, sharding. **Out of the goal entirely:** any
> connection to a production or remote database — the agent uses only the local container.

## After the goal is reached (operator-run; NOT part of the codex goal)

Only once the port is complete and 100% green on the local container does **the operator**
(a supervised session, never the unattended agent) run a **bounded acceptance against their
own production snapshot database** — the one check that needs a real environment — to
confirm the identity masquerade satisfies that environment's login/audit policy. Allowed:
- connect to **that one production snapshot database only**; set the client identity
  (`osuser`/`program`/`machine`) its policy expects; run the environment's login/
  registration routine; verify writes are then admitted;
- **table-level CRUD on `PORTTEST_*` scratch tables only**
  (CREATE/INSERT/SELECT/UPDATE/DELETE/DROP TABLE).
- **HARD-FORBIDDEN:** DROP/SHUTDOWN/STARTUP DATABASE, ALTER DATABASE/SYSTEM, DROP USER/
  TABLESPACE, TRUNCATE, anything on non-`PORTTEST_*` objects.
  **Never terminate, delete, drop, or destroy the database — table operations at most.**

This is a separate human sign-off after the autonomous goal is done — not a goal milestone,
never run unattended, and any environment-specific values stay with the operator, never in
the repo.

## Planning-session note

Nail three things in planning: (1) the **differential harness** that drives python-oracledb
and `oracledb` side-by-side and diffs (the convergence engine, and the lever that ports
"everything" via the reference's own tests); (2) the **asupersync I/O abstraction** (deepest
deviation from the reference); (3) the **disposable-container-only** rule for the autonomous
run, with a scratch-prefix allowlist and a single-target lock so no remote database is ever
reachable from the loop. Use `planning-workflow` to expand, `asupersync-mega-skill` for the
runtime, `running-the-gauntlet-on-your-rust-port` to certify.
