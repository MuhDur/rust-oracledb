# rust-oracledb — Road to 1.0

> **Status:** planning (v3.1 — GPT-Pro review integrated; R8 resolved to the full
> external contract; internal-consistency pass done). Authored via
> `/planning-workflow`. Repository facts/`file:line` describe the **reviewed
> baseline**; W0-T1 pins the source commit and generates inventories under
> `docs/baseline/` so a future contributor can tell whether a count changed
> because the code changed or the plan was stale.
> **North Star:** ship a **correctness-hardened, diagnosable, resource-bounded**
> thin-mode Oracle driver. **oraclemcp** is the primary consumer (shipped as a
> binary), but the crate is **publicly usable and carries a full SemVer + support
> contract** (ADR-0002, decision **(b)**, §13): from **0.3.0** onward a blocking
> `cargo-semver-checks` gate prevents *unintended* breaking changes; intentional
> breaks are allowed but require the correct version bump + a baseline update.
> 1.0 is a maturity *and* compatibility milestone. A driver is a bounded domain —
> once parity + extras land and the API is designed deliberately (pre-0.3.0),
> evolution is overwhelmingly **additive** (new types via `#[non_exhaustive]` +
> `as_*` accessors, new methods), which the blocking gate fully allows.

Self-contained: a fresh agent can implement any task without prior context. Tasks
name dependencies, rationale, and **acceptance in terms of observable behavior /
artifacts** (implementation techniques and agent skills are *guidance*, §11, not
gates). Converts to a beads graph. (Provenance notes like "review change N" / "review
N" cite item N of the integrated GPT-Pro review round; they are rationale, not tasks.)

---

## 1. Decisions (ADRs, with review triggers)

Decisions live under `docs/adr/` and stay binding for this roadmap; each lists
objective review triggers. Changing one requires a new ADR.

**ADR-0001 — Keep asupersync + the pinned nightly through 1.0.** oracledb is
oraclemcp's engine; oraclemcp ships a single static binary, so nightly is a
build-time detail invisible to its users. asupersync is an *asset* (cancel-correct;
LabRuntime+DPOR reach a bug class the single-threaded python suite cannot).
*Review triggers:* `try_trait_v2` stabilizes or changes; an equivalent stable path
appears; the pin blocks a security update; external Rust-crate adoption becomes a
product goal.

**ADR-0002 — Full external SemVer + support contract; blocking semver-checks from
0.3.0 (decision (b)).** Because `#[non_exhaustive]`, field-privatization, and method
removal are themselves breaking, the breaking *cleanup* happens in a **0.3.0
migration release**; all first-party consumers (pyshim, oraclemcp) migrate.
**Sequencing (load-bearing — getting it wrong would block the redesign):**
`cargo-semver-checks` runs **advisory** during 0.3.0 development (so the intended
API redesign is *not* blocked); the moment **0.3.0 ships, its public API becomes
the baseline and the gate flips to BLOCKING** for every subsequent release. The
gate guards against *unintended* breaks only — an *intentional* break is fine but
must take the correct bump and update the baseline. **0.x SemVer semantics (what
the gate enforces):** a breaking change is a **minor** bump (0.3.x → 0.4.0); patch
releases (0.3.1) must be non-breaking — so a "must-break" change is 0.4.0, not
0.3.1, and a breaking 0.3.1 is exactly the error the gate catches. At 1.0 the same
machinery continues (1.x additive; 2.0 for a real break). Obsolete shims /
accidental internals are removed before **1.0.0-rc.1**. *Review trigger:* revisit
strictness only if the maintenance cost of the typed dependency bridges (chrono/
uuid/rust_decimal/serde_json — the one recurring involuntary break source, §13)
becomes disproportionate.

**ADR-0003 — Release evidence is per exact candidate SHA.** Scheduled lanes run the
default branch and are discovery, not qualification; the 1.0 gate is a manual
exact-SHA run (§10 gating summary; W4-T2).

### Verified baseline counts (reviewed commit; regenerate via W0-T1)
`crates/oracledb/src/lib.rs`: 159 `pub fn`, 66 `pub async fn`, 19 `execute_query*`
variants (10 async + 9 blocking). 118 public structs/enums across the 3 published
crates. 0 `#[non_exhaustive]`. These are *baseline notes*, not acceptance criteria.

---

## 2. Grounding facts (verified)

- `oracledb-protocol` (wire/codecs): zero asupersync refs, no nightly features,
  `#![forbid(unsafe_code)]` (`oracledb-protocol/src/lib.rs:1`) — sans-io,
  stable-compatible (W0-T3 adds a stable-compiler lane so this advantage can't rot).
- `oracledb` (driver): small asupersync surface (`Cx`/`io`/`net::TcpStream`/
  `tls::TlsStream`/`sync::Mutex`/`runtime`) threaded through 109 `async fn`s; TLS is
  sans-io rustls; asupersync vendored at **0.3.4** (`Cargo.lock:211`).
- `oracledb-pyshim` is `publish=false` (`Cargo.toml:8`) — the python-oracledb
  conformance harness, layered on top of the native crate; bridges Python-async→Rust
  via `spawn_blocking`.
- Releases 0.2.0→0.2.2 via tag-driven `release.yml` + `release_preflight.sh` +
  `publish_crates.sh`. Pin: `rust-toolchain.toml:13` → `nightly-2026-05-11`.
- CI today: only push/PR to `main`; **no** `schedule`/`workflow_dispatch` except
  `release.yml:19`.
- Out of scope: Group-A auth/wallet GH #2/#3/#4/#6 (beads `o0b`/`qm4`/`x1p`) and the
  `57z` "beat-python" features.

---

## 3. Program shape (waves) — architecture before deep qualification

Reordered per the review so deep evidence is collected only *after* the API + private
core stabilize (otherwise fuzz/property/DPOR targets bind to types about to change).

- **Wave 0 — Evidence & CI:** baseline inventories, tiered reusable CI, feature/
  SemVer/stable lanes, ADRs. (§4)
- **Wave 1 — Architecture:** operation-specific public API; type-evolution policy;
  private transport/connector + `ConnectionCore`; session-recovery state machine;
  `ProtocolLimits`; pure pool model; typed errors/disposition + redacted observability.
  (§5)
- **Wave 2 — Migration release:** publish **0.3.0**; migrate pyshim + oraclemcp;
  remove deprecations before RC. (§6)
- **Wave 3 — Qualification:** property, fuzz (manifest), DPOR (on the seam), loom (on
  the pure pool shell), fault/fragmentation matrix, secure cassette replay, live
  support matrix + direct oraclemcp contract suite, perf/resource gates, multi-pass
  hunts — all to exact-SHA bounded evidence. (§7)
- **Wave 4 — Freeze & release:** cut `1.0.0-rc.1`; exact-SHA qualification;
  packaged-source/provenance preflight; publish `1.0.0`. (§8)

Substantial parallelism remains *within* a wave; only outputs that survive into the
candidate are collected deep.

---

## 4. Wave 0 — Evidence & CI

### W0-T1 — Pin baseline + generate inventories
- **What:** record the source commit; generate `docs/baseline/` artifacts — public-API
  listing (per supported profile), enabled-feature matrix, fuzz-target manifest dump,
  test inventory, and the version/pin set. All later counts derive from these, not
  from prose.
- **Deps:** none. **Acceptance:** `docs/baseline/*` committed + regenerable by a script.

### W0-T2 — Tiered, reusable CI (replaces the single nightly workflow)
- **What:** a reusable `_quality.yml` (`workflow_call`, inputs: profile, budget,
  `candidate_sha`) invoked by four tiers:
  - `required.yml` — PR/push deterministic checks (current gates).
  - `canary.yml` — daily at a **non-zero** minute; floating-nightly (ADR-0001 early
    warning) + moderate fuzz/property budgets.
  - `soak.yml` — weekly deep, sharded fuzz/property/model/perf.
  - `release-qualification.yml` — **manual, exact-SHA** full gate (ADR-0003).
- **Correctness fixes from review:** evidence jobs **fail normally** — non-blocking
  is achieved via *branch-protection* selection, **not** `continue-on-error` (which
  makes a job "pass" and defeats a later `if: failure()`). A separate `if: always()`
  reporter inspects `needs.*.result`, runs only on schedule/manual, and creates/updates
  **one fingerprint-deduplicated** issue. Default `permissions: contents: read`; grant
  `issues: write`/attestation only to the reporter/release jobs; pin third-party
  actions to full SHAs; set `timeout-minutes` + `concurrency`. Avoid cron at minute 0.
- **Deps:** W0-T1. **Acceptance:** the four tiers run the same commands at different
  budgets; a forced evidence failure opens exactly one issue; required CI unchanged in
  scope; a scheduled red job does **not** silently pass.

### W0-T3 — Feature-profile + SemVer + stable-protocol lanes (decision (b))
- **What:** define supported profiles in `docs/SUPPORT.md` (minimal/default/all +
  each optional-integration combo — chrono/uuid/serde_json/rust_decimal/arrow/soda);
  exercise the **full** supported matrix with `cargo hack`; install + wire
  `cargo-semver-checks` against the latest published baseline; keep `cargo public-api`
  snapshots per supported profile; build+test **`oracledb-protocol` on current stable**
  so its stable-compatibility can't silently rot.
- **Verify (the one unproven mechanic):** confirm `cargo-semver-checks` runs under the
  pinned nightly. It uses the same rustdoc-JSON path as `cargo public-api`, which the
  grounding research already confirmed works under `nightly-2026-05-11`, so this is expected to
  pass — but it is the one tool not yet run, so verify it explicitly here and record
  the command + result in `docs/baseline/`.
- **Advisory-now / blocking-at-0.3.0 (ADR-0002):** semver-checks runs **advisory**
  through 0.3.0 development so it does not block the intended API redesign (W1-T3);
  it is **flipped to blocking at the 0.3.0 release** (W2-T1), with 0.3.0 as the
  baseline, and stays blocking thereafter.
- **Deps:** W0-T1; pairs with W1 API work. **Acceptance:** the full supported matrix
  compiles/tests; an unsupported combo is documented, not implied by `--all-features`;
  protocol crate green on stable; `cargo-semver-checks` verified runnable under the pin
  and wired advisory.

### W0-T4 — Close `nto` (off the release gate)
- **What:** close the sole in_progress bead with the ADR-0001 rationale (shim async =
  `spawn_blocking` over native futures; `publish=false`; zero release impact). **Not a
  1.0 gate** — PM cleanup is tracked outside the release DAG.
- **Deps:** none. **Acceptance:** `nto` closed; recorded as non-evidence.

---

## 5. Wave 1 — Architecture (the seams everything else rests on)

### W1-T1 — Private transport / connector + `ConnectionCore`
- **What:** introduce private `WireTransport` + `Connector` contracts beneath the
  public `Connection`; route production TCP/TLS, recording, strict replay, and a
  **scripted** transport through one `ConnectionCore<T>`. The scripted transport
  injects short read/write, `Poll::Pending`, EOF, errors, exact expected-writes, and
  virtual-time advances — the one capability that unlocks cassette (W3), DPOR (W3), and
  the fault matrix (W3). Keep deterministic harnesses **crate-local** (access private
  seams); do **not** publish a `connect_over`/`Mock` public surface. Make accidental
  transport-half exports private during 0.3.
- **Why:** one seam for prod + all tests avoids drift and a permanent public test
  surface (supersedes v2's public `Mock` variants / `connect_over`, resolving R6/R7).
- **Deps:** none (foundational). **Skill:** `asupersync-mega-skill`. **Acceptance:**
  a `ConnectionCore` runs connect/execute/fetch over a scripted transport, crate-local,
  zero sockets, no new public API.

### W1-T2 — Session-recovery state machine (cancellation as a contract)
- **What:** an explicit internal machine `Ready → InFlight → BreakSent → Draining →
  Ready|Dead`. Only `Ready` may start an op; each response belongs to exactly one op;
  drain is at-most-once; close is idempotent. BREAK/RESET/drain stay **private**
  (do not expose `drain_cancel_response`). `CancelHandle` only *requests* cancellation;
  the op-completion path reconciles the wire and returns the session `Ready` or `Dead`.
- **Why:** the public contract becomes "every completed op leaves the session reusable
  or dead," which is also the DPOR invariant (W3) and what oraclemcp needs. Supersedes
  v2's "make drain public" symmetry note.
- **Deps:** W1-T1. **Skill:** `asupersync-mega-skill`. **Acceptance:** the machine is
  the single owner of recovery; property/DPOR tests assert the ready-or-dead invariant
  across success/timeout/cancel/error/dropped-future.

### W1-T3 — Operation-specific public API (replaces the mega-builder)
- **Verified sprawl to subsume** (async `Connection`, `crates/oracledb/src/lib.rs`) —
  13 async execute/query entries (= the 10 `execute_query*`-prefixed methods from §1's
  baseline count, plus the `query`/`query_named`/`query_named_with_timeout` ergonomic
  sugar; §1's "19" counts the `execute_query*` prefix across *both* surfaces = 10 async
  + 9 blocking):
  `execute_query` (`:2469`), `_collect` (`:2530`),
  `_with_timeout` (`:2561`), `_with_binds` (`:2578`), `_with_binds_and_timeout`
  (`:2600`), `query` (`:2629`), `query_named` (`:2659`), `query_named_with_timeout`
  (`:2675`), `_with_bind_rows` (`:2694`), `_with_bind_rows_and_options` (`:2711`, the
  real core + ORA-932/1007 refetch `:2736`), `_with_bind_rows_and_timeout` (`:2941`),
  `_with_bind_rows_options_and_timeout` (`:2959`), `execute_query_for_registration`
  (`:1869`); + `BlockingConnection` twins (`:5296`–`:5525`). The fetch/paging family
  (`fetch_rows*` `:3053…`, `for_each_row_ref` `:3281`, `define_and_fetch…` `:3377`,
  `fetch_cursor` `:3418`) **stays** (distinct low-level capability).
- **Design (review change 5 — four operation families over a private `OperationCore`
  dispatch layer atop W1-T1's `ConnectionCore` (the two private layers in §12's
  diagram), NOT one mega-builder):**
  ```rust
  pub async fn query(&mut self, cx: &Cx, req: Query<'_>) -> Result<Rows>;
  pub async fn execute(&mut self, cx: &Cx, req: Execute<'_>) -> Result<ExecuteResult>;
  pub async fn execute_many(&mut self, cx: &Cx, req: Batch<'_>) -> Result<BatchResult>;
  pub async fn register_query(&mut self, cx: &Cx, req: Registration<'_>) -> Result<RegistrationResult>;
  ```
  Distinct borrowed request/result types per family (no invalid combos: batch rows ≠
  scalar binds; fetch/LOB policy only on `Query`; CQN only on `Registration`). Prefer
  **borrowed** params with owned convenience `From` conversions; relative timeouts use
  **`Duration`** (translated once into the op context/deadline at the engine boundary,
  not a public `timeout_ms`); validated **`NonZero`** sizes. `BlockingConnection`
  mirrors 1:1.
- **Deps:** W1-T1, W1-T2. **Skill:** `code-simplifier`, `oracledb`. **Acceptance:**
  the four families cover every prior capability (old→new mapping table in the PR);
  invalid states unrepresentable; `harness/run.sh diff` stays green (anti-drift guard).

### W1-T4 — Type-evolution policy (calibrated, not blanket `#[non_exhaustive]`)
- **What:** during 0.3, by type class — **options/config** (`ConnectOptions`,
  `ExecuteOptions`-derived request types, `PoolConfig`, `ArrowFetchOptions`): private
  fields + builders + getters; **results/metadata** (`Rows`/`ExecuteResult`/
  `BatchResult`, `ColumnMetadata`, `DecodedObject`, `DbmsOutput`, `BatchServerError`):
  accessor APIs so the representation can change without caller destructuring;
  **open enums** (`Error` `:679`, `SessionlessError` `:890`, `PoolError`,
  `SodaError`, `ConversionError`, `ArrowConversionError`, `NotificationOutcome`,
  `ProtocolError`, `SqlError`, wallet/DN errors): selective `#[non_exhaustive]`;
  **value enums** (`BindValue` `types.rs:444`, `QueryValue`/`QueryValueRef`): add
  stable `as_*`/conversion/visitor APIs **before** `#[non_exhaustive]` so callers
  aren't forced into fragile variant matches; **fixed wire-image structs**: keep
  internal or document as fixed.
- **Why:** review change 6 — blanket `non_exhaustive` makes additions *possible* but
  imposes immediate caller costs; accessor/builder APIs are more usable for 1.x. The
  pyshim is in-workspace so unaffected; the additivity scratch test must be
  **out-of-workspace** to be meaningful. (Calibrated to effort given oraclemcp is the
  consumer — accessor-ize the genuinely-evolvable types; don't gold-plate fixed ones.)
- **Deps:** W1-T3. **Acceptance:** out-of-workspace fixture proves construct/match/
  additive-evolution; adding a field/variant is non-breaking for supported types.

### W1-T5 — `ProtocolLimits` (centralized resource bounds + checked allocation)
- **What:** one `ProtocolLimits` policy used by every decoder/state machine — packet/
  frame size, cumulative response bytes, columns, binds, batch rows, object depth/
  elements, vector dims, LOB chunks, redirect loops, and every length-prefixed
  collection. Validate + checked-arith before allocate/index. Typed `ResourceLimit`
  error (limit name + observed value) with a defined connection disposition
  (some pre-sync → `Ready`; some → `Dead`).
- **Why:** review change 10 — `BoundedReader` closed the per-read OOM class but not
  cumulative size/depth/redirect/repeated-frame classes; an MCP server consumes
  arbitrary server responses.
- **Deps:** W1-T1. **Skill:** `multi-pass-bug-hunting`. **Acceptance:** boundary∓1
  tests in unit/property/fuzz; malformed-input stress stays within documented memory/
  time ceilings; no parser allocates from an unchecked wire length.

### W1-T6 — Structured errors, recovery hints, redacted observability (now foundational)
- **What:** stable inspection — `Error::kind() -> ErrorKind` (network/timeout/cancel/
  protocol/database/conversion/pool/resource-limit), `oracle_code()`,
  `connection_disposition() -> {Reusable, Dead}`, conservative `retry_hint()` (never
  auto-retry non-idempotent ops). Prevalidate bind shape/type where unambiguous.
  Opt-in tracing/pool metrics record phase, duration, statement fingerprint, rows/
  bytes, pool wait, cancel phase, disposition — **raw SQL/binds/credentials excluded
  by default**, covered by redaction tests.
- **Why:** review change 17 — promoted from "optional differentiator" to foundational;
  it's exactly what oraclemcp needs and pairs with W1-T2.
- **Deps:** W1-T2. **Skill:** `oracledb`. **Acceptance:** classification + disposition
  via methods (no display-string parsing); redaction tests prove no secret leakage.

### W1-T7 — Pure pool lifecycle model (before any loom)
- **What:** extract pure bookkeeping from sync/I/O in `pool.rs` (`:154-162`): explicit
  states `Opening/Idle/CheckedOut/Validating/Retiring/Closing/Closed`, counts derived
  from state; run open/ping/close **outside** the lock and feed results back as events;
  specify waiter ordering, acquire-cancel, reap, graceful/force close.
- **Why:** review change 9 — loom-ing the big effectful module directly is expensive +
  opaque, and the v2 invariant `open == free+busy+to_drop` is incomplete mid-transition.
  This gives ordinary deterministic tests a precise oracle and shrinks the later loom
  model; it also keeps network calls out of the critical section (perf).
- **Deps:** none. **Skill:** `multi-pass-bug-hunting`. **Acceptance:** pure model with
  deterministic tests over the full lifecycle; counts derived; effects event-driven.

---

## 6. Wave 2 — Migration release 0.3.0

### W2-T1 — Publish 0.3.0 + migrate first-party consumers
- **What:** ship the W1 API/architecture with old execute names as `#[deprecated]`
  shims; migrate the **~17 pyshim execute call-sites** (`conn.rs:142,184,333,350,368,
  398`; `cursor.rs:1077,1411,1420,1446,1643`; `async_conn.rs:136`; `async_cursor.rs:
  74,85,117,201`; `pool.rs:126`; `subscr.rs:336`) and oraclemcp to the four operation
  families (hand edits, file-by-file per AGENTS.md); keep `harness/run.sh diff` green.
- **Why:** ADR-0002 — give the real downstream one published release to exercise; do
  the breaking cleanup here, not at/after 1.0.
- **Flip the SemVer gate (ADR-0002):** on the 0.3.0 release, snapshot the published
  public API (per supported profile) as the `cargo-semver-checks` **baseline** and
  flip the gate from advisory to **blocking** in required CI. From 0.3.1 onward an
  unintended break fails CI; an intentional break must take a minor bump (0.4.0) and
  refresh the baseline in the same release.
- **Deps:** all of §5. **Acceptance:** 0.3.0 published; pyshim + oraclemcp on the new
  API; conformance suite green; deprecations scheduled for removal pre-RC; semver-checks
  baseline committed and the gate is blocking on `main`.

---

## 7. Wave 3 — Qualification (exact-SHA bounded evidence)

Evidence rules (review change 11): fixed seeds in required CI + rotating seeds in
canary/soak; record tool versions, bounds (CPU/time/state), corpus hashes, target
manifests; severity triage (no open P0/P1, no untriaged finding; P2 needs fix or
signed exception); call a model **exhaustive only** when its finite queue empties
under recorded bounds — else "bounded clean." Wave-3 runs during development are
**discovery** (on moving commits); the single **qualifying** run that must be green on
one exact SHA is the W4-T2 release-qualification on the frozen RC (ADR-0003) — Wave 3
builds the suites, W4-T2 certifies them together.

### W3-E1 — Property round-trips: the `FromSql`/`ToSql` bridge
Codec-layer properties already exist (`thin/proptests.rs`, `tests/codec_properties.rs`,
`number_inline_byte_identical.rs`; proptest dev-dep `Cargo.toml:56`). **Gap:** the
`FromSql`/`ToSql` bridge (`sql_convert.rs`, 17 `FromSql` + 12 `ToSql` incl. feature-
gated chrono/uuid/serde_json/rust_decimal + vector) has only example tests. Encode the
verified asymmetries (`f64/f32`↔`BinaryDouble/Float` carries server-rendered text →
live + tolerance; `String` many-to-one; `Decimal` exact only scale 0..=28; vector
cross-format lossy + Binary/Sparse error; `bool` from Number only 0/1). **Per-property
budgets** (not one global), fixed seeds in required, `PROPTEST_CASES` raised in soak;
commit every minimized shrink. **Acceptance:** pure bridge proptests for every pair +
an `#[ignore]` live matrix in `live_typed.rs`; green at the soak budget; shrinks pinned.

### W3-E2 — Manifest-driven fuzzing
Harness exists (10 cargo-fuzz targets; 4 DoS bugs already fixed; `BoundedReader`;
4 committed regressions; a differential oracle `harness/differential/diff_oracle.py`,
5,944 cases 0 divergences). Fix the stale "8 targets" CI comment (`ci.yml:112`).
- **`fuzz/targets.toml` manifest:** target, owner, parser entry, risk tier, corpus,
  dictionary, max-input/timeout/RSS/malloc limits, lane budgets.
- **9 new targets (verified missing, all `pub`-reachable offline; 1 needs a loop
  driver):** `parse_auth_response` (`auth.rs:255`), `parse_accept_payload`
  (`connect.rs:38`), DbObject image-walk (`DbObjectPackedReader` `dbobject.rs:11`),
  DbObject scalar decoders (`dbobject.rs:371/388/523/537/484`), LOB-op responses
  (`lob.rs:220-326`), sessionless/TPC (`sessionless.rs:106/130/342/396`),
  `parse_oac_record` (`subscr.rs:379`), wallet parsers (`tls/wallet.rs:138/209/238`,
  `tls/sso.rs:201`, `tls/dn.rs:63`), `parse_query_response_borrowed` (`fetch.rs:1339`).
- **Tiers:** required = build all + replay **all** regressions + short smoke for
  changed/high-risk; canary = sharded moderate + rotating seeds; soak = risk-weighted
  deep + ASan + corpus merge/minimize + coverage summary; release = candidate SHA, ≥2
  independent seeds for high-risk targets, no untriaged finding.
- **Acceptance:** decoder coverage (or an approved exclusion) per the manifest — not a
  fixed target count; every finding fixed + corpus'd.

### W3-E3 — DPOR over the wire/cancel paths (asupersync)
Verified **0.3.4** API (skill docs describe a newer tree): `LabRuntime`/`LabConfig`
(`asupersync src/lib.rs:402`), `DporExplorer`/`ScheduleExplorer`/`ExplorationReport`
(`src/lab/explorer.rs`), `CoverageMetrics::is_saturated/discovery_rate`
(`:165/156`); `test_utils::run_test` is NOT the lab. Runs on **W1-T1's scripted
transport + W1-T2's state machine** (no separate seam needed now). Paths (verified
`crates/oracledb/src/lib.rs`): P1 timeout→BREAK→drain→reuse (`:6077/6234`); P2 cancel/
drop-cancel auto-drain (`:6198/3003`); P3 shared write-mutex vs `CancelHandle`
(`:5922`, needs the `CancelHandle::cancel(&Cx)` async variant from W1-T2); P4 close-
cursors piggyback vs `in_use_cursors` (`:1246/1255`, `ScheduleExplorer`); P5
speculative prefetch drop (`:3171`). **Stopping:** P1/P2/P4/P5 exhaustive (queue
empties under recorded `max_runs`, 0 violations); P3 bounded (`is_saturated(512)` &&
`discovery_rate<0.005`). Assert the W1-T2 ready-or-dead invariant + obligation-leak/
futurelock/cancellation oracles; emit `write_json_summary()` artifacts + reproducer
seeds. **Acceptance:** all paths clean under recorded bounds.

### W3-E4 — loom over the pool synchronization shell
Model the *small* event/state shell from W1-T7 (not the raw effectful module) with
explicit branch/permutation/preemption/duration/thread bounds; mock `PoolBackend`
(`pool.rs:90-109`). Invariants: no double-handout, no lost wakeup, no leaked waiter,
no returned-retired connection, idempotent close. **Stopping:** loom default budget
(or `LOOM_MAX_PREEMPTIONS=3`), reported as *bounded clean*. **Deps:** W1-T7.

### W3-E5 — Deterministic fault & fragmentation matrix
On W1-T1's scripted transport: generate fault points from protocol phases (connect/
auth/execute/fetch/LOB/cancel/close) and inject short read/write, `Pending`, EOF, I/O
error, timeout, virtual-time advances; assert **fragmentation invariance** (every legal
byte split → same semantic result); exercise faults around BREAK/RESET (next op never
consumes the previous response). **Acceptance:** generated phase/fault coverage report;
every case ends `Ready`/`Dead`/`Closed`; no live DB.

### W3-E6 — Secure full-connection cassette replay
Seam already built (format `net/cassette.rs`; `Recording`/`Replay` transport
`transport.rs:49-71`; capture scope; committed fixture; offline replay test). **Real
work:** (1) replay the **whole `ConnectionCore`** through the W1-T1 `ReplayTransport`
(prod + replay share framing/state — no public seam); (2) **fixtures are security-
sensitive** — manifest (format/schema ver, source commit, server profile, charset/TZ,
scenario, sanitizer ver, checksum, expected-writes, `sanitized:true`), disposable
accounts or synthetic post-auth transcripts, scrubber + leak scanner, never commit
production captures; (3) strict replay fails on unexpected writes/unread frames/
checksum mismatch/truncation/unsupported version; (4) enable `--features cassette` in
CI (zero refs today). **Acceptance:** strict offline full-`ConnectionCore` replay in
CI; sanitized fixtures with provenance; altered decoder fails.

### W3-E7 — Live support matrix + direct oraclemcp contract suite
- **`docs/SUPPORT.md`:** the Oracle server families, TLS modes, charsets/timezones,
  and platform targets 1.0 promises; unsupported auth/wallet modes listed + fail-closed.
- **Live matrix:** conformance (`harness/run.sh diff`) across the promised
  configurations, with reviewed normalization/allowlist rules + recorded reference
  versions.
- **oraclemcp contract suite:** built here — query/binds/DML/batch/LOB/pool/cancel-reuse/
  typed-errors — and run definitively on the RC SHA in W4-T2. Because the North Star
  names oraclemcp, it gets a first-class contract test, not just indirect conformance.
- **Acceptance:** matrix green per `SUPPORT.md`; oraclemcp suite green (definitively on
  the RC SHA in W4-T2).

### W3-E8 — Multiple multi-pass bug-hunt sweeps
The `multi-pass-bug-hunting` cycle over protocol/codec/multi-packet/async paths;
several independent fresh-eyes passes consuming the findings from E1–E7 and E9
(matching the DAG); triage by severity.
**Acceptance:** ≥2 consecutive full passes with zero new in-scope findings; all
triaged findings resolved or signed-off; per-pass log committed.

### W3-E9 — Performance & resource regression gates
Benchmark hot/risky paths (codecs, conversion/binds, streaming fetch, large/multi-
packet results, LOB chunks, batch setup, cancel/recovery, pool contention, cassette
overhead) + **x86_64-musl binary size** (the single-binary product property). Shared
runners = report-only trend; release thresholds on a controlled runner or repeated
statistically-justified comparison. **Acceptance:** baselines tracked; no unaccepted
regression vs the approved candidate baseline; large-result streaming has a documented
bounded-memory profile.

### W3-E10 — (post-1.0 backlog) idea-wizard differentiators
Speculative idea generation is **post-1.0** unless a separately approved, supportable
requirement enters scope before RC freeze. (Was v2's W3-E6; demoted per review.)

---

## 8. Wave 4 — Freeze & release

### W4-T1 — Cut `1.0.0-rc.1`
Remove pre-1.0 deprecations + accidental public internals; freeze the intended 1.x
surface. **Deps:** W0 (CI tiers) ∧ §7 E1–E9 (which transitively require §5 architecture
+ §6 0.3.0 migration) — matches the DAG.

### W4-T2 — Qualify the exact RC SHA (ADR-0003)
Run the **`release-qualification` workflow** (W0-T2 — it takes the RC SHA as
`candidate_sha`) at soak-equivalent budget: every gate + live/oraclemcp suites + perf
comparison, all on the **frozen RC commit**; any code change → a new RC. (Per ADR-0003
the scheduled canary/soak lanes run `main` for *discovery*; this manual exact-SHA run
is the *qualification* — they are not the same thing.) Convergence synthesis (E1 green at
soak budget; E2 manifest coverage + differential 0 divergences; E3 exhaustive/bounded
clean + E4 loom clean with artifacts; E5 fault matrix green; E6 cassette green;
E7 matrix + oraclemcp green; E8 ≥2 zero-finding passes; E9 no regression) — meeting the
§7 severity policy in full (no open P0/P1, no untriaged finding, P2 fixed-or-signed-
exception). **Acceptance:** a committed exact-SHA evidence bundle.

### W4-T3 — Packaged-source & provenance preflight
Verify all workspace + **inter-crate** versions (close the `release_preflight.sh` gap:
it checks package versions agree `:32-42` but not that `oracledb/Cargo.toml:71-72`
inter-crate *requirements* equal the workspace version — we risked exactly this at
0.2.1/0.2.2). Inspect `cargo package --list`; create/unpack/test `.crate` files in
dependency order without workspace path resolution; publish dry-runs; build docs for
supported profiles; produce checksums + SBOM + action/dep inventory + musl-binary
provenance (GitHub build attestations). **Acceptance:** `.crate` artifacts build/test
standalone; preflight rejects a mismatched inter-crate pin.

### W4-T4 — Publish `1.0.0`
Publish in dependency order (`publish_crates.sh`, idempotent); tag `1.0.0`; then build
a clean consumer + oraclemcp from **public registry** artifacts; document yank/rollback
criteria; the `cargo public-api` snapshot becomes the 1.x baseline.

---

## 9. Dependency graph (DAG)

```
W0-T1 ─► W0-T2, W0-T3 ;  W0-T4 (independent, off-gate)

W1-T1 ─► W1-T2 ─► W1-T3 ─► W1-T4
W1-T1 ─► W1-T5 ;  W1-T2 ─► W1-T6 ;  W1-T7 (independent)

(all §5) ─► W2-T1 (0.3.0 migration)

W2-T1 ─► W3-E1, W3-E2, W3-E5, W3-E6, W3-E7, W3-E9
W1-T1+W1-T2 ─► W3-E3 ;  W1-T7 ─► W3-E4
(E1..E7,E9) ─► W3-E8

(W0) ∧ (E1..E9) ─► W4-T1 ─► W4-T2 ─► W4-T3 ─► W4-T4
```
No cycles; every task feeds another or the release.

---

## 10. Evidence & gating summary (per exact candidate SHA)
- Required CI: deterministic, fixed-seed, fast; branch-protected.
- Canary/soak: rotating seeds, deep budgets, report-only (red ≠ blocked, but visible).
- Release-qualification: manual exact-SHA; the only definitive 1.0 gate.
- "Exhaustive" only when a finite model queue empties under recorded bounds; else
  "bounded clean." Severity policy gates, not raw pass/fail proxies.

---

## 11. Skills are guidance, not gates (D3)
We still leverage every applicable skill — `asupersync-mega-skill` (W1-T1/T2, W3-E3),
`multi-pass-bug-hunting` (W3-E8 + triage), `code-simplifier` (W1-T3), `oracledb`
(API/behavior), `idea-wizard` (post-1.0 W3-E10), `testing-conformance-harnesses`
(W3-E6/E7) — but acceptance criteria name **observable behavior/artifacts**, never
"which skill was used." Skills live in contributor/issue metadata.

---

## 12. Recommended resulting architecture (review)

```
 async facade            blocking facade
        \                    /
         \-- operation reqs --/   (Query/Execute/Batch/Registration)
                  |
            private OperationCore
                  |
   session recovery (Ready/InFlight/BreakSent/Draining/Ready|Dead)
        + typed errors/disposition + ProtocolLimits
                  |
            private ConnectionCore
                  |
          private WireTransport
   TCP/TLS · Recording · Replay · Scripted(fault/time)

 pool facade → sync shell → pure PoolState → effects(open/ping/close)
                        (deterministic + loom models)
```

---

## 13. External-contract decision — RESOLVED: (b) full contract
**Decided (R8 = b).** Reasoning: a driver is a *bounded* domain — once parity +
extras land and the API is designed deliberately pre-0.3.0, the public surface
genuinely stabilizes and future growth is overwhelmingly **additive** (new Oracle
types via `#[non_exhaustive]` value enums + `as_*` accessors; new methods; new
protocol capabilities). Oracle is backward-compatible, so the TTC/TNS protocol never
forces an API break. Blocking `cargo-semver-checks` therefore does **not** impede
evolution — it only blocks *unintended* breaks; additive growth passes freely. So
the lock fits this domain and is worth doing.

**What (b) commits us to** (calibrates W0-T3, W3-E7, W4-T3):
- Blocking `cargo-semver-checks` from the 0.3.0 baseline onward (advisory during the
  0.3.0 redesign; see ADR-0002 sequencing).
- The full supported feature-profile matrix in CI (`docs/SUPPORT.md`).
- SBOM + build provenance/attestations + build-a-clean-consumer-from-registry at 1.0.
- The live support matrix (W3-E7) covers every promised server/TLS/charset config.

**The one honest ongoing cost** (the only recurring *involuntary* break source): the
typed dependency bridges (`chrono`/`uuid`/`rust_decimal`/`serde_json`) re-export those
crates' types, so a *their*-major bump is a break in *our* API. Mitigation: keep them
feature-gated, minimize public dep-typed surface, and treat a dep-major bump as a
deliberate, baseline-updating bump. This is the ADR-0002 review trigger, not a reason
to weaken the contract.

**Intentional breaks remain allowed** — the gate is a safety net against *accidental*
breaks, not a prohibition. A real, needed break ships with the correct version bump
(0.x: minor; ≥1.0: major) and a baseline refresh in the same release.

---

## 14. Risks (status)
- R1 budgets → **reframed** (review 11): per-property/target budgets, recorded bounds,
  severity triage, exact-SHA — not plateau/saturation-as-proof.
- R2 API churn → 0.3.0 migration window + deprecations + conformance guard.
- R3 `cargo public-api` under nightly → YES (rustdoc-JSON verified; raw-JSON fallback).
- R4 scope → Group-A/#2-#4/#6 + `57z` out (E10 post-1.0).
- R5 convergence-is-asymptotic → folded into R1 + §7: per-target bounds, severity
  triage, and "exhaustive only when the finite queue empties" replace open-ended chase.
- R6/R7 → subsumed by the single private transport seam (W1-T1) + state machine (W1-T2).
- **R8 — RESOLVED (b):** full external SemVer/support contract; blocking semver-checks
  from the 0.3.0 baseline (advisory during the redesign). §13.
- **R9 (new, the residual real risk):** typed dependency bridges (chrono/uuid/decimal/
  serde_json) couple our SemVer to theirs — the only recurring involuntary break.
  Mitigate (feature-gate, minimize public dep-typed surface); ADR-0002 review trigger.
- **R10 (verify in W0-T3):** `cargo-semver-checks` not yet run under the pinned nightly
  (expected OK — same rustdoc-JSON path as the verified `cargo public-api`).
- Unverified flags for W0-T1/review: accidental-leak candidates' intended visibility;
  absence of a native PyO3↔asupersync async bridge (W0-T4 rationale).

---

## 15. Next steps (planning-workflow)
R8 is resolved (§13 = full contract). 1. Optional further review round to
steady-state. 2. Convert to beads with the DAG intact (`W{n}-T{m}`/`W3-E*` → ids +
`br dep` edges). 3. Implement Wave 0 → Wave 4; tag 1.0 at W4-T4.
