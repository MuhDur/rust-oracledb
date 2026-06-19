# rust-oracledb — Road to 1.0

> **Status:** planning (v3.3 — folded the five asupersync-review refinements, each
> verified against the vendored 0.3.4 API: single op-deadline (W1-T3), bounded recovery
> budget + checkpoint invariant (W1-T2), `Outcome`/`CancelKind` discipline (W1-T6),
> lab oracles-as-gates + crashpacks + chaos/`VirtualTcp` (W3-E3); v3.2 — pool made
> async-native with a `block_on` sync facade (W1-T7/W3-E4) per the asupersync-mega-skill
> review; v3.1 — GPT-Pro review integrated; R8 resolved to the full external contract;
> internal-consistency pass done). Authored via
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
  sans-io rustls; asupersync vendored at **0.3.4** (`Cargo.lock:211`). The connection
  layer is "async core + thin `block_on` sync facade": one async `Connection` engine,
  `BlockingConnection` = `block_on` wrappers over it.
- `oracledb` connection pool (`pool.rs`) — **baseline note (changes in W1-T7):** today
  it is *synchronous* and the lone inversion of that rule — `Mutex<PoolState>` +
  `Condvar` waiters + a blocking reaper `std::thread`; `acquire` is a sync `fn`
  (`pool.rs:234`) returning a `u64` handle from a low-level `PoolEngine` with **zero
  `async fn`**. The `Condvar`/"no foreign locks held" design exists **only** so the PyO3
  pyshim can release the GIL before blocking — i.e. it is shaped by a `publish=false`
  test harness, not by a Rust consumer, and it forces an async caller to block a runtime
  worker. W1-T7 flips it to async-native (sync facade), matching the connection layer.
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

- **Wave 0 — Evidence & CI:** baseline inventories + ADRs, tiered reusable CI +
  toolchain runbook, feature/SemVer/stable lanes, the API-surface ledger. (§4)
- **Wave 1 — Architecture:** operation-specific public API; type-evolution policy;
  private transport/connector + `ConnectionCore`; session-recovery state machine;
  `ProtocolLimits`; async-native pool (sync facade); typed errors/disposition + redacted observability;
  async↔blocking symmetry; module/re-export coherence. (§5)
- **Wave 2 — Migration release:** publish **0.3.0**; migrate pyshim + oraclemcp;
  remove deprecations before RC. (§6)
- **Wave 3 — Qualification:** property, fuzz (manifest), DPOR (wire/cancel seam **and**
  the async pool — loom dropped), fault/fragmentation matrix, secure cassette replay, live
  support matrix + direct oraclemcp contract suite, perf/resource gates, multi-pass
  hunts — all to exact-SHA bounded evidence. (§7)
- **Wave 4 — Freeze & release:** cut `1.0.0-rc.1`; exact-SHA qualification;
  packaged-source/provenance preflight; publish `1.0.0`. (§8)

Substantial parallelism remains *within* a wave; only outputs that survive into the
candidate are collected deep.

---

## 4. Wave 0 — Evidence & CI

### W0-T1 — Pin baseline + generate inventories + record ADRs
- **What:** record the source commit; generate `docs/baseline/` artifacts — public-API
  listing (per supported profile), enabled-feature matrix, fuzz-target manifest dump,
  test inventory, and the version/pin set. All later counts derive from these, not
  from prose. Also write the three decisions in §1 as files under `docs/adr/`
  (`0001-nightly-asupersync.md`, `0002-semver-contract.md`, `0003-exact-sha-evidence.md`),
  each with its review triggers — the plan references `docs/adr/` but nothing creates it.
- **Deps:** none. **Acceptance:** `docs/baseline/*` and `docs/adr/000{1,2,3}-*.md`
  committed; the baseline is regenerable by a script.

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
- **Companion runbook (`docs/TOOLCHAIN.md`):** the floating-nightly canary *detects* a
  toolchain break; this doc is the *response* — why nightly (asupersync/`try_trait_v2`,
  build-time-only), how the pin is chosen, and the exact re-pin steps when the canary
  goes red. Link it from `README.md` + the `oracledb` skill. (Restores a v2 deliverable
  the v3 restructure dropped.)
- **Deps:** W0-T1. **Acceptance:** the four tiers run the same commands at different
  budgets; a forced evidence failure opens exactly one issue; required CI unchanged in
  scope; a scheduled red job does **not** silently pass; `docs/TOOLCHAIN.md` lets a
  fresh agent re-pin from it alone.

### W0-T3 — Feature-profile + SemVer + stable-protocol lanes (decision (b))
- **What:** define supported profiles in `docs/SUPPORT.md` (minimal/default/all +
  each optional-integration combo — chrono/uuid/serde_json/rust_decimal/arrow/soda);
  exercise the **full** supported matrix with `cargo hack`; wire `cargo-semver-checks`
  (gate it on the **library** crates `oracledb-protocol` + `oracledb` **only** — it
  skips `oracledb-derive`, a proc-macro, and *errors* if asked to check it; cover the
  derive's generated surface with a separate `trybuild` compile test instead); keep
  `cargo public-api` snapshots per supported profile; build+test **`oracledb-protocol`
  on current stable** so its stable-compatibility can't silently rot.
- **VERIFIED (R10 resolved):** `cargo-semver-checks 0.48.0` runs cleanly under
  `nightly-2026-05-11` — `semver-checks check-release -p oracledb-protocol` and
  `-p oracledb` both pass (196 checks each) and it does **not** choke on `oracledb`'s
  transitive asupersync `try_trait_v2` (its own rustdoc-JSON step handles the
  nightly-only crate; ~60–75 s cold build for `oracledb`). Proven non-trivial: vs
  `--baseline-rev 27fb6c3` it correctly flags the `ConnectOptions.statement_cache_size`
  field-add as major-requiring. Offline runners pre-fetch the baseline + pass
  `--baseline-rustdoc`; the manual rustdoc-JSON fallback is **not** needed.
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

### W0-T5 — API-surface ledger + accidental-leak adjudication
- **What:** from W0-T1's public-API listing, produce `docs/API_LEDGER.md` — every `pub`
  item × disposition (keep / `pub(crate)` / rename / consolidate / deprecate) + a
  one-line reason. **Adjudicate the accidental-leak candidates** flagged by research:
  `ObsSpanGuard` (`obs.rs:106`), `OracleReadHalf`/`OracleWriteHalf` (`transport.rs:40/58`),
  `PoolEngine<B>` (`pool.rs:164`), `DirectPathStream`/`BatchLoadState`/`DirectPathPieceBuffer`
  (`dpl.rs:722/792/391`), `ExecutemanyManager`/`…Error` (`cursor_logic.rs:45/15`) — each
  kept-public or made `pub(crate)` with a recorded reason (several may be deliberate).
- **Why:** the ledger is the driving artifact of the API audit (D2) — Wave 1 *applies*
  its dispositions (W1-T1 already privatizes the transport halves; W1-T3 consolidates;
  W1-T4 evolves types; W1-T9 tidies modules). Restores a deliverable the v3 restructure
  dropped.
- **Deps:** W0-T1. **Skill:** `oracledb` (intended-surface ref). **Acceptance:** ledger
  committed; human signs off on removals/renames; every leak candidate has a recorded
  keep/`pub(crate)` decision.

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
- **Bounded recovery budget (pairs with the single op-deadline, W1-T3 / `API_DESIGN.md`
  principle 7):** the BREAK→`Draining` cleanup runs under its **own fresh, bounded recovery
  deadline**, independent of the op's already-expired deadline — otherwise the very
  timeout/cancel that triggered recovery would instantly cancel the drain and force `Dead`
  on a recoverable session. Only a *second* failure during the bounded drain yields `Dead`.
  *Mechanism note (verified):* asupersync `Cx::masked` (`cx/cx.rs:2238`) masks only a
  **synchronous** finalize closure (`mask_depth`/`MaskGuard`, invariant
  `inv.cancel.mask_bounded`), so the **async** drain is protected by a fresh recovery
  sub-context/deadline, **not** by wrapping the await in `masked`. (Today recovery re-arms
  a fresh relative `timeout_ms` — `recover_from_call_timeout(cx, timeout_ms)`,
  `lib.rs:1926`; the single-deadline move makes the explicit bounded recovery budget
  **necessary**, not optional.)
- **Cancellation-observability invariant:** every multi-round-trip loop (fetch-batch
  continuation, LOB chunk, recovery drain, retry) carries a `cx.checkpoint()` so a pending
  cancel is observed *between* round-trips. Checkpoints already exist pervasively (~20+
  sites incl. the fetch region `lib.rs:3081/3134/3195/3229`); this **codifies it as a
  contract** that the W3-E3 DPOR cancel paths verify — it is not "add missing checkpoints."
- **Why:** the public contract becomes "every completed op leaves the session reusable
  or dead," which is also the DPOR invariant (W3) and what oraclemcp needs. Supersedes
  v2's "make drain public" symmetry note.
- **Deps:** W1-T1. **Skill:** `asupersync-mega-skill`. **Acceptance:** the machine is
  the single owner of recovery; property/DPOR tests assert the ready-or-dead invariant
  across success/timeout/cancel/error/dropped-future.

### W1-T3 — Operation-specific public API (replaces the mega-builder)
- **WORKED DESIGN: [`docs/API_DESIGN.md`](API_DESIGN.md)** — the full spec (four families
  `query`/`execute`/`execute_many`/`register_query` + `query_one`/`query_opt`/`query_all`,
  `Params`/`Query`/`Execute`/`Batch`/`Registration` + `Rows`/`ExecuteOutcome`/`BatchOutcome`/`RegistrationOutcome`,
  the retained low-level surface, the `BlockingConnection` mirror, examples, and the
  old→new "nothing lost" map across all 24 capability groups). Built from the verified
  capability inventory + the Rust-DB precedent survey. This bullet's enumeration is the
  summary; the doc is authoritative.
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
  // Builder-taking forms (the bare query/execute/execute_many take (sql, params) —
  // see API_DESIGN.md §3–§5 for the full surface incl. query_one/opt/all).
  pub async fn query_with       (&mut self, cx: &Cx, q: Query<'_>)        -> Result<Rows>;
  pub async fn execute_with     (&mut self, cx: &Cx, e: Execute<'_>)      -> Result<ExecuteOutcome>;
  pub async fn execute_many_with(&mut self, cx: &Cx, b: Batch<'_>)        -> Result<BatchOutcome>;
  pub async fn register_query   (&mut self, cx: &Cx, r: Registration<'_>) -> Result<RegistrationOutcome>;
  ```
  Distinct borrowed request/result types per family (no invalid combos: batch rows ≠
  scalar binds; fetch/LOB policy only on `Query`; CQN only on `Registration`). Prefer
  **borrowed** params with owned convenience `From` conversions; relative timeouts use
  **`Duration`**, translated **once into a single absolute deadline carried in the op/
  cursor context across *every* round-trip of the logical op** — the initial call plus all
  `Rows::next_batch`/`collect` continuations and LOB chunks — **never re-armed per
  round-trip**, and never a public `timeout_ms`. (Fixes the current per-call model:
  `timeout_ms: u32` is applied per call — `lib.rs:1911/2981` — so an N-batch fetch can run
  up to N× the intended budget.) A tighter `Cx` deadline wins; the post-timeout drain runs
  under its own bounded budget (W1-T2). Validated **`NonZero`** sizes. `BlockingConnection`
  mirrors 1:1. (See `API_DESIGN.md` principle 7.)
- **Deps:** W1-T1, W1-T2. **Skill:** `code-simplifier`, `oracledb`. **Acceptance:**
  the four families cover every prior capability (old→new mapping table in the PR);
  invalid states unrepresentable; `harness/run.sh diff` stays green (anti-drift guard).

### W1-T4 — Type-evolution policy (calibrated, not blanket `#[non_exhaustive]`)
- **What:** during 0.3, by type class — **options/config** (`ConnectOptions`,
  `ExecuteOptions`-derived request types, `PoolConfig`, `ArrowFetchOptions`): private
  fields + builders + getters; **results/metadata** (`Rows`/`ExecuteOutcome`/
  `BatchOutcome`, `ColumnMetadata`, `DecodedObject`, `DbmsOutput`, `BatchServerError`):
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
  auto-retry non-idempotent ops). **Internal `Outcome`/`CancelKind` discipline:** keep
  asupersync's four-valued `Outcome<T,E>` (verified `types/outcome.rs:216`) and
  `CancelKind`/`CancelReason` (`types/cancel.rs:264/521`) threaded internally and branch on
  the cancel kind — `Timeout` (drain + set `retry_hint` where idempotent), `Shutdown`
  (close the connection), `RaceLost` (loser drains quietly) — to drive
  `connection_disposition()`; flatten `Outcome`→`Result` only at the public boundary,
  mapping `Cancelled` to a distinct cancel/timeout error variant (never a generic I/O
  error). Prevalidate bind shape/type where unambiguous.
  Opt-in tracing/pool metrics record phase, duration, statement fingerprint, rows/
  bytes, pool wait, cancel phase, disposition — **raw SQL/binds/credentials excluded
  by default**, covered by redaction tests.
- **Why:** review change 17 — promoted from "optional differentiator" to foundational;
  it's exactly what oraclemcp needs and pairs with W1-T2.
- **Deps:** W1-T2. **Skill:** `oracledb`. **Acceptance:** classification + disposition
  via methods (no display-string parsing); redaction tests prove no secret leakage.

### W1-T7 — Async-native connection pool (sync facade) — *was "pure pool model + loom"*

**DECISION (binding for this roadmap): the pool is async-native; sync is a `block_on`
facade.** Today `pool.rs` is *synchronous* — `Mutex<PoolState>` + `Condvar` waiters + a
blocking reaper `std::thread`, with `acquire` a sync `fn` (`pool.rs:234`) returning a
`u64` handle from a low-level `PoolEngine` (zero `async fn`). That `Condvar`/"no foreign
locks held" design exists **only** so the PyO3 pyshim can release the GIL before
blocking. The pyshim is `publish=false` (a conformance harness) and **must not dictate
the shipped driver's architecture**. Every other part of the driver is "async core +
thin `block_on` sync facade" (`Connection`/`BlockingConnection`); the pool is the lone
inversion (sync core; async callers must adapt), which forces an async consumer (e.g.
oraclemcp on asupersync) to **block a runtime worker on a `Condvar`** — the canonical
"blocking call inside a cooperative runtime" anti-pattern. W1-T7 flips the pool to match
the rest of the driver. *(There is no GIL in Rust; the GIL constraint was never the
driver's — only the test shim's.)*

**What (detailed, self-contained):**
1. **Async core.** `pub async fn acquire(&self, cx: &Cx, opts: AcquireOptions) ->
   Result<PooledConnection, PoolError>`. Fast path takes an idle connection from
   `PoolState`; slow path registers in an **ordered waiter queue** and `.await`s a
   yielding wakeup (asupersync `Notify`/semaphore — **no `Condvar`, no parked OS
   thread**). The acquire timeout is a **bounded deadline/`Budget`** on the wait (not a
   sleep race), and `cx.checkpoint()` rides the wait so a cancel is observed promptly.
2. **Cancel-safety (a DPOR invariant).** If the acquire future is dropped/cancelled in
   the hand-off race window, the waiter removes itself from the queue and any connection
   it was just granted is **returned to the pool** — never leaked, never double-handed.
3. **Sync facade.** A blocking facade = `block_on(acquire(...))`, exposed the same way
   `BlockingConnection` mirrors `Connection` (a `Blocking*` surface, **not** an
   `_blocking` method suffix); exact surface/naming per W1-T3 + the W1-T8 sweep. Mirror
   `close`/`drain`/`stats`/`release`. One engine, two doors.
4. **Region-owned reaper.** The idle-ping / expiry reaper becomes an **async task owned
   by the pool's region/scope**, not a detached `std::thread`: `close(&Cx).await` cancels
   and joins it deterministically; a bare `drop` (no `close`) lets the owning region
   **abort** it (no await in `Drop` — R11), so it never leaks either way. It runs
   open/ping/close as async **effects outside the state lock**, fed back as events.
5. **Pure `PoolState` retained (the salvaged half of the old task).** Explicit states
   `Opening/Idle/CheckedOut/Validating/Retiring/Closing/Closed`, counts **derived** from
   state, effects as events, specified waiter ordering / acquire-cancel / reap /
   graceful-vs-force close. It now sits behind an async mutex + yielding waiter queue
   instead of a `Condvar`; its concurrency is explored by **DPOR (W3-E4), not loom**.
   (Keeps network calls out of the critical section — the original perf rationale.)
6. **`PooledConnection` guard + the Drop problem.** Checkout returns a guard that returns
   the connection on release. Because **async checkin cannot run in `Drop`**, drop
   **enqueues a non-blocking "return" event** reconciled by the next acquire / the reaper
   (the same effects-as-events model), with a bounded sync fallback; `Drop` never blocks
   or spawns (see R11). An explicit `release(&Cx).await` is offered for the eager path. A
   returned connection is reconciled to `Ready` (W1-T2) or retired.
7. **Visibility.** The low-level handle engine (`PoolEngine<B>`, the W0-T5 leak candidate
   at `pool.rs:164`) becomes `pub(crate)`; the public surface is the async `Pool` + its
   sync facade.
8. **pyshim.** Keeps driving the **blocking** facade; its GIL handling stays the pyshim's
   own concern (it already does `spawn_blocking`). No `Condvar`/GIL rationale survives in
   the shipped crate.

- **Why:** consistency with the rest of the driver (async core + sync facade); removes a
  runtime-blocking anti-pattern for async consumers; and lets the **single** deterministic-
  concurrency tool (asupersync DPOR) cover the pool — eliminating the second runtime
  (loom) entirely (W3-E4).
- **Deps:** W1-T1 (mint/ping `Connection`s via `ConnectionCore`), W1-T2 (return-to-pool
  reconciles session disposition `Ready`/`Dead`). The **pure `PoolState` sub-model has no
  deps** and can land first with ordinary deterministic tests. **Skill:**
  `asupersync-mega-skill`, `multi-pass-bug-hunting`.
- **Acceptance:** async `acquire` **yields** under contention (no OS thread parked —
  observable in the DPOR/lab schedule); a cancel mid-acquire leaks/duplicates no
  connection; the sync facade is exactly `block_on` of the async path; the reaper is
  joined on `close(&Cx).await` and aborted (not leaked) on bare `drop`; the pure `PoolState` has full-lifecycle
  deterministic tests with derived counts; `harness/run.sh diff` stays green.

### W1-T8 — async↔blocking symmetry sweep
- **What:** make the two public surfaces mirror exactly. **Verified genuine I/O gaps to
  fill:** `BlockingConnection::cancel` is missing (async `:4361`; only
  `CancelHandle::cancel` `:4933` exists); CQN `recv_notification`/`notify_register`
  (`:1816/1787`) are async-only; `free_temp_lobs` (`:3910`) and `trim_lob` (`:3873`)
  have only `*_with_timeout` blocking twins; `execute_query_with_bind_rows_and_options`
  (`:2711`, no-timeout) is async-only. **Deliberately async-only (document, don't wrap):**
  the zero-copy `_ref` fetch family + direct-path (borrow lifetimes don't cross
  `block_on`). The v2 "make `drain_cancel_response` public" reversed-gap is **mooted by
  W1-T2** (drain stays private on both surfaces). Standardize naming
  `x`/`x_with_timeout`/`x_named`. **Pool surface (W1-T7):** the async `Pool` and its
  blocking facade are part of this sweep — `acquire`/`close`/`drain`/`stats`/`release`
  mirror 1:1 (or carry a documented async-only exception), named per the `BlockingConnection`
  pattern (a `Blocking*` surface, not an `_blocking` suffix); this is where the W1-T7
  facade naming is finalized.
- **Why:** asymmetry is a silent papercut and contradicts W1-T3's "`BlockingConnection`
  mirrors 1:1"; this is the task that verifies that claim. Restores v2's symmetry sweep.
- **Deps:** W1-T3 (consolidation sets the final method set), W1-T2 (cancel semantics),
  W1-T7 (the pool's two surfaces).
  **Skill:** `oracledb`. **Acceptance:** a generated table shows 1:1 async↔blocking
  coverage or an explicit documented exception per method.

### W1-T9 — Module / re-export coherence
- **What:** review the module tree and the `oracledb::protocol` re-export story; ensure
  each public type is exported from one obvious path (no duplicate paths); tidy/define a
  `prelude` if warranted. Apply the W0-T5 ledger's module-placement dispositions.
- **Why:** a coherent import story is part of the 1.0 contract and ages badly if left
  organic. Restores v2's module-coherence task.
- **Deps:** W0-T5, W1-T3. **Skill:** `code-simplifier`. **Acceptance:** doc-tests/examples
  compile against the tidied paths; no type reachable via two public paths.

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
`discovery_rate<0.005` — a **heuristic, not an exhaustiveness proof**, §7). Assert the
W1-T2 ready-or-dead invariant with the lab's oracles as **hard gates** — set `LabConfig`
`panic_on_futurelock` + `panic_on_leak` + `futurelock_max_idle_steps` (verified
`lab/config.rs`) plus the **quiescence** oracle (`lab/crashpack/oracle.rs`), so a
futurelock / obligation-leak / non-quiescent-shutdown violation **fails the run**, not
just a summary line; and **test-assert** the W1-T2 ready-or-dead contract + loser-drain
(cancelled losers drain their cursor; cf. `lab/atp_path/harness.rs`). On any failure emit a **crashpack**
(`CrashPack`, `lab/crashpack/…`) + `repro_manifest` (`lab/replay.rs`) +
`write_json_summary()` (`lab/explorer.rs:335`) + reproducer seed (one-command replay).
Layer deterministic chaos onto the cancel paths — `with_light_chaos()` in canary /
`with_heavy_chaos()` in soak (`lab/chaos.rs:49/53`) over a `VirtualTcp`/`VirtualNet`
surface (`net/tcp/virtual_tcp.rs`, `lab/network/harness.rs`) — to shake out lost-wakeup /
budget-exhaustion classes pure backtracking may miss. **Acceptance:** all paths clean
under recorded bounds; oracles green as gates; crashpack + seed captured on any failure.

### W3-E4 — DPOR over the async pool (shares the E3 lab; loom dropped)
With W1-T7 the pool is **async-native on asupersync sync primitives**, so its
interleavings are visible to the **same DPOR/`LabRuntime`** machinery as the wire/cancel
paths (E3) — and **loom is removed from the plan**. *Rationale:* loom intercepts
`std`/`loom` sync + atomics, whereas DPOR explores async task/await/obligation
interleavings; once the pool holds no `std::sync`/atomics concurrency island, loom has
nothing left to model. *Guard:* if any std-sync island *does* remain after W1-T7, loom
covers **only** that, reported *bounded clean*; otherwise loom is not a dependency.
Model the W1-T7 pure `PoolState` shell + yielding waiter queue with a mock `PoolBackend`
(`pool.rs:90-109`) under DPOR. **Invariants:** no double-handout, no lost wakeup, no
leaked waiter, no returned-retired connection, idempotent close, **and acquire-cancel
returns (never leaks/duplicates) its connection** (W1-T7). **Stopping:** finite queue
empties under recorded `max_runs` → *exhaustive*; else *bounded clean* — `is_saturated`/
`discovery_rate` is a **heuristic, not a completeness proof** (§7; asupersync's documented
guarantee is Optimal-DPOR *efficiency*, not saturation=exhaustiveness). Oracles as hard
gates + crashpack / `repro_manifest` / `write_json_summary()` + reproducer seed on any
failure, exactly as W3-E3. **Deps:** W1-T7.

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
clean + E4 pool-DPOR exhaustive/bounded clean with artifacts; E5 fault matrix green; E6 cassette green;
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
W0-T1 ─► W0-T2, W0-T3, W0-T5 ;  W0-T4 (independent, off-gate)

W0-T5 ─► W1-T1, W1-T3, W1-T4, W1-T9   (ledger dispositions feed the audit work)
W1-T1 ─► W1-T2 ─► W1-T3 ─► W1-T4
W1-T1 ─► W1-T5 ;  W1-T2 ─► W1-T6 ;  (W1-T1 ∧ W1-T2) ─► W1-T7   (its pure-state core can start earlier)
(W1-T3 + W1-T2 + W1-T7) ─► W1-T8 ;  (W0-T5 + W1-T3) ─► W1-T9

(all §5 — W1-T1..T9) ─► W2-T1 (0.3.0 migration)

W2-T1 ─► W3-E1, W3-E2, W3-E5, W3-E6, W3-E7, W3-E9
W1-T1+W1-T2 ─► W3-E3 ;  W1-T7 ─► W3-E4 (pool DPOR — shares the E3 lab; loom dropped)
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
We still leverage every applicable skill — `asupersync-mega-skill` (W1-T1/T2, W1-T7, W3-E3, W3-E4),
`multi-pass-bug-hunting` (W3-E8 + triage), `code-simplifier` (W1-T3, W1-T9), `oracledb`
(W0-T5 ledger, W1-T8 symmetry, API/behavior), `idea-wizard` (post-1.0 W3-E10),
`testing-conformance-harnesses` (W3-E6/E7) — but acceptance criteria name **observable
behavior/artifacts**, never "which skill was used." Skills live in contributor/issue
metadata.

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

 async pool core: acquire(&Cx).await · yielding waiter queue · region-owned reaper
        → pure PoolState (Opening/Idle/CheckedOut/Validating/Retiring/Closing/Closed)
        → effects(open/ping/close) as events
 blocking pool facade = block_on(async pool)
                        (deterministic unit tests + DPOR; loom dropped)
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
- **R10 — RESOLVED:** `cargo-semver-checks 0.48.0` verified working under the pin (196
  checks pass on `oracledb-protocol` + `oracledb`; no `try_trait_v2` choke). Gate those
  two **library** crates only — `oracledb-derive` (proc-macro) is skipped by the tool →
  guard its generated surface with `trybuild`; offline CI uses `--baseline-rustdoc`. W0-T3.
- Accidental-leak candidates' intended visibility → adjudicated in **W0-T5** (ledger).
- **Async-bridge flag — RESOLVED:** verified there is **no** native PyO3↔asupersync
  bridge — asupersync 0.3.4 exposes no foreign-loop/external-waker surface and
  `pyo3-async-runtimes` supports only tokio/async-std. The shim's PyO3 `experimental-async`
  + `spawn_blocking` + per-op `block_on` is the correct design (node-oracledb-analogous),
  so `nto` is rightly won't-fixed (W0-T4).
- **R11 (new): async return-to-pool cannot run in `Drop`.** The W1-T7 `PooledConnection`
  guard can't perform an async checkin on drop. Mitigation: `Drop` enqueues a
  non-blocking "return" event reconciled by the next acquire / the reaper (the
  effects-as-events model) with a bounded sync fallback — `Drop` never blocks or spawns;
  an explicit `release(&Cx).await` is offered for the eager path. W3-E4 DPOR asserts no
  connection is lost or double-returned across a future dropped mid-acquire/mid-use.

---

## 15. Next steps (planning-workflow)
**Done this round:** R8 resolved (§13); R10 + the async-bridge flag verified and
resolved (§14); the W1-T3 public API designed in full ([`docs/API_DESIGN.md`](API_DESIGN.md));
the **pool decided async-native with a `block_on` sync facade** (W1-T7 rewritten in
detail; loom dropped — DPOR covers the pool, W3-E4); and the **five asupersync-review
refinements folded in, each verified against vendored 0.3.4** (single op-deadline W1-T3 +
`API_DESIGN.md` principle 7; bounded recovery budget + checkpoint invariant W1-T2;
`Outcome`/`CancelKind` discipline W1-T6; lab oracles-as-gates + crashpacks + chaos/
`VirtualTcp` W3-E3) — all per the asupersync-mega-skill review.
The plan is decision-complete and the highest-stakes design (the 1.0 API contract) is
specified. Remaining: 1. (optional) a review round on `API_DESIGN.md` itself. 2. Convert
the plan to beads with the DAG intact (`W{n}-T{m}`/`W3-E*` → ids + `br dep` edges) via
`/beads-workflow`. 3. Implement Wave 0 → Wave 4; tag 1.0 at W4-T4.

---

## 16. Beads are now the authoritative execution graph
This plan has been converted to a beads graph (root epic **`rust-oracledb-road-to-1-0-llv`**:
1 program epic + 5 wave epics + the `W*`/`E*` tasks + decomposed subtasks, with the §9 DAG
overlaid as `blocks` edges and intra-task ordering edges). **The beads are the authoritative
source for execution** (`br ready` / `bv --robot-next`); this markdown is the seed. Polish
rounds + a **Codex (gpt-5.5) multi-model triangulation** refined the graph beyond this doc:
- **W1-T10** — the pure `PoolState` model split out of W1-T7 as a standalone, dependency-free
  task (it can land first, in parallel — the doc's W1-T7 said so but the bead had gated it).
- **W1-T2.4** — the async `CancelHandle::cancel(&Cx)` variant that W3-E3 P3 needs (today only
  `Connection::cancel(&Cx)` + a *sync* `CancelHandle::cancel` exist; `lib.rs:4361/4933`).
- **W1-T3.9** — resolve the `API_DESIGN.md` §10 deferred signatures (ColumnIndex/Cow/scroll/
  OutBinds/ReturningRows/cursor accessors; park `query_stream`) *before* the 0.3.0 freeze.
- **W3-E7.4** — driver-native e2e integration scripts with detailed structured logging.
- **W2-T1.6** — an external-facing 0.3.0 migration guide + CHANGELOG (decision (b) contract).
- Dependency fixes: **W2-T1 now depends on W0-T2/W0-T3** (the SemVer-gate flip needs the
  semver lanes + required CI — the doc's §9 reduction had dropped this); the W0-T5 leak
  candidate `dpl.rs` lives in **`oracledb-protocol`**, not the driver crate.
