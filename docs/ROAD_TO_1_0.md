# rust-oracledb — Road to 1.0

> **Status:** planning (v2, research-grounded). Authored via `/planning-workflow`;
> every load-bearing claim verified against the tree by a 4-agent research pass
> (cites are `file:line`; the few unverifiable items are marked **unverified**).
> **North Star:** make `oracledb` a **correctness-hardened engine for oraclemcp**
> that we are willing to stamp **1.0**. "Stable" here means *correct, and does
> not break oraclemcp* — **not** a frozen public contract for external crates.

Self-contained: a fresh agent can implement any task without prior context.
Every task names dependencies, rationale, skill(s), and acceptance. Converts to a
beads graph (IDs `W{n}-T{m}` / `W3-E{n}`).

---

## 1. Why this plan exists (the reframe) + Decisions log

The project shipped **0.2.2** (clean-room thin-mode Oracle driver; passes
python-oracledb's own thin suite). The question "is it *stable*, and is it just
features from here?" produced three binding decisions.

### Decisions log (binding — do not re-litigate without the human)

**D1 — Keep asupersync; keep nightly; harden it.**
`oracledb` is the **engine for oraclemcp**, which ships as a single static binary
(`README.md` "single static binary on x86_64-musl"). The nightly requirement
(asupersync's `#![feature(try_trait_v2)]`) is therefore a **build-time** detail
invisible to anyone *running* oraclemcp. There is no external "Rust dev depends
on the crate on stable" audience. Consequences:
- **Do NOT** build a stable sync-only backend or drop the async surface.
- asupersync is an **asset**: cancel-correctness underwrites the timeout/BREAK/
  RESET paths, and its **LabRuntime + DPOR** reach a bug class the single-threaded
  python suite structurally cannot.
- WS1 = *harden* the nightly story (early-warning CI + runbook), not re-architect.

**D2 — Full API audit, nothing deferred.** Make the public surface coherent and
painlessly evolvable. **Verified counts (corrected from v1):** `crates/oracledb/src/lib.rs`
has **159 `pub fn`** (not 98) and **66 `pub async fn`** (confirmed); **19
`execute_query*` variants** (10 async + 9 blocking, confirmed); **118 public
structs/enums** across the 3 published crates (not 14); **0 `#[non_exhaustive]`**
workspace-wide (confirmed).

**D3 — Full correctness arsenal to convergence; 1.0 gate; multiple bug hunts;
leverage every applicable skill.** "Stable" = correctness hardened *beyond* the
reference suite, run to convergence, and that convergence is the explicit gate
for **1.0**.

### What 1.0 means (the gate)
Tag **1.0** when **WS0 ∧ WS1 ∧ WS2 ∧ WS3** complete and WS3 reaches its
convergence bar (§5.7). A *maturity* milestone, not API immutability.

---

## 2. Grounding facts (verified)

- `oracledb-protocol` (wire/codecs): **zero** asupersync refs, **no** nightly
  features, `#![forbid(unsafe_code)]` (`oracledb-protocol/src/lib.rs:1`) — sans-io,
  stable-compatible, runnable offline.
- `oracledb` (driver): asupersync surface used is small (`Cx`, `io`,
  `net::TcpStream`, `tls::TlsStream`, `sync::Mutex`, `runtime`) but threaded
  through **109 `async fn`s**. TLS is **sans-io rustls** (`ClientConnection`);
  asupersync only wraps the stream. asupersync is vendored at **0.3.4**
  (`Cargo.lock:211`).
- `oracledb-pyshim` is `publish = false` (`crates/oracledb-pyshim/Cargo.toml:8`) —
  the conformance harness; its async path bridges Python-async→Rust via
  `spawn_blocking` (see W1-T3).
- Releases 0.2.0→0.2.2 via tag-driven `.github/workflows/release.yml`;
  `scripts/release_preflight.sh` + `scripts/publish_crates.sh`.
- Toolchain pin: `rust-toolchain.toml:13` → `nightly-2026-05-11`.
- **CI has no `schedule:`/`cron:`/`workflow_dispatch`** anywhere except
  `release.yml:19` — see WS0 (this is the missing primitive four epics need).
- Out of scope: Group-A auth/wallet GH #2/#3/#4/#6 (beads `o0b`/`qm4`/`x1p`) and
  the `57z` "beat-python" features, except W3-E6's winnowed correctness items.

---

## 3. WS0 — Foundational CI primitive (do first; unblocks WS1+WS3)

**Goal:** add the scheduled, non-blocking long-run lane that four epics depend on.
Verified gap: CI runs only on push/PR to `main`; there is **no** scheduled or
manually-dispatchable workflow (`grep` over `.github/workflows/` finds only
`release.yml:19 workflow_dispatch`).

### W0-T1 — New `nightly-long-run.yml` workflow (schedule + workflow_dispatch)
- **What:** a separate workflow with
  `on: { schedule: [{cron: "0 6 * * 1"}], workflow_dispatch: {}, pull_request: { paths: ["rust-toolchain.toml", "**/Cargo.toml", "Cargo.lock"] } }`
  that hosts every non-PR-blocking lane: floating-nightly (W1-T1), sustained fuzz
  (W3-E2), high-`PROPTEST_CASES` property runs (W3-E1), DPOR/loom (W3-E3),
  `--features cassette` replay (W3-E5). Jobs are `continue-on-error: true`; a final
  `if: failure()` step files an issue (needs `permissions: { issues: write }` —
  scoped to this workflow only; main CI stays `contents: read`).
- **Why:** keep it *separate* from required CI so its triggers/permissions don't
  perturb the required matrix; it is the single highest-leverage CI change in the
  plan (W1-T1, W3-E1/E2/E3/E5 all need it).
- **Deps:** none. **Skill:** none. **Acceptance:** workflow runs on
  `workflow_dispatch`; a forced failure opens an issue; required CI unchanged.

---

## 4. WS1 — Nightly hardening (small)

**Goal:** make the (permanent, intentional) nightly requirement robust and
self-documenting.

### W1-T1 — Floating-nightly early-warning job
- **What:** in `nightly-long-run.yml` (W0-T1), a `continue-on-error` job on
  `dtolnay/rust-toolchain@master` with `toolchain: nightly` (floating, **not** the
  pin) running `cargo build/test --workspace --exclude oracledb-pyshim`.
- **Why:** the top operational risk is a future nightly breaking asupersync's
  `try_trait_v2`; an early signal lets us re-pin deliberately, not at release time.
- **Deps:** W0-T1. **Skill:** none. **Acceptance:** job runs floating nightly;
  a deliberately bad `rust-toolchain.toml` on a branch makes it fail loudly via
  `workflow_dispatch`; failure opens an issue.

### W1-T2 — `docs/TOOLCHAIN.md` + re-pin runbook
- **What:** why nightly (asupersync/`try_trait_v2`; `rust-toolchain.toml:1-14`
  header), that it's build-time-only (oraclemcp ships a binary), how the pin is
  chosen, exact re-pin steps when W1-T1 goes red. Link from `README.md` + the
  `oracledb` skill.
- **Why:** removes recurring "why won't this build on stable?" confusion; makes
  re-pinning a checklist.
- **Deps:** W1-T1. **Skill:** none. **Acceptance:** a fresh agent can re-pin from
  it alone.

### W1-T3 — Close `nto` won't-fix
- **What:** close the sole in_progress bead with the verified D1 reason. The shim
  async path uses `spawn_blocking_task` + per-op `block_on` driving **native**
  `Connection` futures (`async_bridge.rs:126,196,209`; `async_cursor.rs:165,239,283`;
  `async_conn.rs:777`; `pool.rs:675,687,698,710`) — node-oracledb's thread-pool
  model. Fully removing it needs native PyO3↔asupersync async integration that
  does not exist upstream (**unverified** absence — bead author's claim).
  `oracledb-pyshim` is `publish=false` → zero release impact.
- **Deps:** none. **Skill:** none. **Acceptance:** `nto` closed; **no** in_progress
  beads remain (it is the only one).

---

## 5. WS2 — Full API audit (medium)

**Goal:** a coherent, intentional, **evolvable** public surface, snapshot-locked.
**Drift verdict (grounded): LOW risk.** The native `oracledb` API was never a
mirror of python-oracledb's Python API — port fidelity lives in `oracledb-protocol`
(wire/behavior, untouched here) and in the pyshim's python-oracledb-shaped surface
(`conn.rs`/`cursor.rs`, layered *on top of* the native crate). The shim calls only
**6 distinct native execute/fetch entry points**, never the 19. The guard is the
conformance suite (`harness/run.sh diff`): WS2 changes must keep it green.

### W2-T1 — Enumerate + classify the public surface (the ledger)
- **What:** run `cargo public-api` (verified to work under the pin, §W2-T6) and
  produce `docs/API_LEDGER.md`: every `pub` item × disposition (keep /
  `pub(crate)` / rename / consolidate / deprecate) + one-line reason. Adjudicate
  the **accidental-leak candidates** flagged by research: `ObsSpanGuard`
  (`obs.rs:106`), `OracleReadHalf`/`OracleWriteHalf` (`transport.rs:40,58`),
  `PoolEngine<B>` (`pool.rs:164`), `DirectPathStream`/`BatchLoadState`/
  `DirectPathPieceBuffer` (`dpl.rs:722,792,391`), `ExecutemanyManager`/`…Error`
  (`cursor_logic.rs:45,15`) — each may be deliberate; verify against intent.
- **Deps:** none. **Skill:** `oracledb` (API.md as intended-surface ref).
  **Acceptance:** ledger committed; human signs off on removals/renames.

### W2-T2 — Consolidate the execute/query family (19 → builder core)
- **Verified enumeration to subsume** (async `Connection`, `crates/oracledb/src/lib.rs`):
  `execute_query` (`:2469`), `execute_query_collect` (`:2530`),
  `execute_query_with_timeout` (`:2561`), `execute_query_with_binds` (`:2578`),
  `execute_query_with_binds_and_timeout` (`:2600`), `query` (`:2629`),
  `query_named` (`:2659`), `query_named_with_timeout` (`:2675`),
  `execute_query_with_bind_rows` (`:2694`),
  `execute_query_with_bind_rows_and_options` (`:2711`, **the real core** — also
  runs the ORA-932/1007 refetch retry `:2736`),
  `execute_query_with_bind_rows_and_timeout` (`:2941`),
  `execute_query_with_bind_rows_options_and_timeout` (`:2959`, the timeout
  super-method everything funnels through),
  `execute_query_for_registration` (`:1869`, CQN). The 3 timeout entries delegate
  to private `*_call_timeout` helpers (`:3944/3966/3991`) → so a single real core
  already exists. `BlockingConnection` mirrors all of these (`:5296`–`:5525`).
- **The fetch/paging family STAYS** (distinct lower-level capability, not execute
  sprawl): `fetch_rows*` (`:3053/3064/3127/3189/3223`), `for_each_row_ref`
  (`:3281`), `define_and_fetch_rows_with_columns` (`:3377`), `fetch_cursor` (`:3418`).
- **Design** (mirrors the proven `ConnectOptions` consuming-builder, `lib.rs:1085-1177`):
  ```rust
  pub enum Binds { None, Positional(Vec<BindValue>), Named(Vec<(String,BindValue)>), Rows(Vec<Vec<BindValue>>) }
  impl<T: IntoBinds> From<T> for Binds {}
  #[non_exhaustive] pub struct Execute<'a> { /* sql, binds, prefetch=1, timeout_ms=None, options: ExecuteOptions, materialize=false */ }
  // builder: .binds() .prefetch() .timeout_ms() .options() .collect_lobs() .registration_id()
  pub async fn execute(&mut self, cx, req: Execute) -> Result<QueryResult>;          // subsumes the 13 execute variants
  pub async fn execute_for_each(&mut self, cx, req, f) -> Result<()>;                // subsumes for_each_row_ref
  // KEEP query()/query_named() verbatim as sugar over execute().
  ```
  A full old→new mapping table goes in the PR (research produced it; nothing is
  lost). `execute_query_for_registration` folds into
  `execute(Execute::new(sql).registration_id(id))?.query_id`.
- **Deprecation:** old names become `#[deprecated]` shims for **one** release, then
  removed (AGENTS.md forbids permanent shims). Update the **~17 pyshim execute
  call-sites** (`conn.rs:142,184,333,350,368,398`; `cursor.rs:1077,1411,1420,1446,1643`;
  `async_conn.rs:136`; `async_cursor.rs:74,85,117,201`; `pool.rs:126`;
  `subscr.rs:336`) in the same change (hand edits per AGENTS.md, file-by-file).
- **Deps:** W2-T1. **Skill:** `code-simplifier`, `oracledb`. **Acceptance:** new
  API covers every old capability (mapping table in PR); **`harness/run.sh diff`
  stays green** (the anti-drift guard); deprecations land; full live suite green.

### W2-T3 — `#[non_exhaustive]` pass
- **MUST mark (verified, will grow):** `Error` (`lib.rs:679`; already grows — Arrow
  variant `#[cfg]`-gated at `:743`), `ExecuteOptions` (`types.rs:645`),
  `QueryResult` (`types.rs:537`, 18 pub fields), `ConnectOptions` (`lib.rs:987`),
  `BindValue` (`types.rs:444`), `QueryValue`/`QueryValueRef` (`types.rs:129/320`),
  `SessionlessError` (`lib.rs:890`), `PoolError` (`pool.rs:30`), `SodaError`
  (`soda/error.rs:9`), `ConversionError` (`sql_convert.rs:44`), `ArrowConversionError`
  (`arrow.rs:52`), `NotificationOutcome` (`lib.rs:1315`), `PoolConfig`/`AcquireOptions`
  (`pool.rs:64/79`), `ArrowFetchOptions` (`arrow.rs:116`), `ColumnMetadata`
  (`types.rs:47`), `ObjectType`/`ObjectAttribute`/`CollectionElement`/`DecodedObject`/
  `DbmsOutput` (`lib.rs:540,513,527,557,499`), `ProtocolError`/`SqlError`/`WalletError`/
  `CassetteError`/`DnMatchError`, the AQ option/result structs (`aq.rs:121,139,84,170,553,33`),
  SODA metadata (`soda/metadata.rs:51,11,28,40`), `BatchServerError` (`types.rs:716`).
- **NOT needed:** fixed wire-image structs (`AcceptInfo`, `AuthResponse`,
  `ClientCapabilities`, `TnsPacket`, `TtcWriter`/`Reader`, `EncryptedPassword`,
  `ServerErrorDetails`, `ClientIdentity`), `AccessToken` (opaque newtype), borrow
  types (`TypedRow<'a>`, `BorrowedFetchResult`, `DbObjectPackedReader<'a>` — grow by
  methods not fields).
- **Caveat (verified):** `#[non_exhaustive]` on all-pub-field structs (`QueryResult`,
  `ExecuteOptions`, `ColumnMetadata`, `PoolConfig`) forbids **external** struct-literal
  construction + exhaustive destructuring. The pyshim constructs/destructures these
  (e.g. `QueryResult{ ..Default::default() }` at `lib.rs:3470`) but is **in-workspace**,
  so it's unaffected. The acceptance "scratch test confirms adding a variant is
  non-breaking" must use an **out-of-workspace** crate to be meaningful.
- **Deps:** W2-T1. **Skill:** none. **Acceptance:** every grow-able type marked;
  out-of-workspace scratch test proves additivity; workspace compiles.

### W2-T4 — async↔blocking symmetry sweep
- **Verified genuine I/O gaps to fill:** `BlockingConnection::cancel` missing
  (async `:4361`; only `CancelHandle::cancel` `:4933` + `drain_cancel_response`
  exist); CQN `recv_notification`/`notify_register` (`:1816/1787`) async-only;
  `free_temp_lobs` (`:3910`) and `trim_lob` (`:3873`) have only `*_with_timeout`
  blocking twins; `execute_query_with_bind_rows_and_options` (`:2711`, no-timeout)
  async-only. **Deliberate async-only (document, don't wrap):** the zero-copy
  `_ref` family + direct-path (borrow lifetimes don't cross `block_on`). **Reversed
  gap:** `BlockingConnection::drain_cancel_response` (`:5811`) exists but the async
  `Connection::drain_cancel_response` (`:4320`) is private — make it `pub` or fold
  into the `cancel` story. Trivial `&self` accessors are correctly async-only.
- **Deps:** W2-T2 (consolidation changes the method set). **Skill:** `oracledb`.
  **Acceptance:** a generated table shows 1:1 coverage or an explicit documented
  exception; naming standardized `x`/`x_with_timeout`/`x_named`.

### W2-T5 — module/re-export coherence
- **What:** review the module tree + the `oracledb::protocol` re-export; ensure
  one obvious export path per type; tidy any prelude.
- **Deps:** W2-T1. **Skill:** `code-simplifier`. **Acceptance:** doc-tests/examples
  compile against tidied paths; no duplicate export paths.

### W2-T6 — Lock with `cargo public-api` snapshot (VERIFIED feasible)
- **Verified:** the tool isn't installed yet (`cargo install cargo-public-api
  --locked`), but its nightly-rustdoc-JSON step **runs under the pin** — research
  produced both `oracledb_protocol.json` (2.4 MB) and the full `oracledb.json`
  (2.7 MB, ~68 s, asupersync/`try_trait_v2` and all) via
  `RUSTDOCFLAGS="-Z unstable-options --output-format json" cargo
  +nightly-2026-05-11 rustdoc -p <crate> --lib`. So **R3 resolves YES**.
- **What:** CI job (in `nightly-long-run.yml` or a fast required check) diffing the
  public API vs a committed snapshot; intentional changes update it in-PR. Fallback
  if `cargo public-api` chokes: raw rustdoc-JSON diff (verified working).
- **Deps:** W2-T1..T5 (snapshot the final surface). **Skill:** none. **Acceptance:**
  CI fails on an unsnapshotted public change; snapshot committed as the 1.0 baseline.

---

## 6. WS3 — Correctness beyond the reference (large; the centerpiece)

Reframed by research: **fuzz, property tests, and cassette are already
substantially built** — WS3 is *gap-closing + convergence-formalization + the
greenfield DPOR lane*, not greenfield across the board. Each epic has a concrete
convergence criterion (resolves R1). Epics run largely in parallel; W3-E7
synthesizes the gate.

### W3-E1 — Property round-trips: close the `FromSql`/`ToSql` gap
- **Verified state:** `proptest` is already a dev-dep (`Cargo.toml:56`, 1.11);
  ~30 properties exist **at the codec layer** (`thin/proptests.rs` CASES=2048;
  `tests/codec_properties.rs` CASES=1024; `number_inline_byte_identical.rs`
  CASES=4096) covering NUMBER/date/tz/interval/binary-float/text/vector/oson/lob.
  **Gap:** there are **no** property tests at the `FromSql`/`ToSql` bridge
  (`sql_convert.rs`) — only example tests. That bridge is the real E1 deliverable.
- **The matrix (verified, all `sql_convert.rs`):** 17 `FromSql` impls + 12 `ToSql`
  impls incl. feature-gated `chrono`/`uuid`/`serde_json`/`rust_decimal` and the
  core `Vec<f32>`/`Vec<f64>` vector bridges. Encode the **verified asymmetries**:
  `f64/f32`↔`BinaryDouble/Float` is *not* a clean round-trip (`QueryValue::BinaryDouble`
  carries a server-rendered `String`, `types.rs:139`; needs live DB + tolerance);
  `String` FromSql is many-to-one; `Decimal` exact-path only for scale 0..=28;
  vector cross-format (int8→float, f64→f32) lossy + `Binary`/`Sparse` are errors;
  `bool` from `Number` only 0/1.
- **Pure vs live split (verified):** pure offline = `to_sql()`→(test-only
  `BindValue→QueryValue` adapter)→`from_sql()` for same-carrier pairs (NUMBER, text,
  raw, bool, interval, date, vector, decimal/uuid/chrono-date/json over those). Live
  `#[ignore]` (home: `live_typed.rs`) = the float-via-text pairs + server-acceptance
  edges.
- **Budgets (R1):** pure `CASES=4096`; live `CASES=256`; nightly override
  `PROPTEST_CASES=65536` (no code change). Keep `proptest-regressions/` committed.
- **Convergence:** full property suite green at the deep `PROPTEST_CASES` with no
  new shrinks added. **Deps:** W0-T1 (deep nightly lane). **Skill:**
  `multi-pass-bug-hunting` (triage). **Acceptance:** bridge proptests for every
  pair (pure) + the live `#[ignore]` matrix; green; every shrink fixed+pinned.

### W3-E2 — Fuzz: close the decoder gaps + run on a schedule
- **Verified state:** a 10-target cargo-fuzz harness exists
  (`crates/oracledb-protocol/fuzz/`, documented in `docs/FUZZING.md`, 376 lines);
  it already found+fixed **4 DoS bugs** (3 OOM-from-length, 1 sb4/sb8 negate panic),
  closed the OOM class via the `BoundedReader` trait, and has 4 committed regression
  inputs (`fuzz/regressions/*.bin`). A **differential oracle** also exists
  (`harness/differential/diff_oracle.py`, 5,944 cases, 0 divergences vs python-oracledb).
  CI only **builds** targets (`ci.yml:116-132`, `continue-on-error`), never runs
  them; the "8 targets" comment (`ci.yml:112`) is **stale** (there are 10) — fix it.
- **9 NEW targets (verified missing, all `pub`-reachable offline; only 1 needs a
  loop driver):** `parse_auth_response` (`auth.rs:255`), `parse_accept_payload`
  (`connect.rs:38`), DbObject image-walk (`DbObjectPackedReader`, `dbobject.rs:11`,
  loop `read_header`→`read_value_bytes`/`read_atomic_null`), DbObject scalar decoders
  (`dbobject.rs:371/388/523/537/484`), LOB-op responses (`lob.rs:220-326`),
  sessionless/TPC (`sessionless.rs:106/130/342/396`), `parse_oac_record`
  (`subscr.rs:379`), wallet parsers (`tls/wallet.rs:138/209/238`, `tls/sso.rs:201`,
  `tls/dn.rs:63`), `parse_query_response_borrowed` (`fetch.rs:1339`). The plan's
  explicitly-named "auth responses / DbObject image walk / LOB locators" are **all
  currently unfuzzed**.
- **Budgets (R1, tied to observed exec/s in FUZZING.md):** PR gate = build (exists)
  **+ replay the committed `fuzz/regressions/*.bin` corpus** (sub-second, makes the
  corpus an actual gate) + 60–180 s/target. Nightly (`nightly-long-run.yml`) = 1–4 h/
  target on `x86_64-unknown-linux-gnu` (ASan), corpus cached via `actions/cache`,
  crashes uploaded + issue filed. **Convergence per target:** a deep run with **zero
  new crashes AND no new libFuzzer `NEW` coverage in the final ≥30 min** (cov plateau);
  all targets clear the 1000 exec/s parser floor (already met).
- **Deps:** W0-T1. **Skill:** `multi-pass-bug-hunting`. **Acceptance:** 19 targets
  total; corpus-replay PR gate; scheduled deep run; every finding fixed + corpus'd;
  stale CI comment fixed.

### W3-E3 — DPOR / model-checked concurrency (greenfield; the asupersync asset)
Research **corrected the API**: the asupersync skill docs describe a *newer* dev
tree; the **vendored 0.3.4** has different APIs (verified against
`~/.cargo/registry/.../asupersync-0.3.4/`). Real API: `LabRuntime`/`LabConfig`
(`src/lib.rs:402`), explorers `DporExplorer`/`ScheduleExplorer`/`ExplorationReport`
(`src/lab/explorer.rs`), `Cx::for_testing()` (`src/cx/cx.rs:3232`); note
`test_utils::run_test` does **not** use the lab (it's a production runtime). Coverage
saturation primitives: `CoverageMetrics::is_saturated(window)` / `discovery_rate()`
(`explorer.rs:165/156`). Authoring uses `DporExplorer::new(ExplorerConfig::new(seed,
max_runs)).explore(|rt: &mut LabRuntime| { rt.state.create_root_region(...); /* spawn
*/ rt.run_until_quiescent(); })`, assert `!report.has_violations()`.

**Load-bearing obstacle (verified):** the driver's real-socket I/O **prevents lab
determinism** today and there is **no ready injection point** — `OracleReadHalf`/
`OracleWriteHalf` are a closed enum over real fds (`transport.rs:40-72`);
asupersync's owned halves are fd-bound; `VirtualTcpStream` can't become an
`OwnedReadHalf`; `LabReactor` is event-injection/virtual-time only. So a transport
**mock seam is a prerequisite**.

#### W3-E3a — Mock transport seam (prerequisite)
- **What:** add `#[cfg(any(test, feature="testkit"))]` `Mock` variants to
  `OracleReadHalf`/`OracleWriteHalf` — a scripted byte source that can return
  `Poll::Pending` at chosen offsets + a write recorder — wired into `poll_read`/
  `poll_write` exactly like the existing cassette `Replay` arms (`transport.rs:151/171`).
  Add a `CancelHandle::cancel(&Cx)` async variant (today `CancelHandle::cancel`
  `:4933` builds its own runtime, un-lab-drivable; keep the sync facade).
- **Why:** lets a `Connection` be built over an in-memory transport and driven under
  `LabRuntime` with zero sockets (mirrors the proven cassette `Replay` flow).
- **Deps:** none. **Skill:** `asupersync-mega-skill`. **Acceptance:** a `Connection`
  runs connect/execute/fetch over a scripted mock under `LabRuntime` to quiescence.

#### W3-E3b — Wire/cancel DPOR (asupersync)
- **Paths (verified file:line + invariant):** **P1** call-timeout→BREAK→drain(RESET)→
  reuse (`break_and_drain_wire` `:6077`, `drain_break_response` `:6234`; invariant:
  next op reads its own response, `dead` correct). **P2** cancel-then-reuse +
  drop-cancel auto-drain (`CancelDrainGuard` `:6198`, `drain_pending_cancel` `:3003`;
  invariant: exactly-once drain, session alive, cancellation-protocol oracle green).
  **P3** shared write-mutex: `CancelHandle` BREAK vs main RESET (`lock_write` `:5922`;
  bounded). **P4** close-cursors piggyback vs `in_use_cursors` (`take_close_cursors_
  piggyback`, `:1246/1255`; `ScheduleExplorer` state-machine). **P5** speculative
  prefetch drop (`:3171`).
- **Stopping (R1):** P1/P2/P5 **exhaustive** = `DporExplorer` work-queue empties under
  `max_runs` (cap 100k) with zero violations; P4 exhaustive via `ScheduleExplorer`
  over the finite op-permutations; P3 **bounded** = `is_saturated(window=512)` &&
  `discovery_rate()<0.005`, zero violations. Emit `ExplorationReport::write_json_summary()`
  as CI artifacts (+ reproducer seeds).
- **Deps:** W3-E3a, W0-T1. **Skill:** `asupersync-mega-skill`, `multi-pass-bug-hunting`.
  **Acceptance:** all listed paths clean; obligation-leak/futurelock/cancellation
  oracles green; recorded marker stream is a legal BREAK/RESET sequence under every
  interleaving.

#### W3-E3c — Pool model-check via `loom` (NOT asupersync)
- **Verified:** `pool.rs` is sans-io `std::sync` (`Mutex`+`Condvar`+bg thread,
  `:154-162`), not asupersync tasks → it belongs to **loom**, not LabRuntime. `loom`
  is not yet a dep (dev-deps has only `allocation-counter`).
- **What:** add `loom` dev-dep + a `cfg(loom)` shim over `std::sync` in `pool.rs`;
  model-check `acquire`/`return_connection`/bg-reap/ping-fail/max-lifetime over a
  mock `PoolBackend` (`pool.rs:90-109`). Invariants: no double-hand-out;
  `open_count == free+busy+to_drop`; reaped conn never returned to a waiter; no
  Condvar lost-wakeup; `close(force)` drains all.
- **Stopping (R1):** loom default budget (or `LOOM_MAX_PREEMPTIONS=3`) clean.
- **Deps:** W0-T1. **Skill:** `multi-pass-bug-hunting`. **Acceptance:** loom suite
  green; invariants asserted.

### W3-E4 — Multiple multi-pass bug-hunt sweeps
- **What:** run the `multi-pass-bug-hunting` cycle (audit→fix→rescan→fresh-eyes→
  integration→verify) over protocol/codec/multi-packet/async paths; **several**
  independent fresh-eyes passes (per D3) until a full pass yields zero new findings.
  Consumes E1/E2/E3 findings; log per pass.
- **Why:** single-pass misses bugs hidden behind bugs (the edition-under-token HIGH
  was found this way).
- **Deps:** runs alongside E1–E3. **Skill:** `multi-pass-bug-hunting` (primary).
  **Acceptance:** ≥2 consecutive full passes, zero new findings; all fixed; log
  committed.

### W3-E5 — Cassette-replay through the full `Connection` + CI
- **Verified state (far more built than v1 assumed):** the full record/replay seam
  exists — wire format `oracledb-protocol/src/net/cassette.rs` (magic/version/frames,
  strict decode), transport decorators `Recording`/`Replay` (`transport.rs:49-71`),
  zero-API capture via `capture_scope()` thread-local (`:374-400`), a committed real
  fixture (`tests/fixtures/cassettes/select_7_plus_5.tns-cassette`), a live-capture
  test + an **offline** replay test that reproduces `select 7+5`=12 with no socket
  (`tests/cassette_record_replay.rs`).
- **Gaps (the real E5 work):** (1) replay is **not wired into `Connection::connect`**
  — the offline test re-implements framing + calls the bare decoder. Add a test-only/
  `cfg(feature="cassette")` `Connection::connect_over((read, write), ...)` seam
  (`connect()` builds the transport at `lib.rs:1375-1396` with no inject point) so the
  **whole** state machine runs against a cassette — the largest E5 task. (2) **CI
  never enables `cassette`** (`grep`: zero refs) — add `--features cassette` to a
  job (cheap, high-value). (3) corpus is one scenario — add fixtures (binds,
  multi-packet, LOB, DML/rowcount, error responses, E1 datatypes), each via a
  live-capture run. (4) make the "altered-decoder-fails" guard explicit once replay
  runs through `Connection`.
- **Deps:** W0-T1; benefits from E1–E4. **Skill:** `testing-conformance-harnesses`
  if available, else `multi-pass-bug-hunting`. **Acceptance:** a real `Connection`
  replays the corpus offline in CI (`--features cassette`); zero live-DB dependency;
  a deliberately altered decoder fails a cassette test.

### W3-E6 — (optional) idea-wizard differentiator correctness features
- **What:** one `idea-wizard` pass for *accretive* correctness/robustness features
  beyond the reference (richer typed error classification, stricter bind validation,
  …); winnow hard; cross-check `57z` to avoid dup; file as beads, implement only
  sign-off'd items.
- **Deps:** after E1–E4. **Skill:** `idea-wizard`. **Acceptance:** winnowed beads
  under `57z`; nothing speculative implemented.

### W3-E7 — Convergence synthesis = the 1.0 gate
- **What:** one report on one commit confirming simultaneously: E1 green at deep
  `PROPTEST_CASES` (no new shrinks); E2 all 19 targets converged (cov plateau + zero
  crashes) + differential oracle 0 divergences; E3b DPOR exhaustive/saturated clean +
  E3c loom clean (with the JSON artifacts + reproducer seeds); E4 ≥2 zero-finding
  passes; E5 cassette CI green. Every bug across E1–E6 fixed.
- **Deps:** E1, E2, E3 (a,b,c), E4, E5. **Skill:** `multi-pass-bug-hunting`
  (completeness critic). **Acceptance:** convergence report committed; all gates
  green on the release commit.

---

## 7. WS4 — The 1.0 release

### W4-T1 — Tag 1.0 once WS0∧WS1∧WS2∧WS3 complete
- **Verified bump surface (3 pins, not 1):** `Cargo.toml:11`
  `[workspace.package] version` (single source of truth; all crates use
  `version.workspace = true`), **plus two hardcoded literals** in
  `crates/oracledb/Cargo.toml:71` (`oracledb-derive = { …, version = "0.2.2" }`)
  and `:72` (`oracledb-protocol = { …, version = "0.2.2" }`). `oracledb-pyshim` has
  no `version =` on its path-dep → no edit. Update `Cargo.lock` (`--locked`).
- **Add a preflight guard (verified gap):** `release_preflight.sh` checks *package*
  versions agree (`:32-42`) but **not** that the inter-crate *dependency
  requirements* (`oracledb/Cargo.toml:71-72`) equal the workspace version — a bump
  missing line 71/72 passes preflight but publishes a broken dep graph (we hit
  exactly this risk at 0.2.1/0.2.2). Add the check.
- **What:** bump to `1.0.0`, run the pipeline (gates→musl binary→crates.io→GH
  release), README/CHANGELOG state what 1.0 means (correctness-hardened engine;
  nightly build-time req; API audited + snapshot-locked).
- **Deps:** W0, W1-T1..T3, W2-T1..T6, W3-E7. **Skill:** none. **Acceptance:**
  1.0.0 published; release notes state the gate; the `cargo public-api` snapshot is
  the 1.0 baseline.

---

## 8. Dependency graph (DAG)

```
W0-T1 (scheduled CI)  ─► W1-T1 ─► W1-T2
                       └► W3-E1, W3-E2, W3-E3(a→b; c), W3-E5   (all need the long-run lane)
W1-T3 (close nto)  — independent

W2-T1 ─► W2-T2 ─► W2-T4
      ├► W2-T3
      └► W2-T5
   (T2,T3,T4,T5) ─► W2-T6 (snapshot final surface)

W3-E3a ─► W3-E3b ;  W3-E3c independent (loom)
W3-E1, W3-E2, W3-E3* ─► W3-E4 ─► W3-E6
(E1,E2,E3*,E4,E5) ─► W3-E7

(W0) ∧ (W1-T1..T3) ∧ (W2-T1..T6) ∧ (W3-E7) ─► W4-T1
```
No cycles; every task feeds another or the 1.0 gate.

---

## 9. Skill leverage map (D3)

| Skill | Where |
|---|---|
| `planning-workflow` | this doc; review rounds; beads conversion |
| `asupersync-mega-skill` | **W3-E3a/b** LabRuntime/DPOR (the unique asset); WS1 framing |
| `multi-pass-bug-hunting` | **W3-E4** (primary); triage across E1/E2/E3/E5/E7 |
| `oracledb` | W2 (intended surface / behavior preservation); WS1 docs |
| `code-simplifier` | W2-T2 (execute consolidation), W2-T5 (module tidy) |
| `idea-wizard` | W3-E6 differentiator features (winnowed) |
| `testing-conformance-harnesses` | W3-E5 cassette (if available) |

---

## 10. Sequencing (waves)

- **Wave 0:** W0-T1 (scheduled CI), W1-T1/T2/T3, W2-T1 (ledger). Cheap, unblocks all.
- **Wave 1 (parallel):** W2-T2/T3/T5 ‖ W3-E1, W3-E2, W3-E3a, W3-E3c.
- **Wave 2:** W2-T4→W2-T6; W3-E3b (after E3a); W3-E4 (consuming E1–E3).
- **Wave 3:** W3-E5, W3-E6 → W3-E7.
- **Wave 4:** W4-T1 (tag 1.0).
WS2 and WS3 run concurrently; only the 1.0 tag joins them.

---

## 11. Risks & open questions — status after research round 1

- **R1 — convergence budgets:** RESOLVED with concrete numbers — fuzz: cov-plateau
  (no new `NEW` in final ≥30 min) + zero crashes, per-target deep budgets 1–4 h;
  property: deep `PROPTEST_CASES=65536`, no new shrinks; DPOR: exhaustive
  queue-drain (P1/P2/P4/P5) or `is_saturated(512)`+`discovery_rate<0.005` (P3); pool:
  loom default/`LOOM_MAX_PREEMPTIONS=3`.
- **R2 — execute consolidation churns oraclemcp:** RESOLVED — ~17 shim call-sites
  reduce to 6 native methods via 5 shim helpers; `#[deprecated]` shims keep the
  suite green during cut-over; remove shims only after `harness/run.sh diff` is clean.
- **R3 — `cargo public-api` under nightly:** RESOLVED YES (rustdoc-JSON verified to
  run under the pin; raw-JSON fallback verified).
- **R4 — scope discipline:** Group-A (#2/#3/#4/#6) + `57z` stay out except E6's
  winnowed items.
- **R5 — asymptote → reachable "good enough":** RESOLVED via the per-technique
  stopping criteria above (plateau/saturation/queue-drain), each emitting a
  committed artifact for W3-E7.
- **R6 (new) — DPOR needs a transport seam + `CancelHandle(&Cx)`:** captured as the
  prerequisite **W3-E3a**; pool split to `loom` (**W3-E3c**).
- **R7 (new) — E5 replay isn't wired through `Connection`:** captured as the largest
  E5 task (the `connect_over` seam).
- **Unverified flags to resolve in W2-T1 / review:** whether the accidental-leak
  candidates (`PoolEngine`, `DirectPathStream`, `ExecutemanyManager`, …) are intended
  public; the absence of a native PyO3↔asupersync async bridge (W1-T3 rationale).

---

## 12. Next steps (planning-workflow)

1. **Review rounds (≥4):** run the plan-review prompt against a strong reasoning
   model; integrate; repeat to steady-state. (This is post-round-1: research already
   corrected counts, reframed E1/E2/E5, and surfaced R6/R7.)
2. **Validation loop each round:** self-containment, DAG (no cycles/orphans),
   justification, steady-state diff.
3. **Convert to beads** with the DAG intact (`W{n}-T{m}`/`W3-E*` → ids + `br dep`).
4. **Implement** Wave 0 → Wave 4; tag 1.0 at W4-T1.
