# rust-oracledb — Road to 1.0

> **Status:** planning (v1, pre-review). Authored via `/planning-workflow`.
> **North Star:** make `oracledb` a **correctness-hardened engine for oraclemcp**
> that we are willing to stamp **1.0**. "Stable" here means *correct, and does
> not break oraclemcp* — **not** a frozen public contract for external crates.

This document is self-contained: a fresh agent can implement any task below
without prior context. Every task names its dependencies, its rationale, the
skill(s) that power it, and its acceptance criteria. It converts directly to a
beads graph (IDs `W{n}-T{m}`).

---

## 1. Why this plan exists (the reframe)

The project shipped 0.2.2 (clean-room thin-mode Oracle driver, passes
python-oracledb's own thin suite). The open question was "is it *stable*, and is
it just features from here?" Working that question to ground produced three
decisions that reshape what "1.0 / stable" should mean.

### Decisions log (binding — do not re-litigate without the human)

**D1 — Keep asupersync; keep nightly; harden it.**
`oracledb` is the **engine for oraclemcp**, which ships as a single static
binary. Therefore the nightly-Rust requirement (asupersync uses
`#![feature(try_trait_v2)]`) is a **build-time** detail invisible to anyone
*running* oraclemcp. There is no external "Rust dev depends on the crate on
stable" audience to serve. Consequences:
- **Do NOT** build a stable sync-only backend or drop the async surface — that
  solved for an audience that does not exist, at the cost of asupersync's value.
- asupersync is an **asset**: its cancel-correctness underwrites the
  timeout/BREAK/RESET paths, and its **LabRuntime + DPOR** deterministic
  concurrency testing reaches a bug class the python suite structurally cannot.
- WS1 is therefore small: *harden* the nightly story (pin + multi-nightly CI +
  docs), not re-architect it.
- Rationale for why async stays even though most use is via `BlockingConnection`:
  the async `Connection` is the *primary* implementation and `BlockingConnection`
  is a thin `block_on` facade over it; ripping out async would be a net rewrite
  for zero user benefit. Keep both.

**D2 — Full API audit, nothing deferred.**
Even without an external contract to freeze, the public surface must be
*coherent and painlessly evolvable* for oraclemcp and our own future work.
Verified state to fix: **98 `pub fn`, 66 async methods, 19 `execute_query*`
variants, 14 public structs/enums, 0 `#[non_exhaustive]`.** The whole surface
gets audited, consolidated, made symmetric (async↔blocking), and snapshot-locked.

**D3 — Full correctness arsenal to convergence; 1.0 gate; multiple bug hunts;
leverage every applicable skill.**
"Earns the word stable" = correctness hardened *beyond* the reference suite, run
to convergence, and that convergence is the explicit gate for tagging **1.0**.

### What 1.0 means (the gate)
Tag **1.0** when **WS1 ∧ WS2 ∧ WS3** are complete and WS3 has reached its
convergence bar (§5.7). 1.0 is a *maturity* milestone ("correctness-hardened
engine"), not a promise of API immutability to external consumers.

---

## 2. Grounding facts (verified against the tree, 2026-06)

- `oracledb-protocol` (wire/codecs): **zero** asupersync refs, **no** nightly
  features — already stable-compatible, sans-io.
- `oracledb` (driver): asupersync surface actually used is small — `Cx`,
  `io`, `net::TcpStream`, `tls::TlsStream`, `sync::Mutex`, `runtime` — but
  threaded through **109 `async fn`s**. TLS is **sans-io rustls**
  (`ClientConnection`); asupersync only wraps the stream.
- `oracledb-pyshim` is `publish = false` (the conformance harness). Its async
  path bridges Python-async → Rust via `spawn_blocking`; the `nto` bead (native
  async bridge) is **moot** under D1 and should be closed as won't-fix.
- Releases shipped: 0.2.0 → 0.2.1 → 0.2.2 via tag-driven
  `.github/workflows/release.yml` (gates → static musl binary → crates.io →
  GitHub release). `scripts/release_preflight.sh` validates the 3 published
  crates + tag/version match.
- Open oraclemcp GH issues: #2/#3/#4/#6 (auth/wallet — Group A; **out of scope**
  for this plan, tracked by beads `o0b`/`qm4`/`x1p`). This plan is WS1–WS3 only.

---

## 3. WS1 — Nightly hardening (small)

**Goal:** make the (intentional, permanent) nightly requirement robust and
self-documenting, so a nightly toolchain bump cannot silently break a build and
so the constraint is unambiguous to anyone compiling oraclemcp.

### W1-T1 — Multi-nightly CI early-warning matrix
- **What:** add a non-blocking CI job that builds + tests the workspace on
  `nightly` (floating, latest) in addition to the pinned `nightly-2026-05-11`,
  on a weekly schedule and on PRs touching `rust-toolchain.toml` or asupersync.
  Surface breakage as an issue, not a red required check (the pinned toolchain
  stays the source of truth).
- **Why:** the single biggest operational risk is a future nightly breaking
  asupersync's `try_trait_v2` usage. An early-warning signal lets us re-pin
  deliberately instead of discovering breakage at release time.
- **Deps:** none. **Skill:** none. **Acceptance:** CI job green on the pinned
  toolchain; the floating-nightly job runs and reports; a deliberately bad pin
  makes it fail loudly in a dry run.

### W1-T2 — Document the nightly contract + re-pin runbook
- **What:** a short `docs/TOOLCHAIN.md`: why nightly (asupersync/try_trait_v2),
  that it is build-time-only (oraclemcp ships a binary), how the pin is chosen,
  and the exact steps to re-pin when the floating-nightly job goes red. Link it
  from `README.md` and the `oracledb` skill.
- **Why:** removes the recurring "why won't this build on stable?" confusion and
  makes re-pinning a checklist, not an investigation.
- **Deps:** W1-T1 (runbook references the matrix). **Skill:** none.
  **Acceptance:** doc exists, linked from README + skill; a fresh agent can
  re-pin from it alone.

### W1-T3 — Close `nto` as won't-fix (record the decision)
- **What:** close bead `nto` with the D1 rationale: the PyO3↔asupersync "native
  async bridge" is unnecessary — the shim's `spawn_blocking` bridge is the
  correct design (matches node-oracledb's C thread-pool model), and oracledb is
  oraclemcp's engine so no native-async public consumer exists.
- **Why:** stop carrying a phantom obligation; keep the bead graph honest.
- **Deps:** none. **Skill:** none. **Acceptance:** `nto` closed with rationale;
  no in_progress beads remain except those this plan opens.

---

## 4. WS2 — Full API audit (medium) → painless evolvability

**Goal:** a coherent, intentional, **evolvable** public surface, snapshot-locked
so drift is visible. Not an external freeze — an internal contract that lets us
add to `Error`, options, and types *without* breaking changes, and that collapses
organic sprawl.

### W2-T1 — Enumerate + classify the entire public surface (audit ledger)
- **What:** install/run `cargo public-api`; produce a ledger of every `pub`
  item across all 3 published crates. For each: **keep public / make
  `pub(crate)` / rename / consolidate / deprecate**, with a one-line reason.
  Flag accidental leaks (internal helpers that are `pub`).
- **Why:** you cannot audit what you have not enumerated; the ledger is the
  driving artifact for the rest of WS2 and the input to the snapshot test.
- **Deps:** none. **Skill:** `oracledb` (API.md as the intended-surface
  reference). **Acceptance:** a committed `docs/API_LEDGER.md`; every public item
  has a disposition; the human signs off on items proposed for removal/rename.

### W2-T2 — Consolidate the `execute_query*` family (19 → coherent core)
- **What:** design a minimal coherent set replacing the 19 `execute_query*`
  variants — most likely a small number of methods plus an
  `ExecuteOptions`-style builder (prefetch, binds, bind-rows, timeout,
  batch-errors, DML-rowcounts, scrollable). Keep thin `#[deprecated]` shims for
  the old names for one release to ease the oraclemcp cut-over, then remove.
- **Why:** 19 near-duplicate entry points is the worst sprawl in the surface;
  it confuses callers and multiplies maintenance + test cost. A builder makes the
  matrix of options explicit and future-proof.
- **Deps:** W2-T1. **Skill:** `code-simplifier` (consolidation),
  `oracledb` (preserve documented behavior). **Acceptance:** new API covers
  every capability the 19 variants had (mapping table in the PR); oraclemcp
  call-sites updated; old names deprecated; full live suite still green.

### W2-T3 — `#[non_exhaustive]` pass on evolvable types
- **What:** add `#[non_exhaustive]` to every public enum/struct we expect to
  grow — `Error`, `ErrorKind`/classification types, options structs, any result
  metadata type — and audit `match` sites for the resulting wildcard arms.
- **Why:** without it, adding an `Error` variant or an options field post-1.0 is
  a *breaking* change. This is the cheap insurance that makes "just add features"
  literally true for the surface.
- **Deps:** W2-T1. **Skill:** none. **Acceptance:** every grow-able type marked;
  workspace compiles; a scratch test confirms adding a variant is non-breaking.

### W2-T4 — async ↔ blocking symmetry sweep
- **What:** verify every async `Connection` method has a `BlockingConnection`
  wrapper and vice-versa; fill gaps; standardize naming
  (`x` / `x_with_timeout` / `x_named`) so the two surfaces mirror exactly.
- **Why:** asymmetry is a silent papercut (a feature exists async-only or
  sync-only). Symmetry makes the surface predictable and the docs trivial.
- **Deps:** W2-T2 (consolidation changes the method set). **Skill:** `oracledb`.
  **Acceptance:** a generated table shows 1:1 async↔blocking coverage; no method
  exists on only one surface without an explicit, documented reason.

### W2-T5 — Module structure + re-export coherence
- **What:** review the module tree and the `oracledb::protocol` re-export story;
  ensure types are exported from one obvious place; tidy `prelude` if warranted.
- **Why:** a coherent import story is part of the contract; scattered re-exports
  age badly.
- **Deps:** W2-T1. **Skill:** `code-simplifier`. **Acceptance:** doc-tests and
  examples compile against the tidied paths; no duplicate export paths for the
  same type.

### W2-T6 — Lock the surface with a `cargo public-api` snapshot test in CI
- **What:** add a CI check that diffs the current public API against a committed
  snapshot; intentional changes update the snapshot in the same PR.
- **Why:** turns "accidental API drift" into a caught, reviewable event — the
  durable guarantee that the audited surface stays audited.
- **Deps:** W2-T1..T5 (snapshot the *final* surface). **Skill:** none.
  **Acceptance:** CI fails on an unsnapshotted public change; passes on a matching
  snapshot; snapshot committed.

---

## 5. WS3 — Correctness beyond the reference (large) → the centerpiece

**Goal:** prove `oracledb` is *more* correct than "as-correct-as-python-oracledb"
by exercising the paths the reference suite structurally cannot, **to
convergence**, with every finding fixed. This is the work that earns "stable".

Each technique below is an epic with a convergence criterion. They are largely
parallel; the synthesis (§5.7) is the 1.0 gate.

### W3-E1 — Property-based round-trip tests (every `FromSql`/`ToSql` pair)
- **What:** `proptest`-based round-trip: for every supported type
  (NUMBER/Decimal, VARCHAR2/NVARCHAR2, DATE/TIMESTAMP[/TZ/LTZ], INTERVAL, RAW,
  BOOLEAN, LOB text, BINARY_FLOAT/DOUBLE, VECTOR, JSON/OSON, and feature-gated
  chrono/uuid/serde_json bridges), generate random values, bind them, fetch them
  back, assert value-preservation incl. NULL, boundary, and precision edges.
  Where a live DB is needed, gate `#[ignore]`; where the codec is sans-io
  (oracledb-protocol), test it offline.
- **Why:** the python suite tests *example* values; property testing finds the
  precision/rounding/encoding edges (NUMBER scale, TZ regions, multibyte CHAR
  semantics) that examples miss. Bead `p5h`.
- **Deps:** none (codec layer is sans-io). **Skill:** `multi-pass-bug-hunting`
  (triage findings). **Acceptance:** a generator+round-trip for every pair;
  green; every discovered mismatch fixed and pinned with a regression case.

### W3-E2 — Fuzzing every wire decoder
- **What:** extend the existing fuzz harness (`docs/FUZZING.md`) to cover every
  decoder entry point (TTC messages, packet framing, NUMBER/date/interval codecs,
  DbObject image walk, OSON/JSON, vector, LOB locators, auth responses). Run to a
  sustained budget in CI (e.g. scheduled long run) and fix every
  crash/panic/OOM/timeout. Add each crashing input as a corpus regression.
- **Why:** decoders consume server- (and potentially attacker-) controlled bytes;
  `#![forbid(unsafe_code)]` rules out UB but **not** panics or OOM-from-length.
  Fuzzing is the only systematic way to prove the bounded-decode invariants hold.
- **Deps:** none. **Skill:** `multi-pass-bug-hunting`. **Acceptance:** every
  decoder has a fuzz target; sustained run clean; corpus committed; any finding
  fixed with a regression input.

### W3-E3 — DPOR / deterministic concurrency tests (the asupersync asset)
- **What:** use asupersync's `LabRuntime` + DPOR (see asupersync-mega-skill
  `LAB-TRACE-DPOR.md`, `TESTING-FORENSICS.md`) to exhaustively explore
  interleavings of the concurrency-sensitive paths: call-timeout → BREAK → RESET
  → reuse, cancel-then-reuse, the close-cursors piggyback vs in-use cursors,
  shared-connection mutex acquisition, and pool acquire/return/ping/max-lifetime.
  Assert cancel-correctness and clean wire state under every interleaving.
- **Why:** this is the bug class the python suite **cannot** reach (it is
  single-threaded asyncio) and the class that has already bitten us (the 4116
  mutex-recursion hang; the call-timeout wire-poisoning fix). asupersync was
  built for exactly this — it is the highest-leverage WS3 epic.
- **Deps:** none. **Skill:** `asupersync-mega-skill` (LabRuntime/DPOR),
  `multi-pass-bug-hunting`. **Acceptance:** DPOR tests over each listed path;
  clean; any discovered interleaving bug fixed and pinned as a deterministic test.

### W3-E4 — Multiple multi-pass bug-hunt sweeps
- **What:** run the `multi-pass-bug-hunting` cycle (audit → fix → rescan →
  fresh-eyes → integration → verify) over the protocol/codec/multi-packet/async
  paths. Run **several independent passes** (per D3), each with fresh eyes, until
  a full pass yields zero new findings. Document findings per pass.
- **Why:** single-pass review misses bugs hidden behind other bugs; the recent
  edition-under-token HIGH was found exactly this way. Multiple passes to
  zero-new-findings is the convergence signal.
- **Deps:** runs alongside E1–E3 (their findings feed it). **Skill:**
  `multi-pass-bug-hunting` (primary). **Acceptance:** ≥2 consecutive full passes
  with zero new findings; all confirmed findings fixed; per-pass log committed.

### W3-E5 — Cassette-replay deterministic CI (bead `1s2`)
- **What:** record real Oracle wire traces (via the `cassette` feature seam) for
  a representative operation set and replay them deterministically in CI with no
  live DB. Lock correctness against regression offline.
- **Why:** makes the hard-won correctness *durable* — every future change is
  diffed against real recorded server behavior without needing a container, and
  it closes the deterministic-CI gap.
- **Deps:** benefits from E1–E4 (record the scenarios they harden). **Skill:**
  `testing-conformance-harnesses` if available; else `multi-pass-bug-hunting`.
  **Acceptance:** cassette corpus committed; CI replays green with zero live-DB
  dependency; a deliberately altered decoder makes a cassette test fail.

### W3-E6 — (optional, idea-wizard) differentiator correctness features
- **What:** a single `idea-wizard` pass to surface *accretive* correctness/robust-
  ness features that would make oracledb meaningfully better than the reference
  (e.g. richer typed error classification, stricter bind validation). Winnow to a
  short list; only adopt items that are clearly net-positive and in-scope.
- **Why:** "beyond the reference" is partly about robustness features, not only
  bug-finding; idea-wizard is the structured way to generate+winnow these without
  scope creep. Cross-check against the existing `57z` epic to avoid duplication.
- **Deps:** after E1–E4 (informed by what the hunts reveal). **Skill:**
  `idea-wizard`. **Acceptance:** a winnowed list filed as beads under `57z`;
  nothing implemented speculatively without sign-off.

### W3-E7 — Convergence synthesis = the 1.0 gate
- **What:** a final synthesis confirming the convergence bar holds simultaneously:
  E1 green for all pairs; E2 fuzz clean for the sustained budget; E3 DPOR clean on
  all listed paths; E4 ≥2 zero-finding passes; E5 cassette CI green. Every bug
  found across E1–E6 fixed. Then cut **1.0** (§6).
- **Why:** convergence is only meaningful checked *together* at one point in time
  on one commit; this is the explicit, auditable gate.
- **Deps:** E1, E2, E3, E4, E5. **Skill:** `multi-pass-bug-hunting`
  (completeness critic). **Acceptance:** a checked-in convergence report; all
  gates green on the release commit.

---

## 6. The 1.0 release

### W4-T1 — Tag 1.0 once WS1 ∧ WS2 ∧ WS3 are complete
- **What:** bump workspace to `1.0.0`, run the existing release pipeline
  (gates → static musl binary → crates.io → GitHub release), update
  README/CHANGELOG to state what 1.0 means (correctness-hardened engine; nightly
  build-time requirement; API audited + snapshot-locked).
- **Why:** 1.0 signals the maturity milestone defined by this plan.
- **Deps:** W1-T1..T3, W2-T1..T6, W3-E7. **Skill:** none (existing release
  machinery). **Acceptance:** 0.x→1.0.0 published; release notes state the gate;
  `cargo public-api` snapshot is the 1.0 baseline going forward.

---

## 7. Dependency graph (DAG)

```
WS1 (independent, do early/cheap):
  W1-T1 ─► W1-T2
  W1-T3 (independent)

WS2 (audit-first, then parallel, snapshot last):
  W2-T1 ─► W2-T2 ─► W2-T4
        ├► W2-T3
        └► W2-T5
  (W2-T2,T3,T4,T5) ─► W2-T6   (snapshot the final surface)

WS3 (E1–E4 parallel; E4 consumes their findings; E5 then E6; E7 gates):
  W3-E1 ┐
  W3-E2 ├─► W3-E4 ─► W3-E6
  W3-E3 ┘                 \
  (E1,E2,E3,E4) ─► W3-E5 ──► W3-E7
                                  \
1.0:  (W1-T1..T3) ∧ (W2-T1..T6) ∧ (W3-E7) ─► W4-T1
```

No cycles. Orphan check: every task feeds either another task or the 1.0 gate.

---

## 8. Skill leverage map (per D3: "leverage as many skills as possible")

| Skill | Where it powers the work |
|---|---|
| `planning-workflow` | this document; review rounds; beads conversion |
| `asupersync-mega-skill` | **W3-E3** DPOR/LabRuntime concurrency tests (the unique asset); WS1 framing |
| `multi-pass-bug-hunting` | **W3-E4** (primary), and triage across E1/E2/E3/E5/E7 |
| `oracledb` | W2 (intended surface / behavior preservation); WS1 docs link |
| `code-simplifier` | W2-T2 (execute consolidation), W2-T5 (module tidy) |
| `idea-wizard` | W3-E6 differentiator features (winnowed, non-speculative) |
| `testing-conformance-harnesses` | W3-E5 cassette-replay (if available) |

---

## 9. Sequencing (waves)

- **Wave 0 (cheap, immediate):** W1-T1, W1-T2, W1-T3, W2-T1 (the audit ledger).
- **Wave 1 (parallel):** W2-T2/T3/T5 (API consolidation) ‖ W3-E1/E2/E3 (property,
  fuzz, DPOR kick off independently of the API work).
- **Wave 2:** W2-T4 (symmetry, after consolidation) → W2-T6 (snapshot);
  W3-E4 (bug-hunt sweeps, consuming E1–E3 findings).
- **Wave 3:** W3-E5 (cassette), W3-E6 (idea-wizard) → W3-E7 (convergence).
- **Wave 4:** W4-T1 (tag 1.0).

WS2 and WS3 are independent and run concurrently; only the 1.0 tag joins them.

---

## 10. Risks & open questions (to resolve in review rounds)

- **R1 — Fuzz/DPOR budgets are open-ended.** "Convergence" needs concrete
  numbers (fuzz wall-clock / exec count; which DPOR paths are exhaustive vs
  bounded). *Decide in review round 1.*
- **R2 — execute-API consolidation churns oraclemcp.** Mitigated by deprecated
  shims for one release; confirm oraclemcp's cut-over window.
- **R3 — `cargo public-api` on a nightly-only crate.** Verify the tool runs under
  the pinned nightly; if not, find the equivalent.
- **R4 — Scope discipline:** Group-A auth/wallet (#2/#3/#4/#6) and the broader
  `57z` "beat-python" features are **out of scope** here except W3-E6's winnowed
  correctness items. Keep them out to keep the 1.0 gate reachable.
- **R5 — Convergence is asymptotic.** Define "good enough" per technique so 1.0
  is reachable, not perpetually one-more-pass away.

---

## 11. Next steps (per planning-workflow)

1. **Review rounds (≥4):** run §"THE EXACT PROMPT — Plan Review" against a strong
   reasoning model; integrate; repeat to steady-state. Resolve R1–R5.
2. **Validation loop each round:** self-containment, dependency-graph (no
   cycles/orphans), justification, steady-state diff.
3. **Convert to beads** with the DAG intact (`W{n}-T{m}` → bead ids + `br dep`
   edges), so implementation agents pull ready work via `br ready --json`.
4. **Implement** Wave 0 → Wave 4. Tag 1.0 at W4-T1.
