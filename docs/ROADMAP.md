# Archived Roadmap — 2026-06 (post-Wave-2)

> **Historical planning record.** This plan predates the 0.7.x–0.8.4
> implementation work. Its Wave and status claims apply only to the June 2026
> snapshot; they are not current feature or release guidance.
>
> **Current status (2026-07-16).** The workspace has a prepared, unpublished
> 0.8.4 candidate. TCPS and wallet support are implemented and tested; the
> 2,462 / 2,578 parity result is historical qualification evidence, not a fresh
> candidate run. See [CURRENT_ROADMAP.md](CURRENT_ROADMAP.md) for the current
> plan, [SUPPORT.md](SUPPORT.md) for support boundaries, and
> [PUBLISHING.md](PUBLISHING.md) for release state.

Historical forward plan for reaching the goal: a real, certified pure-Rust port
of python-oracledb thin mode. Companion to `docs/GROUND_TRUTH.md` (the
contemporaneous state snapshot) and `PLAN_TO_PORT_PYTHON_ORACLEDB_THIN_TO_RUST.md`
(the milestone contract). Beads: epic `rust-oracledb-j0o`; wave beads listed per
section.

## Two finish lines (do not conflate)

1. **Suite green** — the plan's Definition of Done #1: the filtered 72-module python-oracledb
   suite **matches-or-beats the recorded baseline manifest** on the local 23ai container.
   Reached by **Wave 2 (in flight) + Wave 3**. This is the literal "passes python-oracledb's
   own tests" goal.
2. **Gauntlet-certified port** — the full vision (M6 + the differentiators): a usable Rust
   crate API, TLS/wallet (M3), structural debt cleared, fuzzed wire decoder, honest perf,
   certification. Reached by **Waves 4–6**. This is "a real driver someone can depend on,"
   not just "tests pass through a shim."

The suite running against a **non-TLS** local container means TLS/wallet is **not** required
for finish line 1 — but it is the headline differentiator and milestone M3, so it is in scope
for finish line 2.

## Status snapshot (archived June 2026; after Wave 1 + pipeline merge — verified)

- Large majority of the 72 in-scope modules at/near baseline. Pipelining 49/49.
- **At this snapshot only — untouched:** TLS/rustls (M3) — driver is plain TCP; wallet readers (ewallet.pem/cwallet.sso)
  — none; fuzzing/benches (M6) — none; DIVERGENCES/claim-contract/fake-parity artifacts — none.
- **Structural debt:** pyshim 12.5k LOC (still hosts SQL/bind/type driver logic; ~12 `d49:`
  markers); protocol `thin.rs` re-grown to 5.4k LOC (monolith again after intervals/vector/dpl).
- Native (non-shim) crate tests exist only for arrow/dpl/pipeline golden+live; no broad suite.

---

## Wave 3 — Suite green (loop-until-dry residual cleanup)  ·  bead rust-oracledb-w3  ·  BLOCKS all later waves

The gate for finish line 1. Driven by a **full 72-module sweep vs the baseline manifest**, run
the instant Wave 2 merges (`harness/run.sh` segmented over the in-scope filter → `compare_pytest_json.py`).

- Collect **every individual test** that differs from baseline; group by root cause; assign
  disjoint clusters to lanes (isolated worktree + venv + container per lane, as in Waves 1–2).
- Known-likely residual clusters (confirm against the sweep): TTC error offset/detail
  (4300/6300), 12.1 feature remnants (3200), occurrence-positional binds (1626), and whatever
  tail the Wave-2 vector/dataframe/cursor lanes could not fully close.
- **Loop until dry:** re-run the full sweep after each merge; a completeness-critic agent each
  round asks "which individual tests still differ from baseline, and why." Stop when only
  baseline-skips remain.
- Produce **`DIVERGENCES.md`**: every place we intentionally beat a reference bug (Q7 policy —
  e.g. the dataframe literal-NULL corruption, JSON UDS-flag mask), with the upstream issue or a
  minimal repro. Required for honest "match-or-beat" claims.
- **Gate:** `harness/run.sh diff` = 0 regressions, 0 missing, beats documented.

Skills: `systematic-debugging`, `multi-pass-bug-hunting`, `testing-real-service-e2e-no-mocks`.

## Wave 4 — Structural reality: make it a real port, not a shim  ·  bead rust-oracledb-w4

Once green, pay down the debt so the crate stands alone (advances `d49`).

- **d49 migration:** move SQL parsing, bind/type math, statement cache, and the executemany
  manager from `pyshim` into `oracledb` / `oracledb-protocol`. Behavior-preserving; the suite
  stays green at every step. Shim shrinks toward pure marshalling.
- **Split `protocol/thin.rs`** (5.4k LOC monolith) via `de-monolithize-your-codebase-isomorphically`
  into `thin/{auth,execute,fetch,lob,dbobject,types,intervals,vector,dpl,...}.rs`. Same isomorphism
  gate as Wave 0 (line-multiset equality + sentinel suite modules unchanged).
- **Crate-as-library:** harden the `oracledb` public async API + blocking facade into a clean,
  documented, Rust-consumer-usable surface; add **crate-level integration tests against the
  container (NOT via the shim)** proving the driver works standalone. This is what makes it a
  port rather than a test harness.

Skills: `de-monolithize-your-codebase-isomorphically`, `simplify-and-refactor-code-isomorphically`,
`asupersync-mega-skill`.

## Wave 5 — TLS / wallet (M3, the differentiator)  ·  bead rust-oracledb-w5  ·  parallel-OK with Wave 4

Mostly new transport code, so it can run alongside Wave 4 (coordinate on `transport`/driver
`lib.rs`).

- **rustls on the asupersync transport** (TCPS): sans-io `ClientConnection` driven over the
  async socket; SNI string format `S{len}.{service}.V3.{ver}`; `ssl_server_cert_dn` matching
  (reference disables hostname check and matches DN itself).
- **Wallet readers:** `ewallet.pem`; **`cwallet.sso` (value-add, D8)** — its own Rust tests
  (the reference's thin mode reads only PEM, so the suite can't cover SSO; experimental flag if
  the SSO obfuscation format proves risky).
- **Stand up a TLS listener** (self-signed) against a container variant to exercise TCPS
  end-to-end + unskip any TLS-gated suite tests; add differential tests (the main suite container
  is non-TLS, so TLS is otherwise uncovered).

Skills: `asupersync-mega-skill`, `research-software` (rustls sans-io patterns).

## Wave 6 — Gauntlet (M6 certification)  ·  bead rust-oracledb-w6  ·  blocked by 3,4,5

- **Fuzz** the wire decoder with `cargo-fuzz` (fail-closed proof; the protocol crate is
  `#![forbid(unsafe_code)]` so this is pure-Rust safe).
- **Final fake-parity sweep** (`mock-code-finder` + `scripts/fake_parity_scan.py`) →
  `fake-parity-scan.md` clean; retire any remaining shim emulations flagged in `.intake/lob.md`.
- **Perf:** `criterion` benches vs **python-oracledb thin AND rust-oracle thick** —
  connect / single-row / bulk fetch / LOB / executemany / DPL — with honest published methodology.
- **Certify** with `/running-the-gauntlet-on-your-rust-port`; emit the release scorecard vs the
  rust-oracle gap matrix (identity, proxy auth, cwallet.sso, tnsnames, DN match, objects,
  LONG/XMLType/BFILE, Arrow/DPL).
- **Docs:** `de-slopify` + `readme-writing` final pass; `claim-contract.md`; NOTICE/LICENSE check.

Skills: `testing-fuzzing`, `profiling-software-performance`, `extreme-software-optimization`,
`running-the-gauntlet-on-your-rust-port`, `testing-golden-artifacts`, `de-slopify`, `readme-writing`.

## Post-goal (operator-gated, NOT part of the autonomous goal)

- Publish a refreshed **filtered** mirror to `MuhDur/rust-oracledb` (re-run the `filter-repo`
  export excluding plan.md/PLAN_TO_PORT/AGENTS.md — NEVER push local master directly) and
  prepare crates.io (`rust-crates-publishing`).
- The production-snapshot acceptance (operator-run, per plan §"After the goal is reached").
- Possible v0.2 expansion: **AQ** (the largest excluded chunk), then SODA/CQN.

## Coordination invariants (carry forward every wave)

- One builder per checkout; lane = isolated worktree + own venv (`ORACLEDB_VENV_DIR`) + own
  container; `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-<lane>` and `TMPDIR=$HOME/.cache/tmp`
  (NOT /tmp — it filled twice). Main checkout + container **1522** = global verification only.
- After any host/API outage: **restart ALL containers including 1522** and verify each with
  `select count(*) from TestNumbers` before trusting a red run (a down container looks like a
  code regression — mass `Connection refused` setup-errors).
- Commit every working change (<5 min uncommitted) — interrupts (spend limit, API outage) recur.
- Serial verified merges into master; resolve conflicts as **union of behaviors**, never drop a
  capability; re-verify sentinels (1100 57p/5s, 3600 79p, 2400 51p/9s, 4100 27p-must-complete)
  after each merge.
- Never delete files without explicit user approval (a lane deleted a build cache once — propagate
  the no-deletion rule into every agent prompt).
- Never push local master to the public mirror; it is a filtered export.
