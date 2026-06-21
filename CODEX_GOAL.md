# Codex Goal — drive the Road-to-1.0 beads to ZERO

## Mission (simple)
Drive the **open bead count to 0** for the Road-to-1.0 program, then verify, then keep
finding and doing more useful work. The program is a beads graph rooted at
**`rust-oracledb-road-to-1-0-llv`** — about **113 beads** (6 epics + 30 tasks + 77
subtasks). Implement each bead, test it, close it. **Keep going until every one is closed.**

This repo is a pure-Rust async **thin-mode Oracle Database driver** (clean-room port of
python-oracledb thin mode, built on the **asupersync** runtime). Read **AGENTS.md** first
(binding rules), and use **docs/ROAD_TO_1_0.md** + **docs/API_DESIGN.md** for context — but
the **beads are authoritative**; a bead's own description should be enough to execute it.

## The loop — repeat until no open beads remain
1. `br ready --json` (or `bv --robot-next`) → pick the top actionable bead under
   `rust-oracledb-road-to-1-0-llv`.
2. Claim it: `br update <id> --status in_progress`.
3. Implement it per its description (WHY / SCOPE / ACCEPTANCE / TESTS / DEPS). Write the
   **unit + e2e tests** the bead calls for, **with detailed structured logging** so a run
   visibly proves it works.
4. **Gates before closing/committing:** `cargo fmt --check`,
   `cargo clippy --workspace --no-deps -- -D warnings`, `cargo test --workspace`. For
   conformance-affecting work keep `harness/run.sh diff` green.
5. Close it honestly: `br close <id> --reason "..."`. Only close what is **actually
   implemented + tested + gates-green**. NO false completions.
6. Commit `.beads/` **together with** the code change (AGENTS.md): `br sync --flush-only`
   then `git add .beads/ <files> && git commit`. **Commit locally; do NOT `git push`**
   unless explicitly told.
7. Discovered new work? File a bead (`br create … --deps discovered-from:<parent>`); never
   silently expand scope. Only ever touch beads via `br`.
- Use **/repeatedly-apply-skill** to keep this loop running without stalling.

## Do not run out of disk
Long autonomous runs fill `/tmp` and `target/`. **Clear scratch before you run low** so a
full disk never stops the loop:
- `rm -f /tmp/*.txt /tmp/codex_*.txt` and any scratch you created.
- `cargo clean -p <crate>` or prune `target/` when it balloons; drop stale fuzz/corpus
  artifacts you generated.
- Check free space periodically (`df -h /tmp .`); clean proactively, not after failure.

## When all beads are closed
Run **/beads-compliance-and-completion-verification** to confirm every bead is genuinely
done (implemented, tested, no stubs, acceptance met). Reopen and finish anything it flags.

## After convergence — find MORE useful work
Once all open/stalled beads are done **and** your review rounds are **saturating** (few new
bugs found relative to the effort + tokens spent), switch to discovery. Apply the skills
below **only to the extent applicable to a thin DB driver**, turn findings into **new beads
via `br`** (see **/beads-workflow**), and execute them through the same loop.

**Discovery skills (find new work):**
- **/mock-code-finder** — stubs / mocks / placeholders left in real code paths.
- **/deadlock-finder-and-fixer** — concurrency hazards.
- **/reality-check-for-project** — does it actually do what it claims?
- **/modes-of-reasoning-project-analysis** — structured project critique.
- **/profiling-software-performance** — hot paths and regressions.
- **/security-audit-for-saas** — security issues (only where applicable to a driver).

**Improvement / hardening skills (apply where they help):**
- **/de-monolithize-your-codebase-isomorphically** — split monoliths, behavior unchanged.
- **/rust-undefined-behavior-exorcist** (and related `/rust-*` safety skills) — UB / unsafe
  review. Note: the protocol crate is `#![forbid(unsafe_code)]`; the only sanctioned unsafe
  is the pyshim Arrow C-Data FFI.
- **/asupersync-mega-skill** — correct asupersync usage (cancel-correctness, DPOR, budgets,
  checkpoints) — this driver runs on asupersync; many beads depend on getting this right.
- **/simplify-and-refactor-code-isomorphically** — clarity without behavior change.
- **/profiling-software-performance** + **/extreme-software-optimization** — make hot paths
  fast (codecs, fetch, binds, pool, LOB).
- **testing skills** — strengthen unit / e2e / property / fuzz coverage with detailed logging.
- **/repeatedly-apply-skill** — drive any of the above to completion.

For each finding worth doing: create a self-contained bead (description + deps), then
implement + test + close it like any other.

## Quality bar (non-negotiable)
**Implemented + tested (live where applicable) + clippy/fmt clean + committed + bead closed.**
Honesty over false completion: if something fails, say so and fix it; never mark a bead done
that isn't. Respect AGENTS.md — especially around destructive git/filesystem operations.
