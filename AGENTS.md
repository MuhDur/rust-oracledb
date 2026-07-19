# AGENTS.md — rust-oracledb

Operating rules for agents working in this repository.

**rust-oracledb** is a pure-Rust, async, **thin-mode** Oracle Database driver: it
speaks the Oracle TNS/TTC wire protocol directly over TCP, so it needs **no
Oracle Instant Client, no OCI, no ODPI-C, no `libclntsh`, and no C toolchain** to
talk to a database. It is a faithful clean-room port of python-oracledb v4.0.1
thin mode and tracks that reference's behaviour. The crate ships on crates.io as
[`oracledb`](https://crates.io/crates/oracledb). Independent open-source project;
**not affiliated with Oracle** — "Oracle" and "python-oracledb" are named only to
describe what this driver is compatible with.

## RULE 1 — ABSOLUTE

Do not delete any file or directory unless the operator gives the exact command
in-session. This includes files you just created (tests, scratch scripts, tmp
files). You do not get to decide something is "safe" to remove. If something
should go, stop and ask first.

## Irreversible / outward-facing actions

Never run `git reset --hard`, `git clean -fd`, `git push --force`, branch
deletion, or `rm -rf` on tracked paths without explicit in-session approval.
Never force-push `main`. **Do not commit or push on the operator's behalf without
a clear in-session go-ahead.** crates.io publishes are **permanent** — a version
is immutable once uploaded and the `oracledb` name is claimed forever — so treat
publishing as a gated, deliberate, operator-authorized step, never an incidental one.

## Rust toolchain & gates

- Cargo workspace, `resolver = "2"`, workspace version **0.8.4**,
  `edition = "2021"`.
- **NIGHTLY Rust is required** — pinned to **`nightly-2026-05-11`** in
  `rust-toolchain.toml` (components `rustfmt`, `clippy`). There is **no stable
  MSRV**: asupersync's default `nightly-outcome-try` feature (pinned `=0.3.9`)
  enables `#![feature(try_trait_v2)]` / `try_trait_v2_residual`, so a stable
  toolchain fails with `E0554` before any of this crate's code is reached. Bump
  the pin in lockstep with asupersync upgrades (and the matching
  `dtolnay/rust-toolchain` pins under `.github/workflows/`).
- The whole workspace forbids `unsafe`: `unsafe_code = "forbid"` in
  `[workspace.lints.rust]`, and `oracledb-protocol` / `oracledb` are each
  `#![forbid(unsafe_code)]`. The **only** `unsafe` is one audited Arrow C-Data
  FFI module (`crates/oracledb-pyshim/src/arrow_capsule.rs`), quarantined to the
  **non-published** PyO3 test harness. Do not introduce `unsafe` anywhere else.
- **Before committing code**, run the same gates CI enforces
  (`.github/workflows/ci.yml`), using the pinned toolchain:
  ```bash
  cargo fmt --all -- --check
  cargo clippy --workspace --exclude oracledb-pyshim --no-deps -- -D warnings
  cargo test --workspace --exclude oracledb-pyshim
  cargo test -p oracledb --features cassette
  cargo deny check
  ```
  `oracledb-pyshim` is excluded from the local gates because it needs a Python
  toolchain and a live database; it is exercised by the conformance harness, not
  plain `cargo test`.

### Build-resource discipline

- **Do not touch `/tmp/cargo-target`.** It is a managed target directory backed
  by the root disk after the 2026-07-16 tmpfs exhaustion incident; never delete,
  recreate, replace, or redirect it.
- Iterate with scoped commands only: `cargo check -p <crate>` and
  `cargo test -p <crate> [testname]`. Do not compile the whole workspace merely
  to validate a single crate.
- `~/.cargo/config.toml` caps Cargo at four jobs. Never override that cap with
  `-j` or `--jobs`.
- Before every workspace-wide compile (`cargo build`, `cargo test`, or
  `cargo clippy --workspace`) and before a full commit gate, acquire an MCP
  Agent Mail build slot for this repository. At most two slots may be active;
  wait and retry when unavailable, and release the slot immediately when the
  build finishes.

## Thin-mode invariants (do not weaken)

These are the reasons this project exists — never regress them:

- **Thin mode only.** No thick mode, no OCI, no ODPI-C, no Instant Client. A
  thick path would re-introduce the native dependency the project exists to
  avoid. The driver links **no native Oracle library**.
- **Nightly + asupersync stay.** asupersync is the single load-bearing
  async-runtime dependency, pinned exactly (`=0.3.9`) because its `tls` feature
  fixes the rustls/ring graph; its default `nightly-outcome-try` feature keeps
  the release graph on nightly. Bump it deliberately, never via a caret floor.
- **Fail-closed decode.** Every untrusted input path (wire decoder, TLS/wallet
  readers, connect-string parser) returns a structured error on hostile or
  malformed input — never a panic, OOM, or stack overflow. The
  OOM-from-wire-length class is closed by construction via the protocol crate's
  `BoundedReader` invariant (an allocation can never exceed the bytes remaining
  in the message buffer). Do not add a decode path that allocates on an
  attacker-controlled length without that bound.

## The quality bar that defines this repo

This driver's credibility rests on evidence, not claims. Preserve it:

- **Reference parity.** rust-oracledb passes python-oracledb's **own** thin-mode
  pytest suite driven through the Rust engine: **2462 of 2578** reference tests
  pass, with **116** skips (every skip proven legitimate — thick-mode-only,
  external/OS auth, an inverted older-client vector check, or upstream
  hardcoded `@pytest.mark.skip`) and **0** regressions vs the recorded baseline.
  See `docs/PARITY_SKIPS.md`, `docs/RELEASE_CERTIFICATION.md`,
  `docs/FAKE_PARITY_AUDIT.md`. A change that drops parity or turns a proven skip
  into a hidden defect is a release blocker.
- **Fuzzing / robustness.** 20 cargo-fuzz targets cover the untrusted decode
  boundaries plus the connect-string parser, with a differential oracle that
  cross-checks the decoder against python-oracledb's on identical wire bytes.
  See `docs/FUZZING.md`.
- **Multi-version live matrix (standing release gate).** `scripts/version_matrix.sh`
  runs one gvenzl-backed container per server generation (11g / 18c / 21c / 23ai;
  `.github/workflows/version-matrix.yml`). The 11g lane sits **below** the
  protocol floor (`TNS_VERSION_MIN_ACCEPTED = 315`; 11g negotiates 314) and its
  assertion is **inverted** — it passes only when the driver cleanly *refuses*
  with the structured `UnsupportedVersion` error. A release cannot ship without a
  green full-matrix artifact recorded for the release SHA
  (`scripts/release_matrix_gate.sh` → `tests/artifacts/version_matrix/`).

## Workspace layout

Three published crates plus one test-only harness:

```text
crates/oracledb-protocol   sans-I/O TNS/TTC codec. #![forbid(unsafe_code)].
                           Decodes everything an untrusted server puts on the
                           wire; every length-driven alloc is BoundedReader-checked.
crates/oracledb            the async driver on the asupersync runtime, plus the
                           BlockingConnection synchronous facade. Connection /
                           execute / fetch / LOB / pool / TLS / SODA.
crates/oracledb-derive     build-time proc-macro crate behind #[derive(FromRow)].
crates/oracledb-pyshim     PyO3 harness (publish = false) that slots under
                           python-oracledb so the reference's OWN pytest suite
                           drives the Rust engine. Holds the one audited unsafe.
```

**K10 row-by-row streaming contract.** `crates/oracledb/src/row_stream.rs`
defines `OwnedRowStream` — an owning `futures_core::Stream` of owned query rows.
It is `#[must_use]` and **holds the connection** until it is fully drained or the
connection is explicitly reclaimed with `OwnedRowStream::into_connection()`.
Preserve that ownership/recovery contract: a dropped or timed-out stream must
leave the connection reusable (break-and-drain), never poisoned.

## Code editing discipline

- Optimize for a clean architecture now, not backwards compatibility. No compat
  shims or `v2` file clones; migrate callers and remove old code.
- The bar for adding files is high; new files only for genuinely new domains.
- No bulk codemods or giant `sed`/regex refactors. Break large mechanical
  changes into small, reviewable edits; edit subtle changes by hand.
- Structured, minimal logs — no spammy debug output. The packet-level connect
  trace is a deliberate hard switch (`ORACLEDB_TRACE_CONNECT=1`), independent of
  `RUST_LOG`; keep it that way and keep secrets out of it.

## Release flow

The `vX.Y.Z` git tag is the source of truth (`.github/workflows/release.yml`):

- The tag must match the workspace version and be contained in `origin/main`
  (`scripts/release_preflight.sh`), and the release SHA must carry a green
  version-matrix artifact.
- On a non-pre-release tag the workspace publishes to crates.io in dependency
  order (`scripts/publish_crates.sh`, idempotent across retries), then the GitHub
  release is cut with the static binary attached. Pre-release tags (containing
  `-`) skip the crates.io publish. `workflow_dispatch` validates gates + build
  **without** publishing.
- `oracledb-pyshim` is `publish = false` and never goes to crates.io.

## Issue tracking — br (Beads), repo-local

This repo has its **own** local Beads database under `.beads/`
(`issue_prefix: rust-oracledb`, ids like `rust-oracledb-004o`). Work beads from
this repo root with `br`. **Never** use the sibling `oraclemcp` /
`plsql-intelligence` trackers for this repo's work.

```bash
br ready --json                      # unblocked work
br update <id> --status in_progress  # claim
br close  <id> --reason "…"          # finish; commit .beads/ with the code
br create "Title" -t bug|feature|task -p 0-4 --deps discovered-from:<id>
br sync --flush-only                 # export .beads/issues.jsonl before commit
```

Types: `bug`, `feature`, `task`, `epic`, `chore`. Priorities: `0` critical …
`4` backlog (default `2`). `.beads/` is authoritative state and must be committed
with the code (or planning) change it describes. Do not hand-edit `.beads/*.jsonl`
or keep markdown TODO lists or a second tracker.

## bv — graph-aware triage sidecar

`bv` computes PageRank / critical paths / parallel tracks over the beads graph.
**Use only `--robot-*` flags; bare `bv` opens a blocking TUI.**

```bash
bv --robot-triage   # start here   ·   bv --robot-next   # top pick + claim cmd
bv --robot-plan     # parallel tracks   ·   bv --robot-insights   # graph metrics
```

## cass / cass-memory — reuse prior work

`cass` indexes past agent sessions; `cm` surfaces procedural memory. Never run
bare `cass` (TUI); use `--robot`/`--json`.

```bash
cass search "<problem>" --robot --limit 5    # has this been solved before?
cm context "<task>" --json                   # relevant rules, anti-patterns, history
```

## MCP Agent Mail — multi-agent coordination

For concurrent agents: identities, inboxes, searchable threads, and advisory
file reservations (leases) so agents don't clobber each other.

- Register: `ensure_project` then `register_agent` with this repo's absolute path
  as `project_key`.
- Reserve before editing:
  `file_reservation_paths(project_key, agent, ["crates/**"], ttl_seconds=3600, exclusive=true)`.
- Communicate: `send_message(..., thread_id=…)`, then `fetch_inbox` /
  `acknowledge_message`. Pitfalls: `from_agent not registered` → re-`register_agent`
  with the right `project_key`; `FILE_RESERVATION_CONFLICT` → adjust patterns or
  wait for expiry.

## Landing the plane (session completion)

Work is not complete until it is pushed. When ending a session:

1. File repo-local beads in this checkout for any remaining work.
2. Run the quality gates above if code changed.
3. Update bead status; close finished work.
4. Push (only with the operator's go-ahead):
   ```bash
   git pull --rebase
   br sync --flush-only
   git add .beads/
   git commit -m "…"
   git push
   git status            # MUST show "up to date with origin"
   ```
5. Leave a short handoff for the next session.

Do not stop before pushing; that strands work locally. If a push fails, resolve
and retry. Never commit or push without the operator's in-session go-ahead.
