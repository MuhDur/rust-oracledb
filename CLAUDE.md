# CLAUDE.md

Project conventions, agent rules, and the development workflow for `rust-oracledb`
live in **[AGENTS.md](AGENTS.md)** — read it first.

Quick orientation:

- **What this is:** a pure-Rust async thin-mode Oracle Database driver, a
  clean-room port of python-oracledb's thin mode. No OCI / Instant Client.
- **Crates:** `oracledb-protocol` (sans-io wire protocol, `#![forbid(unsafe_code)]`),
  `oracledb` (the async driver), `oracledb-derive` (`#[derive(FromRow)]`),
  `oracledb-pyshim` (a PyO3 harness used only to drive python-oracledb's own test
  suite against the Rust engine — not published).
- **Conformance:** the bar is python-oracledb's own thin-mode test suite. Run it
  via `harness/run.sh baseline | rust | diff`.
- **Gates before any commit:** `cargo fmt --check`, `cargo clippy --workspace
  --no-deps -- -D warnings`, `cargo test --workspace`.
- **Issue tracking:** Beads (`br`); `.beads/` is committed with code.

See AGENTS.md for the binding rules (especially around destructive git/filesystem
operations and issue tracking).
