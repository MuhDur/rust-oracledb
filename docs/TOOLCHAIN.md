# Rust Toolchain Runbook

`rust-oracledb` is pinned to `nightly-2026-05-11`. Required CI, release
qualification, and local builds use that pinned nightly; the canary lane uses
floating `nightly` so breakage is discovered before the pin must move.

## Why Nightly

The driver depends on `asupersync` for its async runtime. `asupersync` currently
uses `#![feature(try_trait_v2)]` and `#![feature(try_trait_v2_residual)]`, so the
crate graph does not compile on stable Rust. A stable build fails with `E0554`
before it reaches `oracledb` code.

This nightly requirement is build-time only for consumers that ship a static
binary. The built driver has no OCI or Instant Client dependency, and the
nightly compiler is not part of the runtime artifact.

The decision is recorded in [ADR-0001](adr/0001-nightly-asupersync.md).

## Pin Policy

Move the pin only when there is a concrete reason:

- the canary lane finds a floating-nightly break that affects this workspace;
- an `asupersync` upgrade requires a newer nightly;
- a toolchain or dependency security fix requires a newer nightly.

Do not move the pin just because a newer nightly exists. The replacement pin must
pass the same quality commands as the current pin before it becomes the repo
default.

Current pin sites:

- `rust-toolchain.toml`
- `.github/workflows/_quality.yml` for every non-canary profile
- `.github/workflows/ci.yml`
- `.github/workflows/release.yml`
- `docs/TOOLCHAIN.md`
- `.claude/skills/oracledb/SKILL.md`

## Canary Response

When `canary.yml` goes red, first decide whether it is a toolchain break:

1. Compare it with the required lane on the same `main` commit.
2. If required CI is also red, treat it as a normal repository regression.
3. If only canary is red, inspect the failing command and the reported `rustc`
   version.
4. Continue only when the failure is caused by floating nightly behavior, such as
   a `try_trait_v2` change, rustdoc-json drift, rustfmt/clippy drift, or a
   compiler regression.

Scheduled canary and soak runs are discovery lanes. They fail normally so their
status is truthful, but they do not replace required CI or manual release
qualification.

## Re-pin Procedure

Use this procedure from a clean checkout on `main`.

1. Pick the candidate nightly from the canary log:

   ```bash
   export NEW_NIGHTLY=nightly-YYYY-MM-DD
   rustup toolchain install "$NEW_NIGHTLY" --component rustfmt --component clippy
   ```

2. Edit every pin site:

   - `rust-toolchain.toml`
   - `.github/workflows/_quality.yml`
   - `.github/workflows/ci.yml`
   - `.github/workflows/release.yml`
   - this file
   - `.claude/skills/oracledb/SKILL.md`

3. Run the toolchain-specific gates with the candidate:

   ```bash
   cargo +"$NEW_NIGHTLY" fmt --all -- --check
   cargo +"$NEW_NIGHTLY" clippy --workspace --no-deps -- -D warnings
   cargo +"$NEW_NIGHTLY" test --workspace
   ```

4. Run the repository gates that should stay green after the pin move:

   ```bash
   bash scripts/release_preflight.sh
   cargo deny check
   scripts/gen_baseline.sh --check
   git diff --check
   br dep cycles
   ```

5. If `scripts/gen_baseline.sh --check` reports only expected toolchain or
   public-API snapshot drift, regenerate and inspect the baseline:

   ```bash
   scripts/gen_baseline.sh
   scripts/gen_baseline.sh --check
   ```

   Do not pass `--refresh-pin` unless the reviewed source baseline commit is
   intentionally moving. A toolchain-only re-pin should keep
   `docs/baseline/source_commit.txt` anchored to the same source commit.

6. Commit the pin change, the baseline drift if any, and the Beads update
   together. Push it to `main`, then rerun `canary.yml` and confirm the required
   lane is still green.

## Do Not

- Do not make canary failures pass with `continue-on-error`.
- Do not make branch protection depend on discovery lanes.
- Do not switch this workspace to stable Rust while `asupersync` needs nightly
  features.
- Do not change only one pin site. Local builds, legacy CI, reusable CI, and
  release jobs must move together.
