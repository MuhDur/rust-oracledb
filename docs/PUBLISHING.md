# Publishing to crates.io

This document is the runbook for cutting a release of the
`oraclemcp-driver-cx` family to [crates.io](https://crates.io). It is the source
of truth for publish order, version, package contents, and post-publish checks.
Publishing remains operator-gated; this runbook is not authorization to upload.

## Release version

| field | value |
|---|---|
| Original legacy public release | **0.1.0** |
| Latest legacy `oracledb` release | **0.9.1** |
| First `oraclemcp-driver-cx` release | **0.9.2** (candidate; not yet published) |
| Workspace release version | **0.9.2** |
| Candidate source | `[workspace.package].version` in the root `Cargo.toml` |

`0.9.2` deliberately preserves the project's version lineage for the first
renamed-family publication instead of restarting at `0.1.0`. The `0.x` major
continues to signal that the public API may evolve before `1.0`.

All workspace crates share the workspace release version via `version.workspace = true`.

## Mandatory pre-tag live-matrix check

Before creating any release tag, the candidate must be the current `main` SHA
and must already have every Required check-run, including the four live Oracle
version-matrix lanes. A tag push does not trigger those lanes: the matrix is
intentionally limited to path-filtered pushes to `main`, so a CHANGELOG-only or
version-hygiene candidate otherwise reaches the release workflow with no
exact-SHA live evidence.

Run this from a clean checkout at the intended `main` candidate, replacing the
example tag with the workspace version:

```bash
candidate_sha="$(git rev-parse HEAD)"
RELEASE_TAG=vX.Y.Z bash scripts/release_preflight.sh --pre-tag
```

If it reports `E_PRETAG_LIVE_MATRIX_MISSING`, dispatch the existing live matrix
on `main`, wait for all four lanes, and rerun the pre-tag check. The final
taxonomy command must report `ci_green: true` for the same SHA before the tag is
created:

```bash
gh workflow run version-matrix.yml --ref main
run_id="$(gh run list --workflow version-matrix.yml --commit "$candidate_sha" --limit 1 --json databaseId --jq '.[0].databaseId')"
test -n "$run_id"
gh run watch "$run_id" --exit-status
python3 scripts/ci_taxonomy.py --status "$candidate_sha"
RELEASE_TAG=vX.Y.Z bash scripts/release_preflight.sh --pre-tag
```

Only after the last command passes may the operator create and push the exact
candidate tag:

```bash
git tag vX.Y.Z "$candidate_sha"
git push origin vX.Y.Z
```

This is an early, fail-closed process gate; it does not weaken the tag workflow's
own exact-SHA evidence validation, which remains the final publish backstop.

## Crates and the publish dependency graph

```
asupersync (already on crates.io, 0.3.9)
        ^
        |  (external dep)
        |
oraclemcp-driver-cx-protocol  ──┐
        ^            │
        │            ▼
oraclemcp-driver-cx-derive ──> oraclemcp-driver-cx
```

Resolved publish order (dependencies first, so each crate's registry deps already
exist when it is uploaded):

1. `oraclemcp-driver-cx-protocol` — sans-I/O TNS/TTC wire protocol core
2. `oraclemcp-driver-cx-derive` — `#[derive(FromRow)]` proc-macro
3. `oraclemcp-driver-cx` — the async thin-mode driver

The driver depends on the protocol package always and the derive package under
the default `derive` feature. Both use a local `path` and a registry version pin
matching the workspace release. The dependency keys remain short internal
aliases; `package` carries the public crates.io identity:

```toml
oracledb-protocol = { package = "oraclemcp-driver-cx-protocol", path = "../oracledb-protocol", version = "0.9.2" }
oracledb-derive = { package = "oraclemcp-driver-cx-derive", path = "../oracledb-derive", version = "0.9.2", optional = true }
```

`asupersync = "=0.3.9"` is the only non-trivial external runtime dependency and
is confirmed live on crates.io. The exact pin is deliberate because the runtime
defines the driver's cancellation and timer semantics.

## NOT published

| crate | reason | guard |
|---|---|---|
| `oracledb-pyshim` | PyO3 test harness for python-oracledb conformance; not a library users consume | `publish = false` in its `Cargo.toml` |
| `oracledb-protocol-fuzz` | cargo-fuzz libFuzzer harness; standalone (empty `[workspace]`) and not a workspace member | `publish = false` in its `Cargo.toml` |

## Publish commands (exact, in order)

Run from the repository root. Use `--locked` so the committed `Cargo.lock` is
honored, and `--all-features` so the full API surface is verified.

```bash
# 0. Authenticate once (crates.io API token with publish scope).
#    Prefer the env var so the token never lands in shell history / files:
export CARGO_REGISTRY_TOKEN=<crates.io-publish-token>

# 1. Dry-run the independently resolvable leaves.
cargo publish --dry-run -p oraclemcp-driver-cx-protocol --locked --all-features
cargo publish --dry-run -p oraclemcp-driver-cx-derive --locked --all-features
cargo package --list -p oraclemcp-driver-cx

# 2. Publish the leaves and wait for each to become index-visible.
cargo publish -p oraclemcp-driver-cx-protocol --locked --all-features
cargo publish -p oraclemcp-driver-cx-derive --locked --all-features

# 3. Only now can the main registry package resolve. Dry-run it before upload.
cargo publish --dry-run -p oraclemcp-driver-cx --locked --all-features
cargo publish -p oraclemcp-driver-cx --locked --all-features
```

crates.io usually makes a new version resolvable within a minute or two. If the
`oraclemcp-driver-cx` publish reports a missing protocol or derive package,
the index simply has not caught up yet — wait and retry. `cargo publish` recent
versions block until the just-uploaded crate is available, so a manual wait is
rarely needed.

## What is excluded from each package, and why

Each published crate ships only what is needed to build and use it. Heavy
dev-time assets are excluded to keep the tarball lean.

| crate | `exclude` | rationale |
|---|---|---|
| `oraclemcp-driver-cx` | `tests/` | integration + live tests and the `tests/fixtures` cassette/TLS corpus (~300 KB) are dev-only. `benches/` and `examples/` are kept so the `[[bench]]` targets and doc examples resolve. |
| `oraclemcp-driver-cx-protocol` | `tests/`, `fuzz/`, `proptest-regressions/` | the `tests/golden/` wire-trace corpus is ~744 KB; the `fuzz/` sub-crate and proptest regression seeds are dev-only. Inline `src/.../proptests.rs` stay (they are source). |
| `oraclemcp-driver-cx-derive` | (none needed) | already only `src/lib.rs`. |

Targeted rename qualification on 2026-07-30 recorded these package file counts:

| crate | `cargo package --list` files |
|---|---:|
| `oraclemcp-driver-cx` | 49 |
| `oraclemcp-driver-cx-protocol` | 46 |
| `oraclemcp-driver-cx-derive` | 5 |

These counts qualify the current dirty-tree candidate only. The publish task
must record them again from the committed release candidate.

## README and license

- `oraclemcp-driver-cx` ships `crates/oracledb/README.md` (cargo cannot package
  a README outside the crate dir, so the rich repo-root `README.md` cannot be
  referenced directly; the crate README links to the repo for the full docs).
- `oraclemcp-driver-cx-protocol` and `oraclemcp-driver-cx-derive` set
  `readme = false`; user-facing docs live on `oraclemcp-driver-cx`.
- Licensing is declared via the SPDX `license = "MIT OR Apache-2.0"` field
  (inherited from `[workspace.package]`). crates.io accepts the SPDX expression;
  no per-crate `LICENSE-*` file copy is required. The canonical `LICENSE-MIT`,
  `LICENSE-APACHE`, and `NOTICE` live at the repo root.

## Metadata completeness (per published crate)

All three inherit `version`, `edition`, `license`, `repository`,
and `homepage` from `[workspace.package]`, plus a `documentation = https://docs.rs/<crate>`
default. Per-crate specifics:

| field | oraclemcp-driver-cx | oraclemcp-driver-cx-protocol | oraclemcp-driver-cx-derive |
|---|---|---|---|
| `description` | workspace default (driver) | "Sans-I/O Oracle TNS/TTC protocol core…" | "Procedural macros for oraclemcp-driver-cx…" |
| `keywords` | oracle, database, driver, async, tns | oracle, database, protocol, tns, ttc | oracle, database, derive, proc-macro, fromrow |
| `categories` | database, asynchronous | database, network-programming | database |
| `readme` | `README.md` | `false` | `false` |

## Renamed-family pre-publish qualification

Targeted checks on 2026-07-30 established the dependency-order boundary without
uploading any crate:

- `cargo publish --dry-run -p oraclemcp-driver-cx-protocol --allow-dirty` ->
  **PASS** (upload aborted by dry-run).
- `cargo publish --dry-run -p oraclemcp-driver-cx-derive --allow-dirty` ->
  **PASS** (upload aborted by dry-run).
- `cargo package -p oraclemcp-driver-cx --allow-dirty --no-verify` ->
  **expected registry-order failure**: `no matching package named
  oraclemcp-driver-cx-derive found` on crates.io.

The main package failure is not a source-build failure. Cargo resolves its
versioned registry dependencies while preparing the package, so the protocol
and derive crates must be published and visible in the index before the main
crate can receive a green package/dry-run result. Task
`rust-oracledb-cx-driver-handover-wgwq.4` therefore publishes the two leaves,
waits for index visibility, re-runs the main package qualification, and only
then publishes `oraclemcp-driver-cx`. The tag workflow enforces this order in
`scripts/publish_crates.sh`; a failed main dry-run stops before the main upload.

## Pre-transition dry-run results (historical only)

The results below qualify the old package names, not the renamed family. They
must not be used as 0.9.2 release evidence:

Run with `CARGO_TARGET_DIR` / `TMPDIR` pointed at a scratch cache:

- `cargo publish --dry-run -p oracledb-protocol` → **PASS** (packages + verify-builds
  clean; 38 files).
- `cargo publish --dry-run -p oracledb-derive` → **PASS** (5 files, builds clean).
- `cargo publish --dry-run -p oracledb` → **fails at dependency resolution**:
  `no matching package named oracledb-derive found / location searched: crates.io
  index`. This is **expected**: `oracledb` depends on `oracledb-protocol` and
  `oracledb-derive` by `version`, and those are not on crates.io until steps 1–2
  of the real publish run. The check fires during packaging (even with
  `--no-verify`), so the only way to fully green this dry-run is to publish the
  two dependency crates first. `cargo package --list -p oracledb` confirms the
  package contents and metadata are otherwise correct.

## Post-publish checklist

After each `cargo publish`, and once all three are live:

- [ ] Each crate page loads: `https://crates.io/crates/oraclemcp-driver-cx`,
      `.../oraclemcp-driver-cx-protocol`, `.../oraclemcp-driver-cx-derive`.
- [ ] docs.rs build succeeds: `https://docs.rs/oraclemcp-driver-cx` (check the build log;
      enable any required features there if the default docs are thin).
- [ ] `cargo add oraclemcp-driver-cx@0.9.2` in a fresh project resolves `0.9.2`
      and compiles `use oraclemcp_driver_cx::ConnectOptions;`.
- [ ] Tag the release in git: `git tag v0.9.2 "$candidate_sha" && git push origin v0.9.2`.
- [ ] Verify the published `oraclemcp-driver-cx` README renders correctly on crates.io
      (links point at the GitHub repo, not broken relative `docs/` paths).
- [ ] Confirm `oracledb-pyshim` and `oracledb-protocol-fuzz` did NOT get
      published (they should not appear on crates.io).

## Re-publish notes

- A version, once uploaded, is immutable. To ship a fix, bump
  `[workspace.package].version` (e.g. `0.1.1`) and re-run the order above.
- Never `cargo yank` casually; yank only removes a version from new resolution,
  it does not delete it.
