# Publishing to crates.io

This document is the runbook for cutting a release of the `oracledb` driver and
its workspace crates to [crates.io](https://crates.io). It is the source of truth
for the publish order, the version, what ships, and the post-publish checklist.

## Release version

| field | value |
|---|---|
| Initial public release | **0.1.0** |
| Latest published release | [**0.8.4**](https://github.com/MuhDur/rust-oracledb/releases/tag/v0.8.4), 2026-07-17 |
| Next patch target | **0.8.5** |
| Current workspace version | **0.8.4**, matching the published tag |
| Target source | `[workspace.package].version` in the root `Cargo.toml`; bump it to exactly **0.8.5** during release preparation |

`0.1.0` (not `0.0.0`, which crates.io treats as a placeholder, and not a
`-alpha` pre-release) is the deliberate first public cut: the driver passes the
reference python-oracledb thin-mode test suite and is honestly usable, while the
`0.x` major signals that the public API may still evolve before `1.0`.

All workspace crates share the workspace version via `version.workspace = true`.
The 0.8.4 release was exact-SHA qualified and published by the tag pipeline:

- [release tag and assets](https://github.com/MuhDur/rust-oracledb/releases/tag/v0.8.4)
- [exact-SHA qualification run](https://github.com/MuhDur/rust-oracledb/actions/runs/29583399057)
- [tag-driven release run](https://github.com/MuhDur/rust-oracledb/actions/runs/29596141970)
- crates.io packages:
  [`oracledb`](https://crates.io/crates/oracledb/0.8.4),
  [`oracledb-protocol`](https://crates.io/crates/oracledb-protocol/0.8.4), and
  [`oracledb-derive`](https://crates.io/crates/oracledb-derive/0.8.4)

## Crates and the publish dependency graph

```
asupersync (already on crates.io, 0.3.9)
        ^
        |  (external dep)
        |
oracledb-protocol  ──┐
        ^            │
        │            ▼
oracledb-derive ──> oracledb   (the driver / flagship crate)
```

Resolved publish order (dependencies first, so each crate's registry deps already
exist when it is uploaded):

1. `oracledb-protocol`  — sans-I/O TNS/TTC wire protocol core
2. `oracledb-derive`    — `#[derive(FromRow)]` proc-macro
3. `oracledb`           — the async thin-mode driver

`oracledb` depends on `oracledb-protocol` (always) and `oracledb-derive` (under
the default `derive` feature). Both are declared with **both** a `path` (used for
local development) and a version pin matching the workspace release. During the
next release-preparation bump, all three versions must move together to 0.8.5:

```toml
oracledb-protocol = { path = "../oracledb-protocol", version = "0.8.5" }
oracledb-derive   = { path = "../oracledb-derive",   version = "0.8.5", optional = true }
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

# 1. Dry-run everything first (see "Dry-run" below for expected results).
cargo publish --dry-run -p oracledb-protocol
cargo publish --dry-run -p oracledb-derive
cargo publish --dry-run -p oracledb            # fails until 1 & 2 are live — expected

# 2. Real publish, dependency order. Wait for the index to update between steps.
cargo publish -p oracledb-protocol --locked --all-features
cargo publish -p oracledb-derive   --locked --all-features
cargo publish -p oracledb          --locked --all-features
```

crates.io usually makes a new version resolvable within a minute or two. If the
`oracledb` publish reports "no matching package named `oracledb-protocol`",
the index simply has not caught up yet — wait and retry. `cargo publish` recent
versions block until the just-uploaded crate is available, so a manual wait is
rarely needed.

## What is excluded from each package, and why

Each published crate ships only what is needed to build and use it. Heavy
dev-time assets are excluded to keep the tarball lean.

| crate | `exclude` | rationale |
|---|---|---|
| `oracledb` | `tests/` | integration + live tests and the `tests/fixtures` cassette/TLS corpus (~300 KB) are dev-only. `benches/` and `examples/` are kept so the `[[bench]]` targets and doc examples resolve. |
| `oracledb-protocol` | `tests/`, `fuzz/`, `proptest-regressions/` | the `tests/golden/` wire-trace corpus is ~744 KB; the `fuzz/` sub-crate and proptest regression seeds are dev-only. Inline `src/.../proptests.rs` stay (they are source). |
| `oracledb-derive` | (none needed) | already only `src/lib.rs`. |

Resulting package file counts: `oracledb-protocol` ~38 files, `oracledb-derive`
5 files, `oracledb` ~21 files (17 `src/` + README + manifests + benches/examples).

## README and license

- `oracledb` ships a crate-local `crates/oracledb/README.md` (cargo cannot package
  a README outside the crate dir, so the rich repo-root `README.md` cannot be
  referenced directly; the crate README links to the repo for the full docs).
- `oracledb-protocol` and `oracledb-derive` set `readme = false` — they are
  internal crates whose user-facing docs live on the `oracledb` crate.
- Licensing is declared via the SPDX `license = "MIT OR Apache-2.0"` field
  (inherited from `[workspace.package]`). crates.io accepts the SPDX expression;
  no per-crate `LICENSE-*` file copy is required. The canonical `LICENSE-MIT`,
  `LICENSE-APACHE`, and `NOTICE` live at the repo root.

## Metadata completeness (per published crate)

All three inherit `version`, `edition`, `license`, `repository`,
and `homepage` from `[workspace.package]`, plus a `documentation = https://docs.rs/<crate>`
default. Per-crate specifics:

| field | oracledb | oracledb-protocol | oracledb-derive |
|---|---|---|---|
| `description` | workspace default (driver) | "Sans-I/O Oracle TNS/TTC protocol core…" | "Procedural macros for the `oracledb` driver…" |
| `keywords` | oracle, database, driver, async, tns | oracle, database, protocol, tns, ttc | oracle, database, derive, proc-macro, fromrow |
| `categories` | database, asynchronous | database, network-programming | database |
| `readme` | `README.md` | `false` | `false` |

## Expected next-release dry-run behavior

After the 0.8.5 version bump, run with the repository's managed Cargo target
configuration (never redirect to or alter `/tmp/cargo-target`):

- `cargo publish --dry-run -p oracledb-protocol` → **PASS** (packages + verify-builds
  clean; 38 files).
- `cargo publish --dry-run -p oracledb-derive` → **PASS** (5 files, builds clean).
- `cargo publish --dry-run -p oracledb` → **fails at dependency resolution**
  before the 0.8.5 dependency crates are published:
  `no matching package named oracledb-derive found / location searched: crates.io
  index`. This is **expected**: `oracledb` depends on `oracledb-protocol` and
  `oracledb-derive` by `version`, and those are not on crates.io until steps 1–2
  of the real publish run. The check fires during packaging (even with
  `--no-verify`), so the only way to fully green this dry-run is to publish the
  two dependency crates first. `cargo package --list -p oracledb` confirms the
  package contents and metadata are otherwise correct.

## Post-publish checklist

After each `cargo publish`, and once all three are live:

- [ ] Each crate page loads: `https://crates.io/crates/oracledb`,
      `.../oracledb-protocol`, `.../oracledb-derive`.
- [ ] docs.rs build succeeds: `https://docs.rs/oracledb` (check the build log;
      enable any required features there if the default docs are thin).
- [ ] `cargo add oracledb@0.8.5` in a fresh project resolves `0.8.5` and compiles a
      trivial `use oracledb::ConnectOptions;`.
- [ ] Tag the release in git: `git tag v0.8.5 && git push origin v0.8.5` (operator-authorized only).
- [ ] Verify the published `oracledb` README renders correctly on crates.io
      (links point at the GitHub repo, not broken relative `docs/` paths).
- [ ] Confirm `oracledb-pyshim` and `oracledb-protocol-fuzz` did NOT get
      published (they should not appear on crates.io).

## Re-publish notes

- A version, once uploaded, is immutable. To ship a fix, bump
  `[workspace.package].version` (e.g. `0.1.1`) and re-run the order above.
- Never `cargo yank` casually; yank only removes a version from new resolution,
  it does not delete it.
