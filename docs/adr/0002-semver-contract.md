# ADR-0002: Full External SemVer and Support Contract

Status: Accepted

Date: 2026-06-19

## Context

The crate already has public Rust consumers and first-party consumers. The
Road-to-1.0 plan chooses a full external SemVer and support contract instead of
treating the Rust API as an unstable internal detail.

The hard part is sequencing. Cleanup that removes methods, privatizes fields, or
adds `#[non_exhaustive]` is itself breaking. Blocking `cargo-semver-checks`
before that cleanup would prevent the intended redesign.

## Decision

Ship a 0.3.0 migration release that performs the planned breaking cleanup and
migrates first-party consumers. During 0.3.0 development,
`cargo-semver-checks` is advisory. Once 0.3.0 ships, its public API becomes the
baseline and the SemVer gate flips to blocking for every later release.

The gate catches unintended breaks. Intentional breaks remain allowed, but they
must use the correct version bump and refresh the baseline in the same release.

0.x versioning follows these rules:

- breaking change: minor bump, for example 0.3.x to 0.4.0.
- patch release: non-breaking only, for example 0.3.1.

At 1.0 and later, additive changes remain in 1.x and real breaking changes move
to 2.0 or later.

## Consequences

- W0-T3 wires `cargo-semver-checks` as advisory first, then W2-T1 flips it to
  blocking at the 0.3.0 release.
- Public API snapshots under `docs/baseline/public_api/` are the reviewed input
  for API-ledger and SemVer work.
- Obsolete shims and accidental public internals must be removed before
  `1.0.0-rc.1`.
- The supported feature-profile matrix must be explicit; `--all-features` is not
  a substitute for promised support.

## Review Trigger

Revisit the strictness only if maintenance cost from typed dependency bridges
becomes disproportionate. The known recurring involuntary break source is public
surface that exposes dependency types from:

- `chrono`
- `uuid`
- `rust_decimal`
- `serde_json`

The preferred mitigation is to keep those bridges feature-gated, minimize public
dependency-typed surface, and treat dependency-major upgrades as deliberate
baseline-updating releases.

Changing this decision requires a new ADR.
