# ADR-0001: Keep asupersync and the Pinned Nightly Through 1.0

Status: Accepted

Date: 2026-06-19

## Context

`oracledb` is the Rust thin driver used by first-party consumers such as the
Python shim and oraclemcp. The driver runs on asupersync, which currently
requires nightly Rust through `try_trait_v2` support. The nightly requirement is
a build-time constraint for the Rust crate and is invisible to users of
oraclemcp's single static binary.

The Road-to-1.0 baseline records the active toolchain pin in
`docs/baseline/version_pins.tsv`. CI and release jobs must use the same pin
unless the pin is intentionally moved by the toolchain runbook.

## Decision

Keep asupersync and the pinned nightly Rust toolchain through the 1.0 release.
Treat asupersync as part of the driver architecture, not as a temporary porting
artifact.

The rationale is practical:

- asupersync gives the driver a cancel-correct runtime model.
- LabRuntime and DPOR support cover concurrency failures the single-threaded
  Python reference suite cannot expose.
- the nightly toolchain is reproducibly pinned and can be monitored by CI.

## Consequences

- Required local and CI builds use `rust-toolchain.toml`.
- The canary CI lane must detect floating-nightly breakage without silently
  passing.
- `docs/TOOLCHAIN.md` is the response playbook for moving the pin when a
  detected break is real and relevant.
- Stable Rust compatibility is not a 1.0 requirement for the driver crate.

## Review Triggers

Revisit this decision only when at least one objective trigger occurs:

- `try_trait_v2` stabilizes.
- `try_trait_v2` changes in a way that invalidates the current pin.
- an equivalent stable runtime path appears.
- the pinned nightly blocks a security update.
- external Rust-crate adoption becomes a product goal.

Changing this decision requires a new ADR.
