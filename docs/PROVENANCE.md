# Supply-chain provenance

This document describes the provenance and packaged-source guarantees for the
`rust-oracledb` workspace and how to verify them. The supporting checks run in
the `release-qualification` CI profile and at release time.

## Artifacts

`scripts/gen_sbom.sh` regenerates the following committed artifacts under
`docs/provenance/` deterministically from `cargo metadata` (no external SBOM
tooling required; sorted, no timestamps, so they diff cleanly):

| Artifact | Contents |
| --- | --- |
| `cyclonedx.json` | CycloneDX 1.5 SBOM of the published crates' **non-dev** dependency closure (`oracledb` + `oracledb-protocol` + `oracledb-derive`, following normal/build edges only). |
| `dependencies.tsv` | Human dependency inventory: `name`, `version`, `license`, `source`. |
| `github-actions.tsv` | Every external `uses:` in `.github/workflows/`, with its pinned ref. |

`scripts/gen_sbom.sh --check` fails CI if these committed artifacts are stale
(a dependency or workflow change without a regenerated SBOM), mirroring the
`gen_baseline.sh --check` public-API drift gate.

## Release-time provenance

The `Release` workflow (`.github/workflows/release.yml`) additionally:

- builds the static `x86_64-unknown-linux-musl` smoke binary and emits a
  `*.tar.gz.sha256` checksum;
- regenerates the SBOM + inventories and attaches them to the GitHub release
  (`oracledb-sbom-cyclonedx-<tag>.json`, `oracledb-dependencies-<tag>.tsv`,
  `oracledb-github-actions-<tag>.tsv`);
- records a signed GitHub **build-provenance attestation** for the static binary
  via `actions/attest-build-provenance` (job permissions `id-token: write` +
  `attestations: write`).

Verify the binary attestation after a release:

```bash
gh attestation verify oracledb-smoke-x86_64-unknown-linux-musl.tar.gz \
  --repo MuhDur/rust-oracledb
```

Every external action is pinned to a full-length commit SHA (enforced by review
and surfaced in `docs/provenance/github-actions.tsv`), including
`actions/attest-build-provenance` (`e8998f9…` == `v2`).

## Packaged-source guarantees

Two checks prove the published crates are self-consistent and build without any
workspace path resolution:

- **Inter-crate version-pin guard** (`scripts/release_preflight.sh`, tested by
  `scripts/test_release_preflight_pins.sh`): the published `oracledb` crate's
  path-dependency version requirements on `oracledb-protocol` /
  `oracledb-derive` must equal the workspace version. This closes the stale-pin
  gap that bit 0.2.1/0.2.2 (a crate published with a requirement that resolves a
  wrong/old sibling from crates.io).
- **Standalone packaged-crate build** (`scripts/check_standalone_package.sh`):
  packages all three crates, asserts each packaged `Cargo.toml` strips the
  inter-crate `path =` and pins the workspace version, then extracts every
  `.crate` outside the workspace and builds each one there — `oracledb` against
  the extracted sibling tarballs, never the workspace — plus `cargo publish
  --dry-run` for the two leaf crates.

## Crate integrity

Crates are published in dependency order by `scripts/publish_crates.sh`
(idempotent across retries). crates.io records each crate's `.crate` SHA-256 in
its index; consumers verify it automatically via `Cargo.lock`'s `checksum`
field. No external `[patch]`/`path`/`git` source is used by the published
crates, so a from-registry build resolves the full dependency closure from
crates.io.
