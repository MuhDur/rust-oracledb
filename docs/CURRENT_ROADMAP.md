# Current Roadmap — 0.8.5 Patch

**Status as of 2026-07-18.** Version
[**0.8.4**](https://github.com/MuhDur/rust-oracledb/releases/tag/v0.8.4) was
published on 2026-07-17 from exact tag SHA
[`2d8f4cb`](https://github.com/MuhDur/rust-oracledb/commit/2d8f4cb015eb7ed4180415342d8ca50a7ecb384c).
It is live on crates.io as
[`oracledb`](https://crates.io/crates/oracledb/0.8.4),
[`oracledb-protocol`](https://crates.io/crates/oracledb-protocol/0.8.4), and
[`oracledb-derive`](https://crates.io/crates/oracledb-derive/0.8.4). The next
patch target is exactly **0.8.5**. Workspace manifests remain at the published
0.8.4 value until the deliberate 0.8.5 release-preparation bump. The June 2026
planning and qualification records remain available as
[ROADMAP.md](ROADMAP.md) and [GROUND_TRUTH.md](GROUND_TRUTH.md).

## Current capability and evidence

| Area | Current state | Evidence boundary |
| --- | --- | --- |
| Thin-mode transport | Plain TCP and TCPS are implemented in pure Rust; no OCI, ODPI-C, Instant Client, or native Oracle library is used. | TCPS uses rustls and supports TLS 1.2/1.3, certificate validation, and server-DN matching. |
| Wallets | `ewallet.pem`, `ewallet.p12`, and `cwallet.sso` are supported without a feature flag. | The 0.8.4 release includes live Autonomous Database endpoint validation for downloaded-wallet TCPS/mTLS and IAM proof-of-possession authentication. |
| Reference parity | **2,462 passed of 2,578 collected**, with **116 justified skips** and no recorded regression versus the baseline. | This is historical qualification evidence from `b4a0cd3e77e3d7ed9cd875ba8002968860c9954a`, not a fresh 0.8.4 run. See [RELEASE_CERTIFICATION.md](RELEASE_CERTIFICATION.md). |
| Toolchain | The pinned nightly toolchain is required at build time because `asupersync` currently uses `try_trait_v2` and `try_trait_v2_residual`. | This requirement belongs to the async-runtime dependency, not to Oracle connectivity or the released runtime artifact. See [TOOLCHAIN.md](TOOLCHAIN.md). |
| 0.8.4 release | Published from `2d8f4cb015eb7ed4180415342d8ca50a7ecb384c`. | The [qualification run](https://github.com/MuhDur/rust-oracledb/actions/runs/29583399057) and [tag-driven release run](https://github.com/MuhDur/rust-oracledb/actions/runs/29596141970) both completed successfully. |

For the detailed transport and wallet compatibility boundary, see
[SUPPORT.md](SUPPORT.md). For the exact dependency and nightly attribution, see
[TOOLCHAIN.md](TOOLCHAIN.md).

## Route to the 0.8.5 patch

1. Implement and close the scoped repository-local Beads selected for 0.8.5,
   preserving the thin-only, fail-closed decode, reference-parity, and
   `OwnedRowStream` recovery contracts.
2. During release preparation, bump the workspace and internal dependency pins
   exactly from 0.8.4 to 0.8.5. Qualify that exact candidate SHA with the
   required gates and version-matrix artifact; neither the June parity record
   nor the 0.8.4 qualification may substitute for 0.8.5 evidence.
3. Verify that `v0.8.5` names the candidate workspace version and is contained
   in `origin/main`, following [PUBLISHING.md](PUBLISHING.md) and the release
   preflight.
4. Tag and publish only after operator authorization. The tag workflow remains
   the release authority.

## After the published 0.8.4 release

The release work remains evidence-led: preserve the thin-only, fail-closed
decode, reference-parity, and `OwnedRowStream` recovery contracts while taking
work from the repository-local Beads graph. Any 0.8.5 release claim must name
the exact SHA and distinguish new evidence from both the archived June record
and the published 0.8.4 proof.
