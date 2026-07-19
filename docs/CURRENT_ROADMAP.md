# Current Roadmap — 0.8.4 Release Candidate

**Status as of 2026-07-16.** The workspace version is **0.8.4**, prepared as a
release candidate. The latest published version is **0.8.3**; 0.8.4 has not
been tagged or published. This document is the current planning view. The June
2026 planning and qualification records remain available as
[ROADMAP.md](ROADMAP.md) and [GROUND_TRUTH.md](GROUND_TRUTH.md).

## Current capability and evidence

| Area | Current state | Evidence boundary |
| --- | --- | --- |
| Thin-mode transport | Plain TCP and TCPS are implemented in pure Rust; no OCI, ODPI-C, Instant Client, or native Oracle library is used. | TCPS uses rustls and supports TLS 1.2/1.3, certificate validation, and server-DN matching. |
| Wallets | `ewallet.pem`, `ewallet.p12`, and `cwallet.sso` are supported without a feature flag. | Real wallet files and a rustls live handshake are tested. A full Autonomous Database endpoint acceptance run remains a separate qualification item. |
| Reference parity | **2,462 passed of 2,578 collected**, with **116 justified skips** and no recorded regression versus the baseline. | This is historical qualification evidence from `b4a0cd3e77e3d7ed9cd875ba8002968860c9954a`, not a fresh 0.8.4 run. See [RELEASE_CERTIFICATION.md](RELEASE_CERTIFICATION.md). |
| Toolchain | The pinned nightly toolchain is required at build time because `asupersync` currently uses `try_trait_v2` and `try_trait_v2_residual`. | This requirement belongs to the async-runtime dependency, not to Oracle connectivity or the released runtime artifact. See [TOOLCHAIN.md](TOOLCHAIN.md). |

For the detailed transport and wallet compatibility boundary, see
[SUPPORT.md](SUPPORT.md). For the exact dependency and nightly attribution, see
[TOOLCHAIN.md](TOOLCHAIN.md).

## Route to an 0.8.4 release

1. Qualify the exact candidate SHA with the required release gates and record
   the version-matrix artifact for that SHA. Historical evidence cannot be
   substituted for a new candidate's qualification.
2. Verify the tag names the candidate workspace version and is contained in
   `origin/main`, following [PUBLISHING.md](PUBLISHING.md) and the release
   preflight.
3. Publish only after the operator authorizes the release. The tag workflow is
   the release authority; a prepared workspace version is not a published
   release.

## After the candidate

The release work remains evidence-led: preserve the thin-only, fail-closed
decode, reference-parity, and `OwnedRowStream` recovery contracts while taking
work from the repository-local Beads graph. Any new release claim should name
the exact SHA and distinguish new evidence from the archived June record.
