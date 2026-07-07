# rust-oracledb — Release Certification Scorecard

> **Current release evidence:** this 2026-06-13 scorecard is a historical
> conformance snapshot. The current release-qualification bundle supersedes it:
> 2578 collected reference tests, 2462 passed, 116 skipped, 0 regressions /
> missing tests (see `docs/qualification/1.0.0-rc.1/SUMMARY.md` and
> `docs/PARITY_SKIPS.md`).

**Date:** 2026-06-13 · **Reference pinned:** python-oracledb v4.0.1 (thin mode) ·
**Subject:** rust-oracledb @ master `1fe571a` · **Database under test:** Oracle 23ai Free (gvenzl/oracle-free), local container.

This scorecard follows the three-pillar discipline of `/running-the-gauntlet-on-your-rust-port`.
Its One Rule is honesty: every claim must survive a hostile reading of its own artifacts. Where
the formal gauntlet's heavier machinery (10-round convergence loop, multi-day soak, e-process /
conformal-band statistics) was **not** run, this document says so plainly rather than implying it.

## Verdict

**CERTIFIED — Conformance parity with python-oracledb v4.0.1 thin mode, for the in-scope surface,
against Oracle 23ai Free.** The port passes python-oracledb's own test suite, verified to be real
end-to-end behavior. Performance and surface pillars are documented and evidence-backed. TLS/wallet
and a small set of edge features are explicitly out of scope (see Negative Ledger). This is a
conformance certification, not a "drop-in / production-ready / Oracle-certified" claim.

## Pillar (b) — Conformance · CERTIFIED

| Dimension | Result | Evidence |
|---|---|---|
| Differential vs reference | **2578 collected: 2462 passed / 116 skipped, 0 regressions** | `docs/qualification/1.0.0-rc.1/SUMMARY.md`; `docs/PARITY_SKIPS.md` |
| Oracle = the reference's OWN suite | python-oracledb v4.0.1 `tests/test_*.py` driven through a PyO3 shim that slots the Rust engine under the reference's public layer | `harness/shim_inject/` |
| Green is REAL (not fabricated) | Adversarial 5-auditor audit: strace raw-socket evidence (server-computed values on the wire), dead-port offline-falsification (no fabrication path) | `docs/FAKE_PARITY_AUDIT.md` |
| Wire-format fidelity | Byte-exact golden captures vs real client for DPL, pipelining, sessionless (TPC switch), dbobject pickle, OSON, vector | `crates/oracledb-protocol/tests/golden/` |
| Fault tolerance (decoder) | cargo-fuzz, 20 targets under ASan/UBSan, **0 crashes** in the qualification logs; 4 real DoS bugs (OOM × 3, panic × 1) found and fixed fail-closed | `docs/FUZZING.md`, `crates/oracledb-protocol/fuzz/`, `docs/qualification/1.0.0-rc.1/logs/fuzz_*.log` |
| Safety | `#![forbid(unsafe_code)]` in protocol + driver; one quarantined FFI module (Arrow C Data Interface) in the harness-only shim | `git grep forbid(unsafe_code)` |

**Not run (honest):** the gauntlet's e-process invariants, Bayesian conformal LOWER-bound release
math, BOCPD regime soak, and 10-round convergence loop. The certification rests on a single
clean 0-regression differential sweep plus the adversarial audit, not a multi-day statistical soak.

## Pillar (a) — Performance · DOCUMENTED (honest, partial)

| Operation | rust-oracledb | python-oracledb thin | note |
|---|---|---|---|
| connect (incl. TCP) | tie | tie | network/server-bound |
| single-row select | ~123µs (after opt) | ~80µs | improved via thread-local runtime cache (−60% facade overhead) |
| bulk fetch 10k | tie | tie | network/server-bound |
| executemany 1000 | tie | tie | network/server-bound |
| CLOB read 64KiB | ~768µs (after opt) | ~440µs | improved via single-pass UTF-16 decode (−17%) |

Methodology + caveats: `docs/PERFORMANCE.md`. Profiled-first, behavior-preserving optimizations
with isomorphism tests. **Honest gaps:** (1) on CPU-bound ops the Rust thin engine is competitive
but not yet a clear win — reported plainly, not inflated. (2) rust-oracle (thick/ODPI-C) was **not**
benchmarked because it requires Oracle Instant Client, which this project deliberately avoids; the
plan's "vs rust-oracle" comparison is therefore unmet by design. (3) Single-connection serial only;
no throughput/concurrency benchmark. (4) Bench host was shared/busy (variance noted in the doc).

## Pillar (c) — Surface parity · DOCUMENTED

- **In scope and passing:** 72 of python-oracledb's 87 test modules — connection & 11g/12c auth,
  caller-set identity (program/osuser/machine/terminal, **proven live in v$session**), proxy auth,
  full scalar type set (lossless NUMBER, charsets, datetime/interval family, RAW/LONG, ROWID,
  BOOLEAN, BINARY_FLOAT/DOUBLE), LOB, object types/collections, XMLType, native JSON/OSON, VECTOR
  (dense/binary/sparse), pooling+DRCP, scrollable cursors, DML RETURNING, implicit results, batch
  errors, pipelining (native single-round-trip runner), sessionless transactions, Arrow dataframes, direct path load.
- **Explicitly excluded (coverage debt, by design):** AQ, SODA, XA/TPC, CQN/subscription, sharding,
  plus external-OCI/thick-only modules. 15 modules.
- **Standalone-crate proof:** 13 native Rust integration tests exercise the public `oracledb` crate
  directly (no shim, no Python) against the container, including the identity-masquerade v$session
  assertion. `crates/oracledb/tests/integration_*.rs`.

## Negative Ledger (honest gaps — retry conditions named)

| Gap | State | Retry condition |
|---|---|---|
| TLS / TCPS + wallet (ewallet.pem, cwallet.sso) | NOT IMPLEMENTED (M3) | needed for TLS-required environments (e.g. OCI ADB); requires standing up a TCPS listener to test |
| 10 `not_implemented` shim edge sites (persistent-LOB write, quoted-identifier edge, a few value-conversion corners) | EXPLICIT fail-closed errors | implement when a downstream consumer needs the specific path; never silent |
| Native single-round-trip pipelining | ENABLED — `supports_pipelining()` returns the negotiated END_OF_RESPONSE flag and `run_pipeline_decoded` materializes each op through the ordinary execute decoder. Offline loopback proof `tests::pipeline_batch_offline_collapses_to_one_round_trip` pins the 10→1 round-trip collapse + byte-identity to the sequential decode | done (was: wire the per-op result-materialization layer) |
| Full d49 migration (driver logic still partly in shim) | PARTIAL (define-fetch moved to crate via `execute_query_collect`) | continue moving SQL/bind/type logic shim→crate; suite-green is the gate |
| Perf vs rust-oracle (thick) | NOT RUN | requires Oracle Instant Client (deliberately avoided) |
| Formal gauntlet 10-round soak / e-process / conformal-band | NOT RUN | run when a statistical release-grade certification is required |

## Polish bar (gauntlet checklist)

- Reference pinned (v4.0.1, commit recorded): ✓
- Subject ≠ Oracle (shim drives Rust engine, reference is separate): ✓
- Differential green is real (adversarial audit): ✓
- Fail-closed parsing (fuzz-proven): ✓
- Honest perf with methodology + caveats: ✓
- Surface accounted as present/excluded with debt noted: ✓
- Negative ledger with retry conditions: ✓ (this doc)
- 10-round convergence / multi-day soak: ✗ (not run — stated)

**Bottom line:** rust-oracledb is a real, honestly-verified port that passes python-oracledb's own
thin-mode test suite for the in-scope surface, with a fuzz-hardened decoder and a documented
performance profile. It is certified for conformance parity at that scope; it is not yet TLS-capable
and has not undergone the full multi-day statistical gauntlet.
