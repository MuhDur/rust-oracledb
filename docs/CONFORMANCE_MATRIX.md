# Conformance Matrix

This is the live W3-E7.2 runtime conformance record for the support contract in
[`docs/SUPPORT.md`](SUPPORT.md). It distinguishes the configuration that was
actually run on this machine from configurations that are covered by CI or still
require a manual environment.

The machine-readable run output is written to `harness/.results/matrix.json`.
That path is intentionally under the existing ignored results directory; it is a
local evidence artifact, not a source file. The runner redacts connect strings,
usernames, and passwords.

## Live Run

Command:

```sh
eval "$(scripts/container.sh env)"
bash scripts/conformance_matrix.sh
```

Local cell:

| cell | status | evidence |
|---|---|---|
| Oracle Free TCP, password auth, AL32UTF8, `x86_64-unknown-linux-gnu` | GREEN | `harness/run.sh diff` compared `harness/.baseline/baseline.json` to `harness/.results/rust.json`: baseline `2578`, current `2578`, regressions `0`, missing `0`, beats `0`. |

Reference versions recorded by the live run:

| item | value |
|---|---|
| python-oracledb reference | `4.0.1` (`reference/python-oracledb` tag `v4.0.1`, revision `3daef052904e41668bb862e6fa40f43c22a81beb`) |
| Oracle server | `Oracle AI Database 26ai Free Release 23.26.1.0.0 - Develop, Learn, and Run for Free` / `Version 23.26.1.0.0` |
| Session NLS / charset | `AMERICAN_AMERICA.AL32UTF8`; database `NLS_CHARACTERSET=AL32UTF8`, `NLS_NCHAR_CHARACTERSET=AL16UTF16`; `nls_charset_id('AL32UTF8')=873` |
| Rust toolchain | recorded in `harness/.results/matrix.json` from `rustc --version`, `rustc -Vv`, `cargo --version`, and `rustup show active-toolchain` |

## SUPPORT.md Coverage

| SUPPORT.md dimension | status | local evidence / reason | coverage outside this local cell |
|---|---|---|---|
| Oracle Database 23ai/26ai family on the local Free container | GREEN | The live differential cell ran against Oracle Free `23.26.1.0.0` over TCP and matched the cached python-oracledb baseline with `2578/2578` tests and zero regressions/missing tests. | This is the primary live parity cell for W3-E7.2. |
| Oracle Database 12.1 / 12.2 / 18c / 19c / 21c server families | MANUAL | Not runnable on the single local `rust-oracledb-free` container, which is Oracle Free `23.26.1.0.0`; older listener/TTC versions require separate database installations. | Protocol version floor/cap behavior is covered by Rust tests around `TnsVersion::negotiate` and TTC gates. Live differential proof requires rerunning `scripts/conformance_matrix.sh` against each older server family. |
| TNS floor `300`, desired `319`, TTC field-version gating | GREEN for the local negotiated server; MANUAL for older server releases | The local 23.26 server exercises the normal handshake path under the 2578-test diff. It does not prove each lower release gate live. | Unit coverage for reject-below-floor and cap-to-desired behavior lives in `crates/oracledb-protocol/src/capabilities.rs`; older release live coverage is manual as above. |
| Plain TCP transport | GREEN | The local differential cell uses the TCP listener exposed by `scripts/container.sh env`. | Also covered throughout live Rust integration tests that use the same container env. |
| TCPS / rustls TLS 1.2 and 1.3 | CI/MANUAL | Not runnable on this container: the gvenzl Oracle Free image exposes a TCP listener only and does not provide a TLS-configured Oracle listener/wallet. | `crates/oracledb/tests/tls_handshake.rs` exercises a real rustls handshake, certificate validation, DN/name checks, and data round-trip against a local rustls server. `crates/oracledb-protocol/tests/tls_wallet.rs` covers wallet parsing and SNI/DN helpers. End-to-end Oracle TCPS remains manual with a TCPS listener as described in `docs/TLS_SETUP.md`. |
| Client charset AL32UTF8, id `873` | GREEN | The live run recorded `AMERICAN_AMERICA.AL32UTF8`, database `NLS_CHARACTERSET=AL32UTF8`, and `nls_charset_id('AL32UTF8')=873`, with zero diff regressions. | Protocol constants and codec paths remain covered by the Rust test suite. |
| NCHAR/NVARCHAR/NCLOB over UTF-8 wire handling | GREEN within the python-oracledb selected suite | Covered only to the extent those cases are present in the 2578 selected python-oracledb tests. No separate NCHAR-only local cell was run. | Additional codec and LOB coverage lives in Rust tests; gaps should be promoted into the differential corpus rather than allowlisted. |
| `TIMESTAMP WITH TIME ZONE` fixed offset | GREEN within the python-oracledb selected suite | Covered only to the extent fixed-offset TSTZ cases are present in the selected differential suite. | Codec tests cover fixed-offset decode paths. |
| Named-region `TIMESTAMP WITH TIME ZONE` unsupported/fail-closed | CI | Not a green runtime support promise; `SUPPORT.md` marks named-region TSTZ reads unsupported. | Rust codec tests cover the typed unsupported-feature path; no local green cell should claim named-region support. |
| `x86_64-unknown-linux-gnu` platform | GREEN | The live harness ran on the host GNU/Linux Python/pytest environment and produced the zero-regression diff. | `_quality.yml` also runs quality jobs on `ubuntu-latest`. |
| `x86_64-unknown-linux-musl` platform | CI | Not runnable through this Python differential harness on the host glibc Python/pytest environment. | `.github/workflows/release.yml` builds the static musl smoke binary; release-qualification in `_quality.yml` runs `scripts/check_musl_size.sh` with `x86_64-unknown-linux-musl`. |
| Other platforms | MANUAL | `SUPPORT.md` marks other Linux arches, macOS, and Windows best-effort/untested; no local run was performed. | No CI proof in this repo at W3-E7.2. |
| Password auth (O5LOGON) | GREEN | The local differential cell connects as the password-authenticated local test user and matched the python-oracledb baseline. | Live integration tests also use password auth. |
| Proxy auth | GREEN within the python-oracledb selected suite | The container env provides proxy credentials and the selected python-oracledb suite includes thin-mode proxy coverage where applicable. | Rust auth payload tests cover `PROXY_CLIENT_NAME` wiring. |
| Change-password at connect | GREEN within the python-oracledb selected suite | Covered only to the extent python-oracledb's selected thin-mode tests exercise change-password behavior against this container. | Rust auth tests cover the request shape. |
| OCI IAM / OAuth2 bearer token auth | CI/MANUAL | Not run locally because `SUPPORT.md` requires token auth over TCPS and this container has no TCPS listener/token-capable database setup. | `crates/oracledb/tests/access_token.rs` covers token redaction and the typed `AccessTokenRequiresTcps` fail-closed guard. Real token auth needs a TCPS/token-capable database. |
| Unsupported auth modes: unknown verifier, NNE, IAM request signing, Kerberos, RADIUS/native MFA, external/passwordless auth | CI/MANUAL | These are not green support promises. The matrix accounts for them as fail-closed or post-1.0 unsupported paths. | Rust protocol/driver tests cover typed errors for implemented fail-closed paths; Kerberos/RADIUS/external/IAM signing remain intentionally out of scope per `SUPPORT.md` and require future beads before any green claim. |

## Diff Contract

`harness/run.sh diff` runs:

```sh
python scripts/compare_pytest_json.py \
  harness/.baseline/baseline.json \
  harness/.results/rust.json
```

The comparator loads each pytest JSON report and reduces it to:

```text
tests[].nodeid -> tests[].outcome
```

It then reports:

| field | meaning |
|---|---|
| `regression_count` | Baseline `passed`, current not `passed`. |
| `missing_count` | Baseline nodeid absent from the Rust-shim report. |
| `beat_count` | Baseline `failed` or `error`, current `passed`; informational, not a failure. |

The pass/fail gate is strict: `regression_count == 0` and `missing_count == 0`.
The script fails on either count being non-zero.
The runner also records the raw `harness/run.sh rust` and `harness/run.sh diff`
exit statuses in `matrix.json`; the conformance verdict is based on the diff
counts once a complete Rust pytest report exists.

Normalization is intentionally minimal. The comparator ignores pytest duration,
stdout/stderr, traceback text, ordering, and other metadata because it compares
only `nodeid` and final `outcome`. It does not rewrite test names, statuses, or
error messages, and it does not contain a regression allowlist.

The selected corpus is controlled by `harness/filter.txt`. At W3-E7.2 that file
declares the python-oracledb `v4.0.1` full-parity suite and contains no
`exclude` entries. Thin-mode upstream self-skips remain ordinary pytest
`skipped` outcomes in both baseline and Rust-shim reports; they are not hidden by
the comparator.
