# Changelog

All notable changes to the `oracledb` workspace are documented here. The format
is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project follows the SemVer contract described in
[`docs/adr/0002-semver-contract.md`](docs/adr/0002-semver-contract.md).

Scope window: canonical published release entries from 0.3.0 through 0.8.4.
The 0.7.4–0.8.2 spine was
reconstructed from git tags, GitHub Releases, tag ranges, release-matrix
artifacts, and the checked-in API/provenance ledgers.

## Version Timeline

| Version | Released | Release role |
| --- | --- | --- |
| [0.8.4](https://github.com/MuhDur/rust-oracledb/releases/tag/v0.8.4) | 2026-07-17 | Autonomous Database / IAM release with TCPS, wallet, and cancellation hardening |
| [0.8.3](https://github.com/MuhDur/rust-oracledb/releases/tag/v0.8.3) | 2026-07-14 | Arrow interoperability, endpoint APIs, and recovery hardening |
| [0.8.2](https://github.com/MuhDur/rust-oracledb/releases/tag/v0.8.2) | 2026-07-08 | K10 owned-row stream, timer correction, and quality campaigns |
| [0.8.1](https://github.com/MuhDur/rust-oracledb/releases/tag/v0.8.1) | 2026-07-08 | Wallet/session accessors and opt-in support capture |
| [0.8.0](https://github.com/MuhDur/rust-oracledb/releases/tag/v0.8.0) | 2026-07-07 | Breaking single-path consolidation plus additive routine/telemetry work |
| [0.7.4](https://github.com/MuhDur/rust-oracledb/releases/tag/v0.7.4) | 2026-07-07 | Clean security/provenance patch superseding 0.7.3 |

## Representative commits

- [0.8.0 single-path consolidation](https://github.com/MuhDur/rust-oracledb/commit/7627a1e50241f29523069cf47288f9ac651ff7bc)
- [0.8.1 additive introspection APIs](https://github.com/MuhDur/rust-oracledb/commit/e3c4b69333ae790cf12f6ea6f4df70633bf17f9f)
- [0.8.1 support capture](https://github.com/MuhDur/rust-oracledb/commit/899123075b7ae21eb779e19ab0a5be765b82ecfd)
- [0.8.2 runtime timer correction](https://github.com/MuhDur/rust-oracledb/commit/9059165a34b43921f44d56c6a2c12009f2fb23db)
- [0.8.2 K10 owning row stream](https://github.com/MuhDur/rust-oracledb/commit/06244bfa4c8aab2efb62bfdbd8793dc1e089bd02)

## [Unreleased]

## [0.9.0] - 2026-07-21

### Added

- **Bracket proxy usernames and proxy-only token authentication.**
  `ConnectOptions::new` accepts the reference forms `base[proxy]` and
  `[proxy]`; token authentication sends `PROXY_CLIENT_NAME` before
  `AUTH_TOKEN`, including the empty-base proxy-only form. `full_user()` exposes
  the display form without revealing credentials.
- **Offline Oracle 19c capability coverage.** A conservative,
  reference-derived synthetic 19c capability profile exercises the decoder and
  version gates, including their protocol gates. It proves offline branch
  selection, not live 19c session behavior.

### Changed

- **`ConnectOptions::new` now desugars bracketed usernames.** Existing 0.8.x
  callers that pass `foo[bar]` no longer retain that literal as the base user:
  it becomes base user `foo` plus proxy user `bar` (and `[bar]` becomes the
  proxy-only form). A later `with_proxy_user(...)` still overrides it.
- **Out-of-range Oracle session serials are rejected.** `AUTH_SERIAL_NUM`
  values outside the `u16` range no longer wrap to their low 16 bits; they
  return the existing session-field error.
- **TCPS trust anchors are a union.** Platform roots (or explicit
  `SSL_CERT_FILE` / `SSL_CERT_DIR` roots) are loaded through
  `rustls-native-certs` and wallet CA certificates are added to them. A wallet
  no longer narrows platform trust; explicit environment roots replace platform
  discovery before the wallet union.
- **Required-quality evidence is more deterministic.** Quality fan-out and
  release-qualification preparation preserve exact command evidence, fuzz
  target coverage, and non-mutating baseline checks.

### Fixed

- **TCPS setup and shutdown hardening.** Configuration errors fail before a
  transport attempt; DSN-only `SSL_SERVER_CERT_DN` reaches the verifier and
  DSN `SSL_SERVER_DN_MATCH=OFF` is honored explicitly while chain validation
  remains mandatory; configured TLS handshake budgets are honored; the OCI ADB
  SNI carve-out recognizes the supported endpoint forms; and a terminal TLS
  close without `close_notify` no longer causes a follow-on EOF write.
- **Connection and pool recovery.** A final pool handle is tracked without a
  race, `TIMEDWAIT` acquisition no longer creates one OS thread per waiter, and
  retries refuse `SELECT` statements hidden behind leading comments.
- **Value/parity correctness.** Arrow TSTZ values apply their display offset,
  and Arrow NUMBER sentinels can no longer collapse into `-1`; chrono preserves
  sub-minute `FixedOffset` instants; the Python harness classifies `NUMBER`
  values by declared scale, binds `Decimal` through exact text, and hardens
  conversion and cancellation behavior.
- **Malformed bind-name robustness.** A lone double quote no longer panics the
  public bind-name formatter.
- **Required-check stability.** Parallel `tnsnames.ora` tests now use a unique
  temporary-directory counter instead of colliding at coarse clock resolution.

## [0.8.4]

> Published to crates.io and tagged as `v0.8.4` on 2026-07-17.

### Added

- **OCI Autonomous Database (TCPS) connectivity.** Thin-mode connections to
  Oracle Autonomous Database over TCPS: SNI from the service descriptor,
  downloaded-wallet TLS material, and strict server-certificate-DN / host
  matching for Autonomous Database service endpoints.
- **OCI IAM database-token authentication with proof-of-possession.** An OCI IAM
  database token (`oci iam db-token get`) is a proof-of-possession credential:
  the driver now signs the auth header (RSA-PKCS1v15 + SHA-256) with the token's
  bound private key and sends `AUTH_HEADER` / `AUTH_SIGNATURE` (setting
  `TNS_AUTH_MODE_IAM_TOKEN`), so the database authenticates the token instead of
  rejecting it with `ORA-01017`. Set the pair with
  `ConnectOptions::with_access_token_and_key`; a plain OAuth2 bearer token still
  uses `ConnectOptions::with_access_token`. Validated live against a real
  Autonomous Database.

### Fixed

- **23ai TSTZ object metadata.** `TIMESTAMP WITH TIME ZONE` and `TIMESTAMP
  WITH LOCAL TIME ZONE` DbObject descriptors now retain their real
  precision/scale through the thin wire decoder, with a captured 23ai fixture
  and live regression gate.
- **Connection timeouts / keepalive (GH#14).** The thin connect path now honors
  the parsed `EXPIRE_TIME` TCP keepalive and `TRANSPORT`/`TCP_CONNECT_TIMEOUT`,
  and applies a read-inactivity timeout that resets on read progress, so a
  half-open or idle connection can no longer hang forever.
- **Deterministic in-flight cancellation typing.** Cancelling a pending read no
  longer races: a cancellation is always surfaced as the typed `Error::Cancelled`
  (`ORA-01013`) instead of occasionally leaking a raw `Io(Interrupted)` when the
  cancel flag had not yet propagated to a later checkpoint on the reading thread.
- **Hostile-input bounds.** Wallet readers and `tnsnames.ora` `IFILE` graphs are
  bounded against malformed/adversarial input, returning structured errors
  rather than unbounded work.
- **Exact-SHA tag qualification.** A release tag must consume immutable,
  exact-SHA Required and live-matrix evidence from the qualification workflow;
  a parent artifact is never substituted for the tagged candidate.

### Changed

- **Runtime update.** The async runtime is now pinned to `asupersync` 0.3.9.

## [0.8.3](https://github.com/MuhDur/rust-oracledb/compare/v0.8.2...v0.8.3) - 2026-07-14

### Added

- **Arrow interval support.** `INTERVAL DAY TO SECOND` and `INTERVAL YEAR TO
  MONTH` columns now map to the Arrow `MonthDayNano` interval on both the row and
  columnar `fetch_df` paths (previously an unsupported-type error), matching
  python-oracledb's DataFrame conversion. See
  [e0be7e8](https://github.com/MuhDur/rust-oracledb/commit/e0be7e8).
- **Columnar VECTOR fast path.** Dense, fixed-dimension `VECTOR` columns decode
  directly into a contiguous Arrow `FixedSizeList` child buffer on the columnar
  path — no per-row `QueryValue` materialisation — the 23ai/RAG bulk-embedding
  path where python-oracledb returns one GIL-bound list per row. See
  [238dc27](https://github.com/MuhDur/rust-oracledb/commit/238dc27).
- **Connection endpoint accessors.** `Connection::host()`, `port()`, and
  `protocol()` expose the connected endpoint, and `db_unique_name()` returns
  `SYS_CONTEXT('USERENV','DB_UNIQUE_NAME')` (parsed from the AUTH phase-two
  `AUTH_SC_REAL_DBUNIQUE_NAME` field). Additive.

### Fixed

- **TIMESTAMP WITH TIME ZONE Arrow values are wall-clock.** A TSTZ column
  fetched into a tz-naive Arrow `Timestamp` is now emitted at its wall-clock time
  instead of being UTC-normalised, matching python-oracledb (an Arrow
  `Timestamp(_, None)` carries no zone). See
  [eee7599](https://github.com/MuhDur/rust-oracledb/commit/eee7599).
- **DbObject TIMESTAMP/INTERVAL attribute precision and scale.** Object-type
  attribute describe now reports the data-dictionary precision/scale for
  TIMESTAMP / TSTZ / TSLTZ / INTERVAL types instead of `(0, 0)`. See
  [66750d2](https://github.com/MuhDur/rust-oracledb/commit/66750d2).
- **Pool double-release is a typed error.** Releasing a connection that is not
  checked out returns `PoolError::ConnectionNotAcquired` (surfaced as DPY-1001
  through the Python shim) instead of a silent `Ok`. See
  [e29a611](https://github.com/MuhDur/rust-oracledb/commit/e29a611).

- **One logical deadline for owning row streams.** Continuation pages now reuse
  the deadline captured for the initial query instead of receiving a fresh
  timeout budget per page. See [0708550](https://github.com/MuhDur/rust-oracledb/commit/0708550).
- **No idle BREAK for work that never started.** Deadline handling now
  distinguishes expiry before the database future is polled from an in-flight
  timeout, preserving a reusable idle connection and structured cancellation.
  See [b16d42d](https://github.com/MuhDur/rust-oracledb/commit/b16d42d).
- **Fail-closed support-capture redaction.** Stateful scrubbing covers auth
  markers and secret values split across transport frames, including long
  suffixes, before a cassette can be written. See
  [6ad3631](https://github.com/MuhDur/rust-oracledb/commit/6ad3631).
- **LOB reads advance by Oracle-reported progress.** `LobReader` validates and
  adopts the returned amount and locator rather than skipping units after a
  short read or retaining a stale locator. See
  [6a9f669](https://github.com/MuhDur/rust-oracledb/commit/6a9f669).
- **Failed query cursors are retired before connection reuse.** Continuation,
  scroll, and define/fetch error paths now evict stale cache and registry state,
  deduplicate close piggybacks, and preserve normal release only when an
  operation is proven not to have started. See
  [02eef5e](https://github.com/MuhDur/rust-oracledb/commit/02eef5e).
- **Abandoned borrowed streams cannot poison a reused connection.** Dropping a
  borrowed query stream now retires its cursor ownership instead of leaving a
  live server cursor behind. See
  [2bff86b](https://github.com/MuhDur/rust-oracledb/commit/2bff86b).
- **SODA query ownership and pagination are complete and fail closed.**
  One-shot operations release their query ownership, failed SODA queries retire
  their cursors, collection-name discovery fetches every page, terminal empty
  define fetches are not decoded as rows, raw identifiers are validated, and
  requested document-version predicates are preserved. See
  [7ce1124](https://github.com/MuhDur/rust-oracledb/commit/7ce1124),
  [9deb2f8](https://github.com/MuhDur/rust-oracledb/commit/9deb2f8),
  [e05057b](https://github.com/MuhDur/rust-oracledb/commit/e05057b),
  [cd31646](https://github.com/MuhDur/rust-oracledb/commit/cd31646),
  [f2cf86e](https://github.com/MuhDur/rust-oracledb/commit/f2cf86e), and
  [e2e5b40](https://github.com/MuhDur/rust-oracledb/commit/e2e5b40).
- **Cancellation drains every abandoned server response before reuse.** Define
  fetches, plain functions, LOB, AQ, password-change, CQN, TPC, and Direct Path
  operations now use the same recovery discipline, preventing a late response
  from being mistaken for the next operation's reply. See
  [f0938d0](https://github.com/MuhDur/rust-oracledb/commit/f0938d0),
  [99c24e4](https://github.com/MuhDur/rust-oracledb/commit/99c24e4),
  [ac8668d](https://github.com/MuhDur/rust-oracledb/commit/ac8668d),
  [24db0a4](https://github.com/MuhDur/rust-oracledb/commit/24db0a4),
  [cef60b0](https://github.com/MuhDur/rust-oracledb/commit/cef60b0),
  [cfd30d8](https://github.com/MuhDur/rust-oracledb/commit/cfd30d8),
  [92c45e3](https://github.com/MuhDur/rust-oracledb/commit/92c45e3), and
  [105fb65](https://github.com/MuhDur/rust-oracledb/commit/105fb65).
- **Retry loops stop immediately after cancellation.** A cancelled operation is
  no longer eligible for another attempt or backoff cycle. See
  [2cd419e](https://github.com/MuhDur/rust-oracledb/commit/2cd419e).
- **Bind shape and type errors fail before the first round trip.** Execute now
  preflights declared bind counts and rejects inconsistent first-execute batch
  column types without touching Oracle. See
  [0fc86a3](https://github.com/MuhDur/rust-oracledb/commit/0fc86a3).

## [0.8.2](https://github.com/MuhDur/rust-oracledb/releases/tag/v0.8.2) - 2026-07-08

### Added

- **K10 owning row stream.** `OwnedRowStream` implements
  `futures_core::Stream<Item = Result<Vec<Option<QueryValue>>>>`, consumes a
  `Connection`, and returns it after draining or through `into_connection`.
  `Connection::into_row_stream` and `into_query_stream` provide row-by-row pull
  without holding `&mut Connection` across yields. The implementation preserves
  duplicate-column continuation state, define-fetch behavior, successful
  drain/early-stop cursor release, and eager-query result parity. See the
  [implementation commit](https://github.com/MuhDur/rust-oracledb/commit/06244bfa4c8aab2efb62bfdbd8793dc1e089bd02)
  and the updated [design record](docs/design/k10-row-stream.md).
- **Decode and concurrency quality campaigns.** A typed-decode fuzz target,
  metamorphic tests for the differentiator paths, mutation-focused decoder
  tests, a deterministic performance-regression gate, and a nightly
  ThreadSanitizer lane were added. The documented decode mutation score reached
  90.8% (376 caught, 38 missed, excluding 17 unviable mutants).

### Changed

- **Accurate sub-250 ms runtime timers.** The pinned `asupersync` runtime moved
  from 0.3.4 to 0.3.5, removing the timer floor that stretched short deadlines,
  sleeps, and retry backoffs toward 250 ms. Workspace pins, provenance, and the
  SBOM were regenerated in the
  [0.8.2 release commit](https://github.com/MuhDur/rust-oracledb/commit/9059165a34b43921f44d56c6a2c12009f2fb23db).

### Fixed

- **K10 builds under the tracing feature.** The internal continuation-fetch
  future no longer requires `Send`; tracing holds a non-`Send` entered-span
  guard across an await, while the stream is intentionally driven on one
  asupersync lane. Default, tracing, and all-feature builds were verified before
  the release tag. See the
  [fix](https://github.com/MuhDur/rust-oracledb/commit/07f12efdea8f4b056efdf0a404a8d2db4e9fe959).

## [0.8.1](https://github.com/MuhDur/rust-oracledb/releases/tag/v0.8.1) - 2026-07-08

### Added

- **Wallet and negotiated-session introspection.** New additive APIs expose
  certificate validity metadata, the selected wallet file/resolution,
  negotiated TNS protocol version, and whether fast authentication was used.
  Wallet reporting and actual wallet loading share one resolver so their
  decisions cannot drift. See the
  [accessor implementation](https://github.com/MuhDur/rust-oracledb/commit/e3c4b69333ae790cf12f6ea6f4df70633bf17f9f).
- **Opt-in support capture.** With the `cassette` feature,
  `ORACLEDB_CAPTURE=<path>` records a session for offline replay. Authentication
  material is scrubbed and a final secret scan refuses the artifact rather than
  writing a known-secret capture. See the
  [K6 capture implementation](https://github.com/MuhDur/rust-oracledb/commit/899123075b7ae21eb779e19ab0a5be765b82ecfd).

## [0.8.0](https://github.com/MuhDur/rust-oracledb/releases/tag/v0.8.0) - 2026-07-07

### Changed

- **Single canonical public paths (breaking).** The redundant
  `oracledb::shape_cache::*` and `oracledb::lob_stream::*` module paths were
  removed. Their crate-root exports (`StatementShapeCache`, `ColumnShape`,
  `ShapeObservation`, `LobReader`, `LobWriter`, and `ClobReader`) remain the
  supported paths. This completed the consolidation deferred from 0.7.4. See
  the [0.8.0 consolidation](https://github.com/MuhDur/rust-oracledb/commit/7627a1e50241f29523069cf47288f9ac651ff7bc).

### Added

- **Real routine `IN OUT` binds.** `BindValue::InOut` and
  `RoutineCall::arg_in_out` preserve the input value, size the output buffer,
  and decode the server-returned value through the existing IO-vector path.
- **Operator-facing differentiator telemetry.** Feature-gated spans report only
  counts, booleans, and enum-like outcomes for row streaming, batch errors,
  statement-shape cache events, and LOB streaming; adversarial tests pin the
  no-SQL/no-bind/no-row/no-secret contract.
- **Stronger release evidence.** Native single-resource-manager TPC coverage and
  decoded-assert cassette replay for AQ and direct-path operations were added.

## [0.7.4](https://github.com/MuhDur/rust-oracledb/releases/tag/v0.7.4) - 2026-07-07

### Fixed

- **Clean patch superseding 0.7.3.** The lockfile and SBOM moved to
  `crossbeam-epoch` 0.9.20 for RUSTSEC-2026-0204, and the public-API baselines,
  API ledger, and provenance artifacts were regenerated. The 0.7.3 public API
  remained unchanged; its temporary dual-path exception was documented for the
  subsequent 0.8.0 removal. See the
  [release commit](https://github.com/MuhDur/rust-oracledb/commit/f3e218527290676c5b55b1db0106502b151cea41).

## [0.7.3] - 2026-07-07

### Added

- **OCI wallet + TCPS transport (`A2.1` / `A2.2` / `C1` / `C2`).** The driver
  can now connect over TCPS (TLS) using an Oracle wallet: it decrypts the
  legacy-3DES PKCS#12 client identity (`ewallet.p12`, bead `A2.1`) and falls
  through to `cwallet.sso` when the primary wallet is unusable (bead `A2.2`).
  The TCPS handshake is driven over rustls with the Oracle server-cert chain
  validation and DN / CN name match (positive plus the fail-closed negatives:
  host mismatch, cert-DN mismatch, wrong trust anchor). Exercised offline by a
  local rustls TCPS lane over synthetic wallet fixtures whose identifiers are
  fictional (`CN=oracle-test.invalid`), so no live wallet or secret is required
  to test it (`C1` fixtures + `gen_test_wallets.sh`, `C2` handshake lane).
- **OCI IAM token authentication seam (`A3` / `C3`).** A `TokenSource` trait lets
  a caller supply an OCI IAM database token; the driver sends the `AUTH_TOKEN`
  fast-auth frame over a TCPS descriptor and fail-closed refuses a token over a
  plaintext descriptor. Validated with a mock token source over the local TCPS
  lane (`C3`); no OCI account needed for the offline suite.
- **Pipelining: N operations in a single round trip (`A8`, 23ai).** Independent
  operations are buffered per-op and collapsed into one server round trip
  (N+1 responses), decoded and verified individually. Gated to 23ai, which
  negotiates the framing.
- **`executemany` per-row error continuation (`a4-j1w`).** A batch DML that hits
  a row-level error (e.g. `ORA-00001`) now reports a per-row `BatchError` at the
  correct index and continues applying the remaining rows, rather than aborting
  the whole batch.
- **VECTOR → Arrow `FixedSizeList` columnar fast path (`a4-0mk`, opt-in).**
  Fixed-dimension `VECTOR` columns can be fetched into a single Arrow
  `FixedSizeList` array (opt-in) for a columnar zero-copy-friendly read path.
- **Cross-connection statement-shape cache + DDL self-heal (`a4-8pp`).** The
  describe/bind shape of a statement is cached across connections and
  automatically invalidated and re-derived when DDL changes the underlying
  object, so a shape change never yields a stale decode.
- **Lazy LOB streaming reader/writer (`a4-bbx`).** CLOB/BLOB values can be read
  and written as streams rather than materialized whole, with a UTF-16-aware
  CLOB boundary so multi-byte characters are never split across chunks.
- **Idempotency-gated retry executor (`a4-r9a`).** A retry executor classifies
  failures against the ORA error taxonomy and retries only idempotent
  operations up to a budget; a non-idempotent failure (e.g. `ORA-00054`) is
  surfaced rather than retried.
- **Cancel-safe async row streaming and out-of-band cancel (`a4-x3s` /
  `a4-cn4`).** Async row streams are cancel-safe, and an out-of-band cancel
  (break/marker) leaves the session cleanly recovered and reusable.
- **Driver-native `RoutineCall` API (`a4-plsql-routine`).** Call stored
  procedures and functions directly with `IN`, `OUT`, and `RETURN` binds without
  hand-writing the anonymous PL/SQL block.
- **Thin-mode SODA breadth (`a4-h74`, gated < 21c).** Thin-mode SODA gains
  cursor / skip / limit / keys / version support and `insertManyAndGet` /
  QBE-operator breadth; version-gated below 21c (proof-of-gate test
  `a4-soda-pre21c`).
- **`oracledb::VERSION` constant (`A6`).** The crate version is exported as a
  `pub const` for callers that report it.
- **Deterministic cassette-replay CI (`a4-nnnz` / `a4-1s2`).** Post-auth LOB
  (create/write/read) exchanges are recorded and replayed byte-for-byte offline,
  with an orphan guard and a binary secret-scan hardening the CI capture.
- **Reference conformance + version matrix.** Behavior is checked against
  python-oracledb (field-by-field ground-truth differential), and the release
  gate runs the full live suite across Oracle 18c / 21c / 23ai plus the local
  OCI TCPS lane (the 11g lane asserts the below-floor structured version
  refusal).

### Fixed

- **Cold VECTOR fetch desync for define-requiring columns (bead `a4-0mk`).** A
  `VECTOR` (or other define-requiring) column fetched on a cold cursor could
  desync the row decode because no client-side define was established first. The
  driver now establishes the client-side define up front, so the first fetch of
  such a column decodes correctly.

## [0.7.2] - 2026-07-06

### Fixed

- **Direct-path load worked only on 23ai (bead `dpl23`).** The direct-path
  prepare / op / load-stream TTC messages wrote the ub8 pipeline token
  unconditionally; a pre-23ai server misparses the stray token and reads a
  later mandatory field past the end (`ORA-03147: missing mandatory TTC
  field`). New `*_with_version` builders gate the token via
  `write_function_header` on the negotiated `ttc_field_version`; the original
  builders are retained as byte-identical wrappers (semver-additive). Found by
  running the live suites across the version matrix; `live_dpl_arrow` now green
  on Oracle 18c, 21c, and 23ai.

- **Recovery drain now respects classic (pre-23ai) framing (bead `99xu`).**
  The break/cancel recovery drain read the trailing-error boundary assuming
  23ai `END_OF_RESPONSE` framing; on a pre-23ai server (which never negotiates
  it) the drain could misframe the trailing error. The session's negotiated
  framing is now threaded into the recovery path so break/cancel recovery is
  correct on Oracle 18c/21c as well as 23ai.
- **TPC (two-phase commit) payloads gate the ub8 token by negotiated version
  (bead `hkwd`).** `build_tpc_switch_payload_with_seq` /
  `build_tpc_change_state_payload_with_seq` unconditionally emitted the ub8 TTC
  token (23.1+ framing); a pre-23ai server misparses the stray token and fails
  the call (`ORA-03120`) — the same class as the earlier function-header gating.
  New version-aware `*_and_version` builders emit the token only when
  `ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1_EXT_1`; the original
  builders are retained as byte-identical wrappers (API-stable, semver-additive).

### Added

- **L2 version cassettes: per-version connect-negotiation wire, replayed
  offline per-PR (bead `so3w.3`).** The live version matrix records the real
  TTC wire exchange across the Oracle 11g/18c/21c/23ai Docker fleet, but it is
  slow and needs containers, so it only runs nightly. L2 records the
  connect-negotiation handshake (`CONNECT` + any `RESEND` + the server `ACCEPT`)
  **once per version** into a committed `.tns-cassette`
  (`crates/oracledb/tests/fixtures/cassettes/<lane>-connect.tns-cassette` +
  manifest) and **replays it offline** in ordinary `cargo test --features
  cassette` (`crates/oracledb/src/version_cassettes.rs`). The replay
  reconstructs each version's `CONNECT` request and asserts it byte-matches the
  recording (`ReplayWriteMode::Check` + `ReplayAudit`), then decodes the REAL
  server `ACCEPT` and asserts the version-gated outcome: xe11 → structured
  `UnsupportedVersion` refusal (protocol 314 < floor 315); xe18 → 317;
  xe21 → 318; free23 → 319 with `fast_auth` + `END_OF_RESPONSE`. So a
  cross-version wire regression (a version gate that flips the emitted request
  bytes or mis-decodes a real server response) now fails on **every PR** in
  seconds with no database and no network — pinning the negotiation decoder
  against ground-truth wire, not a hand-crafted fixture. The cassettes carry no
  secrets and no client randomness (the auth phase, with its `OsRng` session
  key and verifier/session-key/salt material, is captured *and* committed
  **never** — capture stops at `ACCEPT`) and use a fixed synthetic connect
  descriptor so the recorded `CONNECT` bytes leak no local identity and are
  byte-reproducible. A sanitization gate refuses to write any cassette that
  contains a known auth field name, and the offline replay re-checks the
  manifest checksum and re-scans for leaks. Test-only and behind the `cassette`
  feature (off by default) — no public API or default-build change. Broader
  per-version op coverage (a post-auth typed query and LOB/AQ/DPL/CQN
  round-trips) is tracked as a follow-up (`rust-oracledb-cwsr`).

- **L2 cassettes reach a post-auth typed query (bead `cwsr`).** The connect
  cassettes stop at `ACCEPT` because the auth phase is non-deterministic (the
  client `OsRng` session key makes its `C->S` bytes unrepeatable) and carries
  secrets. Post-auth coverage uses a **slice + seeded-loopback** approach: a
  full `connect + auth + execute` session is captured, the connect+auth prefix
  (and the trailing logoff) are sliced off, and only the deterministic,
  secret-free execute request/response frames are committed
  (`crates/oracledb/tests/fixtures/cassettes/<lane>-postauth.tns-cassette` +
  manifest). Offline replay (`replay_postauth_query_cassettes_offline`, in
  ordinary `cargo test --features cassette`) rebuilds a loopback `Connection`
  *seeded* from the manifest with the negotiated capabilities and the post-auth
  `ttc_seq_num` — both of which shape the execute request bytes — and replays a
  recorded `select cast(7 + 5 as number(6))` under `ReplayWriteMode::Check`, so
  a post-auth request-byte regression fails per-PR with no database. The seeding
  is what makes the replay byte-exact across generations, which a fresh
  connection cannot be: xe18 (`ttc_field_version=11`), xe21 (`16`) and free23
  (`24`) each negotiate a different field version and are pinned independently.
  Because the committed frames begin *after* authentication they carry no
  verifier / session-key / token material; a secret-field scan gates both
  capture and offline replay. Test-only, behind the `cassette` feature — no
  public API or default-build change.

- **Shared live-test connection helper (bead `8eew`).** The ~24 live
  (`#[ignore]`d) integration suites each repeated the same `PYO_TEST_*`
  environment resolution and free23 fallbacks. That resolution now lives in one
  place (`crates/oracledb/tests/common/mod.rs`): `live_creds_opt` (skip-if-unset
  idiom), `live_creds_required` (panic-if-unset idiom), and the
  `live_{conn_string,user,password}_or` fallback helpers. Behavior is byte-for-
  byte unchanged (same variable names, same per-lane defaults); test-only, no
  driver or public API change.

- **Discoverable connect/handshake trace (bead `vdr0`).** The connect path's
  packet-level trace (steps + hex dumps to stderr, python-oracledb
  `PYO_DEBUG_PACKETS` parity) is now documented under README →
  *Troubleshooting → Capturing a connect/handshake trace*: enable it with
  `ORACLEDB_TRACE_CONNECT=1` (and `ORACLEDB_TRACE_QUERY=1` for statement
  bytes), what a healthy handshake vs. a `RESEND` vs. a missing/failed
  fast-auth exchange looks like, and how to diff a working against a failing
  capture. The trace is gated on that env var and is deliberately **not**
  controlled by `RUST_LOG` — a field-triage session running `RUST_LOG=trace`
  saw zero protocol detail. Three additive milestones were added so the trace
  reads end to end: the negotiated `ACCEPT` capabilities line (`fast_auth=…`,
  the fork point between fast and classic auth), a `REFUSE received` step, and
  a final `session established sid=… serial=…`. The secret-exclusion invariant
  (passwords are O5LOGON-encrypted before tracing; the fast-auth access-token
  payload is never dumped) is now pinned by a live regression test
  (`tests/connect_trace_secret.rs`, `#[ignore]`) and a deterministic CI source
  lint (`scripts/check_trace_secret_exclusion.sh`). Docs / test / lint only —
  no public API change.

- Version-portable live-test fixtures: `live_object_decode` resolves the
  fixture owner from the connecting session's own schema (portable across the
  matrix lanes) rather than a hard-coded owner, and `pipeline_live`
  version-gates with an explicit, evidence-based reason (pipelining requires
  the 23ai `END_OF_RESPONSE` capability; documented, never a silent skip).

- **HA / multi-address connect-string support (bead `clvm`).** A
  `DESCRIPTION` with an `ADDRESS_LIST` / multiple `ADDRESS` entries now fails
  over: each address is tried in order (honoring `LOAD_BALANCE` shuffle,
  `FAILOVER=OFF`, `RETRY_COUNT` / `RETRY_DELAY`) until one dials; only transport
  errors fail over (config/auth aborts immediately), and an all-fail is
  aggregated into the new `Error::AllAddressesFailed`. DSN transport parameters
  that were previously parsed and silently dropped are now applied: the
  `CONNECT_TIMEOUT` connect deadline (added as an alias of
  `transport_connect_timeout`), the DSN `(SDU=)` value (resolved: explicit
  builder wins, else DSN, else 8192), and DSN wallet / `USE_SNI` settings.
  `use_sni=true` that cannot be honored now fails closed with
  `Error::UnsupportedSni` instead of silently degrading to no-SNI.
- **Listener REDIRECT handling.** A CONNECT answered with a REDIRECT packet
  (shared-server / RAC configurations, routine on many listeners) now follows
  the redirect: the driver reconnects the transport to the redirected address
  and resends CONNECT with the `TNS_PACKET_FLAG_REDIRECT` flag, bounded against
  redirect loops. New public constant `TNS_PACKET_FLAG_REDIRECT`; new errors
  `Error::InvalidRedirectData` and `Error::ConnectRedirectLoop`. A redirect that
  demands a transport-protocol downgrade (e.g. `tcps` → `tcp`) is refused.

### Fixed

- **Statement cache no longer reuses stale bind metadata when a rebind changes
  the bind type** — a rebind that changed a parameter's type against a cached
  statement previously surfaced `ORA-01722`. The cache now re-describes when
  bind types diverge from the cached shape. Live-verified on Oracle XE 18 and
  FREE 23ai.
- **Diagnostics: every remaining site that mislabeled a network-layer TNS
  packet-type byte as a TTC message type** (the flag-framed boundary reader and
  the pipeline decoder sites) now reports `Error::UnexpectedPacket` naming the
  packet type, not `unknown TTC message type … at position 4`.

## [0.7.1] - 2026-07-04

### Added

- **Encrypted `ewallet.pem` private keys decrypt with `wallet_password`**
  (bead `rust-oracledb-encrypted-pem-p12-wallets-1u8f`, GH #6 follow-up): a
  PKCS#8 `ENCRYPTED PRIVATE KEY` block (PBES2 / PBKDF2-HMAC-SHA1/SHA256 /
  AES-128/192/256-CBC — the scheme ADB wallet downloads and
  `openssl pkcs8 -topk8` emit) is now decrypted when a wallet password is
  supplied. RustCrypto only; `#![forbid(unsafe_code)]` holds. Fail-closed
  typed errors: `WalletError::PasswordRequired` (no password),
  `WalletError::KeyDecrypt` (wrong password / unsupported scheme, incl.
  scrypt and legacy `Proc-Type: 4,ENCRYPTED` PEM encryption) — never a
  silent verify-only downgrade.
- **Standalone `ewallet.p12` wallets** are a first-class format:
  `tls::wallet::parse_ewallet_p12` / `read_ewallet_p12` /
  `p12_wallet_path` / `P12_WALLET_FILE_NAME` reuse the internal PKCS#12
  (PFX) reader. Requires `wallet_password` (typed `PasswordRequired`
  otherwise); legacy 3DES/RC2 wallets fail closed with a typed
  `WalletError::Pkcs12` naming the unsupported OID. An untouched ADB wallet
  zip (`cwallet.sso` + `ewallet.p12`) now connects directly.
- **`cwallet.sso` promoted from `--features experimental` to always-on**
  after the reader was verified against a REAL `orapki` 23.26-generated
  wallet fixture (`crates/oracledb/tests/fixtures/tls/cwallet_orapki.sso`)
  whose extracted certs/keys are byte-identical to its paired
  `ewallet.p12`. The `experimental` cargo feature remains as an empty no-op.
  Unsupported outer sub-types (`5`, `0x35`) still fail closed with typed
  errors.
- New `WalletError` variants (additive, `#[non_exhaustive]`): `Pkcs12`,
  `KeyDecrypt`, `PasswordRequired`. All wallet diagnostics keep redacting
  wallet paths and never echo passwords.
- Wallet loader precedence (`load_wallet`): `ewallet.pem` (reference
  parity) → `ewallet.p12` with password → `cwallet.sso` → passwordless
  `ewallet.p12` fails closed with `PasswordRequired`.
- Lab-only synthetic wallet fixtures: a genuine `oraclepki` 23.26 wallet
  (`ewallet_orapki.p12` + `cwallet_orapki.sso`) and openssl-generated
  encrypted-PEM variants (SHA-256/SHA-1 PRFs, scrypt and legacy negatives);
  generation commands documented in `docs/TLS_SETUP.md` §5. Honesty note:
  offline parsing/decryption is proven; live ADB acceptance still pending
  (see `docs/SUPPORT.md`).

## [0.7.0] - 2026-07-04

### Added

- **Below-floor server refusal (Oracle 11g and older).** The ACCEPT parser now
  enforces the reference's protocol floor (`TNS_VERSION_MIN_ACCEPTED` = 315,
  the 12.1 wire format; python-oracledb `ERR_SERVER_VERSION_NOT_SUPPORTED`,
  DPY-3010): a server negotiating below it — Oracle 11g answers with 314 — is
  refused with a structured, self-explanatory error naming both the offered
  version and the floor. Previously the 11g ACCEPT (an older, shorter payload
  layout) surfaced a misleading `truncated TTC payload` decode error.
- **Standing multi-version live matrix as a release gate**
  (`scripts/version_matrix.sh`): new `full` subcommand runs a deep
  value-asserting suite (`examples/matrix_full.rs`) per lane — session
  identity, 600-row multi-packet fetch verified value-by-value, wide rows
  above one SDU, bind DML with `rows_affected` checks, rollback semantics +
  cross-connection commit visibility, CLOB/BLOB write+readback above one
  chunk, describe/metadata (names, types, precision/scale), NULL handling,
  NUMBER/VARCHAR2/DATE/TIMESTAMP round-trips, and deliberate error paths (bad
  SQL, unknown table, wrong password → clean ORA-01017). New `xe11` lane
  (gvenzl/oracle-xe:11-slim, port 1511) asserts the structured below-floor
  refusal. `scripts/release_matrix_gate.sh` records a per-SHA verdict file,
  and `scripts/release_preflight.sh` now REFUSES any release tag without a
  committed all-green matrix artifact for the exact release SHA.
  `.github/workflows/version-matrix.yml` runs the full matrix (gvenzl service
  containers, one runner per lane) on pushes to `main` touching `crates/**`
  and nightly.
- Sans-I/O golden regression tests for the pre-23ai handshake
  (`oracledb-protocol/tests/pre23ai_handshake_golden.rs`), from lab captures
  only: the 8-byte RESEND packet, below-floor 11g ACCEPT (structured refusal,
  refusal-precedes-decode), XE 18 classic ACCEPT (no fast-auth flags), FREE
  23ai contrast ACCEPT, and classic protocol-negotiation / data-types / auth
  phase one + two responses proven complete ONLY at their terminal message —
  the terminate-without-END_OF_RESPONSE loop contract, including the
  incomplete-prefix "read more packets" behavior.

### Fixed

- **Classic (pre-23ai) connect-phase reads now handle break MARKERs.** A
  pre-23ai server answers a failed classic login (wrong password) with a
  MARKER packet before the ERROR response; the connect-phase reader now runs
  the same marker/reset dance as the post-connect readers, so the real
  ORA-01017 is surfaced instead of `unexpected TNS packet type 12 (MARKER)`.

### Changed

- `ProtocolError::UnsupportedVersion` now carries `{ version, minimum }` (was
  `{ version }`) and its message names the floor and the reference error it
  mirrors; `TnsVersion::negotiate` refuses below 315 (the reference's accepted
  floor) instead of 300 (the advertised CONNECT minimum).

## [0.6.0] - 2026-07-04

Minor release: the thin driver now connects to and queries **pre-23ai Oracle
servers** (18c/19c/21c generation). Previously every pre-23ai connect failed
during the TNS handshake; a field test against a 19c fleet surfaced the gap,
and the fixes were live-verified against Oracle XE 18, XE 21, and FREE 23ai
(`scripts/version_matrix.sh`). Breaking protocol-crate builder signature
changes force the minor bump per ADR-0002; the 23ai wire behavior is
byte-identical (goldens and cassettes unchanged).

### Added

- Pre-23ai session establishment: the connect path now handles the TNS
  `RESEND` packet (resending CONNECT plus the split connect-data packet for
  descriptors above `TNS_MAX_CONNECT_DATA`), and runs the classic
  protocol-negotiation / data-types / two-phase-auth handshake when the
  server does not advertise fast authentication. New protocol builders:
  `build_protocol_negotiation_payload`, `build_data_types_payload`,
  `build_auth_phase_one_payload`, `connect_data_fits_inline`, and the
  classic completion checker `classic_connect_response_is_complete`.
- Classic response framing: servers that never negotiated `END_OF_RESPONSE`
  framing (protocol version below 319) complete responses at their terminal
  TTC message. Execute, fetch (owned and borrowed), commit/rollback/ping/
  logoff, LOB operations, scroll, and change-password now finish on such
  servers instead of hanging until the call timeout.
- `TtcWriter::write_function_header` / `write_piggyback_header`: function
  and piggyback headers with the ub8 pipeline-token written only when the
  negotiated ttc field version is at least 23.1 ext 1.
- Errors: `Error::UnexpectedPacket` (names the TNS packet type, replacing a
  misleading "unknown TTC message type ... at position 4" report for
  packet-layer bytes) and `Error::ConnectResendLoop`.
- `TNS_PACKET_TYPE_RESEND`, `TNS_MSG_TYPE_FAST_AUTH`, and
  `TNS_MAX_CONNECT_DATA` protocol constants.

### Fixed

- Fetch continuation across pages: `Rows` now tracks the previous page's
  last row for bit-vector duplicate-column decompression. Previously
  `Rows::collect()` drained the batch before the next fetch, so a page whose
  first row was duplicate-compressed against the prior page failed to decode
  (also affected 23ai).
- Field-version negotiation takes the minimum of the server-reported and
  client-supported ttc field versions (was: maximum), matching the reference
  `capabilities.pyx` and preventing 23ai-era field formats from being used
  against older servers.

### Changed

- **Breaking (oracledb-protocol, re-exported via `oracledb::protocol`):**
  execute/fetch/LOB/AQ/subscription payload builders,
  `build_function_payload*`, `build_auth_phase_two_payload_with_proxy_with_seq`,
  and `build_change_password_payload_with_seq` take the negotiated
  `ttc_field_version` so the ub8 token is version-gated. Pass
  `ClientCapabilities::default().ttc_field_version` to keep the previous
  23ai-era bytes.
- Pipelining fails closed with a structured unsupported-feature error on
  connections without `END_OF_RESPONSE` framing (pre-23ai servers) instead
  of hanging on the first boundary read.
- Token (IAM/OAuth) authentication explicitly requires a fast-auth-capable
  (23ai-generation) server; password auth uses the classic flow on older
  servers.

## [0.5.1] - 2026-06-29

Patch release focused on downstream capability honesty for `oraclemcp` doctor
checks and release-documentation truth. No breaking changes.

### Added

- Added a typed authentication capability surface: `AuthMode`,
  `AuthModeKind`, `AuthModeSupport`, `AuthCapabilities`, and
  `Error::UnsupportedAuthMode`. Password, proxy, and IAM/OAuth token auth remain
  the supported thin modes; external/passwordless, Kerberos, and RADIUS/native
  MFA are now expressible and fail before network I/O with a machine-classifiable
  unsupported-mode error instead of requiring dummy credentials.
- Added passwordless/unsupported auth constructors and builders on
  `ConnectOptions`: `external_auth`, `kerberos_auth`, `radius_auth`,
  `with_external_auth`, `with_kerberos_auth`, and `with_radius_auth`.
- Added a typed wallet-format diagnostic for unsupported standalone
  `ewallet.p12` wallets (`WalletError::UnsupportedFormat`), leaving encrypted
  PEM and p12 backend implementation deferred.
- Added offset-preserving `TIMESTAMP WITH TIME ZONE` surfaces:
  `QueryValue::TimestampTz`, `BindValue::TimestampTz`, and chrono
  `DateTime<FixedOffset>` / `DateTime<Utc>` `FromSql` and `ToSql`
  conversions.

### Fixed

- IAM/OAuth token connections now preserve TCPS in the listener/auth connect
  descriptors, forward non-wallet `SECURITY` pass-through fields, and inject
  `(TOKEN_AUTH=OCI_TOKEN)` for token-auth listeners.
- `transport_connect_timeout` / `connect_timeout` in DSN descriptors now bound
  the full connect handshake, including post-dial listener ACCEPT and AUTH
  reads, instead of only relying on a fixed TCP dial timeout.
- Fixed-offset `TIMESTAMP WITH TIME ZONE` fetch and bind no longer drop the
  numeric offset. Legacy `NaiveDateTime` conversion remains available, while
  offset-aware chrono types preserve the zone offset.
- Redacted wallet paths, wallet passwords, server certificate DN material,
  Kerberos principals/keytabs, RADIUS challenge hints, and access tokens from
  formatted debug/error surfaces covered by the new release scope.
- Updated public documentation to match current release evidence: 20 fuzz
  targets and the current python-oracledb thin differential count
  (2578 collected, 2462 passed, 116 skipped, 0 regressions).
- Cleaned rustdoc/public-api warning sites so release public-API snapshots do
  not emit avoidable broken/private/redundant link warnings.

## [0.5.0] - 2026-06-23

Brings the workspace to the intended 1.x public-API contract and ships it as a
published `0.x` release for real-world validation ahead of `1.0`. The pre-1.0
deprecation shims are removed and the remaining accidental internals are made
crate-private, so the surface is exactly the intended 1.0 contract. This is the
code that was internally frozen and exhaustively qualified as `1.0.0-rc.1`
(release-qualification suite green; python-oracledb thin differential 2578 = 2578,
0 regressions) plus three further driver-correctness fixes found afterward
(below); it is released as `0.5.0` rather than `1.0.0` so the 1.0 stability
promise is made only after downstream production validation. See
[`docs/MIGRATING-0.3.md`](docs/MIGRATING-0.3.md) for the upgrade path.

### Removed (BREAKING)

- The pre-1.0 `#[deprecated(since = "0.3.0")]` query/execute shims are removed
  (each existed on both `Connection` and `BlockingConnection`): `execute_query`,
  `execute_query_collect`, `execute_query_with_timeout`, `execute_query_with_binds`,
  `execute_query_with_binds_and_timeout`, `query_named`, `query_named_with_timeout`,
  `execute_query_with_bind_rows`, `execute_query_with_bind_rows_and_options`,
  `execute_query_with_bind_rows_and_timeout`,
  `execute_query_with_bind_rows_options_and_timeout`, and
  `execute_query_for_registration`. Use the operation-family API instead —
  `query` / `query_with` (rows), `execute` / `execute_with` (DML/PLSQL),
  `execute_many` / `execute_many_with` (array DML), `register_query` (CQN), with
  `Query::timeout` / `Execute::timeout` / `Batch::timeout` for deadlines and
  `params!{}` for named binds — or `execute_raw` for the byte-identical raw
  `QueryResult`. Every removed name and its replacement is documented in
  `docs/MIGRATING-0.3.md`.
- Accidental public internals are now crate-private (never part of the intended
  API): the SODA query-by-example SQL helpers (`soda::qbe`), the driver-side TLS
  handoff type (`tls::TlsParams`), the direct-path encoder buffer
  (`DirectPathPieceBuffer`), and the raw `DirectPathStream` fields
  (`pieces` / `total_piece_length`). The frozen 1.x public surface is now
  exactly the API ledger's `keep` set.

### Changed (BREAKING)

- **`Rows::into_typed` (async) now takes `&Cx` and is `async`**, and drains the
  full result set before typing. Previously it typed only the first fetch batch
  and silently discarded every later batch on a multi-batch result (data loss).
  Call it as `rows.into_typed::<T>(&cx).await`. The blocking
  `BlockingRows::into_typed` is unchanged (it already collected all batches).

### Fixed

- **Borrowed LOB-prefetch request/response pairing** (`for_each_row_ref` over
  CLOB/NCLOB/BLOB): the borrowed paging loop registered the cursor for LOB
  prefetch but then sent a plain FETCH while parsing the response as a
  define-fetch (or vice versa), so a multi-batch borrowed fetch over a LOB column
  could drop the `size` / `chunk_size` fields and desynchronize. The request and
  the response decode are now paired on the same per-cursor LOB-prefetch state
  (bead rust-oracledb-bur7).
- **CLOB/BLOB refetched as LONG/LONG RAW** (`unknown TTC message type 129`): a
  cached query cursor re-executed after a column's type changed from
  CHAR/VARCHAR/RAW to CLOB/BLOB is streamed by the server in LONG/LONG RAW form
  (`adjust_refetch_metadata`), which carries the LONG status trailer
  (null-indicator + return code) after each value. The execute parse path did not
  consume that trailer, so the next message byte was mis-framed and the connection
  desynchronized. The parser now consumes the trailer whenever a column is folded
  to LONG/LONG RAW (bead rust-oracledb-f0ad).
- **Stranded speculative fetch no longer wedges the connection**: issuing a
  low-level `fetch_rows_request` and then abandoning it (without consuming the
  paired response) left the connection in an unrecoverable in-flight state where
  every subsequent operation errored. The next operation now breaks + drains the
  stranded page and reclaims the wire, exactly as it already did for a dropped
  fetch future (bead rust-oracledb-004o).
- **Pool close race**: a `force`-close racing an in-flight connection open
  failure or an unhealthy ping no longer requeues the associated waiter after
  the pool has begun closing. Previously this could leave a closed pool with a
  stale waiter in its queue, blocking clean close finalization. The in-flight
  failure paths now suppress waiter requeue while closing; the close drain owns
  waiter resolution (the awaiting caller is woken with the pool-closed error).
  Found by exhaustive depth-7 model-checking of the pool lifecycle
  (road-to-1.0 W3-E4).
- **`query_one` / `query_opt` cardinality on single-row LONG results**: these no
  longer raise `Error::TooManyRows` for a query that returns exactly one row
  whose column is `LONG` / `LONG RAW`. The per-row LONG define-fetch ignores the
  requested arraysize and returns one row with `more_rows` still set; the
  cardinality check misread that "end not yet confirmed" flag as a second row.
  `query_one` / `query_opt` now fetch ahead (at most one extra round trip, only
  when a single row is in hand with `more_rows` set) to confirm whether a real
  second row follows. Found by the W3-E1.2 live typed round-trip matrix.
- **`execute_many` RETURNING aggregation**: `BatchOutcome::returning().rows_for(bind)`
  now returns one value per affected input row, instead of only the first
  iteration's value. Array DML decodes `RETURNING` once per iteration, so a single
  RETURNING bind arrives as one group per iteration; the curated `BatchOutcome`
  now coalesces groups that share a bind index (single-statement `RETURNING` is
  unaffected — it already arrives as one group per bind). Found by the W3-E7.4
  live e2e suite.
- **`Query::stream_lobs()` over CLOB/NCLOB**: streamed (locator-only) LOB fetches no
  longer fail with `Protocol(TtcDecode("invalid ub8 length"))`. The LOB column decoder
  unconditionally read the `size` (ub8) and `chunk_size` (ub4) fields, but those are
  present only in LOB-prefetch (define-fetch) responses — a plain streamed locator fetch
  omits them, so the decoder misaligned onto the locator's length prefix. The decoder now
  tracks per-cursor LOB-prefetch state and selects the locator-only vs prefetch decode
  shape accordingly (BFILE always uses the locator-only shape). Default LOB
  materialization is unchanged. Found by the W3-E7.4 live e2e suite (rust-oracledb-jbh9).
- **`f32` conversion overflow** (`FromSql for f32`): a finite NUMBER / BINARY_DOUBLE that
  exceeds the `f32` range now returns `ConversionError::OutOfRange` instead of silently
  yielding `inf` (the `f64` path already rejected non-finite). Found by W3-E8.
- **INTERVAL DAY TO SECOND sub-microsecond precision**: interval encoding is now
  nanosecond-native, so a fractional-seconds value with more than 6 significant digits no
  longer truncates on round-trip (notably OSON/JSON `IntervalDS`). `encode_interval_ds`
  became symmetric with the nanosecond-returning decoder. Found by W3-E8.
- **Borrowed-fetch cancel recovery** (`fetch_rows_ref`): a borrowed (zero-copy) fetch
  future dropped mid-read now arms BREAK → drain recovery like the owned fetch path, so the
  next operation on the connection is not desynchronized by a stranded response. Found by
  W3-E8.
- **Borrowed vs owned NUMBER canonicalization**: the borrowed (zero-copy) and owned fetch
  paths now produce identical canonical text for trailing-zero `NUMBER` values. Found by
  W3-E8.
- **DbObject long attribute values**: a DbObject/collection attribute value longer than 252
  bytes is now decoded correctly. The encoder emits the long form as chunked `ub4` segments
  (matching python-oracledb), but the decoder read a single fixed `u32` length, mis-decoding
  such values on fetch; the decoder now consumes the chunked form. Found by W3-E8.
- **Sparse VECTOR validation**: encoding a sparse VECTOR now validates that the index and
  value counts match and that the dimension count fits the `u16` wire field (fail-closed
  instead of silently wrapping at 65 536). Found by W3-E8.
- **AQ dequeue truncation**: a RAW/JSON AQ dequeue whose declared payload-image length
  exceeds the bytes actually present now returns a decode error instead of silently
  returning truncated data. Found by W3-E8.
- **SODA mixed-case columns**: generated SODA SQL now quotes every descriptor column name
  (not only the media-type column), so collections mapped onto case-sensitive mixed-case
  columns work. (SODA is an experimental feature.) Found by W3-E8.
- **Long-bind ordering on STANDARD databases** (data integrity): the threshold that decides
  whether a bind is written in the trailing "long" section now uses the **negotiated**
  `max_string_size` (4000 on a default `MAX_STRING_SIZE=STANDARD` database) instead of a
  hard-coded 32767. Previously, on a STANDARD database a bind between 4001 and 32767 bytes
  was mis-ordered and its value could land in the wrong column. Found by W3-E8.
- **Connection wire-state after a dropped cancellable operation**: `read_lob`, `commit`,
  `rollback`, `ping`, the AQ enqueue/dequeue calls, LOB write/trim/free, direct-path load,
  `change_password`, CQN (un)subscribe, sessionless/TPC transaction control, `scroll_cursor`,
  and pipeline run now break-and-drain a stranded server call before issuing their request —
  matching the fetch/execute paths — so dropping a cancellable query/fetch future no longer
  desynchronizes the next operation on the connection. Found by W3-E8.
- **Pool growth under concurrent waiters**: with a growable pool (`min < max`,
  `increment >= 1`), multiple concurrent `acquire()` calls on an empty pool now all grow the
  pool toward `max` and are served, instead of only the first acquirer being served while the
  rest wait forever. Found by W3-E8.
- **DbObject image value format** (corrects a 0.3.x-unreleased fix): DbObject/collection
  attribute values longer than 245 bytes are encoded and decoded as a single big-endian
  `u32` length (no chunking), matching python-oracledb's `DbObjectPickleBuffer`. An earlier
  unreleased change had matched the wrong (chunked) form on both sides. Found by W3-E8.
- **NULL native BOOLEAN OUT/RETURNING binds**: a PL/SQL `OUT`/`IN OUT`/`RETURNING` native
  `BOOLEAN` that comes back SQL NULL now decodes as NULL instead of raising a spurious
  "truncated OUT bind value" error (the negative actual-length NULL signalling that some
  server versions use is now special-cased, as in python-oracledb). Found by W3-E8.
- **Borrowed (zero-copy) CLOB/BLOB fetch on multi-page results**: the borrowed fetch path
  (`Connection::for_each_row_ref`, Arrow columnar streaming) now selects the LOB decode mode
  from the cursor's LOB-prefetch state, exactly like the owned fetch path, so a multi-page
  query selecting a CLOB/BLOB column no longer desynchronizes (or errors) on the second and
  later pages. This is the borrowed-path counterpart of the earlier `stream_lobs()` CLOB fix.
  Found by W3-E8.
- **`ConnectOptions` Debug no longer leaks secrets** (security): `ConnectOptions`'s `Debug`
  output now redacts `password` and `wallet_password` (the access token was already redacted),
  so logging or formatting an options value with `{:?}` cannot expose credentials. Found by
  W3-E8.
- **UROWID fetch on describe-size-0 columns**: a `UROWID` column whose describe buffer size
  is 0 is no longer wrongly nulled (which also desynced the rest of the row). The
  `buffer_size == 0` short-circuit now exempts `UROWID` in addition to `LONG`/`LONG RAW`, as
  python-oracledb does, on both the owned and borrowed decode paths. Found by W3-E8.
- **NUMBER -> JSON precision**: converting a high-precision `NUMBER` to a `serde_json` value
  no longer silently rounds through `f64`; values that do not fit `f64` losslessly are kept
  as their exact text, honoring the no-loss intent. Small/exact values are unchanged. Found
  by W3-E8.
- **High session serial numbers**: a server `AUTH_SERIAL_NUM` greater than 65535 no longer
  aborts connect; it is read with `ub2` (16-bit) semantics like python-oracledb. Found by
  W3-E8.
- **Pool disposal of returned-dead connections**: a connection returned to the pool while
  already dead is now routed through the backend's close path (`PoolBackend::close_connection`
  runs), instead of being dropped without the backend's lifecycle hook. Found by W3-E8.

### Added

- **Deterministic concurrency model-checking** (road-to-1.0 Wave-3 qualification):
  DPOR / exhaustive-enumeration test harnesses over the wire cancel/timeout
  recovery path (W3-E3: cancel maps to `Error::Cancelled`, timeout to
  `Error::CallTimeout`, exactly one BREAK + one RESET, recovery ends at a clean
  `Ready` boundary) and the async pool lifecycle (W3-E4: no missed wakeup, FIFO
  fairness, no double-hand-out, force-close drains all waiters). Test-only; no
  public API change.

### Performance

- Borrowed `for_each_row_ref` paging snapshots only the last row of each page as
  the next page's duplicate-column seed, instead of materializing every row to
  owned values — preserving the zero-copy fast path across page boundaries.
- Removed a per-cell allocation on the text-bind encode path and a per-page deep
  clone of column metadata.

### Internal

- The driver crate was de-monolithized (`lib.rs` and the arrow/pool modules split
  into focused submodules: recovery state machine, connect-string parser, pool
  acquire/engine, arrow builders/schema/direct-path, request builders, row
  facades). API-preserving: the public surface is byte-identical to the API
  ledger's frozen `keep` set (verified by the cargo-public-api baseline).
- `asupersync` is pinned to `=0.3.4`; GitHub Actions are SHA-pinned and emit build
  provenance attestations.

## [0.3.0] — 2026-06-21

The migration release: it ships the permanent 1.0 query/execute API (the four
operation families) and deprecates the 0.2.x execute/query names, giving
downstream code one minor release to move before the names are removed ahead of
`1.0.0-rc.1`.

See [`docs/MIGRATING-0.3.md`](docs/MIGRATING-0.3.md) for a method-by-method
old → new map with before/after snippets.

### Added

- **Four operation families** as the permanent 1.0 contract, on both
  `Connection` (async) and `BlockingConnection` (blocking):
  - `query` / `query_with` returning a lazy `Rows` (`BlockingRows`) facade, plus
    the cardinality helpers `query_one`, `query_opt`, and `query_all`.
  - `execute` / `execute_with` returning a structured `ExecuteOutcome`
    (`rows_affected`, `last_rowid`, OUT/IN-OUT binds, RETURNING, implicit result
    sets, compilation warning).
  - `execute_many` / `execute_many_with` returning a `BatchOutcome`
    (`rows_affected`, per-row counts, collected batch errors, RETURNING).
  - `register_query` (CQN) returning a `RegistrationOutcome`.
- **Builders**: `Query`, `Execute`, `Batch`, and `Registration`, with
  `bind`, `timeout`, `prefetch`/`arraysize`, `stream_lobs`, `scrollable`,
  `parse_only`, `collect_errors`, `row_counts`, and `raw_options` as applicable.
- **Structured error classification** on `Error`: `kind() -> ErrorKind`,
  `ora_code()` / `oracle_code()`, `is_connection_lost()`, `is_transient()`,
  `retry_hint() -> RetryHint`, `is_retryable()`, and `resource_limit()`.
- **`execute_raw`** on `Connection` and `BlockingConnection`: a low-level raw
  execute primitive returning the unprojected `QueryResult`, the execute-side
  counterpart to the retained `fetch_rows*` / `define_and_fetch_rows_with_columns`
  / `scroll_cursor` / `fetch_cursor` primitives. For statement-type-agnostic
  dispatch, parse-only describe, or per-bind-row OUT/RETURNING aggregation; the
  four families remain the ergonomic surface for ordinary code.

### Changed

- **Single operation deadline for timeouts.** The new `timeout(Duration)`
  builders translate the duration **once** into a single absolute deadline that
  spans the initial call and every `Rows::next_batch` / `Rows::collect`
  continuation and LOB chunk of the one logical operation, instead of re-arming a
  per-round-trip `timeout_ms`. An N-batch fetch is now bounded by the budget you
  set rather than up to N× it. On expiry the driver still performs
  BREAK → drain → `Error::CallTimeout` and leaves the session `Ready`.
- Several error and value enums (e.g. `ErrorKind`, `BindValue`, `QueryValue`)
  are `#[non_exhaustive]`; match them with a wildcard arm.

### Deprecated

All of the following are `#[deprecated(since = "0.3.0")]` on **both**
`Connection` and `BlockingConnection`, and are scheduled for removal before
`1.0.0-rc.1` (road-to-1.0 W4-T1). Each delegates to the same private operation
core as its replacement, so behavior is unchanged in 0.3.0.

- `execute_query` → `query` / `query_with` (rows) or `execute` / `execute_with`
  (DML/DDL/PL/SQL).
- `execute_query_collect` → `query` / `query_with` (LOB/JSON/vector cells are
  materialized by default; opt out with `Query::stream_lobs()`).
- `execute_query_with_timeout` → `Query::timeout` / `Execute::timeout`.
- `execute_query_with_binds` → `query` / `execute` with a `Params` argument.
- `execute_query_with_binds_and_timeout` → `Query`/`Execute` `bind(..).timeout(..)`.
- `query_named` → `query(cx, sql, params!{ ... })`.
- `query_named_with_timeout` → `Query::new(sql).bind(params!{ ... }).timeout(..)`.
- `execute_query_with_bind_rows` → `execute_many` / `Batch::new`.
- `execute_query_with_bind_rows_and_options` → `Batch::raw_options` (or
  `Execute::raw_options` / `Query` builders, per family).
- `execute_query_with_bind_rows_and_timeout` → `Batch::timeout` (or
  `Query::timeout`).
- `execute_query_with_bind_rows_options_and_timeout` →
  `Batch::raw_options(..).timeout(..)` (or `Execute::raw_options(..).timeout(..)`).
- `execute_query_for_registration` → `register_query` with
  `Registration::new(sql, registration_id)`.

The low-level fetch/paging primitives (`fetch_rows*`,
`define_and_fetch_rows_with_columns`, `scroll_cursor`, `fetch_cursor`, …) and the
LOB/AQ/objects/transactions/pooling/pipeline/SODA/Arrow/direct-path/CQN surfaces
are **retained** — only the execute/query sprawl is consolidated. See
[`docs/API_DESIGN.md` §8](docs/API_DESIGN.md) for the full "nothing lost" map.

### Fixed

Closed all 103 differential-conformance gaps against python-oracledb's own
thin-mode suite — the full suite now diffs to **0 regressions** vs the live
python-oracledb baseline (2578/2578). Three root causes, all pre-existing on
`main` and surfaced by the first clean full conformance run:

- **Bind-shape validation** (66 tests): the raw `execute` path no longer
  enforces SQL placeholder *occurrence* count (it cannot know whether binds were
  supplied by name or by position). A repeated named bind (`:v` used N times) is
  satisfied by a single value — matching python-oracledb — and `parse()` (which
  supplies no binds) is no longer rejected. Positional-count validation is kept
  in the style-aware `Params::Positional` path, and the ragged-batch-row check is
  preserved.
- **Direct path load** (36 tests): the default `batch_size` sentinel
  (`2**32 - 1`, "all rows in one batch") is no longer misread as a row count and
  no longer trips the protocol `max_batch_rows` limit. `batch_size` is a chunking
  upper bound (clamped to the data length), exactly as in python-oracledb.
- **Pool timed-wait acquire** (1 test): a `POOL_GETMODE_TIMEDWAIT` acquire now
  reliably honors its `wait_timeout` and raises `DPY-4005`, via an explicit
  deadline that does not depend on the async runtime's timer wheel; and pool
  teardown no longer risks a deadlock when a finalizer drops the pool while
  holding the embedder's VM lock (e.g. the Python GIL).
