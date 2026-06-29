# Ground Truth — rust-oracledb

> **CURRENT STATUS (release qualification): SUITE GREEN.** The python-oracledb thin
> differential currently records 2578 collected tests: 2462 passed, 116 skipped,
> 0 regressions / missing tests against Oracle 23ai Free. Adversarially verified
> real (no fake parity) — see `docs/FAKE_PARITY_AUDIT.md`,
> `docs/PARITY_SKIPS.md`, and `docs/qualification/1.0.0-rc.1/SUMMARY.md`.
> Forward plan in `docs/ROADMAP.md` (Waves 4-6: structural, TLS/wallet, gauntlet).
> Journey this session: ~38% -> 91% (Wave 2) -> 100% of baseline (Wave 3).
> `thin.rs` de-monolithized into `thin/` (12 modules). Below is the historical detail.

---

Recorded 2026-06-11 (~18:00Z) at HEAD `978491a` ("Fix async executemany parity gaps"), branch
`master`. Every claim cites its evidence source or is marked UNVERIFIED. Where evidence batches
disagree, the newer per-module JSON wins. Update this file whenever the recorded state changes
materially (re-baseline completion, wave merges, milestone gates).

## 1. Identity and goal

Pure-Rust clean-room port of python-oracledb v4.0.1 **thin mode** (no OCI, no Instant Client).
Definition of done: the filtered 72-module reference test suite **matches-or-beats** the recorded
python-oracledb baseline manifest on the same local Oracle Free container. Authoritative contract:
[PLAN_TO_PORT_PYTHON_ORACLEDB_THIN_TO_RUST.md](../PLAN_TO_PORT_PYTHON_ORACLEDB_THIN_TO_RUST.md)
(milestones M0-M6, claim contract, fake-parity guard); intent draft in `plan.md`.

## 2. Headline status

| Metric | Value | Evidence |
|---|---|---|
| Baseline (real python-oracledb v4.0.1 thin) | 2,236 passed / 24 skipped / 2,260 total | `harness/.baseline/baseline.json` (2026-06-10) |
| Rust shim, freshest-evidence composite (per-module best, mixed vintage) | 1,026 passed / 916 failed / 291 skipped across 71 of 72 modules (2,233 tests) | computed from per-module JSONs listed in §5 |
| Pass rate of executed tests (composite) | 1,026 / 1,942 = **52.8%** | same; stale modules undercount HEAD (see below) |
| Pass rate on modules with at-HEAD evidence only (17:29Z run + focused runs) | 472 / 631 = **74.8%** | `parts-rust-20260611T172927Z-3160370/` + focused JSONs |
| Modules fully green (with passing tests) | **24 of 72** | §5 table |
| Modules zero-fail but all-skip (false greens, see debt item 2) | 13 | §5 table |
| Modules red | 34 | §5 table |
| Modules UNVERIFIED | 0 — `test_4100`'s suspected hang was re-checked on `branch fix-hangs` and **does not reproduce** (27p/4.29s live; §5) | live run |

Milestones: **M0 done** (bead `yoq` closed; baseline manifest recorded). **M1 done** including the
identity-masquerade gate (bead `hq6` closed; test_1100 56 pass / 6 skip at HEAD,
`parts-rust-20260611T172927Z-3160370/001-test_1100_connection.json`; baseline is 57/5 — the
1-test delta is the DRCP-gated skip family, UNVERIFIED which exact test). **M2 in progress**
(bead `grk`). M3 (TLS), M4 (objects/JSON/vector/pool breadth), M5 (Arrow/DPL), M6 (gauntlet)
not started as milestones, though M4-scope clusters have briefs ready (§7).

Skips inflation: 291 shim-run skips vs baseline's 24. Roughly 267 are *false skips* from one bug —
the shim hardcodes `server_version = (0,0,0,0,0)`, so conftest believes the DB lacks
vector/JSON/boolean/sessionless/12.2 support. Fixing it converts those into honest runs (most will
then fail until their codecs exist). Debt item 2.

A re-baseline of red modules from HEAD was **in flight** at the 2026-06-11 recording
(`harness/.results/parts-rust-20260611T172927Z-3160370/`, started 17:29Z; 17 module JSONs written,
then stalled >25 minutes inside `test_4100_cursor_callproc` with ~0 CPU). That stall is **resolved**:
on `branch fix-hangs` HEAD (2026-06-14) `test_4100` runs cleanly (27p/4.29s, §5) — the historical
hang no longer reproduces, so it was not a permanent callproc bug but earlier-wave drain/reset
fallout that intervening fixes cleared.

**DML-RETURNING error hang (bead `zhm`, fixed on `branch fix-hangs`, 2026-06-14).** `test_1600`
`test_1612` (an INSERT…RETURNING that trips ORA-12899 "value too large for column") deadlocked the
client for 180 s while the DB session sat idle on "SQL*Net message from client". Root cause, proven
by a live `strace` of the socket plus a `faulthandler` dump showing the freeze *inside*
`cursor.execute()` (not at teardown): on the RETURNING error path the server reports the error
out-of-band via a BREAK marker; the client runs the RESET dance correctly, then the server sends a
`FLUSH_OUT_BINDS` *request* — a DATA packet with data-flags `0x0000` (no END_OF_RESPONSE flag,
because the break-recovery path is not request-boundary framed) whose payload ends in the
FLUSH_OUT_BINDS message byte `0x13`. The reference replies with a FLUSH_OUT_BINDS message and then
reads the real ORA-12899 packet. Our `read_data_response_boundary` fed that post-reset packet back
through `data_packet_ends_response`, which (correctly, for the bead-`n2s` wide-row guard) returns
false for a flagless packet that merely *ends* in `0x13` — so the loop read another packet the
server would never send (it was awaiting our FLUSH_OUT_BINDS reply) and blocked in epoll forever.
Fix (`crates/oracledb/src/lib.rs`): once the boundary loop has run a RESET, treat a packet whose
payload ends in `FLUSH_OUT_BINDS`/`END_OF_RESPONSE` as the response boundary (matching the
reference's message-byte framing for post-reset packets), gated on the post-reset context so the
wide-row guard is untouched. After-the-fix: `test_1612` passes in 0.33 s; full `test_1600` 27p/4.69s
= reference. Hermetic regression test
`dml_returning_error_flush_out_binds_after_reset_completes_without_hang` replays the exact wire
sequence and was confirmed to hang→time-out before the fix and pass after.

## 3. Architecture as-built

Cargo workspace, three crates. Workspace lints: `unsafe_code = "forbid"`; clippy
`todo`/`dbg_macro`/`unwrap_used` = deny (root `Cargo.toml`).

| Crate | LOC (src, HEAD) | Role |
|---|---|---|
| `crates/oracledb-protocol` | 6,360 total: `thin.rs` 4,559; `sql.rs` 727; `wire.rs` 474; `crypto.rs` 214; `lib.rs` 142; `packet/mod.rs` 131; `net/mod.rs` 71; `capabilities.rs` 42 | sans-I/O TNS/TTC core: packet framing, auth crypto (11g SHA1 + 12c PBKDF2 verifiers), TTC message encode/decode, SQL parser, EZConnect parsing. No I/O deps (RustCrypto, serde, thiserror only) |
| `crates/oracledb` | 1,640 (single `lib.rs`) | async driver: `Connection` with `&Cx`-first `pub async fn` API (connect/ping/commit/rollback/execute/fetch/LOB ops/cancel/close) on **asupersync 0.3.4** (no tokio), plus a `BlockingConnection` facade (lib.rs:942-1253) |
| `crates/oracledb-pyshim` | 9,512 (single `lib.rs`) | PyO3 0.28 cdylib (abi3-py310, experimental-async) masquerading as `oracledb.thin_impl`: `ThinConnImpl`/`AsyncThinConnImpl`, cursors, vars, LOBs, DbObjects, pool stubs |

Async-native vs facade: async connect/close/execute/fetch and async LOB ops run on driver futures
(commits `ea63707`, `48b9b87`, `978491a`); `BlockingConnection` still backs the sync half and
residual async paths (beads `nto`/`tmr` in_progress). Verified absent from every Cargo.toml:
**any TLS stack** (rustls; M3 not started) and **arrow-rs** (M5 not started; the `arrow` feature
on the `oracledb` crate is an empty flag).

Monolith warning: `pyshim/src/lib.rs` and `protocol/src/thin.rs` are single-file contention bombs;
the Wave 0 isomorphic split (§7, `split_plan` brief) is the enabling refactor for parallel lanes.

## 4. Harness mechanics

The harness runs the **unmodified reference Python package** (public `connection.py`, `cursor.py`,
genuine compiled Cython `base_impl`) and swaps only `sys.modules["oracledb.thin_impl"]` for the
Rust shim via the pytest plugin in `harness/shim_inject/`.

- Reference pin: `reference/python-oracledb` at tag v4.0.1
  (`3daef052904e41668bb862e6fa40f43c22a81beb`), gitignored; re-pin via `scripts/pin-reference.sh`.
- Containers (gvenzl/oracle-free:23-slim): main `rust-oracledb-free` on host port **1522**
  (global verification only); lane containers `rust-oracledb-lane-1523/1524/1525` (ports
  1523-1525, provisioned 2026-06-11 with schemas per epic-bead decision D3) and
  `rust-oracledb-lane-1526` (added 2026-06-11 ~17:53Z for the M5 lane). Manage via
  `scripts/container.sh up|health|env|stop`; override with `ORACLEDB_CONTAINER_NAME`,
  `ORACLEDB_HOST_PORT`. Lane schema-prep state beyond the D3 note: UNVERIFIED — run
  `scripts/prepare-local-oracle.py` against the lane before first use.
- Venvs: `.venv-py313` (Python 3.13, default for run.sh) and `.venv` (3.14). Per-lane override:
  `ORACLEDB_VENV_DIR`.
- Runner: `harness/run.sh baseline|rust|diff|list`. `rust` = `maturin develop` of the pyshim into
  the selected venv, then pytest with `-p shim_inject`. Default segmented mode runs one pytest
  process per module and writes `harness/.results/parts-rust-<UTC>-<pid>/NNN-<module>.json`,
  merged into `harness/.results/rust.json` at the end; `ORACLEDB_HARNESS_MODE=single` forces one
  process. Other env knobs: `ORACLEDB_REFERENCE_DIR`, `ORACLEDB_FILTER_FILE`,
  `ORACLEDB_BASELINE_DIR`, `ORACLEDB_RESULTS_DIR`, `PYTHON`.
- Scope filter: `harness/filter.txt` excludes 15 module globs (AQ, SODA, CQN, XA/TPC, external
  auth, external-OCI) leaving **72 modules** in scope.
- Env once per shell: `eval "$(scripts/container.sh env)"` (sets `PYO_TEST_*` incl.
  `PYO_TEST_CONNECT_STRING`). Schema prep: `scripts/prepare-local-oracle.py`.
- Fast loop for ONE module:
  ```bash
  cd /home/durakovic/projects/rust-oracledb
  .venv-py313/bin/python -m maturin develop -m crates/oracledb-pyshim/Cargo.toml
  PYTHONPATH=harness .venv-py313/bin/python -m pytest \
    reference/python-oracledb/tests/test_1800_interval_var.py -p shim_inject --tb=short
  ```
- Everything: `harness/run.sh rust`, then `harness/run.sh diff` against the baseline. Do not
  re-run `baseline` unless the container changed. Rust checks:
  `cargo fmt --check && cargo clippy --workspace -- -D warnings && cargo test --workspace`.
- Match-or-beat rule: the objective is the baseline manifest, not "green". Skips are allowed only
  where baseline skips (24 total: 9 in 2400 DRCP, 5 in 1100, 5 in 6500, 2 in 2300, 2 in 6400,
  1 in 2000).
- Guardrail: `scripts/fake_parity_scan.py` before any milestone claim (bead `xvf` wants it
  tightened).

## 5. Per-module status (evidence-backed)

Freshest evidence per module as of 2026-06-11 ~18:00Z. "Fail" includes pytest `error` outcomes.
Baseline column is from `harness/.baseline/baseline.json`. Rows citing the
`parts-rust-20260611T060544Z-1595374` run are **stale** relative to HEAD (that run started 06:05Z,
~5 commits behind; its 027+ files were written 12:36-12:50Z, ~9 commits behind) — red counts there
are upper bounds on HEAD failures. Rows citing `parts-rust-20260611T172927Z-3160370` or the focused
JSONs are at (or within one commit of) HEAD `978491a`. Evidence paths are relative to `harness/`.

| Module | Pass | Fail | Skip | Baseline | Status | Evidence (UTC 2026-06-11) |
|---|---|---|---|---|---|---|
| test_1000_module | 24 | 0 | 0 | 24p/0s | GREEN | `.results/parts-rust-20260611T060544Z-1595374/001-test_1000_module.json` (06:05Z) |
| test_1100_connection | 56 | 0 | 6 | 57p/5s | GREEN | `.results/parts-rust-20260611T172927Z-3160370/001-test_1100_connection.json` (17:29Z) |
| test_1300_cursor_var | 21 | 0 | 0 | 21p/0s | GREEN | `.results/parts-rust-20260611T060544Z-1595374/003-test_1300_cursor_var.json` (06:06Z) |
| test_1400_datetime_var | 19 | 0 | 0 | 19p/0s | GREEN | `.results/parts-rust-20260611T060544Z-1595374/004-test_1400_datetime_var.json` (06:06Z) |
| test_1500_types | 32 | 0 | 0 | 32p/0s | GREEN | `.results/parts-rust-20260611T060544Z-1595374/005-test_1500_types.json` (06:06Z) |
| test_1600_dml_returning | 27 | 0 | 0 | 27p/0s | GREEN (fix-hangs branch, 2026-06-14: test_1612 ORA-12899 DML-RETURNING-error hang FIXED, bead zhm; full module 27p in 4.69s vs reference 27p) | live run, `branch fix-hangs` HEAD; see §"DML-RETURNING error hang" below |
| test_1700_error | 10 | 0 | 0 | 10p/0s | GREEN | `.results/parts-rust-20260611T172927Z-3160370/003-test_1700_error.json` (17:29Z) |
| test_1800_interval_var | 1 | 11 | 0 | 12p/0s | RED | `.results/parts-rust-20260611T172927Z-3160370/004-test_1800_interval_var.json` (17:29Z) |
| test_1900_lob_var | 39 | 0 | 3 | 42p/0s | GREEN (3 false skips, debt item 2) | `.results/parts-rust-20260611T172927Z-3160370/005-test_1900_lob_var.json` (17:30Z) |
| test_2000_long_var | 5 | 0 | 1 | 5p/1s | GREEN | `.results/parts-rust-20260611T172927Z-3160370/006-test_2000_long_var.json` (17:30Z) |
| test_2100_nchar_var | 14 | 4 | 0 | 18p/0s | RED | `.results/parts-rust-20260611T172927Z-3160370/007-test_2100_nchar_var.json` (17:30Z) |
| test_2200_number_var | 31 | 7 | 1 | 39p/0s | RED | `.results/parts-rust-20260611T172927Z-3160370/008-test_2200_number_var.json` (17:30Z) |
| test_2300_object_var | 47 | 0 | 3 | 48p/2s | GREEN | `.results/parts-rust-20260611T172927Z-3160370/009-test_2300_object_var.json` (17:30Z) |
| test_2400_pool | 16 | 31 | 13 | 51p/9s | RED | `.results/parts-rust-20260611T172927Z-3160370/010-test_2400_pool.json` (17:30Z) |
| test_2500_string_var | 28 | 5 | 1 | 34p/0s | RED | `.results/parts-rust-20260611T172927Z-3160370/011-test_2500_string_var.json` (17:30Z) |
| test_2600_timestamp_var | 12 | 0 | 0 | 12p/0s | GREEN | `.results/parts-rust-20260611T060544Z-1595374/016-test_2600_timestamp_var.json` (06:07Z) |
| test_2900_rowid | 8 | 0 | 0 | 8p/0s | GREEN | `.results/parts-rust-20260611T172927Z-3160370/012-test_2900_rowid.json` (17:30Z) |
| test_3100_boolean_var | 0 | 0 | 16 | 16p/0s | ALL-SKIP (false, debt item 2) | `.results/parts-rust-20260611T060544Z-1595374/018-test_3100_boolean_var.json` (06:07Z) |
| test_3200_features_12_1 | 5 | 30 | 0 | 35p/0s | RED | `.results/parts-rust-20260611T172927Z-3160370/013-test_3200_features_12_1.json` (17:30Z) |
| test_3500_json | 0 | 0 | 17 | 17p/0s | ALL-SKIP (false) | `.results/parts-rust-20260611T060544Z-1595374/020-test_3500_json.json` (06:07Z) |
| test_3600_outputtypehandler | 46 | 33 | 0 | 79p/0s | RED | `.results/parts-rust-20260611T172927Z-3160370/014-test_3600_outputtypehandler.json` (17:31Z) |
| test_3700_var | 5 | 27 | 1 | 33p/0s | RED | `.results/parts-rust-20260611T172927Z-3160370/015-test_3700_var.json` (17:32Z) |
| test_3800_typehandler | 2 | 6 | 1 | 9p/0s | RED | `.results/parts-rust-20260611T172927Z-3160370/016-test_3800_typehandler.json` (17:32Z) |
| test_3900_cursor_execute | 37 | 0 | 0 | 37p/0s | GREEN | `.results/parts-rust-20260611T060544Z-1595374/024-test_3900_cursor_execute.json` (06:07Z) |
| test_4000_cursor_executemany | 34 | 4 | 0 | 38p/0s | RED (was 24F+1E at 06:05Z) | `.results/parts-rust-20260611T172927Z-3160370/017-test_4000_cursor_executemany.json` (17:32Z) |
| test_4100_cursor_callproc | 27 | 0 | 0 | 27p/0s | GREEN (fix-hangs branch, 2026-06-14: the historical suspected hang NO LONGER REPRODUCES at this HEAD; full module 27p in 4.29s vs reference 27p — already fixed by prior waves, no fix needed here) | live run, `branch fix-hangs` HEAD; see §"DML-RETURNING error hang" below |
| test_4200_cursor_scrollable | 2 | 11 | 5 | 18p/0s | RED (stale) | `.results/parts-rust-20260611T060544Z-1595374/027-test_4200_cursor_scrollable.json` (12:36Z) |
| test_4300_cursor_other | 47 | 24 | 2 | 73p/0s | RED (stale) | `.results/parts-rust-20260611T060544Z-1595374/028-test_4300_cursor_other.json` (12:36Z) |
| test_4500_connect_params | 84 | 0 | 0 | 84p/0s | GREEN (note: exercises base_impl's Python parser; corpus differential owed at M3) | `.results/parts-rust-20260611T060544Z-1595374/029-test_4500_connect_params.json` (12:36Z) |
| test_4600_type_changes | 9 | 13 | 0 | 22p/0s | RED (confirmed live at HEAD per scalars brief) | `.results/parts-rust-20260611T060544Z-1595374/030-test_4600_type_changes.json` (12:36Z) |
| test_4700_pool_params | 3 | 0 | 0 | 3p/0s | GREEN | `.results/parts-rust-20260611T060544Z-1595374/031-test_4700_pool_params.json` (12:36Z) |
| test_4800_timestamp_ltz_var | 12 | 0 | 0 | 12p/0s | GREEN | `.results/parts-rust-20260611T060544Z-1595374/032-test_4800_timestamp_ltz_var.json` (12:36Z) |
| test_4900_timestamp_tz_var | 12 | 0 | 0 | 12p/0s | GREEN | `.results/parts-rust-20260611T060544Z-1595374/033-test_4900_timestamp_tz_var.json` (12:36Z) |
| test_5100_arrayvar | 1 | 6 | 0 | 7p/0s | RED | `.results/parts-rust-20260611T060544Z-1595374/034-test_5100_arrayvar.json` (12:36Z) |
| test_5200_sql_parser | 19 | 0 | 0 | 19p/0s | GREEN | `.results/parts-rust-20260611T060544Z-1595374/035-test_5200_sql_parser.json` (12:36Z) |
| test_5300_connection_async | 47 | 0 | 1 | 48p/0s | GREEN | `.results/parts-rust-20260611T060544Z-1595374/036-test_5300_connection_async.json` (12:36Z) |
| test_5400_cursor_async | 38 | 0 | 0 | 38p/0s | GREEN (focused run) | `.results/async-5400-verbose.json` (15:06Z) |
| test_5500_pool_async | 12 | 34 | 1 | 47p/0s | RED (stale) | `.results/parts-rust-20260611T060544Z-1595374/038-test_5500_pool_async.json` (12:47Z) |
| test_5600_dbobject_async | 17 | 3 | 0 | 20p/0s | RED (stale; cc9686c may have fixed, re-verify) | `.results/parts-rust-20260611T060544Z-1595374/039-test_5600_dbobject_async.json` (12:47Z) |
| test_5700_lob_var_async | 28 | 0 | 0 | 28p/0s | GREEN (focused run, 1 commit before HEAD) | `.results/async-5700-tmr.json` (15:19Z) |
| test_5800_cursor_var_async | 16 | 0 | 0 | 16p/0s | GREEN | `.results/parts-rust-20260611T060544Z-1595374/041-test_5800_cursor_var_async.json` (12:47Z) |
| test_5900_dml_returning_async | 23 | 0 | 0 | 23p/0s | GREEN | `.results/parts-rust-20260611T060544Z-1595374/042-test_5900_dml_returning_async.json` (12:47Z) |
| test_6000_typehandler_async | 2 | 5 | 1 | 8p/0s | RED (stale) | `.results/parts-rust-20260611T060544Z-1595374/043-test_6000_typehandler_async.json` (12:47Z) |
| test_6100_cursor_executemany_async | 34 | 0 | 0 | 34p/0s | GREEN (focused run at HEAD) | `.results/full-async-executemany-green-candidate.json` (15:57Z) |
| test_6200_cursor_callproc_async | 6 | 5 | 0 | 11p/0s | RED (confirmed live at HEAD per async_tail brief) | `.results/parts-rust-20260611T060544Z-1595374/045-test_6200_cursor_callproc_async.json` (12:47Z) |
| test_6300_cursor_other_async | 38 | 17 | 2 | 57p/0s | RED (stale) | `.results/parts-rust-20260611T060544Z-1595374/046-test_6300_cursor_other_async.json` (12:47Z) |
| test_6400_vector_var | 0 | 0 | 48 | 46p/2s | ALL-SKIP (false) | `.results/parts-rust-20260611T060544Z-1595374/047-test_6400_vector_var.json` (12:47Z) |
| test_6500_vector_interop | 0 | 0 | 5 | 0p/5s | ALL-SKIP (matches baseline: baseline also skips all 5) | `.results/parts-rust-20260611T060544Z-1595374/048-test_6500_vector_interop.json` (12:47Z) |
| test_6600_defaults | 9 | 7 | 0 | 16p/0s | RED (defaults plumbing confirmed live at HEAD) | `.results/parts-rust-20260611T060544Z-1595374/049-test_6600_defaults.json` (12:47Z) |
| test_6700_json_23 | 0 | 0 | 13 | 13p/0s | ALL-SKIP (false) | `.results/parts-rust-20260611T060544Z-1595374/050-test_6700_json_23.json` (12:47Z) |
| test_6800_error_async | 8 | 2 | 0 | 10p/0s | RED (stale) | `.results/parts-rust-20260611T060544Z-1595374/051-test_6800_error_async.json` (12:47Z) |
| test_6900_oson | 0 | 0 | 7 | 7p/0s | ALL-SKIP (false) | `.results/parts-rust-20260611T060544Z-1595374/052-test_6900_oson.json` (12:47Z) |
| test_7000_connection_async_shortcut_methods | 14 | 7 | 0 | 21p/0s | RED (partly confirmed live at HEAD) | `.results/parts-rust-20260611T060544Z-1595374/053-test_7000_connection_async_shortcut_methods.json` (12:48Z) |
| test_7100_interval_ym_var | 1 | 10 | 0 | 11p/0s | RED (confirmed live at HEAD: no interval codecs) | `.results/parts-rust-20260611T060544Z-1595374/054-test_7100_interval_ym_var.json` (12:48Z) |
| test_7200_tnsnames | 24 | 0 | 0 | 24p/0s | GREEN | `.results/parts-rust-20260611T060544Z-1595374/055-test_7200_tnsnames.json` (12:48Z) |
| test_7300_unsupported_features_thin | 1 | 7 | 0 | 8p/0s | RED | `.results/parts-rust-20260611T060544Z-1595374/056-test_7300_unsupported_features_thin.json` (12:48Z) |
| test_7500_binary_vector | 0 | 0 | 3 | 3p/0s | ALL-SKIP (false) | `.results/parts-rust-20260611T060544Z-1595374/057-test_7500_binary_vector.json` (12:48Z) |
| test_7600_pipelining_async | 0 | 49 | 0 | 49p/0s | RED (confirmed live at HEAD: no pipelining anywhere) | `.results/parts-rust-20260611T060544Z-1595374/058-test_7600_pipelining_async.json` (12:48Z) |
| test_7700_sparse_vector | 0 | 0 | 37 | 37p/0s | ALL-SKIP (false) | `.results/parts-rust-20260611T060544Z-1595374/059-test_7700_sparse_vector.json` (12:48Z) |
| test_8000_dataframe | 2 | 68 | 12 | 82p/0s | RED (confirmed live: no Arrow fetch) | `.results/parts-rust-20260611T060544Z-1595374/060-test_8000_dataframe.json` (12:48Z) |
| test_8100_dataframe_async | 0 | 65 | 4 | 69p/0s | RED (same) | `.results/parts-rust-20260611T060544Z-1595374/061-test_8100_dataframe_async.json` (12:48Z) |
| test_8600_cursor_scrollable_async | 0 | 0 | 18 | 18p/0s | ALL-SKIP (false) | `.results/parts-rust-20260611T060544Z-1595374/062-test_8600_cursor_scrollable_async.json` (12:48Z) |
| test_8700_sessionless_transaction | 0 | 0 | 17 | 17p/0s | ALL-SKIP (false) | `.results/parts-rust-20260611T060544Z-1595374/063-test_8700_sessionless_transaction.json` (12:48Z) |
| test_8800_sessionless_transaction_async | 0 | 0 | 17 | 17p/0s | ALL-SKIP (false) | `.results/parts-rust-20260611T060544Z-1595374/064-test_8800_sessionless_transaction_async.json` (12:48Z) |
| test_8900_dataframe_ingestion | 0 | 39 | 3 | 42p/0s | RED (confirmed live: no DataFrame ingestion) | `.results/parts-rust-20260611T060544Z-1595374/065-test_8900_dataframe_ingestion.json` (12:48Z) |
| test_9000_dataframe_ingestion_async | 0 | 39 | 3 | 42p/0s | RED (same) | `.results/parts-rust-20260611T060544Z-1595374/066-test_9000_dataframe_ingestion_async.json` (12:49Z) |
| test_9100_dataframe_vector | 0 | 0 | 14 | 14p/0s | ALL-SKIP (false) | `.results/parts-rust-20260611T060544Z-1595374/067-test_9100_dataframe_vector.json` (12:49Z) |
| test_9200_dataframe_vector_async | 0 | 0 | 14 | 14p/0s | ALL-SKIP (false) | `.results/parts-rust-20260611T060544Z-1595374/068-test_9200_dataframe_vector_async.json` (12:49Z) |
| test_9300_dataframe_requested_schema | 0 | 143 | 0 | 143p/0s | RED (confirmed live) | `.results/parts-rust-20260611T060544Z-1595374/069-test_9300_dataframe_requested_schema.json` (12:49Z) |
| test_9400_dataframe_requested_schema_async | 0 | 143 | 0 | 143p/0s | RED (same) | `.results/parts-rust-20260611T060544Z-1595374/070-test_9400_dataframe_requested_schema_async.json` (12:50Z) |
| test_9600_direct_path_load | 0 | 30 | 0 | 30p/0s | RED (confirmed live: no DPL) | `.results/parts-rust-20260611T060544Z-1595374/071-test_9600_direct_path_load.json` (12:50Z) |
| test_9700_direct_path_load_async | 0 | 30 | 0 | 30p/0s | RED (same) | `.results/parts-rust-20260611T060544Z-1595374/072-test_9700_direct_path_load_async.json` (12:50Z) |

Do NOT cite `harness/.results/rust.json` (7.1 MB, 2026-06-10): it is the M0-era all-red artifact
(180 passed / 1,896 errored at shim placeholders).

## 6. Known debt ledger

1. **Shim-resident driver logic** (beads `d49` in_progress, `p5o` open). The plan's fake-parity
   guard demands "shim contains marshalling ONLY"; today bind/value conversion, SQL rewrite and
   type math live in `pyshim/src/lib.rs` (e.g. `py_value_to_bind` ~line 2280, the whole `binds`
   block lines 846-2247). Migration into protocol/driver is active.
2. **server_version hardcode** — `pyshim/src/lib.rs:5287, 5391, 8914` return `(0,0,0,0,0)`; the
   driver captures only `AUTH_VERSION_STRING` (`crates/oracledb/src/lib.rs:228`). Causes ~267
   false skips (boolean/JSON/vector/OSON/sessionless/scrollable-async/df-vector modules plus
   3 in test_1900). Reference parses `AUTH_VERSION_NO`
   (`impl/thin/messages/auth.pyx:179-197`, two bit layouts keyed on ttc_field_version).
   Smallest highest-leverage fix in the repo; do before trusting any "green" claim on the
   ALL-SKIP modules.
3. **Fake-parity emulations** (full list in `.intake/lob.md` §1/§3; suite does not currently
   catch these): LOB `open/close/is_open` emulated with a client-side bool instead of server
   ops (`lib.rs:3578-3602`); BFILE `file_exists` and BFILE `read` hardcode ORA-22285
   (`lib.rs:3571-3576`, `:3375-3380`) and would misbehave on an existing BFILE; LOB size/chunk
   computed client-side (`chars().count()` is wrong for supplemental chars — server counts UCS-2
   units); NCLOB CREATE_TEMP always writes UTF8 charset instead of ncharset
   (`protocol/thin.rs:1945`); PL/SQL >32767 binds are not converted to temp LOBs (latent,
   surfaces once debt 2 lands).
4. **Blocking facade remnants** — `BlockingConnection` referenced ~31x in pyshim; sync half is
   plan-conformant, but beads `nto`/`tmr` (in_progress) still moving async paths onto driver
   futures.
5. **No TLS** — no rustls/TLS dep anywhere (verified). M3 (bead `0ue`) not started: TCPS,
   ewallet.pem, connect-string corpus differential all owed.
6. **No arrow-rs** — M5 (bead `12e`) not started; `arrow` feature on `oracledb` is an empty flag.
7. **Monolith files vs plan layout** — plan prescribes `protocol/{packet,capabilities,messages,
   auth,types,net}` modules; reality is one 4.6k-line `thin.rs` and one 9.5k-line pyshim
   `lib.rs`. Wave 0 split plan ready (`.intake/split_plan.md` §7-9).
8. **test_4100_cursor_callproc suspected hang — RESOLVED (no longer reproduces).** Re-checked on
   `branch fix-hangs` HEAD (2026-06-14): the full module runs **27p in 4.29s** under the shim,
   matching the reference (27p), with no hang. The historical >25 min stall in the 17:29Z
   re-baseline is gone — fixed incidentally by intervening waves (the same break/reset-drain and
   reset-marker fixes referenced in §"DML-RETURNING error hang"), not by a callproc-specific
   change. No callproc fix was required; the row is now GREEN with live evidence.
9. **test_1607 regression at HEAD — RESOLVED.** On `branch fix-hangs` HEAD test_1607 (DML returning
   of an object) PASSES; the full test_1600 module is 27p/0f. The ORA-00932 CHAR-vs-ADT regression
   seen at 17:29Z no longer reproduces (the d49 merge's ADT-projection fix, commit d781c39, covers
   it).
10. **`.beads/`, `.claude/`, `AGENTS.md` untracked** — plan says commit `.beads/` with code;
    `git status` shows them untracked. (`.ntm/` is gitignored by design; `.intake/` now ignored
    too — briefs stay local per decision D7.)
11. **Error-metadata parity** — shim error builders raise strings in places where reference
    populates `_Error.full_code/code/offset/isrecoverable`; partially improved (test_1700 green
    at HEAD), residue tracked in cursor_misc/async_tail briefs.
12. **fetch_lobs sticky override** — per-call `fetch_lobs` override on a cursor leaks into later
    executes (setter sets `fetch_lobs_overridden`, never cleared; reference re-reads defaults
    every `_prepare_for_execute`). Latent divergence, see `.intake/async_tail.md` §D.

## 7. Cluster work map (briefs live in `.intake/`, untracked)

- **`.intake/split_plan.md`** — the master root-cause table (~16 distinct causes covering all
  1,066 evidence failures) plus the Wave 0 enabling refactor: a behavior-identical, step-compiling
  split of `pyshim/src/lib.rs` (9,512 LOC) into 16 modules (errors, async_bridge, hooks, pyutil,
  binds, convert, lob, var, typehandler, conn, dbobject, metadata, cursor, async_cursor,
  async_conn, pool — with exact HEAD line ranges) and `protocol/src/thin.rs` (4,559 LOC) into a
  `thin/` directory. Ground rules: verbatim moves, `pub(crate)` visibility, glob re-exports,
  build+test after every step, no public-API change. Includes per-fix yield table (arrow ~387,
  pool ~69, DPL ~60, handlers ~75, pipelining ~51, intervals ~23, ...). Land this before
  parallel lanes touch the monoliths.
- **`.intake/pool.md`** — pool engine entirely stubbed: `ThinPoolImpl.acquire/drop/
  return_connection` and the async twin raise placeholders (`pyshim/lib.rs:9291-9432`); async
  pool also missing every readonly attr and get/set method. Secondary causes: `pool.name` must be
  None not `""`; pool-creation passwords never reach the shim (shim_inject only wraps
  `Connection.__init__`); cursor defaults not sourced from `oracledb.defaults`; latent getmode
  constant bug (FORCEGET=2/TIMEDWAIT=3); DPY-4011 dead-connection semantics missing. Top fix:
  implement the reference pool state machine (free/busy lists, growth, ping_interval, timeouts,
  LIFO reuse, DPY-4005) pooling conn-impl objects in a new shim `pool.rs`. Est yield ~68-69
  (2400/5500/6600/7300).
- **`.intake/lob.md`** — cluster effectively green at HEAD (1900: 39p/3s at 17:30Z; 5700: 28/28
  at 15:19Z); the 06:05Z evidence (53 fails) is obsolete. Remaining baseline delta: 3 false skips
  behind debt 2. The brief's real value is the fake-parity ledger (debt 3) and exact reference
  wire behavior for LOB ops (op codes, locator formats, UCS-2 amount semantics, free-temp
  piggyback) for honest follow-ups.
- **`.intake/handlers_vars.md`** — 3600/3700/3800/6000/5100 (~89-101 fails, still red at HEAD
  per 17:29Z run: 33+27+6+...). Root causes: `ThinVar` has a single value slot with no
  per-element storage, no `metadata`/`buffer_size`/`actual_elements` surface; `setvalue` performs
  none of `_check_value`'s coercion/validation (DPY-2016/3005/3013); output-type-handler
  conversion matrix incomplete and applied at fetch instead of execute (prefetched rows bypass
  handler vars). Mostly shim work (var.rs/typehandler.rs after the split). Est yield ~75-89.
- **`.intake/scalars.md`** — two live root causes at HEAD: (1) INTERVAL DS/YM codecs absent from
  all three crates (bind arm, bind template, column parse for ORA types 182/183) — 21 tests in
  1800/7100; (2) no statement re-describe/retry on type change for the 4600 family (13 tests).
  Most other 06:05Z scalar evidence was fixed by the 07:56-09:55 commit train (confirmed by the
  17:29Z run: 1700/2900/2000 green, 2100/2200/2500 reduced). Est yield ~23 (intervals) + ~13
  (4600) + small tails.
- **`.intake/cursor_misc.md`** — executemany bind shaping largely fixed at HEAD (4000 went
  24F+1E to 4F; 6100 34/34). Confirmed-at-HEAD missing: `executemany(None, N)` prior-bind reuse
  + DPY-2016; batcherrors/arraydmlrowcounts/`get_array_dml_row_counts`/`get_batch_errors`
  (protocol options 0x80000/0x4000 + error-batch decode); implicit results
  (TNS_EXEC_FLAGS_IMPLICIT_RESULTSET for PL/SQL + `get_implicit_results`); scrollable cursors
  (sync `scroll` absent, async stub; fetch orientation flags); cursor-misc semantics in
  4300/6300 (DPY validations, lastrowid, warnings). Est yield ~35 (3200) + ~29 (scroll incl.
  unskips) + ~41 (4300/6300) + 4000 tail.
- **`.intake/pipelining.md`** — 7600: 49/49 failing, all from missing `supports_pipelining`/
  `run_pipeline_with_pipelining`/`run_pipeline_without_pipelining` on `AsyncThinConnImpl`
  (confirmed at HEAD: zero pipeline code in any crate). Key design landmine: the genuine Cython
  `PipelineOpResultImpl` attrs are readonly — the shim must swap `result._impl` for its own
  result-impl object. Staged path: green all 49 via `run_pipeline_without_pipelining`
  (sequential execution) first; true pipelined transport (END_OF_RESPONSE capability retention,
  BEGIN_PIPELINE piggyback, EndPipeline message, token handling) is honest-parity follow-up.
  Est yield ~51 (7600 + 2 in 6800).
- **`.intake/dataframe_dpl.md`** (M5, staged per decision D4) — 557 fails across 8 modules.
  Causes: no `fetching_arrow`/`fetch_df_all`/`fetch_df_batches` on cursor impls (387, incl. all
  of 9300/9400); `requested_schema` / `ArrowSchemaImpl` capsule handling (286 subset); DataFrame
  ingestion through `_prepare_for_executemany` batch manager (78); direct path load — three new
  TTC messages, fn codes 128/129/130, plus batch manager (60); leading-NULL bind inference
  (possibly fixed at HEAD, re-verify 32). Requires the arrow-rs dependency decision and Arrow C
  Data Interface (PyCapsule) export in the shim. The brief carries the full capsule-protocol
  contract (one batch per stream, zero-length arrays for empty results).
- **`.intake/async_tail.md`** — 31 fails, 11 small causes; confirmed-at-HEAD: `bind_var_from_value`
  uses instance `.name` instead of `type(value).__name__`, so plain callproc args synthesize
  VARCHAR vars and OUT numbers come back as `str` (6200/7010); `fetch_decimals` stored but never
  applied and never seeded from `oracledb.defaults` (7018-7020, 6601); `defaults.arraysize/
  prefetchrows` ignored at cursor creation (6600/6603). Possibly-fixed-at-HEAD (re-verify):
  per-call `fetch_lobs` (7015-7017), async DbObject CLOB attrs (5602/5605). Plus thick-only API
  stubs that must raise DPY-3001 (7300). Mostly small shim fixes. Est yield ~31.
- **`.intake/ground_truth.md`** — the analyst draft this document was verified against and
  distilled from; keeps the RC1-C20 cluster table with reference file:line pointers and the
  dependency-sorted implementation order (server_version first, re-baseline second, LONG bind
  promotion, executemany completion, pool, define-override path, small codec lanes, then
  boolean/OSON/JSON/vector, sessionless, pipelining, Arrow/DPL last).

## 8. Decisions log (epic bead `rust-oracledb-j0o`, comment 2026-06-11 17:32Z)

Orchestration session 2026-06-11 (Claude, ultracode), driving while codex is rate-limited:

- **D1** Re-baseline the ~40 red modules from HEAD before assigning work — the 06:05Z manifest
  was 5 commits stale.
- **D2** Enabling refactor first: split `pyshim/lib.rs` and `protocol/thin.rs` into modules so
  parallel lanes own disjoint files; advances bead `d49`.
- **D3** 3 isolated lanes: each gets a git worktree + own venv (`ORACLEDB_VENV_DIR`) + own
  container `rust-oracledb-lane-1523/1524/1525` (schemas provisioned); main checkout + container
  1522 reserved for global verification; builds never share a checkout.
- **D4** M5 dataframe/Arrow + DPL (~557 fails) staged as a dedicated workflow after wave 1.
- **D5** Perf/optimization deferred until the suite is green (correctness first, M6 ordering).
- **D6** Release publishing uses the configured GitHub `origin/main` remote plus the tag-driven
  release workflow. A release tag must match the workspace version and be contained in
  `origin/main` before `scripts/release_preflight.sh` allows publish jobs to proceed.
- **D7** Intake briefs live in `.intake/` (untracked); lane agents read them from disk.

Coordination addendum from the same comment: codex may resume on master; lanes rebase before
merge; the tree stays clean between waves.

## 9. Coordination rules for future agents

- **One builder per checkout.** `maturin develop` installs the shim into the checkout's venv; two
  agents building different revisions into one venv silently corrupt each other's runs.
- **Main checkout (`/home/durakovic/projects/rust-oracledb`) + container 1522 = global
  verification only.** Do feature work in a lane worktree with its own venv and lane container
  (1523-1526; export `ORACLEDB_VENV_DIR`, `ORACLEDB_HOST_PORT` or `PYO_TEST_CONNECT_STRING=
  localhost:<port>/FREEPDB1`, `ORACLEDB_RESULTS_DIR`). Run schema prep on a lane before first
  use. Current worktrees at `978491a`: `rust-oracledb-w0` (branch `wave0-split`),
  `rust-oracledb-m5` (branch `m5-arrow-foundation`).
- **Release publishing uses `origin/main` directly** (decision D6). Keep release commits on the
  configured GitHub remote and cut `vX.Y.Z` tags only from commits that are contained in
  `origin/main`, matching `scripts/release_preflight.sh`.
- **Codex agent may resume on `master` at any time.** Do not rewrite master history; rebase lane
  branches onto master before merging.
- **Until Wave 0 lands**, `pyshim/src/lib.rs` and `protocol/src/thin.rs` are exclusive locks —
  one writer at a time, coordinate via beads.
- **Stale-evidence trap:** never claim "still broken" or "fixed" without a module-level pytest
  JSON produced at the claiming commit. Check artifact mtimes against `git log` before trusting.
- **Match-or-beat, not green:** skips allowed only where baseline skips. Run
  `scripts/fake_parity_scan.py` before any milestone claim. Never edit `reference/` except via
  re-pinning.
- **AGENTS.md hard rules apply:** no deletions without explicit operator approval, no
  `git reset --hard`, no `rm -rf`.
- Task state lives in `.beads/issues.jsonl` (use `br`); 28 issues as of this writing — open:
  `j0o` (epic), `grk` (M2, in_progress), `0ue` (M3), `gj0` (M4), `12e` (M5), `7r6` (M6),
  `d49` (in_progress) / `p5o` (shim logic migration), `nto`/`tmr` (async facade, in_progress),
  `xvf` (scanner), `u4w` (operator: GitHub remote); 16 closed.

## Build-contention hazard (discovered 2026-06-11, M5 lane)

A global `CARGO_TARGET_DIR=/tmp/cargo-target` is shared across checkouts: concurrent
builds from different worktrees cross-contaminate artifacts (one lane's in-progress
enum variants leaked into another lane's build). RULE: every concurrent agent exports
its own `CARGO_TARGET_DIR=/tmp/cargo-target-<lane>` for all cargo/maturin invocations.
Single-builder sessions (main checkout only) may keep the default for cache warmth.

## Container fleet (verification + lanes)

- **rust-oracledb-free** (port 1522): the GLOBAL verification container — every post-merge
  check on the main checkout runs here. MUST be restarted after any host/Docker event.
- **rust-oracledb-lane-1523/1524/1526/1527** (+ 1525 retired): per-lane containers.
- Containers stop on host events (API outages, reboots); schemas PERSIST across `docker start`.
  After any outage, restart ALL of them (1522 included — easy to forget) and verify each with a
  `select count(*) from TestNumbers` before trusting a red run. A down container shows as mass
  `RuntimeError: Connection refused (os error 111)` setup-errors, which looks like a code
  regression but is not — check container health FIRST.
