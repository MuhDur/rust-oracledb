# Parity Ledger — rust-oracledb vs python-oracledb thin

> **Purpose.** A living record of where this driver stands against
> python-oracledb thin's *own open issues*: which thin-mode bugs we faithfully
> inherited (and must decide whether to fix or hold for byte-parity), which we
> are structurally **immune** to, and where we are **better than upstream**.
> This complements the conformance evidence (`PARITY_SKIPS.md`,
> `CONFORMANCE_MATRIX.md`) — that proves we *match* the reference suite; this
> tracks where matching the reference is *not* the goal.
>
> **How to maintain.** When a new upstream issue is triaged against our code,
> add a row with the verdict + file:line. When we ship a fix that intentionally
> diverges from upstream's (buggy) wire behavior, record it in the Divergences
> section so the conformance harness's expected-failure set stays honest.

Last full audit: **2026-06-29** (all 24 open `oracle/python-oracledb` issues;
reference vendored at v4.0.1). Tracking beads carry the `upstream-parity` label.

---

## 1. Inherited thin-mode bugs (we reproduce upstream)

| Upstream | Behavior we inherit | Status | Our code |
|---|---|---|---|
| #376 | thin TPC `tpc_begin` sends `timeout=0`/`flags=NEW`; cross-connection `tpc_commit` can hit ORA-24756, but clean Oracle Free 23ai can also commit the prepared branch successfully | HOLD for parity (gate on upstream); ignored live probe records whether the inherited ORA-24756 reproduces in the current clean DB | `lib.rs:tpc_begin`; `tests/e2e_live.rs:tpc_cross_connection_commit_default_timeout_documents_ora_24756` |

## 2. Robustness gaps (present in our code)

| Upstream | Gap | Status | Our code |
|---|---|---|---|
| _(none currently open in this audit set)_ | | | |

## 2a. Fixed / surpassed in 0.5.1

| Upstream | Previous behavior | 0.5.1 behavior | Evidence |
|---|---|---|---|
| #374 | tz-aware TSTZ bind lost the caller's offset | `BindValue::TimestampTz` and chrono `DateTime<FixedOffset>` / `DateTime<Utc>` bind real offset bytes | `codecs.rs:encode_oracle_timestamp_tz_with_offset`; `sql_convert.rs:chrono_to_sql`; `thin/proptests.rs:timestamp_tz_preserves_negative_half_hour_offset_bytes` |
| #274/#373/#20 | TSTZ fetch returned tz-naive values and discarded the fixed offset | fixed-offset TSTZ fetch returns `QueryValue::TimestampTz`; chrono `DateTime<FixedOffset>` / `Utc` preserve the offset; named-region zones still fail closed | `codecs.rs:decode_datetime_value`; `sql_convert.rs:chrono_impls`; `live_typed.rs:assert_timestamp_tz` |
| #502 | Post-dial listener ACCEPT/AUTH reads were unbounded, and DSN timeout was ignored | parsed `transport_connect_timeout` / `connect_timeout` bounds dial, TLS, ACCEPT, and AUTH as one connect deadline | `lib.rs:Connection::connect`; `lib.rs:transport_connect_timeout_bounds_post_dial_accept_read` |
| #579 | IAM-token auth omitted `TOKEN_AUTH=OCI_TOKEN` and flattened TCPS descriptors to TCP | token auth emits TCPS descriptors, preserves non-wallet `SECURITY` passthrough, and injects `(TOKEN_AUTH=OCI_TOKEN)` | `lib.rs:token_auth_descriptor_uses_tcps_security_and_passthrough` |

## 3. Where we are IMMUNE or BETTER than upstream thin ✅

| Upstream | Their behavior | Ours | Why |
|---|---|---|---|
| #595 | `LookupError: unknown encoding` crash on a column whose charsetid maps to an empty codec name | **Immune** | We keep no charsetid→codec table; describe parsing discards per-column charset ids and text decode uses `csfrm` only. Guard: `fetch.rs:column_metadata_discards_server_charset_id_and_keeps_csfrm_only`. |
| #400 | `ValueError: year -4712 is out of range` (Python `datetime` MINYEAR=1) | **Better** | We decode into chrono, which supports negative/BCE years. Guard: `sql_convert.rs:chrono_from_and_to_sql` covers year `-4712`. |
| #596 | `fetch_df_all` double-applies the TSTZ offset (thick) | **Immune** | Our Arrow path converts `QueryValue::TimestampTz` to epoch once; there is no second offset application and no thick/OCI path. Guard: `arrow/mod.rs:timestamp_tz_maps_to_arrow_epoch_once`. |
| #422 | thin-only ORA-00979 on `GROUP BY CASE … :bind` because thin sends one positional bind **per occurrence** | **Better (named binds)** | Our named-bind path dedups by name, so Oracle binds the repeated placeholder by name and the optimizer matches the expressions. Guards: `sql_convert.rs:resolve_params_dedups_group_by_case_repeated_named_bind`; ignored live `tests/repro_repeated_named_bind.rs`. We only reproduce #422 if a caller uses *positional* binds with per-occurrence duplication (faithful thin). |

## 4. Already tracked elsewhere

| Upstream | Topic | Bead |
|---|---|---|
| #82, #381 | Kerberos (thin, + pool) | `qm4` (typed-unsupported surface) + `kerberos-radius-backends` (real backend) |
| #592 | ORA-24964 on cross-TZ-version PDB open | `mwu` (compile-caps TZ-version gap; inherited, pending upstream) |
| #2/#3/#4/#6 | passwordless/Kerberos/RADIUS/wallet auth capability surfaces | `o0b`, `qm4`, `x1p` (see `downstream-capability-honesty`) |

## 5. Feature gaps shared with upstream (parity, documented out-of-scope)

These are absent in python-oracledb thin too; we match by parity. They are
*surpass* opportunities, not bugs, and are already noted in `SUPPORT.md` /
`CONFORMANCE_MATRIX.md`.

| Upstream | Feature |
|---|---|
| #111 | LDAP / directory naming (ldap.ora) |
| #253 | SOCKS proxy (note: `https_proxy` is parsed but not wired) |
| #318 | sharding key / super sharding key |
| #324 | SQL\*Net network compression |
| #366 | OracleConfigurationProvider (file/https config providers) |
| #564 | Direct-Path Load into a specific partition |

## 6. Not applicable

#398 (thick TNS_ADMIN/LDAP), #122 (disable thick build), #302 (Python `Record`
object), #594 (Python 3.14 t-strings) — thick-mode-only or Python-API-only; the
Rust driver has no thick mode and uses `FromRow`/`Row`.

---

## Intentional divergences from upstream (expected-failure register)

When we fix an inherited bug by changing wire output, upstream's buggy behavior
becomes an *intentional divergence* — record it here so the conformance harness
treats it as XFAIL, not a regression (see `testing-conformance-harnesses`).

| ID | Divergence | Rationale | Conformance impact |
|---|---|---|---|
| #374/#274 | Fixed-offset TSTZ bind/fetch preserves numeric offsets instead of matching python-oracledb thin's offset-dropping behavior | Offset loss is a correctness bug and the Rust API can expose offset-aware chrono types additively | Upstream tests that assert tz-naive/offset-dropped values should be XFAILed; the conformance target is preserved instant/offset, not the buggy reference value |
