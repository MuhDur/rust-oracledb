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
| #596 | TSTZ column converted incorrectly to Arrow (thin, fixed upstream 714178610379 to emit wall-clock tz-naive) | **Reconciled — now matches upstream** | The wire (and `QueryValue::TimestampTz`) `year`..`nanosecond` fields are the raw UTC instant, with the display timezone carried separately in `offset_minutes` (`decoders.pyx::decode_date`). Upstream's `converters.pyx::convert_date_to_python` builds a naive `datetime` from those UTC fields and then **adds** the offset to it before `convert_date_to_arrow_timestamp` computes the Arrow epoch — the offset is applied, not dropped, and the resulting Arrow value is the **display wall clock** (UTC + offset), not the raw UTC instant. Our 0.8.4 and earlier Arrow path used the raw UTC fields verbatim for TSTZ (offset never applied), which under-rendered the display offset entirely — a bug, not immunity, and the guard tests asserting that behavior were self-fulfilling (hand-built `QueryValue`s with the wall clock already baked into the "UTC" fields). Fixed by `epoch_parts_from_tstz` (`arrow/builders.rs`), which adds `offset_minutes` before computing the epoch. LTZ already decoded as offset-less `DateTime`, so it was always wall-clock and is unaffected. The offset-preserving instant remains available on the row (non-Arrow) fetch path (`DateTime<FixedOffset>`). Guard: `arrow/mod.rs:timestamp_tz_maps_to_arrow_display_wall_clock` + `timestamp_tz_arrow_epoch_is_offset_covariant` (both decoder-produced fixtures, not hand-built `QueryValue`s). |
| #596-ns | Arrow `Timestamp(Nanosecond)` fractional seconds are truncated to **microseconds** upstream (`decoders.pyx:102` does `decode_uint32be(&ptr[7]) // 1000` because `value.fsecond` feeds a Python µs-only `datetime`), so upstream Arrow output always zero-pads the last 3 nanosecond digits | **Better** | We keep the wire's true nanosecond fraction verbatim (`codecs.rs` decode → `arrow/builders.rs:epoch_parts_from_components`/`epoch_parts_from_tstz`), so Arrow `Timestamp(Nanosecond)` carries full ns precision rather than upstream's µs-capped value. Deterministic and lossless; the cap is a Python-`datetime` limitation, not an Oracle one. Intentional divergence surfaced by the python-oracledb parity verification (bead `F-NS`); we keep the more-precise value. Guard: `arrow/mod.rs` ns assertions (`…123_456_789`). |
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
| retry-classify-sql | `oracledb::retry::Idempotency::classify_sql` peels **whitespace only**; a leading `--` / `/* */` before `SELECT` classifies as `NonIdempotent` | Rust-only retry eligibility gate (python-oracledb has no equivalent auto-retry helper). The reference's comment-skipping first-keyword scan is for protocol `_is_query`, not automatic replay. Fail-safe: never silently widen retry eligibility by peeling comments | No reference-suite impact (API is Rust-only). Guard: `retry.rs::classify_sql_leading_comment_select_is_non_idempotent` |
| sni-service-form (P2-4) | Under `use_sni=true`, python-oracledb sends the Oracle **service-form** SNI token `S{len}.{service}[.T1.c].V3.{version}` (`transport.pyx::_calc_sni_data`) as `ssl` `server_hostname`. CPython's ssl accepts arbitrary ASCII there, which is how Oracle's one-negotiation SNI routing fast-path works. rustls `ServerName` is RFC-strict and **cannot** carry that token (underscores in service labels + the all-numeric `.V3.319` terminal label fail its DNS grammar). This driver: (1) fails closed with typed `Error::UnsupportedSni` for non-ADB unencodable tokens (no silent no-SNI downgrade); (2) for OCI ADB descriptor shapes only, falls back to sending the **listener host** as SNI (`decide_sni` / `is_oci_adb_endpoint` in `tls.rs`) so the handshake completes. Host-as-SNI does **not** trigger Oracle's routing fast-path — a documented **performance nuance**, not a functional gap. The post-handshake Oracle DN/name match remains authoritative either way. True service-form parity would require patching rustls. | Structural stack limit (rustls), not an incomplete port. Guard: `tls.rs::decide_sni_for_oci_adb_uses_the_valid_endpoint_host`, `decide_sni_private_endpoint_adb_uses_host_fallback`, `decide_sni_with_use_sni_fails_closed_not_silent`, `only_the_oci_adb_descriptor_shape_gets_the_host_sni_fallback`. Docs: `TLS_SETUP.md` §3. |
| tls-config-fail-fast (B5/4sfc) | python-oracledb's multi-address retry loop (`connection.pyx` connect) swallows per-address exceptions — including **terminal TLS configuration** failures (bad wallet, unusable client cert, UnsupportedSni) — until the last attempt / call budget expires, often surfacing as a useless timeout. This driver runs `prepare_tls_handshake` **before** the address loop and returns typed `Error::Tls` / `Error::UnsupportedSni` / wallet errors on attempt 1. | **Deliberate strictly-better deviation.** Do not "fix back" to reference retry swallowing in a parity audit. Guard: `connection_connect_returns_tls_configuration_error_without_dialling`, `tls_configuration_failure_is_typed_before_any_transport_attempt`, `f2_transport_attempt_type_only_covers_io_and_tls_handshake`. Implementing commits: `880134e`, `d99927d`. |
