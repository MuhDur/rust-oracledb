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
| #374 | tz-aware TSTZ/LTZ **bind** stored as UTC (offset dropped) — and no `ToSql for DateTime<Tz>` exists at all | FIX planned (surpass) | `codecs.rs:75-76`; `sql_convert.rs:1060-1087` |
| #274/#373/#20 | TSTZ/LTZ **fetch** returns tz-naive datetime; named-region zones error | FIX planned (surpass, additive) | `codecs.rs:99-110` |
| #376 | thin TPC `tpc_begin` sends `timeout=0`/`flags=NEW`; cross-connection `tpc_commit` can hit ORA-24756 | HOLD for parity (gate on upstream) | `sessionless.rs:288` |
| #579 | IAM-token thin omits `TOKEN_AUTH=OCI_TOKEN` from the TNS connect descriptor → ADB-S private-endpoint listener refuses | FIX planned (we are currently *worse* — see below) | `lib.rs:8106` |

## 2. Robustness gaps (present in our code)

| Upstream | Gap | Status | Our code |
|---|---|---|---|
| #502 | Only the TCP dial is bounded (fixed 20s, ignores `tcp_connect_timeout`); post-dial ACCEPT/AUTH reads are unbounded → a server that accepts then stalls hangs forever | FIX planned | `lib.rs:2210`, `lib.rs:2265` |

## 3. Where we are IMMUNE or BETTER than upstream thin ✅

| Upstream | Their behavior | Ours | Why |
|---|---|---|---|
| #595 | `LookupError: unknown encoding` crash on a column whose charsetid maps to an empty codec name | **Immune** | We keep no charsetid→codec table; force AL32UTF8 (csid 873) in the define metadata and decode by `csfrm` only (`codecs.rs:724-738`). The whole failure class is gone. |
| #400 | `ValueError: year -4712 is out of range` (Python `datetime` MINYEAR=1) | **Better** | We decode into chrono, which supports negative/BCE years (`sql_convert.rs:368/411`). |
| #596 | `fetch_df_all` double-applies the TSTZ offset (thick) | **Immune** | Our Arrow path reuses the single-decoded row `QueryValue`; there is no second offset application and no thick/OCI path. Arrow value == row value by construction. |
| #422 | thin-only ORA-00979 on `GROUP BY CASE … :bind` because thin sends one positional bind **per occurrence** | **Better (named binds)** | Our named-bind path dedups by name (`order_named_binds`), so Oracle binds the repeated placeholder by name and the optimizer matches the expressions. Proven live: `tests/repro_repeated_named_bind.rs` (`select :v + :v` → 10, not ORA-01008). We only reproduce #422 if a caller uses *positional* binds with per-occurrence duplication (faithful thin). |

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
| _(none yet — populate when the #374 bind fix lands)_ | | | |
