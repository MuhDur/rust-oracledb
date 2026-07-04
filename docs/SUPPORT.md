# Supported Profiles

This file is the support contract for the published Rust crates. It defines the
feature profiles that CI must keep compiling and testing. It does not claim that
every arbitrary feature combination is separately supported.

See [docs/TOOLCHAIN.md](TOOLCHAIN.md) for the Rust toolchain pin and re-pin
procedure.

## Driver Profiles

The `oracledb` crate is nightly-only because the async runtime dependency uses
nightly Rust. The sans-I/O `oracledb-protocol` crate has a separate stable lane
tracked by W0-T3.4.

| profile | command | contract |
|---|---|---|
| minimal | `cargo check -p oracledb --locked --no-default-features` | Driver core without `derive` or optional integrations. |
| default | `cargo check -p oracledb --locked` | Standard user build; includes `derive`. |
| all-features | `cargo check -p oracledb --locked --all-features` | Maximal compile smoke for the driver crate. This does not imply every arbitrary subset is individually supported. |

## Optional Integration Matrix

The supported optional integration slices are:

| feature | purpose |
|---|---|
| `chrono` | `FromSql` / `ToSql` bridge for `chrono` date/time types. |
| `uuid` | `FromSql` / `ToSql` bridge for `uuid::Uuid`. |
| `serde_json` | `FromSql` / `ToSql` bridge for `serde_json::Value`. |
| `rust_decimal` | Lossless `rust_decimal::Decimal` bridge for NUMBER. |
| `arrow` | Arrow `RecordBatch` fetch and ingest helpers. |
| `soda` | Experimental thin-mode SODA facade over the thin protocol. |

CI exercises those slices with `cargo-hack 0.6.45`:

```sh
cargo hack check -p oracledb --locked \
  --feature-powerset --depth 1 \
  --include-features chrono,uuid,serde_json,rust_decimal,arrow,soda

cargo hack test -p oracledb --locked --lib \
  --feature-powerset --depth 1 \
  --include-features chrono,uuid,serde_json,rust_decimal,arrow,soda
```

With these flags, cargo-hack runs `--no-default-features --features <feature>`
for each named integration. The default and all-features profiles above cover
the ordinary `derive` build and maximal compile smoke.

## Documented But Not Matrix Profiles

These features are intentionally outside the W0-T3.1 optional integration
matrix:

| feature | status |
|---|---|
| `derive` | Default feature; covered by the default profile and derive-specific tests. |
| `tracing` | Observability feature; covered by all-features compile smoke and observability tests. |
| `cassette` | Transport record/replay seam; covered by record/replay tests and all-features compile smoke. |
| `experimental` | Historical no-op since 0.7.x: the wallet readers (`ewallet.pem` incl. encrypted keys, `ewallet.p12`, `cwallet.sso`) are always compiled. Kept so `--features experimental` builds keep working. |

Unsupported feature combinations should be documented explicitly before they are
relied on. Do not infer support from `--all-features` alone.

---

# Live Support Matrix

The section above is the *build* support contract (which feature profiles CI keeps
compiling and testing). This section is the *runtime* support promise for 1.0:
which Oracle servers, transports, charsets, time zones, platforms, and
authentication modes the driver claims to work against.

Every claim below cites the code it is derived from (`file:line`). A 1.0 support
promise must be accurate, so where a behaviour is intended but not provable from
the code alone it is marked **intended/unverified**. This matrix is *verified* end
to end by the live conformance run (W3-E7.2, `harness/run.sh diff`); per-profile
doc builds are W4-T3.

## 1. Oracle Database server support

The driver is a thin-mode TTC/TNS client. It negotiates two independent version
axes with the server:

| axis | client value | meaning |
|---|---|---|
| TNS transport version | advertised minimum `TNS_VERSION_MIN = 300`, accepted floor `TNS_VERSION_MIN_ACCEPTED = 315`, desired `TNS_VERSION_DESIRED = 319` | the listener/transport protocol version. The CONNECT packet advertises 300 like the reference, but any ACCEPT below `315` (the 12.1 wire format; Oracle 11g answers `314`) is refused with the structured `UnsupportedVersion` error naming the floor — python-oracledb `TNS_VERSION_MIN_ACCEPTED` / DPY-3010 parity. Sessions cap at `min(server, 319)`. |
| TTC capability (field) version | client advertises `ttc_field_version = 24` (the FAST_AUTH compile-caps blob), negotiated down to the server's value | drives which message/response shapes are used (12.1 → 23ai feature gates). |

Citations:

- TNS version constants (`TNS_VERSION_MIN`, `TNS_VERSION_MIN_ACCEPTED`, `TNS_VERSION_DESIRED`): `crates/oracledb-protocol/src/lib.rs`.
- TNS negotiation (reject `< 315` at ACCEPT-parse time, before the rest of the payload is touched; cap at `319`): `parse_accept_payload` in `crates/oracledb-protocol/src/thin/connect.rs` and `crates/oracledb-protocol/src/capabilities.rs`. A server below the floor yields the structured `ProtocolError::UnsupportedVersion { version, minimum }` — i.e. **fail closed** with a self-explanatory refusal, not a silent downgrade or a decode error. Live-verified against Oracle 11g XE (`scripts/version_matrix.sh`, xe11 lane) and pinned by `oracledb-protocol/tests/pre23ai_handshake_golden.rs`.
- TTC field-version floor/negotiation (`server_ttc_field_version.max(default)`): `crates/oracledb-protocol/src/thin/connect.rs:154-159`.
- Default client TTC field version (`24`): `crates/oracledb-protocol/src/thin/types.rs:38`.
- TTC field-version → Oracle release map (the constants the gates compare against): `TNS_CCAP_FIELD_VERSION_12_1 = 7`, `_12_2 = 8`, `_20_1 = 14`, `_21_1 = 16`, `_23_1 = 17`, `_23_1_EXT_1 = 18`, `_23_1_EXT_3 = 20` — `crates/oracledb-protocol/src/thin/constants.rs:78,283-288`.

**Server families promised for 1.0:**

| Oracle Database release | status |
|---|---|
| 12.1 / 12.2 | Supported — TTC field-version gates 7/8 exist and are honoured (`constants.rs:78,283`). The accepted TNS floor (315 = the 12.1 wire format) admits these servers. |
| 18c / 19c | Supported — covered by the 12.2-and-up gates; no release-specific gate is required between 12.2 and 20.1. |
| 21c | Supported — `_21_1 = 16` gate (`constants.rs:285`). |
| 23ai (23.x) | Supported — this is the client's own capability level (`ttc_field_version = 24`, above `_23_1_EXT_3 = 20`); 23ai features (native BOOLEAN, SQL domains, VECTOR, annotations) are wired (`types.rs:69-75,357`). |
| Pre-12.1 (11g and older servers) | Refused — any server whose ACCEPT carries a TNS transport version below `315` (11g negotiates `314`) is rejected with the structured `UnsupportedVersion` error naming the floor; asserted continuously by the `xe11` matrix lane. |

Notes on the version mapping: the TNS transport version (`319`) and the TTC field
version (`24`) are protocol-internal numbers, **not** Oracle marketing release
numbers; the release rows above are derived from the TTC field-version constants,
which are the values that actually gate behaviour. The **minimum** promised server
is one that negotiates TTC field version `>= 7` (12.1) and TNS transport version
`>= 315`. The **tested** set is whatever the live conformance matrix (W3-E7.2)
pins; the driver is developed and conformance-tested against the python-oracledb
reference at tag `v4.0.1` (`crates/oracledb-protocol/src/lib.rs:17-18`). The exact
tested server releases are recorded by that live run, not asserted here
(**intended/unverified** at the doc level until W3-E7.2 pins them).

## 2. TLS / TCPS

The driver speaks Oracle TCPS using **rustls** driven over the asupersync TLS
transport (`asupersync` `tls` feature — `crates/oracledb/Cargo.toml:67-69`). The
sans-I/O crate holds the TLS *algorithms* (SNI string, DN match, wallet parsing);
the `oracledb` crate builds the rustls `ClientConfig` and the custom verifier.

| aspect | 1.0 promise | citation |
|---|---|---|
| Transport | TCPS (TLS over TCP). Plain TCP is also supported for non-TLS connections. | `crates/oracledb/src/tls.rs` (TCPS config + handshake) |
| TLS versions | rustls **safe defaults**: TLS 1.3 **and** TLS 1.2 (the `tls12` rustls feature is enabled). Not pinned to a single version. | `.with_safe_default_protocol_versions()` at `crates/oracledb/src/tls.rs:250`; `tls12` at `Cargo.toml:30` |
| Crypto provider | **ring** (per-config provider; no global install). aws-lc-rs is **not** used. | `crates/oracledb/src/tls.rs:221-223,249`; `rustls`/`rustls-webpki` use the `ring` feature only (`Cargo.toml:30,39`) |
| Cert-verification model | A custom `OracleServerCertVerifier` does **real chain/path validation** against the trust anchors via webpki (`verify_for_usage`, server-auth key usage) **and then** the Oracle DN/SAN/CN match. Standard DNS-hostname verification is intentionally disabled (mirrors python-oracledb `check_hostname=False`); the Oracle DN match replaces it. | verifier `crates/oracledb/src/tls.rs:63,92-137`; chain validation `:101-128`; DN match `:78-89`; hostname param ignored `:97` |
| DN match | If `server_cert_dn` is set, exact DN equality (`check_cert_dn`); otherwise SAN/CN name match against the expected host (`check_server_name`). | `crates/oracledb-protocol/src/tls/dn.rs:149,228`; SNI string `crates/oracledb-protocol/src/tls/sni.rs:35-51` |
| Trust anchors | Wallet CA certs when a wallet provides them; otherwise the OS CA bundle read directly from disk (`/etc/ssl/certs/ca-certificates.crt` and known fallbacks). No `webpki-roots`/`rustls-native-certs` crate. Zero usable anchors → `Error::Tls("no trust anchors…")` (**fail closed**). | selection `crates/oracledb/src/tls.rs:229-239`; OS bundle list `:294-321` |
| mTLS (client cert) | Supported: a wallet that carries a client cert chain + key is wired into the config via `with_client_auth_cert`; otherwise `with_no_client_auth`. | `crates/oracledb/src/tls.rs:256-272`; key parse `:279-289` |

**Wallet / certificate formats accepted:**

| format | status | citation |
|---|---|---|
| `ewallet.pem` (PEM trust anchors; optional client chain + unencrypted PKCS#8/PKCS#1/SEC1 key for mTLS) | **Fully supported.** | `crates/oracledb-protocol/src/tls/wallet.rs` (`parse_ewallet_pem`) |
| Raw PEM CA bundles (system roots) | Supported for the no-wallet path. | `crates/oracledb-protocol/src/tls/wallet.rs` (`parse_pem_certificates`) |
| `cwallet.sso` (SSO auto-login wallet, PKCS#12 container) | **Supported** (promoted from experimental in 0.7.x): outer container magic `A1F84E` versions 6/7/8 with the `num3 == 6` AES-128-CBC auto-login sub-type (incl. auto-login-local key re-derivation), inner PKCS#12 PBES2/PBKDF2/AES-CBC. Unsupported sub-types (`5` no-key, `0x35` single-DES) and legacy 3DES/RC2 PKCS#12 schemes fail closed with typed `WalletError::Sso`/`WalletError::Pkcs12` errors. Verified offline against a **real `orapki` 23.26-generated wallet** (`tests/fixtures/tls/cwallet_orapki.sso`) whose extracted certs/keys are byte-identical to its paired `ewallet.p12`. | reader `crates/oracledb-protocol/src/tls/sso.rs`; PFX core `crates/oracledb-protocol/src/tls/pfx.rs`; wiring `crates/oracledb/src/tls.rs` (`load_wallet`); tests `crates/oracledb-protocol/tests/tls_wallet.rs` |
| Encrypted `ewallet.pem` private key | **Supported since 0.7.x** — a PKCS#8 `ENCRYPTED PRIVATE KEY` block is decrypted with `wallet_password` (PBES2 / PBKDF2-HMAC-SHA1/SHA256 / AES-128/192/256-CBC — the scheme ADB wallet downloads and `openssl pkcs8 -topk8` emit; RustCrypto only). Missing password → typed `WalletError::PasswordRequired`; wrong password or unsupported scheme (scrypt, PBES1, legacy `Proc-Type: 4,ENCRYPTED`) → typed `WalletError::KeyDecrypt` naming the remediation. Never silently degrades to verify-only. | `crates/oracledb-protocol/src/tls/wallet.rs` (`parse_ewallet_pem`), `crates/oracledb-protocol/src/tls/pfx.rs` (`decrypt_encrypted_private_key_info`); tests `crates/oracledb-protocol/tests/tls_wallet.rs` |
| `ewallet.p12` standalone PKCS#12 wallet | **Supported since 0.7.x** — `parse_ewallet_p12`/`read_ewallet_p12` reuse the PFX reader; requires `wallet_password` (typed `WalletError::PasswordRequired` when absent). Modern PBES2/PBKDF2/AES-CBC wallets (orapki 19c+/23ai, `openssl pkcs12 -export` defaults) verified offline against a **real `orapki` 23.26-generated `ewallet.p12`**; legacy 3DES/RC2 wallets fail closed with a typed `WalletError::Pkcs12` naming the unsupported OID. | `crates/oracledb-protocol/src/tls/wallet.rs` (`parse_ewallet_p12`); loader precedence `crates/oracledb/src/tls.rs` (`load_wallet`); tests `crates/oracledb-protocol/tests/tls_wallet.rs` |

**Honesty note (offline vs live):** the encrypted-PEM / p12 / sso rows above are
proven **offline** — real `orapki`-generated and openssl-generated wallets are
parsed, decrypted, and cross-checked (sso ≡ p12 contents) in unit/integration
tests, and the identical rustls wiring is exercised by the live TCPS handshake
tests. A full end-to-end acceptance against a real **Autonomous Database**
TCPS endpoint (real ADB wallet zip) still requires ADB infrastructure and has
**not** been run; until then treat ADB-specific behaviour as
**intended/unverified**.

**Wallet loading precedence** (`crates/oracledb/src/tls.rs::load_wallet`):
`ewallet.pem` first (python-oracledb thin parity — the reference reads *only*
this file); else `ewallet.p12` when `wallet_password` is supplied; else
`cwallet.sso`; else a passwordless `ewallet.p12` fails closed with
`PasswordRequired`. Note that `ewallet.p12`/`cwallet.sso` reading **exceeds**
the pinned python-oracledb 4.0.1 thin reference (which requires `ewallet.pem`);
the encrypted-PEM decryption matches the reference's
`load_cert_chain(..., password=...)` behaviour.

**Wallet resolution precedence:** explicit `wallet_location` (the literal `SYSTEM`
means "no wallet, use system roots"), then `TNS_ADMIN`
(`crates/oracledb-protocol/src/tls/wallet.rs:94-108`).
Formatted wallet diagnostics redact wallet paths and wallet passwords. `ConnectOptions`
`Debug` redacts `wallet_location`, `wallet_password`, access tokens, and server
certificate DN material; `WalletError` display/debug messages do not print the
filesystem path stored in the error.

> Native Network Encryption / Data Integrity (the non-TLS Oracle NNE transport
> encryption) is **not** supported and is handled in the auth section below
> (fail-closed).

## 3. Character sets, NLS, and time zones

| aspect | 1.0 promise | citation |
|---|---|---|
| Client character set | The thin protocol always negotiates **AL32UTF8** (charset id `873`). All `VARCHAR2`/`CHAR`/`CLOB` text crosses the wire as UTF-8; the driver does not transcode to a legacy client charset. | `TNS_CHARSET_UTF8 = 873` `crates/oracledb-protocol/src/thin/constants.rs:316`; `ClientCapabilities::charset_id = 873` default `crates/oracledb-protocol/src/thin/types.rs:38-42` |
| Server / DB charset | The DB charset id is read from the protocol-info response and carried in `ClientCapabilities.charset_id`; ids `>= 800` are treated as multi-byte (drives direct-path CLOB form selection). The server is expected to be AL32UTF8 (or otherwise UTF-8-compatible on the wire). | `crates/oracledb-protocol/src/thin/connect.rs:143,169-173`; `crates/oracledb-protocol/src/thin/types.rs:30-32` |
| NCHAR / NVARCHAR2 / NCLOB (`csfrm = NCHAR`) | Supported; the national character set is also handled as UTF-8 on the wire (`CS_FORM_NCHAR` path). | `csfrm` handling in `crates/oracledb-protocol/src/thin/types.rs:49` and the LOB/text codecs |
| `TIMESTAMP WITH TIME ZONE` — fixed UTC offset | Supported; fixed-offset values decode as `QueryValue::TimestampTz` and can be read as `chrono::DateTime<FixedOffset>` / `DateTime<Utc>` with the offset preserved. `NaiveDateTime` conversion remains available for legacy callers. `DateTime<FixedOffset>` / `DateTime<Utc>` bind through offset-preserving `BindValue::TimestampTz`. | `crates/oracledb-protocol/src/thin/codecs.rs`; `crates/oracledb/src/sql_convert.rs` |
| `TIMESTAMP WITH TIME ZONE` — **named region** (e.g. `America/New_York`) | **Not supported** on read — a named-region TSTZ value returns `ProtocolError::UnsupportedFeature("named TIMESTAMP WITH TIME ZONE region")` (**fail closed**). Only fixed numeric offsets are decoded. | `crates/oracledb-protocol/src/thin/codecs.rs:100-104` |
| TZ-file-version capability gap (ORA-24964) | **Known limitation, inherited, not client-fixable.** The FAST_AUTH compile-caps blob is byte-identical to python-oracledb thin and omits the TZ-file-version capability that thick mode advertises; a server performing a cross-TZ-version PDB switch can raise **ORA-24964** exactly as it does against python-oracledb thin. Tracked as bead `rust-oracledb-mwu` (upstream python-oracledb **#592**, still open/unconfirmed by Oracle). No code change until upstream adds the capability byte. | compile-caps blob `crates/oracledb-protocol/src/thin/constants.rs:346-414`; bead `rust-oracledb-mwu` |

## 4. Platform targets

CI runs entirely on `ubuntu-latest` and pins the project's nightly toolchain
(`.github/workflows/ci.yml:29-49`; nightly is build-time-only — 1.0 ships a single
static binary, so the nightly pin is invisible to consumers). There is no MSRV /
stable lane for the `oracledb` driver crate (nightly-only, per the Driver Profiles
note above).

| target triple | tier | citation |
|---|---|---|
| `x86_64-unknown-linux-gnu` | **Tier 1** — all CI build/test/clippy/fuzz jobs run here. | `.github/workflows/ci.yml:29,42,58,72,90,103,121`; fuzz pinned to gnu `:141` |
| `x86_64-unknown-linux-musl` | **Tier 1 (release artifact)** — the published static `oracledb-smoke` binary is built fully-static for musl in the release workflow. | `.github/workflows/release.yml:69-105` |
| Other targets (other Linux arches, macOS, Windows) | **Best-effort / untested.** The crate is portable pure-Rust (`#![forbid(unsafe_code)]` in `oracledb-protocol`) but no CI proves these; treat as unverified for 1.0. | **intended/unverified** — no workflow builds them |

## 5. Authentication modes (and fail-closed guarantee)

Every unsupported authentication path returns a **typed** error; the driver never
silently falls back to a weaker or no-auth mode.

**Supported in 1.0:**

| mode | how | citation |
|---|---|---|
| Password (O5LOGON) | Two-phase challenge/response with mandatory server-response MAC verification. Verifier types: 11g (`0xB152`, `0x1B25`, SHA-1) and 12c (`0x4815`, PBKDF2-HMAC-SHA512). | `crates/oracledb-protocol/src/crypto.rs:30-59`; verifier constants `crates/oracledb-protocol/src/thin/constants.rs:194-196`; server-MAC check `crypto.rs:142-157`, driver `lib.rs:3600-3603` |
| Proxy auth (`user[proxy_user]`) | Writes `PROXY_CLIENT_NAME`. Surfaced via `ConnectOptions::with_proxy_user`. | `crates/oracledb-protocol/src/thin/auth.rs:193-195`; `crates/oracledb/src/lib.rs:3131-3133` |
| Change-password at connect | `TNS_AUTH_MODE_CHANGE_PASSWORD`. | `crates/oracledb-protocol/src/thin/auth.rs:236-254` |
| OCI IAM / OAuth2 database **token** (pre-supplied bearer token) | `AUTH_TOKEN` sent with no password verifier, via `ConnectOptions::with_access_token`; the connect descriptor uses TCPS and injects `(TOKEN_AUTH=OCI_TOKEN)` for token auth. Token is redacted in `Debug`. **Requires TLS:** a token over plain TCP fails with `Error::AccessTokenRequiresTcps` (**fail closed**). Bead `rust-oracledb-5bh` (delivered). | builder `crates/oracledb/src/lib.rs`; TLS guard and descriptor builders `crates/oracledb/src/lib.rs`; payload `crates/oracledb-protocol/src/thin/connect.rs`, `auth.rs` |
| OCI IAM **instance/resource-principal request signing** (`AUTH_HEADER`/`AUTH_SIGNATURE`) | **Signing implemented + offline-verified; live ADB pending.** `ConnectOptions::with_iam_token(token, private_key_pem)` sends the token as `AUTH_TOKEN` *and* signs a `date`/`(request-target)`/`host` header with the RSA private key (PKCS#1 v1.5 over SHA-256, PKCS#8 or PKCS#1 PEM), sending `AUTH_HEADER` + `AUTH_SIGNATURE` and OR-ing `TNS_AUTH_MODE_IAM_TOKEN` into the auth mode. The private key is wrapped in a redacting `IamPrivateKey` newtype (never in `Debug`/logs/errors). **Requires TLS** — a signed token over plain TCP fails closed with `Error::AccessTokenRequiresTcps` *before any dial*. **Offline evidence:** the signature is pinned byte-for-byte against an `openssl dgst -sha256 -sign` vector and the wire layout by a deterministic cassette. Full server acceptance still needs OCI/ADB infra (not live-exercised). Bead `rust-oracledb-cco`. | `crypto::iam_signature` `crates/oracledb-protocol/src/crypto.rs`; `iam_signing_string` / `append_auth_phase_two_token_iam` `crates/oracledb-protocol/src/thin/auth.rs`; `build_fast_auth_token_payload_iam` `crates/oracledb-protocol/src/thin/connect.rs`; builder + signing + RFC 1123 date `crates/oracledb/src/lib.rs` |

**Not supported in 1.0** (each fails closed with a typed, machine-classifiable error — never a silent fallback):

| mode | status / behaviour | citation |
|---|---|---|
| Unknown / unsupported password verifier type | `ProtocolError::UnsupportedVerifier { verifier_type }` — the verifier `match` has no default/fallback arm. | `crates/oracledb-protocol/src/crypto.rs:54-58`; variant `lib.rs:74-75` |
| Native Network Encryption / Data Integrity (NNE) | If the server requires NNE, connect returns `ProtocolError::UnsupportedFeature("Native Network Encryption and Data Integrity")`. The client also advertises `TNS_NSI_DISABLE_NA`. | `crates/oracledb-protocol/src/thin/connect.rs:24-25,44-48` |
| Kerberos | **Selectable but not implemented.** `ConnectOptions::kerberos_auth` / `with_kerberos_auth` express intent without sending dummy credentials, redact principal/keytab in `Debug`, and return `Error::UnsupportedAuthMode` before network I/O. Real GSSAPI/keytab exchange is deferred. | `AuthMode::Kerberos`, `AuthCapabilities::THIN`, `Error::UnsupportedAuthMode` in `crates/oracledb/src/lib.rs`; bead `rust-oracledb-qm4` |
| RADIUS / native MFA (challenge-response) | **Selectable but not implemented.** `ConnectOptions::radius_auth` / `with_radius_auth` express intent, redact the challenge hint in `Debug`, and return `Error::UnsupportedAuthMode` before network I/O. Real RADIUS/MFA exchange is deferred. | `AuthMode::Radius`, `AuthCapabilities::THIN`, `Error::UnsupportedAuthMode` in `crates/oracledb/src/lib.rs`; bead `rust-oracledb-qm4` |
| External / OS / passwordless auth | **Selectable but not implemented.** `ConnectOptions::external_auth` / `with_external_auth` express passwordless intent without caller-supplied dummy username/password and return `Error::UnsupportedAuthMode` before network I/O. | `AuthMode::External`, `AuthCapabilities::THIN`, `Error::UnsupportedAuthMode` in `crates/oracledb/src/lib.rs`; bead `rust-oracledb-o0b` |

**Fail-closed guarantee (verified):** mode selection is driven solely by the
inputs (`AuthMode` + access-token presence); there is no path that downgrades an
unsupported request to a weaker one. Supporting typed errors:
`Error::UnsupportedAuthMode` for known unsupported modes, missing verifier type
-> `Error::MissingSessionField`, fast-auth not negotiated ->
`Error::FastAuthRequired`, server-response MAC mismatch ->
`ProtocolError::InvalidServerResponse` (`crypto.rs:142-157`).

> The "Not implemented" auth/wallet rows are also called out as out-of-scope for
> 1.0 in the Road-to-1.0 program epic (`rust-oracledb-road-to-1-0-llv`, "Out of
> scope"): Group-A auth (Kerberos, RADIUS/native-MFA, passwordless-external,
> broader wallet — beads `o0b`/`qm4`/`x1p`) is post-1.0.

## 6. Connection string and transport topology

The structured `ConnectOptions` builder is the primary surface for connect
tuning (TLS, DN match, wallet, SNI toggle, access token, proxy user, SDU), but
the DSN `DESCRIPTION` / EZConnect-Plus transport parameters are now **applied to
the live connection**, not merely parsed for diagnostics. Where a requested
parameter genuinely cannot be honored the driver **fails closed** with a typed
error naming it — never a silent ignore. The multi-address / redirect / SDU /
timeout behaviours below were the transport-hardening gaps tracked under bead
`rust-oracledb-clvm` (F1–F4); they are now closed as noted.

| feature | 1.0 behaviour | citation |
|---|---|---|
| Multi-address `ADDRESS_LIST` failover / load-balance (**F2**) | **Supported.** A `DESCRIPTION` with an `ADDRESS_LIST` (or several `ADDRESS` entries) is tried in order until one address establishes a transport; `LOAD_BALANCE=ON` shuffles the order, `RETRY_COUNT`/`RETRY_DELAY` re-scan the set, and `FAILOVER=OFF` restricts a list to its first address. Only transport-establishment errors (TCP dial / TLS handshake) fail over; a config/auth error aborts. If every address fails the per-address reasons are aggregated into `Error::AllAddressesFailed`. The overall (transport connect timeout) deadline is shared across attempts. **Live-proven** (xe18: dead-first→live-second connects; live-first uses the first). | `resolve_connect_addresses` / failover loop in `crates/oracledb/src/lib.rs`; `crates/oracledb/tests/live_transport_failover.rs` |
| Listener `REDIRECT` (RAC / SCAN / shared-server handoff) (**F4**) | **Followed** for a same-transport redirect: the driver reconnects to the redirect target and resends the CONNECT (bounded against redirect loops). A redirect that demands a **transport-protocol change** (e.g. `tcps → tcp`, a silent TLS downgrade) still **fails closed** with `Error::RedirectUnsupported`. | REDIRECT loop in `Connection::connect`, `crates/oracledb/src/lib.rs` |
| DSN `transport_connect_timeout` / `connect_timeout` (**F1**) | **Applied** as the overall connect deadline: TCP dial, TLS, listener ACCEPT, and AUTH reads all share the parsed duration (across failover attempts). A short DSN timeout against a black-hole address times out at that bound instead of hanging on the old hard-coded 20s. Because the thin driver uses a single connect deadline, `CONNECT_TIMEOUT` is honored as an alias of `transport_connect_timeout` — a deliberate footgun-removal *beyond* python-oracledb thin, which drops DESCRIPTION `CONNECT_TIMEOUT`. **Live-proven** (a 2s DSN bound stops a black-hole dial at ~2s, surfacing a bounded transport failure that names the timeout). | `canonical_param_name`, `crates/oracledb-protocol/src/net/connectstring/mod.rs`; `transport_connect_timeout_duration` / `Connection::connect`, `crates/oracledb/src/lib.rs`; `crates/oracledb/tests/live_transport_failover.rs` |
| DSN `SDU` (**F1**) | **Applied.** The SDU advertised in the CONNECT packet honours a DSN `(SDU=...)` (`resolve_effective_sdu`); an explicit `ConnectOptions::with_sdu` wins, else the DSN value, else the 8192 default. Advertised through the classic 16-bit CONNECT-packet field (ceiling 65535; a larger DSN SDU is advertised at 65535), and the server negotiates the effective SDU down in its ACCEPT. Offline-proven (unit test decodes the SDU back out of the built CONNECT packet). | `resolve_effective_sdu`, `crates/oracledb/src/lib.rs` |
| DSN `SECURITY` wallet directory + DN-match/cert-DN (**F1**) | **Applied.** `SSL_SERVER_DN_MATCH` / `SSL_SERVER_CERT_DN` and the DSN wallet directory (`MY_WALLET_DIRECTORY` / `WALLET_LOCATION`) feed the TLS setup; an explicit `ConnectOptions` value wins, else the DSN `SECURITY` value. TCPS descriptors also preserve non-wallet `SECURITY` pass-through keys; token auth injects `(TOKEN_AUTH=OCI_TOKEN)`. Wallet paths remain client-side and are not re-emitted into trace-visible descriptors. Offline-proven (wallet-loader tests); a full live-ADB TCPS acceptance still needs ADB infra (**intended/unverified**, see §2). | `resolve_tls_params` / `Connection::connect`, `crates/oracledb/src/lib.rs` |
| Oracle Server Name Indication (SNI) over TCPS (**F3**) | **Fail closed** when requested-but-un-encodable. `use_sni=true` no longer silently degrades to no-SNI: because the Oracle SNI (`S{len}.{service}.V3.{version}`) ends in an all-numeric label that RFC-strict rustls rejects, `use_sni=true` returns the typed `Error::UnsupportedSni` naming the SNI; reconnect with `use_sni=false` (the default) to rely on the post-handshake Oracle DN match. A real Oracle-format SNI handshake remains a follow-up (limited TCPS test infra). Offline-proven (`decide_sni` unit tests). | `decide_sni` / `tls_handshake`, `crates/oracledb/src/tls.rs` |

These transport-completeness gaps are now implemented (F1/F2 supported, F3
fail-closed, F4 redirect-follow). None ever caused silent data corruption; they
were out of the original W3-E8 correctness-bug scope (protocol/codec/
multi-packet/async) and are closed under bead `rust-oracledb-clvm`.
