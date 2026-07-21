# TLS / TCPS (encrypted transport) — setup and operator guide

Wave 5 adds **TCPS** (TLS-over-TCP) support to the Rust Oracle thin driver: the
M3 differentiator that lets the driver connect to TLS-required Oracle
environments (Autonomous Database, hardened on-prem listeners, mTLS-gated DBs).

The transport is a sans-io [`rustls`](https://github.com/rustls/rustls)
`ClientConnection` driven over the asupersync async socket. The crate is pure
safe Rust — `#![forbid(unsafe_code)]` is preserved (rustls + ring are safe).

This document covers:

1. [Client usage](#1-client-usage) — how to open a TCPS connection.
2. [Wallets](#2-wallets) — `ewallet.pem` and `cwallet.sso`.
3. [Server-certificate verification](#3-server-certificate-verification) — the
   Oracle DN/SAN match and the SNI caveat.
4. [Standing up a TCPS Oracle listener](#4-standing-up-a-tcps-oracle-listener) —
   operator infra, and why the bundled gvenzl Free container can't host it.
5. [Test fixtures](#5-test-fixtures) — how the real test wallets/certs were
   generated (reproducible with openssl).

---

## 1. Client usage

A TCPS connection is requested by a `tcps://` EZConnect prefix (default port
**2484**), plus a wallet and the DN-match settings:

```rust
use oracledb::{Connection, ConnectOptions, ClientIdentity};

let options = ConnectOptions::new(
        "tcps://db.example.com:2484/FREEPDB1", // tcps:// => TLS, default port 2484
        "scott", "tiger",
        // program, machine, osuser, terminal, driver — caller-chosen identity
        ClientIdentity::new("myapp", "host", "appuser", "term", "rust-oracledb")?,
    )
    .with_wallet_location("/etc/oracle/wallets/db1") // dir with ewallet.pem
    .with_ssl_server_dn_match(true)                  // default
    // .with_ssl_server_cert_dn("CN=db.example.com,O=Acme,C=US") // optional exact DN
    // .with_wallet_password("…")  // only for an encrypted mTLS key
    // .with_use_sni(true)         // opt-in Oracle SNI (see §3 caveat)
    ;

let conn = Connection::connect(&cx, options).await?;
```

The TLS handshake happens **immediately after the TCP connect and before any TNS
bytes** (implicit TLS, exactly as python-oracledb thin's `_connect_tcp`
ordering). Plain `tcp://` / no-prefix connections are unchanged.

Wallet directory resolution mirrors python-oracledb:

| Precedence | Source |
|---|---|
| 1 | `with_wallet_location(...)` — the explicit dir. `SYSTEM` (case-insensitive) means "use the OS trust store, no wallet file". |
| 2 | the `TNS_ADMIN` environment variable |
| — | if neither is set, the OS root bundle is used (verify against public CAs) |

---

## 2. Wallets

### `ewallet.pem` — fully supported

A single PEM file holding the trust-anchor certificate(s) and, for mutual TLS,
the client certificate chain plus the client private key. This is the format
python-oracledb thin loads (`transport.pyx::create_ssl_context`). The reader
([`oracledb_protocol::tls::wallet`]):

- loads **every** `CERTIFICATE` block as a trust anchor (server verification),
- if an (unencrypted PKCS#8 / PKCS#1 / SEC1) `PRIVATE KEY` block is also
  present, uses the certs + key as the **client identity for mTLS**,
- decrypts a PKCS#8 `ENCRYPTED PRIVATE KEY` block with the supplied
  `wallet_password` (PBES2 / PBKDF2-HMAC-SHA1/SHA256 / AES-CBC — the scheme
  ADB wallet downloads and `openssl pkcs8 -topk8` emit). A missing password
  yields a typed `PasswordRequired` error; a wrong password or unsupported
  scheme (scrypt, legacy `Proc-Type: 4,ENCRYPTED`) yields a typed `KeyDecrypt`
  error — never a silent verify-only downgrade.

A wallet with no private key is **verify-only** (the common server-verification
case) — e.g. a PEM containing just the CA certificate.

### `ewallet.p12` — supported (requires `wallet_password`)

The standard Oracle PKCS#12 wallet — the file `orapki wallet create` produces
and Autonomous Database wallet zips ship. `parse_ewallet_p12` /
`read_ewallet_p12` reuse the internal PFX reader (`tls/pfx.rs`):

- modern **PBES2 / PBKDF2 / AES-CBC** wallets (orapki 19c+/23ai and
  `openssl pkcs12 -export` defaults) are fully parsed: trust-anchor certs,
  client chain, and the (shrouded or plain) private key;
- the wallet password is **required** — without it the loader fails closed
  with a typed `PasswordRequired` remediation (Oracle p12 wallets are always
  password-protected);
- legacy PBE-SHA1-3DES / RC2 wallets return a typed `Pkcs12` error naming the
  unsupported OID (re-export the wallet with a modern cipher).

Proven against a **real `orapki` 23.26-generated wallet** (see §5).

### `cwallet.sso` — supported

The SSO auto-login wallet: a proprietary Oracle binary container wrapping a
standard PKCS#12. Ported from the open-source
[go-ora](https://github.com/sijms/go-ora) prior art
(`v3/configurations/wallet.go`, `wallet_algo.go`).

**Implemented** (and proven against real fixtures, see §5):

- Outer container: magic `A1 F8 4E`, magic version `6`/`7`/`8`, header version
  `6`, and the **`num3 == 6` AES-128-CBC auto-login** sub-type — including the
  auto-login-**local** (`magic version 8`) host/user HMAC-SHA1 re-derivation.
- Inner PKCS#12 with the **PBES2 / PBKDF2 / AES-CBC** scheme (modern AES
  wallets): salt, IV, iteration count, key length and PRF (HMAC-SHA256 /
  HMAC-SHA1) are all read from the wallet. Both unencrypted `keyBag` and
  PBES2-shrouded key bags are decrypted.

**Not implemented** (returns an explicit error — never a silent wrong answer):

- `num3 == 0x35` single-DES and `num3 == 5` no-key outer sub-types,
- inner PKCS#12 PBE-with-SHA-and-3DES (`…12.1.3`) and RC2 schemes.

If a wallet hits an unsupported branch, **convert it to `ewallet.pem`** (which is
fully supported): `orapki wallet pkcs12_to_pem` or export from the wallet tool.

Promoted from the `experimental` feature to always-on in 0.7.x after the reader
was verified against a real `orapki` 23.26-generated `cwallet.sso` whose
extracted certs/keys are byte-identical to its paired `ewallet.p12` (see §5).
The `experimental` cargo feature remains as an empty no-op.

### Which file wins? (loader precedence)

`load_wallet` (`crates/oracledb/src/tls.rs`) picks, in order:

1. `ewallet.pem` — parity with python-oracledb thin, which reads **only** this
   file;
2. `ewallet.p12` **when a `wallet_password` is supplied**;
3. `cwallet.sso` (auto-login, no password);
4. a passwordless `ewallet.p12` → typed `PasswordRequired` error.

So an untouched ADB wallet zip (`cwallet.sso` + `ewallet.p12`, no
`ewallet.pem`) connects with a password (p12 path) *or* without one (sso path).
Note that reading `ewallet.p12`/`cwallet.sso` **exceeds** the pinned
python-oracledb 4.0.1 thin reference; full live acceptance against a real ADB
endpoint is still pending (offline parsing is proven, see `docs/SUPPORT.md`).

---

## 3. Server-certificate verification

python-oracledb thin **disables standard TLS hostname verification**
(`check_hostname = False`) and instead runs its own check after the handshake
(`crypto.pyx::check_server_dn`). The Rust driver reproduces this exactly with a
custom rustls `ServerCertVerifier`
([`oracledb::tls::OracleServerCertVerifier`]):

1. **Chain validation** — the server's leaf is validated to a trust anchor
   (wallet CA, or OS roots) using `rustls-webpki`'s name-unbound
   `EndEntityCert::verify_for_usage`. This is the same crypto rustls uses
   internally, just without binding the SNI/DNS name — mirroring OpenSSL
   `CERT_REQUIRED` + `check_hostname = False`.
2. **Oracle DN / name match** (when `ssl_server_dn_match`, the default):
   - if `ssl_server_cert_dn` is set, the server's subject DN must **equal** it
     (order-independent attribute-map comparison, `crypto.pyx` `DN_REGEX`);
   - otherwise the connect host is matched against the certificate's **SAN DNS
     names** then **common names**, with wildcard support
     (`crypto.pyx::_name_matches`).

### SNI caveat

The Oracle TCPS SNI string is `S{len}.{service}[.T1.{c}].V3.{version}`
(`transport.pyx::_calc_sni_data`; the builder and its tests live in
[`oracledb_protocol::tls::sni`]). In python-oracledb this is **opt-in**
(`use_sni` defaults to `false`); by default no SNI is sent and the server is
identified purely by the DN match.

rustls's `ServerName` is RFC-strict and **rejects** the Oracle service-form SNI
because it ends in an all-numeric label (`.V3.319`) — a DNS name cannot end in
an all-numeric label (RFC 1123 / the IP-address ambiguity rule) — and ADB service
names also contain underscores. Consequently this driver:

- **`use_sni = false` (default):** no SNI is sent; the DN match secures the
  connection. This is the normal, fully-working path (and matches both the
  driver default and python-oracledb's default).
- **`use_sni = true` on an OCI Autonomous Database descriptor:** the service-form
  token is not rustls-encodable, so the driver sends the **listener host** as
  SNI instead (`is_oci_adb_endpoint` / `decide_sni` in `crates/oracledb/src/tls.rs`).
  Covered host shapes: shared LB `adb.<region>.oraclecloud.com`, private-endpoint
  `<label>.adb.<region>.oraclecloud.com`, and the same patterns under sovereign
  suffixes (`.oraclecloud.eu`, `.oraclegovcloud.com`, `.oraclecloud.com.au`).
  The post-handshake Oracle DN/name match remains authoritative. Host-as-SNI
  completes the handshake but does **not** trigger Oracle's one-negotiation
  routing fast-path (a documented performance nuance; see
  `docs/PARITY_LEDGER.md` entry `sni-service-form`).
- **`use_sni = true` on any other descriptor** whose service-form SNI is not a
  valid rustls DNS name: **fails closed** with typed `Error::UnsupportedSni`
  naming the token. There is **no** silent downgrade to no-SNI (that was the
  pre-F3 behavior and is deliberately gone). Reconnect with `use_sni=false` to
  rely on the DN match alone.

True service-form parity with python-oracledb is impossible without patching
rustls so it can carry Oracle's non-DNS SNI token.

---

## 4. Standing up a TCPS Oracle listener

A real TCPS endpoint requires the **Oracle listener** to be configured with a
**server wallet** (an Oracle PKCS#12 wallet, `ewallet.p12` + auto-login
`cwallet.sso`) created by **`orapki`**, plus `listener.ora` / `sqlnet.ora`
endpoints on a TCPS port (conventionally **2484**).

### Operator steps (on a host with a full Oracle install / `orapki`)

```bash
# 1. Create a server wallet with an auto-login (cwallet.sso) component.
orapki wallet create -wallet /opt/oracle/tls_wallet -auto_login -pwd "WalletPw1"

# 2. Add a self-signed server cert (or a CA-issued one). CN/SAN must match the
#    value the client will DN-match against (the connect HOST, or the explicit
#    ssl_server_cert_dn).
orapki wallet add -wallet /opt/oracle/tls_wallet -dn "CN=db.example.com" \
    -keysize 2048 -self_signed -validity 3650 -pwd "WalletPw1"

# 3. Export the cert for the CLIENT wallet (ewallet.pem) so the Rust driver
#    trusts it:
orapki wallet export -wallet /opt/oracle/tls_wallet \
    -dn "CN=db.example.com" -cert /opt/oracle/tls_wallet/server.crt
#    Convert the wallet to PEM for the client (verify-only):
orapki wallet pkcs12_to_pem -wallet /opt/oracle/tls_wallet -pwd "WalletPw1" \
    > /etc/oracle/wallets/db1/ewallet.pem   # client uses just the cert block
```

Append a TCPS endpoint + the wallet location to the network files (preserve the
existing TCP endpoint):

```ini
# listener.ora — add a TCPS address to the existing DESCRIPTION
LISTENER =
  (DESCRIPTION_LIST =
    (DESCRIPTION =
      (ADDRESS = (PROTOCOL = TCP)(HOST = 0.0.0.0)(PORT = 1521))
      (ADDRESS = (PROTOCOL = TCPS)(HOST = 0.0.0.0)(PORT = 2484)))) # <-- new
WALLET_LOCATION =
  (SOURCE = (METHOD = FILE)(METHOD_DATA = (DIRECTORY = /opt/oracle/tls_wallet)))
SSL_CLIENT_AUTHENTICATION = FALSE   # TRUE for mTLS

# sqlnet.ora
WALLET_LOCATION =
  (SOURCE = (METHOD = FILE)(METHOD_DATA = (DIRECTORY = /opt/oracle/tls_wallet)))
SSL_CLIENT_AUTHENTICATION = FALSE
SSL_VERSION = 1.2 or 1.3
```

Then reload the listener:

```bash
lsnrctl reload     # or: lsnrctl stop && lsnrctl start
lsnrctl status     # confirm a (PROTOCOL=tcps)(PORT=2484) endpoint is listed
```

### Why the bundled gvenzl Free container can't host TCPS

The lane's test container (`rust-oracledb-lane-1523`, Oracle 26ai **Free** /
gvenzl image) **cannot** be configured as a TCPS server within reason:

- it ships **no `orapki`-usable Java**, **no `openssl`**, and **no `mkstore`**,
  so an Oracle server wallet (`ewallet.p12` + `cwallet.sso`) cannot be created
  inside it;
- the host's JDK can't be injected either — the container's glibc (2.28) is
  older than the host JDK requires (≥ 2.34), so `orapki` cannot run;
- the container publishes only the TCP port; a TCPS port would need a new
  published mapping (recreating the container, which the lane forbids).

This is an **operator-infra** requirement, not a driver limitation: the driver's
TCPS client path is fully implemented and proven (see §5 and the test suite).
To exercise an end-to-end TCPS connect against a real Oracle DB, point the
driver at a listener configured with the operator steps above (Autonomous
Database, or any on-prem 19c+/23ai/26ai with a TCPS endpoint).

> Note: the host *can* reach the lane container directly on its bridge IP
> (`172.17.0.2`) on any port, so if a future image includes `orapki`/Java the
> end-to-end host-side TCPS test in `tests/` can be un-skipped without a new
> port mapping.

---

## 5. Test fixtures

The TLS code path is proven with **real** fixtures and **real** crypto — not
mocks of our own code. Fixtures live in
`crates/oracledb/tests/fixtures/tls/` and were generated with openssl 3.x:

```bash
# Self-signed server cert + key, and the combined ewallet.pem (cert + key):
openssl req -x509 -newkey rsa:2048 -keyout server.key -out server.crt -days 3650 \
  -nodes -subj "/C=US/O=ExampleDB/CN=db.example.com" \
  -addext "subjectAltName=DNS:db.example.com,DNS:localhost,IP:127.0.0.1"
cat server.crt server.key > ewallet.pem

# A CA + a CA-signed leaf (the realistic verify-against-CA case):
openssl req -x509 -newkey rsa:2048 -keyout ca.key -out ca.crt -days 3650 -nodes \
  -subj "/C=US/O=ExampleDB CA/CN=ExampleDB Root CA" \
  -addext "basicConstraints=critical,CA:TRUE"
openssl req -newkey rsa:2048 -keyout leaf.key -out leaf.csr -nodes \
  -subj "/C=US/O=ExampleDB/CN=db.example.com"
openssl x509 -req -in leaf.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 3650 \
  -extfile <(printf "subjectAltName=DNS:db.example.com,DNS:localhost,IP:127.0.0.1\nbasicConstraints=CA:FALSE") \
  -out leaf.crt
cp ca.crt ca_wallet.pem   # verify-only client wallet (trusts the CA)

# A PKCS#12 (PBES2/PBKDF2/AES-256-CBC) wrapped in a real cwallet.sso container.
# (16-byte password so the SSO no-pad AES password block round-trips; the
#  Python wrapper in the fixture-build notes emits the A1F84E/ver6/num3=6 outer
#  container — see the test commit message and tests/fixtures/tls/.)
openssl pkcs12 -export -out wallet_oracle.p12 -inkey leaf.key -in leaf.crt \
  -certfile ca.crt -name oracle-test -passout pass:OracleWallet1234 \
  -keypbe NONE -certpbe AES-256-CBC -macalg sha256

# Encrypted-key ewallet.pem variants (password WalletPassword16), the ADB
# ewallet.pem shape: leaf.crt + ca.crt + an ENCRYPTED PRIVATE KEY block.
openssl pkcs8 -topk8 -in leaf.key -passout pass:WalletPassword16 \
  -v2 aes-256-cbc -v2prf hmacWithSHA256   # -> ewallet_encrypted.pem
openssl pkcs8 -topk8 -in leaf.key -passout pass:WalletPassword16 \
  -v2 aes-128-cbc -v2prf hmacWithSHA1     # -> ewallet_encrypted_sha1.pem
openssl pkcs8 -topk8 -in leaf.key -passout pass:WalletPassword16 \
  -scrypt                                 # -> ewallet_encrypted_scrypt.pem (typed-unsupported test)
openssl rsa -in leaf.key -aes256 -traditional \
  -passout pass:WalletPassword16          # -> ewallet_encrypted_legacy.pem (typed-unsupported test)

# Standalone PKCS#12 wallet, OpenSSL 3 defaults (PBES2/AES-256-CBC shrouded key):
openssl pkcs12 -export -out ewallet_openssl.p12 -inkey leaf.key -in leaf.crt \
  -certfile ca.crt -name oracle-test -passout pass:WalletPassword16
```

Additionally, `ewallet_orapki.p12` + `cwallet_orapki.sso` are a **genuine
Oracle-tooling wallet** (password `WalletPass123`, self-signed lab-only
`CN=db.example.com` content) generated with the `oraclepki` 23.26.2.0.0 jar
from Maven Central (`orapki` is a thin wrapper around it):

```bash
java -cp oraclepki-23.26.2.0.0.jar oracle.security.pki.textui.OraclePKITextUI \
  wallet create -wallet . -pwd "WalletPass123" -auto_login
java -cp oraclepki-23.26.2.0.0.jar oracle.security.pki.textui.OraclePKITextUI \
  wallet add -wallet . -pwd "WalletPass123" \
  -dn "CN=db.example.com,O=ExampleDB,C=US" -keysize 2048 -self_signed -validity 3650
```

The tests that consume them:

| Test file | What it proves |
|---|---|
| `crates/oracledb-protocol/tests/tls_wallet.rs` | `ewallet.pem` parse (cert+key, verify-only), SNI string format, DN accept/reject, SAN/wildcard match, wallet-dir precedence, and **end-to-end `cwallet.sso` parse** for both unencrypted-keyBag and PBES2-shrouded-key wallets. |
| `crates/oracledb/tests/tls_handshake.rs` | A **real rustls handshake** against a blocking rustls server presenting the CA-signed leaf: handshake succeeds with a CA wallet + SAN name match, **rejects** on a DN/name mismatch, **accepts** an explicit matching `ssl_server_cert_dn`, and **data round-trips** through the `OracleReadHalf`/`OracleWriteHalf` transport. |

These run as part of `cargo test --workspace`. The end-to-end Oracle-server TCPS
test self-skips when no TCPS endpoint is reachable (see §4).

### Synthetic throwaway wallets (`fixtures/tls/synthetic/`)

For **0.7.3 / C1** offline coverage with a fictional DN (`CN=oracle-test.invalid`),
regenerate committed artifacts with:

```bash
./scripts/gen_test_wallets.sh
# optional: SYNTHETIC_WALLET_PASSWORD='oracle-test-wallet-16' ./scripts/gen_test_wallets.sh
```

Produces `ewallet.pem`, `ewallet_encrypted.pem`, `ewallet.p12`, and
`ewallet_3des_openssl.p12` (OpenSSL 3 **legacy** provider required for 3DES).
See `crates/oracledb/tests/fixtures/tls/synthetic/PROVENANCE.md` for commands.
`cwallet.sso` is not minted here; SSO auto-login is covered by the committed
`cwallet_orapki.sso` / `cwallet.sso` fixtures in the parent `tls/` directory.
`crates/oracledb-protocol/tests/tls_wallet.rs` runs `synthetic_*` decrypt tests
against the synthetic tree.
