# Synthetic TLS wallet fixtures (lab-only)

Fictional subject `CN=oracle-test.invalid, O=Oracle Synthetic Test, C=US`.
Password for encrypted artifacts: `oracle-test-wallet-16` (16 bytes, SSO-friendly).

**Do not use in production.** Committed bytes are authoritative; PKCS#12 salt/IV vary per
generation, so tests assert decrypt semantics (key/cert/DN), not byte identity.

## Regenerate

```bash
./scripts/gen_test_wallets.sh
# optional override:
SYNTHETIC_WALLET_PASSWORD='oracle-test-wallet-16' ./scripts/gen_test_wallets.sh
```

Requires **OpenSSL 3.x** with the **legacy** provider enabled for `ewallet_3des_openssl.p12`.

## Commands (OpenSSL 3.5.5 validated)

| Artifact | Command |
|----------|---------|
| `ca.pem` | `openssl req -new -x509 -days 3650 -key ca.key -out ca.pem -subj "/CN=oracle-test.invalid/..."` |
| `ewallet.pem` | `cat server.pem server.key` |
| `ewallet_encrypted.pem` | `openssl rsa -aes256 -passout pass:oracle-test-wallet-16` + concat with cert |
| `ewallet.p12` | `openssl pkcs12 -export -inkey server.key -in server.pem -certfile ca.pem -passout pass:oracle-test-wallet-16` |
| `ewallet_3des_openssl.p12` | `openssl pkcs12 -export -legacy -certpbe PBE-SHA1-3DES -keypbe PBE-SHA1-3DES ...` (OID `1.2.840.113549.1.12.1.3`) |

## `cwallet.sso`

Not generated here (`orapki` unavailable). Auto-login SSO coverage uses the committed
`../cwallet_orapki.sso` fixture (see `docs/TLS_SETUP.md`).

## Tests

`crates/oracledb-protocol/tests/tls_wallet.rs` — `synthetic_*` tests. Legacy
`ewallet_3des_openssl.p12` asserts typed `Pkcs12` failure until **A2.1** adds decrypt.