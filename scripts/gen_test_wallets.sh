#!/usr/bin/env bash
# Generate throwaway TLS wallet fixtures for offline wallet tests (C1 / 0.7.3).
# Fictional DN only — never use in production.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${ROOT}/crates/oracledb/tests/fixtures/tls/synthetic"
PASSWORD="${SYNTHETIC_WALLET_PASSWORD:-oracle-test-wallet-16}"
SUBJ="/CN=oracle-test.invalid/O=Oracle Synthetic Test/C=US"
DAYS=3650

mkdir -p "${OUT}"
cd "${OUT}"

echo "Generating synthetic wallets in ${OUT}"
echo "Subject: ${SUBJ}"

# CA + server cert (same pattern as TLS_SETUP.md, fictional DN).
openssl genrsa -out ca.key 2048 2>/dev/null
openssl req -new -x509 -days "${DAYS}" -key ca.key -out ca.pem -subj "${SUBJ}" 2>/dev/null

openssl genrsa -out server.key 2048 2>/dev/null
openssl req -new -key server.key -out server.csr -subj "${SUBJ}" 2>/dev/null
openssl x509 -req -days "${DAYS}" -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out server.pem 2>/dev/null

# Plaintext PEM wallet (cert + key, no encryption).
cat server.pem server.key > ewallet.pem

# Encrypted PEM (OpenSSL 3.x: PBES2/PBKDF2 on the private key).
openssl rsa -aes256 -passout pass:"${PASSWORD}" -in server.key -out server_encrypted.key 2>/dev/null
cat server.pem server_encrypted.key > ewallet_encrypted.pem
rm -f server_encrypted.key

# Modern PKCS#12 (default PBES2/PBKDF2/AES-CBC on OpenSSL 3.x).
openssl pkcs12 -export \
  -inkey server.key -in server.pem -certfile ca.pem \
  -out ewallet.p12 -passout pass:"${PASSWORD}" \
  -name oracle-test-invalid 2>/dev/null

# Legacy 3DES PKCS#12 (OID 1.2.840.113549.1.12.1.3) — requires -legacy on OpenSSL 3.x.
openssl pkcs12 -export -legacy \
  -certpbe PBE-SHA1-3DES -keypbe PBE-SHA1-3DES \
  -inkey server.key -in server.pem -certfile ca.pem \
  -out ewallet_3des_openssl.p12 -passout pass:"${PASSWORD}" \
  -name oracle-test-invalid-3des 2>/dev/null

# Cleanup intermediate secrets (committed fixtures retain only wallet artifacts).
rm -f ca.key server.key server.csr server.pem server.csr ca.srl 2>/dev/null || true

echo "Done. Artifacts:"
ls -la "${OUT}"