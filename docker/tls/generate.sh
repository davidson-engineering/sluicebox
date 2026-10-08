#!/usr/bin/env bash
# Generate a throwaway CA and server certificate for the TLS test servers.
# Usage: docker/tls/generate.sh   (idempotent; regenerates only missing files)
set -euo pipefail
cd "$(dirname "$0")"
if [[ -f ca.crt && -f server.crt && -f server.key && -f client.crt ]]; then
  exit 0
fi
# CA extensions are required: Python 3.13+ verifies with VERIFY_X509_STRICT.
CA_EXT=(-addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign")
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=influxkit test CA" "${CA_EXT[@]}" \
  -keyout ca.key -out ca.crt 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" -keyout server.key -out server.csr 2>/dev/null
cat > server.ext <<'EXT'
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost, IP:127.0.0.1, IP:::1, DNS:influxdb2-tls, DNS:influxdb3-tls
authorityKeyIdentifier = keyid,issuer
subjectKeyIdentifier = hash
EXT
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 3650 \
  -extfile server.ext -out server.crt 2>/dev/null
# A client certificate for mutual TLS (nginx requires it on its mTLS port).
openssl req -newkey rsa:2048 -nodes -subj "/CN=influxkit test client" -keyout client.key -out client.csr 2>/dev/null
cat > client.ext <<'EXT'
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
authorityKeyIdentifier = keyid,issuer
subjectKeyIdentifier = hash
EXT
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 3650 \
  -extfile client.ext -out client.crt 2>/dev/null
# A second, unrelated CA: certificates from it must be rejected.
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=influxkit wrong CA" "${CA_EXT[@]}" \
  -keyout wrong-ca.key -out wrong-ca.crt 2>/dev/null
rm -f server.csr server.ext client.csr client.ext ca.srl
chmod 644 server.key client.key   # read by the non-root users inside the containers
echo "generated test certificates in $(pwd)"
