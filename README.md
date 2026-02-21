# Kafka Proxy
High-performance Kafka proxy focused on low CPU/memory overhead.

## Highlights
- Zero-copy streaming for non-rewritten response paths.
- Selective Kafka response rewrite for:
  - `Metadata`
  - `FindCoordinator`
  - `DescribeCluster`
- Config-file driven runtime (`config.yaml` by default).
- Supports downstream:
  - `PLAINTEXT`
  - `SASL_PLAINTEXT`
  - `SSL`
  - `SASL_SSL`
- Supports upstream:
  - `PLAINTEXT`
  - `SASL_PLAINTEXT`
  - `SSL`
  - `SASL_SSL`
- Auth swap between downstream identity and upstream SASL credentials.

## Mapping Modes
- `sni`:
  - Single TLS listener.
  - Host rewrite via regex/template (`upstream`/`downstream` rule).
  - Unknown pre-learn SNI routes to default upstream bootstrap.
- `port_offset`:
  - Port-based downstream mapping.
  - Reserved bootstrap window at `base..base+19`.
  - Broker listeners begin at `base+20+broker_id`.
  - Full topology snapshots reconcile listeners/routes (add/remove).

## Security and Auth
- Downstream TLS termination with `server.pem` + `server_key.pem`.
- Optional downstream mTLS.
- Upstream TLS:
  - system CA (default), or
  - custom CA (`upstream.tls.ca_certs`), or
  - trust-all (`upstream.tls.trust_server_certs: true`).
- Downstream SASL mechanisms:
  - `PLAIN`
  - `SCRAM-256` / `SCRAM-SHA-256`
  - `SCRAM-512` / `SCRAM-SHA-512`
- Upstream SASL mechanisms:
  - `PLAIN`
  - `SCRAM-256` / `SCRAM-SHA-256`
  - `SCRAM-512` / `SCRAM-SHA-512`
- SSL-only downstream auth-swap identity:
  - one-way TLS => `$anonymous`
  - mTLS => certificate `CN`

## Cert Utilities
Generate certs:
```bash
cargo run -- --generate-certs --sni-suffix beatbox.com
```
Generated files:
- `ca.pem`
- `server.pem`
- `server_key.pem`
- `client1.pem`
- `client1_key.pem`
- `client2.pem`
- `client2_key.pem`

Save issuer cert from upstream server:
```bash
cargo run -- --save-ca-certs --server pkc-xxxxx.region.aws.confluent.cloud:9092
```
Writes `server_ca.pem`.

## Config
Run with config file:
```bash
cargo run -- --config config.yaml
```
`--config` defaults to `config.yaml`.

Environment variable expansion is supported inside YAML:
- `${VAR}`: required, non-empty.
- `${VAR:default}`: fallback to `default` when undefined/empty.

## Templates
See `config-templates/`:
- `01-client-plaintext-noauth_upstream-saslssl-plain.yaml`
- `02-client-saslssl-scram_upstream-saslssl-plain.yaml`
- `03-client-saslplaintext-plain_upstream-saslssl-plain.yaml`
- `04-client-mtls-with-sasl-scram256-server-saslssl-plain.yaml`
- `05-client-ssl-onewaytls_upstream-saslssl-plain.yaml`
- `06-client-ssl-mtls_upstream-saslssl-plain.yaml`
