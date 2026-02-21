# Kafka Proxy
A config-driven Kafka proxy written in Rust.

## Highlights
- Config-file driven runtime (`config.yaml` by default).
- Selective Kafka response rewrite for `Metadata`, `FindCoordinator`, and `DescribeCluster`.
- Supports downstream: `PLAINTEXT`, `SASL_PLAINTEXT`, `SSL`, `SASL_SSL`.
- Supports upstream: `PLAINTEXT`, `SASL_PLAINTEXT`, `SSL`, `SASL_SSL`.
- Optional auth swap between downstream identity and upstream SASL credentials.

## Mapping Modes
- `sni`
  - Single TLS listener.
  - Host rewrite via regex/template (`broker_mapping.sni.upstream`/`broker_mapping.sni.downstream`).
  - Unknown pre-learn SNI routes to the default upstream bootstrap.
- `port_offset`
  - Port-based downstream mapping.
  - Reserved bootstrap window at `base..base+19`.
  - Broker listeners begin at `base+20+broker_id`.
  - Topology updates reconcile listeners/routes.

## Security and Auth
- Downstream TLS termination with `server.pem` + `server_key.pem`.
- Optional downstream mTLS.
- Upstream TLS options:
  - system CA (default),
  - custom CA (`upstream.tls.ca_certs`),
  - trust-all (`upstream.tls.trust_server_certs: true`).
- Downstream SASL mechanisms:
  - `PLAIN`
  - `SCRAM-256` / `SCRAM-SHA-256`
  - `SCRAM-512` / `SCRAM-SHA-512`
- Upstream SASL mechanisms:
  - `PLAIN`
  - `SCRAM-256` / `SCRAM-SHA-256`
  - `SCRAM-512` / `SCRAM-SHA-512`
- For TLS-only downstream auth identity:
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
- `07-client-plaintext-noauth_upstream-plaintext-noauth.yaml`
