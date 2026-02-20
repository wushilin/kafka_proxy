# Kafka SNI Proxy

SNI-based Kafka TLS proxy that:
- Terminates TLS from clients.
- Connects to upstream brokers over TLS.
- Rewrites broker host/port in selected Kafka responses so clients reconnect through the proxy.
- Learns broker routes dynamically from metadata/coordinator responses.

## Features

- Client-side TLS termination (`server.pem`/`key.pem`).
- Optional mTLS for clients (`--mtls-enable`, `--ca-certs`).
- Upstream TLS with verification by:
  - system trust roots (default), or
  - custom CA bundle (`--upstream-ca-certs`), or
  - trust-all mode (`--trust-upstream-certs`, test only).
- Dynamic route learning from:
  - `MetadataResponse`
  - `FindCoordinatorResponse`
  - `DescribeClusterResponse`
- Bootstrap route available by default:
  - `bootstrap.<sni-suffix>` -> first `--upstream`
- Flexible hostname rewrite rule:
  - `--broker-mapping-rule "<regex>::<replacement>"`
  - falls back to `b<id>.<sni-suffix>` if no rule match.
- Optional upstream hosts override file with wildcard support:
  - `--hosts-file <path>`
  - hosts-file match takes priority over DNS; otherwise falls back to system DNS.

## Quick Start

Generate certs in current directory:

```bash
cargo run -- --sni-suffix beatbox.com --generate-certs
```

Run proxy:

```bash
cargo run -- \
  --bind 0.0.0.0:9092 \
  --sni-suffix beatbox.com \
  --upstream lkc-abcde.confluent.cloud:9092
```

With custom upstream CA:

```bash
cargo run -- \
  --sni-suffix beatbox.com \
  --upstream lkc-abcde.confluent.cloud:9092 \
  --upstream-ca-certs ca.pem
```

## Hosts File

Example file:

```text
192.165.22.33 b0.upstream.com *.upstream.com *.abc.upstream.com
```

Resolution rules:
1. Specific hostname wins over wildcard.
2. For wildcards, longer wildcard pattern wins.
3. If still tied, later definition wins.
4. `*` does not match `.`.

## CLI Flags

- `--bind` (default: `0.0.0.0:9092`)
- `--sni-suffix`
- `--cert` (default: `server.pem`)
- `--key` (default: `key.pem`)
- `--upstream` (comma-separated)
- `--mtls-enable` / `--mtls-disable`
- `--ca-certs`
- `--upstream-ca-certs`
- `--trust-upstream-certs`
- `--generate-certs`
- `--broker-mapping-rule`
- `--hosts-file`

## Notes

- mTLS is disabled by default.
- Generated certs are valid for 20 years.
- Rewrite path is selective: non-target APIs stream through without full-frame mutation.
