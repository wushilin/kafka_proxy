# Kafka Proxy - Design Document

## Overview

Kafka Proxy is a config-driven Layer-7 Kafka proxy written in Rust. It accepts downstream client connections, forwards requests to upstream brokers, and selectively rewrites broker/coordinator addresses in Kafka responses so clients can continue routing through the proxy.

Unlike a pure TCP tunnel, it is protocol-aware for specific response types and transport-aware for TLS/SASL on both downstream and upstream paths.

## Goals

- Provide stable downstream endpoints for Kafka clients while upstream broker topology can change.
- Support multiple downstream/upstream security combinations:
  - `PLAINTEXT`
  - `SASL_PLAINTEXT`
  - `SSL`
  - `SASL_SSL`
- Keep routing deterministic and fail fast on mapping inconsistencies.
- Keep runtime lightweight with async streaming and minimal rewrite scope.

## Non-Goals

- Full Kafka message inspection/filtering across all APIs.
- Generic load-balancing policy engine.
- Automatic upstream cluster management/orchestration.

## Runtime Architecture

### High-Level Flow

1. Client connects to proxy (TLS or plaintext downstream).
2. Proxy resolves an upstream target route.
3. Proxy optionally performs auth bootstrap (`auth_swap`) before normal forwarding.
4. Bidirectional frame forwarding runs concurrently.
5. Upstream responses are rewritten only for selected APIs:
   - `Metadata`
   - `FindCoordinator`
   - `DescribeCluster`
6. Rewrites also feed route-learning state for future connections.

### Core Components

- `src/lib.rs`
  - Startup, config load, protocol/security mode decisions, listener setup.
- `src/proxy.rs`
  - Connection lifecycle, ingress route resolution, forwarding loops, auth bootstrap.
- `src/rewrite.rs`
  - Kafka response decode/rewrite/re-encode and route discovery updates.
- `src/auth.rs`
  - Auth swap model, principal mapping, credential resolution.
- `src/hosts.rs`
  - Config-based hostname-to-IP overrides (`resolve` entries).
- `src/config.rs`
  - YAML schema and environment variable expansion.

## Configuration Model

Runtime behavior is primarily controlled through `config.yaml`:

- `bind_address`
- `upstream`
  - `bootstrap_servers`
  - `security_protocol`
  - `sasl_mechanism`
  - `tls.ca_certs`
  - `tls.trust_server_certs`
- `downstream`
  - `security_protocol`
  - `sasl_mechanism`
  - `tls.mtls_enabled`
  - `tls.cert`, `tls.key`, `tls.ca_certs`
- `broker_mapping`
  - `mode`: `sni` or `port_offset`
  - mode-specific config
- `auth_swap`
- `resolve`

CLI is intentionally small (`src/cli.rs`):
- `--config` (default `config.yaml`)
- `--generate-certs`
- `--save-ca-certs`

## Mapping Modes

### 1) `sni` Mode

Use when you want a single downstream listener port and hostname-based routing.

- Requires downstream TLS, because routing key comes from SNI.
- Uses:
  - `broker_mapping.sni.bind_port`
  - `broker_mapping.sni.upstream` (regex)
  - `broker_mapping.sni.downstream` (template with capture groups and `<$id>`)
- Deterministic behavior:
  - A broker id maps to one stable downstream endpoint.
  - Mapping collisions/inconsistencies are treated as fatal.
- Pre-learning behavior:
  - Unknown SNI hostnames can route to default upstream bootstrap until routes are learned.

### 2) `port_offset` Mode

Use when you want deterministic hostname+port routing (works well for plaintext clients and simpler networking).

- Uses:
  - `broker_mapping.port_offset.hostname`
  - `broker_mapping.port_offset.base`
  - `broker_mapping.port_offset.max_broker_id` (default 400)
- Deterministic formula:
  - Reserved bootstrap slots: `base..base+19`
  - Broker endpoint: `base + 20 + broker_id`
- Startup pre-seeding:
  - Bootstrap slots map to first upstream bootstrap servers (up to 20).
- Guardrails:
  - Negative/too-large broker ids are rejected.
  - Port range violations and endpoint collisions are fatal.

## Rewrite and Route Learning

The proxy rewrites only the broker/coordinator location fields in selected responses:

- `MetadataResponse`
- `FindCoordinatorResponse`
- `DescribeClusterResponse`

Behavior:

- Decode response by API/version.
- Compute downstream advertised endpoint per broker/coordinator using mapping mode.
- Rewrite host/port fields.
- Record discovered upstream routes (`b<id>` and mode-specific keys).
- Re-encode response payload.

If API/version is unsupported for rewrite, payload is forwarded unchanged.

For `port_offset`:

- Full topology snapshots (`Metadata`, `DescribeCluster`) can replace existing broker-id assignments.
- Incremental updates (`FindCoordinator`) can add/update assignments without full replacement.

## Security and Auth Model

### Downstream

- TLS optional (depends on `downstream.security_protocol`).
- mTLS optional (`downstream.tls.mtls_enabled`).
- SASL optional, mechanisms supported include:
  - `PLAIN`
  - `SCRAM-SHA-256`
  - `SCRAM-SHA-512`

### Upstream

- TLS optional (depends on `upstream.security_protocol`).
- Upstream TLS verification options:
  - system roots
  - custom CA bundle
  - trust-all (`upstream.tls.trust_server_certs: true`)
- SASL optional, mechanisms supported include:
  - `PLAIN`
  - `SCRAM-SHA-256`
  - `SCRAM-SHA-512`

### Auth Swap

`auth_swap` maps downstream identity to upstream credentials.

- Enabled only when configured and meaningful for protocol mix.
- If both downstream and upstream are non-SASL, proxy uses auth passthrough mode and ignores auth swap.
- Supports downstream credential validation and principal mapping.
- Supports upstream SASL bootstrap (`PLAIN`/`SCRAM`) using mapped principal+secret.

Note: In current config schema, `auth_swap.upstream_credentials` is required when `auth_swap` block is present, even if `enabled: false`.

## DNS Resolution Overrides (`resolve`)

`resolve` allows host-to-IP overrides from config (exact host and wildcard patterns). Resolution precedence:

1. Exact match over wildcard.
2. More specific wildcard over less specific wildcard.
3. Later rule over earlier rule when ties remain.

If no rule matches, normal system DNS is used.

## Failure Semantics and Determinism

The proxy intentionally fails fast on mapping corruption to avoid silently misrouting Kafka traffic. Examples:

- Two broker ids mapping to same downstream endpoint.
- A broker id changing to a different downstream endpoint in deterministic mode.
- Computed port outside valid range.
- Broker id outside configured `max_broker_id` in `port_offset`.

This favors correctness and predictability over partial availability in invalid states.

## Performance Considerations

- Async I/O on Tokio.
- Bidirectional streaming forwarding with minimal copies on passthrough paths.
- Rewrite work is constrained to selected API responses.
- Lightweight per-connection control path with periodic stats logging.

## Current Limitations

- Rewrite scope is intentionally limited to selected APIs.
- No dedicated admin/metrics HTTP endpoint.
- No dynamic external control plane; behavior is config- and protocol-discovery-driven.
