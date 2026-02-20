# Kafka SNI Proxy - Design Document

## Overview

A lightweight TLS proxy for Kafka that uses Server Name Indication (SNI) to route client connections to backend Kafka brokers. The proxy terminates TLS connections from clients and forwards traffic to upstream brokers based on the SNI hostname.

## Problem Statement

Kafka clients need to connect to multiple brokers in a cluster. When using TLS with load balancers or proxies, clients must be able to specify which broker they want to reach. This proxy solves this by using the SNI field in the TLS handshake to determine the target broker.

## Architecture

```
Client (TLS) --[SNI: b1.kafka.abc.com]--> Proxy --[Plaintext]--> b1.upstream.com:9092
Client (TLS) --[SNI: b2.kafka.abc.com]--> Proxy --[Plaintext]--> b2.upstream.com:9092
Client (TLS) --[SNI: b3.kafka.abc.com]--> Proxy --[Plaintext]--> b3.upstream.com:9092
```

### Components

1. **TLS Acceptor**: Terminates TLS connections from Kafka clients
2. **SNI Router**: Extracts broker ID from SNI and maps to upstream broker
3. **Transparent Proxy**: Bidirectionally forwards all traffic without modification

## SNI Mapping

The proxy uses a predictable naming pattern:

- **Client SNI**: `b<broker-id>.<sni-suffix>`
  - Example: `b1.kafka.abc.com`, `b2.kafka.abc.com`, `b3.kafka.abc.com`
  
- **Upstream**: `b<broker-id>.upstream.com:9092`
  - Example: `b1.upstream.com:9092`, `b2.upstream.com:9092`

The broker ID (`b1`, `b2`, etc.) is extracted from the SNI and used to look up the corresponding upstream broker address.

## Features

### TLS Termination
- Accepts TLS connections from clients
- Configurable server certificate and private key
- Supports standard TLS 1.2 and 1.3

### Mutual TLS (mTLS)
- Optional client certificate verification
- Configurable CA certificates for client validation
- Can be enabled/disabled via command-line flags

### Transparent Proxying
- All Kafka protocol traffic is forwarded unchanged
- SASL authentication is passed through to upstream brokers
- No protocol parsing or modification

### Async I/O
- Built on Tokio for high-performance async networking
- Concurrent handling of multiple client connections
- Bidirectional streaming with zero-copy where possible

## Configuration

The proxy is configured via command-line arguments:

| Flag | Description | Example |
|------|-------------|---------|
| `--bind` | Listen address and port | `0.0.0.0:9092` |
| `--sni-suffix` | Expected SNI suffix for broker identification | `kafka.abc.com` |
| `--cert` | Server TLS certificate | `cert.pem` |
| `--key` | Server private key | `key.pem` |
| `--ca-certs` | CA certificates for mTLS (optional) | `ca.pem` |
| `--mtls-enable` | Enable mutual TLS | |
| `--mtls-disable` | Disable mutual TLS (default) | |
| `--upstream` | Comma-separated list of upstream brokers | `b1.upstream.com:9092,b2.upstream.com:9092` |

## Use Cases

1. **Multi-Region Kafka**: Route clients to geographically distributed Kafka brokers
2. **Security Layer**: Add TLS encryption to legacy Kafka clusters
3. **mTLS Gateway**: Enforce client certificate authentication at the edge
4. **Load Balancer Alternative**: Simple SNI-based routing without complex load balancer configurations

## Limitations

- Upstream connections are plaintext (no TLS to backend)
- No protocol-aware features (message inspection, filtering, etc.)
- No connection pooling or keep-alive optimization
- Broker mapping must be configured at startup

## Future Enhancements

- TLS connections to upstream brokers
- Dynamic upstream configuration
- Metrics and monitoring endpoints
- Connection pooling for upstream brokers
- Health checking of upstream brokers
