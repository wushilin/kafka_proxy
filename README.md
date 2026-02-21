# Kafka Proxy
A config-driven Kafka proxy written in Rust.

## Performance Snapshot
The proxy can sustain ~1.0-1.17 GiB/s in a local benchmark while using about 24-25% of one CPU core and ~5.6 MiB memory.

**It saves about 70% CPU and 70% less memory compared to the golang variant.**

Benchmark setup: Kafka backend on RAM disk.

Producer perf test:
```bash
root@titan /o/test [SIGINT]# kafka-producer-perf-test --topic perf-test --throughput -1 --num-records 9999999999999 --payload-file payloads.txt --producer.config client.properties
Reading payloads from: /opt/test/payloads.txt
Number of messages read: 10000
3832823 records sent, 766564.6 records/sec (1000.08 MB/sec), 27.8 ms avg latency, 270.0 ms max latency.
4423225 records sent, 884645.0 records/sec (1154.13 MB/sec), 26.7 ms avg latency, 76.0 ms max latency.
4512400 records sent, 902480.0 records/sec (1177.40 MB/sec), 26.2 ms avg latency, 78.0 ms max latency.
```

CPU usage sample:
```bash
top - 23:09:58 up 12 days, 16:47, 14 users,  load average: 2.29, 1.45, 0.89
Tasks:   1 total,   0 running,   1 sleeping,   0 stopped,   0 zombie
%Cpu(s): 14.9 us,  8.9 sy,  0.0 ni, 74.7 id,  0.0 wa,  0.2 hi,  1.3 si,  0.0 st
MiB Mem :  63863.8 total,  12594.5 free,  46874.1 used,  36270.3 buff/cache
MiB Swap:  65536.0 total,  65349.8 free,    186.2 used.  16989.8 avail Mem

    PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
1660465 root      20   0   37620   5624   2572 S  24.0   0.0   0:23.73 kafka_proxy-0.1
```

Memory usage sample:

```bash
Every 2.0s: cat /proc/1660465/status | grep -i rss                                                                                                                                                                                               titan: Sat Feb 21 23:12:18 2026

VmRSS:      5636 kB
RssAnon:            3064 kB
RssFile:            2572 kB
RssShmem:              0 kB
```

## Highlights
- Config-file driven runtime (`config.yaml` by default).
- Selective Kafka response rewrite for `Metadata`, `FindCoordinator`, and `DescribeCluster`.
- Supports downstream: `PLAINTEXT`, `SASL_PLAINTEXT`, `SSL`, `SASL_SSL`.
- Supports upstream: `PLAINTEXT`, `SASL_PLAINTEXT`, `SSL`, `SASL_SSL`.
- Optional auth swap between downstream identity and upstream SASL credentials.

## Mapping Modes
### `sni`
If you enable TLS to your client, you only need 1 port number!

Best when you want one listener port and hostname-based routing.

- Requires downstream TLS (uses SNI from client handshake).
- Single listener port (`broker_mapping.sni.bind_port`) for bootstrap and brokers.
- Hostname rewrite is rule-driven with regex + template:
  - `broker_mapping.sni.upstream` (match regex)
  - `broker_mapping.sni.downstream` (replacement template, supports capture groups and `<$id>`)
- Deterministic behavior:
  - For a given upstream hostname + broker id, the same downstream hostname is always generated.
  - Broker id to downstream endpoint mapping is held stable across updates (inconsistencies are treated as fatal).
- Convergence behavior:
  - Before route learning, unknown SNI hostnames fall back to the default upstream bootstrap.
  - After `Metadata`/`FindCoordinator`/`DescribeCluster` responses are seen, routing becomes broker-specific.

### `port_offset`
Best when you want deterministic routing by port and support plaintext clients easily.

- Uses a fixed downstream hostname and deterministic port mapping.
- Deterministic formula:
  - bootstrap reserved range: `base..base+19`
  - broker listener port: `base + 20 + broker_id`
- `base..base+19` routes to the first upstream bootstrap servers (up to 20 entries).
- Safety guardrails:
  - `max_broker_id` bounds accepted broker ids.
  - Endpoint collisions and out-of-range computed ports are treated as errors.
- Operational benefit:
  - Clients can route by static hostname+port without SNI, useful for non-TLS or simpler network setups.

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

### DNS Resolution Customization (`resolve`)
Use `resolve` to override upstream host DNS lookups directly in config.

```yaml
resolve:
  - host: "pkc-312o0.ap-southeast-1.aws.confluent.cloud"
    ipv4: "10.10.10.10"
  - host: "*.aws.confluent.cloud"
    ipv4: "10.10.10.20"
  - host: "broker-v6.internal"
    ipv6: "2001:db8::10"
```

Notes:
- Both exact and `*` wildcard hosts are supported.
- Exact host rules win over wildcard rules.
- If multiple wildcard rules match, the more specific one wins; if still tied, the later entry wins.

## Templates
See `config-templates/`:
- `01-client-plaintext-noauth_upstream-saslssl-plain.yaml`
- `02-client-saslssl-scram_upstream-saslssl-plain.yaml`
- `03-client-saslplaintext-plain_upstream-saslssl-plain.yaml`
- `04-client-mtls-with-sasl-scram256-server-saslssl-plain.yaml`
- `05-client-ssl-onewaytls_upstream-saslssl-plain.yaml`
- `06-client-ssl-mtls_upstream-saslssl-plain.yaml`
- `07-client-plaintext-noauth_upstream-plaintext-noauth.yaml`
