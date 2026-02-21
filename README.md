# Kafka Proxy
Use this if your backend is Confluent Cloud. it is super easy to setup, battery included. Scales to gigabits per second with only less than 10% CPU utilization on one core.

Pay respect to the power of Rust. The proxy happily proxy 1.1GiB/s at 25% of a core.

Perf test (Kafka backend on RAMDISK)
```bash
root@titan /o/test [SIGINT]# kafka-producer-perf-test --topic perf-test --throughput -1 --num-records 9999999999999 --payload-file payloads.txt --producer.config client.properties
Reading payloads from: /opt/test/payloads.txt
Number of messages read: 10000
3832823 records sent, 766564.6 records/sec (1000.08 MB/sec), 27.8 ms avg latency, 270.0 ms max latency.
4423225 records sent, 884645.0 records/sec (1154.13 MB/sec), 26.7 ms avg latency, 76.0 ms max latency.
4512400 records sent, 902480.0 records/sec (1177.40 MB/sec), 26.2 ms avg latency, 78.0 ms max latency.
```

CPU usage
```bash
top - 23:09:58 up 12 days, 16:47, 14 users,  load average: 2.29, 1.45, 0.89
Tasks:   1 total,   0 running,   1 sleeping,   0 stopped,   0 zombie
%Cpu(s): 14.9 us,  8.9 sy,  0.0 ni, 74.7 id,  0.0 wa,  0.2 hi,  1.3 si,  0.0 st
MiB Mem :  63863.8 total,  12594.5 free,  46874.1 used,  36270.3 buff/cache
MiB Swap:  65536.0 total,  65349.8 free,    186.2 used.  16989.8 avail Mem

    PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
1660465 root      20   0   37620   5624   2572 S  24.0   0.0   0:23.73 kafka_proxy-0.1
```

Memory usage at 5.6MiB:

```bash
Every 2.0s: cat /proc/1660465/status | grep -i rss                                                                                                                                                                                               titan: Sat Feb 21 23:12:18 2026

VmRSS:      5636 kB
RssAnon:            3064 kB
RssFile:            2572 kB
RssShmem:              0 kB
```


# Highlights

SNI-based Kafka TLS proxy that:
- Is high-performance and lightweight for production traffic.
- Uses a zero-copy streaming path for non-rewritten Kafka responses.
- Terminates TLS from clients.
- Connects to upstream brokers over TLS.
- Rewrites broker host/port in selected Kafka responses so clients reconnect through the proxy.
- Learns broker routes dynamically from metadata/coordinator responses.

## Features

- High-performance, low-overhead forwarding for Kafka traffic.
- Zero-copy passthrough for response types that do not require rewrite.
- Lightweight runtime profile (minimal allocations on hot path).
- Client-side TLS termination (`server.pem`/`server_key.pem`).
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
- `--key` (default: `server_key.pem`)
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
