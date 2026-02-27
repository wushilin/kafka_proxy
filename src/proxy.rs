use crate::auth::AuthSwap;
use crate::hosts::HostsResolver;
use crate::kafka_io::{
    copy_exact_n, read_frame_len, write_frame_len, write_kafka_frame,
};
use crate::rewrite::{maybe_rewrite_kafka_response, should_attempt_rewrite, BrokerAdvertiseMode};
use crate::stats::ProxyStats;
use crate::upstream::upstream_host;
use anyhow::{anyhow, Context, Result};
use base64::Engine;
use bytes::{Buf, Bytes, BytesMut};
use hmac::{Hmac, Mac};
use kafka_protocol::messages::{
    ApiKey, RequestHeader, ResponseHeader, SaslAuthenticateRequest, SaslAuthenticateResponse,
    SaslHandshakeRequest, SaslHandshakeResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, Message, StrBytes};
use pbkdf2::pbkdf2;
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, oneshot};
use tokio::time::sleep;
use tokio_rustls::rustls;
use tokio_rustls::server::TlsStream as DownstreamTlsStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::{error, info, warn};
use uuid::Uuid;
use x509_parser::parse_x509_certificate;
type HmacSha256 = Hmac<sha2::Sha256>;
type HmacSha512 = Hmac<sha2::Sha512>;

fn ensure_buffer_capacity(buf: &mut Vec<u8>, needed: usize, label: &str, conn_ctx: &str) {
    if needed > buf.len() {
        info!(
            "{} increasing {} buffer from {} to {} on demand.",
            conn_ctx,
            label,
            buf.len(),
            needed
        );
        buf.resize(needed, 0);
    }
}

/// Ring buffer of the last N Fetch response sizes, used to decide when to shrink the rewrite buffer.
struct FetchSizeRing {
    buf: [usize; 10],
    len: usize,
    pos: usize,
}

impl FetchSizeRing {
    fn new() -> Self {
        Self {
            buf: [0; 10],
            len: 0,
            pos: 0,
        }
    }

    fn push(&mut self, v: usize) {
        self.buf[self.pos] = v;
        self.pos = (self.pos + 1) % 10;
        if self.len < 10 {
            self.len += 1;
        }
    }

    fn max(&self) -> usize {
        if self.len == 0 {
            0
        } else {
            self.buf[..self.len].iter().copied().max().unwrap_or(0)
        }
    }
}

fn short_id() -> String {
    Uuid::new_v4().simple().to_string()[..8].to_string()
}

fn parse_broker_route_key(key: &str) -> Option<i32> {
    key.strip_prefix('b')?.parse::<i32>().ok()
}

fn remove_owned_route_keys(
    routes: &mut HashMap<String, String>,
    route_owners: &mut HashMap<String, i32>,
    broker_id: i32,
    keep_keys: Option<&HashSet<String>>,
) {
    let owned_keys: Vec<String> = route_owners
        .iter()
        .filter(|(key, owner_id)| {
            **owner_id == broker_id && keep_keys.is_none_or(|keep| !keep.contains(*key))
        })
        .map(|(key, _)| key.clone())
        .collect();
    for key in owned_keys {
        route_owners.remove(&key);
        routes.remove(&key);
    }
}

fn format_topology_map(map: &HashMap<i32, String>) -> String {
    let mut entries: Vec<(i32, String)> = map.iter().map(|(k, v)| (*k, v.clone())).collect();
    entries.sort_by_key(|(k, _)| *k);
    let body = entries
        .into_iter()
        .map(|(k, v)| format!("{}->{}", k, v))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{}}}", body)
}

fn cert_dn_summary(cert_der: &[u8]) -> Option<(String, String)> {
    let (_, cert) = parse_x509_certificate(cert_der).ok()?;
    Some((
        cert.subject().to_string(),
        cert.issuer().to_string(),
    ))
}

fn cert_common_name(cert_der: &[u8]) -> Option<String> {
    let (_, cert) = parse_x509_certificate(cert_der).ok()?;
    let cn = cert.subject().iter_common_name().next()?;
    Some(cn.as_str().ok()?.to_string())
}

#[derive(Debug)]
struct IngressRouteContext {
    full_sni: String,
    local_port: u16,
    broker_alias: Option<String>,
}

impl IngressRouteContext {
    fn from_connection(sni: &str, local_port: u16, mode: &BrokerAdvertiseMode) -> Self {
        let broker_alias = match mode {
            BrokerAdvertiseMode::Sni { .. } => None,
            BrokerAdvertiseMode::PortOffset {
                base_port,
                bootstrap_reserved,
                ..
            } => {
                let broker_start = base_port.saturating_add(*bootstrap_reserved);
                if local_port >= broker_start {
                    Some(format!("b{}", local_port - broker_start))
                } else {
                    None
                }
            }
        };
        Self {
            full_sni: sni.to_string(),
            local_port,
            broker_alias,
        }
    }

    fn candidate_keys(&self, mode: &BrokerAdvertiseMode) -> Vec<String> {
        let mut keys = Vec::with_capacity(3);
        match mode {
            BrokerAdvertiseMode::Sni { .. } => {
                keys.push(self.full_sni.clone());
            }
            BrokerAdvertiseMode::PortOffset { hostname, .. } => {
                keys.push(format!("{}:{}", self.full_sni, self.local_port));
                keys.push(format!("{}:{}", hostname, self.local_port));
            }
        }
        if let Some(alias) = &self.broker_alias {
            keys.push(alias.clone());
        }
        keys
    }
}

fn resolve_upstream_for_ingress(
    routes: &HashMap<String, String>,
    ingress: &IngressRouteContext,
    mode: &BrokerAdvertiseMode,
    default_upstream: &str,
) -> (Option<String>, bool, Option<String>, Option<String>) {
    for candidate in ingress.candidate_keys(mode) {
        if let Some(found) = routes.get(&candidate) {
            return (Some(found.clone()), false, Some(candidate), None);
        }
    }
    match mode {
        BrokerAdvertiseMode::Sni { .. } => {
            (Some(default_upstream.to_string()), true, None, None)
        }
        BrokerAdvertiseMode::PortOffset {
            base_port,
            bootstrap_reserved,
            max_broker_id,
            ..
        } => {
            if ingress.local_port < *base_port {
                return (
                    None,
                    false,
                    None,
                    Some(format!(
                        "Port {} is outside configured port_offset listener range {}..={}",
                        ingress.local_port,
                        base_port,
                        base_port + bootstrap_reserved + max_broker_id
                    )),
                );
            }
            let slot = ingress.local_port - *base_port;
            if slot < *bootstrap_reserved {
                (
                    None,
                    false,
                    None,
                    Some(format!(
                        "No bootstrap upstream configured for reserved bootstrap port {}",
                        ingress.local_port
                    )),
                )
            } else {
                let broker_id = slot - *bootstrap_reserved;
                if broker_id > *max_broker_id {
                    return (
                        None,
                        false,
                        None,
                        Some(format!(
                            "Port {} is outside configured broker listener range {}..={}",
                            ingress.local_port,
                            base_port + bootstrap_reserved,
                            base_port + bootstrap_reserved + max_broker_id
                        )),
                    );
                }
                (
                    None,
                    false,
                    None,
                    Some(format!(
                        "No broker mapping learned yet for port_offset broker id {} (port {})",
                        broker_id, ingress.local_port
                    )),
                )
            }
        }
    }
}

#[derive(Debug)]
struct ParsedRequest {
    api_key: ApiKey,
    api_version: i16,
    header: RequestHeader,
    body: Bytes,
    raw_payload: Bytes,
}

async fn read_request_frame<S>(stream: &mut S) -> Result<Option<ParsedRequest>>
where
    S: AsyncRead + Unpin,
{
    let Some(frame_len) = read_frame_len(stream).await? else {
        return Ok(None);
    };
    let mut payload = vec![0u8; frame_len];
    stream
        .read_exact(&mut payload)
        .await
        .context("Failed to read full Kafka request payload")?;

    if payload.len() < 4 {
        return Err(anyhow!("Kafka request too short for api key/version"));
    }
    let api_key_raw = i16::from_be_bytes([payload[0], payload[1]]);
    let api_version = i16::from_be_bytes([payload[2], payload[3]]);
    // Auth-swap bootstrap parses/handles only known Kafka APIs (Sasl*/ApiVersions);
    // unknown API keys are rejected here instead of passthrough.
    let api_key =
        ApiKey::try_from(api_key_raw).map_err(|_| anyhow!("Unknown request API key {}", api_key_raw))?;

    let header_version = api_key.request_header_version(api_version);
    let mut decode_buf = Bytes::from(payload);
    let raw_payload = decode_buf.clone();
    let header = RequestHeader::decode(&mut decode_buf, header_version)
        .context("Failed to decode request header")?;
    let body = decode_buf.copy_to_bytes(decode_buf.remaining());
    Ok(Some(ParsedRequest {
        api_key,
        api_version,
        header,
        body,
        raw_payload,
    }))
}

async fn relay_passthrough_request_response<C, U>(
    client_stream: &mut C,
    upstream_stream: &mut U,
    req: &ParsedRequest,
    conn_ctx: &str,
) -> Result<()>
where
    C: AsyncRead + AsyncWrite + Unpin,
    U: AsyncRead + AsyncWrite + Unpin,
{
    write_kafka_frame(upstream_stream, &req.raw_payload)
        .await
        .context("Failed to forward passthrough request to upstream during auth bootstrap")?;
    tracing::debug!(
        "{} bootstrap passthrough request api={:?} version={} corr_id={} bytes={}",
        conn_ctx,
        req.api_key,
        req.api_version,
        req.header.correlation_id,
        req.raw_payload.len()
    );

    let Some(resp_len) = read_frame_len(upstream_stream).await? else {
        return Err(anyhow!(
            "Upstream closed while waiting for bootstrap passthrough response"
        ));
    };
    let mut resp_payload = vec![0u8; resp_len];
    upstream_stream
        .read_exact(&mut resp_payload)
        .await
        .context("Failed to read bootstrap passthrough response from upstream")?;

    let resp_payload = Bytes::from(resp_payload);
    write_kafka_frame(client_stream, &resp_payload)
        .await
        .context("Failed to forward bootstrap passthrough response to client")?;
    tracing::debug!(
        "{} bootstrap passthrough response bytes={}",
        conn_ctx,
        resp_len
    );
    Ok(())
}

fn encode_response_with_header<M: Encodable + Message>(
    correlation_id: i32,
    api_key: ApiKey,
    api_version: i16,
    body: &M,
) -> Result<Bytes> {
    let header_version = api_key.response_header_version(api_version);
    let response_header = ResponseHeader::default().with_correlation_id(correlation_id);
    let mut buf = BytesMut::new();
    response_header
        .encode(&mut buf, header_version)
        .context("Failed to encode response header")?;
    body.encode(&mut buf, api_version)
        .context("Failed to encode response body")?;
    Ok(buf.freeze())
}

async fn send_sasl_handshake_response<S>(
    stream: &mut S,
    correlation_id: i32,
    version: i16,
    mechanism: &str,
    error_code: i16,
    error_message: Option<&str>,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    let resp = SaslHandshakeResponse::default()
        .with_error_code(error_code)
        .with_mechanisms(vec![StrBytes::from_string(mechanism.to_string())]);
    let payload = encode_response_with_header(correlation_id, ApiKey::SaslHandshake, version, &resp)?;
    write_kafka_frame(stream, &payload).await?;
    if let Some(msg) = error_message {
        warn!("SASL handshake response error_code={} message={}", error_code, msg);
    }
    Ok(())
}

async fn send_sasl_auth_response<S>(
    stream: &mut S,
    correlation_id: i32,
    version: i16,
    error_code: i16,
    error_message: Option<&str>,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    let resp = SaslAuthenticateResponse::default()
        .with_error_code(error_code)
        .with_error_message(error_message.map(|m| StrBytes::from_string(m.to_string())))
        .with_auth_bytes(Bytes::new())
        .with_session_lifetime_ms(0);
    let payload =
        encode_response_with_header(correlation_id, ApiKey::SaslAuthenticate, version, &resp)?;
    write_kafka_frame(stream, &payload).await?;
    Ok(())
}

fn parse_plain_auth(auth_bytes: &[u8]) -> Result<(String, String)> {
    let mut parts = auth_bytes.split(|b| *b == 0u8);
    let _authzid = parts.next().unwrap_or_default();
    let authcid = parts
        .next()
        .ok_or_else(|| anyhow!("Invalid SASL/PLAIN payload: missing username"))?;
    let passwd = parts
        .next()
        .ok_or_else(|| anyhow!("Invalid SASL/PLAIN payload: missing password"))?;
    let username =
        String::from_utf8(authcid.to_vec()).context("Invalid UTF-8 username in SASL/PLAIN payload")?;
    let password =
        String::from_utf8(passwd.to_vec()).context("Invalid UTF-8 password in SASL/PLAIN payload")?;
    Ok((username, password))
}

fn build_plain_auth(username: &str, secret: &str) -> Bytes {
    let mut out = Vec::with_capacity(username.len() + secret.len() + 2);
    out.push(0);
    out.extend_from_slice(username.as_bytes());
    out.push(0);
    out.extend_from_slice(secret.as_bytes());
    Bytes::from(out)
}

#[derive(Clone, Copy)]
enum ScramAlgo {
    Sha256,
    Sha512,
}

impl ScramAlgo {
    fn from_mechanism(mechanism: &str) -> Option<Self> {
        match normalize_mechanism(mechanism).as_str() {
            "SCRAM-SHA-256" => Some(Self::Sha256),
            "SCRAM-SHA-512" => Some(Self::Sha512),
            _ => None,
        }
    }
}

fn normalize_mechanism(mechanism: &str) -> String {
    match mechanism.to_ascii_uppercase().as_str() {
        "SCRAM-256" => "SCRAM-SHA-256".to_string(),
        "SCRAM-512" => "SCRAM-SHA-512".to_string(),
        other => other.to_string(),
    }
}

fn hmac_bytes(algo: ScramAlgo, key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
    match algo {
        ScramAlgo::Sha256 => {
            let mut mac = HmacSha256::new_from_slice(key).context("Failed to init HMAC-SHA-256")?;
            mac.update(data);
            Ok(mac.finalize().into_bytes().to_vec())
        }
        ScramAlgo::Sha512 => {
            let mut mac = HmacSha512::new_from_slice(key).context("Failed to init HMAC-SHA-512")?;
            mac.update(data);
            Ok(mac.finalize().into_bytes().to_vec())
        }
    }
}

fn hash_bytes(algo: ScramAlgo, data: &[u8]) -> Vec<u8> {
    match algo {
        ScramAlgo::Sha256 => {
            use sha2::Digest;
            sha2::Sha256::digest(data).to_vec()
        }
        ScramAlgo::Sha512 => {
            use sha2::Digest;
            sha2::Sha512::digest(data).to_vec()
        }
    }
}

fn xor_bytes(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().zip(b.iter()).map(|(x, y)| x ^ y).collect()
}

fn parse_scram_client_first(auth_bytes: &[u8]) -> Result<(String, String, String)> {
    let msg = std::str::from_utf8(auth_bytes).context("Invalid UTF-8 in SASL/SCRAM payload")?;
    let mut parts = msg.splitn(3, ',');
    let p1 = parts.next().unwrap_or_default();
    let p2 = parts.next().unwrap_or_default();
    let bare = parts
        .next()
        .ok_or_else(|| anyhow!("Invalid SCRAM client-first message"))?;
    if p1 != "n" || p2 != "" {
        return Err(anyhow!("Invalid SCRAM GS2 header"));
    }

    let username_field = bare
        .split(',')
        .find(|part| part.starts_with("n="))
        .ok_or_else(|| anyhow!("Invalid SCRAM payload: missing n=<username> field"))?;
    let nonce_field = bare
        .split(',')
        .find(|part| part.starts_with("r="))
        .ok_or_else(|| anyhow!("Invalid SCRAM payload: missing r=<nonce> field"))?;

    let username = username_field
        .trim_start_matches("n=")
        .replace("=2C", ",")
        .replace("=3D", "=");
    let nonce = nonce_field.trim_start_matches("r=").to_string();
    Ok((username, nonce, bare.to_string()))
}

fn parse_scram_server_first(msg: &str) -> Result<(String, Vec<u8>, u32)> {
    let nonce = msg
        .split(',')
        .find(|p| p.starts_with("r="))
        .ok_or_else(|| anyhow!("Invalid SCRAM server-first: missing nonce"))?
        .trim_start_matches("r=")
        .to_string();
    let salt_b64 = msg
        .split(',')
        .find(|p| p.starts_with("s="))
        .ok_or_else(|| anyhow!("Invalid SCRAM server-first: missing salt"))?
        .trim_start_matches("s=");
    let iter = msg
        .split(',')
        .find(|p| p.starts_with("i="))
        .ok_or_else(|| anyhow!("Invalid SCRAM server-first: missing iteration"))?
        .trim_start_matches("i=")
        .parse::<u32>()
        .context("Invalid SCRAM server-first iteration")?;
    let salt = base64::engine::general_purpose::STANDARD
        .decode(salt_b64)
        .context("Invalid SCRAM server-first salt base64")?;
    Ok((nonce, salt, iter))
}

fn parse_scram_client_final(msg: &str) -> Result<(String, String, Vec<u8>)> {
    let cbind = msg
        .split(',')
        .find(|p| p.starts_with("c="))
        .ok_or_else(|| anyhow!("Invalid SCRAM client-final: missing c="))?
        .trim_start_matches("c=")
        .to_string();
    let nonce = msg
        .split(',')
        .find(|p| p.starts_with("r="))
        .ok_or_else(|| anyhow!("Invalid SCRAM client-final: missing r="))?
        .trim_start_matches("r=")
        .to_string();
    let proof_b64 = msg
        .split(',')
        .find(|p| p.starts_with("p="))
        .ok_or_else(|| anyhow!("Invalid SCRAM client-final: missing p="))?
        .trim_start_matches("p=");
    let proof = base64::engine::general_purpose::STANDARD
        .decode(proof_b64)
        .context("Invalid SCRAM client proof base64")?;
    Ok((cbind, nonce, proof))
}

fn compute_scram_material(
    algo: ScramAlgo,
    password: &str,
    salt: &[u8],
    iterations: u32,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut salted = vec![
        0u8;
        match algo {
            ScramAlgo::Sha256 => 32,
            ScramAlgo::Sha512 => 64,
        }
    ];
    match algo {
        ScramAlgo::Sha256 => {
            pbkdf2::<HmacSha256>(password.as_bytes(), salt, iterations, &mut salted)
                .context("PBKDF2-SHA256 failed")?;
        }
        ScramAlgo::Sha512 => {
            pbkdf2::<HmacSha512>(password.as_bytes(), salt, iterations, &mut salted)
                .context("PBKDF2-SHA512 failed")?;
        }
    }
    let client_key = hmac_bytes(algo, &salted, b"Client Key")?;
    let server_key = hmac_bytes(algo, &salted, b"Server Key")?;
    Ok((client_key, server_key))
}

async fn read_expected_response<S, M>(
    stream: &mut S,
    api_key: ApiKey,
    api_version: i16,
    expected_correlation_id: i32,
) -> Result<M>
where
    S: AsyncRead + Unpin,
    M: Decodable,
{
    let Some(frame_len) = read_frame_len(stream).await? else {
        return Err(anyhow!("Upstream closed connection while waiting for response"));
    };
    let mut payload = vec![0u8; frame_len];
    stream
        .read_exact(&mut payload)
        .await
        .context("Failed to read full Kafka response payload")?;

    let header_version = api_key.response_header_version(api_version);
    let mut decode_buf = Bytes::from(payload);
    let header = ResponseHeader::decode(&mut decode_buf, header_version)
        .context("Failed to decode response header")?;
    if header.correlation_id != expected_correlation_id {
        return Err(anyhow!(
            "Unexpected correlation id in upstream response: expected {}, got {}",
            expected_correlation_id,
            header.correlation_id
        ));
    }

    let body = M::decode(&mut decode_buf, api_version).context("Failed to decode response body")?;
    Ok(body)
}

async fn send_request<S, M>(
    stream: &mut S,
    api_key: ApiKey,
    api_version: i16,
    correlation_id: i32,
    body: &M,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
    M: Encodable + Message,
{
    let header_version = api_key.request_header_version(api_version);
    let header = RequestHeader::default()
        .with_request_api_key(api_key as i16)
        .with_request_api_version(api_version)
        .with_correlation_id(correlation_id)
        .with_client_id(None);

    let mut payload = BytesMut::new();
    header
        .encode(&mut payload, header_version)
        .context("Failed to encode request header")?;
    body.encode(&mut payload, api_version)
        .context("Failed to encode request body")?;
    write_kafka_frame(stream, &payload.freeze()).await?;
    Ok(())
}

async fn run_auth_swap_bootstrap<C, U>(
    client_stream: &mut C,
    upstream_stream: &mut U,
    auth_swap: &AuthSwap,
    downstream_mechanism: Option<&str>,
    upstream_mechanism: Option<&str>,
    downstream_preauth_principal: Option<&str>,
    conn_ctx: &str,
) -> Result<()>
where
    C: AsyncRead + AsyncWrite + Unpin,
    U: AsyncRead + AsyncWrite + Unpin,
{
    let mut downstream_principal: Option<String> =
        downstream_preauth_principal.map(|s| s.to_string());

    if let Some(downstream_mechanism) = downstream_mechanism {
        let requested_mechanism = normalize_mechanism(downstream_mechanism);

        let handshake_req = loop {
            let req = read_request_frame(client_stream)
                .await?
                .ok_or_else(|| anyhow!("Client closed before SASL handshake"))?;
            match req.api_key {
                ApiKey::SaslHandshake => break req,
                ApiKey::ApiVersions => {
                    relay_passthrough_request_response(
                        client_stream,
                        upstream_stream,
                        &req,
                        conn_ctx,
                    )
                    .await?;
                }
                other => {
                    return Err(anyhow!(
                        "Expected SaslHandshake request (or ApiVersions preflight), got {:?}",
                        other
                    ));
                }
            }
        };
        let mut hs_buf = handshake_req.body.clone();
        let hs_req = SaslHandshakeRequest::decode(&mut hs_buf, handshake_req.api_version)
            .context("Failed to decode downstream SaslHandshakeRequest")?;
        let client_mechanism = normalize_mechanism(hs_req.mechanism.as_str());
        if client_mechanism != requested_mechanism {
            send_sasl_handshake_response(
                client_stream,
                handshake_req.header.correlation_id,
                handshake_req.api_version,
                downstream_mechanism,
                33,
                Some("Unsupported SASL mechanism"),
            )
            .await?;
            return Err(anyhow!(
                "{} downstream requested mechanism '{}' but config requires '{}'",
                conn_ctx,
                client_mechanism,
                requested_mechanism
            ));
        }
        send_sasl_handshake_response(
            client_stream,
            handshake_req.header.correlation_id,
            handshake_req.api_version,
            downstream_mechanism,
            0,
            None,
        )
        .await?;

        let auth_req = loop {
            let req = read_request_frame(client_stream)
                .await?
                .ok_or_else(|| anyhow!("Client closed before SASL authentication"))?;
            match req.api_key {
                ApiKey::SaslAuthenticate => break req,
                ApiKey::ApiVersions => {
                    relay_passthrough_request_response(
                        client_stream,
                        upstream_stream,
                        &req,
                        conn_ctx,
                    )
                    .await?;
                }
                other => {
                    return Err(anyhow!(
                        "Expected SaslAuthenticate request (or ApiVersions preflight), got {:?}",
                        other
                    ));
                }
            }
        };
        let mut auth_buf = auth_req.body.clone();
        let sasl_auth = SaslAuthenticateRequest::decode(&mut auth_buf, auth_req.api_version)
            .context("Failed to decode downstream SaslAuthenticateRequest")?;
        let mut already_responded = false;

        match requested_mechanism.as_str() {
            "PLAIN" => {
                let (username, password) = parse_plain_auth(&sasl_auth.auth_bytes)?;
                auth_swap.validate_downstream_plain(&username, &password)?;
                downstream_principal = Some(username);
            }
            "SCRAM-SHA-256" | "SCRAM-SHA-512" => {
                let algo = ScramAlgo::from_mechanism(requested_mechanism.as_str())
                    .ok_or_else(|| anyhow!("Unsupported SCRAM mechanism"))?;
                let (username, client_nonce, client_first_bare) =
                    parse_scram_client_first(&sasl_auth.auth_bytes)?;
                let password = auth_swap.downstream_secret(&username)?;

                let mut server_nonce_bytes = [0u8; 16];
                rand::thread_rng().fill(&mut server_nonce_bytes);
                let server_nonce = base64::engine::general_purpose::STANDARD.encode(server_nonce_bytes);
                let combined_nonce = format!("{}{}", client_nonce, server_nonce);
                let mut salt = [0u8; 16];
                rand::thread_rng().fill(&mut salt);
                let iterations = 4096u32;
                let server_first = format!(
                    "r={},s={},i={}",
                    combined_nonce,
                    base64::engine::general_purpose::STANDARD.encode(salt),
                    iterations
                );
                let challenge = SaslAuthenticateResponse::default()
                    .with_error_code(0)
                    .with_auth_bytes(Bytes::from(server_first.clone()))
                    .with_session_lifetime_ms(0);
                let challenge_payload = encode_response_with_header(
                    auth_req.header.correlation_id,
                    ApiKey::SaslAuthenticate,
                    auth_req.api_version,
                    &challenge,
                )?;
                write_kafka_frame(client_stream, &challenge_payload).await?;

                let final_req = read_request_frame(client_stream)
                    .await?
                    .ok_or_else(|| anyhow!("Client closed before SCRAM final message"))?;
                if final_req.api_key != ApiKey::SaslAuthenticate {
                    return Err(anyhow!(
                        "Expected SaslAuthenticate final request, got {:?}",
                        final_req.api_key
                    ));
                }
                let mut final_buf = final_req.body.clone();
                let final_auth =
                    SaslAuthenticateRequest::decode(&mut final_buf, final_req.api_version).context(
                        "Failed to decode downstream SCRAM final SaslAuthenticateRequest",
                    )?;
                let final_msg = std::str::from_utf8(&final_auth.auth_bytes)
                    .context("Invalid UTF-8 in downstream SCRAM final payload")?;
                let (cbind, final_nonce, client_proof) = parse_scram_client_final(final_msg)?;
                if final_nonce != combined_nonce {
                    send_sasl_auth_response(
                        client_stream,
                        final_req.header.correlation_id,
                        final_req.api_version,
                        58,
                        Some("SCRAM nonce mismatch"),
                    )
                    .await?;
                    return Err(anyhow!("Downstream SCRAM nonce mismatch"));
                }
                let client_final_without_proof = format!("c={},r={}", cbind, final_nonce);
                let auth_message = format!(
                    "{},{},{}",
                    client_first_bare, server_first, client_final_without_proof
                );

                let (client_key, server_key) =
                    compute_scram_material(algo, &password, &salt, iterations)?;
                let stored_key = hash_bytes(algo, &client_key);
                let client_signature = hmac_bytes(algo, &stored_key, auth_message.as_bytes())?;
                let expected_client_key = xor_bytes(&client_proof, &client_signature);
                if expected_client_key != client_key {
                    send_sasl_auth_response(
                        client_stream,
                        final_req.header.correlation_id,
                        final_req.api_version,
                        58,
                        Some("SCRAM password verification failed"),
                    )
                    .await?;
                    return Err(anyhow!(
                        "{} downstream SCRAM proof verification failed for principal '{}'",
                        conn_ctx, username
                    ));
                }

                let server_signature = hmac_bytes(algo, &server_key, auth_message.as_bytes())?;
                let server_final = format!(
                    "v={}",
                    base64::engine::general_purpose::STANDARD.encode(server_signature)
                );
                let success = SaslAuthenticateResponse::default()
                    .with_error_code(0)
                    .with_auth_bytes(Bytes::from(server_final))
                    .with_session_lifetime_ms(0);
                let success_payload = encode_response_with_header(
                    final_req.header.correlation_id,
                    ApiKey::SaslAuthenticate,
                    final_req.api_version,
                    &success,
                )?;
                write_kafka_frame(client_stream, &success_payload).await?;
                downstream_principal = Some(username);
                already_responded = true;
            }
            other => {
                send_sasl_auth_response(
                    client_stream,
                    auth_req.header.correlation_id,
                    auth_req.api_version,
                    33,
                    Some("Unsupported downstream SASL mechanism"),
                )
                .await?;
                return Err(anyhow!(
                    "Unsupported downstream SASL mechanism '{}'",
                    other
                ));
            }
        }

        if !already_responded {
            send_sasl_auth_response(
                client_stream,
                auth_req.header.correlation_id,
                auth_req.api_version,
                0,
                None,
            )
            .await?;
        }
    }

    let use_preauth_identity_for_mapping =
        downstream_mechanism.is_none() && downstream_preauth_principal.is_some();
    let resolved = auth_swap.resolve_upstream_credential(
        downstream_principal.as_deref(),
        use_preauth_identity_for_mapping,
    )?;
    info!(
        "{} auth_swap resolved downstream principal '{}' -> upstream principal '{}'",
        conn_ctx, resolved.downstream_principal, resolved.mapped_principal
    );

    if let Some(upstream_mechanism) = upstream_mechanism {
        let upstream_mechanism = normalize_mechanism(upstream_mechanism);
        let handshake_version = 1i16;
        let auth_version = 1i16;
        let handshake_corr = -1001i32;
        let auth_corr = -1002i32;

        let hs_req =
            SaslHandshakeRequest::default().with_mechanism(StrBytes::from_string(upstream_mechanism.clone()));
        send_request(
            upstream_stream,
            ApiKey::SaslHandshake,
            handshake_version,
            handshake_corr,
            &hs_req,
        )
        .await?;
        let hs_resp: SaslHandshakeResponse =
            read_expected_response(upstream_stream, ApiKey::SaslHandshake, handshake_version, handshake_corr)
                .await?;
        if hs_resp.error_code != 0 {
            return Err(anyhow!(
                "Upstream SASL handshake failed with error_code={}",
                hs_resp.error_code
            ));
        }

        let auth_payload = match upstream_mechanism.as_str() {
            "PLAIN" => build_plain_auth(&resolved.mapped_principal, &resolved.upstream_secret),
            "SCRAM-SHA-256" | "SCRAM-SHA-512" => {
                let algo = ScramAlgo::from_mechanism(upstream_mechanism.as_str())
                    .ok_or_else(|| anyhow!("Unsupported upstream SCRAM mechanism"))?;
                let mut nonce_bytes = [0u8; 16];
                rand::thread_rng().fill(&mut nonce_bytes);
                let client_nonce =
                    base64::engine::general_purpose::STANDARD.encode(nonce_bytes);
                let client_first_bare =
                    format!("n={},r={}", resolved.mapped_principal, client_nonce);
                let client_first = format!("n,,{}", client_first_bare);
                let auth_req = SaslAuthenticateRequest::default()
                    .with_auth_bytes(Bytes::from(client_first));
                send_request(
                    upstream_stream,
                    ApiKey::SaslAuthenticate,
                    auth_version,
                    auth_corr,
                    &auth_req,
                )
                .await?;
                let auth_resp_1: SaslAuthenticateResponse = read_expected_response(
                    upstream_stream,
                    ApiKey::SaslAuthenticate,
                    auth_version,
                    auth_corr,
                )
                .await?;
                if auth_resp_1.error_code != 0 {
                    return Err(anyhow!(
                        "Upstream SCRAM first-step failed with error_code={} message={}",
                        auth_resp_1.error_code,
                        auth_resp_1
                            .error_message
                            .as_ref()
                            .map(|s| s.as_str())
                            .unwrap_or("")
                    ));
                }
                let server_first = std::str::from_utf8(&auth_resp_1.auth_bytes)
                    .context("Invalid UTF-8 in upstream SCRAM server-first")?;
                let (server_nonce, salt, iterations) = parse_scram_server_first(server_first)?;
                if !server_nonce.starts_with(&client_nonce) {
                    return Err(anyhow!("Upstream SCRAM nonce does not extend client nonce"));
                }
                let (client_key, server_key) = compute_scram_material(
                    algo,
                    &resolved.upstream_secret,
                    &salt,
                    iterations,
                )?;
                let stored_key = hash_bytes(algo, &client_key);
                let cbind = "biws";
                let client_final_without_proof = format!("c={},r={}", cbind, server_nonce);
                let auth_message = format!(
                    "{},{},{}",
                    client_first_bare, server_first, client_final_without_proof
                );
                let client_signature = hmac_bytes(algo, &stored_key, auth_message.as_bytes())?;
                let client_proof = xor_bytes(&client_key, &client_signature);
                let final_msg = format!(
                    "{},p={}",
                    client_final_without_proof,
                    base64::engine::general_purpose::STANDARD.encode(client_proof)
                );
                let auth_req_2 = SaslAuthenticateRequest::default()
                    .with_auth_bytes(Bytes::from(final_msg));
                let auth_corr_2 = auth_corr - 1;
                send_request(
                    upstream_stream,
                    ApiKey::SaslAuthenticate,
                    auth_version,
                    auth_corr_2,
                    &auth_req_2,
                )
                .await?;
                let auth_resp_2: SaslAuthenticateResponse = read_expected_response(
                    upstream_stream,
                    ApiKey::SaslAuthenticate,
                    auth_version,
                    auth_corr_2,
                )
                .await?;
                if auth_resp_2.error_code != 0 {
                    return Err(anyhow!(
                        "Upstream SCRAM final-step failed with error_code={} message={}",
                        auth_resp_2.error_code,
                        auth_resp_2
                            .error_message
                            .as_ref()
                            .map(|s| s.as_str())
                            .unwrap_or("")
                    ));
                }
                let server_final = std::str::from_utf8(&auth_resp_2.auth_bytes)
                    .context("Invalid UTF-8 in upstream SCRAM server-final")?;
                let server_sig_b64 = server_final
                    .split(',')
                    .find(|p| p.starts_with("v="))
                    .ok_or_else(|| anyhow!("Invalid upstream SCRAM server-final: missing v="))?
                    .trim_start_matches("v=");
                let server_sig = base64::engine::general_purpose::STANDARD
                    .decode(server_sig_b64)
                    .context("Invalid upstream SCRAM server signature base64")?;
                let expected_server_sig =
                    hmac_bytes(algo, &server_key, auth_message.as_bytes())?;
                if server_sig != expected_server_sig {
                    return Err(anyhow!("Upstream SCRAM server signature mismatch"));
                }
                Bytes::new()
            }
            _ => {
                return Err(anyhow!(
                    "Unsupported upstream SASL mechanism '{}'",
                    upstream_mechanism
                ));
            }
        };

        if !auth_payload.is_empty() {
            let auth_req = SaslAuthenticateRequest::default().with_auth_bytes(auth_payload);
            send_request(
                upstream_stream,
                ApiKey::SaslAuthenticate,
                auth_version,
                auth_corr,
                &auth_req,
            )
            .await?;
            let auth_resp: SaslAuthenticateResponse = read_expected_response(
                upstream_stream,
                ApiKey::SaslAuthenticate,
                auth_version,
                auth_corr,
            )
            .await?;
            if auth_resp.error_code != 0 {
                return Err(anyhow!(
                    "Upstream SASL authenticate failed with error_code={} message={}",
                    auth_resp.error_code,
                    auth_resp
                        .error_message
                        .as_ref()
                        .map(|s| s.as_str())
                        .unwrap_or("")
                ));
            }
        }
    } else {
        warn!(
            "{} auth_swap is enabled but upstream has no SASL configured; continuing without upstream auth",
            conn_ctx
        );
    }

    Ok(())
}

async fn forward_client_to_upstream<R, W>(
    client_read: &mut R,
    upstream_write: &mut W,
    in_flight: Arc<Mutex<HashMap<i32, (ApiKey, i16)>>>,
    client_addr: SocketAddr,
    reject_downstream_sasl_frames: bool,
    conn_ctx: &str,
    stats: Arc<ProxyStats>,
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut scratch = vec![0u8; 1024];
    ProxyStats::update_max(&stats.max_c2u_buffer, scratch.len());

    loop {
        let Some(frame_len) = read_frame_len(client_read).await? else {
            break;
        };
        stats.frames_client_to_upstream.fetch_add(1, Ordering::Relaxed);
        stats
            .bytes_client_to_upstream
            .fetch_add((frame_len + 4) as u64, Ordering::Relaxed);

        write_frame_len(upstream_write, frame_len).await?;

        let header_len = frame_len.min(8);
        let mut header = [0u8; 8];
        if header_len > 0 {
            client_read
                .read_exact(&mut header[..header_len])
                .await
                .context("Failed to read Kafka request header prefix")?;
            upstream_write
                .write_all(&header[..header_len])
                .await
                .context("Failed to write Kafka request header prefix")?;
        }

        if frame_len >= 8 {
            let api_key_raw = i16::from_be_bytes([header[0], header[1]]);
            let api_version = i16::from_be_bytes([header[2], header[3]]);
            let correlation_id = i32::from_be_bytes([header[4], header[5], header[6], header[7]]);
            match ApiKey::try_from(api_key_raw) {
                Ok(api_key) => {
                    if reject_downstream_sasl_frames
                        && matches!(api_key, ApiKey::SaslHandshake | ApiKey::SaslAuthenticate)
                    {
                        return Err(anyhow!(
                            "{} unexpected downstream SASL frame {:?} after auth bootstrap; closing connection",
                            conn_ctx,
                            api_key
                        ));
                    }
                    let mut map = in_flight.lock().await;
                    map.insert(correlation_id, (api_key, api_version));
                    tracing::debug!(
                        "{} received client request api={:?} version={} corr_id={} bytes={}",
                        conn_ctx,
                        api_key,
                        api_version,
                        correlation_id,
                        frame_len
                    );
                }
                Err(_) => {
                    warn!(
                        "Client {} sent request with unknown API key {}; forwarding request as passthrough (no rewrite tracking for this correlation id)",
                        client_addr, api_key_raw
                    );
                }
            }
        }

        let remaining = frame_len.saturating_sub(header_len);
        if remaining > 0 {
            ensure_buffer_capacity(
                &mut scratch,
                remaining.min(64 * 1024),
                "client -> upstream",
                conn_ctx,
            );
            ProxyStats::update_max(&stats.max_c2u_buffer, scratch.len());
            copy_exact_n(
                client_read,
                upstream_write,
                remaining,
                scratch.as_mut_slice(),
            )
            .await?;
        }
        tracing::debug!("{} wrote {} bytes to upstream", conn_ctx, frame_len + 4);
    }

    upstream_write
        .shutdown()
        .await
        .context("Failed to shutdown upstream writer")?;

    info!("{} client->upstream forwarding ended for {}", conn_ctx, client_addr);
    Ok(())
}

async fn forward_upstream_to_client<R, W>(
    upstream_read: &mut R,
    client_write: &mut W,
    in_flight: Arc<Mutex<HashMap<i32, (ApiKey, i16)>>>,
    upstream_routes: Arc<Mutex<HashMap<String, String>>>,
    upstream_route_owners: Arc<Mutex<HashMap<String, i32>>>,
    advertise_mode: Arc<BrokerAdvertiseMode>,
    listener_spawner: Option<Arc<ListenerSpawner>>,
    client_addr: SocketAddr,
    conn_ctx: &str,
    stats: Arc<ProxyStats>,
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut scratch = vec![0u8; 1024];
    let mut rewrite_frame = BytesMut::with_capacity(1024);
    let mut fetch_size_ring = FetchSizeRing::new();
    ProxyStats::update_max(&stats.max_u2c_buffer, scratch.len());
    ProxyStats::update_max(&stats.max_rewrite_buffer, rewrite_frame.capacity());

    loop {
        let Some(frame_len) = read_frame_len(upstream_read).await? else {
            break;
        };
        stats.frames_upstream_to_client.fetch_add(1, Ordering::Relaxed);
        stats
            .bytes_upstream_to_client
            .fetch_add((frame_len + 4) as u64, Ordering::Relaxed);

        if frame_len < 4 {
            return Err(anyhow!(
                "Invalid Kafka response frame: length {} smaller than correlation id header",
                frame_len
            ));
        }

        let mut corr_buf = [0u8; 4];
        upstream_read
            .read_exact(&mut corr_buf)
            .await
            .context("Failed to read Kafka response correlation id")?;
        let correlation_id = i32::from_be_bytes(corr_buf);

        let maybe_rewritten = {
            let request_info = {
                let mut map = in_flight.lock().await;
                map.remove(&correlation_id)
            };

            if let Some((api_key, api_version)) = request_info {
                let should_rewrite = should_attempt_rewrite(api_key, api_version);

                if should_rewrite {
                    if frame_len > rewrite_frame.capacity() {
                        info!(
                            "{} increasing {} buffer from {} to {} on demand.",
                            conn_ctx,
                            "upstream -> client",
                            rewrite_frame.capacity(),
                            frame_len
                        );
                        rewrite_frame.reserve(frame_len - rewrite_frame.capacity());
                    }
                    rewrite_frame.resize(frame_len, 0);
                    ProxyStats::update_max(&stats.max_rewrite_buffer, rewrite_frame.capacity());
                    rewrite_frame[..4].copy_from_slice(&corr_buf);
                    upstream_read
                        .read_exact(&mut rewrite_frame[4..frame_len])
                        .await
                        .context("Failed to read full rewritable response payload")?;
                    let frame_bytes = rewrite_frame.split_to(frame_len).freeze();

                    let rewritten_payload = match maybe_rewrite_kafka_response(
                        &frame_bytes,
                        api_key,
                        api_version,
                        advertise_mode.as_ref(),
                        conn_ctx,
                    ) {
                        Ok(outcome) => {
                            let mut removed_broker_ids: Vec<i32> = Vec::new();
                            let mut added_broker_ids: Vec<i32> = Vec::new();
                            if let (Some(spawner), Some(snapshot_ids)) =
                                (listener_spawner.as_ref(), outcome.topology_snapshot_ids.as_ref())
                            {
                                let mut snapshot_set: HashSet<i32> = HashSet::new();
                                for id in snapshot_ids {
                                    snapshot_set.insert(*id);
                                }
                                let mut snapshot_topology: HashMap<i32, String> = HashMap::new();
                                for route in &outcome.discovered_routes {
                                    if let Some(id) = parse_broker_route_key(&route.key) {
                                        if snapshot_set.contains(&id) {
                                            snapshot_topology.insert(id, route.upstream_addr.clone());
                                        }
                                    }
                                }

                                let mut before_topology: HashMap<i32, String> = HashMap::new();
                                {
                                    let routes = upstream_routes.lock().await;
                                    for (k, v) in routes.iter() {
                                        if let Some(id) = parse_broker_route_key(k) {
                                            before_topology.insert(id, v.clone());
                                        }
                                    }
                                }

                                let mut snapshot_ids_sorted = snapshot_ids.clone();
                                snapshot_ids_sorted.sort_unstable();
                                snapshot_ids_sorted.dedup();
                                for id in &snapshot_ids_sorted {
                                    if !before_topology.contains_key(id) {
                                        added_broker_ids.push(*id);
                                    }
                                }

                                if !outcome.discovered_routes.is_empty() {
                                    let route_keys: Vec<String> = outcome
                                        .discovered_routes
                                        .iter()
                                        .map(|route| route.key.clone())
                                        .collect();
                                    spawner
                                        .ensure_listeners_for_route_keys(&route_keys)
                                        .await
                                        .context("listener setup failed")?;
                                }
                                removed_broker_ids = spawner
                                    .reconcile_broker_listeners(snapshot_ids)
                                    .await
                                    .context("listener reconciliation failed")?;

                                info!(
                                    "{} backend topology transitioning from {} to {}",
                                    conn_ctx,
                                    format_topology_map(&before_topology),
                                    format_topology_map(&snapshot_topology)
                                );
                                for id in &added_broker_ids {
                                    if let Some(addr) = snapshot_topology.get(id) {
                                        info!(
                                            "{} adding new listener for {}->{}",
                                            conn_ctx, id, addr
                                        );
                                    }
                                }
                                for id in &removed_broker_ids {
                                    if let Some(addr) = before_topology.get(id) {
                                        info!(
                                            "{} removing listener for {}->{}",
                                            conn_ctx, id, addr
                                        );
                                    } else {
                                        info!(
                                            "{} removing listener for {}",
                                            conn_ctx, id
                                        );
                                    }
                                }
                            } else if !outcome.discovered_routes.is_empty() {
                                if let Some(spawner) = listener_spawner.as_ref() {
                                    let route_keys: Vec<String> = outcome
                                        .discovered_routes
                                        .iter()
                                        .map(|route| route.key.clone())
                                        .collect();
                                    spawner
                                        .ensure_listeners_for_route_keys(&route_keys)
                                        .await
                                        .context("listener setup failed")?;
                                }
                            }

                            if !outcome.discovered_routes.is_empty() || !removed_broker_ids.is_empty() {
                                let mut routes = upstream_routes.lock().await;
                                let mut route_owners = upstream_route_owners.lock().await;

                                let mut keep_keys_by_broker: HashMap<i32, HashSet<String>> = HashMap::new();
                                for route in &outcome.discovered_routes {
                                    keep_keys_by_broker
                                        .entry(route.broker_id)
                                        .or_default()
                                        .insert(route.key.clone());
                                }
                                for (broker_id, keep_keys) in &keep_keys_by_broker {
                                    remove_owned_route_keys(
                                        &mut routes,
                                        &mut route_owners,
                                        *broker_id,
                                        Some(keep_keys),
                                    );
                                }

                                for route in &outcome.discovered_routes {
                                    routes.insert(route.key.clone(), route.upstream_addr.clone());
                                    route_owners.insert(route.key.clone(), route.broker_id);
                                }
                                if let BrokerAdvertiseMode::PortOffset {
                                    hostname,
                                    base_port,
                                    bootstrap_reserved,
                                    ..
                                } = advertise_mode.as_ref()
                                {
                                    let broker_start = base_port.saturating_add(*bootstrap_reserved);
                                    for broker_id in &removed_broker_ids {
                                        remove_owned_route_keys(
                                            &mut routes,
                                            &mut route_owners,
                                            *broker_id,
                                            None,
                                        );
                                        routes.remove(&format!("b{}", broker_id));
                                        route_owners.remove(&format!("b{}", broker_id));
                                        if let Ok(id_u16) = u16::try_from(*broker_id) {
                                            let key = format!(
                                                "{}:{}",
                                                hostname,
                                                broker_start.saturating_add(id_u16)
                                            );
                                            routes.remove(&key);
                                            route_owners.remove(&key);
                                        }
                                    }
                                }
                            }
                            if let Some(rewritten) = outcome.payload {
                                stats.rewritten_responses.fetch_add(1, Ordering::Relaxed);
                                tracing::debug!(
                                    "{} received server response type={:?} corr_id={} bytes={} rewritten=true",
                                    conn_ctx,
                                    api_key,
                                    correlation_id,
                                    frame_len
                                );
                                Some(rewritten)
                            } else {
                                stats.passthrough_responses.fetch_add(1, Ordering::Relaxed);
                                tracing::debug!(
                                    "{} received server response type={:?} corr_id={} bytes={} rewritten=false",
                                    conn_ctx,
                                    api_key,
                                    correlation_id,
                                    frame_len
                                );
                                Some(frame_bytes)
                            }
                        }
                        Err(e) => {
                            return Err(anyhow!(
                                "{} rewrite failed for client {} (corr_id={}, api={:?}, version={}): {}",
                                conn_ctx,
                                client_addr,
                                correlation_id,
                                api_key,
                                api_version,
                                e
                            ));
                        }
                    };
                    if matches!(api_key, ApiKey::Fetch) {
                        fetch_size_ring.push(frame_len as usize);
                        let target = (fetch_size_ring.max() * 2).max(64 * 1024);
                        if target <= rewrite_frame.capacity() / 2 {
                            rewrite_frame = BytesMut::with_capacity(target);
                        }
                    }
                    rewritten_payload
                } else {
                    stats.passthrough_responses.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(
                        "{} received server response type={:?} corr_id={} bytes={} rewritten=false",
                        conn_ctx,
                        api_key,
                        correlation_id,
                        frame_len
                    );
                    write_frame_len(client_write, frame_len).await?;
                    client_write
                        .write_all(&corr_buf)
                        .await
                        .context("Failed to write Kafka response correlation id")?;
                    let remaining = frame_len - 4;
                    if remaining > 0 {
                        ensure_buffer_capacity(
                            &mut scratch,
                            remaining.min(64 * 1024),
                            "upstream -> client",
                            conn_ctx,
                        );
                        ProxyStats::update_max(&stats.max_u2c_buffer, scratch.len());
                        copy_exact_n(
                            upstream_read,
                            client_write,
                            remaining,
                            scratch.as_mut_slice(),
                        )
                        .await?;
                    }
                    tracing::debug!("{} wrote {} bytes to client", conn_ctx, frame_len + 4);
                    None
                }
            } else {
                stats.passthrough_responses.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    "{} received unmatched server response corr_id={} bytes={} rewritten=false",
                    conn_ctx,
                    correlation_id,
                    frame_len
                );
                write_frame_len(client_write, frame_len).await?;
                client_write
                    .write_all(&corr_buf)
                    .await
                    .context("Failed to write Kafka response correlation id")?;
                let remaining = frame_len - 4;
                if remaining > 0 {
                    ensure_buffer_capacity(
                        &mut scratch,
                        remaining.min(64 * 1024),
                        "upstream -> client",
                        conn_ctx,
                    );
                    ProxyStats::update_max(&stats.max_u2c_buffer, scratch.len());
                    copy_exact_n(
                        upstream_read,
                        client_write,
                        remaining,
                        scratch.as_mut_slice(),
                    )
                    .await?;
                }
                tracing::debug!("{} wrote {} bytes to client", conn_ctx, frame_len + 4);
                None
            }
        };

        if let Some(payload) = maybe_rewritten {
            write_kafka_frame(client_write, &payload).await?;
            tracing::debug!("{} wrote {} bytes to client", conn_ctx, payload.len() + 4);
        }
    }

    client_write
        .shutdown()
        .await
        .context("Failed to shutdown client writer")?;

    info!("{} upstream->client forwarding ended for {}", conn_ctx, client_addr);
    Ok(())
}

async fn proxy_connection(
    client_stream: TcpStream,
    acceptor: Option<TlsAcceptor>,
    upstream_tls_connector: Option<TlsConnector>,
    hosts_resolver: Option<Arc<HostsResolver>>,
    auth_swap: Option<Arc<AuthSwap>>,
    downstream_sasl_mechanism: Option<Arc<String>>,
    upstream_sasl_mechanism: Option<Arc<String>>,
    advertise_mode: Arc<BrokerAdvertiseMode>,
    listener_spawner: Option<Arc<ListenerSpawner>>,
    upstream_routes: Arc<Mutex<HashMap<String, String>>>,
    upstream_route_owners: Arc<Mutex<HashMap<String, i32>>>,
    default_upstream: Arc<String>,
    client_addr: SocketAddr,
    stats: Arc<ProxyStats>,
) -> Result<()> {
    let client_conn_id = short_id();
    let local_port = client_stream
        .local_addr()
        .context("Failed to read local listener address")?
        .port();
    let (downstream_stream, sni, downstream_tls_principal) = if let Some(acceptor) = acceptor {
        let tls_stream = acceptor
            .accept(client_stream)
            .await
            .context("TLS handshake failed")?;
        let (_io, server_conn) = tls_stream.get_ref();
        let sni = server_conn.server_name().unwrap_or("").to_string();
        let mut principal: Option<String> = Some("$anonymous".to_string());
        if let Some(chain) = server_conn.peer_certificates() {
            if let Some(leaf) = chain.first() {
                if let Some((subject_dn, issuer_dn)) = cert_dn_summary(leaf.as_ref()) {
                    info!(
                        "client_conn={} authenticated client certificate subject_dn='{}' issuer_dn='{}'",
                        client_conn_id, subject_dn, issuer_dn
                    );
                } else {
                    info!(
                        "client_conn={} authenticated client certificate present, but DN parse failed",
                        client_conn_id
                    );
                }
                if let Some(cn) = cert_common_name(leaf.as_ref()) {
                    principal = Some(cn.clone());
                    info!(
                        "client_conn={} mTLS principal derived from certificate CN='{}'",
                        client_conn_id, cn
                    );
                }
            }
        }
        (EitherDownstreamStream::Tls(tls_stream), sni, principal)
    } else {
        let peer = client_stream.peer_addr().ok();
        info!(
            "client_conn={} plaintext downstream connection from {:?}",
            client_conn_id, peer
        );
        (EitherDownstreamStream::Plain(client_stream), String::new(), None)
    };

    if matches!(advertise_mode.as_ref(), BrokerAdvertiseMode::Sni { .. }) && sni.is_empty() {
        return Err(anyhow!("No SNI provided by client"));
    }

    info!(
        "client_conn={} client={} connected with SNI={}",
        client_conn_id,
        client_addr,
        if sni.is_empty() { "<none>" } else { sni.as_str() }
    );

    let (upstream_addr, used_default) = {
        let routes = upstream_routes.lock().await;
        let ingress = IngressRouteContext::from_connection(&sni, local_port, advertise_mode.as_ref());
        let (target, used_default, matched_key, reject_reason) =
            resolve_upstream_for_ingress(&routes, &ingress, advertise_mode.as_ref(), default_upstream.as_str());
        if let Some(reason) = reject_reason {
            return Err(anyhow!(reason));
        }
        let target = target.ok_or_else(|| anyhow!("No upstream route resolved"))?;
        if let Some(key) = matched_key {
            info!(
                "client_conn={} ingress mapping resolved via key='{}' to upstream={}",
                client_conn_id, key, target
            );
        }
        (target, used_default)
    };

    if used_default {
        info!(
            "client_conn={} no learned upstream for SNI {} yet; using default upstream {}",
            client_conn_id, sni, upstream_addr
        );
    }

    info!(
        "client_conn={} routing client={} sni={} to upstream={}",
        client_conn_id, client_addr, sni, upstream_addr
    );

    let upstream_host = upstream_host(&upstream_addr)?.to_string();
    let upstream_port = upstream_addr
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("Invalid upstream '{}': expected host:port", upstream_addr))?
        .1;

    let dial_addr = if let Some(resolver) = hosts_resolver.as_ref() {
        if let Some(ip) = resolver.resolve(&upstream_host) {
            info!(
                "client_conn={} hosts-file override resolved upstream host '{}' to {}",
                client_conn_id, upstream_host, ip
            );
            format!("{}:{}", ip, upstream_port)
        } else {
            info!(
                "client_conn={} no hosts-file match for upstream host '{}'; using system DNS",
                client_conn_id, upstream_host
            );
            upstream_addr.clone()
        }
    } else {
        upstream_addr.clone()
    };

    let upstream_stream = {
        const MAX_RETRIES: usize = 3;
        const TOTAL_ATTEMPTS: usize = MAX_RETRIES + 1;
        let mut attempt = 1usize;

        loop {
            let connect_result = async {
                let upstream_tcp_stream = TcpStream::connect(&dial_addr)
                    .await
                    .context(format!(
                        "Failed to connect to upstream: {} (dialed {})",
                        upstream_addr, dial_addr
                    ))?;

                if let Some(connector) = &upstream_tls_connector {
                    let upstream_server_name =
                        rustls::pki_types::ServerName::try_from(upstream_host.clone()).context(
                            format!(
                                "Invalid upstream TLS server name extracted from '{}'",
                                upstream_addr
                            ),
                        )?;

                    let upstream_tls_stream = connector
                        .connect(upstream_server_name, upstream_tcp_stream)
                        .await
                        .context(format!(
                            "Failed TLS handshake with upstream broker {}",
                            upstream_addr
                        ))?;
                    Ok::<_, anyhow::Error>(EitherUpstreamStream::Tls(upstream_tls_stream))
                } else {
                    Ok::<_, anyhow::Error>(EitherUpstreamStream::Plain(upstream_tcp_stream))
                }
            }
            .await;

            match connect_result {
                Ok(stream) => break stream,
                Err(err) => {
                    if attempt >= TOTAL_ATTEMPTS {
                        return Err(anyhow!(
                            "Failed to establish upstream connection to {} after {} attempts: {}",
                            upstream_addr,
                            TOTAL_ATTEMPTS,
                            err
                        ));
                    }

                    let delay_ms = rand::thread_rng().gen_range(1000..=3000);
                    tracing::debug!(
                        "client_conn={} upstream connect attempt {}/{} to {} failed: {}. Retrying in {}ms",
                        client_conn_id,
                        attempt,
                        TOTAL_ATTEMPTS,
                        upstream_addr,
                        err,
                        delay_ms
                    );
                    sleep(Duration::from_millis(delay_ms)).await;
                    attempt += 1;
                }
            }
        }
    };

    let upstream_conn_id = short_id();
    let conn_ctx = format!("client_conn={} upstream_conn={}", client_conn_id, upstream_conn_id);
    info!(
        "{} connected to upstream {} with {} for client {}",
        conn_ctx,
        upstream_addr,
        if upstream_tls_connector.is_some() {
            "TLS"
        } else {
            "PLAINTEXT"
        },
        client_addr
    );

    run_proxy_data_plane(
        downstream_stream,
        upstream_stream,
        auth_swap,
        downstream_sasl_mechanism,
        upstream_sasl_mechanism,
        downstream_tls_principal,
        advertise_mode,
        listener_spawner,
        upstream_routes,
        upstream_route_owners,
        client_addr,
        conn_ctx,
        stats,
    )
    .await
}

enum EitherUpstreamStream {
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
    Plain(TcpStream),
}

enum EitherDownstreamStream {
    Tls(DownstreamTlsStream<TcpStream>),
    Plain(TcpStream),
}

async fn run_proxy_data_plane(
    mut downstream_stream: EitherDownstreamStream,
    mut upstream_stream: EitherUpstreamStream,
    auth_swap: Option<Arc<AuthSwap>>,
    downstream_sasl_mechanism: Option<Arc<String>>,
    upstream_sasl_mechanism: Option<Arc<String>>,
    downstream_tls_principal: Option<String>,
    advertise_mode: Arc<BrokerAdvertiseMode>,
    listener_spawner: Option<Arc<ListenerSpawner>>,
    upstream_routes: Arc<Mutex<HashMap<String, String>>>,
    upstream_route_owners: Arc<Mutex<HashMap<String, i32>>>,
    client_addr: SocketAddr,
    conn_ctx: String,
    stats: Arc<ProxyStats>,
) -> Result<()> {
    if let Some(auth_swap) = auth_swap.as_ref() {
        let bootstrap_result = match (&mut downstream_stream, &mut upstream_stream) {
            (EitherDownstreamStream::Tls(ds), EitherUpstreamStream::Tls(us)) => {
                run_auth_swap_bootstrap(
                    ds,
                    us,
                    auth_swap,
                    downstream_sasl_mechanism.as_deref().map(|s| s.as_str()),
                    upstream_sasl_mechanism.as_deref().map(|s| s.as_str()),
                    downstream_tls_principal.as_deref(),
                    &conn_ctx,
                )
                .await
            }
            (EitherDownstreamStream::Tls(ds), EitherUpstreamStream::Plain(us)) => {
                run_auth_swap_bootstrap(
                    ds,
                    us,
                    auth_swap,
                    downstream_sasl_mechanism.as_deref().map(|s| s.as_str()),
                    upstream_sasl_mechanism.as_deref().map(|s| s.as_str()),
                    downstream_tls_principal.as_deref(),
                    &conn_ctx,
                )
                .await
            }
            (EitherDownstreamStream::Plain(ds), EitherUpstreamStream::Tls(us)) => {
                run_auth_swap_bootstrap(
                    ds,
                    us,
                    auth_swap,
                    downstream_sasl_mechanism.as_deref().map(|s| s.as_str()),
                    upstream_sasl_mechanism.as_deref().map(|s| s.as_str()),
                    downstream_tls_principal.as_deref(),
                    &conn_ctx,
                )
                .await
            }
            (EitherDownstreamStream::Plain(ds), EitherUpstreamStream::Plain(us)) => {
                run_auth_swap_bootstrap(
                    ds,
                    us,
                    auth_swap,
                    downstream_sasl_mechanism.as_deref().map(|s| s.as_str()),
                    upstream_sasl_mechanism.as_deref().map(|s| s.as_str()),
                    downstream_tls_principal.as_deref(),
                    &conn_ctx,
                )
                .await
            }
        };
        if let Err(err) = bootstrap_result {
            error!("{} auth bootstrap failed for client {}: {}", conn_ctx, client_addr, err);
            return Err(anyhow!("auth bootstrap failed"));
        }
        info!("{} auth bootstrap completed", conn_ctx);
    }

    let (mut client_read, mut client_write) = match downstream_stream {
        EitherDownstreamStream::Tls(ds) => {
            let (r, w) = tokio::io::split(ds);
            (EitherDownstreamReadHalf::Tls(r), EitherDownstreamWriteHalf::Tls(w))
        }
        EitherDownstreamStream::Plain(ds) => {
            let (r, w) = tokio::io::split(ds);
            (EitherDownstreamReadHalf::Plain(r), EitherDownstreamWriteHalf::Plain(w))
        }
    };
    let (mut upstream_read, mut upstream_write) = match upstream_stream {
        EitherUpstreamStream::Tls(us) => {
            let (r, w) = tokio::io::split(us);
            (EitherReadHalf::Tls(r), EitherWriteHalf::Tls(w))
        }
        EitherUpstreamStream::Plain(us) => {
            let (r, w) = tokio::io::split(us);
            (EitherReadHalf::Plain(r), EitherWriteHalf::Plain(w))
        }
    };

    let in_flight: Arc<Mutex<HashMap<i32, (ApiKey, i16)>>> = Arc::new(Mutex::new(HashMap::new()));
    let reject_downstream_sasl_frames = auth_swap.is_some();

    let client_to_upstream = forward_client_to_upstream(
        &mut client_read,
        &mut upstream_write,
        in_flight.clone(),
        client_addr,
        reject_downstream_sasl_frames,
        &conn_ctx,
        stats.clone(),
    );

    let upstream_to_client = forward_upstream_to_client(
        &mut upstream_read,
        &mut client_write,
        in_flight,
        upstream_routes,
        upstream_route_owners,
        advertise_mode,
        listener_spawner,
        client_addr,
        &conn_ctx,
        stats.clone(),
    );

    tokio::pin!(client_to_upstream);
    tokio::pin!(upstream_to_client);

    enum FirstDone {
        C2U(Result<()>),
        U2C(Result<()>),
    }

    let first = tokio::select! {
        r = &mut client_to_upstream => FirstDone::C2U(r),
        r = &mut upstream_to_client => FirstDone::U2C(r),
    };

    match first {
        FirstDone::C2U(Err(e)) => {
            return Err(anyhow!("client->upstream error for {}: {}", client_addr, e));
        }
        FirstDone::U2C(Err(e)) => {
            return Err(anyhow!("upstream->client error for {}: {}", client_addr, e));
        }
        FirstDone::C2U(Ok(())) => {
            if let Err(e) = upstream_to_client.await {
                return Err(anyhow!("upstream->client error for {}: {}", client_addr, e));
            }
        }
        FirstDone::U2C(Ok(())) => {
            if let Err(e) = client_to_upstream.await {
                return Err(anyhow!("client->upstream error for {}: {}", client_addr, e));
            }
        }
    }

    info!("{} connection closed for client {}", conn_ctx, client_addr);
    Ok(())
}

enum EitherReadHalf {
    Tls(tokio::io::ReadHalf<tokio_rustls::client::TlsStream<TcpStream>>),
    Plain(tokio::io::ReadHalf<TcpStream>),
}

enum EitherDownstreamReadHalf {
    Tls(tokio::io::ReadHalf<DownstreamTlsStream<TcpStream>>),
    Plain(tokio::io::ReadHalf<TcpStream>),
}

enum EitherDownstreamWriteHalf {
    Tls(tokio::io::WriteHalf<DownstreamTlsStream<TcpStream>>),
    Plain(tokio::io::WriteHalf<TcpStream>),
}

impl AsyncRead for EitherDownstreamReadHalf {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            EitherDownstreamReadHalf::Tls(r) => std::pin::Pin::new(r).poll_read(cx, buf),
            EitherDownstreamReadHalf::Plain(r) => std::pin::Pin::new(r).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for EitherDownstreamWriteHalf {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match &mut *self {
            EitherDownstreamWriteHalf::Tls(w) => std::pin::Pin::new(w).poll_write(cx, buf),
            EitherDownstreamWriteHalf::Plain(w) => std::pin::Pin::new(w).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            EitherDownstreamWriteHalf::Tls(w) => std::pin::Pin::new(w).poll_flush(cx),
            EitherDownstreamWriteHalf::Plain(w) => std::pin::Pin::new(w).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            EitherDownstreamWriteHalf::Tls(w) => std::pin::Pin::new(w).poll_shutdown(cx),
            EitherDownstreamWriteHalf::Plain(w) => std::pin::Pin::new(w).poll_shutdown(cx),
        }
    }
}

enum EitherWriteHalf {
    Tls(tokio::io::WriteHalf<tokio_rustls::client::TlsStream<TcpStream>>),
    Plain(tokio::io::WriteHalf<TcpStream>),
}

impl AsyncRead for EitherReadHalf {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            EitherReadHalf::Tls(r) => std::pin::Pin::new(r).poll_read(cx, buf),
            EitherReadHalf::Plain(r) => std::pin::Pin::new(r).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for EitherWriteHalf {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match &mut *self {
            EitherWriteHalf::Tls(w) => std::pin::Pin::new(w).poll_write(cx, buf),
            EitherWriteHalf::Plain(w) => std::pin::Pin::new(w).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            EitherWriteHalf::Tls(w) => std::pin::Pin::new(w).poll_flush(cx),
            EitherWriteHalf::Plain(w) => std::pin::Pin::new(w).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            EitherWriteHalf::Tls(w) => std::pin::Pin::new(w).poll_shutdown(cx),
            EitherWriteHalf::Plain(w) => std::pin::Pin::new(w).poll_shutdown(cx),
        }
    }
}

pub async fn serve(
    bind: &str,
    acceptor: Option<TlsAcceptor>,
    upstream_tls_connector: Option<TlsConnector>,
    hosts_resolver: Option<Arc<HostsResolver>>,
    auth_swap: Option<Arc<AuthSwap>>,
    downstream_sasl_mechanism: Option<Arc<String>>,
    upstream_sasl_mechanism: Option<Arc<String>>,
    advertise_mode: Arc<BrokerAdvertiseMode>,
    upstream_routes: Arc<Mutex<HashMap<String, String>>>,
    upstream_route_owners: Arc<Mutex<HashMap<String, i32>>>,
    default_upstream: Arc<String>,
    stats: Arc<ProxyStats>,
) -> Result<()> {
    let bind_host = bind_host_part(bind)?;
    let listener_spawner = if matches!(advertise_mode.as_ref(), BrokerAdvertiseMode::PortOffset { .. }) {
        Some(Arc::new(ListenerSpawner::new(
            bind_host.clone(),
            acceptor.clone(),
            upstream_tls_connector.clone(),
            hosts_resolver.clone(),
            auth_swap.clone(),
            downstream_sasl_mechanism.clone(),
            upstream_sasl_mechanism.clone(),
            advertise_mode.clone(),
            upstream_routes.clone(),
            upstream_route_owners.clone(),
            default_upstream.clone(),
            stats.clone(),
        )))
    } else {
        None
    };

    let listeners = bind_listeners(bind, advertise_mode.as_ref()).await?;
    for listener in listeners {
        let acceptor = acceptor.clone();
        let upstream_tls_connector = upstream_tls_connector.clone();
        let hosts_resolver = hosts_resolver.clone();
        let auth_swap = auth_swap.clone();
        let downstream_sasl_mechanism = downstream_sasl_mechanism.clone();
        let upstream_sasl_mechanism = upstream_sasl_mechanism.clone();
        let advertise_mode = advertise_mode.clone();
        let listener_spawner = listener_spawner.clone();
        let upstream_routes = upstream_routes.clone();
        let upstream_route_owners = upstream_route_owners.clone();
        let default_upstream = default_upstream.clone();
        let stats = stats.clone();

        spawn_accept_loop(
            listener,
            acceptor,
            upstream_tls_connector,
            hosts_resolver,
            auth_swap,
            downstream_sasl_mechanism,
            upstream_sasl_mechanism,
            advertise_mode,
            listener_spawner,
            upstream_routes,
            upstream_route_owners,
            default_upstream,
            stats,
            None,
        );
    }

    std::future::pending::<()>().await;
    #[allow(unreachable_code)]
    Ok(())
}

async fn bind_listeners(bind: &str, mode: &BrokerAdvertiseMode) -> Result<Vec<TcpListener>> {
    match mode {
        BrokerAdvertiseMode::Sni { .. } => {
            let listener = TcpListener::bind(bind)
                .await
                .context(format!("Failed to bind to {}", bind))?;
            info!("Listening on {}", bind);
            Ok(vec![listener])
        }
        BrokerAdvertiseMode::PortOffset {
            base_port,
            bootstrap_count,
            ..
        } => {
            let host = bind_host_part(bind)?;
            let mut listeners = Vec::new();
            for slot in 0..*bootstrap_count {
                let addr = format!("{}:{}", host, base_port + slot);
                let listener = TcpListener::bind(&addr)
                    .await
                    .context(format!("Failed to bind to {}", addr))?;
                info!("Listening on {} (port_offset bootstrap slot {})", addr, slot);
                listeners.push(listener);
            }
            Ok(listeners)
        }
    }
}

fn bind_host_part(bind: &str) -> Result<String> {
    if let Ok(addr) = bind.parse::<std::net::SocketAddr>() {
        return Ok(addr.ip().to_string());
    }
    bind.rsplit_once(':')
        .map(|(h, _)| h.to_string())
        .ok_or_else(|| anyhow!("Invalid bind address '{}': expected host:port", bind))
}

#[derive(Clone)]
struct ListenerSpawner {
    bind_host: String,
    acceptor: Option<TlsAcceptor>,
    upstream_tls_connector: Option<TlsConnector>,
    hosts_resolver: Option<Arc<HostsResolver>>,
    auth_swap: Option<Arc<AuthSwap>>,
    downstream_sasl_mechanism: Option<Arc<String>>,
    upstream_sasl_mechanism: Option<Arc<String>>,
    advertise_mode: Arc<BrokerAdvertiseMode>,
    upstream_routes: Arc<Mutex<HashMap<String, String>>>,
    upstream_route_owners: Arc<Mutex<HashMap<String, i32>>>,
    default_upstream: Arc<String>,
    stats: Arc<ProxyStats>,
    active_ports: Arc<Mutex<HashMap<u16, oneshot::Sender<()>>>>,
}

impl ListenerSpawner {
    fn new(
        bind_host: String,
        acceptor: Option<TlsAcceptor>,
        upstream_tls_connector: Option<TlsConnector>,
        hosts_resolver: Option<Arc<HostsResolver>>,
        auth_swap: Option<Arc<AuthSwap>>,
        downstream_sasl_mechanism: Option<Arc<String>>,
        upstream_sasl_mechanism: Option<Arc<String>>,
        advertise_mode: Arc<BrokerAdvertiseMode>,
        upstream_routes: Arc<Mutex<HashMap<String, String>>>,
        upstream_route_owners: Arc<Mutex<HashMap<String, i32>>>,
        default_upstream: Arc<String>,
        stats: Arc<ProxyStats>,
    ) -> Self {
        Self {
            bind_host,
            acceptor,
            upstream_tls_connector,
            hosts_resolver,
            auth_swap,
            downstream_sasl_mechanism,
            upstream_sasl_mechanism,
            advertise_mode,
            upstream_routes,
            upstream_route_owners,
            default_upstream,
            stats,
            active_ports: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn ensure_listeners_for_route_keys(&self, route_keys: &[String]) -> Result<()> {
        let BrokerAdvertiseMode::PortOffset {
            hostname,
            base_port,
            bootstrap_reserved,
            max_broker_id,
            ..
        } = self.advertise_mode.as_ref() else {
            return Ok(());
        };

        let broker_start = base_port.saturating_add(*bootstrap_reserved);
        let max_port = broker_start.saturating_add(*max_broker_id);
        let mut wanted_ports = Vec::new();
        for route_key in route_keys {
            let Some((host, port_str)) = route_key.rsplit_once(':') else {
                continue;
            };
            if host != hostname {
                continue;
            }
            let port = port_str
                .parse::<u16>()
                .context(format!("Invalid learned route port '{}'", port_str))?;
            if port < broker_start {
                continue;
            }
            if port > max_port {
                return Err(anyhow!(
                    "fatal port_offset listener range violation: learned broker port {} outside configured range {}..={}",
                    port,
                    broker_start,
                    max_port
                ));
            }
            wanted_ports.push(port);
        }

        if wanted_ports.is_empty() {
            return Ok(());
        }
        wanted_ports.sort_unstable();
        wanted_ports.dedup();

        let to_bind = {
            let active = self.active_ports.lock().await;
            let mut pending = Vec::new();
            for port in wanted_ports {
                if !active.contains_key(&port) {
                    pending.push(port);
                }
            }
            pending
        };
        if to_bind.is_empty() {
            return Ok(());
        }

        // Bind all listeners first; publish them only when all binds succeeded.
        let mut bound = Vec::new();
        for port in &to_bind {
            let addr = format!("{}:{}", self.bind_host, port);
            let listener = TcpListener::bind(&addr)
                .await
                .with_context(|| {
                    format!(
                        "fatal port_offset listener bind failure on {} (possibly port in use)",
                        addr
                    )
                })?;
            bound.push((*port, addr, listener));
        }
        for (port, addr, listener) in bound {
            info!("Listening on {} (port_offset learned broker)", addr);

            let acceptor = self.acceptor.clone();
            let upstream_tls_connector = self.upstream_tls_connector.clone();
            let hosts_resolver = self.hosts_resolver.clone();
            let auth_swap = self.auth_swap.clone();
            let downstream_sasl_mechanism = self.downstream_sasl_mechanism.clone();
            let upstream_sasl_mechanism = self.upstream_sasl_mechanism.clone();
            let advertise_mode = self.advertise_mode.clone();
            let listener_spawner = Some(Arc::new(self.clone()));
            let upstream_routes = self.upstream_routes.clone();
            let upstream_route_owners = self.upstream_route_owners.clone();
            let default_upstream = self.default_upstream.clone();
            let stats = self.stats.clone();
            let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

            {
                let mut active = self.active_ports.lock().await;
                active.insert(port, shutdown_tx);
            }

            spawn_accept_loop(
                listener,
                acceptor,
                upstream_tls_connector,
                hosts_resolver,
                auth_swap,
                downstream_sasl_mechanism,
                upstream_sasl_mechanism,
                advertise_mode,
                listener_spawner,
                upstream_routes,
                upstream_route_owners,
                default_upstream,
                stats,
                Some(shutdown_rx),
            );
        }

        Ok(())
    }

    async fn reconcile_broker_listeners(&self, broker_ids: &[i32]) -> Result<Vec<i32>> {
        let BrokerAdvertiseMode::PortOffset {
            base_port,
            bootstrap_reserved,
            max_broker_id,
            ..
        } = self.advertise_mode.as_ref() else {
            return Ok(Vec::new());
        };

        let broker_start = base_port.saturating_add(*bootstrap_reserved);
        let max_port = broker_start.saturating_add(*max_broker_id);
        let mut desired_ports: HashSet<u16> = HashSet::new();
        for id in broker_ids {
            if *id < 0 {
                return Err(anyhow!(
                    "port_offset mapping error: negative broker id {} is not supported",
                    id
                ));
            }
            let id_u16 = u16::try_from(*id).unwrap_or(u16::MAX);
            if id_u16 > *max_broker_id {
                return Err(anyhow!(
                    "port_offset mapping error: broker id {} exceeds configured max_broker_id {}",
                    id,
                    max_broker_id
                ));
            }
            let port = broker_start.saturating_add(id_u16);
            if port < broker_start || port > max_port {
                return Err(anyhow!(
                    "fatal port_offset listener range violation: learned broker port {} outside configured range {}..={}",
                    port,
                    broker_start,
                    max_port
                ));
            }
            desired_ports.insert(port);
        }

        let mut removed = Vec::new();
        let mut to_shutdown = Vec::new();
        {
            let mut active = self.active_ports.lock().await;
            let existing_ports: Vec<u16> = active.keys().copied().collect();
            for port in existing_ports {
                if !desired_ports.contains(&port) {
                    if let Some(tx) = active.remove(&port) {
                        to_shutdown.push((port, tx));
                    }
                }
            }
        }
        for (port, tx) in to_shutdown {
            let _ = tx.send(());
            let id = i32::from(port.saturating_sub(broker_start));
            removed.push(id);
            info!(
                "stopped listener on {}:{} for removed broker id {}",
                self.bind_host, port, id
            );
        }

        Ok(removed)
    }
}

fn spawn_accept_loop(
    listener: TcpListener,
    acceptor: Option<TlsAcceptor>,
    upstream_tls_connector: Option<TlsConnector>,
    hosts_resolver: Option<Arc<HostsResolver>>,
    auth_swap: Option<Arc<AuthSwap>>,
    downstream_sasl_mechanism: Option<Arc<String>>,
    upstream_sasl_mechanism: Option<Arc<String>>,
    advertise_mode: Arc<BrokerAdvertiseMode>,
    listener_spawner: Option<Arc<ListenerSpawner>>,
    upstream_routes: Arc<Mutex<HashMap<String, String>>>,
    upstream_route_owners: Arc<Mutex<HashMap<String, i32>>>,
    default_upstream: Arc<String>,
    stats: Arc<ProxyStats>,
    shutdown_rx: Option<oneshot::Receiver<()>>,
) {
    tokio::spawn(async move {
        Box::pin(run_accept_loop(
            listener,
            acceptor,
            upstream_tls_connector,
            hosts_resolver,
            auth_swap,
            downstream_sasl_mechanism,
            upstream_sasl_mechanism,
            advertise_mode,
            listener_spawner,
            upstream_routes,
            upstream_route_owners,
            default_upstream,
            stats,
            shutdown_rx,
        ))
        .await;
    });
}

async fn run_accept_loop(
    listener: TcpListener,
    acceptor: Option<TlsAcceptor>,
    upstream_tls_connector: Option<TlsConnector>,
    hosts_resolver: Option<Arc<HostsResolver>>,
    auth_swap: Option<Arc<AuthSwap>>,
    downstream_sasl_mechanism: Option<Arc<String>>,
    upstream_sasl_mechanism: Option<Arc<String>>,
    advertise_mode: Arc<BrokerAdvertiseMode>,
    listener_spawner: Option<Arc<ListenerSpawner>>,
    upstream_routes: Arc<Mutex<HashMap<String, String>>>,
    upstream_route_owners: Arc<Mutex<HashMap<String, i32>>>,
    default_upstream: Arc<String>,
    stats: Arc<ProxyStats>,
    mut shutdown_rx: Option<oneshot::Receiver<()>>,
) {
    loop {
        let accepted = if let Some(rx) = shutdown_rx.as_mut() {
            tokio::select! {
                _ = rx => break,
                res = listener.accept() => res,
            }
        } else {
            listener.accept().await
        };
        match accepted {
            Ok((stream, addr)) => {
                let acceptor = acceptor.clone();
                let upstream_tls_connector = upstream_tls_connector.clone();
                let hosts_resolver = hosts_resolver.clone();
                let auth_swap = auth_swap.clone();
                let downstream_sasl_mechanism = downstream_sasl_mechanism.clone();
                let upstream_sasl_mechanism = upstream_sasl_mechanism.clone();
                let advertise_mode = advertise_mode.clone();
                let listener_spawner = listener_spawner.clone();
                let upstream_routes = upstream_routes.clone();
                let upstream_route_owners = upstream_route_owners.clone();
                let default_upstream = default_upstream.clone();
                let stats = stats.clone();

                tokio::spawn(async move {
                    if let Err(e) = proxy_connection(
                        stream,
                        acceptor,
                        upstream_tls_connector,
                        hosts_resolver,
                        auth_swap,
                        downstream_sasl_mechanism,
                        upstream_sasl_mechanism,
                        advertise_mode,
                        listener_spawner,
                        upstream_routes,
                        upstream_route_owners,
                        default_upstream,
                        addr,
                        stats,
                    )
                    .await
                    {
                        if is_fatal_proxy_error(&e) {
                            error!("Fatal proxy error detected; shutting down process: {}", e);
                            std::process::exit(1);
                        } else {
                            info!("Connection from {} closed: {}", addr, e);
                        }
                    }
                });
            }
            Err(e) => {
                error!("Failed to accept connection: {}", e);
            }
        }
    }
}

fn is_fatal_proxy_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        let msg = cause.to_string();
        msg.contains("port_offset mapping error")
            || msg.contains("listener setup failed")
            || msg.contains("fatal port_offset listener")
    })
}
