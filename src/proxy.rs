use crate::hosts::HostsResolver;
use crate::kafka_io::{
    copy_exact_n, read_frame_len, write_frame_len, write_kafka_frame,
};
use crate::rewrite::{maybe_rewrite_kafka_response, BrokerMappingRule};
use crate::stats::ProxyStats;
use crate::upstream::upstream_host;
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use kafka_protocol::messages::ApiKey;
use rand::Rng;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio_rustls::rustls;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::{error, info, warn};
use uuid::Uuid;

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

fn short_id() -> String {
    Uuid::new_v4().simple().to_string()[..8].to_string()
}

async fn forward_client_to_upstream<R, W>(
    client_read: &mut R,
    upstream_write: &mut W,
    in_flight: Arc<Mutex<HashMap<i32, (ApiKey, i16)>>>,
    client_addr: SocketAddr,
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
                        "Client {} sent request with unknown API key {}",
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
    broker_mapping_rule: Option<Arc<BrokerMappingRule>>,
    sni_suffix: &str,
    bind_port: u16,
    client_addr: SocketAddr,
    conn_ctx: &str,
    stats: Arc<ProxyStats>,
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut scratch = vec![0u8; 1024];
    let mut rewrite_frame = vec![0u8; 1024];
    ProxyStats::update_max(&stats.max_u2c_buffer, scratch.len());
    ProxyStats::update_max(&stats.max_rewrite_buffer, rewrite_frame.len());

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
                let should_rewrite = matches!(
                    api_key,
                    ApiKey::Metadata | ApiKey::FindCoordinator | ApiKey::DescribeCluster
                );

                if should_rewrite {
                    ensure_buffer_capacity(
                        &mut rewrite_frame,
                        frame_len,
                        "upstream -> client",
                        conn_ctx,
                    );
                    ProxyStats::update_max(&stats.max_rewrite_buffer, rewrite_frame.len());
                    rewrite_frame[..4].copy_from_slice(&corr_buf);
                    upstream_read
                        .read_exact(&mut rewrite_frame[4..frame_len])
                        .await
                        .context("Failed to read full rewritable response payload")?;

                    match maybe_rewrite_kafka_response(
                        &rewrite_frame[..frame_len],
                        api_key,
                        api_version,
                        sni_suffix,
                        bind_port,
                        broker_mapping_rule.as_deref(),
                        conn_ctx,
                    ) {
                        Ok(outcome) => {
                            if !outcome.discovered_routes.is_empty() {
                                let mut routes = upstream_routes.lock().await;
                                for (broker, addr) in outcome.discovered_routes {
                                    routes.insert(broker, addr);
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
                                Some(Bytes::copy_from_slice(&rewrite_frame[..frame_len]))
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Failed to rewrite Kafka response for client {} (corr_id={}, api={:?}, version={}): {}",
                                client_addr, correlation_id, api_key, api_version, e
                            );
                            Some(Bytes::copy_from_slice(&rewrite_frame[..frame_len]))
                        }
                    }
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
    acceptor: TlsAcceptor,
    upstream_tls_connector: TlsConnector,
    hosts_resolver: Option<Arc<HostsResolver>>,
    upstream_routes: Arc<Mutex<HashMap<String, String>>>,
    default_upstream: Arc<String>,
    broker_mapping_rule: Option<Arc<BrokerMappingRule>>,
    sni_suffix: Arc<String>,
    bind_port: u16,
    client_addr: SocketAddr,
    stats: Arc<ProxyStats>,
) -> Result<()> {
    let client_conn_id = short_id();
    let tls_stream = acceptor
        .accept(client_stream)
        .await
        .context("TLS handshake failed")?;

    let (_io, server_conn) = tls_stream.get_ref();
    let sni = server_conn
        .server_name()
        .ok_or_else(|| anyhow!("No SNI provided by client"))?;

    info!(
        "client_conn={} client={} connected with SNI={}",
        client_conn_id, client_addr, sni
    );

    let suffix_with_dot = format!(".{}", sni_suffix.as_str());
    let broker_name = if sni.ends_with(suffix_with_dot.as_str()) {
        sni.strip_suffix(suffix_with_dot.as_str())
            .ok_or_else(|| anyhow!("Failed to extract broker name from SNI"))?
    } else {
        ""
    };

    let (upstream_addr, used_default) = {
        let routes = upstream_routes.lock().await;
        if let Some(found) = routes.get(sni) {
            (found.clone(), false)
        } else if !broker_name.is_empty() {
            if let Some(found) = routes.get(broker_name) {
                (found.clone(), false)
            } else {
                (default_upstream.as_str().to_string(), true)
            }
        } else {
            (default_upstream.as_str().to_string(), true)
        }
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

    let upstream_tls_stream = {
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

                let upstream_server_name =
                    rustls::pki_types::ServerName::try_from(upstream_host.clone()).context(
                        format!(
                            "Invalid upstream TLS server name extracted from '{}'",
                            upstream_addr
                        ),
                    )?;

                let upstream_tls_stream = upstream_tls_connector
                    .connect(upstream_server_name, upstream_tcp_stream)
                    .await
                    .context(format!(
                        "Failed TLS handshake with upstream broker {}",
                        upstream_addr
                    ))?;

                Ok::<_, anyhow::Error>(upstream_tls_stream)
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
        "{} connected to upstream {} with TLS for client {}",
        conn_ctx, upstream_addr, client_addr
    );

    let (mut client_read, mut client_write) = tokio::io::split(tls_stream);
    let (mut upstream_read, mut upstream_write) = tokio::io::split(upstream_tls_stream);

    let in_flight: Arc<Mutex<HashMap<i32, (ApiKey, i16)>>> = Arc::new(Mutex::new(HashMap::new()));

    let client_to_upstream = forward_client_to_upstream(
        &mut client_read,
        &mut upstream_write,
        in_flight.clone(),
        client_addr,
        &conn_ctx,
        stats.clone(),
    );

    let upstream_to_client = forward_upstream_to_client(
        &mut upstream_read,
        &mut client_write,
        in_flight,
        upstream_routes,
        broker_mapping_rule,
        sni_suffix.as_str(),
        bind_port,
        client_addr,
        &conn_ctx,
        stats.clone(),
    );

    let (c2u_result, u2c_result) = tokio::join!(client_to_upstream, upstream_to_client);

    if let Err(e) = c2u_result {
        warn!("Client {} -> upstream error: {}", client_addr, e);
    }
    if let Err(e) = u2c_result {
        warn!("Upstream -> client {} error: {}", client_addr, e);
    }

    info!("{} connection closed for client {}", conn_ctx, client_addr);
    Ok(())
}

pub async fn serve(
    bind: &str,
    acceptor: TlsAcceptor,
    upstream_tls_connector: TlsConnector,
    hosts_resolver: Option<Arc<HostsResolver>>,
    upstream_routes: Arc<Mutex<HashMap<String, String>>>,
    default_upstream: Arc<String>,
    broker_mapping_rule: Option<Arc<BrokerMappingRule>>,
    sni_suffix: Arc<String>,
    bind_port: u16,
    stats: Arc<ProxyStats>,
) -> Result<()> {
    let listener = TcpListener::bind(bind)
        .await
        .context(format!("Failed to bind to {}", bind))?;

    info!("Listening on {}", bind);

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                let acceptor = acceptor.clone();
                let upstream_tls_connector = upstream_tls_connector.clone();
                let hosts_resolver = hosts_resolver.clone();
                let upstream_routes = upstream_routes.clone();
                let default_upstream = default_upstream.clone();
                let broker_mapping_rule = broker_mapping_rule.clone();
                let sni_suffix = sni_suffix.clone();
                let stats = stats.clone();

                tokio::spawn(async move {
                    if let Err(e) = proxy_connection(
                        stream,
                        acceptor,
                        upstream_tls_connector,
                        hosts_resolver,
                        upstream_routes,
                        default_upstream,
                        broker_mapping_rule,
                        sni_suffix,
                        bind_port,
                        addr,
                        stats,
                    )
                    .await
                    {
                        error!("Error handling connection from {}: {}", addr, e);
                    }
                });
            }
            Err(e) => {
                error!("Failed to accept connection: {}", e);
            }
        }
    }
}
