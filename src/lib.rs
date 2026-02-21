mod auth;
pub mod cli;
mod certs;
mod config;
mod env_expand;
mod hosts;
mod kafka_io;
mod proxy;
mod rewrite;
mod stats;
mod upstream;

use crate::auth::AuthSwap;
use crate::certs::{generate_default_certs, load_certs, load_private_key};
use crate::cli::Args;
use crate::config::FileConfig;
use crate::hosts::HostsResolver;
use crate::proxy::serve;
use crate::rewrite::{BrokerAdvertiseMode, BrokerMappingRule, PortOffsetState};
use crate::stats::ProxyStats;
use crate::upstream::{build_upstream_map, build_upstream_tls_connector};
use anyhow::{anyhow, Context, Result};
use base64::Engine;
use rustls::pki_types::CertificateDer;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_rustls::rustls;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::{info, warn};

const PORT_OFFSET_BOOTSTRAP_RESERVED: u16 = 20;

pub async fn run(args: Args) -> Result<()> {
    if args.generate_certs {
        let sni_suffix = args
            .sni_suffix
            .as_deref()
            .ok_or_else(|| anyhow!("--sni-suffix is required with --generate-certs"))?;
        generate_default_certs(sni_suffix)?;
        info!(
            "Generated cert files: ca.pem, server.pem, server_key.pem, client1.pem, client1_key.pem, client2.pem, client2_key.pem"
        );
        return Ok(());
    }

    if args.save_ca_certs {
        let server = args
            .server
            .as_deref()
            .ok_or_else(|| anyhow!("--server is required with --save-ca-certs"))?;
        save_server_ca_cert(server).await?;
        return Ok(());
    }

    let file_config = FileConfig::from_file(&args.config)?;

    let bind_address = file_config
        .bind_address
        .clone()
        .unwrap_or_else(|| "0.0.0.0".to_string());
    let mapping_mode = file_config
        .broker_mapping
        .as_ref()
        .and_then(|b| b.mode.as_deref())
        .unwrap_or("sni");

    if mapping_mode == "sni" {
        if file_config.upstream.is_none() {
            return Err(anyhow!(
                "Missing required config section 'upstream' for broker_mapping.mode=sni"
            ));
        }
        if file_config.downstream.is_none() {
            return Err(anyhow!(
                "Missing required config section 'downstream' for broker_mapping.mode=sni"
            ));
        }
    }

    let downstream_security_protocol = file_config
        .downstream
        .as_ref()
        .and_then(|d| d.security_protocol.as_deref())
        .unwrap_or("SASL_SSL");
    let upstream_security_protocol = file_config
        .upstream
        .as_ref()
        .and_then(|u| u.security_protocol.as_deref())
        .unwrap_or("SASL_SSL");

    let downstream_tls_enabled = protocol_uses_tls(Some(downstream_security_protocol));
    let upstream_tls_enabled = protocol_uses_tls(Some(upstream_security_protocol));

    let downstream_tls = file_config.downstream.as_ref().and_then(|d| d.tls.as_ref());
    if !downstream_tls_enabled && downstream_tls.is_some() {
        warn!(
            "downstream.security_protocol={} disables TLS; downstream.tls section will be ignored",
            downstream_security_protocol
        );
    }
    if !upstream_tls_enabled
        && file_config
            .upstream
            .as_ref()
            .and_then(|u| u.tls.as_ref())
            .is_some()
    {
        warn!(
            "upstream.security_protocol={} disables TLS; upstream.tls section will be ignored",
            upstream_security_protocol
        );
    }

    if mapping_mode == "sni" && !downstream_tls_enabled {
        return Err(anyhow!(
            "downstream.security_protocol={} is not supported by this proxy: SNI routing is only supported when downstream TLS is enabled",
            downstream_security_protocol
        ));
    }
    let cert_path = downstream_tls.and_then(|t| t.cert.as_deref()).unwrap_or("server.pem");
    let key_path = downstream_tls
        .and_then(|t| t.key.as_deref())
        .unwrap_or("server_key.pem");
    let mtls_enabled = downstream_tls.and_then(|t| t.mtls_enabled).unwrap_or(false);
    let downstream_ca_path = downstream_tls.and_then(|t| t.ca_certs.as_deref());

    let upstreams = file_config
        .upstream
        .as_ref()
        .map(|u| u.bootstrap_servers.clone())
        .unwrap_or_default();
    if upstreams.is_empty() {
        return Err(anyhow!(
            "Missing required config field: upstream.bootstrap_servers"
        ));
    }
    let upstream_sasl_enabled = file_config
        .upstream
        .as_ref()
        .map(|u| protocol_uses_sasl(u.security_protocol.as_deref()) || u.sasl_mechanism.is_some())
        .unwrap_or(false);
    let downstream_sasl_enabled = file_config
        .downstream
        .as_ref()
        .map(|d| protocol_uses_sasl(d.security_protocol.as_deref()) || d.sasl_mechanism.is_some())
        .unwrap_or(false);
    let auth_passthrough = !upstream_sasl_enabled && !downstream_sasl_enabled;
    let downstream_sasl_mechanism = file_config
        .downstream
        .as_ref()
        .and_then(|d| d.sasl_mechanism.as_ref())
        .map(|s| s.to_ascii_uppercase());
    let upstream_sasl_mechanism = file_config
        .upstream
        .as_ref()
        .and_then(|u| u.sasl_mechanism.as_ref())
        .map(|s| s.to_ascii_uppercase());

    info!("Starting Kafka SNI Proxy");
    info!("Bind address: {}", bind_address);
    info!("Broker mapping mode: {}", mapping_mode);
    info!(
        "downstream TLS: {}",
        if downstream_tls_enabled {
            "enabled"
        } else {
            "disabled"
        }
    );
    info!(
        "mTLS: {}",
        if downstream_tls_enabled && mtls_enabled {
            "enabled"
        } else {
            "disabled"
        }
    );
    info!("Upstream brokers: {:?}", upstreams);

    let acceptor = if downstream_tls_enabled {
        let certs = server_cert_chain_for_handshake(cert_path, downstream_ca_path)?;
        let key = load_private_key(key_path)?;

        let mut server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .context("Failed to build server config")?;

        if mtls_enabled {
            info!("Configuring mTLS client certificate verification");

            let ca_certs_path = downstream_ca_path.unwrap_or("ca.pem");
            let ca_certs = load_certs(ca_certs_path)?;
            let mut root_store = rustls::RootCertStore::empty();

            for cert in ca_certs {
                root_store
                    .add(cert)
                    .context("Failed to add CA certificate to root store")?;
            }

            let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
                .build()
                .context("Failed to build client certificate verifier")?;

            let certs = server_cert_chain_for_handshake(cert_path, downstream_ca_path)?;
            let key = load_private_key(key_path)?;

            server_config = rustls::ServerConfig::builder()
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
                .context("Failed to build server config with mTLS")?;
        }

        Some(TlsAcceptor::from(Arc::new(server_config)))
    } else {
        if mtls_enabled {
            warn!("mTLS is ignored because downstream TLS is disabled");
        }
        None
    };
    let upstream_tls_connector = build_upstream_tls_connector(&file_config, upstream_tls_enabled)?;
    let hosts_resolver = if let Some(resolve_entries) = file_config.resolve.as_ref() {
        Some(Arc::new(HostsResolver::from_entries(resolve_entries)?))
    } else {
        None
    };

    let auth_swap_enabled = file_config.auth_swap.as_ref().map(|a| a.enabled).unwrap_or(false);
    if auth_swap_enabled && !upstream_sasl_enabled {
        warn!(
            "auth_swap is enabled but upstream has no SASL security configured; upstream auth initiation will be skipped"
        );
    }

    let auth_swap = if auth_passthrough {
        if auth_swap_enabled {
            warn!(
                "Both upstream and downstream are configured without SASL; auth_swap is ignored and passthrough mode is used"
            );
        } else {
            info!("Auth passthrough mode: upstream/downstream have no SASL configured");
        }
        None
    } else if let Some(cfg) = file_config.auth_swap.clone() {
        AuthSwap::from_config(cfg)?
    } else {
        None
    }
    .map(Arc::new);
    let advertise_mode = match mapping_mode {
        "sni" => {
            let sni_cfg = file_config
                .broker_mapping
                .as_ref()
                .and_then(|b| b.sni.as_ref())
                .ok_or_else(|| anyhow!("Missing broker_mapping.sni config for mode=sni"))?;
            let sni_bind_port = sni_cfg
                .bind_port
                .ok_or_else(|| anyhow!("Missing required config field: broker_mapping.sni.bind_port"))?;
            let (up, down) = (
                sni_cfg.upstream.as_deref().ok_or_else(|| {
                    anyhow!("Missing required config field: broker_mapping.sni.upstream")
                })?,
                sni_cfg.downstream.as_deref().ok_or_else(|| {
                    anyhow!("Missing required config field: broker_mapping.sni.downstream")
                })?,
            );
            let mapping_rule = BrokerMappingRule::parse(&format!("{}::{}", up, down))?;
            BrokerAdvertiseMode::Sni {
                bind_port: sni_bind_port,
                mapping_rule: Some(mapping_rule),
                state: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            }
        }
        "port_offset" => {
            let po_cfg = file_config
                .broker_mapping
                .as_ref()
                .and_then(|b| b.port_offset.as_ref())
                .ok_or_else(|| anyhow!("Missing broker_mapping.port_offset config for mode=port_offset"))?;
            let hostname = po_cfg
                .hostname
                .clone()
                .ok_or_else(|| anyhow!("Missing broker_mapping.port_offset.hostname"))?;
            let base_port = po_cfg
                .base
                .ok_or_else(|| anyhow!("Missing broker_mapping.port_offset.base"))?;
            let max_broker_id = po_cfg.max_broker_id.unwrap_or(400);
            let bootstrap_count =
                std::cmp::min(PORT_OFFSET_BOOTSTRAP_RESERVED as usize, upstreams.len()) as u16;
            BrokerAdvertiseMode::PortOffset {
                hostname,
                base_port,
                bootstrap_reserved: PORT_OFFSET_BOOTSTRAP_RESERVED,
                bootstrap_count,
                max_broker_id,
                state: Arc::new(std::sync::Mutex::new(PortOffsetState::default())),
            }
        }
        other => {
            return Err(anyhow!(
                "Unsupported broker_mapping.mode '{}': supported values are 'sni' and 'port_offset'",
                other
            ));
        }
    };
    let advertise_mode = Arc::new(advertise_mode);

    let default_upstream = Arc::new(
        upstreams
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("At least one upstream.bootstrap_servers entry is required"))?,
    );

    let mut initial_routes = build_upstream_map(&upstreams)?;
    initial_routes.insert("bootstrap".to_string(), default_upstream.as_ref().clone());
    match advertise_mode.as_ref() {
        BrokerAdvertiseMode::Sni { .. } => {}
        BrokerAdvertiseMode::PortOffset {
            hostname,
            base_port,
            bootstrap_reserved,
            bootstrap_count,
            max_broker_id: _,
            state: _,
        } => {
            let count = (*bootstrap_count as usize).min(*bootstrap_reserved as usize);
            for (idx, upstream) in upstreams.iter().take(count).enumerate() {
                initial_routes.insert(
                    format!("{}:{}", hostname, *base_port + idx as u16),
                    upstream.clone(),
                );
            }
        }
    }
    let upstream_routes = Arc::new(Mutex::new(initial_routes));
    let downstream_sasl_mechanism = downstream_sasl_mechanism.map(Arc::new);
    let upstream_sasl_mechanism = upstream_sasl_mechanism.map(Arc::new);
    let stats = ProxyStats::shared();

    {
        let stats = stats.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                ticker.tick().await;
                info!(
                    "stats frames_c2u={} frames_u2c={} bytes_c2u={} bytes_u2c={} rewritten={} passthrough={} max_c2u_buf={} max_u2c_buf={} max_rewrite_buf={}",
                    stats
                        .frames_client_to_upstream
                        .load(std::sync::atomic::Ordering::Relaxed),
                    stats
                        .frames_upstream_to_client
                        .load(std::sync::atomic::Ordering::Relaxed),
                    stats
                        .bytes_client_to_upstream
                        .load(std::sync::atomic::Ordering::Relaxed),
                    stats
                        .bytes_upstream_to_client
                        .load(std::sync::atomic::Ordering::Relaxed),
                    stats
                        .rewritten_responses
                        .load(std::sync::atomic::Ordering::Relaxed),
                    stats
                        .passthrough_responses
                        .load(std::sync::atomic::Ordering::Relaxed),
                    stats.max_c2u_buffer.load(std::sync::atomic::Ordering::Relaxed),
                    stats.max_u2c_buffer.load(std::sync::atomic::Ordering::Relaxed),
                    stats
                        .max_rewrite_buffer
                        .load(std::sync::atomic::Ordering::Relaxed),
                );
            }
        });
    }

    info!("Default upstream: {}", default_upstream);
    info!("Bootstrap route enabled to {}", default_upstream);

    let bind = match advertise_mode.as_ref() {
        BrokerAdvertiseMode::Sni { bind_port, .. } => format!("{}:{}", bind_address, bind_port),
        BrokerAdvertiseMode::PortOffset { .. } => format!("{}:0", bind_address),
    };

    serve(
        &bind,
        acceptor,
        upstream_tls_connector,
        hosts_resolver,
        auth_swap,
        downstream_sasl_mechanism,
        upstream_sasl_mechanism,
        advertise_mode,
        upstream_routes,
        default_upstream,
        stats,
    )
    .await
}

fn protocol_uses_sasl(protocol: Option<&str>) -> bool {
    protocol
        .map(|p| p.to_ascii_uppercase().contains("SASL"))
        .unwrap_or(false)
}

fn protocol_uses_tls(protocol: Option<&str>) -> bool {
    match protocol.map(|p| p.to_ascii_uppercase()) {
        Some(p) if p == "SSL" || p == "SASL_SSL" => true,
        Some(p) if p == "PLAINTEXT" || p == "SASL_PLAINTEXT" => false,
        Some(_) => true,
        None => true,
    }
}

async fn save_server_ca_cert(server: &str) -> Result<()> {
    let (host, _port) = server
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("Invalid --server '{}': expected host:port", server))?;

    let mut roots = rustls::RootCertStore::empty();
    let native_certs = rustls_native_certs::load_native_certs();
    for cert in native_certs.certs {
        roots
            .add(cert)
            .context("Failed to add system CA certificate")?;
    }
    if roots.is_empty() {
        return Err(anyhow!(
            "No system trust roots available; cannot verify server cert"
        ));
    }

    let client_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(client_config));

    let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
        .context(format!("Invalid server host for SNI: '{}'", host))?;

    let tcp = TcpStream::connect(server)
        .await
        .context(format!("Failed to connect to {}", server))?;
    let tls = connector
        .connect(server_name, tcp)
        .await
        .context(format!("TLS handshake failed for {}", server))?;

    let (_io, conn) = tls.get_ref();
    let chain = conn
        .peer_certificates()
        .ok_or_else(|| anyhow!("Server did not provide peer certificates"))?;

    if chain.is_empty() {
        return Err(anyhow!("Server returned empty certificate chain"));
    }

    let issuer = if chain.len() >= 2 { &chain[1] } else { &chain[0] };
    if chain.len() < 2 {
        info!(
            "Server {} returned only leaf cert; saving leaf as server_ca.pem",
            server
        );
    } else {
        info!("Server {} chain has {} certs; saving issuer cert", server, chain.len());
    }

    let pem = der_to_pem("CERTIFICATE", issuer.as_ref());
    fs::write("server_ca.pem", pem).context("Failed to write server_ca.pem")?;
    info!("Saved issuer certificate to server_ca.pem");
    Ok(())
}

fn der_to_pem(label: &str, der: &[u8]) -> String {
    let b64 = base64::engine::general_purpose::STANDARD.encode(der);
    let mut out = String::new();
    out.push_str("-----BEGIN ");
    out.push_str(label);
    out.push_str("-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(chunk).unwrap_or_default());
        out.push('\n');
    }
    out.push_str("-----END ");
    out.push_str(label);
    out.push_str("-----\n");
    out
}

fn server_cert_chain_for_handshake(
    cert_path: &str,
    ca_cert_hint: Option<&str>,
) -> Result<Vec<CertificateDer<'static>>> {
    let mut certs = load_certs(cert_path)?;
    if certs.is_empty() {
        return Ok(certs);
    }

    let mut appended = 0usize;
    if certs.len() == 1 {
        if let Some(hint) = ca_cert_hint {
            let mut ca_candidates: Vec<PathBuf> = Vec::new();
            ca_candidates.push(PathBuf::from(hint));
            if let Some(parent) = Path::new(cert_path).parent() {
                let sibling = parent.join(hint);
                if sibling != PathBuf::from(hint) {
                    ca_candidates.push(sibling);
                }
            }

            for candidate in ca_candidates {
                if let Ok(extra) = load_certs(candidate.to_string_lossy().as_ref()) {
                    for cert in extra {
                        if !certs.iter().any(|existing| existing.as_ref() == cert.as_ref()) {
                            certs.push(cert);
                            appended += 1;
                        }
                    }
                    if appended > 0 {
                        break;
                    }
                }
            }
        }
    }

    info!(
        "TLS certificate chain loaded from {} with {} cert(s) ({} appended)",
        cert_path,
        certs.len(),
        appended
    );

    Ok(certs)
}
