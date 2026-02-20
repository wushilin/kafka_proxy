pub mod cli;
mod certs;
mod hosts;
mod kafka_io;
mod proxy;
mod rewrite;
mod stats;
mod upstream;

use crate::certs::{generate_default_certs, load_certs, load_private_key};
use crate::cli::Args;
use crate::hosts::HostsResolver;
use crate::proxy::serve;
use crate::rewrite::BrokerMappingRule;
use crate::stats::ProxyStats;
use crate::upstream::{build_upstream_map, build_upstream_tls_connector};
use anyhow::{anyhow, Context, Result};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_rustls::rustls;
use tokio_rustls::TlsAcceptor;
use tracing::info;

fn bind_port(bind: &str) -> Result<u16> {
    if let Ok(addr) = bind.parse::<std::net::SocketAddr>() {
        return Ok(addr.port());
    }

    let maybe_port = bind
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("Failed to parse bind port from '{}': expected host:port", bind))?
        .1;

    maybe_port
        .parse::<u16>()
        .context(format!("Failed to parse bind port from '{}': invalid port", bind))
}

pub async fn run(args: Args) -> Result<()> {
    if args.generate_certs {
        generate_default_certs(&args.sni_suffix)?;
        info!("Generated cert files: ca.pem, server.pem, key.pem");
        return Ok(());
    }

    let bind_port = bind_port(&args.bind)?;
    let mtls_enabled = args.mtls_enable && !args.mtls_disable;

    info!("Starting Kafka SNI Proxy");
    info!("Bind address: {}", args.bind);
    info!("SNI suffix: {}", args.sni_suffix);
    info!("mTLS: {}", if mtls_enabled { "enabled" } else { "disabled" });
    info!("Upstream brokers: {:?}", args.upstream);

    let certs = load_certs(&args.cert)?;
    let key = load_private_key(&args.key)?;

    let mut server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("Failed to build server config")?;

    if mtls_enabled {
        info!("Configuring mTLS client certificate verification");

        let ca_certs_path = args.ca_certs.as_deref().unwrap_or("ca.pem");
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

        let certs = load_certs(&args.cert)?;
        let key = load_private_key(&args.key)?;

        server_config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .context("Failed to build server config with mTLS")?;
    }

    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    let upstream_tls_connector = build_upstream_tls_connector(&args)?;
    let hosts_resolver = args
        .hosts_file
        .as_deref()
        .map(HostsResolver::from_file)
        .transpose()?
        .map(Arc::new);
    let broker_mapping_rule = args
        .broker_mapping_rule
        .as_deref()
        .map(BrokerMappingRule::parse)
        .transpose()?
        .map(Arc::new);

    let default_upstream = Arc::new(
        args.upstream
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("At least one --upstream is required"))?,
    );

    let mut initial_routes = build_upstream_map(&args.upstream)?;
    initial_routes.insert("bootstrap".to_string(), default_upstream.as_ref().clone());
    initial_routes.insert(
        format!("bootstrap.{}", args.sni_suffix),
        default_upstream.as_ref().clone(),
    );
    let upstream_routes = Arc::new(Mutex::new(initial_routes));
    let sni_suffix = Arc::new(args.sni_suffix.clone());
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
    info!(
        "Bootstrap route enabled: bootstrap.{} -> {}",
        args.sni_suffix, default_upstream
    );

    serve(
        &args.bind,
        acceptor,
        upstream_tls_connector,
        hosts_resolver,
        upstream_routes,
        default_upstream,
        broker_mapping_rule,
        sni_suffix,
        bind_port,
        stats,
    )
    .await
}
