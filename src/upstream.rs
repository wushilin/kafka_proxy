use crate::certs::load_certs;
use crate::config::FileConfig;
use anyhow::{anyhow, Context, Result};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::DigitallySignedStruct;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_rustls::rustls;
use tokio_rustls::TlsConnector;
use tracing::{info, warn};

#[derive(Debug)]
struct TrustAllUpstreamCertVerifier;

impl ServerCertVerifier for TrustAllUpstreamCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

pub fn build_upstream_map(upstreams: &[String]) -> Result<HashMap<String, String>> {
    if upstreams.is_empty() {
        return Err(anyhow!(
            "At least one upstream bootstrap server is required (example: b1.upstream.com:9092)"
        ));
    }

    let mut map = HashMap::new();

    for upstream in upstreams {
        if !upstream.contains(':') {
            return Err(anyhow!(
                "Invalid upstream '{}': expected host:port format",
                upstream
            ));
        }

        let parts: Vec<&str> = upstream.split('.').collect();
        let broker_name = parts
            .first()
            .ok_or_else(|| anyhow!("Invalid upstream format: {}", upstream))?
            .trim();

        if broker_name.is_empty() {
            return Err(anyhow!("Invalid upstream format: {}", upstream));
        }

        if map
            .insert(broker_name.to_string(), upstream.clone())
            .is_some()
        {
            return Err(anyhow!(
                "Duplicate upstream broker id '{}'; each broker id must be unique",
                broker_name
            ));
        }
    }

    Ok(map)
}

pub fn upstream_host(upstream_addr: &str) -> Result<&str> {
    let (host, _port) = upstream_addr
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("Invalid upstream '{}': expected host:port", upstream_addr))?;
    Ok(host)
}

pub fn build_upstream_tls_connector(
    file_config: &FileConfig,
    tls_enabled: bool,
) -> Result<Option<TlsConnector>> {
    if !tls_enabled {
        return Ok(None);
    }

    let cfg_tls = file_config
        .upstream
        .as_ref()
        .and_then(|up| up.tls.as_ref());

    let trust_upstream_certs = cfg_tls
        .and_then(|tls| tls.trust_server_certs)
        .unwrap_or(false);
    let upstream_ca_certs = cfg_tls.and_then(|tls| tls.ca_certs.as_deref());

    let client_config = if trust_upstream_certs {
        info!("Upstream TLS verification disabled by --trust-upstream-certs");
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(TrustAllUpstreamCertVerifier))
            .with_no_client_auth()
    } else {
        let mut root_store = rustls::RootCertStore::empty();

        if let Some(path) = upstream_ca_certs {
            info!("Using upstream CA bundle from {}", path);
            for cert in load_certs(path)? {
                root_store
                    .add(cert)
                    .context("Failed to add upstream CA certificate")?;
            }
        } else {
            let native_certs = rustls_native_certs::load_native_certs();
            for cert in native_certs.certs {
                root_store
                    .add(cert)
                    .context("Failed to add system CA certificate")?;
            }
            for err in native_certs.errors {
                warn!("Ignoring system certificate load error: {}", err);
            }
            if root_store.is_empty() {
                return Err(anyhow!(
                    "No system trust roots available; provide --upstream-ca-certs"
                ));
            }
        }

        rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth()
    };

    Ok(Some(TlsConnector::from(Arc::new(client_config))))
}
