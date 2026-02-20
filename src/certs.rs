use anyhow::{anyhow, Context, Result};
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, IsCa, KeyPair, SanType,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::fs::{self, File};
use std::io::BufReader;
use time::{Duration, OffsetDateTime};

pub fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path).context(format!("Failed to open certificate file: {}", path))?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .context("Failed to parse certificates")?;
    Ok(certs)
}

pub fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    let file = File::open(path).context(format!("Failed to open private key file: {}", path))?;
    let mut reader = BufReader::new(file);

    let key = rustls_pemfile::private_key(&mut reader)
        .context("Failed to parse private key")?
        .ok_or_else(|| anyhow!("No private key found in file"))?;

    Ok(key)
}

pub fn generate_default_certs(sni_suffix: &str) -> Result<()> {
    for path in ["ca.pem", "server.pem", "key.pem"] {
        if std::path::Path::new(path).exists() {
            return Err(anyhow!(
                "Refusing to overwrite existing file: {}. Remove it first or move it aside.",
                path
            ));
        }
    }

    let not_before = OffsetDateTime::now_utc()
        .checked_sub(Duration::days(1))
        .ok_or_else(|| anyhow!("Failed to compute certificate not_before"))?;
    let not_after = not_before
        .checked_add(Duration::days(365 * 20))
        .ok_or_else(|| anyhow!("Failed to compute certificate validity period"))?;

    let ca_key = KeyPair::generate().context("Failed to generate CA key")?;
    let mut ca_params = CertificateParams::new(Vec::new())
        .context("Failed to build CA certificate params")?;
    ca_params.not_before = not_before;
    ca_params.not_after = not_after;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.distinguished_name = DistinguishedName::new();
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "Kafka Proxy CA");
    let ca_cert = ca_params
        .self_signed(&ca_key)
        .context("Failed to create CA certificate")?;

    let wildcard = format!("*.{}", sni_suffix);
    let mut server_params = CertificateParams::new(vec![wildcard, sni_suffix.to_string()])
        .context("Failed to build server certificate params")?;
    server_params.not_before = not_before;
    server_params.not_after = not_after;
    server_params.is_ca = IsCa::NoCa;
    server_params.distinguished_name = DistinguishedName::new();
    server_params
        .distinguished_name
        .push(DnType::CommonName, format!("*.{}", sni_suffix));
    server_params
        .subject_alt_names
        .push(SanType::DnsName(format!("*.{}", sni_suffix).try_into()?));
    server_params
        .subject_alt_names
        .push(SanType::DnsName(sni_suffix.to_string().try_into()?));

    let server_key = KeyPair::generate().context("Failed to generate server key")?;
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .context("Failed to sign server certificate")?;

    let ca_pem = ca_cert.pem();
    let server_pem = server_cert.pem();

    fs::write("ca.pem", ca_pem).context("Failed to write ca.pem")?;
    fs::write("server.pem", server_pem).context("Failed to write server.pem")?;
    fs::write("key.pem", server_key.serialize_pem()).context("Failed to write key.pem")?;

    Ok(())
}
