use crate::env_expand::expand_env_placeholders;
use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;

#[derive(Debug, Clone, Deserialize)]
pub struct FileConfig {
    pub bind_address: Option<String>,
    pub upstream: Option<UpstreamConfig>,
    pub downstream: Option<DownstreamConfig>,
    pub broker_mapping: Option<BrokerMappingConfig>,
    pub auth_swap: Option<AuthSwapConfig>,
    pub resolve: Option<Vec<ResolveEntry>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamConfig {
    pub bootstrap_servers: Vec<String>,
    pub security_protocol: Option<String>,
    pub sasl_mechanism: Option<String>,
    pub tls: Option<UpstreamTlsConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamTlsConfig {
    pub ca_certs: Option<String>,
    pub trust_server_certs: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DownstreamConfig {
    pub security_protocol: Option<String>,
    pub sasl_mechanism: Option<String>,
    pub tls: Option<DownstreamTlsConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DownstreamTlsConfig {
    pub mtls_enabled: Option<bool>,
    pub ca_certs: Option<String>,
    pub cert: Option<String>,
    pub key: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BrokerMappingConfig {
    pub mode: Option<String>,
    pub sni: Option<SniMappingConfig>,
    pub port_offset: Option<PortOffsetMappingConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SniMappingConfig {
    pub bind_port: Option<u16>,
    pub upstream: Option<String>,
    pub downstream: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PortOffsetMappingConfig {
    pub hostname: Option<String>,
    pub base: Option<u16>,
    #[serde(alias = "max_brokers")]
    pub max_broker_id: Option<u16>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResolveEntry {
    pub host: String,
    pub ipv4: Option<String>,
    pub ipv6: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthSwapConfig {
    pub enabled: bool,
    pub downstream_credentials: Option<Vec<String>>,
    pub upstream_credentials: Vec<String>,
    pub mapping: Option<Vec<String>>,
}

impl FileConfig {
    pub fn from_file(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file '{}'", path))?;
        let expanded = expand_env_placeholders(&content)?;
        let cfg: FileConfig = serde_yaml::from_str(&expanded)
            .with_context(|| format!("Failed to parse yaml config '{}'", path))?;
        Ok(cfg)
    }
}

pub fn parse_key_value_pairs(values: &[String], label: &str) -> Result<HashMap<String, String>> {
    let mut out = HashMap::new();
    for value in values {
        let (k, v) = value
            .split_once('=')
            .ok_or_else(|| anyhow!("Invalid {} entry '{}': expected key=value", label, value))?;
        if k.is_empty() {
            return Err(anyhow!(
                "Invalid {} entry '{}': key cannot be empty",
                label,
                value
            ));
        }
        out.insert(k.to_string(), v.to_string());
    }
    Ok(out)
}
