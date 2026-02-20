use anyhow::{Context, Result};
use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, DescribeClusterResponse, FindCoordinatorResponse, MetadataResponse, ResponseHeader,
    ResponseKind,
};
use kafka_protocol::protocol::{Decodable, Encodable, Message};
use regex::Regex;
use tracing::{info, warn};

fn rewrite_advertised_host(node_id: i32, sni_suffix: &str) -> String {
    format!("b{}.{}", node_id, sni_suffix)
}

#[derive(Debug)]
pub struct BrokerMappingRule {
    regex: Regex,
    replacement: String,
}

impl BrokerMappingRule {
    pub fn parse(spec: &str) -> Result<Self> {
        let (pattern, replacement) = spec
            .split_once("::")
            .ok_or_else(|| anyhow::anyhow!("Invalid broker mapping rule '{}': expected '<regex>::<replacement>'", spec))?;
        let regex = Regex::new(pattern)
            .with_context(|| format!("Invalid broker mapping regex pattern '{}'", pattern))?;
        Ok(Self {
            regex,
            replacement: replacement.to_string(),
        })
    }

    fn map_host(&self, upstream_host: &str) -> Option<String> {
        if self.regex.is_match(upstream_host) {
            Some(
                self.regex
                    .replace(upstream_host, self.replacement.as_str())
                    .into_owned(),
            )
        } else {
            None
        }
    }
}

fn supports_rewrite_version(api_key: ApiKey, api_version: i16) -> bool {
    let in_range = |min: i16, max: i16| api_version >= min && api_version <= max;
    match api_key {
        ApiKey::Metadata => in_range(
            MetadataResponse::VERSIONS.min,
            MetadataResponse::VERSIONS.max,
        ),
        ApiKey::FindCoordinator => in_range(
            FindCoordinatorResponse::VERSIONS.min,
            FindCoordinatorResponse::VERSIONS.max,
        ),
        ApiKey::DescribeCluster => in_range(
            DescribeClusterResponse::VERSIONS.min,
            DescribeClusterResponse::VERSIONS.max,
        ),
        _ => false,
    }
}

pub struct RewriteOutcome {
    pub payload: Option<Bytes>,
    pub discovered_routes: Vec<(String, String)>,
}

pub fn maybe_rewrite_kafka_response(
    payload: &[u8],
    api_key: ApiKey,
    api_version: i16,
    sni_suffix: &str,
    bind_port: u16,
    mapping_rule: Option<&BrokerMappingRule>,
    log_ctx: &str,
) -> Result<RewriteOutcome> {
    if !matches!(
        api_key,
        ApiKey::Metadata | ApiKey::FindCoordinator | ApiKey::DescribeCluster
    ) {
        return Ok(RewriteOutcome {
            payload: None,
            discovered_routes: Vec::new(),
        });
    }

    if !supports_rewrite_version(api_key, api_version) {
        info!(
            "{} skipping rewrite for unsupported API version: api={:?} version={}",
            log_ctx, api_key, api_version
        );
        return Ok(RewriteOutcome {
            payload: None,
            discovered_routes: Vec::new(),
        });
    }

    let response_header_version = api_key.response_header_version(api_version);
    let mut decode_buf = Bytes::copy_from_slice(payload);

    let response_header = ResponseHeader::decode(&mut decode_buf, response_header_version)
        .context("Failed to decode Kafka response header")?;

    let mut response = ResponseKind::decode(api_key, &mut decode_buf, api_version)
        .context("Failed to decode Kafka response")?;

    if decode_buf.has_remaining() {
        warn!(
            "{} kafka response decode left {} unread bytes (api={:?}, version={})",
            log_ctx, decode_buf.remaining(), api_key, api_version
        );
    }

    let mut rewritten_any = false;
    let mut discovered_routes = Vec::new();
    let target_host = |node_id: i32, upstream_host: &str| {
        mapping_rule
            .and_then(|rule| rule.map_host(upstream_host))
            .unwrap_or_else(|| rewrite_advertised_host(node_id, sni_suffix))
    };

    match &mut response {
        ResponseKind::Metadata(metadata) => {
            for broker in &mut metadata.brokers {
                let original_host = broker.host.as_str().to_string();
                let node_id = broker.node_id.0;
                let original_port = broker.port;
                let rewritten_host = target_host(node_id, &original_host);
                let upstream_addr = format!("{}:{}", original_host, original_port);
                discovered_routes.push((format!("b{}", node_id), upstream_addr.clone()));
                discovered_routes.push((rewritten_host.clone(), upstream_addr));
                broker.host = rewritten_host.clone().into();
                broker.port = bind_port as i32;
                rewritten_any = true;

                info!(
                    "{} rewrote Metadata broker node_id={} host='{}' -> '{}' port={}",
                    log_ctx, node_id, original_host, rewritten_host, bind_port
                );
            }
        }
        ResponseKind::FindCoordinator(find_coordinator) => {
            if api_version <= 3 {
                let original_host = find_coordinator.host.as_str().to_string();
                let node_id = find_coordinator.node_id.0;
                let original_port = find_coordinator.port;
                let rewritten_host = target_host(node_id, &original_host);
                let upstream_addr = format!("{}:{}", original_host, original_port);
                discovered_routes.push((format!("b{}", node_id), upstream_addr.clone()));
                discovered_routes.push((rewritten_host.clone(), upstream_addr));
                find_coordinator.host = rewritten_host.clone().into();
                find_coordinator.port = bind_port as i32;
                rewritten_any = true;

                info!(
                    "{} rewrote FindCoordinator node_id={} host='{}' -> '{}' port={}",
                    log_ctx, node_id, original_host, rewritten_host, bind_port
                );
            } else {
                for coordinator in &mut find_coordinator.coordinators {
                    let original_host = coordinator.host.as_str().to_string();
                    let node_id = coordinator.node_id.0;
                    let original_port = coordinator.port;
                    let rewritten_host = target_host(node_id, &original_host);
                    let upstream_addr = format!("{}:{}", original_host, original_port);
                    discovered_routes.push((format!("b{}", node_id), upstream_addr.clone()));
                    discovered_routes.push((rewritten_host.clone(), upstream_addr));
                    coordinator.host = rewritten_host.clone().into();
                    coordinator.port = bind_port as i32;
                    rewritten_any = true;

                    info!(
                        "{} rewrote FindCoordinator key='{}' node_id={} host='{}' -> '{}' port={}",
                        log_ctx,
                        coordinator.key.as_str(),
                        node_id,
                        original_host,
                        rewritten_host,
                        bind_port
                    );
                }
            }
        }
        ResponseKind::DescribeCluster(describe_cluster) => {
            for broker in &mut describe_cluster.brokers {
                let original_host = broker.host.as_str().to_string();
                let broker_id = broker.broker_id.0;
                let original_port = broker.port;
                let rewritten_host = target_host(broker_id, &original_host);
                let upstream_addr = format!("{}:{}", original_host, original_port);
                discovered_routes.push((format!("b{}", broker_id), upstream_addr.clone()));
                discovered_routes.push((rewritten_host.clone(), upstream_addr));
                broker.host = rewritten_host.clone().into();
                broker.port = bind_port as i32;
                rewritten_any = true;

                info!(
                    "{} rewrote DescribeCluster broker_id={} host='{}' -> '{}' port={}",
                    log_ctx, broker_id, original_host, rewritten_host, bind_port
                );
            }
        }
        _ => {}
    }

    if !rewritten_any {
        return Ok(RewriteOutcome {
            payload: None,
            discovered_routes,
        });
    }

    let mut encoded = BytesMut::with_capacity(payload.len() + 256);
    response_header
        .encode(&mut encoded, response_header_version)
        .context("Failed to encode Kafka response header")?;
    response
        .encode(&mut encoded, api_version)
        .context("Failed to encode Kafka response")?;

    Ok(RewriteOutcome {
        payload: Some(encoded.freeze()),
        discovered_routes,
    })
}
