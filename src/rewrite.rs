use anyhow::{Context, Result};
use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, DescribeClusterResponse, FetchResponse, FindCoordinatorResponse, MetadataResponse,
    ProduceResponse, ResponseHeader, ResponseKind, ShareAcknowledgeResponse, ShareFetchResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, Message};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub enum BrokerAdvertiseMode {
    Sni {
        bind_port: u16,
        mapping_rule: Option<BrokerMappingRule>,
        state: Arc<Mutex<HashMap<i32, (String, i32)>>>,
    },
    PortOffset {
        hostname: String,
        base_port: u16,
        bootstrap_reserved: u16,
        bootstrap_count: u16,
        max_broker_id: u16,
        state: Arc<Mutex<PortOffsetState>>,
    },
}

#[derive(Debug, Default)]
pub struct PortOffsetState {
    id_to_endpoint: HashMap<i32, (String, i32)>,
}

impl PortOffsetState {
    fn update_ids(
        &mut self,
        ids: &[i32],
        hostname: &str,
        base_port: u16,
        bootstrap_reserved: u16,
        max_broker_id: u16,
        replace_existing: bool,
        log_ctx: &str,
    ) -> Result<()> {
        let mut pending = Vec::with_capacity(ids.len());
        for id in ids {
            if *id < 0 {
                return Err(anyhow::anyhow!(
                    "{} port_offset mapping error: negative broker id {} is not supported",
                    log_ctx,
                    id
                ));
            }
            let id_u16 = u16::try_from(*id).unwrap_or(u16::MAX);
            if id_u16 > max_broker_id {
                return Err(anyhow::anyhow!(
                    "{} port_offset mapping error: broker id {} exceeds configured max_broker_id {}",
                    log_ctx,
                    id,
                    max_broker_id
                ));
            }
            let port = (base_port as i32) + (bootstrap_reserved as i32) + (*id);
            if !(1..=65535).contains(&port) {
                return Err(anyhow::anyhow!(
                    "{} port_offset mapping error: computed port {} out of valid range for broker id {}",
                    log_ctx,
                    port,
                    id
                ));
            }
            pending.push((*id, port));
        }

        for (id, port) in pending {
            self.id_to_endpoint
                .insert(id, (hostname.to_string(), port));
        }
        if replace_existing {
            let keep: HashSet<i32> = ids.iter().copied().collect();
            self.id_to_endpoint.retain(|id, _| keep.contains(id));
        }
        Ok(())
    }

    fn endpoint_for(&self, broker_id: i32) -> Option<(String, i32)> {
        self.id_to_endpoint.get(&broker_id).cloned()
    }
}

#[derive(Debug, Clone)]
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

    fn map_host(&self, upstream_host: &str, broker_id: i32) -> Option<String> {
        let captures = self.regex.captures(upstream_host)?;
        Some(render_mapping_template(
            self.replacement.as_str(),
            &captures,
            broker_id,
        ))
    }
}

fn render_mapping_template(
    template: &str,
    captures: &regex::Captures<'_>,
    broker_id: i32,
) -> String {
    let mut out = String::with_capacity(template.len() + 16);
    let mut i = 0usize;
    while i < template.len() {
        let rem = &template[i..];
        if rem.starts_with("<$") {
            if let Some(end_rel) = rem.find('>') {
                let token = &rem[2..end_rel];
                if token == "id" {
                    out.push_str(&broker_id.to_string());
                } else if let Ok(group_idx) = token.parse::<usize>() {
                    if let Some(m) = captures.get(group_idx) {
                        out.push_str(m.as_str());
                    }
                } else {
                    out.push_str("<$");
                    out.push_str(token);
                    out.push('>');
                }
                i += end_rel + 1;
                continue;
            }
        }

        let ch = rem.chars().next().unwrap_or_default();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
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
        ApiKey::Fetch => in_range(FetchResponse::VERSIONS.min, FetchResponse::VERSIONS.max),
        ApiKey::Produce => in_range(ProduceResponse::VERSIONS.min, ProduceResponse::VERSIONS.max),
        ApiKey::ShareFetch => in_range(
            ShareFetchResponse::VERSIONS.min,
            ShareFetchResponse::VERSIONS.max,
        ),
        ApiKey::ShareAcknowledge => in_range(
            ShareAcknowledgeResponse::VERSIONS.min,
            ShareAcknowledgeResponse::VERSIONS.max,
        ),
        _ => false,
    }
}

pub fn should_attempt_rewrite(api_key: ApiKey, api_version: i16) -> bool {
    matches!(
        api_key,
        ApiKey::Metadata
            | ApiKey::FindCoordinator
            | ApiKey::DescribeCluster
            | ApiKey::Fetch
            | ApiKey::Produce
            | ApiKey::ShareFetch
            | ApiKey::ShareAcknowledge
    ) && supports_rewrite_version(api_key, api_version)
}

pub struct RewriteOutcome {
    pub payload: Option<Bytes>,
    pub discovered_routes: Vec<DiscoveredRoute>,
    pub topology_snapshot_ids: Option<Vec<i32>>,
}

pub struct DiscoveredRoute {
    pub key: String,
    pub upstream_addr: String,
    pub broker_id: i32,
}

fn remember_discovered_routes(
    discovered_routes: &mut Vec<DiscoveredRoute>,
    broker_id: i32,
    upstream_addr: String,
    route_key: Option<String>,
) {
    discovered_routes.push(DiscoveredRoute {
        key: format!("b{}", broker_id),
        upstream_addr: upstream_addr.clone(),
        broker_id,
    });
    if let Some(key) = route_key {
        discovered_routes.push(DiscoveredRoute {
            key,
            upstream_addr,
            broker_id,
        });
    }
}

fn advertised_target(
    mode: &BrokerAdvertiseMode,
    node_id: i32,
    upstream_host: &str,
    log_ctx: &str,
) -> Result<(String, i32, Option<String>)> {
    match mode {
        BrokerAdvertiseMode::Sni {
            bind_port,
            mapping_rule,
            state,
        } => {
            let computed_host = mapping_rule
                .as_ref()
                .and_then(|rule| rule.map_host(upstream_host, node_id))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "No SNI downstream host mapping available for broker {} from broker_mapping.sni rule",
                        node_id
                    )
                })?;
            let computed_port = *bind_port as i32;
            let mut guard = state
                .lock()
                .map_err(|_| anyhow::anyhow!("sni mapping state lock poisoned"))?;
            if let Some((host, port)) = guard.get(&node_id).cloned() {
                if host != computed_host || port != computed_port {
                    panic!(
                        "{} fatal broker mapping inconsistency: broker {} was mapped to {}:{}, now wants {}:{}",
                        log_ctx, node_id, host, port, computed_host, computed_port
                    );
                }
                Ok((host.clone(), port, Some(host)))
            } else {
                assert_unique_mapping(&guard, node_id, &computed_host, computed_port, log_ctx);
                guard.insert(node_id, (computed_host.clone(), computed_port));
                Ok((computed_host.clone(), computed_port, Some(computed_host)))
            }
        }
        BrokerAdvertiseMode::PortOffset {
            hostname,
            base_port,
            bootstrap_reserved,
            max_broker_id,
            state,
            ..
        } => {
            let guard = state
                .lock()
                .map_err(|_| anyhow::anyhow!("port_offset state lock poisoned"))?;
            let (host, port) = guard
                .endpoint_for(node_id)
                .ok_or_else(|| anyhow::anyhow!("No endpoint assigned for broker id {}", node_id))?;
            let id_u16 = u16::try_from(node_id).unwrap_or(u16::MAX);
            if id_u16 > *max_broker_id {
                return Err(anyhow::anyhow!(
                    "{} port_offset mapping error: broker id {} exceeds configured max_broker_id {}",
                    log_ctx,
                    node_id,
                    max_broker_id
                ));
            }
            if !(1..=65535).contains(&port) {
                return Err(anyhow::anyhow!(
                    "Invalid advertised port {} computed for node_id {} in port_offset mode",
                    port,
                    node_id
                ));
            }
            if host != *hostname || port < (*base_port as i32 + *bootstrap_reserved as i32) {
                panic!(
                    "{} fatal port_offset mapping corruption for broker {} -> {}:{}",
                    log_ctx, node_id, host, port
                );
            }
            Ok((host.clone(), port, Some(format!("{}:{}", host, port))))
        }
    }
}

pub fn maybe_rewrite_kafka_response(
    payload: &Bytes,
    api_key: ApiKey,
    api_version: i16,
    advertise_mode: &BrokerAdvertiseMode,
    log_ctx: &str,
) -> Result<RewriteOutcome> {
    if !matches!(
        api_key,
        ApiKey::Metadata
            | ApiKey::FindCoordinator
            | ApiKey::DescribeCluster
            | ApiKey::Fetch
            | ApiKey::Produce
            | ApiKey::ShareFetch
            | ApiKey::ShareAcknowledge
    ) {
        return Ok(RewriteOutcome {
            payload: None,
            discovered_routes: Vec::new(),
            topology_snapshot_ids: None,
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
            topology_snapshot_ids: None,
        });
    }

    let response_header_version = api_key.response_header_version(api_version);
    let mut decode_buf = payload.clone();

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
    let mut topology_snapshot_ids: Option<Vec<i32>> = None;
    let mut seen_downstream_endpoints: HashMap<(String, i32), i32> = HashMap::new();
    match &mut response {
        ResponseKind::Metadata(metadata) => {
            let ids: Vec<i32> = metadata.brokers.iter().map(|b| b.node_id.0).collect();
            update_port_offset_assignments(advertise_mode, &ids, true, log_ctx)?;
            topology_snapshot_ids = Some(ids.clone());
            for broker in &mut metadata.brokers {
                let original_host = broker.host.as_str().to_string();
                let node_id = broker.node_id.0;
                let original_port = broker.port;
                let (rewritten_host, rewritten_port, route_key) =
                    advertised_target(advertise_mode, node_id, &original_host, log_ctx)?;
                assert_unique_downstream_endpoint(
                    &mut seen_downstream_endpoints,
                    node_id,
                    &rewritten_host,
                    rewritten_port,
                    log_ctx,
                );
                let upstream_addr = format!("{}:{}", original_host, original_port);
                remember_discovered_routes(&mut discovered_routes, node_id, upstream_addr, route_key);
                broker.host = rewritten_host.clone().into();
                broker.port = rewritten_port;
                rewritten_any = true;

                info!(
                    "{} rewrote Metadata broker node_id={} host='{}' -> '{}' port={}",
                    log_ctx, node_id, original_host, rewritten_host, rewritten_port
                );
            }
        }
        ResponseKind::FindCoordinator(find_coordinator) => {
            if api_version <= 3 {
                update_port_offset_assignments(
                    advertise_mode,
                    &[find_coordinator.node_id.0],
                    false,
                    log_ctx,
                )?;
                let original_host = find_coordinator.host.as_str().to_string();
                let node_id = find_coordinator.node_id.0;
                let original_port = find_coordinator.port;
                let (rewritten_host, rewritten_port, route_key) =
                    advertised_target(advertise_mode, node_id, &original_host, log_ctx)?;
                assert_unique_downstream_endpoint(
                    &mut seen_downstream_endpoints,
                    node_id,
                    &rewritten_host,
                    rewritten_port,
                    log_ctx,
                );
                let upstream_addr = format!("{}:{}", original_host, original_port);
                remember_discovered_routes(&mut discovered_routes, node_id, upstream_addr, route_key);
                find_coordinator.host = rewritten_host.clone().into();
                find_coordinator.port = rewritten_port;
                rewritten_any = true;

                info!(
                    "{} rewrote FindCoordinator node_id={} host='{}' -> '{}' port={}",
                    log_ctx, node_id, original_host, rewritten_host, rewritten_port
                );
            } else {
                let ids: Vec<i32> = find_coordinator
                    .coordinators
                    .iter()
                    .map(|c| c.node_id.0)
                    .collect();
                update_port_offset_assignments(advertise_mode, &ids, false, log_ctx)?;
                for coordinator in &mut find_coordinator.coordinators {
                    let original_host = coordinator.host.as_str().to_string();
                    let node_id = coordinator.node_id.0;
                    let original_port = coordinator.port;
                    let (rewritten_host, rewritten_port, route_key) =
                        advertised_target(advertise_mode, node_id, &original_host, log_ctx)?;
                    assert_unique_downstream_endpoint(
                        &mut seen_downstream_endpoints,
                        node_id,
                        &rewritten_host,
                        rewritten_port,
                        log_ctx,
                    );
                    let upstream_addr = format!("{}:{}", original_host, original_port);
                    remember_discovered_routes(
                        &mut discovered_routes,
                        node_id,
                        upstream_addr,
                        route_key,
                    );
                    coordinator.host = rewritten_host.clone().into();
                    coordinator.port = rewritten_port;
                    rewritten_any = true;

                    info!(
                        "{} rewrote FindCoordinator key='{}' node_id={} host='{}' -> '{}' port={}",
                        log_ctx,
                        coordinator.key.as_str(),
                        node_id,
                        original_host,
                        rewritten_host,
                        rewritten_port
                    );
                }
            }
        }
        ResponseKind::DescribeCluster(describe_cluster) => {
            let ids: Vec<i32> = describe_cluster.brokers.iter().map(|b| b.broker_id.0).collect();
            update_port_offset_assignments(advertise_mode, &ids, true, log_ctx)?;
            topology_snapshot_ids = Some(ids.clone());
            for broker in &mut describe_cluster.brokers {
                let original_host = broker.host.as_str().to_string();
                let broker_id = broker.broker_id.0;
                let original_port = broker.port;
                let (rewritten_host, rewritten_port, route_key) =
                    advertised_target(advertise_mode, broker_id, &original_host, log_ctx)?;
                assert_unique_downstream_endpoint(
                    &mut seen_downstream_endpoints,
                    broker_id,
                    &rewritten_host,
                    rewritten_port,
                    log_ctx,
                );
                let upstream_addr = format!("{}:{}", original_host, original_port);
                remember_discovered_routes(
                    &mut discovered_routes,
                    broker_id,
                    upstream_addr,
                    route_key,
                );
                broker.host = rewritten_host.clone().into();
                broker.port = rewritten_port;
                rewritten_any = true;

                info!(
                    "{} rewrote DescribeCluster broker_id={} host='{}' -> '{}' port={}",
                    log_ctx, broker_id, original_host, rewritten_host, rewritten_port
                );
            }
        }
        ResponseKind::Fetch(fetch) => {
            let ids: Vec<i32> = fetch.node_endpoints.iter().map(|e| e.node_id.0).collect();
            update_port_offset_assignments(advertise_mode, &ids, false, log_ctx)?;
            for endpoint in &mut fetch.node_endpoints {
                let original_host = endpoint.host.as_str().to_string();
                let node_id = endpoint.node_id.0;
                let original_port = endpoint.port;
                let (rewritten_host, rewritten_port, route_key) =
                    advertised_target(advertise_mode, node_id, &original_host, log_ctx)?;
                assert_unique_downstream_endpoint(
                    &mut seen_downstream_endpoints,
                    node_id,
                    &rewritten_host,
                    rewritten_port,
                    log_ctx,
                );
                let upstream_addr = format!("{}:{}", original_host, original_port);
                remember_discovered_routes(&mut discovered_routes, node_id, upstream_addr, route_key);
                endpoint.host = rewritten_host.clone().into();
                endpoint.port = rewritten_port;
                rewritten_any = true;

                info!(
                    "{} rewrote Fetch node_endpoint node_id={} host='{}' -> '{}' port={}",
                    log_ctx, node_id, original_host, rewritten_host, rewritten_port
                );
            }
        }
        ResponseKind::Produce(produce) => {
            let ids: Vec<i32> = produce.node_endpoints.iter().map(|e| e.node_id.0).collect();
            update_port_offset_assignments(advertise_mode, &ids, false, log_ctx)?;
            for endpoint in &mut produce.node_endpoints {
                let original_host = endpoint.host.as_str().to_string();
                let node_id = endpoint.node_id.0;
                let original_port = endpoint.port;
                let (rewritten_host, rewritten_port, route_key) =
                    advertised_target(advertise_mode, node_id, &original_host, log_ctx)?;
                assert_unique_downstream_endpoint(
                    &mut seen_downstream_endpoints,
                    node_id,
                    &rewritten_host,
                    rewritten_port,
                    log_ctx,
                );
                let upstream_addr = format!("{}:{}", original_host, original_port);
                remember_discovered_routes(&mut discovered_routes, node_id, upstream_addr, route_key);
                endpoint.host = rewritten_host.clone().into();
                endpoint.port = rewritten_port;
                rewritten_any = true;

                info!(
                    "{} rewrote Produce node_endpoint node_id={} host='{}' -> '{}' port={}",
                    log_ctx, node_id, original_host, rewritten_host, rewritten_port
                );
            }
        }
        ResponseKind::ShareFetch(share_fetch) => {
            let ids: Vec<i32> = share_fetch.node_endpoints.iter().map(|e| e.node_id.0).collect();
            update_port_offset_assignments(advertise_mode, &ids, false, log_ctx)?;
            for endpoint in &mut share_fetch.node_endpoints {
                let original_host = endpoint.host.as_str().to_string();
                let node_id = endpoint.node_id.0;
                let original_port = endpoint.port;
                let (rewritten_host, rewritten_port, route_key) =
                    advertised_target(advertise_mode, node_id, &original_host, log_ctx)?;
                assert_unique_downstream_endpoint(
                    &mut seen_downstream_endpoints,
                    node_id,
                    &rewritten_host,
                    rewritten_port,
                    log_ctx,
                );
                let upstream_addr = format!("{}:{}", original_host, original_port);
                remember_discovered_routes(&mut discovered_routes, node_id, upstream_addr, route_key);
                endpoint.host = rewritten_host.clone().into();
                endpoint.port = rewritten_port;
                rewritten_any = true;

                info!(
                    "{} rewrote ShareFetch node_endpoint node_id={} host='{}' -> '{}' port={}",
                    log_ctx, node_id, original_host, rewritten_host, rewritten_port
                );
            }
        }
        ResponseKind::ShareAcknowledge(share_acknowledge) => {
            let ids: Vec<i32> = share_acknowledge
                .node_endpoints
                .iter()
                .map(|e| e.node_id.0)
                .collect();
            update_port_offset_assignments(advertise_mode, &ids, false, log_ctx)?;
            for endpoint in &mut share_acknowledge.node_endpoints {
                let original_host = endpoint.host.as_str().to_string();
                let node_id = endpoint.node_id.0;
                let original_port = endpoint.port;
                let (rewritten_host, rewritten_port, route_key) =
                    advertised_target(advertise_mode, node_id, &original_host, log_ctx)?;
                assert_unique_downstream_endpoint(
                    &mut seen_downstream_endpoints,
                    node_id,
                    &rewritten_host,
                    rewritten_port,
                    log_ctx,
                );
                let upstream_addr = format!("{}:{}", original_host, original_port);
                remember_discovered_routes(&mut discovered_routes, node_id, upstream_addr, route_key);
                endpoint.host = rewritten_host.clone().into();
                endpoint.port = rewritten_port;
                rewritten_any = true;

                info!(
                    "{} rewrote ShareAcknowledge node_endpoint node_id={} host='{}' -> '{}' port={}",
                    log_ctx, node_id, original_host, rewritten_host, rewritten_port
                );
            }
        }
        _ => {}
    }

    if !rewritten_any {
        return Ok(RewriteOutcome {
            payload: None,
            discovered_routes,
            topology_snapshot_ids,
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
        topology_snapshot_ids,
    })
}

fn assert_unique_downstream_endpoint(
    seen: &mut HashMap<(String, i32), i32>,
    broker_id: i32,
    host: &str,
    port: i32,
    log_ctx: &str,
) {
    let key = (host.to_string(), port);
    if let Some(existing) = seen.insert(key.clone(), broker_id) {
        if existing != broker_id {
            panic!(
                "{} fatal broker mapping collision: brokers {} and {} both mapped to downstream {}:{}",
                log_ctx, existing, broker_id, key.0, key.1
            );
        }
    }
}

fn assert_unique_mapping(
    existing: &HashMap<i32, (String, i32)>,
    broker_id: i32,
    host: &str,
    port: i32,
    log_ctx: &str,
) {
    if let Some((other_id, _)) = existing
        .iter()
        .find(|(id, endpoint)| **id != broker_id && endpoint.0 == host && endpoint.1 == port)
    {
        panic!(
            "{} fatal broker mapping collision: brokers {} and {} both mapped to downstream {}:{}",
            log_ctx, other_id, broker_id, host, port
        );
    }
}

fn update_port_offset_assignments(
    mode: &BrokerAdvertiseMode,
    ids: &[i32],
    replace_existing: bool,
    log_ctx: &str,
) -> Result<()> {
    if let BrokerAdvertiseMode::PortOffset {
        state,
        hostname,
        base_port,
        bootstrap_reserved,
        max_broker_id,
        ..
    } = mode
    {
        let mut guard = state
            .lock()
            .map_err(|_| anyhow::anyhow!("port_offset state lock poisoned"))?;
        let before = guard.id_to_endpoint.len();
        guard.update_ids(
            ids,
            hostname,
            *base_port,
            *bootstrap_reserved,
            *max_broker_id,
            replace_existing,
            log_ctx,
        )?;
        let after = guard.id_to_endpoint.len();
        if after != before {
            info!(
                "{} updated port_offset broker slot assignments: {} known broker ids",
                log_ctx, after
            );
        }
    }
    Ok(())
}
