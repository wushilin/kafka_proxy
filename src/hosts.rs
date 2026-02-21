use anyhow::{anyhow, Context, Result};
use std::fs;
use std::net::IpAddr;

use crate::config::ResolveEntry;

#[derive(Debug, Clone)]
struct HostRule {
    ip: IpAddr,
    pattern: String,
    is_wildcard: bool,
    wildcard_weight: usize,
    order: usize,
}

impl HostRule {
    fn matches(&self, host: &str) -> bool {
        if !self.is_wildcard {
            return self.pattern == host;
        }

        wildcard_match_no_dot(&self.pattern, host)
    }
}

#[derive(Debug, Clone)]
pub struct HostsResolver {
    rules: Vec<HostRule>,
}

impl HostsResolver {
    #[allow(dead_code)]
    pub fn from_file(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read hosts file '{}'", path))?;

        let mut rules = Vec::new();
        let mut order = 0usize;

        for (line_no, line) in content.lines().enumerate() {
            let line = line.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }

            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 2 {
                return Err(anyhow!(
                    "Invalid hosts file {}:{}; expected '<ip> <host...>'",
                    path,
                    line_no + 1
                ));
            }

            let ip: IpAddr = parts[0].parse().with_context(|| {
                format!(
                    "Invalid IP '{}' in hosts file {}:{}",
                    parts[0],
                    path,
                    line_no + 1
                )
            })?;

            for host in &parts[1..] {
                let is_wildcard = host.contains('*');
                let wildcard_weight = host.chars().filter(|c| *c != '*').count();
                rules.push(HostRule {
                    ip,
                    pattern: (*host).to_string(),
                    is_wildcard,
                    wildcard_weight,
                    order,
                });
                order += 1;
            }
        }

        Ok(Self { rules })
    }

    pub fn resolve(&self, host: &str) -> Option<IpAddr> {
        let mut best: Option<&HostRule> = None;

        for rule in &self.rules {
            if !rule.matches(host) {
                continue;
            }

            if let Some(current) = best {
                if prefer(rule, current) {
                    best = Some(rule);
                }
            } else {
                best = Some(rule);
            }
        }

        best.map(|r| r.ip)
    }

    pub fn from_entries(entries: &[ResolveEntry]) -> Result<Self> {
        let mut rules = Vec::new();
        let mut order = 0usize;

        for (idx, entry) in entries.iter().enumerate() {
            let mut any = false;
            if let Some(v4) = &entry.ipv4 {
                let ip: IpAddr = v4.parse().with_context(|| {
                    format!("Invalid ipv4 '{}' in resolve entry {}", v4, idx + 1)
                })?;
                let is_wildcard = entry.host.contains('*');
                let wildcard_weight = entry.host.chars().filter(|c| *c != '*').count();
                rules.push(HostRule {
                    ip,
                    pattern: entry.host.clone(),
                    is_wildcard,
                    wildcard_weight,
                    order,
                });
                order += 1;
                any = true;
            }

            if let Some(v6) = &entry.ipv6 {
                let ip: IpAddr = v6.parse().with_context(|| {
                    format!("Invalid ipv6 '{}' in resolve entry {}", v6, idx + 1)
                })?;
                let is_wildcard = entry.host.contains('*');
                let wildcard_weight = entry.host.chars().filter(|c| *c != '*').count();
                rules.push(HostRule {
                    ip,
                    pattern: entry.host.clone(),
                    is_wildcard,
                    wildcard_weight,
                    order,
                });
                order += 1;
                any = true;
            }

            if !any {
                return Err(anyhow!(
                    "Invalid resolve entry {} for host '{}': expected ipv4 or ipv6",
                    idx + 1,
                    entry.host
                ));
            }
        }

        Ok(Self { rules })
    }
}

fn prefer(candidate: &HostRule, current: &HostRule) -> bool {
    if candidate.is_wildcard != current.is_wildcard {
        // Specific hostname wins over wildcard.
        return !candidate.is_wildcard;
    }

    if candidate.is_wildcard && current.is_wildcard {
        // Longer wildcard (more non-* chars) wins.
        if candidate.wildcard_weight != current.wildcard_weight {
            return candidate.wildcard_weight > current.wildcard_weight;
        }
    }

    // Later definition wins.
    candidate.order > current.order
}

fn wildcard_match_no_dot(pattern: &str, text: &str) -> bool {
    fn helper(p: &[u8], t: &[u8]) -> bool {
        if p.is_empty() {
            return t.is_empty();
        }

        if p[0] == b'*' {
            // '*' can match empty or any non-dot chars.
            if helper(&p[1..], t) {
                return true;
            }

            let mut i = 0;
            while i < t.len() && t[i] != b'.' {
                i += 1;
                if helper(&p[1..], &t[i..]) {
                    return true;
                }
            }

            return false;
        }

        if t.is_empty() || p[0] != t[0] {
            return false;
        }

        helper(&p[1..], &t[1..])
    }

    helper(pattern.as_bytes(), text.as_bytes())
}
