use crate::config::{parse_key_value_pairs, AuthSwapConfig};
use anyhow::{anyhow, Result};
use std::collections::HashMap;

const ANONYMOUS: &str = "$anonymous";

#[derive(Debug, Clone)]
pub struct AuthSwap {
    downstream_credentials: Option<HashMap<String, String>>,
    upstream_credentials: HashMap<String, String>,
    mapping: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedAuth {
    pub downstream_principal: String,
    pub mapped_principal: String,
    pub upstream_secret: String,
}

impl AuthSwap {
    pub fn from_config(cfg: AuthSwapConfig) -> Result<Option<Self>> {
        if !cfg.enabled {
            return Ok(None);
        }

        let downstream_credentials = match cfg.downstream_credentials {
            Some(values) if !values.is_empty() => {
                Some(parse_key_value_pairs(&values, "auth_swap.downstream_credentials")?)
            }
            _ => None,
        };
        let upstream_credentials =
            parse_key_value_pairs(&cfg.upstream_credentials, "auth_swap.upstream_credentials")?;
        let mapping = parse_mapping(cfg.mapping.unwrap_or_default())?;

        Ok(Some(Self {
            downstream_credentials,
            upstream_credentials,
            mapping,
        }))
    }

    pub fn resolve_upstream_credential(
        &self,
        downstream_principal: Option<&str>,
        use_present_principal_without_downstream_credentials: bool,
    ) -> Result<ResolvedAuth> {
        let principal = match (&self.downstream_credentials, downstream_principal) {
            (None, Some(p)) if use_present_principal_without_downstream_credentials => {
                p.to_string()
            }
            (None, _) => ANONYMOUS.to_string(),
            (Some(_), Some(p)) => p.to_string(),
            (Some(_), None) => {
                return Err(anyhow!(
                    "auth_swap enabled with downstream_credentials, but no authenticated downstream principal is available yet"
                ));
            }
        };

        if let Some(allowed) = &self.downstream_credentials {
            if !allowed.contains_key(&principal) {
                return Err(anyhow!(
                    "No downstream credential configured for principal '{}'",
                    principal
                ));
            }
        }

        let mapped = if principal == ANONYMOUS {
            self.mapping
                .get(ANONYMOUS)
                .cloned()
                .unwrap_or_else(|| ANONYMOUS.to_string())
        } else {
            self.mapping.get(&principal).cloned().ok_or_else(|| {
                anyhow!(
                    "No auth_swap mapping found for downstream principal '{}'",
                    principal
                )
            })?
        };

        let upstream_secret = self
            .upstream_credentials
            .get(&mapped)
            .cloned()
            .ok_or_else(|| {
                anyhow!(
                    "No upstream credential configured for mapped principal '{}'",
                    mapped
                )
            })?;

        Ok(ResolvedAuth {
            downstream_principal: principal,
            mapped_principal: mapped,
            upstream_secret,
        })
    }

    pub fn validate_downstream_plain(&self, username: &str, password: &str) -> Result<()> {
        let Some(allowed) = &self.downstream_credentials else {
            return Ok(());
        };

        let expected = allowed.get(username).ok_or_else(|| {
            anyhow!(
                "No downstream credential configured for principal '{}'",
                username
            )
        })?;

        if expected != password {
            return Err(anyhow!(
                "Invalid downstream credential for principal '{}'",
                username
            ));
        }
        Ok(())
    }

    pub fn downstream_secret(&self, username: &str) -> Result<String> {
        let Some(allowed) = &self.downstream_credentials else {
            return Err(anyhow!(
                "downstream credentials are not configured for auth_swap"
            ));
        };
        allowed.get(username).cloned().ok_or_else(|| {
            anyhow!(
                "No downstream credential configured for principal '{}'",
                username
            )
        })
    }

}

fn parse_mapping(entries: Vec<String>) -> Result<HashMap<String, String>> {
    let mut mapping = HashMap::new();
    for entry in entries {
        let (src, dst) = entry
            .split_once('=')
            .ok_or_else(|| anyhow!("Invalid auth_swap.mapping entry '{}': expected src=dst", entry))?;
        if src.is_empty() {
            return Err(anyhow!(
                "Invalid auth_swap.mapping entry '{}': source cannot be empty",
                entry
            ));
        }
        // Last mapping wins.
        mapping.insert(src.to_string(), dst.to_string());
    }
    Ok(mapping)
}
