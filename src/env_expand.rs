use anyhow::{anyhow, Context, Result};
use regex::Regex;

pub fn expand_env_placeholders(input: &str) -> Result<String> {
    let re = Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::([^}]*))?\}")
        .context("Failed to compile environment placeholder regex")?;

    let mut out = String::with_capacity(input.len());
    let mut last = 0usize;

    for caps in re.captures_iter(input) {
        let m = caps
            .get(0)
            .ok_or_else(|| anyhow!("Invalid environment placeholder match"))?;
        out.push_str(&input[last..m.start()]);

        let var_name = caps
            .get(1)
            .map(|v| v.as_str())
            .ok_or_else(|| anyhow!("Invalid environment placeholder variable name"))?;
        let default = caps.get(2).map(|v| v.as_str());
        let value = std::env::var(var_name).ok().filter(|v| !v.is_empty());

        let replacement = match (value, default) {
            (Some(v), _) => v,
            (None, Some(d)) => d.to_string(),
            (None, None) => {
                return Err(anyhow!(
                    "{} environment variable is not defined",
                    var_name
                ));
            }
        };
        out.push_str(&replacement);
        last = m.end();
    }

    out.push_str(&input[last..]);
    Ok(out)
}
