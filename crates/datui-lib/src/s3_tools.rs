//! S3-compatible servers other tools already know about: the MinIO client's aliases,
//! `MC_HOST_<alias>`, and s3cmd's config.
//!
//! Anyone running MinIO has `mc`, and anyone using Ceph, Wasabi or DigitalOcean Spaces
//! from a terminal often has s3cmd. Both keep an endpoint and its keys in a file datui
//! can read, so those servers appear without being described again in datui's config.

use crate::cloud_browse::Environment;
use std::path::PathBuf;

/// One S3-compatible server another tool describes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ToolServer {
    /// The alias, or `s3cmd`.
    pub name: String,
    /// `https://host:port`, or `None` for AWS itself.
    pub endpoint: Option<String>,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub region: Option<String>,
    /// `Some(false)` for path-style, `Some(true)` for virtual-hosted, `None` for the
    /// endpoint's usual style.
    pub virtual_hosted: Option<bool>,
    /// Where it was found, for the row's note: `mc alias`, `MC_HOST_lab`, `s3cmd`.
    pub origin: String,
}

/// The placeholder keys `mc` writes into the aliases it creates for you.
const MC_PLACEHOLDER_KEYS: [&str; 2] = ["YOUR-ACCESS-KEY-HERE", "YOUR-SECRET-KEY-HERE"];

/// Where `mc` keeps `config.json`: `MC_CONFIG_DIR`, else one in the home directory
/// named after the binary: `.mc` (or `.mcli`, as some distributions package
/// it), and without the dot on Windows.
pub fn mc_config_paths(env: &Environment<'_>) -> Vec<PathBuf> {
    if let Some(dir) = (env.var)("MC_CONFIG_DIR").filter(|v| !v.trim().is_empty()) {
        return vec![PathBuf::from(dir.trim()).join("config.json")];
    }
    let Some(home) = &env.home else {
        return Vec::new();
    };
    ["mc", "mcli"]
        .iter()
        .map(|name| {
            let directory = if env.windows {
                name.to_string()
            } else {
                format!(".{name}")
            };
            home.join(directory).join("config.json")
        })
        .collect()
}

/// Aliases from an `mc` `config.json` (format version 10), leaving out the ones with no
/// keys or `mc`'s placeholders, and the public `play` server `mc` adds by default.
pub fn parse_mc_config(text: &str) -> Vec<ToolServer> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
        return Vec::new();
    };
    let Some(aliases) = value.get("aliases").and_then(|a| a.as_object()) else {
        return Vec::new();
    };
    let mut servers: Vec<ToolServer> = aliases
        .iter()
        .filter(|(name, _)| name.as_str() != "play")
        .filter_map(|(name, alias)| {
            let field = |key: &str| {
                alias
                    .get(key)
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
                    .filter(|v| !v.is_empty())
            };
            let url = field("url")?;
            let access_key_id = field("accessKey")?;
            let secret_access_key = field("secretKey")?;
            if MC_PLACEHOLDER_KEYS.contains(&access_key_id.as_str()) {
                return None;
            }
            Some(ToolServer {
                name: name.clone(),
                endpoint: server_endpoint(&url),
                access_key_id,
                secret_access_key,
                session_token: field("sessionToken"),
                region: None,
                virtual_hosted: match field("path").as_deref() {
                    Some("on") => Some(false),
                    Some("off") => Some(true),
                    _ => None,
                },
                origin: "mc alias".to_string(),
            })
        })
        .collect();
    servers.sort_by(|a, b| a.name.cmp(&b.name));
    servers
}

/// An alias from `MC_HOST_<alias>`: `https://ACCESS:SECRET@host:port`, or
/// `https://ACCESS:SECRET:TOKEN@host:port`, as `mc` reads it.
pub fn parse_mc_host(alias: &str, value: &str) -> Option<ToolServer> {
    let value = value.trim();
    let (scheme, rest) = value.split_once("://")?;
    if !matches!(scheme, "http" | "https") {
        return None;
    }
    let (credentials, host) = rest.rsplit_once('@')?;
    let host = host.trim_end_matches('/');
    let mut parts = credentials.splitn(3, ':');
    let access_key_id = parts.next()?.to_string();
    let second = parts.next()?;
    let (secret_access_key, session_token) = match parts.next() {
        Some(token) => (second.to_string(), Some(token.to_string())),
        None => (second.to_string(), None),
    };
    if access_key_id.is_empty() || secret_access_key.is_empty() || host.is_empty() {
        return None;
    }
    Some(ToolServer {
        name: alias.to_string(),
        endpoint: server_endpoint(&format!("{scheme}://{host}")),
        access_key_id,
        secret_access_key,
        session_token: session_token.filter(|t| !t.is_empty()),
        region: None,
        virtual_hosted: None,
        origin: format!("MC_HOST_{alias}"),
    })
}

/// Every `MC_HOST_<alias>` in `vars`.
pub fn mc_hosts(vars: &[(String, String)]) -> Vec<ToolServer> {
    let mut servers: Vec<ToolServer> = vars
        .iter()
        .filter_map(|(key, value)| {
            let alias = key.strip_prefix("MC_HOST_")?;
            (!alias.is_empty()).then(|| parse_mc_host(alias, value))?
        })
        .collect();
    servers.sort_by(|a, b| a.name.cmp(&b.name));
    servers
}

/// Where s3cmd keeps its config: `S3CMD_CONFIG`, else `%APPDATA%\s3cmd.ini` on Windows
/// and `~/.s3cfg` elsewhere.
pub fn s3cfg_path(env: &Environment<'_>) -> Option<PathBuf> {
    if let Some(path) = (env.var)("S3CMD_CONFIG").filter(|v| !v.trim().is_empty()) {
        return Some(PathBuf::from(path.trim()));
    }
    if env.windows {
        return (env.var)("APPDATA").map(|dir| PathBuf::from(dir).join("s3cmd.ini"));
    }
    env.home.as_ref().map(|home| home.join(".s3cfg"))
}

/// The server in the `[default]` section of an s3cmd config, when it has keys.
pub fn parse_s3cfg(text: &str) -> Option<ToolServer> {
    let mut in_default = false;
    let mut values = std::collections::HashMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_default = line == "[default]";
            continue;
        }
        if !in_default || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            values.insert(key.trim().to_string(), value.trim().to_string());
        }
    }
    let get = |key: &str| values.get(key).filter(|v| !v.is_empty()).cloned();
    let access_key_id = get("access_key")?;
    let secret_access_key = get("secret_key")?;
    let host_base = get("host_base").unwrap_or_else(|| "s3.amazonaws.com".to_string());
    let https = get("use_https").is_none_or(|v| v.eq_ignore_ascii_case("true"));
    let aws = host_base.eq_ignore_ascii_case("s3.amazonaws.com");
    let region = get("bucket_location").map(|location| {
        // s3cmd's "US" is AWS's us-east-1.
        if location.eq_ignore_ascii_case("us") {
            "us-east-1".to_string()
        } else {
            location
        }
    });
    Some(ToolServer {
        name: "s3cmd".to_string(),
        endpoint: (!aws).then(|| format!("{}://{host_base}", if https { "https" } else { "http" })),
        access_key_id,
        secret_access_key,
        session_token: get("access_token"),
        region,
        // `%(bucket)s` in the host template means the bucket goes in the host name.
        virtual_hosted: get("host_bucket").map(|template| template.contains("%(bucket)s")),
        origin: "s3cmd".to_string(),
    })
}

/// The endpoint of an alias URL, or `None` when it is AWS itself.
fn server_endpoint(url: &str) -> Option<String> {
    let url = url.trim().trim_end_matches('/');
    let host = url.split_once("://").map(|(_, h)| h).unwrap_or(url);
    if host.eq_ignore_ascii_case("s3.amazonaws.com") {
        return None;
    }
    Some(url.to_string())
}

#[cfg(test)]
mod tests;
