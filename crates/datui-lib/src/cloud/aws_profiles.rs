//! AWS profiles, read from the files the AWS CLI and SDKs share.
//!
//! object_store reads only `AWS_*` environment variables, and Polars reads
//! `~/.aws/credentials` with a pattern that takes the first key in the file, whichever
//! profile it belongs to. So datui reads the files itself and hands both libraries
//! explicit keys. Static keys come straight from the files, `credential_process` is
//! run, and everything else a profile can be (SSO, assume-role, web identity) comes
//! from `aws configure export-credentials`, so none of it is reimplemented here.

use crate::cloud::cloud_browse::Environment;
use crate::cloud::cloud_command::CommandError;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::SystemTime;

/// One profile, merged from the config and credentials files.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Profile {
    pub name: String,
    pub region: Option<String>,
    /// `endpoint_url` directly in the profile.
    pub endpoint_url: Option<String>,
    /// `endpoint_url` under `s3` in the profile's `services` section.
    pub s3_endpoint_url: Option<String>,
    pub ignore_configured_endpoint_urls: bool,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub credential_process: Option<String>,
    /// Credentials only the AWS CLI can produce here: SSO, assume-role, web identity.
    pub needs_cli: bool,
}

/// Keys that make a profile something only the CLI can resolve.
const CLI_ONLY_KEYS: [&str; 6] = [
    "sso_session",
    "sso_start_url",
    "role_arn",
    "credential_source",
    "web_identity_token_file",
    "login_session",
];

impl Profile {
    /// Whether the profile says how to get credentials at all. A profile holding only a
    /// region is settings for some other profile's use, not a login.
    pub fn has_credentials(&self) -> bool {
        (self.access_key_id.is_some() && self.secret_access_key.is_some())
            || self.credential_process.is_some()
            || self.needs_cli
    }

    /// The S3 endpoint for this profile, in the order the AWS SDKs apply:
    /// `AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL`, the `services` entry, then the
    /// profile's own `endpoint_url`. `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS` or the
    /// profile's `ignore_configured_endpoint_urls` turns all of them off.
    pub fn s3_endpoint(&self, var: &dyn Fn(&str) -> Option<String>) -> Option<String> {
        let ignored = var("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS")
            .is_some_and(|v| v.trim().eq_ignore_ascii_case("true"));
        if ignored || self.ignore_configured_endpoint_urls {
            return None;
        }
        ["AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL"]
            .iter()
            .filter_map(|key| var(key))
            .chain(self.s3_endpoint_url.clone())
            .chain(self.endpoint_url.clone())
            .map(|v| v.trim().to_string())
            .find(|v| !v.is_empty())
    }
}

/// The config file: `AWS_CONFIG_FILE`, else `~/.aws/config`.
pub fn config_path(env: &Environment<'_>) -> Option<PathBuf> {
    file_path(env, "AWS_CONFIG_FILE", "config")
}

/// The credentials file: `AWS_SHARED_CREDENTIALS_FILE`, else `~/.aws/credentials`.
pub fn credentials_path(env: &Environment<'_>) -> Option<PathBuf> {
    file_path(env, "AWS_SHARED_CREDENTIALS_FILE", "credentials")
}

fn file_path(env: &Environment<'_>, variable: &str, name: &str) -> Option<PathBuf> {
    match (env.var)(variable).filter(|v| !v.trim().is_empty()) {
        Some(path) => Some(crate::config::expand_config_path(path.trim())),
        None => env.home.as_ref().map(|home| home.join(".aws").join(name)),
    }
}

/// The profile the AWS tools would use with no `--profile`: `AWS_PROFILE`, then
/// `AWS_DEFAULT_PROFILE`, then `default`.
pub fn active_profile(env: &Environment<'_>) -> String {
    ["AWS_PROFILE", "AWS_DEFAULT_PROFILE"]
        .iter()
        .filter_map(|key| (env.var)(key))
        .map(|v| v.trim().to_string())
        .find(|v| !v.is_empty())
        .unwrap_or_else(|| "default".to_string())
}

/// Every profile in the two files, `default` first, then by name.
pub fn load(env: &Environment<'_>) -> Vec<Profile> {
    let config = config_path(env)
        .and_then(|p| (env.read)(&p))
        .unwrap_or_default();
    let credentials = credentials_path(env)
        .and_then(|p| (env.read)(&p))
        .unwrap_or_default();
    parse(&config, &credentials)
}

/// One `key = value`, with the indented lines under a key that has no value of its own
/// (`s3 =` followed by `  endpoint_url = ...`).
#[derive(Debug, Default)]
struct Entry {
    value: String,
    nested: HashMap<String, String>,
}

type Sections = Vec<(String, HashMap<String, Entry>)>;

/// The sections of an AWS-style INI file, keys lowercased, in file order.
fn sections(text: &str) -> Sections {
    let mut out: Sections = Vec::new();
    let mut last_key: Option<String> = None;
    for raw in text.lines() {
        let line = raw.trim_end();
        let trimmed = line.trim_start();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with(';') {
            continue;
        }
        if let Some(header) = trimmed.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
            out.push((header.trim().to_string(), HashMap::new()));
            last_key = None;
            continue;
        }
        let Some((key, value)) = trimmed.split_once('=') else {
            continue;
        };
        let key = key.trim().to_ascii_lowercase();
        let value = value.trim().to_string();
        let Some((_, entries)) = out.last_mut() else {
            continue;
        };
        let indented = line.len() != trimmed.len();
        match &last_key {
            Some(parent) if indented && entries.get(parent).is_some_and(|e| e.value.is_empty()) => {
                if let Some(entry) = entries.get_mut(parent) {
                    entry.nested.insert(key, value);
                }
            }
            _ => {
                entries.insert(
                    key.clone(),
                    Entry {
                        value,
                        nested: HashMap::new(),
                    },
                );
                last_key = Some(key);
            }
        }
    }
    out
}

/// Profiles from the text of the config and credentials files.
pub fn parse(config: &str, credentials: &str) -> Vec<Profile> {
    let config = sections(config);
    let credentials = sections(credentials);
    let value = |entries: &HashMap<String, Entry>, key: &str| {
        entries
            .get(key)
            .map(|e| e.value.clone())
            .filter(|v| !v.is_empty())
    };

    let mut profiles: HashMap<String, Profile> = HashMap::new();
    for (header, entries) in &config {
        let name = if header == "default" {
            "default"
        } else if let Some(name) = header.strip_prefix("profile ") {
            name.trim()
        } else {
            continue;
        };
        let profile = profiles.entry(name.to_string()).or_insert_with(|| Profile {
            name: name.to_string(),
            ..Default::default()
        });
        profile.region = value(entries, "region").or(profile.region.take());
        profile.endpoint_url = value(entries, "endpoint_url");
        profile.ignore_configured_endpoint_urls = value(entries, "ignore_configured_endpoint_urls")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));
        profile.access_key_id = value(entries, "aws_access_key_id");
        profile.secret_access_key = value(entries, "aws_secret_access_key");
        profile.session_token = value(entries, "aws_session_token");
        profile.credential_process = value(entries, "credential_process");
        profile.needs_cli = CLI_ONLY_KEYS.iter().any(|k| entries.contains_key(*k));
        if let Some(services) = value(entries, "services") {
            profile.s3_endpoint_url = config
                .iter()
                .find(|(h, _)| {
                    h.strip_prefix("services ").map(str::trim) == Some(services.as_str())
                })
                .and_then(|(_, e)| e.get("s3"))
                .and_then(|s3| s3.nested.get("endpoint_url"))
                .cloned()
                .filter(|v| !v.is_empty());
        }
    }
    // The credentials file wins for keys, as it does for the AWS tools.
    for (name, entries) in &credentials {
        let profile = profiles.entry(name.clone()).or_insert_with(|| Profile {
            name: name.clone(),
            ..Default::default()
        });
        if let Some(key) = value(entries, "aws_access_key_id") {
            profile.access_key_id = Some(key);
        }
        if let Some(secret) = value(entries, "aws_secret_access_key") {
            profile.secret_access_key = Some(secret);
        }
        if let Some(token) = value(entries, "aws_session_token") {
            profile.session_token = Some(token);
        }
        if let Some(process) = value(entries, "credential_process") {
            profile.credential_process = Some(process);
        }
    }

    let mut out: Vec<Profile> = profiles.into_values().collect();
    out.sort_by(|a, b| {
        (a.name != "default")
            .cmp(&(b.name != "default"))
            .then_with(|| a.name.cmp(&b.name))
    });
    out
}

/// Keys that sign requests, and when they stop working.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub expires: Option<SystemTime>,
}

/// Temporary credentials by profile, so a command runs once per expiry rather than once
/// per open.
fn cached() -> &'static crate::cloud::cloud_command::Expiring<Credentials> {
    static CACHE: OnceLock<crate::cloud::cloud_command::Expiring<Credentials>> = OnceLock::new();
    CACHE.get_or_init(Default::default)
}

/// The keys for `profile`: straight from the files, from its `credential_process`, or
/// from the AWS CLI. Runs commands, so only ever call it from a worker.
pub fn credentials(profile: &Profile, env: &Environment<'_>) -> Result<Credentials, String> {
    if let (Some(key), Some(secret)) = (&profile.access_key_id, &profile.secret_access_key)
        && profile.credential_process.is_none()
        && !profile.needs_cli
    {
        return Ok(Credentials {
            access_key_id: key.clone(),
            secret_access_key: secret.clone(),
            session_token: profile.session_token.clone(),
            expires: None,
        });
    }

    if let Some(fresh) = cached().get(&profile.name) {
        return Ok(fresh);
    }

    let output = if let Some(line) = &profile.credential_process {
        let words = crate::cloud::cloud_command::split_command_line(line)
            .ok_or_else(|| format!("credential_process for {} cannot be read", profile.name))?;
        let args: Vec<&str> = words[1..].iter().map(String::as_str).collect();
        (env.run)(&words[0], &args).map_err(|e| match e {
            CommandError::Missing(program) => {
                format!(
                    "credential_process for {}: {program} not found",
                    profile.name
                )
            }
            other => format!("credential_process for {}: {other}", profile.name),
        })?
    } else if profile.needs_cli {
        let args = [
            "configure",
            "export-credentials",
            "--profile",
            profile.name.as_str(),
            "--format",
            "process",
        ];
        (env.run)("aws", &args).map_err(|e| match e {
            CommandError::Missing(_) => "needs the AWS CLI".to_string(),
            other => other.to_string(),
        })?
    } else {
        return Err(format!("profile {} has no credentials", profile.name));
    };

    let fresh = parse_process_output(&output)
        .ok_or_else(|| format!("profile {}: credentials were not readable", profile.name))?;
    cached().put(&profile.name, fresh.clone(), fresh.expires);
    Ok(fresh)
}

/// The JSON `credential_process` prints, which `aws configure export-credentials
/// --format process` prints too.
pub fn parse_process_output(text: &str) -> Option<Credentials> {
    let value: serde_json::Value = serde_json::from_str(text.trim()).ok()?;
    let field = |name: &str| {
        value
            .get(name)
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .filter(|v| !v.is_empty())
    };
    Some(Credentials {
        access_key_id: field("AccessKeyId")?,
        secret_access_key: field("SecretAccessKey")?,
        session_token: field("SessionToken"),
        expires: field("Expiration")
            .and_then(|at| chrono::DateTime::parse_from_rfc3339(&at).ok())
            .map(SystemTime::from),
    })
}

#[cfg(test)]
mod tests;
