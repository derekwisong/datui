//! Google Cloud logins through `gcloud`, and the projects a login can see.
//!
//! `gcloud auth login` leaves no file object_store can read: the application-default
//! file is a separate login that many people never create. So `gcloud` is asked for a
//! token, the same way `az` is, rather than its credential database being read. One
//! token serves the listing and the open, so a bucket that is listed is one that can
//! be read.
//!
//! Each `gcloud` configuration names an account and a project. Configurations with
//! different accounts are different logins, and each is a source.
//!
//! Everything here that runs `gcloud` or touches the network blocks, and is only called
//! from a worker.

use crate::cloud_browse::Environment;
use crate::cloud_command::CommandError;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime};

/// One `gcloud` configuration: `configurations/config_<name>`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Configuration {
    pub name: String,
    pub account: Option<String>,
    pub project: Option<String>,
}

/// Where `gcloud` keeps its configuration: `CLOUDSDK_CONFIG`, else `%APPDATA%\gcloud`
/// on Windows and `~/.config/gcloud` elsewhere.
pub fn config_dir(env: &Environment<'_>) -> Option<PathBuf> {
    if let Some(dir) = (env.var)("CLOUDSDK_CONFIG").filter(|d| !d.trim().is_empty()) {
        return Some(PathBuf::from(dir));
    }
    if env.windows {
        return (env.var)("APPDATA").map(|appdata| PathBuf::from(appdata).join("gcloud"));
    }
    env.home
        .as_ref()
        .map(|home| home.join(".config").join("gcloud"))
}

/// The name of the active configuration: `CLOUDSDK_ACTIVE_CONFIG_NAME`, else the
/// `active_config` file, else `default`.
pub fn active_name(env: &Environment<'_>) -> String {
    if let Some(name) = (env.var)("CLOUDSDK_ACTIVE_CONFIG_NAME").filter(|n| !n.trim().is_empty()) {
        return name.trim().to_string();
    }
    config_dir(env)
        .and_then(|dir| (env.read)(&dir.join("active_config")))
        .map(|text| text.trim().to_string())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "default".to_string())
}

/// The `[core]` account and project of a configuration file.
pub fn parse_configuration(name: &str, text: &str) -> Configuration {
    let mut section = String::new();
    let mut configuration = Configuration {
        name: name.to_string(),
        ..Default::default()
    };
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with(['#', ';']) {
            continue;
        }
        if let Some(header) = line.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
            section = header.trim().to_ascii_lowercase();
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let value = value.trim();
        if section != "core" || value.is_empty() {
            continue;
        }
        match key.trim() {
            "account" => configuration.account = Some(value.to_string()),
            "project" => configuration.project = Some(value.to_string()),
            _ => {}
        }
    }
    configuration
}

/// Every configuration, the active one first, then by name.
pub fn configurations(env: &Environment<'_>) -> Vec<Configuration> {
    let Some(dir) = config_dir(env) else {
        return Vec::new();
    };
    let active = active_name(env);
    let mut found: Vec<Configuration> = (env.list)(&dir.join("configurations"))
        .into_iter()
        .filter_map(|path| {
            let name = path
                .file_name()?
                .to_str()?
                .strip_prefix("config_")?
                .to_string();
            let text = (env.read)(&path)?;
            Some(parse_configuration(&name, &text))
        })
        .collect();
    found.sort_by(|a, b| {
        (a.name != active)
            .cmp(&(b.name != active))
            .then_with(|| a.name.cmp(&b.name))
    });
    found
}

/// The credential type in an application-default credentials file, when it is one
/// object_store cannot use itself: `external_account` (workload identity federation),
/// `impersonated_service_account`, and anything newer.
pub fn unsupported_credential_type(text: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    let kind = value.get("type")?.as_str()?;
    (!matches!(kind, "service_account" | "authorized_user")).then(|| kind.to_string())
}

type TokenCache = Mutex<HashMap<String, (String, SystemTime)>>;

fn tokens() -> &'static TokenCache {
    static TOKENS: OnceLock<TokenCache> = OnceLock::new();
    TOKENS.get_or_init(Default::default)
}

/// A token from `gcloud` for `configuration`, with its expiry. Kept until five
/// minutes before it runs out.
pub fn token(configuration: &str, env: &Environment<'_>) -> Result<(String, SystemTime), String> {
    if let Some(cached) = tokens().lock().ok().and_then(|t| {
        t.get(configuration)
            .cloned()
            .filter(|(_, expires)| *expires > SystemTime::now() + Duration::from_secs(5 * 60))
    }) {
        return Ok(cached);
    }
    let output = (env.run)(
        "gcloud",
        &[
            "config",
            "config-helper",
            "--format=json",
            "--configuration",
            configuration,
        ],
    )
    .map_err(|e| match e {
        CommandError::Missing(_) => "needs gcloud".to_string(),
        CommandError::Failed(message)
            if message.contains("gcloud auth login") || message.contains("reauth") =>
        {
            format!("not logged in: run gcloud auth login ({})", message.trim())
        }
        other => other.to_string(),
    })?;
    let (token, expires) =
        parse_config_helper(&output).ok_or_else(|| "gcloud returned no token".to_string())?;
    if let Ok(mut cached) = tokens().lock() {
        cached.insert(configuration.to_string(), (token.clone(), expires));
    }
    Ok((token, expires))
}

/// The access token and its expiry from `gcloud config config-helper --format=json`.
/// A token without a readable expiry is kept for five minutes past the refresh margin.
pub fn parse_config_helper(text: &str) -> Option<(String, SystemTime)> {
    let value: serde_json::Value = serde_json::from_str(text.trim()).ok()?;
    let credential = value.get("credential")?;
    let token = credential.get("access_token")?.as_str()?.to_string();
    if token.is_empty() {
        return None;
    }
    let expires = credential
        .get("token_expiry")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .and_then(|at| u64::try_from(at.timestamp()).ok())
        .map(|secs| SystemTime::UNIX_EPOCH + Duration::from_secs(secs))
        .unwrap_or_else(|| SystemTime::now() + Duration::from_secs(10 * 60));
    Some((token, expires))
}

/// Polars' credential provider for a `gcloud` configuration. One per configuration for
/// the life of the process: Polars caches stores by provider, so a new provider per
/// open would build a new store, and a new connection pool, every time.
pub fn polars_provider(
    configuration: &str,
) -> polars::io::cloud::credential_provider::PlCredentialProvider {
    use polars::io::cloud::credential_provider::PlCredentialProvider;
    static PROVIDERS: OnceLock<Mutex<HashMap<String, PlCredentialProvider>>> = OnceLock::new();
    let providers = PROVIDERS.get_or_init(Default::default);
    let mut providers = providers.lock().unwrap_or_else(|e| e.into_inner());
    providers
        .entry(configuration.to_string())
        .or_insert_with(|| {
            let configuration = configuration.to_string();
            PlCredentialProvider::from_func(move || {
                let configuration = configuration.clone();
                Box::pin(async move {
                    let fetched = tokio::task::spawn_blocking(move || {
                        token(&configuration, &Environment::current())
                    })
                    .await
                    .map_err(|e| polars::error::polars_err!(ComputeError: "{e}"))?
                    .map_err(|e| polars::error::polars_err!(ComputeError: "{e}"))?;
                    let (bearer, expires) = fetched;
                    let expires = expires
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    Ok((
                        polars::io::cloud::credential_provider::ObjectStoreCredential::Gcp(
                            std::sync::Arc::new(object_store::gcp::GcpCredential { bearer }),
                        ),
                        expires,
                    ))
                })
            })
        })
        .clone()
}

/// A project a login can see.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Project {
    pub id: String,
    pub name: Option<String>,
}

/// At most this many pages of 50 projects.
const MAX_PROJECT_PAGES: usize = 10;

/// Every active project `bearer` can see, through Resource Manager's `projects:search`.
pub fn search_projects(bearer: &str) -> Result<Vec<Project>, String> {
    let mut projects = Vec::new();
    let mut page_token: Option<String> = None;
    for _ in 0..MAX_PROJECT_PAGES {
        let mut url = "https://cloudresourcemanager.googleapis.com/v3/projects:search?pageSize=50"
            .to_string();
        if let Some(token) = &page_token {
            url.push_str(&format!(
                "&pageToken={}",
                crate::cloud_browse::urlencode(token)
            ));
        }
        let mut response = crate::cloud_browse::http_agent()
            .get(&url)
            .config()
            .http_status_as_error(false)
            .build()
            .header("Authorization", &format!("Bearer {bearer}"))
            .call()
            .map_err(|e| format!("{e}"))?;
        let status = response.status().as_u16();
        let body = response
            .body_mut()
            .read_to_string()
            .map_err(|e| format!("could not read the response: {e}"))?;
        if status != 200 {
            return Err(describe_error(status, &body));
        }
        let (page, next) = parse_projects(&body)?;
        projects.extend(page);
        match next {
            Some(token) => page_token = Some(token),
            None => break,
        }
    }
    Ok(projects)
}

/// One page of `projects:search`: active projects, and the next page's token.
pub fn parse_projects(body: &str) -> Result<(Vec<Project>, Option<String>), String> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("unreadable project list: {e}"))?;
    let projects = value
        .get("projects")
        .and_then(|p| p.as_array())
        .map(|items| {
            items
                .iter()
                .filter(|p| {
                    p.get("state")
                        .and_then(|s| s.as_str())
                        .is_none_or(|s| s == "ACTIVE")
                })
                .filter_map(|p| {
                    Some(Project {
                        id: p.get("projectId")?.as_str()?.to_string(),
                        name: p
                            .get("displayName")
                            .and_then(|n| n.as_str())
                            .map(str::to_string),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let next = value
        .get("nextPageToken")
        .and_then(|t| t.as_str())
        .filter(|t| !t.is_empty())
        .map(str::to_string);
    Ok((projects, next))
}

/// `403 PERMISSION_DENIED: ...` from a Google error document, with what fixes the
/// common ones.
pub fn describe_error(status: u16, body: &str) -> String {
    let value: Option<serde_json::Value> = serde_json::from_str(body).ok();
    let error = value.as_ref().and_then(|v| v.get("error"));
    let message = error
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
        .unwrap_or("")
        .to_string();
    let reason = error
        .and_then(|e| e.get("status"))
        .and_then(|s| s.as_str())
        .unwrap_or("");
    let mut text = format!("{status} {reason}: {message}");
    if message.contains("storage.buckets.list") {
        text.push_str(". Listing buckets needs storage.buckets.list on the project (Storage Admin, or a custom role)");
    } else if message.contains("resourcemanager.projects") || message.contains("quota project") {
        text.push_str(". Finding projects needs resourcemanager.projects.get; name a project with GOOGLE_CLOUD_PROJECT, or log in with gcloud auth login");
    } else if status == 401 {
        text.push_str(". Log in again with gcloud auth login");
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configuration_files() {
        let parsed = parse_configuration(
            "work",
            "[core]\naccount = me@example.com\nproject = analytics-prod\n\n[compute]\nregion = us-east1\n",
        );
        assert_eq!(parsed.account.as_deref(), Some("me@example.com"));
        assert_eq!(parsed.project.as_deref(), Some("analytics-prod"));
        let empty = parse_configuration("new", "[core]\n");
        assert_eq!((empty.account, empty.project), (None, None));
    }

    #[test]
    fn credential_types_object_store_cannot_read() {
        assert_eq!(
            unsupported_credential_type(r#"{"type": "external_account", "audience": "x"}"#)
                .as_deref(),
            Some("external_account")
        );
        assert_eq!(
            unsupported_credential_type(r#"{"type": "authorized_user"}"#),
            None
        );
        assert_eq!(
            unsupported_credential_type(r#"{"type": "service_account"}"#),
            None
        );
    }

    #[test]
    fn config_helper_output() {
        let (token, expires) = parse_config_helper(
            r#"{"configuration": {}, "credential": {"access_token": "ya29.x", "token_expiry": "2030-01-02T03:04:05Z"}}"#,
        )
        .unwrap();
        assert_eq!(token, "ya29.x");
        assert_eq!(
            expires,
            SystemTime::UNIX_EPOCH + Duration::from_secs(1_893_553_445)
        );
        assert!(parse_config_helper(r#"{"credential": {"access_token": ""}}"#).is_none());
    }

    #[test]
    fn project_pages() {
        let (projects, next) = parse_projects(
            r#"{"projects": [
                {"projectId": "a-prod", "displayName": "A", "state": "ACTIVE"},
                {"projectId": "gone", "state": "DELETE_REQUESTED"},
                {"name": "projects/1"}
            ], "nextPageToken": "abc"}"#,
        )
        .unwrap();
        assert_eq!(
            projects,
            [Project {
                id: "a-prod".to_string(),
                name: Some("A".to_string())
            }]
        );
        assert_eq!(next.as_deref(), Some("abc"));
        let (none, last) = parse_projects("{}").unwrap();
        assert!(none.is_empty() && last.is_none());
    }

    #[test]
    fn errors_say_what_fixes_them() {
        let refused = describe_error(
            403,
            r#"{"error": {"code": 403, "status": "PERMISSION_DENIED", "message": "x does not have storage.buckets.list access"}}"#,
        );
        assert!(refused.contains("Storage Admin"), "{refused}");
    }
}
