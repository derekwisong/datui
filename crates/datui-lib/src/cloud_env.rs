//! Cloud variables from `[cloud] env_files`, beside the real environment.
//!
//! A project's `.env` often holds the keys to its bucket. Reading one is opt-in: a
//! `.env` picked up from whatever directory datui starts in, unasked, would be picking
//! up secrets. Only cloud variable names are taken, the real environment wins over the
//! files, and nothing is exported, so no program datui starts ever sees them.

use crate::config::CloudConfig;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

/// Variables datui itself reads. `MC_HOST_<alias>` is matched by prefix, and the names
/// `[[cloud.sources]]` point at with `*_env` are added to these.
pub const KNOWN: &[&str] = &[
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "AWS_PROFILE",
    "AWS_ENDPOINT_URL",
    "AWS_ENDPOINT_URL_S3",
    "AWS_ENDPOINT",
    "AWS_CONFIG_FILE",
    "AWS_SHARED_CREDENTIALS_FILE",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GOOGLE_CLOUD_PROJECT",
    "GCLOUD_PROJECT",
    "CLOUDSDK_CORE_PROJECT",
    "DATUI_GCP_PROJECT",
    "AZURE_STORAGE_CONNECTION_STRING",
    "AZURE_STORAGE_ACCOUNT_NAME",
    "AZURE_STORAGE_ACCOUNT_KEY",
    "AZURE_STORAGE_SAS_TOKEN",
    "AZURE_TENANT_ID",
    "AZURE_CLIENT_ID",
    "AZURE_CLIENT_SECRET",
    "AZURE_FEDERATED_TOKEN_FILE",
];

fn store() -> &'static RwLock<HashMap<String, String>> {
    static VARS: std::sync::OnceLock<RwLock<HashMap<String, String>>> = std::sync::OnceLock::new();
    VARS.get_or_init(Default::default)
}

/// Read the files `cloud.env_files` names, relative to `dir`, and keep their cloud
/// variables for [`var`]. Returns a note for each file that could not be read.
pub fn load(cloud: &CloudConfig, dir: &Path) -> Vec<String> {
    let named: Vec<&str> = cloud
        .sources
        .iter()
        .flat_map(|s| {
            [
                s.access_key_id_env.as_deref(),
                s.secret_access_key_env.as_deref(),
                s.session_token_env.as_deref(),
                s.account_key_env.as_deref(),
                s.sas_env.as_deref(),
                s.connection_string_env.as_deref(),
            ]
        })
        .flatten()
        .collect();
    let allowed =
        |key: &str| KNOWN.contains(&key) || key.starts_with("MC_HOST_") || named.contains(&key);
    let mut vars = HashMap::new();
    let mut notes = Vec::new();
    for file in &cloud.env_files {
        let path = resolve_path(file, dir);
        match std::fs::read_to_string(&path) {
            Ok(text) => vars.extend(parse(&text, &allowed)),
            Err(e) => notes.push(format!("env_files: {}: {e}", path.display())),
        }
    }
    if let Ok(mut store) = store().write() {
        *store = vars;
    }
    notes
}

/// `~/x` under the home directory, a relative path under `dir`.
fn resolve_path(file: &str, dir: &Path) -> PathBuf {
    if let Some(rest) = file.strip_prefix("~/").or_else(|| file.strip_prefix("~\\"))
        && let Some(home) = dirs::home_dir()
    {
        return home.join(rest);
    }
    let path = PathBuf::from(file);
    if path.is_absolute() {
        path
    } else {
        dir.join(path)
    }
}

/// The `KEY=value` lines of a dotenv file whose key `allowed` accepts. Handles
/// `export `, `#` comments, and single or double quotes around a value.
pub fn parse(text: &str, allowed: &dyn Fn(&str) -> bool) -> Vec<(String, String)> {
    text.lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let line = line.strip_prefix("export ").unwrap_or(line);
            let (key, value) = line.split_once('=')?;
            let key = key.trim();
            if !allowed(key) {
                return None;
            }
            let value = value.trim();
            let value = match value.chars().next() {
                Some(quote @ ('"' | '\'')) => {
                    let inner = &value[1..];
                    inner.split(quote).next().unwrap_or(inner)
                }
                // An unquoted value ends at a comment.
                _ => value.split(" #").next().unwrap_or(value).trim(),
            };
            Some((key.to_string(), value.to_string()))
        })
        .collect()
}

/// A variable: the real environment's, else one from the env files.
pub fn var(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .or_else(|| store().read().ok()?.get(key).cloned())
}

/// Every variable: the real environment, and the env files' where it has none.
pub fn vars() -> Vec<(String, String)> {
    let mut all: Vec<(String, String)> = std::env::vars().collect();
    if let Ok(store) = store().read() {
        for (key, value) in store.iter() {
            if std::env::var_os(key).is_none() {
                all.push((key.clone(), value.clone()));
            }
        }
    }
    all
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_cloud_variables_are_read() {
        let text = "# a project's .env\n\
                    DATABASE_URL=postgres://secret\n\
                    export AWS_ACCESS_KEY_ID=AKIAFILE\n\
                    AWS_SECRET_ACCESS_KEY=\"with spaces\" # quoted\n\
                    AZURE_STORAGE_ACCOUNT_NAME=acct # trailing comment\n\
                    MC_HOST_lab=http://k:s@127.0.0.1:9000\n\
                    ONPREM_KEY='single'\n";
        let allowed =
            |k: &str| KNOWN.contains(&k) || k.starts_with("MC_HOST_") || k == "ONPREM_KEY";
        let parsed: HashMap<String, String> = parse(text, &allowed).into_iter().collect();
        assert_eq!(parsed.get("DATABASE_URL"), None);
        assert_eq!(parsed["AWS_ACCESS_KEY_ID"], "AKIAFILE");
        assert_eq!(parsed["AWS_SECRET_ACCESS_KEY"], "with spaces");
        assert_eq!(parsed["AZURE_STORAGE_ACCOUNT_NAME"], "acct");
        assert_eq!(parsed["MC_HOST_lab"], "http://k:s@127.0.0.1:9000");
        assert_eq!(parsed["ONPREM_KEY"], "single");
    }

    #[test]
    fn files_are_found_relative_to_the_working_directory() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            dir.path().join(".env"),
            "DATUI_ENV_FILE_TEST_ONLY=x\nONPREM_SECRET_FOR_ENV_FILE_TEST=s3cr3t\n",
        )
        .unwrap();
        let cloud = CloudConfig {
            env_files: vec![".env".to_string(), "missing.env".to_string()],
            sources: vec![crate::config::CloudSourceConfig {
                name: "onprem".to_string(),
                secret_access_key_env: Some("ONPREM_SECRET_FOR_ENV_FILE_TEST".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let notes = load(&cloud, dir.path());
        assert_eq!(notes.len(), 1, "{notes:?}");
        assert!(notes[0].contains("missing.env"));
        assert_eq!(
            var("ONPREM_SECRET_FOR_ENV_FILE_TEST").as_deref(),
            Some("s3cr3t")
        );
        assert_eq!(
            var("DATUI_ENV_FILE_TEST_ONLY"),
            None,
            "not a cloud variable"
        );
        assert!(
            std::env::var("ONPREM_SECRET_FOR_ENV_FILE_TEST").is_err(),
            "never exported"
        );
        load(&CloudConfig::default(), dir.path());
    }
}
