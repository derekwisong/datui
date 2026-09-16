//! Every object store datui can read, as a list of sources rather than one of each kind.
//!
//! A source is a store plus the login that reaches it: the default S3 settings, a
//! Google login, or an entry in `[[cloud.sources]]`. Each has an ID, and the ID is what
//! keeps two stores apart when their bucket names collide: two MinIO servers can both
//! have a bucket called `data`, so a URL from an S3-compatible source names it,
//! `s3://<id>@bucket/key`. Everywhere else the location is unambiguous and URLs stay the
//! standard ones, so a URL copied out of datui still works in any other tool.
//!
//! Discovery here reads environment variables and asks whether files exist. It never
//! touches the network: listing is `cloud_browse`'s job, and it runs on a worker.

use crate::cloud_browse::{Environment, ProviderKind};
use crate::config::{CloudConfig, CloudSourceConfig};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// The source built from `[cloud] s3_*`, `--s3-*` and the `AWS_*` environment: what a
/// plain `s3://bucket/key` has always meant.
pub const DEFAULT_S3: &str = "s3-default";
/// The Google login found in the environment or the application-default file.
pub const DEFAULT_GCS: &str = "gcs-default";

/// Where a source came from. Lower wins when the same ID turns up twice, and sources
/// are listed in this order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Tier {
    /// `[[cloud.sources]]`, or `[cloud] s3_*` for the default source.
    Config,
    /// Environment variables.
    Environment,
    /// Files other tools keep: `~/.aws`, the `gcloud` login.
    Tools,
}

/// How to reach one S3 or S3-compatible store.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct S3Settings {
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub region: Option<String>,
    /// `None` takes the endpoint's usual style: virtual-hosted for AWS, path-style for a
    /// custom endpoint.
    pub virtual_hosted: Option<bool>,
    /// Fill gaps from the `AWS_*` environment. Only the default source does: a source
    /// in the config names its own keys, and borrowing the shell's would sign its
    /// requests as somebody else.
    pub from_env: bool,
}

impl S3Settings {
    /// The default source's settings, from the effective `[cloud]` config.
    pub fn from_config(cloud: &CloudConfig) -> Self {
        Self {
            endpoint: cloud.s3_endpoint_url.clone(),
            access_key_id: cloud.s3_access_key_id.clone(),
            secret_access_key: cloud.s3_secret_access_key.clone(),
            session_token: None,
            region: cloud.s3_region.clone(),
            virtual_hosted: None,
            from_env: true,
        }
    }

    /// Whether requests put the bucket in the host name.
    ///
    /// A custom endpoint is almost always path-style: a MinIO container on localhost has
    /// no wildcard DNS to give each bucket a subdomain of its own.
    pub fn virtual_hosted_style(&self) -> bool {
        self.virtual_hosted.unwrap_or(self.endpoint.is_none())
    }
}

/// One store and the login that reaches it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    pub id: String,
    /// Shown on the home screen.
    pub label: String,
    pub kind: ProviderKind,
    pub tier: Tier,
    /// Which credentials were found, in a word or two: `datui config`, `AWS_PROFILE`.
    pub origin: String,
    /// Only meaningful for [`ProviderKind::S3`].
    pub s3: S3Settings,
    /// The GCP project whose buckets are listed.
    pub project: Option<String>,
    /// The AWS profile in use, when one is named.
    pub profile: Option<String>,
    /// Buckets named in the config, shown even when the login cannot list them.
    pub buckets: Vec<String>,
    /// Why this source cannot be used, when something in its own definition says so.
    pub problem: Option<String>,
}

impl Source {
    /// Whether URLs from this source carry its ID. Only an S3-compatible server other
    /// than the default one needs to: its buckets are named only within its endpoint.
    pub fn named_in_urls(&self) -> bool {
        self.kind == ProviderKind::S3 && self.id != DEFAULT_S3 && self.s3.endpoint.is_some()
    }

    /// The URL of one of this source's buckets.
    pub fn bucket_url(&self, bucket: &str) -> String {
        let scheme = self.kind.scheme();
        if self.named_in_urls() {
            format!("{scheme}://{}@{bucket}", self.id)
        } else {
            format!("{scheme}://{bucket}")
        }
    }

    /// The account the buckets belong to, when there is one to name.
    pub fn detail(&self) -> Option<String> {
        match (&self.project, &self.profile) {
            (Some(project), _) => Some(format!("project: {project}")),
            (None, Some(profile)) => Some(format!("profile: {profile}")),
            (None, None) => self.s3.endpoint.as_deref().and_then(endpoint_host),
        }
    }

    /// True when this source can enumerate its own buckets.
    pub fn can_list_buckets(&self) -> bool {
        match self.kind {
            ProviderKind::Gcs => self.project.is_some(),
            ProviderKind::S3 => true,
        }
    }

    /// What this source points at. Anything cached under the source's ID is stale
    /// once this changes: an endpoint moved to another server lists other buckets.
    pub fn fingerprint(&self) -> String {
        [
            self.kind.scheme(),
            self.s3.endpoint.as_deref().unwrap_or(""),
            self.s3.access_key_id.as_deref().unwrap_or(""),
            self.project.as_deref().unwrap_or(""),
            self.profile.as_deref().unwrap_or(""),
        ]
        .join("|")
    }
}

/// The host and port of an endpoint URL, for display.
pub fn endpoint_host(endpoint: &str) -> Option<String> {
    let rest = endpoint
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(endpoint);
    let host = rest.split(['/', '?', '#']).next()?.trim();
    (!host.is_empty()).then(|| host.to_string())
}

/// Every source this machine and the config describe, in display order.
///
/// `config` is the effective one, with the environment and command line folded in.
/// A configured source whose name matches a detected one replaces it.
pub fn discover(config: &CloudConfig, env: &Environment<'_>) -> Vec<Source> {
    let mut sources: Vec<Source> = crate::cloud_browse::detect(config, env)
        .into_iter()
        .map(|provider| {
            let tier = match provider.note.as_str() {
                "datui config" => Tier::Config,
                "~/.aws" | "gcloud" => Tier::Tools,
                _ => Tier::Environment,
            };
            let id = match provider.kind {
                ProviderKind::S3 => DEFAULT_S3,
                ProviderKind::Gcs => DEFAULT_GCS,
            };
            Source {
                id: id.to_string(),
                label: match provider.kind {
                    ProviderKind::S3 if provider.endpoint.is_none() => "Amazon S3".to_string(),
                    ProviderKind::S3 => "S3-compatible".to_string(),
                    ProviderKind::Gcs => "Google Cloud".to_string(),
                },
                kind: provider.kind,
                tier,
                origin: provider.note,
                s3: match provider.kind {
                    ProviderKind::S3 => S3Settings::from_config(config),
                    ProviderKind::Gcs => S3Settings::default(),
                },
                project: provider.project,
                profile: provider.profile,
                buckets: Vec::new(),
                problem: None,
            }
        })
        .collect();

    for configured in &config.sources {
        let source = configured_source(configured, env.var);
        match sources.iter_mut().find(|s| s.id == source.id) {
            Some(existing) => *existing = source,
            None => sources.push(source),
        }
    }

    // Stable: rows must not move when a listing lands.
    sources.sort_by(|a, b| a.tier.cmp(&b.tier).then_with(|| a.id.cmp(&b.id)));
    sources
}

/// A `[[cloud.sources]]` entry as a source. The config has been validated, so the kind
/// is one datui knows.
fn configured_source(
    configured: &CloudSourceConfig,
    var: &dyn Fn(&str) -> Option<String>,
) -> Source {
    let kind = match configured.kind.as_deref() {
        Some("gcs") => ProviderKind::Gcs,
        _ => ProviderKind::S3,
    };
    let mut problem = None;
    // A variable that is named but unset is a mistake worth reporting, not a reason to
    // fall back to whatever the shell happens to hold.
    let mut from_named = |name: &Option<String>| -> Option<String> {
        let name = name.as_deref()?;
        match var(name).map(|v| v.trim().to_string()) {
            Some(value) if !value.is_empty() => Some(value),
            _ => {
                problem.get_or_insert_with(|| format!("{name} is not set"));
                None
            }
        }
    };
    let s3 = S3Settings {
        endpoint: configured.endpoint_url.clone(),
        access_key_id: from_named(&configured.access_key_id_env),
        secret_access_key: from_named(&configured.secret_access_key_env),
        session_token: from_named(&configured.session_token_env),
        region: configured.region.clone(),
        virtual_hosted: configured.addressing.as_deref().map(|a| a == "virtual"),
        from_env: false,
    };
    Source {
        id: configured.name.clone(),
        label: configured
            .label
            .clone()
            .unwrap_or_else(|| configured.name.clone()),
        kind,
        tier: Tier::Config,
        origin: "datui config".to_string(),
        s3,
        project: None,
        profile: None,
        buckets: configured.buckets.clone(),
        problem,
    }
}

/// A cloud URL resolved to the plain URL the libraries understand and the settings
/// that reach it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    /// The URL without a source ID.
    pub url: String,
    pub kind: ProviderKind,
    pub source_id: String,
    pub s3: S3Settings,
}

/// Which source a plain `s3://bucket` belongs to, when a source other than the default
/// listed it. Opening a bucket you browsed to has to use the login that found it.
fn bucket_sources() -> &'static Mutex<HashMap<String, String>> {
    static MAP: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();
    MAP.get_or_init(Default::default)
}

/// Remember that `source` reaches `bucket`, for URLs that do not name their source.
pub fn remember_bucket(source: &Source, bucket: &str) {
    if source.named_in_urls() || source.id == DEFAULT_S3 || source.id == DEFAULT_GCS {
        return;
    }
    let key = format!("{}://{bucket}", source.kind.scheme());
    if let Ok(mut map) = bucket_sources().lock() {
        map.insert(key, source.id.clone());
    }
}

fn remembered(kind: ProviderKind, bucket: &str) -> Option<String> {
    let key = format!("{}://{bucket}", kind.scheme());
    bucket_sources().lock().ok()?.get(&key).cloned()
}

/// Resolve `url` against the sources in `config`, reading `*_env` variables from the
/// process environment.
pub fn resolve(url: &str, config: &CloudConfig) -> Result<Resolved, String> {
    resolve_with(url, config, &|key| std::env::var(key).ok())
}

/// As [`resolve`], with the environment supplied.
pub fn resolve_with(
    url: &str,
    config: &CloudConfig,
    var: &dyn Fn(&str) -> Option<String>,
) -> Result<Resolved, String> {
    let (id, plain) = crate::source::split_source_id(url);
    let (kind, bucket, _) = crate::cloud_browse::split_bucket_url(&plain)
        .ok_or_else(|| format!("not an object-store URL: {url}"))?;
    let named = |id: &str| config.sources.iter().find(|s| s.name == id);

    let source = match id {
        Some(id) => {
            let Some(configured) = named(id) else {
                return Err(unknown_source(id, config));
            };
            let source = configured_source(configured, var);
            if !source.named_in_urls() {
                return Err(format!(
                    "\"{id}\" has no endpoint_url, so it is not S3-compatible and its URLs are \
                     plain s3://bucket/key"
                ));
            }
            Some(source)
        }
        None => remembered(kind, &bucket)
            .and_then(|id| named(&id).map(|c| configured_source(c, var)))
            .filter(|s| s.kind == kind),
    };

    match source {
        Some(source) => {
            if let Some(problem) = &source.problem {
                return Err(format!("source \"{}\": {problem}", source.id));
            }
            Ok(Resolved {
                url: plain.into_owned(),
                kind,
                source_id: source.id,
                s3: source.s3,
            })
        }
        None => Ok(Resolved {
            url: plain.into_owned(),
            kind,
            source_id: match kind {
                ProviderKind::S3 => DEFAULT_S3,
                ProviderKind::Gcs => DEFAULT_GCS,
            }
            .to_string(),
            s3: match kind {
                ProviderKind::S3 => S3Settings::from_config(config),
                ProviderKind::Gcs => S3Settings::default(),
            },
        }),
    }
}

fn unknown_source(id: &str, config: &CloudConfig) -> String {
    let names: Vec<&str> = config
        .sources
        .iter()
        .filter(|s| s.kind.as_deref() == Some("s3") && s.endpoint_url.is_some())
        .map(|s| s.name.as_str())
        .collect();
    if names.is_empty() {
        format!("no S3-compatible source is named \"{id}\" in [[cloud.sources]]")
    } else {
        format!(
            "no S3-compatible source is named \"{id}\". Sources: {}",
            names.join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    fn minio(name: &str, endpoint: &str) -> CloudSourceConfig {
        CloudSourceConfig {
            name: name.to_string(),
            kind: Some("s3".to_string()),
            endpoint_url: Some(endpoint.to_string()),
            access_key_id_env: Some(format!("{}_KEY", name.to_uppercase())),
            secret_access_key_env: Some(format!("{}_SECRET", name.to_uppercase())),
            ..Default::default()
        }
    }

    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |key| map.get(key).cloned()
    }

    fn nothing_on_disk() -> (impl Fn(&Path) -> bool, impl Fn(&Path) -> Option<String>) {
        (|_: &Path| false, |_: &Path| None)
    }

    #[test]
    fn two_servers_with_the_same_bucket_resolve_to_their_own_endpoints() {
        let config = CloudConfig {
            sources: vec![
                minio("lab", "http://127.0.0.1:9000"),
                minio("onprem", "https://minio.corp.example:9000"),
            ],
            ..Default::default()
        };
        let var = vars(&[
            ("LAB_KEY", "lab-key"),
            ("LAB_SECRET", "lab-secret"),
            ("ONPREM_KEY", "corp-key"),
            ("ONPREM_SECRET", "corp-secret"),
        ]);

        let lab = resolve_with("s3://lab@data/sales.parquet", &config, &var).unwrap();
        let corp = resolve_with("s3://onprem@data/sales.parquet", &config, &var).unwrap();

        assert_eq!(lab.url, "s3://data/sales.parquet");
        assert_eq!(corp.url, "s3://data/sales.parquet");
        assert_eq!(lab.s3.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
        assert_eq!(
            corp.s3.endpoint.as_deref(),
            Some("https://minio.corp.example:9000")
        );
        assert_eq!(lab.s3.access_key_id.as_deref(), Some("lab-key"));
        assert_eq!(corp.s3.secret_access_key.as_deref(), Some("corp-secret"));
        assert!(!lab.s3.from_env && !lab.s3.virtual_hosted_style());
    }

    #[test]
    fn a_plain_url_is_the_default_source_as_before() {
        let config = CloudConfig {
            s3_endpoint_url: Some("http://localhost:9000".to_string()),
            sources: vec![minio("lab", "http://127.0.0.1:9000")],
            ..Default::default()
        };
        let resolved = resolve_with("s3://data/key.parquet", &config, &vars(&[])).unwrap();
        assert_eq!(resolved.source_id, DEFAULT_S3);
        assert_eq!(resolved.url, "s3://data/key.parquet");
        assert_eq!(
            resolved.s3.endpoint.as_deref(),
            Some("http://localhost:9000")
        );
        assert!(resolved.s3.from_env);

        let gcs = resolve_with("gs://bucket/key", &config, &vars(&[])).unwrap();
        assert_eq!(gcs.source_id, DEFAULT_GCS);
    }

    #[test]
    fn an_unknown_source_names_the_ones_that_exist() {
        let config = CloudConfig {
            sources: vec![minio("lab", "http://127.0.0.1:9000")],
            ..Default::default()
        };
        let err = resolve_with("s3://nope@data/key", &config, &vars(&[])).unwrap_err();
        assert!(err.contains("\"nope\"") && err.contains("lab"), "{err}");
    }

    #[test]
    fn an_aws_source_cannot_be_named_in_a_url() {
        let config = CloudConfig {
            sources: vec![CloudSourceConfig {
                name: "second-account".to_string(),
                kind: Some("s3".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let err = resolve_with("s3://second-account@data/key", &config, &vars(&[])).unwrap_err();
        assert!(err.contains("not S3-compatible"), "{err}");
    }

    #[test]
    fn a_named_variable_that_is_unset_is_reported_not_borrowed() {
        let config = CloudConfig {
            sources: vec![minio("lab", "http://127.0.0.1:9000")],
            ..Default::default()
        };
        let err =
            resolve_with("s3://lab@data/key", &config, &vars(&[("LAB_KEY", "k")])).unwrap_err();
        assert!(err.contains("LAB_SECRET is not set"), "{err}");
    }

    #[test]
    fn a_bucket_listed_by_an_aws_source_opens_with_that_login() {
        let config = CloudConfig {
            sources: vec![CloudSourceConfig {
                name: "second-account".to_string(),
                kind: Some("s3".to_string()),
                access_key_id_env: Some("SECOND_KEY".to_string()),
                secret_access_key_env: Some("SECOND_SECRET".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let var = vars(&[("SECOND_KEY", "k2"), ("SECOND_SECRET", "s2")]);
        let source = configured_source(&config.sources[0], &var);
        remember_bucket(&source, "only-in-second-account");

        let resolved =
            resolve_with("s3://only-in-second-account/x.parquet", &config, &var).unwrap();
        assert_eq!(resolved.source_id, "second-account");
        assert_eq!(resolved.s3.access_key_id.as_deref(), Some("k2"));
        assert_eq!(source.bucket_url("b"), "s3://b");
    }

    #[test]
    fn configured_sources_join_detected_ones_and_replace_a_matching_id() {
        let (exists, read) = nothing_on_disk();
        let var = vars(&[
            ("AWS_ACCESS_KEY_ID", "env-key"),
            ("LAB_KEY", "k"),
            ("LAB_SECRET", "s"),
        ]);
        let env = Environment {
            var: &var,
            exists: &exists,
            read: &read,
            home: Some(PathBuf::from("/home/u")),
        };
        let config = CloudConfig {
            sources: vec![minio("lab", "http://127.0.0.1:9000")],
            ..Default::default()
        };
        let found = discover(&config, &env);
        let ids: Vec<&str> = found.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(ids, ["lab", DEFAULT_S3]);
        assert_eq!(found[0].bucket_url("data"), "s3://lab@data");
        assert_eq!(found[1].label, "Amazon S3");

        let replacing = CloudConfig {
            sources: vec![CloudSourceConfig {
                name: DEFAULT_S3.to_string(),
                label: Some("Work AWS".to_string()),
                kind: Some("s3".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let found = discover(&replacing, &env);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].label, "Work AWS");
        assert_eq!(found[0].tier, Tier::Config);
    }

    #[test]
    fn the_fingerprint_changes_with_the_endpoint() {
        let a = configured_source(&minio("lab", "http://127.0.0.1:9000"), &vars(&[]));
        let b = configured_source(&minio("lab", "http://127.0.0.1:9001"), &vars(&[]));
        assert_ne!(a.fingerprint(), b.fingerprint());
    }
}
