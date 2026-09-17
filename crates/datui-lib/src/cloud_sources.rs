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
/// A signed-in `az`.
pub const DEFAULT_AZURE_LOGIN: &str = "az";
/// An Azure account named in the environment: a connection string, or an account with a
/// key or SAS token.
pub const DEFAULT_AZURE_ENV: &str = "azure-env";

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
    /// Only meaningful for [`ProviderKind::Azure`].
    pub azure: crate::azure::AzureSettings,
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

    /// The URL of one of this source's buckets. For Azure, the first level is storage
    /// accounts, and an account has no URL of its own, so it is a home-screen place.
    pub fn bucket_url(&self, bucket: &str) -> String {
        if self.kind == ProviderKind::Azure {
            return format!("cloud://{}/{bucket}", self.id);
        }
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
            ProviderKind::S3 | ProviderKind::Azure => true,
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
            self.azure.account.as_deref().unwrap_or(""),
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
/// A configured source whose name matches a detected one replaces it. Nothing here
/// runs a command: credentials a profile gets from the AWS CLI are fetched when the
/// source is used ([`Source::with_credentials`]).
pub fn discover(config: &CloudConfig, env: &Environment<'_>) -> Vec<Source> {
    let profiles = crate::aws_profiles::load(env);
    let active = crate::aws_profiles::active_profile(env);
    let mut default_uses_profile = false;

    let mut sources: Vec<Source> = crate::cloud_browse::detect(config, env)
        .into_iter()
        .map(|provider| {
            let tier = match provider.note.as_str() {
                "datui config" => Tier::Config,
                "~/.aws" | "gcloud" => Tier::Tools,
                _ => Tier::Environment,
            };
            let mut source = Source {
                id: match provider.kind {
                    ProviderKind::S3 => DEFAULT_S3,
                    ProviderKind::Gcs => DEFAULT_GCS,
                    ProviderKind::Azure => DEFAULT_AZURE_LOGIN,
                }
                .to_string(),
                label: String::new(),
                kind: provider.kind,
                tier,
                origin: provider.note.clone(),
                s3: match provider.kind {
                    ProviderKind::S3 => S3Settings::from_config(config),
                    ProviderKind::Gcs | ProviderKind::Azure => S3Settings::default(),
                },
                project: provider.project,
                profile: None,
                buckets: Vec::new(),
                problem: None,
                azure: Default::default(),
            };
            // Found through a profile rather than keys: the active profile supplies the
            // keys, and its endpoint and region fill whatever the config and the
            // environment did not say.
            if provider.kind == ProviderKind::S3
                && matches!(provider.note.as_str(), "AWS_PROFILE" | "~/.aws")
            {
                default_uses_profile = true;
                source.profile = Some(active.clone());
                if let Some(profile) = profiles.iter().find(|p| p.name == active) {
                    fill_from_profile(&mut source.s3, profile, env.var);
                }
            }
            source.label = match source.kind {
                ProviderKind::S3 if source.s3.endpoint.is_none() => "Amazon S3".to_string(),
                ProviderKind::S3 => "S3-compatible".to_string(),
                ProviderKind::Gcs => "Google Cloud".to_string(),
                ProviderKind::Azure => "Azure".to_string(),
            };
            source
        })
        .collect();

    // Every other profile that can log in is a source of its own. The active one is
    // already the default source when that is how the default logs in.
    for profile in profiles.iter().filter(|p| p.has_credentials()) {
        if default_uses_profile && profile.name == active {
            continue;
        }
        let mut s3 = S3Settings::default();
        fill_from_profile(&mut s3, profile, env.var);
        sources.push(Source {
            id: profile_source_id(&profile.name),
            label: profile.name.clone(),
            kind: ProviderKind::S3,
            tier: Tier::Tools,
            origin: "aws profile".to_string(),
            s3,
            project: None,
            profile: Some(profile.name.clone()),
            buckets: Vec::new(),
            problem: None,
            azure: Default::default(),
        });
    }

    // Azure: an account named in the environment, and a signed-in `az`, which reaches
    // every account it can see.
    if let Some((settings, origin)) = crate::azure::from_environment(env.var) {
        sources.push(Source {
            id: DEFAULT_AZURE_ENV.to_string(),
            label: settings
                .account
                .clone()
                .unwrap_or_else(|| "Azure".to_string()),
            kind: ProviderKind::Azure,
            tier: Tier::Environment,
            origin,
            s3: S3Settings::default(),
            project: None,
            profile: None,
            buckets: Vec::new(),
            problem: None,
            azure: settings,
        });
    }
    if crate::azure::az_login_evidence(env) {
        sources.push(Source {
            id: DEFAULT_AZURE_LOGIN.to_string(),
            label: "Azure".to_string(),
            kind: ProviderKind::Azure,
            tier: Tier::Tools,
            origin: "az login".to_string(),
            s3: S3Settings::default(),
            project: None,
            profile: None,
            buckets: Vec::new(),
            problem: None,
            azure: crate::azure::AzureSettings {
                auth: crate::azure::AzureAuth::AzCli,
                ..Default::default()
            },
        });
    }

    for configured in &config.sources {
        let source = configured_source(configured, env);
        match sources.iter_mut().find(|s| s.id == source.id) {
            Some(existing) => *existing = source,
            None => sources.push(source),
        }
    }

    // Stable: rows must not move when a listing lands.
    sources.sort_by(|a, b| {
        a.tier
            .cmp(&b.tier)
            .then_with(|| a.label.to_lowercase().cmp(&b.label.to_lowercase()))
            .then_with(|| a.id.cmp(&b.id))
    });
    sources
}

/// The ID of the source for an AWS profile: `aws-` and the profile's name, lowercased,
/// with anything that cannot go in an ID turned into `-`.
pub fn profile_source_id(profile: &str) -> String {
    let slug: String = profile
        .chars()
        .map(|c| {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_lowercase() || c.is_ascii_digit() {
                c
            } else {
                '-'
            }
        })
        .collect();
    let mut id = format!("aws-{}", slug.trim_matches('-'));
    id.truncate(40);
    id
}

/// Take a profile's endpoint and region where `s3` does not already say.
fn fill_from_profile(
    s3: &mut S3Settings,
    profile: &crate::aws_profiles::Profile,
    var: &dyn Fn(&str) -> Option<String>,
) {
    if s3.endpoint.is_none() {
        s3.endpoint = profile.s3_endpoint(var);
    }
    if s3.region.is_none() {
        s3.region = profile.region.clone();
    }
}

impl Source {
    /// This source with the keys it signs with filled in from its AWS profile, when it
    /// logs in through one. Runs `credential_process` or the AWS CLI when the profile
    /// needs them, so call it on a worker.
    pub fn with_credentials(mut self, env: &Environment<'_>) -> Result<Source, String> {
        if let Some(problem) = &self.problem {
            return Err(problem.clone());
        }
        let Some(name) = self.profile.clone() else {
            return Ok(self);
        };
        if self.s3.access_key_id.is_some() {
            return Ok(self);
        }
        let profiles = crate::aws_profiles::load(env);
        let profile = profiles
            .iter()
            .find(|p| p.name == name)
            .ok_or_else(|| format!("profile {name} is not in the AWS config"))?;
        let credentials = crate::aws_profiles::credentials(profile, env)?;
        self.s3.access_key_id = Some(credentials.access_key_id);
        self.s3.secret_access_key = Some(credentials.secret_access_key);
        self.s3.session_token = credentials.session_token;
        // The keys are the profile's now; the shell's AWS_* variables must not add a
        // session token or region from some other login.
        self.s3.from_env = false;
        Ok(self)
    }
}

/// A `[[cloud.sources]]` entry as a source. The config has been validated, so the kind
/// is one datui knows.
fn configured_source(configured: &CloudSourceConfig, env: &Environment<'_>) -> Source {
    let var = env.var;
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
    let mut s3 = S3Settings {
        endpoint: configured.endpoint_url.clone(),
        access_key_id: from_named(&configured.access_key_id_env),
        secret_access_key: from_named(&configured.secret_access_key_env),
        session_token: from_named(&configured.session_token_env),
        region: configured.region.clone(),
        virtual_hosted: configured.addressing.as_deref().map(|a| a == "virtual"),
        from_env: false,
    };
    if let Some(name) = &configured.profile {
        match crate::aws_profiles::load(env)
            .iter()
            .find(|p| &p.name == name)
        {
            Some(profile) => fill_from_profile(&mut s3, profile, var),
            None => {
                problem.get_or_insert_with(|| format!("profile {name} is not in the AWS config"));
            }
        }
    }
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
        profile: configured.profile.clone(),
        buckets: configured.buckets.clone(),
        problem,
        azure: Default::default(),
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
    /// For an Azure URL, the settings with a token in place of `az`.
    pub azure: crate::azure::AzureSettings,
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

/// Resolve `url` against the sources in `config` and on this machine. May run a
/// credential command, so call it on a worker.
pub fn resolve(url: &str, config: &CloudConfig) -> Result<Resolved, String> {
    resolve_with(url, config, &Environment::current())
}

/// As [`resolve`], with the environment supplied.
pub fn resolve_with(
    url: &str,
    config: &CloudConfig,
    env: &Environment<'_>,
) -> Result<Resolved, String> {
    if let Some((account, container, path)) = crate::source::azure_parts(url) {
        return resolve_azure(&account, &container, &path, config, env);
    }
    let (id, plain) = crate::source::split_source_id(url);
    let (kind, bucket, _) = crate::cloud_browse::split_bucket_url(&plain)
        .ok_or_else(|| format!("not an object-store URL: {url}"))?;
    let find = |id: &str| discover(config, env).into_iter().find(|s| s.id == id);

    let source = match id {
        Some(id) => {
            let Some(source) = find(id) else {
                return Err(unknown_source(id, config, env));
            };
            if !source.named_in_urls() {
                return Err(format!(
                    "\"{id}\" has no endpoint, so it is not S3-compatible and its URLs are \
                     plain s3://bucket/key"
                ));
            }
            source
        }
        None => match remembered(kind, &bucket)
            .and_then(|id| find(&id))
            .filter(|s| s.kind == kind)
        {
            Some(source) => source,
            None => {
                let default_id = match kind {
                    ProviderKind::S3 => DEFAULT_S3,
                    ProviderKind::Gcs => DEFAULT_GCS,
                    ProviderKind::Azure => DEFAULT_AZURE_LOGIN,
                };
                // The default source as discovered, when it was: that is what carries
                // the active profile. Otherwise the settings as they have always been.
                find(default_id).unwrap_or_else(|| Source {
                    id: default_id.to_string(),
                    label: String::new(),
                    kind,
                    tier: Tier::Environment,
                    origin: String::new(),
                    s3: match kind {
                        ProviderKind::S3 => S3Settings::from_config(config),
                        ProviderKind::Gcs | ProviderKind::Azure => S3Settings::default(),
                    },
                    project: None,
                    profile: None,
                    buckets: Vec::new(),
                    problem: None,
                    azure: Default::default(),
                })
            }
        },
    };

    let id = source.id.clone();
    let source = source
        .with_credentials(env)
        .map_err(|e| format!("source \"{id}\": {e}"))?;
    Ok(Resolved {
        url: plain.into_owned(),
        kind,
        source_id: source.id,
        s3: source.s3,
        azure: Default::default(),
    })
}

/// An Azure URL: the account named in the environment when it is this one, else a
/// signed-in `az`, else no signature at all, which is how public containers are read.
fn resolve_azure(
    account: &str,
    container: &str,
    path: &str,
    config: &CloudConfig,
    env: &Environment<'_>,
) -> Result<Resolved, String> {
    let sources = discover(config, env);
    let named = sources
        .iter()
        .find(|s| s.kind == ProviderKind::Azure && s.azure.account.as_deref() == Some(account));
    let login = sources.iter().find(|s| s.id == DEFAULT_AZURE_LOGIN);
    let (source_id, settings) = match named.or(login) {
        Some(source) => (
            source.id.clone(),
            source
                .azure
                .clone()
                .with_token(env)
                .map_err(|e| format!("source \"{}\": {e}", source.id))?,
        ),
        None => (String::new(), crate::azure::AzureSettings::default()),
    };
    Ok(Resolved {
        url: crate::source::azure_url(account, container, path),
        kind: ProviderKind::Azure,
        source_id,
        s3: S3Settings::default(),
        azure: settings,
    })
}

fn unknown_source(id: &str, config: &CloudConfig, env: &Environment<'_>) -> String {
    let names: Vec<String> = discover(config, env)
        .into_iter()
        .filter(Source::named_in_urls)
        .map(|s| s.id)
        .collect();
    if names.is_empty() {
        format!("no S3-compatible source is named \"{id}\"")
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
    use crate::cloud_command::CommandError;
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

    /// A machine described by literals: environment variables, the text of files by
    /// path, and a runner that fails as if nothing were installed.
    struct Machine {
        vars: HashMap<String, String>,
        files: HashMap<PathBuf, String>,
    }

    impl Machine {
        fn new(vars: &[(&str, &str)], files: &[(&str, &str)]) -> Self {
            Machine {
                vars: vars
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                files: files
                    .iter()
                    .map(|(p, t)| (PathBuf::from(p), t.to_string()))
                    .collect(),
            }
        }
    }

    fn with_machine<T>(machine: &Machine, body: impl FnOnce(&Environment<'_>) -> T) -> T {
        let var = |key: &str| machine.vars.get(key).cloned();
        let exists = |path: &Path| machine.files.contains_key(path);
        let read = |path: &Path| machine.files.get(path).cloned();
        let run = |program: &str, _: &[&str]| Err(CommandError::Missing(program.to_string()));
        let env = Environment {
            var: &var,
            exists: &exists,
            read: &read,
            home: Some(PathBuf::from("/home/u")),
            windows: false,
            run: &run,
        };
        body(&env)
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
        let machine = Machine::new(
            &[
                ("LAB_KEY", "lab-key"),
                ("LAB_SECRET", "lab-secret"),
                ("ONPREM_KEY", "corp-key"),
                ("ONPREM_SECRET", "corp-secret"),
            ],
            &[],
        );
        with_machine(&machine, |env| {
            let lab = resolve_with("s3://lab@data/sales.parquet", &config, env).unwrap();
            let corp = resolve_with("s3://onprem@data/sales.parquet", &config, env).unwrap();
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
        });
    }

    #[test]
    fn a_plain_url_is_the_default_source_as_before() {
        let config = CloudConfig {
            s3_endpoint_url: Some("http://localhost:9000".to_string()),
            sources: vec![minio("lab", "http://127.0.0.1:9000")],
            ..Default::default()
        };
        with_machine(&Machine::new(&[], &[]), |env| {
            let resolved = resolve_with("s3://data/key.parquet", &config, env).unwrap();
            assert_eq!(resolved.source_id, DEFAULT_S3);
            assert_eq!(resolved.url, "s3://data/key.parquet");
            assert_eq!(
                resolved.s3.endpoint.as_deref(),
                Some("http://localhost:9000")
            );
            assert!(resolved.s3.from_env);

            let gcs = resolve_with("gs://bucket/key", &config, env).unwrap();
            assert_eq!(gcs.source_id, DEFAULT_GCS);
        });
    }

    #[test]
    fn an_unknown_source_names_the_ones_that_exist() {
        let config = CloudConfig {
            sources: vec![minio("lab", "http://127.0.0.1:9000")],
            ..Default::default()
        };
        with_machine(&Machine::new(&[], &[]), |env| {
            let err = resolve_with("s3://nope@data/key", &config, env).unwrap_err();
            assert!(err.contains("\"nope\"") && err.contains("lab"), "{err}");
        });
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
        with_machine(&Machine::new(&[], &[]), |env| {
            let err = resolve_with("s3://second-account@data/key", &config, env).unwrap_err();
            assert!(err.contains("not S3-compatible"), "{err}");
        });
    }

    #[test]
    fn a_named_variable_that_is_unset_is_reported_not_borrowed() {
        let config = CloudConfig {
            sources: vec![minio("lab", "http://127.0.0.1:9000")],
            ..Default::default()
        };
        with_machine(&Machine::new(&[("LAB_KEY", "k")], &[]), |env| {
            let err = resolve_with("s3://lab@data/key", &config, env).unwrap_err();
            assert!(err.contains("LAB_SECRET is not set"), "{err}");
        });
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
        let machine = Machine::new(&[("SECOND_KEY", "k2"), ("SECOND_SECRET", "s2")], &[]);
        with_machine(&machine, |env| {
            let source = configured_source(&config.sources[0], env);
            remember_bucket(&source, "only-in-second-account");
            let resolved =
                resolve_with("s3://only-in-second-account/x.parquet", &config, env).unwrap();
            assert_eq!(resolved.source_id, "second-account");
            assert_eq!(resolved.s3.access_key_id.as_deref(), Some("k2"));
            assert_eq!(source.bucket_url("b"), "s3://b");
        });
    }

    #[test]
    fn configured_sources_join_detected_ones_and_replace_a_matching_id() {
        let machine = Machine::new(
            &[
                ("AWS_ACCESS_KEY_ID", "env-key"),
                ("LAB_KEY", "k"),
                ("LAB_SECRET", "s"),
            ],
            &[],
        );
        with_machine(&machine, |env| {
            let config = CloudConfig {
                sources: vec![minio("lab", "http://127.0.0.1:9000")],
                ..Default::default()
            };
            let found = discover(&config, env);
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
            let found = discover(&replacing, env);
            assert_eq!(found.len(), 1);
            assert_eq!(found[0].label, "Work AWS");
            assert_eq!(found[0].tier, Tier::Config);
        });
    }

    #[test]
    fn the_fingerprint_changes_with_the_endpoint() {
        with_machine(&Machine::new(&[], &[]), |env| {
            let a = configured_source(&minio("lab", "http://127.0.0.1:9000"), env);
            let b = configured_source(&minio("lab", "http://127.0.0.1:9001"), env);
            assert_ne!(a.fingerprint(), b.fingerprint());
        });
    }

    const AWS_CONFIG: &str = "
[default]
region = us-east-1

[profile work]
region = eu-west-1

[profile lab]
endpoint_url = http://localhost:9000

[profile regional-only]
region = ap-south-1
";

    const AWS_CREDENTIALS: &str = "
[default]
aws_access_key_id = AKIADEFAULT
aws_secret_access_key = default-secret

[work]
aws_access_key_id = AKIAWORK
aws_secret_access_key = work-secret

[lab]
aws_access_key_id = minioadmin
aws_secret_access_key = minioadmin
";

    fn aws_machine(vars: &[(&str, &str)]) -> Machine {
        Machine::new(
            vars,
            &[
                ("/home/u/.aws/config", AWS_CONFIG),
                ("/home/u/.aws/credentials", AWS_CREDENTIALS),
            ],
        )
    }

    /// The bug #174 fixes: with `AWS_PROFILE=work`, the keys that sign are work's, not
    /// the first ones in the credentials file.
    #[test]
    fn the_default_source_signs_with_the_active_profile() {
        with_machine(&aws_machine(&[("AWS_PROFILE", "work")]), |env| {
            let config = CloudConfig::from_env(env.var);
            let resolved = resolve_with("s3://bucket/key.parquet", &config, env).unwrap();
            assert_eq!(resolved.source_id, DEFAULT_S3);
            assert_eq!(resolved.s3.access_key_id.as_deref(), Some("AKIAWORK"));
            assert_eq!(
                resolved.s3.secret_access_key.as_deref(),
                Some("work-secret")
            );
            assert_eq!(resolved.s3.region.as_deref(), Some("eu-west-1"));
            assert!(!resolved.s3.from_env);
        });
        with_machine(&aws_machine(&[]), |env| {
            let config = CloudConfig::from_env(env.var);
            let resolved = resolve_with("s3://bucket/key.parquet", &config, env).unwrap();
            assert_eq!(resolved.s3.access_key_id.as_deref(), Some("AKIADEFAULT"));
        });
    }

    #[test]
    fn keys_in_the_environment_still_beat_a_profile() {
        let machine = aws_machine(&[
            ("AWS_PROFILE", "work"),
            ("AWS_ACCESS_KEY_ID", "AKIAENV"),
            ("AWS_SECRET_ACCESS_KEY", "env-secret"),
        ]);
        with_machine(&machine, |env| {
            let config = CloudConfig::from_env(env.var);
            let resolved = resolve_with("s3://bucket/key", &config, env).unwrap();
            assert_eq!(resolved.s3.access_key_id.as_deref(), Some("AKIAENV"));
            let ids: Vec<String> = discover(&config, env).into_iter().map(|s| s.id).collect();
            assert!(ids.contains(&"aws-work".to_string()), "{ids:?}");
        });
    }

    #[test]
    fn every_other_profile_that_can_log_in_is_a_source() {
        with_machine(&aws_machine(&[("AWS_PROFILE", "work")]), |env| {
            let config = CloudConfig::from_env(env.var);
            let found = discover(&config, env);
            let ids: Vec<&str> = found.iter().map(|s| s.id.as_str()).collect();
            // The active profile is the default source; a profile with only a region
            // cannot log in and is not listed.
            assert_eq!(ids, [DEFAULT_S3, "aws-default", "aws-lab"]);
            let lab = found.iter().find(|s| s.id == "aws-lab").unwrap();
            assert!(
                lab.named_in_urls(),
                "a profile with an endpoint is S3-compatible"
            );
            assert_eq!(lab.origin, "aws profile");

            let resolved = resolve_with("s3://aws-lab@data/x.parquet", &config, env).unwrap();
            assert_eq!(
                resolved.s3.endpoint.as_deref(),
                Some("http://localhost:9000")
            );
            assert_eq!(resolved.s3.access_key_id.as_deref(), Some("minioadmin"));
        });
    }

    #[test]
    fn a_configured_source_can_log_in_through_a_profile() {
        let config = CloudConfig {
            sources: vec![CloudSourceConfig {
                name: "minio".to_string(),
                kind: Some("s3".to_string()),
                profile: Some("lab".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };
        with_machine(&aws_machine(&[]), |env| {
            let resolved = resolve_with("s3://minio@data/x", &config, env).unwrap();
            assert_eq!(
                resolved.s3.endpoint.as_deref(),
                Some("http://localhost:9000")
            );
            assert_eq!(resolved.s3.access_key_id.as_deref(), Some("minioadmin"));
        });
        let missing = CloudConfig {
            sources: vec![CloudSourceConfig {
                name: "minio".to_string(),
                kind: Some("s3".to_string()),
                endpoint_url: Some("http://x".to_string()),
                profile: Some("nope".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };
        with_machine(&aws_machine(&[]), |env| {
            let err = resolve_with("s3://minio@data/x", &missing, env).unwrap_err();
            assert!(
                err.contains("profile nope is not in the AWS config"),
                "{err}"
            );
        });
    }

    #[test]
    fn profile_ids_are_valid_source_ids() {
        assert_eq!(profile_source_id("Prod_Admin.RO"), "aws-prod-admin-ro");
        assert!(crate::config::is_valid_source_id(&profile_source_id(
            "a very long profile name that goes on and on"
        )));
    }
}
