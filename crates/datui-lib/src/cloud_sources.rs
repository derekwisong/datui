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
/// The built-in `Public datasets` source.
pub const PUBLIC: &str = "public";

/// Data anyone can read: a bucket, container or folder, with what the details pane says
/// about it.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(default)]
pub struct Dataset {
    pub name: String,
    pub url: String,
    pub description: String,
    pub publisher: String,
    pub license: String,
    pub homepage: String,
}

/// The datasets of the built-in source, from `public_datasets.toml`.
pub fn builtin_datasets() -> Vec<Dataset> {
    #[derive(serde::Deserialize)]
    struct File {
        dataset: Vec<Dataset>,
    }
    toml::from_str::<File>(include_str!("public_datasets.toml"))
        .map(|file| file.dataset)
        .unwrap_or_default()
}

/// A dataset for a URL someone named or opened, with nothing known about it but where
/// it is: named by its bucket or container and the path inside.
pub fn dataset_for_url(url: &str) -> Dataset {
    let name = match crate::source::azure_parts(url) {
        Some((account, container, path)) => {
            format!("{account}/{container}/{}", path.trim_matches('/'))
        }
        None => url
            .split_once("://")
            .map_or(url, |(_, rest)| rest)
            .to_string(),
    };
    Dataset {
        name: name.trim_end_matches('/').to_string(),
        url: url.to_string(),
        ..Default::default()
    }
}

/// Whether `url` is `root` or somewhere inside it. Azure URLs are compared in their
/// canonical form, and a trailing slash does not matter.
pub fn is_within(url: &str, root: &str) -> bool {
    let canonical = |u: &str| match crate::source::azure_parts(u) {
        Some((account, container, path)) => crate::source::azure_url(&account, &container, &path),
        None => u.to_string(),
    };
    let (url, root) = (canonical(url), canonical(root));
    let (url, root) = (url.trim_end_matches('/'), root.trim_end_matches('/'));
    url == root
        || url
            .strip_prefix(root)
            .is_some_and(|rest| rest.starts_with('/'))
}

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
    /// Shipped with datui: the public datasets.
    BuiltIn,
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
    /// Send requests with no signature at all, as public data is read.
    pub skip_signature: bool,
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
            skip_signature: false,
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
    /// Public data, read with no login. Its first level is `datasets`, which may be on
    /// any provider; `kind` means nothing for it.
    pub public: bool,
    pub datasets: Vec<Dataset>,
    /// The `gcloud` configuration whose token a Google source signs with. `None` is
    /// object_store's own login: the environment or the application-default file.
    pub gcloud: Option<String>,
    /// A program that prints an S3 source's secret access key.
    pub secret_command: Option<String>,
    /// The Google service account or application-default file a source logs in with.
    pub google_credentials: Option<std::path::PathBuf>,
}

impl Source {
    /// Whether URLs from this source carry its ID. Only an S3-compatible server other
    /// than the default one needs to: its buckets are named only within its endpoint.
    pub fn named_in_urls(&self) -> bool {
        !self.public
            && self.kind == ProviderKind::S3
            && self.id != DEFAULT_S3
            && self.s3.endpoint.is_some()
    }

    /// The URL of one of this source's buckets. For Azure, the first level is storage
    /// accounts, and an account has no URL of its own, so it is a home-screen place.
    pub fn bucket_url(&self, bucket: &str) -> String {
        // A public source's datasets are URLs already.
        if self.public {
            return bucket.to_string();
        }
        // Azure's first level is storage accounts and Google's is projects; neither
        // has a URL of its own.
        if matches!(self.kind, ProviderKind::Azure | ProviderKind::Gcs) {
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

    /// What this source points at. Anything cached under the source's ID is stale
    /// once this changes: an endpoint moved to another server lists other buckets.
    pub fn fingerprint(&self) -> String {
        [
            // Google's first level became projects; a listing of buckets from before
            // is not one of projects.
            if self.kind == ProviderKind::Gcs {
                "gs-projects"
            } else {
                self.kind.scheme()
            },
            self.gcloud.as_deref().unwrap_or(""),
            self.s3.endpoint.as_deref().unwrap_or(""),
            self.s3.access_key_id.as_deref().unwrap_or(""),
            self.project.as_deref().unwrap_or(""),
            self.profile.as_deref().unwrap_or(""),
            self.azure.account.as_deref().unwrap_or(""),
            &self
                .datasets
                .iter()
                .map(|d| d.url.as_str())
                .collect::<Vec<_>>()
                .join(","),
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
                public: false,
                datasets: Vec::new(),
                gcloud: None,
                secret_command: None,
                google_credentials: None,
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

    // A credentials file named in the environment is passed along by path, so a value
    // from `[cloud] env_files`, which object_store cannot see, reaches it too. Keys from
    // the environment bring their session token the same way.
    if let Some(google) = sources.iter_mut().find(|s| s.id == DEFAULT_GCS)
        && let Some(path) = (env.var)("GOOGLE_APPLICATION_CREDENTIALS")
    {
        google.google_credentials = Some(std::path::PathBuf::from(path));
    }
    if let Some(s3) = sources.iter_mut().find(|s| s.id == DEFAULT_S3)
        && s3.s3.access_key_id.is_some()
        && s3.s3.access_key_id == (env.var)("AWS_ACCESS_KEY_ID")
    {
        s3.s3.session_token = (env.var)("AWS_SESSION_TOKEN");
    }

    // Google through `gcloud`: the active configuration is the default login when
    // object_store has none of its own, or one it cannot read; every configuration
    // with another account is a source of its own.
    let configurations = crate::gcloud::configurations(env);
    let active_name = crate::gcloud::active_name(env);
    let active_configuration = configurations
        .iter()
        .find(|c| c.name == active_name && c.account.is_some());
    match sources.iter_mut().find(|s| s.id == DEFAULT_GCS) {
        Some(default) => {
            if let Some(kind) = crate::cloud_browse::unreadable_google_login(env) {
                match active_configuration {
                    Some(configuration) => {
                        default.gcloud = Some(configuration.name.clone());
                        default.origin = "gcloud".to_string();
                    }
                    None => default.problem = Some(format!("unsupported login: {kind}")),
                }
            }
            if default.project.is_none() {
                default.project = active_configuration.and_then(|c| c.project.clone());
            }
        }
        None => {
            if let Some(configuration) = active_configuration {
                sources.push(Source {
                    id: DEFAULT_GCS.to_string(),
                    label: "Google Cloud".to_string(),
                    kind: ProviderKind::Gcs,
                    tier: Tier::Tools,
                    origin: "gcloud".to_string(),
                    s3: S3Settings::default(),
                    azure: Default::default(),
                    project: crate::cloud_browse::gcp_project(env)
                        .or_else(|| configuration.project.clone()),
                    profile: None,
                    buckets: Vec::new(),
                    problem: None,
                    public: false,
                    datasets: Vec::new(),
                    gcloud: Some(configuration.name.clone()),
                    secret_command: None,
                    google_credentials: None,
                });
            }
        }
    }
    let default_account = active_configuration.and_then(|c| c.account.clone());
    let mut accounts_seen: Vec<String> = default_account.into_iter().collect();
    for configuration in &configurations {
        let Some(account) = &configuration.account else {
            continue;
        };
        if accounts_seen.contains(account) {
            continue;
        }
        accounts_seen.push(account.clone());
        sources.push(Source {
            id: slug_id("gcloud", &configuration.name),
            label: configuration.name.clone(),
            kind: ProviderKind::Gcs,
            tier: Tier::Tools,
            origin: "gcloud configuration".to_string(),
            s3: S3Settings::default(),
            azure: Default::default(),
            project: configuration.project.clone(),
            profile: None,
            buckets: Vec::new(),
            problem: None,
            public: false,
            datasets: Vec::new(),
            gcloud: Some(configuration.name.clone()),
            secret_command: None,
            google_credentials: None,
        });
    }

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
            public: false,
            datasets: Vec::new(),
            gcloud: None,
            secret_command: None,
            google_credentials: None,
            azure: Default::default(),
        });
    }

    // S3-compatible servers other tools describe. An `MC_HOST_<alias>` in the
    // environment replaces the alias of the same name in `mc`'s config, as it does
    // for `mc` itself.
    let mut tool_sources: Vec<Source> = Vec::new();
    for path in crate::s3_tools::mc_config_paths(env) {
        if let Some(text) = (env.read)(&path) {
            for server in crate::s3_tools::parse_mc_config(&text) {
                tool_sources.push(tool_source(server, Tier::Tools));
            }
        }
    }
    for server in crate::s3_tools::mc_hosts(&(env.all_vars)()) {
        let source = tool_source(server, Tier::Environment);
        tool_sources.retain(|s| s.id != source.id);
        tool_sources.push(source);
    }
    if let Some(server) = crate::s3_tools::s3cfg_path(env)
        .and_then(|path| (env.read)(&path))
        .and_then(|text| crate::s3_tools::parse_s3cfg(&text))
    {
        tool_sources.push(tool_source(server, Tier::Tools));
    }
    for source in tool_sources {
        if !sources.iter().any(|s| s.id == source.id) {
            sources.push(source);
        }
    }

    // Azure: an account or a service principal named in the environment, and a
    // signed-in `az` or Azure PowerShell, which reaches every account it can see.
    let from_environment = crate::azure::from_environment(env.var).or_else(|| {
        crate::cloud_browse::instance_identity(config, env)
            .azure
            .then(|| {
                (
                    crate::azure::AzureSettings {
                        account: (env.var)("AZURE_STORAGE_ACCOUNT_NAME"),
                        auth: crate::azure::AzureAuth::ManagedIdentity,
                        ..Default::default()
                    },
                    "managed identity".to_string(),
                )
            })
    });
    if let Some((settings, origin)) = from_environment {
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
            public: false,
            datasets: Vec::new(),
            gcloud: None,
            secret_command: None,
            google_credentials: None,
            azure: settings,
        });
    }
    let az = crate::azure::az_login_evidence(env);
    let powershell = crate::azure::powershell_login_evidence(env);
    let not_signed_in = crate::azure::not_signed_in(env);
    if az || powershell || not_signed_in.is_some() {
        let auth = if az || !powershell {
            crate::azure::AzureAuth::AzCli
        } else {
            crate::azure::AzureAuth::PowerShell
        };
        sources.push(Source {
            id: DEFAULT_AZURE_LOGIN.to_string(),
            label: "Azure".to_string(),
            kind: ProviderKind::Azure,
            tier: Tier::Tools,
            origin: if not_signed_in.is_some() {
                "not signed in".to_string()
            } else {
                auth.describe().to_string()
            },
            s3: S3Settings::default(),
            project: None,
            profile: None,
            buckets: Vec::new(),
            problem: not_signed_in,
            public: false,
            datasets: Vec::new(),
            gcloud: None,
            secret_command: None,
            google_credentials: None,
            azure: crate::azure::AzureSettings {
                auth,
                ..Default::default()
            },
        });
    }

    if config.public_datasets != Some(false) {
        sources.push(Source {
            id: PUBLIC.to_string(),
            label: "Public datasets".to_string(),
            kind: ProviderKind::S3,
            tier: Tier::BuiltIn,
            origin: "built in".to_string(),
            s3: S3Settings::default(),
            azure: Default::default(),
            project: None,
            profile: None,
            buckets: Vec::new(),
            problem: None,
            public: true,
            datasets: builtin_datasets(),
            gcloud: None,
            secret_command: None,
            google_credentials: None,
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

    // The same server with the same key is one source, however many tools describe it:
    // the one from the highest tier stays, and its note says where else it was found.
    let mut kept: Vec<Source> = Vec::new();
    for source in sources {
        let same_as = kept.iter_mut().find(|k| {
            k.kind == ProviderKind::S3
                && source.kind == ProviderKind::S3
                && k.s3.access_key_id.is_some()
                && k.s3.access_key_id == source.s3.access_key_id
                && normalized_endpoint(&k.s3) == normalized_endpoint(&source.s3)
        });
        match same_as {
            Some(existing) => {
                if !existing.origin.contains(&source.origin) {
                    existing.origin = format!("{}, {}", existing.origin, source.origin);
                }
            }
            None => kept.push(source),
        }
    }
    kept
}

fn normalized_endpoint(s3: &S3Settings) -> String {
    s3.endpoint
        .as_deref()
        .unwrap_or("")
        .trim_end_matches('/')
        .to_ascii_lowercase()
}

/// A server another tool describes, as a source: `mc-<alias>`, or `s3cfg`.
fn tool_source(server: crate::s3_tools::ToolServer, tier: Tier) -> Source {
    let id = if server.origin == "s3cmd" {
        "s3cfg".to_string()
    } else {
        slug_id("mc", &server.name)
    };
    let label = if server.origin == "s3cmd" {
        server
            .endpoint
            .as_deref()
            .and_then(endpoint_host)
            .unwrap_or_else(|| "s3cmd".to_string())
    } else {
        server.name.clone()
    };
    Source {
        id,
        label,
        kind: ProviderKind::S3,
        tier,
        origin: server.origin,
        s3: S3Settings {
            endpoint: server.endpoint,
            access_key_id: Some(server.access_key_id),
            secret_access_key: Some(server.secret_access_key),
            session_token: server.session_token,
            region: server.region,
            virtual_hosted: server.virtual_hosted,
            from_env: false,
            skip_signature: false,
        },
        project: None,
        profile: None,
        buckets: Vec::new(),
        problem: None,
        public: false,
        datasets: Vec::new(),
        gcloud: None,
        secret_command: None,
        google_credentials: None,
        azure: Default::default(),
    }
}

/// The ID of the source for an AWS profile: `aws-` and the profile's name, lowercased,
/// with anything that cannot go in an ID turned into `-`.
pub fn profile_source_id(profile: &str) -> String {
    slug_id("aws", profile)
}

/// `<prefix>-<name>`, lowercased, with anything that cannot go in an ID turned into `-`.
fn slug_id(prefix: &str, name: &str) -> String {
    let slug: String = name
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
    let mut id = format!("{prefix}-{}", slug.trim_matches('-'));
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
        if let Some(command) = &self.secret_command
            && self.s3.secret_access_key.is_none()
        {
            self.s3.secret_access_key = Some(crate::cloud_command::secret(command, env)?);
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
    if configured.kind.as_deref() == Some("azure") {
        return configured_azure_source(configured, env);
    }
    if configured.kind.as_deref() == Some("gcs") {
        let google_credentials = configured
            .credentials_file
            .as_deref()
            .map(|file| expand_home(file, env));
        let problem = google_credentials
            .as_ref()
            .filter(|path| !(env.exists)(path))
            .map(|path| format!("credentials_file {} does not exist", path.display()));
        let file_project = google_credentials
            .as_ref()
            .and_then(|path| (env.read)(path))
            .and_then(|text| google_file_project(&text));
        return Source {
            id: configured.name.clone(),
            label: configured
                .label
                .clone()
                .unwrap_or_else(|| configured.name.clone()),
            kind: ProviderKind::Gcs,
            tier: Tier::Config,
            origin: "datui config".to_string(),
            s3: S3Settings::default(),
            azure: Default::default(),
            project: configured
                .project
                .clone()
                .or(file_project)
                .or_else(|| crate::cloud_browse::gcp_project(env)),
            profile: None,
            buckets: configured.buckets.clone(),
            problem,
            public: false,
            datasets: Vec::new(),
            gcloud: configured.configuration.clone(),
            secret_command: None,
            google_credentials,
        };
    }
    if configured.public == Some(true) {
        return Source {
            id: configured.name.clone(),
            label: configured
                .label
                .clone()
                .unwrap_or_else(|| configured.name.clone()),
            kind: ProviderKind::S3,
            tier: Tier::Config,
            origin: "datui config".to_string(),
            s3: S3Settings::default(),
            azure: Default::default(),
            project: None,
            profile: None,
            buckets: Vec::new(),
            problem: None,
            public: true,
            datasets: configured
                .buckets
                .iter()
                .map(|url| dataset_for_url(url))
                .collect(),
            gcloud: None,
            secret_command: None,
            google_credentials: None,
        };
    }
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
        skip_signature: false,
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
        public: false,
        datasets: Vec::new(),
        gcloud: None,
        secret_command: configured.secret_command.clone(),
        google_credentials: None,
    }
}

/// `~/x` under the home directory; anything else as written.
fn expand_home(file: &str, env: &Environment<'_>) -> std::path::PathBuf {
    match (
        file.strip_prefix("~/").or_else(|| file.strip_prefix("~\\")),
        &env.home,
    ) {
        (Some(rest), Some(home)) => home.join(rest),
        _ => std::path::PathBuf::from(file),
    }
}

/// The project a Google credentials file names: a service account's `project_id`, or
/// an application-default login's `quota_project_id`.
fn google_file_project(text: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    ["project_id", "quota_project_id"]
        .iter()
        .find_map(|key| value.get(*key)?.as_str().map(str::to_string))
        .filter(|p| !p.is_empty())
}

/// A `kind = "azure"` source: one account, signed in with the named key, SAS or
/// connection string, or else through `az` or Azure PowerShell.
fn configured_azure_source(configured: &CloudSourceConfig, env: &Environment<'_>) -> Source {
    use crate::azure::{AzureAuth, AzureSettings};
    let named = |name: &Option<String>| -> Option<Result<String, String>> {
        let name = name.as_deref()?;
        Some(
            (env.var)(name)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .ok_or_else(|| format!("{name} is not set")),
        )
    };
    let mut problem = None;
    let mut settings = AzureSettings {
        account: configured.account.clone(),
        auth: AzureAuth::AzCli,
        ..Default::default()
    };
    if let Some(key) = named(&configured.account_key_env) {
        match key {
            Ok(key) => settings.auth = AzureAuth::Key(key),
            Err(e) => problem = Some(e),
        }
    } else if let Some(sas) = named(&configured.sas_env) {
        match sas {
            Ok(sas) => settings.auth = AzureAuth::Sas(sas.trim_start_matches('?').to_string()),
            Err(e) => problem = Some(e),
        }
    } else if let Some(text) = named(&configured.connection_string_env) {
        match text.map(|t| crate::azure::parse_connection_string(&t)) {
            Ok(Some(parsed)) => {
                settings = AzureSettings {
                    account: configured.account.clone().or(parsed.account.clone()),
                    ..parsed
                }
            }
            Ok(None) => {
                problem = Some("the connection string names no account and key or SAS".to_string())
            }
            Err(e) => problem = Some(e),
        }
    } else if let Some(command) = &configured.secret_command {
        settings.auth = AzureAuth::KeyCommand(command.clone());
    } else if !crate::azure::az_login_evidence(env) && crate::azure::powershell_login_evidence(env)
    {
        settings.auth = AzureAuth::PowerShell;
    }
    Source {
        id: configured.name.clone(),
        label: configured
            .label
            .clone()
            .unwrap_or_else(|| configured.name.clone()),
        kind: ProviderKind::Azure,
        tier: Tier::Config,
        origin: "datui config".to_string(),
        s3: S3Settings::default(),
        azure: settings,
        project: None,
        profile: None,
        buckets: Vec::new(),
        problem,
        public: false,
        datasets: Vec::new(),
        gcloud: None,
        secret_command: None,
        google_credentials: None,
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
    pub signing: Signing,
    /// The bucket or container, as [`access_key`] names it.
    pub place: String,
    /// For a Google URL signed through `gcloud`: the configuration, and its token.
    pub gcloud: Option<(String, String)>,
    /// For a Google URL signed with a credentials file.
    pub google_credentials: Option<std::path::PathBuf>,
}

/// Whether requests to a place carry a signature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signing {
    Signed,
    /// No signature: public data, or no login for this provider at all.
    Unsigned,
    /// Signed, by a login that may have nothing to do with this place. A refusal is
    /// tried again with no signature, since the place may be public: Azure refuses a
    /// public container to a token from another tenant.
    Try,
}

impl Resolved {
    /// The same place, read with no signature.
    pub fn unsigned(mut self) -> Self {
        self.s3 = S3Settings {
            endpoint: self.s3.endpoint.take(),
            region: self.s3.region.take(),
            virtual_hosted: self.s3.virtual_hosted,
            skip_signature: true,
            ..Default::default()
        };
        self.azure.auth = crate::azure::AzureAuth::None;
        self.gcloud = None;
        self.google_credentials = None;
        self.signing = Signing::Unsigned;
        self
    }
}

/// The bucket or container `url` is in, as one string: `s3://bucket`, `s3://<id>@bucket`,
/// `gs://bucket`, `abfss://container@account`. What is learned about signing is kept
/// per place.
pub fn access_key(url: &str) -> Option<String> {
    if let Some((account, container, _)) = crate::source::azure_parts(url) {
        return Some(format!("abfss://{container}@{account}"));
    }
    let (id, _) = crate::source::split_source_id(url);
    let (kind, bucket, _) = crate::cloud_browse::split_bucket_url(url)?;
    Some(match id {
        Some(id) => format!("{}://{id}@{bucket}", kind.scheme()),
        None => format!("{}://{bucket}", kind.scheme()),
    })
}

fn access() -> &'static Mutex<HashMap<String, bool>> {
    static MAP: OnceLock<Mutex<HashMap<String, bool>>> = OnceLock::new();
    MAP.get_or_init(Default::default)
}

/// Remember for the session whether `place` (an [`access_key`]) is read unsigned.
pub fn remember_access(place: &str, unsigned: bool) {
    if let Ok(mut map) = access().lock() {
        map.insert(place.to_string(), unsigned);
    }
    if unsigned {
        found_public(place);
    }
}

fn public_places() -> &'static Mutex<Vec<String>> {
    static PLACES: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
    PLACES.get_or_init(Default::default)
}

/// Note that the place (an [`access_key`]) was read with no signature, for the public
/// source to list from now on.
pub fn found_public(place: &str) {
    if let Ok(mut places) = public_places().lock()
        && !places.iter().any(|p| p == place)
    {
        places.push(place.to_string());
    }
}

/// The public places found since the last call, as URLs of their roots.
pub fn take_public_places() -> Vec<String> {
    let places = public_places()
        .lock()
        .map(|mut places| std::mem::take(&mut *places))
        .unwrap_or_default();
    places
        .into_iter()
        .map(
            |place| match crate::source::azure_parts(&format!("{place}.dfs.core.windows.net/")) {
                Some((account, container, _)) => crate::source::azure_url(&account, &container, ""),
                None => format!("{place}/"),
            },
        )
        .collect()
}

/// What this session learned about signing requests to the place `url` is in: `true`
/// for unsigned.
pub fn known_access(url: &str) -> Option<bool> {
    let key = access_key(url)?;
    access().lock().ok()?.get(&key).copied()
}

/// Which source a plain `s3://bucket` belongs to, when a source other than the default
/// listed it. Opening a bucket you browsed to has to use the login that found it.
fn bucket_sources() -> &'static Mutex<HashMap<String, String>> {
    static MAP: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();
    MAP.get_or_init(Default::default)
}

/// Remember that `source` reaches `bucket`, for URLs that do not name their source.
pub fn remember_bucket(source: &Source, bucket: &str) {
    if source.public
        || source.named_in_urls()
        || source.id == DEFAULT_S3
        || source.id == DEFAULT_GCS
    {
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

/// Resolve `url` against the sources in `config` and on this machine, with an Amazon
/// S3 bucket's own region. May run a credential command and ask S3 where the bucket is,
/// so call it on a worker.
pub fn resolve(url: &str, config: &CloudConfig) -> Result<Resolved, String> {
    let mut resolved = resolve_with(url, config, &Environment::current())?;
    if resolved.kind == ProviderKind::S3
        && resolved.s3.endpoint.is_none()
        && let Some((_, bucket, _)) = crate::cloud_browse::split_bucket_url(&resolved.url)
        && let Some(region) = crate::cloud_browse::s3_bucket_region(&bucket)
    {
        resolved.s3.region = Some(region);
    }
    Ok(resolved)
}

/// As [`resolve`], for opening an object. A place whose signing is still [`Signing::Try`]
/// is settled first with one unsigned request, since the libraries that open it make
/// many requests and cannot retry them without a signature.
pub fn resolve_for_open(url: &str, config: &CloudConfig) -> Result<Resolved, String> {
    let resolved = settle_signing(resolve(url, config)?);
    Ok(with_azure_key_if_refused(resolved, config))
}

/// An Azure place signed with a sign-in's token that the account refuses for want of a
/// data role: checked with one listing request, once per account, and read with the
/// account's key from then on, when the fallback is on.
fn with_azure_key_if_refused(resolved: Resolved, config: &CloudConfig) -> Resolved {
    let enabled = config.azure_account_keys != Some(false);
    if resolved.kind != ProviderKind::Azure
        || resolved.signing == Signing::Unsigned
        || resolved.azure.identity.is_none()
        || !matches!(resolved.azure.auth, crate::azure::AzureAuth::Bearer(_))
        || !enabled
    {
        return resolved;
    }
    let Some((account, container, path)) = crate::source::azure_parts(&resolved.url) else {
        return resolved;
    };
    if crate::azure::token_reads(&account) {
        return resolved;
    }
    match crate::azure::check_read(&account, &container, &path, &resolved.azure) {
        Ok(()) => {
            crate::azure::remember_token_reads(&account);
            resolved
        }
        Err(refusal) => match crate::azure::with_account_key(
            &account,
            &resolved.azure,
            &refusal,
            enabled,
            &Environment::current(),
        ) {
            Ok(azure) => Resolved { azure, ..resolved },
            // The open itself fails with the same refusal, and says why.
            Err(_) => resolved,
        },
    }
}

/// A place still [`Signing::Try`] settled with one unsigned request.
fn settle_signing(resolved: Resolved) -> Resolved {
    if resolved.signing != Signing::Try {
        return resolved;
    }
    match crate::cloud_browse::probe_unsigned(&resolved) {
        Some(true) => {
            remember_access(&resolved.place, true);
            resolved.unsigned()
        }
        Some(false) => {
            remember_access(&resolved.place, false);
            Resolved {
                signing: Signing::Signed,
                ..resolved
            }
        }
        None => resolved,
    }
}

/// An `az://`, `adl://` or `azure://` URL, `container/path`, as its canonical `abfss://`
/// form. These name no account, so it comes from where the URL was typed (inside an
/// account on the home screen), the environment, or the one account in the config.
/// Every other path comes back as it is.
pub fn expand_azure_short_url(
    path: &std::path::Path,
    config: &CloudConfig,
    browsing: Option<&std::path::Path>,
) -> Result<std::path::PathBuf, String> {
    let text = path.to_string_lossy();
    let Some((scheme, rest)) = text.split_once("://") else {
        return Ok(path.to_path_buf());
    };
    if !matches!(scheme.to_ascii_lowercase().as_str(), "az" | "adl" | "azure") {
        return Ok(path.to_path_buf());
    }
    let (container, key) = rest.split_once('/').unwrap_or((rest, ""));
    // `az://container@account.dfs.core.windows.net/path` names its account after all.
    if container.contains('@')
        && let Some((account, container, key)) =
            crate::source::azure_parts(&format!("abfss://{rest}"))
    {
        return Ok(std::path::PathBuf::from(crate::source::azure_url(
            &account, &container, &key,
        )));
    }
    if container.is_empty() {
        return Err(format!("{text} names no container"));
    }
    let from_browsing = browsing.and_then(|place| {
        crate::home::cloud_account(place)
            .map(|(_, account)| account)
            .or_else(|| crate::source::azure_parts(&place.to_string_lossy()).map(|(a, _, _)| a))
    });
    let configured: Vec<&str> = config
        .sources
        .iter()
        .filter(|s| s.kind.as_deref() == Some("azure"))
        .filter_map(|s| s.account.as_deref())
        .collect();
    let account = from_browsing
        .or_else(|| {
            crate::azure::from_environment(&|k| std::env::var(k).ok())
                .and_then(|(settings, _)| settings.account)
        })
        .or_else(|| (configured.len() == 1).then(|| configured[0].to_string()))
        .ok_or_else(|| {
            format!(
                "{text} does not say which storage account. Use \
                 abfss://{container}@<account>.dfs.core.windows.net/{key}"
            )
        })?;
    Ok(std::path::PathBuf::from(crate::source::azure_url(
        &account, container, key,
    )))
}

/// The public source whose datasets hold `url`.
fn public_source_for<'a>(url: &str, sources: &'a [Source]) -> Option<&'a Source> {
    let (id, plain) = crate::source::split_source_id(url);
    if id.is_some() {
        return None;
    }
    sources
        .iter()
        .filter(|s| s.public)
        .find(|s| s.datasets.iter().any(|d| is_within(&plain, &d.url)))
}

/// As [`resolve`], with the environment supplied.
pub fn resolve_with(
    url: &str,
    config: &CloudConfig,
    env: &Environment<'_>,
) -> Result<Resolved, String> {
    let sources = discover(config, env);
    let place = access_key(url).ok_or_else(|| format!("not an object-store URL: {url}"))?;
    if let Some(public) = public_source_for(url, &sources) {
        let resolved = match crate::source::azure_parts(url) {
            Some((account, container, path)) => Resolved {
                url: crate::source::azure_url(&account, &container, &path),
                kind: ProviderKind::Azure,
                source_id: public.id.clone(),
                s3: S3Settings::default(),
                azure: Default::default(),
                signing: Signing::Unsigned,
                place,
                gcloud: None,
                google_credentials: None,
            },
            None => {
                let (kind, _, _) = crate::cloud_browse::split_bucket_url(url)
                    .ok_or_else(|| format!("not an object-store URL: {url}"))?;
                Resolved {
                    url: url.to_string(),
                    kind,
                    source_id: public.id.clone(),
                    s3: S3Settings::default(),
                    azure: Default::default(),
                    signing: Signing::Unsigned,
                    place,
                    gcloud: None,
                    google_credentials: None,
                }
            }
        };
        return Ok(resolved.unsigned());
    }
    let known = access()
        .lock()
        .ok()
        .and_then(|map| map.get(&place).copied());
    if let Some((account, container, path)) = crate::source::azure_parts(url) {
        return resolve_azure(&account, &container, &path, &sources, known, place, env);
    }
    let (id, plain) = crate::source::split_source_id(url);
    let (kind, bucket, _) = crate::cloud_browse::split_bucket_url(&plain)
        .ok_or_else(|| format!("not an object-store URL: {url}"))?;
    let find = |id: &str| sources.iter().find(|s| s.id == id).cloned();
    // A source that lists this bucket, or is named in the URL, owns it: its login is
    // the one to read it with. The default login may be anybody's.
    let mut owned = true;
    let mut no_login = false;

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
                // the active profile. Otherwise there is no login for this provider, and
                // the settings are only where to send an unsigned request.
                owned = false;
                no_login = find(default_id).is_none();
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
                    public: false,
                    datasets: Vec::new(),
                    gcloud: None,
                    secret_command: None,
                    google_credentials: None,
                    azure: Default::default(),
                })
            }
        },
    };

    let signing = match known {
        Some(true) => Signing::Unsigned,
        Some(false) => Signing::Signed,
        // Never a metadata service: a machine with no login reads public data.
        None if no_login => Signing::Unsigned,
        None if owned => Signing::Signed,
        None => Signing::Try,
    };
    let resolved = Resolved {
        url: plain.into_owned(),
        kind,
        source_id: source.id.clone(),
        s3: source.s3.clone(),
        azure: Default::default(),
        signing,
        place,
        gcloud: None,
        google_credentials: None,
    };
    if signing == Signing::Unsigned {
        return Ok(resolved.unsigned());
    }
    let id = source.id.clone();
    let gcloud = match &source.gcloud {
        Some(configuration) if kind == ProviderKind::Gcs => {
            let (token, _) = crate::gcloud::token(configuration, env)
                .map_err(|e| format!("source \"{id}\": {e}"))?;
            Some((configuration.clone(), token))
        }
        _ => None,
    };
    let source = source
        .with_credentials(env)
        .map_err(|e| format!("source \"{id}\": {e}"))?;
    let google_credentials = match kind {
        ProviderKind::Gcs => source.google_credentials.clone(),
        _ => None,
    };
    Ok(Resolved {
        s3: source.s3,
        gcloud,
        google_credentials,
        ..resolved
    })
}

/// An Azure URL: the account named in the environment when it is this one, else a
/// signed-in `az`, else no signature at all, which is how public containers are read.
fn resolve_azure(
    account: &str,
    container: &str,
    path: &str,
    sources: &[Source],
    known: Option<bool>,
    place: String,
    env: &Environment<'_>,
) -> Result<Resolved, String> {
    let named = sources
        .iter()
        .find(|s| s.kind == ProviderKind::Azure && s.azure.account.as_deref() == Some(account));
    // A sign-in that reaches any account: `az` or PowerShell, else an identity from the
    // environment that names no account. Never one known not to be signed in.
    let login = sources
        .iter()
        .filter(|s| s.problem.is_none())
        .find(|s| s.id == DEFAULT_AZURE_LOGIN)
        .or_else(|| {
            sources.iter().find(|s| {
                s.id == DEFAULT_AZURE_ENV
                    && s.problem.is_none()
                    && s.azure.account.is_none()
                    && s.azure.auth.is_identity()
            })
        });
    let signing = match (known, named, login) {
        (Some(true), _, _) | (None, None, None) => Signing::Unsigned,
        (Some(false), _, _) | (None, Some(_), _) => Signing::Signed,
        (None, None, Some(_)) => Signing::Try,
    };
    let resolved = Resolved {
        url: crate::source::azure_url(account, container, path),
        kind: ProviderKind::Azure,
        source_id: String::new(),
        s3: S3Settings::default(),
        azure: Default::default(),
        signing,
        place,
        gcloud: None,
        google_credentials: None,
    };
    match named.or(login) {
        Some(source) if signing != Signing::Unsigned => {
            // A sign-in refused for want of a data role reads with the account's key,
            // once the key has been fetched this session.
            if source.azure.auth.is_identity()
                && let Some(key) = crate::azure::remembered_key(account)
            {
                return Ok(Resolved {
                    source_id: source.id.clone(),
                    azure: crate::azure::AzureSettings {
                        identity: Some(source.azure.auth.clone()),
                        auth: crate::azure::AzureAuth::Key(key),
                        ..source.azure.clone()
                    },
                    signing: Signing::Signed,
                    ..resolved
                });
            }
            match source.azure.clone().with_token(env) {
                Ok(azure) => Ok(Resolved {
                    source_id: source.id.clone(),
                    azure,
                    ..resolved
                }),
                // A sign-in that may have nothing to do with this account is no reason
                // not to read it: public containers need none.
                Err(_) if signing == Signing::Try => Ok(resolved.unsigned()),
                Err(e) => Err(format!("source \"{}\": {e}", source.id)),
            }
        }
        _ => Ok(resolved.unsigned()),
    }
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
        let all_vars = || {
            machine
                .vars
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };
        let list = |dir: &Path| {
            machine
                .files
                .keys()
                .filter(|path| path.parent() == Some(dir))
                .cloned()
                .collect()
        };
        let env = Environment {
            var: &var,
            exists: &exists,
            read: &read,
            home: Some(PathBuf::from("/home/u")),
            windows: false,
            run: &run,
            all_vars: &all_vars,
            list: &list,
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
            s3_access_key_id: Some("key".to_string()),
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
            assert_eq!(resolved.s3.access_key_id.as_deref(), Some("key"));
            assert_eq!(resolved.signing, Signing::Try);
        });
    }

    #[test]
    fn with_no_login_for_a_provider_its_urls_are_read_unsigned() {
        let config = CloudConfig {
            s3_endpoint_url: Some("http://localhost:9000".to_string()),
            ..Default::default()
        };
        with_machine(&Machine::new(&[], &[]), |env| {
            let s3 = resolve_with("s3://nologin-data/key.parquet", &config, env).unwrap();
            assert_eq!(s3.signing, Signing::Unsigned);
            assert!(s3.s3.skip_signature && !s3.s3.from_env);
            assert_eq!(
                s3.s3.endpoint.as_deref(),
                Some("http://localhost:9000"),
                "still sent to the configured server"
            );
            let gcs = resolve_with("gs://nologin-bucket/key", &config, env).unwrap();
            assert_eq!(gcs.source_id, DEFAULT_GCS);
            assert_eq!(gcs.signing, Signing::Unsigned);
            let azure = resolve_with(
                "abfss://c@nologinacct.dfs.core.windows.net/k.parquet",
                &config,
                env,
            )
            .unwrap();
            assert_eq!(azure.signing, Signing::Unsigned);
            assert_eq!(azure.azure.auth, crate::azure::AzureAuth::None);
        });
    }

    #[test]
    fn a_login_that_may_not_own_the_place_tries_then_remembers() {
        let machine = Machine::new(
            &[
                ("AWS_ACCESS_KEY_ID", "AKIA"),
                ("AWS_SECRET_ACCESS_KEY", "s"),
            ],
            &[],
        );
        with_machine(&machine, |env| {
            let config = CloudConfig::from_env(env.var);
            let first = resolve_with("s3://tries-bucket/a.parquet", &config, env).unwrap();
            assert_eq!(first.signing, Signing::Try);
            assert_eq!(first.place, "s3://tries-bucket");
            assert_eq!(first.s3.access_key_id.as_deref(), Some("AKIA"));

            let unsigned = first.unsigned();
            assert!(unsigned.s3.skip_signature);
            assert_eq!(unsigned.s3.access_key_id, None, "no key goes with it");

            remember_access("s3://tries-bucket", true);
            let again = resolve_with("s3://tries-bucket/b/c.parquet", &config, env).unwrap();
            assert_eq!(again.signing, Signing::Unsigned);
            assert!(take_public_places().contains(&"s3://tries-bucket/".to_string()));

            remember_access("s3://signed-bucket", false);
            let signed = resolve_with("s3://signed-bucket/x", &config, env).unwrap();
            assert_eq!(signed.signing, Signing::Signed);
        });
    }

    #[test]
    fn public_datasets_are_built_in_and_read_unsigned() {
        let machine = Machine::new(
            &[
                ("AWS_ACCESS_KEY_ID", "AKIA"),
                ("AWS_SECRET_ACCESS_KEY", "s"),
            ],
            &[],
        );
        with_machine(&machine, |env| {
            let config = CloudConfig::from_env(env.var);
            let public = discover(&config, env)
                .into_iter()
                .find(|s| s.id == PUBLIC)
                .expect("on by default");
            assert!(public.public && public.datasets.len() >= 6);
            assert!(
                public
                    .datasets
                    .iter()
                    .all(|d| !d.name.is_empty() && !d.license.is_empty() && !d.homepage.is_empty())
            );
            for dataset in &public.datasets {
                let resolved = resolve_with(&dataset.url, &config, env).unwrap();
                assert_eq!(resolved.signing, Signing::Unsigned, "{}", dataset.url);
                assert_eq!(resolved.source_id, PUBLIC);
                assert_eq!(resolved.s3.access_key_id, None);
            }
            let inside = resolve_with(
                "s3://noaa-ghcn-pds/parquet/by_year/YEAR=2020/",
                &config,
                env,
            )
            .unwrap();
            assert_eq!(inside.signing, Signing::Unsigned);
            // The rest of the bucket is not a dataset.
            let beside = resolve_with("s3://noaa-ghcn-pds/csv/", &config, env).unwrap();
            assert_eq!(beside.signing, Signing::Try);

            let off = CloudConfig {
                public_datasets: Some(false),
                ..config.clone()
            };
            assert!(discover(&off, env).iter().all(|s| s.id != PUBLIC));
        });
    }

    #[test]
    fn a_configured_public_source_mixes_providers() {
        with_machine(&Machine::new(&[], &[]), |env| {
            let config = CloudConfig {
                sources: vec![CloudSourceConfig {
                    name: "open-data".to_string(),
                    public: Some(true),
                    buckets: vec![
                        "s3://gbif-open-data-us-east-1/occurrence/".to_string(),
                        "abfss://nyctlc@azureopendatastorage.dfs.core.windows.net/".to_string(),
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            };
            let source = discover(&config, env)
                .into_iter()
                .find(|s| s.id == "open-data")
                .unwrap();
            assert!(source.public && !source.named_in_urls());
            assert_eq!(
                source.datasets[0].name,
                "gbif-open-data-us-east-1/occurrence"
            );
            assert_eq!(source.datasets[1].name, "azureopendatastorage/nyctlc");
            assert_eq!(
                source.bucket_url(&source.datasets[0].url),
                "s3://gbif-open-data-us-east-1/occurrence/"
            );
            let azure = resolve_with(
                "abfss://nyctlc@azureopendatastorage.dfs.core.windows.net/yellow/",
                &config,
                env,
            )
            .unwrap();
            assert_eq!(azure.source_id, "open-data");
            assert_eq!(azure.signing, Signing::Unsigned);
        });
    }

    #[test]
    fn gcloud_configurations_are_logins() {
        let dir = "/home/u/.config/gcloud";
        let machine = Machine::new(
            &[],
            &[
                (&format!("{dir}/active_config") as &str, "work\n"),
                (
                    &format!("{dir}/configurations/config_work"),
                    "[core]\naccount = a@example.com\nproject = analytics\n",
                ),
                (
                    &format!("{dir}/configurations/config_other-project"),
                    "[core]\naccount = a@example.com\nproject = billing\n",
                ),
                (
                    &format!("{dir}/configurations/config_Personal"),
                    "[core]\naccount = me@example.org\n",
                ),
                (&format!("{dir}/configurations/config_empty"), "[core]\n"),
            ],
        );
        with_machine(&machine, |env| {
            let found = discover(&CloudConfig::default(), env);
            let google: Vec<(&str, Option<&str>, Option<&str>)> = found
                .iter()
                .filter(|s| s.kind == ProviderKind::Gcs)
                .map(|s| (s.id.as_str(), s.gcloud.as_deref(), s.project.as_deref()))
                .collect();
            // Only `gcloud auth login`: the active configuration is the default login,
            // one more account is a source, and a second configuration of the same
            // account is not.
            assert_eq!(
                google,
                [
                    (DEFAULT_GCS, Some("work"), Some("analytics")),
                    ("gcloud-personal", Some("Personal"), None),
                ]
            );
            let err = resolve_with("gs://some-bucket/key.parquet", &CloudConfig::default(), env)
                .unwrap_err();
            assert!(err.contains("needs gcloud"), "{err}");
        });
    }

    #[test]
    fn a_google_login_object_store_cannot_read_goes_through_gcloud() {
        let adc = "/home/u/.config/gcloud/application_default_credentials.json";
        let federated = r#"{"type": "external_account", "audience": "//iam.googleapis.com/x"}"#;
        let with_gcloud = Machine::new(
            &[],
            &[
                (adc, federated),
                (
                    "/home/u/.config/gcloud/configurations/config_default",
                    "[core]\naccount = a@example.com\n",
                ),
            ],
        );
        with_machine(&with_gcloud, |env| {
            let google = discover(&CloudConfig::default(), env)
                .into_iter()
                .find(|s| s.id == DEFAULT_GCS)
                .unwrap();
            assert_eq!(google.gcloud.as_deref(), Some("default"));
            assert_eq!(google.problem, None);
        });
        let without = Machine::new(&[], &[(adc, federated)]);
        with_machine(&without, |env| {
            let google = discover(&CloudConfig::default(), env)
                .into_iter()
                .find(|s| s.id == DEFAULT_GCS)
                .unwrap();
            assert_eq!(
                google.problem.as_deref(),
                Some("unsupported login: external_account")
            );
        });
    }

    #[test]
    fn a_configured_google_source_names_its_configuration_and_project() {
        with_machine(&Machine::new(&[], &[]), |env| {
            let config = CloudConfig {
                sources: vec![CloudSourceConfig {
                    name: "research".to_string(),
                    kind: Some("gcs".to_string()),
                    configuration: Some("research".to_string()),
                    project: Some("research-prod".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            };
            let source = discover(&config, env)
                .into_iter()
                .find(|s| s.id == "research")
                .unwrap();
            assert_eq!(source.gcloud.as_deref(), Some("research"));
            assert_eq!(source.project.as_deref(), Some("research-prod"));
            assert_eq!(
                source.bucket_url("research-prod"),
                "cloud://research/research-prod"
            );
        });
    }

    #[test]
    fn configured_azure_sources() {
        let azure = |name: &str| CloudSourceConfig {
            name: name.to_string(),
            kind: Some("azure".to_string()),
            account: Some(format!("{name}acct")),
            ..Default::default()
        };
        let config = CloudConfig {
            sources: vec![
                CloudSourceConfig {
                    account_key_env: Some("RESEARCH_KEY".to_string()),
                    ..azure("research")
                },
                CloudSourceConfig {
                    sas_env: Some("SHARED_SAS".to_string()),
                    ..azure("shared")
                },
                CloudSourceConfig {
                    account: None,
                    connection_string_env: Some("APP_STORAGE".to_string()),
                    ..azure("app")
                },
                azure("signin"),
                CloudSourceConfig {
                    account_key_env: Some("UNSET_KEY".to_string()),
                    ..azure("broken")
                },
            ],
            ..Default::default()
        };
        let machine = Machine::new(
            &[
                ("RESEARCH_KEY", "a2V5"),
                ("SHARED_SAS", "?sv=2024&sig=x"),
                (
                    "APP_STORAGE",
                    "DefaultEndpointsProtocol=https;AccountName=appdata;AccountKey=a2V5;EndpointSuffix=core.windows.net",
                ),
            ],
            &[],
        );
        with_machine(&machine, |env| {
            let found = discover(&config, env);
            let get = |id: &str| found.iter().find(|s| s.id == id).unwrap();
            use crate::azure::AzureAuth;
            assert_eq!(
                get("research").azure.auth,
                AzureAuth::Key("a2V5".to_string())
            );
            assert_eq!(
                get("shared").azure.auth,
                AzureAuth::Sas("sv=2024&sig=x".to_string())
            );
            assert_eq!(get("app").azure.account.as_deref(), Some("appdata"));
            assert_eq!(get("signin").azure.auth, AzureAuth::AzCli);
            assert_eq!(
                get("broken").problem.as_deref(),
                Some("UNSET_KEY is not set")
            );
            assert!(
                found
                    .iter()
                    .all(|s| s.kind != ProviderKind::Azure || !s.named_in_urls())
            );

            // A key fetched after a 403 is used for the account from then on.
            crate::azure::remember_key_for_test("signinacct", "a2V5Mg==");
            let resolved = resolve_with(
                "abfss://data@signinacct.dfs.core.windows.net/x.parquet",
                &config,
                env,
            )
            .unwrap();
            assert_eq!(resolved.source_id, "signin");
            assert_eq!(resolved.azure.auth, AzureAuth::Key("a2V5Mg==".to_string()));
            assert_eq!(resolved.azure.identity, Some(AzureAuth::AzCli));
        });
    }

    #[test]
    fn azure_tools_not_signed_in_do_not_block_public_containers() {
        let machine = Machine::new(&[("PATH", "/usr/bin")], &[("/usr/bin/az", "")]);
        with_machine(&machine, |env| {
            let config = CloudConfig::default();
            let az = discover(&config, env)
                .into_iter()
                .find(|s| s.id == DEFAULT_AZURE_LOGIN)
                .expect("a not signed in row");
            assert!(az.problem.as_deref().unwrap().starts_with("not signed in"));
            let resolved = resolve_with(
                "abfss://nyctlc@azureopendatastorage.dfs.core.windows.net/yellow/",
                &config,
                env,
            )
            .unwrap();
            assert_eq!(resolved.signing, Signing::Unsigned);
        });
        // Signed in by the look of it, but `az` cannot give a token: still readable.
        let expired = Machine::new(&[], &[("/home/u/.azure", "")]);
        with_machine(&expired, |env| {
            let resolved = resolve_with(
                "abfss://release@overturemapswestus2.dfs.core.windows.net/x/",
                &CloudConfig {
                    public_datasets: Some(false),
                    ..Default::default()
                },
                env,
            )
            .unwrap();
            assert_eq!(resolved.signing, Signing::Unsigned);
        });
    }

    #[test]
    fn azure_urls_without_an_account() {
        let config = CloudConfig {
            sources: vec![CloudSourceConfig {
                name: "research".to_string(),
                kind: Some("azure".to_string()),
                account: Some("datuiresearch".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let expand = |url: &str, browsing: Option<&str>| {
            expand_azure_short_url(Path::new(url), &config, browsing.map(Path::new))
        };
        assert_eq!(
            expand("az://raw/2024/a.parquet", None).unwrap(),
            PathBuf::from("abfss://raw@datuiresearch.dfs.core.windows.net/2024/a.parquet")
        );
        assert_eq!(
            expand("adl://raw/x.csv", Some("cloud://az/lake001")).unwrap(),
            PathBuf::from("abfss://raw@lake001.dfs.core.windows.net/x.csv"),
            "typed inside an account"
        );
        assert_eq!(
            expand("azure://raw@other.blob.core.windows.net/x.csv", None).unwrap(),
            PathBuf::from("abfss://raw@other.dfs.core.windows.net/x.csv")
        );
        assert_eq!(
            expand("s3://bucket/key", None).unwrap(),
            PathBuf::from("s3://bucket/key")
        );
        let none =
            expand_azure_short_url(Path::new("az://raw/x.csv"), &CloudConfig::default(), None);
        if std::env::var("AZURE_STORAGE_ACCOUNT_NAME").is_err()
            && std::env::var("AZURE_STORAGE_CONNECTION_STRING").is_err()
        {
            assert!(none.unwrap_err().contains("abfss://raw@<account>"));
        }
    }

    /// A machine whose runner answers `pass show …` with a secret, and fails otherwise.
    fn with_secret_runner<T>(machine: &Machine, body: impl FnOnce(&Environment<'_>) -> T) -> T {
        let var = |key: &str| machine.vars.get(key).cloned();
        let exists = |path: &Path| machine.files.contains_key(path);
        let read = |path: &Path| machine.files.get(path).cloned();
        let run = |program: &str, args: &[&str]| match (program, args) {
            ("pass", ["show", "minio/onprem"]) => Ok("s3cr3t-from-pass\n".to_string()),
            ("op", ["read", "op://vault/azure/key"]) => Ok("YWNjb3VudC1rZXk=".to_string()),
            ("pass", _) => Err(CommandError::Failed(
                "Error: minio/missing is not in the password store.".to_string(),
            )),
            _ => Err(CommandError::Missing(program.to_string())),
        };
        let all_vars = Vec::new;
        let list = |_: &Path| Vec::new();
        body(&Environment {
            var: &var,
            exists: &exists,
            read: &read,
            home: Some(PathBuf::from("/home/u")),
            windows: false,
            run: &run,
            all_vars: &all_vars,
            list: &list,
        })
    }

    #[test]
    fn secret_commands_supply_the_secret() {
        let config = CloudConfig {
            sources: vec![
                CloudSourceConfig {
                    secret_access_key_env: None,
                    secret_command: Some("pass show minio/onprem".to_string()),
                    ..minio("onprem", "https://minio.corp.example:9000")
                },
                CloudSourceConfig {
                    secret_access_key_env: None,
                    secret_command: Some("pass show minio/missing".to_string()),
                    ..minio("broken", "https://minio.corp.example:9000")
                },
                CloudSourceConfig {
                    name: "research".to_string(),
                    kind: Some("azure".to_string()),
                    account: Some("research".to_string()),
                    secret_command: Some("op read op://vault/azure/key".to_string()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let machine = Machine::new(&[("ONPREM_KEY", "AKIAONPREM"), ("BROKEN_KEY", "k")], &[]);
        with_secret_runner(&machine, |env| {
            let found = discover(&config, env);
            let onprem = found.iter().find(|s| s.id == "onprem").unwrap();
            assert_eq!(
                onprem.problem, None,
                "the command runs when the source is used"
            );
            let resolved = resolve_with("s3://onprem@data/x.parquet", &config, env).unwrap();
            assert_eq!(
                resolved.s3.secret_access_key.as_deref(),
                Some("s3cr3t-from-pass")
            );
            assert_eq!(resolved.s3.access_key_id.as_deref(), Some("AKIAONPREM"));

            let err = resolve_with("s3://broken@data/x.parquet", &config, env).unwrap_err();
            assert!(
                err.contains("secret_command failed: Error: minio/missing"),
                "{err}"
            );

            let research = found.iter().find(|s| s.id == "research").unwrap();
            let settings = research.azure.clone().with_token(env).unwrap();
            assert_eq!(
                settings.auth,
                crate::azure::AzureAuth::Key("YWNjb3VudC1rZXk=".to_string())
            );
        });
    }

    #[test]
    fn a_credentials_file_logs_a_google_source_in() {
        let config = CloudConfig {
            sources: vec![
                CloudSourceConfig {
                    name: "analytics".to_string(),
                    kind: Some("gcs".to_string()),
                    credentials_file: Some("~/keys/analytics-sa.json".to_string()),
                    ..Default::default()
                },
                CloudSourceConfig {
                    name: "gone".to_string(),
                    kind: Some("gcs".to_string()),
                    credentials_file: Some("/nowhere/sa.json".to_string()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let machine = Machine::new(
            &[],
            &[(
                "/home/u/keys/analytics-sa.json",
                r#"{"type": "service_account", "project_id": "analytics-prod"}"#,
            )],
        );
        with_machine(&machine, |env| {
            let found = discover(&config, env);
            let analytics = found.iter().find(|s| s.id == "analytics").unwrap();
            assert_eq!(
                analytics.google_credentials.as_deref(),
                Some(Path::new("/home/u/keys/analytics-sa.json"))
            );
            assert_eq!(analytics.project.as_deref(), Some("analytics-prod"));
            let gone = found.iter().find(|s| s.id == "gone").unwrap();
            assert!(gone.problem.as_deref().unwrap().contains("does not exist"));
        });
    }

    #[test]
    fn instance_identity_only_when_asked_or_the_platform_says() {
        let ids = |config: &CloudConfig, vars: &[(&str, &str)]| -> Vec<(String, String)> {
            with_machine(&Machine::new(vars, &[]), |env| {
                discover(config, env)
                    .into_iter()
                    .filter(|s| !s.public)
                    .map(|s| (s.id, s.origin))
                    .collect()
            })
        };
        assert!(
            ids(&CloudConfig::default(), &[]).is_empty(),
            "nothing asks a metadata service"
        );
        let opted_in = CloudConfig {
            instance_identity: Some(true),
            ..Default::default()
        };
        let found = ids(&opted_in, &[]);
        for (id, origin) in [
            (DEFAULT_S3, "instance role"),
            (DEFAULT_GCS, "instance identity"),
            (DEFAULT_AZURE_ENV, "managed identity"),
        ] {
            assert!(
                found.contains(&(id.to_string(), origin.to_string())),
                "{id} in {found:?}"
            );
        }
        assert_eq!(
            ids(&CloudConfig::default(), &[("K_SERVICE", "api")]),
            [(DEFAULT_GCS.to_string(), "instance identity".to_string())],
            "Cloud Run"
        );
        assert_eq!(
            ids(
                &CloudConfig::default(),
                &[("IDENTITY_ENDPOINT", "http://localhost:8081/msi/token")]
            ),
            [(
                DEFAULT_AZURE_ENV.to_string(),
                "managed identity".to_string()
            )],
            "App Service"
        );
        // Without the opt-in, a no-login URL is unsigned: no metadata request.
        with_machine(&Machine::new(&[], &[]), |env| {
            let resolved =
                resolve_with("s3://instance-test/key", &CloudConfig::default(), env).unwrap();
            assert_eq!(resolved.signing, Signing::Unsigned);
        });
    }

    #[test]
    fn urls_within_a_root() {
        assert!(is_within("s3://b/parquet/x", "s3://b/parquet/"));
        assert!(is_within("s3://b/parquet", "s3://b/parquet/"));
        assert!(!is_within("s3://b/parquetx", "s3://b/parquet/"));
        assert!(is_within(
            "https://acct.blob.core.windows.net/release/2026/",
            "abfss://release@acct.dfs.core.windows.net/"
        ));
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
            assert_eq!(ids, ["lab", DEFAULT_S3, PUBLIC]);
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
            assert_eq!(
                found.len(),
                2,
                "the replaced source and the public datasets"
            );
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
            assert_eq!(ids, [DEFAULT_S3, "aws-default", "aws-lab", PUBLIC]);
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
    fn mc_aliases_mc_host_and_s3cmd_are_sources() {
        let mc = r#"{"version": "10", "aliases": {
            "lab": {"url": "http://127.0.0.1:9000", "accessKey": "minioadmin", "secretKey": "minioadmin", "api": "S3v4", "path": "auto"},
            "Corp MinIO": {"url": "https://minio.corp.example", "accessKey": "corp", "secretKey": "s", "api": "S3v4", "path": "on"}
        }}"#;
        let s3cfg = "[default]\naccess_key = CEPH\nsecret_key = s\nhost_base = ceph.example:7480\nhost_bucket = ceph.example:7480\n";
        let machine = Machine::new(
            &[("MC_HOST_lab", "http://envkey:envsecret@127.0.0.1:9100")],
            &[("/home/u/.mc/config.json", mc), ("/home/u/.s3cfg", s3cfg)],
        );
        with_machine(&machine, |env| {
            let config = CloudConfig::default();
            let found = discover(&config, env);
            let mut ids: Vec<&str> = found.iter().map(|s| s.id.as_str()).collect();
            ids.sort();
            assert_eq!(ids, ["mc-corp-minio", "mc-lab", PUBLIC, "s3cfg"]);
            let lab = found.iter().find(|s| s.id == "mc-lab").unwrap();
            assert_eq!(
                lab.origin, "MC_HOST_lab",
                "the environment replaces the alias"
            );
            assert_eq!(lab.s3.endpoint.as_deref(), Some("http://127.0.0.1:9100"));
            let ceph = found.iter().find(|s| s.id == "s3cfg").unwrap();
            assert_eq!(ceph.label, "ceph.example:7480");
            assert!(ceph.named_in_urls());

            let resolved = resolve_with("s3://mc-corp-minio@data/x.parquet", &config, env).unwrap();
            assert_eq!(
                resolved.s3.endpoint.as_deref(),
                Some("https://minio.corp.example")
            );
            assert_eq!(resolved.s3.access_key_id.as_deref(), Some("corp"));
            assert_eq!(resolved.s3.virtual_hosted, Some(false));
        });
    }

    #[test]
    fn one_server_found_twice_is_one_source() {
        let mc = r#"{"version": "10", "aliases": {
            "lab": {"url": "http://127.0.0.1:9000/", "accessKey": "minioadmin", "secretKey": "minioadmin"}
        }}"#;
        let machine = Machine::new(
            &[("LAB_KEY", "minioadmin"), ("LAB_SECRET", "minioadmin")],
            &[("/home/u/.mc/config.json", mc)],
        );
        with_machine(&machine, |env| {
            let config = CloudConfig {
                sources: vec![CloudSourceConfig {
                    name: "lab".to_string(),
                    kind: Some("s3".to_string()),
                    endpoint_url: Some("http://127.0.0.1:9000".to_string()),
                    access_key_id_env: Some("LAB_KEY".to_string()),
                    secret_access_key_env: Some("LAB_SECRET".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            };
            let found = discover(&config, env);
            let ids: Vec<&str> = found.iter().map(|s| s.id.as_str()).collect();
            assert_eq!(ids, ["lab", PUBLIC], "the config's source stays");
            assert_eq!(found[0].origin, "datui config, mc alias");
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
