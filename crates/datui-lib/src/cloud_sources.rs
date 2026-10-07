//! Every object store datui can read, as a list of sources rather than one of each kind.
//!
//! A source is a store plus the login that reaches it: the default S3 settings, a
//! Google login, or an entry in `[[cloud.connections]]`. Each has an ID, and the ID is what
//! keeps two stores apart when their bucket names collide: two MinIO servers can both
//! have a bucket called `data`, so a URL from an S3-compatible source names it,
//! `s3://<id>@bucket/key`. Everywhere else the location is unambiguous and URLs stay the
//! standard ones, so a URL copied out of datui still works in any other tool.
//!
//! Discovery here reads environment variables and asks whether files exist. It never
//! touches the network: listing is `cloud_browse`'s job, and it runs on a worker.

use crate::cloud_browse::{Environment, ProviderKind};
use crate::config::{CloudConfig, CloudConnectionConfig, DatasetAccess, DatasetAuth};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

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
/// Whether `url` is `root` or somewhere inside it. Azure URLs are compared in their
/// canonical form, and a trailing slash does not matter.
pub fn is_within(url: &str, root: &str) -> bool {
    let url = crate::source::canonical_cloud_place(url);
    let root = crate::source::canonical_cloud_place(root);
    url == root
        || url
            .strip_prefix(&root)
            .is_some_and(|rest| rest.starts_with('/'))
}

/// Where a source came from. Lower wins when the same ID turns up twice, and sources
/// are listed in this order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Tier {
    /// `[[cloud.connections]]`, or `[cloud] s3_*` for the default source.
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
        self.kind == ProviderKind::S3 && self.id != DEFAULT_S3 && self.s3.endpoint.is_some()
    }

    /// The URL of one of this source's buckets. For Azure, the first level is storage
    /// accounts, and an account has no URL of its own, so it is a home-screen place.
    pub fn bucket_url(&self, bucket: &str) -> String {
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
            gcloud: None,
            secret_command: None,
            google_credentials: None,
            azure: crate::azure::AzureSettings {
                auth,
                ..Default::default()
            },
        });
    }

    for configured in &config.connections {
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

/// A `[[cloud.connections]]` entry as a source. The config has been validated, so the kind
/// is one datui knows.
fn configured_source(configured: &CloudConnectionConfig, env: &Environment<'_>) -> Source {
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
            gcloud: configured.configuration.clone(),
            secret_command: None,
            google_credentials,
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
fn configured_azure_source(configured: &CloudConnectionConfig, env: &Environment<'_>) -> Source {
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
    /// Why the login that would have signed is not signing: an expired session, a
    /// missing CLI. The place is read unsigned instead, in case it is public; when it
    /// is refused, this is the error to report, since it is the one to fix.
    pub login_error: Option<String>,
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
    if source.named_in_urls() || source.id == DEFAULT_S3 || source.id == DEFAULT_GCS {
        return;
    }
    let key = format!("{}://{bucket}", source.kind.scheme());
    if let Ok(mut map) = bucket_sources().lock() {
        map.insert(key, source.id.clone());
    }
}

/// Buckets an earlier run listed, remembered as if listed now: a bucket under Recent
/// opens with the login that found it before its source is listed again. Only S3:
/// Google's first level is projects, and an Azure URL names its account.
pub fn remember_listed(source: &Source, buckets: &[String]) {
    if source.kind != ProviderKind::S3 {
        return;
    }
    for bucket in buckets {
        remember_bucket(source, bucket);
    }
}

/// The sources the home screen shows: every `[[cloud.connections]]` entry, and of the
/// logins found on the machine, the kinds `[cloud] discover` allows.
pub fn on_home(sources: Vec<Source>, config: &CloudConfig) -> Vec<Source> {
    let Some(discover) = &config.discover else {
        return sources;
    };
    sources
        .into_iter()
        .filter(|source| {
            config.connections.iter().any(|c| c.name == source.id)
                || discover.allows(match source.kind {
                    ProviderKind::S3 => "s3",
                    ProviderKind::Gcs => "gcs",
                    ProviderKind::Azure => "azure",
                })
        })
        .collect()
}

fn remembered(kind: ProviderKind, bucket: &str) -> Option<String> {
    let key = format!("{}://{bucket}", kind.scheme());
    bucket_sources().lock().ok()?.get(&key).cloned()
}

/// Resolve `url` against the sources in `config` and on this machine, with an Amazon
/// S3 bucket's own region. May run a credential command and ask S3 where the bucket is,
/// so call it on a worker.
pub fn resolve(url: &str, config: &CloudConfig) -> Result<Resolved, String> {
    let sources = session_sources(config);
    let mut resolved = resolve_among(url, config, &sources, &Environment::current())?;
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
    // Read unsigned only because the login failed: when that is refused too, the
    // login is what to fix.
    if let Some(error) = &resolved.login_error
        && crate::cloud_browse::probe_unsigned(&resolved) == Some(false)
    {
        return Err(error.clone());
    }
    Ok(with_azure_key_if_refused(resolved, config))
}

/// An Azure place signed with a sign-in's token that the account refuses for want of a
/// data role: checked with one listing request, once per account, and read with the
/// account's key from then on, when the fallback is on.
fn with_azure_key_if_refused(resolved: Resolved, config: &CloudConfig) -> Resolved {
    let enabled = config.use_azure_account_keys;
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
    if !crate::source::is_azure_short_scheme(scheme) {
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
        .connections
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

/// How the catalogs say to read `url`: the innermost dataset URL holding it, and of two
/// that are the same place, the one in the catalog listed first, so the user's catalogs
/// outrank the bundled one. A URL naming its own source has said.
fn configured_access<'a>(url: &str, config: &'a CloudConfig) -> Option<&'a DatasetAccess> {
    let (id, plain) = crate::source::split_source_id(url);
    if id.is_some() {
        return None;
    }
    config
        .dataset_access
        .iter()
        .filter(|access| is_within(&plain, &access.url))
        .rev()
        .max_by_key(|access| crate::source::canonical_cloud_place(&access.url).len())
}

/// As [`resolve`], with the environment supplied.
pub fn resolve_with(
    url: &str,
    config: &CloudConfig,
    env: &Environment<'_>,
) -> Result<Resolved, String> {
    resolve_among(url, config, &discover(config, env), env)
}

/// What discovery found, kept so that each resolve does not read every tool's files
/// again: one is asked for per peek, per listing and per open. Kept for one config at a
/// time; [`SessionSources::refresh`] looks again.
#[derive(Debug, Default)]
pub struct SessionSources(Mutex<Option<(CloudConfig, Arc<[Source]>)>>);

impl SessionSources {
    /// The sources found for `config`, discovering them when nothing was kept for it.
    pub fn get(
        &self,
        config: &CloudConfig,
        discover: impl FnOnce() -> Vec<Source>,
    ) -> Arc<[Source]> {
        if let Ok(kept) = self.0.lock()
            && let Some((asked, sources)) = kept.as_ref()
            && asked == config
        {
            return sources.clone();
        }
        self.refresh(config, discover)
    }

    /// Discover again and keep what is found.
    pub fn refresh(
        &self,
        config: &CloudConfig,
        discover: impl FnOnce() -> Vec<Source>,
    ) -> Arc<[Source]> {
        // Not under the lock: discovery reads files, and a resolve waiting on another
        // would wait for nothing it needs.
        let sources: Arc<[Source]> = discover().into();
        if let Ok(mut kept) = self.0.lock() {
            *kept = Some((config.clone(), sources.clone()));
        }
        sources
    }
}

fn session() -> &'static SessionSources {
    static SESSION: OnceLock<SessionSources> = OnceLock::new();
    SESSION.get_or_init(SessionSources::default)
}

/// The sources on this machine for `config`, discovered once for the session.
pub fn session_sources(config: &CloudConfig) -> Arc<[Source]> {
    session().get(config, || discover(config, &Environment::current()))
}

/// The sources on this machine discovered again, as Ctrl+R asks: a login made since
/// is found.
pub fn rediscover(config: &CloudConfig) -> Arc<[Source]> {
    session().refresh(config, || discover(config, &Environment::current()))
}

/// As [`resolve_with`], among `sources` already discovered.
fn resolve_among(
    url: &str,
    config: &CloudConfig,
    sources: &[Source],
    env: &Environment<'_>,
) -> Result<Resolved, String> {
    let place = access_key(url).ok_or_else(|| format!("not an object-store URL: {url}"))?;
    let configured = configured_access(url, config);
    // A dataset read anonymously is read that way whoever is logged in: no login is
    // asked for, so none can fail or be refused in its place.
    if let Some(DatasetAccess {
        auth: DatasetAuth::Anonymous,
        catalog,
        ..
    }) = configured
    {
        let resolved = match crate::source::azure_parts(url) {
            Some((account, container, path)) => Resolved {
                url: crate::source::azure_url(&account, &container, &path),
                kind: ProviderKind::Azure,
                source_id: catalog.clone(),
                s3: S3Settings::default(),
                azure: Default::default(),
                signing: Signing::Unsigned,
                place,
                gcloud: None,
                google_credentials: None,
                login_error: None,
            },
            None => {
                let (kind, _, _) = crate::cloud_browse::split_bucket_url(url)
                    .ok_or_else(|| format!("not an object-store URL: {url}"))?;
                Resolved {
                    url: url.to_string(),
                    kind,
                    source_id: catalog.clone(),
                    s3: S3Settings::default(),
                    azure: Default::default(),
                    signing: Signing::Unsigned,
                    place,
                    gcloud: None,
                    google_credentials: None,
                    login_error: None,
                }
            }
        };
        return Ok(resolved.unsigned());
    }
    // Signed with the connection the dataset names, whatever an earlier read found.
    let connection = match configured {
        Some(DatasetAccess {
            auth: DatasetAuth::Connection(name),
            ..
        }) => match sources.iter().find(|s| &s.id == name) {
            Some(source) => Some(source.clone()),
            None => return Err(format!("no connection is named \"{name}\"")),
        },
        _ => None,
    };
    let known = access()
        .lock()
        .ok()
        .and_then(|map| map.get(&place).copied())
        .filter(|_| connection.is_none());
    if let Some(parts) = crate::source::azure_parts(url) {
        return resolve_azure(&parts, sources, connection.as_ref(), known, place, env);
    }
    let (id, plain) = crate::source::split_source_id(url);
    let (kind, bucket, _) = crate::cloud_browse::split_bucket_url(&plain)
        .ok_or_else(|| format!("not an object-store URL: {url}"))?;
    let find = |id: &str| sources.iter().find(|s| s.id == id).cloned();
    // A source that lists this bucket, or is named in the URL, owns it: its login is
    // the one to read it with. The default login may be anybody's.
    let mut owned = true;
    let mut no_login = false;

    let source = match (id, connection) {
        (None, Some(source)) => source,
        (Some(id), _) => {
            let Some(source) = find(id) else {
                return Err(unknown_source(id, sources));
            };
            if !source.named_in_urls() {
                return Err(format!(
                    "\"{id}\" has no endpoint, so it is not S3-compatible and its URLs are \
                     plain s3://bucket/key"
                ));
            }
            source
        }
        (None, None) => match remembered(kind, &bucket)
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
        login_error: None,
    };
    if signing == Signing::Unsigned {
        return Ok(resolved.unsigned());
    }
    let id = source.id.clone();
    let gcloud = match &source.gcloud {
        Some(configuration) if kind == ProviderKind::Gcs => {
            match crate::gcloud::token(configuration, env) {
                Ok((token, _)) => Some((configuration.clone(), token)),
                Err(e) => return login_failed(resolved, format!("source \"{id}\": {e}")),
            }
        }
        _ => None,
    };
    let source = match source.with_credentials(env) {
        Ok(source) => source,
        Err(e) => return login_failed(resolved, format!("source \"{id}\": {e}")),
    };
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

/// The login that would sign `resolved` failed. A login that owns the place reports it;
/// one that only might (`Signing::Try`) is no reason not to try the place unsigned,
/// since it may be public, keeping the error for when it is not.
fn login_failed(resolved: Resolved, error: String) -> Result<Resolved, String> {
    if resolved.signing != Signing::Try {
        return Err(error);
    }
    Ok(Resolved {
        login_error: Some(error),
        ..resolved.unsigned()
    })
}

/// An Azure URL: the account named in the environment when it is this one, else a
/// signed-in `az`, else no signature at all, which is how public containers are read.
/// `parts` are the URL's account, container and path.
fn resolve_azure(
    (account, container, path): &(String, String, String),
    sources: &[Source],
    connection: Option<&Source>,
    known: Option<bool>,
    place: String,
    env: &Environment<'_>,
) -> Result<Resolved, String> {
    let named = connection.or_else(|| {
        sources
            .iter()
            .find(|s| s.kind == ProviderKind::Azure && s.azure.account.as_ref() == Some(account))
    });
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
        login_error: None,
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
                Err(e) => login_failed(resolved, format!("source \"{}\": {e}", source.id)),
            }
        }
        _ => Ok(resolved.unsigned()),
    }
}

fn unknown_source(id: &str, sources: &[Source]) -> String {
    let names: Vec<&str> = sources
        .iter()
        .filter(|s| s.named_in_urls())
        .map(|s| s.id.as_str())
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
mod tests;
