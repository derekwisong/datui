//! Finding the object stores this machine can already read, and listing their
//! contents, so buckets can be browsed like local directories. Two rules:
//!
//! **Never list a provider datui cannot then read.** Discovery looks only at the
//! credentials `object_store` will use to open a file (not, say, `gcloud`'s own),
//! so every listed bucket opens.
//!
//! **Nothing here runs on the drawing thread.** Every network function is `async` and
//! driven from a worker, as remote filesystem roots are.

use crate::cloud::cloud_sources::{S3Settings, Signing, Source};
use crate::cloud::source::ProviderKind;
use crate::config::CloudConfig;
use crate::discover::is_empty_marker;
use std::path::{Path, PathBuf};

/// An object store datui believes it can read, and why it believes that.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Provider {
    pub kind: ProviderKind,
    /// Section title on the home screen.
    pub label: String,
    /// Which credentials were found.
    pub note: String,
    /// The GCP project whose buckets are listed: Google cannot enumerate buckets without
    /// one (typed URLs still open).
    pub project: Option<String>,
    /// The AWS profile in use, when one is named.
    pub profile: Option<String>,
    /// Set when the endpoint is not the provider's own (MinIO or another S3-compatible
    /// service).
    pub endpoint: Option<String>,
}

/// Everything discovery may look at, in one place so a test can supply it;
/// [`Environment::current`] is the real one.
pub struct Environment<'a> {
    /// Reads an environment variable.
    pub var: &'a dyn Fn(&str) -> Option<String>,
    /// Whether a path exists: detecting a credential file needs no contents.
    pub exists: &'a dyn Fn(&Path) -> bool,
    /// Reads a file, for the one case needing it (the project id in gcloud's credentials);
    /// `None` on any failure, costing only the project.
    pub read: &'a dyn Fn(&Path) -> Option<String>,
    /// The user's home directory, if there is one.
    pub home: Option<PathBuf>,
    /// Tools keep their files in different places on Windows, so lookups need to know.
    pub windows: bool,
    /// Runs a credential command: `aws`, a profile's `credential_process`.
    pub run: &'a crate::cloud::cloud_command::Runner<'a>,
    /// Every environment variable, for the ones named by pattern: `MC_HOST_<alias>`.
    pub all_vars: &'a dyn Fn() -> Vec<(String, String)>,
    /// A directory's entries, for tools keeping one file per login (`gcloud`
    /// configurations); empty when unreadable.
    pub list: &'a dyn Fn(&Path) -> Vec<PathBuf>,
}

impl Environment<'_> {
    /// The real environment.
    pub fn current() -> Environment<'static> {
        Environment {
            var: &crate::cloud::cloud_env::var,
            exists: &|path| path.exists(),
            read: &|path| std::fs::read_to_string(path).ok(),
            home: dirs::home_dir(),
            windows: cfg!(windows),
            run: &|program, args| {
                crate::cloud::cloud_command::run(
                    program,
                    args,
                    crate::cloud::cloud_command::CREDENTIAL_TIMEOUT,
                )
            },
            all_vars: &crate::cloud::cloud_env::vars,
            list: &|dir| {
                std::fs::read_dir(dir)
                    .map(|entries| entries.flatten().map(|e| e.path()).collect())
                    .unwrap_or_default()
            },
        }
    }
}

/// Object stores this machine can read, in display order. Empty is normal without
/// cloud credentials; nothing prompts, installs or logs in.
pub fn detect(config: &CloudConfig, env: &Environment<'_>) -> Vec<Provider> {
    let mut providers = Vec::new();
    if let Some(gcs) = detect_gcs(config, env) {
        providers.push(gcs);
    }
    if let Some(s3) = detect_s3(config, env) {
        providers.push(s3);
    }
    providers
}

/// Google Cloud Storage, when credentials `object_store` accepts are present.
fn detect_gcs(config: &CloudConfig, env: &Environment<'_>) -> Option<Provider> {
    // An explicit service account is named ahead of the ambient developer login: someone
    // chose it.
    let note = if (env.var)("GOOGLE_SERVICE_ACCOUNT").is_some()
        || (env.var)("GOOGLE_SERVICE_ACCOUNT_PATH").is_some()
    {
        "service account"
    } else if (env.var)("GOOGLE_SERVICE_ACCOUNT_KEY").is_some() {
        "service account key"
    } else if (env.var)("GOOGLE_APPLICATION_CREDENTIALS").is_some() {
        "GOOGLE_APPLICATION_CREDENTIALS"
    } else if adc_path(env).is_some() {
        // Short: it sits beside the title in a fixed half of the line, where a long phrase
        // would be cut.
        "gcloud"
    } else if instance_identity(config, env).gcp {
        "instance identity"
    } else {
        return None;
    };

    Some(Provider {
        kind: ProviderKind::Gcs,
        label: "Google Cloud Storage".to_string(),
        note: note.to_string(),
        project: gcp_project(env),
        profile: None,
        endpoint: None,
    })
}

/// Which clouds' VM or platform identity may be used. Finding one queries a metadata
/// service that can hang, so only when configured or signaled by the platform
/// (`K_SERVICE` on Cloud Run/Functions; `IDENTITY_ENDPOINT` or `MSI_ENDPOINT` on Azure
/// App Service, Functions, Container Apps). ECS and EKS are found by their own
/// variables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct InstanceIdentity {
    pub aws: bool,
    pub gcp: bool,
    pub azure: bool,
}

pub fn instance_identity(config: &CloudConfig, env: &Environment<'_>) -> InstanceIdentity {
    let opted_in = config.instance_identity;
    let set = |key: &str| (env.var)(key).is_some_and(|v| !v.trim().is_empty());
    InstanceIdentity {
        aws: opted_in,
        gcp: opted_in || set("K_SERVICE"),
        azure: opted_in || set("IDENTITY_ENDPOINT") || set("MSI_ENDPOINT"),
    }
}

/// The credential type of a Google login `object_store` cannot read (workload identity
/// federation, an impersonated service account), when the environment or ADC file has
/// one.
pub fn unreadable_google_login(env: &Environment<'_>) -> Option<String> {
    let path = (env.var)("GOOGLE_APPLICATION_CREDENTIALS")
        .map(PathBuf::from)
        .or_else(|| adc_path(env))?;
    crate::cloud::gcloud::unsupported_credential_type(&(env.read)(&path)?)
}

/// The application default credentials file where `object_store` reads it
/// (`%APPDATA%\gcloud\` on Windows, `$HOME/.config/gcloud/` elsewhere), kept in step so
/// discovery agrees with opening.
pub fn adc_path(env: &Environment<'_>) -> Option<PathBuf> {
    const FILE: &str = "application_default_credentials.json";
    let path = if env.windows {
        PathBuf::from((env.var)("APPDATA")?)
            .join("gcloud")
            .join(FILE)
    } else {
        env.home.as_ref()?.join(".config").join("gcloud").join(FILE)
    };
    (env.exists)(&path).then_some(path)
}

/// The project whose buckets to list: an environment variable, else the
/// `quota_project_id` `gcloud auth application-default login` writes to the
/// credentials file, so a logged-in developer needs no configuration. gcloud's
/// active project is not read: it lives in a private sqlite database.
pub(crate) fn gcp_project(env: &Environment<'_>) -> Option<String> {
    for key in [
        "DATUI_GCP_PROJECT",
        "GOOGLE_CLOUD_PROJECT",
        "GCLOUD_PROJECT",
        "CLOUDSDK_CORE_PROJECT",
        "GCP_PROJECT",
    ] {
        if let Some(value) = (env.var)(key) {
            let value = value.trim().to_string();
            if !value.is_empty() {
                return Some(value);
            }
        }
    }
    adc_quota_project(env)
}

/// The `quota_project_id` in the ADC file; only that field (the refresh token is
/// `object_store`'s).
fn adc_quota_project(env: &Environment<'_>) -> Option<String> {
    let path = adc_path(env)?;
    let contents = (env.read)(&path)?;
    let value: serde_json::Value = serde_json::from_str(&contents).ok()?;
    let project = value.get("quota_project_id")?.as_str()?.trim();
    if project.is_empty() {
        return None;
    }
    Some(project.to_string())
}

/// S3, or anything speaking it, when credentials are present. `config` is effective
/// (environment and command line folded in, `OpenOptions::effective_cloud`), so the
/// title names the host listing and opening reach; the environment is consulted
/// only for credential evidence.
fn detect_s3(config: &CloudConfig, env: &Environment<'_>) -> Option<Provider> {
    let endpoint = config.s3_endpoint_url.clone();

    let configured_keys = config.s3_access_key_id.is_some();
    let env_keys = (env.var)("AWS_ACCESS_KEY_ID").is_some();
    let profile = (env.var)("AWS_PROFILE");
    let shared_credentials = env.home.as_ref().is_some_and(|home| {
        (env.exists)(&home.join(".aws/credentials")) || (env.exists)(&home.join(".aws/config"))
    });
    // A role rather than a key: ECS/Fargate serve credentials over a loopback endpoint,
    // EKS via a projected web identity token; neither leaves a key or `~/.aws` file.
    // `object_store` resolves both.
    let container_role = (env.var)("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI").is_some()
        || (env.var)("AWS_CONTAINER_CREDENTIALS_FULL_URI").is_some();
    let web_identity = (env.var)("AWS_WEB_IDENTITY_TOKEN_FILE").is_some();

    let note = if configured_keys {
        "datui config"
    } else if env_keys {
        "AWS_ACCESS_KEY_ID"
    } else if profile.is_some() {
        "AWS_PROFILE"
    } else if container_role {
        "container role"
    } else if web_identity {
        "web identity"
    } else if shared_credentials {
        "~/.aws"
    } else if instance_identity(config, env).aws {
        "instance role"
    } else {
        // An EC2 instance role has no local evidence; asking the metadata service can hang,
        // so it is not discovered. URLs still open; AWS_PROFILE or `~/.aws/config` brings
        // back the listing.
        return None;
    };

    // An endpoint says "not AWS", not which S3-compatible service, so it is not named.
    let label = match endpoint.as_deref().and_then(endpoint_host) {
        Some(host) => format!("S3-compatible ({host})"),
        None => "Amazon S3".to_string(),
    };

    Some(Provider {
        kind: ProviderKind::S3,
        label,
        note: note.to_string(),
        project: None,
        profile,
        endpoint,
    })
}

/// The host and port of an endpoint URL, for display; `None` for non-URLs, so a
/// malformed config line never lands in a title.
fn endpoint_host(endpoint: &str) -> Option<String> {
    let rest = endpoint
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(endpoint);
    let host = rest.split(['/', '?', '#']).next()?.trim();
    if host.is_empty() {
        return None;
    }
    Some(host.to_string())
}

/// Bucket names from a GCS `storage/v1/b` response. Tolerant: no `items` means no
/// buckets, and an entry without a usable `name` is skipped.
pub fn parse_gcs_buckets(body: &str) -> Result<Vec<String>, String> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("not JSON: {e}"))?;

    // An error response is JSON too, with a more useful message than "no buckets".
    if let Some(message) = value
        .get("error")
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
    {
        return Err(message.to_string());
    }

    let Some(items) = value.get("items").and_then(|i| i.as_array()) else {
        return Ok(Vec::new());
    };
    Ok(items
        .iter()
        .filter_map(|item| item.get("name").and_then(|n| n.as_str()))
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect())
}

/// The `pageToken` for the next page of a bucket list, when the response has one.
pub fn gcs_next_page_token(body: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()?
        .get("nextPageToken")?
        .as_str()
        .map(str::to_string)
}

/// Bucket names from an S3 `ListBuckets` response. A custom endpoint may be hostile,
/// so quick-xml parses it with a depth cap no legitimate response reaches.
pub fn parse_s3_buckets(body: &str) -> Result<Vec<String>, String> {
    use quick_xml::events::Event;

    let mut reader = quick_xml::Reader::from_str(body);
    reader.config_mut().trim_text(true);

    let mut buckets = Vec::new();
    let mut path: Vec<Vec<u8>> = Vec::new();
    let mut error_message: Option<String> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(tag)) => {
                if path.len() >= MAX_XML_DEPTH {
                    return Err("response nested implausibly deeply".to_string());
                }
                path.push(tag.local_name().as_ref().as_bytes().to_vec());
            }
            Ok(Event::End(_)) => {
                path.pop();
            }
            Ok(Event::Text(text)) => {
                let value = text.xml10_content().into_owned();
                match path_tail(&path) {
                    // .../Buckets/Bucket/Name
                    (Some(b"Name"), Some(b"Bucket")) if !value.is_empty() => {
                        buckets.push(value);
                    }
                    // An Error document often arrives with a 200, so it is read.
                    (Some(b"Message"), Some(b"Error")) => error_message = Some(value),
                    _ => {}
                }
            }
            Ok(Event::Eof) => {
                // quick-xml reaches Eof on a truncated document; a body ending mid-element is
                // refused rather than its partial list reported as whole.
                if !path.is_empty() {
                    return Err("response ended inside an element".to_string());
                }
                break;
            }
            Err(e) => return Err(format!("malformed XML: {e}")),
            _ => {}
        }
    }

    if let Some(message) = error_message {
        return Err(message);
    }
    Ok(buckets)
}

/// No `ListBuckets` response nests this deep; the cap stops memory exhaustion.
const MAX_XML_DEPTH: usize = 32;

/// The innermost two element names of a path, for matching a leaf in its parent.
fn path_tail(path: &[Vec<u8>]) -> (Option<&[u8]>, Option<&[u8]>) {
    let len = path.len();
    let last = len.checked_sub(1).map(|i| path[i].as_slice());
    let parent = len.checked_sub(2).map(|i| path[i].as_slice());
    (last, parent)
}

/// How long any cloud request may take. Global, not per socket: a server trickling a
/// byte every twenty seconds defeats a read timeout.
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

pub(crate) fn http_agent() -> ureq::Agent {
    crate::cloud::user_agent::ureq_config()
        .timeout_global(Some(REQUEST_TIMEOUT))
        .build()
        .into()
}

/// The one S3 builder, so listing and opening authenticate identically (separate
/// builders once let the listing drop the configured endpoint and keys). `settings`
/// are one source's (`cloud_sources::resolve`). Addressed by bucket name: only
/// `s3://bucket/key` URLs.
pub fn s3_builder(bucket: &str, settings: &S3Settings) -> object_store::aws::AmazonS3Builder {
    // Only the default source borrows the shell's AWS variables; a configured source
    // would otherwise sign as whoever the shell is.
    let builder = if settings.from_env && !settings.skip_signature {
        object_store::aws::AmazonS3Builder::from_env()
    } else {
        object_store::aws::AmazonS3Builder::new()
    };
    let mut builder = builder.with_bucket_name(bucket).with_config(
        object_store::aws::AmazonS3ConfigKey::Client(crate::cloud::user_agent::CLIENT_KEY),
        crate::cloud::user_agent::get(),
    );
    if settings.skip_signature {
        builder = builder.with_skip_signature(true);
    }
    if let Some(endpoint) = &settings.endpoint {
        // `object_store` refuses plain `http` unless told, which MinIO containers speak.
        builder = builder.with_endpoint(endpoint.clone());
        if endpoint.starts_with("http://") {
            builder = builder.with_allow_http(true);
        }
    }
    if settings.endpoint.is_some() || settings.virtual_hosted.is_some() {
        builder = builder.with_virtual_hosted_style_request(settings.virtual_hosted_style());
    }
    if let Some(region) = &settings.region {
        builder = builder.with_region(region.clone());
    }
    // Each on its own, as the Polars scan applies them (key in the file, secret in
    // AWS_SECRET_ACCESS_KEY).
    if let Some(key) = &settings.access_key_id {
        builder = builder.with_access_key_id(key.clone());
    }
    if let Some(secret) = &settings.secret_access_key {
        builder = builder.with_secret_access_key(secret.clone());
    }
    if let Some(token) = &settings.session_token {
        builder = builder.with_token(token.clone());
    }
    builder
}

/// The object_store path for a key as stored. `Path::from` percent-encodes `%`, so
/// `100%.csv.gz` became `100%25.csv.gz` and 404'd; existing keys are parsed as is,
/// unless they cannot be a path.
pub fn object_path(key: &str) -> object_store::path::Path {
    object_store::path::Path::parse(key).unwrap_or_else(|_| object_store::path::Path::from(key))
}

/// An object store that also lists a page at a time: every store datui builds is both.
pub trait Store: object_store::ObjectStore + object_store::list::PaginatedListStore {}

impl<T: object_store::ObjectStore + object_store::list::PaginatedListStore> Store for T {}

/// The store for a resolved place, signed as the resolver decided, and the key inside it.
pub fn store(
    resolved: &crate::cloud::cloud_sources::Resolved,
) -> Result<(std::sync::Arc<dyn Store>, String), String> {
    if let Some((account, container, key)) = crate::cloud::source::azure_parts(&resolved.url) {
        let store = crate::cloud::azure::store(&account, &container, &resolved.azure)?;
        return Ok((std::sync::Arc::new(store), key));
    }
    let (kind, bucket, key) = split_bucket_url(&resolved.url)
        .ok_or_else(|| format!("not an object-store URL: {}", resolved.url))?;
    let store: std::sync::Arc<dyn Store> = match kind {
        ProviderKind::Gcs => std::sync::Arc::new(gcs_store(
            &bucket,
            resolved.signing == Signing::Unsigned,
            resolved.gcloud.as_ref().map(|(_, token)| token.as_str()),
            resolved.google_credentials.as_deref(),
        )?),
        ProviderKind::S3 => std::sync::Arc::new(
            s3_builder(&bucket, &resolved.s3)
                .build()
                .map_err(|e| format!("S3 is not configured: {e}"))?,
        ),
        ProviderKind::Azure => return Err("an Azure container needs its account".to_string()),
    };
    Ok((store, key))
}

/// A Google Cloud Storage store for one bucket, signed as the resolver decided.
fn gcs_store(
    bucket: &str,
    unsigned: bool,
    google_token: Option<&str>,
    google_credentials: Option<&Path>,
) -> Result<object_store::gcp::GoogleCloudStorage, String> {
    // Unsigned means no credential lookup, so no wait on an absent metadata service.
    let builder = match (unsigned, google_token) {
        (true, _) => object_store::gcp::GoogleCloudStorageBuilder::new().with_skip_signature(true),
        (false, Some(token)) => object_store::gcp::GoogleCloudStorageBuilder::new()
            .with_credentials(std::sync::Arc::new(
                object_store::StaticCredentialProvider::new(object_store::gcp::GcpCredential {
                    bearer: token.to_string(),
                }),
            )),
        (false, None) => match google_credentials {
            Some(file) => object_store::gcp::GoogleCloudStorageBuilder::new()
                .with_application_credentials(file.to_string_lossy()),
            None => object_store::gcp::GoogleCloudStorageBuilder::from_env(),
        },
    };
    builder
        .with_bucket_name(bucket)
        .with_config(
            object_store::gcp::GoogleConfigKey::Client(crate::cloud::user_agent::CLIENT_KEY),
            crate::cloud::user_agent::get(),
        )
        .build()
        .map_err(|e| format!("Google Cloud Storage is not configured: {e}"))
}

/// Entries one peek reads: enough to tell partitions from files, in one request.
const PEEK_KEYS: usize = 100;

/// What a cloud directory holds, from the first page of a delimited listing: `Hive`
/// for `key=value` children, `MultiFile` for Parquet files, else `Directory`. One
/// request; no object is read.
pub async fn peek_kind(
    url: &str,
    config: &CloudConfig,
) -> Result<(crate::discover::EntryKind, crate::discover::Holds), String> {
    let resolved = {
        let (url, config) = (url.to_string(), config.clone());
        tokio::task::spawn_blocking(move || crate::cloud::cloud_sources::resolve(&url, &config))
            .await
            .map_err(|e| format!("{e}"))??
    };
    match peek_page(&resolved).await {
        Err(refused) if resolved.signing == Signing::Try && is_refusal(&refused) => {
            peek_page(&resolved.unsigned()).await.map_err(|_| refused)
        }
        Err(refused) if is_refusal(&refused) && resolved.login_error.is_some() => {
            Err(resolved.login_error.clone().unwrap_or(refused))
        }
        other => other,
    }
}

async fn peek_page(
    resolved: &crate::cloud::cloud_sources::Resolved,
) -> Result<(crate::discover::EntryKind, crate::discover::Holds), String> {
    use object_store::list::PaginatedListOptions;
    let (store, prefix) = store(resolved)?;
    let prefix = format!("{}/", prefix.trim_matches('/'));
    let page = store
        .list_paginated(
            Some(&prefix),
            PaginatedListOptions {
                delimiter: Some("/".into()),
                max_keys: Some(PEEK_KEYS),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| format!("{e}"))?;
    let directories: Vec<String> = page
        .result
        .common_prefixes
        .iter()
        .map(|p| p.as_ref().to_string())
        .collect();
    let objects: Vec<(String, u64)> = page
        .result
        .objects
        .iter()
        .map(|o| (o.location.as_ref().to_string(), o.size))
        .collect();
    let (kind, holds) = look_at_page(&prefix, &directories, &objects, page.page_token.as_deref());
    if kind != crate::discover::EntryKind::MultiFile {
        return Ok((kind, holds));
    }
    // The listing says the files share an extension; whether they are one table needs
    // their footers (one ranged read each, with sizes known). The count is unchanged.
    Ok((
        verified_kind(resolved, &objects).await.unwrap_or(kind),
        holds,
    ))
}

/// Whether a `multi` directory is one table, from a few footers; `None` when
/// undecided, leaving the listing's (reversible, optimistic) answer.
async fn verified_kind(
    resolved: &crate::cloud::cloud_sources::Resolved,
    objects: &[(String, u64)],
) -> Option<crate::discover::EntryKind> {
    let store: std::sync::Arc<dyn object_store::ObjectStore> = store(resolved).ok()?.0;
    kind_from_footers(&store, objects).await
}

/// The half of [`verified_kind`] that reads, given a store to read from.
async fn kind_from_footers(
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    objects: &[(String, u64)],
) -> Option<crate::discover::EntryKind> {
    let parquet: Vec<&(String, u64)> = objects
        .iter()
        .filter(|(key, _)| crate::discover::is_parquet_key(key))
        .collect();
    if parquet.len() < 2 {
        return None;
    }
    let meter = std::sync::Arc::new(crate::measurements::Meter::default());
    let store = store.clone();
    let mut reads = tokio::task::JoinSet::new();
    for index in crate::discover::spread(parquet.len()) {
        let (key, size) = parquet[index].clone();
        let (store, meter) = (store.clone(), meter.clone());
        reads.spawn(async move {
            let file = crate::formats::dataset_files::DatasetFile {
                key,
                size,
                stamp: 0,
                etag: None,
            };
            crate::cloud::cloud_hive::footer_of_file(&store, &file, &meter)
                .await
                .ok()
        });
    }

    let mut per_file: Vec<Vec<String>> = Vec::new();
    while let Some(joined) = reads.join_next().await {
        if let Ok(Some(footer)) = joined {
            per_file.push(footer.schema.iter_names().map(|n| n.to_string()).collect());
        }
    }
    crate::discover::one_table_from(&per_file).map(|one| match one {
        true => crate::discover::EntryKind::MultiFile,
        false => crate::discover::EntryKind::Directory,
    })
}

/// One listing page with its continuation token, separate so the `+` is testable. A
/// token means more lies behind: the count is a floor (`100+ parquet`), as past
/// `MAX_ENTRIES_PER_DIR` locally.
fn look_at_page(
    prefix: &str,
    directories: &[String],
    objects: &[(String, u64)],
    next_page: Option<&str>,
) -> (crate::discover::EntryKind, crate::discover::Holds) {
    let (kind, mut holds) = look_at_listing(prefix, directories, objects);
    holds.truncated = next_page.is_some();
    (kind, holds)
}

/// The kind and holdings of a listing by [`crate::discover::classify`], the local rule;
/// a prefix's row label comes from the holdings.
pub fn look_at_listing(
    prefix: &str,
    directories: &[String],
    objects: &[(String, u64)],
) -> (crate::discover::EntryKind, crate::discover::Holds) {
    use crate::discover::Seen;
    let last = |key: &str| {
        key.trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or("")
            .to_string()
    };
    let here = prefix.trim_matches('/');
    let prefixes: Vec<String> = directories.iter().map(|d| last(d)).collect();
    let objects = objects.iter().filter_map(|(key, size)| {
        let name = last(key);
        // Dropped first: the prefix's own key (a console's folder marker, of any size) and an
        // empty object named like a sibling prefix.
        let stands_for_a_prefix = (!here.is_empty() && key.trim_matches('/') == here)
            || (*size == 0 && prefixes.contains(&name));
        (!name.is_empty() && !stands_for_a_prefix).then_some(Seen {
            name,
            is_dir: false,
            is_file: true,
            size: Some(*size),
        })
    });
    let seen = prefixes
        .iter()
        .map(|name| Seen {
            name: name.clone(),
            is_dir: true,
            is_file: false,
            size: None,
        })
        .chain(objects);
    let rules = crate::discover::Rules {
        directory: &last(here),
        sniff: None,
        in_bucket: true,
    };
    crate::discover::classify(seen, &rules)
}

/// Split a `gs://` or `s3://` URL into bucket and prefix (no leading or trailing slash,
/// empty for the root, as `object_store` wants). A source id (`s3://<id>@bucket`) is
/// dropped.
pub fn split_bucket_url(url: &str) -> Option<(ProviderKind, String, String)> {
    let (_, plain) = crate::cloud::source::split_source_id(url);
    let (scheme, rest) = plain.split_once("://")?;
    let kind = match scheme {
        "gs" | "gcs" => ProviderKind::Gcs,
        "s3" | "s3a" => ProviderKind::S3,
        _ => return None,
    };
    let rest = rest.trim_end_matches('/');
    let (bucket, prefix) = match rest.split_once('/') {
        Some((bucket, prefix)) => (bucket, prefix),
        None => (rest, ""),
    };
    if bucket.is_empty() {
        return None;
    }
    Some((
        kind,
        bucket.to_string(),
        prefix.trim_matches('/').to_string(),
    ))
}

/// The most rows one bucket level lists, as for a local directory; past it the listing
/// stops and says so (141,000 partitions would be 141 requests held in memory).
pub const MAX_LEVEL_ROWS: usize = crate::discover::MAX_ENTRIES_PER_DIR;

/// What one level of a place listed.
#[derive(Debug, Clone, Default)]
pub struct Level {
    pub rows: Vec<crate::discover::Entry>,
    /// The level held more than [`MAX_LEVEL_ROWS`]; `rows` are the first of them.
    pub truncated: bool,
    /// Stopped between pages because nobody wants it any more; `rows` are what came.
    pub cancelled: bool,
}

/// What a listing hands each page of rows as it comes.
pub type Progress = std::sync::Arc<dyn Fn(&[crate::discover::Entry]) + Send + Sync>;

/// How a listing is watched while it runs.
#[derive(Clone, Default)]
pub struct Watch {
    /// Handed each page's rows, directories first, after every page but the last.
    pub progress: Option<Progress>,
    /// Set, the listing stops before its next page.
    pub cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Only names starting with this: a filter asked of the server.
    pub names_from: Option<String>,
}

impl Watch {
    fn cancelled(&self) -> bool {
        self.cancelled.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// One level of a bucket or prefix as home rows, up to [`MAX_LEVEL_ROWS`]. A delimited
/// listing: a million objects under a hundred prefixes is one request, a hundred rows.
pub async fn list_objects(
    url: &str,
    config: &CloudConfig,
) -> Result<Vec<crate::discover::Entry>, String> {
    list_objects_watched(url, config, &Watch::default())
        .await
        .map(|level| level.rows)
}

/// [`list_objects`] a page at a time: `watch` sees rows as they come and can stop
/// between pages.
pub async fn list_objects_watched(
    url: &str,
    config: &CloudConfig,
    watch: &Watch,
) -> Result<Level, String> {
    // Resolving can run a credential command, which blocks: off the runtime's threads.
    let resolved = {
        let (url, config) = (url.to_string(), config.clone());
        tokio::task::spawn_blocking(move || crate::cloud::cloud_sources::resolve(&url, &config))
            .await
            .map_err(|e| format!("{e}"))??
    };
    let signing = resolved.signing;
    let place = resolved.place.clone();
    let listed = list_level(url, &resolved, watch).await;
    // A sign-in with no data role on an Azure account: its keys, as the Portal does.
    let (listed, resolved) = match listed {
        Err(refusal)
            if resolved.kind == ProviderKind::Azure
                && crate::cloud::azure::is_permission_mismatch(&refusal)
                && resolved.azure.identity.is_some() =>
        {
            let enabled = config.use_azure_account_keys;
            let keyed = {
                let (resolved, refusal) = (resolved.clone(), refusal.clone());
                tokio::task::spawn_blocking(move || {
                    let (account, _, _) = crate::cloud::source::azure_parts(&resolved.url)
                        .ok_or_else(|| refusal.clone())?;
                    crate::cloud::azure::with_account_key(
                        &account,
                        &resolved.azure,
                        &refusal,
                        enabled,
                        &Environment::current(),
                    )
                    .map(|azure| crate::cloud::cloud_sources::Resolved { azure, ..resolved })
                })
                .await
                .map_err(|e| format!("{e}"))?
            };
            match keyed {
                Ok(keyed) => (list_level(url, &keyed, watch).await, keyed),
                Err(why) => (Err(why), resolved),
            }
        }
        other => (other, resolved),
    };
    if resolved.kind == ProviderKind::Azure
        && listed.is_ok()
        && matches!(
            resolved.azure.auth,
            crate::cloud::azure::AzureAuth::Bearer(_)
        )
        && let Some((account, _, _)) = crate::cloud::source::azure_parts(&resolved.url)
    {
        crate::cloud::azure::remember_token_reads(&account);
    }
    match listed {
        Err(refused) if signing == Signing::Try && is_refusal(&refused) => {
            // Perhaps public, refused only because another login signed the request.
            let level = list_level(url, &resolved.unsigned(), watch)
                .await
                .map_err(|_| refused)?;
            crate::cloud::cloud_sources::remember_access(&place, true);
            Ok(level)
        }
        Ok(level) => {
            if signing == Signing::Try {
                crate::cloud::cloud_sources::remember_access(&place, false);
            }
            Ok(level)
        }
        // Unsigned because the login failed, and refused: the login is what to fix.
        Err(refused) if is_refusal(&refused) && resolved.login_error.is_some() => {
            Err(resolved.login_error.clone().unwrap_or(refused))
        }
        Err(e) => Err(e),
    }
}

/// The server-side prefix a home filter can ask a cut-short level for, if any. The
/// filter is fuzzy and the prefix literal, so it asks for the names' shared part up
/// to their last separator (`STATION=`, `year=`) plus the filter, in the names'
/// case; a filter already spelling the shared part is taken as typed.
pub fn narrowing_prefix(filter: &str, names: &[&str]) -> Option<String> {
    let filter = filter.trim();
    if filter.is_empty() || filter.contains('/') {
        return None;
    }
    let first = names.first()?;
    let mut common = first.len();
    for name in &names[1..] {
        common = common.min(
            first
                .bytes()
                .zip(name.bytes())
                .take_while(|(a, b)| a == b)
                .count(),
        );
    }
    while !first.is_char_boundary(common) {
        common -= 1;
    }
    // Back to the last separator: `STATION=A` shares `STATION=`; the `A` is where the page
    // ended.
    let shared = first[..common]
        .rfind(|c: char| !c.is_alphanumeric())
        .map_or("", |at| &first[..=at]);
    let typed = match filter.get(..shared.len()) {
        Some(head) if !shared.is_empty() && head.eq_ignore_ascii_case(shared) => {
            &filter[shared.len()..]
        }
        _ => filter,
    };
    let rest = names.iter().flat_map(|n| n[shared.len()..].chars());
    let (mut upper, mut lower) = (false, false);
    for c in rest {
        upper |= c.is_uppercase();
        lower |= c.is_lowercase();
    }
    let typed = match (upper, lower) {
        (true, false) => typed.to_uppercase(),
        (false, true) => typed.to_lowercase(),
        _ => typed.to_string(),
    };
    Some(format!("{shared}{typed}"))
}

/// Whether an error is the service refusing the request, rather than failing to answer.
pub fn is_refusal(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    [
        "403",
        "401",
        "forbidden",
        "unauthorized",
        "accessdenied",
        "access denied",
        "permissiondenied",
        "authorizationfailure",
        "authenticationfailed",
        "invalidauthenticationinfo",
        "noauthenticationinformation",
    ]
    .iter()
    .any(|word| lower.contains(word))
}

/// A key that is not data to open: job receipts and folder markers. Narrower than
/// [`crate::discover::is_bookkeeping`] (which decides a directory's kind): a leading
/// `_` is not enough here, since `_manifest.parquet` may be worth opening.
pub fn is_marker(name: &str) -> bool {
    name == "_SUCCESS"
        || name.starts_with("_committed_")
        || name.starts_with("_started_")
        || name.ends_with("_$folder$")
}

/// Whether an object in one level of `prefix` is a row: not a console's fake folder
/// (trailing slash, or an empty object named like a directory), nor the directory's
/// own key (`census/` comes back as `census` and 404s).
fn is_listed_object(location: &str, size: u64, prefix: &str, prefixes: &[String]) -> bool {
    let name = location.rsplit('/').next().unwrap_or(location);
    !(name.is_empty()
        || is_marker(name)
        || crate::cloud::azure::is_folder_marker(location, size, prefixes)
        || is_empty_marker(name, size)
        || location.trim_end_matches('/') == prefix)
}

/// One level of a place, signed or not as `resolved` says.
async fn list_level(
    url: &str,
    resolved: &crate::cloud::cloud_sources::Resolved,
    watch: &Watch,
) -> Result<Level, String> {
    let (pager, prefix) = store(resolved)?;
    let prefix = prefix.trim_matches('/').to_string();
    // Rows keep the listing's source so opening reaches the same server; Azure places by
    // canonical URL, directories with their slash.
    let (base, directory_end) = match crate::cloud::source::azure_parts(&resolved.url) {
        Some((account, container, _)) => (
            crate::cloud::source::azure_url(&account, &container, ""),
            "/",
        ),
        None => {
            let (kind, bucket, _) = split_bucket_url(&resolved.url)
                .ok_or_else(|| format!("not an object-store URL: {url}"))?;
            let base = match crate::cloud::source::split_source_id(url).0 {
                Some(id) => format!("{}://{id}@{bucket}/", kind.scheme()),
                None => format!("{}://{bucket}/", kind.scheme()),
            };
            (base, "")
        }
    };
    list_pages(pager.as_ref(), &prefix, watch, |result| {
        let prefixes: Vec<String> = result
            .common_prefixes
            .iter()
            .map(|p| p.as_ref().to_string())
            .collect();
        let directories = prefixes
            .iter()
            .map(|common| {
                let name = common
                    .rsplit('/')
                    .find(|part| !part.is_empty())
                    .unwrap_or(common)
                    .to_string();
                let path = format!("{base}{common}{directory_end}");
                crate::discover::Entry::directory(Path::new(&path)).with_name(name)
            })
            .collect();
        let objects = result
            .objects
            .into_iter()
            .filter(|object| {
                is_listed_object(object.location.as_ref(), object.size, &prefix, &prefixes)
            })
            .map(|object| {
                let location = object.location.as_ref().to_string();
                let name = location.rsplit('/').next().unwrap_or(&location).to_string();
                let path = PathBuf::from(format!("{base}{location}"));
                // Extensionless keys stay openable: a part file may be Parquet.
                let kind = if crate::discover::unreadable_by_name(&path) {
                    crate::discover::EntryKind::Other
                } else {
                    crate::discover::EntryKind::File
                };
                object_row(path, kind, name, &object)
            })
            .collect();
        (directories, objects)
    })
    .await
}

/// A listed object as a home-screen row.
fn object_row(
    path: PathBuf,
    kind: crate::discover::EntryKind,
    name: String,
    object: &object_store::ObjectMeta,
) -> crate::discover::Entry {
    let mut row = crate::discover::Entry::new(path, kind).with_name(name);
    row.size = Some(object.size);
    row.modified = Some(object.last_modified.into());
    row
}

/// One level under `prefix` a page at a time (via `rows_of`), stopped at
/// [`MAX_LEVEL_ROWS`] or a cancelled `watch`. Pages rather than `list_with_delimiter`,
/// which fetches every page before answering.
async fn list_pages(
    pager: &dyn object_store::list::PaginatedListStore,
    prefix: &str,
    watch: &Watch,
    mut rows_of: impl FnMut(
        object_store::ListResult,
    ) -> (Vec<crate::discover::Entry>, Vec<crate::discover::Entry>),
) -> Result<Level, String> {
    let mut key_prefix = if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    };
    if let Some(names) = &watch.names_from {
        key_prefix.push_str(names);
    }
    let (mut directories, mut objects) = (Vec::new(), Vec::new());
    let mut token = None;
    // Directories above objects, as every local listing has them.
    let rows = |directories: &[crate::discover::Entry], objects: &[crate::discover::Entry]| {
        let mut rows = directories.to_vec();
        rows.extend_from_slice(objects);
        rows
    };
    loop {
        if watch.cancelled() {
            return Ok(Level {
                rows: rows(&directories, &objects),
                truncated: false,
                cancelled: true,
            });
        }
        let page = pager
            .list_paginated(
                (!key_prefix.is_empty()).then_some(key_prefix.as_str()),
                object_store::list::PaginatedListOptions {
                    delimiter: Some("/".into()),
                    page_token: token.take(),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| format!("{e}"))?;
        let (more_directories, more_objects) = rows_of(page.result);
        let page_rows = watch
            .progress
            .as_ref()
            .map(|_| rows(&more_directories, &more_objects));
        directories.extend(more_directories);
        objects.extend(more_objects);
        let mut listed = rows(&directories, &objects);
        if listed.len() > MAX_LEVEL_ROWS {
            listed.truncate(MAX_LEVEL_ROWS);
            return Ok(Level {
                rows: listed,
                truncated: true,
                cancelled: false,
            });
        }
        match page.page_token {
            Some(next) => {
                if let (Some(progress), Some(page_rows)) = (&watch.progress, &page_rows) {
                    progress(page_rows);
                }
                token = Some(next);
            }
            None => {
                return Ok(Level {
                    rows: listed,
                    truncated: false,
                    cancelled: false,
                });
            }
        }
    }
}

/// A source's first level as home lists it: buckets for S3 and Google, storage
/// accounts for Azure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    pub name: String,
    /// Where Enter goes: a bucket URL, or `cloud://<id>/<account>`.
    pub place: PathBuf,
    /// Lines for the details pane.
    pub details: Vec<(String, String)>,
}

/// Everything at the top of a source.
pub async fn list_first_level(source: &Source) -> Result<Vec<Listed>, String> {
    if source.kind == ProviderKind::Gcs {
        let source = source.clone();
        return tokio::task::spawn_blocking(move || {
            tokio::runtime::Handle::current().block_on(list_gcs_projects(&source))
        })
        .await
        .map_err(|e| format!("{e}"))?;
    }
    if source.kind != ProviderKind::Azure {
        // The bucket listings use a blocking client, on their own thread so a silent server
        // holds up only its source.
        let blocking = source.clone();
        let names = tokio::task::spawn_blocking(move || {
            tokio::runtime::Handle::current().block_on(list_buckets(&blocking))
        })
        .await
        .map_err(|e| format!("{e}"))??;
        return Ok(names
            .into_iter()
            .map(|name| Listed {
                place: PathBuf::from(source.bucket_url(&name)),
                name,
                details: Vec::new(),
            })
            .collect());
    }
    let source = source.clone();
    tokio::task::spawn_blocking(move || {
        if let Some(problem) = &source.problem {
            return Err(problem.clone());
        }
        // A key, SAS or connection string names its one account.
        if let Some(account) = &source.azure.account {
            return Ok(vec![Listed {
                name: account.clone(),
                place: PathBuf::from(source.bucket_url(account)),
                details: Vec::new(),
            }]);
        }
        let accounts =
            crate::cloud::azure::discover_accounts(&source.azure.auth, &Environment::current())?;
        Ok(accounts
            .into_iter()
            .map(|account| {
                let mut details = Vec::new();
                if let Some(subscription) = account.subscription {
                    details.push(("subscription".to_string(), subscription));
                }
                if let Some(location) = account.location {
                    details.push(("region".to_string(), location));
                }
                let namespace = if account.hierarchical_namespace {
                    "hierarchical"
                } else {
                    "flat"
                };
                details.push(("namespace".to_string(), namespace.to_string()));
                if account.private_network {
                    details.push(("network".to_string(), "private".to_string()));
                }
                if !account.shared_key_access {
                    details.push(("shared keys".to_string(), "disabled".to_string()));
                }
                Listed {
                    place: PathBuf::from(source.bucket_url(&account.name)),
                    name: account.name,
                    details,
                }
            })
            .collect())
    })
    .await
    .map_err(|e| format!("{e}"))?
}

/// Where S3 keeps `bucket`, from the `x-amz-bucket-region` header sent unauthenticated
/// whatever the status; once per bucket per session, `None` if unsaid.
pub fn s3_bucket_region(bucket: &str) -> Option<String> {
    static REGIONS: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, Option<String>>>,
    > = std::sync::OnceLock::new();
    let regions = REGIONS.get_or_init(Default::default);
    if let Some(known) = regions.lock().ok()?.get(bucket) {
        return known.clone();
    }
    // A bucket with a dot in its name does not match the wildcard certificate.
    let url = if bucket.contains('.') {
        format!("https://s3.amazonaws.com/{bucket}")
    } else {
        format!("https://{bucket}.s3.amazonaws.com/")
    };
    let response = probe_agent().head(&url).call();
    let region = match response {
        Ok(response) => response
            .headers()
            .get("x-amz-bucket-region")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
        // No answer is not an answer: ask again next time.
        Err(_) => return None,
    };
    if let Ok(mut map) = regions.lock() {
        map.insert(bucket.to_string(), region.clone());
    }
    region
}

/// A client for one short request whose status is the answer: errors are statuses,
/// redirects not followed.
fn probe_agent() -> ureq::Agent {
    crate::cloud::user_agent::ureq_config()
        .timeout_global(Some(std::time::Duration::from_secs(10)))
        .http_status_as_error(false)
        .max_redirects(0)
        .build()
        .into()
}

/// Whether `resolved`'s place reads unsigned: `Some(true)` if an unsigned request
/// succeeds, `Some(false)` if refused, `None` otherwise (no network, missing object,
/// custom endpoint). One request: a `HEAD`, or a one-key listing.
pub fn probe_unsigned(resolved: &crate::cloud::cloud_sources::Resolved) -> Option<bool> {
    let url = probe_url(resolved)?;
    let agent = probe_agent();
    let mut request = if url.contains('?') {
        agent.get(&url)
    } else {
        agent.head(&url)
    };
    if resolved.kind == ProviderKind::Azure {
        request = request.header("x-ms-version", crate::cloud::azure::API_VERSION);
    }
    let response = request.call().ok()?;
    match response.status().as_u16() {
        200..=299 => Some(true),
        401 | 403 => Some(false),
        _ => None,
    }
}

/// The plain HTTPS URL for an unsigned look; `None` for custom endpoints and
/// emulators.
fn probe_url(resolved: &crate::cloud::cloud_sources::Resolved) -> Option<String> {
    let encode = |key: &str| key.split('/').map(urlencode).collect::<Vec<_>>().join("/");
    // The object, or for a prefix or glob the directory part to list one key from.
    let split = |key: &str| -> (String, bool) {
        let before_glob = key.split('*').next().unwrap_or("");
        if key.contains('*') || key.is_empty() || key.ends_with('/') {
            let directory = match before_glob.rsplit_once('/') {
                Some((directory, _)) => format!("{directory}/"),
                None => String::new(),
            };
            (directory, true)
        } else {
            (key.to_string(), false)
        }
    };
    match resolved.kind {
        ProviderKind::S3 => {
            if resolved.s3.endpoint.is_some() {
                return None;
            }
            let (_, bucket, _) = split_bucket_url(&resolved.url)?;
            let key = resolved.url.split_once("://")?.1;
            let key = key.split_once('/').map_or("", |(_, key)| key);
            let region = resolved.s3.region.as_deref().unwrap_or("us-east-1");
            let base = if bucket.contains('.') {
                format!("https://s3.{region}.amazonaws.com/{bucket}")
            } else {
                format!("https://{bucket}.s3.{region}.amazonaws.com")
            };
            Some(match split(key) {
                (directory, true) => format!(
                    "{base}/?list-type=2&max-keys=1&prefix={}",
                    urlencode(&directory)
                ),
                (object, false) => format!("{base}/{}", encode(&object)),
            })
        }
        ProviderKind::Gcs => {
            let (_, bucket, _) = split_bucket_url(&resolved.url)?;
            let key = resolved.url.split_once("://")?.1;
            let key = key.split_once('/').map_or("", |(_, key)| key);
            Some(match split(key) {
                (directory, true) => format!(
                    "https://storage.googleapis.com/storage/v1/b/{bucket}/o?maxResults=1&prefix={}",
                    urlencode(&directory)
                ),
                (object, false) => {
                    format!(
                        "https://storage.googleapis.com/{bucket}/{}",
                        encode(&object)
                    )
                }
            })
        }
        ProviderKind::Azure => {
            if resolved.azure.blob_endpoint.is_some() || resolved.azure.use_emulator {
                return None;
            }
            let (account, container, key) = crate::cloud::source::azure_parts(&resolved.url)?;
            let base = format!("https://{account}.blob.core.windows.net/{container}");
            Some(match split(&key) {
                (directory, true) => format!(
                    "{base}?restype=container&comp=list&maxresults=1&prefix={}",
                    urlencode(&directory)
                ),
                (object, false) => format!("{base}/{}", encode(&object)),
            })
        }
    }
}

/// The containers of one account in an Azure source, as rows to step into.
pub async fn list_account(
    source_id: &str,
    account: &str,
    config: &CloudConfig,
) -> Result<Vec<crate::discover::Entry>, String> {
    let (source_id, account, config) = (source_id.to_string(), account.to_string(), config.clone());
    let source = {
        let (source_id, config) = (source_id.clone(), config.clone());
        tokio::task::spawn_blocking(move || {
            crate::cloud::cloud_sources::session_sources(&config)
                .iter()
                .find(|s| s.id == source_id)
                .cloned()
                .ok_or_else(|| format!("source not found: {source_id}"))
        })
        .await
        .map_err(|e| format!("{e}"))??
    };
    if source.kind == ProviderKind::Gcs {
        let blocking = Source {
            project: Some(account.clone()),
            ..source.clone()
        };
        let buckets = tokio::task::spawn_blocking(move || {
            tokio::runtime::Handle::current().block_on(list_gcs_buckets(&blocking))
        })
        .await
        .map_err(|e| format!("{e}"))??;
        return Ok(buckets
            .into_iter()
            .map(|bucket| {
                // Opening a bucket found here has to use the login that found it.
                crate::cloud::cloud_sources::remember_bucket(&source, &bucket);
                crate::discover::Entry::directory(Path::new(&format!("gs://{bucket}")))
                    .with_name(bucket)
            })
            .collect());
    }
    tokio::task::spawn_blocking(move || {
        let env = Environment::current();
        if source.kind != ProviderKind::Azure {
            return Err(format!("{source_id} has no accounts"));
        }
        let settings = source.azure.with_token(&env)?;
        let containers = crate::cloud::azure::list_containers(&account, &settings)?;
        Ok(containers
            .into_iter()
            .map(|container| {
                crate::discover::Entry::directory(Path::new(&crate::cloud::source::azure_url(
                    &account, &container, "",
                )))
                .with_name(container)
            })
            .collect())
    })
    .await
    .map_err(|e| format!("{e}"))?
}

/// Every bucket the provider's credentials can see. Per provider, since `object_store`
/// is bucket-scoped; both borrow its credential handling (no new crypto), so listing
/// and opening use the same credentials.
pub async fn list_buckets(source: &Source) -> Result<Vec<String>, String> {
    if let Some(problem) = &source.problem {
        return Err(problem.clone());
    }
    let source = {
        let source = source.clone();
        tokio::task::spawn_blocking(move || source.with_credentials(&Environment::current()))
            .await
            .map_err(|e| format!("{e}"))??
    };
    let source = &source;
    match source.kind {
        ProviderKind::Gcs => list_gcs_buckets(source).await,
        ProviderKind::S3 => list_s3_buckets(&source.s3).await,
        ProviderKind::Azure => Err("Azure lists storage accounts, not buckets".to_string()),
    }
}

/// GCS buckets via the JSON API, with a bearer token from the store's credential
/// provider (built with a placeholder bucket only to ask for the credential).
async fn list_gcs_buckets(source: &Source) -> Result<Vec<String>, String> {
    let project = source.project.as_deref().ok_or_else(|| {
        "no GCP project is set, so there is nothing to list buckets for. Set \
         GOOGLE_CLOUD_PROJECT or DATUI_GCP_PROJECT."
            .to_string()
    })?;
    let bearer = google_bearer(source).await?;

    let mut buckets = crate::cloud::cloud_command::paged(MAX_BUCKET_PAGES, |token| {
        let mut url = format!(
            "https://storage.googleapis.com/storage/v1/b?project={}&maxResults=1000",
            urlencode(project)
        );
        if let Some(token) = token {
            url.push_str(&format!("&pageToken={}", urlencode(token)));
        }
        let body = crate::cloud::gcloud::get(&url, &bearer)?;
        Ok((parse_gcs_buckets(&body)?, gcs_next_page_token(&body)))
    })?;
    buckets.sort();
    Ok(buckets)
}

/// A Google source's bearer token: from `gcloud` for a configuration login, else
/// object_store's credential chain.
async fn google_bearer(source: &Source) -> Result<String, String> {
    if let Some(problem) = &source.problem {
        return Err(problem.clone());
    }
    if let Some(configuration) = source.gcloud.clone() {
        return tokio::task::spawn_blocking(move || {
            crate::cloud::gcloud::token(&configuration, &Environment::current())
                .map(|(token, _)| token)
        })
        .await
        .map_err(|e| format!("{e}"))?;
    }
    // A store needs a bucket name; this one exists only to yield a credential.
    let store = gcs_store(
        "datui-credential-probe",
        false,
        None,
        source.google_credentials.as_deref(),
    )?;
    store
        .credentials()
        .get_credential()
        .await
        .map(|credential| credential.bearer.clone())
        .map_err(|e| format!("could not obtain Google credentials: {e}"))
}

/// A Google source's projects as its first level: all Resource Manager finds,
/// configured project first; the configured one alone when projects cannot be searched.
async fn list_gcs_projects(source: &Source) -> Result<Vec<Listed>, String> {
    let bearer = google_bearer(source).await?;
    let searched = {
        let bearer = bearer.clone();
        tokio::task::spawn_blocking(move || crate::cloud::gcloud::search_projects(&bearer))
            .await
            .map_err(|e| format!("{e}"))?
    };
    let mut projects = match (searched, &source.project) {
        (Ok(projects), _) => projects,
        (Err(_), Some(project)) => vec![crate::cloud::gcloud::Project {
            id: project.clone(),
            name: None,
        }],
        (Err(e), None) => return Err(e),
    };
    if let Some(configured) = &source.project {
        match projects.iter().position(|p| &p.id == configured) {
            Some(i) => {
                let first = projects.remove(i);
                projects.insert(0, first);
            }
            None => projects.insert(
                0,
                crate::cloud::gcloud::Project {
                    id: configured.clone(),
                    name: None,
                },
            ),
        }
    }
    Ok(projects
        .into_iter()
        .map(|project| {
            let mut details = Vec::new();
            if let Some(name) = project.name.filter(|n| n != &project.id) {
                details.push(("name".to_string(), name));
            }
            if source.project.as_deref() == Some(project.id.as_str()) {
                details.push(("project".to_string(), "configured".to_string()));
            }
            Listed {
                place: PathBuf::from(source.bucket_url(&project.id)),
                name: project.id,
                details,
            }
        })
        .collect())
}

/// S3 buckets via `ListBuckets` on the endpoint root, signed with `object_store`'s
/// `AwsAuthorizer` (the SigV4 implementation datui already relies on).
async fn list_s3_buckets(settings: &S3Settings) -> Result<Vec<String>, String> {
    use object_store::aws::AwsAuthorizer;

    // As for GCS: the store holds credentials and region; `ListBuckets` is not addressed to
    // a bucket.
    let s3 = s3_builder("datui-credential-probe", settings)
        .build()
        .map_err(|e| format!("S3 is not configured: {e}"))?;
    let credential = s3
        .credentials()
        .get_credential()
        .await
        .map_err(|e| format!("could not obtain AWS credentials: {e}"))?;

    // The default source's settings already carry AWS_REGION / AWS_DEFAULT_REGION.
    let region = settings
        .region
        .clone()
        .unwrap_or_else(|| "us-east-1".to_string());
    let url = s3_list_buckets_url(settings);

    // Signed as an `http::Request` (what the authorizer takes), then replayed on the
    // existing agent rather than adding a second HTTP client.
    let mut signed = http::Request::builder()
        .method("GET")
        .uri(&url)
        .body(object_store::client::HttpRequestBody::empty())
        .map_err(|e| format!("could not build the request: {e}"))?;
    AwsAuthorizer::new(&credential, "s3", &region).authorize(&mut signed, None);

    let mut request = http_agent().get(&url);
    for (name, value) in signed.headers() {
        if let Ok(value) = value.to_str() {
            request = request.header(name.as_str(), value);
        }
    }
    let body = request
        .call()
        .map_err(|e| format!("{e}"))?
        .body_mut()
        .read_to_string()
        .map_err(|e| format!("could not read the response: {e}"))?;

    let mut buckets = parse_s3_buckets(&body)?;
    buckets.sort();
    Ok(buckets)
}

/// Where `ListBuckets` goes: the effective endpoint's root (AWS if none), the same
/// host `s3_builder` opens against.
fn s3_list_buckets_url(settings: &S3Settings) -> String {
    let endpoint = settings
        .endpoint
        .as_deref()
        .unwrap_or("https://s3.amazonaws.com");
    format!("{}/", endpoint.trim_end_matches('/'))
}

/// A cap on pages, since a token that never ends is a loop; far past real accounts.
const MAX_BUCKET_PAGES: usize = 20;

/// Percent-encode a query parameter value (project ids, page tokens); local rather than
/// a dependency for two call sites.
pub(crate) fn urlencode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod aws_role_tests;

#[cfg(test)]
mod one_table_tests;
