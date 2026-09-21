//! Finding the object stores this machine can already read, and listing what is in
//! them.
//!
//! datui could open a `gs://` or `s3://` URL long before this module existed, but only
//! if you already knew the URL and typed it. That is a poor fit for how object storage
//! is actually used: the bucket names live in somebody's head, or in a console tab, and
//! the thing you want on a home screen is the same thing you want for a local
//! directory — a list of what is there.
//!
//! Two rules shape everything here.
//!
//! **Never list a provider datui cannot then read.** Discovery deliberately looks at
//! exactly the credentials `object_store` will use when the file is opened, and nowhere
//! else. It would be easy to enumerate buckets through `gcloud`, which is authenticated
//! on most developer machines when nothing else is, and the result would be a screen of
//! buckets that every `Enter` fails on. A provider that is invisible because its
//! credentials are missing is a smaller problem than one that lies.
//!
//! **Nothing here runs on the thread that draws.** Every function that touches the
//! network is `async` and is driven from a worker, the same arrangement remote
//! filesystem roots use. A bucket list is a network round trip, and a round trip on the
//! event thread is a frozen interface.

use crate::cloud_sources::{S3Settings, Signing, Source};
use crate::config::CloudConfig;
use std::path::{Path, PathBuf};

/// Which API a provider speaks. Not which company runs it: MinIO, Ceph, R2 and AWS
/// itself are all [`ProviderKind::S3`], and are told apart by their endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderKind {
    Gcs,
    S3,
    Azure,
}

impl ProviderKind {
    /// The URL scheme datui opens this provider's objects with.
    pub fn scheme(self) -> &'static str {
        match self {
            ProviderKind::Gcs => "gs",
            ProviderKind::S3 => "s3",
            ProviderKind::Azure => "abfss",
        }
    }
}

/// An object store datui believes it can read, and why it believes that.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Provider {
    pub kind: ProviderKind,
    /// Section title on the home screen.
    pub label: String,
    /// Which credentials were found.
    pub note: String,
    /// The GCP project whose buckets get listed. Google's API cannot enumerate buckets
    /// without one, so a GCS provider with no project can still open a URL you type but
    /// cannot offer you a list.
    pub project: Option<String>,
    /// The AWS profile in use, when one is named.
    pub profile: Option<String>,
    /// Set when the endpoint is not the provider's own, which is what makes this MinIO
    /// or another S3-compatible service rather than AWS.
    pub endpoint: Option<String>,
}

impl Provider {
    /// Shown beside the section title: the account the buckets belong to, when there
    /// is one to name. The title already says which store this is.
    pub fn detail(&self) -> Option<String> {
        match (&self.project, &self.profile) {
            (Some(project), _) => Some(format!("project: {project}")),
            (None, Some(profile)) => Some(format!("profile: {profile}")),
            (None, None) => None,
        }
    }

    /// True when this provider can enumerate its own buckets.
    ///
    /// Being unable to is not an error and not a reason to hide it. A GCS provider
    /// without a project, or an S3 provider whose credentials are scoped to one bucket,
    /// still opens anything you point it at.
    pub fn can_list_buckets(&self) -> bool {
        match self.kind {
            ProviderKind::Gcs => self.project.is_some(),
            ProviderKind::S3 | ProviderKind::Azure => true,
        }
    }
}

/// Everything discovery is allowed to look at, gathered in one place so it can be
/// supplied verbatim by a test.
///
/// Discovery is otherwise a function of the whole machine — environment, home
/// directory, config file — and a function of the whole machine cannot be tested. The
/// real one is [`Environment::current`].
pub struct Environment<'a> {
    /// Reads an environment variable.
    pub var: &'a dyn Fn(&str) -> Option<String>,
    /// True when the path exists. This is how a credential file is detected; deciding
    /// whether a provider is worth showing does not need its contents.
    pub exists: &'a dyn Fn(&Path) -> bool,
    /// Reads a file, for the one case that needs it: the project id inside the gcloud
    /// credentials file. Returns `None` on any failure, so an unreadable or malformed
    /// file costs the project and nothing else.
    pub read: &'a dyn Fn(&Path) -> Option<String>,
    /// The user's home directory, if there is one.
    pub home: Option<PathBuf>,
    /// Tools keep their files in different places on Windows, so lookups need to know.
    pub windows: bool,
    /// Runs a credential command: `aws`, a profile's `credential_process`.
    pub run: &'a crate::cloud_command::Runner<'a>,
    /// Every environment variable, for the ones named by pattern: `MC_HOST_<alias>`.
    pub all_vars: &'a dyn Fn() -> Vec<(String, String)>,
    /// The entries of a directory, for tools that keep one file per login: `gcloud`
    /// configurations. Empty when it cannot be read.
    pub list: &'a dyn Fn(&Path) -> Vec<PathBuf>,
}

impl Environment<'_> {
    /// The real environment.
    pub fn current() -> Environment<'static> {
        Environment {
            var: &crate::cloud_env::var,
            exists: &|path| path.exists(),
            read: &|path| std::fs::read_to_string(path).ok(),
            home: dirs::home_dir(),
            windows: cfg!(windows),
            run: &|program, args| {
                crate::cloud_command::run(program, args, crate::cloud_command::CREDENTIAL_TIMEOUT)
            },
            all_vars: &crate::cloud_env::vars,
            list: &|dir| {
                std::fs::read_dir(dir)
                    .map(|entries| entries.flatten().map(|e| e.path()).collect())
                    .unwrap_or_default()
            },
        }
    }
}

/// Object stores this machine can read, in the order they should appear.
///
/// Empty is the normal answer on a machine with no cloud credentials, and it is not a
/// failure. Nothing here prompts, installs or logs in.
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
    // Order matters only for the note: an explicit service account is worth naming
    // ahead of the ambient developer login, because it is the one someone chose.
    let note = if (env.var)("GOOGLE_SERVICE_ACCOUNT").is_some()
        || (env.var)("GOOGLE_SERVICE_ACCOUNT_PATH").is_some()
    {
        "service account"
    } else if (env.var)("GOOGLE_SERVICE_ACCOUNT_KEY").is_some() {
        "service account key"
    } else if (env.var)("GOOGLE_APPLICATION_CREDENTIALS").is_some() {
        "GOOGLE_APPLICATION_CREDENTIALS"
    } else if adc_path(env).is_some() {
        // Short on purpose. This sits beside the section title in a fixed half of the
        // line, and "gcloud application default credentials" truncated from the front
        // to "…lication default credentials" says less than one word does.
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

/// Which clouds' VM or platform identity may be used. Finding one is a request to a
/// metadata service that hangs on some networks, so it is only made when the config
/// asks, or where the platform itself says it is there: `K_SERVICE` on Cloud Run and
/// Cloud Functions, `IDENTITY_ENDPOINT` or `MSI_ENDPOINT` on Azure App Service,
/// Functions and Container Apps. ECS and EKS need neither: they are found by their
/// own variables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct InstanceIdentity {
    pub aws: bool,
    pub gcp: bool,
    pub azure: bool,
}

pub fn instance_identity(config: &CloudConfig, env: &Environment<'_>) -> InstanceIdentity {
    let opted_in = config.instance_identity == Some(true);
    let set = |key: &str| (env.var)(key).is_some_and(|v| !v.trim().is_empty());
    InstanceIdentity {
        aws: opted_in,
        gcp: opted_in || set("K_SERVICE"),
        azure: opted_in || set("IDENTITY_ENDPOINT") || set("MSI_ENDPOINT"),
    }
}

/// The credential type of a Google login object_store cannot read, when that is what the
/// environment or the application-default file holds: workload identity federation,
/// an impersonated service account.
pub fn unreadable_google_login(env: &Environment<'_>) -> Option<String> {
    let path = (env.var)("GOOGLE_APPLICATION_CREDENTIALS")
        .map(PathBuf::from)
        .or_else(|| adc_path(env))?;
    crate::gcloud::unsupported_credential_type(&(env.read)(&path)?)
}

/// The application default credentials file, where `object_store` reads it:
/// `%APPDATA%\gcloud\` on Windows, `$HOME/.config/gcloud/` elsewhere. Kept in step with
/// `object_store::gcp` deliberately: discovery must agree with the code that will later
/// do the opening, and looking under the home directory on Windows found nothing.
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

/// The project whose buckets to list.
///
/// An environment variable wins, because it is the one someone set for this shell. The
/// fallback is the `quota_project_id` that `gcloud auth application-default login`
/// writes into the credentials file, which is what makes the common case work with no
/// configuration at all: a developer who has logged in has a project, and asking them
/// to restate it in an environment variable to see their own buckets would be a poor
/// welcome.
///
/// `gcloud`'s active project setting is deliberately not consulted. It lives in a
/// private sqlite database rather than a documented file, and reading another tool's
/// internal state is the kind of cleverness that breaks silently when that tool
/// changes. The credentials file is different: it is a documented format, and
/// `object_store` already reads it.
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

/// The `quota_project_id` recorded in the application default credentials file.
///
/// Only that one field is taken. The file also holds a refresh token, which is none of
/// this module's business: the token is `object_store`'s to use, and discovery has no
/// reason to touch it.
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

/// S3, or anything that speaks it, when credentials are present.
///
/// `config` is the effective one, with the environment and the command line already
/// folded in (`OpenOptions::effective_cloud`). The endpoint is taken from it alone, so
/// the section title names the host the listing and the open actually reach; the
/// environment is consulted only for the credential evidence that never lives in a
/// config file.
fn detect_s3(config: &CloudConfig, env: &Environment<'_>) -> Option<Provider> {
    let endpoint = config.s3_endpoint_url.clone();

    let configured_keys = config.s3_access_key_id.is_some();
    let env_keys = (env.var)("AWS_ACCESS_KEY_ID").is_some();
    let profile = (env.var)("AWS_PROFILE");
    let shared_credentials = env.home.as_ref().is_some_and(|home| {
        (env.exists)(&home.join(".aws/credentials")) || (env.exists)(&home.join(".aws/config"))
    });
    // A role rather than a key: ECS and Fargate hand credentials to a task over a
    // loopback endpoint, and EKS hands them over as a projected web identity token.
    // Neither leaves a key in the environment or a file in `~/.aws`, so a check for
    // those alone would find nothing on exactly the machines that are most likely to be
    // reading from S3 in the first place. `object_store` resolves both.
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
        // An EC2 instance role is the one credential source with no local evidence at
        // all: the only way to know is to ask the instance metadata service, which is a
        // network request to a link-local address that hangs rather than refuses on some
        // networks. Doing that at startup on every machine to answer a question that is
        // "no" almost everywhere is not a trade worth making, so an instance role is not
        // discovered. Opening a URL still works; only the listing is missing, and
        // setting AWS_PROFILE or writing an ~/.aws/config is enough to bring it back.
        return None;
    };

    // Naming the service would be a guess. An endpoint is evidence that this is not
    // AWS; it is not evidence of which of the dozen S3-compatible services it is, and
    // labelling somebody's Ceph cluster "MinIO" is worse than not labelling it.
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

/// The host and port of an endpoint URL, for display. Returns `None` for anything that
/// does not look like a URL, so a malformed config line shows nothing rather than
/// putting its raw contents in a section title.
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

/// Bucket names from a Google Cloud Storage `storage/v1/b` response.
///
/// Tolerant on purpose. A response missing `items` means the project has no buckets,
/// which is an answer rather than a failure, and an entry without a usable `name` is
/// skipped rather than allowed to discard the rest of the page.
pub fn parse_gcs_buckets(body: &str) -> Result<Vec<String>, String> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("not JSON: {e}"))?;

    // An error response is JSON too, and its message is far more useful than "no
    // buckets found" would be.
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

/// Bucket names from an S3 `ListBuckets` response.
///
/// The body is XML from a service the user pointed datui at, which for a custom
/// endpoint is not necessarily a service they control. It is parsed with quick-xml
/// rather than by hand for that reason, and the parser is told to stop at a depth no
/// legitimate response reaches, so a hostile endpoint cannot answer with a billion
/// nested elements.
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
                    // An Error document, which arrives with a 200 often enough to be
                    // worth reading rather than assuming a well-formed list.
                    (Some(b"Message"), Some(b"Error")) => error_message = Some(value),
                    _ => {}
                }
            }
            Ok(Event::Eof) => {
                // quick-xml reaches Eof happily on a truncated document, reporting
                // whatever it managed to read. A body that ends mid-element is a
                // truncated response, and reporting the buckets found before the cut as
                // though they were the whole list is the one outcome worth refusing.
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

/// No `ListBuckets` response has elements this deep. The cap exists so a response that
/// does cannot be used to exhaust memory in the parser.
const MAX_XML_DEPTH: usize = 32;

/// The innermost two element names of a path, for matching a leaf in its parent.
fn path_tail(path: &[Vec<u8>]) -> (Option<&[u8]>, Option<&[u8]>) {
    let len = path.len();
    let last = len.checked_sub(1).map(|i| path[i].as_slice());
    let parent = len.checked_sub(2).map(|i| path[i].as_slice());
    (last, parent)
}

/// How long any single cloud request may take before it is abandoned.
///
/// Bounded globally rather than per socket. A server that accepts the connection and
/// then trickles one byte every twenty seconds defeats a read timeout and would hold a
/// worker indefinitely; `timeout_global` covers the whole exchange, which is the only
/// bound that actually ends.
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

pub(crate) fn http_agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(REQUEST_TIMEOUT))
        .build()
        .into()
}

/// The S3 builder datui uses everywhere, so that every path authenticates identically.
///
/// This existing separately matters more than it looks. The bucket listing needs the
/// concrete `AmazonS3` in order to ask it for a credential, while opening an object
/// needs only `dyn ObjectStore`; when the two built their own builders, the listing
/// quietly dropped the configured endpoint and keys and authenticated from the
/// environment instead. Against MinIO that fails every time, and against AWS it would
/// silently use whichever account the environment happened to name.
///
/// `settings` are one source's (`cloud_sources::resolve`); nothing here reads the
/// config or the command line again. The builder is addressed by bucket name, so only
/// `s3://bucket/key` URLs are served; a virtual-hosted URL would need `with_url`, which
/// nothing in datui produces.
pub fn s3_builder(bucket: &str, settings: &S3Settings) -> object_store::aws::AmazonS3Builder {
    // Only the default source borrows the shell's AWS variables. A source from the
    // config names its own keys, and filling its gaps from the environment would sign
    // its requests as whoever the shell happens to be.
    let builder = if settings.from_env && !settings.skip_signature {
        object_store::aws::AmazonS3Builder::from_env()
    } else {
        object_store::aws::AmazonS3Builder::new()
    };
    let mut builder = builder.with_bucket_name(bucket);
    if settings.skip_signature {
        builder = builder.with_skip_signature(true);
    }
    if let Some(endpoint) = &settings.endpoint {
        // `object_store` refuses plain `http` unless told otherwise, which is exactly
        // what a MinIO container speaks; `https` endpoints are left alone.
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
    // Each on its own, as the Polars scan applies them: the generated config suggests
    // the key in the file and the secret from AWS_SECRET_ACCESS_KEY, and requiring the
    // pair here left every store but Polars' own authenticating from the environment.
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

/// The object_store path for a key as the service stores it.
///
/// `Path::from` percent-encodes characters it considers unsafe, `%` among them, which
/// is right for a name being made up and wrong for one that already exists: a key
/// `100%.csv.gz` became `100%25.csv.gz` and was then encoded again on the way out, so
/// the request asked for an object that is not there. A key from a listing or a URL is
/// taken as it is, unless it cannot be a path at all.
pub fn object_path(key: &str) -> object_store::path::Path {
    object_store::path::Path::parse(key).unwrap_or_else(|_| object_store::path::Path::from(key))
}

/// An object store for a bucket, with no key.
///
/// The store builders already in `lib.rs` require a `bucket/key` URL, because every
/// caller they had was opening one object. Listing a bucket has no key, so this builds
/// from the bucket name alone.
pub fn store_for_bucket(
    kind: ProviderKind,
    bucket: &str,
    settings: &S3Settings,
    unsigned: bool,
    google_token: Option<&str>,
    google_credentials: Option<&Path>,
) -> Result<std::sync::Arc<dyn object_store::ObjectStore>, String> {
    match kind {
        ProviderKind::Gcs => Ok(std::sync::Arc::new(gcs_store(
            bucket,
            unsigned,
            google_token,
            google_credentials,
        )?)),
        ProviderKind::S3 => {
            let store = s3_builder(bucket, settings)
                .build()
                .map_err(|e| format!("S3 is not configured: {e}"))?;
            Ok(std::sync::Arc::new(store))
        }
        ProviderKind::Azure => Err("an Azure container needs its account".to_string()),
    }
}

/// A Google Cloud Storage store for one bucket, signed as the resolver decided.
fn gcs_store(
    bucket: &str,
    unsigned: bool,
    google_token: Option<&str>,
    google_credentials: Option<&Path>,
) -> Result<object_store::gcp::GoogleCloudStorage, String> {
    // Unsigned means no credential lookup at all, so a machine with no Google login
    // never waits on a metadata service that is not there.
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
        .build()
        .map_err(|e| format!("Google Cloud Storage is not configured: {e}"))
}

/// How many entries one peek inside a folder reads: enough to tell partitions from
/// files, and a single request however large the folder is.
const PEEK_KEYS: usize = 100;

/// What a cloud folder holds, from the first page of a delimited listing of it:
/// `Hive` when its children are `key=value` partitions, `MultiFile` when they are
/// Parquet files, else `Directory`. One request; nothing is read from any object.
pub async fn peek_kind(
    url: &str,
    config: &CloudConfig,
) -> Result<crate::discover::EntryKind, String> {
    let resolved = {
        let (url, config) = (url.to_string(), config.clone());
        tokio::task::spawn_blocking(move || crate::cloud_sources::resolve(&url, &config))
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
    resolved: &crate::cloud_sources::Resolved,
) -> Result<crate::discover::EntryKind, String> {
    use object_store::list::{PaginatedListOptions, PaginatedListStore};
    let (store, prefix): (std::sync::Arc<dyn PaginatedListStore>, String) =
        if let Some((account, container, key)) = crate::source::azure_parts(&resolved.url) {
            (
                crate::azure::paginated_store(&account, &container, &resolved.azure)?,
                key,
            )
        } else {
            let (kind, bucket, key) = split_bucket_url(&resolved.url)
                .ok_or_else(|| format!("not an object-store URL: {}", resolved.url))?;
            let unsigned = resolved.signing == Signing::Unsigned;
            let store: std::sync::Arc<dyn PaginatedListStore> = match kind {
                ProviderKind::Gcs => std::sync::Arc::new(gcs_store(
                    &bucket,
                    unsigned,
                    resolved.gcloud.as_ref().map(|(_, token)| token.as_str()),
                    resolved.google_credentials.as_deref(),
                )?),
                ProviderKind::S3 => std::sync::Arc::new(
                    s3_builder(&bucket, &resolved.s3)
                        .build()
                        .map_err(|e| format!("S3 is not configured: {e}"))?,
                ),
                ProviderKind::Azure => return Err("an Azure URL names its account".to_string()),
            };
            (store, key)
        };
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
    let folders: Vec<String> = page
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
    let kind = classify_listing(&folders, &objects);
    if kind != crate::discover::EntryKind::MultiFile {
        return Ok(kind);
    }
    // The listing said these files share an extension. Whether they are one table is a
    // question only their footers answer, and the objects just listed carry the sizes
    // that make reading a footer a single ranged request.
    Ok(verified_kind(resolved, &objects).await.unwrap_or(kind))
}

/// Parquet footers read to decide whether a folder is one table.
///
/// Three is enough to catch a folder of separate tables, whose files have nothing in
/// common with each other, while costing a fraction of what counting the dataset does.
/// A folder that survives this is read as one table and its real schema union is built
/// at open time, where every footer is read.
const VERIFY_FOOTERS: usize = 3;

/// Which of a folder's files to read, spread across the listing rather than taken from
/// its head.
///
/// Keys come back in lexicographic order, so the first files of a folder written table
/// by table can easily be the same table — `circuits`, `constructor_standings`,
/// `constructors` — while its ends never are.
fn footers_to_verify(files: usize) -> Vec<usize> {
    if files <= VERIFY_FOOTERS {
        (0..files).collect()
    } else {
        vec![0, files / 2, files - 1]
    }
}

/// Whether a folder the listing called `multi` holds one table, from a few of its
/// footers. `None` when it could not be decided, and the listing's answer stands: the
/// optimistic reading is the reversible one.
async fn verified_kind(
    resolved: &crate::cloud_sources::Resolved,
    objects: &[(String, u64)],
) -> Option<crate::discover::EntryKind> {
    // Azure lists through a different store, which `store_for_bucket` cannot build.
    if crate::source::azure_parts(&resolved.url).is_some() {
        return None;
    }
    let (provider, bucket, _) = split_bucket_url(&resolved.url)?;
    let store = store_for_bucket(
        provider,
        &bucket,
        &resolved.s3,
        resolved.signing == Signing::Unsigned,
        resolved.gcloud.as_ref().map(|(_, token)| token.as_str()),
        resolved.google_credentials.as_deref(),
    )
    .ok()?;
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
    let picks = footers_to_verify(parquet.len());

    let meter = std::sync::Arc::new(crate::measurements::Meter::default());
    let store = store.clone();
    let mut reads = tokio::task::JoinSet::new();
    for index in picks {
        let (key, size) = parquet[index].clone();
        let (store, meter) = (store.clone(), meter.clone());
        reads.spawn(async move {
            let file = crate::cloud_hive::DatasetFile {
                key,
                size,
                stamp: 0,
                etag: None,
            };
            crate::cloud_hive::footer_of_file(&store, &file, &meter)
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
    // One readable footer says nothing about agreement, and none says nothing at all.
    if per_file.len() < 2 {
        return None;
    }
    Some(if crate::schema_union::is_one_table(&per_file) {
        crate::discover::EntryKind::MultiFile
    } else {
        crate::discover::EntryKind::Directory
    })
}

/// A folder's kind from what one listing of it shows, by the rules a local folder is
/// classified by, except that only Parquet counts as data: it is the one format a
/// prefix of files is read in place as.
pub fn classify_listing(
    folders: &[String],
    objects: &[(String, u64)],
) -> crate::discover::EntryKind {
    use crate::discover::EntryKind;
    let last = |key: &str| {
        key.trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or("")
            .to_string()
    };
    let partitions = folders
        .iter()
        .filter(|f| matches!(last(f).find('='), Some(i) if i > 0))
        .count();
    // Data, not the markers and job files tools leave beside it.
    let files: Vec<&String> = objects
        .iter()
        .filter(|(key, size)| {
            let name = last(key);
            !name.is_empty()
                && !name.starts_with('.')
                && !is_job_file(&name)
                && !is_empty_marker(&name, *size)
                && !(*size == 0 && folders.iter().any(|f| last(f) == name))
        })
        .map(|(key, _)| key)
        .collect();
    // A lake table first: its data files genuinely agree on a schema, so every rule
    // below says "one table" and is right about the schema and wrong about the rows.
    // The markers are prefixes in the listing that already happened, so this costs
    // nothing.
    let folder = |name: &str| folders.iter().any(|f| last(f) == name);
    if folder("_delta_log") {
        return EntryKind::Delta;
    }
    if folder(".hoodie") {
        return EntryKind::Hudi;
    }
    // Iceberg's marker is a plain name, so it takes the whole shape rather than the
    // name alone: `metadata/` beside `data/`, and nothing else of the table's at the
    // root. Whether `metadata/` holds a `*.metadata.json` is not asked — that is a
    // second listing, and this is the layout the spec describes.
    if folder("metadata") && folder("data") && files.is_empty() {
        return EntryKind::Iceberg;
    }
    let parquet = files
        .iter()
        .filter(|key| crate::discover::is_parquet_key(key))
        .count();
    if partitions > 0 && partitions >= files.len() {
        return EntryKind::Hive;
    }
    let seen = folders.len() + files.len();
    if parquet > 1 && parquet == files.len() && parquet * 2 >= seen {
        EntryKind::MultiFile
    } else {
        EntryKind::Directory
    }
}

/// Split a `gs://` or `s3://` URL into its bucket and the prefix inside it.
///
/// The prefix comes back without a leading or trailing slash, and empty for the bucket
/// root, which is the shape `object_store` wants. A source ID (`s3://<id>@bucket`) is
/// not part of the bucket and is dropped.
pub fn split_bucket_url(url: &str) -> Option<(ProviderKind, String, String)> {
    let (_, plain) = crate::source::split_source_id(url);
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

/// One level of a bucket or prefix, as home-screen rows.
///
/// Uses a delimited listing, so a bucket holding a million objects under a hundred
/// prefixes costs one request and returns a hundred rows. A recursive listing of the
/// same bucket would be the wrong thing in every dimension: slower, larger, billed by
/// the request, and unreadable on screen.
pub async fn list_objects(
    url: &str,
    config: &CloudConfig,
) -> Result<Vec<crate::discover::Entry>, String> {
    // Resolving can run a credential command, which blocks; keep it off the runtime's
    // own threads.
    let resolved = {
        let (url, config) = (url.to_string(), config.clone());
        tokio::task::spawn_blocking(move || crate::cloud_sources::resolve(&url, &config))
            .await
            .map_err(|e| format!("{e}"))??
    };
    let signing = resolved.signing;
    let place = resolved.place.clone();
    let listed = list_level(url, &resolved).await;
    // A sign-in with no data role on an Azure account: its keys, as the Portal does.
    let (listed, resolved) = match listed {
        Err(refusal)
            if resolved.kind == ProviderKind::Azure
                && crate::azure::is_permission_mismatch(&refusal)
                && resolved.azure.identity.is_some() =>
        {
            let enabled = config.azure_account_keys != Some(false);
            let keyed = {
                let (resolved, refusal) = (resolved.clone(), refusal.clone());
                tokio::task::spawn_blocking(move || {
                    let (account, _, _) =
                        crate::source::azure_parts(&resolved.url).ok_or_else(|| refusal.clone())?;
                    crate::azure::with_account_key(
                        &account,
                        &resolved.azure,
                        &refusal,
                        enabled,
                        &Environment::current(),
                    )
                    .map(|azure| crate::cloud_sources::Resolved { azure, ..resolved })
                })
                .await
                .map_err(|e| format!("{e}"))?
            };
            match keyed {
                Ok(keyed) => (list_level(url, &keyed).await, keyed),
                Err(why) => (Err(why), resolved),
            }
        }
        other => (other, resolved),
    };
    if resolved.kind == ProviderKind::Azure
        && listed.is_ok()
        && matches!(resolved.azure.auth, crate::azure::AzureAuth::Bearer(_))
        && let Some((account, _, _)) = crate::source::azure_parts(&resolved.url)
    {
        crate::azure::remember_token_reads(&account);
    }
    match listed {
        Err(refused) if signing == Signing::Try && is_refusal(&refused) => {
            // Perhaps public, and refused only because the request was signed by a
            // login from somewhere else.
            let rows = list_level(url, &resolved.unsigned())
                .await
                .map_err(|_| refused)?;
            crate::cloud_sources::remember_access(&place, true);
            Ok(rows)
        }
        Ok(rows) => {
            match signing {
                Signing::Try => crate::cloud_sources::remember_access(&place, false),
                // Read with no login, and not one of the public datasets already.
                Signing::Unsigned if !is_public_source(&resolved.source_id, config) => {
                    crate::cloud_sources::found_public(&place)
                }
                _ => {}
            }
            Ok(rows)
        }
        // Unsigned because the login failed, and refused: the login is what to fix.
        Err(refused) if is_refusal(&refused) && resolved.login_error.is_some() => {
            Err(resolved.login_error.clone().unwrap_or(refused))
        }
        Err(e) => Err(e),
    }
}

/// Whether `id` names a source of public datasets, whose places are listed already.
fn is_public_source(id: &str, config: &CloudConfig) -> bool {
    id == crate::cloud_sources::PUBLIC
        || config
            .sources
            .iter()
            .any(|s| s.name == id && s.public == Some(true))
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

/// Files that jobs leave beside their output, and markers that stand in for folders.
/// Neither is data, and neither is worth a row.
pub fn is_job_file(name: &str) -> bool {
    name == "_SUCCESS"
        || name.starts_with("_committed_")
        || name.starts_with("_started_")
        || name.ends_with("_$folder$")
}

/// An empty object with no extension: a marker some tool left for a folder, whether or
/// not the folder still has anything in it (`yellow/year=2032` beside no `year=2032/`).
/// Nothing datui opens is both empty and nameless.
pub fn is_empty_marker(name: &str, size: u64) -> bool {
    size == 0 && !name.contains('.')
}

/// One level of a place, signed or not as `resolved` says.
async fn list_level(
    url: &str,
    resolved: &crate::cloud_sources::Resolved,
) -> Result<Vec<crate::discover::Entry>, String> {
    if resolved.kind == ProviderKind::Azure {
        return list_azure_objects(resolved).await;
    }
    let (kind, bucket, prefix) =
        split_bucket_url(&resolved.url).ok_or_else(|| format!("not an object-store URL: {url}"))?;
    let store = store_for_bucket(
        kind,
        &bucket,
        &resolved.s3,
        resolved.signing == Signing::Unsigned,
        resolved.gcloud.as_ref().map(|(_, token)| token.as_str()),
        resolved.google_credentials.as_deref(),
    )?;

    let os_prefix = if prefix.is_empty() {
        None
    } else {
        Some(object_path(&prefix))
    };
    let result = store
        .list_with_delimiter(os_prefix.as_ref())
        .await
        .map_err(|e| format!("{e}"))?;

    // Rows keep the source the listing was asked for, so opening one reaches the same
    // server.
    let base = match crate::source::split_source_id(url).0 {
        Some(id) => format!("{}://{id}@{bucket}", kind.scheme()),
        None => format!("{}://{bucket}", kind.scheme()),
    };
    let mut rows = Vec::new();

    // Prefixes first. They are the directories of an object store, and putting them
    // above the objects matches what every local listing does.
    let prefixes: Vec<String> = result
        .common_prefixes
        .iter()
        .map(|p| p.as_ref().to_string())
        .collect();
    for common in result.common_prefixes {
        let name = common
            .as_ref()
            .rsplit('/')
            .find(|part| !part.is_empty())
            .unwrap_or(common.as_ref())
            .to_string();
        rows.push(crate::discover::Entry {
            path: PathBuf::from(format!("{base}/{}", common.as_ref())),
            kind: crate::discover::EntryKind::Directory,
            name,
            size: None,
            modified: None,
            rows: None,
            cols: None,
            columns: Vec::new(),
            cost: Default::default(),
        });
    }

    for object in result.objects {
        let location = object.location.as_ref().to_string();
        let name = location.rsplit('/').next().unwrap_or(&location).to_string();
        // A key ending in a slash is how consoles fake a folder. It is not data, and
        // offering it as openable would be offering a zero-byte file. So is an empty
        // object named like a folder beside it, or like the folder being listed.
        if name.is_empty()
            || is_job_file(&name)
            || crate::azure::is_folder_marker(&location, object.size, &prefixes)
            || is_empty_marker(&name, object.size)
            || (object.size == 0 && location.trim_end_matches('/') == prefix)
        {
            continue;
        }
        rows.push(crate::discover::Entry {
            path: PathBuf::from(format!("{base}/{location}")),
            kind: crate::discover::EntryKind::File,
            name,
            size: Some(object.size),
            modified: Some(object.last_modified.into()),
            rows: None,
            cols: None,
            columns: Vec::new(),
            cost: Default::default(),
        });
    }

    Ok(rows)
}

/// One level of an Azure container or folder. Accounts with hierarchical namespace list
/// each folder as a prefix and as an empty blob of the same name; only the prefix is
/// kept.
async fn list_azure_objects(
    resolved: &crate::cloud_sources::Resolved,
) -> Result<Vec<crate::discover::Entry>, String> {
    let (account, container, prefix) = crate::source::azure_parts(&resolved.url)
        .ok_or_else(|| format!("not an Azure URL: {}", resolved.url))?;
    let store = crate::azure::store(&account, &container, &resolved.azure)?;
    let prefix = prefix.trim_matches('/').to_string();
    let os_prefix = (!prefix.is_empty()).then(|| object_path(&prefix));
    let result = store
        .list_with_delimiter(os_prefix.as_ref())
        .await
        .map_err(|e| format!("{e}"))?;

    let prefixes: Vec<String> = result
        .common_prefixes
        .iter()
        .map(|p| p.as_ref().to_string())
        .collect();
    let mut rows = Vec::new();
    for common in &prefixes {
        let name = common
            .rsplit('/')
            .find(|part| !part.is_empty())
            .unwrap_or(common)
            .to_string();
        let mut row = crate::discover::Entry::directory(Path::new(&crate::source::azure_url(
            &account,
            &container,
            &format!("{common}/"),
        )));
        row.name = name;
        rows.push(row);
    }
    for object in result.objects {
        let location = object.location.as_ref().to_string();
        let name = location.rsplit('/').next().unwrap_or(&location).to_string();
        if name.is_empty()
            || is_job_file(&name)
            || crate::azure::is_folder_marker(&location, object.size, &prefixes)
            || is_empty_marker(&name, object.size)
            || (object.size == 0 && location.trim_end_matches('/') == prefix)
        {
            continue;
        }
        rows.push(crate::discover::Entry {
            path: PathBuf::from(crate::source::azure_url(&account, &container, &location)),
            kind: crate::discover::EntryKind::File,
            name,
            size: Some(object.size),
            modified: Some(object.last_modified.into()),
            rows: None,
            cols: None,
            columns: Vec::new(),
            cost: Default::default(),
        });
    }
    Ok(rows)
}

/// A source's first level, as the home screen lists it: buckets for S3 and Google
/// Cloud, storage accounts for Azure.
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
    // Known in advance: nothing to ask anyone.
    if source.public {
        return Ok(source
            .datasets
            .iter()
            .map(|dataset| Listed {
                name: dataset.name.clone(),
                place: PathBuf::from(&dataset.url),
                details: dataset_details(dataset),
            })
            .collect());
    }
    if source.kind == ProviderKind::Gcs {
        let source = source.clone();
        return tokio::task::spawn_blocking(move || {
            tokio::runtime::Handle::current().block_on(list_gcs_projects(&source))
        })
        .await
        .map_err(|e| format!("{e}"))?;
    }
    if source.kind != ProviderKind::Azure {
        // The bucket listings send their request with a blocking client. On a thread
        // of its own, a server that never answers holds up only its own source, not
        // one of the runtime's few workers and everything queued behind it.
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
            crate::azure::discover_accounts(&source.azure.auth, &Environment::current())?;
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

/// Details-pane lines for a public dataset.
pub fn dataset_details(dataset: &crate::cloud_sources::Dataset) -> Vec<(String, String)> {
    [
        ("about", &dataset.description),
        ("publisher", &dataset.publisher),
        ("license", &dataset.license),
        ("homepage", &dataset.homepage),
        ("url", &dataset.url),
    ]
    .into_iter()
    .filter(|(_, value)| !value.is_empty())
    .map(|(key, value)| (key.to_string(), value.clone()))
    .collect()
}

/// Where Amazon S3 keeps `bucket`, from the `x-amz-bucket-region` header S3 sends with
/// no credentials, whatever the status. Asked once per bucket per session, and `None`
/// when S3 does not say.
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
/// redirects are not followed.
fn probe_agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(10)))
        .http_status_as_error(false)
        .max_redirects(0)
        .build()
        .into()
}

/// Whether the place `resolved` points at can be read with no signature: `Some(true)`
/// when an unsigned request succeeds, `Some(false)` when it is refused, `None` when the
/// answer says neither (no network, a missing object, a custom endpoint).
///
/// One request: a `HEAD` of an object, or a one-key listing of a prefix.
pub fn probe_unsigned(resolved: &crate::cloud_sources::Resolved) -> Option<bool> {
    let url = probe_url(resolved)?;
    let agent = probe_agent();
    let mut request = if url.contains('?') {
        agent.get(&url)
    } else {
        agent.head(&url)
    };
    if resolved.kind == ProviderKind::Azure {
        request = request.header("x-ms-version", crate::azure::API_VERSION);
    }
    let response = request.call().ok()?;
    match response.status().as_u16() {
        200..=299 => Some(true),
        401 | 403 => Some(false),
        _ => None,
    }
}

/// The plain HTTPS URL for an unsigned look at `resolved`'s place. `None` for a custom
/// endpoint or an emulator, which are left to the signed path.
fn probe_url(resolved: &crate::cloud_sources::Resolved) -> Option<String> {
    let encode = |key: &str| key.split('/').map(urlencode).collect::<Vec<_>>().join("/");
    // The object, or for a prefix or glob the folder part to list one key from.
    let split = |key: &str| -> (String, bool) {
        let before_glob = key.split('*').next().unwrap_or("");
        if key.contains('*') || key.is_empty() || key.ends_with('/') {
            let folder = match before_glob.rsplit_once('/') {
                Some((folder, _)) => format!("{folder}/"),
                None => String::new(),
            };
            (folder, true)
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
                (folder, true) => format!(
                    "{base}/?list-type=2&max-keys=1&prefix={}",
                    urlencode(&folder)
                ),
                (object, false) => format!("{base}/{}", encode(&object)),
            })
        }
        ProviderKind::Gcs => {
            let (_, bucket, _) = split_bucket_url(&resolved.url)?;
            let key = resolved.url.split_once("://")?.1;
            let key = key.split_once('/').map_or("", |(_, key)| key);
            Some(match split(key) {
                (folder, true) => format!(
                    "https://storage.googleapis.com/storage/v1/b/{bucket}/o?maxResults=1&prefix={}",
                    urlencode(&folder)
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
            let (account, container, key) = crate::source::azure_parts(&resolved.url)?;
            let base = format!("https://{account}.blob.core.windows.net/{container}");
            Some(match split(&key) {
                (folder, true) => format!(
                    "{base}?restype=container&comp=list&maxresults=1&prefix={}",
                    urlencode(&folder)
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
            crate::cloud_sources::discover(&config, &Environment::current())
                .into_iter()
                .find(|s| s.id == source_id)
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
                crate::cloud_sources::remember_bucket(&source, &bucket);
                let mut entry =
                    crate::discover::Entry::directory(Path::new(&format!("gs://{bucket}")));
                entry.name = bucket;
                entry
            })
            .collect());
    }
    tokio::task::spawn_blocking(move || {
        let env = Environment::current();
        if source.kind != ProviderKind::Azure {
            return Err(format!("{source_id} has no accounts"));
        }
        let settings = source.azure.with_token(&env)?;
        let containers = crate::azure::list_containers(&account, &settings)?;
        Ok(containers
            .into_iter()
            .map(|container| {
                let mut entry = crate::discover::Entry::directory(Path::new(
                    &crate::source::azure_url(&account, &container, ""),
                ));
                entry.name = container;
                entry
            })
            .collect())
    })
    .await
    .map_err(|e| format!("{e}"))?
}

/// Every bucket the provider's credentials can see.
///
/// Enumeration is per-provider because `object_store` is deliberately bucket-scoped:
/// it will read and write objects but has no notion of "list the buckets". Both
/// implementations below borrow that crate's credential handling rather than
/// reimplementing a token exchange or a SigV4 signer, so there is no new cryptography
/// here and, more importantly, the credentials used to list are the same ones used to
/// open.
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

/// GCS buckets, through the JSON API.
///
/// The bearer token comes from the store's own credential provider. Building a store
/// requires a bucket name, and there is no bucket yet — that is what is being asked —
/// so a placeholder is used. Nothing is addressed with it: the store is built only to
/// be asked for a credential, and the request below goes to the project-scoped bucket
/// listing endpoint.
async fn list_gcs_buckets(source: &Source) -> Result<Vec<String>, String> {
    let project = source.project.as_deref().ok_or_else(|| {
        "no GCP project is set, so there is nothing to list buckets for. Set \
         GOOGLE_CLOUD_PROJECT or DATUI_GCP_PROJECT."
            .to_string()
    })?;
    let bearer = google_bearer(source).await?;

    let mut buckets = Vec::new();
    let mut page_token: Option<String> = None;
    // Bounded rather than "while there is a token". A paginating API that keeps
    // handing back a token is a loop, and this runs on a worker nobody is watching.
    for _ in 0..MAX_BUCKET_PAGES {
        let mut url = format!(
            "https://storage.googleapis.com/storage/v1/b?project={}&maxResults=1000",
            urlencode(project)
        );
        if let Some(token) = &page_token {
            url.push_str(&format!("&pageToken={}", urlencode(token)));
        }
        let mut response = http_agent()
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
            return Err(crate::gcloud::describe_error(status, &body));
        }

        buckets.extend(parse_gcs_buckets(&body)?);
        match gcs_next_page_token(&body) {
            Some(token) => page_token = Some(token),
            None => break,
        }
    }

    buckets.sort();
    Ok(buckets)
}

/// A bearer token for a Google source: from `gcloud` when it logs in through a
/// configuration, else from object_store's own credential chain.
async fn google_bearer(source: &Source) -> Result<String, String> {
    if let Some(problem) = &source.problem {
        return Err(problem.clone());
    }
    if let Some(configuration) = source.gcloud.clone() {
        return tokio::task::spawn_blocking(move || {
            crate::gcloud::token(&configuration, &Environment::current()).map(|(token, _)| token)
        })
        .await
        .map_err(|e| format!("{e}"))?;
    }
    // Building a store needs a bucket name, and there is none: the store is built only
    // to be asked for a credential.
    let builder = match &source.google_credentials {
        Some(file) => object_store::gcp::GoogleCloudStorageBuilder::new()
            .with_application_credentials(file.to_string_lossy()),
        None => object_store::gcp::GoogleCloudStorageBuilder::from_env(),
    };
    let store = builder
        .with_bucket_name("datui-credential-probe")
        .build()
        .map_err(|e| format!("Google Cloud Storage is not configured: {e}"))?;
    store
        .credentials()
        .get_credential()
        .await
        .map(|credential| credential.bearer.clone())
        .map_err(|e| format!("could not obtain Google credentials: {e}"))
}

/// A Google source's projects, as the first level: every project Resource Manager
/// finds, with the configured project first. When projects cannot be searched (no
/// permission, or an application-default login without a quota project), the
/// configured project alone.
async fn list_gcs_projects(source: &Source) -> Result<Vec<Listed>, String> {
    let bearer = google_bearer(source).await?;
    let searched = {
        let bearer = bearer.clone();
        tokio::task::spawn_blocking(move || crate::gcloud::search_projects(&bearer))
            .await
            .map_err(|e| format!("{e}"))?
    };
    let mut projects = match (searched, &source.project) {
        (Ok(projects), _) => projects,
        (Err(_), Some(project)) => vec![crate::gcloud::Project {
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
                crate::gcloud::Project {
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

/// S3 buckets, through `ListBuckets` on the endpoint root.
///
/// Signed with `object_store`'s own `AwsAuthorizer`, which is the SigV4 implementation
/// the rest of datui's S3 access already relies on. Hand-rolling a signer for this one
/// request would be both more code and a worse idea.
async fn list_s3_buckets(settings: &S3Settings) -> Result<Vec<String>, String> {
    use object_store::aws::AwsAuthorizer;

    // Same placeholder-bucket reasoning as the GCS path: the store exists to hold
    // credentials and a region, and `ListBuckets` is not addressed to a bucket.
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

    // Signed as an `http::Request`, which is what the authorizer understands, and then
    // replayed onto the agent datui already uses. The alternative is a second HTTP
    // client in the tree for the sake of one request.
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

/// Where `ListBuckets` is sent: the root of the effective endpoint, AWS when there is
/// none. The same endpoint `s3_builder` opens objects against, so the section title,
/// the listing and the open all name one host.
fn s3_list_buckets_url(settings: &S3Settings) -> String {
    let endpoint = settings
        .endpoint
        .as_deref()
        .unwrap_or("https://s3.amazonaws.com");
    format!("{}/", endpoint.trim_end_matches('/'))
}

/// A paginating API that never stops handing back a token is a loop. Twenty pages of a
/// thousand buckets is far past any real account.
const MAX_BUCKET_PAGES: usize = 20;

/// Percent-encode a query parameter value.
///
/// A project id or page token goes into a URL, and neither is guaranteed to be free of
/// characters that mean something there. Small and local rather than a new dependency
/// for two call sites.
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
mod tests {
    use super::*;

    fn resolved(url: &str, kind: ProviderKind) -> crate::cloud_sources::Resolved {
        crate::cloud_sources::Resolved {
            url: url.to_string(),
            kind,
            source_id: String::new(),
            s3: S3Settings::default(),
            azure: Default::default(),
            signing: Signing::Try,
            place: crate::cloud_sources::access_key(url).unwrap(),
            gcloud: None,
            google_credentials: None,
            login_error: None,
        }
    }

    #[test]
    fn the_unsigned_look_is_one_small_request() {
        let mut s3 = resolved("s3://aws-public-blockchain/v1.0/btc/", ProviderKind::S3);
        s3.s3.region = Some("us-east-2".to_string());
        assert_eq!(
            probe_url(&s3).unwrap(),
            "https://aws-public-blockchain.s3.us-east-2.amazonaws.com/?list-type=2&max-keys=1&prefix=v1.0%2Fbtc%2F"
        );
        let object = resolved("s3://my.dotted.bucket/a b/100%.parquet", ProviderKind::S3);
        assert_eq!(
            probe_url(&object).unwrap(),
            "https://s3.us-east-1.amazonaws.com/my.dotted.bucket/a%20b/100%25.parquet"
        );
        let glob = resolved("gs://bucket/year=*/part-*.parquet", ProviderKind::Gcs);
        assert_eq!(
            probe_url(&glob).unwrap(),
            "https://storage.googleapis.com/storage/v1/b/bucket/o?maxResults=1&prefix="
        );
        let azure = resolved(
            "abfss://release@overturemapswestus2.dfs.core.windows.net/2026-08-19.0/",
            ProviderKind::Azure,
        );
        assert_eq!(
            probe_url(&azure).unwrap(),
            "https://overturemapswestus2.blob.core.windows.net/release?restype=container&comp=list&maxresults=1&prefix=2026-08-19.0%2F"
        );
        let mut minio = resolved("s3://data/x.parquet", ProviderKind::S3);
        minio.s3.endpoint = Some("http://127.0.0.1:9000".to_string());
        assert_eq!(probe_url(&minio), None, "a custom endpoint is not probed");
    }

    #[test]
    fn a_folder_is_classified_by_one_page_of_its_listing() {
        use crate::discover::EntryKind;
        let folders = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        let files = |names: &[(&str, u64)]| {
            names
                .iter()
                .map(|(n, s)| (n.to_string(), *s))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            classify_listing(
                &folders(&[
                    "v1.0/btc/blocks/date=2009-01-03/",
                    "v1.0/btc/blocks/date=2009-01-09/"
                ]),
                &files(&[("v1.0/btc/blocks/_SUCCESS", 0)]),
            ),
            EntryKind::Hive
        );
        assert_eq!(
            classify_listing(
                &folders(&[]),
                &files(&[
                    ("gbif/occurrence.parquet/000001", 10),
                    ("gbif/occurrence.parquet/000002", 10)
                ]),
            ),
            EntryKind::MultiFile,
            "part files with no extension"
        );
        assert_eq!(
            classify_listing(&folders(&[]), &files(&[("a/x.csv", 5), ("a/y.csv", 5)])),
            EntryKind::Directory,
            "CSV cannot be read in place as one table"
        );
        assert_eq!(
            classify_listing(&folders(&["a/by_year/", "a/by_station/"]), &files(&[])),
            EntryKind::Directory
        );
        assert_eq!(
            classify_listing(&folders(&["a/b/"]), &files(&[("a/one.parquet", 5)])),
            EntryKind::Directory,
            "one file is a file to open, not a dataset"
        );
    }

    /// A lake table's data files agree on a schema, so the one-table rule says `multi`
    /// and is right about the schema and wrong about the rows.
    #[test]
    fn a_lake_table_is_not_a_folder_of_parquet_files() {
        use crate::discover::EntryKind;
        let folders = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        let files = |names: &[(&str, u64)]| {
            names
                .iter()
                .map(|(n, s)| (n.to_string(), *s))
                .collect::<Vec<_>>()
        };
        let parts = files(&[
            ("t/part-00000.parquet", 10),
            ("t/part-00001.parquet", 10),
            ("t/part-00002.parquet", 10),
        ]);

        assert_eq!(
            classify_listing(&folders(&["t/_delta_log/"]), &parts),
            EntryKind::Delta
        );
        assert_eq!(
            classify_listing(&folders(&["t/.hoodie/"]), &parts),
            EntryKind::Hudi
        );
        assert_eq!(
            classify_listing(&folders(&["t/metadata/", "t/data/"]), &files(&[])),
            EntryKind::Iceberg
        );

        // The plain name alone is not the marker.
        assert_eq!(
            classify_listing(&folders(&["t/metadata/"]), &parts),
            EntryKind::MultiFile,
            "a folder called metadata beside part files is not an Iceberg table"
        );
        assert_eq!(
            classify_listing(&folders(&["t/metadata/", "t/data/"]), &parts),
            EntryKind::MultiFile,
            "an Iceberg root holds no data files of its own"
        );
        assert_eq!(
            classify_listing(&folders(&[]), &parts),
            EntryKind::MultiFile,
            "and a folder of part files with no log is still one table"
        );
    }

    #[test]
    fn refusals_and_job_files() {
        assert!(is_refusal(
            "Client error with status 403 Forbidden: <Code>AccessDenied</Code>"
        ));
        assert!(is_refusal(
            "Server returned 401 NoAuthenticationInformation"
        ));
        assert!(!is_refusal("error sending request: connection refused"));
        for name in [
            "_SUCCESS",
            "_committed_123",
            "_started_123",
            "yellow_$folder$",
        ] {
            assert!(is_job_file(name), "{name}");
        }
        assert!(!is_job_file("part-0000.parquet"));
        assert!(is_empty_marker("year=2032", 0));
        assert!(!is_empty_marker("year=2032", 10));
        assert!(!is_empty_marker("empty.csv", 0));
    }

    #[test]
    fn a_key_is_taken_as_the_service_stores_it() {
        assert_eq!(object_path("edge/100%.csv.gz").as_ref(), "edge/100%.csv.gz");
        assert_eq!(
            object_path("edge/a+b=c&d#e.parquet").as_ref(),
            "edge/a+b=c&d#e.parquet"
        );
        assert_eq!(
            object_path("edge/name with spaces").as_ref(),
            "edge/name with spaces"
        );
        // Not a path as it stands: made into one the way object_store does.
        assert_eq!(
            object_path("a/../b").as_ref(),
            object_store::path::Path::from("a/../b").as_ref()
        );
    }
    use std::collections::HashMap;

    /// An environment built from literals, so a test says exactly what the machine
    /// looks like and nothing leaks in from the machine running it.
    fn env_of(
        vars: &[(&str, &str)],
        files: &[&str],
        home: Option<&str>,
    ) -> (HashMap<String, String>, Vec<PathBuf>, Option<PathBuf>) {
        (
            vars.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            files.iter().map(PathBuf::from).collect(),
            home.map(PathBuf::from),
        )
    }

    macro_rules! environment {
        ($vars:expr_2021, $files:expr_2021, $home:expr_2021) => {
            Environment {
                var: &|key| $vars.get(key).cloned(),
                exists: &|path| $files.iter().any(|f: &PathBuf| f == path),
                read: &|_| None,
                home: $home.clone(),
                windows: false,
                run: &|_, _| {
                    Err(crate::cloud_command::CommandError::Missing(
                        "test".to_string(),
                    ))
                },
                all_vars: &|| Vec::new(),
                list: &|_| Vec::new(),
            }
        };
        ($vars:expr_2021, $files:expr_2021, $home:expr_2021, $contents:expr_2021) => {
            Environment {
                var: &|key| $vars.get(key).cloned(),
                exists: &|path| $files.iter().any(|f: &PathBuf| f == path),
                read: &|_| Some($contents.to_string()),
                home: $home.clone(),
                windows: false,
                run: &|_, _| {
                    Err(crate::cloud_command::CommandError::Missing(
                        "test".to_string(),
                    ))
                },
                all_vars: &|| Vec::new(),
                list: &|_| Vec::new(),
            }
        };
    }

    #[test]
    fn nothing_configured_finds_nothing() {
        let (vars, files, home) = env_of(&[], &[], Some("/home/u"));
        let env = environment!(vars, files, home);
        assert!(detect(&CloudConfig::default(), &env).is_empty());
    }

    #[test]
    fn gcloud_default_credentials_are_enough_to_list_gcs() {
        let (vars, files, home) = env_of(
            &[("GOOGLE_CLOUD_PROJECT", "example-project")],
            &["/home/u/.config/gcloud/application_default_credentials.json"],
            Some("/home/u"),
        );
        let env = environment!(vars, files, home);
        let found = detect(&CloudConfig::default(), &env);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].kind, ProviderKind::Gcs);
        assert_eq!(found[0].project.as_deref(), Some("example-project"));
        assert!(found[0].can_list_buckets());
        assert_eq!(found[0].note, "gcloud");
        assert_eq!(
            found[0].detail().as_deref(),
            Some("project: example-project")
        );
    }

    #[test]
    fn an_aws_profile_is_the_detail_for_s3() {
        let (vars, files, home) = env_of(&[("AWS_PROFILE", "research")], &[], Some("/home/u"));
        let env = environment!(vars, files, home);
        let found = detect(&CloudConfig::default(), &env);
        assert_eq!(found[0].detail().as_deref(), Some("profile: research"));
    }

    #[test]
    fn gcs_without_a_project_is_shown_but_cannot_enumerate() {
        // Worth keeping visible: the credentials work, so a URL the user types still
        // opens. Only the listing is impossible, and the UI can say so.
        let (vars, files, home) = env_of(
            &[],
            &["/home/u/.config/gcloud/application_default_credentials.json"],
            Some("/home/u"),
        );
        let env = environment!(vars, files, home);
        let found = detect(&CloudConfig::default(), &env);
        assert_eq!(found.len(), 1);
        assert!(found[0].project.is_none());
        assert!(!found[0].can_list_buckets());
    }

    #[test]
    fn a_service_account_outranks_the_developer_login_in_the_note() {
        let (vars, files, home) = env_of(
            &[("GOOGLE_SERVICE_ACCOUNT", "/keys/sa.json")],
            &["/home/u/.config/gcloud/application_default_credentials.json"],
            Some("/home/u"),
        );
        let env = environment!(vars, files, home);
        let found = detect(&CloudConfig::default(), &env);
        assert_eq!(found[0].note, "service account");
    }

    #[test]
    fn aws_keys_in_the_environment_are_amazon_until_an_endpoint_says_otherwise() {
        let (vars, files, home) = env_of(&[("AWS_ACCESS_KEY_ID", "AKIA")], &[], Some("/home/u"));
        let env = environment!(vars, files, home);
        let found = detect(&CloudConfig::default(), &env);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].label, "Amazon S3");
        assert_eq!(found[0].note, "AWS_ACCESS_KEY_ID");
    }

    #[test]
    fn a_custom_endpoint_is_named_by_its_host_and_not_guessed_at() {
        let config = CloudConfig {
            s3_endpoint_url: Some("http://localhost:9000".to_string()),
            s3_access_key_id: Some("minioadmin".to_string()),
            ..CloudConfig::default()
        };
        let (vars, files, home) = env_of(&[], &[], Some("/home/u"));
        let env = environment!(vars, files, home);
        let found = detect(&config, &env);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].label, "S3-compatible (localhost:9000)");
        assert_eq!(found[0].note, "datui config");
        assert_eq!(found[0].endpoint.as_deref(), Some("http://localhost:9000"));
    }

    /// The config as `run()` hands it to discovery: the file's settings with the
    /// environment folded in.
    fn effective(config: &CloudConfig, env: &Environment<'_>) -> CloudConfig {
        let mut merged = config.clone();
        merged.merge(CloudConfig::from_env(env.var));
        merged
    }

    #[test]
    fn an_endpoint_from_the_environment_counts_too() {
        let (vars, files, home) = env_of(
            &[
                ("AWS_ACCESS_KEY_ID", "minioadmin"),
                ("AWS_ENDPOINT_URL", "https://minio.internal:9000/"),
            ],
            &[],
            Some("/home/u"),
        );
        let env = environment!(vars, files, home);
        let config = effective(&CloudConfig::default(), &env);
        let found = detect(&config, &env);
        assert_eq!(found[0].label, "S3-compatible (minio.internal:9000)");
        // The bug this guards against: the title named the environment's host while
        // the listing, reading the config alone, went to AWS.
        assert_eq!(
            s3_list_buckets_url(&S3Settings::from_config(&config)),
            "https://minio.internal:9000/"
        );
    }

    #[test]
    fn the_service_specific_endpoint_variable_outranks_the_general_one() {
        let (vars, files, home) = env_of(
            &[
                ("AWS_ACCESS_KEY_ID", "k"),
                ("AWS_ENDPOINT", "http://third:1"),
                ("AWS_ENDPOINT_URL", "http://second:2"),
                ("AWS_ENDPOINT_URL_S3", "http://first:3"),
            ],
            &[],
            None,
        );
        let env = environment!(vars, files, home);
        let config = effective(&CloudConfig::default(), &env);
        assert_eq!(
            s3_list_buckets_url(&S3Settings::from_config(&config)),
            "http://first:3/"
        );
        assert_eq!(detect(&config, &env)[0].label, "S3-compatible (first:3)");
    }

    #[test]
    fn a_blank_endpoint_variable_does_not_erase_the_configured_one() {
        let (vars, files, home) = env_of(
            &[("AWS_ACCESS_KEY_ID", "k"), ("AWS_ENDPOINT_URL", "  ")],
            &[],
            None,
        );
        let env = environment!(vars, files, home);
        let file = CloudConfig {
            s3_endpoint_url: Some("http://localhost:9000".to_string()),
            ..CloudConfig::default()
        };
        let config = effective(&file, &env);
        assert_eq!(
            s3_list_buckets_url(&S3Settings::from_config(&config)),
            "http://localhost:9000/"
        );
        assert_eq!(
            detect(&config, &env)[0].label,
            "S3-compatible (localhost:9000)"
        );
        // A blank flag says nothing either.
        let options = crate::OpenOptions {
            s3_endpoint_url_override: Some(String::new()),
            ..crate::OpenOptions::default()
        };
        assert_eq!(
            options.effective_cloud(&file).s3_endpoint_url.as_deref(),
            Some("http://localhost:9000")
        );
    }

    #[test]
    fn a_key_without_a_secret_still_reaches_the_builder() {
        // The generated config recommends the key in the file and the secret from
        // AWS_SECRET_ACCESS_KEY. Applying them only as a pair dropped the key.
        use object_store::aws::AmazonS3ConfigKey;
        let config = CloudConfig {
            s3_access_key_id: Some("from-config".to_string()),
            ..CloudConfig::default()
        };
        let builder = s3_builder("bucket", &S3Settings::from_config(&config));
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::AccessKeyId),
            Some("from-config".to_string())
        );
        let config = CloudConfig {
            s3_secret_access_key: Some("from-env".to_string()),
            ..CloudConfig::default()
        };
        let builder = s3_builder("bucket", &S3Settings::from_config(&config));
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::SecretAccessKey),
            Some("from-env".to_string())
        );
    }

    #[test]
    fn a_shared_credentials_file_is_enough() {
        let (vars, files, home) = env_of(&[], &["/home/u/.aws/credentials"], Some("/home/u"));
        let env = environment!(vars, files, home);
        let found = detect(&CloudConfig::default(), &env);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].note, "~/.aws");
    }

    #[test]
    fn both_providers_appear_when_both_are_usable() {
        let (vars, files, home) = env_of(
            &[("AWS_ACCESS_KEY_ID", "AKIA"), ("GOOGLE_CLOUD_PROJECT", "p")],
            &["/home/u/.config/gcloud/application_default_credentials.json"],
            Some("/home/u"),
        );
        let env = environment!(vars, files, home);
        let found = detect(&CloudConfig::default(), &env);
        assert_eq!(found.len(), 2);
        assert_eq!(found[0].kind, ProviderKind::Gcs);
        assert_eq!(found[1].kind, ProviderKind::S3);
    }

    #[test]
    fn a_blank_project_variable_is_not_a_project() {
        let (vars, files, home) = env_of(
            &[("GOOGLE_CLOUD_PROJECT", "   ")],
            &["/home/u/.config/gcloud/application_default_credentials.json"],
            Some("/home/u"),
        );
        let env = environment!(vars, files, home);
        let found = detect(&CloudConfig::default(), &env);
        assert!(found[0].project.is_none());
    }

    #[test]
    fn the_project_comes_from_the_credentials_file_when_nothing_else_says() {
        // The shape gcloud writes: an authorized_user with the project the developer
        // was working in. Without this fallback, a machine that has only ever run
        // `gcloud auth application-default login` can open a bucket but not find one.
        let adc = r#"{
          "type": "authorized_user",
          "client_id": "x.apps.googleusercontent.com",
          "refresh_token": "secret-and-not-read-here",
          "quota_project_id": "example-project"
        }"#;
        let (vars, files, home) = env_of(
            &[],
            &["/home/u/.config/gcloud/application_default_credentials.json"],
            Some("/home/u"),
        );
        let env = environment!(vars, files, home, adc);
        let found = detect(&CloudConfig::default(), &env);
        assert_eq!(found[0].project.as_deref(), Some("example-project"));
        assert!(found[0].can_list_buckets());
    }

    #[test]
    fn an_environment_variable_outranks_the_credentials_file() {
        let adc = r#"{"quota_project_id": "from-the-file"}"#;
        let (vars, files, home) = env_of(
            &[("GOOGLE_CLOUD_PROJECT", "from-the-shell")],
            &["/home/u/.config/gcloud/application_default_credentials.json"],
            Some("/home/u"),
        );
        let env = environment!(vars, files, home, adc);
        let found = detect(&CloudConfig::default(), &env);
        assert_eq!(found[0].project.as_deref(), Some("from-the-shell"));
    }

    #[test]
    fn an_unparseable_credentials_file_costs_the_project_and_nothing_else() {
        let (vars, files, home) = env_of(
            &[],
            &["/home/u/.config/gcloud/application_default_credentials.json"],
            Some("/home/u"),
        );
        let env = environment!(vars, files, home, "{ not json");
        let found = detect(&CloudConfig::default(), &env);
        assert_eq!(found.len(), 1, "the provider is still usable");
        assert!(found[0].project.is_none());
    }

    #[test]
    fn gcs_buckets_come_out_of_a_real_shaped_response() {
        let body = r#"{
          "kind": "storage#buckets",
          "items": [
            {"kind": "storage#bucket", "name": "example-data", "location": "US-CENTRAL1"},
            {"kind": "storage#bucket", "name": "example-backups"}
          ]
        }"#;
        assert_eq!(
            parse_gcs_buckets(body).unwrap(),
            vec!["example-data", "example-backups"]
        );
    }

    #[test]
    fn a_project_with_no_buckets_is_an_answer_not_an_error() {
        assert_eq!(
            parse_gcs_buckets(r#"{"kind": "storage#buckets"}"#).unwrap(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn a_gcs_error_body_is_reported_rather_than_read_as_emptiness() {
        let body =
            r#"{"error": {"code": 403, "message": "does not have storage.buckets.list access"}}"#;
        let err = parse_gcs_buckets(body).unwrap_err();
        assert!(err.contains("storage.buckets.list"), "{err}");
    }

    #[test]
    fn a_malformed_gcs_entry_does_not_discard_the_page() {
        let body = r#"{"items": [{"name": ""}, {"nome": "typo"}, {"name": "good"}]}"#;
        assert_eq!(parse_gcs_buckets(body).unwrap(), vec!["good"]);
    }

    #[test]
    fn gcs_pagination_token_is_found_when_present() {
        assert_eq!(
            gcs_next_page_token(r#"{"nextPageToken": "abc", "items": []}"#).as_deref(),
            Some("abc")
        );
        assert!(gcs_next_page_token(r#"{"items": []}"#).is_none());
    }

    #[test]
    fn the_listing_goes_to_amazon_when_no_endpoint_is_set() {
        assert_eq!(
            s3_list_buckets_url(&S3Settings::from_config(&CloudConfig::default())),
            "https://s3.amazonaws.com/"
        );
    }

    #[test]
    fn the_listing_honours_the_endpoint_override() {
        // The bug this guards against: the section title named the override's host
        // while `ListBuckets` went to AWS. Listing must see the same merged endpoint
        // the open path uses, with the CLI/environment override beating the config.
        let config = CloudConfig {
            s3_endpoint_url: Some("http://localhost:9000/".to_string()),
            ..CloudConfig::default()
        };
        let options = crate::OpenOptions {
            s3_endpoint_url_override: Some("http://127.0.0.1:9101".to_string()),
            ..crate::OpenOptions::default()
        };
        let effective = options.effective_cloud(&config);
        assert_eq!(
            s3_list_buckets_url(&S3Settings::from_config(&effective)),
            "http://127.0.0.1:9101/"
        );
        // The title names the same host the listing goes to.
        let (vars, files, home) = env_of(&[("AWS_ACCESS_KEY_ID", "testing")], &[], None);
        let env = environment!(vars, files, home);
        let found = detect(&effective, &env);
        assert_eq!(found[0].label, "S3-compatible (127.0.0.1:9101)");

        // Without an override the config file's endpoint stands.
        let effective = crate::OpenOptions::default().effective_cloud(&config);
        assert_eq!(
            s3_list_buckets_url(&S3Settings::from_config(&effective)),
            "http://localhost:9000/"
        );
    }

    #[test]
    fn the_override_carries_keys_and_region_too() {
        let config = CloudConfig {
            s3_access_key_id: Some("from-config".to_string()),
            s3_region: Some("eu-west-1".to_string()),
            ..CloudConfig::default()
        };
        let options = crate::OpenOptions {
            s3_access_key_id_override: Some("from-cli".to_string()),
            s3_secret_access_key_override: Some("secret".to_string()),
            ..crate::OpenOptions::default()
        };
        let effective = options.effective_cloud(&config);
        assert_eq!(effective.s3_access_key_id.as_deref(), Some("from-cli"));
        assert_eq!(effective.s3_secret_access_key.as_deref(), Some("secret"));
        assert_eq!(effective.s3_region.as_deref(), Some("eu-west-1"));
    }

    #[test]
    fn s3_buckets_come_out_of_a_real_shaped_response() {
        let body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Owner><ID>abc</ID><DisplayName>owner</DisplayName></Owner>
          <Buckets>
            <Bucket><Name>first-bucket</Name><CreationDate>2024-01-01T00:00:00.000Z</CreationDate></Bucket>
            <Bucket><Name>second-bucket</Name><CreationDate>2024-02-01T00:00:00.000Z</CreationDate></Bucket>
          </Buckets>
        </ListAllMyBucketsResult>"#;
        assert_eq!(
            parse_s3_buckets(body).unwrap(),
            vec!["first-bucket", "second-bucket"]
        );
    }

    #[test]
    fn the_owner_display_name_is_not_mistaken_for_a_bucket() {
        // Owner/DisplayName and Bucket/Name are both leaves called something plausible.
        // Matching on the leaf alone picked up the owner; matching on the parent too is
        // what makes this right.
        let body = r#"<ListAllMyBucketsResult>
          <Owner><ID>x</ID><DisplayName>Name</DisplayName></Owner>
          <Buckets><Bucket><Name>only-bucket</Name></Bucket></Buckets>
        </ListAllMyBucketsResult>"#;
        assert_eq!(parse_s3_buckets(body).unwrap(), vec!["only-bucket"]);
    }

    #[test]
    fn an_s3_error_document_is_reported() {
        let body = r#"<Error><Code>InvalidAccessKeyId</Code><Message>The key is not valid</Message></Error>"#;
        let err = parse_s3_buckets(body).unwrap_err();
        assert!(err.contains("not valid"), "{err}");
    }

    #[test]
    fn no_buckets_is_not_an_error() {
        let body = r#"<ListAllMyBucketsResult><Buckets></Buckets></ListAllMyBucketsResult>"#;
        assert_eq!(parse_s3_buckets(body).unwrap(), Vec::<String>::new());
    }

    #[test]
    fn implausibly_deep_xml_is_refused_rather_than_followed() {
        let body = "<a>".repeat(MAX_XML_DEPTH + 2);
        assert!(parse_s3_buckets(&body).is_err());
    }

    #[test]
    fn malformed_xml_is_an_error_and_not_a_panic() {
        assert!(parse_s3_buckets("<Buckets><Bucket><Name>x").is_err());
    }

    #[test]
    fn an_endpoint_host_is_extracted_or_declined() {
        assert_eq!(
            endpoint_host("http://localhost:9000"),
            Some("localhost:9000".into())
        );
        assert_eq!(endpoint_host("https://a.b/c/d"), Some("a.b".into()));
        assert_eq!(endpoint_host("minio:9000"), Some("minio:9000".into()));
        assert_eq!(endpoint_host(""), None);
        assert_eq!(endpoint_host("http://"), None);
    }
}

#[cfg(test)]
mod aws_role_tests {
    use super::*;
    use std::collections::HashMap;

    fn detect_with(vars: &[(&str, &str)]) -> Vec<Provider> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let env = Environment {
            var: &|key| vars.get(key).cloned(),
            exists: &|_| false,
            read: &|_| None,
            home: Some(PathBuf::from("/home/u")),
            windows: false,
            run: &|_, _| {
                Err(crate::cloud_command::CommandError::Missing(
                    "test".to_string(),
                ))
            },
            all_vars: &|| Vec::new(),
            list: &|_| Vec::new(),
        };
        detect(&CloudConfig::default(), &env)
    }

    #[test]
    fn a_gcloud_login_on_windows_is_found_under_appdata() {
        let vars: HashMap<String, String> = [("APPDATA", r"C:\Users\u\AppData\Roaming")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let adc = PathBuf::from(r"C:\Users\u\AppData\Roaming")
            .join("gcloud")
            .join("application_default_credentials.json");
        let under_home = PathBuf::from(r"C:\Users\u")
            .join(".config")
            .join("gcloud")
            .join("application_default_credentials.json");
        let windows = |exists: PathBuf, windows: bool| {
            let env = Environment {
                var: &|key| vars.get(key).cloned(),
                exists: &|path| path == exists,
                read: &|_| None,
                home: Some(PathBuf::from(r"C:\Users\u")),
                windows,
                run: &|_, _| {
                    Err(crate::cloud_command::CommandError::Missing(
                        "test".to_string(),
                    ))
                },
                all_vars: &|| Vec::new(),
                list: &|_| Vec::new(),
            };
            detect(&CloudConfig::default(), &env)
                .iter()
                .any(|p| p.kind == ProviderKind::Gcs)
        };
        assert!(windows(adc.clone(), true));
        // The Unix location means nothing on Windows, and the reverse.
        assert!(!windows(under_home, true));
        assert!(!windows(adc, false));
    }

    #[test]
    fn an_ecs_task_role_is_enough() {
        // Fargate's usual shape: no key anywhere, credentials fetched from a loopback
        // endpoint. A check for keys and ~/.aws finds nothing on exactly the machines
        // most likely to be reading from S3.
        for key in [
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        ] {
            let found = detect_with(&[(key, "/v2/credentials/abc")]);
            assert_eq!(found.len(), 1, "{key} should be enough");
            assert_eq!(found[0].kind, ProviderKind::S3);
            assert_eq!(found[0].label, "Amazon S3");
            assert_eq!(found[0].note, "container role");
        }
    }

    #[test]
    fn an_eks_web_identity_is_enough() {
        let found = detect_with(&[
            ("AWS_WEB_IDENTITY_TOKEN_FILE", "/var/run/secrets/token"),
            ("AWS_ROLE_ARN", "arn:aws:iam::1:role/r"),
        ]);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].note, "web identity");
    }

    #[test]
    fn a_region_alone_is_not_a_credential() {
        // Worth pinning down. A region is configuration, not authorisation, and a
        // provider listed on the strength of one would fail on every Enter.
        assert!(detect_with(&[("AWS_REGION", "us-east-1")]).is_empty());
        assert!(detect_with(&[("AWS_DEFAULT_REGION", "us-east-1")]).is_empty());
    }

    #[test]
    fn a_key_still_outranks_a_role_in_the_note() {
        let found = detect_with(&[
            ("AWS_ACCESS_KEY_ID", "AKIA"),
            ("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/creds"),
        ]);
        assert_eq!(found[0].note, "AWS_ACCESS_KEY_ID");
    }
}

#[cfg(test)]
mod one_table_tests {
    use super::*;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path as OsPath};
    use polars::prelude::*;
    use std::sync::Arc;

    /// One row of Parquet with the given columns.
    fn parquet(columns: &[&str]) -> Vec<u8> {
        let mut frame = DataFrame::new(
            1,
            columns
                .iter()
                .map(|c| Column::new((*c).into(), &[1i32]))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let mut bytes = Vec::new();
        ParquetWriter::new(&mut bytes).finish(&mut frame).unwrap();
        bytes
    }

    /// Put `files` in a store and ask what the folder is.
    fn kind_of(files: &[(&str, &[&str])]) -> Option<crate::discover::EntryKind> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        rt.block_on(async {
            let mut objects = Vec::new();
            for (key, columns) in files {
                let bytes = parquet(columns);
                objects.push((format!("data/{key}"), bytes.len() as u64));
                store
                    .put(
                        &OsPath::from(format!("data/{key}")),
                        PutPayload::from(bytes),
                    )
                    .await
                    .unwrap();
            }
            kind_from_footers(&store, &objects).await
        })
    }

    /// The shape that prompted this, as it sits in a bucket: one file per table.
    #[test]
    fn separate_tables_in_a_bucket_are_a_folder() {
        let kind = kind_of(&[
            ("circuits.parquet", &["circuit_id", "lat", "lng"]),
            ("drivers.parquet", &["driver_id", "code", "nationality"]),
            ("laps.parquet", &["lap", "position", "time_millis"]),
        ]);
        assert_eq!(kind, Some(crate::discover::EntryKind::Directory));
    }

    #[test]
    fn parts_of_one_table_stay_one_dataset() {
        let kind = kind_of(&[
            ("part-00000.parquet", &["id", "ts", "amount"]),
            ("part-00001.parquet", &["id", "ts", "amount"]),
            ("part-00002.parquet", &["id", "ts", "amount"]),
        ]);
        assert_eq!(kind, Some(crate::discover::EntryKind::MultiFile));
    }

    /// A dataset that gained columns over the years is still one dataset, and the
    /// files read are its ends, which is where the difference is.
    #[test]
    fn a_dataset_that_gained_columns_stays_one_dataset() {
        let kind = kind_of(&[
            ("date=2009-01-03.parquet", &["id", "ts"]),
            ("date=2015-06-01.parquet", &["id", "ts", "fee"]),
            (
                "date=2025-06-01.parquet",
                &["id", "ts", "fee", "witness", "address"],
            ),
        ]);
        assert_eq!(kind, Some(crate::discover::EntryKind::MultiFile));
    }

    /// Nothing readable means nothing decided, and the listing's answer stands.
    #[test]
    fn a_folder_that_cannot_be_read_is_left_as_it_was() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let objects = vec![
            ("data/a.parquet".to_string(), 10),
            ("data/b.parquet".to_string(), 10),
        ];
        assert_eq!(rt.block_on(kind_from_footers(&store, &objects)), None);
    }

    /// One file cannot disagree with anything.
    #[test]
    fn a_single_file_decides_nothing() {
        assert_eq!(kind_of(&[("only.parquet", &["a", "b"])]), None);
    }

    /// The ends of a listing are where a table-per-file folder differs; its head can
    /// be three files of the same table by alphabetical accident.
    #[test]
    fn the_files_read_span_the_listing() {
        assert_eq!(footers_to_verify(2), vec![0, 1]);
        assert_eq!(footers_to_verify(3), vec![0, 1, 2]);
        assert_eq!(footers_to_verify(15), vec![0, 7, 14]);
    }
}
