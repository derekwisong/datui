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
use crate::discover::is_empty_marker;
use crate::source::ProviderKind;
use std::path::{Path, PathBuf};

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
    let opted_in = config.instance_identity;
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
    crate::user_agent::ureq_config()
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
    let mut builder = builder.with_bucket_name(bucket).with_config(
        object_store::aws::AmazonS3ConfigKey::Client(crate::user_agent::CLIENT_KEY),
        crate::user_agent::get(),
    );
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

/// An object store that also lists a page at a time: every store datui builds is both.
pub trait Store: object_store::ObjectStore + object_store::list::PaginatedListStore {}

impl<T: object_store::ObjectStore + object_store::list::PaginatedListStore> Store for T {}

/// The store for a resolved place, signed as the resolver decided, and the key inside it.
pub fn store(
    resolved: &crate::cloud_sources::Resolved,
) -> Result<(std::sync::Arc<dyn Store>, String), String> {
    if let Some((account, container, key)) = crate::source::azure_parts(&resolved.url) {
        let store = crate::azure::store(&account, &container, &resolved.azure)?;
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
        .with_config(
            object_store::gcp::GoogleConfigKey::Client(crate::user_agent::CLIENT_KEY),
            crate::user_agent::get(),
        )
        .build()
        .map_err(|e| format!("Google Cloud Storage is not configured: {e}"))
}

/// How many entries one peek inside a directory reads: enough to tell partitions from
/// files, and a single request however large the directory is.
const PEEK_KEYS: usize = 100;

/// What a cloud directory holds, from the first page of a delimited listing of it:
/// `Hive` when its children are `key=value` partitions, `MultiFile` when they are
/// Parquet files, else `Directory`. One request; nothing is read from any object.
pub async fn peek_kind(
    url: &str,
    config: &CloudConfig,
) -> Result<(crate::discover::EntryKind, crate::discover::Holds), String> {
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
    // The listing said these files share an extension. Whether they are one table is a
    // question only their footers answer, and the objects just listed carry the sizes
    // that make reading a footer a single ranged request. What the prefix holds is
    // unchanged by the answer: the count is a count either way.
    Ok((
        verified_kind(resolved, &objects).await.unwrap_or(kind),
        holds,
    ))
}

/// Whether a directory the listing called `multi` holds one table, from a few of its
/// footers. `None` when it could not be decided, and the listing's answer stands: the
/// optimistic reading is the reversible one.
async fn verified_kind(
    resolved: &crate::cloud_sources::Resolved,
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
            let file = crate::dataset_files::DatasetFile {
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
    crate::discover::one_table_from(&per_file).map(|one| match one {
        true => crate::discover::EntryKind::MultiFile,
        false => crate::discover::EntryKind::Directory,
    })
}

/// One page of a listing, and the token the store returned with it.
///
/// Split from the request that fetched it so the `+` can be tested: `peek_page` builds
/// its store from a URL and cannot be handed one. The token comes in whole rather than
/// already asked whether it is `Some`, because that question is the one thing here
/// worth getting wrong: asked backwards, every single-page prefix reads `100+ parquet`
/// and every prefix with more behind it reads an exact hundred nobody counted.
///
/// A prefix with more behind it counted what it saw and says so, the way a local
/// directory past `MAX_ENTRIES_PER_DIR` does.
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

/// The kind *and* what the listing found, by [`crate::discover::classify`], the rule a
/// local directory is classified by. A prefix's row is labelled from the second.
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
        // Dropped before anything counts them: the prefix's own key, which a console
        // writes to make a folder and the listing hands straight back, whatever its size
        // (`cloud-samples-data` writes eleven bytes into its markers); and an empty
        // object named like a prefix beside it, which is that prefix, counted once.
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

/// The most rows one level of a bucket lists, as for a local directory: past it the
/// listing stops and says so. A prefix of 141,000 partitions is otherwise 141 requests
/// and every one of them held in memory before the first row is drawn.
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

/// One level of a bucket or prefix, as home-screen rows, up to [`MAX_LEVEL_ROWS`].
///
/// Uses a delimited listing, so a bucket holding a million objects under a hundred
/// prefixes costs one request and returns a hundred rows. A recursive listing of the
/// same bucket would be the wrong thing in every dimension: slower, larger, billed by
/// the request, and unreadable on screen.
pub async fn list_objects(
    url: &str,
    config: &CloudConfig,
) -> Result<Vec<crate::discover::Entry>, String> {
    list_objects_watched(url, config, &Watch::default())
        .await
        .map(|level| level.rows)
}

/// [`list_objects`], a page at a time: `watch` sees the rows as they come and can stop
/// the listing between pages.
pub async fn list_objects_watched(
    url: &str,
    config: &CloudConfig,
    watch: &Watch,
) -> Result<Level, String> {
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
    let listed = list_level(url, &resolved, watch).await;
    // A sign-in with no data role on an Azure account: its keys, as the Portal does.
    let (listed, resolved) = match listed {
        Err(refusal)
            if resolved.kind == ProviderKind::Azure
                && crate::azure::is_permission_mismatch(&refusal)
                && resolved.azure.identity.is_some() =>
        {
            let enabled = config.use_azure_account_keys;
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
                Ok(keyed) => (list_level(url, &keyed, watch).await, keyed),
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
            let level = list_level(url, &resolved.unsigned(), watch)
                .await
                .map_err(|_| refused)?;
            crate::cloud_sources::remember_access(&place, true);
            Ok(level)
        }
        Ok(level) => {
            if signing == Signing::Try {
                crate::cloud_sources::remember_access(&place, false);
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

/// The server-side prefix a home filter can ask a cut-short level for, or `None` when
/// it cannot ask for one.
///
/// A name filter is fuzzy and the server's prefix is literal, so this asks for the names
/// the filter most plausibly starts: the part `names` share up to their last separator
/// (`STATION=`, `year=`), then the filter, in the case the names are written in. A
/// filter already spelling that shared part is taken as typed.
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
    // Back to the last separator: names sharing `STATION=A` share `STATION=`, and the
    // `A` is only where the first page happened to end.
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

/// A key that stands for something other than data a user could open: the receipts a job
/// leaves behind, and the marker some tools write in place of a folder.
///
/// Narrower than [`crate::discover::is_bookkeeping`] on purpose. That one answers "does
/// this count as data", which decides a directory's kind; this one answers "is there
/// anything here to open", which decides whether a row is shown at all. A leading `_` is
/// enough for the first and not for the second: `_manifest.parquet` is a real object
/// somebody may want to look at, and the local listing has always shown its equivalent.
pub fn is_marker(name: &str) -> bool {
    name == "_SUCCESS"
        || name.starts_with("_committed_")
        || name.starts_with("_started_")
        || name.ends_with("_$folder$")
}

/// Whether an object in one level of `prefix` is shown as a row.
///
/// A key ending in a slash is how consoles fake a folder. It is not data, and offering
/// it as openable would be offering a zero-byte file. So is an empty object named like a
/// directory beside it. And so is the directory's own key, whatever its size:
/// `object_store` hands `census/` back as `census`, which names no object, so opening
/// the row was a 404.
fn is_listed_object(location: &str, size: u64, prefix: &str, prefixes: &[String]) -> bool {
    let name = location.rsplit('/').next().unwrap_or(location);
    !(name.is_empty()
        || is_marker(name)
        || crate::azure::is_folder_marker(location, size, prefixes)
        || is_empty_marker(name, size)
        || location.trim_end_matches('/') == prefix)
}

/// One level of a place, signed or not as `resolved` says.
async fn list_level(
    url: &str,
    resolved: &crate::cloud_sources::Resolved,
    watch: &Watch,
) -> Result<Level, String> {
    let (pager, prefix) = store(resolved)?;
    let prefix = prefix.trim_matches('/').to_string();
    // Rows keep the source the listing was asked for, so opening one reaches the same
    // server. An Azure place is named by its canonical URL, a directory with its slash.
    let (base, directory_end) = match crate::source::azure_parts(&resolved.url) {
        Some((account, container, _)) => (crate::source::azure_url(&account, &container, ""), "/"),
        None => {
            let (kind, bucket, _) = split_bucket_url(&resolved.url)
                .ok_or_else(|| format!("not an object-store URL: {url}"))?;
            let base = match crate::source::split_source_id(url).0 {
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
                // No extension is left openable: a part file with none may well be
                // Parquet.
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

/// One level under `prefix`, a page at a time, as `rows_of` makes each page into
/// directories and objects; stopped at [`MAX_LEVEL_ROWS`], or when `watch` is
/// cancelled.
///
/// Pages rather than `list_with_delimiter`, which asks for every page before it
/// answers: 141 of them, one after another, for a prefix of 141,000 partitions.
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
    crate::user_agent::ureq_config()
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
            let (account, container, key) = crate::source::azure_parts(&resolved.url)?;
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
            crate::cloud_sources::session_sources(&config)
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
                crate::cloud_sources::remember_bucket(&source, &bucket);
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
        let containers = crate::azure::list_containers(&account, &settings)?;
        Ok(containers
            .into_iter()
            .map(|container| {
                crate::discover::Entry::directory(Path::new(&crate::source::azure_url(
                    &account, &container, "",
                )))
                .with_name(container)
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

    let mut buckets = crate::cloud_command::paged(MAX_BUCKET_PAGES, |token| {
        let mut url = format!(
            "https://storage.googleapis.com/storage/v1/b?project={}&maxResults=1000",
            urlencode(project)
        );
        if let Some(token) = token {
            url.push_str(&format!("&pageToken={}", urlencode(token)));
        }
        let body = crate::gcloud::get(&url, &bearer)?;
        Ok((parse_gcs_buckets(&body)?, gcs_next_page_token(&body)))
    })?;
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
mod tests;

#[cfg(test)]
mod aws_role_tests;

#[cfg(test)]
mod one_table_tests;
