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

use crate::cloud_sources::{S3Settings, Source};
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
}

impl Environment<'_> {
    /// The real environment.
    pub fn current() -> Environment<'static> {
        Environment {
            var: &|key| std::env::var(key).ok(),
            exists: &|path| path.exists(),
            read: &|path| std::fs::read_to_string(path).ok(),
            home: dirs::home_dir(),
            windows: cfg!(windows),
            run: &|program, args| {
                crate::cloud_command::run(program, args, crate::cloud_command::CREDENTIAL_TIMEOUT)
            },
            all_vars: &|| std::env::vars().collect(),
        }
    }
}

/// Object stores this machine can read, in the order they should appear.
///
/// Empty is the normal answer on a machine with no cloud credentials, and it is not a
/// failure. Nothing here prompts, installs or logs in.
pub fn detect(config: &CloudConfig, env: &Environment<'_>) -> Vec<Provider> {
    let mut providers = Vec::new();
    if let Some(gcs) = detect_gcs(env) {
        providers.push(gcs);
    }
    if let Some(s3) = detect_s3(config, env) {
        providers.push(s3);
    }
    providers
}

/// Google Cloud Storage, when credentials `object_store` accepts are present.
fn detect_gcs(env: &Environment<'_>) -> Option<Provider> {
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
fn gcp_project(env: &Environment<'_>) -> Option<String> {
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
    let builder = if settings.from_env {
        object_store::aws::AmazonS3Builder::from_env()
    } else {
        object_store::aws::AmazonS3Builder::new()
    };
    let mut builder = builder.with_bucket_name(bucket);
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
) -> Result<std::sync::Arc<dyn object_store::ObjectStore>, String> {
    match kind {
        ProviderKind::Gcs => {
            let store = object_store::gcp::GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(bucket)
                .build()
                .map_err(|e| format!("Google Cloud Storage is not configured: {e}"))?;
            Ok(std::sync::Arc::new(store))
        }
        ProviderKind::S3 => {
            let store = s3_builder(bucket, settings)
                .build()
                .map_err(|e| format!("S3 is not configured: {e}"))?;
            Ok(std::sync::Arc::new(store))
        }
        ProviderKind::Azure => Err("an Azure container needs its account".to_string()),
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
    if resolved.kind == ProviderKind::Azure {
        return list_azure_objects(&resolved).await;
    }
    let (kind, bucket, prefix) =
        split_bucket_url(&resolved.url).ok_or_else(|| format!("not an object-store URL: {url}"))?;
    let store = store_for_bucket(kind, &bucket, &resolved.s3)?;

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
        // offering it as openable would be offering a zero-byte file.
        if name.is_empty() {
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
        if name.is_empty() || crate::azure::is_folder_marker(&location, object.size, &prefixes) {
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
        let accounts = crate::azure::discover_accounts(&Environment::current())?;
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

/// The containers of one account in an Azure source, as rows to step into.
pub async fn list_account(
    source_id: &str,
    account: &str,
    config: &CloudConfig,
) -> Result<Vec<crate::discover::Entry>, String> {
    let (source_id, account, config) = (source_id.to_string(), account.to_string(), config.clone());
    tokio::task::spawn_blocking(move || {
        let env = Environment::current();
        let source = crate::cloud_sources::discover(&config, &env)
            .into_iter()
            .find(|s| s.id == source_id && s.kind == ProviderKind::Azure)
            .ok_or_else(|| format!("source not found: {source_id}"))?;
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

    let store = object_store::gcp::GoogleCloudStorageBuilder::from_env()
        .with_bucket_name("datui-credential-probe")
        .build()
        .map_err(|e| format!("Google Cloud Storage is not configured: {e}"))?;
    let credential = store
        .credentials()
        .get_credential()
        .await
        .map_err(|e| format!("could not obtain Google credentials: {e}"))?;

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
        let body = http_agent()
            .get(&url)
            .header("Authorization", &format!("Bearer {}", credential.bearer))
            .call()
            .map_err(|e| format!("{e}"))?
            .body_mut()
            .read_to_string()
            .map_err(|e| format!("could not read the response: {e}"))?;

        buckets.extend(parse_gcs_buckets(&body)?);
        match gcs_next_page_token(&body) {
            Some(token) => page_token = Some(token),
            None => break,
        }
    }

    buckets.sort();
    Ok(buckets)
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
