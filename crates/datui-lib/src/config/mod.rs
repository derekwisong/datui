pub mod config_command;
pub mod themes;

pub use crate::home::catalog::is_object_store_dataset;
use crate::numfmt::{self, Glob, Grouping, NumberFormat, NumberFormatSettings};
use color_eyre::Result;
use color_eyre::eyre::eyre;
pub use datui_cli::units::{ByteSize, Interval};
use ratatui::style::Color;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use supports_color::Stream;

/// Manages config directory and config file operations
#[derive(Clone)]
pub struct ConfigManager {
    pub(crate) config_dir: PathBuf,
}

impl ConfigManager {
    /// Create a ConfigManager with a custom config directory (primarily for testing)
    pub fn with_dir(config_dir: PathBuf) -> Self {
        Self { config_dir }
    }

    /// Create a ConfigManager for `app_name`. `DATUI_CONFIG_DIR` overrides the location;
    /// the test suite sets it so tests never write into the developer's config (views
    /// live there), and a test reaching the real directory refuses.
    pub fn new(app_name: &str) -> Result<Self> {
        #[cfg(test)]
        crate::cache::isolate_cache();
        if let Some(dir) = std::env::var_os("DATUI_CONFIG_DIR") {
            return Ok(Self {
                config_dir: PathBuf::from(dir),
            });
        }
        if crate::cache::running_as_a_cargo_test() {
            panic!(
                "DATUI_CONFIG_DIR is not set: a test would read and write the real \
                 config (saved views included). Call common::isolate_cache() before \
                 building an App or a ConfigManager."
            );
        }

        let config_dir = dirs::config_dir()
            .ok_or_else(|| eyre!("Could not determine config directory"))?
            .join(app_name);

        Ok(Self { config_dir })
    }

    /// Get the config directory path
    pub fn config_dir(&self) -> &Path {
        &self.config_dir
    }

    /// Get path to a specific config file or subdirectory
    pub fn config_path(&self, path: &str) -> PathBuf {
        self.config_dir.join(path)
    }

    /// Ensure the config directory exists
    pub fn ensure_config_dir(&self) -> Result<()> {
        if !self.config_dir.exists() {
            std::fs::create_dir_all(&self.config_dir)?;
        }
        Ok(())
    }

    /// Ensure a subdirectory exists within the config directory
    pub fn ensure_subdir(&self, subdir: &str) -> Result<PathBuf> {
        let subdir_path = self.config_dir.join(subdir);
        if !subdir_path.exists() {
            std::fs::create_dir_all(&subdir_path)?;
        }
        Ok(subdir_path)
    }

    /// The commented file `datui config init` writes, from the option registry: every
    /// key with its doc line and its default, commented out so the defaults keep
    /// applying. Datasets are not here: they are in `catalog.toml`.
    pub fn generate_default_config(&self) -> String {
        use datui_cli::settings::{DefaultValue, Kind, SECTIONS, in_section};
        let mut out = String::from(
            "# datui configuration file (TOML: https://toml.io).\n\
             # Every setting is commented out at its default; remove the # to change one.\n\
             # `datui config keys` lists them with the values in effect.\n",
        );
        for section in SECTIONS {
            let settings: Vec<_> = in_section(section.name)
                .filter(|s| s.kind != Kind::Tables)
                .collect();
            if settings.is_empty() {
                continue;
            }
            out.push('\n');
            if !section.name.is_empty() {
                out.push_str(&format!(
                    "# {rule}\n# {}\n# {rule}\n# [{}]\n",
                    section.title,
                    section.name,
                    rule = "=".repeat(76)
                ));
            }
            for setting in settings {
                for line in wrap(setting.doc, 86) {
                    out.push_str(&format!("# {line}\n"));
                }
                let value = match setting.default {
                    DefaultValue::Value(v) | DefaultValue::Unset(v) => v.to_string(),
                    DefaultValue::Color { dark, .. } => format!("\"{dark}\""),
                };
                if setting.key.ends_with(".*") {
                    out.push_str(&format!("# {value}\n"));
                } else {
                    out.push_str(&format!("# {} = {value}\n", setting.name()));
                }
            }
        }
        out
    }

    /// Write default configuration to config file
    pub fn write_default_config(&self, force: bool) -> Result<PathBuf> {
        let config_path = self.config_path("config.toml");

        if config_path.exists() && !force {
            return Err(eyre!(
                "Config file already exists at {}. Use --force to overwrite.",
                config_path.display()
            ));
        }

        self.ensure_config_dir()?;

        // Generate and write default config
        let template = self.generate_default_config();
        write_private(&config_path, &template)?;

        // The catalog is the user's data: written only when absent, even with --force.
        let catalog = self.config_path(crate::home::catalog::MINE_FILE);
        if !catalog.exists() {
            std::fs::write(&catalog, crate::home::catalog::MINE_TEMPLATE)?;
        }
        // Where more catalogs go: the header's `> catalogs/examples.toml` needs it there.
        self.ensure_subdir(crate::home::catalog::FOLDER)?;
        // Where theme files go: `datui theme show NAME` prints one to start from.
        self.ensure_subdir(crate::config::themes::FOLDER)?;

        Ok(config_path)
    }
}

/// One `catalogs` entry: a path, or a table giving a file an id and label of its
/// own.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum CatalogRef {
    Path(String),
    Table {
        path: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        label: Option<String>,
    },
}

impl CatalogRef {
    /// The file, as written.
    pub fn path(&self) -> &str {
        match self {
            Self::Path(path) | Self::Table { path, .. } => path,
        }
    }

    /// The id the entry gives, when it gives one: else the file's name is the id.
    pub fn id(&self) -> Option<&str> {
        match self {
            Self::Table { id: Some(id), .. } => Some(id),
            _ => None,
        }
    }

    /// The label the entry gives, over the file's own.
    pub fn label(&self) -> Option<&str> {
        match self {
            Self::Table {
                label: Some(label), ..
            } => Some(label),
            _ => None,
        }
    }
}

impl<'de> Deserialize<'de> for CatalogRef {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        match toml::Value::deserialize(deserializer)? {
            toml::Value::String(path) => Ok(Self::Path(path)),
            toml::Value::Table(table) => {
                let text = |key: &str| -> Result<Option<String>, D::Error> {
                    match table.get(key) {
                        None => Ok(None),
                        Some(toml::Value::String(s)) => Ok(Some(s.clone())),
                        Some(_) => Err(D::Error::custom(format!(
                            "catalogs: {key} must be a string"
                        ))),
                    }
                };
                if let Some(key) = table
                    .keys()
                    .find(|k| !matches!(k.as_str(), "path" | "id" | "label"))
                {
                    return Err(D::Error::custom(format!(
                        "catalogs: unknown key '{key}'. Expected one of: path, id, label"
                    )));
                }
                let path = text("path")?
                    .ok_or_else(|| D::Error::custom("catalogs: a table needs path = \"...\""))?;
                Ok(Self::Table {
                    path,
                    id: text("id")?,
                    label: text("label")?,
                })
            }
            _ => Err(D::Error::custom(
                "catalogs: each entry is a path, or { path, id, label }",
            )),
        }
    }
}

/// Complete application configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    /// Additional config files merged in before this file's own values.
    pub import: Vec<String>,
    /// Catalog files elsewhere, listed on the home screen besides `catalog.toml` and
    /// `catalogs/`: a path, or `{ path, id, label }`.
    pub catalogs: Vec<CatalogRef>,
    /// The catalogs read (`catalog.toml`, then each of `catalogs`). Not a key: filled by
    /// [`AppConfig::read_catalog_files`] after merging.
    #[serde(skip)]
    pub read_catalogs: Vec<crate::home::catalog::Catalog>,
    /// The directory `catalog.toml` was looked for in: the config file's.
    #[serde(skip)]
    pub catalog_dir: Option<PathBuf>,
    /// Catalog files left out for a mistake, each with what is wrong.
    #[serde(skip)]
    pub broken_catalogs: Vec<crate::home::catalog::Broken>,
    pub read: ReadConfig,
    pub csv: CsvConfig,
    pub display: DisplayConfig,
    pub performance: PerformanceConfig,
    pub analysis: AnalysisConfig,
    pub chart: ChartConfig,
    pub home: HomeConfig,
    pub cloud: CloudConfig,
    pub http: HttpConfig,
    pub query: QueryConfig,
    pub views: ViewsConfig,
    pub clipboard: ClipboardConfig,
    pub formats: FormatsConfig,
    pub limits: LimitsConfig,
    pub log: LogConfig,
    pub theme: ThemeConfig,
    pub glyphs: GlyphsConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CloudConfig {
    /// The S3 endpoint, keys and region from the environment (`AWS_*`); never file keys,
    /// where secrets would sit in plain text. `[[cloud.connections]]` names variables.
    #[serde(skip)]
    pub s3_endpoint_url: Option<String>,
    #[serde(skip)]
    pub s3_access_key_id: Option<String>,
    #[serde(skip)]
    pub s3_secret_access_key: Option<String>,
    #[serde(skip)]
    pub s3_region: Option<String>,
    /// Stores named in `[[cloud.connections]]`, beside the ones found on the machine.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<CloudConnectionConfig>,
    /// Source IDs never shown on the home screen.
    pub hide: Vec<String>,
    /// Read an Azure account with its access keys when a sign-in has no data role, as
    /// the Portal does.
    pub use_azure_account_keys: bool,
    /// Files to read cloud variables from, relative to the working directory (`.env`).
    /// Only known variable names are taken; nothing is exported.
    pub env_files: Vec<String>,
    /// Use the identity of the cloud VM datui runs on (EC2, GCE, Azure). Off by default:
    /// finding it queries a metadata service.
    pub instance_identity: bool,
    /// Which logins found on this machine become home-screen sources. Unset means all.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discover: Option<CloudDiscover>,
    /// List every source's buckets when home opens. Off: a source lists, and runs its
    /// credential command, only when entered or on Ctrl+R.
    pub list_on_start: bool,
    /// How the catalogs' object-store datasets are read. Not a key: derived by
    /// [`AppConfig`], so resolving a URL needs only this section.
    #[serde(skip)]
    pub dataset_access: Vec<DatasetAccess>,
}

impl Default for CloudConfig {
    fn default() -> Self {
        Self {
            s3_endpoint_url: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_region: None,
            connections: Vec::new(),
            hide: Vec::new(),
            use_azure_account_keys: true,
            env_files: Vec::new(),
            instance_identity: false,
            discover: None,
            list_on_start: false,
            dataset_access: Vec::new(),
        }
    }
}

/// How a catalog dataset's URL in an object store is read, when it says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetAccess {
    /// The dataset's URL: everything under it is read the same way.
    pub url: String,
    /// The catalog it is listed in.
    pub catalog: String,
    pub auth: DatasetAuth,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatasetAuth {
    /// As any URL is read: the login found for it, unsigned when there is none.
    Auto,
    /// No credentials and no signature.
    Anonymous,
    /// Signed with the `[[cloud.connections]]` entry of this name.
    Connection(String),
}

/// The kinds of source `[cloud] discover` can name.
pub const CLOUD_DISCOVER_KINDS: [&str; 3] = ["s3", "gcs", "azure"];

/// `[cloud] discover`: `true` or `"all"`, `false` or `"none"`, or a list of kinds.
/// `"all"` and `"none"` are words, not list members.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "CloudDiscoverValue", into = "CloudDiscoverValue")]
pub enum CloudDiscover {
    All,
    None,
    Kinds(Vec<String>),
}

impl CloudDiscover {
    /// Whether sources of `kind` (`s3`, `gcs`, `azure`) are found.
    pub fn allows(&self, kind: &str) -> bool {
        match self {
            CloudDiscover::All => true,
            CloudDiscover::None => false,
            CloudDiscover::Kinds(kinds) => kinds.iter().any(|k| k == kind),
        }
    }

    fn from_kinds<S: AsRef<str>>(kinds: &[S]) -> std::result::Result<Self, String> {
        let mut out: Vec<String> = Vec::new();
        for kind in kinds {
            let kind = kind.as_ref().trim().to_ascii_lowercase();
            if !CLOUD_DISCOVER_KINDS.contains(&kind.as_str()) {
                return Err(format!(
                    "cloud.discover: unknown kind \"{kind}\"; use {}",
                    CLOUD_DISCOVER_KINDS.join(", ")
                ));
            }
            if !out.contains(&kind) {
                out.push(kind);
            }
        }
        Ok(CloudDiscover::Kinds(out))
    }
}

impl std::str::FromStr for CloudDiscover {
    type Err = String;

    /// `all`, `none`, or kinds separated by commas: the command-line form.
    fn from_str(text: &str) -> std::result::Result<Self, String> {
        match text.trim().to_ascii_lowercase().as_str() {
            "all" => Ok(CloudDiscover::All),
            "none" => Ok(CloudDiscover::None),
            _ => CloudDiscover::from_kinds(&text.split(',').collect::<Vec<_>>()).map_err(|_| {
                format!(
                    "cloud.discover: \"{text}\" is not \"all\", \"none\", or kinds from {}",
                    CLOUD_DISCOVER_KINDS.join(", ")
                )
            }),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum CloudDiscoverValue {
    Switch(bool),
    Word(String),
    Kinds(Vec<String>),
}

impl TryFrom<CloudDiscoverValue> for CloudDiscover {
    type Error = String;

    fn try_from(value: CloudDiscoverValue) -> std::result::Result<Self, String> {
        match value {
            CloudDiscoverValue::Switch(true) => Ok(CloudDiscover::All),
            CloudDiscoverValue::Switch(false) => Ok(CloudDiscover::None),
            // The command line's form: "all", "none", or "s3,gcs".
            CloudDiscoverValue::Word(word) => word.parse(),
            CloudDiscoverValue::Kinds(kinds) => CloudDiscover::from_kinds(&kinds),
        }
    }
}

impl From<CloudDiscover> for CloudDiscoverValue {
    fn from(discover: CloudDiscover) -> Self {
        match discover {
            CloudDiscover::All => CloudDiscoverValue::Switch(true),
            CloudDiscover::None => CloudDiscoverValue::Switch(false),
            CloudDiscover::Kinds(kinds) => CloudDiscoverValue::Kinds(kinds),
        }
    }
}

/// One store in `[[cloud.connections]]`: names and pointers only; secrets come from
/// the environment variables named here.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(default)]
pub struct CloudConnectionConfig {
    /// The source's ID: used in `s3://<name>@bucket/key`, `hide` and cache keys.
    pub name: String,
    /// Shown instead of the name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// `s3`, `gcs` or `azure`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Buckets to show when the credentials can read but not list.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub buckets: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// `path` or `virtual`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub addressing: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_key_id_env: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_access_key_env: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_token_env: Option<String>,
    /// An AWS profile to take the keys, endpoint and region from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    /// A `gcloud` configuration whose login to use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configuration: Option<String>,
    /// The Google Cloud project listed first, and the one listed when projects cannot
    /// be searched.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    /// The Azure storage account.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_key_env: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sas_env: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_string_env: Option<String>,
    /// A program that prints the secret: the S3 secret access key, or the Azure account
    /// key. Run without a shell, its output kept in memory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_command: Option<String>,
    /// A Google service account or application-default JSON file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials_file: Option<String>,
    /// Keys that are not recognised, kept so validation can name them.
    #[serde(flatten)]
    pub unknown: std::collections::BTreeMap<String, toml::Value>,
}

/// Field names accepted in `[[cloud.connections]]`, for error messages.
const CLOUD_SOURCE_KEYS: &str = "name, label, kind, buckets, endpoint_url, region, \
     addressing, access_key_id_env, secret_access_key_env, session_token_env, profile, \
     configuration, project, account, account_key_env, sas_env, connection_string_env, \
     secret_command, credentials_file";

/// Whether `id` can name a source: lowercase letters, digits and `-`, starting with
/// a letter or digit, at most 40 characters (it goes into URLs and cache keys).
pub fn is_valid_source_id(id: &str) -> bool {
    let bytes = id.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 40
        && bytes[0] != b'-'
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

impl CloudConnectionConfig {
    fn validate(&self) -> Result<()> {
        let name = &self.name;
        if name.is_empty() {
            return Err(eyre!("cloud.connections: every source needs a name"));
        }
        if !is_valid_source_id(name) {
            return Err(eyre!(
                "cloud.connections: \"{name}\" is not a valid name. Use lowercase letters, digits \
                 and '-', up to 40 characters"
            ));
        }
        // A secret in the file is refused with the way out, not as an unknown key.
        for secret in ["access_key_id", "secret_access_key", "session_token"] {
            if self.unknown.contains_key(secret) {
                return Err(eyre!(
                    "cloud.connections \"{name}\": {secret} cannot be written in the config. Put it \
                     in an environment variable and name that with {secret}_env"
                ));
            }
        }
        if !self.unknown.is_empty() {
            let keys: Vec<String> = self.unknown.keys().map(|k| format!("'{k}'")).collect();
            return Err(eyre!(
                "cloud.connections \"{name}\": unknown key{} {}. Expected one of: {}",
                if keys.len() > 1 { "s" } else { "" },
                keys.join(", "),
                CLOUD_SOURCE_KEYS
            ));
        }
        for secret in ["account_key", "sas", "sas_token", "connection_string"] {
            if self.unknown.contains_key(secret) {
                return Err(eyre!(
                    "cloud.connections \"{name}\": {secret} cannot be written in the config. Put it \
                     in an environment variable and name that with {}_env",
                    secret.trim_end_matches("_token")
                ));
            }
        }
        let kind = match self.kind.as_deref() {
            Some(kind @ ("s3" | "gcs" | "azure")) => kind,
            Some(other) => {
                return Err(eyre!(
                    "cloud.connections \"{name}\": kind \"{other}\" is not supported. Expected s3, gcs or azure"
                ));
            }
            None => {
                return Err(eyre!(
                    "cloud.connections \"{name}\": kind is required (s3, gcs or azure)"
                ));
            }
        };
        if let Some(command) = &self.secret_command {
            if !matches!(kind, "s3" | "azure") {
                return Err(eyre!(
                    "cloud.connections \"{name}\": secret_command applies only to kind = \"s3\" or \"azure\""
                ));
            }
            if command.trim().is_empty() {
                return Err(eyre!(
                    "cloud.connections \"{name}\": secret_command is not a command line"
                ));
            }
            let clash = if kind == "s3" {
                [
                    (
                        "secret_access_key_env",
                        self.secret_access_key_env.is_some(),
                    ),
                    ("profile", self.profile.is_some()),
                    ("", false),
                ]
            } else {
                [
                    ("account_key_env", self.account_key_env.is_some()),
                    ("sas_env", self.sas_env.is_some()),
                    (
                        "connection_string_env",
                        self.connection_string_env.is_some(),
                    ),
                ]
            };
            if let Some((field, _)) = clash.iter().find(|(_, set)| *set) {
                return Err(eyre!(
                    "cloud.connections \"{name}\": secret_command and {field} both say where the \
                     secret comes from. Use one"
                ));
            }
            if kind == "s3" && self.access_key_id_env.is_none() {
                return Err(eyre!(
                    "cloud.connections \"{name}\": secret_command prints the secret; name the key \
                     ID with access_key_id_env"
                ));
            }
        }
        if let Some(_file) = &self.credentials_file {
            if kind != "gcs" {
                return Err(eyre!(
                    "cloud.connections \"{name}\": credentials_file applies only to kind = \"gcs\""
                ));
            }
            if self.configuration.is_some() {
                return Err(eyre!(
                    "cloud.connections \"{name}\": credentials_file and configuration both say how \
                     to log in. Use one"
                ));
            }
        }
        if kind != "azure" {
            let azure_only = [
                ("account", self.account.is_some()),
                ("account_key_env", self.account_key_env.is_some()),
                ("sas_env", self.sas_env.is_some()),
                (
                    "connection_string_env",
                    self.connection_string_env.is_some(),
                ),
            ];
            if let Some((field, _)) = azure_only.iter().find(|(_, set)| *set) {
                return Err(eyre!(
                    "cloud.connections \"{name}\": {field} applies only to kind = \"azure\""
                ));
            }
        } else {
            let secrets = [
                self.account_key_env.is_some(),
                self.sas_env.is_some(),
                self.connection_string_env.is_some(),
            ];
            if secrets.iter().filter(|set| **set).count() > 1 {
                return Err(eyre!(
                    "cloud.connections \"{name}\": account_key_env, sas_env and \
                     connection_string_env each say how to sign in. Use one"
                ));
            }
            if self.account.is_none() && self.connection_string_env.is_none() {
                return Err(eyre!(
                    "cloud.connections \"{name}\": an azure source needs account, or \
                     connection_string_env"
                ));
            }
            if !self.buckets.is_empty() {
                return Err(eyre!(
                    "cloud.connections \"{name}\": buckets does not apply to kind = \"azure\""
                ));
            }
        }
        if kind != "s3" {
            let s3_only = [
                ("endpoint_url", self.endpoint_url.is_some()),
                ("region", self.region.is_some()),
                ("addressing", self.addressing.is_some()),
                ("access_key_id_env", self.access_key_id_env.is_some()),
                (
                    "secret_access_key_env",
                    self.secret_access_key_env.is_some(),
                ),
                ("session_token_env", self.session_token_env.is_some()),
                ("profile", self.profile.is_some()),
            ];
            if let Some((field, _)) = s3_only.iter().find(|(_, set)| *set) {
                return Err(eyre!(
                    "cloud.connections \"{name}\": {field} applies only to kind = \"s3\""
                ));
            }
        }
        if kind != "gcs" {
            let gcs_only = [
                ("configuration", self.configuration.is_some()),
                ("project", self.project.is_some()),
            ];
            if let Some((field, _)) = gcs_only.iter().find(|(_, set)| *set) {
                return Err(eyre!(
                    "cloud.connections \"{name}\": {field} applies only to kind = \"gcs\""
                ));
            }
        }
        if self.profile.is_some()
            && (self.access_key_id_env.is_some()
                || self.secret_access_key_env.is_some()
                || self.session_token_env.is_some())
        {
            return Err(eyre!(
                "cloud.connections \"{name}\": profile and the *_env keys both say where the keys \
                 come from. Use one"
            ));
        }
        if let Some(addressing) = self.addressing.as_deref()
            && !matches!(addressing, "path" | "virtual")
        {
            return Err(eyre!(
                "cloud.connections \"{name}\": addressing \"{addressing}\" is not valid. Expected path \
                 or virtual"
            ));
        }
        if let Some(bucket) = self.buckets.iter().find(|b| b.contains(['/', '@'])) {
            return Err(eyre!(
                "cloud.connections \"{name}\": \"{bucket}\" is not a bucket name{}",
                if bucket.contains("://") {
                    ". A dataset URL goes in a catalog"
                } else {
                    ""
                }
            ));
        }
        Ok(())
    }
}

/// Write `contents` to `path`, readable only by the owner: the generated config
/// invites S3 credentials, and a plain write would be world-readable (0644). The mode
/// is set on create (no 0644 window) and again with `set_permissions` (for `--force`
/// over an existing file). Elsewhere a plain write: Windows ACLs are already
/// per-user.
fn write_private(path: &Path, contents: &str) -> Result<()> {
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, contents)?;
    }
    Ok(())
}

/// The variables naming an S3 endpoint, in lookup order: service-specific, general
/// (as AWS SDKs read them), then `AWS_ENDPOINT` (what `object_store` accepts).
pub const S3_ENDPOINT_VARS: [&str; 3] = ["AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL", "AWS_ENDPOINT"];

/// A value that says something: a blank variable or flag must not erase the config's.
fn non_blank(value: String) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

impl CloudConfig {
    /// The S3 settings the environment sets; the one variable list, so discovery,
    /// listing and opening agree. `var` is injectable for tests.
    pub fn from_env(var: &dyn Fn(&str) -> Option<String>) -> Self {
        let first = |keys: &[&str]| keys.iter().find_map(|key| var(key).and_then(non_blank));
        Self {
            s3_endpoint_url: first(&S3_ENDPOINT_VARS),
            s3_access_key_id: first(&["AWS_ACCESS_KEY_ID"]),
            s3_secret_access_key: first(&["AWS_SECRET_ACCESS_KEY"]),
            s3_region: first(&["AWS_REGION", "AWS_DEFAULT_REGION"]),
            ..Default::default()
        }
    }

    /// Lay the environment's S3 settings over these: each one `over` gives wins; blanks
    /// say nothing.
    pub fn overlay(&mut self, over: Self) {
        for (slot, value) in [
            (&mut self.s3_endpoint_url, over.s3_endpoint_url),
            (&mut self.s3_access_key_id, over.s3_access_key_id),
            (&mut self.s3_secret_access_key, over.s3_secret_access_key),
            (&mut self.s3_region, over.s3_region),
        ] {
            if let Some(value) = value.and_then(non_blank) {
                *slot = Some(value);
            }
        }
    }

    /// Reject `[[cloud.connections]]` entries that would be silently wrong.
    pub fn validate(&self) -> Result<()> {
        for (i, connection) in self.connections.iter().enumerate() {
            connection.validate()?;
            if self.connections[..i]
                .iter()
                .any(|c| c.name == connection.name)
            {
                return Err(eyre!(
                    "cloud.connections: the name \"{}\" is used twice",
                    connection.name
                ));
            }
        }
        Ok(())
    }
}

/// `[read]`: how files are read.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ReadConfig {
    /// Which string columns are typed: all, none, or those named.
    pub infer_types: InferTypes,
    /// How a partitioned Parquet dataset's schema is found.
    pub parquet_schema: ParquetSchema,
    /// Decompress a compressed CSV, TSV or PSV into memory instead of to a temp file.
    pub decompress_in_memory: bool,
    /// Directory for decompression temp files. Unset: the system's (e.g. TMPDIR).
    pub temp_dir: Option<String>,
    /// `--follow`: how often a followed file is checked (on Linux, where changes are
    /// notified, the least time between reads). Appends within one interval are one
    /// refresh.
    pub follow_interval: Interval,
    /// Datasets of more files than this show an estimated row count until asked to count
    /// exactly. 0 always counts.
    pub exact_count_files: usize,
    /// Ask before reading more than this of a file whole into memory (JSON, Avro, ORC,
    /// Excel and other in-memory formats). 0 never asks.
    pub memory_warning: ByteSize,
    /// Integer audio samples as float in [-1, 1].
    pub audio_float: bool,
}

impl ReadConfig {
    /// The bytes past which an in-memory read asks first; `None` when 0.
    pub fn memory_warning(&self) -> Option<u64> {
        let bytes = self.memory_warning.bytes();
        (bytes > 0).then_some(bytes)
    }
}

impl Default for ReadConfig {
    fn default() -> Self {
        Self {
            infer_types: InferTypes::Switch(true),
            parquet_schema: ParquetSchema::Union,
            decompress_in_memory: false,
            temp_dir: None,
            follow_interval: Interval(crate::loading::follow::DEFAULT_INTERVAL),
            exact_count_files: 50_000,
            memory_warning: ByteSize::mib(1024),
            audio_float: false,
        }
    }
}

/// `[read] infer_types`: every string column, none, or the columns named.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum InferTypes {
    Switch(bool),
    Columns(Vec<String>),
}

/// `[read] parquet_schema`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ParquetSchema {
    /// Every column any file has, from their footers.
    Union,
    /// Polars' schema from one file.
    First,
}

/// Bounds of `[read] follow_interval`: faster than ten a second redraws unreadably;
/// slower than a minute is not following.
const FOLLOW_INTERVAL: std::ops::RangeInclusive<std::time::Duration> =
    std::time::Duration::from_millis(10)..=std::time::Duration::from_secs(60);

/// `[csv]`: CSV, TSV and PSV, and the dialect a delimited spec writes with these keys.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CsvConfig {
    /// Lines starting with this are comments, before the header and in the data
    /// (Frictionless `commentChar`).
    pub comment: Option<String>,
    /// What joins a column's pieces when `--header-rows` names several lines
    /// (Frictionless `headerJoin`).
    pub header_join: String,
    /// Ignore the spaces after a delimiter (Frictionless `skipInitialSpace`).
    pub skip_initial_space: bool,
    /// Read as null: `VAL` everywhere, `COL=VAL` in one column.
    pub null_values: Vec<String>,
    /// Rows read to infer column types, by Polars and by datui's string typing.
    pub infer_rows: usize,
    /// Skip rows that do not parse instead of failing.
    pub ignore_errors: bool,
}

impl Default for CsvConfig {
    fn default() -> Self {
        Self {
            comment: None,
            header_join: crate::formats::csv_dialect::DEFAULT_HEADER_JOIN.to_string(),
            skip_initial_space: false,
            null_values: Vec::new(),
            infer_rows: 1000,
            ignore_errors: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DisplayConfig {
    /// Whether to draw box-drawing and arrow characters, or fall back to ASCII.
    pub unicode: crate::glyphs::UnicodeMode,
    pub row_numbers: RowNumbers,
    /// The first row's number.
    pub row_numbers_start: usize,
    /// Spacing between table columns: `"comfortable"`, `"compact"` or a count of cells.
    pub cell_padding: CellPadding,
    /// When true, colorize main table cells by column type (string, int, float, bool, temporal).
    pub column_colors: bool,
    /// Show a second header row naming each column's type. `D` toggles it for the session.
    pub type_row: bool,
    /// Accent the `i` key when datui has notes about the data the Info panel has not
    /// shown yet. Notes are collected either way.
    pub notes_accent: bool,
    /// Take the mouse (wheel scrolls, click selects); terminal text selection then needs
    /// its bypass modifier (usually Shift).
    pub mouse: bool,
    /// A fixed width for every sidebar (Info, Sort & Filter, Views, Pivot & Melt). None:
    /// each sidebar's own.
    pub sidebar_width: Option<u16>,
    /// Right-align numeric columns and their headers in the data table.
    pub right_align_numbers: bool,
    /// How numbers are displayed. Either a preset name (`number_format = "thousands"`)
    /// or a `[display.number_format]` table for finer control.
    pub number_format: NumberFormatConfig,
}

/// Whether `#` shows row numbers when a file opens: for text and logs (`"auto"`), or
/// for every format or none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RowNumbers {
    #[default]
    Auto,
    On,
    Off,
}

impl RowNumbers {}

impl From<bool> for RowNumbers {
    fn from(on: bool) -> Self {
        if on { Self::On } else { Self::Off }
    }
}

impl Serialize for RowNumbers {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Auto => serializer.serialize_str("auto"),
            Self::On => serializer.serialize_bool(true),
            Self::Off => serializer.serialize_bool(false),
        }
    }
}

impl<'de> Deserialize<'de> for RowNumbers {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Bool(bool),
            Name(String),
        }
        const EXPECTED: &str = "row_numbers is \"auto\", true or false";
        match Raw::deserialize(deserializer).map_err(|_| D::Error::custom(EXPECTED))? {
            Raw::Bool(on) => Ok(on.into()),
            Raw::Name(name) if name == "auto" => Ok(Self::Auto),
            Raw::Name(other) => Err(D::Error::custom(format!("{EXPECTED}, not {other:?}"))),
        }
    }
}

/// Spacing between the main table's columns, frozen and scrolling alike: a density
/// by name, or a count of cells.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum CellPadding {
    Cells(usize),
    Density(Density),
}

/// The named spacings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Density {
    /// One cell between columns: more columns on screen.
    Compact,
    /// Two cells between columns, the default.
    Comfortable,
}

impl Default for CellPadding {
    fn default() -> Self {
        Self::Density(Density::Comfortable)
    }
}

impl CellPadding {
    /// Cells between two columns.
    pub fn cells(self) -> u16 {
        match self {
            Self::Cells(n) => u16::try_from(n).unwrap_or(u16::MAX),
            Self::Density(Density::Compact) => 1,
            Self::Density(Density::Comfortable) => 2,
        }
    }
}

impl<'de> Deserialize<'de> for CellPadding {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Cells(usize),
            Name(String),
        }
        const EXPECTED: &str = "cell_padding is \"compact\", \"comfortable\" or a number of cells";
        match Raw::deserialize(deserializer).map_err(|_| D::Error::custom(EXPECTED))? {
            Raw::Cells(n) => Ok(Self::Cells(n)),
            Raw::Name(name) => match name.as_str() {
                "compact" => Ok(Self::Density(Density::Compact)),
                "comfortable" => Ok(Self::Density(Density::Comfortable)),
                other => Err(D::Error::custom(format!("{EXPECTED}, not {other:?}"))),
            },
        }
    }
}

/// Number display settings: a preset name (`number_format = "thousands"`) or a
/// `[display.number_format]` table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum NumberFormatConfig {
    /// Shorthand: one of [`NumberFormat::PRESET_NAMES`].
    Preset(String),
    /// Long form with individual overrides.
    Custom(Box<NumberFormatTable>),
}

impl Default for NumberFormatConfig {
    fn default() -> Self {
        // Default renders exactly as before, so upgrading changes nothing.
        NumberFormatConfig::Preset("none".to_string())
    }
}

/// Long-form number formatting options; unset fields take the value of the preset
/// named by `grouping` (or the default).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct NumberFormatTable {
    /// `none` | `thousands` | `indian` | `system` | any preset name.
    pub grouping: Option<String>,
    /// Character placed between digit groups.
    pub group_separator: Option<String>,
    /// Character used as the decimal point.
    pub decimal_separator: Option<String>,
    /// Whether float columns get grouping too.
    pub floats: Option<bool>,
    /// Fixed decimal places for floats. Unset keeps Polars' own rendering.
    pub float_precision: Option<u8>,
    /// Columns never formatted. Supports `*` and `?` globs.
    pub exclude_columns: Vec<String>,
    /// Unrecognized keys, kept so [`NumberFormatConfig::resolve`] can name a typo
    /// (otherwise it would look like no formatting, the default).
    #[serde(flatten)]
    pub unknown: std::collections::BTreeMap<String, toml::Value>,
}

/// Field names accepted inside `[display.number_format]`, for error messages.
const NUMBER_FORMAT_KEYS: &str =
    "grouping, group_separator, decimal_separator, floats, float_precision, exclude_columns";

impl NumberFormatConfig {
    /// Resolve into renderer settings. Errors on unknown presets, multi-character
    /// separators, and a group separator equal to the decimal one.
    pub fn resolve(&self, align_numeric_right: bool) -> Result<NumberFormatSettings> {
        let (format, exclude) = match self {
            NumberFormatConfig::Preset(name) => (Self::lookup_preset(name)?, Vec::new()),
            NumberFormatConfig::Custom(table) => {
                if !table.unknown.is_empty() {
                    let keys: Vec<&str> = table.unknown.keys().map(String::as_str).collect();
                    return Err(eyre!(
                        "display.number_format: unknown key{} {}. Expected one of: {}",
                        if keys.len() > 1 { "s" } else { "" },
                        keys.iter()
                            .map(|k| format!("'{}'", k))
                            .collect::<Vec<_>>()
                            .join(", "),
                        NUMBER_FORMAT_KEYS
                    ));
                }
                let base = match table.grouping.as_deref() {
                    Some(name) => Self::lookup_preset(name)?,
                    None => NumberFormat::PLAIN,
                };
                let mut fmt = base;
                if let Some(sep) = table.group_separator.as_deref() {
                    fmt.group_sep = Self::single_char(sep, "group_separator")?;
                }
                if let Some(sep) = table.decimal_separator.as_deref() {
                    fmt.decimal_sep = Self::single_char(sep, "decimal_separator")?;
                }
                if let Some(v) = table.floats {
                    fmt.floats = v;
                }
                if table.float_precision.is_some() {
                    fmt.float_precision = table.float_precision;
                }
                (fmt, table.exclude_columns.iter().map(Glob::new).collect())
            }
        };

        if format.grouping != Grouping::None && format.group_sep == format.decimal_sep {
            return Err(eyre!(
                "display.number_format: group_separator and decimal_separator are both '{}'; \
                 they must differ or numbers become ambiguous",
                format.group_sep
            ));
        }

        // Formatting is on only if the user configured something. Otherwise `,` still needs
        // a target: Thousands grouping, keeping any other settings they chose.
        let enabled = !format.is_noop();
        let format = if enabled {
            format
        } else {
            NumberFormat {
                grouping: Grouping::Thousands,
                ..format
            }
        };

        Ok(NumberFormatSettings {
            format,
            enabled,
            exclude,
            align_numeric_right,
        })
    }

    /// Override only the grouping style, keeping the user's long-form settings
    /// (`exclude_columns`, `min_digits`, precision).
    pub fn with_grouping_override(&self, name: &str) -> Self {
        match self {
            NumberFormatConfig::Preset(_) => NumberFormatConfig::Preset(name.to_string()),
            NumberFormatConfig::Custom(table) => {
                let mut table = table.clone();
                table.grouping = Some(name.to_string());
                NumberFormatConfig::Custom(table)
            }
        }
    }

    /// Resolve a preset name, expanding the opt-in `system` value.
    fn lookup_preset(name: &str) -> Result<NumberFormat> {
        // "system" is the only locale-dependent value, opt-in: data files are
        // locale-neutral.
        let name = if name == "system" {
            match numfmt::system_locale_tag() {
                Some(tag) => numfmt::preset_for_locale_tag(&tag),
                // Unset or C/POSIX: group plainly rather than do nothing.
                None => "thousands",
            }
        } else {
            name
        };
        NumberFormat::preset(name).ok_or_else(|| {
            eyre!(
                "display.number_format: unknown value '{}'. Expected one of: {}, system",
                name,
                NumberFormat::PRESET_NAMES.join(", ")
            )
        })
    }

    fn single_char(s: &str, field: &str) -> Result<char> {
        let mut chars = s.chars();
        match (chars.next(), chars.next()) {
            (Some(c), None) => Ok(c),
            _ => Err(eyre!(
                "display.number_format.{}: expected a single character, got {:?}",
                field,
                s
            )),
        }
    }
}

/// Rows an analysis samples by default: stable distributions and correlations to two
/// decimals, read in seconds.
pub const DEFAULT_ANALYSIS_SAMPLE_ROWS: usize = 100_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PerformanceConfig {
    /// Pages of rows buffered ahead of and behind the screen.
    pub pages_ahead: usize,
    pub pages_behind: usize,
    /// Most rows the table buffers between reads; 0 for no limit.
    pub max_buffered_rows: usize,
    /// Most memory buffered rows may take, estimated from the schema; 0 for no limit. Caps
    /// rows kept between reads, not the process.
    pub max_buffered: ByteSize,
    /// Use the Polars streaming engine for collects where it applies.
    pub streaming: bool,
    /// Most threads Polars computes with; 0 for every core. Applied once, at startup.
    pub threads: usize,
}

impl PerformanceConfig {
    /// `max_buffered` in whole MiB (rounded up, so a nonzero cap is at least one).
    pub fn max_buffered_mb(&self) -> usize {
        usize::try_from(self.max_buffered.bytes().div_ceil(1 << 20)).unwrap_or(usize::MAX)
    }
}

/// Default for `analysis.quality_local_copy`: 2 GiB.
pub const DEFAULT_QUALITY_LOCAL_COPY: ByteSize = ByteSize::mib(2048);

/// Default maximum rows used for chart data when not overridden by config or UI.
pub const DEFAULT_CHART_ROW_LIMIT: usize = 10_000;
/// Maximum chart row limit (Polars slice takes u32).
pub const MAX_CHART_ROW_LIMIT: usize = u32::MAX as usize;

/// `[analysis]`: Analysis, Data Quality and charts.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AnalysisConfig {
    /// The analysis sample's starting size: rows every tool reads, spread across a larger
    /// table. 0 starts at every row.
    pub sample_rows: usize,
    /// Rows a chart reads: all up to n, else a sample of n spread across the table.
    pub chart_rows: usize,
    /// Whether a chart starts with its grid at the major ticks.
    pub chart_grid: bool,
    /// The most a Data Quality full scan may copy of a remote dataset into the cache, to
    /// read objects once instead of per pass. 0 never copies.
    pub quality_local_copy: ByteSize,
    /// The most memory a view's sample may take. Unset: available memory decides, before
    /// and during the draw. 0: no warning, no stop.
    pub sample_memory_limit: Option<ByteSize>,
}

impl Default for AnalysisConfig {
    fn default() -> Self {
        Self {
            sample_rows: DEFAULT_ANALYSIS_SAMPLE_ROWS,
            chart_rows: DEFAULT_CHART_ROW_LIMIT,
            chart_grid: false,
            quality_local_copy: DEFAULT_QUALITY_LOCAL_COPY,
            sample_memory_limit: None,
        }
    }
}

/// Which built-in color defaults to start from. Chrome colors (header fills, striping,
/// borders) must sit near the background without matching it, which no ANSI color
/// expresses, so they are fixed per set.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThemeMode {
    /// Detect from the environment, falling back to `Dark`.
    #[default]
    Auto,
    Dark,
    Light,
}

impl ThemeMode {
    /// Resolve `Auto` from the environment (`Dark` and `Light` pass through): `COLORFGBG`
    /// with background 7 or 15 means light; otherwise dark. The terminal's own answer,
    /// when it comes, replaces this (`crate::app::terminal_color`).
    pub fn resolve(self) -> Self {
        match self {
            Self::Auto => detect_terminal_mode(),
            other => other,
        }
    }
}

/// Best-effort light/dark detection from `COLORFGBG`. Defaults to `Dark`.
fn detect_terminal_mode() -> ThemeMode {
    let Ok(raw) = std::env::var("COLORFGBG") else {
        return ThemeMode::Dark;
    };
    // Format is "fg;bg" or "fg;default;bg" — the background is the last field.
    match raw
        .rsplit(';')
        .next()
        .and_then(|b| b.trim().parse::<u8>().ok())
    {
        Some(7) | Some(15) => ThemeMode::Light,
        _ => ThemeMode::Dark,
    }
}

/// Where datui looks for datasets on home: a short list of places, `PATH`-shaped; it
/// records nothing about what it finds there.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HomeConfig {
    /// Also offer directories the desktop records you opening data from (never file
    /// names).
    pub desktop_recents: bool,
    /// List unreadable files, dimmed, from the start; `Ctrl+A` flips it per session.
    pub show_unreadable: bool,
    /// Catalogs never shown on the home screen, by id: `mine`, `public`, or a listed
    /// file's name.
    pub hide: Vec<String>,
    /// The largest local file whose first rows home reads for its preview (Parquet: its
    /// average row group). Those rows are the open's first page, read once. 0 turns
    /// previews off.
    pub preview_max: ByteSize,
    /// Recursive search of the working directory from the home screen's filter.
    pub search: SearchConfig,
}

/// Recursive search under the working directory, driven by home's filter: one
/// background walk on first typing, then each keystroke scores its results off the
/// UI thread. These limits bound the walk and the list.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SearchConfig {
    /// Search below the working directory at all.
    pub enabled: bool,
    /// How deep to descend; data is rarely deep, and every branch pays.
    pub max_depth: usize,
    /// List at most this many matches, best first; the heading counts the rest. The walk
    /// keeps every data file, so no match is lost.
    pub max_results: usize,
    /// Stop walking after this long, keeping what was found: a huge tree degrades to
    /// partial results, never a wait.
    pub time_budget: Interval,
    /// Descend into other filesystems. Off by default: it keeps the walk off network
    /// shares, and off autofs mounts it would trigger.
    pub cross_filesystems: bool,
    /// Obey `.gitignore`. Off by default: data directories are gitignored because they
    /// are too big to commit, the reason to open them here. The skip list handles noise.
    pub follow_gitignore: bool,
    /// Directory names never descended into. Replaces the defaults entirely.
    pub skip: Vec<String>,
    /// Directory names to skip in addition to the defaults.
    pub skip_extra: Vec<String>,
    /// File extensions searched for. Empty: every format datui opens, `json` and `txt`
    /// included (noisy in a source tree).
    pub extensions: Vec<String>,
}

/// Directories that are never data and always expensive. Hidden ones are skipped
/// already; these are the visible offenders, full of openable `.json`.
pub const DEFAULT_SEARCH_SKIP: &[&str] = &[
    "node_modules",
    "target",
    "build",
    "dist",
    "vendor",
    "site-packages",
    "__pycache__",
    "venv",
    "env",
];

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_depth: 8,
            max_results: 1_000,
            time_budget: Interval::ms(1_500),
            cross_filesystems: false,
            follow_gitignore: false,
            skip: DEFAULT_SEARCH_SKIP.iter().map(|s| s.to_string()).collect(),
            skip_extra: Vec::new(),
            extensions: Vec::new(),
        }
    }
}

impl SearchConfig {
    /// Every directory name to skip: the configured list plus the additions.
    pub fn skipped_dirs(&self) -> Vec<String> {
        let mut out = self.skip.clone();
        out.extend(self.skip_extra.iter().cloned());
        out
    }
}

impl Default for HomeConfig {
    fn default() -> Self {
        Self {
            // On by default: it only adds places, giving a fresh install somewhere to point.
            desktop_recents: true,
            show_unreadable: false,
            hide: Vec::new(),
            preview_max: ByteSize::mib(64),
            search: SearchConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ThemeConfig {
    /// Which mode's theme to use; `None` (absent) means `Auto`. A loaded config holds the
    /// resolved mode.
    pub mode: Option<ThemeMode>,
    /// The theme used when the terminal is dark: a built-in's name or a file's in
    /// `themes/`.
    pub dark: String,
    /// The theme used when the terminal is light.
    pub light: String,
    pub colors: ColorConfig,
    /// The mode was `auto`: the palette follows the terminal's background, at startup
    /// and when asked again. Set by `from_layers`.
    #[serde(skip)]
    pub follow: bool,
    /// The `theme.colors` slots the config set, laid over the active theme in either
    /// mode.
    #[serde(skip)]
    pub overrides: toml::Table,
    /// The themes there are, read from the config directory's `themes/`.
    #[serde(skip)]
    pub library: crate::config::themes::Library,
    /// The theme in use for each mode: `dark` and `light`, or the built-in when the named
    /// one could not be used.
    #[serde(skip)]
    pub dark_theme: String,
    #[serde(skip)]
    pub light_theme: String,
    /// Each mode's theme resolved, before `overrides`.
    #[serde(skip)]
    pub dark_palette: ColorConfig,
    #[serde(skip)]
    pub light_palette: ColorConfig,
    /// Why a named theme was not used, one line each, for a warning.
    #[serde(skip)]
    pub problems: Vec<String>,
    /// The same, without the why: short enough for the footer.
    #[serde(skip)]
    pub fallbacks: Vec<String>,
}

impl Default for ThemeConfig {
    fn default() -> Self {
        Self {
            mode: None,
            dark: crate::config::themes::NIGHT_MARKET.to_string(),
            light: crate::config::themes::DAY_MARKET.to_string(),
            colors: ColorConfig::default(),
            follow: false,
            overrides: toml::Table::new(),
            library: crate::config::themes::Library::default(),
            dark_theme: crate::config::themes::NIGHT_MARKET.to_string(),
            light_theme: crate::config::themes::DAY_MARKET.to_string(),
            dark_palette: ColorConfig::dark(),
            light_palette: ColorConfig::light(),
            problems: Vec::new(),
            fallbacks: Vec::new(),
        }
    }
}

impl ThemeConfig {
    /// The theme for `mode` with the configured slots laid over it.
    pub fn palette_for(&self, mode: ThemeMode) -> Result<ColorConfig> {
        let base = match mode.resolve() {
            ThemeMode::Light => &self.light_palette,
            _ => &self.dark_palette,
        };
        let mut palette = crate::config::themes::slots(base);
        palette.extend(self.overrides.clone());
        Ok(toml::Value::Table(palette).try_into()?)
    }

    /// Every theme file left out and every name not used, one warning each.
    pub fn warnings(&self) -> Vec<String> {
        let broken = self
            .library
            .broken
            .iter()
            .map(|b| format!("warning: theme left out: {}", b.full()));
        let problems = self.problems.iter().map(|p| format!("warning: {p}"));
        broken.chain(problems).collect()
    }

    /// Resolve `dark` and `light` against `library`, keeping it. An unusable name falls
    /// back to its mode's built-in, noted in `problems` when that mode can be in use.
    pub fn use_library(&mut self, library: crate::config::themes::Library, active: ThemeMode) {
        self.problems.clear();
        self.fallbacks.clear();
        for mode in [ThemeMode::Dark, ThemeMode::Light] {
            let (key, name) = match mode {
                ThemeMode::Light => ("theme.light", self.light.clone()),
                _ => ("theme.dark", self.dark.clone()),
            };
            let (used, palette) = match library.resolve(&name, mode) {
                Ok(palette) => (name, palette),
                Err(why) => {
                    let fallback = crate::config::themes::built_in_name(mode);
                    if self.follow || active == mode {
                        let short = format!("{key}: using {fallback}, not {name}");
                        self.problems.push(format!("{short}: {why}"));
                        self.fallbacks.push(short);
                    }
                    (fallback.to_string(), ColorConfig::for_mode(mode))
                }
            };
            match mode {
                ThemeMode::Light => (self.light_theme, self.light_palette) = (used, palette),
                _ => (self.dark_theme, self.dark_palette) = (used, palette),
            }
        }
        self.library = library;
    }
}

/// Color configuration for the application theme: one slot per role, each a name
/// (`"cyan"`, `"default"`), `"#rrggbb"` or `"indexed(N)"`. The option registry
/// documents every slot and holds its dark and light defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ColorConfig {
    pub chip_key: String,
    pub chip_label: String,
    pub throbber: String,
    pub success: String,
    pub error: String,
    pub warning: String,
    pub dimmed: String,
    pub background: String,
    pub surface: String,
    pub controls_bg: String,
    pub text_primary: String,
    pub text_secondary: String,
    pub text_inverse: String,
    pub table_header: String,
    pub table_header_bg: String,
    /// The row-number column. "default" is the terminal's.
    pub table_row_numbers: String,
    pub table_column_separator: String,
    /// Tint under the current row; "reversed" swaps text and background instead.
    pub table_selected: String,
    /// Tint under the column cursor's cells.
    pub table_column_cursor: String,
    /// The column cursor's header and the current cell.
    pub table_cell_cursor: String,
    /// Every other row; "default" turns the stripe off.
    pub table_alternate_row: String,
    pub sidebar_border: String,
    pub modal_border_active: String,
    pub modal_border_error: String,
    pub distribution_normal: String,
    pub distribution_skewed: String,
    pub distribution_other: String,
    pub outlier_marker: String,
    /// The text caret; "default" reverses the text under it.
    pub input_cursor: String,
    /// Text under the caret block; "default" picks black or white by contrast.
    pub input_cursor_text: String,
    /// Cells and headers by column type.
    pub type_str: String,
    pub type_int: String,
    pub type_float: String,
    pub type_bool: String,
    pub type_temporal: String,
    /// The `‹binary›` stub of a binary column.
    pub type_binary: String,
    /// The chart series, in order; `chart_1` is also histogram bars and Q-Q points.
    pub chart_1: String,
    pub chart_2: String,
    pub chart_3: String,
    pub chart_4: String,
    pub chart_5: String,
    pub chart_6: String,
    pub chart_7: String,
    pub chart_8: String,
    pub chart_9: String,
    pub chart_10: String,
    /// The chart grid, a shade dimmer than `dimmed`.
    pub chart_grid: String,
    /// The one color meaning "this is the thing": focused titles, key chips, the selection
    /// rail.
    pub accent: String,
    /// A brighter accent for a focused title or a value that just changed.
    pub accent_bright: String,
    /// Two stops for home's wordmark, used nowhere else: on data it would be decoration.
    pub gradient_start: String,
    pub gradient_end: String,
    /// Behind the cell a find landed on; its text takes black or white by contrast.
    pub find_match: String,
    /// The hex view's bytes by class, as hexyl colors them.
    pub hex_null: String,
    pub hex_printable: String,
    pub hex_whitespace: String,
    pub hex_control: String,
    pub hex_high: String,
    pub hex_ff: String,
}

/// `[http]`: what every request datui makes says about it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct HttpConfig {
    /// The User-Agent header; empty sends [`crate::cloud::user_agent::DEFAULT`].
    pub user_agent: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct QueryConfig {
    pub history_limit: usize,
    /// Remember queries.
    pub history: bool,
    pub default_mode: QueryMode,
}

/// The language the command line runs a query in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum QueryMode {
    #[default]
    Sql,
    /// Datui's q-inspired language.
    Q,
}

impl QueryMode {
    /// The languages this build offers; SQL only with the `sql` feature.
    pub fn available() -> &'static [QueryMode] {
        #[cfg(feature = "sql")]
        {
            &[QueryMode::Sql, QueryMode::Q]
        }
        #[cfg(not(feature = "sql"))]
        {
            &[QueryMode::Q]
        }
    }

    /// This language if the build offers it, otherwise q.
    pub fn resolve(self) -> QueryMode {
        if Self::available().contains(&self) {
            self
        } else {
            QueryMode::Q
        }
    }

    /// The prefix as the command line draws it: `sql:`, `q:`.
    pub fn prefix_colon(self) -> &'static str {
        match self {
            QueryMode::Sql => "sql:",
            QueryMode::Q => "q:",
        }
    }

    /// The other language, where the build has one.
    pub fn next(self) -> QueryMode {
        let modes = Self::available();
        let at = modes.iter().position(|&m| m == self).unwrap_or(0);
        modes[(at + 1) % modes.len()]
    }
}

/// `[chart]`: charts exported to a file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ChartConfig {
    /// Whether an exported chart carries its recipe: the source, query, chart and
    /// sample it was made from. The export dialog's Recipe row starts from it.
    pub export_recipe: bool,
}

impl Default for ChartConfig {
    fn default() -> Self {
        Self {
            export_recipe: true,
        }
    }
}

/// `[views]`: saved views.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ViewsConfig {
    pub auto_apply: bool,
}

/// `[log]`: the log file and how much it says.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct LogConfig {
    /// Where the log goes. Unset: `datui.log` in the cache directory.
    pub file: Option<String>,
    /// error, warn, info, debug, trace or off. Unset: `DATUI_LOG`, else warn.
    pub level: Option<String>,
}

/// `[formats]`: where format specs and dictionaries are found.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct FormatsConfig {
    /// Directories (or files) of specs and dictionaries, searched after the config
    /// directory's `formats` and `$DATUI_FORMATS_PATH`. Adds up across imports.
    pub path: Vec<String>,
}

/// `[clipboard]`: how the copy dialog reaches the system clipboard.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ClipboardConfig {
    /// "auto", "native" (display server via arboard) or "osc52" (a terminal escape; works
    /// over SSH).
    pub backend: String,
    /// Longest OSC 52 payload to attempt, as base64; terminals cap what they accept.
    pub osc52_limit: ByteSize,
}

impl Default for ClipboardConfig {
    fn default() -> Self {
        Self {
            backend: "auto".to_string(),
            osc52_limit: ByteSize::kib(100),
        }
    }
}

/// `[glyphs]`: per-slot overrides over the Unicode set, for fonts with more than the
/// coverage floor. Keys are `glyphs.rs` slot names; values keep the replaced glyph's
/// width. The ASCII set is never overridden. Layered like every section.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GlyphsConfig {
    #[serde(flatten)]
    pub overrides: std::collections::BTreeMap<String, crate::glyphs::SlotOverride>,
}

/// `[limits]`: the most of a file some readers take in. Each note or error that says a
/// cap was reached names its key here.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct LimitsConfig {
    /// Records one pass indexes, all types together (4 bytes each under 4 GiB, 8 past).
    pub indexed_records: usize,
    pub elf_symbols: usize,
    pub midi_bytes: ByteSize,
    /// Events of every MIDI file of one open together.
    pub midi_events: usize,
    /// Rows of a list on an Info panel tab.
    pub detail_rows: usize,
    /// Journal JSON read into memory, all files of one open together.
    pub journal_bytes: ByteSize,
    /// Fields of an SDF file, each a column.
    pub sdf_fields: usize,
    /// Signals of a VCD file, each a column.
    pub vcd_signals: usize,
    /// Tags of a FIX file, each a column.
    pub fix_tags: usize,
    /// Fields of one FIX message.
    pub fix_fields: usize,
    /// Extension fields of a GPX file, each a column.
    pub gpx_fields: usize,
    /// A NumPy file's header.
    pub npy_header_bytes: ByteSize,
}

impl LimitsConfig {
    pub const DEFAULT: Self = Self {
        indexed_records: 64 << 20,
        elf_symbols: 10_000_000,
        midi_bytes: ByteSize::mib(64),
        midi_events: 10_000_000,
        detail_rows: 10_000,
        journal_bytes: ByteSize::mib(1024),
        sdf_fields: 4096,
        vcd_signals: 1 << 20,
        fix_tags: 4096,
        fix_fields: 4096,
        gpx_fields: 256,
        npy_header_bytes: ByteSize::mib(4),
    };
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        let mut config = Self {
            import: Vec::new(),
            catalogs: Vec::new(),
            read_catalogs: Vec::new(),
            catalog_dir: None,
            broken_catalogs: Vec::new(),
            read: ReadConfig::default(),
            csv: CsvConfig::default(),
            display: DisplayConfig::default(),
            performance: PerformanceConfig::default(),
            analysis: AnalysisConfig::default(),
            chart: ChartConfig::default(),
            home: HomeConfig::default(),
            cloud: CloudConfig::default(),
            http: HttpConfig::default(),
            query: QueryConfig::default(),
            views: ViewsConfig::default(),
            clipboard: ClipboardConfig::default(),
            formats: FormatsConfig::default(),
            limits: LimitsConfig::DEFAULT,
            log: LogConfig::default(),
            theme: ThemeConfig::default(),
            glyphs: GlyphsConfig::default(),
        };
        config.sync_dataset_access();
        config
    }
}

impl Default for DisplayConfig {
    fn default() -> Self {
        Self {
            unicode: crate::glyphs::UnicodeMode::default(),
            row_numbers: RowNumbers::Auto,
            row_numbers_start: 1,
            cell_padding: CellPadding::default(),
            column_colors: true,
            type_row: true,
            notes_accent: true,
            mouse: true,
            sidebar_width: None,
            right_align_numbers: true,
            number_format: NumberFormatConfig::default(),
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            pages_ahead: 3,
            pages_behind: 3,
            max_buffered_rows: crate::table::DEFAULT_MAX_BUFFERED_ROWS,
            max_buffered: ByteSize::mib(512),
            streaming: true,
            threads: 0,
        }
    }
}

impl Default for ColorConfig {
    /// Dark, datui's historical defaults; light is opt-in via `theme.mode`.
    fn default() -> Self {
        Self::dark()
    }
}

impl ColorConfig {
    /// The built-in set for whichever mode is in effect.
    pub fn for_mode(mode: ThemeMode) -> Self {
        match mode.resolve() {
            ThemeMode::Light => Self::light(),
            _ => Self::dark(),
        }
    }

    /// Defaults tuned for a dark terminal background.
    pub fn dark() -> Self {
        // "Night Market": Tokyo Night's palette with one cyan accent. Chrome sits in three
        // tiers a few percent apart, and the current row is tinted, not reversed, so cell
        // colors survive.
        Self {
            chip_key: "#7dcfff".to_string(),
            chip_label: "#a9b1d6".to_string(),
            throbber: "#7dcfff".to_string(),
            success: "#9ece6a".to_string(),
            error: "#f7768e".to_string(),
            warning: "#e0af68".to_string(),
            dimmed: "#565f89".to_string(),
            background: "default".to_string(),
            surface: "default".to_string(),
            controls_bg: "#262a3f".to_string(),
            text_primary: "default".to_string(),
            text_secondary: "#737aa2".to_string(),
            text_inverse: "#1a1b26".to_string(),
            table_header: "#c0caf5".to_string(),
            table_header_bg: "#2b3047".to_string(),
            table_row_numbers: "#565f89".to_string(),
            table_column_separator: "#3b4261".to_string(),
            table_selected: "#283457".to_string(),
            // A grey a step off the stripe for the column, lighter where it crosses the current
            // row.
            table_column_cursor: "#292e42".to_string(),
            table_cell_cursor: "#3b4261".to_string(),
            // Box titles use the border color, so it must read as text.
            sidebar_border: "#565f89".to_string(),
            modal_border_active: "#7dcfff".to_string(),
            modal_border_error: "#f7768e".to_string(),
            distribution_normal: "#9ece6a".to_string(),
            distribution_skewed: "#e0af68".to_string(),
            distribution_other: "#c0caf5".to_string(),
            outlier_marker: "#f7768e".to_string(),
            input_cursor: "default".to_string(),
            input_cursor_text: "default".to_string(),
            table_alternate_row: "#1e2030".to_string(),
            type_str: "#9ece6a".to_string(),
            type_int: "#7aa2f7".to_string(),
            type_float: "#2ac3de".to_string(),
            type_bool: "#e0af68".to_string(),
            type_temporal: "#bb9af7".to_string(),
            type_binary: "#565f89".to_string(),
            chart_1: "#7dcfff".to_string(),
            chart_2: "#bb9af7".to_string(),
            chart_3: "#9ece6a".to_string(),
            chart_4: "#e0af68".to_string(),
            chart_5: "#7aa2f7".to_string(),
            chart_6: "#f7768e".to_string(),
            chart_7: "#ff9e64".to_string(),
            // Teal, pink-magenta and light yellow: apart by lightness as well as hue, for common
            // color-vision deficiencies.
            chart_8: "#1abc9c".to_string(),
            chart_9: "#ff5fd2".to_string(),
            chart_10: "#f4ef8a".to_string(),
            // Dimmer than `dimmed`, but blue rather than black (the background) on 16 colors.
            chart_grid: "#3d4785".to_string(),
            accent: "#7dcfff".to_string(),
            accent_bright: "#a4daff".to_string(),
            gradient_start: "#7aa2f7".to_string(),
            gradient_end: "#bb9af7".to_string(),
            find_match: "#e0af68".to_string(),
            hex_null: "#565f89".to_string(),
            hex_printable: "#7dcfff".to_string(),
            hex_whitespace: "#9ece6a".to_string(),
            hex_control: "#bb9af7".to_string(),
            hex_high: "#e0af68".to_string(),
            hex_ff: "#f7768e".to_string(),
        }
    }

    /// Defaults for a light terminal: chrome shades darker than the background rather
    /// than lighter, and hues illegible on white (plain cyan, yellow) replaced.
    pub fn light() -> Self {
        // Tokyo Night "day": the same hues, darkened to clear 4.5:1 on white; chrome tiers
        // darker than the terminal.
        Self {
            chip_key: "#2e7de9".to_string(),
            chip_label: "#3760bf".to_string(),
            throbber: "#2e7de9".to_string(),
            success: "#587539".to_string(),
            error: "#f52a65".to_string(),
            warning: "#8c6c3e".to_string(),
            dimmed: "#848cb5".to_string(),
            background: "default".to_string(),
            surface: "default".to_string(),
            controls_bg: "#d0d5e3".to_string(),
            text_primary: "default".to_string(),
            text_secondary: "#6172b0".to_string(),
            text_inverse: "#e1e2e7".to_string(),
            table_header: "#3760bf".to_string(),
            table_header_bg: "#c4c8da".to_string(),
            table_row_numbers: "#848cb5".to_string(),
            table_column_separator: "#a8aecb".to_string(),
            table_selected: "#b6bfe2".to_string(),
            table_column_cursor: "#cbd3f2".to_string(),
            table_cell_cursor: "#a0aef0".to_string(),
            sidebar_border: "#6172b0".to_string(),
            modal_border_active: "#2e7de9".to_string(),
            modal_border_error: "#f52a65".to_string(),
            distribution_normal: "#587539".to_string(),
            distribution_skewed: "#8c6c3e".to_string(),
            distribution_other: "#3760bf".to_string(),
            outlier_marker: "#f52a65".to_string(),
            input_cursor: "default".to_string(),
            input_cursor_text: "default".to_string(),
            table_alternate_row: "#dcdfea".to_string(),
            type_str: "#587539".to_string(),
            type_int: "#2e7de9".to_string(),
            type_float: "#007197".to_string(),
            type_bool: "#8c6c3e".to_string(),
            type_temporal: "#9854f1".to_string(),
            type_binary: "#848cb5".to_string(),
            chart_1: "#2e7de9".to_string(),
            chart_2: "#9854f1".to_string(),
            chart_3: "#587539".to_string(),
            chart_4: "#8c6c3e".to_string(),
            chart_5: "#007197".to_string(),
            chart_6: "#f52a65".to_string(),
            chart_7: "#b15c00".to_string(),
            // A yellow does not read on white: a deep navy takes its place.
            chart_8: "#118c74".to_string(),
            chart_9: "#d1188c".to_string(),
            chart_10: "#24357a".to_string(),
            // The cyan halfway to the background: a lighter grey is white on 16 colors.
            chart_grid: "#70aabf".to_string(),
            accent: "#2e7de9".to_string(),
            accent_bright: "#1a6cd0".to_string(),
            gradient_start: "#2e7de9".to_string(),
            gradient_end: "#9854f1".to_string(),
            find_match: "#f0c35a".to_string(),
            hex_null: "#848cb5".to_string(),
            hex_printable: "#007197".to_string(),
            hex_whitespace: "#587539".to_string(),
            hex_control: "#9854f1".to_string(),
            hex_high: "#8c6c3e".to_string(),
            hex_ff: "#f52a65".to_string(),
        }
    }
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            history_limit: 1000,
            history: true,
            default_mode: QueryMode::default(),
        }
    }
}

/// Maximum depth of an `import` chain; deeper is a mistake, reported clearly.
const MAX_IMPORT_DEPTH: usize = 8;

/// Expand a leading `~` and any `$VAR` / `${VAR}` in a config path; unset variables
/// expand to nothing, as in a shell.
pub fn expand_config_path(raw: &str) -> PathBuf {
    expand_path(raw)
}

/// `path` with a leading `~` expanded, nothing else: for command-line paths, which
/// cmd, older PowerShell and quoting pass through with `~` (a `$` was already
/// expanded and is part of a name). A path that exists as typed (a file named `~`)
/// is kept.
pub fn expand_home(path: &Path) -> PathBuf {
    expand_home_unless(path, |p| p.symlink_metadata().is_ok())
}

fn expand_home_unless(path: &Path, there: impl FnOnce(&Path) -> bool) -> PathBuf {
    path.to_str()
        .and_then(home_path)
        .filter(|_| !there(path))
        .unwrap_or_else(|| path.to_path_buf())
}

/// `~`, `~/x` and, on Windows, `~\x` under the home directory; `None` for anything
/// else, or with no home directory.
fn home_path(text: &str) -> Option<PathBuf> {
    if text == "~" {
        return dirs::home_dir();
    }
    let rest = text
        .strip_prefix("~/")
        // What `display_path` writes there, and what a Windows user types.
        .or_else(|| text.strip_prefix("~\\").filter(|_| cfg!(windows)))?;
    dirs::home_dir().map(|home| home.join(rest))
}

/// `path` normalized without the filesystem: rebuilt from components so separators
/// compare equal (Windows mixes `\` and `/` after expansion), `.` and a trailing
/// separator dropped. `..` stays: past a symlink it is not the textual parent.
pub(crate) fn path_place(path: &Path) -> PathBuf {
    use std::path::{Component, Prefix};
    let mut place = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Prefix(prefix) => match prefix.kind() {
                Prefix::Disk(drive) => {
                    place.push(format!("{}:", char::from(drive.to_ascii_uppercase())));
                }
                Prefix::UNC(server, share) => {
                    let mut unc = std::ffi::OsString::from(r"\\");
                    unc.push(server);
                    unc.push(r"\");
                    unc.push(share);
                    place.push(unc);
                }
                _ => place.push(prefix.as_os_str()),
            },
            other => place.push(other),
        }
    }
    place
}

pub(crate) fn expand_path(raw: &str) -> PathBuf {
    let mut expanded = String::with_capacity(raw.len());
    let mut chars = raw.chars().peekable();

    while let Some(c) = chars.next() {
        if c != '$' {
            expanded.push(c);
            continue;
        }

        let braced = chars.peek() == Some(&'{');
        if braced {
            chars.next();
        }

        let mut name = String::new();
        while let Some(&next) = chars.peek() {
            if braced && next == '}' {
                chars.next();
                break;
            }
            if !next.is_ascii_alphanumeric() && next != '_' {
                break;
            }
            name.push(next);
            chars.next();
        }

        if name.is_empty() {
            // A bare `$`, or `${}` — leave it as written rather than guessing.
            expanded.push('$');
        } else if let Ok(value) = std::env::var(&name) {
            expanded.push_str(&value);
        }
    }

    // `~` expands only at the start of the path, as in a shell.
    home_path(&expanded).unwrap_or_else(|| PathBuf::from(expanded))
}

/// One config file's settings as written. Partial, so a later file can set a value
/// back to its default and an omitted key changes nothing;
/// [`AppConfig::from_layers`] fills defaults once after merging.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConfigLayer {
    table: toml::Table,
    /// The files this one imports, as written. Never merged: a load-time directive.
    imports: Vec<String>,
}

/// Where a layer of the configuration came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LayerSource {
    /// A config file: the user's, or one it imports.
    File(PathBuf),
    /// `-c KEY=VALUE` on the command line.
    Override,
}

impl std::fmt::Display for LayerSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(path) => write!(f, "{}", path.display()),
            Self::Override => f.write_str("-c"),
        }
    }
}

/// How a key combines across layers when a later layer does not simply replace it.
#[derive(Debug, Clone, Copy)]
enum Combine {
    /// An array of tables matched by `name`: an entry replaces the earlier one of its
    /// name, a new name appends; duplicates within one file are kept for validation.
    ByName,
    /// A list that adds up across files, without repeats.
    Union,
}

/// Keys that do not simply replace: tables merge key by key, these combine; all else
/// is replaced whole.
const COMBINED_KEYS: &[(&str, Combine)] = &[
    ("formats.path", Combine::Union),
    ("catalogs", Combine::Union),
    ("cloud.connections", Combine::ByName),
    ("cloud.hide", Combine::Union),
    ("cloud.env_files", Combine::Union),
    ("home.hide", Combine::Union),
];

/// The line after a config mistake saying how to proceed: a missing import is
/// skipped, and `datui config init` writes a root file only where there is none.
pub fn way_out(imported: bool, what: &str) -> String {
    if imported {
        format!("Fix that {what}, or move the file aside: a missing import is skipped.")
    } else {
        format!(
            "Fix that {what}, or move the file aside to start from the defaults; \
             `datui config init` then writes a fresh one."
        )
    }
}

impl ConfigLayer {
    /// A layer from TOML text, type-checked here so a mistake names its file.
    pub fn parse(text: &str) -> Result<Self> {
        let typed: AppConfig = toml::from_str(text)?;
        let table: toml::Table = toml::from_str(text)?;
        Ok(Self::from_table(table, typed.import))
    }

    /// The layer `-c KEY=VALUE` makes, the last of one key winning. Keys and shapes were
    /// checked on the command line; types are checked here.
    pub fn from_overrides(overrides: &[datui_cli::settings::Override]) -> Result<Self> {
        let mut table = toml::Table::new();
        for o in overrides {
            let mut at = &mut table;
            let mut parts: Vec<&str> = o.key.split('.').collect();
            let last = parts.pop().unwrap_or_default();
            for part in parts {
                let entry = at
                    .entry(part.to_string())
                    .or_insert_with(|| toml::Value::Table(toml::Table::new()));
                if !entry.is_table() {
                    *entry = toml::Value::Table(toml::Table::new());
                }
                at = entry.as_table_mut().expect("just made a table");
            }
            at.insert(last.to_string(), o.value.clone());
        }
        toml::Value::Table(table.clone())
            .try_into::<AppConfig>()
            .map_err(|e| eyre!("-c: {}", e.message().trim_end()))?;
        Ok(Self::from_table(table, Vec::new()))
    }

    fn from_table(mut table: toml::Table, imports: Vec<String>) -> Self {
        table.remove("import");
        Self { table, imports }
    }

    /// The layer in `path`, or `None` without the file. An unreadable or unparsable file
    /// is an error naming it and its `importer`, if any.
    fn read(path: &Path, importer: Option<&Path>) -> Result<Option<Self>> {
        // On the first line, ahead of a parse error's excerpt of the file.
        let named = match importer {
            Some(importer) => format!("{} (imported by {})", path.display(), importer.display()),
            None => path.display().to_string(),
        };
        let content = match std::fs::read_to_string(path) {
            Ok(content) => content,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(eyre!("Failed to read config file at {named}: {e}")),
        };
        let mut layer = Self::parse(&content).map_err(|e| {
            eyre!(
                "Failed to parse config file at {named}: {}\n{}",
                parse_reason(&e),
                way_out(importer.is_some(), "line")
            )
        })?;
        // Serde skips unknown keys; warn, or a typo changes nothing silently.
        for unknown in unknown_keys_in(&layer.table) {
            eprintln!("datui: warning: {}: {unknown}", path.display());
        }
        layer.anchor_paths(path.parent().unwrap_or_else(|| Path::new(".")));
        Ok(Some(layer))
    }

    /// Resolve relative format and catalog paths against `dir` (the naming file's
    /// directory) before merging with layers from elsewhere.
    fn anchor_paths(&mut self, dir: &Path) {
        if let Some(toml::Value::Array(entries)) = self
            .table
            .get_mut("formats")
            .and_then(|f| f.get_mut("path"))
        {
            for entry in entries {
                if let toml::Value::String(path) = entry
                    && !path.trim().is_empty()
                    && expand_path(path).is_relative()
                {
                    *path = dir.join(expand_path(path)).to_string_lossy().into_owned();
                }
            }
        }
        if let Some(toml::Value::Array(files)) = self.table.get_mut("catalogs") {
            for entry in files {
                // A path, or a table's `path`.
                let path = match entry {
                    toml::Value::Table(table) => table.get_mut("path"),
                    other => Some(other),
                };
                if let Some(toml::Value::String(path)) = path
                    && !path.trim().is_empty()
                    && expand_path(path).is_relative()
                {
                    *path = dir.join(expand_path(path)).to_string_lossy().into_owned();
                }
            }
        }
    }

    /// The value this layer writes at the dotted `key`, if it writes one.
    pub fn get(&self, key: &str) -> Option<&toml::Value> {
        let mut parts = key.split('.');
        let mut value = self.table.get(parts.next()?)?;
        for part in parts {
            value = value.as_table()?.get(part)?;
        }
        Some(value)
    }

    /// Lay `upper` over this layer: its keys win (except `COMBINED_KEYS`); keys it
    /// omits keep this layer's values. `upper`'s imports are not carried over.
    pub fn merge(&mut self, upper: ConfigLayer) {
        merge_tables(&mut self.table, upper.table, "");
    }
}

/// Keys 0.4.0 retired, and where what they said goes now.
const RETIRED_KEYS: &[(&str, &str)] = &[
    (
        "sources",
        "a collection is a catalog file now; put its datasets in catalog.toml as [id] \
         tables, or list the file in catalogs = [...] (datui catalog check FILE)",
    ),
    (
        "home.directories",
        "a directory is a catalog entry now; Ctrl+D on its row adds it to catalog.toml",
    ),
    (
        "home.builtin_catalog",
        "home.hide = [\"examples\"] hides the example datasets",
    ),
];

/// The keys `table` writes that the option registry does not know, each with its
/// nearest known keys, sorted. Registered keys' values (tables like
/// `[[cloud.connections]]`) are not inspected.
fn unknown_keys_in(table: &toml::Table) -> Vec<String> {
    fn walk(table: &toml::Table, prefix: &str, out: &mut Vec<String>) {
        for (key, value) in table {
            let path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            if datui_cli::settings::find(&path).is_some() {
                continue;
            }
            if let Some((_, moved)) = RETIRED_KEYS.iter().find(|(key, _)| *key == path) {
                out.push(format!("{path} is not read any more: {moved}"));
                continue;
            }
            match value {
                // A section, known or not: its keys are named individually, so a renamed section's
                // keys each find their place.
                toml::Value::Table(inner) => walk(inner, &path, out),
                _ => {
                    let near = datui_cli::settings::suggestions(&path);
                    let mut said = format!("{path} is not a config key, and is not read");
                    if !near.is_empty() {
                        said.push_str(&format!("; did you mean {}?", near.join(" or ")));
                    }
                    out.push(said);
                }
            }
        }
    }
    let mut out = Vec::new();
    walk(table, "", &mut out);
    out.sort();
    out
}

/// A TOML error with reason and place on the first line, then the excerpt (some
/// callers, like the Python binding, show only the first line).
fn parse_reason(error: &color_eyre::eyre::Report) -> String {
    let Some(toml_error) = error.downcast_ref::<toml::de::Error>() else {
        return error.to_string();
    };
    let message = toml_error.message().trim_end();
    let full = toml_error.to_string();
    let body = full.trim_end().strip_suffix(message).unwrap_or(&full);
    match body.split_once('\n') {
        Some((head, excerpt)) if head.starts_with("TOML parse error at ") => format!(
            "{message} ({})\n{}",
            head.trim_start_matches("TOML parse error at "),
            excerpt.trim_end()
        ),
        _ => message.to_string(),
    }
}

fn merge_tables(lower: &mut toml::Table, upper: toml::Table, prefix: &str) {
    for (key, value) in upper {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        let combine = COMBINED_KEYS
            .iter()
            .find(|(p, _)| *p == path)
            .map(|(_, c)| *c);
        match (combine, value) {
            (Some(combine), toml::Value::Array(upper)) => {
                let mut combined = match lower.remove(&key) {
                    Some(toml::Value::Array(lower)) => lower,
                    _ => Vec::new(),
                };
                match combine {
                    Combine::ByName => merge_by_name(&mut combined, upper),
                    Combine::Union => {
                        for item in upper {
                            if !combined.contains(&item) {
                                combined.push(item);
                            }
                        }
                    }
                }
                lower.insert(key, toml::Value::Array(combined));
            }
            (None, toml::Value::Table(upper)) => match lower.get_mut(&key) {
                Some(toml::Value::Table(lower)) => merge_tables(lower, upper, &path),
                _ => {
                    lower.insert(key, toml::Value::Table(upper));
                }
            },
            (_, value) => {
                lower.insert(key, value);
            }
        }
    }
}

fn merge_by_name(lower: &mut Vec<toml::Value>, upper: Vec<toml::Value>) {
    let name_of = |entry: &toml::Value| {
        entry
            .get("name")
            .and_then(toml::Value::as_str)
            .map(str::to_owned)
    };
    let mut seen = std::collections::HashSet::new();
    for entry in upper {
        let name = name_of(&entry);
        let first = name.clone().is_some_and(|n| seen.insert(n));
        match lower
            .iter()
            .position(|e| name.is_some() && name_of(e) == name)
        {
            Some(i) if first => lower[i] = entry,
            _ => lower.push(entry),
        }
    }
}

// Configuration loading and layering
impl AppConfig {
    /// Load configuration from all layers (default, imports, user config), with
    /// `-c KEY=VALUE` over the files.
    pub fn load_with(app_name: &str, overrides: &[datui_cli::settings::Override]) -> Result<Self> {
        match ConfigManager::new(app_name) {
            Ok(manager) => {
                Self::load_from_file_with(&manager.config_path("config.toml"), overrides)
            }
            // No config directory on this platform: defaults are all there is.
            Err(_) => {
                let layers = vec![ConfigLayer::from_overrides(overrides)?];
                let mut config =
                    Self::from_layers(layers).map_err(|e| eyre!("Invalid configuration: {}", e))?;
                config.read_theme_files(None)?;
                config
                    .validate()
                    .map_err(|e| eyre!("Invalid configuration: {}", e))?;
                Ok(config)
            }
        }
    }

    /// Load configuration rooted at `config_path` with its `import` chain. Layers, lowest
    /// first: defaults, each import in order (depth-first), then the file; each changes
    /// only the keys it writes. A missing root means defaults; an unreadable root or
    /// existing import is an error; a missing import is skipped with a warning (a theme
    /// system may not have generated it yet).
    pub fn load_from_file(config_path: &Path) -> Result<Self> {
        Self::load_from_file_with(config_path, &[])
    }

    /// [`Self::load_from_file`], with `-c KEY=VALUE` laid over every file.
    pub fn load_from_file_with(
        config_path: &Path,
        overrides: &[datui_cli::settings::Override],
    ) -> Result<Self> {
        let layers = Self::read_layers(config_path, overrides)?;
        Self::from_read_layers(config_path, overrides, &layers)
    }

    /// Every layer of the configuration at `config_path`, lowest first: imports
    /// depth-first, the file, then `-c`, each named by its origin. A missing root adds
    /// nothing.
    pub fn read_layers(
        config_path: &Path,
        overrides: &[datui_cli::settings::Override],
    ) -> Result<Vec<(LayerSource, ConfigLayer)>> {
        let mut layers = Vec::new();
        if let Some(root) = ConfigLayer::read(config_path, None)? {
            let canonical = crate::canonical::canonicalize(config_path)
                .unwrap_or_else(|_| config_path.to_path_buf());
            let mut stack = vec![canonical];
            Self::collect_imports(&root.imports, config_path, &mut stack, &mut layers)?;
            layers.push((LayerSource::File(config_path.to_path_buf()), root));
        }
        if !overrides.is_empty() {
            layers.push((
                LayerSource::Override,
                ConfigLayer::from_overrides(overrides)?,
            ));
        }
        Ok(layers)
    }

    /// The configuration `layers` (from [`Self::read_layers`]) describe, merged over
    /// defaults and validated.
    pub fn from_read_layers(
        config_path: &Path,
        overrides: &[datui_cli::settings::Override],
        layers: &[(LayerSource, ConfigLayer)],
    ) -> Result<Self> {
        // A bad value may be the file's or a `-c`'s.
        let place = if overrides.is_empty() {
            config_path.display().to_string()
        } else {
            format!("{} with -c", config_path.display())
        };
        let imports = layers
            .iter()
            .find(|(source, _)| *source == LayerSource::File(config_path.to_path_buf()))
            .map(|(_, root)| root.imports.clone())
            .unwrap_or_default();

        let mut config = Self::from_layers(layers.iter().map(|(_, layer)| layer.clone()))
            .map_err(|e| eyre!("Invalid configuration in {place}: {e}"))?;
        // `import` is a load-time directive, never merged; report what the root declared.
        config.import = imports;
        config.read_theme_files(config_path.parent())?;
        // A catalog's mistake names its own file and line.
        config.read_catalog_files(config_path.parent())?;
        for broken in &config.broken_catalogs {
            eprintln!("datui: warning: catalog left out: {}", broken.full());
            log::warn!(target: "datui", "catalog left out: {}", broken.full());
        }
        // A name that hides nothing is likely a typo, but not worth refusing to start.
        for name in config.unknown_hidden() {
            eprintln!("datui: warning: home.hide: {}", Self::hides_nothing(&name));
        }

        config.validate().map_err(|e| {
            eyre!(
                "Invalid configuration in {place}: {e}\n{}",
                way_out(false, "setting")
            )
        })?;

        Ok(config)
    }

    /// Append every file `imports` names to `out`, depth-first, relative to `origin`'s
    /// directory. `stack` holds the canonical paths being loaded, to report cycles.
    fn collect_imports(
        imports: &[String],
        origin: &Path,
        stack: &mut Vec<PathBuf>,
        out: &mut Vec<(LayerSource, ConfigLayer)>,
    ) -> Result<()> {
        if imports.is_empty() {
            return Ok(());
        }

        if stack.len() >= MAX_IMPORT_DEPTH {
            return Err(eyre!(
                "config import chain is more than {} files deep (at {}); \
                 flatten the chain or remove the extra levels",
                MAX_IMPORT_DEPTH,
                origin.display()
            ));
        }

        let origin_dir = origin.parent().unwrap_or_else(|| Path::new("."));

        for entry in imports {
            let expanded = expand_path(entry);
            let path = if expanded.is_absolute() {
                expanded
            } else {
                origin_dir.join(expanded)
            };

            let canonical = crate::canonical::canonicalize(&path).unwrap_or_else(|_| path.clone());
            if stack.contains(&canonical) {
                return Err(eyre!(
                    "circular config import: {} is already being loaded (imported by {})",
                    canonical.display(),
                    origin.display()
                ));
            }

            let Some(layer) = ConfigLayer::read(&path, Some(origin))? else {
                eprintln!(
                    "datui: warning: config import not found, skipping: {} (imported by {})",
                    path.display(),
                    origin.display()
                );
                continue;
            };

            stack.push(canonical);
            Self::collect_imports(&layer.imports, &path, stack, out)?;
            stack.pop();

            out.push((LayerSource::File(path), layer));
        }

        Ok(())
    }

    /// The configuration `layers` describe, lowest first, over the defaults (resolved
    /// once here). Colors start from the built-in theme `theme.dark`/`theme.light` names
    /// for the declared mode; `from_read_layers` adds theme files. `import` is left
    /// empty. Not validated.
    pub fn from_layers(layers: impl IntoIterator<Item = ConfigLayer>) -> Result<Self> {
        let mut merged = ConfigLayer::default();
        for layer in layers {
            merged.merge(layer);
        }
        let mut table = merged.table;

        let theme = table.get_mut("theme").and_then(toml::Value::as_table_mut);
        let mode: ThemeMode = match theme.as_ref().and_then(|t| t.get("mode")) {
            Some(mode) => mode.clone().try_into()?,
            None => ThemeMode::default(),
        };
        let resolved = mode.resolve();
        let colors = theme.and_then(|t| t.remove("colors"));

        let mut config: AppConfig = toml::Value::Table(table).try_into()?;

        if let Some(toml::Value::Table(colors)) = colors {
            config.theme.overrides = colors;
        }
        config.theme.follow = mode == ThemeMode::Auto;
        config.theme.mode = Some(resolved);
        // The built-ins only; `from_read_layers` reads the theme files and resolves again.
        config
            .theme
            .use_library(crate::config::themes::Library::default(), resolved);
        config.theme.colors = config.theme.palette_for(resolved)?;
        config.sync_dataset_access();
        Ok(config)
    }

    /// Resolve `theme.dark` and `theme.light` with `config_dir`'s `themes/` files as well
    /// as built-ins. A broken file or unusable name is reported and falls back to the
    /// built-in; it never stops startup.
    pub fn read_theme_files(&mut self, config_dir: Option<&Path>) -> Result<()> {
        let library = crate::config::themes::Library::read(config_dir);
        let active = self.theme.mode.unwrap_or_default().resolve();
        self.theme.use_library(library, active);
        for warning in self.theme.warnings() {
            eprintln!("datui: {warning}");
            log::warn!(target: "datui", "{warning}");
        }
        self.theme.colors = self.theme.palette_for(active)?;
        Ok(())
    }

    /// Every catalog, hidden ones included: `catalog.toml`, the listed files, then the
    /// bundled `examples` (unless a listed `examples.toml` replaces it).
    pub fn catalogs(&self) -> Vec<crate::home::catalog::Catalog> {
        let mut all = self.read_catalogs.clone();
        if !all.iter().any(|c| c.id == crate::home::catalog::EXAMPLES) {
            all.push(crate::home::catalog::bundled());
        }
        all
    }

    /// The catalogs home shows: [`Self::catalogs`] less `[home] hide` (a catalog id, or
    /// `catalog/id` for one entry).
    pub fn shown_catalogs(&self) -> Vec<crate::home::catalog::Catalog> {
        self.catalogs()
            .into_iter()
            .filter(|c| !self.home.hide.contains(&c.id))
            .map(|mut c| {
                c.datasets.retain(|d| {
                    !self
                        .home
                        .hide
                        .iter()
                        .any(|h| h.split_once('/') == Some((c.id.as_str(), d.id.as_str())))
                });
                c
            })
            .collect()
    }

    /// Why `name` in `home.hide` hides nothing, with the fix when it is the bundled
    /// catalog's old id (`public`, now `examples`).
    pub fn hides_nothing(name: &str) -> String {
        let old = crate::home::catalog::OLD_EXAMPLES_ID;
        let renamed = match name.split_once('/') {
            None if name == old => Some(crate::home::catalog::EXAMPLES.to_string()),
            Some((catalog, id)) if catalog == old => {
                Some(format!("{}/{id}", crate::home::catalog::EXAMPLES))
            }
            _ => None,
        };
        match renamed {
            Some(new) => format!("`{name}` is now `{new}`: hide = [\"{new}\"]"),
            None => format!("no catalog or entry is named {name}"),
        }
    }

    /// The `[home] hide` names that match no catalog or entry, each once.
    pub fn unknown_hidden(&self) -> Vec<String> {
        let catalogs = self.catalogs();
        let mut out: Vec<String> = Vec::new();
        for name in &self.home.hide {
            let known = match name.split_once('/') {
                None => {
                    catalogs.iter().any(|c| c.id == *name)
                        || self.broken_catalogs.iter().any(|b| b.id == *name)
                }
                Some((catalog, _)) if self.broken_catalogs.iter().any(|b| b.id == catalog) => true,
                Some((catalog, id)) => catalogs
                    .iter()
                    .any(|c| c.id == catalog && c.datasets.iter().any(|d| d.id == id)),
            };
            if !known && !out.contains(name) {
                out.push(name.clone());
            }
        }
        out
    }

    /// Read `catalog.toml` from `config_dir`, its `catalogs/*.toml` by name, and every
    /// file `catalogs` lists. A missing listed file is skipped with a warning (it may be
    /// on an unmounted share).
    pub fn read_catalog_files(&mut self, config_dir: Option<&Path>) -> Result<()> {
        use crate::home::catalog::{self, Origin};
        let mut read: Vec<catalog::Catalog> = Vec::new();
        let mut broken: Vec<catalog::Broken> = Vec::new();
        let connections = self.cloud.connections.clone();
        // A broken file is left out and reported; it must not stop startup.
        let take = |found: std::result::Result<Option<catalog::Catalog>, catalog::Broken>,
                    read: &mut Vec<catalog::Catalog>,
                    broken: &mut Vec<catalog::Broken>|
         -> bool {
            match found {
                Ok(Some(c)) => match c.check_connections(&connections) {
                    Ok(()) => {
                        read.push(c);
                        true
                    }
                    Err(e) => {
                        broken.push(catalog::Broken {
                            id: c.id.clone(),
                            origin: c.origin,
                            file: c.file.clone().unwrap_or_default(),
                            line: e.line,
                            message: e.message,
                        });
                        true
                    }
                },
                Ok(None) => false,
                Err(b) => {
                    broken.push(b);
                    true
                }
            }
        };
        // Each file, where it was found, and the id and label a `catalogs` table gives it.
        let mut files: Vec<(PathBuf, Origin, Option<String>, Option<String>)> = Vec::new();
        if let Some(dir) = config_dir {
            take(
                catalog::load(&dir.join(catalog::MINE_FILE), catalog::MINE, Origin::Mine),
                &mut read,
                &mut broken,
            );
            let folder = dir.join(catalog::FOLDER);
            let mut found: Vec<PathBuf> = match std::fs::read_dir(&folder) {
                Ok(entries) => entries
                    .filter_map(|e| e.ok().map(|e| e.path()))
                    .filter(|p| {
                        p.extension().is_some_and(|x| x == "toml")
                            && std::fs::metadata(p).is_ok_and(|m| m.is_file())
                    })
                    .collect(),
                Err(_) => Vec::new(),
            };
            found.sort();
            files.extend(found.into_iter().map(|p| (p, Origin::Folder, None, None)));
        }
        files.extend(self.catalogs.iter().map(|entry| {
            (
                expand_path(entry.path()),
                Origin::Listed,
                entry.id().map(str::to_string),
                entry.label().map(str::to_string),
            )
        }));
        for (path, origin, given_id, label) in files {
            let id = given_id
                .clone()
                .unwrap_or_else(|| catalog::id_of_file(&path));
            let refuse = |message: String| catalog::Broken {
                id: id.clone(),
                origin,
                file: path.clone(),
                line: None,
                message,
            };
            if !is_valid_source_id(&id) || id == catalog::MINE {
                broken.push(refuse(format!(
                    "\"{id}\" cannot be a catalog's id: lowercase letters, digits and '-', \
                     and not \"{}\", which is catalog.toml's. {}",
                    catalog::MINE,
                    if given_id.is_some() {
                        "Give another id = \"...\""
                    } else {
                        "Rename the file, or list it as { path = \"...\", id = \"...\" }"
                    }
                )));
                continue;
            }
            if let Some(first) = read.iter().find(|c| c.id == id) {
                let first = first.file_name();
                broken.push(refuse(format!(
                    "{first} and this file are both the catalog \"{id}\". Rename one, or \
                     list one as {{ path = \"...\", id = \"...\" }}"
                )));
                continue;
            }
            let found = catalog::load(&path, &id, origin).map(|found| {
                found.map(|mut listed| {
                    if let Some(label) = &label {
                        listed.label = label.clone();
                    }
                    listed
                })
            });
            if !take(found, &mut read, &mut broken) {
                eprintln!(
                    "datui: warning: catalog not found, skipping: {}",
                    path.display()
                );
            }
        }
        self.read_catalogs = read;
        self.broken_catalogs = broken;
        self.catalog_dir = config_dir.map(Path::to_path_buf);
        self.sync_dataset_access();
        Ok(())
    }

    /// Derive `[cloud]`'s view of how catalog URLs are read. Called by `from_layers`,
    /// `read_catalog_files` and `default`; call it after changing the catalogs by hand.
    pub fn sync_dataset_access(&mut self) {
        self.cloud.dataset_access = self
            .catalogs()
            .iter()
            .flat_map(|catalog| {
                catalog.datasets.iter().filter_map(|dataset| {
                    Some(DatasetAccess {
                        url: dataset.url.clone()?,
                        catalog: catalog.id.clone(),
                        auth: dataset.object_store_auth()?,
                    })
                })
            })
            .collect();
    }

    /// Validate configuration values
    pub fn validate(&self) -> Result<()> {
        let rows = self.analysis.chart_rows;
        if rows == 0 || rows > MAX_CHART_ROW_LIMIT {
            return Err(eyre!(
                "analysis.chart_rows must be between 1 and {MAX_CHART_ROW_LIMIT}, got {rows}"
            ));
        }

        // Resolve number formatting now so bad presets and separator clashes fail at load.
        self.display
            .number_format
            .resolve(self.display.right_align_numbers)?;

        let interval = self.read.follow_interval;
        if !FOLLOW_INTERVAL.contains(&interval.duration()) {
            return Err(eyre!(
                "read.follow_interval must be between 10ms and 1m, got {interval}"
            ));
        }

        if let Some(c) = &self.csv.comment {
            crate::formats::csv_dialect::check_comment_char(c)
                .map_err(|e| eyre!("csv.comment: {e}"))?;
        }

        if let Some(level) = &self.log.level
            && !datui_cli::LOG_LEVELS.contains(&level.as_str())
        {
            return Err(eyre!(
                "log.level must be one of {}, got {level:?}",
                datui_cli::LOG_LEVELS.join(", ")
            ));
        }

        self.cloud.validate()?;
        for catalog in &self.read_catalogs {
            catalog
                .check_connections(&self.cloud.connections)
                .map_err(|e| eyre!("{}", e.in_file(&catalog.file_name())))?;
        }
        let hide_name = |name: &str| match name.split_once('/') {
            Some((catalog, id)) => is_valid_source_id(catalog) && is_valid_source_id(id),
            None => is_valid_source_id(name),
        };
        if let Some(name) = self.home.hide.iter().find(|name| !hide_name(name)) {
            return Err(eyre!(
                "home.hide: \"{name}\" is not a catalog id or catalog/id. Use the ids (mine, \
                 examples, a listed file's name; examples/nyc-taxis for one entry), not the labels"
            ));
        }

        let parser = ColorParser::new();
        self.theme.colors.validate(&parser)?;

        crate::glyphs::validate_overrides(&self.glyphs.overrides)
            .map_err(|e| eyre!("[glyphs]: {e}"))?;

        if crate::clipboard::BackendChoice::parse(&self.clipboard.backend).is_none() {
            return Err(eyre!(
                "[clipboard] backend must be auto, native or osc52, got {:?}",
                self.clipboard.backend
            ));
        }
        if self.clipboard.osc52_limit.bytes() == 0 {
            return Err(eyre!("[clipboard] osc52_limit must be greater than 0"));
        }
        if !crate::cloud::user_agent::is_valid(&self.http.user_agent) {
            return Err(eyre!(
                "[http] user_agent must be printable ASCII, got {:?}",
                self.http.user_agent
            ));
        }

        Ok(())
    }
}

impl ColorConfig {
    /// Every slot by name, as the config writes it.
    pub(crate) fn slots(&self) -> Vec<(String, String)> {
        match toml::Value::try_from(self) {
            Ok(toml::Value::Table(table)) => table
                .into_iter()
                .map(|(name, value)| (name, value.as_str().unwrap_or_default().to_string()))
                .collect(),
            _ => Vec::new(),
        }
    }

    /// Validate all color strings can be parsed
    fn validate(&self, parser: &ColorParser) -> Result<()> {
        for (name, value) in self.slots() {
            // "default" is no stripe, not a color.
            if name == "table_alternate_row" && value == "default" {
                continue;
            }
            parser.parse(&value).map_err(|e| {
                eyre!(
                    "theme.colors.{name}: {e}. Use a valid color name (e.g. red, cyan, \
                     bright_red), hex (#rrggbb), or indexed(0-255)"
                )
            })?;
        }
        Ok(())
    }
}

/// Color parser with terminal capability detection
pub struct ColorParser {
    supports_true_color: bool,
    supports_256: bool,
    no_color: bool,
}

impl ColorParser {
    /// Create a new ColorParser with automatic terminal capability detection
    pub fn new() -> Self {
        let no_color = std::env::var("NO_COLOR").is_ok();
        let support = supports_color::on(Stream::Stdout);
        #[cfg(windows)]
        let console = windows_console_true_color(
            // `FORCE_COLOR` names a level for `supports_color` to answer with.
            std::env::var_os("TERM").is_some() || std::env::var_os("FORCE_COLOR").is_some(),
            std::io::IsTerminal::is_terminal(&std::io::stdout()),
            crossterm::ansi_support::supports_ansi,
        );
        #[cfg(not(windows))]
        let console = false;

        Self {
            supports_true_color: console || support.as_ref().is_some_and(|s| s.has_16m),
            supports_256: console || support.as_ref().is_some_and(|s| s.has_256),
            no_color,
        }
    }

    /// Parse a color string (hex or named) and convert to appropriate terminal color
    pub fn parse(&self, s: &str) -> Result<Color> {
        if self.no_color {
            return Ok(Color::Reset);
        }

        let trimmed = s.trim();

        // Hex format: "#ff0000" or "#FF0000" (6-character hex)
        if trimmed.starts_with('#') && trimmed.len() == 7 {
            let (r, g, b) = parse_hex(trimmed)?;
            return Ok(self.convert_rgb_to_terminal_color(r, g, b));
        }

        // Indexed colors: "indexed(236)" for explicit 256-color palette
        if trimmed.to_lowercase().starts_with("indexed(") && trimmed.ends_with(')') {
            let num_str = &trimmed[8..trimmed.len() - 1]; // Extract number between parentheses
            let num = num_str.parse::<u8>().map_err(|_| {
                eyre!(
                    "Invalid indexed color: '{}'. Expected format: indexed(0-255)",
                    trimmed
                )
            })?;
            return Ok(Color::Indexed(num));
        }

        // Named colors (case-insensitive)
        let lower = trimmed.to_lowercase();
        match lower.as_str() {
            // Basic ANSI colors
            "black" => Ok(Color::Black),
            "red" => Ok(Color::Red),
            "green" => Ok(Color::Green),
            "yellow" => Ok(Color::Yellow),
            "blue" => Ok(Color::Blue),
            "magenta" => Ok(Color::Magenta),
            "cyan" => Ok(Color::Cyan),
            "white" => Ok(Color::White),

            // Bright variants (256-color palette)
            "bright_black" | "bright black" => Ok(Color::Indexed(8)),
            "bright_red" | "bright red" => Ok(Color::Indexed(9)),
            "bright_green" | "bright green" => Ok(Color::Indexed(10)),
            "bright_yellow" | "bright yellow" => Ok(Color::Indexed(11)),
            "bright_blue" | "bright blue" => Ok(Color::Indexed(12)),
            "bright_magenta" | "bright magenta" => Ok(Color::Indexed(13)),
            "bright_cyan" | "bright cyan" => Ok(Color::Indexed(14)),
            "bright_white" | "bright white" => Ok(Color::Indexed(15)),

            // Gray aliases
            "gray" | "grey" => Ok(Color::Indexed(8)),
            "dark_gray" | "dark gray" | "dark_grey" | "dark grey" => Ok(Color::Indexed(8)),
            "light_gray" | "light gray" | "light_grey" | "light grey" => Ok(Color::Indexed(7)),

            // Special modifiers (pass through as Reset - handled specially in rendering)
            "reset" | "default" | "none" | "reversed" => Ok(Color::Reset),

            _ => Err(eyre!(
                "Unknown color name: '{}'. Supported: basic ANSI colors (red, blue, etc.), \
                 bright variants (bright_red, etc.), or hex colors (#ff0000)",
                trimmed
            )),
        }
    }

    /// Convert RGB values to appropriate terminal color based on capabilities
    fn convert_rgb_to_terminal_color(&self, r: u8, g: u8, b: u8) -> Color {
        if self.supports_true_color {
            Color::Rgb(r, g, b)
        } else if self.supports_256 {
            Color::Indexed(rgb_to_256_color(r, g, b))
        } else {
            rgb_to_basic_ansi(r, g, b)
        }
    }
}

/// Whether a Windows console draws 24-bit color where `supports_color` cannot tell
/// (it reads `TERM`/`COLORTERM`, unset by Windows Terminal and conhost). Both do once
/// virtual terminal processing is on (`vt`); legacy consoles refuse it. With `TERM`
/// set (mintty, MSYS2) or `FORCE_COLOR`, its answer stands.
#[cfg(windows)]
fn windows_console_true_color(env_says: bool, terminal: bool, vt: impl FnOnce() -> bool) -> bool {
    !env_says && terminal && vt()
}

impl Default for ColorParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse hex color string (#ff0000) to RGB components
fn parse_hex(s: &str) -> Result<(u8, u8, u8)> {
    // `len()` counts bytes: slicing at fixed offsets is safe only once the rest is ASCII
    // ("#\u{1f600}xy" is also seven bytes).
    let hex = s
        .strip_prefix('#')
        .filter(|hex| hex.len() == 6 && hex.is_ascii())
        .ok_or_else(|| {
            eyre!(
                "Invalid hex color format: '{}'. Expected format: #rrggbb",
                s
            )
        })?;

    let r = u8::from_str_radix(&hex[0..2], 16)
        .map_err(|_| eyre!("Invalid red component in hex color: {}", s))?;
    let g = u8::from_str_radix(&hex[2..4], 16)
        .map_err(|_| eyre!("Invalid green component in hex color: {}", s))?;
    let b = u8::from_str_radix(&hex[4..6], 16)
        .map_err(|_| eyre!("Invalid blue component in hex color: {}", s))?;

    Ok((r, g, b))
}

/// The nearest xterm 256-color palette index to an RGB color.
pub fn rgb_to_256_color(r: u8, g: u8, b: u8) -> u8 {
    // Nearest palette entry by RGB distance: the cube's levels are far apart, so dark
    // tints are often nearer a ramp grey than any cube color.
    let dist = |cr: i32, cg: i32, cb: i32| -> i32 {
        let (dr, dg, db) = (cr - r as i32, cg - g as i32, cb - b as i32);
        dr * dr + dg * dg + db * db
    };
    const LEVELS: [i32; 6] = [0, 95, 135, 175, 215, 255];
    let mut best = (i32::MAX, 16u8);
    for (ri, &cr) in LEVELS.iter().enumerate() {
        for (gi, &cg) in LEVELS.iter().enumerate() {
            for (bi, &cb) in LEVELS.iter().enumerate() {
                let d = dist(cr, cg, cb);
                if d < best.0 {
                    best = (d, 16 + 36 * ri as u8 + 6 * gi as u8 + bi as u8);
                }
            }
        }
    }
    for i in 0..24u8 {
        let v = 8 + 10 * i as i32;
        let d = dist(v, v, v);
        if d < best.0 {
            best = (d, 232 + i);
        }
    }
    best.1
}

/// Convert RGB to nearest basic ANSI color (8 colors)
pub fn rgb_to_basic_ansi(r: u8, g: u8, b: u8) -> Color {
    // Simple threshold-based conversion
    let r_bright = r > 128;
    let g_bright = g > 128;
    let b_bright = b > 128;

    // Check for grayscale
    let max_diff = r.max(g).max(b) as i16 - r.min(g).min(b) as i16;
    if max_diff < 30 {
        let avg = (r as u16 + g as u16 + b as u16) / 3;
        return if avg < 64 { Color::Black } else { Color::White };
    }

    match (r_bright, g_bright, b_bright) {
        (false, false, false) => Color::Black,
        (true, false, false) => Color::Red,
        (false, true, false) => Color::Green,
        (true, true, false) => Color::Yellow,
        (false, false, true) => Color::Blue,
        (true, false, true) => Color::Magenta,
        (false, true, true) => Color::Cyan,
        (true, true, true) => Color::White,
    }
}

/// Theme containing parsed colors ready for use
#[derive(Debug, Clone)]
pub struct Theme {
    pub colors: HashMap<String, Color>,
}

/// The theme's chart series slots, `chart_1` to `chart_10`.
pub const CHART_SERIES_SLOTS: usize = 10;

/// Typed accessors for the theme's color slots, so a misspelled slot does not compile
/// rather than drawing in `Color::Reset`.
macro_rules! color_slots {
    ($($slot:ident),* $(,)?) => {
        impl Theme {
            $(
                pub fn $slot(&self) -> Color {
                    self.get(stringify!($slot))
                }
            )*
        }

        /// Every slot with an accessor, for the test that the theme defines each.
        #[cfg(test)]
        pub(crate) const COLOR_SLOTS: &[&str] = &[$(stringify!($slot)),*];
    };
}

color_slots!(
    accent,
    accent_bright,
    background,
    chart_1,
    chart_grid,
    chip_key,
    chip_label,
    controls_bg,
    dimmed,
    distribution_normal,
    distribution_skewed,
    error,
    find_match,
    gradient_end,
    gradient_start,
    hex_control,
    hex_ff,
    hex_high,
    hex_null,
    hex_printable,
    hex_whitespace,
    input_cursor,
    label,
    modal_border,
    modal_border_active,
    modal_border_error,
    outlier_marker,
    sidebar_border,
    success,
    surface,
    table_column_separator,
    table_header,
    table_header_bg,
    table_row_numbers,
    text_inverse,
    text_primary,
    text_secondary,
    throbber,
    type_binary,
    type_bool,
    type_float,
    type_int,
    type_str,
    type_temporal,
    warning,
);

impl Theme {
    /// Create a Theme from a ThemeConfig by parsing all color strings
    pub fn from_config(config: &ThemeConfig) -> Result<Self> {
        let parser = ColorParser::new();
        let mut colors = HashMap::new();
        for (name, value) in config.colors.slots() {
            // Left out of the map so widgets can tell them apart via `get_optional`: "reversed"
            // swaps the current row's colors, "default" is no stripe.
            let absent = match name.as_str() {
                "table_selected" => value.trim().eq_ignore_ascii_case("reversed"),
                "table_alternate_row" => value == "default",
                _ => false,
            };
            if !absent {
                colors.insert(name, parser.parse(&value)?);
            }
        }
        // Sidebars and the input strip draw their resting border from `modal_border` (the
        // config's `sidebar_border`); labels are secondary text.
        colors.insert("modal_border".to_string(), colors["sidebar_border"]);
        colors.insert("label".to_string(), colors["text_secondary"]);
        Ok(Self { colors })
    }

    /// The color in slot `name`, `Reset` where there is none. Read through the typed
    /// accessors ([`color_slots`]).
    fn get(&self, name: &str) -> Color {
        self.colors.get(name).copied().unwrap_or(Color::Reset)
    }

    /// Get a color by name, returns None if not found
    pub fn get_optional(&self, name: &str) -> Option<Color> {
        self.colors.get(name).copied()
    }

    /// The colors series are drawn in: `chart_1`..`chart_10` as this terminal shows them,
    /// deduplicated so two series never share a color; a chart draws at most this many.
    pub fn series_colors(&self) -> Vec<Color> {
        let mut colors: Vec<Color> = Vec::with_capacity(CHART_SERIES_SLOTS);
        for i in 1..=CHART_SERIES_SLOTS {
            let color = self.get(&format!("chart_{i}"));
            if !colors.contains(&color) {
                colors.push(color);
            }
        }
        colors
    }

    /// Style of the row or item the cursor is on: the theme's tint, or reversed video
    /// when `table_selected = "reversed"`.
    pub fn highlight_style(&self) -> ratatui::style::Style {
        match self.get_optional("table_selected") {
            Some(bg) => ratatui::style::Style::default().bg(bg),
            None => {
                ratatui::style::Style::default().add_modifier(ratatui::style::Modifier::REVERSED)
            }
        }
    }

    /// Style of the column cursor's cells; see [`column_cursor_style`].
    pub fn column_cursor_style(&self) -> ratatui::style::Style {
        column_cursor_style(self.get_optional("table_column_cursor"))
    }

    /// Style of the column cursor's header and the current cell; see
    /// [`cell_cursor_style`].
    pub fn cell_cursor_style(&self) -> ratatui::style::Style {
        cell_cursor_style(self.get_optional("table_cell_cursor"))
    }

    /// Selected text in a field: the highlight tint, or reversed video where the tint
    /// may match the background (16 colors, `NO_COLOR`); a field has no rail instead.
    pub fn text_selection_style(&self) -> ratatui::style::Style {
        match self.get_optional("table_selected") {
            Some(Color::Reset | Color::Black | Color::White) => {
                ratatui::style::Style::default().add_modifier(ratatui::style::Modifier::REVERSED)
            }
            _ => self.highlight_style(),
        }
    }

    /// The cell a find landed on: the `find_match` tint under black or white text,
    /// whichever reads on it, or reversed bold video where there is no color.
    pub fn find_match_style(&self) -> ratatui::style::Style {
        use ratatui::style::{Modifier, Style};
        match self.get("find_match") {
            Color::Reset => Style::default().add_modifier(Modifier::REVERSED | Modifier::BOLD),
            bg => Style::default().bg(bg).fg(contrasting_text(bg)),
        }
    }

    /// Text color for the solid cursor block: `cursor_text`, or black/white by the
    /// cursor's luminance when "default"; here so widgets never pick colors.
    pub fn cursor_text_for(&self, cursor: Color) -> Color {
        match self.get("input_cursor_text") {
            Color::Reset => contrasting_text(cursor),
            configured => configured,
        }
    }
}

/// Whether a tint can be told from the terminal's own background: a 16-color terminal
/// turns the default tints into black or white, and `NO_COLOR` into none.
pub fn tint_shows(tint: Option<Color>) -> Option<Color> {
    tint.filter(|c| !matches!(c, Color::Reset | Color::Black | Color::White))
}

/// Style of the column cursor's cells: the `column_cursor` tint, or nothing where the
/// tint would not show; the header and the current cell still mark the column there.
pub fn column_cursor_style(tint: Option<Color>) -> ratatui::style::Style {
    match tint_shows(tint) {
        Some(bg) => ratatui::style::Style::default().bg(bg),
        None => ratatui::style::Style::default(),
    }
}

/// Style of the column cursor's header and of the current cell: the `cell_cursor`
/// tint in bold, or reversed video where the tint would not show, so the cell is
/// marked on any terminal.
pub fn cell_cursor_style(tint: Option<Color>) -> ratatui::style::Style {
    use ratatui::style::{Modifier, Style};
    match tint_shows(tint) {
        Some(bg) => Style::default().bg(bg).add_modifier(Modifier::BOLD),
        None => Style::default().add_modifier(Modifier::REVERSED | Modifier::BOLD),
    }
}

/// Black or white, whichever reads on a solid block of `bg` (Rec. 601 luma).
fn contrasting_text(bg: Color) -> Color {
    let (r, g, b) = approx_rgb(bg);
    let luma = 299 * r as u32 + 587 * g as u32 + 114 * b as u32;
    if luma >= 128_000 {
        Color::Black
    } else {
        Color::White
    }
}

/// An approximate RGB for any terminal color (xterm defaults), good enough to pick
/// black or white text.
fn approx_rgb(color: Color) -> (u8, u8, u8) {
    match color {
        Color::Rgb(r, g, b) => (r, g, b),
        Color::Indexed(i) => xterm_rgb(i),
        Color::Black => (0, 0, 0),
        Color::Red => (205, 0, 0),
        Color::Green => (0, 205, 0),
        Color::Yellow => (205, 205, 0),
        Color::Blue => (0, 0, 238),
        Color::Magenta => (205, 0, 205),
        Color::Cyan => (0, 205, 205),
        Color::Gray => (229, 229, 229),
        Color::DarkGray => (127, 127, 127),
        Color::LightRed => (255, 0, 0),
        Color::LightGreen => (0, 255, 0),
        Color::LightYellow => (255, 255, 0),
        Color::LightBlue => (92, 92, 255),
        Color::LightMagenta => (255, 0, 255),
        Color::LightCyan => (0, 255, 255),
        Color::White => (255, 255, 255),
        Color::Reset => (0, 0, 0),
    }
}

/// The standard xterm 256-color palette entry, as RGB.
fn xterm_rgb(i: u8) -> (u8, u8, u8) {
    match i {
        0..=15 => approx_rgb(match i {
            0 => Color::Black,
            1 => Color::Red,
            2 => Color::Green,
            3 => Color::Yellow,
            4 => Color::Blue,
            5 => Color::Magenta,
            6 => Color::Cyan,
            7 => Color::Gray,
            8 => Color::DarkGray,
            9 => Color::LightRed,
            10 => Color::LightGreen,
            11 => Color::LightYellow,
            12 => Color::LightBlue,
            13 => Color::LightMagenta,
            14 => Color::LightCyan,
            _ => Color::White,
        }),
        16..=231 => {
            let level = |n: u8| if n == 0 { 0 } else { 55 + 40 * n };
            let c = i - 16;
            (level(c / 36), level(c / 6 % 6), level(c % 6))
        }
        232..=255 => {
            let v = 8 + 10 * (i - 232);
            (v, v, v)
        }
    }
}

/// `text` in lines of at most `width` characters, broken between words.
fn wrap(text: &str, width: usize) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    for word in text.split_whitespace() {
        match lines.last_mut() {
            Some(line) if line.len() + 1 + word.len() <= width => {
                line.push(' ');
                line.push_str(word);
            }
            _ => lines.push(word.to_string()),
        }
    }
    lines
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    /// A path from the command line has been through the shell: only a leading `~`
    /// is left for datui to expand.
    #[test]
    fn a_command_line_path_expands_only_a_leading_tilde() {
        let home = dirs::home_dir().expect("a home directory");
        let expand = |p: &str| super::expand_home(Path::new(p));
        assert_eq!(expand("~"), home);
        assert_eq!(expand("~/data/a.csv"), home.join("data/a.csv"));
        for kept in [
            "a/~/b.csv",
            "~user/a.csv",
            "$HOME/a.csv",
            "-",
            "s3://b/~/a.csv",
        ] {
            assert_eq!(expand(kept), PathBuf::from(kept), "{kept}");
        }
        #[cfg(windows)]
        assert_eq!(expand(r"~\data\a.csv"), home.join(r"data\a.csv"));
        // A backslash is part of a name off Windows.
        #[cfg(not(windows))]
        assert_eq!(expand(r"~\a.csv"), PathBuf::from(r"~\a.csv"));
        // A file named `~`, or under a directory named `~`, is that file.
        for there in ["~", "~/a.csv"] {
            let kept = super::expand_home_unless(Path::new(there), |_| true);
            assert_eq!(kept, PathBuf::from(there), "{there}");
        }
    }

    /// Under `NO_COLOR` every color, named or hex, is no color. Set on the parser
    /// rather than in the environment, which other tests read.
    #[test]
    fn no_color_parses_every_color_as_reset() {
        use ratatui::style::Color;
        let parser = super::ColorParser {
            supports_true_color: true,
            supports_256: true,
            no_color: true,
        };
        for name in ["red", "#ff0000", "cyan", "indexed(240)"] {
            assert_eq!(parser.parse(name).unwrap(), Color::Reset, "{name}");
        }
    }

    /// Windows Terminal and conhost set no `TERM`; with virtual terminal processing
    /// on, they take 24-bit color. A legacy console, or a terminal that sets `TERM`
    /// for `supports_color` to read, is left to it.
    #[cfg(windows)]
    #[test]
    fn a_windows_console_with_vt_takes_true_color() {
        use super::windows_console_true_color as rule;
        assert!(rule(false, true, || true));
        assert!(!rule(false, true, || false), "a legacy console");
        assert!(
            !rule(true, true, || true),
            "TERM or FORCE_COLOR set: supports_color decides"
        );
        assert!(!rule(false, false, || true), "not a terminal");
    }

    #[test]
    fn a_path_place_ignores_spelling_but_not_meaning() {
        let place = |p: &str| super::path_place(std::path::Path::new(p));
        for (a, b) in [
            ("/d/a.csv", "/d//a.csv"),
            ("/d/a.csv", "/d/./a.csv"),
            ("/d/sub", "/d/sub/"),
            ("a.csv", "./a.csv"),
        ] {
            assert_eq!(place(a), place(b), "{a} and {b}");
        }
        for (a, b) in [
            ("/d/../a.csv", "/a.csv"),
            ("/d/a.csv", "/d/A.csv"),
            ("/d/a.csv", "d/a.csv"),
            ("/d/a.csv", "/d/a.csv.gz"),
        ] {
            assert_ne!(place(a), place(b), "{a} and {b}");
        }
        #[cfg(windows)]
        {
            for (a, b) in [
                (r"C:\d\a.csv", r"c:\d\a.csv"),
                (r"C:\d\a.csv", "C:/d/a.csv"),
                (r"\\srv\share\a.csv", "//srv/share/a.csv"),
            ] {
                assert_eq!(place(a), place(b), "{a} and {b}");
            }
            for (a, b) in [
                (r"C:\d\a.csv", r"D:\d\a.csv"),
                (r"C:\a.csv", "C:a.csv"),
                (r"\\srv\share\a.csv", r"\\srv\other\a.csv"),
            ] {
                assert_ne!(place(a), place(b), "{a} and {b}");
            }
        }
    }

    use super::*;

    /// Each typed accessor names a slot the theme has: a slot it lacks would draw in
    /// `Color::Reset`.
    #[test]
    fn every_color_accessor_names_a_slot() {
        let theme = Theme::from_config(&AppConfig::default().theme).unwrap();
        for slot in COLOR_SLOTS {
            assert!(theme.colors.contains_key(*slot), "no {slot} slot");
        }
    }

    #[test]
    fn a_key_the_registry_does_not_know_is_named_with_the_nearest() {
        let found = |text: &str| unknown_keys_in(&toml::from_str(text).unwrap());
        let unknown = found(
            "[file_loading]\ncomment_char = \"#\"\n[display]\nmouse = false\nrow_numbr = true\n\
             number_format = { grouping = \"thousands\" }\n[glyphs]\nspinner = [\"a\"]\n\
             [theme.colors]\naccent = \"red\"\n[[cloud.connections]]\nname = \"x\"\n\
             [[sources]]\nname = \"x\"\n[home]\ndirectories = [\"/d\"]\n",
        );
        assert_eq!(unknown.len(), 4, "{unknown:?}");
        // A retired key says where what it said goes now.
        assert!(
            unknown[2].starts_with("home.directories is not read any more")
                && unknown[2].contains("Ctrl+D"),
            "{unknown:?}"
        );
        assert!(
            unknown[3].starts_with("sources is not read any more")
                && unknown[3].contains("catalog.toml"),
            "{unknown:?}"
        );
        assert!(
            unknown[0].starts_with("display.row_numbr")
                && unknown[0].contains("display.row_numbers")
        );
        assert!(
            unknown[1].starts_with("file_loading.comment_char")
                && unknown[1].contains("csv.comment")
        );
    }

    #[test]
    fn a_field_selection_stays_visible_when_the_tint_degrades() {
        use ratatui::style::{Modifier, Style};
        let theme_with = |tint: Option<Color>| Theme {
            colors: tint
                .map(|c| HashMap::from([("table_selected".to_string(), c)]))
                .unwrap_or_default(),
        };
        let reversed = Style::default().add_modifier(Modifier::REVERSED);
        // The default tints on a 16-color terminal, and NO_COLOR.
        for tint in [Color::Black, Color::White, Color::Reset] {
            assert_eq!(
                theme_with(Some(tint)).text_selection_style(),
                reversed,
                "{tint:?}"
            );
        }
        assert_eq!(theme_with(None).text_selection_style(), reversed);
        for tint in [
            Color::Rgb(0x28, 0x34, 0x57),
            Color::Indexed(237),
            Color::Blue,
        ] {
            assert_eq!(
                theme_with(Some(tint)).text_selection_style(),
                Style::default().bg(tint)
            );
        }
    }

    /// Every setting the defaults serialize, plus the ones unset by default, as dotted
    /// paths with their values. Arrays of tables (`[[sources]]`) are not settings.
    fn leaf_settings() -> Vec<(String, toml::Value)> {
        fn walk(table: &toml::Table, prefix: &str, out: &mut Vec<(String, toml::Value)>) {
            for (key, value) in table {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                match value {
                    toml::Value::Table(inner) => walk(inner, &path, out),
                    toml::Value::Array(items) if items.iter().any(toml::Value::is_table) => {}
                    _ => out.push((path, value.clone())),
                }
            }
        }
        let toml::Value::Table(defaults) = toml::Value::try_from(AppConfig::default()).unwrap()
        else {
            unreachable!("a struct serializes to a table")
        };
        let mut out = Vec::new();
        walk(&defaults, "", &mut out);
        for setting in datui_cli::settings::SETTINGS {
            if let datui_cli::settings::DefaultValue::Unset(example) = setting.default
                && !setting.key.ends_with(".*")
                && setting.kind != datui_cli::settings::Kind::Tables
            {
                let value: toml::Table = toml::from_str(&format!("v = {example}")).unwrap();
                out.push((setting.key.to_string(), value["v"].clone()));
            }
        }
        out
    }

    /// A layer that writes `value` at the dotted `path` and nothing else.
    fn layer_at(path: &str, value: toml::Value) -> Result<ConfigLayer> {
        let table = path.rsplit('.').fold(value, |inner, key| {
            toml::Value::Table(toml::Table::from_iter([(key.to_string(), inner)]))
        });
        ConfigLayer::parse(&toml::to_string(&table)?)
    }

    fn value_at<'a>(table: &'a toml::Table, path: &str) -> Option<&'a toml::Value> {
        let (parents, key) = path.rsplit_once('.').unwrap_or(("", path));
        let mut table = table;
        for part in parents.split('.').filter(|p| !p.is_empty()) {
            table = table.get(part)?.as_table()?;
        }
        table.get(key)
    }

    /// The registry and the config structs describe the same keys with the same
    /// defaults: every key the defaults serialize is registered with that value, and
    /// every registered key is one the structs read.
    #[test]
    fn the_registry_and_the_config_structs_agree() {
        use datui_cli::settings::{DefaultValue, Kind, SETTINGS, find};
        let toml::Value::Table(defaults) = toml::Value::try_from(AppConfig::default()).unwrap()
        else {
            unreachable!("a struct serializes to a table")
        };
        let light = toml::Value::try_from(ColorConfig::light()).unwrap();
        for (path, value) in leaf_settings() {
            let setting = find(&path).unwrap_or_else(|| panic!("{path} is not registered"));
            let registered = match setting.default {
                DefaultValue::Value(v) | DefaultValue::Unset(v) => {
                    toml::from_str::<toml::Table>(&format!("v = {v}")).unwrap()["v"].clone()
                }
                DefaultValue::Color { dark, light: lit } => {
                    let name = setting.name();
                    assert_eq!(
                        light.get(name).and_then(|v| v.as_str()),
                        Some(lit),
                        "{path} (light)"
                    );
                    toml::Value::String(dark.to_string())
                }
            };
            assert_eq!(value, registered, "{path}: the registry's default differs");
        }
        for setting in SETTINGS {
            if setting.key.ends_with(".*") || setting.kind == Kind::Tables {
                continue;
            }
            let serialized = value_at(&defaults, setting.key).is_some();
            let unset = matches!(setting.default, DefaultValue::Unset(_));
            assert_eq!(
                serialized,
                !unset,
                "{}: registered as {}set by default",
                setting.key,
                if unset { "un" } else { "" }
            );
            // Each value it shows is one the config reads.
            let example = match setting.default {
                DefaultValue::Value(v) | DefaultValue::Unset(v) => v.to_string(),
                DefaultValue::Color { dark, .. } => format!("\"{dark}\""),
            };
            let value =
                toml::from_str::<toml::Table>(&format!("v = {example}")).unwrap()["v"].clone();
            layer_at(setting.key, value).unwrap_or_else(|e| panic!("{}: {e}", setting.key));
        }
    }

    #[test]
    fn the_generated_config_shows_every_key_and_parses_uncommented() {
        let generated = ConfigManager::with_dir(PathBuf::new()).generate_default_config();
        let mut shown = std::collections::HashSet::new();
        let mut section = String::new();
        let mut uncommented = String::new();
        for line in generated.lines() {
            let Some(line) = line.strip_prefix("# ") else {
                uncommented.push_str(line);
                uncommented.push('\n');
                continue;
            };
            if let Some(name) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
                section = name.to_string();
                uncommented.push_str(line);
                uncommented.push('\n');
            } else if let Some((key, _)) = line.split_once(" = ")
                && !key.contains(' ')
            {
                shown.insert(match section.as_str() {
                    "" => key.to_string(),
                    s => format!("{s}.{key}"),
                });
                uncommented.push_str(line);
                uncommented.push('\n');
            }
        }
        for (path, _) in leaf_settings() {
            assert!(
                shown.contains(&path),
                "{path} is not in the generated config"
            );
        }
        // Every value shown, uncommented, is a config datui reads.
        ConfigLayer::parse(&uncommented).unwrap();
    }

    #[test]
    fn every_setting_written_as_its_default_overrides_an_import() {
        // Layers are generic, so no option has merge code of its own; this holds every
        // one of them to it. The import moves each setting it can off its default, and
        // the user's file then writes every default back.
        let defaults = leaf_settings();
        let mut import = ConfigLayer::default();
        let mut moved = Vec::new();
        for (path, value) in &defaults {
            // These follow their own rules, tested on their own.
            if path == "import" || COMBINED_KEYS.iter().any(|(p, _)| p == path) {
                continue;
            }
            let other = match value {
                toml::Value::Boolean(b) => toml::Value::Boolean(!b),
                toml::Value::Integer(n) => toml::Value::Integer(n + 1),
                toml::Value::Array(items) if items.is_empty() => vec!["x"].into(),
                toml::Value::Array(_) => toml::Value::Array(Vec::new()),
                // An `auto` that also takes a bool.
                toml::Value::String(text) if text == "auto" && path == "display.row_numbers" => {
                    true.into()
                }
                toml::Value::String(text) => format!("{text}0").into(),
                other => panic!("{path}: no rule to change {other}"),
            };
            // A string naming a choice, such as `unicode`, has no generic other value.
            if let Ok(layer) = layer_at(path, other.clone()) {
                import.merge(layer);
                moved.push((path.clone(), other));
            }
        }
        assert!(moved.len() > 100, "only {} settings moved", moved.len());

        let serialized = |config: AppConfig| match toml::Value::try_from(config).unwrap() {
            toml::Value::Table(table) => table,
            _ => unreachable!("a struct serializes to a table"),
        };
        let kept = serialized(AppConfig::from_layers([import.clone()]).unwrap());
        for (path, other) in &moved {
            assert_eq!(value_at(&kept, path), Some(other), "{path} was not read");
        }

        let mut own = ConfigLayer::default();
        for (path, value) in &defaults {
            own.merge(layer_at(path, value.clone()).unwrap());
        }
        let restored = serialized(AppConfig::from_layers([import, own]).unwrap());
        for (path, _) in &moved {
            let default = defaults.iter().find(|(p, _)| p == path).map(|(_, v)| v);
            assert_eq!(value_at(&restored, path), default, "{path} kept the import");
        }
    }
}
