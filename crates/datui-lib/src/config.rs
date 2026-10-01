use crate::numfmt::{self, Glob, Grouping, NumberFormat, NumberFormatSettings};
use color_eyre::Result;
use color_eyre::eyre::eyre;
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

    /// Create a new ConfigManager for the given app name.
    ///
    /// `DATUI_CONFIG_DIR` overrides the location. The test suite sets it: templates
    /// live under the config directory, so without the override every App-level test
    /// that saved one wrote it into the developer's own template list — dozens of
    /// "pivot then break" entries were found there. As with the cache, a test that
    /// reaches the real directory refuses rather than writes.
    pub fn new(app_name: &str) -> Result<Self> {
        if let Some(dir) = std::env::var_os("DATUI_CONFIG_DIR") {
            return Ok(Self {
                config_dir: PathBuf::from(dir),
            });
        }
        if crate::cache::running_as_a_cargo_test() {
            panic!(
                "DATUI_CONFIG_DIR is not set: a test would read and write the real \
                 config (templates included). Call common::isolate_cache() before \
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

    /// Generate the default configuration template.
    ///
    /// Ordinary settings are commented out so defaults continue to apply. The built-in
    /// catalog is active: deleting one of its dataset tables must remove that dataset.
    pub fn generate_default_config(&self) -> String {
        // Serialize default config to TOML
        let config = AppConfig::default();
        let toml_str = toml::to_string_pretty(&config)
            .unwrap_or_else(|e| panic!("Failed to serialize default config: {}", e));

        // Build comment map from all struct comment constants
        let comments = Self::collect_all_comments();

        // Comment out ordinary fields, then append the built-in catalog as live TOML.
        // Keeping it separate also avoids teaching the generic formatter about arrays of
        // tables and their nested arrays.
        let mut result = Self::comment_all_fields(toml_str, comments);
        result.push_str(
            "\n# ============================================================================\n\
             # Sources\n\
             # ============================================================================\n\
             # Named collections of datasets, local (path) or remote (url), each listed under\n\
             # its label on the home screen. A collection named \"public\" replaces the\n\
             # built-in catalog below; [data] builtin_catalog = false drops it, and\n\
             # [data] hide_sources hides any collection by name.\n\
             #\n\
             # This active catalog is a snapshot. Delete or edit a dataset table to curate it;\n\
             # configs generated today do not automatically receive future catalog updates.\n",
        );
        result.push_str(&serialize_builtin_catalog());
        result
    }

    /// Collect all field comments from struct constants into a map
    fn collect_all_comments() -> std::collections::HashMap<String, String> {
        let mut comments = std::collections::HashMap::new();

        // Top-level fields
        for (field, comment) in APP_COMMENTS {
            comments.insert(field.to_string(), comment.to_string());
        }

        // Cloud fields
        for (field, comment) in CLOUD_COMMENTS {
            comments.insert(format!("cloud.{}", field), comment.to_string());
        }

        comments.insert(
            "display.unicode".to_string(),
            DISPLAY_UNICODE_COMMENT.to_string(),
        );

        // Data (home screen roots)
        for (field, comment) in DATA_COMMENTS {
            comments.insert(format!("data.{}", field), comment.to_string());
        }

        // Data search (recursive search below the working directory)
        for (field, comment) in DATA_SEARCH_COMMENTS {
            comments.insert(format!("data.search.{}", field), comment.to_string());
        }

        // File loading fields
        for (field, comment) in FILE_LOADING_COMMENTS {
            comments.insert(format!("file_loading.{}", field), comment.to_string());
        }

        // Display fields
        for (field, comment) in DISPLAY_COMMENTS {
            comments.insert(format!("display.{}", field), comment.to_string());
        }

        // Performance fields
        for (field, comment) in PERFORMANCE_COMMENTS {
            comments.insert(format!("performance.{}", field), comment.to_string());
        }

        // Chart fields
        for (field, comment) in CHART_COMMENTS {
            comments.insert(format!("chart.{}", field), comment.to_string());
        }

        // Theme fields
        for (field, comment) in THEME_COMMENTS {
            comments.insert(format!("theme.{}", field), comment.to_string());
        }

        // Color fields
        for (field, comment) in COLOR_COMMENTS {
            comments.insert(format!("theme.colors.{}", field), comment.to_string());
        }

        // Clipboard fields
        for (field, comment) in CLIPBOARD_COMMENTS {
            comments.insert(format!("clipboard.{}", field), comment.to_string());
        }

        // Query fields
        for (field, comment) in QUERY_COMMENTS {
            comments.insert(format!("query.{}", field), comment.to_string());
        }

        // Template fields
        for (field, comment) in TEMPLATE_COMMENTS {
            comments.insert(format!("templates.{}", field), comment.to_string());
        }

        // Debug fields
        for (field, comment) in DEBUG_COMMENTS {
            comments.insert(format!("debug.{}", field), comment.to_string());
        }

        comments
    }

    /// Comment out all fields in TOML and add comments, then add the settings that
    /// are unset by default as commented examples
    fn comment_all_fields(
        toml: String,
        comments: std::collections::HashMap<String, String>,
    ) -> String {
        let mut result = String::new();
        result.push_str("# datui configuration file\n");
        result
            .push_str("# This file uses TOML format. See https://toml.io/ for syntax reference.\n");
        result.push('\n');

        let lines: Vec<&str> = toml.lines().collect();
        let mut i = 0;
        let mut current_section = String::new();
        let mut seen_fields: std::collections::HashSet<String> = std::collections::HashSet::new();

        // First pass: process existing fields and track what we've seen
        while i < lines.len() {
            let line = lines[i];

            // Check if this is a section header
            if let Some(section) = Self::extract_section_name(line) {
                current_section = section.clone();

                // Add section header comment if we have one
                if let Some(header) = SECTION_HEADERS.iter().find(|(s, _)| s == &section) {
                    result.push_str(header.1);
                    result.push('\n');
                }

                // Comment out the section header
                result.push_str("# ");
                result.push_str(line);
                result.push('\n');
                i += 1;
                continue;
            }

            // Check if this is a field assignment
            if let Some(field_path) = Self::extract_field_path_simple(line, &current_section) {
                seen_fields.insert(field_path.clone());

                // Add comment if we have one
                if let Some(comment) = comments.get(&field_path) {
                    for comment_line in comment.lines() {
                        // Blank comment lines stay bare so generated configs
                        // carry no trailing whitespace.
                        if comment_line.is_empty() {
                            result.push_str("#\n");
                            continue;
                        }
                        result.push_str("# ");
                        result.push_str(comment_line);
                        result.push('\n');
                    }
                }

                // Comment out the field line, and every line it continues onto.
                // `toml` renders a non-empty array across several lines, and
                // commenting only the first leaves the elements behind as bare
                // text — a generated config that does not parse.
                result.push_str("# ");
                result.push_str(line);
                result.push('\n');
                let mut depth = bracket_depth(line);
                while depth > 0 && i + 1 < lines.len() {
                    i += 1;
                    result.push_str("# ");
                    result.push_str(lines[i]);
                    result.push('\n');
                    depth += bracket_depth(lines[i]);
                }
            } else {
                // Empty line or other content - preserve as-is
                result.push_str(line);
                result.push('\n');
            }

            i += 1;
        }

        // Second pass: settings that are unset by default and so were not serialized
        result = Self::add_unset_settings(result, &comments, &seen_fields);

        result
    }

    /// Add the documented settings that are unset by default, which serializing the
    /// defaults leaves out, each as a commented example in its section. A section with
    /// nothing serialized of its own, such as `[theme]` beside `[theme.colors]`, gets
    /// its header placed before its first subsection.
    fn add_unset_settings(
        mut result: String,
        comments: &std::collections::HashMap<String, String>,
        seen_fields: &std::collections::HashSet<String>,
    ) -> String {
        let mut sections: Vec<(&str, String)> = Vec::new();
        for (path, example) in UNSET_EXAMPLES {
            if seen_fields.contains(*path) {
                continue;
            }
            let (section, field) = path
                .rsplit_once('.')
                .expect("an unset setting lives in a section");
            let block = match sections.iter().position(|(s, _)| *s == section) {
                Some(i) => &mut sections[i].1,
                None => {
                    sections.push((section, String::new()));
                    &mut sections.last_mut().expect("just pushed").1
                }
            };
            if let Some(comment) = comments.get(*path) {
                for line in comment.lines() {
                    if line.is_empty() {
                        block.push_str("#\n");
                    } else {
                        block.push_str(&format!("# {line}\n"));
                    }
                }
            }
            block.push_str(&format!("# {field} = {example}\n"));
        }

        for (section, block) in sections {
            let header = format!("# [{section}]\n");
            if let Some(pos) = result.find(&header) {
                result.insert_str(pos + header.len(), &block);
                continue;
            }
            let mut at = result
                .find(&format!("# [{section}."))
                .unwrap_or(result.len());
            // Keep a subsection's banner above the subsection.
            let sub = Self::extract_section_name(
                result[at..]
                    .lines()
                    .next()
                    .unwrap_or("")
                    .trim_start_matches("# "),
            );
            if let Some((_, banner)) =
                sub.and_then(|sub| SECTION_HEADERS.iter().find(|(s, _)| *s == sub))
                && result[..at].ends_with(&format!("{banner}\n"))
            {
                at -= banner.len() + 1;
            }
            let mut opened = String::new();
            if let Some((_, banner)) = SECTION_HEADERS.iter().find(|(s, _)| *s == section) {
                opened.push_str(banner);
                opened.push('\n');
            }
            opened.push_str(&header);
            opened.push_str(&block);
            opened.push('\n');
            result.insert_str(at, &opened);
        }

        result
    }

    /// Extract section name from TOML line like "[performance]" or "[theme.colors]"
    fn extract_section_name(line: &str) -> Option<String> {
        let trimmed = line.trim();
        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            Some(trimmed[1..trimmed.len() - 1].to_string())
        } else {
            None
        }
    }

    /// Extract field path from a line (simpler version)
    fn extract_field_path_simple(line: &str, current_section: &str) -> Option<String> {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with('[') {
            return None;
        }

        // Extract field name from line (e.g., "analysis_sample_rows = 10000")
        if let Some(eq_pos) = trimmed.find('=') {
            let field_name = trimmed[..eq_pos].trim();
            if current_section.is_empty() {
                Some(field_name.to_string())
            } else {
                Some(format!("{}.{}", current_section, field_name))
            }
        } else {
            None
        }
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

        // Ensure config directory exists
        self.ensure_config_dir()?;

        // Generate and write default template
        let template = self.generate_default_config();
        write_private(&config_path, &template)?;

        Ok(config_path)
    }
}

/// Complete application configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    /// Additional config files merged in before this file's own values.
    /// See `APP_COMMENTS` for the user-facing description.
    pub import: Vec<String>,
    /// Configuration format version (for future compatibility)
    pub version: String,
    /// Named collections of datasets, `[[sources]]`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<SourceConfig>,
    pub cloud: CloudConfig,
    pub file_loading: FileLoadingConfig,
    pub display: DisplayConfig,
    pub performance: PerformanceConfig,
    pub chart: ChartConfig,
    pub theme: ThemeConfig,
    pub glyphs: GlyphsConfig,
    pub clipboard: ClipboardConfig,
    pub data: DataConfig,
    pub query: QueryConfig,
    pub templates: TemplateConfig,
    pub debug: DebugConfig,
}

// Field comments for AppConfig (top-level fields)
const APP_COMMENTS: &[(&str, &str)] = &[
    (
        "import",
        "Config files to merge in before this file's own values.\n\
         Precedence, lowest first: datui defaults -> each import in order -> this file.\n\
         So an imported theme restyles datui, but anything you set here still wins.\n\
         Paths may be absolute, relative to this file, or use ~ and $VAR.\n\
         An import that does not exist is skipped with a warning on stderr.\n\
         To follow the active Omarchy theme:\n\
         import = [\"~/.local/state/omarchy/current/theme/datui.toml\"]",
    ),
    (
        "version",
        "Configuration format version (for future compatibility)",
    ),
];

// Section header comments
const SECTION_HEADERS: &[(&str, &str)] = &[
    (
        "cloud",
        "# ============================================================================\n# Cloud / Object Storage (S3, MinIO)\n# ============================================================================\n# Optional overrides for s3:// URLs. Leave unset to use AWS defaults (env, ~/.aws/).\n# Set endpoint_url to use MinIO or other S3-compatible backends.",
    ),
    (
        "file_loading",
        "# ============================================================================\n# File Loading Defaults\n# ============================================================================",
    ),
    (
        "display",
        "# ============================================================================\n# Display Settings\n# ============================================================================",
    ),
    (
        "performance",
        "# ============================================================================\n# Performance Settings\n# ============================================================================",
    ),
    (
        "chart",
        "# ============================================================================\n# Chart View\n# ============================================================================",
    ),
    (
        "theme",
        "# ============================================================================\n# Color Theme\n# ============================================================================",
    ),
    (
        "theme.colors",
        "# Color definitions\n# Supported formats:\n#   - Named colors: \"red\", \"blue\", \"bright_red\", \"dark_gray\", etc. (case-insensitive)\n#   - Hex colors: \"#ff0000\" or \"#FF0000\" (case-insensitive)\n#   - Indexed colors: \"indexed(0-255)\" for specific xterm 256-color palette entries\n# Colors automatically adapt to your terminal's capabilities",
    ),
    (
        "glyphs",
        "# ============================================================================\n# Glyph Overrides\n# ============================================================================\n# Replace individual UI glyphs when your font carries more than the tested\n# coverage floor (see scripts/code/audit_glyphs.py in the datui repo). Keys\n# are the slot names in glyphs.rs; an override must keep the display width of\n# the glyph it replaces, and applies only when the Unicode set is active — the\n# ASCII tier never changes. Examples, for fonts that carry them:\n#   in_object_store = \"☁\"                  # the cloud, back again\n#   spinner = [\"◐\", \"◓\", \"◑\", \"◒\"]  # quarter-circle spinner",
    ),
    (
        "clipboard",
        "# ============================================================================\n# Clipboard\n# ============================================================================\n# How the copy dialog (y) reaches the system clipboard.",
    ),
    (
        "query",
        "# ============================================================================\n# Query System\n# ============================================================================",
    ),
    (
        "templates",
        "# ============================================================================\n# View Settings (the section keeps its pre-0.4 name)\n# ============================================================================",
    ),
    (
        "debug",
        "# ============================================================================\n# Debug Settings\n# ============================================================================",
    ),
];

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct CloudConfig {
    /// Custom endpoint for S3-compatible storage (e.g. MinIO). Example: "http://localhost:9000"
    pub s3_endpoint_url: Option<String>,
    /// Access key for S3-compatible backends when not using env / AWS config
    pub s3_access_key_id: Option<String>,
    /// Secret key for S3-compatible backends when not using env / AWS config
    pub s3_secret_access_key: Option<String>,
    /// Region (e.g. us-east-1). Often required when using a custom endpoint (MinIO uses us-east-1).
    pub s3_region: Option<String>,
    /// Stores named in `[[cloud.connections]]`, beside the ones found on the machine.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<CloudConnectionConfig>,
    /// Source IDs never shown on the home screen.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub hide: Vec<String>,
    /// Read an Azure account with its access keys when a sign-in has no data role, as
    /// the Portal does. On unless set to `false`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub azure_account_keys: Option<bool>,
    /// Files to read cloud variables from, relative to the working directory: `.env`.
    /// Only known cloud variable names are taken, and nothing is exported.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub env_files: Vec<String>,
    /// Use the identity of the cloud VM datui runs on (EC2, GCE, Azure). Finding it is a
    /// request to a metadata service, so it is off unless the platform says so.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_identity: Option<bool>,
    /// Which logins found on this machine become home-screen sources. Unset means all.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discover: Option<CloudDiscover>,
    /// List every source's buckets when the home screen opens. Off: a source is listed
    /// when it is entered or on Ctrl+R, and its credential command runs only then.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub list_on_start: Option<bool>,
    /// How the object-store datasets in `[[sources]]` are read. Not a key: derived from
    /// the collections by [`AppConfig`], so resolving a URL needs only this section.
    #[serde(skip)]
    pub dataset_access: Vec<DatasetAccess>,
}

/// How a `[[sources.datasets]]` URL in an object store is read, when it says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetAccess {
    /// The dataset's URL: everything under it is read the same way.
    pub url: String,
    /// The collection it is listed in.
    pub collection: String,
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
///
/// `"all"` and `"none"` are words rather than list members, so no list can say both
/// "everything" and "only s3".
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

/// One store in `[[cloud.connections]]`. Names and pointers only: a secret comes from the
/// environment variable named here, never from the config file itself.
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

/// Whether `id` can name a source: lowercase letters, digits and `-`, starting with a
/// letter or digit, at most 40 characters. It goes into URLs and cache keys, so
/// nothing that needs escaping is allowed in.
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
        // A secret written into the file is refused with the way out, rather than as
        // one more unknown key.
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
                    ". A dataset URL goes in [[sources.datasets]]"
                } else {
                    ""
                }
            ));
        }
        Ok(())
    }
}

/// The ID of the built-in catalog. A configured collection with this name replaces it.
pub const BUILTIN_CATALOG: &str = "public";

/// Two collections of one name in one file.
fn check_source_names(sources: &[SourceConfig]) -> Result<()> {
    for (i, source) in sources.iter().enumerate() {
        if sources[..i].iter().any(|s| s.name == source.name) {
            return Err(eyre!("sources: the name \"{}\" is used twice", source.name));
        }
    }
    Ok(())
}

/// One named collection in `[[sources]]`: datasets wherever they live, on this machine
/// or remote, listed under one heading on the home screen. Only references: nothing is
/// read until a dataset is opened.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(default)]
pub struct SourceConfig {
    /// The collection's ID: what `[data] hide_sources` names, and what a later file
    /// uses to replace it.
    pub name: String,
    /// Shown instead of the name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub datasets: Vec<DatasetConfig>,
    /// Keys that are not recognized, kept so validation can name them.
    #[serde(flatten)]
    pub unknown: std::collections::BTreeMap<String, toml::Value>,
}

/// One dataset in a collection: a local `path` or a remote `url`, never both.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(default)]
pub struct DatasetConfig {
    pub name: String,
    /// A file or directory on this machine. `~` and `$VAR` expand, and a relative path
    /// is relative to the config file that names it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// A file or directory in an object store, or a data file on an HTTP(S) server.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// How an object-store URL is read: `auto` (the default) or `anonymous`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<String>,
    /// The `[[cloud.connections]]` entry whose login reads an object-store URL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection: Option<String>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub publisher: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub license: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub homepage: String,
    /// Keys that are not recognized, kept so validation can name them.
    #[serde(flatten)]
    pub unknown: std::collections::BTreeMap<String, toml::Value>,
}

const SOURCE_KEYS: &str = "name, label, datasets";
const DATASET_KEYS: &str =
    "name, path, url, auth, connection, description, publisher, license, homepage";
const AUTH_VALUES: &str = "auto or anonymous";

/// Where a dataset URL lives, as far as reading it is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UrlPlace {
    /// S3, Google Cloud or Azure, with its connection kind.
    ObjectStore(&'static str),
    Http,
}

impl SourceConfig {
    /// The heading the home screen shows.
    pub fn label(&self) -> &str {
        self.label.as_deref().unwrap_or(&self.name)
    }

    fn validate(&self, connections: &[CloudConnectionConfig]) -> Result<()> {
        let name = &self.name;
        if name.is_empty() {
            return Err(eyre!("sources: every collection needs a name"));
        }
        if !is_valid_source_id(name) {
            return Err(eyre!(
                "sources: \"{name}\" is not a valid name. Use lowercase letters, digits and \
                 '-', up to 40 characters"
            ));
        }
        if !self.unknown.is_empty() {
            return Err(unknown_keys(
                &format!("sources \"{name}\""),
                &self.unknown,
                SOURCE_KEYS,
            ));
        }
        if self.label.as_deref().is_some_and(|l| l.trim().is_empty()) {
            return Err(eyre!("sources \"{name}\": label is blank"));
        }
        if self.datasets.is_empty() {
            return Err(eyre!(
                "sources \"{name}\": no datasets. Add [[sources.datasets]] tables after it"
            ));
        }
        let mut names = std::collections::HashSet::new();
        let mut places = std::collections::HashSet::new();
        for dataset in &self.datasets {
            dataset.validate(name, connections)?;
            if !names.insert(dataset.name.as_str()) {
                return Err(eyre!(
                    "sources \"{name}\": dataset name \"{}\" is used twice",
                    dataset.name
                ));
            }
            if !places.insert(dataset.place_key()) {
                return Err(eyre!(
                    "sources \"{name}\": \"{}\" is listed twice",
                    dataset
                        .path
                        .as_deref()
                        .or(dataset.url.as_deref())
                        .unwrap_or("")
                ));
            }
        }
        Ok(())
    }
}

impl DatasetConfig {
    /// The local path with `~` and `$VAR` expanded, when this is a local dataset.
    pub fn local_path(&self) -> Option<PathBuf> {
        self.path.as_deref().map(expand_path)
    }

    /// What two entries naming the same data have in common.
    fn place_key(&self) -> String {
        match (&self.path, &self.url) {
            (Some(path), _) => format!("path:{}", expand_path(path).display()),
            (None, Some(url)) => format!("url:{}", crate::source::canonical_cloud_place(url)),
            (None, None) => String::new(),
        }
    }

    fn validate(&self, collection: &str, connections: &[CloudConnectionConfig]) -> Result<()> {
        if self.name.trim().is_empty() {
            return Err(eyre!(
                "sources \"{collection}\": every dataset needs a nonempty name"
            ));
        }
        let what = format!("sources \"{collection}\" dataset \"{}\"", self.name);
        if !self.unknown.is_empty() {
            return Err(unknown_keys(&what, &self.unknown, DATASET_KEYS));
        }
        let url = match (&self.path, &self.url) {
            (None, None) => return Err(eyre!("{what}: say where it is with path or url")),
            (Some(_), Some(_)) => {
                return Err(eyre!("{what}: path and url both say where it is. Use one"));
            }
            (Some(path), None) => {
                if path.trim().is_empty() {
                    return Err(eyre!("{what}: path is blank"));
                }
                if path.contains("://") {
                    return Err(eyre!("{what}: \"{path}\" is a URL. Use url = \"{path}\""));
                }
                for (field, set) in [
                    ("auth", self.auth.is_some()),
                    ("connection", self.connection.is_some()),
                ] {
                    if set {
                        return Err(eyre!(
                            "{what}: {field} applies only to a url; a path is read as a file"
                        ));
                    }
                }
                return Ok(());
            }
            (None, Some(url)) => url,
        };
        let place = dataset_url_place(url).ok_or_else(|| {
            if crate::source::split_source_id(url).0.is_some() {
                eyre!(
                    "{what}: name the connection with connection = \"...\" rather than in \
                     the URL"
                )
            } else if url.starts_with("http://") || url.starts_with("https://") {
                eyre!(
                    "{what}: \"{url}\" is not a data file. An HTTP server has no listing, so \
                     a web URL must name a file datui reads, such as .csv or .parquet"
                )
            } else {
                eyre!("{what}: \"{url}\" is not an s3://, gs://, Azure or HTTP(S) URL")
            }
        })?;
        if let Some(auth) = self.auth.as_deref()
            && !matches!(auth, "auto" | "anonymous")
        {
            return Err(eyre!(
                "{what}: auth \"{auth}\" is not valid. Expected {AUTH_VALUES}"
            ));
        }
        let Some(connection) = self.connection.as_deref() else {
            return Ok(());
        };
        let UrlPlace::ObjectStore(kind) = place else {
            return Err(eyre!(
                "{what}: connection applies only to s3://, gs:// and Azure URLs. A web URL is \
                 read with no login"
            ));
        };
        if self.auth.is_some() {
            return Err(eyre!(
                "{what}: auth and connection both say how to read it. Use one"
            ));
        }
        let Some(configured) = connections.iter().find(|c| c.name == connection) else {
            let names: Vec<&str> = connections.iter().map(|c| c.name.as_str()).collect();
            return Err(eyre!(
                "{what}: no [[cloud.connections]] entry is named \"{connection}\"{}",
                if names.is_empty() {
                    String::new()
                } else {
                    format!(". Connections: {}", names.join(", "))
                }
            ));
        };
        let connection_kind = configured.kind.as_deref().unwrap_or("");
        if connection_kind != kind {
            return Err(eyre!(
                "{what}: connection \"{connection}\" is kind = \"{connection_kind}\", which \
                 does not read {kind} URLs"
            ));
        }
        if let (Some(account), Some((url_account, _, _))) = (
            configured.account.as_deref(),
            crate::source::azure_parts(url),
        ) && !account.eq_ignore_ascii_case(&url_account)
        {
            return Err(eyre!(
                "{what}: connection \"{connection}\" signs in to account \"{account}\", but \
                 the URL is in \"{url_account}\""
            ));
        }
        Ok(())
    }
}

fn unknown_keys(
    what: &str,
    unknown: &std::collections::BTreeMap<String, toml::Value>,
    expected: &str,
) -> color_eyre::Report {
    let keys: Vec<String> = unknown.keys().map(|k| format!("'{k}'")).collect();
    eyre!(
        "{what}: unknown key{} {}. Expected one of: {expected}",
        if keys.len() > 1 { "s" } else { "" },
        keys.join(", ")
    )
}

/// Where a dataset URL is, when datui can read it: a place in S3, Google Cloud or Azure
/// (a file or a directory), or a data file on a web server, which has no listing.
fn dataset_url_place(url: &str) -> Option<UrlPlace> {
    if url.chars().any(char::is_whitespace) {
        return None;
    }
    match crate::source::input_source(Path::new(url)) {
        crate::source::InputSource::Azure(_) => Some(UrlPlace::ObjectStore("azure")),
        crate::source::InputSource::S3(rest) | crate::source::InputSource::Gcs(rest) => {
            let host = rest.split('/').next().unwrap_or("");
            let kind = if url.to_ascii_lowercase().starts_with("s3") {
                "s3"
            } else {
                "gcs"
            };
            (!host.is_empty() && !host.contains('@')).then_some(UrlPlace::ObjectStore(kind))
        }
        crate::source::InputSource::Http(_) => {
            let (_, rest) = url.split_once("://")?;
            let (host, path) = rest.split_once('/')?;
            let path = path.split(['?', '#']).next().unwrap_or("");
            (!host.is_empty()
                && !host.contains('@')
                && crate::discover::is_data_file(Path::new(path)))
            .then_some(UrlPlace::Http)
        }
        crate::source::InputSource::Local(_) => None,
    }
}

/// Whether a dataset URL is in an object store, and so browsed as well as opened.
pub fn is_object_store_dataset(url: &str) -> bool {
    matches!(dataset_url_place(url), Some(UrlPlace::ObjectStore(_)))
}

/// The built-in catalog, in the shape a `[[sources]]` entry takes.
pub fn builtin_catalog() -> SourceConfig {
    static CATALOG: std::sync::OnceLock<SourceConfig> = std::sync::OnceLock::new();
    CATALOG
        .get_or_init(|| {
            #[derive(Deserialize)]
            struct Catalog {
                sources: Vec<SourceConfig>,
            }
            let mut sources = toml::from_str::<Catalog>(include_str!("public_datasets.toml"))
                .expect("built-in catalog must be valid TOML")
                .sources;
            assert_eq!(sources.len(), 1, "built-in catalog must be one collection");
            let catalog = sources.remove(0);
            assert_eq!(
                catalog.name, BUILTIN_CATALOG,
                "built-in catalog keeps its ID"
            );
            catalog
                .validate(&[])
                .expect("built-in catalog must validate");
            catalog
        })
        .clone()
}

fn serialize_builtin_catalog() -> String {
    #[derive(Serialize)]
    struct Catalog {
        sources: Vec<SourceConfig>,
    }
    toml::to_string_pretty(&Catalog {
        sources: vec![builtin_catalog()],
    })
    .expect("built-in catalog must serialize")
}

/// Write `contents` to `path`, readable only by the owner.
///
/// The generated config carries a `[cloud]` section inviting an S3 access key
/// and secret. A plain `fs::write` creates the file at 0666 minus the umask,
/// which on most systems is 0644: world-readable. On a machine with more than
/// one account that hands the user's credentials to everybody, and it is not a
/// choice the user made knowingly, since datui is the one that wrote the file.
///
/// The mode is applied twice on purpose. `OpenOptions::mode` only takes effect
/// when the file is created, so it does nothing for `--generate-config --force`
/// over a config that already exists at 0644; `set_permissions` fixes that
/// case. Creating with the mode still matters, because it closes the window
/// where a new file exists at 0644 before the permissions are corrected.
///
/// Non-Unix platforms fall back to a plain write: Windows inherits ACLs from
/// the containing directory, which is already per-user.
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

/// Documented settings that are unset by default, so serializing the defaults leaves
/// them out, with the value the generated config shows for each. Every key here needs
/// a comment, and every commented key must reach the generated config one way or the
/// other; a test checks both.
const UNSET_EXAMPLES: &[(&str, &str)] = &[
    ("cloud.s3_endpoint_url", "\"http://localhost:9000\""),
    ("cloud.s3_access_key_id", "\"\""),
    ("cloud.s3_secret_access_key", "\"\""),
    ("cloud.s3_region", "\"us-east-1\""),
    ("cloud.azure_account_keys", "true"),
    ("cloud.env_files", "[\".env\"]"),
    ("cloud.instance_identity", "false"),
    ("cloud.discover", "true"),
    ("cloud.list_on_start", "false"),
    ("cloud.hide", "[]"),
    ("file_loading.null_values", "[\"NA\", \"amount=\"]"),
    ("file_loading.parse_dates", "true"),
    ("file_loading.parse_strings", "true"),
    ("file_loading.parse_strings_sample_rows", "1000"),
    ("file_loading.infer_schema_length", "1000"),
    ("file_loading.ignore_errors", "false"),
    ("file_loading.decompress_in_memory", "false"),
    ("file_loading.temp_dir", "\"/tmp\""),
    ("file_loading.single_spine_schema", "true"),
    ("display.sidebar_width", "70"),
    ("theme.mode", "\"auto\""),
    ("debug.log_file", "\"~/datui.log\""),
];

const CLOUD_COMMENTS: &[(&str, &str)] = &[
    (
        "s3_endpoint_url",
        "Custom endpoint for S3-compatible storage (MinIO, etc.). Example: \"http://localhost:9000\". Unset = AWS.",
    ),
    (
        "s3_access_key_id",
        "Access key when using custom endpoint (or set AWS_ACCESS_KEY_ID).",
    ),
    (
        "s3_secret_access_key",
        "Secret key when using custom endpoint. Prefer AWS_SECRET_ACCESS_KEY, or the usual AWS credential chain: a secret written here sits in a plain file that backups and dotfile repos will happily copy.",
    ),
    (
        "s3_region",
        "Region (e.g. us-east-1). Required for custom endpoints; MinIO often uses us-east-1.",
    ),
    (
        "azure_account_keys",
        "Read an Azure account with its access keys when a sign-in has no data role (default: true)",
    ),
    (
        "env_files",
        "Files to read cloud variables from, relative to the working directory. None unless listed.\n\
         Only known cloud variable names are taken, and nothing is exported. Adds up across imports.",
    ),
    (
        "instance_identity",
        "Use the identity of the cloud VM datui runs on: EC2, GCE or Azure (default: false)",
    ),
    (
        "discover",
        "Logins found on this machine that become home-screen sources:\n\
         true or \"all\" (default), false or \"none\", or a list such as [\"s3\", \"gcs\", \"azure\"]",
    ),
    (
        "list_on_start",
        "List every source's buckets when the home screen opens, not when one is entered (default: false)",
    ),
    (
        "hide",
        "Cloud source IDs never shown on the home screen. Adds up across imports.",
    ),
];

/// The variables that name an S3 endpoint, in the order they are consulted. The AWS
/// SDKs read the service-specific one first, then the general one; `AWS_ENDPOINT` is
/// what `object_store` accepts.
pub const S3_ENDPOINT_VARS: [&str; 3] = ["AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL", "AWS_ENDPOINT"];

/// A value that says something. `AWS_ENDPOINT_URL=` in a shell, or an empty flag, is
/// not an endpoint and must not erase the one in the config file.
fn non_blank(value: String) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

impl CloudConfig {
    /// The S3 settings the environment sets. The variable list lives here and nowhere
    /// else, so discovery, listing and opening cannot disagree about it. `var` is the
    /// environment, passed in so a test can supply one.
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

    /// Lay the environment or the command line over the config file's settings: each
    /// S3 setting and `discover` that `over` gives wins, and a blank value says
    /// nothing. Nothing else in `over` is read; config files layer through
    /// [`ConfigLayer`].
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
        if over.discover.is_some() {
            self.discover = over.discover;
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct FileLoadingConfig {
    /// When true, CSV and JSON string columns that look like dates or ISO 8601 timestamps become Date or Datetime. Default: true.
    pub parse_dates: Option<bool>,
    /// When true, decompress compressed CSV into memory (eager read). When false (default), decompress to a temp file and use lazy scan.
    pub decompress_in_memory: Option<bool>,
    /// Directory for decompression temp files. Unset = system default (e.g. TMPDIR).
    pub temp_dir: Option<String>,
    /// When true (default), infer Hive/partitioned Parquet schema from one file (single-spine) for faster "Caching schema". When false, use Polars collect_schema() over all files.
    pub single_spine_schema: Option<bool>,
    /// CSV null values: list of strings. Plain string = treat as null in all columns; "COL=VAL" = treat VAL as null only in column COL (first "=" separates). Example: ["NA", "amount="].
    pub null_values: Option<Vec<String>>,
    /// When false, disable parse-strings for CSV. When true or unset, trim and parse all CSV string columns (default). Use CLI --parse-strings=COL for specific columns, --no-parse-strings to disable.
    pub parse_strings: Option<bool>,
    /// Number of rows to sample for parse_strings type inference (single file or multiple/partitioned). Default 1000.
    pub parse_strings_sample_rows: Option<usize>,
    /// Number of rows to use when inferring CSV schema. Unset = use default (1000 in datui). Larger values reduce risk of wrong type (e.g. int then N/A).
    pub infer_schema_length: Option<usize>,
    /// When true, CSV reader ignores parse errors and continues with the next batch. Default false.
    pub ignore_errors: Option<bool>,
}

/// `[file_loading]` keys that described one file's layout rather than a preference,
/// and so mangled every other file they were applied to, each with the flag that
/// says the same thing about the one file being opened.
const REMOVED_FILE_LOADING_KEYS: [(&str, &str); 5] = [
    ("delimiter", "--delimiter"),
    ("has_header", "--no-header"),
    ("skip_lines", "--skip-lines"),
    ("skip_rows", "--skip-rows"),
    ("skip_tail_rows", "--skip-tail-rows"),
];

/// The removed layout keys a config file still sets, so loading can say they are
/// ignored rather than dropping them without a word.
fn removed_file_loading_keys(layer: &toml::Table) -> Vec<(&'static str, &'static str)> {
    let Some(section) = layer.get("file_loading").and_then(|v| v.as_table()) else {
        return Vec::new();
    };
    REMOVED_FILE_LOADING_KEYS
        .into_iter()
        .filter(|(k, _)| section.contains_key(*k))
        .collect()
}

// Field comments for FileLoadingConfig
// Format: (field_name, comment_text)
const FILE_LOADING_COMMENTS: &[(&str, &str)] = &[
    (
        "parse_dates",
        "When true (default), CSV and JSON string columns that look like dates or ISO 8601 timestamps become Date or Datetime",
    ),
    (
        "decompress_in_memory",
        "When true, decompress compressed CSV into memory (eager). When false (default), decompress to a temp file and use lazy scan",
    ),
    (
        "temp_dir",
        "Directory for decompression temp files. Unset = system default (e.g. TMPDIR)",
    ),
    (
        "single_spine_schema",
        "When true (default), a partitioned Parquet dataset's schema is every column any of its files has, read from their footers. When false, Polars decides it from one file.",
    ),
    (
        "null_values",
        "CSV: values to treat as null. Plain string = all columns; \"COL=VAL\" = column COL only. Example: [\"NA\", \"amount=\"]",
    ),
    (
        "parse_strings",
        "When false, disable parse-strings. When true or unset, parse all CSV string columns (default). Use CLI --parse-strings=COL or --no-parse-strings.",
    ),
    (
        "parse_strings_sample_rows",
        "Rows to sample for parse_strings type inference (default 1000).",
    ),
    (
        "infer_schema_length",
        "Number of rows to use when inferring CSV schema (default 1000). Larger values reduce risk of wrong type (e.g. int then N/A).",
    ),
    (
        "ignore_errors",
        "When true, CSV reader ignores parse errors and continues with the next batch (default false).",
    ),
];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DisplayConfig {
    /// Whether to draw box-drawing and arrow characters, or fall back to ASCII.
    pub unicode: crate::glyphs::UnicodeMode,
    pub pages_lookahead: usize,
    pub pages_lookback: usize,
    /// Max rows in scroll buffer (0 = no limit).
    pub max_buffered_rows: usize,
    /// Max buffer size in MB (0 = no limit).
    pub max_buffered_mb: usize,
    pub row_numbers: bool,
    pub row_start_index: usize,
    pub table_cell_padding: usize,
    /// When true, colorize main table cells by column type (string, int, float, bool, temporal).
    pub column_colors: bool,
    /// Show a second header row naming each column's type. `D` toggles it for the session.
    #[serde(default = "default_true")]
    pub dtype_row: bool,
    /// Give the `i` key a quiet accent when datui has noticed something about the data
    /// and the Info panel has not been opened since. The notes are collected either
    /// way; this only decides whether the control bar points at them.
    #[serde(default = "default_true")]
    pub notes_accent: bool,
    /// Optional fixed width for all sidebars (Info, Sort & Filter, Template, Pivot & Melt). When None, use built-in defaults per sidebar.
    #[serde(default)]
    pub sidebar_width: Option<u16>,
    /// Right-align numeric columns and their headers in the data table.
    pub align_numeric_right: bool,
    /// How numbers are displayed. Either a preset name (`number_format = "thousands"`)
    /// or a `[display.number_format]` table for finer control.
    #[serde(default)]
    pub number_format: NumberFormatConfig,
}

/// Number display settings: a preset name shorthand, or a full table.
///
/// Both forms are accepted:
/// ```toml
/// [display]
/// number_format = "thousands"
/// ```
/// ```toml
/// [display.number_format]
/// grouping = "thousands"
/// min_digits = 5
/// ```
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

/// Long-form number formatting options. Every field is optional; unset fields
/// take their value from the preset named by `grouping` (or the default).
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
    /// Keys that are not recognised, captured rather than discarded.
    ///
    /// A misspelled key here would otherwise be invisible: every field has a
    /// default, so the table resolves to "no formatting" — which is also what
    /// the default config does. The user would see identical output whether
    /// they typo'd the key or never wrote it. Capturing unknown keys lets
    /// [`NumberFormatConfig::resolve`] name the offending one instead.
    #[serde(flatten)]
    pub unknown: std::collections::BTreeMap<String, toml::Value>,
}

/// Field names accepted inside `[display.number_format]`, for error messages.
const NUMBER_FORMAT_KEYS: &str =
    "grouping, group_separator, decimal_separator, floats, float_precision, exclude_columns";

impl NumberFormatConfig {
    /// Resolve into the runtime settings used by the renderer.
    ///
    /// Returns a descriptive error for unknown preset names, multi-character
    /// separators, and a group separator equal to the decimal separator (which
    /// would render `1.234.567` ambiguously).
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

        // Formatting starts on only if the user actually configured something.
        // When they did not, F still needs a format to turn on, so the toggle
        // target becomes Thousands grouping while keeping every other setting
        // they chose (separators, min_digits, precision). Comma grouping is what
        // the default user pressing F is asking for.
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

    /// Override just the grouping style, keeping any long-form settings the
    /// user configured.
    ///
    /// `--number-format thousands` should change the grouping without silently
    /// discarding the `exclude_columns` / `min_digits` / precision a user set up
    /// in `[display.number_format]`.
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
        // "system" is the only environment-dependent value, and it is opt-in:
        // data files are locale-neutral, so rendering does not follow the
        // ambient locale unless the user explicitly asks for it.
        let name = if name == "system" {
            match numfmt::system_locale_tag() {
                Some(tag) => numfmt::preset_for_locale_tag(&tag),
                // Unset or C/POSIX: no meaningful locale, so group plainly
                // rather than silently doing nothing.
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

// Field comments for DisplayConfig
const DISPLAY_COMMENTS: &[(&str, &str)] = &[
    (
        "pages_lookahead",
        "Number of pages to buffer ahead of visible area\nLarger values = smoother scrolling but more memory",
    ),
    (
        "pages_lookback",
        "Number of pages to buffer behind visible area\nLarger values = smoother scrolling but more memory",
    ),
    (
        "max_buffered_rows",
        "Maximum rows in scroll buffer (0 = no limit)\nPrevents unbounded memory use when scrolling",
    ),
    (
        "max_buffered_mb",
        "Maximum buffer size in MB (0 = no limit)\nUses estimated memory; helps with very wide tables",
    ),
    (
        "row_numbers",
        "Display row numbers on the left side of the table",
    ),
    ("row_start_index", "Starting index for row numbers (0 or 1)"),
    (
        "table_cell_padding",
        "Number of spaces between columns in the main data table (>= 0)\nDefault 2",
    ),
    (
        "column_colors",
        "Colorize main table cells by column type (string, int, float, bool, date/datetime)\nSet to false to use default text color for all cells",
    ),
    (
        "dtype_row",
        "Show a second header row naming each column's type (str, i64, f64, bool, datetime ...)
D toggles it for the session",
    ),
    (
        "notes_accent",
        "Accent the i key when datui has noticed something about the data and the Info
panel has not been opened since. The Notes tab is there either way",
    ),
    (
        "sidebar_width",
        "Optional: fixed width in characters for all sidebars (Info, Sort & Filter, Views, Pivot & Melt). When unset, each sidebar uses its default width. Example: sidebar_width = 70",
    ),
    (
        "align_numeric_right",
        "Right-align numeric columns and their headers in the data table (default: true)\nSet to false to left-align everything as in datui 0.2.55 and earlier",
    ),
    (
        "number_format",
        "How numbers are displayed in the data table. Press F to toggle on/off while running.\n\
         Shorthand — one of:\n\
         \x20  none         1234567    (default: renders exactly as the file stores it)\n\
         \x20  thousands    1,234,567\n\
         \x20  european     1.234.567,89\n\
         \x20  si           1 234 567.89   (narrow no-break space, ISO 31-0)\n\
         \x20  swiss        1'234'567.89\n\
         \x20  indian       12,34,567.89   (lakh / crore)\n\
         \x20  underscore   1_234_567\n\
         \n\
         For finer control, replace the line below with a table:\n\
         \x20  [display.number_format]\n\
         \x20  grouping = \"thousands\"     # none | thousands | indian | system | any preset above\n\
         \x20  group_separator = \",\"\n\
         \x20  decimal_separator = \".\"\n\
         \x20  floats = true              # group float columns too\n\
         \x20  float_precision = 2        # omit to keep the file's own decimal rendering\n\
         \x20  exclude_columns = [\"*_id\", \"year\"]   # never format these (globs: * and ?)\n\
         \n\
         Every value in a formatted column is grouped. Use exclude_columns for columns that hold\n\
         identifiers rather than quantities -- years, sample IDs, ZIP codes, accession numbers.\n\
         \n\
         grouping = \"system\" is opt-in: it reads LC_ALL / LC_NUMERIC / LANG and picks a matching\n\
         preset. Formatting is otherwise never taken from the environment, because a data file has\n\
         no locale and the same file should render identically on every machine.\n\
         \n\
         Formatting is display-only. Exports, queries, filters and views always use raw values.",
    ),
];

/// Rows an analysis samples by default. Enough that a distribution's shape and a
/// correlation are stable to two decimals; few enough to read in seconds.
pub const DEFAULT_ANALYSIS_SAMPLE_ROWS: usize = 100_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PerformanceConfig {
    /// The analysis sample's starting size: the rows every tool (Describe,
    /// Distribution, Correlation, Data Quality) reads from a table with more, spread
    /// across all of it. 0 starts at every row.
    pub analysis_sample_rows: usize,
    /// When true (default), use Polars streaming engine for LazyFrame collect when the streaming feature is enabled (lower memory, batch processing).
    pub polars_streaming: bool,
    /// The most a Data Quality full scan of a remote dataset may copy into the cache
    /// directory, in MiB, to read the objects once instead of once per pass. 0 never
    /// copies.
    pub quality_local_copy_mb: u64,
}

// Field comments for PerformanceConfig
const PERFORMANCE_COMMENTS: &[(&str, &str)] = &[
    (
        "analysis_sample_rows",
        "The analysis sample's starting size (default 100000): the rows every analysis tool\nreads from a larger table, spread across the whole of it. A smaller table is read whole.\n0 starts at every row. The Sample form (s) changes it, and the method, per session.",
    ),
    (
        "polars_streaming",
        "Use Polars streaming engine for LazyFrame collect when available (default: true). Reduces memory and can improve performance on large or partitioned data.",
    ),
    (
        "quality_local_copy_mb",
        "Data Quality full scans of a remote dataset (default 2048): up to this many MiB are\nfetched once into the cache directory and every pass reads the copy. A larger dataset,\nor one past the free disk, is read in its passes from the source. 0 never copies.",
    ),
];

/// Default for `performance.quality_local_copy_mb`: 2 GiB.
pub const DEFAULT_QUALITY_LOCAL_COPY_MB: u64 = 2048;

/// Default maximum rows used for chart data when not overridden by config or UI.
pub const DEFAULT_CHART_ROW_LIMIT: usize = 10_000;
/// Maximum chart row limit (Polars slice takes u32).
pub const MAX_CHART_ROW_LIMIT: usize = u32::MAX as usize;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ChartConfig {
    /// Rows a chart reads: every row up to n, and a sample of n spread across the table past
    /// it. None = every row. Default 10000.
    pub row_limit: Option<usize>,
}

// Field comments for ChartConfig
const CHART_COMMENTS: &[(&str, &str)] = &[(
    "row_limit",
    "Rows a chart reads (display and export). A larger table is sampled across all of it, and the chart says so.\nCan also be changed in the chart view (Sample size). Example: row_limit = 10000",
)];

impl Default for ChartConfig {
    fn default() -> Self {
        Self {
            row_limit: Some(DEFAULT_CHART_ROW_LIMIT),
        }
    }
}

/// Which set of built-in colour defaults to start from.
///
/// datui's stock chrome (header fills, row striping, borders, secondary text) has
/// to sit *near* the terminal background without matching it. There is no ANSI
/// colour that means "slightly off from the background", so those slots resolve to
/// fixed values — and a set tuned for a dark terminal is unreadable on a light one.
/// This selects which set to use.
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
    /// Resolve `Auto` against the environment. `Dark` and `Light` pass through.
    ///
    /// Detection reads `COLORFGBG`, which several terminals set to `fg;bg` using
    /// ANSI colour numbers — a background of 7 or 15 (white) means a light terminal.
    /// Terminals that do not set it (Alacritty, Kitty and Ghostty among them) fall
    /// back to `Dark`, which is why `mode` can also be set explicitly.
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

/// Where datui looks for datasets on the home screen.
///
/// This is `PATH`-shaped: a short, stable list of *places*, not per-dataset
/// metadata. datui records nothing about the datasets it finds there.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DataConfig {
    /// Directories to offer as roots on the home screen, in order.
    /// Supports `~` and `$VAR`.
    pub directories: Vec<String>,
    /// Whether to also offer directories the desktop records you opening data from.
    /// Only the directories are used, never the file names.
    pub use_desktop_recents: bool,
    /// Whether the home screen lists files datui has no reader for, dimmed, from the
    /// start. `Ctrl+A` flips it for the session either way.
    pub show_unreadable_files: bool,
    /// Whether the built-in `public` collection exists.
    pub builtin_catalog: bool,
    /// Collection names never shown on the home screen, built-in or configured.
    pub hide_sources: Vec<String>,
    /// Recursive search of the working directory from the home screen's filter.
    pub search: SearchConfig,
}

/// Recursive search under the working directory, driven by the home screen's filter.
///
/// The walk happens once, in the background, the first time you type; every keystroke
/// after that filters the result in memory. The limits here bound that one walk.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SearchConfig {
    /// Search below the working directory at all.
    pub enabled: bool,
    /// How deep to descend. Data is rarely twelve directories down, and the cost of
    /// looking is paid on every branch.
    pub max_depth: usize,
    /// Stop after this many datasets. The list is a way to find something, not an
    /// inventory.
    pub max_results: usize,
    /// Give up walking after this long and keep what was found. A cold or enormous
    /// tree must degrade to partial results, never to a wait.
    pub time_budget_ms: u64,
    /// Descend into directories on a different filesystem than the one started in.
    ///
    /// Off by default, and the most important limit here: it is what stops a walk
    /// from wandering onto a network share, and on a machine using autofs it is what
    /// stops the walk from *mounting* one by looking at it.
    pub cross_filesystems: bool,
    /// Obey `.gitignore`.
    ///
    /// Off by default, and deliberately: people gitignore data directories precisely
    /// because the data is too big to commit, which is the same reason they want to
    /// open it in datui. In datui's own repository, honouring it hides 38 real test
    /// datasets while hiding 69 files of virtualenv noise — wrong in both directions.
    /// The skip list below is the mechanism for the noise.
    pub follow_gitignore: bool,
    /// Directory names never descended into. Replaces the defaults entirely.
    pub skip: Vec<String>,
    /// Directory names to skip *in addition* to the defaults, so adding one does not
    /// mean restating the list.
    pub skip_extra: Vec<String>,
    /// File extensions searched for. Empty means every format datui can open, which
    /// includes `json` and `txt` — noisy in a source tree, so narrow this if that
    /// bothers you.
    pub extensions: Vec<String>,
}

/// Directories that are never data, and are always expensive.
///
/// Hidden directories are already skipped, which covers `.git`, `.venv`, `.tox` and
/// the various caches. What is left is the offenders that are not hidden — and they
/// matter: `node_modules` and `site-packages` are full of `.json`, which datui can
/// open, so without this every package manifest on the machine is a search result.
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
            max_results: 20_000,
            time_budget_ms: 1_500,
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

impl Default for DataConfig {
    fn default() -> Self {
        Self {
            directories: Vec::new(),
            // On by default: it only ever contributes *places*, and it is the one
            // thing that gives a fresh install somewhere to point you.
            use_desktop_recents: true,
            show_unreadable_files: false,
            builtin_catalog: true,
            hide_sources: Vec::new(),
            search: SearchConfig::default(),
        }
    }
}

impl DataConfig {
    /// Configured directories with `~`/`$VAR` expanded. Non-existent paths are kept:
    /// the home screen shows an unavailable root rather than hiding it, because
    /// "the mount is down" is information.
    pub fn resolved_directories(&self) -> Vec<PathBuf> {
        self.directories.iter().map(|d| expand_path(d)).collect()
    }
}

const DISPLAY_UNICODE_COMMENT: &str = "Draw box-drawing and arrow characters: \"auto\" (default), \"always\", or \"never\".\n\
     \"auto\" uses them when the locale is UTF-8. Set \"never\" on a terminal that shows\n\
     replacement boxes instead — datui falls back to plain ASCII throughout.";

const DATA_COMMENTS: &[(&str, &str)] = &[
    (
        "directories",
        "Directories to offer as roots on the datui home screen (opened with no arguments).\n\
     Think of this like PATH: a list of places, not a catalog. datui stores nothing\n\
     about what it finds. Supports ~ and $VAR.\n\
     Directories of datasets you opened recently are offered automatically, so this is\n\
     only needed for places you have not visited yet. Ctrl+D on the home screen keeps\n\
     a directory listed without editing this file.\n\
     Example: directories = [\"/mnt/data\", \"~/datasets\"]",
    ),
    (
        "use_desktop_recents",
        "Also offer directories your desktop records you opening data files from\n\
         (freedesktop's recently-used list, written by file managers and GTK apps).\n\
         Only the DIRECTORIES are used, never the file names: that list often holds\n\
         things you would not want on a screen you are sharing.\n\
         Set false to ignore it entirely.",
    ),
    (
        "show_unreadable_files",
        "List files datui has no reader for (README.md, model.onnx) on the home screen,\n\
         dimmed, instead of hiding them. Ctrl+A shows or hides them for the session.",
    ),
    (
        "builtin_catalog",
        "Offer the built-in \"public\" collection of datasets on the home screen.\n\
         A [[sources]] entry named \"public\" replaces it instead.",
    ),
    (
        "hide_sources",
        "[[sources]] collections not to show, by name, the built-in \"public\" included.\n\
         Names add up across imported files. Example: hide_sources = [\"public\"]",
    ),
];

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ThemeConfig {
    /// Which built-in palette to start from. `None` means the key was absent, which
    /// is treated as `Auto`; a loaded config holds the resolved mode.
    pub mode: Option<ThemeMode>,
    pub colors: ColorConfig,
}

// Field comments for ThemeConfig
const THEME_COMMENTS: &[(&str, &str)] = &[(
    "mode",
    "Which built-in color set to start from: \"auto\" (default), \"dark\" or \"light\".\n\
     datui's stock chrome (header fills, row striping, borders, dim text) uses fixed\n\
     shades, and a set tuned for a dark terminal is unreadable on a light one.\n\
     \"auto\" reads COLORFGBG and falls back to dark; Alacritty, Kitty and Ghostty do\n\
     not set it, so on a light background in those terminals set this to \"light\".\n\
     Individual colors below always override whichever set is chosen.",
)];

fn default_row_numbers_color() -> String {
    "dark_gray".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
/// Color configuration for the application theme.
///
/// This struct defines all color settings used throughout the UI. Colors can be specified as:
/// - Named colors: "cyan", "red", "yellow", etc.
/// - Hex colors: "#ff0000"
/// - Indexed colors: "indexed(236)" for 256-color palette
/// - Special modifiers: "reversed" for selected rows
///
/// ## Color Usage:
///
/// **UI Element Colors:**
/// - `keybind_hints`: Keybind hints (modals, breadcrumb, correlation matrix)
/// - `keybind_labels`: Action labels in controls bar
/// - `throbber`: Busy indicator (spinner) in control bar
/// - `table_header`: Table column header text
/// - `table_header_bg`: Table column header background
/// - `column_separator`: Vertical line between columns
/// - `sidebar_border`: Sidebar borders
/// - `modal_border_active`: Active modal elements
/// - `modal_border_error`: Error modal borders
///
/// **Chart Colors:**
/// - `primary_chart_series_color`: Chart data (histogram bars, Q-Q plot data points)
/// - `secondary_chart_series_color`: Chart theory (histogram overlays, Q-Q plot reference line)
///
/// **Status Colors:**
/// - `success`: Success indicators, normal distributions
/// - `error`: Error messages, outliers
/// - `warning`: Warnings, skewed distributions
/// - `distribution_normal`: Normal distribution indicator
/// - `distribution_skewed`: Skewed distribution indicator
/// - `distribution_other`: Other distribution types
/// - `outlier_marker`: Outlier indicators
///
/// **Text Colors:**
/// - `text_primary`: Primary text
/// - `text_secondary`: Secondary text
/// - `text_inverse`: Text on light backgrounds
///
/// **Background Colors:**
/// - `background`: Main background
/// - `surface`: Modal/surface backgrounds
/// - `controls_bg`: Controls bar and table header backgrounds
///
/// **Other:**
/// - `dimmed`: Dimmed elements, axis lines
/// - `table_selected`: Selected row style (special modifier)
pub struct ColorConfig {
    pub keybind_hints: String,
    pub keybind_labels: String,
    pub throbber: String,
    pub primary_chart_series_color: String,
    pub secondary_chart_series_color: String,
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
    /// Row numbers column text. Use "default" for terminal default.
    #[serde(default = "default_row_numbers_color")]
    pub row_numbers: String,
    pub column_separator: String,
    pub table_selected: String,
    pub sidebar_border: String,
    pub modal_border_active: String,
    pub modal_border_error: String,
    pub distribution_normal: String,
    pub distribution_skewed: String,
    pub distribution_other: String,
    pub outlier_marker: String,
    pub cursor_focused: String,
    pub cursor_dimmed: String,
    /// Text under the solid cursor block. "default" picks black or white by the
    /// cursor color's luminance.
    #[serde(default = "default_cursor_text")]
    pub cursor_text: String,
    /// "default" = no alternate row color; any other value is parsed as a color (e.g. "dark_gray")
    pub alternate_row_color: String,
    /// Column type colors (main data table): string, integer, float, boolean, temporal
    pub str_col: String,
    pub int_col: String,
    pub float_col: String,
    pub bool_col: String,
    pub temporal_col: String,
    /// Main data table: placeholder color for binary columns (the `‹binary›` stub)
    pub binary_col: String,
    /// Chart view: series colors 1–7 (line/scatter/bar series)
    pub chart_series_color_1: String,
    pub chart_series_color_2: String,
    pub chart_series_color_3: String,
    pub chart_series_color_4: String,
    pub chart_series_color_5: String,
    pub chart_series_color_6: String,
    pub chart_series_color_7: String,
    /// The one colour that means "this is the thing": focused titles, key chips, the
    /// selection rail. Absent from older configs, so it falls back to the palette.
    #[serde(default = "default_accent")]
    pub accent: String,
    /// A brighter accent for a focused title or a value that just changed.
    #[serde(default = "default_accent_bright")]
    pub accent_bright: String,
    /// Two stops for the wordmark on the home screen. Used nowhere else on purpose:
    /// a gradient on data would be decoration.
    #[serde(default = "default_gradient_start")]
    pub gradient_start: String,
    #[serde(default = "default_gradient_end")]
    pub gradient_end: String,
}

fn default_true() -> bool {
    true
}

fn default_cursor_text() -> String {
    ColorConfig::default().cursor_text
}
fn default_accent() -> String {
    ColorConfig::default().accent
}
fn default_accent_bright() -> String {
    ColorConfig::default().accent_bright
}
fn default_gradient_start() -> String {
    ColorConfig::default().gradient_start
}
fn default_gradient_end() -> String {
    ColorConfig::default().gradient_end
}

// Field comments for ColorConfig
const COLOR_COMMENTS: &[(&str, &str)] = &[
    (
        "keybind_hints",
        "Keybind hints (modals, breadcrumb, correlation matrix)",
    ),
    ("keybind_labels", "Action labels in controls bar"),
    ("throbber", "Busy indicator (spinner) in control bar"),
    (
        "primary_chart_series_color",
        "Chart data (histogram bars, Q-Q plot data points)",
    ),
    (
        "secondary_chart_series_color",
        "Chart theory (histogram overlays, Q-Q plot reference line)",
    ),
    ("success", "Success indicators, normal distributions"),
    ("error", "Error messages, outliers"),
    ("warning", "Warnings, skewed distributions"),
    ("dimmed", "Dimmed elements, axis lines"),
    ("background", "Main background"),
    ("surface", "Modal/surface backgrounds"),
    ("controls_bg", "Controls bar background"),
    ("text_primary", "Primary text"),
    ("text_secondary", "Secondary text"),
    ("text_inverse", "Text on light backgrounds"),
    ("table_header", "Table column header text"),
    ("table_header_bg", "Table column header background"),
    (
        "row_numbers",
        "Row numbers column text; use \"default\" for terminal default",
    ),
    ("column_separator", "Vertical line between columns"),
    ("table_selected", "Selected row style"),
    ("sidebar_border", "Sidebar borders"),
    ("modal_border_active", "Active modal elements"),
    ("modal_border_error", "Error modal borders"),
    ("distribution_normal", "Normal distribution indicator"),
    ("distribution_skewed", "Skewed distribution indicator"),
    ("distribution_other", "Other distribution types"),
    ("outlier_marker", "Outlier indicators"),
    (
        "cursor_focused",
        "Cursor color when text input is focused\n\"default\" reverses the text under the cursor instead",
    ),
    (
        "cursor_dimmed",
        "Cursor color when text input is unfocused (currently unused - unfocused inputs hide cursor)",
    ),
    (
        "cursor_text",
        "Text color under the cursor block\n\"default\" picks black or white by the cursor color's luminance",
    ),
    (
        "alternate_row_color",
        "Background color for every other row in the main data table\nSet to \"default\" to disable alternate row coloring",
    ),
    ("str_col", "Main table: string column text color"),
    ("int_col", "Main table: integer column text color"),
    ("float_col", "Main table: float column text color"),
    ("bool_col", "Main table: boolean column text color"),
    (
        "temporal_col",
        "Main table: date/datetime/time column text color",
    ),
    ("binary_col", "Main table: binary column placeholder color"),
    ("chart_series_color_1", "Chart view: first series color"),
    ("chart_series_color_2", "Chart view: second series color"),
    ("chart_series_color_3", "Chart view: third series color"),
    ("chart_series_color_4", "Chart view: fourth series color"),
    ("chart_series_color_5", "Chart view: fifth series color"),
    ("chart_series_color_6", "Chart view: sixth series color"),
    ("chart_series_color_7", "Chart view: seventh series color"),
    (
        "accent",
        "The accent: key chips in the control bar, focused section titles, the selection rail",
    ),
    (
        "accent_bright",
        "A brighter accent, for the section the cursor is in",
    ),
    (
        "gradient_start",
        "First stop of the wordmark gradient on the home screen",
    ),
    (
        "gradient_end",
        "Last stop of the wordmark gradient on the home screen",
    ),
];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct QueryConfig {
    pub history_limit: usize,
    pub enable_history: bool,
    pub default_mode: QueryMode,
}

/// A mode of the query prompt, in tab order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum QueryMode {
    #[default]
    Sql,
    /// Rows whose text columns contain every word (the fuzzy search).
    Search,
    /// Datui's q-inspired language.
    QStyle,
}

impl QueryMode {
    /// The modes this build offers, in tab order. SQL is absent without the
    /// `sql` feature rather than present and broken.
    pub fn available() -> &'static [QueryMode] {
        #[cfg(feature = "sql")]
        {
            &[QueryMode::Sql, QueryMode::Search, QueryMode::QStyle]
        }
        #[cfg(not(feature = "sql"))]
        {
            &[QueryMode::Search, QueryMode::QStyle]
        }
    }

    /// This mode if the build offers it, otherwise the next one in tab order.
    pub fn resolve(self) -> QueryMode {
        if Self::available().contains(&self) {
            self
        } else {
            QueryMode::Search
        }
    }

    pub fn title(self) -> &'static str {
        match self {
            QueryMode::Sql => "SQL",
            QueryMode::Search => "Search",
            QueryMode::QStyle => "q-style",
        }
    }

    /// Position among the available modes: the tab index.
    pub fn index(self) -> usize {
        Self::available()
            .iter()
            .position(|&m| m == self)
            .unwrap_or(0)
    }

    pub fn next(self) -> QueryMode {
        let modes = Self::available();
        modes[(self.index() + 1) % modes.len()]
    }

    pub fn prev(self) -> QueryMode {
        let modes = Self::available();
        modes[(self.index() + modes.len() - 1) % modes.len()]
    }
}

// Field comments for QueryConfig
const QUERY_COMMENTS: &[(&str, &str)] = &[
    (
        "history_limit",
        "Maximum number of queries to keep in history",
    ),
    ("enable_history", "Enable query history caching"),
    (
        "default_mode",
        "Mode / opens on when no query is active: \"sql\", \"search\" or \"q-style\".\n\
         Editing an active query reopens its own mode.\n\
         A build without SQL opens on \"search\" instead of \"sql\".",
    ),
];

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TemplateConfig {
    pub auto_apply: bool,
}

// Field comments for TemplateConfig
const TEMPLATE_COMMENTS: &[(&str, &str)] = &[(
    "auto_apply",
    "Apply the best-matching view when a file opens",
)];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DebugConfig {
    pub enabled: bool,
    pub show_performance: bool,
    pub show_query: bool,
    pub show_transformations: bool,
    /// Where the log goes. Unset: `datui.log` in the cache directory.
    pub log_file: Option<String>,
}

// Field comments for DebugConfig
const DEBUG_COMMENTS: &[(&str, &str)] = &[
    ("enabled", "Enable debug overlay by default"),
    (
        "show_performance",
        "Show performance metrics in debug overlay",
    ),
    ("show_query", "Show LazyFrame query in debug overlay"),
    (
        "show_transformations",
        "Show transformation state in debug overlay",
    ),
    (
        "log_file",
        "Log file path (default: datui.log in the cache directory; DATUI_LOG sets the level)",
    ),
];

// Default implementations
/// `[clipboard]`: how the copy dialog reaches the system clipboard.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ClipboardConfig {
    /// "auto", "native" (display server through arboard) or "osc52" (an
    /// escape sequence the terminal applies; what works over SSH).
    pub backend: String,
    /// Longest OSC 52 payload to attempt, in KB of base64. Terminals cap the
    /// sequences they accept; a generous terminal's user can raise this.
    pub osc52_limit_kb: usize,
}

impl Default for ClipboardConfig {
    fn default() -> Self {
        Self {
            backend: "auto".to_string(),
            osc52_limit_kb: 100,
        }
    }
}

// Field comments for ClipboardConfig
const CLIPBOARD_COMMENTS: &[(&str, &str)] = &[
    (
        "backend",
        "How the copy dialog (y) reaches the system clipboard\n\
         \"auto\": native where a display server answers, osc52 elsewhere (SSH)\n\
         \"native\": the display server, with an HTML flavor beside tabular copies\n\
         \"osc52\": an escape sequence the terminal applies; tmux needs set-clipboard on",
    ),
    (
        "osc52_limit_kb",
        "Longest osc52 copy to attempt, in KB of base64 (terminals cap what they accept)",
    ),
];

/// `[glyphs]`: per-slot overrides laid over the Unicode set, so a font that has
/// more than the coverage floor gets to use it — `☁` back for the object-store
/// mark, a Nerd Font icon for a checkbox. Keys are the slot names in
/// `glyphs.rs`; values keep the display width of the glyph they replace.
/// Overrides never touch the ASCII set, which stays the tested floor. Layered
/// like every section: defaults, then each import, then the user's file.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GlyphsConfig {
    #[serde(flatten)]
    pub overrides: std::collections::BTreeMap<String, crate::glyphs::SlotOverride>,
}

impl Default for AppConfig {
    fn default() -> Self {
        let mut config = Self {
            import: Vec::new(),
            version: "0.2".to_string(),
            sources: Vec::new(),
            cloud: CloudConfig::default(),
            file_loading: FileLoadingConfig::default(),
            display: DisplayConfig::default(),
            performance: PerformanceConfig::default(),
            chart: ChartConfig::default(),
            theme: ThemeConfig::default(),
            glyphs: GlyphsConfig::default(),
            clipboard: ClipboardConfig::default(),
            data: DataConfig::default(),
            query: QueryConfig::default(),
            templates: TemplateConfig::default(),
            debug: DebugConfig::default(),
        };
        config.sync_dataset_access();
        config
    }
}

impl Default for DisplayConfig {
    fn default() -> Self {
        Self {
            unicode: crate::glyphs::UnicodeMode::default(),
            pages_lookahead: 3,
            pages_lookback: 3,
            max_buffered_rows: crate::widgets::datatable::DEFAULT_MAX_BUFFERED_ROWS,
            max_buffered_mb: 512,
            row_numbers: false,
            row_start_index: 1,
            table_cell_padding: 2,
            column_colors: true,
            dtype_row: true,
            notes_accent: true,
            sidebar_width: None,
            align_numeric_right: true,
            number_format: NumberFormatConfig::default(),
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            analysis_sample_rows: DEFAULT_ANALYSIS_SAMPLE_ROWS,
            polars_streaming: true,
            quality_local_copy_mb: DEFAULT_QUALITY_LOCAL_COPY_MB,
        }
    }
}

impl Default for ColorConfig {
    /// Dark, preserving datui's historical defaults. Light is opt-in via
    /// `theme.mode`, so no existing config changes appearance.
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
        // "Night Market": Tokyo Night's palette with one cyan accent. Chrome sits in
        // three tiers a few percent apart (controls_bg, table_header_bg, the stripe)
        // rather than one grey shared by everything, and the row under the cursor is
        // tinted rather than reversed so cell colours survive on it.
        Self {
            keybind_hints: "#7dcfff".to_string(),
            keybind_labels: "#a9b1d6".to_string(),
            throbber: "#7dcfff".to_string(),
            primary_chart_series_color: "#7dcfff".to_string(),
            secondary_chart_series_color: "#565f89".to_string(),
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
            row_numbers: "#565f89".to_string(),
            column_separator: "#3b4261".to_string(),
            table_selected: "#283457".to_string(),
            // Box titles are drawn in the border colour, so this has to read as text:
            // the theme's comment grey, not the hairline shade the rules use.
            sidebar_border: "#565f89".to_string(),
            modal_border_active: "#7dcfff".to_string(),
            modal_border_error: "#f7768e".to_string(),
            distribution_normal: "#9ece6a".to_string(),
            distribution_skewed: "#e0af68".to_string(),
            distribution_other: "#c0caf5".to_string(),
            outlier_marker: "#f7768e".to_string(),
            cursor_focused: "default".to_string(),
            cursor_dimmed: "default".to_string(),
            cursor_text: "default".to_string(),
            alternate_row_color: "#1e2030".to_string(),
            str_col: "#9ece6a".to_string(),
            int_col: "#7aa2f7".to_string(),
            float_col: "#2ac3de".to_string(),
            bool_col: "#e0af68".to_string(),
            temporal_col: "#bb9af7".to_string(),
            binary_col: "#565f89".to_string(),
            chart_series_color_1: "#7dcfff".to_string(),
            chart_series_color_2: "#bb9af7".to_string(),
            chart_series_color_3: "#9ece6a".to_string(),
            chart_series_color_4: "#e0af68".to_string(),
            chart_series_color_5: "#7aa2f7".to_string(),
            chart_series_color_6: "#f7768e".to_string(),
            chart_series_color_7: "#ff9e64".to_string(),
            accent: "#7dcfff".to_string(),
            accent_bright: "#a4daff".to_string(),
            gradient_start: "#7aa2f7".to_string(),
            gradient_end: "#bb9af7".to_string(),
        }
    }

    /// Defaults tuned for a light terminal background.
    ///
    /// The chrome shades are inverted rather than merely lightened: on a light
    /// terminal the "slightly off from background" shades must be *darker* than the
    /// background, where on a dark terminal they are lighter. Hues that are legible
    /// on black and not on white (plain `cyan`, plain `yellow`) are replaced with
    /// darker equivalents from the 256-colour cube.
    pub fn light() -> Self {
        // Tokyo Night's "day" variant: the same hues, darkened until every one of them
        // clears 4.5:1 on a white or near-white background. The chrome tiers go the
        // other way — a little darker than the terminal rather than lighter.
        Self {
            keybind_hints: "#2e7de9".to_string(),
            keybind_labels: "#3760bf".to_string(),
            throbber: "#2e7de9".to_string(),
            primary_chart_series_color: "#2e7de9".to_string(),
            secondary_chart_series_color: "#848cb5".to_string(),
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
            row_numbers: "#848cb5".to_string(),
            column_separator: "#a8aecb".to_string(),
            table_selected: "#b6bfe2".to_string(),
            sidebar_border: "#6172b0".to_string(),
            modal_border_active: "#2e7de9".to_string(),
            modal_border_error: "#f52a65".to_string(),
            distribution_normal: "#587539".to_string(),
            distribution_skewed: "#8c6c3e".to_string(),
            distribution_other: "#3760bf".to_string(),
            outlier_marker: "#f52a65".to_string(),
            cursor_focused: "default".to_string(),
            cursor_dimmed: "default".to_string(),
            cursor_text: "default".to_string(),
            alternate_row_color: "#dcdfea".to_string(),
            str_col: "#587539".to_string(),
            int_col: "#2e7de9".to_string(),
            float_col: "#007197".to_string(),
            bool_col: "#8c6c3e".to_string(),
            temporal_col: "#9854f1".to_string(),
            binary_col: "#848cb5".to_string(),
            chart_series_color_1: "#2e7de9".to_string(),
            chart_series_color_2: "#9854f1".to_string(),
            chart_series_color_3: "#587539".to_string(),
            chart_series_color_4: "#8c6c3e".to_string(),
            chart_series_color_5: "#007197".to_string(),
            chart_series_color_6: "#f52a65".to_string(),
            chart_series_color_7: "#b15c00".to_string(),
            accent: "#2e7de9".to_string(),
            accent_bright: "#1a6cd0".to_string(),
            gradient_start: "#2e7de9".to_string(),
            gradient_end: "#9854f1".to_string(),
        }
    }
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            history_limit: 1000,
            enable_history: true,
            default_mode: QueryMode::default(),
        }
    }
}

impl Default for DebugConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            show_performance: true,
            show_query: true,
            show_transformations: true,
            log_file: None,
        }
    }
}

/// Maximum number of config files an `import` chain may stack up.
///
/// Chains this deep are a mistake rather than a use case; the cap turns a
/// runaway (or merely confusing) graph into a clear error.
const MAX_IMPORT_DEPTH: usize = 8;

/// Expand a leading `~` and any `$VAR` / `${VAR}` reference in a config path.
///
/// Unset variables expand to nothing, as in a shell. This is what lets a config
/// name a path such as `~/.local/state/omarchy/current/theme/datui.toml` without
/// hardcoding a home directory.
pub fn expand_config_path(raw: &str) -> PathBuf {
    expand_path(raw)
}

fn expand_path(raw: &str) -> PathBuf {
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
    if expanded == "~" {
        if let Some(home) = dirs::home_dir() {
            return home;
        }
    } else if let Some(rest) = expanded
        .strip_prefix("~/")
        // What `display_path` writes there, and what a Windows user types.
        .or_else(|| expanded.strip_prefix("~\\").filter(|_| cfg!(windows)))
        && let Some(home) = dirs::home_dir()
    {
        return home.join(rest);
    }

    PathBuf::from(expanded)
}

/// One config file's settings as written: the keys it sets and nothing else.
///
/// Keeping a layer partial is what lets a later file set a value back to its
/// default: `notes_accent = true` in your config undoes an import's `false`, and a
/// file that leaves the key out changes nothing. [`AppConfig::from_layers`] fills in
/// the defaults once, after every layer is merged.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConfigLayer {
    table: toml::Table,
    /// The files this one imports, as written. Never merged: a load-time directive.
    imports: Vec<String>,
}

/// How a key combines across layers when a later layer does not simply replace it.
#[derive(Debug, Clone, Copy)]
enum Combine {
    /// An array of tables matched by `name`: an entry replaces the earlier one of its
    /// name whole, and a new name appends. Two of one name in one file are both kept,
    /// for validation to name.
    ByName,
    /// A list that adds up across files, without repeats.
    Union,
}

/// The keys that do not follow "a later layer's value replaces the earlier one".
/// Tables merge key by key; everything else not listed here is replaced whole.
const COMBINED_KEYS: &[(&str, Combine)] = &[
    ("sources", Combine::ByName),
    ("cloud.connections", Combine::ByName),
    ("cloud.hide", Combine::Union),
    ("cloud.env_files", Combine::Union),
    ("data.hide_sources", Combine::Union),
];

/// `[cloud]` keys where a blank value says nothing, so `s3_region = ""` cannot erase
/// an imported region. The same rule as for the environment and the command line.
const CLOUD_BLANK_IS_UNSET: [&str; 4] = [
    "s3_endpoint_url",
    "s3_access_key_id",
    "s3_secret_access_key",
    "s3_region",
];

impl ConfigLayer {
    /// A layer from TOML text. Types are checked here, so a mistake is reported
    /// against the file that holds it rather than after merging.
    pub fn parse(text: &str) -> Result<Self> {
        let typed: AppConfig = toml::from_str(text)?;
        let mut table: toml::Table = toml::from_str(text)?;
        table.remove("import");
        if let Some(cloud) = table.get_mut("cloud").and_then(toml::Value::as_table_mut) {
            for key in CLOUD_BLANK_IS_UNSET {
                if let Some(value) = cloud.get(key).and_then(toml::Value::as_str) {
                    match non_blank(value.to_string()) {
                        Some(trimmed) => cloud.insert(key.to_string(), trimmed.into()),
                        None => cloud.remove(key),
                    };
                }
            }
        }
        Ok(Self {
            table,
            imports: typed.import,
        })
    }

    /// The layer in `path`, or `None` when there is no such file. A file that exists
    /// but cannot be read or parsed is an error naming it, and `importer`, the file
    /// that imported it, if any.
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
                "Failed to parse config file at {named}: {}",
                parse_reason(&e)
            )
        })?;
        for (key, flag) in removed_file_loading_keys(&layer.table) {
            eprintln!(
                "datui: warning: {}: file_loading.{key} is no longer read; \
                 pass {flag} when opening the file it describes",
                path.display(),
            );
        }
        layer.anchor_paths(path.parent().unwrap_or_else(|| Path::new(".")));
        Ok(Some(layer))
    }

    /// Resolve relative dataset `path`s against `dir`, the directory of the file that
    /// named them, before a layer from another directory can be merged with them.
    fn anchor_paths(&mut self, dir: &Path) {
        let Some(toml::Value::Array(sources)) = self.table.get_mut("sources") else {
            return;
        };
        let datasets = sources
            .iter_mut()
            .filter_map(|s| s.get_mut("datasets"))
            .filter_map(toml::Value::as_array_mut)
            .flatten();
        for dataset in datasets {
            if let Some(toml::Value::String(path)) = dataset.get_mut("path")
                && !path.trim().is_empty()
                && expand_path(path).is_relative()
            {
                *path = dir.join(expand_path(path)).to_string_lossy().into_owned();
            }
        }
    }

    /// Lay `upper` over this layer: every key `upper` writes wins, except the
    /// combined keys in [`COMBINED_KEYS`], and keys it leaves out keep this layer's
    /// value. `upper`'s imports are not carried over.
    pub fn merge(&mut self, upper: ConfigLayer) {
        merge_tables(&mut self.table, upper.table, "");
    }
}

/// A TOML error with its reason and place on the first line, then the excerpt of the
/// file under it. TOML puts the reason last, but some callers, such as the Python
/// binding, show only the first line.
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
    /// Load configuration from all layers (default → imports → user config)
    pub fn load(app_name: &str) -> Result<Self> {
        match ConfigManager::new(app_name) {
            Ok(manager) => Self::load_from_file(&manager.config_path("config.toml")),
            // No config directory on this platform: defaults are all there is.
            Err(_) => {
                let config = AppConfig::default();
                config
                    .validate()
                    .map_err(|e| eyre!("Invalid configuration: {}", e))?;
                Ok(config)
            }
        }
    }

    /// Load configuration rooted at `config_path`, resolving its `import` chain.
    ///
    /// Layers apply lowest precedence first: datui's defaults, then every file named
    /// by `import` in declaration order (depth-first, so an imported file's own
    /// imports land before it), then `config_path`'s own values. Each layer changes
    /// only the keys it writes, so an imported theme restyles datui while anything
    /// the user writes, a default value included, still wins.
    ///
    /// A missing root file means defaults. A root file that exists but cannot be read
    /// or parsed is an error naming it: running on defaults would quietly discard every
    /// setting in it. A missing import is skipped with a warning — the file is often
    /// generated by a theme system that may not have run yet — but an import that
    /// exists and cannot be read or parsed is an error too.
    pub fn load_from_file(config_path: &Path) -> Result<Self> {
        let mut layers: Vec<ConfigLayer> = Vec::new();
        let mut imports: Vec<String> = Vec::new();

        if let Some(root) = ConfigLayer::read(config_path, None)? {
            let canonical = crate::canonical::canonicalize(config_path)
                .unwrap_or_else(|_| config_path.to_path_buf());
            let mut stack = vec![canonical];
            imports = root.imports.clone();
            Self::collect_imports(&imports, config_path, &mut stack, &mut layers)?;
            layers.push(root);
        }

        let mut config = Self::from_layers(layers)
            .map_err(|e| eyre!("Invalid configuration in {}: {}", config_path.display(), e))?;
        // `import` is a load-time directive, never merged; report what the root declared.
        config.import = imports;

        config
            .validate()
            .map_err(|e| eyre!("Invalid configuration in {}: {}", config_path.display(), e))?;

        Ok(config)
    }

    /// Append every file named by `imports` to `out`, depth-first, in order.
    ///
    /// `origin` is the file that declared them; relative paths resolve against its
    /// directory. `stack` holds the canonical paths currently being loaded, so a
    /// cycle is reported instead of followed.
    fn collect_imports(
        imports: &[String],
        origin: &Path,
        stack: &mut Vec<PathBuf>,
        out: &mut Vec<ConfigLayer>,
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

            out.push(layer);
        }

        Ok(())
    }

    /// The configuration `layers` describe, lowest precedence first, over datui's
    /// defaults. Defaults are resolved here, once: a layer holds only what it wrote.
    ///
    /// The colors start from the built-in palette for the `theme.mode` the layers
    /// declare, so a light theme's unset slots take light values. `import` is left
    /// empty; `load_from_file` follows imports and reports them. Not validated.
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

        let mut palette = match toml::Value::try_from(ColorConfig::for_mode(resolved))? {
            toml::Value::Table(palette) => palette,
            _ => unreachable!("a struct serializes to a table"),
        };
        if let Some(toml::Value::Table(colors)) = colors {
            palette.extend(colors);
        }
        config.theme.colors = toml::Value::Table(palette).try_into()?;
        config.theme.mode = Some(resolved);
        config.sync_dataset_access();
        Ok(config)
    }

    /// Every collection: the configured ones in the order defined, imports first, then
    /// the built-in catalog, unless a configured collection named `public` replaces it
    /// or `[data] builtin_catalog = false` drops it. Hidden ones included.
    pub fn collections(&self) -> Vec<SourceConfig> {
        let mut all = self.sources.clone();
        if self.data.builtin_catalog && !all.iter().any(|s| s.name == BUILTIN_CATALOG) {
            all.push(builtin_catalog());
        }
        all
    }

    /// The collections the home screen shows: [`Self::collections`] less
    /// `[data] hide_sources`.
    pub fn shown_collections(&self) -> Vec<SourceConfig> {
        self.collections()
            .into_iter()
            .filter(|s| !self.data.hide_sources.contains(&s.name))
            .collect()
    }

    /// Derive `[cloud]`'s view of how collection URLs are read. Called by
    /// `from_layers` and `default`; call it after changing `sources` or `data` by hand.
    pub fn sync_dataset_access(&mut self) {
        self.cloud.dataset_access = self
            .collections()
            .iter()
            .flat_map(|collection| {
                collection.datasets.iter().filter_map(|dataset| {
                    let url = dataset.url.as_deref()?;
                    if !is_object_store_dataset(url) {
                        return None;
                    }
                    let auth = match (dataset.connection.as_deref(), dataset.auth.as_deref()) {
                        (Some(connection), _) => DatasetAuth::Connection(connection.to_string()),
                        (None, Some("anonymous")) => DatasetAuth::Anonymous,
                        _ => DatasetAuth::Auto,
                    };
                    Some(DatasetAccess {
                        url: url.to_string(),
                        collection: collection.name.clone(),
                        auth,
                    })
                })
            })
            .collect();
    }

    /// Validate configuration values
    pub fn validate(&self) -> Result<()> {
        // Validate version compatibility
        if !self.version.starts_with("0.2") {
            return Err(eyre!(
                "Unsupported config version: {}. Expected 0.2.x",
                self.version
            ));
        }

        if let Some(n) = self.chart.row_limit
            && (n == 0 || n > MAX_CHART_ROW_LIMIT)
        {
            return Err(eyre!(
                "chart.row_limit must be between 1 and {} when set, got {}",
                MAX_CHART_ROW_LIMIT,
                n
            ));
        }

        // Resolve number formatting so bad preset names and separator clashes
        // are reported at load time rather than silently ignored at render time.
        self.display
            .number_format
            .resolve(self.display.align_numeric_right)?;

        self.cloud.validate()?;
        check_source_names(&self.sources)?;
        for source in &self.sources {
            source.validate(&self.cloud.connections)?;
        }
        if let Some(name) = self
            .data
            .hide_sources
            .iter()
            .find(|name| !is_valid_source_id(name))
        {
            return Err(eyre!(
                "data.hide_sources: \"{name}\" is not a collection name. Use the name, not \
                 the label"
            ));
        }

        // Validate all colors can be parsed
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
        if self.clipboard.osc52_limit_kb == 0 {
            return Err(eyre!("[clipboard] osc52_limit_kb must be greater than 0"));
        }

        Ok(())
    }
}

impl ColorConfig {
    /// Validate all color strings can be parsed
    fn validate(&self, parser: &ColorParser) -> Result<()> {
        // Helper macro to validate a color field (reports as theme.colors.<name> for config file context)
        macro_rules! validate_color {
            ($field:expr_2021, $name:expr_2021) => {
                parser.parse($field).map_err(|e| {
                    eyre!(
                        "theme.colors.{}: {}. Use a valid color name (e.g. red, cyan, bright_red), \
                         hex (#rrggbb), or indexed(0-255)",
                        $name,
                        e
                    )
                })?;
            };
        }

        validate_color!(&self.keybind_hints, "keybind_hints");
        validate_color!(&self.keybind_labels, "keybind_labels");
        validate_color!(&self.throbber, "throbber");
        validate_color!(
            &self.primary_chart_series_color,
            "primary_chart_series_color"
        );
        validate_color!(
            &self.secondary_chart_series_color,
            "secondary_chart_series_color"
        );
        validate_color!(&self.success, "success");
        validate_color!(&self.error, "error");
        validate_color!(&self.warning, "warning");
        validate_color!(&self.dimmed, "dimmed");
        validate_color!(&self.background, "background");
        validate_color!(&self.surface, "surface");
        validate_color!(&self.controls_bg, "controls_bg");
        validate_color!(&self.text_primary, "text_primary");
        validate_color!(&self.text_secondary, "text_secondary");
        validate_color!(&self.text_inverse, "text_inverse");
        validate_color!(&self.table_header, "table_header");
        validate_color!(&self.table_header_bg, "table_header_bg");
        validate_color!(&self.row_numbers, "row_numbers");
        validate_color!(&self.column_separator, "column_separator");
        validate_color!(&self.table_selected, "table_selected");
        validate_color!(&self.sidebar_border, "sidebar_border");
        validate_color!(&self.modal_border_active, "modal_border_active");
        validate_color!(&self.modal_border_error, "modal_border_error");
        validate_color!(&self.distribution_normal, "distribution_normal");
        validate_color!(&self.distribution_skewed, "distribution_skewed");
        validate_color!(&self.distribution_other, "distribution_other");
        validate_color!(&self.outlier_marker, "outlier_marker");
        validate_color!(&self.cursor_focused, "cursor_focused");
        validate_color!(&self.cursor_dimmed, "cursor_dimmed");
        validate_color!(&self.cursor_text, "cursor_text");
        if self.alternate_row_color != "default" {
            validate_color!(&self.alternate_row_color, "alternate_row_color");
        }
        validate_color!(&self.str_col, "str_col");
        validate_color!(&self.int_col, "int_col");
        validate_color!(&self.float_col, "float_col");
        validate_color!(&self.bool_col, "bool_col");
        validate_color!(&self.temporal_col, "temporal_col");
        validate_color!(&self.chart_series_color_1, "chart_series_color_1");
        validate_color!(&self.chart_series_color_2, "chart_series_color_2");
        validate_color!(&self.chart_series_color_3, "chart_series_color_3");
        validate_color!(&self.chart_series_color_4, "chart_series_color_4");
        validate_color!(&self.chart_series_color_5, "chart_series_color_5");
        validate_color!(&self.chart_series_color_6, "chart_series_color_6");
        validate_color!(&self.chart_series_color_7, "chart_series_color_7");
        validate_color!(&self.accent, "accent");
        validate_color!(&self.accent_bright, "accent_bright");
        validate_color!(&self.gradient_start, "gradient_start");
        validate_color!(&self.gradient_end, "gradient_end");

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

        Self {
            supports_true_color: support.as_ref().map(|s| s.has_16m).unwrap_or(false),
            supports_256: support.as_ref().map(|s| s.has_256).unwrap_or(false),
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

impl Default for ColorParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse hex color string (#ff0000) to RGB components
fn parse_hex(s: &str) -> Result<(u8, u8, u8)> {
    // `len()` counts bytes, so a seven-byte length is not seven characters and the
    // fixed offsets below are only safe once the rest is known to be ASCII. "#\u{1f600}xy"
    // is also seven bytes, and slicing it at 3 lands inside the emoji.
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

/// Convert RGB to nearest 256-color palette index
/// Uses standard xterm 256-color palette
pub fn rgb_to_256_color(r: u8, g: u8, b: u8) -> u8 {
    // The nearest entry of the xterm palette, by distance in RGB. The cube's six
    // levels are far apart (0, 95, 135, 175, 215, 255), so a dark tint like #262a3f
    // is nearer a grey on the ramp than any cube colour; rounding each channel to
    // a cube level instead sent every dark tint to the same navy or black.
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

    // Map to primary/secondary colors
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

impl Theme {
    /// Create a Theme from a ThemeConfig by parsing all color strings
    pub fn from_config(config: &ThemeConfig) -> Result<Self> {
        let parser = ColorParser::new();
        let mut colors = HashMap::new();

        // Parse all colors from config
        colors.insert(
            "keybind_hints".to_string(),
            parser.parse(&config.colors.keybind_hints)?,
        );
        colors.insert(
            "keybind_labels".to_string(),
            parser.parse(&config.colors.keybind_labels)?,
        );
        colors.insert(
            "throbber".to_string(),
            parser.parse(&config.colors.throbber)?,
        );
        colors.insert(
            "primary_chart_series_color".to_string(),
            parser.parse(&config.colors.primary_chart_series_color)?,
        );
        colors.insert(
            "secondary_chart_series_color".to_string(),
            parser.parse(&config.colors.secondary_chart_series_color)?,
        );
        colors.insert("success".to_string(), parser.parse(&config.colors.success)?);
        colors.insert("error".to_string(), parser.parse(&config.colors.error)?);
        colors.insert("warning".to_string(), parser.parse(&config.colors.warning)?);
        colors.insert("dimmed".to_string(), parser.parse(&config.colors.dimmed)?);
        colors.insert(
            "background".to_string(),
            parser.parse(&config.colors.background)?,
        );
        colors.insert("surface".to_string(), parser.parse(&config.colors.surface)?);
        colors.insert(
            "controls_bg".to_string(),
            parser.parse(&config.colors.controls_bg)?,
        );
        colors.insert(
            "text_primary".to_string(),
            parser.parse(&config.colors.text_primary)?,
        );
        colors.insert(
            "text_secondary".to_string(),
            parser.parse(&config.colors.text_secondary)?,
        );
        colors.insert(
            "text_inverse".to_string(),
            parser.parse(&config.colors.text_inverse)?,
        );
        colors.insert(
            "table_header".to_string(),
            parser.parse(&config.colors.table_header)?,
        );
        colors.insert(
            "table_header_bg".to_string(),
            parser.parse(&config.colors.table_header_bg)?,
        );
        colors.insert(
            "row_numbers".to_string(),
            parser.parse(&config.colors.row_numbers)?,
        );
        colors.insert(
            "column_separator".to_string(),
            parser.parse(&config.colors.column_separator)?,
        );
        // "reversed" keeps the old swap-fg-and-bg selection; anything else is the tint
        // painted under the row the cursor is on. Left out of the map for "reversed"
        // so a widget can ask `get_optional` and tell the two apart.
        if !config
            .colors
            .table_selected
            .trim()
            .eq_ignore_ascii_case("reversed")
        {
            colors.insert(
                "table_selected".to_string(),
                parser.parse(&config.colors.table_selected)?,
            );
        }
        let sidebar_border = parser.parse(&config.colors.sidebar_border)?;
        colors.insert("sidebar_border".to_string(), sidebar_border);
        // Every sidebar and the input strip draw their resting border from
        // `modal_border`; it is the same slot as `sidebar_border`, which is the name
        // the config documents.
        colors.insert("modal_border".to_string(), sidebar_border);
        colors.insert(
            "label".to_string(),
            parser.parse(&config.colors.text_secondary)?,
        );
        colors.insert(
            "modal_border_active".to_string(),
            parser.parse(&config.colors.modal_border_active)?,
        );
        colors.insert(
            "modal_border_error".to_string(),
            parser.parse(&config.colors.modal_border_error)?,
        );
        colors.insert(
            "distribution_normal".to_string(),
            parser.parse(&config.colors.distribution_normal)?,
        );
        colors.insert(
            "distribution_skewed".to_string(),
            parser.parse(&config.colors.distribution_skewed)?,
        );
        colors.insert(
            "distribution_other".to_string(),
            parser.parse(&config.colors.distribution_other)?,
        );
        colors.insert(
            "outlier_marker".to_string(),
            parser.parse(&config.colors.outlier_marker)?,
        );
        colors.insert(
            "cursor_focused".to_string(),
            parser.parse(&config.colors.cursor_focused)?,
        );
        colors.insert(
            "cursor_dimmed".to_string(),
            parser.parse(&config.colors.cursor_dimmed)?,
        );
        colors.insert(
            "cursor_text".to_string(),
            parser.parse(&config.colors.cursor_text)?,
        );
        if config.colors.alternate_row_color != "default" {
            colors.insert(
                "alternate_row_color".to_string(),
                parser.parse(&config.colors.alternate_row_color)?,
            );
        }
        colors.insert("str_col".to_string(), parser.parse(&config.colors.str_col)?);
        colors.insert("int_col".to_string(), parser.parse(&config.colors.int_col)?);
        colors.insert(
            "float_col".to_string(),
            parser.parse(&config.colors.float_col)?,
        );
        colors.insert(
            "bool_col".to_string(),
            parser.parse(&config.colors.bool_col)?,
        );
        colors.insert(
            "temporal_col".to_string(),
            parser.parse(&config.colors.temporal_col)?,
        );
        colors.insert(
            "binary_col".to_string(),
            parser.parse(&config.colors.binary_col)?,
        );
        colors.insert(
            "chart_series_color_1".to_string(),
            parser.parse(&config.colors.chart_series_color_1)?,
        );
        colors.insert(
            "chart_series_color_2".to_string(),
            parser.parse(&config.colors.chart_series_color_2)?,
        );
        colors.insert(
            "chart_series_color_3".to_string(),
            parser.parse(&config.colors.chart_series_color_3)?,
        );
        colors.insert(
            "chart_series_color_4".to_string(),
            parser.parse(&config.colors.chart_series_color_4)?,
        );
        colors.insert(
            "chart_series_color_5".to_string(),
            parser.parse(&config.colors.chart_series_color_5)?,
        );
        colors.insert(
            "chart_series_color_6".to_string(),
            parser.parse(&config.colors.chart_series_color_6)?,
        );
        colors.insert(
            "chart_series_color_7".to_string(),
            parser.parse(&config.colors.chart_series_color_7)?,
        );
        colors.insert("accent".to_string(), parser.parse(&config.colors.accent)?);
        colors.insert(
            "accent_bright".to_string(),
            parser.parse(&config.colors.accent_bright)?,
        );
        colors.insert(
            "gradient_start".to_string(),
            parser.parse(&config.colors.gradient_start)?,
        );
        colors.insert(
            "gradient_end".to_string(),
            parser.parse(&config.colors.gradient_end)?,
        );

        Ok(Self { colors })
    }

    /// Get a color by name, returns Reset if not found
    pub fn get(&self, name: &str) -> Color {
        self.colors.get(name).copied().unwrap_or(Color::Reset)
    }

    /// Get a color by name, returns None if not found
    pub fn get_optional(&self, name: &str) -> Option<Color> {
        self.colors.get(name).copied()
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

    /// Style of selected text in a field: the highlight tint, or reversed video
    /// where the tint could match the terminal's own background. A 16-color
    /// terminal turns the default tints into black or white and `NO_COLOR` into
    /// none, and a field has no rail to show the selection instead.
    pub fn text_selection_style(&self) -> ratatui::style::Style {
        match self.get_optional("table_selected") {
            Some(Color::Reset | Color::Black | Color::White) => {
                ratatui::style::Style::default().add_modifier(ratatui::style::Modifier::REVERSED)
            }
            _ => self.highlight_style(),
        }
    }

    /// Text color for the solid cursor block: the `cursor_text` slot, or black or
    /// white by the cursor color's luminance when the slot says "default". Lives
    /// here so widgets never pick colors themselves.
    pub fn cursor_text_for(&self, cursor: Color) -> Color {
        match self.get("cursor_text") {
            Color::Reset => contrasting_text(cursor),
            configured => configured,
        }
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

/// A representative RGB for any terminal color, for luminance arithmetic. The
/// named colors use the xterm defaults; the real palette is the terminal's, so
/// this is an estimate — good enough to pick black or white.
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

const DATA_SEARCH_COMMENTS: &[(&str, &str)] = &[
    (
        "enabled",
        "Search below the working directory when you type on the home screen.\n\
         The walk runs once, in the background, the first time you type; every\n\
         keystroke after that filters the result in memory. Set false to list only\n\
         the directories themselves.",
    ),
    (
        "max_depth",
        "How deep to descend. Data is rarely twelve directories down, and every\n\
         extra level costs a listing on every branch.",
    ),
    (
        "max_results",
        "Stop after this many datasets. The list is a way to find something, not an\n\
         inventory. Hitting the limit is reported on screen, never silent.",
    ),
    (
        "time_budget_ms",
        "Give up walking after this long and keep whatever was found. A cold or\n\
         enormous tree must degrade to partial results, never to a wait.",
    ),
    (
        "cross_filesystems",
        "Descend into directories on a different filesystem than the one you started\n\
         in. Off by default, and the most important limit here: it is what keeps a\n\
         search from wandering onto a network share, and on autofs, from MOUNTING one\n\
         merely by looking at it. Turn it on only if your data lives on a mount\n\
         beneath your working directory and you know that mount is fast.",
    ),
    (
        "follow_gitignore",
        "Obey .gitignore. Off by default, and deliberately: people gitignore data\n\
         directories precisely because the data is too big to commit, which is the\n\
         same reason they want to open it in datui. In datui's own repository,\n\
         honouring it hides 38 real test datasets while hiding 69 files of virtualenv\n\
         noise -- wrong in both directions. Use skip/skip_extra for the noise.",
    ),
    (
        "skip",
        "Directory names never descended into. Setting this REPLACES the defaults:\n\
         node_modules, target, build, dist, vendor, site-packages, __pycache__,\n\
         venv, env. Hidden directories (.git, .venv, the caches) are always skipped.\n\
         To add to the defaults rather than replace them, use skip_extra.",
    ),
    (
        "skip_extra",
        "Directory names to skip in addition to the defaults, so adding one does not\n\
         mean restating the whole list. Example: skip_extra = [\"archive\", \"raw\"]",
    ),
    (
        "extensions",
        "File extensions to search for. Empty (default) means every format datui can\n\
         open -- which includes json and txt, noisy in a source tree. Narrow it if\n\
         that bothers you. Example: extensions = [\"parquet\", \"csv\"]",
    ),
];

/// Net change in unclosed brackets across one line of TOML.
///
/// Enough to tell whether a rendered array is still open at the end of the line.
/// Brackets inside strings would fool it, and none of the values here contain any.
fn bracket_depth(line: &str) -> i32 {
    line.chars().fold(0, |acc, c| match c {
        '[' => acc + 1,
        ']' => acc - 1,
        _ => acc,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_config_that_still_sets_a_layout_key_is_told_so() {
        let found = |text: &str| removed_file_loading_keys(&toml::from_str(text).unwrap());
        assert_eq!(
            found("[file_loading]\nskip_rows = 2\nhas_header = false\nparse_dates = true\n"),
            [("has_header", "--no-header"), ("skip_rows", "--skip-rows")]
        );
        assert!(found("[display]\nskip_rows = 2\n").is_empty());
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
        for (path, example) in UNSET_EXAMPLES {
            let value: toml::Table = toml::from_str(&format!("v = {example}")).unwrap();
            out.push((path.to_string(), value["v"].clone()));
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

    #[test]
    fn every_setting_is_documented_in_the_generated_config() {
        // A new option needs a comment and a line in the generated config, whether its
        // default serializes or it is unset and so needs an `UNSET_EXAMPLES` entry.
        let comments = ConfigManager::collect_all_comments();
        let generated = ConfigManager::with_dir(PathBuf::new()).generate_default_config();
        let mut shown = std::collections::HashSet::new();
        let mut section = String::new();
        for line in generated.lines() {
            let Some(line) = line.strip_prefix("# ") else {
                continue;
            };
            if let Some(name) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
                section = name.to_string();
            } else if let Some((key, _)) = line.split_once(" = ")
                && !key.contains(' ')
            {
                shown.insert(match section.as_str() {
                    "" => key.to_string(),
                    s => format!("{s}.{key}"),
                });
            }
        }
        let settings = leaf_settings();
        for (path, _) in &settings {
            assert!(comments.contains_key(path), "{path} has no comment");
            assert!(
                shown.contains(path),
                "{path} is not in the generated config"
            );
        }
        for path in comments.keys() {
            assert!(
                settings.iter().any(|(p, _)| p == path),
                "{path} is commented but is no setting"
            );
        }
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
            let blank_is_unset = path
                .strip_prefix("cloud.")
                .is_some_and(|key| CLOUD_BLANK_IS_UNSET.contains(&key));
            if path == "import" || blank_is_unset || COMBINED_KEYS.iter().any(|(p, _)| p == path) {
                continue;
            }
            let other = match value {
                toml::Value::Boolean(b) => toml::Value::Boolean(!b),
                toml::Value::Integer(n) => toml::Value::Integer(n + 1),
                toml::Value::Array(items) if items.is_empty() => vec!["x"].into(),
                toml::Value::Array(_) => toml::Value::Array(Vec::new()),
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
