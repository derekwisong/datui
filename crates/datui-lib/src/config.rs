use color_eyre::eyre::eyre;
use color_eyre::Result;
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

    /// Create a new ConfigManager for the given app name
    pub fn new(app_name: &str) -> Result<Self> {
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

    /// Generate default configuration template as a string with comments
    /// All fields are commented out so defaults are used, but users can uncomment to override
    pub fn generate_default_config(&self) -> String {
        // Serialize default config to TOML
        let config = AppConfig::default();
        let toml_str = toml::to_string_pretty(&config)
            .unwrap_or_else(|e| panic!("Failed to serialize default config: {}", e));

        // Build comment map from all struct comment constants
        let comments = Self::collect_all_comments();

        // Comment out all fields and add comments
        Self::comment_all_fields(toml_str, comments)
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

        // Controls fields
        for (field, comment) in CONTROLS_COMMENTS {
            comments.insert(format!("ui.controls.{}", field), comment.to_string());
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

    /// Comment out all fields in TOML and add comments
    /// Also adds missing Option fields as commented-out `# field = null`
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
                        result.push_str("# ");
                        result.push_str(comment_line);
                        result.push('\n');
                    }
                }

                // Comment out the field line
                result.push_str("# ");
                result.push_str(line);
                result.push('\n');
            } else {
                // Empty line or other content - preserve as-is
                result.push_str(line);
                result.push('\n');
            }

            i += 1;
        }

        // Second pass: add missing Option fields (those with comments but not in TOML)
        result = Self::add_missing_option_fields(result, &comments, &seen_fields);

        result
    }

    /// Add missing Option fields that weren't serialized (because they're None)
    fn add_missing_option_fields(
        mut result: String,
        comments: &std::collections::HashMap<String, String>,
        seen_fields: &std::collections::HashSet<String>,
    ) -> String {
        // Option fields that should appear even when None
        let option_fields = [
            "cloud.s3_endpoint_url",
            "cloud.s3_access_key_id",
            "cloud.s3_secret_access_key",
            "cloud.s3_region",
            "file_loading.delimiter",
            "file_loading.has_header",
            "file_loading.skip_lines",
            "file_loading.skip_rows",
            "file_loading.single_spine_schema",
            "chart.row_limit",
            "ui.controls.custom_controls",
        ];

        // Group missing fields by section
        let mut missing_by_section: std::collections::HashMap<String, Vec<&str>> =
            std::collections::HashMap::new();

        for field_path in &option_fields {
            if !seen_fields.contains(*field_path) && comments.contains_key(*field_path) {
                if let Some(dot_pos) = field_path.find('.') {
                    let section = &field_path[..dot_pos];
                    missing_by_section
                        .entry(section.to_string())
                        .or_default()
                        .push(field_path);
                }
            }
        }

        // Insert missing fields into appropriate sections
        for (section, fields) in &missing_by_section {
            let section_header = format!("[{}]", section);
            if let Some(section_pos) = result.find(&section_header) {
                // Find the newline after the section header
                let after_header_start = section_pos + section_header.len();
                let after_header = &result[after_header_start..];

                // Find the first newline after the section header
                let newline_pos = after_header.find('\n').unwrap_or(0);
                let insert_pos = after_header_start + newline_pos + 1;

                // Build content to insert
                let mut new_content = String::new();
                for field_path in fields {
                    if let Some(comment) = comments.get(*field_path) {
                        for comment_line in comment.lines() {
                            new_content.push_str("# ");
                            new_content.push_str(comment_line);
                            new_content.push('\n');
                        }
                    }
                    let field_name = field_path.rsplit('.').next().unwrap_or(field_path);
                    new_content.push_str(&format!("# {} = null\n", field_name));
                    new_content.push('\n');
                }

                result.insert_str(insert_pos, &new_content);
            }
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

        // Extract field name from line (e.g., "sampling_threshold = 10000")
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
        std::fs::write(&config_path, template)?;

        Ok(config_path)
    }
}

/// Complete application configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    /// Configuration format version (for future compatibility)
    pub version: String,
    pub cloud: CloudConfig,
    pub file_loading: FileLoadingConfig,
    pub display: DisplayConfig,
    pub performance: PerformanceConfig,
    pub chart: ChartConfig,
    pub theme: ThemeConfig,
    pub ui: UiConfig,
    pub query: QueryConfig,
    pub templates: TemplateConfig,
    pub debug: DebugConfig,
}

// Field comments for AppConfig (top-level fields)
const APP_COMMENTS: &[(&str, &str)] = &[(
    "version",
    "Configuration format version (for future compatibility)",
)];

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
        "ui",
        "# ============================================================================\n# UI Layout\n# ============================================================================",
    ),
    ("ui.controls", "# Control bar settings"),
    (
        "query",
        "# ============================================================================\n# Query System\n# ============================================================================",
    ),
    (
        "templates",
        "# ============================================================================\n# Template Settings\n# ============================================================================",
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
}

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
        "Secret key when using custom endpoint (or set AWS_SECRET_ACCESS_KEY).",
    ),
    (
        "s3_region",
        "Region (e.g. us-east-1). Required for custom endpoints; MinIO often uses us-east-1.",
    ),
];

impl CloudConfig {
    pub fn merge(&mut self, other: Self) {
        if other.s3_endpoint_url.is_some() {
            self.s3_endpoint_url = other.s3_endpoint_url;
        }
        if other.s3_access_key_id.is_some() {
            self.s3_access_key_id = other.s3_access_key_id;
        }
        if other.s3_secret_access_key.is_some() {
            self.s3_secret_access_key = other.s3_secret_access_key;
        }
        if other.s3_region.is_some() {
            self.s3_region = other.s3_region;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct FileLoadingConfig {
    pub delimiter: Option<u8>,
    pub has_header: Option<bool>,
    pub skip_lines: Option<usize>,
    pub skip_rows: Option<usize>,
    /// When true, CSV reader tries to parse string columns as dates (YYYY-MM-DD, ISO datetime). Default: true.
    pub parse_dates: Option<bool>,
    /// When true, decompress compressed CSV into memory (eager read). When false (default), decompress to a temp file and use lazy scan.
    pub decompress_in_memory: Option<bool>,
    /// Directory for decompression temp files. null = system default (e.g. TMPDIR).
    pub temp_dir: Option<String>,
    /// When true (default), infer Hive/partitioned Parquet schema from one file (single-spine) for faster "Caching schema". When false, use Polars collect_schema() over all files.
    pub single_spine_schema: Option<bool>,
}

// Field comments for FileLoadingConfig
// Format: (field_name, comment_text)
const FILE_LOADING_COMMENTS: &[(&str, &str)] = &[
    (
        "delimiter",
        "Default delimiter for CSV files (as ASCII value, e.g., 44 for comma)\nIf not specified, auto-detection is used",
    ),
    (
        "has_header",
        "Whether files have headers by default\nnull = auto-detect, true = has header, false = no header",
    ),
    ("skip_lines", "Number of lines to skip at the start of files"),
    ("skip_rows", "Number of rows to skip when reading files"),
    (
        "parse_dates",
        "When true (default), CSV reader tries to parse string columns as dates (e.g. YYYY-MM-DD, ISO datetime)",
    ),
    (
        "decompress_in_memory",
        "When true, decompress compressed CSV into memory (eager). When false (default), decompress to a temp file and use lazy scan",
    ),
    (
        "temp_dir",
        "Directory for decompression temp files. null = system default (e.g. TMPDIR)",
    ),
    (
        "single_spine_schema",
        "When true (default), infer Hive/partitioned Parquet schema from one file for faster load. When false, use full schema scan (Polars collect_schema).",
    ),
];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DisplayConfig {
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
    ("row_numbers", "Display row numbers on the left side of the table"),
    ("row_start_index", "Starting index for row numbers (0 or 1)"),
    (
        "table_cell_padding",
        "Number of spaces between columns in the main data table (>= 0)\nDefault 2",
    ),
    (
        "column_colors",
        "Colorize main table cells by column type (string, int, float, bool, date/datetime)\nSet to false to use default text color for all cells",
    ),
];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PerformanceConfig {
    /// When None, analysis uses full dataset (no sampling). When Some(n), datasets with >= n rows are sampled.
    pub sampling_threshold: Option<usize>,
    pub event_poll_interval_ms: u64,
    /// When true (default), use Polars streaming engine for LazyFrame collect when the streaming feature is enabled (lower memory, batch processing).
    pub polars_streaming: bool,
}

// Field comments for PerformanceConfig
const PERFORMANCE_COMMENTS: &[(&str, &str)] = &[
    (
        "sampling_threshold",
        "Optional: when set, datasets with >= this many rows are sampled for analysis (faster, less memory).\nWhen unset or omitted, full dataset is used. Example: sampling_threshold = 10000",
    ),
    (
        "event_poll_interval_ms",
        "Event polling interval in milliseconds\nLower values = more responsive but higher CPU usage",
    ),
    (
        "polars_streaming",
        "Use Polars streaming engine for LazyFrame collect when available (default: true). Reduces memory and can improve performance on large or partitioned data.",
    ),
];

/// Default maximum rows used for chart data when not overridden by config or UI.
pub const DEFAULT_CHART_ROW_LIMIT: usize = 10_000;
/// Maximum chart row limit (Polars slice takes u32).
pub const MAX_CHART_ROW_LIMIT: usize = u32::MAX as usize;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ChartConfig {
    /// Maximum rows for chart data. None (null in TOML) = unlimited; Some(n) = cap at n. Default 10000.
    pub row_limit: Option<usize>,
}

// Field comments for ChartConfig
const CHART_COMMENTS: &[(&str, &str)] = &[
    (
        "row_limit",
        "Maximum rows used when building charts (display and export).\nSet to null for unlimited (uses full dataset). Set to a number (e.g. 10000) to cap. Can also be changed in chart view (Limit Rows). Example: row_limit = 10000",
    ),
];

impl Default for ChartConfig {
    fn default() -> Self {
        Self {
            row_limit: Some(DEFAULT_CHART_ROW_LIMIT),
        }
    }
}

impl ChartConfig {
    pub fn merge(&mut self, other: Self) {
        if other.row_limit.is_some() {
            self.row_limit = other.row_limit;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ThemeConfig {
    pub colors: ColorConfig,
}

// Field comments for ThemeConfig
const THEME_COMMENTS: &[(&str, &str)] = &[];

fn default_row_numbers_color() -> String {
    "dark_gray".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// "default" = no alternate row color; any other value is parsed as a color (e.g. "dark_gray")
    pub alternate_row_color: String,
    /// Column type colors (main data table): string, integer, float, boolean, temporal
    pub str_col: String,
    pub int_col: String,
    pub float_col: String,
    pub bool_col: String,
    pub temporal_col: String,
    /// Chart view: series colors 1–7 (line/scatter/bar series)
    pub chart_series_color_1: String,
    pub chart_series_color_2: String,
    pub chart_series_color_3: String,
    pub chart_series_color_4: String,
    pub chart_series_color_5: String,
    pub chart_series_color_6: String,
    pub chart_series_color_7: String,
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
    ("row_numbers", "Row numbers column text; use \"default\" for terminal default"),
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
        "Cursor color when text input is focused\nText under cursor uses reverse of this color",
    ),
    (
        "cursor_dimmed",
        "Cursor color when text input is unfocused (currently unused - unfocused inputs hide cursor)",
    ),
    (
        "alternate_row_color",
        "Background color for every other row in the main data table\nSet to \"default\" to disable alternate row coloring",
    ),
    ("str_col", "Main table: string column text color"),
    ("int_col", "Main table: integer column text color"),
    ("float_col", "Main table: float column text color"),
    ("bool_col", "Main table: boolean column text color"),
    ("temporal_col", "Main table: date/datetime/time column text color"),
    ("chart_series_color_1", "Chart view: first series color"),
    ("chart_series_color_2", "Chart view: second series color"),
    ("chart_series_color_3", "Chart view: third series color"),
    ("chart_series_color_4", "Chart view: fourth series color"),
    ("chart_series_color_5", "Chart view: fifth series color"),
    ("chart_series_color_6", "Chart view: sixth series color"),
    ("chart_series_color_7", "Chart view: seventh series color"),
];

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct UiConfig {
    pub controls: ControlsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ControlsConfig {
    pub custom_controls: Option<Vec<(String, String)>>,
    pub row_count_width: usize,
}

// Field comments for ControlsConfig
const CONTROLS_COMMENTS: &[(&str, &str)] = &[
    (
        "custom_controls",
        "Custom control keybindings (optional)\nFormat: [[\"key\", \"label\"], [\"key\", \"label\"], ...]\nIf not specified, uses default controls",
    ),
    ("row_count_width", "Row count display width in characters"),
];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct QueryConfig {
    pub history_limit: usize,
    pub enable_history: bool,
}

// Field comments for QueryConfig
const QUERY_COMMENTS: &[(&str, &str)] = &[
    (
        "history_limit",
        "Maximum number of queries to keep in history",
    ),
    ("enable_history", "Enable query history caching"),
];

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TemplateConfig {
    pub auto_apply: bool,
}

// Field comments for TemplateConfig
const TEMPLATE_COMMENTS: &[(&str, &str)] = &[(
    "auto_apply",
    "Auto-apply most relevant template on file open",
)];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DebugConfig {
    pub enabled: bool,
    pub show_performance: bool,
    pub show_query: bool,
    pub show_transformations: bool,
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
];

// Default implementations
impl Default for AppConfig {
    fn default() -> Self {
        Self {
            version: "0.2".to_string(),
            cloud: CloudConfig::default(),
            file_loading: FileLoadingConfig::default(),
            display: DisplayConfig::default(),
            performance: PerformanceConfig::default(),
            chart: ChartConfig::default(),
            theme: ThemeConfig::default(),
            ui: UiConfig::default(),
            query: QueryConfig::default(),
            templates: TemplateConfig::default(),
            debug: DebugConfig::default(),
        }
    }
}

impl Default for DisplayConfig {
    fn default() -> Self {
        Self {
            pages_lookahead: 3,
            pages_lookback: 3,
            max_buffered_rows: 100_000,
            max_buffered_mb: 512,
            row_numbers: false,
            row_start_index: 1,
            table_cell_padding: 2,
            column_colors: true,
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            sampling_threshold: None,
            event_poll_interval_ms: 25,
            polars_streaming: true,
        }
    }
}

impl Default for ColorConfig {
    fn default() -> Self {
        Self {
            keybind_hints: "cyan".to_string(),
            keybind_labels: "indexed(252)".to_string(),
            throbber: "cyan".to_string(),
            primary_chart_series_color: "cyan".to_string(),
            secondary_chart_series_color: "indexed(245)".to_string(),
            success: "green".to_string(),
            error: "red".to_string(),
            warning: "yellow".to_string(),
            dimmed: "dark_gray".to_string(),
            background: "default".to_string(),
            surface: "default".to_string(),
            controls_bg: "indexed(235)".to_string(),
            text_primary: "default".to_string(),
            text_secondary: "indexed(240)".to_string(),
            text_inverse: "black".to_string(),
            table_header: "white".to_string(),
            table_header_bg: "indexed(235)".to_string(),
            row_numbers: "dark_gray".to_string(),
            column_separator: "cyan".to_string(),
            table_selected: "reversed".to_string(),
            sidebar_border: "indexed(235)".to_string(),
            modal_border_active: "yellow".to_string(),
            modal_border_error: "red".to_string(),
            distribution_normal: "green".to_string(),
            distribution_skewed: "yellow".to_string(),
            distribution_other: "white".to_string(),
            outlier_marker: "red".to_string(),
            cursor_focused: "default".to_string(),
            cursor_dimmed: "default".to_string(),
            alternate_row_color: "indexed(235)".to_string(),
            str_col: "green".to_string(),
            int_col: "cyan".to_string(),
            float_col: "blue".to_string(),
            bool_col: "yellow".to_string(),
            temporal_col: "magenta".to_string(),
            chart_series_color_1: "cyan".to_string(),
            chart_series_color_2: "magenta".to_string(),
            chart_series_color_3: "green".to_string(),
            chart_series_color_4: "yellow".to_string(),
            chart_series_color_5: "blue".to_string(),
            chart_series_color_6: "red".to_string(),
            chart_series_color_7: "bright_cyan".to_string(),
        }
    }
}

impl Default for ControlsConfig {
    fn default() -> Self {
        Self {
            custom_controls: None,
            row_count_width: 20,
        }
    }
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            history_limit: 1000,
            enable_history: true,
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
        }
    }
}

// Configuration loading and merging
impl AppConfig {
    /// Load configuration from all layers (default → user)
    pub fn load(app_name: &str) -> Result<Self> {
        let mut config = AppConfig::default();

        // Try to load user config (if exists)
        let config_path = ConfigManager::new(app_name)
            .ok()
            .map(|m| m.config_path("config.toml"));
        if let Ok(user_config) = Self::load_user_config(app_name) {
            config.merge(user_config);
        }

        // Validate configuration (e.g. color names); report config file path on error
        config.validate().map_err(|e| {
            let path_hint = config_path
                .as_ref()
                .map(|p| format!(" in {}", p.display()))
                .unwrap_or_default();
            eyre!("Invalid configuration{}: {}", path_hint, e)
        })?;

        Ok(config)
    }

    /// Load user configuration from ~/.config/datui/config.toml
    fn load_user_config(app_name: &str) -> Result<AppConfig> {
        let config_manager = ConfigManager::new(app_name)?;
        let config_path = config_manager.config_path("config.toml");

        if !config_path.exists() {
            return Ok(AppConfig::default());
        }

        let content = std::fs::read_to_string(&config_path).map_err(|e| {
            eyre!(
                "Failed to read config file at {}: {}",
                config_path.display(),
                e
            )
        })?;

        toml::from_str(&content).map_err(|e| {
            eyre!(
                "Failed to parse config file at {}: {}",
                config_path.display(),
                e
            )
        })
    }

    /// Merge another config into this one (other takes precedence)
    pub fn merge(&mut self, other: AppConfig) {
        // Version: take other's version if present and different from default
        if other.version != AppConfig::default().version {
            self.version = other.version;
        }

        // Merge each section
        self.cloud.merge(other.cloud);
        self.file_loading.merge(other.file_loading);
        self.display.merge(other.display);
        self.performance.merge(other.performance);
        self.chart.merge(other.chart);
        self.theme.merge(other.theme);
        self.ui.merge(other.ui);
        self.query.merge(other.query);
        self.templates.merge(other.templates);
        self.debug.merge(other.debug);
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

        // Validate performance settings
        if let Some(t) = self.performance.sampling_threshold {
            if t == 0 {
                return Err(eyre!("sampling_threshold must be greater than 0 when set"));
            }
        }

        if self.performance.event_poll_interval_ms == 0 {
            return Err(eyre!("event_poll_interval_ms must be greater than 0"));
        }

        if let Some(n) = self.chart.row_limit {
            if n == 0 || n > MAX_CHART_ROW_LIMIT {
                return Err(eyre!(
                    "chart.row_limit must be between 1 and {} when set, got {}",
                    MAX_CHART_ROW_LIMIT,
                    n
                ));
            }
        }

        // Validate all colors can be parsed
        let parser = ColorParser::new();
        self.theme.colors.validate(&parser)?;

        Ok(())
    }
}

// Merge implementations for each config section
impl FileLoadingConfig {
    pub fn merge(&mut self, other: Self) {
        if other.delimiter.is_some() {
            self.delimiter = other.delimiter;
        }
        if other.has_header.is_some() {
            self.has_header = other.has_header;
        }
        if other.skip_lines.is_some() {
            self.skip_lines = other.skip_lines;
        }
        if other.skip_rows.is_some() {
            self.skip_rows = other.skip_rows;
        }
        if other.parse_dates.is_some() {
            self.parse_dates = other.parse_dates;
        }
        if other.decompress_in_memory.is_some() {
            self.decompress_in_memory = other.decompress_in_memory;
        }
        if other.temp_dir.is_some() {
            self.temp_dir = other.temp_dir.clone();
        }
        if other.single_spine_schema.is_some() {
            self.single_spine_schema = other.single_spine_schema;
        }
    }
}

impl DisplayConfig {
    pub fn merge(&mut self, other: Self) {
        let default = DisplayConfig::default();
        if other.pages_lookahead != default.pages_lookahead {
            self.pages_lookahead = other.pages_lookahead;
        }
        if other.pages_lookback != default.pages_lookback {
            self.pages_lookback = other.pages_lookback;
        }
        if other.max_buffered_rows != default.max_buffered_rows {
            self.max_buffered_rows = other.max_buffered_rows;
        }
        if other.max_buffered_mb != default.max_buffered_mb {
            self.max_buffered_mb = other.max_buffered_mb;
        }
        if other.row_numbers != default.row_numbers {
            self.row_numbers = other.row_numbers;
        }
        if other.row_start_index != default.row_start_index {
            self.row_start_index = other.row_start_index;
        }
        if other.table_cell_padding != default.table_cell_padding {
            self.table_cell_padding = other.table_cell_padding;
        }
        if other.column_colors != default.column_colors {
            self.column_colors = other.column_colors;
        }
    }
}

impl PerformanceConfig {
    pub fn merge(&mut self, other: Self) {
        let default = PerformanceConfig::default();
        if other.sampling_threshold != default.sampling_threshold {
            self.sampling_threshold = other.sampling_threshold;
        }
        if other.event_poll_interval_ms != default.event_poll_interval_ms {
            self.event_poll_interval_ms = other.event_poll_interval_ms;
        }
        if other.polars_streaming != default.polars_streaming {
            self.polars_streaming = other.polars_streaming;
        }
    }
}

impl ThemeConfig {
    pub fn merge(&mut self, other: Self) {
        self.colors.merge(other.colors);
    }
}

impl ColorConfig {
    /// Validate all color strings can be parsed
    fn validate(&self, parser: &ColorParser) -> Result<()> {
        // Helper macro to validate a color field (reports as theme.colors.<name> for config file context)
        macro_rules! validate_color {
            ($field:expr, $name:expr) => {
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

        Ok(())
    }

    pub fn merge(&mut self, other: Self) {
        let default = ColorConfig::default();

        // Macro would be nice here, but keeping it explicit for clarity
        if other.keybind_hints != default.keybind_hints {
            self.keybind_hints = other.keybind_hints;
        }
        if other.keybind_labels != default.keybind_labels {
            self.keybind_labels = other.keybind_labels;
        }
        if other.throbber != default.throbber {
            self.throbber = other.throbber;
        }
        if other.primary_chart_series_color != default.primary_chart_series_color {
            self.primary_chart_series_color = other.primary_chart_series_color;
        }
        if other.secondary_chart_series_color != default.secondary_chart_series_color {
            self.secondary_chart_series_color = other.secondary_chart_series_color;
        }
        if other.success != default.success {
            self.success = other.success;
        }
        if other.error != default.error {
            self.error = other.error;
        }
        if other.warning != default.warning {
            self.warning = other.warning;
        }
        if other.dimmed != default.dimmed {
            self.dimmed = other.dimmed;
        }
        if other.background != default.background {
            self.background = other.background;
        }
        if other.surface != default.surface {
            self.surface = other.surface;
        }
        if other.controls_bg != default.controls_bg {
            self.controls_bg = other.controls_bg;
        }
        if other.text_primary != default.text_primary {
            self.text_primary = other.text_primary;
        }
        if other.text_secondary != default.text_secondary {
            self.text_secondary = other.text_secondary;
        }
        if other.text_inverse != default.text_inverse {
            self.text_inverse = other.text_inverse;
        }
        if other.table_header != default.table_header {
            self.table_header = other.table_header;
        }
        if other.table_header_bg != default.table_header_bg {
            self.table_header_bg = other.table_header_bg;
        }
        if other.row_numbers != default.row_numbers {
            self.row_numbers = other.row_numbers;
        }
        if other.column_separator != default.column_separator {
            self.column_separator = other.column_separator;
        }
        if other.table_selected != default.table_selected {
            self.table_selected = other.table_selected;
        }
        if other.sidebar_border != default.sidebar_border {
            self.sidebar_border = other.sidebar_border;
        }
        if other.modal_border_active != default.modal_border_active {
            self.modal_border_active = other.modal_border_active;
        }
        if other.modal_border_error != default.modal_border_error {
            self.modal_border_error = other.modal_border_error;
        }
        if other.distribution_normal != default.distribution_normal {
            self.distribution_normal = other.distribution_normal;
        }
        if other.distribution_skewed != default.distribution_skewed {
            self.distribution_skewed = other.distribution_skewed;
        }
        if other.distribution_other != default.distribution_other {
            self.distribution_other = other.distribution_other;
        }
        if other.outlier_marker != default.outlier_marker {
            self.outlier_marker = other.outlier_marker;
        }
        if other.cursor_focused != default.cursor_focused {
            self.cursor_focused = other.cursor_focused;
        }
        if other.cursor_dimmed != default.cursor_dimmed {
            self.cursor_dimmed = other.cursor_dimmed;
        }
        if other.alternate_row_color != default.alternate_row_color {
            self.alternate_row_color = other.alternate_row_color;
        }
        if other.str_col != default.str_col {
            self.str_col = other.str_col;
        }
        if other.int_col != default.int_col {
            self.int_col = other.int_col;
        }
        if other.float_col != default.float_col {
            self.float_col = other.float_col;
        }
        if other.bool_col != default.bool_col {
            self.bool_col = other.bool_col;
        }
        if other.temporal_col != default.temporal_col {
            self.temporal_col = other.temporal_col;
        }
        if other.chart_series_color_1 != default.chart_series_color_1 {
            self.chart_series_color_1 = other.chart_series_color_1;
        }
        if other.chart_series_color_2 != default.chart_series_color_2 {
            self.chart_series_color_2 = other.chart_series_color_2;
        }
        if other.chart_series_color_3 != default.chart_series_color_3 {
            self.chart_series_color_3 = other.chart_series_color_3;
        }
        if other.chart_series_color_4 != default.chart_series_color_4 {
            self.chart_series_color_4 = other.chart_series_color_4;
        }
        if other.chart_series_color_5 != default.chart_series_color_5 {
            self.chart_series_color_5 = other.chart_series_color_5;
        }
        if other.chart_series_color_6 != default.chart_series_color_6 {
            self.chart_series_color_6 = other.chart_series_color_6;
        }
        if other.chart_series_color_7 != default.chart_series_color_7 {
            self.chart_series_color_7 = other.chart_series_color_7;
        }
    }
}

impl UiConfig {
    pub fn merge(&mut self, other: Self) {
        self.controls.merge(other.controls);
    }
}

impl ControlsConfig {
    pub fn merge(&mut self, other: Self) {
        if other.custom_controls.is_some() {
            self.custom_controls = other.custom_controls;
        }
        let default = ControlsConfig::default();
        if other.row_count_width != default.row_count_width {
            self.row_count_width = other.row_count_width;
        }
    }
}

impl QueryConfig {
    pub fn merge(&mut self, other: Self) {
        let default = QueryConfig::default();
        if other.history_limit != default.history_limit {
            self.history_limit = other.history_limit;
        }
        if other.enable_history != default.enable_history {
            self.enable_history = other.enable_history;
        }
    }
}

impl TemplateConfig {
    pub fn merge(&mut self, other: Self) {
        let default = TemplateConfig::default();
        if other.auto_apply != default.auto_apply {
            self.auto_apply = other.auto_apply;
        }
    }
}

impl DebugConfig {
    pub fn merge(&mut self, other: Self) {
        let default = DebugConfig::default();
        if other.enabled != default.enabled {
            self.enabled = other.enabled;
        }
        if other.show_performance != default.show_performance {
            self.show_performance = other.show_performance;
        }
        if other.show_query != default.show_query {
            self.show_query = other.show_query;
        }
        if other.show_transformations != default.show_transformations {
            self.show_transformations = other.show_transformations;
        }
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
    if !s.starts_with('#') || s.len() != 7 {
        return Err(eyre!(
            "Invalid hex color format: '{}'. Expected format: #rrggbb",
            s
        ));
    }

    let r = u8::from_str_radix(&s[1..3], 16)
        .map_err(|_| eyre!("Invalid red component in hex color: {}", s))?;
    let g = u8::from_str_radix(&s[3..5], 16)
        .map_err(|_| eyre!("Invalid green component in hex color: {}", s))?;
    let b = u8::from_str_radix(&s[5..7], 16)
        .map_err(|_| eyre!("Invalid blue component in hex color: {}", s))?;

    Ok((r, g, b))
}

/// Convert RGB to nearest 256-color palette index
/// Uses standard xterm 256-color palette
pub fn rgb_to_256_color(r: u8, g: u8, b: u8) -> u8 {
    // Check if it's a gray shade (r ≈ g ≈ b)
    let max_diff = r.max(g).max(b) as i16 - r.min(g).min(b) as i16;
    if max_diff < 10 {
        // Map to grayscale ramp (232-255)
        let gray = (r as u16 + g as u16 + b as u16) / 3;
        if gray < 8 {
            return 16; // Black
        } else if gray > 247 {
            return 231; // White
        } else {
            return 232 + ((gray - 8) * 24 / 240) as u8;
        }
    }

    // Map to 6x6x6 color cube (16-231)
    let r_idx = (r as u16 * 5 / 255) as u8;
    let g_idx = (g as u16 * 5 / 255) as u8;
    let b_idx = (b as u16 * 5 / 255) as u8;

    16 + 36 * r_idx + 6 * g_idx + b_idx
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
        colors.insert(
            "table_selected".to_string(),
            parser.parse(&config.colors.table_selected)?,
        );
        colors.insert(
            "sidebar_border".to_string(),
            parser.parse(&config.colors.sidebar_border)?,
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
}
