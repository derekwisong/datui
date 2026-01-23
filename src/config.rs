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

    /// Generate default configuration template as a string
    pub fn generate_default_config(&self) -> String {
        DEFAULT_CONFIG_TEMPLATE.to_string()
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

        // Write default template
        std::fs::write(&config_path, DEFAULT_CONFIG_TEMPLATE)?;

        Ok(config_path)
    }
}

/// Complete application configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    /// Configuration format version (for future compatibility)
    pub version: String,
    pub file_loading: FileLoadingConfig,
    pub display: DisplayConfig,
    pub performance: PerformanceConfig,
    pub theme: ThemeConfig,
    pub ui: UiConfig,
    pub query: QueryConfig,
    pub templates: TemplateConfig,
    pub debug: DebugConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct FileLoadingConfig {
    pub delimiter: Option<u8>,
    pub has_header: Option<bool>,
    pub skip_lines: Option<usize>,
    pub skip_rows: Option<usize>,
    pub compression: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DisplayConfig {
    pub pages_lookahead: usize,
    pub pages_lookback: usize,
    pub row_numbers: bool,
    pub row_start_index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PerformanceConfig {
    pub sampling_threshold: usize,
    pub event_poll_interval_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ThemeConfig {
    pub color_mode: String,
    pub colors: ColorConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ColorConfig {
    pub primary: String,
    pub secondary: String,
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
    pub table_border: String,
    pub table_selected: String,
    pub modal_border: String,
    pub modal_border_active: String,
    pub modal_border_error: String,
    pub distribution_normal: String,
    pub distribution_skewed: String,
    pub distribution_other: String,
    pub outlier_marker: String,
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct QueryConfig {
    pub history_limit: usize,
    pub enable_history: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TemplateConfig {
    pub auto_apply: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DebugConfig {
    pub enabled: bool,
    pub show_performance: bool,
    pub show_query: bool,
    pub show_transformations: bool,
}

// Default implementations
impl Default for AppConfig {
    fn default() -> Self {
        Self {
            version: "0.2".to_string(),
            file_loading: FileLoadingConfig::default(),
            display: DisplayConfig::default(),
            performance: PerformanceConfig::default(),
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
            row_numbers: false,
            row_start_index: 1,
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            sampling_threshold: 10000,
            event_poll_interval_ms: 25,
        }
    }
}

impl Default for ThemeConfig {
    fn default() -> Self {
        Self {
            color_mode: "auto".to_string(),
            colors: ColorConfig::default(),
        }
    }
}

impl Default for ColorConfig {
    fn default() -> Self {
        Self {
            primary: "cyan".to_string(),
            secondary: "yellow".to_string(),
            success: "green".to_string(),
            error: "red".to_string(),
            warning: "yellow".to_string(),
            dimmed: "dark_gray".to_string(),
            background: "black".to_string(),
            surface: "black".to_string(),
            controls_bg: "indexed(236)".to_string(),
            text_primary: "white".to_string(),
            text_secondary: "dark_gray".to_string(),
            text_inverse: "black".to_string(),
            table_header: "white".to_string(),
            table_border: "cyan".to_string(),
            table_selected: "reversed".to_string(),
            modal_border: "cyan".to_string(),
            modal_border_active: "yellow".to_string(),
            modal_border_error: "red".to_string(),
            distribution_normal: "green".to_string(),
            distribution_skewed: "yellow".to_string(),
            distribution_other: "white".to_string(),
            outlier_marker: "red".to_string(),
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
        if let Ok(user_config) = Self::load_user_config(app_name) {
            config.merge(user_config);
        }

        // Validate configuration
        config.validate()?;

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
        self.file_loading.merge(other.file_loading);
        self.display.merge(other.display);
        self.performance.merge(other.performance);
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
        if self.performance.sampling_threshold == 0 {
            return Err(eyre!("sampling_threshold must be greater than 0"));
        }

        if self.performance.event_poll_interval_ms == 0 {
            return Err(eyre!("event_poll_interval_ms must be greater than 0"));
        }

        // Validate color mode
        match self.theme.color_mode.as_str() {
            "light" | "dark" | "auto" => {}
            _ => {
                return Err(eyre!(
                    "Invalid color_mode: {}. Must be 'light', 'dark', or 'auto'",
                    self.theme.color_mode
                ))
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
        if other.compression.is_some() {
            self.compression = other.compression;
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
        if other.row_numbers != default.row_numbers {
            self.row_numbers = other.row_numbers;
        }
        if other.row_start_index != default.row_start_index {
            self.row_start_index = other.row_start_index;
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
    }
}

impl ThemeConfig {
    pub fn merge(&mut self, other: Self) {
        let default = ThemeConfig::default();
        if other.color_mode != default.color_mode {
            self.color_mode = other.color_mode;
        }
        self.colors.merge(other.colors);
    }
}

impl ColorConfig {
    /// Validate all color strings can be parsed
    fn validate(&self, parser: &ColorParser) -> Result<()> {
        // Helper macro to validate a color field
        macro_rules! validate_color {
            ($field:expr, $name:expr) => {
                parser
                    .parse($field)
                    .map_err(|e| eyre!("Invalid color value for '{}': {}", $name, e))?;
            };
        }

        validate_color!(&self.primary, "primary");
        validate_color!(&self.secondary, "secondary");
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
        validate_color!(&self.table_border, "table_border");
        validate_color!(&self.table_selected, "table_selected");
        validate_color!(&self.modal_border, "modal_border");
        validate_color!(&self.modal_border_active, "modal_border_active");
        validate_color!(&self.modal_border_error, "modal_border_error");
        validate_color!(&self.distribution_normal, "distribution_normal");
        validate_color!(&self.distribution_skewed, "distribution_skewed");
        validate_color!(&self.distribution_other, "distribution_other");
        validate_color!(&self.outlier_marker, "outlier_marker");

        Ok(())
    }

    pub fn merge(&mut self, other: Self) {
        let default = ColorConfig::default();

        // Macro would be nice here, but keeping it explicit for clarity
        if other.primary != default.primary {
            self.primary = other.primary;
        }
        if other.secondary != default.secondary {
            self.secondary = other.secondary;
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
        if other.table_border != default.table_border {
            self.table_border = other.table_border;
        }
        if other.table_selected != default.table_selected {
            self.table_selected = other.table_selected;
        }
        if other.modal_border != default.modal_border {
            self.modal_border = other.modal_border;
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
            "reset" | "reversed" => Ok(Color::Reset),

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
        colors.insert("primary".to_string(), parser.parse(&config.colors.primary)?);
        colors.insert(
            "secondary".to_string(),
            parser.parse(&config.colors.secondary)?,
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
            "table_border".to_string(),
            parser.parse(&config.colors.table_border)?,
        );
        colors.insert(
            "table_selected".to_string(),
            parser.parse(&config.colors.table_selected)?,
        );
        colors.insert(
            "modal_border".to_string(),
            parser.parse(&config.colors.modal_border)?,
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

// Default configuration template
const DEFAULT_CONFIG_TEMPLATE: &str = include_str!("../config/default.toml");
