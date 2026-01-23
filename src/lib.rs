use color_eyre::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use polars::prelude::{LazyFrame, Schema};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{mpsc::Sender, Arc};
use widgets::info::DataTableInfo;

use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};

use ratatui::widgets::{
    Block, Borders, Cell, Clear, Gauge, List, ListItem, Paragraph, Row, StatefulWidget, Table,
};

pub mod analysis_modal;
pub mod cache;
pub mod cli;
pub mod config;
pub mod filter_modal;
pub mod pivot_melt_modal;
mod query;
pub mod sort_modal;
pub mod statistics;
pub mod template;
pub mod widgets;

pub use cache::CacheManager;
pub use cli::Args;
pub use config::{
    rgb_to_256_color, rgb_to_basic_ansi, AppConfig, ColorParser, ConfigManager, Theme,
};

use analysis_modal::AnalysisModal;
use filter_modal::{FilterFocus, FilterModal, FilterOperator, FilterStatement, LogicalOperator};
use pivot_melt_modal::{MeltSpec, PivotMeltFocus, PivotMeltModal, PivotMeltTab, PivotSpec};
use sort_modal::{SortColumn, SortFocus, SortModal};
pub use template::{Template, TemplateManager};
use widgets::controls::Controls;
use widgets::datatable::{DataTable, DataTableState};
use widgets::debug::DebugState;
use widgets::pivot_melt;
use widgets::template_modal::{CreateFocus, TemplateFocus, TemplateModal, TemplateModalMode};

/// Application name used for cache directory and other app-specific paths
pub const APP_NAME: &str = "datui";

/// Re-export compression format from CLI module
pub use cli::CompressionFormat;

impl CompressionFormat {
    /// Detect compression format from file extension
    pub fn from_extension(path: &std::path::Path) -> Option<Self> {
        // Check final extension (e.g., .csv.gz -> gz)
        if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
            match ext.to_lowercase().as_str() {
                "gz" => Some(Self::Gzip),
                "zst" | "zstd" => Some(Self::Zstd),
                "bz2" | "bz" => Some(Self::Bzip2),
                "xz" => Some(Self::Xz),
                _ => None,
            }
        } else {
            None
        }
    }

    /// Get file extension for this compression format
    pub fn extension(&self) -> &'static str {
        match self {
            Self::Gzip => "gz",
            Self::Zstd => "zst",
            Self::Bzip2 => "bz2",
            Self::Xz => "xz",
        }
    }
}

#[cfg(test)]
mod compression_tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_compression_detection() {
        assert_eq!(
            CompressionFormat::from_extension(Path::new("file.csv.gz")),
            Some(CompressionFormat::Gzip)
        );
        assert_eq!(
            CompressionFormat::from_extension(Path::new("file.csv.zst")),
            Some(CompressionFormat::Zstd)
        );
        assert_eq!(
            CompressionFormat::from_extension(Path::new("file.csv.bz2")),
            Some(CompressionFormat::Bzip2)
        );
        assert_eq!(
            CompressionFormat::from_extension(Path::new("file.csv.xz")),
            Some(CompressionFormat::Xz)
        );
        assert_eq!(
            CompressionFormat::from_extension(Path::new("file.csv")),
            None
        );
        assert_eq!(CompressionFormat::from_extension(Path::new("file")), None);
    }

    #[test]
    fn test_compression_extension() {
        assert_eq!(CompressionFormat::Gzip.extension(), "gz");
        assert_eq!(CompressionFormat::Zstd.extension(), "zst");
        assert_eq!(CompressionFormat::Bzip2.extension(), "bz2");
        assert_eq!(CompressionFormat::Xz.extension(), "xz");
    }
}

#[cfg(test)]
pub mod tests {
    use std::path::Path;
    use std::process::Command;
    use std::sync::Once;

    static INIT: Once = Once::new();

    /// Ensures that sample data files are generated before tests run.
    /// This function uses `std::sync::Once` to ensure it only runs once,
    /// even if called from multiple tests.
    pub fn ensure_sample_data() {
        INIT.call_once(|| {
            let sample_data_dir = Path::new("tests/sample-data");

            // Check if key files exist to determine if we need to generate data
            // We check for a few representative files that should always be generated
            let key_files = [
                "people.parquet",
                "sales.parquet",
                "large_dataset.parquet",
                "empty.parquet",
                "pivot_long.parquet",
                "melt_wide.parquet",
            ];

            let needs_generation = !sample_data_dir.exists()
                || key_files
                    .iter()
                    .any(|file| !sample_data_dir.join(file).exists());

            if needs_generation {
                eprintln!("Sample data not found. Generating test data...");

                // Get the path to the Python script
                let script_path = Path::new("scripts/generate_sample_data.py");
                if !script_path.exists() {
                    panic!(
                        "Sample data generation script not found at: {}. \
                        Please ensure you're running tests from the repository root.",
                        script_path.display()
                    );
                }

                // Try to find Python (python3 or python)
                let python_cmd = if Command::new("python3").arg("--version").output().is_ok() {
                    "python3"
                } else if Command::new("python").arg("--version").output().is_ok() {
                    "python"
                } else {
                    panic!(
                        "Python not found. Please install Python 3 to generate test data. \
                        The script requires: polars>=0.20.0 and numpy>=1.24.0"
                    );
                };

                // Run the generation script
                let output = Command::new(python_cmd)
                    .arg(script_path)
                    .output()
                    .unwrap_or_else(|e| {
                        panic!(
                            "Failed to run sample data generation script: {}. \
                            Make sure Python is installed and the script is executable.",
                            e
                        );
                    });

                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    panic!(
                        "Sample data generation failed!\n\
                        Exit code: {:?}\n\
                        stdout:\n{}\n\
                        stderr:\n{}",
                        output.status.code(),
                        stdout,
                        stderr
                    );
                }

                eprintln!("Sample data generation complete!");
            }
        });
    }
}

#[derive(Default, Clone)]
pub struct OpenOptions {
    pub delimiter: Option<u8>,
    pub has_header: Option<bool>,
    pub skip_lines: Option<usize>,
    pub skip_rows: Option<usize>,
    pub compression: Option<CompressionFormat>,
    pub pages_lookahead: Option<usize>,
    pub pages_lookback: Option<usize>,
    pub row_numbers: bool,
    pub row_start_index: usize,
}

impl OpenOptions {
    pub fn new() -> Self {
        Self {
            delimiter: None,
            has_header: None,
            skip_lines: None,
            skip_rows: None,
            compression: None,
            pages_lookahead: None,
            pages_lookback: None,
            row_numbers: false,
            row_start_index: 1,
        }
    }

    pub fn with_skip_lines(mut self, skip_lines: usize) -> Self {
        self.skip_lines = Some(skip_lines);
        self
    }

    pub fn with_skip_rows(mut self, skip_rows: usize) -> Self {
        self.skip_rows = Some(skip_rows);
        self
    }

    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = Some(delimiter);
        self
    }

    pub fn with_has_header(mut self, has_header: bool) -> Self {
        self.has_header = Some(has_header);
        self
    }

    pub fn with_compression(mut self, compression: CompressionFormat) -> Self {
        self.compression = Some(compression);
        self
    }
}

impl OpenOptions {
    /// Create OpenOptions from CLI args and config, with CLI args taking precedence
    pub fn from_args_and_config(args: &cli::Args, config: &AppConfig) -> Self {
        let mut opts = OpenOptions::new();

        // File loading options: CLI args override config
        opts.delimiter = args.delimiter.or(config.file_loading.delimiter);
        opts.skip_lines = args.skip_lines.or(config.file_loading.skip_lines);
        opts.skip_rows = args.skip_rows.or(config.file_loading.skip_rows);

        // Handle has_header: CLI no_header flag overrides config
        opts.has_header = if let Some(no_header) = args.no_header {
            Some(!no_header)
        } else {
            config.file_loading.has_header
        };

        // Handle compression: CLI arg overrides config
        opts.compression = args.compression.or_else(|| {
            config
                .file_loading
                .compression
                .as_ref()
                .and_then(|s| match s.as_str() {
                    "gzip" => Some(CompressionFormat::Gzip),
                    "zstd" => Some(CompressionFormat::Zstd),
                    "bzip2" => Some(CompressionFormat::Bzip2),
                    "xz" => Some(CompressionFormat::Xz),
                    _ => None,
                })
        });

        // Display options: CLI args override config
        opts.pages_lookahead = args
            .pages_lookahead
            .or(Some(config.display.pages_lookahead));
        opts.pages_lookback = args.pages_lookback.or(Some(config.display.pages_lookback));

        // Row numbers: CLI flag overrides config
        opts.row_numbers = args.row_numbers || config.display.row_numbers;

        // Row start index: CLI arg overrides config
        opts.row_start_index = args
            .row_start_index
            .unwrap_or(config.display.row_start_index);

        opts
    }
}

impl From<&cli::Args> for OpenOptions {
    fn from(args: &cli::Args) -> Self {
        // Use default config if creating from args alone
        let config = AppConfig::default();
        Self::from_args_and_config(args, &config)
    }
}

pub enum AppEvent {
    Key(KeyEvent),
    Open(PathBuf, OpenOptions),
    DoLoad(PathBuf, OpenOptions), // Internal event to actually perform loading after UI update
    DoDecompress(PathBuf, OpenOptions), // Internal event to perform decompression after UI shows "Decompressing"
    Exit,
    Crash(String),
    Search(String),
    Filter(Vec<FilterStatement>),
    Sort(Vec<String>, bool),         // Columns, Ascending
    ColumnOrder(Vec<String>, usize), // Column order, locked columns count
    Pivot(PivotSpec),
    Melt(MeltSpec),
    Collect,
    Update,
    Reset,
    Resize(u16, u16), // resized (width, height)
}

#[derive(Debug, Default, PartialEq, Eq)]
pub enum InputMode {
    #[default]
    Normal,
    Filtering,
    Sorting,
    PivotMelt,
    Editing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputType {
    Search,
    Filter,
}

#[derive(Default)]
pub struct ErrorModal {
    pub active: bool,
    pub message: String,
}

impl ErrorModal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn show(&mut self, message: String) {
        self.active = true;
        self.message = message;
    }

    pub fn hide(&mut self) {
        self.active = false;
        self.message.clear();
    }
}

#[derive(Clone, Debug, Default)]
pub enum LoadingState {
    #[default]
    Idle,
    Loading {
        file_path: PathBuf,
        file_size: u64,        // Size of compressed file in bytes
        current_phase: String, // e.g., "Opening file", "Decompressing", "Building lazyframe", "Rendering data"
        progress_percent: u16, // 0-100
    },
}

impl LoadingState {
    pub fn is_loading(&self) -> bool {
        matches!(self, LoadingState::Loading { .. })
    }
}

// Helper struct to save state before template application
struct TemplateApplicationState {
    lf: LazyFrame,
    schema: Arc<Schema>,
    active_query: String,
    filters: Vec<FilterStatement>,
    sort_columns: Vec<String>,
    sort_ascending: bool,
    column_order: Vec<String>,
    locked_columns_count: usize,
}

pub struct App {
    pub data_table_state: Option<DataTableState>,
    path: Option<PathBuf>,
    events: Sender<AppEvent>,
    focus: u32,
    debug: DebugState,
    info_visible: bool,
    input: String,
    input_cursor: usize, // Cursor position in input string
    pub input_mode: InputMode,
    input_type: Option<InputType>,
    pub sort_modal: SortModal,
    pub filter_modal: FilterModal,
    pub pivot_melt_modal: PivotMeltModal,
    pub template_modal: TemplateModal,
    pub analysis_modal: AnalysisModal,
    error_modal: ErrorModal,
    show_help: bool,
    help_scroll: usize, // Scroll position for help content
    cache: CacheManager,
    template_manager: TemplateManager,
    active_template_id: Option<String>, // ID of currently applied template
    query_history: Vec<String>,         // History of successful queries
    query_history_index: Option<usize>, // Current position in history (None = editing new query)
    query_history_temp: Option<String>, // Temporary storage for current input when navigating history
    loading_state: LoadingState,        // Current loading state for progress indication
    theme: Theme,                       // Color theme for UI rendering
    sampling_threshold: usize,          // Threshold for sampling large datasets
}

impl App {
    pub fn send_event(&mut self, event: AppEvent) -> Result<()> {
        self.events.send(event)?;
        Ok(())
    }

    fn render_loading_gauge(loading_state: &LoadingState, area: Rect, buf: &mut Buffer) {
        if let LoadingState::Loading {
            current_phase,
            progress_percent,
            ..
        } = loading_state
        {
            // Center the gauge in the area
            let gauge_width = (area.width as f64 * 0.33) as u16; // 1/3 of available width
            let gauge_height = 5u16; // Height for title, subtitle, and gauge

            let center_layout = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Fill(1),
                    Constraint::Length(gauge_height),
                    Constraint::Fill(1),
                ])
                .split(area);

            let gauge_area_layout = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Fill(1),
                    Constraint::Length(gauge_width),
                    Constraint::Fill(1),
                ])
                .split(center_layout[1]);

            let gauge_area = gauge_area_layout[1];

            // Create gauge with progress percentage
            let gauge = Gauge::default()
                .block(Block::default().borders(Borders::ALL).title("Loading"))
                .percent(*progress_percent)
                .label(current_phase.clone());

            gauge.render(gauge_area, buf);
        }
    }

    pub fn new(events: Sender<AppEvent>) -> App {
        // Create default theme for backward compatibility
        let theme = Theme::from_config(&AppConfig::default().theme).unwrap_or_else(|e| {
            eprintln!(
                "Warning: Failed to create default theme: {}. Using fallback.",
                e
            );
            // Create a minimal fallback theme
            Theme {
                colors: std::collections::HashMap::new(),
            }
        });

        Self::new_with_config(events, theme, AppConfig::default())
    }

    pub fn new_with_theme(events: Sender<AppEvent>, theme: Theme) -> App {
        Self::new_with_config(events, theme, AppConfig::default())
    }

    pub fn new_with_config(events: Sender<AppEvent>, theme: Theme, app_config: AppConfig) -> App {
        let cache = CacheManager::new(APP_NAME).unwrap_or_else(|e| {
            eprintln!("Warning: Could not initialize cache manager: {}", e);
            CacheManager {
                cache_dir: std::env::temp_dir().join(APP_NAME),
            }
        });

        let config_manager = ConfigManager::new(APP_NAME).unwrap_or_else(|e| {
            eprintln!("Warning: Could not initialize config manager: {}", e);
            ConfigManager {
                config_dir: std::env::temp_dir().join(APP_NAME).join("config"),
            }
        });

        let template_manager = TemplateManager::new(&config_manager).unwrap_or_else(|e| {
            eprintln!("Warning: Could not initialize template manager: {}", e);
            let temp_config = ConfigManager::new("datui").unwrap_or_else(|_| ConfigManager {
                config_dir: std::env::temp_dir().join("datui").join("config"),
            });
            TemplateManager::new(&temp_config).unwrap_or_else(|_| {
                let last_resort = ConfigManager {
                    config_dir: std::env::temp_dir().join("datui_config"),
                };
                TemplateManager::new(&last_resort).unwrap()
            })
        });

        let mut app = App {
            path: None,
            data_table_state: None,
            events,
            focus: 0,
            debug: DebugState::default(),
            info_visible: false,
            input: String::new(),
            input_cursor: 0,
            input_mode: InputMode::Normal,
            input_type: None,
            sort_modal: SortModal::new(),
            filter_modal: FilterModal::new(),
            pivot_melt_modal: PivotMeltModal::new(),
            template_modal: TemplateModal::new(),
            analysis_modal: AnalysisModal::new(),
            error_modal: ErrorModal::new(),
            show_help: false,
            help_scroll: 0,
            cache,
            template_manager,
            active_template_id: None,
            query_history: Vec::new(),
            query_history_index: None,
            query_history_temp: None,
            loading_state: LoadingState::Idle,
            theme,
            sampling_threshold: app_config.performance.sampling_threshold,
        };

        app.load_query_history();
        app
    }

    pub fn enable_debug(&mut self) {
        self.debug.enabled = true;
    }

    /// Get a color from the theme by name
    fn color(&self, name: &str) -> Color {
        self.theme.get(name)
    }

    fn load_query_history(&mut self) {
        let history_file = self.cache.cache_file("query_history.txt");

        match std::fs::read_to_string(&history_file) {
            Ok(content) => {
                self.query_history = content
                    .lines()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .take(1000)
                    .collect();
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                eprintln!("Warning: Could not read query history: {}", e);
            }
        }
    }

    fn save_query_history(&self) {
        if let Err(e) = self.cache.ensure_cache_dir() {
            eprintln!("Warning: Could not create cache directory: {}", e);
            return;
        }

        let history_file = self.cache.cache_file("query_history.txt");

        match std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&history_file)
        {
            Ok(mut file) => {
                if let Err(e) = fs2::FileExt::try_lock_exclusive(&file) {
                    eprintln!("Warning: Could not lock history file: {}", e);
                }

                for query in &self.query_history {
                    if let Err(e) = writeln!(file, "{}", query) {
                        eprintln!("Warning: Could not write query to history: {}", e);
                        break;
                    }
                }

                if let Err(e) = file.flush() {
                    eprintln!("Warning: Could not flush history file: {}", e);
                }
            }
            Err(e) => {
                eprintln!("Warning: Could not create history file: {}", e);
            }
        }
    }

    fn add_to_history(&mut self, query: String) {
        let query = query.trim().to_string();

        if query.is_empty() {
            return;
        }

        if let Some(last) = self.query_history.last() {
            if last == &query {
                return;
            }
        }

        self.query_history.push(query);

        if self.query_history.len() > 1000 {
            self.query_history.remove(0);
        }

        self.save_query_history();
    }

    fn load(&mut self, path: &Path, options: &OpenOptions) -> Result<()> {
        // Check for compressed CSV files (e.g., file.csv.gz, file.csv.zst, etc.)
        let compression = options
            .compression
            .or_else(|| CompressionFormat::from_extension(path));
        let is_csv = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .map(|stem| {
                stem.ends_with(".csv")
                    || path
                        .extension()
                        .and_then(|e| e.to_str())
                        .map(|e| e.eq_ignore_ascii_case("csv"))
                        .unwrap_or(false)
            })
            .unwrap_or(false);
        let is_compressed_csv = compression.is_some() && is_csv;

        // For compressed files, decompression phase is already set in DoLoad handler
        // Now actually perform decompression and CSV reading (this is the slow part)
        if is_compressed_csv {
            let lf = DataTableState::from_csv(path, options)?; // Already passes pages_lookahead/lookback via options

            // Phase 2: Building lazyframe (after decompression, before rendering)
            if let LoadingState::Loading {
                file_path,
                file_size,
                ..
            } = &self.loading_state
            {
                self.loading_state = LoadingState::Loading {
                    file_path: file_path.clone(),
                    file_size: *file_size,
                    current_phase: "Building lazyframe".to_string(),
                    progress_percent: 60,
                };
            }

            // Phase 3: Rendering data
            if let LoadingState::Loading {
                file_path,
                file_size,
                ..
            } = &self.loading_state
            {
                self.loading_state = LoadingState::Loading {
                    file_path: file_path.clone(),
                    file_size: *file_size,
                    current_phase: "Rendering data".to_string(),
                    progress_percent: 90,
                };
            }

            // Clear loading state after successful load
            self.loading_state = LoadingState::Idle;
            self.data_table_state = Some(lf);
            self.path = Some(path.to_path_buf());
            self.sort_modal = SortModal::new();
            self.filter_modal = FilterModal::new();
            self.pivot_melt_modal = PivotMeltModal::new();
            return Ok(());
        }

        // For non-gzipped files, proceed with normal loading
        // Phase 2: Building lazyframe
        if let LoadingState::Loading {
            file_path,
            file_size,
            ..
        } = &self.loading_state
        {
            self.loading_state = LoadingState::Loading {
                file_path: file_path.clone(),
                file_size: *file_size,
                current_phase: "Building lazyframe".to_string(),
                progress_percent: 60,
            };
        }

        let lf = match path.extension() {
            Some(ext) if ext.eq_ignore_ascii_case("parquet") => DataTableState::from_parquet(
                path,
                options.pages_lookahead,
                options.pages_lookback,
                options.row_numbers,
                options.row_start_index,
            )?,
            Some(ext) if ext.eq_ignore_ascii_case("csv") => {
                DataTableState::from_csv(path, options)? // Already passes row_numbers via options
            }
            Some(ext) if ext.eq_ignore_ascii_case("tsv") => DataTableState::from_delimited(
                path,
                b'\t',
                options.pages_lookahead,
                options.pages_lookback,
                options.row_numbers,
                options.row_start_index,
            )?,
            Some(ext) if ext.eq_ignore_ascii_case("psv") => DataTableState::from_delimited(
                path,
                b'|',
                options.pages_lookahead,
                options.pages_lookback,
                options.row_numbers,
                options.row_start_index,
            )?,
            Some(ext) if ext.eq_ignore_ascii_case("json") => DataTableState::from_json(
                path,
                options.pages_lookahead,
                options.pages_lookback,
                options.row_numbers,
                options.row_start_index,
            )?,
            Some(ext) if ext.eq_ignore_ascii_case("jsonl") => DataTableState::from_json_lines(
                path,
                options.pages_lookahead,
                options.pages_lookback,
                options.row_numbers,
                options.row_start_index,
            )?,
            Some(ext) if ext.eq_ignore_ascii_case("ndjson") => DataTableState::from_ndjson(
                path,
                options.pages_lookahead,
                options.pages_lookback,
                options.row_numbers,
                options.row_start_index,
            )?,
            _ => {
                self.loading_state = LoadingState::Idle;
                return Err(color_eyre::eyre::eyre!("Unsupported file type"));
            }
        };

        // Phase 3: Rendering data
        if let LoadingState::Loading {
            file_path,
            file_size,
            ..
        } = &self.loading_state
        {
            self.loading_state = LoadingState::Loading {
                file_path: file_path.clone(),
                file_size: *file_size,
                current_phase: "Rendering data".to_string(),
                progress_percent: 90,
            };
        }

        // Clear loading state after successful load
        self.loading_state = LoadingState::Idle;
        self.data_table_state = Some(lf);
        self.path = Some(path.to_path_buf());
        self.sort_modal = SortModal::new();
        self.filter_modal = FilterModal::new();
        self.pivot_melt_modal = PivotMeltModal::new();
        Ok(())
    }

    fn key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        self.debug.on_key(event);

        // Handle error modal first - it has highest priority
        if self.error_modal.active {
            match event.code {
                KeyCode::Esc | KeyCode::Enter => {
                    self.error_modal.hide();
                }
                _ => {}
            }
            return None;
        }

        if self.show_help
            || (self.template_modal.active && self.template_modal.show_help)
            || (self.analysis_modal.active && self.analysis_modal.show_help)
        {
            match event.code {
                KeyCode::Esc => {
                    if self.analysis_modal.active && self.analysis_modal.show_help {
                        self.analysis_modal.show_help = false;
                    } else if self.template_modal.active && self.template_modal.show_help {
                        self.template_modal.show_help = false;
                    } else {
                        self.show_help = false;
                    }
                    self.help_scroll = 0;
                }
                KeyCode::Char('h') if event.modifiers.contains(KeyModifiers::CONTROL) => {
                    if self.analysis_modal.active && self.analysis_modal.show_help {
                        self.analysis_modal.show_help = false;
                    } else if self.template_modal.active && self.template_modal.show_help {
                        self.template_modal.show_help = false;
                    } else {
                        self.show_help = false;
                    }
                    self.help_scroll = 0;
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.help_scroll = self.help_scroll.saturating_add(1);
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.help_scroll = self.help_scroll.saturating_sub(1);
                }
                KeyCode::PageDown => {
                    self.help_scroll = self.help_scroll.saturating_add(10);
                }
                KeyCode::PageUp => {
                    self.help_scroll = self.help_scroll.saturating_sub(10);
                }
                KeyCode::Home => {
                    self.help_scroll = 0;
                }
                KeyCode::End => {
                    // Will be set based on content height in render
                }
                _ => {}
            }
            return None;
        }

        if event.code == KeyCode::Char('h') && event.modifiers.contains(KeyModifiers::CONTROL) {
            // If analysis modal is active, show analysis help
            if self.analysis_modal.active {
                self.analysis_modal.show_help = true;
            } else if self.template_modal.active {
                self.template_modal.show_help = true;
            } else {
                self.show_help = true;
            }
            return None;
        }

        if self.input_mode == InputMode::Filtering {
            match event.code {
                KeyCode::Esc => {
                    self.input_mode = InputMode::Normal;
                    self.filter_modal.active = false;
                }
                KeyCode::Tab => {
                    self.filter_modal.focus = match self.filter_modal.focus {
                        FilterFocus::Column => FilterFocus::Operator,
                        FilterFocus::Operator => FilterFocus::Value,
                        FilterFocus::Value => FilterFocus::Logical,
                        FilterFocus::Logical => FilterFocus::Add,
                        FilterFocus::Add => {
                            if !self.filter_modal.statements.is_empty() {
                                FilterFocus::Statements
                            } else {
                                FilterFocus::Confirm
                            }
                        }
                        FilterFocus::Statements => FilterFocus::Confirm,
                        FilterFocus::Confirm => FilterFocus::Clear,
                        FilterFocus::Clear => FilterFocus::Column,
                    };
                }
                KeyCode::BackTab => {
                    self.filter_modal.focus = match self.filter_modal.focus {
                        FilterFocus::Column => FilterFocus::Clear,
                        FilterFocus::Operator => FilterFocus::Column,
                        FilterFocus::Value => FilterFocus::Operator,
                        FilterFocus::Logical => FilterFocus::Value,
                        FilterFocus::Add => FilterFocus::Logical,
                        FilterFocus::Statements => FilterFocus::Add,
                        FilterFocus::Confirm => {
                            if !self.filter_modal.statements.is_empty() {
                                FilterFocus::Statements
                            } else {
                                FilterFocus::Add
                            }
                        }
                        FilterFocus::Clear => FilterFocus::Confirm,
                    };
                }
                KeyCode::Down | KeyCode::Char('j')
                    if self.filter_modal.focus == FilterFocus::Statements =>
                {
                    let i = match self.filter_modal.list_state.selected() {
                        Some(i) => {
                            if i >= self.filter_modal.statements.len().saturating_sub(1) {
                                0
                            } else {
                                i + 1
                            }
                        }
                        None => 0,
                    };
                    self.filter_modal.list_state.select(Some(i));
                }
                KeyCode::Up | KeyCode::Char('k')
                    if self.filter_modal.focus == FilterFocus::Statements =>
                {
                    let i = match self.filter_modal.list_state.selected() {
                        Some(i) => {
                            if i == 0 {
                                self.filter_modal.statements.len().saturating_sub(1)
                            } else {
                                i - 1
                            }
                        }
                        None => 0,
                    };
                    self.filter_modal.list_state.select(Some(i));
                }
                KeyCode::Enter => {
                    match self.filter_modal.focus {
                        FilterFocus::Add => self.filter_modal.add_statement(),
                        FilterFocus::Confirm => {
                            self.input_mode = InputMode::Normal;
                            self.filter_modal.active = false;
                            return Some(AppEvent::Filter(self.filter_modal.statements.clone()));
                        }
                        FilterFocus::Clear => {
                            self.filter_modal.statements.clear();
                            self.filter_modal.list_state.select(None);
                        }
                        FilterFocus::Statements => {
                            // Remove selected
                            if let Some(idx) = self.filter_modal.list_state.selected() {
                                if idx < self.filter_modal.statements.len() {
                                    self.filter_modal.statements.remove(idx);
                                    if self.filter_modal.statements.is_empty() {
                                        self.filter_modal.list_state.select(None);
                                        self.filter_modal.focus = FilterFocus::Column;
                                    } else if idx >= self.filter_modal.statements.len() {
                                        self.filter_modal
                                            .list_state
                                            .select(Some(self.filter_modal.statements.len() - 1));
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
                KeyCode::Char(c) if self.filter_modal.focus == FilterFocus::Value => {
                    self.filter_modal.new_value.push(c);
                }
                KeyCode::Backspace if self.filter_modal.focus == FilterFocus::Value => {
                    self.filter_modal.new_value.pop();
                }
                KeyCode::Right | KeyCode::Char('l') => match self.filter_modal.focus {
                    FilterFocus::Column => {
                        self.filter_modal.new_column_idx = (self.filter_modal.new_column_idx + 1)
                            % self.filter_modal.available_columns.len().max(1);
                    }
                    FilterFocus::Operator => {
                        self.filter_modal.new_operator_idx = (self.filter_modal.new_operator_idx
                            + 1)
                            % FilterOperator::iterator().count();
                    }
                    FilterFocus::Logical => {
                        self.filter_modal.new_logical_idx = (self.filter_modal.new_logical_idx + 1)
                            % LogicalOperator::iterator().count();
                    }
                    _ => {}
                },
                KeyCode::Left | KeyCode::Char('h') => match self.filter_modal.focus {
                    FilterFocus::Column => {
                        self.filter_modal.new_column_idx = if self.filter_modal.new_column_idx == 0
                        {
                            self.filter_modal.available_columns.len().saturating_sub(1)
                        } else {
                            self.filter_modal.new_column_idx - 1
                        };
                    }
                    FilterFocus::Operator => {
                        self.filter_modal.new_operator_idx =
                            if self.filter_modal.new_operator_idx == 0 {
                                FilterOperator::iterator().count() - 1
                            } else {
                                self.filter_modal.new_operator_idx - 1
                            };
                    }
                    FilterFocus::Logical => {
                        self.filter_modal.new_logical_idx =
                            if self.filter_modal.new_logical_idx == 0 {
                                LogicalOperator::iterator().count() - 1
                            } else {
                                self.filter_modal.new_logical_idx - 1
                            };
                    }
                    _ => {}
                },
                _ => {}
            }
            return None;
        }

        if self.input_mode == InputMode::Sorting {
            match event.code {
                KeyCode::Esc => {
                    // Clear all unapplied changes including to-be-locked state
                    for col in &mut self.sort_modal.columns {
                        col.is_to_be_locked = false;
                    }
                    self.sort_modal.has_unapplied_changes = false;
                    self.input_mode = InputMode::Normal;
                    self.sort_modal.active = false;
                }
                KeyCode::Tab => {
                    self.sort_modal.next_focus();
                }
                KeyCode::BackTab => {
                    self.sort_modal.prev_focus();
                }
                KeyCode::Char(']') => {
                    if self.sort_modal.focus == SortFocus::ColumnList {
                        self.sort_modal.move_selection_down();
                    }
                }
                KeyCode::Char('[') => {
                    if self.sort_modal.focus == SortFocus::ColumnList {
                        self.sort_modal.move_selection_up();
                    }
                }
                KeyCode::Char('+') | KeyCode::Char('=') => {
                    if self.sort_modal.focus == SortFocus::ColumnList {
                        self.sort_modal.move_column_display_up();
                        self.sort_modal.has_unapplied_changes = true;
                    }
                }
                KeyCode::Char('-') | KeyCode::Char('_') => {
                    if self.sort_modal.focus == SortFocus::ColumnList {
                        self.sort_modal.move_column_display_down();
                        self.sort_modal.has_unapplied_changes = true;
                    }
                }
                KeyCode::Char('L') => {
                    if self.sort_modal.focus == SortFocus::ColumnList {
                        self.sort_modal.toggle_lock_at_column();
                        self.sort_modal.has_unapplied_changes = true;
                    }
                }
                KeyCode::Char('v') => {
                    if self.sort_modal.focus == SortFocus::ColumnList {
                        self.sort_modal.toggle_visibility();
                        self.sort_modal.has_unapplied_changes = true;
                    }
                }
                KeyCode::Down => {
                    if self.sort_modal.focus == SortFocus::ColumnList {
                        let i = match self.sort_modal.table_state.selected() {
                            Some(i) => {
                                if i >= self.sort_modal.filtered_columns().len().saturating_sub(1) {
                                    0
                                } else {
                                    i + 1
                                }
                            }
                            None => 0,
                        };
                        self.sort_modal.table_state.select(Some(i));
                    } else {
                        self.sort_modal.next_focus();
                    }
                }
                KeyCode::Up => {
                    if self.sort_modal.focus == SortFocus::ColumnList {
                        let i = match self.sort_modal.table_state.selected() {
                            Some(i) => {
                                if i == 0 {
                                    self.sort_modal.filtered_columns().len().saturating_sub(1)
                                } else {
                                    i - 1
                                }
                            }
                            None => 0,
                        };
                        self.sort_modal.table_state.select(Some(i));
                    } else {
                        self.sort_modal.prev_focus();
                    }
                }
                KeyCode::Enter if event.modifiers.contains(KeyModifiers::CONTROL) => {
                    let columns = self.sort_modal.get_sorted_columns();
                    self.input_mode = InputMode::Normal;
                    self.sort_modal.active = false;
                    return Some(AppEvent::Sort(columns, self.sort_modal.ascending));
                }
                KeyCode::Enter => {
                    match self.sort_modal.focus {
                        SortFocus::Filter => self.sort_modal.focus = SortFocus::ColumnList,
                        SortFocus::ColumnList => {
                            self.sort_modal.toggle_selection();
                            // Apply sort without closing if Enter is pressed in ColumnList
                            let columns = self.sort_modal.get_sorted_columns();
                            let column_order = self.sort_modal.get_column_order();
                            let locked_count = self.sort_modal.get_locked_columns_count();
                            self.sort_modal.has_unapplied_changes = false;
                            // Apply both sort and column order
                            let _ =
                                self.send_event(AppEvent::ColumnOrder(column_order, locked_count));
                            return Some(AppEvent::Sort(columns, self.sort_modal.ascending));
                        }
                        SortFocus::Order => {
                            self.sort_modal.ascending = !self.sort_modal.ascending;
                            self.sort_modal.has_unapplied_changes = true;
                        }
                        SortFocus::Apply => {
                            let columns = self.sort_modal.get_sorted_columns();
                            let column_order = self.sort_modal.get_column_order();
                            let locked_count = self.sort_modal.get_locked_columns_count();
                            self.input_mode = InputMode::Normal;
                            self.sort_modal.active = false;
                            self.sort_modal.has_unapplied_changes = false;
                            // Apply both sort and column order
                            let _ =
                                self.send_event(AppEvent::ColumnOrder(column_order, locked_count));
                            return Some(AppEvent::Sort(columns, self.sort_modal.ascending));
                        }
                        SortFocus::Cancel => {
                            // Clear all unapplied changes including to-be-locked state
                            for col in &mut self.sort_modal.columns {
                                col.is_to_be_locked = false;
                            }
                            self.sort_modal.has_unapplied_changes = false;
                            self.input_mode = InputMode::Normal;
                            self.sort_modal.active = false;
                        }
                        SortFocus::Clear => self.sort_modal.clear_selection(),
                    }
                }
                KeyCode::Char(' ') => {
                    match self.sort_modal.focus {
                        SortFocus::ColumnList => {
                            self.sort_modal.toggle_selection();
                        }
                        SortFocus::Order => {
                            self.sort_modal.ascending = !self.sort_modal.ascending;
                            self.sort_modal.has_unapplied_changes = true;
                        }
                        SortFocus::Apply => {
                            let columns = self.sort_modal.get_sorted_columns();
                            let column_order = self.sort_modal.get_column_order();
                            let locked_count = self.sort_modal.get_locked_columns_count();
                            self.sort_modal.has_unapplied_changes = false;
                            // Apply both sort and column order
                            let _ =
                                self.send_event(AppEvent::ColumnOrder(column_order, locked_count));
                            return Some(AppEvent::Sort(columns, self.sort_modal.ascending));
                        }
                        _ => {}
                    }
                }
                KeyCode::Char(c)
                    if self.sort_modal.focus == SortFocus::ColumnList && c.is_ascii_digit() =>
                {
                    if let Some(digit) = c.to_digit(10) {
                        self.sort_modal.jump_selection_to_order(digit as usize);
                    }
                }
                KeyCode::Left if self.sort_modal.focus == SortFocus::Filter => {
                    if self.sort_modal.filter_cursor > 0 {
                        self.sort_modal.filter_cursor -= 1;
                    }
                }
                KeyCode::Right if self.sort_modal.focus == SortFocus::Filter => {
                    let char_count = self.sort_modal.filter.chars().count();
                    if self.sort_modal.filter_cursor < char_count {
                        self.sort_modal.filter_cursor += 1;
                    }
                }
                KeyCode::Home if self.sort_modal.focus == SortFocus::Filter => {
                    self.sort_modal.filter_cursor = 0;
                }
                KeyCode::End if self.sort_modal.focus == SortFocus::Filter => {
                    self.sort_modal.filter_cursor = self.sort_modal.filter.chars().count();
                }
                KeyCode::Char(c) if self.sort_modal.focus == SortFocus::Filter => {
                    // Convert character position to byte position
                    let byte_pos = self
                        .sort_modal
                        .filter
                        .chars()
                        .take(self.sort_modal.filter_cursor)
                        .map(|c| c.len_utf8())
                        .sum();
                    self.sort_modal.filter.insert(byte_pos, c);
                    self.sort_modal.filter_cursor += 1;
                    self.sort_modal.table_state.select(None); // Reset selection on filter change
                }
                KeyCode::Backspace if self.sort_modal.focus == SortFocus::Filter => {
                    if self.sort_modal.filter_cursor > 0 {
                        // Convert character position to byte position for the character before cursor
                        let prev_byte_pos: usize = self
                            .sort_modal
                            .filter
                            .chars()
                            .take(self.sort_modal.filter_cursor - 1)
                            .map(|c| c.len_utf8())
                            .sum();
                        let char_to_delete = self.sort_modal.filter[prev_byte_pos..]
                            .chars()
                            .next()
                            .unwrap();
                        let char_len = char_to_delete.len_utf8();
                        self.sort_modal
                            .filter
                            .drain(prev_byte_pos..prev_byte_pos + char_len);
                        self.sort_modal.filter_cursor -= 1;
                        self.sort_modal.table_state.select(None);
                    }
                }
                KeyCode::Delete if self.sort_modal.focus == SortFocus::Filter => {
                    let char_count = self.sort_modal.filter.chars().count();
                    if self.sort_modal.filter_cursor < char_count {
                        // Convert character position to byte position
                        let byte_pos: usize = self
                            .sort_modal
                            .filter
                            .chars()
                            .take(self.sort_modal.filter_cursor)
                            .map(|c| c.len_utf8())
                            .sum();
                        let char_to_delete =
                            self.sort_modal.filter[byte_pos..].chars().next().unwrap();
                        let char_len = char_to_delete.len_utf8();
                        self.sort_modal.filter.drain(byte_pos..byte_pos + char_len);
                        self.sort_modal.table_state.select(None);
                    }
                }
                _ => {}
            }
            return None;
        }

        if self.input_mode == InputMode::PivotMelt {
            if event.code == KeyCode::Char('h') && event.modifiers.contains(KeyModifiers::CONTROL) {
                self.show_help = true;
                return None;
            }
            match event.code {
                KeyCode::Esc => {
                    self.pivot_melt_modal.close();
                    self.input_mode = InputMode::Normal;
                }
                KeyCode::Tab => self.pivot_melt_modal.next_focus(),
                KeyCode::BackTab => self.pivot_melt_modal.prev_focus(),
                KeyCode::Left => {
                    if self.pivot_melt_modal.focus == PivotMeltFocus::PivotFilter {
                        if self.pivot_melt_modal.pivot_filter_cursor > 0 {
                            self.pivot_melt_modal.pivot_filter_cursor -= 1;
                        }
                        self.pivot_melt_modal.pivot_index_table.select(None);
                    } else if self.pivot_melt_modal.focus == PivotMeltFocus::MeltFilter {
                        if self.pivot_melt_modal.melt_filter_cursor > 0 {
                            self.pivot_melt_modal.melt_filter_cursor -= 1;
                        }
                        self.pivot_melt_modal.melt_index_table.select(None);
                    } else if self.pivot_melt_modal.focus == PivotMeltFocus::MeltPattern
                        && self.pivot_melt_modal.melt_pattern_cursor > 0
                    {
                        self.pivot_melt_modal.melt_pattern_cursor -= 1;
                    } else if self.pivot_melt_modal.focus == PivotMeltFocus::MeltVarName
                        && self.pivot_melt_modal.melt_variable_cursor > 0
                    {
                        self.pivot_melt_modal.melt_variable_cursor -= 1;
                    } else if self.pivot_melt_modal.focus == PivotMeltFocus::MeltValName
                        && self.pivot_melt_modal.melt_value_cursor > 0
                    {
                        self.pivot_melt_modal.melt_value_cursor -= 1;
                    } else if self.pivot_melt_modal.focus == PivotMeltFocus::TabBar {
                        self.pivot_melt_modal.switch_tab();
                    } else {
                        self.pivot_melt_modal.prev_focus();
                    }
                }
                KeyCode::Right => {
                    if self.pivot_melt_modal.focus == PivotMeltFocus::PivotFilter {
                        let n = self.pivot_melt_modal.pivot_filter.chars().count();
                        if self.pivot_melt_modal.pivot_filter_cursor < n {
                            self.pivot_melt_modal.pivot_filter_cursor += 1;
                        }
                    } else if self.pivot_melt_modal.focus == PivotMeltFocus::MeltFilter {
                        let n = self.pivot_melt_modal.melt_filter.chars().count();
                        if self.pivot_melt_modal.melt_filter_cursor < n {
                            self.pivot_melt_modal.melt_filter_cursor += 1;
                        }
                    } else if self.pivot_melt_modal.focus == PivotMeltFocus::MeltPattern {
                        let n = self.pivot_melt_modal.melt_pattern.chars().count();
                        if self.pivot_melt_modal.melt_pattern_cursor < n {
                            self.pivot_melt_modal.melt_pattern_cursor += 1;
                        }
                    } else if self.pivot_melt_modal.focus == PivotMeltFocus::MeltVarName {
                        let n = self.pivot_melt_modal.melt_variable_name.chars().count();
                        if self.pivot_melt_modal.melt_variable_cursor < n {
                            self.pivot_melt_modal.melt_variable_cursor += 1;
                        }
                    } else if self.pivot_melt_modal.focus == PivotMeltFocus::MeltValName {
                        let n = self.pivot_melt_modal.melt_value_name.chars().count();
                        if self.pivot_melt_modal.melt_value_cursor < n {
                            self.pivot_melt_modal.melt_value_cursor += 1;
                        }
                    } else if self.pivot_melt_modal.focus == PivotMeltFocus::TabBar {
                        self.pivot_melt_modal.switch_tab();
                    } else {
                        self.pivot_melt_modal.next_focus();
                    }
                }
                KeyCode::Enter => match self.pivot_melt_modal.focus {
                    PivotMeltFocus::Apply => {
                        return match self.pivot_melt_modal.active_tab {
                            PivotMeltTab::Pivot => {
                                if let Some(err) = self.pivot_melt_modal.pivot_validation_error() {
                                    self.error_modal.show(err);
                                    None
                                } else {
                                    self.pivot_melt_modal
                                        .build_pivot_spec()
                                        .map(AppEvent::Pivot)
                                }
                            }
                            PivotMeltTab::Melt => {
                                if let Some(err) = self.pivot_melt_modal.melt_validation_error() {
                                    self.error_modal.show(err);
                                    None
                                } else {
                                    self.pivot_melt_modal.build_melt_spec().map(AppEvent::Melt)
                                }
                            }
                        };
                    }
                    PivotMeltFocus::Cancel => {
                        self.pivot_melt_modal.close();
                        self.input_mode = InputMode::Normal;
                    }
                    PivotMeltFocus::Clear => {
                        self.pivot_melt_modal.reset_form();
                    }
                    _ => {}
                },
                KeyCode::Up | KeyCode::Char('k') => match self.pivot_melt_modal.focus {
                    PivotMeltFocus::PivotIndexList => {
                        self.pivot_melt_modal.pivot_move_index_selection(false);
                    }
                    PivotMeltFocus::PivotPivotCol => {
                        self.pivot_melt_modal.pivot_move_pivot_selection(false);
                    }
                    PivotMeltFocus::PivotValueCol => {
                        self.pivot_melt_modal.pivot_move_value_selection(false);
                    }
                    PivotMeltFocus::PivotAggregation => {
                        self.pivot_melt_modal.pivot_move_aggregation(false);
                    }
                    PivotMeltFocus::MeltIndexList => {
                        self.pivot_melt_modal.melt_move_index_selection(false);
                    }
                    PivotMeltFocus::MeltStrategy => {
                        self.pivot_melt_modal.melt_move_strategy(false);
                    }
                    PivotMeltFocus::MeltType => {
                        self.pivot_melt_modal.melt_move_type_filter(false);
                    }
                    PivotMeltFocus::MeltExplicitList => {
                        self.pivot_melt_modal.melt_move_explicit_selection(false);
                    }
                    _ => {}
                },
                KeyCode::Down | KeyCode::Char('j') => match self.pivot_melt_modal.focus {
                    PivotMeltFocus::PivotIndexList => {
                        self.pivot_melt_modal.pivot_move_index_selection(true);
                    }
                    PivotMeltFocus::PivotPivotCol => {
                        self.pivot_melt_modal.pivot_move_pivot_selection(true);
                    }
                    PivotMeltFocus::PivotValueCol => {
                        self.pivot_melt_modal.pivot_move_value_selection(true);
                    }
                    PivotMeltFocus::PivotAggregation => {
                        self.pivot_melt_modal.pivot_move_aggregation(true);
                    }
                    PivotMeltFocus::MeltIndexList => {
                        self.pivot_melt_modal.melt_move_index_selection(true);
                    }
                    PivotMeltFocus::MeltStrategy => {
                        self.pivot_melt_modal.melt_move_strategy(true);
                    }
                    PivotMeltFocus::MeltType => {
                        self.pivot_melt_modal.melt_move_type_filter(true);
                    }
                    PivotMeltFocus::MeltExplicitList => {
                        self.pivot_melt_modal.melt_move_explicit_selection(true);
                    }
                    _ => {}
                },
                KeyCode::Char(' ') => match self.pivot_melt_modal.focus {
                    PivotMeltFocus::PivotIndexList => {
                        self.pivot_melt_modal.pivot_toggle_index_at_selection();
                    }
                    PivotMeltFocus::PivotSortToggle => {
                        self.pivot_melt_modal.sort_new_columns =
                            !self.pivot_melt_modal.sort_new_columns;
                    }
                    PivotMeltFocus::MeltIndexList => {
                        self.pivot_melt_modal.melt_toggle_index_at_selection();
                    }
                    PivotMeltFocus::MeltExplicitList => {
                        self.pivot_melt_modal.melt_toggle_explicit_at_selection();
                    }
                    _ => {}
                },
                KeyCode::Home if self.pivot_melt_modal.focus == PivotMeltFocus::PivotFilter => {
                    self.pivot_melt_modal.pivot_filter_cursor = 0;
                }
                KeyCode::End if self.pivot_melt_modal.focus == PivotMeltFocus::PivotFilter => {
                    self.pivot_melt_modal.pivot_filter_cursor =
                        self.pivot_melt_modal.pivot_filter.chars().count();
                }
                KeyCode::Char(c) if self.pivot_melt_modal.focus == PivotMeltFocus::PivotFilter => {
                    let byte_pos: usize = self
                        .pivot_melt_modal
                        .pivot_filter
                        .chars()
                        .take(self.pivot_melt_modal.pivot_filter_cursor)
                        .map(|ch| ch.len_utf8())
                        .sum();
                    self.pivot_melt_modal.pivot_filter.insert(byte_pos, c);
                    self.pivot_melt_modal.pivot_filter_cursor += 1;
                    self.pivot_melt_modal.pivot_index_table.select(None);
                }
                KeyCode::Backspace
                    if self.pivot_melt_modal.focus == PivotMeltFocus::PivotFilter =>
                {
                    if self.pivot_melt_modal.pivot_filter_cursor > 0 {
                        let prev_byte: usize = self
                            .pivot_melt_modal
                            .pivot_filter
                            .chars()
                            .take(self.pivot_melt_modal.pivot_filter_cursor - 1)
                            .map(|ch| ch.len_utf8())
                            .sum();
                        let ch = self.pivot_melt_modal.pivot_filter[prev_byte..]
                            .chars()
                            .next()
                            .unwrap();
                        self.pivot_melt_modal
                            .pivot_filter
                            .drain(prev_byte..prev_byte + ch.len_utf8());
                        self.pivot_melt_modal.pivot_filter_cursor -= 1;
                        self.pivot_melt_modal.pivot_index_table.select(None);
                    }
                }
                KeyCode::Delete if self.pivot_melt_modal.focus == PivotMeltFocus::PivotFilter => {
                    let n = self.pivot_melt_modal.pivot_filter.chars().count();
                    if self.pivot_melt_modal.pivot_filter_cursor < n {
                        let byte_pos: usize = self
                            .pivot_melt_modal
                            .pivot_filter
                            .chars()
                            .take(self.pivot_melt_modal.pivot_filter_cursor)
                            .map(|ch| ch.len_utf8())
                            .sum();
                        let ch = self.pivot_melt_modal.pivot_filter[byte_pos..]
                            .chars()
                            .next()
                            .unwrap();
                        self.pivot_melt_modal
                            .pivot_filter
                            .drain(byte_pos..byte_pos + ch.len_utf8());
                        self.pivot_melt_modal.pivot_index_table.select(None);
                    }
                }
                KeyCode::Home if self.pivot_melt_modal.focus == PivotMeltFocus::MeltFilter => {
                    self.pivot_melt_modal.melt_filter_cursor = 0;
                }
                KeyCode::End if self.pivot_melt_modal.focus == PivotMeltFocus::MeltFilter => {
                    self.pivot_melt_modal.melt_filter_cursor =
                        self.pivot_melt_modal.melt_filter.chars().count();
                }
                KeyCode::Char(c) if self.pivot_melt_modal.focus == PivotMeltFocus::MeltFilter => {
                    let byte_pos: usize = self
                        .pivot_melt_modal
                        .melt_filter
                        .chars()
                        .take(self.pivot_melt_modal.melt_filter_cursor)
                        .map(|ch| ch.len_utf8())
                        .sum();
                    self.pivot_melt_modal.melt_filter.insert(byte_pos, c);
                    self.pivot_melt_modal.melt_filter_cursor += 1;
                    self.pivot_melt_modal.melt_index_table.select(None);
                }
                KeyCode::Backspace if self.pivot_melt_modal.focus == PivotMeltFocus::MeltFilter => {
                    if self.pivot_melt_modal.melt_filter_cursor > 0 {
                        let prev_byte: usize = self
                            .pivot_melt_modal
                            .melt_filter
                            .chars()
                            .take(self.pivot_melt_modal.melt_filter_cursor - 1)
                            .map(|ch| ch.len_utf8())
                            .sum();
                        let ch = self.pivot_melt_modal.melt_filter[prev_byte..]
                            .chars()
                            .next()
                            .unwrap();
                        self.pivot_melt_modal
                            .melt_filter
                            .drain(prev_byte..prev_byte + ch.len_utf8());
                        self.pivot_melt_modal.melt_filter_cursor -= 1;
                        self.pivot_melt_modal.melt_index_table.select(None);
                    }
                }
                KeyCode::Delete if self.pivot_melt_modal.focus == PivotMeltFocus::MeltFilter => {
                    let n = self.pivot_melt_modal.melt_filter.chars().count();
                    if self.pivot_melt_modal.melt_filter_cursor < n {
                        let byte_pos: usize = self
                            .pivot_melt_modal
                            .melt_filter
                            .chars()
                            .take(self.pivot_melt_modal.melt_filter_cursor)
                            .map(|ch| ch.len_utf8())
                            .sum();
                        let ch = self.pivot_melt_modal.melt_filter[byte_pos..]
                            .chars()
                            .next()
                            .unwrap();
                        self.pivot_melt_modal
                            .melt_filter
                            .drain(byte_pos..byte_pos + ch.len_utf8());
                        self.pivot_melt_modal.melt_index_table.select(None);
                    }
                }
                KeyCode::Home if self.pivot_melt_modal.focus == PivotMeltFocus::MeltPattern => {
                    self.pivot_melt_modal.melt_pattern_cursor = 0;
                }
                KeyCode::End if self.pivot_melt_modal.focus == PivotMeltFocus::MeltPattern => {
                    self.pivot_melt_modal.melt_pattern_cursor =
                        self.pivot_melt_modal.melt_pattern.chars().count();
                }
                KeyCode::Char(c) if self.pivot_melt_modal.focus == PivotMeltFocus::MeltPattern => {
                    let byte_pos: usize = self
                        .pivot_melt_modal
                        .melt_pattern
                        .chars()
                        .take(self.pivot_melt_modal.melt_pattern_cursor)
                        .map(|ch| ch.len_utf8())
                        .sum();
                    self.pivot_melt_modal.melt_pattern.insert(byte_pos, c);
                    self.pivot_melt_modal.melt_pattern_cursor += 1;
                }
                KeyCode::Backspace
                    if self.pivot_melt_modal.focus == PivotMeltFocus::MeltPattern =>
                {
                    if self.pivot_melt_modal.melt_pattern_cursor > 0 {
                        let prev_byte: usize = self
                            .pivot_melt_modal
                            .melt_pattern
                            .chars()
                            .take(self.pivot_melt_modal.melt_pattern_cursor - 1)
                            .map(|ch| ch.len_utf8())
                            .sum();
                        let ch = self.pivot_melt_modal.melt_pattern[prev_byte..]
                            .chars()
                            .next()
                            .unwrap();
                        self.pivot_melt_modal
                            .melt_pattern
                            .drain(prev_byte..prev_byte + ch.len_utf8());
                        self.pivot_melt_modal.melt_pattern_cursor -= 1;
                    }
                }
                KeyCode::Delete if self.pivot_melt_modal.focus == PivotMeltFocus::MeltPattern => {
                    let n = self.pivot_melt_modal.melt_pattern.chars().count();
                    if self.pivot_melt_modal.melt_pattern_cursor < n {
                        let byte_pos: usize = self
                            .pivot_melt_modal
                            .melt_pattern
                            .chars()
                            .take(self.pivot_melt_modal.melt_pattern_cursor)
                            .map(|ch| ch.len_utf8())
                            .sum();
                        let ch = self.pivot_melt_modal.melt_pattern[byte_pos..]
                            .chars()
                            .next()
                            .unwrap();
                        self.pivot_melt_modal
                            .melt_pattern
                            .drain(byte_pos..byte_pos + ch.len_utf8());
                    }
                }
                KeyCode::Home if self.pivot_melt_modal.focus == PivotMeltFocus::MeltVarName => {
                    self.pivot_melt_modal.melt_variable_cursor = 0;
                }
                KeyCode::End if self.pivot_melt_modal.focus == PivotMeltFocus::MeltVarName => {
                    self.pivot_melt_modal.melt_variable_cursor =
                        self.pivot_melt_modal.melt_variable_name.chars().count();
                }
                KeyCode::Char(c) if self.pivot_melt_modal.focus == PivotMeltFocus::MeltVarName => {
                    let byte_pos: usize = self
                        .pivot_melt_modal
                        .melt_variable_name
                        .chars()
                        .take(self.pivot_melt_modal.melt_variable_cursor)
                        .map(|ch| ch.len_utf8())
                        .sum();
                    self.pivot_melt_modal.melt_variable_name.insert(byte_pos, c);
                    self.pivot_melt_modal.melt_variable_cursor += 1;
                }
                KeyCode::Backspace
                    if self.pivot_melt_modal.focus == PivotMeltFocus::MeltVarName =>
                {
                    if self.pivot_melt_modal.melt_variable_cursor > 0 {
                        let prev_byte: usize = self
                            .pivot_melt_modal
                            .melt_variable_name
                            .chars()
                            .take(self.pivot_melt_modal.melt_variable_cursor - 1)
                            .map(|ch| ch.len_utf8())
                            .sum();
                        let ch = self.pivot_melt_modal.melt_variable_name[prev_byte..]
                            .chars()
                            .next()
                            .unwrap();
                        self.pivot_melt_modal
                            .melt_variable_name
                            .drain(prev_byte..prev_byte + ch.len_utf8());
                        self.pivot_melt_modal.melt_variable_cursor -= 1;
                    }
                }
                KeyCode::Delete if self.pivot_melt_modal.focus == PivotMeltFocus::MeltVarName => {
                    let n = self.pivot_melt_modal.melt_variable_name.chars().count();
                    if self.pivot_melt_modal.melt_variable_cursor < n {
                        let byte_pos: usize = self
                            .pivot_melt_modal
                            .melt_variable_name
                            .chars()
                            .take(self.pivot_melt_modal.melt_variable_cursor)
                            .map(|ch| ch.len_utf8())
                            .sum();
                        let ch = self.pivot_melt_modal.melt_variable_name[byte_pos..]
                            .chars()
                            .next()
                            .unwrap();
                        self.pivot_melt_modal
                            .melt_variable_name
                            .drain(byte_pos..byte_pos + ch.len_utf8());
                    }
                }
                KeyCode::Home if self.pivot_melt_modal.focus == PivotMeltFocus::MeltValName => {
                    self.pivot_melt_modal.melt_value_cursor = 0;
                }
                KeyCode::End if self.pivot_melt_modal.focus == PivotMeltFocus::MeltValName => {
                    self.pivot_melt_modal.melt_value_cursor =
                        self.pivot_melt_modal.melt_value_name.chars().count();
                }
                KeyCode::Char(c) if self.pivot_melt_modal.focus == PivotMeltFocus::MeltValName => {
                    let byte_pos: usize = self
                        .pivot_melt_modal
                        .melt_value_name
                        .chars()
                        .take(self.pivot_melt_modal.melt_value_cursor)
                        .map(|ch| ch.len_utf8())
                        .sum();
                    self.pivot_melt_modal.melt_value_name.insert(byte_pos, c);
                    self.pivot_melt_modal.melt_value_cursor += 1;
                }
                KeyCode::Backspace
                    if self.pivot_melt_modal.focus == PivotMeltFocus::MeltValName =>
                {
                    if self.pivot_melt_modal.melt_value_cursor > 0 {
                        let prev_byte: usize = self
                            .pivot_melt_modal
                            .melt_value_name
                            .chars()
                            .take(self.pivot_melt_modal.melt_value_cursor - 1)
                            .map(|ch| ch.len_utf8())
                            .sum();
                        let ch = self.pivot_melt_modal.melt_value_name[prev_byte..]
                            .chars()
                            .next()
                            .unwrap();
                        self.pivot_melt_modal
                            .melt_value_name
                            .drain(prev_byte..prev_byte + ch.len_utf8());
                        self.pivot_melt_modal.melt_value_cursor -= 1;
                    }
                }
                KeyCode::Delete if self.pivot_melt_modal.focus == PivotMeltFocus::MeltValName => {
                    let n = self.pivot_melt_modal.melt_value_name.chars().count();
                    if self.pivot_melt_modal.melt_value_cursor < n {
                        let byte_pos: usize = self
                            .pivot_melt_modal
                            .melt_value_name
                            .chars()
                            .take(self.pivot_melt_modal.melt_value_cursor)
                            .map(|ch| ch.len_utf8())
                            .sum();
                        let ch = self.pivot_melt_modal.melt_value_name[byte_pos..]
                            .chars()
                            .next()
                            .unwrap();
                        self.pivot_melt_modal
                            .melt_value_name
                            .drain(byte_pos..byte_pos + ch.len_utf8());
                    }
                }
                _ => {}
            }
            return None;
        }

        if self.analysis_modal.active {
            match event.code {
                KeyCode::Esc => {
                    if self.analysis_modal.show_help {
                        self.analysis_modal.show_help = false;
                    } else if self.analysis_modal.view != analysis_modal::AnalysisView::Main {
                        // Close detail view
                        self.analysis_modal.close_detail();
                    } else {
                        self.analysis_modal.close();
                    }
                }
                KeyCode::Char('h') if event.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.analysis_modal.show_help = !self.analysis_modal.show_help;
                }
                KeyCode::Char('r') => {
                    self.analysis_modal.recalculate();
                    // Clear cached results to trigger recomputation
                    self.analysis_modal.analysis_results = None;
                }
                KeyCode::Tab => {
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main {
                        // Switch focus between main area and sidebar
                        self.analysis_modal.switch_focus();
                    } else if self.analysis_modal.view
                        == analysis_modal::AnalysisView::DistributionDetail
                    {
                        // In distribution detail view, only the distribution selector is focusable
                        // Tab does nothing - focus stays on the distribution selector
                    } else {
                        // In other detail views, Tab cycles through sections
                        self.analysis_modal.next_detail_section();
                    }
                }
                KeyCode::Enter => {
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main {
                        if self.analysis_modal.focus == analysis_modal::AnalysisFocus::Sidebar {
                            // Select tool from sidebar
                            self.analysis_modal.select_tool();
                        } else {
                            // Enter in main area opens detail view if applicable
                            match self.analysis_modal.selected_tool {
                                analysis_modal::AnalysisTool::DistributionAnalysis => {
                                    self.analysis_modal.open_distribution_detail();
                                }
                                analysis_modal::AnalysisTool::CorrelationMatrix => {
                                    self.analysis_modal.open_correlation_detail();
                                }
                                _ => {} // Describe tool doesn't have detail view
                            }
                        }
                    }
                    // Enter key no longer needed for distribution selection - charts update on navigation
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    match self.analysis_modal.view {
                        analysis_modal::AnalysisView::Main => {
                            match self.analysis_modal.focus {
                                analysis_modal::AnalysisFocus::Sidebar => {
                                    // Navigate sidebar tool list
                                    self.analysis_modal.next_tool();
                                }
                                analysis_modal::AnalysisFocus::Main => {
                                    // Navigate in main area based on selected tool
                                    match self.analysis_modal.selected_tool {
                                        analysis_modal::AnalysisTool::Describe => {
                                            if let Some(state) = &self.data_table_state {
                                                let max_rows = state.schema.len();
                                                self.analysis_modal.next_row(max_rows);
                                            }
                                        }
                                        analysis_modal::AnalysisTool::DistributionAnalysis => {
                                            if let Some(results) =
                                                &self.analysis_modal.analysis_results
                                            {
                                                let max_rows = results.distribution_analyses.len();
                                                self.analysis_modal.next_row(max_rows);
                                            }
                                        }
                                        analysis_modal::AnalysisTool::CorrelationMatrix => {
                                            if let Some(results) =
                                                &self.analysis_modal.analysis_results
                                            {
                                                if let Some(corr) = &results.correlation_matrix {
                                                    let max_rows = corr.columns.len();
                                                    // Calculate visible columns (same logic as horizontal moves)
                                                    let row_header_width = 20u16;
                                                    let cell_width = 12u16;
                                                    let column_spacing = 1u16;
                                                    let estimated_width = 80u16;
                                                    let available_width = estimated_width
                                                        .saturating_sub(row_header_width);
                                                    let mut calculated_visible = 0usize;
                                                    let mut used = 0u16;
                                                    let max_cols = corr.columns.len();
                                                    loop {
                                                        let needed = if calculated_visible == 0 {
                                                            cell_width
                                                        } else {
                                                            column_spacing + cell_width
                                                        };
                                                        if used + needed <= available_width
                                                            && calculated_visible < max_cols
                                                        {
                                                            used += needed;
                                                            calculated_visible += 1;
                                                        } else {
                                                            break;
                                                        }
                                                    }
                                                    let visible_cols =
                                                        calculated_visible.max(1).min(max_cols);
                                                    self.analysis_modal.move_correlation_cell(
                                                        (1, 0),
                                                        max_rows,
                                                        max_rows,
                                                        visible_cols,
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                        analysis_modal::AnalysisView::DistributionDetail => {
                            if self.analysis_modal.focus
                                == analysis_modal::AnalysisFocus::DistributionSelector
                            {
                                self.analysis_modal.next_distribution();
                            }
                        }
                        _ => {}
                    }
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main {
                        self.analysis_modal.previous_row();
                    } else if self.analysis_modal.view
                        == analysis_modal::AnalysisView::DistributionDetail
                        && self.analysis_modal.focus
                            == analysis_modal::AnalysisFocus::DistributionSelector
                    {
                        self.analysis_modal.previous_distribution();
                    }
                }
                KeyCode::Left | KeyCode::Char('h')
                    if !event.modifiers.contains(KeyModifiers::CONTROL) =>
                {
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main {
                        match self.analysis_modal.focus {
                            analysis_modal::AnalysisFocus::Sidebar => {
                                // Sidebar navigation handled by Up/Down
                            }
                            analysis_modal::AnalysisFocus::DistributionSelector => {
                                // Distribution selector navigation handled by Up/Down
                            }
                            analysis_modal::AnalysisFocus::Main => {
                                match self.analysis_modal.selected_tool {
                                    analysis_modal::AnalysisTool::Describe => {
                                        self.analysis_modal.scroll_left();
                                    }
                                    analysis_modal::AnalysisTool::DistributionAnalysis => {
                                        self.analysis_modal.scroll_left();
                                    }
                                    analysis_modal::AnalysisTool::CorrelationMatrix => {
                                        if let Some(results) = &self.analysis_modal.analysis_results
                                        {
                                            if let Some(corr) = &results.correlation_matrix {
                                                let max_cols = corr.columns.len();
                                                // Calculate visible columns using same logic as render function
                                                // This matches the render_correlation_matrix calculation
                                                let row_header_width = 20u16;
                                                let cell_width = 12u16;
                                                let column_spacing = 1u16;
                                                // Use a conservative estimate for available width
                                                // In practice, main_area.width would be available, but we don't have access here
                                                // Using a reasonable default that works for most terminals
                                                let estimated_width = 80u16; // Conservative estimate (most terminals are 80+ wide)
                                                let available_width = estimated_width
                                                    .saturating_sub(row_header_width);
                                                // Match render logic: first column has no spacing, subsequent ones do
                                                let mut calculated_visible = 0usize;
                                                let mut used = 0u16;
                                                loop {
                                                    let needed = if calculated_visible == 0 {
                                                        cell_width
                                                    } else {
                                                        column_spacing + cell_width
                                                    };
                                                    if used + needed <= available_width
                                                        && calculated_visible < max_cols
                                                    {
                                                        used += needed;
                                                        calculated_visible += 1;
                                                    } else {
                                                        break;
                                                    }
                                                }
                                                let visible_cols =
                                                    calculated_visible.max(1).min(max_cols);
                                                self.analysis_modal.move_correlation_cell(
                                                    (0, -1),
                                                    max_cols,
                                                    max_cols,
                                                    visible_cols,
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                KeyCode::Right | KeyCode::Char('l') => {
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main {
                        match self.analysis_modal.focus {
                            analysis_modal::AnalysisFocus::Sidebar => {
                                // Sidebar navigation handled by Up/Down
                            }
                            analysis_modal::AnalysisFocus::DistributionSelector => {
                                // Distribution selector navigation handled by Up/Down
                            }
                            analysis_modal::AnalysisFocus::Main => {
                                match self.analysis_modal.selected_tool {
                                    analysis_modal::AnalysisTool::Describe => {
                                        // Number of statistics: count, null_count, mean, std, min, 25%, 50%, 75%, max, skewness, kurtosis, distribution
                                        let max_stats = 12;
                                        // Estimate visible stats based on terminal width (rough estimate)
                                        let visible_stats = 8; // Will be calculated more accurately in widget
                                        self.analysis_modal.scroll_right(max_stats, visible_stats);
                                    }
                                    analysis_modal::AnalysisTool::DistributionAnalysis => {
                                        // Number of statistics: Distribution, P-value, Shapiro-Wilk, SW p-value, CV, Outliers, Skewness, Kurtosis
                                        let max_stats = 8;
                                        // Estimate visible stats based on terminal width (rough estimate)
                                        let visible_stats = 6; // Will be calculated more accurately in widget
                                        self.analysis_modal.scroll_right(max_stats, visible_stats);
                                    }
                                    analysis_modal::AnalysisTool::CorrelationMatrix => {
                                        if let Some(results) = &self.analysis_modal.analysis_results
                                        {
                                            if let Some(corr) = &results.correlation_matrix {
                                                let max_cols = corr.columns.len();
                                                // Calculate visible columns using same logic as render function
                                                let row_header_width = 20u16;
                                                let cell_width = 12u16;
                                                let column_spacing = 1u16;
                                                let estimated_width = 80u16; // Conservative estimate
                                                let available_width = estimated_width
                                                    .saturating_sub(row_header_width);
                                                let mut calculated_visible = 0usize;
                                                let mut used = 0u16;
                                                loop {
                                                    let needed = if calculated_visible == 0 {
                                                        cell_width
                                                    } else {
                                                        column_spacing + cell_width
                                                    };
                                                    if used + needed <= available_width
                                                        && calculated_visible < max_cols
                                                    {
                                                        used += needed;
                                                        calculated_visible += 1;
                                                    } else {
                                                        break;
                                                    }
                                                }
                                                let visible_cols =
                                                    calculated_visible.max(1).min(max_cols);
                                                self.analysis_modal.move_correlation_cell(
                                                    (0, 1),
                                                    max_cols,
                                                    max_cols,
                                                    visible_cols,
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                KeyCode::PageDown => {
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main
                        && self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main
                    {
                        match self.analysis_modal.selected_tool {
                            analysis_modal::AnalysisTool::Describe => {
                                if let Some(state) = &self.data_table_state {
                                    let max_rows = state.schema.len();
                                    let page_size = 10;
                                    self.analysis_modal.page_down(max_rows, page_size);
                                }
                            }
                            analysis_modal::AnalysisTool::DistributionAnalysis => {
                                if let Some(results) = &self.analysis_modal.analysis_results {
                                    let max_rows = results.distribution_analyses.len();
                                    let page_size = 10;
                                    self.analysis_modal.page_down(max_rows, page_size);
                                }
                            }
                            analysis_modal::AnalysisTool::CorrelationMatrix => {
                                if let Some(results) = &self.analysis_modal.analysis_results {
                                    if let Some(corr) = &results.correlation_matrix {
                                        let max_rows = corr.columns.len();
                                        let page_size = 10;
                                        self.analysis_modal.page_down(max_rows, page_size);
                                    }
                                }
                            }
                        }
                    }
                }
                KeyCode::PageUp => {
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main
                        && self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main
                    {
                        let page_size = 10;
                        self.analysis_modal.page_up(page_size);
                    }
                }
                KeyCode::Home => {
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main {
                        match self.analysis_modal.focus {
                            analysis_modal::AnalysisFocus::Sidebar => {
                                self.analysis_modal.sidebar_state.select(Some(0));
                            }
                            analysis_modal::AnalysisFocus::DistributionSelector => {
                                self.analysis_modal
                                    .distribution_selector_state
                                    .select(Some(0));
                            }
                            analysis_modal::AnalysisFocus::Main => {
                                match self.analysis_modal.selected_tool {
                                    analysis_modal::AnalysisTool::Describe => {
                                        self.analysis_modal.table_state.select(Some(0));
                                    }
                                    analysis_modal::AnalysisTool::DistributionAnalysis => {
                                        self.analysis_modal
                                            .distribution_table_state
                                            .select(Some(0));
                                    }
                                    analysis_modal::AnalysisTool::CorrelationMatrix => {
                                        self.analysis_modal.correlation_table_state.select(Some(0));
                                        self.analysis_modal.selected_correlation = Some((0, 0));
                                    }
                                }
                            }
                        }
                    }
                }
                KeyCode::End => {
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main {
                        match self.analysis_modal.focus {
                            analysis_modal::AnalysisFocus::Sidebar => {
                                self.analysis_modal.sidebar_state.select(Some(2));
                                // Last tool
                            }
                            analysis_modal::AnalysisFocus::DistributionSelector => {
                                self.analysis_modal
                                    .distribution_selector_state
                                    .select(Some(13)); // Last distribution (Weibull, index 13 of 14 total)
                            }
                            analysis_modal::AnalysisFocus::Main => {
                                match self.analysis_modal.selected_tool {
                                    analysis_modal::AnalysisTool::Describe => {
                                        if let Some(state) = &self.data_table_state {
                                            let max_rows = state.schema.len();
                                            if max_rows > 0 {
                                                self.analysis_modal
                                                    .table_state
                                                    .select(Some(max_rows - 1));
                                            }
                                        }
                                    }
                                    analysis_modal::AnalysisTool::DistributionAnalysis => {
                                        if let Some(results) = &self.analysis_modal.analysis_results
                                        {
                                            let max_rows = results.distribution_analyses.len();
                                            if max_rows > 0 {
                                                self.analysis_modal
                                                    .distribution_table_state
                                                    .select(Some(max_rows - 1));
                                            }
                                        }
                                    }
                                    analysis_modal::AnalysisTool::CorrelationMatrix => {
                                        if let Some(results) = &self.analysis_modal.analysis_results
                                        {
                                            if let Some(corr) = &results.correlation_matrix {
                                                let max_rows = corr.columns.len();
                                                if max_rows > 0 {
                                                    self.analysis_modal
                                                        .correlation_table_state
                                                        .select(Some(max_rows - 1));
                                                    self.analysis_modal.selected_correlation =
                                                        Some((max_rows - 1, max_rows - 1));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
            return None;
        }

        if self.template_modal.active {
            match event.code {
                KeyCode::Esc => {
                    if self.template_modal.show_score_details {
                        // Close score details popup
                        self.template_modal.show_score_details = false;
                    } else if self.template_modal.delete_confirm {
                        // Cancel delete confirmation
                        self.template_modal.delete_confirm = false;
                    } else if self.template_modal.mode == TemplateModalMode::Create
                        || self.template_modal.mode == TemplateModalMode::Edit
                    {
                        // In create/edit mode, Esc goes back to list mode
                        self.template_modal.exit_create_mode();
                    } else {
                        // In list mode, Esc closes modal
                        if self.template_modal.show_help {
                            self.template_modal.show_help = false;
                        } else {
                            self.template_modal.active = false;
                            self.template_modal.show_help = false;
                            self.template_modal.delete_confirm = false;
                        }
                    }
                }
                KeyCode::BackTab if self.template_modal.delete_confirm => {
                    // Toggle between Cancel and Delete buttons (reverse)
                    self.template_modal.delete_confirm_focus =
                        !self.template_modal.delete_confirm_focus;
                }
                KeyCode::Tab if !self.template_modal.delete_confirm => {
                    self.template_modal.next_focus();
                }
                KeyCode::BackTab => {
                    self.template_modal.prev_focus();
                }
                KeyCode::Char('s') if self.template_modal.mode == TemplateModalMode::List => {
                    // Switch to create mode from list mode
                    self.template_modal.enter_create_mode();
                    // Auto-populate fields
                    if let Some(ref path) = self.path {
                        // Auto-populate name
                        self.template_modal.create_name =
                            self.template_manager.generate_next_template_name();
                        self.template_modal.create_name_cursor =
                            self.template_modal.create_name.len();

                        // Auto-populate exact_path (absolute) - canonicalize to ensure absolute path
                        let absolute_path = if path.is_absolute() {
                            path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
                        } else {
                            // If relative, make it absolute from current dir
                            if let Ok(cwd) = std::env::current_dir() {
                                let abs = cwd.join(path);
                                abs.canonicalize().unwrap_or(abs)
                            } else {
                                path.to_path_buf()
                            }
                        };
                        self.template_modal.create_exact_path =
                            absolute_path.to_string_lossy().to_string();
                        self.template_modal.create_exact_path_cursor =
                            self.template_modal.create_exact_path.len();

                        // Auto-populate relative_path from current working directory
                        if let Ok(cwd) = std::env::current_dir() {
                            let abs_path = if path.is_absolute() {
                                path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
                            } else {
                                let abs = cwd.join(path);
                                abs.canonicalize().unwrap_or(abs)
                            };
                            if let Ok(canonical_cwd) = cwd.canonicalize() {
                                if let Ok(rel_path) = abs_path.strip_prefix(&canonical_cwd) {
                                    // Ensure relative path starts with ./ or just the path
                                    let rel_str = rel_path.to_string_lossy().to_string();
                                    self.template_modal.create_relative_path =
                                        rel_str.strip_prefix('/').unwrap_or(&rel_str).to_string();
                                    self.template_modal.create_relative_path_cursor =
                                        self.template_modal.create_relative_path.len();
                                } else {
                                    // Path is not under CWD, leave empty or use full path
                                    self.template_modal.create_relative_path.clear();
                                    self.template_modal.create_relative_path_cursor = 0;
                                }
                            } else {
                                // Fallback: try without canonicalization
                                if let Ok(rel_path) = abs_path.strip_prefix(&cwd) {
                                    let rel_str = rel_path.to_string_lossy().to_string();
                                    self.template_modal.create_relative_path =
                                        rel_str.strip_prefix('/').unwrap_or(&rel_str).to_string();
                                    self.template_modal.create_relative_path_cursor =
                                        self.template_modal.create_relative_path.len();
                                } else {
                                    self.template_modal.create_relative_path.clear();
                                    self.template_modal.create_relative_path_cursor = 0;
                                }
                            }
                        } else {
                            self.template_modal.create_relative_path.clear();
                            self.template_modal.create_relative_path_cursor = 0;
                        }

                        // Suggest path pattern
                        if let Some(parent) = path.parent() {
                            if let Some(parent_str) = parent.to_str() {
                                if path.file_name().is_some() {
                                    if let Some(ext) = path.extension() {
                                        self.template_modal.create_path_pattern =
                                            format!("{}/*.{}", parent_str, ext.to_string_lossy());
                                    }
                                }
                            }
                        }

                        // Suggest filename pattern
                        if let Some(filename) = path.file_name() {
                            if let Some(filename_str) = filename.to_str() {
                                // Try to create a pattern by replacing numbers/dates with *
                                let mut pattern = filename_str.to_string();
                                // Simple heuristic: replace sequences of digits with *
                                use regex::Regex;
                                if let Ok(re) = Regex::new(r"\d+") {
                                    pattern = re.replace_all(&pattern, "*").to_string();
                                }
                                self.template_modal.create_filename_pattern = pattern;
                            }
                        }
                    }

                    // Suggest schema match
                    if let Some(ref state) = self.data_table_state {
                        if !state.schema.is_empty() {
                            self.template_modal.create_schema_match_enabled = false;
                            // Not auto-enabled, just suggested
                        }
                    }
                }
                KeyCode::Char('e') if self.template_modal.mode == TemplateModalMode::List => {
                    // Edit selected template
                    if let Some(idx) = self.template_modal.table_state.selected() {
                        if let Some((template, _)) = self.template_modal.templates.get(idx) {
                            let template_clone = template.clone();
                            self.template_modal.enter_edit_mode(&template_clone);
                        }
                    }
                }
                KeyCode::Char('d')
                    if self.template_modal.mode == TemplateModalMode::List
                        && !self.template_modal.delete_confirm =>
                {
                    // Show delete confirmation
                    if let Some(_idx) = self.template_modal.table_state.selected() {
                        self.template_modal.delete_confirm = true;
                        self.template_modal.delete_confirm_focus = false; // Cancel is default
                    }
                }
                KeyCode::Char('?')
                    if self.template_modal.mode == TemplateModalMode::List
                        && !self.template_modal.delete_confirm =>
                {
                    // Show score details popup
                    self.template_modal.show_score_details = true;
                }
                KeyCode::Char('D') if self.template_modal.delete_confirm => {
                    // Delete with capital D
                    if let Some(idx) = self.template_modal.table_state.selected() {
                        if let Some((template, _)) = self.template_modal.templates.get(idx) {
                            if let Err(e) = self.template_manager.delete_template(&template.id) {
                                eprintln!("Error deleting template: {}", e);
                            } else {
                                // Reload templates
                                if let Some(ref state) = self.data_table_state {
                                    if let Some(ref path) = self.path {
                                        self.template_modal.templates = self
                                            .template_manager
                                            .find_relevant_templates(path, &state.schema);
                                        if !self.template_modal.templates.is_empty() {
                                            let new_idx = idx.min(
                                                self.template_modal
                                                    .templates
                                                    .len()
                                                    .saturating_sub(1),
                                            );
                                            self.template_modal.table_state.select(Some(new_idx));
                                        } else {
                                            self.template_modal.table_state.select(None);
                                        }
                                    }
                                }
                            }
                            self.template_modal.delete_confirm = false;
                        }
                    }
                }
                KeyCode::Tab if self.template_modal.delete_confirm => {
                    // Toggle between Cancel and Delete buttons
                    self.template_modal.delete_confirm_focus =
                        !self.template_modal.delete_confirm_focus;
                }
                KeyCode::Enter if self.template_modal.delete_confirm => {
                    // Enter cancels by default (Cancel is selected)
                    if self.template_modal.delete_confirm_focus {
                        // Delete button is selected
                        if let Some(idx) = self.template_modal.table_state.selected() {
                            if let Some((template, _)) = self.template_modal.templates.get(idx) {
                                if let Err(e) = self.template_manager.delete_template(&template.id)
                                {
                                    eprintln!("Error deleting template: {}", e);
                                } else {
                                    // Reload templates
                                    if let Some(ref state) = self.data_table_state {
                                        if let Some(ref path) = self.path {
                                            self.template_modal.templates = self
                                                .template_manager
                                                .find_relevant_templates(path, &state.schema);
                                            if !self.template_modal.templates.is_empty() {
                                                let new_idx = idx.min(
                                                    self.template_modal
                                                        .templates
                                                        .len()
                                                        .saturating_sub(1),
                                                );
                                                self.template_modal
                                                    .table_state
                                                    .select(Some(new_idx));
                                            } else {
                                                self.template_modal.table_state.select(None);
                                            }
                                        }
                                    }
                                }
                                self.template_modal.delete_confirm = false;
                            }
                        }
                    } else {
                        // Cancel button is selected (default)
                        self.template_modal.delete_confirm = false;
                    }
                }
                KeyCode::Enter => {
                    match self.template_modal.mode {
                        TemplateModalMode::List => {
                            match self.template_modal.focus {
                                TemplateFocus::TemplateList => {
                                    // Apply selected template
                                    let template_idx = self.template_modal.table_state.selected();
                                    if let Some(idx) = template_idx {
                                        if let Some((template, _)) =
                                            self.template_modal.templates.get(idx)
                                        {
                                            let template_clone = template.clone();
                                            if let Err(e) = self.apply_template(&template_clone) {
                                                // Show error modal instead of just printing
                                                self.error_modal.show(format!(
                                                    "Error applying template: {}",
                                                    e
                                                ));
                                                // Keep template modal open so user can see what failed
                                            } else {
                                                // Only close template modal on success
                                                self.template_modal.active = false;
                                            }
                                        }
                                    }
                                }
                                TemplateFocus::CreateButton => {
                                    // Same as 's' key - enter create mode
                                    // (handled by 's' key handler above)
                                }
                                _ => {}
                            }
                        }
                        TemplateModalMode::Create | TemplateModalMode::Edit => {
                            // If in description field, Enter adds a newline instead of moving to next field
                            if self.template_modal.create_focus == CreateFocus::Description {
                                let cursor = self.template_modal.create_description_cursor;
                                self.template_modal.create_description.insert(cursor, '\n');
                                self.template_modal.create_description_cursor += 1;
                                // Auto-scroll
                                let cursor_line = self
                                    .template_modal
                                    .create_description
                                    .chars()
                                    .take(cursor)
                                    .filter(|&c| c == '\n')
                                    .count();
                                if cursor_line >= self.template_modal.description_scroll + 2 {
                                    self.template_modal.description_scroll =
                                        cursor_line.saturating_sub(1);
                                }
                                return None;
                            }
                            match self.template_modal.create_focus {
                                CreateFocus::SaveButton => {
                                    // Validate name
                                    self.template_modal.name_error = None;
                                    if self.template_modal.create_name.trim().is_empty() {
                                        self.template_modal.name_error =
                                            Some("(required)".to_string());
                                        self.template_modal.create_focus = CreateFocus::Name;
                                        return None;
                                    }

                                    // Check for duplicate name (only if creating new, not editing)
                                    if self.template_modal.editing_template_id.is_none()
                                        && self
                                            .template_manager
                                            .template_exists(self.template_modal.create_name.trim())
                                    {
                                        self.template_modal.name_error =
                                            Some("(name already exists)".to_string());
                                        self.template_modal.create_focus = CreateFocus::Name;
                                        return None;
                                    }

                                    // Create template from current state
                                    let match_criteria = template::MatchCriteria {
                                        exact_path: if !self
                                            .template_modal
                                            .create_exact_path
                                            .trim()
                                            .is_empty()
                                        {
                                            Some(std::path::PathBuf::from(
                                                self.template_modal.create_exact_path.trim(),
                                            ))
                                        } else {
                                            None
                                        },
                                        relative_path: if !self
                                            .template_modal
                                            .create_relative_path
                                            .trim()
                                            .is_empty()
                                        {
                                            Some(
                                                self.template_modal
                                                    .create_relative_path
                                                    .trim()
                                                    .to_string(),
                                            )
                                        } else {
                                            None
                                        },
                                        path_pattern: if !self
                                            .template_modal
                                            .create_path_pattern
                                            .is_empty()
                                        {
                                            Some(self.template_modal.create_path_pattern.clone())
                                        } else {
                                            None
                                        },
                                        filename_pattern: if !self
                                            .template_modal
                                            .create_filename_pattern
                                            .is_empty()
                                        {
                                            Some(
                                                self.template_modal.create_filename_pattern.clone(),
                                            )
                                        } else {
                                            None
                                        },
                                        schema_columns: if self
                                            .template_modal
                                            .create_schema_match_enabled
                                        {
                                            self.data_table_state.as_ref().map(|state| {
                                                state
                                                    .schema
                                                    .iter_names()
                                                    .map(|s| s.to_string())
                                                    .collect()
                                            })
                                        } else {
                                            None
                                        },
                                        schema_types: None, // Can be enhanced later
                                    };

                                    let description =
                                        if !self.template_modal.create_description.is_empty() {
                                            Some(self.template_modal.create_description.clone())
                                        } else {
                                            None
                                        };

                                    if let Some(ref editing_id) =
                                        self.template_modal.editing_template_id
                                    {
                                        // Update existing template
                                        if let Some(mut template) = self
                                            .template_manager
                                            .get_template_by_id(editing_id)
                                            .cloned()
                                        {
                                            template.name =
                                                self.template_modal.create_name.trim().to_string();
                                            template.description = description;
                                            template.match_criteria = match_criteria;
                                            // Update settings from current state
                                            if let Some(state) = &self.data_table_state {
                                                template.settings = template::TemplateSettings {
                                                    query: if state.get_active_query().is_empty() {
                                                        None
                                                    } else {
                                                        Some(state.get_active_query().to_string())
                                                    },
                                                    filters: state.get_filters().to_vec(),
                                                    sort_columns: state.get_sort_columns().to_vec(),
                                                    sort_ascending: state.get_sort_ascending(),
                                                    column_order: state.get_column_order().to_vec(),
                                                    locked_columns_count: state
                                                        .locked_columns_count(),
                                                };
                                            }

                                            match self.template_manager.update_template(&template) {
                                                Ok(_) => {
                                                    // Reload templates and go back to list mode
                                                    if let Some(ref state) = self.data_table_state {
                                                        if let Some(ref path) = self.path {
                                                            self.template_modal.templates = self
                                                                .template_manager
                                                                .find_relevant_templates(
                                                                    path,
                                                                    &state.schema,
                                                                );
                                                            self.template_modal.table_state.select(
                                                                if self
                                                                    .template_modal
                                                                    .templates
                                                                    .is_empty()
                                                                {
                                                                    None
                                                                } else {
                                                                    Some(0)
                                                                },
                                                            );
                                                        }
                                                    }
                                                    self.template_modal.exit_create_mode();
                                                }
                                                Err(e) => {
                                                    eprintln!("Error updating template: {}", e);
                                                }
                                            }
                                        }
                                    } else {
                                        // Create new template
                                        match self.create_template_from_current_state(
                                            self.template_modal.create_name.trim().to_string(),
                                            description,
                                            match_criteria,
                                        ) {
                                            Ok(_) => {
                                                // Reload templates and go back to list mode
                                                if let Some(ref state) = self.data_table_state {
                                                    if let Some(ref path) = self.path {
                                                        self.template_modal.templates = self
                                                            .template_manager
                                                            .find_relevant_templates(
                                                                path,
                                                                &state.schema,
                                                            );
                                                        self.template_modal.table_state.select(
                                                            if self
                                                                .template_modal
                                                                .templates
                                                                .is_empty()
                                                            {
                                                                None
                                                            } else {
                                                                Some(0)
                                                            },
                                                        );
                                                    }
                                                }
                                                self.template_modal.exit_create_mode();
                                            }
                                            Err(e) => {
                                                eprintln!("Error creating template: {}", e);
                                            }
                                        }
                                    }
                                }
                                CreateFocus::CancelButton => {
                                    self.template_modal.exit_create_mode();
                                }
                                _ => {
                                    // Move to next field
                                    self.template_modal.next_focus();
                                }
                            }
                        }
                    }
                }
                KeyCode::Up => {
                    match self.template_modal.mode {
                        TemplateModalMode::List => {
                            if self.template_modal.focus == TemplateFocus::TemplateList {
                                let i = match self.template_modal.table_state.selected() {
                                    Some(i) => {
                                        if i == 0 {
                                            self.template_modal.templates.len().saturating_sub(1)
                                        } else {
                                            i - 1
                                        }
                                    }
                                    None => 0,
                                };
                                self.template_modal.table_state.select(Some(i));
                            }
                        }
                        TemplateModalMode::Create | TemplateModalMode::Edit => {
                            // If in description field, move cursor up one line
                            if self.template_modal.create_focus == CreateFocus::Description {
                                let lines: Vec<&str> =
                                    self.template_modal.create_description.lines().collect();
                                let cursor_line = self
                                    .template_modal
                                    .create_description
                                    .chars()
                                    .take(self.template_modal.create_description_cursor)
                                    .filter(|&c| c == '\n')
                                    .count();

                                if cursor_line > 0 {
                                    // Move to previous line
                                    let prev_line_idx = cursor_line - 1;
                                    let prev_line = lines.get(prev_line_idx).unwrap_or(&"");

                                    // Calculate position in previous line (try to maintain column position)
                                    let _current_line = lines.get(cursor_line).unwrap_or(&"");
                                    let current_col = {
                                        let chars_before_cursor: Vec<char> = self
                                            .template_modal
                                            .create_description
                                            .chars()
                                            .take(self.template_modal.create_description_cursor)
                                            .collect();
                                        let last_newline_pos = chars_before_cursor
                                            .iter()
                                            .rposition(|&c| c == '\n')
                                            .map(|p| p + 1)
                                            .unwrap_or(0);
                                        chars_before_cursor.len() - last_newline_pos
                                    };

                                    // Calculate new cursor position
                                    let new_col = current_col.min(prev_line.chars().count());
                                    let mut new_cursor = 0;
                                    for (i, line) in lines.iter().enumerate() {
                                        if i < prev_line_idx {
                                            new_cursor += line.chars().count() + 1;
                                        // +1 for newline
                                        } else if i == prev_line_idx {
                                            new_cursor += new_col;
                                            break;
                                        }
                                    }

                                    self.template_modal.create_description_cursor = new_cursor;

                                    // Auto-scroll to keep cursor visible
                                    if cursor_line - 1 < self.template_modal.description_scroll {
                                        self.template_modal.description_scroll = cursor_line - 1;
                                    }
                                } else {
                                    // Already at first line, move to previous field
                                    self.template_modal.prev_focus();
                                }
                            } else {
                                // Move to previous field (works for all fields)
                                self.template_modal.prev_focus();
                            }
                        }
                    }
                }
                KeyCode::Down => {
                    match self.template_modal.mode {
                        TemplateModalMode::List => {
                            if self.template_modal.focus == TemplateFocus::TemplateList {
                                let i = match self.template_modal.table_state.selected() {
                                    Some(i) => {
                                        if i >= self
                                            .template_modal
                                            .templates
                                            .len()
                                            .saturating_sub(1)
                                        {
                                            0
                                        } else {
                                            i + 1
                                        }
                                    }
                                    None => 0,
                                };
                                self.template_modal.table_state.select(Some(i));
                            }
                        }
                        TemplateModalMode::Create | TemplateModalMode::Edit => {
                            // If in description field, move cursor down one line
                            if self.template_modal.create_focus == CreateFocus::Description {
                                let lines: Vec<&str> =
                                    self.template_modal.create_description.lines().collect();
                                let cursor_line = self
                                    .template_modal
                                    .create_description
                                    .chars()
                                    .take(self.template_modal.create_description_cursor)
                                    .filter(|&c| c == '\n')
                                    .count();

                                if cursor_line < lines.len().saturating_sub(1) {
                                    // Move to next line
                                    let next_line_idx = cursor_line + 1;
                                    let next_line = lines.get(next_line_idx).unwrap_or(&"");

                                    // Calculate position in current line (to maintain column position)
                                    let _current_line = lines.get(cursor_line).unwrap_or(&"");
                                    let current_col = {
                                        let chars_before_cursor: Vec<char> = self
                                            .template_modal
                                            .create_description
                                            .chars()
                                            .take(self.template_modal.create_description_cursor)
                                            .collect();
                                        let last_newline_pos = chars_before_cursor
                                            .iter()
                                            .rposition(|&c| c == '\n')
                                            .map(|p| p + 1)
                                            .unwrap_or(0);
                                        chars_before_cursor.len() - last_newline_pos
                                    };

                                    // Calculate new cursor position
                                    let new_col = current_col.min(next_line.chars().count());
                                    let mut new_cursor = 0;
                                    for (i, line) in lines.iter().enumerate() {
                                        if i < next_line_idx {
                                            new_cursor += line.chars().count() + 1;
                                        // +1 for newline
                                        } else if i == next_line_idx {
                                            new_cursor += new_col;
                                            break;
                                        }
                                    }

                                    self.template_modal.create_description_cursor = new_cursor;

                                    // Auto-scroll to keep cursor visible
                                    let available_height = 6;
                                    if next_line_idx
                                        >= self.template_modal.description_scroll + available_height
                                    {
                                        self.template_modal.description_scroll =
                                            next_line_idx.saturating_sub(available_height - 1);
                                    }
                                } else {
                                    // Already at last line, move to next field
                                    self.template_modal.next_focus();
                                }
                            } else {
                                // Move to next field (works for all fields)
                                self.template_modal.next_focus();
                            }
                        }
                    }
                }
                KeyCode::Char(c)
                    if self.template_modal.mode == TemplateModalMode::Create
                        || self.template_modal.mode == TemplateModalMode::Edit =>
                {
                    match self.template_modal.create_focus {
                        CreateFocus::Name => {
                            // Clear error when user starts typing
                            self.template_modal.name_error = None;
                            let cursor = self.template_modal.create_name_cursor;
                            self.template_modal.create_name.insert(cursor, c);
                            self.template_modal.create_name_cursor += 1;
                        }
                        CreateFocus::Description => {
                            let cursor = self.template_modal.create_description_cursor;
                            if c == '\n' {
                                self.template_modal.create_description.insert(cursor, '\n');
                                self.template_modal.create_description_cursor += 1;
                            } else {
                                self.template_modal.create_description.insert(cursor, c);
                                self.template_modal.create_description_cursor += 1;
                            }
                        }
                        CreateFocus::ExactPath => {
                            let cursor = self.template_modal.create_exact_path_cursor;
                            self.template_modal.create_exact_path.insert(cursor, c);
                            self.template_modal.create_exact_path_cursor += 1;
                        }
                        CreateFocus::RelativePath => {
                            let cursor = self.template_modal.create_relative_path_cursor;
                            self.template_modal.create_relative_path.insert(cursor, c);
                            self.template_modal.create_relative_path_cursor += 1;
                        }
                        CreateFocus::PathPattern => {
                            let cursor = self.template_modal.create_path_pattern_cursor;
                            self.template_modal.create_path_pattern.insert(cursor, c);
                            self.template_modal.create_path_pattern_cursor += 1;
                        }
                        CreateFocus::FilenamePattern => {
                            let cursor = self.template_modal.create_filename_pattern_cursor;
                            self.template_modal
                                .create_filename_pattern
                                .insert(cursor, c);
                            self.template_modal.create_filename_pattern_cursor += 1;
                        }
                        CreateFocus::SchemaMatch => {
                            // Space toggles
                            if c == ' ' {
                                self.template_modal.create_schema_match_enabled =
                                    !self.template_modal.create_schema_match_enabled;
                            }
                        }
                        _ => {}
                    }
                }
                KeyCode::Left
                    if self.template_modal.mode == TemplateModalMode::Create
                        || self.template_modal.mode == TemplateModalMode::Edit =>
                {
                    match self.template_modal.create_focus {
                        CreateFocus::Description => {
                            if self.template_modal.create_description_cursor > 0 {
                                self.template_modal.create_description_cursor -= 1;
                            }
                        }
                        CreateFocus::Name => {
                            if self.template_modal.create_name_cursor > 0 {
                                self.template_modal.create_name_cursor -= 1;
                            }
                        }
                        CreateFocus::ExactPath => {
                            if self.template_modal.create_exact_path_cursor > 0 {
                                self.template_modal.create_exact_path_cursor -= 1;
                            }
                        }
                        CreateFocus::RelativePath => {
                            if self.template_modal.create_relative_path_cursor > 0 {
                                self.template_modal.create_relative_path_cursor -= 1;
                            }
                        }
                        CreateFocus::PathPattern => {
                            if self.template_modal.create_path_pattern_cursor > 0 {
                                self.template_modal.create_path_pattern_cursor -= 1;
                            }
                        }
                        CreateFocus::FilenamePattern => {
                            if self.template_modal.create_filename_pattern_cursor > 0 {
                                self.template_modal.create_filename_pattern_cursor -= 1;
                            }
                        }
                        _ => {}
                    }
                }
                KeyCode::Right
                    if self.template_modal.mode == TemplateModalMode::Create
                        || self.template_modal.mode == TemplateModalMode::Edit =>
                {
                    match self.template_modal.create_focus {
                        CreateFocus::Description => {
                            let char_count = self.template_modal.create_description.chars().count();
                            if self.template_modal.create_description_cursor < char_count {
                                self.template_modal.create_description_cursor += 1;
                            }
                        }
                        CreateFocus::Name => {
                            let char_count = self.template_modal.create_name.chars().count();
                            if self.template_modal.create_name_cursor < char_count {
                                self.template_modal.create_name_cursor += 1;
                            }
                        }
                        CreateFocus::ExactPath => {
                            let char_count = self.template_modal.create_exact_path.chars().count();
                            if self.template_modal.create_exact_path_cursor < char_count {
                                self.template_modal.create_exact_path_cursor += 1;
                            }
                        }
                        CreateFocus::RelativePath => {
                            let char_count =
                                self.template_modal.create_relative_path.chars().count();
                            if self.template_modal.create_relative_path_cursor < char_count {
                                self.template_modal.create_relative_path_cursor += 1;
                            }
                        }
                        CreateFocus::PathPattern => {
                            let char_count =
                                self.template_modal.create_path_pattern.chars().count();
                            if self.template_modal.create_path_pattern_cursor < char_count {
                                self.template_modal.create_path_pattern_cursor += 1;
                            }
                        }
                        CreateFocus::FilenamePattern => {
                            let char_count =
                                self.template_modal.create_filename_pattern.chars().count();
                            if self.template_modal.create_filename_pattern_cursor < char_count {
                                self.template_modal.create_filename_pattern_cursor += 1;
                            }
                        }
                        _ => {}
                    }
                }
                KeyCode::PageUp
                    if self.template_modal.mode == TemplateModalMode::Create
                        || self.template_modal.mode == TemplateModalMode::Edit =>
                {
                    if self.template_modal.create_focus == CreateFocus::Description {
                        // Move cursor up by 5 lines
                        let lines: Vec<&str> =
                            self.template_modal.create_description.lines().collect();
                        let cursor_line = self
                            .template_modal
                            .create_description
                            .chars()
                            .take(self.template_modal.create_description_cursor)
                            .filter(|&c| c == '\n')
                            .count();

                        if cursor_line >= 5 {
                            let target_line = cursor_line - 5;
                            let _current_line = lines.get(cursor_line).unwrap_or(&"");
                            let current_col = {
                                let chars_before_cursor: Vec<char> = self
                                    .template_modal
                                    .create_description
                                    .chars()
                                    .take(self.template_modal.create_description_cursor)
                                    .collect();
                                let last_newline_pos = chars_before_cursor
                                    .iter()
                                    .rposition(|&c| c == '\n')
                                    .map(|p| p + 1)
                                    .unwrap_or(0);
                                chars_before_cursor.len() - last_newline_pos
                            };

                            let target_line_str = lines.get(target_line).unwrap_or(&"");
                            let new_col = current_col.min(target_line_str.chars().count());
                            let mut new_cursor = 0;
                            for (i, line) in lines.iter().enumerate() {
                                if i < target_line {
                                    new_cursor += line.chars().count() + 1;
                                } else if i == target_line {
                                    new_cursor += new_col;
                                    break;
                                }
                            }

                            self.template_modal.create_description_cursor = new_cursor;

                            // Auto-scroll
                            if target_line < self.template_modal.description_scroll {
                                self.template_modal.description_scroll = target_line;
                            }
                        } else if cursor_line > 0 {
                            // Move to first line
                            let target_line = 0;
                            let target_line_str = lines.get(target_line).unwrap_or(&"");
                            let _current_line = lines.get(cursor_line).unwrap_or(&"");
                            let current_col = {
                                let chars_before_cursor: Vec<char> = self
                                    .template_modal
                                    .create_description
                                    .chars()
                                    .take(self.template_modal.create_description_cursor)
                                    .collect();
                                let last_newline_pos = chars_before_cursor
                                    .iter()
                                    .rposition(|&c| c == '\n')
                                    .map(|p| p + 1)
                                    .unwrap_or(0);
                                chars_before_cursor.len() - last_newline_pos
                            };
                            let new_col = current_col.min(target_line_str.chars().count());
                            self.template_modal.create_description_cursor = new_col;
                            self.template_modal.description_scroll = 0;
                        }
                    }
                }
                KeyCode::PageDown
                    if self.template_modal.mode == TemplateModalMode::Create
                        || self.template_modal.mode == TemplateModalMode::Edit =>
                {
                    if self.template_modal.create_focus == CreateFocus::Description {
                        // Move cursor down by 5 lines
                        let lines: Vec<&str> =
                            self.template_modal.create_description.lines().collect();
                        let cursor_line = self
                            .template_modal
                            .create_description
                            .chars()
                            .take(self.template_modal.create_description_cursor)
                            .filter(|&c| c == '\n')
                            .count();

                        if cursor_line + 5 < lines.len() {
                            let target_line = cursor_line + 5;
                            let _current_line = lines.get(cursor_line).unwrap_or(&"");
                            let current_col = {
                                let chars_before_cursor: Vec<char> = self
                                    .template_modal
                                    .create_description
                                    .chars()
                                    .take(self.template_modal.create_description_cursor)
                                    .collect();
                                let last_newline_pos = chars_before_cursor
                                    .iter()
                                    .rposition(|&c| c == '\n')
                                    .map(|p| p + 1)
                                    .unwrap_or(0);
                                chars_before_cursor.len() - last_newline_pos
                            };

                            let target_line_str = lines.get(target_line).unwrap_or(&"");
                            let new_col = current_col.min(target_line_str.chars().count());
                            let mut new_cursor = 0;
                            for (i, line) in lines.iter().enumerate() {
                                if i < target_line {
                                    new_cursor += line.chars().count() + 1;
                                } else if i == target_line {
                                    new_cursor += new_col;
                                    break;
                                }
                            }

                            self.template_modal.create_description_cursor = new_cursor;

                            // Auto-scroll
                            let available_height = 6;
                            if target_line
                                >= self.template_modal.description_scroll + available_height
                            {
                                self.template_modal.description_scroll =
                                    target_line.saturating_sub(available_height - 1);
                            }
                        } else if cursor_line < lines.len().saturating_sub(1) {
                            // Move to last line
                            let target_line = lines.len() - 1;
                            let target_line_str = lines.get(target_line).unwrap_or(&"");
                            let _current_line = lines.get(cursor_line).unwrap_or(&"");
                            let current_col = {
                                let chars_before_cursor: Vec<char> = self
                                    .template_modal
                                    .create_description
                                    .chars()
                                    .take(self.template_modal.create_description_cursor)
                                    .collect();
                                let last_newline_pos = chars_before_cursor
                                    .iter()
                                    .rposition(|&c| c == '\n')
                                    .map(|p| p + 1)
                                    .unwrap_or(0);
                                chars_before_cursor.len() - last_newline_pos
                            };
                            let new_col = current_col.min(target_line_str.chars().count());
                            let mut new_cursor = 0;
                            for (i, line) in lines.iter().enumerate() {
                                if i < target_line {
                                    new_cursor += line.chars().count() + 1;
                                } else if i == target_line {
                                    new_cursor += new_col;
                                    break;
                                }
                            }
                            self.template_modal.create_description_cursor = new_cursor;

                            // Auto-scroll
                            let available_height = 6;
                            let max_scroll = lines.len().saturating_sub(available_height).max(0);
                            self.template_modal.description_scroll = max_scroll;
                        }
                    }
                }
                KeyCode::Backspace
                    if self.template_modal.mode == TemplateModalMode::Create
                        || self.template_modal.mode == TemplateModalMode::Edit =>
                {
                    match self.template_modal.create_focus {
                        CreateFocus::Name => {
                            if self.template_modal.create_name_cursor > 0 {
                                self.template_modal.create_name_cursor -= 1;
                                self.template_modal
                                    .create_name
                                    .remove(self.template_modal.create_name_cursor);
                            }
                        }
                        CreateFocus::Description => {
                            if self.template_modal.create_description_cursor > 0 {
                                self.template_modal.create_description_cursor -= 1;
                                self.template_modal
                                    .create_description
                                    .remove(self.template_modal.create_description_cursor);
                            }
                        }
                        CreateFocus::PathPattern => {
                            self.template_modal.create_path_pattern.pop();
                        }
                        CreateFocus::FilenamePattern => {
                            self.template_modal.create_filename_pattern.pop();
                        }
                        _ => {}
                    }
                }
                KeyCode::Delete
                    if self.template_modal.mode == TemplateModalMode::Create
                        || self.template_modal.mode == TemplateModalMode::Edit =>
                {
                    match self.template_modal.create_focus {
                        CreateFocus::Name => {
                            if self.template_modal.create_name_cursor
                                < self.template_modal.create_name.chars().count()
                            {
                                self.template_modal
                                    .create_name
                                    .remove(self.template_modal.create_name_cursor);
                            }
                        }
                        CreateFocus::Description => {
                            if self.template_modal.create_description_cursor
                                < self.template_modal.create_description.chars().count()
                            {
                                self.template_modal
                                    .create_description
                                    .remove(self.template_modal.create_description_cursor);
                            }
                        }
                        CreateFocus::ExactPath => {
                            if self.template_modal.create_exact_path_cursor
                                < self.template_modal.create_exact_path.chars().count()
                            {
                                self.template_modal
                                    .create_exact_path
                                    .remove(self.template_modal.create_exact_path_cursor);
                            }
                        }
                        CreateFocus::RelativePath => {
                            if self.template_modal.create_relative_path_cursor
                                < self.template_modal.create_relative_path.chars().count()
                            {
                                self.template_modal
                                    .create_relative_path
                                    .remove(self.template_modal.create_relative_path_cursor);
                            }
                        }
                        CreateFocus::PathPattern => {
                            if self.template_modal.create_path_pattern_cursor
                                < self.template_modal.create_path_pattern.chars().count()
                            {
                                self.template_modal
                                    .create_path_pattern
                                    .remove(self.template_modal.create_path_pattern_cursor);
                            }
                        }
                        CreateFocus::FilenamePattern => {
                            if self.template_modal.create_filename_pattern_cursor
                                < self.template_modal.create_filename_pattern.chars().count()
                            {
                                self.template_modal
                                    .create_filename_pattern
                                    .remove(self.template_modal.create_filename_pattern_cursor);
                            }
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
            return None;
        }

        if self.input_mode == InputMode::Editing {
            match event.code {
                // History navigation (only for Search/Query input)
                KeyCode::Up if self.input_type == Some(InputType::Search) => {
                    if !self.query_history.is_empty() {
                        if self.query_history_index.is_none() {
                            // Save current input when starting to navigate history
                            self.query_history_temp = Some(self.input.clone());
                            self.query_history_index =
                                Some(self.query_history.len().saturating_sub(1));
                        } else if let Some(idx) = self.query_history_index {
                            if idx > 0 {
                                self.query_history_index = Some(idx - 1);
                            }
                        }

                        if let Some(idx) = self.query_history_index {
                            if let Some(query) = self.query_history.get(idx) {
                                self.input = query.clone();
                                self.input_cursor = self.input.chars().count();
                            }
                        }
                    }
                }
                KeyCode::Down if self.input_type == Some(InputType::Search) => {
                    if let Some(idx) = self.query_history_index {
                        if idx < self.query_history.len().saturating_sub(1) {
                            // Move to next (newer) history entry
                            self.query_history_index = Some(idx + 1);
                            if let Some(query) = self.query_history.get(idx + 1) {
                                self.input = query.clone();
                                self.input_cursor = self.input.chars().count();
                            }
                        } else {
                            // At end of history - restore temp input or clear
                            self.query_history_index = None;
                            self.input = self.query_history_temp.take().unwrap_or_default();
                            self.input_cursor = self.input.chars().count();
                        }
                    }
                }
                KeyCode::Enter => {
                    if self.input_type == Some(InputType::Search) {
                        // Clear history navigation state before executing
                        self.query_history_index = None;
                        self.query_history_temp = None;
                        return Some(AppEvent::Search(self.input.clone()));
                    }
                    let _input = std::mem::take(&mut self.input);
                    self.input_cursor = 0;
                    self.input_mode = InputMode::Normal;
                    return match self.input_type {
                        Some(InputType::Filter) => None,
                        None => None,
                        _ => None,
                    };
                }
                KeyCode::Esc => {
                    // Clear history navigation state
                    self.query_history_index = None;
                    self.query_history_temp = None;
                    self.input_mode = InputMode::Normal;
                    self.input.clear();
                    self.input_cursor = 0;
                    if let Some(state) = &mut self.data_table_state {
                        // Clear error and re-enable error display in main view
                        state.error = None;
                        state.suppress_error_display = false;
                    }
                }
                KeyCode::Left => {
                    if self.input_cursor > 0 {
                        // Move cursor left by one character (not byte)
                        self.input_cursor -= 1;
                    }
                }
                KeyCode::Right => {
                    let char_count = self.input.chars().count();
                    if self.input_cursor < char_count {
                        self.input_cursor += 1;
                    }
                }
                KeyCode::Home => {
                    self.input_cursor = 0;
                }
                KeyCode::End => {
                    self.input_cursor = self.input.chars().count();
                }
                KeyCode::Char(c) => {
                    // Convert character position to byte position
                    let byte_pos = self
                        .input
                        .chars()
                        .take(self.input_cursor)
                        .map(|c| c.len_utf8())
                        .sum();
                    self.input.insert(byte_pos, c);
                    self.input_cursor += 1;
                }
                KeyCode::Backspace => {
                    if self.input_cursor > 0 {
                        // Convert character position to byte position for the character before cursor
                        let prev_byte_pos: usize = self
                            .input
                            .chars()
                            .take(self.input_cursor - 1)
                            .map(|c| c.len_utf8())
                            .sum();
                        let char_to_delete = self.input[prev_byte_pos..].chars().next().unwrap();
                        let char_len = char_to_delete.len_utf8();
                        self.input.drain(prev_byte_pos..prev_byte_pos + char_len);
                        self.input_cursor -= 1;
                    }
                }
                KeyCode::Delete => {
                    let char_count = self.input.chars().count();
                    if self.input_cursor < char_count {
                        // Convert character position to byte position
                        let byte_pos: usize = self
                            .input
                            .chars()
                            .take(self.input_cursor)
                            .map(|c| c.len_utf8())
                            .sum();
                        let char_to_delete = self.input[byte_pos..].chars().next().unwrap();
                        let char_len = char_to_delete.len_utf8();
                        self.input.drain(byte_pos..byte_pos + char_len);
                    }
                }
                _ => {}
            }
            return None;
        }

        const RIGHT_KEYS: [KeyCode; 2] = [KeyCode::Right, KeyCode::Char('l')];

        const LEFT_KEYS: [KeyCode; 2] = [KeyCode::Left, KeyCode::Char('h')];

        const DOWN_KEYS: [KeyCode; 2] = [KeyCode::Down, KeyCode::Char('j')];

        const UP_KEYS: [KeyCode; 2] = [KeyCode::Up, KeyCode::Char('k')];

        match event.code {
            KeyCode::Char('q') | KeyCode::Char('Q') => Some(AppEvent::Exit),
            KeyCode::Char('c') if event.modifiers.contains(KeyModifiers::CONTROL) => {
                Some(AppEvent::Exit)
            }
            KeyCode::Char('R') => Some(AppEvent::Reset),
            KeyCode::Char('N') => {
                if let Some(ref mut state) = self.data_table_state {
                    state.toggle_row_numbers();
                }
                None
            }
            KeyCode::Esc => {
                // First check if we're in drill-down mode
                if let Some(ref mut state) = self.data_table_state {
                    if state.is_drilled_down() {
                        if let Err(e) = state.drill_up() {
                            eprintln!("Error drilling up: {}", e);
                        }
                        return None;
                    }
                }
                // Close info dialog if visible
                // Note: Modals handle Esc themselves, so this only applies when no modals are active
                if self.info_visible {
                    self.info_visible = false;
                }
                // Escape no longer exits - use 'q' or Ctrl-C to exit
                None
            }
            code if event.is_press() && RIGHT_KEYS.contains(&code) => {
                if let Some(ref mut state) = self.data_table_state {
                    state.scroll_right();
                }
                None
            }
            code if event.is_press() && LEFT_KEYS.contains(&code) => {
                if let Some(ref mut state) = self.data_table_state {
                    state.scroll_left();
                }
                None
            }
            code if event.is_press() && DOWN_KEYS.contains(&code) => {
                if let Some(ref mut state) = self.data_table_state {
                    state.select_next();
                }
                None
            }
            code if event.is_press() && UP_KEYS.contains(&code) => {
                if let Some(ref mut state) = self.data_table_state {
                    state.select_previous();
                }
                None
            }
            KeyCode::PageDown if event.is_press() => {
                if let Some(ref mut state) = self.data_table_state {
                    state.page_down();
                }
                None
            }
            KeyCode::Home if event.is_press() => {
                if let Some(ref mut state) = self.data_table_state {
                    // Only scroll if not already at top
                    if state.start_row > 0 {
                        state.scroll_to(0);
                    }
                }
                None
            }
            KeyCode::PageUp if event.is_press() => {
                if let Some(ref mut state) = self.data_table_state {
                    state.page_up();
                }
                None
            }
            KeyCode::Enter if event.is_press() => {
                // Only drill down if not in a modal and viewing grouped data
                if self.input_mode == InputMode::Normal {
                    if let Some(ref mut state) = self.data_table_state {
                        if state.is_grouped() && !state.is_drilled_down() {
                            if let Some(selected) = state.table_state.selected() {
                                let group_index = state.start_row + selected;
                                if let Err(e) = state.drill_down_into_group(group_index) {
                                    eprintln!("Error drilling down: {}", e);
                                }
                            }
                        }
                    }
                }
                None
            }
            KeyCode::Tab if event.is_press() => {
                self.focus = (self.focus + 1) % 2;
                None
            }
            KeyCode::Char('i') if event.is_press() => {
                self.info_visible = !self.info_visible;
                None
            }
            KeyCode::Char('/') => {
                self.input_mode = InputMode::Editing;
                self.input_type = Some(InputType::Search);
                // Clear history navigation state when opening query input
                self.query_history_index = None;
                self.query_history_temp = None;
                if let Some(state) = &mut self.data_table_state {
                    self.input = state.active_query.clone();
                    self.input_cursor = self.input.chars().count();
                    // Suppress error display in main view when query input is active
                    state.suppress_error_display = true;
                } else {
                    self.input.clear();
                    self.input_cursor = 0;
                }
                None
            }
            KeyCode::Char('f') => {
                if let Some(state) = &self.data_table_state {
                    // Always update available_columns from current schema (reflects query state if query exists)
                    // This ensures the filter modal shows only columns available after any queries
                    self.filter_modal.available_columns = state.headers();
                    // Reset column index if it's out of bounds
                    if !self.filter_modal.available_columns.is_empty() {
                        self.filter_modal.new_column_idx = self
                            .filter_modal
                            .new_column_idx
                            .min(self.filter_modal.available_columns.len().saturating_sub(1));
                    } else {
                        self.filter_modal.new_column_idx = 0;
                    }
                    self.filter_modal.active = true;
                    self.input_mode = InputMode::Filtering;
                }
                None
            }
            KeyCode::Char('T') => {
                // Apply most relevant template immediately (no modal)
                if let Some(ref state) = self.data_table_state {
                    if let Some(ref path) = self.path {
                        if let Some(template) =
                            self.template_manager.get_most_relevant(path, &state.schema)
                        {
                            // Apply template settings
                            if let Err(e) = self.apply_template(&template) {
                                // Show error modal instead of just printing
                                self.error_modal
                                    .show(format!("Error applying template: {}", e));
                            }
                        }
                    }
                }
                None
            }
            KeyCode::Char('t') => {
                // Open template modal
                if let Some(ref state) = self.data_table_state {
                    if let Some(ref path) = self.path {
                        // Load relevant templates
                        self.template_modal.templates = self
                            .template_manager
                            .find_relevant_templates(path, &state.schema);
                        self.template_modal.table_state.select(
                            if self.template_modal.templates.is_empty() {
                                None
                            } else {
                                Some(0)
                            },
                        );
                        self.template_modal.active = true;
                        self.template_modal.mode = TemplateModalMode::List;
                        self.template_modal.focus = TemplateFocus::TemplateList;
                    }
                }
                None
            }
            KeyCode::Char('s') => {
                if let Some(state) = &self.data_table_state {
                    // Populate columns from schema (all columns, including hidden ones)
                    // Use schema to get all columns, not headers() which only returns visible columns
                    let headers: Vec<String> =
                        state.schema.iter_names().map(|s| s.to_string()).collect();
                    let locked_count = state.locked_columns_count();

                    // Sync columns: update existing or create new ones
                    let mut existing_columns: std::collections::HashMap<String, SortColumn> = self
                        .sort_modal
                        .columns
                        .iter()
                        .map(|c| (c.name.clone(), c.clone()))
                        .collect();

                    self.sort_modal.columns = headers
                        .iter()
                        .enumerate()
                        .map(|(i, h)| {
                            if let Some(mut col) = existing_columns.remove(h) {
                                // Update display order and locked status, preserve visibility but clear to-be-locked
                                col.display_order = i;
                                col.is_locked = i < locked_count;
                                col.is_to_be_locked = false; // Clear to-be-locked when syncing (only applied locks persist)
                                                             // Visibility is preserved from existing column
                                col
                            } else {
                                SortColumn {
                                    name: h.clone(),
                                    sort_order: None,
                                    display_order: i,
                                    is_locked: i < locked_count,
                                    is_to_be_locked: false,
                                    is_visible: true,
                                }
                            }
                        })
                        .collect();

                    // Don't keep columns that were removed from the schema (e.g., by a query)
                    // The sort modal should only show columns that exist in the current schema
                    // This ensures it reflects the post-query state if a query was applied

                    self.sort_modal.active = true;
                    self.sort_modal.filter.clear();
                    self.sort_modal.focus = SortFocus::ColumnList; // Default focus to ColumnList
                    self.input_mode = InputMode::Sorting;
                }
                None
            }
            KeyCode::Char('r') => {
                if let Some(state) = &mut self.data_table_state {
                    state.reverse();
                }
                None
            }
            KeyCode::Char('a') => {
                // Open analysis modal if data is available
                if self.data_table_state.is_some() && self.input_mode == InputMode::Normal {
                    self.analysis_modal.open();
                }
                None
            }
            KeyCode::Char('p') => {
                if let Some(state) = &self.data_table_state {
                    if self.input_mode == InputMode::Normal {
                        self.pivot_melt_modal.available_columns =
                            state.schema.iter_names().map(|s| s.to_string()).collect();
                        self.pivot_melt_modal.column_dtypes = state
                            .schema
                            .iter()
                            .map(|(n, d)| (n.to_string(), d.clone()))
                            .collect();
                        self.pivot_melt_modal.open();
                        self.input_mode = InputMode::PivotMelt;
                    }
                }
                None
            }
            _ => None,
        }
    }

    pub fn event(&mut self, event: &AppEvent) -> Option<AppEvent> {
        self.debug.num_events += 1;
        match event {
            AppEvent::Key(key) => self.key(key),
            AppEvent::Open(path, options) => {
                // Set loading state first, then trigger a render before actually loading
                let file_size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);

                self.loading_state = LoadingState::Loading {
                    file_path: path.clone(),
                    file_size,
                    current_phase: "Opening file".to_string(),
                    progress_percent: 10,
                };

                // Return DoLoad event to perform actual loading after UI renders
                Some(AppEvent::DoLoad(path.clone(), options.clone()))
            }
            AppEvent::DoLoad(path, options) => {
                // Check if file is compressed
                let compression = options
                    .compression
                    .or_else(|| CompressionFormat::from_extension(path));
                let is_csv = path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .map(|stem| {
                        stem.ends_with(".csv")
                            || path
                                .extension()
                                .and_then(|e| e.to_str())
                                .map(|e| e.eq_ignore_ascii_case("csv"))
                                .unwrap_or(false)
                    })
                    .unwrap_or(false);
                let is_compressed_csv = compression.is_some() && is_csv;

                if is_compressed_csv {
                    // Set "Decompressing" phase and return event to trigger render
                    if let LoadingState::Loading {
                        file_path,
                        file_size,
                        ..
                    } = &self.loading_state
                    {
                        self.loading_state = LoadingState::Loading {
                            file_path: file_path.clone(),
                            file_size: *file_size,
                            current_phase: "Decompressing".to_string(),
                            progress_percent: 30,
                        };
                    }
                    // Return DoDecompress to allow UI to render "Decompressing" before blocking
                    Some(AppEvent::DoDecompress(path.clone(), options.clone()))
                } else {
                    // For non-compressed files, proceed with normal loading
                    match self.load(path, options) {
                        Ok(_) => Some(AppEvent::Collect),
                        Err(e) => {
                            // Clear loading state on error
                            self.loading_state = LoadingState::Idle;
                            Some(AppEvent::Crash(e.to_string()))
                        }
                    }
                }
            }
            AppEvent::DoDecompress(path, options) => {
                // Actually perform decompression now (after UI has rendered "Decompressing")
                match self.load(path, options) {
                    Ok(_) => Some(AppEvent::Collect),
                    Err(e) => {
                        // Clear loading state on error
                        self.loading_state = LoadingState::Idle;
                        Some(AppEvent::Crash(e.to_string()))
                    }
                }
            }
            AppEvent::Resize(_cols, rows) => {
                if let Some(state) = &mut self.data_table_state {
                    state.visible_rows = *rows as usize;
                    state.collect();
                }
                None
            }
            AppEvent::Collect => {
                if let Some(ref mut state) = self.data_table_state {
                    state.collect();
                }
                None
            }
            AppEvent::Search(query) => {
                let query_succeeded = if let Some(state) = &mut self.data_table_state {
                    state.query(query.clone());
                    state.error.is_none()
                } else {
                    false
                };

                // Only close input mode if query succeeded (no error after execution)
                if query_succeeded {
                    // Add successful query to history
                    self.add_to_history(query.clone());
                    // Clear history navigation state
                    self.query_history_index = None;
                    self.query_history_temp = None;
                    self.input_mode = InputMode::Normal;
                    self.input.clear();
                    self.input_cursor = 0;
                    // Re-enable error display in main view when closing query input
                    if let Some(state) = &mut self.data_table_state {
                        state.suppress_error_display = false;
                    }
                }
                // If there's an error, keep input mode open so user can fix the query
                // suppress_error_display remains true to keep main view clean
                None
            }
            AppEvent::Filter(statements) => {
                if let Some(state) = &mut self.data_table_state {
                    state.filter(statements.clone());
                }
                None
            }
            AppEvent::Sort(columns, ascending) => {
                if let Some(state) = &mut self.data_table_state {
                    state.sort(columns.clone(), *ascending);
                }
                None
            }
            AppEvent::Reset => {
                if let Some(state) = &mut self.data_table_state {
                    state.reset();
                }
                // Clear active template when resetting
                self.active_template_id = None;
                None
            }
            AppEvent::ColumnOrder(order, locked_count) => {
                if let Some(state) = &mut self.data_table_state {
                    state.set_column_order(order.clone());
                    state.set_locked_columns(*locked_count);
                }
                None
            }
            AppEvent::Pivot(spec) => {
                if let Some(state) = &mut self.data_table_state {
                    match state.pivot(spec) {
                        Ok(()) => {
                            self.pivot_melt_modal.close();
                            self.input_mode = InputMode::Normal;
                            Some(AppEvent::Collect)
                        }
                        Err(e) => {
                            self.error_modal.show(e.to_string());
                            None
                        }
                    }
                } else {
                    None
                }
            }
            AppEvent::Melt(spec) => {
                if let Some(state) = &mut self.data_table_state {
                    match state.melt(spec) {
                        Ok(()) => {
                            self.pivot_melt_modal.close();
                            self.input_mode = InputMode::Normal;
                            Some(AppEvent::Collect)
                        }
                        Err(e) => {
                            self.error_modal.show(e.to_string());
                            None
                        }
                    }
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn apply_template(&mut self, template: &Template) -> Result<()> {
        // Save state before applying template so we can restore on failure
        let saved_state = self
            .data_table_state
            .as_ref()
            .map(|state| TemplateApplicationState {
                lf: state.lf.clone(),
                schema: state.schema.clone(),
                active_query: state.active_query.clone(),
                filters: state.get_filters().to_vec(),
                sort_columns: state.get_sort_columns().to_vec(),
                sort_ascending: state.get_sort_ascending(),
                column_order: state.get_column_order().to_vec(),
                locked_columns_count: state.locked_columns_count(),
            });
        let saved_active_template_id = self.active_template_id.clone();

        if let Some(state) = &mut self.data_table_state {
            // Clear any previous errors
            state.error = None;

            // Apply query if present
            if let Some(ref query) = template.settings.query {
                if !query.is_empty() {
                    state.query(query.clone());
                    // Check for errors after query
                    let error_opt = state.error.clone();
                    if let Some(error) = error_opt {
                        // End the if let block to drop the borrow
                        if let Some(saved) = saved_state {
                            self.restore_state(saved);
                        }
                        self.active_template_id = saved_active_template_id;
                        return Err(color_eyre::eyre::eyre!("{}", error));
                    }
                }
            }

            // Apply filters
            if !template.settings.filters.is_empty() {
                state.filter(template.settings.filters.clone());
                // Check for errors after filter
                let error_opt = state.error.clone();
                if let Some(error) = error_opt {
                    // End the if let block to drop the borrow
                    if let Some(saved) = saved_state {
                        self.restore_state(saved);
                    }
                    self.active_template_id = saved_active_template_id;
                    return Err(color_eyre::eyre::eyre!("{}", error));
                }
            }

            // Apply sort
            if !template.settings.sort_columns.is_empty() {
                state.sort(
                    template.settings.sort_columns.clone(),
                    template.settings.sort_ascending,
                );
                // Check for errors after sort
                let error_opt = state.error.clone();
                if let Some(error) = error_opt {
                    // End the if let block to drop the borrow
                    if let Some(saved) = saved_state {
                        self.restore_state(saved);
                    }
                    self.active_template_id = saved_active_template_id;
                    return Err(color_eyre::eyre::eyre!("{}", error));
                }
            }

            // Apply column order and locks
            if !template.settings.column_order.is_empty() {
                state.set_column_order(template.settings.column_order.clone());
                // Check for errors after set_column_order
                let error_opt = state.error.clone();
                if let Some(error) = error_opt {
                    // End the if let block to drop the borrow
                    if let Some(saved) = saved_state {
                        self.restore_state(saved);
                    }
                    self.active_template_id = saved_active_template_id;
                    return Err(color_eyre::eyre::eyre!("{}", error));
                }
                state.set_locked_columns(template.settings.locked_columns_count);
                // Check for errors after set_locked_columns
                let error_opt = state.error.clone();
                if let Some(error) = error_opt {
                    // End the if let block to drop the borrow
                    if let Some(saved) = saved_state {
                        self.restore_state(saved);
                    }
                    self.active_template_id = saved_active_template_id;
                    return Err(color_eyre::eyre::eyre!("{}", error));
                }
            }
        }

        // Update template usage statistics
        // Note: We need to clone and update the template, then save it
        // For now, we'll update the template manager's internal state
        // A more complete implementation would reload templates after saving
        if let Some(path) = &self.path {
            let mut updated_template = template.clone();
            updated_template.last_used = Some(std::time::SystemTime::now());
            updated_template.usage_count += 1;
            updated_template.last_matched_file = Some(path.clone());

            // Save updated template
            if let Err(e) = self.template_manager.save_template(&updated_template) {
                eprintln!("Warning: Could not save template usage stats: {}", e);
            }
        }

        // Track active template
        self.active_template_id = Some(template.id.clone());

        Ok(())
    }

    fn restore_state(&mut self, saved: TemplateApplicationState) {
        if let Some(state) = &mut self.data_table_state {
            // Clone saved lf and schema so we can restore them after applying methods
            let saved_lf = saved.lf.clone();
            let saved_schema = saved.schema.clone();

            // Restore lf and schema directly (these are public fields)
            // This preserves the exact LazyFrame state from before template application
            state.lf = saved.lf;
            state.schema = saved.schema;
            state.active_query = saved.active_query;
            // Clear error
            state.error = None;
            // Restore private fields using public methods
            // Note: These methods will modify lf by applying transformations, but since
            // we've already restored lf to the saved state, we need to restore it again after
            state.filter(saved.filters.clone());
            if state.error.is_none() {
                state.sort(saved.sort_columns.clone(), saved.sort_ascending);
            }
            if state.error.is_none() {
                state.set_column_order(saved.column_order.clone());
            }
            if state.error.is_none() {
                state.set_locked_columns(saved.locked_columns_count);
            }
            // Restore the exact saved lf and schema (in case filter/sort modified them)
            state.lf = saved_lf;
            state.schema = saved_schema;
            state.collect();
        }
    }

    pub fn create_template_from_current_state(
        &mut self,
        name: String,
        description: Option<String>,
        match_criteria: template::MatchCriteria,
    ) -> Result<template::Template> {
        let settings = if let Some(state) = &self.data_table_state {
            template::TemplateSettings {
                query: if state.get_active_query().is_empty() {
                    None
                } else {
                    Some(state.get_active_query().to_string())
                },
                filters: state.get_filters().to_vec(),
                sort_columns: state.get_sort_columns().to_vec(),
                sort_ascending: state.get_sort_ascending(),
                column_order: state.get_column_order().to_vec(),
                locked_columns_count: state.locked_columns_count(),
            }
        } else {
            template::TemplateSettings {
                query: None,
                filters: Vec::new(),
                sort_columns: Vec::new(),
                sort_ascending: true,
                column_order: Vec::new(),
                locked_columns_count: 0,
            }
        };

        self.template_manager
            .create_template(name, description, match_criteria, settings)
    }

    fn get_help_info(&self) -> (String, String) {
        let (title, content) = match self.input_mode {
            InputMode::Normal => ("Main View Help", "\
Navigation:
  Arrows (h/j/k/l): Scroll table
  PgUp/PgDown:     Scroll pages
  Home:             Go to top

Data Operations:
  /:                Open Query input
  f:                Open Filter menu
  s:                Open Sort & Column Order menu
  a:                Open Statistical Analysis
  r:                Reverse sort order
  R:                Reset table (clear queries, filters, sorts, locks)
  T:                Apply most relevant template
  t:                Open Template menu

Display:
  i:                Toggle Info view
  N:                Toggle row numbers
  Ctrl+h:           Toggle this help

Help Navigation:
  Arrow keys (↑↓):  Scroll help content
  PageUp/PageDown:  Scroll help pages
  Home/End:         Jump to top/bottom

Analysis View:
  a:                Open statistical analysis
  r:                Resample data (if sampled)
  Arrow keys:       Scroll statistics table
  Esc:              Return to main view

Exit:
  q / Esc:          Quit"),
            InputMode::Editing => match self.input_type {
                Some(InputType::Search) => ("Query Help", "\
Query Syntax:
  select [columns] [by group_cols] [where conditions]

Basic Examples:
  select a, b where a > 10, b < 5
  select a, b by category where a > 10
  select by city, state
  select avg[price], count[a] by category, region
  select a, b:\"foo\" where name=\"george\", age > 7
  select col[\"first name\"], col[last_name]:\"derek\"

Column Selection:
  - List columns: a, b, c
  - Use aliases: total:a + b
  - Empty select: select (selects all columns)
  - With grouping: select by city (all columns grouped by city)
  - String literals: b:\"foo\" (creates column b with value \"foo\")
  - Column names with spaces: col[\"first name\"] or col[first name]
  - col[] syntax works for any column: col[name] or col[\"name\"]

Column Assignment (by clause):
  - Create computed columns: by new_col:city+state
  - Use expressions: by area:width*height
  - Supports same operations as select clause

Aggregation Functions (square brackets optional):
  avg[expr] or avg expr    - Average value (also: mean)
  min[expr] or min expr    - Minimum value
  max[expr] or max expr    - Maximum value
  count[expr] or count expr - Count of non-null values
  std[expr] or std expr    - Standard deviation (also: stddev)
  med[expr] or med expr    - Median value (also: median)
  sum[expr] or sum expr    - Sum of values

Aggregation Examples:
  select avg[price], min[quantity], max[date] by category
  select avg price, min quantity, max date by category
  select total:sum[price*qty], count[id] by region
  select avg[price], std[price] by category, region

Functions (square brackets optional):
  not[expr] or not expr    - Logical negation
  Examples:
    not[a=b] or not a=b    - Equivalent to a!=b
    not[a>10] or not a>10  - Equivalent to a<=10

Grouping:
  - Group by columns: select a, b by category, region
  - Group with aliases: by region_name:region, year:date.year
  - Empty select with grouping: select by city, state
  - All non-group columns collected as lists

Where Conditions:
  - Multiple conditions: a > 10, b < 5 (AND)
  - OR within condition: a > 10 | a < 5
  - Expressions: (a + b) * 2 > 100
  - String comparisons: name=\"george\", city=\"New York\"
  - Use not function: not[a=b]
  - Note: Where clause does NOT support column assignment

Operators:
  Math:        +, -, *, %
  Comparison:  =, <, >, <=, >=
  Logic:       , (AND), | (OR)
  Note: Use not[expr] or not expr instead of !=

Expression Evaluation:
  - Operators evaluated right-to-left
  - Example: 1%c+a evaluates as 1%(c+a)
  - Use parentheses () for grouping: (a+b)*2
  - Square brackets [] are for function calls only

Function Syntax:
  - Aggregation: avg[expr], sum[expr], etc.
  - Logic: not[expr]
  - Brackets optional: avg 5+a (same as avg[5+a])
  - Brackets optional: not a=b (same as not[a=b])
  - Use parentheses for grouping: (a+b)*2
  - Example: b:avg[(1%c)+a] or b:avg (1%c)+a

Press Enter to apply query."),
                _ => ("Editing Help", "Editing..."),
            },
            InputMode::Filtering => ("Filter Help", "\
Create filters to narrow down data.

Steps:
  1. Select a Column
  2. Choose an Operator (=, !=, >, <, >=, <=, contains, not contains)
  3. Enter a Value
  4. Choose Logical operator (AND/OR) for next filter

Navigation:
  Tab/BackTab:      Move between fields
  Arrow keys:       Navigate column/operator/value lists
  Enter:            Add filter / Apply filters
  Esc:              Cancel and close modal"),
            InputMode::Sorting => ("Sort & Column Order Help", "\
Manage column sorting and display order.

Column List:
  Filter:           Type to filter columns (contrasted background when focused)
  Arrow keys:       Navigate column list
  Space:            Toggle column for sorting
  Enter:            Toggle selection and apply sort (does not close modal)

Sort Order:
  [:                Move selected column UP in sort order
  ]:                Move selected column DOWN in sort order
  1-9:              Jump selected item to that sort order (e.g., 1 for first)

Display Order:
  +:                Move selected column LEFT in display order
  -:                Move selected column RIGHT in display order
  L:                Lock/unlock columns up to selected column
  v:                Toggle column visibility (hidden columns can still be sorted)

Navigation:
  Tab/BackTab:      Move focus (Filter → Columns → Order → Apply → Cancel → Clear)
  Space (on Apply): Apply changes (does not close modal)
  Enter (on Apply): Apply changes and close modal
  Esc:              Cancel and close modal

Visual Indicators:
  [n]:              Sort order number
  ●:                Locked column indicator
  ◐:                To-be-locked column indicator (pending)
  #:                Display order number
  Shaded Apply:     Unapplied changes exist

Table Columns:
  Locked:           Shows ● if column is locked, ◐ if to-be-locked
  Order:            Display order number (blank for hidden columns)
  Sort:             Sort order number (blank if not sorted)
  Name:             Column name

Locked columns stay visible when scrolling horizontally.
Hidden columns (toggled with 'v') can still be used for sorting but won't appear in the main table."),
            InputMode::PivotMelt => ("Pivot / Melt Help", "\
Reshape data between long and wide formats.

  Tab / Shift+Tab:  Move focus (tab bar → form → Apply → Cancel → Clear → tab bar)
  Left / Right:     Tab bar: switch Pivot/Melt. In text fields: move cursor
  ↑ / ↓:            Change selection (index, pivot, value, aggregation, strategy, etc.)
  Space:            Toggle index/explicit list; toggle Sort new columns (Pivot)
  Enter:            Apply, or Cancel/Clear when focused
  Esc:              Close without applying
  Ctrl+h:           Show this help

Pivot: long → wide (index, pivot col, value col, aggregation). Melt: wide → long (index, strategy, variable/value names)."),
        };
        (title.to_string(), content.to_string())
    }
}

impl Widget for &mut App {
    fn render(self, area: Rect, buf: &mut Buffer) {
        self.debug.num_frames += 1;

        // Set background color for the entire application area
        let background_color = self.color("background");
        Block::default()
            .style(Style::default().bg(background_color))
            .render(area, buf);

        let mut constraints = vec![Constraint::Fill(1)];

        // Adjust layout if sorting to show panel on the right
        let mut has_error = false;
        let mut err_msg = String::new();
        if let Some(state) = &self.data_table_state {
            if let Some(e) = &state.error {
                has_error = true;
                err_msg = e.to_string().lines().next().unwrap_or_default().to_string();
            }
        }

        if self.input_mode == InputMode::Editing {
            let height = if has_error { 6 } else { 3 };
            constraints.insert(1, Constraint::Length(height));
        }
        constraints.push(Constraint::Length(1)); // Controls
        if self.debug.enabled {
            constraints.push(Constraint::Length(1));
        }
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints(constraints)
            .split(area);

        let main_area = layout[0];
        let mut data_area = main_area;
        let mut sort_area = Rect::default();

        if self.filter_modal.active {
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Min(0), Constraint::Length(50)])
                .split(main_area);
            data_area = chunks[0];
            sort_area = chunks[1]; // Reuse sort_area variable for filter panel
        }
        if self.sort_modal.active {
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Min(0), Constraint::Length(50)])
                .split(main_area);
            data_area = chunks[0];
            sort_area = chunks[1];
        }
        if self.template_modal.active {
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Min(0), Constraint::Length(80)]) // Wider for 30 char descriptions
                .split(main_area);
            data_area = chunks[0];
            sort_area = chunks[1]; // Reuse sort_area for template modal
        }
        if self.pivot_melt_modal.active {
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Min(0), Constraint::Length(50)])
                .split(main_area);
            data_area = chunks[0];
            sort_area = chunks[1];
        }

        // Extract colors before mutable borrow to avoid borrow checker issues
        let primary_color = self.color("keybind_hints");
        let _controls_bg_color = self.color("controls_bg");
        let table_header_color = self.color("table_header");
        let dimmed_color = self.color("dimmed");
        let column_separator_color = self.color("column_separator");
        let sidebar_border_color = self.color("sidebar_border");
        let table_header_bg_color = self.color("table_header_bg");

        match &mut self.data_table_state {
            Some(state) => {
                // Render breadcrumb if drilled down
                let mut table_area = data_area;
                if state.is_drilled_down() {
                    if let Some(ref key_values) = state.drilled_down_group_key {
                        let breadcrumb_layout = Layout::default()
                            .direction(Direction::Vertical)
                            .constraints([Constraint::Length(3), Constraint::Fill(1)])
                            .split(data_area);

                        // Render breadcrumb with better styling
                        let empty_vec = Vec::new();
                        let key_columns = state
                            .drilled_down_group_key_columns
                            .as_ref()
                            .unwrap_or(&empty_vec);
                        let breadcrumb_parts: Vec<String> = key_columns
                            .iter()
                            .zip(key_values.iter())
                            .map(|(col, val)| format!("{}={}", col, val))
                            .collect();
                        let breadcrumb_text = format!(
                            "← Group: {} (Press Esc to go back)",
                            breadcrumb_parts.join(" | ")
                        );

                        Block::default()
                            .borders(Borders::ALL)
                            .border_style(Style::default().fg(primary_color))
                            .title("Breadcrumb")
                            .render(breadcrumb_layout[0], buf);

                        let inner = Block::default().inner(breadcrumb_layout[0]);
                        Paragraph::new(breadcrumb_text)
                            .style(
                                Style::default()
                                    .fg(primary_color)
                                    .add_modifier(Modifier::BOLD),
                            )
                            .wrap(ratatui::widgets::Wrap { trim: true })
                            .render(inner, buf);

                        table_area = breadcrumb_layout[1];
                    }
                }

                if self.info_visible {
                    let info_layout = Layout::default()
                        .direction(Direction::Horizontal)
                        .constraints([Constraint::Fill(1), Constraint::Max(50)])
                        .split(table_area);
                    DataTable::new()
                        .with_colors(
                            table_header_bg_color,
                            table_header_color,
                            dimmed_color,
                            column_separator_color,
                        )
                        .render(info_layout[0], buf, state);
                    DataTableInfo::new(state)
                        .with_border_color(sidebar_border_color)
                        .render(info_layout[1], buf);
                } else {
                    DataTable::new()
                        .with_colors(
                            table_header_bg_color,
                            table_header_color,
                            dimmed_color,
                            column_separator_color,
                        )
                        .render(table_area, buf, state);
                }
            }
            None => {
                // Show loading indicator if loading, otherwise show "No data loaded"
                if self.loading_state.is_loading() {
                    App::render_loading_gauge(&self.loading_state, layout[0], buf);
                } else {
                    Paragraph::new("No data loaded").render(layout[0], buf);
                }
            }
        }

        let mut controls_area = layout[1];
        let debug_area_index = layout.len() - 1;

        if self.input_mode == InputMode::Editing {
            let input_area = layout[1];
            controls_area = layout[layout.len() - 1];

            let title = match self.input_type {
                Some(InputType::Search) => "Query",
                Some(InputType::Filter) => "Filter",
                None => "Input",
            };

            let mut border_style = Style::default();
            if has_error {
                border_style = Style::default().fg(self.color("error"));
            }

            if self.debug.enabled {
                controls_area = layout[layout.len() - 2];
            }

            let block = Block::default()
                .borders(Borders::ALL)
                .title(title)
                .border_style(border_style);
            let inner_area = block.inner(input_area);
            block.render(input_area, buf);

            if has_error {
                let chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Length(1),
                        Constraint::Length(1),
                        Constraint::Min(1),
                    ])
                    .split(inner_area);

                // Render input with cursor
                let input_text = self.input.as_str();
                let cursor_pos = self.input_cursor.min(input_text.chars().count());
                let mut chars = input_text.chars();
                let before_cursor: String = chars.by_ref().take(cursor_pos).collect();
                let at_cursor = chars
                    .next()
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| " ".to_string());
                let after_cursor: String = chars.collect();

                use ratatui::text::{Line, Span};
                let mut line = Line::default();
                line.spans.push(Span::raw(before_cursor));
                line.spans.push(Span::styled(
                    at_cursor,
                    Style::default()
                        .bg(self.color("text_primary"))
                        .fg(self.color("text_inverse")),
                ));
                if !after_cursor.is_empty() {
                    line.spans.push(Span::raw(after_cursor));
                }

                Paragraph::new(line).render(chunks[0], buf);
                Paragraph::new(err_msg)
                    .style(Style::default().fg(self.color("error")))
                    .wrap(ratatui::widgets::Wrap { trim: true })
                    .render(chunks[2], buf);
            } else {
                // Render input with cursor
                let input_text = self.input.as_str();
                let cursor_pos = self.input_cursor.min(input_text.chars().count());
                let mut chars = input_text.chars();
                let before_cursor: String = chars.by_ref().take(cursor_pos).collect();
                let at_cursor = chars
                    .next()
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| " ".to_string());
                let after_cursor: String = chars.collect();

                use ratatui::text::{Line, Span};
                let mut line = Line::default();
                line.spans.push(Span::raw(before_cursor));
                line.spans.push(Span::styled(
                    at_cursor,
                    Style::default()
                        .bg(self.color("text_primary"))
                        .fg(self.color("text_inverse")),
                ));
                if !after_cursor.is_empty() {
                    line.spans.push(Span::raw(after_cursor));
                }

                Paragraph::new(line).render(inner_area, buf);
            }
        }

        if self.filter_modal.active {
            Clear.render(sort_area, buf);
            let block = Block::default()
                .borders(Borders::ALL)
                .title("Filter (Enter to Add/Apply)");
            let inner_area = block.inner(sort_area);
            block.render(sort_area, buf);

            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3), // New Filter Row
                    Constraint::Length(3), // Add Button
                    Constraint::Min(0),    // List of filters
                    Constraint::Length(3), // Apply/Clear
                ])
                .split(inner_area);

            // New Filter Row
            let row_layout = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Percentage(30), // Column
                    Constraint::Percentage(20), // Op
                    Constraint::Percentage(30), // Value
                    Constraint::Percentage(20), // Logic
                ])
                .split(chunks[0]);

            let col_name = if self.filter_modal.available_columns.is_empty() {
                ""
            } else {
                &self.filter_modal.available_columns[self.filter_modal.new_column_idx]
            };
            let col_style = if self.filter_modal.focus == FilterFocus::Column {
                Style::default().fg(self.color("modal_border_active"))
            } else {
                Style::default()
            };
            Paragraph::new(col_name)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("Col")
                        .border_style(col_style),
                )
                .render(row_layout[0], buf);

            let op_name = FilterOperator::iterator()
                .nth(self.filter_modal.new_operator_idx)
                .unwrap()
                .as_str();
            let op_style = if self.filter_modal.focus == FilterFocus::Operator {
                Style::default().fg(self.color("modal_border_active"))
            } else {
                Style::default()
            };
            Paragraph::new(op_name)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("Op")
                        .border_style(op_style),
                )
                .render(row_layout[1], buf);

            let val_style = if self.filter_modal.focus == FilterFocus::Value {
                Style::default().fg(self.color("modal_border_active"))
            } else {
                Style::default()
            };
            Paragraph::new(self.filter_modal.new_value.as_str())
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("Val")
                        .border_style(val_style),
                )
                .render(row_layout[2], buf);

            let log_name = LogicalOperator::iterator()
                .nth(self.filter_modal.new_logical_idx)
                .unwrap()
                .as_str();
            let log_style = if self.filter_modal.focus == FilterFocus::Logical {
                Style::default().fg(self.color("modal_border_active"))
            } else {
                Style::default()
            };
            Paragraph::new(log_name)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("Logic")
                        .border_style(log_style),
                )
                .render(row_layout[3], buf);

            // Add Button
            let add_style = if self.filter_modal.focus == FilterFocus::Add {
                Style::default().fg(self.color("modal_border_active"))
            } else {
                Style::default()
            };
            Paragraph::new("Add Filter")
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(add_style),
                )
                .centered()
                .render(chunks[1], buf);

            // List
            let items: Vec<ListItem> = self
                .filter_modal
                .statements
                .iter()
                .enumerate()
                .map(|(i, s)| {
                    let prefix = if i > 0 {
                        format!("{} ", s.logical_op.as_str())
                    } else {
                        "".to_string()
                    };
                    ListItem::new(format!(
                        "{}{}{}{}",
                        prefix,
                        s.column,
                        s.operator.as_str(),
                        s.value
                    ))
                })
                .collect();
            let list_style = if self.filter_modal.focus == FilterFocus::Statements {
                Style::default().fg(self.color("modal_border_active"))
            } else {
                Style::default()
            };
            let list = List::new(items)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("Current Filters")
                        .border_style(list_style),
                )
                .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
            StatefulWidget::render(list, chunks[2], buf, &mut self.filter_modal.list_state);

            // Apply/Clear
            let btn_layout = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(chunks[3]);

            let confirm_style = if self.filter_modal.focus == FilterFocus::Confirm {
                Style::default().fg(self.color("modal_border_active"))
            } else {
                Style::default()
            };
            Paragraph::new("Apply")
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(confirm_style),
                )
                .centered()
                .render(btn_layout[0], buf);

            let clear_style = if self.filter_modal.focus == FilterFocus::Clear {
                Style::default().fg(self.color("modal_border_active"))
            } else {
                Style::default()
            };
            Paragraph::new("Clear")
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(clear_style),
                )
                .centered()
                .render(btn_layout[1], buf);
        }

        if self.sort_modal.active {
            Clear.render(sort_area, buf);
            let block = Block::default()
                .borders(Borders::ALL)
                .title("Sort & Column Order");
            let inner_area = block.inner(sort_area);
            block.render(sort_area, buf);
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3), // Filter
                    Constraint::Min(0),    // Table
                    Constraint::Length(2), // Keybind Hints
                    Constraint::Length(3), // Order
                    Constraint::Length(3), // Buttons
                ])
                .split(inner_area);

            // Filter Input
            let filter_block_title = "Filter Columns";
            let mut filter_block_border_style = Style::default();
            if self.sort_modal.focus == SortFocus::Filter {
                filter_block_border_style =
                    filter_block_border_style.fg(self.color("modal_border_active"));
            }
            let filter_block = Block::default()
                .borders(Borders::ALL)
                .title(filter_block_title)
                .border_style(filter_block_border_style);
            let filter_inner_area = filter_block.inner(chunks[0]);
            filter_block.render(chunks[0], buf);

            // Text input with cursor (like query input)
            let filter_text = self.sort_modal.filter.as_str();
            let cursor_pos = self
                .sort_modal
                .filter_cursor
                .min(filter_text.chars().count());
            let mut chars = filter_text.chars();
            let before_cursor: String = chars.by_ref().take(cursor_pos).collect();
            let at_cursor = chars
                .next()
                .map(|c| c.to_string())
                .unwrap_or_else(|| " ".to_string());
            let after_cursor: String = chars.collect();

            let mut line = ratatui::text::Line::default();
            line.spans.push(ratatui::text::Span::raw(before_cursor));
            line.spans.push(ratatui::text::Span::styled(
                at_cursor,
                Style::default()
                    .bg(self.color("text_primary"))
                    .fg(self.color("text_inverse")),
            ));
            if !after_cursor.is_empty() {
                line.spans.push(ratatui::text::Span::raw(after_cursor));
            }

            Paragraph::new(line).render(filter_inner_area, buf);

            // Column Table
            let filtered = self.sort_modal.filtered_columns();
            let rows: Vec<Row> = filtered
                .iter()
                .map(|(_, col)| {
                    let lock_cell = if col.is_locked {
                        "●" // Full circle for locked
                    } else if col.is_to_be_locked {
                        "◐" // Half circle to indicate pending lock
                    } else {
                        " "
                    };
                    let lock_style = if col.is_locked {
                        Style::default()
                    } else if col.is_to_be_locked {
                        Style::default().fg(self.color("dimmed")) // Dimmed style for pending lock
                    } else {
                        Style::default()
                    };
                    let order_cell = if col.is_visible && col.display_order < 9999 {
                        format!("{:2}", col.display_order + 1)
                    } else {
                        "  ".to_string()
                    };
                    let sort_cell = if let Some(order) = col.sort_order {
                        format!("{:2}", order)
                    } else {
                        "  ".to_string()
                    };
                    let name_cell = Cell::from(col.name.clone());

                    // Apply dimmed style to hidden columns
                    let row_style = if col.is_visible {
                        Style::default()
                    } else {
                        Style::default().fg(self.color("dimmed"))
                    };

                    Row::new(vec![
                        Cell::from(lock_cell).style(lock_style),
                        Cell::from(order_cell).style(row_style),
                        Cell::from(sort_cell).style(row_style),
                        name_cell.style(row_style),
                    ])
                })
                .collect();

            let header = Row::new(vec![
                Cell::from("🔒").style(Style::default().add_modifier(Modifier::BOLD)),
                Cell::from("Order").style(Style::default().add_modifier(Modifier::BOLD)),
                Cell::from("Sort").style(Style::default().add_modifier(Modifier::BOLD)),
                Cell::from("Name").style(Style::default().add_modifier(Modifier::BOLD)),
            ])
            .style(Style::default().add_modifier(Modifier::UNDERLINED));

            let table_border_style = if self.sort_modal.focus == SortFocus::ColumnList {
                Style::default().fg(self.color("modal_border_active"))
            } else {
                Style::default()
            };
            let table = Table::new(
                rows,
                [
                    Constraint::Length(2),
                    Constraint::Length(6),
                    Constraint::Length(6),
                    Constraint::Min(0),
                ],
            )
            .header(header)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Columns")
                    .border_style(table_border_style),
            )
            .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

            StatefulWidget::render(table, chunks[1], buf, &mut self.sort_modal.table_state);

            // Keybind Hints
            use ratatui::text::{Line, Span};
            let mut hint_line1 = Line::default();
            hint_line1.spans.push(Span::raw("Sort:    "));
            hint_line1.spans.push(Span::styled(
                "Space",
                Style::default()
                    .fg(self.color("keybind_hints"))
                    .add_modifier(Modifier::BOLD),
            ));
            hint_line1.spans.push(Span::raw(" Toggle "));
            hint_line1.spans.push(Span::styled(
                "[]",
                Style::default()
                    .fg(self.color("keybind_hints"))
                    .add_modifier(Modifier::BOLD),
            ));
            hint_line1.spans.push(Span::raw(" Reorder "));
            hint_line1.spans.push(Span::styled(
                "1-9",
                Style::default()
                    .fg(self.color("keybind_hints"))
                    .add_modifier(Modifier::BOLD),
            ));
            hint_line1.spans.push(Span::raw(" Jump"));

            let mut hint_line2 = Line::default();
            hint_line2.spans.push(Span::raw("Display: "));
            hint_line2.spans.push(Span::styled(
                "L",
                Style::default()
                    .fg(self.color("keybind_hints"))
                    .add_modifier(Modifier::BOLD),
            ));
            hint_line2.spans.push(Span::raw(" Lock "));
            hint_line2.spans.push(Span::styled(
                "+-",
                Style::default()
                    .fg(self.color("keybind_hints"))
                    .add_modifier(Modifier::BOLD),
            ));
            hint_line2.spans.push(Span::raw(" Reorder"));

            Paragraph::new(vec![hint_line1, hint_line2]).render(chunks[2], buf);

            // Order Toggle - Radio Button Style with single border
            let order_border_style = if self.sort_modal.focus == SortFocus::Order {
                Style::default().fg(self.color("modal_border_active"))
            } else {
                Style::default()
            };

            let order_block = Block::default()
                .borders(Borders::ALL)
                .title("Order")
                .border_style(order_border_style);
            let order_inner = order_block.inner(chunks[3]);
            order_block.render(chunks[3], buf);

            let order_layout = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(order_inner);

            // Ascending option
            let ascending_indicator = if self.sort_modal.ascending {
                "●"
            } else {
                "○"
            };
            let ascending_text = format!("{} Ascending", ascending_indicator);
            let ascending_style = if self.sort_modal.ascending {
                Style::default().add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            Paragraph::new(ascending_text)
                .style(ascending_style)
                .centered()
                .render(order_layout[0], buf);

            // Descending option
            let descending_indicator = if !self.sort_modal.ascending {
                "●"
            } else {
                "○"
            };
            let descending_text = format!("{} Descending", descending_indicator);
            let descending_style = if !self.sort_modal.ascending {
                Style::default().add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            Paragraph::new(descending_text)
                .style(descending_style)
                .centered()
                .render(order_layout[1], buf);

            // Buttons
            let btn_layout = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Percentage(33),
                    Constraint::Percentage(33),
                    Constraint::Percentage(34),
                ])
                .split(chunks[4]);

            let mut apply_text_style = Style::default();
            let mut apply_border_style = Style::default();

            if self.sort_modal.focus == SortFocus::Apply {
                apply_text_style = apply_text_style.fg(self.color("modal_border_active"));
                apply_border_style = apply_border_style.fg(self.color("modal_border_active"));
            }

            if self.sort_modal.has_unapplied_changes {
                apply_text_style = apply_text_style.add_modifier(Modifier::BOLD);
            }

            Paragraph::new("Apply")
                .style(apply_text_style) // Apply bold text when there are unapplied changes
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(apply_border_style),
                ) // Border style for the block
                .centered()
                .render(btn_layout[0], buf);

            let cancel_style = if self.sort_modal.focus == SortFocus::Cancel {
                Style::default().fg(self.color("modal_border_active"))
            } else {
                Style::default()
            };
            Paragraph::new("Cancel")
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(cancel_style),
                )
                .centered()
                .render(btn_layout[1], buf);

            let clear_style = if self.sort_modal.focus == SortFocus::Clear {
                Style::default().fg(self.color("modal_border_active"))
            } else {
                Style::default()
            };
            Paragraph::new("Clear")
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(clear_style),
                )
                .centered()
                .render(btn_layout[2], buf);
        }

        if self.template_modal.active {
            Clear.render(sort_area, buf);
            let modal_title = match self.template_modal.mode {
                TemplateModalMode::List => "Templates",
                TemplateModalMode::Create => "Create Template",
                TemplateModalMode::Edit => "Edit Template",
            };
            let block = Block::default().borders(Borders::ALL).title(modal_title);
            let inner_area = block.inner(sort_area);
            block.render(sort_area, buf);

            match self.template_modal.mode {
                TemplateModalMode::List => {
                    // List Mode: Show templates as a table with relevance scores
                    let chunks = Layout::default()
                        .direction(Direction::Vertical)
                        .constraints([
                            Constraint::Min(0),    // Template table
                            Constraint::Length(1), // Hints
                        ])
                        .split(inner_area);

                    // Template Table
                    // Find max score for normalization
                    let max_score = self
                        .template_modal
                        .templates
                        .iter()
                        .map(|(_, score)| *score)
                        .fold(0.0, f64::max);

                    // Calculate column widths
                    // Score column: 2 chars, Active column: 1 char, Name column: 20 chars, Description: remaining
                    let score_col_width = 2;
                    let active_col_width = 1;
                    let name_col_width = 20;

                    let rows: Vec<Row> = self
                        .template_modal
                        .templates
                        .iter()
                        .map(|(template, score)| {
                            // Check if this template is active
                            let is_active = self
                                .active_template_id
                                .as_ref()
                                .map(|id| id == &template.id)
                                .unwrap_or(false);

                            // Visual score indicator (circle with fill) - color foreground only
                            let score_ratio = if max_score > 0.0 {
                                score / max_score
                            } else {
                                0.0
                            };
                            let (circle_char, circle_color) = if score_ratio >= 0.8 {
                                // High scores: green, filled circles
                                if score_ratio >= 0.95 {
                                    ('●', self.color("success"))
                                } else if score_ratio >= 0.9 {
                                    ('◉', self.color("success"))
                                } else {
                                    ('◐', self.color("success"))
                                }
                            } else if score_ratio >= 0.4 {
                                // Medium scores: yellow
                                if score_ratio >= 0.7 {
                                    ('◐', self.color("warning"))
                                } else if score_ratio >= 0.55 {
                                    ('◑', self.color("warning"))
                                } else {
                                    ('○', self.color("warning"))
                                }
                            } else {
                                // Low scores: uncolored
                                if score_ratio >= 0.2 {
                                    ('○', self.color("text_primary"))
                                } else {
                                    ('○', self.color("dimmed"))
                                }
                            };

                            // Score cell with colored circle (foreground only)
                            let score_cell = Cell::from(circle_char.to_string())
                                .style(Style::default().fg(circle_color));

                            // Active indicator cell (checkmark)
                            let active_cell = if is_active {
                                Cell::from("✓")
                            } else {
                                Cell::from(" ")
                            };

                            // Name cell
                            let name_cell = Cell::from(template.name.clone());

                            // Description cell - get first line and truncate if needed
                            // Note: actual truncation will be handled by the table widget based on available space
                            let desc = template.description.as_deref().unwrap_or("");
                            let first_line = desc.lines().next().unwrap_or("");
                            let desc_display = first_line.to_string();
                            let desc_cell = Cell::from(desc_display);

                            // Create row with cells (no highlighting)
                            Row::new(vec![score_cell, active_cell, name_cell, desc_cell])
                        })
                        .collect();

                    // Header row
                    let header = Row::new(vec![
                        Cell::from("●").style(Style::default().add_modifier(Modifier::BOLD)),
                        Cell::from(" ").style(Style::default().add_modifier(Modifier::BOLD)), // Active column header (empty)
                        Cell::from("Name").style(Style::default().add_modifier(Modifier::BOLD)),
                        Cell::from("Description")
                            .style(Style::default().add_modifier(Modifier::BOLD)),
                    ])
                    .style(Style::default().add_modifier(Modifier::UNDERLINED));

                    let table_border_style =
                        if self.template_modal.focus == TemplateFocus::TemplateList {
                            Style::default().fg(self.color("modal_border_active"))
                        } else {
                            Style::default()
                        };

                    let table = Table::new(
                        rows,
                        [
                            Constraint::Length(score_col_width),
                            Constraint::Length(active_col_width),
                            Constraint::Length(name_col_width),
                            Constraint::Min(0), // Description takes remaining space
                        ],
                    )
                    .header(header)
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .title("Templates")
                            .border_style(table_border_style),
                    )
                    .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

                    StatefulWidget::render(
                        table,
                        chunks[0],
                        buf,
                        &mut self.template_modal.table_state,
                    );

                    // Keybind Hints - Single line
                    use ratatui::text::{Line, Span};
                    let mut hint_line = Line::default();
                    hint_line.spans.push(Span::styled(
                        "Enter",
                        Style::default()
                            .fg(self.color("keybind_hints"))
                            .add_modifier(Modifier::BOLD),
                    ));
                    hint_line.spans.push(Span::raw(" Apply "));
                    hint_line.spans.push(Span::styled(
                        "s",
                        Style::default()
                            .fg(self.color("keybind_hints"))
                            .add_modifier(Modifier::BOLD),
                    ));
                    hint_line.spans.push(Span::raw(" Create "));
                    hint_line.spans.push(Span::styled(
                        "e",
                        Style::default()
                            .fg(self.color("keybind_hints"))
                            .add_modifier(Modifier::BOLD),
                    ));
                    hint_line.spans.push(Span::raw(" Edit "));
                    hint_line.spans.push(Span::styled(
                        "d",
                        Style::default()
                            .fg(self.color("keybind_hints"))
                            .add_modifier(Modifier::BOLD),
                    ));
                    hint_line.spans.push(Span::raw(" Delete "));
                    hint_line.spans.push(Span::styled(
                        "Esc",
                        Style::default()
                            .fg(self.color("keybind_hints"))
                            .add_modifier(Modifier::BOLD),
                    ));
                    hint_line.spans.push(Span::raw(" Close"));

                    Paragraph::new(vec![hint_line]).render(chunks[1], buf);
                }
                TemplateModalMode::Create | TemplateModalMode::Edit => {
                    // Create/Edit Mode: Multi-step dialog
                    let chunks = Layout::default()
                        .direction(Direction::Vertical)
                        .constraints([
                            Constraint::Length(3), // Name
                            Constraint::Length(6), // Description (taller for multi-line)
                            Constraint::Length(3), // Exact Path
                            Constraint::Length(3), // Relative Path
                            Constraint::Length(3), // Path Pattern
                            Constraint::Length(3), // Filename Pattern
                            Constraint::Length(3), // Schema Match
                            Constraint::Length(3), // Buttons
                        ])
                        .split(inner_area);

                    // Name input
                    let name_style = if self.template_modal.create_focus == CreateFocus::Name {
                        Style::default().fg(self.color("modal_border_active"))
                    } else {
                        Style::default()
                    };
                    let name_title = if let Some(error) = &self.template_modal.name_error {
                        format!("Name {}", error)
                    } else {
                        "Name".to_string()
                    };
                    let name_block = Block::default()
                        .borders(Borders::ALL)
                        .title(name_title)
                        .title_style(if self.template_modal.name_error.is_some() {
                            Style::default().fg(self.color("error"))
                        } else {
                            Style::default().add_modifier(Modifier::BOLD)
                        })
                        .border_style(name_style);
                    let name_inner = name_block.inner(chunks[0]);
                    name_block.render(chunks[0], buf);
                    let name_display = format!(
                        "{}{}",
                        self.template_modal
                            .create_name
                            .chars()
                            .take(self.template_modal.create_name_cursor)
                            .collect::<String>(),
                        self.template_modal
                            .create_name
                            .chars()
                            .skip(self.template_modal.create_name_cursor)
                            .collect::<String>()
                    );
                    let name_input_style = if self.template_modal.create_focus == CreateFocus::Name
                    {
                        Style::default().bg(self.color("surface"))
                    } else {
                        Style::default()
                    };
                    // Add cursor indicator
                    let name_with_cursor = if self.template_modal.create_focus == CreateFocus::Name
                    {
                        format!(
                            "{}█{}",
                            self.template_modal
                                .create_name
                                .chars()
                                .take(self.template_modal.create_name_cursor)
                                .collect::<String>(),
                            self.template_modal
                                .create_name
                                .chars()
                                .skip(self.template_modal.create_name_cursor)
                                .collect::<String>()
                        )
                    } else {
                        name_display
                    };
                    Paragraph::new(name_with_cursor)
                        .style(name_input_style)
                        .render(name_inner, buf);

                    // Description input (scrollable, multi-line)
                    let desc_style = if self.template_modal.create_focus == CreateFocus::Description
                    {
                        Style::default().fg(self.color("modal_border_active"))
                    } else {
                        Style::default()
                    };
                    let desc_block = Block::default()
                        .borders(Borders::ALL)
                        .title("Description")
                        .border_style(desc_style);
                    let desc_inner = desc_block.inner(chunks[1]);
                    desc_block.render(chunks[1], buf);
                    let desc_input_style =
                        if self.template_modal.create_focus == CreateFocus::Description {
                            Style::default().bg(self.color("surface"))
                        } else {
                            Style::default()
                        };

                    // Split description into lines and handle cursor
                    let lines: Vec<&str> = self.template_modal.create_description.lines().collect();
                    let cursor_line = self
                        .template_modal
                        .create_description
                        .chars()
                        .take(self.template_modal.create_description_cursor)
                        .filter(|&c| c == '\n')
                        .count();
                    // Calculate cursor position within the current line
                    let cursor_pos_in_line = {
                        let chars_before_cursor: Vec<char> = self
                            .template_modal
                            .create_description
                            .chars()
                            .take(self.template_modal.create_description_cursor)
                            .collect();
                        let last_newline_pos = chars_before_cursor
                            .iter()
                            .rposition(|&c| c == '\n')
                            .map(|p| p + 1)
                            .unwrap_or(0);
                        chars_before_cursor.len() - last_newline_pos
                    };

                    // Calculate visible lines based on scroll
                    let available_height = desc_inner.height as usize;
                    let max_scroll = lines.len().saturating_sub(available_height).max(0);
                    self.template_modal.description_scroll =
                        self.template_modal.description_scroll.min(max_scroll);

                    // Auto-scroll to keep cursor visible
                    if cursor_line < self.template_modal.description_scroll {
                        self.template_modal.description_scroll = cursor_line;
                    } else if cursor_line
                        >= self.template_modal.description_scroll + available_height
                    {
                        self.template_modal.description_scroll =
                            cursor_line.saturating_sub(available_height - 1);
                    }

                    // Render description with cursor using the same approach as query input
                    use ratatui::text::{Line, Span};
                    let desc_lines: Vec<Line> = if lines.is_empty()
                        && self.template_modal.create_focus == CreateFocus::Description
                    {
                        // Empty description - show cursor on first line
                        vec![Line::from(vec![Span::styled(
                            " ",
                            Style::default()
                                .bg(self.color("text_primary"))
                                .fg(self.color("text_inverse")),
                        )])]
                    } else {
                        lines
                            .iter()
                            .skip(self.template_modal.description_scroll)
                            .take(available_height)
                            .enumerate()
                            .map(|(i, line)| {
                                let line_idx = self.template_modal.description_scroll + i;
                                if self.template_modal.create_focus == CreateFocus::Description
                                    && line_idx == cursor_line
                                {
                                    // This is the line with the cursor - render with styled cursor character
                                    let before_cursor: String =
                                        line.chars().take(cursor_pos_in_line).collect();
                                    let mut chars_iter = line.chars().skip(cursor_pos_in_line);
                                    let at_cursor = chars_iter
                                        .next()
                                        .map(|c| c.to_string())
                                        .unwrap_or_else(|| " ".to_string());
                                    let after_cursor: String = chars_iter.collect();

                                    let mut line_spans = Line::default();
                                    line_spans.spans.push(Span::raw(before_cursor));
                                    line_spans.spans.push(Span::styled(
                                        at_cursor,
                                        Style::default()
                                            .bg(self.color("text_primary"))
                                            .fg(self.color("text_inverse")),
                                    ));
                                    if !after_cursor.is_empty() {
                                        line_spans.spans.push(Span::raw(after_cursor));
                                    }
                                    line_spans
                                } else {
                                    // Regular line without cursor
                                    Line::from(line.to_string())
                                }
                            })
                            .collect()
                    };

                    Paragraph::new(desc_lines)
                        .style(desc_input_style)
                        .wrap(ratatui::widgets::Wrap { trim: false })
                        .render(desc_inner, buf);

                    // Exact Path
                    let exact_path_style =
                        if self.template_modal.create_focus == CreateFocus::ExactPath {
                            Style::default().fg(self.color("modal_border_active"))
                        } else {
                            Style::default()
                        };
                    let exact_path_block = Block::default()
                        .borders(Borders::ALL)
                        .title("Exact Path")
                        .border_style(exact_path_style);
                    let exact_path_inner = exact_path_block.inner(chunks[2]);
                    exact_path_block.render(chunks[2], buf);
                    let exact_path_input_style =
                        if self.template_modal.create_focus == CreateFocus::ExactPath {
                            Style::default().bg(self.color("surface"))
                        } else {
                            Style::default()
                        };
                    let exact_path_with_cursor =
                        if self.template_modal.create_focus == CreateFocus::ExactPath {
                            format!(
                                "{}█{}",
                                self.template_modal
                                    .create_exact_path
                                    .chars()
                                    .take(self.template_modal.create_exact_path_cursor)
                                    .collect::<String>(),
                                self.template_modal
                                    .create_exact_path
                                    .chars()
                                    .skip(self.template_modal.create_exact_path_cursor)
                                    .collect::<String>()
                            )
                        } else {
                            self.template_modal.create_exact_path.clone()
                        };
                    Paragraph::new(exact_path_with_cursor)
                        .style(exact_path_input_style)
                        .render(exact_path_inner, buf);

                    // Relative Path
                    let relative_path_style =
                        if self.template_modal.create_focus == CreateFocus::RelativePath {
                            Style::default().fg(self.color("modal_border_active"))
                        } else {
                            Style::default()
                        };
                    let relative_path_block = Block::default()
                        .borders(Borders::ALL)
                        .title("Relative Path")
                        .border_style(relative_path_style);
                    let relative_path_inner = relative_path_block.inner(chunks[3]);
                    relative_path_block.render(chunks[3], buf);
                    let relative_path_input_style =
                        if self.template_modal.create_focus == CreateFocus::RelativePath {
                            Style::default().bg(self.color("surface"))
                        } else {
                            Style::default()
                        };
                    let relative_path_with_cursor =
                        if self.template_modal.create_focus == CreateFocus::RelativePath {
                            format!(
                                "{}█{}",
                                self.template_modal
                                    .create_relative_path
                                    .chars()
                                    .take(self.template_modal.create_relative_path_cursor)
                                    .collect::<String>(),
                                self.template_modal
                                    .create_relative_path
                                    .chars()
                                    .skip(self.template_modal.create_relative_path_cursor)
                                    .collect::<String>()
                            )
                        } else {
                            self.template_modal.create_relative_path.clone()
                        };
                    Paragraph::new(relative_path_with_cursor)
                        .style(relative_path_input_style)
                        .render(relative_path_inner, buf);

                    // Path Pattern
                    let path_pattern_style =
                        if self.template_modal.create_focus == CreateFocus::PathPattern {
                            Style::default().fg(self.color("modal_border_active"))
                        } else {
                            Style::default()
                        };
                    let path_pattern_block = Block::default()
                        .borders(Borders::ALL)
                        .title("Path Pattern")
                        .border_style(path_pattern_style);
                    let path_pattern_inner = path_pattern_block.inner(chunks[4]);
                    path_pattern_block.render(chunks[4], buf);
                    let path_pattern_input_style =
                        if self.template_modal.create_focus == CreateFocus::PathPattern {
                            Style::default().bg(self.color("surface"))
                        } else {
                            Style::default()
                        };
                    let path_pattern_with_cursor =
                        if self.template_modal.create_focus == CreateFocus::PathPattern {
                            format!(
                                "{}█{}",
                                self.template_modal
                                    .create_path_pattern
                                    .chars()
                                    .take(self.template_modal.create_path_pattern_cursor)
                                    .collect::<String>(),
                                self.template_modal
                                    .create_path_pattern
                                    .chars()
                                    .skip(self.template_modal.create_path_pattern_cursor)
                                    .collect::<String>()
                            )
                        } else {
                            self.template_modal.create_path_pattern.clone()
                        };
                    Paragraph::new(path_pattern_with_cursor)
                        .style(path_pattern_input_style)
                        .render(path_pattern_inner, buf);

                    // Filename Pattern
                    let filename_pattern_style =
                        if self.template_modal.create_focus == CreateFocus::FilenamePattern {
                            Style::default().fg(self.color("modal_border_active"))
                        } else {
                            Style::default()
                        };
                    let filename_pattern_block = Block::default()
                        .borders(Borders::ALL)
                        .title("Filename Pattern")
                        .border_style(filename_pattern_style);
                    let filename_pattern_inner = filename_pattern_block.inner(chunks[5]);
                    filename_pattern_block.render(chunks[5], buf);
                    let filename_pattern_input_style =
                        if self.template_modal.create_focus == CreateFocus::FilenamePattern {
                            Style::default().bg(self.color("surface"))
                        } else {
                            Style::default()
                        };
                    let filename_pattern_with_cursor =
                        if self.template_modal.create_focus == CreateFocus::FilenamePattern {
                            format!(
                                "{}█{}",
                                self.template_modal
                                    .create_filename_pattern
                                    .chars()
                                    .take(self.template_modal.create_filename_pattern_cursor)
                                    .collect::<String>(),
                                self.template_modal
                                    .create_filename_pattern
                                    .chars()
                                    .skip(self.template_modal.create_filename_pattern_cursor)
                                    .collect::<String>()
                            )
                        } else {
                            self.template_modal.create_filename_pattern.clone()
                        };
                    Paragraph::new(filename_pattern_with_cursor)
                        .style(filename_pattern_input_style)
                        .render(filename_pattern_inner, buf);

                    // Schema Match
                    let schema_style =
                        if self.template_modal.create_focus == CreateFocus::SchemaMatch {
                            Style::default().fg(self.color("modal_border_active"))
                        } else {
                            Style::default()
                        };
                    let schema_text = if self.template_modal.create_schema_match_enabled {
                        "Enabled"
                    } else {
                        "Disabled"
                    };
                    Paragraph::new(schema_text)
                        .block(
                            Block::default()
                                .borders(Borders::ALL)
                                .title("Schema Match")
                                .border_style(schema_style),
                        )
                        .render(chunks[6], buf);

                    // Buttons
                    let btn_layout = Layout::default()
                        .direction(Direction::Horizontal)
                        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                        .split(chunks[7]);

                    let save_style = if self.template_modal.create_focus == CreateFocus::SaveButton
                    {
                        Style::default().fg(self.color("modal_border_active"))
                    } else {
                        Style::default()
                    };
                    Paragraph::new("Save")
                        .block(
                            Block::default()
                                .borders(Borders::ALL)
                                .border_style(save_style),
                        )
                        .centered()
                        .render(btn_layout[0], buf);

                    let cancel_create_style =
                        if self.template_modal.create_focus == CreateFocus::CancelButton {
                            Style::default().fg(self.color("modal_border_active"))
                        } else {
                            Style::default()
                        };
                    Paragraph::new("Cancel")
                        .block(
                            Block::default()
                                .borders(Borders::ALL)
                                .border_style(cancel_create_style),
                        )
                        .centered()
                        .render(btn_layout[1], buf);
                }
            }

            // Delete Confirmation Dialog
            if self.template_modal.delete_confirm {
                if let Some(template) = self.template_modal.selected_template() {
                    let confirm_area = centered_rect(sort_area, 50, 20);
                    Clear.render(confirm_area, buf);
                    let block = Block::default()
                        .borders(Borders::ALL)
                        .title("Delete Template");
                    let inner_area = block.inner(confirm_area);
                    block.render(confirm_area, buf);

                    let chunks = Layout::default()
                        .direction(Direction::Vertical)
                        .constraints([
                            Constraint::Min(0),    // Message
                            Constraint::Length(3), // Buttons
                        ])
                        .split(inner_area);

                    let message = format!(
                        "Are you sure you want to delete the template \"{}\"?\n\nThis action cannot be undone.",
                        template.name
                    );
                    Paragraph::new(message)
                        .wrap(ratatui::widgets::Wrap { trim: false })
                        .render(chunks[0], buf);

                    let btn_layout = Layout::default()
                        .direction(Direction::Horizontal)
                        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                        .split(chunks[1]);

                    // Delete button - highlight "D" in blue
                    use ratatui::text::{Line, Span};
                    let mut delete_line = Line::default();
                    delete_line.spans.push(Span::styled(
                        "D",
                        Style::default()
                            .fg(self.color("keybind_hints"))
                            .add_modifier(Modifier::BOLD),
                    ));
                    delete_line.spans.push(Span::raw("elete"));

                    let delete_style = if self.template_modal.delete_confirm_focus {
                        Style::default().fg(self.color("modal_border_active"))
                    } else {
                        Style::default()
                    };
                    Paragraph::new(vec![delete_line])
                        .block(
                            Block::default()
                                .borders(Borders::ALL)
                                .border_style(delete_style),
                        )
                        .centered()
                        .render(btn_layout[0], buf);

                    // Cancel button (default selected)
                    let cancel_style = if !self.template_modal.delete_confirm_focus {
                        Style::default().fg(self.color("modal_border_active"))
                    } else {
                        Style::default()
                    };
                    Paragraph::new("Cancel")
                        .block(
                            Block::default()
                                .borders(Borders::ALL)
                                .border_style(cancel_style),
                        )
                        .centered()
                        .render(btn_layout[1], buf);
                }
            }

            // Score Details Dialog
            if self.template_modal.show_score_details {
                if let Some((template, score)) = self
                    .template_modal
                    .table_state
                    .selected()
                    .and_then(|idx| self.template_modal.templates.get(idx))
                {
                    if let Some(ref state) = self.data_table_state {
                        if let Some(ref path) = self.path {
                            let details_area = centered_rect(sort_area, 60, 50);
                            Clear.render(details_area, buf);
                            let block = Block::default()
                                .borders(Borders::ALL)
                                .title(format!("Score Details: {}", template.name));
                            let inner_area = block.inner(details_area);
                            block.render(details_area, buf);

                            // Calculate score components
                            let exact_path_match = template
                                .match_criteria
                                .exact_path
                                .as_ref()
                                .map(|exact| exact == path)
                                .unwrap_or(false);

                            let relative_path_match = if let Some(relative_path) =
                                &template.match_criteria.relative_path
                            {
                                if let Ok(cwd) = std::env::current_dir() {
                                    if let Ok(rel_path) = path.strip_prefix(&cwd) {
                                        rel_path.to_string_lossy() == *relative_path
                                    } else {
                                        false
                                    }
                                } else {
                                    false
                                }
                            } else {
                                false
                            };

                            let exact_schema_match = if let Some(required_cols) =
                                &template.match_criteria.schema_columns
                            {
                                let file_cols: std::collections::HashSet<&str> =
                                    state.schema.iter_names().map(|s| s.as_str()).collect();
                                let required_cols_set: std::collections::HashSet<&str> =
                                    required_cols.iter().map(|s| s.as_str()).collect();
                                required_cols_set.is_subset(&file_cols)
                                    && file_cols.len() == required_cols_set.len()
                            } else {
                                false
                            };

                            // Build score details text
                            let mut details = format!("Total Score: {:.1}\n\n", score);

                            if exact_path_match && exact_schema_match {
                                details.push_str("Exact Path + Exact Schema: 2000.0\n");
                            } else if exact_path_match {
                                details.push_str("Exact Path: 1000.0\n");
                            } else if relative_path_match && exact_schema_match {
                                details.push_str("Relative Path + Exact Schema: 1950.0\n");
                            } else if relative_path_match {
                                details.push_str("Relative Path: 950.0\n");
                            } else if exact_schema_match {
                                details.push_str("Exact Schema: 900.0\n");
                            } else {
                                // For non-exact matches, show component breakdown
                                if let Some(pattern) = &template.match_criteria.path_pattern {
                                    if path
                                        .to_str()
                                        .map(|p| p.contains(pattern.trim_end_matches("/*")))
                                        .unwrap_or(false)
                                    {
                                        details.push_str("Path Pattern Match: 50.0+\n");
                                    }
                                }
                                if let Some(pattern) = &template.match_criteria.filename_pattern {
                                    if path
                                        .file_name()
                                        .and_then(|f| f.to_str())
                                        .map(|f| {
                                            f.contains(pattern.trim_end_matches("*"))
                                                || pattern == "*"
                                        })
                                        .unwrap_or(false)
                                    {
                                        details.push_str("Filename Pattern Match: 30.0+\n");
                                    }
                                }
                                if let Some(required_cols) = &template.match_criteria.schema_columns
                                {
                                    let file_cols: std::collections::HashSet<&str> =
                                        state.schema.iter_names().map(|s| s.as_str()).collect();
                                    let matching_count = required_cols
                                        .iter()
                                        .filter(|col| file_cols.contains(col.as_str()))
                                        .count();
                                    if matching_count > 0 {
                                        details.push_str(&format!(
                                            "Partial Schema Match: {:.1} ({} columns)\n",
                                            matching_count as f64 * 2.0,
                                            matching_count
                                        ));
                                    }
                                }
                            }

                            if template.usage_count > 0 {
                                details.push_str(&format!(
                                    "Usage Count: {:.1}\n",
                                    (template.usage_count.min(10) as f64) * 1.0
                                ));
                            }
                            if let Some(last_used) = template.last_used {
                                if let Ok(duration) =
                                    std::time::SystemTime::now().duration_since(last_used)
                                {
                                    let days_since = duration.as_secs() / 86400;
                                    if days_since <= 7 {
                                        details.push_str("Recent Usage: 5.0\n");
                                    } else if days_since <= 30 {
                                        details.push_str("Recent Usage: 2.0\n");
                                    }
                                }
                            }
                            if let Ok(duration) =
                                std::time::SystemTime::now().duration_since(template.created)
                            {
                                let months_old = (duration.as_secs() / (30 * 86400)) as f64;
                                if months_old > 0.0 {
                                    details.push_str(&format!(
                                        "Age Penalty: -{:.1}\n",
                                        months_old * 1.0
                                    ));
                                }
                            }

                            Paragraph::new(details)
                                .wrap(ratatui::widgets::Wrap { trim: false })
                                .render(inner_area, buf);
                        }
                    }
                }
            }
        }

        if self.pivot_melt_modal.active {
            let border = self.color("modal_border");
            let active = self.color("modal_border_active");
            let text_primary = self.color("text_primary");
            let text_inverse = self.color("text_inverse");
            pivot_melt::render_shell(
                sort_area,
                buf,
                &mut self.pivot_melt_modal,
                border,
                active,
                text_primary,
                text_inverse,
            );
        }

        // Render analysis modal (full screen in main area, leaving toolbar visible)
        if self.analysis_modal.active {
            // Use main_area so toolbar remains visible at bottom
            let analysis_area = main_area;

            if let Some(state) = &self.data_table_state {
                // Compute statistics if not already computed or if seed changed
                let needs_recompute = self.analysis_modal.analysis_results.is_none()
                    || self
                        .analysis_modal
                        .analysis_results
                        .as_ref()
                        .map(|r| r.sample_seed != self.analysis_modal.random_seed)
                        .unwrap_or(true);

                if needs_recompute {
                    // Use the LazyFrame directly from state (it already respects queries/filters)
                    let lf = state.lf.clone();
                    // Only compute basic statistics by default (no distribution analysis, no correlation matrix)
                    let options = crate::statistics::ComputeOptions {
                        include_distribution_info: false,
                        include_distribution_analyses: false,
                        include_correlation_matrix: false,
                        include_skewness_kurtosis_outliers: false,
                    };
                    match crate::statistics::compute_statistics_with_options(
                        &lf,
                        Some(self.sampling_threshold),
                        self.analysis_modal.random_seed,
                        options,
                    ) {
                        Ok(results) => {
                            self.analysis_modal.analysis_results = Some(results);
                        }
                        Err(e) => {
                            eprintln!("Error computing statistics: {}", e);
                            // Still render the modal with error message
                            Clear.render(analysis_area, buf);
                            let error_msg = format!("Error computing statistics: {}", e);
                            Paragraph::new(error_msg)
                                .centered()
                                .style(Style::default().fg(self.color("error")))
                                .render(analysis_area, buf);
                            // Don't return - continue to render toolbar
                        }
                    }
                }

                // Lazy computation: compute additional stats if needed for the selected tool
                {
                    let lf = state.lf.clone();
                    let selected_tool = self.analysis_modal.selected_tool;
                    if let Some(ref mut results) = self.analysis_modal.analysis_results {
                        match selected_tool {
                            crate::analysis_modal::AnalysisTool::DistributionAnalysis => {
                                if results.distribution_analyses.is_empty() {
                                    if let Err(e) =
                                        crate::statistics::compute_distribution_statistics(
                                            results,
                                            &lf,
                                            None,
                                            self.analysis_modal.random_seed,
                                        )
                                    {
                                        eprintln!("Error computing distribution statistics: {}", e);
                                    }
                                }
                            }
                            crate::analysis_modal::AnalysisTool::CorrelationMatrix => {
                                if results.correlation_matrix.is_none() {
                                    if let Err(e) =
                                        crate::statistics::compute_correlation_statistics(
                                            results, &lf,
                                        )
                                    {
                                        eprintln!("Error computing correlation statistics: {}", e);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }

                if let Some(ref results) = self.analysis_modal.analysis_results {
                    let context = state.get_analysis_context();
                    Clear.render(analysis_area, buf);
                    // Use tool-specific column_offset
                    let column_offset = match self.analysis_modal.selected_tool {
                        analysis_modal::AnalysisTool::Describe => {
                            self.analysis_modal.describe_column_offset
                        }
                        analysis_modal::AnalysisTool::DistributionAnalysis => {
                            self.analysis_modal.distribution_column_offset
                        }
                        analysis_modal::AnalysisTool::CorrelationMatrix => {
                            self.analysis_modal.correlation_column_offset
                        }
                    };

                    let config = widgets::analysis::AnalysisWidgetConfig {
                        state,
                        results: Some(results),
                        context: &context,
                        view: self.analysis_modal.view,
                        selected_tool: self.analysis_modal.selected_tool,
                        column_offset,
                        selected_correlation: self.analysis_modal.selected_correlation,
                        focus: self.analysis_modal.focus,
                        selected_theoretical_distribution: self
                            .analysis_modal
                            .selected_theoretical_distribution,
                        theme: &self.theme,
                    };
                    let widget = widgets::analysis::AnalysisWidget::new(
                        config,
                        &mut self.analysis_modal.table_state,
                        &mut self.analysis_modal.distribution_table_state,
                        &mut self.analysis_modal.correlation_table_state,
                        &mut self.analysis_modal.sidebar_state,
                        &mut self.analysis_modal.distribution_selector_state,
                    );
                    widget.render(analysis_area, buf);
                } else {
                    // Still computing
                    Clear.render(analysis_area, buf);
                    Paragraph::new("Computing statistics...")
                        .centered()
                        .render(analysis_area, buf);
                }
            } else {
                // No data available
                Clear.render(analysis_area, buf);
                Paragraph::new("No data available for analysis")
                    .centered()
                    .style(Style::default().fg(self.color("warning")))
                    .render(analysis_area, buf);
            }
            // Don't return - continue to render toolbar and other UI elements
        }

        // Render error modal (has highest priority, shows on top of everything)
        if self.error_modal.active {
            let popup_area = centered_rect(area, 70, 40);
            Clear.render(popup_area, buf);
            let block = Block::default()
                .borders(Borders::ALL)
                .title("Error")
                .border_style(Style::default().fg(self.color("modal_border_error")));
            let inner_area = block.inner(popup_area);
            block.render(popup_area, buf);

            // Split inner area into message and button
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Min(0),    // Message (takes available space)
                    Constraint::Length(3), // OK button
                ])
                .split(inner_area);

            // Render error message (wrapped)
            Paragraph::new(self.error_modal.message.as_str())
                .style(Style::default().fg(self.color("error")))
                .wrap(ratatui::widgets::Wrap { trim: true })
                .render(chunks[0], buf);

            // Render OK button
            let ok_style = Style::default().fg(self.color("modal_border_active"));
            Paragraph::new("[ OK ]")
                .centered()
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(ok_style),
                )
                .render(chunks[1], buf);
        }

        if self.show_help
            || (self.template_modal.active && self.template_modal.show_help)
            || (self.analysis_modal.active && self.analysis_modal.show_help)
        {
            let popup_area = centered_rect(area, 60, 50);
            Clear.render(popup_area, buf);
            let (title, text): (String, String) = if self.analysis_modal.active
                && self.analysis_modal.show_help
            {
                // Context-aware help based on current view and tool
                match self.analysis_modal.view {
                    analysis_modal::AnalysisView::DistributionDetail => (
                        "Distribution Detail Help".to_string(),
                        "\
SW: The W statistic of a Shapiro-Wilk test ranges from 0 to 1, where 1 indicates perfect normality. The p-value reflects the probability of observing such a W value under the hypothesis of normality.
Skew: Measures asymmetry of the data distribution (positive = right-tailed, negative = left-tailed).
Kurtosis: Tail heaviness compared to normal distribution (high = heavy tails, low = light tails).
Median: Middle value when data is sorted.
Mean: Average value of all data points.
Std: Standard deviation (spread of data around the mean).
CV: Coefficient of variation (std/mean, relative variability independent of scale).

Q-Q Plot:

Compares your data against a theoretical distribution. Points following the diagonal reference line indicate a good match. Deviations show where your data differs from the theoretical model.

Histogram:

Shows the frequency distribution of your data as bars, with a theoretical distribution overlaid as a gray line. The height of bars represents how many data points fall in each bin range. Compare bar heights to the theoretical line to see how well your data matches the expected distribution.


Distributions:

Select different theoretical distributions from the list to overlay them for comparison with your data. This helps identify which distribution type best fits your data.

Navigation:

↑↓ / j/k:    Scroll through distributions to compare different overlays
Esc:         Return to distribution table"
                            .to_string(),
                    ),
                    analysis_modal::AnalysisView::CorrelationDetail => (
                        "Correlation Detail Help".to_string(),
                        "\
Correlation Pair Detail View shows detailed information about a selected pair.

Sections:
  1. Relationship Summary:
     - Correlation coefficient with interpretation
     - Statistical significance (p-value)
     - Sample size
     - Plain-language interpretation

  2. Scatter Plot Approximation:
     - Text-based scatter plot showing relationship
     - Trend indicators

  3. Key Statistics:
     - Summary statistics for both variables
     - Covariance and R-squared

Navigation:
  ↑↓:            Scroll if content is long
  Esc:           Return to correlation matrix"
                            .to_string(),
                    ),
                    analysis_modal::AnalysisView::Main => match self.analysis_modal.selected_tool {
                        analysis_modal::AnalysisTool::DistributionAnalysis => (
                            "Distribution Analysis Help".to_string(),
                            "\
Distribution Analysis identifies the distribution type for each numeric column and provides key statistical measures.

Columns:
  Column:        Name of the numeric column
  Distribution:  Inferred distribution type (Normal, LogNormal, Uniform, PowerLaw, Exponential)
  Shapiro-Wilk:  W statistic from Shapiro-Wilk normality test (0-1, higher = more normal)
  SW p-value:    P-value from Shapiro-Wilk test (probability of observing W under normality)
  CV:            Coefficient of variation (std/mean, relative variability independent of scale)
  Outliers:      Count and percentage of outliers (IQR method)
  Skewness:      Asymmetry measure (positive = right-tailed, negative = left-tailed)
  Kurtosis:      Tail heaviness compared to normal distribution (3.0 = normal)

Color Coding:
  Distribution types are color-coded:
    - Green/Cyan: Good fit quality (>0.75)
    - Yellow:     Moderate fit quality (≤0.75)
    - Red:        Very high outlier percentage (>20%) or extreme skewness/kurtosis

Navigation:
  ↑↓ / j/k:      Navigate rows
  ←→ / h/l:      Scroll columns horizontally
  Tab:           Switch focus between main area and sidebar
  Enter:         Open detail view for selected column (shows Q-Q plot and histogram)
  Esc:           Close analysis view
  r:             Resample data (only shown if data was sampled)

Detail View:
  Press Enter on a row to see detailed analysis with Q-Q plots and histograms comparing your data to theoretical distributions."
                                .to_string(),
                        ),
                        analysis_modal::AnalysisTool::Describe => (
                            "Describe Tool Help".to_string(),
                            "\
The Describe tool behaves like Polars' describe() function and displays similar descriptive statistics.

Navigation:
  Tab:            Switch focus between main area and sidebar
  ↑↓ / j/k:      Navigate rows (or sidebar tools if sidebar focused)
  ←→ / h/l:      Scroll statistics columns horizontally
  Home/End:      Jump to first/last row
  PageUp/PageDown: Navigate by page
  Enter:         Select tool from sidebar (when sidebar focused)

Actions:
  r:             Resample data (only shown if data was sampled)
  Esc:           Close analysis view or help dialog"
                                .to_string(),
                        ),
                        analysis_modal::AnalysisTool::CorrelationMatrix => (
                            "Correlation Matrix Help".to_string(),
                            "\
The Correlation Matrix tool displays pairwise correlations between numeric columns.

Navigation:
  Tab:            Switch focus between main area and sidebar
  ↑↓ / j/k:      Navigate matrix rows (or sidebar tools if sidebar focused)
  ←→ / h/l:      Navigate matrix columns
  Home/End:      Jump to first/last row
  PageUp/PageDown: Navigate by page
  Enter:         Open pair detail view (on a cell) or select tool (sidebar)

Actions:
  r:             Resample data (only shown if data was sampled)
  Esc:           Close analysis view or help dialog"
                                .to_string(),
                        ),
                    },
                }
            } else if self.template_modal.active {
                (
                    "Template Help".to_string(),
                    "\
Templates allow you to save and automatically apply customizations for specific files.

Template List:
  - Templates are displayed in a table sorted by relevance (descending)
  - Score column shows colored circles indicating match quality:
    * Green circles: high scores (80%+ of max)
    * Yellow circles: medium scores (40-80% of max)
    * Gray/white circles: low scores (<40% of max)
  - Active template is indicated by a checkmark (✓) in the active column

Actions:
  Enter:        Apply the selected template
  s:            Create a new template from current state
  e:            Edit the selected template
  d:            Delete the selected template (with confirmation)
  Ctrl+h:       Show this help
  Esc:          Close template menu

Template Creation/Editing:
  - Name:       Required identifier for the template
  - Description: Optional description
  - Exact Path: Match files by absolute path
  - Relative Path: Match files by path relative to current directory
  - Path Pattern: Match files by glob-like path pattern
  - Filename Pattern: Match files by glob-like filename pattern
  - Schema Match: Match files with matching column names

Template Matching:
  Templates are scored based on:
  1. Exact path match (highest priority)
  2. Relative path match
  3. Path/filename pattern matches
  4. Schema column matches
  5. Generic templates (lowest priority)

  The most relevant template can be applied with 'T' from the main view.

Delete Confirmation:
  - Cancel is selected by default
  - Enter:      Cancel (if Cancel selected) or Delete (if Delete selected)
  - Tab:        Switch between Cancel and Delete buttons
  - D:          Delete immediately
  - Esc:        Cancel and close dialog"
                        .to_string(),
                )
            } else {
                let (t, txt) = self.get_help_info();
                (t.to_string(), txt.to_string())
            };

            // Create layout with scrollbar
            let help_layout = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Fill(1), Constraint::Length(1)])
                .split(popup_area);

            let text_area = help_layout[0];
            let scrollbar_area = help_layout[1];

            // Render text with scroll offset
            let block = Block::default().title(title).borders(Borders::ALL);
            let inner_area = block.inner(text_area);
            block.render(text_area, buf);

            // Split text into source lines
            let text_lines: Vec<&str> = text.as_str().lines().collect();
            let available_width = inner_area.width as usize;
            let available_height = inner_area.height as usize;

            // Calculate wrapped lines for each source line
            let mut wrapped_lines = Vec::new();
            for line in &text_lines {
                if line.len() <= available_width {
                    wrapped_lines.push(*line);
                } else {
                    // Split long lines into wrapped segments
                    let mut remaining = *line;
                    while !remaining.is_empty() {
                        let take = remaining.len().min(available_width);
                        let (chunk, rest) = remaining.split_at(take);
                        wrapped_lines.push(chunk);
                        remaining = rest;
                    }
                }
            }

            let total_wrapped_lines = wrapped_lines.len();

            // Clamp scroll position
            let max_scroll = total_wrapped_lines.saturating_sub(available_height).max(0);
            // Use analysis modal's help scroll if in analysis help, otherwise use main help scroll
            let current_scroll = if self.analysis_modal.active && self.analysis_modal.show_help {
                // For now, use main help_scroll - could add separate scroll for analysis if needed
                self.help_scroll
            } else {
                self.help_scroll
            };
            let clamped_scroll = current_scroll.min(max_scroll);
            if self.analysis_modal.active && self.analysis_modal.show_help {
                // Could store in analysis_modal if needed, but for now use main help_scroll
                self.help_scroll = clamped_scroll;
            } else {
                self.help_scroll = clamped_scroll;
            }

            // Get visible lines (use clamped scroll)
            let scroll_pos = self.help_scroll;
            let visible_lines: Vec<&str> = wrapped_lines
                .iter()
                .skip(scroll_pos)
                .take(available_height)
                .copied()
                .collect();

            let visible_text = visible_lines.join("\n");
            Paragraph::new(visible_text)
                .wrap(ratatui::widgets::Wrap { trim: false })
                .render(inner_area, buf);

            // Render scrollbar if content is scrollable
            if total_wrapped_lines > available_height {
                let scrollbar_height = scrollbar_area.height;
                let scroll_pos = self.help_scroll;
                let scrollbar_pos = if max_scroll > 0 {
                    ((scroll_pos as f64 / max_scroll as f64)
                        * (scrollbar_height.saturating_sub(1) as f64)) as u16
                } else {
                    0
                };

                // Calculate thumb size (proportion of visible content)
                let thumb_size = ((available_height as f64 / total_wrapped_lines as f64)
                    * scrollbar_height as f64)
                    .max(1.0) as u16;
                let thumb_size = thumb_size.min(scrollbar_height);

                // Draw scrollbar track
                for y in 0..scrollbar_height {
                    let is_thumb = y >= scrollbar_pos && y < scrollbar_pos + thumb_size;
                    let style = if is_thumb {
                        Style::default().bg(self.color("text_primary"))
                    } else {
                        Style::default().bg(self.color("surface"))
                    };
                    buf.set_string(scrollbar_area.x, scrollbar_area.y + y, "█", style);
                }
            }
        }

        // Get row count from state if available
        let row_count = self.data_table_state.as_ref().map(|s| s.num_rows);
        // Check if query is active
        let query_active = self
            .data_table_state
            .as_ref()
            .map(|s| !s.active_query.trim().is_empty())
            .unwrap_or(false);
        // Dim controls when any modal is active (except analysis modal uses its own controls)
        let is_modal_active = self.show_help
            || self.input_mode == InputMode::Editing
            || self.input_mode == InputMode::Filtering
            || self.input_mode == InputMode::PivotMelt
            || self.sort_modal.active
            || self.filter_modal.active;

        // Build controls - use analysis-specific controls if analysis modal is active
        let mut controls = Controls::with_row_count(row_count.unwrap_or(0)).with_colors(
            self.color("controls_bg"),
            self.color("keybind_hints"),  // Keys in cyan (bold)
            self.color("keybind_labels"), // Labels in yellow
        );

        if self.analysis_modal.active {
            // Build analysis-specific controls based on view
            let mut analysis_controls = vec![
                ("Esc", "Back"),
                ("↑↓", "Navigate"),
                ("←→", "Scroll Columns"),
                ("Tab", "Sidebar"),
                ("Enter", "Select"),
            ];

            // Add r Resample if data is sampled
            if let Some(results) = &self.analysis_modal.analysis_results {
                if results.sample_size.is_some() {
                    analysis_controls.push(("r", "Resample"));
                }
            }

            controls = controls.with_custom_controls(analysis_controls);
        } else {
            controls = controls
                .with_dimmed(is_modal_active)
                .with_query_active(query_active);
        }

        controls.render(controls_area, buf);
        if self.debug.enabled && layout.len() > debug_area_index {
            self.debug.render(layout[debug_area_index], buf);
        }
    }
}

fn centered_rect(r: Rect, percent_x: u16, percent_y: u16) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
